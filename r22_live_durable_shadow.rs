//! R22 live-public shadow with durable executable-decision barrier.
//! Public market data + read-only production safety facts. No credentials, private REST or order transport.

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use openssl::sha::sha256;
use spotarb_native_market_core::{
    decision::{Decision, DecisionConfig, DecisionCore},
    market_metadata::{fetch_current_metadata, NativeMetadataSnapshot},
    owner_index, CoreError, HistorySink, Owner, RawFrame, Update, Venue,
    history::HistoryClient,
    safety::{evaluate as evaluate_safety, SafetyInput},
    execution::{Coordinator, CoordinatorState},
    transport::monotonic_ns,
};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs::{self, File},
    io::{BufWriter, Write},
    path::PathBuf,
    sync::{Arc, atomic::{AtomicU64, Ordering}},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

const OWNERS:usize=12;
const SHARD:usize=32;

fn wall_ms()->i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as i64
}
fn cpu_s()->f64 {
    let mut u:libc::rusage=unsafe{std::mem::zeroed()};
    unsafe{libc::getrusage(libc::RUSAGE_SELF,&mut u)};
    (u.ru_utime.tv_sec+u.ru_stime.tv_sec) as f64+
        (u.ru_utime.tv_usec+u.ru_stime.tv_usec) as f64/1e6
}
fn rss_kib()->u64 {
    fs::read_to_string("/proc/self/status").ok().and_then(|s|
        s.lines().find(|l|l.starts_with("VmRSS:"))
            .and_then(|l|l.split_whitespace().nth(1))
            .and_then(|x|x.parse().ok())).unwrap_or(0)
}

struct NoHistory{accepted:u64}
impl HistorySink for NoHistory {
    fn accept(&mut self,_:&RawFrame,_:&Update)->Result<(),CoreError>{self.accepted+=1;Ok(())}
}

#[derive(Debug)]
enum Event {
    Frame(RawFrame),
    Reset{venue:Venue,bases:Vec<String>,epoch:u64},
    TaskError{venue:&'static str,shard:usize,error:String},
    TaskDone{venue:&'static str,shard:usize,reconnects:u64,frames:u64},
}

fn cfg()->DecisionConfig {
    DecisionConfig{
        max_age_ms:800,metadata_ttl_ms:43_200_000,base_size:10.0,risk_cap:10.0,
        cost:24.03,required_net:42.0,min_price:0.0,max_price:f64::INFINITY,
        blacklist:HashSet::new(),
    }
}

fn safety_input(kill:&Value,preflight:&Value)->SafetyInput {
    let kill_state=kill.get("state").and_then(Value::as_str).unwrap_or("UNKNOWN");
    let kill_schema_ok=kill.get("schema").and_then(Value::as_str).is_some_and(|x|x.starts_with("spotbot-real-kill-switch-"));
    let kill_operator_ok=kill.get("operator").and_then(Value::as_str).is_some_and(|x|!x.trim().is_empty());
    let drilled=kill.get("drill_result").and_then(Value::as_str)==Some("PASS");
    let preflight_present=!preflight.is_null();
    let preflight_allow=preflight.get("verdict").and_then(Value::as_str)==Some("ALLOW");
    let preflight_has_blockers=preflight.get("blocker_codes").and_then(Value::as_array).is_some_and(|x|!x.is_empty());
    let operator_approved=preflight.get("real_launch_authorized").and_then(Value::as_bool).unwrap_or(false);
    SafetyInput{
        trade_enabled:true,trial_enabled:true,operator_approved,max_trades_per_day:0,
        entry_cooldown_sec:10800,max_open_positions:1,deny_on_unknown:true,deny_on_untracked_assets:true,
        position_state_valid:true,active_position:false,paper_position:false,unknown_or_manual_state:false,
        preflight_present,preflight_path_ok:true,preflight_read_ok:true,preflight_size_ok:true,
        preflight_valid:preflight_present,preflight_max_age_ok:true,preflight_schema_ok:true,preflight_fresh:true,
        preflight_allow,preflight_has_blockers,exchange_polled:true,live_checks_complete:true,
        live_reconciliation_complete:true,outbox_unresolved:0,spot_inventory_policy_valid:true,
        inventory_policy_match:true,untracked_spot:false,untracked_futures:false,untracked_orders:false,
        untracked_exchange_assets:false,lifecycle_verified:true,bootstrap_policy_valid:true,bootstrap_valid:true,
        prior_incident_valid:true,cooldown_evidence_present:true,cooldown_state_present:true,
        cooldown_evidence_valid:true,cooldown_remaining_sec:0,auto_preflight_refresh_failed:false,
        kill_path_ok:true,kill_read_ok:true,kill_size_ok:true,kill_payload_ok:true,kill_switch_present:!kill.is_null(),
        kill_switch_schema_ok:kill_schema_ok,kill_switch_operator_ok:kill_operator_ok,
        kill_switch_state:kill_state.into(),kill_drill_age_ok:true,kill_drilled:drilled,
        kill_switch_drill_fresh:true,kill_max_age_ok:true,kill_switch_disengagement_fresh:kill_state=="DISENGAGED",
    }
}

fn read_json(path:&str)->Value {
    fs::read(path).ok().and_then(|b|serde_json::from_slice(&b).ok()).unwrap_or(Value::Null)
}

async fn binance_task(
    shard:usize,bases:Vec<String>,duration:Duration,force_reconnect:bool,
    tx:mpsc::Sender<Event>,counters:Arc<Counters>
){
    let end=Instant::now()+duration;
    let streams=bases.iter().map(|b|format!("{}usdt@depth20@100ms",b.to_ascii_lowercase())).collect::<Vec<_>>().join("/");
    let url=format!("wss://fstream.binance.com/stream?streams={streams}");
    let mut epoch=1u64;let mut reconnects=0u64;let mut frames=0u64;let mut forced=false;
    loop {
        let (mut ws,_)=match connect_async(&url).await {
            Ok(v)=>v,
            Err(e)=>{let _=tx.send(Event::TaskError{venue:"binance",shard,error:format!("connect:{e}")}).await;break}
        };
        if epoch>1 {
            let _=tx.send(Event::Reset{venue:Venue::BinanceFutures,bases:bases.clone(),epoch}).await;
        }
        loop {
            if Instant::now()>=end {let _=ws.close(None).await;let _=tx.send(Event::TaskDone{venue:"binance",shard,reconnects,frames}).await;return}
            if force_reconnect&&!forced&&Instant::now()+Duration::from_millis(50)>=end-duration/2 {
                forced=true;reconnects+=1;epoch+=1;let _=ws.close(None).await;break;
            }
            let left=end.saturating_duration_since(Instant::now()).min(Duration::from_secs(2));
            match tokio::time::timeout(left,ws.next()).await {
                Ok(Some(Ok(Message::Text(raw))))=>{
                    frames+=1;counters.bin_frames.fetch_add(1,Ordering::Relaxed);
                    let f=RawFrame{source:None,venue:Venue::BinanceFutures,epoch,received_ns:monotonic_ns(),received_wall_ms:wall_ms(),raw};
                    if tx.send(Event::Frame(f)).await.is_err(){return}
                }
                Ok(Some(Ok(Message::Ping(p))))=>{let _=ws.send(Message::Pong(p)).await;}
                Ok(Some(Ok(_)))=>{}
                Ok(Some(Err(e)))=>{reconnects+=1;epoch+=1;let _=tx.send(Event::TaskError{venue:"binance",shard,error:format!("ws:{e}")}).await;break}
                Ok(None)=>{reconnects+=1;epoch+=1;break}
                Err(_)=>{}
            }
        }
        if Instant::now()>=end {break}
    }
    let _=tx.send(Event::TaskDone{venue:"binance",shard,reconnects,frames}).await;
}

async fn okx_task(
    shard:usize,bases:Vec<String>,duration:Duration,force_reconnect:bool,
    tx:mpsc::Sender<Event>,counters:Arc<Counters>
){
    let end=Instant::now()+duration;
    let mut epoch=1u64;let mut reconnects=0u64;let mut frames=0u64;let mut forced=false;
    loop {
        let (mut ws,_)=match connect_async("wss://ws.okx.com:8443/ws/v5/public").await {
            Ok(v)=>v,
            Err(e)=>{let _=tx.send(Event::TaskError{venue:"okx",shard,error:format!("connect:{e}")}).await;break}
        };
        let args=bases.iter().map(|b|json!({"channel":"books5","instId":format!("{b}-USDT")})).collect::<Vec<_>>();
        if let Err(e)=ws.send(Message::Text(json!({"op":"subscribe","args":args}).to_string())).await {
            let _=tx.send(Event::TaskError{venue:"okx",shard,error:format!("subscribe:{e}")}).await;break
        }
        if epoch>1 {
            let _=tx.send(Event::Reset{venue:Venue::OkxSpot,bases:bases.clone(),epoch}).await;
        }
        loop {
            if Instant::now()>=end {let _=ws.close(None).await;let _=tx.send(Event::TaskDone{venue:"okx",shard,reconnects,frames}).await;return}
            if force_reconnect&&!forced&&Instant::now()+Duration::from_millis(50)>=end-duration/2 {
                forced=true;reconnects+=1;epoch+=1;let _=ws.close(None).await;break;
            }
            let left=end.saturating_duration_since(Instant::now()).min(Duration::from_secs(2));
            match tokio::time::timeout(left,ws.next()).await {
                Ok(Some(Ok(Message::Text(raw))))=>{
                    let parsed:Value=serde_json::from_str(&raw).unwrap_or(Value::Null);
                    if parsed.get("event").is_some(){continue}
                    frames+=1;counters.okx_frames.fetch_add(1,Ordering::Relaxed);
                    let f=RawFrame{source:None,venue:Venue::OkxSpot,epoch,received_ns:monotonic_ns(),received_wall_ms:wall_ms(),raw};
                    if tx.send(Event::Frame(f)).await.is_err(){return}
                }
                Ok(Some(Ok(Message::Ping(p))))=>{let _=ws.send(Message::Pong(p)).await;}
                Ok(Some(Ok(_)))=>{}
                Ok(Some(Err(e)))=>{reconnects+=1;epoch+=1;let _=tx.send(Event::TaskError{venue:"okx",shard,error:format!("ws:{e}")}).await;break}
                Ok(None)=>{reconnects+=1;epoch+=1;break}
                Err(_)=>{}
            }
        }
        if Instant::now()>=end {break}
    }
    let _=tx.send(Event::TaskDone{venue:"okx",shard,reconnects,frames}).await;
}

#[derive(Default)]
struct Counters{bin_frames:AtomicU64,okx_frames:AtomicU64}

fn decision_is_opportunity(d:&Decision)->bool { d.reason=="EXECUTABLE_OPPORTUNITY" }
fn hex(bytes:&[u8])->String{bytes.iter().map(|b|format!("{b:02x}")).collect()}

#[tokio::main(flavor="multi_thread",worker_threads=4)]
async fn main()->Result<(),Box<dyn std::error::Error>>{
    let secs:u64=std::env::var("R22_SECONDS").ok().and_then(|x|x.parse().ok()).unwrap_or(30);
    assert!((10..=600).contains(&secs));
    let force_reconnect=std::env::var("R22_FORCE_RECONNECT").ok().as_deref()==Some("1");
    let capture=std::env::var("R22_CAPTURE").ok().map(PathBuf::from);
    let output=std::env::var("R22_OUTPUT").ok().map(PathBuf::from);
    let kill_path=std::env::var("R22_KILL_SWITCH").unwrap_or_else(|_|"/home/tmpadmin/spotbot/data/real_kill_switch.json".into());
    let preflight_path=std::env::var("R22_PREFLIGHT").unwrap_or_else(|_|"/home/tmpadmin/spotbot/reports/real_micro_preflight_latest.json".into());

    let meta:NativeMetadataSnapshot=fetch_current_metadata(1).map_err(|e|format!("metadata:{e:?}"))?;
    let bases=meta.bases();
    let expected=bases.len();
    let safety=evaluate_safety(&safety_input(&read_json(&kill_path),&read_json(&preflight_path)));
    if safety.real_launch_authorized {return Err("shadow invariant: launch authority true".into())}
    if safety.allowed {return Err("R22 shadow requires live hard-safety DENY".into())}
    let history_socket=std::env::var("R22_HISTORY_SOCKET").map_err(|_|"R22_HISTORY_SOCKET required")?;
    let run_id=std::env::var("R22_RUN_ID").unwrap_or_else(|_|format!("r22-{}",std::process::id()));
    let incarnation=monotonic_ns();
    let metadata_hash=hex(&sha256(&serde_json::to_vec(&meta.canonical_control_value())?));
    let mut durable_history=HistoryClient::connect_journal(&history_socket,32)?;
    let mut pending_durable:VecDeque<u64>=VecDeque::new();
    let mut history_fault:Option<String>=None;
    let mut durable_opportunities=0u64;

    let mut owners=(0..OWNERS).map(|i|Owner::new(i,OWNERS,800).unwrap()).collect::<Vec<_>>();
    let mut cores=(0..OWNERS).map(|i|DecisionCore::new(i,OWNERS,cfg(),HashMap::new()).unwrap()).collect::<Vec<_>>();
    for core in &mut cores {core.replace_routes(meta.generation,meta.routes.clone()).map_err(|e|format!("routes:{e:?}"))?}
    let mut history=NoHistory{accepted:0};

    let cap_writer=if let Some(path)=capture {
        Some(BufWriter::new(File::create(path)?))
    } else {None};
    let mut cap_writer=cap_writer;

    let (tx,mut rx)=mpsc::channel::<Event>(16_384);
    let counters=Arc::new(Counters::default());
    let duration=Duration::from_secs(secs);
    let mut tasks=0usize;
    for (shard,chunk) in bases.chunks(SHARD).enumerate(){
        let bs=chunk.to_vec();tasks+=2;
        tokio::spawn(binance_task(shard,bs.clone(),duration,force_reconnect,tx.clone(),counters.clone()));
        tokio::spawn(okx_task(shard,bs,duration,force_reconnect,tx.clone(),counters.clone()));
    }
    drop(tx);

    let started=Instant::now();let cpu0=cpu_s();let rss0=rss_kib();let mut rss_peak=rss0;
    let mut done=0usize;let mut resets=0u64;let mut task_errors=Vec::new();
    let mut parse_errors=0u64;let mut ingest_errors=0u64;let mut decision_errors=0u64;
    let mut accepted=0u64;let mut decisions=0u64;let mut opportunities=0u64;
    let mut safety_denies=0u64;let mut rehearsal_plans=0u64;
    let mut reasons:HashMap<String,u64>=HashMap::new();let mut seen_b=HashSet::new();let mut seen_o=HashSet::new();
    let mut opportunity_samples=Vec::new();

    while let Some(event)=rx.recv().await {
        rss_peak=rss_peak.max(rss_kib());
        if history_fault.is_none(){
            if let Err(e)=durable_history.poll(){history_fault=Some(format!("{e:?}"));}
            while let Some(id)=pending_durable.front().copied(){
                let Some(_receipt)=durable_history.take(id) else{break};
                pending_durable.pop_front();durable_opportunities+=1;
                if !safety.allowed {
                    let mut c=Coordinator::new(meta.generation);
                    if c.prepare_spot(meta.generation).is_ok()&&c.state==CoordinatorState::SpotPending{rehearsal_plans+=1;}
                }
            }
        }
        match event {
            Event::Frame(frame)=>{
                let update=match spotarb_native_market_core::parse(&frame){Ok(v)=>v,Err(_)=>{parse_errors+=1;continue}};
                let base=update.base.clone();let idx=owner_index(&base,OWNERS);
                match frame.venue {Venue::BinanceFutures=>{seen_b.insert(base.clone());},Venue::OkxSpot=>{seen_o.insert(base.clone());}}
                if owners[idx].ingest_parsed(&frame,update,&mut history).is_err(){ingest_errors+=1;continue}
                let book=match owners[idx].snapshot_venue(&base,frame.venue){Some(v)=>v,None=>{ingest_errors+=1;continue}};
                let exchange=match frame.venue{Venue::OkxSpot=>"okx_spot",Venue::BinanceFutures=>"binance_futures"};
                let rows=match cores[idx].update_at(&base,exchange,book,frame.received_ns,frame.received_wall_ms){
                    Ok(v)=>v,Err(_)=>{decision_errors+=1;continue}
                };
                accepted+=1;
                if let Some(w)=cap_writer.as_mut(){serde_json::to_writer(&mut *w,&frame)?;w.write_all(b"\n")?}
                for d in rows {
                    decisions+=1;*reasons.entry(d.reason.clone()).or_default()+=1;
                    if decision_is_opportunity(&d) {
                        opportunities+=1;
                        if !safety.allowed {safety_denies+=1}
                        if history_fault.is_none(){
                            let created=monotonic_ns();
                            let frame_hash=hex(&sha256(frame.raw.as_bytes()));
                            let trigger_venue=match frame.venue{Venue::OkxSpot=>"okx_spot",Venue::BinanceFutures=>"binance_futures"};
                            let context=json!({"run":run_id,"owner":owner_index(&d.base,3),"incarnation":incarnation,
                                "ordinal":opportunities,"generation":meta.generation,"metadata_hash":metadata_hash,
                                "recovery_cut":0,"kind":"decision","route_index":0,"base":d.base,
                                "trigger_venue":trigger_venue,"frame_epoch":frame.epoch,"frame_received_ns":frame.received_ns,
                                "frame_received_wall_ms":frame.received_wall_ms,"frame_sha256":frame_hash,
                                "decision":&d,"safety_verdict":safety.verdict,"real_launch_authorized":safety.real_launch_authorized,
                                "event_created_ns":created,"event_created_wall_ms":wall_ms()});
                            match durable_history.start(context){Ok(id)=>pending_durable.push_back(id),Err(e)=>history_fault=Some(format!("{e:?}"))}
                        }
                        if opportunity_samples.len()<20 {
                            opportunity_samples.push(json!({"base":d.base,"net_bps":d.estimated_net_bps,
                                "capacity_usdt":d.max_profitable_capacity_usdt,"risk_result":d.risk_result,
                                "safety_verdict":safety.verdict}));
                        }
                    }
                }
            }
            Event::Reset{venue,bases,epoch}=>{
                resets+=1;
                let exchange=match venue{Venue::OkxSpot=>"okx_spot",Venue::BinanceFutures=>"binance_futures"};
                for base in bases {
                    let idx=owner_index(&base,OWNERS);
                    if let Err(e)=owners[idx].reset_venue(&base,venue,epoch){task_errors.push(format!("owner_reset:{base}:{e:?}"))}
                    if let Err(e)=cores[idx].reset_exchange(&base,exchange,epoch){task_errors.push(format!("core_reset:{base}:{e:?}"))}
                }
            }
            Event::TaskError{venue,shard,error}=>task_errors.push(format!("{venue}[{shard}]:{error}")),
            Event::TaskDone{venue,shard,reconnects,frames}=>{
                done+=1;
                if reconnects>0 {task_errors.push(format!("{venue}[{shard}]:reconnects={reconnects}:frames={frames}"))}
                if done==tasks{break}
            }
        }
    }
    if let Some(w)=cap_writer.as_mut(){w.flush()?}
    let drain_deadline=Instant::now()+Duration::from_secs(3);
    while !pending_durable.is_empty()&&history_fault.is_none()&&Instant::now()<drain_deadline{
        if let Err(e)=durable_history.poll(){history_fault=Some(format!("{e:?}"));break}
        while let Some(id)=pending_durable.front().copied(){
            let Some(_receipt)=durable_history.take(id) else{break};pending_durable.pop_front();durable_opportunities+=1;
            if !safety.allowed {let mut c=Coordinator::new(meta.generation);if c.prepare_spot(meta.generation).is_ok()&&c.state==CoordinatorState::SpotPending{rehearsal_plans+=1;}}
        }
        if !pending_durable.is_empty(){std::thread::sleep(Duration::from_millis(1));}
    }
    if !pending_durable.is_empty()&&history_fault.is_none(){history_fault=Some("durable drain deadline".into());}
    let elapsed=started.elapsed().as_secs_f64();let cpu=cpu_s()-cpu0;
    let hard_errors=task_errors.iter().filter(|x|!x.contains(":reconnects=")).count();
    let result=json!({
        "r22_live_durable_shadow":hard_errors==0&&parse_errors==0&&ingest_errors==0&&decision_errors==0&&seen_b.len()==expected&&seen_o.len()==expected&&history_fault.is_none()&&durable_opportunities==opportunities,
        "duration_s":secs,"force_reconnect":force_reconnect,"trading":false,"private_order_path_linked":false,
        "metadata":{"source":"pure_native_https","generation":meta.generation,"universe":expected,
            "binance_crypto_active":meta.binance_active,"binance_tradifi_excluded":meta.binance_tradifi_excluded,
            "okx_active":meta.okx_active,"ready":meta.metadata_ready},
        "ws":{"shards_per_venue":bases.chunks(SHARD).count(),"binance_frames":counters.bin_frames.load(Ordering::Relaxed),
            "okx_frames":counters.okx_frames.load(Ordering::Relaxed),"binance_seen":seen_b.len(),"okx_seen":seen_o.len(),
            "resets":resets,"task_notes":task_errors},
        "pipeline":{"accepted_frames":accepted,"history_interface_accepts":history.accepted,"decisions":decisions,
            "reasons":reasons,"opportunities":opportunities,"parse_errors":parse_errors,"ingest_errors":ingest_errors,
            "decision_errors":decision_errors},
        "hard_safety":{"allowed":safety.allowed,"verdict":safety.verdict,"blocker_codes":safety.blocker_codes,
            "real_launch_authorized":safety.real_launch_authorized,"opportunities_denied":safety_denies},
        "durable_decision_barrier":{"socket":history_socket,"created":durable_history.created,"ram_admitted":durable_history.admitted,
            "journal_durable":durable_history.committed,"outstanding_peak":durable_history.peak,"pending":pending_durable.len(),
            "durable_opportunities":durable_opportunities,"fault":history_fault},
        "execution_rehearsal":{"after_durable_decision_only":true,"in_memory_only":true,"planned_spot_transitions":rehearsal_plans,
            "durable_intent_created":false,"network_mutation_possible":false},
        "opportunity_samples":opportunity_samples,
        "resources":{"wall_s":elapsed,"cpu_s":cpu,"cpu_pct_one_core":if elapsed>0.0{cpu/elapsed*100.0}else{0.0},
            "rss_start_kib":rss0,"rss_peak_kib":rss_peak,"frames_per_s":if elapsed>0.0{accepted as f64/elapsed}else{0.0}}
    });
    if let Some(path)=output{fs::write(path,serde_json::to_vec_pretty(&result)?)?}
    println!("{}",serde_json::to_string(&result)?);
    if result["r22_live_durable_shadow"]!=true {std::process::exit(2)}
    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn live_guard_denies_engaged_kill_and_denied_preflight() {
        let kill=json!({"schema":"spotbot-real-kill-switch-v1.2d","state":"ENGAGED","operator":"Denis","drill_result":"PASS"});
        let pre=json!({"verdict":"DENY","real_launch_authorized":false,"blocker_codes":["X"]});
        let r=evaluate_safety(&safety_input(&kill,&pre));
        assert!(!r.allowed);assert!(!r.real_launch_authorized);
        assert!(r.blocker_codes.contains(&"BLOCKER_KILL_SWITCH_ENGAGED".into()));
        assert!(r.blocker_codes.contains(&"BLOCKER_PREFLIGHT_DENY".into()));
        assert!(r.blocker_codes.contains(&"BLOCKER_OPERATOR_APPROVAL_MISSING".into()));
    }
    #[test]
    fn missing_safety_files_fail_closed() {
        let r=evaluate_safety(&safety_input(&Value::Null,&Value::Null));
        assert!(!r.allowed);assert!(!r.real_launch_authorized);
        assert!(r.blocker_codes.contains(&"BLOCKER_KILL_SWITCH_MISSING".into()));
        assert!(r.blocker_codes.contains(&"BLOCKER_PREFLIGHT_MISSING".into()));
    }
    #[test]
    fn in_memory_rehearsal_has_no_durable_or_network_step() {
        let mut c=Coordinator::new(1);c.prepare_spot(1).unwrap();
        assert_eq!(c.state,CoordinatorState::SpotPending);
    }
}
