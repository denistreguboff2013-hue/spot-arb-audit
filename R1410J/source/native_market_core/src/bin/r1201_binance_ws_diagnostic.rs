#![recursion_limit = "256"]
//! R1195 bounded PUBLIC-only source demonstration. No PAPER/REAL, databases or orders.
#[path = "../quality_manager_r1195.rs"]
mod quality;
use quality::{Manager,History};
use std::{path::{Path,PathBuf},time::{Duration,Instant,SystemTime,UNIX_EPOCH},fs};
use futures_util::{StreamExt,SinkExt};
use tokio_tungstenite::{connect_async,tungstenite::Message,MaybeTlsStream};
use spotarb_native_market_core::native_binance_ws_ip_guard_r1110::{r1110_reserve_at_lock,R1110Outcome};
const GUARD:&str="/mnt/spotarb_db_test/astra_paper_continuous_v1/r1110_binance_ws_ip_guard.lock";
const KILL:&str="/home/tmpadmin/spotbot/data/real_kill_switch.json";
const SYMBOLS:[&str;8]=["SEIUSDT","SENTUSDT","SKLUSDT","SKYUSDT","SLPUSDT","SLXUSDT","SNXUSDT","SOLUSDT"];
fn utc()->Result<i64,String> {
 let d=SystemTime::now().duration_since(UNIX_EPOCH).map_err(|e|e.to_string())?;
 i64::try_from(d.as_millis()).map_err(|_|"timestamp overflow".into())
}
fn safety()->Result<(),String> {
 if std::env::var("ASTRA_R1201_SOURCE_ONLY").ok().as_deref()!=Some("YES") {
     return Err("R1199 explicit source-only opt-in required".into())
 }
 let value:serde_json::Value=serde_json::from_slice(&fs::read(KILL).map_err(|e|e.to_string())?)
     .map_err(|e|e.to_string())?;
 if value["state"]!="ENGAGED" {return Err("REAL kill is not ENGAGED".into())}
 Ok(())
}
async fn permit(deadline:Instant)->Result<(),String>{
 loop{
  let outcome=tokio::task::spawn_blocking(||r1110_reserve_at_lock(Path::new(GUARD)))
   .await.map_err(|e|e.to_string())?.map_err(|e|format!("guard:{e:?}"))?;
  match outcome{
   R1110Outcome::Granted=>return Ok(()),
   R1110Outcome::WaitMs(t)=>{
     if Instant::now()+Duration::from_millis(t)>=deadline {return Err("quota deadline".into());}
     tokio::time::sleep(Duration::from_millis(t)).await;
   }
  }
 }
}
enum Incoming { Connected(u64,String,String), Data(u64,String,i64,u64,u64,u64), Paused(u64,u64), Done(u64,String) }
async fn reader(id:u64,start:Instant,end:Instant,tx:tokio::sync::mpsc::Sender<Incoming>, mut cancel:tokio::sync::watch::Receiver<bool>)->Result<(),String>{
 permit(end).await?;
 let query=SYMBOLS.iter().map(|s|format!("{}@depth20@500ms",s.to_ascii_lowercase())).collect::<Vec<_>>().join("/");
 let url=format!("wss://fstream.binance.com/public/stream?streams={query}");
 let (mut ws,_)=tokio::time::timeout(Duration::from_secs(12),connect_async(url.as_str()))
    .await.map_err(|_|"connect timeout".to_string())?.map_err(|e|e.to_string())?;
 let peer=match ws.get_ref(){
  MaybeTlsStream::NativeTls(tls)=>tls.get_ref().get_ref().get_ref().peer_addr()
    .map(|a|a.to_string()).unwrap_or_else(|_|"unknown".into()),
  MaybeTlsStream::Plain(tcp)=>tcp.peer_addr().map(|a|a.to_string()).unwrap_or_else(|_|"unknown".into()),
  _=>"unavailable".into()
 };
 let local=match ws.get_ref(){
  MaybeTlsStream::NativeTls(tls)=>tls.get_ref().get_ref().get_ref().local_addr()
    .map(|a|a.to_string()).unwrap_or_else(|_|"unknown".into()),
  MaybeTlsStream::Plain(tcp)=>tcp.local_addr().map(|a|a.to_string()).unwrap_or_else(|_|"unknown".into()),
  _=>"unavailable".into()
 };
 let fd=match ws.get_ref(){
  MaybeTlsStream::NativeTls(tls)=>{use std::os::fd::AsRawFd;tls.get_ref().get_ref().get_ref().as_raw_fd()},
  _=>-1
 };
 println!("R1201_SOCKET {}",serde_json::json!({"id":id,"peer":peer,"local":local,"fd":fd,"mono_ms":start.elapsed().as_millis(),"utc_ms":utc()?,"epoch":1,"symbols":SYMBOLS}));
 tx.send(Incoming::Connected(id,peer,local)).await.map_err(|e|e.to_string())?;
 let mut injected_read=false;
 let mut prev_return=Instant::now();
 let reason=loop{
  if Instant::now()>=end {break "normal_bound_end".to_string();}
  let timeout=end.saturating_duration_since(Instant::now()).min(Duration::from_secs(2));
  
  if id==7 && !injected_read && start.elapsed().as_millis()>=1240000 {
   injected_read=true;
   tx.send(Incoming::Paused(id,start.elapsed().as_millis() as u64)).await.map_err(|e|e.to_string())?;
   tokio::time::sleep(Duration::from_secs(8)).await;
  }
  let gap_us=prev_return.elapsed().as_micros() as u64;
  let poll_start=Instant::now();
  let next=tokio::select! { biased;
   result=cancel.changed()=>{
    if result.is_err(){break "local_control_dropped".to_string();}
    if *cancel.borrow(){
      // Keep TCP/TLS socket OPEN; temporarily stop polling this reader.
      let paused_at=start.elapsed().as_millis() as u64;
      tx.try_send(Incoming::Paused(id,paused_at)).map_err(|_|"pause event queue full".to_string())?;
      tokio::time::sleep(Duration::from_secs(8)).await;
      continue;
    }
    continue;
   },
   result=tokio::time::timeout(timeout,ws.next())=>result,
  };
  let wait_us=poll_start.elapsed().as_micros() as u64;
 prev_return=Instant::now();
 match next{
   Ok(Some(Ok(Message::Text(raw))))=>{
    let t=utc()?;let m=start.elapsed().as_millis() as u64;
    if tx.try_send(Incoming::Data(id,raw,t,m,gap_us,wait_us)).is_err(){
      break "bounded_telemetry_queue_pressure".to_string();
    }
   },
   Ok(Some(Ok(Message::Ping(_))))=>{let _=ws.flush().await;},
   Ok(Some(Ok(Message::Close(_))))=>break "remote_close".to_string(),
   Ok(Some(Err(e)))=>break format!("ws:{e}"),
   Ok(None)=>break "stream_closed".to_string(),
   Err(_)=>continue,
   _=>{}
  }
 };
 let _=tokio::time::timeout(Duration::from_millis(400),ws.close(None)).await;
 drop(ws); // Actual transport released before signaling Done to selection manager.
 let _=tx.send(Incoming::Done(id,reason)).await;
 Ok(())
}
fn valid_depth(v:&serde_json::Value)->bool {
 let d=&v["data"];
 let levels=|key:&str| d[key].as_array().map(|a| {
   !a.is_empty() && a.iter().all(|p|p.as_array().is_some_and(|v| {
     v.len()==2 && v.iter().all(|x|x.as_str()
       .and_then(|s|s.parse::<f64>().ok()).is_some_and(|n|n.is_finite()&&n>0.0))
   }))
 }).unwrap_or(false);
 levels("b") && levels("a")
}

fn age_summary(x:Option<&std::collections::VecDeque<(u64,i64)>>)->serde_json::Value {
 let mut a=x.map(|v|v.iter().map(|(_,age)|*age).collect::<Vec<_>>()).unwrap_or_default();
 a.sort_unstable();
 let p=|q:usize|a.get((a.len().saturating_sub(1))*q/100).copied();
 serde_json::json!({"sample_count":a.len(),"p50":p(50),"p95":p(95),"p99":p(99),"max":a.last()})
}
#[tokio::main(flavor="multi_thread",worker_threads=2)]
async fn main()->Result<(),String>{
 safety()?;
 let dir=PathBuf::from(std::env::args().nth(1).ok_or("scratch path required")?)
    .canonicalize().map_err(|e|e.to_string())?;
 if !dir.to_string_lossy().starts_with(
  "/mnt/spotarb_db_test/astra_p001_r874_test_scratch_20261025/r1201_root_cause_"){
  return Err("output must be approved isolated scratch".into())
 }
 const N:usize=8;
 let mut manager=Manager::default();
 manager.define_group("g",SYMBOLS.iter().map(|s|s.to_string()).collect())?;
 let history_path=dir.join("quality_history.json");
 manager.history=History::load(&history_path,utc()?)?;
 let start=Instant::now();
 let injection=std::env::var("ASTRA_R1199_INJECT_PAUSE").ok().as_deref()==Some("YES");
 if injection{return Err("R1200 forbids injection".into())}
 let end=start+Duration::from_secs(1320);
 let (tx,mut rx)=tokio::sync::mpsc::channel::<Incoming>(4096);
 let mut stops=std::collections::HashMap::<u64,tokio::sync::watch::Sender<bool>>::new();
 for id in 1..=(N as u64){
  let central=tx.clone();
  let (task_tx,mut relay_rx)=tokio::sync::mpsc::channel::<Incoming>(512);
  tokio::spawn(async move {
   let mut delayed=false;
   while let Some(msg)=relay_rx.recv().await {
    if id==8 && !delayed && start.elapsed().as_millis()>=1280000 {
     delayed=true;
     tokio::time::sleep(Duration::from_secs(8)).await;
    }
    if central.send(msg).await.is_err(){break}
   }
  });
  let (stop_tx,stop_rx)=tokio::sync::watch::channel(false);
  stops.insert(id,stop_tx);
  tokio::spawn(async move{
   if let Err(e)=reader(id,start,end,task_tx.clone(),stop_rx).await {
    let _=task_tx.send(Incoming::Done(id,format!("reader_error:{e}"))).await;
   }
  });
 }
 let queue_observer=tx.downgrade();
 drop(tx);
 let mut first=None::<(u64,u64,u64)>; // id, epoch, monotonic timestamp
 let mut attempted=false;
 let mut pause_ack_mono=None::<u64>;
 let mut paused_source=None::<u64>;
 let mut watchdog_switch_pre=0u64;
 let mut switch_evidence=Vec::<serde_json::Value>::new();
 let mut event_copies=std::collections::BTreeMap::<(String,i64,u64),[Option<i64>;8]>::new();
 let mut metrics=std::collections::BTreeMap::<(u64,u64),Vec<(i64,u64,u64,u64,u64)>>::new();
 let mut incident_samples=Vec::<serde_json::Value>::new();
 let mut controls=Vec::<serde_json::Value>::new();
 let mut sockets=std::collections::HashMap::<u64,serde_json::Value>::new();
 let mut age_window=std::collections::HashMap::<u64,std::collections::VecDeque<(u64,i64)>>::new();
 let mut watchdog_switch_post=0u64;
 let mut watchdog_blocked=0u64;
 let mut watchdog_waiting=0u64;
 let mut max_silent_ms_without_corrob=0u64;
 let mut max_quorum=0usize;
 let mut watchdog_ticks=0u64;
 let mut switched_paused_source=false;
 let mut published_event_watermarks=std::collections::HashMap::<String,i64>::new();
 let mut published_duplicate_or_regression=0u64;
 let mut forced_mono=None::<u64>;
 let mut ready_at_failure=0usize;
 let mut failover=None::<(u64,u64,u64)>; // id, epoch, mono time
 let mut pool_frames=[0u64;N];
 let mut pool_late=[0u64;N];
 let mut published=0u64;
 let mut published_after=0u64;
 let mut blocked_due_to_bad=0u64;
 let mut errors=0u64;
 let mut done=Vec::<String>::new();
 let mut history_saves=0u64;
 let mut last_history_mono=0u64;
 let mut epochs=Vec::<u64>::new();
 let mut max_ready_before=0usize;
 let mut promotions_failed=0u64;
 while Instant::now()<end {
  let m=start.elapsed().as_millis() as u64;
  let now=utc()?;
  // Small, bounded history persistence. The write is NOT a per-frame operation.
  if m>=last_history_mono+30_000 {
   last_history_mono=m;
   for id in 1..=(N as u64){
    if manager.sessions.contains_key(&id) {manager.record_quality(id,now)?;}
   }
   manager.history.save_atomic(&history_path)?;
   history_saves+=1;
  }
  let ready=manager.sessions.values().filter(|s|{
     s.tier==quality::Tier::Hot && manager.suggest_ready_reserve("g",now,m)==Some(s.id)
  }).count();
  max_ready_before=max_ready_before.max(ready);
  if first.is_none() && m>=30_000 {
   if let Some(id)=manager.suggest_ready_reserve("g",now,m){
    if let Ok(epoch)=manager.promote("g",id,now,m){
     first=Some((id,epoch,m));epochs.push(epoch);
    }
   }
  }
  // A deterministic fault time, independent of reserve condition:
  // no valid reserve => stay FAIL_CLOSED; never 'fake' success.
  if injection && !attempted && m>=100_000 {
   attempted=true;
   if let Some(primary)=manager.active.get("g").map(|a|a.session_id).filter(|x|*x>0) {
    forced_mono=Some(m);
    paused_source=Some(primary);
    let mut n=0;
    for candidate in 1..=(N as u64){
     if candidate==primary {continue}
     if manager.sessions.get(&candidate).is_some_and(|s|
          s.tier==quality::Tier::Hot && s.symbols.iter().all(|sym|
          s.quotes.get(sym).is_some_and(|q|
          now>=q.event_ms && now-q.event_ms<=800 &&
          m>=q.receive_mono_ms && m-q.receive_mono_ms<=800))) {
      n+=1;
     }
    }
    ready_at_failure=n;
    stops.get(&primary).ok_or("missing socket control")?.send(true).map_err(|_|"socket reader stopped".to_string())?;
    // Reader stops POLLING, keeps its socket OPEN for 8s.
   }
  }
  if first.is_some() {
   watchdog_ticks+=1;
   let before=manager.active.get("g")
      .and_then(|a|manager.sessions.get(&a.session_id))
      .map(|s|{
         let silence=s.seen.values().copied().max().map(|t|m.saturating_sub(t));
         (s.id, s.tier,silence,s.recent_bad_fraction(),s.samples.len(),
          s.quotes.iter().map(|(k,v)|(k.clone(),v.event_ms)).collect::<std::collections::HashMap<_,_>>())
      });
   match manager.watchdog_correlated_r1199("g",now,m)?{
    quality::WatchdogOutcome::Switched{old_id,new_id,new_epoch}=>{
      if let Some((orig,tier,silent_ms,bad_rate,n,old_e))=before {
       switch_evidence.push(serde_json::json!({
         "mono_ms":m,"old_id":old_id,"new_id":new_id,
         "source_epoch":new_epoch,"reason":if tier!=quality::Tier::Active{"suspect_or_invalid"}else{"session_silent"},
         "prior_tier":format!("{tier:?}"),"source_id_confirmed":orig,
         "source_silent_ms":silent_ms,"source_recent_bad_fraction":bad_rate,
         "source_recent_frames":n,"injection_started":attempted,
         "old_socket":sockets.get(&old_id),"new_socket":sockets.get(&new_id),
         "old_ages_ms":age_summary(age_window.get(&old_id)),
         "new_ages_ms":age_summary(age_window.get(&new_id)),
         "old_valid_e_ms":old_e,
         "new_valid_e_ms":manager.sessions.get(&new_id).map(|s|s.quotes.iter().map(|(k,v)|(k.clone(),v.event_ms)).collect::<std::collections::HashMap<_,_>>()),
         "rx_queue_remaining":queue_observer.upgrade().map(|tx|tx.capacity()),
         "rx_queue_depth":queue_observer.upgrade().map(|tx|4096-tx.capacity()),
         "quorum_peers":manager.sessions.values().filter(|s|s.tier==quality::Tier::Hot).map(|s|s.peer.clone()).collect::<std::collections::HashSet<_>>().len()
       }));
      }
      epochs.push(new_epoch);
      if attempted {watchdog_switch_post+=1;}else{watchdog_switch_pre+=1;}
      if paused_source==Some(old_id) && failover.is_none() {
       switched_paused_source=true;
       failover=Some((new_id,new_epoch,m));
      }
    },
    quality::WatchdogOutcome::Blocked{..}=>watchdog_blocked+=1,
    quality::WatchdogOutcome::WaitingForEvidence{silent_ms,qualified_backups,..}=>{
      watchdog_waiting+=1;
      max_silent_ms_without_corrob=max_silent_ms_without_corrob.max(silent_ms);
      max_quorum=max_quorum.max(qualified_backups);
    },
    _=>{}
   }
  }
  match tokio::time::timeout(Duration::from_millis(25),rx.recv()).await {
   Ok(Some(Incoming::Connected(id,peer,local)))=>{
    sockets.insert(id,serde_json::json!({"remote":peer,"local":local,"connected_mono_ms":m,"connected_utc_ms":now}));
    if manager.add_session(id,&peer,"g").is_err(){errors+=1}
   },
   Ok(Some(Incoming::Data(id,raw,recv_utc,recv_mono,gap_us,wait_us)))=>{
    let dequeue=start.elapsed().as_millis() as u64;
    let queue_ms=dequeue.saturating_sub(recv_mono);
    let parse_start=Instant::now();
    let idx=(id-1) as usize;
    pool_frames[idx]+=1;
    let ages=age_window.entry(id).or_default();
    if let Ok(v)=serde_json::from_str::<serde_json::Value>(&raw){
     let sym=v["data"]["s"].as_str().unwrap_or("").to_ascii_uppercase();
     let event_ms=v["data"]["E"].as_i64().unwrap_or(0);
     let seq=v["data"]["u"].as_u64().or_else(||v["data"]["lastUpdateId"].as_u64());
     let good=valid_depth(&v) && event_ms!=0;
     let parse_us=parse_start.elapsed().as_micros() as u64;
     metrics.entry((id,recv_mono/10000)).or_default().push((recv_utc-event_ms,queue_ms,gap_us,wait_us,parse_us));
     if recv_mono<1200000 {
      event_copies.entry((sym.clone(),event_ms,seq.unwrap_or(0))).or_insert([None;8])[(id-1) as usize]=Some(recv_utc);
     }
     if (recv_utc-event_ms>3000 || queue_ms>1000) && incident_samples.len()<120 {
      incident_samples.push(serde_json::json!({"id":id,"mono_ms":recv_mono,"E":event_ms,"u":seq,"symbol":sym,"age_ms":recv_utc-event_ms,"queue_ms":queue_ms,"gap_us":gap_us,"wait_us":wait_us,"parse_us":parse_us}));
     }
     if recv_utc.saturating_sub(event_ms)>800{pool_late[idx]+=1;}
     ages.push_back((recv_mono,recv_utc.saturating_sub(event_ms)));
     while ages.len()>2048 || ages.front().is_some_and(|(t,_)|recv_mono.saturating_sub(*t)>60_000){ages.pop_front();}
     let accepted=match manager.observe(id,&sym,event_ms,recv_utc,recv_mono,seq,good){
      Ok(ok)=>ok,
      Err(_)=>{errors+=1;false}
     };
     if first.is_none() && recv_mono>=30_000 {
      if let Some(best)=manager.suggest_ready_reserve("g",utc()?,start.elapsed().as_millis() as u64){
       if let Ok(epoch)=manager.promote("g",best,utc()?,start.elapsed().as_millis() as u64){
        first=Some((best,epoch,recv_mono));epochs.push(epoch);
       }
      }
     }
     // NEVER publish a standby-origin message: only the selected live session.
     if manager.active.get("g").is_some_and(|sel|sel.session_id==id) && accepted {
       if let Some(item)=publish_after_queue(&mut manager,&sym,recv_utc,recv_mono,utc()?,start.elapsed().as_millis() as u64){
        if published_event_watermarks.get(&item.symbol).is_some_and(|old|item.event_ms<=*old){published_duplicate_or_regression+=1;}
        published_event_watermarks.insert(item.symbol,item.event_ms);
        published+=1;
        if forced_mono.is_some(){published_after+=1;}
       }else{blocked_due_to_bad+=1;}
     }else if id==first.map(|x|x.0).unwrap_or(0) && attempted {
       blocked_due_to_bad+=1;
     }
    }else{errors+=1}
   },
   Ok(Some(Incoming::Paused(id,reader_mono)))=>{
    controls.push(serde_json::json!({"session":id,"reader_pause_mono_ms":reader_mono,"duration_ms":8000,"synthetic":true}));
    if paused_source==Some(id){pause_ack_mono=Some(reader_mono);}
   },
   Ok(Some(Incoming::Done(id,why)))=>{
    done.push(format!("session{id}:{why}"));
    if manager.sessions.contains_key(&id) {
     manager.invalidate(id)?;
    }
   },
   Ok(None)=>break,
   Err(_)=>{}
  }
 }
 let now=utc()?;
 for id in 1..=(N as u64){
  if manager.sessions.contains_key(&id) {manager.record_quality(id,now)?;}
 }
 manager.history.save_atomic(&history_path)?;
 history_saves+=1;
 let reread=History::load(&history_path,now)?;
 let mut minute_metrics=Vec::new();
 for ((id,slot),values) in metrics {
  let quant=|mut xs:Vec<i64>| {
   xs.sort_unstable();let n=xs.len();
   serde_json::json!({"n":n,"p50":xs[(n-1)/2],"p95":xs[(n-1)*95/100],"p99":xs[(n-1)*99/100],"max":xs[n-1]})
  };
  minute_metrics.push(serde_json::json!({"id":id,"slot_10s":slot,
   "age_ms":quant(values.iter().map(|x|x.0).collect()),
   "queue_ms":quant(values.iter().map(|x|x.1 as i64).collect()),
   "outside_poll_us":quant(values.iter().map(|x|x.2 as i64).collect()),
   "await_ws_us":quant(values.iter().map(|x|x.3 as i64).collect()),
   "parse_us":quant(values.iter().map(|x|x.4 as i64).collect()),
   "gt800":values.iter().filter(|x|x.0>800).count(),"gt3000":values.iter().filter(|x|x.0>3000).count()}));
 }
 let mut completeness=[0u64;9];let mut full=0u64;let mut mixed=0u64;let mut all_stale=0u64;let mut max_skew=0i64;
 let mut examples=Vec::new();
 for ((sym,e,u),times) in &event_copies {
  let n=times.iter().filter(|x|x.is_some()).count();completeness[n]+=1;
  if n!=8{continue} full+=1;
  let ages:Vec<i64>=times.iter().map(|t|t.unwrap()-e).collect();
  let lo=*ages.iter().min().unwrap();let hi=*ages.iter().max().unwrap();
  if lo<=800 && hi>800{mixed+=1} if lo>800{all_stale+=1}
  max_skew=max_skew.max(hi-lo);
  if hi-lo>3000 && examples.len()<24 {examples.push(serde_json::json!({"symbol":sym,"E":e,"u":u,"ages_ms":ages}));}
 }
 let response=serde_json::json!({
  "natural_window_mono_ms":[0,1200000],"controls_window_mono_ms":[1200000,1320000],
  "control_read_pause":controls,"control_parse_queue":{"id":8,"at_mono_ms":1280000,"duration_ms":8000,"synthetic":true,"reader_continues":true},
  "metrics_10s":minute_metrics,"incident_samples_bounded":incident_samples,
  "matching_natural":{"unique":event_copies.len(),"completeness_n_of8":completeness,"matched_all8":full,"mixed_fresh_stale":mixed,"all8_stale":all_stale,"max_same_event_skew_ms":max_skew,"examples":examples},

  "schema":"SPOTARB/R1201/DIAGNOSTIC/V1",
  "source":"Binance public futures depth20@500ms","mode":"source_only",
  "real_orders":0,"paper_orders":0,"trading_db_writes":0,
  "symbols":SYMBOLS,"sessions_requested":N,"bounded_duration_s":1320,"sockets":sockets,
  "frames_received_by_session":pool_frames,
  "aged_gt800_by_session":pool_late,
  "first_active":first.map(|(id,ep,t)|serde_json::json!({"id":id,"epoch":ep,"at_mono_ms":t})),
  "injection_enabled":injection,
  "reader_pause_requested_mono_ms":if injection{Some(100000_u64)}else{None},
  "reader_pause_ack_mono_ms":pause_ack_mono,
  "paused_source_id":paused_source,
  "watchdog_ticks":watchdog_ticks,
  "switch_evidence":switch_evidence,
  "watchdog_switches_before_fault":watchdog_switch_pre,
  "watchdog_switches_after_fault":watchdog_switch_post,
  "watchdog_blocked":watchdog_blocked,
  "watchdog_waiting_for_evidence_ticks":watchdog_waiting,
  "max_silent_ms_without_corroboration":max_silent_ms_without_corrob,
  "max_partial_quorum":max_quorum,
  "paused_source_switched_by_watchdog":switched_paused_source,
  "publisher_duplicate_or_event_regression":published_duplicate_or_regression,
  "actual_fault_mono_ms":forced_mono,
  "ready_hot_reserves_at_fault":ready_at_failure,
  "best_backup_promoted":failover.map(|(id,ep,t)|serde_json::json!({"id":id,"epoch":ep,"at_mono_ms":t})),
  "pause_to_promotion_ms":match (pause_ack_mono,failover){(Some(t),Some((_,_,f)))=>Some(f.saturating_sub(t)),_=>None},
  "fault_to_promotion_ms":match (forced_mono,failover){
   (Some(t),Some((_,_,f)))=>Some(f.saturating_sub(t)),_=>None},
  "epochs":epochs,"published_total":published,
  "published_post_fault":published_after,
  "blocked_on_stale_or_invalid":blocked_due_to_bad,
  "errors":errors,"promotions_failed":promotions_failed,
  "max_ready_control":max_ready_before,
  "completed_sources":done,
  "history_saves":history_saves,
  "history_reloaded_peer_records":reread.peers.len(),
  "history_profiles":reread.peers,
  "limitations":"Correlated session policy under pilot thresholds; if injection=true, locally pauses reader for 8s while TCP stays open. NOT spontaneous Binance stall or proven production SLA; no DB1/DB2/ACK or 233-symbol certification."
 });
 let out=dir.join("r1201_diagnostic_report.json");
 let bytes=serde_json::to_vec_pretty(&response).map_err(|e|e.to_string())?;
 fs::write(&out,&bytes).map_err(|e|e.to_string())?;
 println!("R1200_REPORT {}",out.display());
 println!("R1200_OUTCOME {}",serde_json::json!({
   "first_active":response["first_active"],
   "ready_hot_reserves_at_fault":ready_at_failure,
   "best_backup_promoted":response["best_backup_promoted"],
   "fault_to_promotion_ms":response["fault_to_promotion_ms"],
   "pause_to_promotion_ms":response["pause_to_promotion_ms"],
   "paused_source_switched_by_watchdog":switched_paused_source,
   "pre_fault_watchdog_switches":watchdog_switch_pre,
   "post_fault_watchdog_switches":watchdog_switch_post,
   "waiting_ticks":watchdog_waiting,
   "injection_enabled":injection,
   "publisher_duplicate_or_event_regression":published_duplicate_or_regression,
   "received_frames":pool_frames,"late_frames":pool_late,
   "published_after_fault":published_after,"history_saves":history_saves,
   "errors":errors
 }));
 Ok(())
}

/// Publication is a dequeue-time admission; receive clocks remain provenance only.
fn publish_after_queue(manager:&mut Manager,symbol:&str,recv_utc:i64,recv_mono:u64,now_utc:i64,now_mono:u64)->Option<quality::Publish>{
    if now_utc<recv_utc || now_mono<recv_mono ||
       now_utc-recv_utc>800 || now_mono-recv_mono>800 {return None}
    manager.publish("g",symbol,now_utc,now_mono)
}
#[cfg(test)]
mod r1201_queue_tests {
 use super::*;
 fn primed()->Manager{
  let mut m=Manager::default();m.define_group("g",vec!["SOLUSDT".into()]).unwrap();m.add_session(1,"peer","g").unwrap();
  let base=1_800_000_000_000_i64;
  for (seq,t) in [0_u64,500,1000,1500].iter().enumerate(){
   assert!(m.observe(1,"SOLUSDT",base+*t as i64-100,base+*t as i64,*t,Some(seq as u64+1),true).unwrap());
  }
  m.promote("g",1,base+1550,1550).unwrap();m
 }
 #[test] fn delayed_queue_never_reuses_fresh_receive_time(){
  let b=1_800_000_000_000_i64;let mut m=primed();
  assert!(publish_after_queue(&mut m,"SOLUSDT",b+1500,1500,b+9500,9500).is_none());
 }
 #[test] fn short_queue_still_admits_fresh_snapshot(){
  let b=1_800_000_000_000_i64;let mut m=primed();
  assert!(publish_after_queue(&mut m,"SOLUSDT",b+1500,1500,b+1600,1600).is_some());
 }
}
