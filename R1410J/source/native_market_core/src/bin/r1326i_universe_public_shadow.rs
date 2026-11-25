use serde_json::{json,Value};
use spotarb_native_market_core::{
    ingress_runtime_r1205::{run_r1246_volatile,Config,Summary,mono_ns,wall_ms},
    market_data_ingress_r1205::{Authority,Output},
};
use std::{collections::HashMap,fs,path::PathBuf,sync::{Arc,RwLock},time::Duration};
use tokio::sync::mpsc;

fn scrub_capture_environment()->Result<(),String>{
    std::env::remove_var("R1268_CAPTURE");
    std::env::remove_var("R1268_CAPTURE_DIR");
    if std::env::var_os("R1268_CAPTURE").is_some()||std::env::var_os("R1268_CAPTURE_DIR").is_some(){
        return Err("R1326E_CAPTURE_ENV_NOT_ISOLATED".into())
    }
    Ok(())
}
fn percentile(samples:&[u64])->Value{
    if samples.is_empty(){return json!({"samples":0})}
    let mut sorted=samples.to_vec();sorted.sort_unstable();
    let q=|p:usize|sorted[((sorted.len()-1)*p+99)/100];
    json!({"samples":sorted.len(),"p50":q(50),"p95":q(95),"p99":q(99)})
}
fn readiness_row(v:&Value)->Value{
    fn source(v:&Value)->Value{json!({
        "scope":v["scope"],"session":v["session"],"generation":v["generation"],
        "selected":v["selected"],"failed_predicates":v["failed_predicates"],
        "manager_state":v["manager_state"],"manager_subscribed":v["manager_subscribed"],
        "manager_good_samples":v["manager_good_samples"],"manager_errors":v["manager_errors"],
        "manager_last_receive_age_ms":v["manager_last_receive_age_ms"],
        "manager_last_valid_exchange_age_ms":v["manager_last_valid_exchange_age_ms"],
        "book_state":v["book_state"],"book_fresh":v["book_fresh"],"book_exchange_age_ms":v["book_exchange_age_ms"]
    })}
    json!({"kind":v["kind"],"mono_ns":v["mono_ns"],"scope":v["scope"],"session":v["session"],
        "ready_sources":v["ready_sources"],"source_count":v["source_count"],
        "lost_predicates":v["lost_predicates"],"gained_predicates":v["gained_predicates"],
        "snapshot":if v["snapshot"].is_object(){source(&v["snapshot"])}else{Value::Null},
        "before":if v["before"].is_object(){source(&v["before"])}else{Value::Null},
        "after":if v["after"].is_object(){source(&v["after"])}else{Value::Null},
        "sources":v["sources"].as_array().map(|a|a.iter().map(source).collect::<Vec<_>>())})
}
fn audit_counts(rows:&[Value],field:&str)->Value{
    let mut counts=std::collections::BTreeMap::<String,u64>::new();
    for row in rows{if let Some(value)=row[field].as_str(){*counts.entry(value.to_owned()).or_default()+=1}}
    json!(counts)
}
fn compact(summary:&Summary)->Value{
    let sockets=summary.sockets.iter().map(|s|json!({
        "session":s.session,"venue":s.venue,"url":s.url,"connection_epoch":s.connection_epoch,
        "local":s.local,"peer":s.peer,"dns_ips":s.dns_ips,
        "established_ns":s.established_ns,"received":s.received,"stale":s.stale,"connects":s.connects,"end":s.end,
        "reader_queue_depth_max":s.reader_queue_depth_max,"reader_queue_overflow":s.reader_queue_overflow,
        "r1382_reader_classify":&s.r1382_reader_classify,
        "tcp_sample_count":s.tcp_samples.len(),
        "tcp_recvq_max_bytes":s.tcp_samples.iter().filter_map(|v|v["inq_bytes"].as_i64()).max(),
        "tcp_rtt_max_us":s.tcp_samples.iter().filter_map(|v|v["rtt_us"].as_u64()).max(),
        "age_ms":percentile(&s.age_samples.iter().filter_map(|v|u64::try_from(*v).ok()).collect::<Vec<_>>()),
        "queue_send_wait_ns":percentile(&s.queue_send_wait_ns),
        "reader_queue_push_ns":percentile(&s.reader_queue_push_ns)
    })).collect::<Vec<_>>();
    json!({
        "sockets":sockets,"counts":summary.counts,"coverage_time":summary.coverage_time,
        "diagnostics":{
            "readiness_audit_total":summary.readiness_audit.len(),
            "readiness_audit_kinds":audit_counts(&summary.readiness_audit,"kind"),
            "readiness_audit_tail":summary.readiness_audit.iter().rev().take(12).map(readiness_row).collect::<Vec<_>>(),
            "selection_audit_total":summary.selection_audit.len(),
            "selection_audit_tail":summary.selection_audit.iter().rev().take(4).map(|v|json!({"scope":v["scope"],"reason":v["reason"],"manager_proposal_reason":v["manager_proposal_reason"],"book_fresh":v["book_fresh"],"generation":v["generation"],"epoch":v["epoch"]})).collect::<Vec<_>>(),
            "okx_books_invalid_total":summary.okx_books_invalid_audit.len(),
            "okx_books_invalid_reasons":audit_counts(&summary.okx_books_invalid_audit,"failure_reason"),
            "okx_books_invalid_tail":summary.okx_books_invalid_audit.iter().rev().take(4).map(|v|json!({"failure_reason":v["failure_reason"],"source":v["source"],"instrument":v["instrument"],"channel":v["channel"],"action":v["action"],"snapshot":v["snapshot"],"seq_id":v["seq_id"],"prev_seq_id":v["prev_seq_id"],"expected_previous_seq":v["expected_previous_seq"],"event_age_ms":v["event_age_ms"],"checksum_validation":v["checksum_validation"]})).collect::<Vec<_>>(),
            "stale_frame_audit_total":summary.stale_frame_audit.len(),
            "stale_frame_audit_tail":summary.stale_frame_audit.iter().rev().take(4).map(|v|json!({"category":v["category"],"freshness_reject_reason":v["freshness_reject_reason"],"venue":v["venue"],"instrument":v["instrument"],"exchange_age_at_t1_ms":v["exchange_age_at_t1_ms"],"exchange_age_at_t4_ms":v["exchange_age_at_t4_ms"]})).collect::<Vec<_>>(),
            "session_events":summary.events.iter().filter(|e|matches!(e["kind"].as_str(),Some("connected"|"subscribed"|"okx_symbol_resubscribe_requested"))).rev().take(12).cloned().collect::<Vec<_>>()
        },
        "engine_counts":{
            "r1402_ucm_worker":summary.engine["r1402_ucm_worker"],
            "received":summary.engine["received"],"duplicates":summary.engine["duplicates"],
            "invalid":summary.engine["invalid"],"switches":summary.engine["switches"],
            "blocked":summary.engine["blocked"],"submitted":summary.engine["submitted"],
            "hot_deficits":summary.engine["hot_deficits"],"reorder":summary.engine["reorder"],
            "audit_evictions":summary.engine["audit_evictions"],
            "invalid_categories":summary.engine["invalid_categories"]
        },
        "reader_received_total":summary.reader_received_total,
        "reader_queue_overflow_events":summary.reader_queue_overflow_events,
        "peak_reader_frame_queue_depth":summary.peak_reader_frame_queue_depth,
        "central_frame_queue_capacity":summary.central_frame_queue_capacity,
        "reader_frame_queue_capacity_per_session":summary.reader_frame_queue_capacity_per_session,
        "central_queue_depth_max":summary.central_queue_depth_max,
        "engine_queue_age_ns":percentile(&summary.engine_queue_age_samples_ns),
        "engine_batch_service_ns":percentile(&summary.engine_batch_service_samples_ns),
        "r1273_engine_profile":summary.r1273_engine_profile.clone(),
        "r1273_central_queue":summary.r1273_central_queue.clone(),
        "r1379_e0_queue_phases":summary.r1379_e0_queue_phases.clone(),
        "output_send_wait_ns":percentile(&summary.output_send_wait_samples_ns),
        "diagnostic_event_evictions":summary.diagnostic_event_evictions,
        "r1279_engine_trace_dropped":summary.r1279_engine_trace_dropped
    })
}
fn safe_output(arg:&str)->Result<PathBuf,String>{
    let p=PathBuf::from(arg);
    let parent=p.parent().ok_or("output parent missing")?.canonicalize().map_err(|e|e.to_string())?;
    let allowed="/mnt/spotarb_disk2/SPOT_ARB_R1326_TRADE_TRIGGERED_DURABILITY_20261118/R1326E_LIVE_SHADOW";
    if parent.to_string_lossy()!=allowed{return Err("output outside isolated R1326E shadow directory".into())}
    let out=parent.join(p.file_name().ok_or("output name missing")?);
    if out.exists(){return Err("output already exists".into())}
    Ok(out)
}
fn kill_is_engaged()->Result<(),String>{
    let p="/home/tmpadmin/spotbot/data/real_kill_switch.json";
    let v:Value=serde_json::from_slice(&fs::read(p).map_err(|e|e.to_string())?).map_err(|e|e.to_string())?;
    if v["state"]!="ENGAGED"{return Err("real kill switch not ENGAGED".into())}
    Ok(())
}
// Temporary selected-frame flight recorder: no additional clone of a
// book/raw payload, no hot-path formatting or file writes.
const CONSUMER_JOURNAL_CAP:usize=4096;
struct ConsumerRecord{
 receive_ns:u64,selection_epoch:u64,connection_epoch:u64,venue:u8,
 base:[u8;32],session:[u8;96],trace:[u8;192],truncated:bool,
 upstream:[u64;12],stamps:[[u64;3];7],outcome:u8,
}
fn detail_copy<const N:usize>(dst:&mut [u8;N],s:&str)->bool{
 let n=s.len().min(N);dst[..n].copy_from_slice(&s.as_bytes()[..n]);s.len()>N
}
fn detail_text(b:&[u8])->String{
 String::from_utf8_lossy(&b[..b.iter().position(|x|*x==0).unwrap_or(b.len())]).into_owned()
}
fn detail_clock()->[u64;3]{
 let before=mono_ns();
 let mut t=libc::timespec{tv_sec:0,tv_nsec:0};
 let cpu=if unsafe{libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID,&mut t)}==0{
 t.tv_sec as u64*1_000_000_000+t.tv_nsec as u64}else{0};
 [before,cpu,mono_ns()]
}
struct ConsumerJournal{path:PathBuf,rows:std::collections::VecDeque<ConsumerRecord>,total:u64}
impl ConsumerJournal{
 fn begin<'a>(&'a mut self,f:&spotarb_native_market_core::RawFrame,base:&str)->ConsumerSpan<'a>{
  use spotarb_native_market_core::Venue;
  let mut r=ConsumerRecord{receive_ns:f.received_ns,selection_epoch:0,connection_epoch:0,venue:if f.venue==Venue::OkxSpot{1}else{2},
   base:[0;32],session:[0;96],trace:[0;192],truncated:false,upstream:[0;12],stamps:[[0;3];7],outcome:0};
  r.truncated=detail_copy(&mut r.base,base);
  if let Some(s)=&f.source{
   r.connection_epoch=s.connection_epoch;r.selection_epoch=s.selection_epoch;
   r.truncated|=detail_copy(&mut r.session,&s.session_id)|detail_copy(&mut r.trace,&s.frame_trace_id);
   if let Some(t)=&s.r1279_timing{
    r.upstream=[t.ws_receive_ns,t.reader_enqueue_ns,t.reader_dequeue_ns,t.central_permit_acquired_ns,t.central_enqueue_ns,t.central_dequeue_ns,
     t.validation_start_ns,t.validation_end_ns,t.engine_receive_begin_ns,t.engine_receive_end_ns,t.source_selection_end_ns,t.r1325_output_enqueued_ns].map(|x|x.unwrap_or(0));
   }
  }
  r.stamps[0]=detail_clock();ConsumerSpan{journal:self,row:r}
 }
 fn flush(&self)->Result<(),String>{
  use std::io::Write;
  let f=fs::OpenOptions::new().create_new(true).write(true).open(&self.path).map_err(|e|e.to_string())?;
  let mut w=std::io::BufWriter::new(f);
  writeln!(w,"{}",json!({"schema":"R1410J/CONSUMER/2","total":self.total,"retained":self.rows.len(),"overwritten":self.total.saturating_sub(self.rows.len() as u64),"capacity":CONSUMER_JOURNAL_CAP,"record_bytes":std::mem::size_of::<ConsumerRecord>(),
   "upstream":["ws_receive","reader_enqueue","reader_dequeue","central_permit","central_enqueue","central_dequeue","validation_start","validation_end","engine_begin","engine_end","selection_end","output_enqueue"],
   "stamps":["consumer_start_after_scope_lookup","provider_end","observation_end","owner_parts_end","owner_ingest_end","snapshot_end","consumer_end"],"clock_columns":["mono_before_ns","thread_cpu_ns","mono_after_ns"],"outcomes":{"0":"error_before_decision","1":"provider_denied","2":"observation_denied","3":"decision_updated"}})).map_err(|e|e.to_string())?;
  for r in &self.rows{
   writeln!(w,"{}",json!({"receive_ns":r.receive_ns,"selection_epoch":r.selection_epoch,"connection_epoch":r.connection_epoch,"venue":r.venue,"base":detail_text(&r.base),"session":detail_text(&r.session),"frame_trace_id":detail_text(&r.trace),"identity_truncated":r.truncated,"upstream":r.upstream,"stamps":r.stamps,"outcome":r.outcome})).map_err(|e|e.to_string())?;
  }
  w.flush().map_err(|e|e.to_string())
 }
}
struct ConsumerSpan<'a>{journal:&'a mut ConsumerJournal,row:ConsumerRecord}
impl Drop for ConsumerSpan<'_>{fn drop(&mut self){
 self.row.stamps[6]=detail_clock();self.journal.total+=1;
 if self.journal.rows.len()==CONSUMER_JOURNAL_CAP{self.journal.rows.pop_front();}
 let row=std::mem::replace(&mut self.row,ConsumerRecord{receive_ns:0,selection_epoch:0,connection_epoch:0,venue:0,base:[0;32],session:[0;96],trace:[0;192],truncated:false,upstream:[0;12],stamps:[[0;3];7],outcome:0});
 self.journal.rows.push_back(row);
}}
fn consumer_mark(s:&mut Option<ConsumerSpan<'_>>,n:usize){if let Some(s)=s{s.row.stamps[n]=detail_clock();}}
fn consumer_outcome(s:&mut Option<ConsumerSpan<'_>>,code:u8){if let Some(s)=s{s.row.outcome=code;}}
// Public, execution-free census using the integrated runtime's metadata
// eligibility and source shard width. Keep native metadata route math.
#[tokio::main(flavor="multi_thread",worker_threads=2)]
async fn main()->Result<(),String>{
    use spotarb_native_market_core as core;
    use core::{Owner,Venue,decision::DecisionCore,native_paper_bridge_r1301::{PaperSource1325,observation_r1410}};
    let args=std::env::args().skip(1).collect::<Vec<_>>();
    if args.len()!=2{return Err("usage: r1326i_universe_public_shadow SECONDS OUTPUT_JSON".into())}
    let seconds:u64=args[0].parse().map_err(|_|"invalid duration")?;
    if !matches!(seconds,60|300|900|3600){return Err("duration must be exactly 60, 300, 900 or 3600 seconds".into())}
    scrub_capture_environment()?;kill_is_engaged()?;
    let output=safe_output(&args[1])?;
    let scan_path=safe_output(&format!("{}.scan.json",output.display()))?;
    let trading=core::native_trading_config::NativeTradingConfig::parse(include_str!("../../config/native_trading.yaml")).map_err(|e|format!("{e:?}"))?;
    let policy=core::native_paper_config_bridge::inspect_public_paper_policy_readonly(&trading).map_err(|e|format!("{e:?}"))?;
    if policy.max_age_ms!=800{return Err("freshness must remain 800ms".into())}
    let meta=core::market_metadata::fetch_current_metadata(1).map_err(|e|format!("{e:?}"))?;
    let bases=meta.markets.iter().filter(|(_,m)|m.metadata_state()=="SIZE_METADATA_READY").map(|(b,_)|b.clone()).collect::<Vec<_>>();
    let groups=core::native_r167_source_ws_shards_review::split_source_groups_for_ingress(&[bases.clone()],core::ingress_runtime_r1205::R1205_MAX_BASES_PER_SHARD).map_err(str::to_owned)?;
    let scan=json!({"generation":meta.generation,"loaded_wall_ms":meta.loaded_wall_ms,"binance_active":meta.binance_active,
        "okx_active":meta.okx_active,"binance_tradifi_excluded":meta.binance_tradifi_excluded,"intersection":meta.intersection(),
        "eligible":bases,"static_denied":meta.static_denied,"markets":meta.markets,"groups":groups,"policy_sha256":trading.sha256});
    save_new(&scan_path,&scan)?;
    println!("R1410E_SCAN {}",json!({"binance":meta.binance_active,"okx":meta.okx_active,"intersection":meta.intersection(),"eligible":bases.len(),"denied":meta.static_denied,"shards":groups.len(),"sockets":groups.len()*4,"scan":scan_path}));
    let mut consumer_journal=None;
    std::env::remove_var("R1410I_JOURNAL_DIR");
    if std::env::var("R1410I_JOURNAL").ok().as_deref()==Some("1"){
        // 64 owner rings plus consumer ring fit the agreed RAM budget;
        // fail explicitly, never substitute a subset of the scanner universe.
        if groups.len()>64{return Err("diagnostic RAM budget: too many owner groups".into())}
        let dir=safe_output(&format!("{}.journal",output.display()))?;
        use std::os::unix::fs::DirBuilderExt;
        fs::DirBuilder::new().mode(0o700).create(&dir).map_err(|e|e.to_string())?;
        std::env::set_var("R1410I_JOURNAL_DIR",&dir);
        consumer_journal=Some(ConsumerJournal{path:dir.join("consumer.jsonl"),rows:std::collections::VecDeque::with_capacity(CONSUMER_JOURNAL_CAP),total:0});
    }
    let authority:Authority=Arc::new(RwLock::new(HashMap::new()));
    let mut sources:HashMap<String,PaperSource1325>=bases.iter().map(|b|(b.clone(),PaperSource1325::new(authority.clone(),"R1410E_PUBLIC_OBSERVATION".into()))).collect();
    let routes=meta.routes.into_iter().filter(|(b,_)|sources.contains_key(b)).collect();
    let mut decision=DecisionCore::new(0,1,policy,HashMap::new()).map_err(|e|format!("{e:?}"))?;
    decision.replace_routes(meta.generation,routes).map_err(|e|format!("{e:?}"))?;
    let mut owners=(0..12).map(|i|Owner::new(i,12,800).map_err(|e|format!("{e:?}"))).collect::<Result<Vec<_>,_>>()?;
    let (tx,mut rx)=mpsc::channel::<Output>(core::ingress_runtime_r1205::R1205_OUTPUT_QUEUE_CAPACITY);
    let mut runs=tokio::task::JoinSet::new();
    std::env::set_var("R1248_BINANCE_GROUP_SIZE","10");
    kill_is_engaged()?;
    for (shard,bases) in groups.into_iter().enumerate(){
        let cfg=Config{shard,bases,copies:2,duration:Duration::from_secs(seconds),controlled_close_after:None,endpoint_variant:0,probe_rotation_after:None};
        let auth=authority.clone();let tx=tx.clone();
        runs.spawn(async move{(shard,run_r1246_volatile(cfg,auth,tx).await)});
    }
    drop(tx);
    let started=std::time::Instant::now();
    let mut scopes=std::collections::BTreeMap::<String,[u64;8]>::new();
    for base in &bases{for venue in [Venue::BinanceFutures,Venue::OkxSpot]{scopes.insert(format!("{venue:?}:{base}"),[0;8]);}}
    let mut reasons=std::collections::BTreeMap::<String,u64>::new();
    let mut summaries=std::collections::BTreeMap::new();let mut failure=None;let mut rx_closed=false;
    let mut total_decisions=0u64;
    loop{
        if runs.is_empty()&&rx_closed{break}
        tokio::select!{
            result=runs.join_next(),if !runs.is_empty()=>{
                match result{
                    Some(Ok((shard,Ok(summary))))=>{
                        let path=PathBuf::from(format!("{}.shard-{shard}.json",output.display()));
                        save_new(&path,&compact(&summary))?;summaries.insert(shard,path);
                    },
                    Some(Ok((shard,Err(e))))=>{failure=Some(format!("shard {shard}: {e}"));break},
                    Some(Err(e))=>{failure=Some(format!("join: {e}"));break},None=>{}
                }
            },
            item=rx.recv(),if !rx_closed=>{
                let Some(item)=item else{rx_closed=true;continue};
                let result=(||->Result<(),String>{
                    match item{
                        Output::Reset{venue,base,epoch}=>{
                            let k=format!("{venue:?}:{base}");scopes.get_mut(&k).ok_or("unknown reset")?[0]+=1;
                            sources.get_mut(&base).ok_or("unknown source")?.reset(venue,&base,epoch).map_err(|e|format!("reset:{e:?}"))?;
                            owners[core::owner_index(&base,12)].reset_venue(&base,venue,epoch).map_err(|e|format!("owner reset:{e:?}"))?;
                            decision.reset_exchange(&base,if venue==Venue::OkxSpot{"okx_spot"}else{"binance_futures"},epoch).map_err(|e|format!("decision reset:{e:?}"))?;
                        },
                        Output::Blocked{venue,base,..}=>{scopes.get_mut(&format!("{venue:?}:{base}")).ok_or("unknown blocked scope")?[1]+=1;},
                        Output::Resubscribe{..}=>return Err("unexpected resubscribe output".into()),
                        item=>{
                            let (venue,base)=match &item{
                                Output::Book(f,u)=>(f.venue,u.base.clone()),
                                Output::Frame(f)=>(f.venue,core::parse(f).map_err(|e|format!("{e:?}"))?.base),
                                _=>return Err("unexpected output".into())
                            };
                            let row=scopes.get_mut(&format!("{venue:?}:{base}")).ok_or("unknown selected scope")?;
                            row[2]+=1;let n=mono_ns();let w=wall_ms();
                            let mut detail=consumer_journal.as_mut().map(|j|match &item{
                                Output::Frame(f)|Output::Book(f,_)=>j.begin(f,&base),
                                _=>unreachable!("selected output matched above"),
                            });
                            let source=sources.get_mut(&base).ok_or("unknown source")?;
                            let input=match source.accept_observation(item,n,w){
                                Ok(i)=>i,Err(e)=>{consumer_mark(&mut detail,1);consumer_outcome(&mut detail,1);row[6]+=1;*reasons.entry(format!("provider:{e:?}")).or_default()+=1;return Ok(())}
                            };
                            consumer_mark(&mut detail,1);
                            let live=match observation_r1410(&input,source.receiver(),n,w){
                                Ok(live)=>live,Err(e)=>{consumer_mark(&mut detail,2);consumer_outcome(&mut detail,2);row[6]+=1;*reasons.entry(format!("observation:{e:?}")).or_default()+=1;return Ok(())}
                            };
                            consumer_mark(&mut detail,2);
                            row[3]+=1;if !live{row[5]+=1;}
                            let (frame,update)=input.owner_parts_r1322();
                            consumer_mark(&mut detail,3);
                            row[7]=row[7].max(n.saturating_sub(frame.received_ns));
                            let owner=&mut owners[core::owner_index(&base,12)];
                            owner.ingest_parsed(&frame,update,&mut core::history::PreparedSignal).map_err(|e|format!("owner:{e:?}"))?;
                            consumer_mark(&mut detail,4);
                            let book=owner.snapshot_venue(&base,venue).ok_or("owner missing book")?;
                            consumer_mark(&mut detail,5);
                            let rows=decision.update_at(&base,if venue==Venue::OkxSpot{"okx_spot"}else{"binance_futures"},book,n,w).map_err(|e|format!("decision:{e:?}"))?;
                            consumer_outcome(&mut detail,3);
                            row[4]+=1;total_decisions+=1;
                            for d in rows{*reasons.entry(format!("decision:{}",d.reason)).or_default()+=1;}
                        }
                    }
                    Ok(())
                })();
                if let Err(e)=result{failure=Some(e);break}
            }
        }
    }
    if failure.is_some(){runs.abort_all();}
    // Wait for existing aborted task destructors, including owner-local
    // diagnostic flushes. No new data accepted or trading enabled.
    while runs.join_next().await.is_some(){}
    let journal_flush_error=consumer_journal.as_ref().and_then(|j|j.flush().err());
    let report=json!({"schema":"SPOTARB/R1410E/PUBLIC_OBSERVATION/V1","duration_requested_s":seconds,"elapsed_ms":started.elapsed().as_millis(),"bases":bases,
        "scan":scan_path,"orders":0,"paper":false,"private_api":false,"db1_db2":"unused","universum":"untouched",
        "failure":failure,"journal_flush_error":journal_flush_error,"output_drained":rx_closed,"completed_shards":summaries,"total_decision_updates":total_decisions,
        "columns":["reset","blocked","selected","admitted","decision","historical","denied","receive_age_max_ns"],"scopes":scopes,"reasons":reasons});
    save_new(&output,&report)?;
    println!("R1410E_RESULT {}",json!({"output":output,"failure":failure,"decisions":total_decisions,"completed_shards":summaries.len(),"orders":0}));
    kill_is_engaged()?;
    if let Some(e)=failure{return Err(e)}
    Ok(())
}
fn save_new(path:&PathBuf,value:&Value)->Result<(),String>{
    use std::io::Write;
    let bytes=serde_json::to_vec(value).map_err(|e|e.to_string())?;
    if bytes.len()>8*1024*1024{return Err("bounded report exceeded 8MiB".into())}
    let mut f=fs::OpenOptions::new().write(true).create_new(true).open(path).map_err(|e|e.to_string())?;
    f.write_all(&bytes).map_err(|e|e.to_string())?;f.sync_all().map_err(|e|e.to_string())
}
#[cfg(test)]
mod tests{
    use super::*;
    #[test]fn r1326e_capture_yes_is_scrubbed_before_runtime(){
        let old=std::env::var_os("R1268_CAPTURE");let old_dir=std::env::var_os("R1268_CAPTURE_DIR");
        std::env::set_var("R1268_CAPTURE","YES");std::env::set_var("R1268_CAPTURE_DIR","/tmp/forbidden-r1326e-capture");
        assert!(scrub_capture_environment().is_ok());
        assert!(std::env::var_os("R1268_CAPTURE").is_none());
        assert!(std::env::var_os("R1268_CAPTURE_DIR").is_none());
        if let Some(v)=old{std::env::set_var("R1268_CAPTURE",v)}else{std::env::remove_var("R1268_CAPTURE")}
        if let Some(v)=old_dir{std::env::set_var("R1268_CAPTURE_DIR",v)}else{std::env::remove_var("R1268_CAPTURE_DIR")}
    }
}
