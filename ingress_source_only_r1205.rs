//! Actual ingress/Owner/DB1/DB2/ACK observer. No Portfolio or order transport.
use crate as core;
use core::{Owner,Venue,parse,owner_index,RawFrame,Update};
use core::decision::{DecisionConfig,DecisionCore,RouteSpec,Limits};
use core::native_r1173_compact_writer::Inject;
use core::custody_executor_r1241::CustodyWorker;
use core::native_r1173_compact_writer::ingress_store_r1205::{Store,StoreWorker,digest};
use core::market_data_ingress_r1205::{Output,Permit,authorization_at_normalized};
use core::ingress_runtime_r1205::{Config,mono_ns,wall_ms,R1205_REQUIRED_COPIES,R1205_OUTPUT_QUEUE_CAPACITY};
use std::{sync::{Arc,Mutex,RwLock},collections::{HashMap,VecDeque},path::PathBuf,time::Duration,os::unix::fs::DirBuilderExt};

fn push_evidence(evidence:&mut VecDeque<serde_json::Value>,value:serde_json::Value,evictions:&mut u64){
 if evidence.len()>=8192{evidence.pop_front();*evictions=evictions.saturating_add(1);}
 evidence.push_back(value);
}
fn latency_summary(samples:&VecDeque<u64>,total:u64,evictions:u64,over_800ms:u64,max_ns:u64)->serde_json::Value{
 let mut sorted=samples.iter().copied().collect::<Vec<_>>();sorted.sort_unstable();
 let at=|p:usize|sorted.get(((sorted.len().saturating_sub(1))*p)/100).copied();
 serde_json::json!({"total":total,"samples_retained":sorted.len(),"sample_evictions":evictions,"p50_ns":at(50),"p95_ns":at(95),"p99_ns":at(99),"max_ns":max_ns,"over_800ms":over_800ms})
}
#[derive(Default)]
struct StageLatency{samples:VecDeque<u64>,total:u64,evictions:u64,over_800ms:u64,max_ns:u64}
impl StageLatency{
 fn observe(&mut self,ns:u64){
  self.total+=1;self.max_ns=self.max_ns.max(ns);if ns>800_000_000{self.over_800ms+=1;}
  if self.samples.len()>=8192{self.samples.pop_front();self.evictions+=1;}
  self.samples.push_back(ns);
 }
 fn summary(&self)->serde_json::Value{latency_summary(&self.samples,self.total,self.evictions,self.over_800ms,self.max_ns)}
}

struct PendingAck {
 frame: RawFrame,
 source_authority: Permit,
 base: String,
 event_timestamp: Option<i64>,
 gate: bool,
 decision_digest: String,
 dequeue_ns: u64,
 queue_age_ns: u64,
 owner_guard_ns: u64,
 owner_guard_wall_ms: i64,
 owner_ns: u64,
 owner_commit_ns: u64,
 decision_ns: u64,
 decision_service_ns: u64,
 custody_start_ns: u64,
}
fn take_consecutive_frame_outputs(
 first:(RawFrame,Option<Update>),
 rx:&mut tokio::sync::mpsc::Receiver<Output>,
 deferred:&mut Option<Output>,
)->Vec<(RawFrame,Option<Update>)>{
 let mut batch=Vec::with_capacity(32);batch.push(first);
 while batch.len()<32&&deferred.is_none(){
  match rx.try_recv(){
   Ok(Output::Frame(frame))=>batch.push((frame,None)),
   Ok(Output::Book(frame,update))=>batch.push((frame,Some(update))),
   Ok(control)=>{*deferred=Some(control);break},
   Err(_)=>break,
  }
 }
 batch
}
pub async fn run_source_only(root:PathBuf)->Result<serde_json::Value,String>{
 run_source_only_with_config(root,DecisionConfig{max_age_ms:800,metadata_ttl_ms:43_200_000,base_size:10.,risk_cap:10.,cost:24.03,required_net:42.,min_price:0.,max_price:f64::INFINITY,blacklist:Default::default()}).await
}
/// Execution-free evaluation with the actual entry point decision policy. No
/// fabricated exchange limits: missing metadata must deny any opportunity.
pub async fn run_source_only_with_config(root:PathBuf,policy:DecisionConfig)->Result<serde_json::Value,String>{
 if std::env::var("R1241_ASYNC_CANONICAL").ok().as_deref()==Some("YES") {
  return run_source_only_async_canonical(root,policy).await;
 }
 async {
 if std::env::var("R1205_SOURCE_ONLY").ok().as_deref()!=Some("YES"){return Err("explicit source-only gate required".into())}

 let seconds=std::env::var("R1206_SOURCE_ONLY_SECONDS").unwrap_or_else(|_|"90".into()).parse::<u64>()?;if !matches!(seconds,90|300|900|1800|3600){return Err("R1214 source-only duration must be90,300,900,1800,or3600".into())}
 let controlled_close_after=if std::env::var("R1216_SOURCE_ONLY_DISABLE_CONTROLLED_CLOSE").ok().as_deref()==Some("YES"){None}else{Some(Duration::from_secs(30))};
 let cfg=Config{shard:0,bases:vec!["BTC".into(),"ETH".into()],copies:R1205_REQUIRED_COPIES,duration:Duration::from_secs(seconds),controlled_close_after,endpoint_variant:0,probe_rotation_after:if controlled_close_after.is_some()&&seconds>=900{Some(Duration::from_secs(300))}else{None}};
 let inbox_root=root.join("inbox");if !inbox_root.exists(){std::fs::DirBuilder::new().mode(0o700).create(&inbox_root)?;}
 let binding=root.join("canonical_path.json");let resume=binding.exists();
 let db=if resume{serde_json::from_slice::<PathBuf>(&std::fs::read(&binding)?)?}else{let p=std::env::temp_dir().join(format!("spotarb_r1173_r1205_live_{}_{}",std::process::id(),mono_ns()));std::fs::DirBuilder::new().mode(0o700).create(&p)?;p};
 let store=Arc::new(Mutex::new(Store::open(&inbox_root)?));store.lock().unwrap().profile_capacity(65536)?;let auth=Arc::new(RwLock::new(HashMap::new()));let w=CustodyWorker::open(db.clone(),resume).await?;store.lock().unwrap().bind_canonical(&db)?;
 if !resume{use std::os::unix::fs::OpenOptionsExt;use std::io::Write;let mut f=std::fs::OpenOptions::new().write(true).create_new(true).mode(0o600).open(&binding)?;f.write_all(&serde_json::to_vec(&db)?)?;f.sync_all()?;std::fs::File::open(&root)?.sync_all()?;}
 let recovery_store=store.clone();
 w.call(move|writer|{
  writer.recover_ingress_observer(&*recovery_store.lock().map_err(|_|"inbox poison")?)?;
  core::market_data_ingress_r1205::reconcile(&recovery_store,writer)?;
  writer.sweep_ingress_receipts(&mut *recovery_store.lock().map_err(|_|"inbox poison")?)?;
  Ok(())
 }).await?;
 let store_worker=StoreWorker::start(store.clone())?;
 let absent=Limits{min_amount:String::new(),step:String::new(),min_cost:String::new(),max_amount:None};
 let routes=["BTC","ETH"].into_iter().map(|b|(b.into(),vec![RouteSpec{buy_exchange:"okx_spot".into(),sell_exchange:"binance_futures".into(),label:"SOURCE_ONLY_METADATA_UNAVAILABLE".into(),buy:absent.clone(),sell:absent.clone(),loaded_ns:0,metadata_state:"SIZE_METADATA_MISSING".into()}])).collect::<HashMap<_,_>>();
 let mut cores=(0..12).map(|i|DecisionCore::new(i,12,policy.clone(),routes.clone()).map_err(|e|format!("{e:?}"))).collect::<Result<Vec<_>,_>>()?;
 let mut decision_rows=0u64;let mut decision_reasons=std::collections::BTreeMap::<String,u64>::new();
 let mut owners=(0..12).map(|i|Owner::new(i,12,800).unwrap()).collect::<Vec<_>>();
 let (tx,mut rx)=tokio::sync::mpsc::channel(R1205_OUTPUT_QUEUE_CAPACITY);let run=tokio::spawn(core::ingress_runtime_r1205::run_with_store_worker(cfg,store.clone(),store_worker.clone(),auth.clone(),tx));
 let mut published=0u64;let mut quarantined=0u64;let mut reset=0u64;let mut decisions=0u64;let mut evidence=VecDeque::new();let mut evidence_evictions=0u64;let mut last_ns=mono_ns();let mut max_queue_ms=0u64;let mut queue_samples=VecDeque::new();let mut queue_sample_evictions=0u64;let mut queue_frames=0u64;let mut queue_over_800ms=0u64;let mut max_queue_ns=0u64;let mut owner_latency=StageLatency::default();let mut decision_latency=StageLatency::default();let mut custody_latency=StageLatency::default();let mut service_latency=StageLatency::default();let mut queue_outliers=VecDeque::<serde_json::Value>::new();let mut queue_outlier_evictions=0u64;let mut custody_batches=0u64;let mut custody_batch_frames=0u64;let mut custody_batch_max=0usize;let mut custody_batch_sizes=VecDeque::<u64>::new();let mut custody_batch_evictions=0u64;

 let mut deferred_output:Option<Output>=None;
 loop{
  let incoming=if let Some(output)=deferred_output.take(){Some(output)}else{rx.recv().await};
  let Some(output)=incoming else{break};
  let (output,normalized_update)=match output{Output::Book(frame,update)=>(Output::Frame(frame),Some(update)),other=>(other,None)};
  match output{
   Output::Reset{venue,base,epoch}=>{let mut owned=std::mem::take(&mut owners);let reset_base=base.clone();
    owners=w.call(move|writer|{writer.source_reset(&mut owned,venue,&[reset_base],epoch,wall_ms(),"PUBLIC_WS_RESET",Inject::None)?;Ok(owned)}).await?;cores[owner_index(&base,12)].reset_exchange(&base,if venue==Venue::OkxSpot{"okx_spot"}else{"binance_futures"},epoch).map_err(|e|format!("observer decision reset:{e:?}"))?;reset+=1;push_evidence(&mut evidence,serde_json::json!({"stage":"reset_ack","mono_ns":mono_ns(),"venue":format!("{venue:?}"),"base":base,"epoch":epoch}),&mut evidence_evictions);},
   Output::Resubscribe{..}=>unreachable!("runtime consumes internal resubscribe control"),
   Output::Blocked{venue,base,reason}=>push_evidence(&mut evidence,serde_json::json!({"stage":"blocked","mono_ns":mono_ns(),"venue":format!("{venue:?}"),"base":base,"reason":reason}),&mut evidence_evictions),
   Output::Frame(frame)=>{
    let batch=take_consecutive_frame_outputs((frame,normalized_update),&mut rx,&mut deferred_output);
    async {
     let mut publish_batch=Vec::<PendingAck>::with_capacity(batch.len());
     for (frame,normalized_update) in batch{
      let dequeue_ns=mono_ns();let queue_age_ns=dequeue_ns.saturating_sub(frame.received_ns);max_queue_ns=max_queue_ns.max(queue_age_ns);max_queue_ms=max_queue_ms.max(queue_age_ns/1_000_000);queue_frames+=1;if queue_age_ns>800_000_000{queue_over_800ms+=1;}if queue_samples.len()>=8192{queue_samples.pop_front();queue_sample_evictions+=1;}queue_samples.push_back(queue_age_ns);
      let owner_guard_ns=mono_ns();let owner_guard_wall_ms=wall_ms();
      let update=match normalized_update{Some(u)=>u,None=>parse(&frame).map_err(|e|format!("{e:?}"))?};
      let Some(source_authority)=authorization_at_normalized(&auth,&frame,&update,owner_guard_ns,owner_guard_wall_ms)else{let quarantine_frame=frame.clone();store_worker.call_async(move|inbox,_|inbox.finish(&quarantine_frame,"quarantined","selection_fenced_before_owner",None)).await?;quarantined+=1;push_evidence(&mut evidence,serde_json::json!({"stage":"queue_quarantined","reason":"selection_fenced_before_owner","session":frame.source.as_ref().map(|p|p.session_id.as_str()),"id":frame.source.as_ref().map(|p|p.accepted_id),"received_ns":frame.received_ns,"dequeue_ns":dequeue_ns,"queue_age_ns":queue_age_ns,"raw_sha256":frame.source.as_ref().map(|p|p.payload_sha256.as_str())}),&mut evidence_evictions);continue};
      let event_timestamp=update.book.exchange_ts_ms;let base=update.base.clone();let idx=owner_index(&base,12);
      let owner_started_ns=mono_ns();let mut owned=std::mem::take(&mut owners);let committed_frame=frame.clone();let committed_update=update.clone();
      owners=w.call(move|writer|{writer.source_commit_guarded(&mut owned,&[(committed_frame,committed_update,idx)],&[],Inject::None)?;Ok(owned)}).await?;
      let owner_ns=mono_ns();let owner_commit_ns=owner_ns.saturating_sub(owner_started_ns);owner_latency.observe(owner_commit_ns);let (s,f)=owners[idx].snapshot_pair(&base);
      let gate=core::market_data_ingress_r1205::owner_pair_authorized(&auth,&owners[idx],&base,mono_ns(),wall_ms())&&[s.as_ref(),f.as_ref()].iter().all(|b|b.is_some_and(|b|mono_ns()>=b.received_ns&&mono_ns()-b.received_ns<=800_000_000&&b.exchange_ts_ms.is_some_and(|e|wall_ms()>=e&&wall_ms()-e<=800)));
      if gate{decisions+=1;}
      let book=if frame.venue==Venue::OkxSpot{s.clone()}else{f.clone()}.ok_or("committed Owner book missing")?;
      let rows=cores[idx].update_at_native_verified_books5(&base,if frame.venue==Venue::OkxSpot{"okx_spot"}else{"binance_futures"},book,mono_ns(),wall_ms(),frame.venue==Venue::OkxSpot).map_err(|e|format!("observer DecisionCore:{e:?}"))?;
      decision_rows+=rows.len()as u64;for row in &rows{*decision_reasons.entry(row.reason.clone()).or_default()+=1;}
      let decision_ns=mono_ns();let decision_service_ns=decision_ns.saturating_sub(owner_ns);decision_latency.observe(decision_service_ns);let decision_digest=digest(&serde_json::to_vec(&serde_json::json!({"rows":rows,"action_gate":gate,"execution_enabled":false,"metadata_verified":false}))?);
      // Owner and DecisionCore remain ordered per frame. Only the durable
      // terminal publish/reclaim round trips are coalesced across ready frames.
      let custody_start_ns=mono_ns();let sealed_frame=frame.clone();let sealed_digest=decision_digest.clone();
      w.call(move|writer|writer.note_guarded_decision(&sealed_frame,"DECISION_DENIED",&sealed_digest)).await?;
      publish_batch.push(PendingAck{frame,source_authority,base,event_timestamp,gate,decision_digest,dequeue_ns,queue_age_ns,owner_guard_ns,owner_guard_wall_ms,owner_ns,owner_commit_ns,decision_ns,decision_service_ns,custody_start_ns});
     }
     if !publish_batch.is_empty(){
      let batch_size=publish_batch.len();custody_batches+=1;custody_batch_frames+=batch_size as u64;custody_batch_max=custody_batch_max.max(batch_size);if custody_batch_sizes.len()>=4096{custody_batch_sizes.pop_front();custody_batch_evictions+=1;}custody_batch_sizes.push_back(batch_size as u64);
      let frames=publish_batch.iter().map(|item|item.frame.clone()).collect::<Vec<_>>();
      let verify_frames=frames.clone();let verified=w.call(move|writer|writer.verify_ingress_batch(&verify_frames)).await?;
      let (committed,verified)=store_worker.call_async(move|inbox,_|{let committed=inbox.finish_published_batch_verified(&frames,&verified)?;Ok((committed,verified))}).await?;
      let acked=w.call(move|writer|{
       let ticket=writer.prepare_ingress_release_after_publish(&verified,&committed)?.ok_or("missing batch ingress release ticket")?;
       writer.commit_ingress_release(ticket)?;
       writer.confirm_ingress_reclaimed(&committed)
      }).await?;
      store_worker.call_async(move|inbox,_|inbox.forget_reclaimed_after_ack(&acked)).await?;
      let ack_ns=mono_ns();
      for item in publish_batch{
       let custody_ns=ack_ns.saturating_sub(item.custody_start_ns);let service_ns=ack_ns.saturating_sub(item.dequeue_ns);custody_latency.observe(custody_ns);service_latency.observe(service_ns);
       let p=item.frame.source.as_ref().ok_or("missing provenance")?;
       if item.queue_age_ns>=100_000_000||service_ns>=100_000_000{if queue_outliers.len()>=128{queue_outliers.pop_front();queue_outlier_evictions+=1;}queue_outliers.push_back(serde_json::json!({"session":p.session_id,"accepted_id":p.accepted_id,"venue":format!("{:?}",item.frame.venue),"instrument":item.base,"connection_generation":item.frame.epoch,"exchange_ts_ms":item.event_timestamp,"received_ns":item.frame.received_ns,"dequeue_ns":item.dequeue_ns,"ack_ns":ack_ns,"queue_age_ns":item.queue_age_ns,"owner_commit_ns":item.owner_commit_ns,"decision_service_ns":item.decision_service_ns,"custody_ack_ns":custody_ns,"local_service_ns":service_ns,"payload_sha256":p.payload_sha256}));}
       published+=1;push_evidence(&mut evidence,serde_json::json!({"stage":"published_ack","source_authority":item.source_authority,"owner_guard_ns":item.owner_guard_ns,"owner_guard_wall_ms":item.owner_guard_wall_ms,"id":p.accepted_id,"session":p.session_id,"venue":format!("{:?}",item.frame.venue),"base":item.base,"epoch":item.frame.epoch,"received_ns":item.frame.received_ns,"dequeue_ns":item.dequeue_ns,"queue_age_ns":item.queue_age_ns,"owner_ns":item.owner_ns,"owner_commit_ns":item.owner_commit_ns,"decision_ns":item.decision_ns,"decision_service_ns":item.decision_service_ns,"custody_ack_ns":custody_ns,"local_service_ns":service_ns,"decision_digest":item.decision_digest,"ack_ns":ack_ns,"E":item.event_timestamp,"action_gate":item.gate,"execution_enabled":false,"raw_sha256":p.payload_sha256,"transport":p.transport,"accepted_ns":p.accepted_ns,"selected_ns":p.selected_ns}),&mut evidence_evictions);
      }
     }
     if mono_ns()-last_ns>1_000_000_000{last_ns=mono_ns();let counts=store_worker.call_async(|inbox,_|inbox.counts()).await?;std::fs::write(root.join("progress.json"),serde_json::to_vec(&serde_json::json!({"published":published,"reset":reset,"counts":counts}))?)?;}
     Ok::<(),Box<dyn std::error::Error>>(())
    }.await?;
   }
   Output::Book(_,_)=>unreachable!("R1211 books output is normalized before dispatch"),
  }
 }

 let summary=run.await.map_err(|e|e.to_string())??;
 let final_store=store.clone();
 let (canonical_cursor,source_custody)=w.call(move|writer|{
  writer.recover_ingress_source_only()?;core::market_data_ingress_r1205::reconcile(&final_store,writer)?;
  writer.sweep_ingress_receipts(&mut *final_store.lock().map_err(|_|"inbox poison")?)?;
  writer.require_decisions_drained()?;writer.verify_pair()?;
  Ok((writer.cursor()?,writer.source_custody_counts()?))
 }).await?;
 let counts=store.lock().unwrap().counts()?;
 let sqlite_accept_profile=store_worker.call_async(|inbox,_|Ok(inbox.accept_profile())).await?;let receipt=serde_json::json!({"schema":"R1205/ACTUAL_SOURCE_OWNER_DB_ACK/V1","runtime":summary,"durable_writer":store_worker.profile(),"duration_seconds":seconds,"sqlite_accept_profile":sqlite_accept_profile,"accepted_counts":counts,"published":published,"queue_quarantined":quarantined,"resets":reset,"action_gate_evaluations":decisions,"decision_core_rows":decision_rows,"decision_core_reasons":decision_reasons,"decision_metadata_verified":false,"max_queue_ms":max_queue_ms,"queue_latency":latency_summary(&queue_samples,queue_frames,queue_sample_evictions,queue_over_800ms,max_queue_ns),"local_stage_latency":{"owner_commit":owner_latency.summary(),"decision_core":decision_latency.summary(),"custody_ack":custody_latency.summary(),"post_dequeue_service":service_latency.summary()},"custody_batching":{"batches":custody_batches,"frames":custody_batch_frames,"max_frames":custody_batch_max,"mean_frames":if custody_batches==0{0.0}else{custody_batch_frames as f64/custody_batches as f64},"size_samples_retained":custody_batch_sizes.len(),"size_sample_evictions":custody_batch_evictions},"queue_outlier_events":queue_outliers.len(),"queue_outlier_evictions":queue_outlier_evictions,"queue_outliers":queue_outliers,"observer_evidence_retained":evidence.len(),"observer_evidence_evictions":evidence_evictions,"canonical_custody_executor":w.profile(),"canonical_sql_commits":w.call(|_|Ok(core::custody_executor_r1241::sql_commit_profile())).await?,"canonical_cursor":canonical_cursor,"source_custody":source_custody,"canonical_db_root":db,"inbox_root":inbox_root,"orders":0,"paper_trades":0,"portfolio_constructed":false,"installed_binary_changed":false,"events":evidence});
 std::fs::write(root.join("live_e2e.json"),serde_json::to_vec(&receipt)?)?;
 w.shutdown().await?;
 println!("{}",serde_json::json!({"published":published,"counts":counts,"orders":0,"paper_trades":0,"db1_db2_ack":"PASS"}));Ok::<serde_json::Value,Box<dyn std::error::Error>>(receipt)
 }.await.map_err(|e|e.to_string())
}

#[cfg(test)]
mod r1216_batch_tests{
 use super::*;
 fn frame(received_ns:u64)->RawFrame{RawFrame{venue:Venue::OkxSpot,epoch:1,received_ns,received_wall_ms:1_000,raw:String::new(),source:None}}
 #[tokio::test]
 async fn r1216_output_batch_defers_control_without_reordering(){
  let(tx,mut rx)=tokio::sync::mpsc::channel(8);
  tx.send(Output::Frame(frame(1))).await.unwrap();
  tx.send(Output::Frame(frame(2))).await.unwrap();
  tx.send(Output::Blocked{venue:Venue::OkxSpot,base:"BTC".into(),reason:"test".into()}).await.unwrap();
  tx.send(Output::Frame(frame(3))).await.unwrap();
  let first=match rx.recv().await.unwrap(){Output::Frame(f)=>(f,None),_=>panic!("expected frame")};
  let mut deferred=None;let batch=take_consecutive_frame_outputs(first,&mut rx,&mut deferred);
  assert_eq!(batch.len(),2);
  assert!(matches!(deferred,Some(Output::Blocked{..})));
  assert!(matches!(rx.try_recv(),Ok(Output::Frame(_))));
 }
 #[tokio::test]
 async fn r1216_output_batch_is_bounded_to_writer_limit(){
  let(tx,mut rx)=tokio::sync::mpsc::channel(40);
  for n in 0..34{tx.send(Output::Frame(frame(n))).await.unwrap();}
  let first=match rx.recv().await.unwrap(){Output::Frame(f)=>(f,None),_=>panic!("expected frame")};
  let mut deferred=None;let batch=take_consecutive_frame_outputs(first,&mut rx,&mut deferred);
  assert_eq!(batch.len(),32);
  assert!(deferred.is_none());
  assert!(matches!(rx.try_recv(),Ok(Output::Frame(_))));
 }
}
async fn run_source_only_async_canonical(
 root:PathBuf, policy:DecisionConfig
)->Result<serde_json::Value,String>{
 async {
  if std::env::var("R1205_SOURCE_ONLY").ok().as_deref()!=Some("YES"){
   return Err("explicit source-only gate required".into())
  }
  let seconds=std::env::var("R1206_SOURCE_ONLY_SECONDS")
   .unwrap_or_else(|_|"300".into()).parse::<u64>()?;
  if !matches!(seconds,300|900|1800|3600){
   return Err("R1241 source-only duration must be 300/900/1800/3600".into())
  }
  if !root.exists(){std::fs::DirBuilder::new().mode(0o700).recursive(true).create(&root)?;}
  let cfg=Config{shard:0,bases:vec!["BTC".into(),"ETH".into()],
   copies:R1205_REQUIRED_COPIES,duration:Duration::from_secs(seconds),
   controlled_close_after:None,endpoint_variant:0,probe_rotation_after:None};
  let inbox_root=root.join("inbox");
  if !inbox_root.exists(){std::fs::DirBuilder::new().mode(0o700).create(&inbox_root)?;}
  let binding=root.join("canonical_path.json");
  let resume=binding.exists();
  let db=if resume{serde_json::from_slice::<PathBuf>(&std::fs::read(&binding)?)?}
   else{
    let path=std::env::temp_dir().join(format!(
     "spotarb_r1173_r1241_source_{}_{}",std::process::id(),mono_ns()));
    std::fs::DirBuilder::new().mode(0o700).create(&path)?;
    path
   };
  let store=Arc::new(Mutex::new(Store::open(&inbox_root)?));
  store.lock().map_err(|_|"R1241 inbox lock poisoned")?.profile_capacity(65536)?;
  let auth=Arc::new(RwLock::new(HashMap::new()));
  let w=CustodyWorker::open(db.clone(),resume).await?;
  store.lock().map_err(|_|"R1241 inbox lock poisoned")?.bind_canonical(&db)?;
  if !resume{
   use std::os::unix::fs::OpenOptionsExt;
   use std::io::Write;
   let mut file=std::fs::OpenOptions::new().write(true).create_new(true)
    .mode(0o600).open(&binding)?;
   file.write_all(&serde_json::to_vec(&db)?)?;
   file.sync_all()?;
   std::fs::File::open(&root)?.sync_all()?;
  }
  let recovery_store=store.clone();
  w.call(move|writer|{
   writer.recover_ingress_observer(&*recovery_store.lock().map_err(|_|"inbox poison")?)?;
   core::market_data_ingress_r1205::reconcile(&recovery_store,writer)?;
   writer.sweep_ingress_receipts(&mut *recovery_store.lock().map_err(|_|"inbox poison")?)?;
   Ok(())
  }).await?;

  let store_worker=StoreWorker::start(store.clone())?;
  let absent=Limits{min_amount:String::new(),step:String::new(),
   min_cost:String::new(),max_amount:None};
  let routes=["BTC","ETH"].into_iter().map(|base|(base.into(),vec![RouteSpec{
   buy_exchange:"okx_spot".into(),sell_exchange:"binance_futures".into(),
   label:"SOURCE_ONLY_METADATA_UNAVAILABLE".into(),buy:absent.clone(),
   sell:absent.clone(),loaded_ns:0,metadata_state:"SIZE_METADATA_MISSING".into()
  }])).collect::<HashMap<_,_>>();
  let mut cores=(0..12).map(|index|
   DecisionCore::new(index,12,policy.clone(),routes.clone())
    .map_err(|error|format!("{error:?}"))
  ).collect::<Result<Vec<_>,_>>()?;
  let owners=(0..12).map(|index|Owner::new(index,12,800).unwrap())
   .collect::<Vec<_>>();
  let client=w.client()?;
  let (command_tx,control_tx,mut canonical_rx)=
   core::r1241_ingress_pipeline::spawn(client,owners,store_worker.clone(),auth.clone());
  let (output_tx,mut output_rx)=tokio::sync::mpsc::channel(R1205_OUTPUT_QUEUE_CAPACITY);
  let mut runtime=Some(tokio::spawn(core::ingress_runtime_r1205::run_with_store_worker(
   cfg,store.clone(),store_worker.clone(),auth.clone(),output_tx)));
  let initial_cursor=w.call(|writer|writer.cursor()).await?;
  let mut durable_tx=initial_cursor.0;
  let mut source_done=false;
  let mut pipeline_stopped=false;
  let mut shutdown_sent=false;
  let mut active_batch:Option<(u64,usize)>=None;
  let mut next_reset_id=0_u64;
  let mut runtime_summary=serde_json::Value::Null;
  let mut pipeline_profile=serde_json::Value::Null;
  let mut published=0_u64;
  let mut quarantined=0_u64;
  let mut blocked=0_u64;
  let mut blocked_reasons=std::collections::BTreeMap::<String,u64>::new();
  let mut decision_rows=0_u64;
  let mut reasons=std::collections::BTreeMap::<String,u64>::new();
  let mut books=HashMap::<String,(Option<core::Book>,Option<core::Book>)>::new();
  let mut last_progress=std::time::Instant::now();

  loop{
   if source_done&&pipeline_stopped{break}
   tokio::select!{
    output=output_rx.recv(),if !source_done=>{
     match output{
      None=>{
       source_done=true;
       runtime_summary=serde_json::to_value(
        runtime.take().ok_or("R1241 runtime handle already consumed")?
         .await.map_err(|error|error.to_string())??
       )?;
       if !shutdown_sent{
        command_tx.send(core::r1241_ingress_pipeline::Command::Shutdown).await
         .map_err(|_|"R1241 source-only shutdown queue closed")?;
        shutdown_sent=true;
       }
      }
      Some(Output::Reset{venue,base,epoch})=>{
       next_reset_id=next_reset_id.checked_add(1).ok_or("reset id overflow")?;
       core::r1241_ingress_pipeline::submit(&command_tx,
        core::r1241_ingress_pipeline::Command::Reset{
         request_id:next_reset_id,venue,bases:vec![base],epoch
        })?;
      }
      Some(Output::Blocked{venue,base,reason})=>{
       blocked=blocked.saturating_add(1);
       let key=format!("{venue:?}:{base}:{reason}");
       let count=blocked_reasons.entry(key).or_insert(0);
       *count=count.saturating_add(1);
      },
      Some(Output::Resubscribe{..})=>unreachable!("runtime consumes resubscribe controls"),
      Some(output)=>{
       let(frame,update)=match output{
        Output::Frame(frame)=>(frame,None),
        Output::Book(frame,update)=>(frame,Some(update)),
        _=>unreachable!("control output handled above"),
       };
       let update=match update{Some(update)=>update,None=>parse(&frame)
        .map_err(|error|format!("R1241 source parse: {error:?}"))?};
       if authorization_at_normalized(&auth,&frame,&update,mono_ns(),wall_ms()).is_none(){
        quarantined=quarantined.saturating_add(1);
        core::r1241_ingress_pipeline::submit(&command_tx,
         core::r1241_ingress_pipeline::Command::Quarantine{
          frame,reason:"selection_fenced_before_owner".into()
         })?;
       }else{
        let owner_index=owner_index(&update.base,12);
        core::r1241_ingress_pipeline::submit(&command_tx,
         core::r1241_ingress_pipeline::Command::Frame(
          core::r1241_ingress_pipeline::CanonicalFrame{frame,update,owner_index}
         ))?;
       }
      }
     }
    },

    event=canonical_rx.recv(),if !pipeline_stopped=>{
     match event{
      None=>return Err("R1241 pipeline event channel closed before drain".into()),
      Some(core::r1241_ingress_pipeline::Event::BatchReady{batch_id,source_seq,frames})=>{
       if active_batch.is_some()||frames.is_empty()||source_seq!=durable_tx+1{
        return Err("R1241 source-only batch cursor/order mismatch".into())
       }
       durable_tx=source_seq;
       let frame_count=frames.len();
       let mut seals=Vec::with_capacity(frame_count);
       for item in frames{
        let now_ns=mono_ns();
        let now_wall=wall_ms();
        let base=item.update.base.clone();
        let (spot,future)=books.entry(base.clone()).or_insert((None,None));
        match item.frame.venue{
         Venue::OkxSpot=>*spot=Some(item.update.book.clone()),
         Venue::BinanceFutures=>*future=Some(item.update.book.clone()),
        }
        let pair_fresh=spot.as_ref().is_some_and(|book|
         now_ns>=book.received_ns&&now_ns-book.received_ns<=800_000_000&&
         book.exchange_ts_ms.is_some_and(|ts|now_wall>=ts&&now_wall-ts<=800))
         &&future.as_ref().is_some_and(|book|
         now_ns>=book.received_ns&&now_ns-book.received_ns<=800_000_000&&
         book.exchange_ts_ms.is_some_and(|ts|now_wall>=ts&&now_wall-ts<=800));
        let authorized=authorization_at_normalized(&auth,&item.frame,&item.update,
         now_ns,now_wall).is_some();
        let gate=pair_fresh&&authorized;
        let venue=if item.frame.venue==Venue::OkxSpot{"okx_spot"}else{"binance_futures"};
        let rows=cores[item.owner_index].update_at_native_verified_books5(
         &base,venue,item.update.book.clone(),now_ns,now_wall,
         item.frame.venue==Venue::OkxSpot
        ).map_err(|error|format!("R1241 source-only DecisionCore: {error:?}"))?;
        decision_rows=decision_rows.saturating_add(rows.len() as u64);
        for row in &rows{*reasons.entry(row.reason.clone()).or_default()+=1;}
        let witness=serde_json::json!({
         "schema":"SPOTARB/R1241/SOURCE-ONLY-DECISION/V1",
         "base":base,"venue":venue,"epoch":item.frame.epoch,
         "received_ns":item.frame.received_ns,"decision_ns":now_ns,
         "action_gate":gate,"execution_enabled":false,
         "metadata_verified":false,"rows":rows
        });
        let result_hash=digest(&serde_json::to_vec(&witness)
         .map_err(|error|format!("decision witness serialization: {error}"))?);
        seals.push(core::r1241_ingress_pipeline::DecisionSeal{
         frame:item.frame,result_hash,pre_decision_commit:None
        });
       }
       control_tx.send(core::r1241_ingress_pipeline::Control::FinalizeBatch{
        batch_id,decisions:seals
       }).await.map_err(|_|"R1241 source-only finalizer closed")?;
       active_batch=Some((batch_id,frame_count));
      }

      Some(core::r1241_ingress_pipeline::Event::BatchAcked{
       batch_id,transition_seqs,decision_seq,release_seq,frame_count
      })=>{
       let(expected_id,expected_count)=active_batch.take()
        .ok_or("R1241 unexpected source-only ACK")?;
       if batch_id!=expected_id||frame_count!=expected_count||!transition_seqs.is_empty(){
        return Err("R1241 source-only ACK identity/count mismatch".into())
       }
       let decision_seq=decision_seq.ok_or("R1241 decision seal missing")?;
       if decision_seq!=durable_tx+1{return Err("R1241 decision cursor gap".into())}
       durable_tx=decision_seq;
       let release_seq=release_seq.ok_or("R1241 release ACK missing")?;
       if release_seq!=durable_tx+1{return Err("R1241 release cursor gap".into())}
       durable_tx=release_seq;
       published=published.saturating_add(frame_count as u64);
      }
      Some(core::r1241_ingress_pipeline::Event::ResetReady{
       request_id,seq,venue,bases,epoch
      })=>{
       if seq!=durable_tx+1{return Err("R1241 reset cursor gap".into())}
       durable_tx=seq;
       let exchange=if venue==Venue::OkxSpot{"okx_spot"}else{"binance_futures"};
       for base in bases{
        cores[owner_index(&base,12)].reset_exchange(&base,exchange,epoch)
         .map_err(|error|format!("source-only reset refused: {error:?}"))?;
        if let Some(pair)=books.get_mut(&base){
         match venue{Venue::OkxSpot=>pair.0=None,Venue::BinanceFutures=>pair.1=None}
        }
       }
       control_tx.send(core::r1241_ingress_pipeline::Control::ResetApplied{request_id})
        .await.map_err(|_|"R1241 source-only reset control closed")?;
      }
      Some(core::r1241_ingress_pipeline::Event::Stopped{profile})=>{
       if !source_done||active_batch.is_some(){
        return Err("R1241 stopped before source and batch drain".into())
       }
       pipeline_profile=profile;
       pipeline_stopped=true;
      }
      Some(core::r1241_ingress_pipeline::Event::Failed(error))=>{
       return Err(format!("R1241 source-only pipeline failed: {error}").into())
      }
     }
    }
   }
   if last_progress.elapsed()>=Duration::from_secs(10){
    let counts=store_worker.call_async(|inbox,_|inbox.counts()).await?;
    std::fs::write(root.join("progress.json"),serde_json::to_vec(
     &serde_json::json!({"published":published,"quarantined":quarantined,
      "blocked":blocked,"counts":counts,"canonical_cursor":durable_tx})
    )?)?;
    last_progress=std::time::Instant::now();
   }
  }

  let final_store=store.clone();
  let(canonical_cursor,source_custody)=w.call(move|writer|{
   writer.recover_ingress_source_only()?;
   core::market_data_ingress_r1205::reconcile(&final_store,writer)?;
   writer.sweep_ingress_receipts(&mut *final_store.lock().map_err(|_|"inbox poison")?)?;
   writer.require_decisions_drained()?;
   writer.verify_pair()?;
   Ok((writer.cursor()?,writer.source_custody_counts()?))
  }).await?;
  if canonical_cursor.0!=durable_tx||
   source_custody["db1_source_frames"]!=source_custody["db2_source_frames"]||
   source_custody["db1_source_frames"]!=source_custody["acked_source_frames"]{
   return Err("R1241 final source custody/cursor mismatch".into())
  }
  let counts=store_worker.call_async(|inbox,_|inbox.counts()).await?;
  if counts.pending!=0{return Err("R1241 final inbox pending count is nonzero".into())}
  let sqlite_accept_profile=store_worker.call_async(
   |inbox,_|Ok(inbox.accept_profile())
  ).await?;
  let receipt=serde_json::json!({
   "schema":"SPOTARB/R1241/ASYNC-CANONICAL-SOURCE-ONLY/V1",
   "runtime":runtime_summary,"duration_seconds":seconds,
   "accepted_counts":counts,"published":published,
   "quarantined":quarantined,"blocked":blocked,
   "blocked_reasons":blocked_reasons,
   "decision_core_rows":decision_rows,"decision_core_reasons":reasons,
   "decision_metadata_verified":false,"action_gate_enabled":false,
   "sqlite_accept_profile":sqlite_accept_profile,
   "durable_writer":store_worker.profile(),
   "canonical_custody_executor":w.profile(),
   "canonical_sql_commits":w.call(
    |_|Ok(core::custody_executor_r1241::sql_commit_profile())
   ).await?,
   "async_canonical_ingress":pipeline_profile,
   "canonical_cursor":canonical_cursor,"source_custody":source_custody,
   "canonical_db_root":db,"inbox_root":inbox_root,
   "orders":0,"paper_trades":0,"portfolio_constructed":false,
   "installed_binary_changed":false
  });
  std::fs::write(root.join("live_e2e.json"),serde_json::to_vec(&receipt)?)?;
  w.shutdown().await?;
  println!("{}",serde_json::json!({
   "published":published,"counts":counts,"orders":0,
   "paper_trades":0,"db1_db2_ack":"PASS",
   "async_canonical_pipeline":"PASS"
  }));
  Ok::<serde_json::Value,Box<dyn std::error::Error>>(receipt)
 }.await.map_err(|error|error.to_string())
}
