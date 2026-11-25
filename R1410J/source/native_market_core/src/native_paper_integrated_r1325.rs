//! Explicit public-data / simulated-execution integration. No private adapter.
use super::*;
use spotarb_native_market_core as core;
use core::native_r1173_compact_writer::{Writer,Inject,ingress_store_r1205::Store};
use core::market_data_ingress_r1205::{Authority,Output};
use core::pipeline_diagnostics_r1325 as diag;
use std::{sync::{Arc,Mutex},time::{Duration,Instant},collections::BTreeMap};
use std::os::unix::fs::{DirBuilderExt,MetadataExt};
const PUBLIC_RESET_PROVENANCE:&str="PUBLIC_WS_RESET";
const LAB:&str="/mnt/spotarb_db_test/r1273_r1275_engine_latency_20261111/R1301/lab/r1325";
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
 schema:u32, runtime:String, real_off:bool, synthetic_policy_permitted:bool,
 duration_ms:u64, identity:isolated_policy::Identity,
 #[serde(default)] instruments:Vec<isolated_policy::Identity>,
}
fn validate_manifest(m:&Manifest)->Result<(),String>{
 if m.schema!=1||m.runtime!="IntegratedPaper"||!m.real_off||!m.synthetic_policy_permitted{return Err("explicit PAPER-only synthetic policy required".into())}
 if m.duration_ms==0||m.duration_ms>60_000{return Err("initial integrated scope is 1..60000 ms".into())}
 m.identity.validate()?;
 let mut bases=std::collections::BTreeSet::new();
 for id in std::iter::once(&m.identity).chain(&m.instruments){
  id.validate()?;
  if id.run_id!=m.identity.run_id||id.clock_domain!=m.identity.clock_domain||id.exit.is_none()||id.profile.metadata_ttl_ns!=m.identity.profile.metadata_ttl_ns||!bases.insert(id.profile.base.clone()){return Err("R1324A_DUPLICATE_OR_INCOMPATIBLE_PROFILE".into())}
  bridge1301::validate_public_endpoints(&id.profile.base).map_err(|e|format!("{e:?}"))?;
 }
 if m.identity.exit.is_none(){return Err("existing exit rules required".into())}
 bridge1301::validate_public_endpoints(&m.identity.profile.base).map_err(|e|format!("{e:?}"))
}
fn private_dir(path:&std::path::Path)->Result<(),Failure>{
 if !path.exists(){std::fs::DirBuilder::new().mode(0o700).create(path)?;}
 let m=fs::symlink_metadata(path)?;
 if !m.is_dir()||m.file_type().is_symlink()||m.uid()!=unsafe{libc::geteuid()}||m.mode()&0o077!=0{return Err("unsafe private directory".into())}Ok(())
}
fn save(path:&std::path::Path,value:&Value)->Result<(),Failure>{
 use std::os::unix::fs::OpenOptionsExt;
 let mut f=fs::OpenOptions::new().write(true).create_new(true).mode(0o600).custom_flags(libc::O_NOFOLLOW).open(path)?;
 serde_json::to_writer_pretty(&mut f,value)?;f.sync_all()?;fs::File::open(path.parent().ok_or("parent")?)?.sync_all()?;Ok(())
}
// R1326P: bounded process-memory checkpoints, no heap snapshot or private data.
fn memory_checkpoint(root:&std::path::Path,phase:&str,details:serde_json::Value){
 use std::io::Write;
 let mut fields=serde_json::Map::new();
 for (path,prefix) in [("/proc/self/status","status"),("/proc/self/smaps_rollup","smaps")]{
  if let Ok(body)=std::fs::read_to_string(path){for line in body.lines(){
   if let Some((key,value))=line.split_once(':'){
    if matches!(key,"VmRSS"|"VmHWM"|"VmSize"|"VmData"|"VmSwap"|"Threads"|"Rss"|"Pss"|"Private_Clean"|"Private_Dirty"|"Anonymous"|"Swap"){
     fields.insert(format!("{prefix}_{key}"),serde_json::Value::String(value.trim().to_string()));
    }
   }
  }}
 }
 let row=serde_json::json!({"phase":phase,"mono_ns":core::ingress_runtime_r1205::mono_ns(),"memory":fields,"details":details});
 if let Ok(mut f)=std::fs::OpenOptions::new().create(true).append(true).open(root.join("memory-checkpoints.jsonl")){
  let _=serde_json::to_writer(&mut f,&row);let _=f.write_all(b"\n");
 }
}
fn metadata_binding(m:&core::market_metadata::NativeMarketMetadata,p:&isolated_policy::InstrumentProfile)->Result<[String;2],Failure>{
 use core::fill_economics::decimal_to_atoms as atoms;
 if m.metadata_state()!="SIZE_METADATA_READY"||m.base!=p.base||m.spot_symbol!=p.spot_symbol||m.future_symbol!=p.futures_symbol{return Err("current metadata scope/eligibility".into())}
 let step=atoms(&p.step).map_err(offline_adapter_error)?;let min=atoms(&p.minimum_quantity).map_err(offline_adapter_error)?;
 let max=atoms(&p.maximum_quantity).map_err(offline_adapter_error)?;let notional=atoms(&p.minimum_notional).map_err(offline_adapter_error)?;
 for value in [&m.spot_step,&m.future_step_base]{let v=atoms(value.as_deref().ok_or("missing step")?).map_err(offline_adapter_error)?;if v==0||step%v!=0{return Err("profile step does not satisfy current venue rules".into())}}
 for value in [&m.spot_min_amount,&m.future_min_base]{if min<atoms(value.as_deref().ok_or("missing minimum")?).map_err(offline_adapter_error)?{return Err("profile minimum below venue minimum".into())}}
 for value in [&m.spot_max_amount,&m.future_max_base].into_iter().flatten(){if max>atoms(value).map_err(offline_adapter_error)?{return Err("profile maximum exceeds venue maximum".into())}}
 for value in [&m.spot_min_cost,&m.future_min_cost]{if notional<atoms(value).map_err(offline_adapter_error)?{return Err("profile notional below venue minimum".into())}}
 Ok([m.spot_tick.clone().ok_or("spot tick missing")?,m.future_tick.clone().ok_or("future tick missing")?])
}
#[derive(Default,serde::Serialize)]
struct Ledger {selected:u64,rejected_ram:u64,ram_updates:u64,resets:u64,blocked:u64,waiting_pair:u64,candidates:u64,retired_peers:u64,reasons:BTreeMap<String,u64>}
// Only called before canonical source commit or any execution effect for this event.
// Quarantine is a durable rejected-input outcome, never a canonical publication ACK.
fn quarantine_unadmitted(_store:&Arc<Mutex<Store>>,owner:&DurablePaper,revision:u64,_frame:&RawFrame,denial:&bridge1301::Denial)->Result<bool,Failure>{
 if !matches!(denial,bridge1301::Denial::Hot|bridge1301::Denial::Stale){return Ok(false)}
 if owner.sequence()!=revision{return Err("R1325_REJECTION_EXECUTION_REVISION_CHANGED".into())}
 let p=owner.projection();
 if !matches!(owner.lifecycle().state,paper_risk::LifecycleState::Flat|paper_risk::LifecycleState::Closed)||!p.exposure_known||p.pending_orders!=0||p.unknown_orders!=0{return Ok(false)}
 Ok(true)
}
fn observe_committed(pipe:&mut TypedPipeline,r:&mut IsolatedPaperRuntime,input:bridge1301::ValidatedInput,book:core::Book,source:&bridge1301::PaperSource1325,b:&Boundary,writer:&mut Writer,ledger:&mut Ledger)->Result<(),Failure>{
 if pipe.terminal{return Err("terminal typed pipeline".into())}
 let (n,w)=clocks();
 // Retire ONLY independently proven superseded cached witnesses. No unconditional reset.
 for leg in &mut pipe.pair {
  if leg.as_ref().is_some_and(|old|bridge1301::revalidate_r1322(old,source.receiver(),n,w).is_err()&&source.receiver().superseded(old)){
   *leg=None;ledger.retired_peers+=1;
  }
 }
 let started=Instant::now();
 let candidate=pipe.observe_inner(r,input,source.receiver(),source.health(),b,Some(book),source.ready());
 diag::record("OwnerDecisionRisk",started.elapsed().as_nanos().min(u64::MAX as u128) as u64,candidate.is_ok());
 match candidate {
  Ok(Some(c))=>{ledger.candidates+=1;let started=Instant::now();let result=pipe.consume_with_writer(r,c,source.receiver(),source.health(),b,writer,clocks);
   diag::record("ExecutionOwner",started.elapsed().as_nanos().min(u64::MAX as u128) as u64,result.is_ok());result},
  Ok(None)=>{ledger.waiting_pair+=1;Ok(())},
  Err(e)=>{pipe.terminal=true;Err(e)}
 }
}
include!("native_paper_auto_r1325.rs");
pub(crate) async fn run_integrated_r1325(manifest_path:Option<PathBuf>,root:PathBuf)->Result<(),Failure>{
 if !kill_engaged_r1301(){return Err("kill must remain ENGAGED".into())}
 if std::env::var("R1273_PROFILE").as_deref()!=Ok("1")||std::env::var("R1310_TRACE").as_deref()!=Ok("1"){return Err("required bounded source diagnostics not enabled".into())}
 let supplied=if let Some(path)=manifest_path{let raw=fs::read(path)?;if raw.len()>4*1024*1024{return Err("manifest bound".into())}let m:Manifest=serde_json::from_slice(&raw)?;validate_manifest(&m)?;Some(m)}else{None};
 let auto=if supplied.is_none(){Some(Arc::new(AutoPolicy::load()?))}else{None};
 let mut meta=core::market_metadata::fetch_current_metadata(1).map_err(offline_adapter_error)?;
 let m=if let Some(m)=supplied{m}else{auto.as_ref().ok_or("auto policy")?.manifest(&meta,&root)?};
 validate_manifest(&m)?;
 // The dispatcher returns here before legacy config, portfolio or any private adapter construction.
 let parent=root.parent().ok_or("run parent")?.canonicalize()?;
 if parent!=PathBuf::from(LAB).canonicalize()?||root.file_name().and_then(|s|s.to_str())!=Some(&m.identity.run_id){return Err("run root must be dedicated LAB/run_id".into())}
 private_dir(&root)?;let root=root.canonicalize()?;
 if let Some(a)=&auto{if !root.join("auto-policy.json").exists(){save(&root.join("auto-policy.json"),&a.evidence())?;}else{let saved:Value=serde_json::from_slice(&fs::read(root.join("auto-policy.json"))?)?;if saved!=a.evidence(){return Err("R1325_AUTO_RECOVERY_POLICY_MISMATCH".into())}}}
 let identity_file=root.join("universe-identities.json");let resume=identity_file.exists();
 let requested:BTreeMap<String,isolated_policy::Identity>=std::iter::once(&m.identity).chain(&m.instruments).map(|i|(i.profile.base.clone(),i.clone())).collect();
 let mut identities=requested;
 // Only a separately authorized invocation reaches this existing metadata API.
 let eligible:std::collections::BTreeSet<String>=meta.markets.iter().filter(|(_,m)|m.metadata_state()=="SIZE_METADATA_READY").map(|(b,_)|b.clone()).collect();
 if eligible!=identities.keys().cloned().collect(){return Err("R1324A_ELIGIBLE_UNIVERSE_PROFILE_COVERAGE".into())}
 let mut ticks=BTreeMap::new();
 for (base,id) in &mut identities {
  ticks.insert(base.clone(),metadata_binding(meta.markets.get(base).ok_or("metadata symbol")?,&id.profile)?);
  id.profile.metadata_loaded_ns=meta.loaded_ns;
 }
 if !resume{save(&identity_file,&serde_json::to_value(&identities)?)?;}
 let identity=identities.values().next().ok_or("empty eligible universe")?.clone();
 let mut diagnostics=diag::Diagnostics::default();diag::begin("execution-recovery");let started=Instant::now();
 let opened=(||->Result<IsolatedPaperRuntime,Failure>{
  let journal=root.join("execution.jsonl");
  let execution=if resume{DurablePaper::recover_universe_scope_r1324a(&journal,&m.identity.run_id,&m.identity.clock_domain)}else{DurablePaper::create_universe_r1324a(&journal,&identities,meta.generation)}.map_err(offline_adapter_error)?;
  if resume{
   let previous=execution.universe_identities_r1324a();
   for (base,id) in &mut identities{if let Some(old)=previous.get(base){id.profile.metadata_loaded_ns=old.profile.metadata_loaded_ns;}}
   meta.generation=execution.universe_generation_r1324a().ok_or("universe generation missing")?.checked_add(1).ok_or("generation overflow")?;
  }
  let mut config=if let Some(a)=&auto{a.decision()?}else{cfg()};config.metadata_ttl_ms=identity.profile.metadata_ttl_ns/1_000_000;
  let routes=identities.iter().map(|(b,id)|(b.clone(),vec![isolated_route(&id.profile)])).collect();
  let mut decision=DecisionCore::new(0,1,config,HashMap::new()).map_err(offline_adapter_error)?;
  decision.replace_routes(meta.generation,routes).map_err(offline_adapter_error)?;
  Ok(IsolatedPaperRuntime{book_owner:Owner::new(0,1,800).map_err(offline_adapter_error)?,decision,history:NoHistory{accepted:0},execution,identity:identity.clone(),frames_seen:0,actions:Vec::new(),seen_inputs:HashMap::new()})
 })();
 diag::record("RecoveryExecution",started.elapsed().as_nanos().min(u64::MAX as u128) as u64,opened.is_ok());diagnostics.merge(diag::finish());
 let mut runtime=opened?;
 let canonical=std::env::temp_dir().join(format!("spotarb_r1173_r1325_{}",identity.run_id));
 if !resume&&canonical.exists(){return Err("canonical path already exists for new run".into())}private_dir(&canonical)?;
 let mut writer=if resume{Writer::resume(&canonical)?}else{Writer::create(&canonical)?};
 let inbox=root.join("inbox");private_dir(&inbox)?;let store=Arc::new(Mutex::new(Store::open(&inbox)?));
 store.lock().map_err(|_|"inbox poison")?.bind_canonical(&canonical)?;
 diag::begin("startup-recovery");let started=Instant::now();
 let recovery=(||->Result<(),Failure>{
  writer.recover_ingress_source_only()?;
  // Never cold-deny a source that may already have reached the execution journal.
  writer.reconcile_execution_completion_r1324a(&runtime.execution)?;
  writer.require_decisions_drained()?;
  let records=runtime.execution.r1326_records_after(0).map_err(offline_adapter_error)?;
  let mut legacy_recovery_required=Vec::new();
  let mut discovered_sequences=std::collections::BTreeSet::new();
  for (base,id) in &identities {
   let trade_id=super::trade_id_r1326(id)?;
   let intent_context=writer.trade_intent_context_r1326(&trade_id)?;
   let has_intent=intent_context.is_some();
   let identity_records=records.iter().filter(|(_,record)|super::record_matches_identity_r1326(id,record)).collect::<Vec<_>>();
   discovered_sequences.extend(identity_records.iter().map(|(sequence,_)|*sequence));
   let legacy_records=identity_records.iter().filter(|(_,record)|super::record_has_order_obligation_r1326(record)).collect::<Vec<_>>();
   if !has_intent&&!legacy_records.is_empty(){
    for (sequence,record) in &legacy_records{
     writer.record_legacy_execution_recovery_r1326(&runtime.execution,*sequence,(*record).clone(),Inject::None)?;
    }
    legacy_recovery_required.push(format!("{base}:{} verified R1289 order records",legacy_records.len()));
    continue
   }
   if !has_intent{continue}
   let (command_key,metadata_generation,owner_generation,owner_revision,decision_id)=intent_context.as_ref().ok_or("R1326 canonical intent context missing")?;
   let trade_records=super::trade_records_r1326(id,records.clone(),std::collections::BTreeSet::new(),false,Some((command_key.as_str(),*metadata_generation,*owner_generation,*owner_revision,decision_id.as_str())))?;
   for (sequence,record) in trade_records {
    let witness=super::trade_event_witness_r1326(id,sequence,record)?;
    writer.commit_trade_event_r1326(witness,Inject::None)?;
   }
  }
  let active_trade_ids=identities.iter().map(|(_,id)|super::trade_id_r1326(id)).collect::<Result<std::collections::BTreeSet<_>,_>>()?;
  for trade_id in writer.trade_intent_ids_r1326()? {
   if !active_trade_ids.contains(&trade_id){legacy_recovery_required.push(format!("removed-identity canonical TRADE_INTENT requires owner census: {trade_id}"));}
  }
  let orphan_obligations=records.iter().filter(|(sequence,record)|
   !discovered_sequences.contains(sequence)&&super::record_has_order_obligation_r1326(record)).collect::<Vec<_>>();
  for (sequence,record) in &orphan_obligations{
   writer.record_legacy_execution_recovery_r1326(&runtime.execution,*sequence,(*record).clone(),Inject::None)?;
  }
  if !orphan_obligations.is_empty(){
   legacy_recovery_required.push(format!("unknown-or-removed-identity:{} verified R1289 order records",orphan_obligations.len()));
  }
  if !legacy_recovery_required.is_empty(){
   return Err(format!("R1326_LEGACY_RECOVERY_REQUIRED:{}",legacy_recovery_required.join("," )).into())
  }
  if !runtime.execution.exposure_view().known{return Err("execution recovery exposure unknown".into())}
  core::market_data_ingress_r1205::reconcile(&store,&writer)?;
  writer.sweep_ingress_receipts(&mut *store.lock().map_err(|_|"inbox poison")?)?;
  if resume{runtime.execution.replace_universe_r1324a(&identities,meta.generation).map_err(offline_adapter_error)?;}Ok(())
 })();
 diag::record("Recovery",started.elapsed().as_nanos().min(u64::MAX as u128) as u64,recovery.is_ok());diagnostics.merge(diag::finish());
 if let Err(e)=recovery {save(&root.join(format!("recovery-blocked-{}.json",core::ingress_runtime_r1205::mono_ns())),&json!({"error":e.to_string(),"diagnostics":diagnostics,"runtime":"NO_GO"}))?;return Err(e)}
 // Generation replacement occurs before constructing any source task, provider or pair cache.
 let metadata_record=json!({"generation":meta.generation,"captured_wall_ms":meta.loaded_wall_ms,"universe":meta.bases(),"routes":meta.canonical_control_value(),"profiles":identities,"ticks":ticks});
 save(&root.join(format!("startup-{}.json",core::ingress_runtime_r1205::mono_ns())),&json!({"metadata":metadata_record,"metadata_sha256":isolated_policy::digest(&metadata_record)?,"canonical":canonical,"real_off":true,"private_adapter_constructed":false,"scope":"WHOLE_ELIGIBLE_UNIVERSE_ONE_EXECUTION_OWNER"}))?;
 let authority:Authority=Default::default();
 let mut sources:BTreeMap<String,bridge1301::PaperSource1325>=identities.keys().map(|b|(b.clone(),bridge1301::PaperSource1325::new(authority.clone(),identity.clock_domain.clone()))).collect();
 let mut owners=(0..12).map(|i|Owner::new(i,12,800).map_err(offline_adapter_error)).collect::<Result<Vec<_>,_>>()?;
 let mut pipes:BTreeMap<String,TypedPipeline>=identities.keys().map(|b|(b.clone(),{let mut p=TypedPipeline::new();p.auto=auto.clone();p})).collect();let mut ledger=Ledger::default();
 let mut symbol_counts:BTreeMap<String,[u64;3]>=identities.keys().map(|b|(b.clone(),[0;3])).collect();
 let(tx,mut rx)=tokio::sync::mpsc::channel(core::ingress_runtime_r1205::R1205_OUTPUT_QUEUE_CAPACITY);
 let bases:Vec<String>=identities.keys().cloned().collect();
 let mut ticker_runs=if let Some(a)=&auto{Some(turnover::start(a.turnover.clone(),&bases,meta.generation,m.duration_ms)?)}else{None};
 let groups=core::native_r167_source_ws_shards_review::split_source_groups_for_ingress(&[bases],core::ingress_runtime_r1205::R1205_MAX_BASES_PER_SHARD)?;
 let mut runs=Vec::new();
 for (shard,bases) in groups.into_iter().enumerate(){
  let cfg=core::ingress_runtime_r1205::Config{shard,bases,copies:2,duration:Duration::from_millis(m.duration_ms),controlled_close_after:None,endpoint_variant:0,probe_rotation_after:None};
  runs.push(tokio::spawn(core::ingress_runtime_r1205::run_r1246_volatile(cfg,authority.clone(),tx.clone())));
 }
 drop(tx);
 let mut inflight_base:Option<String>=None;
 let mut failure_stage:&'static str="receive_loop";
 let processing=async {
  loop {
   if !kill_engaged_r1301(){return Err::<(),Failure>("kill changed during PAPER".into())}
   let output=match tokio::time::timeout(Duration::from_millis(100),rx.recv()).await {
    Ok(Some(v))=>v,Ok(None)=>break,Err(_)=>{
     for source in sources.values(){if source.ready(){source.health().heartbeat();source.health().watchdog(core::ingress_runtime_r1205::mono_ns());if !source.health().healthy(){return Err("source watchdog".into())}}}
     continue
    }
   };
   match output {
    Output::Reset{venue,base,epoch}=>{
     failure_stage="source_reset_authority";
     let source=sources.get_mut(&base).ok_or("R1324A_RESET_UNKNOWN_SYMBOL")?;
     let pipe=pipes.get_mut(&base).ok_or("R1324A_PIPE_UNKNOWN_SYMBOL")?;
     source.reset(venue,&base,epoch).map_err(offline_adapter_error)?;
     diag::begin("source-reset");let started=Instant::now();
     failure_stage="source_reset_durable";
     writer.source_reset(&mut owners,venue,&[base.clone()],epoch,core::ingress_runtime_r1205::wall_ms(),PUBLIC_RESET_PROVENANCE,Inject::None)?;
     failure_stage="source_reset_decision";
     runtime.decision.reset_exchange(&base,if venue==Venue::OkxSpot{"okx"}else{"binance"},epoch).map_err(offline_adapter_error)?;
     pipe.pair[if venue==Venue::OkxSpot{0}else{1}]=None;ledger.resets+=1;
     diag::record("GenerationReset",started.elapsed().as_nanos().min(u64::MAX as u128) as u64,true);diagnostics.merge(diag::finish());
    },
    Output::Blocked{..}=>{ledger.blocked+=1;*ledger.reasons.entry("SOURCE_HOT_BLOCKED".into()).or_default()+=1;},
    Output::Resubscribe{..}=>return Err("unexpected external resubscribe".into()),
    output=>{
     failure_stage="frame_parse";
     ledger.selected+=1;let n=core::ingress_runtime_r1205::mono_ns();let w=core::ingress_runtime_r1205::wall_ms();
     let trace=match &output{Output::Frame(f)|Output::Book(f,_)=>core::market_data_ingress_r1205::frame_trace_id_r1304(f),_=>return Err("non-frame source output".into())};
     diag::begin(&trace);let started=Instant::now();
     let base=match &output{Output::Book(_,u)=>u.base.clone(),Output::Frame(f)=>core::parse(f).map_err(offline_adapter_error)?.base,_=>return Err("source output kind".into())};
     let source=sources.get_mut(&base).ok_or("R1324A_EVENT_UNKNOWN_SYMBOL")?;
     let pipe=pipes.get_mut(&base).ok_or("R1324A_EVENT_UNKNOWN_PIPE")?;
     diag::context(diag::Context{venue:match &output{Output::Frame(f)|Output::Book(f,_)=>format!("{:?}",f.venue),_=>String::new()},symbol:base.clone(),owner:market_owner_index(&base,12),generation:meta.generation,queue_occupied_slots_at_enqueue:match &output{Output::Frame(f)|Output::Book(f,_)=>f.source.as_ref().and_then(|s|s.r1279_timing.as_ref()).and_then(|t|t.r1324a_output_occupied_slots),_=>None}});
     let count=symbol_counts.get_mut(&base).ok_or("symbol counters")?;count[0]+=1;inflight_base=Some(base.clone());
     runtime.execution.select_instrument_r1324a(&base,meta.generation).map_err(offline_adapter_error)?;
     runtime.identity=identities.get(&base).ok_or("symbol identity")?.clone();
     let unadmitted=match &output{Output::Frame(f)|Output::Book(f,_)=>f.clone(),_=>return Err("source output kind".into())};
     let revision=runtime.execution.sequence();failure_stage="provider_accept";let validated=source.accept_observation(output,n,w);
     diag::record("ProviderAdmission",started.elapsed().as_nanos().min(u64::MAX as u128) as u64,validated.is_ok());
     let input=match validated{Ok(input)=>input,Err(denial)=>{
      writer.require_decisions_drained()?;
      let quarantine_start=Instant::now();
      if quarantine_unadmitted(&store,&runtime.execution,revision,&unadmitted,&denial)?{
       ledger.rejected_ram+=1;*ledger.reasons.entry(format!("R1325_UNADMITTED_{denial:?}")).or_default()+=1;
       count[2]+=1;inflight_base=None;pipe.pair=[None,None];
       diag::record("SelectedQuarantine",quarantine_start.elapsed().as_nanos().min(u64::MAX as u128) as u64,true);diagnostics.merge(diag::finish());continue
      }
      return Err(offline_adapter_error(denial).into())
     }};
     if trace!=input.trace_id(){return Err("trace identity changed at provider".into())}
     if let Some(t)=input.r1310_timing(){
      for(stage,start,end)in [("WSDecode",t.ws_decode_start_ns,t.ws_decode_end_ns),("Validator",t.validator_start_ns,t.validator_end_ns),("BookReconstruction",t.book_start_ns,t.book_end_ns),("SourceSelection",t.source_selection_start_ns,t.source_selection_end_ns),("Provider",t.provider_start_ns,t.provider_end_ns),("IngressRAM",t.validation_start_ns,t.validation_end_ns),("ReaderQueueResidence",t.reader_enqueue_ns,t.reader_dequeue_ns),("CentralQueueResidence",t.central_enqueue_ns,t.central_dequeue_ns)]{
       if let Some(ns)=start.zip(end).and_then(|(a,b)|b.checked_sub(a)){diag::record(stage,ns,true)}
      }
     }
     let(frame,update)=input.owner_parts_r1322();let idx=market_owner_index(&update.base,12);
     // received_ns is frame age, not queue residence; retain correct naming.
     diag::record("SelectedFrameAge",n.saturating_sub(frame.received_ns),true);
     let enqueued=frame.source.as_ref().and_then(|s|s.r1279_timing.as_ref()).and_then(|t|t.r1325_output_enqueued_ns).ok_or("selected enqueue timestamp absent")?;
     let residence=n.checked_sub(enqueued).ok_or("selected queue clock regression")?;
     diag::record("SelectedQueueResidence",residence,true);
     failure_stage="ram_market_owner";
     let started=Instant::now();
     owners[idx].ingest_parsed(&frame,update,&mut core::history::PreparedSignal).map_err(offline_adapter_error)?;
     diag::record("ProcessingAge",core::ingress_runtime_r1205::mono_ns().saturating_sub(n),true);
     ledger.ram_updates+=1;diag::record("RamMarketOwner",started.elapsed().as_nanos().min(u64::MAX as u128) as u64,true);
     let book=owners[idx].snapshot_venue(input.instrument(),input.venue()).ok_or("RAM Owner book absent")?;
     let boundary=Boundary{identity:runtime.identity.clone(),metadata_generation:meta.generation,price_ticks:ticks.get(&base).ok_or("symbol ticks")?.clone(),real_off:true,kill_engaged:kill_engaged_r1301(),storage_healthy:true};
     failure_stage="typed_pipeline";observe_committed(pipe,&mut runtime,input.clone(),book,source,&boundary,&mut writer,&mut ledger)?;
     if runtime.execution.is_fenced(){return Err("execution uncertain commit".into())}
     for action in &runtime.actions{
      if let Some(reason)=action["risk_result"].as_str(){let key=if ledger.reasons.len()<128||ledger.reasons.contains_key(reason){reason}else{"DIAGNOSTIC_REASON_OVERFLOW"};*ledger.reasons.entry(key.into()).or_default()+=1;}
      if action["action"]=="auto_exit_requested"{*ledger.reasons.entry("AUTO_EXIT_REQUESTED".into()).or_default()+=1;}

      if let Some(rows)=action["rows"].as_array(){for row in rows{for field in ["reason","risk_result"]{
       if let Some(reason)=row[field].as_str(){let key=if ledger.reasons.len()<128||ledger.reasons.contains_key(reason){reason}else{"DIAGNOSTIC_REASON_OVERFLOW"};*ledger.reasons.entry(key.into()).or_default()+=1;}
      }}}
     }
     diag::record("EventProcessingTotal",core::ingress_runtime_r1205::mono_ns().saturating_sub(n),true);
     count[1]+=1;inflight_base=None;runtime.actions.clear();diagnostics.merge(diag::finish());
    }
   }
  }Ok(())
 }.await;
 diagnostics.merge(diag::finish());
 // Errors stop only this newly created public task. No service/supervisor interaction.
 if processing.is_err(){if let Some(base)=&inflight_base{if let Some(count)=symbol_counts.get_mut(base){count[2]+=1;}}*ledger.reasons.entry("PIPELINE_FAIL_CLOSED".into()).or_default()+=1;for run in &runs{run.abort();}}
 memory_checkpoint(&root,"processing_complete",json!({"selected":ledger.selected,"candidates":ledger.candidates}));
 let error=processing.as_ref().err().map(|e|e.to_string());
 let error_stage=if error.is_some(){Some(failure_stage)}else{None};
 let mut source_ok=true;let mut summary=Vec::new();
 for run in runs {match run.await {Ok(Ok(v))=>summary.push(serde_json::to_value(v)?),Ok(Err(e))=>{source_ok=false;summary.push(json!({"error":e}));},Err(e)=>{source_ok=false;summary.push(json!({"join_error":e.to_string()}));}}}
 let source_shards=summary.len();
 let readiness_rows=summary.iter().map(|x|x["readiness_audit"].as_array().map_or(0,|v|v.len())).sum::<usize>();
 let stale_frame_audit_rows=summary.iter().map(|x|x["stale_frame_audit"].as_array().map_or(0,|v|v.len())).sum::<usize>();
 memory_checkpoint(&root,"source_summaries_materialized",json!({"shards":source_shards,"readiness_rows":readiness_rows,"stale_frame_audit_rows":stale_frame_audit_rows}));
 if let Some(t)=&mut ticker_runs{t.stop().await;}
 let counts=store.lock().map_err(|_|"inbox poison")?.counts()?;
 let trace_retention=store.lock().map_err(|_|"inbox poison")?.frame_trace_retention_r1325()?;
 let residual=ledger.selected as i128-symbol_counts.values().map(|c|c[1]+c[2]).sum::<u64>() as i128;
 let mut report=json!({"mode":"IntegratedPaper","automatic":auto.is_some(),"turnover":auto.as_ref().map(|a|a.turnover.report()),"runtime_pass":false,"error":error,"error_stage":error_stage,"ledger":ledger,"selected_unfinished":residual,
  "symbol_counter_columns":["selected","processed","rejected"],"symbol_counts":symbol_counts,"unexplained_selected_residual":ledger.selected as i128-symbol_counts.values().map(|c|c[1]+c[2]).sum::<u64>() as i128,"universe_lifecycle":runtime.execution.universe_states_r1324a(),"universe_positions":runtime.execution.universe_projection_r1324a(),"source_counts":counts,"trace_retention":trace_retention,"canonical":writer.source_custody_counts(),"execution_sequence":runtime.execution.sequence(),"projection":runtime.execution.projection(),
  "lifecycle":runtime.execution.lifecycle(),"diagnostics":diagnostics,"private_adapter_constructed":false,"real_orders":0});
 report["source_pipeline"]=Value::Array(summary);
 let report_path=root.join(format!("report-{}.json",core::ingress_runtime_r1205::mono_ns()));
 memory_checkpoint(&root,"report_materialized",json!({"source_shards":source_shards,"readiness_rows":readiness_rows,"stale_frame_audit_rows":stale_frame_audit_rows,"output":"streaming"}));
 save(&report_path,&report)?;
 let report_bytes=fs::metadata(&report_path)?.len();
 memory_checkpoint(&root,"report_written",json!({"report_bytes":report_bytes}));
 processing?;writer.verify_pair()?;writer.require_decisions_drained()?;
 if !source_ok{return Err("public source task failed; inspect preserved report".into())}
 if residual!=0||counts.pending!=0{return Err("unreconciled selected/legacy pending events".into())}
 if runtime.execution.universe_states_r1324a().values().any(|state|!matches!(state,paper_risk::LifecycleState::Flat|paper_risk::LifecycleState::Closed)){return Err("bounded session ended with lifecycle requiring recovery".into())}Ok(())
}
#[cfg(test)]
mod tests {
 use super::*;
 #[test]fn r1325_actual_public_reset_is_durable_and_accepted(){
  let root=std::env::temp_dir().join(format!("spotarb_r1173_r1325_reset_{}_{}",std::process::id(),core::ingress_runtime_r1205::mono_ns()));
  private_dir(&root).unwrap();let mut writer=Writer::create(&root).unwrap();
  let mut owners=(0..12).map(|i|Owner::new(i,12,800).unwrap()).collect::<Vec<_>>();
  writer.source_reset(&mut owners,Venue::OkxSpot,&["AAA".into()],1,core::ingress_runtime_r1205::wall_ms(),PUBLIC_RESET_PROVENANCE,Inject::None).unwrap();
  writer.verify_pair().unwrap();writer.require_decisions_drained().unwrap();
 }
 #[cfg(feature="offline-test-clock")]
 #[test]fn r1410d_integrated_first_leg_and_actual_source_switch(){
  use super::super::tests::{setup,boundary};
  use bridge1301::engine_fixture::raw;
  let(mut f,mut r,mut pipe)=setup();
  let first=f.inputs.iter().find(|i|i.venue()==Venue::BinanceFutures).unwrap().clone();
  let spot=f.inputs.iter().find(|i|i.venue()==Venue::OkxSpot).unwrap().clone();
  let b=boundary(&r);
  let root=std::env::temp_dir().join(format!("spotarb_r1173_r1410d_{}_{}",std::process::id(),core::ingress_runtime_r1205::mono_ns()));
  private_dir(&root).unwrap();let mut writer=Writer::create(&root).unwrap();
  let mut owners=(0..12).map(|i|Owner::new(i,12,800).unwrap()).collect::<Vec<_>>();
  let mut ledger=Ledger::default();
  let(n,w)=clocks();
  set_clock(n-900_000_000,w-900);
  let mut source=bridge1301::PaperSource1325::new(f.engine.authority.clone(),first.clock_domain().into());
  source.health().set_test_clock(clocks);
  set_clock(n,w);
  let mut process=|output:Output,source:&mut bridge1301::PaperSource1325,r:&mut IsolatedPaperRuntime,pipe:&mut TypedPipeline|{
   let(n,w)=clocks();
   match output{
    Output::Reset{venue,base,epoch}=>{
     source.reset(venue,&base,epoch).unwrap();
     writer.source_reset(&mut owners,venue,&[base.clone()],epoch,w,PUBLIC_RESET_PROVENANCE,Inject::None).unwrap();
     r.decision.reset_exchange(&base,if venue==Venue::OkxSpot{"okx"}else{"binance"},epoch).unwrap();
     pipe.pair[if venue==Venue::OkxSpot{0}else{1}]=None;ledger.resets+=1;
    },
    Output::Blocked{..}=>{},
    Output::Frame(_)|Output::Book(_,_)=>{
     let input=source.accept_observation(output,n,w).unwrap();
     let(frame,update)=input.owner_parts_r1322();let idx=market_owner_index(&update.base,12);
     owners[idx].ingest_parsed(&frame,update,&mut core::history::PreparedSignal).unwrap();
     let book=owners[idx].snapshot_venue(input.instrument(),input.venue()).unwrap();
     observe_committed(pipe,r,input,book,source,&b,&mut writer,&mut ledger).unwrap();
    },
    _=>panic!("unexpected test output"),
   }
  };
  let(frame,update)=first.owner_parts_r1322();
  let old_epoch=frame.epoch;let old_session=first.source_provenance().session_id.clone();
  process(Output::Reset{venue:Venue::BinanceFutures,base:"AAA".into(),epoch:old_epoch},&mut source,&mut r,&mut pipe);
  process(Output::Book(frame,update),&mut source,&mut r,&mut pipe);
  assert!(!source.ready());assert!(source.health().healthy());assert_eq!(r.frames_seen,1);
  assert_eq!(r.actions.last().unwrap()["observation_only"],json!(true));
  assert_eq!(r.decision.exchange_count("AAA"),1);
  assert_eq!(pipe.last_observed[1].as_ref().unwrap().0,old_epoch);
  r.actions.clear();
  advance_clock_ns(1_000_000);let(n,w)=clocks();
  let disconnected=f.engine.disconnected(&old_session,n,w).unwrap();
  for output in disconnected{process(output,&mut source,&mut r,&mut pipe);}
  let survivor=if old_session=="binance-a"{"binance-b"}else{"binance-a"};
  f.engine.register_connection("binance-c",Venue::BinanceFutures,1).unwrap();f.engine.mark_subscribed("binance-c");
  let mut selected=0;
  for seq in 103..=108i64{
   advance_clock_ns(100_000_000);let(n,w)=clocks();
   let frames=[survivor,"binance-c"].into_iter().map(|session|raw(Venue::BinanceFutures,session,"AAA",n,w,seq,true)).collect();
   let outputs=f.engine.receive_batch(frames,n,w).unwrap();
   for output in outputs{
    if matches!(&output,Output::Frame(_)|Output::Book(_,_)){selected+=1;}
    process(output,&mut source,&mut r,&mut pipe);
   }
   r.actions.clear();
  }
  assert!(selected>0);assert!(!source.ready());assert!(!pipe.terminal);
  assert_eq!(r.frames_seen,1+selected);
  assert!(pipe.last_observed[1].as_ref().unwrap().0>old_epoch);
  assert_ne!(pipe.last_observed[1].as_ref().unwrap().2,old_session);
  let(n,w)=clocks();
  assert!(bridge1301::observation_r1410(&first,source.receiver(),n,w).is_err());
  // Other venue's first book is still fresh, but must now use its actual
  // original selected authority. The replacement future bids100, ask100
  // spot: no executable entry, no dispatch.
  advance_clock_ns(1_000_000);
  let(frame,update)=spot.owner_parts_r1322();
  process(Output::Reset{venue:Venue::OkxSpot,base:"AAA".into(),epoch:frame.epoch},&mut source,&mut r,&mut pipe);
  process(Output::Book(frame,update),&mut source,&mut r,&mut pipe);
  assert!(source.ready());assert!(source.health().healthy());assert!(!pipe.terminal);
  assert_eq!(r.frames_seen,2+selected);assert_eq!(r.decision.exchange_count("AAA"),2);
  assert!(r.execution.lifecycle().orders.is_empty());
  drop(process);assert_eq!(ledger.candidates,0);assert_eq!(ledger.resets,3);
  writer.verify_pair().unwrap();writer.require_decisions_drained().unwrap();
  println!("R1410D_WITNESS actual integrated Owner/Decision: first leg after900ms startup, real Engine source switch, selected={selected}, decisions={}, resets3, old generation denied, orders0",r.frames_seen);
 }
 #[test]fn r1325_manifest_rejects_real_and_missing_exit(){
  let v:Value=serde_json::from_str(include_str!("../fixtures/r1298/full_exit.json")).unwrap();
  let id:isolated_policy::Identity=serde_json::from_value(v["identity"].clone()).unwrap();
  let mut m=Manifest{schema:1,runtime:"IntegratedPaper".into(),real_off:false,synthetic_policy_permitted:true,duration_ms:1000,identity:id,instruments:Vec::new()};
  assert!(validate_manifest(&m).is_err());m.real_off=true;m.identity.exit=None;assert!(validate_manifest(&m).is_err());
 }
 #[test]fn r1325_unknown_mode_and_args_denied(){
  assert!(parse_runtime_plan(&["--paper-runtime=integrated-paper".into(),"--endpoint=private".into()],&[]).is_err());
  assert!(parse_runtime_plan(&["--paper-runtime=integrated-paper".into(),"--integration-manifest=x".into(),"--journal-root=y".into()],&["REAL"]).is_err());
 }
}
#[cfg(test)] mod quarantine_tests {
 use super::*;
 #[test] fn r1326_unadmitted_hot_stays_ram_only(){
  let n=core::ingress_runtime_r1205::mono_ns();let run=format!("r1326-ram-reject-{}-{n}",std::process::id());
  let root=PathBuf::from(LAB).join(&run);private_dir(&root).unwrap();let inbox=root.join("inbox");private_dir(&inbox).unwrap();
  let v:Value=serde_json::from_str(include_str!("../fixtures/r1298/full_exit.json")).unwrap();let mut id:isolated_policy::Identity=serde_json::from_value(v["identity"].clone()).unwrap();id.run_id=run;
  let owner=DurablePaper::create_universe_r1324a(root.join("execution.jsonl"),&BTreeMap::from([(id.profile.base.clone(),id)]),1).unwrap();
  let store=Arc::new(Mutex::new(Store::open(&inbox).unwrap()));
  let frame=RawFrame{venue:Venue::OkxSpot,epoch:1,received_ns:n,received_wall_ms:core::ingress_runtime_r1205::wall_ms(),raw:String::new(),source:None};
  let revision=owner.sequence();let denial=bridge1301::Denial::Hot;
  assert!(quarantine_unadmitted(&store,&owner,revision,&frame,&denial).unwrap());
  let counts=store.lock().unwrap().counts().unwrap();
  assert_eq!((counts.accepted,counts.quarantined,counts.published,counts.pending),(0,0,0,0));
 }
}
