// Included only in the existing entrypoint. Observation is never trade authorization.
#[cfg(not(test))]
use spotarb_native_market_core::native_paper_bridge_r1301 as bridge1301;
#[derive(serde::Serialize,serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ObservationManifest1301 {
 schema:u32, runtime:String, real_off:bool, synthetic_policy_permitted:bool,
 duration_ms:u64, identity:isolated_policy::Identity,
}
fn parse_observation_args_r1301(args:&[String],flags:&[&str])->Result<RuntimePlan,String>{
 if !flags.is_empty(){return Err("observation conflicts with legacy flags".into())}
 let mut manifest=None;let mut journal_root=None;
 for arg in args {
  if arg=="--paper-runtime=public-observation"{continue}
  if let Some(p)=arg.strip_prefix("--observation-manifest="){if manifest.replace(PathBuf::from(p)).is_some(){return Err("duplicate observation manifest".into())}}
  else if let Some(p)=arg.strip_prefix("--journal-root="){if journal_root.replace(PathBuf::from(p)).is_some(){return Err("duplicate journal root".into())}}
  else{return Err("unknown observation argument (endpoint overrides forbidden)".into())}
 }
 Ok(RuntimePlan{mode:RuntimeMode::PublicObservation{manifest:manifest.ok_or("missing manifest")?,journal_root:journal_root.ok_or("missing journal root")?}})
}
fn kill_engaged_r1301()->bool{
 #[cfg(test)] if let Some(value)=bridge1301::r1306_test_support::kill_value(){return value}
 let p=PathBuf::from("/home/tmpadmin/spotbot/data/real_kill_switch.json");
 fs::metadata(&p).ok().filter(|m|m.is_file()&&m.len()<=65536)
  .and_then(|_|fs::read(p).ok()).and_then(|b|serde_json::from_slice::<Value>(&b).ok())
  .is_some_and(|v|v["state"]=="ENGAGED"&&v["schema"].as_str().is_some_and(|s|s.starts_with("spotbot-real-kill-switch-")))
}
fn validate_observation_manifest_r1301(m:&ObservationManifest1301)->Result<(),String>{
 if m.schema!=1||m.runtime!="ExecutionIsolated"||!m.real_off{return Err("explicit ExecutionIsolated / REAL OFF required".into())}
 if m.duration_ms==0||m.duration_ms>60_000{return Err("observation duration must be 1..60000 ms".into())}
 m.identity.validate()?;
 Ok(())
}
fn observation_context_r1301(identity:&isolated_policy::Identity,id:String)->isolated_policy::MarketContext{
 isolated_policy::MarketContext{input_id:id,clock_domain:identity.clock_domain.clone(),
 now_ns:spotarb_native_market_core::ingress_runtime_r1205::mono_ns(),
 wall_ms:spotarb_native_market_core::ingress_runtime_r1205::wall_ms(),spot_hot:false,futures_hot:false}
}
fn dispatch_observation_input_r1301(runtime:&mut IsolatedPaperRuntime,input:&bridge1301::ValidatedInput,
 pair:&mut [Option<bridge1301::ValidatedInput>;2],receiver:&bridge1301::BridgeReceiver,
 health:&bridge1301::Health,policy:bool)->Result<(),String>{
 #[cfg(test)] bridge1301::r1306_test_support::checkpoint("dispatch");
 if input.instrument()!=runtime.identity.profile.base||input.clock_domain()!=runtime.identity.clock_domain{return Err("network input identity".into())}
 let trace_id=input.trace_id().to_string();
 let mut timing=input.r1310_timing();
 let sampled=timing.as_ref().is_some_and(|t|t.r1310_trace_sampled);
 let dispatcher_start=sampled.then(spotarb_native_market_core::ingress_runtime_r1205::mono_ns);
 let mut pipeline_trace=if sampled{input.trace().to_vec()}else{Vec::new()};
 if let Err(e)=receiver.revalidate(input){
  let dispatcher_end=dispatcher_start.map(|_|spotarb_native_market_core::ingress_runtime_r1205::mono_ns());
  if let (Some(start),Some(end),Some(t))=(dispatcher_start,dispatcher_end,timing.as_mut()){
   t.dispatcher_start_ns=Some(start);t.dispatcher_end_ns=Some(end);
   pipeline_trace.push(spotarb_native_market_core::r1310_stage_event("Dispatcher",&trace_id,start,end));
  }
  // A validator-backed replacement retires this input; it never grants admission.
  // Keep all other failures sticky, and never clear an existing incident.
  if e!=bridge1301::Denial::Generation||!receiver.superseded(input){health.trip(e.clone());}
  if runtime.actions.len()<128{
   let mut action=json!({"input_id":input.input_id(),"instrument":input.instrument(),"snapshot_generation":input.snapshot_generation(),"admission_decision":"DENIED_BEFORE_PAIR","admission_reason":e,"decision_ns":spotarb_native_market_core::ingress_runtime_r1205::mono_ns()});
   if sampled{action["trace_id"]=json!(trace_id);action["pipeline_trace"]=json!(pipeline_trace);}
   runtime.actions.push(action);
   if sampled{health.record_r1310_trace(false)}
  }else if sampled{health.record_r1310_trace(true)}
  return Ok(())
 }
 let dispatcher_end=dispatcher_start.map(|_|spotarb_native_market_core::ingress_runtime_r1205::mono_ns());
 if let (Some(start),Some(end),Some(t))=(dispatcher_start,dispatcher_end,timing.as_mut()){
  t.dispatcher_start_ns=Some(start);t.dispatcher_end_ns=Some(end);
  pipeline_trace.push(spotarb_native_market_core::r1310_stage_event("Dispatcher",&trace_id,start,end));
 }
 let mut receipt_index=None;
 if input.snapshot_generation().is_some(){
  let owner_start=sampled.then(spotarb_native_market_core::ingress_runtime_r1205::mono_ns);
  let mut receipt=runtime.execution.observe_public_input_r1301a(&input.input_id(),input.instrument()).map_err(|e|format!("{e:?}"))?;
  let owner_end=owner_start.map(|_|spotarb_native_market_core::ingress_runtime_r1205::mono_ns());
  receipt["validated_input"]=serde_json::to_value(input).map_err(|e|e.to_string())?;
  receipt["trace_id"]=json!(trace_id);
  if sampled{
   if let (Some(start),Some(end),Some(t))=(owner_start,owner_end,timing.as_mut()){
    t.owner_observation_start_ns=Some(start);t.owner_observation_end_ns=Some(end);
    let mut owner=spotarb_native_market_core::r1310_stage_event("Owner",&trace_id,start,end);
    owner["effect"]=json!("observation_only");
    owner["end_to_end_ns"]=t.ws_receive_ns.and_then(|receive|end.checked_sub(receive)).map_or(serde_json::Value::Null,|v|json!(v));
    owner["market_age_ms"]=spotarb_native_market_core::ingress_runtime_r1205::wall_ms().checked_sub(input.exchange_ms()).map_or(serde_json::Value::Null,|v|json!(v));
    pipeline_trace.push(json!({"stage":"Entrypoint","trace_id":trace_id,"mono_ns":start}));
    pipeline_trace.push(owner);
   }
  }else{
   pipeline_trace=input.trace().to_vec();pipeline_trace.push(json!({"stage":"Entrypoint"}));pipeline_trace.push(json!({"stage":"Owner","effect":"observation_only"}));
  }
  receipt["pipeline_trace"]=json!(pipeline_trace);
  receipt["entrypoint"]=json!("native_paper_live_portfolio_v1");
  receipt["dispatcher"]=json!("ExecutionIsolated");
  receipt["admission_decision"]=json!("WAITING_FOR_VALIDATED_PAIR");
  if runtime.actions.len()<128{receipt_index=Some(runtime.actions.len());runtime.actions.push(receipt);if sampled{health.record_r1310_trace(false)}}
  else if sampled{health.record_r1310_trace(true)}
 }
 let index=if input.venue()==Venue::OkxSpot{0}else{1};pair[index]=Some(input.clone());
 let (Some(s),Some(f))=(&pair[0],&pair[1])else{return Ok(())};
 #[cfg(test)] bridge1301::r1306_test_support::checkpoint("pair");
 for leg in [s,f]{
  let leg_id=leg.trace_id().to_string();
  let leg_sampled=leg.r1310_timing().is_some_and(|t|t.r1310_trace_sampled);
  let pair_start=leg_sampled.then(spotarb_native_market_core::ingress_runtime_r1205::mono_ns);
  let result=receiver.revalidate(leg);
  let superseded=matches!(&result,Err(bridge1301::Denial::Generation))&&receiver.superseded(leg);
  let pair_end=pair_start.map(|_|spotarb_native_market_core::ingress_runtime_r1205::mono_ns());
  if let (Some(start),Some(end))=(pair_start,pair_end){
   let mut event=spotarb_native_market_core::r1310_stage_event("PairRevalidation",&leg_id,start,end);
   event["result"]=json!(if result.is_ok(){"valid"}else{"denied"});
   if let Err(e)=&result{event["denial_reason"]=json!(format!("{e:?}"));event["superseded"]=json!(superseded);}
   let mut recorded=false;
   if let Some(action)=runtime.actions.iter_mut().find(|r|r["trace_id"]==leg_id){
    if let Some(stages)=action["pipeline_trace"].as_array_mut(){
     if stages.len()<32{stages.push(event);recorded=true;}
    }
   }
   health.record_r1310_trace(!recorded);
  }
  if let Err(e)=result{
   // A validator-backed replacement retires this input; it never grants admission.
   // Keep all other failures sticky, and never clear an existing incident.
   if !superseded{health.trip(e.clone());}
   if let Some(receipt)=receipt_index.and_then(|i|runtime.actions.get_mut(i)){
    receipt["admission_decision"]=json!("DENIED_PAIR_REVALIDATION");receipt["admission_reason"]=json!(e);
    receipt["decision_ns"]=json!(spotarb_native_market_core::ingress_runtime_r1205::mono_ns());
   }
   return Ok(())
  }
 }
 if s.snapshot_generation().is_none()||f.snapshot_generation().is_none(){return Ok(())}
 // R1301 cannot mint confirmed real rules. No pretend attestation from a synthetic profile.
 #[cfg(test)] bridge1301::r1306_test_support::checkpoint("admission");
 let decision=bridge1301::order_admission(health,!runtime.execution.is_fenced(),true,true,kill_engaged_r1301(),policy);
 if let Some(receipt)=receipt_index.and_then(|i|runtime.actions.get_mut(i)){
  receipt["admission_decision"]=json!(if decision.is_ok(){"PERMITTED"}else{"DENIED"});
  receipt["admission_reason"]=json!(decision.as_ref().err());
  receipt["decision_ns"]=json!(spotarb_native_market_core::ingress_runtime_r1205::mono_ns());
 }
 if decision.is_err(){return Ok(())}
 if s.rules_identity().is_none()||f.rules_identity().is_none()||s.rules_identity()!=f.rules_identity(){return Err("rules not bound".into())}
 let mut c=observation_context_r1301(&runtime.identity,input.input_id());c.spot_hot=true;c.futures_hot=true;
 let exchange=if input.venue()==Venue::OkxSpot{"okx"}else{"binance"};
 let rows=runtime.decision.update_at(input.instrument(),exchange,input.book().clone(),c.now_ns,c.wall_ms).map_err(|e|format!("{e:?}"))?;
 process_isolated_pair(runtime,input.venue(),&c,s.book().clone(),f.book().clone(),rows).map_err(|e|e.to_string())
}
async fn run_public_observation_r1301(manifest:PathBuf,journal_root:PathBuf)->Result<(),Box<dyn std::error::Error>>{
 let lab=PathBuf::from("/mnt/spotarb_db_test/r1273_r1275_engine_latency_20261111/R1301/lab").canonicalize()?;
 let manifest=manifest.canonicalize()?;
 if !manifest.starts_with(&lab)||fs::metadata(&manifest)?.len()>65536{return Err("observation manifest confinement".into())}
 let m:ObservationManifest1301=serde_json::from_slice(&fs::read(manifest)?)?;
 validate_observation_manifest_r1301(&m).map_err(offline_adapter_error)?;
 if !kill_engaged_r1301(){return Err("R1301 kill switch not ENGAGED".into())}
 if !runtime_conflicting_flags().is_empty(){return Err("legacy flags denied".into())}
 // Existing provider may select alternate channel/capture profiles through environment.
 // Deny them, rather than mutate process-global environment.
 for key in ["R1246_BOOK_PROFILE","R1268_CAPTURE","R1268_CAPTURE_DIR","R1248_BINANCE_GROUP_SIZE","R21_KILL_SWITCH"]{
  if std::env::var_os(key).is_some(){return Err(format!("observation environment override denied: {key}").into())}
 }
 let clock=format!("linux-monotonic:{}",fs::read_to_string("/proc/sys/kernel/random/boot_id")?.trim());
 if m.identity.clock_domain!=clock{return Err("observation requires current boot CLOCK_MONOTONIC domain".into())}
 let parent=journal_root.parent().ok_or("journal parent")?.canonicalize()?;
 if !parent.starts_with(&lab){return Err("observation journal confinement".into())}
 fs::create_dir_all(&journal_root)?;let journal_root=journal_root.canonicalize()?;
 if !journal_root.starts_with(&lab){return Err("journal symlink escape".into())}
 let runtime=new_isolated_identity_runtime(&m.identity,&journal_root.join("paper-execution.jsonl"))?;
 if runtime.execution.is_fenced(){return Err("owner fenced".into())}
 let duration=Duration::from_millis(m.duration_ms);
 let(receiver,health,provider)=bridge1301::public_observation(m.identity.profile.base.clone(),clock,duration).map_err(offline_adapter_error)?;
 let done=Arc::new(AtomicBool::new(false));let wd_done=done.clone();let wd_health=health.clone();
 let watchdog=tokio::spawn(observation_watchdog_r1304(wd_health,wd_done));
 let worker_health=health.clone();
 let worker=tokio::task::spawn_blocking(move||observation_pump_r1304(runtime,receiver,worker_health,duration,m.synthetic_policy_permitted));
 let result=worker.await;
 done.store(true,Ordering::SeqCst);watchdog.await?;
 let provider_result=provider.await?;
 let mut report=result?.map_err(offline_adapter_error)?;
 report["provider_trace"]=match &provider_result{Ok(trace)=>trace.clone(),Err(error)=>json!({"error":error})};
 fs::write(journal_root.join("observation-result.json"),serde_json::to_vec_pretty(&report)?)?;
 provider_result.map_err(offline_adapter_error)?;
 println!("{}",serde_json::to_string(&report)?);Ok(())
}

async fn observation_watchdog_r1304(wd_health:Arc<bridge1301::Health>,wd_done:Arc<AtomicBool>){
  struct TaskGuard(Arc<bridge1301::Health>);
  impl Drop for TaskGuard { fn drop(&mut self){self.0.trip(bridge1301::Denial::Consumer)} }
  let _guard=TaskGuard(wd_health.clone());
  #[cfg(test)] match wd_health.r1306_task_exit.load(Ordering::SeqCst){1=>panic!("R1306 controlled watchdog panic"),2=>return,_=>{}}
  let mut tick=tokio::time::interval(Duration::from_millis(50));
  while !wd_done.load(Ordering::SeqCst){
   tick.tick().await;
   if !kill_engaged_r1301(){wd_health.trip(bridge1301::Denial::Kill)}
   wd_health.watchdog(spotarb_native_market_core::ingress_runtime_r1205::mono_ns());
  }
}

fn observation_pump_r1304(mut runtime:IsolatedPaperRuntime,receiver:bridge1301::BridgeReceiver,worker_health:Arc<bridge1301::Health>,duration:Duration,policy:bool)->Result<Value,String>{
  let end=std::time::Instant::now()+duration+Duration::from_secs(1);
  let mut pair=[None,None];let mut accepted=0u64;let mut diagnostic_committed=false;
  while std::time::Instant::now()<end {
   if runtime.execution.is_fenced(){worker_health.trip(bridge1301::Denial::Owner)}
   if !worker_health.healthy()&&!diagnostic_committed {
    // Preserve already Closed; never derive Closed from loss of feed.
    if runtime.execution.lifecycle().state!=paper_risk::LifecycleState::Closed {
     let c=observation_context_r1301(&runtime.identity,format!("r1301:incident:{}",runtime.execution.sequence()));
     runtime.execution.control_isolated(IsolatedControl::RecoveryIncident{reason:format!("R1301 {:?}",worker_health.incident())},&c)
      .map_err(|e|{worker_health.trip(bridge1301::Denial::Storage);format!("{e:?}")})?;
    }
    diagnostic_committed=true;
   }
   #[cfg(test)] bridge1301::r1306_test_support::checkpoint("receive");
   match receiver.receive(){
    Ok(Some(input))=>{accepted+=1;if let Err(e)=dispatch_observation_input_r1301(&mut runtime,&input,&mut pair,&receiver,&worker_health,policy){worker_health.trip(bridge1301::Denial::Provider);if runtime.actions.len()<128{runtime.actions.push(json!({"rejected":e}))}}},
    Ok(None)=>{},
    Err(bridge1301::Denial::Closed)=>break,
    Err(e)=>worker_health.trip(e)
   }
   #[cfg(test)] bridge1301::r1306_test_support::checkpoint("iteration");
  }
  if !diagnostic_committed&&runtime.execution.lifecycle().state!=paper_risk::LifecycleState::Closed {
   worker_health.trip(bridge1301::Denial::Closed);
   let c=observation_context_r1301(&runtime.identity,format!("r1301:incident:{}",runtime.execution.sequence()));
   runtime.execution.control_isolated(IsolatedControl::RecoveryIncident{reason:"R1301 bounded observation ended".into()},&c)
    .map_err(|e|{worker_health.trip(bridge1301::Denial::Storage);format!("{e:?}")})?;
  }
  Ok(json!({"schema":1,"mode":"ExecutionIsolated/PublicObservation","validated_inputs":accepted,
   "order_admission":false,"rules_confirmed":false,"snapshot_generation_confirmed":false,"admission_blockers":["REAL_RULES_UNCONFIRMED","SNAPSHOT_GENERATION_UNCONFIRMED"],"orders":runtime.execution.lifecycle().orders.len(),
   "state":runtime.execution.lifecycle().state,"incident":worker_health.incident(),"metrics":worker_health.metrics(),
   "actions":runtime.actions,"functional_validation":"NOT_ATTESTED_BY_COMPILATION"}))
}
