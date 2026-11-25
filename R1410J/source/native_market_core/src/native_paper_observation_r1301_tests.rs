// Actual entrypoint dispatch tests with explicitly synthetic validated-contract fixtures.
// These do not claim socket/Engine E2E validation; no tests were run by Astra.
use super::*;
fn batch1301()->OfflineMarketBatch{serde_json::from_str(include_str!("../fixtures/r1298/full_exit.json")).unwrap()}
fn root1301()->PathBuf{
 let n=SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
 PathBuf::from(format!("/mnt/spotarb_db_test/r1273_r1275_engine_latency_20261111/R1301/lab/tests/{}-{n}/paper.jsonl",std::process::id()))
}
fn setup1301()->(IsolatedPaperRuntime,PathBuf){
 let b=batch1301();let p=root1301();(new_isolated_identity_runtime(&b.identity,&p).unwrap(),p)
}
fn observed1301(r:&mut IsolatedPaperRuntime){
 let(inputs,receiver,health)=bridge1301::fixture_pair_for_test(&r.identity.clock_domain);
 let mut pair=[None,None];
 for input in &inputs{dispatch_observation_input_r1301(r,input,&mut pair,&receiver,&health,true).unwrap()}
}
#[test] fn r1301_19_private_endpoint_argument(){
 assert!(parse_runtime_plan(&["--paper-runtime=public-observation".into(),"--endpoint=https://private/order".into()],&[]).is_err());
 let b=batch1301();
 let v=json!({"schema":1,"runtime":"ExecutionIsolated","real_off":true,"synthetic_policy_permitted":true,"duration_ms":1000,"identity":b.identity,"private_endpoint":"https://private/order"});
 assert!(serde_json::from_value::<ObservationManifest1301>(v).is_err());
}
#[test] fn r1301_24_actual_dispatch_no_legacy(){
 let(mut r,_)=setup1301();observed1301(&mut r);
 assert_eq!(r.history.accepted,0);assert_eq!(r.frames_seen,0);assert!(r.execution.lifecycle().orders.is_empty());
 assert!(matches!(parse_runtime_plan(&[],&[]).unwrap().mode,RuntimeMode::Legacy));
 assert!(matches!(parse_runtime_plan(&["--paper-runtime=public-observation".into(),"--observation-manifest=x".into(),"--journal-root=y".into()],&[]).unwrap().mode,RuntimeMode::PublicObservation{..}));
}
#[test] fn r1301_25_recovery_preserved(){
 let(mut r,_)=setup1301();let c=observation_context_r1301(&r.identity,"test-recovery".into());
 r.execution.control_isolated(IsolatedControl::RecoveryIncident{reason:"test".into()},&c).unwrap();
 let before=r.execution.projection().clone();observed1301(&mut r);
 assert_eq!(r.execution.lifecycle().state,paper_risk::LifecycleState::RecoveryRequired);
 assert_eq!(r.execution.projection().spot_base,before.spot_base);assert_eq!(r.execution.projection().futures_base,before.futures_base);
}
#[test] fn r1301_26_second_owner(){
 let(r,p)=setup1301();assert!(new_isolated_identity_runtime(&r.identity,&p).is_err());drop(r);
}
#[test] fn r1301_27_restart_pending(){
 let(mut r,p)=setup1301();let b=batch1301();
 for i in &b.inputs{process_isolated_input(&mut r,i).unwrap();if !r.execution.lifecycle().orders.is_empty(){break}}
 assert_eq!(r.execution.lifecycle().orders.len(),1);assert!(r.execution.lifecycle().processed_events.is_empty());
 let id=r.execution.lifecycle().orders.keys().next().unwrap().clone();let remaining=r.execution.lifecycle().orders[&id].remaining_quantity;
 let identity=r.identity.clone();drop(r);let mut r=new_isolated_identity_runtime(&identity,&p).unwrap();observed1301(&mut r);
 assert_eq!(r.execution.lifecycle().orders.len(),1);assert_eq!(r.execution.lifecycle().orders[&id].remaining_quantity,remaining);
 assert_eq!(r.execution.lifecycle().state,paper_risk::LifecycleState::RecoveryRequired);
}
#[test] fn r1301_29_lost_frames_never_close(){
 let(mut r,_)=setup1301();let c=observation_context_r1301(&r.identity,"no-feed".into());
 r.execution.control_isolated(IsolatedControl::RecoveryIncident{reason:"no frames".into()},&c).unwrap();
 assert_ne!(r.execution.lifecycle().state,paper_risk::LifecycleState::Closed);assert!(r.execution.lifecycle().processed_events.is_empty());
}
#[test] fn r1301_30_actual_observation_no_orders(){
 let(mut r,_)=setup1301();let before=r.execution.sequence();observed1301(&mut r);
 assert_eq!(r.execution.sequence(),before);assert!(r.execution.lifecycle().orders.is_empty());assert!(r.execution.lifecycle().processed_events.is_empty());
}
#[test] fn r1301_32_no_unregistered_fill(){
 let(mut r,_)=setup1301();let(inputs,_,_)=bridge1301::fixture_pair_for_test(&r.identity.clock_domain);
 let c=observation_context_r1301(&r.identity,"unknown-order".into());
 assert!(r.execution.simulate_isolated_order("unregistered",inputs[0].book(),inputs[1].book(),&c).is_err());
 assert!(r.execution.lifecycle().processed_events.is_empty());assert!(r.execution.lifecycle().orders.is_empty());
}

#[test] fn r1301a_actual_engine_bridge_entrypoint_owner(){
 let(mut runtime,path)=setup1301();
 let before=runtime.execution.sequence();
 let mut fixture=bridge1301::engine_fixture::EngineFixture::new(&runtime.identity.clock_domain);
 fixture.qualify();
 let mut pair=[None,None];
 for _ in 0..2{
  let input=fixture.dequeue();
  dispatch_observation_input_r1301(&mut runtime,&input,&mut pair,&fixture.receiver,&fixture.health,true).unwrap();
 }
 let receipts:Vec<_>=runtime.actions.iter().filter(|r|r["stage"]=="owner_observation").collect();
 assert_eq!(receipts.len(),2);
 assert_eq!(receipts[0]["admission_decision"],"WAITING_FOR_VALIDATED_PAIR");
 assert_eq!(receipts[1]["admission_decision"],"DENIED");
 assert!(!receipts[1]["admission_reason"].is_null());
 for receipt in &receipts{
  assert_eq!(receipt["owner"],"R1289");
  assert!(matches!(receipt["admission_decision"].as_str(),Some("WAITING_FOR_VALIDATED_PAIR"|"DENIED")));
  assert!(receipt["validated_input"]["snapshot_generation"].as_u64().unwrap()>0);
 }
 assert_eq!(runtime.execution.sequence(),before,"observation cannot mutate journal");
 assert!(runtime.execution.lifecycle().orders.is_empty());
 assert!(runtime.execution.lifecycle().processed_events.is_empty());
 assert_eq!(runtime.history.accepted,0);assert_eq!(runtime.frames_seen,0);
 assert!(new_isolated_identity_runtime(&runtime.identity,&path).is_err(),"one existing owner");
 assert_eq!(bridge1301::order_admission(&fixture.health,true,true,true,true,true),Err(bridge1301::Denial::Rules));
 fixture.trace.extend(runtime.actions.clone());
 std::fs::write(path.with_extension("r1301a_trace.json"),serde_json::to_vec_pretty(&fixture.trace).unwrap()).unwrap();
}

#[test] fn r1301a_16_inconsistent_pair(){
 let(mut runtime,_)=setup1301();
 let mut a=bridge1301::engine_fixture::EngineFixture::new(&runtime.identity.clock_domain);a.qualify();
 let mut b=bridge1301::engine_fixture::EngineFixture::for_base(&runtime.identity.clock_domain,"BBB");b.qualify();
 let spot=a.inputs.iter().find(|i|i.venue()==Venue::OkxSpot).unwrap();
 let future=b.inputs.iter().find(|i|i.venue()==Venue::BinanceFutures).unwrap();
 let mut pair=[None,None];
 dispatch_observation_input_r1301(&mut runtime,spot,&mut pair,&a.receiver,&a.health,true).unwrap();
 assert!(dispatch_observation_input_r1301(&mut runtime,future,&mut pair,&b.receiver,&b.health,true).is_err());
 assert!(runtime.execution.lifecycle().orders.is_empty());
}
#[test] fn r1301a_20_simulated_order_without_admission(){
 let mut batch=batch1301();
 // Refresh only the explicitly synthetic test metadata, never real-rule admission.
 batch.identity.profile.metadata_loaded_ns=spotarb_native_market_core::ingress_runtime_r1205::mono_ns();
 let mut runtime=new_isolated_identity_runtime(&batch.identity,&root1301()).unwrap();
 let mut f=bridge1301::engine_fixture::EngineFixture::new(&runtime.identity.clock_domain);f.qualify();
 let mut pair=[None,None];
 for _ in 0..2{let input=f.dequeue();dispatch_observation_input_r1301(&mut runtime,&input,&mut pair,&f.receiver,&f.health,true).unwrap();}
 let mut context=observation_context_r1301(&runtime.identity,"attempt-without-admission".into());
 context.spot_hot=f.receiver.revalidate(pair[0].as_ref().unwrap()).is_ok();
 context.futures_hot=f.receiver.revalidate(pair[1].as_ref().unwrap()).is_ok();
 assert!(context.spot_hot&&context.futures_hot);
 assert_eq!(bridge1301::order_admission(&f.health,true,true,true,true,true),Err(bridge1301::Denial::Rules));
 let result=runtime.execution.simulate_isolated_order("unadmitted-order",pair[0].as_ref().unwrap().book(),pair[1].as_ref().unwrap().book(),&context);
 assert!(matches!(result,Err(spotarb_native_market_core::native_paper_execution_r1289::ExecutionError::Invalid("unregistered fill"))));
 assert!(runtime.execution.lifecycle().orders.is_empty());
 assert!(runtime.execution.lifecycle().processed_events.is_empty());
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn r1304_actual_watchdog_abort_trips_health(){
 let health=bridge1301::Health::new();
 let done=Arc::new(AtomicBool::new(false));
 let task=tokio::spawn(observation_watchdog_r1304(health.clone(),done));
 // Let the actual task install its cancellation guard.
 tokio::time::sleep(Duration::from_millis(60)).await;
 assert!(health.healthy(),"watchdog must be healthy before cancellation; kill switch must be engaged");
 task.abort();let result=task.await;
 assert!(result.unwrap_err().is_cancelled());
 assert!(!health.healthy());assert!(health.incident().is_some());
}
#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn r1304_production_pump_concurrent_watchdog_restart(){
 let(runtime,path)=setup1301();let identity=runtime.identity.clone();
 let mut fixture=bridge1301::engine_fixture::EngineFixture::new(&identity.clock_domain);
 fixture.qualify();
 let health=fixture.health.clone();
 let done=Arc::new(AtomicBool::new(false));
 let watchdog=tokio::spawn(observation_watchdog_r1304(health.clone(),done.clone()));
 let receiver=fixture.receiver;
 let worker=tokio::task::spawn_blocking(move||observation_pump_r1304(runtime,receiver,health,Duration::from_millis(1),true));
 assert!(new_isolated_identity_runtime(&identity,&path).is_err(),"concurrent second owner must be refused while pump owns journal");
 let report=worker.await.unwrap().unwrap();
 done.store(true,Ordering::SeqCst);watchdog.await.unwrap();
 assert_eq!(report["orders"],0);
 assert!(report["validated_inputs"].as_u64().unwrap()<=2);
 let recovered=new_isolated_identity_runtime(&identity,&path).unwrap();
 assert_eq!(recovered.execution.lifecycle().state,paper_risk::LifecycleState::RecoveryRequired);
 assert!(recovered.execution.lifecycle().orders.is_empty());
 assert!(new_isolated_identity_runtime(&identity,&path).is_err());
 drop(recovered);
 let again=new_isolated_identity_runtime(&identity,&path).unwrap();
 assert_eq!(again.execution.lifecycle().state,paper_risk::LifecycleState::RecoveryRequired);
}

#[test]
fn r1304_correlated_actual_pipeline_trace(){
 let(mut runtime,path)=setup1301();
 let mut fixture=bridge1301::engine_fixture::EngineFixture::new(&runtime.identity.clock_domain);
 fixture.engine.enable_probe_audit();fixture.qualify();
 let mut pair=[None,None];let mut combined=fixture.engine.audit();
 for _ in 0..2{
  let input=fixture.dequeue();let id=input.trace_id().to_string();
  for stage in ["Decoder","Reconstruction","Validator","Witness"]{
   assert!(combined.iter().any(|r|r["trace_id"]==id&&r["stage"]==stage),"missing {stage} for {id}");
  }
  dispatch_observation_input_r1301(&mut runtime,&input,&mut pair,&fixture.receiver,&fixture.health,true).unwrap();
  let receipt=runtime.actions.iter().find(|r|r["trace_id"]==id).unwrap();
  for stage in ["Provider","BridgeEnqueue","BridgeDequeue","Entrypoint","Owner"]{
   assert!(receipt["pipeline_trace"].as_array().unwrap().iter().any(|r|r["stage"]==stage),"missing {stage}");
  }
 }
 combined.extend(runtime.actions.clone());
 std::fs::write(path.with_extension("r1304-correlated-trace.json"),serde_json::to_vec_pretty(&combined).unwrap()).unwrap();
 assert!(runtime.execution.lifecycle().orders.is_empty());
}

#[test] fn r1310_actual_pipeline_spans_are_correlated_and_monotonic(){
 let(mut runtime,_)=setup1301();
 let mut fixture=bridge1301::engine_fixture::EngineFixture::new(&runtime.identity.clock_domain);
 fixture.engine.enable_probe_audit();
 fixture.engine.enable_r1310_trace();
 fixture.qualify();
 let engine=fixture.engine.audit();
 let mut pairs=[None,None];
 for _ in 0..2{
  let input=fixture.dequeue();let id=input.trace_id().to_string();
  let mut engine_spans=Vec::new();
  for stage in ["Decoder","Reconstruction","Validator","Witness"]{
   let row=engine.iter().find(|r|r["kind"]=="r1310_pipeline"&&r["trace_id"]==id&&r["stage"]==stage)
    .unwrap_or_else(||panic!("missing timestamped {stage} for {id}"));
   let start=row["start_ns"].as_u64().expect("stage start");
   let end=row["end_ns"].as_u64().expect("stage end");
   let duration=row["duration_ns"].as_u64().expect("stage duration");
   assert_eq!(end.checked_sub(start),Some(duration),"negative/invalid span: {stage}");
   engine_spans.push((stage,start,end));
  }
  for i in 0..engine_spans.len()-1{assert!(engine_spans[i].2<=engine_spans[i+1].1,"Engine stage order: {:?}",engine_spans);}
  dispatch_observation_input_r1301(&mut runtime,&input,&mut pairs,&fixture.receiver,&fixture.health,true).unwrap();
  let receipt=runtime.actions.iter().find(|r|r["trace_id"]==id).expect("owner trace");
  let trace=receipt["pipeline_trace"].as_array().expect("pipeline trace");
  let mut spans=Vec::new();
  for stage in ["Provider","BridgeEnqueue","BridgeDequeue","Dispatcher","Owner"]{
   let row=trace.iter().find(|r|r["stage"]==stage).unwrap_or_else(||panic!("missing {stage}"));
   assert_eq!(row["trace_id"],id,"frame correlation at {stage}");
   let start=row["start_ns"].as_u64().expect("stage start");
   let end=row["end_ns"].as_u64().expect("stage end");
   assert_eq!(end.checked_sub(start),row["duration_ns"].as_u64(),"nonnegative span at {stage}");
   spans.push((stage,start,end));
  }
  for i in 0..3{assert!(spans[i].2<=spans[i+1].1,"Bridge stage order: {:?}",spans);}
  let enqueue=trace.iter().find(|r|r["stage"]=="BridgeEnqueue").unwrap();
  assert_eq!(enqueue["span_kind"],"pre_enqueue_service");
  let dequeue=trace.iter().find(|r|r["stage"]=="BridgeDequeue").unwrap();
  assert!(dequeue["queue_residence_ns"].as_u64().is_some());
  assert!(dequeue["queue_depth"].as_u64().is_some_and(|n|n<=bridge1301::CAPACITY as u64));
  let owner=trace.iter().find(|r|r["stage"]=="Owner").unwrap();
  let receive=engine.iter().find(|r|r["kind"]=="r1310_pipeline"&&r["trace_id"]==id&&r["stage"]=="Decoder").unwrap()["ws_receive_ns"].as_u64().unwrap();
  assert_eq!(owner["end_to_end_ns"].as_u64(),owner["end_ns"].as_u64().unwrap().checked_sub(receive));
  println!("R1310_TRACE_SAMPLE {}",serde_json::to_string(&serde_json::json!({"trace_id":id,"engine":engine.iter().filter(|r|r["kind"]=="r1310_pipeline"&&r["trace_id"]==id).collect::<Vec<_>>(),"entrypoint":receipt["pipeline_trace"]})).unwrap());
 }
 for action in runtime.actions.iter().filter(|r|r["trace_id"].is_string()){
  let id=action["trace_id"].as_str().unwrap();let trace=action["pipeline_trace"].as_array().unwrap();
  let row=trace.iter().find(|r|r["stage"]=="PairRevalidation").expect("pair dispatcher span");
  assert_eq!(row["trace_id"],id);assert_eq!(row["result"],"valid");
  let start=row["start_ns"].as_u64().unwrap();let end=row["end_ns"].as_u64().unwrap();
  assert_eq!(end.checked_sub(start),row["duration_ns"].as_u64());
 }
 assert!(runtime.execution.lifecycle().orders.is_empty());
 assert!(runtime.execution.lifecycle().processed_events.is_empty());
}
#[test] fn r1310_reset_reconnect_and_queue_overflow_are_observable(){
 let mut fixture=bridge1301::engine_fixture::EngineFixture::new("r1310-lifecycle");
 fixture.engine.enable_probe_audit();fixture.engine.enable_r1310_trace();fixture.qualify();
 let before=fixture.engine.audit();
 assert!(before.iter().any(|r|r["kind"]=="r1310_lifecycle"&&r["event"]=="authority_reset"&&r["frame_trace_id"].as_str().is_some()),"selection Reset trace");
 assert!(before.iter().any(|r|r["kind"]=="r1310_lifecycle"&&r["event"]=="snapshot"&&r["frame_trace_id"].as_str().is_some()),"snapshot/resnapshot trace");
 fixture.engine.mark_reconnecting("okx-a");
 let after=fixture.engine.audit();
 assert!(after.iter().any(|r|r["kind"]=="r1310_lifecycle"&&r["event"]=="reconnect"&&r["frame_trace_id"].as_str().is_some()),"reconnect trace");
 let input=fixture.inputs[0].clone();
 for _ in 0..bridge1301::CAPACITY+2{let _=fixture.r1306_enqueue(input.clone());}
 let metrics=fixture.health.metrics();
 assert!(metrics["overflow"].as_u64().unwrap()>0);
 assert!(metrics["dropped"].as_u64().unwrap()>0);
 assert_eq!(bridge1301::order_admission(&fixture.health,true,true,true,true,true),Err(bridge1301::Denial::Overflow));
}
fn r1310_overhead_mode(enabled:bool)->(u128,usize,usize,usize,usize,serde_json::Value){
 let(mut runtime,_)=setup1301();let started=Instant::now();let mut input_frames=0usize;let mut trace_dropped=0usize;let mut last_metrics=json!({});
 for _ in 0..64{
  let mut fixture=bridge1301::engine_fixture::EngineFixture::new(&runtime.identity.clock_domain);
  if enabled{fixture.engine.enable_r1310_trace();}
  fixture.qualify();input_frames+=12;
  last_metrics=fixture.engine.r1310_trace_metrics();
  for _ in 0..2{let input=fixture.dequeue();dispatch_observation_input_r1301(&mut runtime,&input,&mut [None,None],&fixture.receiver,&fixture.health,true).unwrap();trace_dropped+=fixture.health.metrics()["r1310_trace_dropped"].as_u64().unwrap_or(0)as usize;}
 }
 let elapsed=started.elapsed().as_nanos();
 assert_eq!(runtime.actions.len(),128);
 assert!(runtime.execution.lifecycle().orders.is_empty());
 assert!(runtime.execution.lifecycle().processed_events.is_empty());
 (elapsed,128,input_frames,runtime.actions.len(),trace_dropped,last_metrics)
}
#[test] fn r1310_instrumentation_overhead_offline_e2e(){
 let off=r1310_overhead_mode(false);let on=r1310_overhead_mode(true);
 assert_eq!((off.1,off.2,off.3),(on.1,on.2,on.3));
 assert_eq!(off.4,0);assert_eq!(on.4,0);
 let overhead_pct=if off.0==0{0.0}else{(on.0 as f64/off.0 as f64-1.0)*100.0};
 println!("R1310_OVERHEAD {{\"selected_frames_per_mode\":{},\"raw_engine_frames_per_mode\":{},\"off_elapsed_ns\":{},\"on_elapsed_ns\":{},\"off_ns_per_selected\":{},\"on_ns_per_selected\":{},\"overhead_pct\":{},\"capture\":\"forced 100% sample; 64 fixtures, existing Engine/Provider/Bridge/dispatcher/Owner; owner read-only\",\"sample_metrics\":{}}}",off.1,off.2,off.0,on.0,off.0/off.1 as u128,on.0/on.1 as u128,overhead_pct,on.5);
}
include!("native_paper_observation_r1306_tests.rs");
