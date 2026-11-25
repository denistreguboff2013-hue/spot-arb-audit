// Included only by the binary's cfg(test) observation module.
// Close both rendezvous endpoints before joining on assertion unwind.
// Worker cleanup must not replace the controller's original failure.
struct R1308PumpJoin {
 go:Option<std::sync::mpsc::SyncSender<()>>,
 rx:Option<std::sync::mpsc::Receiver<String>>,
 worker:Option<std::thread::JoinHandle<Result<Value,String>>>,
}
impl Drop for R1308PumpJoin {
 fn drop(&mut self){
  drop(self.rx.take());drop(self.go.take());
  if let Some(worker)=self.worker.take(){let _=worker.join();}
 }
}
fn r1306_pump_schedule(stage:&'static str,change:&str,expected:Option<bridge1301::Denial>){
 let(runtime,path)=setup1301();let identity=runtime.identity.clone();
 let mut fixture=bridge1301::engine_fixture::EngineFixture::new(&identity.clock_domain);fixture.qualify();
 let placeholder=bridge1301::engine_fixture::EngineFixture::new(&identity.clock_domain);
 let receiver=std::mem::replace(&mut fixture.receiver,placeholder.receiver);
 let health=fixture.health.clone();
 let worker_health=health.clone();
 let(tx,rx)=std::sync::mpsc::sync_channel(0);
 let(go,resume)=std::sync::mpsc::sync_channel(0);
 let worker=std::thread::spawn(move||{
  let mut stopped=false;let mut inspected=false;
  bridge1301::r1306_test_support::install(move|point|{
   if (!stopped&&point==stage)||(stopped&&!inspected&&point=="iteration"){
    if stopped{inspected=true}else{stopped=true}
    if tx.send(point.to_string()).is_ok(){
     match resume.recv_timeout(Duration::from_secs(3)){
      Ok(())|Err(std::sync::mpsc::RecvTimeoutError::Disconnected)=>{},
      Err(std::sync::mpsc::RecvTimeoutError::Timeout)=>panic!("controller must release checkpoint"),
     }
    }
   }
  });
  observation_pump_r1304(runtime,receiver,worker_health,Duration::from_millis(1),true)
 });
 let mut joined=R1308PumpJoin{go:Some(go),rx:Some(rx),worker:Some(worker)};
 assert_eq!(joined.rx.as_ref().unwrap().recv_timeout(Duration::from_secs(3)).unwrap(),stage);
 let mut replacement=bridge1301::engine_fixture::EngineFixture::new(&identity.clock_domain);
 let old=fixture.engine.authority.read().unwrap().clone();
 match change {
  "mismatch"=>{for p in fixture.engine.authority.write().unwrap().values_mut(){p.session="invalid-current-session".into();}},
  "deny"=>fixture.engine.authority.write().unwrap().clear(),
  "deny_permit"=>{fixture.engine.authority.write().unwrap().clear();*fixture.engine.authority.write().unwrap()=old;},
  "reset"|"reset_pair"|"resets"|"overflow"|"reset_revoked"|"reset_sticky"=>{
   for _ in 0..if change=="resets"{3}else{1}{
    replacement=bridge1301::engine_fixture::EngineFixture::new(&identity.clock_domain);
    replacement.qualify();fixture.r1306_reset_from(&replacement);
   }
   if change=="reset_revoked"{
    for id in ["okx-a","okx-b","binance-a","binance-b"]{replacement.engine.mark_reconnecting(id);}
   }
   if change=="reset_sticky"{health.trip(bridge1301::Denial::Consumer);}
   if change=="overflow"{
    for _ in 0..bridge1301::CAPACITY+1{let _=fixture.r1306_enqueue(replacement.inputs[0].clone());}
    assert!(health.metrics()["overflow"].as_u64().unwrap()>0);
   }else{for i in replacement.inputs.clone(){fixture.r1306_enqueue(i).unwrap();}}
  },
  "revoke"=>{for id in ["okx-a","okx-b","binance-a","binance-b"]{fixture.engine.mark_reconnecting(id);}},
  _=>panic!("unknown schedule")
 }
 joined.go.as_ref().unwrap().send(()).unwrap();
 assert_eq!(joined.rx.as_ref().unwrap().recv_timeout(Duration::from_secs(3)).unwrap(),"iteration");
 let incident=health.incident();
 if incident!=expected&&change=="reset_pair"{let current=fixture.engine.authority.read().unwrap().values().map(|p|(p.session.clone(),p.epoch,p.snapshot_witness.as_ref().map(|w|w.generation()))).collect::<Vec<_>>();println!("R1308_PAIR_GENERATIONS old={:?} replacement={:?} authority={:?}",fixture.inputs.iter().map(|i|(i.input_id(),i.snapshot_generation())).collect::<Vec<_>>(),replacement.inputs.iter().map(|i|(i.input_id(),i.snapshot_generation())).collect::<Vec<_>>(),current);}
 assert_eq!(incident,expected,"incident at controlled interleaving");
 joined.go.as_ref().unwrap().send(()).unwrap();
 let report=joined.worker.take().unwrap().join().unwrap().unwrap();
 assert_eq!(report["orders"],0);
 let mut ids=std::collections::HashSet::new();
 for r in report["actions"].as_array().unwrap(){
  assert_ne!(r["admission_decision"],"PERMITTED");
  if change!="overflow"&&change!="reset_pair"&&r["stage"]=="owner_observation"{assert!(ids.insert(r["input_id"].clone().to_string()),"double dispatch");}
 }
 if change=="reset"||change=="reset_pair"{
  let actions=report["actions"].as_array().unwrap();
  // Fixture-local input IDs can repeat across independently qualified Engines.
  // Check uniqueness using the validator-minted witness as well.
  let mut observed=std::collections::HashSet::new();
  for r in actions.iter().filter(|r|r["stage"]=="owner_observation"){
   assert!(observed.insert((r["input_id"].to_string(),r["validated_input"]["snapshot_generation"].to_string())),"double dispatch within witness generation");
  }
  let fresh_ids:std::collections::HashSet<_>=replacement.inputs.iter().filter_map(|i|i.snapshot_generation()).collect();
  assert!(actions.iter().any(|r|r["stage"]=="owner_observation"&&r["validated_input"]["snapshot_generation"].as_u64().is_some_and(|id|fresh_ids.contains(&id))),"same pump must observe replacement authority");
  if stage=="dispatch"{
   let old_ids:std::collections::HashSet<_>=fixture.inputs.iter().filter_map(|i|i.snapshot_generation()).collect();
   assert!(!actions.iter().any(|r|r["stage"]=="owner_observation"&&r["validated_input"]["snapshot_generation"].as_u64().is_some_and(|id|old_ids.contains(&id))),"retired input must not reach owner observation");
  }
 }
 let recovered=new_isolated_identity_runtime(&identity,&path).unwrap();
 assert!(recovered.execution.lifecycle().orders.is_empty());
 assert!(recovered.execution.lifecycle().processed_events.is_empty());
 assert_eq!(recovered.execution.lifecycle().state,paper_risk::LifecycleState::RecoveryRequired);
}
#[test] fn r1306_reset_with_queued_frame(){r1306_pump_schedule("receive","reset",None)}
#[test] fn r1306_new_witness_during_dequeue(){r1306_pump_schedule("dequeue","reset",None)}
#[test] fn r1306_permit_removed_during_dispatch(){r1306_pump_schedule("dispatch","deny",Some(bridge1301::Denial::Hot))}
#[test] fn r1306_market_deny_then_permit_still_no_orders(){r1306_pump_schedule("receive","deny_permit",None)}
#[test] fn r1306_repeated_resets(){r1306_pump_schedule("receive","resets",None)}
#[test] fn r1306_overflow_with_generation_switch(){r1306_pump_schedule("receive","overflow",Some(bridge1301::Denial::Overflow))}
#[test] fn r1306_invalidated_witness_before_pair_admission(){r1306_pump_schedule("pair","revoke",Some(bridge1301::Denial::Generation))}
#[test] fn r1306_late_revoke_still_cannot_admit(){r1306_pump_schedule("admission","revoke",None)}
// Safety regression: a valid newer authority should retire an obsolete in-flight frame.
// R1307 reproduced the false incident; the original assertion remains unchanged.
#[test] fn r1306_reset_after_dequeue_no_false_sticky(){r1306_pump_schedule("dispatch","reset",None)}
#[test] fn r1308_reset_during_pair_revalidation(){r1306_pump_schedule("pair","reset_pair",None)}
#[test] fn r1308_current_authority_mismatch_stays_sticky(){r1306_pump_schedule("dispatch","mismatch",Some(bridge1301::Denial::Generation))}
#[test] fn r1308_revoked_replacement_stays_sticky(){r1306_pump_schedule("dispatch","reset_revoked",Some(bridge1301::Denial::Generation))}
#[test] fn r1308_reset_preserves_existing_incident(){r1306_pump_schedule("dispatch","reset_sticky",Some(bridge1301::Denial::Consumer))}


async fn r1306_watchdog_case(kind:&str,expected:bridge1301::Denial){
 let mut fixture=bridge1301::engine_fixture::EngineFixture::new("r1306-watchdog");fixture.qualify();
 let health=fixture.health.clone();
 match kind{
  "heartbeat"|"stall"=>bridge1301::r1306_test_support::age(&health,true,false,false),
  "silence"=>bridge1301::r1306_test_support::age(&health,false,true,false),
  "saturation"=>{for _ in 0..bridge1301::CAPACITY+1{let _=fixture.r1306_enqueue(fixture.inputs[0].clone());}},
  "panic"=>health.r1306_task_exit.store(1,Ordering::SeqCst),
  "completion"=>health.r1306_task_exit.store(2,Ordering::SeqCst),
  _=>panic!("watchdog case")
 }
 let done=Arc::new(AtomicBool::new(false));
 let task=tokio::spawn(observation_watchdog_r1304(health.clone(),done.clone()));
 if kind=="panic"{assert!(task.await.unwrap_err().is_panic());}
 else if kind=="completion"{task.await.unwrap();}
 else{
  tokio::time::timeout(Duration::from_secs(2),async{
   while bridge1301::r1306_test_support::last_tick(&health)==0{tokio::task::yield_now().await;}
  }).await.unwrap();
  assert_eq!(health.incident(),Some(expected.clone()));
  done.store(true,Ordering::SeqCst);task.await.unwrap();
 }
 assert!(!health.healthy());assert_eq!(health.incident(),Some(expected.clone()));
 assert_eq!(bridge1301::order_admission(&health,true,true,true,true,true),Err(expected.clone()));
 health.r1306_task_exit.store(0,Ordering::SeqCst);
 let prior=bridge1301::r1306_test_support::last_tick(&health);
 let restart_done=Arc::new(AtomicBool::new(false));
 let restarted=tokio::spawn(observation_watchdog_r1304(health.clone(),restart_done.clone()));
 tokio::time::timeout(Duration::from_secs(2),async{
  while bridge1301::r1306_test_support::last_tick(&health)==prior{tokio::task::yield_now().await;}
 }).await.unwrap();
 restart_done.store(true,Ordering::SeqCst);restarted.await.unwrap();
 assert_eq!(health.incident(),Some(expected.clone()));
 assert_eq!(bridge1301::order_admission(&health,true,true,true,true,true),Err(expected));
 let fresh=bridge1301::Health::new();
 assert_eq!(bridge1301::order_admission(&fresh,true,true,true,true,true),Err(bridge1301::Denial::Rules));
}
#[tokio::test] async fn r1306_watchdog_heartbeat_timeout_consumer_stall(){r1306_watchdog_case("heartbeat",bridge1301::Denial::Consumer).await}
#[tokio::test] async fn r1306_watchdog_producer_silence(){r1306_watchdog_case("silence",bridge1301::Denial::Stale).await}
#[tokio::test] async fn r1306_watchdog_queue_saturation(){r1306_watchdog_case("saturation",bridge1301::Denial::Overflow).await}
#[tokio::test] async fn r1306_watchdog_panic(){r1306_watchdog_case("panic",bridge1301::Denial::Consumer).await}
#[tokio::test] async fn r1306_watchdog_unexpected_completion(){r1306_watchdog_case("completion",bridge1301::Denial::Consumer).await}

#[test] fn r1306_admission_guard_priority_matrix(){
 use bridge1301::Denial::*;
 // (owner, storage, real_off, kill_engaged, policy, health incident, first denial)
 let rows=[
 (true,true,true,true,true,None,Rules),
 (true,true,true,true,false,None,Policy),
 (true,false,true,true,true,None,Storage),
 (false,true,true,true,true,None,Owner),
 (true,true,true,false,true,None,Kill),
 (true,true,false,true,true,None,Real),
 (false,false,false,false,false,Some(Overflow),Real),
 (false,false,true,false,false,Some(Consumer),Kill),
 (false,false,true,true,false,Some(Overflow),Overflow),
 (false,false,true,true,false,None,Owner),
 (true,false,true,true,false,None,Storage),
 (true,true,true,true,true,Some(Stale),Stale),
 (true,true,true,true,true,Some(Generation),Generation),
 (true,true,true,true,true,Some(Hot),Hot),
 (true,true,true,true,true,Some(Consumer),Consumer)];
 for (owner,storage,real,kill,policy,incident,expected) in rows{
  let health=bridge1301::Health::new();if let Some(cause)=incident{health.trip(cause)}
  assert_eq!(bridge1301::order_admission(&health,owner,storage,real,kill,policy),Err(expected));
 }
 let health=bridge1301::Health::new();
 bridge1301::r1306_test_support::age(&health,false,false,true);
 assert_eq!(bridge1301::order_admission(&health,true,true,true,true,true),Err(Provider));
}
#[test] fn r1306_admission_receipt_matrix(){
 for (fault,concurrent,expected) in [
  ("rules",None,"Rules"),("kill",None,"Kill"),("kill",Some(bridge1301::Denial::Consumer),"Kill"),("stale",None,"Stale"),("witness",None,"Generation"),
  ("generation",None,"Generation"),("hot",None,"Hot"),
  ("rules",Some(bridge1301::Denial::Consumer),"Consumer"),
  ("rules",Some(bridge1301::Denial::Overflow),"Overflow"),
  ("stale",Some(bridge1301::Denial::Consumer),"Stale"),
  ("generation",Some(bridge1301::Denial::Storage),"Generation")]{
  let _kill=bridge1301::r1306_test_support::kill_override(fault!="kill");
  let(mut runtime,_)=setup1301();let mut f=bridge1301::engine_fixture::EngineFixture::new(&runtime.identity.clock_domain);f.qualify();
  let mut pair=[None,None];let first=f.dequeue();
  dispatch_observation_input_r1301(&mut runtime,&first,&mut pair,&f.receiver,&f.health,true).unwrap();
  let mut input=f.dequeue();
  if let Some(ref denial)=concurrent{f.health.trip(denial.clone())}
  if fault=="hot"{f.engine.authority.write().unwrap().clear();}
  else if fault!="rules"&&fault!="kill"{input=bridge1301::r1306_test_support::corrupt(&input,fault);}
  dispatch_observation_input_r1301(&mut runtime,&input,&mut pair,&f.receiver,&f.health,true).unwrap();
  let receipt=runtime.actions.last().unwrap();
  assert_eq!(receipt["admission_reason"],expected);
  assert!(receipt["admission_decision"].as_str().unwrap().starts_with("DENIED"));
  if let Some(denial)=concurrent{assert_eq!(f.health.incident(),Some(denial),"first sticky health incident must remain");}
  assert!(runtime.execution.lifecycle().orders.is_empty());
  assert!(runtime.execution.lifecycle().processed_events.is_empty());
 }
}
