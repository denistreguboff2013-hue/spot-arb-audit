// Prepared for Luna. Uses actual Engine/Owner/DecisionCore/dispatcher/journal.
use super::*;
#[test] fn r1322_entry_pair_and_reverse_arrival(){
 for reverse in [false,true]{
  let(f,mut r,mut p)=setup();let mut inputs=f.inputs.clone();if reverse{inputs.reverse();}let b=boundary(&r);
  assert!(p.observe(&mut r,inputs.remove(0),&f.receiver,&f.health,&b).unwrap().is_none());
  let c=p.observe(&mut r,inputs.remove(0),&f.receiver,&f.health,&b).unwrap().unwrap();
  let b=boundary(&r);p.consume(&mut r,c,&f.receiver,&f.health,&b,||clocks()).unwrap();
  assert_eq!(r.execution.lifecycle().orders.len(),1);
  assert!(r.execution.registered_intents().unwrap().values().all(|x|x.witness["context"]["input_id"].as_str().unwrap().starts_with("r1322:")));
 }
}
#[test] fn r1322_missing_second_side_has_no_intent(){
 let(f,mut r,mut p)=setup();let b=boundary(&r);
 assert!(p.observe(&mut r,f.inputs[0].clone(),&f.receiver,&f.health,&b).unwrap().is_none());
 assert!(r.execution.lifecycle().orders.is_empty());
}
#[test] fn r1322_owner_boundary_rechecks_safety_and_metadata(){
 for case in 0..5{
  let(f,mut r,mut p)=setup();let c=candidate(&f,&mut r,&mut p);let mut b=boundary(&r);
  match case{0=>b.metadata_generation+=1,1=>b.real_off=false,2=>b.kill_engaged=false,3=>b.storage_healthy=false,_=>b.identity.profile.fee_bps="11".into()}
  let before=r.execution.sequence();assert!(p.consume(&mut r,c,&f.receiver,&f.health,&b,||clocks()).is_err());
  assert_eq!(r.execution.sequence(),before);assert!(r.execution.lifecycle().orders.is_empty());
 }
}
#[test] fn r1322_revoked_witness_before_owner_creates_no_intent(){
 let(mut f,mut r,mut p)=setup();let c=candidate(&f,&mut r,&mut p);let b=boundary(&r);
 let source=f.inputs[0].source_provenance().session_id.clone();f.engine.mark_reconnecting(&source);
 let before=r.execution.sequence();assert!(p.consume(&mut r,c,&f.receiver,&f.health,&b,||clocks()).is_err());
 assert_eq!(before,r.execution.sequence());assert!(r.execution.lifecycle().orders.is_empty());
}
use bridge1301::engine_fixture::{EngineFixture,raw};
fn frames(n:u64,w:i64,seq:i64,snapshot:bool,depth:&str)->Vec<RawFrame>{
 let mut result=Vec::new();
 for (venue,prefix) in [(Venue::OkxSpot,"okx"),(Venue::BinanceFutures,"binance")]{
  for copy in ["a","b"]{
   let mut f=raw(venue,&format!("{prefix}-{copy}"),"AAA",n,w,seq,snapshot);
   let mut v:Value=serde_json::from_str(&f.raw).unwrap();
   if venue==Venue::OkxSpot{v["data"][0]["bids"]=json!([["99.99",depth],["99.98",depth]]);v["data"][0]["asks"]=json!([["100",depth],["100.01",depth]]);}
   else{v["data"]["b"]=json!([["102",depth],["101.99",depth]]);v["data"]["a"]=json!([["102.01",depth],["102.02",depth]]);}
   f.raw=v.to_string();result.push(f);
  }
 }
 result
}
pub(super) fn setup()->(EngineFixture,IsolatedPaperRuntime,TypedPipeline){
 reset_clock();let(n,_)=clocks();
 let b:OfflineMarketBatch=serde_json::from_str(include_str!("../fixtures/r1298/full_exit.json")).unwrap();let mut identity=b.identity;
 identity.clock_domain="r1322-offline".into();identity.profile.metadata_loaded_ns=n;
 let(run_id,path)=allocate_test_journal();identity.run_id=run_id;
 let mut r=new_isolated_identity_runtime(&identity,&path).unwrap();
 r.decision.replace_routes(1,HashMap::from([("AAA".into(),vec![isolated_route(&identity.profile)])])).unwrap();
 let mut f=EngineFixture::new("r1322-offline");
 // Feed Engine and RawFrames from the same deterministic source clock.
 f.engine.set_test_clock(clocks);f.health.set_test_clock(clocks);
 for (step,age) in [300u64,150,0].into_iter().enumerate(){
  if step>0{advance_clock_ns(1_000_000);}let(n,w)=clocks();
  let(frame_ns,frame_wall)=past_clock(n,w,age);
  let got=f.feed_r1322(frames(frame_ns,frame_wall,100+step as i64,step==0,"20"),n,w);f.inputs.extend(got);
 }
 assert_eq!(f.inputs.len(),2,"expected one validated frame per venue through Engine/Provider; received={} invalid={} categories={:?} hot_deficits={}",
  f.engine.metrics.received,f.engine.metrics.invalid,f.engine.metrics.invalid_categories,f.engine.metrics.hot_deficits);
 (f,r,TypedPipeline::new())
}
#[test] fn r1322_shared_clock_frames_pass_actual_provider(){
 let(f,_,_)=setup();
 assert!(f.inputs.iter().any(|i|i.venue()==Venue::OkxSpot&&i.snapshot_generation().is_some()));
 assert!(f.inputs.iter().any(|i|i.venue()==Venue::BinanceFutures&&i.snapshot_generation().is_some()));
 assert_ne!(f.inputs[0].input_id(),f.inputs[1].input_id());
}
pub(super) fn boundary(r:&IsolatedPaperRuntime)->Boundary{Boundary{identity:r.identity.clone(),metadata_generation:1,price_ticks:["0.01".into(),"0.01".into()],real_off:true,kill_engaged:true,storage_healthy:true}}
fn candidate(f:&EngineFixture,r:&mut IsolatedPaperRuntime,p:&mut TypedPipeline)->TypedCandidate{
 let b=boundary(r);assert!(p.observe(r,f.inputs[0].clone(),&f.receiver,&f.health,&b).unwrap().is_none());
 p.observe(r,f.inputs[1].clone(),&f.receiver,&f.health,&b).unwrap().expect("actual DecisionCore executable pair")
}
fn r1327_writer_root(label:&str)->std::path::PathBuf{
 use std::os::unix::fs::DirBuilderExt;
 let n=std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
 let root=std::env::temp_dir().join(format!("spotarb_r1173_r1327_{label}_{}_{}",std::process::id(),n));
 std::fs::DirBuilder::new().mode(0o700).create(&root).unwrap();root
}
fn advance(f:&mut EngineFixture,r:&mut IsolatedPaperRuntime,p:&mut TypedPipeline,seq:i64,depth:&str){
 advance_clock_ns(1_000_000);let(n,w)=clocks();let mut inputs=f.feed_r1322(frames(n,w,seq,false,depth),n,w);assert!(!inputs.is_empty());
 // Engine replaces the Binance snapshot witness before returning this batch.
 // Refresh the cached futures leg before pairing OKX with it; Engine scope order
 // comes from a HashSet and is not an arrival-order guarantee.
 inputs.sort_by_key(|input|if input.venue()==Venue::BinanceFutures{0}else{1});
 let b=boundary(r);
 for input in inputs{if let Some(c)=p.observe(r,input,&f.receiver,&f.health,&b).unwrap(){p.consume(r,c,&f.receiver,&f.health,&b,||clocks()).unwrap();}}
}
#[test] fn r1322_freshness_virtual_799_800_801_boundary(){
 for age_ms in [0i64,799,800,801]{
  let(f,mut r,mut p)=setup();let c=candidate(&f,&mut r,&mut p);let b=boundary(&r);
  let n=c.inputs.iter().map(|i|i.book().received_ns).min().unwrap().checked_add((age_ms as u64)*1_000_000).expect("offline test timestamp out of range");
  let w=c.inputs.iter().map(|i|i.exchange_ms()).min().unwrap().checked_add(age_ms).expect("offline test timestamp out of range");set_clock(n,w);
  let source_check=bridge1301::revalidate_r1322(&c.inputs[0],&f.receiver,n,w);
  assert_eq!(f.health.healthy(),age_ms<=800,"virtual Health watchdog boundary at {age_ms}ms");
  let before=r.execution.sequence();let result=p.consume(&mut r,c,&f.receiver,&f.health,&b,clocks);
  if age_ms<=800{assert!(source_check.is_ok(),"freshness should allow age {age_ms}ms: {source_check:?}");assert!(result.is_ok());assert_eq!(r.execution.lifecycle().orders.len(),1);}
  else{assert!(source_check.is_err(),"provider must reject age {age_ms}ms");assert!(result.is_err());assert_eq!(r.execution.sequence(),before);assert!(r.execution.lifecycle().orders.is_empty());}
 }
}
#[test] fn r1322_future_and_inconsistent_virtual_clocks_fail_closed(){
 for mode in 0..4{
  let(f,mut r,mut p)=setup();let c=candidate(&f,&mut r,&mut p);let b=boundary(&r);
  let receive=c.inputs.iter().map(|i|i.book().received_ns).min().unwrap();let event=c.inputs.iter().map(|i|i.exchange_ms()).min().unwrap();
  let(n,w)=match mode{0=>(receive.checked_sub(1).expect("offline test timestamp out of range"),event),1=>(receive,event.checked_sub(1).expect("offline test timestamp out of range")),2=>(receive.checked_add(799_000_000).expect("offline test timestamp out of range"),event.checked_add(801).expect("offline test timestamp out of range")),_=>(receive.checked_add(801_000_000).expect("offline test timestamp out of range"),event.checked_add(799).expect("offline test timestamp out of range"))};
  set_clock(n,w);assert!(bridge1301::revalidate_r1322(&c.inputs[0],&f.receiver,n,w).is_err(),"clock mismatch mode {mode} must fail Provider validation");
  let before=r.execution.sequence();assert!(p.consume(&mut r,c,&f.receiver,&f.health,&b,clocks).is_err());
  assert_eq!(r.execution.sequence(),before);assert!(r.execution.lifecycle().orders.is_empty());
 }
}
#[test] fn r1322_duplicate_conflict_and_repeat_intent_denied(){
 for conflict in [false,true]{
  let(f,mut r,mut p)=setup();let mut c=candidate(&f,&mut r,&mut p);let b=boundary(&r);
  if conflict{c.rows[0].quantity=Some(99.0);assert!(p.consume(&mut r,c,&f.receiver,&f.health,&b,||clocks()).is_err());assert!(r.execution.lifecycle().orders.is_empty());}
  else{p.consume(&mut r,c,&f.receiver,&f.health,&b,||clocks()).unwrap();let n=r.execution.sequence();assert!(p.observe(&mut r,f.inputs[1].clone(),&f.receiver,&f.health,&b).is_err());assert_eq!(r.execution.sequence(),n);assert_eq!(r.execution.lifecycle().orders.len(),1);}
 }
}
#[test] fn r1322_reset_after_candidate_denied(){
 let(mut f,mut r,mut p)=setup();let c=candidate(&f,&mut r,&mut p);let b=boundary(&r);
 let mut replacement=EngineFixture::new("r1322-offline");replacement.qualify_r1322(clocks,advance_clock_ns);f.r1306_reset_from(&replacement);
 assert!(p.consume(&mut r,c,&f.receiver,&f.health,&b,||clocks()).is_err());assert!(r.execution.lifecycle().orders.is_empty());
}
#[test] fn r1322_rechecks_between_clock_commit_and_intent(){
 let(f,mut r,mut p)=setup();let c=candidate(&f,&mut r,&mut p);let b=boundary(&r);let mut calls=0;
 assert!(p.consume(&mut r,c,&f.receiver,&f.health,&b,||{calls+=1;if calls<3{clocks()}else{advance_clock_ns(801_000_000);clocks()}}).is_err());
 assert!(calls>=3);assert!(r.execution.lifecycle().orders.is_empty());
}
#[test] fn r1322_full_lifecycle_and_closed_recovery(){
 let(mut f,mut r,mut p)=setup();let c=candidate(&f,&mut r,&mut p);let b=boundary(&r);p.consume(&mut r,c,&f.receiver,&f.health,&b,||clocks()).unwrap();
 advance(&mut f,&mut r,&mut p,103,"20");advance(&mut f,&mut r,&mut p,104,"20");advance(&mut f,&mut r,&mut p,105,"20");
 assert_eq!(r.execution.lifecycle().state,paper_risk::LifecycleState::Hedged);
 let(n,w)=clocks();let ctx=isolated_policy::MarketContext{input_id:"r1322-exit".into(),clock_domain:r.identity.clock_domain.clone(),now_ns:n,wall_ms:w,spot_hot:true,futures_hot:true};
 r.execution.request_isolated_exit(&ctx).unwrap();
 for seq in 106..112{if r.execution.lifecycle().state!=paper_risk::LifecycleState::Closed{advance(&mut f,&mut r,&mut p,seq,"20");}}
 assert_eq!(r.execution.lifecycle().state,paper_risk::LifecycleState::Closed);assert_eq!(r.execution.lifecycle().orders.len(),4);
 let path=r.execution.journal_path().to_path_buf();let id=r.identity.clone();let n=r.execution.sequence();let projection=serde_json::to_value(r.execution.projection()).unwrap();drop(r);
 let recovered=DurablePaper::recover_isolated(path,&id).unwrap();assert_eq!(recovered.sequence(),n);assert_eq!(serde_json::to_value(recovered.projection()).unwrap(),projection);
}
#[test] fn r1326m_open_hedge_restart_preserves_exposure(){
 let(mut f,mut r,mut p)=setup();let c=candidate(&f,&mut r,&mut p);let b=boundary(&r);
 p.consume(&mut r,c,&f.receiver,&f.health,&b,||clocks()).unwrap();
 advance(&mut f,&mut r,&mut p,103,"20");advance(&mut f,&mut r,&mut p,104,"20");advance(&mut f,&mut r,&mut p,105,"20");
 assert_eq!(r.execution.lifecycle().state,paper_risk::LifecycleState::Hedged);
 let path=r.execution.journal_path().to_path_buf();let id=r.identity.clone();
 let seq=r.execution.sequence();let mut projection=serde_json::to_value(r.execution.projection()).unwrap();
 projection["state"]=serde_json::json!("RecoveryRequired");
 let orders=serde_json::to_value(&r.execution.lifecycle().orders).unwrap();drop(r);
 let mut recovered=new_isolated_identity_runtime(&id,&path).unwrap();
 assert!(recovered.execution.sequence()>=seq,"recovery may append a recovery marker");
 assert_eq!(serde_json::to_value(recovered.execution.projection()).unwrap(),projection);
 assert_eq!(serde_json::to_value(&recovered.execution.lifecycle().orders).unwrap(),orders);
 assert_eq!(recovered.execution.lifecycle().state,paper_risk::LifecycleState::RecoveryRequired);
 let post_recovery_seq=recovered.execution.sequence();
 let mut fresh=TypedPipeline::new();let b=boundary(&recovered);
 assert!(fresh.observe(&mut recovered,f.inputs[0].clone(),&f.receiver,&f.health,&b).is_err());
 assert_eq!(recovered.execution.sequence(),post_recovery_seq);
 assert_eq!(serde_json::to_value(recovered.execution.projection()).unwrap(),projection);
 assert_eq!(serde_json::to_value(&recovered.execution.lifecycle().orders).unwrap(),orders);
}
#[test] fn r1322_partial_fill_uses_existing_lifecycle(){
 let(mut f,mut r,mut p)=setup();let c=candidate(&f,&mut r,&mut p);let b=boundary(&r);p.consume(&mut r,c,&f.receiver,&f.health,&b,||clocks()).unwrap();
 advance(&mut f,&mut r,&mut p,103,"0.01");
 assert!(r.execution.lifecycle().orders.values().any(|o|o.leg==paper_risk::FillLeg::Spot&&o.status==paper_risk::OrderStatus::PartiallyFilled));
 assert!(r.execution.projection().spot_base>0.0&&r.execution.projection().spot_base<0.1);
}
#[test] fn r1322_reject_timeout_unknown_and_restart_deny_reexecution(){
 for timeout in [false,true]{
  let(f,mut r,mut p)=setup();let c=candidate(&f,&mut r,&mut p);let b=boundary(&r);p.consume(&mut r,c,&f.receiver,&f.health,&b,||clocks()).unwrap();
  let order=r.execution.lifecycle().orders.keys().next().unwrap().clone();let(n,w)=clocks();
  let ctx=isolated_policy::MarketContext{input_id:"r1322-control".into(),clock_domain:r.identity.clock_domain.clone(),now_ns:n,wall_ms:w,spot_hot:true,futures_hot:true};
  let control=if timeout{IsolatedControl::Timeout{order_id:order.clone()}}else{IsolatedControl::Rejected{order_id:order.clone()}};
  r.execution.control_isolated(control,&ctx).unwrap();
  if timeout{assert!(r.execution.lifecycle().orders[&order].uncertain);}
  else{assert_eq!(r.execution.lifecycle().orders[&order].status,paper_risk::OrderStatus::Rejected);}
  let path=r.execution.journal_path().to_path_buf();let id=r.identity.clone();drop(r);
  let mut recovered=new_isolated_identity_runtime(&id,&path).unwrap();let before=recovered.execution.sequence();let mut fresh=TypedPipeline::new();let b=boundary(&recovered);
  assert!(fresh.observe(&mut recovered,f.inputs[0].clone(),&f.receiver,&f.health,&b).is_err());
  assert_eq!(recovered.execution.sequence(),before);assert_eq!(recovered.execution.lifecycle().orders.len(),1);
 }
}
#[test] fn r1322_owner_revision_change_denies_pending_candidate(){
 let(f,mut r,mut p)=setup();let c=candidate(&f,&mut r,&mut p);let b=boundary(&r);let(n,w)=clocks();
 let ctx=isolated_policy::MarketContext{input_id:"other-owner-command".into(),clock_domain:r.identity.clock_domain.clone(),now_ns:n,wall_ms:w,spot_hot:true,futures_hot:true};
 r.execution.control_isolated(IsolatedControl::Clock,&ctx).unwrap();let seq=r.execution.sequence();
 assert!(p.consume(&mut r,c,&f.receiver,&f.health,&b,||clocks()).is_err());assert_eq!(seq,r.execution.sequence());assert!(r.execution.lifecycle().orders.is_empty());
}

#[test] fn r1322_actual_core_metadata_and_risk_replacement_denied(){
 for metadata in [true,false]{
  let(f,mut r,mut p)=setup();let c=candidate(&f,&mut r,&mut p);let b=boundary(&r);
  if metadata{r.decision.replace_routes(2,std::collections::HashMap::from([(r.identity.profile.base.clone(),vec![isolated_route(&r.identity.profile)])])).unwrap();}
  else{r.decision.cfg.risk_cap+=1.0;}
  let before=r.execution.sequence();
  assert!(p.consume(&mut r,c,&f.receiver,&f.health,&b,||clocks()).is_err());
  assert_eq!(r.execution.sequence(),before);assert!(r.execution.lifecycle().orders.is_empty());
 }
}

#[test] fn r1322d_original_underflow_and_fixed_epoch(){
 assert_eq!(1u64.checked_sub(300_000_000),None);
 assert!(std::panic::catch_unwind(||past_clock(1,1,300)).is_err());
 reset_clock();let(n,w)=clocks();assert_eq!(past_clock(n,w,300),(700_000_000,700));
 assert_eq!(clocks(),(n,w)); // Past frame construction must not refresh the clock.
}
#[test] fn r1322d_reset_set_advance_and_fractional_milliseconds(){
 reset_clock();let initial=clocks();advance_clock_ns(999_999);
 assert_eq!(clocks(),(initial.0+999_999,initial.1));advance_clock_ns(1);
 assert_eq!(clocks(),(initial.0+1_000_000,initial.1+1));
 set_clock(42,7);advance_clock_ns(1);assert_eq!(clocks(),(43,7));
 reset_clock();assert_eq!(clocks(),initial);
}
#[test] fn r1322d_invalid_arithmetic_does_not_publish_time(){
 for (n,w,delta) in [(u64::MAX,1,1),(1,i64::MAX,1_000_000)]{
  set_clock(n,w);assert!(std::panic::catch_unwind(||advance_clock_ns(delta)).is_err());
  assert_eq!(clocks(),(n,w));
 }
 assert!(std::panic::catch_unwind(||past_clock(u64::MAX,0,u64::MAX)).is_err());
 assert!(std::panic::catch_unwind(||past_clock(1_000_000,i64::MIN,1)).is_err());
 reset_clock();
}
#[test] fn r1325_auto_entry_requires_live_turnover_witness(){
 for volume in [false,true]{
  let(f,mut r,mut p)=setup();let(_,path)=allocate_test_journal();
  r.execution=DurablePaper::create_universe_r1324a(&path,&std::collections::BTreeMap::from([(r.identity.profile.base.clone(),r.identity.clone())]),1).unwrap();
  let auto=std::sync::Arc::new(integrated::AutoPolicy::load().unwrap());
  if volume{let(n,w)=clocks();auto.seed_turnover(&r.identity.profile.base,n,w);}
  p.auto=Some(auto);let b=boundary(&r);let mut inputs=f.inputs.clone();
  assert!(p.observe(&mut r,inputs.remove(0),&f.receiver,&f.health,&b).unwrap().is_none());
  let c=p.observe(&mut r,inputs.remove(0),&f.receiver,&f.health,&b).unwrap().unwrap();
  p.consume(&mut r,c,&f.receiver,&f.health,&b,clocks).unwrap();
  assert_eq!(r.execution.lifecycle().orders.len(),if volume{1}else{0});
  assert_eq!(r.execution.universe_states_r1324a().len(),1);
 }
}

#[test] fn r1326_integrated_gate_durably_intents_before_owner_order(){
 use std::os::unix::fs::DirBuilderExt;
 let(f,mut r,mut p)=setup();let c=candidate(&f,&mut r,&mut p);let b=boundary(&r);
 let n=std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
 let root=std::env::temp_dir().join(format!("spotarb_r1173_r1326_typed_{}_{}",std::process::id(),n));
 std::fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
 let mut writer=spotarb_native_market_core::native_r1173_compact_writer::Writer::create(&root).unwrap();
 p.consume_with_writer(&mut r,c,&f.receiver,&f.health,&b,&mut writer,||clocks()).unwrap();
 assert_eq!(r.execution.lifecycle().orders.len(),1);
 let(intents,events)=writer.trade_counts_r1326().unwrap();assert_eq!(intents,1);assert!(events>=1);
 writer.verify_pair().unwrap();let prior=(intents,events);drop(writer);
 let mut resumed=spotarb_native_market_core::native_r1173_compact_writer::Writer::resume(&root).unwrap();
 let records=r.execution.r1326_records_after(0).unwrap();
 for (sequence,record) in trade_records_r1326(&r.identity,records,std::collections::BTreeSet::new(),false,None).unwrap(){
  let witness=trade_event_witness_r1326(&r.identity,sequence,record).unwrap();
  resumed.commit_trade_event_r1326(witness,spotarb_native_market_core::native_r1173_compact_writer::Inject::None).unwrap();
 }
 assert_eq!(resumed.trade_counts_r1326().unwrap(),prior,"restart replay must be idempotent");resumed.verify_pair().unwrap();drop(resumed);std::fs::remove_dir_all(root).unwrap();
}

#[test] fn r1326i_risk_rejection_writes_no_trade_intent(){
 use std::os::unix::fs::DirBuilderExt;
 let(f,mut r,mut p)=setup();let c=candidate(&f,&mut r,&mut p);let mut b=boundary(&r);
 let n=std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
 let root=std::env::temp_dir().join(format!("spotarb_r1173_r1326i_risk_{}_{}",std::process::id(),n));
 std::fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
 let mut writer=spotarb_native_market_core::native_r1173_compact_writer::Writer::create(&root).unwrap();
 b.storage_healthy=false;
 assert!(p.consume_with_writer(&mut r,c,&f.receiver,&f.health,&b,&mut writer,||clocks()).is_err());
 assert!(r.execution.lifecycle().orders.is_empty());
 assert!(r.execution.registered_intents().unwrap().is_empty());
 assert_eq!(writer.trade_counts_r1326().unwrap(),(0,0));
 writer.verify_pair().unwrap();drop(writer);
 let mut resumed=spotarb_native_market_core::native_r1173_compact_writer::Writer::resume(&root).unwrap();
 resumed.recover().unwrap();assert_eq!(resumed.trade_counts_r1326().unwrap(),(0,0));
 resumed.verify_pair().unwrap();drop(resumed);std::fs::remove_dir_all(root).unwrap();
}

#[test] fn r1326_integrated_intent_mirror_failure_blocks_owner_order(){
 use std::os::unix::fs::DirBuilderExt;
 let(f,mut r,mut p)=setup();let c=candidate(&f,&mut r,&mut p);let b=boundary(&r);
 let n=std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
 let root=std::env::temp_dir().join(format!("spotarb_r1173_r1326_commit_fail_{}_{}",std::process::id(),n));
 std::fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
 let mut writer=spotarb_native_market_core::native_r1173_compact_writer::Writer::create(&root).unwrap();
 p.set_test_intent_inject(spotarb_native_market_core::native_r1173_compact_writer::Inject::AfterDb1);
 let error=p.consume_with_writer(&mut r,c,&f.receiver,&f.health,&b,&mut writer,||clocks()).unwrap_err();
 assert!(error.to_string().contains("injected_trade_intent_after_db1"),"fault point must be observed: {error:?}");
 assert!(r.execution.lifecycle().orders.is_empty(),"no Owner order may dispatch after mirror commit failure");
 assert!(r.execution.registered_intents().unwrap().is_empty(),"no Owner intent may publish before mirrored durability");
 assert!(writer.verify_pair().is_err(),"injected DB1/DB2 divergence must remain visible");
 drop(writer);
 let mut resumed=spotarb_native_market_core::native_r1173_compact_writer::Writer::resume(&root).unwrap();
 assert!(resumed.verify_pair().is_err(),"restart must detect unfinished DB1 mirror");
 resumed.recover().unwrap();
 assert_eq!(resumed.trade_counts_r1326().unwrap(),(1,0),"durable intent must survive DB1-only crash");
 assert_eq!(resumed.trade_intent_ids_r1326().unwrap().len(),1);
 resumed.verify_pair().unwrap();drop(resumed);
 let mut replay=spotarb_native_market_core::native_r1173_compact_writer::Writer::resume(&root).unwrap();
 replay.recover().unwrap();assert_eq!(replay.trade_counts_r1326().unwrap(),(1,0),"second recovery must not duplicate intent");
 replay.verify_pair().unwrap();drop(replay);
 assert!(r.execution.lifecycle().orders.is_empty(),"recovery of intent alone must not dispatch Owner");
 std::fs::remove_dir_all(root).unwrap();
}

#[test] fn r1327_db1_commit_error_rolls_back_and_dispatches_no_order(){
 let(f,mut r,mut p)=setup();let c=candidate(&f,&mut r,&mut p);let b=boundary(&r);
 let root=r1327_writer_root("db1_commit");
 let mut writer=spotarb_native_market_core::native_r1173_compact_writer::Writer::create(&root).unwrap();
 writer.r1327_fail_next_db1_commit();
 let error=p.consume_with_writer(&mut r,c,&f.receiver,&f.health,&b,&mut writer,||clocks()).unwrap_err();
 assert!(error.to_string().contains("commit_failed:injected_sqlite_commit_failure"));
 assert!(error.to_string().contains("transaction_rolled_back"));
 assert!(r.execution.lifecycle().orders.is_empty());
 assert!(r.execution.registered_intents().unwrap().is_empty());
 assert_eq!(writer.trade_counts_r1326().unwrap(),(0,0));
 writer.verify_pair().unwrap();drop(writer);
 let mut recovered=spotarb_native_market_core::native_r1173_compact_writer::Writer::resume(&root).unwrap();
 recovered.recover().unwrap();assert_eq!(recovered.trade_counts_r1326().unwrap(),(0,0));
 recovered.verify_pair().unwrap();drop(recovered);std::fs::remove_dir_all(root).unwrap();
}

#[test] fn r1327_db2_commit_error_recovers_one_intent_without_owner_dispatch(){
 let(f,mut r,mut p)=setup();let c=candidate(&f,&mut r,&mut p);let b=boundary(&r);
 let root=r1327_writer_root("db2_commit");
 let mut writer=spotarb_native_market_core::native_r1173_compact_writer::Writer::create(&root).unwrap();
 writer.r1327_fail_next_db2_commit();
 let error=p.consume_with_writer(&mut r,c,&f.receiver,&f.health,&b,&mut writer,||clocks()).unwrap_err();
 assert!(error.to_string().contains("commit_failed:injected_sqlite_commit_failure"));
 assert!(error.to_string().contains("transaction_rolled_back"));
 assert!(r.execution.lifecycle().orders.is_empty());
 assert!(r.execution.registered_intents().unwrap().is_empty());
 assert!(writer.verify_pair().is_err(),"DB1 intent must remain durable while DB2 commit is unresolved");
 drop(writer);
 let mut recovered=spotarb_native_market_core::native_r1173_compact_writer::Writer::resume(&root).unwrap();
 recovered.recover().unwrap();assert_eq!(recovered.trade_counts_r1326().unwrap(),(1,0));
 assert_eq!(recovered.trade_intent_ids_r1326().unwrap().len(),1);
 recovered.verify_pair().unwrap();drop(recovered);
 assert!(r.execution.lifecycle().orders.is_empty(),"recovery of intent cannot dispatch an Owner order");
 std::fs::remove_dir_all(root).unwrap();
}

#[test] fn r1327_sigkill_after_owner_dispatch_before_canonical_receipt(){
 use std::os::unix::process::ExitStatusExt;
 use std::process::{Command,Stdio};
 if std::env::var_os("SPOTARB_R1327_DISPATCH_CHILD").is_some(){
  let root=std::path::PathBuf::from(std::env::var("SPOTARB_R1327_DISPATCH_ROOT").unwrap());
  let(f,mut r,mut p)=setup();let c=candidate(&f,&mut r,&mut p);let b=boundary(&r);
  let journal=r.execution.journal_path().to_path_buf();
  std::fs::write(root.join("owner_meta.json"),serde_json::to_vec(&serde_json::json!({"identity":r.identity,"journal":journal})).unwrap()).unwrap();
  let mut writer=spotarb_native_market_core::native_r1173_compact_writer::Writer::create(&root).unwrap();
  let marker=root.join("owner_dispatched_before_trade_receipt");
  spotarb_native_market_core::native_r1173_compact_writer::r1327_before_trade_event_hook(Box::new(move||{
   std::fs::write(marker,b"durable Owner dispatch complete; canonical event not committed").unwrap();
   std::thread::sleep(std::time::Duration::from_secs(30));
  }));
  let _=p.consume_with_writer(&mut r,c,&f.receiver,&f.health,&b,&mut writer,||clocks());
  panic!("child passed dispatch-to-receipt seam without being killed");
 }
 let root=r1327_writer_root("dispatch_receipt_sigkill");
 let exe=std::env::current_exe().unwrap();
 let mut child=Command::new(exe).arg("r1327_sigkill_after_owner_dispatch_before_canonical_receipt")
  .arg("--test-threads=1").env("SPOTARB_R1327_DISPATCH_CHILD","1")
  .env("SPOTARB_R1327_DISPATCH_ROOT",&root)
  .stdout(Stdio::from(std::fs::File::create(root.with_extension("child_stdout.log")).unwrap()))
  .stderr(Stdio::from(std::fs::File::create(root.with_extension("child_stderr.log")).unwrap()))
  .spawn().unwrap();
 let marker=root.join("owner_dispatched_before_trade_receipt");let mut ready=false;
 for _ in 0..400{if marker.exists(){ready=true;break}if child.try_wait().unwrap().is_some(){break}std::thread::sleep(std::time::Duration::from_millis(25));}
 if !ready{let _=child.kill();let _=child.wait();panic!("child did not reach Owner-dispatch/canonical-receipt seam: {}",std::fs::read_to_string(root.with_extension("child_stderr.log")).unwrap_or_default())}
 child.kill().unwrap();let status=child.wait().unwrap();assert_eq!(status.signal(),Some(9),"actual SIGKILL required");
 let meta:serde_json::Value=serde_json::from_slice(&std::fs::read(root.join("owner_meta.json")).unwrap()).unwrap();
 let identity:isolated_policy::Identity=serde_json::from_value(meta["identity"].clone()).unwrap();
 let journal=std::path::PathBuf::from(meta["journal"].as_str().unwrap());
 let mut recovered_owner=new_isolated_identity_runtime(&identity,&journal).unwrap();
 let owner_order_ids=recovered_owner.execution.lifecycle().orders.keys().cloned().collect::<std::collections::BTreeSet<_>>();
 assert!(!owner_order_ids.is_empty(),"cold recovery must preserve dispatched Owner obligations");
 let records=recovered_owner.execution.r1326_records_after(0).unwrap();
 let mut writer=spotarb_native_market_core::native_r1173_compact_writer::Writer::resume(&root).unwrap();
 writer.recover().unwrap();assert_eq!(writer.trade_counts_r1326().unwrap(),(1,0),"intent survives and no canonical event committed before SIGKILL");
 let trade_id=trade_id_r1326(&identity).unwrap();let context=writer.trade_intent_context_r1326(&trade_id).unwrap().unwrap();
 let selected=trade_records_r1326(&identity,records,std::collections::BTreeSet::new(),false,
  Some((context.0.as_str(),context.1,context.2,context.3,context.4.as_str()))).unwrap();
 assert!(!selected.is_empty(),"cold Owner journal must yield replayable canonical events");
 let accepted_ids=selected.iter().filter(|(_,v)|v["event"]["kind"]=="Accepted")
  .filter_map(|(_,v)|v["event"]["order_id"].as_str().map(str::to_owned)).collect::<std::collections::BTreeSet<_>>();
 assert_eq!(accepted_ids,owner_order_ids,"Owner order census must equal accepted dispatched order identities");
 for (sequence,record) in selected{writer.commit_trade_event_r1326(trade_event_witness_r1326(&identity,sequence,record).unwrap(),
  spotarb_native_market_core::native_r1173_compact_writer::Inject::None).unwrap();}
 writer.verify_pair().unwrap();let recovered_counts=writer.trade_counts_r1326().unwrap();
 assert_eq!(recovered_counts.0,1);assert!(recovered_counts.1>0);drop(writer);
 let mut replay=spotarb_native_market_core::native_r1173_compact_writer::Writer::resume(&root).unwrap();replay.recover().unwrap();
 let replay_records=recovered_owner.execution.r1326_records_after(0).unwrap();let context=replay.trade_intent_context_r1326(&trade_id).unwrap().unwrap();
 for (sequence,record) in trade_records_r1326(&identity,replay_records,std::collections::BTreeSet::new(),false,
  Some((context.0.as_str(),context.1,context.2,context.3,context.4.as_str()))).unwrap(){
  replay.commit_trade_event_r1326(trade_event_witness_r1326(&identity,sequence,record).unwrap(),
   spotarb_native_market_core::native_r1173_compact_writer::Inject::None).unwrap();
 }
 assert_eq!(replay.trade_counts_r1326().unwrap(),recovered_counts,"cold replay must be idempotent");
 replay.verify_pair().unwrap();drop(replay);drop(recovered_owner);
 let journal_parent=journal.parent().unwrap().to_path_buf();std::fs::remove_dir_all(journal_parent).unwrap();
 std::fs::remove_dir_all(&root).unwrap();
 let _=std::fs::remove_file(root.with_extension("child_stdout.log"));let _=std::fs::remove_file(root.with_extension("child_stderr.log"));
}

#[test] fn r1326l_sigkill_after_db1_before_db2(){
 use std::os::unix::fs::DirBuilderExt;
 use std::os::unix::process::ExitStatusExt;
 use std::process::{Command,Stdio};
 let n=std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
 let root=std::env::temp_dir().join(format!("spotarb_r1173_r1326l_sigkill_{}_{}",std::process::id(),n));
 if std::env::var_os("SPOTARB_R1326L_CHILD").is_some(){
  let root=std::path::PathBuf::from(std::env::var("SPOTARB_R1326L_ROOT").unwrap());
  let(f,mut r,mut p)=setup();let c=candidate(&f,&mut r,&mut p);let b=boundary(&r);
  let mut w=spotarb_native_market_core::native_r1173_compact_writer::Writer::create(&root).unwrap();
  p.set_test_intent_inject(spotarb_native_market_core::native_r1173_compact_writer::Inject::AfterDb1);
  assert!(p.consume_with_writer(&mut r,c,&f.receiver,&f.health,&b,&mut w,||clocks()).unwrap_err().to_string().contains("injected_trade_intent_after_db1"));
  assert!(r.execution.lifecycle().orders.is_empty());
  assert!(w.verify_pair().is_err());
  drop(w);
  std::fs::write(root.join("db1_committed_ready_for_kill"),b"ready").unwrap();
  std::thread::sleep(std::time::Duration::from_secs(30));
  panic!("child was not SIGKILLed");
 }
 std::fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
 let exe=std::env::current_exe().unwrap();
 let mut child=Command::new(exe).arg("r1326l_sigkill_after_db1_before_db2")
  .arg("--test-threads=1").env("SPOTARB_R1326L_CHILD","1")
  .env("SPOTARB_R1326L_ROOT",&root).stdout(Stdio::from(std::fs::File::create(root.with_extension("child_stdout.log")).unwrap())).stderr(Stdio::from(std::fs::File::create(root.with_extension("child_stderr.log")).unwrap())).spawn().unwrap();
 let marker=root.join("db1_committed_ready_for_kill");
 let mut ready=false;
 for _ in 0..200{if marker.exists(){ready=true;break}if child.try_wait().unwrap().is_some(){break}std::thread::sleep(std::time::Duration::from_millis(25));}
 if !ready{let _=child.kill();let _=child.wait();panic!("child failed to reach DB1 durable fault point: {}",std::fs::read_to_string(root.with_extension("child_stdout.log")).unwrap_or_default())}
 child.kill().unwrap();let status=child.wait().unwrap();
 assert_eq!(status.signal(),Some(9),"actual SIGKILL required");
 let mut recovered=spotarb_native_market_core::native_r1173_compact_writer::Writer::resume(&root).unwrap();
 assert!(recovered.verify_pair().is_err());
 recovered.recover().unwrap();assert_eq!(recovered.trade_counts_r1326().unwrap(),(1,0));
 recovered.verify_pair().unwrap();drop(recovered);
 let mut second=spotarb_native_market_core::native_r1173_compact_writer::Writer::resume(&root).unwrap();
 second.recover().unwrap();assert_eq!(second.trade_counts_r1326().unwrap(),(1,0));
 second.verify_pair().unwrap();drop(second);
 std::fs::remove_dir_all(&root).unwrap();
 let _=std::fs::remove_file(root.with_extension("child_stdout.log"));
 let _=std::fs::remove_file(root.with_extension("child_stderr.log"));
}

#[test] fn r1326h_after_db2_ack_crash_recovers_one_intent_without_dispatch(){
 use std::os::unix::fs::DirBuilderExt;
 let(f,mut r,mut p)=setup();let c=candidate(&f,&mut r,&mut p);let b=boundary(&r);
 let n=std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
 let root=std::env::temp_dir().join(format!("spotarb_r1173_r1326h_after_db2_{}_{}",std::process::id(),n));
 std::fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
 let mut writer=spotarb_native_market_core::native_r1173_compact_writer::Writer::create(&root).unwrap();
 p.set_test_intent_inject(spotarb_native_market_core::native_r1173_compact_writer::Inject::AfterDb2);
 let error=p.consume_with_writer(&mut r,c,&f.receiver,&f.health,&b,&mut writer,||clocks()).unwrap_err();
 assert!(error.to_string().contains("injected_crash_after_db2"),"fault point must be observed: {error:?}");
 assert!(writer.verify_pair().is_ok(),"DB1 and DB2 have matching durable heads, but ACK is pending");
 assert!(r.execution.lifecycle().orders.is_empty());
 assert!(r.execution.registered_intents().unwrap().is_empty());
 drop(writer);
 let mut resumed=spotarb_native_market_core::native_r1173_compact_writer::Writer::resume(&root).unwrap();
 resumed.recover().unwrap();
 assert_eq!(resumed.trade_intent_ids_r1326().unwrap().len(),1);
 assert_eq!(resumed.trade_counts_r1326().unwrap(),(1,0));
 assert!(resumed.verify_pair().is_ok());
 drop(resumed);
 let mut second=spotarb_native_market_core::native_r1173_compact_writer::Writer::resume(&root).unwrap();
 second.recover().unwrap();
 assert_eq!(second.trade_counts_r1326().unwrap(),(1,0),"ACK replay must not duplicate intent");
 second.verify_pair().unwrap();drop(second);
 assert!(r.execution.lifecycle().orders.is_empty(),"repeated ACK recovery cannot dispatch an order");
 std::fs::remove_dir_all(root).unwrap();
}

#[test] fn r1326_trade_replay_binds_each_event_to_exact_order_and_owner(){
 let(f,r,_)=setup();let identity=r.identity.clone();let base=identity.profile.base.clone();
 let accepted=serde_json::json!({"event":{"kind":"Accepted","order_id":"owned-1","intent":{"order_id":"owned-1","instrument":base}},
  "lifecycle":{"orders":{"owned-1":{}}},"isolated":{"identity":identity}});
 let fill=serde_json::json!({"event":{"kind":"PartialFill","order_id":"owned-1","intent":null},
  "lifecycle":{"orders":{"owned-1":{},"foreign-2":{}}},"isolated":{"identity":r.identity}});
 let foreign=serde_json::json!({"event":{"kind":"PartialFill","order_id":"foreign-2","intent":null},
  "lifecycle":{"orders":{"owned-1":{},"foreign-2":{}}},"isolated":{"identity":r.identity}});
 let other_identity=serde_json::json!({"run_id":"other-owner","clock_domain":identity.clock_domain,
  "profile":identity.profile,"exit":null});
 let foreign_accepted=serde_json::json!({"event":{"kind":"Accepted","order_id":"foreign-3",
  "intent":{"order_id":"foreign-3","instrument":"ZZZUSDT"}},
  "lifecycle":{"orders":{"foreign-3":{}}},"isolated":{"identity":other_identity}});
 let census=serde_json::json!({"event":{"kind":"OrderCensus","order_id":"owner","census_order_ids":["owned-1"]},
  "lifecycle":{"orders":{"owned-1":{}}},"isolated":{"identity":identity}});
 let recovery=serde_json::json!({"event":{"kind":"Recovery","order_id":"recovery:5"},
  "lifecycle":{"orders":{"owned-1":{}}},"isolated":{"identity":r.identity}});
 let got=trade_records_r1326(&r.identity,vec![(1,accepted.clone()),(2,fill.clone()),(3,foreign),
  (4,foreign_accepted),(5,census),(6,recovery)],std::collections::BTreeSet::new(),false,None).unwrap();
 assert_eq!(got.iter().map(|v|v.0).collect::<Vec<_>>(),vec![1,2,5,6]);
 let mut wrong_context=accepted.clone();wrong_context["event"]["intent"]["instrument"]=serde_json::json!("UNRELATED-PERP");
 assert!(trade_records_r1326(&r.identity,vec![(4,wrong_context)],
  std::collections::BTreeSet::new(),false,None).is_err());
 let mut wrong_owner=accepted.clone();wrong_owner["isolated"]["identity"]["run_id"]=serde_json::json!("other-run");
 assert!(trade_records_r1326(&r.identity,vec![(5,wrong_owner)],
  std::collections::BTreeSet::from(["owned-1".to_string()]),true,None).is_err());
 let mut missing_order=fill;missing_order["event"]["order_id"]=serde_json::json!("");
 assert!(trade_records_r1326(&r.identity,vec![(6,missing_order)],
  std::collections::BTreeSet::from(["owned-1".to_string()]),true,None).is_err());
 let intent_id="entry-intent";
 let mut canonical=accepted.clone();
 canonical["event"]["intent"]["intent_id"]=serde_json::json!(intent_id);
 canonical["event"]["intent"]["leg"]=serde_json::json!("Spot");
 canonical["event"]["intent"]["side"]=serde_json::json!("Buy");
 canonical["event"]["intent"]["exit_reason"]=serde_json::Value::Null;
 canonical["isolated"]["full"]=serde_json::json!({"generation":7});
 canonical["universe"]=serde_json::json!({"generation":3});
 canonical["isolated"]["keys"]=serde_json::json!({"entry:7":"owned-1"});
 canonical["isolated"]["intents"]=serde_json::json!({"owned-1":{"command_key":"entry:7","intent":{"intent_id":intent_id,"order_id":"owned-1"},"witness":{"owner_revision":4,"context":{"input_id":"decision-canonical","clock_domain":r.identity.clock_domain}}}});
 assert!(trade_records_r1326(&r.identity,vec![(5,canonical.clone())],std::collections::BTreeSet::new(),false,Some(("entry:7",3,7,4,"decision-canonical"))).is_ok());
 let mut hedge=canonical.clone();
 hedge["event"]["order_id"]=serde_json::json!("hedge-2");
 hedge["event"]["intent"]["order_id"]=serde_json::json!("hedge-2");
 hedge["event"]["intent"]["intent_id"]=serde_json::json!("hedge-intent");
 hedge["event"]["intent"]["leg"]=serde_json::json!("Futures");
 hedge["event"]["intent"]["side"]=serde_json::json!("Short");
 hedge["event"]["intent"]["instrument"]=serde_json::json!(r.identity.profile.futures_symbol);
 hedge["isolated"]["keys"]["hedge:1"]=serde_json::json!("hedge-2");
 hedge["isolated"]["intents"]["hedge-2"]=serde_json::json!({"command_key":"hedge:1","intent":{"intent_id":"hedge-intent","order_id":"hedge-2"},"witness":{"owner_revision":5,"context":{"input_id":"decision-hedge","clock_domain":r.identity.clock_domain}}});
 hedge["lifecycle"]["orders"]["hedge-2"]=serde_json::json!({});
 assert_eq!(trade_records_r1326(&r.identity,vec![(5,canonical.clone()),(6,hedge)],std::collections::BTreeSet::new(),false,Some(("entry:7",3,7,4,"decision-canonical"))).unwrap().len(),2);
 assert!(trade_records_r1326(&r.identity,vec![(5,canonical.clone())],std::collections::BTreeSet::new(),false,Some(("entry:8",3,7,4,"decision-canonical"))).is_err());
 let mut wrong_sequence=canonical.clone();
 assert!(trade_records_r1326(&r.identity,vec![(6,wrong_sequence.clone())],std::collections::BTreeSet::new(),false,Some(("entry:7",3,7,4,"decision-canonical"))).is_err());
 wrong_sequence["universe"]["generation"]=serde_json::json!(4);
 assert!(trade_records_r1326(&r.identity,vec![(5,wrong_sequence)],std::collections::BTreeSet::new(),false,Some(("entry:7",3,7,4,"decision-canonical"))).is_err());
 assert!(trade_records_r1326(&r.identity,Vec::new(),std::collections::BTreeSet::new(),false,Some(("entry:7",3,7,4,"decision-canonical"))).is_err());
 let mut unowned=accepted.clone();unowned["isolated"]=serde_json::json!({});
 assert!(trade_records_r1326(&r.identity,vec![(7,unowned)],std::collections::BTreeSet::new(),false,None).is_err());
 let _=f;
}

#[test] fn r1326_legacy_discovery_uses_durable_owner_and_order_state(){
 let(_f,r,_)=setup();let identity=r.identity.clone();
 let make=|instrument:Option<&str>,kind:&str,status:&str|{
  let mut event=serde_json::json!({"order_id":"legacy-order","kind":kind,"intent":{"order_id":"legacy-order"}});
  if let Some(i)=instrument{event["intent"]["instrument"]=serde_json::json!(i);}
  serde_json::json!({"event":event,"lifecycle":{"orders":{"legacy-order":{"status":status}}},"isolated":{"identity":identity}})
 };
 let current=make(Some(&identity.profile.base),"Accepted","Open");
 let excluded_universe=make(Some("REMOVEDCOIN-PERP"),"PartialFill","PartiallyFilled");
 let missing_instrument=make(None,"Unknown","Unknown");
 let closed=make(None,"FullFill","Filled");
 for record in [&current,&excluded_universe,&missing_instrument,&closed]{
  assert!(record_matches_identity_r1326(&identity,record));
  assert!(record_has_order_obligation_r1326(record));
 }
 let orphan=serde_json::json!({"event":{"order_id":"legacy-order","kind":"Unknown"},
  "lifecycle":{"orders":{"legacy-order":{"status":"Unknown"}}}});
 assert!(!record_matches_identity_r1326(&identity,&orphan));
 assert!(record_has_order_obligation_r1326(&orphan),"unidentifiable durable obligation must be surfaced for global fail-closed handling");
 let census=serde_json::json!({"event":{"kind":"OrderCensus","order_id":"owner","census_order_ids":["legacy-order"]},
  "lifecycle":{"orders":{"legacy-order":{"status":"Filled"}}}});
 assert!(record_has_order_obligation_r1326(&census));
 let malformed=serde_json::json!({"event":{"order_id":""},"lifecycle":{"orders":{}}});
 assert!(!record_has_order_obligation_r1326(&malformed));
}

#[test] fn r1327_sqlite_os_eio_enospc_owner_recovery(){
 use std::os::unix::fs::DirBuilderExt;
 use std::process::{Command,Stdio};
 if std::env::var_os("SPOTARB_R1327_FAULT_CHILD").is_some(){
  let root=std::path::PathBuf::from(std::env::var("SPOTARB_R1327_FAULT_ROOT").unwrap());
  let(f,mut r,mut p)=setup();let c=candidate(&f,&mut r,&mut p);let b=boundary(&r);
  let journal=r.execution.journal_path().to_path_buf();
  std::fs::write(root.join("owner_meta.json"),serde_json::to_vec(&serde_json::json!({"identity":r.identity,"journal":journal})).unwrap()).unwrap();
  let mut writer=spotarb_native_market_core::native_r1173_compact_writer::Writer::create(&root).unwrap();
  std::fs::write(root.join("arm"),b"inject after Writer open").unwrap();
  let err=p.consume_with_writer(&mut r,c,&f.receiver,&f.health,&b,&mut writer,||clocks()).unwrap_err();
  assert!(!err.to_string().is_empty(),"SQLite syscall fault must fail closed");
  assert!(r.execution.lifecycle().orders.is_empty(),"no Owner order after DB2 storage fault");
  assert!(r.execution.registered_intents().unwrap().is_empty(),"intent must not publish to Owner before mirrored durability");
  assert!(root.join("hit").exists(),"fault shim did not intercept a real SQLite write");
  drop(writer);return;
 }
 let source=include_str!("../tests/r1327_write_fault_preload.c");
 for mode in ["EIO","ENOSPC"]{
  let root=r1327_writer_root(&format!("os_{mode}"));
  let shim=root.join("fault.so");
  let cc=Command::new("cc").args(["-shared","-fPIC","-O2","-x","c","-","-o"]).arg(&shim).stdin(Stdio::piped()).spawn().unwrap();
  let mut cc=cc;use std::io::Write;cc.stdin.take().unwrap().write_all(source.as_bytes()).unwrap();
  assert!(cc.wait().unwrap().success(),"could not compile syscall fault shim");
  std::fs::create_dir_all(&root).unwrap();
  let out=std::fs::File::create(root.with_extension("stdout")).unwrap();
  let err=std::fs::File::create(root.with_extension("stderr")).unwrap();
  let status=Command::new(std::env::current_exe().unwrap())
   .arg("r1327_sqlite_os_eio_enospc_owner_recovery").arg("--test-threads=1")
   .env("SPOTARB_R1327_FAULT_CHILD","1").env("SPOTARB_R1327_FAULT_ROOT",&root)
   .env("SPOTARB_R1327_ARM_FILE",root.join("arm")).env("SPOTARB_R1327_HIT_FILE",root.join("hit"))
   .env("SPOTARB_R1327_DB_TARGET","db2.sqlite").env("SPOTARB_R1327_FAULT_MODE",mode)
   .env("LD_PRELOAD",&shim).stdout(Stdio::from(out)).stderr(Stdio::from(err)).status().unwrap();
  assert!(status.success(),"child failed in {mode}; stderr={}",std::fs::read_to_string(root.with_extension("stderr")).unwrap_or_default());
  let hit=std::fs::read_to_string(root.join("hit")).unwrap();
  assert!(hit.contains(mode)&&hit.contains("db2.sqlite"),"unexpected injected target: {hit}");
  let meta:serde_json::Value=serde_json::from_slice(&std::fs::read(root.join("owner_meta.json")).unwrap()).unwrap();
  let identity:isolated_policy::Identity=serde_json::from_value(meta["identity"].clone()).unwrap();
  let journal=std::path::PathBuf::from(meta["journal"].as_str().unwrap());
  let mut owner=new_isolated_identity_runtime(&identity,&journal).unwrap();
  assert!(owner.execution.lifecycle().orders.is_empty(),"recovered Owner must not invent orders");
  let mut writer=spotarb_native_market_core::native_r1173_compact_writer::Writer::resume(&root).unwrap();
  writer.recover().unwrap();
  assert_eq!(writer.trade_counts_r1326().unwrap(),(1,0),"durable DB1 intent must replay once after DB2 OS fault");
  assert_eq!(writer.trade_intent_ids_r1326().unwrap().len(),1);
  writer.verify_pair().unwrap();drop(writer);drop(owner);
  let parent=journal.parent().unwrap().to_path_buf();std::fs::remove_dir_all(parent).unwrap();
  std::fs::remove_dir_all(&root).unwrap();
  let _=std::fs::remove_file(root.with_extension("stdout"));let _=std::fs::remove_file(root.with_extension("stderr"));
 }
}


// Same R1410A offered sequence. Preserve the original baseline evidence
// on the server. Historical A must reach Decision but never mint a candidate.
#[test]
fn r1410b_unique_snapshot_decision_census(){
 let mut expected_rows=None;
 for queued in [false,true]{
  let(mut f,mut r,mut p)=setup();
  let spot=f.inputs.iter().find(|i|i.venue()==Venue::OkxSpot).unwrap().clone();
  let a=f.inputs.iter().find(|i|i.venue()==Venue::BinanceFutures).unwrap().clone();
  let b=boundary(&r);
  assert!(p.observe(&mut r,spot,&f.receiver,&f.health,&b).unwrap().is_none());
  let mut executable_a=false;
  if !queued{
   executable_a=p.observe(&mut r,a.clone(),&f.receiver,&f.health,&b).unwrap().is_some();
   assert!(executable_a);
  }
  advance_clock_ns(1_000_000);let(n,w)=clocks();
  let mut physical=frames(n,w,103,false,"20");
  physical.retain(|frame|frame.venue==Venue::BinanceFutures);
  for frame in &mut physical{
   let mut v:Value=serde_json::from_str(&frame.raw).unwrap();
   v["data"]["b"]=json!([["100.10","20"],["100.09","20"]]);
   v["data"]["a"]=json!([["100.11","20"],["100.12","20"]]);
   frame.raw=v.to_string();
  }
  let mut current=f.feed_r1322(physical,n,w);assert_eq!(current.len(),1);
  let next=current.remove(0);
  assert_eq!(a.source_provenance().session_id,next.source_provenance().session_id);
  assert_eq!(a.source_provenance().selection_epoch,next.source_provenance().selection_epoch);
  assert_ne!(a.input_id(),next.input_id());
  assert_ne!(a.book().bids,next.book().bids);
  if queued{
   assert_eq!(bridge1301::revalidate_r1322(&a,&f.receiver,n,w),Err(bridge1301::Denial::Generation));
   assert_eq!(bridge1301::observation_r1410(&a,&f.receiver,n,w),Ok(false));
   assert!(p.observe(&mut r,a.clone(),&f.receiver,&f.health,&b).unwrap().is_none());
   assert!(!p.terminal);
  }
  assert!(p.observe(&mut r,next,&f.receiver,&f.health,&b).unwrap().is_none());
  assert!(!p.terminal);assert!(r.execution.lifecycle().orders.is_empty());
  let rows=r.actions.iter().filter(|x|x["action"]=="typed_decision").collect::<Vec<_>>();
  assert_eq!(rows.len(),3);
  assert_eq!(rows[1]["observation_only"],json!(queued));
  assert!(rows[1]["rows"].as_array().unwrap().iter().any(|row|row["reason"]=="EXECUTABLE_OPPORTUNITY"));
  assert!(rows[2]["rows"].as_array().unwrap().iter().all(|row|row["reason"]!="EXECUTABLE_OPPORTUNITY"));
  let reasons=rows.iter().map(|x|x["rows"].as_array().unwrap().iter().map(|row|(row["reason"].clone(),row["risk_result"].clone(),row["quantity"].clone())).collect::<Vec<_>>()).collect::<Vec<_>>();
  if let Some(expected)=&expected_rows{assert_eq!(&reasons,expected);}else{expected_rows=Some(reasons);}
  println!("R1410B_CENSUS queued={queued} physical_frames=14 unique_selected=3 typed_decisions=3 observation_only_A={queued} execution_candidate_A={executable_a} B_processed=true terminal=false orders=0");
 }
}

#[test]
fn r1410b_destructive_replacement_not_historical(){
 let(mut f,mut r,mut p)=setup();
 let a=f.inputs.iter().find(|i|i.venue()==Venue::BinanceFutures).unwrap().clone();
 let source=a.source_provenance().session_id.clone();
 f.engine.mark_reconnecting(&source);
 advance_clock_ns(1_000_000);let(n,w)=clocks();
 assert!(bridge1301::observation_r1410(&a,&f.receiver,n,w).is_err());
 let b=boundary(&r);assert!(p.observe(&mut r,a,&f.receiver,&f.health,&b).is_err());
 assert!(p.terminal);assert!(r.execution.lifecycle().orders.is_empty());
}


#[test]
fn r1410c_actual_decision_stream_beyond_4096_and_old_replay(){
 let(mut f,mut r,mut p)=setup();
 let b=boundary(&r);
 for input in f.inputs.clone(){p.observe(&mut r,input,&f.receiver,&f.health,&b).unwrap();}
 r.actions.clear();
 let mut recent=None;
 for seq in 103..=2202i64{
  advance_clock_ns(1_000_000);let(n,w)=clocks();
  let inputs=f.feed_r1322(frames(n,w,seq,false,"20"),n,w);
  assert_eq!(inputs.len(),2);
  for input in inputs{
   if seq==2201&&input.venue()==Venue::BinanceFutures{recent=Some(input.clone());}
   p.observe(&mut r,input,&f.receiver,&f.health,&b).unwrap();
  }
  assert_eq!(r.actions.iter().filter(|a|a["action"]=="typed_decision").count(),2);
  r.actions.clear();
 }
 assert_eq!(r.frames_seen,4202);assert!(r.seen_inputs.is_empty());
 assert!(p.last_observed.iter().all(|m|m.as_ref().is_some_and(|m|m.3==2202)));
 assert!(r.execution.lifecycle().orders.is_empty());
 // The previous snapshot is ordinary-superseded and fresh, not live.
 let old=recent.unwrap();let(n,w)=clocks();
 assert_eq!(bridge1301::observation_r1410(&old,&f.receiver,n,w),Ok(false));
 // Pair eviction must not erase replay protection.
 p.pair=[None,None];
 let before=r.frames_seen;
 let err=p.observe(&mut r,old,&f.receiver,&f.health,&b).err().expect("old selected event denied");
 assert!(err.to_string().contains("duplicate/conflicting event"));
 assert_eq!(r.frames_seen,before);assert!(p.terminal);assert!(r.execution.lifecycle().orders.is_empty());
 println!("R1410C_WITNESS 4202 actual Decision updates; two retained watermarks; old fresh event denied after pair eviction; orders0");
}
