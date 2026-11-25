// Prepared for Luna; not executed by Astra.
use super::*;
use risk::{FillLeg,OrderStatus,LifecycleState};
fn identity()->hedge::Identity{
 serde_json::from_value(serde_json::json!({"run_id":"r1296-test","clock_domain":"synthetic-r1296",
 "profile":serde_json::from_str::<serde_json::Value>(include_str!("../fixtures/r1296/synthetic_profile.json")).unwrap()})).unwrap()
}
pub(super) fn context(n:u64)->hedge::MarketContext{hedge::MarketContext{input_id:format!("input-{n}"),clock_domain:"synthetic-r1296".into(),now_ns:1_000_000_000+n*1_000_000,wall_ms:1_700_000_000_000+n as i64,spot_hot:true,futures_hot:true}}
pub(super) fn books(n:u64,depth:f64)->(Book,Book){
 let c=context(n);let b=Book{bids:vec![(0.99,depth)],asks:vec![(1.,depth)],received_ns:c.now_ns,
 received_wall_ms:c.wall_ms,exchange_ts_ms:Some(c.wall_ms),changed_ns:c.now_ns,epoch:1,sequence:Some(n as i64)};
 let mut f=b.clone();f.bids[0].0=1.01;f.asks[0].0=1.02;(b,f)
}
fn owner()->DurablePaper{
 let n=std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
 let p=PathBuf::from(format!("/mnt/spotarb_db_test/r1273_r1275_engine_latency_20261111/R1296/lab/tests/{}-{n}/paper.jsonl",std::process::id()));
 DurablePaper::create_isolated(p,identity()).unwrap()
}
fn spot(d:&mut DurablePaper)->String{
 let(s,f)=books(1,20.);d.ensure_isolated_intent(d.sequence(),"entry:1".into(),FillLeg::Spot,2.,&s,&f,&context(1)).unwrap().intent.order_id
}
fn fill(d:&mut DurablePaper,id:&str,n:u64,depth:f64)->SimExecutionEvent{
 let(s,f)=books(n,depth);d.simulate_isolated_order(id,&s,&f,&context(n)).unwrap().unwrap()
}
fn hedge_order(d:&mut DurablePaper,n:u64)->RegisteredIntent{
 let(s,f)=books(n,20.);let c=context(n);
 let hedge::HedgeDecision::Short{quantity,key}=hedge::plan_hedge(d.isolated_identity().unwrap(),d.lifecycle(),d.sequence(),&s,&f,&c) else{panic!("expected SHORT policy")};
 d.ensure_isolated_intent(d.sequence(),key,FillLeg::Futures,quantity,&s,&f,&c).unwrap()
}
fn with_spot(depth:f64)->(DurablePaper,String){let mut d=owner();let id=spot(&mut d);fill(&mut d,&id,2,depth);(d,id)}
fn restart(d:DurablePaper)->DurablePaper{let p=d.path.clone();drop(d);DurablePaper::recover_isolated(p,&identity()).unwrap()}
#[test] fn r1296_01_full_spot_hedge(){let(mut d,_)=with_spot(20.);let h=hedge_order(&mut d,3);assert_eq!(h.intent.quantity,2.);assert_eq!(h.intent.side,risk::FillSide::Short);fill(&mut d,&h.intent.order_id,4,20.);assert_eq!(d.projection.residual_base,0.);assert_eq!(d.lifecycle.state,LifecycleState::Hedged);}
#[test] fn r1296_02_partial_spot_proportional(){let(mut d,_)=with_spot(0.5);assert_eq!(hedge_order(&mut d,3).intent.quantity,0.5);}
#[test] fn r1296_03_partial_hedge(){let(mut d,_)=with_spot(20.);let h=hedge_order(&mut d,3);assert_eq!(fill(&mut d,&h.intent.order_id,4,0.5).kind,SimEventKind::PartialFill);assert_eq!(d.lifecycle.orders[&h.intent.order_id].remaining_quantity,1.5);fill(&mut d,&h.intent.order_id,5,20.);assert_eq!(d.projection.residual_base,0.);}
#[test] fn r1296_04_duplicate_spot(){let mut d=owner();let id=spot(&mut d);let e=fill(&mut d,&id,2,20.);let seq=d.sequence();d.retry_isolated_execution(&e).unwrap();assert_eq!(seq,d.sequence());assert_eq!(d.projection.spot_base,2.);}
#[test] fn r1296_05_duplicate_hedge(){let(mut d,_)=with_spot(20.);let h=hedge_order(&mut d,3);let e=fill(&mut d,&h.intent.order_id,4,20.);let seq=d.sequence();d.retry_isolated_execution(&e).unwrap();assert_eq!(seq,d.sequence());assert_eq!(d.projection.futures_base,2.);}
#[test] fn r1296_06_restart_before_registration(){let(d,_)=with_spot(20.);let d=restart(d);assert_eq!(d.projection.spot_base,2.);assert_eq!(d.lifecycle.orders.len(),1);assert!(!d.exposure_view().known);}
#[test] fn r1296_07_restart_after_registration(){let(mut d,_)=with_spot(20.);let h=hedge_order(&mut d,3);let mut d=restart(d);assert_eq!(d.lifecycle.orders.len(),2);let(s,f)=books(4,20.);let r=d.ensure_isolated_intent(0,h.command_key,FillLeg::Futures,2.,&s,&f,&context(4)).unwrap();assert_eq!(r.intent.order_id,h.intent.order_id);assert!(!d.exposure_view().known);}
#[test] fn r1296_08_restart_partial(){let(mut d,_)=with_spot(20.);let h=hedge_order(&mut d,3);fill(&mut d,&h.intent.order_id,4,0.5);let d=restart(d);assert_eq!(d.projection.futures_base,0.5);assert_eq!(d.lifecycle.orders[&h.intent.order_id].remaining_quantity,1.5);assert!(!d.exposure_view().known);}
#[test] fn r1296_09_reject(){let(mut d,_)=with_spot(20.);let h=hedge_order(&mut d,3);d.control_isolated(IsolatedControl::Rejected{order_id:h.intent.order_id},&context(4)).unwrap();assert_eq!(d.lifecycle.state,LifecycleState::RecoveryRequired);assert_eq!(d.projection.spot_base,2.);}
#[test] fn r1296_10_timeout(){let(mut d,_)=with_spot(20.);let h=hedge_order(&mut d,3);d.control_isolated(IsolatedControl::Timeout{order_id:h.intent.order_id.clone()},&context(4)).unwrap();assert!(d.lifecycle.orders[&h.intent.order_id].uncertain);assert!(!d.exposure_view().known);}
#[test] fn r1296_11_cancel_pending(){let(mut d,_)=with_spot(20.);let h=hedge_order(&mut d,3);d.control_isolated(IsolatedControl::CancelRequested{order_id:h.intent.order_id.clone()},&context(4)).unwrap();assert_eq!(d.lifecycle.orders[&h.intent.order_id].status,OrderStatus::CancelPending);assert_eq!(d.lifecycle.orders[&h.intent.order_id].remaining_quantity,2.);let(s,f)=books(5,20.);assert!(matches!(hedge::plan_hedge(&identity(),d.lifecycle(),d.sequence(),&s,&f,&context(5)),hedge::HedgeDecision::WaitExisting));}
#[test] fn r1296_12_late_fill(){let(mut d,_)=with_spot(20.);let h=hedge_order(&mut d,3);d.control_isolated(IsolatedControl::CancelRequested{order_id:h.intent.order_id.clone()},&context(4)).unwrap();d.control_isolated(IsolatedControl::Cancelled{order_id:h.intent.order_id.clone()},&context(5)).unwrap();let(_,f)=books(6,20.);let e=simulate_marketable(&h.intent,0.5,1,&f,true,context(6).now_ns,context(6).wall_ms).unwrap().pop().unwrap();let ctl=IsolatedControl::LateFill{event:e.clone()};d.control_isolated(ctl.clone(),&context(6)).unwrap();let seq=d.sequence();d.control_isolated(ctl,&context(6)).unwrap();assert_eq!(seq,d.sequence());assert!(d.isolated_events().unwrap().contains_key(&e.event_id));assert!(!d.exposure_view().known);assert_eq!(d.projection.futures_base,0.);}
#[test] fn r1296_13_stale(){let(mut d,_)=with_spot(20.);let(s,f)=books(3,20.);let mut c=context(3);c.now_ns+=800_000_001;assert!(hedge::validate_pair(&identity(),&s,&f,&c).is_err());d.control_isolated(IsolatedControl::StaleMarket,&c).unwrap();assert!(!d.exposure_view().known);}
#[test] fn r1296_14_hot_loss(){let(mut d,_)=with_spot(20.);let mut c=context(3);c.futures_hot=false;d.control_isolated(IsolatedControl::Clock,&c).unwrap();assert_eq!(d.lifecycle.state,LifecycleState::RecoveryRequired);}
#[test] fn r1296_15_invalid_rules(){for(mult,step)in[("0","0.001"),("2","0.001"),("1","0")]{let mut i=identity();i.profile.multiplier=mult.into();i.profile.step=step.into();assert!(i.hash().is_err());}}
#[test] fn r1296_16_rounding(){let(mut d,_)=with_spot(0.5005);let h=hedge_order(&mut d,3);assert_eq!(h.intent.quantity,0.5);assert!(h.intent.quantity<=d.projection.spot_base);}
#[test] fn r1296_17_unsupported_instrument(){let mut i=identity();i.profile.synthetic=false;assert!(i.hash().is_err());i=identity();i.profile.product="INVERSE".into();assert!(i.hash().is_err());}
#[test] fn r1296_18_second_writer(){let d=owner();assert!(DurablePaper::recover_isolated(&d.path,&identity()).is_err());}
#[test] fn r1296_19_commit_error(){
 // This local write-error test is PREPARED, NOT RUN by Astra.
 let mut d=owner();let seq=d.sequence();let before=serde_json::to_value(&d.projection).unwrap();
 let _held_lock=d.lock.try_clone().unwrap();d.lock=OpenOptions::new().write(true).open("/dev/full").unwrap();
 assert!(matches!(d.control_isolated(IsolatedControl::RecoveryIncident{reason:"prepared fault".into()},&context(1)),Err(ExecutionError::UncertainCommit(_))));
 assert!(d.is_fenced());assert_eq!(d.sequence(),seq);assert_eq!(serde_json::to_value(&d.projection).unwrap(),before);
}
#[test] fn r1296_20_no_legacy_mutation(){
 let(mut d,_)=with_spot(20.);let h=hedge_order(&mut d,3);let(s,f)=books(4,20.);
 let gate=risk::MarketGate{spot_hot:true,futures_hot:true,spot_book_valid:true,futures_book_valid:true};
 assert!(d.commit_order_intent(h.intent.clone(),&s,&f,&gate,context(4).now_ns,context(4).wall_ms).is_err());
 assert!(d.commit_census(vec![],context(4).now_ns,context(4).wall_ms).is_err());
 assert!(!include_str!("native_paper_isolated_r1296.rs").contains(".on_frame("));
}

#[test] fn r1296_manifest_mismatch_does_not_append(){
 let(d,_)=with_spot(20.);let p=d.path.clone();drop(d);let before=std::fs::read(&p).unwrap();
 let mut wrong=identity();wrong.run_id="other-run".into();assert!(DurablePaper::recover_isolated(&p,&wrong).is_err());
 assert_eq!(std::fs::read(&p).unwrap(),before);
}
#[test] fn r1296_incomplete_tail_is_not_repaired(){
 let d=owner();let p=d.path.clone();drop(d);
 OpenOptions::new().append(true).open(&p).unwrap().write_all(b"{").unwrap();
 let before=std::fs::read(&p).unwrap();assert!(DurablePaper::recover_isolated(&p,&identity()).is_err());assert_eq!(std::fs::read(&p).unwrap(),before);
}
#[test] fn r1296_repeated_snapshot_never_reuses_liquidity(){
 let mut d=owner();let id=spot(&mut d);fill(&mut d,&id,2,0.5);
 let(mut s,mut f)=books(2,0.5);s.received_ns=context(3).now_ns;f.received_ns=context(3).now_ns;
 assert!(d.simulate_isolated_order(&id,&s,&f,&context(3)).unwrap().is_none());
 assert_eq!(d.projection.spot_base,0.5);
}
#[test] fn r1296_unknown_fill_cannot_autoregister(){
 let mut d=owner();let(s,_)=books(2,20.);
 let i=OrderIntent{intent_id:"unknown".into(),order_id:"unknown".into(),leg:FillLeg::Spot,side:risk::FillSide::Buy,
 quantity:1.,instrument:"AAA-USDT".into(),created_wall_ms:context(1).wall_ms,created_mono_ns:context(1).now_ns,exit_reason:None};
 let e=simulate_marketable(&i,1.,1,&s,true,context(2).now_ns,context(2).wall_ms).unwrap().pop().unwrap();
 assert!(d.commit_execution(e,&i,1.,1).is_err());assert!(d.lifecycle.orders.is_empty());
}

#[test] fn r1322_existing_owner_storage_failure_fences_before_publication(){
 for point in [storage_eio::Point::Append,storage_eio::Point::Sync]{
  let mut d=owner();let(s,f)=books(1,20.);let c=context(1);let seq=d.sequence();
  let fault=storage_eio::arm(d.journal_path(),point);
  let result=d.ensure_isolated_intent(seq,d.isolated_entry_key(),FillLeg::Spot,2.,&s,&f,&c);
  assert!(matches!(result,Err(ExecutionError::UncertainCommit(_))));drop(fault);
  assert!(d.is_fenced());assert_eq!(d.sequence(),seq);assert!(d.lifecycle().orders.is_empty());
  let again=d.ensure_isolated_intent(seq,d.isolated_entry_key(),FillLeg::Spot,2.,&s,&f,&c);
  assert!(matches!(again,Err(ExecutionError::OwnerFenced)));assert!(d.lifecycle().orders.is_empty());
 }
}
