// Owner contract tests. These are NOT entrypoint/E2E tests. Prepared, not run by Astra.
use super::*;
use risk::{FillLeg,OrderStatus,LifecycleState};
use super::r1296_tests::{books,context};
fn identity()->hedge::Identity{let v:serde_json::Value=serde_json::from_str(include_str!("../fixtures/r1298/full_exit.json")).unwrap();serde_json::from_value(v["identity"].clone()).unwrap()}
fn owner()->DurablePaper{
 let n=std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
 DurablePaper::create_isolated(format!("/mnt/spotarb_db_test/r1273_r1275_engine_latency_20261111/R1298/lab/tests/{}-{n}/journal.jsonl",std::process::id()),identity()).unwrap()
}
fn fill(d:&mut DurablePaper,id:&str,n:u64,q:f64)->SimExecutionEvent{let(s,f)=books(n,q);d.simulate_isolated_order(id,&s,&f,&context(n)).unwrap().unwrap()}
fn spot()->DurablePaper{let mut d=owner();let(s,f)=books(1,20.);let i=d.ensure_isolated_intent(d.sequence(),d.isolated_entry_key(),FillLeg::Spot,2.,&s,&f,&context(1)).unwrap();fill(&mut d,&i.intent.order_id,2,20.);d}
fn hedged()->DurablePaper{let mut d=spot();let(s,f)=books(3,20.);let hedge::HedgeDecision::Short{quantity,key}=hedge::plan_hedge(&identity(),d.lifecycle(),d.sequence(),&s,&f,&context(3)) else{panic!("hedge")};let h=d.ensure_isolated_intent(d.sequence(),key,FillLeg::Futures,quantity,&s,&f,&context(3)).unwrap();fill(&mut d,&h.intent.order_id,4,20.);d}
fn exit(d:&mut DurablePaper,n:u64)->RegisteredIntent{let(s,f)=books(n,20.);d.register_isolated_exit(&s,&f,&context(n)).unwrap()}
fn cover_pending()->(DurablePaper,String){let mut d=hedged();d.request_isolated_exit(&context(5)).unwrap();let h=exit(&mut d,6);(d,h.intent.order_id)}
fn covered()->DurablePaper{let(mut d,id)=cover_pending();fill(&mut d,&id,7,20.);d}
fn sell_pending()->(DurablePaper,String){let mut d=covered();let i=exit(&mut d,8);(d,i.intent.order_id)}
fn zero()->DurablePaper{let(mut d,id)=sell_pending();fill(&mut d,&id,9,20.);d}
fn closed()->DurablePaper{let mut d=zero();d.close_isolated_generation(&context(10)).unwrap();d}
fn restart(d:DurablePaper)->DurablePaper{let p=d.path.clone();drop(d);DurablePaper::recover_isolated(p,&identity()).unwrap()}
#[test] fn r1298_01_spot(){let d=spot();assert_eq!(d.projection.spot_base,2.);assert_eq!(d.adapter_registry().unwrap().orders.len(),1);}
#[test] fn r1298_02_hedge(){let d=hedged();assert_eq!(d.lifecycle.state,LifecycleState::Hedged);assert_eq!(d.projection.futures_base,2.);}
#[test] fn r1298_03_hold(){let d=hedged();let(s,f)=books(5,20.);assert_eq!(d.isolated_exit_decision(&s,&f,&context(5)),exit_policy::ExitDecision::Hold);}
#[test] fn r1298_04_trigger_cover(){let(d,id)=cover_pending();assert_eq!(d.lifecycle.orders[&id].side,risk::FillSide::Cover);assert_eq!(d.projection.futures_base,2.);}
#[test] fn r1298_05_partial_cover(){let(mut d,id)=cover_pending();fill(&mut d,&id,7,0.5);assert_eq!(d.projection.futures_base,1.5);assert_eq!(d.lifecycle.orders[&id].remaining_quantity,1.5);}
#[test] fn r1298_06_full_cover(){let d=covered();assert_eq!(d.projection.futures_base,0.);assert_eq!(d.projection.spot_base,2.);}
#[test] fn r1298_07_spot_sell(){let(d,id)=sell_pending();assert_eq!(d.lifecycle.orders[&id].side,risk::FillSide::Sell);assert_eq!(d.lifecycle.orders[&id].original_quantity,2.);}
#[test] fn r1298_08_partial_sell(){let(mut d,id)=sell_pending();fill(&mut d,&id,9,0.5);assert_eq!(d.projection.spot_base,1.5);assert_ne!(d.lifecycle.state,LifecycleState::Closed);}
#[test] fn r1298_09_zero_exposure(){let d=zero();assert_eq!(d.projection.spot_base,0.);assert_eq!(d.projection.futures_base,0.);assert_ne!(d.lifecycle.state,LifecycleState::Closed);}
#[test] fn r1298_10_terminal_census(){let d=closed();assert_eq!(d.lifecycle.state,LifecycleState::Closed);assert!(d.adapter_registry().unwrap().census(d.lifecycle(),1).is_ok());assert!(d.projection.realized_pnl_quote.is_some());}
#[test] fn r1298_11_missing_order(){let d=zero();let mut a=d.adapter_registry().unwrap().clone();let id=a.orders.keys().next().unwrap().clone();a.orders.remove(&id);assert!(a.census(d.lifecycle(),1).is_err());}
#[test] fn r1298_12_extra_order(){let d=zero();let mut a=d.adapter_registry().unwrap().clone();let mut o=a.orders.values().next().unwrap().clone();o.order_id="extra".into();a.orders.insert("extra".into(),o);assert!(a.census(d.lifecycle(),1).is_err());}
#[test] fn r1298_13_unknown_order(){let d=zero();let mut a=d.adapter_registry().unwrap().clone();a.orders.values_mut().next().unwrap().status=OrderStatus::Unknown;assert!(a.census(d.lifecycle(),1).is_err());}
#[test] fn r1298_14_duplicate_execution(){let d=zero();let mut l=d.lifecycle.clone();let o=l.orders.values_mut().next().unwrap();o.execution_event_ids.push(o.execution_event_ids[0].clone());assert!(d.adapter_registry().unwrap().census(&l,1).is_err());}
#[test] fn r1298_15_cancel_pending(){let(mut d,id)=cover_pending();d.control_isolated(IsolatedControl::CancelRequested{order_id:id.clone()},&context(7)).unwrap();assert_eq!(d.lifecycle.orders[&id].remaining_quantity,2.);assert!(d.adapter_registry().unwrap().census(d.lifecycle(),1).is_err());}
#[test] fn r1298_16_cancel_ack(){let(mut d,id)=cover_pending();d.control_isolated(IsolatedControl::CancelRequested{order_id:id.clone()},&context(7)).unwrap();d.control_isolated(IsolatedControl::Cancelled{order_id:id.clone()},&context(8)).unwrap();assert!(d.adapter_registry().unwrap().orders[&id].cancel_acknowledged);assert_eq!(d.projection.futures_base,2.);assert_ne!(d.lifecycle.state,LifecycleState::Closed);}
#[test] fn r1298_17_late_fill(){let(mut d,id)=cover_pending();d.control_isolated(IsolatedControl::CancelRequested{order_id:id.clone()},&context(7)).unwrap();d.control_isolated(IsolatedControl::Cancelled{order_id:id.clone()},&context(8)).unwrap();let(_,f)=books(9,20.);let e=simulate_marketable(&d.registered_intents().unwrap()[&id].intent,0.5,2,&f,true,context(9).now_ns,context(9).wall_ms).unwrap().pop().unwrap();d.control_isolated(IsolatedControl::LateFill{event:e},&context(9)).unwrap();assert!(!d.adapter_registry().unwrap().unresolved_late.is_empty());assert_eq!(d.lifecycle.state,LifecycleState::RecoveryRequired);}
#[test] fn r1298_18_restart_before_cover(){let mut d=hedged();d.request_isolated_exit(&context(5)).unwrap();let mut d=restart(d);let(s,f)=books(6,20.);assert!(d.register_isolated_exit(&s,&f,&context(6)).is_err());assert_eq!(d.projection.futures_base,2.);}
#[test] fn r1298_19_restart_partial_cover(){let(mut d,id)=cover_pending();fill(&mut d,&id,7,0.5);let d=restart(d);assert_eq!(d.projection.futures_base,1.5);assert_eq!(d.lifecycle.orders[&id].remaining_quantity,1.5);assert_eq!(d.lifecycle.state,LifecycleState::RecoveryRequired);}
#[test] fn r1298_20_restart_before_sell(){let mut d=restart(covered());let(s,f)=books(8,20.);assert!(d.register_isolated_exit(&s,&f,&context(8)).is_err());assert_eq!(d.projection.spot_base,2.);}
#[test] fn r1298_21_restart_partial_sell(){let(mut d,id)=sell_pending();fill(&mut d,&id,9,0.5);let d=restart(d);assert_eq!(d.projection.spot_base,1.5);assert_eq!(d.lifecycle.orders[&id].remaining_quantity,1.5);}
#[test] fn r1298_22_restart_before_closed(){let mut d=restart(zero());assert!(d.close_isolated_generation(&context(10)).is_err());assert_eq!(d.lifecycle.state,LifecycleState::RecoveryRequired);}
#[test] fn r1298_23_restart_closed(){let d=closed();let before=serde_json::to_value(&d.projection).unwrap();let seq=d.sequence();let d=restart(d);assert_eq!(d.lifecycle.state,LifecycleState::Closed);assert_eq!(d.sequence(),seq);assert_eq!(serde_json::to_value(&d.projection).unwrap(),before);}
#[test] fn r1298_24_duplicate_snapshot(){let(mut d,id)=cover_pending();fill(&mut d,&id,7,0.5);let(s,f)=books(7,0.5);let seq=d.sequence();assert!(d.simulate_isolated_order(&id,&s,&f,&context(7)).unwrap().is_none());assert_eq!(d.sequence(),seq);}
#[test] fn r1298_25_hot_loss(){let(mut d,_)=cover_pending();let mut c=context(7);c.futures_hot=false;d.control_isolated(IsolatedControl::Clock,&c).unwrap();assert_eq!(d.lifecycle.state,LifecycleState::RecoveryRequired);}
#[test] fn r1298_26_stale(){let(d,_)=cover_pending();let(s,f)=books(7,20.);let mut c=context(7);c.now_ns+=800_000_001;assert!(matches!(d.isolated_exit_decision(&s,&f,&c),exit_policy::ExitDecision::FailClosed(_)));}
#[test] fn r1298_27_units(){let mut i=identity();i.exit.as_mut().unwrap().quantity_units="CONTRACTS".into();assert!(i.validate().is_err());}
#[test] fn r1298_28_fee_mismatch(){let d=zero();let mut l=d.lifecycle.clone();l.ledger.as_mut().unwrap().fees_quote+=1.;assert!(d.adapter_registry().unwrap().census(&l,1).is_err());let mut i=identity();i.exit.as_mut().unwrap().fee_model="BASE".into();assert!(i.validate().is_err());}
#[test] fn r1298_29_uncertain_commit(){
 // Fault code is prepared only. No fault injection is executed by Astra.
 let mut d=zero();let before=d.sequence();let _held=d.lock.try_clone().unwrap();d.lock=OpenOptions::new().write(true).open("/dev/full").unwrap();
 assert!(d.close_isolated_generation(&context(10)).is_err());assert!(d.fenced);assert_eq!(d.sequence(),before);assert_ne!(d.lifecycle.state,LifecycleState::Closed);
}
#[test] fn r1298_30_second_writer(){let d=owner();assert!(DurablePaper::recover_isolated(&d.path,&identity()).is_err());}
#[test] fn r1298_31_no_legacy(){let mut d=hedged();assert!(d.commit_census(vec![],context(5).now_ns,context(5).wall_ms).is_err());assert!(!include_str!("native_paper_isolated_r1296.rs").contains(".on_frame("));}
#[test] fn r1298_32_new_generation(){let mut d=closed();d.start_isolated_generation(&context(11)).unwrap();let(s,f)=books(12,20.);let i=d.ensure_isolated_intent(d.sequence(),d.isolated_entry_key(),FillLeg::Spot,2.,&s,&f,&context(12)).unwrap();assert!(i.intent.order_id.ends_with(":5"));let full=d.isolated.as_ref().unwrap().full.as_ref().unwrap();assert_eq!(full.generation,2);assert_eq!(full.archives.len(),1);assert_eq!(full.archives[0].lifecycle.state,LifecycleState::Closed);}

#[test] fn r1298_late_conflict_after_closed_freezes(){
 let mut d=closed();let mut e=d.isolated_events().unwrap().values().next().unwrap().clone();e.quantity=Some(0.1);
 d.control_isolated(IsolatedControl::LateFill{event:e},&context(11)).unwrap();
 assert_eq!(d.lifecycle.state,LifecycleState::RecoveryRequired);assert!(d.start_isolated_generation(&context(12)).is_err());
}
#[test] fn r1298_historical_late_freezes_new_generation(){
 let mut d=closed();let mut e=d.isolated_events().unwrap().values().next().unwrap().clone();e.event_id="late-old-generation".into();
 d.start_isolated_generation(&context(11)).unwrap();
 d.control_isolated(IsolatedControl::LateFill{event:e},&context(12)).unwrap();
 assert_eq!(d.lifecycle.state,LifecycleState::RecoveryRequired);assert_eq!(d.isolated.as_ref().unwrap().full.as_ref().unwrap().historical_late.len(),1);
}

#[test] fn r1298_exit_deadline_prevents_pending_fill(){
 let(mut d,id)=cover_pending();let mut c=context(7);c.input_id="deadline".into();c.now_ns=context(5).now_ns+1_000_000_001;
 d.control_isolated(IsolatedControl::Clock,&c).unwrap();
 assert_eq!(d.lifecycle.state,LifecycleState::RecoveryRequired);
 let(s,f)=books(7,20.);assert!(d.simulate_isolated_order(&id,&s,&f,&c).is_err());assert_eq!(d.projection.futures_base,2.);
}
