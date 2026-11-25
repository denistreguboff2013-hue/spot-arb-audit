// Contract fixtures only. No socket is opened; this is not a full provider E2E proof.
use super::*;
use crate::market_data_ingress_r1205::Permit;
pub(super) fn fixture(v:Venue)->(Provider,RawFrame,Update){
 let n=crate::ingress_runtime_r1205::mono_ns();let w=crate::ingress_runtime_r1205::wall_ms();
 let auth:Authority=Default::default();
 for venue in [Venue::OkxSpot,Venue::BinanceFutures]{
  auth.write().unwrap().insert(crate::market_data_ingress_r1205::key(venue,"AAA"),Permit{snapshot_witness:None,
   session:"active".into(),epoch:1,received_ns:n,event_ms:w,standby_session:"standby".into(),standby_received_ns:n,standby_event_ms:w});
 }
 let f:RawFrame=serde_json::from_value(serde_json::json!({"venue":v,"epoch":1,"received_ns":n,"received_wall_ms":w,"raw":"{}",
  "source":{"journal_id":"synthetic-test","accepted_id":1,"session_id":"active","connection_epoch":1,"selection_epoch":1,"payload_sha256":"fixture"}})).unwrap();
 let u=Update{venue:v,base:"AAA".into(),book:Book{bids:vec![(1.,10.)],asks:vec![(1.01,10.)],received_ns:n,received_wall_ms:w,exchange_ts_ms:Some(w),changed_ns:n,epoch:1,sequence:Some(1)}};
 (Provider{authority:auth,clock_domain:"test-clock".into(),seen:HashMap::new()},f,u)
}
fn bind(p:&mut Provider,f:RawFrame,u:Update)->Result<ValidatedInput,Denial>{let n=f.received_ns;let w=f.received_wall_ms;p.bind(f,u,n,w)}
#[test] fn r1301_01_valid_okx(){let(mut p,f,u)=fixture(Venue::OkxSpot);assert_eq!(bind(&mut p,f,u).unwrap().venue(),Venue::OkxSpot)}
#[test] fn r1301_02_valid_binance(){let(mut p,f,u)=fixture(Venue::BinanceFutures);assert_eq!(bind(&mut p,f,u).unwrap().venue(),Venue::BinanceFutures)}
#[test] fn r1301_03_raw_denied(){let(mut p,mut f,u)=fixture(Venue::OkxSpot);f.source=None;assert_eq!(bind(&mut p,f,u).unwrap_err(),Denial::Raw)}
#[test] fn r1301_04_missing_timestamp(){let(mut p,f,mut u)=fixture(Venue::OkxSpot);u.book.exchange_ts_ms=None;assert_eq!(bind(&mut p,f,u).unwrap_err(),Denial::Timestamp)}
#[test] fn r1301_05_stale(){let(mut p,f,u)=fixture(Venue::OkxSpot);let i=bind(&mut p,f,u).unwrap();assert_eq!(i.fresh(i.receive_ns+MAX_AGE_NS+1,i.exchange_ms+801),Err(Denial::Stale))}
#[test] fn r1301_06_hot_loss(){let(mut p,f,u)=fixture(Venue::OkxSpot);p.authority.write().unwrap().clear();assert_eq!(bind(&mut p,f,u).unwrap_err(),Denial::Hot)}
#[test] fn r1301_07_gap_signal(){let(mut p,_,_)=fixture(Venue::OkxSpot);assert_eq!(p.accept(Output::Blocked{venue:Venue::OkxSpot,base:"AAA".into(),reason:"sequence gap".into()},1,1).unwrap_err(),Denial::Hot)}
#[test] fn r1301_08_connection_regression(){let(mut p,f,u)=fixture(Venue::OkxSpot);bind(&mut p,f.clone(),u.clone()).unwrap();let mut f=f;f.source.as_mut().unwrap().connection_epoch=0;assert_eq!(bind(&mut p,f,u).unwrap_err(),Denial::Generation)}
#[test] fn r1301_09_snapshot_mismatch(){let(mut p,mut f,u)=fixture(Venue::OkxSpot);f.source.as_mut().unwrap().selection_epoch=2;assert_eq!(bind(&mut p,f,u).unwrap_err(),Denial::Hot)}
#[test] fn r1301_10_overflow(){let(mut p,f,u)=fixture(Venue::OkxSpot);let i=bind(&mut p,f,u).unwrap();let(tx,_rx,h)=channel(p.authority);for _ in 0..CAPACITY{tx.send(i.clone()).unwrap()}assert_eq!(tx.send(i),Err(Denial::Overflow));assert!(!h.healthy())}
#[test] fn r1301_11_residence(){let(mut p,f,u)=fixture(Venue::OkxSpot);let i=bind(&mut p,f,u).unwrap();let(tx,rx,h)=channel(p.authority);tx.tx.send(Envelope{input:i,queued:Instant::now()-Duration::from_secs(1),queued_mono_ns:None}).unwrap();assert_eq!(rx.receive().unwrap_err(),Denial::Residence);assert!(!h.healthy())}
#[test] fn r1301_12_consumer_stall(){let h=Health::new();*h.heartbeat.lock().unwrap()=Instant::now()-Duration::from_secs(1);h.watchdog(1);assert_eq!(h.incident(),Some(Denial::Consumer))}
#[test] fn r1301_13_no_frames(){let h=Health::new();*h.last_input.lock().unwrap()=[Instant::now()-Duration::from_secs(1);2];h.watchdog(1);assert_eq!(h.incident(),Some(Denial::Stale))}
#[test] fn r1301_14_frozen_clock(){let h=Health::new();h.watchdog(1);h.watchdog(1);assert_eq!(h.incident(),Some(Denial::Clock))}
#[test] fn r1301_15_owner_fenced(){assert_eq!(order_admission(&Health::new(),false,true,true,true,true),Err(Denial::Owner))}
#[test] fn r1301_16_storage_failed(){assert_eq!(order_admission(&Health::new(),true,false,true,true,true),Err(Denial::Storage))}
#[test] fn r1301_17_kill_denied(){assert_eq!(order_admission(&Health::new(),true,true,true,false,true),Err(Denial::Kill))}
#[test] fn r1301_18_real_denied(){assert_eq!(order_admission(&Health::new(),true,true,false,true,true),Err(Denial::Real))}
#[test] fn r1301_20_rules_missing(){assert_eq!(order_admission(&Health::new(),true,true,true,true,true),Err(Denial::Rules))}
#[test] fn r1301_21_policy_denied(){assert_eq!(order_admission(&Health::new(),true,true,true,true,false),Err(Denial::Policy))}
#[test] fn r1301_22_duplicate(){let(mut p,f,u)=fixture(Venue::OkxSpot);bind(&mut p,f.clone(),u.clone()).unwrap();assert_eq!(bind(&mut p,f,u).unwrap_err(),Denial::Duplicate)}
#[test] fn r1301_23_out_of_order(){let(mut p,f,mut u)=fixture(Venue::OkxSpot);u.book.sequence=Some(2);bind(&mut p,f.clone(),u.clone()).unwrap();u.book.sequence=Some(1);assert_eq!(bind(&mut p,f,u).unwrap_err(),Denial::Sequence)}
#[test] fn r1301_28_restart_health_not_restored(){let old=Health::new();old.trip(Denial::Hot);let new=Health::new();assert_eq!(order_admission(&new,true,true,true,true,true),Err(Denial::Rules));assert!(!old.healthy())}
#[test] fn r1301_31_no_single_frame_rearm(){let h=Health::new();h.trip(Denial::Hot);h.heartbeat();*h.last_input.lock().unwrap()=[Instant::now();2];h.watchdog(1);assert!(!h.healthy());assert_eq!(order_admission(&h,true,true,true,true,true),Err(Denial::Hot))}
#[test] fn r1301_conflicting_duplicate(){let(mut p,f,mut u)=fixture(Venue::OkxSpot);bind(&mut p,f.clone(),u.clone()).unwrap();u.book.bids[0].1=9.;assert_eq!(bind(&mut p,f,u).unwrap_err(),Denial::Sequence)}
#[test] fn r1301_single_leg_silence(){let h=Health::new();h.last_input.lock().unwrap()[1]=Instant::now()-Duration::from_secs(1);h.watchdog(1);assert_eq!(h.incident(),Some(Denial::Stale))}

#[test] fn r1301_snapshot_generation_is_not_fabricated(){let(inputs,_,_)=fixture_pair_for_test("test");for input in inputs{assert_eq!(input.snapshot_generation(),None);assert_eq!(input.rules_identity(),None)}}

#[test] fn r1301_zero_watchdog_clock_denied(){let h=Health::new();h.watchdog(0);assert_eq!(h.incident(),Some(Denial::Clock))}

#[test] fn r1301_closed_consumer_counts_drop(){let(mut p,f,u)=fixture(Venue::OkxSpot);let i=bind(&mut p,f,u).unwrap();let(tx,rx,h)=channel(p.authority);drop(rx);assert_eq!(tx.send(i),Err(Denial::Consumer));assert_eq!(h.metrics()["dropped"],1);assert!(!h.healthy())}

#[test] fn r1304_watchdog_lease_closes_admission_without_tick(){
 let h=Health::new();*h.watchdog_tick.lock().unwrap()=Instant::now()-Duration::from_secs(1);
 assert!(!h.healthy());assert_eq!(order_admission(&h,true,true,true,true,true),Err(Denial::Provider));
}
#[test] fn r1304_reset_requires_matching_engine_authority(){
 let(mut p,_,_)=fixture(Venue::OkxSpot);
 assert_eq!(p.selection_reset(Venue::OkxSpot,"AAA",2),Err(Denial::Generation));
 assert_eq!(p.selection_reset(Venue::OkxSpot,"BBB",1),Err(Denial::Generation));
}

// R1317 only incremented the same Permit epoch; R1311 correctly keeps that current-witness violation sticky. This case uses a genuinely new Engine witness.
#[test] fn r1304_queued_old_epoch_dropped_after_new_authority(){
 let mut engine=engine_fixture::EngineFixture::new("r1319-queued-retirement");engine.qualify();
 let stale=engine.inputs.first().unwrap().clone();
 let key=crate::market_data_ingress_r1205::key(stale.venue(),stale.instrument());
 let mut replacement=engine_fixture::EngineFixture::new("r1319-queued-retirement");replacement.qualify();
 let permit=replacement.engine.authority.read().unwrap().get(&key).unwrap().clone();
 let witness=permit.snapshot_witness.as_ref().unwrap();
 assert!(witness.live_for(stale.venue(),stale.instrument(),&permit.session));
 assert!(!stale.witness.as_ref().unwrap().same_identity(witness));
 *engine.engine.authority.write().unwrap()=replacement.engine.authority.read().unwrap().clone();
 assert!(engine.receiver.receive().unwrap().is_none());
 assert!(engine.health.healthy());
 assert_eq!(engine.health.metrics()["dropped"],1);
}
#[test] fn r1304_current_epoch_violation_stays_sticky(){
 let(mut p,f,u)=fixture(Venue::OkxSpot);let mut input=bind(&mut p,f,u).unwrap();input.source="wrong-current-source".into();
 let(tx,rx,h)=channel(p.authority);tx.send(input).unwrap();
 assert_eq!(rx.receive().unwrap_err(),Denial::Generation);assert!(!h.healthy());
}

#[test] fn r1311_valid_live_witness_retires_epoch_2_after_engine_epoch_1(){
 let mut old=engine_fixture::EngineFixture::for_single_venue("r1311-epoch-reversal",Venue::OkxSpot);
 old.qualify_single_venue(Venue::OkxSpot);
 let mut stale=old.inputs[0].clone();
 // Pin the stale frame to the R1310 failing epoch tuple; its witness remains
 // genuinely Engine-minted and bound to its original source/session.
 stale.selection_generation=2;
 let scope=crate::market_data_ingress_r1205::key(Venue::OkxSpot,&stale.instrument);
 let mut replacement=engine_fixture::EngineFixture::for_single_venue("r1311-epoch-reversal",Venue::OkxSpot);
 replacement.qualify_single_venue(Venue::OkxSpot);
 // Pin only the replacement Permit epoch to the lower-epoch reset case. The
 // validator-minted witness/session and HOT state come from the actual qualified Engine.
 {let mut authority=replacement.engine.authority.write().unwrap();authority.get_mut(&scope).unwrap().epoch=1;}
 let current=replacement.engine.authority.read().unwrap().get(&scope).unwrap().clone();
 assert_eq!(current.epoch,1,"replacement authority epoch is pinned to the R1310 failure tuple");
 let live=current.snapshot_witness.as_ref().unwrap();
 let old_permit=old.engine.authority.read().unwrap().get(&scope).unwrap().clone();
 assert_eq!(old_permit.session,current.session,"deterministic fixture keeps textual session identity constant");
 assert!(live.live_for(Venue::OkxSpot,&stale.instrument,&current.session));
 assert!(!stale.witness.as_ref().unwrap().same_identity(live),"new Engine witness is a distinct authority even when the session label is reused");
 *old.engine.authority.write().unwrap()=replacement.engine.authority.read().unwrap().clone();
 assert_eq!(old.receiver.revalidate(&stale),Err(Denial::Generation));
 assert!(old.receiver.superseded(&stale),"live replacement witness, not epoch arithmetic, retires the stale frame");
 assert_eq!(old.health.incident(),None,"valid replacement must not create a sticky incident");
}

#[test] fn r1319_same_witness_epoch_change_stays_sticky(){
 let mut engine=engine_fixture::EngineFixture::for_single_venue("r1319-same-witness",Venue::OkxSpot);
 engine.qualify_single_venue(Venue::OkxSpot);
 let stale=engine.inputs[0].clone();
 let scope=crate::market_data_ingress_r1205::key(Venue::OkxSpot,&stale.instrument);
 engine.engine.authority.write().unwrap().get_mut(&scope).unwrap().epoch+=1;
 assert!(!engine.receiver.superseded(&stale),"numeric epoch change with the same witness cannot retire input");
 assert_eq!(engine.receiver.receive().unwrap_err(),Denial::Generation);
 assert_eq!(engine.health.incident(),Some(Denial::Generation));
 assert!(!engine.health.healthy());
}

#[test] fn r1319_missing_current_witness_stays_sticky(){
 let mut engine=engine_fixture::EngineFixture::for_single_venue("r1319-missing-witness",Venue::OkxSpot);
 engine.qualify_single_venue(Venue::OkxSpot);
 let stale=engine.inputs[0].clone();
 let scope=crate::market_data_ingress_r1205::key(Venue::OkxSpot,&stale.instrument);
 engine.engine.authority.write().unwrap().get_mut(&scope).unwrap().snapshot_witness=None;
 assert!(!engine.receiver.superseded(&stale),"missing replacement witness cannot retire queued input");
 assert_eq!(engine.receiver.receive().unwrap_err(),Denial::Generation);
 assert_eq!(engine.health.incident(),Some(Denial::Generation));
}

#[test] fn r1319_authority_swap_between_revalidate_and_superseded_is_deterministic(){
 let mut old=engine_fixture::EngineFixture::for_single_venue("r1319-revalidate-race",Venue::OkxSpot);
 old.qualify_single_venue(Venue::OkxSpot);
 let stale=old.inputs[0].clone();
 assert_eq!(old.receiver.revalidate(&stale),Ok(()));
 let mut replacement=engine_fixture::EngineFixture::for_single_venue("r1319-revalidate-race",Venue::OkxSpot);
 replacement.qualify_single_venue(Venue::OkxSpot);
 let scope=crate::market_data_ingress_r1205::key(Venue::OkxSpot,&stale.instrument);
 let current=replacement.engine.authority.read().unwrap().get(&scope).unwrap().clone();
 assert_eq!(current.session,stale.source,"session label reused across replacement");
 assert!(!stale.witness.as_ref().unwrap().same_identity(current.snapshot_witness.as_ref().unwrap()));
 *old.engine.authority.write().unwrap()=replacement.engine.authority.read().unwrap().clone();
 assert!(old.receiver.superseded(&stale),"authority replacement in the revalidate/superseded interval is retired by witness identity");
 assert_eq!(old.health.incident(),None);
}

#[test] fn r1319_valid_replacement_with_different_session_retires_stale_input(){
 let mut old=engine_fixture::EngineFixture::for_single_venue("r1319-old-session",Venue::OkxSpot);
 old.qualify_single_venue(Venue::OkxSpot);
 let stale=old.inputs[0].clone();
 let mut replacement=engine_fixture::EngineFixture::for_single_venue("r1319-new-session",Venue::OkxSpot);
 replacement.engine.mark_reconnecting("okx-a");
 replacement.engine.mark_reconnecting("okx-b");
 for id in ["okx-c","okx-d"] {replacement.engine.register_connection(id,Venue::OkxSpot,1).unwrap();replacement.engine.mark_subscribed(id);}
 // Qualify a genuinely new two-copy cohort; the replacement witness remains
 // Engine-minted and is bound to a distinct session identity.
 let n=crate::ingress_runtime_r1205::mono_ns(); let w=crate::ingress_runtime_r1205::wall_ms();
 for (step,age) in [300u64,150,0].into_iter().enumerate(){
  let frames=["okx-c","okx-d"].into_iter().map(|id|engine_fixture::raw(Venue::OkxSpot,id,"AAA",n-age*1_000_000,w-age as i64,300+step as i64,step==0)).collect();
  let _outputs=replacement.engine.receive_batch(frames,n,w).unwrap();
 }
 let scope=crate::market_data_ingress_r1205::key(Venue::OkxSpot,&stale.instrument);
 let current=replacement.engine.authority.read().unwrap().get(&scope).unwrap().clone();
 assert_ne!(current.session,stale.source,"replacement uses a different session identity");
 assert!(current.snapshot_witness.as_ref().unwrap().live_for(Venue::OkxSpot,&stale.instrument,&current.session));
 assert!(!stale.witness.as_ref().unwrap().same_identity(current.snapshot_witness.as_ref().unwrap()));
 *old.engine.authority.write().unwrap()=replacement.engine.authority.read().unwrap().clone();
 assert!(old.receiver.superseded(&stale));
 assert!(old.receiver.receive().unwrap().is_none());
 assert!(old.health.healthy());
 assert_eq!(old.health.metrics()["dropped"],1);
}

#[test] fn r1319_retirement_preserves_existing_sticky_incident(){
 let mut old=engine_fixture::EngineFixture::for_single_venue("r1319-sticky",Venue::OkxSpot);
 old.qualify_single_venue(Venue::OkxSpot);
 old.health.trip(Denial::Hot);
 let stale=old.inputs[0].clone();
 let mut replacement=engine_fixture::EngineFixture::for_single_venue("r1319-sticky",Venue::OkxSpot);
 replacement.qualify_single_venue(Venue::OkxSpot);
 *old.engine.authority.write().unwrap()=replacement.engine.authority.read().unwrap().clone();
 assert!(old.receiver.receive().unwrap().is_none());
 assert_eq!(old.health.incident(),Some(Denial::Hot));
 assert!(!old.health.healthy());
 assert_eq!(old.health.metrics()["dropped"],1);
 assert!(old.receiver.superseded(&stale));
}

#[test] fn r1311_mismatched_or_revoked_current_authority_is_not_superseded(){
 let mut old=engine_fixture::EngineFixture::new("r1311-negative");old.qualify();
 let mut replacement=engine_fixture::EngineFixture::new("r1311-negative");replacement.qualify();
 let stale=old.inputs.iter().find(|i|i.venue==Venue::OkxSpot).unwrap().clone();
 let scope=crate::market_data_ingress_r1205::key(Venue::OkxSpot,&stale.instrument);
 let mut mismatched=replacement.engine.authority.read().unwrap().clone();
 mismatched.get_mut(&scope).unwrap().session="forged-session".into();
 *old.engine.authority.write().unwrap()=mismatched;
 assert!(!old.receiver.superseded(&stale),"Permit and witness scope mismatch remains sticky");
 let mut replacement=engine_fixture::EngineFixture::new("r1311-negative");replacement.qualify();
 let permit=replacement.engine.authority.read().unwrap().get(&scope).unwrap().clone();
 replacement.engine.mark_reconnecting(&permit.session);
 *old.engine.authority.write().unwrap()=replacement.engine.authority.read().unwrap().clone();
 assert!(!old.receiver.superseded(&stale),"revoked Engine witness cannot retire an input");
}

#[test]
fn r1321_public_provider_cannot_mint_rules_or_admit_orders(){
 let (mut provider,frame,update)=fixture(Venue::OkxSpot);
 let validated=bind(&mut provider,frame,update).expect("valid public input");
 assert!(validated.rules_identity().is_none(),"public market validator cannot attest account/venue execution rules");
 let health=Health::new();
 assert_eq!(order_admission(&health,true,true,true,true,true),Err(Denial::Rules),"healthy public input is observation-only without independently attested rules");
 assert_eq!(order_admission(&health,true,true,false,true,true),Err(Denial::Real),"REAL gate precedes rules gate");
 assert_eq!(order_admission(&health,true,true,true,false,true),Err(Denial::Kill),"kill switch precedes rules gate");
}

#[test]
fn r1321_provider_preserves_complete_source_custody(){
 let (mut provider,mut frame,update)=fixture(Venue::OkxSpot);
 let source=frame.source.as_mut().unwrap();
 source.frame_trace_id="r1321-custody-trace".into();
 source.journal_id="r1321-offline-custody".into();
 source.accepted_id=27;
 source.accepted_ns=Some(frame.received_ns);
 source.selected_ns=Some(frame.received_ns);
 let expected=source.clone();
 let input=bind(&mut provider,frame,update).unwrap();
 assert_eq!(input.source_provenance(),&expected);
 assert_eq!(serde_json::to_value(input.source_provenance()).unwrap(),serde_json::to_value(&expected).unwrap());
 assert!(input.rules_identity().is_none());
 assert_eq!(order_admission(&Health::new(),true,true,true,true,true),Err(Denial::Rules));
}
