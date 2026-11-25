//! Negative cases derive capabilities only through the actual Engine fixture.
use super::*;
use super::engine_fixture::{EngineFixture,raw};
fn fixture()->EngineFixture{let mut f=EngineFixture::new("r1301a-synthetic-transport");f.qualify();f}
fn spot(f:&EngineFixture)->ValidatedInput{f.inputs.iter().find(|i|i.venue==Venue::OkxSpot).unwrap().clone()}
fn clocks()->(u64,i64){(crate::ingress_runtime_r1205::mono_ns(),crate::ingress_runtime_r1205::wall_ms())}
fn deny(result:Result<(),Denial>){assert!(matches!(result,Err(Denial::Generation)|Err(Denial::Hot)));}
#[test] fn r1301a_01_missing_snapshot(){
 let mut f=EngineFixture::new("test");let(n,w)=clocks();
 let out=f.engine.receive_batch(vec![raw(Venue::OkxSpot,"okx-a","AAA",n,w,100,false)],n,w).unwrap();
 assert!(!out.iter().any(|o|matches!(o,Output::Frame(_)|Output::Book(_,_))));
 assert!(f.receiver.authority.read().unwrap().is_empty());
}
#[test] fn r1301a_02_unqualified_snapshot(){
 let mut f=EngineFixture::new("test");let(n,w)=clocks();
 let out=f.engine.receive_batch(vec![raw(Venue::OkxSpot,"okx-a","AAA",n,w,100,true)],n,w).unwrap();
 assert!(!out.iter().any(|o|matches!(o,Output::Frame(_)|Output::Book(_,_))));
 assert!(f.receiver.authority.read().unwrap().is_empty());
}
#[test] fn r1301a_03_sequence_gap(){
 let mut f=fixture();let old=spot(&f);let(n,w)=clocks();
 f.engine.receive_batch(vec![raw(Venue::OkxSpot,&old.source,"AAA",n,w,200,false)],n,w).unwrap();
 assert_eq!(old.snapshot_generation(),None);deny(f.receiver.revalidate(&old));
}
#[test] fn r1301a_04_out_of_order(){
 let mut f=fixture();let old=spot(&f);let(n,w)=clocks();
 f.engine.receive_batch(vec![raw(Venue::OkxSpot,&old.source,"AAA",n,w,101,false)],n,w).unwrap();
 assert_eq!(old.snapshot_generation(),None);deny(f.receiver.revalidate(&old));
}
#[test] fn r1301a_05_06_resnapshot_revokes_queued_generation(){
 let mut f=fixture();let old=spot(&f);let generation=old.snapshot_generation().unwrap();let(n,w)=clocks();
 f.engine.receive_batch(vec![raw(Venue::OkxSpot,&old.source,"AAA",n,w,200,true)],n,w).unwrap();
 assert_eq!(old.snapshot_generation(),None);deny(f.receiver.revalidate(&old));
 let auth=f.receiver.authority.read().unwrap();
 let witness=auth.get(&crate::market_data_ingress_r1205::key(Venue::OkxSpot,"AAA")).unwrap().snapshot_witness.as_ref().unwrap();
 assert_ne!(witness.generation(),generation);assert!(witness.matches(Venue::OkxSpot,"AAA",&old.source,1));
}
#[test] fn r1301a_07_reconnect_no_sync(){
 let mut f=fixture();let old=spot(&f);f.engine.mark_reconnecting(&old.source);
 assert_eq!(old.snapshot_generation(),None);deny(f.receiver.revalidate(&old));
 let(n,w)=clocks();f.engine.receive_batch(vec![raw(Venue::OkxSpot,&old.source,"AAA",n,w,103,false)],n,w).unwrap();
 assert_eq!(old.snapshot_generation(),None);deny(f.receiver.revalidate(&old));
}
#[test] fn r1301a_08_connection_mismatch(){let f=fixture();let mut old=spot(&f);old.connection_generation+=1;deny(f.receiver.revalidate(&old));}
#[test] fn r1301a_09_missing_witness_no_admission(){
 let f=fixture();let mut old=spot(&f);old.witness=None;
 assert_eq!(old.snapshot_generation(),None);deny(f.receiver.revalidate(&old));
 assert_eq!(order_admission(&f.health,true,true,true,true,true),Err(Denial::Rules));
}
#[test] fn r1301a_10_instrument_provenance(){let f=fixture();let mut old=spot(&f);old.instrument="OTHER".into();deny(f.receiver.revalidate(&old));}
#[test] fn r1301a_11_venue_provenance(){let f=fixture();let mut old=spot(&f);old.venue=Venue::BinanceFutures;deny(f.receiver.revalidate(&old));}
#[test] fn r1301a_12_stale_validated(){let f=fixture();let mut old=spot(&f);old.receive_ns-=800_000_001;assert_eq!(f.receiver.revalidate(&old),Err(Denial::Stale));}
#[test] fn r1301a_13_queue_overflow(){
 let f=fixture();let input=spot(&f);let(tx,_rx,h)=channel(f.receiver.authority.clone());
 for _ in 0..CAPACITY{tx.send(input.clone()).unwrap();}
 assert_eq!(tx.send(input),Err(Denial::Overflow));assert!(!h.healthy());
}
#[test] fn r1301a_14_consumer_stall(){let f=fixture();*f.health.heartbeat.lock().unwrap()=Instant::now()-Duration::from_secs(1);f.health.watchdog(1);assert_eq!(f.health.incident(),Some(Denial::Consumer));}
#[test] fn r1301a_15_watchdog_no_frames(){let f=fixture();*f.health.last_input.lock().unwrap()=[Instant::now()-Duration::from_secs(1);2];f.health.watchdog(1);assert_eq!(f.health.incident(),Some(Denial::Stale));}
#[test] fn r1301a_17_hot_loss(){let f=fixture();let old=spot(&f);f.receiver.authority.write().unwrap().clear();assert_eq!(f.receiver.revalidate(&old),Err(Denial::Hot));}
#[test] fn r1301a_18_restart_old_generation(){
 let f=fixture();let old=spot(&f);drop(f);
 let fresh=EngineFixture::new("test");assert_eq!(old.snapshot_generation(),None);deny(fresh.receiver.revalidate(&old));
}
#[test] fn r1301a_19_unconfirmed_real_rules(){let f=fixture();assert!(f.inputs.iter().all(|i|i.rules_identity().is_none()));assert_eq!(order_admission(&f.health,true,true,true,true,true),Err(Denial::Rules));}

#[test] fn r1301a_08_engine_connection_mismatch(){
 let mut f=fixture();let old=spot(&f);let(n,w)=clocks();
 let mut message=raw(Venue::OkxSpot,&old.source,"AAA",n,w,103,false);
 message.source.as_mut().unwrap().connection_epoch=2;
 assert!(f.engine.receive_batch(vec![message],n,w).is_err());
 assert_eq!(old.snapshot_generation(),None);deny(f.receiver.revalidate(&old));
}
