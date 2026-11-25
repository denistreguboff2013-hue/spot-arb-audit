//! R14 review-only adapter contract. No network transport.
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum Venue { OkxSpot, BinanceUsdm }

#[derive(Clone,Debug,PartialEq,Eq)]
pub struct OrderIntent {
 pub venue: Venue,
 pub symbol: String,
 pub side: String,
 pub amount: String,
 pub client_id: String,
}

#[derive(Clone,Debug,PartialEq,Eq)]
pub enum AdapterError {
 MissingClientId,
 InvalidClientId,
 NotPrepared,
 IdentityMismatch,
 StaleSafety,
 BatchUnsupported,
}

fn valid_client_id(s:&str)->bool {
 !s.is_empty() && s.len()<=32 &&
 s.bytes().all(|b| b.is_ascii_alphanumeric() || b==b'_')
}

pub fn authorize(
 intent:&OrderIntent,
 prepared:bool,
 expected_client_id:&str,
 safety_generation:u64,
 current_generation:u64,
)->Result<(),AdapterError>{
 if !prepared { return Err(AdapterError::NotPrepared); }
 if safety_generation!=current_generation { return Err(AdapterError::StaleSafety); }
 if intent.client_id.is_empty() { return Err(AdapterError::MissingClientId); }
 if !valid_client_id(&intent.client_id) { return Err(AdapterError::InvalidClientId); }
 if intent.client_id!=expected_client_id { return Err(AdapterError::IdentityMismatch); }
 Ok(())
}

pub fn authorize_batch()->Result<(),AdapterError>{
 Err(AdapterError::BatchUnsupported)
}

#[cfg(test)]
mod tests {
 use super::*;
 fn intent()->OrderIntent { OrderIntent{venue:Venue::OkxSpot,symbol:"BNT/USDT".into(),side:"buy".into(),amount:"1".into(),client_id:"sab0123456789abcdef0123456789abc".into()} }
 #[test] fn exact_identity_passes(){let x=intent();assert_eq!(authorize(&x,true,&x.client_id,4,4),Ok(()));}
 #[test] fn missing_prepare_denies(){let x=intent();assert_eq!(authorize(&x,false,&x.client_id,4,4),Err(AdapterError::NotPrepared));}
 #[test] fn stale_safety_denies(){let x=intent();assert_eq!(authorize(&x,true,&x.client_id,4,5),Err(AdapterError::StaleSafety));}
 #[test] fn identity_mismatch_denies(){let x=intent();assert_eq!(authorize(&x,true,"other",4,4),Err(AdapterError::IdentityMismatch));}
 #[test] fn invalid_client_id_denies(){let mut x=intent();x.client_id="bad-id".into();assert_eq!(authorize(&x,true,&x.client_id,4,4),Err(AdapterError::InvalidClientId));}
 #[test] fn batch_denies(){assert_eq!(authorize_batch(),Err(AdapterError::BatchUnsupported));}
}

#[derive(Clone,Copy,Debug,PartialEq,Eq)] pub enum OrderKind { Market, AggressiveLimitFok }
#[derive(Clone,Debug,PartialEq)] pub struct EntryPlan { pub venue:Venue,pub side:&'static str,pub kind:OrderKind,pub reduce_only:bool }
pub fn spot_entry(kind:OrderKind)->EntryPlan { EntryPlan{venue:Venue::OkxSpot,side:"buy",kind,reduce_only:false} }
pub fn futures_entry()->EntryPlan { EntryPlan{venue:Venue::BinanceUsdm,side:"sell",kind:OrderKind::Market,reduce_only:false} }
pub fn futures_unwind()->EntryPlan { EntryPlan{venue:Venue::BinanceUsdm,side:"buy",kind:OrderKind::Market,reduce_only:true} }
pub fn spot_unwind()->EntryPlan { EntryPlan{venue:Venue::OkxSpot,side:"sell",kind:OrderKind::Market,reduce_only:false} }

#[derive(Clone,Copy,Debug,PartialEq,Eq)] pub enum ResponseClass { Acknowledged, Rejected, Unknown }
pub fn classify_response(has_response:bool,status:Option<&str>,transport_error:bool)->ResponseClass {
 if transport_error || !has_response { return ResponseClass::Unknown; }
 match status.unwrap_or("").to_ascii_lowercase().as_str() {
  "rejected"|"expired"=>ResponseClass::Rejected,
  _=>ResponseClass::Acknowledged,
 }
}

#[cfg(test)] mod python_strategy_contract { use super::*;
 #[test]fn entry_legs_match_python(){assert_eq!(spot_entry(OrderKind::AggressiveLimitFok),EntryPlan{venue:Venue::OkxSpot,side:"buy",kind:OrderKind::AggressiveLimitFok,reduce_only:false});assert_eq!(futures_entry(),EntryPlan{venue:Venue::BinanceUsdm,side:"sell",kind:OrderKind::Market,reduce_only:false});}
 #[test]fn unwind_legs_match_python(){assert_eq!(spot_unwind().side,"sell");let f=futures_unwind();assert_eq!(f.side,"buy");assert!(f.reduce_only);}
 #[test]fn rejected_and_expired_are_terminal_rejects(){assert_eq!(classify_response(true,Some("rejected"),false),ResponseClass::Rejected);assert_eq!(classify_response(true,Some("expired"),false),ResponseClass::Rejected);}
 #[test]fn transport_failure_is_unknown(){assert_eq!(classify_response(false,None,true),ResponseClass::Unknown);}
 #[test]fn normal_response_is_acknowledged(){assert_eq!(classify_response(true,Some("closed"),false),ResponseClass::Acknowledged);assert_eq!(classify_response(true,Some("open"),false),ResponseClass::Acknowledged);}
}

fn valid_client_id_for(v:Venue,s:&str)->bool {
 match v {
  Venue::OkxSpot => !s.is_empty() && s.len()<=32 && s.bytes().all(|b|b.is_ascii_alphanumeric()),
  Venue::BinanceUsdm => !s.is_empty() && s.len()<=36 && s.bytes().all(|b|b.is_ascii_alphanumeric() || b"._:/-".contains(&b)),
 }
}
pub fn authorize_exact(intent:&OrderIntent,prepared:bool,expected:&str,safety_generation:u64,current_generation:u64)->Result<(),AdapterError>{
 if !prepared{return Err(AdapterError::NotPrepared)}
 if safety_generation!=current_generation{return Err(AdapterError::StaleSafety)}
 if intent.client_id.is_empty(){return Err(AdapterError::MissingClientId)}
 if !valid_client_id_for(intent.venue,&intent.client_id){return Err(AdapterError::InvalidClientId)}
 if intent.client_id!=expected{return Err(AdapterError::IdentityMismatch)}
 Ok(())
}

#[cfg(test)] mod exact_client_id_contract {use super::*;
 fn q(v:Venue,id:&str)->OrderIntent{OrderIntent{venue:v,symbol:"BNT/USDT".into(),side:"buy".into(),amount:"1".into(),client_id:id.into()}}
 #[test]fn okx_universal_is_alnum_32(){let a="sab0123456789abcdef0123456789abc";assert_eq!(authorize_exact(&q(Venue::OkxSpot,a),true,a,1,1),Ok(()));assert_eq!(authorize_exact(&q(Venue::OkxSpot,"bad-id"),true,"bad-id",1,1),Err(AdapterError::InvalidClientId));}
 #[test]fn binance_accepts_its_exact_extra_charset(){for id in ["a-b","a_b","a.b","a:b","a/b"]{assert_eq!(authorize_exact(&q(Venue::BinanceUsdm,id),true,id,1,1),Ok(()));}}
 #[test]fn venue_lengths_are_exact(){let ok="A".repeat(33);assert_eq!(authorize_exact(&q(Venue::OkxSpot,&ok),true,&ok,1,1),Err(AdapterError::InvalidClientId));let bn="A".repeat(36);assert_eq!(authorize_exact(&q(Venue::BinanceUsdm,&bn),true,&bn,1,1),Ok(()));}
}

#[cfg(test)] mod integrated_r13_r14_fake {
 use super::*;
 use crate::execution::{Action,CoordinatorState,FakePipeline};
 #[test] fn happy_two_leg_adapter_path(){
  let mut p=FakePipeline::new(11);
  let s=q(Venue::OkxSpot,"sab00000000000000000000000000001");
  assert!(matches!(p.spot_begin(11,"spot1",&s.client_id).unwrap(),Action::PersistPrepare{..}));
  assert!(matches!(p.spot_sent("spot1").unwrap(),Action::NetworkMutation{..}));
  authorize_exact(&s,true,&s.client_id,11,11).unwrap();
  assert_eq!(classify_response(true,Some("closed"),false),ResponseClass::Acknowledged);
  p.spot_ack("spot1",10,10).unwrap();
  let f=q(Venue::BinanceUsdm,"sab00000000000000000000000000002");
  p.fut_begin(11,"fut1",&f.client_id).unwrap();p.fut_sent("fut1").unwrap();
  authorize_exact(&f,true,&f.client_id,11,11).unwrap();
  p.fut_ack("fut1",10,10).unwrap();assert_eq!(p.coord.state,CoordinatorState::Hedged);
 }
 #[test] fn transport_unknown_never_opens_second_leg(){
  let mut p=FakePipeline::new(12);let s=q(Venue::OkxSpot,"sab00000000000000000000000000003");
  p.spot_begin(12,"spot2",&s.client_id).unwrap();p.spot_sent("spot2").unwrap();authorize_exact(&s,true,&s.client_id,12,12).unwrap();
  assert_eq!(classify_response(false,None,true),ResponseClass::Unknown);let _=p.spot_unknown("spot2");
  assert_eq!(p.coord.state,CoordinatorState::ManualRequired);assert!(p.gate.recovery_only);
 }
 #[test] fn safety_flip_stops_adapter_before_second_leg(){
  let mut p=FakePipeline::new(13);let s=q(Venue::OkxSpot,"sab00000000000000000000000000004");
  p.spot_begin(13,"spot3",&s.client_id).unwrap();p.spot_ack("spot3",10,10).unwrap();
  assert!(p.fut_begin(14,"fut3","sab00000000000000000000000000005").is_err());
 }
 fn q(v:Venue,id:&str)->OrderIntent{OrderIntent{venue:v,symbol:"BNT/USDT".into(),side:"buy".into(),amount:"1".into(),client_id:id.into()}}
}

#[cfg(test)] mod integrated_failure_recovery_matrix {
 use super::*; use crate::execution::{Action,CoordinatorState,FakePipeline,Intent,MutationGate,ProbeOutcome,Resolution};
 fn id(n:u8)->String{format!("sab{:029}",n)}
 fn req(v:Venue,c:&str)->OrderIntent{OrderIntent{venue:v,symbol:"BNT/USDT".into(),side:"buy".into(),amount:"1".into(),client_id:c.into()}}
 #[test]fn crash_after_prepare_reopens_recovery_without_send(){
  let mut p=FakePipeline::new(20);let c=id(1);p.spot_begin(20,"s",&c).unwrap();
  let raw=serde_json::to_string(&p.gate.durable.intents).unwrap();let intents:Vec<Intent>=serde_json::from_str(&raw).unwrap();let r=MutationGate::reopen(intents);
  assert!(r.recovery_only);assert!(r.after_durable_prepare("s").is_err());
 }
 #[test]fn unknown_probe_found_still_needs_explicit_reconcile(){
  let mut p=FakePipeline::new(21);let c=id(2);p.spot_begin(21,"s",&c).unwrap();p.spot_sent("s").unwrap();let _=p.spot_unknown("s");
  p.gate.recovery_probe("s",ProbeOutcome::Found).unwrap();assert!(p.gate.recovery_only);assert!(p.gate.unresolved_for_test());
  p.gate.reconcile("s",Resolution::Confirmed,true).unwrap();assert!(!p.gate.recovery_only);
 }
 #[test]fn error_probe_cannot_become_not_found(){
  let mut p=FakePipeline::new(22);let c=id(3);p.spot_begin(22,"s",&c).unwrap();let _=p.spot_unknown("s");
  p.gate.recovery_probe("s",ProbeOutcome::Error).unwrap();assert!(p.gate.recovery_only);assert!(p.gate.unresolved_for_test());
 }
 #[test]fn identity_mismatch_blocks_before_fake_transport(){
  let c=id(4);let x=req(Venue::OkxSpot,&c);assert_eq!(authorize_exact(&x,true,"different",23,23),Err(AdapterError::IdentityMismatch));
 }
 #[test]fn stale_safety_blocks_before_fake_transport(){
  let c=id(5);let x=req(Venue::BinanceUsdm,&c);assert_eq!(authorize_exact(&x,true,&c,24,25),Err(AdapterError::StaleSafety));
 }
 #[test]fn rejected_response_is_terminal_not_unknown(){
  let mut p=FakePipeline::new(26);let c=id(6);p.spot_begin(26,"s",&c).unwrap();assert!(matches!(p.spot_sent("s").unwrap(),Action::NetworkMutation{..}));
  assert_eq!(classify_response(true,Some("rejected"),false),ResponseClass::Rejected);p.gate.network_ack("s",true).unwrap();assert!(!p.gate.recovery_only);assert!(!p.gate.unresolved_for_test());
 }
 #[test]fn futures_unknown_preserves_non_hedged_state(){
  let mut p=FakePipeline::new(27);let s=id(7);p.spot_begin(27,"s",&s).unwrap();p.spot_ack("s",10,10).unwrap();let f=id(8);p.fut_begin(27,"f",&f).unwrap();p.fut_sent("f").unwrap();let _=p.fut_unknown("f");
  assert_eq!(p.coord.state,CoordinatorState::ManualRequired);assert!(p.gate.recovery_only);
 }
}
