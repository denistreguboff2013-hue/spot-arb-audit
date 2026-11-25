use serde::{Deserialize,Serialize};

#[derive(Clone,Copy,Debug,PartialEq,Eq,Serialize,Deserialize)]
pub enum IntentState{Prepared,Acknowledged,Rejected,Unknown,Reconciled}
#[derive(Clone,Copy,Debug,PartialEq,Eq,Serialize,Deserialize)]
pub enum Resolution{Confirmed,NotFound,Rejected,Duplicate}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
pub struct Intent{pub id:String,pub client_order_id:Option<String>,pub state:IntentState}
#[derive(Clone,Debug,Default)]
pub struct ExecutionState{pub intents:Vec<Intent>}

#[derive(Clone,Debug,PartialEq,Eq)]
pub enum ExecError{UnresolvedMutation,MissingClientOrderId,DuplicateIntent,InvalidTransition,MissingEvidence}

impl ExecutionState{
 pub fn unresolved(&self)->Option<&Intent>{self.intents.iter().find(|x|matches!(x.state,IntentState::Prepared|IntentState::Unknown))}
 pub fn prepare(&mut self,id:&str,is_create_order:bool,client_order_id:Option<&str>)->Result<(),ExecError>{
  if self.unresolved().is_some(){return Err(ExecError::UnresolvedMutation)}
  if is_create_order&&client_order_id.map(str::is_empty).unwrap_or(true){return Err(ExecError::MissingClientOrderId)}
  if self.intents.iter().any(|x|x.id==id){return Err(ExecError::DuplicateIntent)}
  self.intents.push(Intent{id:id.into(),client_order_id:client_order_id.map(str::to_owned),state:IntentState::Prepared});Ok(())
 }
 fn get_mut(&mut self,id:&str)->Result<&mut Intent,ExecError>{self.intents.iter_mut().find(|x|x.id==id).ok_or(ExecError::InvalidTransition)}
 pub fn acknowledge(&mut self,id:&str,rejected:bool)->Result<(),ExecError>{let x=self.get_mut(id)?;if x.state!=IntentState::Prepared{return Err(ExecError::InvalidTransition)};x.state=if rejected{IntentState::Rejected}else{IntentState::Acknowledged};Ok(())}
 pub fn mark_unknown(&mut self,id:&str)->Result<(),ExecError>{let x=self.get_mut(id)?;if x.state!=IntentState::Prepared{return Err(ExecError::InvalidTransition)};x.state=IntentState::Unknown;Ok(())}
 pub fn reconcile(&mut self,id:&str,_resolution:Resolution,evidence_present:bool)->Result<(),ExecError>{if !evidence_present{return Err(ExecError::MissingEvidence)};let x=self.get_mut(id)?;if !matches!(x.state,IntentState::Prepared|IntentState::Unknown){return Err(ExecError::InvalidTransition)};x.state=IntentState::Reconciled;Ok(())}
}

#[cfg(test)]
mod tests{
 use super::*;
 #[test]fn happy_ack(){let mut s=ExecutionState::default();s.prepare("i1",true,Some("c1")).unwrap();s.acknowledge("i1",false).unwrap();assert_eq!(s.intents[0].state,IntentState::Acknowledged);assert!(s.unresolved().is_none());}
 #[test]fn create_requires_stable_client_id(){let mut s=ExecutionState::default();assert_eq!(s.prepare("i1",true,None),Err(ExecError::MissingClientOrderId));assert!(s.intents.is_empty());}
 #[test]fn prepared_blocks_next_mutation(){let mut s=ExecutionState::default();s.prepare("i1",true,Some("c1")).unwrap();assert_eq!(s.prepare("i2",true,Some("c2")),Err(ExecError::UnresolvedMutation));}
 #[test]fn unknown_blocks_next_mutation(){let mut s=ExecutionState::default();s.prepare("i1",true,Some("c1")).unwrap();s.mark_unknown("i1").unwrap();assert_eq!(s.prepare("i2",true,Some("c2")),Err(ExecError::UnresolvedMutation));}
 #[test]fn evidence_required_to_reconcile(){let mut s=ExecutionState::default();s.prepare("i1",true,Some("c1")).unwrap();s.mark_unknown("i1").unwrap();assert_eq!(s.reconcile("i1",Resolution::NotFound,false),Err(ExecError::MissingEvidence));assert_eq!(s.intents[0].state,IntentState::Unknown);s.reconcile("i1",Resolution::NotFound,true).unwrap();assert_eq!(s.intents[0].state,IntentState::Reconciled);}
 #[test]fn terminal_cannot_transition_again(){let mut s=ExecutionState::default();s.prepare("i1",true,Some("c1")).unwrap();s.acknowledge("i1",false).unwrap();assert_eq!(s.mark_unknown("i1"),Err(ExecError::InvalidTransition));assert_eq!(s.reconcile("i1",Resolution::Confirmed,true),Err(ExecError::InvalidTransition));}
 #[test]fn cancel_does_not_require_client_id(){let mut s=ExecutionState::default();s.prepare("cancel1",false,None).unwrap();assert_eq!(s.intents[0].state,IntentState::Prepared);}
 #[test]fn reopen_snapshot_preserves_barrier(){let mut s=ExecutionState::default();s.prepare("i1",true,Some("c1")).unwrap();s.mark_unknown("i1").unwrap();let raw=serde_json::to_string(&s.intents).unwrap();let intents:Vec<Intent>=serde_json::from_str(&raw).unwrap();let mut reopened=ExecutionState{intents};assert_eq!(reopened.prepare("i2",true,Some("c2")),Err(ExecError::UnresolvedMutation));}
}

#[derive(Clone,Copy,Debug,PartialEq,Eq)]pub enum Leg{Spot,Futures}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]pub enum CoordinatorState{Ready,SpotPending,SpotFilled,FuturesPending,Hedged,UnwindPending,Closed,ManualRequired}
#[derive(Clone,Debug)]pub struct Coordinator{pub state:CoordinatorState,pub safety_generation:u64,pub spot_filled:u64,pub fut_filled:u64}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]pub enum CoordError{SafetyChanged,WrongState,UnknownExecution,PartialFill}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]pub enum EntryFillProgress{NoFill,Exposure{remaining:u64},Hedged}
impl Coordinator{
 pub fn new(g:u64)->Self{Self{state:CoordinatorState::Ready,safety_generation:g,spot_filled:0,fut_filled:0}}
 fn safe(&self,g:u64)->Result<(),CoordError>{if g!=self.safety_generation{Err(CoordError::SafetyChanged)}else{Ok(())}}
 pub fn prepare_spot(&mut self,g:u64)->Result<(),CoordError>{self.safe(g)?;if self.state!=CoordinatorState::Ready{return Err(CoordError::WrongState)};self.state=CoordinatorState::SpotPending;Ok(())}
 pub fn spot_result(&mut self,filled:u64,requested:u64,unknown:bool)->Result<(),CoordError>{if self.state!=CoordinatorState::SpotPending{return Err(CoordError::WrongState)};if unknown{self.state=CoordinatorState::ManualRequired;return Err(CoordError::UnknownExecution)}self.spot_filled=filled;if filled==0{self.state=CoordinatorState::Closed;return Ok(())}if filled<requested{self.state=CoordinatorState::UnwindPending;return Err(CoordError::PartialFill)}self.state=CoordinatorState::SpotFilled;Ok(())}
 pub fn prepare_futures(&mut self,g:u64)->Result<(),CoordError>{self.safe(g)?;if self.state!=CoordinatorState::SpotFilled{return Err(CoordError::WrongState)};self.state=CoordinatorState::FuturesPending;Ok(())}
 pub fn futures_result(&mut self,filled:u64,requested:u64,unknown:bool)->Result<(),CoordError>{if self.state!=CoordinatorState::FuturesPending{return Err(CoordError::WrongState)};if unknown{self.state=CoordinatorState::ManualRequired;return Err(CoordError::UnknownExecution)}self.fut_filled=filled;if filled<requested{self.state=CoordinatorState::UnwindPending;return Err(CoordError::PartialFill)}self.state=CoordinatorState::Hedged;Ok(())}
 pub fn spot_result_revaluable(&mut self,filled:u64,requested:u64,terminal:bool,unknown:bool)->Result<EntryFillProgress,CoordError>{
  if self.state!=CoordinatorState::SpotPending{return Err(CoordError::WrongState)}
  if unknown{self.state=CoordinatorState::ManualRequired;return Err(CoordError::UnknownExecution)}
  if filled>requested{return Err(CoordError::PartialFill)}
  self.spot_filled=filled;
  if !terminal{return Ok(if filled==0{EntryFillProgress::NoFill}else{EntryFillProgress::Exposure{remaining:filled}})}
  if filled==0{self.state=CoordinatorState::Closed;return Ok(EntryFillProgress::NoFill)}
  self.state=CoordinatorState::SpotFilled;
  Ok(EntryFillProgress::Exposure{remaining:filled.saturating_sub(self.fut_filled)})
 }
 pub fn futures_result_revaluable(&mut self,cumulative_filled:u64,terminal:bool,unknown:bool)->Result<EntryFillProgress,CoordError>{
  if self.state!=CoordinatorState::FuturesPending{return Err(CoordError::WrongState)}
  if unknown{self.state=CoordinatorState::ManualRequired;return Err(CoordError::UnknownExecution)}
  if cumulative_filled>self.spot_filled{return Err(CoordError::PartialFill)}
  self.fut_filled=cumulative_filled;
  if !terminal{return Ok(EntryFillProgress::Exposure{remaining:self.spot_filled-cumulative_filled})}
  if cumulative_filled==self.spot_filled{self.state=CoordinatorState::Hedged;return Ok(EntryFillProgress::Hedged)}
  self.state=CoordinatorState::SpotFilled;
  Ok(EntryFillProgress::Exposure{remaining:self.spot_filled-cumulative_filled})
 }
 pub fn prepare_unwind(&mut self,g:u64)->Result<(),CoordError>{self.safe(g)?;if self.state!=CoordinatorState::UnwindPending{return Err(CoordError::WrongState)};Ok(())}
 pub fn unwind_result(&mut self,confirmed:bool){self.state=if confirmed{CoordinatorState::Closed}else{CoordinatorState::ManualRequired};}
}

#[cfg(test)]mod coordinator_tests{use super::*;
 #[test]fn two_leg_happy(){let mut c=Coordinator::new(7);c.prepare_spot(7).unwrap();c.spot_result(10,10,false).unwrap();c.prepare_futures(7).unwrap();c.futures_result(10,10,false).unwrap();assert_eq!(c.state,CoordinatorState::Hedged);}
 #[test]fn safety_change_blocks_second_leg(){let mut c=Coordinator::new(7);c.prepare_spot(7).unwrap();c.spot_result(10,10,false).unwrap();assert_eq!(c.prepare_futures(8),Err(CoordError::SafetyChanged));assert_eq!(c.state,CoordinatorState::SpotFilled);}
 #[test]fn spot_partial_requires_unwind(){let mut c=Coordinator::new(7);c.prepare_spot(7).unwrap();assert_eq!(c.spot_result(4,10,false),Err(CoordError::PartialFill));assert_eq!(c.state,CoordinatorState::UnwindPending);c.prepare_unwind(7).unwrap();c.unwind_result(true);assert_eq!(c.state,CoordinatorState::Closed);}
 #[test]fn futures_partial_requires_unwind(){let mut c=Coordinator::new(7);c.prepare_spot(7).unwrap();c.spot_result(10,10,false).unwrap();c.prepare_futures(7).unwrap();assert_eq!(c.futures_result(3,10,false),Err(CoordError::PartialFill));assert_eq!(c.state,CoordinatorState::UnwindPending);}
 #[test]fn unknown_spot_is_manual(){let mut c=Coordinator::new(7);c.prepare_spot(7).unwrap();assert_eq!(c.spot_result(0,10,true),Err(CoordError::UnknownExecution));assert_eq!(c.state,CoordinatorState::ManualRequired);}
 #[test]fn unknown_futures_is_manual(){let mut c=Coordinator::new(7);c.prepare_spot(7).unwrap();c.spot_result(10,10,false).unwrap();c.prepare_futures(7).unwrap();assert_eq!(c.futures_result(0,10,true),Err(CoordError::UnknownExecution));assert_eq!(c.state,CoordinatorState::ManualRequired);}
 #[test]fn failed_unwind_is_manual(){let mut c=Coordinator::new(7);c.prepare_spot(7).unwrap();let _=c.spot_result(4,10,false);c.prepare_unwind(7).unwrap();c.unwind_result(false);assert_eq!(c.state,CoordinatorState::ManualRequired);}
 #[test]fn safety_change_blocks_unwind_mutation(){let mut c=Coordinator::new(7);c.prepare_spot(7).unwrap();let _=c.spot_result(4,10,false);assert_eq!(c.prepare_unwind(8),Err(CoordError::SafetyChanged));assert_eq!(c.state,CoordinatorState::UnwindPending);}
 #[test]fn zero_spot_fill_closes_without_second_leg(){let mut c=Coordinator::new(7);c.prepare_spot(7).unwrap();c.spot_result(0,10,false).unwrap();assert_eq!(c.state,CoordinatorState::Closed);assert_eq!(c.prepare_futures(7),Err(CoordError::WrongState));}
}

#[derive(Clone,Debug,PartialEq,Eq)]pub enum Action{PersistPrepare{intent_id:String,leg:Leg},NetworkMutation{intent_id:String,leg:Leg},EnterRecovery{intent_id:String}}
#[derive(Clone,Debug)]pub struct MutationGate{pub durable:ExecutionState,pub pending:Option<(String,Leg)>,pub recovery_only:bool}
impl Default for MutationGate{fn default()->Self{Self{durable:ExecutionState::default(),pending:None,recovery_only:false}}}
impl MutationGate{
 pub fn begin(&mut self,id:&str,leg:Leg,client_id:&str)->Result<Action,ExecError>{if self.recovery_only{return Err(ExecError::UnresolvedMutation)};self.durable.prepare(id,true,Some(client_id))?;self.pending=Some((id.into(),leg));Ok(Action::PersistPrepare{intent_id:id.into(),leg})}
 pub fn after_durable_prepare(&self,id:&str)->Result<Action,ExecError>{let (pending,leg)=self.pending.as_ref().ok_or(ExecError::InvalidTransition)?;if pending!=id{return Err(ExecError::InvalidTransition)};let intent=self.durable.intents.iter().find(|x|x.id==id).ok_or(ExecError::InvalidTransition)?;if intent.state!=IntentState::Prepared{return Err(ExecError::InvalidTransition)};Ok(Action::NetworkMutation{intent_id:id.into(),leg:*leg})}
 pub fn network_ack(&mut self,id:&str,rejected:bool)->Result<(),ExecError>{self.durable.acknowledge(id,rejected)?;self.pending=None;Ok(())}
 pub fn network_ambiguous(&mut self,id:&str)->Result<Action,ExecError>{self.durable.mark_unknown(id)?;self.pending=None;self.recovery_only=true;Ok(Action::EnterRecovery{intent_id:id.into()})}
 pub fn reopen(intents:Vec<Intent>)->Self{let recovery_only=intents.iter().any(|x|matches!(x.state,IntentState::Prepared|IntentState::Unknown));Self{durable:ExecutionState{intents},pending:None,recovery_only}}
 pub fn reconcile(&mut self,id:&str,resolution:Resolution,evidence:bool)->Result<(),ExecError>{self.durable.reconcile(id,resolution,evidence)?;self.recovery_only=self.durable.unresolved().is_some();Ok(())}
 pub fn unresolved_for_test(&self)->bool{self.durable.unresolved().is_some()}
}

#[cfg(test)]mod mutation_gate_tests{use super::*;
 #[test]fn network_action_only_after_prepare(){let mut g=MutationGate::default();let a=g.begin("i1",Leg::Spot,"c1").unwrap();assert!(matches!(a,Action::PersistPrepare{..}));let n=g.after_durable_prepare("i1").unwrap();assert!(matches!(n,Action::NetworkMutation{leg:Leg::Spot,..}));}
 #[test]fn wrong_intent_cannot_reach_network(){let mut g=MutationGate::default();g.begin("i1",Leg::Spot,"c1").unwrap();assert_eq!(g.after_durable_prepare("i2"),Err(ExecError::InvalidTransition));}
 #[test]fn ambiguous_network_enters_recovery_only(){let mut g=MutationGate::default();g.begin("i1",Leg::Spot,"c1").unwrap();let _=g.after_durable_prepare("i1").unwrap();let a=g.network_ambiguous("i1").unwrap();assert!(matches!(a,Action::EnterRecovery{..}));assert!(g.recovery_only);assert_eq!(g.begin("i2",Leg::Futures,"c2"),Err(ExecError::UnresolvedMutation));}
 #[test]fn crash_after_prepare_reopens_recovery_only(){let mut g=MutationGate::default();g.begin("i1",Leg::Spot,"c1").unwrap();let raw=serde_json::to_string(&g.durable.intents).unwrap();let intents:Vec<Intent>=serde_json::from_str(&raw).unwrap();let mut r=MutationGate::reopen(intents);assert!(r.recovery_only);assert_eq!(r.begin("i2",Leg::Futures,"c2"),Err(ExecError::UnresolvedMutation));}
 #[test]fn crash_after_unknown_never_retries_blindly(){let mut g=MutationGate::default();g.begin("i1",Leg::Spot,"c1").unwrap();g.network_ambiguous("i1").unwrap();let raw=serde_json::to_string(&g.durable.intents).unwrap();let intents:Vec<Intent>=serde_json::from_str(&raw).unwrap();let r=MutationGate::reopen(intents);assert!(r.recovery_only);assert_eq!(r.after_durable_prepare("i1"),Err(ExecError::InvalidTransition));}
 #[test]fn evidence_reconciliation_releases_barrier(){let mut g=MutationGate::default();g.begin("i1",Leg::Spot,"c1").unwrap();g.network_ambiguous("i1").unwrap();assert_eq!(g.reconcile("i1",Resolution::NotFound,false),Err(ExecError::MissingEvidence));assert!(g.recovery_only);g.reconcile("i1",Resolution::NotFound,true).unwrap();assert!(!g.recovery_only);assert!(g.begin("i2",Leg::Spot,"c2").is_ok());}
 #[test]fn ack_releases_pending_for_next_intent(){let mut g=MutationGate::default();g.begin("i1",Leg::Spot,"c1").unwrap();g.network_ack("i1",false).unwrap();assert!(g.begin("i2",Leg::Futures,"c2").is_ok());}
}

#[derive(Clone,Copy,Debug,PartialEq,Eq)]pub enum CrashPoint{BeforePrepare,AfterPrepare,AfterSend,BetweenLegs}
#[derive(Clone,Debug)]pub struct FakePipeline{pub coord:Coordinator,pub gate:MutationGate}
impl FakePipeline{
 pub fn new(g:u64)->Self{Self{coord:Coordinator::new(g),gate:MutationGate::default()}}
 pub fn spot_begin(&mut self,g:u64,id:&str,cid:&str)->Result<Action,CoordError>{self.coord.prepare_spot(g)?;self.gate.begin(id,Leg::Spot,cid).map_err(|_|CoordError::WrongState)}
 pub fn spot_sent(&self,id:&str)->Result<Action,CoordError>{self.gate.after_durable_prepare(id).map_err(|_|CoordError::WrongState)}
 pub fn spot_ack(&mut self,id:&str,filled:u64,requested:u64)->Result<(),CoordError>{self.gate.network_ack(id,false).map_err(|_|CoordError::WrongState)?;self.coord.spot_result(filled,requested,false)}
 pub fn spot_unknown(&mut self,id:&str)->Result<(),CoordError>{self.gate.network_ambiguous(id).map_err(|_|CoordError::WrongState)?;let _=self.coord.spot_result(0,1,true);Err(CoordError::UnknownExecution)}
 pub fn fut_begin(&mut self,g:u64,id:&str,cid:&str)->Result<Action,CoordError>{self.coord.prepare_futures(g)?;self.gate.begin(id,Leg::Futures,cid).map_err(|_|CoordError::WrongState)}
 pub fn fut_sent(&self,id:&str)->Result<Action,CoordError>{self.gate.after_durable_prepare(id).map_err(|_|CoordError::WrongState)}
 pub fn fut_ack(&mut self,id:&str,filled:u64,requested:u64)->Result<(),CoordError>{self.gate.network_ack(id,false).map_err(|_|CoordError::WrongState)?;self.coord.futures_result(filled,requested,false)}
 pub fn fut_unknown(&mut self,id:&str)->Result<(),CoordError>{self.gate.network_ambiguous(id).map_err(|_|CoordError::WrongState)?;let _=self.coord.futures_result(0,1,true);Err(CoordError::UnknownExecution)}
}

#[cfg(test)]mod failure_pipeline_tests{use super::*;
 #[test]fn e2e_happy(){let mut p=FakePipeline::new(9);assert!(matches!(p.spot_begin(9,"s","cs").unwrap(),Action::PersistPrepare{..}));assert!(matches!(p.spot_sent("s").unwrap(),Action::NetworkMutation{..}));p.spot_ack("s",10,10).unwrap();p.fut_begin(9,"f","cf").unwrap();p.fut_sent("f").unwrap();p.fut_ack("f",10,10).unwrap();assert_eq!(p.coord.state,CoordinatorState::Hedged);}
 #[test]fn crash_after_spot_prepare_is_recovery_only(){let mut p=FakePipeline::new(9);p.spot_begin(9,"s","cs").unwrap();let raw=serde_json::to_string(&p.gate.durable.intents).unwrap();let intents=serde_json::from_str(&raw).unwrap();let r=MutationGate::reopen(intents);assert!(r.recovery_only);assert!(r.unresolved_for_test());}
 #[test]fn timeout_after_spot_send_never_opens_futures(){let mut p=FakePipeline::new(9);p.spot_begin(9,"s","cs").unwrap();p.spot_sent("s").unwrap();assert_eq!(p.spot_unknown("s"),Err(CoordError::UnknownExecution));assert_eq!(p.coord.state,CoordinatorState::ManualRequired);assert!(p.gate.recovery_only);assert_eq!(p.fut_begin(9,"f","cf"),Err(CoordError::WrongState));}
 #[test]fn safety_flip_between_legs_blocks_futures_before_prepare(){let mut p=FakePipeline::new(9);p.spot_begin(9,"s","cs").unwrap();p.spot_ack("s",10,10).unwrap();assert_eq!(p.fut_begin(10,"f","cf"),Err(CoordError::SafetyChanged));assert!(p.gate.durable.intents.iter().all(|x|x.id!="f"));}
 #[test]fn spot_partial_does_not_create_futures_intent(){let mut p=FakePipeline::new(9);p.spot_begin(9,"s","cs").unwrap();assert_eq!(p.spot_ack("s",4,10),Err(CoordError::PartialFill));assert_eq!(p.coord.state,CoordinatorState::UnwindPending);assert!(p.gate.durable.intents.iter().all(|x|x.id!="f"));}
 #[test]fn futures_timeout_is_manual_and_recovery_only(){let mut p=FakePipeline::new(9);p.spot_begin(9,"s","cs").unwrap();p.spot_ack("s",10,10).unwrap();p.fut_begin(9,"f","cf").unwrap();p.fut_sent("f").unwrap();assert_eq!(p.fut_unknown("f"),Err(CoordError::UnknownExecution));assert_eq!(p.coord.state,CoordinatorState::ManualRequired);assert!(p.gate.recovery_only);}
 #[test]fn futures_partial_enters_unwind_not_hedged(){let mut p=FakePipeline::new(9);p.spot_begin(9,"s","cs").unwrap();p.spot_ack("s",10,10).unwrap();p.fut_begin(9,"f","cf").unwrap();assert_eq!(p.fut_ack("f",3,10),Err(CoordError::PartialFill));assert_eq!(p.coord.state,CoordinatorState::UnwindPending);}
}

#[derive(Clone,Copy,Debug,PartialEq,Eq)]pub enum ProbeOutcome{Found,NotFound,Mismatch,Error,Unsupported}
impl MutationGate{
 pub fn recovery_probe(&mut self,id:&str,outcome:ProbeOutcome)->Result<(),ExecError>{
  let x=self.durable.intents.iter().find(|x|x.id==id).ok_or(ExecError::InvalidTransition)?;
  if !matches!(x.state,IntentState::Prepared|IntentState::Unknown){return Err(ExecError::InvalidTransition)}
  match outcome{ProbeOutcome::Found|ProbeOutcome::NotFound|ProbeOutcome::Mismatch|ProbeOutcome::Error|ProbeOutcome::Unsupported=>{self.recovery_only=true;Ok(())}}
 }
}
#[cfg(test)]mod recovery_matrix_tests{use super::*;
 fn unknown_gate()->MutationGate{let mut g=MutationGate::default();g.begin("i",Leg::Spot,"c").unwrap();g.network_ambiguous("i").unwrap();g}
 #[test]fn found_probe_does_not_resolve(){let mut g=unknown_gate();g.recovery_probe("i",ProbeOutcome::Found).unwrap();assert!(g.recovery_only);assert!(g.unresolved_for_test());}
 #[test]fn not_found_probe_does_not_resolve(){let mut g=unknown_gate();g.recovery_probe("i",ProbeOutcome::NotFound).unwrap();assert!(g.recovery_only);assert!(g.unresolved_for_test());}
 #[test]fn mismatch_probe_does_not_resolve(){let mut g=unknown_gate();g.recovery_probe("i",ProbeOutcome::Mismatch).unwrap();assert!(g.recovery_only);assert!(g.unresolved_for_test());}
 #[test]fn error_probe_never_means_not_found(){let mut g=unknown_gate();g.recovery_probe("i",ProbeOutcome::Error).unwrap();assert!(g.recovery_only);assert_eq!(g.durable.intents[0].state,IntentState::Unknown);}
 #[test]fn unsupported_probe_stays_blocked(){let mut g=unknown_gate();g.recovery_probe("i",ProbeOutcome::Unsupported).unwrap();assert!(g.recovery_only);}
 #[test]fn crash_between_legs_has_no_phantom_second_intent(){let mut p=FakePipeline::new(4);p.spot_begin(4,"s","cs").unwrap();p.spot_ack("s",10,10).unwrap();let raw=serde_json::to_string(&p.gate.durable.intents).unwrap();let intents:Vec<Intent>=serde_json::from_str(&raw).unwrap();let r=MutationGate::reopen(intents);assert!(!r.recovery_only);assert_eq!(r.durable.intents.len(),1);assert_eq!(r.durable.intents[0].state,IntentState::Acknowledged);}
 #[test]fn crash_after_second_prepare_blocks_resend(){let mut p=FakePipeline::new(4);p.spot_begin(4,"s","cs").unwrap();p.spot_ack("s",10,10).unwrap();p.fut_begin(4,"f","cf").unwrap();let raw=serde_json::to_string(&p.gate.durable.intents).unwrap();let intents:Vec<Intent>=serde_json::from_str(&raw).unwrap();let mut r=MutationGate::reopen(intents);assert!(r.recovery_only);assert_eq!(r.begin("f2",Leg::Futures,"cf2"),Err(ExecError::UnresolvedMutation));assert_eq!(r.after_durable_prepare("f"),Err(ExecError::InvalidTransition));}
}

#[cfg(test)]mod exact_python_contract_tests{use super::*;
 #[test]fn all_four_resolutions_require_evidence_and_close_unresolved(){for res in [Resolution::Confirmed,Resolution::NotFound,Resolution::Rejected,Resolution::Duplicate]{let mut g=MutationGate::default();g.begin("i",Leg::Spot,"c").unwrap();g.network_ambiguous("i").unwrap();assert_eq!(g.reconcile("i",res,false),Err(ExecError::MissingEvidence));g.reconcile("i",res,true).unwrap();assert_eq!(g.durable.intents[0].state,IntentState::Reconciled);assert!(!g.recovery_only);}}
 #[test]fn rejected_ack_is_terminal_and_not_unresolved(){let mut g=MutationGate::default();g.begin("i",Leg::Spot,"c").unwrap();g.network_ack("i",true).unwrap();assert_eq!(g.durable.intents[0].state,IntentState::Rejected);assert!(!g.recovery_only);assert!(g.begin("next",Leg::Spot,"c2").is_ok());}
 #[test]fn duplicate_ack_is_invalid_transition(){let mut g=MutationGate::default();g.begin("i",Leg::Spot,"c").unwrap();g.network_ack("i",false).unwrap();assert_eq!(g.network_ack("i",false),Err(ExecError::InvalidTransition));}
 #[test]fn terminal_reconciliation_is_invalid(){for terminal_rejected in [false,true]{let mut g=MutationGate::default();g.begin("i",Leg::Spot,"c").unwrap();g.network_ack("i",terminal_rejected).unwrap();assert_eq!(g.reconcile("i",Resolution::Confirmed,true),Err(ExecError::InvalidTransition));}}
 #[test]fn duplicate_intent_id_fails_even_after_terminal(){let mut g=MutationGate::default();g.begin("i",Leg::Spot,"c").unwrap();g.network_ack("i",false).unwrap();assert_eq!(g.begin("i",Leg::Spot,"c"),Err(ExecError::DuplicateIntent));}
}

#[derive(Clone,Debug,PartialEq,Eq)]
pub struct IntentIdentity { pub intent_id:String, pub client_order_id:String, pub request_hash:String }
#[derive(Clone,Debug,Default)]
pub struct IdentityLedger { pub rows:Vec<IntentIdentity> }
impl IdentityLedger {
 pub fn add(&mut self,i:&str,c:&str,h:&str)->Result<(),ExecError>{
  if i.is_empty()||c.is_empty()||h.is_empty(){return Err(ExecError::InvalidTransition)}
  if self.rows.iter().any(|x|x.intent_id==i||x.client_order_id==c){return Err(ExecError::DuplicateIntent)}
  self.rows.push(IntentIdentity{intent_id:i.into(),client_order_id:c.into(),request_hash:h.into()}); Ok(())
 }
 pub fn verify(&self,i:&str,c:&str,h:&str)->Result<(),ExecError>{
  match self.rows.iter().find(|x|x.intent_id==i){Some(x) if x.client_order_id==c&&x.request_hash==h=>Ok(()),_=>Err(ExecError::InvalidTransition)}
 }
}

#[cfg(test)] mod identity_tests { use super::*;
 #[test] fn exact_identity_matches(){let mut l=IdentityLedger::default();l.add("i","c","h1").unwrap();assert_eq!(l.verify("i","c","h1"),Ok(()));}
 #[test] fn hash_mismatch_denies(){let mut l=IdentityLedger::default();l.add("i","c","h1").unwrap();assert_eq!(l.verify("i","c","h2"),Err(ExecError::InvalidTransition));}
 #[test] fn client_mismatch_denies(){let mut l=IdentityLedger::default();l.add("i","c","h1").unwrap();assert_eq!(l.verify("i","x","h1"),Err(ExecError::InvalidTransition));}
 #[test] fn duplicate_client_id_denies(){let mut l=IdentityLedger::default();l.add("i1","c","h1").unwrap();assert_eq!(l.add("i2","c","h2"),Err(ExecError::DuplicateIntent));}
 #[test] fn duplicate_intent_id_denies(){let mut l=IdentityLedger::default();l.add("i","c1","h1").unwrap();assert_eq!(l.add("i","c2","h2"),Err(ExecError::DuplicateIntent));}
}


#[cfg(test)] mod r19_revaluation_coordinator_tests { use super::*;
 #[test]fn partial_spot_can_enter_revaluation_without_forced_unwind(){let mut c=Coordinator::new(7);c.prepare_spot(7).unwrap();
  assert_eq!(c.spot_result_revaluable(4,10,true,false),Ok(EntryFillProgress::Exposure{remaining:4}));assert_eq!(c.state,CoordinatorState::SpotFilled);assert_eq!(c.spot_filled,4);}
 #[test]fn partial_futures_can_continue_only_remaining_hedge(){let mut c=Coordinator::new(7);c.prepare_spot(7).unwrap();c.spot_result_revaluable(4,10,true,false).unwrap();
  c.prepare_futures(7).unwrap();assert_eq!(c.futures_result_revaluable(3,true,false),Ok(EntryFillProgress::Exposure{remaining:1}));assert_eq!(c.state,CoordinatorState::SpotFilled);
  c.prepare_futures(7).unwrap();assert_eq!(c.futures_result_revaluable(4,true,false),Ok(EntryFillProgress::Hedged));assert_eq!(c.state,CoordinatorState::Hedged);}
 #[test]fn revaluable_path_never_allows_futures_overhedge(){let mut c=Coordinator::new(7);c.prepare_spot(7).unwrap();c.spot_result_revaluable(4,10,true,false).unwrap();c.prepare_futures(7).unwrap();
  assert_eq!(c.futures_result_revaluable(5,true,false),Err(CoordError::PartialFill));assert_ne!(c.state,CoordinatorState::Hedged);}
 #[test]fn legacy_partial_contract_remains_unchanged(){let mut c=Coordinator::new(7);c.prepare_spot(7).unwrap();assert_eq!(c.spot_result(4,10,false),Err(CoordError::PartialFill));assert_eq!(c.state,CoordinatorState::UnwindPending);}
 #[test]fn nonterminal_partial_order_blocks_followup_mutation(){let mut c=Coordinator::new(7);c.prepare_spot(7).unwrap();
  assert_eq!(c.spot_result_revaluable(4,10,false,false),Ok(EntryFillProgress::Exposure{remaining:4}));assert_eq!(c.prepare_futures(7),Err(CoordError::WrongState));
  c.state=CoordinatorState::SpotFilled;c.prepare_futures(7).unwrap();assert_eq!(c.futures_result_revaluable(2,false,false),Ok(EntryFillProgress::Exposure{remaining:2}));assert_eq!(c.state,CoordinatorState::FuturesPending);assert_eq!(c.prepare_futures(7),Err(CoordError::WrongState));}
 #[test]fn safety_generation_still_rechecked_before_every_followup_hedge(){let mut c=Coordinator::new(7);c.prepare_spot(7).unwrap();c.spot_result_revaluable(4,10,true,false).unwrap();
  assert_eq!(c.prepare_futures(8),Err(CoordError::SafetyChanged));assert_eq!(c.state,CoordinatorState::SpotFilled);}
}
