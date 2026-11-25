//! R89: read-only cold R14 ACK/PREPARED/UNKNOWN vs R15 pending intent
//! vs existing five-store R88 recovery. Never a network mutation permit.
use std::{fs::{self,File},io::{Read},path::Path};
use openssl::sha::sha256;
use crate::{
 execution::IntentState,
 execution_store::DurableIntentStore,
 position_lifecycle::{PositionLifecycle,LifecycleState},
 native_r15_cross_store_cold_review::inspect_r88_cold_original_close_boundary,
};
const MAX_R14:u64=1024*1024;
const MAX_R15:u64=1024*1024;
#[derive(Debug,Clone,PartialEq,Eq)]
pub enum R89Error{Denied(&'static str)}
fn deny(s:&'static str)->R89Error{R89Error::Denied(s)}
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct R89R14ColdAckDiagnostic{
 pub original_pending_state:IntentState,
 pub same_r14_r15_intent_and_client:bool,
 pub r14_ack_does_not_prove_terminal_fill:bool,
 pub r14_pending_reconciliation_required:bool,
 pub independently_authenticated_exchange_fill:bool,
 pub all_six_files_atomically_committed:bool,
 pub may_send_or_retry_or_resume:bool,
 pub verified_closed_or_flat:bool,
}
fn bounded_hash(path:&Path,max:u64)->Result<[u8;32],R89Error>{
 let m=fs::symlink_metadata(path).map_err(|_|deny("R14/R15 source absent"))?;
 if !m.file_type().is_file()||m.len()==0||m.len()>max{
  return Err(deny("R14/R15 source must be bounded regular file"))
 }
 let mut data=Vec::new();
 File::open(path).map_err(|_|deny("R14/R15 file open failed"))?
   .take(max+1).read_to_end(&mut data)
   .map_err(|_|deny("R14/R15 file read failed"))?;
 if data.is_empty()||data.len() as u64>max{
  return Err(deny("R14/R15 source grew during audit"))
 }
 Ok(sha256(&data))
}
fn no_stage(path:&Path)->Result<(),R89Error>{
 if path.with_extension("tmp").symlink_metadata().is_ok(){
  return Err(deny("R14/R15 fsync stage interrupted"))
 }
 Ok(())
}
pub fn inspect_r89_r14_r15_v2_paper_cold_ack(
 r14_intents_file:&Path,r15_lifecycle_file:&Path,
 r87_original_binding:&Path,v2_file:&Path,
 immutable_close_archive:&Path,paper_journal:&Path,
 base:&str,paper_seq:u64,paper_hash:&str
)->Result<R89R14ColdAckDiagnostic,R89Error>{
 // R14 is deliberately checked BEFORE R88; read-only check does not
 // consume the original intent or change a durable recovery status.
 no_stage(r14_intents_file)?;
 no_stage(r15_lifecycle_file)?;
 let r14_before=bounded_hash(r14_intents_file,MAX_R14)?;
 let r15_before=bounded_hash(r15_lifecycle_file,MAX_R15)?;
 let local=inspect_r88_cold_original_close_boundary(
  r15_lifecycle_file,r87_original_binding,v2_file,
  immutable_close_archive,paper_journal,base,paper_seq,paper_hash)
  .map_err(|_|deny("R88 five-file cold owner failed"))?;
 if local.may_resume_or_send_or_retry||
    !local.original_r15_owner_exact_local_match||
    !local.original_close_pending_reconciliation_required{
  return Err(deny("R88 local source would incorrectly authorize execution"))
 }
 let store=DurableIntentStore::open(r14_intents_file)
   .map_err(|_|deny("R14 store inaccessible"))?;
 let intents=store.load().map_err(|_|deny("R14 journal unreadable or staged"))?;
 if intents.is_empty(){return Err(deny("R14 has no original intent"))}
 let raw=fs::read(r15_lifecycle_file)
   .map_err(|_|deny("R15 source disappeared"))?;
 if raw.len()>MAX_R15 as usize{return Err(deny("R15 source too large"))}
 let r15:PositionLifecycle=serde_json::from_slice(&raw)
   .map_err(|_|deny("R15 original intent decode failed"))?;
 if matches!(r15.state,LifecycleState::Closed|LifecycleState::ManualRequired){
  return Err(deny("R15 is not unresolved on cold start"))
 }
 let pending=r15.pending.as_ref()
   .ok_or(deny("R15 pending original intent missing"))?;
 if pending.intent_id.is_empty()||pending.client_id.is_empty(){
  return Err(deny("R15 original intent identity absent"))
 }
 let mut matched=None;
 for intent in &intents{
  // Duplicate intent or client identities across even retired generations
  // cannot be accepted as evidence of a unique pending mutation.
  if intents.iter().filter(|x|x.id==intent.id).count()!=1||
     intent.client_order_id.as_ref().is_some_and(|client|
       intents.iter().filter(|x|x.client_order_id.as_ref()==Some(client)).count()!=1){
   return Err(deny("R14 duplicate source identity"))
  }
  if intent.id==pending.intent_id{
   if intent.client_order_id.as_deref()!=Some(pending.client_id.as_str()){
    return Err(deny("R14 ACK belongs to another client/order"))
   }
   match intent.state{
    IntentState::Prepared|IntentState::Acknowledged|IntentState::Unknown=>{
       matched=Some(intent.state)
    },
    IntentState::Rejected|IntentState::Reconciled=>
       return Err(deny("R14 terminal/other disposition cannot certify pending R15")),
   }
  }else if matches!(intent.state,
    IntentState::Prepared|IntentState::Acknowledged|
    IntentState::Unknown|IntentState::Rejected){
     return Err(deny("different unresolved R14 intent exists"))
  }
 }
 let status=matched.ok_or(deny("R14 has no exact R15 pending owner"))?;
 // Check that no R14/R15 source changed during the multi-step join.
 no_stage(r14_intents_file)?;
 no_stage(r15_lifecycle_file)?;
 if r14_before!=bounded_hash(r14_intents_file,MAX_R14)?||
    r15_before!=bounded_hash(r15_lifecycle_file,MAX_R15)?{
  return Err(deny("R14/R15 sources changed during audit"))
 }
 Ok(R89R14ColdAckDiagnostic{
  original_pending_state:status,same_r14_r15_intent_and_client:true,
  r14_ack_does_not_prove_terminal_fill:true,
  r14_pending_reconciliation_required:true,
  independently_authenticated_exchange_fill:false,
  all_six_files_atomically_committed:false,
  may_send_or_retry_or_resume:false,verified_closed_or_flat:false,
 })
}
