//! Astra R88 P0-02: bounded zero-network crash-boundary review of five
//! separately durable R15 / R87 original owner / V2 / source / PAPER files.
//! A successful LOCAL match is never cross-store atomicity, authenticated
//! exchange proof, automatic retry, flatness or permission to trade.
use std::{fs::{self,File},io::Read,path::Path};
use openssl::sha::sha256;
use crate::{
 position_lifecycle::{PositionLifecycle,LifecycleState},
 native_trade2f3_v2_versioned_store::load_v2,
 native_paper_v2_close_source_archive_review::read_local_close_source,
 native_paper_v2_cold_files_review::inspect_cold_p002_archived_four_leg_files,
 native_paper_durable_journal_review::inspect_journal_against_supplied_floor,
 native_r15_original_owner_binding_review::inspect_r87_cold_owner_join,
};
const MAX_LOCAL_BYTES:u64=16*1024*1024;
#[derive(Debug,Clone,PartialEq,Eq)]
pub enum R88Error{Incomplete(&'static str),Different(&'static str)}
fn incomplete(s:&'static str)->R88Error{R88Error::Incomplete(s)}
fn different(s:&'static str)->R88Error{R88Error::Different(s)}
/// Supplied paths and journal hash floor are only local operator values.
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct R88ColdFilesDiagnostic{
 pub five_local_files_stable_during_read:bool,
 pub original_r15_owner_exact_local_match:bool,
 pub source_archive_is_valid_local_witness:bool,
 pub original_close_pending_reconciliation_required:bool,
 pub atomic_cross_store_commit_proven:bool,
 pub independent_exchange_account_truth:bool,
 pub external_antirollback_floor_proven:bool,
 pub may_resume_or_send_or_retry:bool,
 pub verified_closed_or_flat:bool,
}
fn file_bytes(path:&Path)->Result<Vec<u8>,R88Error>{
 let meta=fs::symlink_metadata(path)
   .map_err(|_|incomplete("source file missing or not readable"))?;
 if !meta.file_type().is_file()||meta.len()==0||
    meta.len()>MAX_LOCAL_BYTES{
  return Err(incomplete("local source not a bounded regular file"))
 }
 let f=File::open(path).map_err(|_|incomplete("local file open denied"))?;
 let mut bytes=Vec::new();
 f.take(MAX_LOCAL_BYTES+1).read_to_end(&mut bytes)
   .map_err(|_|incomplete("local file read denied"))?;
 if bytes.is_empty()||bytes.len() as u64>MAX_LOCAL_BYTES{
  return Err(incomplete("local source grew or disappeared"))
 }
 Ok(bytes)
}
fn fingerprint(path:&Path)->Result<[u8;32],R88Error>{
 Ok(sha256(&file_bytes(path)?))
}
/// Reject an uncommitted R15 staged write. Existing R15 legacy recovery may
/// interpret prior state as recovery-only; here ambiguity blocks the JOIN.
fn require_no_r15_stage(path:&Path)->Result<(),R88Error>{
 if path.with_extension("tmp").symlink_metadata().is_ok(){
  return Err(incomplete("R15 temp write remains after crash"))
 }
 Ok(())
}
pub fn inspect_r88_cold_original_close_boundary(
 r15_file:&Path,r87_owner_sidecar:&Path,v2_owner_file:&Path,
 close_archive_file:&Path,paper_journal:&Path,
 base:&str,expected_paper_seq:u64,expected_paper_hash:&str
)->Result<R88ColdFilesDiagnostic,R88Error>{
 // Strictly fail closed before reading ANY source whose previous writer
 // did not complete its own local durable publication contract.
 require_no_r15_stage(r15_file)?;
 let paths=[r15_file,r87_owner_sidecar,v2_owner_file,
     close_archive_file,paper_journal];
 let mut before=Vec::with_capacity(paths.len());
 for path in paths{before.push(fingerprint(path)?);}
 let raw_r15=file_bytes(r15_file)?;
 let mut r15:PositionLifecycle=serde_json::from_slice(&raw_r15)
   .map_err(|_|incomplete("R15 durable owner decode invalid"))?;
 if r15.pending.is_some()&&!matches!(r15.state,
   LifecycleState::Closed|LifecycleState::ManualRequired){
  r15.state=LifecycleState::RecoveryRequired;
 }
 // Validate LOCAL V2 revision and immutable source archive under the
 // existing R72/R74/R79 journal owner/four-leg constraints.
 let local=inspect_cold_p002_archived_four_leg_files(
   paper_journal,v2_owner_file,close_archive_file,base,
   expected_paper_seq,expected_paper_hash)
   .map_err(|_|different("V2/PAPER/close archive local cold alignment differs"))?;
 if !local.source_archive_verified||!local.source_matches_local_paper_cas||
    !local.local_paper_legs_fee_and_qty_consistent||
    local.may_resume_or_retry_paper||local.may_trade_or_cancel_real{
   return Err(different("local archive claims forbidden authority"))
 }
 let v2=load_v2(v2_owner_file)
   .map_err(|_|different("V2 source identity changed after archived cold check"))?;
 let close=read_local_close_source(close_archive_file,&v2)
   .map_err(|_|different("close archive changed after cold check"))?;
 let paper=inspect_journal_against_supplied_floor(
   paper_journal,expected_paper_seq,expected_paper_hash)
   .map_err(|_|different("PAPER durable CAS changed after cold check"))?;
 let owner=inspect_r87_cold_owner_join(
   r87_owner_sidecar,&r15,&paper,base,&v2,&close)
   .map_err(|_|different("R15 sidecar original owner or pending close differs"))?;
 if !owner.local_original_order_identity_matches||
    !owner.r86.r15_pending_reconciliation_required||
    owner.may_retry_or_resume||owner.r86.verified_closed{
   return Err(different("cross-store join attempted to authorize progress"))
 }
 // Second pass catches ordinary file changes during sequential reads.
 // It is NOT a transactional snapshot or external antirollback witness.
 require_no_r15_stage(r15_file)?;
 for (path,previous) in paths.iter().zip(before.iter()){
  if fingerprint(path)?!=*previous{
   return Err(different("one of five source files changed during audit"))
  }
 }
 Ok(R88ColdFilesDiagnostic{
  five_local_files_stable_during_read:true,
  original_r15_owner_exact_local_match:true,
  source_archive_is_valid_local_witness:true,
  original_close_pending_reconciliation_required:true,
  atomic_cross_store_commit_proven:false,
  independent_exchange_account_truth:false,
  external_antirollback_floor_proven:false,
  may_resume_or_send_or_retry:false,
  verified_closed_or_flat:false,
 })
}
