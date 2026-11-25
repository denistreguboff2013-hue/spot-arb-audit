// Astra P0-01 R972: read-only cross-check of TWO local modeled PAPER
// files after an interrupted episode. It CAN describe unresolved modeled
// exposure, never release a quarantine or reinitialize a fresh portfolio.
// Neither local SHA chain is independent rollback or exchange-fill proof.
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct InterruptedModeledBundleCensus{
 pub checkpoint_revision:u64,
 pub journal_sequence:u64,
 pub journal_local_hash:String,
 pub observed_entries:u64,
 pub observed_closed:u64,
 pub open_bases:Vec<String>,
 pub modeled_phase_by_base:std::collections::BTreeMap<String,String>,
 pub modeled_realized_bits:u64,
 pub automatic_resume_allowed:bool,
 pub real_orders_allowed:bool,
}
pub fn inspect_interrupted_modeled_bundle(
 checkpoint:&Path,journal:&Path,session:&str,
 expected_day:i64,started_wall_ms:i64
)->Result<InterruptedModeledBundleCensus,CheckpointError>{
 if session.is_empty()||session.len()>100||
   !session.starts_with("session_")||
   !session.bytes().all(|byte|byte.is_ascii_alphanumeric()||byte==b'_'){
  return Err(deny("missing or invalid modeled episode identity"))
 }
 let first=crate::native_virtual_four_leg_provenance_review::
   inspect_model_journal(journal)
   .map_err(|_|deny("missing or corrupt modeled audit journal"))?;
 if first.sealed||first.session!=session{
  return Err(deny("not an interrupted modeled episode"))
 }
 inspect_checkpoint_model_alignment(checkpoint,journal,session,false)?;
 let incident=inspect_interrupted_virtual_exposure(
   checkpoint,expected_day,started_wall_ms)?;
 let last=crate::native_virtual_four_leg_provenance_review::
   inspect_model_journal(journal)
   .map_err(|_|deny("modeled journal changed during incident census"))?;
 if first!=last||
   incident.observed_entries!=last.entries||
   incident.observed_closed!=last.closed||
   incident.open_bases.len()!=last.open||
   incident.open_bases.iter().ne(
     last.model_phase_by_base.keys()){
  return Err(deny("modeled checkpoint and journal split-brain"))
 }
 Ok(InterruptedModeledBundleCensus{
  checkpoint_revision:incident.checkpoint_revision,
  journal_sequence:last.sequence,
  journal_local_hash:last.last_hash,
  observed_entries:incident.observed_entries,
  observed_closed:incident.observed_closed,
  open_bases:incident.open_bases,
  modeled_phase_by_base:last.model_phase_by_base,
  modeled_realized_bits:last.modeled_realized_bits,
  automatic_resume_allowed:false,
  real_orders_allowed:false,
 })
}
