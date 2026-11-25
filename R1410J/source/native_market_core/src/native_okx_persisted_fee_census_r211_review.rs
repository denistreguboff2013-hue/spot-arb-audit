//! Astra R211: locally complete ARCHIVE DIRECTORY with persisted V2 owners
//! plus R210 fee day diagnostics. Does not certify exchange-wide completeness.
use std::path::Path;
use crate::{
 native_trading_config::NativeTradingConfig,
 native_trade2f3_v2_versioned_store::{load_v2,V2ReviewState},
 native_paper_v2_close_source_archive_review::{
  inspect_persisted_local_archive_directory_census,
  read_local_close_source,LocalArchivedCloseCandidate,
  PersistedLocalArchivedCloseCandidate,ArchiveError,
 },
 native_okx_close_fee_archive_r210_review::{
  inspect_archived_okx_buy_sell_fee_day_r210,
  OkxArchiveFeeDayReviewR210,OkxArchiveFeeError,
 },
};
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum PersistedFeeCensusErrorR211{
 Config,Archive(ArchiveError),Review(OkxArchiveFeeError),Drift,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct PersistedFeeCensusR211{
 pub local_archive_directory_matches_inputs:bool,
 pub local_archives_checked:usize,
 pub fee_day:OkxArchiveFeeDayReviewR210,
 pub account_wide_exchange_history_authenticated:bool,
 pub complete_unresolved_owner_inventory_proven:bool,
 pub day_fees_proven_complete:bool,
 pub independently_verified_closed:bool,
 pub may_trade_real:bool,
}
/// Bounded read-only local diagnostic; callers provide an archive-only
/// DIRECTORY and explicit matching persisted, immutable owner files.
/// Existing R200+ directory census denies omissions within this directory,
/// duplicate generations, symlinks, stale owner links and bad archives.
/// Cannot discover another directory, absent rows or external rollbacks.
pub fn inspect_persisted_okx_fee_directory_r211(
 cfg:&NativeTradingConfig,now_ms:u64,account_owner:&str,
 archive_directory:&Path,
 sources:&[PersistedLocalArchivedCloseCandidate<'_>],
 unresolved_orders:&[String],
)->Result<PersistedFeeCensusR211,PersistedFeeCensusErrorR211>{
 if !cfg.source_projection_unchanged()
   ||sources.is_empty()||sources.len()>128
   ||cfg.policy.real_execution_authorized||cfg.policy.real_daily_trades!=0{
   return Err(PersistedFeeCensusErrorR211::Config)
 }
 let census=inspect_persisted_local_archive_directory_census(
   archive_directory,sources).map_err(PersistedFeeCensusErrorR211::Archive)?;
 if census.exchange_account_authenticated||census.complete_owner_universe_proven
   ||census.independently_verified_closed||census.may_trade_real{
   return Err(PersistedFeeCensusErrorR211::Drift)
 }
 // Build the read-only fee view exclusively from cold-loaded original owner
 // states. Never let an arbitrary in-memory caller fabricate this relation.
 let owners:Vec<V2ReviewState>=sources.iter().map(|item|
   load_v2(item.preclose_v2_file)
     .map_err(|_|PersistedFeeCensusErrorR211::Drift)
 ).collect::<Result<_,_>>()?;
 let linked:Vec<_>=sources.iter().zip(&owners).map(|(i,owner)|
   LocalArchivedCloseCandidate{
    preclose_v2:owner,archive_path:i.archive_path,
   }
 ).collect();
 let original_closes:Vec<_>=linked.iter().map(|c|
   read_local_close_source(c.archive_path,c.preclose_v2)
      .map_err(PersistedFeeCensusErrorR211::Archive)
 ).collect::<Result<_,_>>()?;
 let day=inspect_archived_okx_buy_sell_fee_day_r210(
   cfg,now_ms,account_owner,&linked,unresolved_orders
 ).map_err(PersistedFeeCensusErrorR211::Review)?;
 // Re-run the existing exact directory census and check per-archive source
 // consistency after computing totals. This is still NOT an atomic snapshot
 // or an external monotonic witness.
 let after=inspect_persisted_local_archive_directory_census(
   archive_directory,sources).map_err(PersistedFeeCensusErrorR211::Archive)?;
 if after!=census{return Err(PersistedFeeCensusErrorR211::Drift)}
 for (i,source) in sources.iter().enumerate(){
  let reread=load_v2(source.preclose_v2_file)
    .map_err(|_|PersistedFeeCensusErrorR211::Drift)?;
  if reread!=owners[i] ||
    read_local_close_source(source.archive_path,&reread)
     .map_err(PersistedFeeCensusErrorR211::Archive)?
      !=original_closes[i]{
   return Err(PersistedFeeCensusErrorR211::Drift)
  }
 }
 Ok(PersistedFeeCensusR211{
  local_archive_directory_matches_inputs:true,
  local_archives_checked:census.local_archives_checked,
  fee_day:day,
  account_wide_exchange_history_authenticated:false,
  complete_unresolved_owner_inventory_proven:false,
  day_fees_proven_complete:false,
  independently_verified_closed:false,
  may_trade_real:false,
 })
}

/// R136: LOCAL cross-store view; no authenticated shared account identity.
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum PendingDirectoryJoinErrorR136{
 Config,Native(crate::authoritative_state::NativeStateError),
 Directory(PersistedFeeCensusErrorR211),Drift,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct PendingDirectoryJoinR136{
 pub native_generation:u64,
 pub native_spot_pending:usize,
 pub native_futures_pending:usize,
 pub local_unknown_count:usize,
 pub directory:PersistedFeeCensusR211,
 pub shared_exchange_account_proven:bool,
 pub complete_account_fees_proven:bool,
 pub may_trade_real:bool,
}

/// Local UNKNOWN classification shared by R136 and its R137 regression.
/// Futures unresolved identity is not an OKX client-order ID.
fn classify_native_unknown_r137(
 pending:&crate::authoritative_state::NativePendingEntryQuery,
 other_okx_unknown:&[String],
)->Result<(Vec<String>,usize,usize),PendingDirectoryJoinErrorR136>{
 use crate::authoritative_state::NativeEntryLeg;
 use std::collections::BTreeSet;
 if pending.client_id.is_empty(){return Err(PendingDirectoryJoinErrorR136::Drift)}
 let mut ids=BTreeSet::new();
 for id in other_okx_unknown{
   if !ids.insert(id.clone()){return Err(PendingDirectoryJoinErrorR136::Drift)}
 }
 let (spot,futures)=match pending.leg{
   NativeEntryLeg::Spot=>{
      if !ids.insert(format!("client:{}",pending.client_id)){
        return Err(PendingDirectoryJoinErrorR136::Drift)
      }
      (1,0)
   },
   NativeEntryLeg::Futures=>(0,1),
 };
 Ok((ids.into_iter().collect(),spot,futures))
}
/// Never interpret local completeness of R211 as an account-wide snapshot.
/// Futures UNKNOWN stays a separate risk barrier, not an OKX fee owner.
pub fn inspect_native_pending_directory_r136(
 cfg:&NativeTradingConfig,now_ms:u64,account_owner:&str,
 directory:&Path,sources:&[PersistedLocalArchivedCloseCandidate<'_>],
 native:&crate::authoritative_state::NativeStateStore,
 expected_generation:u64,other_okx_unknown:&[String],
)->Result<PendingDirectoryJoinR136,PendingDirectoryJoinErrorR136>{
 if !cfg.source_projection_unchanged()
    ||cfg.policy.real_execution_authorized||cfg.policy.real_daily_trades!=0
    ||cfg.raw["real_trial"]["deny_on_unknown"].as_bool()!=Some(true){
   return Err(PendingDirectoryJoinErrorR136::Config)
 }
 let before=native.load().map_err(PendingDirectoryJoinErrorR136::Native)?
    .ok_or(PendingDirectoryJoinErrorR136::Drift)?;
 let pending=native.pending_native_entry_query(expected_generation)
    .map_err(PendingDirectoryJoinErrorR136::Native)?;
 if before.generation!=expected_generation
    ||pending.created_at_ms==0||pending.created_at_ms>now_ms
    ||before.entry.as_ref().is_some_and(|e|
       e.policy_binding.as_ref().is_some_and(|b|b.config_sha256!=cfg.sha256)){
    return Err(PendingDirectoryJoinErrorR136::Drift)
 }
 let (unknowns,spot,futures)=
   classify_native_unknown_r137(&pending,other_okx_unknown)?;
 let directory_result=inspect_persisted_okx_fee_directory_r211(
    cfg,now_ms,account_owner,directory,sources,&unknowns)
    .map_err(PendingDirectoryJoinErrorR136::Directory)?;
 let after=native.load().map_err(PendingDirectoryJoinErrorR136::Native)?
    .ok_or(PendingDirectoryJoinErrorR136::Drift)?;
 let pending_after=native.pending_native_entry_query(expected_generation)
    .map_err(PendingDirectoryJoinErrorR136::Native)?;
 if after!=before||pending_after!=pending
    ||directory_result.fee_day.quote_fee_day.open_or_unknown_order_count!=unknowns.len()
    ||directory_result.may_trade_real
    ||directory_result.day_fees_proven_complete{
   return Err(PendingDirectoryJoinErrorR136::Drift)
 }
 Ok(PendingDirectoryJoinR136{
   native_generation:expected_generation,
   native_spot_pending:spot,native_futures_pending:futures,
   local_unknown_count:unknowns.len(),directory:directory_result,
   shared_exchange_account_proven:false,
   complete_account_fees_proven:false,may_trade_real:false,
 })
}

#[cfg(test)]
mod r137_classifier_tests{
 use super::*;
 use crate::authoritative_state::{NativeEntryLeg,NativePendingEntryQuery};
 fn fixture(leg:NativeEntryLeg)->NativePendingEntryQuery{
   NativePendingEntryQuery{
     generation:5,leg,symbol:"BNTUSDT".into(),
     created_at_ms:1_780_000_000_000,
     spot_symbol:"BNT/USDT".into(),futures_symbol:"BNTUSDT".into(),
     base:"BNT".into(),intent_id:"future-intent".into(),
     client_id:"FUTURES_CLIENT".into(),
     requested_atoms_e18:100_000_000_000_000_000,
     spot_baseline_atoms_e18:0,
     confirmed_spot_atoms_e18:100_000_000_000_000_000,
     confirmed_futures_atoms_e18:0,
   }
 }
 #[test]
 fn r137_futures_unknown_not_forged_as_okx_fee_order(){
   let (ids,spot,futures)=classify_native_unknown_r137(
      &fixture(NativeEntryLeg::Futures),&[]).unwrap();
   assert!(ids.is_empty());assert_eq!((spot,futures),(0,1));
   let (ids,spot,futures)=classify_native_unknown_r137(
      &fixture(NativeEntryLeg::Futures),
      &["client:EXISTING_OKX".to_string()]).unwrap();
   assert_eq!(ids,vec!["client:EXISTING_OKX".to_string()]);
   assert_eq!((spot,futures),(0,1));
 }
 #[test]
 fn r137_native_spot_client_collision_denied(){
   let pending=fixture(NativeEntryLeg::Spot);
   let (ids,spot,futures)=classify_native_unknown_r137(&pending,&[]).unwrap();
   assert_eq!((spot,futures),(1,0));
   assert_eq!(ids,vec!["client:FUTURES_CLIENT".to_string()]);
   assert_eq!(classify_native_unknown_r137(
      &pending,&ids).err(),Some(PendingDirectoryJoinErrorR136::Drift));
   assert_eq!(classify_native_unknown_r137(
      &pending,&[ids[0].clone(),ids[0].clone()]).err(),
      Some(PendingDirectoryJoinErrorR136::Drift));
 }
}
