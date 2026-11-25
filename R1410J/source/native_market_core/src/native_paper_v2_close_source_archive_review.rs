//! Astra P0-02 local MOCK/source-shaped close evidence, immutable single-file
//! write, durable fsync and cold readback. Not authenticated exchange history,
//! not independent antirollback, NOT real trading or VerifiedClosed.
use crate::{
    native_trade2f3_v2_versioned_store::{load_v2,V2ReviewState,V2ReviewPhase},
    native_trade2f3_v2_revision_journal::{journal_path,read_local_journal,review_disk_local_journal,V2CallerHeldFloor},
    native_trade2f3_v2_four_leg_close_review::{
        V2FourLegCloseObservation,inspect_v2_four_leg_close,
    },
};
use openssl::sha::sha256;
use serde::{Deserialize,Serialize};
use std::{collections::{BTreeMap,BTreeSet},fs::{self,File,OpenOptions},
    io::{self,Read,Write},path::{Path,PathBuf}};
const MAX_ARCHIVE:u64=512*1024;
const DOMAIN:&str="SPOTARB/P0-02/LOCAL-FOUR-LEG-SOURCE/V1";
#[derive(Debug,Clone,PartialEq,Eq)]
pub enum ArchiveError{
  Io(io::ErrorKind),Invalid(&'static str),Encoding,Integrity,WrongSource,
}
impl From<io::Error> for ArchiveError{
 fn from(x:io::Error)->Self{Self::Io(x.kind())}
}
fn deny(s:&'static str)->ArchiveError{ArchiveError::Invalid(s)}
fn hex(b:&[u8])->String{sha256(b).iter().map(|x|format!("{x:02x}")).collect()}
fn owner_hash(v2:&V2ReviewState)->Result<String,ArchiveError>{
 v2.validate().map_err(|_|deny("invalid V2 original owner"))?;
 if v2.revision!=4||v2.phase!=V2ReviewPhase::FuturesEvidenceRecorded||
    v2.production_authorized||v2.may_send_or_cancel_or_verify_closed{
    return Err(deny("source archive requires immutable V2 rev4 pre-close"))
 }
 let bytes=serde_json::to_vec(v2).map_err(|_|ArchiveError::Encoding)?;
 Ok(hex(&bytes))
}
#[derive(Debug,Clone,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
struct ArchiveRecord{
 domain:String,
 owner_revision:u64,
 original_owner_generation:u64,
 owner_sha256:String,
 close:V2FourLegCloseObservation,
 close_sha256:String,
 local_archive_only:bool,
 source_exchange_authenticated:bool,
 external_floor_authenticated:bool,
 verified_closed:bool,
 real_order_authorized:bool,
 digest_sha256:String,
}
fn calc(rec:&ArchiveRecord)->Result<String,ArchiveError>{
 let b=serde_json::to_vec(&(
    DOMAIN,rec.owner_revision,rec.original_owner_generation,
    &rec.owner_sha256,&rec.close_sha256,
    rec.local_archive_only,rec.source_exchange_authenticated,
    rec.external_floor_authenticated,rec.verified_closed,
    rec.real_order_authorized
 )).map_err(|_|ArchiveError::Encoding)?;
 Ok(hex(&b))
}
fn validate(rec:&ArchiveRecord,v2:&V2ReviewState)->Result<(),ArchiveError>{
 let vhash=owner_hash(v2)?;
 if rec.domain!=DOMAIN||rec.owner_revision!=4||
    rec.original_owner_generation!=v2.genesis.original_owner_generation||
    rec.owner_sha256!=vhash||
    rec.source_exchange_authenticated||rec.external_floor_authenticated||
    rec.verified_closed||rec.real_order_authorized||
    !rec.local_archive_only||rec.digest_sha256!=calc(rec)?{
    return Err(ArchiveError::WrongSource)
 }
 let bytes=serde_json::to_vec(&rec.close)
     .map_err(|_|ArchiveError::Encoding)?;
 if rec.close_sha256!=hex(&bytes){
    return Err(ArchiveError::Integrity)
 }
 inspect_v2_four_leg_close(v2,&rec.close)
    .map_err(|_|deny("wrong original/close source IDs, fees or quantities"))?;
 Ok(())
}
fn archive_bytes(path:&Path)->Result<Vec<u8>,ArchiveError>{
 let meta=fs::symlink_metadata(path)?;
 if !meta.file_type().is_file()||meta.len()==0||meta.len()>MAX_ARCHIVE{
     return Err(deny("missing, symlinked or oversized source archive"))
 }
 let mut bytes=Vec::new();
 File::open(path)?.take(MAX_ARCHIVE+1).read_to_end(&mut bytes)?;
 if bytes.is_empty()||bytes.len() as u64>MAX_ARCHIVE{
     return Err(deny("source archive read limit exceeded"))
 }
 Ok(bytes)
}
/// Strictly read one previously fsynced source archive (not a trading permit).
pub fn read_local_close_source(
    path:&Path,v2:&V2ReviewState,
)->Result<V2FourLegCloseObservation,ArchiveError>{
 let bytes=archive_bytes(path)?;
 let rec:ArchiveRecord=serde_json::from_slice(&bytes)
    .map_err(|_|ArchiveError::Encoding)?;
 validate(&rec,v2)?;
 Ok(rec.close)
}
/// An explicitly supplied, already durable ORIGINAL local V2 review close
/// archive paired with its revision-4 owner. No private calls or file writes.
pub struct LocalArchivedCloseCandidate<'a>{
 pub preclose_v2:&'a V2ReviewState,
 pub archive_path:&'a Path,
}
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct LocalArchivedCloseCensus{
 /// LOCAL set only, no proof that omitted owners do not exist.
 pub claimed_owner_generations:Vec<u64>,
 pub local_archives_checked:usize,
 pub exchange_account_authenticated:bool,
 pub complete_owner_universe_proven:bool,
 pub independently_verified_closed:bool,
 pub may_trade_real:bool,
}
// Local-only source key: venue + kind + symbol + original exchange-shaped ID.
// This is a conservative duplicate guard WITHIN ONE CALLER-SUPPLIED SERIES,
// not an account-identity check. Different actual accounts may reuse IDs.
type LocalSourceIdentity=(&'static str,&'static str,String,String);
fn local_id(set:&mut BTreeSet<LocalSourceIdentity>,venue:&'static str,
 kind:&'static str,symbol:&str,id:&str)->Result<(),ArchiveError>{
 if symbol.is_empty()||id.is_empty(){return Err(deny("empty local original source identity"))}
 set.insert((venue,kind,symbol.to_owned(),id.to_owned()));
 Ok(())
}
fn local_four_leg_ids(owner:&V2ReviewState,close:&V2FourLegCloseObservation)
 ->Result<BTreeSet<LocalSourceIdentity>,ArchiveError>{
 let mut ids=BTreeSet::new();
 let opening=&owner.genesis.checkpoint;
 let spot=&opening.plan.spot_symbol;
 let futures=owner.c3_futures.as_ref().ok_or(deny("original futures source absent"))?;
 local_id(&mut ids,"OKX","ORDER",spot,&opening.original.spot_order_id)?;
 for row in &opening.source_shaped_fee_fills{
   local_id(&mut ids,"OKX","TRADE",&row.inst_id,&row.trade_id)?;
 }
 for bill in &owner.genesis.original_okx_base_bill_window.source_shaped_base_bills_desc{
   local_id(&mut ids,"OKX","BILL",&bill.inst_id,&bill.bill_id)?;
 }
 for id in &opening.original.fill_bill_ids{
   local_id(&mut ids,"OKX","BILL",spot,id)?;
 }
 let future_symbol=&futures.futures_symbol;
 if let Some(order)=&futures.order_id{
   local_id(&mut ids,"BINANCE","ORDER",future_symbol,order)?;
 }
 for row in &futures.rows{
   local_id(&mut ids,"BINANCE","TRADE",&row.symbol,&row.trade_id)?;
 }
 local_id(&mut ids,"BINANCE","ORDER",future_symbol,&close.futures_cover_order_id)?;
 local_id(&mut ids,"OKX","ORDER",spot,&close.spot_sell_order_id)?;
 for row in &close.futures_cover_rows{
   local_id(&mut ids,"BINANCE","TRADE",&row.symbol,&row.trade_id)?;
 }
 for row in &close.spot_sell_rows{
   local_id(&mut ids,"OKX","TRADE",&row.symbol,&row.trade_id)?;
   local_id(&mut ids,"OKX","BILL",&row.symbol,&row.bill_id)?;
 }
 Ok(ids)
}
fn record_distinct_cycle_ids(
 seen:&mut BTreeMap<LocalSourceIdentity,u64>,generation:u64,
 cycle_ids:BTreeSet<LocalSourceIdentity>,
)->Result<(),ArchiveError>{
 for key in cycle_ids{
   if let Some(previous_owner)=seen.insert(key,generation){
     if previous_owner!=generation{
       return Err(deny("original venue source identity reused across V2 owners"))
     }
   }
   if seen.len()>200_000{
     return Err(deny("local source identity census exceeds bounded budget"))
   }
 }
 Ok(())
}
/// Cold-read and validate every separately provided source-shaped archive.
/// Missing/omitted archive paths cannot be detected by THIS function; a
/// separate independently grounded enumeration boundary is still required.
/// Never turn PUBLIC PAPER journals or V2 mock source into real fills.
pub fn inspect_supplied_local_archived_close_census(
 inputs:&[LocalArchivedCloseCandidate<'_>]
)->Result<LocalArchivedCloseCensus,ArchiveError>{
 if inputs.is_empty()||inputs.len()>20_000{
    return Err(deny("empty or unbounded local source archive census"))
 }
 let mut generations=BTreeSet::new();
 let mut paths=BTreeSet::new();
 let mut original_ids=BTreeMap::new();
 for item in inputs{
    if !paths.insert(item.archive_path){
       return Err(deny("reused local close archive path"))
    }
    // Existing typed reader verifies local SHA, V2 revision 4 owner binding,
    // exact closing source shapes and NO exchange/VerifiedClosed attestation.
    let close=read_local_close_source(item.archive_path,item.preclose_v2)?;
    let generation=item.preclose_v2.genesis.original_owner_generation;
    if generation==0||close.source_original_generation!=generation||
       !generations.insert(generation){
       return Err(deny("duplicate or mismatched local Native owner"))
    }
    let ids=local_four_leg_ids(item.preclose_v2,&close)?;
    record_distinct_cycle_ids(&mut original_ids,generation,ids)?;
 }
 Ok(LocalArchivedCloseCensus{
    claimed_owner_generations:generations.into_iter().collect(),
    local_archives_checked:inputs.len(),
    exchange_account_authenticated:false,
    complete_owner_universe_proven:false,
    independently_verified_closed:false,
    may_trade_real:false,
 })
}
/// Strictly compare supplied local V2 archive candidates against every
/// directory entry in ONE explicitly designated archive-only directory.
/// Detects omissions WITHIN that directory, not omitted directories/periods,
/// nonpersisted closes, rollbacks or fabricated original exchange records.
pub fn inspect_complete_local_archive_directory_census(
 directory:&Path,inputs:&[LocalArchivedCloseCandidate<'_>],
)->Result<LocalArchivedCloseCensus,ArchiveError>{
 if !directory.is_absolute()||!fs::symlink_metadata(directory)?.file_type().is_dir(){
    return Err(deny("local archive census root must be an absolute real directory"))
 }
 if inputs.is_empty()||inputs.len()>20_000{
    return Err(deny("empty or unbounded archive-only directory census"))
 }
 let mut supplied=BTreeSet::<PathBuf>::new();
 for item in inputs{
    if item.archive_path.parent()!=Some(directory)||
       item.archive_path.extension().and_then(|v|v.to_str())!=Some("json")||
       !supplied.insert(item.archive_path.to_path_buf()){
       return Err(deny("archive candidate outside dedicated directory or duplicated"))
    }
 }
 let mut found=BTreeSet::<PathBuf>::new();
 for entry in fs::read_dir(directory)?{
    let entry=entry?;
    if !entry.file_type()?.is_file()||
       entry.path().extension().and_then(|v|v.to_str())!=Some("json")||
       !found.insert(entry.path())||
       found.len()>20_000{
       return Err(deny("archive directory has non-archive or too many entries"))
    }
 }
 if found!=supplied{
    return Err(deny("local archive directory differs from supplied owner list"))
 }
 // Keep all validation centralized in the existing strict cold reader.
 // This is a local filesystem comparison only, never an authenticated
 // exchange-wide owner census or an external rollback witness.
 inspect_supplied_local_archived_close_census(inputs)
}
/// Use actual persisted, cold-loaded V2 owners rather than accepting an
/// arbitrary in-memory revision-4 snapshot as the basis of a local archive
/// census. Both the owner file and its archive remain LOCAL, UNAUTHENTICATED.
pub struct PersistedLocalArchivedCloseCandidate<'a>{
 pub preclose_v2_file:&'a Path,
 pub archive_path:&'a Path,
}
pub fn inspect_persisted_local_archive_directory_census(
 directory:&Path,inputs:&[PersistedLocalArchivedCloseCandidate<'_>],
)->Result<LocalArchivedCloseCensus,ArchiveError>{
 if inputs.is_empty()||inputs.len()>20_000{
    return Err(deny("no bounded persisted V2 owner inputs"))
 }
 let mut source_paths=BTreeSet::new();
 let mut owners=Vec::with_capacity(inputs.len());
 for item in inputs{
    let path=item.preclose_v2_file;
    if !path.is_absolute()||path==item.archive_path||
       path.parent()==Some(directory)||
       !fs::symlink_metadata(path)?.file_type().is_file()||
       !source_paths.insert(path){
       return Err(deny("not a unique cold regular V2 owner source"))
    }
    let v2=load_v2(path).map_err(|_|deny("invalid persisted V2 preclose owner"))?;
    owners.push(v2);
 }
 let linked:Vec<_>=inputs.iter().zip(owners.iter()).map(|(row,owner)|
    LocalArchivedCloseCandidate{preclose_v2:owner,archive_path:row.archive_path}
 ).collect();
 let candidate=inspect_complete_local_archive_directory_census(directory,&linked)?;
 // Fail if any original V2 owner file changed during this local scan;
 // still NOT an atomic snapshot or rollback-proof trusted catalogue.
 for (row,before) in inputs.iter().zip(owners.iter()){
    let after=load_v2(row.preclose_v2_file)
       .map_err(|_|deny("V2 owner missing or corrupt on cold re-read"))?;
    if &after!=before{return Err(deny("V2 owner changed during archive census"))}
 }
 Ok(candidate)
}
#[cfg(test)]
mod local_source_id_tests{
 use super::*;
 fn one(venue:&'static str,kind:&'static str,symbol:&str,id:&str)
   ->BTreeSet<LocalSourceIdentity>{
   let mut s=BTreeSet::new();
   local_id(&mut s,venue,kind,symbol,id).unwrap();
   s
 }
 #[test]
 fn different_owner_same_original_trade_denied_but_distinct_venue_or_symbol_allowed(){
   let mut seen=BTreeMap::new();
   record_distinct_cycle_ids(&mut seen,11,
      one("OKX","TRADE","BNT-USDT","9001")).unwrap();
   // Original fill ID is NOT minted a second time by generation 12.
   assert!(record_distinct_cycle_ids(&mut seen,12,
      one("OKX","TRADE","BNT-USDT","9001")).is_err());
   // These are different source namespaces, not recycled OKX fills.
   record_distinct_cycle_ids(&mut seen,12,
      one("BINANCE","TRADE","BNT-USDT","9001")).unwrap();
   record_distinct_cycle_ids(&mut seen,13,
      one("OKX","TRADE","ONE-USDT","9001")).unwrap();
 }
 #[test]
 fn different_owner_reused_original_bill_or_order_is_denied_locally(){
   let mut seen=BTreeMap::new();
   record_distinct_cycle_ids(&mut seen,7,
      one("OKX","BILL","BNT-USDT","301")).unwrap();
   assert!(record_distinct_cycle_ids(&mut seen,8,
      one("OKX","BILL","BNT-USDT","301")).is_err());
   let mut seen=BTreeMap::new();
   record_distinct_cycle_ids(&mut seen,7,
      one("BINANCE","ORDER","BNTUSDT","779")).unwrap();
   assert!(record_distinct_cycle_ids(&mut seen,8,
      one("BINANCE","ORDER","BNTUSDT","779")).is_err());
   // A single trade may be referenced by both a fill and its own bill;
   // per-cycle BTreeSet normalization avoids false repeat denial.
   let mut within=BTreeSet::new();
   local_id(&mut within,"OKX","TRADE","BNT-USDT","9001").unwrap();
   local_id(&mut within,"OKX","TRADE","BNT-USDT","9001").unwrap();
   assert_eq!(within.len(),1);
 }
}
/// Exhausts one designated LOCAL directory of persisted Native V2 owners.
/// The directory itself and its completeness are NOT externally witnessed.
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct LocalV2LifecycleInventory{
 pub locally_reviewed_four_leg_generations:Vec<u64>,
 pub unresolved_owner_generations:Vec<u64>,
 pub examined_owner_files:usize,
 pub source_account_authenticated:bool,
 pub actual_closed_trades_proven:bool,
 pub full_owner_history_proven:bool,
 pub may_trade_real:bool,
}
/// All .json entries in a strictly dedicated directory must load as typed
/// Native V2 state. No caller-nominated owner subset is silently accepted.
pub fn scan_local_v2_owner_directory(
 directory:&Path,
)->Result<LocalV2LifecycleInventory,ArchiveError>{
 if !directory.is_absolute()||
   !fs::symlink_metadata(directory)?.file_type().is_dir(){
   return Err(deny("V2 inventory root must be a real absolute directory"))
 }
 let mut paths=BTreeSet::new();
 let mut owners=Vec::new();
 for item in fs::read_dir(directory)?{
   let item=item?;
   if !item.file_type()?.is_file()||
      item.path().extension().and_then(|x|x.to_str())!=Some("json")||
      !paths.insert(item.path())||paths.len()>20_000{
      return Err(deny("unexpected V2 owner inventory entry"))
   }
   let state=load_v2(&item.path()).map_err(|_|deny("invalid local V2 owner"))?;
   owners.push((item.path(),state));
 }
 if owners.is_empty(){return Err(deny("empty local V2 owner inventory"))}
 let mut generations=BTreeSet::new();
 let mut local_closed=BTreeSet::new();
 let mut unresolved=BTreeSet::new();
 for (path,state) in &owners{
   let g=state.genesis.original_owner_generation;
   if g==0||!generations.insert(g){
     return Err(deny("duplicate or zero local V2 owner generation"))
   }
   if state.phase==V2ReviewPhase::FourLegCloseReviewed &&
       state.close_four_leg.is_some(){
     local_closed.insert(g);
   }else{
     unresolved.insert(g);
   }
   let again=load_v2(path).map_err(|_|deny("V2 owner changed during scan"))?;
   if &again!=state{return Err(deny("V2 owner changed during scan"))}
 }
 let mut final_paths=BTreeSet::new();
 for item in fs::read_dir(directory)?{
   let item=item?;
   if !item.file_type()?.is_file()||!final_paths.insert(item.path()){
     return Err(deny("V2 inventory changed during scan"))
   }
 }
 if final_paths!=paths{return Err(deny("V2 inventory changed during scan"))}
 Ok(LocalV2LifecycleInventory{
   locally_reviewed_four_leg_generations:local_closed.into_iter().collect(),
   unresolved_owner_generations:unresolved.into_iter().collect(),
   examined_owner_files:owners.len(),
   source_account_authenticated:false,
   actual_closed_trades_proven:false,
   full_owner_history_proven:false,
   may_trade_real:false,
 })
}
/// Separate strict layout: one .json V2 owner and one matching sibling
/// .v2_revision_journal per owner. No orphan or missing revision journals.
/// Both files remain same-host LOCAL evidence (rollback together possible).
pub fn scan_local_v2_owner_journal_pairs(
 directory:&Path,
)->Result<LocalV2LifecycleInventory,ArchiveError>{
 if !directory.is_absolute()||
    !fs::symlink_metadata(directory)?.file_type().is_dir(){
    return Err(deny("V2 paired inventory root not a real absolute directory"))
 }
 let mut owner_paths=BTreeSet::new();
 let mut journal_paths=BTreeSet::new();
 for row in fs::read_dir(directory)?{
    let row=row?;
    if !row.file_type()?.is_file(){
      return Err(deny("nonregular file in paired owner inventory"))
    }
    let path=row.path();
    match path.extension().and_then(|s|s.to_str()){
      Some("json")=>{owner_paths.insert(path);},
      Some("v2_revision_journal")=>{journal_paths.insert(path);},
      _=>return Err(deny("foreign file in paired owner inventory")),
    }
    if owner_paths.len()>20_000||journal_paths.len()>20_000{
      return Err(deny("unbounded paired owner inventory"))
    }
 }
 if owner_paths.is_empty()||owner_paths.len()!=journal_paths.len(){
    return Err(deny("owner/journal population mismatch"))
 }
 for owner in &owner_paths{
    if !journal_paths.contains(&journal_path(owner)){
       return Err(deny("missing owner-matched revision journal"))
    }
 }
 let mut used_generations=BTreeSet::new();
 let mut local_closed=BTreeSet::new();
 let mut unresolved=BTreeSet::new();
 for owner_path in &owner_paths{
    let owner=load_v2(owner_path).map_err(|_|deny("invalid paired V2 owner"))?;
    let tip=review_disk_local_journal(owner_path,&owner,None)
      .map_err(|_|deny("owner/journal chain or tip mismatch"))?;
    if !tip.local_chain_consistent||tip.external_independence_authenticated||
       tip.may_send_retry_cancel_verify_closed{
       return Err(deny("invalid local journal authority"))
    }
    let g=owner.genesis.original_owner_generation;
    if g==0||!used_generations.insert(g){
       return Err(deny("duplicate paired Native owner generation"))
    }
    if owner.phase==V2ReviewPhase::FourLegCloseReviewed &&
       owner.close_four_leg.is_some(){
       local_closed.insert(g);
    }else{unresolved.insert(g);}
    let again=load_v2(owner_path).map_err(|_|deny("owner changed during paired scan"))?;
    if again!=owner{return Err(deny("owner changed during paired scan"))}
    let latest=review_disk_local_journal(owner_path,&again,None)
       .map_err(|_|deny("journal changed during paired scan"))?;
    if latest.current_entry_sha256!=tip.current_entry_sha256{
       return Err(deny("journal tip changed during paired scan"))
    }
 }
 let mut final_entries=BTreeSet::new();
 for row in fs::read_dir(directory)?{
    let row=row?;
    if !row.file_type()?.is_file()||!final_entries.insert(row.path()){
       return Err(deny("paired inventory changed during scan"))
    }
 }
 let expected:BTreeSet<_>=owner_paths.union(&journal_paths).cloned().collect();
 if final_entries!=expected{return Err(deny("paired inventory changed during scan"))}
 Ok(LocalV2LifecycleInventory{
   locally_reviewed_four_leg_generations:local_closed.into_iter().collect(),
   unresolved_owner_generations:unresolved.into_iter().collect(),
   examined_owner_files:owner_paths.len(),
   source_account_authenticated:false,
   actual_closed_trades_proven:false,
   full_owner_history_proven:false,
   may_trade_real:false,
 })
}
/// A separately preserved claimant's list of owner files + historical
/// revision-chain anchors. It is NOT authenticated or independently stored
/// by this offline reader. A trusted operator must pin this outside the VM.
pub struct ClaimedLocalOwnerAnchor<'a>{
 pub owner_filename:&'a str,
 pub floor:&'a V2CallerHeldFloor,
}
/// Cross-check every paired local owner against the EXACT caller-claimed
/// population and historical revision anchor. An owner+log rollback is
/// rejected only if its prior tip was genuinely held outside that rollback
/// domain. This function cannot verify where claims came from.
pub fn scan_paired_v2_against_claimed_owner_floors(
 directory:&Path,anchors:&[ClaimedLocalOwnerAnchor<'_>],
)->Result<LocalV2LifecycleInventory,ArchiveError>{
 let before=scan_local_v2_owner_journal_pairs(directory)?;
 if anchors.is_empty()||anchors.len()!=before.examined_owner_files{
   return Err(deny("claimed V2 owner population mismatch"))
 }
 let mut filenames=BTreeSet::new();
 let mut generations=BTreeSet::new();
 for claim in anchors{
   let name=claim.owner_filename;
   if name.len()<6||name.len()>128||!name.ends_with(".json")||
      name.starts_with('.')||name.contains("..")||
      !name.bytes().all(|b|b.is_ascii_alphanumeric()||
          b==b'_'||b==b'-'||b==b'.')||
      !filenames.insert(name)||
      !generations.insert(claim.floor.original_owner_generation){
      return Err(deny("invalid duplicate or unsafe claimed V2 owner anchor"))
   }
   let path=directory.join(name);
   if !fs::symlink_metadata(&path)?.file_type().is_file(){
     return Err(deny("missing or nonregular claimed V2 owner"))
   }
   let owner=load_v2(&path).map_err(|_|deny("invalid claimed V2 owner"))?;
   if owner.genesis.original_owner_generation!=claim.floor.original_owner_generation{
     return Err(deny("claimed owner generation differs from local file"))
   }
   let check=review_disk_local_journal(&path,&owner,Some(claim.floor))
      .map_err(|_|deny("V2 owner below or different from claimed historical floor"))?;
   if !check.supplied_floor_matches||check.external_independence_authenticated||
      check.may_send_retry_cancel_verify_closed{
     return Err(deny("invalid claimed owner floor review authority"))
   }
 }
 let after=scan_local_v2_owner_journal_pairs(directory)?;
 if before!=after{return Err(deny("paired owner inventory changed during review"))}
 Ok(before)
}
/// Portable JSON snapshot for an operator to preserve OUTSIDE this VM.
/// SHA-256 is a self-integrity check only. Authenticity requires the
/// expected digest to be retained independently and supplied later.
const FLOOR_MANIFEST_DOMAIN:&str="SPOTARB/LOCAL/V2-OWNER-FLOORS/EXPORT/V1";
#[derive(Debug,Clone,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortableOwnerFloor{
 pub owner_filename:String,
 pub floor:V2CallerHeldFloor,
}
#[derive(Debug,Clone,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PortableOwnerFloorManifest{
 pub schema_version:u32,
 pub domain:String,
 pub owners:Vec<PortableOwnerFloor>,
 pub manifest_sha256:String,
}
fn portable_manifest_hash(m:&PortableOwnerFloorManifest)->Result<String,ArchiveError>{
 let bytes=serde_json::to_vec(&(
   m.schema_version,&m.domain,&m.owners
 )).map_err(|_|ArchiveError::Encoding)?;
 Ok(hex(&bytes))
}
/// Construct a portable floor manifest from a locally checked directory.
/// It gains independence ONLY if the operator keeps a copy elsewhere.
pub fn export_local_v2_floor_manifest(
 directory:&Path
)->Result<PortableOwnerFloorManifest,ArchiveError>{
 let before=scan_local_v2_owner_journal_pairs(directory)?;
 let mut owners=Vec::with_capacity(before.examined_owner_files);
 for entry in fs::read_dir(directory)?{
   let path=entry?.path();
   if path.extension().and_then(|e|e.to_str())!=Some("json"){continue}
   let name=path.file_name().and_then(|s|s.to_str())
      .ok_or(deny("non-UTF8 local owner path"))?;
   let tip=read_local_journal(&path)
      .map_err(|_|deny("invalid V2 floor journal"))?
      .pop().ok_or(deny("missing V2 journal tip"))?;
   owners.push(PortableOwnerFloor{
     owner_filename:name.to_owned(),
     floor:V2CallerHeldFloor{
        original_owner_generation:tip.original_owner_generation,
        minimum_revision:tip.revision,
        exact_entry_sha256:tip.entry_sha256,
     },
   });
 }
 owners.sort_by(|a,b|a.owner_filename.cmp(&b.owner_filename));
 if owners.len()!=before.examined_owner_files ||
    scan_local_v2_owner_journal_pairs(directory)?!=before{
    return Err(deny("V2 floor export inventory changed"))
 }
 let mut m=PortableOwnerFloorManifest{
    schema_version:1,domain:FLOOR_MANIFEST_DOMAIN.to_owned(),
    owners,manifest_sha256:String::new(),
 };
 m.manifest_sha256=portable_manifest_hash(&m)?;
 Ok(m)
}
/// Verify against the independently supplied EXPECTED digest of one
/// operator-retained manifest; no same-host file is treated as a trust pin.
pub fn verify_local_v2_portable_floor_manifest(
 directory:&Path,m:&PortableOwnerFloorManifest,
 externally_supplied_pin:&str,
)->Result<LocalV2LifecycleInventory,ArchiveError>{
 if m.schema_version!=1||m.domain!=FLOOR_MANIFEST_DOMAIN||
    m.owners.is_empty()||m.owners.len()>20_000||
    externally_supplied_pin.len()!=64||
    !externally_supplied_pin.bytes().all(|b|b.is_ascii_hexdigit()&&
       !b.is_ascii_uppercase()){
    return Err(deny("invalid untrusted portable manifest or missing pin"))
 }
 if m.manifest_sha256!=externally_supplied_pin||
    portable_manifest_hash(m)?!=externally_supplied_pin{
    return Err(deny("portable manifest differs from supplied independent pin"))
 }
 if m.owners.windows(2).any(|pair|
     pair[0].owner_filename>=pair[1].owner_filename){
    return Err(deny("portable manifest owner list is not strictly ordered"))
 }
 let anchors:Vec<_>=m.owners.iter().map(|entry|ClaimedLocalOwnerAnchor{
   owner_filename:&entry.owner_filename,
   floor:&entry.floor,
 }).collect();
 scan_paired_v2_against_claimed_owner_floors(directory,&anchors)
}
/// Review whether a newer local manifest preserves EVERY owner and
/// historical anchor in a separately held prior manifest. Both supplied
/// pins are caller-provided; this cannot establish off-host custody.
pub fn inspect_portable_v2_floor_successor(
 directory:&Path,
 previous:&PortableOwnerFloorManifest,previous_pin:&str,
 candidate:&PortableOwnerFloorManifest,candidate_pin:&str,
)->Result<LocalV2LifecycleInventory,ArchiveError>{
 let current=verify_local_v2_portable_floor_manifest(
    directory,candidate,candidate_pin)?;
 if previous.schema_version!=1||
    previous.domain!=FLOOR_MANIFEST_DOMAIN||
    previous.owners.is_empty()||
    previous.owners.len()>candidate.owners.len()||
    previous_pin.len()!=64||
    !previous_pin.bytes().all(|b|b.is_ascii_hexdigit()&&!b.is_ascii_uppercase())||
    previous.manifest_sha256!=previous_pin||
    portable_manifest_hash(previous)?!=previous_pin||
    previous.owners.windows(2).any(|w|
      w[0].owner_filename>=w[1].owner_filename){
    return Err(deny("previous floor pin/population invalid"))
 }
 let mut advanced=candidate.owners.len()>previous.owners.len();
 let mut seen_generations=BTreeSet::new();
 for old in &previous.owners{
   if !seen_generations.insert(old.floor.original_owner_generation){
      return Err(deny("duplicate generation in previous floor"))
   }
   let ix=candidate.owners.binary_search_by(|row|
       row.owner_filename.cmp(&old.owner_filename))
       .map_err(|_|deny("previously pinned owner disappeared"))?;
   let new=&candidate.owners[ix];
   if new.floor.original_owner_generation!=old.floor.original_owner_generation||
      new.floor.minimum_revision<old.floor.minimum_revision{
      return Err(deny("owner scope changed or revision retreated"))
   }
   if new.floor.minimum_revision==old.floor.minimum_revision &&
      new.floor.exact_entry_sha256!=old.floor.exact_entry_sha256{
      return Err(deny("historical revision SHA changed"))
   }
   if new.floor.minimum_revision>old.floor.minimum_revision{
      advanced=true;
      let path=directory.join(&old.owner_filename);
      let owner=load_v2(&path).map_err(|_|deny("new owner inaccessible"))?;
      let proof=review_disk_local_journal(&path,&owner,Some(&old.floor))
          .map_err(|_|deny("old journal ancestry not preserved"))?;
      if !proof.supplied_floor_matches{
          return Err(deny("earlier journal SHA not preserved"))
      }
   }
 }
 if !advanced{
   return Err(deny("candidate is unchanged; no monotonic advancement"))
 }
 if verify_local_v2_portable_floor_manifest(
    directory,candidate,candidate_pin)?!=current{
    return Err(deny("local successor inventory changed during review"))
 }
 Ok(current)
}
#[derive(Debug,Clone,Copy,PartialEq,Eq)]
pub enum LocalArchiveFailpoint{None,AfterFileSyncBeforeDirectorySync}
/// Exactly-once create-new archive. A late error may mean bytes ARE durable:
/// caller must cold-inspect before retry; never remove and overwrite by guess.
/// `fp` exists exclusively for offline fault-injection reviews.
pub fn write_local_close_source_once(
  path:&Path,v2:&V2ReviewState,close:&V2FourLegCloseObservation,
  fp:LocalArchiveFailpoint,
)->Result<String,ArchiveError>{
 let vhash=owner_hash(v2)?;
 inspect_v2_four_leg_close(v2,close)
     .map_err(|_|deny("invalid closing four-leg local source"))?;
 let close_sha256=hex(&serde_json::to_vec(close)
     .map_err(|_|ArchiveError::Encoding)?);
 let mut rec=ArchiveRecord{
    domain:DOMAIN.into(),owner_revision:4,
    original_owner_generation:v2.genesis.original_owner_generation,
    owner_sha256:vhash,close:close.clone(),close_sha256,
    local_archive_only:true,source_exchange_authenticated:false,
    external_floor_authenticated:false,verified_closed:false,
    real_order_authorized:false,digest_sha256:String::new(),
 };
 rec.digest_sha256=calc(&rec)?;
 let bytes=serde_json::to_vec(&rec).map_err(|_|ArchiveError::Encoding)?;
 if bytes.is_empty()||bytes.len() as u64>MAX_ARCHIVE{
    return Err(deny("closing-source archive over bounded limit"))
 }
 let dir=path.parent().ok_or(deny("missing source archive directory"))?;
 if !fs::symlink_metadata(dir)?.file_type().is_dir(){
    return Err(deny("source archive directory is not regular"))
 }
 let mut file=OpenOptions::new().write(true).create_new(true).open(path)?;
 file.write_all(&bytes)?;
 file.sync_all()?;
 if fp==LocalArchiveFailpoint::AfterFileSyncBeforeDirectorySync{
    return Err(deny("injected AFTER file fsync, before directory fsync"))
 }
 File::open(dir)?.sync_all()?;
 let cold=read_local_close_source(path,v2)?;
 if cold!=*close{return Err(ArchiveError::Integrity)}
 Ok(rec.digest_sha256)
}
