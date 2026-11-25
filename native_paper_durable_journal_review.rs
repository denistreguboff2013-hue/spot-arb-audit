//! Isolated offline paper-state recovery review.
//! No exchange access and no automatic restart of trading.
use crate::native_paper_partial_recovery_gate::PaperPartialGate;
use serde::{Serialize,Deserialize};
use std::collections::BTreeMap;
use crate::native_paper_portfolio_review::{PaperPortfolio,MAX_CONCURRENT_PAPER,MAX_DAILY_PAPER};
use crate::native_paper_market_execution_review::PaperPhase;
use openssl::sha::sha256;
use serde_json::{Value,json};
use std::{fs::{self,File,OpenOptions},io::{self,Read,Write},path::Path};
#[derive(Debug,Clone,PartialEq,Eq)]
pub enum JournalError{Io(io::ErrorKind),Invalid(&'static str)}
impl From<io::Error> for JournalError{
 fn from(e:io::Error)->Self{Self::Io(e.kind())}
}
fn deny(reason:&'static str)->JournalError{JournalError::Invalid(reason)}
fn hash(data:&[u8])->String{
 sha256(data).iter().map(|v|format!("{v:02x}")).collect()
}
const ZERO:&str="0000000000000000000000000000000000000000000000000000000000000000";
#[derive(Clone,Debug,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReviewPosition{
 pub base:String,pub phase:String,pub paper_details:Value,
 pub fills:PaperPartialGate,pub quarantine:bool,
}
#[derive(Clone,Debug,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReviewSnapshot{
 pub schema:u32,pub day_yek:i64,pub started_wall_ms:i64,
 pub entries:u64,pub closed:u64,pub modeled_pnl_usdt:f64,
 pub positions:BTreeMap<String,ReviewPosition>,
 pub real_trading_authorized:bool,
}
impl ReviewSnapshot{
 pub fn from_paper(p:&PaperPortfolio,gates:&BTreeMap<String,PaperPartialGate>)
   ->Result<Self,JournalError>{
   if p.positions.len()!=gates.len(){return Err(deny("missing per-position fill evidence"))}
   let mut positions=BTreeMap::new();
   for (base,machine) in &p.positions{
     let fills=gates.get(base).ok_or(deny("missing fill gate"))?;
     let (phase,details)=match &machine.phase{
       PaperPhase::AwaitingHedge(pos)=>("AWAITING_HEDGE",json!(pos)),
       PaperPhase::Hedged(pos)=>("HEDGED",json!(pos)),
       PaperPhase::AwaitingSpotExit{pos,cover_quote,cover_ns,close_estimate_quote}=>
         ("EXIT_PENDING",json!({"position":pos,"cover_quote":cover_quote,
                     "cover_ns":cover_ns,"close_estimate_quote":close_estimate_quote})),
       _=>return Err(deny("active position is flat or already closed")),
     };
     positions.insert(base.to_owned(),ReviewPosition{
       base:base.to_owned(),phase:phase.to_owned(),paper_details:details,
       fills:fills.clone(),quarantine:false});
   }
   let r=Self{schema:1,day_yek:p.day_yek,started_wall_ms:p.started_wall_ms,
     entries:p.stats.entries,closed:p.stats.closed,
     modeled_pnl_usdt:p.stats.modeled_realized_usdt,
     positions,real_trading_authorized:false};
   r.validate()?;Ok(r)
 }
 pub fn validate(&self)->Result<(),JournalError>{
   if self.schema!=1||self.real_trading_authorized{return Err(deny("invalid review authority"))}
   // Journal v1 records opening fill evidence only. It has NO durable evidence
   // for Binance BUY cover + OKX SELL (legs 3/4). Never record a paper
   // position as Closed or claim modeled *realized* profit from this owner.
   // A future closure schema must carry and validate both terminal ledgers.
   if self.closed!=0||self.modeled_pnl_usdt!=0.0{
      return Err(deny("paper journal v1 lacks closing-leg fill proof"))
   }
   Ok(())
 }
}
/// Version 2 seals the already-durable two closing legs inside the SAME
/// hash-chained PAPER journal. Local source-shape only: never real account truth.
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TerminalV2Seal{
 pub base:String,pub original_generation:u64,
 pub original_spot_order_id:String,pub original_futures_order_id:String,
 pub source_digest_sha256:String,
 pub futures_cover:crate::fill_economics::LegFillLedger,
 pub spot_sell:crate::fill_economics::LegFillLedger,
 pub local_source_only:bool,pub verified_closed:bool,
 pub may_send_or_retry:bool,
}
#[derive(Clone,Debug,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
struct JournalLine{
 version:u32,seq:u64,previous_hash:String,
 snapshot:ReviewSnapshot,
 #[serde(default,skip_serializing_if="Option::is_none")]
 terminal_v2:Option<TerminalV2Seal>,
 checksum:String,
}
fn line_hash(seq:u64,previous:&str,snapshot:&ReviewSnapshot)
  ->Result<String,JournalError>{
 // Serialize the typed struct directly: serde_json::Value cannot safely
 // hold e12-derived u128 quote×quantity accumulator numbers > u64::MAX.
 let bytes=serde_json::to_vec(&(1u32,seq,previous,snapshot))
    .map_err(|_|deny("serialization failed"))?;
 Ok(hash(&bytes))
}
fn line_hash_v2(seq:u64,previous:&str,snapshot:&ReviewSnapshot,
 seal:&TerminalV2Seal)->Result<String,JournalError>{
 let raw=serde_json::to_vec(&(2u32,seq,previous,snapshot,seal))
   .map_err(|_|deny("terminal V2 checksum encoding failed"))?;
 Ok(hash(&raw))
}
/// Validated on append AND cold replay. The seal is never a permission or
/// a real settlement; it only pins the same local owner/terminal fills.
fn validate_terminal_v2(s:&ReviewSnapshot,p:&TerminalV2Seal)
 ->Result<(),JournalError>{
 if !p.local_source_only||p.verified_closed||p.may_send_or_retry
    ||p.base.is_empty()||p.original_generation==0
    ||p.source_digest_sha256.len()!=64||
      !p.source_digest_sha256.bytes().all(|b|b.is_ascii_hexdigit()&&!b.is_ascii_uppercase()){
   return Err(deny("terminal v2 cannot assert authority or weak source identity"))
 }
 let r=s.positions.get(&p.base).ok_or(deny("terminal owner missing"))?;
 let g=&r.fills;
 let marker=g.v2_local_alignment.as_ref().ok_or(deny("terminal V2 source marker missing"))?;
 if r.phase!="EXIT_PENDING"||r.quarantine||g.restart_quarantine||
    g.unknown.is_some()||g.generation!=p.original_generation||
    marker.original_generation!=p.original_generation||
    marker.source_digest_sha256!=p.source_digest_sha256||
    marker.independent_exchange_truth||marker.verified_closed||marker.may_send_or_retry||
    g.spot.exchange_order_id.as_deref()!=Some(p.original_spot_order_id.as_str())||
    g.shorts.len()!=1||
    g.shorts[0].exchange_order_id.as_deref()!=Some(p.original_futures_order_id.as_str())||
    g.cover.as_ref()!=Some(&p.futures_cover)||
    g.sell.as_ref()!=Some(&p.spot_sell){
   return Err(deny("terminal v2 differs from original owned close source"))
 }
 let v=g.paper_close_exposure().map_err(|_|deny("terminal ledger invalid"))?;
 if !v.mock_both_closing_legs_complete||v.original_net_base_atoms==0||
    v.remaining_short_atoms!=0||v.remaining_spot_base_atoms!=0||
    v.independently_verified_closed||v.real_execution_authorized||
    !p.futures_cover.cumulative_truth_present||!p.spot_sell.cumulative_truth_present||
    p.futures_cover.exchange_order_id.as_deref().unwrap_or("").is_empty()||
    p.spot_sell.exchange_order_id.as_deref().unwrap_or("").is_empty()||
    p.futures_cover.fills.is_empty()||p.spot_sell.fills.is_empty(){
   return Err(deny("terminal v2 has missing closing leg or incomplete source"))
 }
 Ok(())
}
#[derive(Debug)]
pub struct RecoveredPaper{
 pub sequence:u64,pub hash:String,pub state:ReviewSnapshot,
 /// Local immutable terminal ledgers. NOT authorized Closed / PnL.
 pub sealed_terminal_v2:BTreeMap<String,TerminalV2Seal>,
 pub pending_reconciliation:Vec<String>,
 pub may_resume_paper:bool,pub may_trade_real:bool,
}
// A journal is only an observation; no permission is granted on recovery.
fn validate_snapshot(s:&ReviewSnapshot)->Result<(),JournalError>{
 s.validate()?;
 if s.started_wall_ms<=0||s.entries>MAX_DAILY_PAPER as u64{
  return Err(deny("invalid counters"))
 }
 Ok(())
}
// R78: enforce same original-fill ancestry at the PUBLIC journal boundary,
// not only within PaperOwner::transact. Also applies on cold journal replay.
// This closes direct append_snapshot_if_prev bypass of R74 owner CAS checks.
fn validate_snapshot_successor(
 previous:&ReviewSnapshot,next:&ReviewSnapshot
)->Result<(),JournalError>{
 if next.day_yek!=previous.day_yek
    ||next.started_wall_ms!=previous.started_wall_ms
    ||next.entries<previous.entries||next.closed<previous.closed{
  return Err(deny("paper counter or day regression"))
 }
 let vanished=previous.positions.keys()
    .filter(|base|!next.positions.contains_key(*base)).count();
 if next.closed-previous.closed<vanished as u64{
  return Err(deny("lost paper position without completed close"))
 }
 for (base,old) in &previous.positions{
  if let Some(new)=next.positions.get(base){
   // R85: never rewind a retained owner's observable lifecycle even
   // when original fill ancestry is itself non-regressing.
   let stage=|phase:&str|match phase{
     "AWAITING_HEDGE"=>Some(0u8),"HEDGED"=>Some(1),
     "EXIT_PENDING"=>Some(2),_=>None,
   };
   if stage(&new.phase).zip(stage(&old.phase))
      .map_or(true,|(now,prior)|now<prior){
     return Err(deny("durable PAPER owner phase regressed"))
   }
   if !crate::native_paper_owner_transaction_review::PaperOwner::
      fill_gate_extends(&old.fills,&new.fills){
    return Err(deny("original owned fill history regressed"))
   }
   if old.fills.v2_local_alignment.is_some()&&
      old.fills.v2_local_alignment!=new.fills.v2_local_alignment{
     return Err(deny("durable V2 source marker changed or disappeared"))
   }
  }
 }
 Ok(())
}
fn validate_positions(s:&ReviewSnapshot)->Result<(),JournalError>{
 if s.positions.len()>MAX_CONCURRENT_PAPER||s.closed>s.entries||
   s.positions.len() as u64+s.closed>s.entries||
   !s.modeled_pnl_usdt.is_finite(){return Err(deny("invalid portfolio bounds"))}
 for (base,row) in &s.positions{
   let pos=if row.phase=="EXIT_PENDING"{&row.paper_details["position"]}else{&row.paper_details};
   if row.base!=*base||row.fills.spot.symbol!=format!("{base}USDT")||
     pos["base"].as_str()!=Some(base)||
     !matches!(row.phase.as_str(),"AWAITING_HEDGE"|"HEDGED"|"EXIT_PENDING"){
      return Err(deny("owner or paper phase mismatch"))
   }
   let exposure=row.fills.exposure()
      .map_err(|_|deny("invalid order fill gate"))?;
   if (row.fills.cover.is_some()||row.fills.sell.is_some())
       &&row.phase!="EXIT_PENDING"{
      return Err(deny("closing legs require owned EXIT_PENDING phase"))
   }
   row.fills.paper_close_exposure()
      .map_err(|_|deny("invalid source-shaped closing ledger"))?;
   // R85: public append MUST enforce the same phase-vs-original-fill
   // contract as PaperOwner::validate_legs, not merely legal phase names.
   // Cold journal reload runs this check on EVERY row, before recovery.
   let typed:crate::native_paper_market_execution_review::PaperPosition=
      serde_json::from_value(pos.clone())
        .map_err(|_|deny("typed PAPER position absent or malformed"))?;
   if typed.base!=*base{return Err(deny("wrong typed PAPER base"))}
   match row.phase.as_str(){
      "AWAITING_HEDGE"=>{
         if typed.futures_short_ns.is_some()||
            (exposure.futures_filled_atoms>0&&
             (!exposure.spot_terminal||!exposure.spot_fee_complete)){
            return Err(deny("partial paper hedge phase contradicts owned fills"))
         }
      },
      "HEDGED"|"EXIT_PENDING"=>{
         let qty=crate::fill_economics::decimal_to_atoms(
            &format!("{:.12}",typed.spot_delivered_base))
            .map_err(|_|deny("invalid modeled position quantity"))?;
         let close_unknown=row.phase=="EXIT_PENDING"
            &&!row.fills.restart_quarantine
            &&row.fills.unknown.as_ref().is_some_and(|u|
               row.fills.cover.as_ref().is_some_and(|c|
                  c.intent_id==u.intent_id&&c.client_order_id==u.client_order_id)
               ||row.fills.sell.as_ref().is_some_and(|s|
                  s.intent_id==u.intent_id&&s.client_order_id==u.client_order_id));
         if (exposure.unknown_or_quarantine&&!close_unknown)
            ||!exposure.spot_terminal||!exposure.spot_fee_complete
            ||!exposure.all_futures_terminal
            ||exposure.futures_filled_atoms==0
            ||exposure.unhedged_base_atoms!=0
            ||exposure.spot_net_delivered_atoms!=qty
            ||exposure.futures_filled_atoms!=qty
            ||typed.futures_short_ns.is_none(){
            return Err(deny("hedged PAPER phase has no full owned futures-fill truth"))
         }
         if row.phase=="EXIT_PENDING"{
            let cover=&row.paper_details;
            if !cover["cover_quote"].as_f64().is_some_and(|x|x.is_finite()&&x>0.0)
                ||!cover["close_estimate_quote"].as_f64().is_some_and(|x|x.is_finite())
                ||!cover["cover_ns"].as_u64().is_some_and(|x|
                    x>typed.futures_short_ns.unwrap_or(0)){
               return Err(deny("invalid owned PAPER exit-preparation clock or quote"))
            }
         }
      },
      _=>return Err(deny("unsupported PAPER lifecycle stage")),
   }
 }
 Ok(())
}
pub fn read_journal(path:&Path)->Result<Option<RecoveredPaper>,JournalError>{
 if !path.exists(){return Ok(None)}
 let mut bytes=Vec::new();
 File::open(path)?.take(8_388_609).read_to_end(&mut bytes)?;
 if bytes.is_empty()||bytes.len()>8_388_608||bytes.last()!=Some(&b'\n'){
   return Err(deny("missing or incomplete journal tail"))
 }
 let mut seq=0u64;let mut last=ZERO.to_owned();let mut state:Option<ReviewSnapshot>=None;
 let mut sealed=BTreeMap::<String,TerminalV2Seal>::new();
 for raw in bytes.split(|c|*c==b'\n').filter(|v|!v.is_empty()){
   let line:JournalLine=serde_json::from_slice(raw).map_err(|_|deny("invalid journal row"))?;
   line.snapshot.validate()?;validate_positions(&line.snapshot)?;
   if line.seq!=seq+1||line.previous_hash!=last{
      return Err(deny("gap or wrong parent hash"))
   }
   let calculated=match (line.version,line.terminal_v2.as_ref()){
     (1,None)=>line_hash(line.seq,&last,&line.snapshot)?,
     (2,Some(proof))=>{
       let prior=state.as_ref().ok_or(deny("terminal v2 needs original v1 journal"))?;
       if serde_json::to_vec(prior).map_err(|_|deny("prior snapshot invalid"))?
         !=serde_json::to_vec(&line.snapshot).map_err(|_|deny("v2 snapshot invalid"))?{
          return Err(deny("terminal v2 cannot mutate snapshot or fabricate Closed"))
       }
       validate_terminal_v2(&line.snapshot,proof)?;
       if sealed.contains_key(&proof.base){return Err(deny("duplicate terminal v2 owner"))}
       line_hash_v2(line.seq,&last,&line.snapshot,proof)?
     },
     _=>return Err(deny("invalid journal version or terminal payload")),
   };
   if calculated!=line.checksum{return Err(deny("checkpoint checksum mismatch"))}
   if let Some(previous)=state.as_ref(){
     validate_snapshot_successor(previous,&line.snapshot)?;
   }
   if let Some(proof)=line.terminal_v2{
     sealed.insert(proof.base.clone(),proof);
   }else if !sealed.is_empty(){
     return Err(deny("v1 append after terminal v2 seal is forbidden"))
   }
   seq=line.seq;last=calculated;state=Some(line.snapshot);
 }
 let mut state=state.ok_or(deny("journal contains no records"))?;
 for row in state.positions.values_mut(){
   let raw=row.fills.paper_checkpoint().map_err(|_|deny("invalid fill checkpoint"))?;
   row.fills=PaperPartialGate::restart_from_checkpoint(&raw)
     .map_err(|_|deny("could not quarantine recovered fills"))?;
   row.quarantine=true;
 }
 let pending=state.positions.keys().cloned().collect();
 Ok(Some(RecoveredPaper{
  sequence:seq,hash:last,state,sealed_terminal_v2:sealed,
  pending_reconciliation:pending,
  may_resume_paper:false,may_trade_real:false,
 }))
}
/// Review-only comparison to a caller-supplied *exact* journal floor.
/// The caller MUST obtain this tip outside the rollback/failure domain and
/// authenticate its currentness. This code cannot provision or verify that.
/// A local signed/hash-correct journal by itself is NOT proof of latest state.
/// A match NEVER grants trading, resumption or automatic repair.
pub fn inspect_journal_against_supplied_floor(
 path:&Path,expected_sequence:u64,expected_hash:&str,
)->Result<RecoveredPaper,JournalError>{
 let hex_ok=expected_hash.len()==64&&expected_hash.bytes()
    .all(|b|b.is_ascii_hexdigit()&&!b.is_ascii_uppercase());
 if expected_sequence==0||!hex_ok{return Err(deny("unprovisioned journal floor"))}
 let recovered=read_journal(path)?.ok_or(deny("journal missing against supplied floor"))?;
 if recovered.sequence!=expected_sequence||recovered.hash!=expected_hash{
    return Err(deny("PAPER journal diverges from independently supplied floor"))
 }
 if recovered.may_resume_paper||recovered.may_trade_real{
    return Err(deny("recovered review unexpectedly grants trading"))
 }
 Ok(recovered)
}
struct LocalWriteGuard(std::path::PathBuf);
impl Drop for LocalWriteGuard{
 fn drop(&mut self){let _=fs::remove_file(&self.0);}
}
fn acquire(path:&Path)->Result<LocalWriteGuard,JournalError>{
 let lock=path.with_extension("writer_lock");
 let mut f=OpenOptions::new().write(true).create_new(true).open(&lock)?;
 f.write_all(b"paper-review-single-writer")?;
 f.sync_all()?;
 Ok(LocalWriteGuard(lock))
}
type AlignmentSource<'a>=(&'a str,
 &'a crate::native_trade2f3_v2_versioned_store::V2ReviewState,
 &'a crate::native_trade2f3_v2_four_leg_close_review::V2FourLegCloseObservation);
#[cfg(test)]
pub fn append_snapshot(path:&Path,s:&ReviewSnapshot)->Result<u64,JournalError>{
 append_snapshot_inner(path,s,None,None)
}
pub fn append_snapshot_if_prev(path:&Path,s:&ReviewSnapshot,expected_seq:u64)
 ->Result<u64,JournalError>{append_snapshot_inner(path,s,Some(expected_seq),None)}
pub(crate) fn append_v2_aligned_snapshot_if_prev(
 path:&Path,s:&ReviewSnapshot,expected_seq:u64,source:AlignmentSource<'_>
)->Result<u64,JournalError>{
 append_snapshot_inner(path,s,Some(expected_seq),Some(source))
}
// Only a source-rechecked V2 alignment may create a new journal marker.
// The public append API always supplies None, even with a correct CAS seq.
fn verify_new_alignment(
 prior:Option<&ReviewSnapshot>,next:&ReviewSnapshot,parent_seq:u64,
 source:Option<AlignmentSource<'_>>
)->Result<(),JournalError>{
 let mut granted=false;
 for (base,row) in &next.positions{
  let before=prior.and_then(|p|p.positions.get(base))
     .and_then(|p|p.fills.v2_local_alignment.as_ref());
  let after=row.fills.v2_local_alignment.as_ref();
  if before==after{continue}
  if before.is_some()||after.is_none()||granted||row.phase!="EXIT_PENDING"{
   return Err(deny("V2 marker forged, removed or modified"))
  }
  let (permitted,v2,obs)=source
    .ok_or(deny("V2 marker requires original source recheck"))?;
  if permitted!=base||parent_seq==0{
   return Err(deny("wrong V2 marker source owner"))
  }
  let expected=crate::native_paper_v2_closure_bridge_review::
    inspect_paper_v2_alignment(&row.fills,v2,obs,parent_seq)
    .map_err(|_|deny("V2 source validation failed at journal append"))?;
  if after!=Some(&expected){return Err(deny("V2 marker not source derived"))}
  granted=true;
 }
 if granted!=source.is_some(){return Err(deny("unexpected V2 marker permission"))}
 Ok(())
}
fn append_snapshot_inner(path:&Path,s:&ReviewSnapshot,expected:Option<u64>,
 alignment:Option<AlignmentSource<'_>>)->Result<u64,JournalError>{
 validate_snapshot(s)?;validate_positions(s)?;
 let parent=path.parent().ok_or(deny("missing journal directory"))?;
 fs::create_dir_all(parent)?;
 let _guard=acquire(path)?;
 let prior=read_journal(path)?;
 let (seq,hash)=match prior.as_ref(){
  Some(old)=>(old.sequence,old.hash.clone()),
  None=>(0,ZERO.to_owned()),
 };
 if expected.is_some_and(|want|want!=seq){return Err(deny("stale journal revision"))}
 if prior.as_ref().is_some_and(|p|!p.sealed_terminal_v2.is_empty()){
   return Err(deny("old snapshot append after terminal v2 seal forbidden"))
 }
 if let Some(previous)=prior.as_ref(){
   validate_snapshot_successor(&previous.state,s)?;
 }
 verify_new_alignment(prior.as_ref().map(|p|&p.state),s,seq,alignment)?;
 let next=seq.checked_add(1).ok_or(deny("journal revision overflow"))?;
 let checksum=line_hash(next,&hash,s)?;
 let row=JournalLine{version:1,seq:next,previous_hash:hash,
   snapshot:s.clone(),terminal_v2:None,checksum};
 let mut bytes=serde_json::to_vec(&row).map_err(|_|deny("encode journal row"))?;
 bytes.push(b'\n');
 let old_size=fs::metadata(path).map(|m|m.len()).unwrap_or(0);
 if old_size+bytes.len() as u64>8_388_608{
    return Err(deny("journal size budget reached"))
 }
 // Fault injection is compiled into lib unit-test targets ONLY.
 // Marker must be explicitly set in the isolated subprocess test worker.
 #[cfg(test)]
 let fail=if std::env::var("SPOTARB_TEST_JOURNAL_FAULT_CHILD").ok().as_deref()
      ==Some("isolated_subprocess"){
     std::env::var("SPOTARB_TEST_JOURNAL_FAULT_KIND").unwrap_or_default()
 }else{String::new()};
 #[cfg(test)]
 if fail=="before_append"{
    return Err(JournalError::Io(io::ErrorKind::Other))
 }
 let mut f=OpenOptions::new().create(true).append(true).open(path)?;
 #[cfg(test)]
 if fail=="partial_write"{
    f.write_all(&bytes[..bytes.len()/2])?;
    f.sync_all()?;
    return Err(JournalError::Io(io::ErrorKind::Other))
 }
 f.write_all(&bytes)?;
 #[cfg(test)]
 if fail=="after_full_write_before_sync"{
    return Err(JournalError::Io(io::ErrorKind::Other))
 }
 f.sync_all()?;
 #[cfg(test)]
 if fail=="after_file_sync_error"{
    return Err(JournalError::Io(io::ErrorKind::Other))
 }
 // Test-binary ONLY: simulate an entire process dying inside the
 // file-fsync -> directory-fsync boundary. This cannot model power loss.
 #[cfg(test)]
 if std::env::var("SPOTARB_TEST_JOURNAL_CRASH_POINT").ok().as_deref()
      ==Some("after_file_fsync_before_directory_fsync")
    &&std::env::var("SPOTARB_TEST_JOURNAL_CRASH_CHILD").ok().as_deref()
      ==Some("offline_unit_test"){
    std::process::exit(94);
 }
 File::open(parent)?.sync_all()?;
 Ok(next)
}
/// Append a V2 terminal proof inside this already-existing journal.
 /// Owner, both legs and local V2 source are rechecked under the journal lock.
 /// This does NOT close the position or recognize realized profit.
pub fn append_terminal_v2_if_prev(
 path:&Path,base:&str,expected_seq:u64,
 v2:&crate::native_trade2f3_v2_versioned_store::V2ReviewState,
 obs:&crate::native_trade2f3_v2_four_leg_close_review::V2FourLegCloseObservation,
)->Result<u64,JournalError>{
 let parent=path.parent().ok_or(deny("terminal journal directory absent"))?;
 if !parent.symlink_metadata()?.file_type().is_dir(){
    return Err(deny("terminal journal parent not regular directory"))
 }
 let _guard=acquire(path)?;
 let recovered=read_journal(path)?.ok_or(deny("terminal journal absent"))?;
 if recovered.sequence!=expected_seq||expected_seq==0||
    recovered.sealed_terminal_v2.contains_key(base){
   return Err(deny("stale or duplicate terminal V2 CAS"))
 }
 crate::native_paper_v2_closure_bridge_review::
   inspect_recovered_alignment_consistency(&recovered,base,v2,obs)
   .map_err(|_|deny("V2 terminal source does not match immutable PAPER alignment"))?;
 // Read the exact unquarantined last on-disk snapshot after the lock.
 // RecoveredPaper.state is deliberately quarantined and must not be written.
 let raw=fs::read(path)?;
 if raw.len()>8_388_608||raw.last()!=Some(&b'\n'){
   return Err(deny("terminal journal partial prior row"))
 }
 let last=raw.split(|b|*b==b'\n').filter(|x|!x.is_empty()).last()
   .ok_or(deny("terminal journal last row absent"))?;
 let previous:JournalLine=serde_json::from_slice(last)
   .map_err(|_|deny("terminal journal last row invalid"))?;
 if previous.seq!=expected_seq||previous.checksum!=recovered.hash{
   return Err(deny("terminal journal parent changed during audit"))
 }
 let state=previous.snapshot;
 let row=state.positions.get(base).ok_or(deny("terminal base absent"))?;
 let g=&row.fills;
 let marker=g.v2_local_alignment.as_ref().ok_or(deny("V2 alignment missing"))?;
 let proof=TerminalV2Seal{
    base:base.into(),original_generation:g.generation,
    original_spot_order_id:g.spot.exchange_order_id.clone()
       .ok_or(deny("original spot exchange order absent"))?,
    original_futures_order_id:g.shorts.first()
       .and_then(|s|s.exchange_order_id.clone())
       .ok_or(deny("original futures exchange order absent"))?,
    source_digest_sha256:marker.source_digest_sha256.clone(),
    futures_cover:g.cover.clone().ok_or(deny("terminal futures cover absent"))?,
    spot_sell:g.sell.clone().ok_or(deny("terminal spot sell absent"))?,
    local_source_only:true,verified_closed:false,may_send_or_retry:false,
 };
 validate_terminal_v2(&state,&proof)?;
 let seq=expected_seq.checked_add(1).ok_or(deny("terminal sequence overflow"))?;
 let checksum=line_hash_v2(seq,&recovered.hash,&state,&proof)?;
 let entry=JournalLine{version:2,seq,previous_hash:recovered.hash,
      snapshot:state,terminal_v2:Some(proof),checksum};
 let mut bytes=serde_json::to_vec(&entry).map_err(|_|deny("terminal v2 serialization"))?;
 bytes.push(b'\n');
 if raw.len().checked_add(bytes.len()).map_or(true,|n|n>8_388_608){
   return Err(deny("terminal journal size cap"))
 }
 let mut f=OpenOptions::new().append(true).open(path)?;
 f.write_all(&bytes)?;f.sync_all()?;
 File::open(parent)?.sync_all()?;
 Ok(seq)
}
#[cfg(test)]
mod tests{
 use super::*;
 use crate::native_paper_market_execution_review::{
    PaperPosition,PaperMachine,PaperPhase,PaperAssumptions,
 };
 use crate::fill_economics::decimal_to_atoms;
 use std::time::{SystemTime,UNIX_EPOCH};
 fn dir(label:&str)->std::path::PathBuf{
   let v=SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
   std::env::temp_dir().join(format!("spotarb-paper-journal-{label}-{v}"))
 }
 fn fixture()->(PaperPortfolio,BTreeMap<String,PaperPartialGate>){
   let mut p=PaperPortfolio::new(1_791_786_000_000).unwrap();
   p.stats.entries=1;
   let position=PaperPosition{
    base:"ONE".into(),signal_ns:12,spot_buy_ns:12,
    signal_estimated_net_bps:70.0,spot_gross_base:4.0,
    spot_delivered_base:4.0,spot_paid_quote:10.0,
    futures_short_ns:None,futures_short_proceeds_quote:None,
    futures_short_base:None,hedge_entry_estimated_net_bps:None,
   };
   p.positions.insert("ONE".into(),PaperMachine{
      config:PaperAssumptions::default(),phase:PaperPhase::AwaitingHedge(position)
   });
   let gate=PaperPartialGate::new(7,"owner1","ONEUSDT","spot_intent","spot_client",
      decimal_to_atoms("4").unwrap()).unwrap();
   (p,BTreeMap::from([("ONE".into(),gate)]))
 }
 fn snapshot()->ReviewSnapshot{
    let (p,g)=fixture();ReviewSnapshot::from_paper(&p,&g).unwrap()
 }
 #[test]fn journal_write_failure_child(){
   use crate::native_paper_owner_transaction_review::PaperOwner;
   let Ok(raw)=std::env::var("SPOTARB_TEST_JOURNAL_FAULT_PATH") else{return};
   let mut owner=PaperOwner{
      portfolio:PaperPortfolio::new(1_791_786_000_000).unwrap(),
      gate_by_base:BTreeMap::new(),
      journal:std::path::PathBuf::from(raw),
      last_seq:1,frozen:false,
   };
   let result=owner.transact(|p,_|{
      p.stats.entries+=1;
      Ok(())
   });
   assert!(result.is_err(),"simulated write failure must deny ACK");
   assert!(owner.frozen,"ambiguous local write must freeze owner");
   assert_eq!(owner.last_seq,1,"in-memory generation cannot advance");
   assert_eq!(owner.portfolio.stats.entries,0,"RAM cannot publish unacked state");
 }
 #[test]fn local_write_faults_are_fail_closed_across_processes(){
   use crate::native_paper_owner_transaction_review::PaperOwner;
   use std::process::{Command,Stdio};
   for kind in ["before_append","partial_write",
       "after_full_write_before_sync","after_file_sync_error"]{
      let folder=dir(kind);
      fs::create_dir_all(&folder).unwrap();
      let path=folder.join("fault.jsonl");
      let starter=PaperOwner::start_new(&path,1_791_786_000_000).unwrap();
      assert_eq!(starter.last_seq,1);
      drop(starter);
      let child=Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("native_paper_durable_journal_review::tests::journal_write_failure_child")
        .arg("--nocapture")
        .env("SPOTARB_TEST_JOURNAL_FAULT_PATH",&path)
        .env("SPOTARB_TEST_JOURNAL_FAULT_CHILD","isolated_subprocess")
        .env("SPOTARB_TEST_JOURNAL_FAULT_KIND",kind)
        .stdout(Stdio::null()).stderr(Stdio::null())
        .status().unwrap();
      assert!(child.success(),"offline injected error must be rejected: {kind}");
      let recovered=read_journal(&path);
      match kind{
         "before_append"=>{
            let r=recovered.unwrap().unwrap();
            assert_eq!(r.sequence,1);
            assert!(!r.may_resume_paper&&!r.may_trade_real);
         }
         "partial_write"=>{
            assert!(recovered.is_err(),"torn JSON tail must quarantine");
            assert!(append_snapshot_if_prev(&path,&snapshot(),1).is_err(),
               "no automatic truncation of partial tail");
         }
         "after_file_sync_error"=>{
            let r=recovered.unwrap().unwrap();
            assert_eq!(r.sequence,2);
            assert!(!r.may_resume_paper&&!r.may_trade_real);
            assert!(append_snapshot_if_prev(&path,&snapshot(),1).is_err(),
                "stale pre-error CAS must fail");
         }
         "after_full_write_before_sync"=>{
            // Kernel buffers may still expose the full row. Do NOT assume
            // a machine power cut would preserve it; no ACK was returned.
            if let Ok(Some(r))=recovered {
                assert!(r.sequence==1||r.sequence==2);
                assert!(!r.may_resume_paper&&!r.may_trade_real);
            }
         }
         _=>unreachable!(),
      }
      assert!(PaperOwner::start_new(&path,1_791_786_000_000).is_err());
      fs::remove_dir_all(folder).unwrap();
   }
 }
 #[test]fn journal_process_crash_inside_fsync_child(){
   let Ok(path)=std::env::var("SPOTARB_TEST_JOURNAL_PATH") else{return};
   let p=std::path::PathBuf::from(path);
   // This must exit from INSIDE append_snapshot_inner after file fsync,
   // not return and acknowledge this transaction.
   let _=append_snapshot_if_prev(&p,&snapshot(),1);
   panic!("fault injection did not exit at file->dir sync boundary");
 }
 #[test]fn process_crash_after_file_fsync_before_dir_fsync_is_quarantined(){
   use std::process::{Command,Stdio};
   let folder=dir("fsync-process-boundary");
   fs::create_dir_all(&folder).unwrap();
   let path=folder.join("paper.jsonl");
   assert_eq!(append_snapshot_if_prev(&path,&snapshot(),0).unwrap(),1);
   let child=Command::new(std::env::current_exe().unwrap())
     .arg("--exact")
     .arg("native_paper_durable_journal_review::tests::journal_process_crash_inside_fsync_child")
     .arg("--nocapture")
     .env("SPOTARB_TEST_JOURNAL_PATH",&path)
     .env("SPOTARB_TEST_JOURNAL_CRASH_POINT","after_file_fsync_before_directory_fsync")
     .env("SPOTARB_TEST_JOURNAL_CRASH_CHILD","offline_unit_test")
     .stdout(Stdio::null()).stderr(Stdio::null())
     .status().unwrap();
   assert_eq!(child.code(),Some(94));
   // An OS-process death does not power off the storage. This proves only
   // process fail-closed behavior, NEVER stable-power-loss durability.
   let rec=read_journal(&path).unwrap().unwrap();
   assert_eq!(rec.sequence,2);
   assert!(rec.state.positions["ONE"].quarantine);
   assert!(rec.state.positions["ONE"].fills.restart_quarantine);
   assert!(!rec.may_resume_paper&&!rec.may_trade_real);
   assert!(append_snapshot_if_prev(&path,&snapshot(),1).is_err(),
     "stale caller must not replay after lost durable ACK");
   assert_eq!(read_journal(&path).unwrap().unwrap().sequence,2);
   fs::remove_dir_all(folder).unwrap();
 }
 #[test]
 fn r85_direct_append_cannot_forge_hedged_without_original_futures_fills(){
    let folder=dir("r85-no-futures");
    let path=folder.join("paper.jsonl");
    let source=snapshot();
    assert_eq!(append_snapshot_if_prev(&path,&source,0).unwrap(),1);
    let mut forged=source.clone();
    forged.positions.get_mut("ONE").unwrap().phase="HEDGED".into();
    assert!(append_snapshot_if_prev(&path,&forged,1).is_err(),
      "a hedge state needs terminal original owned fill ledgers, not just a phase string");
    let recovered=read_journal(&path).unwrap().unwrap();
    assert_eq!(recovered.sequence,1);
    assert_eq!(recovered.state.positions["ONE"].phase,"AWAITING_HEDGE");
    fs::remove_dir_all(folder).unwrap();
 }
 #[test]
 fn r85_checksum_valid_forged_hedge_is_rejected_on_cold_replay(){
    let folder=dir("r85-forged-checksum");
    let path=folder.join("paper.jsonl");
    let legitimate=snapshot();
    append_snapshot_if_prev(&path,&legitimate,0).unwrap();
    let parent=read_journal(&path).unwrap().unwrap();
    let mut forged=legitimate.clone();
    forged.positions.get_mut("ONE").unwrap().phase="HEDGED".into();
    let hash=line_hash(2,&parent.hash,&forged).unwrap();
    let illegal=JournalLine{version:1,seq:2,
      previous_hash:parent.hash,snapshot:forged,terminal_v2:None,checksum:hash};
    use std::io::Write;
    let mut file=OpenOptions::new().append(true).open(&path).unwrap();
    writeln!(file,"{}",serde_json::to_string(&illegal).unwrap()).unwrap();
    file.sync_all().unwrap();
    assert!(read_journal(&path).is_err(),
       "even a locally recomputed SHA cannot make a hedged state without fills valid");
    fs::remove_dir_all(folder).unwrap();
 }
 #[test]
 fn r85_existing_exit_phase_cannot_rewind_to_entry_in_journal_cas(){
    let mut exit=snapshot();
    exit.positions.get_mut("ONE").unwrap().phase="EXIT_PENDING".into();
    let mut rewound=exit.clone();
    rewound.positions.get_mut("ONE").unwrap().phase="AWAITING_HEDGE".into();
    assert!(validate_snapshot_successor(&exit,&rewound).is_err(),
      "a later journal row cannot regress EXIT_PENDING -> AWAITING_HEDGE");
 }
 #[test]fn reread_state_always_quarantined(){
    let folder=dir("reopen");let file=folder.join("journal.jsonl");
    assert_eq!(append_snapshot(&file,&snapshot()).unwrap(),1);
    let recovered=read_journal(&file).unwrap().unwrap();
    assert_eq!(recovered.sequence,1);
    assert!(recovered.state.positions["ONE"].quarantine);
    assert!(recovered.state.positions["ONE"].fills.restart_quarantine);
    assert!(!recovered.may_resume_paper);
    assert!(!recovered.may_trade_real);
    let _=fs::remove_dir_all(folder);
 }
 #[test]fn two_revisions_reopen_exact_latest(){
   let dir=dir("twoseq");let path=dir.join("paper.jsonl");
   let s=snapshot();
   assert_eq!(append_snapshot(&path,&s).unwrap(),1);
   assert_eq!(append_snapshot(&path,&s).unwrap(),2);
   let rec=read_journal(&path).unwrap().unwrap();
   assert_eq!(rec.sequence,2);
   assert_eq!(rec.state.entries,1);
   assert!(!rec.may_resume_paper);
   let _=fs::remove_dir_all(dir);
 }
 #[test]fn damaged_tail_is_rejected(){
   let dir=dir("bad-t");let path=dir.join("paper.jsonl");
   append_snapshot(&path,&snapshot()).unwrap();
   fs::write(&path,b"broken").unwrap();
   assert!(read_journal(&path).is_err());
   let _=fs::remove_dir_all(dir);
 }
 #[test]fn lost_active_position_cannot_disappear_silently(){
   let dir=dir("missing");let path=dir.join("paper.jsonl");
   let mut s=snapshot();append_snapshot(&path,&s).unwrap();
   s.positions.clear();
   assert!(append_snapshot(&path,&s).is_err());
   assert_eq!(read_journal(&path).unwrap().unwrap().state.positions.len(),1);
   let _=fs::remove_dir_all(dir);
 }
 #[test]fn closing_counter_without_two_closing_legs_must_not_commit(){
   let dir=dir("no-fake-close");let path=dir.join("paper.jsonl");
   let original=snapshot();append_snapshot(&path,&original).unwrap();
   let mut fake=original.clone();
   fake.closed=1;fake.positions.clear();
   assert!(append_snapshot(&path,&fake).is_err());
   assert_eq!(read_journal(&path).unwrap().unwrap().sequence,1);
   assert_eq!(read_journal(&path).unwrap().unwrap().state.closed,0);
   let _=fs::remove_dir_all(dir);
 }
 #[test]fn invented_realized_money_must_not_commit_even_if_position_is_kept(){
   let dir=dir("no-fake-money");let path=dir.join("paper.jsonl");
   let original=snapshot();append_snapshot(&path,&original).unwrap();
   for value in [0.2,-0.5] {
     let mut fake=original.clone();fake.modeled_pnl_usdt=value;
     assert!(append_snapshot(&path,&fake).is_err());
   }
   assert_eq!(read_journal(&path).unwrap().unwrap().sequence,1);
   assert_eq!(read_journal(&path).unwrap().unwrap().state.modeled_pnl_usdt,0.0);
   let _=fs::remove_dir_all(dir);
 }
 #[test]fn missing_closing_fill_evidence_rejects_closed_counter(){
   let folder=dir("close-without-fills");let file=folder.join("paper.jsonl");
   let original=snapshot();append_snapshot(&file,&original).unwrap();
   let mut candidate=original.clone();candidate.closed=1;
   assert!(append_snapshot(&file,&candidate).is_err());
   assert_eq!(read_journal(&file).unwrap().unwrap().sequence,1);
 }
 #[test]fn missing_closing_fill_evidence_rejects_realized_amount(){
   let folder=dir("money-without-fills");let file=folder.join("paper.jsonl");
   let original=snapshot();append_snapshot(&file,&original).unwrap();
   for amount in [0.3,-0.5]{
      let mut candidate=original.clone();candidate.modeled_pnl_usdt=amount;
      assert!(append_snapshot(&file,&candidate).is_err());
   }
   assert_eq!(read_journal(&file).unwrap().unwrap().sequence,1);
 }
 #[test]fn corrupt_sha_never_unlocks_state(){
   let dir=dir("hash");let path=dir.join("paper.jsonl");
   append_snapshot(&path,&snapshot()).unwrap();
   let txt=fs::read_to_string(&path).unwrap();
   let changed=txt.replacen("\"checksum\":\"","\"checksum\":\"f",1);
   fs::write(&path,changed).unwrap();
   assert!(read_journal(&path).is_err());
   let _=fs::remove_dir_all(dir);
 }
}
#[cfg(test)]
mod persistence_checks {
 use super::*;
 #[test] fn no_checkpoint_means_no_resume(){
   let dir=std::env::temp_dir().join("paper-review-never-written-20261012");
   assert!(read_journal(&dir).unwrap().is_none());
 }
}
