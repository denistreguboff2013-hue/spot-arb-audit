//! Rust public-PAPER virtual position continuity across PLANNED observer
//! boundaries only. Same machine boot, short handoff, hash-checked fsync.
//! This is not native exchange durability, private fill proof, real PnL or
//! independent rollback protection. Never auto-restore after uncertain crash.
use crate::{
  native_paper_portfolio_review::{PaperPortfolio,MAX_CONCURRENT_PAPER,MAX_DAILY_PAPER},
  native_paper_market_execution_review::{PaperMachine,PaperPhase},
};
use openssl::sha::sha256;
use serde::{Serialize,Deserialize};
use std::{fs::{self,File,OpenOptions},io::{self,Read,Write},os::{fd::AsRawFd,unix::fs::{MetadataExt,OpenOptionsExt}},path::Path,time::{SystemTime,UNIX_EPOCH}};
const FORMAT:&str="native-paper-virtual-continuity-v1";
const MAX_BYTES:u64=256*1024;
const FRESH_MS:i64=180_000;
const YEK_OFFSET_MS:i64=5*3_600_000;
const DAY_MS:i64=86_400_000;
#[derive(Debug,Clone,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
struct Payload{
    schema:String,revision:u64,updated_ms:i64,monotonic_ns:u64,
    boot_id:String,completed:bool,portfolio:PaperPortfolio,
}
#[derive(Debug,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope{payload:Payload,sha256:String}
#[derive(Debug)]
pub enum CheckpointError{
    Io(io::Error),Invalid(&'static str),
}
impl From<io::Error> for CheckpointError{
    fn from(e:io::Error)->Self{Self::Io(e)}
}
fn deny(reason:&'static str)->CheckpointError{CheckpointError::Invalid(reason)}
fn digest(payload:&Payload)->Result<String,CheckpointError>{
    let bytes=serde_json::to_vec(payload).map_err(|_|deny("invalid serialization"))?;
    Ok(sha256(&bytes).iter().map(|x|format!("{x:02x}")).collect())
}
fn now_ms()->i64{
    SystemTime::now().duration_since(UNIX_EPOCH)
        .unwrap_or_default().as_millis() as i64
}
fn day(ms:i64)->i64{(ms+YEK_OFFSET_MS).div_euclid(DAY_MS)}
fn boot()->Result<String,CheckpointError>{
    let b=fs::read_to_string("/proc/sys/kernel/random/boot_id")?;
    let s=b.trim().to_ascii_lowercase();
    if s.len()!=36||!s.bytes().all(|c|c.is_ascii_hexdigit()||c==b'-'){
       return Err(deny("bad system boot identity"))
    }
    Ok(s)
}
fn position_valid(m:&PaperMachine,base:&str,now_ns:u64)->bool{
    if PaperMachine::new(m.config.clone()).is_err(){return false}
    let p=match &m.phase{
        PaperPhase::AwaitingHedge(x)|PaperPhase::Hedged(x)=>x,
        PaperPhase::AwaitingSpotExit{pos,..}=>pos,
        _=>return false
    };
    if p.base!=base||p.signal_ns==0||p.spot_buy_ns<p.signal_ns||
       p.spot_buy_ns>now_ns||
       !p.signal_estimated_net_bps.is_finite()||
       !p.spot_gross_base.is_finite()||p.spot_gross_base<=0.0||
       !p.spot_delivered_base.is_finite()||p.spot_delivered_base<=0.0||
       p.spot_delivered_base>p.spot_gross_base*(1.0+1e-9)||
       !p.spot_paid_quote.is_finite()||p.spot_paid_quote<=0.0{
        return false
    }
    match &m.phase{
       PaperPhase::AwaitingHedge(_)=>{
         p.futures_short_ns.is_none()&&p.futures_short_base.is_none()&&
         p.futures_short_proceeds_quote.is_none()
       }
       PaperPhase::Hedged(_)|PaperPhase::AwaitingSpotExit{..}=>{
         if !p.futures_short_ns.is_some_and(|v|
                v>=p.spot_buy_ns&&v<=now_ns)||
            !p.futures_short_base.is_some_and(|x|x.is_finite()&&x>0.0)||
            !p.futures_short_proceeds_quote.is_some_and(|x|x.is_finite()&&x>0.0)||
            !p.hedge_entry_estimated_net_bps.is_some_and(|x|x.is_finite()){
             return false
         }
         match &m.phase {
             PaperPhase::AwaitingSpotExit{
               cover_quote,cover_ns,close_estimate_quote,..}=>{
                *cover_ns>=p.futures_short_ns.unwrap()&&*cover_ns<=now_ns&&
                cover_quote.is_finite()&&*cover_quote>0.0&&
                close_estimate_quote.is_finite()
             }
             _=>true,
         }
       }
       _=>false,
    }
}
fn portfolio_valid(p:&PaperPortfolio,t:i64,mono:u64)->bool{
    // R186-R195: this is the LEGACY compiled-500 branch. Persisted
    // source-owned 3000/day quotas are only valid in the separate R153
    // checkpoint and must never be laundered into this old fallback.
    // Every modeled open/close must be accounted for in the daily entries.
    let accounted=p.positions.len() as u64;
    let stats=&p.stats;
    if p.started_wall_ms<=0||p.started_wall_ms>t||
       p.max_daily_entries!=MAX_DAILY_PAPER as u64||
       p.max_concurrent_positions!=MAX_CONCURRENT_PAPER||
       p.positions.len()>MAX_CONCURRENT_PAPER||
       stats.entries>MAX_DAILY_PAPER as u64||
       stats.closed>stats.entries||
       accounted.checked_add(stats.closed).map_or(true,|n|n>stats.entries)||
       stats.closed_wins.checked_add(stats.closed_losses)
         .map_or(true,|n|n>stats.closed)||
       p.day_yek!=day(t)||
       !stats.modeled_realized_usdt.is_finite()||
       PaperMachine::new(p.assumptions.clone()).is_err(){return false}
    p.positions.iter().all(|(name,m)|position_valid(m,name,mono))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawEnvelope{
    payload:Box<serde_json::value::RawValue>,
    sha256:String,
}
/// R181-R185: local same-path serialization only. Does NOT confer
/// REAL permissions or replace broker-side recovery evidence.
struct PaperCheckpointLock(File);
impl PaperCheckpointLock{
 fn acquire(path:&Path)->Result<Self,CheckpointError>{
  let parent=path.parent().ok_or(deny("checkpoint parent missing"))?;
  let name=path.file_name().and_then(|x|x.to_str())
   .ok_or(deny("bad checkpoint name"))?;
  if name.is_empty()||name.len()>140{return Err(deny("bad checkpoint name"))}
  let lock=parent.join(format!(".{name}.r181.lock"));
  let f=OpenOptions::new().read(true).write(true).create(true).mode(0o600)
   .custom_flags(libc::O_NOFOLLOW|libc::O_CLOEXEC).open(&lock)?;
  let before=f.metadata()?;
  if !before.is_file()||before.nlink()!=1||before.len()!=0||before.mode()&0o077!=0{
   return Err(deny("unsafe checkpoint lock"))
  }
  if unsafe{libc::flock(f.as_raw_fd(),libc::LOCK_EX)}!=0{
   return Err(CheckpointError::Io(io::Error::last_os_error()))
  }
  let after=fs::symlink_metadata(&lock)?;
  if !after.file_type().is_file()||after.dev()!=before.dev()
   ||after.ino()!=before.ino()||after.nlink()!=1
   ||after.len()!=0||after.mode()&0o077!=0{
   return Err(deny("checkpoint lock replaced"))
  }
  Ok(Self(f))
 }
}
impl Drop for PaperCheckpointLock{
 fn drop(&mut self){unsafe{libc::flock(self.0.as_raw_fd(),libc::LOCK_UN);}}
}
fn read(path:&Path)->Result<Envelope,CheckpointError>{
    let _lock=PaperCheckpointLock::acquire(path)?;
    read_unlocked(path)
}
fn read_unlocked(path:&Path)->Result<Envelope,CheckpointError>{
    let mut f=OpenOptions::new().read(true)
       .custom_flags(libc::O_NOFOLLOW|libc::O_CLOEXEC).open(path)?;
    let meta=f.metadata()?;
    if !meta.file_type().is_file()||meta.nlink()!=1||meta.mode()&0o077!=0
     ||meta.len()==0||meta.len()>MAX_BYTES{
     return Err(deny("unsafe paper checkpoint file"))
    }
    let mut bytes=Vec::with_capacity(meta.len() as usize);
    Read::by_ref(&mut f).take(MAX_BYTES+1).read_to_end(&mut bytes)?;
    if bytes.len() as u64!=meta.len(){return Err(deny("changed checkpoint size"))}
    let current=fs::symlink_metadata(path)?;
    if !current.file_type().is_file()||current.dev()!=meta.dev()
     ||current.ino()!=meta.ino()||current.len()!=meta.len()
     ||current.nlink()!=1||current.mode()&0o077!=0{
     return Err(deny("checkpoint path changed during read"))
    }
    // Verify the exact JSON payload bytes that were fsynced by the writer.
    // Re-serializing deserialized floating-point numbers can produce a
    // different byte sequence even when the modeled values are identical.
    let raw:RawEnvelope=serde_json::from_slice(&bytes)
        .map_err(|_|deny("malformed raw checkpoint envelope"))?;
    let raw_hash:String=sha256(raw.payload.get().as_bytes()).iter()
        .map(|b|format!("{b:02x}")).collect();
    if raw_hash!=raw.sha256{
        return Err(deny("checkpoint payload SHA256 mismatch"))
    }
    let payload:Payload=serde_json::from_str(raw.payload.get())
        .map_err(|_|deny("malformed typed paper payload"))?;
    let value=Envelope{payload,sha256:raw.sha256};
    if value.payload.schema!=FORMAT||value.payload.revision==0{
        return Err(deny("unknown paper checkpoint schema or revision"))
    }
    if !portfolio_valid(&value.payload.portfolio,
                        value.payload.updated_ms,value.payload.monotonic_ns){
        return Err(deny("invalid paper position economics or phases"))
    }
    Ok(value)
}
pub fn save_checkpoint(path:&Path,portfolio:&PaperPortfolio,
     revision:u64,mono:u64,completed:bool)->Result<(),CheckpointError>{
    if revision==0{return Err(deny("zero checkpoint revision"))}
    let timestamp=now_ms();
    if !portfolio_valid(portfolio,timestamp,mono){
        return Err(deny("paper state invalid for durable virtual checkpoint"))
    }
    let payload=Payload{schema:FORMAT.into(),revision,updated_ms:timestamp,
        monotonic_ns:mono,boot_id:boot()?,completed,portfolio:portfolio.clone()};
    let contents=serde_json::to_vec(&Envelope{
        sha256:digest(&payload)?,payload
    }).map_err(|_|deny("checkpoint JSON failed"))?;
    if contents.len()>MAX_BYTES as usize{return Err(deny("oversized checkpoint"))}
    let parent=path.parent().ok_or(deny("checkpoint parent missing"))?;
    fs::create_dir_all(parent)?;
    let _writer_lock=PaperCheckpointLock::acquire(path)?;
    // R182: reject stale/duplicate revisions; do not let a second writer
    // overwrite an already published modeled PAPER state.
    match fs::symlink_metadata(path){
     Ok(m)=>{
      if !m.file_type().is_file(){return Err(deny("unsafe checkpoint target"))}
      let old=read_unlocked(path)?;
      if old.payload.revision.checked_add(1)!=Some(revision)
       ||old.payload.boot_id!=boot()?||old.payload.monotonic_ns>mono{
       return Err(deny("checkpoint revision or boot mismatch"))
      }
     },
     Err(e) if e.kind()==io::ErrorKind::NotFound=>{},
     Err(e)=>return Err(CheckpointError::Io(e))
    }
    let tmp=path.with_extension("pending");
    let mut f=OpenOptions::new().create_new(true).write(true)
        .mode(0o600).custom_flags(libc::O_NOFOLLOW|libc::O_CLOEXEC).open(&tmp)?;
    f.write_all(&contents)?;
    f.sync_all()?;drop(f);
    fs::rename(&tmp,path)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}
#[derive(Debug)]
pub struct RestoredVirtualPaper{
    pub portfolio:PaperPortfolio,
    pub previous_revision:u64,
    pub previous_day:i64,
    pub carried_open_positions:usize,
}
pub fn load_planned_handoff(path:&Path,now_mono:u64,
    next_day_entries:u64)->Result<RestoredVirtualPaper,CheckpointError>{
    if next_day_entries>MAX_DAILY_PAPER as u64{return Err(deny("invalid day budget"))}
    let e=read(path)?;
    if !e.payload.completed{return Err(deny("unsealed or interrupted paper episode"))}
    if e.payload.boot_id!=boot()?{
       return Err(deny("restarted OS boot requires quarantine"))
    }
    let now=now_ms();
    if now<e.payload.updated_ms||now-e.payload.updated_ms>FRESH_MS||
       now_mono<e.payload.monotonic_ns{
       return Err(deny("stale or backward-clock virtual checkpoint"))
    }
    let now_day=day(now);
    if now_day<e.payload.portfolio.day_yek||
       now_day>e.payload.portfolio.day_yek+1{
       return Err(deny("unexpected EKB day jump"))
    }
    // R183: caller-supplied PAPER_PREVIOUS_DAY_ENTRIES is not an
    // independently authenticated ledger. On the SAME EKB day it must
    // never decrease the persisted counter and grant fresh entries.
    // Next-day budgets still need independent operational verification.
    if now_day==e.payload.portfolio.day_yek
      &&next_day_entries<e.payload.portfolio.stats.entries{
       return Err(deny("same-day virtual daily quota rollback"))
    }
    let mut state=e.payload.portfolio;
    let count=state.positions.len();
    let previous_day=state.day_yek;
    state.day_yek=now_day;
    state.stats.entries=next_day_entries;
    if !portfolio_valid(&state,now,now_mono){
        return Err(deny("restored paper positions invalid"))
    }
    Ok(RestoredVirtualPaper{
        portfolio:state,previous_revision:e.payload.revision,
        previous_day,carried_open_positions:count,
    })
}
// R1063: READ-ONLY cold rehydration review when a valid attended R1059
// planned stop exceeds the legacy 180 second automatic-handoff window.
// Absolutely not an automatic startup path. Source and archive must agree,
// the same host boot and day must survive, with a local exact phase journal.
// No network, new entry permits, or checkpoint timestamp rewriting.
pub fn r1063_stopped_extended_preview(
  checkpoint:&Path, paper_root:&Path, archive:&Path, receipt:&Path,
  now_mono:u64, next_day_entries:u64,
)->Result<RestoredVirtualPaper,CheckpointError>{
 let proof=crate::native_r1062_sealed_stop_bundle_review::
   inspect_stopped_bundle(archive,paper_root,receipt)
   .map_err(|_|deny("R1063 original stopped-source attestation refused"))?;
 if proof["ok"]!=true||proof["legacy_fresh_180s"]!=false||
    proof["auto_resume_authorized"]!=false||
    proof["real_orders_authorized"]!=false{
   return Err(deny("R1063 proof cannot grant live resume"))
 }
 if checkpoint!=paper_root.join("portfolio_checkpoint.json"){
   return Err(deny("R1063 wrong source checkpoint"))
 }
 if next_day_entries>MAX_DAILY_PAPER as u64{
   return Err(deny("R1063 bad restored day budget"))
 }
 let e=read(checkpoint)?;
 if !e.payload.completed||e.payload.boot_id!=boot()?{
   return Err(deny("R1063 stopped checkpoint unsealed or changed boot"))
 }
 let now=now_ms();
 if now<e.payload.updated_ms||
    now-e.payload.updated_ms>12*60*60*1000||
    now_mono<e.payload.monotonic_ns{
   return Err(deny("R1063 invalid clock or aged beyond explicit research window"))
 }
 // Day changes can imply untracked funding / fee accrual. Do not
 // reinterpret a past-day virtual position as a current opening.
 if day(now)!=e.payload.portfolio.day_yek||
    next_day_entries<e.payload.portfolio.stats.entries{
   return Err(deny("R1063 day crossing or quota rollback"))
 }
 let mut state=e.payload.portfolio;
 if proof["modeled_entries"].as_u64()!=Some(state.stats.entries)||
    proof["modeled_closed"].as_u64()!=Some(state.stats.closed)||
    proof["modeled_open"].as_u64()!=Some(state.positions.len() as u64){
   return Err(deny("R1063 counts differ from original stop proof"))
 }
 if !portfolio_valid(&state,now,now_mono){
   return Err(deny("R1063 untrusted restored position economics"))
 }
 // keep cold_boot_unknown=true after deserialization. This intentionally
 // blocks all NEW PAPER entries while preserving legacy modeled exits.
 if !state.cold_boot_unknown{
   return Err(deny("R1063 cold model is unexpectedly open to new entries"))
 }
 let count=state.positions.len();
 let old_day=state.day_yek;
 state.stats.entries=next_day_entries;
 Ok(RestoredVirtualPaper{portfolio:state,previous_revision:e.payload.revision,
      previous_day:old_day,carried_open_positions:count})
}
/// R1078: research-only cross-day, *in-memory* quarantined view of
/// an operator-sealed modeled portfolio. No cold startup approval.
/// Historic counters belong to PREVIOUS day and are NOT reset or
/// represented as independently reconciled current-day accounting.
pub fn r1078_stopped_cross_day_quarantine_preview(
 checkpoint:&Path,paper_root:&Path,archive:&Path,receipt:&Path,
 now_mono:u64,
)->Result<RestoredVirtualPaper,CheckpointError>{
 let proof=crate::native_r1062_sealed_stop_bundle_review::
  inspect_stopped_bundle(archive,paper_root,receipt)
  .map_err(|_|deny("R1078 exact stopped archive attestation refused"))?;
 if proof["ok"]!=true||proof["auto_resume_authorized"]!=false||
   proof["real_orders_authorized"]!=false||proof["legacy_fresh_180s"]!=false{
  return Err(deny("R1078 no cross-day quarantine authority"))
 }
 if checkpoint!=paper_root.join("portfolio_checkpoint.json"){
  return Err(deny("R1078 wrong original checkpoint path"))
 }
 let envelope=read(checkpoint)?;
 if !envelope.payload.completed||envelope.payload.boot_id!=boot()?{
   return Err(deny("R1078 stopped model unsealed or changed boot"))
 }
 let now=now_ms();
 let original_day=envelope.payload.portfolio.day_yek;
 if now<envelope.payload.updated_ms||
    now-envelope.payload.updated_ms>30*60*60*1000||
    now_mono<envelope.payload.monotonic_ns||
    day(now)!=original_day+1 {
  return Err(deny("R1078 cross-day/bound/monotonic guard"))
 }
 let mut frozen=envelope.payload.portfolio;
 // Validate ORIGINAL model and original financial/temporal fields
 // at its sealed wall-clock day, not at the current day.
 if !portfolio_valid(&frozen,envelope.payload.updated_ms,now_mono)||
    proof["modeled_entries"].as_u64()!=Some(frozen.stats.entries)||
    proof["modeled_closed"].as_u64()!=Some(frozen.stats.closed)||
    proof["modeled_open"].as_u64()!=Some(frozen.positions.len() as u64){
  return Err(deny("R1078 exact source portfolio does not match journal"))
 }
 let before=serde_json::to_vec(&frozen)
    .map_err(|_|deny("R1078 cannot serialize source portfolio"))?;
 let old_day=frozen.day_yek;
 let count=frozen.positions.len();
 // Only this ephemeral object moves to current day, so the existing
 // `on_frame` day gate can be studied. No source checkpoint changes,
 // no new entries, no actions, no invented daily trading budget.
 frozen.day_yek=day(now);
 if !frozen.cold_boot_unknown||
   frozen.stats.entries>MAX_DAILY_PAPER as u64||
   frozen.max_daily_entries!=MAX_DAILY_PAPER as u64{
  return Err(deny("R1078 quarantine/limit violated"))
 }
 // Validate all positions are still exact after the day pointer change.
 let mut old_for_compare=frozen.clone();
 old_for_compare.day_yek=old_day;
 if serde_json::to_vec(&old_for_compare)
      .map_err(|_|deny("R1078 encode compare failed"))?!=before{
  return Err(deny("R1078 portfolio mutated during quarantine"))
 }
 Ok(RestoredVirtualPaper{portfolio:frozen,previous_revision:envelope.payload.revision,
    previous_day:old_day,carried_open_positions:count})
}
/// R80: READ-ONLY inspection of an interrupted virtual PAPER episode.
/// Diagnostic only: cannot clear quarantine or authorize a restored position.
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct InterruptedVirtualExposure{
 pub checkpoint_revision:u64,
 pub observed_entries:u64,
 pub observed_closed:u64,
 pub open_bases:Vec<String>,
}
pub fn inspect_interrupted_virtual_exposure(
 path:&Path,expected_day:i64,session_start_ms:i64
)->Result<InterruptedVirtualExposure,CheckpointError>{
 let e=read(path)?;
 // A prior session's rotating checkpoint is not evidence for this crash.
 // This path NEVER calls load_planned_handoff or grants any resume right.
 if e.payload.completed||e.payload.portfolio.day_yek!=expected_day||
    session_start_ms<=0||e.payload.updated_ms<session_start_ms||
    e.payload.updated_ms>now_ms().saturating_add(2_000)||
    e.payload.boot_id!=boot()?{
    return Err(deny("interrupted checkpoint not from this session"))
 }
 Ok(InterruptedVirtualExposure{
    checkpoint_revision:e.payload.revision,
    observed_entries:e.payload.portfolio.stats.entries,
    observed_closed:e.payload.portfolio.stats.closed,
    open_bases:e.payload.portfolio.positions.keys().cloned().collect(),
 })
}
pub fn inspect_planned_seal(path:&Path,expected_day:i64,
    expected_entries:u64)->Result<(),CheckpointError>{
    let e=read(path)?;
    if !e.payload.completed||e.payload.portfolio.day_yek!=expected_day||
       e.payload.portfolio.stats.entries!=expected_entries{
        return Err(deny("paper checkpoint not matching successful run"))
    }
    if e.payload.boot_id!=boot()?{return Err(deny("boot changed"))}
    Ok(())
}
/// Cross-check the independently fsynced rotating checkpoint and modeled
/// four-leg journal at a cold boundary. Detects either write winning alone,
/// including hedge/cover transitions that do NOT change entry/open counters.
/// This is modeled PAPER consistency only, never exchange fill authority.
pub fn inspect_checkpoint_model_alignment(
    checkpoint:&Path,journal:&Path,expected_session:&str,
    require_sealed:bool,
)->Result<(),CheckpointError>{
    let e=read(checkpoint)?;
    let audit=crate::native_virtual_four_leg_provenance_review::
        inspect_model_journal(journal)
        .map_err(|_|deny("modeled four-leg journal invalid or absent"))?;
    if expected_session.is_empty()||audit.session!=expected_session||
       e.payload.completed!=audit.sealed||
       (require_sealed&&!e.payload.completed)||
       e.payload.portfolio.stats.entries!=audit.entries||
       e.payload.portfolio.stats.closed!=audit.closed||
       e.payload.portfolio.stats.modeled_realized_usdt.to_bits()
          !=audit.modeled_realized_bits||
       e.payload.portfolio.positions.len()!=audit.open||
       audit.model_phase_by_base.len()!=audit.open{
        return Err(deny("checkpoint and modeled journal disagree"))
    }
    // R968: compare the COMPLETE current modeled phase after-images,
    // including quote/base/timestamps and amounts, not only stage names.
    // This remains a read-only local integrity diagnostic, never an
    // authorization for automated crash restoration or REAL orders.
    let phase_images=crate::native_virtual_four_leg_provenance_review::
        inspect_modeled_exact_open_phase_payloads(journal)
        .map_err(|_|deny("full modeled phase journal invalid"))?;
    if phase_images.len()!=e.payload.portfolio.positions.len(){
        return Err(deny("exact modeled position count differs"))
    }
    for (base,machine) in &e.payload.portfolio.positions{
        let actual_image=serde_json::to_value(&machine.phase)
            .map_err(|_|deny("cannot serialize exact modeled phase"))?;
        if phase_images.get(base)!=Some(&actual_image){
            return Err(deny("checkpoint modeled position amount/leg mismatch"))
        }
        let actual=match &machine.phase{
            PaperPhase::AwaitingHedge(_)=>"SPOT_BUY_MODELED",
            PaperPhase::Hedged(_)=>"FUTURES_SHORT_MODELED",
            PaperPhase::AwaitingSpotExit{..}=>"FUTURES_COVER_MODELED",
            _=>return Err(deny("invalid checkpoint virtual position phase")),
        };
        if audit.model_phase_by_base.get(base).map(String::as_str)!=Some(actual){
            return Err(deny("checkpoint and journal four-leg phase mismatch"))
        }
    }
    Ok(())
}
#[cfg(test)]
mod tests{
 use super::*;
 use crate::native_paper_market_execution_review::{
    PaperAssumptions,PaperPosition,PaperMachine,PaperPhase
 };
 fn location()->std::path::PathBuf{
    use std::sync::atomic::{AtomicU64,Ordering};
    static SERIAL:AtomicU64=AtomicU64::new(1);
    std::env::temp_dir().join(format!("native_paper_checkpoint_{}_{}_{}",
       std::process::id(),now_ms(),SERIAL.fetch_add(1,Ordering::Relaxed)))
 }
 fn state()->PaperPortfolio{
    let mut p=PaperPortfolio::new(now_ms()).unwrap();
    p.stats.entries=1;
    let t=crate::transport::monotonic_ns()-5_000_000;
    let mut m=PaperMachine::new(PaperAssumptions::default()).unwrap();
    m.phase=PaperPhase::Hedged(PaperPosition{
       base:"ONE".into(),signal_ns:t,spot_buy_ns:t+1,
       signal_estimated_net_bps:60.0,spot_gross_base:1.0,
       spot_delivered_base:1.0,spot_paid_quote:10.0,
       futures_short_ns:Some(t+2),
       futures_short_proceeds_quote:Some(10.2),futures_short_base:Some(1.0),
       hedge_entry_estimated_net_bps:Some(60.0)
    });
    p.positions.insert("ONE".into(),m);p
 }
 #[test]
 fn r80_unsealed_checkpoint_reports_virtual_exposure_without_restoring(){
    let file=location();
    let p=state();
    save_checkpoint(&file,&p,8,crate::transport::monotonic_ns(),false).unwrap();
    let episode_started=now_ms()-5_000;
    let report=inspect_interrupted_virtual_exposure(
        &file,p.day_yek,episode_started).unwrap();
    assert_eq!(report.checkpoint_revision,8);
    assert_eq!(report.observed_entries,1);
    assert_eq!(report.observed_closed,0);
    assert_eq!(report.open_bases,vec!["ONE".to_string()]);
    assert!(load_planned_handoff(&file,crate::transport::monotonic_ns(),1).is_err());
    assert!(inspect_interrupted_virtual_exposure(&file,p.day_yek+1,episode_started).is_err());
    save_checkpoint(&file,&p,9,crate::transport::monotonic_ns(),true).unwrap();
    assert!(inspect_interrupted_virtual_exposure(&file,p.day_yek,episode_started).is_err());
    fs::remove_file(&file).unwrap();
 }
 #[test]
 fn checkpoint_and_four_leg_journal_reject_both_partial_write_orders(){
    use crate::native_virtual_four_leg_provenance_review::ModeledFourLegJournal;
    let checkpoint=location();
    let journal=checkpoint.with_extension("modeled.jsonl");
    let before=state();
    let mut writer=ModeledFourLegJournal::start(
        &journal,"session_1",&before,1).unwrap();
    save_checkpoint(&checkpoint,&before,1,
        crate::transport::monotonic_ns(),false).unwrap();
    assert!(inspect_checkpoint_model_alignment(
        &checkpoint,&journal,"session_1",false).is_ok());
    assert!(inspect_checkpoint_model_alignment(
        &checkpoint,&journal,"session_1",true).is_err());

    let mut after=before.clone();
    let hedged=match &before.positions["ONE"].phase{
        PaperPhase::Hedged(pos)=>pos.clone(),_=>panic!("expected modeled hedge"),
    };
    let cover_ns=crate::transport::monotonic_ns();
    after.positions.get_mut("ONE").unwrap().phase=PaperPhase::AwaitingSpotExit{
        pos:hedged,cover_quote:10.0,cover_ns,close_estimate_quote:0.2,
    };
    after.stats.futures_covers+=1;
    // Power failure between checkpoint fsync and journal fsync:
    // counters remain IDENTICAL, only the modeled leg phase differs.
    save_checkpoint(&checkpoint,&after,2,
        crate::transport::monotonic_ns(),false).unwrap();
    assert!(inspect_checkpoint_model_alignment(
        &checkpoint,&journal,"session_1",false).is_err());
    writer.record_change("ONE",&before,&after).unwrap();
    assert!(inspect_checkpoint_model_alignment(
        &checkpoint,&journal,"session_1",false).is_ok());
    // The other direction: journal is ahead, rotating checkpoint lags.
    save_checkpoint(&checkpoint,&before,3,
        crate::transport::monotonic_ns(),false).unwrap();
    assert!(inspect_checkpoint_model_alignment(
        &checkpoint,&journal,"session_1",false).is_err());
    save_checkpoint(&checkpoint,&after,4,
        crate::transport::monotonic_ns(),false).unwrap();
    assert!(inspect_checkpoint_model_alignment(
        &checkpoint,&journal,"session_1",false).is_ok());
    // A forged checkpoint may preserve ALL counters and all four-leg
    // phases but change accumulated modeled PnL. Its own SHA is valid;
    // the independent journal amount must nevertheless reject it.
    let mut forged=after.clone();
    forged.stats.modeled_realized_usdt+=0.25;
    save_checkpoint(&checkpoint,&forged,5,
        crate::transport::monotonic_ns(),false).unwrap();
    assert!(inspect_checkpoint_model_alignment(
        &checkpoint,&journal,"session_1",false).is_err());
    save_checkpoint(&checkpoint,&after,6,
        crate::transport::monotonic_ns(),false).unwrap();
    assert!(inspect_checkpoint_model_alignment(
        &checkpoint,&journal,"session_1",false).is_ok());
    // A falsely claimed completed checkpoint cannot substitute for seal.
    save_checkpoint(&checkpoint,&after,7,
        crate::transport::monotonic_ns(),true).unwrap();
    assert!(inspect_checkpoint_model_alignment(
        &checkpoint,&journal,"session_1",true).is_err());
    writer.seal(&after).unwrap();
    assert!(inspect_checkpoint_model_alignment(
        &checkpoint,&journal,"session_1",true).is_ok());
    assert!(inspect_checkpoint_model_alignment(
        &checkpoint,&journal,"session_other",true).is_err());
    fs::remove_file(&checkpoint).unwrap();
    fs::remove_file(&journal).unwrap();
 }
 #[test]fn sealed_handoff_retains_modeled_hedge_and_daily_guard(){
    let path=location();let p=state();
    let mono=crate::transport::monotonic_ns();
    save_checkpoint(&path,&p,2,mono,false).unwrap();
    assert!(load_planned_handoff(&path,mono+1,1).is_err());
    save_checkpoint(&path,&p,3,mono,true).unwrap();
    let restored=load_planned_handoff(
       &path,crate::transport::monotonic_ns(),499).unwrap();
    assert_eq!(restored.carried_open_positions,1);
    assert_eq!(restored.portfolio.stats.entries,499);
    assert!(matches!(restored.portfolio.positions["ONE"].phase,PaperPhase::Hedged(_)));
    assert!(load_planned_handoff(&path,mono+1,501).is_err());
    let old=fs::read(&path).unwrap();
    let mut broken=old.clone();let n=broken.len()/2;broken[n]^=1;
    fs::write(&path,broken).unwrap();
    assert!(load_planned_handoff(&path,
        crate::transport::monotonic_ns(),499).is_err());
    let _=fs::remove_file(path);
 }
 #[test]fn complex_realistic_float_state_roundtrips_against_raw_signed_bytes(){
    let path=location();
    let mut p=state();
    p.stats.entries=41;
    p.stats.closed=39;
    p.stats.modeled_realized_usdt=0.8975153416053947;
    let mut second=p.positions["ONE"].clone();
    if let PaperPhase::Hedged(ref mut pos)=second.phase{
        pos.base="NEAR".into();
        pos.signal_estimated_net_bps=53.56649820931275;
        pos.spot_gross_base=0.4127159273000001;
        pos.spot_delivered_base=0.4127159273000001;
        pos.spot_paid_quote=9.999999999999998;
        pos.futures_short_proceeds_quote=Some(10.00153245311332);
        pos.hedge_entry_estimated_net_bps=Some(53.56649820931275);
    }
    p.positions.insert("NEAR".into(),second);
    save_checkpoint(&path,&p,206,crate::transport::monotonic_ns(),true).unwrap();
    inspect_planned_seal(&path,p.day_yek,41).unwrap();
    let r=load_planned_handoff(&path,crate::transport::monotonic_ns(),41).unwrap();
    assert_eq!(r.carried_open_positions,2);
    assert_eq!(r.previous_revision,206);
    let mut bytes=fs::read(&path).unwrap();
    let i=bytes.windows(8).position(|w|w==b"53.56649").unwrap();
    bytes[i]=b'6';
    fs::write(&path,bytes).unwrap();
    assert!(inspect_planned_seal(&path,p.day_yek,41).is_err());
    let _=fs::remove_file(path);
 }
 #[test]fn invalid_synthetic_position_fails_closed(){
    let path=location();
    let mut p=state();
    if let PaperPhase::Hedged(ref mut pos)=p.positions.get_mut("ONE").unwrap().phase {
       pos.spot_paid_quote=f64::NAN;
    }
    assert!(save_checkpoint(&path,&p,1,
        crate::transport::monotonic_ns(),true).is_err());
    assert!(!path.exists());
 }
 #[test]fn r181_private_checkpoint_fd_rejects_links_and_group_readable(){
    use std::os::unix::fs::{symlink,PermissionsExt};
    let path=location();let p=state();let mono=crate::transport::monotonic_ns();
    save_checkpoint(&path,&p,1,mono,true).unwrap();
    assert_eq!(fs::metadata(&path).unwrap().permissions().mode()&0o777,0o600);
    let clone=location();fs::hard_link(&path,&clone).unwrap();
    assert!(inspect_planned_seal(&path,p.day_yek,1).is_err());
    fs::remove_file(clone).unwrap();
    fs::set_permissions(&path,fs::Permissions::from_mode(0o644)).unwrap();
    assert!(inspect_planned_seal(&path,p.day_yek,1).is_err());
    fs::set_permissions(&path,fs::Permissions::from_mode(0o600)).unwrap();
    assert!(inspect_planned_seal(&path,p.day_yek,1).is_ok());
    let target=location();
    fs::rename(&path,&target).unwrap();
    symlink(&target,&path).unwrap();
    assert!(inspect_planned_seal(&path,p.day_yek,1).is_err());
    assert!(save_checkpoint(&path,&p,2,mono+2,true).is_err());
    assert!(inspect_planned_seal(&target,p.day_yek,1).is_ok());
    fs::remove_file(&path).unwrap();fs::remove_file(&target).unwrap();
 }
 #[test]fn r182_two_writers_cannot_publish_duplicate_revision(){
    use std::sync::{Arc,Barrier};
    let path=location();let p=state();let mono=crate::transport::monotonic_ns();
    save_checkpoint(&path,&p,1,mono,false).unwrap();
    let barrier=Arc::new(Barrier::new(3));
    let tasks=(0..2).map(|_|{
      let path=path.clone();let p=p.clone();let barrier=barrier.clone();
      std::thread::spawn(move||{
        barrier.wait();
        save_checkpoint(&path,&p,2,mono+10,true).is_ok()
      })
    }).collect::<Vec<_>>();
    barrier.wait();
    let successes=tasks.into_iter().map(|t|t.join().unwrap()).filter(|r|*r).count();
    assert_eq!(successes,1);
    assert_eq!(read(&path).unwrap().payload.revision,2);
    assert!(save_checkpoint(&path,&p,2,mono+12,true).is_err());
    assert!(save_checkpoint(&path,&p,4,mono+12,true).is_err());
    fs::remove_file(path).unwrap();
 }
 #[test]fn r183_same_day_restore_never_lowers_source_daily_counter(){
    let path=location();let mut p=state();let mono=crate::transport::monotonic_ns();
    p.stats.entries=490;
    save_checkpoint(&path,&p,1,mono,true).unwrap();
    assert!(load_planned_handoff(&path,mono+1,0).is_err());
    assert!(load_planned_handoff(&path,mono+1,489).is_err());
    let recovered=load_planned_handoff(&path,mono+1,490).unwrap();
    assert_eq!(recovered.portfolio.stats.entries,490);
    assert_eq!(recovered.portfolio.max_daily_entries,500);
    assert!(load_planned_handoff(&path,mono+1,501).is_err());
    fs::remove_file(path).unwrap();
 }
 #[test]fn r184_orphan_pending_is_not_truncated_or_sealed(){
    let path=location();let p=state();let mono=crate::transport::monotonic_ns();
    save_checkpoint(&path,&p,1,mono,false).unwrap();
    let pending=path.with_extension("pending");
    fs::write(&pending,b"aborted snapshot stage").unwrap();
    assert!(save_checkpoint(&path,&p,2,mono+1,true).is_err());
    assert_eq!(fs::read(&pending).unwrap(),b"aborted snapshot stage");
    assert_eq!(read(&path).unwrap().payload.revision,1);
    assert!(load_planned_handoff(&path,mono+2,1).is_err());
    fs::remove_file(&pending).unwrap();
    save_checkpoint(&path,&p,2,mono+3,true).unwrap();
    assert!(load_planned_handoff(&path,mono+4,1).is_ok());
    fs::remove_file(path).unwrap();
 }
 #[test]fn r184_lock_symlink_and_hardlink_fail_closed(){
    use std::os::unix::fs::symlink;
    let path=location();let p=state();let mono=crate::transport::monotonic_ns();
    let lock=path.parent().unwrap().join(
      format!(".{}.r181.lock",path.file_name().unwrap().to_str().unwrap()));
    let victim=location();fs::write(&victim,b"keep").unwrap();
    symlink(&victim,&lock).unwrap();
    assert!(save_checkpoint(&path,&p,1,mono,false).is_err());
    assert_eq!(fs::read(&victim).unwrap(),b"keep");
    fs::remove_file(&lock).unwrap();
    drop(PaperCheckpointLock::acquire(&path).unwrap());
    let alias=location();fs::hard_link(&lock,&alias).unwrap();
    assert!(save_checkpoint(&path,&p,1,mono,false).is_err());
    fs::remove_file(&alias).unwrap();
    save_checkpoint(&path,&p,1,mono,false).unwrap();
    fs::remove_file(path).unwrap();fs::remove_file(victim).unwrap();
 }
 /// Build a SELF-CONSISTENT SHA256 envelope with deliberately forged
 /// modeled accounting. This proves a checksum alone is not policy proof.
 fn forge_valid_hash(path:&Path,changed:impl FnOnce(&mut serde_json::Value)){
    let mut wire:serde_json::Value=
      serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
    changed(&mut wire["payload"]["portfolio"]);
    let payload=serde_json::to_vec(&wire["payload"]).unwrap();
    wire["sha256"]=serde_json::Value::String(
      openssl::sha::sha256(&payload).iter().map(|b|format!("{b:02x}")).collect());
    fs::write(path,serde_json::to_vec(&wire).unwrap()).unwrap();
 }
 fn forged_scenario(change:impl FnOnce(&mut serde_json::Value)){
    let path=location();let p=state();let mono=crate::transport::monotonic_ns();
    save_checkpoint(&path,&p,1,mono,true).unwrap();
    forge_valid_hash(&path,change);
    assert!(read(&path).is_err());
    assert!(inspect_planned_seal(&path,p.day_yek,1).is_err());
    assert!(load_planned_handoff(&path,mono+1,1).is_err());
    fs::remove_file(path).unwrap();
 }
 #[test]fn r186_forged_source_quota_3000_cannot_enter_compiled_500_lane(){
    forged_scenario(|p|p["max_daily_entries"]=3000.into());
 }
 #[test]fn r187_forged_concurrent_quota_cannot_inflate_legacy_model(){
    forged_scenario(|p|p["max_concurrent_positions"]=11.into());
 }
 #[test]fn r188_hash_valid_unaccounted_open_cannot_reclaim_day_budget(){
    forged_scenario(|p|p["stats"]["entries"]=0.into());
 }
 #[test]fn r189_hash_valid_closed_greater_than_entries_denied(){
    forged_scenario(|p|p["stats"]["closed"]=2.into());
 }
 #[test]fn r190_hash_valid_closed_win_loss_overflow_denied(){
    forged_scenario(|p|{
      p["stats"]["closed"]=1.into();
      p["stats"]["closed_wins"]=u64::MAX.into();
      p["stats"]["closed_losses"]=1.into();
    });
 }
 include!("native_r973_incident_bundle_tests.rs");
}

include!("native_r972_interrupted_modeled_bundle_review.rs");
