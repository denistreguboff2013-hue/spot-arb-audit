//! R1116: private offline durable R37 queue checkpoint with exact model floor.
//! Never sends Telegram. No network, API keys, installed PAPER or exchange calls.
use crate::{
 native_r1115_verified_model_telegram_bridge::r1115_plan,
 native_paper_notice_queue_review::PaperWatchState,
 native_trading_config::NativeTradingConfig,
 native_virtual_four_leg_provenance_review::inspect_model_trade_notices,
};
use serde::{Serialize,Deserialize};
use std::{fs::{self,File,OpenOptions},io::Write,
  os::unix::{fs::{MetadataExt,OpenOptionsExt,PermissionsExt},
   io::AsRawFd},path::Path};
const SCRATCH:&str="/mnt/spotarb_db_test/astra_p001_r874_test_scratch_20261025";
const FILE:&str="r1116_durable_model_notice.json";
const STAGED:&str="r1116_durable_model_notice.tmp";
const LOCK:&str="r1116_durable_model_notice.lock";
#[derive(Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope{
 schema:String,
 verified_seq:u64,verified_hash:String,source_session:String,
 config_sha256:String,
 state:PaperWatchState,
}
struct Guard(File);
impl Drop for Guard{fn drop(&mut self){
 unsafe{libc::flock(self.0.as_raw_fd(),libc::LOCK_UN);}
}}
fn secure(root:&Path)->Result<(),String>{
 let parent=root.parent().ok_or("R1116 parent missing")?;
 if parent!=Path::new(SCRATCH)||
   !root.file_name().and_then(|s|s.to_str())
     .is_some_and(|s|s.starts_with("astra_r1116_")&&s.len()>=21||
                   s.starts_with("astra_r1115_")&&s.len()>=21||
                   (s.starts_with("astra_r671_file_r1087_R1119_")&&
                    s.len()>=36&&
                    std::env::var("ASTRA_R1119_FULL_RUST_PAPER")
                      .ok().as_deref()==Some("YES"))){
   return Err("R1116 nonprivate scratch root".into())
 }
 let stat=fs::symlink_metadata(root).map_err(|_|"R1116 root missing")?;
 if !stat.file_type().is_dir()||stat.uid()!=unsafe{libc::geteuid()}||
    stat.permissions().mode()&0o077!=0{
    return Err("R1116 owner/private directory denied".into())
 }
 Ok(())
}
fn file_safe(p:&Path,required:bool)->Result<bool,String>{
 match fs::symlink_metadata(p){
  Ok(m)=>{
   // The modeled journal may legitimately exceed 1 MiB at hundreds of
   // 4-stage virtual trades/day. The bounded queue is a separate file.
   let cap=if p.file_name()==Some(std::ffi::OsStr::new("model.jsonl")){
       8_388_608
   }else{1_048_576};
   if !m.file_type().is_file()||m.uid()!=unsafe{libc::geteuid()}||
      m.nlink()!=1||m.permissions().mode()&0o077!=0||
      m.len()>cap{
      return Err("R1116 unsafe or oversized state file".into())
   }Ok(true)
  },
  Err(e) if e.kind()==std::io::ErrorKind::NotFound&&!required=>Ok(false),
  _=>Err("R1116 expected state file unavailable".into()),
 }
}
fn current_floor(model:&Path,at_seq:u64)->Result<String,String>{
 let (tip,_)=inspect_model_trade_notices(model)
     .map_err(|_|"R1116 invalid model ledger")?;
 if tip.sequence<at_seq||at_seq==0{return Err("R1116 journal truncated".into())}
 let bytes=fs::read(model).map_err(|_|"R1116 journal read failed")?;
 if bytes.len()>8_388_608||bytes.last()!=Some(&b'\n'){
   return Err("R1116 invalid journal size/tail".into())
 }
 for line in bytes.split(|b|*b==b'\n').filter(|x|!x.is_empty()){
    let row:serde_json::Value=serde_json::from_slice(line)
       .map_err(|_|"R1116 journal row malformed")?;
    if row["seq"].as_u64()==Some(at_seq){
      let hash=row["checksum"].as_str().ok_or("R1116 no hash")?;
      if hash.len()!=64{return Err("R1116 hash malformed".into())}
      return Ok(hash.to_owned())
    }
 }
 Err("R1116 missing journal watermark".into())
}
/// Synchronous private single-writer commit, no network. If the staged
/// file already exists, REFUSE recovery pending manual review.
pub fn r1116_commit(
 root:&Path,model:&Path,cfg:&NativeTradingConfig,now_ms:u64,
 simulate_crash_before_publish:bool,
)->Result<(u64,usize),String>{
 secure(root)?;
 if model.parent()!=Some(root){return Err("R1116 model escaped private root".into())}
 file_safe(model,true)?;
 let lp=root.join(LOCK);
 file_safe(&lp,false)?;
 let lock=OpenOptions::new().read(true).write(true).create(true).mode(0o600)
   .custom_flags(libc::O_NOFOLLOW|libc::O_CLOEXEC)
   .open(&lp).map_err(|_|"R1116 local lock unavailable")?;
 if unsafe{libc::flock(lock.as_raw_fd(),libc::LOCK_EX)}!=0{
   return Err("R1116 exclusive lock failed".into())
 }
 let _held=Guard(lock);
 file_safe(&lp,true)?;
 let target=root.join(FILE);let tmp=root.join(STAGED);
 if file_safe(&tmp,false)?{return Err("R1116 orphan stage quarantined".into())}
 let prior=if file_safe(&target,false)?{
   let bytes=fs::read(&target).map_err(|_|"R1116 state unreadable")?;
   let env:Envelope=serde_json::from_slice(&bytes)
      .map_err(|_|"R1116 damaged envelope")?;
   if env.schema!="SPOTARB/ASTRA/R1116/LOCAL-MODEL-NOTICE/V1"||
      env.config_sha256!=cfg.sha256||
      env.verified_seq==0||
      env.verified_hash!=current_floor(model,env.verified_seq)?||
      env.state.oldstyle.as_ref().map(|s|s.verified_journal_seq)!=Some(env.verified_seq)||
      env.state.cursor.last_seen.session!=env.source_session{
     return Err("R1116 persisted cursor/source floor conflict".into())
   }
   Some(env)
 }else{None};
 if prior.is_none(){
   let (source,_)=inspect_model_trade_notices(model).map_err(|_|"R1116 initial model invalid")?;
   if source.sequence!=1||source.sealed||source.entries!=0||source.open!=0{
     return Err("R1116 missing queue, unsafe baseline".into())
   }
 }
 let proposed=r1115_plan(model,prior.as_ref().map(|e|&e.state),cfg,now_ms)?;
 let (tip,_)=inspect_model_trade_notices(model)
    .map_err(|_|"R1116 journal changed after planning")?;
 let floor=current_floor(model,tip.sequence)?;
 if proposed.next.oldstyle.as_ref().map(|o|o.verified_journal_seq)!=Some(tip.sequence)||
    proposed.next.cursor.last_seen.session!=tip.session||
    proposed.next.pending.len()>128{
   return Err("R1116 invalid proposed queue tip".into())
 }
 let envelope=Envelope{
   schema:"SPOTARB/ASTRA/R1116/LOCAL-MODEL-NOTICE/V1".into(),
   verified_seq:tip.sequence,verified_hash:floor,
   source_session:tip.session,config_sha256:cfg.sha256.clone(),
   state:proposed.next,
 };
 let payload=serde_json::to_vec_pretty(&envelope)
    .map_err(|_|"R1116 queue JSON encode failure")?;
 if payload.len()>1_048_576{return Err("R1116 queue full".into())}
 let mut f=OpenOptions::new().write(true).create_new(true).mode(0o600)
   .custom_flags(libc::O_NOFOLLOW|libc::O_CLOEXEC)
   .open(&tmp).map_err(|_|"R1116 staged checkpoint blocked")?;
 f.write_all(&payload).map_err(|_|"R1116 staged write failed")?;
 f.sync_all().map_err(|_|"R1116 staged fsync failed")?;
 File::open(root).and_then(|d|d.sync_all())
   .map_err(|_|"R1116 stage directory fsync failed")?;
 if simulate_crash_before_publish{
    return Err("R1116 injected crash before publish, stage quarantined".into())
 }
 if current_floor(model,envelope.verified_seq)?!=envelope.verified_hash||
    inspect_model_trade_notices(model).map_err(|_|"R1116 source readback invalid")?.0.sequence!=
         envelope.verified_seq {
    return Err("R1116 source drift before publication".into())
 }
 fs::rename(&tmp,&target).map_err(|_|"R1116 atomic publish failed")?;
 File::open(root).and_then(|d|d.sync_all())
   .map_err(|_|"R1116 published directory fsync failed")?;
 let round:Envelope=serde_json::from_slice(&fs::read(&target)
    .map_err(|_|"R1116 roundtrip read failed")?)
    .map_err(|_|"R1116 persisted queue damaged")?;
 if round.verified_hash!=envelope.verified_hash||
    round.state!=envelope.state{
    return Err("R1116 queue roundtrip mismatch".into())
 }
 Ok((round.verified_seq,proposed.newly_queued.len()))
}
#[cfg(test)]
mod tests{
 use super::*;
 use crate::{
  native_paper_market_execution_review::{
   PaperAssumptions,PaperMachine,PaperPhase,PaperPosition
  },native_paper_portfolio_review::PaperPortfolio,
  native_virtual_four_leg_provenance_review::ModeledFourLegJournal,
 };
 use std::os::unix::fs::DirBuilderExt;
 fn root()->std::path::PathBuf{
  let p=Path::new(SCRATCH).join(format!("astra_r1116_{}_{}",
      std::process::id(),crate::transport::monotonic_ns()));
  fs::DirBuilder::new().mode(0o700).create(&p).unwrap();p
 }
 fn cfg()->NativeTradingConfig{
  NativeTradingConfig::parse(include_str!("../config/native_trading_paper_only_r1076.yaml")).unwrap()
 }
 fn pos()->PaperPosition{PaperPosition{
   base:"ONE".into(),signal_ns:1,spot_buy_ns:1,
   signal_estimated_net_bps:52.3,spot_gross_base:4.0,
   spot_delivered_base:4.0,spot_paid_quote:10.0,
   futures_short_ns:None,futures_short_proceeds_quote:None,
   futures_short_base:None,hedge_entry_estimated_net_bps:None
 }}
 const NOW:u64=1_792_080_231_000;
 fn start(root:&Path)->(PaperPortfolio,ModeledFourLegJournal){
  let p=PaperPortfolio::new(NOW as i64).unwrap();
  let session=format!("session_{}_{}",(NOW+18_000_000)/86_400_000,NOW/1000);
  let j=ModeledFourLegJournal::start(
      &root.join("model.jsonl"),&session,&p,0).unwrap();
  // The real service has UMask=0077. Simulate that exact
  // installed-file permission without weakening the R1116 guard.
  fs::set_permissions(root.join("model.jsonl"),
    fs::Permissions::from_mode(0o600)).unwrap();
  (p,j)
 }
 #[test]
 fn r1116_real_writer_persistent_cursor_no_replays_and_rejects_truncation(){
  let root=root();let model=root.join("model.jsonl");
  let (mut p,mut j)=start(&root);let cfg=cfg();
  assert_eq!(r1116_commit(&root,&model,&cfg,NOW+1000,false).unwrap(),(1,0));
  let mut before=p.clone();p.stats.entries=1;
  p.positions.insert("ONE".into(),PaperMachine{
    config:PaperAssumptions::default(),
    phase:PaperPhase::AwaitingHedge(pos())
  });
  j.record_change("ONE",&before,&p).unwrap();
  before=p.clone();let mut h=pos();
  h.futures_short_ns=Some(2);h.futures_short_base=Some(4.0);
  h.futures_short_proceeds_quote=Some(10.2);
  h.hedge_entry_estimated_net_bps=Some(180.0);
  p.stats.futures_hedges=1;
  p.positions.get_mut("ONE").unwrap().phase=PaperPhase::Hedged(h.clone());
  j.record_change("ONE",&before,&p).unwrap();
  assert_eq!(r1116_commit(&root,&model,&cfg,NOW+2000,false).unwrap(),(3,1));
  assert_eq!(r1116_commit(&root,&model,&cfg,NOW+2500,false).unwrap(),(3,0));
  // Deleting a proven queue must not silently rebaseline live stages.
  let original=fs::read(root.join(FILE)).unwrap();
  fs::remove_file(root.join(FILE)).unwrap();
  assert!(r1116_commit(&root,&model,&cfg,NOW+2550,false).is_err());
  assert!(!root.join(FILE).exists());
  fs::write(root.join(FILE),original).unwrap();
  fs::set_permissions(root.join(FILE),
       fs::Permissions::from_mode(0o600)).unwrap();
  before=p.clone();p.stats.futures_covers=1;
  p.positions.get_mut("ONE").unwrap().phase=PaperPhase::AwaitingSpotExit{
     pos:h,cover_quote:10.0,cover_ns:3,close_estimate_quote:0.2
  };
  j.record_change("ONE",&before,&p).unwrap();
  before=p.clone();p.stats.closed=1;p.stats.modeled_realized_usdt=0.3;
  p.positions.remove("ONE");j.record_change("ONE",&before,&p).unwrap();
  assert_eq!(r1116_commit(&root,&model,&cfg,NOW+3000,false).unwrap(),(5,1));
  let current=fs::read(root.join(FILE)).unwrap();
  let state:Envelope=serde_json::from_slice(&current).unwrap();
  assert_eq!(state.state.pending.len(),2);
  assert_eq!(r1116_commit(&root,&model,&cfg,NOW+3500,false).unwrap(),(5,0));
  // Actual model ledger corrupted after successful queue fsync:
  // no more state publication, old queue remains byte-exact.
  let mut corrupted=fs::read(&model).unwrap();corrupted.push(b'x');
  fs::write(&model,corrupted).unwrap();
  let old=fs::read(root.join(FILE)).unwrap();
  assert!(r1116_commit(&root,&model,&cfg,NOW+4000,false).is_err());
  assert_eq!(fs::read(root.join(FILE)).unwrap(),old);
  fs::remove_dir_all(root).unwrap();
 }
 #[test]
 fn r1116_injected_crash_before_rename_quarantines_stage_and_old_queue(){
  let root=root();let model=root.join("model.jsonl");
  let (p,_j)=start(&root);let cfg=cfg();
  assert_eq!(p.stats.entries,0);
  assert_eq!(r1116_commit(&root,&model,&cfg,NOW+1000,false).unwrap(),(1,0));
  let original=fs::read(root.join(FILE)).unwrap();
  assert!(r1116_commit(&root,&model,&cfg,NOW+1500,true).is_err());
  assert_eq!(fs::read(root.join(FILE)).unwrap(),original);
  assert!(root.join(STAGED).is_file());
  assert!(r1116_commit(&root,&model,&cfg,NOW+2000,false).is_err());
  assert_eq!(fs::read(root.join(FILE)).unwrap(),original);
  fs::remove_dir_all(root).unwrap();
 }
 #[test]
 fn r1116_journal_size_compatible_with_high_volume_paper(){
   let root=root();let model=root.join("model.jsonl");
   let f=OpenOptions::new().create_new(true).write(true).mode(0o600)
       .open(&model).unwrap();
   f.set_len(2_000_000).unwrap();
   assert_eq!(file_safe(&model,true).unwrap(),true);
   f.set_len(8_388_609).unwrap();
   assert!(file_safe(&model,true).is_err());
   fs::remove_dir_all(root).unwrap();
 }
 #[test]
 fn r1116_rejects_symlink_model_and_corrupt_committed_envelope(){
  let root=root();let model=root.join("model.jsonl");
  let (_p,_j)=start(&root);let cfg=cfg();
  assert_eq!(r1116_commit(&root,&model,&cfg,NOW+1000,false).unwrap(),(1,0));
  fs::write(root.join(FILE),b"partial-crash").unwrap();
  assert!(r1116_commit(&root,&model,&cfg,NOW+1100,false).is_err());
  fs::remove_file(&model).unwrap();
  std::os::unix::fs::symlink("/dev/null",&model).unwrap();
  assert!(r1116_commit(&root,&model,&cfg,NOW+1200,false).is_err());
  fs::remove_dir_all(root).unwrap();
 }
}
