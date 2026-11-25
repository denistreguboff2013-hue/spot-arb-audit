//! R153: local, same-boot, source-locked virtual PAPER review handoff.
//! Strictly research-only. No private orders, installed PAPER cutover, exchange
//! fill truth, independent rollback witness or automatic crash restoration.
use crate::native_paper_strategy_frame_review::{SupervisedStrategyPaper,StrategyReviewImage};
use crate::native_trading_config::NativeTradingConfig;
use openssl::sha::sha256;
use serde::{Serialize,Deserialize};
use std::{fs::{self,File,OpenOptions},io::{self,Read,Write},
 os::{fd::AsRawFd,unix::fs::{MetadataExt,OpenOptionsExt}},path::Path,
 time::{SystemTime,UNIX_EPOCH}};
const SCHEMA:&str="native-source-paper-planned-review-r153-v1";
const MAX_BYTES:u64=512*1024;
const MAX_AGE_MS:i64=180_000;
#[derive(Debug)]
pub enum SourceCheckpointError{Io(io::Error),Denied(&'static str)}
impl From<io::Error> for SourceCheckpointError{
 fn from(e:io::Error)->Self{Self::Io(e)}
}
fn denied(x:&'static str)->SourceCheckpointError{SourceCheckpointError::Denied(x)}
#[derive(Clone,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
struct Payload{
 schema:String,revision:u64,saved_wall_ms:i64,saved_monotonic_ns:u64,
 boot_id:String,completed:bool,source_sha256:String,image:StrategyReviewImage,
}
#[derive(Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope{payload:Payload,sha256:String}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawEnvelope{payload:Box<serde_json::value::RawValue>,sha256:String}
fn hash(raw:&[u8])->String{
 sha256(raw).iter().map(|b|format!("{b:02x}")).collect()
}
fn wall()->i64{
 SystemTime::now().duration_since(UNIX_EPOCH)
  .unwrap_or_default().as_millis() as i64
}
fn boot_id()->Result<String,SourceCheckpointError>{
 let id=fs::read_to_string("/proc/sys/kernel/random/boot_id")?;
 let id=id.trim().to_ascii_lowercase();
 if id.len()!=36||id.chars().enumerate().any(|(i,c)|
  if [8,13,18,23].contains(&i){c!='-'}else{!c.is_ascii_hexdigit()}){
  return Err(denied("untrusted boot identity"))
 }
 Ok(id)
}
fn sha_valid(s:&str)->bool{
 s.len()==64&&s.bytes().all(|x|x.is_ascii_hexdigit()&&!x.is_ascii_uppercase())
}
/// R180 local same-directory lock serializes a REVIEW checkpoint's
/// revision read, staged publication and planned clean load. It cannot
/// act as a rollback witness across machines or prove exchange truth.
struct SourceCheckpointLock(File);
impl SourceCheckpointLock{
 fn acquire(path:&Path)->Result<Self,SourceCheckpointError>{
  let parent=path.parent().ok_or(denied("checkpoint lock parent missing"))?;
  let name=path.file_name().and_then(|x|x.to_str())
   .ok_or(denied("checkpoint lock name invalid"))?;
  if name.is_empty()||name.len()>140||name.contains(['/', '\\']){
   return Err(denied("checkpoint lock name invalid"))
  }
  let lock=parent.join(format!(".{name}.r180.lock"));
  let f=OpenOptions::new().read(true).write(true).create(true)
   .mode(0o600).custom_flags(libc::O_NOFOLLOW|libc::O_CLOEXEC)
   .open(&lock)?;
  let m=f.metadata()?;
  if !m.file_type().is_file()||m.nlink()!=1||m.mode()&0o077!=0
   ||m.len()>0{return Err(denied("unsafe checkpoint lock"))}
  if unsafe{libc::flock(f.as_raw_fd(),libc::LOCK_EX)}!=0{
   return Err(SourceCheckpointError::Io(io::Error::last_os_error()))
  }
  // Check after acquiring flock: a pathname swap while waiting may
  // otherwise serialize a different inode from a second caller.
  let current=fs::symlink_metadata(&lock)?;
  if !current.file_type().is_file()||current.dev()!=m.dev()
   ||current.ino()!=m.ino()||current.nlink()!=1
   ||current.mode()&0o077!=0||current.len()!=0{
   return Err(denied("checkpoint lock replaced"))
  }
  Ok(Self(f))
 }
}
impl Drop for SourceCheckpointLock{
 fn drop(&mut self){unsafe{libc::flock(self.0.as_raw_fd(),libc::LOCK_UN);}}
}
fn read(path:&Path)->Result<Payload,SourceCheckpointError>{
 // R179: a separate symlink_metadata + fs::read pair is a pathname race.
 // Open a single FD without symlink traversal and verify its exact inode
 // still names the checkpoint path when the bounded read completes.
 let mut file=OpenOptions::new().read(true)
  .custom_flags(libc::O_NOFOLLOW|libc::O_CLOEXEC).open(path)?;
 let meta=file.metadata()?;
 if !meta.file_type().is_file()||meta.len()==0||meta.len()>MAX_BYTES
  ||meta.nlink()!=1||meta.mode()&0o077!=0{
  return Err(denied("unsafe source checkpoint file"))
 }
 let mut bytes=Vec::with_capacity(meta.len() as usize);
 Read::by_ref(&mut file).take(MAX_BYTES+1).read_to_end(&mut bytes)?;
 if bytes.is_empty()||bytes.len() as u64>MAX_BYTES
  ||bytes.len() as u64!=meta.len(){
  return Err(denied("unsafe source checkpoint size"))
 }
 let current=fs::symlink_metadata(path)?;
 if !current.file_type().is_file()||current.dev()!=meta.dev()
  ||current.ino()!=meta.ino()||current.len()!=bytes.len() as u64
  ||current.nlink()!=1||current.mode()&0o077!=0{
  return Err(denied("checkpoint pathname changed during read"))
 }
 let parsed:RawEnvelope=serde_json::from_slice(&bytes)
  .map_err(|_|denied("malformed checkpoint envelope"))?;
 if parsed.sha256!=hash(parsed.payload.get().as_bytes()){
  return Err(denied("checkpoint payload changed"))
 }
 let p:Payload=serde_json::from_str(parsed.payload.get())
  .map_err(|_|denied("malformed checkpoint payload"))?;
 if p.schema!=SCHEMA||p.revision==0||p.saved_wall_ms<=0
  ||p.saved_monotonic_ns==0||!sha_valid(&p.source_sha256)
  ||p.image.real_trading_authorized
  ||p.image.config_sha256!=p.source_sha256
  ||!crate::native_paper_strategy_frame_review::
    strategy_review_image_shape_valid(&p.image){
  return Err(denied("invalid source checkpoint policy or model"))
 }
 Ok(p)
}
/// An interrupted checkpoint can be recorded for diagnostics but must NEVER
/// automatically grant planned carryover. Completed=true only at clean stop.
pub fn save_source_paper_review(
 path:&Path,paper:&SupervisedStrategyPaper,revision:u64,
 mono_ns:u64,completed:bool
)->Result<(),SourceCheckpointError>{
 if revision==0||mono_ns==0{return Err(denied("invalid save revision or clock"))}
 let dir=path.parent().ok_or(denied("checkpoint directory missing"))?;
 fs::create_dir_all(dir)?;
 let _writer_lock=SourceCheckpointLock::acquire(path)?;
 let image=paper.replay_image().map_err(|_|denied("source profile mutated"))?;
 if !sha_valid(&image.config_sha256)||
  !crate::native_paper_strategy_frame_review::
   strategy_review_image_shape_valid(&image){
  return Err(denied("invalid review model image"))
 }
 match fs::symlink_metadata(path){
  Ok(prev)=>{
   if !prev.file_type().is_file(){return Err(denied("unsafe checkpoint target"))}
   let old=read(path)?;
   if old.revision.checked_add(1)!=Some(revision)||
    old.source_sha256!=image.config_sha256||
    old.boot_id!=boot_id()?||old.saved_monotonic_ns>mono_ns{
    return Err(denied("source/revision/clock migration denied"))
   }
  },
  Err(e) if e.kind()==io::ErrorKind::NotFound=>{
   if revision!=1{return Err(denied("first checkpoint must be revision one"))}
  },
  Err(e)=>return Err(SourceCheckpointError::Io(e)),
 }
 let payload=Payload{schema:SCHEMA.into(),revision,saved_wall_ms:wall(),
  saved_monotonic_ns:mono_ns,boot_id:boot_id()?,completed,
  source_sha256:image.config_sha256.clone(),image};
 let raw=serde_json::to_vec(&payload)
  .map_err(|_|denied("checkpoint JSON encoding failed"))?;
 let wire=serde_json::to_vec(&Envelope{payload,sha256:hash(&raw)})
  .map_err(|_|denied("checkpoint envelope encoding failed"))?;
 if wire.len()>MAX_BYTES as usize{return Err(denied("oversized checkpoint"))}
 let tmp=path.with_extension("r153-pending");
 let mut f=OpenOptions::new().write(true).create_new(true)
  .mode(0o600).custom_flags(libc::O_NOFOLLOW|libc::O_CLOEXEC)
  .open(&tmp)?;
 f.write_all(&wire)?;f.sync_all()?;drop(f);
 fs::rename(&tmp,path)?;
 File::open(dir)?.sync_all()?;
 Ok(())
}
/// Only a clean, same-boot handoff of the *exact* source SHA can restore
/// virtual modeled state. Not a crash recovery or REAL-entry authority.
pub fn load_source_paper_planned_review(
 path:&Path,cfg:NativeTradingConfig,now_wall_ms:i64,mono_ns:u64,
 expected_revision:u64
)->Result<SupervisedStrategyPaper,SourceCheckpointError>{
 let _reader_lock=SourceCheckpointLock::acquire(path)?;
 let p=read(path)?;
 if !p.completed||expected_revision==0||p.revision!=expected_revision||
  !cfg.source_projection_unchanged()||cfg.sha256!=p.source_sha256{
  return Err(denied("unsealed, stale revision or different YAML"))
 }
 let real_wall=wall();
 if now_wall_ms<real_wall-5000||now_wall_ms>real_wall+5000
  ||now_wall_ms<p.saved_wall_ms
  ||now_wall_ms-p.saved_wall_ms>MAX_AGE_MS
  ||mono_ns<p.saved_monotonic_ns||p.boot_id!=boot_id()?{
  return Err(denied("time/boot freshness not established"))
 }
 SupervisedStrategyPaper::from_replay_image(cfg,now_wall_ms,p.image)
  .map_err(|_|denied("source policy or virtual lifecycle restore denied"))
}

#[cfg(test)]
mod tests{
 use super::*;
 use crate::native_paper_market_execution_review::{PaperMachine,PaperPhase,PaperPosition};
 use crate::transport::monotonic_ns;
 use std::sync::atomic::{AtomicU64,Ordering};
 static NUMBER:AtomicU64=AtomicU64::new(1);
 fn path()->std::path::PathBuf{
  std::env::temp_dir().join(format!("spotarb-r153-{}-{}-{}.json",
   std::process::id(),wall(),NUMBER.fetch_add(1,Ordering::Relaxed)))
 }
 fn config()->NativeTradingConfig{
  let orig=include_str!("../config/native_trading_operator_baseline_r64.yaml");
  let txt=orig.replacen("scale_in_hints:\n  enabled: true",
   "scale_in_hints:\n  enabled: false",1)
   .replacen("analytics:\n  enabled: true",
    "analytics:\n  enabled: false",1);
  assert_ne!(txt,orig);
  NativeTradingConfig::parse(&txt).unwrap()
 }
 fn seeded(mono:u64)->SupervisedStrategyPaper{
  let mut s=SupervisedStrategyPaper::new(config(),wall()).unwrap();
  let mut machine=PaperMachine::new(s.portfolio.assumptions.clone()).unwrap();
  machine.phase=PaperPhase::Hedged(PaperPosition{
   base:"ONE".into(),signal_ns:mono-20,spot_buy_ns:mono-10,
   signal_estimated_net_bps:80.0,
   spot_gross_base:0.1,spot_delivered_base:0.1,spot_paid_quote:10.0,
   futures_short_ns:Some(mono-5),futures_short_base:Some(0.1),
   futures_short_proceeds_quote:Some(10.2),
   hedge_entry_estimated_net_bps:Some(80.0),
  });
  s.portfolio.positions.insert("ONE".into(),machine);
  s.portfolio.stats.entries=1;
  s.portfolio.stats.futures_hedges=1;
  s
 }
 #[test]
 fn r153_planned_source_locked_recovery_preserves_virtual_hedge(){
  let p=path();let t=monotonic_ns();
  let s=seeded(t);
  assert_eq!(s.portfolio.max_daily_entries,3000);
  save_source_paper_review(&p,&s,1,t,false).unwrap();
  assert!(load_source_paper_planned_review(&p,config(),wall(),t+1,1).is_err());
  save_source_paper_review(&p,&s,2,t+2,true).unwrap();
  let restored=load_source_paper_planned_review(&p,config(),wall(),t+3,2)
   .unwrap();
  assert_eq!(restored.portfolio.positions.len(),1);
  assert_eq!(restored.portfolio.stats.entries,1);
  assert_eq!(restored.portfolio.max_daily_entries,3000);
  assert_eq!(restored.replay_image().unwrap().config_sha256,
   s.replay_image().unwrap().config_sha256);
  assert!(load_source_paper_planned_review(&p,config(),wall(),t+3,1).is_err());
  assert!(load_source_paper_planned_review(&p,config(),wall(),t-1,2).is_err());
  fs::remove_file(&p).unwrap();
 }
 #[test]
 fn r153_yaml_and_data_tamper_are_denied_even_with_matching_temporal(){
  let p=path();let t=monotonic_ns();let s=seeded(t);
  save_source_paper_review(&p,&s,1,t,true).unwrap();
  let orig=include_str!("../config/native_trading_operator_baseline_r64.yaml");
  let modified=orig.replacen("scale_in_hints:\n  enabled: true",
   "scale_in_hints:\n  enabled: false",1)
    .replacen("analytics:\n  enabled: true",
    "analytics:\n  enabled: false",1)
    .replacen("entry_bps: 42","entry_bps: 95",1);
  assert_ne!(config().sha256,NativeTradingConfig::parse(&modified).unwrap().sha256);
  assert!(load_source_paper_planned_review(&p,
   NativeTradingConfig::parse(&modified).unwrap(),wall(),t+1,1).is_err());
  let mut bytes=fs::read(&p).unwrap();
  let at=bytes.iter().position(|c|*c==b'O').unwrap();
  bytes[at]=b'X';
  fs::write(&p,&bytes).unwrap();
  assert!(load_source_paper_planned_review(&p,config(),wall(),t+2,1).is_err());
  fs::remove_file(&p).unwrap();
 }
 #[test]
 fn r153_stale_revision_symlink_and_unsealed_migration_denied(){
  let p=path();let t=monotonic_ns();let s=seeded(t);
  assert!(save_source_paper_review(&p,&s,2,t,false).is_err());
  save_source_paper_review(&p,&s,1,t,false).unwrap();
  assert!(save_source_paper_review(&p,&s,1,t,false).is_err());
  assert!(save_source_paper_review(&p,&s,3,t+2,true).is_err());
  assert!(load_source_paper_planned_review(&p,config(),wall()+200_000,t+3,1).is_err());
  fs::remove_file(&p).unwrap();
  let target=path();fs::write(&target,b"NEVER FOLLOW").unwrap();
  std::os::unix::fs::symlink(&target,&p).unwrap();
  assert!(save_source_paper_review(&p,&s,1,t,false).is_err());
  assert!(load_source_paper_planned_review(&p,config(),wall(),t+1,1).is_err());
  assert_eq!(fs::read(&target).unwrap(),b"NEVER FOLLOW");
  fs::remove_file(&p).unwrap();fs::remove_file(&target).unwrap();
 }
 #[test]
 fn r179_checkpoint_created_owner_only_and_planned_restore_still_works(){
  use std::os::unix::fs::PermissionsExt;
  let p=path();let t=monotonic_ns();let s=seeded(t);
  save_source_paper_review(&p,&s,1,t,true).unwrap();
  assert_eq!(fs::metadata(&p).unwrap().permissions().mode()&0o777,0o600);
  let restored=load_source_paper_planned_review(&p,config(),wall(),t+1,1).unwrap();
  assert_eq!(restored.portfolio.max_daily_entries,3000);
  assert_eq!(restored.portfolio.stats.entries,1);
  fs::remove_file(p).unwrap();
 }
 #[test]
 fn r179_checkpoint_hardlink_denied_before_read_or_rewrite(){
  let p=path();let t=monotonic_ns();let s=seeded(t);
  save_source_paper_review(&p,&s,1,t,true).unwrap();
  let linked=path();
  fs::hard_link(&p,&linked).unwrap();
  assert!(load_source_paper_planned_review(&p,config(),wall(),t+1,1).is_err());
  assert!(save_source_paper_review(&p,&s,2,t+2,true).is_err());
  fs::remove_file(linked).unwrap();
  assert!(load_source_paper_planned_review(&p,config(),wall(),t+3,1).is_ok());
  fs::remove_file(p).unwrap();
 }
 #[test]
 fn r179_checkpoint_group_access_denied_even_with_valid_hash(){
  use std::os::unix::fs::PermissionsExt;
  let p=path();let t=monotonic_ns();let s=seeded(t);
  save_source_paper_review(&p,&s,1,t,true).unwrap();
  fs::set_permissions(&p,fs::Permissions::from_mode(0o640)).unwrap();
  assert!(load_source_paper_planned_review(&p,config(),wall(),t+1,1).is_err());
  assert!(save_source_paper_review(&p,&s,2,t+2,true).is_err());
  fs::set_permissions(&p,fs::Permissions::from_mode(0o600)).unwrap();
  assert!(load_source_paper_planned_review(&p,config(),wall(),t+3,1).is_ok());
  fs::remove_file(p).unwrap();
 }
 #[test]
 fn r179_truncated_and_oversized_checkpoint_file_denied(){
  let p=path();let t=monotonic_ns();let s=seeded(t);
  save_source_paper_review(&p,&s,1,t,true).unwrap();
  fs::write(&p,b"").unwrap();
  assert!(load_source_paper_planned_review(&p,config(),wall(),t+1,1).is_err());
  assert!(save_source_paper_review(&p,&s,2,t+2,true).is_err());
  fs::write(&p,vec![b'Z';(MAX_BYTES+1) as usize]).unwrap();
  assert!(load_source_paper_planned_review(&p,config(),wall(),t+3,1).is_err());
  assert!(save_source_paper_review(&p,&s,2,t+4,true).is_err());
  fs::remove_file(p).unwrap();
 }
 #[test]
 fn r179_staged_symlink_trap_does_not_modify_target(){
  let p=path();let t=monotonic_ns();let s=seeded(t);
  let victim=path();
  fs::write(&victim,b"do not change").unwrap();
  let tmp=p.with_extension("r153-pending");
  std::os::unix::fs::symlink(&victim,&tmp).unwrap();
  assert!(save_source_paper_review(&p,&s,1,t,true).is_err());
  assert_eq!(fs::read(&victim).unwrap(),b"do not change");
  assert!(!p.exists());
  fs::remove_file(tmp).unwrap();fs::remove_file(victim).unwrap();
 }
 #[test]
 fn r179_new_yek_day_never_resets_old_virtual_quota_or_restores(){
  let t=monotonic_ns();let mut source=seeded(t);
  let original_day=source.portfolio.day_yek;
  let entries=source.portfolio.stats.entries;
  let quota=source.portfolio.max_daily_entries;
  let shifted=wall()+86_400_000;
  assert!(source.portfolio.day_check(shifted).is_err());
  assert_eq!(source.portfolio.day_yek,original_day);
  assert_eq!(source.portfolio.stats.entries,entries);
  assert_eq!(source.portfolio.max_daily_entries,quota);
  let image=source.replay_image().unwrap();
  assert!(SupervisedStrategyPaper::from_replay_image(
    config(),shifted,image).is_err());
 }
 fn lock_path(p:&Path)->std::path::PathBuf{
  p.parent().unwrap().join(format!(
   ".{}.r180.lock",p.file_name().unwrap().to_str().unwrap()))
 }
 #[test]
 fn r180_concurrent_same_revision_only_one_checkpoint_writer(){
  use std::sync::{Arc,Barrier};
  let p=path();let t=monotonic_ns();
  save_source_paper_review(&p,&seeded(t),1,t,false).unwrap();
  let gate=Arc::new(Barrier::new(3));
  let handles=(0..2).map(|_|{
   let p=p.clone();let gate=gate.clone();
   std::thread::spawn(move||{
    let s=seeded(t);
    gate.wait();
    save_source_paper_review(&p,&s,2,t+2,true).is_ok()
   })
  }).collect::<Vec<_>>();
  gate.wait();
  let successes=handles.into_iter().map(|h|h.join().unwrap()).filter(|ok|*ok).count();
  assert_eq!(successes,1,"same revision cannot be published twice");
  assert!(load_source_paper_planned_review(&p,config(),wall(),t+3,2).is_ok());
  fs::remove_file(&p).unwrap();
 }
 #[test]
 fn r180_interprocess_flock_is_exclusive_not_just_thread_guard(){
  let p=path();
  let lock=SourceCheckpointLock::acquire(&p).unwrap();
  let f=OpenOptions::new().read(true).write(true).open(lock_path(&p)).unwrap();
  let status=unsafe{libc::flock(f.as_raw_fd(),libc::LOCK_EX|libc::LOCK_NB)};
  assert_eq!(status,-1);
  assert_eq!(io::Error::last_os_error().raw_os_error(),Some(libc::EWOULDBLOCK));
  drop(lock);
  assert_eq!(unsafe{libc::flock(f.as_raw_fd(),libc::LOCK_EX|libc::LOCK_NB)},0);
  assert_eq!(unsafe{libc::flock(f.as_raw_fd(),libc::LOCK_UN)},0);
 }
 #[test]
 fn r180_preexisting_lock_symlink_denies_without_modifying_target(){
  let p=path();let victim=path();let target=lock_path(&p);
  fs::write(&victim,b"never modify").unwrap();
  std::os::unix::fs::symlink(&victim,&target).unwrap();
  assert!(save_source_paper_review(&p,&seeded(monotonic_ns()),1,
   monotonic_ns(),false).is_err());
  assert!(!p.exists());
  assert_eq!(fs::read(&victim).unwrap(),b"never modify");
  fs::remove_file(target).unwrap();fs::remove_file(victim).unwrap();
 }
 #[test]
 fn r180_lock_hardlink_or_group_access_is_denied(){
  use std::os::unix::fs::PermissionsExt;
  let p=path();drop(SourceCheckpointLock::acquire(&p).unwrap());
  let lock=lock_path(&p);let alias=path();
  fs::hard_link(&lock,&alias).unwrap();
  let t=monotonic_ns();let s=seeded(t);
  assert!(save_source_paper_review(&p,&s,1,t,false).is_err());
  fs::remove_file(&alias).unwrap();
  fs::set_permissions(&lock,fs::Permissions::from_mode(0o644)).unwrap();
  assert!(save_source_paper_review(&p,&s,1,t,false).is_err());
  fs::set_permissions(&lock,fs::Permissions::from_mode(0o600)).unwrap();
  save_source_paper_review(&p,&s,1,t,false).unwrap();
  fs::remove_file(p).unwrap();
 }
 #[test]
 fn r180_crash_orphan_pending_cannot_become_sealed_recovery(){
  let p=path();let t=monotonic_ns();let s=seeded(t);
  save_source_paper_review(&p,&s,1,t,false).unwrap();
  let pending=p.with_extension("r153-pending");
  fs::write(&pending,b"crash before atomic rename").unwrap();
  assert!(save_source_paper_review(&p,&s,2,t+2,true).is_err());
  assert!(load_source_paper_planned_review(&p,config(),wall(),t+3,1).is_err());
  assert_eq!(read(&p).unwrap().revision,1);
  assert!(!read(&p).unwrap().completed);
  assert_eq!(fs::read(&pending).unwrap(),b"crash before atomic rename");
  fs::remove_file(&pending).unwrap(); // explicit test-only cleanup
  save_source_paper_review(&p,&s,2,t+4,true).unwrap();
  assert!(load_source_paper_planned_review(&p,config(),wall(),t+5,2).is_ok());
  fs::remove_file(p).unwrap();
 }
}
