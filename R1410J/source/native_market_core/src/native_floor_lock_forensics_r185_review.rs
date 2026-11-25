// R185: operator FORENSIC lock view; never auto-unlock an orphan lock.
// Lock files are local /tmp research fixtures. A process exiting by SIGKILL
// intentionally leaves them until explicit external manual reconciliation.
#[derive(Debug,Clone,PartialEq,Eq)]
pub enum LockStateR185{
 Absent,
 ManualReview{inode:u64,owner_uid:u32,bytes:u64},
}
pub fn inspect_floor_lock_r185(path:&Path)
 ->Result<LockStateR185,PaperAdapterErrorR156>{
 use std::os::unix::fs::MetadataExt;
 floor_guard_r171(path)?;
 let lock=path.with_extension("lock");
 let meta=match fs::symlink_metadata(&lock){
  Ok(m)=>m,
  Err(e) if e.kind()==std::io::ErrorKind::NotFound=>return Ok(LockStateR185::Absent),
  Err(_)=>return Err(PaperAdapterErrorR156::Path)
 };
 if !meta.file_type().is_file()||meta.nlink()!=1||
   meta.mode()&0o077!=0||meta.uid()!=unsafe{libc::geteuid()}||
   meta.len()>128{return Err(PaperAdapterErrorR156::Path)}
 Ok(LockStateR185::ManualReview{
  inode:meta.ino(),owner_uid:meta.uid(),bytes:meta.len()
 })
}
pub fn operator_lock_guidance_r185(s:&LockStateR185)->&'static str{
 match s{
  LockStateR185::Absent=>"No orphaned lock observed in private test root",
  LockStateR185::ManualReview{..}=>"LOCKED: investigate process lifetime, independent witness and DB2 before explicit manual remediation; NO automatic deletion",
 }
}
#[cfg(test)]
mod r185_tests{
 use super::*;
 use std::{os::unix::fs::{DirBuilderExt,symlink},sync::atomic::{AtomicUsize,Ordering}};
 static N:AtomicUsize=AtomicUsize::new(0);
 fn root()->(PathBuf,PathBuf){
  let dir=std::env::temp_dir().join(format!("astra_r171_isolated_r185_{}_{}",
   std::process::id(),N.fetch_add(1,Ordering::Relaxed)));
  fs::DirBuilder::new().mode(0o700).create(&dir).unwrap();
  let floor=dir.join("external-witness-floor.json");
  (dir,floor)
 }
 #[test]fn r185_orphan_lock_requires_review_not_automatic_removal(){
  let (dir,floor)=root();
  assert_eq!(inspect_floor_lock_r185(&floor).unwrap(),LockStateR185::Absent);
  let guard=acquire_floor_lock_r171(&floor).unwrap();
  let observed=inspect_floor_lock_r185(&floor).unwrap();
  assert!(matches!(observed,LockStateR185::ManualReview{..}));
  assert!(operator_lock_guidance_r185(&observed).contains("NO automatic deletion"));
  assert!(floor.with_extension("lock").exists());
  drop(guard);
  assert_eq!(inspect_floor_lock_r185(&floor).unwrap(),LockStateR185::Absent);
  fs::remove_dir_all(dir).unwrap();
 }
 #[test]fn r185_symlink_is_not_trusted_as_private_lock(){
  let (dir,floor)=root();
  let destination=dir.join("ordinary");
  fs::write(&destination,b"do-not-touch").unwrap();
  symlink(&destination,floor.with_extension("lock")).unwrap();
  assert_eq!(inspect_floor_lock_r185(&floor).err(),Some(PaperAdapterErrorR156::Path));
  assert_eq!(fs::read(destination).unwrap(),b"do-not-touch");
  fs::remove_dir_all(dir).unwrap();
 }
}
