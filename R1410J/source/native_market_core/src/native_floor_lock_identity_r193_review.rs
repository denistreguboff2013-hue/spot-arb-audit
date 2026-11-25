// R193: read-only, two-pass forensic lock identity for private /tmp fixtures.
// Old R171 locks contain NO writer PID/boot nonce. Never infer safe unlock.
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct LockFingerprintR193{
 pub inode:u64,pub owner_uid:u32,pub bytes:u64,
 pub writer_verified_dead:bool,pub auto_release_allowed:bool,
}
pub fn inspect_stable_lock_r193(path:&Path)
 ->Result<Option<LockFingerprintR193>,PaperAdapterErrorR156>{
 let a=inspect_floor_lock_r185(path)?;
 let b=inspect_floor_lock_r185(path)?;
 if a!=b{return Err(PaperAdapterErrorR156::Source)}
 match a{
  LockStateR185::Absent=>Ok(None),
  LockStateR185::ManualReview{inode,owner_uid,bytes}=>Ok(Some(LockFingerprintR193{
   inode,owner_uid,bytes,writer_verified_dead:false,auto_release_allowed:false
  }))
 }
}
pub fn lock_release_decision_r193(f:&LockFingerprintR193,
 authority_proved:bool)->Result<(),PaperAdapterErrorR156>{
 // Even a human approval cannot assert an unknown writer identity.
 let _=authority_proved;let _=f;
 Err(PaperAdapterErrorR156::Source)
}
#[cfg(test)]
mod r193_tests{
 use super::*;
 use std::{os::unix::fs::DirBuilderExt,sync::atomic::{AtomicUsize,Ordering}};
 static N:AtomicUsize=AtomicUsize::new(0);
 #[test]fn r193_no_auto_unlock_even_with_human_boolean(){
  let root=std::env::temp_dir().join(format!("astra_r171_isolated_r193_{}_{}",
   std::process::id(),N.fetch_add(1,Ordering::Relaxed)));
  fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
  let path=root.join("external-witness-floor.json");
  assert_eq!(inspect_stable_lock_r193(&path).unwrap(),None);
  let guard=acquire_floor_lock_r171(&path).unwrap();
  let fp=inspect_stable_lock_r193(&path).unwrap().unwrap();
  assert!(!fp.writer_verified_dead&&!fp.auto_release_allowed);
  assert!(lock_release_decision_r193(&fp,true).is_err());
  assert!(path.with_extension("lock").exists());
  drop(guard);
  assert_eq!(inspect_stable_lock_r193(&path).unwrap(),None);
  fs::remove_dir_all(root).unwrap();
 }
}
