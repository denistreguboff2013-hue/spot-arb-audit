// R202: TEST-ONLY real process SIGKILL around durable queue manifest publication.
// Not a physical power-loss test or real PAPER queue integration.
#[cfg(test)]
mod r202_tests{
 use super::*;
 use std::{os::unix::{fs::{DirBuilderExt,OpenOptionsExt},process::ExitStatusExt},
  io::Write,process::Command,sync::atomic::{AtomicUsize,Ordering}};
 static N:AtomicUsize=AtomicUsize::new(0);
 const KEY:&[u8]=b"r202-ephemeral-test-only-local-queue-key-23";
 fn fresh()->(PathBuf,PathBuf){
  let r=std::env::temp_dir().join(format!("astra_r195_isolated_r202_{}_{}",
   std::process::id(),N.fetch_add(1,Ordering::Relaxed)));
  fs::DirBuilder::new().mode(0o700).create(&r).unwrap();
  let q=r.join("paper-session-queue.json");
  initialize_queue_r195(&q,KEY).unwrap();(r,q)
 }
 #[test]fn r202_child_durable_file_sigkill_only(){
  if std::env::var("ASTRA_R202_RESEARCH_CHILD").ok().as_deref()!=Some("ISOLATED"){
   return
  }
  let path=PathBuf::from(std::env::var("ASTRA_R202_PATH").unwrap());
  let phase=std::env::var("ASTRA_R202_PHASE").unwrap();
  assert!(path.parent().unwrap().file_name().unwrap()
    .to_str().unwrap().starts_with("astra_r195_isolated_r202_"));
  let _lock=queue_lock_r195(&path).unwrap();
  let mut manifest=load_queue_r195(&path,KEY).unwrap();
  manifest.revision+=1;manifest.mac=queue_mac_r195(&manifest,KEY).unwrap();
  let bytes=serde_json::to_vec(&manifest).unwrap();
  let tmp=path.with_extension("tmp");
  let mut out=fs::OpenOptions::new().write(true).create_new(true).mode(0o600)
   .open(&tmp).unwrap();
  out.write_all(&bytes).unwrap();out.sync_all().unwrap();drop(out);
  if phase=="after_file_sync"{
   unsafe{libc::raise(libc::SIGKILL);}
  }
  fs::rename(&tmp,&path).unwrap();
  fs::File::open(path.parent().unwrap()).unwrap().sync_all().unwrap();
  if phase=="after_rename"{
   unsafe{libc::raise(libc::SIGKILL);}
  }
  panic!("unrecognized test phase")
 }
 #[test]fn r202_actual_sigkill_at_two_atomic_publish_boundaries(){
  for phase in ["after_file_sync","after_rename"]{
   let (root,path)=fresh();
   let child=Command::new(std::env::current_exe().unwrap())
    .arg("r202_child_durable_file_sigkill_only").arg("--nocapture")
    .env("ASTRA_R202_RESEARCH_CHILD","ISOLATED")
    .env("ASTRA_R202_PATH",&path).env("ASTRA_R202_PHASE",phase)
    .output().unwrap();
   assert_eq!(child.status.signal(),Some(libc::SIGKILL),
    "{phase} -> {}",String::from_utf8_lossy(&child.stdout));
   assert!(path.with_extension("lock").exists(),
    "killed writer leaves orphan for manual review");
   if phase=="after_file_sync"{
    assert!(path.with_extension("tmp").exists());
    assert!(load_queue_r195(&path,KEY).is_err());
   }else{
    assert!(!path.with_extension("tmp").exists());
    assert_eq!(load_queue_r195(&path,KEY).unwrap().revision,1);
   }
   fs::remove_dir_all(root).unwrap();
  }
 }
}
