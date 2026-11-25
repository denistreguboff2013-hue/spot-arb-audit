// R178: separate test-harness CHILD with real SIGKILL, only private /tmp databases.
// All child inputs are synthetic; the running PAPER service is never a target.
#[cfg(test)]
mod r178_process_tests{
 use super::*;
 use std::{os::unix::{fs::DirBuilderExt,process::ExitStatusExt},
  sync::atomic::{AtomicUsize,Ordering},process::Command};
 static COUNTER:AtomicUsize=AtomicUsize::new(0);
 const NOW:u64=1_792_280_000_000;
 const KEY:&[u8]=b"r178-offline-test-only-external-key-0000001";
 const OWNER:&str="r178-research";
 fn make()->(PathBuf,PathBuf){
  let id=format!("{}_{}",std::process::id(),COUNTER.fetch_add(1,Ordering::Relaxed));
  let dbdir=std::env::temp_dir().join(format!("astra_r147_isolated_r178_{id}"));
  let fdir=std::env::temp_dir().join(format!("astra_r171_isolated_r178_{id}"));
  fs::DirBuilder::new().mode(0o700).create(&dbdir).unwrap();
  fs::DirBuilder::new().mode(0o700).create(&fdir).unwrap();
  let db=SqliteMirrorReview::initialize(&dbdir).unwrap();
  let floor=fdir.join("external-witness-floor.json");
  db.initialize_local_floor_r171(&floor,NOW,OWNER,KEY).unwrap();
  (dbdir,floor)
 }
 #[test]fn r178_child_sigkill_in_private_process(){
  if std::env::var("ASTRA_R178_RESEARCH_CHILD").ok().as_deref()!=Some("ISOLATED_ONLY"){
   return
  }
  let root=PathBuf::from(std::env::var("ASTRA_R178_PRIVATE_DB").unwrap());
  let floor=PathBuf::from(std::env::var("ASTRA_R178_PRIVATE_FLOOR").unwrap());
  let phase=std::env::var("ASTRA_R178_PHASE").unwrap();
  assert!(root.file_name().unwrap().to_str().unwrap().starts_with("astra_r147_isolated_r178_"));
  assert_eq!(floor.file_name().unwrap().to_str(),Some("external-witness-floor.json"));
  let db=SqliteMirrorReview::load(&root).unwrap();
  db.replay_with_local_floor_r172(&r156_r157_tests::projection(10),&floor,
    NOW,3,OWNER,KEY,Some(&phase)).unwrap_err();
  panic!("research child unexpectedly survived selected SIGKILL checkpoint")
 }
 #[test]fn r178_real_child_sigkill_four_durable_boundaries(){
  let states=[
   ("sigkill_after_intent",FloorReviewR173::Prepared,0u64),
   ("sigkill_after_first_batch",FloorReviewR173::Partial,45u64),
   ("sigkill_after_db_commit",FloorReviewR173::CommittedUnpublished,52u64),
   ("sigkill_after_publish",FloorReviewR173::Clean,52u64),
  ];
  for (phase,expected_state,expected_seq) in states{
   let (root,floor)=make();
   let child=Command::new(std::env::current_exe().unwrap())
    .arg("r178_child_sigkill_in_private_process")
    .arg("--nocapture")
    .env("ASTRA_R178_RESEARCH_CHILD","ISOLATED_ONLY")
    .env("ASTRA_R178_PRIVATE_DB",&root)
    .env("ASTRA_R178_PRIVATE_FLOOR",&floor)
    .env("ASTRA_R178_PHASE",phase)
    .output().unwrap();
   assert_eq!(child.status.signal(),Some(libc::SIGKILL),
    "{phase}: child output {}",
    String::from_utf8_lossy(&child.stdout));
   let cold=SqliteMirrorReview::load(&root).unwrap();
   assert_eq!(cold.inspect_local_floor_r173(&floor,NOW,OWNER,KEY).unwrap(),
    expected_state,"{phase}");
   assert_eq!(cold.review_integrity_r152(NOW).unwrap().0,expected_seq);
   assert!(floor.with_extension("lock").exists());
   assert!(cold.replay_with_local_floor_r172(&r156_r157_tests::projection(10),
     &floor,NOW,3,OWNER,KEY,None).is_err());
   fs::remove_dir_all(&root).unwrap();
   fs::remove_dir_all(floor.parent().unwrap()).unwrap();
  }
 }
}
