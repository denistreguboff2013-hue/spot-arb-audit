// R396: SIGKILL a disposable Rust test child only, never supervisor PAPER.
// All SQLite inputs are synthetic under owned 0700 /tmp fixture_root.
#[cfg(test)]
mod r396_sigkill_tests{
 use super::*;
 use super::r366_tests::{fixture_root,record};
 use std::os::unix::process::ExitStatusExt;
 #[test]fn r396_isolated_child_process_entry(){
  if std::env::var("ASTRA_R396_TEST_CHILD").ok().as_deref()
    !=Some("DISPOSABLE_PRIVATE_SQLITE"){return}
  let root=PathBuf::from(std::env::var("ASTRA_R396_PRIVATE_ROOT").unwrap());
  assert!(root.parent()==Some(std::env::temp_dir().as_path()));
  assert!(root.file_name().and_then(|s|s.to_str())
    .is_some_and(|s|s.starts_with("astra_r366_isolated_")));
  let phase=std::env::var("ASTRA_R396_TEST_PHASE").unwrap();
  let w=IsolatedEventOutboxR366::reopen(&root).unwrap();
  w.append(&record("r396_sigkill_second","decisions"),1_792_660_000_200,false).unwrap();
  assert_eq!(phase,"committed");
  assert_eq!(w.audit().unwrap().events,2);
  unsafe{libc::raise(libc::SIGKILL);}
  panic!("disposable child unexpectedly survived SIGKILL")
 }
 #[test]fn r396_real_child_sigkill_before_and_after_sqlite_commit(){
  for (phase,expected_events) in [("before",1_u64),("committed",2_u64)]{
   let root=fixture_root();
   let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
   w.append(&record("r396_sigkill_first","errors"),1_792_660_000_100,false).unwrap();
   assert_eq!(w.audit().unwrap().events,1);
   drop(w);
   let child=std::process::Command::new(std::env::current_exe().unwrap())
    .arg("r396_isolated_child_process_entry").arg("--nocapture")
    .env("ASTRA_R396_TEST_CHILD","DISPOSABLE_PRIVATE_SQLITE")
    .env("ASTRA_R396_PRIVATE_ROOT",&root)
    .env("ASTRA_R396_TEST_PHASE",phase)
    .env("ASTRA_R396_PRIVATE_CHILD_SIGKILL",
      if phase=="before"{"OWNED_SQLITE_TX_TEST_ONLY"}else{"no"})
    .output().unwrap();
   assert_eq!(child.status.signal(),Some(libc::SIGKILL),
     "{phase}: stdout={}",String::from_utf8_lossy(&child.stdout));
   let reopened=IsolatedEventOutboxR366::reopen(&root).unwrap();
   let audit=reopened.audit().unwrap();
   assert_eq!((audit.events,audit.category_rows,audit.outbox,audit.last_seq),
      (expected_events,expected_events,expected_events,expected_events),
      "{phase}: partial source rows must not escape transaction");
   if phase=="before"{
     reopened.append(&record("r396_sigkill_second","decisions"),
        1_792_660_000_200,false).unwrap();
     assert_eq!(reopened.audit().unwrap().events,2);
   }
   drop(reopened);
   fs::remove_dir_all(&root).unwrap();
  }
 }
}
