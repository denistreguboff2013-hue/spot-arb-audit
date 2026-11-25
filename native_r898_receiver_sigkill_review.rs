// Astra P0-01 R898-R899: literal SIGKILL of independent Rust DB2 worker
// after durable destination replay and after prefix ACK, then new PID resumes.
// Private original 17-table R671 fixture only, NOT installed PAPER/REAL.
fn r898_child_root()->Option<PathBuf>{
 let root=PathBuf::from(std::env::var_os("ASTRA_R898_ROOT")?);
 r671_guard(&root,"r671-private-db1.sqlite").ok()?;
 Some(root)
}
#[test] fn r898_receiver_sigkill_stage_child(){
 let Some(root)=r898_child_root() else{return};
 let stage=std::env::var("ASTRA_R898_STAGE").unwrap();
 assert!(matches!(stage.as_str(),"after_db2_replay"|"after_prefix_ack"));
 let src=r671_open(&root,"r671-private-db1.sqlite",false).unwrap();
 let dst=r671_open(&root,"r671-private-db2.sqlite",false).unwrap();
 let oracle=r586_oracle();
 assert_eq!(r845_verified_burst(&root,&src,&dst,&oracle,1),Ok(1));
 if stage=="after_prefix_ack"{
  assert_eq!(r879_private_paper_prefix_ack(&root,&src,&dst,&oracle,"r879_probe"),Ok((1,2)));
 }
 std::fs::write(root.join("r898-stage-reached"),stage.as_bytes()).unwrap();
 // Parent must OS-SIGKILL this PID, not politely shut down.
 std::thread::sleep(std::time::Duration::from_secs(25));
 panic!("R898 crash worker must be SIGKILLed by parent");
}
#[test] fn r899_new_pid_replay_after_killed_receiver(){
 let Some(root)=r898_child_root() else{return};
 let src=r671_open(&root,"r671-private-db1.sqlite",false).unwrap();
 let dst=r671_open(&root,"r671-private-db2.sqlite",false).unwrap();
 let oracle=r586_oracle();
 let cleaned=r366_query_num(&src,"SELECT cleanup_through FROM mirror_control").unwrap();
 let applied=r366_query_num(&dst,"SELECT applied_seq FROM mirror_meta").unwrap();
 assert_eq!(applied,1,"durable DB2 replay must survive SIGKILL");
 if cleaned==0 {
  assert_eq!(r879_private_paper_prefix_ack(&root,&src,&dst,&oracle,"r879_probe"),Ok((1,2)));
 }else{assert_eq!(cleaned,1)}
 assert_eq!(r845_verified_burst(&root,&src,&dst,&oracle,128),Ok(2));
 assert_eq!(r860_bulk_verified_ack(&root,&src,&dst,&oracle,1024),Ok((2,0)));
 assert_eq!(r366_query_num(&src,"SELECT COUNT(*) FROM mirror_outbox"),Ok(0));
 assert_eq!(r366_query_num(&dst,"SELECT applied_seq FROM mirror_meta"),Ok(3));
 assert_eq!(r800_streaming_original_parity(&src,&dst,&oracle),Ok(96));
 eprintln!("R899 NEW PID restored original 96 events, ACK=3, no double apply");
}
fn r898_one_sigkill_window(stage:&str){
 assert!(r898_child_root().is_none());
 let (root,h,owner)=pair(3);
 let exe=std::env::current_exe().unwrap();
 let mut stage_cmd=std::process::Command::new(&exe);
 stage_cmd.args(["--exact","native_isolated_sqlite_mirror_r147_review::r879_tests::r898_receiver_sigkill_stage_child","--nocapture"])
   .env("ASTRA_R898_ROOT",&root.0)
   .env("ASTRA_R898_STAGE",stage)
   .stdout(std::process::Stdio::null())
   .stderr(std::process::Stdio::null());
 let mut child=stage_cmd.spawn().unwrap();
 let marker=root.0.join("r898-stage-reached");
 let mut reached=false;
 for _ in 0..1500{
  if marker.exists(){reached=true;break}
  if child.try_wait().unwrap().is_some(){break}
  std::thread::sleep(std::time::Duration::from_millis(5));
 }
 child.kill().expect("literal SIGKILL to private receiver");
 let dead=child.wait().unwrap();
 assert!(!dead.success(),"SIGKILL child must not return success");
 assert!(reached,"R898 stage marker never observed; do not claim crash-window evidence");
 let state=std::fs::read_to_string(marker).unwrap();
 assert_eq!(state,stage);
 assert_eq!(r366_query_num(&h.sink,"SELECT applied_seq FROM mirror_meta"),Ok(1));
 let prior_cleaned=if stage=="after_prefix_ack"{1}else{0};
 assert_eq!(r366_query_num(&h.source,"SELECT cleanup_through FROM mirror_control"),Ok(prior_cleaned));
 let output=std::process::Command::new(&exe)
   .args(["--exact","native_isolated_sqlite_mirror_r147_review::r879_tests::r899_new_pid_replay_after_killed_receiver","--nocapture"])
   .env("ASTRA_R898_ROOT",&root.0).output().unwrap();
 assert!(output.status.success(),
   "R899 resumed PID failed: {}",String::from_utf8_lossy(&output.stderr));
 assert_eq!(r366_query_num(&h.source,"SELECT cleanup_through FROM mirror_control"),Ok(3));
 assert_eq!(r366_query_num(&h.source,"SELECT COUNT(*) FROM events"),Ok(96));
 assert_eq!(r366_query_num(&h.sink,"SELECT COUNT(*) FROM events"),Ok(96));
 let (restored,n)=r834_recover_private_owner(&root.0,"r879_probe",0,1,800).unwrap();
 assert_eq!(n,96);
 assert_eq!(restored.snapshot_venue("BTC",Venue::OkxSpot),
            owner.snapshot_venue("BTC",Venue::OkxSpot));
 eprintln!("R898-899 LITERAL SIGKILL window={stage} verified: source+DB2+Owner 96, no duplicates, ACK durable");
}
#[test] fn r898_sigkill_after_db2_commit_before_source_ack(){
 r898_one_sigkill_window("after_db2_replay");
}
#[test] fn r899_sigkill_after_prefix_ack_before_next_replay(){
 r898_one_sigkill_window("after_prefix_ack");
}
