use std::time::Duration;
// R956-R959 true independent-OS interruption and source-append races
// for the private R954 fused DB2-durable->DB1-ACK protocol.
#[test] fn r956_fused_receiver_child_kill_window(){
 let Some(root)=std::env::var_os("ASTRA_R956_ROOT").map(PathBuf::from) else{return};
 r671_guard(&root,"r671-private-db1.sqlite").unwrap();
 let src=r671_open(&root,"r671-private-db1.sqlite",false).unwrap();
 let dst=r671_open(&root,"r671-private-db2.sqlite",false).unwrap();
 let v=r586_oracle();
 let stage=std::env::var("ASTRA_R956_STAGE").unwrap();
 match stage.as_str(){
  "after_db2"=>{
   let result=r954_private_fused_verified_replay_and_ack(
    &root,&src,&dst,&v,"r956_probe",||{
     std::fs::write(root.join("r956-ready"),b"db2_committed").unwrap();
     std::thread::sleep(Duration::from_secs(20));
     Ok(())
    });
   panic!("receiver was not SIGKILLed, got {result:?}");
  },
  "after_ack"=>{
   assert_eq!(r954_private_fused_verified_replay_and_ack(
    &root,&src,&dst,&v,"r956_probe",||Ok(())),Ok((1,1)));
   std::fs::write(root.join("r956-ready"),b"db1_ack_committed").unwrap();
   std::thread::sleep(Duration::from_secs(20));
   panic!("receiver was not SIGKILLed");
  },
  _=>panic!("invalid kill-stage")
 }
}
fn r956_private_actual_sigkill(stage:&str){
 let root=Root::new();let v=r586_oracle();
 let src=r671_open(&root.0,"r671-private-db1.sqlite",false).unwrap();
 let dst=r671_open(&root.0,"r671-private-db2.sqlite",false).unwrap();
 r924_install_private_paper_only_guards(&src,&dst,&v).unwrap();
 r932_certify_private_empty_genesis(&root.0,&src,&dst,&v,"r956_probe").unwrap();
 let mut h=R819OriginalPaperHistory::new(&root.0,"r956_probe").unwrap();
 let mut owner=Owner::new(0,1,800).unwrap();
 assert_eq!(r824_commit_prepared_batch(&mut h,&mut owner,
  &(1..=32).map(event).collect::<Vec<_>>()),Ok((1,32)));
 let mut child=std::process::Command::new(std::env::current_exe().unwrap())
  .args(["--exact",
    "native_isolated_sqlite_mirror_r147_review::r879_tests::r956_fused_receiver_child_kill_window",
    "--nocapture"])
  .env("ASTRA_R956_ROOT",&root.0).env("ASTRA_R956_STAGE",stage)
  .stdout(std::process::Stdio::null())
  .stderr(std::process::Stdio::null()).spawn().unwrap();
 let mut reached=false;
 for _ in 0..1500{
  if root.0.join("r956-ready").exists(){reached=true;break}
  if child.try_wait().unwrap().is_some(){break}
  std::thread::sleep(Duration::from_millis(5));
 }
 child.kill().expect("R956 mandatory test SIGKILL");
 assert!(!child.wait().unwrap().success());
 assert!(reached,"R956 DB2/ACK durable kill window not reached");
 let cleaned=if stage=="after_ack"{1}else{0};
 assert_eq!(r366_query_num(&dst,"SELECT applied_seq FROM mirror_meta"),Ok(1));
 assert_eq!(r366_query_num(&src,"SELECT cleanup_through FROM mirror_control"),Ok(cleaned));
 if cleaned==0{
  assert_eq!(r945_private_fast_fingerprint_ack(
   &root.0,&src,&dst,&v,"r956_probe"),Ok((1,0)));
 }
 assert_eq!(r800_streaming_original_parity(&src,&dst,&v),Ok(32));
 assert_eq!(r366_query_num(&src,"SELECT COUNT(*) FROM mirror_outbox"),Ok(0));
 let (restored,count)=r834_recover_private_owner(&root.0,"r956_probe",0,1,800).unwrap();
 assert_eq!(count,32);
 assert_eq!(restored.snapshot_venue("BTC",Venue::OkxSpot),
  owner.snapshot_venue("BTC",Venue::OkxSpot));
}
#[test]fn r956_sigkill_fused_after_db2_before_source_ack(){
 if std::env::var_os("ASTRA_R956_ROOT").is_none(){
  r956_private_actual_sigkill("after_db2");
 }
}
#[test]fn r957_sigkill_fused_after_source_ack_durable(){
 if std::env::var_os("ASTRA_R956_ROOT").is_none(){
  r956_private_actual_sigkill("after_ack");
 }
}
#[test]fn r958_new_source_append_during_fused_ack_preserves_future(){
 let root=Root::new();let v=r586_oracle();
 let src=r671_open(&root.0,"r671-private-db1.sqlite",false).unwrap();
 let dst=r671_open(&root.0,"r671-private-db2.sqlite",false).unwrap();
 r924_install_private_paper_only_guards(&src,&dst,&v).unwrap();
 r932_certify_private_empty_genesis(&root.0,&src,&dst,&v,"r958_probe").unwrap();
 let mut h=R819OriginalPaperHistory::new(&root.0,"r958_probe").unwrap();
 let mut owner=Owner::new(0,1,800).unwrap();
 assert_eq!(r824_commit_prepared_batch(&mut h,&mut owner,
  &(1..=32).map(event).collect::<Vec<_>>()),Ok((1,32)));
 let writer=r671_open(&root.0,"r671-private-db1.sqlite",false).unwrap();
 let writer_sink=r671_open(&root.0,"r671-private-db2.sqlite",false).unwrap();
 let ops=(33..=64).map(|i|{
  let (frame,update)=event(i);
  r824_original_paper_op(&v,"r958_probe",2,(i-33) as usize,32,&frame,&update).unwrap()
 }).collect::<Vec<_>>();
 let first=r954_private_fused_verified_replay_and_ack(
  &root.0,&src,&dst,&v,"r958_probe",||{
   assert_eq!(r830_incremental_original_append(
    &root.0,&writer,&writer_sink,&v,&ops),Ok(2));
   Ok(())
  });
 assert_eq!(first,Ok((1,1)));
 assert_eq!(r366_query_num(&src,"SELECT latest_seq FROM mirror_control"),Ok(2));
 assert_eq!(r366_query_num(&src,"SELECT cleanup_through FROM mirror_control"),Ok(1));
 assert_eq!(r954_private_fused_verified_replay_and_ack(
  &root.0,&src,&dst,&v,"r958_probe",||Ok(())),Ok((1,1)));
 assert_eq!(r800_streaming_original_parity(&src,&dst,&v),Ok(64));
 assert_eq!(r366_query_num(&src,"SELECT COUNT(*) FROM mirror_outbox"),Ok(0));
}
#[test]fn r959_poisoned_new_tail_denies_ack_after_db2_persists(){
 let root=Root::new();let v=r586_oracle();
 let src=r671_open(&root.0,"r671-private-db1.sqlite",false).unwrap();
 let dst=r671_open(&root.0,"r671-private-db2.sqlite",false).unwrap();
 r924_install_private_paper_only_guards(&src,&dst,&v).unwrap();
 r932_certify_private_empty_genesis(&root.0,&src,&dst,&v,"r959_probe").unwrap();
 let mut h=R819OriginalPaperHistory::new(&root.0,"r959_probe").unwrap();
 let mut owner=Owner::new(0,1,800).unwrap();
 assert_eq!(r824_commit_prepared_batch(&mut h,&mut owner,
  &(1..=32).map(event).collect::<Vec<_>>()),Ok((1,32)));
 let attacker=r671_open(&root.0,"r671-private-db1.sqlite",false).unwrap();
 let writer_sink=r671_open(&root.0,"r671-private-db2.sqlite",false).unwrap();
 let ops=(33..=64).map(|i|{
  let (frame,update)=event(i);
  r824_original_paper_op(&v,"r959_probe",2,(i-33) as usize,32,&frame,&update).unwrap()
 }).collect::<Vec<_>>();
 assert_eq!(r954_private_fused_verified_replay_and_ack(
  &root.0,&src,&dst,&v,"r959_probe",||{
   assert_eq!(r830_incremental_original_append(
    &root.0,&attacker,&writer_sink,&v,&ops),Ok(2));
   exec(&attacker,"UPDATE mirror_outbox SET payload_hash='poisoned' WHERE seq=2")
    .map_err(r366_error)
  }),Err(EventOutboxR366Error::Corrupt));
 assert_eq!(r366_query_num(&src,"SELECT cleanup_through FROM mirror_control"),Ok(0));
 assert_eq!(r366_query_num(&dst,"SELECT applied_seq FROM mirror_meta"),Ok(1));
 // Proves fail-closed; tampered tail requires independent source repair.
}
