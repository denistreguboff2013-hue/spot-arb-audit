// R1034-R1039: original Python 17/17 physical SQL, real original Python
// mirror_outbox.after/deleted, Rust DB2, bounded ACK. ALL PRIVATE.
fn r1034_run_actual_python_all17(root:&Path,mode:&str){
 let script=Path::new(env!("CARGO_MANIFEST_DIR"))
   .join("offline_original_python_all17_r1034.py");
 let result=std::process::Command::new("/usr/bin/python3")
  .arg(script).arg(root).arg(mode).output().unwrap();
 assert!(result.status.success(),
  "original 17-table Python fixture {mode} failed: {}",
  String::from_utf8_lossy(&result.stderr));
}
#[test]fn r1034_real_python_all_17_insert_to_rust_db2_and_ack(){
 let (root,src,dst,v)=pair();
 r1034_run_actual_python_all17(&root.0,"all_insert");
 let (_,_,rows)=r756_snapshot(&root.0,&src,&dst).unwrap();
 assert_eq!(rows.len(),1);
 let frame:Value=serde_json::from_str(rows[0]["payload_json"].as_str().unwrap()).unwrap();
 let ops=frame["operations"].as_array().unwrap();
 assert_eq!(ops.len(),17);
 let mut seen=std::collections::BTreeSet::new();
 for op in ops {assert!(seen.insert(op["table"].as_str().unwrap().to_owned()));}
 assert_eq!(seen.len(),17);
 assert_eq!(r805_replay_window(&root.0,&src,&dst,&v,1),Ok(1));
 assert_eq!(r800_streaming_original_parity(&src,&dst,&v),Ok(17));
 assert_eq!(r756_cleanup(&root.0,&src,&dst,&v),Ok((1,0)));
 assert_eq!(r366_query_num(&src,"SELECT COUNT(*) FROM mirror_outbox"),Ok(0));
}
#[test]fn r1035_real_python_15_updates_and_17_original_parity(){
 let (root,src,dst,v)=pair();
 r1034_run_actual_python_all17(&root.0,"all_insert");
 r1034_run_actual_python_all17(&root.0,"all_update");
 let (_,_,rows)=r756_snapshot(&root.0,&src,&dst).unwrap();
 assert_eq!(rows.len(),2);
 let frame:Value=serde_json::from_str(rows[1]["payload_json"].as_str().unwrap()).unwrap();
 let ops=frame["operations"].as_array().unwrap();
 assert_eq!(ops.len(),15); // attempts key-only; advisor transitions immutable.
 assert!(ops.iter().all(|op|op["op"]=="update"));
 assert_eq!(r805_replay_window(&root.0,&src,&dst,&v,2),Ok(2));
 assert_eq!(r800_streaming_original_parity(&src,&dst,&v),Ok(17));
 assert_eq!(r756_cleanup(&root.0,&src,&dst,&v),Ok((2,0)));
}
#[test]fn r1036_real_python_15_upserts_and_17_original_parity(){
 let (root,src,dst,v)=pair();
 r1034_run_actual_python_all17(&root.0,"all_insert");
 r1034_run_actual_python_all17(&root.0,"all_upsert");
 let (_,_,rows)=r756_snapshot(&root.0,&src,&dst).unwrap();
 let frame:Value=serde_json::from_str(rows[1]["payload_json"].as_str().unwrap()).unwrap();
 let ops=frame["operations"].as_array().unwrap();
 assert_eq!(ops.len(),15);
 assert!(ops.iter().all(|op|op["op"]=="upsert"));
 assert_eq!(r805_replay_window(&root.0,&src,&dst,&v,2),Ok(2));
 assert_eq!(r800_streaming_original_parity(&src,&dst,&v),Ok(17));
 assert_eq!(r756_cleanup(&root.0,&src,&dst,&v),Ok((2,0)));
}
#[test]fn r1037_real_python_all_17_mixed_delete_with_parent_fk(){
 let (root,src,dst,v)=pair();
 for mode in ["all_insert","all_update","all_upsert","all_delete"]{
  r1034_run_actual_python_all17(&root.0,mode);
 }
 let (_,_,rows)=r756_snapshot(&root.0,&src,&dst).unwrap();
 assert_eq!(rows.len(),4);
 let frame:Value=serde_json::from_str(rows[3]["payload_json"].as_str().unwrap()).unwrap();
 assert_eq!(frame["operations"].as_array().unwrap().len(),15);
 assert!(frame["operations"].as_array().unwrap()
  .iter().all(|op|op["op"]=="delete"));
 assert_eq!(r805_replay_window(&root.0,&src,&dst,&v,4),Ok(4));
 assert_eq!(r800_streaming_original_parity(&src,&dst,&v),Ok(2));
 assert_eq!(r756_cleanup(&root.0,&src,&dst,&v),Ok((4,0)));
 assert_eq!(r366_query_num(&src,"SELECT latest_seq FROM mirror_control"),Ok(4));
}
#[test]fn r1038_all_17_mixed_uncommitted_transaction_cannot_leak(){
 let (root,src,dst,v)=pair();
 r1034_run_actual_python_all17(&root.0,"all_insert");
 r1034_run_actual_python_all17(&root.0,"all_rollback");
 assert_eq!(r366_query_num(&src,"SELECT latest_seq FROM mirror_control"),Ok(1));
 assert_eq!(r366_query_num(&src,"SELECT COUNT(*) FROM mirror_outbox"),Ok(1));
 assert_eq!(r805_replay_window(&root.0,&src,&dst,&v,1),Ok(1));
 assert_eq!(r800_streaming_original_parity(&src,&dst,&v),Ok(17));
 assert_eq!(r756_cleanup(&root.0,&src,&dst,&v),Ok((1,0)));
}
#[test]fn r1039_all_17_python_lost_ack_cold_reopen_exact(){
 let (root,src,dst,v)=pair();
 for mode in ["all_insert","all_update","all_upsert"]{
  r1034_run_actual_python_all17(&root.0,mode);
 }
 assert_eq!(r805_replay_window(&root.0,&src,&dst,&v,3),Ok(3));
 assert_eq!(r366_query_num(&src,"SELECT cleanup_through FROM mirror_control"),Ok(0));
 drop(src);drop(dst);
 let src=r671_open(&root.0,"r671-private-db1.sqlite",false).unwrap();
 let dst=r671_open(&root.0,"r671-private-db2.sqlite",false).unwrap();
 assert_eq!(r805_replay_window(&root.0,&src,&dst,&v,3),Ok(0));
 assert_eq!(r800_streaming_original_parity(&src,&dst,&v),Ok(17));
 assert_eq!(r756_cleanup(&root.0,&src,&dst,&v),Ok((3,0)));
}

#[test]fn r1040_original_immutable_advisor_transition_update_denied(){
 let (root,src,dst,v)=pair();
 r1034_run_actual_python_all17(&root.0,"all_insert");
 r1034_run_actual_python_all17(&root.0,"deny_immutable_update");
 assert_eq!(r366_query_num(&src,"SELECT latest_seq FROM mirror_control"),Ok(1));
 assert_eq!(r805_replay_window(&root.0,&src,&dst,&v,1),Ok(1));
 assert_eq!(r800_streaming_original_parity(&src,&dst,&v),Ok(17));
}
#[test]fn r1041_original_immutable_advisor_transition_delete_denied(){
 let (root,src,dst,v)=pair();
 r1034_run_actual_python_all17(&root.0,"all_insert");
 r1034_run_actual_python_all17(&root.0,"deny_immutable_delete");
 assert_eq!(r366_query_num(&src,"SELECT COUNT(*) FROM advisor_approval_transitions"),Ok(1));
 assert_eq!(r805_replay_window(&root.0,&src,&dst,&v,1),Ok(1));
 assert_eq!(r800_streaming_original_parity(&src,&dst,&v),Ok(17));
}
#[test]fn r1042_original_parent_advisor_fk_delete_denied(){
 let (root,src,dst,v)=pair();
 r1034_run_actual_python_all17(&root.0,"all_insert");
 r1034_run_actual_python_all17(&root.0,"deny_parent_fk_delete");
 assert_eq!(r366_query_num(&src,"SELECT COUNT(*) FROM advisor_recommendation_ledger"),Ok(1));
 assert_eq!(r805_replay_window(&root.0,&src,&dst,&v,1),Ok(1));
 assert_eq!(r800_streaming_original_parity(&src,&dst,&v),Ok(17));
}
#[test]fn r1043_all17_bad_last_after_hash_rolls_back_all_db2_rows(){
 let (root,src,dst,v)=pair();
 r1034_run_actual_python_all17(&root.0,"all_insert");
 let (_,_,rows)=r756_snapshot(&root.0,&src,&dst).unwrap();
 let mut frame:Value=serde_json::from_str(rows[0]["payload_json"].as_str().unwrap()).unwrap();
 assert_eq!(frame["operations"].as_array().unwrap().len(),17);
 frame["operations"][16]["after_hash"]=Value::String("f".repeat(64));
 let raw=r386_python_mirror_canonical(&frame).unwrap();
 assert_eq!(raw.len(),rows[0]["payload_json"].as_str().unwrap().len());
 let sql=format!("UPDATE mirror_outbox SET payload_json={},payload_hash={} WHERE seq=1",
  r586_sql_literal(&Value::String(raw.clone())).unwrap(),
  r586_sql_literal(&Value::String(r366_sha(&raw))).unwrap());
 exec(&src,&sql).unwrap();
 assert_eq!(r805_replay_window(&root.0,&src,&dst,&v,1),
  Err(EventOutboxR366Error::Corrupt));
 assert_eq!(r366_query_num(&dst,"SELECT COUNT(*) FROM attempts"),Ok(0));
 assert_eq!(r366_query_num(&dst,"SELECT COUNT(*) FROM advisor_approval_transitions"),Ok(0));
 assert_eq!(r366_query_num(&dst,"SELECT COUNT(*) FROM signals"),Ok(0));
 assert_eq!(r366_query_num(&dst,"SELECT applied_seq FROM mirror_meta"),Ok(0));
 assert_eq!(r366_query_num(&src,"SELECT cleanup_through FROM mirror_control"),Ok(0));
}
#[test]fn r1044_all17_half_applied_frontier_cannot_delete_outbox(){
 let (root,src,dst,v)=pair();
 for mode in ["all_insert","all_update","all_upsert","all_delete"]{
  r1034_run_actual_python_all17(&root.0,mode);
 }
 assert_eq!(r805_replay_window(&root.0,&src,&dst,&v,2),Ok(2));
 assert_eq!(r756_cleanup(&root.0,&src,&dst,&v),
  Err(EventOutboxR366Error::Conflict));
 assert_eq!(r366_query_num(&src,"SELECT COUNT(*) FROM mirror_outbox"),Ok(4));
 assert_eq!(r805_replay_window(&root.0,&src,&dst,&v,2),Ok(2));
 assert_eq!(r800_streaming_original_parity(&src,&dst,&v),Ok(2));
 assert_eq!(r756_cleanup(&root.0,&src,&dst,&v),Ok((4,0)));
 assert_eq!(r366_query_num(&src,"SELECT COUNT(*) FROM mirror_outbox"),Ok(0));
}

#[test]fn r1045_original_all17_65_envelopes_periodic_full_parity_ack(){
 let (root,src,dst,v)=pair();
 r1034_run_actual_python_all17(&root.0,"all_insert");
 assert_eq!(r805_replay_window(&root.0,&src,&dst,&v,1),Ok(1));
 assert_eq!(r756_cleanup(&root.0,&src,&dst,&v),Ok((1,0)));
 for cycle in 0..4{
  for _ in 0..16{
   r1034_run_actual_python_all17(&root.0,"all_update");
   assert_eq!(r805_replay_window(&root.0,&src,&dst,&v,1),Ok(1));
  }
  assert_eq!(r800_streaming_original_parity(&src,&dst,&v),Ok(17));
  assert_eq!(r756_cleanup(&root.0,&src,&dst,&v),Ok((16,0)));
  assert_eq!(r366_query_num(&src,"SELECT COUNT(*) FROM mirror_outbox"),Ok(0));
  assert_eq!(r366_query_num(&dst,"SELECT applied_seq FROM mirror_meta"),Ok(17+cycle*16));
 }
 assert_eq!(r366_query_num(&src,"SELECT latest_seq FROM mirror_control"),Ok(65));
}
#[test]fn r1046_original_all17_cold_restart_mid_16_update_prefix(){
 let (root,src,dst,v)=pair();
 r1034_run_actual_python_all17(&root.0,"all_insert");
 for _ in 0..16{r1034_run_actual_python_all17(&root.0,"all_update");}
 assert_eq!(r805_replay_window(&root.0,&src,&dst,&v,9),Ok(9));
 assert_eq!(r366_query_num(&src,"SELECT cleanup_through FROM mirror_control"),Ok(0));
 drop(src);drop(dst);
 let src=r671_open(&root.0,"r671-private-db1.sqlite",false).unwrap();
 let dst=r671_open(&root.0,"r671-private-db2.sqlite",false).unwrap();
 assert_eq!(r805_replay_window(&root.0,&src,&dst,&v,16),Ok(8));
 assert_eq!(r800_streaming_original_parity(&src,&dst,&v),Ok(17));
 assert_eq!(r756_cleanup(&root.0,&src,&dst,&v),Ok((17,0)));
 assert_eq!(r366_query_num(&dst,"SELECT applied_seq FROM mirror_meta"),Ok(17));
}
#[test]fn r1047_original_unjournaled_advisor_ledger_mutation_denies_ack(){
 let (root,src,dst,v)=pair();
 r1034_run_actual_python_all17(&root.0,"all_insert");
 assert_eq!(r805_replay_window(&root.0,&src,&dst,&v,1),Ok(1));
 exec(&src,"UPDATE advisor_recommendation_ledger SET reason='unaudited' WHERE recommendation_id='r1034-rec'").unwrap();
 assert_eq!(r756_cleanup(&root.0,&src,&dst,&v),
  Err(EventOutboxR366Error::Corrupt));
 assert_eq!(r366_query_num(&src,"SELECT cleanup_through FROM mirror_control"),Ok(0));
}
#[test]fn r1048_original_db2_forged_advisor_ledger_mutation_denies_ack(){
 let (root,src,dst,v)=pair();
 r1034_run_actual_python_all17(&root.0,"all_insert");
 assert_eq!(r805_replay_window(&root.0,&src,&dst,&v,1),Ok(1));
 exec(&dst,"UPDATE advisor_recommendation_ledger SET reason='forged' WHERE recommendation_id='r1034-rec'").unwrap();
 assert_eq!(r756_cleanup(&root.0,&src,&dst,&v),
  Err(EventOutboxR366Error::Corrupt));
 assert_eq!(r366_query_num(&src,"SELECT cleanup_through FROM mirror_control"),Ok(0));
}
#[test]fn r1049_original_duplicate_delete_must_not_advance_source_cursor(){
 let (root,src,dst,v)=pair();
 r1034_run_actual_python_all17(&root.0,"all_insert");
 r1034_run_actual_python_all17(&root.0,"all_delete");
 let script=Path::new(env!("CARGO_MANIFEST_DIR"))
  .join("offline_original_python_all17_r1034.py");
 let denied=std::process::Command::new("/usr/bin/python3")
  .arg(script).arg(&root.0).arg("all_delete").output().unwrap();
 assert!(!denied.status.success(),"duplicate DELETE should be denied");
 assert_eq!(r366_query_num(&src,"SELECT latest_seq FROM mirror_control"),Ok(2));
 assert_eq!(r805_replay_window(&root.0,&src,&dst,&v,2),Ok(2));
 assert_eq!(r800_streaming_original_parity(&src,&dst,&v),Ok(2));
 assert_eq!(r756_cleanup(&root.0,&src,&dst,&v),Ok((2,0)));
}
