// R1021-R1033 Astra P0-01: actual original ai.mirror_outbox Python
// mixed 2-table INSERT, UPDATE, UPSERT, DELETE -> Rust exact 17-schema
// DB2 transaction and durable ACK. ALL paths are private R671 fixtures.
// This is NOT the installed collector; no historical DB or REAL access.
#[test]fn r1021_python_two_table_insert_replayed_and_acked(){
 let (root,src,dst,v)=pair();
 py(&root.0,"pair_insert");
 let (ctl,meta,rows)=r756_snapshot(&root.0,&src,&dst).unwrap();
 assert_eq!(rows.len(),1);
 assert_eq!(rows[0]["tx_kind"],"r1021_real_python_mixed_two_tables");
 assert_eq!(ctl["latest_seq"],1);
 assert_eq!(meta["applied_seq"],0);
 let frame:Value=serde_json::from_str(rows[0]["payload_json"].as_str().unwrap()).unwrap();
 assert_eq!(frame["operations"].as_array().unwrap().len(),2);
 assert_eq!(frame["operations"][0]["table"],"metadata");
 assert_eq!(frame["operations"][1]["table"],"collector_runs");
 assert_eq!(r805_replay_window(&root.0,&src,&dst,&v,4),Ok(1));
 assert_eq!(r800_streaming_original_parity(&src,&dst,&v),Ok(2));
 assert_eq!(r756_cleanup(&root.0,&src,&dst,&v),Ok((1,0)));
 assert_eq!(count(&src,"mirror_outbox"),0);
}
#[test]fn r1022_python_update_mid_ack_requires_catchup(){
 let (root,src,dst,v)=pair();
 py(&root.0,"pair_insert");
 assert_eq!(r805_replay_window(&root.0,&src,&dst,&v,1),Ok(1));
 py(&root.0,"pair_update");
 assert_eq!(r756_cleanup(&root.0,&src,&dst,&v),
  Err(EventOutboxR366Error::Conflict));
 assert_eq!(r366_query_num(&src,"SELECT cleanup_through FROM mirror_control"),Ok(0));
 assert_eq!(r805_replay_window(&root.0,&src,&dst,&v,1),Ok(1));
 assert_eq!(r800_streaming_original_parity(&src,&dst,&v),Ok(2));
 assert_eq!(r756_cleanup(&root.0,&src,&dst,&v),Ok((2,0)));
 let mut q=Stmt::new(&dst,"SELECT value FROM metadata WHERE key='r1021_meta'").unwrap();
 assert_eq!(q.step().unwrap(),SQLITE_ROW);
 assert_eq!(q.strval(0).unwrap(),"updated_pair");
}
#[test]fn r1023_python_upsert_original_two_tables_replays(){
 let (root,src,dst,v)=pair();
 py(&root.0,"pair_insert");
 py(&root.0,"pair_upsert");
 assert_eq!(r805_replay_window(&root.0,&src,&dst,&v,2),Ok(2));
 assert_eq!(r800_streaming_original_parity(&src,&dst,&v),Ok(2));
 assert_eq!(r756_cleanup(&root.0,&src,&dst,&v),Ok((2,0)));
 let mut q=Stmt::new(&dst,"SELECT stats_json FROM collector_runs WHERE run_id='r1021_run'").unwrap();
 assert_eq!(q.step().unwrap(),SQLITE_ROW);
 assert_eq!(q.strval(0).unwrap(),r#"{"accepted":4}"#);
}
#[test]fn r1024_python_mixed_all_four_kinds_then_exact_empty_parity(){
 let (root,src,dst,v)=pair();
 for mode in ["pair_insert","pair_update","pair_upsert","pair_delete"]{
  py(&root.0,mode);
 }
 assert_eq!(r805_replay_window(&root.0,&src,&dst,&v,4),Ok(4));
 assert_eq!(r800_streaming_original_parity(&src,&dst,&v),Ok(0));
 assert_eq!(count(&src,"metadata"),0);
 assert_eq!(count(&dst,"metadata"),0);
 assert_eq!(count(&src,"collector_runs"),0);
 assert_eq!(count(&dst,"collector_runs"),0);
 assert_eq!(r756_cleanup(&root.0,&src,&dst,&v),Ok((4,0)));
 assert_eq!(r366_query_num(&dst,"SELECT applied_seq FROM mirror_meta"),Ok(4));
}
#[test]fn r1025_python_mixed_transaction_rollback_leaves_only_prior(){
 let (root,src,dst,v)=pair();
 py(&root.0,"pair_insert");
 py(&root.0,"pair_rollback");
 assert_eq!(r366_query_num(&src,"SELECT latest_seq FROM mirror_control"),Ok(1));
 assert_eq!(r366_query_num(&src,"SELECT retained_records FROM mirror_control"),Ok(1));
 let mut q=Stmt::new(&src,"SELECT value FROM metadata WHERE key='r1021_meta'").unwrap();
 assert_eq!(q.step().unwrap(),SQLITE_ROW);
 assert_eq!(q.strval(0).unwrap(),"insert_one");
 drop(q);
 assert_eq!(r805_replay_window(&root.0,&src,&dst,&v,4),Ok(1));
 assert_eq!(r800_streaming_original_parity(&src,&dst,&v),Ok(2));
 assert_eq!(r756_cleanup(&root.0,&src,&dst,&v),Ok((1,0)));
}
#[test]fn r1026_python_mixed_db2_committed_before_ack_cold_reopen(){
 let (root,src,dst,v)=pair();
 py(&root.0,"pair_insert");
 py(&root.0,"pair_update");
 assert_eq!(r805_replay_window(&root.0,&src,&dst,&v,2),Ok(2));
 assert_eq!(r366_query_num(&src,"SELECT cleanup_through FROM mirror_control"),Ok(0));
 drop(src);drop(dst);
 let src=r671_open(&root.0,"r671-private-db1.sqlite",false).unwrap();
 let dst=r671_open(&root.0,"r671-private-db2.sqlite",false).unwrap();
 assert_eq!(r805_replay_window(&root.0,&src,&dst,&v,2),Ok(0));
 assert_eq!(r800_streaming_original_parity(&src,&dst,&v),Ok(2));
 assert_eq!(r756_cleanup(&root.0,&src,&dst,&v),Ok((2,0)));
 assert_eq!(r366_query_num(&dst,"SELECT COUNT(*) FROM metadata"),Ok(1));
}
#[test]fn r1027_original_python_corrupt_outer_hash_refuses_mixed_replay(){
 let (root,src,dst,v)=pair();
 py(&root.0,"pair_insert");
 exec(&src,"UPDATE mirror_outbox SET payload_hash='tampered' WHERE seq=1").unwrap();
 assert!(r805_replay_window(&root.0,&src,&dst,&v,1).is_err());
 assert_eq!(count(&dst,"metadata"),0);
 assert_eq!(count(&dst,"collector_runs"),0);
 assert_eq!(r366_query_num(&dst,"SELECT applied_seq FROM mirror_meta"),Ok(0));
 assert_eq!(r366_query_num(&src,"SELECT cleanup_through FROM mirror_control"),Ok(0));
}
#[test]fn r1028_tampered_destination_mixed_row_blocks_source_cleanup(){
 let (root,src,dst,v)=pair();
 py(&root.0,"pair_insert");
 assert_eq!(r805_replay_window(&root.0,&src,&dst,&v,1),Ok(1));
 exec(&dst,"UPDATE metadata SET value='forged' WHERE key='r1021_meta'").unwrap();
 assert_eq!(r800_streaming_original_parity(&src,&dst,&v),
  Err(EventOutboxR366Error::Corrupt));
 assert_eq!(r756_cleanup(&root.0,&src,&dst,&v),
  Err(EventOutboxR366Error::Corrupt));
 assert_eq!(r366_query_num(&src,"SELECT cleanup_through FROM mirror_control"),Ok(0));
 assert_eq!(count(&src,"mirror_outbox"),1);
}
#[test]fn r1029_unrecorded_collector_mutation_refuses_original_ack(){
 let (root,src,dst,v)=pair();
 py(&root.0,"pair_insert");
 assert_eq!(r805_replay_window(&root.0,&src,&dst,&v,1),Ok(1));
 exec(&src,"UPDATE collector_runs SET stats_json='missing_outbox' WHERE run_id='r1021_run'").unwrap();
 assert!(r756_cleanup(&root.0,&src,&dst,&v).is_err());
 assert_eq!(count(&src,"mirror_outbox"),1);
 assert_eq!(r366_query_num(&src,"SELECT cleanup_through FROM mirror_control"),Ok(0));
}
#[test]fn r1030_after_ack_original_python_delete_exact_and_idempotent(){
 let (root,src,dst,v)=pair();
 py(&root.0,"pair_insert");
 assert_eq!(r805_replay_window(&root.0,&src,&dst,&v,1),Ok(1));
 assert_eq!(r756_cleanup(&root.0,&src,&dst,&v),Ok((1,0)));
 py(&root.0,"pair_delete");
 assert_eq!(r805_replay_window(&root.0,&src,&dst,&v,1),Ok(1));
 assert_eq!(r800_streaming_original_parity(&src,&dst,&v),Ok(0));
 assert_eq!(r756_cleanup(&root.0,&src,&dst,&v),Ok((1,0)));
 assert_eq!(r805_replay_window(&root.0,&src,&dst,&v,1),Ok(0));
 assert!(r756_cleanup(&root.0,&src,&dst,&v).is_ok());
 assert_eq!(r366_query_num(&src,"SELECT cleanup_through FROM mirror_control"),Ok(2));
}
#[test]fn r1031_uncommitted_python_update_does_not_block_python_rust_ack(){
 let (root,src,dst,v)=pair();
 py(&root.0,"pair_insert");
 py(&root.0,"pair_rollback");
 assert_eq!(r805_replay_window(&root.0,&src,&dst,&v,1),Ok(1));
 assert_eq!(r756_cleanup(&root.0,&src,&dst,&v),Ok((1,0)));
 assert_eq!(r800_streaming_original_parity(&src,&dst,&v),Ok(2));
}
#[test]fn r1032_second_operation_after_hash_forged_rolls_back_db2_first(){
 let (root,src,dst,v)=pair();
 py(&root.0,"pair_insert");
 let (_,_,rows)=r756_snapshot(&root.0,&src,&dst).unwrap();
 let mut frame:Value=serde_json::from_str(rows[0]["payload_json"].as_str().unwrap()).unwrap();
 let previous=frame["operations"][1]["after_hash"].as_str().unwrap().to_owned();
 let replacement=if previous.starts_with('0'){"1".repeat(64)}else{"0".repeat(64)};
 frame["operations"][1]["after_hash"]=Value::String(replacement);
 let raw=r386_python_mirror_canonical(&frame).unwrap();
 assert_eq!(raw.len(),rows[0]["payload_json"].as_str().unwrap().len());
 let digest=r366_sha(&raw);
 let sql=format!("UPDATE mirror_outbox SET payload_json={},payload_hash={} WHERE seq=1",
   r586_sql_literal(&Value::String(raw)).unwrap(),
   r586_sql_literal(&Value::String(digest)).unwrap());
 exec(&src,&sql).unwrap();
 assert_eq!(r805_replay_window(&root.0,&src,&dst,&v,1),
   Err(EventOutboxR366Error::Corrupt));
 assert_eq!(count(&dst,"metadata"),0);
 assert_eq!(count(&dst,"collector_runs"),0);
 assert_eq!(r366_query_num(&dst,"SELECT applied_seq FROM mirror_meta"),Ok(0));
 assert_eq!(r366_query_num(&src,"SELECT cleanup_through FROM mirror_control"),Ok(0));
}
#[test]fn r1033_partial_mixed_replay_never_deletes_unapplied_outbox(){
 let (root,src,dst,v)=pair();
 for mode in ["pair_insert","pair_update","pair_upsert","pair_delete"]{py(&root.0,mode);}
 assert_eq!(r805_replay_window(&root.0,&src,&dst,&v,2),Ok(2));
 assert_eq!(r756_cleanup(&root.0,&src,&dst,&v),
  Err(EventOutboxR366Error::Conflict));
 assert_eq!(count(&src,"mirror_outbox"),4);
 assert_eq!(r805_replay_window(&root.0,&src,&dst,&v,2),Ok(2));
 assert_eq!(r800_streaming_original_parity(&src,&dst,&v),Ok(0));
 assert_eq!(r756_cleanup(&root.0,&src,&dst,&v),Ok((4,0)));
}
