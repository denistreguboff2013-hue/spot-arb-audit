#[test] fn r951_prepared_replay_exact_three_envelopes_parity(){
 let (root,h,_)=pair(3);let v=r586_oracle();
 r924_install_private_paper_only_guards(&h.source,&h.sink,&v).unwrap();
 assert_eq!(r951_guarded_verified_burst(&root.0,&h.source,&h.sink,&v,3),Ok(3));
 assert_eq!(r800_streaming_original_parity(&h.source,&h.sink,&v),Ok(96));
 assert_eq!(r879_private_paper_prefix_parity(
  &h.source,&h.sink,&v,"r879_probe",3),Ok(96));
}
#[test] fn r952_prepared_replay_rejects_incorrect_after_hash(){
 let (_root,h,_)=pair(1);let v=r586_oracle();
 r924_install_private_paper_only_guards(&h.source,&h.sink,&v).unwrap();
 let tx=Tx::begin(&h.sink).unwrap();
 let cols=r951_original_events_projection(&v).unwrap();
 let mut stmt=r951_prepared_insert(&h.sink,&cols).unwrap();
 let mut q=Stmt::new(&h.source,&r805_window_sql(R726_OUTBOX,"ORDER BY seq").unwrap()).unwrap();
 assert_eq!(q.step().unwrap(),SQLITE_ROW);
 let row:serde_json::Value=serde_json::from_str(&q.strval(0).unwrap()).unwrap();
 let payload:serde_json::Value=serde_json::from_str(r726_s(&row,"payload_json").unwrap()).unwrap();
 let mut op=payload["operations"][0].clone();
 op["after_hash"]="bad-after-hash".into();
 assert_eq!(r951_bind_and_insert(&mut stmt,&h.sink,&cols,&op),
  Err(EventOutboxR366Error::Corrupt));
 drop(tx); // dest transaction rolls back
 assert_eq!(r366_query_num(&h.sink,"SELECT COUNT(*) FROM events"),Ok(0));
 assert_eq!(r366_query_num(&h.sink,"SELECT applied_seq FROM mirror_meta"),Ok(0));
 assert_eq!(r366_query_num(&h.source,"SELECT cleanup_through FROM mirror_control"),Ok(0));
}

#[test] fn r953_private_db2_2048_replay_prepared_vs_original_microprofile(){
 use std::time::Instant;
 for prepared in [false,true]{
  let (root,h,_)=pair(64);let v=r586_oracle();
  r924_install_private_paper_only_guards(&h.source,&h.sink,&v).unwrap();
  let start=Instant::now();
  let rows=if prepared{
   r951_guarded_verified_burst(&root.0,&h.source,&h.sink,&v,64)
  }else{
   r931_guarded_verified_burst(&root.0,&h.source,&h.sink,&v,64)
  };
  assert_eq!(rows,Ok(64));
  let ms=start.elapsed().as_secs_f64()*1000.0;
  assert_eq!(r800_streaming_original_parity(&h.source,&h.sink,&v),Ok(2048));
  eprintln!("R953 isolated verified 2048 event replay prepared={prepared} elapsed_ms={ms:.2} rate={:.1}/s; not production capacity",2048.0/(ms/1000.0));
 }
}
