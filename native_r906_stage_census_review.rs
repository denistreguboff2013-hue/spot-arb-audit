// Astra P0-01 R906: isolate the unavoidable sources of receiver ACK latency.
// DO NOT weaken any production predicate; run the canonical validators on
// separate private SQLite 17-table fixtures and report component timings.
#[cfg(test)]
mod r906_tests {
 use super::*;
 use std::time::Instant;
 fn one(batches:u64) {
  let (root,h,_owner)=super::pair(batches);
  let oracle=r586_oracle();
  assert_eq!(r845_verified_burst(&root.0,&h.source,&h.sink,&oracle,128),Ok(batches));
  exec(&h.source,"BEGIN DEFERRED").unwrap();
  let started=Instant::now();
  let (head,control,_)=r805_read_head(&root.0,&h.source,&h.sink).unwrap();
  let head_ms=started.elapsed().as_secs_f64()*1000.0;
  let started=Instant::now();
  r805_verify_chain(&h.source,&head,r726_s(&control,"feed_generation").unwrap()).unwrap();
  let chain_ms=started.elapsed().as_secs_f64()*1000.0;
  let started=Instant::now();
  assert_eq!(r879_private_paper_prefix_parity(&h.source,&h.sink,&oracle,"r879_probe",head.applied),Ok(batches*32));
  let parity_ms=started.elapsed().as_secs_f64()*1000.0;
  let mut rows=std::collections::BTreeMap::new();
  let sql=r805_window_sql(R726_OUTBOX,"ORDER BY seq").unwrap();
  let mut q=Stmt::new(&h.source,&sql).unwrap();
  while q.step().unwrap()==SQLITE_ROW{
   let record:serde_json::Value=serde_json::from_str(&q.strval(0).unwrap()).unwrap();
   let frame:serde_json::Value=serde_json::from_str(r726_s(&record,"payload_json").unwrap()).unwrap();
   for op in frame["operations"].as_array().unwrap(){
    let id=op["row"]["event_id"].as_str().unwrap().to_string();
    assert!(rows.insert(id,op["row"].clone()).is_none());
   }
  }
  drop(q);
  let started=Instant::now();
  r895_verify_new_paper_rows(&h.source,&h.sink,&oracle,&rows).unwrap();
  let bind_ms=started.elapsed().as_secs_f64()*1000.0;
  exec(&h.source,"ROLLBACK").unwrap();
  let started=Instant::now();
  assert_eq!(r879_private_paper_prefix_ack(&root.0,&h.source,&h.sink,&oracle,"r879_probe"),Ok((batches,0)));
  let actual_ack_ms=started.elapsed().as_secs_f64()*1000.0;
  let estimated=head_ms+chain_ms+parity_ms+bind_ms;
  eprintln!("R906 PRIVATE events={} steps_ms head={:.2} chain={:.2} 17table_parity={:.2} physical_row_binding={:.2} component_sum={:.2} full_atomic_ack={:.2}; isolated non-production diagnostic",
   batches*32,head_ms,chain_ms,parity_ms,bind_ms,estimated,actual_ack_ms);
  assert_eq!(r366_query_num(&h.source,"SELECT retained_records FROM mirror_control"),Ok(0));
  assert_eq!(r800_streaming_original_parity(&h.source,&h.sink,&oracle),Ok(batches*32));
 }
 #[test] fn r906_verified_ack_cost_by_512_1024_2048_events(){
  for n in [16,32,64]{one(n)}
 }
}
