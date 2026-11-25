// Astra G3/G4 R659+: Rust bridge over the EXACT original 17-table
// Python collector SQL, not the synthetic R506 sink_rows projection.
// Both SQLite DBs are strictly separate :memory: databases. This does
// NOT prove disk durability, installed PAPER use, real orders or DB2 ACK.
fn r659_mem_pair()->Result<(Db,Db,serde_json::Value),EventOutboxR366Error>{
 let v=r586_oracle();
 r566_check_source_manifest(&v)?;
 let source=r586_memory_original_schema(&v)?;
 let sink=r586_memory_original_schema(&v)?;
 exec(&source,"CREATE TABLE r659_source_control(
   id INTEGER PRIMARY KEY CHECK(id=1), latest_seq INTEGER NOT NULL,
   ack_seq INTEGER NOT NULL);
   INSERT INTO r659_source_control VALUES(1,0,0);
   CREATE TABLE r659_source_feed(
    seq INTEGER PRIMARY KEY,prev_seq INTEGER NOT NULL,
    payload_hash TEXT NOT NULL,payload_json TEXT NOT NULL);")
    .map_err(r366_error)?;
 exec(&sink,"CREATE TABLE r659_sink_control(
   id INTEGER PRIMARY KEY CHECK(id=1),applied_seq INTEGER NOT NULL,
   payload_hash TEXT NOT NULL);
   INSERT INTO r659_sink_control VALUES(1,0,'GENESIS');")
    .map_err(r366_error)?;
 Ok((source,sink,v))
}
fn r659_op(v:&serde_json::Value,name:&str,row:&serde_json::Value,kind:&str)
 ->Result<serde_json::Value,EventOutboxR366Error>{
 let fields=v["source_keys"][name].as_array().ok_or(EventOutboxR366Error::Schema)?;
 let mut key=serde_json::Map::new();
 for field in fields{
  let s=field.as_str().ok_or(EventOutboxR366Error::Schema)?;
  key.insert(s.to_string(),row.get(s).ok_or(EventOutboxR366Error::Schema)?.clone());
 }
 if kind=="delete"{
  return Ok(serde_json::json!({"table":name,"op":kind,"key":key}))
 }
 let canonical=r386_python_mirror_canonical(row)?;
 Ok(serde_json::json!({"table":name,"op":kind,"key":key,
  "row":row,"after_hash":r366_sha(&canonical)}))
}
fn r659_append(source:&Db,manifest:&serde_json::Value,operations:&[serde_json::Value])
 ->Result<u64,EventOutboxR366Error>{
 if operations.is_empty()||operations.len()>17{return Err(EventOutboxR366Error::Schema)}
 let tx=Tx::begin(source).map_err(r366_error)?;
 let prior=r366_query_num(source,"SELECT latest_seq FROM r659_source_control WHERE id=1")?;
 let seq=prior.checked_add(1).ok_or(EventOutboxR366Error::Corrupt)?;
 for op in operations{
  r586_apply_original_operation(source,manifest,op)?;
 }
 let frame=serde_json::json!({"protocol_version":1,"schema_generation":1,
  "tx_kind":"original_17table_research","operations":operations});
 let raw=r386_python_mirror_canonical(&frame)?;
 let hash=r366_sha(&raw);
 let sql=format!("INSERT INTO r659_source_feed VALUES({seq},{prior},{},{});
  UPDATE r659_source_control SET latest_seq={seq} WHERE id=1;",
  r586_sql_literal(&serde_json::json!(hash))?,
  r586_sql_literal(&serde_json::json!(raw))?);
 exec(source,&sql).map_err(r366_error)?;
 tx.commit().map_err(r366_error)?;
 Ok(seq)
}
fn r659_source_frame(source:&Db,seq:u64)
 ->Result<(u64,String,String),EventOutboxR366Error>{
 let mut q=Stmt::new(source,
   "SELECT prev_seq,payload_hash,payload_json FROM r659_source_feed WHERE seq=?1")
   .map_err(r366_error)?;
 q.number(1,seq).map_err(r366_error)?;
 if q.step().map_err(r366_error)?!=SQLITE_ROW{
  return Err(EventOutboxR366Error::Conflict)
 }
 let prev=q.num(0).map_err(r366_error)?;
 let hash=q.strval(1).map_err(r366_error)?;
 let raw=q.strval(2).map_err(r366_error)?;
 if q.step().map_err(r366_error)?!=SQLITE_DONE
  ||r366_sha(&raw)!=hash{return Err(EventOutboxR366Error::Corrupt)}
 Ok((prev,hash,raw))
}
fn r659_replay(source:&Db,sink:&Db,manifest:&serde_json::Value,seq:u64,
 fail_before_sink_commit:bool)->Result<(),EventOutboxR366Error>{
 let applied=r366_query_num(sink,"SELECT applied_seq FROM r659_sink_control WHERE id=1")?;
 if seq<=applied{return Err(EventOutboxR366Error::Duplicate)}
 if seq!=applied+1{return Err(EventOutboxR366Error::Conflict)}
 let (prev,hash,raw)=r659_source_frame(source,seq)?;
 if prev!=applied{return Err(EventOutboxR366Error::Corrupt)}
 let frame:serde_json::Value=serde_json::from_str(&raw)
  .map_err(|_|EventOutboxR366Error::Corrupt)?;
 if frame["protocol_version"]!=1||frame["schema_generation"]!=1
   ||frame["tx_kind"]!="original_17table_research"
   ||r386_python_mirror_canonical(&frame)?!=raw{
  return Err(EventOutboxR366Error::Corrupt)
 }
 let ops=frame["operations"].as_array().ok_or(EventOutboxR366Error::Corrupt)?;
 if ops.is_empty()||ops.len()>17{return Err(EventOutboxR366Error::Corrupt)}
 let tx=Tx::begin(sink).map_err(r366_error)?;
 for op in ops{r586_apply_original_operation(sink,manifest,op)?;}
 if fail_before_sink_commit{return Err(EventOutboxR366Error::Interrupted)}
 let sql=format!("UPDATE r659_sink_control SET applied_seq={seq},
  payload_hash={} WHERE id=1;",
  r586_sql_literal(&serde_json::json!(hash))?);
 exec(sink,&sql).map_err(r366_error)?;
 tx.commit().map_err(r366_error)?;
 Ok(())
}
fn r659_ack_cleanup(source:&Db,sink:&Db,seq:u64)
 ->Result<(),EventOutboxR366Error>{
 if r366_query_num(sink,"SELECT applied_seq FROM r659_sink_control WHERE id=1")?!=seq
   ||r366_query_num(source,"SELECT latest_seq FROM r659_source_control WHERE id=1")?!=seq{
  return Err(EventOutboxR366Error::Conflict)
 }
 let (_,hash,_)=r659_source_frame(source,seq)?;
 let mut q=Stmt::new(sink,"SELECT payload_hash FROM r659_sink_control WHERE id=1")
  .map_err(r366_error)?;
 if q.step().map_err(r366_error)?!=SQLITE_ROW
   ||q.strval(0).map_err(r366_error)?!=hash{
  return Err(EventOutboxR366Error::Corrupt)
 }
 drop(q);
 let tx=Tx::begin(source).map_err(r366_error)?;
 let sql=format!("DELETE FROM r659_source_feed WHERE seq<={seq};
  UPDATE r659_source_control SET ack_seq={seq} WHERE id=1;");
 exec(source,&sql).map_err(r366_error)?;
 tx.commit().map_err(r366_error)?;
 Ok(())
}

#[cfg(test)]
mod r659_tests{
 use super::*;
 use serde_json::{Value,json};
 fn op_for(v:&Value,name:&str,kind:&str)->Value{
  let row=v["fixture_snapshot"]["tables"].as_array().unwrap().iter()
   .find(|t|t["schema"]["table"]==name).unwrap()["rows"][0]["row"].clone();
  r659_op(v,name,&row,kind).unwrap()
 }
 fn six(v:&Value)->Vec<Value>{
  ["attempts","collector_runs","events","decisions","hourly_raw_rollups","metadata"]
   .iter().map(|name|op_for(v,name,"insert")).collect()
 }
 fn pair()->(Db,Db,Value){r659_mem_pair().unwrap()}
 fn count(db:&Db,table:&str)->u64{
  r366_query_num(db,&format!("SELECT COUNT(*) FROM {table}")).unwrap()
 }
 fn seed()->(Db,Db,Value){
  let (src,dst,v)=pair();
  assert_eq!(r659_append(&src,&v,&six(&v)),Ok(1));
  (src,dst,v)
 }
 #[test]fn r659_two_independent_original_sqlite_schemas_all_17(){
  let (src,dst,v)=pair();
  for table in v["table_schema"].as_array().unwrap(){
   let name=table["table"].as_str().unwrap();
   assert_eq!(count(&src,name),0);
   assert_eq!(count(&dst,name),0);
  }
  assert_eq!(r366_query_num(&src,"PRAGMA user_version").unwrap(),4);
  assert_eq!(r366_query_num(&dst,"PRAGMA user_version").unwrap(),4);
 }
 #[test]fn r660_original_six_related_rows_arrive_in_one_atomic_source_frame(){
  let (src,dst,v)=seed();
  assert_eq!(count(&src,"decisions"),1);
  assert_eq!(count(&dst,"decisions"),0);
  assert_eq!(count(&src,"r659_source_feed"),1);
  assert_eq!(r659_replay(&src,&dst,&v,1,false),Ok(()));
  for table in ["attempts","collector_runs","events","decisions",
    "hourly_raw_rollups","metadata"]{
   assert_eq!(count(&src,table),count(&dst,table));
  }
  assert_eq!(r366_query_num(&dst,"SELECT COUNT(*) FROM pragma_foreign_key_check").unwrap(),0);
 }
 #[test]fn r661_interrupted_sink_transaction_rolls_back_rows_and_ack(){
  let (src,dst,v)=seed();
  assert_eq!(r659_replay(&src,&dst,&v,1,true),
   Err(EventOutboxR366Error::Interrupted));
  assert_eq!(count(&dst,"events"),0);
  assert_eq!(count(&dst,"decisions"),0);
  assert_eq!(r366_query_num(&dst,"SELECT applied_seq FROM r659_sink_control").unwrap(),0);
  assert_eq!(r659_replay(&src,&dst,&v,1,false),Ok(()));
 }
 #[test]fn r662_ack_before_sink_commit_is_forbidden(){
  let (src,dst,_)=seed();
  assert_eq!(r659_ack_cleanup(&src,&dst,1),Err(EventOutboxR366Error::Conflict));
  assert_eq!(count(&src,"r659_source_feed"),1);
 }
 #[test]fn r663_duplicate_delivery_does_not_duplicate_original_rows(){
  let (src,dst,v)=seed();
  r659_replay(&src,&dst,&v,1,false).unwrap();
  assert_eq!(r659_replay(&src,&dst,&v,1,false),
   Err(EventOutboxR366Error::Duplicate));
  assert_eq!(count(&dst,"events"),1);
 }
 #[test]fn r664_source_frame_hash_tamper_blocks_full_sink_tx(){
  let (src,dst,v)=seed();
  exec(&src,"UPDATE r659_source_feed SET payload_hash='forged' WHERE seq=1").unwrap();
  assert_eq!(r659_replay(&src,&dst,&v,1,false),
   Err(EventOutboxR366Error::Corrupt));
  assert_eq!(count(&dst,"events"),0);
 }
 #[test]fn r665_source_insert_with_bad_fk_rolls_back_entire_source_frame(){
  let (src,dst,v)=pair();
  let mut ops=six(&v);
  ops[3]["row"]["event_id"]=json!("orphan-r665");
  ops[3]["key"]["event_id"]=json!("orphan-r665");
  ops[3]["after_hash"]=json!(r366_sha(
    &r386_python_mirror_canonical(&ops[3]["row"]).unwrap()));
  assert!(r659_append(&src,&v,&ops).is_err());
  assert_eq!(count(&src,"events"),0);
  assert_eq!(count(&src,"attempts"),0);
  assert_eq!(count(&src,"r659_source_feed"),0);
  assert_eq!(count(&dst,"events"),0);
 }
 #[test]fn r666_after_image_hash_forgery_with_rehashed_frame_blocks(){
  let (src,dst,v)=seed();
  let (_,_,raw)=r659_source_frame(&src,1).unwrap();
  let mut frame:Value=serde_json::from_str(&raw).unwrap();
  frame["operations"][3]["after_hash"]=json!("0".repeat(64));
  let canonical=r386_python_mirror_canonical(&frame).unwrap();
  let sql=format!("UPDATE r659_source_feed SET payload_hash={},payload_json={} WHERE seq=1",
    r586_sql_literal(&json!(r366_sha(&canonical))).unwrap(),
    r586_sql_literal(&json!(canonical)).unwrap());
  exec(&src,&sql).unwrap();
  assert_eq!(r659_replay(&src,&dst,&v,1,false),
   Err(EventOutboxR366Error::Corrupt));
  assert_eq!(count(&dst,"events"),0);
  assert_eq!(count(&dst,"decisions"),0);
 }
 #[test]fn r667_durable_ordering_model_cleanup_only_after_matching_sink_ack(){
  let (src,dst,v)=seed();
  r659_replay(&src,&dst,&v,1,false).unwrap();
  r659_ack_cleanup(&src,&dst,1).unwrap();
  assert_eq!(count(&src,"r659_source_feed"),0);
  assert_eq!(r366_query_num(&src,"SELECT ack_seq FROM r659_source_control").unwrap(),1);
  assert_eq!(count(&dst,"events"),1);
  assert_eq!(r659_ack_cleanup(&src,&dst,1),Err(EventOutboxR366Error::Conflict));
 }
 #[test]fn r668_original_eight_column_rollup_key_upserts_after_ack(){
  let (src,dst,v)=seed();
  r659_replay(&src,&dst,&v,1,false).unwrap();
  r659_ack_cleanup(&src,&dst,1).unwrap();
  let original=v["fixture_snapshot"]["tables"].as_array().unwrap().iter()
    .find(|t|t["schema"]["table"]=="hourly_raw_rollups").unwrap()["rows"][0]["row"].clone();
  let mut newer=original.clone();
  newer["event_count"]=json!(101);
  assert_eq!(r659_append(&src,&v,&[r659_op(&v,"hourly_raw_rollups",&newer,"upsert").unwrap()]),Ok(2));
  r659_replay(&src,&dst,&v,2,false).unwrap();
  assert_eq!(count(&dst,"hourly_raw_rollups"),1);
  r659_ack_cleanup(&src,&dst,2).unwrap();
  assert_eq!(count(&src,"r659_source_feed"),0);
 }
 #[test]fn r669_original_parent_child_delete_preserves_fk_rules(){
  let (src,dst,v)=seed();
  r659_replay(&src,&dst,&v,1,false).unwrap();
  r659_ack_cleanup(&src,&dst,1).unwrap();
  let ops=vec![op_for(&v,"decisions","delete"),op_for(&v,"events","delete")];
  assert_eq!(r659_append(&src,&v,&ops),Ok(2));
  r659_replay(&src,&dst,&v,2,false).unwrap();
  assert_eq!(count(&src,"decisions"),0);
  assert_eq!(count(&dst,"decisions"),0);
  assert_eq!(count(&dst,"events"),0);
  assert_eq!(r366_query_num(&dst,"SELECT COUNT(*) FROM pragma_foreign_key_check").unwrap(),0);
 }
 #[test]fn r670_out_of_order_delivery_and_fake_prior_seq_deny(){
  let (src,dst,v)=seed();
  assert_eq!(r659_replay(&src,&dst,&v,2,false),
   Err(EventOutboxR366Error::Conflict));
  exec(&src,"UPDATE r659_source_feed SET prev_seq=18 WHERE seq=1").unwrap();
  assert_eq!(r659_replay(&src,&dst,&v,1,false),
   Err(EventOutboxR366Error::Corrupt));
  assert_eq!(count(&dst,"events"),0);
 }
}
