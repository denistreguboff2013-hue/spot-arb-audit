// R586-R595: original-shaped SQLite in-memory operation parity only.
// Never opens production paths. Source manifest is R566 pinned original Python SQL.
// This is not a durable installed mirror receiver or exchange acceptance.
fn r586_oracle() -> serde_json::Value {
 serde_json::from_str(include_str!("../config/native_r566_original_collector_schema_oracle.json"))
  .expect("pinned R566 manifest")
}
fn r586_memory_original_schema(v:&serde_json::Value)->Result<Db,EventOutboxR366Error>{
 r566_check_source_manifest(v)?;
 let p=CString::new(":memory:").map_err(|_|EventOutboxR366Error::Schema)?;
 let mut raw=std::ptr::null_mut();
 if unsafe{sqlite3_open_v2(p.as_ptr(),&mut raw,6,std::ptr::null())}!=SQLITE_OK
  ||raw.is_null(){if !raw.is_null(){unsafe{sqlite3_close(raw);}}
  return Err(EventOutboxR366Error::Sql)}
 let db=Db(raw);
 exec(&db,"PRAGMA foreign_keys=ON;PRAGMA user_version=4;").map_err(r366_error)?;
 for schema in v["table_schema"].as_array().ok_or(EventOutboxR366Error::Schema)? {
  exec(&db,schema["ddl"].as_str().ok_or(EventOutboxR366Error::Schema)?)
   .map_err(r366_error)?;
 }
 for schema in v["table_schema"].as_array().ok_or(EventOutboxR366Error::Schema)? {
  for idx in schema["indexes"].as_array().ok_or(EventOutboxR366Error::Schema)? {
   if let Some(ddl)=idx["definition"].as_str(){exec(&db,ddl).map_err(r366_error)?;}
  }
  for trigger in schema["triggers"].as_array().ok_or(EventOutboxR366Error::Schema)? {
   exec(&db,trigger[1].as_str().ok_or(EventOutboxR366Error::Schema)?)
    .map_err(r366_error)?;
  }
 }
 Ok(db)
}
fn r586_sql_literal(v:&serde_json::Value)->Result<String,EventOutboxR366Error>{
 match v{
  serde_json::Value::Null=>Ok("NULL".to_string()),
  serde_json::Value::String(s)=>{
   if s.contains('\0'){return Err(EventOutboxR366Error::Corrupt)}
   Ok(format!("'{}'",s.replace("'","''")))
  }
  serde_json::Value::Number(n)=>Ok(n.to_string()),
  serde_json::Value::Bool(b)=>Ok(if *b{"1"}else{"0"}.to_string()),
  _=>Err(EventOutboxR366Error::Corrupt),
 }
}
fn r586_identifier(s:&str)->Result<String,EventOutboxR366Error>{
 if s.is_empty()||!s.bytes().all(|b|b.is_ascii_alphanumeric()||b==b'_'){
  return Err(EventOutboxR366Error::Schema)
 }
 Ok(format!("\"{s}\""))
}
fn r586_apply_original_operation(db:&Db,manifest:&serde_json::Value,op:&serde_json::Value)
 ->Result<(),EventOutboxR366Error>{
 let name=op["table"].as_str().ok_or(EventOutboxR366Error::Schema)?;
 let schema=manifest["table_schema"].as_array().ok_or(EventOutboxR366Error::Schema)?
  .iter().find(|x|x["table"]==name).ok_or(EventOutboxR366Error::Schema)?;
 let key_fields=manifest["source_keys"][name].as_array().ok_or(EventOutboxR366Error::Schema)?;
 let key=op["key"].as_object().ok_or(EventOutboxR366Error::Corrupt)?;
 let expected=key_fields.iter().map(|k|k.as_str()).collect::<Option<Vec<_>>>()
  .ok_or(EventOutboxR366Error::Schema)?;
 if key.len()!=expected.len()||!expected.iter().all(|k|key.contains_key(*k)){
  return Err(EventOutboxR366Error::Corrupt)
 }
 let tbl=r586_identifier(name)?;
 let where_sql=expected.iter().map(|k|{
  Ok(format!("{}={}",r586_identifier(k)?,r586_sql_literal(&key[*k])?))
 }).collect::<Result<Vec<_>,EventOutboxR366Error>>()?.join(" AND ");
 let kind=op["op"].as_str().ok_or(EventOutboxR366Error::Corrupt)?;
 let sql=if kind=="delete" {
  if op.get("row").is_some()||op.get("after_hash").is_some(){
   return Err(EventOutboxR366Error::Corrupt)
  }
  format!("DELETE FROM {tbl} WHERE {where_sql}")
 } else {
  if !matches!(kind,"insert"|"upsert"|"update"){
   return Err(EventOutboxR366Error::Corrupt)
  }
  let row=op["row"].as_object().ok_or(EventOutboxR366Error::Corrupt)?;
  let cols=schema["columns"].as_array().ok_or(EventOutboxR366Error::Schema)?
   .iter().map(|col|col[1].as_str()).collect::<Option<Vec<_>>>()
   .ok_or(EventOutboxR366Error::Schema)?;
  if row.len()!=cols.len()||!cols.iter().all(|c|row.contains_key(*c))
   ||!expected.iter().all(|k|key[*k]==row[*k]){
    return Err(EventOutboxR366Error::Corrupt)
  }
  let canonical=r386_python_mirror_canonical(&op["row"])?;
  if op["after_hash"]!=r366_sha(&canonical){return Err(EventOutboxR366Error::Corrupt)}
  if kind=="update" {
   let set=cols.iter().filter(|c|!expected.contains(c))
    .map(|c|Ok(format!("{}={}",r586_identifier(c)?,r586_sql_literal(&row[*c])?)))
    .collect::<Result<Vec<_>,EventOutboxR366Error>>()?.join(",");
   if set.is_empty(){return Err(EventOutboxR366Error::Schema)}
   format!("UPDATE {tbl} SET {set} WHERE {where_sql}")
  }else{
   let names=cols.iter().map(|c|r586_identifier(c))
    .collect::<Result<Vec<_>,_>>()?.join(",");
   let vals=cols.iter().map(|c|r586_sql_literal(&row[*c]))
    .collect::<Result<Vec<_>,_>>()?.join(",");
   let mut statement=format!("INSERT INTO {tbl} ({names}) VALUES ({vals})");
   if kind=="upsert" {
    let conflicts=expected.iter().map(|c|r586_identifier(c))
     .collect::<Result<Vec<_>,_>>()?.join(",");
    let sets=cols.iter().filter(|c|!expected.contains(c)).map(|c|{
     let s=r586_identifier(c)?;Ok(format!("{s}=excluded.{s}"))
    }).collect::<Result<Vec<_>,EventOutboxR366Error>>()?.join(",");
    if sets.is_empty(){return Err(EventOutboxR366Error::Schema)}
    statement.push_str(&format!(" ON CONFLICT({conflicts}) DO UPDATE SET {sets}"));
   }
   statement
  }
 };
 exec(db,&sql).map_err(r366_error)?;
 if unsafe{sqlite3_changes(db.0)}!=1 {return Err(EventOutboxR366Error::Corrupt)}
 Ok(())
}

#[cfg(test)]
mod r586_operation_tests{
 use super::*;
 use serde_json::{json,Value};
 fn record(name:&str)->(Value,Value){
  let v=r586_oracle();
  let item=v["fixture_snapshot"]["tables"].as_array().unwrap().iter()
   .find(|t|t["schema"]["table"]==name).unwrap();
  let row=item["rows"][0]["row"].clone();
  (v,row)
 }
 fn op(v:&Value,name:&str,row:&Value,kind:&str)->Value{
  let ks=v["source_keys"][name].as_array().unwrap();
  let mut k=serde_json::Map::new();
  for n in ks{let n=n.as_str().unwrap();k.insert(n.to_string(),row[n].clone());}
  let mut x=json!({"table":name,"op":kind,"key":k,"row":row});
  x["after_hash"]=json!(r366_sha(&r386_python_mirror_canonical(row).unwrap()));
  x
 }
 fn count(db:&Db,name:&str)->u64{
  r366_query_num(db,&format!("SELECT COUNT(*) FROM {}",r586_identifier(name).unwrap())).unwrap()
 }
 #[test]fn r586_source_rows_replay_on_original_sql_in_memory(){
  let v=r586_oracle();let db=r586_memory_original_schema(&v).unwrap();
  for name in ["attempts","collector_runs","events","decisions","hourly_raw_rollups","metadata"]{
   let (_,row)=record(name);
   r586_apply_original_operation(&db,&v,&op(&v,name,&row,"insert")).unwrap();
   assert_eq!(count(&db,name),1);
  }
  assert_eq!(r366_query_num(&db,"SELECT COUNT(*) FROM events").unwrap(),1);
 }
 #[test]fn r587_exact_original_update_after_image_and_hash(){
  let (v,mut row)=record("metadata");let db=r586_memory_original_schema(&v).unwrap();
  r586_apply_original_operation(&db,&v,&op(&v,"metadata",&row,"insert")).unwrap();
  row["value"]=json!("changed-after");
  r586_apply_original_operation(&db,&v,&op(&v,"metadata",&row,"update")).unwrap();
  let mut q=Stmt::new(&db,"SELECT value FROM metadata").unwrap();
  assert_eq!(q.step().unwrap(),SQLITE_ROW);
  assert_eq!(q.strval(0).unwrap(),"changed-after");
 }
 #[test]fn r588_eight_part_rollup_key_upsert(){
  let (v,mut row)=record("hourly_raw_rollups");
  let db=r586_memory_original_schema(&v).unwrap();
  r586_apply_original_operation(&db,&v,&op(&v,"hourly_raw_rollups",&row,"insert")).unwrap();
  let nonkey=v["table_schema"].as_array().unwrap().iter()
   .find(|t|t["table"]=="hourly_raw_rollups").unwrap()["columns"].as_array().unwrap()
   .iter().map(|c|c[1].as_str().unwrap())
   .find(|n|!v["source_keys"]["hourly_raw_rollups"].as_array().unwrap()
     .iter().any(|k|k==*n)).unwrap().to_string();
  row[&nonkey]=json!(15);
  r586_apply_original_operation(&db,&v,&op(&v,"hourly_raw_rollups",&row,"upsert")).unwrap();
  assert_eq!(count(&db,"hourly_raw_rollups"),1);
 }
 #[test]fn r589_real_delete_changes_original_table(){
  let (v,row)=record("metadata");let db=r586_memory_original_schema(&v).unwrap();
  r586_apply_original_operation(&db,&v,&op(&v,"metadata",&row,"insert")).unwrap();
  let mut del=op(&v,"metadata",&row,"insert");
  del["op"]=json!("delete");
  del.as_object_mut().unwrap().remove("row");
  del.as_object_mut().unwrap().remove("after_hash");
  r586_apply_original_operation(&db,&v,&del).unwrap();
  assert_eq!(count(&db,"metadata"),0);
  assert!(r586_apply_original_operation(&db,&v,&del).is_err());
 }
 #[test]fn r590_original_foreign_key_orphan_refused(){
  let (v,mut row)=record("decisions");
  let db=r586_memory_original_schema(&v).unwrap();
  row["event_id"]=json!("orphan-r590");
  assert!(r586_apply_original_operation(&db,&v,&op(&v,"decisions",&row,"insert")).is_err());
  assert_eq!(count(&db,"decisions"),0);
 }
 #[test]fn r591_duplicate_insert_not_silently_replayed(){
  let (v,row)=record("metadata");let db=r586_memory_original_schema(&v).unwrap();
  let operation=op(&v,"metadata",&row,"insert");
  r586_apply_original_operation(&db,&v,&operation).unwrap();
  assert!(r586_apply_original_operation(&db,&v,&operation).is_err());
  assert_eq!(count(&db,"metadata"),1);
 }
 #[test]fn r592_wrong_original_key_rejected(){
  let (v,row)=record("metadata");let db=r586_memory_original_schema(&v).unwrap();
  let mut operation=op(&v,"metadata",&row,"insert");
  operation["key"]=json!({"different":"no"});
  assert_eq!(r586_apply_original_operation(&db,&v,&operation),
   Err(EventOutboxR366Error::Corrupt));
 }
 #[test]fn r593_after_image_hash_tamper_rejected(){
  let (v,row)=record("metadata");let db=r586_memory_original_schema(&v).unwrap();
  let mut operation=op(&v,"metadata",&row,"insert");
  operation["after_hash"]=json!("forged");
  assert_eq!(r586_apply_original_operation(&db,&v,&operation),
   Err(EventOutboxR366Error::Corrupt));
  assert_eq!(count(&db,"metadata"),0);
 }
 #[test]fn r594_rollback_original_sql_does_not_publish_partial_row(){
  let (v,row)=record("metadata");let db=r586_memory_original_schema(&v).unwrap();
  {
   let _tx=Tx::begin(&db).unwrap();
   r586_apply_original_operation(&db,&v,&op(&v,"metadata",&row,"insert")).unwrap();
   assert_eq!(count(&db,"metadata"),1);
  }
  assert_eq!(count(&db,"metadata"),0);
 }
 #[test]fn r595_apostrophe_in_row_cannot_inject_sql(){
  let (v,mut row)=record("metadata");let db=r586_memory_original_schema(&v).unwrap();
  row["value"]=json!("it's safe'); DROP TABLE events; --");
  r586_apply_original_operation(&db,&v,&op(&v,"metadata",&row,"insert")).unwrap();
  assert_eq!(count(&db,"metadata"),1);
  assert_eq!(count(&db,"events"),0);
  let mut q=Stmt::new(&db,"SELECT value FROM metadata").unwrap();
  assert_eq!(q.step().unwrap(),SQLITE_ROW);
  assert_eq!(q.strval(0).unwrap(),"it's safe'); DROP TABLE events; --");
 }
}
