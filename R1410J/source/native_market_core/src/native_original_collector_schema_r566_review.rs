// R566–R575: source-extracted original Python DB1 17-table shape oracle.
// All SQL database recreation, if exercised, is SQLite :memory: ONLY.
const R566_SOURCE_MANIFEST_DIGEST:&str=
 "6dbb41f609ce9323ca5f2c799b5024815324af37db365844cd71ce74e94e39ad";
fn r566_check_source_manifest(v:&serde_json::Value)
 ->Result<(),EventOutboxR366Error>{
 let check=|| -> Option<()>{
  let schemas=v["table_schema"].as_array()?;
  let tables=v["fixture_snapshot"]["tables"].as_array()?;
  let keys=v["source_keys"].as_object()?;
  let checks=v["checks"].as_object()?;
  if schemas.len()!=17||tables.len()!=17||keys.len()!=17||
   checks.len()!=10||!checks.values().all(|b|b.as_bool()==Some(true))||
   v["mirrored_tables"]!=17||v["schema_version"]!=4||
   v["fixture_rows"]!=6||v["fixture_snapshot"]["total_rows"]!=6||
   v["fixture_snapshot"]["user_version"]!=4||
   v["control_tables"]!=serde_json::json!(["mirror_outbox","mirror_control"]){
    return None
  }
  let snapshot=r386_python_mirror_canonical(&v["fixture_snapshot"]).ok()?;
  if r366_sha(&snapshot)!=R566_SOURCE_MANIFEST_DIGEST||
   v["fixture_snapshot_sha256"]!=R566_SOURCE_MANIFEST_DIGEST{
   return None
  }
  let mut prior="".to_string();
  let mut total=0_u64;
  for (schema,fixture) in schemas.iter().zip(tables){
   if schema!=&fixture["schema"]{return None}
   let name=schema["table"].as_str()?;
   if name<=prior.as_str()||keys.get(name)?!=&schema["mirror_key"]{
    return None
   }
   prior=name.to_string();
   if !schema["ddl"].as_str()?.to_ascii_uppercase().contains("CREATE TABLE"){
    return None
   }
   let cols=schema["columns"].as_array()?;
   let mut pk=Vec::<(u64,String)>::new();
   for col in cols{
    let a=col.as_array()?;
    if a.len()!=6{return None}
    let keyrank=a[5].as_u64()?;
    if keyrank>0{pk.push((keyrank,a[1].as_str()?.to_string()))}
   }
   pk.sort_by_key(|p|p.0);
   let names=pk.into_iter().map(|p|p.1).collect::<Vec<_>>();
   let expected=schema["mirror_key"].as_array()?.iter()
      .map(|x|x.as_str().map(str::to_string))
      .collect::<Option<Vec<_>>>()?;
   if names!=expected{return None}
   let rows=fixture["rows"].as_array()?;
   if rows.len() as u64!=fixture["row_count"].as_u64()?{return None}
   total=total.checked_add(rows.len() as u64)?;
   for row in rows{
    let key=row["key"].as_array()?;
    if key.len()!=expected.len(){return None}
    for (idx,k) in expected.iter().enumerate(){
     if row["row"][k]!=key[idx]{return None}
    }
   }
  }
  if total!=6||v["fixture_snapshot"]["table_count"]!=17{return None}
  Some(())
 };
 check().ok_or(EventOutboxR366Error::Corrupt)
}
fn r566_recreate_original_schema_in_memory(v:&serde_json::Value)
 ->Result<(u64,u64),EventOutboxR366Error>{
 r566_check_source_manifest(v)?;
 let name=CString::new(":memory:").map_err(|_|EventOutboxR366Error::Schema)?;
 let mut ptr=std::ptr::null_mut();
 // SQLite flags: OPEN_READWRITE(2) | OPEN_CREATE(4), only :memory:.
 if unsafe{sqlite3_open_v2(name.as_ptr(),&mut ptr,6,std::ptr::null())}!=SQLITE_OK
  ||ptr.is_null(){
  if !ptr.is_null(){unsafe{sqlite3_close(ptr);}}
  return Err(EventOutboxR366Error::Sql)
 }
 let db=Db(ptr);
 exec(&db,"PRAGMA foreign_keys=ON;PRAGMA user_version=4;")
   .map_err(r366_error)?;
 let schema=v["table_schema"].as_array().ok_or(EventOutboxR366Error::Schema)?;
 for t in schema{
  exec(&db,t["ddl"].as_str().ok_or(EventOutboxR366Error::Schema)?)
    .map_err(r366_error)?;
 }
 for t in schema{
  for index in t["indexes"].as_array().ok_or(EventOutboxR366Error::Schema)?{
   if let Some(ddl)=index["definition"].as_str(){
    exec(&db,ddl).map_err(r366_error)?;
   }
  }
  for trigger in t["triggers"].as_array().ok_or(EventOutboxR366Error::Schema)?{
   let a=trigger.as_array().ok_or(EventOutboxR366Error::Schema)?;
   exec(&db,a[1].as_str().ok_or(EventOutboxR366Error::Schema)?)
     .map_err(r366_error)?;
  }
 }
 let table_count=r366_query_num(&db,
  "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")?;
 let triggers=r366_query_num(&db,
  "SELECT COUNT(*) FROM sqlite_master WHERE type='trigger'")?;
 if table_count!=17||triggers!=2||
   r366_query_num(&db,"PRAGMA user_version")?!=4{
   return Err(EventOutboxR366Error::Corrupt)
 }
 Ok((table_count,triggers))
}
#[cfg(test)]
mod r566_tests{
 use super::*;
 use serde_json::{json,Value};
 fn oracle()->Value{
  serde_json::from_str(include_str!(
   "../config/native_r566_original_collector_schema_oracle.json")).unwrap()
 }
 fn table<'a>(v:&'a Value,name:&str)->&'a Value{
  v["table_schema"].as_array().unwrap().iter()
   .find(|x|x["table"]==name).unwrap()
 }
 #[test]fn r566_real_python_17_table_manifest_and_digest(){
  let v=oracle();
  r566_check_source_manifest(&v).unwrap();
  assert_eq!(v["mirrored_tables"],17);
  assert_eq!(v["fixture_rows"],6);
  assert_eq!(v["fixture_snapshot_sha256"],R566_SOURCE_MANIFEST_DIGEST);
  assert_eq!(v["source_sha256"].as_object().unwrap().len(),4);
 }
 #[test]fn r567_rust_recreates_original_sqlite_schema_in_memory(){
  let v=oracle();
  assert_eq!(r566_recreate_original_schema_in_memory(&v).unwrap(),(17,2));
 }
 #[test]fn r568_event_type_constraints_and_foreign_keys_grounded(){
  let v=oracle();
  let events=table(&v,"events");
  let names=events["columns"].as_array().unwrap().iter()
    .map(|c|c[1].as_str().unwrap()).collect::<Vec<_>>();
  for name in ["event_id","schema_version","mode","category","name",
   "correlation_id","signal_id","is_exchange_verified","content_hash"]{
   assert!(names.contains(&name));
  }
  assert!(events["foreign_keys"].as_array().unwrap().len()>=2);
  let decisions=table(&v,"decisions");
  assert_eq!(decisions["mirror_key"],json!(["event_id"]));
  assert_eq!(decisions["foreign_keys"].as_array().unwrap().len(),3);
 }
 #[test]fn r569_unique_indexes_and_immutable_approval_triggers(){
  let v=oracle();
  let events=table(&v,"events");
  let indexes=events["indexes"].as_array().unwrap().iter()
   .filter_map(|i|i["name"].as_str()).collect::<Vec<_>>();
  assert!(indexes.contains(&"one_entry_commit"));
  assert!(indexes.contains(&"one_exit_commit"));
  let ledger=table(&v,"advisor_approval_transitions");
  let triggers=ledger["triggers"].as_array().unwrap();
  assert_eq!(triggers.len(),2);
  assert!(triggers.iter().all(|t|t[1].as_str().unwrap().contains("RAISE(ABORT")));
 }
 #[test]fn r570_eight_part_rollup_pk_and_full_keys(){
  let v=oracle();
  let expected=json!(["hour_start_ms","mode","base","category",
   "name","reason_code","decision","skip_reason"]);
  assert_eq!(table(&v,"hourly_raw_rollups")["mirror_key"],expected);
  assert_eq!(v["source_keys"]["hourly_raw_rollups"],expected);
  assert_eq!(table(&v,"hourly_raw_rollups")["columns"].as_array().unwrap().len(),24);
 }
 #[test]fn r571_row_change_without_count_change_rejected(){
  let mut v=oracle();
  assert_eq!(v["checks"]["changed_equal_count"],true);
  let tables=v["fixture_snapshot"]["tables"].as_array_mut().unwrap();
  let metadata=tables.iter_mut()
    .find(|x|x["schema"]["table"]=="metadata").unwrap();
  metadata["rows"][0]["row"]["value"]=json!("forged");
  assert_eq!(r566_check_source_manifest(&v),Err(EventOutboxR366Error::Corrupt));
 }
 #[test]fn r572_sql_schema_or_index_change_rejected(){
  let mut v=oracle();
  assert_eq!(v["checks"]["schema_column_altered"],true);
  assert_eq!(v["checks"]["index_removed"],true);
  let schema=v["table_schema"].as_array_mut().unwrap();
  let metadata=schema.iter_mut().find(|x|x["table"]=="metadata").unwrap();
  metadata["ddl"]=json!("CREATE TABLE metadata(key TEXT,value TEXT)");
  assert_eq!(r566_check_source_manifest(&v),Err(EventOutboxR366Error::Corrupt));
 }
 #[test]fn r573_missing_source_table_refused(){
  let mut v=oracle();
  v["table_schema"].as_array_mut().unwrap().remove(0);
  assert_eq!(r566_check_source_manifest(&v),Err(EventOutboxR366Error::Corrupt));
 }
 #[test]fn r574_bad_source_primary_key_or_count_rejected(){
  let mut v=oracle();
  v["source_keys"]["hourly_raw_rollups"]=json!(["hour_start_ms"]);
  assert_eq!(r566_check_source_manifest(&v),Err(EventOutboxR366Error::Corrupt));
  let mut v=oracle();
  v["fixture_snapshot"]["total_rows"]=json!(99);
  assert_eq!(r566_check_source_manifest(&v),Err(EventOutboxR366Error::Corrupt));
 }
 #[test]fn r575_independent_python_difference_and_rollback_cases(){
  let v=oracle();
  let c=&v["checks"];
  for name in ["same_all_17_tables","independent_insert_order",
   "source_version","missing_row","changed_equal_count",
   "schema_column_altered","index_removed","immutable_trigger_removed",
   "composite_pk_row_tamper","receiver_extra_row"]{
    assert_eq!(c[name],true);
  }
  assert_eq!(v["control_tables"],json!(["mirror_outbox","mirror_control"]));
  assert!(v["scope"].as_str().unwrap().contains("in-memory"));
  r566_check_source_manifest(&v).unwrap();
 }
}
