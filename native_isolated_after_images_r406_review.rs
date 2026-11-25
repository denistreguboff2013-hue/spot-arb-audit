// R406: read exact AFTER row images from *isolated* R366 SQLite inside
// source transaction; never from production DB1 or DB2.
fn r406_source_after(db:&Db,table:&str,id:&str,operation:&str)
 ->Result<serde_json::Value,EventOutboxR366Error>{
 use serde_json::{Value,json};
 if !["insert","upsert"].contains(&operation){
  return Err(EventOutboxR366Error::Schema)
 }
 let key=if table=="attempts"{json!({"correlation_id":id})}
         else{json!({"event_id":id})};
 let row=match table{
  "attempts"=>{
   let mut q=Stmt::new(db,
     "SELECT correlation_id,mode FROM attempts WHERE correlation_id=?1")
      .map_err(r366_error)?;
   q.string(1,id).map_err(r366_error)?;
   if q.step().map_err(r366_error)?!=SQLITE_ROW{
    return Err(EventOutboxR366Error::Corrupt)
   }
   let row=json!({"correlation_id":q.strval(0).map_err(r366_error)?,
     "mode":q.strval(1).map_err(r366_error)?});
   if q.step().map_err(r366_error)?!=SQLITE_DONE{
    return Err(EventOutboxR366Error::Corrupt)
   }
   row
  },

  "events"=>{
   let mut q=Stmt::new(db,
    "SELECT event_id,schema_version,mode,category,name,source,run_id,
      occurred_at_ms,verification_status,is_exchange_verified,payload_json,
      verification_json,source_record_json,content_hash,received_at_ms,
      CASE WHEN correlation_id IS NULL THEN 1 ELSE 0 END,
      COALESCE(correlation_id,'') FROM events WHERE event_id=?1")
    .map_err(r366_error)?;
   q.string(1,id).map_err(r366_error)?;
   if q.step().map_err(r366_error)?!=SQLITE_ROW{
    return Err(EventOutboxR366Error::Corrupt)
   }
   let correlation=if q.num(15).map_err(r366_error)?==1{
    Value::Null
   }else{Value::String(q.strval(16).map_err(r366_error)?)};
   let row=json!({
    "event_id":q.strval(0).map_err(r366_error)?,
    "schema_version":q.num(1).map_err(r366_error)?,
    "mode":q.strval(2).map_err(r366_error)?,
    "category":q.strval(3).map_err(r366_error)?,
    "name":q.strval(4).map_err(r366_error)?,
    "source":q.strval(5).map_err(r366_error)?,
    "run_id":q.strval(6).map_err(r366_error)?,
    "occurred_at_ms":q.num(7).map_err(r366_error)?,
    "verification_status":q.strval(8).map_err(r366_error)?,
    "is_exchange_verified":q.num(9).map_err(r366_error)?,
    "payload_json":q.strval(10).map_err(r366_error)?,
    "verification_json":q.strval(11).map_err(r366_error)?,
    "source_record_json":q.strval(12).map_err(r366_error)?,
    "content_hash":q.strval(13).map_err(r366_error)?,
    "received_at_ms":q.num(14).map_err(r366_error)?,
    "correlation_id":correlation
   });
   if q.step().map_err(r366_error)?!=SQLITE_DONE{
    return Err(EventOutboxR366Error::Corrupt)
   }
   row
  },
  name if r426_projection_fields(name).is_some()=>r426_category_row(db,name,id)?,
  _=>return Err(EventOutboxR366Error::Schema),
 };
 let hash=r366_sha(&r386_python_mirror_canonical(&row)?);
 Ok(json!({"table":table,"op":operation,"key":key,
   "row":row,"after_hash":hash}))
}
#[cfg(test)]
mod r406_after_tests{
 use super::*;
 use serde_json::{json,Value};
 use super::r366_tests::{fixture_root,record};
 #[test]fn r406_after_matches_source_python_two_sqlite_decisions(){
  let fixtures:Value=serde_json::from_str(include_str!(
   "../config/native_r396_python_ordered_and_after_oracle.json")).unwrap();
  let expanded:Value=serde_json::from_str(include_str!(
   "../config/native_r426_python_sqlite_categories_oracle.json")).unwrap();
  let root=fixture_root();
  let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
  for f in fixtures["after"].as_array().unwrap(){
   let expected=&f["expected"];
   let id=expected["row"]["event_id"].as_str().unwrap();
   let mut source=record(id,"decisions");
   let mut payload:Value=serde_json::from_str(source["payload_json"].as_str().unwrap()).unwrap();
   payload["decision"]=expected["row"]["decision"].clone();
   payload["skip_reason"]=expected["row"]["skip_reason"].clone();
   source["payload_json"]=json!(serde_json::to_string(&payload).unwrap());
   w.append(&source,1_792_660_000_100,false).unwrap();
   let db=r366_open(&w.path).unwrap();
   let actual=r406_source_after(&db,"decisions",id,"insert").unwrap();
   // Preserve the historic four-column source oracle independently.
   let original_raw=r386_python_mirror_canonical(&expected["row"]).unwrap();
   assert_eq!(original_raw,f["canonical_row"]);
   assert_eq!(r366_sha(&original_raw),expected["after_hash"]);
   for (field,value) in expected["row"].as_object().unwrap(){
    assert_eq!(&actual["row"][field],value);
   }
   // Extended six-column schema must match a NEW Python sqlite3 oracle.
   let full=expanded["cases"].as_array().unwrap().iter()
    .find(|c|c["category"]=="decisions"&&c["event_id"]==id).unwrap();
   assert_eq!(actual,full["expected_after"]);
  }
  drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r406_detects_two_hash_forgery_on_row_image(){
  let root=fixture_root();
  let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
  w.append(&record("r406_forged","decisions"),1_792_660_000_000,false).unwrap();
  let db=r366_open(&w.path).unwrap();
  let mut q=Stmt::new(&db,"SELECT payload_json FROM mirror_outbox WHERE seq=1").unwrap();
  assert_eq!(q.step().unwrap(),SQLITE_ROW);
  let old=q.strval(0).unwrap();drop(q);
  let mut frame:Value=serde_json::from_str(&old).unwrap();
  let ops=frame["operations"].as_array_mut().unwrap();
  let event=ops.iter_mut().find(|v|v["table"]=="events").unwrap();
  event["row"]["run_id"]=json!("altered_fake_session");
  event["after_hash"]=json!(r366_sha(&r386_python_mirror_canonical(&event["row"]).unwrap()));
  let forged=r386_python_mirror_canonical(&frame).unwrap();
  let forgedhash=r366_sha(&forged);
  let mut update=Stmt::new(&db,
   "UPDATE mirror_outbox SET payload_json=?1,payload_hash=?2,payload_bytes=?3 WHERE seq=1").unwrap();
  update.string(1,&forged).unwrap();update.string(2,&forgedhash).unwrap();
  update.number(3,(forged.len()+384) as u64).unwrap();
  assert_eq!(update.step().unwrap(),SQLITE_DONE);drop(update);drop(db);
  assert_eq!(w.audit(),Err(EventOutboxR366Error::Corrupt));
  fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r418_shared_correlation_is_inserted_into_outbox_once(){
  let root=fixture_root();
  let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
  for id in ["r418_one","r418_two"]{
   let mut row=record(id,"decisions");
   row["correlation_id"]=json!("r418_shared");
   w.append(&row,1_792_660_000_100,false).unwrap();
  }
  assert_eq!(w.audit().unwrap().last_seq,2);
  let db=r366_open(&w.path).unwrap();
  let mut q=Stmt::new(&db,
    "SELECT payload_json FROM mirror_outbox ORDER BY seq").unwrap();
  for i in 0..2{
   assert_eq!(q.step().unwrap(),SQLITE_ROW);
   let frame:Value=serde_json::from_str(&q.strval(0).unwrap()).unwrap();
   let ops=frame["operations"].as_array().unwrap();
   assert_eq!(ops.len(),if i==0{3}else{2});
   assert_eq!(ops.iter().filter(|v|v["table"]=="attempts").count(),
     if i==0{1}else{0});
   for op in ops{
    assert!(op.get("row").is_some()&&op.get("after_hash").is_some());
   }
  }
  assert_eq!(q.step().unwrap(),SQLITE_DONE);
  drop(q);drop(db);drop(w);
  let reopened=IsolatedEventOutboxR366::reopen(&root).unwrap();
  assert_eq!(reopened.audit().unwrap().outbox,2);
  drop(reopened);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r419_changed_category_row_is_not_trusted(){
  let root=fixture_root();
  let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
  w.append(&record("r419_source","decisions"),1_792_660_000_200,false).unwrap();
  let db=r366_open(&w.path).unwrap();
  exec(&db,"UPDATE decisions SET decision='forged' WHERE event_id='r419_source'").unwrap();
  drop(db);
  assert_eq!(w.audit(),Err(EventOutboxR366Error::Corrupt));
  drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r420_orphan_attempt_not_hidden_by_valid_event_outbox(){
  let root=fixture_root();
  let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
  w.append(&record("r420_source","errors"),1_792_660_000_200,false).unwrap();
  let db=r366_open(&w.path).unwrap();
  exec(&db,"INSERT INTO attempts(correlation_id,mode) VALUES('orphan','paper')").unwrap();
  drop(db);
  assert_eq!(w.audit(),Err(EventOutboxR366Error::Corrupt));
  drop(w);fs::remove_dir_all(root).unwrap();
 }
}
