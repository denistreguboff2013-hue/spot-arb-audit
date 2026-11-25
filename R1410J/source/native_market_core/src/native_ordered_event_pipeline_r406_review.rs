// R406-R415: pure research adapter: ordered Python input -> R356 PAPER
// validation -> R396 ordered scrub -> exact R386 source canonical.
// Caller can persist only in the isolated R366 fixture DB. Never live PAPER.
fn r406_source_owned_record(
 event:&serde_json::Value,raw_payload:&str,raw_verification:&str,
 known_secrets:&[&str],now_ms:u64
)->Result<serde_json::Value,EventOutboxR366Error>{
 use serde_json::Value;
 let source_payload:Value=serde_json::from_str(raw_payload)
   .map_err(|_|EventOutboxR366Error::Schema)?;
 let source_verification:Value=serde_json::from_str(raw_verification)
   .map_err(|_|EventOutboxR366Error::Schema)?;
 if !source_payload.is_object()||!source_verification.is_object()||
    event.get("payload")!=Some(&source_payload)||
    event.get("verification")!=Some(&source_verification){
  return Err(EventOutboxR366Error::Schema)
 }
 // The R396 ordered parser refuses duplicate keys and unsupported numbers.
 let scrubbed_payload=r396_python_scrubbed_inner_json(raw_payload,known_secrets)?;
 let scrubbed_verification=r396_python_scrubbed_inner_json(
   raw_verification,known_secrets)?;
 let mut record=r356_paper_event_record_without_io(event,known_secrets,now_ms)
  .map_err(|_|EventOutboxR366Error::Schema)?;
 let payload:Value=serde_json::from_str(&scrubbed_payload)
  .map_err(|_|EventOutboxR366Error::Schema)?;
 let verification:Value=serde_json::from_str(&scrubbed_verification)
  .map_err(|_|EventOutboxR366Error::Schema)?;
 // The ordered and the originally validated unordered scrubs MUST agree
 // semantically (the only permitted difference is object-key order).
 if serde_json::from_str::<Value>(
    record["payload_json"].as_str().ok_or(EventOutboxR366Error::Schema)?)
       .map_err(|_|EventOutboxR366Error::Schema)?!=payload||
    serde_json::from_str::<Value>(
    record["verification_json"].as_str().ok_or(EventOutboxR366Error::Schema)?)
       .map_err(|_|EventOutboxR366Error::Schema)?!=verification{
   return Err(EventOutboxR366Error::Schema)
 }
 record["payload_json"]=Value::String(scrubbed_payload);
 record["verification_json"]=Value::String(scrubbed_verification);
 let bytes=r386_python_collector_json(&record)?.len();
 if bytes>16384||bytes.saturating_add(768)>16384{
  return Err(EventOutboxR366Error::Schema)
 }
 Ok(record)
}

#[cfg(test)]
mod r406_tests{
 use super::*;
 use serde_json::{json,Value};
 use super::r366_tests::fixture_root;
 const NOW:u64=1_792_660_000_000;
 #[test]fn r406_python_ordered_record_and_durable_sqlite_hashes(){
  let fixture:Value=serde_json::from_str(include_str!(
   "../config/native_r406_event_pipeline_oracle.json")).unwrap();
  let rows=fixture["cases"].as_array().unwrap();
  assert_eq!(rows.len(),2);
  let root=fixture_root();
  let writer=IsolatedEventOutboxR366::initialize(&root).unwrap();
  for (i,row) in rows.iter().enumerate(){
   let record=r406_source_owned_record(
     &row["event"],
     row["payload_raw_json"].as_str().unwrap(),
     row["verification_raw_json"].as_str().unwrap(),&[],NOW).unwrap();
   assert_eq!(record,row["expected_record"],"case {}",row["name"]);
   assert_eq!(r386_python_collector_json(&record).unwrap(),
     row["expected_canonical_record"]);
   assert_eq!(r366_sha(&r386_python_collector_json(&record).unwrap()),
     row["expected_content_sha256"]);
   writer.append(&record,NOW+i as u64,false).unwrap();
   assert_eq!(writer.audit().unwrap().events,i as u64+1);
  }
  drop(writer);
  let reopened=IsolatedEventOutboxR366::reopen(&root).unwrap();
  assert_eq!(reopened.audit().unwrap().last_seq,2);
  let db=r366_open(&reopened.path).unwrap();
  let mut query=Stmt::new(&db,
   "SELECT event_id,source_record_json,content_hash,payload_json
      FROM events ORDER BY event_id").unwrap();
  let mut seen=0;
  while query.step().unwrap()==SQLITE_ROW{
   let canonical=query.strval(1).unwrap();
   assert_eq!(r366_sha(&canonical),query.strval(2).unwrap());
   assert!(!canonical.contains("FAKE_API_KEY"));
   assert!(!canonical.contains("FAKE_PRIVATE"));
   assert!(!canonical.contains("password: demo"));
   let stored=json!(query.strval(3).unwrap());
   assert!(stored.is_string());
   seen+=1;
  }
  assert_eq!(seen,2);
  drop(query);drop(db);drop(reopened);
  fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r406_refuses_spoofed_raw_payload_and_real_status(){
  let v=json!({"event_id":"r406_bad","mode":"paper","category":"errors",
   "name":"ERROR","source":"native.paper","run_id":"r406_run",
   "occurred_at_ms":NOW,"payload":{"module":"test"},
   "verification":{"status":"observed"}});
  assert_eq!(r406_source_owned_record(&v,"{\"module\":\"OTHER\"}",
      "{\"status\":\"observed\"}",&[],NOW),
    Err(EventOutboxR366Error::Schema));
  assert_eq!(r406_source_owned_record(&v,"{\"module\":\"test\"}",
      "{\"status\":\"verified\"}",&[],NOW),
    Err(EventOutboxR366Error::Schema));
  let mut real=v.clone();real["mode"]=json!("real");
  assert_eq!(r406_source_owned_record(&real,"{\"module\":\"test\"}",
      "{\"status\":\"observed\"}",&[],NOW),
    Err(EventOutboxR366Error::Schema));
 }
 #[test]fn r406_refuses_duplicate_keys_and_float_before_any_sqlite(){
  let v=json!({"event_id":"r406_bad","mode":"paper","category":"errors",
   "name":"ERROR","source":"native.paper","run_id":"r406_run",
   "occurred_at_ms":NOW,"payload":{"module":"test"},
   "verification":{"status":"observed"}});
  for raw in ["{\"module\":\"test\",\"module\":\"test\"}",
    "{\"module\":\"test\",\"ratio\":1.5}"]{
   assert_eq!(r406_source_owned_record(&v,raw,
     "{\"status\":\"observed\"}",&[],NOW),
     Err(EventOutboxR366Error::Schema));
  }
 }
}
