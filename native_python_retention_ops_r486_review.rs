// R486–R495: exact original Python prune_raw + mirror_outbox operation oracle.
// Fixture-only: Rust "retention_shadow_outbox" is NOT a production outbox.
#[cfg(test)]
mod r486_tests {
 use super::*;
 use super::r366_tests::fixture_root;
 use serde_json::{json,Value};
 const NOW:u64=1_792_660_000_000;
 fn oracle()->Value{
  serde_json::from_str(include_str!(
   "../config/native_r486_python_retention_operations_oracle.json")).unwrap()
 }
 fn case(name:&str)->Value{oracle()[name].clone()}
 fn run_and_compare(name:&str)->Value{
  let expected=case(name);
  let root=fixture_root();
  let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
  let items=expected["events"].as_array().unwrap();
  for s in items{
   let input=json!({"mode":"paper","category":s["category"],
    "name":s["name"],"event_id":s["id"],"base":s["base"],
    "correlation_id":s["correlation_id"],"signal_id":s["signal_id"],
    "source":"native.paper","run_id":"r486_source_oracle",
    "occurred_at_ms":s["occurred_at_ms"],"payload":s["payload"],
    "verification":{"status":"observed"}});
   let event=r356_paper_event_record_without_io(&input,&[],NOW).unwrap();
   w.append(&event,s["received_at_ms"].as_u64().unwrap(),false).unwrap();
  }
  let id=format!("r486_{name}_batch");
  let prepared=w.r466_prepare(&id,NOW,7,false,500,false).unwrap();
  assert_eq!(prepared.candidate_count,
   expected["pruned"].as_u64().unwrap() as usize);
  let applied=w.r476_shadow_apply(&id,false).unwrap();
  assert_eq!(applied.tombstones,expected["pruned"].as_u64().unwrap());
  assert!(!applied.source_rows_deleted&&!applied.production_outbox_written);
  let db=r366_open(&w.path).unwrap();
  let mut q=Stmt::new(&db,"SELECT frame_json FROM retention_shadow_outbox
    WHERE batch_id=?1").unwrap();
  q.string(1,&id).unwrap();
  assert_eq!(q.step().unwrap(),SQLITE_ROW);
  let envelope:Value=serde_json::from_str(&q.strval(0).unwrap()).unwrap();
  assert_eq!(envelope["operations"],expected["operations"]);
  drop(q);drop(db);
  assert_eq!(w.audit().unwrap().events,items.len() as u64);
  drop(w);fs::remove_dir_all(root).unwrap();
  envelope
 }
 #[test]fn r486_full_original_python_plain_operations_exact(){
  let frame=run_and_compare("plain");
  assert_eq!(frame["operations"].as_array().unwrap().len(),7);
 }
 #[test]fn r487_python_execution_linked_case_exact(){
  let frame=run_and_compare("linked");
  assert_eq!(frame["operations"].as_array().unwrap().len(),4);
 }
 #[test]fn r488_metadata_is_python_upsert_with_exact_after_hash(){
  for name in ["plain","linked"]{
   let case=case(name);
   let op=case["operations"].as_array().unwrap().last().unwrap();
   assert_eq!(op["op"],"upsert");
   assert_eq!(op["table"],"metadata");
   assert_eq!(op["key"],json!({"key":"rolled_up_raw_events"}));
   assert_eq!(op["row"]["value"],case["metadata"]["value"]);
   assert_eq!(op["after_hash"],r366_sha(
    &r386_python_mirror_canonical(&op["row"]).unwrap()));
  }
 }
 #[test]fn r489_linked_trade_evidence_is_never_in_deletion_set(){
  let linked=case("linked");
  assert_eq!(linked["remaining_event_ids"],
    json!(["r487_entry","r487_protected"]));
  let ids=linked["operations"].as_array().unwrap().iter()
    .filter(|op|op["op"]=="delete")
    .map(|op|op["key"]["event_id"].as_str().unwrap().to_string())
    .collect::<Vec<_>>();
  assert_eq!(ids,vec!["r487_dec","r487_dec"]);
  let frame=run_and_compare("linked");
  assert_eq!(frame["source_rows_deleted"],false);
 }
 #[test]fn r490_all_python_row_upserts_have_verified_hashes(){
  for name in ["plain","linked"]{
   let case=case(name);
   for op in case["operations"].as_array().unwrap(){
    if op["op"]=="upsert"{
     assert_eq!(op["after_hash"],r366_sha(
      &r386_python_mirror_canonical(&op["row"]).unwrap()));
    }
   }
  }
 }
 #[test]fn r491_deleted_operations_use_only_verified_single_primary_key(){
  for name in ["plain","linked"]{
   let case=case(name);
   for op in case["operations"].as_array().unwrap(){
    if op["op"]=="delete"{
     assert!(["signals","decisions","events"].contains(
      &op["table"].as_str().unwrap()));
     assert_eq!(op["key"].as_object().unwrap().len(),1);
     assert!(op["key"]["event_id"].is_string());
     assert!(op.get("row").is_none());
    }
   }
  }
 }
 #[test]fn r492_category_deletes_precede_events_and_metadata_last(){
  let c=case("plain");
  let ops=c["operations"].as_array().unwrap();
  assert_eq!(ops.len(),7);
  assert_eq!((ops[0]["table"].as_str(),ops[1]["table"].as_str()),
    (Some("hourly_raw_rollups"),Some("hourly_raw_rollups")));
  assert_eq!((ops[2]["table"].as_str(),ops[3]["table"].as_str()),
    (Some("signals"),Some("decisions")));
  assert!(ops[4..6].iter().all(|o|o["table"]=="events"));
  assert_eq!(ops[6]["table"],"metadata");
 }
 #[test]fn r493_rust_shadow_envelope_never_claims_real_mirror_commit(){
  let frame=run_and_compare("plain");
  assert_eq!(frame["protocol_version"],1);
  assert_eq!(frame["schema_generation"],1);
  assert_eq!(frame["tx_kind"],"PRIVATE_SHADOW_RETENTION_NOT_PRODUCTION");
  assert_eq!(frame["source_rows_deleted"],false);
  assert_eq!(frame["db2_acknowledged"],false);
 }
 #[test]fn r494_original_python_canonical_bytes_and_digest_match_rust(){
  for name in ["plain","linked"]{
   let c=case(name);
   let canonical=r386_python_mirror_canonical(&c["operations"]).unwrap();
   assert_eq!(c["operations_canonical"],canonical);
   assert_eq!(c["operations_sha256"],r366_sha(&canonical));
  }
 }
 #[test]fn r495_both_python_oracle_counts_and_survivors_grounded(){
  let c=oracle();
  assert_eq!(c["plain"]["pruned"],2);
  assert_eq!(c["plain"]["remaining_event_ids"],json!([]));
  assert_eq!(c["plain"]["rollups"].as_array().unwrap().len(),2);
  assert_eq!(c["linked"]["pruned"],1);
  assert_eq!(c["linked"]["rollups"].as_array().unwrap().len(),1);
  assert_eq!(c["linked"]["metadata"]["value"],"1");
 }
}
