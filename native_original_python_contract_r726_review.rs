// Astra R726+: EXACT original Python mirror metadata shape + outbox arithmetic.
// Review-only pure validation. Never edits Python, DB1, DB2 or installed PAPER.
// Source audited against /home/tmpadmin/spotbot/ai/mirror_outbox.py and
// /home/tmpadmin/spotbot/scripts/mirror_service.py as pinned in R566.
const R726_CONTROL:&[&str]=&[
 "id","state","reason","latest_seq","cleanup_through","retained_bytes",
 "retained_records","schema_generation","feed_generation","baseline_seq",
 "failure_seq","failure_reason","baseline_source_identity",
 "baseline_schema_version","baseline_user_version","baseline_db1_bytes",
 "baseline_db1_mtime_ns"];
const R726_OUTBOX:&[&str]=&[
 "seq","prev_seq","tx_id","tx_kind","schema_generation","payload_json",
 "payload_hash","payload_bytes","committed_at_ms","feed_generation"];
const R726_META:&[&str]=&[
 "id","phase","source_identity","schema_version","user_version",
 "schema_hash","bootstrap_seq","applied_seq","applied_hash","table_index",
 "feed_generation","baseline_seq","last_key_json","last_success_ms"];
fn r726_shape(v:&serde_json::Value,fields:&[&str])
 ->Result<(),EventOutboxR366Error>{
 let m=v.as_object().ok_or(EventOutboxR366Error::Schema)?;
 if m.len()!=fields.len()||fields.iter().any(|k|!m.contains_key(*k)){
  return Err(EventOutboxR366Error::Schema)
 }
 Ok(())
}
fn r726_u(v:&serde_json::Value,key:&str)->Result<u64,EventOutboxR366Error>{
 v[key].as_u64().ok_or(EventOutboxR366Error::Schema)
}
fn r726_s<'a>(v:&'a serde_json::Value,key:&str)
 ->Result<&'a str,EventOutboxR366Error>{
 v[key].as_str().ok_or(EventOutboxR366Error::Schema)
}
fn r726_python_protocol_contract(control:&serde_json::Value,meta:&serde_json::Value,
 rows:&[serde_json::Value],schema_version:u64,user_version:u64,
 source_identity:&str)->Result<serde_json::Value,EventOutboxR366Error>{
 r566_check_source_manifest(&r586_oracle())?;
 r726_shape(control,R726_CONTROL)?;
 r726_shape(meta,R726_META)?;
 if r726_u(control,"id")?!=1||r726_u(meta,"id")?!=1
   ||r726_u(control,"schema_generation")?!=1||user_version!=4
   ||r726_u(meta,"schema_version")?!=schema_version
   ||r726_u(meta,"user_version")?!=user_version
   ||r726_s(meta,"source_identity")?!=source_identity{
  return Err(EventOutboxR366Error::Schema)
 }
 let latest=r726_u(control,"latest_seq")?;
 let cleaned=r726_u(control,"cleanup_through")?;
 let applied=r726_u(meta,"applied_seq")?;
 let baseline=r726_u(control,"baseline_seq")?;
 let bootstrap=r726_u(meta,"bootstrap_seq")?;
 let mirror_baseline=r726_u(meta,"baseline_seq")?;
 let generation=r726_s(control,"feed_generation")?;
 let sink_generation=r726_s(meta,"feed_generation")?;
 let state=r726_s(control,"state")?;
 let phase=r726_s(meta,"phase")?;
 if !matches!(state,"NORMAL"|"BOOTSTRAP_REQUIRED"|"REBUILD_REQUIRED")
   ||!matches!(phase,"BOOTSTRAP"|"REPLAY"|"SYNCED")
   ||generation.is_empty()||generation.len()>80||generation!=sink_generation
   ||baseline!=bootstrap||baseline!=mirror_baseline
   ||cleaned>applied||applied>latest||baseline>latest{
  return Err(EventOutboxR366Error::Corrupt)
 }
 // Original rebaseline metadata must remain immutable after the bounded
 // stopped-source cutover. A legacy generation does not prove a new baseline.
 if generation!="legacy"{
  if control["baseline_source_identity"].as_str()!=Some(source_identity)
    ||control["baseline_schema_version"].as_u64()!=Some(schema_version)
    ||control["baseline_user_version"].as_u64()!=Some(user_version)
    ||control["baseline_db1_bytes"].as_u64().is_none()
    ||control["baseline_db1_mtime_ns"].as_u64().is_none(){
    return Err(EventOutboxR366Error::Corrupt)
  }
 }
 let mut sum_bytes=0_u64;
 let mut prior=cleaned;
 let mut seen=std::collections::BTreeSet::<String>::new();
 if rows.len()>512{return Err(EventOutboxR366Error::Pressure)}
 for row in rows{
  r726_shape(row,R726_OUTBOX)?;
  let seq=r726_u(row,"seq")?;
  let prev=r726_u(row,"prev_seq")?;
  let tx_id=r726_s(row,"tx_id")?;
  let tx_kind=r726_s(row,"tx_kind")?;
  let payload=r726_s(row,"payload_json")?;
  let hash=r726_s(row,"payload_hash")?;
  let size=r726_u(row,"payload_bytes")?;
  let clock=r726_u(row,"committed_at_ms")?;
  if prev!=prior||seq<=prev||seq>latest||clock==0
    ||!tx_id.starts_with("tx-")||tx_id.len()!=35||tx_kind.is_empty()
    ||!seen.insert(tx_id.to_owned())
    ||r726_u(row,"schema_generation")?!=1
    ||r726_s(row,"feed_generation")?!=generation
    ||payload.as_bytes().len()>4*1024*1024
    ||size!=(payload.as_bytes().len() as u64)+384
    ||r366_sha(payload)!=hash{
   return Err(EventOutboxR366Error::Corrupt)
  }
  let p:serde_json::Value=serde_json::from_str(payload)
   .map_err(|_|EventOutboxR366Error::Corrupt)?;
  if r386_python_mirror_canonical(&p)?!=payload
    ||p["protocol_version"]!=1||p["schema_generation"]!=1
    ||p["tx_kind"]!=tx_kind
    ||p["operations"].as_array().map_or(true,|a|a.is_empty()){
   return Err(EventOutboxR366Error::Corrupt)
  }
  sum_bytes=sum_bytes.checked_add(size).ok_or(EventOutboxR366Error::Pressure)?;
  prior=seq;
 }
 if r726_u(control,"retained_records")?!=rows.len() as u64
   ||r726_u(control,"retained_bytes")?!=sum_bytes
   ||sum_bytes>1024*1024*1024
   ||(rows.is_empty()&&latest!=cleaned)
   ||(!rows.is_empty()&&prior!=latest){
  return Err(EventOutboxR366Error::Corrupt)
 }
 let source_ready=state=="NORMAL";
 let receiver_ready=phase=="SYNCED";
 let full_ack=applied==latest;
 let no_pending=rows.is_empty();
 // Current-field consistency, not independent process or exchange witness.
 Ok(serde_json::json!({
  "schema":"SPOTARB/R726/ORIGINAL-PYTHON-OUTBOX-CONTRACT/OFFLINE/V1",
  "original_python_shape_matches":true,
  "generation_fields_consistent":true,
  "state":state,"phase":phase,
  "latest_seq":latest,"applied_seq":applied,"cleanup_through":cleaned,
  "retained_records":rows.len(),"retained_bytes":sum_bytes,
  "would_allow_review_replay":source_ready&&receiver_ready,
  "would_allow_review_cleanup":source_ready&&receiver_ready&&full_ack,
  "would_allow_review_release":false,
  "fully_caught_up_and_cleaned":source_ready&&receiver_ready&&full_ack&&no_pending,
  "installed_paper_wired":false,
  "source_file_and_account_attested":false,
  "real_trading_authorized":false
 }))
}

#[cfg(test)]
mod r726_tests{
 use super::*;
 use serde_json::{json,Value};
 const ID:&str="sha256:private-test-source";
 fn fixture()->(Value,Value,Vec<Value>){
  let control=json!({
   "id":1,"state":"NORMAL","reason":null,"latest_seq":1,
   "cleanup_through":0,"retained_bytes":0,"retained_records":1,
   "schema_generation":1,"feed_generation":"legacy","baseline_seq":0,
   "failure_seq":null,"failure_reason":null,"baseline_source_identity":null,
   "baseline_schema_version":null,"baseline_user_version":null,
   "baseline_db1_bytes":null,"baseline_db1_mtime_ns":null
  });
  let meta=json!({
   "id":1,"phase":"SYNCED","source_identity":ID,
   "schema_version":12,"user_version":4,"schema_hash":"local-schema-hash",
   "bootstrap_seq":0,"applied_seq":1,"applied_hash":null,
   "table_index":17,"feed_generation":"legacy","baseline_seq":0,
   "last_key_json":null,"last_success_ms":12000
  });
  let v=r586_oracle();
  let row=v["fixture_snapshot"]["tables"].as_array().unwrap()
   .iter().find(|t|t["schema"]["table"]=="metadata").unwrap()
   ["rows"][0]["row"].clone();
  let op=r659_op(&v,"metadata",&row,"insert").unwrap();
  let envelope=json!({"protocol_version":1,"schema_generation":1,
   "tx_kind":"synthetic-source-transaction","operations":[op]});
  let raw=r386_python_mirror_canonical(&envelope).unwrap();
  let source_row=json!({
   "seq":1,"prev_seq":0,"tx_id":format!("tx-{}","1".repeat(32)),
   "tx_kind":"synthetic-source-transaction","schema_generation":1,
   "payload_json":raw,"payload_hash":r366_sha(&raw),
   "payload_bytes":raw.as_bytes().len()+384,
   "committed_at_ms":123456,
   "feed_generation":"legacy"
  });
  let mut control=control;
  control["retained_bytes"]=source_row["payload_bytes"].clone();
  (control,meta,vec![source_row])
 }
 fn run(c:&Value,m:&Value,rows:&[Value])
  ->Result<Value,EventOutboxR366Error>{
  r726_python_protocol_contract(c,m,rows,12,4,ID)
 }
 fn bad(c:&Value,m:&Value,rows:&[Value]){
  assert!(run(c,m,rows).is_err())
 }
 #[test]fn r726_source_contract_all_17_fields_verified(){
  let (c,m,r)=fixture();let z=run(&c,&m,&r).unwrap();
  assert_eq!(z["original_python_shape_matches"],true);
  assert_eq!(z["would_allow_review_cleanup"],true);
  assert_eq!(z["installed_paper_wired"],false);
  assert_eq!(z["real_trading_authorized"],false);
 }
 #[test]fn r727_outbox_exact_ten_columns(){
  let (c,m,r)=fixture();
  assert_eq!(r[0].as_object().unwrap().len(),10);
  assert_eq!(m.as_object().unwrap().len(),14);
  assert_eq!(c.as_object().unwrap().len(),17);
 }
 #[test]fn r728_missing_source_control_column_rejected(){
  let (mut c,m,r)=fixture();
  c.as_object_mut().unwrap().remove("retained_records");bad(&c,&m,&r);
 }
 #[test]fn r729_unknown_source_control_column_rejected(){
  let (mut c,m,r)=fixture();
  c["not_original_field"]=json!(0);bad(&c,&m,&r);
 }
 #[test]fn r730_missing_outbox_feed_generation_rejected(){
  let (c,m,mut r)=fixture();
  r[0].as_object_mut().unwrap().remove("feed_generation");bad(&c,&m,&r);
 }
 #[test]fn r731_retained_bytes_include_exact_python_384_reserve(){
  let (mut c,m,r)=fixture();c["retained_bytes"]=json!(
   c["retained_bytes"].as_u64().unwrap()-384);bad(&c,&m,&r);
 }
 #[test]fn r732_retained_count_divergence_rejected(){
  let (mut c,m,r)=fixture();c["retained_records"]=json!(0);bad(&c,&m,&r);
 }
 #[test]fn r733_payload_hash_mismatch_rejected(){
  let (c,m,mut r)=fixture();r[0]["payload_hash"]=json!("0".repeat(64));
  bad(&c,&m,&r);
 }
 #[test]fn r734_python_canonical_json_contract_enforced(){
  let (mut c,m,mut r)=fixture();
  let original=r[0]["payload_json"].as_str().unwrap();
  let pretty=serde_json::to_string_pretty(
   &serde_json::from_str::<Value>(original).unwrap()).unwrap();
  r[0]["payload_json"]=json!(pretty);
  r[0]["payload_hash"]=json!(r366_sha(&pretty));
  r[0]["payload_bytes"]=json!(pretty.len()+384);
  c["retained_bytes"]=r[0]["payload_bytes"].clone();
  bad(&c,&m,&r);
 }
 #[test]fn r735_different_generation_rejected(){
  let (c,mut m,r)=fixture();m["feed_generation"]=json!("rebase-other");
  bad(&c,&m,&r);
 }
 #[test]fn r736_wrong_source_identity_rejected(){
  let (c,mut m,r)=fixture();m["source_identity"]=json!("foreign-db1");
  bad(&c,&m,&r);
 }
 #[test]fn r737_rebaseline_requires_matching_immutable_evidence(){
  let (mut c,mut m,mut r)=fixture();
  c["feed_generation"]=json!("rebase-abc");
  m["feed_generation"]=json!("rebase-abc");
  r[0]["feed_generation"]=json!("rebase-abc");
  bad(&c,&m,&r);
  c["baseline_source_identity"]=json!(ID);
  c["baseline_schema_version"]=json!(12);
  c["baseline_user_version"]=json!(4);
  c["baseline_db1_bytes"]=json!(121000);
  c["baseline_db1_mtime_ns"]=json!(19500000);
  assert!(run(&c,&m,&r).is_ok());
 }
 #[test]fn r738_cleanup_ahead_of_sink_ack_rejected(){
  let (mut c,m,r)=fixture();c["cleanup_through"]=json!(2);
  bad(&c,&m,&r);
 }
 #[test]fn r739_applied_ahead_of_source_latest_rejected(){
  let (c,mut m,r)=fixture();m["applied_seq"]=json!(9);
  bad(&c,&m,&r);
 }
 #[test]fn r740_rebuild_required_never_promoted_to_normal(){
  let (mut c,m,r)=fixture();c["state"]=json!("REBUILD_REQUIRED");
  let z=run(&c,&m,&r).unwrap();
  assert_eq!(z["would_allow_review_replay"],false);
  assert_eq!(z["would_allow_review_cleanup"],false);
 }
 #[test]fn r741_bootstrap_required_never_promoted_to_normal(){
  let (mut c,m,r)=fixture();c["state"]=json!("BOOTSTRAP_REQUIRED");
  let z=run(&c,&m,&r).unwrap();
  assert_eq!(z["would_allow_review_replay"],false);
  assert_eq!(z["would_allow_review_release"],false);
 }
 #[test]fn r742_receiver_replay_phase_denies_ack(){
  let (c,mut m,r)=fixture();m["phase"]=json!("REPLAY");
  let z=run(&c,&m,&r).unwrap();
  assert_eq!(z["would_allow_review_cleanup"],false);
 }
 #[test]fn r743_outbox_txid_collision_denied_even_if_rows_match(){
  let (mut c,mut m,mut r)=fixture();
  let mut second=r[0].clone();second["seq"]=json!(3);second["prev_seq"]=json!(1);
  r.push(second);c["latest_seq"]=json!(3);
  c["retained_records"]=json!(2);
  c["retained_bytes"]=json!(r[0]["payload_bytes"].as_u64().unwrap()*2);
  m["applied_seq"]=json!(3);
  bad(&c,&m,&r);
 }
 #[test]fn r744_original_autoincrement_gap_with_prev_link_allowed(){
  let (mut c,mut m,mut r)=fixture();
  let mut second=r[0].clone();second["seq"]=json!(3);second["prev_seq"]=json!(1);
  second["tx_id"]=json!(format!("tx-{}","2".repeat(32)));
  r.push(second);c["latest_seq"]=json!(3);
  c["retained_records"]=json!(2);
  c["retained_bytes"]=json!(r[0]["payload_bytes"].as_u64().unwrap()*2);
  m["applied_seq"]=json!(3);
  assert!(run(&c,&m,&r).is_ok());
 }
 #[test]fn r745_cleaned_outbox_can_be_empty_only_at_latest(){
  let (mut c,m,_)=fixture();
  c["retained_records"]=json!(0);
  c["retained_bytes"]=json!(0);
  c["cleanup_through"]=json!(1);
  let z=run(&c,&m,&[]).unwrap();
  assert_eq!(z["fully_caught_up_and_cleaned"],true);
  c["cleanup_through"]=json!(0);
  bad(&c,&m,&[]);
 }
 #[test]fn r746_tx_kind_must_match_payload(){
  let (c,m,mut r)=fixture();
  r[0]["tx_kind"]=json!("DIFFERENT");
  bad(&c,&m,&r);
 }
 #[test]fn r747_protocol_version_must_be_one_even_after_rehash(){
  let (mut c,m,mut r)=fixture();
  let mut payload:Value=serde_json::from_str(r[0]["payload_json"].as_str().unwrap()).unwrap();
  payload["protocol_version"]=json!(2);
  let raw=r386_python_mirror_canonical(&payload).unwrap();
  r[0]["payload_json"]=json!(raw);
  r[0]["payload_hash"]=json!(r366_sha(&raw));
  r[0]["payload_bytes"]=json!(raw.len()+384);
  c["retained_bytes"]=r[0]["payload_bytes"].clone();
  bad(&c,&m,&r);
 }
 #[test]fn r748_wrong_envelope_schema_generation_refused(){
  let (mut c,m,mut r)=fixture();
  let mut payload:Value=serde_json::from_str(r[0]["payload_json"].as_str().unwrap()).unwrap();
  payload["schema_generation"]=json!(7);
  let raw=r386_python_mirror_canonical(&payload).unwrap();
  r[0]["payload_json"]=json!(raw);
  r[0]["payload_hash"]=json!(r366_sha(&raw));
  r[0]["payload_bytes"]=json!(raw.len()+384);
  c["retained_bytes"]=r[0]["payload_bytes"].clone();
  bad(&c,&m,&r);
 }
 #[test]fn r749_row_generation_must_match_source(){
  let (c,m,mut r)=fixture();
  r[0]["feed_generation"]=json!("rebase-OTHER");
  bad(&c,&m,&r);
 }
 #[test]fn r750_payload_bytes_under_report_refuses(){
  let (mut c,m,mut r)=fixture();
  let bad_bytes=r[0]["payload_bytes"].as_u64().unwrap()-1;
  r[0]["payload_bytes"]=json!(bad_bytes);
  c["retained_bytes"]=json!(bad_bytes);
  bad(&c,&m,&r);
 }
 #[test]fn r751_forged_outbox_prev_seq_refuses(){
  let (c,m,mut r)=fixture();
  r[0]["prev_seq"]=json!(15);
  bad(&c,&m,&r);
 }
 #[test]fn r752_non_monotonic_autoincrement_refuses(){
  let (c,m,mut r)=fixture();
  r[0]["seq"]=json!(0);
  bad(&c,&m,&r);
 }
 #[test]fn r753_unlinked_second_frame_chain_refuses(){
  let (mut c,mut m,mut r)=fixture();
  let mut second=r[0].clone();
  second["seq"]=json!(2);second["prev_seq"]=json!(0);
  second["tx_id"]=json!(format!("tx-{}","2".repeat(32)));
  r.push(second);
  c["latest_seq"]=json!(2);
  c["retained_records"]=json!(2);
  c["retained_bytes"]=json!(r[0]["payload_bytes"].as_u64().unwrap()*2);
  m["applied_seq"]=json!(2);
  bad(&c,&m,&r);
 }
 #[test]fn r754_unrecognized_destination_phase_refuses(){
  let (c,mut m,r)=fixture();
  m["phase"]=json!("LIKELY_SYNCED");
  bad(&c,&m,&r);
 }
 #[test]fn r755_all_local_metadata_matching_still_refuses_real_release(){
  let (mut c,m,_)=fixture();
  c["latest_seq"]=json!(1);
  c["cleanup_through"]=json!(1);
  c["retained_records"]=json!(0);
  c["retained_bytes"]=json!(0);
  let z=run(&c,&m,&[]).unwrap();
  assert_eq!(z["fully_caught_up_and_cleaned"],true);
  for key in ["would_allow_review_release","installed_paper_wired",
   "source_file_and_account_attested","real_trading_authorized"]{
    assert_eq!(z[key],false,"{key}");
  }
 }
}
