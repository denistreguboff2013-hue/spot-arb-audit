// R536–R545: read-only original Python rebaseline-generation transition gate.
// It checks LOCAL source metadata consistency only, never grants DB2 or REAL.
#[derive(Clone,Debug,PartialEq,Eq)]
struct R536RebaselineEvidence{
 baseline_seq:u64,latest_seq:u64,feed_generation:String,
 source_identity:String,db2_certified:bool,real_permit:bool
}
fn r536_check_python_rebaseline(
 old:&serde_json::Value,pending:&serde_json::Value,
 new_frame:&serde_json::Value,expected_identity:&str,
)->Result<R536RebaselineEvidence,EventOutboxR366Error>{
 let old_latest=old["latest_seq"].as_u64().ok_or(EventOutboxR366Error::Schema)?;
 let old_cleanup=old["cleanup_through"].as_u64()
   .ok_or(EventOutboxR366Error::Schema)?;
 let baseline=pending["baseline_seq"].as_u64()
   .ok_or(EventOutboxR366Error::Schema)?;
 let latest=pending["latest_seq"].as_u64()
   .ok_or(EventOutboxR366Error::Schema)?;
 let failure=pending["failure_seq"].as_u64()
   .ok_or(EventOutboxR366Error::Schema)?;
 let old_generation=old["feed_generation"].as_str()
   .ok_or(EventOutboxR366Error::Schema)?;
 let generation=pending["feed_generation"].as_str()
   .ok_or(EventOutboxR366Error::Schema)?;
 let identity=pending["baseline_source_identity"].as_str()
   .ok_or(EventOutboxR366Error::Schema)?;
 let previous_reason=old["reason"].as_str().ok_or(EventOutboxR366Error::Schema)?;
 if old["state"]!="REBUILD_REQUIRED"||
  pending["state"]!="BOOTSTRAP_REQUIRED"||
  pending["reason"]!="REBASELINE_PENDING"||
  old_cleanup>old_latest||baseline!=old_latest||failure!=baseline||
  latest!=baseline||generation==old_generation||!generation.starts_with("rebase-")||
  generation.len()!=39||!generation[7..].bytes().all(|c|c.is_ascii_hexdigit())||
  expected_identity.is_empty()||identity!=expected_identity||
  pending["failure_reason"]!=previous_reason||
  pending["baseline_schema_version"].as_i64().is_none()||
  pending["baseline_user_version"].as_i64().is_none()||
  pending["baseline_db1_bytes"].as_i64().is_none()||
  pending["baseline_db1_mtime_ns"].as_i64().is_none()||
  new_frame["prev_seq"].as_u64()!=Some(baseline)||
  new_frame["seq"].as_u64()!=baseline.checked_add(1)||
  new_frame["feed_generation"]!=generation{
  return Err(EventOutboxR366Error::Schema)
 }
 Ok(R536RebaselineEvidence{
  baseline_seq:baseline,latest_seq:latest,
  feed_generation:generation.to_string(),source_identity:identity.to_string(),
  db2_certified:false,real_permit:false
 })
}
#[cfg(test)]
mod r536_tests{
 use super::*;
 use serde_json::{json,Value};
 use super::r366_tests::{fixture_root,record};
 const T:u64=1_792_660_000_000;
 fn oracle()->Value{
  serde_json::from_str(include_str!(
   "../config/native_r536_python_rebaseline_oracle.json")).unwrap()
 }
 fn check(old:&Value,pending:&Value,frame:&Value)
  ->Result<R536RebaselineEvidence,EventOutboxR366Error>{
  r536_check_python_rebaseline(old,pending,frame,
   "sha256:db1-frozen-example")
 }
 #[test]fn r536_original_python_baseline_keeps_history(){
  let v=oracle();
  assert_eq!(v["original"]["state"],"BOOTSTRAP_REQUIRED");
  assert_eq!(v["before_rebuild"]["latest_seq"],1);
  assert_eq!(v["pending"]["baseline_seq"],1);
  assert_eq!(v["pending"]["latest_seq"],1);
  assert_eq!(v["after"]["latest_seq"],2);
  assert_eq!(v["first_seq"],1);
  assert_eq!(v["new_seq"],2);
 }
 #[test]fn r537_python_requires_rebuild_and_valid_metadata(){
  let v=oracle();
  assert_eq!(v["not_rebuild_error"],
   "rebaseline requires REBUILD_REQUIRED state");
  assert_eq!(v["bad_metadata_error"],
   "invalid immutable rebaseline metadata");
  assert_eq!(v["rebuild"]["state"],"REBUILD_REQUIRED");
  assert_eq!(v["rebuild"]["failure_reason"],"MIRROR_COMMIT_GAP");
 }
 #[test]fn r538_python_rebaseline_generation_continues_from_boundary(){
  let v=oracle();
  let evidence=check(&v["rebuild"],&v["pending"],&v["new_frame"]).unwrap();
  assert_eq!(evidence.baseline_seq,1);
  assert_eq!(evidence.latest_seq,1);
  assert!(evidence.feed_generation.starts_with("rebase-"));
  assert_eq!(evidence.source_identity,"sha256:db1-frozen-example");
  assert!(!evidence.db2_certified&&!evidence.real_permit);
  assert_eq!(v["rebaseline_result"]["baseline_seq"],1);
 }
 #[test]fn r539_original_python_storage_limits_fail_closed(){
  let v=oracle();
  assert_eq!(v["byte_limit_reason"],"OUTBOX_BYTE_LIMIT");
  assert_eq!(v["storage_reserve_reason"],"FILESYSTEM_RESERVE");
  assert!(v["no_pressure_reason"].is_null());
  assert_eq!(v["warning_retained_bytes"],512*1024*1024_u64);
  assert_eq!(v["critical_retained_bytes"],768*1024*1024_u64);
  assert_eq!(v["max_retained_bytes"],1024*1024*1024_u64);
  assert_eq!(v["free_reserve_bytes"],7_868_000_000_u64);
 }
 #[test]fn r540_forged_normal_state_cannot_be_rebaseline(){
  let v=oracle();let mut p=v["pending"].clone();
  p["state"]=json!("NORMAL");
  assert_eq!(check(&v["rebuild"],&p,&v["new_frame"]),
   Err(EventOutboxR366Error::Schema));
 }
 #[test]fn r541_wrong_generation_or_missing_frozen_source_identity_rejected(){
  let v=oracle();let mut p=v["pending"].clone();
  p["feed_generation"]=v["rebuild"]["feed_generation"].clone();
  assert_eq!(check(&v["rebuild"],&p,&v["new_frame"]),
   Err(EventOutboxR366Error::Schema));
  let p=v["pending"].clone();
  assert_eq!(r536_check_python_rebaseline(
   &v["rebuild"],&p,&v["new_frame"],"sha256:another-db1"),
   Err(EventOutboxR366Error::Schema));
 }
 #[test]fn r542_sequence_rollback_or_skipped_successor_rejected(){
  let v=oracle();let mut p=v["pending"].clone();
  p["baseline_seq"]=json!(0);
  assert_eq!(check(&v["rebuild"],&p,&v["new_frame"]),
   Err(EventOutboxR366Error::Schema));
  let mut frame=v["new_frame"].clone();
  frame["seq"]=json!(3);
  assert_eq!(check(&v["rebuild"],&v["pending"],&frame),
   Err(EventOutboxR366Error::Schema));
 }
 #[test]fn r543_failure_reason_or_baseline_metadata_tamper_rejected(){
  let v=oracle();let mut p=v["pending"].clone();
  p["failure_reason"]=json!("CLEAN");
  assert_eq!(check(&v["rebuild"],&p,&v["new_frame"]),
   Err(EventOutboxR366Error::Schema));
  let mut p=v["pending"].clone();
  p["baseline_user_version"]=Value::Null;
  assert_eq!(check(&v["rebuild"],&p,&v["new_frame"]),
   Err(EventOutboxR366Error::Schema));
 }
 #[test]fn r544_original_source_r376_rejects_unapproved_normal_transition(){
  let root=fixture_root();
  let src=IsolatedEventOutboxR366::initialize(&root).unwrap();
  src.append(&record("r544_event","decisions"),T,false).unwrap();
  let db=r366_open(&src.path).unwrap();
  exec(&db,"UPDATE mirror_control SET state='NORMAL' WHERE id=1").unwrap();
  drop(db);
  assert_eq!(src.audit(),Err(EventOutboxR366Error::Corrupt));
  drop(src);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r545_all_local_evidence_is_not_db2_certification(){
  let v=oracle();
  let local=check(&v["rebuild"],&v["pending"],&v["new_frame"]).unwrap();
  assert!(!local.db2_certified&&!local.real_permit);
  assert_eq!(v["after"]["baseline_seq"],1);
  assert_eq!(v["after"]["feed_generation"],local.feed_generation);
  assert_eq!(v["new_frame"]["feed_generation"],local.feed_generation);
 }
}
