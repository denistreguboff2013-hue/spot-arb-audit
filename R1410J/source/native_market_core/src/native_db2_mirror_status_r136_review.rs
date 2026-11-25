//! Astra R136 — read-only, offline DB2 status *age* verifier.
//! Database replicas are not equivalent to their status JSON. No I/O, promotion,
//! DB migrations or restart authority follows from this evaluator.
use serde_json::Value;

#[derive(Clone,Debug,PartialEq,Eq)]
pub enum Db2StatusError{Json,Incomplete,Clock,Stale,Conflict,UnresolvedFailure}

#[derive(Clone,Debug,PartialEq,Eq)]
pub struct Db2StatusReadinessR136{
 pub mirror_applied_seq:u64,pub source_latest_seq:u64,
 pub updated_at_ms:u64,pub feed_generation:String,
 pub certified_live_replica:bool,
}
fn nonempty_str<'a>(v:&'a Value,k:&str)->Result<&'a str,Db2StatusError>{
 v.get(k).and_then(Value::as_str).filter(|x|!x.is_empty())
  .ok_or(Db2StatusError::Incomplete)
}
fn num(v:&Value,k:&str)->Result<u64,Db2StatusError>{
 v.get(k).and_then(Value::as_u64).ok_or(Db2StatusError::Incomplete)
}
pub fn audit_db2_status_json(
 body:&str,now_ms:u64,max_age_ms:u64
)->Result<Db2StatusReadinessR136,Db2StatusError>{
 let v:Value=serde_json::from_str(body).map_err(|_|Db2StatusError::Json)?;
 let updated=num(&v,"updated_at_ms")?;
 let last_success=num(&v,"last_success_ms")?;
 if now_ms==0||max_age_ms==0||updated==0||last_success==0
   ||updated>now_ms||last_success>updated{return Err(Db2StatusError::Clock)}
 // R137: a freshly rewritten heartbeat must not conceal a stalled apply.
 if now_ms-updated>max_age_ms || now_ms-last_success>max_age_ms{
  return Err(Db2StatusError::Stale)
 }
 if v["state"]!="SYNCED"||v["mirror_phase"]!="SYNCED"
  ||v["db2_health"]!="AVAILABLE"||v["outbox_pressure_level"]!="NORMAL"
  ||v["lag_records"].as_u64()!=Some(0){
  return Err(Db2StatusError::Conflict)
 }
 if !v["error"].is_null()||!v["failure_reason"].is_null(){
  return Err(Db2StatusError::UnresolvedFailure)
 }
 let generation=nonempty_str(&v,"feed_generation")?;
 let mirror=nonempty_str(&v,"mirror_feed_generation")?;
 if generation!=mirror{return Err(Db2StatusError::Conflict)}
 let source=num(&v,"source_latest_seq")?;
 let applied=num(&v,"mirror_applied_seq")?;
 if applied!=source || applied==0 || num(&v,"baseline_seq")?>applied{
  return Err(Db2StatusError::Conflict)
 }
 Ok(Db2StatusReadinessR136{
  mirror_applied_seq:applied,source_latest_seq:source,updated_at_ms:updated,
  feed_generation:generation.into(),certified_live_replica:false})
}
#[cfg(test)]
mod tests{
 use super::*;
 const NOW:u64=1_791_784_000_000;
 fn valid()->Value{
  serde_json::json!({"state":"SYNCED","mirror_phase":"SYNCED",
    "db2_health":"AVAILABLE","outbox_pressure_level":"NORMAL",
    "updated_at_ms":NOW-500,"last_success_ms":NOW-600,
    "lag_records":0,"error":null,"failure_reason":null,
    "feed_generation":"rebase-test-1","mirror_feed_generation":"rebase-test-1",
    "source_latest_seq":100,"mirror_applied_seq":100,"baseline_seq":1})
 }
 #[test]fn r136_fresh_same_generation_is_review_only_not_certification(){
  let report=audit_db2_status_json(&valid().to_string(),NOW,1_000).unwrap();
  assert_eq!(report.mirror_applied_seq,100);
  assert!(!report.certified_live_replica);
 }
 #[test]fn r136_stale_synced_is_not_fresh_and_future_clock_denied(){
  let mut v=valid();
  v["updated_at_ms"]=serde_json::json!(NOW-100_000);
  v["last_success_ms"]=serde_json::json!(NOW-100_500);
  assert_eq!(audit_db2_status_json(&v.to_string(),NOW,60_000),
   Err(Db2StatusError::Stale));
  let mut v=valid();
  v["updated_at_ms"]=serde_json::json!(NOW+1);
  assert_eq!(audit_db2_status_json(&v.to_string(),NOW,60_000),
   Err(Db2StatusError::Clock));
 }
 #[test]fn r136_source_generation_cursor_and_health_mismatch_denied(){
  let mut v=valid();
  v["mirror_feed_generation"]=serde_json::json!("rebase-other");
  assert_eq!(audit_db2_status_json(&v.to_string(),NOW,60_000),
   Err(Db2StatusError::Conflict));
  let mut v=valid();
  v["source_latest_seq"]=serde_json::json!(101);
  assert_eq!(audit_db2_status_json(&v.to_string(),NOW,60_000),
   Err(Db2StatusError::Conflict));
  let mut v=valid();
  v["lag_records"]=serde_json::json!(1);
  assert_eq!(audit_db2_status_json(&v.to_string(),NOW,60_000),
   Err(Db2StatusError::Conflict));
 }
 #[test]fn r137_fresh_heartbeat_cannot_mask_expired_apply_success(){
  let mut v=valid();
  v["updated_at_ms"]=serde_json::json!(NOW-500);
  v["last_success_ms"]=serde_json::json!(NOW-120_000);
  assert_eq!(audit_db2_status_json(&v.to_string(),NOW,60_000),
    Err(Db2StatusError::Stale));
  v["last_success_ms"]=serde_json::json!(NOW-600);
  assert!(audit_db2_status_json(&v.to_string(),NOW,60_000).is_ok());
 }
 #[test]fn r136_unresolved_prior_failure_not_silent_sync_proof(){
  let mut v=valid();
  v["failure_reason"]=serde_json::json!("OUTBOX_BYTE_LIMIT");
  assert_eq!(audit_db2_status_json(&v.to_string(),NOW,60_000),
    Err(Db2StatusError::UnresolvedFailure));
  let mut v=valid();
  v["error"]=serde_json::json!("storage unavailable");
  assert_eq!(audit_db2_status_json(&v.to_string(),NOW,60_000),
    Err(Db2StatusError::UnresolvedFailure));
 }
}
