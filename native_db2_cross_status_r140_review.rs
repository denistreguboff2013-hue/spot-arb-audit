//! R140: validate two successive DB1=>DB2 status snapshots offline.
//! Neither successive statuses nor a healthy local mirror certify independent
//! durability or authorize re-enabling a stopped legacy service.
use crate::native_db2_mirror_status_r136_review::audit_db2_status_json;
use serde_json::Value;

#[derive(Clone,Debug,PartialEq,Eq)]
pub enum Db2ContinuityError{Status,Provenance,Cursor,Clock}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct Db2ContinuityR140{
 pub observed_generation:String,pub from_seq:u64,pub to_seq:u64,
 pub source_identity:String,pub source_advanced:bool,
 pub independent_durability_proven:bool,pub may_promote_mirror:bool,
}
fn number(v:&Value,key:&str)->Result<u64,Db2ContinuityError>{
 v.get(key).and_then(Value::as_u64).ok_or(Db2ContinuityError::Provenance)
}
fn text_field<'a>(v:&'a Value,key:&str)->Result<&'a str,Db2ContinuityError>{
 v.get(key).and_then(Value::as_str)
  .filter(|s|!s.is_empty()&&s.len()<=128)
  .ok_or(Db2ContinuityError::Provenance)
}
pub fn inspect_status_pair(
 previous_json:&str,current_json:&str,
 previous_observed_ms:u64,current_observed_ms:u64,max_age_ms:u64,
)->Result<Db2ContinuityR140,Db2ContinuityError>{
 if current_observed_ms<=previous_observed_ms{
  return Err(Db2ContinuityError::Clock)
 }
 let old=audit_db2_status_json(previous_json,previous_observed_ms,max_age_ms)
  .map_err(|_|Db2ContinuityError::Status)?;
 let new=audit_db2_status_json(current_json,current_observed_ms,max_age_ms)
  .map_err(|_|Db2ContinuityError::Status)?;
 let prev:Value=serde_json::from_str(previous_json)
  .map_err(|_|Db2ContinuityError::Status)?;
 let cur:Value=serde_json::from_str(current_json)
  .map_err(|_|Db2ContinuityError::Status)?;
 let source=text_field(&prev,"source_identity")?;
 if text_field(&cur,"source_identity")?!=source
  ||old.feed_generation!=new.feed_generation
  ||number(&prev,"baseline_seq")?!=number(&cur,"baseline_seq")?
  ||number(&prev,"mirror_baseline_seq")?!=number(&cur,"mirror_baseline_seq")?{
  return Err(Db2ContinuityError::Provenance)
 }
 if new.mirror_applied_seq<old.mirror_applied_seq
  ||new.source_latest_seq<old.source_latest_seq{
  return Err(Db2ContinuityError::Cursor)
 }
 if new.updated_at_ms<old.updated_at_ms
  ||number(&cur,"last_success_ms")?<number(&prev,"last_success_ms")?{
  return Err(Db2ContinuityError::Clock)
 }
 Ok(Db2ContinuityR140{
  observed_generation:new.feed_generation,
  from_seq:old.mirror_applied_seq,to_seq:new.mirror_applied_seq,
  source_identity:source.into(),
  source_advanced:new.source_latest_seq>old.source_latest_seq,
  independent_durability_proven:false,may_promote_mirror:false,
 })
}
#[cfg(test)]
mod tests{
 use super::*;
 const T:u64=1_791_784_000_000;
 fn status(n:u64,seq:u64)->Value{
  serde_json::json!({
   "state":"SYNCED","mirror_phase":"SYNCED","db2_health":"AVAILABLE",
   "outbox_pressure_level":"NORMAL","error":null,"failure_reason":null,
   "updated_at_ms":n-200,"last_success_ms":n-300,"lag_records":0,
   "feed_generation":"rebase-abcdef","mirror_feed_generation":"rebase-abcdef",
   "source_identity":"2080:100","source_latest_seq":seq,
   "mirror_applied_seq":seq,"baseline_seq":1,"mirror_baseline_seq":1
  })
 }
 #[test]fn r140_monotone_source_and_replica_is_non_authorizing(){
  let a=status(T,100);let b=status(T+1_000,200);
  let result=inspect_status_pair(&a.to_string(),&b.to_string(),T,T+1_000,30_000).unwrap();
  assert_eq!(result.from_seq,100);assert_eq!(result.to_seq,200);
  assert!(result.source_advanced);
  assert!(!result.independent_durability_proven&&!result.may_promote_mirror);
 }
 #[test]fn r140_generation_source_identity_and_baseline_change_denied(){
  let a=status(T,100);let mut b=status(T+1_000,200);
  b["source_identity"]=serde_json::json!("2080:999");
  assert_eq!(inspect_status_pair(&a.to_string(),&b.to_string(),T,T+1_000,30_000),
   Err(Db2ContinuityError::Provenance));
  let mut b=status(T+1_000,200);
  b["baseline_seq"]=serde_json::json!(20);
  assert_eq!(inspect_status_pair(&a.to_string(),&b.to_string(),T,T+1_000,30_000),
   Err(Db2ContinuityError::Provenance));
  let mut b=status(T+1_000,200);
  b["feed_generation"]=serde_json::json!("rebase-other");
  b["mirror_feed_generation"]=serde_json::json!("rebase-other");
  assert_eq!(inspect_status_pair(&a.to_string(),&b.to_string(),T,T+1_000,30_000),
   Err(Db2ContinuityError::Provenance));
 }
 #[test]fn r140_cursor_rewind_and_time_rewind_denied(){
  let a=status(T,100);let b=status(T+1_000,99);
  assert_eq!(inspect_status_pair(&a.to_string(),&b.to_string(),T,T+1_000,30_000),
   Err(Db2ContinuityError::Cursor));
  let b=status(T+1_000,100);
  assert_eq!(inspect_status_pair(&a.to_string(),&b.to_string(),T,T,30_000),
   Err(Db2ContinuityError::Clock));
  let b=status(T+40_000,100);
  assert_eq!(inspect_status_pair(&a.to_string(),&b.to_string(),T,T+41_000,100),
   Err(Db2ContinuityError::Status));
 }
 #[test]fn r140_stale_or_failed_inner_status_blocks_even_if_newer_epoch(){
  let a=status(T,100);let mut b=status(T+1_000,200);
  b["failure_reason"]=serde_json::json!("OUTBOX_BYTE_LIMIT");
  assert_eq!(inspect_status_pair(&a.to_string(),&b.to_string(),T,T+1_000,30_000),
   Err(Db2ContinuityError::Status));
  let mut b=status(T+1_000,200);
  b["mirror_phase"]=serde_json::json!("BOOTSTRAP");
  assert_eq!(inspect_status_pair(&a.to_string(),&b.to_string(),T,T+1_000,30_000),
   Err(Db2ContinuityError::Status));
 }
}
