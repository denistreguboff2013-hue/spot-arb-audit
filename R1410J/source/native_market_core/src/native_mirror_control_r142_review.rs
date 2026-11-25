//! R142: offline readonly DB1 mirror_control to DB2 status comparison.
//! Reflects old mirror_outbox.py fields but grants NO DB2 apply, service
//! restart, historical failure-clearing or trading permission.
use crate::native_db2_mirror_status_r136_review::audit_db2_status_json;
use serde_json::Value;
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum MirrorControlError{Json,Shape,SourceMismatch,Stale,HistoryMarker}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct MirrorControlReviewR142{
 pub latest_seq:u64,pub cleanup_through:u64,
 pub retained_records:u64,pub retained_bytes:u64,
 pub has_historical_failure_marker:bool,
 pub may_restore_mirror:bool,pub may_authorize_trading:bool,
}
fn n(v:&Value,k:&str)->Result<u64,MirrorControlError>{
 v.get(k).and_then(Value::as_u64).ok_or(MirrorControlError::Shape)
}
fn s<'a>(v:&'a Value,k:&str)->Result<&'a str,MirrorControlError>{
 v.get(k).and_then(Value::as_str).filter(|x|!x.is_empty())
  .ok_or(MirrorControlError::Shape)
}
pub fn inspect_db1_mirror_control(
 control_json:&str,status_json:&str,now:u64,max_age_ms:u64,
)->Result<MirrorControlReviewR142,MirrorControlError>{
 let ctrl:Value=serde_json::from_str(control_json)
  .map_err(|_|MirrorControlError::Json)?;
 if ctrl["state"]!="NORMAL"||n(&ctrl,"schema_generation")?!=1{
  return Err(MirrorControlError::Shape)
 }
 if !ctrl["failure_reason"].is_null()||!ctrl["failure_seq"].is_null(){
  return Err(MirrorControlError::HistoryMarker)
 }
 let latest=n(&ctrl,"latest_seq")?;
 let cleanup=n(&ctrl,"cleanup_through")?;
 let records=n(&ctrl,"retained_records")?;
 let bytes=n(&ctrl,"retained_bytes")?;
 if latest==0||cleanup!=latest||records!=0||bytes!=0
  ||n(&ctrl,"baseline_seq")?>latest{
  return Err(MirrorControlError::Shape)
 }
 let status=audit_db2_status_json(status_json,now,max_age_ms)
  .map_err(|_|MirrorControlError::Stale)?;
 let raw:Value=serde_json::from_str(status_json)
  .map_err(|_|MirrorControlError::Json)?;
 if s(&ctrl,"feed_generation")?!=status.feed_generation
  ||s(&ctrl,"baseline_source_identity")?!=s(&raw,"source_identity")?
  ||n(&ctrl,"baseline_seq")?!=n(&raw,"baseline_seq")?
  ||status.mirror_applied_seq!=latest{
  return Err(MirrorControlError::SourceMismatch)
 }
 Ok(MirrorControlReviewR142{
  latest_seq:latest,cleanup_through:cleanup,
  retained_records:records,retained_bytes:bytes,
  has_historical_failure_marker:false,
  may_restore_mirror:false,may_authorize_trading:false,
 })
}
#[cfg(test)]
mod tests{
 use super::*;
 const T:u64=1_791_784_000_000;
 fn ctrl()->Value{serde_json::json!({
  "state":"NORMAL","schema_generation":1,
  "latest_seq":100,"cleanup_through":100,"retained_records":0,"retained_bytes":0,
  "baseline_seq":1,"feed_generation":"rebase-abc",
  "baseline_source_identity":"2080:100",
  "failure_reason":null,"failure_seq":null,
 })}
 fn status()->Value{serde_json::json!({
  "state":"SYNCED","mirror_phase":"SYNCED","db2_health":"AVAILABLE",
  "outbox_pressure_level":"NORMAL","lag_records":0,"error":null,
  "failure_reason":null,"source_identity":"2080:100",
  "feed_generation":"rebase-abc","mirror_feed_generation":"rebase-abc",
  "source_latest_seq":100,"mirror_applied_seq":100,"baseline_seq":1,
  "updated_at_ms":T-100,"last_success_ms":T-200,
 })}
 #[test]fn r142_clean_control_and_status_are_non_authorizing_only(){
  let m=inspect_db1_mirror_control(&ctrl().to_string(),&status().to_string(),T,60_000).unwrap();
  assert_eq!(m.latest_seq,100);
  assert!(!m.may_restore_mirror&&!m.may_authorize_trading);
 }
 #[test]fn r142_historical_outbox_byte_failure_not_erased_by_normal_state(){
  let mut c=ctrl();
  c["failure_reason"]=serde_json::json!("OUTBOX_BYTE_LIMIT");
  c["failure_seq"]=serde_json::json!(100);
  assert_eq!(inspect_db1_mirror_control(&c.to_string(),&status().to_string(),T,60_000),
   Err(MirrorControlError::HistoryMarker));
 }
 #[test]fn r142_source_identity_generation_or_seq_disagreement_denied(){
  let c=ctrl();let mut s=status();
  s["source_identity"]=serde_json::json!("other");
  assert_eq!(inspect_db1_mirror_control(&c.to_string(),&s.to_string(),T,60_000),
   Err(MirrorControlError::SourceMismatch));
  let mut s=status();
  s["feed_generation"]=serde_json::json!("rebase-other");
  s["mirror_feed_generation"]=serde_json::json!("rebase-other");
  assert_eq!(inspect_db1_mirror_control(&c.to_string(),&s.to_string(),T,60_000),
   Err(MirrorControlError::SourceMismatch));
  let mut s=status();
  s["source_latest_seq"]=serde_json::json!(99);
  s["mirror_applied_seq"]=serde_json::json!(99);
  assert_eq!(inspect_db1_mirror_control(&c.to_string(),&s.to_string(),T,60_000),
   Err(MirrorControlError::SourceMismatch));
 }
 #[test]fn r142_cleaned_cursor_must_equal_latest_and_status_must_be_fresh(){
  let mut c=ctrl();
  c["cleanup_through"]=serde_json::json!(99);
  assert_eq!(inspect_db1_mirror_control(&c.to_string(),&status().to_string(),T,60_000),
   Err(MirrorControlError::Shape));
  let c=ctrl();
  assert_eq!(inspect_db1_mirror_control(&c.to_string(),&status().to_string(),T+120_000,60_000),
   Err(MirrorControlError::Stale));
 }
}
