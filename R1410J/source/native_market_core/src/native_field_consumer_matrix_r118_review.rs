//! R118 / Astra P1-06: static provenance of 160 mirrored configuration fields.
//! This is an OFFLINE audit of a conservative evidence ledger, NOT live parity.
//! A "consumer" here is a declared research consumer, NOT proof of execution.
use serde_json::Value;
use std::collections::BTreeSet;

#[derive(Debug,Clone,PartialEq,Eq)]
pub enum FieldAuditError{Json,Count,Duplicate,Missing,Type,Status,Consumer,Authority}
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct FieldAuditSummary{
 pub fields:usize,pub schema_only:usize,pub declared_consumer:usize,
 pub declared_test:usize,pub no_consumer:usize,pub active_binary_proven:usize,
}
pub fn audit_field_sources(schema:&str,status:&str,evidence:&str)
 ->Result<FieldAuditSummary,FieldAuditError>{
 let schema:Value=serde_json::from_str(schema).map_err(|_|FieldAuditError::Json)?;
 let status:Value=serde_json::from_str(status).map_err(|_|FieldAuditError::Json)?;
 let ledger:Value=serde_json::from_str(evidence).map_err(|_|FieldAuditError::Json)?;
 let s=schema.as_object().ok_or(FieldAuditError::Json)?;
 let old=status.as_object().ok_or(FieldAuditError::Json)?;
 let entries=ledger.get("entries").and_then(Value::as_array).ok_or(FieldAuditError::Json)?;
 if s.len()!=160||old.len()!=160||entries.len()!=160
  ||ledger["tracked_fields"].as_u64()!=Some(160)
  ||ledger["original_mirror_only"].as_u64()!=Some(131){
  return Err(FieldAuditError::Count)
 }
 let mut seen=BTreeSet::new();
 let mut summary=FieldAuditSummary{fields:0,schema_only:0,declared_consumer:0,
  declared_test:0,no_consumer:0,active_binary_proven:0};
 for e in entries{
  let key=e["field"].as_str().ok_or(FieldAuditError::Missing)?;
  if !seen.insert(key){return Err(FieldAuditError::Duplicate)}
  let t=s.get(key).and_then(Value::as_str).ok_or(FieldAuditError::Missing)?;
  let prior=old.get(key).ok_or(FieldAuditError::Missing)?;
  if e["syntax_type"].as_str()!=Some(t)||prior["type"].as_str()!=Some(t){
   return Err(FieldAuditError::Type)
  }
  if e["source_status"]!=prior["status"]{return Err(FieldAuditError::Status)}
  if e["active_binary_proven"].as_bool()!=Some(false){
   return Err(FieldAuditError::Authority)
  }
  if e["confirmed_scope"].as_str().is_none(){return Err(FieldAuditError::Missing)}
  let consumer=e["consumer"].as_str().filter(|s|!s.trim().is_empty());
  let test=e["test"].as_str().filter(|s|!s.trim().is_empty());
  if consumer.is_some()!=test.is_some(){return Err(FieldAuditError::Consumer)}
  summary.fields+=1;
  if prior["status"]=="mirrored_schema_only_NOT_YET_runtime_wired"{summary.schema_only+=1}
  if consumer.is_some(){summary.declared_consumer+=1}else{summary.no_consumer+=1}
  if test.is_some(){summary.declared_test+=1}
 }
 if seen.len()!=s.len()||summary.schema_only!=131{return Err(FieldAuditError::Count)}
 Ok(summary)
}
pub fn audit_embedded_legacy_fields()->Result<FieldAuditSummary,FieldAuditError>{
 audit_field_sources(include_str!("../config/native_field_schema.json"),
  include_str!("../config/native_field_status.json"),
  include_str!("../config/native_field_evidence_r113_review.json"))
}

#[cfg(test)]
mod tests{
 use super::*;
 fn texts()->(String,String,String){
  (include_str!("../config/native_field_schema.json").into(),
   include_str!("../config/native_field_status.json").into(),
   include_str!("../config/native_field_evidence_r113_review.json").into())
 }
 #[test]fn r118_schema_and_all_160_evidence_fields_are_consistent(){
  let s=audit_embedded_legacy_fields().unwrap();
  assert_eq!(s.fields,160);
  assert_eq!(s.schema_only,131);
  assert_eq!(s.declared_consumer,9);
  assert_eq!(s.declared_test,9);
  assert_eq!(s.no_consumer,151);
  assert_eq!(s.active_binary_proven,0);
 }
 #[test]fn r118_duplicate_and_missing_consumer_proof_are_denied(){
  let (a,b,c)=texts();
  let mut e:Value=serde_json::from_str(&c).unwrap();
  e["entries"][1]["field"]=e["entries"][0]["field"].clone();
  assert_eq!(audit_field_sources(&a,&b,&e.to_string()).unwrap_err(),FieldAuditError::Duplicate);
  let mut e:Value=serde_json::from_str(&c).unwrap();
  let n=e["entries"].as_array().unwrap().iter().position(|v|v["consumer"].is_string()).unwrap();
  e["entries"][n]["test"]=Value::Null;
  assert_eq!(audit_field_sources(&a,&b,&e.to_string()).unwrap_err(),FieldAuditError::Consumer);
 }
 #[test]fn r118_fake_active_claim_type_and_status_drift_denied(){
  let (a,b,c)=texts();
  let mut e:Value=serde_json::from_str(&c).unwrap();
  e["entries"][0]["active_binary_proven"]=Value::Bool(true);
  assert_eq!(audit_field_sources(&a,&b,&e.to_string()).unwrap_err(),FieldAuditError::Authority);
  let mut e:Value=serde_json::from_str(&c).unwrap();
  e["entries"][0]["syntax_type"]=Value::String("float".into());
  assert_eq!(audit_field_sources(&a,&b,&e.to_string()).unwrap_err(),FieldAuditError::Type);
  let mut e:Value=serde_json::from_str(&c).unwrap();
  e["entries"][0]["source_status"]=Value::String("native_live_confirmed".into());
  assert_eq!(audit_field_sources(&a,&b,&e.to_string()).unwrap_err(),FieldAuditError::Status);
 }
}
#[cfg(test)]
mod r125{
 use super::*;
 #[test]fn r125_reuse_existing_readonly_public_feed_seventeen_consumers(){
  let v:Value=serde_json::from_str(include_str!(
    "../config/native_field_evidence_r125_public_feed_review.json")).unwrap();
  let s:Value=serde_json::from_str(include_str!("../config/native_field_schema.json")).unwrap();
  let e:Value=serde_json::from_str(include_str!(
    "../config/native_field_evidence_r113_review.json")).unwrap();
  assert_eq!(v["active_binary_proven"],false);
  assert_eq!(v["scope"],"rust_research_readonly_only");
  let fields=v["fields"].as_array().unwrap();
  assert_eq!(fields.len(),17);
  let distinct=fields.iter().filter_map(Value::as_str).collect::<BTreeSet<_>>();
  assert_eq!(distinct.len(),17);
  for path in distinct{
   assert!(!s[path].is_null(),"{path}");
   let prior=e["entries"].as_array().unwrap().iter().find(|x|x["field"]==path).unwrap();
   assert_eq!(prior["active_binary_proven"],false);
   assert!(include_str!("native_public_feed_policy_review.rs")
    .contains(path.rsplit('.').next().unwrap()),"{path}");
  }
 }
 #[test]fn r125_public_feed_values_are_consumed_not_merely_parsed(){
  use crate::native_trading_config::NativeTradingConfig;
  use crate::native_public_feed_policy_review::PublicFeedPolicy;
  let source=include_str!("../config/native_trading.yaml");
  let old=PublicFeedPolicy::from_config(&NativeTradingConfig::parse(source).unwrap()).unwrap();
  let adjusted=source.replacen("slip_bps_trade: 5.0","slip_bps_trade: 6.0",1)
   .replacen("max_age_sec: 45.0","max_age_sec: 50.0",1)
   .replacen("every_sec: 120","every_sec: 130",1);
  assert_ne!(source,adjusted);
  let n=PublicFeedPolicy::from_config(&NativeTradingConfig::parse(&adjusted).unwrap()).unwrap();
  assert_eq!(old.slip_bps_trade,5.0);
  assert_eq!(n.slip_bps_trade,6.0);
  assert_eq!(old.telemetry_max_age_ms,45_000);
  assert_eq!(n.telemetry_max_age_ms,50_000);
  assert_eq!(old.reconcile_every_sec,120);
  assert_eq!(n.reconcile_every_sec,130);
 }
}
