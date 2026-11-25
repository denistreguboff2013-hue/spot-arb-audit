// R356–R365: PAPER-only, source-owned Event.record envelope proof.
// No network, private API, order authority, production DB or automatic writer.
// Python source: /home/tmpadmin/spotbot/ai/schemas.py (2026-10-22).
fn r356_approved<'a>(v:&str,allowed:&[&'a str])->bool{allowed.contains(&v)}
fn r356_event_ident(value:&serde_json::Value,required:bool,
 secrets:&[&str])->Result<serde_json::Value,AnalyticsR296Error>{
 if value.is_null()&&!required{return Ok(serde_json::Value::Null)}
 let s=value.as_str().ok_or(AnalyticsR296Error::Schema)?;
 if s.chars().count()<1||s.chars().count()>160
 ||!s.chars().all(|c|c.is_alphanumeric()||"_.:/-@".contains(c))
 ||secrets.iter().any(|t|!t.is_empty()&&s.contains(t))
 ||(0..s.len()).any(|i|s.is_char_boundary(i)&&r336_match_token(s,i).is_some()){
  return Err(AnalyticsR296Error::Schema)
 }
 Ok(value.clone())
}
pub fn r356_paper_event_record_without_io(
 event:&serde_json::Value,known_secrets:&[&str],now_ms:u64,
)->Result<serde_json::Value,AnalyticsR296Error>{
 use serde_json::{Value,json};
 let obj=event.as_object().ok_or(AnalyticsR296Error::Schema)?;
 if obj.get("mode")!=Some(&json!("paper")){return Err(AnalyticsR296Error::Schema)}
 let category=obj.get("category").and_then(Value::as_str)
   .ok_or(AnalyticsR296Error::Schema)?;
 let stage=obj.get("name").and_then(Value::as_str)
   .ok_or(AnalyticsR296Error::Schema)?;
 const CATEGORIES:&[&str]=&[
   "signals","decisions","orders","entries","exits","fills","errors",
   "daily_reports","parameter_snapshots","ai_recommendations"];
 const STAGES:&[&str]=&[
   "SIGNAL_SEEN","DECISION_MADE","PRECHECK_WARN","PRECHECK_DENY","PRECHECK_OK",
   "SPOT_ORDER_SENT","SPOT_FILLED","SPOT_PARTIAL","SPOT_FAILED","FUT_ORDER_SENT",
   "FUT_FILLED","FUT_PARTIAL","FUT_FAILED","STOP_SET","STOP_FAILED","ENTRY_COMMIT",
   "ENTRY_ABORT","UNWIND_DONE","MANUAL_REQUIRED","EXIT_STARTED","EXIT_COMMIT",
   "EXIT_FAILED","ORDER_REQUEST","ORDER_REQUEST_FAILED","ORDER_ACK",
   "ORDER_REJECTED","ORDER_CANCEL_REQUEST","ORDER_CANCELLED",
   "ORDER_STATUS_OBSERVED","TRUTH_CHECK","API_RESPONSE","NOTIFICATION_RESULT",
   "SAFETY_VIOLATION","ERROR","PARAMETER_SNAPSHOT","DAILY_REPORT",
   "RECOMMENDATION_PENDING","FILL_OBSERVED","STATE_SNAPSHOT"];
 const REASONS:&[&str]=&[
   "TELEMETRY_STALE","SPREAD_COLLAPSED","LOW_LIQUIDITY","MARKET_DATA_TIMEOUT",
   "ORDER_TIMEOUT","PARTIAL_FILL","MIN_NOTIONAL","API_ERROR","AUTH_ERROR",
   "RATE_LIMIT","BALANCE_LOW","FUT_LEG_FAILED","SPOT_LEG_FAILED","STOP_NOT_SET",
   "CANCEL_FAILED","EXCHANGE_TRUTH_MISMATCH","TELEGRAM_FAILED",
   "INSTRUMENT_MISMATCH","DATA_MISSING","MAX_OPEN_POSITIONS","DAILY_LIMIT",
   "PAPER_ORDER_BLOCKED","EXECUTION_OUTBOX_UNAVAILABLE","CLIENT_ORDER_ID_INVALID",
   "UNRESOLVED_MUTATION","MUTATION_OUTCOME_UNKNOWN","OTHER",
   "RISK_ENGINE_DISABLED","EXCHANGE_UNAVAILABLE","UNKNOWN_ACTIVE_LEG",
   "COST_MODEL_INCOMPLETE","EXPECTED_NET_BELOW_MINIMUM","INCIDENT_LIMIT"];
 if !r356_approved(category,CATEGORIES)||!r356_approved(stage,STAGES){
   return Err(AnalyticsR296Error::Schema)
 }
 let required_names=["event_id","source","run_id"];
 let optional_names=[
   "correlation_id","signal_id","base","shadow_group_id","comparison_id"];
 let mut identities=serde_json::Map::new();
 for name in required_names{
   identities.insert(name.to_string(),r356_event_ident(
     obj.get(name).ok_or(AnalyticsR296Error::Schema)?,true,known_secrets)?);
 }
 for name in optional_names{
   identities.insert(name.to_string(),r356_event_ident(
     obj.get(name).unwrap_or(&Value::Null),false,known_secrets)?);
 }
 let optional_present=|name:&str|identities.get(name).and_then(Value::as_str)
   .is_some_and(|x|!x.is_empty());
 if optional_present("comparison_id")&&
    (!optional_present("shadow_group_id")||!optional_present("signal_id")){
    return Err(AnalyticsR296Error::Schema)
 }
 if r356_approved(category,&["orders","entries","exits","fills"])&&
   !optional_present("correlation_id"){
    return Err(AnalyticsR296Error::Schema)
 }
 let reason=obj.get("reason_code").unwrap_or(&Value::Null);
 if !reason.is_null()&&
    !reason.as_str().is_some_and(|r|r356_approved(r,REASONS)){
    return Err(AnalyticsR296Error::Schema)
 }
 let time=obj.get("occurred_at_ms").and_then(Value::as_u64)
    .ok_or(AnalyticsR296Error::Schema)?;
 if time==0||time>now_ms.saturating_add(60_000){
    return Err(AnalyticsR296Error::Schema)
 }
 let payload=obj.get("payload").and_then(Value::as_object)
   .ok_or(AnalyticsR296Error::Schema)?;
 let evidence=obj.get("verification").and_then(Value::as_object)
   .ok_or(AnalyticsR296Error::Schema)?;
 if ["mode","is_exchange_verified","event_id","run_id"]
   .iter().any(|x|payload.contains_key(*x)){
    return Err(AnalyticsR296Error::Schema)
 }
 let status=evidence.get("status").map(|x|x.as_str()).unwrap_or(Some("unverified"))
   .ok_or(AnalyticsR296Error::Schema)?;
 let status=if status=="mismatch"{"contradicted"}else{status};
 if !r356_approved(status,&["unknown","unverified","observed",
    "partially_verified","verified","contradicted"])||status=="verified"{
   // Source Python rejects exchange-verified PAPER; no fake fill authority.
   return Err(AnalyticsR296Error::Schema)
 }
 let safe_payload=r336_scrub(&Value::Object(payload.clone()),known_secrets)?;
 let safe_evidence=r336_scrub(&Value::Object(evidence.clone()),known_secrets)?;
 let pjson=serde_json::to_string(&safe_payload).map_err(|_|AnalyticsR296Error::Schema)?;
 let ejson=serde_json::to_string(&safe_evidence).map_err(|_|AnalyticsR296Error::Schema)?;
 let mut record=serde_json::Map::new();
 for (k,v) in identities{record.insert(k,v);}
 for (k,v) in [
  ("mode",json!("paper")),("category",json!(category)),
  ("name",json!(stage)),("occurred_at_ms",json!(time)),
  ("reason_code",reason.clone()),("schema_version",json!(1)),
  ("verification_status",json!(status)),
  ("is_exchange_verified",json!(0)),
  ("payload_json",json!(pjson)),("verification_json",json!(ejson))
 ]{record.insert(k.to_string(),v);}
 // Never silently undercount Python envelope overhead. Bounded research
 // records only: conservative 16,384-byte cap including small spacing slack.
 let serialized=serde_json::to_vec(&record).map_err(|_|AnalyticsR296Error::Schema)?;
 if serialized.len().saturating_add(768)>16_384{
   return Err(AnalyticsR296Error::Schema)
 }
 Ok(Value::Object(record))
}
#[cfg(test)]
mod r356_tests{
 use super::*;
 use serde_json::{json,Value};
 const NOW:u64=1_792_660_000_000;
 fn input()->Value{
  json!({"mode":"paper","category":"decisions","name":"DECISION_MADE",
   "event_id":"r356-p001","source":"native.paper","run_id":"session_20748",
   "occurred_at_ms":1792659900000_u64,
   "correlation_id":null,"signal_id":"signal_1","base":"ONE",
   "shadow_group_id":null,"comparison_id":null,"reason_code":null,
   "payload":{"spread_bps":42,"api_key":"synthetic"}, "verification":{
     "status":"observed","source":"PUBLIC_DEPTH_MODEL_ONLY"}})
 }
 #[test]fn r356_source_paper_event_defaults_and_masked_evidence(){
  let mut e=input();e["verification"]=json!({"status":"mismatch","api-key":"X"});
  let a=r356_paper_event_record_without_io(&e,&[],NOW).unwrap();
  assert_eq!(a["schema_version"],1);
  assert_eq!(a["mode"],"paper");
  assert_eq!(a["verification_status"],"contradicted");
  assert_eq!(a["is_exchange_verified"],0);
  assert_eq!(serde_json::from_str::<Value>(a["payload_json"].as_str().unwrap()).unwrap()["api_key"],"[REDACTED]");
  assert_eq!(serde_json::from_str::<Value>(a["verification_json"].as_str().unwrap()).unwrap()["api-key"],"[REDACTED]");
 }
 #[test]fn r356_no_exchange_verified_paper_even_with_fake_evidence(){
  let mut e=input();e["verification"]=json!({
   "status":"verified","source":"exchange_reconcile",
   "state_matches":true,"instrument_ids_match":true,"hedge_matches":true});
  assert_eq!(r356_paper_event_record_without_io(&e,&[],NOW),
    Err(AnalyticsR296Error::Schema));
 }
 #[test]fn r356_execution_category_requires_correlation(){
  let mut e=input();e["category"]=json!("fills");e["name"]=json!("FILL_OBSERVED");
  assert_eq!(r356_paper_event_record_without_io(&e,&[],NOW),Err(AnalyticsR296Error::Schema));
  e["correlation_id"]=json!("co:r-100");
  assert!(r356_paper_event_record_without_io(&e,&[],NOW).is_ok());
 }
 #[test]fn r356_shadow_comparison_requires_signal_and_group(){
  let mut e=input();e["comparison_id"]=json!("cmp_1");
  assert_eq!(r356_paper_event_record_without_io(&e,&[],NOW),Err(AnalyticsR296Error::Schema));
  e["shadow_group_id"]=json!("shadow_1");
  assert!(r356_paper_event_record_without_io(&e,&[],NOW).is_ok());
 }
 #[test]fn r356_rejects_shadowing_tokens_identifier_and_future(){
  let mut e=input();e["payload"]["mode"]=json!("real");
  assert_eq!(r356_paper_event_record_without_io(&e,&[],NOW),Err(AnalyticsR296Error::Schema));
  e=input();e["event_id"]=json!("synthetic_EXAMPLE_999");
  assert_eq!(r356_paper_event_record_without_io(&e,&["EXAMPLE_999"],NOW),Err(AnalyticsR296Error::Schema));
  e=input();e["occurred_at_ms"]=json!(NOW+60_001);
  assert_eq!(r356_paper_event_record_without_io(&e,&[],NOW),Err(AnalyticsR296Error::Schema));
 }
 #[test]fn r356_rejects_invalid_reason_stage_and_real_mode(){
  let mut e=input();e["reason_code"]=json!("UNSUPPORTED");
  assert_eq!(r356_paper_event_record_without_io(&e,&[],NOW),Err(AnalyticsR296Error::Schema));
  e=input();e["name"]=json!("SPOT_EXECUTED_REAL");
  assert_eq!(r356_paper_event_record_without_io(&e,&[],NOW),Err(AnalyticsR296Error::Schema));
  e=input();e["mode"]=json!("real");
  assert_eq!(r356_paper_event_record_without_io(&e,&[],NOW),Err(AnalyticsR296Error::Schema));
 }
 #[test]fn r356_actual_python_event_record_twelve_cases_parity(){
  let fixtures:Value=serde_json::from_str(include_str!(
    "../config/native_r356_python_event_record_oracle.json")).unwrap();
  let cases=fixtures["cases"].as_array().unwrap();
  assert_eq!(cases.len(),12);
  let mut failed=Vec::new();
  for row in cases{
   let name=row["name"].as_str().unwrap();
   let secrets=row["known_secrets"].as_array().unwrap().iter()
     .map(|x|x.as_str().unwrap()).collect::<Vec<_>>();
   let actual=r356_paper_event_record_without_io(&row["event"],&secrets,NOW);
   let ok=if row["accept"]==true{
    if let Ok(actual)=actual{
     let mut expected=row["expected"].clone();
     // Python emits non-compact JSON text. Compare JSON meaning for payload/evidence.
     let mut a=actual.clone();
     for key in ["payload_json","verification_json"]{
      let pp:Value=serde_json::from_str(expected[key].as_str().unwrap()).unwrap();
      let rr:Value=serde_json::from_str(a[key].as_str().unwrap()).unwrap();
      expected[key]=pp;a[key]=rr;
     }
     a==expected
    }else{false}
   }else{actual.is_err()};
   if !ok{failed.push(name.to_string())}
  }
  assert!(failed.is_empty(),"Python Event.record discrepancies: {failed:?}");
 }
}
