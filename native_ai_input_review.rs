//! R46 P1-06: Rust allowlist adapter for the aggregate-only AI advisor.
 //! Maps Python ai/advisor_input.py's published fields; no filesystem or network.
use serde_json::{json, Map, Value};
use crate::native_ai_advisor_review::{build_pending_advisor_offline,AdvisorError};
use crate::native_ai_output_boundary_review::{checked_counts,checked_hourly,checked_codes,checked_reasons,verify_aggregate_only};

const MODES:[&str;4]=["paper","real","test","replay"];
const STORAGE:&[&str]=&[
 "status","db_size_bytes","db_limit_bytes","used_ratio","pressure_level",
 "wal_bytes","oldest_raw_event","newest_raw_event","rolled_up_raw_events",
 "deleted_raw_events","events_by_mode","events_by_type","collector_dropped",
 "collector_write_failures","collector_last_error","collector_active_runs","thresholds"];
const PAIR:&[&str]=&["symbol","signals","paper_entries","paper_exits",
 "known_pnl_exits","unknown_pnl_exits","errors","reject_reasons",
 "avg_latency_ms","avg_slippage_bps","evidence_score","rankable"];
const QUALITY:&[&str]=&["paper_entries_total","paper_exits_total","real_entries_total",
 "real_exits_total","verified_real_entries","verified_real_exits","unknown_entries",
 "unknown_exits","manual_required_count","contradicted_count","partial_fill_count",
 "missing_fee_count","missing_slippage_count","missing_latency_count","by_mode"];
const COHORT:&[&str]=&["scope","signals_total","signals_with_shadow_group",
 "signals_with_comparison_id","distinct_shadow_groups","distinct_comparison_ids",
 "trade_commits_total","trade_commits_with_comparison_id","unlinked_trade_commits",
 "linkage_complete","matched_verified_cohorts","candidate_cohorts",
 "excluded_cohorts","by_mode"];
const GAP:&[&str]=&["status","candidate_count","matched_count",
 "paper_net_pnl_usdt","real_net_pnl_usdt","gap_usdt","excluded",
 "real_exit_count","verified_real_exit_count","formula"];
const SIGNAL:&[&str]=&["raw_spread_count","raw_spread_min","raw_spread_max",
 "raw_spread_average","net_spread_count","net_spread_min","net_spread_max",
 "net_spread_average","telemetry_age_count","telemetry_age_average",
 "telemetry_age_max","depth_spot_average","depth_fut_average"];

fn field<'a>(v:&'a Value,k:&str)->Option<&'a Value>{
 v.as_object().and_then(|m|m.get(k))
}
fn select(source:Option<&Value>,keys:&[&str])->Value{
 let mut map=Map::new();
 for &key in keys{
  map.insert(key.to_owned(),source.and_then(|v|field(v,key))
   .cloned().unwrap_or(Value::Null));
 }
 Value::Object(map)
}
fn flag(source:&Value,key:&str)->bool{
 field(source,key).and_then(Value::as_bool).unwrap_or(false)
}

pub fn sanitize_daily_report(report:&Value)->Result<Value,AdvisorError>{
 if !report.is_object(){return Err(AdvisorError::InvalidInput)}
 if serde_json::to_vec(report).map_err(|_|AdvisorError::InvalidInput)?.len()>1_000_000{
  return Err(AdvisorError::TooLarge)
 }
 let source_modes=field(report,"modes").and_then(Value::as_object)
  .ok_or(AdvisorError::InvalidInput)?;
 let mut modes=Map::new();
 for mode in MODES{
  let row=source_modes.get(mode).ok_or(AdvisorError::InvalidInput)?;
  if !row.is_object(){return Err(AdvisorError::InvalidInput)}
  let counts=checked_counts(field(row,"counts"))?;
  let safe=json!({
   "counts":counts,
   "known_net_pnl_usdt":if mode=="real"{Value::Null}else{field(row,"known_net_pnl_usdt").cloned().unwrap_or(Value::Null)},
   "unknown_pnl_trades":field(row,"unknown_pnl_trades").cloned().unwrap_or(json!(0)),
   "pnl_complete":if mode=="real"{false}else{flag(row,"pnl_complete")},
   "signal_metrics":select(field(row,"signal_metrics"),SIGNAL)
  });
  modes.insert(mode.to_string(),safe);
 }
 let mut hourly=Map::new();
 for mode in MODES{
  let data=checked_hourly(field(report,"hourly_activity").and_then(|x|field(x,mode)))?;
  hourly.insert(mode.to_string(),data);
 }
 let pair_rows=field(report,"pair_evidence").and_then(Value::as_array)
  .cloned().unwrap_or_default();
 if pair_rows.len()>2_000{return Err(AdvisorError::TooLarge)}
 for row in &pair_rows{
  let symbol=row.get("symbol").and_then(Value::as_str)
   .ok_or(AdvisorError::InvalidInput)?;
  if symbol.is_empty()||symbol.len()>32||!symbol.bytes().all(|b|
     b.is_ascii_uppercase()||b.is_ascii_digit()||b==b'_'||b==b'-'){
   return Err(AdvisorError::InvalidInput)
  }
 }
 let pairs:Vec<Value>=pair_rows.iter().map(|v|select(Some(v),PAIR)).collect();
 let date=field(report,"date").and_then(Value::as_str)
  .ok_or(AdvisorError::InvalidInput)?;
 let timezone=field(report,"timezone").and_then(Value::as_str)
  .ok_or(AdvisorError::InvalidInput)?;
 let summary=json!({
  "analytics_version":field(report,"analytics_version"),
  "modes":modes,
  "storage_pressure_warning":flag(report,"storage_pressure_warning"),
  "evidence_incomplete":true,
  "real_pnl_available":false,
  "advisor_ready":false,
  "real_launch_authorized":false
 });
 let mut storage=select(field(report,"storage_health"),STORAGE);
 if let Some(map)=storage.as_object_mut(){
  // Freeform collector diagnostics cannot cross into the AI prompt.
  map.insert("collector_last_error".into(),Value::Null);
 }
 // Offline report is NOT authenticated account custody/fill evidence.
 // Keep claimed paper aggregates, but never forward "verified real" proof.
 let mut evidence=select(field(report,"evidence_quality"),QUALITY);
 if let Some(m)=evidence.as_object_mut(){
  m.insert("verified_real_entries".into(),json!(0));
  m.insert("verified_real_exits".into(),json!(0));
 }
 let mut gap=select(field(report,"reality_gap"),GAP);
 if let Some(m)=gap.as_object_mut(){
  m.insert("status".into(),json!("UNVERIFIED_OFFLINE_SOURCE"));
  m.insert("real_net_pnl_usdt".into(),Value::Null);
  m.insert("gap_usdt".into(),Value::Null);
  m.insert("verified_real_exit_count".into(),json!(0));
  m.insert("matched_count".into(),json!(0));
 }
 let result=json!({
  "advisor_input_version":"1.0","date":date,"timezone":timezone,
  "summary":summary,"storage_health":storage,
  "evidence_quality":evidence,
  "cohort_coverage":select(field(report,"cohort_coverage"),COHORT),
  "reality_gap":gap,
  "pair_stats":pairs,"hourly_activity":hourly,
  "failure_reasons":checked_reasons(field(report,"failure_reasons"))?,
  "reality_gap_status":"UNVERIFIED_OFFLINE_SOURCE",
  "data_warnings":checked_codes(field(report,"data_warnings"))?,
  "not_enough_data_reasons":checked_codes(field(report,"not_enough_data_reasons"))?
 });
 verify_aggregate_only(&result)?;
 Ok(result)
}

/// Offline R45/R46 integration: sanitized aggregate -> advisory only.
/// The 'advisor_ready' flag is not exchange authentication or an order permit.
pub fn advisory_from_report_without_io(report:&Value)->Result<Value,AdvisorError>{
 let payload=sanitize_daily_report(report)?;
 build_pending_advisor_offline(&payload)
}

#[cfg(test)]
mod tests{
 use super::*;
 fn report()->Value{
  let mut modes=Map::new();
  for mode in MODES{
   modes.insert(mode.into(),json!({
    "counts":{"accepted":2},"known_net_pnl_usdt":null,
    "unknown_pnl_trades":1,"pnl_complete":false,
    "signal_metrics":{"net_spread_count":2,"other_metric":44}
   }));
  }
  json!({"date":"2026-10-16","timezone":"Asia/Yekaterinburg",
   "modes":modes,"advisor_ready":false,"evidence_incomplete":true,
   "storage_health":{"status":"OK","used_ratio":0.2,"other_metric":44},
   "evidence_quality":{"verified_real_exits":0},
   "pair_evidence":[{"symbol":"ZEC","paper_exits":21,"rankable":false,
     "other_metric":44}],"extra_report_field":"DISCARDED"})
 }
 #[test]
 fn python_allowlist_fields_and_injected_extras(){
  let input=sanitize_daily_report(&report()).unwrap();
  assert_eq!(input["advisor_input_version"],"1.0");
  assert_eq!(input["summary"]["real_launch_authorized"],false);
  assert_eq!(input["summary"]["modes"]["paper"]["counts"]["accepted"],2);
  assert_eq!(input["summary"]["modes"]["paper"]["signal_metrics"]["net_spread_count"],2);
  assert!(input["summary"]["modes"]["paper"]["signal_metrics"].get("other_metric").is_none());
  assert!(input["storage_health"].get("other_metric").is_none());
  assert!(input["pair_stats"][0].get("other_metric").is_none());
  assert!(input.get("extra_report_field").is_none());
  assert_eq!(input["evidence_quality"]["verified_real_exits"],0);
  assert_eq!(input["pair_stats"][0]["symbol"],"ZEC");
  assert_eq!(input["hourly_activity"]["test"],json!([]));
 }
 #[test]
 fn end_to_end_is_pending_only(){
  let result=advisory_from_report_without_io(&report()).unwrap();
  assert_eq!(result["strategy_tuning_allowed"],false);
  assert_eq!(result["recommendation_count"],2);
  assert_eq!(result["policy"]["order_submission_allowed"],false);
  assert_eq!(result["policy"]["config_write_allowed"],false);
  assert_eq!(result["recommendations"][0]["reason_code"],"NO_VERIFIED_REAL_EXITS");
  assert_eq!(result["recommendations"][1]["reason_code"],"PAIR_EVIDENCE_NOT_RANKABLE");
 }
 #[test]
 fn absent_mode_and_unbounded_report_are_denied(){
  let mut r=report();
  r["modes"].as_object_mut().unwrap().remove("real");
  assert_eq!(sanitize_daily_report(&r),Err(AdvisorError::InvalidInput));
  let mut r=report();
  r["pair_evidence"]=json!((0..2001).map(|_|json!({})).collect::<Vec<_>>());
  assert_eq!(sanitize_daily_report(&r),Err(AdvisorError::TooLarge));
  let mut r=report();
  r["hourly_activity"]=json!({"paper":vec![0;49]});
  assert_eq!(sanitize_daily_report(&r),Err(AdvisorError::TooLarge));
 }
 #[test]
 fn claimed_real_truth_cannot_unlock_offline_ai(){
  let mut r=report();
  r["advisor_ready"]=json!(true);
  r["real_pnl_available"]=json!(true);
  r["evidence_incomplete"]=json!(false);
  r["evidence_quality"]["verified_real_exits"]=json!(100);
  let result=sanitize_daily_report(&r).unwrap();
  assert_eq!(result["summary"]["advisor_ready"],false);
  assert_eq!(result["summary"]["real_pnl_available"],false);
  assert_eq!(result["summary"]["evidence_incomplete"],true);
  assert_eq!(result["evidence_quality"]["verified_real_exits"],0);
  assert_eq!(result["evidence_quality"]["verified_real_entries"],0);
  assert_eq!(result["reality_gap"]["status"],"UNVERIFIED_OFFLINE_SOURCE");
  assert_eq!(advisory_from_report_without_io(&r).unwrap()["strategy_tuning_allowed"],false);
  let decision=advisory_from_report_without_io(&r).unwrap();
  let reasons=decision["recommendations"]
   .as_array().unwrap().iter().map(|x|x["reason_code"].as_str().unwrap_or(""))
   .collect::<Vec<_>>();
  assert!(reasons.contains(&"NO_VERIFIED_REAL_EXITS"));
 }
 #[test]
 fn arbitrary_nested_details_never_cross_ai_boundary(){
  let mut r=report();
  r["modes"]["paper"]["counts"]["arbitrary"]=json!("user-provided");
  assert_eq!(sanitize_daily_report(&r),Err(AdvisorError::InvalidInput));
  let mut r=report();
  r["hourly_activity"]=json!({"real":[{"hour":1,"events":2},
    {"hour":1,"events":3}]});
  assert_eq!(sanitize_daily_report(&r),Err(AdvisorError::InvalidInput));
  let mut r=report();
  r["storage_health"]["collector_last_error"]=json!("unexpected raw diagnostic");
  let result=sanitize_daily_report(&r).unwrap();
  assert_eq!(result["storage_health"]["collector_last_error"],Value::Null);
  let mut r=report();
  r["pair_evidence"][0]["symbol"]=json!("invalid pair");
  assert_eq!(sanitize_daily_report(&r),Err(AdvisorError::InvalidInput));
 }
}
