//! R45 P1-06: deterministic Python ai/advisor.py pending-only subset port.
//! Offline sanitized aggregate input; NO config writes, POST, keys or background job.
use serde_json::{json,Value};
use std::collections::BTreeSet;

#[derive(Clone,Debug,PartialEq,Eq)]
pub enum AdvisorError{InvalidInput,TooLarge}

fn nonnegative(v:Option<&Value>)->u64{
 v.and_then(Value::as_u64).unwrap_or(0)
}
fn field<'a>(v:&'a Value,k:&str)->Option<&'a Value>{
 v.as_object().and_then(|m|m.get(k))
}
fn count(v:&Value,k:&str)->u64{nonnegative(field(v,k))}
fn string_field(v:&Value,k:&str,default:&str)->String{
 field(v,k).and_then(Value::as_str).unwrap_or(default).to_string()
}
fn pending(day:&str,scope:&str,parameter:&str,current:Value,
 recommended:Value,code:&str,reason:&str,evidence:Value,
 expected_effect:&str,confidence:&str,risk_level:&str,
 side_effect_risk:&str,mode:&str)->Value{
 let item=json!({
  "status":"pending","scope":scope,"parameter":parameter,
  "current":current,"recommended":recommended,"reason_code":code,
  "reason":reason,"evidence":evidence,"expected_effect":expected_effect,
  "confidence":confidence,"risk_level":risk_level,
  "side_effect_risk":side_effect_risk,"applies_to_mode":mode,
  "requires_manual_approval":true,"auto_apply_allowed":false,
  "real_launch_authorized":false
 });
 let digest_body=json!({"day":day,"recommendation":item});
 let bytes=serde_json::to_vec(&digest_body).expect("static JSON");
 let sha=openssl::sha::sha256(&bytes);
 let id=format!("adv-{}",sha[..10].iter()
   .map(|b|format!("{b:02x}")).collect::<String>());
 let mut out=item.as_object().unwrap().clone();
 out.insert("recommendation_id".into(),json!(id));
 Value::Object(out)
}

pub fn build_pending_advisor_offline(input:&Value)->Result<Value,AdvisorError>{
 if !input.is_object(){return Err(AdvisorError::InvalidInput)}
 if serde_json::to_vec(input).map_err(|_|AdvisorError::InvalidInput)?.len()>1_000_000{
  return Err(AdvisorError::TooLarge)
 }
 let summary=field(input,"summary").ok_or(AdvisorError::InvalidInput)?;
 if field(summary,"real_launch_authorized").and_then(Value::as_bool)!=Some(false){
  return Err(AdvisorError::InvalidInput)
 }
 let day=string_field(input,"date","UNKNOWN");
 let timezone=string_field(input,"timezone","UNKNOWN");
 if day.len()>20||timezone.len()>64||
  !day.is_ascii()||!timezone.is_ascii(){
  return Err(AdvisorError::InvalidInput)
 }
 let quality=field(input,"evidence_quality").unwrap_or(&Value::Null);
 let cohorts=field(input,"cohort_coverage").unwrap_or(&Value::Null);
 let storage=field(input,"storage_health").unwrap_or(&Value::Null);
 let mut blockers=BTreeSet::new();
 if let Some(items)=field(input,"not_enough_data_reasons").and_then(Value::as_array){
  if items.len()>256{return Err(AdvisorError::TooLarge)}
  for item in items{
   if let Some(reason)=item.as_str(){
    if reason.len()>128{return Err(AdvisorError::InvalidInput)}
    if !reason.is_empty(){blockers.insert(reason.to_string());}
   }
  }
 }
 let tuning=field(summary,"advisor_ready").and_then(Value::as_bool)==Some(true)
  &&field(summary,"real_pnl_available").and_then(Value::as_bool)==Some(true)
  &&field(summary,"evidence_incomplete").and_then(Value::as_bool)==Some(false)
  &&blockers.is_empty();
 let window=json!({"date":day,"timezone":timezone});
 let mut recs=Vec::new();
 let storage_status=string_field(storage,"status","UNKNOWN");
 if storage_status!="OK"{
  recs.push(pending(&day,"storage","analytics.storage_maintenance",
   json!({"status":storage_status,"used_ratio":field(storage,"used_ratio")}),
   json!("REVIEW_RETENTION_AND_CONTROLLED_MAINTENANCE"),
   "STORAGE_HEALTH_NOT_OK",
   "Storage health is not OK in the sanitized daily snapshot.",
   json!({"window":window,"refs":["advisor_input.storage_health"]}),
   "Reduce the risk of collector pressure or forced pruning.",
   "high","medium",
   "Maintenance can temporarily reduce write throughput.","all"));
 }
 let missing_fees=count(quality,"missing_fee_count");
 let missing_slip=count(quality,"missing_slippage_count");
 let missing_latency=count(quality,"missing_latency_count");
 let missing_total=missing_fees.saturating_add(missing_slip).saturating_add(missing_latency);
 if missing_total>0{
  recs.push(pending(&day,"evidence","evidence.execution_quality_fields",
   json!({"fees":missing_fees,"slippage":missing_slip,"latency":missing_latency}),
   json!({"fees":"required","slippage":"required","latency":"required"}),
   "EXECUTION_EVIDENCE_INCOMPLETE",
   "Execution quality fields are missing and must not be replaced with zero.",
   json!({"window":window,"missing_total":missing_total,"refs":["advisor_input.evidence_quality"]}),
   "Make cost and execution-quality analysis auditable.",
   if missing_total>=100{"high"}else{"medium"},"medium",
   "Stricter evidence rules can reduce the rankable sample.","all"));
 }
 let truth=json!({
  "unknown_entries":count(quality,"unknown_entries"),
  "unknown_exits":count(quality,"unknown_exits"),
  "manual_required":count(quality,"manual_required_count"),
  "contradicted":count(quality,"contradicted_count"),
  "partial_fills":count(quality,"partial_fill_count")
 });
 if truth.as_object().unwrap().values().any(|v|v.as_u64().unwrap_or(0)>0){
  recs.push(pending(&day,"risk","risk_engine.unknown_truth_policy",
   truth,json!("KEEP_DENY_AND_REQUIRE_RECONCILIATION"),
   "TRUTH_EVIDENCE_UNRESOLVED",
   "Unknown, contradicted, partial, or manual states require reconciliation.",
   json!({"window":window,"refs":["advisor_input.evidence_quality"]}),
   "Prevent unsupported real entries while truth is unresolved.",
   "high","high",
   "More candidates remain blocked until exchange truth is complete.","all"));
 }
 let unlinked=count(cohorts,"unlinked_trade_commits");
 if unlinked>0{
  recs.push(pending(&day,"cohort","analytics.comparison_id_required",
   json!({"unlinked_trade_commits":unlinked}),json!(true),
   "COHORT_LINKAGE_INCOMPLETE",
   "Trade commits without comparison identity cannot support Reality Gap.",
   json!({"window":window,"refs":["advisor_input.cohort_coverage"]}),
   "Enable like-for-like paper and verified-real comparison.",
   "high","medium",
   "Incomplete historical commits remain excluded.","all"));
 }
 if count(quality,"verified_real_exits")==0{
  recs.push(pending(&day,"real_guard","risk_engine.real_entry_policy",
   json!({"verified_real_exits":0}),
   json!("DENY_UNTIL_VERIFIED_REAL_EXIT_EVIDENCE"),
   "NO_VERIFIED_REAL_EXITS",
   "There are no exchange-verified real exits for advisor validation.",
   json!({"window":window,"refs":["advisor_input.evidence_quality","advisor_input.reality_gap"]}),
   "Keep paper results from being treated as real readiness.",
   "high","critical",
   "Real entries remain unavailable.","real"));
 }
 let empty=Vec::new();
 let pairs=field(input,"pair_stats").and_then(Value::as_array).unwrap_or(&empty);
 if pairs.len()>2_000{return Err(AdvisorError::TooLarge)}
 let mut unrankable=Vec::new();
 for item in pairs{
  let exits=count(item,"paper_exits");
  if exits<20||field(item,"rankable").and_then(Value::as_bool)==Some(true){continue}
  let symbol=string_field(item,"symbol","UNKNOWN");
  if symbol.is_empty()||symbol.len()>32||!symbol.bytes()
      .all(|b|b.is_ascii_uppercase()||b.is_ascii_digit()||b==b'_'||b==b'-'){
    return Err(AdvisorError::InvalidInput)
  }
  unrankable.push((exits,symbol,item));
 }
 unrankable.sort_by(|a,b|b.0.cmp(&a.0).then_with(||a.1.cmp(&b.1)));
 for (exits,symbol,item) in unrankable.into_iter().take(20){
  recs.push(pending(&day,"pair",&format!("real_candidates.{symbol}"),
   json!("NOT_EVIDENCE_READY"),json!(false),
   "PAIR_EVIDENCE_NOT_RANKABLE",
   "The pair has a paper sample but incomplete execution evidence.",
   json!({"window":window,"paper_exits":exits,
     "known_pnl_exits":count(item,"known_pnl_exits"),
     "unknown_pnl_exits":count(item,"unknown_pnl_exits"),
     "evidence_score":field(item,"evidence_score"),
     "refs":[format!("advisor_input.pair_stats.{symbol}")]}),
   "Keep an incompletely evidenced pair out of future real candidates.",
   "medium","high",
   "A potentially useful pair stays excluded pending better evidence.","real"));
 }
 let blockers:Vec<_>=blockers.into_iter().collect();
 Ok(json!({
  "advisor_version":"1.0","date":day,"timezone":timezone,
  "source_advisor_input_version":field(input,"advisor_input_version"),
  "policy":{"mode":"advisory","status":"pending_only",
   "requires_manual_approval":true,"auto_apply_allowed":false,
   "config_write_allowed":false,"order_submission_allowed":false,
   "real_launch_authorized":false},
  "strategy_tuning_allowed":tuning,
  "strategy_tuning_blockers":blockers,
  "recommendation_count":recs.len(),"recommendations":recs
 }))
}

#[cfg(test)]
mod tests{
 use super::*;
 #[test]
 fn fail_closed_no_real_launch_or_non_mapping(){
  assert_eq!(build_pending_advisor_offline(&Value::Null),
    Err(AdvisorError::InvalidInput));
  for auth in [Value::Null,json!(true),json!(1),json!("false")]{
   let i=json!({"summary":{"real_launch_authorized":auth}});
   assert_eq!(build_pending_advisor_offline(&i),Err(AdvisorError::InvalidInput));
  }
 }
 #[test]
 fn incomplete_facts_never_apply_or_authorize_trading(){
  let input=json!({
   "date":"2026-10-16","timezone":"Asia/Yekaterinburg",
   "advisor_input_version":"1.0",
   "summary":{"real_launch_authorized":false,"advisor_ready":false,
      "real_pnl_available":false,"evidence_incomplete":true},
   "storage_health":{"status":"HIGH","used_ratio":0.9},
   "evidence_quality":{"missing_fee_count":1,"missing_slippage_count":2,
     "missing_latency_count":3,"unknown_entries":1,"partial_fill_count":1,
     "verified_real_exits":0},
   "cohort_coverage":{"unlinked_trade_commits":1},
   "pair_stats":[{"symbol":"ZEC","paper_exits":21,"rankable":false}],
   "not_enough_data_reasons":["NO_VERIFIED_REAL","MISSING_FEES"]
  });
  let a=build_pending_advisor_offline(&input).unwrap();
  assert_eq!(a["recommendation_count"],6);
  assert_eq!(a["strategy_tuning_allowed"],false);
  assert_eq!(a["policy"]["auto_apply_allowed"],false);
  assert_eq!(a["policy"]["order_submission_allowed"],false);
  assert_eq!(a["policy"]["config_write_allowed"],false);
  let codes:Vec<_>=a["recommendations"].as_array().unwrap().iter()
   .map(|x|x["reason_code"].as_str().unwrap()).collect();
  assert_eq!(codes,vec![
   "STORAGE_HEALTH_NOT_OK","EXECUTION_EVIDENCE_INCOMPLETE",
   "TRUTH_EVIDENCE_UNRESOLVED","COHORT_LINKAGE_INCOMPLETE",
   "NO_VERIFIED_REAL_EXITS","PAIR_EVIDENCE_NOT_RANKABLE"]);
  for rec in a["recommendations"].as_array().unwrap(){
   assert_eq!(rec["status"],"pending");
   assert_eq!(rec["requires_manual_approval"],true);
   assert_eq!(rec["auto_apply_allowed"],false);
   assert_eq!(rec["real_launch_authorized"],false);
   assert_eq!(rec["recommendation_id"].as_str().unwrap().len(),24);
  }
  assert_eq!(a,build_pending_advisor_offline(&input).unwrap());
 }
 #[test]
 fn pair_selection_is_limited_and_tie_sorted(){
  let mut pairs=Vec::new();
  for i in 0..100{
   pairs.push(json!({"symbol":format!("X{i:03}"),
     "paper_exits":20,"rankable":false}));
  }
  let input=json!({"date":"2026-10-16","summary":{
      "real_launch_authorized":false},"storage_health":{"status":"OK"},
    "evidence_quality":{"verified_real_exits":1},"pair_stats":pairs});
  let out=build_pending_advisor_offline(&input).unwrap();
  assert_eq!(out["recommendation_count"],20);
  assert_eq!(out["recommendations"][0]["parameter"],"real_candidates.X000");
  assert_eq!(out["recommendations"][19]["parameter"],"real_candidates.X019");
 }
 #[test]
 fn sufficient_advisor_facts_are_only_an_advisory(){
  let input=json!({"date":"2026-10-16","summary":{
      "real_launch_authorized":false,"advisor_ready":true,
      "real_pnl_available":true,"evidence_incomplete":false},
    "storage_health":{"status":"OK"},
    "evidence_quality":{"verified_real_exits":1}});
  let out=build_pending_advisor_offline(&input).unwrap();
  assert_eq!(out["strategy_tuning_allowed"],true);
  assert_eq!(out["recommendation_count"],0);
  assert_eq!(out["policy"]["order_submission_allowed"],false);
  assert_eq!(out["policy"]["real_launch_authorized"],false);
  let mut blocked=input;
  blocked["not_enough_data_reasons"]=json!(["SOURCE_GAP"]);
  let out=build_pending_advisor_offline(&blocked).unwrap();
  assert_eq!(out["strategy_tuning_allowed"],false);
  assert_eq!(out["strategy_tuning_blockers"],json!(["SOURCE_GAP"]));
 }
 #[test]
 fn stable_python_reference_id_for_identical_sanitized_input(){
  // Python ai/advisor.py as deployed, generated on 2026-10-16.
  let input=json!({"date":"2026-10-16",
   "summary":{"real_launch_authorized":false},
   "storage_health":{"status":"OK"},
   "evidence_quality":{"verified_real_exits":0}});
  let out=build_pending_advisor_offline(&input).unwrap();
  assert_eq!(out["recommendation_count"],1);
  assert_eq!(out["recommendations"][0]["recommendation_id"],
    "adv-9761ee3166c4bc45082a");
 }
}
