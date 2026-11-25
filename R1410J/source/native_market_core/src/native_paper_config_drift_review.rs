//! R60: read-only comparison of legacy YAML vs observed PUBLIC PAPER limits.
//! Differences are explicit; never apply real-trading flags or reschedule PAPER.
use crate::{native_trading_config::NativeTradingConfig,
 native_paper_config_bridge::inspect_public_paper_policy_readonly};
use serde_json::{json,Value};

#[derive(Debug,Clone,PartialEq,Eq)]
pub enum DriftError{Untrusted,Missing,Config}
pub fn inspect_native_paper_config_drift(
 yaml:&NativeTradingConfig,status:&Value
)->Result<Value,DriftError>{
 if yaml.policy.real_execution_authorized||yaml.policy.real_daily_trades!=0||
  status.get("live_trading")!=Some(&json!(false))||
  status.get("private_api")!=Some(&json!(false)){
  return Err(DriftError::Untrusted)
 }
 let paper=status.get("paper_portfolio").ok_or(DriftError::Missing)?;
 if paper.get("no_real_or_private_api")!=Some(&json!(true)){
  return Err(DriftError::Untrusted)
 }
 let observed_cap=paper.get("max_daily_entries").and_then(Value::as_u64)
   .ok_or(DriftError::Missing)?;
 let observed_positions=paper.get("max_concurrent").and_then(Value::as_u64)
   .ok_or(DriftError::Missing)?;
 if observed_cap==0||observed_cap>100_000||observed_positions==0||observed_positions>100{
  return Err(DriftError::Untrusted)
 }
 let bound=inspect_public_paper_policy_readonly(yaml).map_err(|_|DriftError::Config)?;
 let legacy_enabled=yaml.raw.get("general")
  .and_then(|x|x.get("paper_entries_enabled"))
  .and_then(|x|x.as_bool()).ok_or(DriftError::Config)?;
 let mut mismatches=Vec::new();
 if yaml.policy.paper_daily_trades!=observed_cap{
  mismatches.push("PAPER_DAILY_CAP_LEGACY_VS_RESEARCH_OVERRIDE");
 }
 if yaml.policy.general_max_positions!=observed_positions{
  mismatches.push("PAPER_CONCURRENT_LIMIT");
 }
 if !legacy_enabled{
  mismatches.push("LEGACY_PAPER_ENTRIES_DISABLED");
 }
 // The supervised PAPER process inherits an env_clear() launch and does
 // not opt into R21's optional YAML DecisionConfig bridge.
 // Its compiled fallback has min price=0 and no blacklist, unlike YAML.
 mismatches.push("R21_DECISION_SOURCE_FALLBACK_NOT_YAML_BOUND");
 Ok(json!({
  "schema":"r60_paper_config_drift_readonly_v1",
  "legacy_paper_enabled":legacy_enabled,
  "legacy_yaml_paper_day_cap":yaml.policy.paper_daily_trades,
  "observed_research_paper_day_cap":observed_cap,
  "legacy_yaml_max_positions":yaml.policy.general_max_positions,
  "observed_research_max_positions":observed_positions,
  "yaml_entry_threshold_bps":bound.required_net,
  "yaml_total_cost_assumption_bps":bound.cost,
  "yaml_min_price_quote":bound.min_price,
  "yaml_max_price_quote":bound.max_price,
  "yaml_blacklist_count":bound.blacklist.len(),
  "policy_differences":mismatches,
  "fallback_assessment_scope":"R21_SOURCE_AND_SUPERVISOR_LAUNCH_REVIEW",
  "live_effective_decision_config_attested":false,
  "real_execution_authorized":false,
  "settings_mutated":false
 }))
}

/// P1-06: caller-collected READ-ONLY executable source facts. These
/// booleans are observations, not a signed running-binary attestation.
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct DeployedPaperConfigSourceReview{
 pub observed_paper_sha256:String,
 pub exact_running_executable_checked:bool,
 pub running_binary_has_yaml_opt_in_marker:bool,
 pub supervisor_child_env_cleared:bool,
 pub supervisor_explicitly_passes_yaml_opt_in:bool,
}
fn lowercase_sha(v:&str)->bool{
 v.len()==64&&v.bytes().all(|b|b.is_ascii_hexdigit()&&!b.is_ascii_uppercase())
}
/// Do not turn config schema parity or a staged DIFFERENT executable
/// into claims about applied runtime strategy and financial limits.
/// The current old PAPER status exposes 10/500, not the
/// later R65 applied_paper_decision_policy evidence object.
pub fn inspect_deployed_paper_config_source_review(
 status:&Value,source:&DeployedPaperConfigSourceReview,
)->Result<Value,DriftError>{
 if !source.exact_running_executable_checked||
    !lowercase_sha(&source.observed_paper_sha256)||
    !source.supervisor_child_env_cleared||
    status.get("live_trading")!=Some(&json!(false))||
    status.get("private_api")!=Some(&json!(false)){
   return Err(DriftError::Untrusted)
 }
 let paper=status.get("paper_portfolio").ok_or(DriftError::Missing)?;
 if paper.get("no_real_or_private_api")!=Some(&json!(true))||
    paper.get("paper_fill_attested")!=Some(&json!(false))||
    paper.get("real_profit_proven")!=Some(&json!(false)){
    return Err(DriftError::Untrusted)
 }
 let concurrent=paper.get("max_concurrent").and_then(Value::as_u64)
     .ok_or(DriftError::Missing)?;
 let daily=paper.get("max_daily_entries").and_then(Value::as_u64)
     .ok_or(DriftError::Missing)?;
 if concurrent==0||concurrent>100||daily==0||daily>100_000{
   return Err(DriftError::Untrusted)
 }
 let reported=status.get("applied_paper_decision_policy");
 if reported.is_some()&&!source.running_binary_has_yaml_opt_in_marker{
   return Err(DriftError::Untrusted)
 }
 if source.supervisor_explicitly_passes_yaml_opt_in&&
    (!source.running_binary_has_yaml_opt_in_marker||reported.is_none()){
   return Err(DriftError::Untrusted)
 }
 if let Some(report)=reported{
   let expected=if source.supervisor_explicitly_passes_yaml_opt_in{
     "RUST_YAML_REVIEW_OPT_IN"
   }else{"COMPILED_FALLBACK"};
   if report.get("source")!=Some(&json!(expected))||
      report.get("real_order_authority")!=Some(&json!(false))||
      report.get("output_is_runtime_observation")!=Some(&json!(true))||
      report.get("decision_and_model_parity_at_start")==Some(&json!(false)){
     return Err(DriftError::Untrusted)
   }
 }
 let counters=paper.get("counters").ok_or(DriftError::Missing)?;
 let entries=counters.get("entries").and_then(Value::as_u64)
    .ok_or(DriftError::Missing)?;
 if entries>daily{return Err(DriftError::Untrusted)}
 Ok(json!({
  "schema":"p106_deployed_paper_source_provenance_review_v1",
  "scope":"CALLER_OBSERVED_LOCAL_FILES_NOT_REMOTE_OR_ATTESTED",
  "observed_running_paper_sha256":source.observed_paper_sha256,
  "reported_public_paper_max_concurrent":concurrent,
  "reported_public_paper_max_daily_entries":daily,
  "reported_modeled_entries":entries,
  "rust_paper_compiled_10_500_parity":
     concurrent==crate::native_paper_portfolio_review::MAX_CONCURRENT_PAPER as u64
     &&daily==crate::native_paper_portfolio_review::MAX_DAILY_PAPER as u64,
  "supervisor_env_clear_observed":source.supervisor_child_env_cleared,
  "supervisor_yaml_opt_in_passed":source.supervisor_explicitly_passes_yaml_opt_in,
  "runtime_binary_yaml_opt_in_marker_present":
       source.running_binary_has_yaml_opt_in_marker,
  "runtime_applied_config_report_present":reported.is_some(),
  "runtime_applied_policy_field":"applied_paper_decision_policy",
  "effective_yaml_binding_proven":false,
  "effective_entry_threshold_42bps_proven":false,
  "effective_legacy_fee_cost_proven":false,
  "effective_legacy_blacklist_proven":false,
  "real_execution_proven":false,
  "paper_fill_attested":false,
  "state_mutated":false
 }))
}
#[cfg(test)]
mod tests {
 use super::*;
 fn config()->NativeTradingConfig{
  NativeTradingConfig::parse(include_str!("../config/native_trading.yaml")).unwrap()
 }
 fn status()->Value{
  json!({"live_trading":false,"private_api":false,"paper_portfolio":{
   "no_real_or_private_api":true,"max_daily_entries":500,"max_concurrent":10
  }})
 }
 fn deployed_source()->DeployedPaperConfigSourceReview{
  DeployedPaperConfigSourceReview{
   observed_paper_sha256:"a".repeat(64),
   exact_running_executable_checked:true,
   running_binary_has_yaml_opt_in_marker:false,
   supervisor_child_env_cleared:true,
   supervisor_explicitly_passes_yaml_opt_in:false,
  }
 }
 fn deployed_status()->Value{
  let mut s=status();
  s["paper_portfolio"]["paper_fill_attested"]=json!(false);
  s["paper_portfolio"]["real_profit_proven"]=json!(false);
  s["paper_portfolio"]["counters"]=json!({"entries":10,"closed":8});
  s
 }
 #[test]
 fn p106_deployed_old_binary_report_cannot_claim_yaml_fee_or_42bps(){
  let review=inspect_deployed_paper_config_source_review(
    &deployed_status(),&deployed_source()).unwrap();
  assert_eq!(review["reported_public_paper_max_concurrent"],10);
  assert_eq!(review["reported_public_paper_max_daily_entries"],500);
  assert_eq!(review["reported_modeled_entries"],10);
  assert_eq!(review["rust_paper_compiled_10_500_parity"],true);
  for key in ["effective_yaml_binding_proven",
   "effective_entry_threshold_42bps_proven",
   "effective_legacy_fee_cost_proven",
   "effective_legacy_blacklist_proven","real_execution_proven",
   "paper_fill_attested","state_mutated"]{
   assert_eq!(review[key],false,"{key} must remain false");
  }
  assert_eq!(review["runtime_applied_config_report_present"],false);
 }
 #[test]
 fn p106_deployed_binary_missing_report_and_spoof_denied(){
  let mut status=deployed_status();
  let mut source=deployed_source();
  status["applied_paper_decision_policy"]=json!({"source":"RUST_YAML_REVIEW_OPT_IN"});
  assert_eq!(inspect_deployed_paper_config_source_review(&status,&source),
    Err(DriftError::Untrusted));
  status=deployed_status();
  source.supervisor_explicitly_passes_yaml_opt_in=true;
  assert_eq!(inspect_deployed_paper_config_source_review(&status,&source),
    Err(DriftError::Untrusted));
  source.supervisor_explicitly_passes_yaml_opt_in=false;
  source.exact_running_executable_checked=false;
  assert_eq!(inspect_deployed_paper_config_source_review(&status,&source),
    Err(DriftError::Untrusted));
  source=deployed_source();
  status["paper_portfolio"]["counters"]["entries"]=json!(501);
  assert_eq!(inspect_deployed_paper_config_source_review(&status,&source),
    Err(DriftError::Untrusted));
  status=deployed_status();
  status["private_api"]=json!(true);
  assert_eq!(inspect_deployed_paper_config_source_review(&status,&source),
    Err(DriftError::Untrusted));
 }
 #[test]
 fn p106_r65_policy_report_real_key_and_split_brain_spoof_denied(){
  let mut s=deployed_status();
  let mut source=deployed_source();
  source.running_binary_has_yaml_opt_in_marker=true;
  s["applied_paper_decision_policy"]=json!({
    "source":"COMPILED_FALLBACK","real_order_authority":false,
    "output_is_runtime_observation":true,
    "decision_and_model_parity_at_start":true,
  });
  let report=inspect_deployed_paper_config_source_review(&s,&source).unwrap();
  assert_eq!(report["runtime_applied_config_report_present"],true);
  assert_eq!(report["effective_yaml_binding_proven"],false);
  source.supervisor_explicitly_passes_yaml_opt_in=true;
  assert_eq!(inspect_deployed_paper_config_source_review(&s,&source),
    Err(DriftError::Untrusted),"YAML optin cannot claim compiled fallback");
  s["applied_paper_decision_policy"]["source"]=json!("RUST_YAML_REVIEW_OPT_IN");
  assert!(inspect_deployed_paper_config_source_review(&s,&source).is_ok());
  s["applied_paper_decision_policy"]["decision_and_model_parity_at_start"]=json!(false);
  assert_eq!(inspect_deployed_paper_config_source_review(&s,&source),
    Err(DriftError::Untrusted));
  s["applied_paper_decision_policy"]["decision_and_model_parity_at_start"]=json!(true);
  s["applied_paper_decision_policy"]["real_order_authority"]=json!(true);
  assert_eq!(inspect_deployed_paper_config_source_review(&s,&source),
    Err(DriftError::Untrusted));
 }
 #[test]
 fn explicit_legacy_vs_research_drift_is_not_hidden(){
  let r=inspect_native_paper_config_drift(&config(),&status()).unwrap();
  assert_eq!(r["legacy_yaml_paper_day_cap"],3000);
  assert_eq!(r["observed_research_paper_day_cap"],500);
  assert_eq!(r["legacy_paper_enabled"],false);
  assert_eq!(r["yaml_entry_threshold_bps"],42.0);
  assert_eq!(r["yaml_total_cost_assumption_bps"],24.03);
  assert_eq!(r["yaml_min_price_quote"],0.03);
  assert_eq!(r["settings_mutated"],false);
  assert_eq!(r["live_effective_decision_config_attested"],false);
  assert!(r["policy_differences"].as_array().unwrap().len()>=3);
 }
 #[test]
 fn migrated_legacy_switches_match_operator_python_baseline(){
  let c=NativeTradingConfig::parse(
   include_str!("../config/native_trading_operator_baseline_r64.yaml")).unwrap();
  let g=&c.raw["general"];
  let r=&c.raw["real_trial"];
  assert_eq!(g["trade_enabled"].as_bool(),Some(false));
  assert_eq!(g["paper_entries_enabled"].as_bool(),Some(true));
  assert_eq!(r["enabled"].as_bool(),Some(false));
  assert_eq!(r["autonomous_real_enabled"].as_bool(),Some(false));
  assert_eq!(c.policy.paper_entries_enabled,true);
  assert_eq!(c.policy.real_execution_authorized,false);
  assert_eq!(c.policy.real_daily_trades,0);
  assert_eq!(c.raw["native_runtime"]["mutation_capability_enabled"].as_bool(),Some(false));
  assert_eq!(c.raw["native_runtime"]["production_cutover_authorized"].as_bool(),Some(false));
 }
 #[test]
 fn real_auth_or_tampered_source_always_denied(){
  let mut c=config();
  c.policy.real_execution_authorized=true;
  assert_eq!(inspect_native_paper_config_drift(&c,&status()),Err(DriftError::Untrusted));
  let mut s=status();
  s["live_trading"]=json!(true);
  assert_eq!(inspect_native_paper_config_drift(&config(),&s),Err(DriftError::Untrusted));
  let mut s=status();
  s["paper_portfolio"]["no_real_or_private_api"]=json!(false);
  assert_eq!(inspect_native_paper_config_drift(&config(),&s),Err(DriftError::Untrusted));
  let mut s=status();
  s["paper_portfolio"]["max_daily_entries"]=json!(0);
  assert_eq!(inspect_native_paper_config_drift(&config(),&s),Err(DriftError::Untrusted));
 }
 #[test]
 fn matching_caps_do_not_hide_legacy_mode_and_decision_drift(){
  let mut s=status();
  s["paper_portfolio"]["max_daily_entries"]=json!(3000);
  let r=inspect_native_paper_config_drift(&config(),&s).unwrap();
  let list=r["policy_differences"].as_array().unwrap();
  assert!(!list.iter().any(|v|v=="PAPER_DAILY_CAP_LEGACY_VS_RESEARCH_OVERRIDE"));
  assert!(list.iter().any(|v|v=="R21_DECISION_SOURCE_FALLBACK_NOT_YAML_BOUND"));
  assert!(list.iter().any(|v|v=="LEGACY_PAPER_ENTRIES_DISABLED"));
 }
}
