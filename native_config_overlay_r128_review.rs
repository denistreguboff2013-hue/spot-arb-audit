//! Astra R128: integrity overlay of previous research config evidence.
//! Source=160 legacy/native fields; base ledger=R113; overlay=R125.
//! 26 independent research claims, zero verified active-binary consumers.
use serde_json::Value;
use std::collections::BTreeSet;
use crate::native_field_consumer_matrix_r118_review::audit_field_sources;
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct ConfigOverlayR128{
 pub total:usize,pub original_research:usize,pub public_feed_research:usize,
 pub unique_research:usize,pub not_evidenced:usize,pub active_binary_proven:usize,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum OverlayError{Json,Base,Overlay,Duplicate,Claim}
pub fn validate_overlay(
 schema:&str,status:&str,base:&str,overlay:&str
)->Result<ConfigOverlayR128,OverlayError>{
 let base_evidence=audit_field_sources(schema,status,base).map_err(|_|OverlayError::Base)?;
 let schema:Value=serde_json::from_str(schema).map_err(|_|OverlayError::Json)?;
 let original:Value=serde_json::from_str(base).map_err(|_|OverlayError::Json)?;
 let overlay:Value=serde_json::from_str(overlay).map_err(|_|OverlayError::Json)?;
 if overlay["schema"]!="R125_RESEARCH_READONLY_CONSUMER_OVERLAY_V1"
  ||overlay["consumer"]!="PublicFeedPolicy::from_config"
  ||overlay["source_module"]!="native_public_feed_policy_review.rs"
  ||overlay["test"]!="binds_seventeen_original_market_data_fields_without_execution"
  ||overlay["active_binary_proven"]!=false
  ||overlay["scope"]!="rust_research_readonly_only"{
  return Err(OverlayError::Claim)
 }
 let fields=overlay["fields"].as_array().ok_or(OverlayError::Overlay)?;
 if fields.len()!=17{return Err(OverlayError::Overlay)}
 let base_entries=original["entries"].as_array().ok_or(OverlayError::Base)?;
 let mut declared=BTreeSet::<String>::new();
 for e in base_entries{
  let field=e["field"].as_str().ok_or(OverlayError::Base)?;
  if e["consumer"].is_string(){
   if !declared.insert(field.into()){return Err(OverlayError::Duplicate)}
  }
 }
 if declared.len()!=base_evidence.declared_consumer{return Err(OverlayError::Base)}
 for f in fields{
  let key=f.as_str().ok_or(OverlayError::Overlay)?;
  if schema.get(key).is_none(){return Err(OverlayError::Overlay)}
  if !declared.insert(key.into()){return Err(OverlayError::Duplicate)}
  if !base_entries.iter().any(|e|e["field"]==key&&e["active_binary_proven"]==false){
   return Err(OverlayError::Claim)
  }
 }
 if declared.len()!=26||base_evidence.fields!=160{
  return Err(OverlayError::Overlay)
 }
 Ok(ConfigOverlayR128{
  total:160,original_research:base_evidence.declared_consumer,
  public_feed_research:fields.len(),unique_research:declared.len(),
  not_evidenced:160-declared.len(),active_binary_proven:0,
 })
}
pub fn embedded_overlay()->Result<ConfigOverlayR128,OverlayError>{
 validate_overlay(
  include_str!("../config/native_field_schema.json"),
  include_str!("../config/native_field_status.json"),
  include_str!("../config/native_field_evidence_r113_review.json"),
  include_str!("../config/native_field_evidence_r125_public_feed_review.json"))
}
/// R130 only adds source-linked Rust RESEARCH decision consumers.
/// Existing R118/R125 ledgers and their original 160 status rows are
/// intentionally preserved; not a live executable deployment claim.
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct ConfigOverlayR130{
 pub total:usize,pub previous_research:usize,pub added_research:usize,
 pub distinct_research:usize,pub not_evidenced:usize,
 pub active_binary_proven:usize,
}
pub fn validate_paper_decision_overlay_r130(
 schema:&str,status:&str,base:&str,feed:&str,review:&str
)->Result<ConfigOverlayR130,OverlayError>{
 let previous=validate_overlay(schema,status,base,feed)?;
 let known:Value=serde_json::from_str(schema).map_err(|_|OverlayError::Json)?;
 let old:Value=serde_json::from_str(base).map_err(|_|OverlayError::Json)?;
 let feed:Value=serde_json::from_str(feed).map_err(|_|OverlayError::Json)?;
 let review:Value=serde_json::from_str(review).map_err(|_|OverlayError::Json)?;
 if review["schema"]!="R130_PAPER_DECISION_CONFIG_RESEARCH_OVERLAY_V1"
   ||review["scope"]!="rust_research_readonly_only"
   ||review["consumer"]!="bind_public_paper_decision_config"
   ||review["source_module"]!="native_paper_config_bridge.rs"
   ||review["test"]!="r130_decision_config_seven_fields_mutation_consumed"
   ||review["active_binary_proven"]!=false
   ||review["real_order_authority"]!=false{
    return Err(OverlayError::Claim)
 }
 let expected=[
   "general.paper_entries_enabled","risk.futures_order_usdt",
   "spread.entry_bps","spread.round_fees_bps","spread.slippage_bps",
   "spread.funding_bps","spread.buffer_bps",
 ].into_iter().collect::<BTreeSet<_>>();
 let fields=review["fields"].as_array().ok_or(OverlayError::Overlay)?;
 if fields.len()!=7{return Err(OverlayError::Overlay)}
 let actual=fields.iter().map(|x|x.as_str().ok_or(OverlayError::Overlay))
    .collect::<Result<BTreeSet<_>,_>>()?;
 if actual!=expected{return Err(OverlayError::Overlay)}
 let original=old["entries"].as_array().ok_or(OverlayError::Base)?;
 let mut older=original.iter().filter(|x|x["consumer"].is_string())
    .filter_map(|x|x["field"].as_str()).collect::<BTreeSet<_>>();
 for path in feed["fields"].as_array().ok_or(OverlayError::Overlay)?{
   older.insert(path.as_str().ok_or(OverlayError::Overlay)?);
 }
 if older.len()!=previous.unique_research{return Err(OverlayError::Base)}
 for path in &actual{
   if !known.get(path).is_some_and(Value::is_string)
     ||older.contains(path)
     ||!original.iter().any(|x|x["field"]==*path
       &&x["active_binary_proven"]==false){
     return Err(OverlayError::Claim)
   }
 }
 Ok(ConfigOverlayR130{
   total:previous.total,previous_research:previous.unique_research,
   added_research:actual.len(),
   distinct_research:previous.unique_research+actual.len(),
   not_evidenced:previous.not_evidenced-actual.len(),
   active_binary_proven:0,
 })
}
pub fn embedded_overlay_r130()->Result<ConfigOverlayR130,OverlayError>{
 validate_paper_decision_overlay_r130(
  include_str!("../config/native_field_schema.json"),
  include_str!("../config/native_field_status.json"),
  include_str!("../config/native_field_evidence_r113_review.json"),
  include_str!("../config/native_field_evidence_r125_public_feed_review.json"),
  include_str!("../config/native_field_evidence_r130_paper_decision_review.json"))
}
/// Two NEW temporal consumer fields, chained to exact 33-field
/// R130 research ledger; NEVER promotes claims to deployed binary coverage.
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct ConfigOverlayTemporal{
 pub total:usize,pub previous_research:usize,pub added_research:usize,
 pub distinct_research:usize,pub not_evidenced:usize,pub active_binary_proven:usize,
}
pub fn validate_temporal_overlay(
 schema:&str,status:&str,base:&str,feed:&str,decision:&str,temporal:&str
)->Result<ConfigOverlayTemporal,OverlayError>{
 let prior=validate_paper_decision_overlay_r130(schema,status,base,feed,decision)?;
 let known:Value=serde_json::from_str(schema).map_err(|_|OverlayError::Json)?;
 let base_v:Value=serde_json::from_str(base).map_err(|_|OverlayError::Json)?;
 let feed_v:Value=serde_json::from_str(feed).map_err(|_|OverlayError::Json)?;
 let decision_v:Value=serde_json::from_str(decision).map_err(|_|OverlayError::Json)?;
 let review:Value=serde_json::from_str(temporal).map_err(|_|OverlayError::Json)?;
 if review["schema"]!="R132_PAPER_TEMPORAL_CONFIG_RESEARCH_OVERLAY_V1"
  ||review["scope"]!="rust_research_optional_paper_only"
  ||review["consumer"]!="bind_public_paper_temporal_policy_review + PaperPortfolio::on_frame"
  ||review["source_module"]!="native_paper_config_bridge.rs + native_paper_portfolio_review.rs"
  ||review["test"]!="p106_temporal_yaml_changes_reach_real_research_paper_consumer"
  ||review["active_binary_proven"]!=false||review["real_order_authority"]!=false{
   return Err(OverlayError::Claim)
 }
 let actual=review["fields"].as_array().ok_or(OverlayError::Overlay)?;
 if actual.len()!=2{return Err(OverlayError::Overlay)}
 let expected=["spread.exit_bps","spread.min_lifetime_sec"]
   .into_iter().collect::<BTreeSet<_>>();
 let fields=actual.iter().map(|x|x.as_str().ok_or(OverlayError::Overlay))
  .collect::<Result<BTreeSet<_>,_>>()?;
 if fields!=expected{return Err(OverlayError::Overlay)}
 let mut old=base_v["entries"].as_array().ok_or(OverlayError::Base)?
   .iter().filter(|e|e["consumer"].is_string())
   .filter_map(|e|e["field"].as_str()).collect::<BTreeSet<_>>();
 for overlay in [&feed_v,&decision_v]{
  for x in overlay["fields"].as_array().ok_or(OverlayError::Overlay)?{
   old.insert(x.as_str().ok_or(OverlayError::Overlay)?);
  }
 }
 // R112 already claimed the cooldown YAML field; do not count it twice.
 if old.len()!=prior.distinct_research||
    !old.contains("cooldown.symbol_after_close_sec"){
    return Err(OverlayError::Base)
 }
 for field in fields{
  if !known.get(field).is_some_and(Value::is_string)
    ||old.contains(field)
    ||!base_v["entries"].as_array().unwrap().iter()
      .any(|e|e["field"]==field&&e["active_binary_proven"]==false){
    return Err(OverlayError::Claim)
  }
 }
 Ok(ConfigOverlayTemporal{
   total:prior.total,previous_research:prior.distinct_research,
   added_research:2,distinct_research:prior.distinct_research+2,
   not_evidenced:prior.not_evidenced-2,active_binary_proven:0,
 })
}
pub fn embedded_temporal_overlay()->Result<ConfigOverlayTemporal,OverlayError>{
 validate_temporal_overlay(
  include_str!("../config/native_field_schema.json"),
  include_str!("../config/native_field_status.json"),
  include_str!("../config/native_field_evidence_r113_review.json"),
  include_str!("../config/native_field_evidence_r125_public_feed_review.json"),
  include_str!("../config/native_field_evidence_r130_paper_decision_review.json"),
  include_str!("../config/native_field_evidence_r132_paper_temporal_review.json"))
}
/// New R133 claim is only research PAPER direct exit-target consumption,
/// not an assertion that any running production executable reads YAML.
pub fn validate_exit_target_overlay_r133(
 schema:&str,base:&str,feed:&str,decision:&str,temporal:&str,review:&str
)->Result<usize,OverlayError>{
 let known:Value=serde_json::from_str(schema).map_err(|_|OverlayError::Json)?;
 let old:Value=serde_json::from_str(base).map_err(|_|OverlayError::Json)?;
 let prev=[feed,decision,temporal].into_iter()
   .map(|raw|serde_json::from_str::<Value>(raw).map_err(|_|OverlayError::Json))
   .collect::<Result<Vec<_>,_>>()?;
 let report:Value=serde_json::from_str(review).map_err(|_|OverlayError::Json)?;
 if report["schema"]!="R133_PAPER_EXIT_TARGET_RESEARCH_OVERLAY_V1"
   ||report["scope"]!="rust_research_optional_paper_only"
   ||report["consumer"]!="PaperPortfolio::on_frame"
   ||report["source_module"]!="native_paper_config_bridge.rs + native_paper_portfolio_review.rs"
   ||report["test"]!="p106_model_exit_target_consumes_real_portfolio_value"
   ||report["active_binary_proven"]!=false||report["real_order_authority"]!=false{
  return Err(OverlayError::Claim)
 }
 let fields=report["fields"].as_array().ok_or(OverlayError::Overlay)?;
 if fields.len()!=1||fields[0]!="spread.net_target_bps"{
  return Err(OverlayError::Overlay)
 }
 let field="spread.net_target_bps";
 if known[field]!="number"
   ||!old["entries"].as_array().ok_or(OverlayError::Base)?
     .iter().any(|x|x["field"]==field&&x["consumer"].is_null()
                          &&x["active_binary_proven"]==false)
   ||prev.iter().any(|x|x["fields"].as_array().is_some_and(
     |a|a.iter().any(|v|v==field))){
   return Err(OverlayError::Claim)
 }
 Ok(36)
}
pub fn embedded_exit_target_overlay_r133()->Result<usize,OverlayError>{
 let before=embedded_temporal_overlay()?;
 if before.distinct_research!=35||before.active_binary_proven!=0{
  return Err(OverlayError::Base)
 }
 validate_exit_target_overlay_r133(
  include_str!("../config/native_field_schema.json"),
  include_str!("../config/native_field_evidence_r113_review.json"),
  include_str!("../config/native_field_evidence_r125_public_feed_review.json"),
  include_str!("../config/native_field_evidence_r130_paper_decision_review.json"),
  include_str!("../config/native_field_evidence_r132_paper_temporal_review.json"),
  include_str!("../config/native_field_evidence_r133_paper_exit_target_review.json"))
}
#[cfg(test)]
mod tests{
 use super::*;
 fn src()->(&'static str,&'static str,&'static str,&'static str){
  (include_str!("../config/native_field_schema.json"),
   include_str!("../config/native_field_status.json"),
   include_str!("../config/native_field_evidence_r113_review.json"),
   include_str!("../config/native_field_evidence_r125_public_feed_review.json"))
 }
 #[test]fn p106_r133_target_is_36th_unique_research_not_deployed(){
  assert_eq!(embedded_exit_target_overlay_r133(),Ok(36));
  assert_eq!(embedded_temporal_overlay().unwrap().distinct_research,35);
 }
 #[test]fn p106_r133_rejects_forged_deployment_or_duplicate(){
  let s=include_str!("../config/native_field_schema.json");
  let a=include_str!("../config/native_field_evidence_r113_review.json");
  let b=include_str!("../config/native_field_evidence_r125_public_feed_review.json");
  let c=include_str!("../config/native_field_evidence_r130_paper_decision_review.json");
  let d=include_str!("../config/native_field_evidence_r132_paper_temporal_review.json");
  let e=include_str!("../config/native_field_evidence_r133_paper_exit_target_review.json");
  let mut v:Value=serde_json::from_str(e).unwrap();
  v["active_binary_proven"]=Value::Bool(true);
  assert_eq!(validate_exit_target_overlay_r133(s,a,b,c,d,&v.to_string()),
    Err(OverlayError::Claim));
  let mut v:Value=serde_json::from_str(e).unwrap();
  v["fields"][0]=Value::String("cooldown.symbol_after_close_sec".into());
  assert_eq!(validate_exit_target_overlay_r133(s,a,b,c,d,&v.to_string()),
    Err(OverlayError::Overlay));
 }
 #[test]fn p106_temporal_overlay_adds_two_unique_review_consumers(){
  assert_eq!(embedded_temporal_overlay().unwrap(),ConfigOverlayTemporal{
    total:160,previous_research:33,added_research:2,
    distinct_research:35,not_evidenced:125,active_binary_proven:0,
  });
 }
 #[test]fn p106_temporal_overlay_rejects_forged_live_or_duplicate_evidence(){
  let (a,b,c,d)=src();
  let decision=include_str!("../config/native_field_evidence_r130_paper_decision_review.json");
  let temporal=include_str!("../config/native_field_evidence_r132_paper_temporal_review.json");
  let mut f:Value=serde_json::from_str(temporal).unwrap();
  f["active_binary_proven"]=Value::Bool(true);
  assert_eq!(validate_temporal_overlay(a,b,c,d,decision,&f.to_string()),
    Err(OverlayError::Claim));
  let mut f:Value=serde_json::from_str(temporal).unwrap();
  f["fields"][0]=f["fields"][1].clone();
  assert_eq!(validate_temporal_overlay(a,b,c,d,decision,&f.to_string()),
    Err(OverlayError::Overlay));
  let mut f:Value=serde_json::from_str(temporal).unwrap();
  f["fields"][0]=Value::String("cooldown.symbol_after_close_sec".into());
  assert_eq!(validate_temporal_overlay(a,b,c,d,decision,&f.to_string()),
    Err(OverlayError::Overlay));
  let mut f:Value=serde_json::from_str(temporal).unwrap();
  f["source_module"]=Value::String("deployed-paper-binary".into());
  assert_eq!(validate_temporal_overlay(a,b,c,d,decision,&f.to_string()),
    Err(OverlayError::Claim));
 }
 #[test]fn r130_overlay_preserves_prior_ledger_and_adds_seven_research_only(){
  let result=embedded_overlay_r130().unwrap();
  assert_eq!(result,ConfigOverlayR130{
    total:160,previous_research:26,added_research:7,
    distinct_research:33,not_evidenced:127,active_binary_proven:0
  });
  assert_eq!(embedded_overlay().unwrap().not_evidenced,134,
    "historical ledger remains unchanged");
 }
 #[test]fn r130_fake_live_consumer_missing_or_overlapping_field_denied(){
  let (a,b,c,d)=src();
  let review=include_str!("../config/native_field_evidence_r130_paper_decision_review.json");
  let mut v:Value=serde_json::from_str(review).unwrap();
  v["active_binary_proven"]=Value::Bool(true);
  assert_eq!(validate_paper_decision_overlay_r130(
    a,b,c,d,&v.to_string()),Err(OverlayError::Claim));
  let mut v:Value=serde_json::from_str(review).unwrap();
  v["fields"][0]=Value::String("blacklist".into());
  assert_eq!(validate_paper_decision_overlay_r130(
    a,b,c,d,&v.to_string()),Err(OverlayError::Overlay));
  let mut v:Value=serde_json::from_str(review).unwrap();
  v["fields"][0]=v["fields"][1].clone();
  assert_eq!(validate_paper_decision_overlay_r130(
    a,b,c,d,&v.to_string()),Err(OverlayError::Overlay));
  let mut v:Value=serde_json::from_str(review).unwrap();
  v["source_module"]=Value::String("fake_live_binary".into());
  assert_eq!(validate_paper_decision_overlay_r130(
    a,b,c,d,&v.to_string()),Err(OverlayError::Claim));
 }
 #[test]fn r130_decision_config_seven_fields_mutation_consumed(){
   use crate::{native_trading_config::NativeTradingConfig,
     native_paper_config_bridge::bind_public_paper_decision_config};
   let yaml=include_str!("../config/native_trading_operator_baseline_r64.yaml");
   let default=NativeTradingConfig::parse(yaml).unwrap();
   let baseline=bind_public_paper_decision_config(&default).unwrap();
   assert_eq!(baseline.base_size,10.0);
   assert_eq!(baseline.required_net,42.0);
   assert!((baseline.cost-24.03).abs()<1e-9);
   for (needle,replace,kind,value) in [
      ("futures_order_usdt: 10","futures_order_usdt: 9","size",9.0),
      ("entry_bps: 42.0","entry_bps: 43.0","threshold",43.0),
      ("round_fees_bps: 24.0","round_fees_bps: 25.0","cost",25.03),
      ("slippage_bps: 0.0","slippage_bps: 1.0","cost",25.03),
      ("funding_bps: 0.03","funding_bps: 0.04","cost",24.04),
      ("buffer_bps: 0.0","buffer_bps: 1.0","cost",25.03),
   ]{
      let text=yaml.replacen(needle,replace,1);
      assert_ne!(text,yaml,"missing source token {needle}");
      let cfg=NativeTradingConfig::parse(&text).unwrap();
      let d=bind_public_paper_decision_config(&cfg).unwrap();
      let actual=match kind{
        "size"=>d.base_size,"threshold"=>d.required_net,
        _=>d.cost,
      };
      assert!((actual-value).abs()<1e-8,
        "research consumer did not use modified {needle}");
   }
   let disabled=yaml.replacen(
      "paper_entries_enabled: true","paper_entries_enabled: false",1);
   assert_ne!(disabled,yaml);
   let cfg=NativeTradingConfig::parse(&disabled).unwrap();
   assert!(bind_public_paper_decision_config(&cfg).is_err(),
     "general.paper_entries_enabled is a real research admission gate");
 }
 #[test]fn r128_count_160_has_26_distinct_research_readers_not_active(){
  let a=embedded_overlay().unwrap();
  assert_eq!(a,ConfigOverlayR128{
   total:160,original_research:9,public_feed_research:17,
   unique_research:26,not_evidenced:134,active_binary_proven:0});
 }
 #[test]fn r128_tamper_duplicated_field_and_overlap_denied(){
  let (s,t,b,o)=src();
  let mut v:Value=serde_json::from_str(o).unwrap();
  v["fields"][1]=v["fields"][0].clone();
  assert_eq!(validate_overlay(s,t,b,&v.to_string()).unwrap_err(),OverlayError::Duplicate);
  let mut v:Value=serde_json::from_str(o).unwrap();
  v["fields"][0]=Value::String("blacklist".into());
  assert_eq!(validate_overlay(s,t,b,&v.to_string()).unwrap_err(),OverlayError::Duplicate);
 }
 #[test]fn r128_anti_claim_and_unknown_field_denied(){
  let (s,t,b,o)=src();
  let mut v:Value=serde_json::from_str(o).unwrap();
  v["active_binary_proven"]=Value::Bool(true);
  assert_eq!(validate_overlay(s,t,b,&v.to_string()).unwrap_err(),OverlayError::Claim);
  let mut v:Value=serde_json::from_str(o).unwrap();
  v["fields"][0]=Value::String("websocket.i_am_not_real".into());
  assert_eq!(validate_overlay(s,t,b,&v.to_string()).unwrap_err(),OverlayError::Overlay);
  let mut v:Value=serde_json::from_str(o).unwrap();
  v["source_module"]=Value::String("fabricated_live.rs".into());
  assert_eq!(validate_overlay(s,t,b,&v.to_string()).unwrap_err(),OverlayError::Claim);
 }
}
