//! P1-06: strict source-YAML -> existing Rust PUBLIC-PAPER decision policy.
//! Opt-in review only; does NOT modify the running PAPER service or grant
//! any private API, real execution, or WebSocket subscription capability.
use crate::{decision::DecisionConfig,native_trading_config::NativeTradingConfig};
use std::collections::HashSet;
const ONE:f64=1_000_000_000_000_000_000.0;
#[derive(Debug,Clone,PartialEq,Eq)]
pub enum BridgeError{Policy,Unsupported}
/// YAML -> PAPER exit timing, target and cooldown; no REAL authority.
#[derive(Debug,Clone,Copy,PartialEq)]
pub struct PaperTemporalPolicyReview{
 pub min_lifetime_ns:u64,pub exit_spread_bps:f64,pub reentry_cooldown_ns:u64,
 pub net_target_bps:f64,
}
pub fn bind_public_paper_temporal_policy_review(
 config:&NativeTradingConfig
)->Result<PaperTemporalPolicyReview,BridgeError>{
 if !config.source_projection_unchanged(){return Err(BridgeError::Policy)}
 let spread=config.raw.get("spread").ok_or(BridgeError::Unsupported)?;
 let cooldown=config.raw.get("cooldown").ok_or(BridgeError::Unsupported)?;
 let min_sec=spread.get("min_lifetime_sec").and_then(|v|v.as_u64())
   .ok_or(BridgeError::Unsupported)?;
 let exit=spread.get("exit_bps").and_then(|v|v.as_f64())
   .ok_or(BridgeError::Unsupported)?;
 let target=spread.get("net_target_bps").and_then(|v|v.as_f64())
   .ok_or(BridgeError::Unsupported)?;
 let reentry=cooldown.get("symbol_after_close_sec").and_then(|v|v.as_u64())
   .ok_or(BridgeError::Unsupported)?;
 if !(1..=21_600).contains(&min_sec)||
    !exit.is_finite()||!(0.0..=100.0).contains(&exit)||
    !target.is_finite()||!(0.0..=1000.0).contains(&target)||
    !(1..=86_400).contains(&reentry){
   return Err(BridgeError::Policy)
 }
 Ok(PaperTemporalPolicyReview{
    min_lifetime_ns:min_sec*1_000_000_000,
    exit_spread_bps:exit,reentry_cooldown_ns:reentry*1_000_000_000,
    net_target_bps:target,
 })
}
/// A3: read-only source PAPER quotas. The running supervisor still uses
/// its independently compiled 500/day cap, so this is NOT opt-in admission.
#[derive(Debug,Clone,Copy,PartialEq,Eq)]
pub struct SourcePaperQuotasReview{
 pub daily:u64,pub concurrent:usize,
}
pub fn bind_source_paper_quotas_review(
 config:&NativeTradingConfig
)->Result<SourcePaperQuotasReview,BridgeError>{
 if !config.source_projection_unchanged(){return Err(BridgeError::Policy)}
 let day=config.raw.get("limits")
   .and_then(|v|v.get("paper_max_trades_per_day"))
   .and_then(|v|v.as_u64()).ok_or(BridgeError::Unsupported)?;
 let max=config.raw.get("risk")
   .and_then(|v|v.get("max_open_positions"))
   .and_then(|v|v.as_u64()).ok_or(BridgeError::Unsupported)?;
 if day!=config.policy.paper_daily_trades
   ||max!=config.policy.general_max_positions
   ||!(1..=10_000).contains(&day)||!(1..=10).contains(&max){
   return Err(BridgeError::Policy)
 }
 Ok(SourcePaperQuotasReview{daily:day,concurrent:max as usize})
}
pub fn bind_public_paper_decision_config(
 config:&NativeTradingConfig,
)->Result<DecisionConfig,BridgeError>{
 // R93: no untracked raw/policy/source-identity edits after typed parse.
 if !config.source_projection_unchanged(){return Err(BridgeError::Policy)}
 // Executable PAPER decision admission must respect the operator's flag.
 if !config.policy.paper_entries_enabled
  || config.raw.get("general").and_then(|g|g.get("paper_entries_enabled"))
    .and_then(|v|v.as_bool())!=Some(true){return Err(BridgeError::Policy)}
 inspect_public_paper_policy_readonly(config)
}
/// R77: a YAML decision policy alone does not bind PAPER portfolio quotas.
/// This opt-in admission check must compare the actual Rust portfolio
/// constants before any new standalone PUBLIC-PAPER session can select YAML.
/// It does not authorize reconfiguration of running PAPER or real orders.
pub fn verify_paper_portfolio_quota_parity(
 config:&NativeTradingConfig,
)->Result<(),BridgeError>{
 if !config.source_projection_unchanged(){return Err(BridgeError::Policy)}
 let p=&config.policy;
 if !p.paper_entries_enabled||p.real_execution_authorized||p.real_daily_trades!=0
  ||config.raw.get("general").and_then(|g|g.get("paper_entries_enabled"))
    .and_then(|v|v.as_bool())!=Some(true){
  return Err(BridgeError::Policy)
 }
 let raw_day=config.raw.get("limits")
  .and_then(|l|l.get("paper_max_trades_per_day"))
  .and_then(|v|v.as_u64()).ok_or(BridgeError::Unsupported)?;
 let raw_max=config.raw.get("risk")
  .and_then(|r|r.get("max_open_positions"))
  .and_then(|v|v.as_u64()).ok_or(BridgeError::Unsupported)?;
 // The existing portfolio does not read either parameter from YAML.
 // Require EXACT parity, not a permissive inequality or silent fallback.
 if raw_day!=p.paper_daily_trades||raw_max!=p.general_max_positions
  ||raw_day!=crate::native_paper_portfolio_review::MAX_DAILY_PAPER as u64
  ||raw_max!=crate::native_paper_portfolio_review::MAX_CONCURRENT_PAPER as u64{
  return Err(BridgeError::Unsupported)
 }
 Ok(())
}
/// R95: an opt-in YAML PAPER cutover must not imply that unsupported
/// enabled strategies or analytics writers already run on the Rust path.
/// This does not block R75's read-only same-frame decision comparison.
/// Restrict the actual binary admission path; NO per-market-frame cost.
pub fn verify_public_paper_unwired_runtime_features(
 config:&NativeTradingConfig,
)->Result<(),BridgeError>{
 if !config.source_projection_unchanged(){return Err(BridgeError::Policy)}
 // R165: the original spread.history_window=3 is not a demonstrated
 // executable strategy consumer. Freeze this legacy knob in the optional
 // research candidate rather than silently accepting a changed value.
 // This is NOT equivalent strategy behavior and earns no field credit.
 if config.raw.get("spread")
   .and_then(|s|s.get("history_window"))
   .and_then(|v|v.as_u64())!=Some(3){
  return Err(BridgeError::Unsupported)
 }
 for section in [
  "dynamic","dynamic_entry","adaptive_entry","velocity",
  "scale_in_hints","analytics",
 ]{
  let enabled=config.raw.get(section).and_then(|v|v.get("enabled"))
   .and_then(|v|v.as_bool()).ok_or(BridgeError::Unsupported)?;
  if enabled{return Err(BridgeError::Unsupported)}
 }
 Ok(())
}
/// P1-06: the actual research PAPER portfolio uses PaperAssumptions,
/// NOT DecisionConfig. A YAML threshold/fee/size can be parsed and used
/// by DecisionCore while the modeled fill engine still uses DIFFERENT
/// compiled values. Reject this split-brain at optional cutover.
pub fn verify_public_paper_decision_model_parity(
 decision:&DecisionConfig,
 model:&crate::native_paper_market_execution_review::PaperAssumptions
)->Result<(),BridgeError>{
 let close=|a:f64,b:f64|a.is_finite()&&b.is_finite()
   &&(a-b).abs()<=1e-9;
 if !close(decision.base_size,model.quote_budget)
   ||!close(decision.risk_cap,model.quote_budget)
   ||!close(decision.required_net,model.min_entry_net_bps)
   ||!close(decision.cost,model.other_cost_bps)
   ||!close(model.spot_buy_fee_base_bps,0.0)
   ||decision.max_age_ms.checked_mul(1_000_000)
      !=Some(model.max_book_age_ns){
    return Err(BridgeError::Unsupported)
 }
 Ok(())
}
/// P1-06 R1019: do not treat the old "unsupported field" guard as a
/// strategy runtime consumer. R174's 9 legacy knobs are UNWIRED, so
/// the optional PAPER admission must freeze their exact original values.
/// This uses the SAME 9-field R174 list, but doesn't reuse its research
/// frame's opposite velocity.enabled switch requirement.
fn verify_frozen_legacy_public_paper_r1019(
 config:&NativeTradingConfig
)->Result<(),BridgeError>{
 if !config.source_projection_unchanged(){return Err(BridgeError::Policy)}
 let original=NativeTradingConfig::parse(
  include_str!("../config/native_trading_operator_baseline_r64.yaml"))
  .map_err(|_|BridgeError::Unsupported)?;
 for (section,key) in crate::native_r174_unwired_legacy_guard::FROZEN_LEGACY_R174{
  if config.raw.get(section).and_then(|v|v.get(key))
    !=original.raw.get(section).and_then(|v|v.get(key)){
    return Err(BridgeError::Unsupported)
  }
 }
 Ok(())
}
/// P1-06: single fail-closed admission for the OPTIONAL research PAPER
/// binary. Parsing the YAML and binding decision fields alone is not enough:
/// quota parity and still-unwired enabled strategies are separate blockers.
/// This routine never enables production dispatch, network writes or REAL.
pub fn admit_public_paper_yaml_review(
 config:&NativeTradingConfig,
)->Result<DecisionConfig,BridgeError>{
 if !config.source_projection_unchanged()||
    config.raw.get("general").and_then(|g|g.get("trade_enabled"))
     .and_then(|v|v.as_bool())!=Some(false)||
    config.raw.get("real_trial").and_then(|g|g.get("enabled"))
     .and_then(|v|v.as_bool())!=Some(false)||
    config.raw.get("real_trial").and_then(|g|g.get("autonomous_real_enabled"))
     .and_then(|v|v.as_bool())!=Some(false){
   return Err(BridgeError::Policy)
 }
 verify_public_paper_unwired_runtime_features(config)?;
 verify_frozen_legacy_public_paper_r1019(config)?;
 verify_paper_portfolio_quota_parity(config)?;
 // Do not admit structurally valid but unbounded temporal YAML.
 // Actual application to the PAPER portfolio remains a separate step.
 bind_public_paper_temporal_policy_review(config)?;
 let decision=bind_public_paper_decision_config(config)?;
 verify_public_paper_decision_model_parity(
   &decision,&crate::native_paper_market_execution_review::PaperAssumptions::default())?;
 Ok(decision)
}
/// Read-only policy projection, including disabled historical YAML.
/// R1325 uses this in an explicitly selected simulation-only runtime; this
/// function grants no trading capability and does not mutate source flags.
pub fn inspect_public_paper_policy_readonly(
 config:&NativeTradingConfig,
)->Result<DecisionConfig,BridgeError>{
 let p=&config.policy;
 if p.real_execution_authorized||p.real_daily_trades!=0
  ||p.real_max_positions!=1||p.general_max_positions>10
  ||p.entry_min_milli_bps<42_000||p.nominal_order_e18==0
  ||p.nominal_order_e18>10_000_000_000_000_000_000
  ||p.quote_cap_e18< p.nominal_order_e18
  ||p.fee_reserve_milli_bps==0||p.fee_reserve_milli_bps>1_000_000
  ||p.min_market_price_quote_e18==0
  ||p.max_market_price_quote_e18<p.min_market_price_quote_e18
  ||!p.websocket_enabled
  ||config.sha256.len()!=64{
  return Err(BridgeError::Policy)
 }
 let stale=config.raw.get("websocket")
  .and_then(|x|x.get("stale_ms")).and_then(|v|v.as_u64())
  .ok_or(BridgeError::Unsupported)?;
 if !(100..=60_000).contains(&stale){return Err(BridgeError::Policy)}
 let quote=config.raw.get("websocket")
  .and_then(|x|x.get("quote")).and_then(|v|v.as_str());
 if quote!=Some("USDT"){return Err(BridgeError::Policy)}
 let mut blacklist=HashSet::new();
 let rows=config.raw.get("blacklist").and_then(|v|v.as_sequence())
  .ok_or(BridgeError::Unsupported)?;
 if rows.len()>300{return Err(BridgeError::Policy)}
 for v in rows {
  let base=v.as_str().ok_or(BridgeError::Unsupported)?;
  if base.is_empty()||base.len()>32||!base.bytes().all(|b|
   b.is_ascii_uppercase()||b.is_ascii_digit())
   ||!blacklist.insert(base.into()){
   return Err(BridgeError::Policy)
  }
 }
 let size=p.nominal_order_e18 as f64/ONE;
 let cap=p.quote_cap_e18 as f64/ONE;
 let cost=p.fee_reserve_milli_bps as f64/1000.0;
 let required_net=p.entry_min_milli_bps as f64/1000.0;
 let min_price=p.min_market_price_quote_e18 as f64/ONE;
 // Max YAML price is integral: divide in atoms before IEEE conversion.
 // Casting 200000*1e18 directly to f64 rounded it below 200000.
 if p.max_market_price_quote_e18%1_000_000_000_000_000_000!=0{
  return Err(BridgeError::Policy)
 }
 let max_price=(p.max_market_price_quote_e18/1_000_000_000_000_000_000) as f64;
 if [size,cap,cost,required_net,min_price,max_price]
  .iter().any(|v|!v.is_finite()||*v<=0.0)
  ||size>cap||cost>1000.0||min_price>max_price{
  return Err(BridgeError::Policy)
 }
 Ok(DecisionConfig{
  max_age_ms:stale,
  // R21 keeps its original bounded 12h metadata TTL; not a legacy
  // YAML field. This is an explicit constant, NOT 156-key completion.
  metadata_ttl_ms:43_200_000,
  base_size:size,risk_cap:cap,cost,required_net,
  min_price,max_price,blacklist
 })
}

#[cfg(test)]
mod tests{
 use super::*;
 fn config()->NativeTradingConfig{
  // Use the separately aligned review candidate; the Telegram-pinned
  // historical primary intentionally disables PAPER entries.
  NativeTradingConfig::parse(
   include_str!("../config/native_trading_operator_baseline_r64.yaml")).unwrap()
 }
 #[test]
 fn p106_temporal_yaml_changes_reach_real_research_paper_consumer(){
  let original=aligned_paper_source();
  let changed=original
    .replacen("min_lifetime_sec: 15","min_lifetime_sec: 30",1)
    .replacen("exit_bps: 0.5","exit_bps: 1.0",1)
    .replacen("symbol_after_close_sec: 120","symbol_after_close_sec: 60",1);
  assert_ne!(changed,original);
  let yaml=NativeTradingConfig::parse(&changed).unwrap();
  let _decision=admit_public_paper_yaml_review(&yaml).unwrap();
  let t=bind_public_paper_temporal_policy_review(&yaml).unwrap();
  assert_eq!(t.min_lifetime_ns,30_000_000_000);
  assert_eq!(t.reentry_cooldown_ns,60_000_000_000);
  assert_eq!(t.exit_spread_bps,1.0);
  let mut paper=crate::native_paper_portfolio_review::PaperPortfolio::new(
     1_791_783_600_000).unwrap();
  paper.apply_public_paper_temporal_policy_review(
     t.min_lifetime_ns,t.exit_spread_bps,t.reentry_cooldown_ns,t.net_target_bps).unwrap();
  assert!(paper.temporal_exit_eligible_review(30_000_000_000,0.9));
  assert!(!paper.temporal_exit_eligible_review(29_999_999_999,0.9));
  assert!(!paper.temporal_exit_eligible_review(30_000_000_000,1.1));
  let mut cold:crate::native_paper_portfolio_review::PaperPortfolio=
     serde_json::from_str(&serde_json::to_string(&paper).unwrap()).unwrap();
  assert!(cold.verify_public_paper_temporal_policy_review(
     t.min_lifetime_ns,t.exit_spread_bps,t.reentry_cooldown_ns,t.net_target_bps).is_ok());
  assert!(cold.verify_public_paper_temporal_policy_review(
     t.min_lifetime_ns,t.exit_spread_bps,120_000_000_000,t.net_target_bps).is_err());
  assert!(cold.apply_public_paper_temporal_policy_review(
     1,1.0,60_000_000_000,t.net_target_bps).is_err());
 }
 #[test]
 fn p106_net_target_yaml_must_reach_modeled_exit_and_survive_restore(){
  let source=aligned_paper_source().replacen("net_target_bps: 12.0","net_target_bps: 5.0",1);
  let cfg=NativeTradingConfig::parse(&source).unwrap();
  let _=admit_public_paper_yaml_review(&cfg).unwrap();
  let t=bind_public_paper_temporal_policy_review(&cfg).unwrap();
  assert_eq!(t.net_target_bps,5.0);
  let mut p=crate::native_paper_portfolio_review::PaperPortfolio::new(
      1_791_783_600_000).unwrap();
  p.apply_public_paper_temporal_policy_review(
      t.min_lifetime_ns,t.exit_spread_bps,t.reentry_cooldown_ns,t.net_target_bps).unwrap();
  assert_eq!(p.net_target_bps,5.0);
  let cold:crate::native_paper_portfolio_review::PaperPortfolio=
      serde_json::from_str(&serde_json::to_string(&p).unwrap()).unwrap();
  assert!(cold.verify_public_paper_temporal_policy_review(
      t.min_lifetime_ns,t.exit_spread_bps,t.reentry_cooldown_ns,5.0).is_ok());
  assert!(cold.verify_public_paper_temporal_policy_review(
      t.min_lifetime_ns,t.exit_spread_bps,t.reentry_cooldown_ns,500.0).is_err());
  let mut legacy=serde_json::to_value(&cold).unwrap();
  legacy.as_object_mut().unwrap().remove("net_target_bps");
  let legacy:crate::native_paper_portfolio_review::PaperPortfolio=
      serde_json::from_value(legacy).unwrap();
  assert_eq!(legacy.net_target_bps,12.0);
  assert!(legacy.verify_public_paper_temporal_policy_review(
      t.min_lifetime_ns,t.exit_spread_bps,t.reentry_cooldown_ns,5.0).is_err());
 }
 #[test]
 fn p106_temporal_invalid_or_divergent_yaml_fails_closed(){
  let src=aligned_paper_source();
  for (old,new) in [
    ("min_lifetime_sec: 15","min_lifetime_sec: 0"),
    ("min_lifetime_sec: 15","min_lifetime_sec: 21601"),
    ("exit_bps: 0.5","exit_bps: 101.0"),
    ("exit_bps: 0.5","exit_bps: -1.0"),
    ("net_target_bps: 12.0","net_target_bps: -1.0"),
    ("net_target_bps: 12.0","net_target_bps: 1001.0"),
    ("symbol_after_close_sec: 120","symbol_after_close_sec: 0"),
    ("symbol_after_close_sec: 120","symbol_after_close_sec: 86401"),
  ]{
    let altered=src.replacen(old,new,1);
    assert_ne!(altered,src);
    if let Ok(yaml)=NativeTradingConfig::parse(&altered){
      assert!(admit_public_paper_yaml_review(&yaml).is_err(),
        "unbounded {old} -> {new} must deny opt-in");
    }
  }
  let mut altered=fully_aligned_review_config();
  altered.raw["spread"]["min_lifetime_sec"]=serde_yaml::Value::from(1u64);
  assert_eq!(bind_public_paper_temporal_policy_review(&altered).err(),
    Some(BridgeError::Policy));
  let mut restored=crate::native_paper_portfolio_review::PaperPortfolio::new(
      1_791_783_600_000).unwrap();
  restored.min_lifetime_ns=0;
  assert!(restored.on_frame("X",crate::Venue::OkxSpot,1,
      1_791_783_600_000,None,None,None,None).is_err());
 }
 #[test]
 fn r95_full_paper_yaml_cutover_denies_unwired_enabled_features(){
  let source=include_str!("../config/native_trading_operator_baseline_r64.yaml");
  let original=NativeTradingConfig::parse(source).unwrap();
  assert_eq!(verify_public_paper_unwired_runtime_features(&original),
   Err(BridgeError::Unsupported));
  // A separately parsed synthetic fixture can test each unsupported
  // feature independently, with no change to operator config on disk.
  let mut all_disabled=source.to_owned();
  for section in ["dynamic","velocity","scale_in_hints","analytics"]{
   let old=format!("\n{section}:\n  enabled: true");
   let new=format!("\n{section}:\n  enabled: false");
   assert!(all_disabled.contains(&old),"missing {section}");
   all_disabled=all_disabled.replacen(&old,&new,1);
  }
  let safe=NativeTradingConfig::parse(&all_disabled).unwrap();
  assert_eq!(verify_public_paper_unwired_runtime_features(&safe),Ok(()));
  for section in ["dynamic","velocity","scale_in_hints","analytics",
                  "dynamic_entry","adaptive_entry"]{
   let old=format!("\n{section}:\n  enabled: false");
   let new=format!("\n{section}:\n  enabled: true");
   assert!(all_disabled.contains(&old),"missing disabled {section}");
   let changed=all_disabled.replacen(&old,&new,1);
   let candidate=NativeTradingConfig::parse(&changed).unwrap();
   assert_eq!(verify_public_paper_unwired_runtime_features(&candidate),
    Err(BridgeError::Unsupported),"unsupported {section}");
  }
 }
 #[test]
 fn r165_original_history_window_is_frozen_not_claimed_as_wired(){
  let source=aligned_paper_source();
  assert!(source.contains("history_window: 3"));
  let original=NativeTradingConfig::parse(&source).unwrap();
  assert_eq!(verify_public_paper_unwired_runtime_features(&original),Ok(()));
  // A new configuration value cannot silently flow into an unchanged
  // strategy without proving what old Python did with that field.
  for new in ["4","0","-1","999999","true"]{
   let altered=source.replacen("history_window: 3",
      &format!("history_window: {new}"),1);
   assert_ne!(altered,source);
   if let Ok(config)=NativeTradingConfig::parse(&altered){
    assert_eq!(verify_public_paper_unwired_runtime_features(&config),
      Err(BridgeError::Unsupported));
    assert_eq!(admit_public_paper_yaml_review(&config).err(),
      Some(BridgeError::Unsupported));
   }
  }
  let mut edited=original.clone();
  edited.raw["spread"]["history_window"]=serde_yaml::Value::from(4u64);
  assert_eq!(verify_public_paper_unwired_runtime_features(&edited),
    Err(BridgeError::Policy));
  // The audited field is still NOT research/installed runtime proven.
  let claim:serde_json::Value=serde_json::from_str(include_str!(
    "../config/native_field_evidence_r149_entry_telegram_review.json")).unwrap();
  assert_eq!(claim["unproven_entry_field"],"spread.history_window");
  assert!(!claim["fields"].as_array().unwrap().iter()
    .any(|x|x.as_str()==Some("spread.history_window")));
 }
 fn fully_aligned_review_config()->NativeTradingConfig{
  NativeTradingConfig::parse(&aligned_paper_source()).unwrap()
 }
 fn aligned_paper_source()->String{
  let mut source=include_str!(
    "../config/native_trading_operator_baseline_r64.yaml").to_string();
  source=source.replacen(
    "paper_max_trades_per_day: 3000","paper_max_trades_per_day: 500",1);
  for section in ["dynamic","velocity","scale_in_hints","analytics"]{
   let enabled=format!("\n{section}:\n  enabled: true");
   let disabled=format!("\n{section}:\n  enabled: false");
   assert!(source.contains(&enabled),"{section} unexpectedly disabled");
   source=source.replacen(&enabled,&disabled,1);
  }
  source
 }
 #[test]
 fn p106_single_admission_refuses_original_and_unwired_yaml(){
  // R1075: the tracked Rust source now has REAL flags set to false,
  // but it still contains unsupported PAPER features and a 3000/day
  // legacy research quota. Admission must remain fail-closed.
  let native_source=NativeTradingConfig::parse(
   include_str!("../config/native_trading.yaml")).unwrap();
  assert_eq!(admit_public_paper_yaml_review(&native_source).err(),
    Some(BridgeError::Unsupported));
  let baseline=config();
  assert_eq!(admit_public_paper_yaml_review(&baseline).err(),
    Some(BridgeError::Unsupported));
  let aligned=fully_aligned_review_config();
  let admitted=admit_public_paper_yaml_review(&aligned).unwrap();
  assert_eq!(admitted.required_net,42.0);
  assert_eq!(admitted.base_size,10.0);
  assert_eq!(admitted.risk_cap,10.0);
  assert_eq!(admitted.max_age_ms,800);
  assert!(admitted.blacklist.contains("ONE"));
  assert_eq!(verify_paper_portfolio_quota_parity(&aligned),Ok(()));
 }
 #[test]
 fn p106_single_admission_denies_quota_and_unwired_flags(){
  let source=include_str!("../config/native_trading_operator_baseline_r64.yaml");
  let mut aligned=source.replacen(
    "paper_max_trades_per_day: 3000","paper_max_trades_per_day: 500",1);
  for section in ["dynamic","velocity","scale_in_hints","analytics"]{
   let before=format!("\n{section}:\n  enabled: true");
   let after=format!("\n{section}:\n  enabled: false");
   aligned=aligned.replacen(&before,&after,1);
  }
  for section in ["dynamic","velocity","scale_in_hints","analytics",
                  "dynamic_entry","adaptive_entry"]{
   let before=format!("\n{section}:\n  enabled: false");
   let after=format!("\n{section}:\n  enabled: true");
   let changed=aligned.replacen(&before,&after,1);
   assert_ne!(changed,aligned,"{section} toggle not found");
   let c=NativeTradingConfig::parse(&changed).unwrap();
   assert_eq!(admit_public_paper_yaml_review(&c).err(),
     Some(BridgeError::Unsupported),"not wired: {section}");
  }
  let mismatch=aligned.replacen(
    "paper_max_trades_per_day: 500","paper_max_trades_per_day: 501",1);
  assert_eq!(admit_public_paper_yaml_review(
   &NativeTradingConfig::parse(&mismatch).unwrap()).err(),
    Some(BridgeError::Unsupported));
  let bad=aligned.replacen("trade_enabled: false","trade_enabled: true",1);
  let c=NativeTradingConfig::parse(&bad).unwrap();
  assert_eq!(admit_public_paper_yaml_review(&c).err(),Some(BridgeError::Policy));
  for field in ["enabled: false","autonomous_real_enabled: false"]{
   // The first is the real_trial enabled flag, not a strategy flag.
   let key=if field=="enabled: false"{
     "real_trial:\n  enabled: false"
   }else{
     "autonomous_real_enabled: false"
   };
   let replacement=key.replace("false","true");
   let changed=aligned.replacen(key,&replacement,1);
   assert_ne!(changed,aligned);
   if let Ok(cfg)=NativeTradingConfig::parse(&changed){
     assert_eq!(admit_public_paper_yaml_review(&cfg).err(),
       Some(BridgeError::Policy));
   }
  }
  let mut tampered=fully_aligned_review_config();
  tampered.raw["filters"]["max_price"]=serde_yaml::Value::from(10u64);
  assert_eq!(admit_public_paper_yaml_review(&tampered).err(),
    Some(BridgeError::Policy));
 }
 #[test]
 fn p106_paper_model_split_brain_denies_yaml_cutover(){
  use crate::native_paper_market_execution_review::PaperAssumptions;
  let aligned=aligned_paper_source();
  let cfg=fully_aligned_review_config();
  let decision=admit_public_paper_yaml_review(&cfg).unwrap();
  let mut model=PaperAssumptions::default();
  assert_eq!(verify_public_paper_decision_model_parity(&decision,&model),Ok(()));
  model.quote_budget=9.0;
  assert_eq!(verify_public_paper_decision_model_parity(&decision,&model),
    Err(BridgeError::Unsupported));
  model=PaperAssumptions::default();model.other_cost_bps=25.03;
  assert_eq!(verify_public_paper_decision_model_parity(&decision,&model),
    Err(BridgeError::Unsupported));
  model=PaperAssumptions::default();model.min_entry_net_bps=43.0;
  assert_eq!(verify_public_paper_decision_model_parity(&decision,&model),
    Err(BridgeError::Unsupported));
  model=PaperAssumptions::default();model.max_book_age_ns+=1_000_000;
  assert_eq!(verify_public_paper_decision_model_parity(&decision,&model),
    Err(BridgeError::Unsupported));
  model=PaperAssumptions::default();model.spot_buy_fee_base_bps=1.0;
  assert_eq!(verify_public_paper_decision_model_parity(&decision,&model),
    Err(BridgeError::Unsupported));
  for (before,after) in [
     ("entry_bps: 42.0","entry_bps: 43.0"),
     ("round_fees_bps: 24.0","round_fees_bps: 25.0"),
     ("futures_order_usdt: 10","futures_order_usdt: 9"),
     ("stale_ms: 800","stale_ms: 801"),
  ]{
    let edited=aligned.replacen(before,after,1);
    assert_ne!(edited,aligned,"missing fixture field {before}");
    let parsed=NativeTradingConfig::parse(&edited).unwrap();
    assert!(bind_public_paper_decision_config(&parsed).is_ok(),
       "control: isolated decision binder permits this valid source");
    assert_eq!(admit_public_paper_yaml_review(&parsed).err(),
      Some(BridgeError::Unsupported),"{before} must not split two PAPER policies");
  }
 }
 #[test]
 fn p106_yaml_rust_decision_engine_consumes_spread_fees_price_blacklist(){
  use crate::decision::{evaluate,Limits,RouteSpec};
  use crate::Book;
  let source=aligned_paper_source();
  let yaml=NativeTradingConfig::parse(&source).unwrap();
  let cfg=admit_public_paper_yaml_review(&yaml).unwrap();
  let limits=Limits{min_amount:"0.001".into(),step:"0.001".into(),
    min_cost:"1".into(),max_amount:None};
  let route=RouteSpec{buy_exchange:"okx_spot".into(),
    sell_exchange:"binance_futures".into(),label:"PUBLIC_PAPER".into(),
    buy:limits.clone(),sell:limits,loaded_ns:1,
    metadata_state:"SIZE_METADATA_READY".into()};
  let buy=Book{bids:vec![(0.99,30.)],asks:vec![(1.0,30.)],
    received_ns:1,received_wall_ms:1000,exchange_ts_ms:Some(1000),
    changed_ns:1,epoch:1,sequence:None};
  let sell=Book{bids:vec![(1.01,30.)],asks:vec![(1.02,30.)],..buy.clone()};
  let run=|base:&str,policy:&DecisionConfig|
    evaluate(base,&route,Some(&buy),Some(&sell),1,1000,policy);
  let result=run("BTC",&cfg);
  assert_eq!(result.reason,"EXECUTABLE_OPPORTUNITY");
  assert_eq!(result.required_net_bps,42.0);
  assert!((result.cost_bps-24.03).abs()<1e-9);
  assert_eq!(result.risk_cap_usdt,10.0);
  assert_eq!(run("ONE",&cfg).reason,"BLACKLIST");
  let threshold=source.replacen("entry_bps: 42.0","entry_bps: 80.0",1);
  let p=bind_public_paper_decision_config(
     &NativeTradingConfig::parse(&threshold).unwrap()).unwrap();
  assert_eq!(run("BTC",&p).reason,"EDGE_TOO_LOW");
  assert_eq!(admit_public_paper_yaml_review(
     &NativeTradingConfig::parse(&threshold).unwrap()).err(),
     Some(BridgeError::Unsupported));
  let fees=source.replacen("round_fees_bps: 24.0","round_fees_bps: 70.0",1);
  let p=bind_public_paper_decision_config(
     &NativeTradingConfig::parse(&fees).unwrap()).unwrap();
  assert_eq!(run("BTC",&p).reason,"EDGE_TOO_LOW");
  let price=source.replacen("min_price: 0.03","min_price: 2.0",1);
  let p=admit_public_paper_yaml_review(
     &NativeTradingConfig::parse(&price).unwrap()).unwrap();
  assert_eq!(run("BTC",&p).reason,"PRICE_FILTER");
 }
 #[test]
 fn r93_source_snapshot_must_not_change_after_parse_before_paper_bind(){
  let mut x=config();
  x.raw["filters"]["max_price"]=serde_yaml::Value::from(100u64);
  assert!(bind_public_paper_decision_config(&x).is_err());
  let mut x=config();
  x.raw["blacklist"]=serde_yaml::from_str("[ZEC, DASH, ONE, BTC]").unwrap();
  assert!(bind_public_paper_decision_config(&x).is_err());
  let mut x=config();
  x.policy.nominal_order_e18=9_000_000_000_000_000_000;
  assert!(bind_public_paper_decision_config(&x).is_err());
  let mut x=config();
  x.sha256="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into();
  assert!(bind_public_paper_decision_config(&x).is_err());
  // Match R77 compiled quota in a freshly parsed synthetic source:
  // source must be consistent BEFORE quota parity may return Ok.
  let aligned=include_str!("../config/native_trading_operator_baseline_r64.yaml")
    .replacen("paper_max_trades_per_day: 3000","paper_max_trades_per_day: 500",1);
  let mut x=NativeTradingConfig::parse(&aligned).unwrap();
  assert_eq!(verify_paper_portfolio_quota_parity(&x),Ok(()));
  x.raw["filters"]["max_price"]=serde_yaml::Value::from(100u64);
  assert_eq!(verify_paper_portfolio_quota_parity(&x),Err(BridgeError::Policy));
 }
 #[test]
 fn r92_public_paper_source_flag_must_match_parsed_policy(){
  let mut cfg=config();
  cfg.raw["general"]["paper_entries_enabled"]=serde_yaml::Value::Bool(false);
  assert!(matches!(bind_public_paper_decision_config(&cfg),Err(BridgeError::Policy)));
  assert_eq!(verify_paper_portfolio_quota_parity(&cfg),Err(BridgeError::Policy));
 }
 #[test]
 fn r76_disabled_paper_entries_must_never_bind_to_an_entry_decision_core(){
  // The historic PRIMARY is SHA pinned to Telegram and deliberately
  // preserves paper_entries_enabled=false. It must never be promoted
  // into a PAPER entry strategy just because the remaining fields parse.
  let original=NativeTradingConfig::parse(
    include_str!("../config/native_trading.yaml")).unwrap();
  assert!(!original.policy.paper_entries_enabled);
  assert!(matches!(bind_public_paper_decision_config(&original),Err(BridgeError::Policy)));
 }
 #[test]
 fn r77_operator_yaml_3000_daily_entries_must_not_claim_500_portfolio_parity(){
  let candidate=config();
  assert_eq!(candidate.policy.paper_daily_trades,3000);
  assert_eq!(candidate.policy.general_max_positions,10);
  assert!(bind_public_paper_decision_config(&candidate).is_ok());
  assert_eq!(verify_paper_portfolio_quota_parity(&candidate),Err(BridgeError::Unsupported));
 }
 #[test]
 fn r77_disabled_historic_primary_must_not_promote_even_at_matching_quotas(){
  let original=NativeTradingConfig::parse(
   include_str!("../config/native_trading.yaml")).unwrap();
  assert!(!original.policy.paper_entries_enabled);
  assert!(verify_paper_portfolio_quota_parity(&original).is_err());
 }
 #[test]
 fn r77_synthetic_exact_cap_parity_and_tampered_policy_cannot_pass_as_source(){
  let source=include_str!("../config/native_trading_operator_baseline_r64.yaml");
  let aligned=source.replacen("paper_max_trades_per_day: 3000",
   "paper_max_trades_per_day: 500",1);
  assert_ne!(source,aligned);
  let mut cfg=NativeTradingConfig::parse(&aligned).unwrap();
  assert_eq!(verify_paper_portfolio_quota_parity(&cfg),Ok(()));
  cfg.policy.paper_daily_trades=3000;
  // R93 rejects any drift from parse-time source before quota comparison.
  assert_eq!(verify_paper_portfolio_quota_parity(&cfg),Err(BridgeError::Policy));
  cfg.policy.paper_daily_trades=500;
  cfg.policy.general_max_positions=9;
  assert_eq!(verify_paper_portfolio_quota_parity(&cfg),Err(BridgeError::Policy));
 }
 #[test]
 fn a3_original_quota_is_bound_but_supervisor_profile_is_still_denied(){
  let cfg=config();
  let source=bind_source_paper_quotas_review(&cfg).unwrap();
  assert_eq!(source.daily,3000);
  assert_eq!(source.concurrent,10);
  assert!(verify_paper_portfolio_quota_parity(&cfg).is_err());
  assert!(admit_public_paper_yaml_review(&cfg).is_err());
  let mut altered=cfg.clone();
  altered.raw["limits"]["paper_max_trades_per_day"]=
     serde_yaml::Value::from(500u64);
  assert_eq!(bind_source_paper_quotas_review(&altered),Err(BridgeError::Policy));
  let mut altered=cfg.clone();
  altered.policy.paper_daily_trades=500;
  assert_eq!(bind_source_paper_quotas_review(&altered),Err(BridgeError::Policy));
 }
 #[test]
 fn r113_complete_review_register_has_no_active_binary_claims(){
  let p:serde_json::Value=serde_json::from_str(
   include_str!("../config/native_field_evidence_r113_review.json")).unwrap();
  let original:serde_json::Value=serde_json::from_str(
   include_str!("../config/native_field_status.json")).unwrap();
  let entries=p["entries"].as_array().unwrap();
  assert_eq!(entries.len(),160);
  for e in entries{
   assert_eq!(e["active_binary_proven"],false);
   let key=e["field"].as_str().unwrap();
   assert_eq!(e["source_status"],original[key]["status"]);
  }
 }
 #[test]
 fn binds_actual_legacy_yaml_to_paper_decision_and_blacklist(){
  let p=bind_public_paper_decision_config(&config()).unwrap();
  assert_eq!(p.max_age_ms,800);
  assert_eq!(p.metadata_ttl_ms,43_200_000);
  assert_eq!(p.base_size,10.0);
  assert_eq!(p.risk_cap,10.0);
  assert_eq!(p.cost,24.03);
  assert_eq!(p.required_net,42.0);
  assert!((p.min_price-0.03).abs()<1e-12);
  assert_eq!(p.max_price,200_000.0);
  assert!(p.blacklist.contains("ZEC"));
  assert!(p.blacklist.contains("DASH"));
  assert!(p.blacklist.contains("ONE"));
  assert!(!p.blacklist.contains("BTC"));
 }
 #[test]
 fn fail_closed_on_tampered_risk_ws_and_blacklist(){
  let mut x=config();
  x.policy.real_execution_authorized=true;
  assert!(bind_public_paper_decision_config(&x).is_err());
  let mut x=config();
  x.policy.real_execution_authorized=false;
  x.policy.quote_cap_e18=1;
  assert!(bind_public_paper_decision_config(&x).is_err());
  let mut x=config();
  x.policy.websocket_enabled=false;
  assert!(bind_public_paper_decision_config(&x).is_err());
  let mut x=config();
  x.raw["websocket"]["stale_ms"]=serde_yaml::Value::from(60_001);
  assert!(bind_public_paper_decision_config(&x).is_err());
  let mut x=config();
  x.raw["websocket"]["quote"]=serde_yaml::Value::from("BTC");
  assert!(bind_public_paper_decision_config(&x).is_err());
  let mut x=config();
  x.raw["blacklist"]=serde_yaml::from_str("[ZEC, ZEC]").unwrap();
  assert!(bind_public_paper_decision_config(&x).is_err());
 }
 include!("native_r979_r1018_p106_yaml_differential_tests.rs");
}
