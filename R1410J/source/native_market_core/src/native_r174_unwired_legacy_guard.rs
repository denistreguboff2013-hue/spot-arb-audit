//! Astra R174: reject modified, unimplemented legacy YAML controls in the
//! isolated source-driven PAPER research profile. No runtime feature credit.
//! This is a startup guard; no filesystem, network, trading or timers.
use crate::native_trading_config::NativeTradingConfig;
use serde_yaml::Value;
#[derive(Debug,Clone,Copy,PartialEq,Eq)]
pub enum R174Error{Source,Unsupported}
/// These nine original YAML values have no proven active research strategy
/// consumer. R165 already freezes history_window for a different route.
pub const FROZEN_LEGACY_R174:[(&str,&str);9]=[
 ("general","bot_name"),
 ("general","debug_write_liquidity"),
 ("general","liquidity_bps"),
 ("general","mode"),
 ("logging","level"),
 ("scale_in_hints","min_imbalance"),
 ("scale_in_hints","min_vel_bps_s"),
 ("scale_in_hints","send_negative"),
 ("spread","history_window"),
];
fn field<'a>(raw:&'a Value,section:&str,key:&str)->Option<&'a Value>{
 raw.get(section)?.get(key)
}
/// Must reject an unsupported source value instead of silently accepting
/// it as if it controlled PAPER. An operator changing any frozen knob needs
/// a separate semantic implementation and tests, NOT just a schema pass.
pub fn verify_unwired_source_r174(cfg:&NativeTradingConfig)->Result<(),R174Error>{
 if !cfg.source_projection_unchanged()
  ||cfg.policy.real_execution_authorized||cfg.policy.real_daily_trades!=0{
   return Err(R174Error::Source)
 }
 let reference=NativeTradingConfig::parse(
  include_str!("../config/native_trading_operator_baseline_r64.yaml"))
  .map_err(|_|R174Error::Unsupported)?;
 for (section,key) in FROZEN_LEGACY_R174{
  let wanted=field(&reference.raw,section,key).ok_or(R174Error::Unsupported)?;
  if field(&cfg.raw,section,key)!=Some(wanted){
   return Err(R174Error::Unsupported)
  }
 }
 // Existing R155::new already rejects both wrong feature switches; repeat
 // here only to make the standalone guard profile fully explicit.
 if field(&cfg.raw,"scale_in_hints","enabled")
  .and_then(Value::as_bool)!=Some(false)
 ||field(&cfg.raw,"velocity","enabled")
  .and_then(Value::as_bool)!=Some(true){
  return Err(R174Error::Unsupported)
 }
 Ok(())
}

#[cfg(test)]
mod tests{
 use super::*;
 const BASE:&str=include_str!("../config/native_trading_operator_baseline_r64.yaml");
 fn candidate()->String{
  let old="scale_in_hints:\n  enabled: true";
  assert_eq!(BASE.matches(old).count(),1);
  BASE.replacen(old,"scale_in_hints:\n  enabled: false",1)
 }
 fn parsed(src:&str)->NativeTradingConfig{
  NativeTradingConfig::parse(src).unwrap()
 }
 #[test]fn r174_research_candidate_frozen_values_accepted(){
  let cfg=parsed(&candidate());
  assert_eq!(verify_unwired_source_r174(&cfg),Ok(()));
  assert_eq!(FROZEN_LEGACY_R174.len(),9);
  assert_eq!(FROZEN_LEGACY_R174.iter().filter(|(sec,_)|
   *sec=="general").count(),4);
 }
 #[test]fn r174_nine_unwired_value_mutations_fail_closed(){
  let base=candidate();
  let changes=[
   ("bot_name: spot-arb-bot","bot_name: another-bot"),
   ("debug_write_liquidity: true","debug_write_liquidity: false"),
   ("liquidity_bps: 50","liquidity_bps: 51"),
   ("mode: soft","mode: strict"),
   ("level: DEBUG","level: INFO"),
   ("min_imbalance: 1.03","min_imbalance: 1.04"),
   ("min_vel_bps_s: 0.08","min_vel_bps_s: 0.09"),
   ("send_negative: false","send_negative: true"),
   ("history_window: 3","history_window: 4")
  ];
  for (old,new) in changes{
   assert_eq!(base.matches(old).count(),1,"fixture {old}");
   let text=base.replacen(old,new,1);
   match NativeTradingConfig::parse(&text){
    Ok(mutated)=>assert_eq!(verify_unwired_source_r174(&mutated),
     Err(R174Error::Unsupported),"acceptance despite drift {old}"),
    Err(_)=>{} // parser fail-closed is also an admissible result
   }
  }
 }
 #[test]fn r174_both_unwired_mode_switches_fail_closed(){
  let cfg=parsed(BASE);
  assert_eq!(verify_unwired_source_r174(&cfg),Err(R174Error::Unsupported));
  let disabled=candidate().replacen("velocity:\n  enabled: true",
   "velocity:\n  enabled: false",1);
  let cfg=parsed(&disabled);
  assert_eq!(verify_unwired_source_r174(&cfg),Err(R174Error::Unsupported));
 }
 #[test]fn r174_raw_yaml_drift_is_rejected(){
  let mut cfg=parsed(&candidate());
  cfg.raw["spread"]["history_window"]=Value::from(4u64);
  assert_eq!(verify_unwired_source_r174(&cfg),Err(R174Error::Source));
 }
}

#[cfg(test)]
mod admission_tests{
 use super::*;
 use crate::native_paper_strategy_frame_review::SupervisedStrategyPaper;
 const AT:i64=1_791_783_600_000;
 fn admissible()->String{
  include_str!("../config/native_trading_operator_baseline_r64.yaml")
   .replacen("paper_max_trades_per_day: 3000",
             "paper_max_trades_per_day: 500",1)
   .replacen("scale_in_hints:\n  enabled: true",
             "scale_in_hints:\n  enabled: false",1)
   .replacen("analytics:\n  enabled: true",
             "analytics:\n  enabled: false",1)
 }
 #[test]fn r174_source_paper_admission_blocks_general_legacy_drift(){
  let src=admissible();
  let ok=NativeTradingConfig::parse(&src).unwrap();
  assert!(SupervisedStrategyPaper::new(ok,AT).is_ok(),
          "unchanged previously admitted research candidate");
  let modified=src.replacen("liquidity_bps: 50","liquidity_bps: 51",1);
  assert_ne!(src,modified);
  let changed=NativeTradingConfig::parse(&modified).unwrap();
  assert!(SupervisedStrategyPaper::new(changed,AT).is_err());
 }
 #[test]fn r174_source_paper_admission_blocks_dead_logging_knob(){
  let src=admissible();
  let text=src.replacen("level: DEBUG","level: INFO",1);
  let changed=NativeTradingConfig::parse(&text).unwrap();
  assert!(SupervisedStrategyPaper::new(changed,AT).is_err());
 }
}
