//! Astra P1-06: strictly READ-ONLY legacy strategy policy projection.
//! No live PAPER mutations, order transport, private REST, or market subscriptions.
//! This is a pure fixture review, not live consumer equivalence or cutover authority.
use crate::native_trading_config::NativeTradingConfig;
use crate::native_paper_model_config_review::inspect_dynamic_exit_from_yaml;
use crate::position_lifecycle::{CloseDecision,CloseTriggerInput,evaluate_close_trigger};
use serde_yaml::Value;

#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum LegacyStrategyError { Source, Missing, Range }
fn sec<'a>(cfg:&'a NativeTradingConfig,section:&str)->Result<&'a Value,LegacyStrategyError>{
 cfg.raw.get(section).ok_or(LegacyStrategyError::Missing)
}
fn num(v:&Value,key:&str)->Result<f64,LegacyStrategyError>{
 let n=v.get(key).and_then(|n|n.as_f64()).ok_or(LegacyStrategyError::Missing)?;
 if !n.is_finite(){return Err(LegacyStrategyError::Range)}
 Ok(n)
}
fn yes(v:&Value,key:&str)->Result<bool,LegacyStrategyError>{
 v.get(key).and_then(|n|n.as_bool()).ok_or(LegacyStrategyError::Missing)
}
fn valid(cfg:&NativeTradingConfig)->Result<(),LegacyStrategyError>{
 if !cfg.source_projection_unchanged()||cfg.policy.real_execution_authorized
   ||cfg.policy.real_daily_trades!=0 {
  return Err(LegacyStrategyError::Source)
 }
 Ok(())
}
#[derive(Clone,Copy,Debug,PartialEq)]
pub struct LegacyCloseObservation{
 pub observed_spread_bps:f64, // current scan/WS spread (entry direction)
 pub executable_close_spread_bps:Option<f64>, // futures ask / spot bid
 pub velocity_bps_s:Option<f64>, // optional WS or previous telemetry slope
 pub elapsed_seconds:u64,
 pub position_units:u128,
 pub force_close:bool, // simulated fixture only
 pub futures_emergency_stop_hit:bool, // fixture, no private verification
}
#[derive(Clone,Copy,Debug,PartialEq)]
pub struct LegacyClosePreview{
 pub dynamic_exit_bps:f64,pub dynamic_min_lifetime_sec:u64,
 pub emergency_by_spread:bool,pub emergency_by_velocity:bool,
 pub decision:CloseDecision,pub can_trade:bool,pub source_exchange_attested:bool,
}
/// Mirror strategy.py checks around 5077-5115, 5250 and the pure close
/// decision. The amount must be known before any CLOSE enum is returned.
/// Does NOT model the original hedge truth/partial fill/manual reconcilers.
pub fn inspect_legacy_close_readonly(
 cfg:&NativeTradingConfig,o:LegacyCloseObservation
)->Result<LegacyClosePreview,LegacyStrategyError>{
 valid(cfg)?;
 if !o.observed_spread_bps.is_finite()
  ||o.velocity_bps_s.is_some_and(|n|!n.is_finite())
  ||o.executable_close_spread_bps.is_some_and(|n|!n.is_finite()){
  return Err(LegacyStrategyError::Range)
 }
 let v=sec(cfg,"velocity")?;
 let velocity_enabled=yes(v,"enabled")?;
 let hold=num(v,"hold_pos_bps_s")?;
 let neg=num(v,"emergency_neg_bps_s")?;
 let em_spread=num(v,"emergency_spread_bps")?;
 let guard=num(v,"emergency_guard_spread_bps")?;
 if hold<0.0||neg>=0.0||em_spread>=0.0||guard<0.0||guard>10_000.0{
  return Err(LegacyStrategyError::Range)
 }
 let (exit,life)=inspect_dynamic_exit_from_yaml(cfg,o.observed_spread_bps)
  .map_err(|_|LegacyStrategyError::Source)?;
 let em_by_spread=o.observed_spread_bps<=em_spread;
 let em_by_velocity=velocity_enabled&&o.velocity_bps_s.is_some_and(|rate|
  rate<=neg&&o.observed_spread_bps<=guard);
 let emergency=o.force_close||o.futures_emergency_stop_hit
  ||em_by_spread||em_by_velocity;
 let decision=evaluate_close_trigger(CloseTriggerInput{
  emergency,amount_units:o.position_units,lifetime_sec:o.elapsed_seconds,
  min_lifetime_sec:life,close_spread_bps:o.executable_close_spread_bps,
  exit_bps:exit,
  velocity_bps_s:if velocity_enabled{o.velocity_bps_s}else{None},
  hold_positive_bps_s:if velocity_enabled{hold}else{0.0},
 });
 Ok(LegacyClosePreview{
  dynamic_exit_bps:exit,dynamic_min_lifetime_sec:life,
  emergency_by_spread:em_by_spread,emergency_by_velocity:em_by_velocity,
  decision,can_trade:false,source_exchange_attested:false
 })
}
/// The legacy _vel_from_tel takes its denominator from telemetry timestamps;
/// for dt<=100us it returns None. Never mutate persisted position history.
pub fn inspect_telemetry_velocity_readonly(
 current_spread:f64,prior:Option<(f64,f64)>,timestamp_sec:f64
)->Result<Option<f64>,LegacyStrategyError>{
 if !current_spread.is_finite()||!timestamp_sec.is_finite(){
  return Err(LegacyStrategyError::Range)
 }
 let Some((previous,prior_sec))=prior else{return Ok(None)};
 if !previous.is_finite()||!prior_sec.is_finite(){
  return Err(LegacyStrategyError::Range)
 }
 let dt=timestamp_sec-prior_sec;
 if dt<=0.0001{return Ok(None)}
 let rate=(current_spread-previous)/dt;
 if !rate.is_finite(){return Err(LegacyStrategyError::Range)}
 Ok(Some(rate))
}

/// Python strategy.py::_pick_dynamic_entry_target uses CURRENT observed spread.
/// It is disabled in operator YAML but the Rust fixture model supports it.
/// Strict invalid YAML is denied rather than using Python's exception fallback.
pub fn inspect_dynamic_entry_target_readonly(
 cfg:&NativeTradingConfig,observed_spread_bps:f64
)->Result<f64,LegacyStrategyError>{
 valid(cfg)?;
 if !observed_spread_bps.is_finite(){return Err(LegacyStrategyError::Range)}
 let spread=sec(cfg,"spread")?;
 let default=num(spread,"net_target_bps")?;
 let dyn_cfg=sec(cfg,"dynamic_entry")?;
 if !yes(dyn_cfg,"enabled")?{return Ok(default)}
 let rows=dyn_cfg.get("segments").and_then(Value::as_sequence)
  .ok_or(LegacyStrategyError::Missing)?;
 if rows.is_empty()||rows.len()>16{return Err(LegacyStrategyError::Range)}
 let mut first_match=None;
 let mut last=None;
 let mut prev_end:Option<f64>=None;
 for row in rows{
  let lo=num(row,"from_entry_bps")?;
  let hi=num(row,"to_entry_bps")?;
  let a=num(row,"target_from_bps")?;
  let b=num(row,"target_to_bps")?;
  if lo<0.0||hi<=lo||hi>1_000_000.0||a<0.0||b<0.0
   ||a>100_000.0||b>100_000.0||prev_end.is_some_and(|end|lo<end){
   return Err(LegacyStrategyError::Range)
  }
  if first_match.is_none()&&lo<=observed_spread_bps&&observed_spread_bps<hi{
   first_match=Some((lo,hi,a,b))
  }
  last=Some((lo,hi,a,b));
  prev_end=Some(hi);
 }
 let (lo,hi,a,b)=first_match.or(last).ok_or(LegacyStrategyError::Missing)?;
 let t=((observed_spread_bps-lo)/(hi-lo)).clamp(0.0,1.0);
 let target=a+(b-a)*t;
 if !target.is_finite(){return Err(LegacyStrategyError::Range)}
 // Python round(x,2) uses ties-to-even; binary midpoints still require
 // separate multi-segment parity certification before runtime cutover.
 let scaled=target*100.0;
 let lower=scaled.floor();
 let frac=scaled-lower;
 let cents=if frac>0.5||(frac==0.5&&(lower as u64)%2==1){
  lower+1.0
 }else{lower};
 Ok(cents/100.0)
}

/// Scanner.py uses scale_in_hints.min_depth_usdt as a liquidity threshold
/// even when scale_in_hints.enabled is false. This is NOT an upsize order.
#[derive(Clone,Copy,Debug,PartialEq)]
pub struct ScannerDepthPreview{
 pub required_min_trade_usdt:f64,
 pub is_upsize_signal:bool,pub may_change_position:bool,
}
pub fn inspect_scanner_min_depth_readonly(
 cfg:&NativeTradingConfig
)->Result<ScannerDepthPreview,LegacyStrategyError>{
 valid(cfg)?;
 let hints=sec(cfg,"scale_in_hints")?;
 let depth=num(hints,"min_depth_usdt")?;
 let adaptive=sec(cfg,"adaptive_entry")?;
 let min=num(adaptive,"min_usdt")?;
 let risk=sec(cfg,"risk")?;
 let nominal=num(risk,"futures_order_usdt")?;
 if depth<0.0||min<0.0||nominal<=0.0{return Err(LegacyStrategyError::Range)}
 // Python uses the first truthy numeric field, not the enabled flag.
 let threshold=if depth>0.0{depth}else if min>0.0{min}else{nominal};
 Ok(ScannerDepthPreview{
  required_min_trade_usdt:threshold,
  is_upsize_signal:false,may_change_position:false
 })
}

#[cfg(test)]
mod tests{
 use super::*;
 fn cfg()->NativeTradingConfig{
  NativeTradingConfig::parse(include_str!("../config/native_trading_operator_baseline_r64.yaml")).unwrap()
 }
 fn observation()->LegacyCloseObservation{
  LegacyCloseObservation{
   observed_spread_bps:40.0,executable_close_spread_bps:Some(0.2),
   velocity_bps_s:Some(0.0),elapsed_seconds:25,position_units:100,
   force_close:false,futures_emergency_stop_hit:false,
  }
 }
 #[test]
 fn r98_close_policy_normal_hold_dynamic_and_emergency_scenarios(){
  let c=cfg();
  let mut o=observation();
  let a=inspect_legacy_close_readonly(&c,o).unwrap();
  assert_eq!(a.decision,CloseDecision::CloseNormal);
  assert_eq!(a.dynamic_exit_bps,0.5);
  assert!(!a.can_trade&&!a.source_exchange_attested);
  o.velocity_bps_s=Some(0.1);
  assert_eq!(inspect_legacy_close_readonly(&c,o).unwrap().decision,CloseDecision::Hold);
  o.elapsed_seconds=2;
  assert_eq!(inspect_legacy_close_readonly(&c,o).unwrap().decision,CloseDecision::Wait);
  o=observation();
  o.observed_spread_bps=50_017.5;
  o.elapsed_seconds=30;
  let v=inspect_legacy_close_readonly(&c,o).unwrap();
  assert_eq!(v.dynamic_min_lifetime_sec,208);
  assert_eq!(v.decision,CloseDecision::Wait);
  o=observation();
  o.observed_spread_bps=-51.0;
  o.executable_close_spread_bps=None;
  let v=inspect_legacy_close_readonly(&c,o).unwrap();
  assert!(v.emergency_by_spread);
  assert_eq!(v.decision,CloseDecision::CloseEmergency);
  o=observation();
  o.observed_spread_bps=20.0;
  o.velocity_bps_s=Some(-7.0);
  let v=inspect_legacy_close_readonly(&c,o).unwrap();
  assert!(v.emergency_by_velocity);
  assert_eq!(v.decision,CloseDecision::CloseEmergency);
  o.observed_spread_bps=26.0;
  let v=inspect_legacy_close_readonly(&c,o).unwrap();
  assert!(!v.emergency_by_velocity);
  assert_eq!(v.decision,CloseDecision::CloseNormal);
  o.position_units=0;
  o.force_close=true;
  assert_eq!(inspect_legacy_close_readonly(&c,o).unwrap().decision,
   CloseDecision::ManualRequired);
  o=observation();
  o.velocity_bps_s=Some(f64::NAN);
  assert_eq!(inspect_legacy_close_readonly(&c,o),
   Err(LegacyStrategyError::Range));
 }
 #[test]
 fn r98_velocity_slope_and_scanner_depth_are_only_advice(){
  let c=cfg();
  assert_eq!(inspect_telemetry_velocity_readonly(42.0,Some((35.0,100.0)),101.0),
   Ok(Some(7.0)));
  assert_eq!(inspect_telemetry_velocity_readonly(42.0,Some((35.0,101.0)),101.0),
   Ok(None));
  assert_eq!(inspect_telemetry_velocity_readonly(42.0,None,101.0),Ok(None));
  assert_eq!(inspect_scanner_min_depth_readonly(&c),Ok(ScannerDepthPreview{
   required_min_trade_usdt:200.0,is_upsize_signal:false,may_change_position:false,
  }));
  let mut drift=cfg();
  drift.raw["scale_in_hints"]["min_depth_usdt"]=Value::from(1u64);
  assert_eq!(inspect_scanner_min_depth_readonly(&drift),
   Err(LegacyStrategyError::Source));
 }
 #[test]
 fn r98_dynamic_entry_off_by_default_and_synthetic_on(){
  let source=include_str!("../config/native_trading_operator_baseline_r64.yaml");
  let c=NativeTradingConfig::parse(source).unwrap();
  assert_eq!(inspect_dynamic_entry_target_readonly(&c,50_017.5),Ok(12.0));
  let modified=source.replacen("dynamic_entry:\n  enabled: false",
   "dynamic_entry:\n  enabled: true",1);
  assert_ne!(source,modified);
  let c=NativeTradingConfig::parse(&modified).unwrap();
  assert_eq!(inspect_dynamic_entry_target_readonly(&c,35.0),Ok(40.0));
  assert_eq!(inspect_dynamic_entry_target_readonly(&c,50_017.5),Ok(270.0));
  assert_eq!(inspect_dynamic_entry_target_readonly(&c,100_000.0),Ok(500.0));
  assert_eq!(inspect_dynamic_entry_target_readonly(&c,f64::NAN),
   Err(LegacyStrategyError::Range));
 }
}
