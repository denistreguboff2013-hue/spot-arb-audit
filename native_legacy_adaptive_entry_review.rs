//! P1-06: native adaptive_entry read-only arithmetic from legacy strategy.py.
//! Pure input snapshots, NO telemetry file I/O, position resize, or exchange order.
use crate::native_trading_config::NativeTradingConfig;
use serde_yaml::Value;
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum AdaptiveError{Denied,Missing,Range}
#[derive(Clone,Copy,Debug,PartialEq)]
pub struct AdaptiveTelemetry{
 pub maximum_entry_usdt:f64,pub maximum_trade_entry_usdt:f64,
 pub age_seconds:Option<f64>,pub trade_slippage_bps:Option<f64>,
}
#[derive(Clone,Copy,Debug,PartialEq)]
pub struct AdaptiveAdvice{
 pub wanted_usdt:f64,pub applied_usdt:f64,pub depth_ok:bool,
 pub age_ok:bool,pub slip_ok:bool,pub can_trade:bool,
 pub exchange_authenticated:bool,
}
fn field(v:&Value,k:&str)->Result<f64,AdaptiveError>{
 v.get(k).and_then(|n|n.as_f64()).filter(|n|n.is_finite())
  .ok_or(AdaptiveError::Missing)
}
fn rounded_down(v:f64,step:f64)->f64{(v/step).trunc()*step}
/// Returns None if adaptive_entry.enabled=false; Python does not use sizing
/// without enabling it. Still a diagnostic; NEVER used to select an order.
pub fn inspect_adaptive_entry_readonly(
 cfg:&NativeTradingConfig,t:AdaptiveTelemetry
)->Result<Option<AdaptiveAdvice>,AdaptiveError>{
 if !cfg.source_projection_unchanged()||cfg.policy.real_execution_authorized
  ||cfg.policy.real_daily_trades!=0{return Err(AdaptiveError::Denied)}
 let ae=cfg.raw.get("adaptive_entry").ok_or(AdaptiveError::Missing)?;
 let enabled=ae.get("enabled").and_then(Value::as_bool)
  .ok_or(AdaptiveError::Missing)?;
 if !enabled{return Ok(None)}
 let min=field(ae,"min_usdt")?;
 let max=field(ae,"max_usdt")?;
 let step=field(ae,"step_usdt")?;
 let manual_max=field(ae,"max_entry_usdt")?;
 let max_slip=field(ae,"slip_bps")?;
 let require_depth=ae.get("require_scanner_depth").and_then(Value::as_bool)
  .ok_or(AdaptiveError::Missing)?;
 let max_age=cfg.raw.get("telemetry").and_then(|s|s.get("orderbook"))
  .and_then(|s|s.get("max_age_sec")).and_then(Value::as_f64)
  .ok_or(AdaptiveError::Missing)?;
 if min<=0.0||max<min||max>100_000.0||step<=0.0||step>max
  ||manual_max<0.0||manual_max>100_000.0
  ||max_slip<0.0||max_slip>1000.0||max_age<=0.0||max_age>3600.0
  ||[t.maximum_entry_usdt,t.maximum_trade_entry_usdt].iter().any(|n|!n.is_finite()||*n<0.0)
  ||t.age_seconds.is_some_and(|n|!n.is_finite()||n<0.0)
  ||t.trade_slippage_bps.is_some_and(|n|!n.is_finite()||n<0.0){
  return Err(AdaptiveError::Range)
 }
 let source_depth=if manual_max>0.0{manual_max}else{t.maximum_entry_usdt};
 let depth_ok=source_depth>0.0;
 let age_ok=t.age_seconds.map_or(true,|age|age<=max_age);
 let slip_ok=max_slip==0.0||t.trade_slippage_bps.is_some_and(|s|s<=max_slip);
 let (wanted,mut applied)=if require_depth&&!(depth_ok&&age_ok&&slip_ok){
  (0.0,0.0)
 }else{
  let mut cap=max;
  if depth_ok{cap=cap.min(source_depth)}
  let mut wanted=rounded_down(cap.max(0.0),step);
  if wanted<min{wanted=min}
  if wanted>max{wanted=max}
  if depth_ok&&wanted>source_depth{wanted=rounded_down(source_depth,step)}
  (wanted,wanted)
 };
 // Trade book cap is optional; apply only if large enough to be meaningful.
 let trade_cap=t.maximum_trade_entry_usdt;
 if depth_ok&&trade_cap>=min&&trade_cap>=step&&applied>trade_cap{
  applied=rounded_down(trade_cap,step);
  if applied<min{applied=0.0}
 }
 Ok(Some(AdaptiveAdvice{wanted_usdt:wanted,applied_usdt:applied,
  depth_ok,age_ok,slip_ok,can_trade:false,exchange_authenticated:false}))
}

#[cfg(test)]
mod tests{
 use super::*;
 fn config(enabled:bool)->NativeTradingConfig{
  let source=include_str!("../config/native_trading_operator_baseline_r64.yaml");
  let content=if enabled{
   source.replacen("adaptive_entry:\n  enabled: false",
                   "adaptive_entry:\n  enabled: true",1)
  }else{source.to_owned()};
  assert!(!enabled||content!=source);
  NativeTradingConfig::parse(&content).unwrap()
 }
 fn data()->AdaptiveTelemetry{
  AdaptiveTelemetry{maximum_entry_usdt:170.0,
   maximum_trade_entry_usdt:90.0,age_seconds:Some(5.0),
   trade_slippage_bps:Some(1.0)}
 }
 #[test]
 fn r99_adaptive_sizing_mirrors_legacy_depth_and_trade_cap(){
  let disabled=config(false);
  assert_eq!(inspect_adaptive_entry_readonly(&disabled,data()),Ok(None));
  let enabled=config(true);
  let a=inspect_adaptive_entry_readonly(&enabled,data()).unwrap().unwrap();
  assert_eq!(a.wanted_usdt,170.0);
  assert_eq!(a.applied_usdt,90.0);
  assert!(a.depth_ok&&a.age_ok&&a.slip_ok);
  assert!(!a.can_trade&&!a.exchange_authenticated);
  let mut stale=data();
  stale.age_seconds=Some(60.0);
  let a=inspect_adaptive_entry_readonly(&enabled,stale).unwrap().unwrap();
  assert_eq!(a.applied_usdt,0.0);
  let mut no_depth=data();
  no_depth.maximum_entry_usdt=0.0;
  let a=inspect_adaptive_entry_readonly(&enabled,no_depth).unwrap().unwrap();
  assert_eq!(a.wanted_usdt,0.0);
  assert!(!a.depth_ok);
  let mut invalid=data();
  invalid.maximum_entry_usdt=f64::NAN;
  assert_eq!(inspect_adaptive_entry_readonly(&enabled,invalid),
   Err(AdaptiveError::Range));
 }
 #[test]
 fn r99_adaptive_denies_in_memory_source_drift(){
  let mut c=config(true);
  c.raw["adaptive_entry"]["max_usdt"]=Value::from(500u64);
  assert_eq!(inspect_adaptive_entry_readonly(&c,data()),
   Err(AdaptiveError::Denied));
 }
}
