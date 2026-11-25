//! Astra P1-06 / R103: compile immutable typed strategy from an authenticated
//! parsed *local* YAML identity ONCE. This is PAPER review math, never real
//! exchange authorization, live deployment or evidence of completed trades.
use crate::native_trading_config::NativeTradingConfig;
use crate::native_paper_model_config_review::PaperModelConfigReview;
use crate::native_legacy_strategy_policy_review::{
 LegacyCloseObservation,LegacyClosePreview,LegacyStrategyError,
};
use crate::position_lifecycle::{CloseTriggerInput,evaluate_close_trigger};
use serde_yaml::Value;
#[derive(Clone,Copy,Debug)]
struct Segment{from:f64,to:f64,exit_from:f64,exit_to:f64,life_from:f64,life_to:f64}
#[derive(Clone,Debug)]
pub struct PreparedExitPolicyReview{
 source_sha256:String,default_exit:f64,default_life:u64,
 dynamic_enabled:bool,segments:Vec<Segment>,
 velocity_enabled:bool,hold:f64,negative:f64,emergency_spread:f64,emergency_guard:f64,
}
fn number(v:&Value,key:&str)->Result<f64,LegacyStrategyError>{
 let x=v.get(key).and_then(Value::as_f64).ok_or(LegacyStrategyError::Missing)?;
 if !x.is_finite(){return Err(LegacyStrategyError::Range)}
 Ok(x)
}
fn round_even(x:f64)->f64{
 let n=x.floor();let fraction=x-n;
 if fraction>0.5||(fraction==0.5&&(n as u64)%2==1){n+1.0}else{n}
}
impl PreparedExitPolicyReview{
 pub fn compile(config:&NativeTradingConfig)->Result<Self,LegacyStrategyError>{
  if !config.source_projection_unchanged()||config.policy.real_execution_authorized
    ||config.policy.real_daily_trades!=0{
   return Err(LegacyStrategyError::Source)
  }
  let defaults=PaperModelConfigReview::from_config(config)
   .map_err(|_|LegacyStrategyError::Source)?;
  let dynamic=config.raw.get("dynamic").ok_or(LegacyStrategyError::Missing)?;
  let enabled=dynamic.get("enabled").and_then(Value::as_bool)
   .ok_or(LegacyStrategyError::Missing)?;
  let mut segments=Vec::new();
  if enabled{
   let rows=dynamic.get("segments").and_then(Value::as_sequence)
    .ok_or(LegacyStrategyError::Missing)?;
   if rows.is_empty()||rows.len()>16{return Err(LegacyStrategyError::Range)}
   let mut last=None;
   for row in rows{
    let s=Segment{
     from:number(row,"from_entry_bps")?,to:number(row,"to_entry_bps")?,
     exit_from:number(row,"exit_from_bps")?,exit_to:number(row,"exit_to_bps")?,
     life_from:number(row,"minlife_from_sec")?,life_to:number(row,"minlife_to_sec")?,
    };
    if s.from<0.0||s.to<=s.from||s.to>1_000_000.0
     ||last.is_some_and(|b|s.from<b)
     ||[s.exit_from,s.exit_to].iter().any(|n|*n<0.0||*n>1_000.0)
     ||[s.life_from,s.life_to].iter().any(|n|*n<1.0||*n>86_400.0){
     return Err(LegacyStrategyError::Range)
    }
    last=Some(s.to);segments.push(s);
   }
  }
  let vel=config.raw.get("velocity").ok_or(LegacyStrategyError::Missing)?;
  let velocity_enabled=vel.get("enabled").and_then(Value::as_bool)
   .ok_or(LegacyStrategyError::Missing)?;
  let hold=number(vel,"hold_pos_bps_s")?;
  let negative=number(vel,"emergency_neg_bps_s")?;
  let emergency_spread=number(vel,"emergency_spread_bps")?;
  let emergency_guard=number(vel,"emergency_guard_spread_bps")?;
  if hold<0.0||negative>=0.0||emergency_spread>=0.0
   ||emergency_guard<0.0||emergency_guard>10_000.0{
   return Err(LegacyStrategyError::Range)
  }
  Ok(Self{
   source_sha256:config.sha256.clone(),
   default_exit:defaults.exit_spread_bps,
   default_life:defaults.minimum_lifetime_ns/1_000_000_000,
   dynamic_enabled:enabled,segments,velocity_enabled,hold,negative,
   emergency_spread,emergency_guard,
  })
 }
 pub fn source_sha256(&self)->&str{&self.source_sha256}
 pub fn inspect(&self,o:LegacyCloseObservation)
  ->Result<LegacyClosePreview,LegacyStrategyError>{
  if !o.observed_spread_bps.is_finite()
    ||!(-1_000_000.0..=1_000_000.0).contains(&o.observed_spread_bps)
    ||o.velocity_bps_s.is_some_and(|n|!n.is_finite())
    ||o.executable_close_spread_bps.is_some_and(|n|!n.is_finite()){
   return Err(LegacyStrategyError::Range)
  }
  let (exit,life)=if self.dynamic_enabled{
   let s=self.segments.iter().find(|s|
    s.from<=o.observed_spread_bps&&o.observed_spread_bps<s.to)
    .or_else(||self.segments.last()).ok_or(LegacyStrategyError::Range)?;
   let t=((o.observed_spread_bps-s.from)/(s.to-s.from)).clamp(0.0,1.0);
   let ex=s.exit_from+(s.exit_to-s.exit_from)*t;
   let life=s.life_from+(s.life_to-s.life_from)*t;
   (round_even(ex*100.0)/100.0,round_even(life) as u64)
  }else{(self.default_exit,self.default_life)};
  let emergency_by_spread=o.observed_spread_bps<=self.emergency_spread;
  let emergency_by_velocity=self.velocity_enabled&&o.velocity_bps_s.is_some_and(|v|
    v<=self.negative&&o.observed_spread_bps<=self.emergency_guard);
  let emergency=o.force_close||o.futures_emergency_stop_hit
   ||emergency_by_spread||emergency_by_velocity;
  let decision=evaluate_close_trigger(CloseTriggerInput{
   emergency,amount_units:o.position_units,lifetime_sec:o.elapsed_seconds,
   min_lifetime_sec:life,close_spread_bps:o.executable_close_spread_bps,
   exit_bps:exit,
   velocity_bps_s:if self.velocity_enabled{o.velocity_bps_s}else{None},
   hold_positive_bps_s:if self.velocity_enabled{self.hold}else{0.0},
  });
  Ok(LegacyClosePreview{
   dynamic_exit_bps:exit,dynamic_min_lifetime_sec:life,
   emergency_by_spread,emergency_by_velocity,decision,
   can_trade:false,source_exchange_attested:false,
  })
 }
}

#[cfg(test)]
mod tests{
 use super::*;
 use crate::native_legacy_strategy_policy_review::inspect_legacy_close_readonly;
 fn baseline()->NativeTradingConfig{
  NativeTradingConfig::parse(include_str!("../config/native_trading_operator_baseline_r64.yaml")).unwrap()
 }
 fn frame(spread:f64,vel:Option<f64>,age:u64)->LegacyCloseObservation{
  LegacyCloseObservation{
   observed_spread_bps:spread,executable_close_spread_bps:Some(0.2),
   velocity_bps_s:vel,elapsed_seconds:age,position_units:100,
   force_close:false,futures_emergency_stop_hit:false,
  }
 }
 #[test]
 fn p106_r103_prepared_math_matches_source_review_across_dynamic_velocity_cases(){
  let cfg=baseline();
  let prepared=PreparedExitPolicyReview::compile(&cfg).unwrap();
  for (spread,velocity,age) in [
   (0.5,Some(0.0),20),(34.0,Some(0.0),20),
   (35.0,Some(0.0),15),(50_017.5,Some(0.0),30),
   (100_000.0,Some(0.0),400),(110_000.0,Some(0.0),400),
   (20.0,Some(-7.0),0),(-51.0,None,0),
   (26.0,Some(-7.0),30),(40.0,Some(0.1),30)
  ]{
   let o=frame(spread,velocity,age);
   assert_eq!(prepared.inspect(o),inspect_legacy_close_readonly(&cfg,o),
    "spread={spread} velocity={velocity:?}");
  }
 }
 #[test]
 fn p106_r103_prepared_denies_invalid_yaml_before_any_frame(){
  let mut c=baseline();
  c.raw["velocity"]["hold_pos_bps_s"]=Value::from(123.0);
  assert_eq!(PreparedExitPolicyReview::compile(&c).err(),
    Some(LegacyStrategyError::Source));
  let src=include_str!("../config/native_trading_operator_baseline_r64.yaml");
  let bad=src.replacen("minlife_to_sec: 400","minlife_to_sec: -1",1);
  assert_ne!(src,bad);
  let c=NativeTradingConfig::parse(&bad).unwrap();
  assert_eq!(PreparedExitPolicyReview::compile(&c).err(),
    Some(LegacyStrategyError::Range));
  let disabled=src.replacen("dynamic:\n  enabled: true",
     "dynamic:\n  enabled: false",1);
  assert_ne!(src,disabled);
  let cfg=NativeTradingConfig::parse(&disabled).unwrap();
  let prepared=PreparedExitPolicyReview::compile(&cfg).unwrap();
  assert_eq!(prepared.inspect(frame(50_017.5,Some(0.0),20)).unwrap().dynamic_min_lifetime_sec,15);
 }
 #[test]
 fn a_b3_velocity_enabled_false_disables_only_velocity_branch(){
  let source=include_str!("../config/native_trading_operator_baseline_r64.yaml");
  let disabled=source.replacen("velocity:\n  enabled: true",
   "velocity:\n  enabled: false",1);
  assert_ne!(source,disabled);
  let enabled_cfg=NativeTradingConfig::parse(source).unwrap();
  let disabled_cfg=NativeTradingConfig::parse(&disabled).unwrap();
  let enabled=PreparedExitPolicyReview::compile(&enabled_cfg).unwrap();
  let inactive=PreparedExitPolicyReview::compile(&disabled_cfg).unwrap();
  for (spread,vel,age) in [(20.0,Some(-7.0),0),(20.0,Some(0.1),30),
      (-51.0,Some(-7.0),0),(50_017.5,None,30)]{
   let obs=frame(spread,vel,age);
   let on=enabled.inspect(obs).unwrap();
   let off=inactive.inspect(obs).unwrap();
   assert_eq!(on.dynamic_exit_bps,off.dynamic_exit_bps);
   assert_eq!(on.dynamic_min_lifetime_sec,off.dynamic_min_lifetime_sec);
   assert_eq!(on.emergency_by_spread,off.emergency_by_spread);
   assert!(!off.emergency_by_velocity);
   assert_eq!(off,inspect_legacy_close_readonly(&disabled_cfg,obs).unwrap());
   assert!(!off.can_trade&&!off.source_exchange_attested);
  }
  assert!(enabled.inspect(frame(20.0,Some(-7.0),0)).unwrap().emergency_by_velocity);
  assert!(!inactive.inspect(frame(20.0,Some(-7.0),0)).unwrap().emergency_by_velocity);
  assert!(inactive.inspect(frame(-51.0,Some(-7.0),0)).unwrap().emergency_by_spread);
 }
}
