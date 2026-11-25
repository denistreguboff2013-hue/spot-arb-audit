//! R61: typed legacy-YAML projection into PUBLIC PAPER model assumptions.
 //! Offline review only; no live strategy mutation, private fills or orders.
use crate::{
 native_trading_config::NativeTradingConfig,
 native_paper_config_bridge::inspect_public_paper_policy_readonly,
 native_paper_market_execution_review::PaperAssumptions,
};
use serde_json::{json,Value};

#[derive(Clone,Debug,PartialEq,Eq)]
pub enum ModelConfigError{Denied,Missing,Range}
#[derive(Clone,Debug)]
pub struct PaperModelConfigReview{
 pub model:PaperAssumptions,
 pub minimum_lifetime_ns:u64,
 pub exit_spread_bps:f64,
 pub net_target_bps:f64,
 pub config_sha256:String,
}
fn spread_field<'a>(cfg:&'a NativeTradingConfig,key:&str)->Result<&'a serde_yaml::Value,ModelConfigError>{
 cfg.raw.get("spread").and_then(|s|s.get(key))
  .ok_or(ModelConfigError::Missing)
}
impl PaperModelConfigReview{
 pub fn from_config(cfg:&NativeTradingConfig)->Result<Self,ModelConfigError>{
  if cfg.policy.real_execution_authorized||cfg.policy.real_daily_trades!=0{
   return Err(ModelConfigError::Denied)
  }
  let d=inspect_public_paper_policy_readonly(cfg).map_err(|_|ModelConfigError::Denied)?;
  let lifetime=spread_field(cfg,"min_lifetime_sec")?
   .as_u64().ok_or(ModelConfigError::Missing)?;
  let exit=spread_field(cfg,"exit_bps")?
   .as_f64().ok_or(ModelConfigError::Missing)?;
  let target=spread_field(cfg,"net_target_bps")?
   .as_f64().ok_or(ModelConfigError::Missing)?;
  if lifetime==0||lifetime>86400||
   !exit.is_finite()||!(0.0..=1000.0).contains(&exit)||
   !target.is_finite()||!(0.0..=1000.0).contains(&target)||
   d.max_age_ms==0||d.max_age_ms>60_000||
   d.base_size<=0.0||d.base_size>10.0{
   return Err(ModelConfigError::Range)
  }
  let age_ns=d.max_age_ms.checked_mul(1_000_000)
   .ok_or(ModelConfigError::Range)?;
  let lifetime_ns=lifetime.checked_mul(1_000_000_000)
   .ok_or(ModelConfigError::Range)?;
  Ok(Self{
   model:PaperAssumptions{
    quote_budget:d.base_size,min_entry_net_bps:d.required_net,
    other_cost_bps:d.cost,
    // Old YAML round_fees covers the entire modeled fee reserve;
    // subtracting another spot fee would double-count it.
    spot_buy_fee_base_bps:0.0,take_profit_quote:None,
    max_book_age_ns:age_ns,
   },
   minimum_lifetime_ns:lifetime_ns,
   exit_spread_bps:exit,net_target_bps:target,
   config_sha256:cfg.sha256.clone(),
  })
 }
 pub fn observation(&self)->Value{
  json!({
   "schema":"r61_paper_model_config_readonly_v1",
   "quote_budget_usdt":self.model.quote_budget,
   "min_entry_net_bps":self.model.min_entry_net_bps,
   "other_round_trip_cost_bps":self.model.other_cost_bps,
   "max_book_age_ms":self.model.max_book_age_ns/1_000_000,
   "minimum_lifetime_sec":self.minimum_lifetime_ns/1_000_000_000,
   "exit_spread_bps":self.exit_spread_bps,
   "target_bps":self.net_target_bps,
   "take_profit_quote_at_target":self.model.quote_budget*self.net_target_bps/10000.0,
   "source_config_sha256":self.config_sha256,
   "simulated_and_not_exchange_attested":true,
   "mutated_live_paper":false,"may_send_orders":false
  })
 }
}

/// Round nonnegative values with Python's tie-to-even integer rule.
/// The floating-point two-decimal edge requires further parity evidence
/// for arbitrary segments; the active operator segment has fixed exit=0.5.
fn r97_round_ties_even(x:f64)->f64{
 let n=x.floor();
 let frac=x-n;
 if frac>0.5||(frac==0.5&&(n as u64)%2==1){n+1.0}else{n}
}
/// READ-ONLY dynamic exit preview, ported from strategy.py::_pick_dynamic_exit.
/// Its caller supplies the CURRENT observed spread (not entry-time spread).
/// This cannot authorize a PAPER position change or real exchange order.
pub fn inspect_dynamic_exit_from_yaml(
 cfg:&NativeTradingConfig, observed_spread_bps:f64
)->Result<(f64,u64),ModelConfigError>{
 if !cfg.source_projection_unchanged()||cfg.policy.real_execution_authorized
  ||cfg.policy.real_daily_trades!=0||!observed_spread_bps.is_finite()
  ||!(-1_000_000.0..=1_000_000.0).contains(&observed_spread_bps){
  return Err(ModelConfigError::Denied)
 }
 let defaults=PaperModelConfigReview::from_config(cfg)?;
 let static_exit=defaults.exit_spread_bps;
 let static_life=defaults.minimum_lifetime_ns/1_000_000_000;
 let dynamic=cfg.raw.get("dynamic").ok_or(ModelConfigError::Missing)?;
 let enabled=dynamic.get("enabled").and_then(|v|v.as_bool())
  .ok_or(ModelConfigError::Missing)?;
 if !enabled{return Ok((static_exit,static_life))}
 let segments=dynamic.get("segments").and_then(|v|v.as_sequence())
  .ok_or(ModelConfigError::Missing)?;
 if segments.is_empty()||segments.len()>16{return Err(ModelConfigError::Range)}
 let number=|seg:&serde_yaml::Value,key:&str|
  seg.get(key).and_then(|v|v.as_f64()).ok_or(ModelConfigError::Missing);
 let mut selected=None;
 let mut last=None;
 let mut prior_to=None;
 for seg in segments{
  let fr=number(seg,"from_entry_bps")?;
  let to=number(seg,"to_entry_bps")?;
  let ex_fr=number(seg,"exit_from_bps")?;
  let ex_to=number(seg,"exit_to_bps")?;
  let lf_fr=number(seg,"minlife_from_sec")?;
  let lf_to=number(seg,"minlife_to_sec")?;
  if [fr,to,ex_fr,ex_to,lf_fr,lf_to].iter().any(|x|!x.is_finite())
   ||fr<0.0||to<=fr||to>1_000_000.0
   ||prior_to.is_some_and(|prior|fr<prior)
   ||ex_fr<0.0||ex_to<0.0||ex_fr>1_000.0||ex_to>1_000.0
   ||lf_fr<1.0||lf_to<1.0||lf_fr>86_400.0||lf_to>86_400.0{
   return Err(ModelConfigError::Range)
  }
  let row=(fr,to,ex_fr,ex_to,lf_fr,lf_to);
  if selected.is_none()&&fr<=observed_spread_bps&&observed_spread_bps<to{
   selected=Some(row);
  }
  last=Some(row);
  prior_to=Some(to);
 }
 // Python _find_segment falls back to LAST segment outside all ranges.
 let (fr,to,ex_fr,ex_to,lf_fr,lf_to)=selected.or(last)
  .ok_or(ModelConfigError::Missing)?;
 let t=((observed_spread_bps-fr)/(to-fr)).clamp(0.0,1.0);
 let ex=ex_fr+(ex_to-ex_fr)*t;
 let life=lf_fr+(lf_to-lf_fr)*t;
 Ok((r97_round_ties_even(ex*100.0)/100.0,
     r97_round_ties_even(life) as u64))
}

#[cfg(test)]
mod tests{
 use super::*;
 fn cfg()->NativeTradingConfig{
  NativeTradingConfig::parse(include_str!("../config/native_trading.yaml")).unwrap()
 }
 #[test]
 fn r97_dynamic_exit_interpolation_uses_observed_spread_and_python_rounding(){
  // Original strategy.py _pick_dynamic_exit: one legacy 35..100000 segment,
  // exit=0.5 and min life interpolates 15..400 seconds.
  let c=cfg();
  for (spread,expected_life) in [
   (0.5,15u64),(34.0,15),(35.0,15),
   (50_017.5,208),(100_000.0,400),(110_000.0,400),
  ]{
   assert_eq!(inspect_dynamic_exit_from_yaml(&c,spread),
    Ok((0.5,expected_life)),"observed spread {spread}");
  }
  // Disable via a newly parsed source, not post-parse tampering.
  let disabled=include_str!("../config/native_trading.yaml")
   .replacen("dynamic:\n  enabled: true","dynamic:\n  enabled: false",1);
  assert_ne!(disabled,include_str!("../config/native_trading.yaml"));
  let disabled=NativeTradingConfig::parse(&disabled).unwrap();
  assert_eq!(inspect_dynamic_exit_from_yaml(&disabled,100_000.0),Ok((0.5,15)));
  assert_eq!(inspect_dynamic_exit_from_yaml(&c,f64::NAN),
   Err(ModelConfigError::Denied));
  let mut forged=cfg();
  forged.raw["dynamic"]["segments"][0]["minlife_to_sec"]=
   serde_yaml::Value::from(1u64);
  assert_eq!(inspect_dynamic_exit_from_yaml(&forged,100_000.0),
   Err(ModelConfigError::Denied));
 }
 #[test]
 fn r97_authored_invalid_dynamic_segment_fails_closed(){
  let src=include_str!("../config/native_trading.yaml");
  let invalid_range=src.replacen("to_entry_bps: 100000",
   "to_entry_bps: 35",1);
  assert_ne!(invalid_range,src);
  let c=NativeTradingConfig::parse(&invalid_range).unwrap();
  assert_eq!(inspect_dynamic_exit_from_yaml(&c,100.0),
   Err(ModelConfigError::Range));
  let invalid_lifetime=src.replacen("minlife_to_sec: 400",
   "minlife_to_sec: -1",1);
  assert_ne!(invalid_lifetime,src);
  let c=NativeTradingConfig::parse(&invalid_lifetime).unwrap();
  assert_eq!(inspect_dynamic_exit_from_yaml(&c,100.0),
   Err(ModelConfigError::Range));
  let c=cfg();
  assert_eq!(inspect_dynamic_exit_from_yaml(&c,f64::INFINITY),
   Err(ModelConfigError::Denied));
 }
 #[test]
 fn paper_runtime_assumptions_come_from_legacy_yaml(){
  let p=PaperModelConfigReview::from_config(&cfg()).unwrap();
  assert_eq!(p.model.quote_budget,10.0);
  assert_eq!(p.model.min_entry_net_bps,42.0);
  assert_eq!(p.model.other_cost_bps,24.03);
  assert_eq!(p.model.max_book_age_ns,800_000_000);
  assert_eq!(p.minimum_lifetime_ns,15_000_000_000);
  assert_eq!(p.exit_spread_bps,0.5);
  assert_eq!(p.net_target_bps,12.0);
  assert_eq!(p.observation()["take_profit_quote_at_target"],0.012);
  assert_eq!(p.observation()["mutated_live_paper"],false);
 }
 #[test]
 fn real_authority_is_never_inherited(){
  let mut x=cfg();
  x.policy.real_execution_authorized=true;
  assert_eq!(PaperModelConfigReview::from_config(&x).err(),Some(ModelConfigError::Denied));
  let mut x=cfg();
  x.policy.real_daily_trades=1;
  assert_eq!(PaperModelConfigReview::from_config(&x).err(),Some(ModelConfigError::Denied));
 }
 #[test]
 fn invalid_exit_and_holding_limits_fail_closed(){
  let mut x=cfg();
  x.raw["spread"]["min_lifetime_sec"]=serde_yaml::Value::from(0);
  assert_eq!(PaperModelConfigReview::from_config(&x).err(),Some(ModelConfigError::Range));
  let mut x=cfg();
  x.raw["spread"]["exit_bps"]=serde_yaml::Value::from(-1.0);
  assert_eq!(PaperModelConfigReview::from_config(&x).err(),Some(ModelConfigError::Range));
  let mut x=cfg();
  x.raw["spread"]["net_target_bps"]=serde_yaml::Value::from(1001.0);
  assert_eq!(PaperModelConfigReview::from_config(&x).err(),Some(ModelConfigError::Range));
 }
}
