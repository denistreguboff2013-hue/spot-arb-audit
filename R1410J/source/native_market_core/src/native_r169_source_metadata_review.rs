//! R169: source-derived LOCAL previews for remaining YAML controls.
//! No actual scheduler, market subscriptions, Telegram I/O or order execution.
use crate::native_trading_config::NativeTradingConfig;
use crate::native_candidate_holdoff_review::CandidateHoldoffPolicy;
use crate::native_legacy_adaptive_entry_review::{
    inspect_adaptive_entry_readonly, AdaptiveTelemetry,
};
use std::collections::BTreeSet;

#[derive(Clone,Debug,PartialEq,Eq)]
pub enum R169Error { Source, Schema, Range }
fn root<'a>(cfg:&'a NativeTradingConfig,section:&str)
 ->Result<&'a serde_yaml::Value,R169Error>{
 if !cfg.source_projection_unchanged()||cfg.policy.real_execution_authorized
  ||cfg.policy.real_daily_trades!=0{return Err(R169Error::Source)}
 cfg.raw.get(section).ok_or(R169Error::Schema)
}
/// Reflect main.py and scanner.py loop interval; no real timer is modified.
pub fn inspect_local_scanner_tick(cfg:&NativeTradingConfig,
 elapsed_ms:u64)->Result<bool,R169Error>{
 root(cfg,"general")?;
 let p=CandidateHoldoffPolicy::from_config(cfg).map_err(|_|R169Error::Schema)?;
 Ok(elapsed_ms>=p.scan_loop_ms)
}
#[derive(Clone,Debug)]
pub struct PublicMarketFixture<'a>{
 pub base:&'a str,pub quote:&'a str,
 pub spot:bool,pub derivative:bool,
}
/// Pure subset of exchanges.py::build_common_markets quote intersection.
/// Caller-provided fixtures are NOT real loaded exchange markets.
pub fn inspect_quote_universe(cfg:&NativeTradingConfig,
 markets:&[PublicMarketFixture<'_>])->Result<Vec<String>,R169Error>{
 let general=root(cfg,"general")?;
 let quote=general.get("base_quote").and_then(serde_yaml::Value::as_str)
  .ok_or(R169Error::Schema)?;
 if quote.is_empty()||quote.len()>12||!quote.bytes().all(|x|x.is_ascii_uppercase()){
  return Err(R169Error::Range)
 }
 let mut b=BTreeSet::new();let mut o=BTreeSet::new();
 for row in markets{
  if row.quote.to_ascii_uppercase()!=quote{continue}
  if row.base.is_empty()||row.base.len()>32
   ||!row.base.bytes().all(|x|x.is_ascii_uppercase()||x.is_ascii_digit()){
   return Err(R169Error::Range)
  }
  if row.derivative{b.insert(row.base);}
  if row.spot{o.insert(row.base);}
 }
 Ok(b.intersection(&o).map(|x|(*x).to_owned()).collect())
}
/// Reuse R99 calculated advice but honor Python tg_report flags;
/// this text is LOCAL diagnostic only, not actually sent to Telegram.
pub fn inspect_adaptive_text(cfg:&NativeTradingConfig,t:AdaptiveTelemetry)
 ->Result<String,R169Error>{
 let ae=root(cfg,"adaptive_entry")?;
 let flags=ae.get("tg_report").ok_or(R169Error::Schema)?;
 let wanted=flags.get("show_wanted").and_then(serde_yaml::Value::as_bool)
  .ok_or(R169Error::Schema)?;
 let applied=flags.get("show_applied").and_then(serde_yaml::Value::as_bool)
  .ok_or(R169Error::Schema)?;
 let advice=inspect_adaptive_entry_readonly(cfg,t)
  .map_err(|_|R169Error::Source)?;
 let Some(a)=advice else{return Ok(String::new())};
 let mut lines=vec!["📊 Стакан / макс. вход (аналитика)".to_owned()];
 if wanted{lines.push(format!("• wanted: {:.0} USDT (лесенка)",a.wanted_usdt));}
 if applied{lines.push(format!("• applied: {:.0} USDT (как на реале)",a.applied_usdt));}
 if a.applied_usdt<=0.0{lines.push("• ⛔ вход запрещён (depth/age/slip_trade/лимиты)".into());}
 Ok(lines.join("\n"))
}
/// Mirrors strategy.py : if now-last_hold >= hold_log_every then emit HOLD log.
/// Pure timestamp decision, no log write, clock access, or Telegram event.
pub fn inspect_hold_log_tick(cfg:&NativeTradingConfig,
 elapsed_ms:u64)->Result<bool,R169Error>{
 let v=root(cfg,"velocity")?;
 let sec=v.get("hold_log_every_sec").and_then(serde_yaml::Value::as_u64)
  .ok_or(R169Error::Schema)?;
 if !(1..=86400).contains(&sec){return Err(R169Error::Range)}
 Ok(elapsed_ms>=sec*1000)
}
#[cfg(test)]
mod tests{
 use super::*;
 const BASE:&str=include_str!("../config/native_trading_operator_baseline_r64.yaml");
 fn cfg(s:&str)->NativeTradingConfig{NativeTradingConfig::parse(s).unwrap()}
 fn replace_once(src:&str,old:&str,new:&str)->String{
  assert_eq!(src.matches(old).count(),1,"unique source change: {old}");
  let s=src.replacen(old,new,1);assert_ne!(src,s);s
 }
 fn data()->AdaptiveTelemetry{AdaptiveTelemetry{
  maximum_entry_usdt:170.0,maximum_trade_entry_usdt:90.0,
  age_seconds:Some(5.0),trade_slippage_bps:Some(1.0)
 }}
 fn adaptive()->String{
  replace_once(BASE,"adaptive_entry:\n  enabled: false",
   "adaptive_entry:\n  enabled: true")
 }
 #[test]
 fn r169_01_general_loop_interval_changes_local_tick(){
  let x=cfg(BASE);
  let changed=cfg(&replace_once(BASE,"loop_interval_sec: 20","loop_interval_sec: 12"));
  assert!(!inspect_local_scanner_tick(&x,15_000).unwrap());
  assert!(inspect_local_scanner_tick(&changed,15_000).unwrap());
  assert_eq!(CandidateHoldoffPolicy::from_config(&changed).unwrap().scan_loop_ms,12_000);
 }
 #[test]
 fn r169_02_general_base_quote_changes_both_venue_intersection(){
  let rows=[
   PublicMarketFixture{base:"BTC",quote:"USDT",spot:true,derivative:false},
   PublicMarketFixture{base:"BTC",quote:"USDT",spot:false,derivative:true},
   PublicMarketFixture{base:"ETH",quote:"USDC",spot:true,derivative:false},
   PublicMarketFixture{base:"ETH",quote:"USDC",spot:false,derivative:true},
   PublicMarketFixture{base:"SOL",quote:"USDC",spot:true,derivative:false},
  ];
  let initial=inspect_quote_universe(&cfg(BASE),&rows).unwrap();
  let mutated=cfg(&replace_once(BASE,"base_quote: USDT","base_quote: USDC"));
  assert_eq!(initial,vec!["BTC"]);
  assert_eq!(inspect_quote_universe(&mutated,&rows).unwrap(),vec!["ETH"]);
 }
 #[test]
 fn r169_03_adaptive_show_wanted_changes_local_text(){
  let on=adaptive();
  let x=inspect_adaptive_text(&cfg(&on),data()).unwrap();
  let y=inspect_adaptive_text(&cfg(&replace_once(&on,"show_wanted: true",
   "show_wanted: false")),data()).unwrap();
  assert!(x.contains("wanted: 170"));assert!(!y.contains("wanted:"));
  assert!(y.contains("applied: 90"));
 }
 #[test]
 fn r169_04_adaptive_show_applied_changes_local_text(){
  let on=adaptive();
  let x=inspect_adaptive_text(&cfg(&on),data()).unwrap();
  let y=inspect_adaptive_text(&cfg(&replace_once(&on,"show_applied: true",
   "show_applied: false")),data()).unwrap();
  assert!(x.contains("applied: 90"));assert!(!y.contains("applied:"));
  assert!(y.contains("wanted: 170"));
 }
 #[test]
 fn r169_05_hold_log_every_changes_local_cadence(){
  let initial=inspect_hold_log_tick(&cfg(BASE),30_000).unwrap();
  let mutated=cfg(&replace_once(BASE,"hold_log_every_sec: 60",
   "hold_log_every_sec: 20"));
  assert!(!initial);assert!(inspect_hold_log_tick(&mutated,30_000).unwrap());
  assert!(!inspect_hold_log_tick(&mutated,19_999).unwrap());
 }
 #[test]
 fn r169_06_no_adaptive_when_disabled_and_raw_drift_denied(){
  assert_eq!(inspect_adaptive_text(&cfg(BASE),data()),Ok(String::new()));
  let mut bad=cfg(&adaptive());
  bad.raw["adaptive_entry"]["tg_report"]["show_wanted"]=
   serde_yaml::Value::from(false);
  assert_eq!(inspect_adaptive_text(&bad,data()),Err(R169Error::Source));
 }
 #[test]
 fn r169_07_python_velocity_fallback_is_unreachable_under_full_schema(){
  // Python _vel_cfg falls back to scale_in_hints.min_vel_bps_s ONLY when
  // velocity.hold_pos_bps_s is absent. Rust schema requires the latter.
  let missing=replace_once(BASE,"  hold_pos_bps_s: 0.06\n","");
  assert!(NativeTradingConfig::parse(&missing).is_err());
  // Do NOT credit scale_in_hints.min_vel_bps_s to any consumer.
 }
}
