//! R53 P1-06: typed offline Rust consumer of original public feed settings.
 //! Review only: cannot authorize subscriptions, entry, order or real PnL.
use crate::native_trading_config::NativeTradingConfig;
use serde_yaml::Value as YamlValue;
use serde_json::{Value as JsonValue,json};
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum PublicFeedPolicyError{Config,Status}
#[derive(Clone,Debug,PartialEq)]
pub struct PublicFeedPolicy{
 pub ws_enabled:bool,pub ws_quote:String,
 pub ws_top_min:u64,pub ws_top_max:u64,
 pub okx_channel:String,pub book_levels:u64,pub stale_ms:u64,
 pub telemetry_orderbook_enabled:bool,pub telemetry_levels:u64,
 pub slip_bps:f64,pub slip_bps_trade:f64,pub slip_bps_upsize:f64,
 pub telemetry_max_age_ms:u64,
 pub probe_enabled:bool,pub probe_levels:u64,
 pub telemetry_velocity_enabled:bool,
 pub reconcile_every_sec:u64,
}
fn req<'a>(v:&'a YamlValue,k:&str)->Result<&'a YamlValue,PublicFeedPolicyError>{
 v.get(k).ok_or(PublicFeedPolicyError::Config)
}
fn u(v:&YamlValue,k:&str)->Result<u64,PublicFeedPolicyError>{
 req(v,k)?.as_u64().ok_or(PublicFeedPolicyError::Config)
}
fn b(v:&YamlValue,k:&str)->Result<bool,PublicFeedPolicyError>{
 req(v,k)?.as_bool().ok_or(PublicFeedPolicyError::Config)
}
fn f(v:&YamlValue,k:&str)->Result<f64,PublicFeedPolicyError>{
 let n=req(v,k)?.as_f64().ok_or(PublicFeedPolicyError::Config)?;
 if !n.is_finite(){return Err(PublicFeedPolicyError::Config)}
 Ok(n)
}
impl PublicFeedPolicy{
 pub fn from_config(cfg:&NativeTradingConfig)->Result<Self,PublicFeedPolicyError>{
  if !cfg.source_projection_unchanged(){return Err(PublicFeedPolicyError::Config)}
  if cfg.policy.real_execution_authorized{return Err(PublicFeedPolicyError::Config)}
  let w=req(&cfg.raw,"websocket")?;
  let t=req(&cfg.raw,"telemetry")?;
  let ob=req(t,"orderbook")?;
  let pr=req(&cfg.raw,"probe")?;
  let rt=req(&cfg.raw,"runtime_reconcile")?;
  let velocity=req(t,"velocity")?;
  let quote=req(w,"quote")?.as_str().ok_or(PublicFeedPolicyError::Config)?.to_owned();
  let channel=req(w,"okx_channel")?.as_str()
   .ok_or(PublicFeedPolicyError::Config)?.to_owned();
  let secs=f(ob,"max_age_sec")?;
  if !(0.1..=120.0).contains(&secs){return Err(PublicFeedPolicyError::Config)}
  let max_age_ms=(secs*1000.0).round() as u64;
  let policy=Self{
   ws_enabled:b(w,"enabled")?,ws_quote:quote,
   ws_top_min:u(w,"top_min")?,ws_top_max:u(w,"top_max")?,
   okx_channel:channel,book_levels:u(w,"max_book_levels")?,
   stale_ms:u(w,"stale_ms")?,
   telemetry_orderbook_enabled:b(ob,"enabled")?,
   telemetry_levels:u(ob,"levels")?,
   slip_bps:f(ob,"slip_bps")?,
   slip_bps_trade:f(ob,"slip_bps_trade")?,
   slip_bps_upsize:f(ob,"slip_bps_upsize")?,
   telemetry_max_age_ms:max_age_ms,
   probe_enabled:b(pr,"enabled")?,probe_levels:u(pr,"orderbook_levels")?,
   telemetry_velocity_enabled:b(velocity,"enabled")?,
   reconcile_every_sec:u(rt,"every_sec")?
  };
  if !policy.ws_enabled||policy.ws_quote!="USDT"||
     policy.okx_channel!="books5"||
     !(1..=500).contains(&policy.ws_top_min)||
     !(1..=500).contains(&policy.ws_top_max)||
     policy.ws_top_min>policy.ws_top_max||
     !(1..=50).contains(&policy.book_levels)||
     !(100..=60_000).contains(&policy.stale_ms)||
     !(1..=100).contains(&policy.telemetry_levels)||
     !(1..=100).contains(&policy.probe_levels)||
     !(1..=3600).contains(&policy.reconcile_every_sec)||
     !(0.0..=500.0).contains(&policy.slip_bps)||
     !(0.0..=500.0).contains(&policy.slip_bps_trade)||
     !(0.0..=500.0).contains(&policy.slip_bps_upsize) {
     return Err(PublicFeedPolicyError::Config)
  }
  Ok(policy)
 }
 /// Compare declared policy to caller's validated PUBLIC PAPER snapshot.
 /// Source counts do NOT prove subscription readiness or full coverage.
 pub fn observe_readonly(&self,status:&JsonValue,now_ms:u64)
  ->Result<JsonValue,PublicFeedPolicyError>{
  let updated=status.get("updated_wall_ms").and_then(JsonValue::as_u64)
   .ok_or(PublicFeedPolicyError::Status)?;
  let frames=status.get("frames").and_then(JsonValue::as_u64)
   .ok_or(PublicFeedPolicyError::Status)?;
  if now_ms==0||updated==0||updated>now_ms.saturating_add(2_000)||
     frames==0{return Err(PublicFeedPolicyError::Status)}
  let age=now_ms.saturating_sub(updated);
  let fresh=age<=self.telemetry_max_age_ms;
  Ok(json!({
   "config_quote":self.ws_quote,
   "config_okx_channel":self.okx_channel,
   "config_ws_top_min":self.ws_top_min,
   "config_ws_top_max":self.ws_top_max,
   "config_book_levels":self.book_levels,
   "configured_stale_ms":self.stale_ms,
   "configured_telemetry_max_age_ms":self.telemetry_max_age_ms,
   "public_status_age_ms":age,
   "status_age_within_telemetry_limit":fresh,
   "public_frames_seen":frames,
   "source_complete":false,
   "exchange_account_verified":false,
   "may_update_ws_or_trade":false
  }))
 }
 /// B5: bounded selector over caller-provided PUBLIC ranked symbols and
 /// virtual owner claims. Never instructs network subscriptions by itself.
 /// Any stale/overloaded/undersized input denies NEW candidate selection,
 /// but must preserve already-modeled owned symbols for safety review.
 pub fn select_owned_public_watchlist_review(
  &self,ranked:&[String],owned:&[String],status:&JsonValue,now_ms:u64,
  queue_depth:usize,queue_capacity:usize,
 )->Result<JsonValue,PublicFeedPolicyError>{
  use std::collections::BTreeSet;
  let valid=|s:&str|!s.is_empty()&&s.len()<=32&&s.bytes().all(|b|
   b.is_ascii_uppercase()||b.is_ascii_digit());
  if ranked.len()>5_000||owned.len()>10||queue_capacity==0
   ||queue_capacity>100_000||queue_depth>queue_capacity{
   return Err(PublicFeedPolicyError::Status)
  }
  let mut seen=BTreeSet::new();
  for base in ranked{
   if !valid(base)||!seen.insert(base.as_str()){
    return Err(PublicFeedPolicyError::Status)
   }
  }
  let mut owned_seen=BTreeSet::new();
  for base in owned{
   if !valid(base)||!owned_seen.insert(base.as_str()){
    return Err(PublicFeedPolicyError::Status)
   }
  }
  if owned.len()>self.ws_top_max as usize{
   return Err(PublicFeedPolicyError::Status)
  }
  let source=self.observe_readonly(status,now_ms)?;
  let age=source["public_status_age_ms"].as_u64()
   .ok_or(PublicFeedPolicyError::Status)?;
  let queue_full=queue_depth==queue_capacity;
  let source_stale=age>self.stale_ms;
  let insufficient_ranked=ranked.len()<self.ws_top_min as usize;
  let may_consider_candidate=!queue_full&&!source_stale&&!insufficient_ranked;
  let mut selected=owned.to_vec();
  if may_consider_candidate{
   for base in ranked{
    if selected.len()>=self.ws_top_max as usize{break}
    if !owned_seen.contains(base.as_str()){selected.push(base.clone())}
   }
  }
  Ok(json!({
   "schema":"SPOTARB/B5/PUBLIC-OWNER-RETAINED-WATCHLIST/OFFLINE/V1",
   "selected_review_bases":selected,
   "owned_claims_preserved":owned.len(),
   "configured_top_min":self.ws_top_min,
   "configured_top_max":self.ws_top_max,
   "configured_stale_ms":self.stale_ms,
   "review_source_age_ms":age,
   "source_stale":source_stale,
   "queue_saturated":queue_full,
   "ranked_below_min":insufficient_ranked,
   "new_candidate_selection_review_allowed":may_consider_candidate,
   "installed_ingress_subscriptions_changed":false,
   "source_volume_and_cooldown_eligibility_certified":false,
   "owner_claims_exchange_attested":false,
   "may_trade_or_subscribe":false
  }))
 }
}

#[cfg(test)]
mod tests{
 use super::*;
 fn original()->NativeTradingConfig{
  NativeTradingConfig::parse(include_str!("../config/native_trading.yaml")).unwrap()
 }
 #[test]
 fn binds_seventeen_original_market_data_fields_without_execution(){
  let x=PublicFeedPolicy::from_config(&original()).unwrap();
  assert!(x.ws_enabled&&x.telemetry_orderbook_enabled&&x.probe_enabled);
  assert!(!x.telemetry_velocity_enabled);
  assert_eq!(x.ws_quote,"USDT");assert_eq!(x.okx_channel,"books5");
  assert_eq!((x.ws_top_min,x.ws_top_max),(20,40));
  assert_eq!(x.book_levels,5);assert_eq!(x.stale_ms,800);
  assert_eq!(x.telemetry_levels,20);assert_eq!(x.probe_levels,20);
  assert_eq!(x.slip_bps,15.0);assert_eq!(x.slip_bps_trade,5.0);
  assert_eq!(x.slip_bps_upsize,15.0);assert_eq!(x.telemetry_max_age_ms,45_000);
  assert_eq!(x.reconcile_every_sec,120);
 }
 #[test]
 fn public_paper_observation_is_only_a_diagnostic(){
  let p=PublicFeedPolicy::from_config(&original()).unwrap();
  let q=p.observe_readonly(&json!({"frames":4_896_767,
   "updated_wall_ms":100_000}),110_000).unwrap();
  assert_eq!(q["public_status_age_ms"],10_000);
  assert_eq!(q["status_age_within_telemetry_limit"],true);
  assert_eq!(q["source_complete"],false);
  assert_eq!(q["may_update_ws_or_trade"],false);
  assert_eq!(p.observe_readonly(&json!({"frames":3,
   "updated_wall_ms":100_000}),147_000).unwrap()
    ["status_age_within_telemetry_limit"],false);
 }
 #[test]
 fn altered_legacy_market_data_policy_fails_closed(){
  let orig=include_str!("../config/native_trading.yaml");
  for (before,after) in [
   ("top_min: 20","top_min: 60"),
   ("okx_channel: books5","okx_channel: unknown"),
   ("stale_ms: 800","stale_ms: 0"),
   ("max_age_sec: 45.0","max_age_sec: -1"),
   ("max_book_levels: 5","max_book_levels: 0"),
   ("websocket:\n  enabled: true\n  quote: USDT","websocket:\n  enabled: true\n  quote: BTC"),
  ]{
   let altered=orig.replace(before,after);
   assert_ne!(orig,altered);
   // Either the strict 156-key schema or this typed consumer must deny.
   if let Ok(cfg)=NativeTradingConfig::parse(&altered){
    assert_eq!(PublicFeedPolicy::from_config(&cfg),Err(PublicFeedPolicyError::Config));
   }
  }
 }
 #[test]
 fn impossible_public_status_is_not_normalized(){
  let p=PublicFeedPolicy::from_config(&original()).unwrap();
  for bad in [json!({}),json!({"frames":0,"updated_wall_ms":1}),
   json!({"frames":2,"updated_wall_ms":200_000})]{
   assert_eq!(p.observe_readonly(&bad,100_000),Err(PublicFeedPolicyError::Status));
  }
 }
}
