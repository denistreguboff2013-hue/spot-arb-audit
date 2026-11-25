//! R52: strict read-only binding of legacy fast_market_shadow settings.
 //! Used only by PAPER diagnostic. Not a WS deployment or trading authority.
use crate::native_trading_config::NativeTradingConfig;
use serde_json::{Value as JsonValue,json};
use serde_yaml::Value as YamlValue;

#[derive(Clone,Debug,PartialEq,Eq)]
pub enum ShadowReviewError{Config,Status}
#[derive(Clone,Debug,PartialEq)]
pub struct NativeShadowReview{
 pub enabled:bool,pub event_prefilter:bool,
 pub targeted_thread_diagnostics:bool,
 pub queue_max:u64,pub max_log_mb:u64,pub summary_sec:u64,
 pub confirmation_enabled:bool,pub confirm_cooldown_sec:u64,
 pub confirm_min_interval_sec:f64,pub confirm_timeout_sec:f64,
 pub confirm_max_queue:u64,pub confirm_max_wait_ms:u64,
 pub fleet_enabled:bool,pub group_size:u64,
 pub base_limit:u64,pub start_interval_sec:f64,
}
fn field<'a>(v:&'a YamlValue,k:&str)->Result<&'a YamlValue,ShadowReviewError>{
 v.get(k).ok_or(ShadowReviewError::Config)
}
fn boolean(v:&YamlValue,k:&str)->Result<bool,ShadowReviewError>{
 field(v,k)?.as_bool().ok_or(ShadowReviewError::Config)
}
fn unsigned(v:&YamlValue,k:&str)->Result<u64,ShadowReviewError>{
 field(v,k)?.as_u64().ok_or(ShadowReviewError::Config)
}
fn decimal(v:&YamlValue,k:&str)->Result<f64,ShadowReviewError>{
 let n=field(v,k)?.as_f64().ok_or(ShadowReviewError::Config)?;
 if !n.is_finite(){return Err(ShadowReviewError::Config)}
 Ok(n)
}
impl NativeShadowReview{
 pub fn from_config(config:&NativeTradingConfig)->Result<Self,ShadowReviewError>{
  if !config.source_projection_unchanged(){return Err(ShadowReviewError::Config)}
  if config.policy.real_execution_authorized{return Err(ShadowReviewError::Config)}
  let f=field(&config.raw,"fast_market_shadow")?;
  let t=field(f,"targeted_thread_diagnostics")?;
  let c=field(f,"targeted_confirmation")?;
  let w=field(f,"ws_fleet")?;
  let p=Self{
   enabled:boolean(f,"enabled")?,
   event_prefilter:boolean(f,"event_prefilter")?,
   targeted_thread_diagnostics:boolean(t,"enabled")?,
   queue_max:unsigned(f,"queue_max")?,
   max_log_mb:unsigned(f,"max_log_mb")?,
   summary_sec:unsigned(f,"summary_sec")?,
   confirmation_enabled:boolean(c,"enabled")?,
   confirm_cooldown_sec:unsigned(c,"cooldown_sec")?,
   confirm_min_interval_sec:decimal(c,"min_interval_sec")?,
   confirm_timeout_sec:decimal(c,"timeout_sec")?,
   confirm_max_queue:unsigned(c,"max_queue")?,
   confirm_max_wait_ms:unsigned(c,"max_wait_ms")?,
   fleet_enabled:boolean(w,"enabled")?,
   group_size:unsigned(w,"group_size")?,
   base_limit:unsigned(w,"base_limit")?,
   start_interval_sec:decimal(w,"start_interval_sec")?,
  };
  if !(64..=65536).contains(&p.queue_max)||
    !(1..=1024).contains(&p.max_log_mb)||
    !(10..=3600).contains(&p.summary_sec)||
    !(1..=3600).contains(&p.confirm_cooldown_sec)||
    !(0.1..=60.0).contains(&p.confirm_min_interval_sec)||
    !(0.05..=30.0).contains(&p.confirm_timeout_sec)||
    !(1..=1024).contains(&p.confirm_max_queue)||
    !(50..=60_000).contains(&p.confirm_max_wait_ms)||
    !(1..=200).contains(&p.group_size)||
    p.base_limit>10_000||
    !(0.1..=60.0).contains(&p.start_interval_sec){
    return Err(ShadowReviewError::Config)
  }
  Ok(p)
 }
 pub fn summarize_paper_observation(&self,status:&JsonValue)
  ->Result<JsonValue,ShadowReviewError>{
  let ws=status.get("websocket_seen").ok_or(ShadowReviewError::Status)?;
  let binance=ws.get("binance").and_then(JsonValue::as_u64)
    .ok_or(ShadowReviewError::Status)?;
  let okx=ws.get("okx").and_then(JsonValue::as_u64)
    .ok_or(ShadowReviewError::Status)?;
  if binance>10_000||okx>10_000{return Err(ShadowReviewError::Status)}
  let stream_count=binance.saturating_add(okx);
  Ok(json!({
   "policy_enabled":self.enabled,
   "public_binance_symbol_count":binance,
   "public_okx_symbol_count":okx,
   "configured_ws_group_size":self.group_size,
   "configured_ws_base_limit":self.base_limit,
   "configured_queue_max":self.queue_max,
   "configured_confirmation_queue_max":self.confirm_max_queue,
   "observed_source_authenticity_proven":false,
   "coverage_complete_proven":false,
   "private_exchange_api_not_accessed":true,
   "may_change_ws_or_trade":false,
   "combined_venue_symbol_count":stream_count
  }))
 }
}

/// R166 isolated projection of the original FastMarketShadow knobs.
/// Predicts resource/diagnostic LIMITS for caller supplied examples only.
/// This is deliberately not the installed Python/Rust ingestion scheduler,
/// and no result is an entry, subscription, order or operator permission.
#[derive(Clone,Copy,Debug)]
pub struct ShadowDryrunInputR166{
 pub eligible_bases:u64,
 pub waiting_market_events:u64,
 pub log_bytes:u64,
 pub seconds_since_summary:u64,
 pub event_hot:bool,
 pub queue_event_is_diagnostic:bool,
 pub pending_confirmations:u64,
 pub since_symbol_confirmation_ms:u64,
 pub since_global_confirmation_ms:u64,
 pub confirmation_elapsed_ms:u64,
 pub confirmation_wait_ms:u64,
}
impl ShadowDryrunInputR166{
 pub fn reference()->Self{
  Self{eligible_bases:235,waiting_market_events:4096,
   log_bytes:32*1024*1024,seconds_since_summary:60,
   event_hot:false,queue_event_is_diagnostic:true,
   pending_confirmations:7,since_symbol_confirmation_ms:5_000,
   since_global_confirmation_ms:1_000,
   confirmation_elapsed_ms:650,confirmation_wait_ms:300}
 }
}
impl NativeShadowReview{
 /// Offline diagnostic envelope. Every original fast_market_shadow option
 /// changes a DECISION/threshold in this projection, not merely an echoed
 /// source value. End-to-end live WS parity remains explicitly UNPROVEN.
 pub fn project_bounded_shadow_review_r166(
  &self,input:ShadowDryrunInputR166
 )->Result<JsonValue,ShadowReviewError>{
  if input.eligible_bases>10_000||
     input.waiting_market_events>1_000_000||
     input.pending_confirmations>1_000_000{
   return Err(ShadowReviewError::Status)
  }
  // Python ShadowWSFleet clamps group size to 1..60 and interval >=1s.
  // Neither static projection starts any WebSocket task.
  let selected=if self.fleet_enabled{
   if self.base_limit==0{input.eligible_bases}
   else{input.eligible_bases.min(self.base_limit)}
  }else{0};
  let group=self.group_size.clamp(1,60);
  let groups=selected/group+u64::from(selected%group!=0);
  let startup_ms=(groups.saturating_sub(1) as f64
    *self.start_interval_sec.max(1.0)*1000.0).ceil() as u64;
  let queue_admission=self.enabled
    &&input.waiting_market_events<self.queue_max
    &&(!self.event_prefilter||input.event_hot);
  let log_rollover_due=self.enabled
    &&input.log_bytes>=self.max_log_mb*1024*1024;
  let summary_due=self.enabled
    &&input.seconds_since_summary>=self.summary_sec;
  let sample_thread=self.enabled && self.targeted_thread_diagnostics
    &&input.queue_event_is_diagnostic;
  let cooldown_elapsed=input.since_symbol_confirmation_ms
    >=self.confirm_cooldown_sec.saturating_mul(1000);
  let global_interval_elapsed=input.since_global_confirmation_ms as f64
    >=(self.confirm_min_interval_sec*1000.0);
  let confirmation_capacity=input.pending_confirmations<self.confirm_max_queue;
  let confirmation_timed_out=input.confirmation_elapsed_ms as f64
    >self.confirm_timeout_sec*1000.0;
  let confirmation_wait_expired=input.confirmation_wait_ms
    >self.confirm_max_wait_ms;
  let confirmation_allowed=self.enabled&&self.confirmation_enabled
    &&cooldown_elapsed&&global_interval_elapsed&&confirmation_capacity
    &&!confirmation_timed_out&&!confirmation_wait_expired;
  Ok(json!({
   "schema":"SPOTARB/R166/STATIC-OFFLINE-SHADOW-ENVELOPE/V1",
   "shadow_diagnostic_active":self.enabled,
   "market_event_admitted_to_example_queue":queue_admission,
   "market_event_prefilter_rejected":self.enabled&&self.event_prefilter&&!input.event_hot,
   "example_market_queue_full":input.waiting_market_events>=self.queue_max,
   "log_rollover_due":log_rollover_due,
   "summary_due":summary_due,
   "thread_trace_sample":sample_thread,
   "confirmation_symbol_cooldown_elapsed":cooldown_elapsed,
   "confirmation_global_interval_elapsed":global_interval_elapsed,
   "confirmation_queue_has_capacity":confirmation_capacity,
   "confirmation_timeout_expired":confirmation_timed_out,
   "confirmation_wait_expired":confirmation_wait_expired,
   "confirmation_locally_eligible":confirmation_allowed,
   "fleet_selected_bases":selected,
   "fleet_shards":groups,
   "fleet_last_stagger_offset_ms":startup_ms,
   "observer_only":true,
   "configuration_values_are_applied_to_installed_ws":false,
   "authenticated_exchange_source":false,
   "can_launch_public_ws":false,
   "can_send_or_cancel_real_orders":false,
   "installed_paper_consumption_proven":false
  }))
 }
}

#[cfg(test)]
mod r166_mutation_tests{
 use super::*;
 use crate::native_trading_config::NativeTradingConfig;
 fn from_source(s:&str)->NativeShadowReview{
  let parsed=NativeTradingConfig::parse(s).unwrap();
  NativeShadowReview::from_config(&parsed).unwrap()
 }
 #[test]
 fn r166_all_sixteen_original_keys_change_offline_decision_without_io(){
  let original=include_str!("../config/native_trading.yaml");
  let baseline=from_source(original).project_bounded_shadow_review_r166(
    ShadowDryrunInputR166::reference()).unwrap();
  let switches=[
    ("fast_market_shadow:\n  enabled: true","fast_market_shadow:\n  enabled: false","shadow_diagnostic_active"),
    ("  event_prefilter: true","  event_prefilter: false",
      "market_event_prefilter_rejected"),
    ("  targeted_thread_diagnostics:\n    enabled: true",
     "  targeted_thread_diagnostics:\n    enabled: false","thread_trace_sample"),
    ("  queue_max: 4096","  queue_max: 4097","example_market_queue_full"),
    ("  max_log_mb: 32","  max_log_mb: 33","log_rollover_due"),
    ("  summary_sec: 60","  summary_sec: 61","summary_due"),
    ("  targeted_confirmation:\n    enabled: true",
     "  targeted_confirmation:\n    enabled: false","confirmation_locally_eligible"),
    ("    cooldown_sec: 5","    cooldown_sec: 6",
      "confirmation_symbol_cooldown_elapsed"),
    ("    min_interval_sec: 1.0","    min_interval_sec: 2.0",
      "confirmation_global_interval_elapsed"),
    ("    timeout_sec: 0.65","    timeout_sec: 0.6",
      "confirmation_timeout_expired"),
    ("    max_queue: 8","    max_queue: 7","confirmation_queue_has_capacity"),
    ("    max_wait_ms: 300","    max_wait_ms: 299","confirmation_wait_expired"),
    ("  ws_fleet:\n    enabled: true","  ws_fleet:\n    enabled: false",
      "fleet_selected_bases"),
    ("    group_size: 40","    group_size: 30","fleet_shards"),
    ("    base_limit: 0","    base_limit: 100","fleet_selected_bases"),
    ("    start_interval_sec: 1.0","    start_interval_sec: 2.0",
      "fleet_last_stagger_offset_ms"),
  ];
  assert_eq!(switches.len(),16);
  for (old,new,key) in switches{
   let changed=original.replacen(old,new,1);
   assert_ne!(changed,original,"missing source {old}");
   let output=from_source(&changed).project_bounded_shadow_review_r166(
     ShadowDryrunInputR166::reference()).unwrap();
   assert_ne!(baseline[key],output[key],
     "source change has no offline behavioral influence: {old} -> {new} / {key}");
   assert_eq!(output["can_send_or_cancel_real_orders"],false);
   assert_eq!(output["configuration_values_are_applied_to_installed_ws"],false);
  }
 }
 #[test]
 fn r166_static_projection_fleet_python_clamp_and_bounded_inputs(){
  let original=include_str!("../config/native_trading.yaml");
  let p=from_source(original);
  let x=p.project_bounded_shadow_review_r166(
   ShadowDryrunInputR166::reference()).unwrap();
  assert_eq!(x["fleet_selected_bases"],235);
  assert_eq!(x["fleet_shards"],6);
  assert_eq!(x["fleet_last_stagger_offset_ms"],5000);
  assert_eq!(x["example_market_queue_full"],true);
  let changed=original.replacen("    group_size: 40","    group_size: 200",1);
  let x=from_source(&changed).project_bounded_shadow_review_r166(
   ShadowDryrunInputR166::reference()).unwrap();
  assert_eq!(x["fleet_shards"],4); // old Python caps group_size at 60
  assert_eq!(x["fleet_last_stagger_offset_ms"],3000);
  let changed=original.replacen("    start_interval_sec: 1.0",
   "    start_interval_sec: 0.5",1);
  let x=from_source(&changed).project_bounded_shadow_review_r166(
   ShadowDryrunInputR166::reference()).unwrap();
  assert_eq!(x["fleet_last_stagger_offset_ms"],5000);
  let mut excess=ShadowDryrunInputR166::reference();
  excess.eligible_bases=10_001;
  assert_eq!(p.project_bounded_shadow_review_r166(excess),
   Err(ShadowReviewError::Status));
 }
 #[test]
 fn r166_never_confuses_policy_projection_with_operational_success(){
  let source=include_str!("../config/native_trading.yaml");
  let p=from_source(source);
  let x=p.project_bounded_shadow_review_r166(
   ShadowDryrunInputR166::reference()).unwrap();
  assert_eq!(x["observer_only"],true);
  assert_eq!(x["authenticated_exchange_source"],false);
  assert_eq!(x["installed_paper_consumption_proven"],false);
  assert_eq!(x["can_launch_public_ws"],false);
  assert_eq!(x["can_send_or_cancel_real_orders"],false);
 }
}
#[cfg(test)]
mod tests{
 use super::*;
 fn cfg()->NativeTradingConfig{
  NativeTradingConfig::parse(include_str!("../config/native_trading.yaml")).unwrap()
 }
 #[test]
 fn legacy_shadow_keys_are_actually_read_by_rust(){
  let p=NativeShadowReview::from_config(&cfg()).unwrap();
  assert!(p.enabled&&p.fleet_enabled&&p.confirmation_enabled);
  assert!(p.event_prefilter&&p.targeted_thread_diagnostics);
  assert_eq!(p.queue_max,4096);
  assert_eq!(p.max_log_mb,32);
  assert_eq!(p.summary_sec,60);
  assert_eq!(p.confirm_cooldown_sec,5);
  assert_eq!(p.confirm_min_interval_sec,1.0);
  assert_eq!(p.confirm_timeout_sec,0.65);
  assert_eq!(p.confirm_max_queue,8);
  assert_eq!(p.confirm_max_wait_ms,300);
  assert_eq!(p.group_size,40);
  assert_eq!(p.base_limit,0);
  assert_eq!(p.start_interval_sec,1.0);
 }
 #[test]
 fn live_public_census_does_not_grant_wss_or_order_permission(){
  let p=NativeShadowReview::from_config(&cfg()).unwrap();
  let s=json!({"websocket_seen":{"okx":234,"binance":234}});
  let r=p.summarize_paper_observation(&s).unwrap();
  assert_eq!(r["combined_venue_symbol_count"],468);
  assert_eq!(r["coverage_complete_proven"],false);
  assert_eq!(r["may_change_ws_or_trade"],false);
 }
 #[test]
 fn unsafe_shadow_config_is_rejected_without_network(){
  let yaml=include_str!("../config/native_trading.yaml");
  for (from,to) in [
   ("queue_max: 4096","queue_max: 0"),
   ("group_size: 40","group_size: 0"),
   ("max_log_mb: 32","max_log_mb: 0"),
   ("max_wait_ms: 300","max_wait_ms: 0"),
   ("timeout_sec: 0.65","timeout_sec: -1.0"),
  ]{
   let altered=yaml.replace(from,to);
   let cfg=NativeTradingConfig::parse(&altered).unwrap();
   assert_eq!(NativeShadowReview::from_config(&cfg),Err(ShadowReviewError::Config));
  }
 }
 #[test]
 fn invalid_public_source_is_denied(){
  let p=NativeShadowReview::from_config(&cfg()).unwrap();
  assert_eq!(p.summarize_paper_observation(&json!({})),
    Err(ShadowReviewError::Status));
 }
}
