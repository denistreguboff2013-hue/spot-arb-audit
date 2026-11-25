//! R54: offline local candidate exclusion and holdoff interpretation.
 //! Not a real exchange/account check, live admission policy or trade permit.
use crate::native_trading_config::NativeTradingConfig;
use serde_yaml::Value;
use serde_json::{json,Value as JsonValue};
use std::collections::BTreeSet;

#[derive(Clone,Debug,PartialEq,Eq)]
pub enum HoldoffError{Config,InvalidCandidate,InvalidClock}

/// B6: caller-provided LOCAL timestamps. They have no owner or exchange
/// authenticity and are not durable unless separately sealed and verified.
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub struct CandidateClockClaims{
 pub last_symbol_entry_ms:Option<u64>,
 pub last_close_ms:Option<u64>,
 pub last_scan_ms:Option<u64>,
 pub last_local_lock_ms:Option<u64>,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct CandidateHoldoffPolicy{
 pub blacklisted_bases:BTreeSet<String>,
 pub symbol_cooldown_ms:u64,
 pub after_close_cooldown_ms:u64,
 pub entry_lock_ttl_ms:u64,
 pub min_lifetime_ms:u64,
 pub scan_loop_ms:u64,
}
fn integer(v:&Value,k:&str)->Result<u64,HoldoffError>{
 v.get(k).and_then(Value::as_u64).ok_or(HoldoffError::Config)
}
fn code(s:&str)->bool{
 !s.is_empty()&&s.len()<=32&&s.bytes().all(|b|
 b.is_ascii_uppercase()||b.is_ascii_digit())
}
impl CandidateHoldoffPolicy{
 pub fn from_config(cfg:&NativeTradingConfig)->Result<Self,HoldoffError>{
  if !cfg.source_projection_unchanged(){return Err(HoldoffError::Config)}
  if cfg.policy.real_execution_authorized{return Err(HoldoffError::Config)}
  let raw=&cfg.raw;
  let b=raw.get("blacklist").and_then(Value::as_sequence).ok_or(HoldoffError::Config)?;
  if b.len()>200{return Err(HoldoffError::Config)}
  let mut blacklist=BTreeSet::new();
  for item in b{
   let base=item.as_str().ok_or(HoldoffError::Config)?;
   if !code(base)||!blacklist.insert(base.to_owned()){
    return Err(HoldoffError::Config)
   }
  }
  let lim=raw.get("limits").ok_or(HoldoffError::Config)?;
  let cool=raw.get("cooldown").ok_or(HoldoffError::Config)?;
  let spread=raw.get("spread").ok_or(HoldoffError::Config)?;
  let general=raw.get("general").ok_or(HoldoffError::Config)?;
  let symbol=integer(lim,"symbol_cooldown_sec")?;
  let after=integer(cool,"symbol_after_close_sec")?;
  let lock=integer(raw,"entry_lock_ttl_sec")?;
  let lifetime=integer(spread,"min_lifetime_sec")?;
  let loop_s=integer(general,"loop_interval_sec")?;
  if !(1..=86_400).contains(&symbol)||
    !(1..=86_400).contains(&after)||
    !(1..=3600).contains(&lock)||
    !(1..=86_400).contains(&lifetime)||
    !(1..=3600).contains(&loop_s){
   return Err(HoldoffError::Config)
  }
  Ok(Self{blacklisted_bases:blacklist,
   symbol_cooldown_ms:symbol*1000,
   after_close_cooldown_ms:after*1000,
   entry_lock_ttl_ms:lock*1000,
   min_lifetime_ms:lifetime*1000,
   scan_loop_ms:loop_s*1000})
 }
 /// B6: evaluate four DISTINCT configured local clocks in one snapshot.
 /// Equality with an expiry boundary is expired (no hidden extra day wait).
 /// A wall-time rewind is an explicit error, never a reset or free pass.
 pub fn inspect_four_clocks_readonly(
  &self,base:&str,now_ms:u64,claims:CandidateClockClaims,
 )->Result<JsonValue,HoldoffError>{
  if !code(base){return Err(HoldoffError::InvalidCandidate)}
  let starts=[claims.last_symbol_entry_ms,claims.last_close_ms,
    claims.last_scan_ms,claims.last_local_lock_ms];
  if now_ms==0||starts.iter().flatten().any(|v|*v==0||*v>now_ms){
   return Err(HoldoffError::InvalidClock)
  }
  let remaining=|time:Option<u64>,period:u64|time.map(|last|
    period.saturating_sub(now_ms-last)).unwrap_or(0);
  let sym=remaining(claims.last_symbol_entry_ms,self.symbol_cooldown_ms);
  let close=remaining(claims.last_close_ms,self.after_close_cooldown_ms);
  let scan=remaining(claims.last_scan_ms,self.scan_loop_ms);
  let lock=remaining(claims.last_local_lock_ms,self.entry_lock_ttl_ms);
  let blacklist=self.blacklisted_bases.contains(base);
  Ok(json!({
   "schema":"SPOTARB/B6/FOUR-LOCAL-CLOCKS/OFFLINE/V1",
   "symbol":base,"clock_observation_ms":now_ms,
   "source_symbol_cooldown_ms":self.symbol_cooldown_ms,
   "source_after_close_cooldown_ms":self.after_close_cooldown_ms,
   "source_scanner_loop_ms":self.scan_loop_ms,
   "source_local_lock_ttl_ms":self.entry_lock_ttl_ms,
   "symbol_remaining_ms":sym,"after_close_remaining_ms":close,
   "scanner_remaining_ms":scan,"local_lock_remaining_ms":lock,
   "blacklisted":blacklist,
   "would_deny_local_candidate":blacklist||sym>0||close>0||scan>0||lock>0,
   "original_timestamps_authenticated":false,
   "durable_restored_clock_evidence_verified":false,
   "actual_portfolio_cooldown_consumer_wired":false,
   "may_enter_paper_or_real":false
  }))
 }
 pub fn configuration_only_summary(&self)->JsonValue{
  json!({
   "blacklist_count":self.blacklisted_bases.len(),
   "symbol_cooldown_ms":self.symbol_cooldown_ms,
   "after_close_cooldown_ms":self.after_close_cooldown_ms,
   "entry_lock_ttl_ms":self.entry_lock_ttl_ms,
   "min_lifetime_ms":self.min_lifetime_ms,
   "scan_loop_ms":self.scan_loop_ms,
   "actual_account_state_known":false,
   "trade_admission_allowed":false
  })
 }
 /// P1-06: source YAML entry_lock_ttl_sec applied to an explicitly
 /// untrusted local lock timestamp. This is not a durable distributed lock
 /// and cannot reserve, unblock, acquire or authorize an exchange entry.
 pub fn inspect_local_entry_lock_readonly(
  &self,base:&str,now_ms:u64,last_local_lock_ms:Option<u64>
 )->Result<JsonValue,HoldoffError>{
  if !code(base){return Err(HoldoffError::InvalidCandidate)}
  if now_ms==0||last_local_lock_ms==Some(0)
    ||last_local_lock_ms.is_some_and(|last|last>now_ms){
   return Err(HoldoffError::InvalidClock)
  }
  let elapsed=last_local_lock_ms.map(|last|now_ms-last);
  let inside=elapsed.is_some_and(|age|age<self.entry_lock_ttl_ms);
  let remaining=elapsed.map(|age|self.entry_lock_ttl_ms.saturating_sub(age));
  Ok(json!({
   "symbol":base,"source_local_entry_lock_ttl_ms":self.entry_lock_ttl_ms,
   "claimed_lock_age_ms":elapsed,"remaining_local_lock_ms":remaining,
   "would_block_claimed_local_candidate":inside,
   "lock_origin_or_owner_authenticated":false,
   "durable_reservation_recorded":false,
   "may_acquire_release_or_bypass_lock":false,
   "may_send_or_cancel_order":false
  }))
 }
 /// Advisory-only: caller-provided timestamp cannot prove an exchange fill.
 pub fn candidate_review(&self,base:&str,now_ms:u64,
  last_close_ms:Option<u64>)->Result<JsonValue,HoldoffError>{
  if !code(base){return Err(HoldoffError::InvalidCandidate)}
  if now_ms==0||last_close_ms.is_some_and(|n|n>now_ms){
   return Err(HoldoffError::InvalidClock)
  }
  let elapsed=last_close_ms.map(|ms|now_ms-ms);
  let waiting=elapsed.is_some_and(|ms|ms<self.after_close_cooldown_ms);
  let blacklisted=self.blacklisted_bases.contains(base);
  Ok(json!({
   "symbol":base,
   "blacklisted_by_legacy_policy":blacklisted,
   "last_close_timestamp_source_verified":false,
   "local_after_close_cooldown_ms":self.after_close_cooldown_ms,
   "local_symbol_cooldown_ms":self.symbol_cooldown_ms,
   "elapsed_since_claimed_close_ms":elapsed,
   "locally_inside_after_close_cooldown":waiting,
   "would_reject_by_local_filters":blacklisted||waiting,
   "allow_real_entry":false,
   "may_submit_or_cancel_order":false
  }))
 }
}

#[cfg(test)]
mod tests{
 use super::*;
 fn cfg()->NativeTradingConfig{
  NativeTradingConfig::parse(include_str!("../config/native_trading.yaml")).unwrap()
 }
 #[test]
 fn exact_legacy_cooldown_and_blacklist_are_typed(){
  let p=CandidateHoldoffPolicy::from_config(&cfg()).unwrap();
  assert_eq!(p.symbol_cooldown_ms,120_000);
  assert_eq!(p.after_close_cooldown_ms,120_000);
  assert_eq!(p.entry_lock_ttl_ms,30_000);
  assert_eq!(p.min_lifetime_ms,15_000);
  assert_eq!(p.scan_loop_ms,20_000);
  assert!(p.blacklisted_bases.contains("ZEC"));
  assert!(p.blacklisted_bases.contains("ONE"));
 }
 #[test]
 fn local_cooldown_never_means_permission_to_trade(){
  let p=CandidateHoldoffPolicy::from_config(&cfg()).unwrap();
  let blocked=p.candidate_review("ZEC",200_000,None).unwrap();
  assert_eq!(blocked["blacklisted_by_legacy_policy"],true);
  assert_eq!(blocked["allow_real_entry"],false);
  let recent=p.candidate_review("BTC",200_000,Some(100_000)).unwrap();
  assert_eq!(recent["locally_inside_after_close_cooldown"],true);
  let old=p.candidate_review("BTC",220_000,Some(100_000)).unwrap();
  assert_eq!(old["locally_inside_after_close_cooldown"],false);
  assert_eq!(old["may_submit_or_cancel_order"],false);
  assert_eq!(old["last_close_timestamp_source_verified"],false);
 }
 #[test]
 fn invalid_inputs_and_future_timestamps_fail_closed(){
  let p=CandidateHoldoffPolicy::from_config(&cfg()).unwrap();
  assert_eq!(p.candidate_review("btc",100_000,None),
   Err(HoldoffError::InvalidCandidate));
  assert_eq!(p.candidate_review("BTC",100_000,Some(100_001)),
   Err(HoldoffError::InvalidClock));
  assert_eq!(p.candidate_review("BTC",0,None),
   Err(HoldoffError::InvalidClock));
 }
 #[test]
 fn p106_source_entry_lock_ttl_changes_local_gate_not_trade_authority(){
  let source=include_str!("../config/native_trading.yaml");
  let changed=source.replacen("entry_lock_ttl_sec: 30","entry_lock_ttl_sec: 15",1);
  assert_ne!(changed,source);
  let original=CandidateHoldoffPolicy::from_config(
    &NativeTradingConfig::parse(source).unwrap()).unwrap();
  let short=CandidateHoldoffPolicy::from_config(
    &NativeTradingConfig::parse(&changed).unwrap()).unwrap();
  let lock_start=100_000;
  let check=120_000;
  let thirty=original.inspect_local_entry_lock_readonly(
    "BTC",check,Some(lock_start)).unwrap();
  let fifteen=short.inspect_local_entry_lock_readonly(
    "BTC",check,Some(lock_start)).unwrap();
  assert_eq!(thirty["source_local_entry_lock_ttl_ms"],30_000);
  assert_eq!(thirty["would_block_claimed_local_candidate"],true);
  assert_eq!(thirty["remaining_local_lock_ms"],10_000);
  assert_eq!(fifteen["source_local_entry_lock_ttl_ms"],15_000);
  assert_eq!(fifteen["would_block_claimed_local_candidate"],false);
  assert_eq!(fifteen["remaining_local_lock_ms"],0);
  for result in [thirty,fifteen]{
   assert_eq!(result["lock_origin_or_owner_authenticated"],false);
   assert_eq!(result["durable_reservation_recorded"],false);
   assert_eq!(result["may_acquire_release_or_bypass_lock"],false);
   assert_eq!(result["may_send_or_cancel_order"],false);
  }
  assert_eq!(original.inspect_local_entry_lock_readonly(
    "BTC",130_000,Some(lock_start)).unwrap()
    ["would_block_claimed_local_candidate"],false);
  assert_eq!(original.inspect_local_entry_lock_readonly(
    "BTC",130_000,None).unwrap()
    ["would_block_claimed_local_candidate"],false);
  assert_eq!(original.inspect_local_entry_lock_readonly(
    "BTC",0,None),Err(HoldoffError::InvalidClock));
  assert_eq!(original.inspect_local_entry_lock_readonly(
    "BTC",100_000,Some(100_001)),Err(HoldoffError::InvalidClock));
  assert_eq!(original.inspect_local_entry_lock_readonly(
    "BTC",100_000,Some(0)),Err(HoldoffError::InvalidClock));
  assert_eq!(original.inspect_local_entry_lock_readonly(
    "btc",100_000,None),Err(HoldoffError::InvalidCandidate));
 }
 #[test]
 fn invalid_policy_rejected_by_schema_or_typed_review(){
  let yaml=include_str!("../config/native_trading.yaml");
  for (before,after) in [
   ("symbol_after_close_sec: 120","symbol_after_close_sec: 0"),
   ("entry_lock_ttl_sec: 30","entry_lock_ttl_sec: 0"),
   ("min_lifetime_sec: 15","min_lifetime_sec: 0"),
   ("loop_interval_sec: 20","loop_interval_sec: 0"),
  ]{
   let altered=yaml.replace(before,after);
   assert_ne!(altered,yaml);
   if let Ok(cfg)=NativeTradingConfig::parse(&altered){
    assert_eq!(CandidateHoldoffPolicy::from_config(&cfg),Err(HoldoffError::Config));
   }
  }
 }
}
