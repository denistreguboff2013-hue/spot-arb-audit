//! Astra B4–B7 (R627+): OFFLINE cross-consumer negative/positive policy checks.
//! No API, no PAPER child, no DB, no runtime subscriptions and no order authority.
use spotarb_native_market_core::{
 native_trading_config::NativeTradingConfig,
 native_paper_config_bridge::verify_public_paper_unwired_runtime_features,
 native_public_feed_policy_review::{PublicFeedPolicy,PublicFeedPolicyError},
 native_candidate_holdoff_review::{CandidateHoldoffPolicy,CandidateClockClaims,HoldoffError},
 native_paper_leg_failure_review::{LegFailurePhase,inspect_paper_leg_failure},
};
use serde_json::json;
fn public()->NativeTradingConfig {
 NativeTradingConfig::parse(include_str!("../config/native_trading.yaml")).unwrap()
}
fn watch()->PublicFeedPolicy{PublicFeedPolicy::from_config(&public()).unwrap()}
fn clocks()->CandidateHoldoffPolicy{
 CandidateHoldoffPolicy::from_config(&public()).unwrap()
}
fn ranked(n:usize)->Vec<String>{(0..n).map(|i|format!("T{i:03}")).collect()}
fn claims()->CandidateClockClaims{
 CandidateClockClaims{last_symbol_entry_ms:None,last_close_ms:None,
 last_scan_ms:None,last_local_lock_ms:None}
}
fn status()->serde_json::Value{json!({"frames":4,"updated_wall_ms":100_000})}
#[test]fn r627_feed_owns_open_even_if_outside_ranked_top(){
 let p=watch();let owner=vec!["OWNED".to_owned()];
 let r=p.select_owned_public_watchlist_review(&ranked(50),&owner,&status(),100_500,0,32).unwrap();
 let list=r["selected_review_bases"].as_array().unwrap();
 assert_eq!(list.len(),40);
 assert_eq!(list[0],"OWNED");
 assert_eq!(list[1],"T000");
 assert_eq!(list[39],"T038");
 assert_eq!(r["new_candidate_selection_review_allowed"],true);
 assert_eq!(r["may_trade_or_subscribe"],false);
}
#[test]fn r628_feed_stale_denies_new_without_dropping_owner(){
 let r=watch().select_owned_public_watchlist_review(
  &ranked(50),&["OWNED".into()],&status(),101_500,0,32).unwrap();
 assert_eq!(r["source_stale"],true);
 assert_eq!(r["selected_review_bases"],json!(["OWNED"]));
 assert_eq!(r["new_candidate_selection_review_allowed"],false);
}
#[test]fn r629_feed_overload_preserves_owner_and_no_new_candidate(){
 let r=watch().select_owned_public_watchlist_review(
  &ranked(50),&["OWNED".into()],&status(),100_500,32,32).unwrap();
 assert_eq!(r["queue_saturated"],true);
 assert_eq!(r["selected_review_bases"],json!(["OWNED"]));
 assert_eq!(r["may_trade_or_subscribe"],false);
}
#[test]fn r630_feed_undersized_source_retains_owners(){
 let r=watch().select_owned_public_watchlist_review(
  &ranked(19),&["OWNED".into()],&status(),100_500,0,32).unwrap();
 assert_eq!(r["ranked_below_min"],true);
 assert_eq!(r["selected_review_bases"],json!(["OWNED"]));
}
#[test]fn r631_feed_untrusted_duplicates_bad_symbols_and_bad_queue_denied(){
 let p=watch();
 let mut bad=ranked(25);bad[1]=bad[0].clone();
 assert_eq!(p.select_owned_public_watchlist_review(
    &bad,&[],&status(),100_500,0,32),Err(PublicFeedPolicyError::Status));
 assert_eq!(p.select_owned_public_watchlist_review(
    &ranked(50),&["bad".into()],&status(),100_500,0,32),
    Err(PublicFeedPolicyError::Status));
 assert_eq!(p.select_owned_public_watchlist_review(
    &ranked(50),&["OWNED".into()],&status(),100_500,33,32),
    Err(PublicFeedPolicyError::Status));
}
#[test]fn r632_four_clocks_each_block_independent(){
 let p=clocks();
 let names=[0,1,2,3];
 for i in names{
  let mut c=claims();
  match i{
   0=>c.last_symbol_entry_ms=Some(290_000),
   1=>c.last_close_ms=Some(290_000),
   2=>c.last_scan_ms=Some(290_000),
   3=>c.last_local_lock_ms=Some(290_000), _=>unreachable!()
  }
  let r=p.inspect_four_clocks_readonly("BTC",300_000,c).unwrap();
  assert_eq!(r["would_deny_local_candidate"],true);
  assert_eq!(r["may_enter_paper_or_real"],false);
  let keys=["symbol_remaining_ms","after_close_remaining_ms",
   "scanner_remaining_ms","local_lock_remaining_ms"];
  for (j,k) in keys.iter().enumerate(){
   assert_eq!(r[k].as_u64().unwrap()>0,j==i);
  }
 }
}
#[test]fn r633_four_clocks_expire_at_exact_boundary(){
 let p=clocks();
 let c=CandidateClockClaims{
  last_symbol_entry_ms:Some(180_000),
  last_close_ms:Some(180_000),
  last_scan_ms:Some(280_000),
  last_local_lock_ms:Some(270_000),
 };
 let r=p.inspect_four_clocks_readonly("BTC",300_000,c).unwrap();
 for k in ["symbol_remaining_ms","after_close_remaining_ms",
   "scanner_remaining_ms","local_lock_remaining_ms"]{
   assert_eq!(r[k],0);
 }
 assert_eq!(r["would_deny_local_candidate"],false);
 assert_eq!(r["may_enter_paper_or_real"],false);
}
#[test]fn r634_four_clocks_rewind_never_clears_hold(){
 let mut c=claims();c.last_close_ms=Some(300_001);
 assert_eq!(clocks().inspect_four_clocks_readonly("BTC",300_000,c),
  Err(HoldoffError::InvalidClock));
 let mut c=claims();c.last_scan_ms=Some(0);
 assert_eq!(clocks().inspect_four_clocks_readonly("BTC",300_000,c),
  Err(HoldoffError::InvalidClock));
}
#[test]fn r635_four_clocks_replayed_snapshot_same_result_not_durable_proof(){
 let p=clocks();
 let c=CandidateClockClaims{last_symbol_entry_ms:Some(181_001),
  last_close_ms:Some(180_000),last_scan_ms:Some(280_001),
  last_local_lock_ms:Some(270_000)};
 let a=p.inspect_four_clocks_readonly("BTC",300_000,c).unwrap();
 let restored=serde_json::from_str::<serde_json::Value>(&a.to_string()).unwrap();
 let b=p.inspect_four_clocks_readonly("BTC",300_000,c).unwrap();
 assert_eq!(restored,b);
 assert_eq!(a["would_deny_local_candidate"],true);
 assert_eq!(a["durable_restored_clock_evidence_verified"],false);
}
#[test]fn r636_after_close_yaml_change_only_changes_its_clock(){
 let source=include_str!("../config/native_trading.yaml");
 let changed=source.replacen("symbol_after_close_sec: 120",
  "symbol_after_close_sec: 20",1);
 assert_ne!(source,changed);
 let q=CandidateHoldoffPolicy::from_config(
  &NativeTradingConfig::parse(&changed).unwrap()).unwrap();
 let c=CandidateClockClaims{last_symbol_entry_ms:None,last_close_ms:Some(270_000),
  last_scan_ms:None,last_local_lock_ms:None};
 let old=clocks().inspect_four_clocks_readonly("BTC",300_000,c).unwrap();
 let new=q.inspect_four_clocks_readonly("BTC",300_000,c).unwrap();
 assert!(old["after_close_remaining_ms"].as_u64().unwrap()>0);
 assert_eq!(new["after_close_remaining_ms"],0);
 assert_eq!(new["would_deny_local_candidate"],false);
 assert_eq!(new["may_enter_paper_or_real"],false);
}
#[test]fn r637_enabled_dynamic_and_adaptive_never_silent_paper_apply(){
 let base=include_str!("../config/native_trading_operator_baseline_r64.yaml");
 let mut safe=base.to_string();
 for k in ["dynamic","velocity","scale_in_hints","analytics"] {
  let from=format!("\n{k}:\n  enabled: true");
  let to=format!("\n{k}:\n  enabled: false");
  assert!(safe.contains(&from),"{k}");
  safe=safe.replacen(&from,&to,1);
 }
 let c=NativeTradingConfig::parse(&safe).unwrap();
 assert_eq!(verify_public_paper_unwired_runtime_features(&c),Ok(()));
 for k in ["dynamic_entry","adaptive_entry"] {
  let from=format!("\n{k}:\n  enabled: false");
  let to=format!("\n{k}:\n  enabled: true");
  assert!(safe.contains(&from),"{k}");
  let changed=safe.replacen(&from,&to,1);
  let c=NativeTradingConfig::parse(&changed).unwrap();
  assert!(verify_public_paper_unwired_runtime_features(&c).is_err());
 }
}
#[test]fn r638_all_four_unknown_and_partial_stages_are_no_retry(){
 for phase in [LegFailurePhase::SpotSubmitUnknown,
   LegFailurePhase::FuturesShortUnknown,LegFailurePhase::FuturesCoverUnknown,
   LegFailurePhase::SpotSaleUnknown,LegFailurePhase::SpotPartial,
   LegFailurePhase::FuturesShortPartial,LegFailurePhase::FuturesCoverPartial,
   LegFailurePhase::SpotSalePartial] {
  let v=inspect_paper_leg_failure(&public(),phase,50_000,Some(80.0)).unwrap();
  assert_eq!(v["may_retry_send_or_cancel_order"],false);
  assert_eq!(v["may_open_or_close_real_leg"],false);
  assert_eq!(v["effective_execution_strategy_wired"],false);
 }
}
