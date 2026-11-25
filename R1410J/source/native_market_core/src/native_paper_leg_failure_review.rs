//! R70: zero-network YAML-bound PAPER leg-failure assessment.
 //! READ ONLY. Not an order plan, exchange attestation or retry permission.
use crate::{
 native_trading_config::NativeTradingConfig,
 native_exit_safety_config_review::{inspect_exit_safety_yaml,ExitReviewError},
};
use serde::{Serialize,Deserialize};
use serde_json::{json,Value};

#[derive(Clone,Copy,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(rename_all="SCREAMING_SNAKE_CASE")]
pub enum LegFailurePhase{
 SpotSubmitUnknown,SpotPartial,SpotConfirmedAwaitingHedge,
 FuturesShortUnknown,FuturesShortPartial,Hedged,
 FuturesCoverUnknown,FuturesCoverPartial,
 FuturesCoveredAwaitingSpotSale,SpotSaleUnknown,SpotSalePartial,
 PaperModeledClosed,
}
#[derive(Debug,Clone,PartialEq,Eq)]
pub enum LegFailureReviewError{Config(ExitReviewError),Malformed}
pub fn inspect_paper_leg_failure(
 cfg:&NativeTradingConfig,phase:LegFailurePhase,
 phase_age_ms:u64,observed_adverse_spread_bps:Option<f64>,
)->Result<Value,LegFailureReviewError>{
 let policy=inspect_exit_safety_yaml(cfg).map_err(LegFailureReviewError::Config)?;
 if phase_age_ms>7*86_400_000||
  observed_adverse_spread_bps.is_some_and(|x|!x.is_finite()||x.abs()>100_000.0){
  return Err(LegFailureReviewError::Malformed)
 }
 let deadline=policy["spot_hard_deadline_ms"].as_u64()
  .ok_or(LegFailureReviewError::Malformed)?;
 let gap=policy["inter_leg_gap_ms"].as_u64()
  .ok_or(LegFailureReviewError::Malformed)?;
 let stop=policy["futures_stop_spread_bps"].as_f64()
  .ok_or(LegFailureReviewError::Malformed)?;
 let unknown=matches!(phase,LegFailurePhase::SpotSubmitUnknown|
  LegFailurePhase::FuturesShortUnknown|LegFailurePhase::FuturesCoverUnknown|
  LegFailurePhase::SpotSaleUnknown);
 let partial=matches!(phase,LegFailurePhase::SpotPartial|
  LegFailurePhase::FuturesShortPartial|LegFailurePhase::FuturesCoverPartial|
  LegFailurePhase::SpotSalePartial);
 let leg_gap_exceeded=phase_age_ms>gap && matches!(phase,
  LegFailurePhase::SpotConfirmedAwaitingHedge|
  LegFailurePhase::FuturesCoveredAwaitingSpotSale);
 let chase_deadline_exceeded=phase_age_ms>deadline && matches!(phase,
  LegFailurePhase::SpotSubmitUnknown|LegFailurePhase::SpotPartial|
  LegFailurePhase::SpotSaleUnknown|LegFailurePhase::SpotSalePartial);
 let hedged=phase==LegFailurePhase::Hedged;
 let stop_review=hedged && observed_adverse_spread_bps.is_some_and(|v|v>=stop);
 let issue=if unknown{"UNKNOWN_OUTCOME_REQUIRES_AUTHORITATIVE_QUERY"}
  else if partial{"PARTIAL_REQUIRES_FILL_FEE_AND_EXPOSURE_RECONCILIATION"}
  else if leg_gap_exceeded{"INTER_LEG_GAP_EXCEEDED_UNHEDGED_MODEL"}
  else if chase_deadline_exceeded{"SPOT_CHASE_DEADLINE_EXCEEDED"}
  else if stop_review{"ADVERSE_SPREAD_STOP_REVIEW_ONLY"}
  else if phase==LegFailurePhase::PaperModeledClosed{
    "PAPER_CLOSED_MODEL_ONLY_NOT_EXCHANGE_PROVEN"
  }else{"NO_REAL_ORDER_ACTION_PAPER_ONLY"};
 let required=if unknown{
  "OWNER_BOUND_SIGNED_ORDER_GET_AND_ACCOUNT_FILLS_POSITIONS"
 }else if partial{
  "CUMULATIVE_QTY_INDIVIDUAL_FILLS_FEES_AND_ACCOUNT_BALANCE"
 }else if leg_gap_exceeded||chase_deadline_exceeded{
  "FRESH_ORDER_STATUS_EXACT_BASE_EXPOSURE_AND_OWNER_JOURNAL"
 }else if stop_review{
  "NEWER_TRUSTED_BOOK_OR_ACCOUNT_POSITION_AND_OWNER_ORDER_STATE"
 }else{
  "OWNER_JOURNAL_AND_VERIFIED_MODEL_STAGE"
 };
 Ok(json!({
  "schema":"r70_paper_leg_failure_readonly_v1",
  "source_config_sha256":cfg.sha256,
  "phase":phase,"phase_age_ms":phase_age_ms,
  "inter_leg_gap_ms":gap,"spot_chase_hard_deadline_ms":deadline,
  "futures_stop_spread_bps":stop,
  "adverse_spread_bps":observed_adverse_spread_bps,
  "unknown_result":unknown,"partial_fill":partial,
  "gap_exceeded":leg_gap_exceeded,
  "chase_deadline_exceeded":chase_deadline_exceeded,
  "emergency_review_required":stop_review,
  "issue_code":issue,"required_evidence":required,
  "position_closed_on_real_exchange_attested":false,
  "actual_exchange_fills_authenticated":false,
  "real_stop_market_order_authorized":false,
  "may_retry_send_or_cancel_order":false,
  "may_open_or_close_real_leg":false,
  "may_change_paper_position":false,
  "effective_execution_strategy_wired":false
 }))
}

#[cfg(test)]
mod tests{
 use super::*;
 fn cfg()->NativeTradingConfig{
  NativeTradingConfig::parse(include_str!("../config/native_trading.yaml")).unwrap()
 }
 #[test]
 fn r94_r70_readonly_leg_failure_cannot_use_tampered_exit_source(){
  let mut c=cfg();
  // The source YAML is already false: true is the actual forbidden mutation.
  c.raw["futures_emergency_stop"]["place_in_real"]=serde_yaml::Value::Bool(true);
  assert_eq!(
   inspect_paper_leg_failure(&c,LegFailurePhase::Hedged,100,Some(55.0)),
   Err(LegFailureReviewError::Config(ExitReviewError::Policy)));
 }
 #[test]
 fn first_leg_unknown_over_deadline_is_never_zero_or_resubmitted(){
  let r=inspect_paper_leg_failure(&cfg(),LegFailurePhase::SpotSubmitUnknown,4001,None).unwrap();
  assert_eq!(r["unknown_result"],true);
  assert_eq!(r["chase_deadline_exceeded"],true);
  assert_eq!(r["issue_code"],"UNKNOWN_OUTCOME_REQUIRES_AUTHORITATIVE_QUERY");
  assert_eq!(r["may_retry_send_or_cancel_order"],false);
  assert_eq!(r["position_closed_on_real_exchange_attested"],false);
 }
 #[test]
 fn second_leg_unknown_does_not_reauthorize_the_hedge(){
  let r=inspect_paper_leg_failure(&cfg(),LegFailurePhase::FuturesShortUnknown,5000,None).unwrap();
  assert_eq!(r["unknown_result"],true);
  assert_eq!(r["may_open_or_close_real_leg"],false);
  assert_eq!(r["required_evidence"],
   "OWNER_BOUND_SIGNED_ORDER_GET_AND_ACCOUNT_FILLS_POSITIONS");
 }
 #[test]
 fn partial_fill_on_either_entry_leg_requires_reconciliation(){
  for phase in [LegFailurePhase::SpotPartial,LegFailurePhase::FuturesShortPartial]{
   let r=inspect_paper_leg_failure(&cfg(),phase,6000,None).unwrap();
   assert_eq!(r["partial_fill"],true);
   assert_eq!(r["may_retry_send_or_cancel_order"],false);
   assert_eq!(r["issue_code"],
     "PARTIAL_REQUIRES_FILL_FEE_AND_EXPOSURE_RECONCILIATION");
  }
 }
 #[test]
 fn timed_inter_leg_gap_not_a_permission_to_emergency_trade(){
  for phase in [LegFailurePhase::SpotConfirmedAwaitingHedge,
    LegFailurePhase::FuturesCoveredAwaitingSpotSale]{
   let okay=inspect_paper_leg_failure(&cfg(),phase,1500,None).unwrap();
   assert_eq!(okay["gap_exceeded"],false);
   let late=inspect_paper_leg_failure(&cfg(),phase,1501,None).unwrap();
   assert_eq!(late["gap_exceeded"],true);
   assert_eq!(late["issue_code"],"INTER_LEG_GAP_EXCEEDED_UNHEDGED_MODEL");
   assert_eq!(late["may_open_or_close_real_leg"],false);
  }
 }
 #[test]
 fn adverse_hedged_spread_is_only_a_stop_review_trigger(){
  let prior=inspect_paper_leg_failure(&cfg(),LegFailurePhase::Hedged,100,Some(49.99)).unwrap();
  assert_eq!(prior["emergency_review_required"],false);
  let hit=inspect_paper_leg_failure(&cfg(),LegFailurePhase::Hedged,100,Some(50.0)).unwrap();
  assert_eq!(hit["emergency_review_required"],true);
  assert_eq!(hit["issue_code"],"ADVERSE_SPREAD_STOP_REVIEW_ONLY");
  assert_eq!(hit["real_stop_market_order_authorized"],false);
  assert_eq!(hit["may_open_or_close_real_leg"],false);
 }
 #[test]
 fn uncertain_or_partial_close_never_claims_flat_position(){
  for phase in [LegFailurePhase::FuturesCoverUnknown,
   LegFailurePhase::FuturesCoverPartial,LegFailurePhase::SpotSaleUnknown,
   LegFailurePhase::SpotSalePartial,LegFailurePhase::PaperModeledClosed]{
   let r=inspect_paper_leg_failure(&cfg(),phase,900,None).unwrap();
   assert_eq!(r["position_closed_on_real_exchange_attested"],false);
   assert_eq!(r["may_retry_send_or_cancel_order"],false);
  }
 }
 #[test]
 fn malformed_observation_and_real_policy_fail_closed(){
  let c=cfg();
  assert_eq!(inspect_paper_leg_failure(&c,LegFailurePhase::Hedged,100,Some(f64::NAN)),
   Err(LegFailureReviewError::Malformed));
  assert_eq!(inspect_paper_leg_failure(&c,LegFailurePhase::Hedged,8*86_400_000,None),
   Err(LegFailureReviewError::Malformed));
  let mut c=cfg();
  c.policy.real_execution_authorized=true;
  assert!(matches!(
   inspect_paper_leg_failure(&c,LegFailurePhase::SpotPartial,100,None),
   Err(LegFailureReviewError::Config(ExitReviewError::Policy))));
 }
}
