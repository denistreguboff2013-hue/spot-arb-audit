//! TRADE2A: immutable-identity, READ-ONLY supervisor recovery triage.
//! A finite C5/C6 account audit does NOT guarantee there are no active
//! orders NOW, no delayed fills, or a safe reduceOnly/unwind order.  Never
//! produce an executable intent, never restart a Binance SELL blindly.
use crate::{
 authoritative_state::{NativeStateStore,NativeStateError,NativeEntryPhase},
 market_metadata::NativeMarketMetadata,
 native_micro_trade_size::{
    preview_residual_spot_reduction,ResidualSpotStatus,MicroSizeDeny,
 },
};
use openssl::sha::sha256;
use serde::Serialize;
pub const MAX_RECOVERY_SOURCE_AGE_MS:u64=10_000;
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum RecoveryTriageError{
 MissingOrIncomplete,StaleGeneration,StaleAccountAudit,StaleMarket,
 WrongIdentity,InvalidQuote,Source(NativeStateError),Sizing(MicroSizeDeny),
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum RecoveryTriage{
 /// Even an apparently zero residual is not a final close or permission
 /// to unwind two legs: get fresh, exchange-global closure evidence.
 EqualLegsManualCloseReview,
 /// This can be a candidate only, requires human action after rechecks.
 PartialSpotReduceOperatorOnly,
 SmallOrOffGridResidualManual,
 UntrustedPotentialOverhedgeManual,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct SupervisedRecoveryTriage{
 pub native_generation:u64,
 pub source_snapshot_at_ms:u64,
 pub source_age_ms:u64,
 pub original_source_sha256:String,
 pub original_futures_client_id:String,
 pub original_futures_order_id:String,
 pub original_spot_client_id:String,
 pub observed_short_e12:u128,
 pub unhedged_spot_e12:u128,
 pub estimate_spot_sell_quote_e12:u128,
 pub triage:RecoveryTriage,
 pub must_recheck_open_orders:bool,
 pub must_recheck_futures_position:bool,
 pub must_recheck_available_spot_balance:bool,
 pub must_recheck_all_partial_fills_and_fees:bool,
 pub actual_transfer_and_funding_history_complete:bool,
 pub current_account_flatness_proven:bool,
 pub actual_orderbook_vwap_proven:bool,
 pub may_send_order:bool,
 pub may_auto_hedge:bool,
 pub may_auto_unwind:bool,
 pub may_release_cooldown:bool,
 pub may_verify_closed:bool,
}
fn fingerprint<T:Serialize>(v:&T)->Result<String,RecoveryTriageError>{
 let input=serde_json::to_vec(v).map_err(|_|RecoveryTriageError::MissingOrIncomplete)?;
 Ok(sha256(&input).iter().map(|b|format!("{b:02x}")).collect())
}
/// No external HTTP at all. Requires a recent C5/C6 signed-GET historical
/// snapshot and fresh PUBLIC market metadata. This function only tells an
/// operator which UNSAFE condition must be investigated next.
pub fn preview_supervised_recovery(
 store:&NativeStateStore,expected_generation:u64,
 market:&NativeMarketMetadata,now_ms:u64,spot_bid_e12:u128,
)->Result<SupervisedRecoveryTriage,RecoveryTriageError>{
 let state=store.load().map_err(RecoveryTriageError::Source)?
   .ok_or(RecoveryTriageError::MissingOrIncomplete)?;
 if state.generation!=expected_generation{
    return Err(RecoveryTriageError::StaleGeneration)
 }
 if state.active.is_some(){return Err(RecoveryTriageError::WrongIdentity)}
 let e=state.entry.as_ref().ok_or(RecoveryTriageError::MissingOrIncomplete)?;
 if e.phase!=NativeEntryPhase::RecoveryRequired{
    return Err(RecoveryTriageError::WrongIdentity)
 }
 let witness=e.futures_uncertain_quarantine.as_ref()
    .ok_or(RecoveryTriageError::MissingOrIncomplete)?;
 let original_spot=e.spot_prehedge_evidence.as_ref()
    .ok_or(RecoveryTriageError::MissingOrIncomplete)?;
 let cp=e.recovery_checkpoint.as_ref()
    .ok_or(RecoveryTriageError::MissingOrIncomplete)?;
 let c5=e.futures_entry_window_evidence.as_ref()
    .ok_or(RecoveryTriageError::MissingOrIncomplete)?;
 let c6=e.prospective_joint_evidence.as_ref()
    .ok_or(RecoveryTriageError::MissingOrIncomplete)?;
 if now_ms==0||c6.captured_at_ms==0||c5.captured_at_ms>c6.captured_at_ms
    ||now_ms<c6.captured_at_ms
    ||now_ms-c6.captured_at_ms>MAX_RECOVERY_SOURCE_AGE_MS{
    return Err(RecoveryTriageError::StaleAccountAudit)
 }
 if now_ms<market.loaded_wall_ms
    ||now_ms-market.loaded_wall_ms>MAX_RECOVERY_SOURCE_AGE_MS{
    return Err(RecoveryTriageError::StaleMarket)
 }
 // This must be validated even on the suspected overhedge branch:
 // an arbitrary client/wallet report is not an eligible recovery source.
 state.quarantined_supervised_assessment()
     .map_err(RecoveryTriageError::Source)?;
 if market.base!=e.plan.base||market.spot_symbol!=e.plan.spot_symbol.replace('/',"-")
    ||market.future_symbol!=e.plan.futures_symbol{
    return Err(RecoveryTriageError::WrongIdentity)
 }
 if witness.overhedge_suspected(){
    // Return manual-only overhedge, but never even offer a sell candidate.
    // The original report is not trusted as actual execution quantity.
    let key=fingerprint(&(
      &e.plan,original_spot,&cp.ledger,witness,c5,c6,state.generation))?;
    return Ok(SupervisedRecoveryTriage{
      native_generation:state.generation,source_snapshot_at_ms:c6.captured_at_ms,
      source_age_ms:now_ms-c6.captured_at_ms,
      original_source_sha256:key,
      original_futures_client_id:cp.client_id.clone(),
      original_futures_order_id:cp.order_id.clone(),
      original_spot_client_id:original_spot.spot_client_id.clone(),
      observed_short_e12:cp.ledger.filled_qty_atoms(),
      unhedged_spot_e12:0,estimate_spot_sell_quote_e12:0,
      triage:RecoveryTriage::UntrustedPotentialOverhedgeManual,
      must_recheck_open_orders:true,must_recheck_futures_position:true,
      must_recheck_available_spot_balance:true,
      must_recheck_all_partial_fills_and_fees:true,
      actual_transfer_and_funding_history_complete:false,
      current_account_flatness_proven:false,actual_orderbook_vwap_proven:false,
      may_send_order:false,may_auto_hedge:false,may_auto_unwind:false,
      may_release_cooldown:false,may_verify_closed:false,
    })
 }
 let reduction=preview_residual_spot_reduction(
     &state,market,now_ms,spot_bid_e12)
     .map_err(RecoveryTriageError::Sizing)?;
 let verdict=match reduction.status{
   ResidualSpotStatus::NoResidual=>RecoveryTriage::EqualLegsManualCloseReview,
   ResidualSpotStatus::ExactLotCandidateOnly=>RecoveryTriage::PartialSpotReduceOperatorOnly,
   ResidualSpotStatus::ManualDustOrBelowMinimum|ResidualSpotStatus::ManualGridMismatch=>
      RecoveryTriage::SmallOrOffGridResidualManual,
   ResidualSpotStatus::ManualSuspectedOverhedge=>
      RecoveryTriage::UntrustedPotentialOverhedgeManual,
 };
 let key=fingerprint(&(
    &e.plan,original_spot,&cp.ledger,witness,c5,c6,state.generation))?;
 Ok(SupervisedRecoveryTriage{
  native_generation:state.generation,source_snapshot_at_ms:c6.captured_at_ms,
  source_age_ms:now_ms-c6.captured_at_ms,original_source_sha256:key,
  original_futures_client_id:cp.client_id.clone(),
  original_futures_order_id:cp.order_id.clone(),
  original_spot_client_id:original_spot.spot_client_id.clone(),
  observed_short_e12:reduction.futures_short_e12,
  unhedged_spot_e12:reduction.unmatched_spot_e12,
  estimate_spot_sell_quote_e12:reduction.estimated_spot_sell_quote_e12,
  triage:verdict,
  must_recheck_open_orders:true,
  must_recheck_futures_position:true,
  must_recheck_available_spot_balance:true,
  must_recheck_all_partial_fills_and_fees:true,
  actual_transfer_and_funding_history_complete:false,
  current_account_flatness_proven:false,
  actual_orderbook_vwap_proven:false,
  may_send_order:false,
  may_auto_hedge:false,
  may_auto_unwind:false,
  may_release_cooldown:false,
  may_verify_closed:false,
 })
}
