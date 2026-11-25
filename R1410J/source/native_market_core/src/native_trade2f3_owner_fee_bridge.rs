//! Astra P0-02/P1-02 owner-scoped OFFLINE adapter over persisted C5/C6/B4 seals.
//! Deliberately uses legacy strict original preflight before evaluating fee rows.
//! This is additive FAIL-CLOSED work; fee-adjusted owner migration is NOT complete.
//! No network calls, egress permissions, VerifiedClosed or real PnL authority.
use crate::{
    authoritative_state::{NativeEntryPhase,NativeStateError,NativeStateStore},
    native_trade2f3_fee_adjusted_cycle_gate::{
        inspect_fee_adjusted_ordinary_four_leg_cycle,CycleGateError,
        FeeAdjustedOrdinaryCycleReview,
    },
    native_trade2f_asset_fee::SpotFeeFill,
    native_trade2f_owner_binding::original_four_leg_preflight,
};
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum OwnerFeeGateError{
    State(NativeStateError),
    Fee(CycleGateError),
    Denied(&'static str),
}
fn deny(s:&'static str)->OwnerFeeGateError{OwnerFeeGateError::Denied(s)}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct OwnerFeeCycleOfflineReport{
    pub sealed_owner_generation:u64,
    pub internally_consistent_source_shaped_fee_review:FeeAdjustedOrdinaryCycleReview,
    pub externally_attested_source_history:bool,
    pub legacy_original_gross_seal_migration_complete:bool,
    pub signed_real_account_flatness_proven:bool,
    pub verified_closed:bool,
    pub may_send_or_cancel_or_retry:bool,
}
/// Read current durable owner TWICE; an externally supplied fee-event slice
/// can only match the original stored LegFillLedger fills, never replace them.
/// The old exact C5/C6/B4 seal stays hard-gated and therefore BASE-fee owner
/// transitions are intentionally STILL denied by the old gross requirements.
/// Positive output is a nonauthoritative diagnostic ONLY.
pub fn inspect_persisted_owner_fee_cycle_offline(
    store:&NativeStateStore,
    expected_generation:u64,
    source_shaped_spot_fee_fills:&[SpotFeeFill],
)->Result<OwnerFeeCycleOfflineReport,OwnerFeeGateError>{
    let before=store.load().map_err(OwnerFeeGateError::State)?
        .ok_or(deny("Native owner file missing"))?;
    if before.generation!=expected_generation {
        return Err(deny("Native owner generation differs before fee source review"))
    }
    if before.active.is_some() {
        return Err(deny("unresolved native active/position account is not flat"))
    }
    let e=before.entry.as_ref().ok_or(deny("Native original entry missing"))?;
    if e.phase!=NativeEntryPhase::RecoveryRequired
        ||e.dryrun_close_latch.is_some() {
        return Err(deny("Native ordinary four legs are not in terminal recovery review state"))
    }
    // This intentionally blocks fee-base cases where old gross amount seals
    // have not been migrated; it must NOT be removed just to make tests green.
    original_four_leg_preflight(&before)
        .map_err(|_|deny("original sealed C5/C6/B4 four-leg owner preflight denied"))?;
    let spot_buy=&e.spot_prehedge_evidence.as_ref()
        .ok_or(deny("persisted OKX buy ledger absent"))?.exact_ledger;
    let futures_short=&e.recovery_checkpoint.as_ref()
        .ok_or(deny("persisted Binance short ledger absent"))?.ledger;
    let close=e.shadow_close_receipt.as_ref()
        .ok_or(deny("persisted closing receipt absent"))?;
    let (spot_sell,futures_cover)=close.bounded_economic_ledgers();
    let review=inspect_fee_adjusted_ordinary_four_leg_cycle(
        spot_buy,spot_sell,futures_short,futures_cover,source_shaped_spot_fee_fills
    ).map_err(OwnerFeeGateError::Fee)?;
    // CAS-style read-only comparison. Does NOT externally certify antirollback.
    if store.load().map_err(OwnerFeeGateError::State)?.as_ref()!=Some(&before){
        return Err(deny("Native owner generation/body changed during fee review"))
    }
    Ok(OwnerFeeCycleOfflineReport{
        sealed_owner_generation:expected_generation,
        internally_consistent_source_shaped_fee_review:review,
        externally_attested_source_history:false,
        legacy_original_gross_seal_migration_complete:false,
        signed_real_account_flatness_proven:false,
        verified_closed:false,
        may_send_or_cancel_or_retry:false,
    })
}
