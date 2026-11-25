//! TRADE2F3 / Stage A: one read-only OFFLINE orchestration of the already
//! tested OKX G->H and Binance I owners. No new parser, mock transport,
//! auth, wallet, execution or recovery permission is implemented here.
//! Success means internally coherent TEST TAPE ONLY, never real flatness,
//! VerifiedClosed, complete PnL, finality or authorization.
use crate::{
 authoritative_state::{NativeStateStore,NativeStateError},
 native_trade2f_okx_owner_mock::MockGetStep,
 native_trade2f_post_settlement_guard::{
   inspect_owner_bound_late_okx_bills_offline,F2HError,F2HOfflineReview},
 native_trade2f_binance_late_income::{
   inspect_owner_bound_binance_late_income_offline,F2IError,F2IOfflineReview},
 native_trade2f_owner_binding::original_four_leg_preflight,
 private_rest::OkxDomain,
};
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum UnifiedOfflineError{
 State(NativeStateError),
 Okx(F2HError),
 Binance(F2IError),
 Deny(&'static str),
}
fn deny(s:&'static str)->UnifiedOfflineError{UnifiedOfflineError::Deny(s)}
/// Deliberately has no VerifiedClosed or RealReady variant.
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum OfflineDisposition{MockOnlyManualNoAuthority}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct UnifiedOfflineReport{
 pub sealed_original_generation:u64,
 pub okx_original_and_late_mock_gets:usize,
 pub binance_original_and_late_mock_gets:usize,
 pub original_linked_okx_bills:usize,
 pub bounded_binance_income_rows:usize,
 pub both_venue_mock_windows_consistent:bool,
 pub disposition:OfflineDisposition,
 pub exchange_authenticated:bool,
 pub history_and_delayed_postings_complete:bool,
 pub third_asset_fee_quote_valued:bool,
 pub independent_wallet_and_position_flatness:bool,
 pub definitive_lifetime_net_pnl_e12:Option<i128>,
 pub verified_closed:bool,
 pub real_ready:bool,
 pub may_send_order_cancel_transfer_or_release:bool,
}
fn coherent(h:&F2HOfflineReview,i:&F2IOfflineReview,gen:u64)
  ->Result<(),UnifiedOfflineError>{
 if h.original_generation!=gen||i.native_owner_generation!=gen
    ||!h.two_late_mock_scans_identical||!h.no_additional_bills_in_mock_window
    ||!i.two_original_and_two_late_mock_scans_stable
    ||i.late_income_rows_in_observed_mock_window!=0
    ||h.original_bill_ids_crosschecked==0||i.source_income_rows==0
    ||h.signed_mock_gets_total==0||i.signed_mock_gets==0
    ||h.independent_real_source_attested||i.all_real_binance_income_proven
    ||h.all_real_account_bill_classes_proven
    ||i.account_wide_history_and_late_finality_proven
    ||h.all_delayed_postings_absent_proven
    ||h.all_binance_late_income_absent_proven
    ||i.third_asset_usdt_fx_independently_proven
    ||h.entire_lifetime_net_pnl_e12.is_some()
    ||i.definitive_lifetime_net_pnl_e12.is_some()
    ||h.real_ready||i.real_ready||h.verified_closed||i.verified_closed
    ||h.may_trade_cancel_transfer_or_release
    ||i.may_trade_cancel_transfer_or_release{
    return Err(deny("review subreport unexpectedly claims live authority or inconsistent owner"))
 }
 Ok(())
}
/// No outside caller can inject a pre-built H or I PASS report: we call the
/// complete existing owner+history validators with exact in-memory mock tapes.
/// first_end and late_end MUST be identical across both venue validators.
pub fn inspect_original_owner_unified_offline(
 store:&NativeStateStore,expected_generation:u64,
 first_end:u64,late_end:u64,okx_domain:OkxDomain,
 okx_initial:&[MockGetStep],okx_late:&[MockGetStep],
 binance_initial_and_late:&[MockGetStep],
)->Result<UnifiedOfflineReport,UnifiedOfflineError>{
 inspect_inner(store,expected_generation,first_end,late_end,okx_domain,
     okx_initial,okx_late,binance_initial_and_late,||{})
}
/// Deterministic debug-only race during venue handoff; never compiled in release.
#[cfg(debug_assertions)]
#[doc(hidden)]
pub fn inspect_original_owner_unified_offline_debug_after_okx<F:FnOnce()>(
 store:&NativeStateStore,expected_generation:u64,
 first_end:u64,late_end:u64,okx_domain:OkxDomain,
 okx_initial:&[MockGetStep],okx_late:&[MockGetStep],
 binance_initial_and_late:&[MockGetStep],after_okx:F,
)->Result<UnifiedOfflineReport,UnifiedOfflineError>{
 inspect_inner(store,expected_generation,first_end,late_end,okx_domain,
     okx_initial,okx_late,binance_initial_and_late,after_okx)
}
fn inspect_inner<F:FnOnce()>(
 store:&NativeStateStore,expected_generation:u64,
 first_end:u64,late_end:u64,okx_domain:OkxDomain,
 okx_initial:&[MockGetStep],okx_late:&[MockGetStep],
 binance_initial_and_late:&[MockGetStep],after_okx:F,
)->Result<UnifiedOfflineReport,UnifiedOfflineError>{
 // Same hard wall as H/I, *before* any scripted signed GET. Even a mock
 // cannot re-label a partial/UnknownManualOnly original owner.
 let before=store.load().map_err(UnifiedOfflineError::State)?
    .ok_or(deny("sealed Native owner missing before both-venue read"))?;
 if before.generation!=expected_generation{
    return Err(deny("unified owner generation stale BEFORE GET"))
 }
 original_four_leg_preflight(&before)
    .map_err(|_|deny("unified full four-leg Native owner denied BEFORE GET"))?;
 let h=inspect_owner_bound_late_okx_bills_offline(
     store,expected_generation,first_end,late_end,okx_domain,
     okx_initial,okx_late).map_err(UnifiedOfflineError::Okx)?;
 after_okx();
 if store.load().map_err(UnifiedOfflineError::State)?.as_ref()!=Some(&before){
     return Err(deny("Native owner changed during OKX->Binance evidence handoff"))
 }
 let i=inspect_owner_bound_binance_late_income_offline(
     store,expected_generation,first_end,late_end,binance_initial_and_late)
     .map_err(UnifiedOfflineError::Binance)?;
 if store.load().map_err(UnifiedOfflineError::State)?.as_ref()!=Some(&before){
     return Err(deny("Native owner changed after both exchange mock windows"))
 }
 coherent(&h,&i,expected_generation)?;
 Ok(UnifiedOfflineReport{
  sealed_original_generation:expected_generation,
  okx_original_and_late_mock_gets:h.signed_mock_gets_total,
  binance_original_and_late_mock_gets:i.signed_mock_gets,
  original_linked_okx_bills:h.original_bill_ids_crosschecked,
  bounded_binance_income_rows:i.source_income_rows,
  both_venue_mock_windows_consistent:true,
  disposition:OfflineDisposition::MockOnlyManualNoAuthority,
  exchange_authenticated:false,
  history_and_delayed_postings_complete:false,
  third_asset_fee_quote_valued:false,
  independent_wallet_and_position_flatness:false,
  definitive_lifetime_net_pnl_e12:None,
  verified_closed:false,real_ready:false,
  may_send_order_cancel_transfer_or_release:false,
 })
}
