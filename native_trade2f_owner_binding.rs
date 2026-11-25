//! TRADE2F2C: strict Native-owner -> sealed original four legs ->
//! TWO signed-MOCK GET money snapshots -> exact receipt binding.
//! No loose caller-provided income/bills/order IDs; no real HTTP/POST.
//! The existing persisted C5/C6 1 BNT spot vs 0.4 BNT short and MOCK
//! UnknownManualOnly latch are DENIED BEFORE ANY private GET.
use std::collections::{BTreeMap,BTreeSet};
use crate::{
 authoritative_state::{
    NativeAuthoritativeState,NativeEntryPhase,NativeStateStore,NativeStateError,
 },
 native_trade2f_income_window::{
    collect_double_money_rows,OwnerBoundMoneyRows,MoneyWindowError,ObservedOkxBill,
 },
 native_trade2f_receipt_binding::{
    reconcile_supplied_four_leg_receipts,BoundedReceiptBinding,ReceiptBindError,
 },
 private_https::HttpExecutor,
 private_rest::{OkxCredentials,BinanceCredentials,OkxDomain},
};
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum OwnerCashError{
 State(NativeStateError),History(MoneyWindowError),Bind(ReceiptBindError),
 Missing(&'static str),
}
fn deny(s:&'static str)->OwnerCashError{OwnerCashError::Missing(s)}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct OwnerScopedTrade2F2C{
    pub sealed_native_generation:u64,
    pub source_bounded_start_ms:u64,
    pub source_bounded_end_ms:u64,
    pub two_independent_get_snapshots_matched:bool,
    pub signed_mock_get_count:u16,
    pub ordinary_four_leg_receipt_match:BoundedReceiptBinding,
    pub original_real_close_dispatch_proven:bool,
    pub complete_income_history_through_settlement_proven:bool,
    pub original_account_externally_antirollback_proven:bool,
    pub complete_lifetime_net_pnl_proven:bool,
    pub may_send_cancel_retry_or_verify_closed:bool,
}
/// F2G already pins original fill bill IDs to the sealed Native owner.
/// F2C must also reject a different billId that happens to reconcile in
/// amount/tradeId. This is ORIGINAL source identity, not another PnL pass.
fn require_original_buy_bill_ancestry(
    sealed:&crate::native_spot_prehedge::NativeSpotPreHedgeEvidence,
    base:&BTreeMap<String,ObservedOkxBill>,
    quote:&BTreeMap<String,ObservedOkxBill>,
)->Result<(),OwnerCashError>{
    let fill_ids:BTreeSet<&str>=sealed.exact_ledger.fills.keys()
        .map(String::as_str).collect();
    let expected:BTreeSet<&str>=sealed.fill_bill_ids.iter()
        .map(String::as_str).collect();
    if fill_ids.is_empty()||expected.len()!=fill_ids.len()
       ||expected.len()!=sealed.fill_bill_ids.len(){
        return Err(deny("sealed opening OKX bill ancestry is absent or ambiguous"))
    }
    let mut seen_trades=BTreeSet::new();
    let mut seen_bills=BTreeSet::new();
    for row in base.values().chain(quote.values()){
        if fill_ids.contains(row.trade_id.as_str()){
            if row.order_id!=sealed.spot_order_id
               ||!seen_trades.insert(row.trade_id.as_str())
               ||!seen_bills.insert(row.bill_id.as_str())
               ||!expected.contains(row.bill_id.as_str()){
                return Err(deny("F2C original OKX billId differs from sealed opening owner"))
            }
        }
    }
    if seen_trades!=fill_ids||seen_bills!=expected{
        return Err(deny("F2C opening original billId missing from owner-scoped money rows"))
    }
    Ok(())
}
/// Enforce origin, policy, exact persisted four fills, zero residual,
/// and all previous shadow close/account/economics seals BEFORE network.
/// This proves only internal immutable review evidence, not execution.
pub(crate) fn original_four_leg_preflight(s:&NativeAuthoritativeState)
 ->Result<(),OwnerCashError>{
 if s.active.is_some(){return Err(deny("Native Active account is hard denied"))}
 let e=s.entry.as_ref().ok_or(deny("original Native entry absent"))?;
 if e.phase!=NativeEntryPhase::RecoveryRequired
    ||e.dryrun_close_latch.is_some(){
    return Err(deny("MOCK unknown close or non-recovery state cannot become trade profit"))
 }
 let open=e.origin_dual_opening_link.as_ref()
   .ok_or(deny("original dual-opening seal missing"))?;
 let close=e.shadow_close_receipt.as_ref()
   .ok_or(deny("persisted full four-leg closing receipts absent"))?;
 let account=e.shadow_close_account_window.as_ref()
   .ok_or(deny("prior independent post-close account snapshots absent"))?;
 let lifetime=e.shadow_lifetime_economics.as_ref()
   .ok_or(deny("original four-leg bounded economics source seal absent"))?;
 // All seals validate exact original policy, IDs, generations and
 // previous source hashes; a copied synthetic loose ledger cannot pass.
 open.verify(e,s.generation).map_err(OwnerCashError::State)?;
 close.verify(e,s.generation).map_err(OwnerCashError::State)?;
 account.verify(e,s.generation).map_err(OwnerCashError::State)?;
 lifetime.verify(e,s.generation).map_err(OwnerCashError::State)?;
 if open.exposure_remaining_e18()!=0||close.residual_spot_e18()!=0
   ||open.futures_short_e18()==0
   ||open.futures_short_e18()!=close.closing_qty_e12()
       .checked_mul(1_000_000).ok_or(deny("closing quantity overflow"))?{
    return Err(deny("unmatched opening/closing spot-futures exposure"))
 }
 let spot_open=&e.spot_prehedge_evidence.as_ref()
    .ok_or(deny("original OKX buy fills missing"))?.exact_ledger;
 let futures_open=&e.recovery_checkpoint.as_ref()
    .ok_or(deny("original Binance short fills missing"))?.ledger;
 let (spot_close,futures_close)=close.bounded_economic_ledgers();
 let q=spot_open.filled_qty_atoms();
 if q==0||futures_open.filled_qty_atoms()!=q
    ||spot_close.filled_qty_atoms()!=q
    ||futures_close.filled_qty_atoms()!=q
    ||spot_open.symbol!=e.plan.spot_symbol
    ||spot_close.symbol!=e.plan.spot_symbol
    ||futures_open.symbol!=e.plan.futures_symbol
    ||futures_close.symbol!=e.plan.futures_symbol{
    return Err(deny("all original four filled source legs must match exactly"))
 }
 Ok(())
}
#[allow(clippy::too_many_arguments)]
pub fn inspect_owner_bound_mock_four_leg_income<E:HttpExecutor>(
 http:&E,store:&NativeStateStore,expected_generation:u64,
 okx:&OkxCredentials,bn:&BinanceCredentials,
 now:u64,domain:OkxDomain,recv_window:u64,
)->Result<OwnerScopedTrade2F2C,OwnerCashError>{
 let initial=store.load().map_err(OwnerCashError::State)?
    .ok_or(OwnerCashError::State(NativeStateError::Generation))?;
 if initial.generation!=expected_generation{
    return Err(deny("Native generation incorrect BEFORE money GET"))
 }
 original_four_leg_preflight(&initial)?;
 // Reuses EXACT TRADE2F2A bounded double GET+empty-cursor and CAS
 // collector. No opportunity to inject user-provided money arrays.
 let OwnerBoundMoneyRows{original,start_ms,end_ms,rows}=
    collect_double_money_rows(http,store,expected_generation,okx,bn,now,domain,recv_window)
       .map_err(OwnerCashError::History)?;
 if original!=initial{
    return Err(deny("Native seal changed between original preflight and GET"))
 }
 original_four_leg_preflight(&original)?;
 let e=original.entry.as_ref().ok_or(deny("Native entry missing after GET"))?;
 let close=e.shadow_close_receipt.as_ref()
    .ok_or(deny("close receipts missing after read"))?;
 let (spot_close,futures_close)=close.bounded_economic_ledgers();
 let original_spot=&e.spot_prehedge_evidence.as_ref()
     .ok_or(deny("original spot source missing"))?.exact_ledger;
 let original_futures=&e.recovery_checkpoint.as_ref()
     .ok_or(deny("original futures source missing"))?.ledger;
 require_original_buy_bill_ancestry(
    e.spot_prehedge_evidence.as_ref()
       .ok_or(deny("sealed original first-leg bill ancestry missing"))?,
    &rows.okx_base,&rows.okx_usdt,
 )?;
 let cash=reconcile_supplied_four_leg_receipts(
    original_spot,spot_close,original_futures,futures_close,
    close.futures_reported_close_pnl_e12(),(start_ms,end_ms),
    &rows.binance.into_values().collect::<Vec<_>>(),
    &rows.okx_base.into_values().collect::<Vec<_>>(),
    &rows.okx_usdt.into_values().collect::<Vec<_>>(),
 ).map_err(OwnerCashError::Bind)?;
 // A second local CAS check after source-shaped reconciliation avoids
 // publishing a positive report across an owner revision.
 if store.load().map_err(OwnerCashError::State)?.as_ref()!=Some(&initial){
    return Err(deny("Native owner changed while binding cash to fills"))
 }
 let count=2u16.checked_mul(
    rows.binance_pages+rows.okx_base_pages+rows.okx_quote_pages
 ).ok_or(deny("mock GET count overflow"))?;
 Ok(OwnerScopedTrade2F2C{
    sealed_native_generation:expected_generation,
    source_bounded_start_ms:start_ms,source_bounded_end_ms:end_ms,
    two_independent_get_snapshots_matched:true,
    signed_mock_get_count:count,
    ordinary_four_leg_receipt_match:cash,
    original_real_close_dispatch_proven:false,
    complete_income_history_through_settlement_proven:false,
    original_account_externally_antirollback_proven:false,
    complete_lifetime_net_pnl_proven:false,
    may_send_cancel_retry_or_verify_closed:false,
 })
}
