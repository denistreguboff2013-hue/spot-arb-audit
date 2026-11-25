//! Astra P1-02/P1-03 PRE-INTEGRATION: fee-aware four-leg ordinary-cycle gate.
//! Strictly offline, caller-supplied history is NOT independently trusted.
//! Does NOT waive the older owner-preflight, does NOT authorize exchange GET,
//! POST, close, verified-flat, profit, or any live capability.
use crate::{
    fill_economics::LegFillLedger,
    native_trade2f_asset_fee::{
        inspect_asset_fees_for_original_spot_ledgers,AssetFeeError,
        BoundedNativeFeeInventory,SpotFeeFill,
    },
    native_trade2f3_first_leg_v2_checkpoint::FirstLegV2CheckpointPayload,
    native_trade2f3_v2_net_hedge_contract::inspect_v2_net_hedge_proposal,
};

#[derive(Clone,Debug,PartialEq,Eq)]
pub enum CycleGateError{
    Source(AssetFeeError),
    Missing(&'static str),
    Mismatch(&'static str),
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct FeeAdjustedOrdinaryCycleReview{
    pub spot_buy_gross_atoms_e12:u128,
    pub spot_inventory_delivered_atoms_e12:u128,
    pub spot_sell_gross_atoms_e12:u128,
    pub futures_short_open_atoms_e12:u128,
    pub futures_short_close_atoms_e12:u128,
    pub base_fee_atoms_e12:u128,
    pub zero_owned_spot_inventory_residual:bool,
    pub exact_qty_hedge_after_native_fee:bool,
    pub ordinary_nonoverlapping_execution_steps_verified:bool,
    pub independent_exchange_account_truth_proven:bool,
    pub late_funding_history_proven:bool,
    pub definitive_net_pnl_atoms_e12:Option<i128>,
    pub may_send_cancel_retry_or_verify_closed:bool,
}
fn mismatch(s:&'static str)->CycleGateError{CycleGateError::Mismatch(s)}
fn missing(s:&'static str)->CycleGateError{CycleGateError::Missing(s)}

fn terminal_quote_fees(l:&LegFillLedger)->bool{
    l.terminal && l.cumulative_truth_present && l.fee_evidence_complete
    && !l.fills.is_empty()
    && l.fills.values().all(|f|
        f.trade_time_ms.unwrap_or(0)>0
        && f.fee_currency.as_deref()==Some("USDT")
        && f.fee_cost_quote_atoms.is_some_and(|v|v>=0)
    )
}
fn boundaries(l:&LegFillLedger)->Result<(u64,u64),CycleGateError>{
    if !l.terminal||!l.cumulative_truth_present||l.fills.is_empty()
        ||l.filled_qty_atoms()==0
        ||l.filled_qty_atoms()!=l.requested_qty_atoms
        ||l.exchange_order_id.as_deref().unwrap_or("").is_empty()
        ||l.client_order_id.is_empty() || l.intent_id.is_empty(){
        return Err(missing("original terminal fill and unique order ownership required"))
    }
    let mut lo=u64::MAX;
    let mut hi=0;
    for fill in l.fills.values(){
        let t=fill.trade_time_ms.ok_or(missing("original timestamp missing"))?;
        if t==0{return Err(missing("original timestamp invalid"))}
        lo=lo.min(t);hi=hi.max(t);
    }
    if l.first_fill_time_ms!=Some(lo)||l.last_fill_time_ms!=Some(hi){
        return Err(mismatch("original cumulative ledger fill timing differs"))
    }
    Ok((lo,hi))
}

/// This gate handles strictly sequential normal opening and closing only:
/// OKX BUY finished -> Binance short opened -> futures short covered
/// -> OKX inventory sold. Emergency/unwind timings need a separate
/// explicit owner-bound gate, NOT reinterpretation as an ordinary cycle.
///
/// It first checks actual original per-fill fee currency and full inventory
/// accounting, then binds the two Binance legs. It never grants authority.
pub fn inspect_fee_adjusted_ordinary_four_leg_cycle(
    spot_buy:&LegFillLedger,
    spot_sell:&LegFillLedger,
    futures_short:&LegFillLedger,
    futures_cover:&LegFillLedger,
    original_spot_fee_fills:&[SpotFeeFill],
)->Result<FeeAdjustedOrdinaryCycleReview,CycleGateError>{
    let inventory:BoundedNativeFeeInventory=
        inspect_asset_fees_for_original_spot_ledgers(
            spot_buy,spot_sell,futures_short,original_spot_fee_fills
        ).map_err(CycleGateError::Source)?;
    if !inventory.completely_self_funded_spot_unwind
        ||inventory.aggregate_base_wallet_delta_e12!=0{
        return Err(mismatch("spot inventory is not provably flat from original BUY only"))
    }
    if !inventory.original_future_short_matches_delivered_spot{
        return Err(mismatch("futures short size does not equal fee-adjusted owned inventory"))
    }
    if futures_cover.venue!="binance"||futures_cover.side!="buy"
        ||futures_cover.symbol!=futures_short.symbol
        ||futures_cover.exchange_order_id==futures_short.exchange_order_id
        ||futures_cover.client_order_id==futures_short.client_order_id
        ||futures_cover.intent_id==futures_short.intent_id{
        return Err(mismatch("original futures COVER has wrong venue side symbol or duplicate identity"))
    }
    if !terminal_quote_fees(futures_short)||!terminal_quote_fees(futures_cover){
        return Err(missing("Binance open/cover complete quote-denominated fills required"))
    }
    let buy=boundaries(spot_buy)?;
    let short=boundaries(futures_short)?;
    let cover=boundaries(futures_cover)?;
    let sell=boundaries(spot_sell)?;
    let q=futures_short.filled_qty_atoms();
    if q==0||futures_cover.filled_qty_atoms()!=q
        ||inventory.buy_delivered_base_after_native_fee_e12!=q{
        return Err(mismatch("futures open/close not exactly fee-adjusted spot hedge"))
    }
    if buy.1>short.0||short.1>cover.0||cover.1>sell.0{
        return Err(mismatch("unhedged/reversed ordinary sequence: expected BUY SHORT COVER SELL"))
    }
    Ok(FeeAdjustedOrdinaryCycleReview{
        spot_buy_gross_atoms_e12:inventory.buy_gross_base_e12,
        spot_inventory_delivered_atoms_e12:inventory.buy_delivered_base_after_native_fee_e12,
        spot_sell_gross_atoms_e12:inventory.sell_gross_base_e12,
        futures_short_open_atoms_e12:q,
        futures_short_close_atoms_e12:futures_cover.filled_qty_atoms(),
        base_fee_atoms_e12:inventory.base_fee_debited_e12,
        zero_owned_spot_inventory_residual:true,
        exact_qty_hedge_after_native_fee:true,
        ordinary_nonoverlapping_execution_steps_verified:true,
        independent_exchange_account_truth_proven:false,
        late_funding_history_proven:false,
        definitive_net_pnl_atoms_e12:None,
        may_send_cancel_retry_or_verify_closed:false,
    })
}

/// P1-02: strict offline bridge between existing V2 first-leg checkpoint,
/// exact delivered-BASE short proposal and ordinary four-leg inventory gate.
/// No V1 owner migration, live send, release, VerifiedClosed or profit authority.
pub fn inspect_v2_first_leg_to_ordinary_four_leg_consistency(
 checkpoint:&FirstLegV2CheckpointPayload,expected_native_generation:u64,
 spot_sell:&LegFillLedger,futures_short:&LegFillLedger,
 futures_cover:&LegFillLedger,sell_source_fees:&[SpotFeeFill],
)->Result<FeeAdjustedOrdinaryCycleReview,CycleGateError>{
 let proposal=inspect_v2_net_hedge_proposal(
    checkpoint,expected_native_generation,
    &futures_short.intent_id,&futures_short.client_order_id,
    futures_short.filled_qty_atoms(),
 ).map_err(|_|mismatch("V2 source-checked net-short proposal invalid"))?;
 if sell_source_fees.is_empty()||!sell_source_fees.iter().all(|f|f.side=="sell")
    ||proposal.futures_symbol!=futures_short.symbol
    ||proposal.original_okx_order_id!=checkpoint.original.exact_ledger
       .exchange_order_id.as_deref().unwrap_or(""){
    return Err(mismatch("V2 first leg or short symbol differs from source"))
 }
 if proposal.may_send_or_cancel_or_verify_closed || proposal.persisted_owner_v2_seal_available || proposal.owner_generation_cas_proven {return Err(mismatch("V2 owner authority unproven"))}
 let mut fee_rows=checkpoint.source_shaped_fee_fills.clone();
 fee_rows.extend_from_slice(sell_source_fees);
 let cycle=inspect_fee_adjusted_ordinary_four_leg_cycle(
     &checkpoint.original.exact_ledger,spot_sell,futures_short,futures_cover,&fee_rows)?;
 if cycle.spot_buy_gross_atoms_e12!=proposal.spot_buy_gross_atoms_e12
    ||cycle.spot_inventory_delivered_atoms_e12!=proposal.spot_delivered_net_atoms_e12
    ||cycle.base_fee_atoms_e12!=proposal.spot_base_fee_atoms_e12{
    return Err(mismatch("V2 source net BASE differs from closed four-leg inventory"))
 }
 if cycle.spot_sell_gross_atoms_e12!=proposal.spot_delivered_net_atoms_e12
    ||cycle.futures_short_open_atoms_e12!=proposal.proposed_futures_short_atoms_e12
    ||cycle.futures_short_close_atoms_e12!=proposal.proposed_futures_short_atoms_e12
    ||cycle.may_send_cancel_retry_or_verify_closed
    ||cycle.independent_exchange_account_truth_proven{
    return Err(mismatch("V2 net hedge and four-leg closure quantities differ"))
 }
 Ok(cycle)
}
