//! TRADE2F1: non-authorizing four-leg USDT economics.
//! Source authentication, complete funding and transfers are NOT proven.
//! Internal/external transfers NEVER become arbitrage profit.
use crate::fill_economics::{LegFillLedger,SCALE_12};
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum Trade2FError{Missing(&'static str),Mismatch(&'static str),
    Unsupported(&'static str),Overflow}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct ObservedMoneyMovements{
    pub bounded_funding_usdt_e12:Option<i128>,
    /// Signed external deposits (+) and withdrawals (-), NOT PnL.
    pub observed_external_flow_usdt_e12:Option<i128>,
    pub combined_spot_and_futures_usdt_wallet_delta_e12:Option<i128>,
    /// Just a diagnostic: internal transfers across included accounts net 0.
    pub internal_transfer_volume_usdt_e12:Option<u128>,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct NonAuthorizingTrade2F{
    pub matched_base_quantity_e12:u128,
    pub spot_buy_usdt_e12:i128,pub spot_sell_usdt_e12:i128,
    pub futures_short_open_usdt_e12:i128,pub futures_short_cover_usdt_e12:i128,
    pub spot_gross_e12:i128,pub futures_recomputed_gross_e12:i128,
    pub four_trade_fees_usdt_e12:i128,
    pub trade_only_net_before_funding_e12:i128,
    /// This is NOT full-life PnL, funding is just a bounded observation.
    pub provisional_net_with_observed_bounded_funding_e12:Option<i128>,
    /// Balance movement after subtracting external non-trading deposits.
    pub combined_wallet_delta_excluding_external_flow_e12:Option<i128>,
    /// Can be omitted funding/fee/transfer history, not necessarily error=0.
    pub unexplained_or_unbounded_account_difference_e12:Option<i128>,
    pub internal_transfer_volume_usdt_e12:Option<u128>,
    pub external_flow_counted_as_trading_profit:bool,
    pub supplied_four_leg_fills_exact:bool,
    pub complete_live_funding_history_proven:bool,
    pub complete_external_flow_history_proven:bool,
    pub source_signed_exchange_attestation_proven:bool,
    pub definitive_lifetime_net_pnl_e12:Option<i128>,
    pub may_send_cancel_retry_or_verify_closed:bool,
}
fn no(x:&'static str)->Trade2FError{Trade2FError::Missing(x)}
fn bad(x:&'static str)->Trade2FError{Trade2FError::Mismatch(x)}
/// Exact quote notional from EACH real-source fill, not from a rounded VWAP.
fn leg(l:&LegFillLedger,venue:&str,side:&str)
    ->Result<(u128,i128,i128,u64,u64),Trade2FError>{
    if l.venue!=venue||l.side!=side||l.fills.is_empty()||!l.terminal
        ||!l.cumulative_truth_present||!l.fee_evidence_complete
        ||l.filled_qty_atoms()==0||l.remaining_qty_atoms()!=0{
        return Err(no("four-leg terminal exact quantity/fee proof missing"))
    }
    let mut qty=0u128;let mut atom2=0u128;let mut fee=0i128;
    let mut first=u64::MAX;let mut last=0u64;
    for (id,f) in &l.fills{
        let time=f.trade_time_ms.ok_or(no("fill trade-time missing"))?;
        let cost=f.fee_cost_quote_atoms.ok_or(no("fee amount missing"))?;
        if id!=&f.fill_id||f.fill_id.is_empty()||time==0
           ||f.price_atoms==0||f.qty_atoms==0
           ||f.fee_currency.as_deref()!=Some("USDT")||cost<0{
            return Err(Trade2FError::Unsupported(
                "invalid trade identity/price/time/unsupported fee currency or rebate"))
        }
        qty=qty.checked_add(f.qty_atoms).ok_or(Trade2FError::Overflow)?;
        atom2=atom2.checked_add(
            f.qty_atoms.checked_mul(f.price_atoms).ok_or(Trade2FError::Overflow)?)
            .ok_or(Trade2FError::Overflow)?;
        fee=fee.checked_add(cost).ok_or(Trade2FError::Overflow)?;
        first=first.min(time);last=last.max(time);
    }
    if qty!=l.filled_qty_atoms()||qty!=l.requested_qty_atoms
        ||fee!=l.fee_cost_quote_atoms{
        return Err(bad("per-fill quantity/fee differs from sealed order ledger"))
    }
    if atom2%SCALE_12!=0{
        return Err(Trade2FError::Unsupported("quote cashflow has sub-e12 remainder"))
    }
    if l.vwap_atoms().map_or(true,|x|(atom2/qty).abs_diff(x)>1){
        return Err(bad("price source is not the exact per-fill VWAP"))
    }
    let quote=i128::try_from(atom2/SCALE_12).map_err(|_|Trade2FError::Overflow)?;
    Ok((qty,quote,fee,first,last))
}

/// Pure offline computation. Four ledgers MUST originate from a separate
/// authenticated source gate; neither input nor output carries authority.
/// Position is fully closed only for mathematical matching; no exchange
/// flatness, late fill absence or complete lifetime funding claim.
#[allow(clippy::too_many_arguments)]
pub fn calculate_bounded_trade_result(
    spot_buy:&LegFillLedger,spot_sell:&LegFillLedger,
    futures_sell_short:&LegFillLedger,futures_buy_close:&LegFillLedger,
    futures_reported_close_pnl_e12:i128,
    observed:&ObservedMoneyMovements,
)->Result<NonAuthorizingTrade2F,Trade2FError>{
    if spot_buy.symbol!=spot_sell.symbol
        ||futures_sell_short.symbol!=futures_buy_close.symbol
        ||spot_buy.symbol.replace(['-','/'], "")!=futures_sell_short.symbol
        ||[spot_buy,spot_sell,futures_sell_short,futures_buy_close].iter().any(|v|
             v.symbol.is_empty()||v.client_order_id.is_empty()
             ||v.exchange_order_id.as_deref().map_or(true,|x|x.is_empty()))
        ||spot_buy.client_order_id==spot_sell.client_order_id
        ||futures_sell_short.client_order_id==futures_buy_close.client_order_id
        ||spot_buy.exchange_order_id==spot_sell.exchange_order_id
        ||futures_sell_short.exchange_order_id==futures_buy_close.exchange_order_id{
        return Err(bad("original opening/closing symbols or order IDs collide"))
    }
    let (q_a,a,fee_a,_,a_last)=leg(spot_buy,"okx","buy")?;
    let (q_b,b,fee_b,b_first,_)=leg(spot_sell,"okx","sell")?;
    let (q_c,c,fee_c,_,c_last)=leg(futures_sell_short,"binance","sell")?;
    let (q_d,d,fee_d,d_first,_)=leg(futures_buy_close,"binance","buy")?;
    if q_a!=q_b||q_a!=q_c||q_a!=q_d{
        return Err(bad("mismatched quantity or unfinished/unhedged roundtrip"))
    }
    if b_first<a_last||d_first<c_last{
        return Err(bad("exit fill precedes last entry fill"))
    }
    let spot=b.checked_sub(a).ok_or(Trade2FError::Overflow)?;
    let futures=c.checked_sub(d).ok_or(Trade2FError::Overflow)?;
    if futures!=futures_reported_close_pnl_e12{
        return Err(bad("Binance realized close PnL differs from recomputed short economics"))
    }
    let fee=fee_a.checked_add(fee_b)
       .and_then(|x|x.checked_add(fee_c))
       .and_then(|x|x.checked_add(fee_d)).ok_or(Trade2FError::Overflow)?;
    let net=spot.checked_add(futures)
       .and_then(|x|x.checked_sub(fee)).ok_or(Trade2FError::Overflow)?;
    let bounded=observed.bounded_funding_usdt_e12
       .map(|f|net.checked_add(f).ok_or(Trade2FError::Overflow)).transpose()?;
    let adjusted_wallet=match (
        observed.combined_spot_and_futures_usdt_wallet_delta_e12,
        observed.observed_external_flow_usdt_e12,
    ){
       (Some(d),Some(flow))=>Some(d.checked_sub(flow).ok_or(Trade2FError::Overflow)?),
       _=>None,
    };
    let residual=match (adjusted_wallet,bounded){
       (Some(d),Some(b))=>Some(d.checked_sub(b).ok_or(Trade2FError::Overflow)?),
       _=>None,
    };
    Ok(NonAuthorizingTrade2F{
       matched_base_quantity_e12:q_a,
       spot_buy_usdt_e12:a,spot_sell_usdt_e12:b,
       futures_short_open_usdt_e12:c,futures_short_cover_usdt_e12:d,
       spot_gross_e12:spot,futures_recomputed_gross_e12:futures,
       four_trade_fees_usdt_e12:fee,
       trade_only_net_before_funding_e12:net,
       provisional_net_with_observed_bounded_funding_e12:bounded,
       combined_wallet_delta_excluding_external_flow_e12:adjusted_wallet,
       unexplained_or_unbounded_account_difference_e12:residual,
       internal_transfer_volume_usdt_e12:observed.internal_transfer_volume_usdt_e12,
       external_flow_counted_as_trading_profit:false,
       supplied_four_leg_fills_exact:true,
       complete_live_funding_history_proven:false,
       complete_external_flow_history_proven:false,
       source_signed_exchange_attestation_proven:false,
       definitive_lifetime_net_pnl_e12:None,
       may_send_cancel_retry_or_verify_closed:false,
    })
}
