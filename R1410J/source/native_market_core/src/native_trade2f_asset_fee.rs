//! TRADE2F2E: economic inventory effects of OKX SPOT native BASE-asset
//! or QUOTE-asset fees, from explicit SOURCE-SHAPED individual fills.
//! This is PURE offline math; external caller rows are not independently
//! signed/attested and there is NO order or VerifiedClosed authority.
//! Fees in base BNT do not magically become USDT: final net PnL UNKNOWN.
use std::collections::BTreeSet;
use serde::{Serialize,Deserialize};
use crate::fill_economics::SCALE_12;

#[derive(Clone,Debug,PartialEq,Eq)]
pub enum AssetFeeError{
 Missing(&'static str),Invalid(&'static str),Overflow,
}
fn no(s:&'static str)->AssetFeeError{AssetFeeError::Invalid(s)}

#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpotFeeFill{
 pub order_id:String,
 pub trade_id:String,
 pub at_ms:u64,
 pub side:String,
 pub inst_id:String,
 pub price_quote_e12:u128,
 pub gross_base_qty_e12:u128,
 /// The actual OKX signed fee in the unit named fee_ccy:
 /// negative fee debit, positive maker rebate. This conservative
 /// phase DENIES rebates and unknown currencies until independently
 /// matched as their own asset movements.
 pub fee_ccy:String,
 pub signed_fee_e12:i128,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct BoundedNativeFeeInventory{
 pub buy_gross_base_e12:u128,
 pub buy_delivered_base_after_native_fee_e12:u128,
 pub sell_gross_base_e12:u128,
 pub base_fee_debited_e12:u128,
 pub quote_fee_debited_e12:u128,
 pub aggregate_base_wallet_delta_e12:i128,
 pub aggregate_quote_wallet_delta_e12:i128,
 pub original_future_short_gross_base_e12:u128,
 pub original_future_short_matches_delivered_spot:bool,
 pub completely_self_funded_spot_unwind:bool,
 pub base_fee_converted_to_quote_by_unverified_mark_price:bool,
 pub source_signed_exchange_attestation_proven:bool,
 pub full_income_or_wallet_bill_history_proven:bool,
 pub complete_hedged_exit_proven:bool,
 pub definitive_lifetime_net_pnl_quote_e12:Option<i128>,
 pub may_send_cancel_retry_or_verify_closed:bool,
}
fn as_i(v:u128)->Result<i128,AssetFeeError>{
 i128::try_from(v).map_err(|_|AssetFeeError::Overflow)
}
fn quote_notional(f:&SpotFeeFill)->Result<i128,AssetFeeError>{
 let x=f.gross_base_qty_e12.checked_mul(f.price_quote_e12)
    .ok_or(AssetFeeError::Overflow)?;
 if x%SCALE_12!=0{
    return Err(no("unrepresentable exact USDT spot notional; do not round"))
 }
 as_i(x/SCALE_12)
}
/// The original future short size is included ONLY to diagnose how a
/// base fee can create unhedged exposure. It is NOT a live position
/// snapshot or authorization to hedge/close a position.
pub fn inspect_native_spot_asset_fee_inventory(
 original_symbol:&str,
 original_futures_short_qty_e12:u128,
 originals:&[SpotFeeFill],
)->Result<BoundedNativeFeeInventory,AssetFeeError>{
 if original_symbol.is_empty()||original_futures_short_qty_e12==0
    ||originals.is_empty()||originals.len()>64{
    return Err(AssetFeeError::Missing("original pair, hedged size or fills absent"))
 }
 let normalized=original_symbol.replace('/',"-");
 let mut splits=normalized.split('-');
 let base=splits.next().unwrap_or("");
 let quote=splits.next().unwrap_or("");
 if base.is_empty()||quote!="USDT"||splits.next().is_some(){
    return Err(no("only an explicitly identified base-USDT spot pair supported"))
 }
 let mut ids=BTreeSet::new();
 let mut buy=0u128;let mut sell=0u128;
 let mut base_fee=0u128;let mut quote_fee=0u128;
 let mut base_delta=0i128;let mut quote_delta=0i128;
 let mut buys_last=0u64;let mut first_sell=u64::MAX;
 let mut n_buy=0usize;let mut n_sell=0usize;
 for f in originals{
    if f.inst_id.replace('/',"-")!=normalized
       ||f.trade_id.is_empty()||f.order_id.is_empty()
       ||f.at_ms==0||f.price_quote_e12==0||f.gross_base_qty_e12==0
       ||!ids.insert(f.trade_id.as_str())
       ||f.signed_fee_e12>0{
       return Err(no("source identity, quantity, price or unsupported rebate"))
    }
    if f.side!="buy"&&f.side!="sell"{
       return Err(no("not original spot buy or exit sell"))
    }
    if f.fee_ccy!=base && f.fee_ccy!="USDT"{
       return Err(no("third-asset fee without separate price/cash evidence"))
    }
    let gross=as_i(f.gross_base_qty_e12)?;
    let quote=quote_notional(f)?;
    let charged=f.signed_fee_e12.checked_neg()
       .ok_or(AssetFeeError::Overflow)?;
    let (d_base,d_quote)=if f.side=="buy"{
       n_buy+=1;buys_last=buys_last.max(f.at_ms);
       buy=buy.checked_add(f.gross_base_qty_e12).ok_or(AssetFeeError::Overflow)?;
       (gross,quote.checked_neg().ok_or(AssetFeeError::Overflow)?)
    }else{
       n_sell+=1;first_sell=first_sell.min(f.at_ms);
       sell=sell.checked_add(f.gross_base_qty_e12).ok_or(AssetFeeError::Overflow)?;
       (-gross,quote)
    };
    let (d_base,d_quote)=if f.fee_ccy==base{
       base_fee=base_fee.checked_add(charged as u128)
           .ok_or(AssetFeeError::Overflow)?;
       (d_base.checked_sub(charged).ok_or(AssetFeeError::Overflow)?,d_quote)
    }else{
       quote_fee=quote_fee.checked_add(charged as u128)
           .ok_or(AssetFeeError::Overflow)?;
       (d_base,d_quote.checked_sub(charged).ok_or(AssetFeeError::Overflow)?)
    };
    base_delta=base_delta.checked_add(d_base).ok_or(AssetFeeError::Overflow)?;
    quote_delta=quote_delta.checked_add(d_quote).ok_or(AssetFeeError::Overflow)?;
 }
 if n_buy==0||n_sell==0||first_sell<buys_last{
    return Err(no("spot buy/close chronology incomplete"))
 }
 let delivered=buy.checked_sub(
    // In the source-only case where some base fees occur on SELL, the
    // inventory initially available from BUY includes only BUY fees.
    originals.iter().filter(|f|f.side=="buy"&&f.fee_ccy==base)
       .try_fold(0u128,|acc,f|
          acc.checked_add(f.signed_fee_e12.unsigned_abs()).ok_or(AssetFeeError::Overflow))?
 ).ok_or(no("base fee consumes entire acquired position"))?;
 let sell_debit=sell.checked_add(
    originals.iter().filter(|f|f.side=="sell"&&f.fee_ccy==base)
       .try_fold(0u128,|acc,f|
           acc.checked_add(f.signed_fee_e12.unsigned_abs()).ok_or(AssetFeeError::Overflow))?
 ).ok_or(AssetFeeError::Overflow)?;
 // No silent external BNT topup: selling more than fee-adjusted BUY
 // would consume pre-existing unrelated holdings, NOT full roundtrip.
 if sell_debit>delivered{
    return Err(no("selling more BASE than the source buy delivered"))
 }
 let is_self_funded=sell_debit==delivered;
 if base_delta!=as_i(delivered.checked_sub(sell_debit)
       .ok_or(AssetFeeError::Overflow)?)?{
    return Err(no("per-fill base cash accounting mismatch"))
 }
 Ok(BoundedNativeFeeInventory{
    buy_gross_base_e12:buy,
    buy_delivered_base_after_native_fee_e12:delivered,
    sell_gross_base_e12:sell,
    base_fee_debited_e12:base_fee,
    quote_fee_debited_e12:quote_fee,
    aggregate_base_wallet_delta_e12:base_delta,
    aggregate_quote_wallet_delta_e12:quote_delta,
    original_future_short_gross_base_e12:original_futures_short_qty_e12,
    original_future_short_matches_delivered_spot:
       original_futures_short_qty_e12==delivered,
    completely_self_funded_spot_unwind:is_self_funded,
    base_fee_converted_to_quote_by_unverified_mark_price:false,
    source_signed_exchange_attestation_proven:false,
    full_income_or_wallet_bill_history_proven:false,
    complete_hedged_exit_proven:false,
    definitive_lifetime_net_pnl_quote_e12:None,
    may_send_cancel_retry_or_verify_closed:false,
 })
}


/// Additionally bind fee-bearing source-shaped fills to the actual
/// previously stored (possibly incomplete-fee) per-order fill ledgers.
/// BASE fee records cannot be represented as a quote fee cost in
/// LegFillLedger; require quote fee field to remain None, not zero.
pub fn inspect_asset_fees_for_original_spot_ledgers(
 original_buy:&crate::fill_economics::LegFillLedger,
 original_sell:&crate::fill_economics::LegFillLedger,
 original_short:&crate::fill_economics::LegFillLedger,
 source_shaped_fee_fills:&[SpotFeeFill],
)->Result<BoundedNativeFeeInventory,AssetFeeError>{
 let a=original_buy;
 let b=original_sell;
 let h=original_short;
 if a.venue!="okx"||a.side!="buy"||b.venue!="okx"||b.side!="sell"
    ||h.venue!="binance"||h.side!="sell"
    ||a.symbol.replace('/',"-")!=b.symbol.replace('/',"-")
    ||a.symbol.replace(['-','/'],"")!=h.symbol
    ||!a.terminal||!b.terminal||!h.terminal
    ||!a.cumulative_truth_present||!b.cumulative_truth_present
    ||!h.cumulative_truth_present||!h.fee_evidence_complete
    ||h.fills.is_empty()
    ||h.fills.values().any(|f|
         f.fee_currency.as_deref()!=Some("USDT")
           ||f.fee_cost_quote_atoms.map_or(true,|cost|cost<0))
    ||a.filled_qty_atoms()==0||b.filled_qty_atoms()==0
    ||h.filled_qty_atoms()==0
    ||a.filled_qty_atoms()!=a.requested_qty_atoms
    ||b.filled_qty_atoms()!=b.requested_qty_atoms
    ||h.filled_qty_atoms()!=h.requested_qty_atoms{
    return Err(AssetFeeError::Missing("original terminal OKX BUY/SELL and Binance SHORT proof"))
 }
 if a.exchange_order_id.is_none()||b.exchange_order_id.is_none()
    ||h.exchange_order_id.is_none()
    ||a.exchange_order_id==b.exchange_order_id{
    return Err(AssetFeeError::Missing("original venue order IDs absent or reused"))
 }
 let mut ledger_by_id=std::collections::BTreeMap::new();
 for ledger in [a,b]{
    for (id,fill) in &ledger.fills{
       if ledger_by_id.insert(id.as_str(),(ledger,fill)).is_some(){
          return Err(no("original OKX duplicate fill across BUY and SELL"))
       }
    }
 }
 if ledger_by_id.is_empty()||ledger_by_id.len()!=source_shaped_fee_fills.len(){
    return Err(AssetFeeError::Missing("one source fee event per original OKX fill required"))
 }
 for event in source_shaped_fee_fills{
    let (l,fill)=ledger_by_id.get(event.trade_id.as_str())
       .ok_or(no("fee event missing from original Native fills"))?;
    if event.order_id!=l.exchange_order_id.as_deref().unwrap_or("")
      ||event.side!=l.side
      ||event.inst_id.replace('/',"-")!=l.symbol.replace('/',"-")
      ||fill.price_atoms!=event.price_quote_e12
      ||fill.qty_atoms!=event.gross_base_qty_e12
      ||fill.trade_time_ms!=Some(event.at_ms)
      ||fill.fee_currency.as_deref()!=Some(event.fee_ccy.as_str()){
      return Err(no("Native fill price, qty, ID, time, fee asset mismatched"))
    }
    if event.fee_ccy=="USDT"{
       if fill.fee_cost_quote_atoms!=Some(
           event.signed_fee_e12.checked_neg().ok_or(AssetFeeError::Overflow)?
       ){
          return Err(no("original USDT fill commission differs from source fee"))
       }
    }else if fill.fee_cost_quote_atoms.is_some(){
       return Err(no("original BASE fee wrongly converted into quote fee"))
    }
 }
 inspect_native_spot_asset_fee_inventory(&a.symbol,h.filled_qty_atoms(),source_shaped_fee_fills)
}
