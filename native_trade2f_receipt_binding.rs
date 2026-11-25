//! TRADE2F2B: PURE OFFLINE four-leg fill <-> income <-> cash bill
//! reconciliation. Inputs are NOT independently authenticated. This
//! ONLY proves coherence for supplied records, NOT exchange completeness.
//! No HTTP, no order/transfer functions and NO authority flags.
use std::collections::{BTreeMap,BTreeSet};
use crate::{
 fill_economics::{LegFillLedger,FillEvent,SCALE_12},
 native_trade2f_economics::{
     calculate_bounded_trade_result,NonAuthorizingTrade2F,ObservedMoneyMovements,
 },
 native_trade2f_income_window::{ObservedBinanceIncome,ObservedOkxBill},
 native_trade2f_asset_fee::{inspect_asset_fees_for_original_spot_ledgers,
    SpotFeeFill,BoundedNativeFeeInventory},
 native_trade2f3_first_leg_v2_checkpoint::FirstLegV2CheckpointPayload,
 native_trade2f3_v2_bill_ancestry::{BoundedSourceBillWindowV2,
    inspect_v2_supplied_bill_ancestry},
};
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum ReceiptBindError{
    Invalid(&'static str),Overflow,
}
fn bad(s:&'static str)->ReceiptBindError{ReceiptBindError::Invalid(s)}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct BoundedReceiptBinding{
    pub spot_fill_ids:Vec<String>,
    pub futures_fill_ids:Vec<String>,
    pub spot_base_delta_e12:i128,
    pub spot_quote_delta_e12:i128,
    pub binance_commissions_e12:i128,
    pub futures_realized_income_e12:i128,
    pub observed_bounded_funding_e12:i128,
    pub observed_unclassified_transfer_e12:i128,
    pub trade_only_net_e12:i128,
    pub window_adjusted_net_e12:i128,
    pub supplied_trade_and_cash_receipts_reconcile:bool,
    pub independent_live_source_attested:bool,
    pub complete_retained_income_history_proven:bool,
    pub complete_wallet_transfer_history_proven:bool,
    pub all_lifetime_funding_proven:bool,
    pub definitive_lifetime_net_pnl_e12:Option<i128>,
    pub may_send_cancel_retry_or_verify_closed:bool,
}
fn mul(f:&FillEvent)->Result<i128,ReceiptBindError>{
 let raw=f.qty_atoms.checked_mul(f.price_atoms)
    .ok_or(ReceiptBindError::Overflow)?;
 if raw%SCALE_12!=0{return Err(bad("unrepresentable spot per-trade quote amount"))}
 i128::try_from(raw/SCALE_12).map_err(|_|ReceiptBindError::Overflow)
}
/// Strictly typed adapter: V2 opening BUY ancestry -> the ONE original
/// OKX account bill per fill used by later review-only receipt matching.
/// No extra QUOTE bill and NO independently authenticated exchange source.
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct V2OpeningBillAdapter{
 pub original_owner_generation:u64,
 pub original_buy_bills:Vec<ObservedOkxBill>,
 pub independent_exchange_account_authenticated:bool,
 pub full_owner_universe_proven:bool,
 pub may_trade_real:bool,
}
/// Adapt only after re-running V2's own complete bounded local ancestry
/// review against a supplied original generation and proposed hedge size.
pub fn adapt_v2_original_buy_bills_for_review(
 checkpoint:&FirstLegV2CheckpointPayload,
 expected_generation:u64,
 window:&BoundedSourceBillWindowV2,
 proposed_short_e12:u128,
)->Result<V2OpeningBillAdapter,ReceiptBindError>{
 let review=inspect_v2_supplied_bill_ancestry(
     checkpoint,expected_generation,window,proposed_short_e12)
     .map_err(|_|bad("original Native V2 BUY bill ancestry denied"))?;
 if !review.source_window_locally_consistent||
    review.generation!=expected_generation||
    review.confirmed_source_bill_ids.len()!=window.source_shaped_base_bills_desc.len(){
    return Err(bad("V2 opening original bill review not self-consistent"))
 }
 let mut rows=Vec::with_capacity(review.confirmed_source_bill_ids.len());
 for bill in &window.source_shaped_base_bills_desc{
    let after=i128::try_from(bill.balance_after_base_e12)
       .map_err(|_|ReceiptBindError::Overflow)?;
    rows.push(ObservedOkxBill{
      bill_id:bill.bill_id.clone(),
      currency:bill.ccy.clone(),
      signed_change_e12:bill.balance_change_base_e12,
      cash_after_e12:after,
      at_ms:bill.event_at_ms,
      bill_type:"2".into(),
      bill_sub_type:bill.sub_type.clone(),
      order_id:bill.order_id.clone(),
      trade_id:bill.trade_id.clone(),
      observed_signed_fee_without_sourced_ccy_e12:Some(bill.signed_fee_e12),
    });
 }
 Ok(V2OpeningBillAdapter{
    original_owner_generation:expected_generation,
    original_buy_bills:rows,
    independent_exchange_account_authenticated:false,
    full_owner_universe_proven:false,
    may_trade_real:false,
 })
}
/// Separate narrow BASE/USDT feeCcy path. The old four-leg PnL
/// function is USDT-only and MUST NOT price BASE charges as USDT.
/// Source-shaped rows and local receipts are not exchange-attested.
pub fn inspect_original_okx_asset_fee_bill_linkage(
 spot_buy:&LegFillLedger,spot_sell:&LegFillLedger,
 future_short:&LegFillLedger,originals:&[SpotFeeFill],
 bills:&[ObservedOkxBill],window:(u64,u64),
)->Result<BoundedNativeFeeInventory,ReceiptBindError>{
 if window.0==0||window.1<window.0||
     window.1-window.0>6*24*60*60*1000||
     bills.is_empty()||bills.len()>128||bills.len()!=originals.len(){
    return Err(bad("invalid one-original-bill-per-fill budget"))
 }
 let inventory=inspect_asset_fees_for_original_spot_ledgers(
    spot_buy,spot_sell,future_short,originals)
    .map_err(|_|bad("Native original asset-fee fill binding failed"))?;
 let base_ccy=spot_buy.symbol.split(['-','/']).next()
     .ok_or(bad("missing original base currency"))?;
 let mut by_trade=BTreeMap::new();
 let mut used_bill_ids=BTreeSet::new();
 for row in bills{
   let id=row.bill_id.parse::<u128>().ok().filter(|v|*v>0)
       .ok_or(bad("invalid original OKX bill identity"))?;
   if row.bill_id!=id.to_string()||
      !used_bill_ids.insert(row.bill_id.as_str())||
      by_trade.insert(row.trade_id.as_str(),row).is_some(){
       return Err(bad("duplicate or noncanonical original OKX bill"))
   }
 }
 for source in originals{
   let row=by_trade.get(source.trade_id.as_str())
       .ok_or(bad("original fee fill lacks its single OKX bill"))?;
   let kind=if source.side=="buy"{"1"}else{"2"};
   if row.order_id!=source.order_id||row.trade_id!=source.trade_id||
      row.bill_type!="2"||row.bill_sub_type!=kind||
      row.at_ms<source.at_ms||row.at_ms-source.at_ms>10_000||
      row.at_ms<window.0||row.at_ms>window.1{
       return Err(bad("original bill side/order/trade/timestamp mismatched"))
   }
   let gross=i128::try_from(source.gross_base_qty_e12)
      .map_err(|_|ReceiptBindError::Overflow)?;
   let product=source.gross_base_qty_e12.checked_mul(source.price_quote_e12)
      .ok_or(ReceiptBindError::Overflow)?;
   if product%SCALE_12!=0{
      return Err(bad("original quote movement has fractional e12"))
   }
   let quote=i128::try_from(product/SCALE_12)
      .map_err(|_|ReceiptBindError::Overflow)?;
   let debit=source.signed_fee_e12.checked_neg()
      .ok_or(ReceiptBindError::Overflow)?;
   let sign=if source.side=="buy"{1i128}else{-1i128};
   let mut expected_base=sign.checked_mul(gross)
      .ok_or(ReceiptBindError::Overflow)?;
   let mut expected_quote=(-sign).checked_mul(quote)
      .ok_or(ReceiptBindError::Overflow)?;
   if source.fee_ccy==base_ccy{
      expected_base=expected_base.checked_sub(debit)
          .ok_or(ReceiptBindError::Overflow)?;
   }else if source.fee_ccy=="USDT"{
      expected_quote=expected_quote.checked_sub(debit)
          .ok_or(ReceiptBindError::Overflow)?;
   }else{return Err(bad("unknown fee currency"))}
   let expected=if row.currency==base_ccy{expected_base}
       else if row.currency=="USDT"{expected_quote}
       else{return Err(bad("invalid ORIGINAL bill denomination"))};
   if row.signed_change_e12!=expected{
      return Err(bad("original bill differs from feeCcy-aware fill amount"))
   }
   if row.observed_signed_fee_without_sourced_ccy_e12
       .is_some_and(|signed|signed!=source.signed_fee_e12){
       return Err(bad("bill fee disagrees with original feeCcy trade detail"))
   }
   // No additional source bill or quote-valued BASE fee is fabricated.
 }
 Ok(inventory)
}
/// Source-only B8-shaped ORIGINAL bill. One bill, never a second synthetic
/// cash row for the other asset. No dependency on standalone B8 crate.
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum LocalB8BillLane{BaseOriginal,QuoteOriginal,BaseDelayed,QuoteDelayed}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct LocalB8OriginalBillShape{
 pub bill_id:String,pub trade_id:String,pub asset:String,
 pub signed_delta_e12:i128,
 pub lane:LocalB8BillLane,
}
/// Compare B8-shaped rows against the existing Native ORIGINAL bill IDs.
/// This checks source-model parity, not authenticated B1 account history.
pub fn require_b8_original_bill_parity_without_network(
 base_asset:&str,native_original_bills:&[ObservedOkxBill],b8_rows:&[LocalB8OriginalBillShape],
)->Result<(),ReceiptBindError>{
 if base_asset.is_empty()||base_asset=="USDT"||
    !base_asset.bytes().all(|b|b.is_ascii_uppercase()||b.is_ascii_digit())||
    native_original_bills.is_empty()||native_original_bills.len()>128||
    native_original_bills.len()!=b8_rows.len(){
   return Err(bad("B8 original bill count differs from Native"))
 }
 let mut by_id=BTreeMap::new();
 let mut native_trades=BTreeSet::new();
 for bill in native_original_bills{
    if !native_trades.insert(bill.trade_id.as_str())||
       by_id.insert(bill.bill_id.as_str(),bill).is_some(){
       return Err(bad("Native billId reused"))
    }
 }
 let mut seen=BTreeSet::new();
 let mut b8_trades=BTreeSet::new();
 for row in b8_rows{
   if !b8_trades.insert(row.trade_id.as_str())||
      !seen.insert(row.bill_id.as_str()){
      return Err(bad("B8 duplicated original source bill"))
   }
   let native=by_id.get(row.bill_id.as_str())
       .ok_or(bad("B8 source bill absent in Native original receipts"))?;
   if row.trade_id!=native.trade_id||
      row.asset!=native.currency||
      row.signed_delta_e12!=native.signed_change_e12{
      return Err(bad("B8 source trade/asset/amount differs from Native"))
   }
   let lane_matches=match row.lane{
      LocalB8BillLane::BaseOriginal=>row.asset==base_asset,
      LocalB8BillLane::QuoteOriginal=>row.asset=="USDT",
      LocalB8BillLane::BaseDelayed|LocalB8BillLane::QuoteDelayed=>false,
   };
   let canonical=row.bill_id.parse::<u128>()
       .ok().filter(|n|*n>0).is_some_and(|n|n.to_string()==row.bill_id);
   if !lane_matches||!canonical{
      return Err(bad("B8 original bill lane or identity invalid"))
   }
 }
 Ok(())
}
/// Each original spot fill has EXACTLY ONE original OKX billId (BASE OR
/// USDT). Other-currency movement is DERIVED from the original fill and
/// its independently persisted USDT fee, not fabricated as a second bill.
fn check_one_okx(
 fill:&FillEvent,ledger:&LegFillLedger,sign:i128,
 base:&BTreeMap<String,&ObservedOkxBill>,
 quote:&BTreeMap<String,&ObservedOkxBill>,
 used_original_ids:&mut BTreeSet<String>,
 window:(u64,u64),
)->Result<(i128,i128),ReceiptBindError>{
 let order=ledger.exchange_order_id.as_deref()
    .ok_or(bad("OKX exchange order ID absent"))?;
 let t=fill.trade_time_ms.ok_or(bad("trade timestamp missing"))?;
 let row=match (base.get(&fill.fill_id),quote.get(&fill.fill_id)){
     (Some(&r),None)|(None,Some(&r))=>r,
     (None,None)=>return Err(bad("original OKX bill for trade ID missing")),
     (Some(_),Some(_))=>return Err(bad("two original OKX bills for one trade forbidden")),
 };
 let subtype=if sign>0{"1"}else{"2"};
 let qty=i128::try_from(fill.qty_atoms).map_err(|_|ReceiptBindError::Overflow)?;
 if fill.fee_currency.as_deref()!=Some("USDT"){
     return Err(bad("non-USDT spot fee requires independently sourced feeCcy asset contract"))
 }
 let fee=fill.fee_cost_quote_atoms.ok_or(bad("missing spot fee"))?;
 if fee<0{return Err(bad("unsupported spot fee rebate"))}
 let expected_base=sign.checked_mul(qty).ok_or(ReceiptBindError::Overflow)?;
 let expected_quote=(-sign).checked_mul(mul(fill)?)
    .and_then(|v|v.checked_sub(fee)).ok_or(ReceiptBindError::Overflow)?;
 if row.order_id!=order||row.trade_id!=fill.fill_id
   ||row.bill_type!="2"||row.bill_sub_type!=subtype
   ||row.at_ms<window.0||row.at_ms>window.1
   ||row.at_ms<t||row.at_ms-t>10_000{
   return Err(bad("OKX original bill order, side, trade or timing mismatch"))
 }
 let base_ccy=ledger.symbol.split(['-','/']).next().unwrap_or("");
 let expected=if row.currency==base_ccy{expected_base}
   else if row.currency=="USDT"{expected_quote}
   else{return Err(bad("unsupported OKX original bill currency"))};
 if row.signed_change_e12!=expected{
   return Err(bad("original OKX bill differs from source fill fee-aware delta"))
 }
 // ccy is the BALANCE currency, not the fee currency; feeCcy is supplied
 // by the original FILL above. Never infer it from this account bill.
 if row.observed_signed_fee_without_sourced_ccy_e12.is_some_and(|x|x!=0){
    return Err(bad("nonzero bill-only fee not independently sourced"))
 }
 let numeric=row.bill_id.parse::<u128>().ok().filter(|v|*v>0)
   .ok_or(bad("original OKX billId invalid"))?;
 if row.bill_id!=numeric.to_string()
   ||!used_original_ids.insert(row.bill_id.clone()){
    return Err(bad("duplicate or noncanonical OKX billId"))
 }
 Ok((expected_base,expected_quote))
}
fn index_bills<'a>(
 rows:&'a [ObservedOkxBill],currency:&str,window:(u64,u64),
)->Result<BTreeMap<String,&'a ObservedOkxBill>,ReceiptBindError>{
 let mut ids=BTreeSet::new();let mut by_trade=BTreeMap::new();
 for row in rows{
    if row.currency!=currency
       ||row.bill_id.is_empty()
       ||!row.bill_id.bytes().all(|b|b.is_ascii_digit())
       ||row.at_ms<window.0||row.at_ms>window.1
       ||row.order_id.is_empty()||row.trade_id.is_empty()
       ||!ids.insert(&row.bill_id)
       ||by_trade.insert(row.trade_id.clone(),row).is_some(){
         return Err(bad("foreign/duplicate OKX currency bill or unsupported income"))
    }
 }
 Ok(by_trade)
}
fn source_futures<'a>(
 ledgers:&[&'a LegFillLedger;2],
)->Result<BTreeMap<String,(&'a FillEvent,bool)>,ReceiptBindError>{
 let mut out=BTreeMap::new();
 for (idx,ledger) in ledgers.iter().enumerate(){
    for (id,f) in &ledger.fills{
       if id!=&f.fill_id||id.is_empty()||out.insert(id.clone(),(f,idx==1)).is_some(){
          return Err(bad("missing or duplicate futures original trade IDs"))
       }
    }
 }
 if out.is_empty(){return Err(bad("missing futures fills"))}
 Ok(out)
}

/// Reconcile source-shaped receipts with four ORIGINAL opening/closing
/// fill ledgers. Does not make caller-supplied histories authenticated.
/// A strict, narrow supported case: USDT commissions, ONE original
/// BASE or USDT bill per OKX fill; unobserved other-currency cash movement
/// is derived from fill arithmetic, not treated as an original account bill.
/// No independent account-wide / externally authenticated completeness.
#[allow(clippy::too_many_arguments)]
pub fn reconcile_supplied_four_leg_receipts(
 spot_buy:&LegFillLedger,spot_sell:&LegFillLedger,
 future_open:&LegFillLedger,future_close:&LegFillLedger,
 reported_close_pnl:i128,window:(u64,u64),
 income:&[ObservedBinanceIncome],
 base_bills:&[ObservedOkxBill],quote_bills:&[ObservedOkxBill],
)->Result<BoundedReceiptBinding,ReceiptBindError>{
 if window.0==0||window.1<window.0
    ||window.1-window.0>6*24*60*60*1000{
    return Err(bad("cash receipt scope too large or missing"))
 }
 if spot_buy.venue!="okx"||spot_sell.venue!="okx"
    ||future_open.venue!="binance"||future_close.venue!="binance"
    ||spot_buy.side!="buy"||spot_sell.side!="sell"
    ||future_open.side!="sell"||future_close.side!="buy"{
    return Err(bad("unrecognized four-leg directions/venues"))
 }
 let root_currency=spot_buy.symbol.split(['-','/']).next()
    .ok_or(bad("missing base currency"))?;
 let base=index_bills(base_bills,root_currency,window)?;
 let quote=index_bills(quote_bills,"USDT",window)?;
 let mut used_original_bill_ids=BTreeSet::new();
 let mut spot_ids=BTreeSet::new();
 let(mut derived_base_sum,mut derived_quote_sum)=(0i128,0i128);
 for (ledger,sign) in [(spot_buy,1i128),(spot_sell,-1i128)]{
    for (id,f) in &ledger.fills{
       if id!=&f.fill_id||!spot_ids.insert(id.clone()){
          return Err(bad("duplicate OKX execution trade IDs"))
       }
       let(base_delta,quote_delta)=check_one_okx(f,ledger,sign,
          &base,&quote,&mut used_original_bill_ids,window)?;
       derived_base_sum=derived_base_sum.checked_add(base_delta)
          .ok_or(ReceiptBindError::Overflow)?;
       derived_quote_sum=derived_quote_sum.checked_add(quote_delta)
          .ok_or(ReceiptBindError::Overflow)?;
    }
 }
 if spot_ids.len()!=base.len()+quote.len()
     ||spot_ids.len()!=used_original_bill_ids.len(){
    return Err(bad("extra or missing ORIGINAL OKX billId"))
 }
 let fut_trades=source_futures(&[future_open,future_close])?;
 let mut commissions=BTreeMap::<String,i128>::new();let mut realized=BTreeMap::<String,i128>::new();
 let mut row_keys=BTreeSet::new();
 let mut funding=0i128;let mut transfer=0i128;
 let mut actual_commissions=0i128;let mut futures_realized=0i128;
 for r in income{
    if r.at_ms<window.0||r.at_ms>window.1
       ||r.asset!="USDT"||r.tran_id.is_empty()
       ||!r.tran_id.bytes().all(|b|b.is_ascii_digit())
       ||!row_keys.insert((r.income_type.clone(),r.tran_id.clone())){
       return Err(bad("duplicate or foreign Binance transaction"))
    }
    match r.income_type.as_str(){
       "COMMISSION"|"REALIZED_PNL"=>{
           if r.symbol!=future_open.symbol{
              return Err(bad("Binance trade income symbol mismatch"))
           }
           let (fill,closing)=fut_trades.get(&r.trade_id)
              .ok_or(bad("Binance income tradeId absent from original ledgers"))?;
           let fill_time=fill.trade_time_ms
              .ok_or(bad("Binance trade lacks source time"))?;
           if r.at_ms<fill_time||r.at_ms-fill_time>180_000{
              return Err(bad("Binance income posting not bounded to execution"))
           }
           if r.income_type=="COMMISSION"{
              // Binance transaction identity is (incomeType, tranId),
              // not tradeId; more than one independently identified
              // negative fee posting may reconcile to ONE execution.
              // Every event must be nonpositive; unknown rebates are
              // NOT silently offset against the trade's fee.
              if r.signed_amount_e12>=0{
                 return Err(bad("nonnegative Binance trade fee/rebate needs separate source"))
              }
              let acc=commissions.entry(r.trade_id.clone()).or_default();
              *acc=acc.checked_add(r.signed_amount_e12)
                  .ok_or(ReceiptBindError::Overflow)?;
              actual_commissions=actual_commissions
                  .checked_sub(r.signed_amount_e12)
                  .ok_or(ReceiptBindError::Overflow)?;
           }else{
              if !*closing{
                 return Err(bad("realized income belongs to opening trade"))
              }
              let acc=realized.entry(r.trade_id.clone()).or_default();
              *acc=acc.checked_add(r.signed_amount_e12)
                  .ok_or(ReceiptBindError::Overflow)?;
              futures_realized=futures_realized.checked_add(r.signed_amount_e12)
                 .ok_or(ReceiptBindError::Overflow)?;
           }
       },
       "FUNDING_FEE"=>{
           if r.symbol!=future_open.symbol||!r.trade_id.is_empty(){
              return Err(bad("Binance funding not scoped to this position symbol"))
           }
           funding=funding.checked_add(r.signed_amount_e12)
              .ok_or(ReceiptBindError::Overflow)?;
       },
       "TRANSFER"|"INTERNAL_TRANSFER"=>{
           if !r.symbol.is_empty()||!r.trade_id.is_empty(){
              return Err(bad("unclassified transfer mislabelled as instrument profit"))
           }
           transfer=transfer.checked_add(r.signed_amount_e12)
              .ok_or(ReceiptBindError::Overflow)?;
       },
       _=>return Err(bad("unrecognized income category needs manual proof")),
    }
 }
 let future_ids=fut_trades.keys().cloned().collect::<BTreeSet<_>>();
 let close_ids=future_close.fills.keys().cloned().collect::<BTreeSet<_>>();
 if commissions.keys().cloned().collect::<BTreeSet<_>>()!=future_ids
    ||realized.keys().cloned().collect::<BTreeSet<_>>()!=close_ids
    ||futures_realized!=reported_close_pnl{
    return Err(bad("Binance commission/PnL not fully matched to exact trades"))
 }
 // Confirm every signed fee group equals that EXACT filled execution's
 // original USDT fee cost; no cross-trade offsetting permitted.
 for (tid,amount) in &commissions{
    let cost=fut_trades.get(tid).and_then(|(f,_)|f.fee_cost_quote_atoms)
       .ok_or(bad("futures original per-fill fee source missing"))?;
    if cost<=0||cost.checked_neg()!=Some(*amount){
       return Err(bad("Binance fee component total differs from original trade"))
    }
 }
 let wanted=future_open.fee_cost_quote_atoms.checked_add(
    future_close.fee_cost_quote_atoms).ok_or(ReceiptBindError::Overflow)?;
 if wanted!=actual_commissions {
    return Err(bad("Binance summed commissions differ from four-leg ledger"))
 }
 // Invoke the independently tested TRADE2F1 calculator with the
 // classified funding only. No caller-supplied wallet transfer as profit.
 let m=ObservedMoneyMovements{
    bounded_funding_usdt_e12:Some(funding),
    observed_external_flow_usdt_e12:None,
    combined_spot_and_futures_usdt_wallet_delta_e12:None,
    internal_transfer_volume_usdt_e12:None,
 };
 let arithmetic:NonAuthorizingTrade2F=calculate_bounded_trade_result(
    spot_buy,spot_sell,future_open,future_close,reported_close_pnl,&m)
    .map_err(|_|bad("TRADE2F1 four-leg source arithmetic mismatch"))?;
 // Counts include only original bills; neither derived movement is
 // emitted as a new bill or booked on top of existing source balChg.
 let base_sum=derived_base_sum;
 if base_sum!=0{
    return Err(bad("OKX original roundtrip derived base quantity not balanced"))
 }
 let quote_sum=derived_quote_sum;
 let spot_fee=spot_buy.fee_cost_quote_atoms.checked_add(
    spot_sell.fee_cost_quote_atoms).ok_or(ReceiptBindError::Overflow)?;
 if quote_sum!=arithmetic.spot_gross_e12.checked_sub(spot_fee)
     .ok_or(ReceiptBindError::Overflow)?{
     return Err(bad("OKX quote movement not spot gross minus exact fees"))
 }
 let adjusted=arithmetic.provisional_net_with_observed_bounded_funding_e12
    .ok_or(bad("bounded funding unaccounted"))?;
 Ok(BoundedReceiptBinding{
    spot_fill_ids:spot_ids.into_iter().collect(),
    futures_fill_ids:future_ids.into_iter().collect(),
    spot_base_delta_e12:base_sum,spot_quote_delta_e12:quote_sum,
    binance_commissions_e12:actual_commissions,
    futures_realized_income_e12:futures_realized,
    observed_bounded_funding_e12:funding,
    observed_unclassified_transfer_e12:transfer,
    trade_only_net_e12:arithmetic.trade_only_net_before_funding_e12,
    window_adjusted_net_e12:adjusted,
    supplied_trade_and_cash_receipts_reconcile:true,
    independent_live_source_attested:false,
    complete_retained_income_history_proven:false,
    complete_wallet_transfer_history_proven:false,
    all_lifetime_funding_proven:false,
    definitive_lifetime_net_pnl_e12:None,
    may_send_cancel_retry_or_verify_closed:false,
 })
}
