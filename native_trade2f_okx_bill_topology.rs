//! TRADE2F2F: actual OKX API-shaped transaction details -> billId ->
//! balance bill, with feeCcy explicitly sourced from the TRANSACTION.
//! A bill's ccy is its balChg currency; it is NOT proof of fee currency.
//! Pure fake-source diagnostic. No real GET, PnL rights or new trade APIs.
use std::collections::{BTreeMap,BTreeSet};
use serde_json::Value;
use crate::{
    fill_economics::{decimal_to_atoms,signed_decimal_to_atoms},
    native_trade2f_asset_fee::{
      inspect_native_spot_asset_fee_inventory,BoundedNativeFeeInventory,
      SpotFeeFill,
    },
};
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum OkxBillTopologyError{
    Invalid(&'static str),Overflow,
}
fn bad(x:&'static str)->OkxBillTopologyError{OkxBillTopologyError::Invalid(x)}
fn field<'a>(v:&'a Value,k:&str)->Result<&'a str,OkxBillTopologyError>{
    v.get(k).and_then(Value::as_str).ok_or(bad("OKX documented field missing"))
}
fn unsigned(v:&Value,k:&str)->Result<u128,OkxBillTopologyError>{
    decimal_to_atoms(field(v,k)?).map_err(|_|bad("OKX decimal quantity malformed"))
}
fn signed(v:&Value,k:&str)->Result<i128,OkxBillTopologyError>{
    signed_decimal_to_atoms(field(v,k)?).map_err(|_|bad("OKX signed decimal malformed"))
}
fn time(v:&Value,k:&str)->Result<u64,OkxBillTopologyError>{
    field(v,k)?.parse::<u64>().ok().filter(|x|*x>0)
      .ok_or(bad("OKX time missing or invalid"))
}
fn id(v:&Value,k:&str)->Result<String,OkxBillTopologyError>{
    let s=field(v,k)?;
    if s.is_empty()||!s.bytes().all(|b|b.is_ascii_digit())
       ||s=="0"{return Err(bad("OKX trade/order/bill id missing or invalid"))}
    Ok(s.to_string())
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct OkxDetailBillWitness{
    pub bill_id:String,
    pub trade_id:String,
    pub order_id:String,
    pub fee_currency_from_trade_detail:String,
    pub bill_balance_currency:String,
    pub signed_fee_e12:i128,
    pub signed_bill_balance_change_e12:i128,
    pub bill_posted_after_trade_ms:u64,
}
/// Two cash-equivalent movements calculated from ONE original OKX fill.
/// Only original_bill_id is an exchange-shaped bill identity. The other
/// currency delta is DERIVED arithmetic, never a second exchange bill.
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct DerivedOkxCashPair{
    pub original_bill_id:String,
    pub original_trade_id:String,
    pub original_bill_ccy:String,
    pub original_bill_balance_change_e12:i128,
    pub derived_base_delta_e12:i128,
    pub derived_quote_delta_e12:i128,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct DiagnosticOkxTopology{
    pub matched_bill_ids:Vec<String>,
    pub exact_detail_bill_witnesses:Vec<OkxDetailBillWitness>,
    /// Not raw source bills, not additional money to book alongside bills.
    pub per_fill_derived_cash_not_source_bills:Vec<DerivedOkxCashPair>,
    pub observed_bill_delta_by_ccy_e12:BTreeMap<String,i128>,
    pub fee_adjusted_inventory:BoundedNativeFeeInventory,
    pub one_bill_per_trade_is_general_exchange_guarantee:bool,
    pub all_base_quote_account_movements_proven_complete:bool,
    pub fee_currency_inferred_from_bill_ccy:bool,
    pub independent_authenticated_signed_history:bool,
    pub real_wallet_flatness_proven:bool,
    pub quote_valuation_of_base_fee_proven:bool,
    pub full_lifetime_net_pnl_e12:Option<i128>,
    pub may_send_cancel_retry_or_mark_verified_closed:bool,
}
/// Caller provides *raw* documented OKX REST objects. This function
/// cannot prove the caller ever fetched them, or that any other cash
/// bills, pending orders or delayed posts do not exist.
pub fn inspect_documented_okx_trade_detail_to_bill_id(
    original_spot_symbol:&str,
    original_futures_short_gross_qty_e12:u128,
    start_ms:u64,end_ms:u64,
    trade_details:&[Value],
    account_bills:&[Value],
)->Result<DiagnosticOkxTopology,OkxBillTopologyError>{
    let pair=original_spot_symbol.replace('/',"-");
    let mut tokens=pair.split('-');
    let base=tokens.next().unwrap_or("");
    if base.is_empty()||tokens.next()!=Some("USDT")||tokens.next().is_some()
      ||start_ms==0||end_ms<start_ms||end_ms-start_ms>6*24*60*60*1000
      ||trade_details.is_empty()||trade_details.len()>64
      ||account_bills.is_empty()||account_bills.len()>128{
        return Err(bad("unsupported narrow pair, time window or row budget"))
    }
    let mut fills=Vec::new();
    let mut observed=BTreeMap::new();
    let mut witnesses=Vec::new();
    let mut derived_cash=Vec::new();
    let mut fills_by_bill=BTreeMap::new();
    let mut used_trade_ids=BTreeSet::new();
    for v in trade_details{
        if field(v,"instType")?!="SPOT"||field(v,"instId")?!=pair
            ||field(v,"tradeQuoteCcy")?!="USDT"{
            return Err(bad("foreign instrument or non-USDT trade quote currency"))
        }
        let bill=id(v,"billId")?;
        let tid=id(v,"tradeId")?;
        let ord=id(v,"ordId")?;
        let side=field(v,"side")?;
        if !["buy","sell"].contains(&side)
            ||field(v,"subType")?!=if side=="buy"{"1"}else{"2"}{
            return Err(bad("OKX trade details subtype/side mismatch"))
        }
        let exec=time(v,"fillTime")?;
        let recorded=time(v,"ts")?;
        if exec<start_ms||exec>end_ms||recorded<exec||recorded>end_ms{
            return Err(bad("transaction details time out of original range"))
        }
        let fee_ccy=field(v,"feeCcy")?;
        if fee_ccy!=base&&fee_ccy!="USDT"{
            return Err(bad("unsupported third-asset OKX commission"))
        }
        let fee=signed(v,"fee")?;
        if fee>0{return Err(bad("OKX rebate needs separate source valuation"))}
        if !used_trade_ids.insert(tid.clone())||fills_by_bill.contains_key(&bill){
            return Err(bad("duplicate transaction detail trade or bill ID"))
        }
        let f=SpotFeeFill{
            order_id:ord.clone(),trade_id:tid.clone(),at_ms:exec,
            side:side.to_string(),inst_id:pair.clone(),
            price_quote_e12:unsigned(v,"fillPx")?,
            gross_base_qty_e12:unsigned(v,"fillSz")?,
            fee_ccy:fee_ccy.into(),signed_fee_e12:fee,
        };
        fills_by_bill.insert(bill.clone(),f.clone());
        fills.push(f);
    }
    let mut bill_ids=BTreeSet::new();
    let mut previous_bal_by_ccy=BTreeMap::<String,i128>::new();
    let mut last_bill_id=None::<u128>;
    let mut last_bill_time=None::<u64>;
    // The caller must provide bills in reverse-chronological order per
    // OKX API. A missing interleaved page can never be certified by
    // this local consistency check; observed completeness stays FALSE.
    for v in account_bills{
        let bill=id(v,"billId")?;
        let numeric=bill.parse::<u128>().map_err(|_|bad("bill id overflow"))?;
        if last_bill_id.is_some_and(|prev|numeric>=prev){
            return Err(bad("OKX reverse chronological billId order broken"))
        }
        last_bill_id=Some(numeric);
        if !bill_ids.insert(bill.clone()){
            return Err(bad("same cash bill repeated"))
        }
        if field(v,"instType")?!="SPOT"||field(v,"instId")?!=pair
            ||field(v,"type")?!="2"{
            return Err(bad("unrelated cash bill or unsupported account movement"))
        }
        let ccy=field(v,"ccy")?;
        if ccy!=base&&ccy!="USDT"{
            return Err(bad("bill cash balance currency unsupported"))
        }
        let ts=time(v,"ts")?;
        if ts<start_ms||ts>end_ms
          ||last_bill_time.is_some_and(|prev|ts>prev){
            return Err(bad("bill timestamp not reverse chronological or out of window"))
        }
        last_bill_time=Some(ts);
        let change=signed(v,"balChg")?;
        let cash=signed(v,"bal")?;
        if let Some(expected)=previous_bal_by_ccy.get(ccy){
            if *expected!=cash{
                return Err(bad("visible reverse-chronological balance chain broken"))
            }
        }
        previous_bal_by_ccy.insert(ccy.into(),
            cash.checked_sub(change).ok_or(OkxBillTopologyError::Overflow)?);
        let acc=observed.entry(ccy.to_string()).or_insert(0i128);
        *acc=acc.checked_add(change).ok_or(OkxBillTopologyError::Overflow)?;
        // This narrow reporter requires an exact fill billId match.
        // It neither demands nor invents a second same-trade bill in
        // another currency; additional unlinked bills remain DENY.
        let f=fills_by_bill.get(&bill)
          .ok_or(bad("unmatched billId: cash event not attributed to original trade"))?;
        if field(v,"ordId")?!=f.order_id
            ||field(v,"tradeId")?!=f.trade_id
            ||field(v,"subType")?!=(if f.side=="buy"{"1"}else{"2"}){
            return Err(bad("cash bill order/trade identity or direction mismatch"))
        }
        if ts<f.at_ms||ts-f.at_ms>10_000{
            return Err(bad("bill posting too early or delayed; manual review"))
        }
        // Account bills report signed 'fee' but do not supply the
        // separate 'feeCcy' required to price it. Its numerical value
        // can be compared with the transaction detail's signed fee;
        // NEVER infer its currency from bill ccy.
        let fee=signed(v,"fee")?;
        if fee!=f.signed_fee_e12{
            return Err(bad("cash bill fee magnitude differs from transaction detail fee"))
        }
        // Deliberately bounded source shape: the ONE observed bill
        // corresponds to that currency's whole per-fill movement.
        // Other valid OKX multi-bill topologies are rejected MANUAL.
        // The bill's balance ccy NEVER sets the fee's denomination.
        let qty=i128::try_from(f.gross_base_qty_e12)
            .map_err(|_|OkxBillTopologyError::Overflow)?;
        let mul=f.gross_base_qty_e12.checked_mul(f.price_quote_e12)
            .ok_or(OkxBillTopologyError::Overflow)?;
        if mul%crate::fill_economics::SCALE_12!=0{
            return Err(bad("OKX bill quote cashflow cannot be rounded"))
        }
        let notional=i128::try_from(mul/crate::fill_economics::SCALE_12)
            .map_err(|_|OkxBillTopologyError::Overflow)?;
        // Both derivatives stem from this single ORIGINAL fill and billId.
        // They are not two cash-source records. Fee is applied in feeCcy.
        let base_principal=if f.side=="buy"{qty}else{-qty};
        let quote_principal=if f.side=="buy"{
            notional.checked_neg().ok_or(OkxBillTopologyError::Overflow)?
        }else{notional};
        let derived_base=base_principal.checked_add(
            if f.fee_ccy==base{fee}else{0}
        ).ok_or(OkxBillTopologyError::Overflow)?;
        let derived_quote=quote_principal.checked_add(
            if f.fee_ccy=="USDT"{fee}else{0}
        ).ok_or(OkxBillTopologyError::Overflow)?;
        let expected=if ccy==base{derived_base}else{derived_quote};
        if change!=expected{
           return Err(bad("visible bill cash change disagrees with exact fee-adjusted trade"))
        }
        derived_cash.push(DerivedOkxCashPair{
            original_bill_id:bill.clone(),original_trade_id:f.trade_id.clone(),
            original_bill_ccy:ccy.to_string(),
            original_bill_balance_change_e12:change,
            derived_base_delta_e12:derived_base,
            derived_quote_delta_e12:derived_quote,
        });
        witnesses.push(OkxDetailBillWitness{
            bill_id:bill,
            trade_id:f.trade_id.clone(),order_id:f.order_id.clone(),
            fee_currency_from_trade_detail:f.fee_ccy.clone(),
            bill_balance_currency:ccy.to_string(),
            signed_fee_e12:fee,signed_bill_balance_change_e12:change,
            bill_posted_after_trade_ms:ts-f.at_ms,
        });
    }
    if bill_ids.len()!=fills_by_bill.len()
       ||fills_by_bill.keys().any(|k|!bill_ids.contains(k)){
       return Err(bad("not every original trade detail billId was retrieved"))
    }
    let inv=inspect_native_spot_asset_fee_inventory(
        &pair,original_futures_short_gross_qty_e12,&fills)
        .map_err(|_|bad("fee-adjusted BNT cash/inventory not internally consistent"))?;
    // Cross-check the projections against the ALREADY EXISTING fee-aware
    // inventory implementation; do not introduce another PnL engine.
    let mut sums=(0i128,0i128);
    for row in &derived_cash{
        sums.0=sums.0.checked_add(row.derived_base_delta_e12)
            .ok_or(OkxBillTopologyError::Overflow)?;
        sums.1=sums.1.checked_add(row.derived_quote_delta_e12)
            .ok_or(OkxBillTopologyError::Overflow)?;
    }
    if sums.0!=inv.aggregate_base_wallet_delta_e12
        ||sums.1!=inv.aggregate_quote_wallet_delta_e12{
        return Err(bad("derived per-fill currencies differ from existing fee-aware inventory"))
    }
    Ok(DiagnosticOkxTopology{
        matched_bill_ids:bill_ids.into_iter().collect(),
        exact_detail_bill_witnesses:witnesses,
        per_fill_derived_cash_not_source_bills:derived_cash,
        observed_bill_delta_by_ccy_e12:observed,
        fee_adjusted_inventory:inv,
        one_bill_per_trade_is_general_exchange_guarantee:false,
        all_base_quote_account_movements_proven_complete:false,
        fee_currency_inferred_from_bill_ccy:false,
        independent_authenticated_signed_history:false,
        real_wallet_flatness_proven:false,
        quote_valuation_of_base_fee_proven:false,
        full_lifetime_net_pnl_e12:None,
        may_send_cancel_retry_or_mark_verified_closed:false,
    })
}
