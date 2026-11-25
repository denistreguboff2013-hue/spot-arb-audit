//! TRADE2F2A: signed-GET (FAKE executor only) bounded income/bill windows.
//! NO definitive lifetime PnL, no transfer automation, no trading authority.
use std::collections::BTreeMap;
use serde_json::Value;
use crate::{
 authoritative_state::{NativeStateStore,NativeStateError,NativeEntryPhase,NativeAuthoritativeState},
 fill_economics::signed_decimal_to_atoms,
 private_https::{HttpExecutor,HttpsRequest,HttpResponse,TransportError},
 private_rest::{
    OkxCredentials,BinanceCredentials,OkxDomain,HttpMethod,
    SignedRequest,RestBuildError,
    build_binance_all_income_page_request,
    build_okx_base_currency_bills_page_request,
 },
};
const MAX_WINDOW_MS:u64=6*24*60*60*1000;
const MAX_RETENTION_MS:u64=89*24*60*60*1000;
const MAX_PAGES:u16=4;
const BINANCE_PAGE:u16=1000;
const OKX_PAGE:u16=100;
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum MoneyWindowError{
 State(NativeStateError),Build(RestBuildError),
 Transport(TransportError),Invalid(&'static str),
}
fn deny(s:&'static str)->MoneyWindowError{MoneyWindowError::Invalid(s)}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct ObservedBinanceIncome{
 pub income_type:String,pub tran_id:String,pub symbol:String,
 pub asset:String,pub signed_amount_e12:i128,pub at_ms:u64,
 pub trade_id:String,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct ObservedOkxBill{
 pub bill_id:String,pub currency:String,pub signed_change_e12:i128,
 pub cash_after_e12:i128,pub at_ms:u64,pub bill_type:String,
 pub bill_sub_type:String,pub order_id:String,pub trade_id:String,
 /// OKX bill fee signed amount WITHOUT its separate feeCcy.
 /// The bill's ccy identifies balChg currency only; the commission
 /// currency needs a matching transaction detail feeCcy source.
 /// None is unknown, never proof of a zero original commission.
 pub observed_signed_fee_without_sourced_ccy_e12:Option<i128>,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub(crate) struct Snapshot{
 pub(crate) binance:BTreeMap<(String,String),ObservedBinanceIncome>,
 pub(crate) okx_base:BTreeMap<String,ObservedOkxBill>,
 pub(crate) okx_usdt:BTreeMap<String,ObservedOkxBill>,
 pub(crate) binance_pages:u16,pub(crate) okx_base_pages:u16,pub(crate) okx_quote_pages:u16,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct BoundedMoneyWindowReport{
 pub source_generation:u64,
 pub window_start_ms:u64,pub window_end_ms:u64,
 pub verified_identical_scans:u8,
 pub signed_fake_gets:u16,
 pub binance_income_rows:usize,
 pub okx_base_bill_rows:usize,pub okx_quote_bill_rows:usize,
 pub binance_funding_rows:usize,
 pub observed_binance_funding_signed_usdt_e12:i128,
 pub observed_binance_transfer_rows:usize,
 pub observed_binance_transfer_signed_usdt_e12:i128,
 pub observed_okx_base_signed_change_e12:i128,
 pub observed_okx_usdt_signed_change_e12:i128,
 /// Inclusion in history does not independently bind money to OUR
 /// opening and closing trade IDs. This cannot be authoritative PnL.
 pub binance_trade_commissions_and_realized_pnl_matched_to_four_legs:bool,
 pub binance_transfers_proven_external_not_internal:bool,
 pub okx_bill_types_matched_to_all_four_trade_fills:bool,
 pub all_future_funding_between_open_and_close_proven:bool,
 pub complete_cash_deposits_withdrawals_proven:bool,
 pub full_lifetime_profit_proven:bool,
 pub no_late_income_or_new_order_proven:bool,
 pub may_send_cancel_retry_or_verify_closed:bool,
}
fn scalar(v:&Value,k:&str)->Result<String,MoneyWindowError>{
 let x=v.get(k).ok_or(deny("missing cash record field"))?;
 if let Some(s)=x.as_str(){return Ok(s.into())}
 if let Some(n)=x.as_u64(){return Ok(n.to_string())}
 Err(deny("cash history type mismatch"))
}
fn at(v:&Value,k:&str,start:u64,end:u64)->Result<u64,MoneyWindowError>{
 let x=scalar(v,k)?.parse::<u64>().map_err(|_|deny("cash record timestamp malformed"))?;
 if x<start||x>end{return Err(deny("cash record outside original window"))}
 Ok(x)
}
fn signed(v:&Value,k:&str)->Result<i128,MoneyWindowError>{
 signed_decimal_to_atoms(&scalar(v,k)?)
   .map_err(|_|deny("cash amount missing, overprecise or overflow"))
}
fn fetch<E:HttpExecutor>(http:&E,req:&SignedRequest)->Result<Value,MoneyWindowError>{
 if req.method!=HttpMethod::Get||req.is_mutation||req.body.is_some(){
    return Err(deny("money review cannot make exchange mutation"))
 }
 let HttpResponse{status,body,..}=http.execute_request(&HttpsRequest::from_signed(req))
    .map_err(MoneyWindowError::Transport)?;
 if status!=200{return Err(deny("private cash read failed"))}
 serde_json::from_str(&body).map_err(|_|deny("cash response not JSON"))
}
fn binance<E:HttpExecutor>(
 http:&E,creds:&BinanceCredentials,now:u64,recv:u64,
 start:u64,end:u64,future_symbol:&str,
)->Result<(BTreeMap<(String,String),ObservedBinanceIncome>,u16),MoneyWindowError>{
 let mut out=BTreeMap::new();
 for page in 1..=MAX_PAGES{
   let req=build_binance_all_income_page_request(
     creds,now,recv,start,end,page,BINANCE_PAGE).map_err(MoneyWindowError::Build)?;
   let result=fetch(http,&req)?;
   let rows=result.as_array().ok_or(deny("Binance income not JSON array"))?;
   if rows.len()>BINANCE_PAGE as usize{
      return Err(deny("Binance income page exceeds limit"))
   }
   if rows.is_empty(){
      return Ok((out,page))
   }
   for row in rows{
      let kind=scalar(row,"incomeType")?;
      let tran=scalar(row,"tranId")?;
      if tran.is_empty()||tran=="0"||!tran.bytes().all(|b|b.is_ascii_digit()){
          return Err(deny("Binance income transaction identity missing"))
      }
      let symbol=scalar(row,"symbol")?;
      let asset=scalar(row,"asset")?;
      if asset!="USDT"{
          return Err(deny("non-USDT future income requires fee FX/source audit"))
      }
      let amount=signed(row,"income")?;
      let time=at(row,"time",start,end)?;
      let trade_id=scalar(row,"tradeId")?;
      // Presence of any unsupported income class could indicate hidden
      // cash change: never classify it as zero or as arbitrage PnL.
      match kind.as_str(){
        "FUNDING_FEE"|"COMMISSION"|"REALIZED_PNL"=>{
          if symbol!=future_symbol{
             return Err(deny("other futures symbol income contaminates pair scope"))
          }
        },
        "TRANSFER"|"INTERNAL_TRANSFER"=>{
          if !symbol.is_empty(){
             return Err(deny("nontrading transfer has unexpected instrument scope"))
          }
        },
        _=>return Err(deny("unexpected Binance income class needs manual audit")),
      }
      if out.insert((kind.clone(),tran.clone()),ObservedBinanceIncome{
        income_type:kind,tran_id:tran,symbol,asset,
        signed_amount_e12:amount,at_ms:time,trade_id
      }).is_some(){
         return Err(deny("duplicate Binance income (incomeType,tranId)"))
      }
   }
 }
 Err(deny("Binance all-income pagination budget exhausted without empty tail"))
}
fn okx_currency<E:HttpExecutor>(
 http:&E,creds:&OkxCredentials,now:u64,domain:OkxDomain,
 start:u64,end:u64,currency:&str,spot_symbol:&str,
)->Result<(BTreeMap<String,ObservedOkxBill>,u16),MoneyWindowError>{
 let mut out=BTreeMap::new();
 let mut after=None::<String>;
 let mut old_bill=None::<u128>;let mut old_time=None::<u64>;
 let mut older_cash_expected=None::<i128>;
 for page in 1..=MAX_PAGES{
    let req=build_okx_base_currency_bills_page_request(
       creds,now,domain,currency,start,end,after.as_deref(),OKX_PAGE)
       .map_err(MoneyWindowError::Build)?;
    let result=fetch(http,&req)?;
    if result.get("code").and_then(Value::as_str)!=Some("0"){
       return Err(deny("OKX currency bill business code nonzero"))
    }
    let rows=result.get("data").and_then(Value::as_array)
        .ok_or(deny("OKX currency bill array absent"))?;
    if rows.len()>OKX_PAGE as usize{
       return Err(deny("OKX currency bill page >limit"))
    }
    if rows.is_empty(){return Ok((out,page))}
    for row in rows{
        if scalar(row,"ccy")?!=currency{
           return Err(deny("wrong OKX bill currency"))
        }
        let bill=scalar(row,"billId")?;
        let n=bill.parse::<u128>().ok().filter(|n|*n>0)
           .ok_or(deny("invalid OKX bill ID"))?;
        if old_bill.is_some_and(|prev|n>=prev){
           return Err(deny("OKX bill IDs not strictly descending"))
        }
        let ts=at(row,"ts",start,end)?;
        if old_time.is_some_and(|prev|ts>prev){
           return Err(deny("OKX cash bills nonmonotonic timestamps"))
        }
        let cash=signed(row,"bal")?;
        let change=signed(row,"balChg")?;
        if older_cash_expected.is_some_and(|expect|expect!=cash){
           return Err(deny("OKX bill cash balance discontinuity"))
        }
        older_cash_expected=Some(
           cash.checked_sub(change).ok_or(MoneyWindowError::Invalid("cash rollback overflow"))?
        );
        let type_id=scalar(row,"type")?;
        let sub_type=scalar(row,"subType")?;
        if type_id.is_empty()||sub_type.is_empty(){
           return Err(deny("OKX bill kind missing"))
        }
        // The OKX bill documents a signed fee but DOES NOT establish
        // its asset denomination from bill ccy: ccy is balance-change
        // asset. Real feeCcy is in the transaction details. No
        // bill-only source may claim a nonzero USDT fee without it.
        let fee=match row.get("fee"){
           None=>None,
           Some(v) if v.as_str()==Some("")=>None,
           Some(_)=>Some(signed(row,"fee")?),
        };
        if type_id=="2"{
           if row.get("instType").is_some()
              &&row.get("instType").and_then(Value::as_str)!=Some("SPOT"){
              return Err(deny("OKX cash bill trade claims non-SPOT instrument"))
           }
           if let Some(v)=row.get("instId"){
              let got=v.as_str().ok_or(deny("OKX trading bill instId not string"))?;
              let normalized=spot_symbol.replace('/',"-");
              if got!=normalized{
                 return Err(deny("OKX trading bill not original spot instrument"))
              }
           }
           if fee.is_some_and(|amount|amount!=0){
               return Err(deny("OKX bill nonzero fee requires linked transaction detail feeCcy"))
           }
        }
        let record=ObservedOkxBill{
           bill_id:bill.clone(),currency:currency.into(),
           signed_change_e12:change,cash_after_e12:cash,at_ms:ts,
           bill_type:type_id,bill_sub_type:sub_type,
           order_id:scalar(row,"ordId")?,trade_id:scalar(row,"tradeId")?,
           observed_signed_fee_without_sourced_ccy_e12:fee,
        };
        if out.insert(bill.clone(),record).is_some(){
           return Err(deny("repeated OKX bill ID"))
        }
        old_bill=Some(n);old_time=Some(ts);after=Some(bill);
    }
 }
 Err(deny("OKX bill archive pagination not proven exhausted"))
}
/// This type cannot be constructed outside the crate. The only source
/// is the double-read, Native-owner-checked signed MOCK GET collector.
#[derive(Clone,Debug,PartialEq,Eq)]
pub(crate) struct OwnerBoundMoneyRows{
 pub(crate) original:NativeAuthoritativeState,
 pub(crate) start_ms:u64,
 pub(crate) end_ms:u64,
 pub(crate) rows:Snapshot,
}
#[allow(clippy::too_many_arguments)]
pub(crate) fn collect_double_money_rows<E:HttpExecutor>(
 http:&E,store:&NativeStateStore,gen:u64,
 okx:&OkxCredentials,bn:&BinanceCredentials,
 now:u64,domain:OkxDomain,recv:u64,
)->Result<OwnerBoundMoneyRows,MoneyWindowError>{
 let original=store.load().map_err(MoneyWindowError::State)?
   .ok_or(MoneyWindowError::State(NativeStateError::Generation))?;
 if original.generation!=gen||original.active.is_some()
   ||recv==0||recv>10_000{
   return Err(deny("invalid Native owner or signed cash window before GET"))
 }
 let entry=original.entry.as_ref().ok_or(deny("original trade not found"))?;
 if entry.phase!=NativeEntryPhase::RecoveryRequired
   ||entry.recovery_checkpoint.is_none()
   ||entry.spot_prehedge_evidence.is_none(){
   return Err(deny("original opening legs not evidenced"))
 }
 let start=entry.plan.created_at_ms;
 if start==0||now<start||now-start>MAX_WINDOW_MS
    ||now-start>MAX_RETENTION_MS{
    return Err(deny("bounded money review outside supported history window"))
 }
 let scan=| | ->Result<Snapshot,MoneyWindowError>{
    let (bi,bp)=binance(http,bn,now,recv,start,now,&entry.plan.futures_symbol)?;
    let (ob,op)=okx_currency(http,okx,now,domain,start,now,
           &entry.plan.base,&entry.plan.spot_symbol)?;
    let (oq,qp)=okx_currency(http,okx,now,domain,start,now,
           "USDT",&entry.plan.spot_symbol)?;
    Ok(Snapshot{
       binance:bi,okx_base:ob,okx_usdt:oq,
       binance_pages:bp,okx_base_pages:op,okx_quote_pages:qp,
    })
 };
 let first=scan()?;
 let second=scan()?;
 if first!=second{
   return Err(deny("Binance income / OKX bills drift between independent rounds"))
 }
 if store.load().map_err(MoneyWindowError::State)?.as_ref()!=Some(&original){
   return Err(deny("Native owner advanced during money history reads"))
 }
 Ok(OwnerBoundMoneyRows{
    original,start_ms:start,end_ms:now,rows:first,
 })
}
#[allow(clippy::too_many_arguments)]
pub fn inspect_bounded_mock_cash_history<E:HttpExecutor>(
 http:&E,store:&NativeStateStore,gen:u64,
 okx:&OkxCredentials,bn:&BinanceCredentials,
 now:u64,domain:OkxDomain,recv:u64,
)->Result<BoundedMoneyWindowReport,MoneyWindowError>{
 let captured=collect_double_money_rows(http,store,gen,okx,bn,now,domain,recv)?;
 let first=&captured.rows;
 let start=captured.start_ms;
 let mut funding=0i128;let mut transfer=0i128;
 let mut funding_count=0;let mut transfer_count=0;
 for row in first.binance.values(){
    match row.income_type.as_str(){
       "FUNDING_FEE"=>{
           funding_count+=1;
           funding=funding.checked_add(row.signed_amount_e12)
              .ok_or(deny("funding aggregation overflow"))?;
       },
       "TRANSFER"|"INTERNAL_TRANSFER"=>{
           transfer_count+=1;
           transfer=transfer.checked_add(row.signed_amount_e12)
              .ok_or(deny("transfer aggregation overflow"))?;
       },
       _=>{},
    }
 }
 let sum=|map:&BTreeMap<String,ObservedOkxBill>|->Result<i128,MoneyWindowError>{
    map.values().try_fold(0i128,|acc,r|
      acc.checked_add(r.signed_change_e12).ok_or(deny("OKX bill sum overflow")))
 };
 let gets=2*(u16::from(first.binance_pages)
     +u16::from(first.okx_base_pages)+u16::from(first.okx_quote_pages));
 Ok(BoundedMoneyWindowReport{
    source_generation:gen,window_start_ms:start,window_end_ms:now,
    verified_identical_scans:2,signed_fake_gets:gets,
    binance_income_rows:first.binance.len(),
    okx_base_bill_rows:first.okx_base.len(),
    okx_quote_bill_rows:first.okx_usdt.len(),
    binance_funding_rows:funding_count,
    observed_binance_funding_signed_usdt_e12:funding,
    observed_binance_transfer_rows:transfer_count,
    observed_binance_transfer_signed_usdt_e12:transfer,
    observed_okx_base_signed_change_e12:sum(&first.okx_base)?,
    observed_okx_usdt_signed_change_e12:sum(&first.okx_usdt)?,
    binance_trade_commissions_and_realized_pnl_matched_to_four_legs:false,
    binance_transfers_proven_external_not_internal:false,
    okx_bill_types_matched_to_all_four_trade_fills:false,
    all_future_funding_between_open_and_close_proven:false,
    complete_cash_deposits_withdrawals_proven:false,
    full_lifetime_profit_proven:false,
    no_late_income_or_new_order_proven:false,
    may_send_cancel_retry_or_verify_closed:false,
 })
}
