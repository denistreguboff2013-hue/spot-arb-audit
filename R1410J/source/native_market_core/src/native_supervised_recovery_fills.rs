//! TRADE2C: independent fresh *per-trade* immutable entry receipt replay.
//! Read-only signed GETs only. Account/order brackets from TRADE2B guard
//! BOTH sides of two exact trade-history scans. No permission to execute.
use std::collections::{BTreeMap,BTreeSet};
use crate::{
 authoritative_state::{NativeStateStore,NativeStateError},
 fill_economics::{FillEvent,decimal_to_atoms},
 market_metadata::NativeMarketMetadata,
 native_supervised_recovery_live::{
     audit_fresh_supervised_recovery_once,FreshRecoveryError,FreshSupervisedRecoveryReview,
 },
 private_https::{HttpExecutor,HttpResponse,HttpsRequest,TransportError},
 private_rest::{
     BinanceCredentials,OkxCredentials,OkxDomain,RestBuildError,
     HttpMethod,SignedRequest,
     build_okx_fills_history_page_request,build_binance_user_trades_page_request,
     parse_okx_fill_events_http,parse_binance_fill_events_http,
 },
};
use serde_json::Value;

const OKX_PAGE_LIMIT:u16=100;
const BINANCE_PAGE_LIMIT:u16=1000;
const OKX_MAX_PAGES:usize=16;

#[derive(Clone,Debug,PartialEq,Eq)]
pub enum Trade2CFillError{
    Previous(FreshRecoveryError),
    State(NativeStateError),
    Build(RestBuildError),
    Transport(TransportError),
    Invalid(&'static str),
}
fn deny(s:&'static str)->Trade2CFillError{Trade2CFillError::Invalid(s)}
#[derive(Clone,Debug,PartialEq,Eq)]
struct MatchedTrades {
    spot:BTreeMap<String,FillEvent>,
    future:BTreeMap<String,FillEvent>,
    spot_pages:usize,
    binance_pages:usize,
    spot_fee_e12:i128,
    futures_fee_e12:i128,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct SupervisedRecoveryFillReview{
    pub before:FreshSupervisedRecoveryReview,
    pub after:FreshSupervisedRecoveryReview,
    pub full_original_trade_scans:u8,
    pub signed_get_history_reads:usize,
    pub spot_trade_ids:Vec<String>,
    pub futures_trade_ids:Vec<String>,
    pub spot_fee_quote_e12:i128,
    pub futures_fee_quote_e12:i128,
    pub every_original_fill_price_qty_fee_time_matches:bool,
    pub spot_history_pagination_exhausted:bool,
    pub futures_bounded_history_page_nonfull:bool,
    pub never_reuse_for_order_authorization:bool,
    pub global_trade_history_complete:bool,
    pub income_funding_transfer_history_complete:bool,
    pub current_account_atomicity_proven:bool,
    pub no_delayed_fills_after_last_read_proven:bool,
    pub may_send_order:bool,
    pub may_auto_unwind:bool,
    pub may_verify_closed:bool,
}
fn text(v:&Value,key:&str)->Result<String,Trade2CFillError>{
    let f=v.get(key).ok_or(deny("fill field absent"))?;
    if let Some(x)=f.as_str(){return Ok(x.to_owned())}
    if let Some(x)=f.as_u64(){return Ok(x.to_string())}
    Err(deny("fill field invalid"))
}
fn read<E:HttpExecutor>(http:&E,req:&SignedRequest)->Result<String,Trade2CFillError>{
    if req.method!=HttpMethod::Get||req.is_mutation||req.body.is_some(){
        return Err(deny("non-readonly fill audit request"))
    }
    let HttpResponse{status,body,..}=http.execute_request(&HttpsRequest::from_signed(req))
        .map_err(Trade2CFillError::Transport)?;
    if status!=200{return Err(deny("private fill history not HTTP 200"))}
    Ok(body)
}
fn spot_scoped<E:HttpExecutor>(
    http:&E, creds:&OkxCredentials,domain:OkxDomain,
    now:u64,symbol:&str,order_id:&str,base:&str,
)->Result<(BTreeMap<String,FillEvent>,usize),Trade2CFillError>{
    let mut out=BTreeMap::new();
    let mut seen_bills=BTreeSet::new();
    let mut prev_bill:Option<u128>=None;
    let mut after:Option<String>=None;
    for p in 0..OKX_MAX_PAGES{
        let request=build_okx_fills_history_page_request(
            creds,now,domain,symbol,order_id,after.as_deref(),OKX_PAGE_LIMIT
        ).map_err(Trade2CFillError::Build)?;
        let raw=read(http,&request)?;
        let value:Value=serde_json::from_str(&raw)
            .map_err(|_|deny("OKX fill history invalid JSON"))?;
        if value.get("code").and_then(Value::as_str)!=Some("0"){
            return Err(deny("OKX business error"))
        }
        let rows=value.get("data").and_then(Value::as_array)
            .ok_or(deny("OKX fills absent"))?;
        if rows.len()>OKX_PAGE_LIMIT as usize{
            return Err(deny("OKX page exceeds limit"))
        }
        if rows.is_empty(){
            if out.is_empty(){return Err(deny("OKX original no fills"))}
            return Ok((out,p+1))
        }
        let fills=parse_okx_fill_events_http(200,&raw,order_id,base,"USDT")
            .map_err(|_|deny("OKX fills malformed"))?;
        if fills.len()!=rows.len(){return Err(deny("foreign OKX order in history"))}
        for (r,f) in rows.iter().zip(fills){
            if text(r,"ordId")?!=order_id
                ||f.fee_cost_quote_atoms.is_none()
                ||f.trade_time_ms.is_none(){
                return Err(deny("OKX order ID/fee/trade time missing"))
            }
            let bill=text(r,"billId")?;
            let bill_id=bill.parse::<u128>()
                .ok().filter(|x|*x>0).ok_or(deny("OKX invalid bill cursor"))?;
            if prev_bill.map_or(false,|v|bill_id>=v)
                ||!seen_bills.insert(bill.clone())
                ||out.insert(f.fill_id.clone(),f).is_some(){
                return Err(deny("OKX duplicate or unordered fill/bill"))
            }
            prev_bill=Some(bill_id);
            after=Some(bill);
        }
    }
    Err(deny("OKX fill pagination not proven exhausted"))
}
fn futures_scoped<E:HttpExecutor>(
    http:&E,creds:&BinanceCredentials,now:u64,recv:u64,
    symbol:&str,order_id:&str,
)->Result<(BTreeMap<String,FillEvent>,usize),Trade2CFillError>{
    // Explicit fromId/limit and exact original order ID. If a response
    // consumes the entire 1000-record page, completeness is UNKNOWN.
    let request=build_binance_user_trades_page_request(
        creds,now,recv,symbol,order_id,0,BINANCE_PAGE_LIMIT
    ).map_err(Trade2CFillError::Build)?;
    let raw=read(http,&request)?;
    let rows:Value=serde_json::from_str(&raw)
        .map_err(|_|deny("Binance fills invalid JSON"))?;
    let rows=rows.as_array().ok_or(deny("Binance fills not array"))?;
    if rows.is_empty()||rows.len()>=BINANCE_PAGE_LIMIT as usize{
        return Err(deny("Binance fills missing or possibly truncated"))
    }
    let fills=parse_binance_fill_events_http(200,&raw,order_id,"USDT")
        .map_err(|_|deny("Binance trade events malformed"))?;
    if fills.len()!=rows.len(){return Err(deny("foreign Binance order in fills"))}
    let mut out=BTreeMap::new();
    for (row,fill) in rows.iter().zip(fills){
        if text(row,"orderId")?!=order_id||text(row,"symbol")?!=symbol
            ||text(row,"side")?!="SELL"||text(row,"positionSide")?!="BOTH"
            ||text(row,"marginAsset")?!="USDT"
            ||row.get("buyer").and_then(Value::as_bool)!=Some(false)
            ||decimal_to_atoms(&text(row,"realizedPnl")?).ok()!=Some(0)
            ||fill.fee_currency.as_deref()!=Some("USDT")
            ||fill.fee_cost_quote_atoms.is_none()
            ||fill.trade_time_ms.is_none()
            ||out.insert(fill.fill_id.clone(),fill).is_some(){
            return Err(deny("Binance identity, trade fees or direction changed"))
        }
    }
    Ok((out,1))
}
fn scan_once<E:HttpExecutor>(
    http:&E,store:&NativeStateStore,expected_generation:u64,
    okx:&OkxCredentials,bn:&BinanceCredentials,
    now:u64,domain:OkxDomain,recv:u64,
)->Result<MatchedTrades,Trade2CFillError>{
    let before=store.load().map_err(Trade2CFillError::State)?
        .ok_or(Trade2CFillError::State(NativeStateError::Generation))?;
    if before.generation!=expected_generation{
        return Err(Trade2CFillError::State(NativeStateError::Generation))
    }
    let e=before.entry.as_ref().ok_or(deny("original native entry absent"))?;
    let spot=e.spot_prehedge_evidence.as_ref().ok_or(deny("original C2 spot proof missing"))?;
    let cp=e.recovery_checkpoint.as_ref().ok_or(deny("original futures checkpoint missing"))?;
    if !spot.exact_ledger.fee_evidence_complete||!cp.ledger.fee_evidence_complete
        ||spot.exact_ledger.fills.is_empty()||cp.ledger.fills.is_empty()
        ||spot.spot_order_id.is_empty()||cp.order_id.is_empty(){
        return Err(deny("original exact fill source incomplete"))
    }
    let (spot_now,spot_pages)=spot_scoped(
        http,okx,domain,now,&e.plan.spot_symbol,&spot.spot_order_id,&e.plan.base)?;
    let (future_now,binance_pages)=futures_scoped(
        http,bn,now,recv,&e.plan.futures_symbol,&cp.order_id)?;
    if spot_now!=spot.exact_ledger.fills||future_now!=cp.ledger.fills{
        return Err(deny("fresh original trade ID/price/quantity/fee/time differs"))
    }
    let current=store.load().map_err(Trade2CFillError::State)?
        .ok_or(Trade2CFillError::State(NativeStateError::Generation))?;
    if current!=before{return Err(Trade2CFillError::State(NativeStateError::Generation))}
    Ok(MatchedTrades{
        spot:spot_now,future:future_now,spot_pages,binance_pages,
        spot_fee_e12:spot.exact_ledger.fee_cost_quote_atoms,
        futures_fee_e12:cp.ledger.fee_cost_quote_atoms,
    })
}
/// B2 account bracket -> exact history -> exact history -> B2 account
/// bracket. Four B2 account snapshots plus two fresh trade-history scans.
/// This is a *read-only diagnostic*, not an instruction or trading token.
#[allow(clippy::too_many_arguments)]
pub fn audit_fresh_supervised_recovery_fills_once<E:HttpExecutor>(
    http:&E,store:&NativeStateStore,expected_generation:u64,
    market:&NativeMarketMetadata,spot_bid_e12:u128,
    okx:&OkxCredentials,bn:&BinanceCredentials,
    now_ms:u64,domain:OkxDomain,recv_window_ms:u64,
)->Result<SupervisedRecoveryFillReview,Trade2CFillError>{
    let first=audit_fresh_supervised_recovery_once(
        http,store,expected_generation,market,spot_bid_e12,
        okx,bn,now_ms,domain,recv_window_ms,
    ).map_err(Trade2CFillError::Previous)?;
    let a=scan_once(http,store,expected_generation,okx,bn,now_ms,domain,recv_window_ms)?;
    let b=scan_once(http,store,expected_generation,okx,bn,now_ms,domain,recv_window_ms)?;
    if a!=b{return Err(deny("trade history changed between repeated scans"))}
    let second=audit_fresh_supervised_recovery_once(
        http,store,expected_generation,market,spot_bid_e12,
        okx,bn,now_ms,domain,recv_window_ms,
    ).map_err(Trade2CFillError::Previous)?;
    if first!=second{return Err(deny("account changed across per-fill scans"))}
    Ok(SupervisedRecoveryFillReview{
        before:first,after:second,full_original_trade_scans:2,
        signed_get_history_reads:a.spot_pages+a.binance_pages
            +b.spot_pages+b.binance_pages,
        spot_trade_ids:a.spot.keys().cloned().collect(),
        futures_trade_ids:a.future.keys().cloned().collect(),
        spot_fee_quote_e12:a.spot_fee_e12,
        futures_fee_quote_e12:a.futures_fee_e12,
        every_original_fill_price_qty_fee_time_matches:true,
        spot_history_pagination_exhausted:true,
        futures_bounded_history_page_nonfull:true,
        never_reuse_for_order_authorization:true,
        global_trade_history_complete:false,
        income_funding_transfer_history_complete:false,
        current_account_atomicity_proven:false,
        no_delayed_fills_after_last_read_proven:false,
        may_send_order:false,may_auto_unwind:false,may_verify_closed:false,
    })
}
