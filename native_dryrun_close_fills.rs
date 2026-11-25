//! TRADE2D2C1 — exact original close fill/fee bounded double-scan for
//! **SIMULATED** lost close replies. Never verified closed, even if receipts
//! match. Account flatness, funding and transfers require separate proof.
use std::collections::BTreeMap;
use serde_json::Value;
use crate::{
 authoritative_state::{NativeStateStore,NativeStateError},
 fill_economics::{FillEvent,signed_decimal_to_atoms},
 native_dryrun_close_status::{
     inspect_mock_unknown_close,MockCloseStatusError,
     MockCloseStatusReview,ObservedCloseState,
 },
 private_https::{HttpExecutor,HttpResponse,HttpsRequest,TransportError},
 private_rest::{
     BinanceCredentials,OkxCredentials,OkxDomain,RestBuildError,
     HttpMethod,SignedRequest,
     build_okx_fills_history_page_request,build_binance_user_trades_page_request,
     parse_okx_fill_events_http,parse_binance_fill_events_http,
 },
};
const OKX_LIMIT:u16=100;
const BINANCE_LIMIT:u16=1000;
fn err(msg:&'static str)->MockCloseFillsError{MockCloseFillsError::Invalid(msg)}
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum MockCloseFillsError{
    Status(MockCloseStatusError),State(NativeStateError),
    Build(RestBuildError),Transport(TransportError),Invalid(&'static str),
}
#[derive(Clone,Debug,PartialEq,Eq)]
struct CloseTradeScan{
    spot:BTreeMap<String,FillEvent>,
    futures:BTreeMap<String,FillEvent>,
    spot_fees_e12:i128,
    futures_fees_e12:i128,
    futures_realized_pnl_reported_e12:i128,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct MockCloseTradeReceipts {
    pub status_before:MockCloseStatusReview,
    pub status_after:MockCloseStatusReview,
    pub original_spot_trade_ids:Vec<String>,
    pub original_futures_trade_ids:Vec<String>,
    pub spot_fee_usdt_e12:i128,
    pub futures_fee_usdt_e12:i128,
    pub futures_reported_close_realized_pnl_usdt_e12:i128,
    pub total_readonly_gets:u8,
    pub complete_bounded_order_fill_scan_twice:bool,
    pub source_is_simulated_not_actual_order_intent:bool,
    pub current_futures_position_flat_proven:bool,
    pub spot_base_balance_flat_proven:bool,
    pub all_outstanding_and_trigger_orders_exhausted:bool,
    pub account_wide_funding_and_transfers_complete:bool,
    pub lifetime_net_pnl_proven:bool,
    pub no_fills_after_last_get_proven:bool,
    pub may_send_or_retry_order:bool,
    pub may_verify_closed:bool,
}
fn text(v:&Value,k:&str)->Result<String,MockCloseFillsError>{
    let f=v.get(k).ok_or(err("close trade ID/field absent"))?;
    if let Some(s)=f.as_str(){return Ok(s.into())}
    f.as_u64().map(|x|x.to_string())
        .ok_or(err("invalid close ID or numeric field"))
}
fn get<E:HttpExecutor>(http:&E,r:&SignedRequest)->Result<Value,MockCloseFillsError>{
    if r.method!=HttpMethod::Get||r.is_mutation||r.body.is_some(){
        return Err(err("close trades GET-only"))
    }
    let HttpResponse{status,body,..}=http.execute_request(&HttpsRequest::from_signed(r))
        .map_err(MockCloseFillsError::Transport)?;
    if status!=200{return Err(err("close trade GET HTTP error"))}
    serde_json::from_str(&body).map_err(|_|err("close trade response invalid JSON"))
}
fn count_sum(events:&BTreeMap<String,FillEvent>)
   ->Result<(u128,i128),MockCloseFillsError>{
    let mut q=0u128;let mut fee=0i128;
    for e in events.values(){
        q=q.checked_add(e.qty_atoms).ok_or(err("close quantity overflow"))?;
        fee=fee.checked_add(e.fee_cost_quote_atoms
            .ok_or(err("missing exact close fee"))?)
            .ok_or(err("close fee sum overflow"))?;
    }
    Ok((q,fee))
}
fn spot_scan<E:HttpExecutor>(
    http:&E,creds:&OkxCredentials,domain:OkxDomain,now:u64,
    symbol:&str,oid:&str,base:&str,target:u128,
)->Result<(BTreeMap<String,FillEvent>,i128),MockCloseFillsError>{
    let request=build_okx_fills_history_page_request(creds,now,domain,symbol,oid,None,OKX_LIMIT)
        .map_err(MockCloseFillsError::Build)?;
    let v=get(http,&request)?;
    if v.get("code").and_then(Value::as_str)!=Some("0"){
        return Err(err("OKX close trade business error"))
    }
    let rows=v.get("data").and_then(Value::as_array)
        .ok_or(err("OKX close trade history missing"))?;
    if rows.is_empty()||rows.len()>=OKX_LIMIT as usize{
        return Err(err("OKX order fill history absent or full"))
    }
    let raw=serde_json::to_string(&v).map_err(|_|err("OKX close JSON encode"))?;
    let fills=parse_okx_fill_events_http(200,&raw,oid,base,"USDT")
        .map_err(|_|err("OKX close trade invalid"))?;
    if fills.len()!=rows.len(){return Err(err("foreign OKX close trade"))}
    let mut out=BTreeMap::new();let mut last=None::<u128>;
    let mut after=None::<String>;
    for (r,f) in rows.iter().zip(fills){
        let bill=text(r,"billId")?;
        let value=bill.parse::<u128>().ok().filter(|v|*v>0)
            .ok_or(err("invalid OKX descending bill cursor"))?;
        if last.is_some_and(|prev|value>=prev)
            ||text(r,"ordId")?!=oid
            ||text(r,"feeCcy")?!="USDT"
            ||f.fee_cost_quote_atoms.map_or(true,|v|v<0)
            ||f.trade_time_ms.map_or(true,|t|t==0||t>now.saturating_add(5_000))
            ||out.insert(f.fill_id.clone(),f).is_some(){
            return Err(err("OKX duplicate/fee/timestamp/identity unexpected"))
        }
        last=Some(value);after=Some(bill);
    }
    // Explicit empty tail is mandatory; a shorter-than-limit page is
    // not by itself proof of pagination exhaustion.
    let tail=build_okx_fills_history_page_request(
        creds,now,domain,symbol,oid,after.as_deref(),OKX_LIMIT)
        .map_err(MockCloseFillsError::Build)?;
    let v=get(http,&tail)?;
    if v.get("code").and_then(Value::as_str)!=Some("0")
        ||v.get("data").and_then(Value::as_array).map_or(true,|rows|!rows.is_empty()){
        return Err(err("OKX close trade pagination not exhausted"))
    }
    let (qty,fees)=count_sum(&out)?;
    if qty!=target{return Err(err("OKX close trade sum != original filled status"))}
    Ok((out,fees))
}

fn futures_scan<E:HttpExecutor>(
    http:&E,creds:&BinanceCredentials,now:u64,recv:u64,
    symbol:&str,oid:&str,target:u128,
)->Result<(BTreeMap<String,FillEvent>,i128,i128),MockCloseFillsError>{
    let r=build_binance_user_trades_page_request(
        creds,now,recv,symbol,oid,0,BINANCE_LIMIT)
        .map_err(MockCloseFillsError::Build)?;
    let v=get(http,&r)?;
    let rows=v.as_array().ok_or(err("Binance close trades not array"))?;
    if rows.is_empty()||rows.len()>=BINANCE_LIMIT as usize{
        return Err(err("Binance close trade page empty or potentially truncated"))
    }
    let raw=serde_json::to_string(&v).map_err(|_|err("Binance fill encode"))?;
    let events=parse_binance_fill_events_http(200,&raw,oid,"USDT")
        .map_err(|_|err("Binance close trade event malformed"))?;
    if events.len()!=rows.len(){return Err(err("foreign Binance close trade"))}
    let mut out=BTreeMap::new();let mut last=0u64;
    let mut pnl=0i128;
    for (row,ev) in rows.iter().zip(events){
        let id=text(row,"id")?.parse::<u64>().ok().filter(|x|*x>last)
            .ok_or(err("Binance close trade ID missing or unordered"))?;
        if text(row,"orderId")?!=oid||text(row,"symbol")?!=symbol
            ||text(row,"side")?!="BUY"||text(row,"positionSide")?!="BOTH"
            ||text(row,"marginAsset")?!="USDT"
            ||row.get("buyer").and_then(Value::as_bool)!=Some(true)
            ||text(row,"commissionAsset")?!="USDT"
            ||ev.fee_cost_quote_atoms.map_or(true,|v|v<0)
            ||ev.trade_time_ms.map_or(true,|t|t==0||t>now.saturating_add(5_000))
            ||out.insert(ev.fill_id.clone(),ev).is_some(){
            return Err(err("Binance close direction/fee/fill identity altered"))
        }
        pnl=pnl.checked_add(signed_decimal_to_atoms(&text(row,"realizedPnl")?)
            .map_err(|_|err("Binance per-trade realized PnL malformed"))?)
            .ok_or(err("Binance realized PnL sum overflow"))?;
        last=id;
    }
    let next=last.checked_add(1).ok_or(err("Binance close fill cursor overflow"))?;
    let r=build_binance_user_trades_page_request(
        creds,now,recv,symbol,oid,next,BINANCE_LIMIT)
        .map_err(MockCloseFillsError::Build)?;
    let tail=get(http,&r)?;
    if tail.as_array().map_or(true,|rows|!rows.is_empty()){
        return Err(err("Binance close fill cursor not exhausted"))
    }
    let (qty,fees)=count_sum(&out)?;
    if qty!=target{return Err(err("Binance close sum != original filled status"))}
    Ok((out,fees,pnl))
}
fn scan<E:HttpExecutor>(
    http:&E,before:&MockCloseStatusReview,
    okx:&OkxCredentials,bn:&BinanceCredentials,
    now:u64,domain:OkxDomain,recv:u64,
    spot_symbol:&str,future_symbol:&str,base:&str,
)->Result<CloseTradeScan,MockCloseFillsError>{
    let spot=&before.okx;let fut=&before.binance;
    if spot.state!=ObservedCloseState::TerminalFilled
        ||fut.state!=ObservedCloseState::TerminalFilled{
        return Err(err("partial or cancelled close not eligible for full exact-close receipt"))
    }
    let (a,spot_fee)=spot_scan(
        http,okx,domain,now,spot_symbol,&spot.exchange_order_id,base,spot.target_e12)?;
    let (b,future_fee,pnl)=futures_scan(
        http,bn,now,recv,future_symbol,&fut.exchange_order_id,fut.target_e12)?;
    Ok(CloseTradeScan{
        spot:a,futures:b,spot_fees_e12:spot_fee,
        futures_fees_e12:future_fee,futures_realized_pnl_reported_e12:pnl,
    })
}
/// Order STATUS double GET -> both CLOSE trade histories with separately
/// empty pagination tails -> repeat exact CLOSE trade histories -> order
/// STATUS double GET. Exactly 16 signed GET in one complete mock fixture.
/// Must NEVER set any real close/account completeness bits.
#[allow(clippy::too_many_arguments)]
pub fn inspect_mock_full_close_trades<E:HttpExecutor>(
    http:&E,store:&NativeStateStore,expected_generation:u64,
    okx:&OkxCredentials,bn:&BinanceCredentials,
    now:u64,domain:OkxDomain,recv:u64,
)->Result<MockCloseTradeReceipts,MockCloseFillsError>{
    let before=inspect_mock_unknown_close(
        http,store,expected_generation,okx,bn,now,domain,recv)
        .map_err(MockCloseFillsError::Status)?;
    if before.okx.state!=ObservedCloseState::TerminalFilled
        ||before.binance.state!=ObservedCloseState::TerminalFilled{
        return Err(err("both close orders not terminal filled; must review partial/cancel"))
    }
    let s=store.load().map_err(MockCloseFillsError::State)?
        .ok_or(MockCloseFillsError::State(NativeStateError::Generation))?;
    if s.generation!=expected_generation||s.active.is_some(){
        return Err(err("Native owner changed during close fill audit"))
    }
    let e=s.entry.as_ref().ok_or(err("original entry not found"))?;
    let first=scan(
        http,&before,okx,bn,now,domain,recv,
        &e.plan.spot_symbol,&e.plan.futures_symbol,&e.plan.base)?;
    let second=scan(
        http,&before,okx,bn,now,domain,recv,
        &e.plan.spot_symbol,&e.plan.futures_symbol,&e.plan.base)?;
    if first!=second{return Err(err("close fill/fee history drift between independent scans"))}
    let after=inspect_mock_unknown_close(
        http,store,expected_generation,okx,bn,now,domain,recv)
        .map_err(MockCloseFillsError::Status)?;
    if before!=after{
        return Err(err("close order status changed across full fill audit"))
    }
    if store.load().map_err(MockCloseFillsError::State)?.as_ref()!=Some(&s){
        return Err(err("Native owner/source changed after close scans"))
    }
    Ok(MockCloseTradeReceipts{
        status_before:before,status_after:after,
        original_spot_trade_ids:first.spot.keys().cloned().collect(),
        original_futures_trade_ids:first.futures.keys().cloned().collect(),
        spot_fee_usdt_e12:first.spot_fees_e12,
        futures_fee_usdt_e12:first.futures_fees_e12,
        futures_reported_close_realized_pnl_usdt_e12:
            first.futures_realized_pnl_reported_e12,
        total_readonly_gets:16,complete_bounded_order_fill_scan_twice:true,
        source_is_simulated_not_actual_order_intent:true,
        current_futures_position_flat_proven:false,
        spot_base_balance_flat_proven:false,
        all_outstanding_and_trigger_orders_exhausted:false,
        account_wide_funding_and_transfers_complete:false,
        lifetime_net_pnl_proven:false,no_fills_after_last_get_proven:false,
        may_send_or_retry_order:false,may_verify_closed:false,
    })
}
