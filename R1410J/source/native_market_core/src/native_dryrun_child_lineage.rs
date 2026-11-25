//! TRADE2D2E2: bounded one-parent -> one ordinary child -> per-fill audit.
//! This is standalone MANUAL-ONLY diagnostic, NOT a new VerifiedClosed
//! route. It deliberately does not satisfy E1's any-history DENY.
//! Branching algo, SAPI TWAP/VP, missing reverse link and retention -> DENY.
use std::collections::BTreeMap;
use crate::{
 authoritative_state::{NativeStateStore,NativeStateError},
 fill_economics::{FillEvent,decimal_to_atoms},
 private_https::{HttpExecutor,HttpsRequest,HttpResponse,TransportError},
 private_rest::{
   OkxCredentials,BinanceCredentials,OkxDomain,RestBuildError,
   HttpMethod,SignedRequest,
   build_okx_algo_by_id_request,build_binance_algo_by_id_request,
   build_okx_child_by_order_id_request,build_binance_child_by_order_id_request,
   build_okx_fills_history_page_request,build_binance_user_trades_page_request,
   parse_okx_fill_events_http,parse_binance_fill_events_http,
 },
};
use serde_json::Value;
const OKX_LIMIT:u16=100;
const BINANCE_LIMIT:u16=1000;
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum ChildLineageError{
 State(NativeStateError),Build(RestBuildError),Transport(TransportError),
 Invalid(&'static str),
}
pub(crate) fn bad(m:&'static str)->ChildLineageError{ChildLineageError::Invalid(m)}
#[derive(Clone,Debug,PartialEq,Eq)]
struct LineageLeg{
 parent_id:String,child_order_id:String,
 trade_ids:Vec<String>,qty_e12:u128,fee_usdt_e12:i128,
}
#[derive(Clone,Debug,PartialEq,Eq)]
struct LineagePair{okx:LineageLeg,binance:LineageLeg}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct BoundedChildLineageReview{
 pub native_generation:u64,
 pub synthetic_signed_get_count:u8,
 pub independent_complete_reads:u8,
 pub okx_parent_id:String,pub okx_child_order_id:String,
 pub okx_trade_ids:Vec<String>,pub okx_fee_usdt_e12:i128,
 pub binance_parent_id:String,pub binance_child_order_id:String,
 pub binance_trade_ids:Vec<String>,pub binance_fee_usdt_e12:i128,
 pub single_child_per_parent_seen_in_mock:bool,
 pub separate_ordinary_order_and_per_fill_identity_seen_in_mock:bool,
 pub global_historical_parent_coverage_complete:bool,
 pub untraced_sapi_twap_vp_child_orders_proven_absent:bool,
 pub all_parent_children_and_subalgos_proven_resolved:bool,
 pub real_order_submission_proven:bool,
 pub independent_final_account_flatness_proven:bool,
 pub whole_account_funding_transfers_and_lifetime_pnl_proven:bool,
 pub may_retry_cancel_send_or_mark_verified_closed:bool,
}
pub(crate) fn str_field<'a>(v:&'a Value,k:&str)->Result<&'a str,ChildLineageError>{
 v.get(k).and_then(Value::as_str).ok_or(bad("missing required parent/child field"))
}
pub(crate) fn id(v:&Value,k:&str)->Result<String,ChildLineageError>{
 let x=v.get(k).ok_or(bad("missing parent/child ID"))?;
 if let Some(s)=x.as_str(){return Ok(s.to_string())}
 x.as_u64().map(|y|y.to_string()).ok_or(bad("invalid parent/child ID"))
}
pub(crate) fn valid_id(v:&str)->bool{!v.is_empty()&&v.len()<=40&&v.bytes().all(|x|x.is_ascii_digit())
    &&v.bytes().any(|x|x!=b'0')}
/// TRADE2D2E3B: OKX specifically restricts trigger->chase / chase
/// algorithm orders to FUTURES/SWAP. They cannot be treated as supported
/// SPOT close descendants. Optional missing fields are NOT proof of
/// complete account history; this just denies documented invalid types.
pub(crate) fn ensure_supported_spot_algo_shape(
    row:&Value,
)->Result<(),ChildLineageError>{
    if row.get("instType").is_some()
       &&row.get("instType").and_then(Value::as_str)!=Some("SPOT"){
        return Err(bad("OKX nested parent claims non-SPOT instrument"))
    }
    for field in ["ordType","advanceOrdType"]{
        let Some(value)=row.get(field) else {continue};
        let kind=value.as_str().ok_or(bad("OKX algo type malformed"))?;
        if kind=="chase"{
            return Err(bad("OKX trigger-chase supports FUTURES/SWAP, not SPOT"))
        }
    }
    if let Some(unit)=row.get("tgtCcy").and_then(Value::as_str){
       if unit!="base_ccy" && unit!=""{
         return Err(bad("OKX spot SELL size not proven in base currency"))
       }
    }
    Ok(())
}
pub(crate) fn fixed(v:&Value,k:&str)->Result<u128,ChildLineageError>{
 decimal_to_atoms(str_field(v,k)?).map_err(|_|bad("quantity not exact decimal"))
}
pub(crate) fn get<E:HttpExecutor>(http:&E,req:&SignedRequest)->Result<Value,ChildLineageError>{
 if req.method!=HttpMethod::Get||req.is_mutation||req.body.is_some(){
   return Err(bad("parent/child history must only use GET"))
 }
 let HttpResponse{status,body,..}=http.execute_request(&HttpsRequest::from_signed(req))
    .map_err(ChildLineageError::Transport)?;
 if status!=200{return Err(bad("private GET error is not absence evidence"))}
 serde_json::from_str(&body).map_err(|_|bad("invalid parent/child JSON"))
}
pub(crate) fn one_object<'a>(v:&'a Value)->Result<&'a Value,ChildLineageError>{
 if v.get("data").is_none(){
   if v.is_object(){return Ok(v)}
   return Err(bad("parent/child object missing"))
 }
 let r=v.get("data").and_then(Value::as_array)
    .ok_or(bad("parent/child object missing"))?;
 if v.get("code").and_then(Value::as_str)!=Some("0")||r.len()!=1{
    return Err(bad("parent/child exchange error or ambiguous result"))
 }
 Ok(&r[0])
}
fn fees_and_qty(events:&BTreeMap<String,FillEvent>,triggered_ms:u64,now:u64)
 ->Result<(u128,i128),ChildLineageError>{
 if events.is_empty(){return Err(bad("no child trade executions"))}
 let mut qty=0u128;let mut fees=0i128;
 for e in events.values(){
  let t=e.trade_time_ms.ok_or(bad("child trade timestamp absent"))?;
  let commission=e.fee_cost_quote_atoms.ok_or(bad("child commission missing"))?;
  if e.price_atoms==0||e.qty_atoms==0||e.fee_currency.as_deref()!=Some("USDT")
     ||commission<0||t<triggered_ms||t>now.saturating_add(5000){
    return Err(bad("wrong child time/price/qty/fee currency"))
  }
  qty=qty.checked_add(e.qty_atoms).ok_or(bad("child quantity overflow"))?;
  fees=fees.checked_add(commission).ok_or(bad("child fee overflow"))?;
 }
 Ok((qty,fees))
}
pub(crate) fn exact_okx_fills<E:HttpExecutor>(
 http:&E,okx:&OkxCredentials,domain:OkxDomain,now:u64,
 symbol:&str,order_id:&str,base:&str,trigger:u64,target:u128,
)->Result<(Vec<String>,i128),ChildLineageError>{
 let req=build_okx_fills_history_page_request(
    okx,now,domain,symbol,order_id,None,OKX_LIMIT)
    .map_err(ChildLineageError::Build)?;
 let v=get(http,&req)?;
 if v.get("code").and_then(Value::as_str)!=Some("0"){
    return Err(bad("OKX fills business error"))
 }
 let rows=v.get("data").and_then(Value::as_array).ok_or(bad("OKX fill list absent"))?;
 if rows.is_empty()||rows.len()>=OKX_LIMIT as usize{
    return Err(bad("OKX fill list missing/potentially truncated"))
 }
 let raw=serde_json::to_string(&v).map_err(|_|bad("OKX fill encode"))?;
 let fills=parse_okx_fill_events_http(200,&raw,order_id,base,"USDT")
    .map_err(|_|bad("OKX fill event malformed"))?;
 if fills.len()!=rows.len(){return Err(bad("foreign OKX child fill"))}
 let mut last=None::<u128>;let mut map=BTreeMap::new();
 for (r,f) in rows.iter().zip(fills){
   let bill=id(r,"billId")?;let n=bill.parse::<u128>().ok().filter(|v|*v>0)
       .ok_or(bad("invalid OKX bill cursor"))?;
   if last.map_or(false,|x|n>=x)||id(r,"ordId")?!=order_id
       ||map.insert(f.fill_id.clone(),f).is_some(){
       return Err(bad("duplicate/foreign/out-of-order OKX child fill"))
   }
   last=Some(n);
 }
 let last_bill=last.ok_or(bad("OKX last bill missing"))?.to_string();
 let req=build_okx_fills_history_page_request(
    okx,now,domain,symbol,order_id,Some(&last_bill),OKX_LIMIT)
    .map_err(ChildLineageError::Build)?;
 let tail=get(http,&req)?;
 if tail.get("code").and_then(Value::as_str)!=Some("0")
    ||tail.get("data").and_then(Value::as_array).map_or(true,|x|!x.is_empty()){
    return Err(bad("OKX child fill cursor not exhausted"))
 }
 let (total,fee)=fees_and_qty(&map,trigger,now)?;
 if total!=target{return Err(bad("OKX child fill sum mismatch"))}
 Ok((map.keys().cloned().collect(),fee))
}

fn exact_binance_fills<E:HttpExecutor>(
 http:&E,bn:&BinanceCredentials,now:u64,recv:u64,
 symbol:&str,order_id:&str,trigger:u64,target:u128,
)->Result<(Vec<String>,i128),ChildLineageError>{
 let req=build_binance_user_trades_page_request(
    bn,now,recv,symbol,order_id,0,BINANCE_LIMIT)
    .map_err(ChildLineageError::Build)?;
 let v=get(http,&req)?;
 let rows=v.as_array().ok_or(bad("Binance trades missing array"))?;
 if rows.is_empty()||rows.len()>=BINANCE_LIMIT as usize{
    return Err(bad("Binance trade history incomplete"))
 }
 let raw=serde_json::to_string(&v).map_err(|_|bad("Binance encode"))?;
 let fills=parse_binance_fill_events_http(200,&raw,order_id,"USDT")
    .map_err(|_|bad("Binance child trade invalid"))?;
 if fills.len()!=rows.len(){return Err(bad("foreign Binance child trade"))}
 let mut map=BTreeMap::new();let mut last=0u64;
 for (row,f) in rows.iter().zip(fills){
    let n=id(row,"id")?.parse::<u64>().ok()
        .filter(|x|*x>last).ok_or(bad("nonmonotone Binance child trade IDs"))?;
    if id(row,"orderId")?!=order_id||str_field(row,"symbol")?!=symbol
        ||str_field(row,"side")?!="BUY"
        ||str_field(row,"positionSide")?!="BOTH"
        ||row.get("buyer").and_then(Value::as_bool)!=Some(true)
        ||str_field(row,"marginAsset")?!="USDT"
        ||str_field(row,"commissionAsset")?!="USDT"
        ||map.insert(f.fill_id.clone(),f).is_some(){
        return Err(bad("wrong Binance child trade identity/direction"))
    }
    last=n;
 }
 let req=build_binance_user_trades_page_request(
    bn,now,recv,symbol,order_id,last.checked_add(1)
         .ok_or(bad("Binance child cursor overflow"))?,BINANCE_LIMIT)
    .map_err(ChildLineageError::Build)?;
 let tail=get(http,&req)?;
 if tail.as_array().map_or(true,|a|!a.is_empty()){
    return Err(bad("Binance child trade cursor not exhausted"))
 }
 let (total,fee)=fees_and_qty(&map,trigger,now)?;
 if total!=target{return Err(bad("Binance child trade sum mismatch"))}
 Ok((map.keys().cloned().collect(),fee))
}
#[allow(clippy::too_many_arguments)]
fn inspect_one<E:HttpExecutor>(
 http:&E,spot:&OkxCredentials,fut:&BinanceCredentials,
 domain:OkxDomain,now:u64,recv:u64,
 spot_symbol:&str,fut_symbol:&str,base:&str,
 spot_expected:u128,fut_expected:u128,
 okx_algo:&str,binance_algo:u64,
)->Result<LineagePair,ChildLineageError>{
 let req=build_okx_algo_by_id_request(spot,now,domain,okx_algo)
    .map_err(ChildLineageError::Build)?;
 let raw=get(http,&req)?;
 let p=one_object(&raw)?;
 ensure_supported_spot_algo_shape(p)?;
 if id(p,"algoId")?!=okx_algo||str_field(p,"instId")?!=spot_symbol
    ||str_field(p,"side")?!="sell"||str_field(p,"tdMode")?!="cash"
    ||str_field(p,"state")?!="effective"
    ||fixed(p,"sz")?!=spot_expected{
    return Err(bad("OKX parent wrong identity, side, size or state"))
 }
 let triggered=id(p,"triggerTime")?.parse::<u64>().ok()
    .filter(|v|*v>0&&*v<=now).ok_or(bad("OKX parent not triggered"))?;
 let ids=p.get("ordIdList").and_then(Value::as_array)
    .ok_or(bad("OKX parent no child list"))?;
 let nested=p.get("subAlgoIdList").and_then(Value::as_array)
    .ok_or(bad("OKX nested parent children not supplied"))?;
 if !nested.is_empty()||ids.len()!=1{
    return Err(bad("OKX split or nested algo requires full recursive review"))
 }
 let ok_child=ids[0].as_str().ok_or(bad("OKX child ID not string"))?;
 if !valid_id(ok_child){return Err(bad("OKX child ID invalid"))}
 let req=build_okx_child_by_order_id_request(
    spot,now,domain,spot_symbol,ok_child).map_err(ChildLineageError::Build)?;
 let row=get(http,&req)?;let order=one_object(&row)?;
 if id(order,"ordId")?!=ok_child
    ||str_field(order,"instId")?!=spot_symbol
    ||str_field(order,"side")?!="sell"
    ||str_field(order,"tdMode")?!="cash"
    ||str_field(order,"algoId")?!=okx_algo
    ||str_field(order,"state")?!="filled"
    ||fixed(order,"sz")?!=spot_expected
    ||fixed(order,"accFillSz")?!=spot_expected{
     return Err(bad("OKX child not proven to match triggered parent"))
 }
 let (spot_ids,spot_fee)=exact_okx_fills(
    http,spot,domain,now,spot_symbol,ok_child,base,triggered,spot_expected)?;

 let req=build_binance_algo_by_id_request(fut,now,recv,binance_algo)
     .map_err(ChildLineageError::Build)?;
 let raw=get(http,&req)?;let p=one_object(&raw)?;
 if id(p,"algoId")?!=binance_algo.to_string()
    ||str_field(p,"symbol")?!=fut_symbol
    ||str_field(p,"algoStatus")?!="TRIGGERED"
    ||str_field(p,"side")?!="BUY"
    ||str_field(p,"positionSide")?!="BOTH"
    ||p.get("reduceOnly").and_then(Value::as_bool)!=Some(true)
    ||fixed(p,"quantity")?!=fut_expected{
    return Err(bad("Binance algo parent wrong symbol/side/size/status/reduceOnly"))
 }
 let triggered=id(p,"triggerTime")?.parse::<u64>().ok()
    .filter(|x|*x>0&&*x<=now).ok_or(bad("Binance parent trigger time invalid"))?;
 let child=id(p,"actualOrderId")?;
 if !valid_id(&child){return Err(bad("Binance actualOrderId absent or invalid"))}
 let n=child.parse::<u64>().map_err(|_|bad("Binance child ID overflow"))?;
 let req=build_binance_child_by_order_id_request(fut,now,recv,fut_symbol,n)
    .map_err(ChildLineageError::Build)?;
 let order=get(http,&req)?;let order=one_object(&order)?;
 if id(order,"orderId")?!=child
    ||str_field(order,"symbol")?!=fut_symbol
    ||str_field(order,"side")?!="BUY"
    ||str_field(order,"positionSide")?!="BOTH"
    ||order.get("reduceOnly").and_then(Value::as_bool)!=Some(true)
    ||str_field(order,"status")?!="FILLED"
    ||fixed(order,"origQty")?!=fut_expected
    ||fixed(order,"executedQty")?!=fut_expected{
    return Err(bad("Binance child order amount/status/identity mismatch"))
 }
 let (fut_ids,fut_fee)=exact_binance_fills(
    http,fut,now,recv,fut_symbol,&child,triggered,fut_expected)?;
 Ok(LineagePair{
   okx:LineageLeg{parent_id:okx_algo.into(),child_order_id:ok_child.into(),
       trade_ids:spot_ids,qty_e12:spot_expected,fee_usdt_e12:spot_fee},
   binance:LineageLeg{parent_id:binance_algo.to_string(),child_order_id:child,
       trade_ids:fut_ids,qty_e12:fut_expected,fee_usdt_e12:fut_fee},
 })
}
/// 2 identical independent sets of 8 GET: OKX parent, child, fills, empty
/// tail; Binance parent, child, fills, empty tail.
/// IMPORTANT: prior E1 refuses *any* history; this is a separate *manual
/// investigation helper*, not a path to automatic release of an E1 DENY.
#[allow(clippy::too_many_arguments)]
pub fn inspect_bounded_mock_trigger_lineage<E:HttpExecutor>(
 http:&E,store:&NativeStateStore,expected_generation:u64,
 okx:&OkxCredentials,bn:&BinanceCredentials,
 now:u64,domain:OkxDomain,recv:u64,
 okx_algo_id:&str,binance_algo_id:u64,
)->Result<BoundedChildLineageReview,ChildLineageError>{
 let before=store.load().map_err(ChildLineageError::State)?
     .ok_or(ChildLineageError::State(NativeStateError::Generation))?;
 if before.generation!=expected_generation||before.active.is_some()
    ||recv==0||recv>10_000||!valid_id(okx_algo_id)||binance_algo_id==0{
    return Err(bad("invalid owner/parent ids/window before any network"))
 }
 let entry=before.entry.as_ref().ok_or(bad("original entry missing"))?;
 let latch=entry.dryrun_close_latch.as_ref().ok_or(bad("mock close latch absent"))?;
 latch.verify(&before).map_err(ChildLineageError::State)?;
 if latch.phase()!=crate::native_recovery_dryrun_latch::DryRunClosePhase::UnknownManualOnly{
    return Err(bad("no original unknown manual-only mock source"))
 }
 let (spot_amt,fut_amt,_)=latch.amounts();
 let first=inspect_one(http,okx,bn,domain,now,recv,
    &entry.plan.spot_symbol.replace('/',"-"),&entry.plan.futures_symbol,
    &entry.plan.base,spot_amt,fut_amt,okx_algo_id,binance_algo_id)?;
 let second=inspect_one(http,okx,bn,domain,now,recv,
    &entry.plan.spot_symbol.replace('/',"-"),&entry.plan.futures_symbol,
    &entry.plan.base,spot_amt,fut_amt,okx_algo_id,binance_algo_id)?;
 if first!=second{return Err(bad("parent-child-trade/fee lineage changed between reads"))}
 if store.load().map_err(ChildLineageError::State)?.as_ref()!=Some(&before){
     return Err(bad("Native owner changed during child lineage reads"))
 }
 Ok(BoundedChildLineageReview{
    native_generation:expected_generation,synthetic_signed_get_count:16,
    independent_complete_reads:2,
    okx_parent_id:first.okx.parent_id,
    okx_child_order_id:first.okx.child_order_id,
    okx_trade_ids:first.okx.trade_ids,okx_fee_usdt_e12:first.okx.fee_usdt_e12,
    binance_parent_id:first.binance.parent_id,
    binance_child_order_id:first.binance.child_order_id,
    binance_trade_ids:first.binance.trade_ids,
    binance_fee_usdt_e12:first.binance.fee_usdt_e12,
    single_child_per_parent_seen_in_mock:true,
    separate_ordinary_order_and_per_fill_identity_seen_in_mock:true,
    global_historical_parent_coverage_complete:false,
    untraced_sapi_twap_vp_child_orders_proven_absent:false,
    all_parent_children_and_subalgos_proven_resolved:false,
    real_order_submission_proven:false,
    independent_final_account_flatness_proven:false,
    whole_account_funding_transfers_and_lifetime_pnl_proven:false,
    may_retry_cancel_send_or_mark_verified_closed:false,
 })
}
