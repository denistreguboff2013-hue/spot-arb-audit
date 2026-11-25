//! TRADE2D2E1, deliberately conservative historical algo presence veto.
//! This is NOT child-order provenance and NEVER proves all account history.
//! At slightest presence/ambiguity -> MANUAL, no VerifiedClosed. NO POST.
use crate::{
 authoritative_state::{NativeStateStore,NativeStateError},
 native_dryrun_pending_algos::{
     inspect_mock_pending_algos_after_close,MockPendingAlgoAudit,MockAlgoError,
     OKX_PENDING_ALGO_TYPES,
 },
 private_https::{HttpExecutor,HttpResponse,HttpsRequest,TransportError},
 private_rest::{
     BinanceCredentials,OkxCredentials,OkxDomain,RestBuildError,
     HttpMethod,SignedRequest,build_binance_all_algo_orders_history_request,
     build_okx_algo_history_type_state_request,
 },
};
use serde_json::Value;
const HISTORY_SPAN:u64=6*24*60*60*1000;
const HISTORY_STATES:[&str;3]=["effective","canceled","order_failed"];

#[derive(Clone,Debug,PartialEq,Eq)]
pub enum AlgoHistoryError{
 Previous(MockAlgoError),State(NativeStateError),
 Build(RestBuildError),Transport(TransportError),
 Invalid(&'static str),
}
fn invalid(s:&'static str)->AlgoHistoryError{AlgoHistoryError::Invalid(s)}
#[derive(Clone,Debug,PartialEq,Eq)]
struct HistoryWitness{
    checked_binance_unfiltered:bool,
    okx_queries:Vec<(String,String)>,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct BoundedAlgoHistoryReview{
    pub previous:MockPendingAlgoAudit,
    pub synthetic_signed_gets:u8,
    pub history_scans:u8,
    pub binance_unfiltered_recent_history_empty_in_both:bool,
    pub okx_known_state_type_history_empty_in_both:bool,
    pub okx_combinations_checked:usize,
    pub binance_window_start_ms:u64,
    pub binance_window_end_ms:u64,
    pub known_old_triggered_child_orders_proven_absent:bool,
    pub across_all_history_and_account_transfers_proven_complete:bool,
    pub full_lifetime_pnl_proven:bool,
    pub actual_live_close_proven:bool,
    pub may_cancel_retry_or_send_order:bool,
    pub may_verify_closed:bool,
}
fn get<E:HttpExecutor>(http:&E,req:&SignedRequest)->Result<Value,AlgoHistoryError>{
    if req.method!=HttpMethod::Get||req.is_mutation||req.body.is_some(){
        return Err(invalid("algo history must only use GET"))
    }
    let HttpResponse{status,body,..}=http.execute_request(&HttpsRequest::from_signed(req))
        .map_err(AlgoHistoryError::Transport)?;
    if status!=200{return Err(invalid("history permission/error is NOT absence"))}
    serde_json::from_str(&body).map_err(|_|invalid("history response not JSON"))
}
fn one<E:HttpExecutor>(
    http:&E,okx:&OkxCredentials,bn:&BinanceCredentials,
    now:u64,domain:OkxDomain,recv:u64,start:u64
)->Result<HistoryWitness,AlgoHistoryError>{
    let req=build_binance_all_algo_orders_history_request(
        bn,now,recv,start,now).map_err(AlgoHistoryError::Build)?;
    let result=get(http,&req)?;
    let rows=result.as_array().ok_or(invalid("Binance historical list missing"))?;
    // Never infer harmlessness from a CANCELED row: it may have spawned
    // a real order already. Strict presence veto until independent lineage.
    if !rows.is_empty(){return Err(invalid("Binance algo history has unresolved record"))}
    let mut checked=Vec::with_capacity(24);
    for kind in OKX_PENDING_ALGO_TYPES{
        for state in HISTORY_STATES{
            let req=build_okx_algo_history_type_state_request(
                okx,now,domain,kind,state).map_err(AlgoHistoryError::Build)?;
            let result=get(http,&req)?;
            if result.get("code").and_then(Value::as_str)!=Some("0"){
                return Err(invalid("OKX history business error"))
            }
            let rows=result.get("data").and_then(Value::as_array)
                .ok_or(invalid("OKX algo historical data absent"))?;
            if !rows.is_empty(){
                return Err(invalid("OKX history has unresolved triggered/canceled record"))
            }
            checked.push((kind.to_string(),state.to_string()));
        }
    }
    Ok(HistoryWitness{
        checked_binance_unfiltered:true,okx_queries:checked
    })
}
/// Chains existing read-only 46-GET mock closing/fill/account/pending
/// reviews with 2 rounds of 25-GET historical algo queries: 96 in all.
/// NO historical completeness claims even on zero-record responses.
#[allow(clippy::too_many_arguments)]
pub fn inspect_bounded_mock_algo_history<E:HttpExecutor>(
    http:&E,store:&NativeStateStore,gen:u64,
    okx:&OkxCredentials,bn:&BinanceCredentials,
    now:u64,domain:OkxDomain,recv:u64
)->Result<BoundedAlgoHistoryReview,AlgoHistoryError>{
    if now<=HISTORY_SPAN||recv==0||recv>10_000{
        return Err(invalid("invalid clock or recvWindow before history requests"))
    }
    let baseline=store.load().map_err(AlgoHistoryError::State)?
        .ok_or(AlgoHistoryError::State(NativeStateError::Generation))?;
    if baseline.generation!=gen||baseline.active.is_some(){
        return Err(invalid("wrong native state owner before history requests"))
    }
    let preceding=inspect_mock_pending_algos_after_close(
        http,store,gen,okx,bn,now,domain,recv).map_err(AlgoHistoryError::Previous)?;
    let start=now-HISTORY_SPAN;
    let first=one(http,okx,bn,now,domain,recv,start)?;
    let second=one(http,okx,bn,now,domain,recv,start)?;
    if first!=second{return Err(invalid("historical query coverage changed between passes"))}
    if store.load().map_err(AlgoHistoryError::State)?.as_ref()!=Some(&baseline){
        return Err(invalid("Native owner changed during historical algo review"))
    }
    Ok(BoundedAlgoHistoryReview{
        previous:preceding,
        synthetic_signed_gets:96,history_scans:2,
        binance_unfiltered_recent_history_empty_in_both:first.checked_binance_unfiltered,
        okx_known_state_type_history_empty_in_both:true,
        okx_combinations_checked:first.okx_queries.len(),
        binance_window_start_ms:start,binance_window_end_ms:now,
        known_old_triggered_child_orders_proven_absent:false,
        across_all_history_and_account_transfers_proven_complete:false,
        full_lifetime_pnl_proven:false,
        actual_live_close_proven:false,
        may_cancel_retry_or_send_order:false,may_verify_closed:false,
    })
}
