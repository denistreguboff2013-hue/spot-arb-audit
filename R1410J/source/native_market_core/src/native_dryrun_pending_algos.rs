//! TRADE2D2D (offline): check signed GET-only pending algorithmic orders
//! across ALL account symbols, not only the original traded BNT market.
//! Never authorization, only a strictly non-executable MOCK diagnostic.
use crate::{
    authoritative_state::{NativeStateStore,NativeStateError},
    native_dryrun_close_accounts::{
        inspect_mock_full_close_accounts,MockFinalAccountError,MockFinalAccountReview,
    },
    private_https::{HttpExecutor,HttpsRequest,HttpResponse,TransportError},
    private_rest::{
        BinanceCredentials,OkxCredentials,OkxDomain,RestBuildError,
        SignedRequest,HttpMethod,build_binance_all_open_algo_orders_request,
        build_okx_pending_algo_type_request,
    },
};
use serde_json::Value;

pub const OKX_PENDING_ALGO_TYPES:[&str;8]=[
    "conditional","oco","trigger","move_order_stop",
    "chase","iceberg","twap","smart_iceberg",
];
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum MockAlgoError{
    Prior(MockFinalAccountError),
    State(NativeStateError),
    Build(RestBuildError),
    Transport(TransportError),
    Invalid(&'static str),
}
fn deny(msg:&'static str)->MockAlgoError{MockAlgoError::Invalid(msg)}
#[derive(Clone,Debug,PartialEq,Eq)]
struct PendingAlgoSnapshot {
    binance_zero_all_symbols:bool,
    okx_types_scanned:Vec<String>,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct MockPendingAlgoAudit {
    pub previous:MockFinalAccountReview,
    pub total_mock_signed_gets:u8,
    pub independent_algo_snapshots:u8,
    pub binance_all_symbols_open_algo_zero_in_both:bool,
    pub okx_all_symbols_known_algo_types_zero_in_both:bool,
    pub checked_okx_types:Vec<String>,
    pub no_new_types_or_endpoint_semantics_proven:bool,
    pub no_future_delayed_trigger_or_cross_venue_race_proven:bool,
    pub actual_exchange_order_ever_sent_proven:bool,
    pub global_income_and_lifetime_pnl_proven:bool,
    pub may_cancel_or_retry_order:bool,
    pub may_send_order:bool,
    pub may_mark_verified_closed:bool,
}
fn get<E:HttpExecutor>(http:&E,req:&SignedRequest)->Result<Value,MockAlgoError>{
    if req.method!=HttpMethod::Get||req.is_mutation||req.body.is_some(){
        return Err(deny("algo inspection must never POST/DELETE"))
    }
    let HttpResponse{status,body,..}=http.execute_request(&HttpsRequest::from_signed(req))
        .map_err(MockAlgoError::Transport)?;
    if status!=200{return Err(deny("algo status non-200; cannot assume empty"))}
    serde_json::from_str(&body).map_err(|_|deny("algo JSON malformed"))
}
fn one<E:HttpExecutor>(
    http:&E,okx:&OkxCredentials,bn:&BinanceCredentials,
    now:u64,domain:OkxDomain,recv:u64,
)->Result<PendingAlgoSnapshot,MockAlgoError>{
    // The expensive unfiltered Binance request carries 40 IP weight.
    // This is NEVER issued to real venues by these synthetic tests.
    let request=build_binance_all_open_algo_orders_request(bn,now,recv)
        .map_err(MockAlgoError::Build)?;
    let v=get(http,&request)?;
    let rows=v.as_array().ok_or(deny("Binance algo orders not array"))?;
    if !rows.is_empty(){return Err(deny("Binance active conditional order exists"))}
    let mut okx_types_scanned=Vec::with_capacity(OKX_PENDING_ALGO_TYPES.len());
    for kind in OKX_PENDING_ALGO_TYPES{
        let request=build_okx_pending_algo_type_request(okx,now,domain,kind)
            .map_err(MockAlgoError::Build)?;
        let v=get(http,&request)?;
        if v.get("code").and_then(Value::as_str)!=Some("0"){
            return Err(deny("OKX pending algo business code !=0"))
        }
        let rows=v.get("data").and_then(Value::as_array)
            .ok_or(deny("OKX pending algo data array absent"))?;
        if !rows.is_empty(){return Err(deny("OKX pending algo order exists"))}
        okx_types_scanned.push(kind.to_string());
    }
    Ok(PendingAlgoSnapshot{binance_zero_all_symbols:true,okx_types_scanned})
}
/// Full prior 28-GET MOCK closing history+balances, then 2 x (1 Binance
/// all-symbol algo + 8 OKX types) = 46 total GETs on positive fixture.
/// Still no real VerifiedClosed or physical execution ability.
#[allow(clippy::too_many_arguments)]
pub fn inspect_mock_pending_algos_after_close<E:HttpExecutor>(
    http:&E,store:&NativeStateStore,expected_gen:u64,
    okx:&OkxCredentials,bn:&BinanceCredentials,
    now:u64,domain:OkxDomain,recv:u64,
)->Result<MockPendingAlgoAudit,MockAlgoError>{
    let initial=store.load().map_err(MockAlgoError::State)?
        .ok_or(MockAlgoError::State(NativeStateError::Generation))?;
    if initial.generation!=expected_gen||initial.active.is_some()||recv==0||recv>10_000{
        return Err(deny("invalid Native owner before private signed GET"))
    }
    let prior=inspect_mock_full_close_accounts(
        http,store,expected_gen,okx,bn,now,domain,recv)
        .map_err(MockAlgoError::Prior)?;
    if prior.may_verify_closed||prior.may_retry_or_cancel
        ||prior.actual_signed_live_close_proven{
        return Err(deny("unexpected authority from previous mock account proof"))
    }
    let first=one(http,okx,bn,now,domain,recv)?;
    let second=one(http,okx,bn,now,domain,recv)?;
    if first!=second{return Err(deny("algo survey changed across rounds"))}
    if store.load().map_err(MockAlgoError::State)?.as_ref()!=Some(&initial){
        return Err(deny("Native owner changed during algo survey"))
    }
    Ok(MockPendingAlgoAudit{
        previous:prior,total_mock_signed_gets:46,
        independent_algo_snapshots:2,
        binance_all_symbols_open_algo_zero_in_both:first.binance_zero_all_symbols,
        okx_all_symbols_known_algo_types_zero_in_both:true,
        checked_okx_types:first.okx_types_scanned,
        no_new_types_or_endpoint_semantics_proven:false,
        no_future_delayed_trigger_or_cross_venue_race_proven:false,
        actual_exchange_order_ever_sent_proven:false,
        global_income_and_lifetime_pnl_proven:false,
        may_cancel_or_retry_order:false,may_send_order:false,
        may_mark_verified_closed:false,
    })
}
