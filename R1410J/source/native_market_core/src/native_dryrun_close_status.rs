//! TRADE2D2B: synthetic signed GET-only order-status review after a MOCK
//! timeout. Four READ-ONLY lookups, no cancel, no POST and no close permit.
//! Even two "FILLED" statuses do not prove fees, inventory or real close.
use crate::{
 authoritative_state::{NativeStateStore,NativeStateError},
 fill_economics::decimal_to_atoms,
 private_https::{HttpExecutor,HttpResponse,HttpsRequest,TransportError},
 private_rest::{
   BinanceCredentials,OkxCredentials,OkxDomain,RestBuildError,
   HttpMethod,SignedRequest,build_binance_query_by_client_id,
   build_okx_query_by_client_id,
 },
 native_recovery_dryrun_latch::DryRunClosePhase,
};
use serde_json::Value;

#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum ObservedCloseState {
    OpenNoFill,OpenPartial,TerminalFilled,
    TerminalCanceledNoFill,TerminalCanceledPartial,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct CloseStatusLeg{
    pub client_id:String,
    pub exchange_order_id:String,
    pub state:ObservedCloseState,
    pub filled_e12:u128,
    pub target_e12:u128,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct MockCloseStatusReview{
    pub native_generation:u64,
    pub signed_get_count:u8,
    pub okx:CloseStatusLeg,
    pub binance:CloseStatusLeg,
    pub identical_double_reads:bool,
    pub requires_manual_reconciliation:bool,
    pub actual_order_dispatch_proven:bool,
    pub original_trades_and_exit_fees_proven:bool,
    pub open_orders_and_positions_flat_proven:bool,
    pub may_cancel_order:bool,
    pub may_retry_order:bool,
    pub may_send_order:bool,
    pub may_verify_closed:bool,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum MockCloseStatusError{
    State(NativeStateError),Build(RestBuildError),
    Transport(TransportError),Invalid(&'static str),
}
fn bad(x:&'static str)->MockCloseStatusError{MockCloseStatusError::Invalid(x)}
fn str_field<'a>(v:&'a Value,key:&str)->Result<&'a str,MockCloseStatusError>{
    v.get(key).and_then(Value::as_str).ok_or(bad("order field absent"))
}
fn id(v:&Value,key:&str)->Result<String,MockCloseStatusError>{
    let f=v.get(key).ok_or(bad("order identity absent"))?;
    if let Some(x)=f.as_str(){return Ok(x.to_string())}
    f.as_u64().map(|x|x.to_string()).ok_or(bad("order identity invalid"))
}
fn positive_id(x:&str)->bool{
    !x.is_empty()&&x.len()<=40&&x.bytes().all(|x|x.is_ascii_digit())
        &&x.bytes().any(|x|x!=b'0')
}
fn fixed(v:&Value,key:&str)->Result<u128,MockCloseStatusError>{
    decimal_to_atoms(str_field(v,key)?).map_err(|_|bad("order quantity not exact fixed-point"))
}
fn received_ms(v:&Value,key:&str,now:u64)->Result<(),MockCloseStatusError>{
    let f=v.get(key).ok_or(bad("order observed timestamp missing"))?;
    let t=f.as_u64().or_else(||f.as_str().and_then(|x|x.parse().ok()))
        .ok_or(bad("order timestamp malformed"))?;
    if now==0||t==0||t>now.saturating_add(5_000)
        ||now.saturating_sub(t)>60_000{
        return Err(bad("order observation stale or in future"))
    }
    Ok(())
}
fn status(s:&str,filled:u128,target:u128)->Result<ObservedCloseState,MockCloseStatusError>{
    if target==0||filled>target{return Err(bad("executed size exceeds original mock target"))}
    match s{
        "NEW"|"new"|"live"=>{
            if filled==0{Ok(ObservedCloseState::OpenNoFill)}
            else{Err(bad("nonzero fill with NEW/live status"))}
        }
        "PARTIALLY_FILLED"|"partially_filled"=>{
            if filled>0&&filled<target{Ok(ObservedCloseState::OpenPartial)}
            else{Err(bad("partial status inconsistent with quantity"))}
        }
        "FILLED"|"filled"=>{
            if filled==target{Ok(ObservedCloseState::TerminalFilled)}
            else{Err(bad("FILLED status without full quantity"))}
        }
        "CANCELED"|"CANCELLED"|"canceled"|"cancelled"|"mmp_canceled"=>{
            if filled==0{Ok(ObservedCloseState::TerminalCanceledNoFill)}
            else if filled<target{Ok(ObservedCloseState::TerminalCanceledPartial)}
            else{Err(bad("canceled status with full quantity requires extra evidence"))}
        }
        _=>Err(bad("exchange status unrecognized or cannot prove state"))
    }
}
fn get<E:HttpExecutor>(http:&E,req:&SignedRequest)->Result<String,MockCloseStatusError>{
    if req.method!=HttpMethod::Get||req.is_mutation||req.body.is_some(){
        return Err(bad("dryrun status GET may not mutate network"))
    }
    let HttpResponse{status,body,..}=http.execute_request(&HttpsRequest::from_signed(req))
        .map_err(MockCloseStatusError::Transport)?;
    if status!=200{return Err(bad("non-200 status: not-found is NOT proof no order"))}
    Ok(body)
}

fn okx_one(body:&str,cid:&str,symbol:&str,target:u128,now:u64)
 ->Result<CloseStatusLeg,MockCloseStatusError>{
    let v:Value=serde_json::from_str(body).map_err(|_|bad("OKX query JSON invalid"))?;
    if v.get("code").and_then(Value::as_str)!=Some("0"){
        return Err(bad("OKX business error; query not definitive"))
    }
    let arr=v.get("data").and_then(Value::as_array)
        .ok_or(bad("OKX order response missing array"))?;
    if arr.len()!=1{return Err(bad("OKX original close order not uniquely found"))}
    let row=&arr[0];
    if str_field(row,"instId")?!=symbol
       ||str_field(row,"clOrdId")?!=cid
       ||str_field(row,"side")?!="sell"
       ||str_field(row,"tdMode")?!="cash"{
        return Err(bad("OKX close identity, venue or sell direction mismatch"))
    }
    let oid=id(row,"ordId")?;
    if !positive_id(&oid){return Err(bad("OKX invalid order ID"))}
    received_ms(row,"uTime",now)?;
    if fixed(row,"sz")?!=target{return Err(bad("OKX original order quantity changed"))}
    let executed=fixed(row,"accFillSz")?;
    let state=status(str_field(row,"state")?,executed,target)?;
    Ok(CloseStatusLeg{
        client_id:cid.to_string(),exchange_order_id:oid,
        state,filled_e12:executed,target_e12:target
    })
}
fn binance_one(body:&str,cid:&str,symbol:&str,target:u128,now:u64)
 ->Result<CloseStatusLeg,MockCloseStatusError>{
    let v:Value=serde_json::from_str(body).map_err(|_|bad("Binance query JSON invalid"))?;
    if str_field(&v,"symbol")?!=symbol
       ||str_field(&v,"clientOrderId")?!=cid
       ||str_field(&v,"side")?!="BUY"
       ||str_field(&v,"positionSide")?!="BOTH"
       ||v.get("reduceOnly").and_then(Value::as_bool)!=Some(true){
        return Err(bad("Binance original close identity or reduceOnly invalid"))
    }
    let oid=id(&v,"orderId")?;
    if !positive_id(&oid){return Err(bad("Binance invalid order ID"))}
    received_ms(&v,"updateTime",now)?;
    if fixed(&v,"origQty")?!=target{return Err(bad("Binance requested close quantity changed"))}
    let executed=fixed(&v,"executedQty")?;
    let state=status(str_field(&v,"status")?,executed,target)?;
    Ok(CloseStatusLeg{
       client_id:cid.to_string(),exchange_order_id:oid,
       state,filled_e12:executed,target_e12:target
    })
}
/// Four signed GET-only observations (two independent rounds).
/// If ANY observation is ambiguous, malformed or changing, deny. This
/// cannot prove that such a real order ever existed: the source latch is
/// MOCK ONLY. A non-200 / not-found response is NEVER absence evidence.
#[allow(clippy::too_many_arguments)]
pub fn inspect_mock_unknown_close<E:HttpExecutor>(
    http:&E,store:&NativeStateStore,expected_generation:u64,
    okx:&OkxCredentials,bn:&BinanceCredentials,
    now:u64,domain:OkxDomain,recv_window_ms:u64,
)->Result<MockCloseStatusReview,MockCloseStatusError>{
    let original=store.load().map_err(MockCloseStatusError::State)?
       .ok_or(MockCloseStatusError::State(NativeStateError::Generation))?;
    if original.generation!=expected_generation||original.active.is_some()
       ||recv_window_ms==0||recv_window_ms>10_000{
        return Err(bad("invalid owner or GET recvWindow"))
    }
    let entry=original.entry.as_ref().ok_or(bad("no original recovered entry"))?;
    let latch=entry.dryrun_close_latch.as_ref()
        .ok_or(bad("no MOCK close latch"))?;
    latch.verify(&original).map_err(MockCloseStatusError::State)?;
    if latch.phase()!=DryRunClosePhase::UnknownManualOnly{
        return Err(bad("only UNKNOWN manual-only mock may be queried"))
    }
    let (spot_id,future_id)=latch.ids();
    let (spot_qty,future_qty,_unmatched)=latch.amounts();
    let okx_request=build_okx_query_by_client_id(
        okx,now,domain,&entry.plan.spot_symbol,spot_id)
        .map_err(MockCloseStatusError::Build)?;
    let binance_request=build_binance_query_by_client_id(
        bn,now,recv_window_ms,&entry.plan.futures_symbol,future_id)
        .map_err(MockCloseStatusError::Build)?;
    let run=||->Result<(CloseStatusLeg,CloseStatusLeg),MockCloseStatusError>{
        let a=get(http,&okx_request)?;
        let b=get(http,&binance_request)?;
        Ok((okx_one(&a,spot_id,&entry.plan.spot_symbol.replace('/',"-"),
                    spot_qty,now)?,
            binance_one(&b,future_id,&entry.plan.futures_symbol,
                    future_qty,now)?))
    };
    let first=run()?;
    let second=run()?;
    if first!=second{
        return Err(bad("original close status changed between independent GET rounds"))
    }
    if store.load().map_err(MockCloseStatusError::State)?.as_ref()!=Some(&original){
        return Err(bad("Native owner or payload changed during close query"))
    }
    Ok(MockCloseStatusReview{
        native_generation:expected_generation,signed_get_count:4,
        okx:first.0,binance:first.1,
        identical_double_reads:true,
        requires_manual_reconciliation:true,
        actual_order_dispatch_proven:false,
        original_trades_and_exit_fees_proven:false,
        open_orders_and_positions_flat_proven:false,
        may_cancel_order:false,may_retry_order:false,
        may_send_order:false,may_verify_closed:false
    })
}
