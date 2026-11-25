//! R32D6A: read-only Binance symbol-WIDE trade causality audit.
//! Compares ALL returned account executions for a futures symbol with one
//! durable unresolved order. Does not grant cross-venue release authority.
use crate::authoritative_state::{
    NativeEntryLeg,NativeEntryRecoveryQuality,NativeStateError,NativeStateStore,
};
use crate::fill_economics::{signed_decimal_to_atoms,FillEvent};
use crate::private_https::{HttpExecutor,HttpsRequest,TransportError};
use crate::private_rest::{
    build_binance_symbol_trade_window_request,parse_binance_fill_events_http,
    BinanceCredentials,HttpMethod,RestBuildError,
};
use serde_json::Value;

const WINDOW_MS:u64=7*24*60*60*1000;
const AGE_MAX_MS:u64=70*24*60*60*1000;
const EARLY_ALLOWANCE_MS:u64=5*60*1000;

#[derive(Clone,Debug,PartialEq,Eq)]
pub enum FuturesTradeScopeError{
    State(NativeStateError),Build(RestBuildError),Transport(TransportError),
    Invalid(&'static str),PotentialTruncation,TooManyWindows,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct FuturesTradeScopeReport{
    pub windows_scanned:usize,
    pub exact_order_fill_events:usize,
    pub sum_signed_position_change_atoms_e12:i128,
    pub matching_trade_ids:usize,
    pub foreign_symbol_order_count:usize,
    pub global_trade_history_proven:bool,
    pub cross_venue_recovery_proven:bool,
    pub may_send_order:bool,
}
fn text_key(row:&Value,key:&str)->Option<String>{
    let value=row.get(key)?;
    if let Some(s)=value.as_str(){return Some(s.to_string())}
    if let Some(n)=value.as_u64(){return Some(n.to_string())}
    None
}
fn typed_event(row:&Value,order_id:&str)->Result<FillEvent,FuturesTradeScopeError>{
    let body=serde_json::to_string(&vec![row])
        .map_err(|_|FuturesTradeScopeError::Invalid("trade serialize"))?;
    let mut events=parse_binance_fill_events_http(200,&body,order_id,"USDT")
        .map_err(|_|FuturesTradeScopeError::Invalid("trade execution fields invalid"))?;
    if events.len()!=1{return Err(FuturesTradeScopeError::Invalid("wrong order ID"))}
    Ok(events.remove(0))
}

/// All order IDs are visible: unlike /userTrades?orderId=..., this must
/// detect unrelated buys/sells even when a zero-net pair leaves the same
/// short position. Entire exchange history is NOT proven by this bounded
/// time-window response, so may_send_order is unconditionally false.
pub fn audit_binance_symbol_wide_trade_scope<E:HttpExecutor>(
    exec:&E,store:&NativeStateStore,expected_generation:u64,
    creds:&BinanceCredentials,now_ms:u64,recv_window_ms:u64,limit:u16,
)->Result<FuturesTradeScopeReport,FuturesTradeScopeError>{
    let pending=store.pending_native_entry_query(expected_generation)
        .map_err(FuturesTradeScopeError::State)?;
    if pending.leg!=NativeEntryLeg::Futures{
        return Err(FuturesTradeScopeError::Invalid("not an unresolved futures intent"))
    }
    let native=store.load().map_err(FuturesTradeScopeError::State)?
        .ok_or(FuturesTradeScopeError::State(NativeStateError::Generation))?;
    if native.generation!=expected_generation{
        return Err(FuturesTradeScopeError::State(NativeStateError::Generation))
    }
    let entry=native.entry.as_ref().ok_or(FuturesTradeScopeError::Invalid("entry absent"))?;
    let proof=entry.recovery_checkpoint.as_ref()
        .ok_or(FuturesTradeScopeError::Invalid("durable fills absent"))?;
    if proof.quality!=NativeEntryRecoveryQuality::FuturesPriorSpotUnverified
        ||proof.leg!=NativeEntryLeg::Futures
        ||proof.intent_id!=pending.intent_id
        ||proof.client_id!=pending.client_id
        ||proof.ledger.fills.is_empty()
        ||!proof.ledger.fee_evidence_complete{
        return Err(FuturesTradeScopeError::Invalid("durable checkpoint mismatch"))
    }
    if now_ms<pending.created_at_ms
        ||now_ms-pending.created_at_ms>AGE_MAX_MS
        ||limit==0 || limit>1000{
        return Err(FuturesTradeScopeError::Invalid("retention or limit invalid"))
    }
    // Validate signing parameters before entering the history scan.
    let begin=pending.created_at_ms.saturating_sub(EARLY_ALLOWANCE_MS).max(1);
    let mut cursor=begin;
    let mut windows=0usize;
    let mut observed=std::collections::BTreeMap::<String,FillEvent>::new();
    let mut signed_qty=0i128;
    while cursor<=now_ms{
        if windows>=12{return Err(FuturesTradeScopeError::TooManyWindows)}
        let until=cursor.saturating_add(WINDOW_MS-1).min(now_ms);
        let request=build_binance_symbol_trade_window_request(
            creds,now_ms,recv_window_ms,&pending.futures_symbol,
            cursor,until,limit,
        ).map_err(FuturesTradeScopeError::Build)?;
        if request.method!=HttpMethod::Get||request.is_mutation{
            return Err(FuturesTradeScopeError::Invalid("nonread-only history request"))
        }
        let response=exec.execute_request(&HttpsRequest::from_signed(&request))
            .map_err(FuturesTradeScopeError::Transport)?;
        if response.status!=200{
            return Err(FuturesTradeScopeError::Invalid("trade list HTTP failed"))
        }
        let json:Value=serde_json::from_str(&response.body)
            .map_err(|_|FuturesTradeScopeError::Invalid("trade list invalid JSON"))?;
        let rows=json.as_array()
            .ok_or(FuturesTradeScopeError::Invalid("trade list not array"))?;
        // An exact page limit cannot prove that a later trade was not
        // silently truncated. Never claim completion from an ambiguous page.
        if rows.len()>=limit as usize{return Err(FuturesTradeScopeError::PotentialTruncation)}
        for row in rows{
            if text_key(row,"symbol").as_deref()!=Some(pending.futures_symbol.as_str()){
                return Err(FuturesTradeScopeError::Invalid("foreign symbol execution in response"))
            }
            let ts=text_key(row,"time").and_then(|s|s.parse::<u64>().ok())
                .ok_or(FuturesTradeScopeError::Invalid("trade time missing"))?;
            if ts<cursor||ts>until{
                return Err(FuturesTradeScopeError::Invalid("trade outside requested window"))
            }
            let trade_id=text_key(row,"id")
                .ok_or(FuturesTradeScopeError::Invalid("trade id absent"))?;
            if observed.contains_key(&trade_id){
                return Err(FuturesTradeScopeError::Invalid("duplicate symbol execution"))
            }
            // Mandatory unconditional ID check: self-offsetting foreign
            // buy+sell executions MUST be rejected BEFORE net-size math.
            if text_key(row,"orderId").as_deref()!=Some(proof.order_id.as_str()){
                return Err(FuturesTradeScopeError::Invalid("unrelated futures execution"))
            }
            if text_key(row,"side").as_deref()!=Some("SELL")
                ||text_key(row,"positionSide").as_deref()!=Some("BOTH")
                ||text_key(row,"marginAsset").as_deref()!=Some("USDT")
                ||row.get("buyer").and_then(Value::as_bool)!=Some(false){
                return Err(FuturesTradeScopeError::Invalid("futures trade direction/mode invalid"))
            }
            let realized=signed_decimal_to_atoms(
                &text_key(row,"realizedPnl")
                    .ok_or(FuturesTradeScopeError::Invalid("PnL absent"))?
            ).map_err(|_|FuturesTradeScopeError::Invalid("PnL malformed"))?;
            if realized!=0{
                return Err(FuturesTradeScopeError::Invalid("realized PnL implies other exposure"))
            }
            let event=typed_event(row,&proof.order_id)?;
            let expected=proof.ledger.fills.get(&trade_id)
                .ok_or(FuturesTradeScopeError::Invalid("execution absent in checkpoint"))?;
            if expected!=&event{
                return Err(FuturesTradeScopeError::Invalid("execution price/quantity/fee changed"))
            }
            signed_qty=signed_qty.checked_sub(
                i128::try_from(event.qty_atoms)
                    .map_err(|_|FuturesTradeScopeError::Invalid("quantity too large"))?
            ).ok_or(FuturesTradeScopeError::Invalid("short quantity overflow"))?;
            observed.insert(trade_id,event);
        }
        windows+=1;
        if until==now_ms{break}
        cursor=until.checked_add(1)
            .ok_or(FuturesTradeScopeError::Invalid("window cursor overflow"))?;
    }
    if observed.len()!=proof.ledger.fills.len()
        ||observed!=proof.ledger.fills{
        return Err(FuturesTradeScopeError::Invalid("not all exact fill IDs accounted"))
    }
    let expected_qty=i128::try_from(proof.ledger.filled_qty_atoms())
        .map_err(|_|FuturesTradeScopeError::Invalid("quantity conversion overflow"))?;
    if signed_qty.checked_add(expected_qty)!=Some(0){
        return Err(FuturesTradeScopeError::Invalid("net exposure mismatch"))
    }
    let current=store.pending_native_entry_query(expected_generation)
        .map_err(FuturesTradeScopeError::State)?;
    if current!=pending{
        return Err(FuturesTradeScopeError::State(NativeStateError::Generation))
    }
    Ok(FuturesTradeScopeReport{
        windows_scanned:windows,
        exact_order_fill_events:observed.len(),
        sum_signed_position_change_atoms_e12:signed_qty,
        matching_trade_ids:observed.len(),
        foreign_symbol_order_count:0,
        global_trade_history_proven:false,
        cross_venue_recovery_proven:false,
        may_send_order:false,
    })
}
