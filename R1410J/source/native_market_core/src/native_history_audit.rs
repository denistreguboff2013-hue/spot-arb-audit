//! R32D4 — review-only bounded, cursor-verified trade history audit.
//! No production I/O in tests, no network mutation and no durable release.
//! Account snapshots are non-atomic and their exchange freshness is NOT proven.
use crate::authoritative_state::{NativeEntryLeg,NativeStateError,NativeStateStore};
use crate::native_entry_recovery::{EntryProbeReport,EntryProbeVerdict};
use crate::private_https::{HttpExecutor,HttpsRequest,TransportError};
use crate::private_rest::{
    OkxCredentials,BinanceCredentials,OkxDomain,SignedRequest,RestBuildError,HttpMethod,
    EconomicTruthClass,build_okx_fills_history_page_request,
    build_binance_user_trades_page_request,
    parse_okx_fill_events_http,parse_binance_fill_events_http,apply_order_economic_truth,
};
use crate::fill_economics::{LegFillLedger,EconError,DEFAULT_AVG_TOLERANCE_ATOMS,signed_decimal_to_atoms};
use serde_json::Value;

const MAX_PAGES:usize=16;
// Conservatively below both exchanges' advertised ~3-month historical reach.
// This is NOT independent evidence that the exchange's data are complete.
const MAX_LOOKBACK_MS:u64=70*24*60*60*1000;
const EXECUTION_EARLY_ALLOWANCE_MS:u64=5*60*1000;
const EXECUTION_FUTURE_ALLOWANCE_MS:u64=5_000;

#[derive(Clone,Debug,PartialEq,Eq)]
pub enum HistoryAuditError{
    State(NativeStateError),Build(RestBuildError),Transport(TransportError),
    Invalid(&'static str),Economics(EconError),
    HistoryWindowUnverifiable,PageLimitExceeded,
}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum HistoryAuditVerdict {
    FullPageExhaustionCandidateAccountFreshnessUnverified,
    PriorSpotFeeOriginUnverified,
}
#[derive(Clone,Debug)]
pub struct HistoryAuditReport {
    pub venue_leg:NativeEntryLeg,
    pub page_count:usize,
    pub trade_count:usize,
    pub gross_filled_atoms_e18:u128,
    pub fee_quote_atoms_e12:i128,
    pub base_fee_atoms_e18:Option<i128>,
    pub verdict:HistoryAuditVerdict,
    pub page_exhaustion_observed:bool,
    pub account_snapshot_freshness_proven:bool,
    pub may_send_order:bool,
}

fn scalar(row:&Value,key:&str)->Option<String>{
    let v=row.get(key)?;
    if let Some(v)=v.as_str(){return Some(v.into())}
    if let Some(v)=v.as_u64(){return Some(v.to_string())}
    None
}

fn signed_read_only<E:HttpExecutor>(
    exec:&E,request:&SignedRequest,
)->Result<crate::private_https::HttpResponse,HistoryAuditError>{
    if request.method!=HttpMethod::Get||request.is_mutation{
        return Err(HistoryAuditError::Invalid("mutation-shaped history request"))
    }
    exec.execute_request(&HttpsRequest::from_signed(request))
        .map_err(HistoryAuditError::Transport)
}

/// R32D4: explicit empty tail for a cursor series, monotonically ordered
/// OKX billId / Binance trade ID and all fill events accounted for.
/// No success can grant a trading permit or modify NativeStateStore.
#[allow(clippy::too_many_arguments)]
pub fn audit_native_entry_paginated_history<E:HttpExecutor>(
    executor:&E,store:&NativeStateStore,probe:&EntryProbeReport,
    okx:&OkxCredentials,binance:&BinanceCredentials,
    unix_ms:u64,okx_domain:OkxDomain,
    recv_window_ms:u64,page_limit:u16,
)->Result<HistoryAuditReport,HistoryAuditError>{
    let target=store.pending_native_entry_query(probe.target.generation)
        .map_err(HistoryAuditError::State)?;
    if target!=probe.target||probe.may_send_order
        ||!probe.state_generation_unchanged
        ||probe.verdict!=EntryProbeVerdict::TerminalCandidateNeedsFillFeesAndInventoryTruth
        ||probe.order.class!=EconomicTruthClass::Known
        ||!probe.order.terminal
        ||probe.order.client_order_id.as_deref()!=Some(target.client_id.as_str()){
        return Err(HistoryAuditError::Invalid("account and exact order probe missing"))
    }
    // Strict source-age guard: old/clock-skewed history cannot silently
    // certify "complete". The exchange may have stopped retaining records.
    if unix_ms<target.created_at_ms
        ||unix_ms.saturating_sub(target.created_at_ms)>MAX_LOOKBACK_MS{
        return Err(HistoryAuditError::HistoryWindowUnverifiable)
    }
    let order_id=probe.order.order_id.as_deref()
        .filter(|x|!x.is_empty()).ok_or(HistoryAuditError::Invalid("order ID missing"))?;
    if page_limit==0 || page_limit>match target.leg{
        NativeEntryLeg::Spot=>100,NativeEntryLeg::Futures=>1000,
    }{
        return Err(HistoryAuditError::Invalid("page limit out of venue bounds"))
    }
    if target.requested_atoms_e18%1_000_000!=0{
        return Err(HistoryAuditError::Invalid("economics atom precision mismatch"))
    }
    let mut ledger=LegFillLedger::new(
        if target.leg==NativeEntryLeg::Spot{"okx"}else{"binance"},
        &target.symbol,if target.leg==NativeEntryLeg::Spot{"buy"}else{"sell"},
        &target.intent_id,&target.client_id,target.requested_atoms_e18/1_000_000,
    ).map_err(HistoryAuditError::Economics)?;
    apply_order_economic_truth(&mut ledger,&probe.order)
        .map_err(|_|HistoryAuditError::Invalid("order economics invalid"))?;
    let mut pages=0usize;
    let mut seen=std::collections::HashSet::<String>::new();
    let mut cursor_okx:Option<String>=None;
    let mut cursor_bin:u64=0;
    let mut last_numeric:Option<u128>=None;
    let mut base_fee_e12=0i128;
    let mut complete=false;
    for _ in 0..MAX_PAGES{
        let request=match target.leg{
            NativeEntryLeg::Spot=>build_okx_fills_history_page_request(
                okx,unix_ms,okx_domain,&target.symbol,order_id,
                cursor_okx.as_deref(),page_limit),
            NativeEntryLeg::Futures=>build_binance_user_trades_page_request(
                binance,unix_ms,recv_window_ms,&target.symbol,order_id,
                cursor_bin,page_limit),
        }.map_err(HistoryAuditError::Build)?;
        let res=signed_read_only(executor,&request)?;
        pages+=1;
        let json:Value=serde_json::from_str(&res.body)
            .map_err(|_|HistoryAuditError::Invalid("invalid history json"))?;
        let rows=if target.leg==NativeEntryLeg::Spot{
            if res.status!=200||json.get("code").and_then(Value::as_str)!=Some("0"){
                return Err(HistoryAuditError::Invalid("OKX history error"))
            }
            json.get("data").and_then(Value::as_array)
        }else{
            if res.status!=200{return Err(HistoryAuditError::Invalid("Binance history error"))}
            json.as_array()
        }.ok_or(HistoryAuditError::Invalid("history rows unavailable"))?;
        if rows.len()>page_limit as usize{
            return Err(HistoryAuditError::Invalid("history page exceeds limit"))
        }
        if rows.is_empty(){
            complete=true;
            break
        }
        let events=match target.leg{
            NativeEntryLeg::Spot=>parse_okx_fill_events_http(
                res.status,&res.body,order_id,&target.base,"USDT"),
            NativeEntryLeg::Futures=>parse_binance_fill_events_http(
                res.status,&res.body,order_id,"USDT"),
        }.map_err(|_|HistoryAuditError::Invalid("unparseable trade rows"))?;
        if events.len()!=rows.len(){
            return Err(HistoryAuditError::Invalid("unrelated order trade present"))
        }
        for (row,fill) in rows.iter().zip(events.into_iter()){
            let order_key=if target.leg==NativeEntryLeg::Spot{"ordId"}else{"orderId"};
            if scalar(row,order_key).as_deref()!=Some(order_id){
                return Err(HistoryAuditError::Invalid("wrong order in history"))
            }
            let cursor_key=if target.leg==NativeEntryLeg::Spot{"billId"}else{"id"};
            let cursor=scalar(row,cursor_key)
                .ok_or(HistoryAuditError::Invalid("missing trade/bill cursor"))?;
            let current=cursor.parse::<u128>()
                .map_err(|_|HistoryAuditError::Invalid("non-numeric history cursor"))?;
            if current==0{
                return Err(HistoryAuditError::Invalid("zero history cursor"))
            }
            if let Some(previous)=last_numeric{
                let monotone=if target.leg==NativeEntryLeg::Spot{
                    current<previous
                }else{current>previous};
                if !monotone{
                    return Err(HistoryAuditError::Invalid("nonmonotonic history cursor"))
                }
            }
            if target.leg==NativeEntryLeg::Futures && current<u128::from(cursor_bin){
                return Err(HistoryAuditError::Invalid("Binance fromId rewind"))
            }
            last_numeric=Some(current);
            if !seen.insert(fill.fill_id.clone()){
                return Err(HistoryAuditError::Invalid("duplicate fill ID across pages"))
            }
            let t=fill.trade_time_ms
                .ok_or(HistoryAuditError::Invalid("fill timestamp missing"))?;
            if t<target.created_at_ms.saturating_sub(EXECUTION_EARLY_ALLOWANCE_MS)
                ||t>unix_ms.saturating_add(EXECUTION_FUTURE_ALLOWANCE_MS){
                return Err(HistoryAuditError::Invalid("fill outside provenance window"))
            }
            if target.leg==NativeEntryLeg::Spot{
                let fee=scalar(row,"fee")
                    .ok_or(HistoryAuditError::Invalid("OKX fee missing"))?;
                let currency=scalar(row,"feeCcy")
                    .ok_or(HistoryAuditError::Invalid("OKX fee asset missing"))?;
                if currency.eq_ignore_ascii_case(&target.base){
                    base_fee_e12=base_fee_e12.checked_add(
                        signed_decimal_to_atoms(&fee).map_err(HistoryAuditError::Economics)?
                    ).ok_or(HistoryAuditError::Invalid("fee overflow"))?;
                }else if !currency.eq_ignore_ascii_case("USDT"){
                    return Err(HistoryAuditError::Invalid("unsupported OKX fee asset"))
                }
            }else if fill.fee_cost_quote_atoms.is_none(){
                return Err(HistoryAuditError::Invalid("unknown Binance fee asset"))
            }
            ledger.apply_fill(fill,DEFAULT_AVG_TOLERANCE_ATOMS)
                .map_err(HistoryAuditError::Economics)?;
        }
        if target.leg==NativeEntryLeg::Spot{
            cursor_okx=Some(last_numeric.unwrap().to_string());
        }else{
            let max=u64::try_from(last_numeric.unwrap())
                .map_err(|_|HistoryAuditError::Invalid("Binance cursor overflow"))?;
            cursor_bin=max.checked_add(1)
                .ok_or(HistoryAuditError::Invalid("Binance next cursor overflow"))?;
        }
    }
    if !complete{return Err(HistoryAuditError::PageLimitExceeded)}
    if ledger.fills.is_empty()||!ledger.fee_evidence_complete
        ||ledger.fills.values().try_fold(0u128,|total,f|total.checked_add(f.qty_atoms))
          !=Some(ledger.cumulative_qty_atoms){
        return Err(HistoryAuditError::Invalid("history not proven quantity/fee complete"))
    }
    let qty=ledger.filled_qty_atoms().checked_mul(1_000_000)
        .ok_or(HistoryAuditError::Invalid("quantity scale overflow"))?;
    let base_fee=if target.leg==NativeEntryLeg::Spot{
        Some(base_fee_e12.checked_mul(1_000_000)
            .ok_or(HistoryAuditError::Invalid("base fee scale overflow"))?)
    }else{None};
    if let Some(fee)=base_fee{
        let cash=probe.observed_spot_cash_atoms_e18
            .and_then(|n|i128::try_from(n).ok());
        let expected=i128::try_from(target.spot_baseline_atoms_e18).ok()
            .and_then(|n|n.checked_add(i128::try_from(qty).ok()?))
            .and_then(|n|n.checked_add(fee));
        if cash.is_none()||cash!=expected{
            return Err(HistoryAuditError::Invalid("net OKX cash provenance mismatch"))
        }
    }
    let current=store.pending_native_entry_query(target.generation)
        .map_err(HistoryAuditError::State)?;
    if current!=target{return Err(HistoryAuditError::State(NativeStateError::Generation))}
    Ok(HistoryAuditReport{
        venue_leg:target.leg,page_count:pages,
        trade_count:ledger.fills.len(),gross_filled_atoms_e18:qty,
        fee_quote_atoms_e12:ledger.fee_cost_quote_atoms,
        base_fee_atoms_e18:base_fee,
        verdict:if target.leg==NativeEntryLeg::Spot{
            HistoryAuditVerdict::FullPageExhaustionCandidateAccountFreshnessUnverified
        }else{HistoryAuditVerdict::PriorSpotFeeOriginUnverified},
        page_exhaustion_observed:true,
        account_snapshot_freshness_proven:false,
        may_send_order:false,
    })
}
