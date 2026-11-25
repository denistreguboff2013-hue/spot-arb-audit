//! R32D5 review-only OKX base-currency movement ancestry audit.
//! Independently checks bill-level provenance against the durable R32D3 fill
//! checkpoint. Does not prove exchange API completeness, transfer-system
//! completeness, or cross-exchange snapshot atomicity. NO trading release.
use crate::authoritative_state::{
    NativeEntryLeg,NativeEntryRecoveryQuality,NativeStateError,NativeStateStore,
};
use crate::fill_economics::{decimal_to_atoms,signed_decimal_to_atoms};
use crate::private_https::{HttpExecutor,HttpsRequest,TransportError};
use crate::private_rest::{
    build_okx_balance_request,build_okx_base_currency_bills_page_request,
    HttpMethod,OkxCredentials,OkxDomain,RestBuildError,SignedRequest,
};
use serde_json::Value;

const MAX_PAGES:usize=16;
const LOOKBACK_LIMIT_MS:u64=70*24*60*60*1000;
const UPDATE_AGE_MAX_MS:u64=180_000;
const FUTURE_TOLERANCE_MS:u64=5_000;

#[derive(Clone,Debug,PartialEq,Eq)]
pub enum CashLineageError{
    State(NativeStateError),Build(RestBuildError),Transport(TransportError),
    Invalid(&'static str),NotFresh,HistoryNotExhausted,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct CashLineageReport{
    pub account_bill_count:usize,
    pub pages:usize,
    pub observed_cash_e18:u128,
    pub accounted_trade_delta_e18:i128,
    pub aggregate_base_fee_e18:i128,
    pub pre_post_update_ms:u64,
    pub base_currency_movement_chain_matched:bool,
    pub cross_venue_snapshot_atomicity_proven:bool,
    pub account_history_global_completeness_proven:bool,
    pub may_send_order:bool,
}

fn scalar(v:&Value,key:&str)->Result<String,CashLineageError>{
    let x=v.get(key).ok_or(CashLineageError::Invalid("field absent"))?;
    if let Some(s)=x.as_str(){return Ok(s.to_string())}
    if let Some(n)=x.as_u64(){return Ok(n.to_string())}
    Err(CashLineageError::Invalid("invalid numeric/text field"))
}
fn number(v:&Value,key:&str)->Result<u64,CashLineageError>{
    scalar(v,key)?.parse::<u64>()
        .map_err(|_|CashLineageError::Invalid("invalid timestamp/ID"))
}
fn decimal_e18(s:&str)->Result<u128,CashLineageError>{
    decimal_to_atoms(s).ok().and_then(|n|n.checked_mul(1_000_000))
        .ok_or(CashLineageError::Invalid("unsupported base-currency precision"))
}
fn signed_e18(s:&str)->Result<i128,CashLineageError>{
    signed_decimal_to_atoms(s).ok().and_then(|n|n.checked_mul(1_000_000))
        .ok_or(CashLineageError::Invalid("unsupported signed amount precision"))
}
fn fetch<E:HttpExecutor>(
    exec:&E,req:&SignedRequest
)->Result<crate::private_https::HttpResponse,CashLineageError>{
    if req.method!=HttpMethod::Get||req.is_mutation{
        return Err(CashLineageError::Invalid("non-read-only account request"))
    }
    exec.execute_request(&HttpsRequest::from_signed(req))
        .map_err(CashLineageError::Transport)
}
fn base_balance<E:HttpExecutor>(
    exec:&E,req:&SignedRequest,base:&str,now_ms:u64,
)->Result<(u128,u64),CashLineageError>{
    let res=fetch(exec,req)?;
    if res.status!=200{return Err(CashLineageError::Invalid("balance HTTP failure"))}
    let body:Value=serde_json::from_str(&res.body)
        .map_err(|_|CashLineageError::Invalid("balance malformed"))?;
    if body.get("code").and_then(Value::as_str)!=Some("0"){
        return Err(CashLineageError::Invalid("balance API error"))
    }
    let data=body.get("data").and_then(Value::as_array)
        .ok_or(CashLineageError::Invalid("balance missing data"))?;
    if data.len()!=1{return Err(CashLineageError::Invalid("balance nonsingleton"))}
    let details=data[0].get("details").and_then(Value::as_array)
        .ok_or(CashLineageError::Invalid("balance no details"))?;
    let filtered=details.iter()
        .filter(|x|x.get("ccy").and_then(Value::as_str)==Some(base))
        .collect::<Vec<_>>();
    if filtered.len()!=1{return Err(CashLineageError::Invalid("base asset missing/duplicated"))}
    let row=filtered[0];
    let cash=decimal_e18(&scalar(row,"cashBal")?)?;
    let updated=number(row,"uTime")?;
    if updated==0||updated>now_ms.saturating_add(FUTURE_TOLERANCE_MS)
        ||now_ms.saturating_sub(updated)>UPDATE_AGE_MAX_MS{
        return Err(CashLineageError::NotFresh)
    }
    Ok((cash,updated))
}

/// The archive query MUST be currency-only, never SPOT-only: non-trade
/// balance changes (funding transfers, rewards, manual activity) must not
/// disappear behind an instType filter. All rows must be accounted for.
#[allow(clippy::too_many_arguments)]
pub fn audit_okx_cash_lineage_once<E:HttpExecutor>(
    exec:&E,store:&NativeStateStore,expected_generation:u64,
    creds:&OkxCredentials,now_ms:u64,domain:OkxDomain,page_limit:u16,
)->Result<CashLineageReport,CashLineageError>{
    let pending=store.pending_native_entry_query(expected_generation)
        .map_err(CashLineageError::State)?;
    if pending.leg!=NativeEntryLeg::Spot{
        return Err(CashLineageError::Invalid("prior spot leg fees not reconstructed"))
    }
    let state=store.load().map_err(CashLineageError::State)?
        .ok_or(CashLineageError::State(NativeStateError::Generation))?;
    if state.generation!=expected_generation{
        return Err(CashLineageError::State(NativeStateError::Generation))
    }
    let entry=state.entry.as_ref().ok_or(CashLineageError::Invalid("no entry"))?;
    let cp=entry.recovery_checkpoint.as_ref()
        .ok_or(CashLineageError::Invalid("no durable fill checkpoint"))?;
    if cp.quality!=NativeEntryRecoveryQuality::SpotCashMatched
        ||cp.intent_id!=pending.intent_id||cp.client_id!=pending.client_id
        ||cp.leg!=NativeEntryLeg::Spot||cp.ledger.fills.is_empty(){
        return Err(CashLineageError::Invalid("checkpoint does not identify this spot order"))
    }
    if now_ms<pending.created_at_ms
        ||now_ms-pending.created_at_ms>LOOKBACK_LIMIT_MS
        ||page_limit==0||page_limit>100{
        return Err(CashLineageError::Invalid("archive window/limit unsupported"))
    }
    let before_req=build_okx_balance_request(creds,now_ms,domain)
        .map_err(CashLineageError::Build)?;
    let (starting,update_before)=base_balance(exec,&before_req,&pending.base,now_ms)?;
    if starting!=cp.observed_spot_cash_atoms_e18
        .ok_or(CashLineageError::Invalid("checkpoint cash absent"))?{
        return Err(CashLineageError::Invalid("balance changed since checkpoint"))
    }
    let mut bills=0usize;
    let mut pages=0usize;
    let mut after:Option<String>=None;
    let mut previous_bill:Option<u128>=None;
    let mut previous_ts:Option<u64>=None;
    let mut rolling=i128::try_from(starting)
        .map_err(|_|CashLineageError::Invalid("balance too large"))?;
    let mut seen=std::collections::HashSet::new();
    let mut delta=0i128;
    let mut fees=0i128;
    let mut ended=false;
    // START with newer bill IDs, reverse each posted balance change,
    // stop ONLY when a complete page sequence ends in explicit empty data.
    for _ in 0..MAX_PAGES{
        let req=build_okx_base_currency_bills_page_request(
            creds,now_ms,domain,&pending.base,pending.created_at_ms,now_ms,
            after.as_deref(),page_limit,
        ).map_err(CashLineageError::Build)?;
        let res=fetch(exec,&req)?;
        pages+=1;
        if res.status!=200{return Err(CashLineageError::Invalid("archive HTTP failure"))}
        let body:Value=serde_json::from_str(&res.body)
            .map_err(|_|CashLineageError::Invalid("archive malformed"))?;
        if body.get("code").and_then(Value::as_str)!=Some("0"){
            return Err(CashLineageError::Invalid("archive API error"))
        }
        let rows=body.get("data").and_then(Value::as_array)
            .ok_or(CashLineageError::Invalid("archive data absent"))?;
        if rows.len()>page_limit as usize{
            return Err(CashLineageError::Invalid("archive page over limit"))
        }
        if rows.is_empty(){ended=true;break}
        for row in rows{
            if scalar(row,"ccy")?!=pending.base{
                return Err(CashLineageError::Invalid("wrong base currency in archive"))
            }
            let id=scalar(row,"billId")?;
            let bill=id.parse::<u128>().ok().filter(|n|*n>0)
                .ok_or(CashLineageError::Invalid("invalid bill ID"))?;
            if previous_bill.map_or(false,|old|bill>=old){
                return Err(CashLineageError::Invalid("nonmonotonic/repeated bill ID"))
            }
            previous_bill=Some(bill);
            after=Some(id);
            let ts=number(row,"ts")?;
            if ts<pending.created_at_ms||ts>now_ms
                ||ts>update_before
                ||previous_ts.map_or(false,|old|ts>old){
                return Err(CashLineageError::Invalid("archive timestamp outside bracket"))
            }
            previous_ts=Some(ts);
            let cash=decimal_e18(&scalar(row,"bal")?)?;
            let cash=i128::try_from(cash)
                .map_err(|_|CashLineageError::Invalid("bill balance overflow"))?;
            let change=signed_e18(&scalar(row,"balChg")?)?;
            if cash!=rolling{
                return Err(CashLineageError::Invalid("bill chain balance discontinuity"))
            }
            rolling=rolling.checked_sub(change)
                .ok_or(CashLineageError::Invalid("running balance overflow"))?;
            let order=scalar(row,"ordId")?;
            if order!=cp.order_id{
                return Err(CashLineageError::Invalid("unrelated base-asset movement"))
            }
            if scalar(row,"instId")?!=pending.spot_symbol.replace('/',"-")
                ||scalar(row,"subType")?!="1"{
                return Err(CashLineageError::Invalid("non-buy/foreign trade bill"))
            }
            let trade_id=scalar(row,"tradeId")?;
            let fill=cp.ledger.fills.get(&trade_id)
                .ok_or(CashLineageError::Invalid("trade id absent in checkpoint"))?;
            if !seen.insert(trade_id){
                return Err(CashLineageError::Invalid("repeated bill trade ID"))
            }
            let fill_time=fill.trade_time_ms
                .ok_or(CashLineageError::Invalid("checkpoint fill time absent"))?;
            if ts<fill_time || ts.saturating_sub(fill_time)>10_000{
                return Err(CashLineageError::Invalid("bill/fill time inconsistent"))
            }
            let qty=decimal_e18(&scalar(row,"sz")?)?;
            if qty!=fill.qty_atoms.checked_mul(1_000_000)
                .ok_or(CashLineageError::Invalid("quantity overflow"))?{
                return Err(CashLineageError::Invalid("bill quantity differs from fill"))
            }
            let fee=signed_e18(&scalar(row,"fee")?)?;
            let expected=i128::try_from(qty).ok()
                .and_then(|n|n.checked_add(fee))
                .ok_or(CashLineageError::Invalid("trade net overflow"))?;
            if expected!=change{
                return Err(CashLineageError::Invalid("bill amount differs from net trade"))
            }
            delta=delta.checked_add(change)
                .ok_or(CashLineageError::Invalid("sum overflow"))?;
            fees=fees.checked_add(fee)
                .ok_or(CashLineageError::Invalid("fee overflow"))?;
            bills+=1;
        }
    }
    if !ended{return Err(CashLineageError::HistoryNotExhausted)}
    if bills!=cp.ledger.fills.len() || rolling
        !=i128::try_from(pending.spot_baseline_atoms_e18)
            .map_err(|_|CashLineageError::Invalid("baseline overflow"))?
        ||fees!=cp.signed_base_fee_atoms_e18
            .ok_or(CashLineageError::Invalid("checkpoint signed fees missing"))?{
        return Err(CashLineageError::Invalid("incomplete original cash lineage"))
    }
    let after_req=build_okx_balance_request(creds,now_ms,domain)
        .map_err(CashLineageError::Build)?;
    let (ending,update_after)=base_balance(exec,&after_req,&pending.base,now_ms)?;
    if ending!=starting||update_after!=update_before{
        return Err(CashLineageError::Invalid("base balance updated during audit"))
    }
    let final_pending=store.pending_native_entry_query(expected_generation)
        .map_err(CashLineageError::State)?;
    if pending!=final_pending{
        return Err(CashLineageError::State(NativeStateError::Generation))
    }
    Ok(CashLineageReport{
        account_bill_count:bills,pages,observed_cash_e18:ending,
        accounted_trade_delta_e18:delta,aggregate_base_fee_e18:fees,
        pre_post_update_ms:update_after,
        base_currency_movement_chain_matched:true,
        cross_venue_snapshot_atomicity_proven:false,
        account_history_global_completeness_proven:false,
        may_send_order:false,
    })
}
