//! R32D6C3 — independent currency-wide OKX cash/bills ancestry BEFORE
//! preparing Futures. Review-only signed GET; no live account used in tests.
//! Complete *queried window* != exchange-global audit or order authorization.
use crate::authoritative_state::{NativeEntryPhase,NativeStateError,NativeStateStore};
use crate::fill_economics::{decimal_to_atoms,signed_decimal_to_atoms,SCALE_12};
use crate::private_https::{HttpExecutor,HttpsRequest,TransportError};
use crate::private_rest::{
    build_okx_balance_request,build_okx_base_currency_bills_page_request,
    HttpMethod,OkxCredentials,OkxDomain,RestBuildError,SignedRequest,
};
use serde::{Deserialize,Serialize};
use serde_json::Value;
use std::collections::{BTreeSet,BTreeMap};

const MAX_PAGES:usize=16;
const MAX_LOOKBACK_MS:u64=70*24*60*60*1000;
const SOURCE_MAX_AGE_MS:u64=180_000;
const CLOCK_ALLOWANCE_MS:u64=5_000;
const BILL_AFTER_FILL_MAX_MS:u64=10_000;
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum SpotBillsError{
    State(NativeStateError),Build(RestBuildError),Transport(TransportError),
    Invalid(&'static str),HistoryNotExhausted,NotFresh,
}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
pub struct NativeSpotBillsEvidence{
    pub source_generation:u64,
    pub spot_first_source_generation:u64,
    pub observed_at_ms:u64,
    pub safety_generation:u64,
    pub spot_intent_id:String,
    pub spot_client_id:String,
    pub spot_order_id:String,
    pub original_base_cash_atoms_e18:u128,
    pub observed_base_cash_atoms_e18:u128,
    pub observed_base_cash_updated_at_ms:u64,
    pub aggregate_base_delta_atoms_e18:i128,
    pub aggregate_signed_base_fee_atoms_e18:i128,
    pub verified_bill_ids:Vec<String>,
    pub billed_trade_ids:Vec<String>,
    pub account_bill_pages:usize,
    pub exact_bills_for_first_leg_matched:bool,
    pub base_cash_ancestry_to_original_baseline_matched:bool,
    pub source_balance_bracket_matched:bool,
    pub account_history_globally_complete:bool,
    pub cross_venue_atomicity_proven:bool,
    pub may_send_order:bool,
}
#[derive(Clone,Debug)]
pub struct SpotBillsReport{
    pub source_generation:u64,
    pub rows:usize,
    pub pages:usize,
    pub observed_cash_e18:u128,
    pub base_delta_e18:i128,
    pub quote_fee_e12:i128,
    pub window_matched:bool,
    pub account_history_globally_complete:bool,
    pub cross_venue_atomicity_proven:bool,
    pub may_send_order:bool,
    pub(crate) sealed:Option<NativeSpotBillsEvidence>,
}
fn text(row:&Value,key:&str)->Result<String,SpotBillsError>{
    let v=row.get(key).ok_or(SpotBillsError::Invalid("missing required OKX bill field"))?;
    if let Some(s)=v.as_str(){return Ok(s.into())}
    if let Some(x)=v.as_u64(){return Ok(x.to_string())}
    Err(SpotBillsError::Invalid("wrong OKX bill field type"))
}
fn number(row:&Value,key:&str)->Result<u64,SpotBillsError>{
    text(row,key)?.parse::<u64>()
        .map_err(|_|SpotBillsError::Invalid("bill timestamp malformed"))
}
fn atoms_e18(s:&str)->Result<u128,SpotBillsError>{
    decimal_to_atoms(s).ok().and_then(|x|x.checked_mul(1_000_000))
        .ok_or(SpotBillsError::Invalid("base unit precision/overflow"))
}
fn signed_e18(s:&str)->Result<i128,SpotBillsError>{
    signed_decimal_to_atoms(s).ok().and_then(|x|x.checked_mul(1_000_000))
        .ok_or(SpotBillsError::Invalid("base delta precision/overflow"))
}
fn signed_e12(s:&str)->Result<i128,SpotBillsError>{
    signed_decimal_to_atoms(s)
        .map_err(|_|SpotBillsError::Invalid("fee precision/overflow"))
}
fn get<E:HttpExecutor>(exec:&E,request:&SignedRequest)->Result<Value,SpotBillsError>{
    if request.method!=HttpMethod::Get||request.is_mutation{
        return Err(SpotBillsError::Invalid("mutation request in cash audit"))
    }
    let res=exec.execute_request(&HttpsRequest::from_signed(request))
        .map_err(SpotBillsError::Transport)?;
    if res.status!=200{return Err(SpotBillsError::Invalid("OKX HTTP failure"))}
    let v:Value=serde_json::from_str(&res.body)
        .map_err(|_|SpotBillsError::Invalid("OKX JSON malformed"))?;
    if v.get("code").and_then(Value::as_str)!=Some("0"){
        return Err(SpotBillsError::Invalid("OKX business code not OK"))
    }
    Ok(v)
}
fn spot_balance<E:HttpExecutor>(exec:&E,req:&SignedRequest,base:&str,now_ms:u64)
    ->Result<(u128,u64),SpotBillsError>{
    let v=get(exec,req)?;
    let data=v.get("data").and_then(Value::as_array)
        .ok_or(SpotBillsError::Invalid("balance missing rows"))?;
    if data.len()!=1{return Err(SpotBillsError::Invalid("ambiguous account balances"))}
    let details=data[0].get("details").and_then(Value::as_array)
        .ok_or(SpotBillsError::Invalid("balance details absent"))?;
    let selected=details.iter().filter(|row|
        row.get("ccy").and_then(Value::as_str)==Some(base)).collect::<Vec<_>>();
    if selected.len()!=1{return Err(SpotBillsError::Invalid("base balance absent/ambiguous"))}
    let cash=atoms_e18(&text(selected[0],"cashBal")?)?;
    let updated=number(selected[0],"uTime")?;
    if updated==0||updated>now_ms.saturating_add(CLOCK_ALLOWANCE_MS)
        ||now_ms.saturating_sub(updated)>SOURCE_MAX_AGE_MS{
        return Err(SpotBillsError::NotFresh)
    }
    Ok((cash,updated))
}
/// All base-currency account bills, NOT spot-filtered trade bills. Any
/// unrelated movement rejects; detecting zero-net unrelated movements is
/// the essential reason NOT to filter by spot instrument/order.
pub fn audit_okx_first_leg_cash_bills_window<E:HttpExecutor>(
    exec:&E,store:&NativeStateStore,expected_generation:u64,
    creds:&OkxCredentials,now_ms:u64,domain:OkxDomain,page_limit:u16,
)->Result<SpotBillsReport,SpotBillsError>{
    let state=store.load().map_err(SpotBillsError::State)?
        .ok_or(SpotBillsError::State(NativeStateError::Generation))?;
    if state.generation!=expected_generation{
        return Err(SpotBillsError::State(NativeStateError::Generation))
    }
    let entry=state.entry.as_ref().ok_or(SpotBillsError::Invalid("native entry absent"))?;
    if state.active.is_some()||entry.phase!=NativeEntryPhase::SpotExposed
        ||entry.spot_prehedge_bills_evidence.is_some()
        ||entry.economics.entry.futures.orders.len()!=0
        ||now_ms<entry.plan.created_at_ms
        ||now_ms-entry.plan.created_at_ms>MAX_LOOKBACK_MS
        ||page_limit==0||page_limit>100{
        return Err(SpotBillsError::Invalid("spot account window not eligible"))
    }
    entry.require_spot_prehedge_provenance().map_err(SpotBillsError::State)?;
    let first=entry.spot_prehedge_evidence.as_ref()
        .ok_or(SpotBillsError::Invalid("settled spot fills were not persisted"))?;
    let ledger=&first.exact_ledger;
    if ledger.fills.len()!=first.fill_bill_ids.len()
        ||ledger.fills.is_empty()||!ledger.fee_evidence_complete{
        return Err(SpotBillsError::Invalid("fill evidence missing original bill IDs"))
    }
    let request=build_okx_balance_request(creds,now_ms,domain)
        .map_err(SpotBillsError::Build)?;
    let (before_cash,before_updated)=spot_balance(exec,&request,&entry.plan.base,now_ms)?;
    if before_cash!=first.observed_base_cash_atoms_e18
        ||before_updated!=first.balance_updated_at_ms{
        return Err(SpotBillsError::Invalid("spot cash changed since fill capture"))
    }
    // A single entry leg: the only acceptable cash movements between the
    // durable original baseline and the observed cash are this order's fills.
    let mut previous_bill:Option<u128>=None;
    let mut previous_ts:Option<u64>=None;
    let mut bill_cursor:Option<String>=None;
    let mut rolling=i128::try_from(before_cash)
        .map_err(|_|SpotBillsError::Invalid("cash too large"))?;
    let mut fee_base_total=0i128;
    let mut delta_total=0i128;
    let mut quote_total=0i128;
    let mut seen_ids=BTreeSet::new();
    let mut ids=BTreeSet::new();
    let mut trades=BTreeMap::<String,String>::new();
    let mut pages=0usize;
    let mut exhausted=false;
    for _ in 0..MAX_PAGES{
        let req=build_okx_base_currency_bills_page_request(
            creds,now_ms,domain,&entry.plan.base,
            entry.plan.created_at_ms,now_ms,bill_cursor.as_deref(),page_limit,
        ).map_err(SpotBillsError::Build)?;
        let data=get(exec,&req)?;
        let rows=data.get("data").and_then(Value::as_array)
            .ok_or(SpotBillsError::Invalid("bills array missing"))?;
        pages+=1;
        if rows.len()>page_limit as usize{
            return Err(SpotBillsError::Invalid("bill page exceeds limit"))
        }
        if rows.is_empty(){exhausted=true;break}
        for row in rows{
            let bill_id=text(row,"billId")?;
            let numeric=bill_id.parse::<u128>().ok().filter(|v|*v>0)
                .ok_or(SpotBillsError::Invalid("invalid billId"))?;
            if previous_bill.map_or(false,|old|numeric>=old)
                ||!ids.insert(bill_id.clone()){
                return Err(SpotBillsError::Invalid("bill cursor repeated/out of order"))
            }
            previous_bill=Some(numeric);
            bill_cursor=Some(bill_id.clone());
            let ts=number(row,"ts")?;
            if ts<entry.plan.created_at_ms||ts>now_ms||ts>before_updated
                ||previous_ts.map_or(false,|prior|ts>prior){
                return Err(SpotBillsError::Invalid("nonmonotonic bill timestamp"))
            }
            previous_ts=Some(ts);
            if text(row,"ccy")?!=entry.plan.base
                ||text(row,"ordId")?!=first.spot_order_id
                ||text(row,"instId")?!=entry.plan.spot_symbol.replace('/',"-")
                ||text(row,"subType")?!="1"{
                return Err(SpotBillsError::Invalid("unexpected base asset bill/transfer"))
            }
            let trade_id=text(row,"tradeId")?;
            let fill=ledger.fills.get(&trade_id)
                .ok_or(SpotBillsError::Invalid("bill trade not in first-leg ledger"))?;
            let fill_time=fill.trade_time_ms
                .ok_or(SpotBillsError::Invalid("missing recorded fill time"))?;
            if ts<fill_time||ts.saturating_sub(fill_time)>BILL_AFTER_FILL_MAX_MS{
                return Err(SpotBillsError::Invalid("bill time mismatches fill"))
            }
            if !seen_ids.insert(trade_id.clone()){
                return Err(SpotBillsError::Invalid("duplicate bill tradeId"))
            }
            trades.insert(trade_id,bill_id);
            let qty=atoms_e18(&text(row,"sz")?)?;
            if qty!=fill.qty_atoms.checked_mul(1_000_000)
                .ok_or(SpotBillsError::Invalid("qty scale overflow"))?{
                return Err(SpotBillsError::Invalid("bill quantity differs from trade"))
            }
            let signed_fee=signed_e12(&text(row,"fee")?)?;
            let fee_asset=text(row,"feeCcy")?;
            if fill.fee_currency.as_deref()!=Some(fee_asset.as_str()){
                return Err(SpotBillsError::Invalid("fee currency differs from fill"))
            }
            let base_fee=if fee_asset==entry.plan.base{
                signed_fee.checked_mul(1_000_000)
                    .ok_or(SpotBillsError::Invalid("base fee overflow"))?
            }else if fee_asset=="USDT"{0}else{
                return Err(SpotBillsError::Invalid("unexpected fee asset"))
            };
            let normalized_quote=if fee_asset=="USDT"{
                signed_fee.checked_neg().ok_or(SpotBillsError::Invalid("fee sign overflow"))?
            }else{
                signed_fee.checked_neg()
                    .and_then(|fee|fee.checked_mul(i128::try_from(fill.price_atoms).ok()?))
                    .and_then(|n|n.checked_div(SCALE_12 as i128))
                    .ok_or(SpotBillsError::Invalid("base fee quote normalization overflow"))?
            };
            if fill.fee_cost_quote_atoms!=Some(normalized_quote){
                return Err(SpotBillsError::Invalid("per-fill signed fee differs"))
            }
            let delta=signed_e18(&text(row,"balChg")?)?;
            let expected_delta=i128::try_from(qty).ok()
                .and_then(|q|q.checked_add(base_fee))
                .ok_or(SpotBillsError::Invalid("signed purchase delta overflow"))?;
            if delta!=expected_delta{
                return Err(SpotBillsError::Invalid("account movement delta differs"))
            }
            let bill_cash=i128::try_from(atoms_e18(&text(row,"bal")?)?)
                .map_err(|_|SpotBillsError::Invalid("bill balance too large"))?;
            if bill_cash!=rolling{
                return Err(SpotBillsError::Invalid("bill running balance discontinuity"))
            }
            rolling=rolling.checked_sub(delta)
                .ok_or(SpotBillsError::Invalid("bill reverse cash underflow"))?;
            delta_total=delta_total.checked_add(delta)
                .ok_or(SpotBillsError::Invalid("total delta overflow"))?;
            fee_base_total=fee_base_total.checked_add(base_fee)
                .ok_or(SpotBillsError::Invalid("base fee sum overflow"))?;
            quote_total=quote_total.checked_add(normalized_quote)
                .ok_or(SpotBillsError::Invalid("quote fee sum overflow"))?;
        }
    }
    if !exhausted{return Err(SpotBillsError::HistoryNotExhausted)}
    if ids.len()!=first.fill_bill_ids.len()
        ||ids.iter().cloned().collect::<Vec<_>>()!=first.fill_bill_ids
        ||trades.len()!=ledger.fills.len()
        ||trades.keys().cloned().collect::<Vec<_>>()
            !=ledger.fills.keys().cloned().collect::<Vec<_>>()
        ||rolling!=i128::try_from(entry.plan.spot_baseline_atoms_e18)
            .map_err(|_|SpotBillsError::Invalid("cash baseline too large"))?
        ||fee_base_total!=first.signed_base_fee_atoms_e18
        ||quote_total!=ledger.fee_cost_quote_atoms{
        return Err(SpotBillsError::Invalid("currency-wide history not equal original spot ancestry"))
    }
    let (after_cash,after_updated)=spot_balance(exec,&request,&entry.plan.base,now_ms)?;
    if after_cash!=before_cash||after_updated!=before_updated{
        return Err(SpotBillsError::Invalid("spot cash changed while reading full bill history"))
    }
    let latest=store.load().map_err(SpotBillsError::State)?
        .ok_or(SpotBillsError::State(NativeStateError::Generation))?;
    if latest!=state{
        return Err(SpotBillsError::State(NativeStateError::Generation))
    }
    let sealed=NativeSpotBillsEvidence{
        source_generation:expected_generation,
        spot_first_source_generation:first.source_generation,
        observed_at_ms:now_ms,safety_generation:entry.plan.safety_generation,
        spot_intent_id:first.spot_intent_id.clone(),
        spot_client_id:first.spot_client_id.clone(),
        spot_order_id:first.spot_order_id.clone(),
        original_base_cash_atoms_e18:entry.plan.spot_baseline_atoms_e18,
        observed_base_cash_atoms_e18:after_cash,
        observed_base_cash_updated_at_ms:after_updated,
        aggregate_base_delta_atoms_e18:delta_total,
        aggregate_signed_base_fee_atoms_e18:fee_base_total,
        verified_bill_ids:ids.into_iter().collect(),
        billed_trade_ids:trades.keys().cloned().collect(),
        account_bill_pages:pages,
        exact_bills_for_first_leg_matched:true,
        base_cash_ancestry_to_original_baseline_matched:true,
        source_balance_bracket_matched:true,
        account_history_globally_complete:false,
        cross_venue_atomicity_proven:false,may_send_order:false,
    };
    Ok(SpotBillsReport{
        source_generation:expected_generation,rows:sealed.billed_trade_ids.len(),
        pages,observed_cash_e18:after_cash,base_delta_e18:delta_total,
        quote_fee_e12:quote_total,window_matched:true,
        account_history_globally_complete:false,
        cross_venue_atomicity_proven:false,may_send_order:false,
        sealed:Some(sealed),
    })
}
