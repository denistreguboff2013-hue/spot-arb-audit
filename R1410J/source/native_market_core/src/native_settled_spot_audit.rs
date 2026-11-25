//! R32D6B2A — independent historical OKX fill+bill+cash audit of a
//! previously ACKed Spot leg WHILE the Futures entry is unresolved.
//! Only fake-credential, mocked signed GET tests. This audit NEVER releases
//! RecoveryRequired, never places an order, never proves global completeness.
use crate::authoritative_state::{NativeEntryLeg,NativeStateError,NativeStateStore};
use crate::fill_economics::{decimal_to_atoms,signed_decimal_to_atoms,LegFillLedger};
use crate::private_https::{HttpExecutor,HttpsRequest,TransportError};
use crate::private_rest::{
    build_okx_fills_history_page_request,build_okx_base_currency_bills_page_request,
    build_okx_balance_request,parse_okx_fill_events_http,
    HttpMethod,OkxCredentials,OkxDomain,RestBuildError,SignedRequest,
};
use serde::{Deserialize,Serialize};
use serde_json::Value;
use std::collections::{BTreeMap,BTreeSet};

const MAX_PAGES:usize=16;
const MAX_LOOKBACK:u64=70*24*60*60*1000;
const MAX_ASSET_UPDATE_AGE:u64=180_000;
const FUTURE_CLOCK_ALLOWANCE:u64=5_000;
const FILL_EARLY_ALLOWANCE:u64=5*60*1000;
const BILL_LAG:u64=10_000;

#[derive(Clone,Debug,PartialEq,Eq)]
pub enum SettledSpotAuditError{
    State(NativeStateError),Build(RestBuildError),Transport(TransportError),
    Invalid(&'static str),HistoryNotExhausted,NotFresh,
}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
pub struct NativeSpotWindowEvidence{
    pub source_generation:u64,
    pub original_dual_capture_generation:u64,
    pub spot_intent_id:String,
    pub spot_client_id:String,
    pub spot_order_id:String,
    pub spot_ledger:LegFillLedger,
    pub observed_cash_atoms_e18:u128,
    pub observed_cash_updated_at_ms:u64,
    pub gross_fill_atoms_e18:u128,
    pub signed_base_fee_atoms_e18:i128,
    pub checked_bill_ids:Vec<String>,
    pub fills_history_pages:usize,
    pub cash_bill_pages:usize,
    pub all_order_fills_and_bills_in_window_matched:bool,
    pub exchange_global_completeness_proven:bool,
    pub cross_venue_atomicity_proven:bool,
    pub may_send_order:bool,
}
#[derive(Clone,Debug)]
pub struct SettledSpotAuditReport{
    pub fill_rows:usize,
    pub bill_rows:usize,
    pub fills_pages:usize,
    pub bills_pages:usize,
    pub cash_e18:u128,
    pub candidate_window_matched:bool,
    pub exchange_global_completeness_proven:bool,
    pub cross_venue_atomicity_proven:bool,
    pub may_send_order:bool,
    pub(crate) sealed:Option<NativeSpotWindowEvidence>,
}
fn value(row:&Value,k:&str)->Result<String,SettledSpotAuditError>{
    let v=row.get(k).ok_or(SettledSpotAuditError::Invalid("required field absent"))?;
    if let Some(t)=v.as_str(){return Ok(t.to_string())}
    if let Some(t)=v.as_u64(){return Ok(t.to_string())}
    Err(SettledSpotAuditError::Invalid("wrong type"))
}
fn number(row:&Value,k:&str)->Result<u64,SettledSpotAuditError>{
    value(row,k)?.parse::<u64>()
        .map_err(|_|SettledSpotAuditError::Invalid("invalid number"))
}
fn e18(s:&str)->Result<u128,SettledSpotAuditError>{
    decimal_to_atoms(s).ok().and_then(|n|n.checked_mul(1_000_000))
        .ok_or(SettledSpotAuditError::Invalid("quantity precision/overflow"))
}
fn signed_e18(s:&str)->Result<i128,SettledSpotAuditError>{
    signed_decimal_to_atoms(s).ok().and_then(|n|n.checked_mul(1_000_000))
        .ok_or(SettledSpotAuditError::Invalid("signed amount precision/overflow"))
}
fn signed_e12(s:&str)->Result<i128,SettledSpotAuditError>{
    signed_decimal_to_atoms(s)
        .map_err(|_|SettledSpotAuditError::Invalid("signed fee precision"))
}
fn fetch<E:HttpExecutor>(exec:&E,req:&SignedRequest)
    ->Result<crate::private_https::HttpResponse,SettledSpotAuditError>{
    if req.method!=HttpMethod::Get||req.is_mutation{
        return Err(SettledSpotAuditError::Invalid("mutation-shaped GET"))
    }
    exec.execute_request(&HttpsRequest::from_signed(req))
        .map_err(SettledSpotAuditError::Transport)
}
fn get_okx_data<E:HttpExecutor>(exec:&E,req:&SignedRequest)
    ->Result<(u16,String,Vec<Value>),SettledSpotAuditError>{
    let res=fetch(exec,req)?;
    if res.status!=200{return Err(SettledSpotAuditError::Invalid("OKX HTTP denied"))}
    let json:Value=serde_json::from_str(&res.body)
        .map_err(|_|SettledSpotAuditError::Invalid("OKX invalid JSON"))?;
    if json.get("code").and_then(Value::as_str)!=Some("0"){
        return Err(SettledSpotAuditError::Invalid("OKX business code nonzero"))
    }
    let arr=json.get("data").and_then(Value::as_array)
        .ok_or(SettledSpotAuditError::Invalid("OKX data missing"))?;
    Ok((res.status,res.body,arr.clone()))
}
fn spot_cash<E:HttpExecutor>(
    exec:&E,req:&SignedRequest,base:&str,now:u64,
)->Result<(u128,u64),SettledSpotAuditError>{
    let (_,_,entries)=get_okx_data(exec,req)?;
    if entries.len()!=1{return Err(SettledSpotAuditError::Invalid("cash account rows"))}
    let details=entries[0].get("details").and_then(Value::as_array)
        .ok_or(SettledSpotAuditError::Invalid("cash details missing"))?;
    let base_rows=details.iter().filter(|x|
        x.get("ccy").and_then(Value::as_str)==Some(base)).collect::<Vec<_>>();
    if base_rows.len()!=1{
        return Err(SettledSpotAuditError::Invalid("cash currency ambiguous"))
    }
    let row=base_rows[0];
    let amount=e18(&value(row,"cashBal")?)?;
    let updated=number(row,"uTime")?;
    if updated==0||updated>now.saturating_add(FUTURE_CLOCK_ALLOWANCE)
        ||now.saturating_sub(updated)>MAX_ASSET_UPDATE_AGE{
        return Err(SettledSpotAuditError::NotFresh)
    }
    Ok((amount,updated))
}
struct ExactBillMatch{
    trade_id:String,
    qty_e18:u128,
    fill_time_ms:u64,
    signed_fee_atoms_e12:i128,
    fee_asset:String,
    base_fee_atoms_e18:i128,
}
/// This is intentionally separate from R32D5A (which only probes an
/// unresolved Spot leg). Here we require a FULL settled Spot ledger and an
/// immutable native dual-leg snapshot BEFORE querying OKX at all.
#[allow(clippy::too_many_arguments)]
pub fn audit_prior_settled_spot_window_once<E:HttpExecutor>(
    exec:&E,store:&NativeStateStore,expected_generation:u64,
    creds:&OkxCredentials,now_ms:u64,domain:OkxDomain,
    page_limit:u16,
)->Result<SettledSpotAuditReport,SettledSpotAuditError>{
    let pending=store.pending_native_entry_query(expected_generation)
        .map_err(SettledSpotAuditError::State)?;
    if pending.leg!=NativeEntryLeg::Futures{
        return Err(SettledSpotAuditError::Invalid("expected unresolved futures"))
    }
    let state=store.load().map_err(SettledSpotAuditError::State)?
        .ok_or(SettledSpotAuditError::State(NativeStateError::Generation))?;
    if state.generation!=expected_generation{
        return Err(SettledSpotAuditError::State(NativeStateError::Generation))
    }
    let entry=state.entry.as_ref().ok_or(SettledSpotAuditError::Invalid("native entry missing"))?;
    let dual=entry.dual_leg_evidence.as_ref()
        .ok_or(SettledSpotAuditError::Invalid("native dual proof absent"))?;
    if dual.pending_futures_checkpoint.intent_id!=pending.intent_id
        ||dual.pending_futures_checkpoint.client_id!=pending.client_id
        ||dual.settled_spot_ledger.fills.is_empty()
        ||!dual.settled_spot_ledger.fee_evidence_complete
        ||!dual.settled_spot_ledger.terminal{
        return Err(SettledSpotAuditError::Invalid("dual identity or spot fees invalid"))
    }
    if now_ms<pending.created_at_ms
        ||now_ms-pending.created_at_ms>MAX_LOOKBACK
        ||page_limit==0||page_limit>100{
        return Err(SettledSpotAuditError::Invalid("window age/size invalid"))
    }
    let base=&pending.base;
    let spot_order=&dual.settled_spot_order_id;
    let balance_req=build_okx_balance_request(creds,now_ms,domain)
        .map_err(SettledSpotAuditError::Build)?;
    let (balance,updated)=spot_cash(exec,&balance_req,base,now_ms)?;
    let mut history_cursor:Option<String>=None;
    let mut fills_pages=0usize;
    let mut expected: BTreeMap<String,ExactBillMatch>=BTreeMap::new();
    let mut seen_trade_ids=BTreeSet::new();
    let mut last_bill_id:Option<u128>=None;
    let mut complete_fills=false;
    let mut total_base_fee=0i128;
    for _ in 0..MAX_PAGES{
        let req=build_okx_fills_history_page_request(
            creds,now_ms,domain,&pending.spot_symbol,spot_order,
            history_cursor.as_deref(),page_limit,
        ).map_err(SettledSpotAuditError::Build)?;
        let (status,body,rows)=get_okx_data(exec,&req)?;
        fills_pages+=1;
        if rows.len()>page_limit as usize{
            return Err(SettledSpotAuditError::Invalid("fill page over limit"))
        }
        if rows.is_empty(){complete_fills=true;break}
        let fills=parse_okx_fill_events_http(status,&body,spot_order,base,"USDT")
            .map_err(|_|SettledSpotAuditError::Invalid("fill parse failed"))?;
        if fills.len()!=rows.len(){
            return Err(SettledSpotAuditError::Invalid("foreign order returned in fills"))
        }
        for (row,fill) in rows.iter().zip(fills.into_iter()){
            if value(row,"ordId")?!=*spot_order{
                return Err(SettledSpotAuditError::Invalid("fill order changed"))
            }
            let raw_bill=value(row,"billId")?;
            let bill_num=raw_bill.parse::<u128>().ok().filter(|n|*n>0)
                .ok_or(SettledSpotAuditError::Invalid("fill bill ID invalid"))?;
            if last_bill_id.map_or(false,|last|bill_num>=last){
                return Err(SettledSpotAuditError::Invalid("fill cursor repeated/ascending"))
            }
            last_bill_id=Some(bill_num);
            history_cursor=Some(raw_bill.clone());
            let recorded=dual.settled_spot_ledger.fills.get(&fill.fill_id)
                .ok_or(SettledSpotAuditError::Invalid("OKX fill not in settled native ledger"))?;
            if recorded!=&fill||!seen_trade_ids.insert(fill.fill_id.clone()){
                return Err(SettledSpotAuditError::Invalid("fill mismatch or duplicate"))
            }
            let fill_time=fill.trade_time_ms
                .ok_or(SettledSpotAuditError::Invalid("fill time missing"))?;
            if fill_time<pending.created_at_ms.saturating_sub(FILL_EARLY_ALLOWANCE)
                ||fill_time>now_ms{
                return Err(SettledSpotAuditError::Invalid("fill outside window"))
            }
            let fee_asset=value(row,"feeCcy")?;
            let signed_fee=signed_e12(&value(row,"fee")?)?;
            let base_fee=if fee_asset==*base{
                signed_fee.checked_mul(1_000_000)
                    .ok_or(SettledSpotAuditError::Invalid("base fee overflow"))?
            }else if fee_asset=="USDT"{0}else{
                return Err(SettledSpotAuditError::Invalid("unsupported fill fee asset"))
            };
            total_base_fee=total_base_fee.checked_add(base_fee)
                .ok_or(SettledSpotAuditError::Invalid("sum base fees overflow"))?;
            let match_row=ExactBillMatch{
                trade_id:fill.fill_id,
                qty_e18:fill.qty_atoms.checked_mul(1_000_000)
                    .ok_or(SettledSpotAuditError::Invalid("fill qty scale overflow"))?,
                fill_time_ms:fill_time,signed_fee_atoms_e12:signed_fee,
                fee_asset,base_fee_atoms_e18:base_fee,
            };
            if expected.insert(raw_bill,match_row).is_some(){
                return Err(SettledSpotAuditError::Invalid("duplicate fill bill ID"))
            }
        }
    }
    if !complete_fills{return Err(SettledSpotAuditError::HistoryNotExhausted)}
    if seen_trade_ids.len()!=dual.settled_spot_ledger.fills.len()
        ||seen_trade_ids.is_empty(){
        return Err(SettledSpotAuditError::Invalid("incomplete settled spot fill history"))
    }
    let gross=dual.settled_spot_ledger.filled_qty_atoms()
        .checked_mul(1_000_000).ok_or(SettledSpotAuditError::Invalid("gross scale overflow"))?;
    let expected_cash=i128::try_from(pending.spot_baseline_atoms_e18).ok()
        .and_then(|v|v.checked_add(i128::try_from(gross).ok()?))
        .and_then(|v|v.checked_add(total_base_fee))
        .ok_or(SettledSpotAuditError::Invalid("cash sum overflow"))?;
    if i128::try_from(balance).ok()!=Some(expected_cash){
        return Err(SettledSpotAuditError::Invalid("spot net cash mismatch"))
    }
    let mut bill_cursor:Option<String>=None;
    let mut last_bill:Option<u128>=None;
    let mut last_ts:Option<u64>=None;
    let mut seen_bills=BTreeSet::new();
    let mut bill_pages=0usize;
    let mut complete_bills=false;
    let mut running=expected_cash;
    for _ in 0..MAX_PAGES{
        let req=build_okx_base_currency_bills_page_request(
            creds,now_ms,domain,base,pending.created_at_ms,now_ms,
            bill_cursor.as_deref(),page_limit,
        ).map_err(SettledSpotAuditError::Build)?;
        let (_,_,rows)=get_okx_data(exec,&req)?;
        bill_pages+=1;
        if rows.len()>page_limit as usize{
            return Err(SettledSpotAuditError::Invalid("bill page over limit"))
        }
        if rows.is_empty(){complete_bills=true;break}
        for row in rows{
            if value(&row,"ccy")?!=*base
                ||value(&row,"ordId")?!=*spot_order
                ||value(&row,"instId")?!=pending.spot_symbol.replace('/',"-")
                ||value(&row,"subType")?!="1"{
                return Err(SettledSpotAuditError::Invalid("unrelated cash bill"))
            }
            let raw_bill=value(&row,"billId")?;
            let bill=raw_bill.parse::<u128>().ok().filter(|n|*n>0)
                .ok_or(SettledSpotAuditError::Invalid("bill cursor invalid"))?;
            if last_bill.map_or(false,|last|bill>=last){
                return Err(SettledSpotAuditError::Invalid("bill cursor repeat/order"))
            }
            last_bill=Some(bill);
            bill_cursor=Some(raw_bill.clone());
            if !seen_bills.insert(raw_bill.clone()){
                return Err(SettledSpotAuditError::Invalid("duplicate bill ID"))
            }
            let matching=expected.get(&raw_bill)
                .ok_or(SettledSpotAuditError::Invalid("bill lacks corresponding fill"))?;
            if value(&row,"tradeId")?!=matching.trade_id{
                return Err(SettledSpotAuditError::Invalid("bill trade ID changed"))
            }
            let t=number(&row,"ts")?;
            if t<pending.created_at_ms||t>updated||t>now_ms
                ||last_ts.map_or(false,|last|t>last)
                ||t<matching.fill_time_ms
                ||t.saturating_sub(matching.fill_time_ms)>BILL_LAG{
                return Err(SettledSpotAuditError::Invalid("bill timing incompatible"))
            }
            last_ts=Some(t);
            let qty=e18(&value(&row,"sz")?)?;
            let fee_asset=value(&row,"feeCcy")?;
            let fee=signed_e12(&value(&row,"fee")?)?;
            if qty!=matching.qty_e18
                ||fee_asset!=matching.fee_asset
                ||fee!=matching.signed_fee_atoms_e12{
                return Err(SettledSpotAuditError::Invalid("bill qty/fee conflict"))
            }
            let close=e18(&value(&row,"bal")?)?;
            if i128::try_from(close).ok()!=Some(running){
                return Err(SettledSpotAuditError::Invalid("bill cash chain broken"))
            }
            let delta=signed_e18(&value(&row,"balChg")?)?;
            let net=i128::try_from(qty).ok()
                .and_then(|n|n.checked_add(matching.base_fee_atoms_e18))
                .ok_or(SettledSpotAuditError::Invalid("bill net overflow"))?;
            if delta!=net{
                return Err(SettledSpotAuditError::Invalid("bill delta != bought base + fee"))
            }
            running=running.checked_sub(delta)
                .ok_or(SettledSpotAuditError::Invalid("reverse cash overflow"))?;
        }
    }
    if !complete_bills{return Err(SettledSpotAuditError::HistoryNotExhausted)}
    if seen_bills.len()!=expected.len()
        ||running!=i128::try_from(pending.spot_baseline_atoms_e18)
            .map_err(|_|SettledSpotAuditError::Invalid("baseline conversion overflow"))?{
        return Err(SettledSpotAuditError::Invalid("incomplete cash bill ancestry"))
    }
    let (closing,updated_after)=spot_cash(exec,&balance_req,base,now_ms)?;
    if closing!=balance||updated_after!=updated{
        return Err(SettledSpotAuditError::Invalid("cash drift during histories"))
    }
    let current=store.pending_native_entry_query(expected_generation)
        .map_err(SettledSpotAuditError::State)?;
    if current!=pending{return Err(SettledSpotAuditError::State(NativeStateError::Generation))}
    let sealed=NativeSpotWindowEvidence{
        source_generation:expected_generation,
        original_dual_capture_generation:dual.native_generation_at_capture,
        spot_intent_id:dual.settled_spot_intent_id.clone(),
        spot_client_id:dual.settled_spot_client_id.clone(),
        spot_order_id:spot_order.clone(),spot_ledger:dual.settled_spot_ledger.clone(),
        observed_cash_atoms_e18:closing,
        observed_cash_updated_at_ms:updated_after,
        gross_fill_atoms_e18:gross,
        signed_base_fee_atoms_e18:total_base_fee,
        checked_bill_ids:seen_bills.into_iter().collect(),
        fills_history_pages:fills_pages,cash_bill_pages:bill_pages,
        all_order_fills_and_bills_in_window_matched:true,
        exchange_global_completeness_proven:false,
        cross_venue_atomicity_proven:false,may_send_order:false,
    };
    // R32D6B2B recheck: if a prior window was durably recorded, every
    // exchange-observed field must remain identical. The only intentional
    // difference is native source generation of this new read-only scan.
    if let Some(previous)=&entry.settled_spot_window_evidence{
        let mut comparable=sealed.clone();
        comparable.source_generation=previous.source_generation;
        if &comparable!=previous{
            return Err(SettledSpotAuditError::Invalid("prior OKX audit evidence changed"))
        }
    }
    Ok(SettledSpotAuditReport{
        fill_rows:seen_trade_ids.len(),bill_rows:sealed.checked_bill_ids.len(),
        fills_pages,bills_pages:bill_pages,cash_e18:closing,
        candidate_window_matched:true,exchange_global_completeness_proven:false,
        cross_venue_atomicity_proven:false,may_send_order:false,
        sealed:Some(sealed),
    })
}
