//! R32D6C2 — prospectively capture full settled OKX first-leg per-fill
//! history BEFORE the native futures leg can be prepared.
//! Read-only signed GET, fixed-point math, no real/private network in tests.
//! Receipt is not a trading permit and does not prove exchange-global history.
use serde::{Deserialize,Serialize};
use serde_json::Value;
use std::collections::{BTreeSet,BTreeMap};
use crate::authoritative_state::{NativeStateStore,NativeEntryPhase,NativeStateError};
use crate::execution::IntentState;
use crate::fill_economics::{
    LegFillLedger,decimal_to_atoms,signed_decimal_to_atoms,DEFAULT_AVG_TOLERANCE_ATOMS,
};
use crate::private_https::{HttpExecutor,HttpsRequest,TransportError};
use crate::private_rest::{
    OkxCredentials,OkxDomain,RestBuildError,SignedRequest,HttpMethod,
    EconomicTruthClass,build_okx_query_by_client_id,
    build_okx_fills_history_page_request,build_okx_balance_request,
    parse_okx_economic_truth_http,parse_okx_fill_events_http,
    apply_order_economic_truth,
};
const MAX_PAGES:usize=16;
const SOURCE_FRESH_MS:u64=180_000;
const CLOCK_ALLOWANCE_MS:u64=5_000;
const MAX_PLAN_AGE_MS:u64=70*24*60*60*1000;
const EARLY_ALLOW_MS:u64=5*60*1000;

#[derive(Clone,Debug,PartialEq,Eq)]
pub enum SpotPreHedgeError{
    State(NativeStateError),Build(RestBuildError),Transport(TransportError),
    Invalid(&'static str),UnexhaustedHistory,Stale,
}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
pub struct NativeSpotPreHedgeEvidence{
    pub source_generation:u64,
    pub observed_at_ms:u64,
    pub safety_generation:u64,
    pub spot_intent_id:String,
    pub spot_client_id:String,
    pub spot_order_id:String,
    pub exact_ledger:LegFillLedger,
    pub signed_base_fee_atoms_e18:i128,
    pub observed_base_cash_atoms_e18:u128,
    pub balance_updated_at_ms:u64,
    pub fill_bill_ids:Vec<String>,
    pub filled_pages:usize,
    pub cash_bracket_matched:bool,
    pub exchange_bill_history_proven:bool,
    pub exchange_global_completeness_proven:bool,
    pub may_send_order:bool,
}
#[derive(Clone,Debug)]
pub struct SpotPreHedgeReport{
    pub source_generation:u64,
    pub fills:usize,
    pub pages:usize,
    pub fee_quote_atoms_e12:i128,
    pub cash_atoms_e18:u128,
    pub is_complete_readonly_candidate:bool,
    pub may_send_order:bool,
    pub(crate) sealed:Option<NativeSpotPreHedgeEvidence>,
}
fn text(row:&Value,key:&str)->Result<String,SpotPreHedgeError>{
    let v=row.get(key).ok_or(SpotPreHedgeError::Invalid("missing required field"))?;
    if let Some(s)=v.as_str(){return Ok(s.into())}
    if let Some(n)=v.as_u64(){return Ok(n.to_string())}
    Err(SpotPreHedgeError::Invalid("field wrong type"))
}
fn integer(row:&Value,key:&str)->Result<u64,SpotPreHedgeError>{
    text(row,key)?.parse::<u64>().map_err(|_|SpotPreHedgeError::Invalid("invalid timestamp"))
}
fn base_atoms(s:&str)->Result<u128,SpotPreHedgeError>{
    decimal_to_atoms(s).ok().and_then(|x|x.checked_mul(1_000_000))
        .ok_or(SpotPreHedgeError::Invalid("base cash precision/overflow"))
}
fn base_signed(s:&str)->Result<i128,SpotPreHedgeError>{
    signed_decimal_to_atoms(s).ok().and_then(|x|x.checked_mul(1_000_000))
        .ok_or(SpotPreHedgeError::Invalid("base fee precision/overflow"))
}
fn get<E:HttpExecutor>(exec:&E,request:&SignedRequest)
    ->Result<(u16,String),SpotPreHedgeError>{
    if request.method!=HttpMethod::Get||request.is_mutation{
        return Err(SpotPreHedgeError::Invalid("only signed GET allowed"))
    }
    let response=exec.execute_request(&HttpsRequest::from_signed(request))
        .map_err(SpotPreHedgeError::Transport)?;
    if response.status!=200{return Err(SpotPreHedgeError::Invalid("HTTP response not OK"))}
    let parsed:Value=serde_json::from_str(&response.body)
        .map_err(|_|SpotPreHedgeError::Invalid("invalid JSON"))?;
    if parsed.get("code").and_then(Value::as_str)!=Some("0"){
        return Err(SpotPreHedgeError::Invalid("OKX business code not OK"))
    }
    Ok((response.status,response.body))
}
fn cash<E:HttpExecutor>(exec:&E,req:&SignedRequest,base:&str,now:u64)
    ->Result<(u128,u64),SpotPreHedgeError>{
    let (_,body)=get(exec,req)?;
    let v:Value=serde_json::from_str(&body)
        .map_err(|_|SpotPreHedgeError::Invalid("invalid balance JSON"))?;
    let data=v.get("data").and_then(Value::as_array)
        .ok_or(SpotPreHedgeError::Invalid("balance data absent"))?;
    if data.len()!=1{return Err(SpotPreHedgeError::Invalid("multiple balance roots"))}
    let details=data[0].get("details").and_then(Value::as_array)
        .ok_or(SpotPreHedgeError::Invalid("cash details absent"))?;
    let selected=details.iter().filter(|v|
        v.get("ccy").and_then(Value::as_str)==Some(base)).collect::<Vec<_>>();
    if selected.len()!=1{return Err(SpotPreHedgeError::Invalid("base currency not unique"))}
    let amount=base_atoms(&text(selected[0],"cashBal")?)?;
    let update=integer(selected[0],"uTime")?;
    if update==0||update>now.saturating_add(CLOCK_ALLOWANCE_MS)
        ||now.saturating_sub(update)>SOURCE_FRESH_MS{
        return Err(SpotPreHedgeError::Stale)
    }
    Ok((amount,update))
}
/// Preflight requires an entirely settled, ACKed FIRST spot order and no
/// pending futures intent. Any missing native details => zero signed GETs.
pub fn audit_okx_first_leg_before_hedge<E:HttpExecutor>(
    exec:&E,store:&NativeStateStore,expected_generation:u64,
    creds:&OkxCredentials,now_ms:u64,domain:OkxDomain,page_limit:u16,
)->Result<SpotPreHedgeReport,SpotPreHedgeError>{
    let state=store.load().map_err(SpotPreHedgeError::State)?
        .ok_or(SpotPreHedgeError::State(NativeStateError::Generation))?;
    if state.generation!=expected_generation{
        return Err(SpotPreHedgeError::State(NativeStateError::Generation))
    }
    let entry=state.entry.as_ref().ok_or(SpotPreHedgeError::Invalid("native entry absent"))?;
    if state.active.is_some()||entry.phase!=NativeEntryPhase::SpotExposed
        ||entry.spot_prehedge_evidence.is_some()
        ||entry.economics.entry.spot.orders.len()!=1
        ||entry.economics.entry.futures.orders.len()!=0
        ||now_ms<entry.plan.created_at_ms
        ||now_ms-entry.plan.created_at_ms>MAX_PLAN_AGE_MS
        ||page_limit==0||page_limit>100{
        return Err(SpotPreHedgeError::Invalid("not eligible for prospective capture"))
    }
    let (intent_id,original)=entry.economics.entry.spot.orders.iter().next()
        .ok_or(SpotPreHedgeError::Invalid("spot original ledger absent"))?;
    if !entry.intents.iter().any(|i|i.id==*intent_id
        &&i.client_order_id.as_deref()==Some(original.client_order_id.as_str())
        &&i.state==IntentState::Acknowledged)
        ||entry.intents.len()!=1
        ||!original.terminal||!original.cumulative_truth_present
        ||original.filled_qty_atoms()==0
        ||original.fills.len()>0
        ||original.fee_evidence_complete
        ||original.venue!="okx"||original.side!="buy"||original.symbol!=entry.plan.spot_symbol
        ||original.client_order_id.is_empty(){
        return Err(SpotPreHedgeError::Invalid("original settled spot ledger unsupported"))
    }
    let spot_order_req=build_okx_query_by_client_id(
        creds,now_ms,domain,&entry.plan.spot_symbol,&original.client_order_id,
    ).map_err(SpotPreHedgeError::Build)?;
    let cash_req=build_okx_balance_request(creds,now_ms,domain)
        .map_err(SpotPreHedgeError::Build)?;
    let (opening_balance,opening_updated)=cash(exec,&cash_req,&entry.plan.base,now_ms)?;
    let (status,raw)=get(exec,&spot_order_req)?;
    let order=parse_okx_economic_truth_http(status,&raw);
    if order.class!=EconomicTruthClass::Known||!order.terminal
        ||order.client_order_id.as_deref()!=Some(original.client_order_id.as_str())
        ||order.order_id.as_ref().map_or(true,|x|x.is_empty())
        ||order.cumulative_qty_atoms!=Some(original.filled_qty_atoms())
        ||order.avg_price_atoms!=original.vwap_atoms(){
        return Err(SpotPreHedgeError::Invalid("exact finished spot order not verified"))
    }
    let order_id=order.order_id.as_ref().unwrap();
    let mut ledger=original.clone();
    apply_order_economic_truth(&mut ledger,&order)
        .map_err(|_|SpotPreHedgeError::Invalid("spot economics changed since ACK"))?;
    let mut bill_cursor:Option<String>=None;
    let mut prev_bill:Option<u128>=None;
    let mut fill_bill_ids=BTreeSet::new();
    let mut bill_for_trade=BTreeMap::new();
    let mut base_fee=0i128;
    let mut pages=0;
    let mut exhausted=false;
    for _ in 0..MAX_PAGES{
        let request=build_okx_fills_history_page_request(
            creds,now_ms,domain,&entry.plan.spot_symbol,order_id,
            bill_cursor.as_deref(),page_limit,
        ).map_err(SpotPreHedgeError::Build)?;
        let (status,raw)=get(exec,&request)?;
        let parsed:Value=serde_json::from_str(&raw)
            .map_err(|_|SpotPreHedgeError::Invalid("fills JSON invalid"))?;
        let rows=parsed.get("data").and_then(Value::as_array)
            .ok_or(SpotPreHedgeError::Invalid("fill data absent"))?;
        pages+=1;
        if rows.len()>page_limit as usize{
            return Err(SpotPreHedgeError::Invalid("fills exceed page capacity"))
        }
        if rows.is_empty(){exhausted=true;break}
        let fills=parse_okx_fill_events_http(status,&raw,order_id,&entry.plan.base,"USDT")
            .map_err(|_|SpotPreHedgeError::Invalid("fill parsing rejected"))?;
        if rows.len()!=fills.len(){
            return Err(SpotPreHedgeError::Invalid("foreign order mixed into fill history"))
        }
        for (row,fill) in rows.iter().zip(fills.into_iter()){
            if text(row,"ordId")?!=*order_id
                ||fill.fee_cost_quote_atoms.is_none()
                ||fill.trade_time_ms.is_none(){
                return Err(SpotPreHedgeError::Invalid("fill ID/fee/time absent"))
            }
            let t=fill.trade_time_ms.unwrap();
            if t<entry.plan.created_at_ms.saturating_sub(EARLY_ALLOW_MS)||t>now_ms{
                return Err(SpotPreHedgeError::Invalid("fill time outside plan window"))
            }
            let bill_id=text(row,"billId")?;
            let numeric=bill_id.parse::<u128>().ok().filter(|x|*x>0)
                .ok_or(SpotPreHedgeError::Invalid("bill cursor nonnumeric"))?;
            if prev_bill.map_or(false,|prior|numeric>=prior)
                ||!fill_bill_ids.insert(bill_id.clone()){
                return Err(SpotPreHedgeError::Invalid("bill cursor not strictly descending"))
            }
            prev_bill=Some(numeric);
            bill_cursor=Some(bill_id.clone());
            if bill_for_trade.insert(fill.fill_id.clone(),bill_id).is_some(){
                return Err(SpotPreHedgeError::Invalid("duplicate fill tradeId"))
            }
            let fee_ccy=text(row,"feeCcy")?;
            if fee_ccy==entry.plan.base{
                base_fee=base_fee.checked_add(base_signed(&text(row,"fee")?)?)
                    .ok_or(SpotPreHedgeError::Invalid("base fee overflow"))?;
            }else if fee_ccy!="USDT"{
                return Err(SpotPreHedgeError::Invalid("unsupported fee asset"))
            }
            ledger.apply_fill(fill,DEFAULT_AVG_TOLERANCE_ATOMS)
                .map_err(|_|SpotPreHedgeError::Invalid("fill/quantity/price/economic mismatch"))?;
        }
    }
    if !exhausted{return Err(SpotPreHedgeError::UnexhaustedHistory)}
    if !ledger.fee_evidence_complete||ledger.fills.len()!=bill_for_trade.len()
        ||ledger.exchange_order_id.as_deref()!=Some(order_id.as_str())
        ||ledger.filled_qty_atoms()!=original.filled_qty_atoms(){
        return Err(SpotPreHedgeError::Invalid("first leg fee/size evidence incomplete"))
    }
    let (final_cash,final_update)=cash(exec,&cash_req,&entry.plan.base,now_ms)?;
    if opening_balance!=final_cash||opening_updated!=final_update{
        return Err(SpotPreHedgeError::Invalid("base cash moved during proof"))
    }
    let gross=ledger.filled_qty_atoms().checked_mul(1_000_000)
        .ok_or(SpotPreHedgeError::Invalid("gross base overflow"))?;
    let net=i128::try_from(entry.plan.spot_baseline_atoms_e18).ok()
        .and_then(|v|v.checked_add(i128::try_from(gross).ok()?))
        .and_then(|v|v.checked_add(base_fee));
    if net!=i128::try_from(final_cash).ok(){
        return Err(SpotPreHedgeError::Invalid("signed base cash != first fill ancestry"))
    }
    let current=store.load().map_err(SpotPreHedgeError::State)?
        .ok_or(SpotPreHedgeError::State(NativeStateError::Generation))?;
    if current!=state{return Err(SpotPreHedgeError::State(NativeStateError::Generation))}
    let sealed=NativeSpotPreHedgeEvidence{
        source_generation:expected_generation,observed_at_ms:now_ms,
        safety_generation:entry.plan.safety_generation,
        spot_intent_id:intent_id.clone(),spot_client_id:original.client_order_id.clone(),
        spot_order_id:order_id.clone(),exact_ledger:ledger.clone(),
        signed_base_fee_atoms_e18:base_fee,
        observed_base_cash_atoms_e18:final_cash,balance_updated_at_ms:final_update,
        fill_bill_ids:fill_bill_ids.into_iter().collect(),filled_pages:pages,
        cash_bracket_matched:true,exchange_bill_history_proven:false,
        exchange_global_completeness_proven:false,may_send_order:false,
    };
    Ok(SpotPreHedgeReport{
        source_generation:expected_generation,fills:ledger.fills.len(),pages,
        fee_quote_atoms_e12:ledger.fee_cost_quote_atoms,cash_atoms_e18:final_cash,
        is_complete_readonly_candidate:true,may_send_order:false,sealed:Some(sealed),
    })
}
