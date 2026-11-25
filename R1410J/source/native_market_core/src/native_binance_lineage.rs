//! R32D5B: Binance USD-M read-only income, position and margin cross-check.
//! This is a conservative DIAGNOSTIC, never a futures trading permit.
//! Signed GET snapshots do not provide exchange-global atomicity.
use crate::authoritative_state::{
    NativeEntryLeg,NativeEntryRecoveryQuality,NativeStateError,NativeStateStore,
};
use crate::fill_economics::{decimal_to_atoms,signed_decimal_to_atoms};
use crate::private_https::{HttpExecutor,HttpsRequest,TransportError};
use crate::private_rest::{
    BinanceCredentials,BinancePositionMode,HttpMethod,RestBuildError,SignedRequest,
    build_binance_account_config_request,build_binance_all_income_page_request,
    build_binance_balance_v3_request,build_binance_open_orders_request,
    build_binance_position_mode_request,build_binance_position_risk_v3_request,
    parse_binance_position_mode_http,
};
use crate::private_truth::{
    parse_binance_account_config,parse_binance_open_orders,PrivateTruthError,
};
use serde_json::Value;

const MAX_PAGES:u16=16;
const MAX_LOOKBACK_MS:u64=70*24*60*60*1000;
const UPDATE_MAX_AGE_MS:u64=180_000;
const FUTURE_TOLERANCE_MS:u64=5_000;
const FILL_TIME_TOLERANCE_MS:u64=10_000;

#[derive(Clone,Debug,PartialEq,Eq)]
pub enum BinanceLineageError{
    State(NativeStateError),Build(RestBuildError),
    Transport(TransportError),PrivateTruth(PrivateTruthError),
    Invalid(&'static str),NotFresh,PageLimitExceeded,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct BinanceLineageReport{
    pub income_pages:u16,
    pub commission_rows:usize,
    pub total_commission_usdt_e12:i128,
    pub position_short_atoms_e18:u128,
    pub position_last_update_ms:u64,
    pub usdt_wallet_atoms_e18:u128,
    pub wallet_last_update_ms:u64,
    pub income_window_exhaustion_seen:bool,
    pub prior_spot_fee_origin_proven:bool,
    pub all_account_activity_proven:bool,
    pub exchange_snapshots_atomicity_proven:bool,
    pub may_send_order:bool,
}

fn scalar(v:&Value,key:&str)->Result<String,BinanceLineageError>{
    let f=v.get(key).ok_or(BinanceLineageError::Invalid("missing field"))?;
    if let Some(s)=f.as_str(){return Ok(s.into())}
    if let Some(n)=f.as_u64(){return Ok(n.to_string())}
    Err(BinanceLineageError::Invalid("unsupported field type"))
}
fn parsed_time(v:&Value,key:&str)->Result<u64,BinanceLineageError>{
    scalar(v,key)?.parse::<u64>()
        .map_err(|_|BinanceLineageError::Invalid("timestamp/id invalid"))
}
fn unsigned_e18(s:&str)->Result<u128,BinanceLineageError>{
    decimal_to_atoms(s).ok().and_then(|x|x.checked_mul(1_000_000))
        .ok_or(BinanceLineageError::Invalid("amount precision/overflow"))
}
fn signed_e12(s:&str)->Result<i128,BinanceLineageError>{
    signed_decimal_to_atoms(s)
        .map_err(|_|BinanceLineageError::Invalid("signed decimal invalid"))
}
fn read<E:HttpExecutor>(
    exec:&E,req:&SignedRequest,
)->Result<crate::private_https::HttpResponse,BinanceLineageError>{
    if req.method!=HttpMethod::Get||req.is_mutation{
        return Err(BinanceLineageError::Invalid("mutation request prohibited"))
    }
    exec.execute_request(&HttpsRequest::from_signed(req))
        .map_err(BinanceLineageError::Transport)
}
fn json_array<E:HttpExecutor>(
    exec:&E,req:&SignedRequest,
)->Result<Vec<Value>,BinanceLineageError>{
    let res=read(exec,req)?;
    if res.status!=200{return Err(BinanceLineageError::Invalid("private GET status failed"))}
    let json:Value=serde_json::from_str(&res.body)
        .map_err(|_|BinanceLineageError::Invalid("private JSON malformed"))?;
    json.as_array().cloned()
        .ok_or(BinanceLineageError::Invalid("private GET not an array"))
}
fn recent(updated:u64,now:u64)->Result<(),BinanceLineageError>{
    if updated==0||updated>now.saturating_add(FUTURE_TOLERANCE_MS)
        ||now.saturating_sub(updated)>UPDATE_MAX_AGE_MS{
        return Err(BinanceLineageError::NotFresh)
    }
    Ok(())
}
fn position<E:HttpExecutor>(
    exec:&E,req:&SignedRequest,symbol:&str,
    expected_short_e18:u128,now:u64,
)->Result<u64,BinanceLineageError>{
    let rows=json_array(exec,req)?;
    let mut match_updated=None;
    for row in &rows{
        let name=scalar(row,"symbol")?;
        let amt=scalar(row,"positionAmt")?;
        let side=scalar(row,"positionSide")?;
        if side!="BOTH"{
            return Err(BinanceLineageError::Invalid("hedge mode position observed"))
        }
        let mag=if let Some(short)=amt.strip_prefix('-'){
            unsigned_e18(short)?
        }else{unsigned_e18(&amt)?};
        if name==symbol{
            if match_updated.is_some()||!amt.starts_with('-')
                ||mag!=expected_short_e18
                ||scalar(row,"marginAsset")?!="USDT"{
                return Err(BinanceLineageError::Invalid("wrong/duplicate futures short"))
            }
            let updated=parsed_time(row,"updateTime")?;
            recent(updated,now)?;
            match_updated=Some(updated);
        }else if mag!=0{
            return Err(BinanceLineageError::Invalid("unrelated nonzero futures position"))
        }
    }
    match_updated.ok_or(BinanceLineageError::Invalid("expected short position absent"))
}
fn wallet<E:HttpExecutor>(
    exec:&E,req:&SignedRequest,now:u64,
)->Result<(u128,u64),BinanceLineageError>{
    let rows=json_array(exec,req)?;
    let mut out=None;
    for row in &rows{
        let asset=scalar(row,"asset")?;
        if asset!="USDT"{continue}
        if out.is_some(){return Err(BinanceLineageError::Invalid("duplicate USDT wallet"))}
        let amount=unsigned_e18(&scalar(row,"balance")?)?;
        let updated=parsed_time(row,"updateTime")?;
        recent(updated,now)?;
        out=Some((amount,updated));
    }
    out.ok_or(BinanceLineageError::Invalid("USDT wallet absent"))
}

fn binance_controls<E:HttpExecutor>(
    exec:&E,creds:&BinanceCredentials,now_ms:u64,recv_window_ms:u64,
)->Result<(),BinanceLineageError>{
    let mode_req=build_binance_position_mode_request(
        creds,now_ms,recv_window_ms).map_err(BinanceLineageError::Build)?;
    let mode=read(exec,&mode_req)?;
    if parse_binance_position_mode_http(mode.status,&mode.body)
        !=BinancePositionMode::OneWay{
        return Err(BinanceLineageError::Invalid("not Binance OneWay"))
    }
    let config_req=build_binance_account_config_request(
        creds,now_ms,recv_window_ms).map_err(BinanceLineageError::Build)?;
    let config=read(exec,&config_req)?;
    let config=parse_binance_account_config(config.status,&config.body)
        .map_err(BinanceLineageError::PrivateTruth)?;
    if config.multi_assets_margin{
        return Err(BinanceLineageError::Invalid("multi-asset margin not audited"))
    }
    let open_req=build_binance_open_orders_request(
        creds,now_ms,recv_window_ms).map_err(BinanceLineageError::Build)?;
    let open=read(exec,&open_req)?;
    let orders=parse_binance_open_orders(open.status,&open.body)
        .map_err(BinanceLineageError::PrivateTruth)?;
    if !orders.is_empty(){
        return Err(BinanceLineageError::Invalid("open orders need separate provenance"))
    }
    Ok(())
}

/// Check the entire account's income history: no symbol or incomeType filter.
/// Reject all unaccounted funding, transfers, PnL, rebates, etc. A full
/// apparently-consistent history remains a CANDIDATE only; not proof of
/// unrelated order absence or cross-exchange atomicity.
#[allow(clippy::too_many_arguments)]
pub fn audit_binance_futures_lineage_once<E:HttpExecutor>(
    exec:&E,store:&NativeStateStore,expected_generation:u64,
    creds:&BinanceCredentials,now_ms:u64,recv_window_ms:u64,
    page_limit:u16,
)->Result<BinanceLineageReport,BinanceLineageError>{
    let target=store.pending_native_entry_query(expected_generation)
        .map_err(BinanceLineageError::State)?;
    if target.leg!=NativeEntryLeg::Futures{
        return Err(BinanceLineageError::Invalid("pending leg is not futures"))
    }
    let state=store.load().map_err(BinanceLineageError::State)?
        .ok_or(BinanceLineageError::State(NativeStateError::Generation))?;
    if state.generation!=expected_generation{
        return Err(BinanceLineageError::State(NativeStateError::Generation))
    }
    let entry=state.entry.as_ref().ok_or(BinanceLineageError::Invalid("entry missing"))?;
    let cp=entry.recovery_checkpoint.as_ref()
        .ok_or(BinanceLineageError::Invalid("futures checkpoint absent"))?;
    if cp.quality!=NativeEntryRecoveryQuality::FuturesPriorSpotUnverified
        ||cp.leg!=NativeEntryLeg::Futures
        ||cp.client_id!=target.client_id||cp.intent_id!=target.intent_id
        ||cp.ledger.fills.is_empty()||!cp.ledger.fee_evidence_complete{
        return Err(BinanceLineageError::Invalid("checkpoint not for exact futures intent"))
    }
    if now_ms<target.created_at_ms
        ||now_ms-target.created_at_ms>MAX_LOOKBACK_MS{
        return Err(BinanceLineageError::Invalid("income retention window exceeded"))
    }
    if page_limit==0||page_limit>1000{
        return Err(BinanceLineageError::Invalid("invalid page limit"))
    }
    let expected_short=target.confirmed_futures_atoms_e18
        .checked_add(cp.ledger.filled_qty_atoms()
            .checked_mul(1_000_000)
            .ok_or(BinanceLineageError::Invalid("short precision overflow"))?)
        .ok_or(BinanceLineageError::Invalid("short amount overflow"))?;
    if expected_short==0{
        return Err(BinanceLineageError::Invalid("zero shorts are not certified"))
    }
    binance_controls(exec,creds,now_ms,recv_window_ms)?;
    let pos_req=build_binance_position_risk_v3_request(
        creds,now_ms,recv_window_ms).map_err(BinanceLineageError::Build)?;
    let bal_req=build_binance_balance_v3_request(
        creds,now_ms,recv_window_ms).map_err(BinanceLineageError::Build)?;
    let before_position=position(
        exec,&pos_req,&target.futures_symbol,expected_short,now_ms)?;
    let before_wallet=wallet(exec,&bal_req,now_ms)?;

    let mut pages=0u16;
    let mut cursor_seen=std::collections::HashSet::<String>::new();
    let mut commission_seen=std::collections::HashSet::<String>::new();
    let mut fee_total=0i128;
    let mut completed=false;
    for page in 1..=MAX_PAGES{
        let req=build_binance_all_income_page_request(
            creds,now_ms,recv_window_ms,target.created_at_ms,now_ms,page,page_limit,
        ).map_err(BinanceLineageError::Build)?;
        let rows=json_array(exec,&req)?;
        pages+=1;
        if rows.len()>page_limit as usize{
            return Err(BinanceLineageError::Invalid("income page exceeds bound"))
        }
        if rows.is_empty(){completed=true;break}
        for row in &rows{
            let kind=scalar(row,"incomeType")?;
            let txn=scalar(row,"tranId")?;
            let uniq=format!("{kind}:{txn}");
            if !cursor_seen.insert(uniq){
                return Err(BinanceLineageError::Invalid("duplicate income transaction"))
            }
            let t=parsed_time(row,"time")?;
            if t<target.created_at_ms || t>now_ms {
                return Err(BinanceLineageError::Invalid("income time outside requested window"))
            }
            if kind!="COMMISSION"{
                // Includes TRANSFER, FUNDING_FEE, REALIZED_PNL and rebates:
                // an exact income-to-trade/position ownership proof is absent.
                return Err(BinanceLineageError::Invalid("unrelated account income movement"))
            }
            if scalar(row,"symbol")?!=target.futures_symbol
                ||scalar(row,"asset")?!="USDT"{
                return Err(BinanceLineageError::Invalid("unknown commission provenance/asset"))
            }
            let trade_id=scalar(row,"tradeId")?;
            let fill=cp.ledger.fills.get(&trade_id)
                .ok_or(BinanceLineageError::Invalid("commission trade ID unknown"))?;
            if !commission_seen.insert(trade_id){
                return Err(BinanceLineageError::Invalid("trade commission duplicated"))
            }
            let fill_time=fill.trade_time_ms
                .ok_or(BinanceLineageError::Invalid("fill timestamp missing"))?;
            if t.abs_diff(fill_time)>FILL_TIME_TOLERANCE_MS{
                return Err(BinanceLineageError::Invalid("commission vs fill time mismatch"))
            }
            let income=signed_e12(&scalar(row,"income")?)?;
            let cost=fill.fee_cost_quote_atoms
                .ok_or(BinanceLineageError::Invalid("fill commission absent"))?;
            if cost<0||income.checked_neg()!=Some(cost){
                return Err(BinanceLineageError::Invalid("commission amount mismatch"))
            }
            fee_total=fee_total.checked_add(cost)
                .ok_or(BinanceLineageError::Invalid("commission sum overflow"))?;
        }
    }
    if !completed{return Err(BinanceLineageError::PageLimitExceeded)}
    if commission_seen.len()!=cp.ledger.fills.len()
        ||fee_total!=cp.ledger.fee_cost_quote_atoms{
        return Err(BinanceLineageError::Invalid("commission history incomplete"))
    }
    let after_wallet=wallet(exec,&bal_req,now_ms)?;
    let after_position=position(
        exec,&pos_req,&target.futures_symbol,expected_short,now_ms)?;
    if before_position!=after_position || before_wallet!=after_wallet{
        return Err(BinanceLineageError::Invalid("futures exposure or USDT wallet changed"))
    }
    // Also re-check OneWay, single-asset margin and account-wide pending
    // orders after the entire income page series.
    binance_controls(exec,creds,now_ms,recv_window_ms)?;
    let current=store.pending_native_entry_query(expected_generation)
        .map_err(BinanceLineageError::State)?;
    if current!=target{return Err(BinanceLineageError::State(NativeStateError::Generation))}
    Ok(BinanceLineageReport{
        income_pages:pages,commission_rows:commission_seen.len(),
        total_commission_usdt_e12:fee_total,
        position_short_atoms_e18:expected_short,
        position_last_update_ms:after_position,
        usdt_wallet_atoms_e18:after_wallet.0,
        wallet_last_update_ms:after_wallet.1,
        income_window_exhaustion_seen:true,
        prior_spot_fee_origin_proven:false,
        all_account_activity_proven:false,
        exchange_snapshots_atomicity_proven:false,
        may_send_order:false,
    })
}
