//! R32D6C8B2B2B (source-window component): STRICT bounded READ-ONLY diagnostic.
//! This cannot establish original native-position ancestry, exchange-global
//! history, cross-venue atomicity, verified close, real PnL, or cooldown.
//! Only synthetic HttpExecutor instances are used in review tests.
use crate::fill_economics::{decimal_to_atoms,signed_decimal_to_atoms,LegFillLedger};
use crate::authoritative_state::{NativeStateStore,NativeStateError,PositionProvenance};
use crate::native_close_readonly::{DualCloseCandidate,DualCloseReadOnlyCandidate};
use crate::private_https::{HttpExecutor,HttpsRequest,TransportError};
use crate::private_rest::{
    BinanceCredentials,OkxCredentials,OkxDomain,RestBuildError,HttpMethod,SignedRequest,
    build_okx_balance_request,build_okx_base_currency_bills_page_request,
    build_binance_position_risk_v3_request,build_binance_balance_v3_request,
    build_binance_open_orders_request,build_binance_all_income_page_request,
    build_binance_symbol_trade_window_request,
};
use serde_json::Value;
use std::collections::BTreeSet;

const MAX_PAGES:u16=8;
const MAX_AGE:u64=180_000;
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum CloseSourceError {
    Invalid(&'static str),Unexhausted,Build(RestBuildError),Transport(TransportError),
    State(NativeStateError),AncestryNotProven,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct CloseSourceObservation {
    spot_bills:usize,spot_quote_bills:usize,income_rows:usize,funding_rows:usize,
    futures_trades:usize,account_reads:usize,
    funding_observed_e12:i128,
}
impl CloseSourceObservation {
    pub fn counts(&self)->(usize,usize,usize,usize,usize){
        (self.spot_bills,self.spot_quote_bills,self.income_rows,self.funding_rows,self.futures_trades)
    }
    pub fn account_reads(&self)->usize{self.account_reads}
    pub fn observed_funding_e12(&self)->i128{self.funding_observed_e12}
    pub fn original_native_position_ancestry_proven(&self)->bool{false}
    pub fn full_account_history_proven(&self)->bool{false}
    pub fn funding_lifetime_complete(&self)->bool{false}
    pub fn cross_venue_atomicity_proven(&self)->bool{false}
    pub fn may_mark_verified_closed(&self)->bool{false}
    pub fn may_send_order(&self)->bool{false}
    pub fn may_start_real_cooldown(&self)->bool{false}
}
fn field<'a>(v:&'a Value,k:&str)->Result<&'a str,CloseSourceError>{
    v.get(k).and_then(Value::as_str).ok_or(CloseSourceError::Invalid("field absent"))
}
fn scalar(v:&Value,k:&str)->Result<String,CloseSourceError>{
    v.get(k).and_then(|x|x.as_str().map(str::to_owned).or_else(||
        x.as_u64().map(|n|n.to_string())))
        .ok_or(CloseSourceError::Invalid("numeric/text field absent"))
}
fn amount(v:&Value,k:&str)->Result<u128,CloseSourceError>{
    decimal_to_atoms(field(v,k)?).map_err(|_|CloseSourceError::Invalid("invalid decimal"))
}
fn signed(v:&Value,k:&str)->Result<i128,CloseSourceError>{
    signed_decimal_to_atoms(field(v,k)?).map_err(|_|CloseSourceError::Invalid("invalid signed amount"))
}
fn timestamp(v:&Value,k:&str,begin:u64,now:u64)->Result<u64,CloseSourceError>{
    let t=scalar(v,k)?.parse::<u64>()
       .map_err(|_|CloseSourceError::Invalid("bad timestamp"))?;
    if t<begin||t>now.saturating_add(5_000){
        return Err(CloseSourceError::Invalid("timestamp outside bounded window"))
    }
    Ok(t)
}
fn recent(v:u64,now:u64)->Result<(),CloseSourceError>{
    if v==0||v>now.saturating_add(5_000)||now.saturating_sub(v)>MAX_AGE{
        Err(CloseSourceError::Invalid("stale account snapshot"))
    }else{Ok(())}
}
fn get<E:HttpExecutor>(exec:&E,r:&SignedRequest)->Result<Value,CloseSourceError>{
    if r.method!=HttpMethod::Get||r.is_mutation||r.body.is_some(){
        return Err(CloseSourceError::Invalid("mutating request"))
    }
    let h=exec.execute_request(&HttpsRequest::from_signed(r)).map_err(CloseSourceError::Transport)?;
    if h.status!=200{return Err(CloseSourceError::Invalid("GET failed"))}
    serde_json::from_str(&h.body).map_err(|_|CloseSourceError::Invalid("malformed GET"))
}
fn okx_rows(v:&Value)->Result<&Vec<Value>,CloseSourceError>{
    if v.get("code").and_then(Value::as_str)!=Some("0"){
        return Err(CloseSourceError::Invalid("OKX business error"))
    }
    v.get("data").and_then(Value::as_array)
        .ok_or(CloseSourceError::Invalid("OKX data missing"))
}
fn bin_rows(v:&Value)->Result<&Vec<Value>,CloseSourceError>{
    v.as_array().ok_or(CloseSourceError::Invalid("Binance data missing"))
}
fn okx_snapshot<E:HttpExecutor>(
    exec:&E,r:&SignedRequest,base:&str,now:u64
)->Result<Vec<(String,u128,u128,u128,u64)>,CloseSourceError>{
    let j=get(exec,r)?;
    let rows=okx_rows(&j)?;
    if rows.len()!=1{return Err(CloseSourceError::Invalid("OKX multiple accounts"))}
    let details=rows[0].get("details").and_then(Value::as_array)
       .ok_or(CloseSourceError::Invalid("OKX balances absent"))?;
    let mut seen=BTreeSet::new();let mut out=Vec::new();
    for row in details{
        let c=field(row,"ccy")?;
        if c!=base&&c!="USDT"{continue}
        if !seen.insert(c.to_owned()){return Err(CloseSourceError::Invalid("duplicate OKX currency"))}
        let cash=amount(row,"cashBal")?;
        let available=amount(row,"availBal")?;
        let frozen=amount(row,"frozenBal")?;
        if available.checked_add(frozen)!=Some(cash){
            return Err(CloseSourceError::Invalid("OKX available/frozen accounting"))
        }
        let tm=timestamp(row,"uTime",now.saturating_sub(MAX_AGE),now)?;
        recent(tm,now)?;
        out.push((c.to_owned(),cash,available,frozen,tm));
    }
    out.sort();
    if seen.len()!=2||out.iter().any(|x|x.3!=0){
        return Err(CloseSourceError::Invalid("missing/frozen spot or quote asset"))
    }
    Ok(out)
}
fn bin_snapshot<E:HttpExecutor>(
    exec:&E,position:&SignedRequest,wallet:&SignedRequest,orders:&SignedRequest,
    symbol:&str,now:u64
)->Result<(u128,u128,u64,u64),CloseSourceError>{
    let positions=get(exec,position)?;
    let mut found=None;
    for p in bin_rows(&positions)? {
        if field(p,"symbol")?!=symbol{continue}
        if found.is_some()||field(p,"positionSide")?!="BOTH"
          ||field(p,"marginAsset")?!="USDT"||signed(p,"positionAmt")?!=0{
            return Err(CloseSourceError::Invalid("Binance futures not flat/OneWay"))
        }
        let t=timestamp(p,"updateTime",now.saturating_sub(MAX_AGE),now)?;
        recent(t,now)?;found=Some(t);
    }
    let position_time=found.ok_or(CloseSourceError::Invalid("position absent or ambiguous"))?;
    if !bin_rows(&get(exec,orders)?)?.is_empty(){
        return Err(CloseSourceError::Invalid("open futures order exists"))
    }
    let balances=get(exec,wallet)?;
    let mut usdt=None;
    for b in bin_rows(&balances)?{
        if field(b,"asset")?!="USDT"{continue}
        if usdt.is_some(){return Err(CloseSourceError::Invalid("duplicate USDT wallet"))}
        let balance=amount(b,"balance")?;
        let available=amount(b,"availableBalance")?;
        if available>balance{return Err(CloseSourceError::Invalid("wallet balance mismatch"))}
        let wallet_time=timestamp(b,"updateTime",now.saturating_sub(MAX_AGE),now)?;
        recent(wallet_time,now)?;
        usdt=Some((balance,available,position_time,wallet_time));
    }
    usdt.ok_or(CloseSourceError::Invalid("wallet USDT missing"))
}
#[allow(clippy::too_many_arguments)]
fn bills<E:HttpExecutor>(
    exec:&E,creds:&OkxCredentials,domain:OkxDomain,base:&str,ccy:&str,
    order_id:&str,ledger:&LegFillLedger,begin:u64,now:u64,limit:u16
)->Result<usize,CloseSourceError>{
    let mut after=None::<String>;let mut prev=None::<u128>;
    let mut seen=BTreeSet::new();let mut count=0;
    let allowed=ledger.fills.keys().cloned().collect::<BTreeSet<_>>();
    for _ in 0..MAX_PAGES{
        let req=build_okx_base_currency_bills_page_request(
            creds,now,domain,ccy,begin,now,after.as_deref(),limit)
            .map_err(CloseSourceError::Build)?;
        let j=get(exec,&req)?;let rows=okx_rows(&j)?;
        if rows.len()>limit as usize{return Err(CloseSourceError::Unexhausted)}
        if rows.is_empty(){
            if seen!=allowed{return Err(CloseSourceError::Invalid("missing per-fill currency bills"))}
            return Ok(count)
        }
        for row in rows{
            if field(row,"ccy")?!=ccy||scalar(row,"ordId")?!=order_id
             ||field(row,"instId")?!=format!("{base}-USDT")
             ||field(row,"subType")?!="2"{
                return Err(CloseSourceError::Invalid("foreign bill or movement"))
            }
            let trade=scalar(row,"tradeId")?;
            if !allowed.contains(&trade)||!seen.insert(trade.clone()){
                return Err(CloseSourceError::Invalid("unexpected/duplicate trade bill"))
            }
            let change=signed(row,"balChg")?;
            let fill=ledger.fills.get(&trade)
                .ok_or(CloseSourceError::Invalid("bill trade fill absent"))?;
            let expected=if ccy==base{
                i128::try_from(fill.qty_atoms).ok().and_then(|q|q.checked_neg())
            }else{
                let quote=fill.price_atoms.checked_mul(fill.qty_atoms)
                    .and_then(|v|if v%1_000_000_000_000==0{Some(v/1_000_000_000_000)}else{None});
                quote.and_then(|q|i128::try_from(q).ok())
                    .and_then(|q|fill.fee_cost_quote_atoms.and_then(|fee|q.checked_sub(fee)))
            };
            if expected!=Some(change){
                return Err(CloseSourceError::Invalid("per-fill exact cash movement mismatch"))
            }
            timestamp(row,"ts",begin,now)?;
            let id=scalar(row,"billId")?;
            let n=id.parse::<u128>().ok().filter(|x|*x>0)
                .ok_or(CloseSourceError::Invalid("bill ID invalid"))?;
            if prev.is_some_and(|p|n>=p){
                return Err(CloseSourceError::Invalid("bill cursor duplicate/drift"))
            }
            prev=Some(n);after=Some(id);count+=1;
        }
    }
    Err(CloseSourceError::Unexhausted)
}
#[allow(clippy::too_many_arguments)]
pub fn audit_close_source_window_once<E:HttpExecutor>(
    exec:&E,identity:&DualCloseCandidate,receipt:&DualCloseReadOnlyCandidate,
    okx:&OkxCredentials,bin:&BinanceCredentials,now:u64,
    domain:OkxDomain,recv_window:u64,page_limit:u16,
)->Result<CloseSourceObservation,CloseSourceError>{
    if page_limit<2||page_limit>100||recv_window==0||recv_window>60_000
      ||identity.created_at_ms==0||identity.created_at_ms>now
      ||now-identity.created_at_ms>MAX_AGE
      ||receipt.position_identity()!=identity.position_identity
      ||receipt.spot_ledger().exchange_order_id.as_deref()
            !=Some(identity.spot.exchange_order_id.as_str())
      ||receipt.futures_ledger().exchange_order_id.as_deref()
            !=Some(identity.futures.exchange_order_id.as_str())
      ||receipt.spot_ledger().filled_qty_atoms()!=identity.expected_qty_e12
      ||receipt.futures_ledger().filled_qty_atoms()!=identity.expected_qty_e12
      ||receipt.close_verified()||receipt.may_send_order(){
        return Err(CloseSourceError::Invalid("unanchored close order receipts"))
    }
    let spot_ids=receipt.spot_ledger().fills.keys().cloned().collect::<BTreeSet<_>>();
    let futures_ids=receipt.futures_ledger().fills.keys().cloned().collect::<BTreeSet<_>>();
    if spot_ids.is_empty()||futures_ids.is_empty(){return Err(CloseSourceError::Invalid("missing fills"))}
    let ob=build_okx_balance_request(okx,now,domain).map_err(CloseSourceError::Build)?;
    let bp=build_binance_position_risk_v3_request(bin,now,recv_window).map_err(CloseSourceError::Build)?;
    let bw=build_binance_balance_v3_request(bin,now,recv_window).map_err(CloseSourceError::Build)?;
    let bo=build_binance_open_orders_request(bin,now,recv_window).map_err(CloseSourceError::Build)?;
    let spot_before=okx_snapshot(exec,&ob,&identity.base,now)?;
    let future_before=bin_snapshot(exec,&bp,&bw,&bo,&identity.futures.symbol,now)?;
    let base_count=bills(exec,okx,domain,&identity.base,&identity.base,
        &identity.spot.exchange_order_id,receipt.spot_ledger(),identity.created_at_ms,now,page_limit)?;
    let quote_count=bills(exec,okx,domain,&identity.base,"USDT",
        &identity.spot.exchange_order_id,receipt.spot_ledger(),identity.created_at_ms,now,page_limit)?;
    let mut page=1u16;let mut income_rows=0usize;let mut funding_rows=0usize;
    let mut funding=0i128;let mut commissions=BTreeSet::new();
    let mut pnl_seen=BTreeSet::new();let mut pnl=0i128;let mut txids=BTreeSet::new();
    loop{
        if page>MAX_PAGES{return Err(CloseSourceError::Unexhausted)}
        let req=build_binance_all_income_page_request(bin,now,recv_window,
            identity.created_at_ms,now,page,page_limit).map_err(CloseSourceError::Build)?;
        let j=get(exec,&req)?;let rows=bin_rows(&j)?;
        if rows.len()>page_limit as usize{return Err(CloseSourceError::Unexhausted)}
        if rows.is_empty(){break}
        for row in rows{
            income_rows+=1;
            if field(row,"asset")?!="USDT"||field(row,"symbol")?!=identity.futures.symbol{
                return Err(CloseSourceError::Invalid("foreign income or asset"))
            }
            timestamp(row,"time",identity.created_at_ms,now)?;
            let k=field(row,"incomeType")?;
            let txn=scalar(row,"tranId")?;
            if !txids.insert(format!("{k}:{txn}")){
                return Err(CloseSourceError::Invalid("duplicate income row"))
            }
            match k{
                "COMMISSION"=>{
                    let trade=scalar(row,"tradeId")?;
                    let fill=receipt.futures_ledger().fills.get(&trade)
                        .ok_or(CloseSourceError::Invalid("income trade not in close ledger"))?;
                    if !commissions.insert(trade)
                      ||signed(row,"income")?.checked_neg()!=fill.fee_cost_quote_atoms{
                        return Err(CloseSourceError::Invalid("commission amount/ID mismatch"))
                    }
                },
                "REALIZED_PNL"=>{
                    let trade=scalar(row,"tradeId")?;
                    if !futures_ids.contains(&trade)||!pnl_seen.insert(trade){
                        return Err(CloseSourceError::Invalid("realized PnL not in close trades"))
                    }
                    pnl=pnl.checked_add(signed(row,"income")?)
                        .ok_or(CloseSourceError::Invalid("PnL overflow"))?;
                },
                "FUNDING_FEE"=>{
                    funding_rows+=1;
                    funding=funding.checked_add(signed(row,"income")?)
                        .ok_or(CloseSourceError::Invalid("funding overflow"))?;
                },
                _=>return Err(CloseSourceError::Invalid("unknown/foreign income type")),
            }
        }
        page+=1;
    }
    if funding_rows==0 || commissions!=futures_ids||pnl_seen!=futures_ids
      ||pnl!=receipt.futures_reported_realized_pnl_e12(){
        return Err(CloseSourceError::Invalid("income-to-fills incomplete"))
    }
    let trade_req=build_binance_symbol_trade_window_request(
        bin,now,recv_window,&identity.futures.symbol,identity.created_at_ms,now,page_limit)
        .map_err(CloseSourceError::Build)?;
    let rows=get(exec,&trade_req)?;
    let trades=bin_rows(&rows)?;
    if trades.len()>=page_limit as usize||trades.is_empty(){return Err(CloseSourceError::Unexhausted)}
    let mut trade_seen=BTreeSet::new();
    for row in trades{
        let trade=scalar(row,"id")?;
        if !futures_ids.contains(&trade)||!trade_seen.insert(trade)
          ||scalar(row,"orderId")?!=identity.futures.exchange_order_id
          ||field(row,"symbol")?!=identity.futures.symbol
          ||field(row,"side")?!="BUY"||field(row,"positionSide")?!="BOTH"{
            return Err(CloseSourceError::Invalid("foreign symbol-wide trade"))
        }
        timestamp(row,"time",identity.created_at_ms,now)?;
    }
    if trade_seen!=futures_ids{return Err(CloseSourceError::Invalid("symbol-wide trade missing"))}
    let spot_after=okx_snapshot(exec,&ob,&identity.base,now)?;
    let future_after=bin_snapshot(exec,&bp,&bw,&bo,&identity.futures.symbol,now)?;
    if spot_before!=spot_after||future_before!=future_after{
        return Err(CloseSourceError::Invalid("exchange account bracket moved"))
    }
    Ok(CloseSourceObservation{
        spot_bills:base_count,spot_quote_bills:quote_count,income_rows,funding_rows,
        futures_trades:trade_seen.len(),funding_observed_e12:funding,account_reads:8,
    })
}

/// A separate mandatory GATE before a future close authority or durable
/// diagnostic. Current native active schema does not persist immutable
/// original position_id plus opening policy hash. R32D6C4 also denies Native
/// active in ordinary builds. NEVER treat caller-provided IDs as ancestry.
pub fn require_persisted_native_close_ancestry(
    store:&NativeStateStore,expected_generation:u64,identity:&DualCloseCandidate
)->Result<(),CloseSourceError>{
    let state=store.load().map_err(CloseSourceError::State)?
        .ok_or(CloseSourceError::AncestryNotProven)?;
    if state.generation!=expected_generation{
        return Err(CloseSourceError::State(NativeStateError::Generation))
    }
    let position=state.active.as_ref().ok_or(CloseSourceError::AncestryNotProven)?;
    if position.provenance!=PositionProvenance::Native
        ||position.reconciliation_required||state.entry.is_some()
        ||position.core.base!=identity.base
        ||position.core.spot_symbol!=identity.spot.symbol
        ||position.core.futures_symbol!=identity.futures.symbol{
        return Err(CloseSourceError::AncestryNotProven)
    }
    // There is no immutable position ID or original entry-policy digest in
    // NativePositionBundle. Do not claim success by comparing symbol/quantity.
    Err(CloseSourceError::AncestryNotProven)
}
