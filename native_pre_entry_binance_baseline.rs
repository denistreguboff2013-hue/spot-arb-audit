//! MICRO5A: signed-GET-only PRE-ENTRY Binance USDT wallet witness.
//! Crucial: capture BEFORE any native order is prepared; later identical
//! account snapshots cannot prove what existed at original entry time.
//! This local witness is NOT a lifetime account ledger, anti-rollback
//! attestation, permission to send, or proof all transfers/income are seen.
use crate::authoritative_state::{
    NativeAuthoritativeState,NativeStateError,NativeStateStore,
};
use crate::private_https::{HttpExecutor,HttpsRequest,TransportError};
use crate::private_rest::{
    BinanceCredentials,BinancePositionMode,HttpMethod,RestBuildError,SignedRequest,
    build_binance_balance_v3_request,build_binance_account_config_request,
    build_binance_open_orders_request,build_binance_position_mode_request,
    build_binance_position_risk_v3_request,parse_binance_position_mode_http,
};
use crate::private_truth::{parse_binance_account_config,parse_binance_open_orders};
use crate::fill_economics::decimal_to_atoms;
use serde::{Serialize,Deserialize};
use serde_json::Value;

fn deny(s:&'static str)->NativeStateError{NativeStateError::Invalid(s)}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
pub struct NativePreEntryBinanceBaseline {
    source_generation:u64,
    captured_at_ms:u64,
    safety_generation:u64,
    usdt_wallet_e12:u128,
    usdt_available_e12:u128,
    usdt_updated_at_ms:u64,
    snapshots:u8,
    open_orders_zero:bool,
    global_futures_positions_zero:bool,
    one_way_mode:bool,
    multi_asset_margin_disabled:bool,
    entire_prior_account_history_proven:bool,
    external_anti_rollback_proven:bool,
    may_send_order:bool,
    may_assume_funding_history_complete:bool,
}
impl NativePreEntryBinanceBaseline{
    pub fn wallet_usdt_e12(&self)->u128{self.usdt_wallet_e12}
    pub fn available_usdt_e12(&self)->u128{self.usdt_available_e12}
    pub fn captured_at_ms(&self)->u64{self.captured_at_ms}
    pub fn source_generation(&self)->u64{self.source_generation}
    pub fn may_send_order(&self)->bool{self.may_send_order}
    pub fn history_complete(&self)->bool{self.entire_prior_account_history_proven}
    pub fn validate(&self,s:&NativeAuthoritativeState)->Result<(),NativeStateError>{
        if self.source_generation==0||self.source_generation>=s.generation
            ||self.safety_generation==0||self.captured_at_ms==0
            ||self.usdt_wallet_e12==0||self.usdt_available_e12==0
            ||self.usdt_available_e12>self.usdt_wallet_e12
            ||self.usdt_updated_at_ms==0
            ||self.usdt_updated_at_ms>self.captured_at_ms.saturating_add(5_000)
            ||self.captured_at_ms.saturating_sub(self.usdt_updated_at_ms)>180_000
            ||self.snapshots!=2||!self.open_orders_zero
            ||!self.global_futures_positions_zero||!self.one_way_mode
            ||!self.multi_asset_margin_disabled
            ||self.entire_prior_account_history_proven
            ||self.external_anti_rollback_proven||self.may_send_order
            ||self.may_assume_funding_history_complete{
            return Err(deny("pre-entry Binance snapshot invalid or overclaims authority"))
        }
        if let Some(e)=s.entry.as_ref(){
            if e.plan.safety_generation!=self.safety_generation
                ||e.plan.created_at_ms<self.captured_at_ms
                ||e.plan.created_at_ms-self.captured_at_ms>60_000{
                return Err(deny("native entry unbound to fresh opening account baseline"))
            }
        }
        if s.active.is_some(){
            return Err(deny("baseline cannot authorize Native active position"))
        }
        Ok(())
    }
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum PreEntryBinanceAuditError{
    State(NativeStateError),Build(RestBuildError),Transport(TransportError),
    Invalid(&'static str),
}
#[derive(Clone,Debug,PartialEq,Eq)]
struct ReadonlyBinanceOpeningSnapshot{
    wallet_e12:u128,
    available_e12:u128,
    updated:u64,
}
fn get<E:HttpExecutor>(exec:&E,signed:&SignedRequest)->Result<String,PreEntryBinanceAuditError>{
    if signed.method!=HttpMethod::Get||signed.is_mutation{
        return Err(PreEntryBinanceAuditError::Invalid("pre-entry request not read-only"))
    }
    let res=exec.execute_request(&HttpsRequest::from_signed(signed))
        .map_err(PreEntryBinanceAuditError::Transport)?;
    if res.status!=200{return Err(PreEntryBinanceAuditError::Invalid("private GET rejected"))}
    Ok(res.body)
}
fn rows(s:&str)->Result<Vec<Value>,PreEntryBinanceAuditError>{
    let v:Value=serde_json::from_str(s)
        .map_err(|_|PreEntryBinanceAuditError::Invalid("Binance JSON malformed"))?;
    v.as_array().cloned().ok_or(PreEntryBinanceAuditError::Invalid("Binance array missing"))
}
fn txt<'a>(v:&'a Value,key:&str)->Result<&'a str,PreEntryBinanceAuditError>{
    v.get(key).and_then(Value::as_str)
        .ok_or(PreEntryBinanceAuditError::Invalid("Binance field absent or wrong type"))
}
fn u12(s:&str)->Result<u128,PreEntryBinanceAuditError>{
    decimal_to_atoms(s).map_err(|_|PreEntryBinanceAuditError::Invalid(
        "Binance decimal noncanonical/overflow"))
}
fn one_snapshot<E:HttpExecutor>(
    exec:&E,creds:&BinanceCredentials,now_ms:u64,recv_window_ms:u64,
)->Result<ReadonlyBinanceOpeningSnapshot,PreEntryBinanceAuditError>{
    let mode=get(exec,&build_binance_position_mode_request(creds,now_ms,recv_window_ms)
        .map_err(PreEntryBinanceAuditError::Build)?)?;
    if parse_binance_position_mode_http(200,&mode)!=BinancePositionMode::OneWay{
        return Err(PreEntryBinanceAuditError::Invalid("non-OneWay Binance position mode"))
    }
    let cfg=get(exec,&build_binance_account_config_request(creds,now_ms,recv_window_ms)
        .map_err(PreEntryBinanceAuditError::Build)?)?;
    let cfg=parse_binance_account_config(200,&cfg)
        .map_err(|_|PreEntryBinanceAuditError::Invalid("account config malformed"))?;
    if cfg.multi_assets_margin{
        return Err(PreEntryBinanceAuditError::Invalid("multi-assets margin not audited"))
    }
    let open=get(exec,&build_binance_open_orders_request(creds,now_ms,recv_window_ms)
        .map_err(PreEntryBinanceAuditError::Build)?)?;
    if !parse_binance_open_orders(200,&open)
        .map_err(|_|PreEntryBinanceAuditError::Invalid("open order shape invalid"))?.is_empty(){
        return Err(PreEntryBinanceAuditError::Invalid("Binance open orders before entry"))
    }
    let positions=get(exec,&build_binance_position_risk_v3_request(
        creds,now_ms,recv_window_ms).map_err(PreEntryBinanceAuditError::Build)?)?;
    for row in rows(&positions)?{
        if txt(&row,"positionSide")?!="BOTH"{
            return Err(PreEntryBinanceAuditError::Invalid("unexpected hedge-side position"))
        }
        let amt=txt(&row,"positionAmt")?;
        let abs=amt.strip_prefix('-').unwrap_or(amt);
        if u12(abs)?!=0{
            return Err(PreEntryBinanceAuditError::Invalid(
                "ANY Binance futures position nonflat before opening"))
        }
    }
    let balances=get(exec,&build_binance_balance_v3_request(
        creds,now_ms,recv_window_ms).map_err(PreEntryBinanceAuditError::Build)?)?;
    let usdt=rows(&balances)?.into_iter().filter(|v|
        v.get("asset").and_then(Value::as_str)==Some("USDT")).collect::<Vec<_>>();
    if usdt.len()!=1{return Err(PreEntryBinanceAuditError::Invalid(
        "missing or duplicate Binance USDT wallet"))}
    let wallet=u12(txt(&usdt[0],"balance")?)?;
    let available=u12(txt(&usdt[0],"availableBalance")?)?;
    let updated=usdt[0].get("updateTime").and_then(Value::as_u64)
        .or_else(||usdt[0].get("updateTime").and_then(Value::as_str)
            .and_then(|x|x.parse().ok()))
        .ok_or(PreEntryBinanceAuditError::Invalid("wallet update time missing"))?;
    // A flat futures account must have its quote wallet genuinely
    // unreserved in the original baseline; hidden reservations are not
    // understood by the current native micro-trading risk model.
    if wallet==0||available==0||available!=wallet
        ||updated==0||updated>now_ms.saturating_add(5_000)
        ||now_ms.saturating_sub(updated)>180_000 {
        return Err(PreEntryBinanceAuditError::Invalid(
            "Binance USDT wallet unavailable, stale or inconsistent"))
    }
    Ok(ReadonlyBinanceOpeningSnapshot{
        wallet_e12:wallet,available_e12:available,updated,
    })
}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum PreEntryBinanceWrite{Recorded,Idempotent}
impl NativeStateStore {
    /// Ten signed, mockable GETs (two 5-request account snapshots), not a
    /// POST.  The baseline MUST precede any native entry/position.
    pub fn audit_and_record_pre_entry_binance_baseline_once<E:HttpExecutor>(
        &self,exec:&E,expected_generation:u64,creds:&BinanceCredentials,
        now_ms:u64,recv_window_ms:u64,safety_generation:u64,
    )->Result<(NativeAuthoritativeState,PreEntryBinanceWrite),
        PreEntryBinanceAuditError>{
        if now_ms==0||safety_generation==0||recv_window_ms==0||recv_window_ms>60_000{
            return Err(PreEntryBinanceAuditError::Invalid("baseline request parameters invalid"))
        }
        let initial=self.load().map_err(PreEntryBinanceAuditError::State)?
            .ok_or(PreEntryBinanceAuditError::State(NativeStateError::Generation))?;
        if initial.generation!=expected_generation||initial.entry.is_some()
            ||initial.active.is_some()||initial.pre_entry_binance_baseline.is_some(){
            return Err(PreEntryBinanceAuditError::Invalid(
                "must capture opening wallet only before Native entry exists"))
        }
        let first=one_snapshot(exec,creds,now_ms,recv_window_ms)?;
        let second=one_snapshot(exec,creds,now_ms,recv_window_ms)?;
        if first!=second{
            return Err(PreEntryBinanceAuditError::Invalid(
                "Binance wallet changed between opening snapshots"))
        }
        let source=NativePreEntryBinanceBaseline{
            source_generation:expected_generation,captured_at_ms:now_ms,
            safety_generation,usdt_wallet_e12:first.wallet_e12,
            usdt_available_e12:first.available_e12,
            usdt_updated_at_ms:first.updated,snapshots:2,
            open_orders_zero:true,global_futures_positions_zero:true,
            one_way_mode:true,multi_asset_margin_disabled:true,
            entire_prior_account_history_proven:false,
            external_anti_rollback_proven:false,may_send_order:false,
            may_assume_funding_history_complete:false,
        };
        let (saved,_)=self.transact(expected_generation,|s|{
            if s.entry.is_some()||s.active.is_some()||s.pre_entry_binance_baseline.is_some(){
                return Err(deny("racing or late pre-entry wallet baseline"))
            }
            s.pre_entry_binance_baseline=Some(source);
            Ok(())
        }).map_err(PreEntryBinanceAuditError::State)?;
        Ok((saved,PreEntryBinanceWrite::Recorded))
    }
}
