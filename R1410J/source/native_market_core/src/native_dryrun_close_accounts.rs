//! TRADE2D2C2: two *SIMULATED* independently sampled account-flatness
//! brackets AFTER exact order-status / per-trade fee proof from C1.
use crate::{
 authoritative_state::{NativeStateStore,NativeStateError},
 fill_economics::decimal_to_atoms,
 native_dryrun_close_fills::{
     inspect_mock_full_close_trades,MockCloseTradeReceipts,MockCloseFillsError,
 },
 private_https::{HttpExecutor,HttpResponse,HttpsRequest,TransportError},
 private_rest::{
     BinanceCredentials,OkxCredentials,OkxDomain,RestBuildError,
     SignedRequest,HttpMethod,
     build_binance_position_risk_v3_request,
     build_binance_open_orders_request,build_binance_balance_v3_request,
     build_okx_pending_spot_orders_request,build_okx_positions_request,
     build_okx_balance_request,
 },
 private_truth::{parse_binance_open_orders,parse_okx_pending_orders,parse_okx_positions},
};
use serde_json::Value;
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum MockFinalAccountError{
    Close(MockCloseFillsError),State(NativeStateError),
    Build(RestBuildError),Transport(TransportError),
    Invalid(&'static str),
}
fn deny(x:&'static str)->MockFinalAccountError{MockFinalAccountError::Invalid(x)}
#[derive(Clone,Debug,PartialEq,Eq)]
struct AccountWitness{
    available_base_e12:u128,
    usdt_wallet_e12:u128,usdt_available_e12:u128,
    positions_nonzero:usize,
    normal_open_orders:usize,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct MockFinalAccountReview{
    pub close_trades:MockCloseTradeReceipts,
    pub total_signed_gets:u8,
    pub independent_post_close_account_brackets:u8,
    /// R135: two mock GET rounds do not prove distinct exchange-side update epochs.
    pub distinct_exchange_update_epochs_proven:bool,
    pub observed_okx_base_available_e12:u128,
    pub observed_binance_usdt_wallet_e12:u128,
    pub synthetic_regular_open_orders_zero:bool,
    pub synthetic_futures_and_okx_derivatives_positions_zero:bool,
    pub synthetic_okx_base_restored_to_pre_entry_e12:bool,
    pub synthetic_binance_wallet_delta_matches_close_fees_and_pnl:bool,
    pub actual_signed_live_close_proven:bool,
    pub all_conditional_and_algo_orders_exhausted:bool,
    pub historical_funding_and_transfers_exhausted:bool,
    pub whole_cycle_realized_net_pnl_proven:bool,
    pub no_post_observation_delayed_execution_proven:bool,
    pub independent_external_antirollback_proven:bool,
    pub may_retry_or_cancel:bool,
    pub may_verify_closed:bool,
}
fn val<'a>(x:&'a Value,k:&str)->Result<&'a str,MockFinalAccountError>{
    x.get(k).and_then(Value::as_str).ok_or(deny("account field missing"))
}
fn parse_ts(x:&Value,k:&str,now:u64)->Result<(),MockFinalAccountError>{
    let f=x.get(k).ok_or(deny("account timestamp missing"))?;
    let t=f.as_u64().or_else(||f.as_str().and_then(|s|s.parse::<u64>().ok()))
        .ok_or(deny("account timestamp malformed"))?;
    if now==0||t==0||t>now.saturating_add(5_000)
        ||now.saturating_sub(t)>10_000{
        return Err(deny("account timestamp not fresh"))
    }
    Ok(())
}
fn fixed(s:&str)->Result<u128,MockFinalAccountError>{
    decimal_to_atoms(s).map_err(|_|deny("invalid unsigned account amount"))
}
fn zero_position(s:&str)->Result<bool,MockFinalAccountError>{
    let a=s.strip_prefix('-').unwrap_or(s);
    Ok(fixed(a)?==0)
}
fn get<E:HttpExecutor>(h:&E,r:&SignedRequest)->Result<String,MockFinalAccountError>{
    if r.method!=HttpMethod::Get||r.is_mutation||r.body.is_some(){
        return Err(deny("non-GET forbidden in final close account review"))
    }
    let HttpResponse{status,body,..}=h.execute_request(&HttpsRequest::from_signed(r))
        .map_err(MockFinalAccountError::Transport)?;
    if status!=200{return Err(deny("signed account GET failed"))}
    Ok(body)
}
fn account<E:HttpExecutor>(
    http:&E,okx:&OkxCredentials,bn:&BinanceCredentials,
    now:u64,dom:OkxDomain,recv:u64,
    base:&str,symbol:&str,baseline_e12:u128,
    expected_wallet_e12:u128,
)->Result<AccountWitness,MockFinalAccountError>{
    let r=build_binance_open_orders_request(bn,now,recv)
        .map_err(MockFinalAccountError::Build)?;
    let raw=get(http,&r)?;
    if !parse_binance_open_orders(200,&raw)
        .map_err(|_|deny("Binance open orders invalid"))?.is_empty(){
        return Err(deny("Binance regular open order after mock close"))
    }
    let r=build_binance_position_risk_v3_request(bn,now,recv)
        .map_err(MockFinalAccountError::Build)?;
    let raw=get(http,&r)?;
    let v:Value=serde_json::from_str(&raw).map_err(|_|deny("Binance position JSON"))?;
    let rows=v.as_array().ok_or(deny("Binance position risk array missing"))?;
    let mut symbol_seen=false;
    for row in rows{
        if val(row,"positionSide")?!="BOTH"{
            return Err(deny("Binance not OneWay/BOTH"))
        }
        let sym=val(row,"symbol")?;
        if sym==symbol{
            if symbol_seen||val(row,"marginAsset")?!="USDT"{
                return Err(deny("Binance original symbol duplicate or margin changed"))
            }
            parse_ts(row,"updateTime",now)?;
            symbol_seen=true;
        }
        if !zero_position(val(row,"positionAmt")?)?{
            return Err(deny("nonzero Binance futures position"))
        }
    }
    if !symbol_seen{return Err(deny("original Binance symbol missing"))}
    let r=build_binance_balance_v3_request(bn,now,recv)
        .map_err(MockFinalAccountError::Build)?;
    let raw=get(http,&r)?;
    let v:Value=serde_json::from_str(&raw).map_err(|_|deny("Binance wallet JSON"))?;
    let rows=v.as_array().ok_or(deny("Binance wallet array missing"))?;
    let mut wallet=None;
    for row in rows{
        if val(row,"asset")?!="USDT"{continue}
        if wallet.is_some(){return Err(deny("duplicate Binance USDT wallet"))}
        parse_ts(row,"updateTime",now)?;
        let w=fixed(val(row,"balance")?)?;
        let available=fixed(val(row,"availableBalance")?)?;
        if w!=expected_wallet_e12||available!=w{
            return Err(deny("Binance wallet delta or free balance unexplained"))
        }
        wallet=Some((w,available));
    }
    let (wallet,available)=wallet.ok_or(deny("USDT Binance wallet missing"))?;
    let r=build_okx_pending_spot_orders_request(okx,now,dom)
        .map_err(MockFinalAccountError::Build)?;
    let raw=get(http,&r)?;
    if !parse_okx_pending_orders(200,&raw)
        .map_err(|_|deny("OKX pending spot invalid"))?.is_empty(){
        return Err(deny("OKX spot order remains pending"))
    }
    let r=build_okx_positions_request(okx,now,dom)
        .map_err(MockFinalAccountError::Build)?;
    let raw=get(http,&r)?;
    for position in parse_okx_positions(200,&raw)
        .map_err(|_|deny("OKX derivatives positions malformed"))?{
        if !zero_position(&position.position)?{
            return Err(deny("OKX derivative position not flat"))
        }
    }
    let r=build_okx_balance_request(okx,now,dom)
        .map_err(MockFinalAccountError::Build)?;
    let raw=get(http,&r)?;
    let v:Value=serde_json::from_str(&raw).map_err(|_|deny("OKX wallet JSON"))?;
    if v.get("code").and_then(Value::as_str)!=Some("0"){
        return Err(deny("OKX business error"))
    }
    let roots=v.get("data").and_then(Value::as_array)
        .ok_or(deny("OKX wallet root absent"))?;
    if roots.len()!=1{return Err(deny("OKX wallet ambiguous"))}
    let rows=roots[0].get("details").and_then(Value::as_array)
        .ok_or(deny("OKX wallet details missing"))?;
    let mut balance=None;
    for row in rows{
        if val(row,"ccy")?!=base{continue}
        if balance.is_some(){return Err(deny("duplicate OKX base asset"))}
        parse_ts(row,"uTime",now)?;
        let cash=fixed(val(row,"cashBal")?)?;
        let free=fixed(val(row,"availBal")?)?;
        let frozen=fixed(val(row,"frozenBal")?)?;
        if cash!=baseline_e12||free!=cash||frozen!=0{
            return Err(deny("OKX base not restored to original baseline"))
        }
        balance=Some(free);
    }
    Ok(AccountWitness{
        available_base_e12:balance.ok_or(deny("OKX base balance missing"))?,
        usdt_wallet_e12:wallet,usdt_available_e12:available,
        positions_nonzero:0,normal_open_orders:0,
    })
}

/// Signed GET-only synthetic bounded diagnostic. C1 exact close trade
/// history + TWO final, independent exchange-wide REGULAR order and
/// position/account snapshots. This is NOT a VerifiedClosed permit.
/// Funding/transfer history, quote wallet and conditional/algo orders
/// remain incomplete even when the numerical wallet delta reconciles.
#[allow(clippy::too_many_arguments)]
pub fn inspect_mock_full_close_accounts<E:HttpExecutor>(
    http:&E,store:&NativeStateStore,expected_generation:u64,
    okx:&OkxCredentials,bn:&BinanceCredentials,
    now:u64,domain:OkxDomain,recv:u64,
)->Result<MockFinalAccountReview,MockFinalAccountError>{
    let receipts=inspect_mock_full_close_trades(
        http,store,expected_generation,okx,bn,now,domain,recv)
        .map_err(MockFinalAccountError::Close)?;
    if !receipts.complete_bounded_order_fill_scan_twice
        ||receipts.may_verify_closed||receipts.may_send_or_retry_order{
        return Err(deny("invalid mock close trade receipt authority"))
    }
    let state=store.load().map_err(MockFinalAccountError::State)?
        .ok_or(MockFinalAccountError::State(NativeStateError::Generation))?;
    if state.generation!=expected_generation||state.active.is_some(){
        return Err(deny("Native state changed before flatness snapshots"))
    }
    let entry=state.entry.as_ref().ok_or(deny("native original entry missing"))?;
    let witness=entry.prospective_joint_evidence.as_ref()
        .ok_or(deny("pre-close independent account baseline absent"))?;
    if entry.plan.spot_baseline_atoms_e18%1_000_000!=0
        ||witness.futures_wallet_usdt_e18%1_000_000!=0{
        return Err(deny("source account baseline greater than e12 precision"))
    }
    let base_e12=entry.plan.spot_baseline_atoms_e18/1_000_000;
    let futures_base=i128::try_from(witness.futures_wallet_usdt_e18/1_000_000)
        .map_err(|_|deny("baseline futures wallet overflow"))?;
    let expected=futures_base
        .checked_add(receipts.futures_reported_close_realized_pnl_usdt_e12)
        .and_then(|v|v.checked_sub(receipts.futures_fee_usdt_e12))
        .ok_or(deny("expected futures USDT wallet under/overflow"))?;
    let expected_e12=u128::try_from(expected)
        .map_err(|_|deny("negative expected wallet"))?;
    let first=account(
        http,okx,bn,now,domain,recv,
        &entry.plan.base,&entry.plan.futures_symbol,
        base_e12,expected_e12)?;
    let second=account(
        http,okx,bn,now,domain,recv,
        &entry.plan.base,&entry.plan.futures_symbol,
        base_e12,expected_e12)?;
    if first!=second{return Err(deny("post-close accounts changed between GET rounds"))}
    if store.load().map_err(MockFinalAccountError::State)?.as_ref()!=Some(&state){
        return Err(deny("Native owner changed during final account audit"))
    }
    if first.positions_nonzero!=0||first.normal_open_orders!=0
        ||first.usdt_available_e12!=first.usdt_wallet_e12{
        return Err(deny("impossible final witness"))
    }
    Ok(MockFinalAccountReview{
        close_trades:receipts,total_signed_gets:28,
        independent_post_close_account_brackets:2,
        distinct_exchange_update_epochs_proven:false,
        observed_okx_base_available_e12:first.available_base_e12,
        observed_binance_usdt_wallet_e12:first.usdt_wallet_e12,
        synthetic_regular_open_orders_zero:true,
        synthetic_futures_and_okx_derivatives_positions_zero:true,
        synthetic_okx_base_restored_to_pre_entry_e12:true,
        synthetic_binance_wallet_delta_matches_close_fees_and_pnl:true,
        actual_signed_live_close_proven:false,
        all_conditional_and_algo_orders_exhausted:false,
        historical_funding_and_transfers_exhausted:false,
        whole_cycle_realized_net_pnl_proven:false,
        no_post_observation_delayed_execution_proven:false,
        independent_external_antirollback_proven:false,
        may_retry_or_cancel:false,may_verify_closed:false,
    })
}
