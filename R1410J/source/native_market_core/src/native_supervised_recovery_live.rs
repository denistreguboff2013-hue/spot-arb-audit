//! TRADE2B: two independent *fresh* full-account / original-order signed
//! GET brackets before even *considering* manual recovery. The result is
//! deliberately a NO-ORDER diagnostic, never an execution permit.
//! Exchange GET observations aren't atomic and do NOT attest new fills,
//! historic funding, cross-wallet transfers or real orderbook depth.
use crate::{
 authoritative_state::{NativeAuthoritativeState,NativeStateError,NativeStateStore},
 fill_economics::decimal_to_atoms,
 market_metadata::NativeMarketMetadata,
 native_supervised_recovery::{
     preview_supervised_recovery,RecoveryTriageError,SupervisedRecoveryTriage,
 },
 private_https::{HttpExecutor,HttpResponse,HttpsRequest,TransportError},
 private_rest::{
     BinanceCredentials,BinancePositionMode,EconomicTruthClass,
     HttpMethod,OkxCredentials,OkxDomain,RestBuildError,SignedRequest,
     build_binance_account_config_request,build_binance_balance_v3_request,
     build_binance_open_orders_request,build_binance_position_mode_request,
     build_binance_position_risk_v3_request,build_binance_query_by_client_id,
     build_okx_account_config_request,build_okx_balance_request,
     build_okx_pending_spot_orders_request,build_okx_positions_request,
     build_okx_query_by_client_id,parse_binance_position_mode_http,
     parse_binance_economic_truth_http,parse_okx_economic_truth_http,
 },
 private_truth::{
     parse_binance_account_config,parse_binance_open_orders,
     parse_okx_account_config,parse_okx_pending_orders,parse_okx_positions,
     PrivateTruthError,
 },
};
use serde_json::Value;
const BALANCE_AGE_MAX_MS:u64=60_000;
const CLOCK_FUTURE_TOLERANCE_MS:u64=5_000;

#[derive(Clone,Debug,PartialEq,Eq)]
pub enum FreshRecoveryError{
    Source(RecoveryTriageError),State(NativeStateError),
    Build(RestBuildError),Transport(TransportError),
    Truth(PrivateTruthError),Invalid(&'static str),
}
#[derive(Clone,Debug,PartialEq,Eq)]
struct RecoveryBracket{
    original_spot_order_qty_e12:u128,
    original_futures_order_qty_e12:u128,
    spot_available_total_e12:u128,
    spot_available_net_e12:u128,
    futures_short_e12:u128,
    futures_wallet_e12:u128,
    futures_available_e12:u128,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct FreshSupervisedRecoveryReview {
    pub previous:SupervisedRecoveryTriage,
    pub read_only_signed_gets:u8,
    pub independent_account_brackets:u8,
    pub original_spot_order_filled_e12:u128,
    pub original_futures_order_filled_e12:u128,
    pub observed_net_available_spot_e12:u128,
    pub observed_futures_short_e12:u128,
    pub observed_futures_wallet_usdt_e12:u128,
    pub original_orders_terminal:bool,
    pub no_open_binance_or_spot_okx_orders_in_two_brackets:bool,
    pub all_snapshots_consistent:bool,
    pub new_fills_since_original_audit_proven_absent:bool,
    pub cross_venue_atomicity_proven:bool,
    pub real_orderbook_liquidity_proven:bool,
    pub may_send_order:bool,
    pub may_auto_hedge:bool,
    pub may_auto_unwind:bool,
    pub may_verify_closed:bool,
    pub may_release_cooldown:bool,
}
fn invalid(x:&'static str)->FreshRecoveryError{FreshRecoveryError::Invalid(x)}
fn fixed(text:&str)->Result<u128,FreshRecoveryError>{
    decimal_to_atoms(text).map_err(|_|invalid("amount is missing, negative or imprecise"))
}
fn field<'a>(v:&'a Value,name:&str)->Result<&'a str,FreshRecoveryError>{
    v.get(name).and_then(Value::as_str).ok_or(invalid("required field absent"))
}
fn date(v:&Value,key:&str,now:u64)->Result<u64,FreshRecoveryError>{
    let raw=v.get(key).ok_or(invalid("account timestamp absent"))?;
    let ts=raw.as_u64().or_else(||raw.as_str().and_then(|s|s.parse().ok()))
        .ok_or(invalid("account timestamp malformed"))?;
    if ts==0||ts>now.saturating_add(CLOCK_FUTURE_TOLERANCE_MS)
        ||now.saturating_sub(ts)>BALANCE_AGE_MAX_MS{
        return Err(invalid("stale or future account timestamp"))
    }
    Ok(ts)
}
fn json<E:HttpExecutor>(
    http:&E,req:&SignedRequest,
)->Result<(u16,String),FreshRecoveryError>{
    if req.method!=HttpMethod::Get||req.is_mutation||req.body.is_some(){
        return Err(invalid("mutation/non-GET forbidden in recovery audit"))
    }
    let HttpResponse{status,body,..}=http.execute_request(&HttpsRequest::from_signed(req))
        .map_err(FreshRecoveryError::Transport)?;
    if status!=200{return Err(invalid("private signed GET HTTP failure"))}
    Ok((status,body))
}
fn array(raw:&str)->Result<Vec<Value>,FreshRecoveryError>{
    serde_json::from_str::<Value>(raw).map_err(|_|invalid("JSON not parseable"))?
        .as_array().cloned().ok_or(invalid("expected account array"))
}
fn positive_position(s:&str)->Result<(bool,u128),FreshRecoveryError>{
    if let Some(mag)=s.strip_prefix('-'){
        return Ok((true,fixed(mag)?))
    }
    Ok((false,fixed(s)?))
}
fn bracket<E:HttpExecutor>(
    http:&E,s:&NativeAuthoritativeState,
    okx:&OkxCredentials,bn:&BinanceCredentials,
    now:u64,domain:OkxDomain,recv:u64,
)->Result<RecoveryBracket,FreshRecoveryError>{
    let e=s.entry.as_ref().ok_or(invalid("entry disappeared"))?;
    let spot=e.spot_prehedge_evidence.as_ref().ok_or(invalid("spot fills missing"))?;
    let cp=e.recovery_checkpoint.as_ref().ok_or(invalid("futures fills missing"))?;
    let c6=e.prospective_joint_evidence.as_ref().ok_or(invalid("C6 source missing"))?;
    let requests=[
        build_binance_query_by_client_id(bn,now,recv,&e.plan.futures_symbol,&cp.client_id)
            .map_err(FreshRecoveryError::Build)?,
        build_okx_query_by_client_id(okx,now,domain,&e.plan.spot_symbol,&spot.spot_client_id)
            .map_err(FreshRecoveryError::Build)?,
        build_binance_position_mode_request(bn,now,recv)
            .map_err(FreshRecoveryError::Build)?,
        build_binance_account_config_request(bn,now,recv)
            .map_err(FreshRecoveryError::Build)?,
        build_binance_open_orders_request(bn,now,recv)
            .map_err(FreshRecoveryError::Build)?,
        build_binance_position_risk_v3_request(bn,now,recv)
            .map_err(FreshRecoveryError::Build)?,
        build_binance_balance_v3_request(bn,now,recv)
            .map_err(FreshRecoveryError::Build)?,
        build_okx_account_config_request(okx,now,domain)
            .map_err(FreshRecoveryError::Build)?,
        build_okx_pending_spot_orders_request(okx,now,domain)
            .map_err(FreshRecoveryError::Build)?,
        build_okx_positions_request(okx,now,domain)
            .map_err(FreshRecoveryError::Build)?,
        build_okx_balance_request(okx,now,domain)
            .map_err(FreshRecoveryError::Build)?,
    ];
    // Order-specific status is *not* fill+fee history. A delayed fill
    // must also change executedQty: reject versus immutable original cp.
    let (code,body)=json(http,&requests[0])?;
    let original= parse_binance_economic_truth_http(code,&body);
    if original.class!=EconomicTruthClass::Known||!original.terminal
        ||original.client_order_id.as_deref()!=Some(cp.client_id.as_str())
        ||original.order_id.as_deref()!=Some(cp.order_id.as_str())
        ||original.cumulative_qty_atoms!=Some(cp.ledger.filled_qty_atoms()){
        return Err(invalid("Binance original order changed or is still pending"))
    }
    let (code,body)=json(http,&requests[1])?;
    let prior=parse_okx_economic_truth_http(code,&body);
    if prior.class!=EconomicTruthClass::Known||!prior.terminal
        ||prior.client_order_id.as_deref()!=Some(spot.spot_client_id.as_str())
        ||prior.order_id.as_deref()!=Some(spot.spot_order_id.as_str())
        ||prior.cumulative_qty_atoms!=Some(spot.exact_ledger.filled_qty_atoms()){
        return Err(invalid("OKX original order changed or is still pending"))
    }
    let (code,body)=json(http,&requests[2])?;
    if parse_binance_position_mode_http(code,&body)!=BinancePositionMode::OneWay{
        return Err(invalid("Binance non-OneWay position mode"))
    }
    let (code,body)=json(http,&requests[3])?;
    let cfg=parse_binance_account_config(code,&body).map_err(FreshRecoveryError::Truth)?;
    if cfg.multi_assets_margin{
        return Err(invalid("Binance margin profile changed"))
    }
    let (code,body)=json(http,&requests[4])?;
    if !parse_binance_open_orders(code,&body)
        .map_err(FreshRecoveryError::Truth)?.is_empty(){
        return Err(invalid("ANY Binance futures open order exists"))
    }
    let (_,body)=json(http,&requests[5])?;
    let mut short=None;
    for row in array(&body)? {
        let name=field(&row,"symbol")?;
        if field(&row,"positionSide")?!="BOTH"{
            return Err(invalid("Binance hedge-mode or unknown position side"))
        }
        let (neg,q)=positive_position(field(&row,"positionAmt")?)?;
        if name==e.plan.futures_symbol {
            if short.is_some()||!neg||q!=cp.ledger.filled_qty_atoms()
                ||field(&row,"marginAsset")?!="USDT"{
                return Err(invalid("original Binance short mismatch"))
            }
            date(&row,"updateTime",now)?;
            short=Some(q);
        }else if q!=0{return Err(invalid("foreign active Binance futures position"))}
    }
    let short=short.ok_or(invalid("original Binance position absent"))?;
    let (_,body)=json(http,&requests[6])?;
    let mut wallet=None;
    for row in array(&body)?{
        if field(&row,"asset")?!="USDT"{continue}
        if wallet.is_some(){return Err(invalid("ambiguous Binance USDT wallet"))}
        date(&row,"updateTime",now)?;
        let total=fixed(field(&row,"balance")?)?;
        let avail=fixed(field(&row,"availableBalance")?)?;
        if avail>total||total==0{return Err(invalid("Binance wallet balance invalid"))}
        wallet=Some((total,avail))
    }
    let (wallet,free_wallet)=wallet.ok_or(invalid("Binance USDT wallet missing"))?;
    if wallet.checked_mul(1_000_000)
        .ok_or(invalid("Binance wallet overflow"))?
        !=c6.futures_wallet_usdt_e18 {
        return Err(invalid("Binance wallet moved since original sealed account audit"))
    }
    let (code,body)=json(http,&requests[7])?;
    let cfg=parse_okx_account_config(code,&body).map_err(FreshRecoveryError::Truth)?;
    if cfg.account_level!="2"||cfg.position_mode!="net_mode"{
        return Err(invalid("OKX account mode changed"))
    }
    let (code,body)=json(http,&requests[8])?;
    if !parse_okx_pending_orders(code,&body)
        .map_err(FreshRecoveryError::Truth)?.is_empty(){
        return Err(invalid("ANY OKX spot order still pending"))
    }
    let (code,body)=json(http,&requests[9])?;
    for p in parse_okx_positions(code,&body).map_err(FreshRecoveryError::Truth)?{
        if fixed(p.position.strip_prefix('-').unwrap_or(&p.position))?!=0{
            return Err(invalid("nonzero unrelated OKX derivatives position"))
        }
    }
    let (_,body)=json(http,&requests[10])?;
    let root:Value=serde_json::from_str(&body).map_err(|_|invalid("OKX balance JSON"))?;
    if root.get("code").and_then(Value::as_str)!=Some("0"){
        return Err(invalid("OKX balance request business error"))
    }
    let roots=root.get("data").and_then(Value::as_array)
        .ok_or(invalid("OKX balance envelope absent"))?;
    if roots.len()!=1{return Err(invalid("OKX balance multiple roots"))}
    let rows=roots[0].get("details").and_then(Value::as_array)
        .ok_or(invalid("OKX balance details absent"))?;
    let mut observed=None;
    for row in rows{
        if field(row,"ccy")?!=e.plan.base{continue}
        if observed.is_some(){return Err(invalid("duplicate OKX base balance"))}
        date(row,"uTime",now)?;
        let cash=fixed(field(row,"cashBal")?)?;
        let avail=fixed(field(row,"availBal")?)?;
        let frozen=fixed(field(row,"frozenBal")?)?;
        if frozen!=0||avail!=cash||avail.checked_add(frozen)!=Some(cash){
            return Err(invalid("original OKX asset frozen or unavailable"))
        }
        let base=e.plan.spot_baseline_atoms_e18;
        if base%1_000_000!=0{return Err(invalid("spot opening baseline precision"))}
        let net=avail.checked_sub(base/1_000_000)
            .ok_or(invalid("OKX available cash below original baseline"))?;
        if net.checked_mul(1_000_000)
            .ok_or(invalid("OKX net cash overflow"))?
            !=c6.spot_available_net_atoms_e18{
            return Err(invalid("OKX available spot moved since sealed opening"))
        }
        observed=Some((avail,net))
    }
    let (available,net)=observed.ok_or(invalid("OKX base wallet missing"))?;
    Ok(RecoveryBracket{
        original_spot_order_qty_e12:prior.cumulative_qty_atoms.unwrap(),
        original_futures_order_qty_e12:original.cumulative_qty_atoms.unwrap(),
        spot_available_total_e12:available,spot_available_net_e12:net,
        futures_short_e12:short,futures_wallet_e12:wallet,
        futures_available_e12:free_wallet,
    })
}
/// Exactly 22 signed GETs on a successful fixture (two 11-GET brackets).
/// Current state must remain identical across the entire observation.
/// This is NOT a proof of simultaneous snapshots and DOES NOT issue a permit.
#[allow(clippy::too_many_arguments)]
pub fn audit_fresh_supervised_recovery_once<E:HttpExecutor>(
    http:&E,store:&NativeStateStore,expected_generation:u64,
    market:&NativeMarketMetadata,spot_bid_e12:u128,
    okx:&OkxCredentials,bn:&BinanceCredentials,
    now_ms:u64,domain:OkxDomain,recv_window_ms:u64,
)->Result<FreshSupervisedRecoveryReview,FreshRecoveryError>{
    let previous=preview_supervised_recovery(
        store,expected_generation,market,now_ms,spot_bid_e12)
        .map_err(FreshRecoveryError::Source)?;
    if recv_window_ms==0||recv_window_ms>10_000{
        return Err(invalid("invalid Binance signed GET recvWindow"))
    }
    let state=store.load().map_err(FreshRecoveryError::State)?
        .ok_or(FreshRecoveryError::State(NativeStateError::Generation))?;
    if state.generation!=expected_generation{
        return Err(FreshRecoveryError::State(NativeStateError::Generation))
    }
    let first=bracket(http,&state,okx,bn,now_ms,domain,recv_window_ms)?;
    let second=bracket(http,&state,okx,bn,now_ms,domain,recv_window_ms)?;
    if first!=second{
        return Err(invalid("live account/order changed between recovery brackets"))
    }
    if store.load().map_err(FreshRecoveryError::State)?.as_ref()!=Some(&state){
        return Err(FreshRecoveryError::State(NativeStateError::Generation))
    }
    Ok(FreshSupervisedRecoveryReview{
        previous,read_only_signed_gets:22,independent_account_brackets:2,
        original_spot_order_filled_e12:first.original_spot_order_qty_e12,
        original_futures_order_filled_e12:first.original_futures_order_qty_e12,
        observed_net_available_spot_e12:first.spot_available_net_e12,
        observed_futures_short_e12:first.futures_short_e12,
        observed_futures_wallet_usdt_e12:first.futures_wallet_e12,
        original_orders_terminal:true,
        no_open_binance_or_spot_okx_orders_in_two_brackets:true,
        all_snapshots_consistent:true,
        new_fills_since_original_audit_proven_absent:false,
        cross_venue_atomicity_proven:false,real_orderbook_liquidity_proven:false,
        may_send_order:false,may_auto_hedge:false,may_auto_unwind:false,
        may_verify_closed:false,may_release_cooldown:false,
    })
}
