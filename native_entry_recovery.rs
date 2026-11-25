//! R32D review-only native ENTRY recovery probe. Every external request is a
//! signed GET. It NEVER writes authoritative state or issues trading permits.
//! Cross-venue snapshots are not atomic and order status is not fee/fill truth.
use crate::authoritative_state::{
    NativeEntryLeg, NativeEntryRecoveryCheckpoint,NativeEntryRecoveryQuality,
    NativePendingEntryQuery, NativeStateError, NativeStateStore,
};
use crate::private_https::{HttpExecutor, HttpResponse, HttpsRequest, TransportError};
use crate::private_rest::{
    build_binance_account_config_request, build_binance_balance_v3_request,
    build_binance_open_orders_request, build_binance_position_mode_request,
    build_binance_position_risk_v3_request, build_binance_query_by_client_id,
    build_okx_account_config_request, build_okx_balance_request,
    build_okx_pending_spot_orders_request, build_okx_positions_request,
    build_okx_query_by_client_id, parse_binance_economic_truth_http,
    parse_binance_position_mode_http, parse_okx_economic_truth_http,
    BinanceCredentials, BinancePositionMode, EconomicTruthClass, HttpMethod,
    OkxCredentials, OkxDomain, OrderEconomicTruth, RestBuildError, SignedRequest,
};
use crate::private_truth::{
    parse_binance_account_config, parse_binance_balances, parse_binance_open_orders,
    parse_binance_positions, parse_okx_account_config, parse_okx_balances,
    parse_okx_pending_orders, parse_okx_positions, summarize, PrivateTruthError,
    PrivateTruthSummary,
};

#[derive(Debug, PartialEq, Eq)]
pub enum EntryProbeError {
    State(NativeStateError),
    Build(RestBuildError),
    Transport(TransportError),
    Truth(PrivateTruthError),
    ForbiddenNetworkShape,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EntryProbeVerdict {
    OrderUnverified,
    AccountConflict,
    OpenOrdersPresent,
    TerminalCandidateNeedsFillFeesAndInventoryTruth,
}

#[derive(Debug)]
pub struct EntryProbeReport {
    pub target: NativePendingEntryQuery,
    pub order: OrderEconomicTruth,
    pub account: PrivateTruthSummary,
    pub verdict: EntryProbeVerdict,
    /// Always false: no continuation can be authorized by this probe.
    pub may_send_order: bool,
    pub state_generation_unchanged: bool,
    /// Exact spot cash from a read-only account snapshot, e18 atoms.
    pub observed_spot_cash_atoms_e18: Option<u128>,
}

fn fetch<E: HttpExecutor>(
    executor: &E, signed: &SignedRequest,
) -> Result<HttpResponse, EntryProbeError> {
    if signed.method != HttpMethod::Get || signed.is_mutation {
        return Err(EntryProbeError::ForbiddenNetworkShape);
    }
    executor.execute_request(&HttpsRequest::from_signed(signed))
        .map_err(EntryProbeError::Transport)
}

fn atoms_e18(text: &str) -> Option<u128> {
    // Only nonnegative fixed decimals represent spot cash and absolute
    // OneWay futures exposure; reject scientific notation and overprecision.
    let atoms = crate::fill_economics::decimal_to_atoms(text).ok()?;
    atoms.checked_mul(1_000_000)
}

/// Astra R131: an overflow in previously confirmed exposure is UNKNOWN,
/// never a saturated invented holding or an order/retry permit.
fn checked_cumulative_exposure(confirmed:u128,increment:Option<u128>,
    apply_increment:bool)->Option<u128>{
    confirmed.checked_add(if apply_increment {increment?} else {0})
}
#[cfg(test)]
mod r131_checked_exposure_tests{
    use super::checked_cumulative_exposure;
    #[test]fn r131_overflow_is_unknown_not_saturated_cash_or_short(){
        assert_eq!(checked_cumulative_exposure(u128::MAX,Some(1),true),None);
        assert_eq!(checked_cumulative_exposure(u128::MAX-1,Some(2),true),None);
        assert_eq!(checked_cumulative_exposure(u128::MAX,Some(100),false),Some(u128::MAX));
    }
    #[test]fn r131_absent_order_quantity_never_assumed_zero(){
        assert_eq!(checked_cumulative_exposure(10,None,true),None);
        assert_eq!(checked_cumulative_exposure(10,None,false),Some(10));
        assert_eq!(checked_cumulative_exposure(10,Some(5),true),Some(15));
    }
}
/// R132: a terminal cancelled order with zero original fills is NOT a
/// candidate for four-leg economic reconciliation or owner recovery.
fn original_order_nonzero_bounded_qty(actual:Option<u128>,requested:u128)->bool{
    requested>0&&actual.is_some_and(|v|v>0&&v<=requested)
}
#[cfg(test)]
mod r132_terminal_zero_tests{
    use super::original_order_nonzero_bounded_qty as valid;
    #[test]fn r132_zero_or_missing_terminal_fill_is_not_a_recovery_candidate(){
        assert!(!valid(None,100));
        assert!(!valid(Some(0),100));
        assert!(!valid(Some(1),0));
        assert!(!valid(Some(101),100));
        assert!(valid(Some(1),100));
        assert!(valid(Some(100),100));
    }
}
fn normalized_futures_symbol(symbol: &str) -> String {
    symbol.split(':').next().unwrap_or(symbol)
        .replace('/', "").to_ascii_uppercase()
}

/// Exactly one signed order GET + nine read-only account GETs across both
/// venues; all ten responses must parse. No live read is invoked by tests.
#[allow(clippy::too_many_arguments)]
pub fn probe_native_entry_readonly_once<E: HttpExecutor>(
    executor: &E,
    store: &NativeStateStore,
    expected_generation: u64,
    okx: &OkxCredentials,
    binance: &BinanceCredentials,
    unix_ms: u64,
    okx_domain: OkxDomain,
    binance_recv_window_ms: u64,
) -> Result<EntryProbeReport, EntryProbeError> {
    let target = store.pending_native_entry_query(expected_generation)
        .map_err(EntryProbeError::State)?;
    // Build every request before fetching any remote data. Invalid signing
    // parameters must reject without a partial observation.
    let order_req = match target.leg {
        NativeEntryLeg::Spot => build_okx_query_by_client_id(
            okx, unix_ms, okx_domain, &target.symbol, &target.client_id),
        NativeEntryLeg::Futures => build_binance_query_by_client_id(
            binance, unix_ms, binance_recv_window_ms, &target.symbol, &target.client_id),
    }.map_err(EntryProbeError::Build)?;
    let bn_mode = build_binance_position_mode_request(
        binance, unix_ms, binance_recv_window_ms).map_err(EntryProbeError::Build)?;
    let bn_cfg = build_binance_account_config_request(
        binance, unix_ms, binance_recv_window_ms).map_err(EntryProbeError::Build)?;
    let bn_balance = build_binance_balance_v3_request(
        binance, unix_ms, binance_recv_window_ms).map_err(EntryProbeError::Build)?;
    let bn_positions = build_binance_position_risk_v3_request(
        binance, unix_ms, binance_recv_window_ms).map_err(EntryProbeError::Build)?;
    let bn_orders = build_binance_open_orders_request(
        binance, unix_ms, binance_recv_window_ms).map_err(EntryProbeError::Build)?;
    let ok_cfg = build_okx_account_config_request(
        okx, unix_ms, okx_domain).map_err(EntryProbeError::Build)?;
    let ok_balance = build_okx_balance_request(
        okx, unix_ms, okx_domain).map_err(EntryProbeError::Build)?;
    let ok_positions = build_okx_positions_request(
        okx, unix_ms, okx_domain).map_err(EntryProbeError::Build)?;
    let ok_orders = build_okx_pending_spot_orders_request(
        okx, unix_ms, okx_domain).map_err(EntryProbeError::Build)?;
    let order_http = fetch(executor, &order_req)?;
    let order = match target.leg {
        NativeEntryLeg::Spot => parse_okx_economic_truth_http(
            order_http.status, &order_http.body),
        NativeEntryLeg::Futures => parse_binance_economic_truth_http(
            order_http.status, &order_http.body),
    };
    let m = fetch(executor, &bn_mode)?;
    let mode = parse_binance_position_mode_http(m.status, &m.body);
    let c = fetch(executor, &bn_cfg)?;
    let bcfg = parse_binance_account_config(c.status, &c.body).map_err(EntryProbeError::Truth)?;
    let b = fetch(executor, &bn_balance)?;
    let bb = parse_binance_balances(b.status, &b.body).map_err(EntryProbeError::Truth)?;
    let p = fetch(executor, &bn_positions)?;
    let bp = parse_binance_positions(p.status, &p.body).map_err(EntryProbeError::Truth)?;
    let o = fetch(executor, &bn_orders)?;
    let bo = parse_binance_open_orders(o.status, &o.body).map_err(EntryProbeError::Truth)?;
    let c = fetch(executor, &ok_cfg)?;
    let ocfg = parse_okx_account_config(c.status, &c.body).map_err(EntryProbeError::Truth)?;
    let b = fetch(executor, &ok_balance)?;
    let ob = parse_okx_balances(b.status, &b.body).map_err(EntryProbeError::Truth)?;
    let p = fetch(executor, &ok_positions)?;
    let op = parse_okx_positions(p.status, &p.body).map_err(EntryProbeError::Truth)?;
    let o = fetch(executor, &ok_orders)?;
    let oo = parse_okx_pending_orders(o.status, &o.body).map_err(EntryProbeError::Truth)?;
    let account = summarize(mode, &bcfg, &bb, &bp, &bo, &ocfg, &ob, &op, &oo)
        .map_err(EntryProbeError::Truth)?;
    // Recheck the durable generation AFTER ten separate reads. This does not
    // make account truth atomic, but prevents handing out a stale report.
    let still = store.pending_native_entry_query(expected_generation)
        .map_err(EntryProbeError::State)?;
    if still != target { return Err(EntryProbeError::State(NativeStateError::Generation)) }
    let client_matches = order.client_order_id.as_deref() == Some(target.client_id.as_str());
    let order_identified = order.order_id.as_deref().map_or(false, |x| !x.is_empty());
    let qty_e18 = order.cumulative_qty_atoms.and_then(|v| v.checked_mul(1_000_000));
    let complete = order.class == EconomicTruthClass::Known && order.terminal
        && client_matches && order_identified
        && original_order_nonzero_bounded_qty(qty_e18,target.requested_atoms_e18)
        && matches!(order.status.as_deref(),
            Some("filled" | "canceled" | "cancelled" | "expired" | "mmp_canceled"));
    let futures_symbol = normalized_futures_symbol(&target.futures_symbol);
    let spot_currency = target.base.to_ascii_uppercase();
    let base_asset=ob.iter().find(|b| b.currency.eq_ignore_ascii_case(&spot_currency));
    let cash=base_asset.and_then(|b| atoms_e18(&b.cash_balance));
    let available=base_asset.and_then(|b| atoms_e18(&b.available_balance));
    let frozen=base_asset.and_then(|b| atoms_e18(&b.frozen_balance));
    let known_spot = checked_cumulative_exposure(
        target.confirmed_spot_atoms_e18,qty_e18,target.leg==NativeEntryLeg::Spot);
    // MICRO2: all of the original spot inventory must actually be
    // AVAILABLE, not merely included in a frozen cash balance. A matching
    // total alone can hide an untracked pending order or asset restriction.
    // Deny on malformed balances, missing available/frozen fields, or a
    // non-zero freeze. This is a prerequisite only, not full account truth.
    let base_ok=match (cash,available,frozen){
        (Some(c),Some(a),Some(f))=>f==0&&c==a
            &&c>=target.spot_baseline_atoms_e18
            &&known_spot.is_some()
            &&(known_spot==Some(0)||a>target.spot_baseline_atoms_e18),
        _=>false,
    };
    let fut_amt = bp.iter().filter(|x| x.symbol.eq_ignore_ascii_case(&futures_symbol))
        .collect::<Vec<_>>();
    let future_qty = checked_cumulative_exposure(
        target.confirmed_futures_atoms_e18,qty_e18,target.leg==NativeEntryLeg::Futures);
    let futures_ok = future_qty.is_some() && fut_amt.len() <= 1 && fut_amt.iter().all(|x| {
        if !x.position_side.eq_ignore_ascii_case("BOTH") { return false }
        let Some(raw) = x.position_amt.strip_prefix('-') else {
            return future_qty == Some(0) && atoms_e18(&x.position_amt) == Some(0)
        };
        // An unaccounted short position is itself a conflict. At the spot
        // entry leg native has not acknowledged any futures hedge yet.
        // Futures contracts have no base-asset fee deduction: the signed
        // OneWay position must match native-known + order-reported exposure
        // exactly. Larger shorts may belong to an unrelated order.
        atoms_e18(raw) == future_qty
    }) && (future_qty == Some(0) || fut_amt.len() == 1);
    let any_open = bo.iter().any(|x| x.client_order_id == target.client_id
        || x.symbol.eq_ignore_ascii_case(&futures_symbol))
        || oo.iter().any(|x| x.client_order_id == target.client_id
            || x.inst_id.eq_ignore_ascii_case(&target.spot_symbol.replace('/', "-")));
    let verdict = if mode != BinancePositionMode::OneWay {
        EntryProbeVerdict::AccountConflict
    } else if !complete {
        // Invalid/missing/overfilled order truth is never overridden by an
        // apparently consistent account snapshot.
        EntryProbeVerdict::OrderUnverified
    } else if !base_ok || !futures_ok {
        EntryProbeVerdict::AccountConflict
    } else if any_open {
        EntryProbeVerdict::OpenOrdersPresent
    } else {
        EntryProbeVerdict::TerminalCandidateNeedsFillFeesAndInventoryTruth
    };
    Ok(EntryProbeReport {
        target, order, account, verdict, may_send_order: false,
        state_generation_unchanged: true,
        observed_spot_cash_atoms_e18: cash,
    })
}


/// R32D2: second, independent, read-only query for trade-by-trade fees and fills.
/// Even a fully matched candidate is NOT an exchange-order send permission.
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum EntryFillVerdict {
    IncompleteFills,
    IncompleteFeeEvidence,
    NetSpotBalanceMismatch,
    PriorSpotFeeOriginUnverified,
    VerifiedCandidateStillBlocked,
}
#[derive(Debug)]
pub struct EntryFillDetailReport {
    pub fill_count:usize,
    pub filled_atoms_e18:u128,
    pub quoted_fee_cost_atoms_e12:i128,
    pub signed_base_fee_atoms_e18:Option<i128>,
    pub verdict:EntryFillVerdict,
    pub may_send_order:bool,
    // Constructed only by a complete read-only order/account/fills probe;
    // callers outside this crate cannot mint a durable proof.
    pub(crate) source_generation:u64,
    pub(crate) sealed_checkpoint:Option<NativeEntryRecoveryCheckpoint>,
}
fn native_entry_fill_base_fee(
    body:&str,expected_order_id:&str,base:&str,quote:&str,
    expected_count:usize,
)->Option<i128>{
    use crate::fill_economics::signed_decimal_to_atoms;
    let v:serde_json::Value=serde_json::from_str(body).ok()?;
    let rows=v.get("data")?.as_array()?;
    if rows.len()!=expected_count{return None}
    let mut total=0i128;
    for row in rows{
        if row.get("ordId")?.as_str()?!=expected_order_id{return None}
        let ccy=row.get("feeCcy")?.as_str()?;
        let fee=signed_decimal_to_atoms(row.get("fee")?.as_str()?).ok()?;
        if ccy.eq_ignore_ascii_case(base){
            total=total.checked_add(fee.checked_mul(1_000_000)?)?;
        }else if !ccy.eq_ignore_ascii_case(quote){return None}
    }
    Some(total)
}

/// The first R32D ten-GET account probe must be complete before this second
/// signed GET is built. A report obtained elsewhere is not sufficient for
/// any authoritative state update; R32D2 changes no native-state bytes.
#[allow(clippy::too_many_arguments)]
pub fn probe_native_entry_fills_readonly_once<E:HttpExecutor>(
    executor:&E,store:&NativeStateStore,
    report:&EntryProbeReport,
    okx:&OkxCredentials,binance:&BinanceCredentials,
    unix_ms:u64,okx_domain:OkxDomain,binance_recv_window_ms:u64,
)->Result<EntryFillDetailReport,EntryProbeError>{
    use crate::fill_economics::{
        LegFillLedger,DEFAULT_AVG_TOLERANCE_ATOMS,
    };
    use crate::private_rest::{
        build_okx_fills_history_request,build_binance_user_trades_request,
        parse_okx_fill_events_http,parse_binance_fill_events_http,
        apply_order_economic_truth,
    };
    let target=store.pending_native_entry_query(report.target.generation)
        .map_err(EntryProbeError::State)?;
    if target!=report.target || !report.state_generation_unchanged
        || report.may_send_order || report.verdict
            !=EntryProbeVerdict::TerminalCandidateNeedsFillFeesAndInventoryTruth{
        return Err(EntryProbeError::ForbiddenNetworkShape)
    }
    let order=&report.order;
    if order.class!=EconomicTruthClass::Known || !order.terminal
        || order.client_order_id.as_deref()!=Some(target.client_id.as_str()){
        return Err(EntryProbeError::ForbiddenNetworkShape)
    }
    let order_id=order.order_id.as_deref()
        .filter(|x|!x.is_empty()).ok_or(EntryProbeError::ForbiddenNetworkShape)?;
    let signed=match target.leg{
        NativeEntryLeg::Spot=>build_okx_fills_history_request(
            okx,unix_ms,okx_domain,&target.symbol,order_id),
        NativeEntryLeg::Futures=>build_binance_user_trades_request(
            binance,unix_ms,binance_recv_window_ms,&target.symbol,order_id),
    }.map_err(EntryProbeError::Build)?;
    let response=fetch(executor,&signed)?;
    let fills=match target.leg{
        NativeEntryLeg::Spot=>parse_okx_fill_events_http(
            response.status,&response.body,order_id,&target.base,"USDT"),
        NativeEntryLeg::Futures=>parse_binance_fill_events_http(
            response.status,&response.body,order_id,"USDT"),
    }.map_err(|_|EntryProbeError::ForbiddenNetworkShape)?;
    // An unrelated order in a supposedly order-filtered history is suspect.
    let parsed:serde_json::Value=serde_json::from_str(&response.body)
        .map_err(|_|EntryProbeError::ForbiddenNetworkShape)?;
    let rows=if target.leg==NativeEntryLeg::Spot{
        parsed.get("data").and_then(|v|v.as_array())
    }else{parsed.as_array()}
        .ok_or(EntryProbeError::ForbiddenNetworkShape)?;
    if rows.len()!=fills.len() || rows.len()>1000 {
        return Err(EntryProbeError::ForbiddenNetworkShape)
    }
    let request_e12=target.requested_atoms_e18
        .checked_div(1_000_000).ok_or(EntryProbeError::ForbiddenNetworkShape)?;
    if request_e12.checked_mul(1_000_000)!=Some(target.requested_atoms_e18){
        return Err(EntryProbeError::ForbiddenNetworkShape)
    }
    let mut ledger=LegFillLedger::new(
        if target.leg==NativeEntryLeg::Spot{"okx"}else{"binance"},
        &target.symbol,
        if target.leg==NativeEntryLeg::Spot{"buy"}else{"sell"},
        &target.intent_id,&target.client_id,request_e12,
    ).map_err(|_|EntryProbeError::ForbiddenNetworkShape)?;
    apply_order_economic_truth(&mut ledger,order)
        .map_err(|_|EntryProbeError::ForbiddenNetworkShape)?;
    let mut all_ids=std::collections::HashSet::new();
    for fill in fills.iter().cloned(){
        if !all_ids.insert(fill.fill_id.clone()){
            return Err(EntryProbeError::ForbiddenNetworkShape)
        }
        ledger.apply_fill(fill,DEFAULT_AVG_TOLERANCE_ATOMS)
            .map_err(|_|EntryProbeError::ForbiddenNetworkShape)?;
    }
    let actual=ledger.fills.values().try_fold(0u128,|sum,f|
        sum.checked_add(f.qty_atoms))
        .ok_or(EntryProbeError::ForbiddenNetworkShape)?;
    let fill_qty=actual.checked_mul(1_000_000)
        .ok_or(EntryProbeError::ForbiddenNetworkShape)?;
    let (base_fee,verdict)=if actual!=ledger.filled_qty_atoms(){
        (None,EntryFillVerdict::IncompleteFills)
    }else if !ledger.fee_evidence_complete{
        (None,EntryFillVerdict::IncompleteFeeEvidence)
    }else if target.leg==NativeEntryLeg::Spot{
        let fee=native_entry_fill_base_fee(
            &response.body,order_id,&target.base,"USDT",fills.len());
        if let Some(fee)=fee{
            let expected_net=i128::try_from(target.spot_baseline_atoms_e18)
                .ok().and_then(|n|n.checked_add(i128::try_from(fill_qty).ok()?))
                .and_then(|n|n.checked_add(fee));
            let actual_cash=report.observed_spot_cash_atoms_e18
                .and_then(|x|i128::try_from(x).ok());
            if expected_net.is_some() && expected_net==actual_cash{
                (Some(fee),EntryFillVerdict::VerifiedCandidateStillBlocked)
            }else{
                (Some(fee),EntryFillVerdict::NetSpotBalanceMismatch)
            }
        }else{(None,EntryFillVerdict::IncompleteFeeEvidence)}
    }else{
        // The preceding spot leg is already native-confirmed, but the
        // historical base-currency fees and original inventory provenance
        // have not yet been independently verified by THIS query.
        (None,EntryFillVerdict::PriorSpotFeeOriginUnverified)
    };
    let final_target=store.pending_native_entry_query(report.target.generation)
        .map_err(EntryProbeError::State)?;
    if final_target!=target{return Err(EntryProbeError::State(NativeStateError::Generation))}
    // Never promote/expose quantities. Only append an immutable diagnostic
    // checkpoint whose full per-trade ledger survives crashes/replays.
    let sealed_checkpoint=match verdict{
        EntryFillVerdict::VerifiedCandidateStillBlocked=>
            Some(NativeEntryRecoveryQuality::SpotCashMatched),
        EntryFillVerdict::PriorSpotFeeOriginUnverified=>
            Some(NativeEntryRecoveryQuality::FuturesPriorSpotUnverified),
        _=>None,
    }.map(|quality|NativeEntryRecoveryCheckpoint{
        leg:target.leg,intent_id:target.intent_id.clone(),
        client_id:target.client_id.clone(),order_id:order_id.into(),
        ledger:ledger.clone(),quality,
        observed_spot_cash_atoms_e18:report.observed_spot_cash_atoms_e18,
        signed_base_fee_atoms_e18:base_fee,
    });
    // Quantity, fees, and net-asset evidence can never grant a trading permit.
    Ok(EntryFillDetailReport{
        source_generation:target.generation,sealed_checkpoint,
        fill_count:ledger.fills.len(),
        filled_atoms_e18:fill_qty,
        quoted_fee_cost_atoms_e12:ledger.fee_cost_quote_atoms,
        signed_base_fee_atoms_e18:base_fee,
        verdict,may_send_order:false,
    })
}
