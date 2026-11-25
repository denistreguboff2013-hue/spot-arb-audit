//! R32D6C6: bounded, read-only prospective receipt chain OKX -> Binance.
//! This is deliberately NOT a live trade release or a cross-venue atomic
//! account snapshot. No native Hedged, no order permits, no mutation transport.
use serde::{Serialize,Deserialize};
use serde_json::Value;
use crate::authoritative_state::{
    NativeStateStore,NativeStateError,NativeEntryPhase,NativeEntryLeg,
    NativeEntryRecoveryQuality,
};
use crate::fill_economics::decimal_to_atoms;
use crate::private_https::{HttpExecutor,HttpsRequest,TransportError};
use crate::private_rest::{
    OkxCredentials,OkxDomain,BinanceCredentials,RestBuildError,
    HttpMethod,SignedRequest,build_okx_balance_request,
};
use crate::native_binance_lineage::{audit_binance_futures_lineage_once,BinanceLineageError};
use crate::native_futures_trade_scope::{
    audit_binance_symbol_wide_trade_scope,FuturesTradeScopeError,
};
const SOURCE_MAX_MS:u64=60_000;
const FUTURE_ALLOW_MS:u64=5_000;

#[derive(Clone,Debug,PartialEq,Eq)]
pub enum ProspectiveJointError{
    State(NativeStateError),Build(RestBuildError),Transport(TransportError),
    Income(BinanceLineageError),Trades(FuturesTradeScopeError),
    Invalid(&'static str),
}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
pub struct NativeProspectiveJointEvidence{
    pub source_generation:u64,
    pub captured_at_ms:u64,
    pub safety_generation:u64,
    pub spot_first_source_generation:u64,
    pub spot_bills_source_generation:u64,
    pub futures_window_source_generation:u64,
    pub spot_intent_id:String,
    pub spot_client_id:String,
    pub spot_order_id:String,
    pub spot_trade_ids:Vec<String>,
    pub spot_bill_ids:Vec<String>,
    pub spot_gross_atoms_e18:u128,
    pub spot_available_net_atoms_e18:u128,
    pub spot_fee_quote_atoms_e12:i128,
    pub spot_cash_e18:u128,
    pub spot_cash_updated_at_ms:u64,
    pub futures_intent_id:String,
    pub futures_client_id:String,
    pub futures_order_id:String,
    pub futures_trade_ids:Vec<String>,
    pub futures_short_atoms_e18:u128,
    pub futures_fee_quote_atoms_e12:i128,
    pub futures_wallet_usdt_e18:u128,
    pub futures_position_updated_at_ms:u64,
    pub futures_wallet_updated_at_ms:u64,
    pub exposed_spot_unhedged_atoms_e18:u128,
    pub okx_balance_bracket_matched:bool,
    pub binance_income_and_position_bracket_matched:bool,
    pub foreign_symbol_trades_absent_in_bounded_window:bool,
    pub cross_venue_exchange_snapshot_atomicity_proven:bool,
    pub exchange_global_history_complete:bool,
    pub may_send_order:bool,
    pub may_promote_hedged:bool,
}
#[derive(Clone,Debug)]
pub struct ProspectiveJointReport{
    pub source_generation:u64,
    pub spot_fill_count:usize,
    pub futures_fill_count:usize,
    pub okx_balances_read:u8,
    pub binance_account_checks:u8,
    pub spot_unhedged_atoms_e18:u128,
    pub candidate_bound_to_same_native_entry:bool,
    pub may_send_order:bool,
    pub may_promote_hedged:bool,
    pub(crate) sealed:Option<NativeProspectiveJointEvidence>,
}
fn selected_cash<E:HttpExecutor>(
    exec:&E,req:&SignedRequest,base:&str,now_ms:u64,required_free_base_e18:u128,
)->Result<(u128,u64),ProspectiveJointError>{
    if req.method!=HttpMethod::Get||req.is_mutation{
        return Err(ProspectiveJointError::Invalid("joint proof requires signed GET"))
    }
    let res=exec.execute_request(&HttpsRequest::from_signed(req))
        .map_err(ProspectiveJointError::Transport)?;
    if res.status!=200{return Err(ProspectiveJointError::Invalid("OKX balance status rejected"))}
    let obj:Value=serde_json::from_str(&res.body)
        .map_err(|_|ProspectiveJointError::Invalid("OKX balance JSON"))?;
    if obj.get("code").and_then(Value::as_str)!=Some("0"){
        return Err(ProspectiveJointError::Invalid("OKX business status invalid"))
    }
    let roots=obj.get("data").and_then(Value::as_array)
        .ok_or(ProspectiveJointError::Invalid("OKX balance roots missing"))?;
    if roots.len()!=1{return Err(ProspectiveJointError::Invalid("multiple OKX balance roots"))}
    let rows=roots[0].get("details").and_then(Value::as_array)
        .ok_or(ProspectiveJointError::Invalid("OKX balance details missing"))?;
    let found:Vec<_>=rows.iter().filter(|row|
        row.get("ccy").and_then(Value::as_str)==Some(base)).collect();
    if found.len()!=1{return Err(ProspectiveJointError::Invalid("ambiguous spot base currency"))}
    let amount=found[0].get("cashBal").and_then(Value::as_str)
        .ok_or(ProspectiveJointError::Invalid("spot cash absent"))?;
    let cash=decimal_to_atoms(amount).ok()
        .and_then(|a|a.checked_mul(1_000_000))
        .ok_or(ProspectiveJointError::Invalid("spot cash precision"))?;
    // R32D6C7: cashBal may include frozen inventory. A total account
    // balance alone is insufficient to prove that the newly acquired net
    // base is AVAILABLE. Reject absent or inconsistent avail/frozen fields.
    let parse_base=|field:&'static str|->Result<u128,ProspectiveJointError>{
        let raw=found[0].get(field).and_then(Value::as_str)
            .ok_or(ProspectiveJointError::Invalid("OKX availability field absent"))?;
        decimal_to_atoms(raw).ok().and_then(|a|a.checked_mul(1_000_000))
            .ok_or(ProspectiveJointError::Invalid("invalid OKX available/frozen precision"))
    };
    let available=parse_base("availBal")?;
    let frozen=parse_base("frozenBal")?;
    if available.checked_add(frozen)!=Some(cash)
        ||available<required_free_base_e18{
        return Err(ProspectiveJointError::Invalid("spot inventory frozen or unavailable"))
    }
    let update=found[0].get("uTime")
        .and_then(|v|v.as_str().and_then(|s|s.parse::<u64>().ok())
            .or_else(||v.as_u64()))
        .ok_or(ProspectiveJointError::Invalid("cash uTime absent"))?;
    if update==0||update>now_ms.saturating_add(FUTURE_ALLOW_MS)
        ||now_ms.saturating_sub(update)>SOURCE_MAX_MS{
        return Err(ProspectiveJointError::Invalid("spot balance stale"))
    }
    Ok((cash,update))
}
/// Full prospective native receipt provenance + fresh OKX bracket and two
/// fresh Binance income/position checks surrounding symbol-wide all-order
/// trade history. No assertion of simultaneous exchange-time atomicity.
#[allow(clippy::too_many_arguments)]
pub fn audit_prospective_joint_chain_once<E:HttpExecutor>(
    exec:&E,store:&NativeStateStore,expected_generation:u64,
    okx:&OkxCredentials,bin:&BinanceCredentials,
    now_ms:u64,domain:OkxDomain,recv_window_ms:u64,
    income_limit:u16,trade_limit:u16,
)->Result<ProspectiveJointReport,ProspectiveJointError>{
    let state=store.load().map_err(ProspectiveJointError::State)?
        .ok_or(ProspectiveJointError::State(NativeStateError::Generation))?;
    if state.generation!=expected_generation||state.active.is_some(){
        return Err(ProspectiveJointError::State(NativeStateError::Generation))
    }
    let entry=state.entry.as_ref()
        .ok_or(ProspectiveJointError::Invalid("native entry missing"))?;
    if entry.phase!=NativeEntryPhase::RecoveryRequired
        ||entry.prospective_joint_evidence.is_some(){
        return Err(ProspectiveJointError::Invalid("not an unresolved entry for prospective proof"))
    }
    let first=entry.spot_prehedge_evidence.as_ref()
        .ok_or(ProspectiveJointError::Invalid("prospective spot fills absent"))?;
    let bills=entry.spot_prehedge_bills_evidence.as_ref()
        .ok_or(ProspectiveJointError::Invalid("prospective spot bills absent"))?;
    let cp=entry.recovery_checkpoint.as_ref()
        .ok_or(ProspectiveJointError::Invalid("futures per-fill checkpoint absent"))?;
    let future=entry.futures_entry_window_evidence.as_ref()
        .ok_or(ProspectiveJointError::Invalid("Binance income/trade evidence absent"))?;
    if first.source_generation>=bills.source_generation
        ||bills.source_generation>=future.source_generation
        ||future.source_generation>=expected_generation
        ||bills.spot_first_source_generation!=first.source_generation
        ||first.safety_generation!=entry.plan.safety_generation
        ||bills.safety_generation!=entry.plan.safety_generation
        ||future.safety_generation!=entry.plan.safety_generation
        ||!first.cash_bracket_matched||!bills.source_balance_bracket_matched
        ||!bills.base_cash_ancestry_to_original_baseline_matched
        ||!bills.exact_bills_for_first_leg_matched
        ||!future.income_position_wallet_bracket_matched
        ||!future.exact_fills_commissions_and_symbol_history_matched
        ||first.may_send_order||bills.may_send_order||future.may_send_order
        ||future.may_promote_hedged
        ||cp.leg!=NativeEntryLeg::Futures
        ||cp.quality!=NativeEntryRecoveryQuality::FuturesPriorSpotUnverified
        ||first.spot_intent_id!=bills.spot_intent_id
        ||first.spot_client_id!=bills.spot_client_id
        ||first.spot_order_id!=bills.spot_order_id
        ||cp.intent_id!=future.futures_intent_id
        ||cp.client_id!=future.futures_client_id
        ||cp.order_id!=future.futures_order_id
        ||cp.ledger.fills.keys().cloned().collect::<Vec<_>>()!=future.trade_ids
        ||entry.spot_filled_atoms_e18!=first.exact_ledger.filled_qty_atoms()
            .checked_mul(1_000_000)
            .ok_or(ProspectiveJointError::Invalid("spot gross precision overflow"))?
        ||cp.ledger.fee_cost_quote_atoms!=future.signed_quote_fee_cost_atoms_e12
        ||cp.ledger.filled_qty_atoms().checked_mul(1_000_000)
            !=Some(future.fill_qty_atoms_e18)
        ||first.exact_ledger.fee_cost_quote_atoms
            !=entry.economics.entry.spot.fee_cost_quote_atoms().map_err(|_|
                ProspectiveJointError::Invalid("spot fee ledger mismatch"))?
    {
        return Err(ProspectiveJointError::Invalid("cross-leg provenance not complete"))
    }
    let pending=store.pending_native_entry_query(expected_generation)
        .map_err(ProspectiveJointError::State)?;
    if pending.leg!=NativeEntryLeg::Futures||pending.client_id!=future.futures_client_id
        ||pending.intent_id!=future.futures_intent_id{
        return Err(ProspectiveJointError::Invalid("futures unresolved original identity changed"))
    }
    // C2/C3 preparation helper deliberately requires SpotExposed. A
    // successfully prepared but UNKNOWN Futures order is instead correctly
    // RecoveryRequired. Validate the already DURABLE immutable C2/C3 proofs
    // through phase-neutral state invariants; never try to reprepare it.
    entry.validate().map_err(ProspectiveJointError::State)?;
    let proof_dates=[first.observed_at_ms,bills.observed_at_ms,future.captured_at_ms];
    for time in proof_dates{
        if time==0||time>now_ms.saturating_add(FUTURE_ALLOW_MS)
            ||now_ms.saturating_sub(time)>SOURCE_MAX_MS{
            return Err(ProspectiveJointError::Invalid("combined source receipt expired"))
        }
    }
    if now_ms<entry.plan.created_at_ms
        ||income_limit==0||income_limit>1000
        ||trade_limit<=cp.ledger.fills.len() as u16||trade_limit>1000
        ||recv_window_ms==0||recv_window_ms>60_000{
        return Err(ProspectiveJointError::Invalid("date or scan bounds invalid"))
    }
    let baseline=entry.plan.spot_baseline_atoms_e18;
    let net=first.observed_base_cash_atoms_e18.checked_sub(baseline)
        .ok_or(ProspectiveJointError::Invalid("spot net below baseline"))?;
    if net==0||net<future.observed_short_atoms_e18
        ||bills.observed_base_cash_atoms_e18!=first.observed_base_cash_atoms_e18
        ||future.observed_short_atoms_e18==0
        ||pending.confirmed_spot_atoms_e18!=entry.spot_filled_atoms_e18{
        return Err(ProspectiveJointError::Invalid("short exceeds confirmed net asset"))
    }
    let request=build_okx_balance_request(okx,now_ms,domain)
        .map_err(ProspectiveJointError::Build)?;
    let before=selected_cash(exec,&request,&entry.plan.base,now_ms,net)?;
    if before!=(first.observed_base_cash_atoms_e18,first.balance_updated_at_ms){
        return Err(ProspectiveJointError::Invalid("OKX balance has changed since C2/C3"))
    }
    let a=audit_binance_futures_lineage_once(
        exec,store,expected_generation,bin,now_ms,recv_window_ms,income_limit,
    ).map_err(ProspectiveJointError::Income)?;
    let trades=audit_binance_symbol_wide_trade_scope(
        exec,store,expected_generation,bin,now_ms,recv_window_ms,trade_limit,
    ).map_err(ProspectiveJointError::Trades)?;
    let b=audit_binance_futures_lineage_once(
        exec,store,expected_generation,bin,now_ms,recv_window_ms,income_limit,
    ).map_err(ProspectiveJointError::Income)?;
    let after=selected_cash(exec,&request,&entry.plan.base,now_ms,net)?;
    if before!=after||a!=b
        ||a.may_send_order||trades.may_send_order
        ||a.income_pages!=future.income_pages
        ||a.position_short_atoms_e18!=future.observed_short_atoms_e18
        ||a.position_last_update_ms!=future.observed_short_updated_at_ms
        ||a.usdt_wallet_atoms_e18!=future.observed_wallet_usdt_atoms_e18
        ||a.wallet_last_update_ms!=future.observed_wallet_updated_at_ms
        ||a.total_commission_usdt_e12!=future.signed_quote_fee_cost_atoms_e12
        ||a.commission_rows!=cp.ledger.fills.len()
        ||trades.exact_order_fill_events!=future.trade_ids.len()
        ||trades.matching_trade_ids!=future.trade_ids.len()
        ||trades.windows_scanned!=future.trade_windows
        ||trades.foreign_symbol_order_count!=0
        ||trades.global_trade_history_proven
        ||trades.cross_venue_recovery_proven{
        return Err(ProspectiveJointError::Invalid("new joint snapshots differ from stored evidence"))
    }
    let current=store.load().map_err(ProspectiveJointError::State)?
        .ok_or(ProspectiveJointError::State(NativeStateError::Generation))?;
    if current!=state{
        return Err(ProspectiveJointError::State(NativeStateError::Generation))
    }
    let residual=net.checked_sub(a.position_short_atoms_e18)
        .ok_or(ProspectiveJointError::Invalid("unexpected overhedge"))?;
    let evidence=NativeProspectiveJointEvidence{
        source_generation:expected_generation,captured_at_ms:now_ms,
        safety_generation:entry.plan.safety_generation,
        spot_first_source_generation:first.source_generation,
        spot_bills_source_generation:bills.source_generation,
        futures_window_source_generation:future.source_generation,
        spot_intent_id:first.spot_intent_id.clone(),
        spot_client_id:first.spot_client_id.clone(),
        spot_order_id:first.spot_order_id.clone(),
        spot_trade_ids:first.exact_ledger.fills.keys().cloned().collect(),
        spot_bill_ids:first.fill_bill_ids.clone(),
        spot_gross_atoms_e18:entry.spot_filled_atoms_e18,
        spot_available_net_atoms_e18:net,
        spot_fee_quote_atoms_e12:first.exact_ledger.fee_cost_quote_atoms,
        spot_cash_e18:before.0,spot_cash_updated_at_ms:before.1,
        futures_intent_id:cp.intent_id.clone(),
        futures_client_id:cp.client_id.clone(),
        futures_order_id:cp.order_id.clone(),
        futures_trade_ids:future.trade_ids.clone(),
        futures_short_atoms_e18:future.observed_short_atoms_e18,
        futures_fee_quote_atoms_e12:future.signed_quote_fee_cost_atoms_e12,
        futures_wallet_usdt_e18:future.observed_wallet_usdt_atoms_e18,
        futures_position_updated_at_ms:future.observed_short_updated_at_ms,
        futures_wallet_updated_at_ms:future.observed_wallet_updated_at_ms,
        exposed_spot_unhedged_atoms_e18:residual,
        okx_balance_bracket_matched:true,
        binance_income_and_position_bracket_matched:true,
        foreign_symbol_trades_absent_in_bounded_window:true,
        cross_venue_exchange_snapshot_atomicity_proven:false,
        exchange_global_history_complete:false,
        may_send_order:false,may_promote_hedged:false,
    };
    Ok(ProspectiveJointReport{
        source_generation:expected_generation,
        spot_fill_count:first.exact_ledger.fills.len(),
        futures_fill_count:cp.ledger.fills.len(),
        okx_balances_read:2,binance_account_checks:2,
        spot_unhedged_atoms_e18:residual,
        candidate_bound_to_same_native_entry:true,
        may_send_order:false,may_promote_hedged:false,
        sealed:Some(evidence),
    })
}
