//! R32D6B2B: bounded combined OKX/Binance observational window.
//! Sequences independent signed GETs; NEVER exchange-atomic, NEVER a
//! recovered-position certification or an order/network mutation permit.
use serde::{Serialize,Deserialize};
use crate::authoritative_state::{
    NativeEntryLeg,NativeStateError,NativeStateStore,
};
use crate::native_settled_spot_audit::{
    audit_prior_settled_spot_window_once,NativeSpotWindowEvidence,
    SettledSpotAuditError,
};
use crate::native_binance_lineage::{
    audit_binance_futures_lineage_once,BinanceLineageError,
};
use crate::native_futures_trade_scope::{
    audit_binance_symbol_wide_trade_scope,FuturesTradeScopeError,
};
use crate::private_https::HttpExecutor;
use crate::private_rest::{OkxCredentials,OkxDomain,BinanceCredentials};

const OBSERVED_AGE_MS:u64=180_000;
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum JointWindowError{
    State(NativeStateError),Spot(SettledSpotAuditError),
    Binance(BinanceLineageError),Trades(FuturesTradeScopeError),
    Invalid(&'static str),
}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
pub struct NativeJointWindowEvidence{
    pub source_generation:u64,
    pub requested_at_ms:u64,
    pub safety_generation:u64,
    pub spot:NativeSpotWindowEvidence,
    pub futures_intent_id:String,
    pub futures_client_id:String,
    pub futures_order_id:String,
    pub futures_trade_ids:Vec<String>,
    pub futures_short_atoms_e18:u128,
    pub futures_position_updated_at_ms:u64,
    pub usdt_wallet_atoms_e18:u128,
    pub usdt_wallet_updated_at_ms:u64,
    pub futures_fee_quote_atoms_e12:i128,
    pub futures_income_pages:u16,
    pub futures_trade_windows:usize,
    pub income_check_repetitions:u8,
    pub spot_window_rechecked:bool,
    pub futures_income_bracket_matched:bool,
    pub futures_symbol_trade_window_matched:bool,
    pub exchange_global_history_complete:bool,
    pub exchange_cross_venue_atomicity_proven:bool,
    pub may_send_order:bool,
}
#[derive(Clone,Debug)]
pub struct JointWindowReport{
    pub okx_fills_count:usize,
    pub okx_bills_count:usize,
    pub futures_trades_count:usize,
    pub futures_income_commissions_count:usize,
    pub income_checks:u8,
    pub read_only_candidate:bool,
    pub exchange_global_history_complete:bool,
    pub exchange_cross_venue_atomicity_proven:bool,
    pub may_send_order:bool,
    pub(crate) sealed:Option<NativeJointWindowEvidence>,
}
/// The native generation cannot change during the sequence, but accounts
/// on separate venues cannot be sampled atomically and signed GET history
/// APIs cannot establish absolute exchange-global completeness.
#[allow(clippy::too_many_arguments)]
pub fn audit_joint_recovery_window_once<E:HttpExecutor>(
    exec:&E,store:&NativeStateStore,expected_generation:u64,
    okx:&OkxCredentials,bin:&BinanceCredentials,
    now_ms:u64,domain:OkxDomain,recv_window_ms:u64,
    okx_page_limit:u16,binance_income_limit:u16,binance_trade_limit:u16,
)->Result<JointWindowReport,JointWindowError>{
    let pending=store.pending_native_entry_query(expected_generation)
        .map_err(JointWindowError::State)?;
    if pending.leg!=NativeEntryLeg::Futures{
        return Err(JointWindowError::Invalid("requires unresolved futures leg"))
    }
    let state=store.load().map_err(JointWindowError::State)?
        .ok_or(JointWindowError::State(NativeStateError::Generation))?;
    if state.generation!=expected_generation{
        return Err(JointWindowError::State(NativeStateError::Generation))
    }
    let entry=state.entry.as_ref().ok_or(JointWindowError::Invalid("entry missing"))?;
    let dual=entry.dual_leg_evidence.as_ref()
        .ok_or(JointWindowError::Invalid("dual-native evidence missing"))?;
    let spot_saved=entry.settled_spot_window_evidence.as_ref()
        .ok_or(JointWindowError::Invalid("previous OKX history not persisted"))?;
    if entry.joint_window_evidence.is_some()
        ||now_ms<pending.created_at_ms
        ||spot_saved.spot_intent_id!=dual.settled_spot_intent_id
        ||spot_saved.spot_order_id!=dual.settled_spot_order_id
        ||dual.pending_futures_checkpoint.intent_id!=pending.intent_id
        ||dual.pending_futures_checkpoint.client_id!=pending.client_id
        ||okx_page_limit==0||okx_page_limit>100
        ||binance_income_limit==0||binance_income_limit>1000
        ||binance_trade_limit==0||binance_trade_limit>1000
        ||recv_window_ms==0||recv_window_ms>60_000{
        return Err(JointWindowError::Invalid("preflight window/identity limits failed"))
    }
    // Fresh account-level OKX full recheck against saved exact native spot
    // checkpoint and prior source window. Never reuses an old cash snapshot
    // silently.
    let spot=audit_prior_settled_spot_window_once(
        exec,store,expected_generation,okx,now_ms,domain,okx_page_limit,
    ).map_err(JointWindowError::Spot)?;
    if !spot.candidate_window_matched||spot.may_send_order{
        return Err(JointWindowError::Invalid("new OKX snapshot unverified"))
    }
    let spot_new=spot.sealed.ok_or(JointWindowError::Invalid("spot seal absent"))?;
    let mut comparable=spot_new.clone();
    comparable.source_generation=spot_saved.source_generation;
    if &comparable!=spot_saved{
        return Err(JointWindowError::Invalid("OKX snapshot differs from stored spot lineage"))
    }
    let before=audit_binance_futures_lineage_once(
        exec,store,expected_generation,bin,now_ms,recv_window_ms,binance_income_limit,
    ).map_err(JointWindowError::Binance)?;
    let trades=audit_binance_symbol_wide_trade_scope(
        exec,store,expected_generation,bin,now_ms,recv_window_ms,binance_trade_limit,
    ).map_err(JointWindowError::Trades)?;
    let after=audit_binance_futures_lineage_once(
        exec,store,expected_generation,bin,now_ms,recv_window_ms,binance_income_limit,
    ).map_err(JointWindowError::Binance)?;
    if before!=after || before.may_send_order||trades.may_send_order
        ||before.prior_spot_fee_origin_proven
        ||before.all_account_activity_proven
        ||before.exchange_snapshots_atomicity_proven
        ||trades.global_trade_history_proven
        ||trades.cross_venue_recovery_proven
        ||trades.foreign_symbol_order_count!=0{
        return Err(JointWindowError::Invalid("Binance before/after income or exposure changed"))
    }
    let checkpoint=&dual.pending_futures_checkpoint;
    if before.position_short_atoms_e18
        !=pending.confirmed_futures_atoms_e18
            .checked_add(checkpoint.ledger.filled_qty_atoms().checked_mul(1_000_000)
                .ok_or(JointWindowError::Invalid("qty overflow"))?)
            .ok_or(JointWindowError::Invalid("short overflow"))?
        ||before.commission_rows!=checkpoint.ledger.fills.len()
        ||before.total_commission_usdt_e12!=checkpoint.ledger.fee_cost_quote_atoms
        ||trades.exact_order_fill_events!=checkpoint.ledger.fills.len()
        ||trades.matching_trade_ids!=checkpoint.ledger.fills.len()
        ||trades.sum_signed_position_change_atoms_e12
            !=-i128::try_from(checkpoint.ledger.filled_qty_atoms())
                .map_err(|_|JointWindowError::Invalid("quantity overflow"))?{
        return Err(JointWindowError::Invalid("Binance external evidence not matching durable fills"))
    }
    // No leap in account timestamps is accepted as "fresh" simply because
    // both endpoint reads happened now. Even after this age check, there
    // is NO atomic cross-venue snapshot.
    let recent=|v:u64|v>0&&v<=now_ms.saturating_add(5_000)
        &&now_ms.saturating_sub(v)<=OBSERVED_AGE_MS;
    if !recent(spot_new.observed_cash_updated_at_ms)
        ||!recent(before.position_last_update_ms)
        ||!recent(before.wallet_last_update_ms){
        return Err(JointWindowError::Invalid("cross-venue source updates stale"))
    }
    let final_target=store.pending_native_entry_query(expected_generation)
        .map_err(JointWindowError::State)?;
    if final_target!=pending{
        return Err(JointWindowError::State(NativeStateError::Generation))
    }
    let futures_ids=checkpoint.ledger.fills.keys().cloned().collect();
    let sealed=NativeJointWindowEvidence{
        source_generation:expected_generation,requested_at_ms:now_ms,
        safety_generation:dual.safety_generation,spot:spot_new,
        futures_intent_id:checkpoint.intent_id.clone(),
        futures_client_id:checkpoint.client_id.clone(),
        futures_order_id:checkpoint.order_id.clone(),
        futures_trade_ids:futures_ids,
        futures_short_atoms_e18:before.position_short_atoms_e18,
        futures_position_updated_at_ms:before.position_last_update_ms,
        usdt_wallet_atoms_e18:before.usdt_wallet_atoms_e18,
        usdt_wallet_updated_at_ms:before.wallet_last_update_ms,
        futures_fee_quote_atoms_e12:before.total_commission_usdt_e12,
        futures_income_pages:before.income_pages,
        futures_trade_windows:trades.windows_scanned,
        income_check_repetitions:2,spot_window_rechecked:true,
        futures_income_bracket_matched:true,
        futures_symbol_trade_window_matched:true,
        exchange_global_history_complete:false,
        exchange_cross_venue_atomicity_proven:false,
        may_send_order:false,
    };
    Ok(JointWindowReport{
        okx_fills_count:spot.fill_rows,okx_bills_count:spot.bill_rows,
        futures_trades_count:trades.matching_trade_ids,
        futures_income_commissions_count:before.commission_rows,
        income_checks:2,read_only_candidate:true,
        exchange_global_history_complete:false,
        exchange_cross_venue_atomicity_proven:false,
        may_send_order:false,sealed:Some(sealed),
    })
}
