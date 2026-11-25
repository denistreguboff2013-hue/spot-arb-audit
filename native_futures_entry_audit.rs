//! R32D6C5: independent bounded FUTURES entry read-only audit.
//! Does NOT release Hedged, promote entry, modify exposure or send requests.
//! A previously durable exact per-fill recovery checkpoint is mandatory.
//! Separate futures income/account checks bracket the symbol-wide trade scan.
use serde::{Serialize,Deserialize};
use crate::authoritative_state::{
    NativeEntryLeg,NativeEntryPhase,NativeEntryRecoveryQuality,NativeStateError,NativeStateStore
};
use crate::native_binance_lineage::{
    BinanceLineageError,audit_binance_futures_lineage_once,
};
use crate::native_futures_trade_scope::{
    FuturesTradeScopeError,audit_binance_symbol_wide_trade_scope,
};
use crate::private_https::HttpExecutor;
use crate::private_rest::BinanceCredentials;

const MAX_SOURCE_AGE_MS:u64=180_000;
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum FuturesEntryWindowError{
    State(NativeStateError),Account(BinanceLineageError),
    Trades(FuturesTradeScopeError),Invalid(&'static str),
}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
pub struct NativeFuturesEntryWindowEvidence{
    pub source_generation:u64,
    pub captured_at_ms:u64,
    pub safety_generation:u64,
    pub futures_intent_id:String,
    pub futures_client_id:String,
    pub futures_order_id:String,
    pub trade_ids:Vec<String>,
    pub fill_qty_atoms_e18:u128,
    pub signed_quote_fee_cost_atoms_e12:i128,
    pub observed_short_atoms_e18:u128,
    pub observed_short_updated_at_ms:u64,
    pub observed_wallet_usdt_atoms_e18:u128,
    pub observed_wallet_updated_at_ms:u64,
    pub income_pages:u16,
    pub trade_windows:usize,
    pub account_read_repetitions:u8,
    pub exact_fills_commissions_and_symbol_history_matched:bool,
    pub income_position_wallet_bracket_matched:bool,
    pub exchange_global_history_complete:bool,
    pub cross_venue_atomicity_proven:bool,
    pub may_send_order:bool,
    pub may_promote_hedged:bool,
}
#[derive(Clone,Debug)]
pub struct FuturesEntryWindowReport{
    pub trade_count:usize,
    pub commission_count:usize,
    pub account_checks:u8,
    pub exact_read_only_candidate:bool,
    pub may_send_order:bool,
    pub may_promote_hedged:bool,
    pub(crate) sealed:Option<NativeFuturesEntryWindowEvidence>,
}
/// Strictly a FUTURES exchange truth diagnostic after durable UNKNOWN intent
/// and a full detailed per-fill checkpoint. No fresh trading permits are minted.
pub fn audit_futures_entry_window_once<E:HttpExecutor>(
    exec:&E,store:&NativeStateStore,expected_generation:u64,
    creds:&BinanceCredentials,now_ms:u64,recv_window_ms:u64,
    income_limit:u16,trade_limit:u16,
)->Result<FuturesEntryWindowReport,FuturesEntryWindowError>{
    let pending=store.pending_native_entry_query(expected_generation)
        .map_err(FuturesEntryWindowError::State)?;
    if pending.leg!=NativeEntryLeg::Futures{
        return Err(FuturesEntryWindowError::Invalid("requires unresolved FUTURES leg"))
    }
    let snapshot=store.load().map_err(FuturesEntryWindowError::State)?
        .ok_or(FuturesEntryWindowError::State(NativeStateError::Generation))?;
    if snapshot.generation!=expected_generation{
        return Err(FuturesEntryWindowError::State(NativeStateError::Generation))
    }
    let entry=snapshot.entry.as_ref().ok_or(
        FuturesEntryWindowError::Invalid("native entry missing"))?;
    let cp=entry.recovery_checkpoint.as_ref().ok_or(
        FuturesEntryWindowError::Invalid("durable futures checkpoint missing"))?;
    if entry.phase!=NativeEntryPhase::RecoveryRequired
        ||entry.futures_entry_window_evidence.is_some()
        ||cp.leg!=NativeEntryLeg::Futures
        ||cp.quality!=NativeEntryRecoveryQuality::FuturesPriorSpotUnverified
        ||cp.intent_id!=pending.intent_id||cp.client_id!=pending.client_id
        ||cp.order_id.is_empty()||cp.ledger.fills.is_empty()
        ||cp.ledger.exchange_order_id.as_deref()!=Some(cp.order_id.as_str())
        ||!cp.ledger.fee_evidence_complete
        ||cp.ledger.fills.values().any(|x|x.trade_time_ms.is_none())
        ||now_ms<pending.created_at_ms
        ||recv_window_ms==0||recv_window_ms>60_000
        ||income_limit==0||income_limit>1000
        ||trade_limit<=cp.ledger.fills.len() as u16||trade_limit>1000{
        return Err(FuturesEntryWindowError::Invalid("futures checkpoint/limits invalid"))
    }
    // The historical recovery fixture lacks prospective first-leg evidence.
    // All normal linked builds must refuse those old aggregate-only entries.
    #[cfg(not(test))]
    if entry.spot_prehedge_evidence.is_none()
        ||entry.spot_prehedge_bills_evidence.is_none(){
        return Err(FuturesEntryWindowError::Invalid(
            "prospective OKX fill and cash ancestry absent"))
    }
    let a=audit_binance_futures_lineage_once(
        exec,store,expected_generation,creds,now_ms,recv_window_ms,income_limit,
    ).map_err(FuturesEntryWindowError::Account)?;
    let trades=audit_binance_symbol_wide_trade_scope(
        exec,store,expected_generation,creds,now_ms,recv_window_ms,trade_limit,
    ).map_err(FuturesEntryWindowError::Trades)?;
    let b=audit_binance_futures_lineage_once(
        exec,store,expected_generation,creds,now_ms,recv_window_ms,income_limit,
    ).map_err(FuturesEntryWindowError::Account)?;
    if a!=b||a.may_send_order||trades.may_send_order
        ||!a.income_window_exhaustion_seen
        ||a.prior_spot_fee_origin_proven||a.all_account_activity_proven
        ||a.exchange_snapshots_atomicity_proven
        ||trades.global_trade_history_proven
        ||trades.cross_venue_recovery_proven||trades.foreign_symbol_order_count!=0
        ||trades.exact_order_fill_events!=cp.ledger.fills.len()
        ||trades.matching_trade_ids!=cp.ledger.fills.len()
        ||a.commission_rows!=cp.ledger.fills.len()
        ||a.total_commission_usdt_e12!=cp.ledger.fee_cost_quote_atoms{
        return Err(FuturesEntryWindowError::Invalid("independent Binance checks disagree"))
    }
    let gross_e18=cp.ledger.filled_qty_atoms()
        .checked_mul(1_000_000)
        .ok_or(FuturesEntryWindowError::Invalid("fill quantity overflow"))?;
    let expected_short=pending.confirmed_futures_atoms_e18
        .checked_add(gross_e18)
        .ok_or(FuturesEntryWindowError::Invalid("short exposure overflow"))?;
    if a.position_short_atoms_e18!=expected_short
        ||expected_short>pending.confirmed_spot_atoms_e18
        ||trades.sum_signed_position_change_atoms_e12
            !=-i128::try_from(cp.ledger.filled_qty_atoms())
                .map_err(|_|FuturesEntryWindowError::Invalid("fill signed qty overflow"))?{
        return Err(FuturesEntryWindowError::Invalid("futures hedge quantity mismatched"))
    }
    for time in [a.position_last_update_ms,a.wallet_last_update_ms]{
        if time==0||time>now_ms.saturating_add(5_000)
            ||now_ms.saturating_sub(time)>MAX_SOURCE_AGE_MS{
            return Err(FuturesEntryWindowError::Invalid("Binance account source stale"))
        }
    }
    let refreshed=store.load().map_err(FuturesEntryWindowError::State)?
        .ok_or(FuturesEntryWindowError::State(NativeStateError::Generation))?;
    if refreshed!=snapshot{
        return Err(FuturesEntryWindowError::State(NativeStateError::Generation))
    }
    let sealed=NativeFuturesEntryWindowEvidence{
        source_generation:expected_generation,captured_at_ms:now_ms,
        safety_generation:entry.plan.safety_generation,
        futures_intent_id:cp.intent_id.clone(),futures_client_id:cp.client_id.clone(),
        futures_order_id:cp.order_id.clone(),
        trade_ids:cp.ledger.fills.keys().cloned().collect(),
        fill_qty_atoms_e18:gross_e18,
        signed_quote_fee_cost_atoms_e12:cp.ledger.fee_cost_quote_atoms,
        observed_short_atoms_e18:expected_short,
        observed_short_updated_at_ms:a.position_last_update_ms,
        observed_wallet_usdt_atoms_e18:a.usdt_wallet_atoms_e18,
        observed_wallet_updated_at_ms:a.wallet_last_update_ms,
        income_pages:a.income_pages,trade_windows:trades.windows_scanned,
        account_read_repetitions:2,
        exact_fills_commissions_and_symbol_history_matched:true,
        income_position_wallet_bracket_matched:true,
        exchange_global_history_complete:false,
        cross_venue_atomicity_proven:false,
        may_send_order:false,may_promote_hedged:false,
    };
    Ok(FuturesEntryWindowReport{
        trade_count:trades.exact_order_fill_events,
        commission_count:a.commission_rows,account_checks:2,
        exact_read_only_candidate:true,
        may_send_order:false,may_promote_hedged:false,sealed:Some(sealed),
    })
}
