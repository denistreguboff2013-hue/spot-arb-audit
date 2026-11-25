//! Astra P0-02/P1-02 C3: typed, offline-only Binance exact net SHORT
//! observation bound to the original immutable OKX fee-adjusted V2 genesis.
//! Captured rows/snapshots are caller supplied (MOCK), NOT remote account truth.
//! No retry/resend, no POST, no opening/closing authority or Hedged promotion.
use std::collections::BTreeSet;
use serde::{Serialize,Deserialize};
use crate::native_trade2f3_v2_owner_genesis::V2OwnerGenesisReview;
#[derive(Clone,Copy,Debug,PartialEq,Eq,Serialize,Deserialize)]
pub enum V2FuturesObservationKind{
    UnknownAcknowledgement,PartialShort,ExactTargetShort,
}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V2FuturesTrade {
    pub trade_id:String,pub order_id:String,pub symbol:String,
    pub side:String,pub position_side:String,
    pub occurred_at_ms:u64,
    pub base_qty_atoms_e12:u128,pub price_quote_atoms_e12:u128,
    pub fee_currency:String,pub signed_fee_quote_atoms_e12:i128,
    pub realized_pnl_quote_atoms_e12:i128,
}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V2FuturesPositionSnapshot {
    pub captured_at_ms:u64,
    pub futures_symbol:String,
    pub short_qty_atoms_e12:u128,
    pub claimed_prior_short_atoms_e12:u128,
}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V2C3FuturesObservation {
    pub source_original_generation:u64,
    pub source_owner_revision:u64,
    pub futures_intent_id:String,
    pub futures_client_id:String,
    pub futures_symbol:String,
    pub order_id:Option<String>,
    pub captured_at_ms:u64,
    pub kind:V2FuturesObservationKind,
    pub rows:Vec<V2FuturesTrade>,
    pub first_position:Option<V2FuturesPositionSnapshot>,
    pub second_position:Option<V2FuturesPositionSnapshot>,
    pub queried_window_from_ms:u64,
    pub queried_window_through_ms:u64,
    pub all_pages_accounted_for:bool,
    pub terminal_empty_page_seen:bool,
    pub order_terminal_claimed:bool,
    pub exchange_global_history_authenticated:bool,
    pub order_send_authorized:bool,
    pub may_promote_hedged_or_verified_closed:bool,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum V2C3Error{Invalid(&'static str),Overflow}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct V2C3Review {
    pub kind:V2FuturesObservationKind,
    pub target_short_atoms_e12:u128,
    /// None means UNKNOWN, not confirmed zero.
    pub observed_short_atoms_e12:Option<u128>,
    /// None means exposure UNKNOWN until exact source and account recovery.
    pub remaining_exposure_atoms_e12:Option<u128>,
    pub locally_consistent:bool,
    pub independently_authenticated:bool,
    pub may_send_or_retry:bool,
    pub may_promote_hedged_or_closed:bool,
}
fn deny(s:&'static str)->V2C3Error{V2C3Error::Invalid(s)}
fn canonical_id(s:&str)->bool{
    s.parse::<u128>().ok().filter(|v|*v>0).map_or(false,|v|v.to_string()==s)
}
/// Validates a caller-supplied mock window and pre/post snapshots only.
/// A syntactically "ExactTargetShort" result still does NOT prove exchange
/// account completeness, safe trading permission, or external ACK certainty.
pub fn inspect_v2_c3_futures_observation(
    owner:&V2OwnerGenesisReview,
    input:&V2C3FuturesObservation,
)->Result<V2C3Review,V2C3Error>{
    if owner.schema_version!=2 || owner.owner_revision!=1
        ||owner.original_owner_cas_proven ||owner.complete_exchange_account_authenticated
        ||owner.trading_or_close_authorized
        ||input.exchange_global_history_authenticated ||input.order_send_authorized
        ||input.may_promote_hedged_or_verified_closed{
        return Err(deny("unexpected V2 version or claimed external authority"))
    }
    if input.source_original_generation!=owner.original_owner_generation
        ||input.source_owner_revision!=3
        ||input.futures_intent_id!=owner.futures_intent_id
        ||input.futures_client_id!=owner.futures_client_id
        ||input.futures_symbol!=owner.checkpoint.plan.futures_symbol
        ||input.captured_at_ms<owner.checkpoint.original.observed_at_ms
        ||input.queried_window_from_ms!=owner.checkpoint.plan.created_at_ms
        ||input.queried_window_through_ms!=input.captured_at_ms
        ||input.captured_at_ms.saturating_sub(input.queried_window_from_ms)>7*24*60*60*1000{
        return Err(deny("stale or foreign owner, source generation, identity or window"))
    }
    let target=owner.planned_net_short_atoms_e12;
    if target==0{return Err(deny("missing net target"))}
    if input.kind==V2FuturesObservationKind::UnknownAcknowledgement{
        if !input.rows.is_empty()||input.order_id.is_some()
            ||input.first_position.is_some()||input.second_position.is_some()
            ||input.all_pages_accounted_for||input.terminal_empty_page_seen
            ||input.order_terminal_claimed{
            return Err(deny("unknown ACK must not invent a zero position or order truth"))
        }
        return Ok(V2C3Review{
            kind:input.kind,target_short_atoms_e12:target,
            observed_short_atoms_e12:None,remaining_exposure_atoms_e12:None,
            locally_consistent:false,independently_authenticated:false,
            may_send_or_retry:false,may_promote_hedged_or_closed:false,
        })
    }
    let oid=input.order_id.as_deref().ok_or(deny("missing Binance orderId"))?;
    if !canonical_id(oid)||!input.all_pages_accounted_for
        ||!input.terminal_empty_page_seen ||input.rows.is_empty()
        ||input.rows.len()>256{
        return Err(deny("missing exact orderId, source pages or fills"))
    }
    // P1-03: even when cumulative SHORT exactly equals delivered net BASE,
    // a transient naked short is unsafe before original OKX spot settlement
    // was observed. All futures fill times must follow that bounded owner
    // evidence; terminal end-state equality alone cannot prove this.
    let original=&owner.checkpoint.original;
    let spot_ready_at_ms=original.observed_at_ms
       .max(original.balance_updated_at_ms)
       .max(original.exact_ledger.last_fill_time_ms.unwrap_or(0));
    if spot_ready_at_ms==0||original.exact_ledger.last_fill_time_ms.is_none()
        ||spot_ready_at_ms>input.captured_at_ms{
        return Err(deny("C3 spot delivery temporal witness absent or after capture"))
    }
    let mut used=BTreeSet::new();
    let mut qty=0u128;
    let mut previous_time=0u64;
    for row in &input.rows {
        if !canonical_id(&row.trade_id)||!used.insert(row.trade_id.as_str())
            ||row.order_id!=oid ||row.symbol!=input.futures_symbol
            ||row.side!="SELL"||row.position_side!="BOTH"
            ||row.fee_currency!="USDT"
            ||row.signed_fee_quote_atoms_e12>0
            ||row.realized_pnl_quote_atoms_e12!=0
            ||row.base_qty_atoms_e12==0||row.price_quote_atoms_e12==0
            ||row.occurred_at_ms<input.queried_window_from_ms
            ||row.occurred_at_ms<spot_ready_at_ms
            ||row.occurred_at_ms>input.captured_at_ms
            ||row.occurred_at_ms<previous_time {
            return Err(deny("futures trade/order/fees not exact or foreign"))
        }
        previous_time=row.occurred_at_ms;
        qty=qty.checked_add(row.base_qty_atoms_e12).ok_or(V2C3Error::Overflow)?;
        if qty>target{return Err(deny("futures overhedge beyond fee-adjusted net BASE"))}
    }
    let (a,b)=(input.first_position.as_ref().ok_or(deny("missing position snapshot"))?,
               input.second_position.as_ref().ok_or(deny("missing repeat position snapshot"))?);
    for p in [a,b]{
        if p.futures_symbol!=input.futures_symbol
            ||p.claimed_prior_short_atoms_e12!=0
            ||p.short_qty_atoms_e12!=qty
            ||p.captured_at_ms<input.queried_window_from_ms
            ||p.captured_at_ms<previous_time
            ||p.captured_at_ms>input.captured_at_ms {
            return Err(deny("futures position snapshots disagree or prior exposure unknown"))
        }
    }
    if a.captured_at_ms>=b.captured_at_ms {
        return Err(deny("position snapshots must be distinct and ordered"))
    }
    match input.kind {
        V2FuturesObservationKind::PartialShort if qty==target
            ||input.order_terminal_claimed => {
            return Err(deny("partial short falsely marked complete or terminal"))
        }
        V2FuturesObservationKind::ExactTargetShort if qty!=target
            ||!input.order_terminal_claimed => {
            return Err(deny("exact short not fully confirmed in source-shaped evidence"))
        }
        _=>{},
    }
    Ok(V2C3Review{
        kind:input.kind,target_short_atoms_e12:target,
        observed_short_atoms_e12:Some(qty),
        remaining_exposure_atoms_e12:Some(target-qty),
        locally_consistent:true,independently_authenticated:false,
        may_send_or_retry:false,may_promote_hedged_or_closed:false,
    })
}
