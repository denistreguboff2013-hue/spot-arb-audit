//! Astra Native V2 C3 opening-side recovery: OFFLINE caller-supplied review.
 //! Original UNKNOWN ACK is never a proven zero or failed order. Original
 //! PARTIAL fills remain immutable and must be present byte-for-byte.
 //! Local reconciliation NEVER grants Hedged, POST, CANCEL or blind re-send.
 use std::collections::{BTreeMap,BTreeSet};
 use serde::{Serialize,Deserialize};
 use crate::native_trade2f3_v2_c3_futures_evidence::{
     V2FuturesObservationKind,V2FuturesTrade,V2FuturesPositionSnapshot,
     inspect_v2_c3_futures_observation,
 };
 use crate::native_trade2f3_v2_versioned_store::{V2ReviewPhase,V2ReviewState};
 #[derive(Clone,Copy,Debug,PartialEq,Eq,Serialize,Deserialize)]
 pub enum V2OpeningRecoveryKind { StillUnknown, ObservedPartial, ObservedExact }
 #[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
 #[serde(deny_unknown_fields)]
 pub struct V2C3OpeningRecoveryEvidence {
     pub original_owner_generation:u64,
     pub parent_v2_revision:u64,
     pub original_intent_id:String,
     pub original_client_id:String,
     pub futures_symbol:String,
     pub observed_order_id:Option<String>,
     pub recovered_client_id:Option<String>,
     pub kind:V2OpeningRecoveryKind,
     pub queried_from_ms:u64,
     pub captured_at_ms:u64,
     pub bounded_pages_complete:bool,
     pub terminal_empty_page_seen:bool,
     pub reported_order_terminal:bool,
     pub fills:Vec<V2FuturesTrade>,
     pub first_position:Option<V2FuturesPositionSnapshot>,
     pub second_position:Option<V2FuturesPositionSnapshot>,
     pub account_wide_exchange_truth_authenticated:bool,
     pub external_antirollback_proven:bool,
     pub may_send_retry_cancel_or_promote_hedged:bool,
 }
 #[derive(Clone,Debug,PartialEq,Eq)]
 pub enum V2OpeningRecoveryError { Denied(&'static str),Overflow }
 #[derive(Clone,Debug,PartialEq,Eq)]
 pub struct V2OpeningRisk {
     pub owned_net_spot_base_atoms_e12:u128,
     pub original_known_short_floor_atoms_e12:Option<u128>,
     pub observed_short_atoms_e12:Option<u128>,
     /// None means UNKNOWN, NOT 0 and NOT "safe to resend".
     pub remaining_unhedged_atoms_e12:Option<u128>,
     pub locally_exact_short_shape:bool,
     pub exchange_authoritative:bool,
     pub may_send_retry_cancel_or_promote_hedged:bool,
 }
 fn deny(s:&'static str)->V2OpeningRecoveryError{V2OpeningRecoveryError::Denied(s)}
 fn numeric(s:&str)->bool{
     s.parse::<u128>().ok().filter(|v|*v>0).is_some_and(|v|v.to_string()==s)
 }
 /// Validates original V2 owner C3 + later caller-supplied account-like window.
 /// A returned ObservedExact only means MOCK-shape arithmetic is consistent,
 /// NOT exchange account truth, final risk, permission to trade, or Hedged.
 pub fn inspect_v2_c3_opening_recovery(
     owner:&V2ReviewState,
     obs:&V2C3OpeningRecoveryEvidence,
 )->Result<V2OpeningRisk,V2OpeningRecoveryError>{
     if owner.phase!=V2ReviewPhase::FuturesEvidenceRecorded||owner.revision!=4
         ||owner.close_four_leg.is_some()||owner.emergency_close.is_some()
         ||owner.opening_recovery.is_some()
         ||owner.production_authorized||owner.may_send_or_cancel_or_verify_closed
         ||obs.original_owner_generation!=owner.genesis.original_owner_generation
         ||obs.parent_v2_revision!=4
         ||obs.original_intent_id!=owner.genesis.futures_intent_id
         ||obs.original_client_id!=owner.genesis.futures_client_id
         ||obs.futures_symbol!=owner.genesis.checkpoint.plan.futures_symbol
         ||obs.account_wide_exchange_truth_authenticated
         ||obs.external_antirollback_proven
         ||obs.may_send_retry_cancel_or_promote_hedged{
         return Err(deny("opening recovery source/generation/authority invalid"))
     }
     let original=owner.c3_futures.as_ref().ok_or(deny("missing original C3"))?;
     let preview=inspect_v2_c3_futures_observation(&owner.genesis,original)
         .map_err(|_|deny("original C3 failed revalidation"))?;
     if original.kind==V2FuturesObservationKind::ExactTargetShort {
         return Err(deny("no opening recovery on C3 already locally exact"))
     }
     let lower_bound=preview.observed_short_atoms_e12;
     let target=owner.genesis.planned_net_short_atoms_e12;
     if obs.queried_from_ms!=original.queried_window_from_ms
         ||obs.captured_at_ms<=original.captured_at_ms
         ||obs.captured_at_ms<obs.queried_from_ms
         ||obs.captured_at_ms-obs.queried_from_ms>7*24*60*60*1000{
         return Err(deny("stale/future/foreign bounded opening read window"))
     }
     if obs.kind==V2OpeningRecoveryKind::StillUnknown {
         if obs.observed_order_id.is_some()||obs.recovered_client_id.is_some()
             ||!obs.fills.is_empty()||obs.first_position.is_some()
             ||obs.second_position.is_some()||obs.bounded_pages_complete
             ||obs.terminal_empty_page_seen||obs.reported_order_terminal {
             return Err(deny("no-ACK/unknown still must not invent order/fills/zero"))
         }
         return Ok(V2OpeningRisk{
             owned_net_spot_base_atoms_e12:target,
             original_known_short_floor_atoms_e12:lower_bound,
             observed_short_atoms_e12:None,
             remaining_unhedged_atoms_e12:None,
             locally_exact_short_shape:false,exchange_authoritative:false,
             may_send_retry_cancel_or_promote_hedged:false,
         })
     }
     let order=obs.observed_order_id.as_deref()
         .ok_or(deny("missing recovered Binance numeric orderId"))?;
     if !numeric(order)
         ||obs.recovered_client_id.as_deref()!=Some(original.futures_client_id.as_str())
         ||!obs.bounded_pages_complete||!obs.terminal_empty_page_seen
         ||obs.fills.is_empty()||obs.fills.len()>256{
         return Err(deny("recovery window incomplete or original client not matched"))
     }
     if original.kind==V2FuturesObservationKind::PartialShort
         &&original.order_id.as_deref()!=Some(order){
         return Err(deny("partial owner orderId must not be replaced"))
     }
     let known:BTreeMap<&str,&V2FuturesTrade>=original.rows.iter()
         .map(|r|(r.trade_id.as_str(),r)).collect();
     let mut seen=BTreeSet::new();
     let mut total=0u128;
     let mut last=0u64;
     for r in &obs.fills{
         if !numeric(&r.trade_id)||!seen.insert(r.trade_id.as_str())
             ||r.order_id!=order||r.symbol!=obs.futures_symbol
             ||r.side!="SELL"||r.position_side!="BOTH"
             ||r.fee_currency!="USDT"||r.signed_fee_quote_atoms_e12>0
             ||r.realized_pnl_quote_atoms_e12!=0
             ||r.base_qty_atoms_e12==0||r.price_quote_atoms_e12==0
             ||r.occurred_at_ms<obs.queried_from_ms
             ||r.occurred_at_ms>obs.captured_at_ms
             ||r.occurred_at_ms<last{
             return Err(deny("foreign, duplicate or malformed recovered opening trade"))
         }
         if let Some(immutable)=known.get(r.trade_id.as_str()){
             if *immutable!=r{
                 return Err(deny("original C3 trade altered during recovery"))
             }
         }
         last=r.occurred_at_ms;
         total=total.checked_add(r.base_qty_atoms_e12)
             .ok_or(V2OpeningRecoveryError::Overflow)?;
         if total>target{return Err(deny("overhedge beyond BASE fee-adjusted target"))}
     }
     if known.len()>seen.len()||known.keys().any(|id|!seen.contains(id)){
         return Err(deny("previously ACKed partial fills disappeared"))
     }
     if lower_bound.is_some_and(|floor|total<floor){
         return Err(deny("recovery cumulative fell below known opening fills"))
     }
     let first=obs.first_position.as_ref().ok_or(deny("missing first account position"))?;
     let second=obs.second_position.as_ref().ok_or(deny("missing second account position"))?;
     for p in [first,second]{
         if p.futures_symbol!=obs.futures_symbol
             ||p.claimed_prior_short_atoms_e12!=0
             ||p.short_qty_atoms_e12!=total
             ||p.captured_at_ms<last||p.captured_at_ms>obs.captured_at_ms{
             return Err(deny("recovered account-like position disagrees with order fills"))
         }
     }
     if first.captured_at_ms>=second.captured_at_ms{
         return Err(deny("position snapshots not sequential and distinct"))
     }
     match obs.kind{
         V2OpeningRecoveryKind::ObservedPartial if total>=target=>{
             return Err(deny("partial cannot report target/overfilled"))
         }
         V2OpeningRecoveryKind::ObservedExact
             if total!=target||!obs.reported_order_terminal=>{
             return Err(deny("full quantity requires mock terminal order"))
         }
         _=>{},
     }
     Ok(V2OpeningRisk{
         owned_net_spot_base_atoms_e12:target,
         original_known_short_floor_atoms_e12:lower_bound,
         observed_short_atoms_e12:Some(total),
         remaining_unhedged_atoms_e12:Some(target-total),
         locally_exact_short_shape:obs.kind==V2OpeningRecoveryKind::ObservedExact,
         exchange_authoritative:false,
         may_send_retry_cancel_or_promote_hedged:false,
     })
 }
