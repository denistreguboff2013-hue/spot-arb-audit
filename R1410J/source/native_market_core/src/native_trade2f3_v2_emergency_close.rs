//! Astra V2 C5/C6 emergency CLOSE: zero-network owner-bound recovery witness.
 //! Caller-provided mock exchange rows are NOT independent account evidence.
 //! UNKNOWN is not zero, partial is not terminal, and NO send/retry or close.
 use std::collections::BTreeSet;
 use serde::{Deserialize,Serialize};
 use crate::native_trade2f3_v2_c3_futures_evidence::{
     inspect_v2_c3_futures_observation,V2FuturesObservationKind,
 };
 use crate::native_trade2f3_v2_four_leg_close_review::{
     V2SpotSellFill,V2FuturesCoverFill,V2CloseFlatObservation,
 };
 use crate::native_trade2f3_v2_versioned_store::{V2ReviewState,V2ReviewPhase};

 #[derive(Clone,Copy,Debug,PartialEq,Eq,Serialize,Deserialize)]
 pub enum V2EmergencyLegOutcome{UnknownAcknowledgement,PartialExecution}

 #[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
 #[serde(deny_unknown_fields)]
 pub struct V2EmergencyFuturesCover{
     pub outcome:V2EmergencyLegOutcome,
     pub intent_id:String,pub client_id:String,pub order_id:Option<String>,
     pub fills:Vec<V2FuturesCoverFill>,
     pub position_first:Option<V2CloseFlatObservation>,
     pub position_second:Option<V2CloseFlatObservation>,
     /// Claimed bounded source page coverage; not independent attestation.
     pub pages_complete:bool,pub terminal_empty_page_seen:bool,
     pub order_terminal_claimed:bool,
 }
 #[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
 #[serde(deny_unknown_fields)]
 pub struct V2EmergencySpotSell{
     pub outcome:V2EmergencyLegOutcome,
     pub intent_id:String,pub client_id:String,pub order_id:Option<String>,
     pub fills:Vec<V2SpotSellFill>,
     pub pages_complete:bool,pub terminal_empty_page_seen:bool,
     pub order_terminal_claimed:bool,
 }
 #[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
 #[serde(deny_unknown_fields)]
 pub struct V2EmergencyCloseEvidence{
     pub source_original_generation:u64,pub source_v2_revision:u64,
     pub window_from_ms:u64,pub captured_at_ms:u64,
     pub futures_cover:V2EmergencyFuturesCover,
     pub spot_sell:V2EmergencySpotSell,
     pub exchange_account_global_history_authenticated:bool,
     pub independent_rollback_floor_verified:bool,
     pub may_retry_send_cancel_or_verify_closed:bool,
 }
 #[derive(Clone,Debug,PartialEq,Eq)]
 pub enum V2EmergencyError{Denied(&'static str),Overflow}
 #[derive(Clone,Debug,PartialEq,Eq)]
 pub struct V2EmergencyRiskPreview{
     pub owned_net_base_atoms_e12:u128,
     pub original_short_atoms_e12:u128,
     /// Unknown order result is None, never a fabricated zero.
     pub observed_futures_cover_atoms_e12:Option<u128>,
     pub observed_spot_sell_atoms_e12:Option<u128>,
     pub remaining_short_atoms_e12:Option<u128>,
     pub remaining_owned_spot_atoms_e12:Option<u128>,
     /// If BOTH are observed, difference in outstanding exposure, signed.
     pub known_net_base_minus_short_atoms_e12:Option<i128>,
     pub independently_account_proven:bool,
     pub definitive_pnl_atoms_e12:Option<i128>,
     pub may_send_retry_cancel_or_verify_closed:bool,
 }
 fn deny(s:&'static str)->V2EmergencyError{V2EmergencyError::Denied(s)}
 fn canonical_id(s:&str)->bool{
     s.parse::<u128>().ok().filter(|v|*v>0).map_or(false,|v|v.to_string()==s)
 }
 fn distinct<'a>(ids:impl IntoIterator<Item=&'a str>)->bool{
     let ids:Vec<&str>=ids.into_iter().collect();
     ids.iter().all(|s|!s.is_empty())&&ids.iter().collect::<BTreeSet<_>>().len()==ids.len()
 }
 pub fn inspect_v2_emergency_close(
     owner:&V2ReviewState,input:&V2EmergencyCloseEvidence,
 )->Result<V2EmergencyRiskPreview,V2EmergencyError>{
     if owner.revision!=4||owner.phase!=V2ReviewPhase::FuturesEvidenceRecorded
         ||owner.close_four_leg.is_some()||owner.emergency_close.is_some()
         ||owner.may_send_or_cancel_or_verify_closed||owner.production_authorized
         ||input.source_v2_revision!=4
         ||input.source_original_generation!=owner.genesis.original_owner_generation
         ||input.exchange_account_global_history_authenticated
         ||input.independent_rollback_floor_verified
         ||input.may_retry_send_cancel_or_verify_closed{
         return Err(deny("review owner/version or claimed trading/attestation authority invalid"))
     }
     let c3=owner.c3_futures.as_ref().ok_or(deny("original C3 absent"))?;
     let c3_check=inspect_v2_c3_futures_observation(&owner.genesis,c3)
         .map_err(|_|deny("original C3 source invalid"))?;
     if c3.kind!=V2FuturesObservationKind::ExactTargetShort
         ||c3_check.observed_short_atoms_e12!=Some(owner.genesis.planned_net_short_atoms_e12)
         ||c3_check.remaining_exposure_atoms_e12!=Some(0){
         return Err(deny("original C3 UNKNOWN or PARTIAL needs separate opening recovery"))
     }
     let target=owner.genesis.planned_net_short_atoms_e12;
     let cp=&owner.genesis.checkpoint;
     if target==0||input.window_from_ms<c3.captured_at_ms
         ||input.window_from_ms>input.captured_at_ms
         ||input.captured_at_ms-input.window_from_ms>7*24*60*60*1000{
         return Err(deny("invalid source window or target"))
     }
     let f=&input.futures_cover;let s=&input.spot_sell;
     if !distinct([f.intent_id.as_str(),s.intent_id.as_str(),
         owner.genesis.futures_intent_id.as_str(),cp.original.spot_intent_id.as_str()])
         ||!distinct([f.client_id.as_str(),s.client_id.as_str(),
         owner.genesis.futures_client_id.as_str(),cp.original.spot_client_id.as_str()]){
         return Err(deny("foreign/reused closing order owner identity"))
     }
     if f.order_id.as_deref()==c3.order_id.as_deref()
         ||s.order_id.as_deref()==Some(cp.original.spot_order_id.as_str()){
         return Err(deny("closing exchange order id reused"))
     }
     let mut used_futures=c3.rows.iter().map(|r|r.trade_id.as_str()).collect::<BTreeSet<_>>();
     let mut used_spot=cp.original.exact_ledger.fills.keys().map(|s|s.as_str()).collect::<BTreeSet<_>>();
     let mut used_bills=cp.original.fill_bill_ids.iter().map(|s|s.as_str()).collect::<BTreeSet<_>>();
     let mut spot_count=None;let mut fut_count=None;
     match f.outcome{
         V2EmergencyLegOutcome::UnknownAcknowledgement=>{
             if f.order_id.is_some()||!f.fills.is_empty()||f.position_first.is_some()
                 ||f.position_second.is_some()||f.pages_complete
                 ||f.terminal_empty_page_seen||f.order_terminal_claimed{
                 return Err(deny("UNKNOWN futures cover must not claim zero fills/account position"))
             }
         }
         V2EmergencyLegOutcome::PartialExecution=>{
             let order=f.order_id.as_deref().ok_or(deny("future cover orderId missing"))?;
             if !canonical_id(order)||!f.pages_complete||!f.terminal_empty_page_seen
                 ||f.order_terminal_claimed||f.fills.is_empty()||f.fills.len()>128{
                 return Err(deny("partial futures source page, terminal status or order invalid"))
             }
             let mut qty=0u128;let mut prev=0u64;
             for r in &f.fills{
                 if !canonical_id(&r.trade_id)||!used_futures.insert(&r.trade_id)
                     ||r.order_id!=order||r.symbol!=cp.plan.futures_symbol
                     ||r.side!="BUY"||r.position_side!="BOTH"
                     ||r.fee_currency!="USDT"||r.signed_fee_quote_atoms_e12>0
                     ||r.base_qty_atoms_e12==0||r.price_quote_atoms_e12==0
                     ||r.occurred_at_ms<input.window_from_ms
                     ||r.occurred_at_ms>input.captured_at_ms||r.occurred_at_ms<prev{
                     return Err(deny("foreign/duplicate/unsupported futures cover fill"))
                 }
                 qty=qty.checked_add(r.base_qty_atoms_e12).ok_or(V2EmergencyError::Overflow)?;
                 if qty>=target{return Err(deny("partial cover may not equal/exceed original short"))}
                 prev=r.occurred_at_ms;
             }
             let a=f.position_first.as_ref().ok_or(deny("first futures position snapshot absent"))?;
             let b=f.position_second.as_ref().ok_or(deny("second futures position snapshot absent"))?;
             for p in [a,b]{
                 if p.futures_symbol!=cp.plan.futures_symbol
                     ||p.claimed_prior_short_atoms_e12!=target
                     ||p.short_qty_atoms_e12!=target-qty
                     ||p.captured_at_ms<prev||p.captured_at_ms>input.captured_at_ms{
                     return Err(deny("partial futures position mismatch or pre-trade snapshot"))
                 }
             }
             if a.captured_at_ms>=b.captured_at_ms{
                 return Err(deny("futures position snapshots not distinct/in order"))
             }
             fut_count=Some(qty);
         }
     }
     match s.outcome{
         V2EmergencyLegOutcome::UnknownAcknowledgement=>{
             if s.order_id.is_some()||!s.fills.is_empty()||s.pages_complete
                 ||s.terminal_empty_page_seen||s.order_terminal_claimed{
                 return Err(deny("UNKNOWN OKX SELL must not invent zero fills/balance"))
             }
         }
         V2EmergencyLegOutcome::PartialExecution=>{
             let order=s.order_id.as_deref().ok_or(deny("OKX spot sell orderId absent"))?;
             if !canonical_id(order)||!s.pages_complete||!s.terminal_empty_page_seen
                 ||s.order_terminal_claimed||s.fills.is_empty()||s.fills.len()>128
                 ||Some(order)==f.order_id.as_deref(){
                 return Err(deny("partial OKX source pages, status or orderId invalid"))
             }
             let mut sold=0u128;let mut last=0u64;
             for r in &s.fills{
                 if !canonical_id(&r.trade_id)||!canonical_id(&r.bill_id)
                     ||!used_spot.insert(r.trade_id.as_str())
                     ||!used_bills.insert(r.bill_id.as_str())
                     ||r.order_id!=order||r.symbol!=cp.plan.spot_symbol.replace('/',"-")
                     ||r.side!="sell"||r.fee_currency!="USDT"
                     ||r.signed_fee_quote_atoms_e12>0
                     ||r.base_qty_atoms_e12==0||r.price_quote_atoms_e12==0
                     ||r.occurred_at_ms<input.window_from_ms
                     ||r.occurred_at_ms>input.captured_at_ms||r.occurred_at_ms<last{
                     return Err(deny("foreign/duplicate/unsupported OKX partial sell/bill"))
                 }
                 sold=sold.checked_add(r.base_qty_atoms_e12).ok_or(V2EmergencyError::Overflow)?;
                 if sold>=target{return Err(deny("partial OKX sell may not consume all BASE"))}
                 let expected_delta=i128::try_from(r.base_qty_atoms_e12)
                     .map_err(|_|V2EmergencyError::Overflow)?;
                 if r.base_balance_delta_e12!=-expected_delta
                     ||r.base_balance_after_e12!=target-sold{
                     return Err(deny("OKX partial BASE cash ancestry mismatch"))
                 }
                 last=r.occurred_at_ms;
             }
             spot_count=Some(sold);
         }
     }
     if let (Some(_),Some(_))=(fut_count,spot_count){
         let f_last=f.fills.iter().map(|r|r.occurred_at_ms).max().unwrap_or(0);
         let s_first=s.fills.iter().map(|r|r.occurred_at_ms).min().unwrap_or(0);
         if f_last>=s_first{return Err(deny("overlapping partial cover/spot sell needs B7 emergency sequencing"))}
     }
     let remaining_short=fut_count.map(|v|target-v);
     let remaining_spot=spot_count.map(|v|target-v);
     let net_exposure=match (remaining_spot,remaining_short){
         (Some(a),Some(b))=>Some(i128::try_from(a).map_err(|_|V2EmergencyError::Overflow)?
             -i128::try_from(b).map_err(|_|V2EmergencyError::Overflow)?),_=>None,
     };
     Ok(V2EmergencyRiskPreview{
         owned_net_base_atoms_e12:target,original_short_atoms_e12:target,
         observed_futures_cover_atoms_e12:fut_count,
         observed_spot_sell_atoms_e12:spot_count,
         remaining_short_atoms_e12:remaining_short,
         remaining_owned_spot_atoms_e12:remaining_spot,
         known_net_base_minus_short_atoms_e12:net_exposure,
         independently_account_proven:false,definitive_pnl_atoms_e12:None,
         may_send_retry_cancel_or_verify_closed:false,
     })
 }
