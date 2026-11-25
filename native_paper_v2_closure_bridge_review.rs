//! Astra P0-02: strict zero-network CROSS-CHECK between the paper CAS owner's
//! raw-shaped four legs and the previously validated source-shaped V2 review.
//! Matching two unauthenticated mock witnesses NEVER authenticates either.
use crate::{
  native_paper_durable_journal_review::RecoveredPaper,
  native_paper_partial_recovery_gate::PaperPartialGate,
  native_trade2f3_v2_versioned_store::V2ReviewState,
  native_trade2f3_v2_four_leg_close_review::{
    V2FourLegCloseObservation,inspect_v2_four_leg_close,
  },
  fill_economics::LegFillLedger,
};
use openssl::sha::sha256;
use crate::position_lifecycle::{PositionLifecycle,LifecycleState,CloseLeg};
use serde::{Serialize,Deserialize};
#[derive(Debug,Clone,PartialEq,Eq)]
pub enum BridgeError { Denied(&'static str), Encoding }
fn deny(s:&'static str)->BridgeError{BridgeError::Denied(s)}
#[derive(Debug,Clone,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaperV2LocalAlignment{
  pub original_generation:u64,
  pub paper_parent_seq:u64,
  pub v2_source_revision:u64,
  pub source_digest_sha256:String,
  pub independent_exchange_truth:bool,
  pub verified_closed:bool,
  pub may_send_or_retry:bool,
}
/// Read-only consistency check of an already-quarantined paper journal.
/// This does not clear any stored quarantine flag or authorize a retry.
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct RecoveryAlignmentDiagnostic{
 pub source_agrees_with_local_journal:bool,
 pub may_resume_paper:bool,
 pub may_trade_real:bool,
 pub verified_closed:bool,
}
pub fn inspect_recovered_alignment_consistency(
 recovered:&RecoveredPaper,base:&str,
 v2:&V2ReviewState,obs:&V2FourLegCloseObservation,
)->Result<RecoveryAlignmentDiagnostic,BridgeError>{
 let row=recovered.state.positions.get(base)
   .ok_or(deny("recovered owner position absent"))?;
 if row.phase!="EXIT_PENDING"||!row.quarantine||
    !row.fills.restart_quarantine||
    recovered.may_resume_paper||recovered.may_trade_real{
    return Err(deny("recovery must retain quarantine"))
 }
 let marker=row.fills.v2_local_alignment.as_ref()
   .ok_or(deny("recovered source marker absent"))?;
 if recovered.sequence<=marker.paper_parent_seq{
    return Err(deny("alignment marker revision not committed"))
 }
 // The temporary clone exists solely to run the strict local comparator;
 // the original in RecoveredPaper remains quarantined and non-executable.
 let mut clone=row.fills.clone();
 clone.restart_quarantine=false;
 let expected=inspect_paper_v2_alignment(&clone,v2,obs,marker.paper_parent_seq)?;
 if &expected!=marker{
    return Err(deny("V2 source diverged from durable alignment marker"))
 }
 Ok(RecoveryAlignmentDiagnostic{
    source_agrees_with_local_journal:true,
    may_resume_paper:false,may_trade_real:false,verified_closed:false,
 })
}
/// R86: exactly correlate an unresolved R15 close intent with the already
/// V2-source-matched PAPER CAS. Cross-store correctness is READ-ONLY.
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct R15V2PaperRecoveryJoin{
 pub locally_matched_generation:u64,
 pub paper_sequence:u64,
 pub pending_leg:CloseLeg,
 pub identical_local_pending_intent:bool,
 pub r15_pending_reconciliation_required:bool,
 pub cross_store_atomic_durability_proven:bool,
 pub immutable_r15_original_owner_order_id_proven:bool,
 pub independent_exchange_account_truth:bool,
 pub may_retry_or_resume:bool,
 pub verified_closed:bool,
}
/// R15 can stage a caller-supplied order ID write-once during quarantine.
/// A local durable ID match is not authenticated original exchange provenance.
/// Matching it must NEVER unlock an R15/V2/PAPER action.
pub fn inspect_r15_v2_paper_recovery_join(
 r15:&PositionLifecycle,recovered:&RecoveredPaper,base:&str,
 v2:&V2ReviewState,obs:&V2FourLegCloseObservation,
)->Result<R15V2PaperRecoveryJoin,BridgeError>{
 let local=inspect_recovered_alignment_consistency(recovered,base,v2,obs)?;
 if !local.source_agrees_with_local_journal||local.may_resume_paper||
    local.may_trade_real||local.verified_closed{
   return Err(deny("local source alignment cannot grant authority"))
 }
 let original_owner=v2.genesis.original_owner_generation;
 let net=v2.genesis.planned_net_short_atoms_e12;
 if original_owner==0||net==0||
    r15.safety_generation!=original_owner||r15.target_units!=net||
    r15.state!=LifecycleState::RecoveryRequired||
    r15.futures_filled_units>net||r15.spot_filled_units>net||
    r15.spot_filled_units>r15.futures_filled_units||
    r15.closed_observations>0{
    return Err(deny("R15 generation, quantity or close state differs"))
 }
 let pending=r15.pending.as_ref()
   .ok_or(deny("R15 has no durable unresolved close intent"))?;
 if pending.intent_id.is_empty()||pending.client_id.is_empty()||
    pending.requested_units==0||pending.requested_units>net||
    pending.recovery_terminal_filled_units.is_some_and(|n|
      n>pending.requested_units){
    return Err(deny("R15 pending intent quantity or evidence malformed"))
 }
 let row=recovered.state.positions.get(base)
   .ok_or(deny("PAPER CAS source position absent"))?;
 let leg=match pending.leg{
  CloseLeg::Futures=>{
    if r15.spot_filled_units!=0{return Err(deny("spot exit before futures cover"))}
    row.fills.cover.as_ref().ok_or(deny("PAPER futures cover absent"))?
  },
  CloseLeg::Spot=>{
    if r15.futures_filled_units!=net{
      return Err(deny("spot close intent without locally complete futures cover"))
    }
    row.fills.sell.as_ref().ok_or(deny("PAPER spot sell absent"))?
  },
 };
 // R15 may have fsynced a terminal quantity before R14 reconciliation.
 // An independently persisted PAPER terminal observation of a different
 // quantity is a cross-store conflict, never a matching pending intent.
 if pending.recovery_terminal_filled_units.is_some_and(|staged|
    !leg.terminal || !leg.cumulative_truth_present ||
    staged!=leg.filled_qty_atoms()){
    return Err(deny("R15 durable terminal quantity differs from V2/PAPER leg"))
 }
 let (wanted_intent,wanted_client,wanted_order)=match pending.leg{
  CloseLeg::Futures=>(&obs.futures_cover_intent_id,&obs.futures_cover_client_id,
                      &obs.futures_cover_order_id),
  CloseLeg::Spot=>(&obs.spot_sell_intent_id,&obs.spot_sell_client_id,
                   &obs.spot_sell_order_id),
 };
 let remains=r15.remaining_units(pending.leg);
 if pending.intent_id!=*wanted_intent||pending.client_id!=*wanted_client||
    pending.recovery_original_exchange_order_id.as_deref()!=Some(wanted_order.as_str())||
    leg.exchange_order_id.as_deref()!=Some(wanted_order.as_str())||
    pending.intent_id!=leg.intent_id||pending.client_id!=leg.client_order_id||
    pending.requested_units!=leg.requested_qty_atoms||
    pending.requested_units>remains{
    return Err(deny("R15 unresolved close intent is not exact V2/PAPER owner"))
 }
 Ok(R15V2PaperRecoveryJoin{
  locally_matched_generation:original_owner,paper_sequence:recovered.sequence,
  pending_leg:pending.leg,identical_local_pending_intent:true,
  r15_pending_reconciliation_required:true,
  cross_store_atomic_durability_proven:false,
  immutable_r15_original_owner_order_id_proven:false,
  independent_exchange_account_truth:false,
  may_retry_or_resume:false,verified_closed:false,
 })
}
fn linked(leg:&LegFillLedger,intent:&str,client:&str,order:&str,
           qty:u128,require_quote_fee_complete:bool)->Result<(),BridgeError>{
  if leg.intent_id!=intent||leg.client_order_id!=client||
     leg.exchange_order_id.as_deref()!=Some(order)||
     !leg.terminal||(require_quote_fee_complete&&!leg.fee_evidence_complete)||
     !leg.cumulative_truth_present||leg.filled_qty_atoms()!=qty||
     leg.fills.is_empty(){return Err(deny("leg identity, terminal or quantity mismatch"))}
  Ok(())
}
fn same_fee(f:&crate::fill_economics::FillEvent, signed:i128)->bool{
  signed.checked_neg().and_then(|v|u128::try_from(v).ok())
    ==f.fee_cost_quote_atoms
    .and_then(|v|u128::try_from(v).ok())
}
/// Invoke only with exact expected paper parent revision and both source objects.
/// A PASS establishes consistency of mock provenance, NOT private API attestation.
pub fn inspect_paper_v2_alignment(
  gate:&PaperPartialGate, v2:&V2ReviewState, obs:&V2FourLegCloseObservation,
  paper_parent_seq:u64,
)->Result<PaperV2LocalAlignment,BridgeError>{
  if paper_parent_seq==0 || gate.restart_quarantine||
    gate.unknown.is_some()||gate.exchange_truth_authenticated||
    gate.may_send_real_or_cancel {
    return Err(deny("wrong paper generation/quarantine or forged authority"))
  }
  let reviewed=inspect_v2_four_leg_close(v2,obs)
    .map_err(|_|deny("V2 original/closing fee and bill ancestry invalid"))?;
  let original=&v2.genesis.checkpoint;
  let net=v2.genesis.planned_net_short_atoms_e12;
  let gross=reviewed.original_spot_buy_gross_e12;
  let paper=gate.paper_close_exposure()
    .map_err(|_|deny("invalid CAS paper ledger"))?;
  if gate.generation!=v2.genesis.original_owner_generation||
     original.original_native_generation!=gate.generation||
     !paper.mock_both_closing_legs_complete||
     paper.original_net_base_atoms!=net||
     paper.opened_short_atoms!=net||
     paper.cover_filled_atoms!=net||paper.spot_sold_atoms!=net||
     paper.remaining_short_atoms!=0||paper.remaining_spot_base_atoms!=0||
     paper.real_execution_authorized||paper.independently_verified_closed||
     gate.spot.symbol!=original.plan.futures_symbol||
     gate.spot.filled_qty_atoms()!=gross||
     gate.spot.venue!="okx"||gate.spot.side!="buy"{
    return Err(deny("paper vs V2 generation, instrument, net quantity or mock authority"))
  }
  linked(&gate.spot,&original.original.spot_intent_id,
         &original.original.spot_client_id,
         &original.original.spot_order_id,gross,false)?;
  let rows=&original.source_shaped_fee_fills;
  let fee=gate.native_spot_fee.as_ref()
     .ok_or(deny("missing original OKX native fee provenance"))?;
  if fee.rows.len()!=rows.len()||gate.spot.fills.len()!=rows.len()||
     fee.total_debited_base_atoms!=reviewed.original_base_fee_e12{
    return Err(deny("first-leg fee ancestor cardinality mismatch"))
  }
  for row in rows{
    let corresponding=fee.rows.iter().find(|r|r.trade_id==row.trade_id)
        .ok_or(deny("missing original OKX fee row"))?;
    if corresponding!=row{return Err(deny("original OKX fee source different"))}
    let fill=gate.spot.fills.get(&row.trade_id)
        .ok_or(deny("missing original OKX fill"))?;
    if fill.qty_atoms!=row.gross_base_qty_e12||
       fill.price_atoms!=row.price_quote_e12||
       fill.fee_currency.as_deref()!=Some(row.fee_ccy.as_str())||
       fill.trade_time_ms!=Some(row.at_ms){
        return Err(deny("original OKX execution ancestry mismatch"))
    }
  }
  let c3=v2.c3_futures.as_ref().ok_or(deny("no V2 Binance opening"))?;
  if gate.shorts.len()!=1||c3.rows.is_empty()||
     c3.rows.len()!=gate.shorts[0].fills.len(){
     return Err(deny("Binance open orders do not match one owned paper leg"))
  }
  let short=&gate.shorts[0];
  linked(short,&v2.genesis.futures_intent_id,&v2.genesis.futures_client_id,
         c3.order_id.as_deref().ok_or(deny("missing Binance opening order"))?,net,true)?;
  for r in &c3.rows{
    let f=short.fills.get(&r.trade_id).ok_or(deny("missing short trade"))?;
    if f.qty_atoms!=r.base_qty_atoms_e12||
      f.price_atoms!=r.price_quote_atoms_e12||
      f.trade_time_ms!=Some(r.occurred_at_ms)||
      f.fee_currency.as_deref()!=Some(r.fee_currency.as_str())||
      !same_fee(f,r.signed_fee_quote_atoms_e12){
      return Err(deny("Binance SELL fee, quantity or trade ID mismatch"))
    }
  }
  let cover=gate.cover.as_ref().ok_or(deny("missing Binance cover"))?;
  let sell=gate.sell.as_ref().ok_or(deny("missing OKX sell"))?;
  linked(cover,&obs.futures_cover_intent_id,&obs.futures_cover_client_id,
         &obs.futures_cover_order_id,net,true)?;
  linked(sell,&obs.spot_sell_intent_id,&obs.spot_sell_client_id,
         &obs.spot_sell_order_id,net,true)?;
  if cover.fills.len()!=obs.futures_cover_rows.len()||
     sell.fills.len()!=obs.spot_sell_rows.len(){
     return Err(deny("closing source row count mismatch"))
  }
  for r in &obs.futures_cover_rows{
    let f=cover.fills.get(&r.trade_id).ok_or(deny("missing Binance cover trade"))?;
    if f.qty_atoms!=r.base_qty_atoms_e12||f.price_atoms!=r.price_quote_atoms_e12||
       f.trade_time_ms!=Some(r.occurred_at_ms)||
       f.fee_currency.as_deref()!=Some(r.fee_currency.as_str())||
       !same_fee(f,r.signed_fee_quote_atoms_e12){
       return Err(deny("Binance BUY cover fill differs from V2 source"))
    }
  }
  for r in &obs.spot_sell_rows{
    let f=sell.fills.get(&r.trade_id).ok_or(deny("missing OKX closing trade"))?;
    if f.qty_atoms!=r.base_qty_atoms_e12||f.price_atoms!=r.price_quote_atoms_e12||
       f.trade_time_ms!=Some(r.occurred_at_ms)||
       f.fee_currency.as_deref()!=Some(r.fee_currency.as_str())||
       !same_fee(f,r.signed_fee_quote_atoms_e12){
       return Err(deny("OKX SELL fee, quantity or trade ID mismatch"))
    }
  }
  let bytes=serde_json::to_vec(&(v2,obs,paper_parent_seq))
    .map_err(|_|BridgeError::Encoding)?;
  let digest=sha256(&bytes).iter().map(|b|format!("{b:02x}")).collect();
  Ok(PaperV2LocalAlignment{
    original_generation:gate.generation,
    paper_parent_seq,v2_source_revision:v2.revision,
    source_digest_sha256:digest,
    independent_exchange_truth:false,verified_closed:false,may_send_or_retry:false,
  })
}
