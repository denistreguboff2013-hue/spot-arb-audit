//! R71: zero-network read-only recovery assessment from a PAPER mock fill checkpoint.
 //! A local checkpoint SHA and account-shaped mock rows are NOT independent
 //! exchange attestation. Every restart is quarantined; no order authority.
use crate::{
 native_trading_config::NativeTradingConfig,
 native_paper_partial_recovery_gate::{PaperPartialGate,PaperGateError},
 native_paper_leg_failure_review::{
  inspect_paper_leg_failure,LegFailurePhase,LegFailureReviewError
 },
};
use openssl::sha::sha256;
use serde_json::{json,Value};

#[derive(Debug,Clone,PartialEq,Eq)]
pub enum R71Error{Snapshot(PaperGateError),Review(LegFailureReviewError),Malformed}

fn phase_of(p:&PaperPartialGate,short:u128,delivered:u128)
 ->Result<LegFailurePhase,R71Error>{
 use LegFailurePhase::*;
 if let Some(u)=&p.unknown {
  if u.intent_id==p.spot.intent_id && u.client_order_id==p.spot.client_order_id{
   return Ok(SpotSubmitUnknown)
  }
  if p.shorts.iter().any(|x|x.intent_id==u.intent_id&&x.client_order_id==u.client_order_id){
   return Ok(FuturesShortUnknown)
  }
  if p.cover.as_ref().is_some_and(|x|x.intent_id==u.intent_id&&x.client_order_id==u.client_order_id){
   return Ok(FuturesCoverUnknown)
  }
  if p.sell.as_ref().is_some_and(|x|x.intent_id==u.intent_id&&x.client_order_id==u.client_order_id){
   return Ok(SpotSaleUnknown)
  }
  return Err(R71Error::Malformed)
 }
 if let Some(s)=&p.sell{
  return if s.terminal&&s.fee_evidence_complete&&s.filled_qty_atoms()==delivered
   {Ok(PaperModeledClosed)}else{Ok(SpotSalePartial)}
 }
 if let Some(c)=&p.cover{
  return if c.terminal&&c.fee_evidence_complete&&c.filled_qty_atoms()==short
    {Ok(FuturesCoveredAwaitingSpotSale)}else{Ok(FuturesCoverPartial)}
 }
 if !p.spot.terminal{return Ok(SpotPartial)}
 if p.shorts.iter().any(|x|!x.terminal||!x.fee_evidence_complete){
  return Ok(FuturesShortPartial)
 }
 if delivered>short{return Ok(SpotConfirmedAwaitingHedge)}
 if delivered==short&&short>0{return Ok(Hedged)}
 Err(R71Error::Malformed)
}
/// Verify any persisted quote fee aggregate against its individual mock
/// fills. Missing quote/base fee evidence is UNKNOWN, never zero.
fn quote_fee_check(p:&PaperPartialGate,spot_fee_complete:bool)
 ->Result<(i128,bool),R71Error>{
 let mut grand_total=0i128;
 let mut complete=spot_fee_complete;
 let base=p.spot.symbol.strip_suffix("USDT").ok_or(R71Error::Malformed)?;
 for leg in std::iter::once(&p.spot).chain(p.shorts.iter())
   .chain(p.cover.iter()).chain(p.sell.iter()){
  if !std::ptr::eq(leg,&p.spot)&&!leg.fee_evidence_complete{
   complete=false
  }
  let mut from_rows=0i128;
  for row in leg.fills.values(){
   match (row.fee_currency.as_deref(),row.fee_cost_quote_atoms){
    (Some("USDT"),Some(amount))=>{
     from_rows=from_rows.checked_add(amount).ok_or(R71Error::Malformed)?;
    },
    (Some(ccy),None) if ccy==base&&std::ptr::eq(leg,&p.spot)=>{},
    _=>complete=false,
   }
  }
  if from_rows!=leg.fee_cost_quote_atoms{return Err(R71Error::Malformed)}
  grand_total=grand_total.checked_add(from_rows).ok_or(R71Error::Malformed)?;
 }
 Ok((grand_total,complete))
}
/// The caller supplies MOCK local checkpoint bytes; their hash is not a
/// signature and must never be used as independent exchange proof.
pub fn inspect_mock_paper_restart(
 checkpoint:&[u8],cfg:&NativeTradingConfig,phase_age_ms:u64,
 adverse_spread_bps:Option<f64>
)->Result<Value,R71Error>{
 if checkpoint.is_empty()||checkpoint.len()>2*1024*1024{
  return Err(R71Error::Malformed)
 }
 let restored=PaperPartialGate::restart_from_checkpoint(checkpoint)
  .map_err(R71Error::Snapshot)?;
 let exposure=restored.exposure().map_err(R71Error::Snapshot)?;
 let close=restored.paper_close_exposure().map_err(R71Error::Snapshot)?;
 let (quote_fee_sum,quote_fee_proof_complete)=quote_fee_check(&restored,exposure.spot_fee_complete)?;
 if !restored.restart_quarantine||!exposure.unknown_or_quarantine
  ||restored.exchange_truth_authenticated||restored.may_send_real_or_cancel
  ||exposure.spot_net_delivered_atoms<exposure.futures_filled_atoms
  ||close.opened_short_atoms!=exposure.futures_filled_atoms
  ||close.original_net_base_atoms!=exposure.spot_net_delivered_atoms{
  return Err(R71Error::Malformed)
 }
 let phase=phase_of(&restored,exposure.futures_filled_atoms,
  exposure.spot_net_delivered_atoms)?;
 let failure=inspect_paper_leg_failure(cfg,phase,phase_age_ms,adverse_spread_bps)
  .map_err(R71Error::Review)?;
 let hash=sha256(checkpoint).iter().map(|b|format!("{b:02x}"))
  .collect::<String>();
 Ok(json!({
  "schema":"r71_restart_paper_ledger_recovery_readonly_v1",
  "checkpoint_sha256_untrusted_source":hash,
  "source_config_sha256":cfg.sha256,
  "owner_generation":restored.generation,
  "fee_proof_complete_mock":exposure.spot_fee_complete,
  "known_quote_fee_cost_atoms_e12":quote_fee_sum.to_string(),
  "all_quote_and_base_fee_evidence_complete_mock":quote_fee_proof_complete,
  "spot_filled_base_atoms_e12":exposure.spot_filled_atoms.to_string(),
  "spot_base_fee_atoms_e12":exposure.spot_base_fee_atoms.to_string(),
  "spot_net_delivered_atoms_e12":exposure.spot_net_delivered_atoms.to_string(),
  "futures_short_filled_atoms_e12":exposure.futures_filled_atoms.to_string(),
  "unhedged_base_atoms_e12":exposure.unhedged_base_atoms.to_string(),
  "futures_cover_filled_atoms_e12":close.cover_filled_atoms.to_string(),
  "spot_sold_atoms_e12":close.spot_sold_atoms.to_string(),
  "remaining_short_atoms_e12":close.remaining_short_atoms.to_string(),
  "remaining_spot_atoms_e12":close.remaining_spot_base_atoms.to_string(),
  "source_shaped_mock_close_complete":close.mock_both_closing_legs_complete,
  "failure":failure,
  "restart_quarantine_engaged":true,
  "independently_exchange_authenticated":false,
  "durable_checkpoint_origin_authenticated":false,
  "real_profit_attested":false,
  "may_unquarantine_or_retry_or_send_order":false,
  "may_mark_exchange_flat":false,
  "may_write_to_working_paper":false
 }))
}

#[cfg(test)]
mod tests{
 use super::*;
 use crate::{
  fill_economics::{decimal_to_atoms,FillEvent},
  native_paper_partial_recovery_gate::PaperOrderReport,
  native_trade2f_asset_fee::SpotFeeFill,
 };
 fn n(s:&str)->u128{decimal_to_atoms(s).unwrap()}
 fn cfg()->NativeTradingConfig{
  NativeTradingConfig::parse(include_str!("../config/native_trading.yaml")).unwrap()
 }
 fn start()->PaperPartialGate{
  PaperPartialGate::new(7,"review_owner","ONEUSDT","buy1","buy_client1",n("10")).unwrap()
 }
 fn fill(id:&str,qty:&str,base_fee:bool)->FillEvent{
  FillEvent{fill_id:id.into(),price_atoms:n("1"),qty_atoms:n(qty),
   fee_cost_quote_atoms:if base_fee{None}else{Some(0)},
   fee_currency:Some(if base_fee{"ONE"}else{"USDT"}.into()),
   trade_time_ms:Some(1234)}
 }
 fn report(intent:&str,client:&str,order:&str,qty:&str,
  terminal:bool,rows:Vec<FillEvent>)->PaperOrderReport{
  PaperOrderReport{
   intent_id:intent.into(),client_order_id:client.into(),
   exchange_order_id:order.into(),cumulative_qty_atoms:n(qty),
   average_price_atoms:Some(n("1")),terminal,
   status:if terminal{
    if qty=="10"||qty=="9"{"filled".into()}else{"canceled".into()}
   }else{"partially_filled".into()},
   observed_at_ms:1234,fills:rows
  }
 }
 fn quote_spot()->PaperPartialGate{
  let mut p=start();
  p.observe_spot(&report("buy1","buy_client1","order_buy","10",
    true,vec![fill("b1","10",false)])).unwrap();
  p
 }
 fn net_fee_spot()->PaperPartialGate{
  let mut p=start();
  p.observe_spot(&report("buy1","buy_client1","order_buy","10",
    true,vec![fill("b1","10",true)])).unwrap();
  p.record_native_spot_fee(vec![SpotFeeFill{
   order_id:"order_buy".into(),trade_id:"b1".into(),at_ms:1234,
   side:"buy".into(),inst_id:"ONE-USDT".into(),
   price_quote_e12:n("1"),gross_base_qty_e12:n("10"),
   fee_ccy:"ONE".into(),signed_fee_e12:-(n("1") as i128)
  }]).unwrap();
  p
 }
 fn hedged_fee()->PaperPartialGate{
  let mut p=net_fee_spot();
  p.begin_paper_short("short1","short_client1",n("9")).unwrap();
  p.observe_short(&report("short1","short_client1","order_short","9",
    true,vec![fill("h1","9",false)])).unwrap();
  p
 }
 fn check(p:&PaperPartialGate)->Value{
  inspect_mock_paper_restart(&p.paper_checkpoint().unwrap(),&cfg(),250,None).unwrap()
 }
 #[test]
 fn partial_spot_survives_restart_without_invented_unfilled_zero(){
  let mut p=start();
  p.observe_spot(&report("buy1","buy_client1","order_buy","4",
   false,vec![fill("b1","4",false)])).unwrap();
  let r=check(&p);
  assert_eq!(r["spot_filled_base_atoms_e12"],n("4").to_string());
  assert_eq!(r["unhedged_base_atoms_e12"],n("4").to_string());
  assert_eq!(r["failure"]["phase"],"SPOT_PARTIAL");
  assert_eq!(r["restart_quarantine_engaged"],true);
  assert_eq!(r["may_unquarantine_or_retry_or_send_order"],false);
 }
 #[test]
 fn unknown_spot_ack_survives_checkpoint_and_never_retries(){
  let mut p=start();p.mark_unknown("buy1","buy_client1","network result unknown").unwrap();
  let r=check(&p);
  assert_eq!(r["failure"]["phase"],"SPOT_SUBMIT_UNKNOWN");
  assert_eq!(r["failure"]["unknown_result"],true);
  assert_eq!(r["failure"]["may_retry_send_or_cancel_order"],false);
  assert_eq!(r["durable_checkpoint_origin_authenticated"],false);
 }
 #[test]
 fn spot_fee_in_base_correctly_debits_available_hedge_qty(){
  let r=check(&net_fee_spot());
  assert_eq!(r["spot_filled_base_atoms_e12"],n("10").to_string());
  assert_eq!(r["spot_base_fee_atoms_e12"],n("1").to_string());
  assert_eq!(r["spot_net_delivered_atoms_e12"],n("9").to_string());
  assert_eq!(r["unhedged_base_atoms_e12"],n("9").to_string());
  assert_eq!(r["fee_proof_complete_mock"],true);
  assert_eq!(r["failure"]["phase"],"SPOT_CONFIRMED_AWAITING_HEDGE");
  assert_eq!(r["may_unquarantine_or_retry_or_send_order"],false);
 }
 #[test]
 fn futures_partial_leaves_exact_remaining_exposure_after_restart(){
  let mut p=net_fee_spot();
  p.begin_paper_short("short1","short_client1",n("9")).unwrap();
  p.observe_short(&report("short1","short_client1","order_short","5",
    false,vec![fill("h1","5",false)])).unwrap();
  let r=check(&p);
  assert_eq!(r["spot_net_delivered_atoms_e12"],n("9").to_string());
  assert_eq!(r["futures_short_filled_atoms_e12"],n("5").to_string());
  assert_eq!(r["unhedged_base_atoms_e12"],n("4").to_string());
  assert_eq!(r["failure"]["phase"],"FUTURES_SHORT_PARTIAL");
  assert_eq!(r["failure"]["partial_fill"],true);
 }
 #[test]
 fn unknown_futures_after_partial_never_assumes_no_short(){
  let mut p=net_fee_spot();
  p.begin_paper_short("short1","short_client1",n("9")).unwrap();
  p.observe_short(&report("short1","short_client1","order_short","5",
    false,vec![fill("h1","5",false)])).unwrap();
  p.mark_unknown("short1","short_client1","ack interrupted").unwrap();
  let r=check(&p);
  assert_eq!(r["failure"]["phase"],"FUTURES_SHORT_UNKNOWN");
  assert_eq!(r["futures_short_filled_atoms_e12"],n("5").to_string());
  assert_eq!(r["unhedged_base_atoms_e12"],n("4").to_string());
  assert_eq!(r["may_mark_exchange_flat"],false);
 }
 #[test]
 fn fully_mock_hedged_restart_still_quarantines_even_at_zero_gap(){
  let r=check(&hedged_fee());
  assert_eq!(r["failure"]["phase"],"HEDGED");
  assert_eq!(r["unhedged_base_atoms_e12"],"0");
  assert_eq!(r["remaining_short_atoms_e12"],n("9").to_string());
  assert_eq!(r["remaining_spot_atoms_e12"],n("9").to_string());
  assert_eq!(r["restart_quarantine_engaged"],true);
  assert_eq!(r["may_mark_exchange_flat"],false);
 }
 #[test]
 fn unknown_cover_preserves_both_exposures_after_restart(){
  let mut p=hedged_fee();
  p.begin_paper_cover("cover1","cover_client1",n("9")).unwrap();
  p.mark_unknown("cover1","cover_client1","timeout").unwrap();
  let r=check(&p);
  assert_eq!(r["failure"]["phase"],"FUTURES_COVER_UNKNOWN");
  assert_eq!(r["remaining_short_atoms_e12"],n("9").to_string());
  assert_eq!(r["remaining_spot_atoms_e12"],n("9").to_string());
  assert_eq!(r["may_unquarantine_or_retry_or_send_order"],false);
 }
 #[test]
 fn partial_cover_and_spot_sell_remain_not_flat_after_restart(){
  let mut p=hedged_fee();
  p.begin_paper_cover("cover1","cover_client1",n("9")).unwrap();
  p.observe_paper_cover(&report("cover1","cover_client1","order_cover","4",
    false,vec![fill("c1","4",false)])).unwrap();
  let r=check(&p);
  assert_eq!(r["failure"]["phase"],"FUTURES_COVER_PARTIAL");
  assert_eq!(r["remaining_short_atoms_e12"],n("5").to_string());
  assert_eq!(r["remaining_spot_atoms_e12"],n("9").to_string());
  assert_eq!(r["source_shaped_mock_close_complete"],false);
  p.observe_paper_cover(&report("cover1","cover_client1","order_cover","9",
    true,vec![fill("c2","5",false)])).unwrap();
  p.begin_paper_sell("sell1","sell_client1",n("9")).unwrap();
  p.observe_paper_sell(&report("sell1","sell_client1","order_sell","3",
    false,vec![fill("s1","3",false)])).unwrap();
  let r=check(&p);
  assert_eq!(r["failure"]["phase"],"SPOT_SALE_PARTIAL");
  assert_eq!(r["remaining_short_atoms_e12"],"0");
  assert_eq!(r["remaining_spot_atoms_e12"],n("6").to_string());
  assert_eq!(r["may_mark_exchange_flat"],false);
 }
 #[test]
 fn four_mock_legs_completed_not_a_real_closed_confirmation(){
  let mut p=hedged_fee();
  p.begin_paper_cover("cover1","cover_client1",n("9")).unwrap();
  p.observe_paper_cover(&report("cover1","cover_client1","order_cover","9",
    true,vec![fill("c1","9",false)])).unwrap();
  p.begin_paper_sell("sell1","sell_client1",n("9")).unwrap();
  p.observe_paper_sell(&report("sell1","sell_client1","order_sell","9",
    true,vec![fill("s1","9",false)])).unwrap();
  assert!(p.paper_close_exposure().unwrap().mock_both_closing_legs_complete);
  let r=check(&p);
  assert_eq!(r["failure"]["phase"],"PAPER_MODELED_CLOSED");
  assert_eq!(r["remaining_short_atoms_e12"],"0");
  assert_eq!(r["remaining_spot_atoms_e12"],"0");
  assert_eq!(r["source_shaped_mock_close_complete"],false);
  assert_eq!(r["may_mark_exchange_flat"],false);
  assert_eq!(r["independently_exchange_authenticated"],false);
 }
 #[test]
 fn missing_native_fee_bill_cannot_be_reclassified_as_complete(){
  let mut p=start();
  p.observe_spot(&report("buy1","buy_client1","order_buy","10",
    true,vec![fill("b1","10",true)])).unwrap();
  let r=check(&p);
  assert_eq!(r["fee_proof_complete_mock"],false);
  assert_eq!(r["spot_net_delivered_atoms_e12"],n("10").to_string());
  assert_eq!(r["failure"]["phase"],"SPOT_CONFIRMED_AWAITING_HEDGE");
  assert_eq!(r["may_unquarantine_or_retry_or_send_order"],false);
 }
 #[test]
 fn quote_fee_rows_survive_restart_without_rounding_or_double_counting(){
  let mut p=start();
  let mut quoted=fill("b1","10",false);
  quoted.fee_cost_quote_atoms=Some(n("0.01") as i128);
  p.observe_spot(&report("buy1","buy_client1","order_buy","10",
   true,vec![quoted])).unwrap();
  let r=check(&p);
  assert_eq!(r["known_quote_fee_cost_atoms_e12"],n("0.01").to_string());
  assert_eq!(r["all_quote_and_base_fee_evidence_complete_mock"],true);
  assert_eq!(r["spot_base_fee_atoms_e12"],"0");
  assert_eq!(r["real_profit_attested"],false);
 }
 #[test]
 fn forged_aggregate_fee_is_detected_even_if_checkpoint_has_valid_json(){
  let mut p=quote_spot();
  p.spot.fee_cost_quote_atoms=n("0.02") as i128;
  let forged=serde_json::to_vec(&p).unwrap();
  assert_eq!(inspect_mock_paper_restart(&forged,&cfg(),250,None),
   Err(R71Error::Malformed));
 }
 #[test]
 fn forged_order_authority_in_serialized_snapshot_is_rejected(){
  let mut p=quote_spot();
  p.may_send_real_or_cancel=true;
  let raw=serde_json::to_vec(&p).unwrap();
  assert!(matches!(inspect_mock_paper_restart(&raw,&cfg(),50,None),
    Err(R71Error::Snapshot(PaperGateError::BadSnapshot))));
  assert_eq!(inspect_mock_paper_restart(&[],&cfg(),50,None),
    Err(R71Error::Malformed));
 }
 #[test]
 fn real_permission_or_malformed_timing_cannot_pass_policy(){
  let bytes=quote_spot().paper_checkpoint().unwrap();
  let mut c=cfg();c.policy.real_execution_authorized=true;
  assert!(matches!(inspect_mock_paper_restart(&bytes,&c,50,None),
    Err(R71Error::Review(LegFailureReviewError::Config(_)))));
  assert_eq!(inspect_mock_paper_restart(&bytes,&cfg(),50,Some(f64::NAN)),
    Err(R71Error::Review(LegFailureReviewError::Malformed)));
 }
}
