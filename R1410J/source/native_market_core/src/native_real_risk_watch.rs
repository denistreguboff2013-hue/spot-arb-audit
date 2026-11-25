//! R32D6C8B2B1 — read-only risk evidence and synthetic financial budgets.
//! NEVER establishes a VERIFIED REAL close or a real order permit.
//! The only genuinely persisted evidence is the existing bounded C6 joint
//! opening-leg receipt. Exit, net PNL, funding and all-day wallet inventory
//! remain UNKNOWN until separate exchange-sourced certification.
use serde::{Deserialize,Serialize};
use std::collections::BTreeSet;
use crate::{
 authoritative_state::{NativeEntryAuthority,NativeEntryPhase},
 native_daily_risk::yekt_day_index,
 native_trading_config::NativeTradingConfig,
};
const MAX_CLOSE_SCENARIO_ROWS:usize=3_000;
const MAX_AGE_MS:u64=60_000;
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum RealWatchError{NoSource,Unsupported,Invalid,Clock,Overflow,Policy}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum RealWatchWrite{Recorded,Idempotent}

/// Checkpoint is diagnostic, not a position transition or a license to trade.
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
pub struct NativeRealRiskObservation{
 pub source_generation:u64,
 pub captured_at_ms:u64,
 pub config_sha256:String,
 pub entry_policy_bound:bool,
 pub safety_generation:u64,
 pub spot_intent_id:String,
 pub futures_intent_id:String,
 pub spot_order_id:String,
 pub futures_order_id:String,
 pub spot_trade_ids:Vec<String>,
 pub futures_trade_ids:Vec<String>,
 pub observed_spot_fee_e12:i128,
 pub observed_futures_fee_e12:i128,
 pub known_entry_fee_e12:i128,
 pub outstanding_spot_exposure_e18:u128,
 pub verified_real_closure_at_ms:Option<u64>,
 pub verified_realized_net_pnl_e12:Option<i128>,
 pub verified_day_loss_e12:Option<u128>,
 pub verified_day_fees_e12:Option<u128>,
 pub verified_funding_complete:bool,
 pub independent_close_truth_complete:bool,
 pub cross_venue_atomicity_proven:bool,
 pub exchange_global_history_complete:bool,
 pub may_open_real:bool,
 pub may_send_order:bool,
 pub may_promote_hedged:bool,
}
impl NativeRealRiskObservation{
 pub fn from_bounded_entry(
  entry:&NativeEntryAuthority,source_generation:u64,
  now_ms:u64,config:&NativeTradingConfig
 )->Result<Self,RealWatchError>{
  if !config.source_projection_unchanged(){return Err(RealWatchError::Policy)}
  let proof=entry.prospective_joint_evidence.as_ref().ok_or(RealWatchError::NoSource)?;
  // R134: generation and clock must agree *before* forming a
  // diagnostic. A later state.validate would otherwise discover a
  // locally constructed but unusable observation.
  if entry.phase!=NativeEntryPhase::RecoveryRequired
   ||proof.source_generation.checked_add(1)!=Some(source_generation)
   ||proof.captured_at_ms>now_ms
   ||now_ms.saturating_sub(proof.captured_at_ms)>MAX_AGE_MS
   ||entry.policy_binding.as_ref().is_some_and(
      |b|b.config_sha256!=config.sha256)
   ||source_generation==0
   ||!proof.okx_balance_bracket_matched
   ||!proof.binance_income_and_position_bracket_matched
   ||proof.spot_trade_ids.is_empty()||proof.futures_trade_ids.is_empty()
   ||proof.cross_venue_exchange_snapshot_atomicity_proven
   ||proof.exchange_global_history_complete
   ||proof.may_send_order||proof.may_promote_hedged
   ||!proof.foreign_symbol_trades_absent_in_bounded_window
   ||config.policy.real_daily_trades!=0
   ||config.policy.real_execution_authorized
   ||config.policy.real_max_positions!=1
   ||config.policy.real_cooldown_sec!=10_800 {
   return Err(RealWatchError::Policy)
  }
  let total=proof.spot_fee_quote_atoms_e12
   .checked_add(proof.futures_fee_quote_atoms_e12)
   .ok_or(RealWatchError::Overflow)?;
  if proof.spot_fee_quote_atoms_e12<0||proof.futures_fee_quote_atoms_e12<0
    ||total<0{return Err(RealWatchError::Invalid)}
  Ok(Self{
   source_generation,captured_at_ms:now_ms,config_sha256:config.sha256.clone(),
   entry_policy_bound:entry.policy_binding.is_some(),
   safety_generation:proof.safety_generation,
   spot_intent_id:proof.spot_intent_id.clone(),
   futures_intent_id:proof.futures_intent_id.clone(),
   spot_order_id:proof.spot_order_id.clone(),
   futures_order_id:proof.futures_order_id.clone(),
   spot_trade_ids:proof.spot_trade_ids.clone(),
   futures_trade_ids:proof.futures_trade_ids.clone(),
   observed_spot_fee_e12:proof.spot_fee_quote_atoms_e12,
   observed_futures_fee_e12:proof.futures_fee_quote_atoms_e12,
   known_entry_fee_e12:total,
   outstanding_spot_exposure_e18:proof.exposed_spot_unhedged_atoms_e18,
   verified_real_closure_at_ms:None,
   verified_realized_net_pnl_e12:None,
   verified_day_loss_e12:None,verified_day_fees_e12:None,
   verified_funding_complete:false,
   independent_close_truth_complete:false,
   cross_venue_atomicity_proven:false,
   exchange_global_history_complete:false,
   may_open_real:false,may_send_order:false,may_promote_hedged:false,
  })
 }
 pub fn validate_against(&self,entry:&NativeEntryAuthority,
    current_generation:u64)->Result<(),RealWatchError>{
  let proof=entry.prospective_joint_evidence.as_ref()
    .ok_or(RealWatchError::NoSource)?;
  if self.source_generation==0||self.source_generation>=current_generation
    ||self.source_generation!=proof.source_generation.checked_add(1)
        .ok_or(RealWatchError::Overflow)?
    ||entry.phase!=NativeEntryPhase::RecoveryRequired
    ||self.entry_policy_bound!=entry.policy_binding.is_some()
    ||entry.policy_binding.as_ref().is_some_and(|b| b.config_sha256!=self.config_sha256)
    ||self.config_sha256.len()!=64
    ||!self.config_sha256.bytes().all(|c|
         c.is_ascii_hexdigit()&&!c.is_ascii_uppercase())
    ||self.captured_at_ms<proof.captured_at_ms
    ||self.captured_at_ms.saturating_sub(proof.captured_at_ms)>MAX_AGE_MS
    ||self.safety_generation!=proof.safety_generation
    ||self.spot_intent_id!=proof.spot_intent_id
    ||self.futures_intent_id!=proof.futures_intent_id
    ||self.spot_order_id!=proof.spot_order_id
    ||self.futures_order_id!=proof.futures_order_id
    ||self.spot_trade_ids!=proof.spot_trade_ids
    ||self.futures_trade_ids!=proof.futures_trade_ids
    ||self.observed_spot_fee_e12!=proof.spot_fee_quote_atoms_e12
    ||self.observed_futures_fee_e12!=proof.futures_fee_quote_atoms_e12
    ||self.known_entry_fee_e12!=self.observed_spot_fee_e12
       .checked_add(self.observed_futures_fee_e12).ok_or(RealWatchError::Overflow)?
    ||self.known_entry_fee_e12<0
    ||self.outstanding_spot_exposure_e18!=proof.exposed_spot_unhedged_atoms_e18
    ||self.verified_real_closure_at_ms.is_some()
    ||self.verified_realized_net_pnl_e12.is_some()
    ||self.verified_day_loss_e12.is_some()
    ||self.verified_day_fees_e12.is_some()
    ||self.verified_funding_complete
    ||self.independent_close_truth_complete
    ||self.cross_venue_atomicity_proven||self.exchange_global_history_complete
    ||self.may_open_real||self.may_send_order||self.may_promote_hedged{
   return Err(RealWatchError::Invalid)
  }
  // The opening-leg receipt can never certify an exit or total daily fees.
  Ok(())
 }
 pub fn real_entry_denial(&self)->&'static str{
  "Нельзя торговать: real=0/сутки, закрытие и дневные комиссии не подтверждены"
 }
}

/// These rows are SYNTHETIC mathematical scenarios, not authenticated proofs.
/// The function cannot generate any authority or persist a "verified close".
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct SimulatedClosedCycle{
 pub id:String,pub completed_at_ms:u64,
 pub quote_net_result_e12:Option<i128>,
 pub all_fees_e12:Option<u128>,
 pub okx_fees_e12:Option<u128>,
 pub funding_complete:bool,
 pub both_closes_confirmed:bool,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct SimulatedRealBudgetView{
 pub yekt_day:u64,pub finished_circuits_today:u64,
 pub loss_total_e12:u128,pub fees_total_e12:u128,
 pub okx_fees_total_e12:u128,
 pub cooldown_remaining_ms:u64,
 pub over_loss_cap:bool,pub over_fee_cap:bool,
 pub may_open_real:bool,
}
/// Mock-only exercise of future arithmetic. Caller-supplied "both closes"
/// booleans are NEVER accepted as independently verified exchange truth.
pub fn simulate_read_only_real_budgets(
 config:&NativeTradingConfig,now_ms:u64,
 cycles:&[SimulatedClosedCycle],
)->Result<SimulatedRealBudgetView,RealWatchError>{
 if !config.source_projection_unchanged(){return Err(RealWatchError::Policy)}
 let now_day=yekt_day_index(now_ms).map_err(|_|RealWatchError::Clock)?;
 if cycles.len()>MAX_CLOSE_SCENARIO_ROWS||config.policy.real_daily_trades!=0
   ||config.policy.real_execution_authorized{return Err(RealWatchError::Policy)}
 let mut ids=BTreeSet::new();
 let mut loss=0u128;
 let mut fees=0u128;
 let mut okx_fees=0u128;
 let mut count=0u64;
 let mut latest_close=None::<u64>;
 for cycle in cycles{
  if cycle.id.is_empty()||cycle.id.len()>96||!ids.insert(&cycle.id)
   ||cycle.completed_at_ms==0||cycle.completed_at_ms>now_ms
   ||!cycle.funding_complete||!cycle.both_closes_confirmed{
   return Err(RealWatchError::Unsupported)
  }
  let net=cycle.quote_net_result_e12.ok_or(RealWatchError::Unsupported)?;
  let cost=cycle.all_fees_e12.ok_or(RealWatchError::Unsupported)?;
  let okx=cycle.okx_fees_e12.ok_or(RealWatchError::Unsupported)?;
  if okx>cost{return Err(RealWatchError::Invalid)}
  latest_close=Some(latest_close.map_or(cycle.completed_at_ms,|v|v.max(cycle.completed_at_ms)));
  if yekt_day_index(cycle.completed_at_ms).map_err(|_|RealWatchError::Clock)?==now_day{
   count=count.checked_add(1).ok_or(RealWatchError::Overflow)?;
   if net<0{loss=loss.checked_add(net.unsigned_abs()).ok_or(RealWatchError::Overflow)?}
   fees=fees.checked_add(cost).ok_or(RealWatchError::Overflow)?;
   okx_fees=okx_fees.checked_add(okx).ok_or(RealWatchError::Overflow)?;
  }
 }
 let cooldown=if let Some(last)=latest_close{
  config.policy.real_cooldown_sec.checked_mul(1000).ok_or(RealWatchError::Overflow)?
   .saturating_sub(now_ms-last)
 }else{0};
 let max_loss=config.policy.daily_loss_cap_e18/1_000_000;
 let max_fee=config.policy.daily_okx_fee_cap_e18/1_000_000;
 Ok(SimulatedRealBudgetView{
  yekt_day:now_day,finished_circuits_today:count,
  loss_total_e12:loss,fees_total_e12:fees,okx_fees_total_e12:okx_fees,
  cooldown_remaining_ms:cooldown,
  over_loss_cap:loss>=max_loss,over_fee_cap:okx_fees>=max_fee,
  may_open_real:false,
 })
}
