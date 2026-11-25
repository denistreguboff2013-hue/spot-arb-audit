//! R69: YAML exit/recovery/stop settings, strict READ-ONLY review.
 //! Configuration fields are not exchange execution capabilities or permits.
use crate::native_trading_config::NativeTradingConfig;
use serde_json::{json,Value};

#[derive(Debug,Clone,PartialEq,Eq)]
pub enum ExitReviewError{Policy,Missing,Range}
fn section<'a>(root:&'a serde_yaml::Value,key:&str)->Result<&'a serde_yaml::Value,ExitReviewError>{
 root.get(key).ok_or(ExitReviewError::Missing)
}
fn boolean(root:&serde_yaml::Value,key:&str)->Result<bool,ExitReviewError>{
 section(root,key)?.as_bool().ok_or(ExitReviewError::Missing)
}
fn integer(root:&serde_yaml::Value,key:&str,max:u64)->Result<u64,ExitReviewError>{
 let n=section(root,key)?.as_u64().ok_or(ExitReviewError::Missing)?;
 if n>max{return Err(ExitReviewError::Range)}
 Ok(n)
}
pub fn inspect_exit_safety_yaml(cfg:&NativeTradingConfig)->Result<Value,ExitReviewError>{
 // R94: provenance is bound to the original parsed source, not mutable raw fields.
 if !cfg.source_projection_unchanged(){return Err(ExitReviewError::Policy)}
 if cfg.policy.real_execution_authorized||cfg.policy.real_daily_trades!=0
  ||cfg.sha256.len()!=64{return Err(ExitReviewError::Policy)}
 let exit=section(&cfg.raw,"exit_safety")?;
 let chase=section(exit,"spot_chase")?;
 let emergency=section(&cfg.raw,"futures_emergency_stop")?;
 let exit_enabled=boolean(exit,"enabled")?;
 let chase_enabled=boolean(chase,"enabled")?;
 let hedge_recover=boolean(exit,"hedge_recover")?;
 let max_steps=integer(chase,"max_steps",100)?;
 let step_wait=integer(chase,"step_wait_ms",60000)?;
 let deadline=integer(chase,"hard_deadline_ms",300000)?;
 let gap=integer(exit,"leg_gap_ms",60000)?;
 let ticks_start=integer(chase,"limit_ticks_start",100)?;
 let ticks_step=integer(chase,"limit_ticks_step",100)?;
 let stop_enabled=boolean(emergency,"enabled")?;
 let paper_simulate=boolean(emergency,"paper_simulate")?;
 let configured_real=boolean(emergency,"place_in_real")?;
 let notify=boolean(emergency,"notify_on_place")?;
 let retry_n=integer(emergency,"retry_n",20)?;
 let retry_ms=integer(emergency,"retry_ms",60000)?;
 let bps=section(emergency,"stop_spread_bps")?
  .as_f64().ok_or(ExitReviewError::Missing)?;
 let order=section(emergency,"order_type")?
  .as_str().ok_or(ExitReviewError::Missing)?;
 let working=section(emergency,"working_type")?
  .as_str().ok_or(ExitReviewError::Missing)?;
 if !exit_enabled||!chase_enabled||!hedge_recover||
  max_steps==0||step_wait==0||deadline==0||gap==0||
  !stop_enabled||!paper_simulate||!bps.is_finite()||
  !(1.0..=1000.0).contains(&bps)||
  order!="STOP_MARKET"||working!="CONTRACT_PRICE"||
  retry_n==0||retry_ms==0||
  step_wait.checked_mul(max_steps).ok_or(ExitReviewError::Range)?>deadline{
  return Err(ExitReviewError::Policy)
 }
 Ok(json!({
  "schema":"r69_exit_safety_readonly_v1",
  "config_sha256":cfg.sha256,
  "exit_enabled_legacy":exit_enabled,
  "spot_chase_enabled_legacy":chase_enabled,
  "spot_ticks_initial":ticks_start,
  "spot_ticks_increment":ticks_step,
  "spot_chase_steps":max_steps,
  "spot_step_wait_ms":step_wait,
  "spot_hard_deadline_ms":deadline,
  "inter_leg_gap_ms":gap,
  "hedge_recovery_configured":hedge_recover,
  "futures_stop_enabled_legacy":stop_enabled,
  "futures_stop_spread_bps":bps,
  "futures_stop_order_type":order,
  "futures_stop_working_type":working,
  "futures_stop_max_retries":retry_n,
  "futures_stop_retry_ms":retry_ms,
  "paper_simulate_configured":paper_simulate,
  "real_stop_placement_configured_legacy":configured_real,
  "telegram_notification_configured_legacy":notify,
  "rust_runtime_execution_wiring_verified":false,
  "real_stop_placement_authorized":false,
  "may_place_stop_order":false,
  "may_recover_exchange_leg":false,
  "may_mutate_active_paper":false
 }))
}

/// R145: only algebra over validated original YAML + caller-supplied
/// local example timestamps/spread. All source/authority flags stay false.
/// No exchange inquiry, state mutation, simulated fill, actual protective
/// STOP order, Telegram send, or authorization after UNKNOWN.
pub fn inspect_local_exit_emergency_scenarios_without_io(
 cfg:&NativeTradingConfig,now_ms:u64,claimed_first_leg_ms:Option<u64>,
 local_signed_spread_milli_bps:i64,
)->Result<Value,ExitReviewError>{
 let reviewed=inspect_exit_safety_yaml(cfg)?;
 if now_ms==0||claimed_first_leg_ms==Some(0)||
   claimed_first_leg_ms.is_some_and(|t|t>now_ms){
   return Err(ExitReviewError::Range)
 }
 let field=|k:&str|reviewed.get(k).and_then(Value::as_u64)
   .ok_or(ExitReviewError::Missing);
 let chase_steps=field("spot_chase_steps")?;
 let chase_wait=field("spot_step_wait_ms")?;
 let chase_ticks_start=field("spot_ticks_initial")?;
 let chase_ticks_step=field("spot_ticks_increment")?;
 let deadline=field("spot_hard_deadline_ms")?;
 let inter_leg_gap=field("inter_leg_gap_ms")?;
 let stop_retries=field("futures_stop_max_retries")?;
 let stop_retry_wait=field("futures_stop_retry_ms")?;
 let stop_threshold=reviewed["futures_stop_spread_bps"].as_f64()
   .ok_or(ExitReviewError::Missing)?;
 let mut planned_chase=Vec::<Value>::new();
 for step in 0..chase_steps{
  let at=step.checked_mul(chase_wait).ok_or(ExitReviewError::Range)?;
  let ticks=step.checked_mul(chase_ticks_step)
    .and_then(|n|n.checked_add(chase_ticks_start))
    .ok_or(ExitReviewError::Range)?;
  if at>deadline{return Err(ExitReviewError::Policy)}
  planned_chase.push(json!({"step":step,"local_elapsed_ms":at,
     "configured_limit_tick_offset":ticks}))
 }
 let mut planned_stop_retries=Vec::<u64>::new();
 for attempt in 0..stop_retries{
  planned_stop_retries.push(attempt.checked_mul(stop_retry_wait)
    .ok_or(ExitReviewError::Range)?);
 }
 let claimed_leg_age=claimed_first_leg_ms.map(|t|now_ms-t);
 let locally_over_leg_gap=claimed_leg_age.map(|ms|ms>=inter_leg_gap);
 let abs_spread=(local_signed_spread_milli_bps as i128).abs() as f64;
 let local_threshold_reached=abs_spread>=stop_threshold*1000.0;
 Ok(json!({
   "schema":"r145_local_exit_emergency_no_io_v1",
   "source_config_sha256":cfg.sha256,
   "exit_chase_steps_timing_and_tick_offsets":planned_chase,
   "chase_deadline_ms":deadline,
   "inter_leg_gap_ms":inter_leg_gap,
   "claimed_leg_age_ms":claimed_leg_age,
   "locally_over_inter_leg_gap":locally_over_leg_gap,
   "hedge_recovery_configured_but_not_executed":
      reviewed["hedge_recovery_configured"],
   "emergency_stop_retry_offsets_ms":planned_stop_retries,
   "local_unsigned_spread_milli_bps":abs_spread as u64,
   "local_abs_spread_reaches_configured_stop_threshold":local_threshold_reached,
   "stop_order_type_configured":reviewed["futures_stop_order_type"],
   "stop_working_type_configured":reviewed["futures_stop_working_type"],
   "source_emergency_enabled":reviewed["futures_stop_enabled_legacy"],
   "source_exit_enabled":reviewed["exit_enabled_legacy"],
   "source_chase_enabled":reviewed["spot_chase_enabled_legacy"],
   "source_paper_simulate":reviewed["paper_simulate_configured"],
   "source_telegram_notify_on_place":reviewed["telegram_notification_configured_legacy"],
   "source_real_stop_placement_configured_legacy":
      reviewed["real_stop_placement_configured_legacy"],
   "source_observation_authenticated":false,
   "exchange_stop_trigger_verified":false,
   "known_exchange_order_outcome":false,
   "may_retry_after_unknown_ack":false,
   "may_place_or_cancel_stop":false,
   "may_hedge_or_unwind_live":false,
   "may_send_telegram":false,
   "may_mutate_active_paper":false,
   "installed_paper_source_attested":false,
   "real_trading_authorized":false
 }))
}

#[cfg(test)]
mod tests{
 use super::*;
 fn config()->NativeTradingConfig{
  NativeTradingConfig::parse(include_str!("../config/native_trading.yaml")).unwrap()
 }
 #[test]
 fn r145_original_eighteen_exit_and_emergency_yaml_fields_affect_local_review(){
  let source=include_str!("../config/native_trading.yaml");
  let baseline=inspect_local_exit_emergency_scenarios_without_io(
     &config(),100_000,Some(98_000),55_000).unwrap();
  assert_eq!(baseline["exit_chase_steps_timing_and_tick_offsets"],
    json!([{"step":0,"local_elapsed_ms":0,"configured_limit_tick_offset":0},
          {"step":1,"local_elapsed_ms":700,"configured_limit_tick_offset":1},
          {"step":2,"local_elapsed_ms":1400,"configured_limit_tick_offset":2}]));
  assert_eq!(baseline["emergency_stop_retry_offsets_ms"],json!([0,300]));
  assert_eq!(baseline["locally_over_inter_leg_gap"],true);
  assert_eq!(baseline["local_abs_spread_reaches_configured_stop_threshold"],true);
  let changes=[
    ("exit_safety:\n  enabled: true","exit_safety:\n  enabled: false"),
    ("spot_chase:\n    enabled: true","spot_chase:\n    enabled: false"),
    ("limit_ticks_start: 0","limit_ticks_start: 2"),
    ("limit_ticks_step: 1","limit_ticks_step: 2"),
    ("max_steps: 3","max_steps: 4"),
    ("step_wait_ms: 700","step_wait_ms: 800"),
    ("hard_deadline_ms: 4000","hard_deadline_ms: 3000"),
    ("leg_gap_ms: 1500","leg_gap_ms: 2500"),
    ("hedge_recover: true","hedge_recover: false"),
    ("futures_emergency_stop:\n  enabled: true",
       "futures_emergency_stop:\n  enabled: false"),
    ("stop_spread_bps: 50.0","stop_spread_bps: 60.0"),
    ("order_type: STOP_MARKET","order_type: MARKET"),
    ("working_type: CONTRACT_PRICE","working_type: MARK_PRICE"),
    ("  retry_n: 2","  retry_n: 3"),
    ("retry_ms: 300","retry_ms: 400"),
    ("place_in_real: false","place_in_real: true"),
    ("paper_simulate: true","paper_simulate: false"),
    ("notify_on_place: true","notify_on_place: false"),
  ];
  assert_eq!(changes.len(),18);
  for (i,(old,new)) in changes.into_iter().enumerate(){
    let altered=source.replacen(old,new,1);
    assert_ne!(altered,source,"source fixture unchanged for {i} {old}");
    let result=NativeTradingConfig::parse(&altered).ok().and_then(|cfg|
       inspect_local_exit_emergency_scenarios_without_io(
          &cfg,100_000,Some(98_000),55_000).ok());
    let expected_denial=matches!(i,0|1|8|9|11|12|16);
    assert_eq!(result.is_none(),expected_denial,
      "unexpected acceptance/refusal for field {i} {old}");
    if let Some(mut valid)=result{
      // Source SHA always changes, so explicitly omit it from differential.
      let mut original=baseline.clone();
      original.as_object_mut().unwrap().remove("source_config_sha256");
      valid.as_object_mut().unwrap().remove("source_config_sha256");
      assert_ne!(valid,original,"no consumer impact for source field {i} {old}");
      for flag in ["source_observation_authenticated",
        "exchange_stop_trigger_verified","known_exchange_order_outcome",
        "may_retry_after_unknown_ack","may_place_or_cancel_stop",
        "may_hedge_or_unwind_live","may_send_telegram",
        "may_mutate_active_paper","installed_paper_source_attested",
        "real_trading_authorized"]{
          assert_eq!(valid[flag],false,"unexpected permission {flag} for {i}");
      }
    }
  }
 }
 #[test]
 fn r145_unknown_timestamps_and_local_bps_never_authorize_orders(){
  let original=config();
  let unknown=inspect_local_exit_emergency_scenarios_without_io(
     &original,100_000,None,-55_000).unwrap();
  assert!(unknown["locally_over_inter_leg_gap"].is_null());
  assert_eq!(unknown["source_observation_authenticated"],false);
  assert_eq!(unknown["may_place_or_cancel_stop"],false);
  assert_eq!(unknown["may_hedge_or_unwind_live"],false);
  let lower=inspect_local_exit_emergency_scenarios_without_io(
     &original,100_000,Some(99_000),49_999).unwrap();
  assert_eq!(lower["locally_over_inter_leg_gap"],false);
  assert_eq!(lower["local_abs_spread_reaches_configured_stop_threshold"],false);
  let upper=inspect_local_exit_emergency_scenarios_without_io(
     &original,100_000,Some(98_500),50_000).unwrap();
  assert_eq!(upper["locally_over_inter_leg_gap"],true);
  assert_eq!(upper["local_abs_spread_reaches_configured_stop_threshold"],true);
  assert_eq!(upper["may_retry_after_unknown_ack"],false);
  for (now,first) in [(0,None),(100_000,Some(0)),(100_000,Some(100_001))]{
    assert_eq!(inspect_local_exit_emergency_scenarios_without_io(
       &original,now,first,55_000),Err(ExitReviewError::Range));
  }
  let mut tamper=original;
  tamper.raw["futures_emergency_stop"]["notify_on_place"]=
    serde_yaml::Value::Bool(false);
  assert_eq!(inspect_local_exit_emergency_scenarios_without_io(
    &tamper,100_000,None,55_000),Err(ExitReviewError::Policy));
 }
 #[test]
 fn r94_exit_review_denies_postparse_untracked_emergency_source_changes(){
  let mut x=config();
  x.raw["futures_emergency_stop"]["place_in_real"]=serde_yaml::Value::Bool(true);
  assert_eq!(inspect_exit_safety_yaml(&x),Err(ExitReviewError::Policy));
  let mut x=config();
  x.raw["exit_safety"]["leg_gap_ms"]=serde_yaml::Value::from(2000_u64);
  assert_eq!(inspect_exit_safety_yaml(&x),Err(ExitReviewError::Policy));
  let mut x=config();
  x.sha256="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into();
  assert_eq!(inspect_exit_safety_yaml(&x),Err(ExitReviewError::Policy));
 }
 #[test]
 fn actual_legacy_exit_and_stop_values_are_observed_without_execution(){
  let r=inspect_exit_safety_yaml(&config()).unwrap();
  assert_eq!(r["spot_chase_steps"],3);
  assert_eq!(r["spot_step_wait_ms"],700);
  assert_eq!(r["spot_hard_deadline_ms"],4000);
  assert_eq!(r["inter_leg_gap_ms"],1500);
  assert_eq!(r["futures_stop_spread_bps"],50.0);
  assert_eq!(r["futures_stop_order_type"],"STOP_MARKET");
  assert_eq!(r["futures_stop_working_type"],"CONTRACT_PRICE");
  assert_eq!(r["real_stop_placement_configured_legacy"],false);
  assert_eq!(r["real_stop_placement_authorized"],false);
  assert_eq!(r["may_place_stop_order"],false);
  assert_eq!(r["rust_runtime_execution_wiring_verified"],false);
 }
 #[test]
 fn native_real_permit_forbidden_and_invalid_yaml_denied(){
  let mut a=config();a.policy.real_execution_authorized=true;
  assert_eq!(inspect_exit_safety_yaml(&a),Err(ExitReviewError::Policy));
  let mut a=config();a.raw["futures_emergency_stop"]["stop_spread_bps"]=
    serde_yaml::Value::from(-1.0);
  assert_eq!(inspect_exit_safety_yaml(&a),Err(ExitReviewError::Policy));
  let mut a=config();a.raw["exit_safety"]["spot_chase"]["max_steps"]=
    serde_yaml::Value::from(1000);
  // R94 denies source drift before ordinary range validation.
  assert_eq!(inspect_exit_safety_yaml(&a),Err(ExitReviewError::Policy));
 }
 #[test]
 fn unrecognized_stop_mode_or_invalid_deadline_denied(){
  let mut a=config();a.raw["futures_emergency_stop"]["order_type"]=
    serde_yaml::Value::from("MARKET");
  assert_eq!(inspect_exit_safety_yaml(&a),Err(ExitReviewError::Policy));
  let mut a=config();a.raw["exit_safety"]["spot_chase"]["hard_deadline_ms"]=
    serde_yaml::Value::from(100);
  assert_eq!(inspect_exit_safety_yaml(&a),Err(ExitReviewError::Policy));
 }
}
