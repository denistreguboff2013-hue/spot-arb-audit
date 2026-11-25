//! R286: cross-module source consumer proof only. All PaperPoint data below
//! are deterministic fixtures, NOT sampled from the installed PAPER.
use crate::native_trading_config::NativeTradingConfig;
use crate::native_legacy_strategy_policy_review::inspect_scanner_min_depth_readonly;
use crate::native_paper_ai_bridge_review::paper_ai_analytics_source_review_without_io;
use crate::native_paper_telegram_observer::PaperPoint;
use crate::native_paper_daily_calendar_review::PaperDailyCalendar;
use crate::native_r258_source_admission_diagnostics::review_two_paper_admissions_r258;
use serde_json::{json,Value};

#[derive(Clone,Debug,PartialEq,Eq)]
pub enum SourceConsumersError{Source,Inconsistent,UnsafeClaim}

fn source_fixture()->(PaperPoint,PaperDailyCalendar,u64){
 let session="session_20742_1792112923".to_string();
 let point=PaperPoint{session:session.clone(),day:20742,
  updated_wall_ms:1_792_120_000_000,entries:500,closes:0,
  open_positions:0,faults:0,unknown_unhedged:0,modeled_milli_usdt:0};
 let day=PaperDailyCalendar{day:20742,opened:0,closed:0,
  model_micro_usdt:0,session_count:1,last_session:session,
  first_seen_ms:1_792_119_000_000,last_seen_ms:1_792_120_000_000,
  last_reported_day:None};
 (point,day,1_792_120_015_000)
}

/// A single exact-source diagnostic that exercises EXISTING Rust consumers:
/// scanner depth -> AI sanitization -> bounded pending adviser -> analytics
/// pressure policy, and ensures none is misrepresented as a durable writer.
pub fn audit_existing_consumer_boundaries_r286(
 cfg:&NativeTradingConfig,
)->Result<Value,SourceConsumersError>{
 let depth=inspect_scanner_min_depth_readonly(cfg)
   .map_err(|_|SourceConsumersError::Source)?;
 let (point,day,now)=source_fixture();
 let (input,advice,analytics)=
   paper_ai_analytics_source_review_without_io(cfg,&point,&day,now,None)
   .map_err(|_|SourceConsumersError::Source)?;
 let admission=review_two_paper_admissions_r258(cfg,now as i64);
 let min_vel=cfg.raw["scale_in_hints"]["min_vel_bps_s"].as_f64()
   .ok_or(SourceConsumersError::Source)?;
 let hold=cfg.raw["velocity"]["hold_pos_bps_s"].as_f64()
   .ok_or(SourceConsumersError::Source)?;
 let hints_enabled=cfg.raw["scale_in_hints"]["enabled"].as_bool()
   .ok_or(SourceConsumersError::Source)?;
 if depth.required_min_trade_usdt<0.0||depth.is_upsize_signal||depth.may_change_position
   ||input["summary"]["real_launch_authorized"]!=false
   ||advice["policy"]["auto_apply_allowed"]!=false
   ||advice["policy"]["order_submission_allowed"]!=false
   ||advice["strategy_tuning_allowed"]!=false
   ||analytics["source_config_sha256"].as_str()!=Some(cfg.sha256.as_str())
   ||analytics["analytics_writer_running_attested"]!=false
   ||analytics["analytics_database_opened"]!=false
   ||analytics["analytics_database_retention_applied"]!=false
   ||analytics["storage_health_observation"]["can_write_db"]!=false
   ||analytics["storage_health_observation"]["can_prune"]!=false
   ||analytics["analytics_source_consumed_by_installed_paper"]!=false
   ||admission["installed_field_applications_proven"]!=0
   ||admission["automatic_cutover_authorized"]!=false{
    return Err(SourceConsumersError::UnsafeClaim)
 }
 if !hold.is_finite()||!min_vel.is_finite()||hold<0.0||min_vel<0.0 {
    return Err(SourceConsumersError::Inconsistent)
 }
 Ok(json!({
   "schema":"SPOTARB/R286/EXISTING-SOURCE-CONSUMERS-OFFLINE/V1",
   "operator_yaml_sha256":cfg.sha256,
   "fixture_is_not_live_paper":true,
   "python_scanner_semantics_reference":"scanner.py:min_trade_usdt first-truthy min_depth_usdt/adaptive_entry.min_usdt/risk.futures_order_usdt",
   "scanner_preview":{
      "scale_hints_enabled":hints_enabled,
      "required_min_trade_usdt":depth.required_min_trade_usdt,
      "creates_upsize_orders":false,
      "may_change_position":false,
      "source_runtime_scanner_wired":false
   },
   "python_velocity_fallback_reference":"strategy.py:_vel_cfg vc.get(hold_pos_bps_s,hints.get(min_vel_bps_s))",
   "velocity":{
      "configured_hold_pos_bps_s":hold,
      "scale_hints_fallback_min_vel_bps_s":min_vel,
      "fallback_shadowed_by_present_velocity_value":true,
      "live_close_trigger_applied":false
   },
   "analytics_source_observation":analytics,
   "ai_advisor":advice,
   "ai_source_input_summary":input["summary"],
   "actual_source_paper_admission":admission,
   "pending_only":true,
   "verified_real_pnl_available":false,
   "durable_analytics_writer_attested":false,
   "installed_paper_consumer_proven":false,
   "exchange_private_api_used":false,
   "websocket_started":false,
   "telegram_delivery_attested":false,
   "real_trading_authorized":false
 }))
}

#[cfg(test)]
mod tests{
 use super::*;
 fn config(yaml:&str)->NativeTradingConfig{NativeTradingConfig::parse(yaml).unwrap()}
 const YAML:&str=include_str!("../config/native_trading.yaml");
 #[test]fn r286_exact_source_scanner_and_ai_is_bounded_not_live(){
  let v=audit_existing_consumer_boundaries_r286(&config(YAML)).unwrap();
  assert_eq!(v["scanner_preview"]["required_min_trade_usdt"],200.0);
  assert_eq!(v["scanner_preview"]["scale_hints_enabled"],true);
  assert_eq!(v["scanner_preview"]["creates_upsize_orders"],false);
  assert_eq!(v["velocity"]["configured_hold_pos_bps_s"],0.06);
  assert_eq!(v["velocity"]["scale_hints_fallback_min_vel_bps_s"],0.08);
  assert_eq!(v["analytics_source_observation"]["analytics_max_db_bytes"],
     8192_u64*1_048_576);
  assert_eq!(v["analytics_source_observation"]["analytics_writer_running_attested"],false);
  assert_eq!(v["ai_advisor"]["strategy_tuning_allowed"],false);
  assert_eq!(v["durable_analytics_writer_attested"],false);
  assert_eq!(v["real_trading_authorized"],false);
 }
 #[test]fn r286_enabled_switch_does_not_control_scanner_min_depth_preview(){
  let changed=YAML.replacen("scale_in_hints:\n  enabled: true",
    "scale_in_hints:\n  enabled: false",1);
  assert_ne!(changed,YAML);
  let a=audit_existing_consumer_boundaries_r286(&config(YAML)).unwrap();
  let b=audit_existing_consumer_boundaries_r286(&config(&changed)).unwrap();
  assert_eq!(a["scanner_preview"]["required_min_trade_usdt"],
             b["scanner_preview"]["required_min_trade_usdt"]);
  assert_ne!(a["operator_yaml_sha256"],b["operator_yaml_sha256"]);
  assert_eq!(b["scanner_preview"]["scale_hints_enabled"],false);
  assert_eq!(b["installed_paper_consumer_proven"],false);
 }
 #[test]fn r286_changing_scanner_depth_changes_preview_not_order_authority(){
  let changed=YAML.replacen("  min_depth_usdt: 200","  min_depth_usdt: 250",1);
  assert_ne!(changed,YAML);
  let r=audit_existing_consumer_boundaries_r286(&config(&changed)).unwrap();
  assert_eq!(r["scanner_preview"]["required_min_trade_usdt"],250.0);
  assert_eq!(r["scanner_preview"]["may_change_position"],false);
  assert_eq!(r["actual_source_paper_admission"]["automatic_cutover_authorized"],false);
  assert_eq!(r["analytics_source_observation"]["api_or_telegram_actions_taken"],false);
 }
}