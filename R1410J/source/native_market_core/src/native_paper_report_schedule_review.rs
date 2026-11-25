//! R57: reconcile legacy report schedule with the existing operator PAPER cadence.
 //! Read-only documentation of differences; does NOT alter Telegram timers.
use crate::native_trading_config::NativeTradingConfig;
use serde_json::{json,Value as JsonValue};

#[derive(Clone,Debug,PartialEq,Eq)]
pub enum ScheduleReviewError{Config,Conflict}

#[derive(Clone,Debug,PartialEq,Eq)]
pub struct NativePaperReportSchedule{
 pub timezone:String,
 pub report_local_time:String,
 pub report_minute_of_day:u64,
 pub legacy_heartbeat_sec:u64,
 pub operator_paper_heartbeat_sec:u64,
 pub service_interval_sec:u64,
 pub overrides_legacy_heartbeat:bool,
}

impl NativePaperReportSchedule{
 pub fn from_config(cfg:&NativeTradingConfig)->Result<Self,ScheduleReviewError>{
  if !cfg.source_projection_unchanged(){return Err(ScheduleReviewError::Config)}
  if cfg.policy.real_execution_authorized{return Err(ScheduleReviewError::Config)}
  let r=&cfg.raw;
  let general=r.get("general").ok_or(ScheduleReviewError::Config)?;
  let tz=general.get("report_timezone").and_then(|x|x.as_str())
   .ok_or(ScheduleReviewError::Config)?;
  let clock=general.get("report_time").and_then(|x|x.as_str())
   .ok_or(ScheduleReviewError::Config)?;
  let heartbeat=general.get("heartbeat_interval_sec").and_then(|x|x.as_u64())
   .ok_or(ScheduleReviewError::Config)?;
  let service=r.get("telemetry").and_then(|x|x.get("service"))
   .and_then(|x|x.get("interval_sec")).and_then(|x|x.as_u64())
   .ok_or(ScheduleReviewError::Config)?;
  if tz!="Asia/Yekaterinburg"||clock!="23:59"{
   return Err(ScheduleReviewError::Conflict)
  }
  if !(60..=86_400).contains(&heartbeat)||!(60..=86_400).contains(&service){
   return Err(ScheduleReviewError::Config)
  }
  let operator_sec=3_600; // Existing Rust R40/R44 hourly heartbeat contract.
  Ok(Self{
   timezone:tz.to_owned(),report_local_time:clock.to_owned(),
   report_minute_of_day:23*60+59,
   legacy_heartbeat_sec:heartbeat,
   operator_paper_heartbeat_sec:operator_sec,
   service_interval_sec:service,
   overrides_legacy_heartbeat:heartbeat!=operator_sec
  })
 }
 pub fn observation(&self)->JsonValue{
  json!({
   "timezone":self.timezone,
   "report_local_time":self.report_local_time,
   "report_minute_of_day":self.report_minute_of_day,
   "legacy_heartbeat_sec":self.legacy_heartbeat_sec,
   "operator_requested_paper_heartbeat_sec":self.operator_paper_heartbeat_sec,
   "operator_heartbeat_differs_from_legacy":self.overrides_legacy_heartbeat,
   "legacy_telemetry_service_interval_sec":self.service_interval_sec,
   "this_readonly_review_reschedules_runtime":false,
   "telegram_network_called":false,
   "real_trading_authorized":false
  })
 }
}

#[cfg(test)]
mod tests{
 use super::*;
 fn cfg()->NativeTradingConfig{
  NativeTradingConfig::parse(include_str!("../config/native_trading.yaml")).unwrap()
 }
 #[test]
 fn operator_hourly_cadence_is_explicit_vs_legacy_quarter_hour(){
  let c=NativePaperReportSchedule::from_config(&cfg()).unwrap();
  assert_eq!(c.timezone,"Asia/Yekaterinburg");
  assert_eq!(c.report_local_time,"23:59");
  assert_eq!(c.report_minute_of_day,1439);
  assert_eq!(c.legacy_heartbeat_sec,900);
  assert_eq!(c.operator_paper_heartbeat_sec,3600);
  assert_eq!(c.service_interval_sec,1800);
  assert!(c.overrides_legacy_heartbeat);
  assert_eq!(c.observation()["this_readonly_review_reschedules_runtime"],false);
 }
 #[test]
 fn timezone_and_report_time_drift_are_rejected(){
  let mut x=cfg();
  x.raw["general"]["report_timezone"]=serde_yaml::Value::from("UTC");
  assert_eq!(NativePaperReportSchedule::from_config(&x),Err(ScheduleReviewError::Config));
  let mut x=cfg();
  x.raw["general"]["report_time"]=serde_yaml::Value::from("12:00");
  assert_eq!(NativePaperReportSchedule::from_config(&x),Err(ScheduleReviewError::Config));
 }
 #[test]
 fn nonexistent_or_unsafe_legacy_intervals_fail(){
  let mut x=cfg();
  x.raw["general"]["heartbeat_interval_sec"]=serde_yaml::Value::from(0);
  assert_eq!(NativePaperReportSchedule::from_config(&x),Err(ScheduleReviewError::Config));
  let mut x=cfg();
  x.raw["telemetry"]["service"]["interval_sec"]=serde_yaml::Value::from(0);
  assert_eq!(NativePaperReportSchedule::from_config(&x),Err(ScheduleReviewError::Config));
 }
}
