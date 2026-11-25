//! R50: real PUBLIC-PAPER native status -> fact-limited Rust AI input.
 //! NO real exit/fee/PnL inference, network, SQLite, cleanup or orders.
use crate::{
 native_paper_telegram_observer::PaperPoint,
 native_paper_daily_calendar_review::PaperDailyCalendar,
 native_paper_oldstyle_notice_review::yekt_date,
 native_ai_input_review::{sanitize_daily_report,advisory_from_report_without_io},
 native_ai_advisor_review::AdvisorError,
};
use serde_json::{json,Value};

#[derive(Clone,Debug,PartialEq,Eq)]
pub enum PaperAiError{InvalidPaperSource,Advisor(AdvisorError)}

pub fn paper_daily_report_from_validated_snapshot(
 point:&PaperPoint, calendar:&PaperDailyCalendar, now_ms:u64,
)->Result<Value,PaperAiError>{
 if point.day==0||point.session.is_empty()||
    calendar.day!=point.day||calendar.session_count==0||
    calendar.first_seen_ms==0||
    calendar.first_seen_ms>calendar.last_seen_ms||
    calendar.last_seen_ms>now_ms||
    point.updated_wall_ms>now_ms.saturating_add(2_000)||
    now_ms.saturating_sub(point.updated_wall_ms)>120_000||
    calendar.last_session.is_empty()||
    calendar.last_reported_day.is_some_and(|d|d>calendar.day)||
    point.open_positions>10{
  return Err(PaperAiError::InvalidPaperSource)
 }
 let date=yekt_date(point.day).map_err(|_|PaperAiError::InvalidPaperSource)?;
 let all_modes=json!({
  "paper":{"counts":{
    "observed_verified_modeled_opens":calendar.opened,
    "observed_verified_modeled_closes":calendar.closed,
    "session_counter_entries_not_trades":point.entries,
    "session_counter_closes":point.closes,
    "observed_paper_sessions":calendar.session_count,
    "observer_faults":point.faults,
    "unknown_unhedged":point.unknown_unhedged
   },
   "known_net_pnl_usdt":null,
   "unknown_pnl_trades":calendar.closed,
   "pnl_complete":false},
  "real":{"counts":{},"known_net_pnl_usdt":null,"unknown_pnl_trades":0,"pnl_complete":false},
  "test":{"counts":{},"known_net_pnl_usdt":null,"unknown_pnl_trades":0,"pnl_complete":false},
  "replay":{"counts":{},"known_net_pnl_usdt":null,"unknown_pnl_trades":0,"pnl_complete":false}
 });
 Ok(json!({
  "analytics_version":"native_r50_public_paper_snapshot_v1",
  "date":date,"timezone":"Asia/Yekaterinburg",
  "modes":all_modes,
  "storage_pressure_warning":false,
  "evidence_incomplete":true,
  "real_pnl_available":false,
  "advisor_ready":false,
  "storage_health":{"status":"UNKNOWN","used_ratio":null},
  "evidence_quality":{
   "paper_entries_total":calendar.opened,
   "paper_exits_total":calendar.closed,
   "verified_real_entries":0,"verified_real_exits":0,
   "unknown_entries":point.unknown_unhedged,
   "unknown_exits":0,"manual_required_count":0,
   "contradicted_count":0,"partial_fill_count":0,
   "missing_fee_count":0,"missing_slippage_count":0,
   "missing_latency_count":0
  },
  "cohort_coverage":{"unlinked_trade_commits":0},
  "reality_gap":{"status":"UNAVAILABLE_NO_VERIFIED_REAL"},
  "pair_evidence":[],
  "not_enough_data_reasons":[
    "PAPER_PARTIAL_CALENDAR_COVERAGE",
    "NO_EXCHANGE_VERIFIED_REAL_FILLS",
    "NO_AUTHORITATIVE_ACTUAL_PNL"
  ],
  "data_warnings":[
    "PAPER_VIRTUAL_ONLY",
    "DAY_COVERAGE_INCOMPLETE",
    "SESSION_ENTRY_COUNTER_MAY_BE_QUOTA_MARKER_500"
  ]
 }))
}
/// Offline pipeline; does not grant strategy tuning or real trading.
pub fn paper_advisor_from_validated_snapshot(
 point:&PaperPoint,calendar:&PaperDailyCalendar,now_ms:u64
)->Result<(Value,Value),PaperAiError>{
 let report=paper_daily_report_from_validated_snapshot(point,calendar,now_ms)?;
 let input=sanitize_daily_report(&report).map_err(PaperAiError::Advisor)?;
 let advisory=advisory_from_report_without_io(&report).map_err(PaperAiError::Advisor)?;
 Ok((input,advisory))
}

/// Astra P1-06: source-locked Rust analytics limits + existing sanitized PAPER
/// advisor in ONE review-only envelope. Reuses R47 storage policy, R50 paper
/// sanitization and R45 advisor; no duplicated SQLite writer or AI strategy.
/// Disk measurements are caller-supplied, never authenticated here.
pub fn paper_ai_analytics_source_review_without_io(
 cfg:&crate::native_trading_config::NativeTradingConfig,
 point:&PaperPoint,calendar:&PaperDailyCalendar,now_ms:u64,
 storage:Option<&crate::native_ai_storage_review::StorageSnapshot>,
)->Result<(Value,Value,Value),PaperAiError>{
 use crate::native_ai_storage_review::{NativeAnalyticsStorageLimits,PressureLevel};
 // The historical 26-section YAML may have legacy trade_enabled=true.
 // That is NOT a Rust real permit: require native_runtime hard gates instead.
 if !cfg.source_projection_unchanged()
   ||cfg.policy.real_execution_authorized||cfg.policy.real_daily_trades!=0{
  return Err(PaperAiError::InvalidPaperSource)
 }
 let limits=NativeAnalyticsStorageLimits::from_native_config(cfg)
   .map_err(|_|PaperAiError::InvalidPaperSource)?;
 let (input,advice)=paper_advisor_from_validated_snapshot(point,calendar,now_ms)?;
 let pressure=match storage{
  Some(s)=>{
   let checked=limits.inspect_without_io(s,now_ms)
      .map_err(|_|PaperAiError::InvalidPaperSource)?;
   let label=match checked.pressure{
    PressureLevel::Normal=>"NORMAL",
    PressureLevel::Warning=>"WARNING",
    PressureLevel::High=>"HIGH",
    PressureLevel::Critical=>"CRITICAL",
   };
   json!({
     "status":"UNAUTHENTICATED_LOCAL_SNAPSHOT",
     "pressure":label,"wal_near_limit":checked.wal_near_limit,
     "manual_review":checked.manual_review,
     "used_db_bytes":s.used_db_bytes,"wal_bytes":s.wal_bytes,
     "authenticated_usage":false,"can_prune":false,"can_write_db":false
   })
  },
  None=>json!({
   "status":"UNKNOWN_NO_DISK_MEASUREMENT",
   "pressure":null,"wal_near_limit":null,
   "manual_review":true,"used_db_bytes":null,"wal_bytes":null,
   "authenticated_usage":false,"can_prune":false,"can_write_db":false
  })
 };
 let status=json!({
  "schema":"p106_rust_paper_ai_analytics_source_readonly_v1",
  "source_config_sha256":cfg.sha256,
  "legacy_general_trade_enabled_is_not_rust_permission":cfg.raw.get("general")
     .and_then(|v|v.get("trade_enabled")).and_then(|v|v.as_bool()),
  "analytics_source_enabled":limits.enabled,
  "analytics_source_mode":"observe",
  "analytics_database_relative_path":limits.primary_relative_path,
  "analytics_mirror_relative_path":limits.mirror_relative_path,
  "analytics_max_db_bytes":limits.max_db_bytes,
  "analytics_max_wal_bytes":limits.max_wal_bytes,
  "analytics_raw_retention_days":limits.raw_retention_days,
  "analytics_writer_running_attested":false,
  "analytics_database_opened":false,
  "analytics_database_retention_applied":false,
  "analytics_source_consumed_by_installed_paper":false,
  "storage_health_observation":pressure,
  "ai_input_from_validated_paper_calendar":true,
  "ai_strategy_tuning_allowed":false,
  "ai_automatic_config_edits_allowed":false,
  "real_pnl_attested":false,
  "real_trading_authorized":false,
  "api_or_telegram_actions_taken":false
 });
 Ok((input,advice,status))
}

#[cfg(test)]
mod tests{
 use super::*;
 fn point()->PaperPoint{
  PaperPoint{session:"session_20742_1792112923".into(),day:20742,
   updated_wall_ms:1_792_120_000_000,entries:500,closes:0,
   open_positions:0,faults:0,unknown_unhedged:0,
   modeled_milli_usdt:0}
 }
 fn calendar()->PaperDailyCalendar{
  PaperDailyCalendar{day:20742,opened:0,closed:0,
   model_micro_usdt:0,session_count:1,
   last_session:"session_20742_1792112923".into(),
   first_seen_ms:1_792_119_000_000,last_seen_ms:1_792_120_000_000,
   last_reported_day:None}
 }
 #[test]
 fn p106_analytics_all_seven_yaml_fields_reach_readonly_ai_report(){
  let cfg=crate::native_trading_config::NativeTradingConfig::parse(
   include_str!("../config/native_trading.yaml")).unwrap();
  let (input,advice,report)=paper_ai_analytics_source_review_without_io(
    &cfg,&point(),&calendar(),1_792_120_015_000,None).unwrap();
  assert_eq!(report["source_config_sha256"],cfg.sha256);
  assert_eq!(report["analytics_source_enabled"],true);
  // The current source YAML disables trade; the analytics report carries
  // this fact without granting any Rust order authority.
  assert_eq!(report["legacy_general_trade_enabled_is_not_rust_permission"],false);
  assert_eq!(report["analytics_source_mode"],"observe");
  assert_eq!(report["analytics_database_relative_path"],"data/arb_stats.sqlite");
  assert_eq!(report["analytics_mirror_relative_path"],"data/arb_stats_mirror.sqlite");
  assert_eq!(report["analytics_max_db_bytes"],8192_u64*1_048_576);
  assert_eq!(report["analytics_max_wal_bytes"],64_u64*1_048_576);
  assert_eq!(report["analytics_raw_retention_days"],1);
  assert_eq!(report["storage_health_observation"]["status"],"UNKNOWN_NO_DISK_MEASUREMENT");
  assert_eq!(report["analytics_writer_running_attested"],false);
  assert_eq!(report["analytics_source_consumed_by_installed_paper"],false);
  assert_eq!(report["analytics_database_retention_applied"],false);
  assert_eq!(report["ai_strategy_tuning_allowed"],false);
  assert_eq!(report["ai_automatic_config_edits_allowed"],false);
  assert_eq!(report["real_trading_authorized"],false);
  assert_eq!(input["summary"]["real_launch_authorized"],false);
  assert_eq!(advice["strategy_tuning_allowed"],false);
 }
 #[test]
 fn p106_all_analytics_source_parameters_change_observed_result_or_fail_closed(){
  let src=include_str!("../config/native_trading.yaml");
  let table=[
   ("enabled: true\n  mode: observe","enabled: false\n  mode: observe",
    "analytics_source_enabled"),
   ("mode: observe\n  database_path:","mode: disabled\n  database_path:",
    "analytics_source_mode"),
   ("database_path: data/arb_stats.sqlite","database_path: data/changed.sqlite",
    "analytics_database_relative_path"),
   ("mirror_database_path: data/arb_stats_mirror.sqlite",
    "mirror_database_path: data/changed_mirror.sqlite","analytics_mirror_relative_path"),
   ("max_db_mb: 8192","max_db_mb: 4096","analytics_max_db_bytes"),
   ("max_wal_mb: 64","max_wal_mb: 32","analytics_max_wal_bytes"),
   ("raw_retention_days: 1","raw_retention_days: 7","analytics_raw_retention_days"),
  ];
  let base_cfg=crate::native_trading_config::NativeTradingConfig::parse(src).unwrap();
  let (_,_,baseline)=paper_ai_analytics_source_review_without_io(
    &base_cfg,&point(),&calendar(),1_792_120_015_000,None).unwrap();
  for (old,new,field) in table{
   let changed=src.replacen(old,new,1);
   assert_ne!(changed,src,"missing fixture marker {old}");
   let outcome=crate::native_trading_config::NativeTradingConfig::parse(&changed)
     .ok().and_then(|cfg|paper_ai_analytics_source_review_without_io(
       &cfg,&point(),&calendar(),1_792_120_015_000,None).ok())
     .map(|(_,_,r)|r);
   assert!(outcome.as_ref().map_or(true,|r|r[field]!=baseline[field]),
     "source mutation did not change its typed analytics field: {field}");
  }
 }
 #[test]
 fn p106_untrusted_pressure_stays_advisory_and_cannot_prune_or_trade(){
  use crate::native_ai_storage_review::StorageSnapshot;
  let cfg=crate::native_trading_config::NativeTradingConfig::parse(
    include_str!("../config/native_trading.yaml")).unwrap();
  let observed=StorageSnapshot{
    sampled_ms:1_792_120_015_000,used_db_bytes:8192*1_048_576,
    wal_bytes:64*1_048_576
  };
  let (_,advice,r)=paper_ai_analytics_source_review_without_io(
    &cfg,&point(),&calendar(),1_792_120_015_000,Some(&observed)).unwrap();
  assert_eq!(r["storage_health_observation"]["pressure"],"CRITICAL");
  assert_eq!(r["storage_health_observation"]["wal_near_limit"],true);
  assert_eq!(r["storage_health_observation"]["manual_review"],true);
  assert_eq!(r["storage_health_observation"]["authenticated_usage"],false);
  assert_eq!(r["storage_health_observation"]["can_prune"],false);
  assert_eq!(r["storage_health_observation"]["can_write_db"],false);
  assert_eq!(r["analytics_database_opened"],false);
  assert_eq!(advice["strategy_tuning_allowed"],false);
  let stale=StorageSnapshot{sampled_ms:observed.sampled_ms-120_001,..observed};
  assert_eq!(paper_ai_analytics_source_review_without_io(
    &cfg,&point(),&calendar(),1_792_120_015_000,Some(&stale)),
    Err(PaperAiError::InvalidPaperSource));
 }
 #[test]
 fn p106_invalid_analytics_paths_and_bad_paper_source_fail_closed(){
  let src=include_str!("../config/native_trading.yaml");
  for (old,new) in [
   ("database_path: data/arb_stats.sqlite","database_path: /tmp/out.sqlite"),
   ("mirror_database_path: data/arb_stats_mirror.sqlite",
    "mirror_database_path: data/arb_stats.sqlite"),
   ("raw_retention_days: 1","raw_retention_days: 0"),
   ("max_wal_mb: 64","max_wal_mb: 0"),
  ]{
   let changed=src.replacen(old,new,1);
   assert_ne!(changed,src);
   if let Ok(cfg)=crate::native_trading_config::NativeTradingConfig::parse(&changed){
    assert!(paper_ai_analytics_source_review_without_io(
     &cfg,&point(),&calendar(),1_792_120_015_000,None).is_err());
   }
  }
  let cfg=crate::native_trading_config::NativeTradingConfig::parse(src).unwrap();
  let mut c=calendar();c.day+=1;
  assert!(paper_ai_analytics_source_review_without_io(
    &cfg,&point(),&c,1_792_120_015_000,None).is_err());
 }
 #[test]
 fn quota_500_not_reported_as_500_trades(){
  let r=paper_daily_report_from_validated_snapshot(
   &point(),&calendar(),1_792_120_015_000).unwrap();
  assert_eq!(r["modes"]["paper"]["counts"]["observed_verified_modeled_opens"],0);
  assert_eq!(r["modes"]["paper"]["counts"]["session_counter_entries_not_trades"],500);
  assert_eq!(r["modes"]["paper"]["known_net_pnl_usdt"],Value::Null);
  assert_eq!(r["modes"]["real"]["known_net_pnl_usdt"],Value::Null);
  assert_eq!(r["real_pnl_available"],false);
  assert_eq!(r["advisor_ready"],false);
  assert_eq!(r["evidence_incomplete"],true);
 }
 #[test]
 fn ai_from_public_paper_never_claims_real_truth(){
  let (input,a)=paper_advisor_from_validated_snapshot(
    &point(),&calendar(),1_792_120_015_000).unwrap();
  assert_eq!(input["summary"]["real_launch_authorized"],false);
  assert_eq!(a["strategy_tuning_allowed"],false);
  assert_eq!(a["policy"]["auto_apply_allowed"],false);
  assert_eq!(a["policy"]["order_submission_allowed"],false);
  assert_eq!(a["recommendation_count"],2);
 }
 #[test]
 fn stale_day_or_impossible_calendar_is_denied(){
  let p=point();
  let mut c=calendar();
  c.day+=1;
  assert_eq!(paper_daily_report_from_validated_snapshot(
   &p,&c,1_792_120_015_000),Err(PaperAiError::InvalidPaperSource));
  let mut c=calendar();
  c.last_seen_ms=1_792_130_000_000;
  assert_eq!(paper_daily_report_from_validated_snapshot(
   &p,&c,1_792_120_015_000),Err(PaperAiError::InvalidPaperSource));
  assert_eq!(paper_daily_report_from_validated_snapshot(
   &p,&calendar(),1_792_120_200_001),Err(PaperAiError::InvalidPaperSource));
 }
 #[test]
 fn partial_model_result_stays_unproven_not_real_pnl(){
  let mut c=calendar();
  c.opened=3;c.closed=2;c.model_micro_usdt=2_000_000;
  let r=paper_daily_report_from_validated_snapshot(
   &point(),&c,1_792_120_015_000).unwrap();
  assert_eq!(r["modes"]["paper"]["counts"]["observed_verified_modeled_closes"],2);
  assert_eq!(r["modes"]["paper"]["pnl_complete"],false);
  assert_eq!(r["modes"]["real"]["known_net_pnl_usdt"],Value::Null);
  assert!(r.get("model_micro_usdt").is_none());
 }
}
