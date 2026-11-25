//! R50: one-shot READ-ONLY validated live Rust PAPER -> Rust advisory.
 //! Never sends orders, private GETs, Telegram messages or updates watch state.
use spotarb_native_market_core::{
 native_trading_config::NativeTradingConfig,
 native_paper_telegram_observer::parse_native_paper_status,
 native_paper_notice_queue_review::PaperWatchState,
 native_paper_ai_bridge_review::paper_ai_analytics_source_review_without_io,
 native_paper_ingest_quality_review::inspect_paper_ingest_samples,
 native_shadow_config_review::NativeShadowReview,
 native_public_feed_policy_review::PublicFeedPolicy,
 native_candidate_holdoff_review::CandidateHoldoffPolicy,
 native_paper_report_schedule_review::NativePaperReportSchedule,
 native_paper_hedge_quality_review::inspect_modeled_hedge_quality,
 native_paper_config_drift_review::inspect_native_paper_config_drift,
 native_paper_model_config_review::PaperModelConfigReview,
 native_exit_safety_config_review::inspect_exit_safety_yaml,
};
use serde_json::{json,Value};
use std::{fs::OpenOptions,io::Read,
 os::unix::fs::{OpenOptionsExt,MetadataExt},path::Path,
 time::{SystemTime,UNIX_EPOCH}};
const SOURCE:&str="/mnt/spotarb_db_test/astra_paper_continuous_v1";
const WATCH:&str="/mnt/spotarb_db_test/astra_p0_02_execution_gate_20261011/r37_paper_telegram_preview_watch_20261015/durable_watch_queue.json";
fn deny(reason:&str)->!{eprintln!("R50_READONLY_AI_DENIED:{reason}");std::process::exit(3)}
fn read_bounded(path:&Path)->Vec<u8>{
 let mut file=OpenOptions::new().read(true)
  .custom_flags(libc::O_NOFOLLOW|libc::O_CLOEXEC).open(path)
  .unwrap_or_else(|_|deny("MISSING_OR_SYMLINK"));
 let meta=file.metadata().unwrap_or_else(|_|deny("METADATA"));
 if !meta.is_file()||meta.mode()&0o077!=0||meta.nlink()!=1||
   meta.len()>65536{deny("PERMISSIONS_OR_SIZE")}
 let mut bytes=Vec::new();
 Read::by_ref(&mut file).take(65537).read_to_end(&mut bytes)
  .unwrap_or_else(|_|deny("READ_ERROR"));
 if bytes.len()>65536{deny("OVERSIZED")}
 bytes
}
fn j(path:&Path)->Value{
 serde_json::from_slice(&read_bounded(path)).unwrap_or_else(|_|deny("JSON_CORRUPT"))
}
fn now_ms()->u64{
 SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}
fn main(){
 let root=Path::new(SOURCE);
 let current_path=root.join("current_session.json");
 let active_path=root.join("active.json");
 let first=read_bounded(&current_path);
 let second=read_bounded(&active_path);
 let current:Value=serde_json::from_slice(&first).unwrap_or_else(|_|deny("CURRENT_JSON"));
 let active:Value=serde_json::from_slice(&second).unwrap_or_else(|_|deny("ACTIVE_JSON"));
 let session=current.get("session").and_then(Value::as_str)
  .unwrap_or_else(||deny("SESSION"));
 if session.len()>100||!session.starts_with("session_")||
   !session.bytes().all(|x|x.is_ascii_alphanumeric()||x==b'_'||x==b'-'){
   deny("SESSION_NAME")
 }
 let status_path=root.join("sessions").join(session).join("status.json");
 if current.get("status_path").and_then(Value::as_str)!=status_path.to_str(){
  deny("SESSION_PATH_MISMATCH")
 }
 let status=j(&status_path);
 if first!=read_bounded(&current_path)||second!=read_bounded(&active_path){
  deny("SESSION_RACE")
 }
 let config_path=Path::new(env!("CARGO_MANIFEST_DIR")).join("config/native_trading.yaml");
 let cfg=NativeTradingConfig::load(&config_path).unwrap_or_else(|_|deny("CONFIG"));
 let now=now_ms();
 let point=parse_native_paper_status(&current,&active,&status,now,&cfg)
  .unwrap_or_else(|_|deny("LIVE_PAPER_SOURCE"));
 let watch:PaperWatchState=serde_json::from_slice(&read_bounded(Path::new(WATCH)))
  .unwrap_or_else(|_|deny("WATCH_STATE"));
 let calendar=watch.calendar.as_ref().unwrap_or_else(||deny("MISSING_CALENDAR"));
 if watch.schema!=1||watch.config_sha256!=cfg.sha256||
   watch.cursor.last_seen.session!=point.session||
   watch.cursor.last_seen.day!=point.day||
   watch.cursor.last_seen.updated_wall_ms>point.updated_wall_ms||
   point.updated_wall_ms-watch.cursor.last_seen.updated_wall_ms>300_000||
   calendar.day!=point.day||
   calendar.last_seen_ms>now||
   watch.revision==0 {deny("WATCH_SOURCE_NOT_FRESH")}
 let ingest_diagnostics=inspect_paper_ingest_samples(&status)
  .unwrap_or_else(|_|deny("INGEST_SAMPLE_GUARD"));
 let shadow_policy=NativeShadowReview::from_config(&cfg)
  .unwrap_or_else(|_|deny("SHADOW_POLICY_GUARD"));
 let shadow_observation=shadow_policy.summarize_paper_observation(&status)
  .unwrap_or_else(|_|deny("PUBLIC_WS_CENSUS_GUARD"));
 let public_feed=PublicFeedPolicy::from_config(&cfg)
  .unwrap_or_else(|_|deny("PUBLIC_FEED_POLICY_GUARD"));
 let public_feed_observation=public_feed.observe_readonly(&status,now)
  .unwrap_or_else(|_|deny("PUBLIC_FEED_SOURCE_GUARD"));
 let holdoff=CandidateHoldoffPolicy::from_config(&cfg)
  .unwrap_or_else(|_|deny("CANDIDATE_HOLDOFF_POLICY_GUARD"));
 let holdoff_diagnostic=holdoff.configuration_only_summary();
 let schedule=NativePaperReportSchedule::from_config(&cfg)
  .unwrap_or_else(|_|deny("PAPER_SCHEDULE_POLICY_GUARD"));
 let schedule_observation=schedule.observation();
 let config_drift=inspect_native_paper_config_drift(&cfg,&status)
  .unwrap_or_else(|_|deny("PAPER_CONFIG_DRIFT_SOURCE_GUARD"));
 let model_cfg=PaperModelConfigReview::from_config(&cfg)
  .unwrap_or_else(|_|deny("PAPER_MODEL_CONFIG_POLICY_GUARD"));
 let exit_policy=inspect_exit_safety_yaml(&cfg)
  .unwrap_or_else(|_|deny("EXIT_SAFETY_CONFIG_POLICY_GUARD"));
 let model_path=root.join("sessions").join(session).join("modeled_four_leg_journal.jsonl");
 let hedge_quality=inspect_modeled_hedge_quality(&model_path)
  .unwrap_or_else(|_|json!({"status":"UNVERIFIED_OR_BUSY_MODELED_SOURCE",
   "authenticated_fills":false,"real_pnl_certified":false,
   "may_trade_or_adjust_config":false}));
 let (input,advice,analytics_source_review)=
   paper_ai_analytics_source_review_without_io(&cfg,&point,calendar,now,None)
     .unwrap_or_else(|_|deny("RUST_ADVISORY_ANALYTICS_SOURCE_GATE"));
 println!("{}",json!({
  "schema":"native_r50_paper_ai_readonly_v1",
  "source":"PUBLIC_PAPER_R37_DURABLE_WATCH",
  "live_paper_snapshot_validated":true,
  "review_revision":watch.revision,
  "session":point.session,
  "paper_day":point.day,
  "status_updated_ms":point.updated_wall_ms,
  "calendar_sessions_observed":calendar.session_count,
  "calendar_incomplete":true,
  "paper_entries_counter_may_be_quota":point.entries,
  "model_opened_observed":calendar.opened,
  "model_closed_observed":calendar.closed,
  "real_pnl_available":false,
  "real_execution_permitted":false,
  "private_exchange_access":false,
  "telegram_sent":false,
  "source_writes":false,
  "ingest_diagnostics":ingest_diagnostics,
  "read_only_shadow_config_observation":shadow_observation,
  "read_only_public_feed_config_observation":public_feed_observation,
  "read_only_candidate_holdoff_config":holdoff_diagnostic,
  "read_only_paper_report_schedule":schedule_observation,
  "read_only_modeled_hedge_quality":hedge_quality,
  "read_only_paper_config_drift":config_drift,
  "read_only_paper_model_config":model_cfg.observation(),
  "rust_analytics_source_review":analytics_source_review,
  "read_only_exit_safety_config":exit_policy,
  "sanitized_advisor_input":input,
  "advice":advice
 }));
}
