//! Astra A1/A2: six existing research consumers must reject post-parse drift.
//! No orders, DB access or network.
use spotarb_native_market_core::{
 native_trading_config::NativeTradingConfig,
 native_observability_review::NativeObservePolicy,
 native_ai_storage_review::NativeAnalyticsStorageLimits,
 native_shadow_config_review::NativeShadowReview,
 native_public_feed_policy_review::PublicFeedPolicy,
 native_candidate_holdoff_review::CandidateHoldoffPolicy,
 native_paper_report_schedule_review::NativePaperReportSchedule,
};
fn assert_six_fail(c:&NativeTradingConfig){
 assert!(NativeObservePolicy::from_config(c).is_err(),"observability");
 assert!(NativeAnalyticsStorageLimits::from_native_config(c).is_err(),"storage");
 assert!(NativeShadowReview::from_config(c).is_err(),"shadow");
 assert!(PublicFeedPolicy::from_config(c).is_err(),"public-feed");
 assert!(CandidateHoldoffPolicy::from_config(c).is_err(),"holdoff");
 assert!(NativePaperReportSchedule::from_config(c).is_err(),"schedule");
}
#[test]
fn a2_all_six_advisory_consumers_reject_mutable_raw_policy_and_sha(){
 let original=NativeTradingConfig::parse(
     include_str!("../config/native_trading.yaml")).unwrap();
 assert!(NativeObservePolicy::from_config(&original).is_ok());
 assert!(NativeAnalyticsStorageLimits::from_native_config(&original).is_ok());
 assert!(NativeShadowReview::from_config(&original).is_ok());
 assert!(PublicFeedPolicy::from_config(&original).is_ok());
 assert!(CandidateHoldoffPolicy::from_config(&original).is_ok());
 assert!(NativePaperReportSchedule::from_config(&original).is_ok());
 let mut raw=original.clone();
 raw.raw["analytics"]["max_db_mb"]=serde_yaml::Value::from(4096u64);
 assert_six_fail(&raw);
 let mut policy=original.clone();
 policy.policy.general_daily_trades+=1;
 assert_six_fail(&policy);
 let mut sha=original.clone();
 sha.sha256="f".repeat(64);
 assert_six_fail(&sha);
}
