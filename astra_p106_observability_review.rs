//! P1-06 Rust-only offline analytics and Telegram-format preview.
use spotarb_native_market_core::{
 native_trading_config::NativeTradingConfig,
 native_observability_review::{
  NativeObservePolicy,OfflineNativeSample,ObservationKind,
  inspect_native_observation_without_io,ObserveError,
 },
};
const CONFIG:&str=include_str!("../config/native_trading.yaml");
fn policy()->NativeObservePolicy{
 NativeObservePolicy::from_config(&NativeTradingConfig::parse(CONFIG).unwrap()).unwrap()
}
fn sample()->OfflineNativeSample{
 OfflineNativeSample{
  observed_ms:1_780_000_000_100,
  owner_generation:7,
  symbol:Some("BNT-USDT".into()),
  net_spread_milli_bps:Some(42_000),
  book_age_ms:200,
  decisions:129_000,
  ws_to_decision_p99_micros:21_030,
  worker_queue_p99_micros:20_540,
  ws_reconnects:0,worker_drops:0,
  fault_code:None,
 }
}
#[test]
fn russian_spread_preview_is_pure_no_telegram_send_no_trade(){
 let p=policy();
 assert_eq!(p.min_entry_milli_bps,42_000);
 assert_eq!(p.book_stale_ms,800);
 assert!(p.analytics_enabled);
 assert!(!p.can_send_telegram());
 assert!(!p.can_trigger_ai_or_trade());
 let s=sample();
 let now=1_780_000_000_200;
 let a=inspect_native_observation_without_io(
   &p,ObservationKind::Candidate,&s,now).unwrap();
 assert_eq!(a.decision_reason,"REVIEW_ONLY_SPREAD");
 assert!(a.russian_telegram_preview.as_ref().unwrap().contains("42.000 б.п."));
 assert!(!a.authenticated_exchange_source);
 assert!(!a.actual_profit_proven);
 assert!(!a.telegram_delivered);
 assert!(!a.may_trade_or_release);
 assert_eq!(a.analytics["exchange_authenticated"],false);
 assert_eq!(a.analytics["real_trading_authorized"],false);
 assert_eq!(a.analytics["config_sha256"],p.config_sha256);
 assert_eq!(a,inspect_native_observation_without_io(
   &p,ObservationKind::Candidate,&s,now).unwrap());
}
#[test]
fn stale_below_threshold_and_disabled_signal_do_not_spam(){
 let mut p=policy();
 let mut s=sample();
 s.net_spread_milli_bps=Some(41_999);
 let a=inspect_native_observation_without_io(
   &p,ObservationKind::Candidate,&s,s.observed_ms).unwrap();
 assert_eq!(a.decision_reason,"BELOW_ENTRY_THRESHOLD");
 assert!(a.russian_telegram_preview.is_none());
 s.net_spread_milli_bps=Some(42_001);
 s.book_age_ms=p.book_stale_ms+1;
 let stale=inspect_native_observation_without_io(
   &p,ObservationKind::Candidate,&s,s.observed_ms).unwrap();
 assert_eq!(stale.decision_reason,"STALE_BOOK");
 assert!(stale.russian_telegram_preview.is_none());
 s.book_age_ms=200;
 p.telegram_signals=false;
 assert_eq!(inspect_native_observation_without_io(
   &p,ObservationKind::Candidate,&s,s.observed_ms).unwrap()
   .decision_reason,"SIGNALS_DISABLED");
 p.telegram_signals=true;p.telegram_enabled=false;
 assert!(inspect_native_observation_without_io(
   &p,ObservationKind::Candidate,&s,s.observed_ms).unwrap()
   .russian_telegram_preview.is_none());
}
#[test]
fn errors_and_service_heartbeat_respect_separate_config_switches(){
 let mut p=policy();
 let mut s=sample();
 s.fault_code=Some("WS_TIMEOUT".into());
 let fault=inspect_native_observation_without_io(
   &p,ObservationKind::Fault,&s,s.observed_ms).unwrap();
 assert!(fault.russian_telegram_preview.unwrap().contains("Ошибка наблюдения Rust"));
 p.telegram_errors=false;
 assert_eq!(inspect_native_observation_without_io(
   &p,ObservationKind::Fault,&s,s.observed_ms).unwrap()
   .decision_reason,"ERRORS_DISABLED");
 p.telegram_heartbeat=true;p.telemetry_service_send_tg=true;
 let alive=inspect_native_observation_without_io(
   &p,ObservationKind::Heartbeat,&s,s.observed_ms).unwrap();
 assert!(alive.russian_telegram_preview.unwrap().contains("решений 129000"));
 p.telemetry_service_send_tg=false;
 assert!(inspect_native_observation_without_io(
   &p,ObservationKind::Heartbeat,&s,s.observed_ms).unwrap()
   .russian_telegram_preview.is_none());
}
#[test]
fn injected_symbol_bad_sample_and_false_source_events_are_rejected(){
 let p=policy();let mut s=sample();
 s.symbol=Some("BNT-USDT\nБИРЖА: УСПЕХ".into());
 assert_eq!(inspect_native_observation_without_io(
   &p,ObservationKind::Candidate,&s,s.observed_ms),
   Err(ObserveError::InvalidSample));
 s=sample();s.observed_ms=1_780_000_000_300;
 assert_eq!(inspect_native_observation_without_io(
   &p,ObservationKind::Candidate,&s,1_780_000_000_200),
   Err(ObserveError::InvalidSample));
 s=sample();s.net_spread_milli_bps=None;
 assert_eq!(inspect_native_observation_without_io(
   &p,ObservationKind::Candidate,&s,s.observed_ms),
   Err(ObserveError::InvalidSample));
 s=sample();s.fault_code=Some("UNKNOWN\nfalse profit".into());
 assert_eq!(inspect_native_observation_without_io(
   &p,ObservationKind::Fault,&s,s.observed_ms),
   Err(ObserveError::InvalidSample));
 s=sample();s.book_age_ms=1_000_001;
 assert_eq!(inspect_native_observation_without_io(
   &p,ObservationKind::Candidate,&s,s.observed_ms),
   Err(ObserveError::InvalidSample));
}
#[test]
fn analytics_disabled_returns_null_and_no_bypass(){
 let mut p=policy();p.analytics_enabled=false;
 let s=sample();
 let a=inspect_native_observation_without_io(
   &p,ObservationKind::Candidate,&s,s.observed_ms).unwrap();
 assert!(a.analytics.is_null());
 assert!(!a.may_trade_or_release);
 assert!(!p.can_send_telegram());
 let mut cfg=NativeTradingConfig::parse(CONFIG).unwrap();
 cfg.raw["analytics"]["mode"]=serde_yaml::Value::String("trade".into());
 assert_eq!(NativeObservePolicy::from_config(&cfg),Err(ObserveError::Config));
}
#[test]
fn replay_fence_suppresses_duplicate_spreads_and_noisy_heartbeat(){
 use spotarb_native_market_core::native_observability_review::OfflineObservationReplayFence;
 let p=policy();let mut fence=OfflineObservationReplayFence::new(&p);
 let mut s=sample();
 assert_eq!(p.symbol_cooldown_sec,120);
 let first=fence.inspect_once(&p,ObservationKind::Candidate,&s,s.observed_ms).unwrap();
 assert_eq!(first.decision_reason,"REVIEW_ONLY_SPREAD");
 assert!(!fence.may_send_real_telegram_or_trade());
 assert_eq!(fence.inspect_once(&p,ObservationKind::Candidate,&s,s.observed_ms)
   .unwrap().decision_reason,"REPLAY_OR_COOLDOWN");
 s.observed_ms+=119_999;
 assert!(fence.inspect_once(&p,ObservationKind::Candidate,&s,s.observed_ms)
   .unwrap().russian_telegram_preview.is_none());
 s.observed_ms+=1;
 assert!(fence.inspect_once(&p,ObservationKind::Candidate,&s,s.observed_ms)
   .unwrap().russian_telegram_preview.is_some());
 let backwards=sample();
 assert_eq!(fence.inspect_once(&p,ObservationKind::Candidate,&backwards,
    s.observed_ms),Err(ObserveError::InvalidSample));
 let h=fence.inspect_once(&p,ObservationKind::Heartbeat,&s,s.observed_ms).unwrap();
 assert_eq!(h.decision_reason,"REVIEW_ONLY_HEARTBEAT");
 s.observed_ms+=500;
 assert_eq!(fence.inspect_once(&p,ObservationKind::Heartbeat,&s,s.observed_ms)
   .unwrap().decision_reason,"REPLAY_OR_COOLDOWN");
 let mut other=sample();other.symbol=Some("ETH-USDT".into());
 assert_eq!(fence.inspect_once(&p,ObservationKind::Candidate,&other,other.observed_ms)
   .unwrap().decision_reason,"REVIEW_ONLY_SPREAD");
 let mut mismatched=p.clone();mismatched.config_sha256="0".repeat(64);
 assert_eq!(fence.inspect_once(&mismatched,ObservationKind::Candidate,&other,
   other.observed_ms),Err(ObserveError::Config));
}
