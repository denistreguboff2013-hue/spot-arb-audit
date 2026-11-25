// Astra P1-06: 40 distinct, deterministic PRIVATE research-only
// differential tests. R979-R1018. Neither installed YAML nor installed
// ELF is modified. Do NOT count these 40 guards as 40 applied YAML fields.
// All retained original 160 fields must remain typed/pinned.
fn r979_change_once(source:&str,before:&str,after:&str)->String{
 assert_eq!(source.matches(before).count(),1,"fixture must be unique: {before}");
 let changed=source.replacen(before,after,1);
 assert_ne!(source,changed);
 changed
}
fn r979_deny_candidate(source:&str){
 match NativeTradingConfig::parse(source){
  Ok(cfg)=>assert!(admit_public_paper_yaml_review(&cfg).is_err(),
   "unsafe candidate must NOT opt in PAPER"),
  Err(_)=>(), // parser rejection also safely denies
 }
}
macro_rules! p106_deny{
 ($name:ident,$before:literal,$after:literal)=>{
  #[test]fn $name(){
   let candidate=r979_change_once(&aligned_paper_source(),$before,$after);
   r979_deny_candidate(&candidate);
  }
 };
}
// R979-R990: no silent REAL, policy, quota or feature bypass.
// These test optional PAPER admission, NOT real-trading exchange safety.
p106_deny!(r979_real_general_trade_flag_denied,
 "trade_enabled: false","trade_enabled: true");
p106_deny!(r980_real_trial_enabled_denied,
 "real_trial:\n  enabled: false","real_trial:\n  enabled: true");
p106_deny!(r981_real_autonomous_flag_denied,
 "autonomous_real_enabled: false","autonomous_real_enabled: true");
p106_deny!(r982_native_real_authority_denied,
 "real_execution_authorized: false","real_execution_authorized: true");
p106_deny!(r983_native_mutation_authority_denied,
 "mutation_capability_enabled: false","mutation_capability_enabled: true");
p106_deny!(r984_native_production_cutover_denied,
 "production_cutover_authorized: false","production_cutover_authorized: true");
p106_deny!(r985_paper_daily_501_mismatched_denied,
 "paper_max_trades_per_day: 500","paper_max_trades_per_day: 501");
p106_deny!(r986_paper_concurrency_9_mismatch_denied,
 "risk:\n  futures_order_usdt: 10\n  max_open_positions: 10",
 "risk:\n  futures_order_usdt: 10\n  max_open_positions: 9");
p106_deny!(r987_paper_entries_disabled_denied,
 "paper_entries_enabled: true","paper_entries_enabled: false");
p106_deny!(r988_unwired_history_window_change_denied,
 "history_window: 3","history_window: 4");
p106_deny!(r989_unwired_dynamic_enabled_denied,
 "dynamic:\n  enabled: false","dynamic:\n  enabled: true");
p106_deny!(r990_unwired_analytics_enabled_denied,
 "analytics:\n  enabled: false","analytics:\n  enabled: true");

// R991-R999: nine unsupported legacy YAML knobs must NOT be accepted by
// the existing R174 source feature-consumer guard merely by parsing.
fn r991_frozen_candidate()->String{
 r979_change_once(
  include_str!("../config/native_trading_operator_baseline_r64.yaml"),
  "scale_in_hints:\n  enabled: true",
  "scale_in_hints:\n  enabled: false")
}
macro_rules! p106_frozen{
 ($name:ident,$before:literal,$after:literal)=>{
  #[test]fn $name(){
   let source=r979_change_once(&r991_frozen_candidate(),$before,$after);
   let parsed=NativeTradingConfig::parse(&source).unwrap();
   assert_eq!(crate::native_r174_unwired_legacy_guard::
     verify_unwired_source_r174(&parsed),
     Err(crate::native_r174_unwired_legacy_guard::R174Error::Unsupported));
   // R1019: a SECOND optional PAPER admission path must also deny it.
   let optional=r979_change_once(&aligned_paper_source(),$before,$after);
   let parsed=NativeTradingConfig::parse(&optional).unwrap();
   assert!(matches!(admit_public_paper_yaml_review(&parsed),
      Err(BridgeError::Unsupported)),"unwired field incorrectly admitted");
  }
 };
}
p106_frozen!(r991_unwired_bot_name_denied,"bot_name: spot-arb-bot","bot_name: another-bot");
p106_frozen!(r992_unwired_debug_liquidity_denied,"debug_write_liquidity: true","debug_write_liquidity: false");
p106_frozen!(r993_unwired_liquidity_bps_denied,"liquidity_bps: 50","liquidity_bps: 51");
p106_frozen!(r994_unwired_mode_denied,"mode: soft","mode: strict");
p106_frozen!(r995_unwired_logging_level_denied,"level: DEBUG","level: INFO");
p106_frozen!(r996_unwired_imbalance_denied,"min_imbalance: 1.03","min_imbalance: 1.04");
p106_frozen!(r997_unwired_velocity_min_denied,"min_vel_bps_s: 0.08","min_vel_bps_s: 0.09");
p106_frozen!(r998_unwired_negative_telemetry_denied,"send_negative: false","send_negative: true");
p106_frozen!(r999_unwired_spread_history_denied,"history_window: 3","history_window: 4");

// R1000-R1009: mutation AFTER native YAML parse must be detected; a
// cloned in-memory raw config is not a new authorized source revision.
// Included cost, risk, real flags, quota, analyst feature, blacklist.
macro_rules! p106_tamper{
 ($name:ident,$section:literal,$key:literal,$value:expr)=>{
  #[test]fn $name(){
   let mut cfg=NativeTradingConfig::parse(&aligned_paper_source()).unwrap();
   cfg.raw[$section][$key]=$value;
   assert!(!cfg.source_projection_unchanged());
   assert!(matches!(admit_public_paper_yaml_review(&cfg),Err(BridgeError::Policy)));
  }
 };
}
p106_tamper!(r1000_after_parse_trade_flag,"general","trade_enabled",serde_yaml::Value::Bool(true));
p106_tamper!(r1001_after_parse_operator_paper_flag,"general","paper_entries_enabled",serde_yaml::Value::Bool(false));
p106_tamper!(r1002_after_parse_real_trial_enable,"real_trial","enabled",serde_yaml::Value::Bool(true));
p106_tamper!(r1003_after_parse_autonomous_real,"real_trial","autonomous_real_enabled",serde_yaml::Value::Bool(true));
p106_tamper!(r1004_after_parse_entry_threshold,"spread","entry_bps",serde_yaml::Value::from(99));
p106_tamper!(r1005_after_parse_spot_budget,"risk","futures_order_usdt",serde_yaml::Value::from(7));
p106_tamper!(r1006_after_parse_paper_quota,"limits","paper_max_trades_per_day",serde_yaml::Value::from(501));
p106_tamper!(r1007_after_parse_max_positions,"risk","max_open_positions",serde_yaml::Value::from(9));
p106_tamper!(r1008_after_parse_unwired_analytics,"analytics","enabled",serde_yaml::Value::Bool(true));
p106_tamper!(r1009_after_parse_ws_staleness,"websocket","stale_ms",serde_yaml::Value::from(60000));

// R1010-R1018: actual pure Rust opt-in research bindings MUST respond to
// supported YAML changes; no claims about the installed old PAPER binary.
// Each compares BASE and a semantically different, separately parsed YAML.
macro_rules! p106_changed{
 ($name:ident,$before:literal,$after:literal,$check:expr)=>{
  #[test]fn $name(){
   let reference=NativeTradingConfig::parse(&aligned_paper_source()).unwrap();
   let edited=r979_change_once(&aligned_paper_source(),$before,$after);
   let altered=NativeTradingConfig::parse(&edited).unwrap();
   assert!(reference.source_projection_unchanged()&&altered.source_projection_unchanged());
   ($check)(&reference,&altered);
  }
 };
}
p106_changed!(r1010_min_lifetime_controls_research_policy,
 "min_lifetime_sec: 15","min_lifetime_sec: 30",
 |a:&NativeTradingConfig,b:&NativeTradingConfig|{
  let old=bind_public_paper_temporal_policy_review(a).unwrap();
  let new=bind_public_paper_temporal_policy_review(b).unwrap();
  assert_eq!(old.min_lifetime_ns,15_000_000_000);
  assert_eq!(new.min_lifetime_ns,30_000_000_000);
 });
p106_changed!(r1011_exit_spread_controls_research_policy,
 "exit_bps: 0.5","exit_bps: 1.0",
 |a:&NativeTradingConfig,b:&NativeTradingConfig|{
  assert_eq!(bind_public_paper_temporal_policy_review(a).unwrap().exit_spread_bps,0.5);
  assert_eq!(bind_public_paper_temporal_policy_review(b).unwrap().exit_spread_bps,1.0);
 });
p106_changed!(r1012_reentry_cooldown_controls_research_policy,
 "symbol_after_close_sec: 120","symbol_after_close_sec: 60",
 |a:&NativeTradingConfig,b:&NativeTradingConfig|{
  assert_eq!(bind_public_paper_temporal_policy_review(a).unwrap().reentry_cooldown_ns,120_000_000_000);
  assert_eq!(bind_public_paper_temporal_policy_review(b).unwrap().reentry_cooldown_ns,60_000_000_000);
 });
p106_changed!(r1013_net_target_controls_research_policy,
 "net_target_bps: 12.0","net_target_bps: 5.0",
 |a:&NativeTradingConfig,b:&NativeTradingConfig|{
  assert_eq!(bind_public_paper_temporal_policy_review(a).unwrap().net_target_bps,12.0);
  assert_eq!(bind_public_paper_temporal_policy_review(b).unwrap().net_target_bps,5.0);
 });
p106_changed!(r1014_websocket_stale_changes_research_decision,
 "stale_ms: 800","stale_ms: 900",
 |a:&NativeTradingConfig,b:&NativeTradingConfig|{
  assert_eq!(bind_public_paper_decision_config(a).unwrap().max_age_ms,800);
  assert_eq!(bind_public_paper_decision_config(b).unwrap().max_age_ms,900);
 });
p106_changed!(r1015_entry_bps_changes_research_decision,
 "entry_bps: 42.0","entry_bps: 43.0",
 |a:&NativeTradingConfig,b:&NativeTradingConfig|{
  assert_eq!(bind_public_paper_decision_config(a).unwrap().required_net,42.0);
  assert_eq!(bind_public_paper_decision_config(b).unwrap().required_net,43.0);
 });
p106_changed!(r1016_min_price_changes_research_decision,
 "min_price: 0.03","min_price: 0.04",
 |a:&NativeTradingConfig,b:&NativeTradingConfig|{
  assert_eq!(bind_public_paper_decision_config(a).unwrap().min_price,0.03);
  assert_eq!(bind_public_paper_decision_config(b).unwrap().min_price,0.04);
 });
p106_changed!(r1017_max_price_changes_research_decision,
 "max_price: 200000","max_price: 100000",
 |a:&NativeTradingConfig,b:&NativeTradingConfig|{
  assert_eq!(bind_public_paper_decision_config(a).unwrap().max_price,200_000.0);
  assert_eq!(bind_public_paper_decision_config(b).unwrap().max_price,100_000.0);
 });
p106_changed!(r1018_blacklist_changes_research_decision,
 "- ZEC\nanalytics:","- ZEC\n- SOL\nanalytics:",
 |a:&NativeTradingConfig,b:&NativeTradingConfig|{
  assert!(!bind_public_paper_decision_config(a).unwrap().blacklist.contains("SOL"));
  assert!(bind_public_paper_decision_config(b).unwrap().blacklist.contains("SOL"));
 });
