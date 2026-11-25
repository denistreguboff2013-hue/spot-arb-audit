//! R32D6C8A ordinary-linked Rust tests: no live exchange/mutation.
use spotarb_native_market_core::{
 authoritative_state::NativeEntryPlan,
 native_trading_config::{NativeTradingConfig,ConfigError},
 fill_economics::{LegFillLedger,FillEvent,decimal_to_atoms,DEFAULT_AVG_TOLERANCE_ATOMS},
};
use std::path::PathBuf;
const CONFIG:&str=include_str!("../config/native_trading.yaml");
fn policy()->NativeTradingConfig{NativeTradingConfig::parse(CONFIG).unwrap()}
fn one(n:u128)->u128{n*1_000_000_000_000_000_000}
fn fixture(q:&str)->NativeEntryPlan{
 NativeEntryPlan{base:"BNT".into(),spot_symbol:"BNT/USDT".into(),
  futures_symbol:"BNTUSDT".into(),created_at_ms:1_780_000_000_000,
  planned_qty_atoms_e18:decimal_to_atoms(q).unwrap()*1_000_000,
  order_quote_atoms_e18:one(10),spot_baseline_atoms_e18:one(0),
  safety_generation:7}
}
fn ledger(q:&str,fee:&str)->LegFillLedger{
 let qty=decimal_to_atoms(q).unwrap();
 let mut row=LegFillLedger::new("okx","BNT/USDT","buy","spot-intent","sab00000000000000000000000000991",qty).unwrap();
 row.apply_cumulative_truth(qty,Some(decimal_to_atoms("100").unwrap()),
  true,Some("filled"),Some("789"),Some(1_780_000_000_100),DEFAULT_AVG_TOLERANCE_ATOMS).unwrap();
 row.apply_fill(FillEvent{fill_id:"T1".into(),price_atoms:decimal_to_atoms("100").unwrap(),
  qty_atoms:qty,fee_cost_quote_atoms:Some(decimal_to_atoms(fee).unwrap() as i128),
  fee_currency:Some("USDT".into()),trade_time_ms:Some(1_780_000_000_100)},
  DEFAULT_AVG_TOLERANCE_ATOMS).unwrap();
 assert!(row.fee_evidence_complete);
 row
}
#[test]
fn r32d6c8_schema_mirrors_all_26_sections_and_156_leaves_in_rust(){
 let x=policy();
 assert_eq!(x.mirrored_sections,26);
 assert_eq!(x.mirrored_leaf_paths,156);
 assert!(x.sha256.len()==64&&x.sha256.bytes().all(|v|v.is_ascii_hexdigit()));
 assert_eq!(x.raw.get("general").unwrap().get("report_timezone").unwrap().as_str(),Some("Asia/Yekaterinburg"));
 assert_eq!(x.raw.get("execution").unwrap().get("spot_entry_mode").unwrap().as_str(),Some("aggressive_limit"));
 assert_eq!(x.raw.get("telegram").unwrap().get("proxy_url").unwrap().as_str(),Some(""));
 assert_eq!(x.policy.quote_cap_e18,one(10));
 assert_eq!(x.policy.nominal_order_e18,one(10));
 assert_eq!(x.policy.general_max_positions,10);
 assert_eq!(x.policy.real_max_positions,1);
 assert_eq!(x.policy.general_daily_trades,300);
 assert_eq!(x.policy.paper_daily_trades,3000);
 assert_eq!(x.policy.real_daily_trades,0);
 assert_eq!(x.policy.real_cooldown_sec,10800);
 assert_eq!(x.policy.entry_min_milli_bps,42_000);
 assert_eq!(x.policy.fee_reserve_milli_bps,24_030);
 assert_eq!(x.policy.daily_loss_cap_e18,one(10));
 assert_eq!(x.policy.daily_okx_fee_cap_e18,one(7));
 assert!(!x.policy.real_execution_authorized);
 assert_eq!(x.migration_status,"MIRRORED_AND_VALIDATED_NOT_FULLY_WIRED");
}
#[test]
fn r32d6c8_schema_file_reader_and_original_source_not_accessed(){
 let file=PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config/native_trading.yaml");
 let a=NativeTradingConfig::load(&file).unwrap();
 let b=policy();
 assert_eq!(a.sha256,b.sha256);
 assert_eq!(a.policy,b.policy);
 assert_eq!(a.mirrored_leaf_paths,156);
}
#[test]
fn r32d6c8_rejects_unknown_param_removed_key_type_drift_or_yaml_invalid(){
 let stripped=CONFIG.replace("  entry_bps: 42.0\n","");
 assert!(matches!(NativeTradingConfig::parse(&stripped),Err(ConfigError::Schema)));
 let unknown=CONFIG.replace("spread:\n","spread:\n  new_unapproved_trade_setting: 1\n");
 assert!(matches!(NativeTradingConfig::parse(&unknown),Err(ConfigError::Schema)));
 let wrong=CONFIG.replace("  max_trades_per_day: 300\n","  max_trades_per_day: '300'\n");
 assert!(matches!(NativeTradingConfig::parse(&wrong),Err(ConfigError::Schema)));
 assert!(NativeTradingConfig::parse("[not a config]").is_err());
 assert!(NativeTradingConfig::parse("x".repeat(300_000).as_str()).is_err());
}
#[test]
fn r32d6c8_legacy_approval_flags_never_authorize_real(){
 let changed=CONFIG
  .replace("  autonomous_real_enabled: false","  autonomous_real_enabled: true")
  .replace("  operator_approved: false","  operator_approved: true");
 let x=NativeTradingConfig::parse(&changed).unwrap();
 assert!(x.raw.get("real_trial").unwrap().get("autonomous_real_enabled").unwrap().as_bool().unwrap());
 assert!(x.raw.get("real_trial").unwrap().get("operator_approved").unwrap().as_bool().unwrap());
 assert!(!x.policy.real_execution_authorized);
 for name in ["real_execution_authorized","mutation_capability_enabled","production_cutover_authorized"]{
  let previous=format!("{name}: false");
  let forbidden=CONFIG.replace(&previous,&format!("{name}: true"));
  assert!(matches!(NativeTradingConfig::parse(&forbidden),Err(ConfigError::Policy)));
 }
}
#[test]
fn r32d6c8_real_daily_limit_spread_and_cap_fail_closed(){
 let variants=[
  CONFIG.replace("  max_trades_per_day: 0\n","  max_trades_per_day: 1\n"),
  CONFIG.replace("  futures_order_usdt: 10\n","  futures_order_usdt: 11\n"),
  CONFIG.replace("  entry_bps: 42.0\n","  entry_bps: 41.0\n"),
  CONFIG.replace("  entry_cooldown_sec: 10800\n","  entry_cooldown_sec: 10799\n"),
  CONFIG.replace("  max_open_positions: 1\n","  max_open_positions: 2\n"),
  CONFIG.replace("  max_daily_loss_usdt: 10.0\n","  max_daily_loss_usdt: 11.0\n"),
  CONFIG.replace("  max_okx_fees_per_day_usdt: 7.0\n","  max_okx_fees_per_day_usdt: 8.0\n"),
 ];
 for (i,text) in variants.iter().enumerate(){
  assert!(matches!(NativeTradingConfig::parse(text),Err(ConfigError::Policy)),
   "unsafe config variant {i} was accepted");
 }
}
#[test]
fn p106_decimal_risk_config_uses_conservative_fixed_point_bps_and_caps(){
 let baseline=policy();
 assert_eq!(baseline.policy.fee_reserve_milli_bps,24_030);
 assert_eq!(baseline.policy.entry_min_milli_bps,42_000);
 let below=CONFIG.replace("  entry_bps: 42.0","  entry_bps: 41.9999");
 assert!(matches!(NativeTradingConfig::parse(&below),Err(ConfigError::Policy)),
   "ceil-to-milli-bps must not admit a below-42 actual minimum");
 let fractional=CONFIG.replace("  funding_bps: 0.03","  funding_bps: 0.0301");
 let parsed=NativeTradingConfig::parse(&fractional).unwrap();
 assert_eq!(parsed.policy.fee_reserve_milli_bps,24_031,
   "fractional fee reserve rounds upward in each raw component");
 let tiny_fee=CONFIG.replace("  funding_bps: 0.03","  funding_bps: 0.0001");
 assert_eq!(NativeTradingConfig::parse(&tiny_fee).unwrap()
   .policy.fee_reserve_milli_bps,24_001);
 let positive_loss=CONFIG.replace("  max_daily_loss_usdt: 10.0",
   "  max_daily_loss_usdt: 0.01");
 assert_eq!(NativeTradingConfig::parse(&positive_loss).unwrap()
   .policy.daily_loss_cap_e18,10_000_000_000_000_000u128);
 let over_fee=CONFIG.replace("  max_okx_fees_per_day_usdt: 7.0",
    "  max_okx_fees_per_day_usdt: 7.0001");
 assert!(matches!(NativeTradingConfig::parse(&over_fee),Err(ConfigError::Policy)));
 assert!(!parsed.policy.real_execution_authorized);
}
#[test]
fn r32d6c8_quote_notional_and_fee_reserve_close_old_100_usdt_gap_in_preview(){
 let x=policy();
 let price=decimal_to_atoms("100").unwrap();
 let fut=decimal_to_atoms("101").unwrap();
 assert!(matches!(x.preview_plan(&fixture("1"),price,fut,7),Err(ConfigError::Notional)));
 assert!(matches!(x.preview_plan(&fixture("0.1"),price,fut,7),Err(ConfigError::Notional)));
 let approved=x.preview_plan(&fixture("0.09"),price,fut,7).unwrap();
 assert_eq!(approved.spot_notional_e18,one(9));
 assert_eq!(approved.futures_notional_e18,9_090_000_000_000_000_000);
 assert!(approved.quote_with_reserved_fees_e18<one(10));
 assert!(!approved.may_send_order);
 assert!(!approved.may_promote_hedged);
 assert_eq!(approved.config_sha256,x.sha256);
 assert!(matches!(x.preview_plan(&fixture("0.09"),price,fut,8),Err(ConfigError::Policy)));
}
#[test]
fn r32d6c8_actual_fill_fees_count_toward_spot_quote_cap(){
 let x=policy();
 assert_eq!(x.preview_actual_spot(&fixture("0.09"),&ledger("0.09","0.009")).unwrap(),
  9_009_000_000_000_000_000);
 let mut unconfirmed=ledger("0.09","0.009");
 unconfirmed.cumulative_truth_present=false;
 assert!(matches!(x.preview_actual_spot(&fixture("0.09"),&unconfirmed),
   Err(ConfigError::Unsupported)),
   "terminal local rows without original cumulative exchange fill proof must not price risk");
 let too_big=x.preview_actual_spot(&fixture("0.1"),&ledger("0.1","0.01"));
 assert!(matches!(too_big,Err(ConfigError::Notional)));
 let fully_unpriced=LegFillLedger::new("okx","BNT/USDT","buy","pending","sab00000000000000000000000000992",
  decimal_to_atoms("0.1").unwrap()).unwrap();
 assert!(matches!(x.preview_actual_spot(&fixture("0.1"),&fully_unpriced),Err(ConfigError::Unsupported)));
}
#[test]
fn r32d6c8_lower_local_cap_can_be_selected_without_granting_execution(){
 let changed=CONFIG.replace("  futures_order_usdt: 10\n","  futures_order_usdt: 5\n");
 let x=NativeTradingConfig::parse(&changed).unwrap();
 assert_eq!(x.policy.nominal_order_e18,one(5));
 assert!(matches!(x.preview_plan(&fixture("0.09"),
  decimal_to_atoms("100").unwrap(),decimal_to_atoms("101").unwrap(),7),Err(ConfigError::Policy)));
 assert!(!x.policy.real_execution_authorized);
}


#[test]
fn p106_legacy_four_market_filters_exact_boundary_offline_no_authority(){
 let c=policy();
 assert_eq!(c.policy.min_market_price_quote_e18,30_000_000_000_000_000);
 assert_eq!(c.policy.max_market_price_quote_e18,one(200_000));
 assert_eq!(c.policy.min_market_volume_24h_quote_e18,one(150_000));
 assert_eq!(c.policy.max_market_abs_spread_milli_bps,10_000_000);
 let price=c.policy.min_market_price_quote_e18;
 let volume=c.policy.min_market_volume_24h_quote_e18;
 let ok=c.inspect_market_filters_read_only(price,volume,-10_000_000).unwrap();
 assert!(ok.passes_legacy_filters_locally);
 assert!(!ok.may_send_or_cancel_order && !ok.exchange_source_authenticated);
 assert_eq!(ok.reason,"LOCAL_FILTER_PASS_NOT_TRADE_AUTHORIZATION");
 for (p,v,s,reason) in [
  (price-1,volume,10_000_000,"BELOW_MIN_PRICE"),
  (one(200_000)+1,volume,0,"ABOVE_MAX_PRICE"),
  (price,volume-1,0,"BELOW_24H_VOLUME"),
  (price,volume,10_000_001,"SANITY_SPREAD_EXCEEDED"),
  (price,volume,-10_000_001,"SANITY_SPREAD_EXCEEDED"),
 ]{
  let result=c.inspect_market_filters_read_only(p,v,s).unwrap();
  assert_eq!(result.reason,reason);
  assert!(!result.passes_legacy_filters_locally);
  assert!(!result.may_send_or_cancel_order && !result.exchange_source_authenticated);
 }
 assert!(c.inspect_market_filters_read_only(0,volume,0).is_err());
 assert!(c.inspect_market_filters_read_only(price,volume,i128::MIN).is_err());
}
#[test]
fn p106_market_filter_config_invalid_or_subatomic_minimum_denies(){
 let minute=CONFIG.replace("  min_price: 0.03",
  "  min_price: 0.03000000000000000000001");
 let typed=NativeTradingConfig::parse(&minute).unwrap();
 assert_eq!(typed.policy.min_market_price_quote_e18,
  30_000_000_000_000_001);
 assert_eq!(typed.inspect_market_filters_read_only(
  30_000_000_000_000_000,one(150_000),0).unwrap().reason,
  "BELOW_MIN_PRICE");
 for change in [
  ("  min_price: 0.03","  min_price: 0.0"),
  ("  min_price: 0.03","  min_price: 2e-2"),
  ("  min_price: 0.03","  min_price: 200001"),
  ("  max_price: 200000","  max_price: 0"),
  ("  min_24h_volume_usdt: 150000","  min_24h_volume_usdt: 0"),
  ("  max_sanity_spread_bps: 10000","  max_sanity_spread_bps: 0"),
 ]{
  let text=CONFIG.replace(change.0,change.1);
  assert!(NativeTradingConfig::parse(&text).is_err(),
    "malformed or unsafe market filter must DENY: {}",change.1);
 }
}

#[test]
fn p106_execution_retry_legacy_attempt_counts_are_review_only(){
 let c=policy();
 let p=&c.policy;
 assert_eq!(p.spot_limit_ticks,0);
 assert_eq!(p.spot_retry_attempts,4);
 assert_eq!(p.spot_retry_delay_ms,250);
 assert_eq!(p.futures_retry_attempts,2);
 assert_eq!(p.futures_retry_delay_ms,500);
 let r=c.inspect_execution_retry_bounds_read_only().unwrap();
 assert_eq!(r.spot_limit_ticks,0);
 assert_eq!(r.spot_max_attempts,4);
 assert_eq!(r.spot_max_wait_before_last_ms,750);
 assert_eq!(r.futures_max_attempts,2);
 assert_eq!(r.futures_max_wait_before_last_ms,500);
 assert!(!r.known_exchange_outcome_authenticated);
 assert!(!r.may_resubmit_after_unknown_ack);
 assert!(!r.may_send_or_cancel_order);
 let once=CONFIG.replace("  spot_retry_n: 4","  spot_retry_n: 1")
  .replace("  fut_retry_n: 2","  fut_retry_n: 1");
 let one=NativeTradingConfig::parse(&once).unwrap()
  .inspect_execution_retry_bounds_read_only().unwrap();
 assert_eq!(one.spot_max_wait_before_last_ms,0);
 assert_eq!(one.futures_max_wait_before_last_ms,0);
 assert!(!one.may_send_or_cancel_order);
}
#[test]
fn p106_execution_retry_invalid_unbounded_or_tampered_settings_deny(){
 for (old,new) in [
  ("  spot_limit_ticks: 0","  spot_limit_ticks: 21"),
  ("  spot_retry_n: 4","  spot_retry_n: 0"),
  ("  spot_retry_n: 4","  spot_retry_n: 9"),
  ("  spot_retry_ms: 250","  spot_retry_ms: 0"),
  ("  spot_retry_ms: 250","  spot_retry_ms: 10001"),
  ("  fut_retry_n: 2","  fut_retry_n: 0"),
  ("  fut_retry_n: 2","  fut_retry_n: 9"),
  ("  fut_retry_ms: 500","  fut_retry_ms: 0"),
  ("  fut_retry_ms: 500","  fut_retry_ms: 10001"),
 ]{
  let changed=CONFIG.replace(old,new);
  assert_ne!(changed,CONFIG,"test fixture key missing: {old}");
  assert!(NativeTradingConfig::parse(&changed).is_err(),"unsafe retry setting {new}");
 }
 let huge=CONFIG.replace("  spot_retry_n: 4","  spot_retry_n: 8")
  .replace("  spot_retry_ms: 250","  spot_retry_ms: 5000");
 assert!(NativeTradingConfig::parse(&huge).is_err(),
  "seven sleeps of 5000ms must exceed bounded review budget");
 let mut tampered=policy();
 tampered.policy.spot_retry_attempts=0;
 assert!(tampered.inspect_execution_retry_bounds_read_only().is_err());
 tampered.policy.spot_retry_attempts=4;
 tampered.policy.real_execution_authorized=true;
 assert!(tampered.inspect_execution_retry_bounds_read_only().is_err());
}

#[test]
fn p106_execution_eight_original_fields_bound_to_bounded_review_schedule(){
 let baseline=policy().inspect_original_execution_timing_read_only().unwrap();
 assert_eq!(baseline.spot_entry_mode,"aggressive_limit");
 assert_eq!(baseline.futures_entry_mode,"market");
 assert_eq!(baseline.unwind_mode,"market");
 assert_eq!(baseline.spot_limit_ticks,0);
 assert_eq!(baseline.spot_retry_offsets_ms,vec![0,250,500,750]);
 assert_eq!(baseline.futures_retry_offsets_ms,vec![0,500]);
 assert!(!baseline.original_exchange_acknowledged);
 assert!(!baseline.may_retry_after_unknown_ack);
 assert!(!baseline.may_send_or_cancel_order);
 assert!(!baseline.installed_paper_source_attested);
 for (old,new) in [
   ("  spot_limit_ticks: 0","  spot_limit_ticks: 2"),
   ("  spot_retry_n: 4","  spot_retry_n: 3"),
   ("  spot_retry_ms: 250","  spot_retry_ms: 300"),
   ("  fut_retry_n: 2","  fut_retry_n: 3"),
   ("  fut_retry_ms: 500","  fut_retry_ms: 600"),
 ]{
  let changed=CONFIG.replacen(old,new,1);
  assert_ne!(changed,CONFIG);
  let cfg=NativeTradingConfig::parse(&changed).unwrap();
  let observed=cfg.inspect_original_execution_timing_read_only().unwrap();
  assert!(observed.spot_limit_ticks!=baseline.spot_limit_ticks
   ||observed.spot_retry_offsets_ms!=baseline.spot_retry_offsets_ms
   ||observed.futures_retry_offsets_ms!=baseline.futures_retry_offsets_ms,
   "no timing/ticks difference for {old}");
  assert!(!observed.may_retry_after_unknown_ack);
  assert!(!observed.may_send_or_cancel_order);
  assert!(!observed.installed_paper_source_attested);
 }
}
#[test]
fn p106_execution_wrong_original_modes_and_source_drift_fail_closed(){
 for (old,new) in [
   ("  spot_entry_mode: aggressive_limit","  spot_entry_mode: market"),
   ("  fut_entry_mode: market","  fut_entry_mode: limit"),
   ("  unwind_mode: market","  unwind_mode: limit"),
 ]{
  let changed=CONFIG.replacen(old,new,1);
  assert_ne!(changed,CONFIG);
  assert!(NativeTradingConfig::parse(&changed).is_err(),
    "unsupported execution mode must be rejected: {old}");
 }
 let mut cfg=policy();
 cfg.raw["execution"]["spot_retry_ms"]=serde_yaml::Value::from(300u64);
 assert_eq!(cfg.inspect_original_execution_timing_read_only(),
   Err(ConfigError::Policy));
 let mut cfg=policy();
 cfg.policy.spot_limit_ticks=7;
 assert_eq!(cfg.inspect_original_execution_timing_read_only(),
   Err(ConfigError::Policy));
 let mut cfg=policy();
 cfg.policy.real_execution_authorized=true;
 assert_eq!(cfg.inspect_original_execution_timing_read_only(),
   Err(ConfigError::Policy));
}
#[test]
fn p106_top_n_original_yaml_changes_ranked_local_candidate_set_not_ws_owner(){
 let short=CONFIG.replacen("  top_n_per_scan: 20","  top_n_per_scan: 2",1);
 assert_ne!(short,CONFIG);
 let original=NativeTradingConfig::parse(CONFIG).unwrap();
 let revised=NativeTradingConfig::parse(&short).unwrap();
 let candidates=[
   ("AAA",90_000),("BBB",88_000),("CCC",86_000),("DDD",84_000),
 ];
 let a=original.inspect_candidate_and_ws_selection_read_only(
   &candidates,&["DDD","AAA"],&["ZZZ"]).unwrap();
 let b=revised.inspect_candidate_and_ws_selection_read_only(
   &candidates,&["DDD","AAA"],&["ZZZ"]).unwrap();
 assert_eq!(a.top_candidate_bases,vec!["AAA","BBB","CCC","DDD"]);
 assert_eq!(b.top_candidate_bases,vec!["AAA","BBB"]);
 assert_eq!(a.ws_target_bases,b.ws_target_bases);
 assert_eq!(a.retained_open_position_bases,b.retained_open_position_bases);
 assert_eq!(a.retained_open_position_bases,vec!["ZZZ"]);
 for view in [a,b]{
  assert!(!view.exchange_source_authenticated);
  assert!(!view.may_update_live_ws_subscriptions&&!view.may_trade);
 }
}
#[test]
fn p106_scanner_top_n_and_ws_top_max_keep_open_owners_offline(){
 let config=CONFIG.replace("  top_n_per_scan: 20","  top_n_per_scan: 2")
  .replace("  top_max: 40","  top_max: 2");
 let x=NativeTradingConfig::parse(&config).unwrap();
 assert_eq!(x.policy.top_n_per_scan,2);
 assert_eq!(x.policy.websocket_top_max,2);
 assert!(x.policy.websocket_enabled);
 let result=x.inspect_candidate_and_ws_selection_read_only(
  &[("AAA",45_000),("BBB",70_000),("CCC",70_000),("DDD",1_000)],
  &["DDD","AAA","EEE"],&["ZZZ","AAA","ZZZ"]).unwrap();
 assert_eq!(result.top_candidate_bases,vec!["BBB","CCC"],
  "strategy sorts net spread descending with stable ties");
 assert_eq!(result.ws_target_bases,vec!["DDD","AAA","ZZZ"],
  "WS leaders cap is separate from candidates; owners survive cap");
 assert_eq!(result.retained_open_position_bases,vec!["ZZZ","AAA"]);
 assert!(!result.exchange_source_authenticated);
 assert!(!result.may_update_live_ws_subscriptions && !result.may_trade);
}
#[test]
fn p106_scanner_disabled_ws_keeps_review_owners_and_candidates(){
 let off=NativeTradingConfig::parse(&CONFIG.replace(
  "  enabled: true\n  quote: USDT","  enabled: false\n  quote: USDT")).unwrap();
 assert!(!off.policy.websocket_enabled);
 let view=off.inspect_candidate_and_ws_selection_read_only(
  &[("AAA",10),("BBB",20)],&["AAA","CCC"],&["CCC"]).unwrap();
 assert_eq!(view.top_candidate_bases,vec!["BBB","AAA"]);
 assert!(view.ws_target_bases.is_empty());
 assert_eq!(view.retained_open_position_bases,vec!["CCC"]);
 assert!(!view.may_update_live_ws_subscriptions && !view.may_trade);
}
#[test]
fn p106_scanner_sources_and_policy_fail_closed_on_invalid_shapes(){
 let x=policy();
 assert_eq!(x.policy.top_n_per_scan,20);
 assert_eq!(x.policy.websocket_top_max,40);
 for candidates in [
  vec![("AAA",1),("AAA",2)],
  vec![("AAA",20_000_001)],
  vec![("A-A",10)],
  vec![("aaa",10)],
 ]{
  assert!(x.inspect_candidate_and_ws_selection_read_only(
    &candidates,&["BBB"],&["AAA"]).is_err());
 }
 assert!(x.inspect_candidate_and_ws_selection_read_only(
  &[("AAA",100)],&["bad"],&["AAA"]).is_err());
 assert!(x.inspect_candidate_and_ws_selection_read_only(
  &[("AAA",100)],&["AAA"],&["BAD/USDT"]).is_err());
 assert!(x.inspect_candidate_and_ws_selection_read_only(
  &vec![("AAA",100);4097],&[],&[]).is_err());
 for (old,new) in [
  ("  top_n_per_scan: 20","  top_n_per_scan: 0"),
  ("  top_n_per_scan: 20","  top_n_per_scan: 1001"),
  ("  top_max: 40","  top_max: 0"),
  ("  top_max: 40","  top_max: 501"),
 ]{
  let altered=CONFIG.replace(old,new);
  assert_ne!(altered,CONFIG);
  assert!(NativeTradingConfig::parse(&altered).is_err(),"{new}");
 }
 let mut forged=policy();
 forged.policy.top_n_per_scan=0;
 assert!(forged.inspect_candidate_and_ws_selection_read_only(
  &[],&[],&[]).is_err());
 forged.policy.top_n_per_scan=20;
 forged.policy.real_execution_authorized=true;
 assert!(forged.inspect_candidate_and_ws_selection_read_only(
  &[],&[],&[]).is_err());
}
fn review_state_dir(tag:&str)->PathBuf{
 let n=std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
 std::env::temp_dir().join(format!("r32d6c8b1-{tag}-{}-{n}",std::process::id()))
}
fn new_economics(q:&str)->spotarb_native_market_core::fill_economics::PositionEconomicsState{
 use spotarb_native_market_core::fill_economics::{
  PositionEconomicsState,EntryEconomicsState,EntryTiming,LegFillAggregate
 };
 let qty=decimal_to_atoms(q).unwrap();
 PositionEconomicsState{entry:EntryEconomicsState{
  signal_gross_microbps:100_000_000,
  signal_expected_net_microbps:80_000_000,
  signal_spot_price_atoms:Some(decimal_to_atoms("100").unwrap()),
  signal_futures_price_atoms:Some(decimal_to_atoms("101").unwrap()),
  projected_cost_reserve_microbps:20_000_000,
  spot:LegFillAggregate::new("okx","BNT/USDT","buy",qty).unwrap(),
  futures:LegFillAggregate::new("binance","BNTUSDT","sell",qty).unwrap(),
  timing:EntryTiming::default(),
  post_leg1_projected_gross_microbps:None,
  post_leg1_projected_net_microbps:None,
  locked_actual_gross_microbps:None,
  locked_actual_net_microbps:None,
  signal_to_leg1_slippage_microbps:None,
  signal_to_hedge_decay_microbps:None,
  execution_cost_vs_signal_microbps:None,
 },exit:None}
}
fn review_config_path()->PathBuf{
 PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config/native_trading.yaml")
}
#[test]
fn r32d6c8b1_begin_native_entry_policy_binding_atomic_persist_reopen(){
 use spotarb_native_market_core::authoritative_state::{
  NativeStateStore,NativeAuthoritativeState,NativeEntryPhase
 };
 let dir=review_state_dir("bound");
 let store=NativeStateStore::open(dir.join("state.json")).unwrap();
 store.create(&NativeAuthoritativeState::empty_from_legacy("11".repeat(32),0)).unwrap();
 let plan=fixture("0.09");
 let (saved,permit)=store.begin_native_entry_with_config(
  1,plan,new_economics("0.09"),
  "r32d6c8b1-spot","sab00000000000000000000000000999",
  decimal_to_atoms("100").unwrap(),decimal_to_atoms("101").unwrap(),
  7,&review_config_path()).unwrap();
 assert_eq!(permit.generation(),saved.generation);
 assert_eq!(saved.generation,2);
 assert_eq!(saved.entry.as_ref().unwrap().phase,NativeEntryPhase::SpotPrepared);
 let binding=saved.entry.as_ref().unwrap().policy_binding.as_ref().unwrap();
 assert_eq!(binding.config_sha256,policy().sha256);
 assert_eq!(binding.safety_generation,7);
 assert_eq!(binding.user_quote_cap_e18,one(10));
 assert_eq!(binding.config_order_quote_e18,one(10));
 assert_eq!(binding.spot_notional_e18,one(9));
 assert_eq!(binding.futures_notional_e18,9_090_000_000_000_000_000);
 assert!(binding.total_reserved_quote_e18>binding.futures_notional_e18);
 assert!(binding.total_reserved_quote_e18<one(10));
 assert!(!binding.may_send_order&&!binding.may_promote_hedged);
 let reopened=NativeStateStore::open(dir.join("state.json")).unwrap();
 assert_eq!(reopened.load().unwrap().unwrap(),saved);
 assert_eq!(reopened.load_for_recovery().unwrap().unwrap()
  .entry.as_ref().unwrap().phase,NativeEntryPhase::RecoveryRequired);
 let bytes=std::fs::read(dir.join("state.json")).unwrap();
 let mut forged=saved.clone();
 forged.entry.as_mut().unwrap().policy_binding.as_mut().unwrap().config_order_quote_e18=one(20);
 assert!(forged.validate().is_err());
 let mut forged=saved.clone();
 forged.entry.as_mut().unwrap().policy_binding.as_mut().unwrap().spot_notional_e18=1;
 assert!(forged.validate().is_err());
 let mut forged=saved.clone();
 forged.entry.as_mut().unwrap().policy_binding.as_mut().unwrap().may_send_order=true;
 assert!(forged.validate().is_err());
 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),bytes);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn r32d6c8b1_rejects_100_usdt_plan_before_durable_intent(){
 use spotarb_native_market_core::authoritative_state::{NativeStateStore,NativeAuthoritativeState};
 let dir=review_state_dir("oversized");
 let store=NativeStateStore::open(dir.join("state.json")).unwrap();
 store.create(&NativeAuthoritativeState::empty_from_legacy("11".repeat(32),0)).unwrap();
 let original=std::fs::read(dir.join("state.json")).unwrap();
 let rejected=store.begin_native_entry_with_config(
  1,fixture("1"),new_economics("1"),
  "r32d6c8b1-spot","sab00000000000000000000000000999",
  decimal_to_atoms("100").unwrap(),decimal_to_atoms("101").unwrap(),
  7,&review_config_path());
 assert!(rejected.is_err());
 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),original);
 assert!(store.load().unwrap().unwrap().entry.is_none());
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn r32d6c8b1_denies_changed_missing_and_wrong_safety_config_before_state_change(){
 use spotarb_native_market_core::authoritative_state::{NativeStateStore,NativeAuthoritativeState};
 let dir=review_state_dir("config");
 std::fs::create_dir_all(&dir).unwrap();
 let p=dir.join("review.yaml");
 std::fs::write(&p,CONFIG).unwrap();
 let store=NativeStateStore::open(dir.join("state.json")).unwrap();
 store.create(&NativeAuthoritativeState::empty_from_legacy("11".repeat(32),0)).unwrap();
 let before=std::fs::read(dir.join("state.json")).unwrap();
 let valid=fixture("0.09");
 let begin=|file:&std::path::Path,gen:u64|{
  store.begin_native_entry_with_config(1,valid.clone(),new_economics("0.09"),
   "r32d6c8b1-spot","sab00000000000000000000000000999",
   decimal_to_atoms("100").unwrap(),decimal_to_atoms("101").unwrap(),gen,file)
 };
 assert!(begin(&p,8).is_err());
 let lower=CONFIG.replace("  futures_order_usdt: 10\n","  futures_order_usdt: 5\n");
 std::fs::write(&p,lower).unwrap();
 assert!(begin(&p,7).is_err());
 std::fs::remove_file(&p).unwrap();
 assert!(begin(&p,7).is_err());
 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),before);
 assert!(store.load().unwrap().unwrap().entry.is_none());
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn r32d6c8b1_fees_on_exact_signed_fill_ledger_cannot_exceed_bound_quote(){
 let cfg=policy();
 let plan=fixture("0.09");
 let binding=cfg.bind_entry(&plan,decimal_to_atoms("100").unwrap(),
  decimal_to_atoms("101").unwrap(),7).unwrap();
 assert_eq!(binding.verify_exact_spot_fills(&plan,&ledger("0.09","0.009")).unwrap(),
  9_009_000_000_000_000_000);
 assert!(binding.verify_exact_spot_fills(&plan,&ledger("0.09","1.1")).is_err());
 let mut counterfeit=binding.clone();
 counterfeit.total_reserved_quote_e18=1;
 assert!(counterfeit.validate_in_state(&plan).is_err());
 let mut counterfeit=binding.clone();
 counterfeit.config_sha256="a".repeat(63);
 assert!(counterfeit.validate_in_state(&plan).is_err());
 let mut counterfeit=binding.clone();
 counterfeit.fee_reserve_milli_bps=0;
 assert!(counterfeit.validate_in_state(&plan).is_err());
}


#[test]
fn r32d6c8b1_bound_entry_authorization_requires_same_hash_and_raw_bypass_denies(){
 use spotarb_native_market_core::{
  authoritative_state::{NativeStateStore,NativeAuthoritativeState},
  execution_adapter::{OrderIntent,Venue},
 };
 for variation in ["good","raw-bypass","changed-nonrisk-yaml","missing-yaml"]{
  let dir=review_state_dir(variation);
  std::fs::create_dir_all(&dir).unwrap();
  let conf=dir.join("trading.yaml");
  std::fs::write(&conf,CONFIG).unwrap();
  let store=NativeStateStore::open(dir.join("state.json")).unwrap();
  store.create(&NativeAuthoritativeState::empty_from_legacy("11".repeat(32),0)).unwrap();
  let (entry,permit)=store.begin_native_entry_with_config(
   1,fixture("0.09"),new_economics("0.09"),
   "r32d6c8b1-bound-spot","sab00000000000000000000000000998",
   decimal_to_atoms("100").unwrap(),decimal_to_atoms("101").unwrap(),7,&conf,
  ).unwrap();
  let order=OrderIntent{
   venue:Venue::OkxSpot,symbol:"BNT/USDT".into(),side:"buy".into(),
   amount:"0.09".into(),client_id:"sab00000000000000000000000000998".into(),
  };
  let bytes=std::fs::read(dir.join("state.json")).unwrap();
  let result=match variation{
   "good"=>store.authorize_entry_order_with_config(permit,&order,7,&conf),
   "raw-bypass"=>store.authorize_entry_order(permit,&order,7),
   "changed-nonrisk-yaml"=>{
    // Same amount, same 42bps and same safety caps; still wrong version.
    let changed=CONFIG.replace("  heartbeat_interval_sec: 900\n",
                               "  heartbeat_interval_sec: 901\n");
    assert_ne!(changed,CONFIG);
    std::fs::write(&conf,changed).unwrap();
    store.authorize_entry_order_with_config(permit,&order,7,&conf)
   },
   "missing-yaml"=>{
    std::fs::remove_file(&conf).unwrap();
    store.authorize_entry_order_with_config(permit,&order,7,&conf)
   },
   _=>unreachable!(),
  };
  if variation=="good"{
   let auth=result.unwrap();
   assert_eq!(auth.generation(),entry.generation);
   assert_eq!(auth.intent().client_id,order.client_id);
  }else{
   assert!(result.is_err(),"{variation} unexpectedly authorized");
  }
  assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),bytes);
  std::fs::remove_dir_all(dir).unwrap();
 }
}
#[test]
fn r32d6c8b1_bound_futures_prepare_requires_config_route_even_before_spot_truth(){
 use spotarb_native_market_core::authoritative_state::{
  NativeStateStore,NativeAuthoritativeState,NativeEntryPhase
 };
 let dir=review_state_dir("futurespolicy");
 std::fs::create_dir_all(&dir).unwrap();
 let conf=dir.join("trading.yaml");
 std::fs::write(&conf,CONFIG).unwrap();
 let store=NativeStateStore::open(dir.join("state.json")).unwrap();
 store.create(&NativeAuthoritativeState::empty_from_legacy("11".repeat(32),0)).unwrap();
 let (state,_permit)=store.begin_native_entry_with_config(
  1,fixture("0.09"),new_economics("0.09"),
  "r32d6c8b1-bound-spot","sab00000000000000000000000000998",
  decimal_to_atoms("100").unwrap(),decimal_to_atoms("101").unwrap(),7,&conf).unwrap();
 let bytes=std::fs::read(dir.join("state.json")).unwrap();
 assert!(store.prepare_futures_entry_hedge(state.generation,7,"future",
  "sab00000000000000000000000000997").is_err());
 assert!(store.prepare_futures_entry_hedge_with_config(state.generation,7,"future",
  "sab00000000000000000000000000997",&conf).is_err()); // Spot is NOT filled.
 std::fs::write(&conf,CONFIG.replace("  heartbeat_interval_sec: 900\n",
   "  heartbeat_interval_sec: 901\n")).unwrap();
 assert!(store.prepare_futures_entry_hedge_with_config(state.generation,7,"future",
  "sab00000000000000000000000000997",&conf).is_err());
 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),bytes);
 assert_eq!(store.load().unwrap().unwrap().entry.unwrap().phase,NativeEntryPhase::SpotPrepared);
 std::fs::remove_dir_all(dir).unwrap();
}


// R32D6C8B2B2B3 - configured opening ancestry, still no two-leg/close authority.
fn create_origin_case(tag:&str)->(
 PathBuf,
 spotarb_native_market_core::authoritative_state::NativeStateStore,
 spotarb_native_market_core::authoritative_state::NativeAuthoritativeState,
){
 use spotarb_native_market_core::authoritative_state::{NativeStateStore,NativeAuthoritativeState};
 let dir=review_state_dir(tag);
 let store=NativeStateStore::open(dir.join("state.json")).unwrap();
 store.create(&NativeAuthoritativeState::empty_from_legacy("ab".repeat(32),0)).unwrap();
 let (state,_permit)=store.begin_native_entry_with_config(
    1,fixture("0.09"),new_economics("0.09"),
    "opening-spot-intent-1","opening-spot-client-1",
    decimal_to_atoms("100").unwrap(),decimal_to_atoms("101").unwrap(),
    7,&review_config_path()).unwrap();
 (dir,store,state)
}
#[test]
fn b3_configured_opening_origin_is_durable_and_non_authorizing(){
 use spotarb_native_market_core::native_entry_origin::read_persisted_origin;
 let (dir,store,state)=create_origin_case("b3-persist");
 let a=read_persisted_origin(&store,state.generation).unwrap();
 assert_eq!(a.initial_generation(),2);
 assert_eq!(a.policy_sha256(),policy().sha256);
 assert_eq!(a.spot_client_id(),"opening-spot-client-1");
 assert_eq!(a.position_key().len(),64);
 assert!(!a.opening_two_legs_proven()&&!a.original_close_lineage_proven());
 assert!(!a.may_send_order()&&!a.may_verify_closed());
 let again=read_persisted_origin(&store,2).unwrap();
 assert_eq!(a,again);
 assert_eq!(state.entry.as_ref().unwrap().origin.as_ref(),Some(&a));
 assert!(state.active.is_none());
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn b3_original_policy_plan_intent_and_anchor_mutation_all_fail_closed(){
 use spotarb_native_market_core::native_entry_origin::read_persisted_origin;
 let (dir,store,state)=create_origin_case("b3-tamper");
 let base=std::fs::read(dir.join("state.json")).unwrap();
 for case in ["config_sha","plan_qty","plan_safety","original_client",
              "original_intent","original_ledger","erase_origin","rebind_anchor"]{
    let mut edited=state.clone();
    let entry=edited.entry.as_mut().unwrap();
    match case {
       "config_sha"=>entry.policy_binding.as_mut().unwrap().config_sha256="00".repeat(32),
       "plan_qty"=>entry.plan.planned_qty_atoms_e18+=1,
       "plan_safety"=>entry.plan.safety_generation=8,
       "original_client"=>entry.intents[0].client_order_id=Some("foreign-client".into()),
       "original_intent"=>entry.intents[0].id="foreign-intent".into(),
       "original_ledger"=>entry.economics.entry.spot.orders.get_mut("opening-spot-intent-1")
          .unwrap().client_order_id="foreign-client".into(),
       "erase_origin"=>entry.origin=None,
       "rebind_anchor"=>{
          let b=entry.policy_binding.as_ref().unwrap();
          let new=spotarb_native_market_core::native_entry_origin::NativeEntryOriginAnchor::
             seal_configured_prepared(2,&entry.plan,b,"foreign-intent","foreign-client").unwrap();
          entry.origin=Some(new);
       },
       _=>unreachable!(),
    }
    // Recomputed state-envelope hash cannot grant a conflicting anchored
    // identity through trusted owner replace API, regardless of payload checksum.
    edited.generation=3;
    assert!(store.replace(2,&edited).is_err(),"case {case} accepted");
    assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),base);
 }
 assert_eq!(read_persisted_origin(&store,2).unwrap().policy_sha256(),policy().sha256);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn b3_owner_transaction_cannot_strip_or_regraft_original(){
 use spotarb_native_market_core::native_entry_origin::read_persisted_origin;
 let (dir,store,state)=create_origin_case("b3-owner");
 let original=read_persisted_origin(&store,state.generation).unwrap();
 let raw=std::fs::read(dir.join("state.json")).unwrap();
 let denied=store.transact(2,|s|{
   s.entry.as_mut().unwrap().origin=None;
   Ok(())
 });
 assert!(denied.is_err());
 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),raw);
 let success=store.transact(2,|s|{
   s.legacy_closed_positions_ignored+=1;
   Ok(())
 }).unwrap();
 assert_eq!(success.0.generation,3);
 assert_eq!(read_persisted_origin(&store,3).unwrap(),original);
 assert!(store.transact(2,|_|Ok(())).is_err());
 assert_eq!(store.load().unwrap().unwrap().generation,3);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn b3_concurrent_authoritative_owner_cas_preserves_origin(){
 use spotarb_native_market_core::native_entry_origin::read_persisted_origin;
 let (dir,store,_)=create_origin_case("b3-threads");
 std::thread::scope(|scope|{
    let t1=scope.spawn(||store.transact(2,|s|{s.legacy_closed_positions_ignored+=1;Ok(())}));
    let t2=scope.spawn(||store.transact(2,|s|{s.legacy_closed_positions_ignored+=2;Ok(())}));
    let a=t1.join().unwrap();let b=t2.join().unwrap();
    assert_eq!(usize::from(a.is_ok())+usize::from(b.is_ok()),1);
 });
 assert_eq!(store.load().unwrap().unwrap().generation,3);
 assert_eq!(read_persisted_origin(&store,3).unwrap().policy_sha256(),policy().sha256);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn b3_legacy_and_unbound_opening_cannot_forge_configured_origin(){
 use spotarb_native_market_core::authoritative_state::{NativeStateStore,NativeAuthoritativeState};
 use spotarb_native_market_core::native_entry_origin::read_persisted_origin;
 let dir=review_state_dir("b3-unbound");
 let store=NativeStateStore::open(dir.join("state.json")).unwrap();
 store.create(&NativeAuthoritativeState::empty_from_legacy("ab".repeat(32),0)).unwrap();
 assert!(read_persisted_origin(&store,1).is_err());
 let (s,_)=store.begin_native_entry(1,fixture("0.09"),new_economics("0.09"),
   "opening-spot-intent","opening-spot-client").unwrap();
 assert!(read_persisted_origin(&store,s.generation).is_err());
 assert!(s.entry.unwrap().origin.is_none());
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn b3_origin_alone_is_not_close_candidate_or_native_active_permit(){
 use spotarb_native_market_core::{
   native_entry_origin::read_persisted_origin,
   native_close_window::require_persisted_native_close_ancestry,
   native_close_readonly::{DualCloseCandidate,CloseOrderIdentity},
 };
 let (dir,store,state)=create_origin_case("b3-no-close");
 let original=read_persisted_origin(&store,state.generation).unwrap();
 let fake=DualCloseCandidate{
   position_identity:original.position_key().to_owned(),base:"BNT".into(),
   created_at_ms:1_780_000_001_000,
   expected_qty_e12:decimal_to_atoms("0.09").unwrap(),
   spot:CloseOrderIdentity{symbol:"BNT/USDT".into(),
      client_id:"any-close-spot".into(),exchange_order_id:"123".into()},
   futures:CloseOrderIdentity{symbol:"BNTUSDT".into(),
      client_id:"any-close-fut".into(),exchange_order_id:"456".into()},
 };
 assert!(require_persisted_native_close_ancestry(&store,2,&fake).is_err());
 assert!(state.active.is_none());
 assert!(!original.opening_two_legs_proven());
 assert!(!original.original_close_lineage_proven());
 assert!(!original.may_send_order());
 std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn b3_original_ids_change_position_key_even_with_identical_policy(){
 use spotarb_native_market_core::native_entry_origin::read_persisted_origin;
 use spotarb_native_market_core::authoritative_state::{NativeStateStore,NativeAuthoritativeState};
 let (dir_a,a,_)=create_origin_case("b3-key-a");
 let origin_a=read_persisted_origin(&a,2).unwrap();
 let dir_b=review_state_dir("b3-key-b");
 let b=NativeStateStore::open(dir_b.join("state.json")).unwrap();
 b.create(&NativeAuthoritativeState::empty_from_legacy("ab".repeat(32),0)).unwrap();
 let (s,_)=b.begin_native_entry_with_config(
     1,fixture("0.09"),new_economics("0.09"),
     "opening-spot-intent-1","opening-spot-client-DIFFERENT",
     decimal_to_atoms("100").unwrap(),decimal_to_atoms("101").unwrap(),
     7,&review_config_path()).unwrap();
 assert_eq!(s.generation,2);
 let origin_b=read_persisted_origin(&b,2).unwrap();
 assert_eq!(origin_a.policy_sha256(),origin_b.policy_sha256());
 assert_ne!(origin_a.position_key(),origin_b.position_key());
 let mut transplanted=b.load().unwrap().unwrap();
 transplanted.entry.as_mut().unwrap().origin=Some(origin_a);
 transplanted.generation=3;
 assert!(b.replace(2,&transplanted).is_err());
 assert_eq!(b.load().unwrap().unwrap().generation,2);
 std::fs::remove_dir_all(dir_a).unwrap();
 std::fs::remove_dir_all(dir_b).unwrap();
}
#[test]
fn b3_raw_failpoint_state_writer_cannot_erase_or_replace_sealed_origin(){
 use spotarb_native_market_core::authoritative_state::StatePersistFailpoint;
 let (dir,store,s)=create_origin_case("b3-failpoint");
 let bytes=std::fs::read(dir.join("state.json")).unwrap();
 let mut modified=s.clone();
 modified.entry.as_mut().unwrap().origin=None;
 for fp in [StatePersistFailpoint::None,
            StatePersistFailpoint::BeforeWrite,
            StatePersistFailpoint::AfterFileSync]{
    assert!(store.persist_with_failpoint(&modified,fp).is_err());
    assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),bytes);
 }
 assert!(store.persist_with_failpoint(&s,StatePersistFailpoint::BeforeWrite).is_err());
 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),bytes);
 std::fs::remove_dir_all(dir).unwrap();
}


// F2G-only numeric source mode used BEFORE any Native proof is sealed.
thread_local!{
 static F2G_NUMERIC_SOURCE:std::cell::Cell<bool>=const{std::cell::Cell::new(false)};
}
struct F2GNumericFixtureGuard;
impl F2GNumericFixtureGuard{
 fn new()->Self{
    F2G_NUMERIC_SOURCE.with(|c|assert!(!c.replace(true)));
    Self
 }
}
impl Drop for F2GNumericFixtureGuard{
 fn drop(&mut self){F2G_NUMERIC_SOURCE.with(|c|c.set(false))}
}
fn f2g_source_id(x:&str)->&str{
 F2G_NUMERIC_SOURCE.with(|c|if c.get(){match x{
   "S1"=>"9001","S10"=>"9002",other=>other
 }}else{x})
}
fn f2g_numericize_json(v:&mut serde_json::Value){
 if !F2G_NUMERIC_SOURCE.with(|c|c.get()){return}
 match v{
  serde_json::Value::String(s)=>{
   if s=="S1"{*s="9001".into()}else if s=="S10"{*s="9002".into()}
  },
  serde_json::Value::Array(xs)=>for x in xs{f2g_numericize_json(x)},
  serde_json::Value::Object(map)=>for (_,x) in map.iter_mut(){f2g_numericize_json(x)},
  _=>{},
 }
}
// R32D6C8B2B2B4A. All fixtures synthetic: no real signed GET, no order
// authority. C2 source fields model an already signed/mock-verified first leg.
fn b4a_seed_review_c2(tag:&str)->(
 PathBuf,
 spotarb_native_market_core::authoritative_state::NativeStateStore,
){
 use spotarb_native_market_core::{
   native_spot_prehedge::NativeSpotPreHedgeEvidence,
   authoritative_state::NativeEntryPhase,
   execution::IntentState,
 };
 let (dir,store,_) = create_origin_case(tag);
 let q=decimal_to_atoms("0.09").unwrap();
 let price=decimal_to_atoms("100").unwrap();
 store.transact(2,|s|{
   let e=s.entry.as_mut().unwrap();
   e.phase=NativeEntryPhase::SpotExposed;
   e.spot_filled_atoms_e18=q*1_000_000;
   e.intents[0].state=IntentState::Acknowledged;
   let ledger=e.economics.entry.spot.order_mut("opening-spot-intent-1").unwrap();
   ledger.apply_cumulative_truth(q,Some(price),true,Some("filled"),
     Some("201"),Some(1_780_000_000_100),DEFAULT_AVG_TOLERANCE_ATOMS).unwrap();
   ledger.apply_fill(FillEvent{
     fill_id:f2g_source_id("S1").into(),price_atoms:price,qty_atoms:q,
     fee_cost_quote_atoms:Some(decimal_to_atoms("0.009").unwrap() as i128),
     fee_currency:Some("USDT".into()),trade_time_ms:Some(1_780_000_000_100)
   },DEFAULT_AVG_TOLERANCE_ATOMS).unwrap();
   assert!(ledger.fee_evidence_complete);
   let first=NativeSpotPreHedgeEvidence{
     source_generation:2,observed_at_ms:1_780_000_001_000,
     safety_generation:7,spot_intent_id:"opening-spot-intent-1".into(),
     spot_client_id:"opening-spot-client-1".into(),spot_order_id:"201".into(),
     exact_ledger:ledger.clone(),signed_base_fee_atoms_e18:0,
     observed_base_cash_atoms_e18:q*1_000_000,
     balance_updated_at_ms:1_780_000_000_900,
     fill_bill_ids:vec!["301".into()],filled_pages:2,
     cash_bracket_matched:true,exchange_bill_history_proven:false,
     exchange_global_completeness_proven:false,may_send_order:false,
   };
   e.spot_prehedge_evidence=Some(first);
   Ok(())
 }).unwrap();
 (dir,store)
}
#[test]
fn b4a_seals_exact_first_spot_fills_to_immutable_origin_after_restart(){
 use spotarb_native_market_core::native_origin_spot_link::OriginSpotLinkWrite;
 let (dir,store)=b4a_seed_review_c2("b4a-roundtrip");
 assert_eq!(store.load().unwrap().unwrap().generation,3);
 let (saved,result)=store.record_origin_spot_link(3).unwrap();
 assert_eq!(result,OriginSpotLinkWrite::Recorded);
 assert_eq!(saved.generation,4);
 let e=saved.entry.unwrap();
 let link=e.origin_spot_link.unwrap();
 assert_eq!(link.position_key(),e.origin.as_ref().unwrap().position_key());
 assert_eq!(link.policy_sha256(),policy().sha256);
 assert_eq!(link.opening_order_id(),"201");
 assert_eq!(link.trade_ids(),&["S1".to_string()]);
 assert_eq!(link.filled_qty_atoms_e18(),decimal_to_atoms("0.09").unwrap()*1_000_000);
 assert_eq!(link.source_generation(),2);
 assert!(!link.may_send_order()&&!link.may_promote_hedged()&&!link.may_verify_closed());
 assert!(!link.proves_both_opening_legs()&&!link.proves_account_wide_history());
 let reopened=spotarb_native_market_core::authoritative_state::NativeStateStore::open(
   dir.join("state.json")).unwrap();
 assert_eq!(reopened.load().unwrap().unwrap().entry.as_ref()
   .unwrap().origin_spot_link.as_ref().unwrap(),&link);
 let (_,again)=reopened.record_origin_spot_link(3).unwrap();
 assert_eq!(again,OriginSpotLinkWrite::Idempotent);
 assert_eq!(reopened.load().unwrap().unwrap().generation,4);
 assert!(reopened.load().unwrap().unwrap().active.is_none());
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn b4a_rejects_absent_source_or_wrong_generation_before_native_write(){
 let (dir,store,_)=create_origin_case("b4a-no-source");
 let bytes=std::fs::read(dir.join("state.json")).unwrap();
 assert!(store.record_origin_spot_link(2).is_err());
 assert!(store.record_origin_spot_link(1).is_err());
 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),bytes);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn b4a_rejects_tampered_spot_order_fee_price_trade_and_bound_policy(){
 let (dir,store)=b4a_seed_review_c2("b4a-tamper");
 let (sealed,_)=store.record_origin_spot_link(3).unwrap();
 let data=std::fs::read(dir.join("state.json")).unwrap();
 for case in ["original_client","wrong_order","wrong_fee","wrong_price",
              "wrong_tradeid","missing_trade_time","wrong_bill","gross_over_cap",
              "changed_binding","origin_graft","remove_link","release_override"]{
    let mut modified=sealed.clone();
    let e=modified.entry.as_mut().unwrap();
    match case{
      "original_client"=>e.spot_prehedge_evidence.as_mut().unwrap().spot_client_id="fake".into(),
      "wrong_order"=>e.spot_prehedge_evidence.as_mut().unwrap().spot_order_id="999".into(),
      "wrong_fee"=>e.spot_prehedge_evidence.as_mut().unwrap().exact_ledger.fee_cost_quote_atoms+=1,
      "wrong_price"=>e.spot_prehedge_evidence.as_mut().unwrap().exact_ledger.cumulative_notional_atoms2+=1,
      "wrong_tradeid"=>{
        let l=&mut e.spot_prehedge_evidence.as_mut().unwrap().exact_ledger;
        let mut fill=l.fills.remove("S1").unwrap();fill.fill_id="S2".into();
        l.fills.insert("S2".into(),fill);
      },
      "missing_trade_time"=>e.spot_prehedge_evidence.as_mut().unwrap().exact_ledger.fills
         .get_mut("S1").unwrap().trade_time_ms=None,
      "wrong_bill"=>e.spot_prehedge_evidence.as_mut().unwrap().fill_bill_ids[0]="999".into(),
      "gross_over_cap"=>e.spot_prehedge_evidence.as_mut().unwrap().exact_ledger.requested_qty_atoms+=1,
      "changed_binding"=>e.policy_binding.as_mut().unwrap().config_sha256="12".repeat(32),
      "origin_graft"=>e.origin=None,
      "remove_link"=>e.origin_spot_link=None,
      "release_override"=>{
          // A private seal must not be able to grant authority after
          // roundtrip JSON deserialization via trusted replace.
          let raw=serde_json::to_string(&e.origin_spot_link).unwrap();
          let modified_raw=raw.replace("\"may_send_order\":false","\"may_send_order\":true");
          e.origin_spot_link=serde_json::from_str(&modified_raw).unwrap();
      },
      _=>unreachable!(),
    }
    modified.generation=5;
    assert!(store.replace(4,&modified).is_err(),"forgery case {case} accepted");
    assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),data);
 }
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn b4a_owner_cas_rejects_stale_replay_and_writer_conflict(){
 use spotarb_native_market_core::native_origin_spot_link::OriginSpotLinkWrite;
 let (dir,store)=b4a_seed_review_c2("b4a-concurrent");
 let x1=store.clone();let x2=store.clone();
 let a=std::thread::spawn(move||x1.record_origin_spot_link(3));
 let b=std::thread::spawn(move||x2.record_origin_spot_link(3));
 let r=[a.join().unwrap(),b.join().unwrap()];
 assert_eq!(r.iter().filter(|x|matches!(x,Ok((_,OriginSpotLinkWrite::Recorded)))).count(),1);
 assert_eq!(r.iter().filter(|x|matches!(x,Ok((_,OriginSpotLinkWrite::Idempotent)))).count()
            +r.iter().filter(|x|x.is_err()).count(),1);
 assert_eq!(store.load().unwrap().unwrap().generation,4);
 assert!(store.record_origin_spot_link(2).is_err());
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn b4a_failure_before_disk_atomic_rename_keeps_old_state_then_retry(){
 use spotarb_native_market_core::native_origin_spot_link::OriginSpotLinkWrite;
 let (dir,store)=b4a_seed_review_c2("b4a-io");
 let old=std::fs::read(dir.join("state.json")).unwrap();
 let blocker=dir.join("state.tmp");
 std::fs::create_dir(&blocker).unwrap();
 assert!(store.record_origin_spot_link(3).is_err());
 assert_eq!(old,std::fs::read(dir.join("state.json")).unwrap());
 std::fs::remove_dir(&blocker).unwrap();
 let (_,status)=store.record_origin_spot_link(3).unwrap();
 assert_eq!(status,OriginSpotLinkWrite::Recorded);
 assert_eq!(store.load().unwrap().unwrap().generation,4);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn b4a_link_never_releases_hedged_or_proves_binance_opening(){
 use spotarb_native_market_core::native_close_window::require_persisted_native_close_ancestry;
 use spotarb_native_market_core::native_close_readonly::{DualCloseCandidate,CloseOrderIdentity};
 let (dir,store)=b4a_seed_review_c2("b4a-no-promotion");
 let (state,_)=store.record_origin_spot_link(3).unwrap();
 let e=state.entry.as_ref().unwrap();
 assert!(e.futures_entry_window_evidence.is_none());
 assert!(e.recovery_checkpoint.is_none());
 assert!(e.prospective_joint_evidence.is_none());
 let proof=e.origin_spot_link.as_ref().unwrap();
 assert!(!proof.proves_both_opening_legs()&&!proof.may_promote_hedged());
 let fake=DualCloseCandidate{position_identity:proof.position_key().to_owned(),
    base:"BNT".into(),created_at_ms:1_780_000_001_000,
    expected_qty_e12:decimal_to_atoms("0.09").unwrap(),
    spot:CloseOrderIdentity{symbol:"BNT/USDT".into(),client_id:"fake-spot-close".into(),exchange_order_id:"400".into()},
    futures:CloseOrderIdentity{symbol:"BNTUSDT".into(),client_id:"fake-futures-close".into(),exchange_order_id:"500".into()},
 };
 assert!(require_persisted_native_close_ancestry(&store,state.generation,&fake).is_err());
 assert!(state.active.is_none());
 std::fs::remove_dir_all(dir).unwrap();
}


// B4B exact-structure fixture. These are SYNTHETIC pre-audited receipts,
// NOT actual signed GET network captures. Prior C2/C3/C5/C6 mock contracts
// test transport/pagination separately; this tests owner-bound ancestry.
fn b4b_seed_synthetic_two_venues(tag:&str)->(
 PathBuf,spotarb_native_market_core::authoritative_state::NativeStateStore,
){
 b4b_seed_synthetic_two_venues_sized(tag,"0.04")
}
fn b4b_seed_synthetic_two_venues_sized(tag:&str,hedge_s:&str)->(
 PathBuf,spotarb_native_market_core::authoritative_state::NativeStateStore,
){
 b4b_seed_synthetic_two_venues_sized_with_ids(tag,hedge_s,["F1","F2"])
}
fn b4b_seed_synthetic_two_venues_sized_with_ids(
 tag:&str,hedge_s:&str,ids:[&str;2],
)->(PathBuf,spotarb_native_market_core::authoritative_state::NativeStateStore){
 use spotarb_native_market_core::{
   authoritative_state::{
     NativeEntryPhase,NativeEntryRecoveryCheckpoint,NativeEntryRecoveryQuality,NativeEntryLeg,
   },
   native_spot_prehedge_bills::NativeSpotBillsEvidence,
   native_futures_entry_audit::NativeFuturesEntryWindowEvidence,
   native_prospective_joint::NativeProspectiveJointEvidence,
   execution::{Intent,IntentState},
 };
 let (dir,store)=b4a_seed_review_c2(tag);
 let (s,_)=store.record_origin_spot_link(3).unwrap();
 assert_eq!(s.generation,4);
 let spot=decimal_to_atoms("0.09").unwrap();
 let hedge=decimal_to_atoms(hedge_s).unwrap();
 let short=hedge*1_000_000;
 let cash=spot*1_000_000;
 store.transact(4,|state|{
    let e=state.entry.as_mut().unwrap();
    e.spot_prehedge_bills_evidence=Some(NativeSpotBillsEvidence{
       source_generation:4,spot_first_source_generation:2,
       observed_at_ms:1_780_000_002_000,safety_generation:7,
       spot_intent_id:"opening-spot-intent-1".into(),
       spot_client_id:"opening-spot-client-1".into(),spot_order_id:"201".into(),
       original_base_cash_atoms_e18:0,observed_base_cash_atoms_e18:cash,
       observed_base_cash_updated_at_ms:1_780_000_000_900,
       aggregate_base_delta_atoms_e18:cash as i128,
       aggregate_signed_base_fee_atoms_e18:0,
       verified_bill_ids:vec!["301".into()],billed_trade_ids:vec![f2g_source_id("S1").into()],
       account_bill_pages:2,exact_bills_for_first_leg_matched:true,
       base_cash_ancestry_to_original_baseline_matched:true,
       source_balance_bracket_matched:true,
       account_history_globally_complete:false,cross_venue_atomicity_proven:false,
       may_send_order:false,
    });
    Ok(())
 }).unwrap();
 store.transact(5,|state|{
    let e=state.entry.as_mut().unwrap();
    e.phase=NativeEntryPhase::RecoveryRequired;
    e.intents.push(Intent{id:"futures-intent-1".into(),
       client_order_id:Some("futures-client-1".into()),state:IntentState::Unknown});
    e.economics.entry.futures.add_order("futures-intent-1","futures-client-1",spot).unwrap();
    let mut ledger=LegFillLedger::new("binance","BNTUSDT","sell",
       "futures-intent-1","futures-client-1",spot).unwrap();
    ledger.apply_cumulative_truth(hedge,Some(decimal_to_atoms(
       if hedge_s=="0.09"{"100"}else{"101"}).unwrap()),
       true,Some("canceled"),Some("601"),Some(1_780_000_003_050),
       DEFAULT_AVG_TOLERANCE_ATOMS).unwrap();
    for (id,time) in [(ids[0],1_780_000_003_050u64),(ids[1],1_780_000_003_100)]{
       ledger.apply_fill(FillEvent{
           fill_id:id.into(),price_atoms:decimal_to_atoms(
           if hedge_s=="0.09"{"100"}else{"101"}).unwrap(),
           qty_atoms:hedge/2,
           fee_cost_quote_atoms:Some(decimal_to_atoms("0.002").unwrap() as i128),
           fee_currency:Some("USDT".into()),trade_time_ms:Some(time),
       },DEFAULT_AVG_TOLERANCE_ATOMS).unwrap();
    }
    assert!(ledger.fee_evidence_complete);
    e.recovery_checkpoint=Some(NativeEntryRecoveryCheckpoint{
       leg:NativeEntryLeg::Futures,intent_id:"futures-intent-1".into(),
       client_id:"futures-client-1".into(),order_id:"601".into(),
       ledger:ledger.clone(),quality:NativeEntryRecoveryQuality::FuturesPriorSpotUnverified,
       observed_spot_cash_atoms_e18:None,signed_base_fee_atoms_e18:None,
    });
    e.futures_entry_window_evidence=Some(NativeFuturesEntryWindowEvidence{
       source_generation:5,captured_at_ms:1_780_000_003_500,
       safety_generation:7,
       futures_intent_id:"futures-intent-1".into(),
       futures_client_id:"futures-client-1".into(),futures_order_id:"601".into(),
       trade_ids:vec![ids[0].into(),ids[1].into()],fill_qty_atoms_e18:short,
       signed_quote_fee_cost_atoms_e12:decimal_to_atoms("0.004").unwrap() as i128,
       observed_short_atoms_e18:short,observed_short_updated_at_ms:1_780_000_003_300,
       observed_wallet_usdt_atoms_e18:one(100),
       observed_wallet_updated_at_ms:1_780_000_003_300,
       income_pages:2,trade_windows:1,account_read_repetitions:2,
       exact_fills_commissions_and_symbol_history_matched:true,
       income_position_wallet_bracket_matched:true,
       exchange_global_history_complete:false,cross_venue_atomicity_proven:false,
       may_send_order:false,may_promote_hedged:false,
    });
    Ok(())
 }).unwrap();
 store.transact(6,|state|{
    let e=state.entry.as_mut().unwrap();
    e.prospective_joint_evidence=Some(NativeProspectiveJointEvidence{
       source_generation:6,captured_at_ms:1_780_000_004_000,
       safety_generation:7,
       spot_first_source_generation:2,spot_bills_source_generation:4,
       futures_window_source_generation:5,
       spot_intent_id:"opening-spot-intent-1".into(),
       spot_client_id:"opening-spot-client-1".into(),spot_order_id:"201".into(),
       spot_trade_ids:vec![f2g_source_id("S1").into()],spot_bill_ids:vec!["301".into()],
       spot_gross_atoms_e18:cash,spot_available_net_atoms_e18:cash,
       spot_fee_quote_atoms_e12:decimal_to_atoms("0.009").unwrap() as i128,
       spot_cash_e18:cash,spot_cash_updated_at_ms:1_780_000_000_900,
       futures_intent_id:"futures-intent-1".into(),
       futures_client_id:"futures-client-1".into(),futures_order_id:"601".into(),
       futures_trade_ids:vec![ids[0].into(),ids[1].into()],futures_short_atoms_e18:short,
       futures_fee_quote_atoms_e12:decimal_to_atoms("0.004").unwrap() as i128,
       futures_wallet_usdt_e18:one(100),
       futures_position_updated_at_ms:1_780_000_003_300,
       futures_wallet_updated_at_ms:1_780_000_003_300,
       exposed_spot_unhedged_atoms_e18:cash-short,
       okx_balance_bracket_matched:true,
       binance_income_and_position_bracket_matched:true,
       foreign_symbol_trades_absent_in_bounded_window:true,
       cross_venue_exchange_snapshot_atomicity_proven:false,
       exchange_global_history_complete:false,may_send_order:false,
       may_promote_hedged:false,
    });
    Ok(())
 }).unwrap();
 assert_eq!(store.load().unwrap().unwrap().generation,7);
 (dir,store)
}
#[test]
fn b4b_original_two_venue_receipt_under_10_usdt_preserves_partial_exposure_and_restart(){
 use spotarb_native_market_core::native_origin_dual_open::OriginDualOpeningWrite;
 let (dir,store)=b4b_seed_synthetic_two_venues("b4b-ok");
 let (s,r)=store.record_origin_dual_opening_link(7).unwrap();
 assert_eq!(r,OriginDualOpeningWrite::Recorded);
 assert_eq!(s.generation,8);
 assert!(s.active.is_none());
 assert_eq!(s.entry.as_ref().unwrap().phase,
     spotarb_native_market_core::authoritative_state::NativeEntryPhase::RecoveryRequired);
 let e=s.entry.as_ref().unwrap();
 let proof=e.origin_dual_opening_link.as_ref().unwrap();
 assert_eq!(proof.position_key(),e.origin.as_ref().unwrap().position_key());
 assert_eq!(proof.futures_order_id(),"601");
 assert_eq!(proof.futures_trade_ids(),&["F1".to_owned(),"F2".to_owned()]);
 assert_eq!(proof.exposure_remaining_e18(),decimal_to_atoms("0.05").unwrap()*1_000_000);
 assert_eq!(proof.futures_short_e18(),decimal_to_atoms("0.04").unwrap()*1_000_000);
 assert!(!proof.may_send_order()&&!proof.may_promote_hedged()&&!proof.may_verify_closed());
 assert!(!proof.exchange_global_history_complete()&&!proof.all_venue_snapshots_atomic());
 assert!(!proof.closing_order_ancestry_proven());
 let reopened=spotarb_native_market_core::authoritative_state::NativeStateStore::open(
     dir.join("state.json")).unwrap();
 assert_eq!(reopened.load().unwrap().unwrap(),s);
 let (again,r)=reopened.record_origin_dual_opening_link(8).unwrap();
 assert_eq!(r,OriginDualOpeningWrite::Idempotent);
 assert_eq!(again,s);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn b4b_absent_signed_sources_stale_generation_and_legacy_identity_are_denied(){
 let (d,store)=b4a_seed_review_c2("b4b-no-binance");
 let before=std::fs::read(d.join("state.json")).unwrap();
 assert!(store.record_origin_dual_opening_link(3).is_err());
 assert!(store.record_origin_spot_link(2).is_err());
 assert_eq!(before,std::fs::read(d.join("state.json")).unwrap());
 std::fs::remove_dir_all(d).unwrap();
 let (d,store)=b4b_seed_synthetic_two_venues("b4b-stale");
 let before=std::fs::read(d.join("state.json")).unwrap();
 assert!(store.record_origin_dual_opening_link(6).is_err());
 assert_eq!(before,std::fs::read(d.join("state.json")).unwrap());
 std::fs::remove_dir_all(d).unwrap();
}

#[test]
fn b4b_dual_opening_tamper_is_denied_by_owner_cas_and_on_reopen(){
 let (dir,store)=b4b_seed_synthetic_two_venues("b4b-forgery");
 let (saved,_)=store.record_origin_dual_opening_link(7).unwrap();
 let raw=std::fs::read(dir.join("state.json")).unwrap();
 for test in ["remove_link","wrong_bill","wrong_trade","foreign_futures_id",
              "wrong_futures_order","wrong_fee","position_residual",
              "missing_c3","missing_c5","missing_c6","changing_policy",
              "future_excessive_quote","fake_verified_close","future_source_drift"]{
    let mut candidate=saved.clone();
    let entry=candidate.entry.as_mut().unwrap();
    match test{
       "remove_link"=>entry.origin_dual_opening_link=None,
       "wrong_bill"=>entry.spot_prehedge_bills_evidence.as_mut().unwrap().verified_bill_ids[0]="999".into(),
       "wrong_trade"=>entry.prospective_joint_evidence.as_mut().unwrap().futures_trade_ids[0]="F999".into(),
       "foreign_futures_id"=>entry.recovery_checkpoint.as_mut().unwrap().client_id="foreign".into(),
       "wrong_futures_order"=>entry.recovery_checkpoint.as_mut().unwrap().order_id="777".into(),
       "wrong_fee"=>entry.futures_entry_window_evidence.as_mut().unwrap().signed_quote_fee_cost_atoms_e12+=1,
       "position_residual"=>entry.prospective_joint_evidence.as_mut().unwrap().exposed_spot_unhedged_atoms_e18=0,
       "missing_c3"=>entry.spot_prehedge_bills_evidence=None,
       "missing_c5"=>entry.futures_entry_window_evidence=None,
       "missing_c6"=>entry.prospective_joint_evidence=None,
       "changing_policy"=>entry.policy_binding.as_mut().unwrap().config_sha256="12".repeat(32),
       "future_excessive_quote"=>{
           let cp=entry.recovery_checkpoint.as_mut().unwrap();
           cp.ledger.cumulative_notional_atoms2*=5;
       },
       "fake_verified_close"=>{
           let v=serde_json::to_value(&entry.origin_dual_opening_link).unwrap();
           let mut obj=v.as_object().unwrap().clone();
           obj.insert("may_verify_closed".into(),serde_json::Value::Bool(true));
           entry.origin_dual_opening_link=Some(serde_json::from_value(
               serde_json::Value::Object(obj)).unwrap());
       },
       "future_source_drift"=>entry.prospective_joint_evidence.as_mut().unwrap().source_generation=3,
       _=>unreachable!(),
    }
    candidate.generation=9;
    assert!(store.replace(8,&candidate).is_err(),"forged {test} accepted");
    assert_eq!(raw,std::fs::read(dir.join("state.json")).unwrap());
 }
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn b4b_futures_immutable_link_survives_cas_and_denies_erasure(){
 let (dir,store)=b4b_seed_synthetic_two_venues("b4b-owner");
 let (saved,_)=store.record_origin_dual_opening_link(7).unwrap();
 let link=saved.entry.as_ref().unwrap().origin_dual_opening_link.clone().unwrap();
 let prev=std::fs::read(dir.join("state.json")).unwrap();
 assert!(store.transact(8,|s|{
    s.entry.as_mut().unwrap().origin_dual_opening_link=None;
    Ok(())
 }).is_err());
 assert_eq!(prev,std::fs::read(dir.join("state.json")).unwrap());
 assert!(store.transact(8,|s|{
    s.entry=None;
    Ok(())
 }).is_err(),"cannot erase native spot+futures exposure by deleting entry");
 assert_eq!(prev,std::fs::read(dir.join("state.json")).unwrap());
 let (next,_)=store.transact(8,|s|{
    s.legacy_closed_positions_ignored+=1;
    Ok(())
 }).unwrap();
 assert_eq!(next.generation,9);
 assert_eq!(next.entry.unwrap().origin_dual_opening_link.unwrap(),link);
 assert!(store.record_origin_dual_opening_link(8).is_err());
 assert_eq!(store.record_origin_dual_opening_link(9).unwrap().1,
    spotarb_native_market_core::native_origin_dual_open::OriginDualOpeningWrite::Idempotent);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn b4b_race_and_failed_tmp_write_never_duplicate_or_lose_receipt(){
 use spotarb_native_market_core::native_origin_dual_open::OriginDualOpeningWrite;
 let (dir,store)=b4b_seed_synthetic_two_venues("b4b-race");
 let a=store.clone();let b=store.clone();
 let x=std::thread::spawn(move||a.record_origin_dual_opening_link(7));
 let y=std::thread::spawn(move||b.record_origin_dual_opening_link(7));
 let r=[x.join().unwrap(),y.join().unwrap()];
 assert_eq!(r.iter().filter(|x|matches!(x,Ok((_,OriginDualOpeningWrite::Recorded)))).count(),1);
 assert_eq!(r.iter().filter(|x|x.is_err()).count()
     +r.iter().filter(|x|matches!(x,Ok((_,OriginDualOpeningWrite::Idempotent)))).count(),1);
 assert_eq!(store.load().unwrap().unwrap().generation,8);
 std::fs::remove_dir_all(dir).unwrap();

 let (dir,store)=b4b_seed_synthetic_two_venues("b4b-io");
 let before=std::fs::read(dir.join("state.json")).unwrap();
 let blocker=dir.join("state.tmp");
 std::fs::create_dir(&blocker).unwrap();
 assert!(store.record_origin_dual_opening_link(7).is_err());
 assert_eq!(before,std::fs::read(dir.join("state.json")).unwrap());
 std::fs::remove_dir(&blocker).unwrap();
 let (after,write)=store.record_origin_dual_opening_link(7).unwrap();
 assert_eq!(write,OriginDualOpeningWrite::Recorded);
 assert_eq!(after.generation,8);
 assert!(after.active.is_none());
 assert!(after.entry.unwrap().origin_dual_opening_link.is_some());
 std::fs::remove_dir_all(dir).unwrap();
}


// R32D6C8B2B2B4C1: permitless SHADOW close identity in RecoveryRequired.
// This is NOT a native active position or a real close lifecycle mutation.
fn b4c1_open_and_candidate(tag:&str)->(
    PathBuf,
    spotarb_native_market_core::authoritative_state::NativeStateStore,
    spotarb_native_market_core::native_close_readonly::DualCloseCandidate,
){
 b4c1_open_and_candidate_sized(tag,"0.04")
}
fn b4c1_open_and_candidate_sized(tag:&str,hedge_s:&str)->(
 PathBuf,
 spotarb_native_market_core::authoritative_state::NativeStateStore,
 spotarb_native_market_core::native_close_readonly::DualCloseCandidate,
){
    use spotarb_native_market_core::native_close_readonly::{DualCloseCandidate,CloseOrderIdentity};
    let (dir,store)=b4b_seed_synthetic_two_venues_sized(tag,hedge_s);
    let (state,_)=store.record_origin_dual_opening_link(7).unwrap();
    let origin=state.entry.as_ref().unwrap().origin.as_ref().unwrap();
    let candidate=DualCloseCandidate{
        position_identity:origin.position_key().to_owned(),base:"BNT".into(),
        created_at_ms:1_780_000_005_000,
        expected_qty_e12:decimal_to_atoms(hedge_s).unwrap(),
        spot:CloseOrderIdentity{
            symbol:"BNT/USDT".into(),client_id:"close-spot-1".into(),
            exchange_order_id:"901".into(),
        },
        futures:CloseOrderIdentity{
            symbol:"BNTUSDT".into(),client_id:"close-future-1".into(),
            exchange_order_id:"902".into(),
        },
    };
    (dir,store,candidate)
}
#[test]
fn b4c1_shadow_close_identity_binds_both_open_orders_but_has_zero_execution_authority(){
    use spotarb_native_market_core::native_origin_close_fence::ShadowCloseFenceWrite;
    let (dir,store,c)=b4c1_open_and_candidate("b4c1-positive");
    let (s,w)=store.record_shadow_close_identity(8,&c).unwrap();
    assert_eq!(w,ShadowCloseFenceWrite::Recorded);
    assert_eq!(s.generation,9);
    assert!(s.active.is_none());
    let e=s.entry.as_ref().unwrap();
    assert_eq!(e.phase,spotarb_native_market_core::authoritative_state::NativeEntryPhase::RecoveryRequired);
    let shadow=e.shadow_close_identity.as_ref().unwrap();
    assert_eq!(shadow.position_key(),e.origin.as_ref().unwrap().position_key());
    assert_eq!(shadow.spot_client_id(),"close-spot-1");
    assert_eq!(shadow.futures_client_id(),"close-future-1");
    assert_eq!(shadow.request_quantity_e12(),decimal_to_atoms("0.04").unwrap());
    assert_eq!(shadow.residual_spot_e18(),decimal_to_atoms("0.05").unwrap()*1_000_000);
    assert_eq!(shadow.source_generation(),8);
    assert!(!shadow.close_receipts_verified()&&!shadow.may_send_order());
    assert!(!shadow.may_verify_closed()&&!shadow.may_release_cooldown());
    assert!(!shadow.is_real_pending_close_intent()&&!shadow.opening_proof_is_exchange_global());
    assert!(e.intents.iter().all(|i|i.id!="close-spot-1"&&i.id!="close-future-1"));
    let reread=spotarb_native_market_core::authoritative_state::NativeStateStore::open(dir.join("state.json")).unwrap();
    assert_eq!(reread.load().unwrap().unwrap(),s);
    let (idempotent,k)=reread.record_shadow_close_identity(8,&c).unwrap();
    assert_eq!(idempotent,s);assert_eq!(k,ShadowCloseFenceWrite::Idempotent);
    assert!(store.prepare_native_close_intent(9,7,"close-futures-real","close-real").is_err());
    std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn b4c1_shadow_close_rejects_wrong_qty_origin_identity_and_reused_open_client(){
    let (dir,store,c)=b4c1_open_and_candidate("b4c1-negative");
    let original=std::fs::read(dir.join("state.json")).unwrap();
    for case in ["wrong_position","wrong_base","wrong_spot_symbol","wrong_futures_symbol",
        "close_0_09_gross","close_0_05_residual","close_0_01_partial_short",
        "reused_open_okx_client","reused_open_binance_client",
        "reused_open_okx_order","reused_open_binance_order",
        "duplicate_close_client","invalid_spot_client","invalid_futures_order",
        "no_amount","observed_before_open","future_clock_excess","zero_timestamp"]{
        let mut forged=c.clone();
        match case{
            "wrong_position"=>forged.position_identity="f".repeat(64),
            "wrong_base"=>forged.base="DOGE".into(),
            "wrong_spot_symbol"=>forged.spot.symbol="DOGE/USDT".into(),
            "wrong_futures_symbol"=>forged.futures.symbol="DOGEUSDT".into(),
            "close_0_09_gross"=>forged.expected_qty_e12=decimal_to_atoms("0.09").unwrap(),
            "close_0_05_residual"=>forged.expected_qty_e12=decimal_to_atoms("0.05").unwrap(),
            "close_0_01_partial_short"=>forged.expected_qty_e12=decimal_to_atoms("0.01").unwrap(),
            "reused_open_okx_client"=>forged.spot.client_id="opening-spot-client-1".into(),
            "reused_open_binance_client"=>forged.futures.client_id="futures-client-1".into(),
            "reused_open_okx_order"=>forged.spot.exchange_order_id="201".into(),
            "reused_open_binance_order"=>forged.futures.exchange_order_id="601".into(),
            "duplicate_close_client"=>forged.futures.client_id=forged.spot.client_id.clone(),
            "invalid_spot_client"=>forged.spot.client_id="bad id".into(),
            "invalid_futures_order"=>forged.futures.exchange_order_id="banana".into(),
            "no_amount"=>forged.expected_qty_e12=0,
            "observed_before_open"=>forged.created_at_ms=1_780_000_004_000,
            "future_clock_excess"=>forged.created_at_ms=1_780_000_604_001,
            "zero_timestamp"=>forged.created_at_ms=0,
            _=>unreachable!(),
        }
        assert!(store.record_shadow_close_identity(8,&forged).is_err(),
            "wrong {case} accepted as shadow close");
        assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),original,
            "wrong {case} changed durable state");
    }
    assert!(store.record_shadow_close_identity(7,&c).is_err());
    assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),original);
    std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn b4c1_shadow_close_owner_cas_denies_removal_source_tamper_and_authority_escalation(){
    let (dir,store,c)=b4c1_open_and_candidate("b4c1-tamper");
    let (state,_)=store.record_shadow_close_identity(8,&c).unwrap();
    let before=std::fs::read(dir.join("state.json")).unwrap();
    for case in ["remove_shadow","delete_entry","swap_config",
        "alter_open_id","increase_source_short","set_verified","set_send",
        "set_cooldown","swap_position_key","wrong_size","change_residual"]{
        let mut spoof=state.clone();
        let entry=spoof.entry.as_mut().unwrap();
        match case{
            "remove_shadow"=>entry.shadow_close_identity=None,
            "delete_entry"=>spoof.entry=None,
            "swap_config"=>entry.policy_binding.as_mut().unwrap().config_sha256="0".repeat(64),
            "alter_open_id"=>entry.recovery_checkpoint.as_mut().unwrap().order_id="876".into(),
            "increase_source_short"=>entry.prospective_joint_evidence.as_mut().unwrap().futures_short_atoms_e18+=1,
            _=>{
                let val=serde_json::to_value(entry.shadow_close_identity.as_ref().unwrap()).unwrap();
                let mut x=val.as_object().unwrap().clone();
                let (key,value)=match case{
                    "set_verified"=>("may_verify_closed",serde_json::Value::Bool(true)),
                    "set_send"=>("may_send_order",serde_json::Value::Bool(true)),
                    "set_cooldown"=>("may_release_cooldown",serde_json::Value::Bool(true)),
                    "swap_position_key"=>("original_position_key_sha256",serde_json::Value::String("0".repeat(64))),
                    "wrong_size"=>("requested_qty_e12",serde_json::json!(decimal_to_atoms("0.09").unwrap())),
                    "change_residual"=>("leftover_spot_atoms_e18",serde_json::json!(0)),
                    _=>unreachable!(),
                };
                x.insert(key.into(),value);
                entry.shadow_close_identity=Some(serde_json::from_value(serde_json::Value::Object(x)).unwrap());
            },
        }
        spoof.generation=10;
        assert!(store.replace(9,&spoof).is_err(),"unsafe {case} accepted");
        assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),before);
    }
    assert!(store.transact(9,|s|{
        s.entry.as_mut().unwrap().shadow_close_identity=None;
        Ok(())
    }).is_err());
    assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),before);
    std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn b4c1_shadow_close_cas_race_idempotence_and_foreign_conflict(){
    use spotarb_native_market_core::native_origin_close_fence::ShadowCloseFenceWrite;
    let (dir,store,c)=b4c1_open_and_candidate("b4c1-concurrency");
    let a=store.clone();let b=store.clone();let x=c.clone();let y=c.clone();
    let t1=std::thread::spawn(move||a.record_shadow_close_identity(8,&x));
    let t2=std::thread::spawn(move||b.record_shadow_close_identity(8,&y));
    let results=[t1.join().unwrap(),t2.join().unwrap()];
    assert_eq!(results.iter().filter(|r|matches!(r,Ok((_,ShadowCloseFenceWrite::Recorded)))).count(),1);
    assert_eq!(results.iter().filter(|r|r.is_err()).count()
        +results.iter().filter(|r|matches!(r,Ok((_,ShadowCloseFenceWrite::Idempotent)))).count(),1);
    assert_eq!(store.load().unwrap().unwrap().generation,9);
    let mut conflict=c.clone();conflict.futures.client_id="foreign-close".into();
    assert!(store.record_shadow_close_identity(9,&conflict).is_err());
    assert!(store.record_shadow_close_identity(7,&c).is_err());
    let (later,_)=store.transact(9,|s|{
        s.legacy_closed_positions_ignored+=1;Ok(())
    }).unwrap();
    assert_eq!(later.generation,10);
    assert!(store.record_shadow_close_identity(8,&c).is_err());
    assert_eq!(store.record_shadow_close_identity(10,&c).unwrap().1,
        ShadowCloseFenceWrite::Idempotent);
    std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn b4c1_shadow_close_disk_write_fails_then_retries_without_lost_exposure(){
    use spotarb_native_market_core::native_origin_close_fence::ShadowCloseFenceWrite;
    let (dir,store,c)=b4c1_open_and_candidate("b4c1-disk");
    let before=std::fs::read(dir.join("state.json")).unwrap();
    let blocker=dir.join("state.tmp");
    std::fs::create_dir(&blocker).unwrap();
    assert!(store.record_shadow_close_identity(8,&c).is_err());
    assert_eq!(before,std::fs::read(dir.join("state.json")).unwrap());
    std::fs::remove_dir(&blocker).unwrap();
    let (s,w)=store.record_shadow_close_identity(8,&c).unwrap();
    assert_eq!(w,ShadowCloseFenceWrite::Recorded);
    assert_eq!(s.generation,9);
    assert!(s.active.is_none());
    assert_eq!(s.entry.unwrap().origin_dual_opening_link.is_some(),true);
    std::fs::remove_dir_all(dir).unwrap();
}

mod b4c2_mock{
use std::sync::atomic::{AtomicUsize,Ordering};
use spotarb_native_market_core::{
 native_close_readonly::{audit_dual_close_order_receipts_once,DualCloseCandidate,CloseOrderIdentity,DualCloseError},
 private_https::{HttpExecutor,HttpsRequest,HttpResponse,TransportError},
 private_rest::{BinanceCredentials,OkxCredentials,OkxDomain,HttpMethod},
 fill_economics::decimal_to_atoms,
};
const NOW:u64=1_780_000_008_000;
#[derive(Clone,Copy,Debug)]
enum Fault{
 None,SpotStatus,FutureNotReduceOnly,FutureSide,WrongOrder,
 ForeignSpotFill,FeeMissing,WrongQty,FullOkxPage,
 SpotNonemptyTail,FutureNonemptyTail,FutureForeignOrder,
 FutureFeeUnknown,FutureDuplicateId,StaleSpotFill,Http500,
 FutureMissingRealizedPnl,FutureWrongMarginAsset,FutureBuyerFalse,FuturePnlWrong,
}
struct Fake{fault:Fault,calls:AtomicUsize,requested:&'static str}
impl Fake{fn new(fault:Fault,requested:&'static str)->Self{
 Self{fault,calls:AtomicUsize::new(0),requested}
}}
fn anchors()->DualCloseCandidate{
 DualCloseCandidate{
  position_identity:"synthetic-candidate-only".into(),base:"BNT".into(),
  created_at_ms:NOW-20_000,expected_qty_e12:decimal_to_atoms("0.04").unwrap(),
  spot:CloseOrderIdentity{
   symbol:"BNT/USDT".into(),client_id:"close-spot-1".into(),
   exchange_order_id:"901".into(),
  },
  futures:CloseOrderIdentity{
   symbol:"BNTUSDT".into(),client_id:"close-future-1".into(),
   exchange_order_id:"902".into(),
  },
 }
}
impl HttpExecutor for Fake {
 fn execute_request(&self,req:&HttpsRequest)->Result<HttpResponse,TransportError>{
  assert_eq!(req.method,HttpMethod::Get);
  assert!(!req.mutation);
  assert!(req.credential_bearing);
  assert!(req.body.is_none());
  let url=url::Url::parse(&req.url).unwrap();
  let qp=url.query_pairs().collect::<std::collections::BTreeMap<_,_>>();
  self.calls.fetch_add(1,Ordering::SeqCst);
  let mut val=match url.path(){
   "/api/v5/trade/order"=>{
     assert_eq!(qp.get("clOrdId").map(|x|x.as_ref()),Some("close-spot-1"));
     serde_json::json!({"code":"0","data":[{
      "instId":"BNT-USDT","ordId":"901","clOrdId":"close-spot-1","side":"sell",
      "state":if matches!(self.fault,Fault::SpotStatus){"partially_filled"}else{"filled"},
      "accFillSz":self.requested,"avgPx":"100","fillTime":(NOW-1000).to_string(),
     }]})
   },
   "/fapi/v1/order"=>{
    assert_eq!(qp.get("origClientOrderId").map(|x|x.as_ref()),Some("close-future-1"));
    serde_json::json!({"symbol":"BNTUSDT",
       "orderId":if matches!(self.fault,Fault::WrongOrder){999}else{902},
       "clientOrderId":"close-future-1",
       "side":if matches!(self.fault,Fault::FutureSide){"SELL"}else{"BUY"},
       "positionSide":"BOTH",
       "reduceOnly":!matches!(self.fault,Fault::FutureNotReduceOnly),
       "status":"FILLED","executedQty":self.requested,"avgPrice":"98",
       "cumQuote":if self.requested=="0.09"{"8.82"}else{"3.92"},"updateTime":NOW-900})
   },
   "/api/v5/trade/fills-history"=>{
    assert_eq!(qp.get("ordId").map(|x|x.as_ref()),Some("901"));
    if qp.contains_key("after"){
     if matches!(self.fault,Fault::SpotNonemptyTail){
      serde_json::json!({"code":"0","data":[{
       "ordId":"901","billId":"300","tradeId":"S20",
       "fillPx":"100","fillSz":"0.004","fee":"-0.001",
       "feeCcy":"USDT","fillTime":(NOW-990).to_string(),
      }]})
     }else{serde_json::json!({"code":"0","data":[]})}
    }else{
     let fill=serde_json::json!({"ordId":if matches!(self.fault,Fault::ForeignSpotFill){"999"}else{"901"},
      "billId":"301","tradeId":"S10","fillPx":"100",
      "fillSz":if matches!(self.fault,Fault::WrongQty){"0.03"}else{self.requested},
      "fee":"-0.009","feeCcy":if matches!(self.fault,Fault::FeeMissing){"DOGE"}else{"USDT"},
      "fillTime":if matches!(self.fault,Fault::StaleSpotFill){NOW-30_000}else{NOW-1000}
       });
     if matches!(self.fault,Fault::FullOkxPage){
      serde_json::json!({"code":"0","data":[fill.clone(),fill]})
     }else{serde_json::json!({"code":"0","data":[fill]})}
    }
   },
   "/fapi/v1/userTrades"=>{
    assert_eq!(qp.get("orderId").map(|x|x.as_ref()),Some("902"));
    if qp.get("fromId").map(|x|x.as_ref())==Some("1002"){
     if matches!(self.fault,Fault::FutureNonemptyTail){
      serde_json::json!([{"orderId":902,"id":1002,"symbol":"BNTUSDT",
       "side":"BUY","positionSide":"BOTH","buyer":true,"marginAsset":"USDT","realizedPnl":"0.03","price":"98","qty":"0.004",
       "commission":"0.001","commissionAsset":"USDT","time":NOW-800}])
     }else{serde_json::json!([])}
    }else{
     assert_eq!(qp.get("fromId").map(|x|x.as_ref()),Some("0"));
     let fill=serde_json::json!({
      "orderId":if matches!(self.fault,Fault::FutureForeignOrder){999}else{902},
      "id":1001,"symbol":"BNTUSDT","side":"BUY","positionSide":"BOTH",
      "buyer":!matches!(self.fault,Fault::FutureBuyerFalse),
      "marginAsset":if matches!(self.fault,Fault::FutureWrongMarginAsset){"BUSD"}else{"USDT"},
      "realizedPnl":if matches!(self.fault,Fault::FutureMissingRealizedPnl){serde_json::Value::Null}else if self.requested=="0.09"&&!matches!(self.fault,Fault::FuturePnlWrong){serde_json::json!("0.18")}else{serde_json::json!("0.27")},
      "price":"98","qty":self.requested,"commission":"0.004",
      "commissionAsset":if matches!(self.fault,Fault::FutureFeeUnknown){"BNB"}else{"USDT"},
      "time":NOW-900,
     });
     if matches!(self.fault,Fault::FutureDuplicateId){serde_json::json!([fill.clone(),fill])}
     else{serde_json::json!([fill])}
    }
   },
   other=>panic!("unexpected mock GET endpoint {other}"),
  };
  super::f2g_numericize_json(&mut val);
  Ok(HttpResponse{
   status:if matches!(self.fault,Fault::Http500){500}else{200},
   headers:vec![],body:val.to_string(),
  })
 }
}
fn audit(fake:&Fake,candidate:&DualCloseCandidate,okx_limit:u16)->Result<
 spotarb_native_market_core::native_close_readonly::DualCloseReadOnlyCandidate,
 DualCloseError>{
 let o=OkxCredentials::new("FAKEOKX","FAKESECRET","FAKEPASS").unwrap();
 let b=BinanceCredentials::new("FAKEBIN","FAKEBINSECRET").unwrap();
 audit_dual_close_order_receipts_once(
  fake,candidate,&o,&b,NOW,OkxDomain::Global,10_000,okx_limit,10)
}
pub(super) fn signed_mock_close(
 candidate:&DualCloseCandidate,kind:&str
)->Result<spotarb_native_market_core::native_close_readonly::DualCloseReadOnlyCandidate,DualCloseError>{
 let f=match kind{
    "ok"=>Fault::None,
    "spot_partial"=>Fault::SpotStatus,
    "futures_not_reduce_only"=>Fault::FutureNotReduceOnly,
    "futures_wrong_side"=>Fault::FutureSide,
    "wrong_futures_order"=>Fault::WrongOrder,
    "foreign_spot_fill"=>Fault::ForeignSpotFill,
    "wrong_fee_asset"=>Fault::FeeMissing,
    "wrong_qty"=>Fault::WrongQty,
    "unexpected_tail_spot"=>Fault::SpotNonemptyTail,
    "unexpected_tail_future"=>Fault::FutureNonemptyTail,
    "foreign_futures_trade"=>Fault::FutureForeignOrder,
    "futures_fee_asset"=>Fault::FutureFeeUnknown,
    "duplicate_future_trade"=>Fault::FutureDuplicateId,
    "stale_spot_trade"=>Fault::StaleSpotFill,
    "http500"=>Fault::Http500,
    "missing_pnl"=>Fault::FutureMissingRealizedPnl,
    "wrong_margin"=>Fault::FutureWrongMarginAsset,
    "wrong_pnl"=>Fault::FuturePnlWrong,
    _=>panic!("unknown B4C2 fault"),
 };
 audit(&Fake::new(f,if candidate.expected_qty_e12==decimal_to_atoms("0.09").unwrap(){"0.09"}else{"0.04"}),candidate,10)
}

}

// R32D6C8B2B2B4C2: the only positive close candidate comes from a
// synthetic signed-GET HttpExecutor, NOT a caller-created receipt struct.
#[test]
fn b4c2_six_signed_get_close_receipts_durably_bound_to_b3_b4b_and_shadow_ids(){
 use spotarb_native_market_core::{
     native_origin_close_receipt::ShadowCloseReceiptWrite,
     authoritative_state::NativeStateStore,
 };
 let (dir,store,proposal)=b4c1_open_and_candidate("b4c2-verified-gets");
 let (saved,_)=store.record_shadow_close_identity(8,&proposal).unwrap();
 assert_eq!(saved.generation,9);
 let receipt=b4c2_mock::signed_mock_close(&proposal,"ok").unwrap();
 assert_eq!(receipt.read_counts(),(2,4));
 assert_eq!(receipt.fill_counts(),(1,1));
 assert_eq!(receipt.spot_ledger().filled_qty_atoms(),decimal_to_atoms("0.04").unwrap());
 assert_eq!(receipt.futures_ledger().filled_qty_atoms(),decimal_to_atoms("0.04").unwrap());
 assert!(!receipt.close_verified()&&!receipt.may_send_order());
 let (s,w)=store.record_shadow_close_receipts(9,&receipt,1_780_000_008_000).unwrap();
 assert_eq!(w,ShadowCloseReceiptWrite::Recorded);
 assert_eq!(s.generation,10);
 assert!(s.active.is_none());
 let e=s.entry.as_ref().unwrap();
 assert_eq!(e.phase,spotarb_native_market_core::authoritative_state::NativeEntryPhase::RecoveryRequired);
 let proof=e.shadow_close_receipt.as_ref().unwrap();
 assert_eq!(proof.position_key(),proposal.position_identity);
 assert_eq!(proof.closing_qty_e12(),decimal_to_atoms("0.04").unwrap());
 assert_eq!(proof.residual_spot_e18(),decimal_to_atoms("0.05").unwrap()*1_000_000);
 assert_eq!(proof.spot_close_trade_ids(),vec!["S10"]);
 assert_eq!(proof.futures_close_trade_ids(),vec!["1001"]);
 assert_eq!(proof.source_generation(),9);
 assert!(proof.two_bounded_order_reads_matched());
 assert!(!proof.may_send_order()&&!proof.may_verify_closed()&&!proof.may_release_cooldown());
 assert!(!proof.lifetime_funding_complete()&&!proof.entire_spot_account_flat());
 let restarted=NativeStateStore::open(dir.join("state.json")).unwrap();
 assert_eq!(restarted.load().unwrap().unwrap(),s);
 let (same,status)=restarted.record_shadow_close_receipts(10,&receipt,1_780_000_008_000).unwrap();
 assert_eq!(same,s);
 assert_eq!(status,ShadowCloseReceiptWrite::Idempotent);
 assert!(store.prepare_native_close_intent(10,7,"forbidden-live-close","forbidden-client").is_err());
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn b4c2_signed_close_get_validator_denies_partial_foreign_fee_missing_or_stale(){
 let (dir,store,proposal)=b4c1_open_and_candidate("b4c2-http-negative");
 let (_,_)=store.record_shadow_close_identity(8,&proposal).unwrap();
 let before=std::fs::read(dir.join("state.json")).unwrap();
 for variant in ["spot_partial","futures_not_reduce_only","futures_wrong_side",
     "wrong_futures_order","foreign_spot_fill","wrong_fee_asset",
     "wrong_qty","unexpected_tail_spot","unexpected_tail_future",
     "foreign_futures_trade","futures_fee_asset","duplicate_future_trade",
     "stale_spot_trade","http500","missing_pnl","wrong_margin"] {
     assert!(b4c2_mock::signed_mock_close(&proposal,variant).is_err(),
         "invalid exchange GET {variant} accepted");
 }
 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),before);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn b4c2_receipts_cannot_be_written_without_a_durable_shadow_identity(){
 let (dir,store,proposal)=b4c1_open_and_candidate("b4c2-noshadow");
 let before=std::fs::read(dir.join("state.json")).unwrap();
 let receipt=b4c2_mock::signed_mock_close(&proposal,"ok").unwrap();
 assert!(store.record_shadow_close_receipts(8,&receipt,1_780_000_008_000).is_err());
 assert!(store.record_shadow_close_receipts(7,&receipt,1_780_000_008_000).is_err());
 assert_eq!(before,std::fs::read(dir.join("state.json")).unwrap());
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn b4c2_receipts_wrong_time_generation_conflicting_proof_and_erasure_fail_closed(){
 let (dir,store,proposal)=b4c1_open_and_candidate("b4c2-owner");
 let (_,_)=store.record_shadow_close_identity(8,&proposal).unwrap();
 let receipt=b4c2_mock::signed_mock_close(&proposal,"ok").unwrap();
 for (at,t) in [(8,1_780_000_008_000),(9,1_780_000_004_000),
                 (9,1_780_000_606_001)]{
    assert!(store.record_shadow_close_receipts(at,&receipt,t).is_err());
 }
 let (s,_)=store.record_shadow_close_receipts(9,&receipt,1_780_000_008_000).unwrap();
 let before=std::fs::read(dir.join("state.json")).unwrap();
 assert!(store.record_shadow_close_receipts(9,&receipt,1_780_000_008_001).is_err());
 assert!(store.record_shadow_close_receipts(8,&receipt,1_780_000_008_000).is_err());
 for what in ["remove_receipt","remove_proposal","delete_entry",
             "fake_order_release","fake_verified_closed","fake_cooldown",
             "swap_trade","fake_spot_flat","futures_changed","opening_changed"]{
    let mut altered=s.clone();
    let e=altered.entry.as_mut().unwrap();
    match what{
       "remove_receipt"=>e.shadow_close_receipt=None,
       "remove_proposal"=>e.shadow_close_identity=None,
       "delete_entry"=>altered.entry=None,
       "futures_changed"=>e.recovery_checkpoint.as_mut().unwrap().order_id="999".into(),
       "opening_changed"=>e.origin_dual_opening_link=None,
       _=>{
          let raw=serde_json::to_string(e.shadow_close_receipt.as_ref().unwrap()).unwrap();
          let modified=match what{
             "fake_order_release"=>raw.replacen("\"may_send_order\":false","\"may_send_order\":true",1),
             "fake_verified_closed"=>raw.replacen("\"may_verify_closed\":false","\"may_verify_closed\":true",1),
             "fake_cooldown"=>raw.replacen("\"may_release_real_cooldown\":false","\"may_release_real_cooldown\":true",1),
             "swap_trade"=>raw.replacen("\"S10\"","\"S1\"",1),
             "fake_spot_flat"=>raw.replacen("\"unmatched_spot_residual_e18\":50000000000000000","\"unmatched_spot_residual_e18\":0",1),
             _=>unreachable!(),
          };
          assert_ne!(raw,modified,"forgery {what} changed nothing");
          e.shadow_close_receipt=Some(serde_json::from_str(&modified).unwrap());
       }
    }
    altered.generation=11;
    assert!(store.replace(10,&altered).is_err(),"forged {what} accepted");
    assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),before);
 }
 assert!(store.transact(10,|s|{s.entry.as_mut().unwrap().shadow_close_receipt=None;Ok(())}).is_err());
 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),before);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn b4c2_receipt_owner_race_and_disk_failure_keep_verified_bytes_consistent(){
 use spotarb_native_market_core::native_origin_close_receipt::ShadowCloseReceiptWrite;
 let (dir,store,proposal)=b4c1_open_and_candidate("b4c2-owner-race");
 let (_,_)=store.record_shadow_close_identity(8,&proposal).unwrap();
 let receipt=b4c2_mock::signed_mock_close(&proposal,"ok").unwrap();
 std::thread::scope(|scope|{
    let a=scope.spawn(||store.record_shadow_close_receipts(9,&receipt,1_780_000_008_000));
    let b=scope.spawn(||store.record_shadow_close_receipts(9,&receipt,1_780_000_008_000));
    let replies=[a.join().unwrap(),b.join().unwrap()];
    assert_eq!(replies.iter().filter(|x|matches!(x,Ok((_,ShadowCloseReceiptWrite::Recorded)))).count(),1);
    assert_eq!(replies.iter().filter(|x|x.is_err()).count()
         +replies.iter().filter(|x|matches!(x,Ok((_,ShadowCloseReceiptWrite::Idempotent)))).count(),1);
 });
 assert_eq!(store.load().unwrap().unwrap().generation,10);
 std::fs::remove_dir_all(dir).unwrap();
 let (dir,store,proposal)=b4c1_open_and_candidate("b4c2-write-fail");
 let (_,_)=store.record_shadow_close_identity(8,&proposal).unwrap();
 let receipt=b4c2_mock::signed_mock_close(&proposal,"ok").unwrap();
 let snapshot=std::fs::read(dir.join("state.json")).unwrap();
 let temp=dir.join("state.tmp");std::fs::create_dir(&temp).unwrap();
 assert!(store.record_shadow_close_receipts(9,&receipt,1_780_000_008_000).is_err());
 assert_eq!(snapshot,std::fs::read(dir.join("state.json")).unwrap());
 std::fs::remove_dir(&temp).unwrap();
 let (after,status)=store.record_shadow_close_receipts(9,&receipt,1_780_000_008_000).unwrap();
 assert_eq!(status,ShadowCloseReceiptWrite::Recorded);
 assert_eq!(after.generation,10);
 assert!(after.active.is_none());
 std::fs::remove_dir_all(dir).unwrap();
}

mod b4c3_mock{
use std::sync::atomic::{AtomicUsize,Ordering};
use spotarb_native_market_core::{
 native_close_readonly::{DualCloseCandidate,CloseOrderIdentity},
 private_https::{HttpExecutor,HttpsRequest,HttpResponse,TransportError},
 private_rest::{BinanceCredentials,OkxCredentials,HttpMethod},
 fill_economics::decimal_to_atoms,
};
const NOW:u64=1_780_000_008_000;
#[derive(Clone,Copy,Debug)]
pub(super) enum Fault {None,Residual,Frozen,ForeignBill,ForeignIncome,MissingFunding,
          ForeignTrade,IncompleteIncome,Drift,WrongIncomeFee,WrongBillAmount,MultiPage,FullSymbol,StaleWallet,WrongPnl}
pub(super) struct Fake{fault:Fault,calls:AtomicUsize,snapshot_calls:AtomicUsize}
impl Fake{pub(super) fn new(fault:Fault)->Self{Self{fault,calls:AtomicUsize::new(0),
     snapshot_calls:AtomicUsize::new(0)}}}
fn candidate()->DualCloseCandidate{
 DualCloseCandidate{
 position_identity:"synthetic-close-1".into(),base:"BNT".into(),
 created_at_ms:NOW-20_000,expected_qty_e12:decimal_to_atoms("0.09").unwrap(),
 spot:CloseOrderIdentity{symbol:"BNT/USDT".into(),client_id:"close-spot-1".into(),exchange_order_id:"901".into()},
 futures:CloseOrderIdentity{symbol:"BNTUSDT".into(),client_id:"close-future-1".into(),exchange_order_id:"902".into()},
 }
}
pub(super) fn creds()->(OkxCredentials,BinanceCredentials){
 (OkxCredentials::new("FAKE_OKX","FAKE_SECRET","FAKE_PASSPHRASE").unwrap(),
  BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap())
}
impl HttpExecutor for Fake {
 fn execute_request(&self,req:&HttpsRequest)->Result<HttpResponse,TransportError>{
  assert_eq!(req.method,HttpMethod::Get);
  assert!(!req.mutation&&req.credential_bearing&&req.body.is_none());
  self.calls.fetch_add(1,Ordering::SeqCst);
  let u=url::Url::parse(&req.url).unwrap();
  let qp=u.query_pairs().collect::<std::collections::BTreeMap<_,_>>();
  let is_after=qp.contains_key("after");
  let page=qp.get("page").map(|x|x.as_ref()).unwrap_or("1");
  let mut j=match u.path() {
   "/api/v5/trade/order"=>serde_json::json!({"code":"0","data":[{
      "instId":"BNT-USDT","ordId":"901","clOrdId":"close-spot-1","side":"sell",
      "state":"filled","accFillSz":"0.09","avgPx":"100","fillTime":NOW-1000}]}),
   "/fapi/v1/order"=>serde_json::json!({"symbol":"BNTUSDT","orderId":902,
      "clientOrderId":"close-future-1","side":"BUY","positionSide":"BOTH",
      "reduceOnly":true,"status":"FILLED","executedQty":"0.09",
      "avgPrice":"98","cumQuote":"8.82","updateTime":NOW-950}),
   "/api/v5/trade/fills-history"=>if is_after{
      serde_json::json!({"code":"0","data":[]})
   } else {serde_json::json!({"code":"0","data":[{
     "ordId":"901","billId":"301","tradeId":"S10","fillPx":"100","fillSz":"0.09",
     "fee":"-0.009","feeCcy":"USDT","fillTime":NOW-1000}]})},
   "/fapi/v1/userTrades" if qp.contains_key("orderId")=>if qp.get("fromId").map(|x|x.as_ref())==Some("1002"){
     serde_json::json!([])
   }else{serde_json::json!([{
      "orderId":902,"id":1001,"symbol":"BNTUSDT","side":"BUY","positionSide":"BOTH",
      "buyer":true,"marginAsset":"USDT","realizedPnl":"0.27",
      "price":"98","qty":"0.09","commission":"0.01",
      "commissionAsset":"USDT","time":NOW-950}])},
   "/api/v5/account/balance"=>{
     let n=self.snapshot_calls.fetch_add(1,Ordering::SeqCst);
     let usd=if matches!(self.fault,Fault::Drift)&&n>0{"101"}else{"100"};
     let f=if matches!(self.fault,Fault::Frozen){"0.001"}else{"0"};
     serde_json::json!({"code":"0","data":[{"details":[
       {"ccy":"BNT","cashBal":f,"availBal":"0","frozenBal":f,"uTime":(NOW-2000).to_string()},
       {"ccy":"USDT","cashBal":usd,"availBal":usd,"frozenBal":"0","uTime":(NOW-2000).to_string()}
     ]}]})
   },
   "/fapi/v3/positionRisk"=>serde_json::json!([{"symbol":"BNTUSDT","positionSide":"BOTH",
     "marginAsset":"USDT","positionAmt":if matches!(self.fault,Fault::Residual){"-0.01"}else{"0"},
     "updateTime":NOW-2000}]),
   "/fapi/v3/balance"=>serde_json::json!([{"asset":"USDT","balance":"500",
     "availableBalance":"499","updateTime":if matches!(self.fault,Fault::StaleWallet){NOW-300_000}else{NOW-2000}}]),
   "/fapi/v1/openOrders"=>serde_json::json!([]),
   "/api/v5/account/bills-archive"=>{
     let ccy=qp.get("ccy").map(|x|x.as_ref()).unwrap_or("");
     if is_after{serde_json::json!({"code":"0","data":[]})}
     else{
      let change=if ccy=="BNT"{"-0.09"}
         else if matches!(self.fault,Fault::WrongBillAmount){"8.992"}else{"8.991"};
      serde_json::json!({"code":"0","data":[{
       "ccy":ccy,"ordId":if matches!(self.fault,Fault::ForeignBill){"999"}else{"901"},
       "instId":"BNT-USDT","subType":"2","tradeId":"S10",
       "billId":if ccy=="BNT"{"310"}else{"311"},
       "balChg":change,"ts":(NOW-950).to_string()}]})
     }
   },
   "/fapi/v1/income"=>{
     if page=="1"{
       let mut rows=vec![
         serde_json::json!({"incomeType":"COMMISSION","tranId":1,"tradeId":1001,
           "income":if matches!(self.fault,Fault::WrongIncomeFee){"-0.05"}else{"-0.004"},
           "asset":"USDT","symbol":"BNTUSDT","time":NOW-950}),
         serde_json::json!({"incomeType":"REALIZED_PNL","tranId":2,"tradeId":1001,
           "income":if matches!(self.fault,Fault::WrongPnl){"0.27"}else{"0.18"},"asset":"USDT","symbol":"BNTUSDT","time":NOW-950}),
       ];
       if !matches!(self.fault,Fault::MissingFunding|Fault::MultiPage){
         rows.push(serde_json::json!({"incomeType":"FUNDING_FEE","tranId":3,
         "income":"-0.02","asset":"USDT","symbol":"BNTUSDT","time":NOW-1500}));
       }
       if matches!(self.fault,Fault::ForeignIncome){
         rows.push(serde_json::json!({"incomeType":"TRANSFER","tranId":4,
         "income":"5","asset":"USDT","symbol":"BNTUSDT","time":NOW-1400}));
       }
       serde_json::json!(rows)
     }else if matches!(self.fault,Fault::MultiPage)&&page=="2"{
       serde_json::json!([{"incomeType":"FUNDING_FEE","tranId":3,
       "income":"-0.02","asset":"USDT","symbol":"BNTUSDT","time":NOW-1500}])
     }else if matches!(self.fault,Fault::IncompleteIncome){
       serde_json::json!([{"incomeType":"FUNDING_FEE","tranId":page,
       "income":"0","asset":"USDT","symbol":"BNTUSDT","time":NOW-1000}])
     }else{serde_json::json!([])}
   },
   "/fapi/v1/userTrades"=>{
      let one=serde_json::json!({"id":1001,"orderId":if matches!(self.fault,Fault::ForeignTrade){999}else{902},
       "symbol":"BNTUSDT","side":"BUY","positionSide":"BOTH","time":NOW-950});
      if matches!(self.fault,Fault::FullSymbol){
       serde_json::json!([one.clone(),one.clone(),one.clone(),one.clone()])
      }else{serde_json::json!([one])}
   },
   other=>panic!("unhandled synthetic mock endpoint: {other}"),
  };
  // Explicitly test malformed selected endpoint in a future fault scenario.
  if j.is_null(){j=serde_json::json!([])}
  super::f2g_numericize_json(&mut j);
  Ok(HttpResponse{status:200,headers:vec![],body:j.to_string()})
 }
}

impl Fake{
 pub(super) fn calls(&self)->usize{self.calls.load(Ordering::SeqCst)}
}

}

// R32D6C8B2B2B4C3 strict bounded-account source audit. No credentialed
// production network; every Fake implements synthetic signed GET only.
#[test]
fn b4c3_partial_original_hedge_rejected_before_any_account_get(){
 use spotarb_native_market_core::native_origin_close_account::OriginCloseAccountError;
 let (dir,store,c)=b4c1_open_and_candidate("b4c3-partial");
 let _=store.record_shadow_close_identity(8,&c).unwrap();
 let receipt=b4c2_mock::signed_mock_close(&c,"ok").unwrap();
 let (_,_)=store.record_shadow_close_receipts(9,&receipt,1_780_000_008_000).unwrap();
 let before=std::fs::read(dir.join("state.json")).unwrap();
 let f=b4c3_mock::Fake::new(b4c3_mock::Fault::None);
 let (okx,bin)=b4c3_mock::creds();
 let denied=store.audit_and_record_shadow_close_account_window(
    10,&receipt,&f,&okx,&bin,1_780_000_008_000,
    spotarb_native_market_core::private_rest::OkxDomain::Global,10_000,10);
 assert!(matches!(denied,Err(OriginCloseAccountError::NonFlatOriginalOpening)));
 assert_eq!(f.calls(),0,"DO NOT send account GET if known 0.05 BNT persists");
 assert_eq!(before,std::fs::read(dir.join("state.json")).unwrap());
 assert!(store.load().unwrap().unwrap().entry.unwrap().shadow_close_account_window.is_none());
 std::fs::remove_dir_all(dir).unwrap();
}
fn b4c3_fully_hedged_mock_case(tag:&str)->(
 PathBuf,spotarb_native_market_core::authoritative_state::NativeStateStore,
 spotarb_native_market_core::native_close_readonly::DualCloseReadOnlyCandidate,
){
 let (dir,store,c)=b4c1_open_and_candidate_sized(tag,"0.09");
 let (x,_)=store.record_shadow_close_identity(8,&c).unwrap();
 assert_eq!(x.generation,9);
 let receipt=b4c2_mock::signed_mock_close(&c,"ok").unwrap();
 assert_eq!(receipt.spot_ledger().filled_qty_atoms(),decimal_to_atoms("0.09").unwrap());
 assert_eq!(receipt.futures_ledger().filled_qty_atoms(),decimal_to_atoms("0.09").unwrap());
 let (x,_)=store.record_shadow_close_receipts(9,&receipt,1_780_000_008_000).unwrap();
 assert_eq!(x.generation,10);
 assert_eq!(x.entry.as_ref().unwrap().origin_dual_opening_link.as_ref()
   .unwrap().exposure_remaining_e18(),0);
 (dir,store,receipt)
}
#[test]
fn b4c3_full_synthetic_hedge_uses_signed_bounded_account_gets_and_persists_without_authority(){
 use spotarb_native_market_core::{
  native_origin_close_account::OriginCloseAccountWrite,
  authoritative_state::NativeStateStore,
  private_rest::OkxDomain,
 };
 let (dir,store,receipt)=b4c3_fully_hedged_mock_case("b4c3-positive");
 let source=b4c3_mock::Fake::new(b4c3_mock::Fault::None);
 let (okx,bin)=b4c3_mock::creds();
 let (state,status)=store.audit_and_record_shadow_close_account_window(
    10,&receipt,&source,&okx,&bin,1_780_000_008_000,OkxDomain::Global,10_000,10).unwrap();
 assert_eq!(status,OriginCloseAccountWrite::Recorded);
 assert_eq!(state.generation,11);
 assert!(source.calls()>8); // 8 bracketing snapshots plus account histories
 let e=state.entry.as_ref().unwrap();
 assert!(state.active.is_none());
 assert_eq!(e.phase,spotarb_native_market_core::authoritative_state::NativeEntryPhase::RecoveryRequired);
 let w=e.shadow_close_account_window.as_ref().unwrap();
 assert_eq!(w.position_key(),e.origin.as_ref().unwrap().position_key());
 assert_eq!(w.source_generation(),10);
 assert_eq!(w.counts(),(1,1,3,1,1));
 assert_eq!(w.account_reads(),8);
 assert_eq!(w.funding_observed_e12(),-(decimal_to_atoms("0.02").unwrap() as i128));
 assert!(!w.may_send_order()&&!w.verified_closed()&&!w.may_release_cooldown());
 assert!(!w.entire_lifetime_funding_proven()&&!w.exchange_global_complete());
 let reopened=NativeStateStore::open(dir.join("state.json")).unwrap();
 assert_eq!(reopened.load().unwrap().unwrap(),state);
 let calls=source.calls();
 let (repeat,status)=reopened.audit_and_record_shadow_close_account_window(
    11,&receipt,&source,&okx,&bin,1_780_000_008_000,OkxDomain::Global,10_000,10).unwrap();
 assert_eq!(repeat,state);
 assert_eq!(status,OriginCloseAccountWrite::Idempotent);
 assert_eq!(source.calls(),calls,"idempotent read must not request network again");
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn b4c3_rejects_residual_locked_moving_stale_and_foreign_account_evidence(){
 use spotarb_native_market_core::private_rest::OkxDomain;
 let (dir,store,receipt)=b4c3_fully_hedged_mock_case("b4c3-faults");
 let old=std::fs::read(dir.join("state.json")).unwrap();
 let (okx,bin)=b4c3_mock::creds();
 for fault in [
    b4c3_mock::Fault::Residual,b4c3_mock::Fault::Frozen,
    b4c3_mock::Fault::Drift,b4c3_mock::Fault::StaleWallet,
    b4c3_mock::Fault::ForeignBill,b4c3_mock::Fault::ForeignIncome,
    b4c3_mock::Fault::ForeignTrade,b4c3_mock::Fault::WrongIncomeFee,
    b4c3_mock::Fault::WrongBillAmount,
    b4c3_mock::Fault::IncompleteIncome,b4c3_mock::Fault::FullSymbol,
    b4c3_mock::Fault::MissingFunding,
 ]{
   let http=b4c3_mock::Fake::new(fault);
   assert!(store.audit_and_record_shadow_close_account_window(
       10,&receipt,&http,&okx,&bin,1_780_000_008_000,OkxDomain::Global,10_000,10).is_err(),
       "unsafe account variant {fault:?} accepted");
   assert_eq!(old,std::fs::read(dir.join("state.json")).unwrap());
 }
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn b4c3_wrong_close_receipt_and_stale_native_generation_rejected_without_get(){
 use spotarb_native_market_core::private_rest::OkxDomain;
 let (dir,store,receipt)=b4c3_fully_hedged_mock_case("b4c3-stale");
 let (okx,bin)=b4c3_mock::creds();
 let fake=b4c3_mock::Fake::new(b4c3_mock::Fault::None);
 let current=std::fs::read(dir.join("state.json")).unwrap();
 assert!(store.audit_and_record_shadow_close_account_window(
   9,&receipt,&fake,&okx,&bin,1_780_000_008_000,OkxDomain::Global,10_000,10).is_err());
 let (otherdir,otherstore,otherc)=b4c1_open_and_candidate("b4c3-foreign-receipt");
 let _=otherstore.record_shadow_close_identity(8,&otherc).unwrap();
 let foreign=b4c2_mock::signed_mock_close(&otherc,"ok").unwrap();
 assert!(store.audit_and_record_shadow_close_account_window(
   10,&foreign,&fake,&okx,&bin,1_780_000_008_000,OkxDomain::Global,10_000,10).is_err());
 assert_eq!(fake.calls(),0);
 assert_eq!(current,std::fs::read(dir.join("state.json")).unwrap());
 std::fs::remove_dir_all(dir).unwrap();
 std::fs::remove_dir_all(otherdir).unwrap();
}
#[test]
fn b4c3_owner_cas_source_generation_drift_denied_after_signed_mock_gets(){
 use spotarb_native_market_core::{
  private_rest::OkxDomain,
  private_https::{HttpExecutor,HttpsRequest,HttpResponse,TransportError},
  authoritative_state::NativeStateStore,
 };
 use std::sync::atomic::{AtomicBool,Ordering};
 struct Drifter<'a>{
     inner:b4c3_mock::Fake,
     store:&'a NativeStateStore,
     tripped:AtomicBool,
 }
 impl HttpExecutor for Drifter<'_>{
     fn execute_request(&self,r:&HttpsRequest)->Result<HttpResponse,TransportError>{
         if !self.tripped.swap(true,Ordering::SeqCst){
             let (updated,_)=self.store.transact(10,|s|{
                 s.legacy_closed_positions_ignored+=1;Ok(())
             }).unwrap();
             assert_eq!(updated.generation,11);
         }
         self.inner.execute_request(r)
     }
 }
 let (dir,store,receipt)=b4c3_fully_hedged_mock_case("b4c3-generation-drift");
 let f=Drifter{inner:b4c3_mock::Fake::new(b4c3_mock::Fault::None),
     store:&store,tripped:AtomicBool::new(false)};
 let (okx,bin)=b4c3_mock::creds();
 assert!(store.audit_and_record_shadow_close_account_window(
      10,&receipt,&f,&okx,&bin,1_780_000_008_000,OkxDomain::Global,10_000,10).is_err());
 let state=store.load().unwrap().unwrap();
 assert_eq!(state.generation,11);
 assert!(state.entry.unwrap().shadow_close_account_window.is_none());
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn b4c3_account_digest_cas_immutable_and_failed_disk_write_retry(){
 use spotarb_native_market_core::private_rest::OkxDomain;
 let (dir,store,receipt)=b4c3_fully_hedged_mock_case("b4c3-tamper");
 let (okx,bin)=b4c3_mock::creds();
 let f=b4c3_mock::Fake::new(b4c3_mock::Fault::None);
 let (s,_)=store.audit_and_record_shadow_close_account_window(
      10,&receipt,&f,&okx,&bin,1_780_000_008_000,OkxDomain::Global,10_000,10).unwrap();
 let original=std::fs::read(dir.join("state.json")).unwrap();
 for case in ["remove_window","delete_entry","remove_close_receipt",
              "spoof_close_verified","spoof_cooldown","spoof_lifetime",
              "corrupt_funding","alter_source_generation"]{
    let mut candidate=s.clone();
    let entry=candidate.entry.as_mut().unwrap();
    match case{
       "remove_window"=>entry.shadow_close_account_window=None,
       "delete_entry"=>candidate.entry=None,
       "remove_close_receipt"=>entry.shadow_close_receipt=None,
       _=>{
         let raw=serde_json::to_string(&entry.shadow_close_account_window).unwrap();
         let (old,new)=match case{
          "spoof_close_verified"=>("\"may_mark_verified_closed\":false","\"may_mark_verified_closed\":true"),
          "spoof_cooldown"=>("\"may_release_real_cooldown\":false","\"may_release_real_cooldown\":true"),
          "spoof_lifetime"=>("\"original_funding_lifetime_complete\":false","\"original_funding_lifetime_complete\":true"),
          "corrupt_funding"=>("\"observed_funding_e12\":-20000000000","\"observed_funding_e12\":0"),
          "alter_source_generation"=>("\"source_generation\":10","\"source_generation\":6"),
          _=>unreachable!(),
         };
         let patched=raw.replacen(old,new,1);
         assert_ne!(raw,patched,"test {case} failed to mutate witness");
         entry.shadow_close_account_window=serde_json::from_str(&patched).unwrap();
       }
    }
    candidate.generation=12;
    assert!(store.replace(11,&candidate).is_err(),"unsafe {case} accepted");
    assert_eq!(original,std::fs::read(dir.join("state.json")).unwrap());
 }
 assert!(store.transact(11,|s|{s.entry.as_mut().unwrap().shadow_close_account_window=None;Ok(())}).is_err());
 std::fs::remove_dir_all(dir).unwrap();
 let (dir,store,receipt)=b4c3_fully_hedged_mock_case("b4c3-disk");
 let (okx,bin)=b4c3_mock::creds();
 let old=std::fs::read(dir.join("state.json")).unwrap();
 let blocker=dir.join("state.tmp");std::fs::create_dir(&blocker).unwrap();
 let f=b4c3_mock::Fake::new(b4c3_mock::Fault::None);
 assert!(store.audit_and_record_shadow_close_account_window(
      10,&receipt,&f,&okx,&bin,1_780_000_008_000,OkxDomain::Global,10_000,10).is_err());
 assert_eq!(old,std::fs::read(dir.join("state.json")).unwrap());
 std::fs::remove_dir(&blocker).unwrap();
 let (after,_)=store.audit_and_record_shadow_close_account_window(
      10,&receipt,&f,&okx,&bin,1_780_000_008_000,OkxDomain::Global,10_000,10).unwrap();
 assert_eq!(after.generation,11);
 assert!(after.active.is_none());
 std::fs::remove_dir_all(dir).unwrap();
}


// B4C4A: non-authorizing, exact four-ledger economics. Full history is
// explicitly NOT proven and every lifetime/VerifiedClosed bit stays false.
fn b4c4_seed_account_window(tag:&str)->(
 PathBuf,
 spotarb_native_market_core::authoritative_state::NativeStateStore,
){
 use spotarb_native_market_core::private_rest::OkxDomain;
 let (dir,store,receipt)=b4c3_fully_hedged_mock_case(tag);
 let get=b4c3_mock::Fake::new(b4c3_mock::Fault::None);
 let (okx,bin)=b4c3_mock::creds();
 let (state,_)=store.audit_and_record_shadow_close_account_window(
    10,&receipt,&get,&okx,&bin,1_780_000_008_000,OkxDomain::Global,10_000,10).unwrap();
 assert_eq!(state.generation,11);
 (dir,store)
}
#[test]
fn b4c4_four_leg_cashflows_reconcile_exact_futures_pnl_but_lifetime_net_is_unknown(){
 use spotarb_native_market_core::{
    native_origin_lifetime_economics::ShadowLifetimeEconomicsWrite,
    authoritative_state::NativeStateStore,
 };
 let (dir,store)=b4c4_seed_account_window("b4c4-positive");
 let (state,status)=store.record_shadow_lifetime_economics_gap(11).unwrap();
 assert_eq!(status,ShadowLifetimeEconomicsWrite::Recorded);
 assert_eq!(state.generation,12);
 let e=state.entry.as_ref().unwrap();
 assert!(state.active.is_none());
 assert_eq!(e.phase,spotarb_native_market_core::authoritative_state::NativeEntryPhase::RecoveryRequired);
 let p=e.shadow_lifetime_economics.as_ref().unwrap();
 assert_eq!(p.source_generation(),11);
 assert_eq!(p.reported_futures_close_pnl_e12(),decimal_to_atoms("0.18").unwrap() as i128);
 assert_eq!(p.recomputed_futures_close_pnl_e12(),decimal_to_atoms("0.18").unwrap() as i128);
 assert_eq!(p.known_trade_fees_e12(),decimal_to_atoms("0.026").unwrap() as i128);
 assert_eq!(p.bounded_funding_e12(),-(decimal_to_atoms("0.02").unwrap() as i128));
 assert_eq!(p.observed_window_net_e12(),decimal_to_atoms("0.134").unwrap() as i128);
 assert!(!p.funding_lifetime_complete()&&!p.authoritative_net_pnl_known());
 assert!(!p.may_send_order()&&!p.may_verify_closed()&&!p.may_release_cooldown());
 let reopened=NativeStateStore::open(dir.join("state.json")).unwrap();
 assert_eq!(reopened.load().unwrap().unwrap(),state);
 let (same,again)=reopened.record_shadow_lifetime_economics_gap(12).unwrap();
 assert_eq!(again,ShadowLifetimeEconomicsWrite::Idempotent);
 assert_eq!(same,state);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn b4c4_binance_reported_pnl_027_disagrees_with_actual_original_018_and_fails_closed(){
 use spotarb_native_market_core::private_rest::OkxDomain;
 let (dir,store,proposal)=b4c1_open_and_candidate_sized("b4c4-wrong-pnl","0.09");
 store.record_shadow_close_identity(8,&proposal).unwrap();
 let wrong=b4c2_mock::signed_mock_close(&proposal,"wrong_pnl").unwrap();
 assert_eq!(wrong.futures_reported_realized_pnl_e12(),decimal_to_atoms("0.27").unwrap() as i128);
 store.record_shadow_close_receipts(9,&wrong,1_780_000_008_000).unwrap();
 let source=b4c3_mock::Fake::new(b4c3_mock::Fault::WrongPnl);
 let (okx,bin)=b4c3_mock::creds();
 let (s,_)=store.audit_and_record_shadow_close_account_window(
      10,&wrong,&source,&okx,&bin,1_780_000_008_000,OkxDomain::Global,10_000,10).unwrap();
 assert_eq!(s.generation,11,"bounded source alone cannot detect wrong opening PNL");
 let before=std::fs::read(dir.join("state.json")).unwrap();
 assert!(store.record_shadow_lifetime_economics_gap(11).is_err(),
     "B4C4 must reject reported PNL not independently derived from original entry");
 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),before);
 assert!(store.load().unwrap().unwrap().entry.unwrap().shadow_lifetime_economics.is_none());
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn b4c4_denies_missing_account_window_or_known_partial_exposure(){
 let (dir,store,_)=b4c1_open_and_candidate("b4c4-partial");
 let initial=std::fs::read(dir.join("state.json")).unwrap();
 assert!(store.record_shadow_lifetime_economics_gap(8).is_err());
 assert!(store.record_shadow_lifetime_economics_gap(7).is_err());
 assert_eq!(initial,std::fs::read(dir.join("state.json")).unwrap());
 std::fs::remove_dir_all(dir).unwrap();
 let (dir,store,_)=b4c3_fully_hedged_mock_case("b4c4-missing-bounded");
 let initial=std::fs::read(dir.join("state.json")).unwrap();
 assert!(store.record_shadow_lifetime_economics_gap(10).is_err());
 assert_eq!(initial,std::fs::read(dir.join("state.json")).unwrap());
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn b4c4_owner_guards_deny_fee_pnl_funding_tampering_and_fake_verified_close(){
 let (dir,store)=b4c4_seed_account_window("b4c4-owner");
 let (saved,_)=store.record_shadow_lifetime_economics_gap(11).unwrap();
 let before=std::fs::read(dir.join("state.json")).unwrap();
 for case in ["erase","erase_original_opening","erase_close","erase_account",
              "spoof_lifetime","spoof_full_net","spoof_verified",
              "spoof_cooldown","spoof_send","spoof_funding","spoof_fee",
              "spoof_net","alter_pnl","swap_source_generation"]{
     let mut edited=saved.clone();
     let e=edited.entry.as_mut().unwrap();
     match case{
        "erase"=>e.shadow_lifetime_economics=None,
        "erase_original_opening"=>e.origin_dual_opening_link=None,
        "erase_close"=>e.shadow_close_receipt=None,
        "erase_account"=>e.shadow_close_account_window=None,
        "alter_pnl"=>e.shadow_close_receipt=None,
        _=>{
           let old=serde_json::to_string(&e.shadow_lifetime_economics).unwrap();
           let (f,t)=match case{
             "spoof_lifetime"=>("\"full_open_to_close_funding_history_complete\":false","\"full_open_to_close_funding_history_complete\":true"),
             "spoof_full_net"=>("\"definitive_lifetime_net_pnl_known\":false","\"definitive_lifetime_net_pnl_known\":true"),
             "spoof_verified"=>("\"may_verify_closed\":false","\"may_verify_closed\":true"),
             "spoof_cooldown"=>("\"may_release_cooldown\":false","\"may_release_cooldown\":true"),
             "spoof_send"=>("\"may_send_order\":false","\"may_send_order\":true"),
             "spoof_funding"=>("\"verified_only_close_window_funding_e12\":-20000000000","\"verified_only_close_window_funding_e12\":0"),
             "spoof_fee"=>("\"trade_fee_total_e12\":26000000000","\"trade_fee_total_e12\":0"),
             "spoof_net"=>("\"observed_window_net_quote_e12\":134000000000","\"observed_window_net_quote_e12\":0"),
             "swap_source_generation"=>("\"source_generation\":11","\"source_generation\":10"),
             _=>unreachable!(),
           };
           let changed=old.replacen(f,t,1);
           assert_ne!(old,changed,"test mutation not effective for {case}");
           e.shadow_lifetime_economics=serde_json::from_str(&changed).unwrap();
        }
     }
     edited.generation=13;
     assert!(store.replace(12,&edited).is_err(),"unexpected accepted {case}");
     assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),before);
 }
 assert!(store.transact(12,|s|{s.entry.as_mut().unwrap().shadow_lifetime_economics=None;Ok(())}).is_err());
 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),before);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn b4c4_cas_race_and_blocked_io_preserve_economic_witness_and_exposure(){
 use spotarb_native_market_core::native_origin_lifetime_economics::ShadowLifetimeEconomicsWrite;
 let (dir,store)=b4c4_seed_account_window("b4c4-race");
 std::thread::scope(|scope|{
    let a=scope.spawn(||store.record_shadow_lifetime_economics_gap(11));
    let b=scope.spawn(||store.record_shadow_lifetime_economics_gap(11));
    let out=[a.join().unwrap(),b.join().unwrap()];
    assert_eq!(out.iter().filter(|x|matches!(x,Ok((_,ShadowLifetimeEconomicsWrite::Recorded)))).count(),1);
    assert_eq!(out.iter().filter(|x|x.is_err()).count()
       +out.iter().filter(|x|matches!(x,Ok((_,ShadowLifetimeEconomicsWrite::Idempotent)))).count(),1);
 });
 assert_eq!(store.load().unwrap().unwrap().generation,12);
 assert!(store.record_shadow_lifetime_economics_gap(11).is_err());
 std::fs::remove_dir_all(dir).unwrap();

 let (dir,store)=b4c4_seed_account_window("b4c4-io");
 let before=std::fs::read(dir.join("state.json")).unwrap();
 let blocker=dir.join("state.tmp");
 std::fs::create_dir(&blocker).unwrap();
 assert!(store.record_shadow_lifetime_economics_gap(11).is_err());
 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),before);
 std::fs::remove_dir(&blocker).unwrap();
 let (s,status)=store.record_shadow_lifetime_economics_gap(11).unwrap();
 assert_eq!(status,ShadowLifetimeEconomicsWrite::Recorded);
 assert_eq!(s.generation,12);
 assert!(s.active.is_none());
 std::fs::remove_dir_all(dir).unwrap();
}


// TRADE2F2C: cross-bind durable B4C4 four-leg owner + source-shaped
// mock-only E2A double GET + pure E2B receipt binder. No real account access.
struct E2CIncomeHistoryMock{
    fault:u8,
    requests:std::sync::atomic::AtomicUsize,
    native:Option<(spotarb_native_market_core::authoritative_state::NativeStateStore,u64)>,
}
impl E2CIncomeHistoryMock{
 fn new(fault:u8)->Self{
    Self{fault,requests:std::sync::atomic::AtomicUsize::new(0),native:None}
 }
 fn n(&self)->usize{self.requests.load(std::sync::atomic::Ordering::SeqCst)}
}
impl spotarb_native_market_core::private_https::HttpExecutor for E2CIncomeHistoryMock{
 fn execute_request(&self,r:&spotarb_native_market_core::private_https::HttpsRequest)
  ->Result<spotarb_native_market_core::private_https::HttpResponse,
           spotarb_native_market_core::private_https::TransportError>{
  use spotarb_native_market_core::private_https::HttpResponse;
  use spotarb_native_market_core::private_rest::HttpMethod;
  use std::sync::atomic::Ordering;
  assert_eq!(r.method,HttpMethod::Get);
  assert!(!r.mutation&&r.body.is_none());
  let n=self.requests.fetch_add(1,Ordering::SeqCst);
  if self.fault==5&&n==9{
     let (store,expected)=self.native.as_ref().unwrap();
     let (updated,_)=store.transact(*expected,|_|Ok(())).unwrap();
     assert_eq!(updated.generation,*expected+1);
  }
  let u=url::Url::parse(&r.url).unwrap();
  let q=u.query_pairs().collect::<std::collections::BTreeMap<_,_>>();
  const AT:u64=1_780_000_008_000;
  let mut result=match u.path(){
   "/fapi/v1/income"=>{
      assert!(!q.contains_key("incomeType")&&!q.contains_key("symbol"));
      if q.get("page").map(|v|v.as_ref())!=Some("1"){
         serde_json::json!([])
      }else{
         serde_json::json!([
          {"incomeType":"COMMISSION","tranId":310,"symbol":"BNTUSDT",
           "tradeId":"F1","income":"-0.002","asset":"USDT",
           "time":1_780_000_003_060u64},
          {"incomeType":"COMMISSION","tranId":311,"symbol":"BNTUSDT",
           "tradeId":"F2","income":"-0.002","asset":"USDT",
           "time":1_780_000_003_110u64},
          {"incomeType":"COMMISSION","tranId":312,"symbol":"BNTUSDT",
           "tradeId":"1001","income":"-0.004","asset":"USDT",
           "time":AT-850},
          {"incomeType":"REALIZED_PNL","tranId":313,"symbol":"BNTUSDT",
           "tradeId":"1001","income":"0.18","asset":"USDT","time":AT-840},
          {"incomeType":"FUNDING_FEE","tranId":314,"symbol":"BNTUSDT",
           "tradeId":"","income":"-0.02","asset":"USDT",
           "time":1_780_000_005_500u64},
          {"incomeType":"TRANSFER","tranId":314,"symbol":"","tradeId":"",
           "income":"10","asset":"USDT","time":AT-700}
         ])
      }
   },
   "/api/v5/account/bills-archive"=>{
      if q.contains_key("after"){
         serde_json::json!({"code":"0","data":[]})
      }else if q.get("ccy").map(|v|v.as_ref())==Some("BNT"){
         // The real-shaped normal case has exactly ONE original bill per
         // execution: both source bills below are USDT, not duplicated BASE.
         // Fault 6 instead models a single BASE cash bill with unsourced
         // bill-only fee for S1, replacing its USDT original.
         if self.fault==6{
          serde_json::json!({"code":"0","data":[
           {"billId":"8001","ccy":"BNT","bal":"0.09","balChg":"0.09",
            "ts":1_780_000_000_150u64,"type":"2","subType":"1",
            "ordId":"201","tradeId":"S1","fee":"-0.0001",
            "instType":"SPOT","instId":"BNT-USDT"}]})
         }else{serde_json::json!({"code":"0","data":[]})}
      }else{
         assert_eq!(q.get("ccy").map(|v|v.as_ref()),Some("USDT"));
         if self.fault==6{
          serde_json::json!({"code":"0","data":[
           {"billId":"9002","ccy":"USDT","bal":"98.991","balChg":"8.991",
            "ts":AT-930,"type":"2","subType":"2",
            "ordId":"901","tradeId":"S10"}]})
         }else{
          serde_json::json!({"code":"0","data":[
           {"billId":"9002","ccy":"USDT","bal":"98.991","balChg":"8.991",
            "ts":AT-930,"type":"2","subType":"2",
            "ordId":"901","tradeId":"S10"},
           {"billId":"301","ccy":"USDT","bal":"90","balChg":"-9.009",
            "ts":1_780_000_000_150u64,"type":"2","subType":"1",
            "ordId":"201","tradeId":"S1"}]})
         }
      }
   },
   other=>panic!("unexpected private mock GET path: {other}"),
  };
  match self.fault{
   1 if u.path()=="/fapi/v1/income"&&q.get("page").map(|x|x.as_ref())==Some("1")=>
       result[0]["income"]=serde_json::json!("-0.003"),
   2 if u.path()=="/api/v5/account/bills-archive"
         &&!q.contains_key("after")&&q.get("ccy").map(|x|x.as_ref())==Some("USDT")=>{
       result["data"][0]["balChg"]=serde_json::json!("8.992");
       result["data"][0]["bal"]=serde_json::json!("98.992");
   },
   3 if u.path()=="/fapi/v1/income"
         &&q.get("page").map(|x|x.as_ref())==Some("1")&&n>=5=>
       result[4]["income"]=serde_json::json!("-0.03"),
   4 if u.path()=="/api/v5/account/bills-archive"
        // BNT has no source rows in valid one-bill history, so the
        // unexhausted cursor fault must target the active USDT lane.
        &&q.contains_key("after")&&q.get("ccy").map(|x|x.as_ref())==Some("USDT")=>
       result["data"]=serde_json::json!([{"billId":"7777"}]),
   7 if u.path()=="/api/v5/account/bills-archive"
       &&!q.contains_key("after")&&q.get("ccy").map(|x|x.as_ref())==Some("USDT")=>{
       result["data"][0]["fee"]=serde_json::json!("-0.777");
       result["data"][0]["instType"]=serde_json::json!("SPOT");
       result["data"][0]["instId"]=serde_json::json!("BNT-USDT");
   },
   8 if u.path()=="/api/v5/account/bills-archive"
       &&!q.contains_key("after")&&q.get("ccy").map(|x|x.as_ref())==Some("USDT")=>{
       result["data"][0]["fee"]=serde_json::json!("-0.009");
       result["data"][1]["fee"]=serde_json::json!("-0.009");
       for row in result["data"].as_array_mut().unwrap(){
           row["instType"]=serde_json::json!("SPOT");
           row["instId"]=serde_json::json!("BNT-USDT");
       }
   },
   10 if u.path()=="/api/v5/account/bills-archive"
       &&!q.contains_key("after")&&q.get("ccy").map(|x|x.as_ref())==Some("USDT")=>{
       // Same fill/trade/amount as before; change ONLY original billId.
       // Previously the F2C owner accepted a foreign bill ancestry.
       result["data"][1]["billId"]=serde_json::json!("999");
   },
   9 if u.path()=="/fapi/v1/income"
      &&q.get("page").map(|x|x.as_ref())==Some("1")=>{
       // original F1 fee 0.002 split over two independently
       // identified transaction IDs; closing income 0.18 split.
       result[0]["income"]=serde_json::json!("-0.001");
       result.as_array_mut().unwrap().push(serde_json::json!({
          "incomeType":"COMMISSION","tranId":320,"symbol":"BNTUSDT",
          "tradeId":"F1","income":"-0.001","asset":"USDT",
          "time":1_780_000_003_070u64,
       }));
       result[3]["income"]=serde_json::json!("0.09");
       result.as_array_mut().unwrap().push(serde_json::json!({
          "incomeType":"REALIZED_PNL","tranId":321,"symbol":"BNTUSDT",
          "tradeId":"1001","income":"0.09","asset":"USDT",
          "time":AT-830,
       }));
   },
   _=>{}
  }
  Ok(HttpResponse{status:200,headers:vec![],body:result.to_string()})
 }
}

fn trade2f2c_run(
 store:&spotarb_native_market_core::authoritative_state::NativeStateStore,
 generation:u64,fake:&E2CIncomeHistoryMock,
)->Result<spotarb_native_market_core::native_trade2f_owner_binding::OwnerScopedTrade2F2C,
    spotarb_native_market_core::native_trade2f_owner_binding::OwnerCashError>{
 use spotarb_native_market_core::{
    native_trade2f_owner_binding::inspect_owner_bound_mock_four_leg_income,
    private_rest::{BinanceCredentials,OkxCredentials,OkxDomain},
 };
 let spot=OkxCredentials::new("FAKEOKX","FAKESECRET","FAKEPASS").unwrap();
 let future=BinanceCredentials::new("FAKEFUT","FAKESECRET").unwrap();
 inspect_owner_bound_mock_four_leg_income(
    fake,store,generation,&spot,&future,
    1_780_000_008_000,OkxDomain::Global,10_000)
}
#[test]
fn trade2f2c_full_durably_sealed_four_leg_owner_double_cash_get_binds_every_id_without_live_authority(){
 let (dir,store)=b4c4_seed_account_window("trade2f2c-original-close");
 let (s,_)=store.record_shadow_lifetime_economics_gap(11).unwrap();
 assert_eq!(s.generation,12);
 let fake=E2CIncomeHistoryMock::new(0);
 let original=std::fs::read(dir.join("state.json")).unwrap();
 let review=trade2f2c_run(&store,s.generation,&fake).unwrap();
 assert_eq!(fake.n(),10);
 assert_eq!(review.signed_mock_get_count,10);
 assert!(review.two_independent_get_snapshots_matched);
 let m=&review.ordinary_four_leg_receipt_match;
 assert_eq!(m.spot_fill_ids,vec!["S1".to_string(),"S10".to_string()]);
 assert_eq!(m.futures_fill_ids,vec!["1001".to_string(),"F1".into(),"F2".into()]);
 assert_eq!(m.trade_only_net_e12,decimal_to_atoms("0.154").unwrap() as i128);
 assert_eq!(m.window_adjusted_net_e12,decimal_to_atoms("0.134").unwrap() as i128);
 assert_eq!(m.observed_unclassified_transfer_e12,
             decimal_to_atoms("10").unwrap() as i128);
 assert!(!review.original_real_close_dispatch_proven
    &&!review.complete_income_history_through_settlement_proven
    &&!review.original_account_externally_antirollback_proven
    &&!review.complete_lifetime_net_pnl_proven
    &&!review.may_send_cancel_retry_or_verify_closed);
 assert!(!m.independent_live_source_attested
    &&!m.complete_retained_income_history_proven
    &&!m.all_lifetime_funding_proven
    &&!m.may_send_cancel_retry_or_verify_closed);
 assert_eq!(m.definitive_lifetime_net_pnl_e12,None);
 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),original);
 assert_eq!(store.load().unwrap().unwrap(),s);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn p104_f2c_original_buy_billid_must_match_persisted_owner_ancestry(){
 let (dir,store)=b4c4_seed_account_window("p104-f2c-owner-raw-bill-id");
 let (owner,_)=store.record_shadow_lifetime_economics_gap(11).unwrap();
 let prior=owner.entry.as_ref().unwrap()
    .spot_prehedge_evidence.as_ref().unwrap().fill_bill_ids.clone();
 assert_eq!(prior,vec!["301".to_string()]);
 let before=std::fs::read(dir.join("state.json")).unwrap();
 let f=E2CIncomeHistoryMock::new(10);
 // All source cash numbers match the original four fills; ONLY the
 // supplied opening billId is changed from the saved owner lineage.
 assert!(trade2f2c_run(&store,owner.generation,&f).is_err(),
    "foreign source billId accepted despite persisted owner ancestry");
 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),before);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn trade2f2c_attested_mock_history_with_fee_quote_errors_or_page_drift_denies_without_mutation(){
 for (label,mode) in [
    ("Binance commission not equal to original trade F1",1u8),
    ("OKX sell quote differs from exact price minus fee",2),
    ("Binance funding changes between two source scans",3),
    ("OKX history cursor never exhausted",4),
 ]{
    let (dir,store)=b4c4_seed_account_window(&format!("trade2f2c-corrupt-{mode}"));
    let (s,_)=store.record_shadow_lifetime_economics_gap(11).unwrap();
    let bytes=std::fs::read(dir.join("state.json")).unwrap();
    let f=E2CIncomeHistoryMock::new(mode);
    assert!(trade2f2c_run(&store,s.generation,&f).is_err(),
        "unsafe monetary receipt passed: {label}");
    assert!(f.n()<=12);
    assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),bytes);
    std::fs::remove_dir_all(dir).unwrap();
 }
}
#[test]
fn trade2f2c_partial_original_spot_vs_future_or_missing_full_close_denies_before_any_get(){
 // Original sealed B4B case: 0.09 BNT spot vs 0.04 BNT short.
 let (dir,store,_proposal)=b4c1_open_and_candidate("trade2f2c-unmatched");
 let state=store.load().unwrap().unwrap();
 let f=E2CIncomeHistoryMock::new(0);
 assert!(trade2f2c_run(&store,state.generation,&f).is_err());
 assert_eq!(f.n(),0);
 std::fs::remove_dir_all(dir).unwrap();
 // Even a fully hedged setup with B4C3 account brackets has no sealed
 // original complete four-leg economics until that separate owner CAS.
 let (dir,store)=b4c4_seed_account_window("trade2f2c-before-lifetime");
 let state=store.load().unwrap().unwrap();
 let f=E2CIncomeHistoryMock::new(0);
 assert!(trade2f2c_run(&store,state.generation,&f).is_err());
 assert_eq!(f.n(),0);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn trade2f2c_native_owner_revision_during_final_money_get_denies_double_pass(){
 let (dir,store)=b4c4_seed_account_window("trade2f2c-owner-race");
 let (s,_)=store.record_shadow_lifetime_economics_gap(11).unwrap();
 let native=spotarb_native_market_core::authoritative_state::NativeStateStore::open(
    dir.join("state.json")).unwrap();
 let mut f=E2CIncomeHistoryMock::new(5);
 f.native=Some((native,s.generation));
 assert!(trade2f2c_run(&store,s.generation,&f).is_err());
 assert_eq!(f.n(),10);
 assert_eq!(store.load().unwrap().unwrap().generation,s.generation+1);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn trade2f2c_wrong_native_generation_refuses_before_any_cash_get(){
 let (dir,store)=b4c4_seed_account_window("trade2f2c-wrong-gen");
 let (s,_)=store.record_shadow_lifetime_economics_gap(11).unwrap();
 let f=E2CIncomeHistoryMock::new(0);
 assert!(trade2f2c_run(&store,s.generation-1,&f).is_err());
 assert_eq!(f.n(),0);
 std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn trade2f2d_owner_bound_unknown_bill_fee_currency_fails_closed(){
 for (label,mode) in [
   ("OKX native BASE fee is NOT USDT profit",6u8),
   ("wrong signed quote fee conflicts with original fill commission",7),
   ("even numerically equal USDT quote fee lacks feeCcy attestation",8),
 ]{
    let (dir,store)=b4c4_seed_account_window(&format!("trade2f2d-guard-{mode}"));
    let (state,_)=store.record_shadow_lifetime_economics_gap(11).unwrap();
    let bytes=std::fs::read(dir.join("state.json")).unwrap();
    let f=E2CIncomeHistoryMock::new(mode);
    assert!(trade2f2c_run(&store,state.generation,&f).is_err(),"{label}");
    assert!(f.n()<=12);
    assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),bytes);
    std::fs::remove_dir_all(dir).unwrap();
 }
}
#[test]
fn trade2f2d_owner_bound_multi_receipt_income_matches_without_double_accounting(){
 for mode in [9u8]{
    let (dir,store)=b4c4_seed_account_window(&format!("trade2f2d-src-{mode}"));
    let (state,_)=store.record_shadow_lifetime_economics_gap(11).unwrap();
    let bytes=std::fs::read(dir.join("state.json")).unwrap();
    let f=E2CIncomeHistoryMock::new(mode);
    let result=trade2f2c_run(&store,state.generation,&f).unwrap();
    assert_eq!(f.n(),10);
    let p=&result.ordinary_four_leg_receipt_match;
    assert_eq!(p.trade_only_net_e12,decimal_to_atoms("0.154").unwrap() as i128);
    assert_eq!(p.window_adjusted_net_e12,decimal_to_atoms("0.134").unwrap() as i128);
    assert_eq!(p.observed_unclassified_transfer_e12,decimal_to_atoms("10").unwrap() as i128);
    assert!(!result.complete_lifetime_net_pnl_proven
       &&!result.original_real_close_dispatch_proven
       &&!result.may_send_cancel_retry_or_verify_closed);
    assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),bytes);
    std::fs::remove_dir_all(dir).unwrap();
 }
}


#[test]
fn trade2f2g_old_sealed_owner_with_nonnumeric_synthetic_okx_trade_ids_denies_before_mock_get(){
 use spotarb_native_market_core::{
   native_trade2f_okx_owner_mock::{inspect_owner_bound_okx_offline_mock,F2GError},
   private_rest::OkxDomain,
 };
 let (dir,store)=b4c4_seed_account_window("f2g-b4c4-alpha-ids");
 let (state,_)=store.record_shadow_lifetime_economics_gap(11).unwrap();
 let original=std::fs::read(dir.join("state.json")).unwrap();
 // Previous F2C fixtures intentionally used fake IDs S1 and S10,
 // not documented numeric OKX trade IDs. They cannot certify F2G.
 let out=inspect_owner_bound_okx_offline_mock(
   &store,state.generation,1_780_000_008_000,OkxDomain::Global,&[]);
 assert!(matches!(out,Err(F2GError::Deny(
    "non-numeric persisted OKX trade ID before GET"))));
 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),original);
 std::fs::remove_dir_all(dir).unwrap();
}


// TRADE2F2G: the OLD synthetic S1/S10 fixture remains unchanged by default.
// Only this thread's scoped guard creates a fresh, genuinely sealed owner
// with canonical numeric IDs before each persisted generation transition.
fn f2g_complete_signed_mock_tape()->Vec<
 spotarb_native_market_core::native_trade2f_okx_owner_mock::MockGetStep>{
 use spotarb_native_market_core::native_trade2f_okx_owner_mock::MockGetStep;
 use serde_json::json;
 const BEGIN:u64=1_780_000_000_000;
 const END:u64=1_780_000_008_000;
 let fill=|ord:&str,tid:&str,bill:&str,side:&str,time:&str,ts:&str|{
   json!({"instType":"SPOT","instId":"BNT-USDT","tradeQuoteCcy":"USDT",
    "side":side,"subType":if side=="buy"{"1"}else{"2"},
    "ordId":ord,"tradeId":tid,"billId":bill,"fillSz":"0.09","fillPx":"100",
    "fee":"-0.009","feeCcy":"USDT","fillTime":time,"ts":ts})
 };
 let bill=|ord:&str,tid:&str,id:&str,side:&str,bal:&str,change:&str,ts:&str|{
    json!({"instType":"SPOT","instId":"BNT-USDT","type":"2",
      "subType":if side=="buy"{"1"}else{"2"},"ccy":"USDT",
      "ordId":ord,"tradeId":tid,"billId":id,"fee":"-0.009",
      "bal":bal,"balChg":change,"ts":ts})
 };
 let item=|path:String,rows:Vec<serde_json::Value>|MockGetStep{
    path_and_query:path,http_status:200,
    body:json!({"code":"0","data":rows}).to_string()
 };
 let fills=|order:&str,after:Option<&str>|{
   let mut p=format!("/api/v5/trade/fills-history?instType=SPOT&instId=BNT-USDT&ordId={order}&limit=100");
   if let Some(v)=after{p.push_str("&after=");p.push_str(v)}
   p
 };
 let bills=|ccy:&str,after:Option<&str>|{
   let mut p=format!("/api/v5/account/bills-archive?ccy={ccy}&begin={BEGIN}&end={END}&limit=100");
   if let Some(v)=after{p.push_str("&after=");p.push_str(v)}
   p
 };
 let mut v=Vec::new();
 for _ in 0..2{
  v.push(item(fills("201",None),vec![fill("201","9001","301",
      "buy","1780000000100","1780000000101")]));
  v.push(item(fills("201",Some("301")),vec![]));
  v.push(item(fills("901",None),vec![fill("901","9002","302",
      "sell","1780000007000","1780000007001")]));
  v.push(item(fills("901",Some("302")),vec![]));
  v.push(item(bills("BNT",None),vec![]));
  v.push(item(bills("USDT",None),vec![
    bill("901","9002","302","sell","99.982","8.991","1780000007050"),
    bill("201","9001","301","buy","90.991","-9.009","1780000000150"),
  ]));
  v.push(item(bills("USDT",Some("301")),vec![]));
 }
 v
}
#[test]
fn trade2f2g_full_numeric_original_native_owner_to_two_signed_mock_source_scans_pass(){
 use spotarb_native_market_core::{
  authoritative_state::NativeStateStore,
  native_trade2f_okx_owner_mock::inspect_owner_bound_okx_offline_mock,
  private_rest::OkxDomain,
 };
 let _numeric=F2GNumericFixtureGuard::new();
 let (dir,store)=b4c4_seed_account_window("f2g-numeric-sealed-owner");
 let (state,_)=store.record_shadow_lifetime_economics_gap(11).unwrap();
 let e=state.entry.as_ref().unwrap();
 assert_eq!(e.spot_prehedge_evidence.as_ref().unwrap().exact_ledger
    .fills.keys().cloned().collect::<Vec<_>>(),vec!["9001".to_string()]);
 assert_eq!(e.shadow_close_receipt.as_ref().unwrap().spot_close_trade_ids(),
    vec!["9002".to_string()]);
 let bytes=std::fs::read(dir.join("state.json")).unwrap();
 let reopen=NativeStateStore::open(dir.join("state.json")).unwrap();
 assert_eq!(reopen.load().unwrap().as_ref(),Some(&state));
 let tape=f2g_complete_signed_mock_tape();
 let result=inspect_owner_bound_okx_offline_mock(
   &reopen,state.generation,1_780_000_008_000,OkxDomain::Global,&tape).unwrap();
 assert_eq!(result.signed_mock_gets,14);
 assert_eq!(result.original_owner_generation,state.generation);
 assert!(result.two_full_identical_mock_scans);
 assert_eq!(result.narrow_detail_bill_topology.matched_bill_ids,
     vec!["301".to_string(),"302".to_string()]);
 assert_eq!(result.narrow_detail_bill_topology.fee_adjusted_inventory.aggregate_base_wallet_delta_e12,0);
 assert!(!result.real_ready && !result.verified_closed
     &&!result.independent_real_source_attested
     &&!result.complete_account_history_proven
     &&!result.may_send_cancel_retry_or_transfer);
 assert_eq!(result.full_lifetime_pnl_e12,None);
 #[cfg(feature="offline-b6-review")]
 {
  use spotarb_native_market_core::native_trade2f3_source_provenance::
      original_okx_b1_b2_fills_as_b6_review;
  let pages=original_okx_b1_b2_fills_as_b6_review(
    &reopen,state.generation,1_780_000_008_000,1_780_000_068_000,
    OkxDomain::Global,&tape,&"b".repeat(64)).unwrap();
  assert_eq!(pages.len(),4);
  assert_eq!(pages[0].lane,b6_catalogue::Lane::OkxBuy);
  assert_eq!(pages[0].rows[0].unique_key,"9001");
  assert_eq!(pages[0].cursor_out.as_deref(),Some("301"));
  assert_eq!(pages[1].cursor_in.as_deref(),Some("301"));
  assert!(pages[1].last_page);
  assert_eq!(pages[2].lane,b6_catalogue::Lane::OkxSell);
  assert_eq!(pages[2].rows[0].unique_key,"9002");
  assert_eq!(pages[2].rows[0].order_id.as_deref(),Some("901"));
  assert_eq!(pages[2].rows[0].symbol.as_deref(),Some("BNT-USDT"));
  assert_eq!(pages[3].cursor_in.as_deref(),Some("302"));
  assert!(pages[3].last_page);
  assert_eq!(pages[0].body_sha256,
      openssl::sha::sha256(tape[0].body.as_bytes())
      .iter().map(|b|format!("{b:02x}")).collect::<String>());
  assert_eq!(pages.iter().flat_map(|p|p.rows.iter().map(|r|r.unique_key.as_str()))
      .collect::<Vec<_>>(),vec!["9001","9002"]);
  let mut forged=tape.clone();
  forged[0].body=forged[0].body.replace("9001","9999");
  assert!(original_okx_b1_b2_fills_as_b6_review(
     &reopen,state.generation,1_780_000_008_000,1_780_000_068_000,
     OkxDomain::Global,&forged,&"b".repeat(64)).is_err());
  assert!(original_okx_b1_b2_fills_as_b6_review(
     &reopen,state.generation,1_780_000_008_000,1_780_000_068_000,
     OkxDomain::Global,&tape,&"0".repeat(64)).is_err());
 }
 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),bytes);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn trade2f2g_full_numeric_original_owner_hostile_source_and_cas_denied(){
 use spotarb_native_market_core::{
  native_trade2f_okx_owner_mock::{inspect_owner_bound_okx_offline_mock,F2GError},
  private_rest::OkxDomain,
 };
 use serde_json::json;
 let _numeric=F2GNumericFixtureGuard::new();
 let (dir,store)=b4c4_seed_account_window("f2g-numeric-hostile");
 let (state,_)=store.record_shadow_lifetime_economics_gap(11).unwrap();
 let before=std::fs::read(dir.join("state.json")).unwrap();
 let original=f2g_complete_signed_mock_tape();
 assert!(matches!(inspect_owner_bound_okx_offline_mock(
   &store,state.generation-1,1_780_000_008_000,OkxDomain::Global,&original),
   Err(F2GError::Deny("stale original generation before GET"))));
 for fault in 0..25u8{
  let mut steps=original.clone();
  let target=match fault{12..=14=>5,15..=16=>2,_=>0};
  let mut j:serde_json::Value=serde_json::from_str(&steps[target].body).unwrap();
  match fault{
    0=>j["data"][0]["tradeId"]=json!("9000"),
    1=>j["data"][0]["ordId"]=json!("999"),
    2=>j["data"][0]["feeCcy"]=json!("BNT"),
    3=>j["data"][0]["fee"]=json!("-0.008"),
    4=>j["data"][0]["fillPx"]=json!("101"),
    5=>j["data"][0]["fillSz"]=json!("0.090001"),
    6=>j["data"][0]["fillTime"]=json!("1780000000200"),
    7=>j["data"][0]["instId"]=json!("ZEC-USDT"),
    8=>j["data"][0]["side"]=json!("sell"),
    9=>j["data"][0]["billId"]=json!("999"),
    10=>j["data"][0]["tradeQuoteCcy"]=json!("BNB"),
    11=>{steps.remove(1);},
    12=>j["data"][0]["balChg"]=json!("8.992"),
    13=>j["data"][0]["fee"]=json!("-0.008"),
    14=>j["data"][0]["billId"]=json!("303"),
    15=>j["data"][0]["ordId"]=json!("201"),
    16=>j["data"][0]["fillTime"]=json!("1780000007100"),
    17=>j["code"]=json!("51000"),
    18=>{let dup=j["data"][0].clone();j["data"].as_array_mut().unwrap().push(dup);},
    19=>{steps[0].http_status=429;},
    20=>{steps[7].body=json!({"code":"0","data":[]}).to_string();},
    21=>{steps[0].path_and_query.push_str("&unexpected=true");},
    22=>{steps[6].body=json!({"code":"0","data":[{"billId":"300"}]}).to_string();},
    23=>{steps.pop();},
    24=>{steps.push(original[0].clone());},
    _=>unreachable!(),
  }
  if !matches!(fault,11|19|20|21|22|23|24){
    steps[target].body=j.to_string();
  }
  let r=inspect_owner_bound_okx_offline_mock(
    &store,state.generation,1_780_000_008_000,OkxDomain::Global,&steps);
  assert!(r.is_err(),"hostile F2G fault {fault} accepted");
  assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),before);
 }
 std::fs::remove_dir_all(dir).unwrap();
}


#[test]
fn trade2f2g_full_numeric_owner_cas_revision_during_two_rounds_must_deny(){
 use spotarb_native_market_core::{
  native_trade2f_okx_owner_mock::{
    inspect_owner_bound_okx_offline_mock_debug_after_first,F2GError},
  private_rest::OkxDomain,
 };
 let _numeric=F2GNumericFixtureGuard::new();
 let (dir,store)=b4c4_seed_account_window("f2g-cas-between-rounds");
 let (state,_)=store.record_shadow_lifetime_economics_gap(11).unwrap();
 let before=std::fs::read(dir.join("state.json")).unwrap();
 let mut hook_calls=0u8;
 let denied=inspect_owner_bound_okx_offline_mock_debug_after_first(
    &store,state.generation,1_780_000_008_000,OkxDomain::Global,
    &f2g_complete_signed_mock_tape(),||{
       hook_calls+=1;
       let (updated,_)=store.transact(state.generation,|s|{
          s.legacy_closed_positions_ignored+=1;Ok(())
       }).unwrap();
       assert_eq!(updated.generation,state.generation+1);
    });
 assert_eq!(hook_calls,1);
 assert!(matches!(denied,Err(F2GError::Deny(
   "Native owner changed during first MOCK read"))));
 assert_ne!(std::fs::read(dir.join("state.json")).unwrap(),before);
 assert_eq!(store.load().unwrap().unwrap().generation,state.generation+1);
 std::fs::remove_dir_all(dir).unwrap();
}


// TRADE2F2H: separate fully offline widened OKX settlement-bills tape.
// A clean finite snapshot does not certify that future account postings
// or Binance late income can never arrive.
fn f2h_settlement_tape()->Vec<
 spotarb_native_market_core::native_trade2f_okx_owner_mock::MockGetStep>{
 use spotarb_native_market_core::native_trade2f_okx_owner_mock::MockGetStep;
 use serde_json::json;
 const START:u64=1_780_000_000_000;
 const END:u64=1_780_000_068_000;
 let make=|path:String,rows:Vec<serde_json::Value>|MockGetStep{
  path_and_query:path,http_status:200,body:json!({"code":"0","data":rows}).to_string()
 };
 let path=|ccy:&str,after:Option<&str>|{
   let mut p=format!("/api/v5/account/bills-archive?ccy={ccy}&begin={START}&end={END}&limit=100");
   if let Some(id)=after{p.push_str("&after=");p.push_str(id)}
   p
 };
 let buy=json!({"instType":"SPOT","instId":"BNT-USDT","type":"2","subType":"1",
    "ccy":"USDT","billId":"301","ordId":"201","tradeId":"9001",
    "balChg":"-9.009","bal":"90.991","fee":"-0.009","ts":"1780000000150"});
 let sell=json!({"instType":"SPOT","instId":"BNT-USDT","type":"2","subType":"2",
    "ccy":"USDT","billId":"302","ordId":"901","tradeId":"9002",
    "balChg":"8.991","bal":"99.982","fee":"-0.009","ts":"1780000007050"});
 let mut t=Vec::new();
 for _ in 0..2{
    t.push(make(path("BNT",None),vec![]));
    t.push(make(path("USDT",None),vec![sell.clone(),buy.clone()]));
    t.push(make(path("USDT",Some("301")),vec![]));
 }
 t
}
#[test]
fn trade2f2h_numeric_sealed_owner_two_widened_settlement_mock_windows_pass_without_profit_authority(){
 use spotarb_native_market_core::{
  native_trade2f_post_settlement_guard::inspect_owner_bound_late_okx_bills_offline,
  private_rest::OkxDomain,
 };
 let _numeric=F2GNumericFixtureGuard::new();
 let (dir,store)=b4c4_seed_account_window("f2h-offline-stable");
 let (state,_)=store.record_shadow_lifetime_economics_gap(11).unwrap();
 let old=std::fs::read(dir.join("state.json")).unwrap();
 let report=inspect_owner_bound_late_okx_bills_offline(
  &store,state.generation,1_780_000_008_000,1_780_000_068_000,
  OkxDomain::Global,&f2g_complete_signed_mock_tape(),&f2h_settlement_tape()).unwrap();
 assert_eq!(report.signed_mock_gets_total,20);
 assert_eq!(report.original_bill_ids_crosschecked,2);
 assert!(report.two_late_mock_scans_identical&&report.no_additional_bills_in_mock_window);
 assert!(!report.all_real_account_bill_classes_proven
  &&!report.independent_real_source_attested
  &&!report.all_delayed_postings_absent_proven
  &&!report.all_binance_late_income_absent_proven
  &&!report.real_ready&&!report.verified_closed
  &&!report.may_trade_cancel_transfer_or_release);
 assert_eq!(report.entire_lifetime_net_pnl_e12,None);
 assert_eq!(old,std::fs::read(dir.join("state.json")).unwrap());
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn trade2f2h_unexplained_late_account_rows_fee_rewrite_cash_chain_and_cursors_all_deny(){
 use spotarb_native_market_core::{
  native_trade2f_post_settlement_guard::inspect_owner_bound_late_okx_bills_offline,
  private_rest::OkxDomain,
 };
 use serde_json::json;
 let _numeric=F2GNumericFixtureGuard::new();
 let (dir,store)=b4c4_seed_account_window("f2h-hostile-window");
 let (state,_)=store.record_shadow_lifetime_economics_gap(11).unwrap();
 let prior=std::fs::read(dir.join("state.json")).unwrap();
 let tape=f2h_settlement_tape();
 let original=f2g_complete_signed_mock_tape();
 for fault in 0..30u8{
  let mut t=tape.clone();
  let mut j:serde_json::Value=serde_json::from_str(&t[1].body).unwrap();
  match fault{
    0=>{j["data"].as_array_mut().unwrap().insert(0,
      json!({"instType":"EVENTS","instId":"","ccy":"USDT","type":"1",
      "subType":"11","billId":"303","ordId":"","tradeId":"",
      "balChg":"5","bal":"104.982","ts":"1780000020000","fee":"0"}));},
    1=>{j["data"].as_array_mut().unwrap().insert(0,
      json!({"instType":"SPOT","instId":"BNT-USDT","ccy":"USDT","type":"2",
      "subType":"1","billId":"303","ordId":"201","tradeId":"9999",
      "balChg":"0.1","bal":"100.082","ts":"1780000012000","fee":"0"}));},
    2=>j["data"][0]["fee"]=json!("-0.008"),
    3=>j["data"][0]["balChg"]=json!("8.992"),
    4=>j["data"][0]["bal"]=json!("100.0"),
    5=>j["data"][0]["tradeId"]=json!("9902"),
    6=>j["data"][0]["ordId"]=json!("999"),
    7=>j["data"][0]["ccy"]=json!("BNT"),
    8=>j["data"][0]["type"]=json!("1"),
    9=>j["data"][0]["subType"]=json!("1"),
    10=>j["data"][0]["instType"]=json!("MARGIN"),
    11=>j["data"][0]["instId"]=json!("OTHER-USDT"),
    12=>j["data"][0]["ts"]=json!("1780000060000"),
    13=>j["data"][0]["ts"]=json!("1780000000001"),
    14=>j["data"][0]["billId"]=json!("301"),
    15=>j["data"][0]["fee"]=json!("0.0001"),
    16=>j["data"][0]["bal"]=json!("0.0000000000001"),
    17=>j["data"]=json!([j["data"][0].clone()]),
    18=>{j["data"][0]["balChg"]=json!("999");},
    19=>{t[1].http_status=403;},
    20=>{t[1].body="{".into();},
    21=>{t[1].path_and_query.push_str("&type=2");},
    22=>{t.remove(2);},
    23=>{t.pop();},
    24=>{t.push(tape[0].clone());},
    25=>{t[4].body=json!({"code":"0","data":[]}).to_string();},
    26=>{t[0].body=json!({"code":"0","data":[
        {"ccy":"BNT","billId":"304","bal":"0.09","balChg":"0.09",
        "type":"1","subType":"11","instType":"EVENTS",
        "instId":"","ts":"1780000020000","fee":"0"}]}).to_string();},
    27=>{j["data"][0]["balChg"]=json!("0.0");},
    28=>{t[1].body=json!({"code":"51000","data":[]}).to_string();},
    29=>{j["data"][0]["ts"]=json!("1780000007051");},
    _=>unreachable!(),
  }
  if !matches!(fault,19|20|21|22|23|24|25|26|28){
     t[1].body=j.to_string();
  }
  let result=inspect_owner_bound_late_okx_bills_offline(
     &store,state.generation,1_780_000_008_000,1_780_000_068_000,
     OkxDomain::Global,&original,&t);
  assert!(result.is_err(),"late CASH fault {fault} accepted");
  assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),prior);
 }
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn trade2f2h_partial_c6_unknown_and_invalid_settlement_window_are_zero_get_denied(){
 use spotarb_native_market_core::{
  native_trade2f_post_settlement_guard::{inspect_owner_bound_late_okx_bills_offline,F2HError},
  private_rest::OkxDomain,
 };
 let (dir,store)=b4c4_seed_account_window("f2h-invalid-before-get");
 let (state,_)=store.record_shadow_lifetime_economics_gap(11).unwrap();
 let missing=inspect_owner_bound_late_okx_bills_offline(
  &store,state.generation,1_780_000_008_000,1_780_000_008_001,
  OkxDomain::Global,&[],&[]);
 assert!(matches!(missing,Err(F2HError::Deny(
   "settlement observation time too short/long for diagnostic"))));
 std::fs::remove_dir_all(dir).unwrap();
 let (dir,store,_proposal)=b4c1_open_and_candidate("f2h-c6-partial");
 let state=store.load().unwrap().unwrap();
 let denied=inspect_owner_bound_late_okx_bills_offline(
  &store,state.generation,1_780_000_008_000,1_780_000_068_000,
  OkxDomain::Global,&[],&[]);
 assert!(matches!(denied,Err(F2HError::First(_))));
 std::fs::remove_dir_all(dir).unwrap();
}


#[test]
fn trade2f2h_native_generation_race_between_late_bill_scans_is_deny(){
 use spotarb_native_market_core::{
  native_trade2f_post_settlement_guard::{
    inspect_owner_bound_late_okx_bills_debug_after_first,F2HError},
  private_rest::OkxDomain,
 };
 let _numeric=F2GNumericFixtureGuard::new();
 let (dir,store)=b4c4_seed_account_window("f2h-late-owner-race");
 let (state,_)=store.record_shadow_lifetime_economics_gap(11).unwrap();
 let mut calls=0u8;
 let result=inspect_owner_bound_late_okx_bills_debug_after_first(
    &store,state.generation,1_780_000_008_000,1_780_000_068_000,
    OkxDomain::Global,&f2g_complete_signed_mock_tape(),
    &f2h_settlement_tape(),||{
      calls+=1;
      let (changed,_)=store.transact(state.generation,|s|{
        s.legacy_closed_positions_ignored+=1;Ok(())
      }).unwrap();
      assert_eq!(changed.generation,state.generation+1);
    });
 assert_eq!(calls,1);
 assert!(matches!(result,Err(F2HError::Deny(
  "Native owner CAS drift after first settlement scan"))));
 assert_eq!(store.load().unwrap().unwrap().generation,state.generation+1);
 std::fs::remove_dir_all(dir).unwrap();
}


// TRADE2F2I: the Binance source proof remains IN-MEMORY ONLY. The exact
// signed request path comes from the known fake-credentials REST builder.
fn f2i_full_income_tape()->Vec<
 spotarb_native_market_core::native_trade2f_okx_owner_mock::MockGetStep>{
 use spotarb_native_market_core::{
   native_trade2f_okx_owner_mock::MockGetStep,
   private_rest::{BinanceCredentials,build_binance_all_income_page_request},
 };
 use serde_json::json;
 const START:u64=1_780_000_000_000;
 let cred=BinanceCredentials::new("F2I_OFFLINE_BINANCE_KEY",
     "F2I_OFFLINE_BINANCE_SECRET").unwrap();
 let mk=|end:u64,page:u16,rows:Vec<serde_json::Value>|{
   let signed=build_binance_all_income_page_request(
      &cred,end,10_000,START,end,page,1000).unwrap();
   MockGetStep{path_and_query:signed.url.strip_prefix("https://fapi.binance.com")
      .unwrap().to_owned(),http_status:200,body:json!(rows).to_string()}
 };
 let rows=vec![
  json!({"incomeType":"COMMISSION","tranId":310,"symbol":"BNTUSDT",
    "tradeId":"F1","income":"-0.002","asset":"USDT","time":1_780_000_003_060u64}),
  json!({"incomeType":"COMMISSION","tranId":311,"symbol":"BNTUSDT",
    "tradeId":"F2","income":"-0.002","asset":"USDT","time":1_780_000_003_110u64}),
  json!({"incomeType":"COMMISSION","tranId":312,"symbol":"BNTUSDT",
    "tradeId":"1001","income":"-0.004","asset":"USDT","time":1_780_000_007_150u64}),
  json!({"incomeType":"REALIZED_PNL","tranId":313,"symbol":"BNTUSDT",
    "tradeId":"1001","income":"0.18","asset":"USDT","time":1_780_000_007_160u64}),
  json!({"incomeType":"FUNDING_FEE","tranId":314,"symbol":"BNTUSDT",
    "tradeId":"","income":"-0.02","asset":"USDT","time":1_780_000_005_500u64}),
 ];
 let mut tape=vec![];
 for end in [1_780_000_008_000u64,1_780_000_008_000,
             1_780_000_068_000,1_780_000_068_000]{
   tape.push(mk(end,1,rows.clone()));
   tape.push(mk(end,2,vec![]));
 }
 tape
}
#[test]
fn trade2f2i_four_original_futures_fills_late_income_mock_stable_and_still_no_profit_permission(){
 use spotarb_native_market_core::native_trade2f_binance_late_income::
   inspect_owner_bound_binance_late_income_offline;
 let _numeric=F2GNumericFixtureGuard::new();
 let (dir,store)=b4c4_seed_account_window("f2i-income-stable");
 let (state,_)=store.record_shadow_lifetime_economics_gap(11).unwrap();
 let old=std::fs::read(dir.join("state.json")).unwrap();
 let result=inspect_owner_bound_binance_late_income_offline(
  &store,state.generation,1_780_000_008_000,1_780_000_068_000,
  &f2i_full_income_tape()).unwrap();
 assert_eq!(result.signed_mock_gets,8);
 assert_eq!(result.source_income_rows,5);
 assert_eq!(result.observed_bounded_funding_rows,1);
 assert!(result.two_original_and_two_late_mock_scans_stable);
 assert_eq!(result.late_income_rows_in_observed_mock_window,0);
 assert!(!result.third_asset_usdt_fx_independently_proven
   &&!result.all_real_binance_income_proven
   &&!result.account_wide_history_and_late_finality_proven
   &&!result.real_ready&&!result.verified_closed
   &&!result.may_trade_cancel_transfer_or_release);
 assert_eq!(result.definitive_lifetime_net_pnl_e12,None);
 #[cfg(feature="offline-b6-review")]
 {
   use spotarb_native_market_core::{
     native_trade2f3_source_provenance::original_binance_income_b1_b2_as_b6_review,
     private_rest::OkxDomain,
   };
   let source=f2i_full_income_tape();
   let typed=original_binance_income_b1_b2_as_b6_review(
     &store,state.generation,1_780_000_008_000,1_780_000_068_000,
     OkxDomain::Global,&source,&"c".repeat(64)).unwrap();
   assert_eq!(typed.len(),2);
   assert_eq!(typed[0].lane,b6_catalogue::Lane::BinanceIncomeOriginal);
   assert_eq!(typed[0].rows.len(),5);
   assert_eq!(typed[0].rows[0].unique_key,"COMMISSION:310");
   assert_eq!(typed[0].rows[3].unique_key,"REALIZED_PNL:313");
   assert_eq!(typed[0].cursor_out.as_deref(),Some("2"));
   assert_eq!(typed[1].cursor_in.as_deref(),Some("2"));
   assert!(typed[1].last_page&&typed[1].rows.is_empty());
   assert_eq!(typed[0].body_sha256,openssl::sha::sha256(source[0].body.as_bytes())
     .iter().map(|x|format!("{x:02x}")).collect::<String>());
   assert_eq!((typed[0].from_ms,typed[0].to_ms),
      (1_780_000_000_000,1_780_000_008_000));
   let mut as_string=source.clone();
   for step in &mut as_string{
     step.body=step.body.replace("\"tranId\":310", "\"tranId\":\"310\"");
   }
   let typed_string=original_binance_income_b1_b2_as_b6_review(
      &store,state.generation,1_780_000_008_000,1_780_000_068_000,
      OkxDomain::Global,&as_string,&"c".repeat(64)).unwrap();
   assert_eq!(typed_string[0].rows[0].unique_key,"COMMISSION:310");
   assert!(original_binance_income_b1_b2_as_b6_review(
      &store,state.generation,1_780_000_008_000,1_780_000_068_000,
      OkxDomain::Global,&source[4..],&"c".repeat(64)).is_err(),
      "widened late snapshot must not be relabelled as original income");
   let mut tampered=source.clone();
   tampered[0].body=tampered[0].body.replace("310","999");
   assert!(original_binance_income_b1_b2_as_b6_review(
     &store,state.generation,1_780_000_008_000,1_780_000_068_000,
     OkxDomain::Global,&tampered,&"c".repeat(64)).is_err());
 }
 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),old);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn trade2f2i_late_funding_external_transfer_foreign_fee_asset_and_pagination_are_hard_denials(){
 use spotarb_native_market_core::native_trade2f_binance_late_income::
   inspect_owner_bound_binance_late_income_offline;
 use serde_json::json;
 let (dir,store)=b4c4_seed_account_window("f2i-late-attack");
 let (state,_)=store.record_shadow_lifetime_economics_gap(11).unwrap();
 let before=std::fs::read(dir.join("state.json")).unwrap();
 let original=f2i_full_income_tape();
 for case in 0u8..28{
   let mut t=original.clone();
   let index=if case<17{4}else{0};
   let mut j:serde_json::Value=serde_json::from_str(&t[index].body).unwrap();
   match case{
    0=>j[0]["income"]=json!("-0.003"),
    1=>j[0]["asset"]=json!("BNB"),
    2=>j[0]["asset"]=json!("BNT"),
    3=>j[0]["tradeId"]=json!("9999"),
    4=>j[0]["symbol"]=json!("ETHUSDT"),
    5=>j[0]["time"]=json!(1_780_000_002_050u64),
    6=>j[0]["tranId"]=json!(0),
    7=>j[0]["incomeType"]=json!("COMMISSION_REBATE"),
    8=>j[0]["incomeType"]=json!("TRANSFER"),
    9=>j[0]["income"]=json!("0.002"),
    10=>j[3]["income"]=json!("0.19"),
    11=>j[4]["income"]=json!("-0.021"),
    12=>j[4]["tradeId"]=json!("1001"),
    13=>j[4]["asset"]=json!("BNB"),
    14=>j[3]["tradeId"]=json!("F1"),
    15=>j[0]["time"]=json!(1_780_000_077_000u64),
    16=>{j.as_array_mut().unwrap().push(json!({
       "incomeType":"FUNDING_FEE","tranId":315,"symbol":"BNTUSDT",
       "tradeId":"","income":"0.001","asset":"USDT",
       "time":1_780_000_050_000u64}));},
    17=>{let duplicate=j[0].clone();j.as_array_mut().unwrap().push(duplicate);},
    18=>{t[0].http_status=429;},
    19=>{t[0].body="not json".into();},
    20=>{t[0].path_and_query.push_str("&symbol=BNTUSDT");},
    21=>{t.remove(1);},
    22=>{t.pop();},
    23=>{t.push(original[0].clone());},
    24=>{t[5].body=json!([{
       "incomeType":"FUNDING_FEE","tranId":315,"symbol":"BNTUSDT",
       "tradeId":"","income":"-0.001","asset":"USDT",
       "time":1_780_000_052_000u64}]).to_string();},
    25=>{t[6].body=json!([]).to_string();},
    26=>{j[0]["income"]=json!("0.0000000000001");},
    27=>{j[0]["asset"]=json!("USDC");},
    _=>unreachable!(),
   }
   if !matches!(case,18|19|20|21|22|23|24|25){
      t[index].body=j.to_string();
   }
   let result=inspect_owner_bound_binance_late_income_offline(
      &store,state.generation,1_780_000_008_000,1_780_000_068_000,&t);
   assert!(result.is_err(),"unsafe late income mock case {case} accepted");
   assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),before);
 }
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn trade2f2i_wrong_generation_and_unknown_original_partial_position_are_zero_get_denied(){
 use spotarb_native_market_core::native_trade2f_binance_late_income::{
 inspect_owner_bound_binance_late_income_offline,F2IError};
 let (dir,store)=b4c4_seed_account_window("f2i-stale-before-read");
 let (state,_)=store.record_shadow_lifetime_economics_gap(11).unwrap();
 assert!(matches!(inspect_owner_bound_binance_late_income_offline(
   &store,state.generation-1,1_780_000_008_000,1_780_000_068_000,
   &[]),Err(F2IError::Deny("Native generation stale before GET"))));
 std::fs::remove_dir_all(dir).unwrap();
 let (dir,store,_)=b4c1_open_and_candidate("f2i-c6-partial");
 let s=store.load().unwrap().unwrap();
 assert!(matches!(inspect_owner_bound_binance_late_income_offline(
   &store,s.generation,1_780_000_008_000,1_780_000_068_000,&[]),
   Err(F2IError::Deny("original four-leg Native owner preflight denied BEFORE GET"))));
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn trade2f2i_native_owner_change_between_income_reads_denied_without_release(){
 use spotarb_native_market_core::native_trade2f_binance_late_income::{
    inspect_owner_bound_binance_late_income_debug_after_initial,F2IError};
 let (dir,store)=b4c4_seed_account_window("f2i-owner-race");
 let (state,_)=store.record_shadow_lifetime_economics_gap(11).unwrap();
 let mut calls=0;
 let result=inspect_owner_bound_binance_late_income_debug_after_initial(
   &store,state.generation,1_780_000_008_000,1_780_000_068_000,
   &f2i_full_income_tape(),||{
       calls+=1;
       let (modified,_)=store.transact(state.generation,|s|{
          s.legacy_closed_positions_ignored+=1;Ok(())
       }).unwrap();
       assert_eq!(modified.generation,state.generation+1);
   });
 assert_eq!(calls,1);
 assert!(matches!(result,Err(F2IError::Deny(
   "Native owner CAS advanced after initial Binance scan"))));
 assert_eq!(store.load().unwrap().unwrap().generation,state.generation+1);
 std::fs::remove_dir_all(dir).unwrap();
}


// TRADE2F3 STAGE A — reuse REAL existing F2G/H and I MOCK fixtures; no
// synthetic replacement parser, no outside-crafted subreport accepted.
#[test]
fn trade2f3_unified_offline_original_four_legs_both_venues_consistent_no_authority(){
 use spotarb_native_market_core::{
   native_trade2f3_unified_offline_gate::{
     inspect_original_owner_unified_offline,OfflineDisposition},
   private_rest::OkxDomain,
 };
 let _numeric=F2GNumericFixtureGuard::new();
 let (dir,store)=b4c4_seed_account_window("trade2f3-combined-okx-binance");
 let (state,_)=store.record_shadow_lifetime_economics_gap(11).unwrap();
 let original=std::fs::read(dir.join("state.json")).unwrap();
 let report=inspect_original_owner_unified_offline(
  &store,state.generation,1_780_000_008_000,1_780_000_068_000,
  OkxDomain::Global,&f2g_complete_signed_mock_tape(),
  &f2h_settlement_tape(),&f2i_full_income_tape()).unwrap();
 assert_eq!(report.sealed_original_generation,state.generation);
 assert_eq!(report.okx_original_and_late_mock_gets,20);
 assert_eq!(report.binance_original_and_late_mock_gets,8);
 assert_eq!(report.original_linked_okx_bills,2);
 assert_eq!(report.bounded_binance_income_rows,5);
 assert!(report.both_venue_mock_windows_consistent);
 assert_eq!(report.disposition,OfflineDisposition::MockOnlyManualNoAuthority);
 assert!(!report.exchange_authenticated
   &&!report.history_and_delayed_postings_complete
   &&!report.third_asset_fee_quote_valued
   &&!report.independent_wallet_and_position_flatness
   &&!report.real_ready&&!report.verified_closed
   &&!report.may_send_order_cancel_transfer_or_release);
 assert_eq!(report.definitive_lifetime_net_pnl_e12,None);
 #[cfg(feature="offline-b6-review")]
 {
  use spotarb_native_market_core::native_trade2f3_source_provenance::
      inspect_widened_late_source_scopes_without_network;
  let f=f2g_complete_signed_mock_tape();
  let h=f2h_settlement_tape();
  let i=f2i_full_income_tape();
  let source=inspect_widened_late_source_scopes_without_network(
   &store,state.generation,1_780_000_008_000,1_780_000_068_000,
   OkxDomain::Global,&f,&h,&i).unwrap();
  assert_eq!(source.len(),3);
  assert_eq!(source[0].intended_lane,b6_catalogue::Lane::OkxBaseDelayed);
  assert_eq!(source[1].intended_lane,b6_catalogue::Lane::OkxQuoteDelayed);
  assert_eq!(source[2].intended_lane,b6_catalogue::Lane::BinanceIncomeDelayed);
  for slot in &source{
   assert_eq!(slot.owner_generation,state.generation);
   assert_eq!(slot.actual_get_from_ms,1_780_000_000_000);
   assert_eq!(slot.logical_late_cutoff_ms,1_780_000_008_000);
   assert_eq!(slot.actual_get_to_ms,1_780_000_068_000);
   assert_eq!(slot.mock_new_late_rows,0);
   assert!(!slot.may_pass_as_b6_delayed_page&&!slot.exchange_authenticated);
   assert!(slot.raw_response_sha256.iter().all(|h|h.len()==64));
  }
  assert_eq!(source[1].raw_response_sha256[0],
   openssl::sha::sha256(h[1].body.as_bytes()).iter()
   .map(|x|format!("{x:02x}")).collect::<String>());
  let mut false_late_okx=h.clone();
  false_late_okx[0].path_and_query=false_late_okx[0].path_and_query
    .replace("begin=1780000000000","begin=1780000008000");
  assert!(inspect_widened_late_source_scopes_without_network(
   &store,state.generation,1_780_000_008_000,1_780_000_068_000,
   OkxDomain::Global,&f,&false_late_okx,&i).is_err());
  let mut false_late_income=i.clone();
  false_late_income[4].path_and_query=false_late_income[4].path_and_query
    .replace("startTime=1780000000000","startTime=1780000008000");
  assert!(inspect_widened_late_source_scopes_without_network(
   &store,state.generation,1_780_000_008_000,1_780_000_068_000,
   OkxDomain::Global,&f,&h,&false_late_income).is_err());
 }
 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),original);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn trade2f3_unified_offline_bad_native_generation_or_partial_c6_denied_before_any_mock_get(){
 use spotarb_native_market_core::{
   native_trade2f3_unified_offline_gate::{
     inspect_original_owner_unified_offline,UnifiedOfflineError},
   private_rest::OkxDomain,
 };
 let _numeric=F2GNumericFixtureGuard::new();
 let (dir,store)=b4c4_seed_account_window("trade2f3-generation");
 let (state,_)=store.record_shadow_lifetime_economics_gap(11).unwrap();
 assert!(matches!(inspect_original_owner_unified_offline(
  &store,state.generation-1,1_780_000_008_000,1_780_000_068_000,
  OkxDomain::Global,&[],&[],&[]),
  Err(UnifiedOfflineError::Deny("unified owner generation stale BEFORE GET"))));
 std::fs::remove_dir_all(dir).unwrap();
 let (dir,store,_)=b4c1_open_and_candidate("trade2f3-partial");
 let state=store.load().unwrap().unwrap();
 assert!(matches!(inspect_original_owner_unified_offline(
  &store,state.generation,1_780_000_008_000,1_780_000_068_000,
  OkxDomain::Global,&[],&[],&[]),
  Err(UnifiedOfflineError::Deny(
      "unified full four-leg Native owner denied BEFORE GET"))));
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn trade2f3_unified_offline_single_venue_fault_never_promoted_to_success(){
 use spotarb_native_market_core::{
  native_trade2f3_unified_offline_gate::{
     inspect_original_owner_unified_offline,UnifiedOfflineError},
  private_rest::OkxDomain,
 };
 use serde_json::json;
 let _numeric=F2GNumericFixtureGuard::new();
 let (dir,store)=b4c4_seed_account_window("trade2f3-one-venue-bad");
 let (state,_)=store.record_shadow_lifetime_economics_gap(11).unwrap();
 let original=std::fs::read(dir.join("state.json")).unwrap();
 let f=f2g_complete_signed_mock_tape();
 let h=f2h_settlement_tape();
 let i=f2i_full_income_tape();
 // OKX late bill fee changed while Binance is perfectly valid.
 let mut bad_h=h.clone();
 let mut j:serde_json::Value=serde_json::from_str(&bad_h[1].body).unwrap();
 j["data"][0]["fee"]=json!("-0.008");
 bad_h[1].body=j.to_string();
 assert!(matches!(inspect_original_owner_unified_offline(
  &store,state.generation,1_780_000_008_000,1_780_000_068_000,
  OkxDomain::Global,&f,&bad_h,&i),Err(UnifiedOfflineError::Okx(_))));
 // Binance extra late income while OKX is perfectly valid.
 let mut bad_i=i.clone();
 let mut j:serde_json::Value=serde_json::from_str(&bad_i[4].body).unwrap();
 j.as_array_mut().unwrap().push(json!({
   "incomeType":"FUNDING_FEE","tranId":315,"symbol":"BNTUSDT",
   "tradeId":"","income":"0.001","asset":"USDT",
   "time":1_780_000_050_000u64}));
 bad_i[4].body=j.to_string();
 assert!(matches!(inspect_original_owner_unified_offline(
  &store,state.generation,1_780_000_008_000,1_780_000_068_000,
  OkxDomain::Global,&f,&h,&bad_i),Err(UnifiedOfflineError::Binance(_))));
 // Different fee denomination with no independently proved FX quote.
 let mut fx=i.clone();
 let mut j:serde_json::Value=serde_json::from_str(&fx[0].body).unwrap();
 j[0]["asset"]=json!("BNB");
 fx[0].body=j.to_string();
 assert!(matches!(inspect_original_owner_unified_offline(
  &store,state.generation,1_780_000_008_000,1_780_000_068_000,
  OkxDomain::Global,&f,&h,&fx),Err(UnifiedOfflineError::Binance(_))));
 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),original);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn trade2f3_unified_offline_native_owner_race_between_venues_denies(){
 use spotarb_native_market_core::{
  native_trade2f3_unified_offline_gate::{
     inspect_original_owner_unified_offline_debug_after_okx,UnifiedOfflineError},
  private_rest::OkxDomain,
 };
 let _numeric=F2GNumericFixtureGuard::new();
 let (dir,store)=b4c4_seed_account_window("trade2f3-cross-venue-cas");
 let (state,_)=store.record_shadow_lifetime_economics_gap(11).unwrap();
 let mut calls=0u8;
 let out=inspect_original_owner_unified_offline_debug_after_okx(
  &store,state.generation,1_780_000_008_000,1_780_000_068_000,
  OkxDomain::Global,&f2g_complete_signed_mock_tape(),
  &f2h_settlement_tape(),&f2i_full_income_tape(),||{
    calls+=1;
    let (changed,_)=store.transact(state.generation,|s|{
       s.legacy_closed_positions_ignored+=1;Ok(())
    }).unwrap();
    assert_eq!(changed.generation,state.generation+1);
  });
 assert_eq!(calls,1);
 assert!(matches!(out,Err(UnifiedOfflineError::Deny(
    "Native owner changed during OKX->Binance evidence handoff"))));
 assert_eq!(store.load().unwrap().unwrap().generation,state.generation+1);
 std::fs::remove_dir_all(dir).unwrap();
}


// TRADE2F3 Stage B1: provenance is byte integrity relative to a caller
// supplied offline manifest, NEVER proof of real exchange TLS/source.
#[test]
fn trade2f3b1_manifest_from_both_existing_venue_tapes_binds_unified_owner_without_trade_authority(){
 use spotarb_native_market_core::{
   native_trade2f3_source_provenance::{
    build_offline_capture_manifest,inspect_unified_with_offline_capture,
    CaptureLane},
   private_rest::OkxDomain,
 };
 let _numeric=F2GNumericFixtureGuard::new();
 let (dir,store)=b4c4_seed_account_window("trade2f3b1-source-manifest");
 let (state,_)=store.record_shadow_lifetime_economics_gap(11).unwrap();
 let before=std::fs::read(dir.join("state.json")).unwrap();
 let okx=f2g_complete_signed_mock_tape();
 let late=f2h_settlement_tape();
 let binance=f2i_full_income_tape();
 let m=build_offline_capture_manifest(state.generation,
   1_780_000_008_000,1_780_000_068_000,OkxDomain::Global,
   &okx,&late,&binance).unwrap();
 assert_eq!(m.count(),28);
 assert_eq!(m.records()[0].seq(),1);
 assert_eq!(m.records()[14].lane(),CaptureLane::OkxDelayed);
 assert_eq!(m.records()[20].lane(),CaptureLane::BinanceOriginalAndDelayed);
 assert_eq!(m.records()[0].endpoint(),"/api/v5/trade/fills-history");
 assert_eq!(m.head_sha256().len(),64);
 assert!(!m.independent_exchange_authentication_proven()
   &&!m.independently_anchored_antirollback_proven()
   &&!m.can_mark_verified_closed_or_authorize_trading());
 let result=inspect_unified_with_offline_capture(
   &store,state.generation,1_780_000_008_000,
   1_780_000_068_000,OkxDomain::Global,&m,&okx,&late,&binance).unwrap();
 assert!(result.local_transcript_bytes_stable);
 assert_eq!(result.manifest_head_sha256,m.head_sha256());
 assert!(!result.independently_authenticated_exchange_source
   &&!result.externally_anchored_antirollback
   &&!result.may_trade_cancel_transfer_or_mark_closed
   &&!result.unified.real_ready
   &&!result.unified.verified_closed
   &&result.unified.definitive_lifetime_net_pnl_e12.is_none());
 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),before);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn trade2f3b1_manifest_byte_tamper_wrong_lane_order_generation_and_cutoffs_fail_closed(){
 use spotarb_native_market_core::{
   native_trade2f3_source_provenance::{
    build_offline_capture_manifest,verify_offline_capture_manifest,
    inspect_unified_with_offline_capture},
   private_rest::OkxDomain,
 };
 let _numeric=F2GNumericFixtureGuard::new();
 let (dir,store)=b4c4_seed_account_window("trade2f3b1-tamper");
 let (state,_)=store.record_shadow_lifetime_economics_gap(11).unwrap();
 let before=std::fs::read(dir.join("state.json")).unwrap();
 let okx=f2g_complete_signed_mock_tape();
 let late=f2h_settlement_tape();
 let binance=f2i_full_income_tape();
 let manifest=build_offline_capture_manifest(state.generation,
  1_780_000_008_000,1_780_000_068_000,OkxDomain::Global,
  &okx,&late,&binance).unwrap();
 let verify=|a:&[_],h:&[_],b:&[_]|{
   verify_offline_capture_manifest(&manifest,state.generation,
     1_780_000_008_000,1_780_000_068_000,OkxDomain::Global,
     a,h,b)
 };
 let mut mutated=okx.clone();
 mutated[0].body.push(' ');
 assert!(verify(&mutated,&late,&binance).is_err());
 let mut changed=late.clone();
 changed[0].body.push('\n');
 assert!(verify(&okx,&changed,&binance).is_err());
 let mut changed=binance.clone();
 changed[7].body.push(' ');
 assert!(verify(&okx,&late,&changed).is_err());
 let mut changed=okx.clone();
 changed.swap(0,1);
 assert!(verify(&changed,&late,&binance).is_err());
 let mut changed=binance.clone();
 changed.pop();
 assert!(verify(&okx,&late,&changed).is_err());
 assert!(verify_offline_capture_manifest(&manifest,
   state.generation+1,1_780_000_008_000,1_780_000_068_000,
   OkxDomain::Global,&okx,&late,&binance).is_err());
 assert!(verify_offline_capture_manifest(&manifest,
   state.generation,1_780_000_008_000,1_780_000_068_001,
   OkxDomain::Global,&okx,&late,&binance).is_err());
 assert!(verify_offline_capture_manifest(&manifest,
   state.generation,1_780_000_008_000,1_780_000_068_000,
   OkxDomain::Eea,&okx,&late,&binance).is_err());
 let mut changed=okx.clone();
 changed[0].body.push(' ');
 assert!(inspect_unified_with_offline_capture(
   &store,state.generation,1_780_000_008_000,
   1_780_000_068_000,OkxDomain::Global,&manifest,
   &changed,&late,&binance).is_err());
 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),before);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn trade2f3b1_manifest_denies_foreign_or_mutating_path_duplicate_query_body_and_missing_lanes(){
 use spotarb_native_market_core::{
  native_trade2f3_source_provenance::build_offline_capture_manifest,
  private_rest::OkxDomain,
 };
 let okx=f2g_complete_signed_mock_tape();
 let late=f2h_settlement_tape();
 let binance=f2i_full_income_tape();
 let build=|o:&[_],l:&[_],b:&[_]|{
    build_offline_capture_manifest(
       12,1_780_000_008_000,1_780_000_068_000,OkxDomain::Global,
       o,l,b)
 };
 for variant in 0..20{
  let mut o=okx.clone();
  let mut l=late.clone();
  let mut b=binance.clone();
  match variant{
    0=>o[0].path_and_query="/api/v5/trade/order?instType=SPOT".into(),
    1=>o[0].path_and_query="https://evil.example/".into(),
    2=>o[0].path_and_query="//evil.example/fapi/v1/income".into(),
    3=>o[0].path_and_query.push_str("&password=TEST"),
    4=>o[0].path_and_query.push_str("&ordId=999"),
    5=>o[0].path_and_query="/api/v5/trade/fills-history?instType=SPOT".into(),
    6=>o[0].path_and_query="/api/v5/trade/fills-history?instType=SPOT#frag".into(),
    7=>o[0].http_status=500,
    8=>o[0].body.clear(),
    9=>o[0].body="x".repeat(1_048_577),
    10=>l[0].path_and_query=o[0].path_and_query.clone(),
    11=>l[0].path_and_query="/api/v5/trade/fills-history?instType=SPOT&instId=BNT-USDT&ordId=201&limit=100".into(),
    12=>b[0].path_and_query="/fapi/v1/order?startTime=1&endTime=2&page=1&limit=10".into(),
    13=>b[0].path_and_query.push_str("&incomeType=COMMISSION"),
    14=>b[0].path_and_query.push_str("&signature=evil"),
    15=>b[0].path_and_query="/fapi/v1/income?page=1&limit=1000".into(),
    16=>b[0].http_status=429,
    17=>b[0].path_and_query.push('\n'),
    18=>o.clear(),
    19=>b.clear(),
    _=>unreachable!(),
  }
  assert!(build(&o,&l,&b).is_err(),"untrusted capture {variant} accepted");
 }
}
#[test]
fn trade2f3b1_rewritten_tape_and_manifest_can_both_match_but_never_authenticate_exchange(){
 use spotarb_native_market_core::{
   native_trade2f3_source_provenance::{
     build_offline_capture_manifest,inspect_unified_with_offline_capture},
   private_rest::OkxDomain,
 };
 let _numeric=F2GNumericFixtureGuard::new();
 let (dir,store)=b4c4_seed_account_window("trade2f3b1-rewritten-unauth");
 let (state,_)=store.record_shadow_lifetime_economics_gap(11).unwrap();
 let okx=f2g_complete_signed_mock_tape();
 let late=f2h_settlement_tape();
 let original=f2i_full_income_tape();
 let mut rewritten=original.clone();
 // Same financial JSON facts with arbitrarily reserialized bytes: a fake
 // maker can compute a NEW internally valid hash chain from the new bytes.
 for step in &mut rewritten{
   let json:serde_json::Value=serde_json::from_str(&step.body).unwrap();
   step.body=serde_json::to_string_pretty(&json).unwrap();
 }
 let first=build_offline_capture_manifest(state.generation,
  1_780_000_008_000,1_780_000_068_000,
  OkxDomain::Global,&okx,&late,&original).unwrap();
 let forged=build_offline_capture_manifest(state.generation,
  1_780_000_008_000,1_780_000_068_000,
  OkxDomain::Global,&okx,&late,&rewritten).unwrap();
 assert_ne!(first.head_sha256(),forged.head_sha256());
 let result=inspect_unified_with_offline_capture(
  &store,state.generation,1_780_000_008_000,
  1_780_000_068_000,OkxDomain::Global,&forged,
  &okx,&late,&rewritten).unwrap();
 assert!(result.local_transcript_bytes_stable);
 assert!(!result.independently_authenticated_exchange_source
   &&!result.externally_anchored_antirollback
   &&!result.may_trade_cancel_transfer_or_mark_closed);
 std::fs::remove_dir_all(dir).unwrap();
}


// TRADE2F3 Stage B2: prepare owner-bound private GET scopes, but NEVER execute.
// Existing HTTPS allowlist remains unchanged: OKX fills-history and
// Binance userTrades permitted by transport policy, but income and OKX
// bills-archive denied. Allowlisted never means privately authorized.
#[test]
fn trade2f3b2_owner_bound_private_read_plan_ten_scopes_no_capability(){
 use spotarb_native_market_core::{
  native_trade2f3_private_read_plan::{
     inspect_private_history_plan_without_network,
     PlannedKind,PlannedWindow,PrivateReadDisposition},
  private_rest::OkxDomain,
 };
 let _numeric=F2GNumericFixtureGuard::new();
 let (dir,store)=b4c4_seed_account_window("f3b2-owner-plan");
 let (state,_)=store.record_shadow_lifetime_economics_gap(11).unwrap();
 let old=std::fs::read(dir.join("state.json")).unwrap();
 let plan=inspect_private_history_plan_without_network(
   &store,state.generation,1_780_000_008_000,1_780_000_068_000,
   OkxDomain::Global).unwrap();
 assert_eq!(plan.owner_generation(),state.generation);
 assert_eq!(plan.owner_sha256().len(),64);
 assert_eq!(plan.first_end(),1_780_000_008_000);
 assert_eq!(plan.late_end(),1_780_000_068_000);
 assert_eq!(plan.slots().len(),10);
 assert_eq!(plan.slots()[0].kind(),PlannedKind::OkxBuyFills);
 assert_eq!(plan.slots()[1].kind(),PlannedKind::OkxSellFills);
 assert_eq!(plan.slots()[0].selector(),"201");
 assert_eq!(plan.slots()[1].selector(),"901");
 assert_eq!(plan.slots()[0].max_pages(),4);
 assert_eq!(plan.slots()[0].page_size(),100);
 assert!(plan.slots()[0].transport_get_currently_allowlisted());
 assert!(plan.slots()[1].transport_get_currently_allowlisted());
 assert!(plan.slots()[0].query_symbol().is_none());
 assert_eq!(plan.slots()[2].kind(),PlannedKind::BinanceShortOpenFills);
 assert_eq!(plan.slots()[3].kind(),PlannedKind::BinanceShortCloseFills);
 assert_eq!(plan.slots()[2].selector(),"601");
 assert_eq!(plan.slots()[3].selector(),"902");
 for idx in 2..4{
   assert_eq!(plan.slots()[idx].endpoint(),"/fapi/v1/userTrades");
   assert_eq!(plan.slots()[idx].query_symbol(),Some("BNTUSDT"));
   assert_eq!(plan.slots()[idx].page_size(),1000);
   assert_eq!(plan.slots()[idx].max_pages(),4);
   assert_eq!(plan.slots()[idx].window(),PlannedWindow::Original);
   assert!(plan.slots()[idx].transport_get_currently_allowlisted());
 }
 for idx in 4..8{
   assert_eq!(plan.slots()[idx].kind(),PlannedKind::OkxAccountBills);
   assert_eq!(plan.slots()[idx].query_symbol(),None);
   assert!(!plan.slots()[idx].transport_get_currently_allowlisted());
 }
 assert_eq!(plan.slots()[4].selector(),"original:BNT");
 assert_eq!(plan.slots()[5].selector(),"original:USDT");
 assert_eq!(plan.slots()[6].selector(),"delayed:BNT");
 assert_eq!(plan.slots()[7].selector(),"delayed:USDT");
 for idx in 8..10{
   assert_eq!(plan.slots()[idx].kind(),PlannedKind::BinanceIncome);
   assert_eq!(plan.slots()[idx].page_size(),1000);
   assert_eq!(plan.slots()[idx].query_symbol(),None);
   assert!(!plan.slots()[idx].transport_get_currently_allowlisted());
 }
 assert_eq!(plan.slots()[8].window(),PlannedWindow::Original);
 assert_eq!(plan.slots()[9].window(),PlannedWindow::Delayed);
 assert_eq!(plan.disposition(),PrivateReadDisposition::UnapprovedDesignOnly);
 assert!(!plan.can_connect_private_exchange()&&!plan.api_key_permissions_verified()
    &&!plan.independently_authenticated_account_data()
    &&!plan.external_antirollback_proven()
    &&!plan.may_trade_cancel_transfer_or_mark_closed());
 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),old);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn p101_original_okx_bill_pages_must_bind_actual_b2_owner_generation(){
 use spotarb_native_market_core::{
  native_trade2f3_source_provenance::require_owner_bound_original_okx_b1_b8_bill_parity_without_network as inspect,
  native_trade2f_income_window::ObservedOkxBill,
  native_trade2f_receipt_binding::{LocalB8OriginalBillShape,LocalB8BillLane},
  fill_economics::signed_decimal_to_atoms as decimal,
  private_rest::OkxDomain,
 };
 let _numeric=F2GNumericFixtureGuard::new();
 let (dir,store)=b4c4_seed_account_window("p101-b2-original-owner");
 let (owner,_)=store.record_shadow_lifetime_economics_gap(11).unwrap();
 let tape=f2g_complete_signed_mock_tape();
 let raw=vec![tape[4].clone(),tape[5].clone()];
 let sources=[("301","9001","201","-9.009",1_780_000_000_150),
              ("302","9002","901","8.991",1_780_000_007_050)];
 let native:Vec<_>=sources.iter().map(|(bill,trade,order,delta,ts)|ObservedOkxBill{
  bill_id:(*bill).into(),trade_id:(*trade).into(),order_id:(*order).into(),
  currency:"USDT".into(),signed_change_e12:decimal(delta).unwrap(),
  cash_after_e12:0,at_ms:*ts,bill_type:"2".into(),bill_sub_type:"1".into(),
  observed_signed_fee_without_sourced_ccy_e12:None,
 }).collect();
 let b8:Vec<_>=native.iter().map(|r|LocalB8OriginalBillShape{
  bill_id:r.bill_id.clone(),trade_id:r.trade_id.clone(),asset:r.currency.clone(),
  signed_delta_e12:r.signed_change_e12,lane:LocalB8BillLane::QuoteOriginal,
 }).collect();
 let check=|gen,first,body:&[_],receipts:&[_]| inspect(
  &store,gen,first,1_780_000_068_000,OkxDomain::Global,
  body,receipts,&b8);
 assert!(check(owner.generation,1_780_000_008_000,&raw,&native).is_ok());
 assert!(check(owner.generation+1,1_780_000_008_000,&raw,&native).is_err());
 assert!(check(owner.generation,1_780_000_007_999,&raw,&native).is_err());
 let mut wrong=raw.clone();
 wrong[0].path_and_query=wrong[0].path_and_query.replace("begin=1780000000000","begin=1780000000001");
 assert!(check(owner.generation,1_780_000_008_000,&wrong,&native).is_err());
 let mut foreign=native.clone();foreign[0].order_id="999".into();
 let mut matching_fake=raw.clone();
 let mut row:serde_json::Value=serde_json::from_str(&matching_fake[1].body).unwrap();
 row["data"][1]["ordId"]=serde_json::json!("999");
 matching_fake[1].body=row.to_string();
 assert!(spotarb_native_market_core::native_trade2f3_source_provenance::
   require_original_okx_b1_b8_bill_parity_without_network(
      "BNT",&matching_fake,&foreign,&b8).is_ok());
 assert!(check(owner.generation,1_780_000_008_000,&matching_fake,&foreign).is_err());
 let mut shifted=raw.clone();
 for page in &mut shifted{page.path_and_query=page.path_and_query
    .replace("begin=1780000000000","begin=1780000000001");}
 assert!(check(owner.generation,1_780_000_008_000,&shifted,&native).is_err());
 #[cfg(feature="offline-b6-review")]
 {
  use spotarb_native_market_core::native_trade2f3_source_provenance::
   original_b1_b2_bill_pages_as_b6_review;
  let pages=original_b1_b2_bill_pages_as_b6_review(
   &store,owner.generation,1_780_000_008_000,1_780_000_068_000,
   OkxDomain::Global,&raw,&native,&b8,&"b".repeat(64)).unwrap();
  assert_eq!(pages.len(),2);
  assert_eq!(pages[0].lane,b6_catalogue::Lane::OkxBaseOriginal);
  assert!(pages[0].rows.is_empty());
  assert_eq!(pages[1].lane,b6_catalogue::Lane::OkxQuoteOriginal);
  assert_eq!(pages[1].rows.len(),2);
  assert_eq!(pages[1].rows[0].unique_key,"302");
  assert_eq!(pages[1].rows[1].unique_key,"301");
  assert_eq!(pages[1].rows[1].order_id.as_deref(),Some("201"));
  assert_eq!(pages[1].rows[1].symbol.as_deref(),Some("BNT-USDT"));
  let mut source_keys=pages.iter().flat_map(|p|p.rows.iter()
     .map(|r|r.unique_key.clone())).collect::<Vec<_>>();
  let mut original_keys=b8.iter().map(|r|r.bill_id.clone()).collect::<Vec<_>>();
  source_keys.sort();original_keys.sort();
  assert_eq!(source_keys,original_keys,"B6 must add no invented billId");
  assert_eq!(pages[1].generation,owner.generation);
  assert_eq!(pages[1].from_ms,1_780_000_000_000);
  assert_eq!(pages[1].to_ms,1_780_000_008_000);
  assert_eq!(pages[1].body_sha256.len(),64);
  let original_hash=openssl::sha::sha256(raw[1].body.as_bytes())
     .iter().map(|b|format!("{b:02x}")).collect::<String>();
  assert_eq!(pages[1].body_sha256,original_hash);
  assert_eq!(pages[1].account_fingerprint,"b".repeat(64));
  assert!(pages[1].last_page);
  let mut multi=vec![raw[0].clone()];
  multi[0].path_and_query=multi[0].path_and_query.replace("limit=100","limit=1");
  let orig:serde_json::Value=serde_json::from_str(&raw[1].body).unwrap();
  for idx in 0..2{
   let mut p=raw[1].clone();
   p.path_and_query=p.path_and_query.replace("limit=100","limit=1");
   if idx==1{p.path_and_query.push_str("&after=302");}
   p.body=serde_json::json!({"code":"0","data":[orig["data"][idx].clone()]}).to_string();
   multi.push(p);
  }
  let mut last=raw[1].clone();
  last.path_and_query=last.path_and_query.replace("limit=100","limit=1");
  last.path_and_query.push_str("&after=301");
  last.body=serde_json::json!({"code":"0","data":[]}).to_string();
  multi.push(last);
  let paged=original_b1_b2_bill_pages_as_b6_review(
   &store,owner.generation,1_780_000_008_000,1_780_000_068_000,
   OkxDomain::Global,&multi,&native,&b8,&"b".repeat(64)).unwrap();
  assert_eq!(paged.len(),4);
  assert_eq!(paged[1].cursor_out.as_deref(),Some("302"));
  assert_eq!(paged[2].cursor_in.as_deref(),Some("302"));
  assert_eq!(paged[2].cursor_out.as_deref(),Some("301"));
  assert_eq!(paged[3].index,2);
  assert_eq!(paged[3].cursor_in.as_deref(),Some("301"));
  assert!(paged[3].last_page);
  let mut wrong_symbol=raw.clone();
  wrong_symbol[1].body=wrong_symbol[1].body.replace("BNT-USDT","BNB-USDT");
  assert!(original_b1_b2_bill_pages_as_b6_review(
   &store,owner.generation,1_780_000_008_000,1_780_000_068_000,
   OkxDomain::Global,&wrong_symbol,&native,&b8,&"b".repeat(64)).is_err());
  assert!(original_b1_b2_bill_pages_as_b6_review(
   &store,owner.generation,1_780_000_008_000,1_780_000_068_000,
   OkxDomain::Global,&raw,&native,&b8,&"0".repeat(64)).is_err());
  assert!(original_b1_b2_bill_pages_as_b6_review(
   &store,owner.generation,1_780_000_008_000,1_780_000_068_000,
   OkxDomain::Global,&matching_fake,&foreign,&b8,&"b".repeat(64)).is_err());
 }
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn trade2f3b2_partial_or_stale_owner_fails_before_plan_and_no_keys_or_get(){
 use spotarb_native_market_core::{
   native_trade2f3_private_read_plan::{
       inspect_private_history_plan_without_network,PlanningError},
   private_rest::OkxDomain,
 };
 let _numeric=F2GNumericFixtureGuard::new();
 let (dir,store)=b4c4_seed_account_window("f3b2-stale");
 let (state,_)=store.record_shadow_lifetime_economics_gap(11).unwrap();
 assert!(matches!(inspect_private_history_plan_without_network(
  &store,state.generation-1,1_780_000_008_000,1_780_000_068_000,
  OkxDomain::Global),Err(PlanningError::Deny(
        "stale Native generation before private read-plan"))));
 std::fs::remove_dir_all(dir).unwrap();
 let (dir,store,_)=b4c1_open_and_candidate("f3b2-c6-incomplete");
 let s=store.load().unwrap().unwrap();
 assert!(matches!(inspect_private_history_plan_without_network(
  &store,s.generation,1_780_000_008_000,1_780_000_068_000,
  OkxDomain::Global),Err(PlanningError::Deny(
        "unsealed original four-leg owner cannot plan private history"))));
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn trade2f3b2_bad_windows_and_wrong_domains_never_grant_private_get(){
 use spotarb_native_market_core::{
   native_trade2f3_private_read_plan::inspect_private_history_plan_without_network,
   private_rest::OkxDomain,
 };
 let _numeric=F2GNumericFixtureGuard::new();
 let (dir,store)=b4c4_seed_account_window("f3b2-bad-window");
 let (state,_)=store.record_shadow_lifetime_economics_gap(11).unwrap();
 let old=std::fs::read(dir.join("state.json")).unwrap();
 for (first,late) in [
   (0,1_780_000_068_000),
   (1_779_999_999_999,1_780_000_068_000),
   (1_780_000_008_000,1_780_000_008_000),
   (1_780_000_008_000,1_780_000_067_999),
   (1_780_000_008_000,1_780_086_408_001),
   (1_780_000_008_000,1_780_518_400_001),
 ]{
   assert!(inspect_private_history_plan_without_network(
      &store,state.generation,first,late,OkxDomain::Global).is_err());
 }
 // Domains only alter future policy-scoping; NONE conveys authorization.
 for domain in [OkxDomain::Global,OkxDomain::Eea,OkxDomain::Us,OkxDomain::OpenApi]{
  let plan=inspect_private_history_plan_without_network(
    &store,state.generation,1_780_000_008_000,1_780_000_068_000,
    domain).unwrap();
  assert!(!plan.can_connect_private_exchange());
  assert!(!plan.independently_authenticated_account_data());
 }
 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),old);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn trade2f3b2_https_policy_probe_is_socket_free_and_rejects_history_and_mutations(){
 use spotarb_native_market_core::{
  private_https::{
     HttpsRequest,inspect_readonly_get_policy_without_network,
     TransportErrorKind},
  private_rest::HttpMethod,
  execution_adapter::Venue,
 };
 let request=|venue:Venue,url:&str|{
  let key=match venue{
    Venue::OkxSpot=>"OK-ACCESS-KEY",
    Venue::BinanceUsdm=>"X-MBX-APIKEY",
  };
  let (signed_url,headers)=match venue{
    Venue::BinanceUsdm=>{
      let separator=if url.contains('?'){"&"}else{"?"};
      (format!("{url}{separator}timestamp=1700000000000&recvWindow=10000&signature={}", "a".repeat(64)),
       vec![(key.into(),"FAKE_OFFLINE_POLICY_PROBE_KEY".into())])
    },
    Venue::OkxSpot=>(
      url.to_string(),
      vec![(key.into(),"FAKE_OFFLINE_POLICY_PROBE_KEY".into()),
       ("OK-ACCESS-SIGN".into(),format!("{}=", "A".repeat(43))),
       ("OK-ACCESS-PASSPHRASE".into(),"FAKE_PASS".into()),
       ("OK-ACCESS-TIMESTAMP".into(),"2026-10-15T12:00:00.000Z".into())])
  };
  HttpsRequest{
    method:HttpMethod::Get,url:signed_url,headers,
    body:None,credential_bearing:true,
    credential_scope:Some(venue),mutation:false,
  }
 };
 // The preflight function calls only the pure URL policy. No execute, DNS,
 // TLS or sockets are exercised by any of these probes.
 let okx_fills=request(Venue::OkxSpot,
      "https://www.okx.com/api/v5/trade/fills-history");
 assert!(inspect_readonly_get_policy_without_network(&okx_fills).is_ok());
 let okx_bills=request(Venue::OkxSpot,
      "https://www.okx.com/api/v5/account/bills-archive");
 let binance_user_trades=request(Venue::BinanceUsdm,
      "https://fapi.binance.com/fapi/v1/userTrades");
 assert!(inspect_readonly_get_policy_without_network(&binance_user_trades).is_ok());
 let binance=request(Venue::BinanceUsdm,
      "https://fapi.binance.com/fapi/v1/income");
 for v in [&okx_bills,&binance]{
  let err=inspect_readonly_get_policy_without_network(v).unwrap_err();
  assert_eq!(err.kind,TransportErrorKind::EndpointDenied);
 }
 let mut post=okx_fills.clone();
 post.method=HttpMethod::Post;
 assert_eq!(inspect_readonly_get_policy_without_network(&post).unwrap_err().kind,
   TransportErrorKind::EndpointDenied);
 let mut mutation=okx_fills.clone();
 mutation.mutation=true;
 assert!(inspect_readonly_get_policy_without_network(&mutation).is_err());
 let mut body=okx_fills.clone();
 body.body=Some("{}".into());
 assert!(inspect_readonly_get_policy_without_network(&body).is_err());
 let foreign=request(Venue::BinanceUsdm,
      "https://www.okx.com/api/v5/trade/fills-history");
 assert!(inspect_readonly_get_policy_without_network(&foreign).is_err());
 let unknown=request(Venue::BinanceUsdm,"https://fapi.binance.com/fapi/v1/order");
 // This exact GET may already be allowlisted for reconciliation, but
 // even policy PASS would not mean B2 has a private-read authorization.
 assert!(inspect_readonly_get_policy_without_network(&unknown).is_ok());
}


// ASTRA P0-02/P1-02: owner-bound fee gate smoke with the already saved and
// fully sealed synthetic Native original. Strictly OFFLINE: the existing B4
// close-mock recorded spot SELL before futures BUY, so it must NEVER be
// relabelled as a safely ordered COVER-THEN-SELL cycle. No signed GET here.
fn astra_spot_fee_rows_from_exact_original(
    buy:&spotarb_native_market_core::fill_economics::LegFillLedger,
    sell:&spotarb_native_market_core::fill_economics::LegFillLedger,
)->Vec<spotarb_native_market_core::native_trade2f_asset_fee::SpotFeeFill>{
    use spotarb_native_market_core::native_trade2f_asset_fee::SpotFeeFill;
    let mut events=Vec::new();
    for l in [buy,sell]{
        for (id,fill) in &l.fills{
            let currency=fill.fee_currency.as_deref().unwrap_or("");
            assert_eq!(currency,"USDT","current legacy sealed fixture is quote-fee only");
            let charged=fill.fee_cost_quote_atoms.expect("source fee amount required");
            assert!(charged>=0);
            events.push(SpotFeeFill{
                order_id:l.exchange_order_id.clone().unwrap(),
                trade_id:id.clone(),
                at_ms:fill.trade_time_ms.unwrap(),
                side:l.side.clone(),inst_id:l.symbol.replace('/',"-"),
                price_quote_e12:fill.price_atoms,
                gross_base_qty_e12:fill.qty_atoms,
                fee_ccy:currency.into(),signed_fee_e12:-charged,
            });
        }
    }
    events
}
#[test]
fn astra_owner_bound_fee_gate_refuses_prehedge_spot_sell_even_with_original_seals(){
    use spotarb_native_market_core::{
        native_trade2f3_owner_fee_bridge::{
            inspect_persisted_owner_fee_cycle_offline as inspect,OwnerFeeGateError,
        },
        native_trade2f3_fee_adjusted_cycle_gate::CycleGateError,
    };
    let (dir,store)=b4c4_seed_account_window("astra-owner-fee-sequence");
    let (state,_)=store.record_shadow_lifetime_economics_gap(11).unwrap();
    assert_eq!(state.generation,12);
    let entry=state.entry.as_ref().unwrap();
    let buys=&entry.spot_prehedge_evidence.as_ref().unwrap().exact_ledger;
    let sells=entry.shadow_close_receipt.as_ref().unwrap().observed_spot_sell_ledger();
    let rows=astra_spot_fee_rows_from_exact_original(buys,sells);
    let saved=std::fs::read(dir.join("state.json")).unwrap();
    let result=inspect(&store,12,&rows);
    assert!(matches!(result,Err(OwnerFeeGateError::Fee(
        CycleGateError::Mismatch("unhedged/reversed ordinary sequence: expected BUY SHORT COVER SELL")
    ))),"original sealed synthetic exit sequence must not falsely prove hedge: {result:?}");
    assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),saved);
    std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn astra_owner_bound_fee_gate_stale_generation_and_tampered_fee_source_fail_closed(){
    use spotarb_native_market_core::native_trade2f3_owner_fee_bridge::
        inspect_persisted_owner_fee_cycle_offline as inspect;
    let (dir,store)=b4c4_seed_account_window("astra-owner-fee-deny");
    let (state,_)=store.record_shadow_lifetime_economics_gap(11).unwrap();
    let e=state.entry.as_ref().unwrap();
    let sell=e.shadow_close_receipt.as_ref().unwrap().observed_spot_sell_ledger();
    let mut rows=astra_spot_fee_rows_from_exact_original(
        &e.spot_prehedge_evidence.as_ref().unwrap().exact_ledger,sell);
    let saved=std::fs::read(dir.join("state.json")).unwrap();
    assert!(inspect(&store,11,&rows).is_err(),"stale owner generation denied");
    rows[0].trade_id="FOREIGN-TAMPERED-ID".into();
    assert!(inspect(&store,12,&rows).is_err(),"source ID injection denied");
    assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),saved);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn trade2f3b1_b2_original_futures_pages_must_match_sealed_owner_before_source_acceptance(){
 use spotarb_native_market_core::{
   native_trade2f3_source_provenance::inspect_owner_bound_original_futures_pages_without_network,
   native_trade2f_okx_owner_mock::MockGetStep,
   private_rest::OkxDomain,
 };
 let _numeric=F2GNumericFixtureGuard::new();
 let (dir,store)=b4c4_seed_account_window("p101-b1-b2-original-identity");
 let (state,_)=store.record_shadow_lifetime_economics_gap(11).unwrap();
 let old=std::fs::read(dir.join("state.json")).unwrap();
 let original=f2g_complete_signed_mock_tape();
 let late=f2h_settlement_tape();
 let mk=|id:&str,from:&str,row_id:u64,symbol:&str,body_order:&str|{
    MockGetStep{
      path_and_query:format!("/fapi/v1/userTrades?symbol={symbol}&orderId={id}&fromId={from}&limit=1000&timestamp=1780000008000&recvWindow=10000&signature={}", "a".repeat(64)),
      http_status:200,
      body:serde_json::json!([{"orderId":body_order.parse::<u64>().unwrap(),"symbol":symbol,"id":row_id}]).to_string()
    }
 };
 let fills=vec![mk("601","0",1001,"BNTUSDT","601"),mk("902","0",2001,"BNTUSDT","902")];
 let inspect=|tape:&[MockGetStep],gen|{
    inspect_owner_bound_original_futures_pages_without_network(
       &store,gen,1_780_000_008_000,1_780_000_068_000,OkxDomain::Global,
       &original,&late,tape)
 };
 let r=inspect(&fills,state.generation).unwrap();
 assert_eq!((r.original_open_pages,r.original_close_pages),(1,1));
 assert_eq!(r.owner_generation,state.generation);
 assert_eq!(r.owner_sha256.len(),64);
 assert!(!r.exchange_source_authenticated && !r.can_mark_verified_closed_or_trade);
 let mut full_first=fills[0].clone();
 full_first.path_and_query=full_first.path_and_query.replace("limit=1000","limit=1");
 let mut empty_tail=mk("601","1002",1002,"BNTUSDT","601");
 empty_tail.body="[]".into();
 let two_page=vec![full_first.clone(),empty_tail.clone(),fills[1].clone()];
 let r2=inspect(&two_page,state.generation).unwrap();
 assert_eq!((r2.original_open_pages,r2.original_close_pages),(2,1));
 #[cfg(feature="offline-b6-review")]
 {
  use spotarb_native_market_core::native_trade2f3_source_provenance::
    original_binance_b1_b2_fills_as_b6_review;
  let out=original_binance_b1_b2_fills_as_b6_review(
    &store,state.generation,1_780_000_008_000,1_780_000_068_000,
    OkxDomain::Global,&original,&late,&two_page,&"c".repeat(64)).unwrap();
  assert_eq!(out.len(),3);
  assert_eq!(out[0].lane,b6_catalogue::Lane::BinanceShortOpen);
  assert_eq!(out[0].rows[0].unique_key,"1001");
  assert_eq!(out[0].cursor_out.as_deref(),Some("1002"));
  assert_eq!(out[1].cursor_in.as_deref(),Some("1002"));
  assert!(out[1].last_page);
  assert_eq!(out[2].lane,b6_catalogue::Lane::BinanceShortClose);
  assert_eq!(out[2].rows[0].unique_key,"2001");
  assert_eq!(out[2].rows[0].order_id.as_deref(),Some("902"));
  assert_eq!(out[2].rows[0].symbol.as_deref(),Some("BNTUSDT"));
  assert_eq!(out[2].generation,state.generation);
  assert_eq!(out[2].account_fingerprint,"c".repeat(64));
  assert_eq!(out[2].body_sha256.len(),64);
  let expected_hash=openssl::sha::sha256(fills[1].body.as_bytes())
     .iter().map(|b|format!("{b:02x}")).collect::<String>();
  assert_eq!(out[2].body_sha256,expected_hash);
  assert_eq!(out.iter().flat_map(|p|p.rows.iter().map(|r|r.unique_key.as_str()))
      .collect::<Vec<_>>(),vec!["1001","2001"]);
  assert!(original_binance_b1_b2_fills_as_b6_review(
    &store,state.generation,1_780_000_008_000,1_780_000_068_000,
    OkxDomain::Global,&original,&late,&two_page,&"0".repeat(64)).is_err());
  let mut forged=two_page.clone();
  forged[0].body=forged[0].body.replace("1001","9999");
  assert!(original_binance_b1_b2_fills_as_b6_review(
    &store,state.generation,1_780_000_008_000,1_780_000_068_000,
    OkxDomain::Global,&original,&late,&forged,&"c".repeat(64)).is_err());
 }
 // B1 bytes alone can hash a forged original OKX order. F2G has the
 // authoritative *offline* owner-bound OKX bill/fill checker: B1/B2
 // must call it on these exact original tape bytes, not rebuild a parser.
 let mut altered_okx=original.clone();
 altered_okx[0].path_and_query=altered_okx[0].path_and_query.replace("ordId=201","ordId=999");
 assert!(inspect_owner_bound_original_futures_pages_without_network(
    &store,state.generation,1_780_000_008_000,1_780_000_068_000,OkxDomain::Global,
    &altered_okx,&late,&fills).is_err(),
    "foreign OKX original order must fail B1/B2 bound review");
 let mut altered_body=original.clone();
 let mut fake_row:serde_json::Value=serde_json::from_str(&altered_body[0].body).unwrap();
 fake_row["data"][0]["tradeId"]=serde_json::json!("9999");
 altered_body[0].body=fake_row.to_string();
 assert!(inspect_owner_bound_original_futures_pages_without_network(
    &store,state.generation,1_780_000_008_000,1_780_000_068_000,OkxDomain::Global,
    &altered_body,&late,&fills).is_err(),
    "OKX original tradeId drift must fail B1/B2 bound review");
 let mut skipped_cursor=empty_tail.clone();
 skipped_cursor.path_and_query=skipped_cursor.path_and_query.replace("fromId=1002","fromId=1003");
 let mut still_full=mk("601","1002",1002,"BNTUSDT","601");
 still_full.path_and_query=still_full.path_and_query.replace("limit=1000","limit=1");
 let cases=vec![
   vec![full_first.clone(),fills[1].clone()], // unresolved full opening page
   vec![full_first.clone(),skipped_cursor,fills[1].clone()],
   vec![full_first.clone(),still_full,fills[1].clone()], // missing terminal tail
   vec![fills[0].clone(),empty_tail.clone(),fills[1].clone()], // page after short result
   vec![fills[1].clone(),fills[0].clone()],
   vec![fills[0].clone()],
   vec![mk("999","0",1001,"BNTUSDT","999"),fills[1].clone()],
   vec![mk("601","0",1001,"BNBUSDT","601"),fills[1].clone()],
   vec![mk("601","0",1001,"BNTUSDT","999"),fills[1].clone()],
   vec![mk("601","2",1001,"BNTUSDT","601"),fills[1].clone()],
   vec![fills[0].clone(),fills[0].clone(),fills[1].clone()],
   vec![fills[0].clone(),mk("601","1002",1001,"BNTUSDT","601"),fills[1].clone()],
   vec![fills[0].clone(),mk("601","1002",1002,"BNTUSDT","601"),
        mk("601","1003",1002,"BNTUSDT","601"),fills[1].clone()],
   vec![fills[0].clone(),fills[1].clone(),fills[0].clone()],
 ];
 for (n,tape) in cases.iter().enumerate(){
    assert!(inspect(tape,state.generation).is_err(),"foreign or non-sequential original owner tape {n}");
 }
 assert!(inspect(&fills,state.generation+1).is_err());
 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),old);
 std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn trade2f3b1_b2_h_i_must_reject_owner_forged_late_income_even_with_valid_futures_fills(){
 use spotarb_native_market_core::{
    native_trade2f3_source_provenance::{
      inspect_owner_bound_b1_b2_h_i_without_network,
      require_original_futures_trade_id_parity_without_network,
      require_original_futures_exact_economics_without_network,
    },
    native_trade2f_okx_owner_mock::MockGetStep,
    private_rest::OkxDomain,
 };
 let _numeric=F2GNumericFixtureGuard::new();
 let (dir,store)=b4c4_seed_account_window("p101-late-unified-negative");
 let (state,_)=store.record_shadow_lifetime_economics_gap(11).unwrap();
 let before=std::fs::read(dir.join("state.json")).unwrap();
 let okx=f2g_complete_signed_mock_tape();
 let late=f2h_settlement_tape();
 let mut income=f2i_full_income_tape();
 let fills=vec![
   MockGetStep{path_and_query:format!("/fapi/v1/userTrades?symbol=BNTUSDT&orderId=601&fromId=0&limit=1000&timestamp=1780000008000&recvWindow=10000&signature={}", "a".repeat(64)),http_status:200,body:serde_json::json!([{"orderId":601,"symbol":"BNTUSDT","id":1001}]).to_string()},
   MockGetStep{path_and_query:format!("/fapi/v1/userTrades?symbol=BNTUSDT&orderId=902&fromId=0&limit=1000&timestamp=1780000008000&recvWindow=10000&signature={}", "a".repeat(64)),http_status:200,body:serde_json::json!([{"orderId":902,"symbol":"BNTUSDT","id":2001}]).to_string()}
 ];
 let verified=inspect_owner_bound_b1_b2_h_i_without_network(
   &store,state.generation,1_780_000_008_000,1_780_000_068_000,
   OkxDomain::Global,&okx,&late,&income,&fills).unwrap();
 assert_eq!(verified.original.original_open_pages,1);
 assert_eq!(verified.original.original_close_pages,1);
 assert_eq!(verified.income_and_bills.original_linked_okx_bills,2);
 assert_eq!(verified.income_and_bills.bounded_binance_income_rows,5);
 assert!(verified.ten_slot_source_page_counts.iter().all(|x|*x>0));
 assert_eq!(verified.ten_slot_source_page_counts.iter().sum::<usize>(),
     okx.len()+late.len()+income.len()+fills.len());
 // Historical synthetic fixtures had F1/F2 original Native fill IDs while
 // userTrades pages claimed 1001/2001. B1 hashes alone cannot certify parity.
 assert!(require_original_futures_trade_id_parity_without_network(
     &store,state.generation,1_780_000_008_000,1_780_000_068_000,
     OkxDomain::Global,&okx,&late,&income,&fills).is_err(),
     "mismatched MOCK trade IDs must not be accepted as exact source parity");
 assert!(require_original_futures_exact_economics_without_network(
     &store,state.generation,1_780_000_008_000,1_780_000_068_000,
     OkxDomain::Global,&okx,&late,&income,&fills).is_err(),
     "synthetic source ID mismatch must deny per-fill economic attestation");
 assert!(!verified.exchange_authenticated && !verified.can_mark_verified_closed_or_trade
      && !verified.income_and_bills.real_ready);
 let mut forged:serde_json::Value=serde_json::from_str(&income[4].body).unwrap();
 forged[4]["income"]=serde_json::json!("0.00");
 income[4].body=forged.to_string();
 assert!(inspect_owner_bound_b1_b2_h_i_without_network(
  &store,state.generation,1_780_000_008_000,1_780_000_068_000,
  OkxDomain::Global,&okx,&late,&income,&fills).is_err(),
  "late Binance income must not be accepted without F2I owner proof");
 let clean=f2i_full_income_tape();
 assert!(inspect_owner_bound_b1_b2_h_i_without_network(
  &store,state.generation+1,1_780_000_008_000,1_780_000_068_000,
  OkxDomain::Global,&okx,&late,&clean,&fills).is_err());
 assert!(inspect_owner_bound_b1_b2_h_i_without_network(
  &store,state.generation,1_780_000_008_000,1_780_000_068_000,
  OkxDomain::Global,&okx,&late,&fills,&clean).is_err(),
  "wrong lane cannot be treated as valid income or fills");
 let mut altered_okx_late=late.clone();
 let mut bill:serde_json::Value=serde_json::from_str(&altered_okx_late[1].body).unwrap();
 bill["data"][0]["balChg"]=serde_json::json!("999");
 altered_okx_late[1].body=bill.to_string();
 assert!(inspect_owner_bound_b1_b2_h_i_without_network(
  &store,state.generation,1_780_000_008_000,1_780_000_068_000,
  OkxDomain::Global,&okx,&altered_okx_late,&clean,&fills).is_err(),
  "late OKX bill must remain linked to original owner and original F2G");

 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),before);
 std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn trade2f3b1_canonical_four_leg_owner_full_economics_offline(){
 use spotarb_native_market_core::{
   native_trade2f3_source_provenance::require_original_futures_exact_economics_without_network,
   native_close_readonly::{DualCloseCandidate,CloseOrderIdentity},
   native_trade2f_okx_owner_mock::MockGetStep,
   private_rest::OkxDomain,
 };
 let _numeric=F2GNumericFixtureGuard::new();
 let (dir,store)=b4b_seed_synthetic_two_venues_sized_with_ids(
   "p101-canonical-four-legs","0.09",["3001","3002"]);
 let (s,_)=store.record_origin_dual_opening_link(7).unwrap();
 let anchor=s.entry.as_ref().unwrap().origin.as_ref().unwrap().position_key().to_owned();
 let candidate=DualCloseCandidate{
   position_identity:anchor,base:"BNT".into(),created_at_ms:1_780_000_005_000,
   expected_qty_e12:decimal_to_atoms("0.09").unwrap(),
   spot:CloseOrderIdentity{symbol:"BNT/USDT".into(),client_id:"close-spot-1".into(),exchange_order_id:"901".into()},
   futures:CloseOrderIdentity{symbol:"BNTUSDT".into(),client_id:"close-future-1".into(),exchange_order_id:"902".into()},
 };
 store.record_shadow_close_identity(8,&candidate).unwrap();
 let receipt=b4c2_mock::signed_mock_close(&candidate,"ok").unwrap();
 store.record_shadow_close_receipts(9,&receipt,1_780_000_008_000).unwrap();
 let mock=b4c3_mock::Fake::new(b4c3_mock::Fault::None);
 let (okx,bin)=b4c3_mock::creds();
 store.audit_and_record_shadow_close_account_window(
  10,&receipt,&mock,&okx,&bin,1_780_000_008_000,OkxDomain::Global,10_000,10).unwrap();
 let (state,_)=store.record_shadow_lifetime_economics_gap(11).unwrap();
 let before=std::fs::read(dir.join("state.json")).unwrap();
 let entry=state.entry.as_ref().unwrap();
 let open=&entry.recovery_checkpoint.as_ref().unwrap().ledger;
 let close=receipt.futures_ledger();
 assert_eq!(open.fills.keys().map(String::as_str).collect::<Vec<_>>(),vec!["3001","3002"]);
 assert_eq!(close.fills.keys().map(String::as_str).collect::<Vec<_>>(),vec!["1001"]);
 let mk=|order:&str,ids:Vec<serde_json::Value>|MockGetStep{
   path_and_query:format!("/fapi/v1/userTrades?symbol=BNTUSDT&orderId={order}&fromId=0&limit=1000&timestamp=1780000008000&recvWindow=10000&signature={}", "a".repeat(64)),
   http_status:200,body:serde_json::Value::Array(ids).to_string()
 };
 let open_row=|id:u64,time:u64|serde_json::json!({
   "id":id,"orderId":601,"symbol":"BNTUSDT","side":"SELL","positionSide":"BOTH",
   "buyer":false,"marginAsset":"USDT","price":"100","qty":"0.045",
   "commission":"0.002","commissionAsset":"USDT","realizedPnl":"0","time":time
 });
 let close_row=serde_json::json!({
   "id":1001,"orderId":902,"symbol":"BNTUSDT","side":"BUY","positionSide":"BOTH",
   "buyer":true,"marginAsset":"USDT","price":"98","qty":"0.09",
   "commission":"0.004","commissionAsset":"USDT","realizedPnl":"0.18","time":1_780_000_007_100u64
 });
 let futures=vec![mk("601",vec![open_row(3001,1_780_000_003_050),open_row(3002,1_780_000_003_100)]),mk("902",vec![close_row])];
 let mut income=f2i_full_income_tape();
 for step in &mut income{
   let mut rows:serde_json::Value=serde_json::from_str(&step.body).unwrap();
   for row in rows.as_array_mut().unwrap(){
     if row.get("tradeId").and_then(|v|v.as_str())==Some("F1"){row["tradeId"]=serde_json::json!("3001")}
     else if row.get("tradeId").and_then(|v|v.as_str())==Some("F2"){row["tradeId"]=serde_json::json!("3002")}
   }
   step.body=rows.to_string();
 }
 let okx_original=f2g_complete_signed_mock_tape();
 let okx_late=f2h_settlement_tape();
 let strict=|f:&[MockGetStep],i:&[MockGetStep]|require_original_futures_exact_economics_without_network(
  &store,state.generation,1_780_000_008_000,1_780_000_068_000,
  OkxDomain::Global,&okx_original,&okx_late,i,f);
 let certified=strict(&futures,&income).unwrap();
 // Connect exact original B1/B2/F2G/H/I source cash to the persisted
 // bounded Native financial gap. Repeated original/delayed pages must
 // not be counted as additional fees, profits or cash movements.
 let cash_bridge=spotarb_native_market_core::native_trade2f3_source_provenance::
    inspect_b1_b2_source_bound_bounded_cash_without_network(
       &store,state.generation,1_780_000_008_000,1_780_000_068_000,
       OkxDomain::Global,&okx_original,&okx_late,&income,&futures).unwrap();
 assert_eq!(cash_bridge.unique_okx_usdt_bills,2);
 assert_eq!(cash_bridge.unique_binance_income_postings,5);
 assert_eq!(cash_bridge.deduplicated_okx_quote_delta_atoms_e12,
    -(decimal_to_atoms("0.018").unwrap() as i128));
 assert_eq!(cash_bridge.deduplicated_binance_money_delta_atoms_e12,
     decimal_to_atoms("0.152").unwrap() as i128);
 assert_eq!(cash_bridge.bounded_source_net_atoms_e12,
     decimal_to_atoms("0.134").unwrap() as i128);
 assert!(cash_bridge.equals_persisted_bounded_native_net
    && !cash_bridge.complete_lifetime_profit
    && !cash_bridge.independently_authenticated_account
    && !cash_bridge.may_trade_or_verify_closed);
 // Source-bound V1 -> fee-aware V2 diagnostic: SAME immutable first-leg,
 // policy and owner generation; a shared integer is not sufficient proof.
 let original=entry.spot_prehedge_evidence.as_ref().unwrap();
 let spot_fill=original.exact_ledger.fills.values().next().unwrap();
 let fee_source=spotarb_native_market_core::native_trade2f_asset_fee::SpotFeeFill{
    order_id:original.spot_order_id.clone(),
    trade_id:spot_fill.fill_id.clone(),
    at_ms:spot_fill.trade_time_ms.unwrap(),side:"buy".into(),
    inst_id:"BNT-USDT".into(),price_quote_e12:spot_fill.price_atoms,
    gross_base_qty_e12:spot_fill.qty_atoms,fee_ccy:"USDT".into(),
    signed_fee_e12:-spot_fill.fee_cost_quote_atoms.unwrap(),
 };
 let mut checkpoint=spotarb_native_market_core::native_trade2f3_first_leg_v2_checkpoint::
    prepare_first_leg_v2(state.generation,entry.policy_binding.as_ref().unwrap(),
       &entry.plan,original,&[fee_source]).unwrap();
 let unpinned_checkpoint=checkpoint.clone();
 spotarb_native_market_core::native_trade2f3_source_provenance::
    pin_v2_first_leg_to_existing_v1_disk_once(&store,&dir.join("state.json"),&mut checkpoint)
    .unwrap();
 assert!(checkpoint.original_v1_owner_file_sha256.is_some());
 assert!(spotarb_native_market_core::native_trade2f3_source_provenance::
    pin_v2_first_leg_to_existing_v1_disk_once(
       &store,&dir.join("state.json"),&mut checkpoint).is_err());
 let linked=|p:&spotarb_native_market_core::native_trade2f3_first_leg_v2_checkpoint::FirstLegV2CheckpointPayload|
    spotarb_native_market_core::native_trade2f3_source_provenance::
      inspect_v2_first_leg_against_strict_b1_b2_owner_without_network(
       &store,state.generation,1_780_000_008_000,1_780_000_068_000,
       OkxDomain::Global,&okx_original,&okx_late,&income,&futures,p);
 let linked_review=linked(&checkpoint).unwrap();
 // V1's immutable owner file and the fee-aware V2 sidecar must still
 // agree AFTER a fresh disk read, not just while objects share memory.
 let sidecar=dir.join("astra_p101_v2_fee_sidecar.json");
 spotarb_native_market_core::native_trade2f3_first_leg_v2_checkpoint::
    persist_v2_once(&sidecar,&checkpoint,
       spotarb_native_market_core::native_trade2f3_first_leg_v2_checkpoint::
         CheckpointFailpoint::None).unwrap();
 let cold=spotarb_native_market_core::authoritative_state::NativeStateStore::
    open(dir.join("state.json")).unwrap();
 let disk_check=|generation,path:&std::path::Path|
    spotarb_native_market_core::native_trade2f3_source_provenance::
       inspect_persisted_v2_first_leg_against_b1_b2_without_network(
          &cold,generation,1_780_000_008_000,1_780_000_068_000,
          OkxDomain::Global,&okx_original,&okx_late,&income,&futures,path);
 let disk_review=disk_check(state.generation,&sidecar).unwrap();
 let strict_pair=|path:&std::path::Path|
    spotarb_native_market_core::native_trade2f3_source_provenance::
       inspect_exact_v1_v2_disk_pair_without_network(
        &cold,&dir.join("state.json"),state.generation,
        1_780_000_008_000,1_780_000_068_000,OkxDomain::Global,
        &okx_original,&okx_late,&income,&futures,path);
 assert!(strict_pair(&sidecar).is_ok(),
    "local full-snapshot V1/V2 pair must agree after cold reopen");
 let legacy_sidecar=dir.join("legacy_unpinned_v2_sidecar.json");
 spotarb_native_market_core::native_trade2f3_first_leg_v2_checkpoint::
    persist_v2_once(&legacy_sidecar,&unpinned_checkpoint,
       spotarb_native_market_core::native_trade2f3_first_leg_v2_checkpoint::
          CheckpointFailpoint::None).unwrap();
 assert!(disk_check(state.generation,&legacy_sidecar).is_ok(),
    "older independent V2 checkpoint remains valid as a sidecar diagnostic");
 assert!(strict_pair(&legacy_sidecar).is_err(),
    "legacy checkpoint with no full V1 owner pin cannot certify the pair");
 // A separately checksum-valid same-generation V1 file can retain first
 // leg/policy/plan while changing another durable owner field. Old per-
 // sidecar check accepted it; strict full-pair reader must refuse it.
 let saved_v1=std::fs::read(dir.join("state.json")).unwrap();
 let mut divergent=state.clone();
 divergent.legacy_closed_positions_ignored+=1;
 let divergent_bytes=serde_json::to_vec(&divergent).unwrap();
 let digest=openssl::sha::sha256(&divergent_bytes);
 let digest_hex=digest.iter().map(|b|format!("{b:02x}")).collect::<String>();
 // Preserve full u128 fixed-point amounts; serde_json::Value would reject
 // them as Number out of range in this synthetic owner fixture.
 let mut divergent_env=b"{\"payload\":".to_vec();
 divergent_env.extend_from_slice(&divergent_bytes);
 divergent_env.extend_from_slice(format!(",\"payload_sha256\":\"{digest_hex}\"}}").as_bytes());
 std::fs::write(dir.join("state.json"),&divergent_env).unwrap();
 assert!(cold.load().is_ok(),"changed V1 file still has valid local checksum");
 assert!(disk_check(state.generation,&sidecar).is_ok(),
    "old per-sidecar checker does not pin all V1 snapshot bytes");
 assert!(strict_pair(&sidecar).is_err(),
    "V2 must reject a different checksum-valid V1 owner snapshot");
 std::fs::write(dir.join("state.json"),&saved_v1).unwrap();
 assert!(strict_pair(&sidecar).is_ok());
 assert!(disk_review.v1_v2_first_leg_bytes_equal
    && !disk_review.owner_v2_migration_certified
    && !disk_review.trading_or_verified_close_authorized);
 // Cold V1 is durable, but V2 is not acknowledged in each failpoint.
 // NEVER infer V2 completion from a valid local checksum alone.
 use spotarb_native_market_core::native_trade2f3_first_leg_v2_checkpoint::{
    persist_v2_once as persist_review_v2,CheckpointFailpoint as V2Crash,
 };
 for (file,crash,file_present) in [
    ("v2_before_create.json",V2Crash::BeforeCreate,false),
    ("v2_half_written.json",V2Crash::AfterIncompleteWrite,true),
    ("v2_unsynced_directory.json",V2Crash::AfterFileSyncBeforeDirSync,true),
 ]{
    let stalled=dir.join(file);
    assert!(persist_review_v2(&stalled,&checkpoint,crash).is_err());
    assert_eq!(stalled.exists(),file_present);
    assert!(disk_check(state.generation,&stalled).is_err(),
        "incomplete V2 must not be accepted across cold V1 reopen");
    if file_present{
       let marker=dir.join(format!("{file}.incomplete"));
       assert!(marker.exists(),"crash must retain durable V2 quarantine");
       assert!(persist_review_v2(&stalled,&checkpoint,V2Crash::None).is_err(),
          "must never retry-overwrite interrupted original V2 checkpoint");
    }
 }
 assert!(!dir.join("astra_p101_v2_fee_sidecar.json.incomplete").exists(),
    "confirmed V2 should remove and sync its pending marker");
 assert!(disk_check(state.generation+1,&sidecar).is_err(),
    "foreign V1 generation cannot accept an already persisted V2 sidecar");
 assert!(disk_check(state.generation,&dir.join("missing_v2.json")).is_err());
 let sidecar_good=std::fs::read(&sidecar).unwrap();
 let mut corrupted=sidecar_good.clone();
 let corruption_offset=corrupted.len()/2;
 corrupted[corruption_offset]^=1;
 std::fs::write(&sidecar,&corrupted).unwrap();
 assert!(disk_check(state.generation,&sidecar).is_err(),
    "changed persisted sidecar must fail closed");
 std::fs::write(&sidecar,&sidecar_good).unwrap();
 assert!(disk_check(state.generation,&sidecar).is_ok());
 assert!(linked_review.v1_v2_first_leg_bytes_equal);
 assert!(!linked_review.owner_v2_migration_certified
    && !linked_review.independent_exchange_source_authenticated
    && !linked_review.trading_or_verified_close_authorized);
 let mut foreign=checkpoint.clone();
 foreign.original.spot_client_id="other-position".into();
 assert!(linked(&foreign).is_err());
 let mut foreign=checkpoint.clone();
 foreign.original_native_generation+=1;
 assert!(linked(&foreign).is_err());
 let mut foreign=checkpoint.clone();
 foreign.source_shaped_fee_fills[0].signed_fee_e12=-1;
 assert!(linked(&foreign).is_err());
 assert!(certified.ten_slot_source_page_counts.iter().all(|n|*n>0));
 assert!(!certified.exchange_authenticated && !certified.can_mark_verified_closed_or_trade);
 let mut wrong_pnl=futures.clone();
 let mut rows:serde_json::Value=serde_json::from_str(&wrong_pnl[1].body).unwrap();
 rows[0]["realizedPnl"]=serde_json::json!("0.17");
 wrong_pnl[1].body=rows.to_string();
 assert!(strict(&wrong_pnl,&income).is_err());
 let mut wrong_fee=futures.clone();
 let mut rows:serde_json::Value=serde_json::from_str(&wrong_fee[0].body).unwrap();
 rows[0]["commission"]=serde_json::json!("0.003");
 wrong_fee[0].body=rows.to_string();
 assert!(strict(&wrong_fee,&income).is_err());
 let mut wrong_trade=futures.clone();
 let mut rows:serde_json::Value=serde_json::from_str(&wrong_trade[0].body).unwrap();
 rows[0]["id"]=serde_json::json!(9999);
 wrong_trade[0].body=rows.to_string();
 assert!(strict(&wrong_trade,&income).is_err());
 assert!(require_original_futures_exact_economics_without_network(
  &store,state.generation+1,1_780_000_008_000,1_780_000_068_000,
  OkxDomain::Global,&okx_original,&okx_late,&income,&futures).is_err());
 let mut wrong_income=income.clone();
 let mut body:serde_json::Value=serde_json::from_str(&wrong_income[4].body).unwrap();
 body[3]["income"]=serde_json::json!("0.17");
 wrong_income[4].body=body.to_string();
 assert!(strict(&futures,&wrong_income).is_err());
 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),before);
 std::fs::remove_dir_all(dir).unwrap();
}


/// Independently processes the same durable V1 state after the writer PID
/// actually terminates. This worker deliberately has no real network/API.
#[test]
fn p101_v1_v2_process_death_worker(){
 use spotarb_native_market_core::{
   authoritative_state::NativeStateStore,
   native_trade2f3_first_leg_v2_checkpoint::{
      prepare_first_leg_v2,persist_v2_once,load_v2,CheckpointFailpoint,
   },
   native_trade2f_asset_fee::SpotFeeFill,
 };
 let Ok(mode)=std::env::var("SPOTARB_P101_V1V2_PID_MODE") else{return};
 let dir=std::path::PathBuf::from(std::env::var_os("SPOTARB_P101_V1V2_PID_DIR").unwrap());
 let store=NativeStateStore::open(dir.join("state.json")).unwrap();
 let state=store.load().unwrap().unwrap();
 let entry=state.entry.as_ref().unwrap();
 let policy=entry.policy_binding.as_ref().unwrap();
 let first=entry.spot_prehedge_evidence.as_ref().unwrap();
 let target=dir.join(format!("v2_{}.json",std::env::var("SPOTARB_P101_V1V2_CASE").unwrap()));
 if let Some(stage)=mode.strip_prefix("write:"){
   let fill=first.exact_ledger.fills.values().next().unwrap();
   let fee=SpotFeeFill{
     order_id:first.spot_order_id.clone(),trade_id:fill.fill_id.clone(),
     at_ms:fill.trade_time_ms.unwrap(),side:"buy".into(),
     inst_id:"BNT-USDT".into(),price_quote_e12:fill.price_atoms,
     gross_base_qty_e12:fill.qty_atoms,fee_ccy:"USDT".into(),
     signed_fee_e12:-fill.fee_cost_quote_atoms.unwrap(),
   };
   let checkpoint=prepare_first_leg_v2(state.generation,policy,&entry.plan,first,&[fee]).unwrap();
   let fp=match stage{
     "before_create"=>CheckpointFailpoint::BeforeCreate,
     "partial"=>CheckpointFailpoint::AfterIncompleteWrite,
     "before_dir_sync"=>CheckpointFailpoint::AfterFileSyncBeforeDirSync,
     "committed"=>CheckpointFailpoint::None,
     _=>panic!("unrecognized cross-PID V2 crash phase"),
   };
   let result=persist_v2_once(&target,&checkpoint,fp);
   if stage=="committed"{assert!(result.is_ok())}else{assert!(result.is_err())}
   // Real child process exit; no destructors, cleanup, recovery or retries.
   std::process::exit(if stage=="committed"{0}else{78});
 }
 if let Some(expected)=mode.strip_prefix("read:"){
    let before=std::fs::read(dir.join("state.json")).unwrap();
    let result=load_v2(&target,state.generation,policy,&entry.plan,first);
    let marker=std::path::PathBuf::from(format!("{}.incomplete",target.display()));
    if expected=="allow"{
       assert!(result.is_ok(),"fully committed sidecar should reopen: {result:?}");
       let (payload,_fee_preview)=result.unwrap();
       assert!(!payload.original_owner_v2_seal_migrated
          && !payload.exchange_global_fees_attested
          && !payload.may_send_or_verify_closed,
          "cross-PID local V2 checkpoint must never imply trade or close authority");
       assert!(!marker.exists());
    }else{
       assert!(result.is_err(),"interrupted sidecar must never reopen as valid");
       if expected=="quarantine"{
       assert!(marker.exists());
       assert!(persist_v2_once(&target,
         &prepare_first_leg_v2(state.generation,policy,&entry.plan,first,&[{
            let fill=first.exact_ledger.fills.values().next().unwrap();
            SpotFeeFill{
              order_id:first.spot_order_id.clone(),trade_id:fill.fill_id.clone(),
              at_ms:fill.trade_time_ms.unwrap(),side:"buy".into(),inst_id:"BNT-USDT".into(),
              price_quote_e12:fill.price_atoms,gross_base_qty_e12:fill.qty_atoms,
              fee_ccy:"USDT".into(),signed_fee_e12:-fill.fee_cost_quote_atoms.unwrap(),
            }
         }]).unwrap(),CheckpointFailpoint::None).is_err(),
         "post-crash writer must never silently overwrite missing/partial V2");
       }
    }
    assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),before);
    std::process::exit(0);
 }
 panic!("unknown P1-01 subprocess mode");
}
fn p101_v1_v2_child(dir:&std::path::Path,case:&str,mode:&str)->i32{
 use std::process::{Command,Stdio};
 Command::new(std::env::current_exe().unwrap())
    .arg("--exact").arg("p101_v1_v2_process_death_worker").arg("--nocapture")
    .env("SPOTARB_P101_V1V2_PID_MODE",mode)
    .env("SPOTARB_P101_V1V2_PID_DIR",dir)
    .env("SPOTARB_P101_V1V2_CASE",case)
    .stdout(Stdio::null()).stderr(Stdio::null())
    .status().unwrap().code().unwrap()
}
#[test]
fn trade2f3b1_v1_v2_process_death_cold_recovery_denies_incomplete_sidecars(){
 let _numeric=F2GNumericFixtureGuard::new();
 let (dir,store)=b4b_seed_synthetic_two_venues_sized_with_ids(
   "p101-v1-v2-death","0.09",["3001","3002"]);
 let before=std::fs::read(dir.join("state.json")).unwrap();
 assert!(store.load().unwrap().unwrap().entry.is_some());
 for (case,expected_exit,expected_read) in [
    ("before_create",78,"deny"),
    ("partial",78,"quarantine"),
    ("before_dir_sync",78,"quarantine"),
    ("committed",0,"allow"),
 ]{
   assert_eq!(p101_v1_v2_child(&dir,case,&format!("write:{case}")),expected_exit);
   assert_eq!(p101_v1_v2_child(&dir,case,&format!("read:{expected_read}")),0);
   assert_eq!(p101_v1_v2_child(&dir,case,&format!("read:{expected_read}")),0);
   assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),before,
      "independent readers/writers must not mutate sealed V1 owner");
 }
 std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn trade2f2i_p105_effective_funding_owned_short_vs_posted_income_time(){
 use spotarb_native_market_core::native_trade2f_binance_late_income::{
   inspect_owner_funding_effective_vs_posted_offline,FundingTimeWitness,
 };
 let (dir,store)=b4c4_seed_account_window("p105-effective-vs-posted");
 let (state,_)=store.record_shadow_lifetime_economics_gap(11).unwrap();
 let unchanged=std::fs::read(dir.join("state.json")).unwrap();
 let mut tape=f2i_full_income_tape();
 let mut w=FundingTimeWitness{
   income_tran_id:314,source_owner_generation:state.generation,
   symbol:"BNTUSDT".into(),
   signed_income_atoms_e12:-(decimal_to_atoms("0.02").unwrap() as i128),
   effective_at_ms:1_780_000_005_000,
   posted_at_ms:1_780_000_005_500,
   captured_at_ms:1_780_000_068_000,
   independent_effective_time_proven:false,
 };
 let check=|t:&[spotarb_native_market_core::native_trade2f_okx_owner_mock::MockGetStep],
            x:&[FundingTimeWitness]|
   inspect_owner_funding_effective_vs_posted_offline(
      &store,state.generation,1_780_000_008_000,1_780_000_068_000,t,x);
 let allowed=check(&tape,&[w.clone()]).unwrap();
 assert!(allowed.effective_within_full_short_exposure
    && allowed.posted_and_captured_times_distinguished
    && !allowed.independent_effective_time_proven
    && !allowed.entire_exchange_history_complete
    && !allowed.may_trade_or_mark_verified_closed);
 // Funding might POST after cover though effective during short exposure.
 // Binance income.time is posted time, not owner eligibility proof.
 for step in tape.iter_mut().step_by(2){
    let mut data:serde_json::Value=serde_json::from_str(&step.body).unwrap();
    data[4]["time"]=serde_json::json!(1_780_000_007_500u64);
    step.body=data.to_string();
 }
 w.posted_at_ms=1_780_000_007_500;
 assert!(check(&tape,&[w.clone()]).is_ok());
 for bad in [
    FundingTimeWitness{effective_at_ms:1_780_000_003_000,..w.clone()},
    FundingTimeWitness{effective_at_ms:1_780_000_007_500,..w.clone()},
    FundingTimeWitness{effective_at_ms:w.posted_at_ms+1,..w.clone()},
    FundingTimeWitness{captured_at_ms:w.posted_at_ms-1,..w.clone()},
    FundingTimeWitness{captured_at_ms:1_780_000_068_001,..w.clone()},
    FundingTimeWitness{income_tran_id:315,..w.clone()},
    FundingTimeWitness{signed_income_atoms_e12:0,..w.clone()},
    FundingTimeWitness{symbol:"ETHUSDT".into(),..w.clone()},
    FundingTimeWitness{source_owner_generation:state.generation+1,..w.clone()},
    FundingTimeWitness{independent_effective_time_proven:true,..w.clone()},
 ]{
   assert!(check(&tape,&[bad]).is_err());
 }
 assert!(check(&tape,&[]).is_err());
 assert!(check(&tape,&[w.clone(),w.clone()]).is_err());
 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),unchanged);
 std::fs::remove_dir_all(dir).unwrap();
}

#[cfg(feature="offline-b6-review")]
#[test]
fn p101_one_native_owner_assembles_original_seven_without_late_forgery(){
 use spotarb_native_market_core::{
  native_trade2f3_source_provenance::{
    inspect_owner_bound_original_seven_without_network,
    inspect_original_seven_with_widened_cash_without_network,
    inspect_owner_bound_ten_b6_raw_lanes_without_network,
    LateOnlyThreeRawTapes,OriginalSevenSourceTapes},
  native_trade2f_okx_owner_mock::MockGetStep,
  native_trade2f_income_window::ObservedOkxBill,
  native_trade2f_receipt_binding::{LocalB8OriginalBillShape,LocalB8BillLane},
  fill_economics::signed_decimal_to_atoms,
  private_rest::OkxDomain,
 };
 let _numeric=F2GNumericFixtureGuard::new();
 let (dir,store)=b4c4_seed_account_window("p101-original-seven-assembled");
 let (state,_)=store.record_shadow_lifetime_economics_gap(11).unwrap();
 let g=state.generation;
 let okx=f2g_complete_signed_mock_tape();
 let late=f2h_settlement_tape();
 let income=f2i_full_income_tape();
 let future=|order:&str,id:u64|MockGetStep{
    path_and_query:format!(
      "/fapi/v1/userTrades?symbol=BNTUSDT&orderId={order}&fromId=0&limit=1000&timestamp=1780000008000&recvWindow=10000&signature={}",
      "a".repeat(64)),
    http_status:200,body:serde_json::json!([{
      "orderId":order.parse::<u64>().unwrap(),"symbol":"BNTUSDT","id":id
    }]).to_string()
 };
 let trades=vec![future("601",1001),future("902",2001)];
 let receipts=[("301","9001","201","-9.009",1_780_000_000_150),
               ("302","9002","901","8.991",1_780_000_007_050)];
 let native:Vec<_>=receipts.iter().map(|(bill,trade,order,delta,ts)|ObservedOkxBill{
    bill_id:(*bill).into(),trade_id:(*trade).into(),order_id:(*order).into(),
    currency:"USDT".into(),signed_change_e12:signed_decimal_to_atoms(delta).unwrap(),
    cash_after_e12:0,at_ms:*ts,bill_type:"2".into(),bill_sub_type:"1".into(),
    observed_signed_fee_without_sourced_ccy_e12:None,
 }).collect();
 let b8:Vec<_>=native.iter().map(|r|LocalB8OriginalBillShape{
    bill_id:r.bill_id.clone(),trade_id:r.trade_id.clone(),asset:r.currency.clone(),
    signed_delta_e12:r.signed_change_e12,lane:LocalB8BillLane::QuoteOriginal,
 }).collect();
 let run=|generation,okx:&[_],future:&[_],okx_fp:&str,bin_fp:&str|{
    inspect_owner_bound_original_seven_without_network(
       &store,generation,1_780_000_008_000,1_780_000_068_000,
       OkxDomain::Global,OriginalSevenSourceTapes{
          okx_double_scan:okx,okx_widened:&late,
          binance_user_trades:future,binance_income:&income,
          original_okx_bills:&native,b8_original_bills:&b8,
          claimed_okx_account_fingerprint:okx_fp,
          claimed_binance_account_fingerprint:bin_fp,
       })
 };
 let good=run(g,&okx,&trades,&"b".repeat(64),&"c".repeat(64)).unwrap();
 assert_eq!(good.lane_count,7);
 assert!(good.page_count>=7);
 assert!(!good.complete_account_history_proven());
 assert!(!good.exchange_origin_proven());
 assert!(!good.may_trade_cancel_transfer_or_release());
 let cash=inspect_original_seven_with_widened_cash_without_network(
   &store,g,1_780_000_008_000,1_780_000_068_000,
   OkxDomain::Global,OriginalSevenSourceTapes{
      okx_double_scan:&okx,okx_widened:&late,
      binance_user_trades:&trades,binance_income:&income,
      original_okx_bills:&native,b8_original_bills:&b8,
      claimed_okx_account_fingerprint:&"b".repeat(64),
      claimed_binance_account_fingerprint:&"c".repeat(64),
   }).unwrap();
 assert_eq!(cash.original.lane_count,7);
 assert_eq!(cash.widened.len(),3);
 assert_eq!(cash.original_unique_okx_bills,2);
 assert_eq!(cash.original_unique_binance_postings,5);
 assert_eq!(cash.widened_new_cash_postings,0);
 assert!(!cash.is_exchange_authenticated());
 assert!(!cash.all_ten_b6_pages_complete());
 assert!(!cash.may_book_new_cash_or_trade());
 // These are actual DELAYED-ONLY signed-MOCK GETs, not F2H/F2I widened
 // owner-start reads. No exchange account authenticity can be inferred.
 let b6late=|path:String,body:serde_json::Value|MockGetStep{
   path_and_query:path,http_status:200,body:body.to_string(),
 };
 let mk_bill=|ccy:&str|format!(
   "/api/v5/account/bills-archive?ccy={ccy}&begin=1780000008000&end=1780000068000&limit=100");
 let lb=vec![b6late(mk_bill("BNT"),serde_json::json!({"code":"0","data":[]}))];
 let lq=vec![b6late(mk_bill("USDT"),serde_json::json!({"code":"0","data":[{
   "billId":"303","ccy":"USDT","instId":"BNT-USDT","balChg":"0.15",
   "ordId":"901","tradeId":"9999","ts":"1780000020000"
 }]}))];
 let mk_inc=|page:usize|format!(
   "/fapi/v1/income?startTime=1780000008000&endTime=1780000068000&page={page}&limit=1000&timestamp=1780000009000&recvWindow=10000&signature={}",
   "a".repeat(64));
 let li=vec![
   b6late(mk_inc(1),serde_json::json!([{
     "symbol":"BNTUSDT","asset":"USDT","incomeType":"COMMISSION",
     "tranId":315,"time":1_780_000_021_000u64,"income":"-0.001"
   }])),
   b6late(mk_inc(2),serde_json::json!([])),
 ];
 let merged=|gen,early_okx:&[_],base:&[_],quote:&[_],future_income:&[_]|
   inspect_owner_bound_ten_b6_raw_lanes_without_network(
     &store,gen,1_780_000_008_000,1_780_000_068_000,
     OkxDomain::Global,OriginalSevenSourceTapes{
       okx_double_scan:early_okx,okx_widened:&late,
       binance_user_trades:&trades,binance_income:&income,
       original_okx_bills:&native,b8_original_bills:&b8,
       claimed_okx_account_fingerprint:&"b".repeat(64),
       claimed_binance_account_fingerprint:&"c".repeat(64),
     },LateOnlyThreeRawTapes{
       okx_base:base,okx_quote:quote,binance_income:future_income,
       claimed_okx_account_fingerprint:&"b".repeat(64),
       claimed_binance_account_fingerprint:&"c".repeat(64),
     });
 let combined=merged(g,&okx,&lb,&lq,&li).unwrap();
 assert!(combined.local_ten_lane_shape_checked());
 assert_eq!(combined.original_source_lanes,7);
 assert_eq!(combined.delayed_source_lanes,3);
 assert_eq!(combined.local_b6_shape.lane_count,10);
 assert_eq!(combined.local_b6_shape.row_count,good.row_count+2);
 assert_eq!(combined.delayed_source_pages,4);
 assert_eq!(combined.sealed_owner_sha256.len(),64);
 assert_eq!(combined.local_transcript_sha256.len(),64);
 assert_eq!(combined.input_tapes_sha256.len(),64);
 assert_eq!(combined.raw_page_sha256.len(),combined.local_b6_shape.page_count);
 assert!(combined.raw_page_sha256.iter().all(|s|s.len()==64));
 let repeated=merged(g,&okx,&lb,&lq,&li).unwrap();
 assert_eq!(combined.local_transcript_sha256,repeated.local_transcript_sha256);
 assert_eq!(combined.input_tapes_sha256,repeated.input_tapes_sha256);
 // The previous v1 digest could not distinguish two different signed
 // GET queries that returned byte-identical financial rows. The v2
 // receipt must bind the ENTIRE request (even its dummy mock signature).
 let mut different_signed_get=li.clone();
 different_signed_get[0].path_and_query=different_signed_get[0]
    .path_and_query.replace(&"a".repeat(64),&"d".repeat(64));
 let altered=merged(g,&okx,&lb,&lq,&different_signed_get).unwrap();
 assert_eq!(combined.local_b6_shape.row_count,altered.local_b6_shape.row_count);
 assert_eq!(combined.raw_page_sha256,altered.raw_page_sha256);
 assert_ne!(combined.input_tapes_sha256,altered.input_tapes_sha256);
 assert_ne!(combined.local_transcript_sha256,altered.local_transcript_sha256);
 // Two independently caller-claimed account identifiers with identical
 // raw exchange bytes MUST NOT share the same local transcript checksum.
 // Note that neither caller claim proves independent account ownership.
 let other_claim=inspect_owner_bound_ten_b6_raw_lanes_without_network(
   &store,g,1_780_000_008_000,1_780_000_068_000,
   OkxDomain::Global,OriginalSevenSourceTapes{
     okx_double_scan:&okx,okx_widened:&late,
     binance_user_trades:&trades,binance_income:&income,
     original_okx_bills:&native,b8_original_bills:&b8,
     claimed_okx_account_fingerprint:&"d".repeat(64),
     claimed_binance_account_fingerprint:&"c".repeat(64),
   },LateOnlyThreeRawTapes{
     okx_base:&lb,okx_quote:&lq,binance_income:&li,
     claimed_okx_account_fingerprint:&"d".repeat(64),
     claimed_binance_account_fingerprint:&"c".repeat(64),
   }).unwrap();
 assert_eq!(combined.raw_page_sha256,other_claim.raw_page_sha256);
 assert_ne!(combined.input_tapes_sha256,other_claim.input_tapes_sha256);
 assert_ne!(combined.local_transcript_sha256,other_claim.local_transcript_sha256);
 assert!(!other_claim.is_independent_account_verified());
 // Keep the existing B1 strict duplicate-field check, including the
 // delayed-only original 3 lanes. Parsing BTreeMap later must not erase
 // evidence of duplicated canonical GET parameters.
 let mut duplicate_query=lq.clone();
 duplicate_query[0].path_and_query.push_str("&begin=1780000008000");
 assert!(merged(g,&okx,&lb,&duplicate_query,&li).is_err());
 let mut duplicate_query_income=li.clone();
 duplicate_query_income[0].path_and_query.push_str("&page=1");
 assert!(merged(g,&okx,&lb,&lq,&duplicate_query_income).is_err());
 assert!(!combined.is_exchange_authenticated());
 assert!(!combined.is_independent_account_verified());
 assert!(!combined.is_antirollback_anchored());
 assert!(!combined.may_post_cash_or_mark_verified_closed());
 assert!(!combined.may_trade_cancel_transfer_or_release());
 assert!(merged(g+1,&okx,&lb,&lq,&li).is_err());
 assert!(merged(g,&okx,&lb,&lq,&li[..1]).is_err());
 let mut duplicate=lq.clone();
 duplicate[0].body=duplicate[0].body.replace("303","302");
 assert!(merged(g,&okx,&lb,&duplicate,&li).is_err(),
   "same OKX billId across original and late must be denied");
 let mut double_income=li.clone();
 double_income[0].body=double_income[0].body
   .replace("315","314").replace("COMMISSION","FUNDING_FEE");
 assert!(merged(g,&okx,&lb,&lq,&double_income).is_err(),
   "same Binance incomeType:tranId across windows must be denied");
 let mut wrong_window=lq.clone();
 wrong_window[0].path_and_query=wrong_window[0].path_and_query
   .replace("begin=1780000008000","begin=1780000000000");
 assert!(merged(g,&okx,&lb,&wrong_window,&li).is_err(),
   "widened H/I query cannot pass as B6 late-only source");
 let mut wrong_symbol=lq.clone();
 wrong_symbol[0].body=wrong_symbol[0].body.replace("BNT-USDT","ETH-USDT");
 assert!(merged(g,&okx,&lb,&wrong_symbol,&li).is_err(),
    "foreign late OKX trade instrument cannot borrow original B6 owner");
 let bad_claim=inspect_owner_bound_ten_b6_raw_lanes_without_network(
    &store,g,1_780_000_008_000,1_780_000_068_000,
    OkxDomain::Global,OriginalSevenSourceTapes{
      okx_double_scan:&okx,okx_widened:&late,
      binance_user_trades:&trades,binance_income:&income,
      original_okx_bills:&native,b8_original_bills:&b8,
      claimed_okx_account_fingerprint:&"b".repeat(64),
      claimed_binance_account_fingerprint:&"c".repeat(64),
    },LateOnlyThreeRawTapes{
      okx_base:&lb,okx_quote:&lq,binance_income:&li,
      claimed_okx_account_fingerprint:&"b".repeat(64),
      claimed_binance_account_fingerprint:&"e".repeat(64),
    });
 assert!(bad_claim.is_err(),"different account claims cannot be joined");
 let mut changed_late=late.clone();
 changed_late[1].body=changed_late[1].body.replace("8.991","8.992");
 assert!(inspect_original_seven_with_widened_cash_without_network(
   &store,g,1_780_000_008_000,1_780_000_068_000,
   OkxDomain::Global,OriginalSevenSourceTapes{
      okx_double_scan:&okx,okx_widened:&changed_late,
      binance_user_trades:&trades,binance_income:&income,
      original_okx_bills:&native,b8_original_bills:&b8,
      claimed_okx_account_fingerprint:&"b".repeat(64),
      claimed_binance_account_fingerprint:&"c".repeat(64),
   }).is_err(),"rewritten repeated cash cannot be booked twice");
 assert!(run(g+1,&okx,&trades,&"b".repeat(64),&"c".repeat(64)).is_err());
 assert!(run(g,&okx,&trades,&"b".repeat(64),&"b".repeat(64)).is_err());
 assert!(run(g,&okx,&trades[..1],&"b".repeat(64),&"c".repeat(64)).is_err());
 let mut tampered=okx.clone();
 tampered[0].body=tampered[0].body.replace("9001","9999");
 assert!(run(g,&tampered,&trades,&"b".repeat(64),&"c".repeat(64)).is_err());
 let mut forged_income=income.clone();
 forged_income[0].body=forged_income[0].body.replace("310","999");
 let bad=inspect_owner_bound_original_seven_without_network(
    &store,g,1_780_000_008_000,1_780_000_068_000,
    OkxDomain::Global,OriginalSevenSourceTapes{
       okx_double_scan:&okx,okx_widened:&late,binance_user_trades:&trades,
       binance_income:&forged_income,original_okx_bills:&native,b8_original_bills:&b8,
       claimed_okx_account_fingerprint:&"b".repeat(64),
       claimed_binance_account_fingerprint:&"c".repeat(64),
    });
 assert!(bad.is_err());
 std::fs::remove_dir_all(dir).unwrap();
}

#[cfg(feature="offline-b6-review")]
#[test]
fn p101_h_i_novel_widened_rows_are_quarantined_not_booked(){
 use spotarb_native_market_core::{
    native_trade2f3_source_provenance::{
       quarantine_untrusted_widened_lane_without_network,
       VersionedWidenedQuarantineJournal,ManualOnlyDisposition,
    },
    private_rest::OkxDomain,
 };
 use b6_catalogue::Lane;
 let _numeric=F2GNumericFixtureGuard::new();
 let (dir,store)=b4c4_seed_account_window("p101-h-i-novel-manual-only");
 let (owner,_)=store.record_shadow_lifetime_economics_gap(11).unwrap();
 let g=owner.generation;
 let original=f2g_complete_signed_mock_tape();
 let late=f2h_settlement_tape();
 let income=f2i_full_income_tape();
 let test=|lane,original:&[_],later_a:&[_],later_b:&[_],generation|{
    quarantine_untrusted_widened_lane_without_network(
      &store,generation,1_780_000_008_000,1_780_000_068_000,
      OkxDomain::Global,lane,original,later_a,later_b)
 };
 let b=test(Lane::OkxBaseDelayed,&original[4..5],&late[0..1],&late[3..4],g).unwrap();
 assert!(b.candidates.is_empty());
 assert_eq!(b.repeated_original_count,0);
 assert!(!b.may_post_cash_or_mark_verified_closed());
 let mut empty=VersionedWidenedQuarantineJournal::from_first(&b).unwrap();
 assert!(empty.rows().is_empty());
 assert_eq!(empty.observe(&b).unwrap(),0);
 assert!(empty.verify().is_ok());
 assert!(!empty.can_book_cash_or_mark_verified_closed());
 let new_bill=serde_json::json!({
   "instType":"SPOT","instId":"BNT-USDT","type":"2","subType":"2",
   "ccy":"USDT","billId":"303","ordId":"901","tradeId":"9999",
   "balChg":"0.15","bal":"100.132","fee":"0",
   "ts":"1780000020000"
 });
 let mut widened_a=late[1..3].to_vec();
 let mut widened_b=late[4..6].to_vec();
 for tape in [&mut widened_a,&mut widened_b]{
   let mut json:serde_json::Value=serde_json::from_str(&tape[0].body).unwrap();
   json["data"].as_array_mut().unwrap().insert(0,new_bill.clone());
   tape[0].body=json.to_string();
 }
 let quote_original=&original[5..7];
 let q=test(Lane::OkxQuoteDelayed,quote_original,&widened_a,&widened_b,g).unwrap();
 assert_eq!(q.repeated_original_count,2);
 assert_eq!(q.candidates.len(),1);
 assert_eq!(q.candidates[0].source_key,"303");
 assert_eq!(q.candidates[0].posting_time_ms,1_780_000_020_000);
 assert!(!q.candidates[0].observed_in_early_window);
 assert_eq!(q.candidates[0].raw_row_sha256.len(),64);
 assert_eq!(q.original_page_sha256.len(),2);
 assert_eq!(q.widened_first_page_sha256.len(),2);
 assert_eq!(q.widened_first_page_sha256,q.widened_repeat_page_sha256);
 assert!(!q.is_exchange_authenticated()&&!q.proves_complete_account_history());
 assert!(!q.may_post_cash_or_mark_verified_closed()&&!q.may_send_orders());
 let mut changed=widened_a.clone();
 let mut edited:serde_json::Value=serde_json::from_str(&changed[0].body).unwrap();
 edited["data"][1]["balChg"]=serde_json::json!("8.992");
 changed[0].body=edited.to_string();
 assert!(test(Lane::OkxQuoteDelayed,quote_original,&changed,&changed,g).is_err());
 assert!(test(Lane::OkxQuoteDelayed,quote_original,&widened_a,&late[4..6],g).is_err());
 assert!(test(Lane::OkxQuoteDelayed,quote_original,&widened_a,&widened_b,g+1).is_err());
 let mut vanished=widened_a.clone();
 let mut rows:serde_json::Value=serde_json::from_str(&vanished[0].body).unwrap();
 rows["data"].as_array_mut().unwrap().pop();
 vanished[0].body=rows.to_string();
 assert!(test(Lane::OkxQuoteDelayed,quote_original,&vanished,&vanished,g).is_err());
 let mut early_a=widened_a.clone();
 let mut early_b=widened_b.clone();
 for tape in [&mut early_a,&mut early_b]{
   let mut json:serde_json::Value=serde_json::from_str(&tape[0].body).unwrap();
   json["data"][0]["ts"]=serde_json::json!("1780000007900");
   tape[0].body=json.to_string();
 }
 let early=test(Lane::OkxQuoteDelayed,quote_original,&early_a,&early_b,g).unwrap();
 assert_eq!(early.candidates.len(),1);
 assert!(early.candidates[0].observed_in_early_window);
 assert!(!early.may_post_cash_or_mark_verified_closed());
 let new_income=serde_json::json!({
   "incomeType":"COMMISSION","tranId":315,"symbol":"BNTUSDT",
   "tradeId":"F3","income":"-0.001","asset":"USDT",
   "time":1_780_000_021_000u64
 });
 let mut income_a=income[4..6].to_vec();
 let mut income_b=income[6..8].to_vec();
 for tape in [&mut income_a,&mut income_b]{
   let mut data:serde_json::Value=serde_json::from_str(&tape[0].body).unwrap();
   data.as_array_mut().unwrap().push(new_income.clone());
   tape[0].body=data.to_string();
 }
 let result=test(Lane::BinanceIncomeDelayed,&income[0..2],
   &income_a,&income_b,g).unwrap();
 assert_eq!(result.repeated_original_count,5);
 assert_eq!(result.candidates.len(),1);
 assert_eq!(result.candidates[0].source_key,"COMMISSION:315");
 assert!(!result.candidates[0].observed_in_early_window);
 assert!(!result.may_post_cash_or_mark_verified_closed());
 let mut journal=VersionedWidenedQuarantineJournal::from_first(&q).unwrap();
 assert_eq!(journal.revision(),1);
 let digest=journal.local_digest().to_owned();
 assert_eq!(journal.observe(&q).unwrap(),0);
 assert_eq!(journal.local_digest(),digest);
 assert_eq!(journal.observe(&result).unwrap(),1);
 assert_eq!(journal.rows().len(),2);
 assert_eq!(journal.observe(&result).unwrap(),0);
 assert!(!journal.can_book_cash_or_mark_verified_closed());
 assert!(!journal.exchange_authenticated());
 assert!(!journal.independently_antirollback_proven());
 let prior=journal.clone();
 let mut missing=result.clone();
 missing.candidates.clear();
 assert!(journal.observe(&missing).is_err());
 let mut revised=result.clone();
 revised.candidates[0].raw_row_sha256="a".repeat(64);
 assert!(journal.observe(&revised).is_err());
 let mut wrong_owner=result.clone();
 wrong_owner.owner_generation+=1;
 assert!(journal.observe(&wrong_owner).is_err());
 let mut swapped_original=result.clone();
 swapped_original.original_page_sha256[0]="f".repeat(64);
 assert!(journal.observe(&swapped_original).is_err());
 assert_eq!(journal,prior);
 let r0=journal.revision();
 journal.review_without_cash_authority(
   r0,"2:303",ManualOnlyDisposition::ReviewNeeded,&"d".repeat(64)).unwrap();
 assert!(journal.review_without_cash_authority(
   r0,"2:303",ManualOnlyDisposition::ExcludedFromAccounting,
   &"e".repeat(64)).is_err());
 let r1=journal.revision();
 journal.review_without_cash_authority(
   r1,"2:303",ManualOnlyDisposition::ExcludedFromAccounting,
   &"e".repeat(64)).unwrap();
 assert!(journal.review_without_cash_authority(
   journal.revision(),"2:303",ManualOnlyDisposition::Pending,
   &"f".repeat(64)).is_err());
 let persisted=journal.to_review_json().unwrap();
 let restored=VersionedWidenedQuarantineJournal::restore_review_json(&persisted).unwrap();
 assert_eq!(restored,journal);
 let mut tampered:serde_json::Value=serde_json::from_str(&persisted).unwrap();
 tampered["rows"]["2:303"]["raw_row_sha256"]=serde_json::json!("b".repeat(64));
 assert!(VersionedWidenedQuarantineJournal::restore_review_json(
   &tampered.to_string()).is_err());
 assert!(!restored.can_book_cash_or_mark_verified_closed());
 let mut shifted=income_a.clone();
 shifted[0].path_and_query=shifted[0].path_and_query
   .replace("startTime=1780000000000","startTime=1780000008000");
 assert!(test(Lane::BinanceIncomeDelayed,&income[0..2],&shifted,
   &income_b,g).is_err());
 std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn p101_b6_true_delayed_only_b2_inert_blueprint_never_forges_widened_h_i(){
 use spotarb_native_market_core::{
  native_trade2f3_private_read_plan::{
   inspect_late_only_b6_blueprint_without_network,
   DelayedOnlyLane,DelayedOnlyPagination,
  },
  private_rest::OkxDomain,
 };
 let _numeric=F2GNumericFixtureGuard::new();
 let (dir,store)=b4c4_seed_account_window("p101-b6-late-only-plan");
 let (owner,_)=store.record_shadow_lifetime_economics_gap(11).unwrap();
 let generation=owner.generation;
 let original_bytes=std::fs::read(dir.join("state.json")).unwrap();
 let first=1_780_000_008_000;
 let later=1_780_000_068_000;
 let plan=inspect_late_only_b6_blueprint_without_network(
   &store,generation,first,later,OkxDomain::Global).unwrap();
 assert_eq!(plan.owner_generation(),generation);
 assert_eq!(plan.native_owner_sha256().len(),64);
 assert!(!plan.may_send_private_get());
 assert!(!plan.proves_exchange_origin_or_account_scope());
 assert!(!plan.can_mark_verified_closed_or_trade());
 let lanes=plan.slots();
 assert_eq!(lanes.len(),3);
 assert_eq!(lanes[0].lane(),DelayedOnlyLane::OkxBaseBills);
 assert_eq!(lanes[1].lane(),DelayedOnlyLane::OkxQuoteBills);
 assert_eq!(lanes[2].lane(),DelayedOnlyLane::BinanceIncome);
 assert_eq!(lanes[0].source_currency(),Some("BNT"));
 assert_eq!(lanes[1].source_currency(),Some("USDT"));
 assert_eq!(lanes[2].source_currency(),None);
 assert_eq!(lanes[0].endpoint(),"/api/v5/account/bills-archive");
 assert_eq!(lanes[2].endpoint(),"/fapi/v1/income");
 for (i,slot) in lanes.iter().enumerate(){
   assert_eq!(slot.start_ms(),first);
   assert_eq!(slot.end_ms(),later);
   assert_eq!(slot.max_pages(),4);
   assert_eq!(slot.limit(),if i<2{100}else{1000});
   assert_eq!(slot.pagination(),if i<2{
      DelayedOnlyPagination::OkxAfterBillId
   }else{DelayedOnlyPagination::BinanceOneBasedPage});
   assert!(!slot.authorized_to_read());
 }
 for domain in [OkxDomain::Global,OkxDomain::Eea,OkxDomain::Us,OkxDomain::OpenApi]{
   let scoped=inspect_late_only_b6_blueprint_without_network(
      &store,generation,first,later,domain).unwrap();
   assert_eq!(scoped.slots(),plan.slots());
   assert!(!scoped.may_send_private_get());
 }
 assert!(inspect_late_only_b6_blueprint_without_network(
   &store,generation+1,first,later,OkxDomain::Global).is_err());
 assert!(inspect_late_only_b6_blueprint_without_network(
   &store,generation,first,first,OkxDomain::Global).is_err());
 assert!(inspect_late_only_b6_blueprint_without_network(
   &store,generation,first,later+86_400_001,OkxDomain::Global).is_err());
 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),original_bytes);
 std::fs::remove_dir_all(dir).unwrap();
}

#[cfg(feature="offline-b6-review")]
#[test]
fn p101_raw_delayed_three_requires_real_late_only_queries_not_h_i_widened(){
 use spotarb_native_market_core::{
   native_trade2f3_source_provenance::{
     inspect_late_only_three_raw_sources_without_network,LateOnlyThreeRawTapes},
   native_trade2f_okx_owner_mock::MockGetStep,private_rest::OkxDomain,
 };
 let _numeric=F2GNumericFixtureGuard::new();
 let (dir,store)=b4c4_seed_account_window("p101-delayed-three-raw");
 let (owner,_)=store.record_shadow_lifetime_economics_gap(11).unwrap();
 let g=owner.generation;
 let initial_bytes=std::fs::read(dir.join("state.json")).unwrap();
 let first=1_780_000_008_000u64;
 let late=1_780_000_068_000u64;
 let make=|url:String,body:serde_json::Value|MockGetStep{
   path_and_query:url,http_status:200,body:body.to_string(),
 };
 let bill_path=|ccy:&str|format!(
   "/api/v5/account/bills-archive?ccy={ccy}&begin={first}&end={late}&limit=100");
 let base=vec![make(bill_path("BNT"),serde_json::json!({"code":"0","data":[]}))];
 let quote=vec![make(bill_path("USDT"),serde_json::json!({"code":"0","data":[{
   "billId":"303","ccy":"USDT","instId":"BNT-USDT","balChg":"0.15",
   "ordId":"901","tradeId":"9999","ts":"1780000020000"
 }]}))];
 let income_path=|page:usize|format!(
   "/fapi/v1/income?startTime={first}&endTime={late}&page={page}&limit=1000&timestamp=1780000009000&recvWindow=10000&signature={}",
   "a".repeat(64));
 let income=vec![
   make(income_path(1),serde_json::json!([{
     "symbol":"BNTUSDT","asset":"USDT","incomeType":"COMMISSION",
     "tranId":315,"time":1_780_000_021_000u64,"income":"-0.001"
   }])),
   make(income_path(2),serde_json::json!([])),
 ];
 let run=|generation,base:&[_],quote:&[_],income:&[_]|{
   inspect_late_only_three_raw_sources_without_network(
     &store,generation,first,late,OkxDomain::Global,
     LateOnlyThreeRawTapes{
       okx_base:base,okx_quote:quote,binance_income:income,
       claimed_okx_account_fingerprint:&"b".repeat(64),
       claimed_binance_account_fingerprint:&"c".repeat(64),
     })
 };
 let shape=run(g,&base,&quote,&income).unwrap();
 assert_eq!(shape.lane_count,3);
 assert_eq!(shape.row_count,2);
 assert_eq!(shape.page_count,4);
 assert!(!shape.complete_account_history_proven());
 assert!(!shape.exchange_origin_proven());
 assert!(!shape.may_trade_cancel_transfer_or_release());
 assert!(run(g+1,&base,&quote,&income).is_err());
 assert!(run(g,&base,&quote,&income[..1]).is_err());
 let mut widened=quote.clone();
 widened[0].path_and_query=widened[0].path_and_query.replace(
    "begin=1780000008000","begin=1780000000000");
 assert!(run(g,&base,&widened,&income).is_err());
 let mut wrong_ccy=quote.clone();
 wrong_ccy[0].body=wrong_ccy[0].body.replace("USDT","BNT");
 assert!(run(g,&base,&wrong_ccy,&income).is_err());
 let mut early=quote.clone();
 early[0].body=early[0].body.replace("1780000020000","1780000007900");
 assert!(run(g,&base,&early,&income).is_err());
 let mut foreign=income.clone();
 foreign[0].body=foreign[0].body.replace("BNTUSDT","BNBUSDT");
 assert!(run(g,&base,&quote,&foreign).is_err());
 assert_eq!(std::fs::read(dir.join("state.json")).unwrap(),initial_bytes);
 std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn p106_reading_rust_config_refuses_symlinks_and_nonregular_files(){
 use std::{fs,os::unix::fs::symlink,time::{SystemTime,UNIX_EPOCH}};
 let id=SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
 let dir=std::env::temp_dir().join(format!("spotarb-p106-config-file-{}-{id}",std::process::id()));
 fs::create_dir(&dir).unwrap();
 let regular=dir.join("native.yaml");
 fs::write(&regular,CONFIG).unwrap();
 let expected=policy();
 assert_eq!(NativeTradingConfig::load(&regular).unwrap().sha256,expected.sha256);
 let link=dir.join("config-link.yaml");
 symlink(&regular,&link).unwrap();
 assert_eq!(NativeTradingConfig::load(&link).unwrap_err(),ConfigError::Io);
 let dir_link=dir.join("dir-link");
 symlink(&dir,&dir_link).unwrap();
 assert_eq!(NativeTradingConfig::load(&dir_link).unwrap_err(),ConfigError::Io);
 let invalid_dir=dir.join("directory");
 fs::create_dir(&invalid_dir).unwrap();
 assert_eq!(NativeTradingConfig::load(&invalid_dir).unwrap_err(),ConfigError::Io);
 let oversized=dir.join("huge.yaml");
 fs::write(&oversized,vec![b'x';300_000]).unwrap();
 assert_eq!(NativeTradingConfig::load(&oversized).unwrap_err(),ConfigError::Io);
 let truncated=dir.join("truncated.yaml");
 fs::write(&truncated,&CONFIG.as_bytes()[..CONFIG.len()/2]).unwrap();
 assert!(NativeTradingConfig::load(&truncated).is_err());
 let binary=dir.join("invalid-utf8.yaml");
 fs::write(&binary,[255u8,254u8,253u8]).unwrap();
 assert_eq!(NativeTradingConfig::load(&binary).unwrap_err(),ConfigError::Malformed);
 use std::os::unix::ffi::OsStrExt;
 let fifo=dir.join("pipe.yaml");
 let name=std::ffi::CString::new(fifo.as_os_str().as_bytes()).unwrap();
 assert_eq!(unsafe{libc::mkfifo(name.as_ptr(),0o600)},0);
 assert_eq!(NativeTradingConfig::load(&fifo).unwrap_err(),ConfigError::Io,
    "named pipe must be refused without a blocking read");
 assert_eq!(NativeTradingConfig::load(&dir.join("absent")).unwrap_err(),ConfigError::Io);
 fs::remove_dir_all(dir).unwrap();
}

#[test]
fn p106_risk_fields_use_exact_source_decimals_not_rounded_yaml_floats(){
 let baseline=NativeTradingConfig::parse(CONFIG).unwrap();
 assert_eq!(baseline.policy.daily_okx_fee_cap_e18,7_000_000_000_000_000_000);
 // serde_yaml stores this as an f64 on many versions, which can round
 // down to exactly 7.0. A DENY must use the original text, not f64.
 for (old,new) in [
    ("  max_okx_fees_per_day_usdt: 7.0",
     "  max_okx_fees_per_day_usdt: 7.0000000000000000000000001"),
    ("  max_daily_loss_usdt: 10.0",
     "  max_daily_loss_usdt: 10.000000000000000000000001"),
    ("  entry_bps: 42.0",
     "  entry_bps: 41.999999999999999999999999999"),
 ]{
   let config=CONFIG.replace(old,new);
   assert!(NativeTradingConfig::parse(&config).is_err(),
     "tiny excess or sub-minimum spread cannot be made safe by f64 rounding");
 }
 let tiny_fee=CONFIG.replace("  funding_bps: 0.03",
   "  funding_bps: 0.00000000000000000000000001");
 let parsed=NativeTradingConfig::parse(&tiny_fee).unwrap();
 assert_eq!(parsed.policy.fee_reserve_milli_bps,24_001,
   "any genuine sub-milli-bps cost reserves one milli-bps atom");
 let atom=CONFIG.replace("  max_daily_loss_usdt: 10.0",
   "  max_daily_loss_usdt: 0.00000000000000000000000001");
 let result=NativeTradingConfig::parse(&atom).unwrap();
 assert_eq!(result.policy.daily_loss_cap_e18,1,
    "any positive sub-e18 loss cap consumes one atomic unit");
 assert!(!result.policy.real_execution_authorized);
}

#[test]
fn p106_risk_decimal_lexer_denies_aliases_exponents_duplicates_and_missing_keys(){
 for (old,new) in [
  ("  max_okx_fees_per_day_usdt: 7.0",
   "  max_okx_fees_per_day_usdt: 7e0"),
  ("  max_okx_fees_per_day_usdt: 7.0",
   "  max_okx_fees_per_day_usdt: +7.0"),
  ("  max_okx_fees_per_day_usdt: 7.0",
   "  max_okx_fees_per_day_usdt: -0.0"),
  ("  max_okx_fees_per_day_usdt: 7.0",
   "  max_okx_fees_per_day_usdt: '7.0'"),
  ("  max_okx_fees_per_day_usdt: 7.0",
   "  max_okx_fees_per_day_usdt: &fee 7.0"),
  ("  max_okx_fees_per_day_usdt: 7.0",
   " max_okx_fees_per_day_usdt: 7.0"),
  ("  max_okx_fees_per_day_usdt: 7.0",
   "  max_okx_fees_per_day_usdt: 7.0\n  max_okx_fees_per_day_usdt: 7.0"),
  ("  max_okx_fees_per_day_usdt: 7.0",
   "  max_okx_fees_per_day_usdt: 7.0 # fine\n  max_okx_fees_per_day_usdt: 7.0"),
  ("  entry_bps: 42.0",
   "  entry_bps: 42_000"),
  ("  funding_bps: 0.03",
   "  funding_bps: .03"),
 ]{
   let text=CONFIG.replace(old,new);
   assert!(NativeTradingConfig::parse(&text).is_err(),
     "noncanonical financial YAML syntax must fail closed: {new}");
 }
 let with_comment=CONFIG.replace(
   "  max_okx_fees_per_day_usdt: 7.0",
   "  max_okx_fees_per_day_usdt: 7.0 # max permitted");
 let x=NativeTradingConfig::parse(&with_comment).unwrap();
 assert_eq!(x.policy.daily_okx_fee_cap_e18,7_000_000_000_000_000_000);
 let oversized_reserve=CONFIG.replace(
    "  round_fees_bps: 24.0","  round_fees_bps: 1001.0");
 assert!(matches!(NativeTradingConfig::parse(&oversized_reserve),
   Err(ConfigError::Policy)));
}
