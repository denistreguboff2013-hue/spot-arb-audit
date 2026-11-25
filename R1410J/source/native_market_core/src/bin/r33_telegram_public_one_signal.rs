//! R33 SINGLE PUBLIC-CANDIDATE Telegram notifier; never a trade executor.
//! Only from a fresh native PUBLIC market capture and explicit opt-in.
//! No private exchange endpoints, production DB, or live PAPER mutations.
use spotarb_native_market_core::{
 native_trading_config::NativeTradingConfig,
 native_observability_review::{NativeObservePolicy,OfflineNativeSample,ObservationKind},
 native_telegram_delivery_review::{
  deliver_one_russian_notification,PinnedTelegramTls,DeliveryResult,
 },
};
use serde_json::Value;
use std::{env,fs::{self,OpenOptions},io::Read,os::unix::fs::OpenOptionsExt,
 path::{Path,PathBuf},time::{SystemTime,UNIX_EPOCH}};
const OUTBOX_PATH:&str=
 "/mnt/spotarb_db_test/astra_p0_02_execution_gate_20261011/r33_telegram_delivery/outbox.json";
const MAX_SOURCE:u64=128*1024;

fn deny(msg:&str)->!{eprintln!("R33_PUBLIC_TG_DENY:{msg}");std::process::exit(3)}
fn now_ms()->u64{
 SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}
fn safe_source_read(path:&Path)->Value{
 let before=fs::symlink_metadata(path).unwrap_or_else(|_|deny("source_missing"));
 if !before.is_file()||before.len()==0||before.len()>MAX_SOURCE{
  deny("source_size_or_type")
 }
 let mut input=OpenOptions::new().read(true).custom_flags(
  libc::O_NOFOLLOW|libc::O_CLOEXEC).open(path)
  .unwrap_or_else(|_|deny("source_fd_denied"));
 let mut bytes=Vec::new();
 input.by_ref().take(MAX_SOURCE+1).read_to_end(&mut bytes)
  .unwrap_or_else(|_|deny("source_read_failed"));
 if bytes.len() as u64!=before.len(){deny("source_changed_while_reading")}
 serde_json::from_slice(&bytes).unwrap_or_else(|_|deny("source_invalid_json"))
}
fn native_readonly_candidate<'a>(
 root:&'a Value,cfg:&NativeTradingConfig,
)->Option<(&'a str,u128,u64)>{
 if root.get("source").and_then(Value::as_str)
  !=Some("OKX public books5 + Binance Futures public bookTicker")
  ||root.get("trading")!=Some(&Value::Bool(false))
  ||root.get("private_api_access")!=Some(&Value::Bool(false))
  ||root.get("exchange_source_authenticated")!=Some(&Value::Bool(false))
  ||root.get("production_ws_subscription_mutation")!=Some(&Value::Bool(false))
  ||root.get("config_sha256").and_then(Value::as_str)!=Some(cfg.sha256.as_str())
  ||root.get("bad_frames").and_then(Value::as_u64)!=Some(0)
  ||root.get("public_spot_ticker_invalid").and_then(Value::as_u64)!=Some(0)
  ||root.get("unexpected_disconnects").and_then(Value::as_u64)!=Some(0)
  ||root.get("public_transport_errors").and_then(Value::as_array)
    .map_or(true,|a|!a.is_empty()){
  deny("untrusted_or_mismatched_public_capture")
 }
 let qualified=root.get("qualified_local_public_market_selection")
  .unwrap_or_else(||deny("missing_native_qualified_selection"));
 if qualified.get("may_trade")!=Some(&Value::Bool(false))
  ||qualified.get("depth_and_actual_fees_proven")!=Some(&Value::Bool(false))
  ||qualified.get("volume_quote_currency").and_then(Value::as_str)!=Some("USDT"){
  deny("invalid_native_qualified_selection")
 }
 let top=qualified.get("top_candidates").and_then(Value::as_array)
  .unwrap_or_else(||deny("missing_candidates"));
 if top.is_empty(){return None}
 if top.len()>cfg.policy.top_n_per_scan{deny("excess_candidates")}
 let base=top[0].as_str().unwrap_or_else(||deny("invalid_candidate_base"));
 if base.is_empty()||base.len()>32||!base.bytes()
  .all(|b|b.is_ascii_uppercase()||b.is_ascii_digit()){deny("candidate_symbol_invalid")}
 let requested=root.get("requested_bases").and_then(Value::as_array)
  .unwrap_or_else(||deny("original_public_universe_missing"));
 if requested.is_empty()||requested.len()>8
  ||requested.iter().filter(|x|x.as_str()==Some(base)).count()!=1{
  deny("candidate_outside_original_public_universe")
 }
 let blocked=cfg.raw.get("blacklist").and_then(|v|v.as_sequence())
  .unwrap_or_else(||deny("native_yaml_blacklist_missing"));
 if blocked.len()>300{deny("native_yaml_blacklist_unbounded")}
 let mut seen=std::collections::BTreeSet::new();
 for entry in blocked{
  let word=entry.as_str().unwrap_or_else(||deny("malformed_blacklist"));
  if word.is_empty()||word.len()>32||
    !word.bytes().all(|b|b.is_ascii_uppercase()||b.is_ascii_digit())
    ||!seen.insert(word){deny("invalid_blacklist")}
  if word==base{deny("blacklisted_symbol_not_notifiable")}
 }
 let snapshots=root.get("fresh_pairs").and_then(Value::as_array)
  .unwrap_or_else(||deny("missing_fresh_pairs"));
 let matched=snapshots.iter().filter(|v|
  v.get("base").and_then(Value::as_str)==Some(base))
  .collect::<Vec<_>>();
 if matched.len()!=1{deny("missing_or_duplicated_fresh_book")}
 let spot_age=matched[0].get("spot_age_ms").and_then(Value::as_u64)
  .unwrap_or_else(||deny("missing_spot_age"));
 let fut_age=matched[0].get("futures_age_ms").and_then(Value::as_u64)
  .unwrap_or_else(||deny("missing_futures_age"));
 if spot_age>800||fut_age>800{deny("stale_public_orderbook")}
 let buy=matched[0].get("spot_ask_usdt").and_then(Value::as_f64)
  .unwrap_or_else(||deny("missing_spot_buy_quote"));
 let sell=matched[0].get("futures_bid_usdt").and_then(Value::as_f64)
  .unwrap_or_else(||deny("missing_futures_sell_quote"));
 if !buy.is_finite()||!sell.is_finite()||buy<=0.0||sell<=0.0
  ||buy>1_000_000.0||sell>1_000_000.0{
  deny("nonfinite_or_unbounded_public_prices")
 }
 let public_gross_milli_bps=((sell/buy-1.0)*10_000_000.0).round() as i128;
 let rows=root.get("public_market_gate_checks").and_then(Value::as_array)
  .unwrap_or_else(||deny("missing_market_gate"));
 let match_gate=rows.iter().filter(|v|
  v.get("base").and_then(Value::as_str)==Some(base))
  .collect::<Vec<_>>();
 if match_gate.len()!=1{deny("missing_or_duplicate_market_gate")}
 let gate=match_gate[0];
 if gate.get("local_screen_pass")!=Some(&Value::Bool(true))
  ||gate.get("reason").and_then(Value::as_str)
   !=Some("LOCAL_PUBLIC_MARKET_SIGNAL_ONLY_NOT_EXECUTABLE")
  ||gate.get("may_trade")!=Some(&Value::Bool(false))
  ||gate.get("telegram_delivery")!=Some(&Value::Bool(false))
  ||gate.get("source_authenticated")!=Some(&Value::Bool(false))
  ||gate.get("public_ticker_present")!=Some(&Value::Bool(true))
  ||gate.get("executable_depth_verified")!=Some(&Value::Bool(false))
  ||gate.get("real_fees_and_funding_verified")!=Some(&Value::Bool(false))
  ||gate.get("public_ticker_age_ms").and_then(Value::as_u64)
   .map_or(true,|age|age>30_000){
   deny("gate_did_not_admit_safe_public_preview")
 }
 let gross=gate.get("gross_spread_milli_bps").and_then(Value::as_str)
  .and_then(|v|v.parse::<i128>().ok()).unwrap_or_else(||deny("missing_gross_bps"));
 let fee=gate.get("reserved_cost_milli_bps").and_then(Value::as_str)
  .and_then(|v|v.parse::<i128>().ok()).unwrap_or_else(||deny("missing_cost_reserve"));
 let net=gate.get("indicative_net_spread_milli_bps").and_then(Value::as_str)
  .and_then(|v|v.parse::<i128>().ok()).unwrap_or_else(||deny("missing_net_bps"));
 let vol=gate.get("public_24h_quote_volume_e12").and_then(Value::as_str)
  .and_then(|v|v.parse::<u128>().ok())
  .unwrap_or_else(||deny("missing_public_quote_volume"));
 if fee<0||fee as u128!=cfg.policy.fee_reserve_milli_bps
  ||gross.checked_sub(fee)!=Some(net)
  ||(gross-public_gross_milli_bps).abs()>5
  ||vol<cfg.policy.min_market_volume_24h_quote_e18/1_000_000
  ||net<i128::try_from(cfg.policy.entry_min_milli_bps).unwrap_or(i128::MAX)
  ||net>10_000_000{deny("invalid_public_net_threshold")}
 Some((base,net as u128,spot_age.max(fut_age)))
}
fn main(){
 let capture=env::var("R33_PUBLIC_MARKET_CAPTURE")
  .unwrap_or_else(|_|deny("capture_path_not_configured"));
 let capture=PathBuf::from(capture);
 if !capture.is_absolute(){deny("relative_public_capture_denied")}
 let cfg=NativeTradingConfig::load(
  &PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config/native_trading.yaml"))
  .unwrap_or_else(|_|deny("source_yaml_denied"));
 let policy=NativeObservePolicy::from_config(&cfg)
  .unwrap_or_else(|_|deny("observability_policy_denied"));
 let source=safe_source_read(&capture);
 let Some((base,net,age))=native_readonly_candidate(&source,&cfg)else{
  println!("R33_NO_QUALIFIED_PUBLIC_SIGNAL_NO_NETWORK");
  return;
 };
 let timestamp=source.get("capture_wall_ms").and_then(Value::as_u64)
  .unwrap_or_else(||deny("capture_without_native_wall_clock"));
 let now=now_ms();
 if timestamp==0||timestamp>now||now-timestamp>30_000{
  deny("stale_public_capture")
 }
 // Absolutely no credentials are read before both native evidence
 // qualification AND an explicit one-shot notification authorization.
 if env::var("R33_TELEGRAM_SEND_ONE").ok().as_deref()!=Some("ONE_PUBLIC_SIGNAL_ONLY"){
  deny("explicit_notification_one_shot_authorization_required")
 }
 let token=env::var("R33_TELEGRAM_BOT_TOKEN")
  .unwrap_or_else(|_|deny("missing_telegram_token_env"));
 let chat=env::var("R33_TELEGRAM_CHAT_ID")
  .unwrap_or_else(|_|deny("missing_telegram_chat_env"));
 // Same read-only loopback Telegram proxy config as the old Python
 // notifier. Never fall back to direct Internet if a proxy is expected.
 let old_config=fs::read_to_string("/home/tmpadmin/spotbot/config.yaml")
  .unwrap_or_else(|_|deny("legacy_telegram_proxy_source_unavailable"));
 let old:serde_yaml::Value=serde_yaml::from_str(&old_config)
  .unwrap_or_else(|_|deny("legacy_telegram_proxy_yaml_invalid"));
 let proxy=old.get("telegram").and_then(|v|v.get("proxy_url"))
  .and_then(|v|v.as_str()).filter(|s|!s.is_empty())
  .unwrap_or_else(||deny("legacy_telegram_proxy_missing"));
 let transport=PinnedTelegramTls::new_with_local_http_proxy(token,&chat,proxy)
  .unwrap_or_else(|_|deny("invalid_telegram_auth_or_proxy"));
 let sample=OfflineNativeSample{
  observed_ms:timestamp,owner_generation:1,
  symbol:Some(base.into()),net_spread_milli_bps:Some(net),
  book_age_ms:age,decisions:source.get("total_ram_accepted_frames")
   .and_then(Value::as_u64).unwrap_or(0),
  ws_to_decision_p99_micros:0,worker_queue_p99_micros:0,
  ws_reconnects:0,worker_drops:0,fault_code:None,
 };
 let ledger=Path::new(OUTBOX_PATH);
 let parent=ledger.parent().unwrap();
 let dir=fs::symlink_metadata(parent).unwrap_or_else(|_|deny("outbox_dir_missing"));
 use std::os::unix::fs::MetadataExt;
 if !dir.is_dir()||dir.mode()&0o077!=0{
  deny("outbox_dir_not_owner_private")
 }
 match deliver_one_russian_notification(ledger,&policy,
  ObservationKind::Candidate,&sample,now,true,&transport){
  Ok(DeliveryResult::Confirmed{message_id,..})=>
    println!("R33_TELEGRAM_ACK_CONFIRMED_ID:{message_id}"),
  Ok(DeliveryResult::Suppressed{reason})=>
    println!("R33_TELEGRAM_DUPLICATE_SUPPRESSED:{reason}"),
  Ok(DeliveryResult::Uncertain{..})=>
    deny("telegram_delivery_uncertain_NO_RETRY"),
  Err(_)=>deny("telegram_local_outbox_or_auth_denied"),
 }
}
