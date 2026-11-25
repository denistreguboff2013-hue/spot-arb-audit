//! One manual user-requested Telegram TEST, never a trade signal.
//! Token is passed only through environment, never sourced from backups,
//! logged, written to disk or placed on a shell command argument.
use spotarb_native_market_core::{
 native_trading_config::NativeTradingConfig,
 native_telegram_durable_outbox::{
   claim_one_user_test_notice,confirm_telegram_ack,ClaimDecision,
 },
 native_telegram_delivery_review::{PinnedTelegramTls,TelegramSendTransport},
};
use std::{
 env,fs,
 os::unix::fs::PermissionsExt,
 path::{Path,PathBuf},
 time::{SystemTime,UNIX_EPOCH},
};

const EVENT:&str="TEST-TELEGRAM-20261015-TWO";
const MESSAGE:&str="Тестовое сообщение SPOT-ARB (Rust) №2.\nПроверяем доставку уведомлений и защиту от повторов.\nЭто не торговый сигнал. Реальная торговля отключена.";
const OUTBOX:&str="/mnt/spotarb_db_test/astra_p0_02_execution_gate_20261011/r34_telegram_test_20261015/outbox.json";
fn deny(reason:&str)->!{
 eprintln!("R35_TEST_DENIED:{reason}");
 std::process::exit(3)
}
fn now_ms()->u64{
 SystemTime::now().duration_since(UNIX_EPOCH)
  .unwrap_or_default().as_millis() as u64
}
fn main(){
 if env::var("R35_USER_APPROVED_SECOND_TEST").ok().as_deref()
   !=Some("SEND_EXACTLY_ONE_NEW_TEST_VIA_PROXY_20261015"){
  deny("explicit_single_test_authorization_missing")
 }
 let repo_path=PathBuf::from(env!("CARGO_MANIFEST_DIR"))
   .join("config/native_trading.yaml");
 let cfg=NativeTradingConfig::load(&repo_path)
  .unwrap_or_else(|_|deny("source_config_unavailable"));
 if cfg.policy.real_execution_authorized{deny("real_trade_flag_must_remain_false")}
 let path=Path::new(OUTBOX);
 let parent=path.parent().unwrap_or_else(||deny("outbox_parent_missing"));
 let meta=fs::symlink_metadata(parent)
  .unwrap_or_else(|_|deny("secure_outbox_directory_missing"));
 if !meta.is_dir()||meta.permissions().mode()&0o077!=0{
  deny("outbox_directory_must_be_owner_private")
 }
 // Validate both values before claiming the one-shot notification.
 // No secrets appear in the return type's printable diagnostics.
 let token=env::var("R35_TG_BOT_TOKEN")
  .unwrap_or_else(|_|deny("notification_token_not_supplied"));
 let chat=env::var("R35_TG_CHAT_ID")
  .unwrap_or_else(|_|deny("notification_chat_not_supplied"));
 // The legacy Python notifier uses telegram.proxy_url from the
 // current server config, NOT the empty placeholder in review YAML.
 // READ ONLY: never patch original Python config or the live PAPER.
 let old_config_text=fs::read_to_string("/home/tmpadmin/spotbot/config.yaml")
  .unwrap_or_else(|_|deny("legacy_local_telegram_proxy_config_unavailable"));
 let old_config:serde_yaml::Value=serde_yaml::from_str(&old_config_text)
  .unwrap_or_else(|_|deny("legacy_local_telegram_proxy_yaml_invalid"));
 let proxy=old_config.get("telegram")
  .and_then(|v|v.get("proxy_url")).and_then(|v|v.as_str())
  .filter(|s|!s.is_empty())
  .unwrap_or_else(||deny("legacy_local_telegram_proxy_not_configured"));
 let sender=PinnedTelegramTls::new_with_local_http_proxy(token,&chat,proxy)
  .unwrap_or_else(|_|deny("notification_auth_or_local_proxy_invalid"));
 // A failed/ambiguous network send is still a consumed event: NEVER
 // retry blindly, even after a crash or DNS/TLS/HTTP timeout.
 let decision=claim_one_user_test_notice(
  path,&cfg.sha256,EVENT,MESSAGE,now_ms()
 ).unwrap_or_else(|_|deny("durable_claim_failed"));
 let body=match decision{
  ClaimDecision::Suppressed{reason}=>{
   println!("R35_TEST_NOT_RESENT:{reason}");
   return
  },
  ClaimDecision::Claimed{body,..}=>body,
 };
 let ack=sender.send_plain_text_once(&body)
  .unwrap_or_else(|_|deny("telegram_delivery_uncertain_DO_NOT_RETRY"));
 confirm_telegram_ack(path,&cfg.sha256,EVENT,ack.message_id)
  .unwrap_or_else(|_|deny("telegram_accepted_but_local_ack_uncertain_DO_NOT_RETRY"));
 println!("R35_TEST_TELEGRAM_ACK_CONFIRMED_MESSAGE_ID:{}",ack.message_id);
 println!("R35_TEST_REAL_TRADING_UNCHANGED:true");
}
