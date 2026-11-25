//! P1-06: opt-in PAPER-only Russian notification transport adapter.
//! No keys or private exchange calls. Production systemd PAPER untouched.
//! Uses previously tested one-shot native Telegram TLS via loopback proxy,
//! durable ClaimUnknown first, and validated Bot API ACK second.
use crate::{
 native_trading_config::NativeTradingConfig,
 native_paper_telegram_observer::{PaperNotice,PaperNoticeKind},
 native_telegram_durable_outbox::{
  claim_paper_status_notice,confirm_telegram_ack,ClaimDecision,
 },
 native_telegram_delivery_review::{
  TelegramSendTransport,DeliveryError,DeliveryResult,
 },
};
use std::path::Path;

/// PAPER status notifications are ONLY an explicitly opted-in sidecar.
/// Missing authorization NEVER consumes the disk event or sends anything.
pub fn deliver_paper_status_once<T:TelegramSendTransport>(
 outbox:&Path,config:&NativeTradingConfig,note:&PaperNotice,
 now_ms:u64,operator_enabled_paper_notifications:bool,transport:&T,
)->Result<DeliveryResult,DeliveryError>{
 // R177: bind both channel flags and durable config identity to the
 // same parsed operator YAML. Mutable raw fields or claimed SHA drift
 // cannot authorize a send under a stale outbox identity.
 if !config.source_projection_unchanged()||
   !operator_enabled_paper_notifications||
   config.policy.real_execution_authorized||
   config.policy.real_daily_trades!=0{
  return Err(DeliveryError::NotAuthorized)
 }
 let telegram=config.raw.get("telegram");
 let enabled=telegram.and_then(|v|v.get("enabled"))
  .and_then(|v|v.as_bool())==Some(true);
 let channel=match note.kind{
  PaperNoticeKind::Fault=>"send_errors",
  PaperNoticeKind::VirtualActivitySummary=>"send_signals",
  PaperNoticeKind::VirtualTradeOpened=>"send_signals",
  PaperNoticeKind::VirtualTradeClosed=>"send_signals",
  PaperNoticeKind::HourlyHeartbeat=>"send_heartbeat",
  PaperNoticeKind::DailyReport=>"send_signals",
 };
 if !enabled||telegram.and_then(|v|v.get(channel))
   .and_then(|v|v.as_bool())!=Some(true){
  return Err(DeliveryError::NotAuthorized)
 }
 let claimed=claim_paper_status_notice(outbox,&config.sha256,note,now_ms)
  .map_err(DeliveryError::Outbox)?;
 let (id,body)=match claimed{
  ClaimDecision::Claimed{event_id,body}=>(event_id,body),
  ClaimDecision::Suppressed{reason}=>{
   return Ok(DeliveryResult::Suppressed{reason});
  },
 };
 let ack=match transport.send_plain_text_once(&body){
  Ok(v) if v.message_id>0=>v,
  Ok(_)=>{
   eprintln!("PAPER_TG_SEND_DIAGNOSTIC:ZERO_MESSAGE_ID");
   return Ok(DeliveryResult::Uncertain{event_id:id})
  },
  Err(kind)=>{
   // Only the bounded error VARIANT is logged. No Telegram token,
   // HTTP response, chat ID or original message text is exposed.
   eprintln!("PAPER_TG_SEND_DIAGNOSTIC:{kind:?}");
   return Ok(DeliveryResult::Uncertain{event_id:id})
  },
 };
 // Network ACK and local disk confirm are separate operations. If
 // local fsync fails, do NOT retry POST: previous claim remains UNKNOWN.
 confirm_telegram_ack(outbox,&config.sha256,&id,ack.message_id)
  .map_err(DeliveryError::Outbox)?;
 Ok(DeliveryResult::Confirmed{event_id:id,message_id:ack.message_id})
}

#[cfg(test)]
mod tests{
 use super::*;
 use crate::native_telegram_durable_outbox::{inspect_local_claim,DeliveryState};
 use crate::native_telegram_delivery_review::{
  TelegramAck,TelegramSendError,
 };
 use std::{
  fs,path::PathBuf,sync::atomic::{AtomicUsize,Ordering},
  time::{SystemTime,UNIX_EPOCH},
 };
 struct FakeSend {called:AtomicUsize,succeed:bool}
 impl TelegramSendTransport for FakeSend {
  fn send_plain_text_once(&self,text:&str)->Result<TelegramAck,TelegramSendError>{
   self.called.fetch_add(1,Ordering::SeqCst);
   assert!(text.starts_with("⚠️ Rust PAPER"));
   if self.succeed{Ok(TelegramAck{message_id:7701})}
   else{Err(TelegramSendError::Network)}
  }
 }
 fn setup(name:&str)->(PathBuf,NativeTradingConfig,PaperNotice){
  let n=SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
  let dir=std::env::temp_dir().join(format!(
   "native_paper_tg_delivery_{}_{}_{}",name,std::process::id(),n));
  fs::create_dir(&dir).unwrap();
  let config=NativeTradingConfig::parse(
   include_str!("../config/native_trading.yaml")).unwrap();
  let notice=PaperNotice{
   kind:PaperNoticeKind::Fault,
   event_id:"PFAULT-0123456789ABCDEF-1".into(),
   russian_text:"⚠️ Rust PAPER: новая ошибка наблюдения. Реальная торговля выключена; требуется проверка.".into(),
   external_delivery:false,may_trigger_real_trade:false,
   real_pnl_attested:false,
  };
  (dir.join("tg.json"),config,notice)
 }
 const NOW:u64=1_800_000_000_000;
 #[test]
 fn r177_source_drift_denied_before_transport_and_durable_claim(){
  for mutate_sha in [false,true]{
   let (path,mut config,note)=setup(if mutate_sha{"sha"}else{"raw"});
   let fake=FakeSend{called:AtomicUsize::new(0),succeed:true};
   if mutate_sha{
    config.sha256="0".repeat(64);
   }else{
    // Even a semantically unrelated post-parse edit invalidates
    // the exact operator source bound to this outbox identity.
    config.raw["general"]["bot_name"]=
     serde_yaml::Value::String("changed-after-parse".into());
   }
   assert_eq!(deliver_paper_status_once(&path,&config,&note,NOW,true,&fake),
    Err(DeliveryError::NotAuthorized));
   assert_eq!(fake.called.load(Ordering::SeqCst),0);
   assert!(!path.exists());
   fs::remove_dir_all(path.parent().unwrap()).unwrap();
  }
 }
 #[test]
 fn explicitly_disabled_no_send_no_ledger(){
  let (path,config,note)=setup("denied");
  let fake=FakeSend{called:AtomicUsize::new(0),succeed:true};
  assert_eq!(deliver_paper_status_once(&path,&config,&note,NOW,false,&fake),
   Err(DeliveryError::NotAuthorized));
  assert_eq!(fake.called.load(Ordering::SeqCst),0);
  assert!(!path.exists());
  fs::remove_dir_all(path.parent().unwrap()).unwrap();
 }
 #[test]
 fn one_paper_fault_mock_ack_then_persistent_dedup(){
  let (path,config,note)=setup("ack");
  let fake=FakeSend{called:AtomicUsize::new(0),succeed:true};
  let a=deliver_paper_status_once(&path,&config,&note,NOW,true,&fake).unwrap();
  assert_eq!(a,DeliveryResult::Confirmed{
   event_id:note.event_id.clone(),message_id:7701});
  assert_eq!(inspect_local_claim(&path,&config.sha256,&note.event_id).unwrap(),
   Some(DeliveryState::Confirmed{telegram_message_id:7701}));
  let b=deliver_paper_status_once(&path,&config,&note,NOW+60_000,true,&fake).unwrap();
  assert_eq!(b,DeliveryResult::Suppressed{
    reason:"ALREADY_CLAIMED_UNKNOWN_OR_CONFIRMED"});
  assert_eq!(fake.called.load(Ordering::SeqCst),1);
  fs::remove_dir_all(path.parent().unwrap()).unwrap();
 }
 #[test]
 fn ambiguous_network_answer_keeps_claimed_unknown_cooldown(){
  let (path,config,mut note)=setup("unknown");
  let fake=FakeSend{called:AtomicUsize::new(0),succeed:false};
  assert!(matches!(deliver_paper_status_once(
   &path,&config,&note,NOW,true,&fake).unwrap(),
   DeliveryResult::Uncertain{..}));
  assert_eq!(inspect_local_claim(&path,&config.sha256,&note.event_id).unwrap(),
   Some(DeliveryState::ClaimedUnknown));
  assert!(matches!(deliver_paper_status_once(
    &path,&config,&note,NOW+5_000,true,&fake).unwrap(),
    DeliveryResult::Suppressed{..}));
  note.event_id="PFAULT-0123456789ABCDEF-2".into();
  assert_eq!(deliver_paper_status_once(
   &path,&config,&note,NOW+60_000,true,&fake).unwrap(),
   DeliveryResult::Suppressed{reason:"PAPER_EVENT_NO_SPAM_COOLDOWN"});
  assert_eq!(fake.called.load(Ordering::SeqCst),1);
  fs::remove_dir_all(path.parent().unwrap()).unwrap();
 }
 #[test]
 fn forged_real_pnl_or_order_claim_is_denied_before_network(){
  let (path,config,mut note)=setup("forged");
  let fake=FakeSend{called:AtomicUsize::new(0),succeed:true};
  note.real_pnl_attested=true;
  assert!(deliver_paper_status_once(
    &path,&config,&note,NOW,true,&fake).is_err());
  note.real_pnl_attested=false;
  note.may_trigger_real_trade=true;
  assert!(deliver_paper_status_once(
    &path,&config,&note,NOW,true,&fake).is_err());
  assert!(!path.exists());
  assert_eq!(fake.called.load(Ordering::SeqCst),0);
  fs::remove_dir_all(path.parent().unwrap()).unwrap();
 }
}
