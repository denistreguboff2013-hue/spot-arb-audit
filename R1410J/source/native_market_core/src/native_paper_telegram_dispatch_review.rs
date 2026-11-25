//! Astra R38 — strictly select at most one fresh, current-session PAPER
//! event for an opt-in Rust Telegram sidecar. Selection never connects to
//! the Internet and never touches exchange balances or trade execution.
use crate::{
 native_paper_notice_queue_review::{PaperWatchState,R37_SCHEMA,MAX_PENDING_NOTICES},
 native_paper_telegram_observer::{PaperNotice,PaperNoticeKind},
 native_trading_config::NativeTradingConfig,
};
use serde::{Serialize,Deserialize};
use std::collections::BTreeSet;

pub const MAX_STATUS_AGE_MS:u64=180_000;
pub const MAX_FAULT_AGE_MS:u64=600_000;
pub const MAX_SUMMARY_AGE_MS:u64=600_000;

#[derive(Clone,Debug,PartialEq,Eq)]
pub enum DispatchGateError{
 NotAuthorized,InvalidState,StaleSource,Clock,UnsafeRecord,
}
#[derive(Clone,Debug,Serialize,Deserialize,PartialEq,Eq)]
pub struct DispatchPreview{
 pub source:String,
 pub latest_session:String,
 pub queue_revision:u64,
 pub total_pending:usize,
 pub stale_notices_skipped:usize,
 pub historical_session_skipped:usize,
 pub permitted_new_events:usize,
 pub candidate:Option<PaperNotice>,
 pub telegram_bot_api_called:bool,
 pub real_orders_permitted:bool,
}

fn code(s:&str)->bool{
 !s.is_empty()&&s.len()<=64&&s.bytes().all(|b|
  b.is_ascii_uppercase()||b.is_ascii_digit()||b==b'-'||b==b'_')
}
fn is_safe_notice(n:&PaperNotice)->bool{
 let prefix=match n.kind{
  PaperNoticeKind::Fault=>"PFAULT-",
  PaperNoticeKind::VirtualActivitySummary=>"PSUM-",
  PaperNoticeKind::VirtualTradeOpened=>"POPEN-",
  PaperNoticeKind::VirtualTradeClosed=>"PCLOSE-",
  PaperNoticeKind::HourlyHeartbeat=>"PBEAT-",
  PaperNoticeKind::DailyReport=>"PDAY-",
 };
 let text_prefix=match n.kind{
  PaperNoticeKind::Fault=>"⚠️ Rust PAPER",
  PaperNoticeKind::VirtualActivitySummary=>"📊 Rust PAPER",
  PaperNoticeKind::VirtualTradeOpened=>"🟢 PAPER",
  PaperNoticeKind::VirtualTradeClosed=>"🔵 PAPER",
  PaperNoticeKind::HourlyHeartbeat=>"❤️ Rust PAPER",
  PaperNoticeKind::DailyReport=>"📊 PAPER",
 };
 code(&n.event_id)&&n.event_id.starts_with(prefix)&&
 !n.external_delivery&&!n.may_trigger_real_trade&&!n.real_pnl_attested&&
 !n.russian_text.is_empty()&&n.russian_text.len()<=2048&&
 n.russian_text.starts_with(text_prefix)&&
 match n.kind{
  PaperNoticeKind::Fault=>n.russian_text.contains("Реальная торговля выключена"),
  PaperNoticeKind::VirtualActivitySummary=>n.russian_text.contains("НЕ реальная прибыль"),
  PaperNoticeKind::VirtualTradeOpened=>
    n.russian_text.contains("Исполнений на биржах НЕ было"),
  PaperNoticeKind::VirtualTradeClosed=>
    n.russian_text.contains("НЕ реальная прибыль"),
  PaperNoticeKind::HourlyHeartbeat=>
    n.russian_text.contains("Реальная торговля отключена"),
  PaperNoticeKind::DailyReport=>
    n.russian_text.contains("НЕ реальная прибыль"),
 }
}
/// Read-only preflight. Strictly no send on stale status or a faulty
/// queue, even if the operator enabled Telegram. An *empty* queue
/// returns a harmless preview and needs no Telegram credentials.
/// Optional virtual model summaries require a second separate opt-in.
pub fn select_next_native_paper_telegram(
 state:&PaperWatchState,config:&NativeTradingConfig,
 now_ms:u64,operator_send_enabled:bool,operator_virtual_summary_enabled:bool,
)->Result<DispatchPreview,DispatchGateError>{
 select_next_native_paper_telegram_with_known(
  state,config,now_ms,operator_send_enabled,operator_virtual_summary_enabled,
  &BTreeSet::new()
 )
}
/// Caller must build known event IDs from a securely checked durable
/// Telegram ledger. A previously ClaimedUnknown or Confirmed event
/// is NEVER transmitted again; select the next fresh notice instead.
pub fn select_next_native_paper_telegram_with_known(
 state:&PaperWatchState,config:&NativeTradingConfig,
 now_ms:u64,operator_send_enabled:bool,operator_virtual_summary_enabled:bool,
 already_claimed:&BTreeSet<String>,
)->Result<DispatchPreview,DispatchGateError>{
 // R177: queue selection and send share an immutable source identity.
 // Do not select under edited Telegram flags with an old config SHA.
 if !config.source_projection_unchanged()||
  !operator_send_enabled||config.policy.real_execution_authorized||
  config.policy.real_daily_trades!=0{
  return Err(DispatchGateError::NotAuthorized)
 }
 if state.schema!=R37_SCHEMA||state.revision==0||
    state.config_sha256!=config.sha256||
    state.pending.len()>MAX_PENDING_NOTICES||
    state.cursor.schema!=1||
    state.cursor.last_summary.session!=state.cursor.last_seen.session||
    state.cursor.last_summary.day!=state.cursor.last_seen.day{
  return Err(DispatchGateError::InvalidState)
 }
 let current=&state.cursor.last_seen;
 if now_ms==0||current.updated_wall_ms>now_ms.saturating_add(2_000){
  return Err(DispatchGateError::Clock)
 }
 if now_ms.saturating_sub(current.updated_wall_ms)>MAX_STATUS_AGE_MS{
  return Err(DispatchGateError::StaleSource)
 }
 let session_started_sec=current.session.rsplit('_').next()
  .and_then(|s|s.parse::<u64>().ok())
  .ok_or(DispatchGateError::InvalidState)?;
 if !current.session.starts_with("session_")||session_started_sec==0{
  return Err(DispatchGateError::InvalidState)
 }
 let session_start_ms=session_started_sec.checked_mul(1000)
  .ok_or(DispatchGateError::InvalidState)?;
 if current.updated_wall_ms<session_start_ms{
  return Err(DispatchGateError::InvalidState)
 }
 let telegram=config.raw.get("telegram").ok_or(DispatchGateError::InvalidState)?;
 let enabled=telegram.get("enabled").and_then(|v|v.as_bool())==Some(true);
 let faults_enabled=telegram.get("send_errors").and_then(|v|v.as_bool())==Some(true);
 let summaries_enabled=telegram.get("send_signals").and_then(|v|v.as_bool())==Some(true);
 let heartbeat_enabled=telegram.get("send_heartbeat").and_then(|v|v.as_bool())==Some(true);
 if !enabled{return Err(DispatchGateError::NotAuthorized)}
 let mut candidates=Vec::<(u8,u64,String,PaperNotice)>::new();
 let mut stale=0usize;
 let mut historical=0usize;
 for (id,item) in &state.pending{
  if id!=&item.notice.event_id||!is_safe_notice(&item.notice)||
    item.send_attempted||item.detected_at_wall_ms==0||
    item.detected_at_wall_ms>current.updated_wall_ms||
    item.detected_at_wall_ms>now_ms.saturating_add(2_000){
    return Err(DispatchGateError::UnsafeRecord)
  }
  if item.detected_at_wall_ms<session_start_ms{
   historical+=1;continue
  }
  let (priority,max_age,allowed)=match item.notice.kind{
   PaperNoticeKind::Fault=>(0,MAX_FAULT_AGE_MS,faults_enabled),
   PaperNoticeKind::VirtualTradeOpened=>(1,MAX_SUMMARY_AGE_MS,summaries_enabled),
   PaperNoticeKind::VirtualTradeClosed=>(1,MAX_SUMMARY_AGE_MS,summaries_enabled),
   PaperNoticeKind::DailyReport=>(2,MAX_SUMMARY_AGE_MS,summaries_enabled),
   PaperNoticeKind::HourlyHeartbeat=>(3,MAX_SUMMARY_AGE_MS,heartbeat_enabled),
   PaperNoticeKind::VirtualActivitySummary=>(4,MAX_SUMMARY_AGE_MS,
    summaries_enabled&&operator_virtual_summary_enabled),
  };
  if now_ms.saturating_sub(item.detected_at_wall_ms)>max_age{
   stale+=1;continue
  }
  if allowed&&!already_claimed.contains(id){
   candidates.push((priority,item.detected_at_wall_ms,
    id.to_owned(),item.notice.clone()));
  }
 }
 // Faults first, then oldest first among same priority; never dispatch
 // more than one distinct notice in one poll.
 candidates.sort_by(|a,b|
  a.0.cmp(&b.0).then_with(||a.1.cmp(&b.1)).then_with(||a.2.cmp(&b.2)));
 let candidate=candidates.first().map(|v|v.3.clone());
 Ok(DispatchPreview{
  source:"NATIVE_RUST_PAPER_QUEUE_PREVIEW_ONLY".into(),
  latest_session:current.session.clone(),
  queue_revision:state.revision,
  total_pending:state.pending.len(),
  stale_notices_skipped:stale,
  historical_session_skipped:historical,
  permitted_new_events:candidates.len(),
  candidate,
  telegram_bot_api_called:false,
  real_orders_permitted:false,
 })
}

#[cfg(test)]
mod tests{
 use super::*;
 use crate::{
  native_paper_notice_queue_review::{PaperWatchState,PendingPaperNotice},
  native_paper_telegram_observer::{PaperPoint,PaperObserveCursor},
 };
 use std::collections::BTreeMap;
 const START:u64=1_792_069_401_000;
 fn cfg()->NativeTradingConfig{
  NativeTradingConfig::parse(
    include_str!("../config/native_trading.yaml")).unwrap()
 }
 fn point()->PaperPoint{
  PaperPoint{
   session:"session_20741_1792069401".into(),
   day:20741,updated_wall_ms:START+75_000,
   entries:32,closes:32,open_positions:0,faults:1,
   unknown_unhedged:0,modeled_milli_usdt:42,
  }
 }
 fn make()->(NativeTradingConfig,PaperWatchState){
  let config=cfg();
  let point=point();
  let state=PaperWatchState{
   schema:R37_SCHEMA,config_sha256:config.sha256.clone(),revision:4,
   virtual_summary_opt_in:false,
   oldstyle:None,
   global_last_heartbeat_ms:None,
   calendar:None,
   cursor:PaperObserveCursor{
    schema:1,last_seen:point.clone(),last_summary:point,
   },
   pending:BTreeMap::new(),
  };
  (config,state)
 }
 fn append(state:&mut PaperWatchState,kind:PaperNoticeKind,id:&str,ts:u64){
  let text=match kind{
   PaperNoticeKind::Fault=>
    "⚠️ Rust PAPER: новая ошибка. Реальная торговля выключена.",
   PaperNoticeKind::VirtualActivitySummary=>
    "📊 Rust PAPER: модельный итог. НЕ реальная прибыль.",
   PaperNoticeKind::VirtualTradeOpened=>
    "🟢 PAPER · Открыта виртуальная сделка. Исполнений на биржах НЕ было.",
   PaperNoticeKind::VirtualTradeClosed=>
    "🔵 PAPER · Закрыта виртуальная сделка. НЕ реальная прибыль.",
   PaperNoticeKind::HourlyHeartbeat=>
    "❤️ Rust PAPER работает. Реальная торговля отключена.",
   PaperNoticeKind::DailyReport=>
    "📊 PAPER · Итог дня. НЕ реальная прибыль.",
  };
  state.pending.insert(id.into(),PendingPaperNotice{
   notice:PaperNotice{
    kind,event_id:id.into(),russian_text:text.into(),
    external_delivery:false,may_trigger_real_trade:false,
    real_pnl_attested:false,
   },
   detected_at_wall_ms:ts,send_attempted:false,
  });
 }
 const F1:&str="PFAULT-1111111111111111-1";
 const F2:&str="PFAULT-2222222222222222-2";
 const S1:&str="PSUM-3333333333333333-1";
 #[test]
 fn deny_missing_operator_authorization_or_real_trade_policy(){
  let (mut c,s)=make();let now=START+80_000;
  assert_eq!(select_next_native_paper_telegram(&s,&c,now,false,false),
   Err(DispatchGateError::NotAuthorized));
  c.policy.real_execution_authorized=true;
  assert_eq!(select_next_native_paper_telegram(&s,&c,now,true,false),
   Err(DispatchGateError::NotAuthorized));
 }
 #[test]
 fn r177_queue_selection_denies_postparse_flags_or_sha_mutation(){
  let (mut c,mut s)=make();
  append(&mut s,PaperNoticeKind::Fault,F1,START+30_000);
  assert_eq!(select_next_native_paper_telegram(
   &s,&c,START+80_000,true,false).unwrap().candidate.unwrap().event_id,F1);
  // Unrelated operator YAML drift must revoke selection as well.
  c.raw["general"]["bot_name"]=
   serde_yaml::Value::String("changed-after-parse".into());
  assert_eq!(select_next_native_paper_telegram(
   &s,&c,START+80_000,true,false),Err(DispatchGateError::NotAuthorized));
  let (mut c,s)=make();
  c.sha256="0".repeat(64);
  assert_eq!(select_next_native_paper_telegram(
   &s,&c,START+80_000,true,false),Err(DispatchGateError::NotAuthorized));
 }
 #[test]
 fn empty_queue_no_network_and_no_credentials_ever_needed(){
  let (c,s)=make();let now=START+80_000;
  let p=select_next_native_paper_telegram(&s,&c,now,true,false).unwrap();
  assert_eq!(p.total_pending,0);
  assert!(p.candidate.is_none());
  assert_eq!(p.permitted_new_events,0);
  assert!(!p.telegram_bot_api_called);
  assert!(!p.real_orders_permitted);
 }
 #[test]
 fn fault_priority_and_summary_requires_separate_opt_in(){
  let (c,mut s)=make();let now=START+80_000;
  append(&mut s,PaperNoticeKind::VirtualActivitySummary,S1,START+40_000);
  append(&mut s,PaperNoticeKind::Fault,F1,START+50_000);
  let p=select_next_native_paper_telegram(&s,&c,now,true,true).unwrap();
  assert_eq!(p.permitted_new_events,2);
  assert_eq!(p.candidate.unwrap().event_id,F1);
  let p=select_next_native_paper_telegram(&s,&c,now,true,false).unwrap();
  assert_eq!(p.permitted_new_events,1);
  assert_eq!(p.candidate.unwrap().event_id,F1);
  s.pending.remove(F1);
  let p=select_next_native_paper_telegram(&s,&c,now,true,false).unwrap();
  assert!(p.candidate.is_none());
  let p=select_next_native_paper_telegram(&s,&c,now,true,true).unwrap();
  assert_eq!(p.candidate.unwrap().event_id,S1);
 }
 #[test]
 fn reject_stale_source_and_future_clock_without_sending(){
  let (c,s)=make();
  assert_eq!(select_next_native_paper_telegram(
   &s,&c,START+75_000+MAX_STATUS_AGE_MS+1,true,false),
   Err(DispatchGateError::StaleSource));
  assert_eq!(select_next_native_paper_telegram(
   &s,&c,START+69_000,true,false),
   Err(DispatchGateError::Clock));
 }
 #[test]
 fn old_session_pending_skipped_and_short_lived_fault_only(){
  let (c,mut s)=make();let now=START+80_000;
  append(&mut s,PaperNoticeKind::Fault,F1,START-10_000);
  append(&mut s,PaperNoticeKind::Fault,F2,START+20_000);
  let p=select_next_native_paper_telegram(&s,&c,now,true,false).unwrap();
  assert_eq!(p.historical_session_skipped,1);
  assert_eq!(p.candidate.unwrap().event_id,F2);
  // Fake an old source with its current timestamp advanced far into
  // the future; expiry must still ignore older pending.
  s.cursor.last_seen.updated_wall_ms=START+MAX_FAULT_AGE_MS+90_000;
  let p=select_next_native_paper_telegram(
    &s,&c,START+MAX_FAULT_AGE_MS+90_000,true,false).unwrap();
  assert!(p.candidate.is_none());
  assert_eq!(p.stale_notices_skipped,1);
 }

 #[test]
 fn claimed_unknown_or_confirmed_not_selected_again(){
  let (c,mut s)=make();let now=START+80_000;
  append(&mut s,PaperNoticeKind::Fault,F1,START+30_000);
  append(&mut s,PaperNoticeKind::Fault,F2,START+40_000);
  let mut known=BTreeSet::new();
  known.insert(F1.into());
  let selected=select_next_native_paper_telegram_with_known(
   &s,&c,now,true,false,&known).unwrap();
  assert_eq!(selected.candidate.unwrap().event_id,F2);
  known.insert(F2.into());
  let selected=select_next_native_paper_telegram_with_known(
   &s,&c,now,true,false,&known).unwrap();
  assert_eq!(selected.permitted_new_events,0);
  assert!(selected.candidate.is_none());
  assert_eq!(selected.total_pending,2);
 }

 #[test]
 fn e2e_queue_selection_durable_fake_telegram_ack_then_no_resend(){
  use crate::{
   native_paper_telegram_delivery_review::deliver_paper_status_once,
   native_telegram_delivery_review::{
    TelegramSendTransport,TelegramAck,TelegramSendError,DeliveryResult,
   },
   native_telegram_durable_outbox::{inspect_local_claim,DeliveryState},
  };
  use std::{sync::atomic::{AtomicUsize,Ordering},
             time::{SystemTime,UNIX_EPOCH},fs};
  struct Fake {
   sends:AtomicUsize,success:bool,
  }
  impl TelegramSendTransport for Fake{
   fn send_plain_text_once(&self,text:&str)
     ->Result<TelegramAck,TelegramSendError>{
    assert!(text.starts_with("⚠️ Rust PAPER"));
    self.sends.fetch_add(1,Ordering::SeqCst);
    if self.success{Ok(TelegramAck{message_id:9912})}
    else{Err(TelegramSendError::Network)}
   }
  }
  let (c,mut state)=make();
  let now=START+80_000;
  append(&mut state,PaperNoticeKind::Fault,F1,START+30_000);
  let tick=SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
  let parent=std::env::temp_dir().join(format!(
   "r38_dispatch_e2e_{}_{}",std::process::id(),tick));
  fs::create_dir(&parent).unwrap();
  let ledger=parent.join("telegram.json");
  for success in [true,false]{
   let note=select_next_native_paper_telegram(
    &state,&c,now,true,false).unwrap().candidate.unwrap();
   let fake=Fake{sends:AtomicUsize::new(0),success};
   let result=deliver_paper_status_once(
    &ledger,&c,&note,now,true,&fake).unwrap();
   if success{
    assert!(matches!(result,DeliveryResult::Confirmed{
     message_id:9912,..}));
    assert_eq!(inspect_local_claim(
      &ledger,&c.sha256,F1).unwrap(),
      Some(DeliveryState::Confirmed{telegram_message_id:9912}));
   }else{
    assert!(matches!(result,DeliveryResult::Suppressed{..}),
      "prior confirmed ACK must never produce another send");
   }
   assert_eq!(fake.sends.load(Ordering::SeqCst),
    if success{1}else{0});
   let known=BTreeSet::from([F1.to_owned()]);
   let after=select_next_native_paper_telegram_with_known(
    &state,&c,now+5_000,true,false,&known).unwrap();
   assert!(after.candidate.is_none());
  }
  fs::remove_dir_all(parent).unwrap();
 }

 #[test]
 fn uncertain_telegram_result_is_consumed_and_not_retransmitted(){
  use crate::{
   native_paper_telegram_delivery_review::deliver_paper_status_once,
   native_telegram_delivery_review::{
    TelegramSendTransport,TelegramAck,TelegramSendError,DeliveryResult,
   },
   native_telegram_durable_outbox::{inspect_local_claim,DeliveryState},
  };
  use std::{sync::atomic::{AtomicUsize,Ordering},
             time::{SystemTime,UNIX_EPOCH},fs};
  struct Uncertain(AtomicUsize);
  impl TelegramSendTransport for Uncertain{
   fn send_plain_text_once(&self,_:&str)->Result<TelegramAck,TelegramSendError>{
    self.0.fetch_add(1,Ordering::SeqCst);
    Err(TelegramSendError::Network)
   }
  }
  let (c,mut state)=make();let now=START+80_000;
  append(&mut state,PaperNoticeKind::Fault,F1,START+30_000);
  let timestamp=SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
  let dir=std::env::temp_dir().join(format!(
   "r38_unknown_{}_{}",std::process::id(),timestamp));
  fs::create_dir(&dir).unwrap();
  let ledger=dir.join("status_outbox.json");
  let fake=Uncertain(AtomicUsize::new(0));
  let note=select_next_native_paper_telegram(
   &state,&c,now,true,false).unwrap().candidate.unwrap();
  assert!(matches!(deliver_paper_status_once(
   &ledger,&c,&note,now,true,&fake).unwrap(),DeliveryResult::Uncertain{..}));
  assert_eq!(inspect_local_claim(&ledger,&c.sha256,F1).unwrap(),
    Some(DeliveryState::ClaimedUnknown));
  let known=BTreeSet::from([F1.to_owned()]);
  assert!(select_next_native_paper_telegram_with_known(
   &state,&c,now+10_000,true,false,&known).unwrap().candidate.is_none());
  assert!(matches!(deliver_paper_status_once(
   &ledger,&c,&note,now+10_000,true,&fake).unwrap(),
   DeliveryResult::Suppressed{..}));
  assert_eq!(fake.0.load(Ordering::SeqCst),1);
  fs::remove_dir_all(dir).unwrap();
 }
 #[test]
 fn altered_or_unsafe_source_record_is_fatal_not_skipped(){
  let (c,mut s)=make();let now=START+80_000;
  append(&mut s,PaperNoticeKind::Fault,F1,START+30_000);
  let mut x=s.clone();x.pending.get_mut(F1).unwrap().send_attempted=true;
  assert_eq!(select_next_native_paper_telegram(&x,&c,now,true,false),
   Err(DispatchGateError::UnsafeRecord));
  let mut x=s.clone();
  x.pending.get_mut(F1).unwrap().notice.real_pnl_attested=true;
  assert_eq!(select_next_native_paper_telegram(&x,&c,now,true,false),
   Err(DispatchGateError::UnsafeRecord));
  let mut x=s.clone();
  x.pending.get_mut(F1).unwrap().notice.russian_text=
   "⚠️ Rust PAPER: real profit guaranteed".into();
  assert_eq!(select_next_native_paper_telegram(&x,&c,now,true,false),
   Err(DispatchGateError::UnsafeRecord));
  let mut x=s.clone();x.config_sha256="0".repeat(64);
  assert_eq!(select_next_native_paper_telegram(&x,&c,now,true,false),
   Err(DispatchGateError::InvalidState));
  let mut x=s.clone();x.pending.get_mut(F1).unwrap().detected_at_wall_ms=now+10_000;
  assert_eq!(select_next_native_paper_telegram(&x,&c,now,true,false),
   Err(DispatchGateError::UnsafeRecord));
 }
}
