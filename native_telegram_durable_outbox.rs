//! Astra P1-06. Durable at-most-once PRE-SEND Telegram reservation.
//! No network transport or Bot API token is accessible from this module.
//! A claimed event with UNKNOWN delivery is NEVER resent automatically.
//! This deliberately trades possible lost notification for no spam after crash.
use std::{
 collections::BTreeMap,fs::{self,File,OpenOptions},
 io::{Read,Write},os::{fd::AsRawFd,unix::fs::{OpenOptionsExt,MetadataExt}},
 path::Path,
};
use serde::{Deserialize,Serialize};
use openssl::sha::sha256;
use crate::native_observability_review::{
 NativeObservePolicy,ObservationKind,OfflineNativeSample,
 inspect_native_observation_without_io,
};
const MAX_LEDGER_BYTES:u64=512*1024;
const MAX_RECORDS:usize=2048;
const MODE:u32=0o600;
const NOFOLLOW:i32=libc::O_NOFOLLOW|libc::O_CLOEXEC;
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum OutboxError{Io,Invalid,Config,Clock,Capacity,Conflict,UnsafePath}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
pub enum DeliveryState {ClaimedUnknown,Confirmed{telegram_message_id:u64}}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
struct Event{
 topic:String,event_id:String,observed_ms:u64,payload_sha256:String,
 state:DeliveryState,
}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
struct Ledger {
 schema:u32,config_sha256:String,generation:u64,
 last_claim_ms:u64,events:BTreeMap<String,Event>,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum ClaimDecision {
 Claimed{event_id:String,body:String},
 Suppressed{reason:&'static str},
}
fn hash(s:&str)->String{
 sha256(s.as_bytes()).iter().map(|b|format!("{b:02x}")).collect()
}
fn code(s:&str)->bool{
 !s.is_empty()&&s.len()<=64&&s.bytes().all(|c|
 c.is_ascii_uppercase()||c.is_ascii_digit()||c==b'_'||c==b'-')
}
fn io(_:std::io::Error)->OutboxError{OutboxError::Io}
fn ensure_regular_secure(file:&File)->Result<(),OutboxError>{
 let meta=file.metadata().map_err(io)?;
 if !meta.is_file()||meta.mode()&0o077!=0||meta.nlink()!=1||
   meta.len()>MAX_LEDGER_BYTES{return Err(OutboxError::UnsafePath)}
 Ok(())
}
struct Lock(File);
impl Lock{
 fn acquire(path:&Path)->Result<Self,OutboxError>{
  let name=path.file_name().and_then(|s|s.to_str())
   .ok_or(OutboxError::UnsafePath)?;
  if name.is_empty()||name.len()>150||name.contains(['/', '\\']){
   return Err(OutboxError::UnsafePath)
  }
  let parent=path.parent().ok_or(OutboxError::UnsafePath)?;
  if !parent.is_dir(){return Err(OutboxError::UnsafePath)}
  let lock=parent.join(format!(".{name}.flock"));
  let file=OpenOptions::new().read(true).write(true).create(true)
   .mode(MODE).custom_flags(NOFOLLOW).open(lock).map_err(io)?;
  ensure_regular_secure(&file)?;
  if unsafe{libc::flock(file.as_raw_fd(),libc::LOCK_EX)}!=0{
   return Err(OutboxError::Io)
  }
  Ok(Self(file))
 }
}
impl Drop for Lock{
 fn drop(&mut self){unsafe{libc::flock(self.0.as_raw_fd(),libc::LOCK_UN);}}
}
fn load(path:&Path,config_sha:&str)->Result<Ledger,OutboxError>{
 let ledger=match OpenOptions::new().read(true).custom_flags(NOFOLLOW).open(path){
  Ok(mut file)=>{
   ensure_regular_secure(&file)?;
   let mut content=Vec::new();
   file.read_to_end(&mut content).map_err(io)?;
   serde_json::from_slice::<Ledger>(&content).map_err(|_|OutboxError::Invalid)?
  },
  Err(e) if e.kind()==std::io::ErrorKind::NotFound=>Ledger{
   schema:1,config_sha256:config_sha.into(),generation:0,
   last_claim_ms:0,events:BTreeMap::new(),
  },
  Err(_)=>return Err(OutboxError::Io)
 };
 if ledger.schema!=1||ledger.config_sha256!=config_sha||
    ledger.generation!=ledger.events.len() as u64||
    ledger.events.len()>MAX_RECORDS{return Err(OutboxError::Config)}
 for (key,e) in &ledger.events{
  if key!=&e.event_id||!code(&e.topic)||e.observed_ms==0||
   e.observed_ms>ledger.last_claim_ms||e.payload_sha256.len()!=64||
   !e.payload_sha256.bytes().all(|b|b.is_ascii_hexdigit()&&!b.is_ascii_uppercase()){
   return Err(OutboxError::Invalid)
  }
  if let DeliveryState::Confirmed{telegram_message_id}=e.state{
   if telegram_message_id==0{return Err(OutboxError::Invalid)}
  }
 }
 Ok(ledger)
}
fn persist(path:&Path,ledger:&Ledger)->Result<(),OutboxError>{
 let parent=path.parent().ok_or(OutboxError::UnsafePath)?;
 let name=path.file_name().and_then(|s|s.to_str()).ok_or(OutboxError::UnsafePath)?;
 let bytes=serde_json::to_vec(ledger).map_err(|_|OutboxError::Invalid)?;
 if bytes.len() as u64>MAX_LEDGER_BYTES{return Err(OutboxError::Capacity)}
 let temporary=parent.join(format!(".{name}.tmp.{}",std::process::id()));
 let mut file=OpenOptions::new().write(true).create_new(true)
  .mode(MODE).custom_flags(NOFOLLOW).open(&temporary).map_err(io)?;
 ensure_regular_secure(&file)?;
 file.write_all(&bytes).map_err(io)?;
 file.sync_all().map_err(io)?;
 drop(file);
 fs::rename(&temporary,path).map_err(io)?;
 File::open(parent).map_err(io)?.sync_all().map_err(io)?;
 Ok(())
}
/// Only a **durable claim**, NOT Telegram network sending. A caller must
/// NEVER submit a message before ClaimDecision::Claimed is durably returned.
/// After a crash / UNKNOWN, event id stays consumed until human reconciliation.
pub fn claim_russian_preview(
 path:&Path,policy:&NativeObservePolicy,kind:ObservationKind,
 sample:&OfflineNativeSample,now_ms:u64,
)->Result<ClaimDecision,OutboxError>{
 if policy.config_sha256.len()!=64
  ||!policy.config_sha256.bytes().all(|b|b.is_ascii_hexdigit()&&!b.is_ascii_uppercase())
  ||now_ms==0{return Err(OutboxError::Config)}
 let preview=inspect_native_observation_without_io(policy,kind,sample,now_ms)
  .map_err(|_|OutboxError::Invalid)?;
 if preview.may_trade_or_release||preview.telegram_delivered
  ||preview.authenticated_exchange_source||preview.actual_profit_proven{
  return Err(OutboxError::Invalid)
 }
 let body=match preview.russian_telegram_preview{
  Some(s)=>s,None=>return Ok(ClaimDecision::Suppressed{reason:"NO_VALID_PREVIEW"})
 };
 if body.is_empty()||body.len()>2_048{return Err(OutboxError::Invalid)}
 let (kind_tag,topic,cooldown_sec)=match kind{
  ObservationKind::Candidate=>("SIGNAL",
    sample.symbol.as_deref().ok_or(OutboxError::Invalid)?,
    policy.symbol_cooldown_sec),
  ObservationKind::Fault=>("FAULT",
    sample.fault_code.as_deref().ok_or(OutboxError::Invalid)?,
    policy.symbol_cooldown_sec),
  ObservationKind::Heartbeat=>("HEARTBEAT","BOT",
    policy.heartbeat_interval_sec.max(policy.service_interval_sec)),
 };
 if !code(topic)||cooldown_sec==0||cooldown_sec>86_400{
  return Err(OutboxError::Invalid)
 }
 let topic=format!("{kind_tag}-{topic}");
 let event_id=format!("{topic}-{}",sample.observed_ms);
 if !code(&topic)||event_id.len()>120{return Err(OutboxError::Invalid)}
 let _lock=Lock::acquire(path)?;
 let mut l=load(path,&policy.config_sha256)?;
 if now_ms<l.last_claim_ms{return Err(OutboxError::Clock)}
 if let Some(old)=l.events.get(&event_id){
  if old.payload_sha256!=hash(&body){return Err(OutboxError::Conflict)}
  return Ok(ClaimDecision::Suppressed{reason:"ALREADY_CLAIMED_UNKNOWN_OR_CONFIRMED"})
 }
 if let Some(prev)=l.events.values().filter(|e|e.topic==topic)
   .max_by_key(|e|e.observed_ms){
  if sample.observed_ms<prev.observed_ms{return Err(OutboxError::Clock)}
  if sample.observed_ms-prev.observed_ms <cooldown_sec*1_000{
   return Ok(ClaimDecision::Suppressed{reason:"DURABLE_TOPIC_COOLDOWN"})
  }
 }
 if l.events.len()>=MAX_RECORDS{return Err(OutboxError::Capacity)}
 l.last_claim_ms=now_ms;
 l.generation=l.generation.checked_add(1).ok_or(OutboxError::Capacity)?;
 l.events.insert(event_id.clone(),Event{
  topic,event_id:event_id.clone(),observed_ms:sample.observed_ms,
  payload_sha256:hash(&body),state:DeliveryState::ClaimedUnknown,
 });
 persist(path,&l)?;
 Ok(ClaimDecision::Claimed{event_id,body})
}
/// Explicit one-shot USER-REQUESTED TEST only, never market data or trades.
/// Preserves the same fsynced ClaimUnknown and same-process/cross-process
/// dedup guarantees as signal notices, with a separate TEST topic.
pub fn claim_one_user_test_notice(
 path:&Path,config_sha:&str,event_id:&str,body:&str,now_ms:u64,
)->Result<ClaimDecision,OutboxError>{
 if config_sha.len()!=64||!config_sha.bytes()
    .all(|b|b.is_ascii_hexdigit()&&!b.is_ascii_uppercase())
  ||!event_id.starts_with("TEST-")||!code(event_id)
  ||now_ms==0||body.len()>2_048||body.is_empty()
  ||!body.starts_with("Тестовое сообщение SPOT-ARB")
  ||!body.contains("не торговый сигнал"){
  return Err(OutboxError::Invalid)
 }
 let _lock=Lock::acquire(path)?;
 let mut l=load(path,config_sha)?;
 if now_ms<l.last_claim_ms{return Err(OutboxError::Clock)}
 if let Some(saved)=l.events.get(event_id){
  if saved.payload_sha256!=hash(body){return Err(OutboxError::Conflict)}
  return Ok(ClaimDecision::Suppressed{
   reason:"ALREADY_CLAIMED_UNKNOWN_OR_CONFIRMED"
  })
 }
 if l.events.len()>=MAX_RECORDS{return Err(OutboxError::Capacity)}
 l.generation=l.generation.checked_add(1).ok_or(OutboxError::Capacity)?;
 l.last_claim_ms=now_ms;
 l.events.insert(event_id.to_string(),Event{
  topic:"TEST".into(),event_id:event_id.to_string(),
  observed_ms:now_ms,payload_sha256:hash(body),
  state:DeliveryState::ClaimedUnknown,
 });
 persist(path,&l)?;
 Ok(ClaimDecision::Claimed{event_id:event_id.into(),body:body.into()})
}

/// Claim a PAPER-only Rust status notice before an OPTIONAL future Bot
/// API send. Never accept real-PnL or trade authorization claims.
/// This is distinct from historical TEST and public arbitrage SIGNAL.
pub fn claim_paper_status_notice(
 path:&Path,config_sha:&str,
 note:&crate::native_paper_telegram_observer::PaperNotice,
 now_ms:u64,
)->Result<ClaimDecision,OutboxError>{
 use crate::native_paper_telegram_observer::PaperNoticeKind;
 let (topic,cooldown)=match note.kind{
  // PAPER service lifecycle events use a dedicated at-most-once topic.
  // No cooldown: distinct STOP and START must not suppress each other.
  PaperNoticeKind::Fault if note.event_id.starts_with("PFAULT-LIFE-")=>
    ("PAPER_LIFECYCLE",0u64),
  PaperNoticeKind::Fault=>("PAPER_FAULT",300_000u64),
  PaperNoticeKind::VirtualActivitySummary=>("PAPER_SUMMARY",1_800_000),
  PaperNoticeKind::VirtualTradeOpened=>("PAPER_OPEN",0),
  PaperNoticeKind::VirtualTradeClosed=>("PAPER_CLOSE",0),
  PaperNoticeKind::HourlyHeartbeat=>("PAPER_HEARTBEAT",3_000_000),
  PaperNoticeKind::DailyReport=>("PAPER_DAILY",72_000_000),
 };
 let prefix=match note.kind{
  PaperNoticeKind::Fault=>"PFAULT-",
  PaperNoticeKind::VirtualActivitySummary=>"PSUM-",
  PaperNoticeKind::VirtualTradeOpened=>"POPEN-",
  PaperNoticeKind::VirtualTradeClosed=>"PCLOSE-",
  PaperNoticeKind::HourlyHeartbeat=>"PBEAT-",
  PaperNoticeKind::DailyReport=>"PDAY-",
 };
 let body=&note.russian_text;
 if note.external_delivery||note.may_trigger_real_trade||
   note.real_pnl_attested||now_ms==0
  ||config_sha.len()!=64||!config_sha.bytes()
    .all(|b|b.is_ascii_hexdigit()&&!b.is_ascii_uppercase())
  ||!code(&note.event_id)||!note.event_id.starts_with(prefix)
  ||body.is_empty()||body.len()>2048
  ||!body.contains("НЕ реальная прибыль")&&
    matches!(note.kind,PaperNoticeKind::VirtualActivitySummary)
  ||!body.contains("Реальная торговля выключена")&&
    matches!(note.kind,PaperNoticeKind::Fault)
  ||!body.contains("Исполнений на биржах НЕ было")&&
    matches!(note.kind,PaperNoticeKind::VirtualTradeOpened)
  ||!body.contains("НЕ реальная прибыль")&&
    matches!(note.kind,PaperNoticeKind::VirtualTradeClosed|PaperNoticeKind::DailyReport)
  ||!body.contains("Реальная торговля отключена")&&
    matches!(note.kind,PaperNoticeKind::HourlyHeartbeat)
  ||!body.starts_with(match note.kind{
    PaperNoticeKind::Fault=>"⚠️ Rust PAPER",
    PaperNoticeKind::VirtualActivitySummary=>"📊 Rust PAPER",
    PaperNoticeKind::VirtualTradeOpened=>"🟢 PAPER",
    PaperNoticeKind::VirtualTradeClosed=>"🔵 PAPER",
    PaperNoticeKind::HourlyHeartbeat=>"❤️ Rust PAPER",
    PaperNoticeKind::DailyReport=>"📊 PAPER",
  }){
  return Err(OutboxError::Invalid)
 }
 let _lock=Lock::acquire(path)?;
 let mut ledger=load(path,config_sha)?;
 if now_ms<ledger.last_claim_ms{return Err(OutboxError::Clock)}
 if let Some(old)=ledger.events.get(&note.event_id){
  if old.payload_sha256!=hash(body){return Err(OutboxError::Conflict)}
  return Ok(ClaimDecision::Suppressed{
   reason:"ALREADY_CLAIMED_UNKNOWN_OR_CONFIRMED"
  })
 }
 if let Some(last)=ledger.events.values().filter(|e|e.topic==topic)
    .max_by_key(|e|e.observed_ms){
   if now_ms.saturating_sub(last.observed_ms)<cooldown{
    return Ok(ClaimDecision::Suppressed{reason:"PAPER_EVENT_NO_SPAM_COOLDOWN"})
   }
 }
 if ledger.events.len()>=MAX_RECORDS{return Err(OutboxError::Capacity)}
 ledger.generation=ledger.generation.checked_add(1).ok_or(OutboxError::Capacity)?;
 ledger.last_claim_ms=now_ms;
 ledger.events.insert(note.event_id.clone(),Event{
  topic:topic.into(),event_id:note.event_id.clone(),
  observed_ms:now_ms,payload_sha256:hash(body),
  state:DeliveryState::ClaimedUnknown,
 });
 persist(path,&ledger)?;
 Ok(ClaimDecision::Claimed{
  event_id:note.event_id.clone(),body:body.clone()
 })
}

/// Only an explicit verifiable Telegram HTTP 200 / ok:true / message_id
/// may call this; UNKNOWN/timeout NEVER triggers resend or auto-confirm.
pub fn confirm_telegram_ack(
 path:&Path,expected_config_sha256:&str,event_id:&str,
 telegram_message_id:u64,
)->Result<(),OutboxError>{
 if !code(event_id)||telegram_message_id==0{return Err(OutboxError::Invalid)}
 let _lock=Lock::acquire(path)?;
 let mut l=load(path,expected_config_sha256)?;
 if l.events.values().any(|e|
   matches!(e.state,DeliveryState::Confirmed{telegram_message_id:id}
   if id==telegram_message_id)&&e.event_id!=event_id){
  return Err(OutboxError::Conflict)
 }
 let e=l.events.get_mut(event_id).ok_or(OutboxError::Invalid)?;
 match e.state{
  DeliveryState::Confirmed{telegram_message_id:id} if id==telegram_message_id=>return Ok(()),
  DeliveryState::Confirmed{..}=>return Err(OutboxError::Conflict),
  DeliveryState::ClaimedUnknown=>{},
 }
 e.state=DeliveryState::Confirmed{telegram_message_id};
 persist(path,&l)
}
/// External antirollback is explicitly DEFERRED. This is on-host local
/// durability and same-host process race protection, not an off-host witness.
pub fn inspect_local_claim(
 path:&Path,expected_config_sha256:&str,event_id:&str
)->Result<Option<DeliveryState>,OutboxError>{
 let _lock=Lock::acquire(path)?;
 Ok(load(path,expected_config_sha256)?.events.get(event_id)
  .map(|e|e.state.clone()))
}

#[cfg(test)]
mod tests {
 use super::*;
 use std::{sync::{Arc,Barrier},thread,time::{SystemTime,UNIX_EPOCH}};
 use crate::native_trading_config::NativeTradingConfig;
 fn setup(name:&str)->(std::path::PathBuf,NativeObservePolicy,OfflineNativeSample){
  let now=SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
  let dir=std::env::temp_dir().join(format!(
   "native_tg_claim_{}_{}_{}",name,std::process::id(),now));
  fs::create_dir(&dir).unwrap();
  let config=NativeTradingConfig::parse(
   include_str!("../config/native_trading.yaml")).unwrap();
  let p=NativeObservePolicy::from_config(&config).unwrap();
  let s=OfflineNativeSample{
   observed_ms:1_800_000_000_000,owner_generation:1,
   symbol:Some("BTC".into()),net_spread_milli_bps:Some(44_000),
   book_age_ms:30,decisions:1,ws_to_decision_p99_micros:10,
   worker_queue_p99_micros:3,ws_reconnects:0,worker_drops:0,
   fault_code:None
  };
  (dir.join("telegram_outbox.json"),p,s)
 }
 fn claim(path:&Path,p:&NativeObservePolicy,s:&OfflineNativeSample)
 ->Result<ClaimDecision,OutboxError>{
  claim_russian_preview(path,p,ObservationKind::Candidate,s,s.observed_ms+100)
 }
 #[test]
 fn durable_claim_reopen_cooldown_unknown_no_blind_resend(){
  let (path,p,mut s)=setup("restart");
  let id=match claim(&path,&p,&s).unwrap(){
   ClaimDecision::Claimed{event_id,body}=>{
    assert!(body.contains("Наблюдение"));
    assert!(body.contains("Только сигнал"));
    event_id
   },x=>panic!("{x:?}")
  };
  assert_eq!(inspect_local_claim(&path,&p.config_sha256,&id).unwrap(),
   Some(DeliveryState::ClaimedUnknown));
  assert!(matches!(claim(&path,&p,&s).unwrap(),
   ClaimDecision::Suppressed{reason:"ALREADY_CLAIMED_UNKNOWN_OR_CONFIRMED"}));
  s.observed_ms+=60_000;
  assert!(matches!(claim(&path,&p,&s).unwrap(),
   ClaimDecision::Suppressed{reason:"DURABLE_TOPIC_COOLDOWN"}));
  s.observed_ms+=61_000;
  let next=claim(&path,&p,&s).unwrap();
  assert!(matches!(next,ClaimDecision::Claimed{..}));
  assert_eq!(inspect_local_claim(&path,&p.config_sha256,&id).unwrap(),
   Some(DeliveryState::ClaimedUnknown),
   "unknown delivery is never sent again after restart");
  confirm_telegram_ack(&path,&p.config_sha256,&id,321).unwrap();
  confirm_telegram_ack(&path,&p.config_sha256,&id,321).unwrap();
  assert_eq!(inspect_local_claim(&path,&p.config_sha256,&id).unwrap(),
   Some(DeliveryState::Confirmed{telegram_message_id:321}));
  assert!(confirm_telegram_ack(&path,&p.config_sha256,&id,999).is_err());
  let newer=match next{ClaimDecision::Claimed{event_id,..}=>event_id,_=>unreachable!()};
  assert!(confirm_telegram_ack(&path,&p.config_sha256,&newer,321).is_err());
  let _=fs::remove_dir_all(path.parent().unwrap());
 }
 #[test]
 fn r83_service_stop_and_start_distinct_no_5min_cooldown_or_double_send(){
  use crate::native_paper_telegram_observer::{PaperNotice,PaperNoticeKind};
  let (path,p,_)=setup("r83_lifecycle");
  let note=|id:&str,verb:&str|PaperNotice{
    kind:PaperNoticeKind::Fault,event_id:id.into(),
    russian_text:format!("⚠️ Rust PAPER: бот {verb}. Реальная торговля выключена."),
    external_delivery:false,may_trigger_real_trade:false,real_pnl_attested:false,
  };
  let a=note("PFAULT-LIFE-STOP-100","остановлен");
  let b=note("PFAULT-LIFE-START-200","запущен");
  let t=1_800_000_000_000;
  assert!(matches!(claim_paper_status_notice(&path,&p.config_sha256,&a,t).unwrap(),
    ClaimDecision::Claimed{..}));
  assert!(matches!(claim_paper_status_notice(&path,&p.config_sha256,&b,t+5).unwrap(),
    ClaimDecision::Claimed{..}),"separate service STOP/START cannot be cooldown suppressed");
  assert!(matches!(claim_paper_status_notice(&path,&p.config_sha256,&b,t+10).unwrap(),
    ClaimDecision::Suppressed{reason:"ALREADY_CLAIMED_UNKNOWN_OR_CONFIRMED"}));
  assert_eq!(inspect_local_claim(&path,&p.config_sha256,&a.event_id).unwrap(),
    Some(DeliveryState::ClaimedUnknown));
  fs::remove_dir_all(path.parent().unwrap()).unwrap();
 }
 #[test]
 fn explicit_user_telegram_test_claim_and_no_duplicate_delivery(){
  let (path,p,_)=setup("user_requested_one_test");
  let content="Тестовое сообщение SPOT-ARB (Rust). Это не торговый сигнал.";
  let identity="TEST-TELEGRAM-20261015-ONE";
  let time=1_800_000_000_000u64;
  let first=claim_one_user_test_notice(&path,&p.config_sha256,
   identity,content,time).unwrap();
  assert!(matches!(first,ClaimDecision::Claimed{..}));
  assert_eq!(inspect_local_claim(&path,&p.config_sha256,identity).unwrap(),
   Some(DeliveryState::ClaimedUnknown));
  assert_eq!(claim_one_user_test_notice(&path,&p.config_sha256,
   identity,content,time+1).unwrap(),
   ClaimDecision::Suppressed{reason:"ALREADY_CLAIMED_UNKNOWN_OR_CONFIRMED"});
  assert_eq!(claim_one_user_test_notice(&path,&p.config_sha256,
   identity,&format!("{content} changed"),time+2).unwrap_err(),
   OutboxError::Conflict);
  assert_eq!(claim_one_user_test_notice(&path,&p.config_sha256,
   "SIGNAL-FAKE",content,time+2).unwrap_err(),OutboxError::Invalid);
  confirm_telegram_ack(&path,&p.config_sha256,identity,777).unwrap();
  assert_eq!(inspect_local_claim(&path,&p.config_sha256,identity).unwrap(),
   Some(DeliveryState::Confirmed{telegram_message_id:777}));
  assert!(matches!(claim_one_user_test_notice(&path,&p.config_sha256,
   identity,content,time+3).unwrap(),ClaimDecision::Suppressed{..}));
  fs::remove_dir_all(path.parent().unwrap()).unwrap();
 }
 #[test]
 fn concurrent_same_topic_exactly_one_claim(){
  let (path,p,s)=setup("race");
  let barrier=Arc::new(Barrier::new(8));
  let handles=(0..8).map(|_|{
   let p=p.clone();let s=s.clone();let path=path.clone();let b=barrier.clone();
   thread::spawn(move||{
    b.wait();claim(&path,&p,&s).unwrap()
   })
  }).collect::<Vec<_>>();
  let decisions=handles.into_iter().map(|h|h.join().unwrap()).collect::<Vec<_>>();
  assert_eq!(decisions.iter().filter(|d|
   matches!(d,ClaimDecision::Claimed{..})).count(),1);
  assert_eq!(decisions.iter().filter(|d|
   matches!(d,ClaimDecision::Suppressed{..})).count(),7);
  let _=fs::remove_dir_all(path.parent().unwrap());
 }
 #[test]
 fn tamper_symlink_clock_config_conflict_fail_closed(){
  let (path,p,mut s)=setup("negative");
  let _=claim(&path,&p,&s).unwrap();
  let mut wrong=p.clone();wrong.config_sha256="f".repeat(64);
  assert_eq!(claim(&path,&wrong,&s).unwrap_err(),OutboxError::Config);
  s.decisions+=1; // same event id but modified on-wire Russian body unchanged
  s.net_spread_milli_bps=Some(50_000);
  assert_eq!(claim(&path,&p,&s).unwrap_err(),OutboxError::Conflict);
  let before=s.observed_ms;s.observed_ms-=1;
  assert_eq!(claim(&path,&p,&s).unwrap_err(),OutboxError::Clock);
  s.observed_ms=before;
  let other=path.parent().unwrap().join("unsafe_symlink.json");
  std::os::unix::fs::symlink(&path,&other).unwrap();
  assert!(claim(&other,&p,&s).is_err());
  fs::write(&path,b"definitely_not_json").unwrap();
  assert_eq!(claim(&path,&p,&s).unwrap_err(),OutboxError::Invalid);
  let _=fs::remove_dir_all(path.parent().unwrap());
 }
}
