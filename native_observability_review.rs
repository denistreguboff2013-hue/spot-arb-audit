//! P1-06: read-only Rust analytics + Russian Telegram preview from mirrored
//! legacy settings. NO Bot API, Telegram delivery, database, AI model,
 //! trade orders, or production integration. Never a send/entry permit.
use crate::native_trading_config::NativeTradingConfig;
use serde_json::{json,Value as JsonValue};

#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum ObservationKind{Candidate,Fault,Heartbeat}
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum ObserveError{Config,InvalidSample}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct NativeObservePolicy{
 pub config_sha256:String,
 pub analytics_enabled:bool,
 pub telegram_enabled:bool,
 pub telegram_signals:bool,
 pub telegram_errors:bool,
 pub telegram_heartbeat:bool,
 pub telemetry_service_send_tg:bool,
 pub heartbeat_interval_sec:u64,
 pub service_interval_sec:u64,
 pub symbol_cooldown_sec:u64,
 pub min_entry_milli_bps:u128,
 pub book_stale_ms:u64,
}
impl NativeObservePolicy{
 pub fn from_config(config:&NativeTradingConfig)->Result<Self,ObserveError>{
  if !config.source_projection_unchanged(){return Err(ObserveError::Config)}
  let r=&config.raw;
  let b=|sec:&str,key:&str|r.get(sec).and_then(|x|x.get(key))
     .and_then(|x|x.as_bool()).ok_or(ObserveError::Config);
  let n=|sec:&str,key:&str|r.get(sec).and_then(|x|x.get(key))
     .and_then(|x|x.as_u64()).ok_or(ObserveError::Config);
  let analytics_enabled=b("analytics","enabled")?;
  if r.get("analytics").and_then(|v|v.get("mode"))
    .and_then(|v|v.as_str())!=Some("observe"){
    return Err(ObserveError::Config)
  }
  let p=Self{
   config_sha256:config.sha256.clone(),analytics_enabled,
   telegram_enabled:b("telegram","enabled")?,
   telegram_signals:b("telegram","send_signals")?,
   telegram_errors:b("telegram","send_errors")?,
   telegram_heartbeat:b("telegram","send_heartbeat")?,
   telemetry_service_send_tg:r.get("telemetry").and_then(|x|x.get("service"))
     .and_then(|x|x.get("send_tg")).and_then(|x|x.as_bool())
     .ok_or(ObserveError::Config)?,
   heartbeat_interval_sec:n("general","heartbeat_interval_sec")?,
   symbol_cooldown_sec:n("limits","symbol_cooldown_sec")?,
   service_interval_sec:r.get("telemetry").and_then(|x|x.get("service"))
     .and_then(|x|x.get("interval_sec")).and_then(|x|x.as_u64())
     .ok_or(ObserveError::Config)?,
   min_entry_milli_bps:config.policy.entry_min_milli_bps,
   book_stale_ms:n("websocket","stale_ms")?,
  };
  if p.config_sha256.len()!=64||p.min_entry_milli_bps<42_000
   ||p.book_stale_ms==0||p.book_stale_ms>60_000
   ||p.heartbeat_interval_sec==0||p.service_interval_sec==0
   ||p.symbol_cooldown_sec==0||p.symbol_cooldown_sec>86_400
   ||config.policy.real_execution_authorized{
    return Err(ObserveError::Config)
  }
  Ok(p)
 }
 pub fn can_send_telegram(&self)->bool{false}
 pub fn can_trigger_ai_or_trade(&self)->bool{false}
}

#[derive(Clone,Debug,PartialEq,Eq)]
pub struct OfflineNativeSample{
 pub observed_ms:u64,
 pub owner_generation:u64,
 pub symbol:Option<String>,
 pub net_spread_milli_bps:Option<u128>,
 pub book_age_ms:u64,
 pub decisions:u64,
 pub ws_to_decision_p99_micros:u64,
 pub worker_queue_p99_micros:u64,
 pub ws_reconnects:u64,
 pub worker_drops:u64,
 pub fault_code:Option<String>,
}
#[derive(Clone,Debug,PartialEq)]
pub struct OfflineNativeObservePreview{
 pub analytics:JsonValue,
 pub russian_telegram_preview:Option<String>,
 pub decision_reason:&'static str,
 pub authenticated_exchange_source:bool,
 pub actual_profit_proven:bool,
 pub telegram_delivered:bool,
 pub may_trade_or_release:bool,
}
fn valid_code(s:&str,max:usize)->bool{
 !s.is_empty()&&s.len()<=max&&s.bytes()
  .all(|x|x.is_ascii_uppercase()||x.is_ascii_digit()||x==b'_'||x==b'-')
}
pub fn inspect_native_observation_without_io(
 policy:&NativeObservePolicy,
 kind:ObservationKind,
 sample:&OfflineNativeSample,
 now_ms:u64,
)->Result<OfflineNativeObservePreview,ObserveError>{
 if now_ms==0||sample.observed_ms==0||sample.observed_ms>now_ms
   ||now_ms-sample.observed_ms>60_000||sample.owner_generation==0
   ||sample.ws_to_decision_p99_micros>60_000_000
   ||sample.worker_queue_p99_micros>60_000_000
   ||sample.book_age_ms>1_000_000
   ||sample.symbol.as_deref().is_some_and(|s|!valid_code(s,32))
   ||sample.fault_code.as_deref().is_some_and(|s|!valid_code(s,32))
   ||sample.net_spread_milli_bps.is_some_and(|v|v>10_000_000){
  return Err(ObserveError::InvalidSample)
 }
 let allowed_channel=policy.telegram_enabled&&match kind{
   ObservationKind::Candidate=>policy.telegram_signals,
   ObservationKind::Fault=>policy.telegram_errors,
   ObservationKind::Heartbeat=>policy.telegram_heartbeat&&policy.telemetry_service_send_tg,
 };
 let fresh=sample.book_age_ms<=policy.book_stale_ms;
 let (reason,preview)=match kind{
  ObservationKind::Candidate=>{
   if sample.symbol.is_none()||sample.net_spread_milli_bps.is_none()
     ||sample.fault_code.is_some(){
     return Err(ObserveError::InvalidSample)
   }
   let spread=sample.net_spread_milli_bps.unwrap();
   if !fresh{("STALE_BOOK",None)}
   else if spread<policy.min_entry_milli_bps{("BELOW_ENTRY_THRESHOLD",None)}
   else if !allowed_channel{("SIGNALS_DISABLED",None)}
   else{("REVIEW_ONLY_SPREAD",Some(format!(
     "Наблюдение: {} | спред {}.{:03} б.п. | возраст книги {} мс. Только сигнал, не подтверждённая прибыль и не команда на сделку.",
     sample.symbol.as_deref().unwrap(),spread/1000,spread%1000,sample.book_age_ms)))}
  },
  ObservationKind::Fault=>{
   if sample.fault_code.is_none(){return Err(ObserveError::InvalidSample)}
   if !allowed_channel{("ERRORS_DISABLED",None)}
   else{("REVIEW_ONLY_FAULT",Some(format!(
     "Ошибка наблюдения Rust: {}. Переподключений: {}, потерь: {}. Торговые действия запрещены.",
     sample.fault_code.as_deref().unwrap(),sample.ws_reconnects,sample.worker_drops)))}
  },
  ObservationKind::Heartbeat=>{
   if !allowed_channel{("HEARTBEAT_DISABLED",None)}
   else{("REVIEW_ONLY_HEARTBEAT",Some(format!(
     "Rust наблюдение: решений {}, p99 {} мкс, очередь p99 {} мкс. Отчёт только для просмотра.",
     sample.decisions,sample.ws_to_decision_p99_micros,sample.worker_queue_p99_micros)))}
  },
 };
 let analytics=json!({
  "schema":"spotarb_native_offline_observe_v1",
  "config_sha256":policy.config_sha256,
  "owner_generation":sample.owner_generation,
  "observed_ms":sample.observed_ms,
  "kind":match kind{ObservationKind::Candidate=>"spread_candidate",
    ObservationKind::Fault=>"fault",ObservationKind::Heartbeat=>"heartbeat"},
  "symbol":sample.symbol,
  "net_spread_milli_bps":sample.net_spread_milli_bps.map(|n|n.to_string()),
  "book_age_ms":sample.book_age_ms,
  "book_fresh":fresh,
  "decisions":sample.decisions,
  "ws_to_decision_p99_micros":sample.ws_to_decision_p99_micros,
  "worker_queue_p99_micros":sample.worker_queue_p99_micros,
  "ws_reconnects":sample.ws_reconnects,
  "worker_drops":sample.worker_drops,
  "fault_code":sample.fault_code,
  "decision_reason":reason,
  "exchange_authenticated":false,
  "real_profit_verified":false,
  "real_trading_authorized":false,
  "telegram_sent":false,
 });
 Ok(OfflineNativeObservePreview{
  analytics:if policy.analytics_enabled{analytics}else{JsonValue::Null},
  russian_telegram_preview:preview,
  decision_reason:reason,
  authenticated_exchange_source:false,
  actual_profit_proven:false,
  telegram_delivered:false,
  may_trade_or_release:false,
 })
}

/// Bounded in-memory no-spam review fence. Not persisted across restart;
/// NEVER enough for true exactly-once Telegram delivery or source provenance.
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct OfflineObservationReplayFence{
 config_sha256:String,
 last_preview_by_topic:std::collections::BTreeMap<(u8,String),u64>,
}
impl OfflineObservationReplayFence{
 pub fn new(policy:&NativeObservePolicy)->Self{
  Self{config_sha256:policy.config_sha256.clone(),
       last_preview_by_topic:Default::default()}
 }
 pub fn may_send_real_telegram_or_trade(&self)->bool{false}
 pub fn inspect_once(
  &mut self,policy:&NativeObservePolicy,kind:ObservationKind,
  sample:&OfflineNativeSample,now_ms:u64,
 )->Result<OfflineNativeObservePreview,ObserveError>{
  if self.config_sha256!=policy.config_sha256{
    return Err(ObserveError::Config)
  }
  let mut next=inspect_native_observation_without_io(policy,kind,sample,now_ms)?;
  if next.russian_telegram_preview.is_none(){return Ok(next)}
  let key=match kind{
    ObservationKind::Candidate=>(1,sample.symbol.clone().ok_or(ObserveError::InvalidSample)?),
    ObservationKind::Fault=>(2,sample.fault_code.clone().ok_or(ObserveError::InvalidSample)?),
    ObservationKind::Heartbeat=>(3,String::from("RUST_HEARTBEAT")),
  };
  let period_sec=match kind{
    ObservationKind::Heartbeat=>policy.heartbeat_interval_sec.max(policy.service_interval_sec),
    _=>policy.symbol_cooldown_sec,
  };
  let period_ms=period_sec.checked_mul(1000).ok_or(ObserveError::Config)?;
  if let Some(&last)=self.last_preview_by_topic.get(&key){
    if sample.observed_ms<last{return Err(ObserveError::InvalidSample)}
    if sample.observed_ms-last<period_ms{
      next.russian_telegram_preview=None;
      next.decision_reason="REPLAY_OR_COOLDOWN";
      if let Some(map)=next.analytics.as_object_mut(){
        map.insert("decision_reason".into(),json!("REPLAY_OR_COOLDOWN"));
      }
      return Ok(next)
    }
  }
  if self.last_preview_by_topic.len()>=256
      && !self.last_preview_by_topic.contains_key(&key){
    return Err(ObserveError::InvalidSample)
  }
  self.last_preview_by_topic.insert(key,sample.observed_ms);
  Ok(next)
 }
}

#[cfg(test)]
mod r149_original_telegram_source_tests{
 use super::*;
 const S:&str=include_str!("../config/native_trading.yaml");
 fn policy(input:&str)->NativeObservePolicy{
  NativeObservePolicy::from_config(
   &NativeTradingConfig::parse(input).unwrap()).unwrap()
 }
 fn sample(at:u64)->OfflineNativeSample{
  OfflineNativeSample{
   observed_ms:at,owner_generation:1,symbol:Some("BTC".into()),
   net_spread_milli_bps:Some(50_000),book_age_ms:100,
   decisions:100,ws_to_decision_p99_micros:900,
   worker_queue_p99_micros:800,ws_reconnects:0,worker_drops:0,
   fault_code:None,
  }
 }
 fn reason(p:&NativeObservePolicy,kind:ObservationKind)->&'static str{
  let mut observed=sample(1_000_000);
  if kind==ObservationKind::Fault{
   observed.fault_code=Some("STALE_OWNER".into());
   observed.symbol=None;
   observed.net_spread_milli_bps=None;
  }
  inspect_native_observation_without_io(
    p,kind,&observed,1_000_000).unwrap().decision_reason
 }
 #[test]
 fn r149_five_original_source_channel_flags_independently_change_preview(){
  let base=policy(S);
  assert_eq!(reason(&base,ObservationKind::Candidate),"REVIEW_ONLY_SPREAD");
  assert_eq!(reason(&base,ObservationKind::Fault),"REVIEW_ONLY_FAULT");
  assert_eq!(reason(&base,ObservationKind::Heartbeat),"REVIEW_ONLY_HEARTBEAT");
  let switches=[
   ("telegram:\n  enabled: true","telegram:\n  enabled: false",
     ObservationKind::Candidate,"SIGNALS_DISABLED"),
   ("  send_signals: true","  send_signals: false",
     ObservationKind::Candidate,"SIGNALS_DISABLED"),
   ("  send_errors: true","  send_errors: false",
     ObservationKind::Fault,"ERRORS_DISABLED"),
   ("  send_heartbeat: true","  send_heartbeat: false",
     ObservationKind::Heartbeat,"HEARTBEAT_DISABLED"),
   ("    send_tg: true","    send_tg: false",
     ObservationKind::Heartbeat,"HEARTBEAT_DISABLED"),
  ];
  for (index,(from,to,kind,expected)) in switches.into_iter().enumerate(){
   let altered=S.replacen(from,to,1);
   assert_ne!(altered,S,"original flag absent: {index}");
   let cfg=policy(&altered);
   assert_eq!(reason(&cfg,kind),expected,"wrong channel: {index}");
   assert!(!cfg.can_send_telegram());
   assert!(!cfg.can_trigger_ai_or_trade());
   let mut event=sample(1_000_000);
   if kind==ObservationKind::Fault{
    event.fault_code=Some("STALE_OWNER".into());
    event.symbol=None;event.net_spread_milli_bps=None;
   }
   let report=inspect_native_observation_without_io(&cfg,kind,&event,1_000_000)
     .unwrap();
   assert!(!report.telegram_delivered&&!report.may_trade_or_release);
   assert!(!report.authenticated_exchange_source);
   assert!(report.russian_telegram_preview.is_none());
  }
 }
 fn second_heartbeat_reason(p:&NativeObservePolicy)->&'static str{
  let mut fence=OfflineObservationReplayFence::new(p);
  let first=sample(1_000_000);
  assert_eq!(fence.inspect_once(p,ObservationKind::Heartbeat,&first,1_000_000)
     .unwrap().decision_reason,"REVIEW_ONLY_HEARTBEAT");
  let later=sample(2_900_000); // 1900 sec after first
  let second=fence.inspect_once(p,ObservationKind::Heartbeat,&later,2_900_000)
    .unwrap();
  assert!(!second.telegram_delivered&&!second.may_trade_or_release);
  second.decision_reason
 }
 #[test]
 fn r149_two_original_intervals_change_real_replay_fence_decision_only(){
  let base=policy(S);
  assert_eq!(base.heartbeat_interval_sec,900);
  assert_eq!(base.service_interval_sec,1800);
  assert_eq!(second_heartbeat_reason(&base),"REVIEW_ONLY_HEARTBEAT");
  for (old,new,heartbeat,service) in [
   ("  heartbeat_interval_sec: 900","  heartbeat_interval_sec: 2000",2000,1800),
   ("    interval_sec: 1800","    interval_sec: 2400",900,2400),
  ]{
   let changed=S.replacen(old,new,1);assert_ne!(changed,S);
   let cfg=policy(&changed);
   assert_eq!(cfg.heartbeat_interval_sec,heartbeat);
   assert_eq!(cfg.service_interval_sec,service);
   assert_eq!(second_heartbeat_reason(&cfg),"REPLAY_OR_COOLDOWN");
   assert!(!cfg.can_send_telegram());
  }
 }
 #[test]
 fn r149_source_mutation_rejected_before_telegram_preview(){
  let mut source=NativeTradingConfig::parse(S).unwrap();
  source.raw["telegram"]["send_heartbeat"]=serde_yaml::Value::Bool(false);
  assert_eq!(NativeObservePolicy::from_config(&source),
    Err(ObserveError::Config));
 }
}
