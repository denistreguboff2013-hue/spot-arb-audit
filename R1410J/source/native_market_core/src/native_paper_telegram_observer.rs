//! Astra P1-06: genuine Rust PAPER status -> bounded Russian notice PLAN.
//! NO Telegram Bot API, credentials, private exchange API or trade permit.
//! Cannot certify live PnL: every figure is explicit virtual-paper model.
use crate::native_trading_config::NativeTradingConfig;
use openssl::sha::sha256;
use serde::{Serialize,Deserialize};
use serde_json::Value;

pub const PAPER_STATUS_MAX_AGE_MS:u64=120_000;
pub const PAPER_MIN_ACTIVITY_INTERVAL_MS:u64=30*60*1_000;

#[derive(Clone,Debug,PartialEq,Eq)]
pub enum PaperObserveError{Source,Stale,Policy,Rollback,Overflow}

#[derive(Clone,Debug,Serialize,Deserialize,PartialEq,Eq)]
pub struct PaperPoint{
 pub session:String,pub day:u64,pub updated_wall_ms:u64,
 pub entries:u64,pub closes:u64,pub open_positions:u64,
 pub faults:u64,pub unknown_unhedged:u64,
 pub modeled_milli_usdt:i64,
}
#[derive(Clone,Debug,Serialize,Deserialize,PartialEq,Eq)]
pub struct PaperObserveCursor{
 pub schema:u32,pub last_seen:PaperPoint,pub last_summary:PaperPoint,
}
#[derive(Clone,Debug,Serialize,Deserialize,PartialEq,Eq)]
pub enum PaperNoticeKind{Fault,VirtualActivitySummary,VirtualTradeOpened,VirtualTradeClosed,HourlyHeartbeat,DailyReport}
#[derive(Clone,Debug,Serialize,Deserialize,PartialEq,Eq)]
pub struct PaperNotice{
 pub kind:PaperNoticeKind,pub event_id:String,pub russian_text:String,
 pub external_delivery:bool,
 pub may_trigger_real_trade:bool,
 pub real_pnl_attested:bool,
}
#[derive(Clone,Debug,Serialize,Deserialize,PartialEq,Eq)]
pub struct PaperObserveOutcome{
 pub source:String,
 pub reason:String,
 pub baseline_created:bool,
 pub notices:Vec<PaperNotice>,
 pub next:PaperObserveCursor,
 pub telegram_network_called:bool,
 pub real_trading_authorized:bool,
}

fn code(s:&str)->bool{
 !s.is_empty()&&s.len()<=100&&s.bytes().all(|c|
 c.is_ascii_alphanumeric()||c==b'_'||c==b'-')
}
fn number(value:&Value,key:&str)->Result<u64,PaperObserveError>{
 value.get(key).and_then(Value::as_u64).ok_or(PaperObserveError::Source)
}
fn negative_forbidden(root:&Value,key:&str)->Result<(),PaperObserveError>{
 if root.get(key).and_then(Value::as_bool)!=Some(false){
  return Err(PaperObserveError::Source)
 }
 Ok(())
}
fn required_true(root:&Value,key:&str)->Result<(),PaperObserveError>{
 if root.get(key).and_then(Value::as_bool)!=Some(true){
  return Err(PaperObserveError::Source)
 }
 Ok(())
}
/// Strictly interpreted current-session + active + status read-only
/// trio. Actual exchanges, order/fill history, executable spreads and
/// REAL profit are NEVER inferred from this virtual PAPER source.
pub fn parse_native_paper_status(
 current:&Value,active:&Value,status:&Value,now_ms:u64,
 config:&NativeTradingConfig,
)->Result<PaperPoint,PaperObserveError>{
 if now_ms==0||config.policy.real_execution_authorized{
  return Err(PaperObserveError::Policy)
 }
 let session=current.get("session").and_then(Value::as_str)
  .ok_or(PaperObserveError::Source)?;
 if !code(session)||!session.starts_with("session_")
  ||active.get("session").and_then(Value::as_str)!=Some(session){
  return Err(PaperObserveError::Source)
 }
 let day=number(current,"day")?;
 if day==0||number(active,"day")?!=day
  ||current.get("paper_only")!=Some(&Value::Bool(true))
  ||current.get("virtual_positions_not_exchange_attested")!=Some(&Value::Bool(true)){
   return Err(PaperObserveError::Source)
 }
 negative_forbidden(active,"real_orders")?;
 negative_forbidden(status,"live_trading")?;
 negative_forbidden(status,"private_api")?;
 required_true(status,"virtual_continuity_checkpoint_only")?;
 let paper=status.get("paper_portfolio").ok_or(PaperObserveError::Source)?;
 if number(paper,"day_yekt")?!=day
  ||paper.get("status").and_then(Value::as_str)!=Some("RESEARCH_PUBLIC_PAPER_ONLY"){
   return Err(PaperObserveError::Source)
 }
 negative_forbidden(paper,"paper_fill_attested")?;
 negative_forbidden(paper,"real_profit_proven")?;
 required_true(paper,"no_real_or_private_api")?;
 required_true(paper,"fees_model_assumed_not_certified")?;
 let opened=number(paper,"open_positions")?;
 if opened>number(paper,"max_concurrent")?
  ||number(paper,"max_concurrent")?>10
  ||number(paper,"max_daily_entries")?>500{
   return Err(PaperObserveError::Source)
 }
 let updated=number(status,"updated_wall_ms")?;
 let started=number(current,"started")?
  .checked_mul(1000).ok_or(PaperObserveError::Overflow)?;
 if updated<started||updated>now_ms.saturating_add(2000)
  ||now_ms.saturating_sub(updated)>PAPER_STATUS_MAX_AGE_MS{
   return Err(PaperObserveError::Stale)
 }
 let counters=paper.get("counters").ok_or(PaperObserveError::Source)?;
 let entries=number(counters,"entries")?;
 let closes=number(counters,"closed")?;
 let unknown=number(counters,"unknown_unhedged")?;
 if entries>100_000_000||closes>100_000_000||unknown>1_000_000{
  return Err(PaperObserveError::Source)
 }
 let e=status.get("errors").ok_or(PaperObserveError::Source)?;
 let mut faults=0u64;
 for key in ["decision","ingest","parse"]{
  faults=faults.checked_add(number(e,key)?)
   .ok_or(PaperObserveError::Overflow)?
 }
 if faults>1_000_000_000{return Err(PaperObserveError::Source)}
 let model=counters.get("modeled_realized_usdt")
  .and_then(Value::as_f64).ok_or(PaperObserveError::Source)?;
 if !model.is_finite()||model.abs()>1_000_000.0{
  return Err(PaperObserveError::Source)
 }
 let modeled_milli_usdt=(model*1000.0).round() as i64;
 Ok(PaperPoint{
  session:session.to_owned(),day,updated_wall_ms:updated,
  entries,closes,open_positions:opened,faults,
  unknown_unhedged:unknown,modeled_milli_usdt,
 })
}
fn event_key(prefix:&str,p:&PaperPoint,counter:u64)->String{
 // Identity includes BOTH fault and virtual-leg counters so a close /
 // reopen cannot accidentally reuse the id of a distinct new event.
 let code=format!("{prefix}:{}:{}:{}:{}:{}:{}:{counter}",
  p.session,p.day,p.entries,p.closes,p.faults,p.unknown_unhedged);
 let hash=sha256(code.as_bytes());
 let mut short=String::new();
 for b in &hash[..8]{short.push_str(&format!("{b:02X}"))}
 format!("{prefix}-{short}-{counter}")
}
fn notice(kind:PaperNoticeKind,id:String,text:String)->PaperNotice{
 PaperNotice{
  kind,event_id:id,russian_text:text,
  external_delivery:false,may_trigger_real_trade:false,
  real_pnl_attested:false
 }
}
fn cursor_valid(c:&PaperObserveCursor)->bool{
 c.schema==1&&code(&c.last_seen.session)&&
 c.last_summary.session==c.last_seen.session&&
 c.last_summary.day==c.last_seen.day&&
 c.last_summary.updated_wall_ms<=c.last_seen.updated_wall_ms&&
 c.last_summary.entries<=c.last_seen.entries&&
 c.last_summary.closes<=c.last_seen.closes&&
 c.last_summary.faults<=c.last_seen.faults
}
/// Bounded, deterministic no-network Russian notices. First observation
/// establishes a baseline: no replay of historical entries/closures.
/// Subsequent snapshots must advance monotonically within a session.
pub fn plan_native_paper_notices(
 previous:Option<&PaperObserveCursor>,current:PaperPoint,
 config:&NativeTradingConfig,allow_virtual_summaries:bool,
)->Result<PaperObserveOutcome,PaperObserveError>{
 let policy=crate::native_observability_review::NativeObservePolicy::from_config(config)
  .map_err(|_|PaperObserveError::Policy)?;
 if config.policy.real_execution_authorized{
  return Err(PaperObserveError::Policy)
 }
 let mut notices=Vec::new();
 let (next,reason,baseline_created)=match previous{
  None=>(
   PaperObserveCursor{schema:1,last_seen:current.clone(),last_summary:current},
   "NEW_BASELINE_NO_HISTORICAL_NOTIFICATIONS",true
  ),
  Some(before)=>{
   if !cursor_valid(before){return Err(PaperObserveError::Rollback)}
   if current.session!=before.last_seen.session||
      current.day!=before.last_seen.day{
    (PaperObserveCursor{schema:1,last_seen:current.clone(),last_summary:current},
     "NEW_SESSION_BASELINE_NO_REPLAY",true)
   }else{
    if current.updated_wall_ms<before.last_seen.updated_wall_ms
       ||current.entries<before.last_seen.entries
       ||current.closes<before.last_seen.closes
       ||current.faults<before.last_seen.faults{
      return Err(PaperObserveError::Rollback)
    }
    if current.updated_wall_ms==before.last_seen.updated_wall_ms {
      if current!=before.last_seen{return Err(PaperObserveError::Rollback)}
      return Ok(PaperObserveOutcome{
       source:"LIVE_NATIVE_RUST_PAPER_STATUS_READ_ONLY".into(),
       reason:"UNCHANGED_SNAPSHOT_NO_NOTIFICATION".into(),
       baseline_created:false,notices,
       next:before.clone(),telegram_network_called:false,
       real_trading_authorized:false,
      })
    }
    if policy.telegram_enabled&&policy.telegram_errors&&
      (current.faults>before.last_seen.faults ||
       (current.unknown_unhedged>0&&before.last_seen.unknown_unhedged==0)){
      notices.push(notice(
       PaperNoticeKind::Fault,
       event_key("PFAULT",&current,current.faults
          .saturating_add(current.unknown_unhedged)),
       format!(
        "⚠️ Rust PAPER: новая ошибка наблюдения. Ошибок +{}, виртуальных непокрытых ног {}. Реальная торговля выключена; требуется проверка, не автоматическое действие.",
        current.faults-before.last_seen.faults,
        current.unknown_unhedged
       ),
      ));
    }
    let mut anchor=before.last_summary.clone();
    let interval=config.raw.get("telemetry")
      .and_then(|x|x.get("service"))
      .and_then(|x|x.get("interval_sec"))
      .and_then(|v|v.as_u64())
      .ok_or(PaperObserveError::Policy)?
      .checked_mul(1000).ok_or(PaperObserveError::Overflow)?
      .max(PAPER_MIN_ACTIVITY_INTERVAL_MS);
    if policy.telegram_enabled&&policy.telegram_signals&&
      allow_virtual_summaries&&
      current.updated_wall_ms.saturating_sub(anchor.updated_wall_ms)>=interval
      &&(current.entries>anchor.entries||current.closes>anchor.closes)
    {
      let period_model=current.modeled_milli_usdt
        .saturating_sub(anchor.modeled_milli_usdt);
      notices.push(notice(
       PaperNoticeKind::VirtualActivitySummary,
       event_key("PSUM",&current,current.closes),
       format!(
        "📊 Rust PAPER за период: виртуальных входов +{}, закрытий +{}, сейчас открыто {}. Модельный результат за период {}{}.{:03} USDT; за всю сессию {}{}.{:03} USDT. Это симуляция, НЕ реальная прибыль и НЕ торговый сигнал.",
        current.entries-anchor.entries,current.closes-anchor.closes,
        current.open_positions,
        if period_model<0{"-"}else{"+"},
        period_model.unsigned_abs()/1000,
        period_model.unsigned_abs()%1000,
        if current.modeled_milli_usdt<0{"-"}else{"+"},
        current.modeled_milli_usdt.unsigned_abs()/1000,
        current.modeled_milli_usdt.unsigned_abs()%1000
       ),
      ));
      anchor=current.clone();
    }
    (PaperObserveCursor{
       schema:1,last_seen:current,last_summary:anchor,
     },"DIFFERENTIAL_LOCAL_PAPER_OBSERVATION",false)
   }
  },
 };
 Ok(PaperObserveOutcome{
  source:"LIVE_NATIVE_RUST_PAPER_STATUS_READ_ONLY".into(),
  reason:reason.into(),baseline_created,notices,next,
  telegram_network_called:false,real_trading_authorized:false
 })
}

#[cfg(test)]
mod tests{
 use super::*;
 use serde_json::json;
 fn cfg()->NativeTradingConfig{
  NativeTradingConfig::parse(include_str!("../config/native_trading.yaml")).unwrap()
 }
 fn fixture()->(Value,Value,Value,u64){
  let started=1_790_000_000u64;
  let now_ms=started*1000+120_000;
  let current=json!({
   "session":"session_20741_1790000000","day":20741,
   "started":started,"paper_only":true,
   "virtual_positions_not_exchange_attested":true
  });
  let active=json!({
   "session":"session_20741_1790000000","day":20741,
   "real_orders":false
  });
  let status=json!({
   "updated_wall_ms":now_ms-1000,"live_trading":false,
   "private_api":false,"virtual_continuity_checkpoint_only":true,
   "errors":{"decision":0,"ingest":0,"parse":0},
   "paper_portfolio":{
    "day_yekt":20741,"status":"RESEARCH_PUBLIC_PAPER_ONLY",
    "paper_fill_attested":false,"real_profit_proven":false,
    "no_real_or_private_api":true,"fees_model_assumed_not_certified":true,
    "open_positions":0,"max_concurrent":10,"max_daily_entries":500,
    "counters":{
     "entries":30,"closed":30,"unknown_unhedged":0,
     "modeled_realized_usdt":1.6717
    }
   }
  });
  (current,active,status,now_ms)
 }
 fn baseline()->(NativeTradingConfig,PaperPoint){
  let (c,a,s,now)=fixture();
  let config=cfg();
  let point=parse_native_paper_status(&c,&a,&s,now,&config).unwrap();
  (config,point)
 }
 #[test]
 fn real_paper_status_source_flags_session_freshness_are_strict(){
  let (c,a,s,now)=fixture();
  let config=cfg();
  let p=parse_native_paper_status(&c,&a,&s,now,&config).unwrap();
  assert_eq!(p.entries,30);
  assert_eq!(p.closes,30);
  assert_eq!(p.modeled_milli_usdt,1672);
  let mut wrong=s.clone();
  wrong["live_trading"]=json!(true);
  assert_eq!(parse_native_paper_status(&c,&a,&wrong,now,&config),
   Err(PaperObserveError::Source));
  let mut wrong=s.clone();
  wrong["paper_portfolio"]["real_profit_proven"]=json!(true);
  assert_eq!(parse_native_paper_status(&c,&a,&wrong,now,&config),
   Err(PaperObserveError::Source));
  let mut wrong=s.clone();
  wrong["paper_portfolio"]["paper_fill_attested"]=json!(true);
  assert_eq!(parse_native_paper_status(&c,&a,&wrong,now,&config),
   Err(PaperObserveError::Source));
  let mut wrong=a.clone();
  wrong["real_orders"]=json!(true);
  assert_eq!(parse_native_paper_status(&c,&wrong,&s,now,&config),
   Err(PaperObserveError::Source));
  let mut wrong=c.clone();
  wrong["session"]=json!("fake_other_session");
  assert_eq!(parse_native_paper_status(&wrong,&a,&s,now,&config),
   Err(PaperObserveError::Source));
  let mut wrong=s.clone();
  wrong["updated_wall_ms"]=json!(now-120_001);
  assert_eq!(parse_native_paper_status(&c,&a,&wrong,now,&config),
   Err(PaperObserveError::Stale));
  let mut wrong=s.clone();
  wrong["errors"]["decision"]=json!(-1);
  assert_eq!(parse_native_paper_status(&c,&a,&wrong,now,&config),
   Err(PaperObserveError::Source));
  let mut bad=cfg();
  bad.policy.real_execution_authorized=true;
  assert_eq!(parse_native_paper_status(&c,&a,&s,now,&bad),
   Err(PaperObserveError::Policy));
 }
 #[test]
 fn first_baseline_no_spam_and_same_sample_suppressed(){
  let (config,point)=baseline();
  let initial=plan_native_paper_notices(None,point.clone(),&config,false).unwrap();
  assert!(initial.baseline_created);
  assert!(initial.notices.is_empty());
  let again=plan_native_paper_notices(Some(&initial.next),point.clone(),
   &config,true).unwrap();
  assert_eq!(again.reason,"UNCHANGED_SNAPSHOT_NO_NOTIFICATION");
  assert!(again.notices.is_empty());
  assert!(!again.telegram_network_called&&!again.real_trading_authorized);
  let mut edited=point;
  edited.entries+=1;
  assert_eq!(plan_native_paper_notices(Some(&initial.next),edited,&config,true),
   Err(PaperObserveError::Rollback));
 }
 #[test]
 fn one_fault_without_heartbeat_and_no_repeat_after_resume(){
  let (config,start)=baseline();
  let init=plan_native_paper_notices(None,start.clone(),&config,false).unwrap();
  let mut fault=start.clone();
  fault.updated_wall_ms+=5_000;
  fault.faults+=1;
  let first=plan_native_paper_notices(Some(&init.next),fault.clone(),&config,false).unwrap();
  assert_eq!(first.notices.len(),1);
  assert_eq!(first.notices[0].kind,PaperNoticeKind::Fault);
  assert!(first.notices[0].event_id.bytes().all(|b|
    b.is_ascii_uppercase()||b.is_ascii_digit()||b==b'-'||b==b'_'));
  assert!(first.notices[0].russian_text.contains("ошибка"));
  assert!(!first.notices[0].external_delivery);
  assert!(!first.notices[0].real_pnl_attested);
  let second=plan_native_paper_notices(Some(&first.next),fault.clone(),&config,false).unwrap();
  assert_eq!(second.notices.len(),0);
  fault.updated_wall_ms+=60_000;
  assert!(plan_native_paper_notices(Some(&second.next),fault,&config,false)
   .unwrap().notices.is_empty());
 }
 #[test]
 fn paper_activity_coalesces_for_30_minutes_when_explicitly_enabled(){
  let (config,start)=baseline();
  let mut state=plan_native_paper_notices(None,start.clone(),&config,true).unwrap().next;
  for minute in 1..30{
   let mut point=start.clone();
   point.updated_wall_ms+=minute*60_000;
   point.entries+=minute;
   point.closes+=minute;
   let out=plan_native_paper_notices(Some(&state),point,&config,true).unwrap();
   assert!(out.notices.is_empty(),"no minute-by-minute Telegram spam");
   state=out.next;
  }
  let mut hour=start.clone();
  hour.updated_wall_ms+=30*60_000;
  hour.entries+=30;hour.closes+=30;
  hour.modeled_milli_usdt=-1255;
  let out=plan_native_paper_notices(Some(&state),hour.clone(),&config,true).unwrap();
  assert_eq!(out.notices.len(),1);
  assert_eq!(out.notices[0].kind,PaperNoticeKind::VirtualActivitySummary);
  assert!(out.notices[0].russian_text.contains("НЕ реальная прибыль"));
  assert!(out.notices[0].russian_text.contains("-2.927 USDT"),
   "period model must subtract the prior cumulative anchor");
  assert!(out.notices[0].russian_text.contains("-1.255 USDT"),
   "session model remains explicitly cumulative");
  assert!(!out.notices[0].external_delivery);
  hour.updated_wall_ms+=60_000;
  let follow=plan_native_paper_notices(Some(&out.next),hour,&config,true).unwrap();
  assert!(follow.notices.is_empty());
 }
 #[test]
 fn summary_opt_out_and_new_session_never_replay_historical_model(){
  let (config,start)=baseline();
  let init=plan_native_paper_notices(None,start.clone(),&config,false).unwrap();
  let mut later=start.clone();
  later.updated_wall_ms+=3_600_000;later.entries+=100;later.closes+=100;
  let opted_out=plan_native_paper_notices(Some(&init.next),
   later.clone(),&config,false).unwrap();
  assert!(opted_out.notices.is_empty());
  let mut changed_session=later;
  changed_session.session="session_20742_1790004000".into();
  changed_session.day+=1;
  changed_session.entries=0;changed_session.closes=0;
  let boundary=plan_native_paper_notices(Some(&opted_out.next),
   changed_session,&config,true).unwrap();
  assert!(boundary.baseline_created&&boundary.notices.is_empty());
  let mut rollback=start;
  rollback.updated_wall_ms-=1;
  assert_eq!(plan_native_paper_notices(Some(&init.next),rollback,&config,true),
   Err(PaperObserveError::Rollback));
 }
}
