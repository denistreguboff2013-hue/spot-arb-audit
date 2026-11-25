//! Astra R37 — Rust PAPER read-only Telegram planning with durable pending queue.
//! No Telegram network, tokens, Binance/OKX account access, or trade execution.
//! Pending notices are intentionally NOT sent and NEVER silently replayed.
use crate::{
 native_paper_telegram_observer::{
  plan_native_paper_notices,PaperNotice,PaperNoticeKind,PaperPoint,
  PaperObserveCursor,
 },
 native_trading_config::NativeTradingConfig,
 native_paper_oldstyle_notice_review::{OldStyleCursor,advance_old_style_with_clock},
 native_paper_daily_calendar_review::{PaperDailyCalendar,advance_calendar},
 native_virtual_four_leg_provenance_review::{AuditState,ModeledTradeNoticeRow},
};
use serde::{Serialize,Deserialize};
use std::collections::BTreeMap;

pub const MAX_PENDING_NOTICES:usize=128;
pub const R37_SCHEMA:u32=1;
// R38 refuses notice delivery after 10 minutes. Keep undeliverable
// pending events another 10 minutes for local diagnostics, then
// prune from the preview queue to prevent permanent capacity loss.
// Durable Bot API claims/ACKs remain in the separate Telegram ledger.
pub const EXPIRED_PREVIEW_PRUNE_AGE_MS:u64=1_200_000;

#[derive(Clone,Debug,Serialize,Deserialize,PartialEq,Eq)]
pub struct PendingPaperNotice {
 pub notice:PaperNotice,
 pub detected_at_wall_ms:u64,
 pub send_attempted:bool,
}
#[derive(Clone,Debug,Serialize,Deserialize,PartialEq,Eq)]
pub struct PaperWatchState {
 pub schema:u32,
 pub config_sha256:String,
 pub revision:u64,
 /// Added after R37 in a backward-compatible way: existing on-disk
 /// queue files deserialize with false. First opt-in never replays
 /// virtual closes observed before the operator enabled summaries.
 #[serde(default)]
 pub virtual_summary_opt_in:bool,
 /// R40 old-bot-style PAPER openings/closings, hourly HB, daily report.
 /// Absence in existing deployed R37 journals means first-run baseline.
 #[serde(default)]
 pub oldstyle:Option<OldStyleCursor>,
 /// R44: durable global heartbeat anchor, independent of PAPER session.
 #[serde(default)]
 pub global_last_heartbeat_ms:Option<u64>,
 /// R44 global EKT day accumulator of VERIFIED new model stage events.
 #[serde(default)]
 pub calendar:Option<PaperDailyCalendar>,
 pub cursor:PaperObserveCursor,
 pub pending:BTreeMap<String,PendingPaperNotice>,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct PaperWatchStep {
 pub next:PaperWatchState,
 pub newly_queued:Vec<String>,
 pub baseline_created:bool,
 pub reason:String,
 pub telegram_network_called:bool,
 pub real_orders_permitted:bool,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum PaperWatchError {UnsafeState,Cursor,Full,Conflict,Overflow}

fn valid_id(id:&str)->bool{
 !id.is_empty()&&id.len()<=120&&id.bytes().all(|b|
 b.is_ascii_uppercase()||b.is_ascii_digit()||b==b'-'||b==b'_')
}
fn validate_state(state:&PaperWatchState,config:&NativeTradingConfig)->Result<(),PaperWatchError>{
 if state.schema!=R37_SCHEMA||state.revision==0||
  state.config_sha256!=config.sha256||
  state.pending.len()>MAX_PENDING_NOTICES||
  state.cursor.schema!=1||
  state.cursor.last_seen.session!=state.cursor.last_summary.session||
  state.cursor.last_seen.day!=state.cursor.last_summary.day{
  return Err(PaperWatchError::UnsafeState)
 }
 if state.global_last_heartbeat_ms==Some(0){return Err(PaperWatchError::UnsafeState)}
 if let Some(c)=&state.calendar{
  if c.day>state.cursor.last_seen.day||c.session_count==0||
   c.first_seen_ms>c.last_seen_ms||c.last_session.is_empty()||
   c.last_reported_day.is_some_and(|d|d>c.day){
   return Err(PaperWatchError::UnsafeState)
  }
 }
 if let Some(old)=&state.oldstyle{
  if old.session!=state.cursor.last_seen.session||
    old.verified_journal_seq==0||old.last_heartbeat_ms==0||
    old.baseline_entries>state.cursor.last_seen.entries||
    old.baseline_closed>state.cursor.last_seen.closes{
   return Err(PaperWatchError::UnsafeState)
  }
 }
 for (id,record) in &state.pending{
  if !valid_id(id)||record.notice.event_id!=*id||
    record.detected_at_wall_ms==0||
    record.detected_at_wall_ms>state.cursor.last_seen.updated_wall_ms||
    record.send_attempted||
    record.notice.external_delivery||
    record.notice.may_trigger_real_trade||
    record.notice.real_pnl_attested||
    record.notice.russian_text.is_empty()||
    record.notice.russian_text.len()>2048{
    return Err(PaperWatchError::UnsafeState)
  }
  let prefix=match record.notice.kind{
   PaperNoticeKind::Fault=>"PFAULT-",
   PaperNoticeKind::VirtualActivitySummary=>"PSUM-",
   PaperNoticeKind::VirtualTradeOpened=>"POPEN-",
   PaperNoticeKind::VirtualTradeClosed=>"PCLOSE-",
   PaperNoticeKind::HourlyHeartbeat=>"PBEAT-",
   PaperNoticeKind::DailyReport=>"PDAY-",
  };
  if !id.starts_with(prefix){return Err(PaperWatchError::UnsafeState)}
 }
 Ok(())
}
/// Pure deterministic transaction: caller must fsync resulting state atomically
/// before publishing any notice. Recovery never replays a historical snapshot
/// as a new network send; this module cannot send.
pub fn advance_native_paper_watch(
 previous:Option<&PaperWatchState>,
 point:PaperPoint,
 config:&NativeTradingConfig,
 allow_virtual_summaries:bool,
)->Result<PaperWatchStep,PaperWatchError>{
 if let Some(state)=previous{validate_state(state,config)?}
 // Transition OFF -> ON gets a new activity anchor at the current
 // snapshot. Still observe fresh faults on that same step, but NEVER
 // send historical virtual close summaries accumulated while OFF.
 let first_virtual_opt_in=allow_virtual_summaries&&
   previous.map(|s|!s.virtual_summary_opt_in).unwrap_or(false);
 let plan=plan_native_paper_notices(
   previous.map(|s|&s.cursor),point,config,
   allow_virtual_summaries&&!first_virtual_opt_in
 ).map_err(|_|PaperWatchError::Cursor)?;
 let mut cursor=plan.next.clone();
 if first_virtual_opt_in{
  cursor.last_summary=cursor.last_seen.clone();
 }
 let mut pending=previous.map(|s|s.pending.clone()).unwrap_or_default();
 // All notices older than 20min are ALREADY ineligible for R38
 // dispatch (10min ceiling) and must not fill this bounded queue
 // forever. This does not mutate the independent Telegram ACK ledger.
 pending.retain(|_,v|cursor.last_seen.updated_wall_ms
  .saturating_sub(v.detected_at_wall_ms)<=EXPIRED_PREVIEW_PRUNE_AGE_MS);
 let mut newly_queued=Vec::new();
 for notice in &plan.notices{
  if !valid_id(&notice.event_id)||notice.external_delivery||
    notice.may_trigger_real_trade||notice.real_pnl_attested||
    notice.russian_text.len()>2048{
    return Err(PaperWatchError::UnsafeState)
  }
  if let Some(old)=pending.get(&notice.event_id){
   if old.notice!=*notice{return Err(PaperWatchError::Conflict)}
   continue;
  }
  if pending.len()>=MAX_PENDING_NOTICES{return Err(PaperWatchError::Full)}
  let id=notice.event_id.clone();
  pending.insert(id.clone(),PendingPaperNotice{
   notice:notice.clone(),detected_at_wall_ms:plan.next.last_seen.updated_wall_ms,
   send_attempted:false,
  });
  newly_queued.push(id);
 }
 let revision=previous.map(|p|p.revision).unwrap_or(0)
  .checked_add(1).ok_or(PaperWatchError::Overflow)?;
 let next=PaperWatchState{
  schema:R37_SCHEMA,config_sha256:config.sha256.clone(),revision,
  virtual_summary_opt_in:allow_virtual_summaries,
  global_last_heartbeat_ms:previous.and_then(|s|s.global_last_heartbeat_ms
   .or_else(||s.oldstyle.as_ref().map(|o|o.last_heartbeat_ms))),
  calendar:previous.and_then(|s|s.calendar.clone()),
  oldstyle:previous.and_then(|s|if s.cursor.last_seen.session==cursor.last_seen.session{s.oldstyle.clone()}else{None}),cursor,pending,
 };
 validate_state(&next,config)?;
 Ok(PaperWatchStep{
  next,newly_queued,baseline_created:plan.baseline_created,
  reason:plan.reason,telegram_network_called:false,
  real_orders_permitted:false,
 })
}


/// R40 verified-model ledger extension; R37 atomic fsync now covers
/// last verified seq, new trade notices, heartbeat and daily report.
/// No Telegram network access here. First activation creates a baseline.
pub fn advance_native_paper_watch_oldstyle(
 previous:Option<&PaperWatchState>,point:PaperPoint,
 config:&NativeTradingConfig,verified:&AuditState,
 trades:&[ModeledTradeNoticeRow],now_ms:u64,
)->Result<PaperWatchStep,PaperWatchError>{
 let mut step=advance_native_paper_watch(previous,point.clone(),config,false)?;
 let old=advance_old_style_with_clock(
   step.next.oldstyle.as_ref(),step.next.global_last_heartbeat_ms,
   &point,verified,trades,now_ms
 ).map_err(|_|PaperWatchError::Cursor)?;
 step.next.global_last_heartbeat_ms=Some(old.next.last_heartbeat_ms);
 step.next.oldstyle=Some(old.next);
 // Count only rows newly verified in the SAME session and local day.
 // No timestamp exists in model rows: after midnight or a new session,
 // baseline without inventing which calendar day those rows belong to.
 let eligible:Vec<&ModeledTradeNoticeRow>=match previous{
  Some(s) if s.calendar.as_ref().is_some_and(|c|c.day==point.day)=>
   match &s.oldstyle{
    Some(cursor) if cursor.session==point.session=>trades.iter()
     .filter(|r|r.seq>cursor.verified_journal_seq).collect(),
    _=>Vec::new(),
   },
  _=>Vec::new(),
 };
 let (calendar,daily_notices)=advance_calendar(
  previous.and_then(|s|s.calendar.as_ref()),&point,&eligible,now_ms,
  previous.and_then(|s|s.oldstyle.as_ref().and_then(|c|c.last_report_local_day))
 ).map_err(|_|PaperWatchError::Cursor)?;
 step.next.calendar=Some(calendar);
 for notice in old.notices.into_iter().filter(|n|n.kind!=PaperNoticeKind::DailyReport)
  .chain(daily_notices.into_iter()){
  if !valid_id(&notice.event_id)||
   notice.external_delivery||notice.may_trigger_real_trade||
   notice.real_pnl_attested||notice.russian_text.is_empty()||
   notice.russian_text.len()>2048{
   return Err(PaperWatchError::UnsafeState)
  }
  let id=notice.event_id.clone();
  if let Some(prior)=step.next.pending.get(&id){
   if prior.notice!=notice{return Err(PaperWatchError::Conflict)}
   continue
  }
  if step.next.pending.len()>=MAX_PENDING_NOTICES{
   return Err(PaperWatchError::Full)
  }
  step.next.pending.insert(id.clone(),PendingPaperNotice{
   notice,detected_at_wall_ms:point.updated_wall_ms,
   send_attempted:false,
  });
  step.newly_queued.push(id);
 }
 validate_state(&step.next,config)?;
 step.baseline_created|=old.baseline_created;
 step.reason="VERIFIED_MODELED_TRADE_EVENTS_PLUS_HOURLY_AND_DAILY".into();
 Ok(step)
}

#[cfg(test)]
mod tests{
 use super::*;
 fn cfg()->NativeTradingConfig{
  NativeTradingConfig::parse(
   include_str!("../config/native_trading.yaml")).unwrap()
 }
 fn snapshot()->PaperPoint{
  PaperPoint{
   session:"session_20741_1792069401".into(),day:20741,
   updated_wall_ms:1_792_070_000_000,
   entries:30,closes:30,open_positions:0,
   faults:0,unknown_unhedged:0,modeled_milli_usdt:1672,
  }
 }
 #[test]
 fn r44_watch_state_clock_calendar_survive_rotation_and_serde_restart(){
  use std::collections::BTreeMap;
  let c=cfg();let a=snapshot();
  let audit=|p:&PaperPoint|AuditState{sequence:1,last_hash:"1".repeat(64),
   entries:p.entries,closed:p.closes,open:p.open_positions as usize,
   sealed:false,model_phase_by_base:BTreeMap::new(),session:p.session.clone(),
   modeled_realized_bits:(p.modeled_milli_usdt as f64/1000.0).to_bits(),
   strategy_config_sha256:None,
   strategy_quarantined_bases:std::collections::BTreeSet::new()};
  let first=advance_native_paper_watch_oldstyle(None,a.clone(),&c,&audit(&a),
   &[],a.updated_wall_ms).unwrap();
  assert!(first.newly_queued.is_empty());
  let mut b=a.clone();b.session="session_20741_1792069402".into();
  b.updated_wall_ms+=1_800_000;b.entries=500;b.closes=0;
  let middle=advance_native_paper_watch_oldstyle(Some(&first.next),b.clone(),
   &c,&audit(&b),&[],b.updated_wall_ms).unwrap();
  assert!(middle.newly_queued.is_empty());
  assert_eq!(middle.next.calendar.as_ref().unwrap().session_count,2);
  let mut end=b.clone();end.session="session_20741_1792069403".into();
  end.updated_wall_ms=a.updated_wall_ms+3_600_001;
  let due=advance_native_paper_watch_oldstyle(Some(&middle.next),end.clone(),
   &c,&audit(&end),&[],end.updated_wall_ms).unwrap();
  assert_eq!(due.newly_queued.len(),1);
  assert!(due.newly_queued[0].starts_with("PBEAT-"));
  assert_eq!(due.next.calendar.as_ref().unwrap().opened,0);
  let resumed:PaperWatchState=serde_json::from_slice(
   &serde_json::to_vec(&due.next).unwrap()).unwrap();
  end.updated_wall_ms+=1_000;
  let again=advance_native_paper_watch_oldstyle(Some(&resumed),end.clone(),
   &c,&audit(&end),&[],end.updated_wall_ms).unwrap();
  assert!(again.newly_queued.is_empty());
 }
 #[test]
 fn first_start_is_no_spam_even_after_thirty_old_paper_closes(){
  let p=snapshot();let c=cfg();
  let start=advance_native_paper_watch(None,p.clone(),&c,false).unwrap();
  assert!(start.baseline_created);
  assert!(start.newly_queued.is_empty());
  assert_eq!(start.next.revision,1);
  assert_eq!(start.next.pending.len(),0);
  assert!(!start.telegram_network_called&&!start.real_orders_permitted);
  let resumed=advance_native_paper_watch(Some(&start.next),p,&c,false).unwrap();
  assert!(!resumed.baseline_created);
  assert!(resumed.newly_queued.is_empty());
  assert!(resumed.next.pending.is_empty());
 }
 #[test]
 fn real_fault_is_queued_once_durably_across_same_status_and_restarts(){
  let c=cfg();let p=snapshot();
  let start=advance_native_paper_watch(None,p.clone(),&c,false).unwrap();
  let mut newer=p.clone();newer.updated_wall_ms+=10_000;
  newer.faults=1;
  let first=advance_native_paper_watch(Some(&start.next),newer.clone(),&c,false).unwrap();
  assert_eq!(first.newly_queued.len(),1);
  assert_eq!(first.next.pending.len(),1);
  let event=first.next.pending.values().next().unwrap();
  assert!(event.notice.russian_text.starts_with("⚠️ Rust PAPER"));
  assert!(event.notice.event_id.starts_with("PFAULT-"));
  assert!(!event.send_attempted);
  let bytes=serde_json::to_vec(&first.next).unwrap();
  let reopened:PaperWatchState=serde_json::from_slice(&bytes).unwrap();
  let second=advance_native_paper_watch(Some(&reopened),newer,&c,false).unwrap();
  assert!(second.newly_queued.is_empty());
  assert_eq!(second.next.pending.len(),1);
  assert_eq!(second.next.pending.values().next().unwrap(),event);
  assert!(!second.telegram_network_called);
 }
 #[test]
 fn malformed_or_modified_pending_state_denied_before_advancing(){
  let c=cfg();let p=snapshot();
  let start=advance_native_paper_watch(None,p.clone(),&c,false).unwrap();
  let mut newer=p.clone();newer.updated_wall_ms+=10_000;newer.faults=1;
  let good=advance_native_paper_watch(Some(&start.next),newer.clone(),&c,false).unwrap();
  let mut tampered=good.next.clone();
  tampered.pending.values_mut().next().unwrap().send_attempted=true;
  assert_eq!(advance_native_paper_watch(Some(&tampered),
   newer.clone(),&c,false),Err(PaperWatchError::UnsafeState));
  let mut tampered=good.next.clone();
  tampered.pending.values_mut().next().unwrap().notice.real_pnl_attested=true;
  assert_eq!(advance_native_paper_watch(Some(&tampered),
   newer.clone(),&c,false),Err(PaperWatchError::UnsafeState));
  let mut tampered=good.next.clone();tampered.config_sha256="0".repeat(64);
  assert_eq!(advance_native_paper_watch(Some(&tampered),
   newer,&c,false),Err(PaperWatchError::UnsafeState));
 }
 #[test]
 fn paper_counter_rollback_and_queue_capacity_fail_closed(){
  let c=cfg();let p=snapshot();
  let state=advance_native_paper_watch(None,p.clone(),&c,false).unwrap();
  let mut rollback=p.clone();rollback.updated_wall_ms+=20_000;
  rollback.entries-=1;
  assert_eq!(advance_native_paper_watch(Some(&state.next),
   rollback,&c,false),Err(PaperWatchError::Cursor));
  let mut full=state.next.clone();
  for n in 0..MAX_PENDING_NOTICES{
   let id=format!("PFAULT-{:016X}-{}",n,n);
   let note=PaperNotice{
    kind:PaperNoticeKind::Fault,event_id:id.clone(),
    russian_text:"⚠️ Rust PAPER: новая ошибка наблюдения. Реальная торговля выключена.".into(),
    external_delivery:false,may_trigger_real_trade:false,real_pnl_attested:false,
   };
   full.pending.insert(id,PendingPaperNotice{
    notice:note,detected_at_wall_ms:p.updated_wall_ms,send_attempted:false
   });
  }
  let mut fault=p;fault.updated_wall_ms+=8_000;fault.faults=1;
  assert_eq!(advance_native_paper_watch(Some(&full),fault,&c,false),
   Err(PaperWatchError::Full));
 }
 #[test]
 fn expired_unsent_preview_is_pruned_only_after_dispatch_expiry_window(){
  let c=cfg();let p=snapshot();
  let start=advance_native_paper_watch(None,p.clone(),&c,false).unwrap();
  let mut fault=p.clone();fault.faults=1;fault.updated_wall_ms+=5_000;
  let queued=advance_native_paper_watch(Some(&start.next),fault.clone(),&c,false)
   .unwrap();
  assert_eq!(queued.next.pending.len(),1);
  let mut edge=fault.clone();
  edge.updated_wall_ms+=EXPIRED_PREVIEW_PRUNE_AGE_MS;
  let kept=advance_native_paper_watch(Some(&queued.next),edge.clone(),&c,false)
   .unwrap();
  assert_eq!(kept.next.pending.len(),1,"retain at exact 20min boundary");
  edge.updated_wall_ms+=1;
  let pruned=advance_native_paper_watch(Some(&kept.next),edge,&c,false)
   .unwrap();
  assert_eq!(pruned.next.pending.len(),0);
  assert!(pruned.newly_queued.is_empty());
  assert!(!pruned.telegram_network_called);
 }
 #[test]
 fn prior_r37_queue_deserializes_and_opt_in_does_not_replay(){
  let config=cfg();
  let current=snapshot();
  let previous=advance_native_paper_watch(None,current.clone(),&config,false)
   .unwrap().next;
  let mut historical_json=serde_json::to_value(&previous).unwrap();
  historical_json.as_object_mut().unwrap().remove("virtual_summary_opt_in");
  let historical:PaperWatchState=serde_json::from_value(historical_json).unwrap();
  assert!(!historical.virtual_summary_opt_in);
  let mut snapshot_late=current.clone();
  snapshot_late.updated_wall_ms+=4_000_000;
  snapshot_late.closes+=9;snapshot_late.entries+=9;
  let enabled=advance_native_paper_watch(
   Some(&historical),snapshot_late.clone(),&config,true).unwrap();
  assert!(enabled.next.virtual_summary_opt_in);
  assert!(enabled.newly_queued.is_empty());
  assert_eq!(enabled.next.cursor.last_summary.closes,snapshot_late.closes);
  let again=advance_native_paper_watch(
   Some(&enabled.next),snapshot_late.clone(),&config,true).unwrap();
  assert!(again.newly_queued.is_empty());
  let disabled=advance_native_paper_watch(
   Some(&again.next),snapshot_late.clone(),&config,false).unwrap();
  assert!(!disabled.next.virtual_summary_opt_in);
  let mut one_more=snapshot_late;
  one_more.updated_wall_ms+=3_600_000;
  one_more.entries+=2;one_more.closes+=2;
  let reenabled=advance_native_paper_watch(
   Some(&disabled.next),one_more.clone(),&config,true).unwrap();
  assert!(reenabled.newly_queued.is_empty(),"disabled interval does not replay");
  let mut newly=one_more;
  newly.updated_wall_ms+=1_800_000;newly.entries+=1;newly.closes+=1;
  let outgoing=advance_native_paper_watch(
   Some(&reenabled.next),newly,&config,true).unwrap();
  assert_eq!(outgoing.newly_queued.len(),1);
 }
 #[test]
 fn virtual_summary_requires_extra_enable_and_session_restart_baselines(){
  let c=cfg();let p=snapshot();
  let start=advance_native_paper_watch(None,p.clone(),&c,false).unwrap();
  let mut later=p.clone();later.updated_wall_ms+=1_800_000;
  later.entries+=10;later.closes+=10;
  let disabled=advance_native_paper_watch(Some(&start.next),
   later.clone(),&c,false).unwrap();
  assert!(disabled.next.pending.is_empty());
  let enabled=advance_native_paper_watch(Some(&start.next),
   later.clone(),&c,true).unwrap();
  assert!(enabled.newly_queued.is_empty(),
   "first opt-in must not replay old PAPER closes");
  assert!(enabled.next.virtual_summary_opt_in);
  assert_eq!(enabled.next.cursor.last_summary.entries,later.entries);
  let mut next=later.clone();
  next.updated_wall_ms+=1_800_000;next.entries+=3;next.closes+=3;
  next.modeled_milli_usdt+=75;
  let summary=advance_native_paper_watch(Some(&enabled.next),
   next.clone(),&c,true).unwrap();
  assert_eq!(summary.newly_queued.len(),1);
  assert!(summary.next.pending.values().next().unwrap().notice.russian_text
   .contains("НЕ реальная прибыль"));
  let mut next_session=next;
  next_session.session="session_20742_1792071401".into();next_session.day+=1;
  next_session.entries=0;next_session.closes=0;
  let reset=advance_native_paper_watch(Some(&summary.next),
   next_session,&c,true).unwrap();
  assert!(reset.baseline_created&&reset.newly_queued.is_empty());
  assert_eq!(reset.next.pending.len(),1,
   "historical unsent notification remains a separate review queue item");
 }
}
