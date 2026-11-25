//! R40: operator-requested classic Telegram behavior for Rust PAPER.
//! One message for each VERIFIED modeled opening / closing, hourly
//! heartbeat and a daily 23:59 Yekaterinburg report. Zero real trades.
//! Source four-leg rows must come from the verified model audit hash
//! chain, NOT current mark price, public spread or exchange fills.
use crate::{
 native_paper_telegram_observer::{PaperNotice,PaperNoticeKind,PaperPoint},
 native_virtual_four_leg_provenance_review::{AuditState,ModeledTradeNoticeRow},
};
use serde::{Serialize,Deserialize};
use openssl::sha::sha256;

const HOUR_MS:u64=3_600_000;
const DAY_MS:u64=86_400_000;
const YEKT_OFFSET_MS:u64=5*HOUR_MS;
#[derive(Clone,Debug,Serialize,Deserialize,PartialEq,Eq)]
pub struct OldStyleCursor{
 pub session:String,
 pub verified_journal_seq:u64,
 pub last_heartbeat_ms:u64,
 pub last_report_local_day:Option<u64>,
 pub baseline_local_day:u64,
 pub baseline_entries:u64,
 pub baseline_closed:u64,
 pub baseline_modeled_milli:i64,
 pub last_snapshot_ms:u64,
 pub last_entries:u64,
 pub last_closed:u64,
 pub last_open:u64,
 pub last_modeled_milli:i64,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct OldStyleOutcome{
 pub next:OldStyleCursor,
 pub notices:Vec<PaperNotice>,
 pub baseline_created:bool,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum OldStyleError{Unverified,Rollback,Clock,Overflow,Capacity}
fn local_day(ms:u64)->Result<u64,OldStyleError>{
 Ok(ms.checked_add(YEKT_OFFSET_MS).ok_or(OldStyleError::Clock)?/DAY_MS)
}
fn minute_in_day(ms:u64)->Result<u64,OldStyleError>{
 Ok(ms.checked_add(YEKT_OFFSET_MS).ok_or(OldStyleError::Clock)?
  %DAY_MS/60_000)
}
fn hash12(s:&str)->String{
 sha256(s.as_bytes())[..8].iter().map(|b|format!("{b:02X}")).collect()
}
fn notice(kind:PaperNoticeKind,event_id:String,body:String)->PaperNotice{
 PaperNotice{kind,event_id,russian_text:body,
   external_delivery:false,may_trigger_real_trade:false,
   real_pnl_attested:false}
}
fn model_money(milli:i64)->String{
 format!("{}{}.{:03}",if milli<0{"-"}else{"+"},
   milli.unsigned_abs()/1000,milli.unsigned_abs()%1000)
}
fn model_money4(val:f64)->String{format!("{val:+.4}")}
fn session_start_ms(s:&str)->Option<u64>{
 s.rsplit('_').next()?.parse::<u64>().ok()?.checked_mul(1000)
}
pub(crate) fn yekt_date(day:u64)->Result<String,OldStyleError>{
 let adjusted_secs=day.checked_mul(86_400).ok_or(OldStyleError::Clock)? as libc::time_t;
 let mut t=std::mem::MaybeUninit::<libc::tm>::uninit();
 let result=unsafe{libc::gmtime_r(&adjusted_secs as *const _,t.as_mut_ptr())};
 if result.is_null(){return Err(OldStyleError::Clock)}
 let t=unsafe{t.assume_init()};
 Ok(format!("{:04}-{:02}-{:02}",t.tm_year+1900,t.tm_mon+1,t.tm_mday))
}
fn heartbeat(point:&PaperPoint,now_ms:u64)->PaperNotice{
 let slot=now_ms/HOUR_MS;
 notice(PaperNoticeKind::HourlyHeartbeat,
  format!("PBEAT-{}-{slot}",hash12("R44_GLOBAL_PAPER_CLOCK")),
  format!(
   "❤️ Rust PAPER работает\nСчётчик PAPER (возможен защитный лимит, не число сделок): {}\nЗакрытий по счётчику сессии: {}\nСейчас открыто: {}\nОшибок наблюдения: {}\nРеальная торговля отключена.",
   point.entries,point.closes,point.open_positions,point.faults
  ))
}
fn daily(id_day:u64,current:&PaperPoint,anchor:&OldStyleCursor)
 ->Result<PaperNotice,OldStyleError>{
 let dn_entries=current.entries.checked_sub(anchor.baseline_entries)
  .ok_or(OldStyleError::Rollback)?;
 let dn_closes=current.closes.checked_sub(anchor.baseline_closed)
  .ok_or(OldStyleError::Rollback)?;
 let modeled=current.modeled_milli_usdt
  .saturating_sub(anchor.baseline_modeled_milli);
 Ok(notice(PaperNoticeKind::DailyReport,
  format!("PDAY-{}-{id_day}",hash12(&anchor.session)),
  format!(
   "📊 PAPER · Итог дня {} (Екатеринбург)\nВиртуальных открытий: {}\nВиртуальных закрытий: {}\nСейчас открыто: {}\nМодельный результат за день: {} USDT\nЭто симуляция по публичным стаканам, НЕ реальная прибыль бирж.",
   yekt_date(id_day)?,dn_entries,dn_closes,current.open_positions,
   model_money(modeled)
  )))
}

/// Pure planner; the caller must fsync all notices and cursor in one
/// transaction BEFORE any Telegram network send.
pub fn advance_old_style(
 previous:Option<&OldStyleCursor>,point:&PaperPoint,
 journal:&AuditState,rows:&[ModeledTradeNoticeRow],now_ms:u64,
)->Result<OldStyleOutcome,OldStyleError>{
 advance_old_style_with_clock(previous,None,point,journal,rows,now_ms)
}
/// R44: optional fsynced global heartbeat clock survives PAPER session rotation.
/// The caller stores the returned clock and queue in the SAME durable transaction.
pub fn advance_old_style_with_clock(
 previous:Option<&OldStyleCursor>,global_heartbeat_ms:Option<u64>,
 point:&PaperPoint,journal:&AuditState,rows:&[ModeledTradeNoticeRow],now_ms:u64,
)->Result<OldStyleOutcome,OldStyleError>{
 let today=local_day(now_ms)?;
 if global_heartbeat_ms.is_some_and(|t|t==0||t>now_ms){
  return Err(OldStyleError::Clock)
 }
 let started=session_start_ms(&point.session).ok_or(OldStyleError::Unverified)?;
 if now_ms<started||point.updated_wall_ms<started||
   point.updated_wall_ms>now_ms.saturating_add(2_000)||
   now_ms.saturating_sub(point.updated_wall_ms)>180_000||
   point.session!=journal.session||journal.sequence==0||
   journal.entries<point.entries.saturating_sub(2)||
   journal.entries>point.entries.saturating_add(2)||
   journal.closed<point.closes.saturating_sub(2)||
   journal.closed>point.closes.saturating_add(2){
  return Err(OldStyleError::Unverified)
 }
 // First activation or session rollover: never replay old model trades.
 // Preserve the globally fsynced heartbeat anchor across rotating sessions.
 if previous.map_or(true,|old|old.session!=point.session){
  let global_anchor=global_heartbeat_ms.unwrap_or(now_ms);
  let due=global_heartbeat_ms.is_some()&&now_ms-global_anchor>=HOUR_MS;
  let mut new_notices=Vec::new();
  if due{new_notices.push(heartbeat(point,now_ms));}
  return Ok(OldStyleOutcome{
   next:OldStyleCursor{
    session:point.session.clone(),
    verified_journal_seq:journal.sequence,
    last_heartbeat_ms:if due{now_ms}else{global_anchor},
    last_report_local_day:None,
    baseline_local_day:today,
    baseline_entries:point.entries,
    baseline_closed:point.closes,
    baseline_modeled_milli:point.modeled_milli_usdt,
    last_snapshot_ms:point.updated_wall_ms,
    last_entries:point.entries,
    last_closed:point.closes,
    last_open:point.open_positions,
    last_modeled_milli:point.modeled_milli_usdt,
   },
   notices:new_notices,baseline_created:true,
  })
 }
 let old=previous.unwrap();
 if old.verified_journal_seq>journal.sequence||
   old.last_heartbeat_ms>now_ms||
   today<old.baseline_local_day||
   old.baseline_entries>point.entries||
   old.baseline_closed>point.closes||
   old.last_snapshot_ms>point.updated_wall_ms||
   old.last_entries>point.entries||
   old.last_closed>point.closes{
  return Err(OldStyleError::Rollback)
 }
 let mut next=old.clone();
 let mut notices=Vec::new();
 let mut last_trade_seq=old.verified_journal_seq;
 for r in rows.iter().filter(|row|row.seq>old.verified_journal_seq){
  if r.session!=point.session||r.seq<=last_trade_seq||
    r.seq>journal.sequence||r.event_hash.len()!=64||
    !r.event_hash.bytes().all(|x|x.is_ascii_hexdigit())||
    r.base.is_empty()||r.base.len()>32||
    !r.base.bytes().all(|x|x.is_ascii_uppercase()||x.is_ascii_digit())||
    !r.cumulative_model_pnl.is_finite()||!r.model_pnl_delta.is_finite(){
   return Err(OldStyleError::Unverified)
  }
  last_trade_seq=r.seq;
  let seq=r.seq;
  let event_tag=hash12(&format!("{}:{}:{}",r.session,r.seq,r.event_hash));
  match r.stage.as_str(){
   "FUTURES_SHORT_MODELED"=>{
    notices.push(notice(PaperNoticeKind::VirtualTradeOpened,
     format!("POPEN-{event_tag}-{seq}"),
     format!(
      "🟢 PAPER · Открыта виртуальная сделка\nПара: {} / USDT\nOKX Spot: модель покупки\nBinance Futures: модель шорта\nОценка размера шорта: {}\nОткрытых виртуальных позиций: {}\nИсполнений на биржах НЕ было.",
      r.base,
      r.model_quote.map(|q|format!("{q:.2} USDT"))
       .unwrap_or_else(||"нет оценки".into()),r.open
     )));
   },
   "SPOT_SELL_MODELED"=>{
    notices.push(notice(PaperNoticeKind::VirtualTradeClosed,
     format!("PCLOSE-{event_tag}-{seq}"),
     format!(
      "🔵 PAPER · Закрыта виртуальная сделка\nПара: {} / USDT\nРезультат модели: {} USDT\nИтог модели за сессию: {} USDT\nОткрытых виртуальных позиций: {}\nЭто НЕ реальная прибыль и НЕ исполнения бирж.",
      r.base,model_money4(r.model_pnl_delta),
      model_money4(r.cumulative_model_pnl),r.open
     )));
   },
   _=>return Err(OldStyleError::Unverified)
  }
  if notices.len()>128{return Err(OldStyleError::Capacity)}
 }
 next.verified_journal_seq=journal.sequence;
 // Rolling 3600s global heartbeat, not reset on PAPER session rotation.
 let anchor=global_heartbeat_ms.unwrap_or(old.last_heartbeat_ms);
 if anchor>now_ms{return Err(OldStyleError::Clock)}
 if now_ms-anchor>=HOUR_MS{
  notices.push(heartbeat(point,now_ms));
  next.last_heartbeat_ms=now_ms;
 }
 // Daily report 23:59 local time in Asia/Yekaterinburg (UTC+05).
 // A delayed start after midnight does NOT guess yesterday's totals.
 let minute=minute_in_day(now_ms)?;
 if today==old.baseline_local_day&&minute>=1439&&
   old.last_report_local_day!=Some(today){
  notices.push(daily(today,point,old)?);
  next.last_report_local_day=Some(today);
 }else if today==old.baseline_local_day.saturating_add(1)&&
   minute<=10&&old.last_report_local_day!=Some(old.baseline_local_day)&&
   local_day(old.last_snapshot_ms)?==old.baseline_local_day&&
   now_ms.saturating_sub(old.last_snapshot_ms)<=900_000{
  // Controlled late report based ONLY on the last proven snapshot
  // taken before midnight. Never count today's entries as yesterday.
  let mut prior=point.clone();
  prior.entries=old.last_entries;
  prior.closes=old.last_closed;
  prior.open_positions=old.last_open;
  prior.modeled_milli_usdt=old.last_modeled_milli;
  let mut report=daily(old.baseline_local_day,&prior,old)?;
  report.russian_text.push_str(
    "\nОтчёт после полуночи: по последнему проверенному снимку вчера."
  );
  notices.push(report);
  next.last_report_local_day=Some(old.baseline_local_day);
 }
 if today!=old.baseline_local_day{
  next.baseline_local_day=today;
  next.baseline_entries=point.entries;
  next.baseline_closed=point.closes;
  next.baseline_modeled_milli=point.modeled_milli_usdt;
 }
 next.last_snapshot_ms=point.updated_wall_ms;
 next.last_entries=point.entries;
 next.last_closed=point.closes;
 next.last_open=point.open_positions;
 next.last_modeled_milli=point.modeled_milli_usdt;
 if notices.len()>128{return Err(OldStyleError::Capacity)}
 Ok(OldStyleOutcome{next,notices,baseline_created:false})
}

#[cfg(test)]
mod tests{
 use super::*;
 use std::collections::BTreeMap;
 const START:u64=1_792_080_231_000;
 fn point(now:u64)->PaperPoint{
  PaperPoint{
   session:"session_20741_1792080231".into(),day:20741,
   updated_wall_ms:now,entries:40,closes:40,open_positions:0,
   faults:0,unknown_unhedged:0,modeled_milli_usdt:2221,
  }
 }
 fn audit(p:&PaperPoint,seq:u64)->AuditState{
  AuditState{
   sequence:seq,last_hash:"1".repeat(64),
   entries:p.entries,closed:p.closes,open:p.open_positions as usize,
   sealed:false,model_phase_by_base:BTreeMap::new(),
   session:p.session.clone(),
   modeled_realized_bits:(p.modeled_milli_usdt as f64/1000.0).to_bits(),
   strategy_config_sha256:None,
   strategy_quarantined_bases:std::collections::BTreeSet::new(),
  }
 }
 fn row(p:&PaperPoint,seq:u64,stage:&str)->ModeledTradeNoticeRow{
  ModeledTradeNoticeRow{
   seq,event_hash:"a".repeat(64),session:p.session.clone(),
   base:"ANIME".into(),stage:stage.into(),
   entries:p.entries,closed:p.closes,open:p.open_positions as usize,
   model_pnl_delta:0.0123,cumulative_model_pnl:2.2333,
   model_quote:Some(9.99),
  }
 }
 #[test]
 fn no_historical_replay_and_one_model_open_then_close(){
  let now=START+900_000;let p=point(now);
  let initial=advance_old_style(None,&p,&audit(&p,100),
    &[row(&p,99,"FUTURES_SHORT_MODELED")],now).unwrap();
  assert!(initial.baseline_created&&initial.notices.is_empty());
  let mut next=p.clone();
  next.entries+=1;next.open_positions=1;
  next.updated_wall_ms=now+1_000;
  let opening=advance_old_style(Some(&initial.next),&next,
    &audit(&next,102),&[row(&next,102,"FUTURES_SHORT_MODELED")],
    now+1_000).unwrap();
  assert_eq!(opening.notices.len(),1);
  assert_eq!(opening.notices[0].kind,PaperNoticeKind::VirtualTradeOpened);
  assert!(opening.notices[0].russian_text.contains("НЕ было"));
  let again=advance_old_style(Some(&opening.next),&next,
    &audit(&next,102),&[],now+2_000).unwrap();
  assert!(again.notices.is_empty(),"restart never replays old opening");
  let mut closed=next;
  closed.closes+=1;closed.open_positions=0;
  closed.updated_wall_ms+=3_000;
  let closing=advance_old_style(Some(&again.next),&closed,
   &audit(&closed,105),&[row(&closed,105,"SPOT_SELL_MODELED")],
   now+4_000).unwrap();
  assert_eq!(closing.notices.len(),1);
  assert_eq!(closing.notices[0].kind,PaperNoticeKind::VirtualTradeClosed);
  assert!(closing.notices[0].russian_text.contains("+0.0123"));
 }
 #[test]
 fn hourly_heartbeat_only_after_3600_seconds(){
  let now=START+900_000;let p=point(now);
  let first=advance_old_style(None,&p,&audit(&p,1),&[],now).unwrap();
  let almost=advance_old_style(Some(&first.next),&p,&audit(&p,1),
   &[],now+HOUR_MS-1).unwrap_err();
  assert_eq!(almost,OldStyleError::Unverified,
   "stale source must block even on a heartbeat tick");
  let mut fresh=point(now+HOUR_MS);
  let once=advance_old_style(Some(&first.next),&fresh,
    &audit(&fresh,1),&[],now+HOUR_MS).unwrap();
  assert_eq!(once.notices.len(),1);
  assert_eq!(once.notices[0].kind,PaperNoticeKind::HourlyHeartbeat);
  assert!(once.notices[0].russian_text.contains("Реальная торговля отключена"));
  fresh.updated_wall_ms+=500;
  let repeated=advance_old_style(Some(&once.next),&fresh,
    &audit(&fresh,1),&[],now+HOUR_MS+500).unwrap();
  assert!(repeated.notices.is_empty());
 }
 #[test]
 fn daily_report_only_once_at_2359_yekaterinburg(){
  let local_day_num=local_day(START).unwrap();
  let report_ms=(local_day_num+1)*DAY_MS-YEKT_OFFSET_MS-25_000;
  let prev=report_ms-120_000;
  let p=point(prev);
  let started=advance_old_style(None,&p,&audit(&p,1),&[],prev).unwrap();
  let mut late=point(report_ms);
  late.entries+=2;late.closes+=2;late.modeled_milli_usdt+=75;
  let report=advance_old_style(Some(&started.next),&late,
    &audit(&late,1),&[],report_ms).unwrap();
  let report_notice=report.notices.iter()
   .find(|r|r.kind==PaperNoticeKind::DailyReport).unwrap();
  assert!(report_notice.russian_text.contains("Виртуальных закрытий: 2"));
  assert!(report_notice.russian_text.contains("+0.075 USDT"));
  let again=advance_old_style(Some(&report.next),&late,
    &audit(&late,1),&[],report_ms+500).unwrap();
  assert!(again.notices.is_empty(),"no double daily report");
 }
 #[test]
 fn missed_2359_tick_recovers_yesterday_not_todays_trades(){
  let d=local_day(START).unwrap();
  let midnight=(d+1)*DAY_MS-YEKT_OFFSET_MS;
  let before=midnight-120_000;
  let start=advance_old_style(
   None,&point(before-60_000),&audit(&point(before-60_000),1),
   &[],before-60_000).unwrap();
  let mut pre=point(before);
  pre.entries+=2;pre.closes+=1;pre.modeled_milli_usdt+=101;
  let seen=advance_old_style(Some(&start.next),&pre,
   &audit(&pre,1),&[],before).unwrap();
  let mut after=pre.clone();
  after.updated_wall_ms=midnight+30_000;
  after.entries+=5;after.closes+=5;after.modeled_milli_usdt+=990;
  let recovered=advance_old_style(Some(&seen.next),&after,
   &audit(&after,1),&[],midnight+30_000).unwrap();
  let report=recovered.notices.iter()
   .find(|n|n.kind==PaperNoticeKind::DailyReport).unwrap();
  assert!(report.russian_text.contains("Виртуальных открытий: 2"));
  assert!(report.russian_text.contains("Виртуальных закрытий: 1"));
  assert!(report.russian_text.contains("последнему проверенному снимку"));
  let again=advance_old_style(Some(&recovered.next),&after,
    &audit(&after,1),&[],midnight+31_000).unwrap();
  assert!(!again.notices.iter().any(|n|
    n.kind==PaperNoticeKind::DailyReport));
 }
 #[test]
 fn r44_global_heartbeat_survives_two_session_rotations(){
  let now=START+900_000;
  let a=point(now);
  let first=advance_old_style(None,&a,&audit(&a,1),&[],now).unwrap();
  let mut b=point(now+HOUR_MS/3);
  b.session="session_20741_1792080240".into();
  let midway=advance_old_style_with_clock(None,Some(first.next.last_heartbeat_ms),
   &b,&audit(&b,1),&[],now+HOUR_MS/3).unwrap();
  assert!(midway.notices.is_empty());
  assert_eq!(midway.next.last_heartbeat_ms,first.next.last_heartbeat_ms);
  let mut c=point(now+HOUR_MS+10);
  c.session="session_20741_1792080242".into();
  let due=advance_old_style_with_clock(None,Some(midway.next.last_heartbeat_ms),
   &c,&audit(&c,1),&[],now+HOUR_MS+10).unwrap();
  assert_eq!(due.notices.len(),1);
  assert_eq!(due.notices[0].kind,PaperNoticeKind::HourlyHeartbeat);
  assert!(due.notices[0].event_id.starts_with("PBEAT-"));
  let repeated=advance_old_style_with_clock(Some(&due.next),
   Some(due.next.last_heartbeat_ms),&c,&audit(&c,1),&[],now+HOUR_MS+20).unwrap();
  assert!(repeated.notices.is_empty());
  assert_eq!(advance_old_style_with_clock(Some(&due.next),
   Some(now+2*HOUR_MS),&c,&audit(&c,1),&[],now+HOUR_MS+20),
   Err(OldStyleError::Clock));
 }
 #[test]
 fn reject_journal_rollback_forged_stage_or_mixed_session(){
  let now=START+900_000;let p=point(now);
  let first=advance_old_style(None,&p,&audit(&p,100),&[],now).unwrap();
  let mut later=point(now+1_000);
  assert_eq!(advance_old_style(Some(&first.next),&later,
    &audit(&later,99),&[],now+1_000),Err(OldStyleError::Rollback));
  let bad=row(&later,101,"SPOT_BUY_MODELED");
  assert_eq!(advance_old_style(Some(&first.next),&later,
    &audit(&later,101),&[bad],now+1_000),Err(OldStyleError::Unverified));
  later.session="session_20741_1792080240".into();
  let reset=advance_old_style(Some(&first.next),&later,
    &audit(&later,1),&[],now+1_000).unwrap();
  assert!(reset.baseline_created&&reset.notices.is_empty());
 }
}
