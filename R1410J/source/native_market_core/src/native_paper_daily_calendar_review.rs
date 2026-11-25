//! R44: evidence-limited, session-independent PAPER daily accounting.
//! Only verified new model stage rows count. Quota markers NEVER count.
//! No exchange API, Telegram sender, or real money attestations.
use crate::{
 native_paper_telegram_observer::{PaperNotice,PaperNoticeKind,PaperPoint},
 native_paper_oldstyle_notice_review::yekt_date,
 native_virtual_four_leg_provenance_review::ModeledTradeNoticeRow,
};
use serde::{Serialize,Deserialize};
use openssl::sha::sha256;

const DAY:u64=86_400_000;
const OFFSET:u64=18_000_000;
#[derive(Clone,Debug,Serialize,Deserialize,PartialEq,Eq)]
pub struct PaperDailyCalendar{
 pub day:u64,
 pub opened:u64,
 pub closed:u64,
 pub model_micro_usdt:i64,
 pub session_count:u64,
 pub last_session:String,
 pub first_seen_ms:u64,
 pub last_seen_ms:u64,
 pub last_reported_day:Option<u64>,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum CalendarError{Clock,Rollback,Unverified,Overflow}
fn day(ms:u64)->Result<u64,CalendarError>{
 Ok(ms.checked_add(OFFSET).ok_or(CalendarError::Clock)?/DAY)
}
fn minute(ms:u64)->Result<u64,CalendarError>{
 Ok(ms.checked_add(OFFSET).ok_or(CalendarError::Clock)?%DAY/60_000)
}
fn report(c:&PaperDailyCalendar,late:bool)->Result<PaperNotice,CalendarError>{
 let date=yekt_date(c.day).map_err(|_|CalendarError::Clock)?;
 let idhash=sha256(b"R44_PAPER_GLOBAL_DAILY");
 let idhash=idhash[..8].iter().map(|x|format!("{x:02X}")).collect::<String>();
 let mut body=format!(
  "📊 PAPER · Сводка дня {} (Екатеринбург)\nПроверенных модельных открытий: {}\nПроверенных модельных закрытий: {}\nМодельный результат подтверждённых закрытий: {:+.6} USDT\nНаблюдалось PAPER-сессий: {}\nДанные НЕПОЛНЫЕ: учитываются только новые проверенные события журнала; периоды между сессиями не доказаны. Лимит 500/500 — НЕ сделки. Это НЕ реальная прибыль.",
  date,c.opened,c.closed,c.model_micro_usdt as f64/1_000_000.0,c.session_count
 );
 if late{body.push_str("\nВосстановлено после полуночи по вчерашнему проверенному состоянию.");}
 Ok(PaperNotice{
  kind:PaperNoticeKind::DailyReport,
  event_id:format!("PDAY-{idhash}-{}",c.day),
  russian_text:body,
  external_delivery:false,may_trigger_real_trade:false,real_pnl_attested:false,
 })
}
fn blank(point:&PaperPoint,now:u64,prior_report:Option<u64>)->Result<PaperDailyCalendar,CalendarError>{
 let d=day(now)?;
 if d!=point.day{return Err(CalendarError::Clock)}
 Ok(PaperDailyCalendar{
  day:d,opened:0,closed:0,model_micro_usdt:0,session_count:1,
  last_session:point.session.clone(),first_seen_ms:now,last_seen_ms:now,
  last_reported_day:prior_report,
 })
}
/// Accepted rows MUST already be validated by the R40 hash-chain planner.
/// A new session is baselined without replay; coverage remains explicitly incomplete.
pub fn advance_calendar(
 previous:Option<&PaperDailyCalendar>,point:&PaperPoint,
 accepted_rows:&[&ModeledTradeNoticeRow],now:u64,seed_report:Option<u64>,
)->Result<(PaperDailyCalendar,Vec<PaperNotice>),CalendarError>{
 let current_day=day(now)?;
 if current_day!=point.day||now<point.updated_wall_ms.saturating_sub(2_000){
  return Err(CalendarError::Clock)
 }
 let mut notices=Vec::new();
 let mut c=match previous{
  None=>{
   if !accepted_rows.is_empty(){return Err(CalendarError::Unverified)}
   blank(point,now,seed_report)?
  },
  Some(old)=>{
   if old.day>current_day||old.last_seen_ms>now||
    old.first_seen_ms>old.last_seen_ms||old.session_count==0||
    old.last_session.is_empty(){return Err(CalendarError::Rollback)}
   if old.day==current_day{
    let mut updated=old.clone();
    if updated.last_session!=point.session{
     updated.session_count=updated.session_count.checked_add(1).ok_or(CalendarError::Overflow)?;
     updated.last_session=point.session.clone();
    }
    updated.last_seen_ms=now;
    updated
   }else{
    if !accepted_rows.is_empty(){return Err(CalendarError::Unverified)}
    if current_day==old.day+1&&minute(now)?<=10&&
     old.last_reported_day!=Some(old.day)&&
     now.saturating_sub(old.last_seen_ms)<=900_000{
     notices.push(report(old,true)?);
    }
    blank(point,now,if notices.is_empty(){old.last_reported_day}else{Some(old.day)})?
   }
  }
 };
 for r in accepted_rows{
  if r.session!=point.session||!r.model_pnl_delta.is_finite(){
   return Err(CalendarError::Unverified)
  }
  match r.stage.as_str(){
   "FUTURES_SHORT_MODELED"=>{
    c.opened=c.opened.checked_add(1).ok_or(CalendarError::Overflow)?;
   },
   "SPOT_SELL_MODELED"=>{
    c.closed=c.closed.checked_add(1).ok_or(CalendarError::Overflow)?;
    let delta=(r.model_pnl_delta*1_000_000.0).round();
    if !delta.is_finite()||delta.abs()>100_000_000_000.0{
     return Err(CalendarError::Overflow)
    }
    c.model_micro_usdt=c.model_micro_usdt.checked_add(delta as i64)
     .ok_or(CalendarError::Overflow)?;
   },
   _=>return Err(CalendarError::Unverified)
  }
 }
 if minute(now)?>=1439&&c.last_reported_day!=Some(c.day){
  notices.push(report(&c,false)?);
  c.last_reported_day=Some(c.day);
 }
 Ok((c,notices))
}

#[cfg(test)]
mod tests{
 use super::*;
 fn p(day_num:u64,at:u64,session:&str)->PaperPoint{
  PaperPoint{day:day_num,session:session.into(),updated_wall_ms:at,
   entries:500,closes:0,open_positions:0,faults:0,unknown_unhedged:0,
   modeled_milli_usdt:0}
 }
 fn r(session:&str,stage:&str,pnl:f64)->ModeledTradeNoticeRow{
  ModeledTradeNoticeRow{seq:2,event_hash:"a".repeat(64),
   session:session.into(),base:"BTC".into(),stage:stage.into(),entries:500,
   closed:0,open:0,model_pnl_delta:pnl,cumulative_model_pnl:pnl,model_quote:Some(10.0)}
 }
 #[test]
 fn rotation_counts_only_verified_new_events_not_quota(){
  let d=20742;
  let start=d*DAY-OFFSET+10_000;
  let a=p(d,start,"session_a");
  let (initial,_)=advance_calendar(None,&a,&[],start,None).unwrap();
  assert_eq!(initial.opened,0);
  let b=p(d,start+3_600_000,"session_b");
  let (second,_)=advance_calendar(Some(&initial),&b,&[],start+3_600_000,None).unwrap();
  assert_eq!((second.opened,second.session_count),(0,2));
  let open=r("session_b","FUTURES_SHORT_MODELED",0.0);
  let (third,_)=advance_calendar(Some(&second),&b,&[&open],start+3_600_010,None).unwrap();
  let close=r("session_b","SPOT_SELL_MODELED",-0.1357);
  let (fourth,_)=advance_calendar(Some(&third),&b,&[&close],start+3_600_020,None).unwrap();
  assert_eq!((fourth.opened,fourth.closed,fourth.model_micro_usdt),(1,1,-135_700));
 }
 #[test]
 fn daily_report_is_partial_and_deduped_at_2359(){
  let d=20742;let last=(d+1)*DAY-OFFSET-30_000;
  let a=p(d,last-120_000,"session_a");
  let (initial,_)=advance_calendar(None,&a,&[],last-120_000,None).unwrap();
  let (reported,notices)=advance_calendar(Some(&initial),&a,&[],last,None).unwrap();
  assert_eq!(notices.len(),1);
  assert!(notices[0].russian_text.contains("НЕПОЛНЫЕ"));
  assert!(notices[0].russian_text.contains("500/500"));
  let (_,twice)=advance_calendar(Some(&reported),&a,&[],last+1,None).unwrap();
  assert!(twice.is_empty());
 }
 #[test]
 fn midnight_late_report_uses_previous_day_only(){
  let d=20742;let boundary=(d+1)*DAY-OFFSET;
  let before=p(d,boundary-120_000,"session_a");
  let (old,_)=advance_calendar(None,&before,&[],boundary-120_000,None).unwrap();
  let after=p(d+1,boundary+60_000,"session_b");
  let (new,notices)=advance_calendar(Some(&old),&after,&[],boundary+60_000,None).unwrap();
  assert_eq!(notices.len(),1);
  assert!(notices[0].russian_text.contains("после полуночи"));
  assert_eq!((new.day,new.opened,new.last_reported_day),(d+1,0,Some(d)));
  assert!(advance_calendar(Some(&new),&after,&[],boundary+61_000,None).unwrap().1.is_empty());
 }
}
