//! R83: pure PAPER supervisor service lifecycle -> one deduplicated Telegram
//! notice. Only systemd *service* status; not exchange connectivity or trade.
use crate::native_paper_telegram_observer::{PaperNotice,PaperNoticeKind};
use serde::{Serialize,Deserialize};

#[derive(Debug,Clone,Copy,PartialEq,Eq)]
pub struct SupervisorProbe {
 pub active:bool,pub pid:u64,pub started_mono_us:u64,
 pub last_stopped_mono_us:u64,
}
#[derive(Debug,Clone,Serialize,Deserialize,PartialEq,Eq)]
#[serde(deny_unknown_fields)]
pub struct SupervisorCursor {
 pub schema:u32,pub active:bool,pub pid:u64,pub started_mono_us:u64,
}
#[derive(Debug,Clone,Copy,PartialEq,Eq)]
pub enum LifecycleError{UnsafeProbe,InvalidCursor,Clock}
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct LifecycleStep{pub next:SupervisorCursor,pub notice:Option<PaperNotice>}

fn notice(kind:&str,id:u64)->PaperNotice {
 let description=match kind{
  "START"=>"🟢 Бот запущен. Наблюдение PAPER работает.",
  "STOP"=>"🔴 Бот остановлен. PAPER-наблюдение остановлено.",
  "RESTART"=>"🔄 Бот перезапущен. PAPER-наблюдение возобновлено.",
  _=>unreachable!(),
 };
 PaperNotice{
  kind:PaperNoticeKind::Fault,
  event_id:format!("PFAULT-LIFE-{kind}-{id}"),
  russian_text:format!(
   "⚠️ Rust PAPER: {description}\n    Это виртуальное моделирование, не сделки на биржах.\n    Реальная торговля выключена."),
  external_delivery:false,may_trigger_real_trade:false,real_pnl_attested:false,
 }
}
/// Pure transition; no disk, no network and no Telegram credentials.
/// A newly installed observer sees a bootstrapped, recently started
/// supervisor as START; an older one is only baseline, never retro-send.
pub fn advance_supervisor_lifecycle(
 previous:Option<&SupervisorCursor>,current:SupervisorProbe,
 now_mono_us:u64,
)->Result<LifecycleStep,LifecycleError>{
 if now_mono_us==0||
    (current.active&&(current.pid==0||current.started_mono_us==0||
        current.started_mono_us>now_mono_us||
        current.last_stopped_mono_us>current.started_mono_us))||
    (!current.active&&current.pid!=0){
  return Err(LifecycleError::UnsafeProbe)
 }
 if let Some(old)=previous{
  if old.schema!=1||(old.active&&(old.pid==0||old.started_mono_us==0))||
     (!old.active&&old.pid!=0){
   return Err(LifecycleError::InvalidCursor)
  }
 }
 let new=SupervisorCursor{
  schema:1,active:current.active,pid:current.pid,
  started_mono_us:if current.active{current.started_mono_us}
      else{previous.map(|p|p.started_mono_us).unwrap_or(0)},
 };
 let alert=match previous{
  None if current.active&&now_mono_us-current.started_mono_us<=1_800_000_000=>
     Some(notice(if current.last_stopped_mono_us>0&&
          current.started_mono_us-current.last_stopped_mono_us<=600_000_000
          {"RESTART"}else{"START"},current.started_mono_us)),
  None=>None,
  Some(old) if old.active&&!current.active=>
     Some(notice("STOP",old.started_mono_us)),
  Some(old) if !old.active&&current.active=>
     Some(notice("START",current.started_mono_us)),
  Some(old) if old.active&&current.active&&
     (old.pid!=current.pid||old.started_mono_us!=current.started_mono_us)=>
     Some(notice("RESTART",current.started_mono_us)),
  _=>None,
 };
 Ok(LifecycleStep{next:new,notice:alert})
}
#[cfg(test)]
mod tests{
 use super::*;
 fn up(pid:u64,when:u64)->SupervisorProbe{
  SupervisorProbe{active:true,pid,started_mono_us:when,
     last_stopped_mono_us:0}
 }
 #[test]fn restart_notice_once_then_no_repeated_send(){
  let old=advance_supervisor_lifecycle(None,up(11,1_000_000),2_000_000).unwrap();
  assert!(old.notice.is_some());
  let new=advance_supervisor_lifecycle(Some(&old.next),up(22,3_000_000),4_000_000).unwrap();
  assert!(new.notice.as_ref().unwrap().russian_text.contains("перезапущен"));
  assert!(new.notice.as_ref().unwrap().event_id.starts_with("PFAULT-LIFE-RESTART-"));
  let repeat=advance_supervisor_lifecycle(Some(&new.next),up(22,3_000_000),6_000_000).unwrap();
  assert!(repeat.notice.is_none());
 }
 #[test]fn separate_stop_and_start_are_distinct(){
  let start=advance_supervisor_lifecycle(None,up(11,1_000_000),2_000_000).unwrap();
  let stop=advance_supervisor_lifecycle(Some(&start.next),
      SupervisorProbe{active:false,pid:0,started_mono_us:0,
        last_stopped_mono_us:0},3_000_000).unwrap();
  assert!(stop.notice.as_ref().unwrap().russian_text.contains("остановлен"));
  let boot=advance_supervisor_lifecycle(Some(&stop.next),up(15,4_000_000),5_000_000).unwrap();
  assert!(boot.notice.as_ref().unwrap().russian_text.contains("запущен"));
  assert_ne!(stop.notice.unwrap().event_id,boot.notice.unwrap().event_id);
 }
 #[test]fn recently_restarted_service_bootstrap_identifies_restart(){
  let mut s=up(21,4_000_000);
  s.last_stopped_mono_us=3_900_000;
  let first=advance_supervisor_lifecycle(None,s,5_000_000).unwrap();
  assert!(first.notice.unwrap().russian_text.contains("перезапущен"));
 }
 #[test]fn stale_first_attach_has_no_retrospective_alert(){
  let a=advance_supervisor_lifecycle(None,up(5,1_000_000),2_000_000_000).unwrap();
  assert!(a.notice.is_none());
  assert!(advance_supervisor_lifecycle(Some(&a.next),up(5,1_000_000),2_000_001_000)
      .unwrap().notice.is_none());
 }
 #[test]fn unsafe_observation_never_generates_alert(){
  assert_eq!(advance_supervisor_lifecycle(None,up(0,1),10),Err(LifecycleError::UnsafeProbe));
  assert_eq!(advance_supervisor_lifecycle(None,up(4,11),10),Err(LifecycleError::UnsafeProbe));
 }
}
