//! R41: deterministic UTC-day Telegram outbox partition routing.
//! Never erases or migrates an existing ClaimUnknown/Confirmed event.
//! An event routes by its *durable PAPER detection timestamp*, not by
//! a dispatcher restart time. No Telegram networking or exchange APIs.
use std::path::{Path,PathBuf};

pub const MS_PER_UTC_DAY:u64=86_400_000;
pub const MAX_REASONABLE_EVENT_DAY:u64=200_000;

#[derive(Clone,Debug,PartialEq,Eq)]
pub enum ShardError{Timestamp,UnsafeRoot}

/// A stable path within a trusted owner-private review directory.
/// The event timestamp comes from the fsynced PAPER queue; same event
/// keeps the same shard even across a midnight dispatcher restart.
pub fn utc_day_shard_path(root:&Path,observed_ms:u64)
 ->Result<PathBuf,ShardError>{
 if observed_ms==0 || !root.is_absolute(){return Err(ShardError::Timestamp)}
 let day=observed_ms/MS_PER_UTC_DAY;
 if day==0 || day>MAX_REASONABLE_EVENT_DAY{
  return Err(ShardError::Timestamp)
 }
 Ok(root.join(format!("telegram_delivery_day_{day}.json")))
}

#[cfg(test)]
mod tests{
 use super::*;
 #[test]
 fn fixed_event_shard_does_not_move_when_midnight_passes(){
  let root=Path::new("/mnt/spotarb_db_test/private_telegram_review");
  let a=utc_day_shard_path(root,20_000*MS_PER_UTC_DAY+MS_PER_UTC_DAY-10)
    .unwrap();
  let after_midnight=utc_day_shard_path(root,20_001*MS_PER_UTC_DAY+10)
    .unwrap();
  assert_ne!(a,after_midnight);
  assert_eq!(a,utc_day_shard_path(
   root,20_000*MS_PER_UTC_DAY+50).unwrap());
  assert!(a.to_str().unwrap().ends_with("telegram_delivery_day_20000.json"));
 }
 #[test]
 fn nonsense_event_timestamp_fails_closed(){
  let root=Path::new("/mnt/spotarb_db_test/private_telegram_review");
  assert_eq!(utc_day_shard_path(root,0),Err(ShardError::Timestamp));
  assert_eq!(utc_day_shard_path(root,MS_PER_UTC_DAY*300_000),
   Err(ShardError::Timestamp));
  assert_eq!(utc_day_shard_path(Path::new("relative"),MS_PER_UTC_DAY),
   Err(ShardError::Timestamp));
 }
 #[test]
 fn multiple_days_have_independent_file_names(){
  let root=Path::new("/mnt/spotarb_db_test/private_telegram_review");
  let mut names=std::collections::BTreeSet::new();
  for day in 20_000..20_100{
   let p=utc_day_shard_path(root,day*MS_PER_UTC_DAY+42).unwrap();
   assert!(p.starts_with(root));
   assert!(names.insert(p));
  }
  assert_eq!(names.len(),100);
 }
}

#[cfg(test)]
mod durable_integration_tests{
 use super::*;
 use crate::{
  native_telegram_durable_outbox::{
   claim_paper_status_notice,inspect_local_claim,confirm_telegram_ack,
   ClaimDecision,DeliveryState,
  },
  native_paper_telegram_observer::{PaperNotice,PaperNoticeKind},
  native_trading_config::NativeTradingConfig,
 };
 use std::time::{SystemTime,UNIX_EPOCH};
 use std::fs;
 fn note(index:u64)->PaperNotice{
  PaperNotice{
   kind:PaperNoticeKind::VirtualTradeOpened,
   event_id:format!("POPEN-TESTSHARD-{index:010}"),
   russian_text:format!(
    "🟢 PAPER · Открыта виртуальная сделка №{index}. Исполнений на биржах НЕ было."
   ),
   external_delivery:false,may_trigger_real_trade:false,real_pnl_attested:false,
  }
 }
 #[test]
 fn utc_day_shards_durable_claims_survive_restart_and_midnight(){
  let stamp=SystemTime::now().duration_since(UNIX_EPOCH).unwrap()
   .as_nanos();
  let dir=std::env::temp_dir().join(format!(
   "spotarb_r41_shards_{}_{}",std::process::id(),stamp));
  fs::create_dir(&dir).unwrap();
  let cfg=NativeTradingConfig::parse(
   include_str!("../config/native_trading.yaml")).unwrap();
  let day=20_000*MS_PER_UTC_DAY;
  let ledger_before=utc_day_shard_path(&dir,day+MS_PER_UTC_DAY-500).unwrap();
  let ledger_after=utc_day_shard_path(&dir,day+MS_PER_UTC_DAY+500).unwrap();
  for n in 1..=20{
   let event=note(n);
   let path=if n<=10{&ledger_before}else{&ledger_after};
   let now=day+MS_PER_UTC_DAY+1_000+n*1000;
   let first=claim_paper_status_notice(path,&cfg.sha256,&event,now).unwrap();
   assert!(matches!(first,ClaimDecision::Claimed{..}));
   assert!(matches!(
    inspect_local_claim(path,&cfg.sha256,&event.event_id).unwrap(),
    Some(DeliveryState::ClaimedUnknown)
   ));
   let retry=claim_paper_status_notice(path,&cfg.sha256,&event,now+1).unwrap();
   assert!(matches!(retry,ClaimDecision::Suppressed{..}));
  }
  assert!(ledger_before.exists()&&ledger_after.exists());
  let first_event=note(1);
  confirm_telegram_ack(&ledger_before,&cfg.sha256,&first_event.event_id,7721)
   .unwrap();
  assert_eq!(
   inspect_local_claim(&ledger_before,&cfg.sha256,&first_event.event_id).unwrap(),
   Some(DeliveryState::Confirmed{telegram_message_id:7721}));
  assert_eq!(inspect_local_claim(&ledger_after,&cfg.sha256,
   &first_event.event_id).unwrap(),None);
  assert!(matches!(claim_paper_status_notice(&ledger_before,&cfg.sha256,
   &first_event,day+MS_PER_UTC_DAY+50_000).unwrap(),
   ClaimDecision::Suppressed{..}));
  fs::remove_dir_all(dir).unwrap();
 }
}
