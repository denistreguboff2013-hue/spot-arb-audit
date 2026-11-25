//! Astra R38 — separate opt-in PAPER-only one-shot Telegram sender.
//! PREVIEW mode is ZERO NETWORK, ZERO credentials. SEND requires explicit
//! operator deployment/permission, no trade orders or private exchange API.
use spotarb_native_market_core::{
 native_paper_notice_queue_review::PaperWatchState,
 native_paper_telegram_dispatch_review::{
  select_next_native_paper_telegram,
  select_next_native_paper_telegram_with_known,DispatchGateError,
 },
 native_paper_pause_review::verified_paper_day_cap_pause,
 native_paper_telegram_delivery_review::deliver_paper_status_once,
 native_paper_service_lifecycle_review::{
  SupervisorCursor,SupervisorProbe,advance_supervisor_lifecycle,
 },
 native_telegram_delivery_review::{PinnedTelegramTls,DeliveryResult},
 native_telegram_durable_outbox::inspect_local_claim,
 native_paper_telegram_shards_review::utc_day_shard_path,
 native_trading_config::NativeTradingConfig,
};
use serde_json::json;
use std::{
 collections::BTreeSet,env,fs::{self,File,OpenOptions},
 io::{Read,Write},
 os::{fd::AsRawFd,unix::fs::{OpenOptionsExt,MetadataExt,PermissionsExt}},
 path::{Path,PathBuf},
 time::{SystemTime,UNIX_EPOCH},
};
const REVIEW:&str="/mnt/spotarb_db_test/astra_p0_02_execution_gate_20261011";
const QUEUE:&str="/mnt/spotarb_db_test/astra_p0_02_execution_gate_20261011/r37_paper_telegram_preview_watch_20261015/durable_watch_queue.json";
// R38 legacy single-file ledger is always consulted for migration safety.
const LEDGER:&str="/mnt/spotarb_db_test/astra_p0_02_execution_gate_20261011/r38_paper_telegram_dispatch_20261015/telegram_delivery_ledger.json";
const OUTBOX_ROOT:&str="/mnt/spotarb_db_test/astra_p0_02_execution_gate_20261011/r38_paper_telegram_dispatch_20261015";
const PAPER_BUDGET:&str="/mnt/spotarb_db_test/astra_paper_continuous_v1/daily_budget.json";
const MAX_QUEUE_BYTES:usize=128*1024;
const MODE_PREVIEW:&str="PREVIEW_NO_TELEGRAM";
const MODE_SEND:&str="ONE_FRESH_PAPER_NOTICE_VIA_TELEGRAM";
fn fail(message:&str)->!{
 eprintln!("R38_FAIL_CLOSED:{message}");
 std::process::exit(3)
}
fn now_ms()->u64{
 SystemTime::now().duration_since(UNIX_EPOCH)
  .unwrap_or_default().as_millis() as u64
}
fn owner_private_directory(path:&Path){
 let m=fs::symlink_metadata(path).unwrap_or_else(|_|fail("review_directory_missing"));
 if !m.is_dir()||m.permissions().mode()&0o077!=0{
  fail("directory_not_owner_private")
 }
}
fn secure_read(path:&Path,max:usize,systemd_credential:bool)->Vec<u8>{
 let mut file=OpenOptions::new().read(true)
  .custom_flags(libc::O_NOFOLLOW|libc::O_CLOEXEC)
  .open(path).unwrap_or_else(|_|fail("secure_read_denied"));
 let m=file.metadata().unwrap_or_else(|_|fail("secure_file_metadata_denied"));
 if !m.is_file()||m.len()>max as u64||m.len()==0{
  fail(if systemd_credential{"systemd_credential_type_or_size_denied"}
    else{"unsafe_read_source"})
 }
 if systemd_credential{
  // A systemd LoadCredential mount may expose a read-only regular
  // file with link count !=1 or group/other READ mode. Its private
  // /run/credentials/<unit> mount is protected by systemd, and the
  // path is separately pinned below. Do not apply the journal's
  // owner-0600 inode restrictions to this runtime credential mount.
  let euid=unsafe{libc::geteuid()};
  if (m.uid()!=euid&&m.uid()!=0)||m.mode()&0o111!=0{
   fail("systemd_credential_owner_or_executable_denied")
  }
 }else if m.nlink()!=1||m.mode()&0o077!=0{
  fail("private_paper_queue_mode_or_links_denied")
 }
 let mut bytes=Vec::new();
 Read::by_ref(&mut file).take((max+1) as u64)
  .read_to_end(&mut bytes).unwrap_or_else(|_|fail("secure_read_failed"));
 if bytes.len()>max{fail("source_too_large")}
 bytes
}
struct SharedLock(File);
impl SharedLock{
 fn acquire(path:&Path)->Self{
  let f=OpenOptions::new().read(true)
   .custom_flags(libc::O_NOFOLLOW|libc::O_CLOEXEC)
   .open(path).unwrap_or_else(|_|fail("observer_shared_lock_unavailable"));
  let m=f.metadata().unwrap_or_else(|_|fail("observer_lock_metadata"));
  if !m.is_file()||m.mode()&0o077!=0||m.nlink()!=1
    ||unsafe{libc::flock(f.as_raw_fd(),libc::LOCK_SH)}!=0{
   fail("observer_lock_not_private")
  }
  Self(f)
 }
}
impl Drop for SharedLock{
 fn drop(&mut self){unsafe{libc::flock(self.0.as_raw_fd(),libc::LOCK_UN);}}
}
fn load_local_proxy()->String{
 // Existing Python local loopback proxy source is read only.
 // No exchange keys or .env access.
 let data=fs::read_to_string("/home/tmpadmin/spotbot/config.yaml")
  .unwrap_or_else(|_|fail("legacy_local_proxy_config_unavailable"));
 if data.len()>256*1024{fail("proxy_source_oversized")}
 let root:serde_yaml::Value=serde_yaml::from_str(&data)
  .unwrap_or_else(|_|fail("legacy_local_proxy_invalid_yaml"));
 root.get("telegram").and_then(|v|v.get("proxy_url"))
  .and_then(|v|v.as_str()).filter(|x|!x.is_empty())
  .unwrap_or_else(||fail("legacy_local_proxy_not_configured")).to_string()
}
fn outbox_for_id(watch:&PaperWatchState,id:&str)->PathBuf{
 let event=watch.pending.get(id)
  .unwrap_or_else(||fail("notice_not_in_durable_paper_queue"));
 utc_day_shard_path(Path::new(OUTBOX_ROOT),event.detected_at_wall_ms)
  .unwrap_or_else(|_|fail("invalid_durable_event_utc_day"))
}
fn already_claimed_in_legacy_or_daily(
 watch:&PaperWatchState,config:&NativeTradingConfig,id:&str
)->bool{
 // Original R38 unsharded ledger ALWAYS takes precedence. A previously
 // ClaimedUnknown event may have been delivered before a crash.
 let legacy=inspect_local_claim(Path::new(LEDGER),&config.sha256,id)
  .unwrap_or_else(|_|fail("legacy_telegram_ledger_invalid"));
 let daily=inspect_local_claim(&outbox_for_id(watch,id),&config.sha256,id)
  .unwrap_or_else(|_|fail("daily_telegram_ledger_invalid"));
 legacy.is_some()||daily.is_some()
}
// Lifecycle notices are independent of R37's market-model cursor. This
// allows STOP messages when the PAPER status naturally becomes stale.
const SERVICE:&str="spotarb-native-paper-continuous.service";
const LIFE_CURSOR:&str="supervisor_lifecycle_observed.json";
const LIFE_DELIVERY:&str="supervisor_lifecycle_telegram_claims.json";
fn systemd_service_probe()->Option<(SupervisorProbe,u64)>{
 let result=std::process::Command::new("/usr/bin/systemctl")
  .args(["show",SERVICE,"-p","ActiveState","-p","MainPID",
         "-p","ExecMainStartTimestampMonotonic",
         "-p","InactiveEnterTimestampMonotonic"])
  .output().ok()?;
 if !result.status.success()||result.stdout.len()>4_096{return None}
 let raw=std::str::from_utf8(&result.stdout).ok()?;
 let value=|key:&str|raw.lines().find_map(|line|
    line.strip_prefix(key).and_then(|v|v.strip_prefix('=')));
 let state=value("ActiveState")?;
 let active=match state{"active"=>true,"inactive"|"failed"=>false,_=>return None};
 let pid=value("MainPID")?.parse::<u64>().ok()?;
 let started=value("ExecMainStartTimestampMonotonic")?
   .parse::<u64>().ok()?;
 let previous_stop=value("InactiveEnterTimestampMonotonic")?
   .parse::<u64>().ok()?;
 let mut ts:libc::timespec=unsafe{std::mem::zeroed()};
 if unsafe{libc::clock_gettime(libc::CLOCK_MONOTONIC,&mut ts)}!=0{return None}
 let current=(ts.tv_sec as u64).checked_mul(1_000_000)?
   .checked_add((ts.tv_nsec as u64)/1_000)?;
 Some((SupervisorProbe{active,pid,started_mono_us:started,
    last_stopped_mono_us:previous_stop},current))
}
fn life_cursor_read(path:&Path)->Option<SupervisorCursor>{
 if !path.exists(){return None}
 let parsed:SupervisorCursor=serde_json::from_slice(&secure_read(path,4096,false))
    .unwrap_or_else(|_|fail("R83_lifecycle_cursor_invalid"));
 if parsed.schema!=1{fail("R83_lifecycle_schema_invalid")}
 Some(parsed)
}
fn life_cursor_write(path:&Path,value:&SupervisorCursor){
 let folder=path.parent().unwrap_or_else(||fail("R83_no_cursor_parent"));
 let temp=folder.join(format!(".r83_cursor_{}.tmp",std::process::id()));
 let mut file=OpenOptions::new().write(true).create_new(true)
    .mode(0o600).custom_flags(libc::O_NOFOLLOW|libc::O_CLOEXEC)
    .open(&temp).unwrap_or_else(|_|fail("R83_cursor_temp_write_denied"));
 let raw=serde_json::to_vec(value).unwrap_or_else(|_|fail("R83_cursor_json_denied"));
 file.write_all(&raw).unwrap_or_else(|_|fail("R83_cursor_write_denied"));
 file.sync_all().unwrap_or_else(|_|fail("R83_cursor_fsync_denied"));
 drop(file);
 fs::rename(&temp,path).unwrap_or_else(|_|fail("R83_cursor_rename_denied"));
 File::open(folder).and_then(|f|f.sync_all())
     .unwrap_or_else(|_|fail("R83_cursor_directory_fsync_denied"));
}
fn maybe_send_lifecycle(config:&NativeTradingConfig,now:u64)->bool{
 if env::var("R38_SEND_OPERATOR_AUTHORIZED").ok().as_deref()
     !=Some("ALLOW_ONE_NEW_PAPER_EVENT_AT_A_TIME"){return false}
 if config.policy.real_execution_authorized||config.policy.real_daily_trades!=0{
    return false
 }
 let (view,monotonic)=match systemd_service_probe(){
   Some(x)=>x,
   None=>{eprintln!("R83_SUPERVISOR_PROBE_UNAVAILABLE");return false}
 };
 owner_private_directory(Path::new(OUTBOX_ROOT));
 let path=Path::new(OUTBOX_ROOT).join(LIFE_CURSOR);
 let prior=life_cursor_read(&path);
 let step=advance_supervisor_lifecycle(prior.as_ref(),view,monotonic)
    .unwrap_or_else(|_|fail("R83_service_lifecycle_rollback_or_clock_denied"));
 let Some(note)=step.notice else{
    if prior.as_ref()!=Some(&step.next){life_cursor_write(&path,&step.next)}
    return false
 };
 // Only Telegram credentials supplied by this dedicated systemd unit.
 let creds=env::var("CREDENTIALS_DIRECTORY")
    .unwrap_or_else(|_|fail("R83_systemd_credentials_missing"));
 let folder=PathBuf::from(creds);
 if !folder.is_absolute()||!folder.starts_with("/run/credentials/")||
    folder.components().any(|c|matches!(c,std::path::Component::ParentDir)){
    fail("R83_credential_directory_denied")
 }
 let token=String::from_utf8(secure_read(&folder.join("tg_bot_token"),256,true))
    .unwrap_or_else(|_|fail("R83_token_encoding_denied"));
 let chat=String::from_utf8(secure_read(&folder.join("tg_chat_id"),64,true))
    .unwrap_or_else(|_|fail("R83_chat_encoding_denied"));
 let token=token.trim_end_matches(['\r','\n']).to_owned();
 let chat=chat.trim_end_matches(['\r','\n']).to_owned();
 if token.contains(['\r','\n'])||chat.contains(['\r','\n']){
    fail("R83_credential_multiline_denied")
 }
 let proxy=load_local_proxy();
 let sender=PinnedTelegramTls::new_with_local_http_proxy(token,&chat,&proxy)
    .unwrap_or_else(|_|fail("R83_telegram_proxy_denied"));
 // R84: no CLAIM until a token-free Telegram TLS handshake succeeds.
 // If Telegram is unreachable, keep event unconsumed for next timer tick.
 if sender.preflight_local_telegram_tls().is_err(){
    eprintln!("R84_LIFECYCLE_TOKEN_FREE_TLS_NOT_READY_NO_CLAIM");
    return false
 }
 let result=deliver_paper_status_once(
    &Path::new(OUTBOX_ROOT).join(LIFE_DELIVERY),
    config,&note,now,true,&sender)
    .unwrap_or_else(|_|fail("R83_lifecycle_claim_or_ack_denied"));
 // Claim happens on durable disk BEFORE possible network call.
 // Persist observed PID only after claim/ACK, never blindly retry unknown.
 life_cursor_write(&path,&step.next);
 match result{
  DeliveryResult::Confirmed{event_id,message_id}=>{
    println!("{}",json!({"schema":"spotarb_r83_lifecycle",
        "result":"TELEGRAM_API_ACK_CONFIRMED","event_id":event_id,
        "message_id":message_id,"real_trading":false}))
  },
  DeliveryResult::Suppressed{reason}=>{
    println!("{}",json!({"schema":"spotarb_r83_lifecycle",
        "result":"DURABLE_DUPLICATE_SUPPRESSED","reason":reason,
        "real_trading":false}))
  },
  DeliveryResult::Uncertain{event_id}=>{
    eprintln!("R83_LIFECYCLE_CLAIMED_UNKNOWN_NO_RETRY:{event_id}");
    std::process::exit(4)
  },
 }
 true
}
fn main(){
 let mode=env::var("R38_TELEGRAM_MODE").unwrap_or_default();
 if mode!=MODE_PREVIEW&&mode!=MODE_SEND{
  fail("must_explicitly_select_preview_or_send_mode")
 }
 let will_send=mode==MODE_SEND;
 let now=now_ms();
 let queue=Path::new(QUEUE);
 let parent=queue.parent().unwrap_or_else(||fail("queue_parent_missing"));
 owner_private_directory(parent);
 owner_private_directory(Path::new(REVIEW));
 let _shared=SharedLock::acquire(&parent.join(".read_only_watch_queue.flock"));
 let input=secure_read(queue,MAX_QUEUE_BYTES,false);
 let watch:PaperWatchState=serde_json::from_slice(&input)
  .unwrap_or_else(|_|fail("watch_queue_corrupt"));
 // Drop flock before any potential network I/O; the observation file
 // is atomically replaced and this snapshot remains immutable in RAM.
 drop(_shared);
 let config=NativeTradingConfig::load(
  &PathBuf::from(env!("CARGO_MANIFEST_DIR"))
   .join("config/native_trading.yaml"))
  .unwrap_or_else(|_|fail("native_rust_policy_denied"));
 // First handle source-independent systemd START/STOP/RESTART. This
 // runs before R37 stale-status rejection and never replays model trades.
 if will_send&&maybe_send_lifecycle(&config,now){return}
 let summary_allowed=env::var("R38_ALLOW_VIRTUAL_SUMMARY")
  .ok().as_deref()==Some("YES_ONLY_VIRTUAL_MODEL");
 let initial=match select_next_native_paper_telegram(
  &watch,&config,now,true,summary_allowed){
   Ok(p)=>p,
   Err(DispatchGateError::StaleSource) if watch.pending.is_empty()=>{
    // Only the Rust PAPER supervisor's validated FULL daily budget
    // allows an idle/no-network tick with stale modeled books.
    // Any other stale source or queued notice is a hard stop.
    let raw=secure_read(Path::new(PAPER_BUDGET),4096,false);
    let budget:serde_json::Value=serde_json::from_slice(&raw)
      .unwrap_or_else(|_|fail("invalid_paper_daily_budget"));
    if verified_paper_day_cap_pause(&budget,now,watch.pending.len()){
     println!("{}",json!({
      "schema":"spotarb_native_r42_daily_budget_pause",
      "result":"PAPER_QUARANTINED_BY_VERIFIED_DAILY_CAP",
      "telegram_bot_api_called":false,
      "credentials_read":false,
      "real_trading":false,
      "source_stale":true
     }));
     return
    }
    fail("stale_paper_source_without_verified_daily_cap")
   },
   Err(_)=>fail("stale_unverified_or_unauthorized_paper_queue"),
 };
 if !will_send{
  println!("{}",json!({
   "schema":"spotarb_native_r38_telegram_dispatch_preview",
   "mode":"NO_NETWORK",
   "paper_only":true,
   "bot_api_called":false,
   "exchange_order_access":false,
   "queue_revision":initial.queue_revision,
   "pending":initial.total_pending,
   "fresh_permitted":initial.permitted_new_events,
   "stale_skipped":initial.stale_notices_skipped,
   "historical_skipped":initial.historical_session_skipped,
   "next_notice_id":initial.candidate.map(|n|n.event_id),
   "credentials_read":false
  }));
  return
 }
 // This separate SEND permission is required even if the timer is
 // installed or a spurious R38_TELEGRAM_MODE is set.
 if env::var("R38_SEND_OPERATOR_AUTHORIZED").ok().as_deref()
  !=Some("ALLOW_ONE_NEW_PAPER_EVENT_AT_A_TIME"){
  fail("explicit_telegram_dispatch_permission_missing")
 }
 owner_private_directory(Path::new(OUTBOX_ROOT));
 let mut known=BTreeSet::new();
 for id in watch.pending.keys(){
  if already_claimed_in_legacy_or_daily(&watch,&config,id){
   known.insert(id.clone());
  }
 }
 let selection=select_next_native_paper_telegram_with_known(
  &watch,&config,now,true,summary_allowed,&known
 ).unwrap_or_else(|_|fail("telegram_dispatch_gate_failed"));
 let mut note=match selection.candidate{
  None=>{
   println!("{}",json!({
    "schema":"spotarb_native_r38_telegram_dispatch",
    "result":"NO_ELIGIBLE_NEW_PAPER_NOTICE",
    "pending":selection.total_pending,
    "already_claimed":known.len(),
    "telegram_bot_api_called":false,
    "real_trading":false
   }));
   return
  },
  Some(n)=>n,
 };
 // ONLY now, after a fresh candidate and prior-claim inspection,
 // load Telegram-only systemd credentials. Never parse the combined
 // Python .env or export bot secrets in the process environment.
 let creds=env::var("CREDENTIALS_DIRECTORY")
  .unwrap_or_else(|_|fail("systemd_credential_directory_not_supplied"));
 let credentials_dir=PathBuf::from(creds);
 if !credentials_dir.is_absolute()||
   !credentials_dir.starts_with("/run/credentials/")||
   credentials_dir.components().any(|c|
    matches!(c,std::path::Component::ParentDir)){
  fail("untrusted_systemd_credential_directory")
 }
 let token=String::from_utf8(secure_read(
    &credentials_dir.join("tg_bot_token"),256,true))
  .unwrap_or_else(|_|fail("telegram_only_token_encoding_invalid"));
 let chat=String::from_utf8(secure_read(
    &credentials_dir.join("tg_chat_id"),64,true))
  .unwrap_or_else(|_|fail("telegram_only_chat_encoding_invalid"));
 let token=token.trim_end_matches(['\r','\n']).to_owned();
 let chat=chat.trim_end_matches(['\r','\n']).to_owned();
 if token.contains(['\r','\n'])||chat.contains(['\r','\n']){
  fail("telegram_credential_multiline_denied")
 }
 let proxy=load_local_proxy();
 let sender=PinnedTelegramTls::new_with_local_http_proxy(token,&chat,&proxy)
  .unwrap_or_else(|_|fail("telegram_auth_or_loopback_proxy_denied"));
 // Strictly bounded burst draining: at most THREE distinct events per
 // minute tick; each is claimed and fsynced BEFORE a separate POST.
 // A failed/UNKNOWN send stops the loop with no network retry.
 for _ in 0..3{
 // Every event uses its original durable detection day, not today's
 // clock. The old R38 global ledger is checked above for migration.
 let selected_shard=outbox_for_id(&watch,&note.event_id);
 let result=deliver_paper_status_once(
  &selected_shard,&config,&note,now,true,&sender)
  .unwrap_or_else(|_|fail("telegram_claim_or_confirm_failed_NO_AUTO_RETRY"));
 match result{
  DeliveryResult::Confirmed{event_id,message_id}=>{
   println!("{}",json!({
    "schema":"spotarb_native_r38_telegram_dispatch",
    "result":"TELEGRAM_API_ACK_CONFIRMED",
    "event_id":event_id,"message_id":message_id,
    "real_trading":false
   }));
  },
  DeliveryResult::Suppressed{reason}=>{
   println!("{}",json!({
    "schema":"spotarb_native_r38_telegram_dispatch",
    "result":"DURABLE_DUPLICATE_OR_COOLDOWN_SUPPRESSED",
    "reason":reason,"real_trading":false
   }));
  },
  DeliveryResult::Uncertain{event_id}=>{
   eprintln!("R38_RESULT_UNKNOWN_NO_AUTO_RETRY_EVENT:{event_id}");
   std::process::exit(4);
  },
 }
 known.insert(note.event_id.clone());
 let next=select_next_native_paper_telegram_with_known(
  &watch,&config,now,true,summary_allowed,&known)
  .unwrap_or_else(|_|fail("next_paper_send_gate_failed"));
 match next.candidate{
  Some(n)=>note=n,
  None=>break,
 }
 }
}

#[cfg(test)]
mod r43_credential_tests{
 use super::*;
 use std::time::{SystemTime,UNIX_EPOCH};
 #[test]
 fn systemd_runtime_credential_can_be_read_only_without_leaking_it(){
  let t=SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
  let root=std::env::temp_dir().join(format!(
   "r43_credential_metadata_{}_{}",std::process::id(),t));
  fs::create_dir(&root).unwrap();
  let p=root.join("fake_credential_not_a_real_secret");
  fs::write(&p,b"TEST_FAKE_VALUE_123").unwrap();
  fs::set_permissions(&p,fs::Permissions::from_mode(0o444)).unwrap();
  assert_eq!(secure_read(&p,64,true),b"TEST_FAKE_VALUE_123");
  fs::set_permissions(&p,fs::Permissions::from_mode(0o400)).unwrap();
  assert_eq!(secure_read(&p,64,true),b"TEST_FAKE_VALUE_123");
  fs::set_permissions(&p,fs::Permissions::from_mode(0o600)).unwrap();
  assert_eq!(secure_read(&p,64,false),b"TEST_FAKE_VALUE_123");
  fs::remove_dir_all(root).unwrap();
 }
}
