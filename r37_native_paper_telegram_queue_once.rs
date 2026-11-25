//! Astra R37: autonomous-friendly PAPER status -> durable PREVIEW queue.
//! Never opens network. Runs one bounded observation per invocation.
//! Queue + cursor fsync atomically. No Telegram token/API or trade.
use spotarb_native_market_core::{
 native_trading_config::NativeTradingConfig,
 native_paper_telegram_observer::{
  parse_native_paper_status,
 },
 native_paper_notice_queue_review::{
  advance_native_paper_watch_oldstyle,PaperWatchState,
 },
 native_virtual_four_leg_provenance_review::inspect_model_trade_notices,
};
use serde_json::{Value,json};
use std::{
 env,fs::{self,File,OpenOptions},io::{Read,Write},
 os::{fd::AsRawFd,unix::fs::{OpenOptionsExt,MetadataExt,PermissionsExt}},
 path::{Path,PathBuf},time::{SystemTime,UNIX_EPOCH},
};
const SOURCE:&str="/mnt/spotarb_db_test/astra_paper_continuous_v1";
const OUTPUT:&str="/mnt/spotarb_db_test/astra_p0_02_execution_gate_20261011/r37_paper_telegram_preview_watch_20261015";
const CURSOR:&str="durable_watch_queue.json";
const LIMIT:usize=64*1024;
fn fail(reason:&str)->!{eprintln!("R37_PAPER_WATCH_DENIED:{reason}");std::process::exit(3)}
fn now_ms()->u64{
 SystemTime::now().duration_since(UNIX_EPOCH)
  .unwrap_or_default().as_millis() as u64
}
fn code(s:&str)->bool{
 !s.is_empty()&&s.len()<=100&&s.bytes().all(|b|
  b.is_ascii_alphanumeric()||b==b'-'||b==b'_')
}
fn safe_bytes(path:&Path)->Vec<u8>{
 let mut f=OpenOptions::new().read(true)
  .custom_flags(libc::O_NOFOLLOW|libc::O_CLOEXEC)
  .open(path).unwrap_or_else(|_|fail("source_file_missing_or_symlink"));
 let meta=f.metadata().unwrap_or_else(|_|fail("source_metadata_denied"));
 if !meta.is_file()||meta.mode()&0o077!=0||meta.nlink()!=1
  ||meta.len()>LIMIT as u64{fail("source_file_permissions_size_or_type")}
 let mut b=Vec::new();
 Read::by_ref(&mut f).take((LIMIT+1) as u64).read_to_end(&mut b)
  .unwrap_or_else(|_|fail("source_file_read_error"));
 if b.len()>LIMIT{fail("source_file_oversize")}
 b
}
fn source(path:&Path)->Value{
 serde_json::from_slice(&safe_bytes(path))
  .unwrap_or_else(|_|fail("malformed_read_only_paper_source"))
}
struct Lock(File);
impl Lock{
 fn acquire(folder:&Path)->Self{
  let f=OpenOptions::new().read(true).write(true).create(true)
   .mode(0o600).custom_flags(libc::O_NOFOLLOW|libc::O_CLOEXEC)
   .open(folder.join(".read_only_watch_queue.flock"))
   .unwrap_or_else(|_|fail("observer_lock_unavailable"));
  if f.metadata().map(|m|!m.is_file()||m.mode()&0o077!=0).unwrap_or(true)
   ||unsafe{libc::flock(f.as_raw_fd(),libc::LOCK_EX)}!=0{
   fail("observer_lock_unsafe")
  }
  Self(f)
 }
}
impl Drop for Lock{
 fn drop(&mut self){unsafe{libc::flock(self.0.as_raw_fd(),libc::LOCK_UN);}}
}
fn load_cursor(path:&Path)->Option<PaperWatchState>{
 if !path.exists(){return None}
 let value:PaperWatchState=serde_json::from_slice(&safe_bytes(path))
  .unwrap_or_else(|_|fail("observer_cursor_corrupt"));
 if value.schema!=1{fail("observer_cursor_schema_mismatch")}
 Some(value)
}
fn persist_cursor(path:&Path,state:&PaperWatchState){
 let data=serde_json::to_vec(state).unwrap_or_else(|_|fail("cursor_json_failure"));
 if data.len()>LIMIT{fail("cursor_oversized")}
 let directory=path.parent().unwrap_or_else(||fail("cursor_parent_missing"));
 let tmp=directory.join(format!(".observer.tmp.{}",std::process::id()));
 let mut file=OpenOptions::new().write(true).create_new(true)
  .mode(0o600).custom_flags(libc::O_NOFOLLOW|libc::O_CLOEXEC)
  .open(&tmp).unwrap_or_else(|_|fail("cursor_tmp_collision"));
 file.write_all(&data).unwrap_or_else(|_|fail("cursor_write_failed"));
 file.sync_all().unwrap_or_else(|_|fail("cursor_fsync_failed"));
 drop(file);
 fs::rename(&tmp,path).unwrap_or_else(|_|fail("cursor_atomic_replace_failed"));
 File::open(directory).and_then(|f|f.sync_all())
  .unwrap_or_else(|_|fail("cursor_parent_fsync_failed"));
}
fn main(){
 // This binary has NO Telegram transport and NO credential handling.
 if env::var("R37_WATCH_PREVIEW_NO_TELEGRAM").ok().as_deref()!=Some("YES"){
  fail("explicit_preview_only_flag_required")
 }
 let root=Path::new(SOURCE);
 let out=Path::new(OUTPUT);
 if !root.is_dir()||!out.is_dir()
  ||fs::symlink_metadata(out).map(|m|!m.is_dir()||m.permissions().mode()&0o077!=0).unwrap_or(true){
  fail("source_or_owner_private_review_directory_missing")
 }
 let _lock=Lock::acquire(out);
 let current_path=root.join("current_session.json");
 let active_path=root.join("active.json");
 let a1=safe_bytes(&current_path);
 let b1=safe_bytes(&active_path);
 let current:Value=serde_json::from_slice(&a1)
  .unwrap_or_else(|_|fail("current_session_json_denied"));
 let active:Value=serde_json::from_slice(&b1)
  .unwrap_or_else(|_|fail("active_session_json_denied"));
 let session=current.get("session").and_then(Value::as_str)
  .unwrap_or_else(||fail("missing_paper_session"));
 if !code(session)||!session.starts_with("session_"){
  fail("invalid_session_directory_name")
 }
 let expected=root.join("sessions").join(session).join("status.json");
 if current.get("status_path").and_then(Value::as_str)
  !=Some(expected.to_str().unwrap_or("")){
  fail("session_status_path_outside_root")
 }
 let status=source(&expected);
 // Avoid mixing status from two running/switching sessions.
 if a1!=safe_bytes(&current_path)||b1!=safe_bytes(&active_path){
  fail("paper_session_changed_during_read")
 }
 let cfg=NativeTradingConfig::load(
  &PathBuf::from(env!("CARGO_MANIFEST_DIR"))
    .join("config/native_trading.yaml"))
  .unwrap_or_else(|_|fail("strict_rust_config_read_denied"));
 let now=now_ms();
 let point=parse_native_paper_status(&current,&active,&status,now,&cfg)
  .unwrap_or_else(|_|fail("unverified_or_stale_paper_state"));
 let modeled_path=root.join("sessions").join(session)
  .join("modeled_four_leg_journal.jsonl");
 let metadata=fs::symlink_metadata(&modeled_path)
  .unwrap_or_else(|_|fail("model_audit_file_missing"));
 if !metadata.is_file()||metadata.permissions().mode()&0o077!=0
  ||metadata.len()==0||metadata.len()>8*1024*1024{
  fail("untrusted_model_four_leg_journal")
 }
 let (verified,trade_rows)=inspect_model_trade_notices(&modeled_path)
  .unwrap_or_else(|_|fail("unverified_or_partial_model_four_leg_journal"));
 let cursor_path=out.join(CURSOR);
 let cursor=load_cursor(&cursor_path);
 // Old-style immediate modeled opens/closes + 1h heartbeat + daily.
 // NEVER enable the cancelled R39 30-minute summary.
 let observation=advance_native_paper_watch_oldstyle(
  cursor.as_ref(),point,&cfg,&verified,&trade_rows,now)
  .unwrap_or_else(|_|fail("oldstyle_four_leg_watch_or_monotonicity_denied"));
 // New notices and monotonic cursor are written in one atomic 0600 fsync
 // transaction. No external Telegram messages can ever be sent here.
 persist_cursor(&cursor_path,&observation.next);
 println!("{}",json!({
  "schema":"spotarb_native_paper_telegram_durable_queue_preview_v1",
  "live_paper_snapshot":true,
  "real_exchange_orders":false,
  "private_exchange_accounts":false,
  "telegram_bot_api_called":false,
  "telegram_credentials_loaded":false,
  "paper_virtual_model_only":true,
  "reason":observation.reason,
  "baseline_created":observation.baseline_created,
  "new_notices_queued":observation.newly_queued.len(),
  "new_notice_ids":observation.newly_queued,
  "unsent_preview_queue_count":observation.next.pending.len(),
  "queue_revision":observation.next.revision,
  "session":observation.next.cursor.last_seen.session,
  "status_updated_wall_ms":observation.next.cursor.last_seen.updated_wall_ms,
  "virtual_entries":observation.next.cursor.last_seen.entries,
  "virtual_closes":observation.next.cursor.last_seen.closes,
  "fault_counter":observation.next.cursor.last_seen.faults,
  "ordinary_heartbeats_sent":0
 }));
}
