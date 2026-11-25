//! P1-06 Rust PAPER status -> no-network Telegram notice planning.
//! Observes existing PAPER service read-only and persists its OWN
//! owner-private no-spam cursor. No Bot API or private exchange paths.
use spotarb_native_market_core::{
 native_trading_config::NativeTradingConfig,
 native_paper_telegram_observer::{
  parse_native_paper_status,plan_native_paper_notices,PaperObserveCursor,
 },
};
use serde_json::{Value,json};
use std::{
 env,fs::{self,File,OpenOptions},io::{Read,Write},
 os::{fd::AsRawFd,unix::fs::{OpenOptionsExt,MetadataExt,PermissionsExt}},
 path::{Path,PathBuf},time::{SystemTime,UNIX_EPOCH},
};
const SOURCE:&str="/mnt/spotarb_db_test/astra_paper_continuous_v1";
const OUTPUT:&str="/mnt/spotarb_db_test/astra_p0_02_execution_gate_20261011/r36_paper_telegram_bridge_20261015";
const CURSOR:&str="read_only_observer_cursor.json";
const LIMIT:usize=64*1024;
fn fail(reason:&str)->!{eprintln!("R36_PUBLIC_PAPER_PREVIEW_DENIED:{reason}");std::process::exit(3)}
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
   .open(folder.join(".read_only_observer.flock"))
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
fn load_cursor(path:&Path)->Option<PaperObserveCursor>{
 if !path.exists(){return None}
 let value:PaperObserveCursor=serde_json::from_slice(&safe_bytes(path))
  .unwrap_or_else(|_|fail("observer_cursor_corrupt"));
 if value.schema!=1{fail("observer_cursor_schema_mismatch")}
 Some(value)
}
fn persist_cursor(path:&Path,state:&PaperObserveCursor){
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
 if env::var("R36_PREVIEW_NO_TELEGRAM").ok().as_deref()!=Some("YES"){
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
 let cursor_path=out.join(CURSOR);
 let cursor=load_cursor(&cursor_path);
 let summaries=env::var("R36_ENABLE_VIRTUAL_SUMMARY_PREVIEW")
  .ok().as_deref()==Some("YES");
 let observation=plan_native_paper_notices(cursor.as_ref(),point,&cfg,summaries)
  .unwrap_or_else(|_|fail("paper_cursor_monotonicity_denied"));
 // Commit no-spam cursor BEFORE any console output. A crash may lose a
 // preview; it cannot produce duplicate Telegram network calls.
 persist_cursor(&cursor_path,&observation.next);
 println!("{}",json!({
  "schema":"spotarb_native_paper_telegram_preview_v1",
  "config_sha256":cfg.sha256,
  "live_paper_snapshot":true,"exchange_orders":false,
  "private_exchange_account_access":false,
  "telegram_bot_api_called":false,
  "paper_model_pnl_only":true,
  "reason":observation.reason,
  "baseline_created":observation.baseline_created,
  "notice_count":observation.notices.len(),
  "russian_preview_not_sent":observation.notices,
  "session":observation.next.last_seen.session,
  "status_updated_ms":observation.next.last_seen.updated_wall_ms,
  "virtual_entries":observation.next.last_seen.entries,
  "virtual_closes":observation.next.last_seen.closes,
  "observed_errors":observation.next.last_seen.faults,
  "ordinary_heartbeats_sent":0
 }));
}
