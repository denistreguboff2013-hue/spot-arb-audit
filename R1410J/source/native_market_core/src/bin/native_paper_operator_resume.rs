//! Explicit, one-shot operator rebaseline for a quarantined PUBLIC PAPER
//! research day. Distinguishes REAL simulated entries from budget burned by
//! legacy uncertainty. Does NOT reconcile orphaned virtual positions.
//! Call only after the PUBLIC PAPER systemd unit is STOPPED; never trades.
use serde_json::{Value,json};
use openssl::sha::sha256;
use std::{
 env,fs::{self,File,OpenOptions},
 io::{self,Write},os::fd::AsRawFd,path::Path,
 time::{SystemTime,UNIX_EPOCH},
};
const DEFAULT_ROOT:&str="/mnt/spotarb_db_test/astra_paper_continuous_v1";
const SOURCE_REPORT:&str="manual_immediate_cutover_20261013_071902/uncarried_virtual_position_report.json";
const CAP:u64=500;
fn problem(msg:&'static str)->io::Error{io::Error::new(io::ErrorKind::Other,msg)}
fn wall_s()->u64{
 SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}
fn yek_day(t:u64)->u64{(t+5*3600)/86400}
fn load(path:&Path)->io::Result<Value>{
 serde_json::from_slice(&fs::read(path)?).map_err(|_|problem("invalid JSON"))
}
fn fsync_new(path:&Path,data:&[u8])->io::Result<()>{
 let mut file=OpenOptions::new().create_new(true).write(true).open(path)?;
 file.write_all(data)?;file.sync_all()?;
 File::open(path.parent().ok_or(problem("no parent"))?)?.sync_all()
}
fn atomic(root:&Path,name:&str,v:&Value)->io::Result<()>{
 let dst=root.join(name);
 let tmp=root.join(format!("{name}.operator_pending"));
 let content=serde_json::to_vec_pretty(v)?;
 fsync_new(&tmp,&content)?;
 fs::rename(tmp,dst)?;
 File::open(root)?.sync_all()?;
 Ok(())
}
#[derive(Debug)]
struct Verified{
 day:u64,real_model_entries:u64,burned_budget:u64,
 prior_session:String,source_sha256:String,
}
fn inspect(root:&Path,archive:&Path,now:u64)->io::Result<Verified>{
 if root.join("operator_resume_applied_20261013.json").exists(){
  return Err(problem("operator resume is strictly single-use"))
 }
 let doc_bytes=fs::read(archive)?;
 let old:Value=serde_json::from_slice(&doc_bytes)?;
 let ledger=load(&root.join("daily_budget.json"))?;
 let current=load(&root.join("current_session.json"))?;
 let status_path=current["status_path"].as_str()
    .ok_or(problem("current PAPER status missing"))?;
 // The supervisor may have been stopped. The final periodic status
 // still must show zero ACTIVE virtual positions before new generation.
 let status=load(Path::new(status_path))?;
 let day=yek_day(now);
 let real=old["last_report_entry_count"].as_u64()
    .ok_or(problem("original actual model entry count missing"))?;
 let burned=ledger["entries"].as_u64()
    .ok_or(problem("missing burned PAPER quota"))?;
 if day!=20739||ledger["day"].as_u64()!=Some(day)||
    old["schema"]!="paper-forced-cutover-quarantine-v1"||
    old["old_session"].as_str()!=Some("session_20739_1791853253")||
    old["status"]!="NO_CONFIRMED_CLOSE_OR_CARRY"||
    old["model_positions_must_not_count_as_closed"]!=true||
    old["old_virtual_open_count"].as_u64()!=Some(1)||
    old["new_paper_entries_allowed_today"]!=false||
    real!=49||burned!=CAP||
    ledger["paper_only"]!=true||
    ledger["reason"]!="complete_public_paper_episode"||
    current["day"].as_u64()!=Some(day)||
    current["paper_only"]!=true||
    status["live_trading"]!=false||
    status["private_api"]!=false||
    status["paper_portfolio"]["open_positions"].as_u64()!=Some(0)||
    status["paper_portfolio"]["counters"]["entries"].as_u64()!=Some(CAP)||
    status["paper_portfolio"]["pending_positions"].as_array().map_or(true,|v|!v.is_empty()){
    return Err(problem("PAPER history or safety mismatch: do NOT reset"))
 }
 let hash=sha256(&doc_bytes).iter().map(|b|format!("{b:02x}")).collect();
 Ok(Verified{day,real_model_entries:real,burned_budget:burned,
    prior_session:current["session"].as_str()
       .ok_or(problem("current session missing"))?.to_owned(),
    source_sha256:hash})
}
fn apply(root:&Path,archive:&Path,now:u64)->io::Result<()>{
 // The exact same OS flock held for the whole lifetime of the running
 // public supervisor. Never mutate counters while it is active.
 let lock=OpenOptions::new().read(true).write(true)
    .open(root.join("supervisor.lock"))?;
 if unsafe{libc::flock(lock.as_raw_fd(),libc::LOCK_EX|libc::LOCK_NB)}!=0{
    return Err(problem("public PAPER supervisor is running; operator reset denied"))
 }
 let v=inspect(root,archive,now)?;
 // Journal first, with create_new and fsync; if any step fails before
 // daily update, old 500/500 quarantine remains intact.
 let receipt=json!({
  "schema":"public-paper-operator-generation-rebaseline-v1",
  "explicit_operator_request":true,
  "day_yek":v.day,"previous_budget":v.burned_budget,
  "actual_modeled_entries":v.real_model_entries,
  "remaining_model_entries":CAP-v.real_model_entries,
  "superseded_quarantine_reason":"historical_unsealed_virtual_episode",
  "orphaned_virtual_positions_remain_unreconciled":true,
  "may_count_orphans_as_closed":false,
  "may_count_orphans_as_pnl":false,
  "new_virtual_book_epoch":true,
  "old_session_before_reset":v.prior_session,
  "archive_source_sha256":v.source_sha256,
  "archive_source_path":archive,
  "private_exchange_api":false,
  "exchange_fill_attestation":false,
  "real_trading_authorized":false,
  "recorded_wall_s":now,
 });
 let path=root.join("operator_resume_applied_20261013.json");
 fsync_new(&path,&serde_json::to_vec_pretty(&receipt)?)?;
 // Preserve the exact orphaned old active episode marker; the next
 // supervisor may then start a NEW empty PAPER generation.
 let active=root.join("active.json");
 if active.exists(){
   fs::rename(&active,root.join("operator_previous_active_20261013.json"))?;
 }
 if root.join("planned_handoff.json").exists(){
   // Do not transport modeled exposures from the old virtual generation.
   fs::rename(root.join("planned_handoff.json"),
              root.join("operator_previous_handoff_20261013.json"))?;
 }
 atomic(root,"daily_budget.json",&json!({
  "day":v.day,"entries":v.real_model_entries,
  "paper_only":true,
  "reason":"operator_approved_paper_only_generation_rebaseline",
  "wall_s":now,
  "source_receipt":path,
  "model_virtual_book_reset_only":true,
  "real_trading_authorized":false,
 }))?;
 println!("PAPER_OPERATOR_RESUME_APPLIED day={} proven_model_entries={} remaining={} real_orders=false",v.day,v.real_model_entries,CAP-v.real_model_entries);
 Ok(())
}
fn main()->io::Result<()>{
 let args:Vec<String>=env::args().collect();
 if args.len()!=2||!matches!(args[1].as_str(),"inspect"|"apply"){
   return Err(problem("usage: native_paper_operator_resume inspect|apply"))
 }
 let root=Path::new(DEFAULT_ROOT);
 let archive=root.join(SOURCE_REPORT);
 let now=wall_s();
 if args[1]=="inspect"{
  let v=inspect(root,&archive,now)?;
  println!("PAPER_OPERATOR_REBASELINABLE day={} actual_entries={} old_artificial_entries={} remaining={} orphans_unresolved=true",v.day,v.real_model_entries,v.burned_budget,CAP-v.real_model_entries);
  return Ok(())
 }
 apply(root,&archive,now)
}
#[cfg(test)]
mod tests{
 use super::*;
 use std::path::PathBuf;
 fn folder()->PathBuf{std::env::temp_dir().join(format!(
  "operator_paper_reset_{}_{}",std::process::id(),
  spotarb_native_market_core::transport::monotonic_ns()))}
 #[test]
 fn explicit_rebaseline_is_bounded_single_use_and_preserves_orphan_audit(){
  let root=folder();
  fs::create_dir_all(&root).unwrap();
  let archive=root.join("orphan.json");
  let status=root.join("last.json");
  fs::write(root.join("supervisor.lock"),b"").unwrap();
  let now=1791858000_u64;
  fs::write(&archive,serde_json::to_vec(&json!({
   "schema":"paper-forced-cutover-quarantine-v1",
   "old_session":"session_20739_1791853253",
   "last_report_entry_count":49,"old_virtual_open_count":1,
   "status":"NO_CONFIRMED_CLOSE_OR_CARRY",
   "model_positions_must_not_count_as_closed":true,
   "new_paper_entries_allowed_today":false,
  })).unwrap()).unwrap();
  fs::write(root.join("daily_budget.json"),serde_json::to_vec(&json!({
   "day":20739,"entries":500,"paper_only":true,
   "reason":"complete_public_paper_episode"})).unwrap()).unwrap();
  fs::write(root.join("current_session.json"),serde_json::to_vec(&json!({
   "day":20739,"session":"session_20739_11","paper_only":true,
   "status_path":status})).unwrap()).unwrap();
  fs::write(&status,serde_json::to_vec(&json!({
   "live_trading":false,"private_api":false,
   "paper_portfolio":{"open_positions":0,
      "counters":{"entries":500},"pending_positions":[]}
  })).unwrap()).unwrap();
  fs::write(root.join("active.json"),b"old_paper_active").unwrap();
  assert_eq!(inspect(&root,&archive,now).unwrap().real_model_entries,49);
  apply(&root,&archive,now).unwrap();
  assert_eq!(load(&root.join("daily_budget.json")).unwrap()["entries"],49);
  assert!(!root.join("active.json").exists());
  assert_eq!(fs::read(root.join("operator_previous_active_20261013.json")).unwrap(),b"old_paper_active");
  assert!(inspect(&root,&archive,now).is_err());
  fs::remove_dir_all(root).unwrap();
 }
 #[test]fn missing_or_mismatched_history_rejects_reset(){
  let root=folder();fs::create_dir_all(&root).unwrap();
  assert!(inspect(&root,&root.join("notfound"),1791858000).is_err());
  fs::remove_dir_all(root).unwrap();
 }
}
