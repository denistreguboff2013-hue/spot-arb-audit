//! Astra R1113: deliberately OFFLINE, owner-private Rust SUPERVISOR child orchestration.
//! Existing installed systemd unit and deployed child are NEVER started or modified.
//! This is NOT a live-paper deployment. It joins previously certified private
//! owner/portfolio STOP+RESTART to independent 17-table-shaped DB2/ACK receiver.
use serde_json::{json,Value};
use std::{ffi::OsStr,fs::{self,File,OpenOptions},io::Write,
  os::unix::fs::{MetadataExt,OpenOptionsExt,PermissionsExt},path::{Path,PathBuf},
  process::Command};
const T:&str="/mnt/spotarb_db_test/astra_p001_r874_test_scratch_20261025";
const OLD:&str="/mnt/spotarb_db_test/astra_paper_continuous_v1/portfolio_checkpoint.json";
const KILL:&str="/home/tmpadmin/spotbot/data/real_kill_switch.json";
const ORIGINAL:&str="423ba8e64e003bc36e73bf5fda4313d3d9ac8b781ecdfb82761d0e9cd7a9f6f9";
fn digest(p:&Path)->Result<String,String>{
 let bytes=fs::read(p).map_err(|e|format!("evidence read: {e}"))?;
 Ok(openssl::sha::sha256(&bytes).iter().map(|b|format!("{b:02x}")).collect())
}
pub fn private_root_name(root:&Path)->bool{
 root.parent()==Some(Path::new(T)) &&
 root.file_name().and_then(OsStr::to_str).is_some_and(|s|
     s.starts_with("astra_r671_file_r1084_R1113_") && s.len()>=35 &&
     s.bytes().all(|x|x.is_ascii_alphanumeric()||x==b'_'))
}
fn guard(root:&Path)->Result<(),String>{
 if std::env::var("ASTRA_R1113_OFFLINE_SUPERVISOR").ok().as_deref()!=Some("YES"){
    return Err("R1113 explicit private mode is required".into())
 }
 if !private_root_name(root){return Err("R1113 unsafe private root path".into())}
 let meta=fs::symlink_metadata(root).map_err(|e|format!("private root: {e}"))?;
 if !meta.file_type().is_dir()||meta.permissions().mode()&0o077!=0||
     meta.uid()!=unsafe{libc::geteuid()}{
    return Err("R1113 root owner, type or mode denied".into())
 }
 let service=Command::new("/usr/bin/systemctl")
   .args(["show","spotarb-native-paper-continuous.service",
      "-p","ActiveState","-p","MainPID","-p","UnitFileState"])
   .output().map_err(|e|format!("service status: {e}"))?;
 let s=String::from_utf8_lossy(&service.stdout);
 if !service.status.success()||
    !s.lines().any(|v|v=="ActiveState=inactive")||
    !s.lines().any(|v|v=="MainPID=0")||
    !s.lines().any(|v|v=="UnitFileState=disabled"){
    return Err("R1113 installed PAPER not fully stopped/disabled".into())
 }
 let kill:Value=serde_json::from_slice(&fs::read(KILL)
     .map_err(|_|"REAL kill switch unavailable")?)
     .map_err(|_|"REAL kill malformed")?;
 if kill["state"]!="ENGAGED"{return Err("R1113 REAL kill not engaged".into())}
 if digest(Path::new(OLD))?!=ORIGINAL {
    return Err("R1113 frozen original PAPER checkpoint changed".into())
 }
 Ok(())
}
fn launch(bin:&Path,mode:&str,root:&Path,envkey:&str)->Result<Value,String>{
 if !bin.is_file()||bin.is_symlink(){
    return Err(format!("R1113 child missing or symlink: {mode}"))
 }
 // Each original Rust child has a hard deadline. Only the private
 // child is killed on a timeout; NEVER signal the installed service.
 let mut c=Command::new("/usr/bin/timeout");
 c.env_clear().env("PATH","/usr/bin:/bin").env("TMPDIR",T)
  .env(envkey,"YES").arg("--signal=TERM").arg("--kill-after=5s")
  .arg("80s").arg(bin).arg(mode).arg(root);
 if envkey=="ASTRA_R1066_PRIVATE_ONLY"{c.arg("1024");}
 let out=c.output().map_err(|e|format!("{mode}: private child start: {e}"))?;
 if !out.status.success(){
    return Err(format!("{mode}: private child returned {:?}, error {}",
       out.status.code(),String::from_utf8_lossy(&out.stderr)))
 }
 let v:Value=serde_json::from_slice(&out.stdout)
    .map_err(|_|format!("{mode}: malformed result"))?;
 if v["ok"]!=true{return Err(format!("{mode}: missing PASS flag"))}
 Ok(v)
}
// R1114: deterministic process-boundary failure injection; ONLY in
// the guarded scratch-mode supervisor. Quarantine is persisted BEFORE
// returning an error, so no DB2 ACK or session release can be advertised.
fn r1114_refuse_at_boundary(root:&Path,stage:&str,joint:&Value)
  ->Result<(),String>{
 let requested=std::env::var("ASTRA_R1114_INJECT_FAILURE")
   .ok().unwrap_or_default();
 if requested.is_empty(){return Ok(())}
 if requested!=stage{return Ok(())}
 guard(root)?;
 let record=json!({
    "schema":"SPOTARB/ASTRA/R1114/PRIVATE-INJECTED-STOP/V1",
    "stage":stage,"quarantined":true,"real_orders":false,
    "installed_service_touched":false,
    "next_model_entries_authorized":false,
    "source_db2_ack_certified":false,
    "old_original_sha256":ORIGINAL,
    "modeled_checkpoint_sha256":joint["checkpoint_sha256"],
    "modeled_journal_sha256":joint["journal_sha256"]
 });
 let path=root.join("r1114_failclosed_hold.json");
 let mut file=OpenOptions::new().write(true).create_new(true)
   .mode(0o600).open(&path).map_err(|e|format!("R1114 hold create: {e}"))?;
 file.write_all(&serde_json::to_vec_pretty(&record)
    .map_err(|_|"R1114 invalid hold")?)
   .map_err(|e|format!("R1114 hold write: {e}"))?;
 file.sync_all().map_err(|e|format!("R1114 hold sync: {e}"))?;
 File::open(root).and_then(|f|f.sync_all())
    .map_err(|e|format!("R1114 parent sync: {e}"))?;
 Err(format!("R1114 INJECTED_FAIL_CLOSED at {stage}; durable private quarantine"))
}
pub fn run(root:&Path)->Result<Value,String>{
 guard(root)?;
 let fail=std::env::var("ASTRA_R1114_INJECT_FAILURE").ok()
    .unwrap_or_default();
 if !fail.is_empty()&&fail!="AFTER_SEALED_STOP"&&fail!="BEFORE_DB2_ACK"{
    return Err("R1114 unknown offline fault injection denied".into())
 }
 for p in ["r671-private-db1.sqlite","r671-private-db2.sqlite",
     "virtual_checkpoint.json","virtual_four_leg.jsonl","joint_stop_receipt.json",
     "r1113_supervisor_join_receipt.json","r1114_failclosed_hold.json"]{
    if root.join(p).exists(){return Err(format!("R1113 private root already used: {p}"))}
 }
 let supervisor=std::env::current_exe().map_err(|e|format!("current exe: {e}"))?;
 let bin=supervisor.parent().ok_or("R1113 missing child binary directory")?;
 let source=bin.join("r1066_private_paper_collector_canary");
 let joined=bin.join("r1084_unified_paper_restart_canary");
 let init=launch(&source,"--init",root,"ASTRA_R1066_PRIVATE_ONLY")?;
 if init["production"]!=false{return Err("R1113 init is not private".into())}
 guard(root)?;
 let stop=launch(&joined,"--stop",root,"ASTRA_R1084_PRIVATE_ONLY")?;
 if stop["phase"]!="STOP"||stop["source_events"]!=512||
     stop["source_transactions"]!=16||stop["portfolio_open"]!=3||
     stop["entries"]!=37||stop["closed"]!=34||stop["real_orders"]!=false{
    return Err("R1113 wrong original joint stopped source/model state".into())
 }
 let joint:Value=serde_json::from_slice(
    &fs::read(root.join("joint_stop_receipt.json"))
       .map_err(|_|"R1113 missing joint stop receipt")?)
    .map_err(|_|"R1113 malformed joint stop receipt")?;
 if joint["joint_storage_transaction_atomic"]!=false||
     joint["no_real_orders"]!=true||
     joint["checkpoint_sha256"]!=digest(&root.join("virtual_checkpoint.json"))?||
     joint["journal_sha256"]!=digest(&root.join("virtual_four_leg.jsonl"))?{
    return Err("R1113 source modeled state/receipt mismatch".into())
 }
 r1114_refuse_at_boundary(root,"AFTER_SEALED_STOP",&joint)?;
 guard(root)?;
 let restart=launch(&joined,"--restart",root,"ASTRA_R1084_PRIVATE_ONLY")?;
 if restart["phase"]!="RESTART"||restart["original_source_replayed"]!=512||
     restart["new_source_events"]!=512||restart["total_private_source_events"]!=1024||
     restart["durable_batches_total"]!=32||
     restart["virtual_positions_restored"]!=3||
     restart["model_checkpoint_unchanged"]!=true||
     restart["modeled_journal_unchanged"]!=true||restart["real_orders"]!=false{
    return Err("R1113 wrong source recovery or modeled virtual portfolio".into())
 }
 r1114_refuse_at_boundary(root,"BEFORE_DB2_ACK",&joint)?;
 guard(root)?;
 let receiver=launch(&source,"--receiver-batched32",root,"ASTRA_R1066_PRIVATE_ONLY")?;
 if receiver["source_seq"]!=32||receiver["db2_applied"]!=32||
    receiver["ack_seq"]!=32||receiver["unacked"]!=0||
    receiver["physical_events_17table"]!=1024||
    receiver["exchange_orders"]!=false{
    return Err("R1113 receiver/source ACK mismatch".into())
 }
 guard(root)?;
 if digest(&root.join("virtual_checkpoint.json"))? !=
    joint["checkpoint_sha256"].as_str().ok_or("R1113 invalid old checkpoint SHA")?||
    digest(&root.join("virtual_four_leg.jsonl"))? !=
    joint["journal_sha256"].as_str().ok_or("R1113 invalid old journal SHA")?{
    return Err("R1113 checkpoint/journal mutation during collector ACK".into())
 }
 let result=json!({
  "schema":"SPOTARB/ASTRA/R1113/PRIVATE-RUST-SUPERVISOR-JOIN/V1",
  "ok":true,"supervisor_invoked_all_children":true,
  "private_root":root.to_string_lossy(),
  "source":"SYNTHETIC_PUBLIC_ONLY","source_events_unique":1024,
  "source_db1_seq":32,"receiver_db2_seq":32,"source_ack_seq":32,
  "pending":0,"virtual_entries":37,"virtual_closed":34,
  "virtual_open_restored":3,"modeled_checkpoint_sha256":joint["checkpoint_sha256"],
  "modeled_journal_sha256":joint["journal_sha256"],
  "joint_cross_file_atomicity_certified":false,
  "installed_paper_touched":false,"telegram_sent":false,
  "exchange_requests":0,"real_orders":0,
  "old_operator_original_checkpoint_sha256":ORIGINAL,
  "rollout_certified":false
 });
 let path=root.join("r1113_supervisor_join_receipt.json");
 let mut f=OpenOptions::new().write(true).create_new(true).mode(0o600)
    .open(&path).map_err(|e|format!("R1113 receipt create: {e}"))?;
 let bytes=serde_json::to_vec_pretty(&result)
    .map_err(|_|"R1113 report serialize")?;
 f.write_all(&bytes).map_err(|e|format!("R1113 report write: {e}"))?;
 f.sync_all().map_err(|e|format!("R1113 report sync: {e}"))?;
 File::open(root).and_then(|f|f.sync_all())
    .map_err(|e|format!("R1113 dir sync: {e}"))?;
 Ok(result)
}
#[cfg(test)]
mod tests{
 use super::*;
 #[test]
 fn r1113_rejects_wrong_roots_or_missing_credentials(){
   assert!(!private_root_name(Path::new("/tmp/astra_r671_file_r1084_R1113_test")));
   assert!(!private_root_name(Path::new(
       "/mnt/spotarb_db_test/astra_p001_r874_test_scratch_20261025/astra_r671_file_r1084_R1112_12345")));
   assert!(private_root_name(Path::new(
       "/mnt/spotarb_db_test/astra_p001_r874_test_scratch_20261025/astra_r671_file_r1084_R1113_12345678")));
 }
}
