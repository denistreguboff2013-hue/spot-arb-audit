//! Astra P0-01 R1062: verify exact STOPPED, archived PAPER MODEL state.
//! READ ONLY: no new checkpoint, no fabricated timestamp, no order/telegram IO.
//! An accepted archive is EVIDENCE, NOT approval to restart expired exposure.
use serde_json::{json, Value};
use openssl::sha::sha256;
use std::{fs, path::Path, os::unix::fs::MetadataExt,
          time::{SystemTime,UNIX_EPOCH}};
use crate::native_paper_continuity_checkpoint_review::{
    inspect_planned_seal, inspect_checkpoint_model_alignment
};
const MAX_ARCHIVE_FILES:usize=16;
fn denied(why:&str)->String{format!("R1062 DENIED: {why}")}
fn safe_bytes(path:&Path,max:u64)->Result<Vec<u8>,String>{
 let m=fs::symlink_metadata(path).map_err(|_|denied("file missing"))?;
 if !m.file_type().is_file()||m.nlink()!=1||m.mode()&0o077!=0||
    m.len()==0||m.len()>max{return Err(denied("file kind, link, permissions or size"))}
 fs::read(path).map_err(|_|denied("cannot read evidence"))
}
fn receipt_bytes(path:&Path)->Result<Vec<u8>,String>{
 // The privileged operator R1059 writes as root into a tmpadmin-owned
 // 0700 directory. Its 0644 receipt cannot be replaced through this
 // read-only verifier. Do not relax 0600 checks for OTHER files.
 let m=fs::symlink_metadata(path).map_err(|_|denied("operator receipt missing"))?;
 let parent=path.parent().ok_or_else(||denied("receipt parent"))?;
 let p=fs::symlink_metadata(parent).map_err(|_|denied("receipt parent absent"))?;
 if path.file_name().and_then(|s|s.to_str())!=Some("privileged_stop_receipt.json")||
    !m.file_type().is_file()||m.uid()!=0||m.nlink()!=1||
    m.mode()&0o022!=0||m.len()==0||m.len()>8192||
    !p.file_type().is_dir()||p.mode()&0o077!=0{
  return Err(denied("untrusted root operator receipt"))
 }
 fs::read(path).map_err(|_|denied("operator receipt unreadable"))
}
fn json_file(path:&Path,max:u64)->Result<Value,String>{
 serde_json::from_slice(&safe_bytes(path,max)?)
    .map_err(|_|denied("malformed source JSON"))
}
fn hex_sha(bytes:&[u8])->String{
 sha256(bytes).iter().map(|b|format!("{b:02x}")).collect()
}
fn exact_sha(v:&Value)->Result<&str,String>{
 let s=v.as_str().ok_or_else(||denied("missing SHA"))?;
 if s.len()!=64||!s.bytes().all(|c|c.is_ascii_hexdigit()){
  return Err(denied("malformed SHA"))
 }
 Ok(s)
}
const FILES:[(&str,&str);7]=[
 ("portfolio_checkpoint.json","portfolio_checkpoint.json"),
 ("planned_handoff.json","planned_handoff.json"),
 ("session_checkpoint_at_exit.json","checkpoint_at_exit.json"),
 ("session_final.json","final.json"),
 ("session_summary.json","supervisor_summary.json"),
 ("session_four_leg_journal.jsonl","modeled_four_leg_journal.jsonl"),
 ("operator_stop_receipt.json","operator_stop_receipt.json"),
];
pub fn inspect_stopped_bundle(
 archive:&Path,paper_root:&Path,receipt:&Path
)->Result<Value,String>{
 let marker=fs::symlink_metadata(archive).map_err(|_|denied("archive absent"))?;
 if !marker.file_type().is_dir()||marker.mode()&0o077!=0{
  return Err(denied("untrusted archive directory"))
 }
 // The archive was copied at the operator-approved seal. Do not treat a
 // freshly self-authored manifest as a rollback witness or broker attestation.
 let manifest=json_file(&archive.join("manifest.json"),65_536)?;
 if manifest["schema"]!="SPOTARB/R1061/SEALED_STOPPED_MODEL_EVIDENCE/V1"||
    manifest["source_checkpoint_unchanged"]!=true||
    manifest["snapshot_not_real_fill_certification"]!=true||
    manifest["stopped_original_service_disabled"]!=true||
    manifest["same_server_only"]!=true{
  return Err(denied("untrusted R1061 archive scope"))
 }
 let name=manifest["session"].as_str().ok_or_else(||denied("session"))?;
 if !name.starts_with("session_")||name.len()>100||
    !name.bytes().all(|c|c.is_ascii_alphanumeric()||c==b'_'){
  return Err(denied("unsafe session identity"))
 }
 let records=manifest["files"].as_object().ok_or_else(||denied("archive manifest files"))?;
 if records.len()!=FILES.len()||records.len()>MAX_ARCHIVE_FILES{
  return Err(denied("archive missing/extra manifest entries"))
 }
 let stop:Value=serde_json::from_slice(&receipt_bytes(receipt)?)
     .map_err(|_|denied("malformed root operator receipt"))?;
 if stop["schema"]!="SPOTARB/R1059/GRACEFUL-STOP/OPERATOR-V1"||
    stop["session"]!=name||stop["service_active_state"]!="inactive"||
    stop["handoff_check_result"]!="PASS"||
    stop["headless_restart_disabled"]!=true||
    stop["real_kill_engaged"]!=true||stop["real_trading"]!=false{
   return Err(denied("operator stop receipt invalid"))
 }
 let session_root=paper_root.join("sessions").join(name);
 let mut verified=serde_json::Map::new();
 for (saved,live_name) in FILES{
  let record=records.get(saved).ok_or_else(||denied("missing manifest file entry"))?;
  let bytes=safe_bytes(&archive.join(saved),5_000_000)?;
  let recorded=exact_sha(&record["sha256"])?;
  if bytes.len() as u64!=record["size_bytes"].as_u64().unwrap_or(u64::MAX)||
     hex_sha(&bytes)!=recorded{
    return Err(denied("archived file mutated since manifest"))
  }
  let current_path=match saved{
   "operator_stop_receipt.json"=>receipt.to_path_buf(),
   "portfolio_checkpoint.json"|"planned_handoff.json"=>paper_root.join(live_name),
   _=>session_root.join(live_name)
  };
  let current=if saved=="operator_stop_receipt.json"{
    receipt_bytes(&current_path)?
  }else{safe_bytes(&current_path,5_000_000)?};
  if bytes!=current{return Err(denied("live source differs from stopped archive"))}
  verified.insert(saved.to_owned(),Value::String(recorded.to_owned()));
 }
 if paper_root.join("active.json").exists(){
  return Err(denied("active episode exists; cannot inspect stopped archive"))
 }
 let current=json_file(&paper_root.join("current_session.json"),16_384)?;
 if current["session"]!=name||current["paper_only"]!=true||
    current["virtual_positions_not_exchange_attested"]!=true{
  return Err(denied("current session mismatch"))
 }
 let plan=json_file(&paper_root.join("planned_handoff.json"),8192)?;
 let entry=plan["entries"].as_u64().ok_or_else(||denied("source entries"))?;
 let day=plan["day"].as_i64().ok_or_else(||denied("source day"))?;
 if plan["validated_seal"]!=true||plan["last_session"]!=name||
    plan["no_real_orders"]!=true{
  return Err(denied("planned handoff is not sealed"))
 }
 let summary=json_file(&archive.join("session_summary.json"),16_384)?;
 let final_result=json_file(&archive.join("session_final.json"),256_000)?;
 if summary["session"]!=name||summary["handoff_check_result"]!="PASS"||
    summary["next_session_can_restore_virtual_positions"]!=true||
    summary["checkpoint_four_leg_cross_file_match"]!=true||
    summary["modeled_four_leg_audit_verified"]!=true||
    summary["killed_for_safety"]!=false||
    final_result["virtual_paper_continuity"]["checkpoint_completed"]!=true||
    final_result["trading"]!=false||
    final_result["private_order_path_linked"]!=false{
  return Err(denied("final proof or checkpoint/journal gate rejected"))
 }
 let count=final_result["paper_portfolio"]["open_positions"].as_u64()
   .ok_or_else(||denied("open count"))?;
 let closed=final_result["paper_portfolio"]["counters"]["closed"].as_u64()
   .ok_or_else(||denied("closed count"))?;
 if stop["four_leg_open_positions"].as_u64()!=Some(count)||
    final_result["paper_portfolio"]["counters"]["entries"].as_u64()!=Some(entry)||
    manifest["sealed_entries"].as_u64()!=Some(entry)||
    manifest["sealed_closed"].as_u64()!=Some(closed)||
    manifest["open_bases"].as_array().map(|x|x.len() as u64)!=Some(count)||
    count+closed!=entry{
  return Err(denied("portfolio economics/counts diverged"))
 }
 let seal_sha=exact_sha(&stop["checkpoint_sha256_after"])?;
 if verified.get("portfolio_checkpoint.json").and_then(Value::as_str)!=Some(seal_sha)||
   verified.get("session_checkpoint_at_exit.json").and_then(Value::as_str)!=Some(seal_sha){
  return Err(denied("stopped checkpoint does not match operator receipt"))
 }
 let cp=archive.join("portfolio_checkpoint.json");
 inspect_planned_seal(&cp,day,entry)
   .map_err(|_|denied("Rust verified sealed checkpoint refused"))?;
 inspect_checkpoint_model_alignment(
     &cp,&archive.join("session_four_leg_journal.jsonl"),name,true)
   .map_err(|_|denied("Rust full original four-leg after-images mismatch"))?;
 let now=SystemTime::now().duration_since(UNIX_EPOCH)
   .map_err(|_|denied("clock"))?.as_millis() as u64;
 let envelope=json_file(&cp,262_144)?;
 let updated=envelope["payload"]["updated_ms"].as_u64()
    .ok_or_else(||denied("checkpoint timestamp"))?;
 let age=now.checked_sub(updated).ok_or_else(||denied("future checkpoint"))?;
 let portfolio=&envelope["payload"]["portfolio"];
 let mut bases=portfolio["positions"].as_object().ok_or_else(||denied("positions"))?
  .keys().cloned().collect::<Vec<_>>();
 bases.sort();
 let mut reported=manifest["open_bases"].as_array().ok_or_else(||denied("bases"))?
  .iter().map(|x|x.as_str().unwrap_or("").to_string()).collect::<Vec<_>>();
 reported.sort();
 if bases!=reported||bases.len() as u64!=count{
  return Err(denied("exact open base identity mismatch"))
 }
 Ok(json!({
  "schema":"SPOTARB/R1062/SEALED-STOPPED-COLD-ARCHIVE-AUDIT/V1",
  "ok":true,"session":name,"open_bases":bases,"modeled_entries":entry,
  "modeled_closed":closed,"modeled_open":count,
  "checkpoint_age_ms":age,"legacy_fresh_180s":age<=180_000,
  "frozen_source_bytes_identical":true,"original_journal_exact_phase_parity":true,
  "verified_files":verified.len(),
  "auto_resume_authorized":false,
  "installed_paper_modified":false,"real_orders_authorized":false,
  "warning":"LOCAL MODELED EVIDENCE ONLY; neither expired restart nor real exchange fills certified"
 }))
}

#[cfg(test)]
mod r1062_private_negative_tests{
 use super::*;
 use std::os::unix::fs::PermissionsExt;
 fn tmp()->std::path::PathBuf{
  let n=SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
  let p=std::env::temp_dir().join(format!("r1062_attested_negative_{}_{}",std::process::id(),n));
  fs::create_dir(&p).unwrap();fs::set_permissions(&p,fs::Permissions::from_mode(0o700)).unwrap();
  p
 }
 #[test]fn r1062_archive_missing_is_fail_closed(){
  let p=tmp();
  assert!(inspect_stopped_bundle(&p,&p,&p.join("missing")).is_err());
  fs::remove_dir_all(p).unwrap();
 }
 #[test]fn r1062_refuse_broken_sha_and_non_sha(){
  let value=json!("0".repeat(64));
  assert_eq!(exact_sha(&value),Ok("0000000000000000000000000000000000000000000000000000000000000000"));
  assert!(exact_sha(&json!("f".repeat(63))).is_err());
  assert!(exact_sha(&json!("z".repeat(64))).is_err());
 }
 #[test]fn r1062_reject_symlink_and_world_readable_archived_evidence(){
  let dir=tmp();
  let file=dir.join("payload.json");
  fs::write(&file,b"{}").unwrap();
  fs::set_permissions(&file,fs::Permissions::from_mode(0o600)).unwrap();
  assert_eq!(safe_bytes(&file,100),Ok(b"{}".to_vec()));
  fs::set_permissions(&file,fs::Permissions::from_mode(0o644)).unwrap();
  assert!(safe_bytes(&file,100).is_err());
  fs::set_permissions(&file,fs::Permissions::from_mode(0o600)).unwrap();
  std::os::unix::fs::symlink(&file,dir.join("alias.json")).unwrap();
  assert!(safe_bytes(&dir.join("alias.json"),100).is_err());
  fs::remove_dir_all(dir).unwrap();
 }
}

#[cfg(test)]
mod r1065_actual_stopped_bundle_adversarial_tests{
 use super::*;
 use std::os::unix::fs::PermissionsExt;
 #[test]
 #[ignore = "R1065 explicit private clone of actual stopped PAPER evidence, never source mutation"]
 fn r1065_tamper_each_archived_artifact_must_deny(){
  assert_eq!(std::env::var("ASTRA_R1065_STOP_ARCHIVE_CLONE").as_deref(),Ok("YES"));
  let live=Path::new("/mnt/spotarb_db_test/astra_paper_continuous_v1");
  let receipt=Path::new("/mnt/spotarb_db_test/astra_collector_original_shadow_v1/privileged_stop_receipt.json");
  let original=Path::new("/mnt/spotarb_db_test/astra_p001_r874_test_scratch_20261025/r1061_sealed_stopped_paper_20261025");
  let manifest=json_file(&original.join("manifest.json"),65_536).unwrap();
  let stamp=SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
  let dir=Path::new("/mnt/spotarb_db_test/astra_p001_r874_test_scratch_20261025")
    .join(format!("r1065_archive_negative_{}_{}",std::process::id(),stamp));
  fs::create_dir(&dir).unwrap();
  fs::set_permissions(&dir,fs::Permissions::from_mode(0o700)).unwrap();
  struct Owned(std::path::PathBuf);
  impl Drop for Owned{fn drop(&mut self){let _=fs::remove_dir_all(&self.0);}}
  let _guard=Owned(dir.clone());
  fs::copy(original.join("manifest.json"),dir.join("manifest.json")).unwrap();
  for (file,_) in FILES{
   fs::copy(original.join(file),dir.join(file)).unwrap();
  }
  assert!(inspect_stopped_bundle(&dir,live,receipt).is_ok(),
    "private byte-identical clone must independently pass");
  for (file,_) in FILES{
   let p=dir.join(file);
   let clean=fs::read(&p).unwrap();
   let mut altered=clean.clone();
   let last=altered.last_mut().unwrap();
   *last^=1;
   fs::write(&p,&altered).unwrap();
   assert!(inspect_stopped_bundle(&dir,live,receipt).is_err(),
     "R1065 mutated archived {file} should fail");
   fs::write(&p,clean).unwrap();
  }
  let p=dir.join("manifest.json");
  let raw=fs::read(&p).unwrap();
  let mut fake=manifest;
  fake["session"]=serde_json::json!("session_999999_999999");
  fs::write(&p,serde_json::to_vec(&fake).unwrap()).unwrap();
  assert!(inspect_stopped_bundle(&dir,live,receipt).is_err(),
    "forged session ID must fail");
  fs::write(&p,raw).unwrap();
  assert!(inspect_stopped_bundle(&dir,live,receipt).is_ok());
 }
}
