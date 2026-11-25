//! R1086: exact original model-only custody copy, no resume/cutover.
/// 3 old virtual positions are QUARANTINED, not tradable or flat.
use spotarb_native_market_core::{
 native_r1062_sealed_stop_bundle_review::inspect_stopped_bundle,
 native_paper_continuity_checkpoint_review::{
  inspect_checkpoint_model_alignment,inspect_planned_seal},
 native_paper_portfolio_review::PaperPortfolio,
};
use serde_json::{Value,json};
use openssl::sha::sha256;
use std::{env,fs::{self,File,OpenOptions},path::Path,io::Write,process::{self,Command}};
use std::os::unix::fs::{MetadataExt,OpenOptionsExt};
const PAPER:&str="/mnt/spotarb_db_test/astra_paper_continuous_v1";
const ARCHIVE:&str="/mnt/spotarb_db_test/astra_p001_r874_test_scratch_20261025/r1061_sealed_stopped_paper_20261025";
const RECEIPT:&str="/mnt/spotarb_db_test/astra_collector_original_shadow_v1/privileged_stop_receipt.json";
const ROOT_PREFIX:&str="/mnt/spotarb_db_test/astra_p001_r874_test_scratch_20261025/astra_r1086_custody_";
const EXPECTED:&str="423ba8e64e003bc36e73bf5fda4313d3d9ac8b781ecdfb82761d0e9cd7a9f6f9";
fn deny(s:&str)->!{eprintln!("R1086 DENIED: {s}");process::exit(70)}
fn sha(data:&[u8])->String{
 sha256(data).iter().map(|x|format!("{x:02x}")).collect()
}
fn read(path:&Path)->Vec<u8>{
 fs::read(path).unwrap_or_else(|_|deny("input inaccessible"))
}
fn write_once(path:&Path,data:&[u8]){
 let mut f=OpenOptions::new().create_new(true).write(true).mode(0o600)
    .open(path).unwrap_or_else(|_|deny("custody file already exists"));
 f.write_all(data).unwrap_or_else(|_|deny("copy write failed"));
 f.sync_all().unwrap_or_else(|_|deny("copy fsync failed"));
}
fn safe_root(root:&Path){
 let s=root.to_string_lossy();
 if !s.starts_with(ROOT_PREFIX)||s.len()>180||
    root.parent()!=Some(Path::new("/mnt/spotarb_db_test/astra_p001_r874_test_scratch_20261025")){
    deny("custody root path refused")
 }
 let meta=fs::symlink_metadata(root).unwrap_or_else(|_|deny("root missing"));
 if !meta.file_type().is_dir()||meta.mode()&0o077!=0||
    meta.uid()!=unsafe{libc::geteuid()}{
     deny("unsafe custody root permissions")
 }
}
fn original_evidence()->Value{
 let state=Command::new("/usr/bin/systemctl").args([
   "show","spotarb-native-paper-continuous.service",
   "-p","ActiveState","-p","UnitFileState","-p","MainPID"])
   .output().unwrap_or_else(|_|deny("PAPER systemd unavailable"));
 let text=String::from_utf8_lossy(&state.stdout);
 if !state.status.success()||
    !text.lines().any(|x|x=="ActiveState=inactive")||
    !text.lines().any(|x|x=="UnitFileState=disabled")||
    !text.lines().any(|x|x=="MainPID=0"){
    deny("PAPER installed service must stay stopped")
 }
 let a=Path::new(ARCHIVE);let p=Path::new(PAPER);let r=Path::new(RECEIPT);
 let v=inspect_stopped_bundle(a,p,r)
   .unwrap_or_else(|e|deny(&format!("old stopped R1062 archive: {e}")));
 if v["ok"]!=true||v["modeled_entries"]!=37||
    v["modeled_closed"]!=34||v["modeled_open"]!=3||
    v["verified_files"]!=7||
    v["auto_resume_authorized"]!=false||
    v["original_journal_exact_phase_parity"]!=true||
    v["real_orders_authorized"]!=false||
    v["legacy_fresh_180s"]!=false{
    deny("original operator seal or old stale status differs")
 }
 v
}
fn checked_model(c:&Path,j:&Path,session:&str)->PaperPortfolio{
 let bytes=read(c);
 if sha(&bytes)!=EXPECTED{deny("stopped checkpoint sha changed")}
 let v:Value=serde_json::from_slice(&bytes)
  .unwrap_or_else(|_|deny("checkpoint JSON invalid"));
 let p:PaperPortfolio=serde_json::from_value(v["payload"]["portfolio"].clone())
  .unwrap_or_else(|_|deny("typed modeled portfolio invalid"));
 if p.stats.entries!=37||p.stats.closed!=34||
    p.stats.closed_wins!=27||p.stats.closed_losses!=7||
    p.stats.modeled_realized_usdt.to_bits()!=1.6578974787089982f64.to_bits()||
    !p.cold_boot_unknown||p.positions.len()!=3||
    p.positions.keys().map(String::as_str).collect::<Vec<_>>()
      !=vec!["DOS","PHA","WLD"]{
    deny("exact original portfolio identity/counters fail")
 }
 let day=v["payload"]["portfolio"]["day_yek"].as_i64()
  .unwrap_or_else(||deny("original day absent"));
 inspect_planned_seal(c,day,37)
   .unwrap_or_else(|_|deny("original stopped seal invalid"));
 inspect_checkpoint_model_alignment(c,j,session,true)
   .unwrap_or_else(|_|deny("original checkpoint/journal phase after-images diverge"));
 p
}
fn main(){
 let args=env::args().collect::<Vec<_>>();
 if args.len()!=3||
    env::var("ASTRA_R1086_READONLY_CUSTODY").as_deref()!=Ok("YES")||
    !["--snapshot","--verify"].contains(&args[1].as_str()){
    deny("private opt-in and --snapshot or --verify required")
 }
 let root=Path::new(&args[2]);safe_root(root);
 let kill:Value=serde_json::from_slice(&read(Path::new(
    "/home/tmpadmin/spotbot/data/real_kill_switch.json")))
    .unwrap_or_else(|_|deny("kill switch invalid"));
 if kill["state"]!="ENGAGED"{deny("REAL kill switch disengaged")}
 let src=original_evidence();
 let session=src["session"].as_str().unwrap_or_else(||deny("session missing"));
 let a=Path::new(ARCHIVE);
 let files=[("portfolio_checkpoint.json","sealed_source_checkpoint.json"),
   ("session_four_leg_journal.jsonl","sealed_source_four_leg_journal.jsonl")];
 let frozen=root.join(files[0].1);
 let journal=root.join(files[1].1);
 let original=checked_model(&a.join(files[0].0),&a.join(files[1].0),session);
 let manifest=root.join("r1086_quarantine_manifest.json");
 if args[1]=="--snapshot"{
   if manifest.exists()||frozen.exists()||journal.exists(){
     deny("quarantine snapshot cannot overwrite an earlier run")
   }
   for (from,to) in files{
    write_once(&root.join(to),&read(&a.join(from)))
   }
   let proof=json!({
    "schema":"SPOTARB/R1086/ORIGINAL-THREE-POSITION-QUARANTINE/V1",
    "source_session":session,
    "checkpoint_sha256":sha(&read(&frozen)),
    "journal_sha256":sha(&read(&journal)),
    "original_day":original.day_yek,
    "original_entries":37,"original_closed":34,"original_open":3,
    "original_closed_wins":27,"original_closed_losses":7,
    "modeled_realized_usdt":original.stats.modeled_realized_usdt,
    "cold_boot_new_entries_denied":true,
    "original_positions_preserved":true,"positions_auto_resumed":false,
    "may_trade_real":false,"may_start_paper":false,
    "cross_store_cash_funding_reconciled":false,
    "source_files_timestamp_unchanged":true
   });
   let content=serde_json::to_vec_pretty(&proof)
      .unwrap_or_else(|_|deny("manifest encoding failed"));
   write_once(&manifest,&content);
   File::open(root).unwrap_or_else(|_|deny("root fsync open"))
      .sync_all().unwrap_or_else(|_|deny("root fsync failed"));
 }
 let proof:Value=serde_json::from_slice(&read(&manifest))
   .unwrap_or_else(|_|deny("custody manifest invalid"));
 if proof["schema"]!="SPOTARB/R1086/ORIGINAL-THREE-POSITION-QUARANTINE/V1"||
    proof["source_session"]!=session||proof["checkpoint_sha256"]!=EXPECTED||
    proof["checkpoint_sha256"]!=sha(&read(&frozen))||
    proof["journal_sha256"]!=sha(&read(&journal))||
    proof["original_entries"]!=37||proof["original_closed"]!=34||
    proof["original_open"]!=3||proof["may_start_paper"]!=false||
    proof["may_trade_real"]!=false||
    proof["source_files_timestamp_unchanged"]!=true{
    deny("quarantine manifest and source digest disagree")
 }
 let copied=checked_model(&frozen,&journal,session);
 if serde_json::to_value(&copied).unwrap_or_else(|_|deny("model encode"))!=
    serde_json::to_value(&original).unwrap_or_else(|_|deny("old encode")){
    deny("typed copied modeled portfolio differs")
 }
 if read(&frozen)!=read(&a.join(files[0].0))||
    read(&journal)!=read(&a.join(files[1].0)){
    deny("copy differs from sealed R1061 archive")
 }
 println!("{}",json!({"schema":"SPOTARB/R1086/CUSTODY-VERIFY/V1",
   "ok":true,"mode":args[1],"modeled_bases":["DOS","PHA","WLD"],
   "entries":37,"closed":34,"modeled_open":3,
   "realized_model_usdt":copied.stats.modeled_realized_usdt,
   "independent_archive_files_verified":7,
   "snapshot_sha256":EXPECTED,
   "source_frozen_bytes_equal":true,"full_journal_phase_parity":true,
   "fresh_restart_authorized":false,"paper_orders":0,"real_orders":0,
   "original_archive_modified":false}));
}
