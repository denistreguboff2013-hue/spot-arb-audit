//! R1084: one Rust PAPER canary process owns BOTH durable source and
//! modeled portfolio per episode. Normal same-day stop / fresh OS PID start.
//! No exchanges, no live installed PAPER, no clock overrides.
use spotarb_native_market_core::{
 native_isolated_sqlite_mirror_r147_review::{
  r1081_private_source_first_half,r1081_private_source_resume
 },
 native_paper_continuity_checkpoint_review::{
  save_checkpoint,load_planned_handoff,inspect_checkpoint_model_alignment
 },
 native_paper_portfolio_review::PaperPortfolio,
 native_paper_market_execution_review::{PaperAssumptions,PaperMachine,PaperPhase,PaperPosition},
 native_virtual_four_leg_provenance_review::ModeledFourLegJournal,
 transport::monotonic_ns,
};
use std::{env,fs,path::Path,process::{self,Command},io::Write};
use serde_json::{json,Value};
use openssl::sha::sha256;

fn deny(s:&str)->!{eprintln!("R1084 PRIVATE UNIFIED DENIED: {s}");process::exit(70)}
fn hash(path:&Path)->String{
 sha256(&fs::read(path).unwrap_or_else(|_|deny("cannot hash source")))
  .iter().map(|x|format!("{x:02x}")).collect()
}
fn is_offline(){
 let unit=Command::new("/usr/bin/systemctl")
   .args(["show","spotarb-native-paper-continuous.service",
      "-p","ActiveState","-p","MainPID","-p","UnitFileState"])
   .output().unwrap_or_else(|_|deny("systemd query failed"));
 let state=String::from_utf8_lossy(&unit.stdout);
 if !unit.status.success()||
  !state.lines().any(|x|x=="ActiveState=inactive")||
  !state.lines().any(|x|x=="MainPID=0")||
  !state.lines().any(|x|x=="UnitFileState=disabled") {
  deny("installed PAPER service not securely stopped")
 }
 let kill:Value=serde_json::from_slice(
   &fs::read("/home/tmpadmin/spotbot/data/real_kill_switch.json")
    .unwrap_or_else(|_|deny("REAL kill unreadable")))
   .unwrap_or_else(|_|deny("REAL kill malformed"));
 if kill["state"]!="ENGAGED"{deny("REAL kill must stay ENGAGED")}
}
fn fixture()->PaperPortfolio{
 let now=std::time::SystemTime::now().duration_since(
    std::time::UNIX_EPOCH).unwrap().as_millis() as i64;
 let mut p=PaperPortfolio::new(now).unwrap_or_else(|_|deny("portfolio new"));
 p.stats.entries=37;
 p.stats.closed=34;
 p.stats.closed_wins=27;
 p.stats.closed_losses=7;
 p.stats.futures_hedges=37;
 p.stats.futures_covers=34;
 p.stats.modeled_realized_usdt=1.6578974787089982;
 let ns=monotonic_ns().checked_sub(20_000_000)
  .unwrap_or_else(||deny("insufficient monotonic uptime"));
 for (i,base) in ["DOS","PHA","WLD"].iter().enumerate(){
  let mut m=PaperMachine::new(PaperAssumptions::default())
    .unwrap_or_else(|_|deny("model machine"));
  let t=ns+(i as u64)*3;
  m.phase=PaperPhase::Hedged(PaperPosition{
   base:base.to_string(),signal_ns:t,spot_buy_ns:t+1,
   signal_estimated_net_bps:60.0,spot_gross_base:1.0,
   spot_delivered_base:1.0,spot_paid_quote:10.0,
   futures_short_ns:Some(t+2),
   futures_short_proceeds_quote:Some(10.2),
   futures_short_base:Some(1.0),
   hedge_entry_estimated_net_bps:Some(60.0)
  });
  p.positions.insert(base.to_string(),m);
 }
 p
}
fn atomic_receipt(path:&Path,record:&Value){
 let body=serde_json::to_vec(record).unwrap_or_else(|_|deny("receipt encode"));
 let mut f=fs::OpenOptions::new().write(true).create_new(true).open(path)
    .unwrap_or_else(|_|deny("joint receipt exists or inaccessible"));
 f.write_all(&body).unwrap_or_else(|_|deny("receipt write"));
 f.sync_all().unwrap_or_else(|_|deny("receipt sync"));
 let directory=fs::File::open(path.parent().unwrap())
    .unwrap_or_else(|_|deny("receipt parent inaccessible"));
 directory.sync_all().unwrap_or_else(|_|deny("receipt directory sync"));
}
fn main(){
 let args=env::args().collect::<Vec<_>>();
 if args.len()!=3 || env::var("ASTRA_R1084_PRIVATE_ONLY").as_deref()!=Ok("YES"){
  deny("private opt-in, --stop|--restart, root required")
 }
 let root=Path::new(&args[2]);
 let root_str=root.to_string_lossy();
 if !root_str.starts_with(
   "/mnt/spotarb_db_test/astra_p001_r874_test_scratch_20261025/astra_r671_file_r1084_"){
   deny("R1084 private R671 root required")
 }
 let md=fs::symlink_metadata(root).unwrap_or_else(|_|deny("root missing"));
 use std::os::unix::fs::{MetadataExt,PermissionsExt};
 if !md.file_type().is_dir()||md.permissions().mode()&0o077!=0||
   md.uid()!=unsafe{libc::geteuid()} {deny("private root unsafe")}
 is_offline();
 let checkpoint=root.join("virtual_checkpoint.json");
 let journal=root.join("virtual_four_leg.jsonl");
 let receipt=root.join("joint_stop_receipt.json");
 let session="session_1084_private";
 match args[1].as_str(){
 "--stop"=>{
  if checkpoint.exists()||journal.exists()||receipt.exists(){
   deny("private run used before")
  }
  // Owner and modeled PaperPortfolio are owned by THIS SINGLE OS
  // process; DB2/ACK worker remains independent for backpressure.
  let src=r1081_private_source_first_half(root,1024)
   .unwrap_or_else(|e|deny(&format!("durable source prefix failure: {e}")));
  if src["accepted"]!=512||src["durable_transactions_this_pid"]!=16||
    src["finalized"]!=false{deny("first 512 public events not durable")}
  let p=fixture();
  let mut modeled=ModeledFourLegJournal::start(&journal,session,&p,0)
   .unwrap_or_else(|_|deny("new modeled journal"));
  save_checkpoint(&checkpoint,&p,1,monotonic_ns(),false)
    .unwrap_or_else(|_|deny("checkpoint provisional"));
  save_checkpoint(&checkpoint,&p,2,monotonic_ns(),true)
    .unwrap_or_else(|_|deny("checkpoint sealed"));
  modeled.seal(&p).unwrap_or_else(|_|deny("modeled journal seal"));
  inspect_checkpoint_model_alignment(&checkpoint,&journal,session,true)
    .unwrap_or_else(|_|deny("source stop checkpoint/journal mismatch"));
  let record=json!({
   "schema":"SPOTARB/R1084/PRIVATE-JOINT-STOP/V1",
   "source_run":"r1069_paper_batch","source_transactions":16,
   "source_physical_events":512,"checkpoint_revision":2,
   "checkpoint_sha256":hash(&checkpoint),"journal_sha256":hash(&journal),
   "modeled_entries":37,"modeled_closed":34,"modeled_open":3,
   "no_real_orders":true,"source_synthetic_public_only":true,
   "joint_storage_transaction_atomic":false,
   "next_run_requires_exact_manifest":true
  });
  atomic_receipt(&receipt,&record);
  println!("{}",json!({"ok":true,"phase":"STOP","source_events":512,
    "source_transactions":16,"portfolio_open":3,"entries":37,
    "closed":34,"joint_receipt":true,"real_orders":false}));
 },
 "--restart"=>{
  // Verify BOTH original sources before Owner accepts its next
  // update. This only certifies the fresh sealed STOP boundary.
  let record:Value=serde_json::from_slice(&fs::read(&receipt)
    .unwrap_or_else(|_|deny("joint stop receipt absent")))
    .unwrap_or_else(|_|deny("joint receipt malformed"));
  if record["schema"]!="SPOTARB/R1084/PRIVATE-JOINT-STOP/V1"||
    record["source_run"]!="r1069_paper_batch"||
    record["source_transactions"]!=16||
    record["source_physical_events"]!=512||
    record["checkpoint_revision"]!=2||
    record["checkpoint_sha256"]!=hash(&checkpoint)||
    record["journal_sha256"]!=hash(&journal)||
    record["modeled_entries"]!=37||record["modeled_closed"]!=34||
    record["modeled_open"]!=3||record["no_real_orders"]!=true{
    deny("joint stop receipt/portfolio/journal mismatch")
  }
  inspect_checkpoint_model_alignment(&checkpoint,&journal,session,true)
   .unwrap_or_else(|_|deny("joint stopped journal invalid"));
  let original_ckpt_sha=hash(&checkpoint);
  let original_journal_sha=hash(&journal);
  let restored=load_planned_handoff(&checkpoint,monotonic_ns(),37)
   .unwrap_or_else(|_|deny("fresh normal PAPER handoff refused"));
  if restored.carried_open_positions!=3||restored.previous_revision!=2||
   restored.previous_day!=restored.portfolio.day_yek||
   restored.portfolio.stats.entries!=37||restored.portfolio.stats.closed!=34||
   !restored.portfolio.cold_boot_unknown||
   restored.portfolio.positions.keys().map(String::as_str)
    .collect::<Vec<_>>()!=vec!["DOS","PHA","WLD"]{
    deny("same-session virtual model not exactly restored")
  }
  // Existing R834 recovers Owner from all 512 committed DB1 events
  // BEFORE the second 512 can be accepted in the same Rust process.
  let src=r1081_private_source_resume(root,1024)
    .unwrap_or_else(|e|deny(&format!("source owner replay/restart denied: {e}")));
  if src["recovered_previous_events"]!=512||
    src["accepted"]!=512||src["durable_transactions_this_pid"]!=16||
    src["finalized"]!=true{deny("source R834 recovery mismatch")}
  if hash(&checkpoint)!=original_ckpt_sha||hash(&journal)!=original_journal_sha{
    deny("PAPER modeled state modified during source restart")
  }
  inspect_checkpoint_model_alignment(&checkpoint,&journal,session,true)
    .unwrap_or_else(|_|deny("modeled journal altered during collector resume"));
  is_offline();
  println!("{}",json!({
   "ok":true,"phase":"RESTART","original_source_replayed":512,
   "new_source_events":512,"total_private_source_events":1024,
   "durable_batches_total":32,"virtual_positions_restored":3,
   "model_checkpoint_unchanged":true,"modeled_journal_unchanged":true,
   "cold_new_entries_denied":true,
   "real_orders":false,"no_installed_paper":true
  }));
 },
 _=>deny("unknown stage")
 }
}
