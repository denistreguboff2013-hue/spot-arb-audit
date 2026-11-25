//! R1114: private offline bridge from a VERIFIED injected R1114
//! fail-closed quarantine to the preexisting R37 durable PAPER
//! notification format. Never sends Telegram; never claims live fills.
use crate::{native_r1113_offline_supervisor_join::private_root_name,
 native_paper_notice_queue_review::{
     advance_native_paper_watch,PaperWatchState
 },
 native_paper_telegram_observer::PaperPoint,
 native_trading_config::NativeTradingConfig,
 native_virtual_four_leg_provenance_review::inspect_model_journal,
};
use serde_json::{json,Value};
use std::{fs::{self,File,OpenOptions},io::Write,
    os::unix::fs::{MetadataExt,OpenOptionsExt,PermissionsExt},path::Path,
    time::{SystemTime,UNIX_EPOCH}};
const SOURCE:&str="/home/tmpadmin/native_rust_registrar_review_20261002/c18_floor_checkpoint_20261006/review/native_market_core/config/native_trading_paper_only_r1076.yaml";
const ORIGINAL:&str="423ba8e64e003bc36e73bf5fda4313d3d9ac8b781ecdfb82761d0e9cd7a9f6f9";
fn sha(p:&Path)->Result<String,String>{
 let content=fs::read(p).map_err(|_|"R1114 SHA source unavailable")?;
 Ok(openssl::sha::sha256(&content).iter()
   .map(|v|format!("{v:02x}")).collect())
}
pub fn quarantined_fault_to_local_notice(root:&Path)->Result<Value,String>{
 if std::env::var("ASTRA_R1114_TELEGRAM_OFFLINE").ok().as_deref()!=Some("YES")||
    std::env::var("ASTRA_R1113_OFFLINE_SUPERVISOR").ok().as_deref()!=Some("YES"){
    return Err("R1114 private no-Telegram opt-in denied".into())
 }
 if !private_root_name(root){return Err("R1114 non-private root denied".into())}
 let meta=fs::symlink_metadata(root).map_err(|_|"R1114 root missing")?;
 if !meta.file_type().is_dir()||meta.uid()!=unsafe{libc::geteuid()}||
    meta.permissions().mode()&0o077!=0{
    return Err("R1114 root safety denied".into())
 }
 let paper=CommandGate::checked()?;
 if sha(Path::new("/mnt/spotarb_db_test/astra_paper_continuous_v1/portfolio_checkpoint.json"))? !=ORIGINAL{
    return Err("R1114 original PAPER portfolio custody denied".into())
 }
 let quarantine:Value=serde_json::from_slice(
    &fs::read(root.join("r1114_failclosed_hold.json"))
     .map_err(|_|"R1114 hold absent")?)
    .map_err(|_|"R1114 hold malformed")?;
 if quarantine["schema"]!="SPOTARB/ASTRA/R1114/PRIVATE-INJECTED-STOP/V1"||
    quarantine["quarantined"]!=true||
    quarantine["real_orders"]!=false||
    quarantine["next_model_entries_authorized"]!=false||
    quarantine["source_db2_ack_certified"]!=false||
    !["AFTER_SEALED_STOP","BEFORE_DB2_ACK"]
     .contains(&quarantine["stage"].as_str().unwrap_or(""))||
    quarantine["old_original_sha256"]!=ORIGINAL{
    return Err("R1114 quarantine integrity rejected".into())
 }
 let ckpt=root.join("virtual_checkpoint.json");
 let journal=root.join("virtual_four_leg.jsonl");
 if quarantine["modeled_checkpoint_sha256"]!=sha(&ckpt)?||
    quarantine["modeled_journal_sha256"]!=sha(&journal)?{
    return Err("R1114 quarantine checkpoint or journal mismatch".into())
 }
 let tip=inspect_model_journal(&journal).map_err(|_|"R1114 journal provenance invalid")?;
 if !tip.sealed||tip.entries!=37||tip.closed!=34||tip.open!=3||
    tip.session!="session_1084_private"{
    return Err("R1114 modeled source does not match fixed private canary".into())
 }
 let cfg=NativeTradingConfig::parse(&fs::read_to_string(SOURCE)
    .map_err(|_|"R1114 source YAML missing")?)
    .map_err(|_|"R1114 source YAML invalid")?;
 if cfg.policy.real_execution_authorized{
    return Err("R1114 source unexpectedly permits REAL".into())
 }
 let now=SystemTime::now().duration_since(UNIX_EPOCH)
    .map_err(|_|"R1114 time invalid")?.as_millis() as u64;
 let day=(now/1000+5*3600)/86400;
 let pnl=f64::from_bits(tip.modeled_realized_bits);
 if !pnl.is_finite()||pnl.abs()>1e5{
    return Err("R1114 untrusted modeled PnL".into())
 }
 // R1114 local fault event IDs must not collide across two
 // independent synthetic failure generations sharing the fixed
 // "session_1084_private" journal fixture. This is a RESEARCH
 // namespace only and is deliberately not an active dispatch session.
 let generation=sha(&root.join("r1114_failclosed_hold.json"))?;
 let study_session=format!("{}_R1114_{}",tip.session,&generation[..12]);
 let first=PaperPoint{
    session:study_session.clone(),day,updated_wall_ms:now,
    entries:tip.entries,closes:tip.closed,open_positions:tip.open as u64,
    faults:0,unknown_unhedged:0,
    modeled_milli_usdt:(pnl*1000.0).round() as i64,
 };
 // Historical events baseline with zero re-sends; ONLY this private
 // injected fault is a new observation. No fabricated trade notice.
 let baseline=advance_native_paper_watch(None,first.clone(),&cfg,false)
    .map_err(|_|"R1114 R37 baseline denied")?;
 if !baseline.baseline_created||!baseline.newly_queued.is_empty(){
    return Err("R1114 forbidden historical Telegram replay".into())
 }
 let mut failure=first.clone();
 failure.updated_wall_ms+=1000;failure.faults=1;
 let fault=advance_native_paper_watch(Some(&baseline.next),
    failure.clone(),&cfg,false)
    .map_err(|_|"R1114 R37 injected fault denied")?;
 if fault.newly_queued.len()!=1||
    !fault.newly_queued[0].starts_with("PFAULT-")||
    fault.telegram_network_called||fault.real_orders_permitted{
    return Err("R1114 expected one local fault notice only".into())
 }
 let bytes=serde_json::to_vec_pretty(&fault.next)
    .map_err(|_|"R1114 R37 queue serialization denied")?;
 let saved:PaperWatchState=serde_json::from_slice(&bytes)
    .map_err(|_|"R1114 R37 queue roundtrip refused")?;
 let retry=advance_native_paper_watch(Some(&saved),failure,&cfg,false)
    .map_err(|_|"R1114 R37 idempotency denied")?;
 if !retry.newly_queued.is_empty()||retry.next.pending.len()!=1||
    retry.telegram_network_called{
    return Err("R1114 duplicate notice after restore".into())
 }
 // Do not append to, or even try to send from, actual R37/R38 files.
 let path=root.join("r1114_private_telegram_queue.json");
 let mut f=OpenOptions::new().write(true).create_new(true)
    .mode(0o600).open(&path)
    .map_err(|_|"R1114 private queue already exists or unsafe")?;
 f.write_all(&bytes).map_err(|_|"R1114 local queue write denied")?;
 f.sync_all().map_err(|_|"R1114 local queue sync denied")?;
 File::open(root).and_then(|f|f.sync_all())
    .map_err(|_|"R1114 queue directory sync denied")?;
 // Re-read persisted bytes to verify replay without a second queue write.
 let persisted:PaperWatchState=serde_json::from_slice(
   &fs::read(&path).map_err(|_|"R1114 queue readback failed")?)
   .map_err(|_|"R1114 queue readback corrupt")?;
 if persisted!=saved ||paper!="STOPPED_REAL_ENGAGED"{
    return Err("R1114 persisted queue custody denied".into())
 }
 Ok(json!({"schema":"SPOTARB/ASTRA/R1114/PRIVATE-QUARANTINE-TELEGRAM-PREVIEW/V1",
    "ok":true,"source":"INJECTED_LOCAL_SOURCE_FAILURE_ONLY",
    "hold_stage":quarantine["stage"],
    "source_model_session":tip.session,
    "offline_research_session":study_session,
    "yaml_sha256":cfg.sha256,
    "queue_sha256":sha(&path)?,
    "queued_event_id":fault.newly_queued[0],
    "queued_count":1,
    "duplicate_events_after_reopen":retry.newly_queued.len(),
    "prior_virtual_trades_replayed":0,
    "telegram_network_calls":0,"real_orders":0,
    "installed_paper_touched":false,
    "installed_telegram_queue_touched":false,
    "telegram_dispatch_authorized":false,
    "real_exchange_fill_attested":false,
 }))
}
struct CommandGate;
impl CommandGate{
 fn checked()->Result<&'static str,String>{
   let status=std::process::Command::new("/usr/bin/systemctl")
     .args(["show","spotarb-native-paper-continuous.service",
            "-p","MainPID","-p","ActiveState","-p","UnitFileState"])
     .output().map_err(|_|"R1114 systemctl unavailable")?;
   let state=String::from_utf8_lossy(&status.stdout);
   if !status.status.success()||
     !state.lines().any(|x|x=="MainPID=0")||
     !state.lines().any(|x|x=="ActiveState=inactive")||
     !state.lines().any(|x|x=="UnitFileState=disabled"){
     return Err("R1114 installed PAPER not stopped".into())
   }
   let kill:Value=serde_json::from_slice(&fs::read(
       "/home/tmpadmin/spotbot/data/real_kill_switch.json")
       .map_err(|_|"R1114 REAL kill unavailable")?)
       .map_err(|_|"R1114 REAL kill malformed")?;
   if kill["state"]!="ENGAGED"{
     return Err("R1114 REAL kill not engaged".into())
   }
   Ok("STOPPED_REAL_ENGAGED")
 }
}
