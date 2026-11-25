//! R1063 OFFLINE-only: in-memory PAPER cold portfolio reconstruction.
//! No live engine, new entries, exchange credentials or source mutation.
use spotarb_native_market_core::{
 native_paper_continuity_checkpoint_review::r1063_stopped_extended_preview,
 transport::monotonic_ns,
};
use std::{path::Path,process::{Command,exit}};
use serde_json::json;
const PAPER:&str="/mnt/spotarb_db_test/astra_paper_continuous_v1";
const ARCHIVE:&str="/mnt/spotarb_db_test/astra_p001_r874_test_scratch_20261025/r1061_sealed_stopped_paper_20261025";
const RECEIPT:&str="/mnt/spotarb_db_test/astra_collector_original_shadow_v1/privileged_stop_receipt.json";
fn unit(key:&str)->bool{
 let p=Command::new("/usr/bin/systemctl")
  .args(["show","spotarb-native-paper-continuous.service","-p",key])
  .output().unwrap();
 p.status.success()&&p.stdout==format!("{key}={}\n",match key{
 "MainPID"=>"0","ActiveState"=>"inactive","UnitFileState"=>"disabled",_=>""}).as_bytes()
}
fn main(){
 if std::env::args_os().len()!=1||!unit("MainPID")||
    !unit("ActiveState")||!unit("UnitFileState"){
    eprintln!("R1063 PREVIEW DENIED: stopped/disabled PAPER required");exit(70)
 }
 let checkpoint=Path::new(PAPER).join("portfolio_checkpoint.json");
 match r1063_stopped_extended_preview(&checkpoint,Path::new(PAPER),
     Path::new(ARCHIVE),Path::new(RECEIPT),monotonic_ns(),37){
  Ok(p)=>{
   let mut bases=p.portfolio.positions.keys().cloned().collect::<Vec<_>>();
   bases.sort();
   println!("{}",json!({
    "schema":"SPOTARB/R1063/IN_MEMORY_STOPPED_PORTFOLIO_PREVIEW/V1",
    "ok":true,"candidate_only":true,"live_restart_authorized":false,
    "real_exchange_orders":false,"paper_order_actions":false,
    "preserved_revision":p.previous_revision,
    "cold_boot_unknown":p.portfolio.cold_boot_unknown,
    "fresh_new_entries_allowed":false,
    "modeled_entries":p.portfolio.stats.entries,
    "modeled_closed":p.portfolio.stats.closed,
    "open_bases":bases,"open_count":p.carried_open_positions,
    "same_boot_only":true,"same_day_only":true,
    "temporary_max_age_hours":12,
   }))
  }
  Err(e)=>{eprintln!("R1063 PREVIEW DENIED {e:?}");exit(70)}
 }
}
