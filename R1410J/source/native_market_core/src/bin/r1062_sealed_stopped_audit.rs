//! R1062 read-only operator stopped PAPER archive verifier.
//! No command arguments or live state mutation, no network/Telegram.
use spotarb_native_market_core::native_r1062_sealed_stop_bundle_review::inspect_stopped_bundle;
use serde_json::Value;
use std::{fs,path::Path,process::{self,Command}};
const PAPER:&str="/mnt/spotarb_db_test/astra_paper_continuous_v1";
const ARCHIVE:&str="/mnt/spotarb_db_test/astra_p001_r874_test_scratch_20261025/r1061_sealed_stopped_paper_20261025";
const RECEIPT:&str="/mnt/spotarb_db_test/astra_collector_original_shadow_v1/privileged_stop_receipt.json";
fn fail(why:&str)->!{eprintln!("R1062 AUDIT DENIED {why}");process::exit(70)}
fn unit_status(key:&str)->String{
 let result=Command::new("/usr/bin/systemctl")
   .arg("show").arg("spotarb-native-paper-continuous.service")
   .arg("-p").arg(key).output().unwrap_or_else(|_|fail("systemctl unavailable"));
 if !result.status.success(){fail("systemd state unavailable")}
 String::from_utf8_lossy(&result.stdout).trim().to_string()
}
fn main(){
 if std::env::args_os().len()!=1 {fail("arguments not accepted")}
 if unit_status("ActiveState")!="ActiveState=inactive"||
    unit_status("UnitFileState")!="UnitFileState=disabled"||
    unit_status("MainPID")!="MainPID=0"{
  fail("main PAPER service is not stopped and disabled")
 }
 let kill:Value=serde_json::from_slice(&fs::read(
  "/home/tmpadmin/spotbot/data/real_kill_switch.json"
 ).unwrap_or_else(|_|fail("REAL kill unreadable")))
  .unwrap_or_else(|_|fail("REAL kill malformed"));
 if kill["state"]!="ENGAGED"{fail("REAL kill is not engaged")}
 match inspect_stopped_bundle(
   Path::new(ARCHIVE),Path::new(PAPER),Path::new(RECEIPT)){
  Ok(v)=>println!("{v}"),
  Err(e)=>fail(&e)
 }
}
