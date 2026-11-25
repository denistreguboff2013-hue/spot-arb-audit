//! R1092 cloned DB benchmark. Never read-write original live PAPER.
use std::{env,path::Path,process::{self,Command},fs,time::Instant};
use spotarb_native_market_core::native_isolated_sqlite_mirror_r147_review::r865_private_fast_ack_drain_once;
fn deny(s:&str)->!{eprintln!("R1092 refused: {s}");process::exit(70)}
fn main(){
 let a=env::args().collect::<Vec<_>>();
 if a.len()!=2 || env::var("ASTRA_R1092_FROZEN_ACK_PROFILE").as_deref()!=Ok("YES"){
  deny("explicit private clone opt-in required")
 }
 let root=Path::new(&a[1]);
 if root.parent()!=Some(Path::new("/mnt/spotarb_db_test/astra_p001_r874_test_scratch_20261025"))||
   !root.file_name().and_then(|x|x.to_str())
      .is_some_and(|s|s.starts_with("astra_r671_file_r1092_")) {
   deny("research-only R1092 isolated clone path")
 }
 for name in ["r671-private-db1.sqlite","r671-private-db2.sqlite"]{
  if !root.join(name).is_file(){deny("missing cloned source/replica")}
 }
 let service=Command::new("/usr/bin/systemctl")
  .args(["show","spotarb-native-paper-continuous.service",
   "-p","ActiveState","-p","MainPID","-p","UnitFileState"])
  .output().unwrap_or_else(|_|deny("service inaccessible"));
 let s=String::from_utf8_lossy(&service.stdout);
 if !s.contains("ActiveState=inactive")||!s.contains("MainPID=0")||
    !s.contains("UnitFileState=disabled"){deny("installed PAPER is not stopped")}
 let real:serde_json::Value=serde_json::from_slice(
  &fs::read("/home/tmpadmin/spotbot/data/real_kill_switch.json")
  .unwrap_or_else(|_|deny("kill unreadable"))).unwrap_or_else(|_|deny("kill invalid"));
 if real["state"]!="ENGAGED"{deny("REAL must remain blocked")}
 let start=Instant::now();
 let result=r865_private_fast_ack_drain_once(root)
    .unwrap_or_else(|e|deny(&format!("cloned DB1/DB2 integrity refused: {e}")));
 println!("{}",serde_json::json!({
   "schema":"SPOTARB/R1092/FROZEN-EXACT-ACK-CLONE-BENCH/V1",
   "ok":true,"copied":result.0,"acknowledged":result.1,
   "retained":result.2,"elapsed_ms":start.elapsed().as_secs_f64()*1000.0,
   "research_cloned_files_only":true,"real_orders":0}));
}
