//! R1087 independent private R671 DB2/ACK worker for bounded real PUBLIC WS.
use spotarb_native_market_core::
 native_isolated_sqlite_mirror_r147_review::r1087_private_public_receiver;
use std::{env,fs,path::Path,process::{self,Command}};
use std::os::unix::fs::MetadataExt;
fn stop(s:&str)->!{eprintln!("R1087 receiver DENIED {s}");process::exit(70)}
fn main(){
 let a=env::args().collect::<Vec<_>>();
 if a.len()!=3||env::var("ASTRA_R1087_PAPER_PUBLIC_ONLY").as_deref()!=Ok("YES"){
  stop("private public-only opt-in missing")
 }
 let root=Path::new(&a[1]);
 if root.parent()!=Some(Path::new(
   "/mnt/spotarb_db_test/astra_p001_r874_test_scratch_20261025"))||
   !root.file_name().and_then(|s|s.to_str())
    .is_some_and(|s|s.starts_with("astra_r671_file_r1087_"))||
   root.to_string_lossy()!=format!("{}/{}",
    root.parent().unwrap().display(),root.file_name().unwrap().to_string_lossy()){
   stop("unsafe isolated private root")
 }
 let meta=fs::symlink_metadata(root).unwrap_or_else(|_|stop("root missing"));
 if !meta.file_type().is_dir()||meta.mode()&0o077!=0||
    meta.uid()!=unsafe{libc::geteuid()}{
   stop("invalid private root owner/permissions")
 }
 let unit=Command::new("/usr/bin/systemctl").args(["show",
  "spotarb-native-paper-continuous.service","-p","ActiveState",
  "-p","MainPID","-p","UnitFileState"]).output()
  .unwrap_or_else(|_|stop("cannot check original PAPER"));
 let status=String::from_utf8_lossy(&unit.stdout);
 if !unit.status.success()||!status.lines().any(|x|x=="ActiveState=inactive")||
    !status.lines().any(|x|x=="MainPID=0")||
    !status.lines().any(|x|x=="UnitFileState=disabled"){
   stop("original PAPER is not stopped")
 }
 let k:serde_json::Value=serde_json::from_slice(
  &fs::read("/home/tmpadmin/spotbot/data/real_kill_switch.json")
    .unwrap_or_else(|_|stop("kill inaccessible")))
  .unwrap_or_else(|_|stop("kill malformed"));
 if k["state"]!="ENGAGED"{stop("REAL switch must be ENGAGED")}
 let max_s=a[2].parse::<u64>().unwrap_or_else(|_|stop("duration malformed"));
 let result=r1087_private_public_receiver(root,max_s)
   .unwrap_or_else(|e|stop(&format!("receiver: {e}")));
 println!("{}",result)
}
