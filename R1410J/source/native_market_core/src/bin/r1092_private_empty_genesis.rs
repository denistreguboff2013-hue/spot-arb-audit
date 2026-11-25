//! R1092 one-shot private PAPER-only empty genesis certification.
/// No installed PAPER start or private exchange endpoints.
use std::{env,fs,path::Path,process::{self,Command}};
use spotarb_native_market_core::native_isolated_sqlite_mirror_r147_review::r1092_private_empty_genesis;
fn deny(s:&str)->!{eprintln!("R1092 genesis REFUSED {s}");process::exit(70)}
fn main(){
 let args=env::args().collect::<Vec<_>>();
 let full_paper=env::var("ASTRA_R1119_FULL_RUST_PAPER")
     .as_deref()==Ok("YES");
 let dated=if full_paper{
   let fields=args.get(2).map(|v|v.split('_').collect::<Vec<_>>()).unwrap_or_default();
   fields.len()==3&&fields[0]=="session"&&
     fields[1].parse::<u64>().is_ok()&&
     fields[2].parse::<u64>().is_ok()
 }else{false};
 if args.len()!=3 ||
  env::var("ASTRA_R1092_PRIVATE_GENESIS").as_deref()!=Ok("YES") ||
  (args[2]!="session_r1087_public"&&!dated){
    deny("opt-in, root, known PAPER session required")
 }
 let root=Path::new(&args[1]);
 if root.parent()!=Some(Path::new(
   "/mnt/spotarb_db_test/astra_p001_r874_test_scratch_20261025"))||
   !root.file_name().and_then(|x|x.to_str())
     .is_some_and(|s|s.starts_with("astra_r671_file_r1087_")){
    deny("isolated R671 scratch root required")
 }
 let proc=Command::new("/usr/bin/systemctl")
   .args(["show","spotarb-native-paper-continuous.service",
    "-p","ActiveState","-p","MainPID","-p","UnitFileState"])
   .output().unwrap_or_else(|_|deny("systemd unavailable"));
 let unit=String::from_utf8_lossy(&proc.stdout);
 if !proc.status.success()||!unit.contains("ActiveState=inactive")||
    !unit.contains("MainPID=0")||!unit.contains("UnitFileState=disabled"){
    deny("installed PAPER not stopped and disabled")
 }
 let kill:serde_json::Value=serde_json::from_slice(
   &fs::read("/home/tmpadmin/spotbot/data/real_kill_switch.json")
      .unwrap_or_else(|_|deny("REAL kill missing")))
      .unwrap_or_else(|_|deny("REAL kill malformed"));
 if kill["state"]!="ENGAGED"{deny("REAL orders not blocked")}
 r1092_private_empty_genesis(root,&args[2])
   .unwrap_or_else(|e|deny(&format!("cannot attest empty original DB1/DB2: {e:?}")));
 println!("{}",serde_json::json!({
   "schema":"SPOTARB/R1092/EMPTY-GENESIS-PROOF/V1",
   "ok":true,"run":args[2],"guarded_17_tables":true,
   "real_orders":false,"old_positions_carried":false
 }));
}
