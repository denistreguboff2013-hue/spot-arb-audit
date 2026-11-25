//! R1066 staged two-process PAPER-only collector / receiver proof.
//! Never launches installed PAPER or connects to exchange credentials.
use std::{env,fs,path::Path,process::{self,Command}};
use serde_json::Value;
use spotarb_native_market_core::native_isolated_sqlite_mirror_r147_review::{
 r1066_private_init,r1066_private_producer,r1066_private_receiver,
 r1069_private_batched_producer,r1069_private_batched_receiver,
 r1070_private_batched32_producer,r1070_private_batched32_receiver,
 r1081_private_source_first_half,r1081_private_source_resume
};
fn denied(reason:&str)->!{eprintln!("R1066 CANARY REFUSED: {reason}");process::exit(70)}
fn systemd(key:&str,expected:&str){
 let p=Command::new("/usr/bin/systemctl")
  .args(["show","spotarb-native-paper-continuous.service","-p",key])
  .output().unwrap_or_else(|_|denied("PAPER service status inaccessible"));
 if !p.status.success()||p.stdout!=format!("{key}={expected}\n").as_bytes(){
  denied("installed PAPER must be stopped and disabled")
 }
}
fn safety(){
 if env::var("ASTRA_R1066_PRIVATE_ONLY").as_deref()!=Ok("YES"){
  denied("explicit private opt-in missing")
 }
 systemd("ActiveState","inactive");
 systemd("MainPID","0");
 systemd("UnitFileState","disabled");
 let kill:Value=serde_json::from_slice(&fs::read(
  "/home/tmpadmin/spotbot/data/real_kill_switch.json")
  .unwrap_or_else(|_|denied("REAL kill unreadable")))
  .unwrap_or_else(|_|denied("REAL kill malformed"));
 if kill["state"]!="ENGAGED"{denied("REAL kill not ENGAGED")}
}
fn main(){
 let args=env::args().collect::<Vec<_>>();
 if args.len()!=4{denied("usage --init|--producer|--receiver PRIVATE_ROOT EVEN_FRAMES")}
 let mode=args[1].as_str();
 let root=Path::new(&args[2]);
 let n:u64=args[3].parse().unwrap_or_else(|_|denied("frame count"));
 if !(2..=1024).contains(&n)||n%2!=0{denied("only even 2..1024 synthetic frames")}
 // The existing R671 filesystem guard enforces the private tempfile root.
 safety();
 let result=match mode{
  "--init"=>r1066_private_init(root).map(|_|serde_json::json!({
     "schema":"r1066_paper_private_init_v1","ok":true,"production":false})),
  "--producer"=>r1066_private_producer(root,n),
  "--receiver"=>r1066_private_receiver(root,n),
  "--producer-batched"=>r1069_private_batched_producer(root,n),
  "--receiver-batched"=>r1069_private_batched_receiver(root,n),
  "--producer-batched32"=>r1070_private_batched32_producer(root,n),
  "--receiver-batched32"=>r1070_private_batched32_receiver(root,n),
  "--producer-first-half"=>r1081_private_source_first_half(root,n),
  "--producer-resume"=>r1081_private_source_resume(root,n),
  _=>denied("unknown mode")
 };
 match result{
  Ok(value)=>println!("{value}"),
  Err(e)=>denied(&e)
 }
}
