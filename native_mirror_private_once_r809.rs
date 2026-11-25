//! Astra P0-01: separate offline Rust ORIGINAL Python outbox mirror worker.
//! Usage: native_mirror_private_once_r809 --once /tmp/astra_r671_file_...
//! Exactly two named SQLite files under owner-owned 0700 test directory.
//! NO production DB paths, PAPER service, exchange credentials, or real orders.
use std::{env,path::Path,process};
use spotarb_native_market_core::native_isolated_sqlite_mirror_r147_review::{
 r805_private_service_once,r805_private_service_selftest,
 r813_private_initialize_once,r813_private_ingest_installed_paper_once,
 r856_private_drain_bounded,r865_private_fast_ack_drain_bounded,
};
fn main(){
 let args=env::args_os().collect::<Vec<_>>();
 if args.len()==2&&args[1]=="--selftest"{
  match r805_private_service_selftest(){
   Ok(())=>println!("R809 PRIVATE PYTHON->RUST SELFTEST PASS real_enabled=false"),
   Err(e)=>{eprintln!("SELFTEST FAIL_CLOSED: {e}");process::exit(70);}
  }
  return
 }
 if args.len()==3&&args[1]=="--paper-source-init"{
  match r813_private_initialize_once(Path::new(&args[2])){
   Ok(())=>println!("R813 private 17-table Rust source initialized real_enabled=false"),
   Err(e)=>{eprintln!("R813 INIT REFUSED: {e}");process::exit(70);}
  }
  return
 }
 if args.len()==3&&args[1]=="--paper-status-once"{
  match r813_private_ingest_installed_paper_once(Path::new(&args[2])){
   Ok((appended,seq,delivered,acked))=>{
    println!("R813 LIVE PAPER STATUS->RUST DB1->DB2 appended={appended} seq={seq} delivered={delivered} acked={acked} real_enabled=false");
   },
   Err(e)=>{eprintln!("R813 PAPER SOURCE REFUSED: {e}");process::exit(70);}
  }
  return
 }
 if args.len()==3&&args[1]=="--drain-private"{
  match r856_private_drain_bounded(Path::new(&args[2]),256){
   Ok((delivered,acked,retained))=>{
    println!("R856 verified_private_only=true delivered={delivered} acked={acked} retained={retained} real_enabled=false");
    if retained!=0{process::exit(75)}
   },
   Err(reason)=>{eprintln!("R856 FAIL_CLOSED: {reason}");process::exit(70);}
  }
  return
 }
 if args.len()==3&&args[1]=="--drain-fast-ack-private"{
  match r865_private_fast_ack_drain_bounded(Path::new(&args[2]),256){
   Ok((delivered,acked,retained))=>{
    println!("R865 verified_private_only=true delivered={delivered} acked={acked} retained={retained} real_enabled=false");
    if retained!=0{process::exit(75)}
   },
   Err(reason)=>{eprintln!("R865 FAIL_CLOSED: {reason}");process::exit(70);}
  }
  return
 }
 if args.len()!=3||args[1]!="--once"{
  eprintln!("usage: --once PRIVATE_ROOT | --drain-private PRIVATE_ROOT | --drain-fast-ack-private PRIVATE_ROOT | --paper-source-init PRIVATE_ROOT | --paper-status-once PRIVATE_ROOT | --selftest");
  process::exit(64);
 }
 match r805_private_service_once(Path::new(&args[2])){
  Ok((delivered,acknowledged,retained))=>{
   println!("private_offline=true delivered={delivered} acked={acknowledged} retained={retained} real_enabled=false");
  },
  Err(reason)=>{
   eprintln!("FAIL_CLOSED private mirror: {reason}");
   process::exit(70);
  }
 }
}
