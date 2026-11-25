//! R146 manual, cross-PID **synthetic** durable journal smoke only.
//! The library itself restricts files to private /tmp/astra_r143_isolated_*/ paths.
use spotarb_native_market_core::native_isolated_mirror_journal_r143_review::{
 initialize_isolated,append_source_row_isolated,recover_isolated,
};
use spotarb_native_market_core::native_mirror_outbox_protocol_r139_review::MirrorOutboxRowR139;
use std::{env,path::PathBuf,time::{SystemTime,UNIX_EPOCH}};
fn run()->Result<(),String>{
 let args:Vec<String>=env::args().collect();
 if !(args.len()==3||(args.len()==5&&args[1]=="append")){
  return Err("usage: MODE init|recover|append PATH [SEQ PREV]".into())
 }
 let path=PathBuf::from(&args[2]);
 let now=SystemTime::now().duration_since(UNIX_EPOCH)
  .map_err(|_|"clock")?.as_millis() as u64;
 match args[1].as_str(){
  "init"=>initialize_isolated(&path).map_err(|e|format!("{e:?}"))?,
  "recover"=>{
   let result=recover_isolated(&path,now).map_err(|e|format!("{e:?}"))?;
   println!("{{\"records\":{},\"last_seq\":{},\"db2_authorized\":false}}",
    result.records,result.last_seq);
  },
  "append"=>{
   let seq=args[3].parse::<u64>().map_err(|_|"bad seq")?;
   let prev=args[4].parse::<u64>().map_err(|_|"bad prev")?;
   let mut row:MirrorOutboxRowR139=serde_json::from_str(include_str!(
    "../../config/native_mirror_outbox_python_fixture_r141_review.json"))
    .map_err(|_|"invalid offline fixture")?;
   row.seq=seq;row.prev_seq=prev;
   row.tx_id=format!("tx-{seq:032x}");
   let result=append_source_row_isolated(&path,row,now)
    .map_err(|e|format!("{e:?}"))?;
   println!("{{\"records\":{},\"last_seq\":{},\"db2_authorized\":false}}",
    result.records,result.last_seq);
  },
  _=>return Err("unknown mode".into())
 }
 Ok(())
}
fn main(){
 if let Err(e)=run(){eprintln!("R146 DENY: {e}");std::process::exit(2)}
}
