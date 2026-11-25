//! Isolated parser+paired-state cost; excludes WS, IPC, observer, SQLite and execution.
use spotarb_native_market_core::{owner_index,CoreError,HistorySink,Owner,RawFrame,Update};
use std::io::{self,BufRead,Write};
use std::time::Instant;
#[derive(Default)]
struct History {count:u64, bytes:u64}
impl HistorySink for History {
 fn accept(&mut self,f:&RawFrame,_:&Update)->Result<(),CoreError>{
  self.count+=1;self.bytes+=f.raw.len() as u64;Ok(())
 }
}
fn cpu()->f64 {
 let mut usage:libc::rusage=unsafe{std::mem::zeroed()};
 unsafe{libc::getrusage(libc::RUSAGE_SELF,&mut usage)};
 (usage.ru_utime.tv_sec+usage.ru_stime.tv_sec) as f64 +
 (usage.ru_utime.tv_usec+usage.ru_stime.tv_usec) as f64/1e6
}
fn main(){
 let initial_cpu=cpu();let began=Instant::now();
 let mut owners:Vec<_>=(0..3).map(|i|Owner::new(i,3,1000).unwrap()).collect();
 let mut history=History::default();
 let mut read=0_u64;let mut compared=0_u64;let mut invalid=0_u64;
 for line in io::stdin().lock().lines(){
  let line=line.unwrap();let frame:RawFrame=serde_json::from_str(&line).unwrap();
  let parsed=spotarb_native_market_core::parse(&frame).unwrap();
  read+=1;
  let owner=&mut owners[owner_index(&parsed.base,3)];
  match owner.ingest(&frame,&mut history){
   Ok(Some(_))=>compared+=1,
   Ok(None)=>(),
   Err(_)=>invalid+=1,
  }
 }
 let wall=began.elapsed().as_secs_f64();
 let states:Vec<_>=owners.iter().flat_map(Owner::snapshot_books).collect();
 let result=serde_json::json!({"read":read,"accepted":history.count,
  "historical_raw_bytes":history.bytes,"compared":compared,"invalid":invalid,
  "cpu_s":cpu()-initial_cpu,"wall_s":wall,"cpu_us_per_frame":(cpu()-initial_cpu)*1e6/read as f64,
  "states":states});
 io::stdout().write_all(serde_json::to_string(&result).unwrap().as_bytes()).unwrap();
}
