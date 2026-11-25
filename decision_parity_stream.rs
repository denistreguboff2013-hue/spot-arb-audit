//! Isolated streaming bridge: Rust owns book state, Python retains canonical
//! eligibility/decision semantics. Every accepted mutation emits a snapshot.
use spotarb_native_market_core::{owner_index,CoreError,HistorySink,Owner,RawFrame,Update};
use std::io::{self,BufRead,Write};
struct History { admitted:u64 }
impl HistorySink for History {
 fn accept(&mut self,_:&RawFrame,_:&Update)->Result<(),CoreError>{
  self.admitted+=1;Ok(())
 }
}
fn main(){
 let mut owners:Vec<_>=(0..3).map(|i|Owner::new(i,3,800).unwrap()).collect();
 let mut history=History{admitted:0};
 let stdin=io::stdin();let stdout=io::stdout();let mut out=stdout.lock();
 for line in stdin.lock().lines(){
  let line=line.expect("input line");
  let frame:RawFrame=serde_json::from_str(&line).expect("frame envelope");
  let update=match spotarb_native_market_core::parse(&frame){
   Ok(x)=>x,Err(error)=>{
    writeln!(out,"{}",serde_json::json!({"accepted":false,"error":format!("{error:?}")})).unwrap();
    out.flush().unwrap();continue
   }
  };
  let base=update.base;
  let owner=&mut owners[owner_index(&base,3)];
  let result=owner.ingest(&frame,&mut history);
  let (spot,future)=owner.snapshot_pair(&base);
  let accepted=result.is_ok();
  writeln!(out,"{}",serde_json::json!({"base":base,"accepted":accepted,
   "error":result.err().map(|e|format!("{e:?}")),"spot":spot,"future":future,
   "history_admitted":history.admitted})).unwrap();
  out.flush().unwrap();
 }
}
