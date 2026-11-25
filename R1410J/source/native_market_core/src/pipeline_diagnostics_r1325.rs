//! Bounded, opt-in, thread-local diagnostics. No I/O, locks, clocks or policy mutation in record().
use serde::Serialize;
use std::{cell::RefCell,collections::{BTreeMap,VecDeque}};
#[derive(Default,Serialize)] pub struct Span {pub count:u64,pub errors:u64,pub total_ns:u64,pub max_ns:u64}
#[derive(Clone,Serialize)] pub struct Context {pub venue:String,pub symbol:String,pub owner:usize,pub generation:u64,pub queue_occupied_slots_at_enqueue:Option<usize>}
#[derive(Serialize)] pub struct Sample {pub context:Option<Context>,pub trace:String,pub stage:String,pub duration_ns:u64,pub ok:bool}
#[derive(Default,Serialize)] pub struct Diagnostics {pub spans:BTreeMap<String,Span>,pub samples:VecDeque<Sample>,pub evicted:u64}
impl Diagnostics {
 pub fn merge(&mut self,other:Self){
  for(k,v)in other.spans {let n=self.spans.entry(k).or_default();n.count+=v.count;n.errors+=v.errors;n.total_ns=n.total_ns.saturating_add(v.total_ns);n.max_ns=n.max_ns.max(v.max_ns);}
  self.evicted+=other.evicted;
  for s in other.samples {if self.samples.len()==128{self.samples.pop_front();self.evicted+=1;}self.samples.push_back(s);}
 }
}
thread_local!{static CURRENT:RefCell<Option<(String,Diagnostics,Option<Context>)>>=const{RefCell::new(None)};}
pub fn begin(trace:&str){CURRENT.with(|c|*c.borrow_mut()=Some((trace.chars().take(192).collect(),Diagnostics::default(),None)));}
pub fn record(stage:&'static str,ns:u64,ok:bool){CURRENT.with(|c|{
 let mut c=c.borrow_mut();let Some((trace,d,context))=c.as_mut()else{return};
 if d.spans.len()>=32&&!d.spans.contains_key(stage){d.evicted+=1;return}
 let n=d.spans.entry(stage.into()).or_default();n.count+=1;n.errors+=u64::from(!ok);n.total_ns=n.total_ns.saturating_add(ns);n.max_ns=n.max_ns.max(ns);
 if d.samples.len()==128{d.samples.pop_front();d.evicted+=1;}
 d.samples.push_back(Sample{context:context.clone(),trace:trace.clone(),stage:stage.into(),duration_ns:ns,ok});
});}
pub fn finish()->Diagnostics{CURRENT.with(|c|c.borrow_mut().take().map(|(_,d,_)|d).unwrap_or_default())}

#[cfg(test)] mod tests {
 use super::*;
 #[test]fn r1325_diagnostics_bounded_and_loss_accounted(){
  begin("trace-one");for _ in 0..300{record("DecisionCore",7,true)}
  let d=finish();assert_eq!(d.samples.len(),128);assert_eq!(d.evicted,172);
  assert_eq!(d.spans["DecisionCore"].count,300);assert_eq!(d.spans["DecisionCore"].total_ns,2100);
  assert!(d.samples.iter().all(|s|s.trace=="trace-one"));
  record("disabled",1,false);assert!(finish().spans.is_empty());
 }
 #[test]fn r1325_trace_identity_survives_custody_assignment(){
  let mut source=crate::market_data_ingress_r1205::provenance("source".into(),1);source.frame_trace_id="ws-stable-id".into();
  let mut f=crate::RawFrame{venue:crate::Venue::OkxSpot,epoch:1,received_ns:1,received_wall_ms:1,raw:String::new(),source:Some(source)};
  let before=crate::market_data_ingress_r1205::frame_trace_id_r1304(&f);f.source.as_mut().unwrap().accepted_id=999;
  assert_eq!(before,crate::market_data_ingress_r1205::frame_trace_id_r1304(&f));assert_eq!(before,"ws-stable-id");
 }
}

pub fn context(value:Context){CURRENT.with(|c|{if let Some((_,_,context))=c.borrow_mut().as_mut(){*context=Some(value)}})}
