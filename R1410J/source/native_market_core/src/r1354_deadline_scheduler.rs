//! R1354 offline-only prototype: event-driven deadline index; not trading authority.
use std::{cmp::Reverse,collections::{BinaryHeap,HashMap,HashSet}};
#[derive(Clone,Eq,PartialEq,Ord,PartialOrd,Debug)]
struct Deadline { at_ns:u64, scope:String, version:u64 }
#[derive(Default)]
pub struct Scheduler {heap:BinaryHeap<Reverse<Deadline>>, versions:HashMap<String,u64>, due:HashSet<String>}
impl Scheduler {
 /// Register next monotonic deadline; previous heap entries become inert.
 pub fn schedule(&mut self,scope:&str,at_ns:u64)->Result<(),&'static str>{
  let version=self.versions.get(scope).copied().unwrap_or(0).checked_add(1).ok_or("deadline version overflow")?;
  self.versions.insert(scope.to_owned(),version);
  self.heap.push(Reverse(Deadline{at_ns,scope:scope.to_owned(),version}));Ok(())
 }
 /// Force immediate re-evaluation after disconnect/reset/manager state transition.
 pub fn invalidate(&mut self,scope:&str)->Result<(),&'static str>{
  let version=self.versions.get(scope).copied().unwrap_or(0).checked_add(1).ok_or("deadline version overflow")?;
  self.versions.insert(scope.to_owned(),version);self.due.insert(scope.to_owned());Ok(())
 }
 /// Returns due scopes only; every consumer must independently verify freshness.
 pub fn take_due(&mut self,now_ns:u64)->Vec<String>{
  while self.heap.peek().is_some_and(|Reverse(d)|d.at_ns<=now_ns){
   let Reverse(d)=self.heap.pop().unwrap();
   if self.versions.get(&d.scope)==Some(&d.version){self.due.insert(d.scope);}
  }
  self.due.drain().collect()
 }
 pub fn pending_entries(&self)->usize{self.heap.len()}
}
#[cfg(test)] mod tests {
 use super::*;
 #[test] fn deadline_boundary_and_reschedule(){let mut s=Scheduler::default();s.schedule("X",800_000_001).unwrap();assert!(s.take_due(800_000_000).is_empty());assert_eq!(s.take_due(800_000_001),vec!["X"]);s.schedule("X",1_700_000_000).unwrap();s.schedule("X",1_800_000_000).unwrap();assert!(s.take_due(1_700_000_000).is_empty());assert_eq!(s.take_due(1_800_000_000),vec!["X"]);}
 #[test] fn reset_disconnect_force_only_affected_scope(){let mut s=Scheduler::default();for i in 0..235{s.schedule(&format!("S{i:03}"),1000).unwrap();}s.invalidate("S007").unwrap();assert_eq!(s.take_due(1),vec!["S007"]);assert_eq!(s.take_due(1000).len(),234);}
 #[test] fn same_time_no_duplicates(){let mut s=Scheduler::default();s.schedule("A",10).unwrap();s.schedule("A",10).unwrap();s.invalidate("A").unwrap();assert_eq!(s.take_due(10),vec!["A"]);}
 #[test] fn monotonic_receive_age_boundary(){let mut s=Scheduler::default();let receive_ns=123u64;s.schedule("A",receive_ns+800_000_001).unwrap();assert!(s.take_due(receive_ns+800_000_000).is_empty());assert_eq!(s.take_due(receive_ns+800_000_001),vec!["A"]);}
}

#[cfg(test)] mod r1354_differential_model {
 use super::*;
 #[derive(Clone,Copy)] struct Source {received_ns:u64,event_ms:i64,hot:bool,connected:bool}
 fn ready(s:Source,ns:u64,wall:i64)->bool{s.connected&&s.hot&&ns>=s.received_ns&&ns-s.received_ns<=800_000_000&&wall.checked_sub(s.event_ms).is_some_and(|age|(0..=800).contains(&age))}
 fn deadline(s:Source,ns:u64,wall:i64)->u64{
  let receive_expiry=s.received_ns.saturating_add(800_000_001);
  let wall_expiry=s.event_ms.saturating_add(801);
  let wall_delta_ms=wall_expiry.saturating_sub(wall).max(0) as u64;
  receive_expiry.min(ns.saturating_add(wall_delta_ms.saturating_mul(1_000_000)))
 }
 #[test] fn r1354_model_235_scope_silence_updates_disconnect_and_wall_jump(){
  let mut scheduler=Scheduler::default();let mut sources=HashMap::<String,Source>::new();
  let mut last=HashMap::<String,bool>::new();let mut compared=0usize;
  let mut ns=1_000_000_000u64;let mut wall=10_000i64;
  for i in 0..235 {let k=format!("S{i:03}");let s=Source{received_ns:ns,event_ms:wall,hot:true,connected:true};sources.insert(k.clone(),s);last.insert(k.clone(),true);scheduler.schedule(&k,deadline(s,ns,wall)).unwrap();}
  for step in 1..=50 {
   ns+=25_000_000;wall+=25;
   if step==8 {let k="S007";let s=sources.get_mut(k).unwrap();s.received_ns=ns;s.event_ms=wall;scheduler.schedule(k,deadline(*s,ns,wall)).unwrap();last.insert(k.into(),true);}
   if step==12 {let k="S008";sources.get_mut(k).unwrap().connected=false;scheduler.invalidate(k).unwrap();}
   if step==18 {let k="S009";sources.get_mut(k).unwrap().hot=false;scheduler.invalidate(k).unwrap();}
   // Clock discontinuity is an explicit global invalidation event, not a normal timer expiry.
   if step==24 {wall+=900;for k in sources.keys(){scheduler.invalidate(k).unwrap();}}
   let due=scheduler.take_due(ns);
   for k in due {let s=sources[&k];last.insert(k.clone(),ready(s,ns,wall));if ready(s,ns,wall){scheduler.schedule(&k,deadline(s,ns,wall)).unwrap();}}
   // Reference full scan is authoritative for this simplified model.
   for (k,s) in &sources {assert_eq!(last[k],ready(*s,ns,wall),"scope={k} step={step}");compared+=1;}
  }
  assert_eq!(compared,235*50);
 }
 #[test] fn r1354_model_expiry_boundary_both_clocks(){
  let s=Source{received_ns:1_000_000_000,event_ms:10_000,hot:true,connected:true};
  assert!(ready(s,1_800_000_000,10_800));assert!(!ready(s,1_800_000_001,10_800));assert!(!ready(s,1_800_000_000,10_801));
  assert_eq!(deadline(s,1_000_000_000,10_000),1_800_000_001);
 }
}
