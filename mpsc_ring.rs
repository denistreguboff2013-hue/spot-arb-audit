//! Bounded preallocated MPSC ring for multiple receiver tasks and one owner.
//! Per-owner mutex serializes producers; no books lock or global queue.
use std::sync::{Condvar,Mutex};
use crate::CoreError;
struct State<T> { slots:Vec<Option<T>>, head:usize, tail:usize, depth:usize, closed:bool }
pub struct BoundedMpscRing<T> { state:Mutex<State<T>>, ready:Condvar }
impl<T> BoundedMpscRing<T> {
 pub fn new(capacity:usize)->Result<Self,CoreError>{
  if capacity==0 || capacity>4096{return Err(CoreError::Invalid("ring capacity"))}
  let slots=(0..capacity).map(|_|None).collect();
  Ok(Self{state:Mutex::new(State{slots,head:0,tail:0,depth:0,closed:false}),ready:Condvar::new()})
 }
 pub fn try_push(&self,item:T)->Result<(),CoreError>{
  let mut s=self.state.lock().expect("ring poisoned");
  if s.closed{return Err(CoreError::Invalid("ring closed"))}
  if s.depth==s.slots.len(){return Err(CoreError::HistoryPressure)}
  let tail=s.tail;s.slots[tail]=Some(item);s.tail=(tail+1)%s.slots.len();s.depth+=1;
  self.ready.notify_one();Ok(())
 }
 pub fn pop(&self)->Option<T>{
  let mut s=self.state.lock().expect("ring poisoned");
  loop {
   if s.depth>0{
    let head=s.head;let item=s.slots[head].take().expect("occupied ring slot");
    s.head=(head+1)%s.slots.len();s.depth-=1;return Some(item)
   }
   if s.closed{return None}
   s=self.ready.wait(s).expect("ring poisoned");
  }
 }
 pub fn close(&self){let mut s=self.state.lock().expect("ring poisoned");s.closed=true;self.ready.notify_all()}
 pub fn depth(&self)->usize{self.state.lock().expect("ring poisoned").depth}
}
#[cfg(test)]mod tests{
 use super::*;
 use std::sync::Arc;
 #[test]fn bounded_fifo_and_close(){
  let q=BoundedMpscRing::new(2).unwrap();q.try_push(1).unwrap();q.try_push(2).unwrap();
  assert_eq!(q.try_push(3),Err(CoreError::HistoryPressure));
  q.close();assert_eq!(q.pop(),Some(1));assert_eq!(q.pop(),Some(2));assert_eq!(q.pop(),None);
  assert_eq!(q.try_push(4),Err(CoreError::Invalid("ring closed")));
 }
 #[test]fn many_producers_exactly_once(){
  let q=Arc::new(BoundedMpscRing::new(4096).unwrap());let mut threads=Vec::new();
  for n in 0..4{let q=q.clone();threads.push(std::thread::spawn(move||{
   for i in 0..1000{loop{match q.try_push((n,i)){
    Ok(())=>break,Err(CoreError::HistoryPressure)=>std::thread::yield_now(),Err(e)=>panic!("{e:?}")
   }}}
  }));}
  let mut got=Vec::new();for _ in 0..4000{got.push(q.pop().unwrap())}
  for t in threads{t.join().unwrap()};q.close();assert_eq!(q.pop(),None);
  got.sort();got.dedup();assert_eq!(got.len(),4000);
 }
}
