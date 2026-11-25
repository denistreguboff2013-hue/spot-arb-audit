//! R1352: isolated, best-effort RAM-only diagnostics. Never authorizes trading.
use std::{collections::HashMap,sync::mpsc::{sync_channel,SyncSender,Receiver,TrySendError},thread};

#[derive(Clone,Debug,PartialEq,Eq)]
pub struct ScopeStatus {
 pub scope:String,
 pub generation:u64,
 pub observed_ns:u64,
 pub last_valid_ns:Option<u64>,
 pub hot_sources:u16,
 pub selected:bool,
}
#[derive(Debug)]
pub enum Message { Update(ScopeStatus), Reset{scope:String,generation:u64}, Stop }
#[derive(Default,Debug)]
pub struct DiagnosticState { pub scopes:HashMap<String,ScopeStatus>,pub accepted:u64,pub rejected_old:u64 }
impl DiagnosticState {
 pub fn apply(&mut self,message:Message){match message {
  Message::Update(status)=>{
   let newer=self.scopes.get(&status.scope).map_or(true,|old|status.generation>old.generation||(status.generation==old.generation&&status.observed_ns>=old.observed_ns));
   if newer {self.scopes.insert(status.scope.clone(),status);self.accepted+=1;}else{self.rejected_old+=1;}
  }
  Message::Reset{scope,generation}=>{
   if self.scopes.get(&scope).map_or(true,|old|generation>=old.generation){self.scopes.insert(scope.clone(),ScopeStatus{scope,generation,observed_ns:0,last_valid_ns:None,hot_sources:0,selected:false});}
   else {self.rejected_old+=1;}
  }
  Message::Stop=>{}
 }}
}
#[derive(Clone)]
pub struct Publisher {tx:SyncSender<Message>}
impl Publisher {
 /// Nonblocking: on full channel only diagnostic updates may be discarded.
 pub fn try_publish(&self,message:Message)->bool {match self.tx.try_send(message){Ok(())=>true,Err(TrySendError::Full(_)|TrySendError::Disconnected(_))=>false}}
}
pub fn spawn(capacity:usize)->(Publisher,thread::JoinHandle<DiagnosticState>){
 let (tx,rx):(SyncSender<Message>,Receiver<Message>)=sync_channel(capacity.max(1));
 let handle=thread::spawn(move||{let mut state=DiagnosticState::default();while let Ok(message)=rx.recv(){if matches!(message,Message::Stop){break;}state.apply(message);}state});
 (Publisher{tx},handle)
}
#[cfg(test)] mod tests {
 use super::*;
 fn status(scope:&str,generation:u64,observed_ns:u64)->ScopeStatus{ScopeStatus{scope:scope.into(),generation,observed_ns,last_valid_ns:Some(observed_ns),hot_sources:2,selected:true}}
 #[test] fn r1352_generation_and_time_fence(){let mut s=DiagnosticState::default();s.apply(Message::Update(status("BTC",2,200)));s.apply(Message::Update(status("BTC",1,300)));s.apply(Message::Update(status("BTC",2,100)));assert_eq!(s.scopes["BTC"].observed_ns,200);assert_eq!(s.rejected_old,2);s.apply(Message::Reset{scope:"BTC".into(),generation:3});assert!(!s.scopes["BTC"].selected);s.apply(Message::Update(status("BTC",2,400)));assert_eq!(s.scopes["BTC"].generation,3);}
 #[test] fn r1352_full_channel_never_waits(){let (tx,_rx)=sync_channel(1);let p=Publisher{tx};assert!(p.try_publish(Message::Update(status("BTC",1,1))));assert!(!p.try_publish(Message::Update(status("ETH",1,2))));}
 #[test] fn r1352_worker_receives_and_exits(){let (p,handle)=spawn(4);assert!(p.try_publish(Message::Update(status("BTC",1,10))));assert!(p.try_publish(Message::Stop));let s=handle.join().unwrap();assert_eq!(s.scopes["BTC"].generation,1);}
}

/// R1353: end-to-end monotonic sensor sample. None means unavailable, not zero.
#[derive(Clone,Debug,Default)]
pub struct StageSample {
 pub scope:String,
 pub connection:String,
 pub ws_decode_ns:Option<u64>,
 pub reader_queue_ns:Option<u64>,
 pub central_queue_ns:Option<u64>,
 pub validation_ns:Option<u64>,
 pub book_ns:Option<u64>,
 pub selection_ns:Option<u64>,
 pub engine_ns:Option<u64>,
}
fn duration(start:Option<u64>,end:Option<u64>)->Option<u64>{start.zip(end).and_then(|(a,b)|b.checked_sub(a))}
impl StageSample {
 pub fn from_timing(scope:&str,connection:&str,t:&crate::R1279FrameTiming)->Self{
  Self{scope:scope.into(),connection:connection.into(),
   ws_decode_ns:duration(t.ws_decode_start_ns,t.ws_decode_end_ns),
   reader_queue_ns:duration(t.reader_enqueue_ns,t.reader_dequeue_ns),
   central_queue_ns:duration(t.central_enqueue_ns,t.central_dequeue_ns),
   validation_ns:duration(t.validation_start_ns,t.validation_end_ns),
   book_ns:duration(t.book_start_ns,t.book_end_ns),
   selection_ns:duration(t.source_selection_start_ns,t.source_selection_end_ns),
   engine_ns:duration(t.engine_receive_begin_ns,t.engine_receive_end_ns)}
 }
}
#[derive(Clone,Debug,Default)]
pub struct StageCounters {pub samples:u64,pub sum_ns:u128,pub max_ns:u64}
impl StageCounters {fn observe(&mut self,v:u64){self.samples+=1;self.sum_ns+=v as u128;self.max_ns=self.max_ns.max(v);}}
#[derive(Default,Debug)]
pub struct ShardState {pub scopes:HashMap<String,ScopeStatus>,pub stages:HashMap<(String,String,&'static str),StageCounters>,pub accepted:u64,pub rejected_old:u64}
#[derive(Debug)]
pub enum ShardMessage {Status(ScopeStatus),Sample(StageSample),Reset{scope:String,generation:u64}}
impl ShardState {
 fn apply(&mut self,msg:ShardMessage){match msg {
  ShardMessage::Status(s)=>{let newer=self.scopes.get(&s.scope).map_or(true,|old|s.generation>old.generation||(s.generation==old.generation&&s.observed_ns>=old.observed_ns));if newer{self.scopes.insert(s.scope.clone(),s);self.accepted+=1;}else{self.rejected_old+=1;}}
  ShardMessage::Reset{scope,generation}=>{if self.scopes.get(&scope).map_or(true,|old|generation>=old.generation){self.scopes.insert(scope.clone(),ScopeStatus{scope,generation,observed_ns:0,last_valid_ns:None,hot_sources:0,selected:false});}else{self.rejected_old+=1;}}
  ShardMessage::Sample(s)=>{let fields=[("ws_decode",s.ws_decode_ns),("reader_queue",s.reader_queue_ns),("central_queue",s.central_queue_ns),("validation",s.validation_ns),("book",s.book_ns),("selection",s.selection_ns),("engine",s.engine_ns)];for (name,value) in fields{if let Some(v)=value{self.stages.entry((s.scope.clone(),s.connection.clone(),name)).or_default().observe(v);}}}
 }}
}
/// Four equal, independent, nonblocking diagnostic channels. No per-instrument priority.
pub struct ShardedDiagnostics {senders:Vec<SyncSender<ShardMessage>>,dropped:std::sync::atomic::AtomicU64}
impl ShardedDiagnostics {
 fn shard(&self,scope:&str)->usize{let mut hash=14695981039346656037u64;for byte in scope.bytes(){hash^=byte as u64;hash=hash.wrapping_mul(1099511628211);} (hash as usize)%self.senders.len()}
 pub fn try_publish(&self,msg:ShardMessage)->bool{let scope=match &msg{ShardMessage::Status(s)=>&s.scope,ShardMessage::Sample(s)=>&s.scope,ShardMessage::Reset{scope,..}=>scope};let result=self.senders[self.shard(scope)].try_send(msg).is_ok();if !result{self.dropped.fetch_add(1,std::sync::atomic::Ordering::Relaxed);}result}
 pub fn dropped(&self)->u64{self.dropped.load(std::sync::atomic::Ordering::Relaxed)}
}
pub fn spawn_four(capacity_per_shard:usize)->(ShardedDiagnostics,Vec<thread::JoinHandle<ShardState>>){
 let mut senders=Vec::with_capacity(4);let mut handles=Vec::with_capacity(4);
 for _ in 0..4{let (tx,rx)=sync_channel(capacity_per_shard.max(1));senders.push(tx);handles.push(thread::spawn(move||{let mut state=ShardState::default();while let Ok(msg)=rx.recv(){state.apply(msg);}state}));}
 (ShardedDiagnostics{senders,dropped:std::sync::atomic::AtomicU64::new(0)},handles)
}
#[cfg(test)] mod r1353_tests {
 use super::*;
 #[test] fn four_shards_235_equal_scopes(){let (bus,handles)=spawn_four(256);for i in 0..235{assert!(bus.try_publish(ShardMessage::Status(ScopeStatus{scope:format!("R{i:03}USDT"),generation:1,observed_ns:10,last_valid_ns:Some(10),hot_sources:2,selected:true})));}assert_eq!(bus.dropped(),0);drop(bus);let total:usize=handles.into_iter().map(|h|h.join().unwrap().scopes.len()).sum();assert_eq!(total,235);}
 #[test] fn monotonic_stage_sensors_and_missing_fields(){let t=crate::R1279FrameTiming{ws_decode_start_ns:Some(10),ws_decode_end_ns:Some(20),reader_enqueue_ns:Some(30),reader_dequeue_ns:Some(25),validation_start_ns:Some(40),validation_end_ns:Some(55),..Default::default()};let s=StageSample::from_timing("R000USDT","session",&t);assert_eq!(s.ws_decode_ns,Some(10));assert_eq!(s.reader_queue_ns,None);assert_eq!(s.validation_ns,Some(15));assert_eq!(s.book_ns,None);}
 #[test] fn shard_sensor_aggregation(){let (bus,handles)=spawn_four(16);assert!(bus.try_publish(ShardMessage::Sample(StageSample{scope:"R000USDT".into(),connection:"s1".into(),validation_ns:Some(8),..Default::default()})));drop(bus);let total:u64=handles.into_iter().map(|h|h.join().unwrap().stages.values().map(|x|x.samples).sum::<u64>()).sum();assert_eq!(total,1);}
}
