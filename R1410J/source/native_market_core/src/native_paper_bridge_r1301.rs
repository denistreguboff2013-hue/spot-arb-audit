//! R1301 sealed public market observation boundary. No order transport.
use crate::{Book,Venue,RawFrame,Update,R1279FrameTiming,r1310_stage_event};
use crate::market_data_ingress_r1205::{Authority,Output,authorization_at_normalized};
use std::{collections::HashMap,sync::{Arc,Mutex,atomic::{AtomicBool,AtomicU64,Ordering},mpsc::{sync_channel,SyncSender,Receiver,TrySendError}},time::{Instant,Duration}};
use serde::Serialize;
pub const CAPACITY:usize=8;
pub const MAX_AGE_NS:u64=800_000_000;
pub const RESIDENCE:Duration=Duration::from_millis(100);
#[derive(Clone,Debug,PartialEq,Eq,Serialize)]
pub enum Denial { Raw,Timestamp,Stale,Hot,Sequence,Generation,Duplicate,Overflow,Residence,Consumer,Clock,Owner,Storage,Kill,Real,Endpoint,Rules,Policy,Closed,Provider }
#[derive(Clone,Debug,Serialize)]
pub enum MarketType { Spot,Futures }
/// Deliberately no Deserialize or public constructor; only the provider can mint this.
#[derive(Clone,Debug,Serialize)]
pub struct ValidatedInput {
 trace_id:String, trace:Vec<serde_json::Value>,
 // Preserve accepted custody identity; this does not assert DB1/DB2 durability.
 source_provenance:crate::SourceProvenance,
 #[serde(skip)] source_frame:RawFrame,
 #[serde(skip)] r1310_timing:Option<R1279FrameTiming>,
 venue:Venue, instrument:String, venue_symbol:String, market_type:MarketType, book:Book,
 exchange_ms:i64, receive_ns:u64, validated_ns:u64,
 #[serde(skip)] witness:Option<crate::market_data_ingress_r1205::SnapshotWitness>,
 sequence:i64, selection_generation:u64, snapshot_generation:Option<u64>, connection_generation:u64,
 source:String, standby:String, hot_at_validation:bool, fresh_at_validation:bool, clock_domain:String, rules_identity:Option<String>,
}
impl ValidatedInput {
 pub fn trace_id(&self)->&str{&self.trace_id}
 pub fn source_provenance(&self)->&crate::SourceProvenance{&self.source_provenance}
 pub fn owner_parts_r1322(&self)->(RawFrame,Update){
  (self.source_frame.clone(),Update{venue:self.venue,base:self.instrument.clone(),book:self.book.clone()})
 }
 pub fn trace(&self)->&[serde_json::Value]{&self.trace}
 pub fn r1310_timing(&self)->Option<R1279FrameTiming>{self.r1310_timing.clone()}
 pub fn exchange_ms(&self)->i64{self.exchange_ms}
 pub fn venue(&self)->Venue{self.venue}
 pub fn instrument(&self)->&str{&self.instrument}
 pub fn venue_symbol(&self)->&str{&self.venue_symbol}
 pub fn book(&self)->&Book{&self.book}
 pub fn input_id(&self)->String{format!("{}:{}:{}:{}",self.source,self.connection_generation,self.selection_generation,self.sequence)}
 pub fn clock_domain(&self)->&str{&self.clock_domain}
 pub fn rules_identity(&self)->Option<&str>{self.rules_identity.as_deref()}
 pub fn snapshot_generation(&self)->Option<u64>{
  self.witness.as_ref().filter(|w|w.matches(self.venue,&self.instrument,&self.source,self.connection_generation)).map(|w|w.generation())
 }
 fn fresh(&self,now:u64,wall:i64)->Result<(),Denial>{
  if now<self.validated_ns||now<self.receive_ns{return Err(Denial::Clock)}
  if now-self.receive_ns>MAX_AGE_NS||wall.checked_sub(self.exchange_ms).map_or(true,|a|!(0..=800).contains(&a)){return Err(Denial::Stale)}
  Ok(())
 }
}
#[derive(Default,Debug,Serialize)]
pub struct Metrics { pub enqueued:u64,pub dequeued:u64,pub rejected:u64,pub duplicates:u64,pub overflow:u64,pub dropped:u64,pub high_water:usize,pub residence_max_ns:u64,#[serde(default)]pub r1310_trace_records:u64,#[serde(default)]pub r1310_trace_dropped:u64,#[serde(default)]pub r1310_queue_wait_total_ns:u64,#[serde(default)]pub r1310_queue_wait_max_ns:u64 }
pub struct Health {
 #[cfg(test)] pub(crate) r1306_task_exit:AtomicU64,
 stopped:AtomicBool, incident:Mutex<Option<Denial>>, metrics:Mutex<Metrics>,
 last_input:Mutex<[Instant;2]>, heartbeat:Mutex<Instant>, watchdog_tick:Mutex<Instant>, last_clock:AtomicU64,
 #[cfg(test)] test_clock:Mutex<Option<fn()->(u64,i64)>>,
 #[cfg(test)] test_last_input_ns:Mutex<[u64;2]>,
 #[cfg(test)] test_heartbeat_ns:AtomicU64,
 #[cfg(test)] test_watchdog_ns:AtomicU64,
}
impl Health {
 pub fn new()->Arc<Self>{Arc::new(Self{#[cfg(test)] r1306_task_exit:AtomicU64::new(0),stopped:AtomicBool::new(false),incident:Mutex::new(None),metrics:Mutex::new(Metrics::default()),last_input:Mutex::new([Instant::now();2]),heartbeat:Mutex::new(Instant::now()),watchdog_tick:Mutex::new(Instant::now()),last_clock:AtomicU64::new(0),#[cfg(test)] test_clock:Mutex::new(None),#[cfg(test)] test_last_input_ns:Mutex::new([0;2]),#[cfg(test)] test_heartbeat_ns:AtomicU64::new(0),#[cfg(test)] test_watchdog_ns:AtomicU64::new(0)})}
 pub fn trip(&self,d:Denial){self.stopped.store(true,Ordering::SeqCst);if let Ok(mut x)=self.incident.lock(){if x.is_none(){*x=Some(d)}}}
 pub fn healthy(&self)->bool{
  #[cfg(test)] if let Some(clock)=self.test_clock.lock().ok().and_then(|clock|*clock){let now=clock().0;let tick=self.test_watchdog_ns.load(Ordering::SeqCst);return !self.stopped.load(Ordering::SeqCst)&&now.checked_sub(tick).is_some_and(|age|age<=800_000_000);}
  !self.stopped.load(Ordering::SeqCst)&&self.watchdog_tick.lock().is_ok_and(|t|t.elapsed()<=Duration::from_millis(800))
 }
 #[cfg(test)] pub(crate) fn set_test_clock(&self,clock:fn()->(u64,i64)){let now=clock().0;if let Ok(mut current)=self.test_clock.lock(){*current=Some(clock);}if let Ok(mut last)=self.test_last_input_ns.lock(){*last=[now;2];}self.test_heartbeat_ns.store(now,Ordering::SeqCst);self.test_watchdog_ns.store(now,Ordering::SeqCst);}
 #[cfg(test)] pub(crate) fn record_test_input(&self,venue:Venue,now_ns:u64){let i=if venue==Venue::OkxSpot{0}else{1};if let Ok(mut last)=self.test_last_input_ns.lock(){last[i]=now_ns;}if let Ok(mut last)=self.last_input.lock(){last[i]=Instant::now();}}
 pub fn incident(&self)->Option<Denial>{self.incident.lock().ok().and_then(|x|x.clone())}
 pub fn metrics(&self)->serde_json::Value{self.metrics.lock().map(|x|serde_json::to_value(&*x).unwrap_or_default()).unwrap_or_default()}
 pub fn record_r1310_trace(&self,dropped:bool){if let Ok(mut m)=self.metrics.lock(){if dropped{m.r1310_trace_dropped=m.r1310_trace_dropped.saturating_add(1)}else{m.r1310_trace_records=m.r1310_trace_records.saturating_add(1)}}}
 pub fn heartbeat(&self){
  #[cfg(test)] if let Some(clock)=self.test_clock.lock().ok().and_then(|clock|*clock){self.test_heartbeat_ns.store(clock().0,Ordering::SeqCst);return;}
  if let Ok(mut x)=self.heartbeat.lock(){*x=Instant::now()}else{self.trip(Denial::Consumer)}
 }
 pub fn watchdog(&self,now_ns:u64){self.watchdog_inner(now_ns,true)}
 // Startup/reset: monitor consumer and clocks even before both legs exist.
 // Full two-leg freshness is still mandatory before execution.
 fn watchdog_inner(&self,now_ns:u64,require_pair:bool){
  if let Ok(mut t)=self.watchdog_tick.lock(){*t=Instant::now()}else{self.trip(Denial::Consumer)}
  let prior=self.last_clock.swap(now_ns,Ordering::SeqCst);
  if now_ns==0||(prior!=0&&now_ns<=prior){self.trip(Denial::Clock)}
  #[cfg(test)] if self.test_clock.lock().ok().is_some_and(|clock|clock.is_some()){
   self.test_watchdog_ns.store(now_ns,Ordering::SeqCst);
   let stale=|then:u64|now_ns.checked_sub(then).map_or(true,|age|age>800_000_000);
   if stale(self.test_heartbeat_ns.load(Ordering::SeqCst)){self.trip(Denial::Consumer)}
   if require_pair&&self.test_last_input_ns.lock().map_or(true,|last|last.iter().any(|then|stale(*then))){self.trip(Denial::Stale)}
   return;
  }
  if self.heartbeat.lock().map_or(true,|x|x.elapsed()>Duration::from_millis(800)){self.trip(Denial::Consumer)}
  if require_pair&&self.last_input.lock().map_or(true,|x|x.iter().any(|t|t.elapsed()>Duration::from_millis(800))){self.trip(Denial::Stale)}
 }
}
struct Envelope { input:ValidatedInput, queued:Instant, queued_mono_ns:Option<u64> }
pub struct BridgeReceiver { rx:Receiver<Envelope>, health:Arc<Health>, authority:Authority }
struct BridgeSender { tx:SyncSender<Envelope>,health:Arc<Health> }
fn channel(authority:Authority)->(BridgeSender,BridgeReceiver,Arc<Health>){
 let(tx,rx)=sync_channel(CAPACITY);let health=Health::new();
 (BridgeSender{tx,health:health.clone()},BridgeReceiver{rx,health:health.clone(),authority},health)
}
impl BridgeSender {
 fn send(&self,mut input:ValidatedInput)->Result<(),Denial>{
  let sampled=input.r1310_timing.as_ref().is_some_and(|t|t.r1310_trace_sampled);
  let start=sampled.then(crate::ingress_runtime_r1205::mono_ns);
  let mut m=self.health.metrics.lock().map_err(|_|Denial::Consumer)?;
  let depth=m.enqueued.saturating_sub(m.dequeued)as usize;
  let end=sampled.then(crate::ingress_runtime_r1205::mono_ns);
  if let (Some(start),Some(end),Some(t))=(start,end,input.r1310_timing.as_mut()){
   t.bridge_enqueue_start_ns=Some(start);t.bridge_enqueue_end_ns=Some(end);t.bridge_queue_depth=Some(depth);
   let mut event=r1310_stage_event("BridgeEnqueue",&input.trace_id,start,end);event["span_kind"]=serde_json::json!("pre_enqueue_service");input.trace.push(event);
  }else{input.trace.push(serde_json::json!({"stage":"BridgeEnqueue","trace_id":input.trace_id}));}
  match self.tx.try_send(Envelope{input,queued:Instant::now(),queued_mono_ns:end}){
   Ok(())=>{m.enqueued+=1;m.high_water=m.high_water.max((m.enqueued-m.dequeued)as usize);if sampled{m.r1310_trace_records=m.r1310_trace_records.saturating_add(1);}Ok(())},
   Err(TrySendError::Full(_))=>{m.overflow+=1;m.rejected+=1;m.dropped+=1;if sampled{m.r1310_trace_dropped=m.r1310_trace_dropped.saturating_add(1);}self.health.trip(Denial::Overflow);Err(Denial::Overflow)},
   Err(TrySendError::Disconnected(_))=>{m.rejected+=1;m.dropped+=1;if sampled{m.r1310_trace_dropped=m.r1310_trace_dropped.saturating_add(1);}self.health.trip(Denial::Consumer);Err(Denial::Consumer)}
  }
 }
}
// Historical admission never issues a live Permit. Require the exact
// originally selected frame and a proven ordinary successor in the
// same live selection/connection lineage. No fallback for a reset,
// duplicate, corrupt book, changed source, or absent current authority.
fn ordinary_observation(f:&RawFrame,u:&Update,p:&crate::market_data_ingress_r1205::Permit,n:u64,w:i64)->bool{
 let Some(s)=f.source.as_ref()else{return false};
 let Some(old)=s.selected_permit.as_ref()else{return false};
 let (Some(a),Some(b))=(&old.snapshot_witness,&p.snapshot_witness)else{return false};
 let Some(event)=u.book.exchange_ts_ms else{return false};
 let fresh=|receive:u64,event:i64|n.checked_sub(receive).is_some_and(|age|age<=MAX_AGE_NS)
   &&w.checked_sub(event).is_some_and(|age|(0..=800).contains(&age));
 f.venue==Venue::BinanceFutures&&old.session==s.session_id&&p.session==old.session
  &&old.epoch==f.epoch&&p.epoch==old.epoch&&s.selection_epoch==f.epoch
  &&old.standby_session==p.standby_session&&p.session!=p.standby_session
  &&old.received_ns==f.received_ns&&old.event_ms==event&&p.received_ns>=old.received_ns&&p.event_ms>=event
  &&a.identity_matches(f.venue,&u.base,&s.session_id,s.connection_epoch)&&a.ordinarily_replaced_by(b)
  &&s.selected_ns.is_some_and(|at|at>=f.received_ns&&at<=n)
  &&fresh(f.received_ns,event)&&fresh(old.standby_received_ns,old.standby_event_ms)
  &&fresh(p.received_ns,p.event_ms)&&fresh(p.standby_received_ns,p.standby_event_ms)
}
impl BridgeReceiver {
 // Only a newer, validator-minted authority can retire a queued or in-flight input.
 // An invalid current-generation input still takes the sticky incident path.
 pub fn superseded(&self,input:&ValidatedInput)->bool{
  let Ok(a)=self.authority.read()else{return false};
  let Some(p)=a.get(&crate::market_data_ingress_r1205::key(input.venue,&input.instrument))else{return false};
  let Some(old)=input.witness.as_ref()else{return false};
  let Some(current)=p.snapshot_witness.as_ref()else{return false};
  // Epochs belong to an Engine selection history and may restart lower after a valid
  // Engine replacement. Retire only a different, live validator witness whose scope
  // agrees with the currently published Permit; a numeric epoch comparison is unsafe.
  p.session!=p.standby_session
   &&old.identity_matches(input.venue,&input.instrument,&input.source,input.connection_generation)
   &&current.live_for(input.venue,&input.instrument,&p.session)
   &&!old.same_identity(current)
 }
 pub fn revalidate(&self,input:&ValidatedInput)->Result<(),Denial>{
    let n=crate::ingress_runtime_r1205::mono_ns();
    let w=crate::ingress_runtime_r1205::wall_ms();
    self.revalidate_at(input,n,w)
 }
 // Internal deterministic clock seam; production always samples clocks above.
 fn revalidate_at(&self,input:&ValidatedInput,n:u64,w:i64)->Result<(),Denial>{
    input.fresh(n,w)?;
    let a=self.authority.read().map_err(|_|Denial::Hot)?;
    let p=a.get(&crate::market_data_ingress_r1205::key(input.venue,&input.instrument)).ok_or(Denial::Hot)?;
    if p.session!=input.source||p.epoch!=input.selection_generation||p.standby_session!=input.standby||p.session==p.standby_session{return Err(Denial::Generation)}
    if let Some(w)=&input.witness{
     if !w.matches(input.venue,&input.instrument,&input.source,input.connection_generation)
      ||!p.snapshot_witness.as_ref().is_some_and(|current|w.same_generation(current)){return Err(Denial::Generation)}
    }else if input.snapshot_generation.is_some(){return Err(Denial::Generation)}
    if n<p.standby_received_ns||n-p.standby_received_ns>MAX_AGE_NS||w.checked_sub(p.standby_event_ms).map_or(true,|x|!(0..=800).contains(&x)){return Err(Denial::Hot)}
    Ok(())
 }
 fn observation_at(&self,input:&ValidatedInput,n:u64,w:i64)->Result<bool,Denial>{
  match self.revalidate_at(input,n,w){
   Ok(())=>Ok(true),
   Err(Denial::Generation)=>{
    let a=self.authority.read().map_err(|_|Denial::Hot)?;
    let p=a.get(&crate::market_data_ingress_r1205::key(input.venue,&input.instrument)).ok_or(Denial::Hot)?;
    let u=Update{venue:input.venue,base:input.instrument.clone(),book:input.book.clone()};
    if ordinary_observation(&input.source_frame,&u,p,n,w){Ok(false)}else{Err(Denial::Generation)}
   },Err(e)=>Err(e),
  }
 }
 pub fn receive(&self)->Result<Option<ValidatedInput>,Denial>{
  self.health.heartbeat();
  match self.rx.recv_timeout(Duration::from_millis(25)){
   Ok(mut e)=>{
    let dequeue_start=crate::ingress_runtime_r1205::mono_ns();
    let queue_wait=e.queued_mono_ns.and_then(|queued|dequeue_start.checked_sub(queued));
    let age=e.queued.elapsed();let mut m=self.health.metrics.lock().map_err(|_|Denial::Consumer)?;
    m.dequeued+=1;m.residence_max_ns=m.residence_max_ns.max(age.as_nanos().min(u64::MAX as u128)as u64);
    let queue_depth=m.enqueued.saturating_sub(m.dequeued)as usize;
    if let Some(wait)=queue_wait{m.r1310_queue_wait_total_ns=m.r1310_queue_wait_total_ns.saturating_add(wait);m.r1310_queue_wait_max_ns=m.r1310_queue_wait_max_ns.max(wait);}
    if age>RESIDENCE{m.rejected+=1;if e.input.r1310_timing.is_some(){m.r1310_trace_dropped=m.r1310_trace_dropped.saturating_add(1);}drop(m);self.health.trip(Denial::Residence);return Err(Denial::Residence)}drop(m);
    #[cfg(test)] r1306_test_support::checkpoint("dequeue");
    // Fresh clocks must be sampled after the wait, not supplied before recv_timeout.
    if let Err(reason)=self.revalidate(&e.input){
     if let Ok(mut m)=self.health.metrics.lock(){m.rejected+=1;m.dropped+=1;if e.input.r1310_timing.is_some(){m.r1310_trace_dropped=m.r1310_trace_dropped.saturating_add(1);}}
     if reason==Denial::Generation && self.superseded(&e.input){return Ok(None)}
     self.health.trip(reason.clone());return Err(reason)
    }
    let dequeue_end=crate::ingress_runtime_r1205::mono_ns();
    if let Some(t)=e.input.r1310_timing.as_mut(){t.bridge_dequeue_start_ns=Some(dequeue_start);t.bridge_dequeue_end_ns=Some(dequeue_end);t.bridge_queue_wait_ns=queue_wait;t.bridge_queue_depth=Some(queue_depth);let mut event=r1310_stage_event("BridgeDequeue",&e.input.trace_id,dequeue_start,dequeue_end);event["queue_residence_ns"]=queue_wait.map_or(serde_json::Value::Null,|v|serde_json::json!(v));event["queue_depth"]=serde_json::json!(queue_depth);e.input.trace.push(event);if let Ok(mut m)=self.health.metrics.lock(){m.r1310_trace_records=m.r1310_trace_records.saturating_add(1);}}
    else{e.input.trace.push(serde_json::json!({"stage":"BridgeDequeue","trace_id":e.input.trace_id}));}
    Ok(Some(e.input))
   },
   Err(std::sync::mpsc::RecvTimeoutError::Timeout)=>Ok(None),
   Err(std::sync::mpsc::RecvTimeoutError::Disconnected)=>Err(Denial::Closed)
  }
 }
}
struct Provider { authority:Authority, clock_domain:String, seen:HashMap<String,(u64,u64,i64,String)> }
impl Provider {
 // A selection Reset is recoverable only when the Engine has published its
 // matching authority. It revokes the old output generation; it grants no entry.
 fn selection_reset(&mut self,venue:Venue,base:&str,epoch:u64)->Result<(),Denial>{
  let k=crate::market_data_ingress_r1205::key(venue,base);
  let a=self.authority.read().map_err(|_|Denial::Generation)?;
  let p=a.get(&k).ok_or(Denial::Generation)?;
  if p.epoch!=epoch||p.session==p.standby_session||p.snapshot_witness.is_none(){return Err(Denial::Generation)}
  drop(a);
  self.seen.remove(&k);
  Ok(())
 }
 fn accept(&mut self,output:Output,now:u64,wall:i64)->Result<ValidatedInput,Denial>{
  self.accept_mode(output,now,wall,false)
 }
 fn accept_mode(&mut self,output:Output,now:u64,wall:i64,observation:bool)->Result<ValidatedInput,Denial>{
  let(f,u)=match output{
   Output::Book(f,u)=>(f,u),
   // This is an Engine-selected full snapshot, not an incoming WS raw message.
   Output::Frame(f)=>{let u=crate::parse(&f).map_err(|_|Denial::Raw)?;(f,u)},
   Output::Reset{..}|Output::Resubscribe{..}=>return Err(Denial::Generation),
   Output::Blocked{..}=>return Err(Denial::Hot)
  };
  self.bind_mode(f,u,now,wall,observation)
 }
 fn bind(&mut self,f:RawFrame,u:Update,now:u64,wall:i64)->Result<ValidatedInput,Denial>{
  self.bind_mode(f,u,now,wall,false)
 }
 fn bind_mode(&mut self,f:RawFrame,u:Update,now:u64,wall:i64,observation:bool)->Result<ValidatedInput,Denial>{
  let source=f.source.as_ref().ok_or(Denial::Raw)?;
  let sampled=source.r1279_timing.as_ref().is_some_and(|t|t.r1310_trace_sampled);
  let provider_start=sampled.then(crate::ingress_runtime_r1205::mono_ns);
  let mut r1310_timing=source.r1279_timing.clone().filter(|t|t.r1310_trace_sampled);
  let event=u.book.exchange_ts_ms.ok_or(Denial::Timestamp)?;
  let seq=u.book.sequence.ok_or(Denial::Sequence)?;
  if f.received_ns==0||u.book.received_ns!=f.received_ns||u.venue!=f.venue||u.book.epoch!=f.epoch||seq<0{return Err(Denial::Timestamp)}
  let permit=authorization_at_normalized(&self.authority,&f,&u,now,wall).ok_or(Denial::Hot)?;
  // The live Permit may advance while an already selected frame waits in the queue.
  // Bind to the original selection, but still require its exact live validator witness
  // to match current authority. Never relabel an old snapshot with a new witness.
  let selected=source.selected_permit.as_ref().unwrap_or(&permit);
  let same_witness=match (&selected.snapshot_witness,&permit.snapshot_witness){
   (Some(old),Some(current))=>old.same_generation(current),(None,None)=>true,_=>false
  };
  let historical=observation&&!same_witness&&ordinary_observation(&f,&u,&permit,now,wall);
  if selected.session!=permit.session||selected.epoch!=permit.epoch||(!same_witness&&!historical){
   crate::pipeline_diagnostics_r1325::record("ProviderSelectedWitness",0,false);return Err(Denial::Generation)
  }
  let witness=selected.snapshot_witness.clone();
  if let Some(w)=&witness{
   if !(if historical{w.identity_matches(f.venue,&u.base,&source.session_id,source.connection_epoch)}else{w.matches(f.venue,&u.base,&source.session_id,source.connection_epoch)})
    ||selected.received_ns!=f.received_ns||selected.event_ms!=event{
    crate::pipeline_diagnostics_r1325::record("ProviderSelectedFrameBinding",0,false);return Err(Denial::Generation)
   }
  }
  let k=crate::market_data_ingress_r1205::key(f.venue,&u.base);
  let payload=crate::native_paper_hedge_r1296::digest(&(&u.book.bids,&u.book.asks)).map_err(|_|Denial::Raw)?;
  let current=(source.connection_epoch,f.epoch,seq,payload);
  if let Some(old)=self.seen.get(&k){
   if current==*old{return Err(Denial::Duplicate)}
   if current.0==old.0&&current.1==old.1&&current.2==old.2{return Err(Denial::Sequence)}
   if current.0<old.0||current.1<old.1{return Err(Denial::Generation)}
   if current.0==old.0&&current.1==old.1&&current.2<old.2{return Err(Denial::Sequence)}
  }
  let trace_id=crate::market_data_ingress_r1205::frame_trace_id_r1304(&f);
  let mut input=ValidatedInput{source_frame:f.clone(),source_provenance:source.clone(),trace_id:trace_id.clone(),trace:Vec::with_capacity(8),r1310_timing:r1310_timing.take(),venue:f.venue,venue_symbol:if f.venue==Venue::OkxSpot{format!("{}-USDT",u.base)}else{format!("{}USDT",u.base)},instrument:u.base,market_type:match f.venue{Venue::OkxSpot=>MarketType::Spot,Venue::BinanceFutures=>MarketType::Futures},
   book:u.book,exchange_ms:event,receive_ns:f.received_ns,validated_ns:now,sequence:seq,
   selection_generation:f.epoch,snapshot_generation:witness.as_ref().map(|w|w.generation()),witness,connection_generation:source.connection_epoch,source:source.session_id.clone(),
   standby:permit.standby_session,hot_at_validation:!historical,fresh_at_validation:true,clock_domain:self.clock_domain.clone(),rules_identity:None};
  input.fresh(now,wall)?;
  let provider_end=sampled.then(crate::ingress_runtime_r1205::mono_ns);
  if let (Some(start),Some(end),Some(t))=(provider_start,provider_end,input.r1310_timing.as_mut()){t.provider_start_ns=Some(start);t.provider_end_ns=Some(end);}
  if let Some(t)=input.r1310_timing.as_ref(){
   if let (Some(start),Some(end))=(t.engine_receive_begin_ns,t.engine_receive_end_ns){input.trace.push(r1310_stage_event("Engine",&trace_id,start,end));}
   if let (Some(start),Some(end))=(t.source_durable_start_ns,t.source_durable_end_ns){let stage=if t.source_storage_durable==Some(true){"EngineStorage"}else{"EngineCustody"};let mut row=r1310_stage_event(stage,&trace_id,start,end);row["span_kind"]=serde_json::json!(if t.source_storage_durable==Some(true){"storage_service"}else{"volatile_custody_service"});input.trace.push(row);}
   if let (Some(start),Some(end))=(provider_start,provider_end){input.trace.push(r1310_stage_event("Provider",&trace_id,start,end));}
  }else{input.trace.push(serde_json::json!({"stage":"Provider","trace_id":trace_id}));}
  self.seen.insert(k,current);Ok(input)
 }
}
/// Admission is separate from observation. No real-rule attestation constructor in R1301.
pub fn order_admission(health:&Health,owner_healthy:bool,storage_healthy:bool,real_off:bool,kill_engaged:bool,synthetic_permitted:bool)->Result<(),Denial>{
 admission_prerequisites(health,owner_healthy,storage_healthy,real_off,kill_engaged,synthetic_permitted)?;
 Err(Denial::Rules)
}
fn admission_prerequisites(health:&Health,owner_healthy:bool,storage_healthy:bool,real_off:bool,kill_engaged:bool,synthetic_permitted:bool)->Result<(),Denial>{
 if !real_off{return Err(Denial::Real)}if !kill_engaged{return Err(Denial::Kill)}
 if !health.healthy(){return Err(health.incident().unwrap_or(Denial::Provider))}
 if !owner_healthy{return Err(Denial::Owner)}if !storage_healthy{return Err(Denial::Storage)}
 if !synthetic_permitted{return Err(Denial::Policy)}
 Ok(())
}
// R1321 checks are shared by the accepted offline path and explicit simulated R1325 mode.
#[allow(dead_code)]
#[path="native_paper_admission_r1321.rs"]
mod admission_r1321;
/// Reuses the existing UCM/Engine public transport and reconstruction, no private API.
pub fn public_observation(base:String,clock_domain:String,duration:Duration)->Result<(BridgeReceiver,Arc<Health>,tokio::task::JoinHandle<Result<serde_json::Value,String>>),String>{
 if base.is_empty()||base.len()>20||!base.bytes().all(|b|b.is_ascii_uppercase()||b.is_ascii_digit())||clock_domain.is_empty()||duration>Duration::from_secs(60)||duration.is_zero(){return Err("R1301 observation scope".into())}
 validate_public_endpoints(&base).map_err(|e|format!("{e:?}"))?;
 let authority:Authority=Default::default();let(tx,rx,health)=channel(authority.clone());let h=health.clone();
 let task=tokio::spawn(async move{
  let(out,mut selected)=tokio::sync::mpsc::channel(crate::ingress_runtime_r1205::R1205_OUTPUT_QUEUE_CAPACITY);
  let run=tokio::spawn(crate::ingress_runtime_r1205::run_r1246_volatile(crate::ingress_runtime_r1205::Config{
   shard:0,bases:vec![base],copies:2,duration,controlled_close_after:None,endpoint_variant:0,probe_rotation_after:None
  },authority.clone(),out));
  let mut provider=Provider{authority,clock_domain,seen:HashMap::new()};
  while let Some(output)=selected.recv().await {
   if let Output::Reset{venue,ref base,epoch}=output {
    if let Err(e)=provider.selection_reset(venue,base,epoch){h.trip(e)}
    continue
   }
   match provider.accept(output,crate::ingress_runtime_r1205::mono_ns(),crate::ingress_runtime_r1205::wall_ms()){
    Ok(input)=>{if let Ok(mut t)=h.last_input.lock(){t[if input.venue==Venue::OkxSpot{0}else{1}]=Instant::now()}if tx.send(input).is_err(){h.trip(Denial::Overflow)}},
    Err(Denial::Duplicate)=>{if let Ok(mut m)=h.metrics.lock(){m.duplicates+=1}},
    Err(e)=>{if let Ok(mut m)=h.metrics.lock(){m.rejected+=1}h.trip(e)}
   }
  }
  let result=run.await.map_err(|e|e.to_string())?.map(|summary|serde_json::json!({"engine_trace":summary.source_audit}));
  h.trip(Denial::Closed);result
 });
 Ok((rx,health,task))
}

pub fn validate_public_endpoints(base:&str)->Result<(),Denial>{
 let b=crate::universal_connection_manager_r1209::adapter_for(Venue::BinanceFutures);
 let o=crate::universal_connection_manager_r1209::adapter_for(Venue::OkxSpot);
 if b.channel()!="depth20@100ms"||o.channel()!="books"||
  b.endpoint(&[base.into()])!=format!("wss://fstream.binance.com/public/stream?streams={}usdt@depth20@100ms",base.to_ascii_lowercase())||
  o.endpoint(&[base.into()])!="wss://ws.okx.com/ws/v5/public"{return Err(Denial::Endpoint)}
 Ok(())
}
#[cfg(test)]
#[path="native_paper_bridge_r1301_tests.rs"]
mod tests;

/// Available only to test compilations. Production has no fixture minting API.
#[cfg(test)]
pub(crate) fn fixture_pair_for_test(clock:&str)->([ValidatedInput;2],BridgeReceiver,Arc<Health>){
 let(mut p,f,u)=tests::fixture(Venue::OkxSpot);p.clock_domain=clock.into();
 let n=f.received_ns;let w=f.received_wall_ms;let spot=p.bind(f,u,n,w).unwrap();
 let(_,f,u)=tests::fixture(Venue::BinanceFutures);let n=f.received_ns;let w=f.received_wall_ms;
 let future=p.bind(f,u,n,w).unwrap();let(_,rx,h)=channel(p.authority);([spot,future],rx,h)
}

#[cfg(test)]
#[path="native_paper_engine_fixture_r1301a.rs"]
pub(crate) mod engine_fixture;

#[cfg(test)]
#[path="native_paper_engine_negative_r1301a.rs"]
mod engine_negative;

#[cfg(test)]
pub(crate) mod r1306_test_support {
 use super::*;
 thread_local! {static HOOK:std::cell::RefCell<Option<Box<dyn FnMut(&str)>>> = std::cell::RefCell::new(None);}
 thread_local! {static KILL:std::cell::Cell<Option<bool>>=const {std::cell::Cell::new(None)};}
 pub(crate) struct KillGuard;
 impl Drop for KillGuard{fn drop(&mut self){KILL.with(|v|v.set(None));}}
 pub(crate) fn kill_override(value:bool)->KillGuard{KILL.with(|v|{assert!(v.get().is_none());v.set(Some(value));});KillGuard}
 pub(crate) fn kill_value()->Option<bool>{KILL.with(|v|v.get())}
 pub(crate) fn install(h:impl FnMut(&str)+'static){HOOK.with(|v|{assert!(v.borrow().is_none());*v.borrow_mut()=Some(Box::new(h));});}
 pub(crate) fn checkpoint(stage:&str){HOOK.with(|v|{if let Some(h)=v.borrow_mut().as_mut(){h(stage)}});}
 pub(crate) fn last_tick(health:&Health)->u64{health.last_clock.load(Ordering::SeqCst)}
 pub(crate) fn age(health:&Health,heartbeat:bool,producer:bool,lease:bool){
  let old=Instant::now()-Duration::from_millis(900);
  if heartbeat{*health.heartbeat.lock().unwrap()=old;}
  if producer{*health.last_input.lock().unwrap()=[old;2];}
  if lease{*health.watchdog_tick.lock().unwrap()=old;}
 }
 pub(crate) fn corrupt(input:&ValidatedInput,kind:&str)->ValidatedInput{
  let mut i=input.clone();
  match kind{
   "stale"=>{i.receive_ns=i.receive_ns.saturating_sub(900_000_000);i.exchange_ms-=900;},
   "witness"=>i.connection_generation+=1,
   "generation"=>i.selection_generation+=1,
   _=>panic!("unsupported input fault")
  }i
 }
}

// Shared typed PAPER checks. Return rule digests only; never a real-order capability.
pub fn review_pair_r1322(inputs:&[ValidatedInput;2],identity:&crate::native_paper_hedge_r1296::Identity,generation:u64,ticks:&[String;2],quantity:f64,receiver:&BridgeReceiver,health:&Health,now:u64,wall:i64)->Result<[String;2],Denial>{
 admission_r1321::review_pair_r1322(inputs,identity,generation,ticks,quantity,receiver,health,now,wall)
}
pub fn revalidate_r1322(input:&ValidatedInput,receiver:&BridgeReceiver,now:u64,wall:i64)->Result<(),Denial>{receiver.revalidate_at(input,now,wall)}
/// True = currently authorized; false = historical observation only.
pub fn observation_r1410(input:&ValidatedInput,receiver:&BridgeReceiver,now:u64,wall:i64)->Result<bool,Denial>{receiver.observation_at(input,now,wall)}

/// R1325 reuses Provider/receiver validation for the existing durable selected-output loop.
/// No transport or execution capability is created here.
pub struct PaperSource1325 { provider:Provider, receiver:BridgeReceiver, health:Arc<Health>, seen_venues:[bool;2], reset_epochs:HashMap<String,u64> }
impl PaperSource1325 {
 pub fn new(authority:Authority,clock_domain:String)->Self {
  let(_,receiver,health)=channel(authority.clone());
  Self{provider:Provider{authority,clock_domain,seen:HashMap::new()},receiver,health,seen_venues:[false;2],reset_epochs:HashMap::new()}
 }
 pub fn receiver(&self)->&BridgeReceiver{&self.receiver}
 pub fn health(&self)->&Health{&self.health}
 pub fn ready(&self)->bool{self.seen_venues.iter().all(|v|*v)}
 pub fn accept(&mut self,output:Output,now:u64,wall:i64)->Result<ValidatedInput,Denial>{
  self.accept_mode(output,now,wall,false)
 }
 pub fn accept_observation(&mut self,output:Output,now:u64,wall:i64)->Result<ValidatedInput,Denial>{
  self.accept_mode(output,now,wall,true)
 }
 fn accept_mode(&mut self,output:Output,now:u64,wall:i64,observation:bool)->Result<ValidatedInput,Denial>{
  // A queued Reset is an invalidation fence, never a grant of live authority.
  // Every subsequent frame must match that fence and still pass Provider/HOT/witness checks.
  let scope=match &output{Output::Book(f,u)=>Some((f.venue,u.base.clone(),f.epoch)),Output::Frame(f)=>crate::parse(f).ok().map(|u|(f.venue,u.base,f.epoch)),_=>None};
  if let Some((venue,base,epoch))=scope{if self.reset_epochs.get(&crate::market_data_ingress_r1205::key(venue,&base)).is_some_and(|fence|*fence!=epoch){return Err(Denial::Generation)}}
  let input=self.provider.accept_mode(output,now,wall,observation)?;let i=if input.venue==Venue::OkxSpot{0}else{1};
  self.seen_venues[i]=true;
  if let Ok(mut last)=self.health.last_input.lock(){last[i]=Instant::now()}else{return Err(Denial::Consumer)}
  #[cfg(test)] self.health.record_test_input(input.venue,now);
  self.health.heartbeat();self.health.watchdog_inner(now,self.ready());
  Ok(input)
 }
 pub fn reset(&mut self,venue:Venue,base:&str,epoch:u64)->Result<(),Denial>{
  let k=crate::market_data_ingress_r1205::key(venue,base);
  if epoch==0||self.reset_epochs.get(&k).is_some_and(|prior|epoch<=*prior)||self.provider.seen.get(&k).is_some_and(|prior|epoch<=prior.1){return Err(Denial::Generation)}
  // Only the existing Engine output loop calls this method. Live authority may
  // already be revoked/advanced while Reset waits; that must not prevent invalidation.
  self.reset_epochs.insert(k.clone(),epoch);self.provider.seen.remove(&k);
  self.seen_venues[if venue==Venue::OkxSpot{0}else{1}]=false;Ok(())
 }
}

#[cfg(test)]
#[test]
fn r1325_reset_without_current_authority_only_invalidates_and_fences(){
 let mut f=engine_fixture::EngineFixture::for_single_venue("r1325-reset-fence",Venue::OkxSpot);f.qualify_single_venue(Venue::OkxSpot);
 let input=f.inputs.last().unwrap().clone();let authority=f.receiver.authority.clone();
 let mut source=PaperSource1325::new(authority.clone(),"r1325-reset-fence".into());
 let output=||Output::Book(input.source_frame.clone(),Update{base:input.instrument.clone(),venue:input.venue,book:input.book.clone()});
 let n=crate::ingress_runtime_r1205::mono_ns();let w=crate::ingress_runtime_r1205::wall_ms();let epoch=input.selection_generation;
 authority.write().unwrap().clear();
 source.reset(Venue::OkxSpot,"AAA",epoch).unwrap();
 assert_eq!(source.accept(output(),n,w).unwrap_err(),Denial::Hot);
 assert_eq!(source.reset(Venue::OkxSpot,"AAA",epoch).unwrap_err(),Denial::Generation);
 source.reset(Venue::OkxSpot,"AAA",epoch+1).unwrap();
 assert_eq!(source.accept(output(),n,w).unwrap_err(),Denial::Generation);
 assert_eq!(source.reset(Venue::OkxSpot,"AAA",epoch).unwrap_err(),Denial::Generation);
 assert!(!source.ready());
}


#[cfg(test)]
#[test]
fn r1410b_integrated_provider_observation_only_old_snapshot(){
 let mut f=engine_fixture::EngineFixture::for_single_venue("r1410b-provider",Venue::BinanceFutures);
 f.qualify_single_venue(Venue::BinanceFutures);
 let old=f.inputs[0].clone();
 let (old_frame,old_update)=old.owner_parts_r1322();
 let n=crate::ingress_runtime_r1205::mono_ns().max(old_frame.received_ns+1_000_000);
 let w=crate::ingress_runtime_r1205::wall_ms().max(old_frame.received_wall_ms+1);
 let seq=old_update.book.sequence.unwrap()+1;
 let next=f.feed_r1322(["a","b"].into_iter().map(|copy|engine_fixture::raw(Venue::BinanceFutures,&format!("binance-{copy}"),"AAA",n,w,seq,true)).collect(),n,w).remove(0);
 let mut source=PaperSource1325::new(f.receiver.authority.clone(),"r1410b-provider".into());
 source.reset(Venue::BinanceFutures,"AAA",old.selection_generation).unwrap();
 let output=||Output::Book(old_frame.clone(),old_update.clone());
 assert_eq!(source.accept(output(),n,w).unwrap_err(),Denial::Generation);
 let historical=source.accept_observation(output(),n,w).unwrap();
 assert_eq!(historical.input_id(),old.input_id());
 assert!(!historical.hot_at_validation);
 assert_eq!(source.receiver().revalidate_at(&historical,n,w),Err(Denial::Generation));
 assert_eq!(source.receiver().observation_at(&historical,n,w),Ok(false));
 let (frame,update)=next.owner_parts_r1322();
 let current=source.accept_observation(Output::Book(frame,update),n,w).unwrap();
 assert!(current.hot_at_validation);
 assert_eq!(source.receiver().revalidate_at(&current,n,w),Ok(()));
 // Current authority revoked: prior observation cannot borrow recovery.
 f.engine.mark_reconnecting(&current.source);
 assert!(source.receiver().observation_at(&historical,n,w).is_err());
 assert!(source.receiver().revalidate_at(&historical,n,w).is_err());
 println!("R1410B_PROVIDER exact old event observed, never live; current event admitted; destructive loss denied");
}


#[cfg(test)]
#[test]
fn r1410b_old_observation_cannot_borrow_destructive_recovery(){
 let mut f=engine_fixture::EngineFixture::for_single_venue("r1410b-lineage",Venue::BinanceFutures);
 f.qualify_single_venue(Venue::BinanceFutures);
 let a=f.inputs[0].clone();
 let (frame,update)=a.owner_parts_r1322();
 let n=crate::ingress_runtime_r1205::mono_ns().max(frame.received_ns+1_000_000);
 let w=crate::ingress_runtime_r1205::wall_ms().max(frame.received_wall_ms+1);
 // All stages must use the same deterministic time. The default
 // Engine clock samples real time and rejects future synthetic frames.
 thread_local!{static CLOCK:std::cell::Cell<(u64,i64)>=const{std::cell::Cell::new((0,0))};}
 fn clock()->(u64,i64){CLOCK.with(|c|c.get())}
 let at=|ns,wall|CLOCK.with(|c|c.set((ns,wall)));
 at(n,w);f.engine.set_test_clock(clock);f.health.set_test_clock(clock);
 let seq=update.book.sequence.unwrap()+1;
 let batch=|n,w,seq|["a","b"].into_iter().map(|copy|engine_fixture::raw(Venue::BinanceFutures,&format!("binance-{copy}"),"AAA",n,w,seq,true)).collect::<Vec<_>>();
 let b=f.feed_r1322(batch(n,w,seq),n,w).remove(0);
 assert_eq!(f.receiver.observation_at(&a,n,w),Ok(false));
 // A repeated sequence destructively revokes B. A later normal frame
 // must mint a new continuity, even if selection/session did not change.
 at(n+1_000_000,w+1);
 assert!(f.feed_r1322(batch(n,w,seq),n+1_000_000,w+1).is_empty());
 assert!(!b.witness.as_ref().unwrap().is_live(),"duplicate must actually revoke witness");
 at(n+2_000_000,w+2);
 let c=f.feed_r1322(batch(n+2_000_000,w+2,seq+1),n+2_000_000,w+2).remove(0);
 assert_eq!(a.source,c.source);assert_eq!(a.selection_generation,c.selection_generation);
 assert_eq!(f.receiver.revalidate_at(&c,n+2_000_000,w+2),Ok(()));
 assert!(a.fresh(n+2_000_000,w+2).is_ok());
 assert!(f.receiver.observation_at(&a,n+2_000_000,w+2).is_err());
 assert!(f.receiver.observation_at(&b,n+2_000_000,w+2).is_err());
 println!("R1410B_LINEAGE ordinary A->B, duplicate B destructive revoke, C live same epoch; A/B denied historical and live");
}
