//! One logical ingress actor. Each venue/symbol has independent HOT books.
//! No order transport, no PAPER execution, no alternate decision engine.
use crate::{RawFrame,Update,Venue,SourceProvenance,parse};
use crate::market_data_processor_r1378::MarketDataProcessor;
use crate::universal_connection_manager_r1209::{QualityPolicy,SourceId,UniversalConnectionManager,ConnectionLifecycleAction,DualHotTransition,adapter_for,SourceHealth,UcmWorker,UcmWorkerEvent,UcmGroupAction,UcmWorkerSnapshot,UcmPendingSnapshot,UcmPendingCommit,UcmPendingHistory,UcmHistoryEvent,UcmHistoryResult,UcmStableChoice,UcmBookFact,UcmBookFacts,UcmEventAdvice,UcmTickPlan,runtime_lane};
use crate::native_r1173_compact_writer::ingress_store_r1205::{Store,StoreWorker};
use std::{collections::{HashMap,HashSet,VecDeque},sync::{Arc,Mutex,RwLock}};
use serde::Serialize;
pub type SharedStore=Arc<Mutex<Store>>;
pub type Authority=Arc<RwLock<HashMap<String,Permit>>>;
fn next_engine_call_id()->u64{static CALL_ID:std::sync::atomic::AtomicU64=std::sync::atomic::AtomicU64::new(1);CALL_ID.fetch_add(1,std::sync::atomic::Ordering::Relaxed)}
fn timed_authority_write<R>(authority:&Authority,operation:impl FnOnce(&mut HashMap<String,Permit>)->R)->Result<(R,u64,u64),String>{
 let wait_started=std::time::Instant::now();let mut guard=authority.write().map_err(|_|"authority poison")?;
 let wait_ns=wait_started.elapsed().as_nanos().min(u64::MAX as u128)as u64;let hold_started=std::time::Instant::now();
 let result=operation(&mut guard);drop(guard);let hold_ns=hold_started.elapsed().as_nanos().min(u64::MAX as u128)as u64;Ok((result,wait_ns,hold_ns))
}

/// Diagnostic correlation only; never an admission capability.
pub fn frame_trace_id_r1304(f:&RawFrame)->String{
 if let Some(id)=f.source.as_ref().map(|s|&s.frame_trace_id).filter(|id|!id.is_empty()){return id.clone()}
 match &f.source{Some(s)=>format!("{:?}:{}:{}:{}:{}",f.venue,s.session_id,s.connection_epoch,s.accepted_id,f.received_ns),None=>format!("{:?}:unattributed:{}",f.venue,f.received_ns)}
}
/// Opaque validator-issued capability. The ID is diagnostic; liveness is not serializable.
#[derive(Clone,Debug)]
pub struct SnapshotWitness {
 generation:u64, session:String, base:String, venue:Venue, connection:u64,
 alive:Arc<std::sync::atomic::AtomicU8>,continuity:u64,
}
impl SnapshotWitness {
 fn mint(f:&RawFrame,base:&str,continuity:Option<u64>)->Result<Self,String>{
  static NEXT:std::sync::atomic::AtomicU64=std::sync::atomic::AtomicU64::new(1);
  let source=f.source.as_ref().ok_or("snapshot source absent")?;
  let generation=NEXT.fetch_update(std::sync::atomic::Ordering::SeqCst,std::sync::atomic::Ordering::SeqCst,|n|n.checked_add(1)).map_err(|_|"snapshot generation exhausted")?;
  Ok(Self{generation,session:source.session_id.clone(),base:base.into(),venue:f.venue,connection:source.connection_epoch,alive:Arc::new(std::sync::atomic::AtomicU8::new(1)),continuity:continuity.unwrap_or(generation)})
 }
 fn revoke(&self){self.alive.store(0,std::sync::atomic::Ordering::SeqCst);}
 // Ordinary replacement never keeps the old execution capability live.
 fn supersede(&self)->Option<u64>{
  self.alive.compare_exchange(1,2,std::sync::atomic::Ordering::SeqCst,std::sync::atomic::Ordering::SeqCst).ok().map(|_|self.continuity)
 }
 /// Read-only observation classification; does not issue or revive a witness.
 pub fn ordinarily_replaced_by(&self,current:&Self)->bool{
  self.venue==Venue::BinanceFutures&&self.alive.load(std::sync::atomic::Ordering::SeqCst)==2
   &&current.is_live()&&self.continuity==current.continuity&&self.generation<current.generation
   &&self.identity_matches(current.venue,&current.base,&current.session,current.connection)
 }
 pub fn generation(&self)->u64{self.generation}
 pub fn matches(&self,venue:Venue,base:&str,session:&str,connection:u64)->bool{
  self.is_live()&&self.identity_matches(venue,base,session,connection)
 }
 /// Checks the validator-minted scope and connection identity independently of liveness.
 /// A revoked old witness can still be identified, but can never authorize admission.
 pub fn identity_matches(&self,venue:Venue,base:&str,session:&str,connection:u64)->bool{
  self.venue==venue&&self.base==base&&self.session==session&&self.connection==connection
 }
 /// Confirms that this exact witness belongs to the currently published Engine authority.
 pub fn live_for(&self,venue:Venue,base:&str,session:&str)->bool{
  self.is_live()&&self.venue==venue&&self.base==base&&self.session==session
 }
 pub fn is_live(&self)->bool{self.alive.load(std::sync::atomic::Ordering::SeqCst)==1}
 pub fn same_identity(&self,other:&Self)->bool{Arc::ptr_eq(&self.alive,&other.alive)}
 pub fn same_generation(&self,other:&Self)->bool{self.same_identity(other)&&self.is_live()}
}
#[derive(Clone,Debug,Serialize)]pub struct Permit{#[serde(skip)] pub snapshot_witness:Option<SnapshotWitness>,pub session:String,pub epoch:u64,pub received_ns:u64,pub event_ms:i64,pub standby_session:String,pub standby_received_ns:u64,pub standby_event_ms:i64}
pub fn key(v:Venue,base:&str)->String{format!("{v:?}:{base}")}
pub fn authorized(auth:&Authority,f:&RawFrame)->bool{
 let Some(s)=&f.source else{return false};let Ok(u)=parse(f)else{return false};
 auth.read().ok().and_then(|m|m.get(&key(f.venue,&u.base)).cloned()).is_some_and(|p|p.session==s.session_id&&p.epoch==f.epoch&&s.selection_epoch==f.epoch)
}
pub fn authorization_at(auth:&Authority,f:&RawFrame,now_ns:u64,wall:i64)->Option<Permit>{
 let u=parse(f).ok()?;authorization_at_normalized(auth,f,&u,now_ns,wall)
}
pub fn authorization_at_normalized(auth:&Authority,f:&RawFrame,u:&Update,now_ns:u64,wall:i64)->Option<Permit>{
 let source=f.source.as_ref()?;let Ok(m)=auth.read()else{return None};let p=m.get(&key(f.venue,&u.base))?;
 (p.session==source.session_id&&p.epoch==f.epoch&&source.selection_epoch==f.epoch&&p.session!=p.standby_session&&now_ns>=f.received_ns&&now_ns-f.received_ns<=800_000_000&&u.book.exchange_ts_ms.and_then(|e|wall.checked_sub(e)).is_some_and(|a|(0..=800).contains(&a))&&now_ns>=p.standby_received_ns&&now_ns-p.standby_received_ns<=800_000_000&&wall.checked_sub(p.standby_event_ms).is_some_and(|a|(0..=800).contains(&a)) ).then(||p.clone())
}
pub fn authorized_at(auth:&Authority,f:&RawFrame,now_ns:u64,wall:i64)->bool{authorization_at(auth,f,now_ns,wall).is_some()}
pub fn owner_pair_authorized(auth:&Authority,owner:&crate::Owner,base:&str,now_ns:u64,wall:i64)->bool{
 let Ok(m)=auth.read()else{return false};
 [Venue::BinanceFutures,Venue::OkxSpot].iter().all(|v|{
  let Some(binding)=owner.source_binding(base,*v)else{return false};let Some(p)=m.get(&key(*v,base))else{return false};
  binding.0==p.session&&binding.1==p.epoch&&p.session!=p.standby_session&&now_ns>=p.standby_received_ns&&now_ns-p.standby_received_ns<=800_000_000&&wall.checked_sub(p.standby_event_ms).is_some_and(|a|(0..=800).contains(&a))&&now_ns>=p.received_ns&&now_ns-p.received_ns<=800_000_000&&wall.checked_sub(p.event_ms).is_some_and(|a|(0..=800).contains(&a))
 })
}
#[derive(Debug)]pub enum Output{Reset{venue:Venue,base:String,epoch:u64},Frame(RawFrame),Book(RawFrame,Update),Resubscribe{session:String,base:String,reason:String},Blocked{venue:Venue,base:String,reason:String}}
#[derive(Clone,Copy,Debug,Serialize,PartialEq,Eq)]pub enum State{Connecting,Syncing,HotReady,Active,Standby,Degraded,Quarantined,Reconnecting}
#[derive(Clone)]struct SourceBook{output_is_books:bool,snapshot_witness:Option<SnapshotWitness>,frame:RawFrame,update:Update,manager_id:SourceId,good:u32,first_ns:u64,state:State,retained:bool,qualified:bool}
#[derive(Clone)]struct Selected{session:String,epoch:u64,changed_ns:u64}
// R1402: one Engine selection is split into a validated input and a
// publication phase. This is not authority and cannot be reused as a Permit.
struct HotSelectionInputs {
 ordered_scope_sessions:Vec<String>,ready:Vec<String>,prior:Option<Selected>,
 candidates:Vec<SourceId>,initial_wait:bool,
}
// A source switch plan reserves an epoch but does not publish a Reset or
// change Engine-selected. A receipt is required before publication.
struct HotSwitchPlan {
 engine_revocation:u64,
 candidate:SourceId,manager_reason:&'static str,prior:Option<Selected>,
 epoch:u64,old_readiness:Option<serde_json::Value>,new_readiness:Option<serde_json::Value>,
 started:Option<std::time::Instant>,
}
struct AppliedHotSwitch(HotSwitchPlan);
struct HistoricalReadiness {session:String,mask:u16,context:serde_json::Value}
struct ScopeHistoryContext{key:String,entries:Vec<HistoricalReadiness>,ready:Vec<String>,observed_ns:u64}
enum PendingHotStage {
 AwaitApplied,
 Reconcile{candidate:SourceId,reason:&'static str,plan:HotSwitchPlan,destructive_revision:u64,hot_loss_revision:u64},
 Commit{reply:UcmPendingCommit,candidate:SourceId,reason:&'static str,plan:Option<HotSwitchPlan>},
}
struct PendingHotSelection {key:String,anchor:SourceId,stage:PendingHotStage,started_ns:Option<u64>,ticket:u64,deadline:Option<std::time::Instant>}
#[derive(Default,Serialize)]
struct UcmScopeTiming{count:u64,sum_ns:u128,max_ns:u64}
impl UcmScopeTiming{fn add(&mut self,ns:u64){self.count+=1;self.sum_ns+=ns as u128;self.max_ns=self.max_ns.max(ns);}}
#[derive(Default,Serialize)]
struct UcmScopeProfile{
 requests:u64,coalesced:u64,retries:u64,started:u64,finished:u64,abandoned:u64,frames:u64,skipped_empty:u64,skipped_history:u64,unchanged_active:u64,certified_proposals:u64,skipped_stable_tick:u64,skipped_dirty_tick:u64,await_applied:u64,projection_busy:u64,event_advice_used:u64,event_advice_rejected:u64,await_next_event:u64,commit_reconciled:u64,commit_destructive_rejected:u64,commit_lifecycle_rejected:u64,commit_witness_rejected:u64,commit_hot_loss_rejected:u64,commit_active_rejected:u64,
 dirty_wait:UcmScopeTiming,completed_cycle:UcmScopeTiming,abandoned_cycle:UcmScopeTiming,receive_to_publication:UcmScopeTiming,
 #[serde(skip)] dirty_since:Option<u64>,
}


// Mutually exclusive ownership of the existing UCM. Worker mode contains
// only a read projection, registration keys and outstanding reply handles.
type UcmReadScope=(String,String,String,String);
fn ucm_read_scope(id:&SourceId)->UcmReadScope{
 (id.exchange.clone(),id.segment.clone(),id.instrument.clone(),id.channel.clone())
}
fn ucm_engine_scope(id:&SourceId)->String{
 key(if id.exchange=="okx"{Venue::OkxSpot}else{Venue::BinanceFutures},&id.instrument)
}
struct WorkerUcmOwner {
 worker:UcmWorker, known:HashSet<SourceId>,
 views:HashMap<UcmReadScope,UcmWorkerSnapshot>,
 retirements:VecDeque<UcmPendingHistory>,capacity:usize,
 census:Option<(usize,usize)>,
}
enum UcmOwner {Local(UniversalConnectionManager),Worker(WorkerUcmOwner),Failed}
pub struct EngineUcm {
 owner:UcmOwner,authority:Authority,fault:Option<String>,
}
impl EngineUcm {
 fn new(authority:Authority)->Self{
  Self{owner:UcmOwner::Local(UniversalConnectionManager::new(QualityPolicy::default())),authority,fault:None}
 }
 fn remote(&self)->Option<&WorkerUcmOwner>{match &self.owner{UcmOwner::Worker(r)=>Some(r),_=>None}}
 fn remote_mut(&mut self)->Option<&mut WorkerUcmOwner>{match &mut self.owner{UcmOwner::Worker(r)=>Some(r),_=>None}}
 fn fail(&mut self,error:impl Into<String>)->String{
  let error=error.into();self.fault=Some(error.clone());
  if let Ok(mut auth)=self.authority.write(){auth.clear();}
  error
 }
 fn check(&mut self)->Result<(),String>{
  if let Some(error)=self.fault.clone(){return Err(self.fail(error));}
  if matches!(self.owner,UcmOwner::Failed)||self.remote().is_some_and(|r|!r.worker.is_healthy()){
   return Err(self.fail("UCM owner failed closed"));
  }
  if let Some(r)=self.remote_mut(){
   if let Err(error)=r.worker.check_command_progress(){return Err(self.fail(error))}
  }
  Ok(())
 }
 fn activate(&mut self,capacity:usize,notify:Arc<tokio::sync::Notify>)->Result<(),String>{
  self.check()?;
  if !(1..=1024).contains(&capacity){return Err("UCM capacity out of bounds".into())}
  if !matches!(self.owner,UcmOwner::Local(_)){return Err("UCM owner already transferred".into())}
  let UcmOwner::Local(manager)=std::mem::replace(&mut self.owner,UcmOwner::Failed)else{unreachable!()};
  match UcmWorker::start_owned(manager,capacity,Some(notify)){
   Ok(worker)=>{self.owner=UcmOwner::Worker(WorkerUcmOwner{worker,known:HashSet::new(),views:HashMap::new(),retirements:VecDeque::new(),capacity,census:None});Ok(())},
   Err(error)=>Err(self.fail(error)),
  }
 }
 fn local_mut(&mut self)->&mut UniversalConnectionManager{
  match &mut self.owner{UcmOwner::Local(m)=>m,_=>panic!("synchronous UCM operation after ownership transfer")}
 }
 fn has_source(&self,id:&SourceId)->bool{
  match &self.owner{UcmOwner::Local(m)=>m.state(id).is_some(),UcmOwner::Worker(r)=>r.known.contains(id),_=>false}
 }
 fn view(&self,id:&SourceId)->Option<&UcmWorkerSnapshot>{
  let r=self.remote()?;let view=r.views.get(&ucm_read_scope(id))?;
  r.worker.view_current(id,view.revision).then_some(view)
 }
 pub fn state(&self,id:&SourceId)->Option<&SourceHealth>{
  match &self.owner{UcmOwner::Local(m)=>m.state(id),_=>self.view(id)?.sources.get(id).map(|v|&v.health)}
 }
 pub fn quality(&self,id:&SourceId)->Option<(u64,u64,usize)>{
  match &self.owner{UcmOwner::Local(m)=>m.quality(id),_=>self.view(id)?.sources.get(id)?.quality}
 }
 pub fn active_for_scope(&self,id:&SourceId)->Option<&SourceId>{
  match &self.owner{UcmOwner::Local(m)=>m.active_for_scope(id),_=>self.view(id)?.active.get(&ucm_read_scope(id))}
 }
 fn qualification_requirements(&self,id:&SourceId)->(u32,u64,bool){
  match &self.owner{UcmOwner::Local(m)=>m.qualification_requirements(id),_=>self.view(id).and_then(|v|v.sources.get(id)).map(|v|v.qualification_requirements).unwrap_or((3,250_000_000,false))}
 }
 fn readiness_previous_len(&self)->Option<usize>{
  match &self.owner{UcmOwner::Local(m)=>Some(m.readiness_previous_len()),_=>self.remote()?.census.map(|c|c.0)}
 }
 fn readiness_previous_payload_capacity_bytes(&self)->Option<usize>{
  match &self.owner{UcmOwner::Local(m)=>Some(m.readiness_previous_payload_capacity_bytes()),_=>self.remote()?.census.map(|c|c.1)}
 }
 #[cfg(test)]
 fn readiness_has_session(&self,session:&str)->bool{
  match &self.owner{UcmOwner::Local(m)=>m.readiness_has_session(session),_=>false}
 }
 fn event(&mut self,event:UcmWorkerEvent)->Result<(),String>{
  self.check()?;
  if let UcmOwner::Local(m)=&mut self.owner{
   return match event{
    UcmWorkerEvent::Group{ids,action}=>{for id in ids{match action{
     UcmGroupAction::Connect{auto_subscribe}=>{m.connect(id.clone());if auto_subscribe{m.subscribed(&id);}},
     UcmGroupAction::Subscribe=>m.subscribed(&id),
     UcmGroupAction::Reconnect=>m.reconnecting(&id),
     UcmGroupAction::Close=>m.close(&id),
     UcmGroupAction::Quarantine=>m.quarantine(&id),
    }}Ok(())},
    UcmWorkerEvent::Connect(id)=>{m.connect(id);Ok(())},
    UcmWorkerEvent::Subscribe(id)=>{m.subscribed(&id);Ok(())},
    UcmWorkerEvent::Reconnect(id)=>{m.reconnecting(&id);Ok(())},
    UcmWorkerEvent::Close(id)=>{m.close(&id);Ok(())},
    UcmWorkerEvent::Quarantine(id)=>{m.quarantine(&id);Ok(())},
    UcmWorkerEvent::BookReset{source,sequence_gap}=>{if sequence_gap{m.reconnecting(&source)}else{m.quarantine(&source)};Ok(())},
    UcmWorkerEvent::Observe{source,received_ns,exchange_ms,received_wall_ms,now_ns,now_ms,valid}=>{m.observe_with_receive_wall(&source,received_ns,exchange_ms,received_wall_ms,now_ns,now_ms,valid);Ok(())},
    UcmWorkerEvent::TransportOnly{source,received_ns,received_wall_ms,now_ns}=>m.observe_transport_liveness(&source,received_ns,received_wall_ms,now_ns),
    UcmWorkerEvent::Tick{now_ns,now_ms}=>{m.tick(now_ns,now_ms);Ok(())},
    UcmWorkerEvent::CommitActive{source,..}=>if m.set_active(&source){Ok(())}else{Err("UCM active failed".into())},
   }
  }
  // A destructive lifecycle event must revoke affected scope authority at
  // submission time, not after the worker catches up.
  let revoke=match &event{
   UcmWorkerEvent::Group{ids,action:UcmGroupAction::Reconnect|UcmGroupAction::Close|UcmGroupAction::Quarantine}=>ids.iter().map(ucm_engine_scope).collect::<Vec<_>>(),
   UcmWorkerEvent::Reconnect(id)|UcmWorkerEvent::Close(id)|UcmWorkerEvent::Quarantine(id)=>vec![ucm_engine_scope(id)],
   UcmWorkerEvent::BookReset{source,..}|UcmWorkerEvent::Observe{source,valid:false,..}=>vec![ucm_engine_scope(source)],
   _=>Vec::new()
  };
  if !revoke.is_empty(){let mut auth=self.authority.write().map_err(|_|"authority poison")?;for k in revoke{auth.remove(&k);}}
  let new_ids=match &event{
   UcmWorkerEvent::Connect(id)=>vec![id.clone()],
   UcmWorkerEvent::Group{ids,action:UcmGroupAction::Connect{..}}=>ids.clone(),
   _=>Vec::new()
  };
  let result={
   let r=self.remote_mut().ok_or("missing UCM owner")?;
   r.worker.revision().checked_add(1).ok_or("UCM revision overflow")
    .and_then(|rev|r.worker.apply(rev,event))
  };
  if let Err(error)=result{return Err(self.fail(error));}
  let r=self.remote_mut().unwrap();
  // Connect is ordered after Close on the existing runtime lane FIFO.
  // The owner validates closed/newer/same scopes before applying Connect.
  // An owner rejection still fails closed. Old views cannot cross the
  // new scope revision fence, and need not keep retired health alive.
  if let Some(first)=new_ids.first(){
   if let Some(lane)=runtime_lane(first){
    r.known.retain(|id|runtime_lane(id)!=Some(lane)||id.generation>=first.generation
     ||!new_ids.iter().any(|new|ucm_read_scope(new)==ucm_read_scope(id)));
    for id in &new_ids{r.views.remove(&ucm_read_scope(id));}
   }
  }
  r.known.extend(new_ids);
  Ok(())
 }
 fn connect(&mut self,id:SourceId){let _=self.event(UcmWorkerEvent::Connect(id));}
 fn subscribed(&mut self,id:&SourceId){let _=self.event(UcmWorkerEvent::Subscribe(id.clone()));}
 pub fn close(&mut self,id:&SourceId){let _=self.event(UcmWorkerEvent::Close(id.clone()));}
 pub fn quarantine(&mut self,id:&SourceId){let _=self.event(UcmWorkerEvent::Quarantine(id.clone()));}
 fn book_reset(&mut self,id:&SourceId,sequence_gap:bool){let _=self.event(UcmWorkerEvent::BookReset{source:id.clone(),sequence_gap});}
 fn group(&mut self,ids:Vec<SourceId>,action:UcmGroupAction){
  if !ids.is_empty(){let _=self.event(UcmWorkerEvent::Group{ids,action});}
 }
 fn apply_connection_lifecycle(&mut self,ids:&[SourceId],action:ConnectionLifecycleAction){
  let action=match action{
   ConnectionLifecycleAction::Subscribe=>UcmGroupAction::Subscribe,
   ConnectionLifecycleAction::Reconnect=>UcmGroupAction::Reconnect,
   ConnectionLifecycleAction::Close=>UcmGroupAction::Close,
  };
  self.group(ids.to_vec(),action);
 }
 fn observe_with_receive_wall(&mut self,id:&SourceId,received_ns:u64,exchange_ms:i64,received_wall_ms:i64,now_ns:u64,now_ms:i64,valid:bool){
  let _=self.event(UcmWorkerEvent::Observe{source:id.clone(),received_ns,exchange_ms,received_wall_ms,now_ns,now_ms,valid});
 }
 fn observe_book_facts(&mut self,id:&SourceId,received_ns:u64,exchange_ms:i64,received_wall_ms:i64,now_ns:u64,now_ms:i64,facts:UcmBookFacts)->Result<(),String>{
  self.check()?;
  let r=self.remote_mut().ok_or("missing UCM owner for book facts")?;
  let result=r.worker.revision().checked_add(1).ok_or("UCM revision overflow").and_then(|rev|r.worker.apply_with_book_facts(rev,UcmWorkerEvent::Observe{source:id.clone(),received_ns,exchange_ms,received_wall_ms,now_ns,now_ms,valid:true},Some(facts)));
  if let Err(error)=result{return Err(self.fail(error))}
  Ok(())
 }
 fn observe_transport_liveness(&mut self,id:&SourceId,received_ns:u64,received_wall_ms:i64,now_ns:u64)->Result<(),String>{
  self.event(UcmWorkerEvent::TransportOnly{source:id.clone(),received_ns,received_wall_ms,now_ns})
 }
 pub fn tick(&mut self,now_ns:u64,now_ms:i64){
  if let Some(r)=self.remote_mut(){
   let result=r.worker.revision().checked_add(1).ok_or("UCM revision overflow")
    .and_then(|rev|r.worker.apply_certified_tick(rev,now_ns,now_ms));
   if let Err(error)=result{self.fail(error);}
  }else{let _=self.event(UcmWorkerEvent::Tick{now_ns,now_ms});}
 }
 fn tick_with_book_facts(&mut self,plan:UcmTickPlan,facts:Vec<UcmBookFacts>)->Result<(),String>{
  self.check()?;
  let r=self.remote_mut().ok_or("UCM owner missing")?;
  let result=r.worker.revision().checked_add(1).ok_or("UCM revision overflow")
    .and_then(|rev|r.worker.apply_planned_tick(rev,plan,facts));
  if let Err(error)=result{return Err(self.fail(error))}Ok(())
 }
 fn choose_candidate(&mut self,ids:&[SourceId],ns:u64,wall:i64)->Option<(SourceId,&'static str)>{self.local_mut().choose_candidate(ids,ns,wall)}
 fn set_active(&mut self,id:&SourceId)->bool{self.local_mut().set_active(id)}
 fn observe_readiness_mask(&mut self,k:&str,s:&str,mask:u16)->(Option<u16>,bool){self.local_mut().observe_readiness_mask(k,s,mask)}
 fn observe_dual_hot(&mut self,k:&str,hot:bool,ns:u64)->DualHotTransition{self.local_mut().observe_dual_hot(k,hot,ns)}
 fn retire_readiness_session(&mut self,session:&str)->usize{
  if let UcmOwner::Local(m)=&mut self.owner{return m.retire_readiness_session(session)}
  let result=if let Some(r)=self.remote_mut(){
   if r.retirements.len()>=r.capacity{Err("UCM outstanding retirement receipts full")}
   else{r.worker.revision().checked_add(1).ok_or("UCM revision overflow")
    .and_then(|rev|r.worker.begin_history(rev,UcmHistoryEvent::RetireSession{session:session.into()}))
    .map(|receipt|r.retirements.push_back(receipt))}
  }else{Err("UCM owner missing")};
  if let Err(error)=result{self.fail(error);}
  0 // actual count applied when the asynchronous receipt arrives
 }
}

fn connection_group(session:&str)->String{let parts=session.split('-').take(3).collect::<Vec<_>>();if parts.len()==3{parts.join("-")}else{session.into()}}
fn advances_highwater(venue:Venue,seq:i64,event:i64,prior:Option<(i64,i64)>)->bool{
 let Some((old_seq,old_event))=prior else{return true};
 if venue==Venue::OkxSpot{(seq>old_seq&&event>=old_event)||event>old_event}else{seq>old_seq&&event>=old_event}
}
fn same_published_book(left:&SourceBook,right:&SourceBook)->bool{
 left.frame.venue==right.frame.venue&&left.update.base==right.update.base&&
 left.update.book.sequence.is_some()&&left.update.book.sequence==right.update.book.sequence&&
 left.update.book.exchange_ts_ms.is_some()&&left.update.book.exchange_ts_ms==right.update.book.exchange_ts_ms&&
 left.update.book.bids==right.update.book.bids&&left.update.book.asks==right.update.book.asks
}
#[derive(Default,Serialize)]
pub struct R1273Profile { samples: std::collections::BTreeMap<String,std::collections::VecDeque<u64>>, evictions: std::collections::BTreeMap<String,u64>, calls:std::collections::BTreeMap<String,u64>, totals_ns:std::collections::BTreeMap<String,u128>, batches:u64 }
impl R1273Profile {
 fn record(&mut self,name:&str,ns:u64){let v=self.samples.entry(name.into()).or_default();if v.len()>=8192{v.pop_front();*self.evictions.entry(name.into()).or_default()+=1;}v.push_back(ns);*self.calls.entry(name.into()).or_default()+=1;*self.totals_ns.entry(name.into()).or_default()+=ns as u128;}
 pub fn snapshot(&self)->serde_json::Value{
  let mut rows=serde_json::Map::new();
  for (name,values) in &self.samples{let mut sorted=values.iter().copied().collect::<Vec<_>>();sorted.sort_unstable();let pct=|p:usize|->u64{if sorted.is_empty(){0}else{let i=(sorted.len()*p+99)/100;sorted[i.saturating_sub(1).min(sorted.len()-1)]}};
   rows.insert(name.clone(),serde_json::json!({"calls":self.calls.get(name).copied().unwrap_or(0),"sampled":values.len(),"sum_ns":self.totals_ns.get(name).copied().unwrap_or(0),"p50_ns":pct(50),"p95_ns":pct(95),"p99_ns":pct(99),"max_ns":sorted.last().copied().unwrap_or(0),"evictions":self.evictions.get(name).copied().unwrap_or(0)}));
  }
  serde_json::json!({"batches":self.batches,"bounded_samples_per_operation":8192,"operations":rows})
 }
}
#[derive(Default,Serialize)]pub struct Metrics{pub received:u64,pub duplicates:u64,pub invalid:u64,pub switches:u64,pub blocked:u64,pub submitted:u64,pub hot_deficits:u64,pub reorder:u64,pub audit_evictions:u64,pub channel_keepalives:u64,pub invalid_audit_evictions:u64,pub okx_books_invalid_audit_evictions:u64,pub selection_audit_evictions:u64,pub readiness_audit_evictions:u64,pub readiness_state_evictions:u64,pub readiness_state_retirements:u64,pub readiness_audit_kind_counts:std::collections::BTreeMap<String,u64>,pub stale_frame_audit_evictions:u64,pub invalid_categories:std::collections::BTreeMap<String,u64>}
const OKX_RESUBSCRIBE_RETRY_MIN_INTERVAL_NS:u64=15_000_000_000;
const STALE_FRAME_AUDIT_LIMIT:usize=128;
const STALE_FRAME_AUDIT_BYTES_LIMIT:usize=64*1024;
const READINESS_AUDIT_LIMIT:usize=128;
const READINESS_AUDIT_BYTES_LIMIT:usize=64*1024;
const READINESS_PREDICATE_NAMES:[&str;14]=["tcp_ready","tls_ready","websocket_ready","subscribed","last_valid_receive_fresh","last_valid_exchange_fresh","last_event_fresh_at_websocket_receive","minimum_good_samples_met","qualification_dwell_met","manager_hot_ready","book_receive_fresh","book_exchange_fresh","book_state_usable","effective_ready"];
fn audit_value_bytes(value:&serde_json::Value)->usize{serde_json::to_vec(value).map(|bytes|bytes.len()).unwrap_or(usize::MAX)}
#[derive(Default)]
struct ScopeClockIndex{
    regions:HashMap<String,[i128;4]>,
    edges:[std::collections::BTreeSet<(i128,String)>;4],
}
impl ScopeClockIndex{
    fn remove(&mut self,k:&str){
        if let Some(old)=self.regions.remove(k){
            for (edge,at) in self.edges.iter_mut().zip(old){edge.remove(&(at,k.to_string()));}
        }
    }
    fn interval(now:i128,stamp:i128,limit:i128)->(i128,i128){
        if now<stamp{(i128::MIN,stamp-1)}
        else if now<=stamp+limit{(stamp,stamp+limit)}
        else{(stamp+limit+1,i128::MAX)}
    }
    fn update(&mut self,k:&str,ns:u64,wall:i64,books:impl Iterator<Item=(u64,Option<i64>)>){
        self.remove(k);
        let mut region=[i128::MIN,i128::MAX,i128::MIN,i128::MAX];
        let mut any=false;
        for (receive,event) in books{
            any=true;
            let (lo,hi)=Self::interval(ns as i128,receive as i128,800_000_000);
            region[0]=region[0].max(lo);region[1]=region[1].min(hi);
            if let Some(event)=event{
                let (lo,hi)=Self::interval(wall as i128,event as i128,800);
                region[2]=region[2].max(lo);region[3]=region[3].min(hi);
            }
        }
        if any{
            for (edge,at) in self.edges.iter_mut().zip(region){edge.insert((at,k.to_string()));}
            self.regions.insert(k.to_string(),region);
        }
    }
    fn due(&self,ns:u64,wall:i64)->HashSet<String>{
        let (n,w)=(ns as i128,wall as i128);
        let mut out=HashSet::new();
        for (i,now) in [(0,n),(2,w)]{
            out.extend(self.edges[i].iter().rev().take_while(|(at,_)|*at>now).map(|(_,k)|k.clone()));
            out.extend(self.edges[i+1].iter().take_while(|(at,_)|*at<now).map(|(_,k)|k.clone()));
        }
        out
    }
}
pub struct Engine{
 ucm_dirty:std::collections::BTreeSet<String>,ucm_pending:HashMap<String,PendingHotSelection>,
 ucm_clock_index:ScopeClockIndex,ucm_fact_refresh:HashSet<String>,
 ucm_wait:std::collections::BTreeSet<(u64,String)>,
 ucm_deadlines:std::collections::BTreeSet<(std::time::Instant,String)>,
 ucm_startable:std::collections::BTreeSet<String>,
 ucm_start_next:bool,ucm_cursor:Option<String>,ucm_notify:Option<Arc<tokio::sync::Notify>>,ucm_scope_profile:Option<std::collections::BTreeMap<String,UcmScopeProfile>>,
 // Memo of exact acknowledged no-op history inputs, not readiness authority.
 // Cleared on any history eviction/retirement. Scope membership bounded by universe.
 ucm_history_memo:HashMap<String,(Vec<(String,u16)>,bool)>,ucm_history_seen:HashSet<String>,ucm_history_context:HashMap<u64,ScopeHistoryContext>,ucm_stable_choices:HashMap<String,UcmStableChoice>,ucm_event_advice_seen:HashMap<String,u64>,ucm_witness_revocations:HashMap<String,u64>,
 pub store:Option<SharedStore>,pub authority:Authority,pub metrics:Metrics,store_worker:Option<StoreWorker>,memory_epoch:u64,
 books:HashMap<(String,String),SourceBook>,book_scope_sessions:HashMap<String,HashSet<String>>,ucm_book_order:Option<HashMap<String,Vec<String>>>,market_data_processor:MarketDataProcessor,prepared_chain_accepted:HashMap<(String,String),u64>,resyncing:HashSet<(String,String)>,resubscribe_last_ns:HashMap<(String,String),u64>,selected:HashMap<String,Selected>,
 highwater:HashMap<String,(i64,i64)>,universe:HashSet<String>,sources:HashMap<String,State>,connection_scopes:HashMap<String,HashSet<String>>,
 quarantines:Mutex<Vec<(RawFrame,String)>>,r1279_terminal_samples:Mutex<VecDeque<serde_json::Value>>,r1279_trace_dropped:std::sync::atomic::AtomicU64,r1279_trace_sampled_out:std::sync::atomic::AtomicU64,audit:VecDeque<serde_json::Value>,audit_limit:usize,invalid_audit:VecDeque<serde_json::Value>,stale_frame_audit:VecDeque<serde_json::Value>,stale_frame_audit_bytes:usize,okx_books_invalid_audit:VecDeque<serde_json::Value>,okx_books_invalid_audit_limit:usize,selection_audit:VecDeque<serde_json::Value>,readiness_audit:VecDeque<serde_json::Value>,readiness_audit_bytes:usize,pub manager:EngineUcm,manager_ids:HashMap<(String,String),SourceId>,pub r1273_profile:Mutex<R1273Profile>, r1273_profile_enabled:bool,
 r1352_diagnostic:Option<crate::r1352_diagnostic_worker::Publisher>,r1352_diagnostic_dropped:u64,
 r1353_shards:Option<std::sync::Arc<crate::r1352_diagnostic_worker::ShardedDiagnostics>>,
 clock:Option<fn()->(u64,i64)>,r1247_trace_enabled:bool,r1310_trace_enabled:bool,r1310_trace_sample_every:u64,r1310_trace_sampled_in:u64,r1310_trace_sampled_out:u64,r1247_stage_audit:VecDeque<serde_json::Value>,r1247_stage_evictions:u64,r1247_selection_evaluations:VecDeque<serde_json::Value>,r1247_selection_eval_last:HashMap<String,u64>,r1247_selection_eval_evictions:u64,
}
impl Engine{
 pub fn new(store:SharedStore,authority:Authority,keys:Vec<String>)->Result<Self,String>{
  let store_worker=StoreWorker::start(store.clone())?;
  Self::new_with_store_worker(store,store_worker,authority,keys)
 }
 pub fn new_with_store_worker(store:SharedStore,store_worker:StoreWorker,authority:Authority,keys:Vec<String>)->Result<Self,String>{
  Self::new_core(Some(store),Some(store_worker),authority,keys)
 }
 pub fn new_volatile(authority:Authority,keys:Vec<String>)->Result<Self,String>{
  Self::new_core(None,None,authority,keys)
 }
 // R1322C: binary test crates link the library without cfg(test).
 // Explicit offline feature only; ordinary builds expose no clock setter.
 #[cfg(any(test,feature="offline-test-clock"))]
 #[doc(hidden)]
 pub fn set_test_clock(&mut self,clock:fn()->(u64,i64)){self.clock=Some(clock);}
 fn new_core(store:Option<SharedStore>,store_worker:Option<StoreWorker>,authority:Authority,keys:Vec<String>)->Result<Self,String>{
  if keys.is_empty()||keys.len()>512||keys.iter().any(|k|k.len()>80){return Err("R1205 scope bounds".into())}
  let manager=EngineUcm::new(authority.clone());
  Ok(Self{ucm_dirty:std::collections::BTreeSet::new(),ucm_pending:HashMap::new(),ucm_clock_index:Default::default(),ucm_fact_refresh:HashSet::new(),ucm_wait:Default::default(),ucm_deadlines:Default::default(),ucm_startable:Default::default(),ucm_start_next:true,ucm_cursor:None,ucm_notify:None,ucm_scope_profile:None,ucm_history_memo:HashMap::new(),ucm_history_seen:HashSet::new(),ucm_history_context:HashMap::new(),ucm_stable_choices:HashMap::new(),ucm_event_advice_seen:HashMap::new(),ucm_witness_revocations:HashMap::new(),store,authority,metrics:Metrics::default(),store_worker,memory_epoch:0,books:HashMap::new(),book_scope_sessions:HashMap::new(),ucm_book_order:None,market_data_processor:MarketDataProcessor::default(),prepared_chain_accepted:HashMap::new(),resyncing:HashSet::new(),resubscribe_last_ns:HashMap::new(),selected:HashMap::new(),highwater:HashMap::new(),universe:keys.into_iter().collect(),sources:HashMap::new(),connection_scopes:HashMap::new(),quarantines:Mutex::new(Vec::new()),r1279_terminal_samples:Mutex::new(VecDeque::new()),r1279_trace_dropped:std::sync::atomic::AtomicU64::new(0),r1279_trace_sampled_out:std::sync::atomic::AtomicU64::new(0),audit:VecDeque::new(),audit_limit:256,invalid_audit:VecDeque::new(),stale_frame_audit:VecDeque::new(),stale_frame_audit_bytes:0,okx_books_invalid_audit:VecDeque::new(),okx_books_invalid_audit_limit:256,selection_audit:VecDeque::new(),readiness_audit:VecDeque::new(),readiness_audit_bytes:0,manager,manager_ids:HashMap::new(),r1273_profile:Mutex::new(R1273Profile::default()),r1273_profile_enabled:std::env::var("R1273_PROFILE").ok().as_deref()==Some("1"),r1352_diagnostic:None,r1352_diagnostic_dropped:0,r1353_shards:None,clock:Some(||(crate::ingress_runtime_r1205::mono_ns(),crate::ingress_runtime_r1205::wall_ms())),r1247_trace_enabled:std::env::var("R1247_TRACE").ok().as_deref()==Some("1"),r1310_trace_enabled:std::env::var("R1310_TRACE").ok().as_deref()==Some("1"),r1310_trace_sample_every:std::env::var("R1310_TRACE_SAMPLE_EVERY").ok().and_then(|x|x.parse::<u64>().ok()).filter(|x|(1..=4096).contains(x)).unwrap_or(64),r1310_trace_sampled_in:0,r1310_trace_sampled_out:0,r1247_stage_audit:VecDeque::new(),r1247_stage_evictions:0,r1247_selection_evaluations:VecDeque::new(),r1247_selection_eval_last:HashMap::new(),r1247_selection_eval_evictions:0})
 }
 /// R1352 optional observer; never consulted by market-data admission or trading.
 pub fn attach_diagnostic_r1352(&mut self,publisher:crate::r1352_diagnostic_worker::Publisher){self.r1352_diagnostic=Some(publisher);}
 pub fn diagnostic_dropped_r1352(&self)->u64{self.r1352_diagnostic_dropped}
 /// R1353: optional, RAM-only observer. Not used for market decisions.
 pub fn attach_sharded_diagnostics_r1353(&mut self,bus:std::sync::Arc<crate::r1352_diagnostic_worker::ShardedDiagnostics>){self.r1353_shards=Some(bus);}
 fn publish_frame_sensor_r1353(&self,frame:&RawFrame){
  let Some(bus)=self.r1353_shards.as_ref() else{return;};
  let Some(provenance)=frame.source.as_ref() else{return;};
  let Some(timing)=provenance.r1279_timing.as_ref() else{return;};
  let Some(scope)=timing.instrument.as_deref() else{return;};
  let sample=crate::r1352_diagnostic_worker::StageSample::from_timing(scope,&provenance.session_id,timing);
  let _=bus.try_publish(crate::r1352_diagnostic_worker::ShardMessage::Sample(sample));
 }

 fn publish_scope_diagnostic_r1352(&mut self,k:&str,ns:u64,wall:i64){
  if self.r1352_diagnostic.is_none(){return;}
  let mut hot_sources=0u16;let mut last_valid_ns=None;
  for ((_,scope),book) in &self.books{if scope!=k{continue;}if self.ready(book,ns,wall){hot_sources=hot_sources.saturating_add(1);}last_valid_ns=Some(last_valid_ns.unwrap_or(0u64).max(book.frame.received_ns));}
  let status=crate::r1352_diagnostic_worker::ScopeStatus{scope:k.to_owned(),generation:self.memory_epoch,observed_ns:ns,last_valid_ns,hot_sources,selected:self.selected.contains_key(k)};
  if let Some(publisher)=&self.r1352_diagnostic{if !publisher.try_publish(crate::r1352_diagnostic_worker::Message::Update(status)){self.r1352_diagnostic_dropped=self.r1352_diagnostic_dropped.saturating_add(1);}}
 }
 pub fn memory_structure_counts_r1326p(&self)->serde_json::Value{
  let quarantines=self.quarantines.lock().map(|x|x.len()).unwrap_or(0);
  let raw_capacity_bytes=self.books.values().map(|b|b.frame.raw.capacity()).sum::<usize>();
  let book_level_capacity_bytes=self.books.values().map(|b|(b.update.book.bids.capacity()+b.update.book.asks.capacity()).saturating_mul(std::mem::size_of::<(f64,f64)>())).sum::<usize>();
  let books_table_capacity_bytes=self.books.capacity().saturating_mul(std::mem::size_of::<((String,String),SourceBook)>());
  let readiness_previous_payload_capacity_bytes=self.manager.readiness_previous_payload_capacity_bytes();
  let connection_scope_payload_capacity_bytes=self.connection_scopes.values().map(|scopes|scopes.capacity().saturating_mul(std::mem::size_of::<String>()).saturating_add(scopes.iter().map(String::capacity).sum::<usize>())).sum::<usize>();
  let (okx_l2_books,okx_l2_state_capacity_bytes)=self.market_data_processor.memory_usage_bytes();
  serde_json::json!({"books":self.books.len(),"books_table_capacity":self.books.capacity(),"books_table_capacity_bytes_estimate":books_table_capacity_bytes,"books_raw_buffer_capacity_bytes":raw_capacity_bytes,"book_levels_capacity_bytes":book_level_capacity_bytes,"okx_l2_books":okx_l2_books,"okx_l2_state_capacity_bytes_estimate":okx_l2_state_capacity_bytes,"resyncing":self.resyncing.len(),"resubscribe_last":self.resubscribe_last_ns.len(),"selected":self.selected.len(),"highwater":self.highwater.len(),"universe":self.universe.len(),"sources":self.sources.len(),"connection_scopes":self.connection_scopes.len(),"connection_scope_payload_capacity_bytes_estimate":connection_scope_payload_capacity_bytes,"quarantines":quarantines,"readiness_audit":self.readiness_audit.len(),"readiness_audit_serialized_bytes":self.readiness_audit_bytes,"readiness_previous":self.manager.readiness_previous_len(),"readiness_previous_payload_capacity_bytes_estimate":readiness_previous_payload_capacity_bytes,"audit":self.audit.len(),"selection_audit":self.selection_audit.len(),"invalid_audit":self.invalid_audit.len(),"stale_audit":self.stale_frame_audit.len(),"stale_audit_serialized_bytes":self.stale_frame_audit_bytes,"r1247_stage_audit":self.r1247_stage_audit.len(),"r1247_selection_evaluations":self.r1247_selection_evaluations.len()})
 }
 pub fn durable_writer_profile(&self)->serde_json::Value{self.store_worker.as_ref().map(|w|w.profile()).unwrap_or(serde_json::json!({"mode":"VOLATILE_RAM_ONLY","sqlite_commit_in_ingress":false,"source_provenance":"RAM_SESSION_NO_DURABLE_RECEIPT","canonical_receipt":false}))}
 pub fn enable_r1310_trace(&mut self){self.r1310_trace_enabled=true;self.r1310_trace_sample_every=1;}
 pub fn r1310_trace_metrics(&self)->serde_json::Value{serde_json::json!({"enabled":self.r1310_trace_enabled,"sample_every":self.r1310_trace_sample_every,"sampled_in":self.r1310_trace_sampled_in,"sampled_out":self.r1310_trace_sampled_out,"audit_evictions":self.metrics.audit_evictions,"terminal_trace_dropped":self.r1279_trace_dropped(),"terminal_trace_sampled_out":self.r1279_trace_sampled_out()})}
 pub fn register(&mut self,id:&str)->Result<(),String>{
  if self.sources.len()>=192||id.is_empty()||id.len()>96||self.sources.contains_key(id){return Err("R1205 source bounds/reuse".into())}
  self.sources.insert(id.into(),State::Connecting);Ok(())
 }
 pub fn register_connection(&mut self,id:&str,venue:Venue,generation:u64)->Result<(),String>{
  let prefix=format!("{venue:?}:");
  let bases=self.universe.iter().filter_map(|scope|scope.strip_prefix(&prefix).map(str::to_string)).collect::<Vec<_>>();
  self.register_connection_scoped(id,venue,generation,&bases)
 }
 pub fn register_connection_scoped(&mut self,id:&str,venue:Venue,generation:u64,bases:&[String])->Result<(),String>{
  let mut scope_keys=HashSet::with_capacity(bases.len());
  for base in bases{
   let scope=key(venue,base);
   if base.is_empty()||!self.universe.contains(&scope)||!scope_keys.insert(scope){
    return Err("R1205 connection scope outside universe, empty, or duplicate".into())
   }
  }
  if bases.is_empty(){return Err("R1205 empty connection scope".into())}
  self.register(id)?;
  let mut group=Vec::with_capacity(bases.len());
  for base in bases{
   let manager_id=SourceId{exchange:if venue==Venue::OkxSpot{"okx"}else{"binance"}.into(),segment:if venue==Venue::OkxSpot{"spot"}else{"usdm"}.into(),instrument:base.clone(),channel:adapter_for(venue).channel().into(),physical_connection:id.into(),logical_session:id.into(),connection_group:connection_group(id),generation};
   group.push(manager_id.clone());
   self.manager_ids.insert((id.into(),key(venue,base)),manager_id);
  }
  self.manager.group(group,UcmGroupAction::Connect{auto_subscribe:venue==Venue::BinanceFutures});
  self.connection_scopes.insert(id.into(),scope_keys);
  self.manager.check()
 }
 pub fn mark_subscribed(&mut self,id:&str){let ids=self.manager_ids.iter().filter(|((session,_),_)|session==id).map(|(_,source)|source.clone()).collect::<Vec<_>>();self.manager.apply_connection_lifecycle(&ids,ConnectionLifecycleAction::Subscribe);}
 pub fn mark_reconnecting(&mut self,id:&str){
  let traces=self.books.iter().filter(|((session,_),book)|session==id&&book.frame.source.as_ref().and_then(|p|p.r1279_timing.as_ref()).is_some_and(|t|t.r1310_trace_sampled)).map(|(_,book)|book.frame.clone()).collect::<Vec<_>>();
  for ((session,_),book) in &self.books{if session==id{if let Some(w)=&book.snapshot_witness{w.revoke();}}}
  for frame in &traces{self.r1310_lifecycle(frame,"reconnect",None);}
  self.market_data_processor.clear_session(id);self.prepared_chain_accepted.retain(|(session,_),_|session!=id);if let Some(s)=self.sources.get_mut(id){*s=State::Reconnecting}
  let ids=self.manager_ids.iter().filter(|((session,_),_)|session==id).map(|(_,source)|source.clone()).collect::<Vec<_>>();self.manager.apply_connection_lifecycle(&ids,ConnectionLifecycleAction::Reconnect);
 }
 fn manager_source_id(frame:&RawFrame,base:&str,decoded:Option<&serde_json::Value>)->Result<SourceId,String>{
  let provenance=frame.source.as_ref().ok_or("manager source provenance")?;
  // R1388: the full market-data processor decoded JSON before Engine admission.
  // A raw fallback is retained for the original test/direct receive path.
  let fallback=decoded.is_none().then(||serde_json::from_str::<serde_json::Value>(&frame.raw).unwrap_or(serde_json::Value::Null));
  let value=decoded.or(fallback.as_ref()).expect("predecoded or fallback");
  let channel=if frame.venue==Venue::OkxSpot{value["arg"]["channel"].as_str().map(str::to_string)}else{value["stream"].as_str().and_then(|stream|stream.split_once('@').map(|(_,channel)|channel.to_string()))}.unwrap_or_else(||adapter_for(frame.venue).channel().into());
  Ok(SourceId{exchange:if frame.venue==Venue::OkxSpot{"okx"}else{"binance"}.into(),segment:if frame.venue==Venue::OkxSpot{"spot"}else{"usdm"}.into(),instrument:base.into(),channel,physical_connection:provenance.session_id.clone(),logical_session:provenance.session_id.clone(),connection_group:connection_group(&provenance.session_id),generation:provenance.connection_epoch})
 }
 fn ensure_manager_source(&mut self,frame:&RawFrame,base:&str)->Result<SourceId,String>{self.ensure_manager_source_decoded(frame,base,None)}
 fn ensure_manager_source_decoded(&mut self,frame:&RawFrame,base:&str,decoded:Option<&serde_json::Value>)->Result<SourceId,String>{
  let started=self.r1273_profile_enabled.then(std::time::Instant::now);
  let result=self.ensure_manager_source_inner(frame,base,decoded);
  self.r1273_record_elapsed("source_manager_update",started);result
 }
 fn ensure_manager_source_inner(&mut self,frame:&RawFrame,base:&str,decoded:Option<&serde_json::Value>)->Result<SourceId,String>{
  let id=Self::manager_source_id(frame,base,decoded)?;let scope=key(frame.venue,base);let session=frame.source.as_ref().ok_or("source")?.session_id.clone();
  if self.manager_ids.get(&(session.clone(),scope.clone())).is_some_and(|registered|registered.generation!=id.generation){
   self.quarantine_session(&session);
   return Err("R1301A registered connection generation mismatch".into())
  }
  if self.connection_scopes.get(&session).is_some_and(|allowed|!allowed.contains(&scope)){
   return Err("R1205 source frame outside assigned subscription scope".into())
  }
  if !self.manager.has_source(&id){self.manager.connect(id.clone());self.manager.subscribed(&id);}
  self.manager.check()?;
  self.manager_ids.insert((session,scope),id.clone());Ok(id)
 }
 fn record_invalid(&mut self,frame:&RawFrame,category:&str,count_invalid:bool){
  if count_invalid{self.metrics.invalid=self.metrics.invalid.saturating_add(1);}
  *self.metrics.invalid_categories.entry(category.into()).or_default()+=1;
  if self.invalid_audit.len()>=64{self.invalid_audit.pop_front();self.metrics.invalid_audit_evictions=self.metrics.invalid_audit_evictions.saturating_add(1);}
  let value=serde_json::from_str::<serde_json::Value>(&frame.raw).unwrap_or(serde_json::Value::Null);
  let null=serde_json::Value::Null;
  let payload=match value.get("data"){
   Some(data) if data.is_array()=>data.as_array().and_then(|rows|rows.first()).unwrap_or(&null),
   Some(data)=>data,
   None=>&value,
  };
  let instrument=value["arg"]["instId"].as_str().and_then(|s|s.strip_suffix("-USDT")).map(str::to_string)
   .or_else(||payload["s"].as_str().map(|s|s.strip_suffix("USDT").unwrap_or(s).to_string()))
   .or_else(||value["stream"].as_str().and_then(|s|s.split_once('@').map(|(symbol,_)|symbol.strip_suffix("usdt").unwrap_or(symbol).to_ascii_uppercase())));
  let sequence=payload["u"].as_i64().or_else(||payload["lastUpdateId"].as_i64()).or_else(||payload["seqId"].as_i64());
  let timestamp_ms=payload["E"].as_i64().or_else(||payload["ts"].as_str().and_then(|s|s.parse().ok()));
  let event_age_ms=timestamp_ms.and_then(|ts|frame.received_wall_ms.checked_sub(ts));
  let source=frame.source.as_ref();
  let channel=value["arg"]["channel"].as_str().map(str::to_string).or_else(||value["stream"].as_str().map(str::to_string));
  let audit_row=serde_json::json!({"source":source.map(|s|s.session_id.as_str()),"instrument":instrument,"generation":source.map(|s|s.connection_epoch),"category":category,"channel":channel,"sequence":sequence,"timestamp_ms":timestamp_ms,"event_age_ms":event_age_ms,"received_ns":frame.received_ns,"received_wall_ms":frame.received_wall_ms,"freshness_limit_ms":800,"payload_sha256":source.map(|s|s.payload_sha256.as_str())});
  self.invalid_audit.push_back(audit_row.clone());
  if category=="market_event_stale_or_clock_skew"{self.push_stale_frame_audit(audit_row);}
 }
 fn trim_stale_frame_audit(&mut self){
  while self.stale_frame_audit.len()>STALE_FRAME_AUDIT_LIMIT||self.stale_frame_audit_bytes>STALE_FRAME_AUDIT_BYTES_LIMIT{
   let Some(old)=self.stale_frame_audit.pop_front()else{break};
   self.stale_frame_audit_bytes=self.stale_frame_audit_bytes.saturating_sub(audit_value_bytes(&old));
   self.metrics.stale_frame_audit_evictions=self.metrics.stale_frame_audit_evictions.saturating_add(1);
  }
 }
 fn push_stale_frame_audit(&mut self,value:serde_json::Value){
  let bytes=audit_value_bytes(&value);
  if bytes>STALE_FRAME_AUDIT_BYTES_LIMIT{self.metrics.stale_frame_audit_evictions=self.metrics.stale_frame_audit_evictions.saturating_add(1);return}
  self.stale_frame_audit.push_back(value);
  self.stale_frame_audit_bytes=self.stale_frame_audit_bytes.saturating_add(bytes);
  self.trim_stale_frame_audit();
 }
 fn annotate_last_stale_gate(&mut self,frame:&RawFrame,processing_ns:u64,processing_wall_ms:i64,event_ms:i64,sequence:Option<i64>,prev_seq:Option<i64>,prior_l2_sequence:Option<i64>,action:&str){
  let receive_age=frame.received_wall_ms.checked_sub(event_ms);
  let processing_age=processing_wall_ms.checked_sub(event_ms);
  let receive_delay=processing_ns.saturating_sub(frame.received_ns);
  let reason=if receive_delay>800_000_000{"receive_to_process_over_800ms"}
   else if processing_ns<frame.received_ns{"monotonic_clock_regression"}
   else if !processing_age.is_some_and(|age|(0..=800).contains(&age)){"exchange_age_over_800ms_or_clock_skew"}
   else{"stale_gate_unspecified"};
  let previous_bytes=self.stale_frame_audit.back().map(audit_value_bytes).unwrap_or(0);
  if let Some(row)=self.stale_frame_audit.back_mut(){
   row["processing_ns"]=serde_json::json!(processing_ns);
   row["processing_wall_ms"]=serde_json::json!(processing_wall_ms);
   row["processing_delay_ns"]=serde_json::json!(receive_delay);
   row["exchange_age_at_receive_ms"]=serde_json::json!(receive_age);
   row["exchange_age_at_processing_ms"]=serde_json::json!(processing_age);
   row["sequence"]=serde_json::json!(sequence);
   row["prev_seq_id"]=serde_json::json!(prev_seq);
   row["pre_apply_l2_sequence"]=serde_json::json!(prior_l2_sequence);
   row["action"]=serde_json::json!(action);
   row["freshness_reject_reason"]=serde_json::json!(reason);
  }
  let current_bytes=self.stale_frame_audit.back().map(audit_value_bytes).unwrap_or(0);
  self.stale_frame_audit_bytes=self.stale_frame_audit_bytes.saturating_sub(previous_bytes).saturating_add(current_bytes);
  self.trim_stale_frame_audit();
 }
 fn record_okx_books_invalid(&mut self,frame:&RawFrame,reason:&str,prior_state:Option<(i64,usize,usize)>){
  self.record_invalid(frame,"okx_book_parse_or_sequence",true);
  if self.okx_books_invalid_audit.len()>=self.okx_books_invalid_audit_limit{
   self.okx_books_invalid_audit.pop_front();
   self.metrics.okx_books_invalid_audit_evictions=self.metrics.okx_books_invalid_audit_evictions.saturating_add(1);
  }
  let value=serde_json::from_str::<serde_json::Value>(&frame.raw).unwrap_or(serde_json::Value::Null);
  let null=serde_json::Value::Null;
  let row=value.get("data").and_then(serde_json::Value::as_array).and_then(|rows|rows.first()).unwrap_or(&null);
  let source=frame.source.as_ref();
  let instrument=value["arg"]["instId"].as_str().and_then(|s|s.strip_suffix("-USDT")).map(str::to_string);
  let timestamp_ms=row["ts"].as_str().and_then(|s|s.parse::<i64>().ok());
  let event_age_ms=timestamp_ms.and_then(|ts|frame.received_wall_ms.checked_sub(ts));
  self.okx_books_invalid_audit.push_back(serde_json::json!({
   "category":"okx_book_parse_or_sequence","failure_reason":reason,
   "source":source.map(|s|s.session_id.as_str()),"connection_generation":source.map(|s|s.connection_epoch),
   "selection_epoch":frame.epoch,"instrument":instrument,"inst_id":value["arg"]["instId"],
   "channel":value["arg"]["channel"],"action":value["action"],
   "seq_id":row["seqId"].as_i64(),"prev_seq_id":row["prevSeqId"].as_i64(),
   "expected_previous_seq":prior_state.map(|s|s.0),
   "pre_apply_book":prior_state.map(|(seq,bids,asks)|serde_json::json!({"sequence":seq,"bids_count":bids,"asks_count":asks})),
   "snapshot":value["action"]=="snapshot","timestamp_ms":timestamp_ms,"event_age_ms":event_age_ms,
   "checksum":row.get("checksum").cloned().unwrap_or(serde_json::Value::Null),
   "checksum_validation":"not_performed","bids_count":row["bids"].as_array().map(Vec::len),
   "asks_count":row["asks"].as_array().map(Vec::len),"raw_bytes":frame.raw.len(),
   "raw_sha256":source.map(|s|s.payload_sha256.as_str()),"received_ns":frame.received_ns,
   "received_wall_ms":frame.received_wall_ms
  }));
 }
 pub fn invalid_audit(&self)->Vec<serde_json::Value>{self.invalid_audit.iter().cloned().collect()}
 pub fn stale_frame_audit(&self)->Vec<serde_json::Value>{self.stale_frame_audit.iter().cloned().collect()}
 pub fn okx_books_invalid_audit(&self)->Vec<serde_json::Value>{self.okx_books_invalid_audit.iter().cloned().collect()}
 pub fn selection_audit(&self)->Vec<serde_json::Value>{self.selection_audit.iter().cloned().collect()}
 pub fn readiness_audit(&self)->Vec<serde_json::Value>{self.readiness_audit.iter().cloned().collect()}
 pub fn r1247_frame_stage_samples(&self)->Vec<serde_json::Value>{self.r1247_stage_audit.iter().cloned().collect()}
 pub fn r1279_frame_trace_samples(&self)->Vec<serde_json::Value>{self.r1279_terminal_samples.lock().map(|s|s.iter().cloned().collect()).unwrap_or_default()}
 pub fn r1279_trace_dropped(&self)->u64{self.r1279_trace_dropped.load(std::sync::atomic::Ordering::Relaxed)}
 pub fn r1279_trace_sampled_out(&self)->u64{self.r1279_trace_sampled_out.load(std::sync::atomic::Ordering::Relaxed)}
 pub fn r1247_frame_stage_evictions(&self)->u64{self.r1247_stage_evictions}
 pub fn r1247_selection_evaluations(&self)->Vec<serde_json::Value>{self.r1247_selection_evaluations.iter().cloned().collect()}
 pub fn r1247_selection_evaluation_evictions(&self)->u64{self.r1247_selection_eval_evictions}
 fn record_r1247_stage(&mut self,frame:&RawFrame,base:Option<&str>,event_ms:Option<i64>,sequence:Option<i64>,
   status:&str,reason:Option<&str>,t4_wall_ms:i64,parse_start_ns:Option<u64>,parse_end_ns:Option<u64>,
   validation_end_ns:Option<u64>,previous_received_ns:Option<u64>){
  if !self.r1247_trace_enabled{return}
  let Some(source)=frame.source.as_ref()else{return};
  if source.accepted_id==0||source.accepted_id%64!=0{return}
  let trace_started_ns=crate::ingress_runtime_r1205::mono_ns();
  let raw_value=serde_json::from_str::<serde_json::Value>(&frame.raw).unwrap_or(serde_json::Value::Null);
  let raw_row=raw_value["data"].as_array().and_then(|rows|rows.first()).unwrap_or(&raw_value["data"]);
  let base=base.map(str::to_string).or_else(||raw_value["arg"]["instId"].as_str().and_then(|s|s.strip_suffix("-USDT")).map(str::to_string)).or_else(||raw_value["stream"].as_str().and_then(|s|s.split_once('@').map(|(symbol,_)|symbol.strip_suffix("usdt").unwrap_or(symbol).to_ascii_uppercase())));
  let event_ms=event_ms.or_else(||raw_row["ts"].as_str().and_then(|s|s.parse().ok())).or_else(||raw_row["E"].as_i64()).or_else(||raw_row["T"].as_i64());
  let sequence=sequence.or_else(||raw_row["seqId"].as_i64()).or_else(||raw_row["u"].as_i64()).or_else(||raw_row["lastUpdateId"].as_i64());
  let timing=source.r1247_timing.as_ref();
  let t1=timing.and_then(|t|t.t1_ws_text_mono_ns);
  let t3=timing.and_then(|t|t.t3_enqueue_mono_ns);
  let t4=timing.and_then(|t|t.t4_engine_start_ns);
  let t1_wall=timing.and_then(|t|t.t1_ws_text_wall_ms);
  let exchange_age_t1=t1_wall.and_then(|w|event_ms.and_then(|e|crate::ingress_runtime_r1205::exchange_age_ms(e,w)));
  let exchange_age_t4=event_ms.and_then(|e|crate::ingress_runtime_r1205::exchange_age_ms(e,t4_wall_ms));
  let id=format!("{}:{}:{}",source.session_id,source.connection_epoch,source.accepted_id);
  let mut value=serde_json::json!({
   "trace_id":id,"accepted_id":source.accepted_id,"source":source.session_id,
   "venue":format!("{:?}",frame.venue),"instrument":base,"connection_epoch":source.connection_epoch,
   "selection_epoch":source.selection_epoch,"sequence":sequence,"exchange_event_ts_ms":event_ms,
   "exchange_age_at_t1_ms":exchange_age_t1,"exchange_age_at_t4_ms":exchange_age_t4,
   "t1_ws_text_mono_ns":t1,"t1_ws_text_wall_ms":t1_wall,
   "previous_t1_ws_text_mono_ns":timing.and_then(|t|t.t1_previous_instrument_mono_ns),
   "time_since_last_local_ws_message_ns":timing.and_then(|t|t.t1_intermessage_gap_ns),
   "previous_exchange_event_ts_ms":timing.and_then(|t|t.previous_exchange_event_ts_ms),
   "exchange_event_timestamp_advanced":timing.and_then(|t|t.exchange_event_timestamp_advanced),
   "exchange_event_timestamp_delta_ms":timing.and_then(|t|t.previous_exchange_event_ts_ms.zip(event_ms).map(|(prior,current)|current.saturating_sub(prior))),
   "frame_received_ns":frame.received_ns,"frame_received_wall_ms":frame.received_wall_ms,
   "t3_enqueue_mono_ns":t3,"t3_queue_wait_ns":timing.and_then(|t|t.t3_queue_wait_ns),
   "t4_engine_start_ns":t4,"t4_engine_wall_ms":timing.and_then(|t|t.t4_engine_wall_ms),
   "t2_parse_start_ns":parse_start_ns,
   "t2_parse_end_ns":parse_end_ns,"t4_validation_end_ns":validation_end_ns,
   "t1_to_t2_ns":parse_start_ns.zip(t1).map(|(a,b)|a.saturating_sub(b)),
   "t1_to_t3_ns":t3.zip(t1).map(|(a,b)|a.saturating_sub(b)),
   "websocket_to_enqueue_ns":t3.zip(t1).map(|(a,b)|a.saturating_sub(b)),
   "enqueue_to_engine_ns":t4.zip(t3).map(|(a,b)|a.saturating_sub(b)),
   "engine_to_parse_ns":parse_start_ns.zip(t4).map(|(a,b)|a.saturating_sub(b)),
   "parse_duration_ns":parse_end_ns.zip(parse_start_ns).map(|(a,b)|a.saturating_sub(b)),
   "validation_duration_after_parse_ns":validation_end_ns.zip(parse_end_ns).map(|(a,b)|a.saturating_sub(b)),
   "previous_received_ns":previous_received_ns,
   "time_since_last_accepted_book_ns":previous_received_ns.map(|p|frame.received_ns.saturating_sub(p)),
   "payload_sha256":source.payload_sha256,"status":status,"reason":reason
  });
  value["telemetry_record_cost_ns"]=serde_json::json!(crate::ingress_runtime_r1205::mono_ns().saturating_sub(trace_started_ns));
  if self.r1247_stage_audit.len()>=1024{self.r1247_stage_audit.pop_front();self.r1247_stage_evictions=self.r1247_stage_evictions.saturating_add(1);}
  self.r1247_stage_audit.push_back(value);
 }
 fn push_readiness_audit(&mut self,value:serde_json::Value){
  let kind=value["kind"].as_str().unwrap_or("unknown").to_string();
  let counter=self.metrics.readiness_audit_kind_counts.entry(kind).or_default();*counter=(*counter).saturating_add(1);
  let bytes=audit_value_bytes(&value);
  if bytes>READINESS_AUDIT_BYTES_LIMIT{self.metrics.readiness_audit_evictions=self.metrics.readiness_audit_evictions.saturating_add(1);return}
  while self.readiness_audit.len()>=READINESS_AUDIT_LIMIT||self.readiness_audit_bytes.saturating_add(bytes)>READINESS_AUDIT_BYTES_LIMIT{
   let Some(old)=self.readiness_audit.pop_front()else{break};
   self.readiness_audit_bytes=self.readiness_audit_bytes.saturating_sub(audit_value_bytes(&old));
   self.metrics.readiness_audit_evictions=self.metrics.readiness_audit_evictions.saturating_add(1);
  }
  self.readiness_audit_bytes=self.readiness_audit_bytes.saturating_add(bytes);
  self.readiness_audit.push_back(value);
 }
 fn readiness_predicate_mask(&self,b:&SourceBook,ns:u64,wall:i64)->u16{
  let h=self.manager.state(&b.manager_id);
  let valid_receive_age_ms=h.and_then(|x|x.last_valid_receive_ns.map(|t|ns.saturating_sub(t)/1_000_000));
  let valid_exchange_age_ms=h.and_then(|x|x.last_valid_exchange_ms.and_then(|t|wall.checked_sub(t)));
  let last_event_age_at_ws_ms=h.and_then(|x|x.last_receive_wall_ms.zip(x.last_exchange_ms).and_then(|(received,event)|received.checked_sub(event)));
  let (minimum_good_samples,qualification_dwell_ns,_)=self.manager.qualification_requirements(&b.manager_id);
  let values=[
   h.is_some_and(|x|x.tcp),h.is_some_and(|x|x.tls),h.is_some_and(|x|x.websocket),h.is_some_and(|x|x.subscribed),
   valid_receive_age_ms.is_some_and(|age|age<=800),valid_exchange_age_ms.is_some_and(|age|(0..=800).contains(&age)),
   last_event_age_at_ws_ms.is_some_and(|age|(0..=800).contains(&age)),
   h.is_some_and(|x|x.good_samples>=minimum_good_samples),
   h.and_then(|x|x.first_good_ns).is_some_and(|first|ns>=first&&ns-first>=qualification_dwell_ns),
   h.is_some_and(|x|x.hot_ready),ns.saturating_sub(b.frame.received_ns)/1_000_000<=800,
   b.update.book.exchange_ts_ms.and_then(|ts|wall.checked_sub(ts)).is_some_and(|age|(0..=800).contains(&age)),
   !matches!(b.state,State::Degraded|State::Quarantined|State::Reconnecting),self.ready(b,ns,wall)
  ];
  values.into_iter().enumerate().fold(0u16,|mask,(index,ready)|if ready{mask|(1u16<<index)}else{mask})
 }
 fn readiness_predicates_json(mask:u16)->serde_json::Value{
  let mut predicates=serde_json::Map::new();
  for(index,name)in READINESS_PREDICATE_NAMES.iter().enumerate(){predicates.insert((*name).into(),serde_json::json!(mask&(1u16<<index)!=0));}
  serde_json::Value::Object(predicates)
 }
 fn readiness_failed_predicates(mask:u16)->Vec<&'static str>{
  READINESS_PREDICATE_NAMES.iter().enumerate().filter_map(|(index,name)|(mask&(1u16<<index)==0).then_some(*name)).collect()
 }
 fn readiness_current_context(&self,b:&SourceBook,ns:u64,wall:i64,mask:u16)->serde_json::Value{
  let h=self.manager.state(&b.manager_id);
  let manager_receive_age_ms=h.and_then(|x|x.last_receive_ns.map(|t|ns.saturating_sub(t)/1_000_000));
  let valid_receive_age_ms=h.and_then(|x|x.last_valid_receive_ns.map(|t|ns.saturating_sub(t)/1_000_000));
  let valid_exchange_age_ms=h.and_then(|x|x.last_valid_exchange_ms.and_then(|t|wall.checked_sub(t)));
  let last_event_age_at_ws_ms=h.and_then(|x|x.last_receive_wall_ms.zip(x.last_exchange_ms).and_then(|(received,event)|received.checked_sub(event)));
  let processing_delay_ns=h.and_then(|x|x.last_observed_ns.zip(x.last_receive_ns).map(|(observed,received)|observed.saturating_sub(received)));
  let qualification_elapsed_ns=h.and_then(|x|x.first_good_ns.map(|first|ns.saturating_sub(first)));
  let (minimum_good_samples,qualification_dwell_ns,requalification_after_prior_hot)=self.manager.qualification_requirements(&b.manager_id);
  let scope=key(b.frame.venue,&b.update.base);
  let selected=self.selected.get(&scope).map(|source|source.session.as_str())==b.frame.source.as_ref().map(|source|source.session_id.as_str());
  let authority_active=self.authority.read().map(|authority|authority.contains_key(&scope)).unwrap_or(false);
  serde_json::json!({
   "generation":b.manager_id.generation,"selection_epoch":self.selected.get(&scope).map(|source|source.epoch),
   "source_selection_epoch":b.frame.source.as_ref().map(|source|source.selection_epoch),"selected":selected,"authority_active":authority_active,
   "failed_predicates":Self::readiness_failed_predicates(mask),"book_state":format!("{:?}",b.state),
   "book_receive_age_ms":ns.saturating_sub(b.frame.received_ns)/1_000_000,
   "book_exchange_age_ms":b.update.book.exchange_ts_ms.and_then(|ts|wall.checked_sub(ts)),
   "book_exchange_ts_ms":b.update.book.exchange_ts_ms,"book_sequence":b.update.book.sequence,
   "manager_state":h.and_then(|x|x.state.map(|state|format!("{state:?}"))),"manager_good_samples":h.map(|x|x.good_samples),
   "manager_first_good_ns":h.and_then(|x|x.first_good_ns),"manager_qualification_elapsed_ns":qualification_elapsed_ns,
   "manager_last_receive_age_ms":manager_receive_age_ms,"manager_last_valid_receive_age_ms":valid_receive_age_ms,
   "manager_last_valid_exchange_age_ms":valid_exchange_age_ms,"manager_last_event_age_at_websocket_receive_ms":last_event_age_at_ws_ms,
   "manager_last_processing_delay_ns":processing_delay_ns,"manager_stale":h.map(|x|x.stale),"manager_errors":h.map(|x|x.errors),
   "minimum_good_samples":minimum_good_samples,"qualification_dwell_ms":qualification_dwell_ns/1_000_000,
   "requalification_after_prior_hot":requalification_after_prior_hot,"freshness_limit_ms":800
  })
 }
 // R1393: source membership index derived only from self.books; never HOT authority.
 fn remove_book(&mut self,key:&(String,String))->Option<SourceBook>{
  let book=self.books.remove(key)?;self.ucm_book_order=None;
  if self.manager.remote().is_some(){self.ucm_clock_index.remove(&key.1);self.ucm_fact_refresh.insert(key.1.clone());}
  if let Some(sessions)=self.book_scope_sessions.get_mut(&key.1){
   sessions.remove(&key.0);
   if sessions.is_empty(){self.book_scope_sessions.remove(&key.1);}
  }
  Some(book)
 }
 fn record_readiness_transitions(&mut self,k:&str,ns:u64,wall:i64,ordered_scope_sessions:&[String]){
  // R1397: preserve source-table order for UCM readiness transitions.
  let entries=ordered_scope_sessions.iter().filter_map(|session|{
   self.books.get(&(session.clone(),k.to_string()))
    .map(|book|(session.clone(),self.readiness_predicate_mask(book,ns,wall)))
  }).collect::<Vec<_>>();
  for(session,mask)in entries{
   let (prior,evicted)=self.manager.observe_readiness_mask(k,&session,mask);
   let context=if prior!=Some(mask){self.books.get(&(session.clone(),k.to_string())).map(|book|self.readiness_current_context(book,ns,wall,mask)).unwrap_or(serde_json::Value::Null)}else{serde_json::Value::Null};
   self.render_readiness_transition(k,ns,session,mask,prior,evicted,context);
  }
 }
 fn render_readiness_transition(&mut self,k:&str,ns:u64,session:String,mask:u16,prior:Option<u16>,evicted:bool,context:serde_json::Value){
   if evicted{self.metrics.readiness_state_evictions=self.metrics.readiness_state_evictions.saturating_add(1);}
   match prior{
    None=>{
     self.push_readiness_audit(serde_json::json!({"kind":"source_readiness_baseline","scope":k,"session":session,"mono_ns":ns,"current_predicates":Self::readiness_predicates_json(mask),"failed_predicates":Self::readiness_failed_predicates(mask),"context":context}));
    }
    Some(before) if before!=mask=>{
     let mut lost=Vec::new();let mut gained=Vec::new();
     for(index,name)in READINESS_PREDICATE_NAMES.iter().enumerate(){
      let old=before&(1u16<<index)!=0;let new=mask&(1u16<<index)!=0;
      if old&&!new{lost.push(*name);}else if !old&&new{gained.push(*name);}
     }
     self.push_readiness_audit(serde_json::json!({"kind":"source_readiness_predicate_change","scope":k,"session":session,"mono_ns":ns,"lost_predicates":lost,"gained_predicates":gained,"current_predicates":Self::readiness_predicates_json(mask),"failed_predicates":Self::readiness_failed_predicates(mask),"context":context}));
    }
    Some(_)=>{}
   }
 }
 fn record_dual_hot_transition(&mut self,k:&str,ready:&[String],ns:u64,_wall:i64){
  let transition=self.manager.observe_dual_hot(k,ready.len()>=2,ns);
  self.render_dual_hot_transition(k,ready,ns,transition);
 }
 fn render_dual_hot_transition(&mut self,k:&str,ready:&[String],ns:u64,transition:DualHotTransition){
  match transition{
   DualHotTransition::Established=>{
    self.push_readiness_audit(serde_json::json!({"kind":"two_hot_sources_established","scope":k,"mono_ns":ns,"ready_sources":ready,"source_count":ready.len()}));
   }
   DualHotTransition::Restored{deficit_duration_ns}=>{
    self.push_readiness_audit(serde_json::json!({"kind":"two_hot_sources_restored","scope":k,"mono_ns":ns,"deficit_duration_ns":deficit_duration_ns,"ready_sources":ready,"source_count":ready.len()}));
   }
   DualHotTransition::DeficitOnset=>{
    self.push_readiness_audit(serde_json::json!({"kind":"two_hot_source_deficit_onset","scope":k,"mono_ns":ns,"ready_sources":ready,"source_count":ready.len()}));
   }
   DualHotTransition::None=>{}
  }
 }
 fn quarantine_session(&mut self,id:&str){
  let ids=self.manager_ids.iter().filter(|((session,_),_)|session==id).map(|(_,source)|source.clone()).collect::<Vec<_>>();
  self.manager.group(ids,UcmGroupAction::Quarantine);
  let scopes=self.books.iter_mut().filter_map(|((session,scope),book)|if session==id{if let Some(w)=&book.snapshot_witness{w.revoke();}book.state=State::Quarantined;Some(scope.clone())}else{None}).collect::<HashSet<_>>();
  if let Ok(mut auth)=self.authority.write(){for scope in scopes{auth.remove(&scope);}}
 }
 fn fresh(b:&SourceBook,ns:u64,wall:i64)->bool{ns>=b.frame.received_ns&&ns-b.frame.received_ns<=800_000_000&&b.update.book.exchange_ts_ms.and_then(|e|wall.checked_sub(e)).is_some_and(|a|(0..=800).contains(&a))}
 // The Engine-owned half of readiness. No UCM read and no authority grant.
 fn book_locally_ready(&self,b:&SourceBook,ns:u64,wall:i64)->bool{
  self.manager_ids.get(&(b.manager_id.logical_session.clone(),key(b.frame.venue,&b.update.base)))==Some(&b.manager_id)
   && self.sources.get(&b.manager_id.logical_session).is_some_and(|s|!matches!(s,State::Quarantined|State::Reconnecting|State::Degraded))
   && b.snapshot_witness.as_ref().is_some_and(|w|w.matches(b.frame.venue,&b.update.base,&b.manager_id.logical_session,b.manager_id.generation))
   && Self::fresh(b,ns,wall)&&!matches!(b.state,State::Degraded|State::Quarantined|State::Reconnecting)
 }
 fn ready(&self,b:&SourceBook,ns:u64,wall:i64)->bool{
  if self.manager.remote().is_some(){
   self.book_locally_ready(b,ns,wall)&&self.manager.view(&b.manager_id).is_some_and(|v|v.source_ready_at(&b.manager_id,ns,wall))
  }else{
   self.manager.state(&b.manager_id).is_some_and(|h|h.hot_ready)&&Self::fresh(b,ns,wall)&&!matches!(b.state,State::Degraded|State::Quarantined|State::Reconnecting)
  }
 }
 fn ucm_book_facts(&mut self,k:&str,anchor:SourceId,ns:u64,wall:i64)->UcmBookFacts{
  let started=self.r1273_profile_enabled.then(std::time::Instant::now);
  let order=self.ucm_ordered_sessions(k);
  self.ucm_fact_refresh.remove(k);
  self.ucm_clock_index.update(k,ns,wall,order.iter().filter_map(|session|self.books.get(&(session.clone(),k.to_string())).map(|b|(b.frame.received_ns,b.update.book.exchange_ts_ms))));
  let prior=self.selected.get(k);
  let highwater=self.highwater.get(k).copied();
  let books=order.iter().filter_map(|session|{
   let book=self.books.get(&(session.clone(),k.to_string()))?;
   if !self.book_locally_ready(book,ns,wall){return None}
   let selected=prior.is_some_and(|p|p.session==*session);
   let equivalent_prior=prior.is_some_and(|p|!selected&&self.books.get(&(p.session.clone(),k.to_string())).is_some_and(|old|same_published_book(old,book)));
   Some(UcmBookFact{source:book.manager_id.clone(),advances:advances_highwater(book.frame.venue,book.update.book.sequence.unwrap_or(0),book.update.book.exchange_ts_ms.unwrap_or(0),highwater),selected,equivalent_prior})
  }).collect();
  self.r1273_record_elapsed("r1408b_fact_build",started);
  UcmBookFacts{anchor,books,now_ns:ns,now_ms:wall}
 }
 fn ucm_event_advice_matches(&self,k:&str,inputs:&HotSelectionInputs,event:&UcmEventAdvice)->bool{
  let ready=inputs.ready.iter().filter_map(|session|self.books.get(&(session.clone(),k.to_string())).map(|b|&b.manager_id));
  ready.eq(event.ready.iter())&&inputs.candidates==event.eligible
 }
 fn readiness_snapshot(&self,b:&SourceBook,ns:u64,wall:i64)->serde_json::Value{
  let h=self.manager.state(&b.manager_id);
  let manager_receive_age_ms=h.and_then(|x|x.last_receive_ns.map(|t|ns.saturating_sub(t)/1_000_000));
  let valid_receive_age_ms=h.and_then(|x|x.last_valid_receive_ns.map(|t|ns.saturating_sub(t)/1_000_000));
  let valid_exchange_age_ms=h.and_then(|x|x.last_valid_exchange_ms.and_then(|t|wall.checked_sub(t)));
  let last_event_age_at_ws_ms=h.and_then(|x|x.last_receive_wall_ms.zip(x.last_exchange_ms).and_then(|(received,event)|received.checked_sub(event)));
  let processing_delay_ns=h.and_then(|x|x.last_observed_ns.zip(x.last_receive_ns).map(|(observed,received)|observed.saturating_sub(received)));
  let qualification_elapsed_ns=h.and_then(|x|x.first_good_ns.map(|first|ns.saturating_sub(first)));
  let book_receive_age_ms=ns.saturating_sub(b.frame.received_ns)/1_000_000;
  let book_exchange_age_ms=b.update.book.exchange_ts_ms.and_then(|t|wall.checked_sub(t));
  let tcp_ready=h.is_some_and(|x|x.tcp);
  let tls_ready=h.is_some_and(|x|x.tls);
  let websocket_ready=h.is_some_and(|x|x.websocket);
  let subscribed=h.is_some_and(|x|x.subscribed);
  let last_receive_fresh=valid_receive_age_ms.is_some_and(|a|a<=800);
  let last_exchange_fresh=valid_exchange_age_ms.is_some_and(|a|(0..=800).contains(&a));
  let last_event_fresh_at_ws=last_event_age_at_ws_ms.is_some_and(|a|(0..=800).contains(&a));
  let (minimum_good_samples,qualification_dwell_ns,requalification_after_prior_hot)=self.manager.qualification_requirements(&b.manager_id);
  let min_samples_met=h.is_some_and(|x|x.good_samples>=minimum_good_samples);
  let qualify_dwell_met=h.and_then(|x|x.first_good_ns).is_some_and(|t|ns>=t&&ns-t>=qualification_dwell_ns);
  let manager_hot_ready=h.is_some_and(|x|x.hot_ready);
  let book_receive_fresh=book_receive_age_ms<=800;
  let book_exchange_fresh=book_exchange_age_ms.is_some_and(|a|(0..=800).contains(&a));
  let book_state_usable=!matches!(b.state,State::Degraded|State::Quarantined|State::Reconnecting);
  let effective_ready=self.ready(b,ns,wall);
  let predicates=serde_json::json!({
   "tcp_ready":tcp_ready,"tls_ready":tls_ready,"websocket_ready":websocket_ready,"subscribed":subscribed,
   "last_valid_receive_fresh":last_receive_fresh,"last_valid_exchange_fresh":last_exchange_fresh,
   "last_event_fresh_at_websocket_receive":last_event_fresh_at_ws,
   "minimum_good_samples_met":min_samples_met,"qualification_dwell_met":qualify_dwell_met,
   "manager_hot_ready":manager_hot_ready,"book_receive_fresh":book_receive_fresh,
   "book_exchange_fresh":book_exchange_fresh,"book_state_usable":book_state_usable,
   "effective_ready":effective_ready
  });
  let failed_predicates=["tcp_ready","tls_ready","websocket_ready","subscribed","last_valid_receive_fresh","last_valid_exchange_fresh","last_event_fresh_at_websocket_receive","minimum_good_samples_met","qualification_dwell_met","manager_hot_ready","book_receive_fresh","book_exchange_fresh","book_state_usable","effective_ready"]
   .into_iter().filter(|name|!predicates[*name].as_bool().unwrap_or(false)).collect::<Vec<_>>();
  let scope=key(b.frame.venue,&b.update.base);
  let selected=self.selected.get(&scope).map(|s|s.session.as_str())==b.frame.source.as_ref().map(|p|p.session_id.as_str());
  let authority_active=self.authority.read().map(|a|a.contains_key(&scope)).unwrap_or(false);
  let mut snapshot=serde_json::json!({
   "scope":scope,"venue":format!("{:?}",b.frame.venue),"instrument":b.update.base,
   "session":b.frame.source.as_ref().map(|p|p.session_id.as_str()),"physical_connection":b.manager_id.physical_connection,
   "generation":b.manager_id.generation,"selection_epoch":self.selected.get(&scope).map(|s|s.epoch),
   "source_selection_epoch":b.frame.source.as_ref().map(|p|p.selection_epoch),
   "selected":selected,"authority_active":authority_active,"predicates":predicates
  });
  let book_details=serde_json::json!({
   "book_state":format!("{:?}",b.state),"book_fresh":Self::fresh(b,ns,wall),
   "book_receive_age_ms":book_receive_age_ms,"book_exchange_age_ms":book_exchange_age_ms,
   "book_exchange_ts_ms":b.update.book.exchange_ts_ms,"book_sequence":b.update.book.sequence
  });
  let manager_details=serde_json::json!({
   "manager_state":h.and_then(|x|x.state.map(|v|format!("{v:?}"))),"manager_hot_ready":manager_hot_ready,
   "manager_effective_hot_ready":manager_hot_ready&&Self::fresh(b,ns,wall),"effective_ready":effective_ready,
   "manager_subscribed":h.map(|x|x.subscribed),"manager_tcp":h.map(|x|x.tcp),
   "manager_tls":h.map(|x|x.tls),"manager_websocket":h.map(|x|x.websocket),
   "manager_good_samples":h.map(|x|x.good_samples),"manager_first_good_ns":h.and_then(|x|x.first_good_ns),
   "manager_qualification_elapsed_ns":qualification_elapsed_ns,
   "manager_last_receive_age_ms":manager_receive_age_ms,"manager_last_valid_receive_age_ms":valid_receive_age_ms,
   "manager_last_valid_exchange_age_ms":valid_exchange_age_ms,
   "manager_last_event_age_at_websocket_receive_ms":last_event_age_at_ws_ms,
   "manager_last_receive_ns":h.and_then(|x|x.last_receive_ns),"manager_last_exchange_ms":h.and_then(|x|x.last_exchange_ms),
   "manager_last_receive_wall_ms":h.and_then(|x|x.last_receive_wall_ms),
   "manager_last_observed_ns":h.and_then(|x|x.last_observed_ns),"manager_last_processing_delay_ns":processing_delay_ns,
   "manager_stale":h.map(|x|x.stale),"manager_errors":h.map(|x|x.errors)
  });
  let policy_details=serde_json::json!({
   "freshness_limit_ms":800,"minimum_good_samples":minimum_good_samples,"qualification_dwell_ms":qualification_dwell_ns/1_000_000,"requalification_after_prior_hot":requalification_after_prior_hot,
   "failed_predicates":failed_predicates,"quality":self.manager.quality(&b.manager_id)
  });
  for details in [book_details,manager_details,policy_details]{
   snapshot.as_object_mut().expect("snapshot object").extend(details.as_object().expect("details object").clone());
  }
  snapshot
 }
 fn trace(&mut self,value:serde_json::Value){if self.audit.len()>=self.audit_limit{self.audit.pop_front();self.metrics.audit_evictions+=1;}self.audit.push_back(value);}
 fn r1310_stage(&mut self,frame:&RawFrame,stage:&str,start:u64,end:u64){
  let Some(source)=frame.source.as_ref()else{return};
  if !source.r1279_timing.as_ref().is_some_and(|t|t.r1310_trace_sampled){return}
  let trace_id=frame_trace_id_r1304(frame);
  let mut row=crate::r1310_stage_event(stage,&trace_id,start,end);
  row["kind"]=serde_json::json!("r1310_pipeline");row["frame_trace_id"]=serde_json::json!(trace_id);
  row["ws_receive_ns"]=source.r1279_timing.as_ref().and_then(|t|t.ws_receive_ns).map_or(serde_json::Value::Null,|v|serde_json::json!(v));
  row["sequence"]=serde_json::json!(source.accepted_id);
  self.trace(row);
 }
 fn r1310_lifecycle(&mut self,frame:&RawFrame,event:&str,generation:Option<u64>){
  let Some(source)=frame.source.as_ref()else{return};
  if !source.r1279_timing.as_ref().is_some_and(|t|t.r1310_trace_sampled){return}
  self.trace(serde_json::json!({"kind":"r1310_lifecycle","event":event,"frame_trace_id":frame_trace_id_r1304(frame),"trace_id":frame_trace_id_r1304(frame),"mono_ns":crate::ingress_runtime_r1205::mono_ns(),"venue":format!("{:?}",frame.venue),"generation":generation,"accepted_id":source.accepted_id}));
 }
 pub fn enable_probe_audit(&mut self){self.audit_limit=8192;}
 pub fn audit(&self)->Vec<serde_json::Value>{self.audit.iter().cloned().collect()}
 fn finish(&self,f:&RawFrame,why:&str)->Result<(),String>{let mut frame=f.clone();if let Some(t)=frame.source.as_mut().and_then(|s|s.r1279_timing.as_mut()){let now=crate::ingress_runtime_r1205::mono_ns();t.terminalization_reason=Some(why.to_string());if t.frame_residence_end_ns.is_none(){t.frame_residence_end_ns=Some(now);}t.frame_residence_status=Some("terminalized".into());}self.quarantines.lock().map_err(|_|"quarantine poison")?.push((frame,why.into()));Ok(())}
 fn flush_quarantines(&self)->Result<(),String>{
  if self.store_worker.is_none(){self.quarantines.lock().map_err(|_|"quarantine poison")?.clear();return Ok(())}
  let start=crate::ingress_runtime_r1205::mono_ns();
  let items={let mut pending=self.quarantines.lock().map_err(|_|"quarantine poison")?;if pending.is_empty(){return Ok(())}for(frame,_)in pending.iter_mut(){if let Some(t)=frame.source.as_mut().and_then(|s|s.r1279_timing.as_mut()){t.quarantine_start_ns=Some(start);t.quarantine_status=Some("incomplete".into());}}pending.clone()};
  let committed=self.store_worker.as_ref().ok_or("R1205 worker missing")?.call(move|store,_|{store.quarantine_batch(&items)?;Ok(items)})?;
  let end=crate::ingress_runtime_r1205::mono_ns();
  if let Ok(mut samples)=self.r1279_terminal_samples.lock(){for(mut frame,_)in committed{if let Some(t)=frame.source.as_mut().and_then(|s|s.r1279_timing.as_mut()){t.quarantine_end_ns=Some(end);t.quarantine_status=Some("complete".into());t.terminal_ns=Some(end);}let Some(source)=frame.source.as_ref()else{continue};if source.r1279_timing.is_none(){continue}if source.accepted_id%64!=0{self.r1279_trace_sampled_out.fetch_add(1,std::sync::atomic::Ordering::Relaxed);continue}if samples.len()>=4096{samples.pop_front();self.r1279_trace_dropped.fetch_add(1,std::sync::atomic::Ordering::Relaxed);}samples.push_back(crate::r1279_trace_row(&frame,"quarantined","not_applicable_quarantine"));}}
  self.quarantines.lock().map_err(|_|"quarantine poison")?.clear();Ok(())
 }
 fn r1273_record_elapsed(&self,name:&str,start:Option<std::time::Instant>)->Option<u64>{let ns=start.map(|t|t.elapsed().as_nanos().min(u64::MAX as u128) as u64);if let Some(value)=ns{if let Ok(mut p)=self.r1273_profile.lock(){p.record(name,value);}}ns}
 fn r1273_record_ns(&self,name:&str,ns:u64){if self.r1273_profile_enabled{if let Ok(mut p)=self.r1273_profile.lock(){p.record(name,ns);}}}
 fn authority_write_timed<R>(&self,operation:impl FnOnce(&mut HashMap<String,Permit>)->R)->Result<R,String>{
  let wait_started=std::time::Instant::now();
  let mut guard=self.authority.write().map_err(|_|"authority poison")?;
  let wait_ns=wait_started.elapsed().as_nanos().min(u64::MAX as u128)as u64;
  let hold_started=std::time::Instant::now();
  let result=operation(&mut guard);
  drop(guard);
  let hold_ns=hold_started.elapsed().as_nanos().min(u64::MAX as u128)as u64;
  self.r1273_record_ns("ucm_authority_write_wait",wait_ns);
  self.r1273_record_ns("ucm_authority_write_hold",hold_ns);
  Ok(result)
 }
 fn r1273_json_decode_once(&self,raw:&str)->Option<serde_json::Value>{let start=self.r1273_profile_enabled.then(std::time::Instant::now);let parsed=crate::market_data_processor_r1378::MarketDataProcessor::decode_json(raw);self.r1273_record_elapsed("json_decode_once",start);parsed}
 pub fn receive_batch(&mut self,frames:Vec<RawFrame>,ns:u64,wall:i64)->Result<Vec<Output>,String>{let call_id=next_engine_call_id();let start=crate::ingress_runtime_r1205::mono_ns();let result=self.receive_batch_inner(frames,ns,wall,call_id);let end=crate::ingress_runtime_r1205::mono_ns();self.trace(serde_json::json!({"kind":"engine_call_span","call":"receive_batch","call_id":call_id,"start_ns":start,"end_ns":end,"duration_ns":end.saturating_sub(start),"status":"complete","outcome":if result.is_ok(){"ok"}else{"error"},"scope":"batch","ingress_batch_id":call_id}));result}
 fn receive_batch_inner(&mut self,mut frames:Vec<RawFrame>,ns:u64,wall:i64,receive_batch_id:u64)->Result<Vec<Output>,String>{
  self.manager.check()?;
  let profile_total=self.r1273_profile_enabled.then(std::time::Instant::now);if let Ok(mut p)=self.r1273_profile.lock(){p.batches=p.batches.saturating_add(1);}
  let call_begin_ns=crate::ingress_runtime_r1205::mono_ns();
  let trace_enabled=std::env::var("R1279_TRACE").ok().as_deref()==Some("1");
  let r1310_enabled=self.r1310_trace_enabled;let r1310_every=self.r1310_trace_sample_every.max(1);
  for frame in &mut frames {crate::ensure_frame_trace_id(frame);if let Some(source)=frame.source.as_mut(){let capture=r1310_enabled&&if self.store_worker.is_some(){source.accepted_id%r1310_every==0}else{let digest=openssl::sha::sha256(source.frame_trace_id.as_bytes());u64::from_be_bytes(digest[..8].try_into().unwrap_or([0;8]))%r1310_every==0};if r1310_enabled&&!capture{self.r1310_trace_sampled_out=self.r1310_trace_sampled_out.saturating_add(1);}if trace_enabled||capture{let timing=source.r1279_timing.get_or_insert_with(crate::R1279FrameTiming::default);if capture{self.r1310_trace_sampled_in=self.r1310_trace_sampled_in.saturating_add(1);timing.r1310_trace_sampled=true;timing.source_storage_durable=Some(self.store_worker.is_some());timing.ws_receive_ns.get_or_insert(frame.received_ns);}timing.receive_batch_id=Some(receive_batch_id);timing.receive_batch_start_ns=Some(call_begin_ns);timing.engine_receive_begin_ns=Some(call_begin_ns);timing.frame_hold_reason=Some("frame accepted in receive_batch; awaiting terminal selection or quarantine".into());timing.frame_residence_start_ns=Some(call_begin_ns);timing.frame_residence_status=Some("incomplete".into());}}}
  let accept_started_ns=ns;let accept_started_wall_ms=wall;
  self.metrics.received+=frames.len()as u64;
  let custody_started=self.r1273_profile_enabled.then(std::time::Instant::now);
  let source_durable_start=self.store_worker.as_ref().map(|_|crate::ingress_runtime_r1205::mono_ns());for frame in &mut frames{if let (Some(start),Some(t))=(source_durable_start,frame.source.as_mut().and_then(|s|s.r1279_timing.as_mut())){t.source_durable_start_ns=Some(start);}}
  let mut frames=if let Some(worker)=self.store_worker.as_ref(){
   worker.call(move|store,lock_wait_ns|{
    store.accept_batch_with_lock_wait(&mut frames,lock_wait_ns)?;
    Ok(frames)
   })?
  }else{
   for frame in &mut frames{
    let source=frame.source.as_mut().ok_or("source")?;
    source.accepted_id=0;
    source.journal_id.clear();
    source.accepted_ns=Some(ns);
    source.payload_sha256=openssl::sha::sha256(frame.raw.as_bytes()).iter().map(|b|format!("{b:02x}")).collect();
    if let Some(t)=source.r1279_timing.as_mut(){t.source_storage_durable=Some(false);}
   }
   frames
  };
  let custody_ns=self.r1273_record_elapsed(if self.store_worker.is_some(){"custody_accept_ack"}else{"custody_ram_validation"},custody_started).unwrap_or(0);
  let source_durable_end=self.store_worker.as_ref().map(|_|crate::ingress_runtime_r1205::mono_ns());for frame in &mut frames{if let (Some(end),Some(t))=(source_durable_end,frame.source.as_mut().and_then(|s|s.r1279_timing.as_mut())){t.source_durable_end_ns=Some(end);}}
  // The durable operation can block: qualify against clocks AFTER its commit.
  let (ns,wall)=self.clock.map(|clock|clock()).unwrap_or((ns,wall));
  for frame in &mut frames{let source=frame.source.as_mut().ok_or("source")?;source.accepted_ns=Some(ns);if let Some(timing)=source.r1247_timing.as_mut(){timing.t4_engine_start_ns=Some(ns);timing.t4_engine_wall_ms=Some(wall);}}
  let mut scopes=HashSet::new();let mut resync_requests=Vec::new();let frame_loop_started=self.r1273_profile_enabled.then(std::time::Instant::now);
  for mut f in frames{
   let s=f.source.as_ref().ok_or("provenance")?.session_id.clone();
   // In-process, non-durable Forwarder result. Remove it before any accepted
   // book is stored or published; Engine still applies all final safety gates.
   let mut forwarder_prepared=f.source.as_mut().and_then(|src|src.prepared.take()).map(|x|*x);
   if !self.sources.contains_key(&s){let validation=self.r1247_trace_enabled.then(crate::ingress_runtime_r1205::mono_ns);self.record_r1247_stage(&f,None,None,None,"rejected",Some("unknown_source"),wall,None,None,validation,None);self.finish(&f,"unknown_source")?;self.record_invalid(&f,"unknown_source",true);continue}
   if self.sources.get(&s).is_some_and(|state|matches!(*state,State::Quarantined|State::Reconnecting)){let validation=self.r1247_trace_enabled.then(crate::ingress_runtime_r1205::mono_ns);self.record_r1247_stage(&f,None,None,None,"rejected",Some("source_not_eligible"),wall,None,None,validation,None);self.finish(&f,"source_not_eligible")?;self.record_invalid(&f,"source_not_eligible_after_quarantine_or_reconnect",true);continue}
   if let Some(t)=f.source.as_mut().and_then(|s|s.r1279_timing.as_mut()){t.validation_start_ns=Some(crate::ingress_runtime_r1205::mono_ns());}
   let decoder_start_ns=f.source.as_ref().and_then(|s|s.r1279_timing.as_ref()).filter(|t|t.r1310_trace_sampled).map(|_|crate::ingress_runtime_r1205::mono_ns());
   if let Some(start)=decoder_start_ns{if let Some(t)=f.source.as_mut().and_then(|s|s.r1279_timing.as_mut()){t.ws_receive_ns.get_or_insert(f.received_ns);t.ws_decode_start_ns=Some(start);}}
   let decoded_frame=if let Some(prepared)=forwarder_prepared.as_mut(){
    prepared.decoded.take()
   }else{self.r1273_json_decode_once(&f.raw)};
   let decoder_end_ns=decoder_start_ns.map(|_|crate::ingress_runtime_r1205::mono_ns());
   if let (Some(_),Some(end))=(decoder_start_ns,decoder_end_ns){if let Some(t)=f.source.as_mut().and_then(|s|s.r1279_timing.as_mut()){t.ws_decode_end_ns=Some(end);}}
   self.trace(serde_json::json!({"kind":"r1304_pipeline","stage":"Decoder","trace_id":frame_trace_id_r1304(&f),"ok":decoded_frame.is_some()}));
   if let (Some(start),Some(end))=(decoder_start_ns,decoder_end_ns){self.r1310_stage(&f,"Decoder",start,end);}
   if let (Some(t),Some(value))=(f.source.as_mut().and_then(|s|s.r1279_timing.as_mut()),decoded_frame.as_ref()){
    t.instrument=value.pointer("/arg/instId").and_then(|v|v.as_str()).and_then(|s|s.strip_suffix("-USDT")).map(str::to_string)
      .or_else(||value.get("stream").and_then(|v|v.as_str()).and_then(|s|s.split_once('@').map(|(base,_)|base.strip_suffix("usdt").unwrap_or(base).to_ascii_uppercase())));
    let row=value.get("data").and_then(|v|v.as_array().and_then(|a|a.first()).or_else(||v.as_object().map(|_|v))).unwrap_or(value);
    let exchange_ts=row.get("ts").and_then(|v|v.as_str()).and_then(|s|s.parse::<i64>().ok()).or_else(||row.get("E").and_then(|v|v.as_i64())).or_else(||row.get("T").and_then(|v|v.as_i64()));if exchange_ts.is_some(){t.exchange_event_ts_ms=exchange_ts;}
   }
   let mut t2_parse_start_ns=self.r1247_trace_enabled.then(crate::ingress_runtime_r1205::mono_ns);
   let mut t2_parse_end_ns=None;
   if let Some(timing)=f.source.as_mut().and_then(|source|source.r1247_timing.as_mut()){timing.t2_parse_start_ns=t2_parse_start_ns;}
   let channel=if f.venue==Venue::OkxSpot{decoded_frame.as_ref().and_then(|v|v["arg"]["channel"].as_str().map(str::to_string))}else{None};
   // Reject stale OKX L2 frames before apply(): they break sequence continuity.
   if channel.as_deref()==Some("books"){
    let preflight=decoded_frame.as_ref().and_then(|v|{
     if !matches!(v["action"].as_str(),Some("snapshot"|"update")){return None}
     let base=v["arg"]["instId"].as_str()?.strip_suffix("-USDT")?.to_string();
     let rows=v["data"].as_array().filter(|rows|rows.len()==1)?;
     let event=rows[0]["ts"].as_str()?.parse::<i64>().ok()?;
     Some((base,event,rows[0]["seqId"].as_i64(),rows[0]["prevSeqId"].as_i64(),v["action"].as_str().unwrap_or_default().to_string()))
    });
    t2_parse_end_ns=self.r1247_trace_enabled.then(crate::ingress_runtime_r1205::mono_ns);
    if let Some(timing)=f.source.as_mut().and_then(|source|source.r1247_timing.as_mut()){timing.t2_parse_end_ns=t2_parse_end_ns;}
    if let Some((base,event_ms,sequence,prev_seq,action))=preflight{
     let freshness_started=self.r1273_profile_enabled.then(std::time::Instant::now);
     let fresh=ns>=f.received_ns&&ns-f.received_ns<=800_000_000&&
      wall.checked_sub(event_ms).is_some_and(|age|(0..=800).contains(&age));
     self.r1273_record_elapsed("freshness_gate",freshness_started);
     if !fresh{
      let k=key(f.venue,&base);let manager_id=self.ensure_manager_source_decoded(&f,&base,decoded_frame.as_ref())?;
      let bk=(s.clone(),k.clone());
      let previous_received_ns=self.books.get(&bk).map(|b|b.frame.received_ns);
      let validation_end_ns=self.r1247_trace_enabled.then(crate::ingress_runtime_r1205::mono_ns);
      if let Some(timing)=f.source.as_mut().and_then(|source|source.r1247_timing.as_mut()){timing.t4_validation_end_ns=validation_end_ns;}
      self.record_r1247_stage(&f,Some(&base),Some(event_ms),sequence,"rejected",Some("stale_or_clock"),wall,t2_parse_start_ns,t2_parse_end_ns,validation_end_ns,previous_received_ns);
      self.trace(serde_json::json!({"id":f.source.as_ref().map(|p|p.accepted_id),"source":s,"key":k,"received_ns":f.received_ns,"received_wall_ms":f.received_wall_ms,"accept_started_ns":accept_started_ns,"accept_started_wall_ms":accept_started_wall_ms,"accepted_ns":ns,"validation_wall_ms":wall,"E":event_ms,"sequence":sequence,"fresh":false,"qualified_before":self.books.get(&bk).is_some_and(|b|b.qualified),"previous_received_ns":self.books.get(&bk).map(|b|b.frame.received_ns)}));
      // A stale message never mutates the last accepted book. If the rejected
      // update advances the source sequence, request a bounded resnapshot now;
      // waiting for its successor would only reveal the already-proven gap.
      self.manager.observe_with_receive_wall(&manager_id,f.received_ns,event_ms,f.received_wall_ms,ns,wall,false);
      let prior_l2_sequence=forwarder_prepared.as_ref().and_then(|p|p.previous_l2.map(|state|state.0))
       .or_else(||self.market_data_processor.diagnostic_state(&s,&base).map(|state|state.0));
      let stale_advancing_delta=action=="update"&&
       sequence.is_some_and(|seq|prior_l2_sequence.map_or(true,|last|seq>last))&&
       prev_seq.is_some_and(|prev|prior_l2_sequence.map_or(true,|last|prev>=last));
      if action=="snapshot"{
       let identity=(s.clone(),base.clone());
       let scope=key(f.venue,&base);
       // A stale initial snapshot never seeded this session's L2 builder.
       // Request its replacement immediately; waiting for the next delta adds
       // avoidable sequence rejects while preserving the strict 800 ms gate.
       if !self.books.contains_key(&(s.clone(),scope)){
        self.resyncing.insert(identity.clone());
        if self.resubscribe_last_ns.get(&identity)
         .map_or(true,|last|ns.saturating_sub(*last)>=OKX_RESUBSCRIBE_RETRY_MIN_INTERVAL_NS){
         self.resubscribe_last_ns.insert(identity,ns);
         resync_requests.push((s.clone(),base.clone(),"stale initial snapshot".into()));
        }
       }
      }else if stale_advancing_delta{
       let identity=(s.clone(),base.clone());self.resyncing.insert(identity.clone());
       if self.resubscribe_last_ns.get(&identity)
        .map_or(true,|last|ns.saturating_sub(*last)>=OKX_RESUBSCRIBE_RETRY_MIN_INTERVAL_NS){
        self.resubscribe_last_ns.insert(identity,ns);
        resync_requests.push((s.clone(),base.clone(),"stale advancing delta".into()));
       }
      }
      self.finish(&f,"stale_or_clock")?;self.record_invalid(&f,"market_event_stale_or_clock_skew",true);
      self.annotate_last_stale_gate(&f,ns,wall,event_ms,sequence,prev_seq,prior_l2_sequence,&action);
      scopes.insert(k);continue
     }
    }
   }
   // After stale input or a sequence fault, no delta may rebuild state until a
   // fresh full snapshot arrives. This prevents a later delta from bridging a gap.
   let recovery_gate_started=self.r1273_profile_enabled.then(std::time::Instant::now);
   if channel.as_deref()==Some("books"){
    let waiting=decoded_frame.as_ref().and_then(|v|{
     let action=v["action"].as_str()?;
     if action!="update"{return None}
     let base=v["arg"]["instId"].as_str()?.strip_suffix("-USDT")?;
     Some((base.to_string(),self.resyncing.contains(&(s.clone(),base.to_string()))))
    });
    if let Some((base,true))=waiting{
     // Keep rejecting deltas until a snapshot arrives, but retry the
     // resubscribe when a new delta proves the snapshot is still missing.
     let identity=(s.clone(),base.clone());
     if self.resubscribe_last_ns.get(&identity)
      .map_or(true,|last|ns.saturating_sub(*last)>=OKX_RESUBSCRIBE_RETRY_MIN_INTERVAL_NS){
      self.resubscribe_last_ns.insert(identity,ns);
      resync_requests.push((s.clone(),base.clone(),"delta before recovery snapshot".into()));
     }
     let k=key(f.venue,&base);let bk=(s.clone(),k.clone());
     let previous_received_ns=self.books.get(&bk).map(|b|b.frame.received_ns);
     let validation_end_ns=self.r1247_trace_enabled.then(crate::ingress_runtime_r1205::mono_ns);
     if let Some(timing)=f.source.as_mut().and_then(|source|source.r1247_timing.as_mut()){timing.t4_validation_end_ns=validation_end_ns;}
     self.record_r1247_stage(&f,Some(&base),None,None,"rejected",Some("okx_delta_before_recovery_snapshot"),wall,t2_parse_start_ns,t2_parse_end_ns,validation_end_ns,previous_received_ns);
     self.finish(&f,"okx_delta_waiting_for_fresh_snapshot")?;
     self.record_invalid(&f,"okx_delta_before_recovery_snapshot",true);
     scopes.insert(k);self.r1273_record_elapsed("recovery_resync_gate",recovery_gate_started);continue
    }
   }
   self.r1273_record_elapsed("recovery_resync_gate",recovery_gate_started);
   let okx_previous_state=if channel.as_deref()==Some("books"){
    decoded_frame.as_ref()
     .and_then(|v|v["arg"]["instId"].as_str().and_then(|inst|inst.strip_suffix("-USDT"))
      .and_then(|base|forwarder_prepared.as_ref().and_then(|p|p.previous_l2)
       .or_else(||self.market_data_processor.diagnostic_state(&s,base))))
   }else{None};
   t2_parse_start_ns=self.r1247_trace_enabled.then(crate::ingress_runtime_r1205::mono_ns);
   if let Some(timing)=f.source.as_mut().and_then(|source|source.r1247_timing.as_mut()){timing.t2_parse_start_ns=t2_parse_start_ns;}
   let book_start=crate::ingress_runtime_r1205::mono_ns();if let Some(t)=f.source.as_mut().and_then(|s|s.r1279_timing.as_mut()){t.book_start_ns=Some(book_start);}
   let decode_started=self.r1273_profile_enabled.then(std::time::Instant::now);
   let parsed=if let Some(prepared)=forwarder_prepared.as_mut(){
    if channel.as_deref()==Some("books") {
     let chain_issue=prepared.chain.as_ref().map(|chain|{
      let expected=self.prepared_chain_accepted.get(&(s.clone(),chain.base.clone())).copied();
      !chain.snapshot&&chain.predecessor!=expected
     }).unwrap_or(false);
     if chain_issue {
      Err("sequence gap: Forwarder prepared L2 parent was not admitted by Engine".to_string())
     } else if prepared.parsed.as_ref().is_some_and(Result::is_ok)&&prepared.chain.is_none(){
      Err("sequence gap: missing prepared L2 proof".to_string())
     } else {prepared.parsed.take().unwrap_or_else(||Err("missing forwarded result".into()))}
    }else{prepared.parsed.take().unwrap_or_else(||Err("missing forwarded result".into()))}
   }else{self.market_data_processor.prepare_update(&s,&f,decoded_frame.as_ref(),channel.as_deref()==Some("books"))};
   self.r1273_record_elapsed(if channel.as_deref()==Some("books"){"l2_reconstruction"}else{"standard_book_decode"},decode_started);
   let book_end=crate::ingress_runtime_r1205::mono_ns();if let Some(t)=f.source.as_mut().and_then(|s|s.r1279_timing.as_mut()){t.book_end_ns=Some(book_end);}
   t2_parse_end_ns=self.r1247_trace_enabled.then(crate::ingress_runtime_r1205::mono_ns);
   if let Some(timing)=f.source.as_mut().and_then(|source|source.r1247_timing.as_mut()){timing.t2_parse_end_ns=t2_parse_end_ns;}
   let u=match parsed{Ok(v)=>v,Err(reason)=>{
    let mut quarantine_reason="invalid_schema_book".to_string();
    if f.venue==Venue::OkxSpot&&channel.as_deref()==Some("books"){
     quarantine_reason=format!("okx_books:{reason}");
     if let Some(base)=decoded_frame.as_ref().and_then(|v|v["arg"]["instId"].as_str().and_then(|x|x.strip_suffix("-USDT")).map(str::to_string)){
      let scope=key(f.venue,&base);if let Some(b)=self.remove_book(&(s.clone(),scope.clone())){if let Some(w)=b.snapshot_witness{w.revoke();}}self.authority.write().map_err(|_|"authority poison")?.remove(&scope);scopes.insert(scope);
      let identity=(s.clone(),base.clone());
      if self.resyncing.insert(identity.clone()){
       self.resubscribe_last_ns.insert(identity,ns);
       resync_requests.push((s.clone(),base,reason.clone()));
      }
     }
    }
    if f.venue==Venue::OkxSpot&&channel.as_deref()==Some("books"){if let Some(base)=decoded_frame.as_ref().and_then(|v|v["arg"]["instId"].as_str().and_then(|x|x.strip_suffix("-USDT")).map(str::to_string)){let manager_id=self.ensure_manager_source_decoded(&f,&base,decoded_frame.as_ref())?;self.manager.book_reset(&manager_id,reason.to_ascii_lowercase().contains("sequence")||reason.to_ascii_lowercase().contains("gap"));}else{self.sources.insert(s.clone(),State::Quarantined);self.quarantine_session(&s);}}else{self.sources.insert(s.clone(),State::Quarantined);self.quarantine_session(&s);}
    let validation_end_ns=self.r1247_trace_enabled.then(crate::ingress_runtime_r1205::mono_ns);
    if let Some(timing)=f.source.as_mut().and_then(|source|source.r1247_timing.as_mut()){timing.t4_validation_end_ns=validation_end_ns;}
    self.record_r1247_stage(&f,None,None,None,"rejected",Some(&reason),wall,t2_parse_start_ns,t2_parse_end_ns,validation_end_ns,None);
    self.finish(&f,&quarantine_reason)?;
    if channel.as_deref()==Some("books"){self.record_okx_books_invalid(&f,&reason,okx_previous_state);}
    else{self.record_invalid(&f,"json_or_book_parse",true);}
    continue
   }};
   self.trace(serde_json::json!({"kind":"r1304_pipeline","stage":"Reconstruction","trace_id":frame_trace_id_r1304(&f),"sequence":u.book.sequence,"mode":if channel.as_deref()==Some("books"){"incremental_l2"}else{"full_book_decode"}}));
   self.r1310_stage(&f,"Reconstruction",book_start,book_end);
   let k=key(f.venue,&u.base);
   let validator_start_ns=f.source.as_ref().and_then(|s|s.r1279_timing.as_ref()).filter(|t|t.r1310_trace_sampled).map(|_|crate::ingress_runtime_r1205::mono_ns());
   if let (Some(start),Some(t))=(validator_start_ns,f.source.as_mut().and_then(|s|s.r1279_timing.as_mut())){t.validator_start_ns=Some(start);t.exchange_event_ts_ms=u.book.exchange_ts_ms;}
   if f.venue==Venue::OkxSpot&&channel.as_deref()==Some("books")&&decoded_frame.as_ref().is_some_and(|v|v["action"]=="snapshot") {
    let identity=(s.clone(),u.base.clone());let was_resyncing=self.resyncing.contains(&identity);
    if was_resyncing{self.r1310_lifecycle(&f,"resnapshot",None);}else{self.r1310_lifecycle(&f,"snapshot",None);}
    self.resyncing.remove(&identity);self.resubscribe_last_ns.remove(&identity);
   }
   let supported=match f.venue{
    Venue::BinanceFutures=>u.book.sequence.is_some_and(|n|n>0)&&decoded_frame.as_ref().and_then(|v|v["stream"].as_str()).is_some_and(|name|name==format!("{}usdt@depth5@500ms",u.base.to_ascii_lowercase())||name==format!("{}usdt@depth20@500ms",u.base.to_ascii_lowercase())||name==format!("{}usdt@depth20@100ms",u.base.to_ascii_lowercase())),
    Venue::OkxSpot=>decoded_frame.as_ref().is_some_and(|v|{let ch=v["arg"]["channel"].as_str();let action=v.get("action").and_then(serde_json::Value::as_str);(ch==Some("books")&&matches!(action,Some("snapshot"|"update"))||ch==Some("books5")&&action.map_or(true,|a|a=="snapshot"))&&v["data"].as_array().is_some_and(|rows|rows.len()==1)&&u.book.sequence.is_some_and(|n|n>=0)}),
   };
   if !supported||!self.universe.contains(&k){let previous=self.books.get(&(s.clone(),k.clone())).map(|b|b.frame.received_ns);let validation=self.r1247_trace_enabled.then(crate::ingress_runtime_r1205::mono_ns);if let Some(timing)=f.source.as_mut().and_then(|source|source.r1247_timing.as_mut()){timing.t4_validation_end_ns=validation;}self.record_r1247_stage(&f,Some(&u.base),u.book.exchange_ts_ms,u.book.sequence,"rejected",Some("unsupported_channel_or_instrument"),wall,t2_parse_start_ns,t2_parse_end_ns,validation,previous);self.finish(&f,"unsupported_market_stream")?;self.sources.insert(s.clone(),State::Quarantined);self.quarantine_session(&s);self.record_invalid(&f,"unsupported_channel_or_instrument",true);scopes.insert(k);continue}
   let channel_keepalive=channel.as_deref()==Some("books")&&decoded_frame.as_ref().is_some_and(|v|{
    v["action"]=="update"&&v["data"].as_array().is_some_and(|rows|rows.len()==1&&rows[0]["bids"].as_array().is_some_and(|a|a.is_empty())&&rows[0]["asks"].as_array().is_some_and(|a|a.is_empty())&&rows[0]["seqId"].as_i64()==rows[0]["prevSeqId"].as_i64())
   });
   // Empty OKX updates continue through freshness, sequence and manager qualification.
   let manager_id=self.ensure_manager_source_decoded(&f,&u.base,decoded_frame.as_ref())?;
   if self.books.iter().any(|((_,scope),book)|scope==&k&&book.manager_id.channel!=manager_id.channel){let previous=self.books.get(&(s.clone(),k.clone())).map(|b|b.frame.received_ns);let validation=self.r1247_trace_enabled.then(crate::ingress_runtime_r1205::mono_ns);if let Some(timing)=f.source.as_mut().and_then(|source|source.r1247_timing.as_mut()){timing.t4_validation_end_ns=validation;}self.record_r1247_stage(&f,Some(&u.base),u.book.exchange_ts_ms,u.book.sequence,"rejected",Some("incompatible_channel_for_scope"),wall,t2_parse_start_ns,t2_parse_end_ns,validation,previous);self.manager.quarantine(&manager_id);self.sources.insert(s.clone(),State::Quarantined);self.quarantine_session(&s);self.finish(&f,"incompatible_channel_for_scope")?;self.record_invalid(&f,"incompatible_channel_for_scope",true);scopes.insert(k);continue}
   let freshness_started=self.r1273_profile_enabled.then(std::time::Instant::now);
   let fresh=u.book.exchange_ts_ms.and_then(|e|wall.checked_sub(e)).is_some_and(|a|(0..=800).contains(&a))&&ns>=f.received_ns&&ns-f.received_ns<=800_000_000;
   self.r1273_record_elapsed("freshness_gate",freshness_started);
   let bk=(s.clone(),k.clone());
   let previous_received_ns=self.books.get(&bk).map(|b|b.frame.received_ns);
   self.trace(serde_json::json!({"id":f.source.as_ref().map(|p|p.accepted_id),"source":s,"key":k,"received_ns":f.received_ns,"received_wall_ms":f.received_wall_ms,"accept_started_ns":accept_started_ns,"accept_started_wall_ms":accept_started_wall_ms,"accepted_ns":ns,"validation_wall_ms":wall,"E":u.book.exchange_ts_ms,"sequence":u.book.sequence,"fresh":fresh,"qualified_before":self.books.get(&bk).is_some_and(|b|b.qualified),"previous_received_ns":previous_received_ns}));
   if !fresh{
    let validation_end_ns=self.r1247_trace_enabled.then(crate::ingress_runtime_r1205::mono_ns);
    if let Some(timing)=f.source.as_mut().and_then(|source|source.r1247_timing.as_mut()){timing.t4_validation_end_ns=validation_end_ns;}
    self.record_r1247_stage(&f,Some(&u.base),u.book.exchange_ts_ms,u.book.sequence,"rejected",Some("stale_or_clock"),wall,t2_parse_start_ns,t2_parse_end_ns,validation_end_ns,previous_received_ns);
    // Quarantine the stale input, but retain an accepted book while that book itself remains fresh.
    self.manager.observe_with_receive_wall(&manager_id,f.received_ns,u.book.exchange_ts_ms.unwrap_or(0),f.received_wall_ms,ns,wall,false);self.finish(&f,"stale_or_clock")?;self.record_invalid(&f,"market_event_stale_or_clock_skew",true);if let Some(b)=self.books.get_mut(&bk){if !Self::fresh(b,ns,wall){b.state=State::Degraded;}}scopes.insert(k);continue
   }
   let contradictory=self.books.iter().any(|((other,scope),b)|other!=&s&&scope==&k&&
      ((f.venue==Venue::BinanceFutures&&u.book.sequence.is_some()&&u.book.sequence==b.update.book.sequence)||
       (f.venue==Venue::OkxSpot&&u.book.exchange_ts_ms==b.update.book.exchange_ts_ms&&u.book.sequence==b.update.book.sequence))&&
      (u.book.bids!=b.update.book.bids||u.book.asks!=b.update.book.asks));
   if contradictory{
      let validation_end_ns=self.r1247_trace_enabled.then(crate::ingress_runtime_r1205::mono_ns);
      if let Some(timing)=f.source.as_mut().and_then(|source|source.r1247_timing.as_mut()){timing.t4_validation_end_ns=validation_end_ns;}
      self.record_r1247_stage(&f,Some(&u.base),u.book.exchange_ts_ms,u.book.sequence,"rejected",Some("cross_connection_identity_conflict"),wall,t2_parse_start_ns,t2_parse_end_ns,validation_end_ns,previous_received_ns);
      self.finish(&f,"inconsistent_same_exchange_identity")?;self.record_invalid(&f,"cross_connection_identity_conflict",true);
      let bad_sessions=self.books.iter().filter(|((_,scope),_)|scope==&k).map(|((session,_),_)|session.clone()).chain(std::iter::once(s.clone())).collect::<HashSet<_>>();for session in bad_sessions{self.sources.insert(session.clone(),State::Quarantined);self.quarantine_session(&session);}
      for ((_,scope),b) in self.books.iter_mut(){if scope==&k{b.good=0;b.qualified=false;b.state=State::Quarantined;}}
      self.authority.write().map_err(|_|"authority poison")?.remove(&k);scopes.insert(k);continue
   }
   let mut good=1;let mut first=f.received_ns;let mut qualified=false;
   if let Some(old)=self.books.get(&bk){
    let sequence_started=self.r1273_profile_enabled.then(std::time::Instant::now);
    let seq_ok=if f.venue==Venue::OkxSpot{
     let is_l2=decoded_frame.as_ref().is_some_and(|v|v["arg"]["channel"]=="books");
     if is_l2{let is_snapshot=decoded_frame.as_ref().is_some_and(|v|v["action"]=="snapshot");if is_snapshot{true}else if channel_keepalive{match(u.book.sequence,old.update.book.sequence){(Some(n),Some(p))=>n==p&&u.book.exchange_ts_ms.is_some_and(|event|old.update.book.exchange_ts_ms.is_some_and(|prior|event>=prior)),_=>false}}else{match(u.book.sequence,old.update.book.sequence){(Some(n),Some(p))=>n!=p&&u.book.exchange_ts_ms>=old.update.book.exchange_ts_ms,_=>false}}}
     else{match(u.book.sequence,old.update.book.sequence){(Some(n),Some(p))=>(n>p&&u.book.exchange_ts_ms>=old.update.book.exchange_ts_ms)||crate::r1121_okx_newer_books5_snapshot(&f,&u,&old.update.book,f.source.is_some()),_=>false}}
    }else{match(u.book.sequence,old.update.book.sequence){(Some(n),Some(p))=>crate::quality_manager_r1195::futures_partial_advances(p,n,old.update.book.exchange_ts_ms.unwrap_or(0),u.book.exchange_ts_ms.unwrap_or(0)),_=>false}};
    self.r1273_record_elapsed("sequence_gate",sequence_started);
    if !seq_ok||u.book.exchange_ts_ms<old.update.book.exchange_ts_ms{
     if let Some(w)=&old.snapshot_witness{w.revoke();}
     if self.manager.remote().is_some(){
      // Reordered/duplicate rejection revokes the witness without an
      // owner Observe. Preserve that destructive fact across later
      // valid full snapshots. A pending switch cannot borrow recovery.
      let rev=self.ucm_witness_revocations.entry(k.clone()).or_insert(0);
      *rev=rev.checked_add(1).ok_or("Engine witness revocation overflow")?;
      self.ucm_stable_choices.remove(&k);
      self.authority_write_timed(|authority|authority.remove(&k))?;
      scopes.insert(k.clone());
     }
     let validation_end_ns=self.r1247_trace_enabled.then(crate::ingress_runtime_r1205::mono_ns);
     if let Some(timing)=f.source.as_mut().and_then(|source|source.r1247_timing.as_mut()){timing.t4_validation_end_ns=validation_end_ns;}
     self.record_r1247_stage(&f,Some(&u.base),u.book.exchange_ts_ms,u.book.sequence,"rejected",Some("sequence_duplicate_or_reordered"),wall,t2_parse_start_ns,t2_parse_end_ns,validation_end_ns,previous_received_ns);
     self.finish(&f,"duplicate_or_reordered")?;self.metrics.duplicates+=1;self.record_invalid(&f,"sequence_duplicate_or_reordered_safely_dropped",false);continue
    }
    if channel_keepalive{
     self.metrics.channel_keepalives=self.metrics.channel_keepalives.saturating_add(1);
     self.manager.observe_transport_liveness(&manager_id,f.received_ns,f.received_wall_ms,ns)?;
     let stale_retained=old.retained&&!Self::fresh(old,ns,wall);
     if stale_retained{
      self.finish(&old.frame,"okx_stale_book_not_published_after_keepalive")?;
     }
     self.trace(serde_json::json!({"event":"okx_books_keepalive_transport_only","source":s,"key":k,
      "received_ns":f.received_ns,"received_wall_ms":f.received_wall_ms,
      "book_received_ns_retained":old.frame.received_ns,
      "book_exchange_ts_ms_retained":old.update.book.exchange_ts_ms,
      "book_sequence_retained":old.update.book.sequence}));
     if stale_retained{
      if let Some(book)=self.books.get_mut(&bk){book.retained=false;book.state=State::Degraded;}
     }
     if let Some(chain)=forwarder_prepared.as_ref().and_then(|p|p.chain.as_ref()){
      self.prepared_chain_accepted.insert((s.clone(),chain.base.clone()),chain.token);
     }
     let validation_end_ns=self.r1247_trace_enabled.then(crate::ingress_runtime_r1205::mono_ns);if let Some(timing)=f.source.as_mut().and_then(|source|source.r1247_timing.as_mut()){timing.t4_validation_end_ns=validation_end_ns;}self.record_r1247_stage(&f,Some(&u.base),u.book.exchange_ts_ms,u.book.sequence,"transport_keepalive",Some("okx_empty_books_transport_liveness"),wall,t2_parse_start_ns,t2_parse_end_ns,validation_end_ns,previous_received_ns);self.finish(&f,"okx_empty_books_keepalive_not_market_update")?;
     scopes.insert(k);
     continue;
    }
    // A valid self-contained snapshot needs no diff reconstruction after
    // silence. Retain the established qualification of this exact session;
    // current exchange/receive ages remain mandatory at every publication.
    qualified=old.qualified;
    if old.good>0&&f.received_ns>=old.frame.received_ns&&(qualified||f.received_ns-old.frame.received_ns<=800_000_000){good=old.good.saturating_add(1);first=old.first_ns;}
    if old.retained{self.finish(&old.frame,"standby_superseded")?;}
   }
   let ordered_observation=(manager_id.clone(),f.received_ns,u.book.exchange_ts_ms.unwrap_or(0),f.received_wall_ms);
   if self.manager.remote().is_none(){self.manager.observe_with_receive_wall(&manager_id,f.received_ns,u.book.exchange_ts_ms.unwrap_or(0),f.received_wall_ms,ns,wall,true);}
   let validation_end_ns=self.r1247_trace_enabled.then(crate::ingress_runtime_r1205::mono_ns);
   let r1279_validation_end=crate::ingress_runtime_r1205::mono_ns();
   if let Some(source)=f.source.as_mut(){if let Some(timing)=source.r1247_timing.as_mut(){timing.t4_validation_end_ns=validation_end_ns;}if let Some(timing)=source.r1279_timing.as_mut(){timing.validation_end_ns=Some(r1279_validation_end);}}
   self.record_r1247_stage(&f,Some(&u.base),u.book.exchange_ts_ms,u.book.sequence,"accepted_for_selection",None,wall,t2_parse_start_ns,t2_parse_end_ns,validation_end_ns,previous_received_ns);
   // Only the actual decoder/reconstructor and sequence gates above can mint this.
   let full_snapshot=f.venue==Venue::BinanceFutures || channel.as_deref()!=Some("books")
    || decoded_frame.as_ref().is_some_and(|v|v["action"]=="snapshot");
   let previous_witness=self.books.get(&bk).and_then(|b|b.snapshot_witness.clone());
   let witness_start_ns=validator_start_ns.map(|_|crate::ingress_runtime_r1205::mono_ns());
   let snapshot_witness=if full_snapshot{
    let continuity=previous_witness.as_ref().and_then(|w|{
     let ordinary=f.venue==Venue::BinanceFutures&&self.books.get(&bk).is_some_and(|old|
      Self::fresh(old,ns,wall)&&!matches!(old.state,State::Degraded|State::Quarantined|State::Reconnecting))
      &&f.source.as_ref().is_some_and(|p|w.matches(f.venue,&u.base,&p.session_id,p.connection_epoch));
     if ordinary{w.supersede()}else{w.revoke();None}
    });
    Some(SnapshotWitness::mint(&f,&u.base,continuity)?)
   }else{previous_witness.filter(|w|f.source.as_ref().is_some_and(|p|w.matches(f.venue,&u.base,&p.session_id,p.connection_epoch)))};
   let witness_end_ns=witness_start_ns.map(|_|crate::ingress_runtime_r1205::mono_ns());
   if let (Some(start),Some(end))=(validator_start_ns,witness_start_ns){if let Some(t)=f.source.as_mut().and_then(|s|s.r1279_timing.as_mut()){t.validator_end_ns=Some(end);}self.r1310_stage(&f,"Validator",start,end);}
   if let (Some(start),Some(end))=(witness_start_ns,witness_end_ns){self.r1310_stage(&f,"Witness",start,end);}
   self.trace(serde_json::json!({"kind":"r1304_pipeline","stage":"Validator","trace_id":frame_trace_id_r1304(&f),"sequence":u.book.sequence}));
   self.trace(serde_json::json!({"kind":"r1304_pipeline","stage":"Witness","trace_id":frame_trace_id_r1304(&f),"generation":snapshot_witness.as_ref().map(|w|w.generation())}));
   let accepted_chain=forwarder_prepared.as_ref().and_then(|p|p.chain.as_ref()).map(|chain|(chain.base.clone(),chain.token));
   let replacement=SourceBook{output_is_books:decoded_frame.as_ref().is_some_and(|v|v["arg"]["channel"]=="books"),snapshot_witness,frame:f,update:u,manager_id,good,first_ns:first,state:State::Syncing,retained:true,qualified};
   if let Some(existing)=self.books.get_mut(&bk){*existing=replacement;}
   else{self.books.insert(bk,replacement);self.ucm_book_order=None;}self.book_scope_sessions.entry(k.clone()).or_default().insert(s.clone());
   if let Some((base,token))=accepted_chain{self.prepared_chain_accepted.insert((s.clone(),base),token);}
   self.sources.insert(s,State::Syncing);
   if self.manager.remote().is_some(){
    // Revoke exactly as on the previous selection path before any owner
    // advice can be applied. The new observation is submitted once, after
    // the actual validated book and witness are installed.
    self.authority_write_timed(|authority|authority.remove(&k))?;
    self.ucm_stable_choices.remove(&k);
    let (id,recv,event,recv_wall)=ordered_observation;
    let facts=self.ucm_book_facts(&k,id.clone(),ns,wall);
    self.manager.observe_book_facts(&id,recv,event,recv_wall,ns,wall,facts)?;
   }
   scopes.insert(k);
  }
  self.manager.check()?;
  let frame_loop_ns=self.r1273_record_elapsed("frame_processing_total",frame_loop_started).unwrap_or(0);
  let mut out=resync_requests.into_iter().map(|(session,base,reason)|Output::Resubscribe{session,base,reason}).collect::<Vec<_>>();let mut selection_ns=0u64;for k in scopes{let selection_started=self.r1273_profile_enabled.then(std::time::Instant::now);let selected_result=self.select(&k,ns,wall,receive_batch_id,"receive_batch");selection_ns=selection_ns.saturating_add(self.r1273_record_elapsed("selection_recovery",selection_started).unwrap_or(0));let selected=selected_result?;out.extend(selected);self.publish_scope_diagnostic_r1352(&k,ns,wall);}
  let quarantine_started=self.r1273_profile_enabled.then(std::time::Instant::now);self.flush_quarantines()?;let quarantine_ns=self.r1273_record_elapsed("quarantine_durable_ack",quarantine_started).unwrap_or(0);
  let receive_end_ns=crate::ingress_runtime_r1205::mono_ns();for output in &mut out{let terminalization_reason=match output{Output::Frame(_)=>"selected_frame",Output::Book(_,_)=>"selected_book",_=>"other"};let frame=match output{Output::Frame(f)|Output::Book(f,_)=>Some(f),_=>None};if let Some(frame)=frame{if let Some(t)=frame.source.as_mut().and_then(|s|s.r1279_timing.as_mut()){if t.receive_batch_id==Some(receive_batch_id){t.engine_receive_end_ns=Some(receive_end_ns);t.receive_batch_end_ns=Some(receive_end_ns);}t.terminalization_reason=Some(terminalization_reason.into());let worker=t.source_epoch_worker_queue_ns.unwrap_or(0).saturating_add(t.source_epoch_store_lock_wait_ns.unwrap_or(0)).saturating_add(t.source_epoch_store_operation_ns.unwrap_or(0)).saturating_add(t.source_epoch_other_wait_ns.unwrap_or(0));let authority=t.source_selection_authority_wait_ns.unwrap_or(0).saturating_add(t.source_selection_authority_hold_ns.unwrap_or(0));t.source_selection_wall_residual_ns=t.source_selection_start_ns.zip(t.source_selection_end_ns).map(|(a,b)|b.saturating_sub(a).saturating_sub(worker).saturating_sub(authority));}}}
  if self.r1353_shards.is_some(){for output in &out{match output{Output::Frame(f)|Output::Book(f,_)=>self.publish_frame_sensor_r1353(f),_=>{}}}}
  let total_ns=self.r1273_record_elapsed("receive_batch_total",profile_total).unwrap_or(0);if self.r1273_profile_enabled{let other=total_ns.saturating_sub(custody_ns).saturating_sub(frame_loop_ns).saturating_sub(selection_ns).saturating_sub(quarantine_ns);if let Ok(mut p)=self.r1273_profile.lock(){p.record("other_operations",other);}}Ok(out)
 }
 fn record_r1247_selection_evaluation(&mut self,k:&str,ns:u64,wall:i64,ready:&[String],candidates:&[SourceId],prior:Option<&Selected>,initial_wait:bool,choice:Option<(&SourceId,&str)>){
  if !self.r1247_trace_enabled{return}
  if self.r1247_selection_eval_last.get(k).is_some_and(|last|ns.saturating_sub(*last)<2_000_000_000){return}
  self.r1247_selection_eval_last.insert(k.into(),ns);
  let highwater=self.highwater.get(k).copied();
  let prior_session=prior.map(|p|p.session.as_str());
  let prior_unready=prior.is_some_and(|p|!self.books.get(&(p.session.clone(),k.to_string())).is_some_and(|b|self.ready(b,ns,wall)));
  let books=self.books.iter().filter(|((_,scope),_)|scope==k).map(|((session,_),book)|{
   let source=book.frame.source.as_ref();
   let seq=book.update.book.sequence.unwrap_or(0);let event=book.update.book.exchange_ts_ms.unwrap_or(0);
   let selected=prior_session==Some(session.as_str());
   let equivalent=prior_unready&&prior_session.is_some_and(|p|p!=session.as_str())&&self.books.get(&(prior.unwrap().session.clone(),k.to_string())).is_some_and(|old|same_published_book(old,book));
   let advances=advances_highwater(book.frame.venue,seq,event,highwater);
   let eligible=candidates.contains(&book.manager_id);
   let quality=self.manager.quality(&book.manager_id);
   let health=self.manager.state(&book.manager_id);
   let book_fresh=Self::fresh(book,ns,wall);let book_ready=self.ready(book,ns,wall);
   let blocked_reason=if !book_fresh{Some("book_stale_or_receive_age_over_800ms")}else if !book_ready{Some("book_or_manager_not_hot_ready")}else if !eligible{Some("highwater_or_failover_eligibility_rejected")}else if quality.is_none(){Some("manager_quality_window_unavailable")}else{None};
   serde_json::json!({
    "session":session,"connection_epoch":source.map(|s|s.connection_epoch),
    "accepted_id":source.map(|s|s.accepted_id),
    "trace_id":source.map(|s|format!("{}:{}:{}",s.session_id,s.connection_epoch,s.accepted_id)),
    "received_ns":book.frame.received_ns,"event_ts_ms":book.update.book.exchange_ts_ms,
    "event_age_ms":book.update.book.exchange_ts_ms.and_then(|e|wall.checked_sub(e)),
    "sequence":book.update.book.sequence,"selection_epoch":book.frame.epoch,
    "engine_book_state":format!("{:?}",book.state),"engine_ready":book_ready,
    "book_fresh":book_fresh,"selected_before":selected,"equivalent_same_book_failover":equivalent,
    "highwater":highwater,"advances_highwater":advances,"candidate_eligible":eligible,
    "candidate_blocked_reason":blocked_reason,
    "manager_active":self.manager.active_for_scope(&book.manager_id)==Some(&book.manager_id),
    "manager_health":health.map(|h|serde_json::json!({
      "state":h.state,"tcp":h.tcp,"tls":h.tls,"websocket":h.websocket,"subscribed":h.subscribed,
      "hot_ready":h.hot_ready,"good_samples":h.good_samples,"qualified_once":h.qualified_once,
      "last_receive_ns":h.last_receive_ns,"last_exchange_ms":h.last_exchange_ms,
      "errors":h.errors,"stale":h.stale,"reconnects":h.reconnects
    })),
    "quality":quality.map(|(p95,event_p99,samples)|serde_json::json!({
      "processing_delay_p95_ns":p95,"event_age_p99_ns":event_p99,"samples":samples,
      "rank_score_ns":p95.saturating_add(event_p99)
    }))
   })
  }).collect::<Vec<_>>();
  let row=serde_json::json!({
   "mono_ns":ns,"scope":k,"freshness_limit_ms":800,
   "selected_before":prior_session,
   "initial_wait_for_two_hot":initial_wait,
   "hot_ready_sessions":ready,
   "candidate_eligible_count":candidates.len(),
   "manager_choice_source":choice.map(|(id,_)|id.logical_session.as_str()),
   "manager_choice_reason":choice.map(|(_,reason)|reason),
   "manager_effective_reason":if initial_wait{"WAITING_FOR_TWO_HOT"}else{choice.map(|(_,reason)|reason).unwrap_or("no_fresh_quality_candidate")},
   "candidate_sources":books
  });
  if self.r1247_selection_evaluations.len()>=3072{self.r1247_selection_evaluations.pop_front();self.r1247_selection_eval_evictions=self.r1247_selection_eval_evictions.saturating_add(1);}
  self.r1247_selection_evaluations.push_back(row);
 }
 fn mark_scope_hold_reason(&mut self,k:&str,reason:&str){for((_,scope),book)in self.books.iter_mut(){if scope==k{if let Some(t)=book.frame.source.as_mut().and_then(|s|s.r1279_timing.as_mut()){t.frame_hold_reason=Some(reason.into());}}}}
 fn select(&mut self,k:&str,ns:u64,wall:i64,parent_call_id:u64,parent_call_kind:&str)->Result<Vec<Output>,String>{
  if self.manager.remote().is_some()&&self.ucm_dirty.contains(k){
   let start=self.r1273_profile_enabled.then(std::time::Instant::now);
   // Already scheduled: retain immediate Permit revocation, all counters,
   // wakeup and revision fences, but do not fabricate a new selection span.
   let result=self.select_inner(k,ns,wall);
   self.r1273_record_elapsed(if parent_call_kind=="tick"{"r1402h_coalesced_tick_schedule"}else{"r1402h_coalesced_batch_schedule"},start);
   return result
  }
  let call_id=next_engine_call_id();let start=crate::ingress_runtime_r1205::mono_ns();let book_keys=self.book_scope_sessions.get(k).map(|sessions|sessions.iter().map(|session|(session.clone(),k.to_owned())).collect::<Vec<_>>()).unwrap_or_default();let mut ingress_batch_ids=book_keys.iter().filter_map(|key|self.books.get(key).and_then(|book|book.frame.source.as_ref()).and_then(|s|s.r1279_timing.as_ref()).and_then(|t|t.receive_batch_id)).collect::<Vec<_>>();ingress_batch_ids.sort_unstable();ingress_batch_ids.dedup();for book_key in &book_keys{if let Some(book)=self.books.get_mut(book_key){if let Some(t)=book.frame.source.as_mut().and_then(|s|s.r1279_timing.as_mut()){t.source_selection_start_ns=Some(start);t.selection_batch_id=Some(call_id);}}}let e0_inner_started=self.r1273_profile_enabled.then(std::time::Instant::now);let mut result=self.select_inner(k,ns,wall);self.r1273_record_elapsed(if parent_call_kind=="tick"{"e0_select_inner_tick"}else{"e0_select_inner_batch"},e0_inner_started);let end=crate::ingress_runtime_r1205::mono_ns();for book_key in &book_keys{if let Some(book)=self.books.get_mut(book_key){if let Some(t)=book.frame.source.as_mut().and_then(|s|s.r1279_timing.as_mut()){if t.selection_batch_id==Some(call_id){t.source_selection_end_ns=Some(end);}if !book.retained&&t.frame_residence_end_ns.is_none(){t.frame_residence_end_ns=Some(end);t.frame_residence_status=Some("released".into());}}}}if let Ok(mut quarantines)=self.quarantines.lock(){for(frame,_)in quarantines.iter_mut(){if let Some(t)=frame.source.as_mut().and_then(|s|s.r1279_timing.as_mut()){if t.selection_batch_id==Some(call_id){t.source_selection_end_ns=Some(end);}}}}if let Ok(outputs)=result.as_mut(){for output in outputs{let frame=match output{Output::Frame(f)|Output::Book(f,_)=>Some(f),_=>None};if let Some(frame)=frame{if let Some(t)=frame.source.as_mut().and_then(|s|s.r1279_timing.as_mut()){if t.selection_batch_id==Some(call_id){t.source_selection_end_ns=Some(end);}if t.frame_residence_end_ns.is_none(){t.frame_residence_end_ns=Some(end);t.frame_residence_status=Some("released".into());}}}}}self.trace(serde_json::json!({"kind":"engine_call_span","call":"select","call_id":call_id,"scope":k,"ingress_batch_ids":ingress_batch_ids,"selection_batch_id":call_id,"parent_call_kind":parent_call_kind,"parent_call_id":parent_call_id,"start_ns":start,"end_ns":end,"duration_ns":end.saturating_sub(start),"status":"complete","outcome":if result.is_ok(){"ok"}else{"error"}}));if self.r1273_profile_enabled{self.r1273_record_ns(if parent_call_kind=="tick"{"e0_select_call_tick"}else{"e0_select_call_batch"},crate::ingress_runtime_r1205::mono_ns().saturating_sub(start));}result}
 // Transfer the one existing manager BEFORE accepting any source. The
 // existing Engine and its final book/authority gates remain the same.
 pub(crate) fn enable_ucm_worker(&mut self,capacity:usize)->Result<Arc<tokio::sync::Notify>,String>{
  if !self.sources.is_empty()||!self.books.is_empty()||!self.manager_ids.is_empty()||!self.selected.is_empty(){
   return Err("UCM cutover requires an empty startup Engine".into())
  }
  let notify=Arc::new(tokio::sync::Notify::new());
  self.manager.activate(capacity,notify.clone())?;
  self.ucm_notify=Some(notify.clone());
  if std::env::var("R1402_UCM_PROFILE").ok().as_deref()==Some("1"){
   self.ucm_scope_profile=Some(self.universe.iter().map(|k|(k.clone(),UcmScopeProfile::default())).collect());
  }
  Ok(notify)
 }
 fn wake_ucm(&self){if let Some(n)=&self.ucm_notify{n.notify_one();}}
 fn mark_ucm_dirty(&mut self,k:String,retry:bool){
  let inserted=self.ucm_dirty.insert(k.clone());
  if !self.ucm_pending.contains_key(&k){self.ucm_startable.insert(k.clone());}
  if let Some(p)=self.ucm_scope_profile.as_mut().and_then(|rows|rows.get_mut(&k)){
   p.requests+=1;if retry{p.retries+=1;}if !inserted{p.coalesced+=1;}
   if inserted{p.dirty_since=Some(crate::ingress_runtime_r1205::mono_ns());}
  }
  self.wake_ucm();
 }
 fn retry_ucm_scope(&mut self,k:String){self.ucm_fact_refresh.insert(k.clone());self.mark_ucm_dirty(k,true);}
 fn record_ucm_cycle(&mut self,k:&str,start:Option<u64>,completed:bool,out:&[Output]){
  if let (Some(start),Some(p))=(start,self.ucm_scope_profile.as_mut().and_then(|rows|rows.get_mut(k))){
   let end=crate::ingress_runtime_r1205::mono_ns();
   if completed{p.finished+=1;p.completed_cycle.add(end.saturating_sub(start));}
   else{p.abandoned+=1;p.abandoned_cycle.add(end.saturating_sub(start));}
   for o in out{if let Output::Frame(f)|Output::Book(f,_)=o{p.frames+=1;p.receive_to_publication.add(end.saturating_sub(f.received_ns));}}
  }
 }
 fn ucm_ordered_sessions(&mut self,k:&str)->Vec<String>{
  if self.ucm_book_order.is_none(){
   let started=self.r1273_profile_enabled.then(std::time::Instant::now);
   let mut order:HashMap<String,Vec<String>>=HashMap::new();
   for ((session,scope),_) in &self.books{order.entry(scope.clone()).or_default().push(session.clone());}
   self.ucm_book_order=Some(order);
   self.r1273_record_elapsed("r1402h_book_order_rebuild",started);
  }
  self.ucm_book_order.as_ref().and_then(|order|order.get(k)).cloned().unwrap_or_default()
 }
 fn ucm_anchor(&mut self,k:&str)->Option<SourceId>{
  self.ucm_ordered_sessions(k).first().and_then(|session|self.books.get(&(session.clone(),k.to_string()))).map(|b|b.manager_id.clone())
   .or_else(||self.manager_ids.iter().find(|((_,scope),_)|scope==k).map(|(_,id)|id.clone()))
   .or_else(||self.manager.remote()?.known.iter().find(|id|ucm_engine_scope(id)==k).cloned())
 }
 #[cfg(test)]
 fn begin_ucm_snapshot(&mut self,id:&SourceId)->Result<UcmPendingSnapshot,String>{
  let r=self.manager.remote_mut().ok_or("UCM worker absent")?;
  Ok(r.worker.begin_scoped_snapshot(r.worker.revision(),id)?)
 }
 fn install_ucm_view(&mut self,id:&SourceId,view:UcmWorkerSnapshot)->Result<(),String>{
  let r=self.manager.remote_mut().ok_or("UCM worker absent")?;
  if !r.worker.view_current(id,view.revision){return Err("UCM snapshot no longer current".into())}
  r.views.insert(ucm_read_scope(id),view);Ok(())
 }
 fn ucm_candidate_allowed(&self,k:&str,inputs:&HotSelectionInputs,candidate:&SourceId,ns:u64,wall:i64)->bool{
  if inputs.initial_wait||!inputs.candidates.contains(candidate){return false}
  let Some(book)=self.books.get(&(candidate.logical_session.clone(),k.to_string()))else{return false};
  book.manager_id==*candidate && self.ready(book,ns,wall)
   && inputs.ready.iter().any(|session|self.books.get(&(session.clone(),k.to_string())).is_some_and(|other|
    other.manager_id.physical_connection!=candidate.physical_connection
      &&ucm_read_scope(&other.manager_id)==ucm_read_scope(candidate)&&self.ready(other,ns,wall)))
 }
 pub(crate) fn ucm_quality_observable(&self,session:&str,venue:Venue,bases:&[String])->bool{
  self.manager.remote().is_none()||bases.iter().all(|base|
   self.manager_ids.get(&(session.to_string(),key(venue,base))).is_some_and(|id|self.manager.view(id).is_some()))
 }
 pub(crate) fn check_ucm_owner(&mut self)->Result<(),String>{self.manager.check()}
 pub(crate) fn request_ucm_census(&mut self)->Result<(),String>{
  if let Some(r)=self.manager.remote_mut(){
   if r.retirements.len()>=r.capacity{return Err(self.manager.fail("UCM pending history controls full"))}
   let revision=r.worker.revision().checked_add(1).ok_or("UCM revision overflow")?;
   let pending=r.worker.begin_history(revision,UcmHistoryEvent::Census)?;
   r.retirements.push_back(pending);
  }
  Ok(())
 }
 // One nonblocking continuation per wake. Dirty scope membership is bounded
 // by the existing universe, not a queue of market frames. All actual UCM
 // mutation commands still pass through the original reliable FIFO.
 pub(crate) fn poll_ucm_worker(&mut self,ns:u64,wall:i64)->Result<Vec<Output>,String>{
  let started=self.r1273_profile_enabled.then(std::time::Instant::now);
  let result=self.poll_ucm_worker_inner(ns,wall);
  self.r1273_record_elapsed("r1402_ucm_continuation",started);
  match result{
   Ok(out)=>{self.manager.check()?;if self.ucm_has_ready(){self.wake_ucm();}Ok(out)},
   Err(error)=>Err(self.manager.fail(error)),
  }
 }
 // All indexes are membership of existing universe scopes, never market
 // frame queues. Exactly one continuation and two index entries per scope.
 fn park_ucm_scope(&mut self,p:PendingHotSelection)->Result<(),String>{
  let k=p.key.clone();
  if !self.universe.contains(&k)||self.ucm_pending.contains_key(&k)
    ||!self.ucm_wait.insert((p.ticket,k.clone()))
    ||p.deadline.is_some_and(|at|!self.ucm_deadlines.insert((at,k.clone()))){
   return Err("UCM scoped continuation invariant: fail closed".into())
  }
  self.ucm_startable.remove(&k);
  self.ucm_pending.insert(k,p);Ok(())
 }
 fn take_ucm_scope(&mut self,k:&str)->Result<PendingHotSelection,String>{
  let p=self.ucm_pending.remove(k).ok_or("UCM continuation missing")?;
  if !self.ucm_wait.remove(&(p.ticket,k.to_string()))||p.deadline.is_some_and(|at|!self.ucm_deadlines.remove(&(at,k.to_string()))){
   return Err("UCM continuation index mismatch: fail closed".into())
  }
  Ok(p)
 }
 fn ucm_has_ready(&self)->bool{
  let Some(r)=self.manager.remote()else{return false};
  let done=r.worker.completed_ticket();let now=std::time::Instant::now();
  !self.ucm_startable.is_empty()
   ||self.ucm_wait.first().is_some_and(|(t,_)|*t<=done)
   ||self.ucm_deadlines.first().is_some_and(|(at,_)|*at<=now)
   ||r.retirements.front().is_some_and(|p|p.ticket<=done||p.deadline<=now)
 }
 fn poll_ucm_worker_inner(&mut self,ns:u64,wall:i64)->Result<Vec<Output>,String>{
  self.manager.check()?;
  let Some(r)=self.manager.remote()else{return Ok(Vec::new())};
  let done=r.worker.completed_ticket();
  let now=std::time::Instant::now();
  let control=r.retirements.front().map(|p|(p.ticket,p.deadline));
  let (ns,wall)=self.clock.map(|clock|clock()).unwrap_or((ns,wall));
  let ready=self.ucm_wait.first().filter(|(t,_)|*t<=done).cloned();
  // Receipt order is FIFO among history commands only. A scope ticket
  // is an applied-fact fence, not the order of diagnostic consumption.
  // Drain at most one ready receipt AND advance one scope per poll.
  let control_ready=control.is_some_and(|(t,_)|t<=done);
  let control_due=control.is_some_and(|(_,at)|at<=now);
  if control_ready||control_due{
   let mut pending=self.manager.remote_mut().unwrap().retirements.pop_front().ok_or("UCM control receipt missing")?;
   let ticket=pending.ticket;
   match self.manager.remote_mut().unwrap().worker.poll_history(&mut pending)?{
    None=>self.manager.remote_mut().unwrap().retirements.push_front(pending),
    Some(receipt)=>match receipt.result{
     UcmHistoryResult::Retired(n)=>{self.ucm_history_memo.clear();self.metrics.readiness_state_retirements=self.metrics.readiness_state_retirements.saturating_add(n as u64);},
     UcmHistoryResult::Census{entries,payload_capacity_bytes}=>self.manager.remote_mut().unwrap().census=Some((entries,payload_capacity_bytes)),
     UcmHistoryResult::Scope{previous,dual_hot}=>{
      let context=self.ucm_history_context.remove(&ticket).ok_or("UCM scope history context missing")?;
      if previous.len()!=context.entries.len(){return Err("UCM history census mismatch".into())}
      if previous.iter().any(|(_,evicted)|*evicted){self.ucm_history_memo.clear();}
      for (entry,(prior,evicted)) in context.entries.into_iter().zip(previous){
       self.render_readiness_transition(&context.key,context.observed_ns,entry.session,entry.mask,prior,evicted,entry.context);
      }
      self.render_dual_hot_transition(&context.key,&context.ready,context.observed_ns,dual_hot);
     },
    }
   }
  }
  let due=if ready.is_none(){self.ucm_deadlines.first().filter(|(at,_)|*at<=now).map(|(_,k)|k.clone())}else{None};
  let selected=ready.map(|(_,k)|k).or(due);
  // Alternate starting independent dirty scopes and progressing ready
  // continuations. Never wait synchronously on a reply or scan all pending.
  if !self.ucm_startable.is_empty()&&(self.ucm_start_next||selected.is_none()){
   self.ucm_start_next=false;
   let k=self.ucm_cursor.as_ref().and_then(|cursor|self.ucm_startable.range((std::ops::Bound::Excluded(cursor.clone()),std::ops::Bound::Unbounded)).next().cloned())
    .or_else(||self.ucm_startable.first().cloned()).ok_or("UCM startable scope missing")?;
   self.ucm_startable.remove(&k);self.ucm_dirty.remove(&k);self.ucm_cursor=Some(k.clone());
   let started_ns=self.ucm_scope_profile.as_mut().and_then(|rows|rows.get_mut(&k)).map(|p|{
    let now=crate::ingress_runtime_r1205::mono_ns();
    if let Some(since)=p.dirty_since.take(){p.dirty_wait.add(now.saturating_sub(since));}now
   });
   if let Some(anchor)=self.ucm_anchor(&k){
    let ticket=self.manager.remote().unwrap().worker.scope_ticket(&anchor);
    if let Some(p)=self.ucm_scope_profile.as_mut().and_then(|rows|rows.get_mut(&k)){p.started+=1;}
    // Await an existing ordered event, not a new snapshot request.
    self.park_ucm_scope(PendingHotSelection{key:k,anchor,stage:PendingHotStage::AwaitApplied,started_ns,ticket,deadline:None})?;
   }
   return Ok(Vec::new())
  }
  let Some(k)=selected else{return Ok(Vec::new())};
  self.ucm_start_next=true;
  let pending=self.take_ucm_scope(&k)?;
  let out=self.advance_ucm_scope(pending,ns,wall,done)?;
  if !self.ucm_pending.contains_key(&k)&&self.ucm_dirty.contains(&k){self.ucm_startable.insert(k);}
  Ok(out)
 }
 fn advance_ucm_scope(&mut self,pending:PendingHotSelection,ns:u64,wall:i64,completed_before:u64)->Result<Vec<Output>,String>{
  let PendingHotSelection{key:k,anchor,stage,started_ns,ticket,deadline}=pending;
  let prior_stage=std::mem::discriminant(&stage);
  let next=match stage{
   PendingHotStage::AwaitApplied=>{
    // A newer event of this scope may have arrived while it was parked.
    // Never read an unapplied scope or mistake scheduling delay for a
    // worker timeout. The actual command watchdog remains fail-closed.
    let required=self.manager.remote().unwrap().worker.scope_ticket(&anchor);
    if required>completed_before{
     if let Some(p)=self.ucm_scope_profile.as_mut().and_then(|rows|rows.get_mut(&k)){p.await_applied+=1;}
     self.park_ucm_scope(PendingHotSelection{key:k,anchor,stage:PendingHotStage::AwaitApplied,started_ns,ticket:required,deadline:None})?;
     return Ok(Vec::new())
    }
    let profile_start=self.r1273_profile_enabled.then(std::time::Instant::now);
    let result=self.manager.remote_mut().unwrap().worker.read_applied_scope(&anchor);
    self.r1273_record_elapsed("r1402i_published_read",profile_start);
    match result{
     Ok(None)=>{
      if let Some(p)=self.ucm_scope_profile.as_mut().and_then(|rows|rows.get_mut(&k)){p.projection_busy+=1;}
      Some(PendingHotStage::AwaitApplied)
     },
     Err(error)=>return Err(error.into()),
     Ok(Some(view))=>{
      self.install_ucm_view(&anchor,view)?;
      let inputs=self.prepare_hot_selection_inner(&k,ns,wall,false);
      self.submit_scope_history(&k,&inputs,ns,wall)?;
      let event=self.manager.view(&anchor).and_then(|v|v.event_advice.as_ref()).filter(|a|
       self.ucm_event_advice_seen.get(&k).copied().unwrap_or(0)<a.revision&&self.ucm_event_advice_matches(&k,&inputs,a)).cloned();
      let choice=if let Some(event)=event{
       self.ucm_event_advice_seen.insert(k.clone(),event.revision);
       if let Some(p)=self.ucm_scope_profile.as_mut().and_then(|rows|rows.get_mut(&k)){p.event_advice_used+=1;}
       self.ucm_stable_choices.remove(&k);
       if let Some(proof)=event.advice.stable{self.ucm_stable_choices.insert(k.clone(),proof);}
       event.advice.candidate
      }else if let Some(proof)=self.manager.view(&anchor).and_then(|v|v.event_advice.as_ref()).and_then(|a|a.advice.stable.as_ref()).filter(|proof|self.ucm_stable_choice_matches(&k,proof,&inputs.candidates,ns,wall)){
       if let Some(p)=self.ucm_scope_profile.as_mut().and_then(|rows|rows.get_mut(&k)){p.certified_proposals+=1;}
       Some((proof.candidate.clone(),proof.reason))
      }else{
       // No query/retry loop. Only a new ordered observation or timer
       // evaluation may produce the next current advisory result.
       if let Some(p)=self.ucm_scope_profile.as_mut().and_then(|rows|rows.get_mut(&k)){p.await_next_event+=1;}
       self.ucm_fact_refresh.insert(k.clone());
       None
      };
      if let Some((candidate,reason))=choice.filter(|(id,_)|self.ucm_candidate_allowed(&k,&inputs,id,ns,wall)){
       let unchanged=self.selected.get(&k).is_some_and(|p|p.session==candidate.logical_session)
        &&self.manager.active_for_scope(&candidate)==Some(&candidate);
       if unchanged{
        if let Some(p)=self.ucm_scope_profile.as_mut().and_then(|rows|rows.get_mut(&k)){p.unchanged_active+=1;}
        return self.finish_ucm_publication(&k,ns,wall,candidate,reason,None,started_ns)
       }
       let plan=Some(self.prepare_hot_switch(&k,ns,wall,inputs.prior.as_ref(),candidate.clone(),reason)?);
       let r=self.manager.remote_mut().unwrap();
       let revision=r.worker.revision().checked_add(1).ok_or("UCM revision overflow")?;
       let reply=r.worker.begin_commit(revision,candidate.clone(),ns,wall)?;
       Some(PendingHotStage::Commit{reply,candidate,reason,plan})
      }else{
       self.record_r1247_selection_evaluation(&k,ns,wall,&inputs.ready,&inputs.candidates,inputs.prior.as_ref(),inputs.initial_wait,None);
       if inputs.ready.len()<2{self.metrics.hot_deficits=self.metrics.hot_deficits.saturating_add(1);}
       None
      }
     }
    }
   },
   PendingHotStage::Commit{mut reply,candidate,reason,plan}=>{
    let result=self.manager.remote_mut().unwrap().worker.poll_applied_commit_receipt(&mut reply)?;
    match result{
     None=>Some(PendingHotStage::Commit{reply,candidate,reason,plan}),
     Some(view)=>Some(PendingHotStage::Reconcile{candidate,reason,plan:plan.ok_or("missing HOT switch plan")?,
      destructive_revision:reply.destructive_revision,hot_loss_revision:view.hot_loss_revision}),
    }
   },
   PendingHotStage::Reconcile{candidate,reason,plan,destructive_revision,hot_loss_revision}=>{
    let worker=&self.manager.remote().unwrap().worker;
    let lifecycle_changed=worker.destructive_revision(&candidate)!=destructive_revision;
    let witness_changed=self.ucm_witness_revocations.get(&k).copied().unwrap_or(0)!=plan.engine_revocation;
    if lifecycle_changed||witness_changed{
     if let Some(p)=self.ucm_scope_profile.as_mut().and_then(|rows|rows.get_mut(&k)){
      p.commit_destructive_rejected+=1;
      if lifecycle_changed{p.commit_lifecycle_rejected+=1;}else{p.commit_witness_rejected+=1;}
     }
     self.retry_ucm_scope(k.clone());None
    }else if worker.scope_ticket(&candidate)>completed_before{
     Some(PendingHotStage::Reconcile{candidate,reason,plan,destructive_revision,hot_loss_revision})
    }else{
     match self.manager.remote_mut().unwrap().worker.read_applied_scope(&candidate)?{
      None=>Some(PendingHotStage::Reconcile{candidate,reason,plan,destructive_revision,hot_loss_revision}),
      Some(view)=>{
       let hot_lost=view.hot_loss_revision!=hot_loss_revision;
       if hot_lost||view.active.get(&ucm_read_scope(&candidate))!=Some(&candidate){
        if let Some(p)=self.ucm_scope_profile.as_mut().and_then(|rows|rows.get_mut(&k)){
         p.commit_destructive_rejected+=1;
         if hot_lost{p.commit_hot_loss_rejected+=1;}else{p.commit_active_rejected+=1;}
        }
        self.retry_ucm_scope(k.clone());None
       }else{
        self.install_ucm_view(&candidate,view)?;
        if let Some(p)=self.ucm_scope_profile.as_mut().and_then(|rows|rows.get_mut(&k)){p.commit_reconciled+=1;}
        return self.finish_ucm_publication(&k,ns,wall,candidate,reason,Some(AppliedHotSwitch(plan)),started_ns)
       }
      }
     }
    }
   },
  };
  if let Some(stage)=next{
   let worker=&self.manager.remote().unwrap().worker;
   let same=std::mem::discriminant(&stage)==prior_stage;
   let ticket=match &stage{
    PendingHotStage::AwaitApplied=>worker.scope_ticket(&anchor).max(completed_before.checked_add(1).ok_or("UCM completion ticket overflow")?),
    PendingHotStage::Reconcile{candidate,..}=>{
     let required=worker.scope_ticket(candidate);
     if same{required.max(completed_before.checked_add(1).ok_or("UCM completion ticket overflow")?)}else{required}
    },
    _ if same=>ticket,
    _=>worker.submitted_ticket(),
   };
   let deadline=if matches!(&stage,PendingHotStage::AwaitApplied|PendingHotStage::Reconcile{..}){None}else if same{deadline}else{Some(std::time::Instant::now()+std::time::Duration::from_millis(50))};
   self.park_ucm_scope(PendingHotSelection{key:k,anchor,stage,started_ns,ticket,deadline})?;
  }
  else{self.record_ucm_cycle(&k,started_ns,false,&[]);}
  Ok(Vec::new())
 }
 fn ucm_stable_choice_matches(&self,k:&str,proof:&UcmStableChoice,candidates:&[SourceId],ns:u64,wall:i64)->bool{
  self.selected.get(k).is_some_and(|p|p.session==proof.candidate.logical_session)
   &&self.manager.active_for_scope(&proof.candidate)==Some(&proof.candidate)
   &&candidates.len()==proof.eligible.len()
   &&candidates.iter().zip(&proof.eligible).all(|(id,(previous,quality))|
    id==previous&&self.manager.view(id).is_some_and(|v|v.source_ready_at(id,ns,wall))
     &&self.manager.quality(id)==*quality)
 }
 // Scope diagnostics use the existing ordered control-receipt lane.
 // They are submitted once per changed input, and never gate publication
 // on their return. A lost mandatory receipt still fails closed.
 fn submit_scope_history(&mut self,k:&str,inputs:&HotSelectionInputs,ns:u64,wall:i64)->Result<(),String>{
  let masks=inputs.ordered_scope_sessions.iter().filter_map(|session|{
   let book=self.books.get(&(session.clone(),k.to_string()))?;
   Some((session.clone(),self.readiness_predicate_mask(book,ns,wall)))
  }).collect::<Vec<_>>();
  let dual_hot=inputs.ready.len()>=2;
  if self.ucm_history_memo.get(k).is_some_and(|(previous,hot)|previous==&masks&&*hot==dual_hot){
   if let Some(p)=self.ucm_scope_profile.as_mut().and_then(|rows|rows.get_mut(k)){p.skipped_history+=1;}
   return Ok(())
  }
  let entries=masks.iter().map(|(session,mask)|{
   let book=&self.books[&(session.clone(),k.to_string())];
   HistoricalReadiness{session:session.clone(),mask:*mask,context:self.readiness_current_context(book,ns,wall,*mask)}
  }).collect::<Vec<_>>();
  let r=self.manager.remote_mut().unwrap();
  if r.retirements.len()>=r.capacity{return Err("UCM pending history controls full".into())}
  let revision=r.worker.revision().checked_add(1).ok_or("UCM revision overflow")?;
  let reply=r.worker.begin_history(revision,UcmHistoryEvent::Scope{scope:k.to_string(),masks:masks.clone(),dual_hot,now_ns:ns})?;
  let ticket=reply.ticket;
  r.retirements.push_back(reply);
  if self.ucm_history_context.insert(ticket,ScopeHistoryContext{key:k.to_string(),entries,ready:inputs.ready.clone(),observed_ns:ns}).is_some(){
   return Err("UCM duplicate history context".into())
  }
  self.ucm_history_memo.insert(k.to_string(),(masks,dual_hot));self.ucm_history_seen.insert(k.to_string());Ok(())
 }
 fn finish_ucm_publication(&mut self,k:&str,ns:u64,wall:i64,candidate:SourceId,reason:&'static str,applied:Option<AppliedHotSwitch>,started_ns:Option<u64>)->Result<Vec<Output>,String>{
  self.finish_ucm_publication_certified(k,ns,wall,candidate,reason,applied,started_ns,None)
 }
 fn finish_ucm_publication_certified(&mut self,k:&str,ns:u64,wall:i64,candidate:SourceId,reason:&'static str,applied:Option<AppliedHotSwitch>,started_ns:Option<u64>,proof:Option<&UcmStableChoice>)->Result<Vec<Output>,String>{
  let (ns,wall)=self.clock.map(|clock|clock()).unwrap_or((ns,wall));
  let inputs=self.prepare_hot_selection_inner(k,ns,wall,false);
  if proof.is_some_and(|proof|!self.ucm_stable_choice_matches(k,proof,&inputs.candidates,ns,wall)){
   self.ucm_stable_choices.remove(k);self.record_ucm_cycle(k,started_ns,false,&[]);self.retry_ucm_scope(k.to_string());return Ok(Vec::new())
  }
  if self.manager.active_for_scope(&candidate)!=Some(&candidate)||!self.ucm_candidate_allowed(k,&inputs,&candidate,ns,wall){
   self.record_ucm_cycle(k,started_ns,false,&[]);self.wake_ucm();return Ok(Vec::new())
  }
  self.record_r1247_selection_evaluation(k,ns,wall,&inputs.ready,&inputs.candidates,inputs.prior.as_ref(),inputs.initial_wait,Some((&candidate,reason)));
  let mut out=Vec::new();
  if let Some(applied)=applied{out.push(self.publish_applied_hot_switch(k,ns,wall,applied)?);}
  else if !self.selected.get(k).is_some_and(|p|p.session==candidate.logical_session){return Err("UCM applied selection mismatch: fail closed".into())}
  let mut out=self.finish_hot_publication(k,ns,wall,inputs.ordered_scope_sessions,inputs.ready,inputs.prior,out)?;
  let end=crate::ingress_runtime_r1205::mono_ns();
  for item in &mut out{if let Output::Frame(frame)|Output::Book(frame,_)=item{
   if let Some(t)=frame.source.as_mut().and_then(|s|s.r1279_timing.as_mut()){
    t.source_selection_end_ns=Some(end);
    if t.frame_residence_end_ns.is_none(){t.frame_residence_end_ns=Some(end);t.frame_residence_status=Some("released".into());}
   }
  }}
  self.record_ucm_cycle(k,started_ns,true,&out);self.wake_ucm();Ok(out)
 }
 fn select_inner(&mut self,k:&str,ns:u64,wall:i64)->Result<Vec<Output>,String>{
  self.manager.check()?;
  if self.manager.remote().is_some(){
   // Only the existing scope registry is marked dirty; no frame/event
   // is dropped or held while the worker answers.
   self.authority_write_timed(|authority|authority.remove(k))?;
   // Never-book scopes have no readiness history to mutate. Previously
   // evaluated empty scopes still record the first actual HOT deficit.
   let no_books=!self.book_scope_sessions.contains_key(k);
   let empty_history=self.ucm_history_memo.get(k).is_some_and(|(masks,hot)|masks.is_empty()&&!*hot);
   let pending_same=self.ucm_pending.contains_key(k);
   if no_books&&!pending_same&&(!self.ucm_history_seen.contains(k)||empty_history){
    self.metrics.hot_deficits=self.metrics.hot_deficits.saturating_add(1);
    if let Some(p)=self.ucm_scope_profile.as_mut().and_then(|rows|rows.get_mut(k)){p.skipped_empty+=1;}
    return Ok(Vec::new())
   }
   self.mark_ucm_dirty(k.to_string(),false);
   return Ok(Vec::new())
  }
  let inputs=self.prepare_hot_selection(k,ns,wall);
  let r1382_manager_started=self.r1273_profile_enabled.then(std::time::Instant::now);
  let manager_choice=if inputs.initial_wait{None}else{self.manager.choose_candidate(&inputs.candidates,ns,wall)};
  self.r1273_record_elapsed("r1382_manager_choice",r1382_manager_started);
  self.finish_hot_selection(k,ns,wall,inputs,manager_choice)
 }
 // Same admission and source order as the original select_inner.
 fn prepare_hot_selection(&mut self,k:&str,ns:u64,wall:i64)->HotSelectionInputs{
  self.prepare_hot_selection_inner(k,ns,wall,true)
 }
 fn prepare_hot_selection_inner(&mut self,k:&str,ns:u64,wall:i64,local_history:bool)->HotSelectionInputs{
  let r1382_ready_started=self.r1273_profile_enabled.then(std::time::Instant::now);
  // R1397: one HashMap pass produces the original traversal order for all
  // per-scope HOT work; unordered HashSet membership is NOT used for ties.
  let mut ready=Vec::new();
  let ordered_scope_sessions=if self.manager.remote().is_some(){
   let order=self.ucm_ordered_sessions(k);
   for session in &order{
    if let Some(book)=self.books.get(&(session.clone(),k.to_string())){
     if self.ready(book,ns,wall){ready.push(session.clone());}
    }
   }
   order
  }else{
   let mut order=Vec::new();
   for ((session,scope),book) in &self.books{
    if scope==k{order.push(session.clone());if self.ready(book,ns,wall){ready.push(session.clone());}}
   }
   order
  };
  self.r1273_record_elapsed("r1382_ready_scan",r1382_ready_started);
  let r1382_mark_started=self.r1273_profile_enabled.then(std::time::Instant::now);
  for session in &ready{if let Some(b)=self.books.get_mut(&(session.clone(),k.to_string())){b.qualified=true;if !matches!(b.state,State::Active|State::Standby){b.state=State::HotReady;}}}
  self.r1273_record_elapsed("r1382_ready_mark",r1382_mark_started);
  let r1382_audit_started=self.r1273_profile_enabled.then(std::time::Instant::now);
  if local_history{
   self.record_readiness_transitions(k,ns,wall,&ordered_scope_sessions);
   self.record_dual_hot_transition(k,&ready,ns,wall);
  }
  self.r1273_record_elapsed("r1382_readiness_audit",r1382_audit_started);
  let prior=self.selected.get(k).cloned();
  let r1382_candidate_started=self.r1273_profile_enabled.then(std::time::Instant::now);
  let prior_unready=prior.as_ref().is_some_and(|p|!self.books.get(&(p.session.clone(),k.to_string())).is_some_and(|b|self.ready(b,ns,wall)));
  // R1386: the ready list was computed with the same (ns,wall) above.
  // Between that scan and this step only already-ready SourceBook display states
  // and UCM diagnostic history changed; neither can add a newly-ready source.
  // Iterate only the known ready sessions, preserving their original HashMap
  // traversal order; retain the exact highwater/failover/authority filters.
  let candidates=ready.iter().filter_map(|session|{
   let book=self.books.get(&(session.clone(),k.to_string()))?;
   let selected_session=prior.as_ref().is_some_and(|p|p.session==*session);
   let equivalent_failover=prior_unready&&prior.as_ref().is_some_and(|p|p.session!=*session&&
    self.books.get(&(p.session.clone(),k.to_string())).is_some_and(|old|same_published_book(old,book)));
   let seq=book.update.book.sequence.unwrap_or(0);let event=book.update.book.exchange_ts_ms.unwrap_or(0);
   (selected_session||equivalent_failover||advances_highwater(book.frame.venue,seq,event,self.highwater.get(k).copied())).then(||book.manager_id.clone())
  }).collect::<Vec<_>>();
  self.r1273_record_elapsed("r1382_candidate_scan",r1382_candidate_started);
  let initial_wait=prior.is_none()&&self.highwater.get(k).is_none()&&ready.len()<2;
  HotSelectionInputs{ordered_scope_sessions,ready,prior,candidates,initial_wait}
 }
 fn prepare_hot_switch(&mut self,k:&str,ns:u64,wall:i64,prior:Option<&Selected>,
  candidate:SourceId,manager_reason:&'static str)->Result<HotSwitchPlan,String>{
    let session=candidate.logical_session.clone();
    let started=self.r1273_profile_enabled.then(std::time::Instant::now);
    let old_readiness=prior.as_ref().and_then(|p|self.books.get(&(p.session.clone(),k.to_string()))).map(|b|self.readiness_snapshot(b,ns,wall));
    let new_readiness=self.books.get(&(session.clone(),k.to_string())).map(|b|self.readiness_snapshot(b,ns,wall));
    let epoch_started_ns=crate::ingress_runtime_r1205::mono_ns();
    if let Some(book)=self.books.get_mut(&(session.clone(),k.to_string())){if let Some(t)=book.frame.source.as_mut().and_then(|s|s.r1279_timing.as_mut()){t.source_epoch_start_ns=Some(epoch_started_ns);}}
    let epoch=if let Some(worker)=self.store_worker.as_ref(){
     let (epoch,timing)=worker.call_profiled(|store|store.next_epoch())?;
     self.r1273_record_ns("source_epoch_worker_total",timing.total_ns);self.r1273_record_ns("source_epoch_worker_queue",timing.queue_wait_ns);self.r1273_record_ns("source_epoch_store_lock_wait",timing.store_lock_wait_ns);self.r1273_record_ns("source_epoch_store_operation",timing.operation_ns);self.r1273_record_ns("source_epoch_other_wait",timing.other_wait_ns);
     if let Some(book)=self.books.get_mut(&(session.clone(),k.to_string())){if let Some(t)=book.frame.source.as_mut().and_then(|s|s.r1279_timing.as_mut()){t.source_epoch_worker_queue_ns=Some(timing.queue_wait_ns);t.source_epoch_store_lock_wait_ns=Some(timing.store_lock_wait_ns);t.source_epoch_store_operation_ns=Some(timing.operation_ns);t.source_epoch_other_wait_ns=Some(timing.other_wait_ns);}}
     epoch
    }else{self.memory_epoch=self.memory_epoch.checked_add(1).ok_or("R1205 volatile epoch overflow")?;self.memory_epoch};
    let epoch_end_ns=crate::ingress_runtime_r1205::mono_ns();self.r1273_record_ns("source_epoch_total",epoch_end_ns.saturating_sub(epoch_started_ns));
    if let Some(book)=self.books.get_mut(&(session.clone(),k.to_string())){if let Some(t)=book.frame.source.as_mut().and_then(|s|s.r1279_timing.as_mut()){t.source_epoch_end_ns=Some(epoch_end_ns);}}

    Ok(HotSwitchPlan{engine_revocation:self.ucm_witness_revocations.get(k).copied().unwrap_or(0),candidate,manager_reason,prior:prior.cloned(),epoch,old_readiness,new_readiness,started})
 }
 // Only the existing synchronous owner is used here today. No fake worker
 // ACK: the applied token is constructed after set_active actually succeeds.
 fn apply_local_hot_switch(&mut self,plan:HotSwitchPlan)->Result<AppliedHotSwitch,String>{
    if !self.manager.set_active(&plan.candidate){return Err("manager candidate lost HOT_READY before commit".into())}
    Ok(AppliedHotSwitch(plan))
 }
 fn publish_applied_hot_switch(&mut self,k:&str,ns:u64,wall:i64,applied:AppliedHotSwitch)->Result<Output,String>{
    let HotSwitchPlan{engine_revocation,candidate,manager_reason,prior,epoch,old_readiness,new_readiness,started}=applied.0;
    let session=candidate.logical_session.clone();
    // A superseded plan must not publish a new selection epoch or Reset.
    let expected=prior.as_ref().map(|p|(p.session.as_str(),p.epoch,p.changed_ns));
    let current=self.selected.get(k).map(|p|(p.session.as_str(),p.epoch,p.changed_ns));
    if expected!=current || self.ucm_witness_revocations.get(k).copied().unwrap_or(0)!=engine_revocation
       || self.manager.active_for_scope(&candidate)!=Some(&candidate)
       || !self.books.get(&(session.clone(),k.to_string())).is_some_and(|b|b.manager_id==candidate){
      self.authority_write_timed(|authority|authority.remove(k))?;
      return Err("applied HOT switch superseded: fail closed".into())
    }
    let b=self.books.get(&(session.clone(),k.to_string())).ok_or("candidate vanished")?;
    let reason=if prior.is_none(){manager_reason}else if manager_reason=="sustained_quality_improvement"{manager_reason}else{"active_source_unready_emergency_failover"};
    let old_quality=prior.as_ref().and_then(|p|self.books.get(&(p.session.clone(),k.to_string())).and_then(|b|self.manager.quality(&b.manager_id)));
    let quality=self.manager.quality(&candidate);
    let old_source_frame=prior.as_ref().and_then(|p|self.books.get(&(p.session.clone(),k.to_string()))).map(|old|{
     let source=old.frame.source.as_ref();
     serde_json::json!({"session":source.map(|s|s.session_id.as_str()),"connection_epoch":source.map(|s|s.connection_epoch),
      "accepted_id":source.map(|s|s.accepted_id),"trace_id":source.map(|s|format!("{}:{}:{}",s.session_id,s.connection_epoch,s.accepted_id)),
      "received_ns":old.frame.received_ns,"event_ts_ms":old.update.book.exchange_ts_ms,
      "sequence":old.update.book.sequence,"selection_epoch":old.frame.epoch})
    });
    let source=b.frame.source.as_ref();
    let new_source_frame=serde_json::json!({"session":session,"connection_epoch":source.map(|s|s.connection_epoch),
     "accepted_id":source.map(|s|s.accepted_id),"trace_id":source.map(|s|format!("{}:{}:{}",s.session_id,s.connection_epoch,s.accepted_id)),
     "received_ns":b.frame.received_ns,"event_ts_ms":b.update.book.exchange_ts_ms,
     "sequence":b.update.book.sequence,"selection_epoch":epoch,"selected_ns":ns,
     "selection_delay_from_receive_ns":ns.saturating_sub(b.frame.received_ns)});
    if self.selection_audit.len()>=256{self.selection_audit.pop_front();self.metrics.selection_audit_evictions=self.metrics.selection_audit_evictions.saturating_add(1);}
    self.selection_audit.push_back(serde_json::json!({"scope":k,"mono_ns":ns,"old_source":prior.as_ref().map(|p|p.session.as_str()),"new_source":session,"reason":reason,"manager_proposal_reason":manager_reason,"old_quality":old_quality,"new_quality":quality,"old_readiness":old_readiness,"new_readiness":new_readiness,"old_source_frame":old_source_frame,"new_source_frame":new_source_frame,"book_fresh":Self::fresh(b,ns,wall),"generation":candidate.generation,"epoch":epoch}));
    if prior.is_some(){self.metrics.switches+=1;}
    let trace_frame=b.frame.clone();
    let reset=Output::Reset{venue:b.frame.venue,base:b.update.base.clone(),epoch};
    self.r1310_lifecycle(&trace_frame,"authority_reset",Some(epoch));
    self.selected.insert(k.into(),Selected{session,epoch,changed_ns:ns});
    if self.manager.remote().is_some(){self.ucm_fact_refresh.insert(k.to_string());}
    self.r1273_record_elapsed("r1382_source_change_audit_epoch",started);
    Ok(reset)
 }
 // Current synchronous call site only. An asynchronous caller must first
 // obtain the worker's applied ACK, revalidate its revision and book/authority
 // identity, and reconcile a superseded commit before entering publication.
 fn finish_hot_selection(&mut self,k:&str,ns:u64,wall:i64,inputs:HotSelectionInputs,
  manager_choice:Option<(SourceId,&'static str)>)->Result<Vec<Output>,String>{
  let HotSelectionInputs{ordered_scope_sessions,ready,prior,candidates,initial_wait}=inputs;
  let r1382_eval_started=self.r1273_profile_enabled.then(std::time::Instant::now);
  self.record_r1247_selection_evaluation(k,ns,wall,&ready,&candidates,prior.as_ref(),initial_wait,manager_choice.as_ref().map(|(id,reason)|(id,*reason)));
  self.r1273_record_elapsed("r1382_selection_eval_audit",r1382_eval_started);
  let mut out=Vec::new();
  if let Some((candidate,manager_reason))=manager_choice{
   let session=candidate.logical_session.clone();
   let manager_still_active=self.manager.active_for_scope(&candidate)==Some(&candidate);let changed=prior.as_ref().map_or(true,|p|p.session!=session)||!manager_still_active;
   if changed{
    let plan=self.prepare_hot_switch(k,ns,wall,prior.as_ref(),candidate,manager_reason)?;
    let applied=self.apply_local_hot_switch(plan)?;
    out.push(self.publish_applied_hot_switch(k,ns,wall,applied)?);
   }else if !self.manager.set_active(&candidate){return Err("selected manager source is no longer HOT_READY".into())}
  }
  self.finish_hot_publication(k,ns,wall,ordered_scope_sessions,ready,prior,out)
 }
 fn finish_hot_publication(&mut self,k:&str,ns:u64,wall:i64,ordered_scope_sessions:Vec<String>,ready:Vec<String>,
  prior:Option<Selected>,mut out:Vec<Output>)->Result<Vec<Output>,String>{
  if ready.len()<2{
   self.mark_scope_hold_reason(k,"waiting_for_two_fresh_hot_sources");
   let was_authorized=self.authority_write_timed(|authority|authority.remove(k))?.is_some();self.metrics.hot_deficits+=1;
   if was_authorized{if let Some(p)=self.selected.get(k){if let Some(book)=self.books.get(&(p.session.clone(),k.to_string())){self.metrics.blocked+=1;out.push(Output::Blocked{venue:book.frame.venue,base:book.update.base.clone(),reason:"R1206_NO_FRESH_HOT_STANDBY".into()})}}}
   return Ok(out)
  }
  let selected=self.selected.get(k).cloned();
  let Some(selected)=selected else{self.mark_scope_hold_reason(k,"no_selected_source");return Ok(out)};
  let (ns,wall)=self.clock.map(|clock|clock()).unwrap_or((ns,wall));
  let r1382_standby_started=self.r1273_profile_enabled.then(std::time::Instant::now);
  let standby=ordered_scope_sessions.iter().filter_map(|session|{
   if session==&selected.session{return None}
   let book=self.books.get(&(session.clone(),k.to_string()))?;
   self.ready(book,ns,wall).then_some((session,book))
  }).max_by_key(|(_,book)|book.frame.received_ns)
   .map(|(id,book)|(id.clone(),book.frame.received_ns,book.update.book.exchange_ts_ms.unwrap_or(0)));
  self.r1273_record_elapsed("r1382_standby_scan",r1382_standby_started);
  let Some((standby_session,standby_received_ns,standby_event_ms))=standby else{self.mark_scope_hold_reason(k,"no_fresh_hot_standby");self.authority_write_timed(|authority|authority.remove(k))?;return Ok(out)};
  let bk=(selected.session.clone(),k.to_string());
  if !self.books.contains_key(&bk){
   if let Some(manager_id)=self.manager_ids.get(&(selected.session.clone(),k.to_string())).cloned(){self.manager.quarantine(&manager_id);}
   self.selected.remove(k);
   if self.manager.remote().is_some(){self.ucm_fact_refresh.insert(k.to_string());}
   self.authority_write_timed(|authority|authority.remove(k))?;
   self.metrics.blocked=self.metrics.blocked.saturating_add(1);
   return Ok(out);
  }
  let r1382_selected_checks_started=self.r1273_profile_enabled.then(std::time::Instant::now);
  let equivalent_failover=prior.as_ref().is_some_and(|p|p.session!=selected.session&&
   self.books.get(&(p.session.clone(),k.to_string())).is_some_and(|old|!self.ready(old,ns,wall)&&
    self.books.get(&bk).is_some_and(|candidate|same_published_book(old,candidate))));
  let selected_fresh=self.books.get(&bk).is_some_and(|book|Self::fresh(book,ns,wall)&&(self.manager.remote().is_none()||self.ready(book,ns,wall)));
  self.r1273_record_elapsed("r1382_selected_checks",r1382_selected_checks_started);
  if !selected_fresh{self.mark_scope_hold_reason(k,"selected_source_book_stale");if let Some(book)=self.books.get_mut(&bk){book.state=State::Degraded;}
   self.authority_write_timed(|authority|authority.remove(k))?;return Ok(out)}
  let mut r1382_output_parse_ns=None;
  let book=self.books.get_mut(&bk).ok_or("selected book")?;
  if !Self::fresh(book,ns,wall){book.state=State::Degraded;self.authority_write_timed(|authority|authority.remove(k))?;return Ok(out)}
  book.state=State::Active;
  let seq=book.update.book.sequence.unwrap_or(0);let event=book.update.book.exchange_ts_ms.unwrap_or(0);
  let permit=Permit{snapshot_witness:book.snapshot_witness.clone(),session:selected.session.clone(),epoch:selected.epoch,received_ns:book.frame.received_ns,event_ms:book.update.book.exchange_ts_ms.ok_or("event time")?,standby_session,standby_received_ns,standby_event_ms};
  let (_,authority_wait_ns,authority_hold_ns)=timed_authority_write(&self.authority,|authority|authority.insert(k.into(),permit.clone()))?;
  if let Some(t)=book.frame.source.as_mut().and_then(|s|s.r1279_timing.as_mut()){t.source_selection_authority_wait_ns=Some(authority_wait_ns);t.source_selection_authority_hold_ns=Some(authority_hold_ns);}
  let newer=advances_highwater(book.frame.venue,seq,event,self.highwater.get(k).copied());
  if (book.retained&&newer)||equivalent_failover{
   let mut frame=book.frame.clone();frame.epoch=selected.epoch;
   let source=frame.source.as_mut().ok_or("source")?;source.selection_epoch=selected.epoch;source.selected_ns=Some(ns);source.selected_permit=Some(permit);
   book.retained=false;if newer{self.highwater.insert(k.into(),(seq,event));if self.manager.remote().is_some(){self.ucm_fact_refresh.insert(k.to_string());}}self.metrics.submitted+=1;let r1382_publish_started=self.r1273_profile_enabled.then(std::time::Instant::now);let is_books=book.output_is_books;r1382_output_parse_ns=r1382_publish_started.map(|t|t.elapsed().as_nanos().min(u64::MAX as u128) as u64);if is_books{let mut update=book.update.clone();update.book.epoch=selected.epoch;out.push(Output::Book(frame,update));}else{out.push(Output::Frame(frame));}
  }
  if let Some(ns)=r1382_output_parse_ns{self.r1273_record_ns("r1402h_output_type_cached",ns);}
  let r1382_finalize_started=self.r1273_profile_enabled.then(std::time::Instant::now);
  for session in &ordered_scope_sessions{
   if session!=&selected.session&&ready.contains(session){
    if let Some(book)=self.books.get_mut(&(session.clone(),k.to_string())){book.state=State::Standby;}
   }
  }
  self.r1273_record_elapsed("r1382_standby_finalize",r1382_finalize_started);
  if out.is_empty(){self.mark_scope_hold_reason(k,"no_newer_sequence_or_frame_already_released");}
  Ok(out)
 }
 pub fn tick(&mut self,ns:u64,wall:i64)->Result<Vec<Output>,String>{let call_id=next_engine_call_id();let start=crate::ingress_runtime_r1205::mono_ns();let result=self.tick_inner(ns,wall,call_id);let end=crate::ingress_runtime_r1205::mono_ns();self.trace(serde_json::json!({"kind":"engine_call_span","call":"tick","call_id":call_id,"start_ns":start,"end_ns":end,"duration_ns":end.saturating_sub(start),"status":"complete","outcome":if result.is_ok(){"ok"}else{"error"}}));if self.r1273_profile_enabled{self.r1273_record_ns("e0_tick_total",crate::ingress_runtime_r1205::mono_ns().saturating_sub(start));}result}
 fn ucm_stable_tick(&mut self,k:&str,ns:u64,wall:i64)->bool{
  if self.ucm_dirty.contains(k)||self.ucm_pending.contains_key(k){return false}
  let order=self.ucm_ordered_sessions(k);
  if order.len()<2{return false}
  let Some(proof)=self.ucm_stable_choices.get(k)else{return false};
  let Some(prior)=self.selected.get(k)else{return false};
  let Some((previous_masks,previous_dual))=self.ucm_history_memo.get(k)else{return false};
  if !*previous_dual||previous_masks.len()!=order.len(){return false}
  let mut candidates=Vec::new();let mut standby=None;
  let mut selected_book=None;
  for (session,(old_session,old_mask)) in order.iter().zip(previous_masks){
   let Some(book)=self.books.get(&(session.clone(),k.to_string()))else{return false};
   if session!=old_session||!self.ready(book,ns,wall)||self.readiness_predicate_mask(book,ns,wall)!=*old_mask{return false}
   let seq=book.update.book.sequence.unwrap_or(0);let event=book.update.book.exchange_ts_ms.unwrap_or(0);
   if session==&prior.session{selected_book=Some(book);candidates.push(book.manager_id.clone());}
   else{
    if advances_highwater(book.frame.venue,seq,event,self.highwater.get(k).copied()){candidates.push(book.manager_id.clone());}
    if standby.map_or(true,|b:&SourceBook|book.frame.received_ns>=b.frame.received_ns){standby=Some(book);}
   }
  }
  let (Some(book),Some(standby))=(selected_book,standby)else{return false};
  if book.retained||!self.ucm_stable_choice_matches(k,proof,&candidates,ns,wall){return false}
  let Ok(auth)=self.authority.read()else{return false};
  auth.get(k).is_some_and(|p|p.session==prior.session&&p.epoch==prior.epoch
   &&p.received_ns==book.frame.received_ns&&Some(p.event_ms)==book.update.book.exchange_ts_ms
   &&p.standby_session==standby.manager_id.logical_session&&p.standby_received_ns==standby.frame.received_ns
   &&Some(p.standby_event_ms)==standby.update.book.exchange_ts_ms
   &&p.snapshot_witness.as_ref().zip(book.snapshot_witness.as_ref()).is_some_and(|(a,b)|a.same_generation(b)))
 }
 fn tick_inner(&mut self,ns:u64,wall:i64,parent_call_id:u64)->Result<Vec<Output>,String>{
  let mut out=Vec::new();
  if self.manager.remote().is_some(){
   let started=self.r1273_profile_enabled.then(std::time::Instant::now);
   let plan=self.manager.remote().unwrap().worker.tick_plan(ns,wall);
   let mut due=self.ucm_clock_index.due(ns,wall);
   due.extend(self.ucm_fact_refresh.iter().cloned());
   for (_,segment,base,_) in &plan.evaluate{
    let venue=match segment.as_str(){"spot"=>Venue::OkxSpot,"usdm"=>Venue::BinanceFutures,_=>continue};
    let k=key(venue,base);if self.universe.contains(&k){due.insert(k);}
   }
   self.r1273_record_elapsed("r1408b_tick_due_plan",started);
   let mut facts=Vec::new();let mut keys=Vec::new();
   for k in due{
    if self.book_scope_sessions.contains_key(&k){
     self.ucm_stable_choices.remove(&k);
     if let Some(anchor)=self.ucm_anchor(&k){facts.push(self.ucm_book_facts(&k,anchor,ns,wall));keys.push(k);}
    }else{
     self.ucm_clock_index.remove(&k);self.ucm_fact_refresh.remove(&k);
     if self.ucm_history_seen.contains(&k){keys.push(k);}
    }
   }
   self.manager.tick_with_book_facts(plan,facts)?;
   for k in keys{
    if self.ucm_dirty.contains(&k){
     self.authority_write_timed(|authority|authority.remove(&k))?;
     if let Some(p)=self.ucm_scope_profile.as_mut().and_then(|rows|rows.get_mut(&k)){p.skipped_dirty_tick+=1;}
    }else{out.extend(self.select(&k,ns,wall,parent_call_id,"tick")?);}
   }
  }else{
   self.manager.tick(ns,wall);
   let keys=self.universe.iter().cloned().collect::<Vec<_>>();
   for k in keys{out.extend(self.select(&k,ns,wall,parent_call_id,"tick")?);}
  }
  self.manager.check()?;
  if self.ucm_has_ready(){self.wake_ucm();}Ok(out)
 }
 pub fn disconnected(&mut self,id:&str,ns:u64,wall:i64)->Result<Vec<Output>,String>{
  self.sources.insert(id.into(),State::Degraded);
  for ((s,k),book) in self.books.iter_mut(){if s==id{
   book.state=State::Degraded;
   if self.manager.remote().is_some(){self.ucm_fact_refresh.insert(k.clone());}
  }}
  let out=self.tick(ns,wall)?;
  let manager_ids=self.manager_ids.iter().filter(|((session,_),_)|session==id).map(|(_,source)|source.clone()).collect::<Vec<_>>();self.manager.apply_connection_lifecycle(&manager_ids,ConnectionLifecycleAction::Close);self.manager_ids.retain(|(session,_),_|session!=id);
  let retired=self.books.keys().filter(|(s,_)|s==id).cloned().collect::<Vec<_>>();
  for k in retired{if let Some(book)=self.remove_book(&k){if let Some(w)=&book.snapshot_witness{w.revoke();}if book.retained{self.finish(&book.frame,"source_disconnected")?;}}}
  self.market_data_processor.clear_session(id);self.prepared_chain_accepted.retain(|(session,_),_|session!=id);self.resyncing.retain(|(session,_)|session!=id);self.resubscribe_last_ns.retain(|(session,_),_|session!=id);self.connection_scopes.remove(id);let retired_readiness_states=self.manager.retire_readiness_session(id);self.metrics.readiness_state_retirements=self.metrics.readiness_state_retirements.saturating_add(retired_readiness_states as u64);self.sources.remove(id);self.manager.check()?;self.flush_quarantines()?;Ok(out)
 }
 pub fn hot_complete(&self,session:&str,venue:Venue,bases:&[String],ns:u64,wall:i64)->bool{
  bases.iter().all(|base|self.books.get(&(session.into(),key(venue,base))).is_some_and(|book|self.ready(book,ns,wall)))
 }
 pub fn bad_scope_count(&self,session:&str,venue:Venue,bases:&[String],ns:u64,wall:i64)->usize{
  bases.iter().filter(|base|!self.books.get(&(session.into(),key(venue,base))).is_some_and(|book|self.ready(book,ns,wall))).count()
 }
 pub fn qualification_proof(&self,venue:Venue,base:&str,ns:u64,wall:i64)->serde_json::Value{
  let k=key(venue,base);let books=self.books.iter().filter(|((_,scope),_)|scope==&k).map(|((session,_),b)|serde_json::json!({"session":session,"samples":b.good,"session_qualified":b.qualified,"qualification_first_ns":b.first_ns,"last_received_ns":b.frame.received_ns,"last_event_ms":b.update.book.exchange_ts_ms,"fresh":Self::fresh(b,ns,wall),"hot_ready":self.ready(b,ns,wall),"state":format!("{:?}",b.state)})).collect::<Vec<_>>();serde_json::json!({"key":k,"observed_ns":ns,"books":books})
 }
 pub fn coverage(&self,ns:u64,wall:i64)->serde_json::Value{
  let mut rows=Vec::new();for k in &self.universe{let sources=self.books.iter().filter(|((_,scope),b)|scope==k&&self.ready(b,ns,wall)).map(|((s,_),_)|s.clone()).collect::<Vec<_>>();let selected=self.selected.get(k);rows.push(serde_json::json!({"key":k,"hot_ready":&sources,"fresh_sources":self.books.iter().filter(|((_,scope),b)|scope==k&&Self::fresh(b,ns,wall)).map(|((s,_),_)|s).collect::<Vec<_>>(),"active":selected.filter(|s|sources.contains(&s.session)&&self.authority.read().map(|m|m.contains_key(k)).unwrap_or(false)).map(|s|&s.session),"selected":selected.map(|s|&s.session),"epoch":selected.map(|s|s.epoch),"last_switch_ns":selected.map(|s|s.changed_ns)}));}rows.sort_by_key(|v|v["key"].as_str().unwrap_or("").to_string());serde_json::json!(rows)
 }
}
pub fn provenance(session:String,connection_epoch:u64)->SourceProvenance{SourceProvenance{selected_permit:None,prepared:None,journal_id:String::new(),accepted_id:0,session_id:session,connection_epoch,selection_epoch:0,payload_sha256:String::new(),transport:None,accepted_ns:None,selected_ns:None,r1247_timing:None,r1279_timing:None,frame_trace_id:String::new()}}
/// Reconcile cold pending against actual independently mirrored canonical receipts.
/// Never replay into a decision. Uncommitted copies are explicitly quarantined.
pub fn reconcile(store:&SharedStore,w:&crate::native_r1173_compact_writer::Writer)->Result<usize,String>{
 w.verify_pair()?;let mut s=store.lock().map_err(|_|"store poison")?;let frames=s.pending()?;
 for f in &frames{let p=f.source.as_ref().ok_or("source")?;let proof=w.ingress_receipt(&p.journal_id,p.accepted_id,&p.payload_sha256)?;match proof{Some(h)=>s.finish(f,"published","cold_mirrored_source",Some(&h))?,None=>s.finish(f,"quarantined","cold_no_trade_replay",None)?,}}
 Ok(frames.len())
}
#[cfg(test)]
#[path="market_data_ingress_r1205_tests.rs"]
mod tests;

impl Drop for Engine { fn drop(&mut self){
 for book in self.books.values(){if let Some(w)=&book.snapshot_witness{w.revoke();}}
 if let Some(rows)=&self.ucm_scope_profile{
  // Preserve existing Engine phase counters even if run_internal returns Err
  // before building its successful-run Summary. No new per-frame tracing.
  if self.r1273_profile_enabled{
   if let Ok(profile)=self.r1273_profile.lock(){eprintln!("R1402_ENGINE_PROFILE {}",profile.snapshot());}
  }
  eprintln!("R1402_ENGINE_CENSUS {}",serde_json::json!({"metrics":self.metrics,"books":self.books.len(),"selected":self.selected.len()}));
  eprintln!("R1402_SCOPE_PROFILE {}",serde_json::json!({"scopes":rows,"unresolved_dirty":self.ucm_dirty.len(),"pending_scopes":self.ucm_pending.len(),"pending_index_entries":self.ucm_wait.len(),"deadline_index_entries":self.ucm_deadlines.len(),"startable_scopes":self.ucm_startable.len()}));
 }
} }
