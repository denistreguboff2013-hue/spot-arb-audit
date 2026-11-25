// Included in the existing durable owner. No independent store.
use crate::native_paper_exit_r1298 as exit_policy;
#[derive(Clone,Debug,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
struct ExitSession {
 generation:u64,
 adapter:AdapterRegistry,
 closed:Option<ClosedProof>,
 archives:Vec<ClosedGeneration>,
 generation_keys:BTreeMap<String,u64>,
 historical_late:BTreeMap<String,SimExecutionEvent>,
 request:Option<exit_policy::ExitRequest>,
}
impl ExitSession {fn new()->Self{Self{generation:1,request:None,adapter:AdapterRegistry::default(),closed:None,archives:Vec::new(),generation_keys:BTreeMap::new(),historical_late:BTreeMap::new()}}}
impl DurablePaper {
 pub fn request_isolated_exit(&mut self,c:&hedge::MarketContext)->Result<(),ExecutionError>{
  if self.fenced{return Err(ExecutionError::OwnerFenced)}
  let mut s=self.isolated.clone().ok_or(ExecutionError::Invalid("isolated owner"))?;
  let rules=s.identity.exit.as_ref().ok_or(ExecutionError::Invalid("exit policy absent"))?;
  let full=s.full.as_mut().ok_or(ExecutionError::Invalid("exit session absent"))?;
  if c.clock_domain!=s.identity.clock_domain||c.input_id.is_empty(){return Err(ExecutionError::Invalid("exit trigger identity"))}
  if let Some(old)=&full.request{return if old.trigger_id==c.input_id{Ok(())}else{Err(ExecutionError::Invalid("exit already requested"))}}
  if c.now_ns<s.last_clock_ns||!c.spot_hot||!c.futures_hot||self.lifecycle.state!=risk::LifecycleState::Hedged||
    !self.exposure_view().known||self.lifecycle.orders.values().any(|o|o.uncertain||
      !matches!(o.status,risk::OrderStatus::Filled|risk::OrderStatus::Cancelled|risk::OrderStatus::Rejected)){
   return Err(ExecutionError::Invalid("exit trigger state"))
  }
  full.request=Some(exit_policy::ExitRequest{trigger_id:c.input_id.clone(),started_ns:c.now_ns,
   deadline_ns:c.now_ns.checked_add(rules.max_exit_duration_ns).ok_or(ExecutionError::Invalid("exit deadline overflow"))?});
  s.last_clock_ns=c.now_ns;
  let e=SimExecutionEvent{event_id:format!("exit-trigger:{}",c.input_id),order_id:"owner".into(),kind:SimEventKind::Recovery,
   simulated:true,event_wall_ms:c.wall_ms,event_mono_ns:c.now_ns,quantity:None,price:None,
   reason:Some("explicit synthetic cover-first exit trigger".into()),intent:None,census_order_ids:None};
  self.commit_transition(e,Some(s),|_,_|Ok(()))
 }
 pub fn exit_requested(&self)->bool{self.isolated.as_ref().and_then(|s|s.full.as_ref()).and_then(|s|s.request.as_ref()).is_some()}
 pub fn isolated_exit_decision(&self,spot:&Book,futures:&Book,c:&hedge::MarketContext)->exit_policy::ExitDecision{
  let Some(s)=&self.isolated else{return exit_policy::ExitDecision::FailClosed("unbound".into())};
  exit_policy::decide(&s.identity,&self.lifecycle,s.full.as_ref().and_then(|f|f.request.as_ref()),spot,futures,c)
 }
}

impl DurablePaper {
 pub fn register_isolated_exit(&mut self,spot:&Book,futures:&Book,c:&hedge::MarketContext)->Result<RegisteredIntent,ExecutionError>{
  if self.fenced{return Err(ExecutionError::OwnerFenced)}
  let mut s=self.isolated.clone().ok_or(ExecutionError::Invalid("isolated owner"))?;
  if c.now_ns<s.last_clock_ns{return Err(ExecutionError::Invalid("clock regression"))}
  let (leg,side,quantity,role)=match self.isolated_exit_decision(spot,futures,c){
   exit_policy::ExitDecision::FuturesCover{quantity}=>(risk::FillLeg::Futures,risk::FillSide::Cover,quantity,"cover"),
   exit_policy::ExitDecision::SpotExit{quantity}=>(risk::FillLeg::Spot,risk::FillSide::Sell,quantity,"sell"),
   _=>return Err(ExecutionError::Invalid("no supported exit intent")),
  };
  let full=s.full.as_ref().ok_or(ExecutionError::Invalid("exit session"))?;
  let key=hedge::digest(&(full.generation,role,&self.lifecycle.processed_events,&full.request))
   .map_err(|_|ExecutionError::Invalid("exit key"))?;
  if let Some(id)=s.keys.get(&key){return s.intents.get(id).cloned().ok_or(ExecutionError::CorruptJournal)}
  s.next_order=s.next_order.checked_add(1).ok_or(ExecutionError::Invalid("order counter overflow"))?;
  let scope=if self.universe.is_some(){format!("{}:{}",s.identity.run_id,s.identity.profile.base)}else{s.identity.run_id.clone()};
  let id=format!("{}:{role}:{}",scope,s.next_order);
  let intent=OrderIntent{intent_id:format!("intent:{id}"),order_id:id.clone(),leg,side,quantity,
   instrument:if leg==risk::FillLeg::Futures{s.identity.profile.futures_symbol.clone()}else{s.identity.profile.spot_symbol.clone()},
   created_wall_ms:c.wall_ms,created_mono_ns:c.now_ns,exit_reason:Some(risk::ExitReason::ManualRecovery)};
  let record=RegisteredIntent{intent:intent.clone(),command_key:key.clone(),rules_hash:s.rules_hash.clone(),
   quantity_units:s.identity.profile.quantity_units.clone(),witness:serde_json::json!({"reduce_only":true,
    "generation":full.generation,"context":c,"spot":spot,"futures":futures,"exit_request":full.request,"owner_revision":self.sequence})};
  s.keys.insert(key,id.clone());s.intents.insert(id.clone(),record.clone());s.last_clock_ns=c.now_ns;
  let event=SimExecutionEvent{event_id:format!("{id}:accepted"),order_id:id.clone(),kind:SimEventKind::Accepted,
   simulated:true,event_wall_ms:c.wall_ms,event_mono_ns:c.now_ns,quantity:None,price:None,
   reason:Some("R1298 reduce-only policy intent".into()),intent:Some(intent),census_order_ids:None};
  self.commit_transition(event,Some(s),|l,e|{
   if l.state==risk::LifecycleState::Hedged{
    l.request_exit(spot,futures,&risk::MarketGate{spot_hot:c.spot_hot,futures_hot:c.futures_hot,
     spot_book_valid:true,futures_book_valid:true},risk::ExitReason::ManualRecovery,c.now_ns,c.wall_ms)?;
   }else if !matches!(l.state,risk::LifecycleState::ExitPending|risk::LifecycleState::ExitPartiallyFilled){
    return Err(ExecutionError::Invalid("exit lifecycle state"))
   }
   let exposure=l.outstanding_exposure().ok_or(ExecutionError::Invalid("unknown exit inventory"))?;
   let available=if leg==risk::FillLeg::Futures{exposure.1/l.config.futures_contract_multiplier}else{exposure.0};
   if quantity>available||quantity<=0.0{return Err(ExecutionError::Invalid("reduce-only bound"))}
   l.order_census_complete=false;
   l.register_simulated_order(&id,leg,side,quantity,c.now_ns)?;
   l.apply_order_event(risk::OrderEvent{event_id:e.event_id.clone(),order_id:id.clone(),kind:risk::OrderEventKind::Accepted,event_ns:c.now_ns,simulated:true})?;
   Ok(())
  })?;
  Ok(record)
 }
}

include!("native_paper_adapter_registry_r1298.rs");

impl ExitSession {
 fn validate_registry(&self,s:&IsolatedState,l:&risk::Lifecycle)->Result<(),ExecutionError>{
  let bad=||ExecutionError::CorruptJournal;
  if !self.historical_late.is_empty()&&l.state!=risk::LifecycleState::RecoveryRequired{return Err(bad())}
  if l.state==risk::LifecycleState::Closed{
   let proof=self.closed.as_ref().ok_or_else(bad)?;
   self.adapter.census(l,self.generation)?;
   if self.adapter.spot_base!=0||self.adapter.futures_base!=0||proof.generation!=self.generation||
      proof.census_revision!=self.adapter.revision||proof.rules_hash!=s.rules_hash||
      proof.adapter_hash!=hedge::digest(&self.adapter).map_err(|_|bad())?||
      proof.execution_ids!=l.processed_events.keys().cloned().collect(){return Err(bad())}
  }
  if self.generation==0||self.adapter.orders.len()!=s.intents.len(){return Err(bad())}
  if self.generation!=self.archives.len() as u64+1{return Err(bad())}
  for (index,old) in self.archives.iter().enumerate(){
   if old.generation!=index as u64+1||old.lifecycle.state!=risk::LifecycleState::Closed||old.adapter.spot_base!=0||old.adapter.futures_base!=0||
      old.proof.generation!=old.generation||old.proof.census_revision!=old.adapter.revision||old.proof.rules_hash!=s.rules_hash||
      old.proof.adapter_hash!=hedge::digest(&old.adapter).map_err(|_|bad())?{return Err(bad())}
   old.adapter.census(&old.lifecycle,old.generation)?;
   if old.intents.len()!=old.adapter.orders.len()||old.events.keys().cloned().collect::<BTreeSet<_>>()!=old.proof.execution_ids||
      old.proof.execution_ids!=old.lifecycle.processed_events.keys().cloned().collect(){return Err(bad())}
   for (id,record) in &old.intents{
    let order=old.adapter.orders.get(id).ok_or_else(bad)?;
    if record.intent.order_id!=*id||record.intent.instrument!=order.instrument||record.intent.leg!=order.leg||
       record.intent.side!=order.side||record.rules_hash!=s.rules_hash||record.quantity_units!=s.identity.profile.quantity_units||
       hedge::quantity_atoms(record.intent.quantity).map_err(|_|bad())?!=order.original{return Err(bad())}
   }
  }
  let mut rebuilt=AdapterRegistry::default();
  for id in &self.adapter.event_order{
   let event=self.adapter.events.get(id).ok_or_else(bad)?;
   rebuilt.observe(event,self.generation,s.identity.profile.fee_bps.parse().map_err(|_|bad())?)?;
  }
  if serde_json::to_value(&rebuilt).ok()!=serde_json::to_value(&self.adapter).ok(){return Err(bad())}
  for (id,a) in &self.adapter.orders{
   let i=&s.intents.get(id).ok_or_else(bad)?.intent;
   if a.generation!=self.generation||a.order_id!=*id||a.instrument!=i.instrument||a.side!=i.side||a.leg!=i.leg||
      a.original!=hedge::quantity_atoms(i.quantity).map_err(|_|bad())?||
      a.venue!=if i.leg==risk::FillLeg::Spot{"OKX_SPOT"}else{"BINANCE_FUTURES"}{return Err(bad())}
  }
  Ok(())
 }
}
impl DurablePaper {
 pub fn adapter_registry(&self)->Option<&AdapterRegistry>{self.isolated.as_ref()?.full.as_ref().map(|f|&f.adapter)}
}

#[derive(Clone,Debug,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
struct ClosedProof {generation:u64,census_revision:u64,adapter_hash:String,rules_hash:String,execution_ids:BTreeSet<String>}
impl DurablePaper {
 pub fn close_isolated_generation(&mut self,c:&hedge::MarketContext)->Result<(),ExecutionError>{
  if self.fenced{return Err(ExecutionError::OwnerFenced)}
  let mut s=self.isolated.clone().ok_or(ExecutionError::Invalid("isolated owner"))?;
  if c.clock_domain!=s.identity.clock_domain||c.input_id.is_empty(){return Err(ExecutionError::Invalid("census clock domain"))}
  if self.lifecycle.state==risk::LifecycleState::Closed{return Ok(())}
  if c.now_ns<s.last_clock_ns||!self.exposure_view().known{return Err(ExecutionError::Invalid("census requires known state"))}
  let f=s.full.as_mut().ok_or(ExecutionError::Invalid("independent registry absent"))?;
  if f.request.is_none()||f.adapter.spot_base!=0||f.adapter.futures_base!=0{return Err(ExecutionError::Invalid("nonzero census exposure"))}
  f.adapter.census(&self.lifecycle,f.generation)?;
  f.closed=Some(ClosedProof{generation:f.generation,census_revision:f.adapter.revision,
    adapter_hash:hedge::digest(&f.adapter).map_err(|_|ExecutionError::Invalid("census hash"))?,
    rules_hash:s.rules_hash.clone(),execution_ids:self.lifecycle.processed_events.keys().cloned().collect()});
  let ids=f.adapter.orders.keys().cloned().collect();
  let scope=if self.universe.is_some(){format!("{}:{}",s.identity.run_id,s.identity.profile.base)}else{s.identity.run_id.clone()};
  let id=format!("closed:{}:{}:{}",scope,f.generation,f.adapter.revision);
  s.last_clock_ns=c.now_ns;
  let e=SimExecutionEvent{event_id:id,order_id:"owner".into(),kind:SimEventKind::OrderCensus,simulated:true,
    event_wall_ms:c.wall_ms,event_mono_ns:c.now_ns,quantity:None,price:None,
    reason:Some("independent adapter terminal census and zero inventory".into()),intent:None,census_order_ids:Some(ids)};
  self.commit_transition(e,Some(s),|l,_|{
   l.confirm_order_census_complete(c.now_ns)?;
   if l.state!=risk::LifecycleState::Closed{return Err(ExecutionError::Invalid("lifecycle refused Closed"))}Ok(())
  })
 }
}

#[derive(Clone,Debug,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
struct ClosedGeneration {
 generation:u64,lifecycle:risk::Lifecycle,adapter:AdapterRegistry,proof:ClosedProof,
 intents:BTreeMap<String,RegisteredIntent>,events:BTreeMap<String,SimExecutionEvent>,
}
impl DurablePaper {
 pub fn isolated_entry_key(&self)->String{format!("entry:{}",self.isolated.as_ref().and_then(|s|s.full.as_ref()).map(|f|f.generation).unwrap_or(1))}
 pub fn start_isolated_generation(&mut self,c:&hedge::MarketContext)->Result<(),ExecutionError>{
  if self.fenced{return Err(ExecutionError::OwnerFenced)}
  let mut s=self.isolated.clone().ok_or(ExecutionError::Invalid("isolated owner"))?;
  if c.clock_domain!=s.identity.clock_domain||c.input_id.is_empty(){return Err(ExecutionError::Invalid("generation command identity"))}
  let f=s.full.as_mut().ok_or(ExecutionError::Invalid("full lifecycle absent"))?;
  if f.generation_keys.contains_key(&c.input_id){return Ok(())}
  if self.lifecycle.state!=risk::LifecycleState::Closed||c.now_ns<=s.last_clock_ns||
   !s.identity.exit.as_ref().is_some_and(|r|r.allow_next_generation){return Err(ExecutionError::Invalid("generation admission requires Closed"))}
  f.adapter.census(&self.lifecycle,f.generation)?;
  let proof=f.closed.clone().ok_or(ExecutionError::Invalid("Closed proof absent"))?;
  f.archives.push(ClosedGeneration{generation:f.generation,lifecycle:self.lifecycle.clone(),adapter:f.adapter.clone(),
    proof,intents:s.intents.clone(),events:s.events.clone()});
  f.generation=f.generation.checked_add(1).ok_or(ExecutionError::Invalid("generation overflow"))?;
  f.generation_keys.insert(c.input_id.clone(),f.generation);
  f.adapter=AdapterRegistry::default();f.request=None;f.closed=None;
  s.intents.clear();s.keys.clear();s.events.clear();s.leg_sequences.clear();
  // Keep consumed market identities across generations: old liquidity cannot be reused.
  s.last_clock_ns=c.now_ns;
  let e=SimExecutionEvent{event_id:format!("new-generation:{}:{}",f.generation,c.input_id),order_id:"owner".into(),
   kind:SimEventKind::Recovery,simulated:true,event_wall_ms:c.wall_ms,event_mono_ns:c.now_ns,
   quantity:None,price:None,reason:Some("explicit generation after confirmed Closed".into()),intent:None,census_order_ids:None};
  let config=self.lifecycle.config.clone();
  self.commit_transition(e,Some(s),|l,_|{*l=risk::Lifecycle::new(config)?;l.order_census_complete=false;Ok(())})
 }
}

impl DurablePaper {
 fn historical_late(&mut self,event:&SimExecutionEvent,c:&hedge::MarketContext)->Result<(),ExecutionError>{
  if self.fenced{return Err(ExecutionError::OwnerFenced)}
  let mut s=self.isolated.clone().ok_or(ExecutionError::Invalid("isolated owner"))?;
  let f=s.full.as_mut().ok_or(ExecutionError::Invalid("full lifecycle"))?;
  if c.clock_domain!=s.identity.clock_domain||!event.simulated||c.now_ns<s.last_clock_ns||
   !matches!(event.kind,SimEventKind::PartialFill|SimEventKind::FullFill)||
   event.quantity.map_or(true,|q|!q.is_finite()||q<=0.)||event.price.map_or(true,|p|!p.is_finite()||p<=0.){
    return Err(ExecutionError::Invalid("historical late identity"))
  }
  let Some(archive)=f.archives.iter().find(|a|a.intents.contains_key(&event.order_id)) else{return self.freeze_late_evidence(event,c)};
  if let Some(old)=archive.events.get(&event.event_id){
   return if serde_json::to_value(old).ok()==serde_json::to_value(event).ok(){Ok(())}else{self.freeze_late_evidence(event,c)}
  }
  if let Some(old)=f.historical_late.get(&event.event_id){
   return if serde_json::to_value(old).ok()==serde_json::to_value(event).ok(){Ok(())}else{self.freeze_late_evidence(event,c)}
  }
  f.historical_late.insert(event.event_id.clone(),event.clone());s.last_clock_ns=c.now_ns;
  let incident=SimExecutionEvent{event_id:format!("historical-late:{}",event.event_id),order_id:"owner".into(),
   kind:SimEventKind::Recovery,simulated:true,event_wall_ms:c.wall_ms,event_mono_ns:c.now_ns,quantity:None,price:None,
   reason:Some("prior generation late fill; current risk frozen".into()),intent:None,census_order_ids:None};
  self.commit_transition(incident,Some(s),|l,_|{l.isolated_recovery_incident(c.now_ns,"historical late execution");Ok(())})
 }
}

#[cfg(test)]
#[path="native_paper_execution_r1298_tests.rs"]
mod r1298_tests;

impl DurablePaper {
 fn freeze_late_evidence(&mut self,event:&SimExecutionEvent,c:&hedge::MarketContext)->Result<(),ExecutionError>{
  if self.fenced{return Err(ExecutionError::OwnerFenced)}
  let mut s=self.isolated.clone().ok_or(ExecutionError::Invalid("isolated owner"))?;
  if !event.simulated||c.clock_domain!=s.identity.clock_domain||c.now_ns<s.last_clock_ns{return Err(ExecutionError::Invalid("late incident context"))}
  let key=hedge::digest(event).map_err(|_|ExecutionError::Invalid("late incident identity"))?;
  let f=s.full.as_mut().ok_or(ExecutionError::Invalid("full lifecycle"))?;
  if f.historical_late.contains_key(&key){return Ok(())}
  f.historical_late.insert(key.clone(),event.clone());s.last_clock_ns=c.now_ns;
  let incident=SimExecutionEvent{event_id:format!("ambiguous-late:{key}"),order_id:"owner".into(),kind:SimEventKind::Recovery,
   simulated:true,event_wall_ms:c.wall_ms,event_mono_ns:c.now_ns,quantity:None,price:None,
   reason:Some("unresolved late execution identity; no new generation".into()),intent:None,census_order_ids:None};
  self.commit_transition(incident,Some(s),|l,_|{l.isolated_recovery_incident(c.now_ns,"ambiguous late fill");Ok(())})
 }
 pub fn isolated_generation_summary(&self)->Option<serde_json::Value>{
  let f=self.isolated.as_ref()?.full.as_ref()?;
  Some(serde_json::json!({"generation":f.generation,"census_revision":f.adapter.revision,
   "closed_proof":f.closed,"archived_generations":f.archives.iter().map(|a|serde_json::json!({
    "generation":a.generation,"projection":PortfolioProjection::derive(&a.lifecycle,None),"closed_proof":a.proof})).collect::<Vec<_>>(),
   "unresolved_historical_late":f.historical_late.len()}))
 }
}
