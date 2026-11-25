// Included in R1289: one owner and one transaction log, not a second writer.
use crate::native_paper_hedge_r1296 as hedge;
use std::collections::BTreeMap;

#[derive(Clone,Debug,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegisteredIntent {
    pub intent:OrderIntent,
    pub command_key:String,
    pub rules_hash:String,
    pub quantity_units:String,
    pub witness:serde_json::Value,
}
#[derive(Clone,Debug,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
struct IsolatedState {
    identity:hedge::Identity,
    #[serde(default,skip_serializing_if="Option::is_none")]
    full:Option<ExitSession>,
    rules_hash:String,
    intents:BTreeMap<String,RegisteredIntent>,
    keys:BTreeMap<String,String>,
    events:BTreeMap<String,SimExecutionEvent>,
    consumed:BTreeMap<String,String>,
    leg_sequences:BTreeMap<risk::FillLeg,u64>,
    next_order:u64,
    last_clock_ns:u64,
}
impl IsolatedState {
    fn new(identity:hedge::Identity)->Result<Self,ExecutionError>{
        let rules_hash=identity.hash().map_err(|_|ExecutionError::Invalid("isolated identity"))?;
        let full=identity.exit.as_ref().map(|_|ExitSession::new());
        Ok(Self{identity,full,rules_hash,intents:BTreeMap::new(),keys:BTreeMap::new(),
          events:BTreeMap::new(),consumed:BTreeMap::new(),leg_sequences:BTreeMap::new(),
          next_order:0,last_clock_ns:0})
    }
}
impl DurablePaper {
    pub fn create_isolated(path:impl AsRef<Path>,identity:hedge::Identity)->Result<Self,ExecutionError>{
        let config=identity.profile.risk_config().map_err(|_|ExecutionError::Invalid("isolated profile"))?;
        Self::create_inner(path,risk::Lifecycle::new(config)?,Some(IsolatedState::new(identity)?))
    }
    pub fn isolated_identity(&self)->Option<&hedge::Identity>{self.isolated.as_ref().map(|s|&s.identity)}
    pub fn registered_intents(&self)->Result<&BTreeMap<String,RegisteredIntent>,ExecutionError>{
        Ok(&self.isolated.as_ref().ok_or(ExecutionError::Invalid("unbound owner"))?.intents)
    }
    pub fn isolated_events(&self)->Result<&BTreeMap<String,SimExecutionEvent>,ExecutionError>{
        Ok(&self.isolated.as_ref().ok_or(ExecutionError::Invalid("unbound owner"))?.events)
    }
    pub fn exposure_view(&self)->hedge::ExposureView{hedge::exposure(&self.lifecycle,self.sequence)}
    pub fn ensure_isolated_intent(&mut self,expected_revision:u64,key:String,
        leg:risk::FillLeg,quantity:f64,spot:&Book,futures:&Book,c:&hedge::MarketContext
    )->Result<RegisteredIntent,ExecutionError>{
        if self.fenced{return Err(ExecutionError::OwnerFenced)}
        let mut state=self.isolated.clone().ok_or(ExecutionError::Invalid("unbound owner"))?;
        if let Some(id)=state.keys.get(&key) {
            let old=state.intents.get(id).ok_or(ExecutionError::CorruptJournal)?;
            if old.intent.leg!=leg || old.intent.quantity.to_bits()!=quantity.to_bits(){
                return Err(ExecutionError::Invalid("idempotency collision"))
            }
            return Ok(old.clone())
        }
        if expected_revision!=self.sequence{return Err(ExecutionError::Invalid("stale owner revision"))}
        hedge::validate_pair(&state.identity,spot,futures,c).map_err(|_|ExecutionError::Invalid("market/profile denied"))?;
        if c.now_ns<state.last_clock_ns{return Err(ExecutionError::Invalid("clock regression"))}
        let v=self.exposure_view();
        if !v.known || matches!(v.state,risk::LifecycleState::RecoveryRequired|risk::LifecycleState::Closed|risk::LifecycleState::FailedClosed){
            return Err(ExecutionError::Invalid("recovery/terminal owner"))
        }
        if self.lifecycle.orders.values().any(|o|o.leg==leg &&
          (!matches!(o.status,risk::OrderStatus::Filled|risk::OrderStatus::Cancelled|risk::OrderStatus::Rejected)||o.uncertain)){
            return Err(ExecutionError::Invalid("pending leg obligation"))
        }
        let side=match leg{risk::FillLeg::Spot=>risk::FillSide::Buy,risk::FillLeg::Futures=>risk::FillSide::Short};
        if leg==risk::FillLeg::Spot {
            if v.state!=risk::LifecycleState::Flat || key!=self.isolated_entry_key(){return Err(ExecutionError::Invalid("only initial spot entry"))}
        } else {
            match hedge::plan_hedge(&state.identity,&self.lifecycle,self.sequence,spot,futures,c){
                hedge::HedgeDecision::Short{quantity:q,key:k} if q.to_bits()==quantity.to_bits()&&k==key=>{},
                _=>return Err(ExecutionError::Invalid("hedge proposal mismatch"))
            }
        }
        let price=if leg==risk::FillLeg::Spot{spot.asks[0].0}else{futures.bids.last().ok_or(ExecutionError::Invalid("depth"))?.0};
        state.identity.profile.validate_quantity(quantity,price,leg==risk::FillLeg::Spot)
            .map_err(|_|ExecutionError::Invalid("quantity rules"))?;
        state.next_order=state.next_order.checked_add(1).ok_or(ExecutionError::Invalid("order id overflow"))?;
        let scope=if self.universe.is_some(){format!("{}:{}",state.identity.run_id,state.identity.profile.base)}else{state.identity.run_id.clone()};
        let order_id=format!("{}:{}:{}",scope,if leg==risk::FillLeg::Spot{"spot"}else{"hedge"},state.next_order);
        let intent=OrderIntent{intent_id:format!("intent:{order_id}"),order_id:order_id.clone(),leg,side,quantity,
            instrument:if leg==risk::FillLeg::Spot{state.identity.profile.spot_symbol.clone()}else{state.identity.profile.futures_symbol.clone()},
            created_wall_ms:c.wall_ms,created_mono_ns:c.now_ns,exit_reason:None};
        let record=RegisteredIntent{intent:intent.clone(),command_key:key.clone(),rules_hash:state.rules_hash.clone(),
            quantity_units:state.identity.profile.quantity_units.clone(),
            witness:serde_json::json!({"context":c,"spot":spot,"futures":futures,"owner_revision":expected_revision})};
        state.keys.insert(key,order_id.clone());state.intents.insert(order_id.clone(),record.clone());
        state.last_clock_ns=c.now_ns;
        let event=SimExecutionEvent{event_id:format!("{order_id}:accepted"),order_id:order_id.clone(),
            kind:SimEventKind::Accepted,simulated:true,event_wall_ms:c.wall_ms,event_mono_ns:c.now_ns,
            quantity:None,price:None,reason:Some("R1296 registered policy intent".into()),intent:Some(intent),census_order_ids:None};
        let gate=risk::MarketGate{spot_hot:c.spot_hot,futures_hot:c.futures_hot,spot_book_valid:true,futures_book_valid:true};
        self.commit_transition(event,Some(state),|l,e|{
            if leg==risk::FillLeg::Spot{l.begin_entry(spot,futures,&gate,quantity,c.now_ns,c.wall_ms)?;}
            else{
                match l.authorize_hedge(spot,futures,&gate,c.now_ns,c.wall_ms)?{
                    risk::LifecycleAction::SimulatedHedgeIntent{futures_short_contracts,..} if quantity<=futures_short_contracts+1e-10=>{},
                    _=>return Err(ExecutionError::Invalid("lifecycle hedge authorization"))
                }
            }
            l.register_simulated_order(&order_id,leg,side,quantity,c.now_ns)?;
            l.apply_order_event(risk::OrderEvent{event_id:e.event_id.clone(),order_id:order_id.clone(),
                kind:risk::OrderEventKind::Accepted,event_ns:c.now_ns,simulated:true})?;
            Ok(())
        })?;
        Ok(record)
    }
}

impl DurablePaper {
    /// Execution is derived from a validated book, never from the decision itself.
    /// Each leg consumes a snapshot at most once across all its orders.
    pub fn simulate_isolated_order(&mut self,order_id:&str,spot:&Book,futures:&Book,
      c:&hedge::MarketContext)->Result<Option<SimExecutionEvent>,ExecutionError>{
        if self.fenced{return Err(ExecutionError::OwnerFenced)}
        let mut state=self.isolated.clone().ok_or(ExecutionError::Invalid("isolated owner required"))?;
        hedge::validate_pair(&state.identity,spot,futures,c).map_err(|_|ExecutionError::Invalid("execution market/profile"))?;
        if c.now_ns<state.last_clock_ns{return Err(ExecutionError::Invalid("clock regression"))}
        let r=state.intents.get(order_id).cloned().ok_or(ExecutionError::Invalid("unregistered fill"))?;
        let o=self.lifecycle.orders.get(order_id).ok_or(ExecutionError::Invalid("missing lifecycle order"))?;
        if o.remaining_quantity<=0.0{return Ok(None)}
        // A terminal or uncertain order cannot be silently simulated again.
        if !self.exposure_view().known || o.uncertain ||
          matches!(o.status,risk::OrderStatus::Filled|risk::OrderStatus::Cancelled|risk::OrderStatus::Rejected){
            return Err(ExecutionError::Invalid("uncertain/terminal execution requires reconciliation"))
        }
        let book=if r.intent.leg==risk::FillLeg::Spot{spot}else{futures};
        if book.received_ns<=r.intent.created_mono_ns{return Ok(None)}
        let sequence_id=book.sequence.ok_or(ExecutionError::Invalid("book sequence missing"))?;
        let source=hedge::digest(&(r.intent.leg,book.epoch,sequence_id)).map_err(|_|ExecutionError::Invalid("book identity"))?;
        let depth_hash=hedge::digest(&(&book.bids,&book.asks)).map_err(|_|ExecutionError::Invalid("depth identity"))?;
        if let Some(old)=state.consumed.get(&source){
            return if old==&depth_hash{Ok(None)}else{Err(ExecutionError::Invalid("book sequence collision"))}
        }
        let sequence=state.leg_sequences.get(&r.intent.leg).copied().unwrap_or(0).checked_add(1)
          .ok_or(ExecutionError::Invalid("execution sequence overflow"))?;
        let mut events=simulate_marketable(&r.intent,o.remaining_quantity,sequence,book,true,c.now_ns,c.wall_ms)?;
        let event=events.pop().ok_or(ExecutionError::Invalid("missing simulated result"))?;
        if !matches!(event.kind,SimEventKind::PartialFill|SimEventKind::FullFill){
            return Err(ExecutionError::Invalid("unsupported simulation result"))
        }
        state.consumed.insert(source,depth_hash);
        state.leg_sequences.insert(r.intent.leg,sequence);
        state.events.insert(event.event_id.clone(),event.clone());
        state.last_clock_ns=c.now_ns;
        self.commit_transition(event.clone(),Some(state),|l,e|{
            // The registered wrapper is mandatory; R1287's legacy auto-registration is unreachable.
            if !l.orders.contains_key(&e.order_id){return Err(ExecutionError::Invalid("unregistered execution"))}
            l.apply_fill(risk::FillEvent{event_id:e.event_id.clone(),order_id:e.order_id.clone(),
                leg:r.intent.leg,side:r.intent.side,quantity:e.quantity.ok_or(ExecutionError::Invalid("quantity"))?,
                price:e.price.ok_or(ExecutionError::Invalid("price"))?,
                contract_multiplier:1.0,sequence,event_ns:e.event_mono_ns,
                status:if e.kind==SimEventKind::FullFill{risk::FillStatus::Complete}else{risk::FillStatus::Partial},simulated:true})?;
            Ok(())
        })?;
        Ok(Some(event))
    }
    /// Exact retries return the durable event without changing quantities or the log.
    pub fn retry_isolated_execution(&self,event:&SimExecutionEvent)->Result<(),ExecutionError>{
        if self.fenced{return Err(ExecutionError::OwnerFenced)}
        let s=self.isolated.as_ref().ok_or(ExecutionError::Invalid("isolated owner required"))?;
        let old=s.events.get(&event.event_id).ok_or(ExecutionError::Invalid("unknown execution"))?;
        if serde_json::to_value(old).ok()!=serde_json::to_value(event).ok(){
            return Err(ExecutionError::Invalid("execution identity collision"))
        }
        Ok(())
    }
}

#[derive(Clone,Debug,Serialize,Deserialize)]
#[serde(tag="type",deny_unknown_fields)]
pub enum IsolatedControl {
    Clock,
    CancelRequested{order_id:String},
    Cancelled{order_id:String},
    Rejected{order_id:String},
    Timeout{order_id:String},
    HotLoss,
    StaleMarket,
    RecoveryIncident{reason:String},
    /// Preserved evidence, not an inferred fill. Reconciliation stays mandatory.
    LateFill{event:SimExecutionEvent},
}
impl DurablePaper {
    pub fn control_isolated(&mut self,control:IsolatedControl,c:&hedge::MarketContext)->Result<(),ExecutionError>{
        if let IsolatedControl::LateFill{event}=&control{
            if self.isolated.as_ref().is_some_and(|s|s.full.is_some()&&s.events.get(&event.event_id).is_some_and(|old|
                serde_json::to_value(old).ok()!=serde_json::to_value(event).ok())){
                return self.freeze_late_evidence(event,c)
            }
            if self.isolated.as_ref().is_some_and(|s|s.full.is_some()&&!s.intents.contains_key(&event.order_id)){
                return self.historical_late(event,c)
            }
        }
        let preserve_closed=self.isolated.as_ref().is_some_and(|s|s.full.is_some())&&self.lifecycle.state==risk::LifecycleState::Closed;
        let exit_expired=self.isolated.as_ref().and_then(|s|s.full.as_ref()).and_then(|f|f.request.as_ref()).is_some_and(|r|c.now_ns>r.deadline_ns);
        if self.fenced{return Err(ExecutionError::OwnerFenced)}
        let mut s=self.isolated.clone().ok_or(ExecutionError::Invalid("isolated owner required"))?;
        if c.clock_domain!=s.identity.clock_domain||c.input_id.is_empty(){return Err(ExecutionError::Invalid("control identity"))}
        let id=format!("control:{}",c.input_id);
        let fingerprint=hedge::digest(&(&control,c)).map_err(|_|ExecutionError::Invalid("control serialization"))?;
        if let Some(old)=s.consumed.get(&id){
            return if old==&fingerprint{Ok(())}else{Err(ExecutionError::Invalid("control ID collision"))}
        }
        if c.now_ns<s.last_clock_ns{return Err(ExecutionError::Invalid("control clock regression"))}
        let (order_id,kind,okind)=match &control{
            IsolatedControl::CancelRequested{order_id}=>(order_id.clone(),SimEventKind::CancelRequested,Some(risk::OrderEventKind::CancelRequested)),
            IsolatedControl::Cancelled{order_id}=>(order_id.clone(),SimEventKind::Cancelled,Some(risk::OrderEventKind::Cancelled)),
            IsolatedControl::Rejected{order_id}=>(order_id.clone(),SimEventKind::Rejected,Some(risk::OrderEventKind::Rejected)),
            IsolatedControl::Timeout{order_id}=>(order_id.clone(),SimEventKind::Timeout,Some(risk::OrderEventKind::Unknown)),
            IsolatedControl::LateFill{event}=>(event.order_id.clone(),SimEventKind::Unknown,Some(risk::OrderEventKind::Unknown)),
            _=>("owner".into(),SimEventKind::Recovery,None),
        };
        if okind.is_some()&&!s.intents.contains_key(&order_id){return Err(ExecutionError::Invalid("unregistered control"))}
        if let IsolatedControl::Cancelled{..}=&control {
            let adapter_pending=s.full.as_ref().and_then(|f|f.adapter.orders.get(&order_id)).is_some_and(|o|o.cancel_requested&&!o.cancel_acknowledged);
            if !adapter_pending&&self.lifecycle.orders.get(&order_id).map(|o|o.status)!=Some(risk::OrderStatus::CancelPending){
                return Err(ExecutionError::Invalid("cancel acknowledgement without pending request"))
            }
        }
        if let IsolatedControl::LateFill{event}=&control {
            if !event.simulated||event.event_id.is_empty()||!matches!(event.kind,SimEventKind::PartialFill|SimEventKind::FullFill)
              ||event.quantity.map_or(true,|q|!q.is_finite()||q<=0.0)
              ||event.price.map_or(true,|p|!p.is_finite()||p<=0.0){
                return Err(ExecutionError::Invalid("invalid late evidence"))
            }
            if let Some(old)=s.events.get(&event.event_id){
                return if serde_json::to_value(old).ok()==serde_json::to_value(event).ok(){Ok(())}
                else{Err(ExecutionError::Invalid("late event collision"))}
            }
            s.events.insert(event.event_id.clone(),event.clone());
        }
        s.consumed.insert(id.clone(),fingerprint);s.last_clock_ns=c.now_ns;
        let event=SimExecutionEvent{event_id:id,order_id:order_id.clone(),kind,simulated:true,
            event_wall_ms:c.wall_ms,event_mono_ns:c.now_ns,quantity:None,price:None,
            reason:Some(serde_json::to_string(&control).map_err(|_|ExecutionError::Invalid("control encode"))?),
            intent:None,census_order_ids:None};
        self.commit_transition(event,Some(s),|l,e|{
            if let Some(k)=okind {
                l.apply_order_event(risk::OrderEvent{event_id:e.event_id.clone(),order_id:order_id.clone(),
                    kind:k,event_ns:c.now_ns,simulated:true})?;
            }
            match control {
                IsolatedControl::Clock=>{
                    if !preserve_closed {if exit_expired{l.isolated_recovery_incident(c.now_ns,"exit deadline");}else{l.on_clock(c.now_ns,&risk::MarketGate{spot_hot:c.spot_hot,futures_hot:c.futures_hot,
                        spot_book_valid:true,futures_book_valid:true});}}
                },
                IsolatedControl::CancelRequested{..}=>{},
                _=>l.isolated_recovery_incident(c.now_ns,"R1296 control requires reconciliation"),
            }
            Ok(())
        })
    }
}

impl IsolatedState {
    fn validate(&self,l:&risk::Lifecycle)->Result<(),ExecutionError>{
        let fail=||ExecutionError::CorruptJournal;
        if self.full.is_some()!=self.identity.exit.is_some(){return Err(fail())}
        if let Some(full)=&self.full{full.validate_registry(self,l)?;}
        if self.identity.hash().map_err(|_|fail())?!=self.rules_hash ||
          serde_json::to_value(&l.config).ok()!=serde_json::to_value(self.identity.profile.risk_config().map_err(|_|fail())?).ok() ||
          self.next_order!=self.intents.len() as u64+self.full.as_ref().map(|f|f.archives.iter().map(|a|a.intents.len() as u64).sum::<u64>()).unwrap_or(0) || self.keys.len()!=self.intents.len() ||
          l.orders.len()!=self.intents.len(){return Err(fail())}
        for (id,r) in &self.intents{
            let o=l.orders.get(id).ok_or_else(fail)?;
            let symbol=if r.intent.leg==risk::FillLeg::Spot{&self.identity.profile.spot_symbol}else{&self.identity.profile.futures_symbol};
            let side=match (r.intent.leg,r.intent.side){
                (risk::FillLeg::Spot,risk::FillSide::Buy)|(risk::FillLeg::Futures,risk::FillSide::Short)=>r.intent.side,
                (risk::FillLeg::Spot,risk::FillSide::Sell)|(risk::FillLeg::Futures,risk::FillSide::Cover)
                    if self.full.is_some()&&r.witness["reduce_only"]==true=>r.intent.side,
                _=>return Err(fail()),
            };
            if id!=&r.intent.order_id||r.rules_hash!=self.rules_hash||r.quantity_units!=self.identity.profile.quantity_units||
              &r.intent.instrument!=symbol||r.intent.side!=side||o.leg!=r.intent.leg||o.side!=side||
              o.original_quantity.to_bits()!=r.intent.quantity.to_bits()||self.keys.get(&r.command_key)!=Some(id)||
              !id.starts_with(&format!("{}:",self.identity.run_id)){return Err(fail())}
        }
        for (id,e) in &l.processed_events{
            let recorded=self.events.get(id).ok_or_else(fail)?;
            if recorded.order_id!=e.order_id||recorded.quantity!=Some(e.quantity)||recorded.price!=Some(e.price)||
              recorded.event_mono_ns!=e.event_ns||!self.intents.contains_key(&e.order_id)||
              self.leg_sequences.get(&e.leg).copied().unwrap_or(0)<e.sequence{return Err(fail())}
        }
        for e in self.events.values(){
            if !e.simulated||!self.intents.contains_key(&e.order_id){return Err(fail())}
        }
        Ok(())
    }
}
impl DurablePaper {
    pub fn recover_isolated(path:impl AsRef<Path>,identity:&hedge::Identity)->Result<Self,ExecutionError>{
        identity.hash().map_err(|_|ExecutionError::Invalid("manifest identity"))?;
        Self::recover_inner(path,Some(identity))
    }
}

#[cfg(test)]
#[path="native_paper_execution_r1296_tests.rs"]
mod r1296_tests;
