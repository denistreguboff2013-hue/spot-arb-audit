// Independent event reducer. It never initializes from Lifecycle or Projection.
use std::collections::BTreeSet;
#[derive(Clone,Debug,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdapterOrder {
 pub order_id:String,pub generation:u64,pub venue:String,pub instrument:String,
 pub side:risk::FillSide,pub leg:risk::FillLeg,
 pub original:u128,pub filled:u128,pub remaining:u128,
 pub status:risk::OrderStatus,pub execution_ids:BTreeSet<String>,
 pub cancel_requested:bool,pub cancel_acknowledged:bool,pub terminal_certain:bool,
}
#[derive(Clone,Debug,Default,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdapterRegistry {
 pub event_order:Vec<String>,pub revision:u64,pub orders:BTreeMap<String,AdapterOrder>,
 pub events:BTreeMap<String,SimExecutionEvent>,pub unresolved_late:BTreeSet<String>,
 pub spot_base:u128,pub futures_base:u128,pub fees_quote:f64,
}
impl AdapterRegistry {
 fn observe(&mut self,e:&SimExecutionEvent,generation:u64,fee_bps:f64)->Result<(),ExecutionError>{
  let invalid=||ExecutionError::Invalid("adapter event inconsistency");
  if matches!(e.kind,SimEventKind::Genesis|SimEventKind::Recovery|SimEventKind::OrderCensus){return Ok(())}
  if let Some(old)=self.events.get(&e.event_id){
   return if serde_json::to_value(old).ok()==serde_json::to_value(e).ok(){Ok(())}else{Err(invalid())}
  }
  if !e.simulated{return Err(invalid())}
  if e.kind==SimEventKind::Accepted{
   let i=e.intent.as_ref().ok_or_else(invalid)?;
   if self.orders.contains_key(&e.order_id)||i.order_id!=e.order_id{return Err(invalid())}
   let q=hedge::quantity_atoms(i.quantity).map_err(|_|invalid())?;
   if q==0{return Err(invalid())}
   self.orders.insert(e.order_id.clone(),AdapterOrder{order_id:e.order_id.clone(),generation,
    venue:if i.leg==risk::FillLeg::Spot{"OKX_SPOT".into()}else{"BINANCE_FUTURES".into()},
    instrument:i.instrument.clone(),side:i.side,leg:i.leg,original:q,filled:0,remaining:q,
    status:risk::OrderStatus::Pending,execution_ids:BTreeSet::new(),cancel_requested:false,
    cancel_acknowledged:false,terminal_certain:false});
  }else{
   let o=self.orders.get_mut(&e.order_id).ok_or_else(invalid)?;
   match e.kind {
    SimEventKind::PartialFill|SimEventKind::FullFill=>{
     if o.terminal_certain||o.status==risk::OrderStatus::Unknown{return Err(invalid())}
     let q=hedge::quantity_atoms(e.quantity.ok_or_else(invalid)?).map_err(|_|invalid())?;
     let p=e.price.ok_or_else(invalid)?;
     if q==0||q>o.remaining||!p.is_finite()||p<=0.0||!o.execution_ids.insert(e.event_id.clone()){return Err(invalid())}
     o.filled=o.filled.checked_add(q).ok_or_else(invalid)?;o.remaining=o.original.checked_sub(o.filled).ok_or_else(invalid)?;
     if (e.kind==SimEventKind::FullFill)!=(o.remaining==0){return Err(invalid())}
     o.status=if o.remaining==0{risk::OrderStatus::Filled}else if o.cancel_requested&&!o.cancel_acknowledged{
      risk::OrderStatus::CancelPending}else{risk::OrderStatus::PartiallyFilled};
     o.terminal_certain=o.remaining==0&&!o.cancel_requested;
     // A fill does not acknowledge an outstanding cancellation.
     match (o.leg,o.side){
      (risk::FillLeg::Spot,risk::FillSide::Buy)=>self.spot_base=self.spot_base.checked_add(q).ok_or_else(invalid)?,
      (risk::FillLeg::Spot,risk::FillSide::Sell)=>self.spot_base=self.spot_base.checked_sub(q).ok_or_else(invalid)?,
      (risk::FillLeg::Futures,risk::FillSide::Short)=>self.futures_base=self.futures_base.checked_add(q).ok_or_else(invalid)?,
      (risk::FillLeg::Futures,risk::FillSide::Cover)=>self.futures_base=self.futures_base.checked_sub(q).ok_or_else(invalid)?,
      _=>return Err(invalid()),
     }
     self.fees_quote+=e.quantity.unwrap()*p*fee_bps/10_000.;
    },
    SimEventKind::CancelRequested=>{
     if o.terminal_certain{return Err(invalid())}o.cancel_requested=true;o.status=risk::OrderStatus::CancelPending;
    },
    SimEventKind::Cancelled=>{
     if !o.cancel_requested{return Err(invalid())}
     o.cancel_acknowledged=true;o.status=risk::OrderStatus::Cancelled;o.terminal_certain=true;
    },
    SimEventKind::Rejected=>{o.status=risk::OrderStatus::Rejected;o.terminal_certain=!o.cancel_requested;},
    SimEventKind::Unknown|SimEventKind::Timeout=>{
     o.status=risk::OrderStatus::Unknown;o.terminal_certain=false;
     if e.reason.as_ref().is_some_and(|r|r.contains("LateFill")){self.unresolved_late.insert(e.event_id.clone());}
    },
    _=>return Err(invalid()),
   }
  }
  self.events.insert(e.event_id.clone(),e.clone());self.event_order.push(e.event_id.clone());
  self.revision=self.revision.checked_add(1).ok_or_else(invalid)?;
  Ok(())
 }
 pub fn census(&self,l:&risk::Lifecycle,generation:u64)->Result<(),ExecutionError>{
  let invalid=||ExecutionError::Invalid("independent adapter census incomplete");
  if !self.unresolved_late.is_empty()||self.orders.is_empty()||self.orders.len()!=l.orders.len(){return Err(invalid())}
  let mut ids=BTreeSet::new();
  for (id,o) in &self.orders {
   let target=l.orders.get(id).ok_or_else(invalid)?;
   let terminal=matches!(o.status,risk::OrderStatus::Filled|risk::OrderStatus::Cancelled|risk::OrderStatus::Rejected);
   if o.generation!=generation||o.order_id!=*id||!terminal||!o.terminal_certain||
    (o.cancel_requested&&!o.cancel_acknowledged)||target.uncertain||target.status!=o.status||
    target.side!=o.side||target.leg!=o.leg||
    hedge::quantity_atoms(target.original_quantity).map_err(|_|invalid())?!=o.original||
    hedge::quantity_atoms(target.confirmed_filled_quantity).map_err(|_|invalid())?!=o.filled||
    hedge::quantity_atoms(target.remaining_quantity).map_err(|_|invalid())?!=o.remaining||
    o.original.checked_sub(o.filled)!=Some(o.remaining){return Err(invalid())}
   let expected:BTreeSet<_>=target.execution_event_ids.iter().cloned().collect();
   if expected.len()!=target.execution_event_ids.len()||expected!=o.execution_ids{return Err(invalid())}
   for execution_id in &o.execution_ids {
    if !ids.insert(execution_id.clone()){return Err(invalid())}
    let e=self.events.get(execution_id).ok_or_else(invalid)?;
    let f=l.processed_events.get(execution_id).ok_or_else(invalid)?;
    if e.order_id!=*id||f.order_id!=*id||e.quantity!=Some(f.quantity)||e.price!=Some(f.price)||
      f.leg!=o.leg||f.side!=o.side||f.contract_multiplier!=1.0||!f.simulated{return Err(invalid())}
   }
  }
  if ids!=l.processed_events.keys().cloned().collect(){return Err(invalid())}
  let ledger=l.ledger.as_ref().ok_or_else(invalid)?;
  if ledger.unknown_execution_status||ledger.valuation_unknown||
   hedge::quantity_atoms(ledger.spot_exposure_base()).map_err(|_|invalid())?!=self.spot_base||
   hedge::quantity_atoms(ledger.futures_short_exposure_base(l.config.futures_contract_multiplier)).map_err(|_|invalid())?!=self.futures_base||
   !self.fees_quote.is_finite()||self.fees_quote.to_bits()!=ledger.fees_quote.to_bits(){return Err(invalid())}
  Ok(())
 }
}
