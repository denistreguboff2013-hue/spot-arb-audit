// Included in R1289: instrument slots belong to its ONE lock, sequence and journal.
#[derive(Clone,Debug,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecutionSlot1324a { lifecycle:risk::Lifecycle, isolated:IsolatedState }
#[derive(Clone,Debug,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
struct UniverseState1324a { generation:u64, slots:BTreeMap<String,ExecutionSlot1324a> }
impl UniverseState1324a {
 fn validate(&self)->Result<(),ExecutionError>{
  if self.generation==0||self.slots.is_empty(){return Err(ExecutionError::Invalid("R1324A_EMPTY_UNIVERSE"))}
  let mut run=None;
  for (base,s) in &self.slots {
   s.isolated.validate(&s.lifecycle)?;
   if base!=&s.isolated.identity.profile.base{return Err(ExecutionError::CorruptJournal)}
   let scope=(&s.isolated.identity.run_id,&s.isolated.identity.clock_domain);
   if run.is_some_and(|r|r!=scope){return Err(ExecutionError::Invalid("R1324A_MIXED_RUN"))}run=Some(scope);
  }Ok(())
 }
}
impl DurablePaper {
 pub fn create_universe_r1324a(path:impl AsRef<Path>,identities:&BTreeMap<String,hedge::Identity>,generation:u64)->Result<Self,ExecutionError>{
  let mut slots=BTreeMap::new();
  for (base,id) in identities {
   let lifecycle=risk::Lifecycle::new(id.profile.risk_config().map_err(|_|ExecutionError::Invalid("profile"))?)?;
   slots.insert(base.clone(),ExecutionSlot1324a{lifecycle,isolated:IsolatedState::new(id.clone())?});
  }
  let universe=UniverseState1324a{generation,slots};universe.validate()?;
  let first=identities.values().next().ok_or(ExecutionError::Invalid("empty universe"))?.clone();
  let mut owner=Self::create_isolated(path,first)?;owner.universe=Some(universe);
  owner.universe_checkpoint_r1324a("universe-initialized")?;Ok(owner)
 }
 fn universe_checkpoint_r1324a(&mut self,reason:&str)->Result<(),ExecutionError>{
  let e=SimExecutionEvent{event_id:format!("r1324a-universe:{}",self.sequence+1),order_id:"owner".into(),kind:SimEventKind::Recovery,simulated:true,event_wall_ms:0,event_mono_ns:0,quantity:None,price:None,reason:Some(reason.into()),intent:None,census_order_ids:None};
  self.commit_event(e,|_,_|Ok(()))
 }
 pub fn recover_universe_r1324a(path:impl AsRef<Path>,identities:&BTreeMap<String,hedge::Identity>)->Result<Self,ExecutionError>{
  let mut owner=Self::recover_mode_r1324a(path,None,true)?;
  let u=owner.universe.as_ref().ok_or(ExecutionError::Invalid("R1324A_UNIVERSE_GENESIS_INCOMPLETE"))?;
  if u.slots.len()!=identities.len()||u.slots.iter().any(|(b,s)|identities.get(b)!=Some(&s.isolated.identity)){return Err(ExecutionError::Invalid("R1324A_UNIVERSE_IDENTITY_MISMATCH"))}
  owner.cold_restart_universe_r1324a()?;Ok(owner)
 }
 fn cold_restart_universe_r1324a(&mut self)->Result<(),ExecutionError>{
  let u=self.universe.as_ref().ok_or(ExecutionError::Invalid("R1324A_UNIVERSE_GENESIS_INCOMPLETE"))?;
  let dirty:Vec<String>=u.slots.iter().filter(|(_,s)|!matches!(s.lifecycle.state,risk::LifecycleState::Flat|risk::LifecycleState::Closed|risk::LifecycleState::FailedClosed|risk::LifecycleState::RecoveryRequired)).map(|(b,_)|b.clone()).collect();
  for base in dirty {
   self.load_slot_r1324a(&base)?;
   let n=self.isolated.as_ref().ok_or(ExecutionError::CorruptJournal)?.last_clock_ns;
   let e=SimExecutionEvent{event_id:format!("recovery:{}",self.sequence+1),order_id:"owner".into(),kind:SimEventKind::Recovery,simulated:true,event_wall_ms:0,event_mono_ns:n,quantity:None,price:None,reason:Some("cold_restart_requires_reconciliation".into()),intent:None,census_order_ids:None};
   self.commit_event(e,|l,_|{l.cold_restart(n);Ok(())})?;
  }Ok(())
 }
 fn load_slot_r1324a(&mut self,base:&str)->Result<(),ExecutionError>{
  let slot=self.universe.as_ref().and_then(|u|u.slots.get(base)).ok_or(ExecutionError::Invalid("R1324A_SYMBOL_NOT_ELIGIBLE"))?.clone();
  self.projection=PortfolioProjection::derive(&slot.lifecycle,None);self.lifecycle=slot.lifecycle;self.isolated=Some(slot.isolated);Ok(())
 }
 pub fn select_instrument_r1324a(&mut self,base:&str,generation:u64)->Result<(),ExecutionError>{
  if self.fenced{return Err(ExecutionError::OwnerFenced)}
  let u=self.universe.as_ref().ok_or(ExecutionError::Invalid("R1324A_UNIVERSE_REQUIRED"))?;
  if u.generation!=generation{return Err(ExecutionError::Invalid("R1324A_STALE_METADATA_GENERATION"))}
  if u.slots.values().any(|s|!hedge::exposure(&s.lifecycle,self.sequence).known){return Err(ExecutionError::Invalid("R1324A_GLOBAL_UNKNOWN_EXPOSURE"))}
  self.load_slot_r1324a(base)
 }
 pub fn universe_generation_r1324a(&self)->Option<u64>{self.universe.as_ref().map(|u|u.generation)}
 pub fn universe_states_r1324a(&self)->BTreeMap<String,risk::LifecycleState>{self.universe.as_ref().map(|u|u.slots.iter().map(|(b,s)|(b.clone(),s.lifecycle.state.clone())).collect()).unwrap_or_default()}
}

#[cfg(test)]mod r1324a_universe_tests {
 use super::*;
 fn identities(n:usize)->BTreeMap<String,hedge::Identity>{
  let v:serde_json::Value=serde_json::from_str(include_str!("../fixtures/r1298/full_exit.json")).unwrap();
  let template:hedge::Identity=serde_json::from_value(v["identity"].clone()).unwrap();
  (0..n).map(|i|{let mut id=template.clone();let base=format!("A{i:04}");id.profile.base=base.clone();id.profile.spot_symbol=format!("{base}-USDT");id.profile.futures_symbol=format!("{base}USDT");(base,id)}).collect()
 }
 fn path()->PathBuf{let n=std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();PathBuf::from(format!("/mnt/spotarb_db_test/r1273_r1275_engine_latency_20261111/R1301/lab/r1324a-tests/{}-{n}/universe.jsonl",std::process::id()))}
 #[test]fn r1325_auto_budget_is_global_and_unknown_fails_closed(){
  let ids=identities(2);let mut e=DurablePaper::create_universe_r1324a(path(),&ids,1).unwrap();
  assert_eq!(e.auto_budget_r1325(24.03).unwrap(),(0,0,0.0,0.0));
  let (spot,futures)=super::r1296_tests::books(1,20.);let context=super::r1296_tests::context(1);
  for base in ids.keys(){e.select_instrument_r1324a(base,1).unwrap();e.ensure_isolated_intent(e.sequence(),e.isolated_entry_key(),risk::FillLeg::Spot,2.,&spot,&futures,&context).unwrap();}
  let (entries,active,_,_)=e.auto_budget_r1325(24.03).unwrap();assert_eq!(active,2);assert_eq!(entries,2);
  e.universe.as_mut().unwrap().slots.get_mut("A0001").unwrap().lifecycle.state=risk::LifecycleState::RecoveryRequired;
  assert!(e.auto_budget_r1325(24.03).is_err());
 }
 #[test]fn r1324a_universe_251_symbols_one_journal_reopen_and_stale_generation(){
  let ids=identities(251);let p=path();let mut e=DurablePaper::create_universe_r1324a(&p,&ids,7).unwrap();let sequence=e.sequence();
  for base in ids.keys(){e.select_instrument_r1324a(base,7).unwrap();assert_eq!(e.isolated_identity().unwrap(),&ids[base]);}
  assert_eq!(e.sequence(),sequence);assert_eq!(e.universe_states_r1324a().len(),251);
  assert!(e.select_instrument_r1324a("A0000",6).is_err());assert!(e.select_instrument_r1324a("REMOVED",7).is_err());drop(e);
  let e=DurablePaper::recover_universe_r1324a(&p,&ids).unwrap();assert_eq!(e.universe_generation_r1324a(),Some(7));assert_eq!(e.universe_states_r1324a().len(),251);
 }
 #[test]fn r1324a_two_symbols_share_owner_preserve_orders_and_unknown_on_reopen(){
  let ids=identities(2);let p=path();let mut e=DurablePaper::create_universe_r1324a(&p,&ids,1).unwrap();let mut order_ids=std::collections::BTreeSet::new();
  let (spot,futures)=super::r1296_tests::books(1,20.);let context=super::r1296_tests::context(1);
  for base in ids.keys(){e.select_instrument_r1324a(base,1).unwrap();let order=e.ensure_isolated_intent(e.sequence(),e.isolated_entry_key(),risk::FillLeg::Spot,2.,&spot,&futures,&context).unwrap();assert!(order_ids.insert(order.intent.order_id));}
  let before=e.sequence();let mut removed=ids.clone();removed.remove("A0000");assert!(e.replace_universe_r1324a(&removed,2).is_err());assert_eq!(e.sequence(),before);drop(e);
  let mut e=DurablePaper::recover_universe_r1324a(&p,&ids).unwrap();let positions=e.universe_projection_r1324a();
  for base in ids.keys(){assert_eq!(positions[base]["orders"],1);assert_eq!(positions[base]["known"],false);assert!(e.select_instrument_r1324a(base,1).is_err());}
 }
 #[test]fn r1324a_unknown_in_other_symbol_fences_selection(){
  let ids=identities(2);let mut e=DurablePaper::create_universe_r1324a(path(),&ids,1).unwrap();
  e.universe.as_mut().unwrap().slots.get_mut("A0001").unwrap().lifecycle.state=risk::LifecycleState::RecoveryRequired;
  assert!(e.select_instrument_r1324a("A0000",1).is_err());
 }
 #[test]fn r1324a_add_remove_barrier_preserves_unique_owner_and_fences_old_generation(){
  let mut ids=identities(2);let p=path();let mut e=DurablePaper::create_universe_r1324a(&p,&ids,1).unwrap();
  let additional=identities(3).remove("A0002").unwrap();ids.remove("A0000");ids.insert("A0002".into(),additional);
  e.replace_universe_r1324a(&ids,2).unwrap();assert_eq!(e.universe_states_r1324a().len(),2);
  assert!(e.select_instrument_r1324a("A0000",2).is_err());assert!(e.select_instrument_r1324a("A0001",1).is_err());e.select_instrument_r1324a("A0002",2).unwrap();drop(e);
  let e=DurablePaper::recover_universe_r1324a(&p,&ids).unwrap();assert_eq!(e.universe_generation_r1324a(),Some(2));
 }
 #[test]fn r1324a_universe_reorder_identical_and_missing_profile_denied(){
  let ids=identities(3);let p=path();let e=DurablePaper::create_universe_r1324a(&p,&ids,1).unwrap();drop(e);
  let reordered=ids.iter().rev().map(|(b,i)|(b.clone(),i.clone())).collect();let e=DurablePaper::recover_universe_r1324a(&p,&reordered).unwrap();drop(e);
  let mut removed=ids;removed.remove("A0001");assert!(DurablePaper::recover_universe_r1324a(&p,&removed).is_err());
 }
}

impl DurablePaper {
 pub fn universe_identities_r1324a(&self)->BTreeMap<String,hedge::Identity>{self.universe.as_ref().map(|u|u.slots.iter().map(|(b,s)|(b.clone(),s.isolated.identity.clone())).collect()).unwrap_or_default()}
 pub fn recover_universe_scope_r1324a(path:impl AsRef<Path>,run:&str,clock:&str)->Result<Self,ExecutionError>{
  let mut owner=Self::recover_mode_r1324a(path,None,true)?;
  let u=owner.universe.as_ref().ok_or(ExecutionError::Invalid("R1324A_UNIVERSE_GENESIS_INCOMPLETE"))?;
  if u.slots.values().any(|s|s.isolated.identity.run_id!=run||s.isolated.identity.clock_domain!=clock){return Err(ExecutionError::Invalid("R1324A_RUN_SCOPE_MISMATCH"))}
  owner.cold_restart_universe_r1324a()?;Ok(owner)
 }
 /// Called only before sockets exist, after canonical recovery has drained all decisions.
 /// Removed instruments must have no unfinished position. Their history stays in this journal.
 pub fn replace_universe_r1324a(&mut self,identities:&BTreeMap<String,hedge::Identity>,generation:u64)->Result<(),ExecutionError>{
  if self.fenced{return Err(ExecutionError::OwnerFenced)}
  let old=self.universe.as_ref().ok_or(ExecutionError::Invalid("R1324A_UNIVERSE_REQUIRED"))?;
  if generation<=old.generation{return Err(ExecutionError::Invalid("R1324A_STALE_METADATA_GENERATION"))}
  if old.slots.values().any(|s|!hedge::exposure(&s.lifecycle,self.sequence).known){return Err(ExecutionError::Invalid("R1324A_GLOBAL_UNKNOWN_EXPOSURE"))}
  for (base,s) in &old.slots {if !identities.contains_key(base)&&(!matches!(s.lifecycle.state,risk::LifecycleState::Flat|risk::LifecycleState::Closed)||s.lifecycle.orders.values().any(|o|!matches!(o.status,risk::OrderStatus::Filled|risk::OrderStatus::Cancelled|risk::OrderStatus::Rejected))||(s.lifecycle.state==risk::LifecycleState::Closed&&!s.lifecycle.order_census_complete)){return Err(ExecutionError::Invalid("R1324A_REMOVE_UNFINISHED_POSITION"))}}
  let mut slots=BTreeMap::new();
  for (base,id) in identities {
   let slot=if let Some(previous)=old.slots.get(base){
    if &previous.isolated.identity!=id{return Err(ExecutionError::Invalid("R1324A_EXISTING_INSTRUMENT_POLICY_CHANGED"))}previous.clone()
   }else{ExecutionSlot1324a{lifecycle:risk::Lifecycle::new(id.profile.risk_config().map_err(|_|ExecutionError::Invalid("profile"))?)?,isolated:IsolatedState::new(id.clone())?}};
   slots.insert(base.clone(),slot);
  }
  let next=UniverseState1324a{generation,slots};next.validate()?;
  let old_scope=self.isolated_identity().ok_or(ExecutionError::CorruptJournal)?;
  if next.slots.values().any(|s|s.isolated.identity.run_id!=old_scope.run_id||s.isolated.identity.clock_domain!=old_scope.clock_domain){return Err(ExecutionError::Invalid("R1324A_RUN_SCOPE_MISMATCH"))}
  let first=next.slots.keys().next().ok_or(ExecutionError::Invalid("empty universe"))?.clone();
  self.universe=Some(next);self.load_slot_r1324a(&first)?;self.universe_checkpoint_r1324a("universe-generation-barrier")
 }
}

impl DurablePaper {
 pub fn universe_projection_r1324a(&self)->serde_json::Value{
  let slots:BTreeMap<String,serde_json::Value>=self.universe.as_ref().map(|u|u.slots.iter().map(|(b,s)|(b.clone(),serde_json::json!({"lifecycle":s.lifecycle.state,"projection":PortfolioProjection::derive(&s.lifecycle,None),"orders":s.lifecycle.orders.len(),"known":hedge::exposure(&s.lifecycle,self.sequence).known}))).collect()).unwrap_or_default();
  serde_json::json!(slots)
 }
}
// Conservative session budget derived from the same durable slots/archives.
// A restart cannot reset these counters. No second position portfolio.
impl DurablePaper {
 pub fn auto_budget_r1325(&self,cost_bps:f64)->Result<(u64,usize,f64,f64),ExecutionError>{
  if !cost_bps.is_finite()||cost_bps<0.0{return Err(ExecutionError::Invalid("R1325_AUTO_COST"))}
  let u=self.universe.as_ref().ok_or(ExecutionError::Invalid("R1325_AUTO_UNIVERSE"))?;
  let(mut entries,mut active,mut reserve,mut realized)=(0u64,0usize,0.0,0.0);
  for s in u.slots.values(){
   if !hedge::exposure(&s.lifecycle,self.sequence).known{return Err(ExecutionError::Invalid("R1325_AUTO_UNKNOWN"))}
   if !matches!(s.lifecycle.state,risk::LifecycleState::Flat|risk::LifecycleState::Closed){active+=1;}
   let full=s.isolated.full.as_ref().ok_or(ExecutionError::Invalid("R1325_AUTO_EXIT_SESSION"))?;
   for l in std::iter::once(&s.lifecycle).chain(full.archives.iter().map(|a|&a.lifecycle)){
    if let Some(ledger)=&l.ledger{entries+=1;reserve+=ledger.spot_buy_quote*cost_bps/10000.0;
     if l.state==risk::LifecycleState::Closed{realized+=ledger.realized_quote();}}
   }
  }
  if !reserve.is_finite()||!realized.is_finite(){return Err(ExecutionError::Invalid("R1325_AUTO_BUDGET_NONFINITE"))}
  Ok((entries,active,reserve,realized))
 }
}
