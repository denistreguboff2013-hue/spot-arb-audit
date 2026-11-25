//! Accepted typed integration reused by explicit IntegratedPaper; virtual clock helpers remain test-only.
use super::*;
use bridge1301::{ValidatedInput,BridgeReceiver,Health};
type Failure=Box<dyn std::error::Error>;
struct Boundary {
 identity:isolated_policy::Identity,metadata_generation:u64,price_ticks:[String;2],
 real_off:bool,kill_engaged:bool,storage_healthy:bool,
}
impl Boundary {
 fn digest(&self)->Result<String,Failure>{Ok(isolated_policy::digest(&(&self.identity,self.metadata_generation,&self.price_ticks)).map_err(offline_adapter_error)?)}
 fn validate(&self,owner:&DurablePaper)->Result<(),Failure>{
  if !self.real_off||!self.kill_engaged||!self.storage_healthy||owner.is_fenced(){return Err("R1322 safety/storage/owner denied".into())}
  if self.metadata_generation==0||owner.isolated_identity()!=Some(&self.identity){return Err("R1322 identity/metadata mismatch".into())}
  if !owner.exposure_view().known{return Err("R1322 unknown/recovery exposure".into())}
  Ok(())
 }
}
// No Deserialize, public constructor, Clone, or caller-supplied Decision.
struct TypedCandidate {
 inputs:[ValidatedInput;2],rows:Vec<Decision>,venue:Venue,
 metadata_digest:String,core_digest:String,rules:[String;2],price_ticks:[String;2],decision_id:String,intent_key:String,metadata_generation:u64,
 owner_revision:u64,journal:PathBuf,review_quantity:f64,
}
fn trade_id_r1326(identity:&isolated_policy::Identity)->Result<String,Failure>{
 let trade_hash=isolated_policy::digest(&(identity.run_id.as_str(),identity.profile.base.as_str())).map_err(offline_adapter_error)?;
 Ok(format!("R1326-{trade_hash}"))
}
fn trade_intent_witness_r1326(c:&TypedCandidate,identity:&isolated_policy::Identity,quantity:f64,owner_revision:u64)->Result<spotarb_native_market_core::native_r1173_compact_writer::PaperWitness,Failure>{
 let trade_id=trade_id_r1326(identity)?;
 let owner_generation=c.intent_key.strip_prefix("entry:").and_then(|v|v.parse::<u64>().ok()).filter(|v|*v>0).ok_or("R1326 invalid owner entry generation")?;
 let audit_hash=openssl::sha::sha256(trade_id.as_bytes()).iter().map(|b|format!("{b:02x}")).collect::<String>();
 let sources=c.inputs.iter().map(|input|{
  let p=input.source_provenance();
  serde_json::json!({"durability":"RAM_ONLY","venue":format!("{:?}",input.venue()),
   "instrument":input.instrument(),"venue_symbol":input.venue_symbol(),
   "trace_id":input.trace_id(),"session_id":p.session_id,
   "connection_epoch":p.connection_epoch,"selection_epoch":p.selection_epoch,
   "payload_sha256":p.payload_sha256,"frame_trace_id":p.frame_trace_id,
   "source_receive_ns":input.book().received_ns,"source_receive_wall_ms":input.book().received_wall_ms,
   "exchange_timestamp_ms":input.book().exchange_ts_ms,"sequence":input.book().sequence})
 }).collect::<Vec<_>>();
 let evidence=serde_json::json!({"strategy_identity":identity,"instrument":identity.profile.base,
  "spot_symbol":identity.profile.spot_symbol,"futures_symbol":identity.profile.futures_symbol,
  "generation":c.metadata_generation,"owner_generation":owner_generation,"owner_revision":owner_revision,
  "candidate_owner_revision":c.owner_revision,"decision_id":c.decision_id,
  "execution_command_key":c.intent_key,"venue_pair":["OkxSpot","BinanceFutures"],
  "source_provenance":sources,"books":[c.inputs[0].book(),c.inputs[1].book()],
  "decision_rows":c.rows,"rule_seal":c.rules,"price_ticks":c.price_ticks,
  "fees":{"model":identity.profile.fee_model,"configured_bps":identity.profile.fee_bps},
  "quantity":quantity,"metadata_digest":c.metadata_digest,"decision_core_digest":c.core_digest});
 let risk=serde_json::json!({"risk_passed":true,"risk_result":"RISK_SIZE_ALLOW",
  "quantity":quantity,"final_rules":c.rules,"final_boundary_recheck":true});
 Ok(spotarb_native_market_core::native_r1173_compact_writer::PaperWitness{id:format!("R1326T{audit_hash}"),
  kind:"TRADE_INTENT".into(),body:serde_json::json!({"schema":"SPOTARB/R1326/TRADE-INTENT/V1",
   "trade_id":trade_id,"risk_passed":true,"generation":c.metadata_generation,
   "paper_only":true,"real_fills_attested":false,"signal_evidence":evidence,"risk_decision":risk})})
}
fn record_matches_identity_r1326(identity:&isolated_policy::Identity,record:&Value)->bool{
 let expected=serde_json::to_value(identity).ok();
 if !record["isolated"]["identity"].is_null(){return expected.as_ref()==Some(&record["isolated"]["identity"])}
 record["event"]["intent"]["instrument"].as_str().is_some_and(|i|identity_matches_instrument_r1326(identity,i))
}
fn record_has_order_obligation_r1326(record:&Value)->bool{
 let event=&record["event"];
 let lifecycle_has_order=record["lifecycle"]["orders"].as_object().is_some_and(|m|!m.is_empty());
 let order=event["order_id"].as_str().filter(|s|!s.is_empty());
 let intent_has_order=order.is_some_and(|id|event["intent"]["order_id"].as_str()==Some(id));
 let census_has_orders=event["census_order_ids"].as_array().is_some_and(|a|!a.is_empty());
 lifecycle_has_order||intent_has_order||census_has_orders
}
fn identity_matches_instrument_r1326(identity:&isolated_policy::Identity,instrument:&str)->bool{
 instrument==identity.profile.base||instrument==identity.profile.spot_symbol||instrument==identity.profile.futures_symbol
}
fn trade_records_r1326(identity:&isolated_policy::Identity,records:Vec<(u64,Value)>,mut order_ids:std::collections::BTreeSet<String>,started:bool,expected_intent:Option<(&str,u64,u64,u64,&str)>)->Result<Vec<(u64,Value)>,Failure>{
 let expected_identity=serde_json::to_value(identity)?;
 let mut out=Vec::new();
 for (sequence,record) in records {
  let event=&record["event"];
  let kind=event["kind"].as_str().ok_or("R1326 execution event missing kind")?;
  let owner_matches=record["isolated"]["identity"]==expected_identity;
  let orders=record["lifecycle"]["orders"].as_object();
  if let Some(intent)=event["intent"].as_object(){
   let order=intent.get("order_id").and_then(Value::as_str).filter(|v|!v.is_empty());
   let instrument=intent.get("instrument").and_then(Value::as_str);
   let event_order=event["order_id"].as_str();
   if order!=event_order||!instrument.is_some_and(|v|identity_matches_instrument_r1326(identity,v)){
    if event_order.is_some_and(|v|order_ids.contains(v))||owner_matches{
     return Err("R1326 execution intent/order context mismatch".into())
    }
    continue
   }
   if !owner_matches{
    if order_ids.contains(order.unwrap()){return Err("R1326 execution event belongs to another owner identity".into())}
    if record["isolated"]["identity"].is_null()&&instrument.is_some_and(|v|identity_matches_instrument_r1326(identity,v)){
     return Err("R1326 matching legacy execution record lacks owner identity".into())
    }
    continue
   }
   if kind=="Accepted" {
    if let Some((command_key,metadata_generation,owner_generation,owner_revision,decision_id))=expected_intent {
     let context_generation=record["isolated"]["full"]["generation"].as_u64().ok_or("R1326 accepted record missing generation")?;
     if context_generation!=owner_generation{return Err("R1326 accepted order generation differs from canonical intent".into())}
     if !record["universe"].is_null(){
      let durable_metadata_generation=record["universe"]["generation"].as_u64().ok_or("R1326 accepted universe missing metadata generation")?;
      if durable_metadata_generation!=metadata_generation{return Err("R1326 accepted metadata generation differs from canonical intent".into())}
     }
     let intent_id=intent.get("intent_id").and_then(Value::as_str).ok_or("R1326 accepted order missing intent id")?;
     let order_id=order.ok_or("R1326 accepted order id absent")?;
     let keys=record["isolated"]["keys"].as_object().ok_or("R1326 accepted record missing owner key map")?;
     let matching_keys=keys.iter().filter(|(_,value)|value.as_str()==Some(order_id)).map(|(key,_)|key.as_str()).collect::<Vec<_>>();
     let registered=record["isolated"]["intents"][order_id].as_object().ok_or("R1326 accepted order missing registered owner intent")?;
     if matching_keys.len()!=1||registered["intent"]["intent_id"].as_str()!=Some(intent_id)||
        registered["intent"]["order_id"].as_str()!=Some(order_id)||
        registered["command_key"].as_str()!=Some(matching_keys[0]){
      return Err("R1326 accepted order key/registered-intent identity mismatch".into())
     }
     let leg=intent.get("leg").and_then(Value::as_str).unwrap_or("");
     let side=intent.get("side").and_then(Value::as_str).unwrap_or("");
     let is_entry=leg=="Spot"&&side=="Buy"&&intent.get("exit_reason").map_or(true,Value::is_null);
     let registered_context=&registered["witness"]["context"];
     let registered_input_id=registered_context["input_id"].as_str().filter(|v|!v.is_empty()).ok_or("R1326 accepted order missing source decision context")?;
     if registered_context["clock_domain"].as_str()!=Some(identity.clock_domain.as_str()){
      return Err("R1326 accepted order clock domain mismatch".into())
     }
     let registered_revision=registered["witness"]["owner_revision"].as_u64().ok_or("R1326 accepted order missing owner revision")?;
     if sequence!=registered_revision.saturating_add(1){return Err("R1326 accepted order does not follow its registered owner revision".into())}
     if is_entry&&(matching_keys[0]!=command_key||sequence!=owner_revision.saturating_add(1)||
       registered_revision!=owner_revision||registered_input_id!=decision_id){
      return Err("R1326 entry order does not match canonical command key/revision/context".into())
     }
    }
   }
   order_ids.insert(order.unwrap().to_string());
  } else if !owner_matches {
   if event["order_id"].as_str().is_some_and(|v|order_ids.contains(v)){
    return Err("R1326 execution event belongs to another owner identity".into())
   }
   continue
  }
  match kind{
   "OrderCensus"=>{
    let census=event["census_order_ids"].as_array().ok_or("R1326 census missing order ids")?;
    let ids=census.iter().map(|v|v.as_str().filter(|s|!s.is_empty()).ok_or("R1326 census invalid order id")).collect::<Result<std::collections::BTreeSet<_>,_>>()?;
    if ids.len()!=census.len(){return Err("R1326 census duplicate order id".into())}
    let state_ids=orders.ok_or("R1326 census missing lifecycle orders")?.keys().map(String::as_str).collect::<std::collections::BTreeSet<_>>();
    if ids!=state_ids{return Err("R1326 census/lifecycle order set mismatch".into())}
    if ids.is_empty(){continue}
    if !ids.iter().all(|id|order_ids.contains(*id)){return Err("R1326 census contains unowned order".into())}
    out.push((sequence,record));
   },
   "Recovery"=>{
    let state_ids=orders.ok_or("R1326 recovery missing lifecycle orders")?.keys().map(String::as_str).collect::<std::collections::BTreeSet<_>>();
    if state_ids.is_empty(){continue}
    if !state_ids.iter().all(|id|order_ids.contains(*id)){return Err("R1326 recovery contains unowned order".into())}
    out.push((sequence,record));
   },
   _=>{
    let order=event["order_id"].as_str().filter(|v|!v.is_empty()).ok_or("R1326 execution event missing order identity")?;
    if order_ids.contains(order){
     if !orders.is_some_and(|map|map.contains_key(order)){return Err("R1326 event order missing from current lifecycle snapshot".into())}
     out.push((sequence,record));
    }
   }
  }
 }
 if !started {
  if let Some((command_key,_,_,_,_))=expected_intent {
   let root_accepted=out.iter().any(|(_,record)|{
    let event=&record["event"];let intent=&event["intent"];
    event["kind"].as_str()==Some("Accepted")&&intent["leg"].as_str()==Some("Spot")&&
    intent["side"].as_str()==Some("Buy")&&intent["exit_reason"].is_null()&&
    intent["order_id"].as_str().and_then(|id|record["isolated"]["intents"][id]["command_key"].as_str())==Some(command_key)
   });
   if !root_accepted{return Err("R1326 durable intent lacks matching root Accepted execution record".into())}
  }
 }
 Ok(out)
}
fn trade_event_witness_r1326(identity:&isolated_policy::Identity,sequence:u64,record:Value)->Result<spotarb_native_market_core::native_r1173_compact_writer::PaperWitness,Failure>{
 let trade_id=trade_id_r1326(identity)?;let event_id=format!("execution-sequence-{sequence}");
 let event_hash=openssl::sha::sha256(format!("{trade_id}:{event_id}").as_bytes()).iter().map(|b|format!("{b:02x}")).collect::<String>();
 Ok(spotarb_native_market_core::native_r1173_compact_writer::PaperWitness{id:format!("R1326E{event_hash}"),kind:"TRADE_EVENT".into(),body:serde_json::json!({
  "schema":"SPOTARB/R1326/TRADE-EVENT/V1","trade_id":trade_id,"event_id":event_id,
  "paper_only":true,"real_fills_attested":false,"event":{"execution_sequence":sequence,"execution_record":record}})})
}
struct TypedPipeline {
 pair:[Option<ValidatedInput>;2],last_observed:[Option<(u64,u64,String,i64)>;2],used:HashSet<String>,terminal:bool,auto:Option<std::sync::Arc<integrated::AutoPolicy>>,last_spread:Option<(u64,f64)>,
 #[cfg(all(test,feature="offline-test-clock"))] test_intent_inject:spotarb_native_market_core::native_r1173_compact_writer::Inject,
}
impl TypedPipeline {
 fn new()->Self{Self{pair:[None,None],last_observed:[None,None],used:HashSet::new(),terminal:false,auto:None,last_spread:None,#[cfg(all(test,feature="offline-test-clock"))] test_intent_inject:spotarb_native_market_core::native_r1173_compact_writer::Inject::None}}
 #[cfg(all(test,feature="offline-test-clock"))]
 fn set_test_intent_inject(&mut self,value:spotarb_native_market_core::native_r1173_compact_writer::Inject){self.test_intent_inject=value;}
 fn observe(&mut self,r:&mut IsolatedPaperRuntime,input:ValidatedInput,rx:&BridgeReceiver,h:&Health,b:&Boundary)->Result<Option<TypedCandidate>,Failure>{
  if self.terminal{return Err("R1322 terminal pipeline".into())}
  let result=self.observe_inner(r,input,rx,h,b,None,true);
  if result.is_err(){self.terminal=true;}result
 }
 fn observe_inner(&mut self,r:&mut IsolatedPaperRuntime,input:ValidatedInput,rx:&BridgeReceiver,h:&Health,b:&Boundary,committed:Option<spotarb_native_market_core::Book>,allow_execution:bool)->Result<Option<TypedCandidate>,Failure>{
  b.validate(&r.execution)?;check_core(r,b)?;if !h.healthy(){return Err("R1322 unhealthy bridge".into())}
  if input.instrument()!=r.identity.profile.base||input.clock_domain()!=r.identity.clock_domain{return Err("R1322 source scope".into())}
  let(n,w)=clocks();let live=bridge1301::observation_r1410(&input,rx,n,w).map_err(offline_adapter_error)?;
  // This pipeline belongs to one base; each venue has an ordered selected
  // stream. Retain its high-water mark even if the cached pair is retired.
  // Provider/Owner still validate the actual source and epoch independently.
  let venue=input.venue();let index=if venue==Venue::OkxSpot{0}else{1};
  let source=input.source_provenance();
  let seq=input.book().sequence.ok_or("R1322 missing event sequence")?;
  let mark=(source.selection_epoch,source.connection_epoch,source.session_id.clone(),seq);
  if self.last_observed[index].as_ref().is_some_and(|old|
   mark.0<old.0||(mark.0==old.0&&(mark.1!=old.1||mark.2!=old.2||mark.3<=old.3))){
   return Err("R1322 duplicate/conflicting event".into())
  }
  let exchange=if input.venue()==Venue::OkxSpot{"okx"}else{"binance"};
  let book=if let Some(book)=committed {
   let mut expected=input.book().clone();expected.changed_ns=book.changed_ns;
   if expected!=book{return Err("R1325 committed Owner book mismatch".into())}book
  }else{
   let(frame,update)=input.owner_parts_r1322();
   let update=r.book_owner.prepare_parsed(&frame,update).map_err(offline_adapter_error)?;
   let book=update.book.clone();r.book_owner.ingest_parsed(&frame,update,&mut r.history).map_err(offline_adapter_error)?;book
  };
  let r1325_start=std::time::Instant::now();
  let result=r.decision.update_at(input.instrument(),exchange,book,n,w);
  spotarb_native_market_core::pipeline_diagnostics_r1325::record("DecisionCore",r1325_start.elapsed().as_nanos().min(u64::MAX as u128) as u64,result.is_ok());
  let rows=result.map_err(offline_adapter_error)?;
  if r.actions.len()>=128{return Err("bounded decision evidence full".into())}
  r.actions.push(json!({"action":"typed_decision","trace_id":input.trace_id(),"observation_only":!live||!allow_execution,"rows":rows}));
  self.last_observed[index]=Some(mark);r.frames_seen+=1;
  self.pair[index]=Some(input);
  let(Some(s),Some(f))=(&self.pair[0],&self.pair[1])else{return Ok(None)};
  let inputs=[s.clone(),f.clone()];
  if !live||!allow_execution{return Ok(None)}
  // Historical observations update Decision but cannot form an intent.
  // Recheck BOTH legs; ordinary replacement waits for the new leg,
  // every other denial remains an error.
  let mut pair_live=true;
  for leg in &inputs{pair_live&=bridge1301::observation_r1410(leg,rx,n,w).map_err(offline_adapter_error)?;}
  if !pair_live{return Ok(None)}
  if r.execution.lifecycle().state==paper_risk::LifecycleState::Closed{return Ok(None)}
  let quantity=if r.execution.lifecycle().state==paper_risk::LifecycleState::Flat{
   let Some(d)=rows.iter().find(|d|d.base==r.identity.profile.base&&d.buy_exchange=="okx"&&d.sell_exchange=="binance"&&d.reason=="EXECUTABLE_OPPORTUNITY"&&d.risk_result.as_deref()==Some("RISK_SIZE_ALLOW"))else{return Ok(None)};
   d.quantity.ok_or("R1322 missing decision quantity")?
  }else{r.execution.registered_intents().map_err(offline_adapter_error)?.values().map(|v|v.intent.quantity).fold(0.0,f64::max)};
  let r1325_start=std::time::Instant::now();
  let result=bridge1301::review_pair_r1322(&inputs,&b.identity,b.metadata_generation,&b.price_ticks,quantity,rx,h,n,w);
  spotarb_native_market_core::pipeline_diagnostics_r1325::record("RiskAdmission",r1325_start.elapsed().as_nanos().min(u64::MAX as u128) as u64,result.is_ok());
  let rules=result.map_err(offline_adapter_error)?;
  let core_digest=core_identity(&r.decision,&r.identity.profile.base)?;
  let metadata_digest=b.digest()?;let intent_key=r.execution.isolated_entry_key();
  let decision_id=format!("r1322:{}",isolated_policy::digest(&(&inputs,&rows,&metadata_digest,&core_digest,&rules,&intent_key)).map_err(offline_adapter_error)?);
  Ok(Some(TypedCandidate{inputs,rows,venue,metadata_digest,core_digest,rules,price_ticks:b.price_ticks.clone(),decision_id,intent_key,metadata_generation:b.metadata_generation,owner_revision:r.execution.sequence(),journal:r.execution.journal_path().to_path_buf(),review_quantity:quantity}))
 }
 fn consume<F>(&mut self,r:&mut IsolatedPaperRuntime,c:TypedCandidate,rx:&BridgeReceiver,h:&Health,b:&Boundary,clock:F)->Result<(),Failure>
 where F:FnMut()->(u64,i64){
  self.consume_inner(r,c,rx,h,b,None,clock)
 }
 fn consume_with_writer<F>(&mut self,r:&mut IsolatedPaperRuntime,c:TypedCandidate,rx:&BridgeReceiver,h:&Health,b:&Boundary,writer:&mut spotarb_native_market_core::native_r1173_compact_writer::Writer,clock:F)->Result<(),Failure>
 where F:FnMut()->(u64,i64){
  self.consume_inner(r,c,rx,h,b,Some(writer),clock)
 }
 fn consume_inner<F>(&mut self,r:&mut IsolatedPaperRuntime,c:TypedCandidate,rx:&BridgeReceiver,h:&Health,b:&Boundary,mut writer:Option<&mut spotarb_native_market_core::native_r1173_compact_writer::Writer>,mut clock:F)->Result<(),Failure>
 where F:FnMut()->(u64,i64){
  if self.terminal{return Err("R1322 terminal pipeline".into())}
  let result=(||{
   check_core(r,b)?;
   if core_identity(&r.decision,&r.identity.profile.base)?!=c.core_digest{return Err("R1322 decision policy replaced".into())}
   if self.used.len()>=4096||self.used.contains(&c.decision_id){return Err("R1322 duplicate decision/capacity".into())}
   if r.execution.sequence()!=c.owner_revision||r.execution.journal_path()!=c.journal||r.execution.isolated_entry_key()!=c.intent_key{return Err("R1322 stale owner/durable identity".into())}
   let expected=format!("r1322:{}",isolated_policy::digest(&(&c.inputs,&c.rows,&c.metadata_digest,&c.core_digest,&c.rules,&c.intent_key)).map_err(offline_adapter_error)?);
   if expected!=c.decision_id{return Err("R1322 candidate conflict".into())}
   let(n,w)=clock();
   check_boundary(&c,&r.execution,rx,h,b,n,w,Some(c.review_quantity))?;
   self.used.insert(c.decision_id.clone());
   let context=isolated_policy::MarketContext{input_id:c.decision_id.clone(),clock_domain:b.identity.clock_domain.clone(),now_ns:n,wall_ms:w,spot_hot:true,futures_hot:true};
   let spot=c.inputs[0].book().clone();let futures=c.inputs[1].book().clone();
   if let Some(auto)=&self.auto{
    let spread=(futures.bids.first().ok_or("future bid")?.0/spot.asks.first().ok_or("spot ask")?.0-1.0)*10000.0;
    let velocity=self.last_spread.and_then(|(t,v)|(n>t).then(||(spread-v)*1e9/(n-t) as f64));
    self.last_spread=Some((n,spread));
    if !auto.apply(r,&context,&spot,&futures,velocity)?{return Ok(())}
   }
   // The actual dispatcher still performs lifecycle/risk/hedge/exit/census and commits.
   let prior_state=r.execution.lifecycle().state;
   let prior_sequence=r.execution.sequence();
   let prior_order_ids=r.execution.lifecycle().orders.keys().cloned().collect::<std::collections::BTreeSet<_>>();
   let trade_identity=r.identity.clone();
   #[cfg(all(test,feature="offline-test-clock"))]
   let intent_inject=self.test_intent_inject;
   #[cfg(not(all(test,feature="offline-test-clock")))]
   let intent_inject=spotarb_native_market_core::native_r1173_compact_writer::Inject::None;
   process_isolated_pair_guarded(r,c.venue,&context,spot,futures,c.rows.clone(),|owner,quantity|{
    let(n,w)=clock();
    if owner.lifecycle().state==paper_risk::LifecycleState::Flat{if let Some(auto)=&self.auto{auto.check_turnover(&b.identity.profile.base,n,w)?;}}
    check_boundary(&c,owner,rx,h,b,n,w,quantity)?;
    if owner.lifecycle().state==paper_risk::LifecycleState::Flat {
     if let (Some(quantity),Some(writer))=(quantity,writer.as_deref_mut()) {
      let intent_owner_revision=owner.sequence();
      let witness=trade_intent_witness_r1326(&c,&trade_identity,quantity,intent_owner_revision)?;
      writer.commit_trade_intent_r1326(witness,intent_inject)?;
     }
    }
    Ok(())
   })?;
   let current_state=r.execution.lifecycle().state;
   if prior_state!=paper_risk::LifecycleState::Flat||!matches!(current_state,paper_risk::LifecycleState::Flat){
    if let Some(writer)=writer.as_deref_mut(){
     let records=r.execution.r1326_records_after(prior_sequence).map_err(offline_adapter_error)?;
     let trade_id=trade_id_r1326(&r.identity)?;
     let intent_context=writer.trade_intent_context_r1326(&trade_id)?;
     let (command_key,metadata_generation,owner_generation,owner_revision,decision_id)=intent_context.as_ref().ok_or("R1326 nonflat lifecycle has no durable canonical intent")?;
     for (sequence,record) in trade_records_r1326(&r.identity,records,prior_order_ids,prior_state!=paper_risk::LifecycleState::Flat,Some((command_key.as_str(),*metadata_generation,*owner_generation,*owner_revision,decision_id.as_str())))? {
      let event=trade_event_witness_r1326(&r.identity,sequence,record)?;
      writer.commit_trade_event_r1326(event,spotarb_native_market_core::native_r1173_compact_writer::Inject::None)?;
     }
    }
   }
   Ok(())
  })();
  if result.is_err(){self.terminal=true;}result
 }
}
fn check_boundary(c:&TypedCandidate,owner:&DurablePaper,rx:&BridgeReceiver,h:&Health,b:&Boundary,n:u64,w:i64,quantity:Option<f64>)->Result<(),Failure>{
 #[cfg(not(test))]
 if !kill_engaged_r1301(){return Err("live kill-switch recheck denied".into())}
 b.validate(owner)?;if !h.healthy()||b.digest()?!=c.metadata_digest{return Err("R1322 changed rules/metadata/health".into())}
 for leg in &c.inputs{bridge1301::revalidate_r1322(leg,rx,n,w).map_err(offline_adapter_error)?;}
 let q=quantity.unwrap_or(c.review_quantity);
 let rules=bridge1301::review_pair_r1322(&c.inputs,&b.identity,b.metadata_generation,&b.price_ticks,q,rx,h,n,w).map_err(offline_adapter_error)?;
 if rules!=c.rules{return Err("R1322 changed rule seal".into())}
 Ok(())
}
// One explicit virtual clock shared by the R1322 Engine, Provider, bridge Health,
// DecisionCore, Admission, and Owner callbacks in the feature-gated offline tests.
#[cfg(all(test,feature="offline-test-clock"))]
mod offline_clock {
 use super::*;
#[derive(Clone,Copy)]
pub(super) struct OfflineTime{mono_ns:u64,wall_ms:i64,sub_ms_ns:u32}
// Fixed epoch with room for the 300ms qualification history; independent of uptime.
pub(super) const OFFLINE_EPOCH:OfflineTime=OfflineTime{mono_ns:1_000_000_000,wall_ms:1_000,sub_ms_ns:0};
thread_local!{static OFFLINE_CLOCK:std::cell::Cell<OfflineTime>=const{std::cell::Cell::new(OFFLINE_EPOCH)};}
pub(super) fn reset_clock(){OFFLINE_CLOCK.with(|c|c.set(OFFLINE_EPOCH));}
pub(super) fn clocks()->(u64,i64){OFFLINE_CLOCK.with(|c|{let t=c.get();(t.mono_ns,t.wall_ms)})}
// Deliberately permits inconsistent/future times for negative admission tests.
pub(super) fn set_clock(mono_ns:u64,wall_ms:i64){OFFLINE_CLOCK.with(|c|c.set(OfflineTime{mono_ns,wall_ms,sub_ms_ns:0}));}
pub(super) fn past_clock(n:u64,w:i64,age_ms:u64)->(u64,i64){
 let age_ns=age_ms.checked_mul(1_000_000).expect("offline frame age overflow");
 let age_wall=i64::try_from(age_ms).expect("offline wall age overflow");
 (n.checked_sub(age_ns).expect("offline frame predates monotonic epoch"),w.checked_sub(age_wall).expect("offline frame wall underflow"))
}
pub(super) fn advance_clock_ns(delta:u64){OFFLINE_CLOCK.with(|c|{
 let t=c.get();let total=u128::from(t.sub_ms_ns)+u128::from(delta);
 let millis=i64::try_from(total/1_000_000).expect("offline elapsed milliseconds overflow");
 let mono_ns=t.mono_ns.checked_add(delta).expect("offline monotonic overflow");
 let wall_ms=t.wall_ms.checked_add(millis).expect("offline wall overflow");
 // Publish only after both checked additions succeed; failed advances are atomic.
 c.set(OfflineTime{mono_ns,wall_ms,sub_ms_ns:(total%1_000_000)as u32});
});}
pub(super) fn allocate_test_journal()->(String,PathBuf){
 static NEXT:std::sync::atomic::AtomicU64=std::sync::atomic::AtomicU64::new(1);
 let root=PathBuf::from("/mnt/spotarb_db_test/r1273_r1275_engine_latency_20261111/R1301/lab/r1322");
 std::fs::create_dir_all(&root).expect("create isolated R1322 test root");
 loop{let id=NEXT.fetch_add(1,std::sync::atomic::Ordering::Relaxed);let name=format!("{}-{id}",std::process::id());let dir=root.join(name);match std::fs::create_dir(&dir){Ok(())=>return(format!("r1322-{}-{id}",std::process::id()),dir.join("paper.jsonl")),Err(e)if e.kind()==std::io::ErrorKind::AlreadyExists=>continue,Err(e)=>panic!("create isolated R1322 run directory: {e}")}}
}
}
#[cfg(all(test,feature="offline-test-clock"))]
use offline_clock::*;
#[cfg(not(all(test,feature="offline-test-clock")))]
fn clocks()->(u64,i64){(spotarb_native_market_core::ingress_runtime_r1205::mono_ns(),spotarb_native_market_core::ingress_runtime_r1205::wall_ms())}
fn check_core(r:&IsolatedPaperRuntime,b:&Boundary)->Result<(),Failure>{
 if r.decision.decisions_paused()||r.decision.metadata_generation()!=b.metadata_generation{return Err("R1322 actual metadata generation/barrier".into())}
 let expected=vec![isolated_route(&b.identity.profile)];
 if isolated_policy::digest(&r.decision.routes_for(&b.identity.profile.base)).map_err(offline_adapter_error)?!=isolated_policy::digest(&Some(expected.as_slice())).map_err(offline_adapter_error)?{return Err("R1322 actual routes mismatch".into())}
 Ok(())
}
fn core_identity(core:&DecisionCore,base:&str)->Result<String,Failure>{
 let c=&core.cfg;let mut blacklist=c.blacklist.iter().collect::<Vec<_>>();blacklist.sort();
 let numbers=[c.base_size.to_bits(),c.risk_cap.to_bits(),c.cost.to_bits(),c.required_net.to_bits(),c.min_price.to_bits(),c.max_price.to_bits()];
 Ok(isolated_policy::digest(&(core.index,core.total,core.metadata_generation(),core.decisions_paused(),core.routes_for(base),c.max_age_ms,c.metadata_ttl_ms,numbers,blacklist)).map_err(offline_adapter_error)?)
}
#[cfg(all(test,feature="offline-test-clock"))]
#[path="native_paper_typed_r1322_tests.rs"] mod tests;
#[path="native_paper_integrated_r1325.rs"]
mod integrated;
pub(super) use integrated::run_integrated_r1325;
