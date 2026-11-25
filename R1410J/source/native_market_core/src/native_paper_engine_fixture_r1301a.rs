//! SYNTHETIC TRANSPORT FIXTURE. No recorded exchange capture and no socket.
//! Uses unchanged production decoder, reconstruction, UCM quality, provider and bridge.
use super::*;
use crate::market_data_ingress_r1205::{Engine,provenance,key};
pub(crate) struct EngineFixture {
 pub engine:Engine, pub receiver:BridgeReceiver, pub health:Arc<Health>,
 pub inputs:Vec<ValidatedInput>, pub trace:Vec<serde_json::Value>,
 provider:Provider, sender:BridgeSender, base:String,
}
pub(crate) fn raw(venue:Venue,session:&str,base:&str,n:u64,w:i64,seq:i64,snapshot:bool)->RawFrame{
 let value=if venue==Venue::OkxSpot{
  serde_json::json!({"arg":{"channel":"books","instId":format!("{base}-USDT")},"action":if snapshot{"snapshot"}else{"update"},
   "data":[{"ts":w.to_string(),"seqId":seq,"prevSeqId":if snapshot{-1}else{seq-1},
    "bids":[["100",seq.to_string()],["99","2"]],"asks":[["101","3"],["102","2"]]}]})
 }else{
  serde_json::json!({"stream":format!("{}usdt@depth20@100ms",base.to_ascii_lowercase()),
   "data":{"s":format!("{base}USDT"),"e":"depthUpdate","E":w,"u":seq,"U":seq,"pu":seq-1,
   "b":[["100",seq.to_string()],["99","2"]],"a":[["101","3"],["102","2"]]}})
 };
 RawFrame{venue,epoch:1,received_ns:n,received_wall_ms:w,raw:value.to_string(),source:Some(provenance(session.into(),1))}
}
impl EngineFixture {
 pub fn new(clock:&str)->Self{Self::for_base(clock,"AAA")}
 pub fn for_base(clock:&str,base:&str)->Self{
  let authority:Authority=Default::default();
  let mut engine=Engine::new_volatile(authority.clone(),vec![key(Venue::OkxSpot,base),key(Venue::BinanceFutures,base)]).unwrap();
  for (venue,prefix) in [(Venue::OkxSpot,"okx"),(Venue::BinanceFutures,"binance")]{
   for copy in ["a","b"]{let id=format!("{prefix}-{copy}");engine.register_connection(&id,venue,1).unwrap();engine.mark_subscribed(&id);}
  }
  let(sender,receiver,health)=channel(authority.clone());
  Self{base:base.into(),engine,receiver,health,inputs:Vec::new(),trace:Vec::new(),provider:Provider{authority,clock_domain:clock.into(),seen:HashMap::new()},sender}
 }
 pub fn for_single_venue(clock:&str,venue:Venue)->Self{
  let base="AAA";let authority:Authority=Default::default();
  let engine=Engine::new_volatile(authority.clone(),vec![key(venue,base)]).unwrap();
  let prefix=if venue==Venue::OkxSpot{"okx"}else{"binance"};
  let mut engine=engine;
  for copy in ["a","b"]{let id=format!("{prefix}-{copy}");engine.register_connection(&id,venue,1).unwrap();engine.mark_subscribed(&id);}
  let(sender,receiver,health)=channel(authority.clone());
  Self{base:base.into(),engine,receiver,health,inputs:Vec::new(),trace:Vec::new(),provider:Provider{authority,clock_domain:clock.into(),seen:HashMap::new()},sender}
 }
 pub fn qualify_single_venue(&mut self,venue:Venue){
  let n=crate::ingress_runtime_r1205::mono_ns();let w=crate::ingress_runtime_r1205::wall_ms();
  let prefix=if venue==Venue::OkxSpot{"okx"}else{"binance"};
  for (step,age) in [300u64,150,0].into_iter().enumerate(){
   let frames=["a","b"].into_iter().map(|copy|raw(venue,&format!("{prefix}-{copy}"),&self.base,n-age*1_000_000,w-age as i64,100+step as i64,step==0)).collect::<Vec<_>>();
   let outputs=self.engine.receive_batch(frames,n,w).unwrap();
   for output in outputs{
    if let Output::Reset{venue,ref base,epoch}=output{self.provider.selection_reset(venue,base,epoch).unwrap();continue}
    if matches!(output,Output::Frame(_)|Output::Book(_,_)){
     let input=self.provider.accept(output,crate::ingress_runtime_r1205::mono_ns(),crate::ingress_runtime_r1205::wall_ms()).unwrap();
     assert!(input.snapshot_generation().is_some(),"single-scope actual Engine must supply witness");
     self.sender.send(input.clone()).unwrap();self.inputs.push(input);
    }
   }
  }
  assert_eq!(self.inputs.len(),1,"one actual Engine scope must qualify");
 }
 pub fn qualify(&mut self){self.qualify_with_clock(||(crate::ingress_runtime_r1205::mono_ns(),crate::ingress_runtime_r1205::wall_ms()),None)}
 #[cfg(feature="offline-test-clock")]
 pub(crate) fn qualify_r1322(&mut self,clock:fn()->(u64,i64),advance:fn(u64)){self.engine.set_test_clock(clock);self.health.set_test_clock(clock);self.qualify_with_clock(clock,Some(advance))}
 fn qualify_with_clock(&mut self,clock:fn()->(u64,i64),advance:Option<fn(u64)>){
  for (step,age) in [300u64,150,0].into_iter().enumerate(){
   if step>0{if let Some(advance)=advance{advance(1_000_000);}}let(n,w)=clock();
   let mut frames=Vec::new();
   for (venue,prefix) in [(Venue::OkxSpot,"okx"),(Venue::BinanceFutures,"binance")]{
    for copy in ["a","b"]{frames.push(raw(venue,&format!("{prefix}-{copy}"),&self.base,n-age*1_000_000,w-age as i64,100+step as i64,step==0));}
   }
   let outputs=self.engine.receive_batch(frames,n,w).unwrap();
   for output in outputs{
    if let Output::Reset{venue,ref base,epoch}=output {
     self.provider.selection_reset(venue,base,epoch).unwrap();
     self.trace.push(serde_json::json!({"stage":"engine_reset","venue":venue,"base":base,"epoch":epoch}));
     continue
    }
    if matches!(output,Output::Frame(_)|Output::Book(_,_)){
     let input=self.provider.accept(output,n,w).unwrap();
     assert!(input.snapshot_generation().is_some(),"real Engine must supply witness");
     self.trace.push(serde_json::json!({"stage":"validated_frame","origin":"SYNTHETIC_TRANSPORT_FIXTURE","input":input}));
     self.sender.send(input.clone()).unwrap();
     self.trace.push(serde_json::json!({"stage":"bridge_enqueue","input_id":input.input_id()}));
     self.inputs.push(input);
    }
   }
  }
  assert_eq!(self.inputs.len(),2,"both venues must qualify through actual Engine");
 }
 pub fn dequeue(&mut self)->ValidatedInput{
  let input=self.receiver.receive().unwrap().expect("actual bounded bridge delivery");
  self.trace.push(serde_json::json!({"stage":"bridge_dequeue","input_id":input.input_id(),"metrics":self.health.metrics()}));input
 }
}

impl EngineFixture {
 pub(crate) fn r1306_enqueue(&self,input:ValidatedInput)->Result<(),Denial>{self.sender.send(input)}
 pub(crate) fn r1306_reset_from(&mut self,other:&EngineFixture){
  let permits=other.engine.authority.read().unwrap().clone();
  *self.engine.authority.write().unwrap()=permits.clone();
  for venue in [Venue::OkxSpot,Venue::BinanceFutures]{
   let p=permits.get(&key(venue,&self.base)).unwrap();
   self.provider.selection_reset(venue,&self.base,p.epoch).unwrap();
  }
 }
}

impl EngineFixture {
 // R1322 feeds synthetic transport through the real Engine/Provider; no raw-to-candidate shortcut.
 pub(crate) fn feed_r1322(&mut self,frames:Vec<RawFrame>,n:u64,w:i64)->Vec<ValidatedInput>{
  let outputs=self.engine.receive_batch(frames,n,w).unwrap();let mut inputs=Vec::new();
  for output in outputs{
   if let Output::Reset{venue,ref base,epoch}=output{self.provider.selection_reset(venue,base,epoch).unwrap();continue}
   if matches!(output,Output::Frame(_)|Output::Book(_,_)){
    inputs.push(self.provider.accept(output,n,w).unwrap());
   }
  }
  if !inputs.is_empty(){
   // Model producer and consumer progress without clearing sticky incidents.
   for input in &inputs{self.health.record_test_input(input.venue(),n);}
   self.health.heartbeat();self.health.watchdog(n);
  }
  inputs
 }
}
#[test] fn r1325_queued_selection_survives_newer_permit_same_live_snapshot(){
 let mut f=EngineFixture::for_single_venue("r1325-queue",Venue::OkxSpot);f.qualify_single_venue(Venue::OkxSpot);
 let input=f.inputs[0].clone();let(frame,update)=input.owner_parts_r1322();
 assert!(frame.source.as_ref().unwrap().selected_permit.is_some());
 let mut p=Provider{authority:f.provider.authority.clone(),clock_domain:"r1325-queue".into(),seen:HashMap::new()};
 {let mut a=p.authority.write().unwrap();let q=a.get_mut(&key(Venue::OkxSpot,"AAA")).unwrap();q.received_ns+=1_000_000;q.event_ms+=1;}
 let n=frame.received_ns+2_000_000;let w=frame.received_wall_ms+2;
 let accepted=p.bind(frame.clone(),update.clone(),n,w).unwrap();assert_eq!(accepted.snapshot_generation(),input.snapshot_generation());
 // Serializing source provenance must not serialize a live capability.
 let replay:RawFrame=serde_json::from_str(&serde_json::to_string(&frame).unwrap()).unwrap();assert!(replay.source.as_ref().unwrap().selected_permit.is_none());
 p.seen.clear();assert_eq!(p.bind(replay,update,n,w).unwrap_err(),Denial::Generation);
}
#[test] fn r1325_queued_selection_cannot_borrow_replacement_snapshot(){
 let mut f=EngineFixture::for_single_venue("r1325-queue",Venue::OkxSpot);f.qualify_single_venue(Venue::OkxSpot);
 let input=f.inputs[0].clone();let(frame,update)=input.owner_parts_r1322();
 let mut other=EngineFixture::for_single_venue("r1325-queue",Venue::OkxSpot);other.qualify_single_venue(Venue::OkxSpot);
 let replacement=other.inputs[0].witness.clone();
 let mut p=Provider{authority:f.provider.authority.clone(),clock_domain:"r1325-queue".into(),seen:HashMap::new()};
 p.authority.write().unwrap().get_mut(&key(Venue::OkxSpot,"AAA")).unwrap().snapshot_witness=replacement;
 assert_eq!(p.bind(frame.clone(),update,frame.received_ns,frame.received_wall_ms).unwrap_err(),Denial::Generation);
}


// Characterization, not permission to revive superseded authority.
#[test]
fn r1407_binance_full_snapshot_revokes_fresh_prior_event(){
 let mut f=EngineFixture::for_single_venue("r1407-witness",Venue::BinanceFutures);
 f.qualify_single_venue(Venue::BinanceFutures);
 let old=f.inputs[0].clone();
 let(old_frame,old_update)=old.owner_parts_r1322();
 let n=crate::ingress_runtime_r1205::mono_ns().max(old_frame.received_ns+1_000_000);
 let w=crate::ingress_runtime_r1205::wall_ms().max(old_frame.received_wall_ms+1);
 old.fresh(n,w).unwrap();
 let seq=old_update.book.sequence.unwrap()+1;
 let inputs=f.feed_r1322(["a","b"].into_iter().map(|copy|raw(Venue::BinanceFutures,&format!("binance-{copy}"),"AAA",n,w,seq,true)).collect(),n,w);
 assert_eq!(inputs.len(),1);
 let current=&inputs[0];
 assert_eq!(current.source,old.source,"no source switch");
 assert_eq!(current.selection_generation,old.selection_generation,"no epoch change");
 assert_ne!(current.book.sequence,old.book.sequence);
 assert_ne!(current.book.bids,old.book.bids,"distinct market content");
 assert!(old.fresh(n,w).is_ok(),"both timestamps still inside800ms");
 assert!(!old.witness.as_ref().unwrap().is_live());
 assert!(current.witness.as_ref().unwrap().is_live());
 assert_eq!(f.receiver.revalidate_at(&old,n,w),Err(Denial::Generation));
 assert!(f.receiver.superseded(&old));
 assert!(f.receiver.revalidate_at(current,n,w).is_ok());
 let mut p=Provider{authority:f.provider.authority.clone(),clock_domain:"r1407-witness".into(),seen:HashMap::new()};
 assert_eq!(p.bind(old_frame,old_update,n,w).unwrap_err(),Denial::Generation);
 println!("R1407_WITNESS actual Engine->Provider->Bridge: fresh distinct Binance snapshot A denied after B, same source and epoch; B valid");
}
