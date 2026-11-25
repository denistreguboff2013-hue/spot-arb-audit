//! R1321 contract review reused by R1325 simulated PAPER; never an exchange-order capability.
//! No transport, journal, portfolio or exchange-order adapter. Internal issuers bind synthetic economics.
//! Fixture attestations explicitly describe simulated economics, never venue/account truth.
use super::{ValidatedInput,BridgeReceiver,Health,Denial,Venue,MarketType};
#[cfg(test)] use super::order_admission;
use crate::native_paper_hedge_r1296 as policy;
use crate::fill_economics::decimal_to_atoms;
use std::collections::HashSet;

struct OfflineRules {
 identity:policy::Identity,venue:Venue,metadata_generation:u64,price_tick:String,seal:String,
}
impl OfflineRules {
 fn digest(&self)->Result<String,Denial>{
  policy::digest(&("R1321_SYNTHETIC_ONLY",&self.identity,self.venue,self.metadata_generation,&self.price_tick)).map_err(|_|Denial::Rules)
 }
 // Internal simulated-economics claim; R1325 validates current metadata and runtime safety before use.
 fn fixture(identity:policy::Identity,venue:Venue,metadata_generation:u64,tick:&str)->Self{
  let mut r=Self{identity,venue,metadata_generation,price_tick:tick.into(),seal:String::new()};
  r.seal=r.digest().unwrap();r
 }
}
struct ReviewRequest {
 now_ns:u64,wall_ms:i64,quantity:f64,price:f64,
 real_off:bool,kill_engaged:bool,owner_healthy:bool,storage_healthy:bool,
}
/// Deliberately not Clone/Deserialize. This is a review receipt, not an Owner input.
#[derive(Debug)]
struct ReviewedInput {
 input:ValidatedInput,rules_digest:String,metadata_generation:u64,quantity:f64,price:f64,
}
struct AdmissionReview {
 identity_digest:String,metadata_generation:u64,capacity:usize,
 seen:HashSet<String>,terminal:Option<Denial>,
}
impl AdmissionReview {
 fn fixture(identity:&policy::Identity,metadata_generation:u64,capacity:usize)->Self{
  Self{identity_digest:identity.hash().unwrap(),metadata_generation,capacity,seen:HashSet::new(),terminal:None}
 }
 fn review(&mut self,input:ValidatedInput,rules:&OfflineRules,receiver:&BridgeReceiver,health:&Health,q:ReviewRequest)->Result<ReviewedInput,Denial>{
  if let Some(e)=&self.terminal{return Err(e.clone())}
  let result=self.check(input,rules,receiver,health,q);
  if let Err(e)=&result{self.terminal=Some(e.clone());}
  result
 }
 fn check(&mut self,input:ValidatedInput,rules:&OfflineRules,receiver:&BridgeReceiver,health:&Health,q:ReviewRequest)->Result<ReviewedInput,Denial>{
  super::admission_prerequisites(health,q.owner_healthy,q.storage_healthy,q.real_off,q.kill_engaged,true)?;
  // Duplicate identity excludes payload: conflicting content must not mint a new receipt.
  let source=&input.source_provenance;
  let key=policy::digest(&(input.venue,&input.instrument,
   &input.source,input.connection_generation,input.selection_generation,input.sequence)).map_err(|_|Denial::Raw)?;
  if self.seen.contains(&key){return Err(Denial::Duplicate)}
  if self.seen.len()>=self.capacity{return Err(Denial::Overflow)}
  rules.identity.validate().map_err(|_|Denial::Rules)?;
  if rules.seal!=rules.digest()?||rules.identity.hash().map_err(|_|Denial::Rules)?!=self.identity_digest{return Err(Denial::Rules)}
  if rules.metadata_generation==0||rules.metadata_generation!=self.metadata_generation{return Err(Denial::Generation)}
  let profile=&rules.identity.profile;
  if input.clock_domain!=rules.identity.clock_domain{return Err(Denial::Clock)}
  if input.instrument!=profile.base||input.venue!=rules.venue{return Err(Denial::Rules)}
  match (&input.market_type,input.venue){
   (MarketType::Spot,Venue::OkxSpot) if input.venue_symbol==profile.spot_symbol=>{},
   (MarketType::Futures,Venue::BinanceFutures) if input.venue_symbol==profile.futures_symbol=>{},
   _=>return Err(Denial::Rules),
  }
  if q.now_ns<profile.metadata_loaded_ns||q.now_ns-profile.metadata_loaded_ns>profile.metadata_ttl_ns{return Err(Denial::Stale)}
  profile.validate_quantity(q.quantity,q.price,true).map_err(|_|Denial::Rules)?;
  let tick=decimal_to_atoms(&rules.price_tick).map_err(|_|Denial::Rules)?;
  let price=policy::quantity_atoms(q.price).map_err(|_|Denial::Rules)?;
  if tick==0||price%tick!=0{return Err(Denial::Rules)}
  let ram_selected=source.accepted_id==0&&source.journal_id.is_empty()&&source.selected_permit.is_some();
  let durable_receipt=source.accepted_id>0&&source.journal_id.len()==64;
  if (!ram_selected&&!durable_receipt)||source.payload_sha256.len()!=64||
   !source.payload_sha256.bytes().all(|b|b.is_ascii_hexdigit())||source.session_id!=input.source||
   source.connection_epoch!=input.connection_generation||source.selection_epoch!=input.selection_generation{return Err(Denial::Raw)}
  let witness=input.witness.as_ref().ok_or(Denial::Generation)?;
  if input.snapshot_generation!=Some(witness.generation())||input.snapshot_generation().is_none(){return Err(Denial::Generation)}
  if !input.hot_at_validation||!input.fresh_at_validation{return Err(Denial::Hot)}
  receiver.revalidate_at(&input,q.now_ns,q.wall_ms)?;
  self.seen.insert(key);
  Ok(ReviewedInput{input,rules_digest:rules.seal.clone(),metadata_generation:rules.metadata_generation,quantity:q.quantity,price:q.price})
 }
}
#[cfg(test)]
#[path="native_paper_admission_r1321_tests.rs"]
mod tests;

pub(super) fn review_pair_r1322(inputs:&[ValidatedInput;2],identity:&policy::Identity,generation:u64,ticks:&[String;2],quantity:f64,receiver:&BridgeReceiver,health:&Health,now:u64,wall:i64)->Result<[String;2],Denial>{
 identity.validate().map_err(|_|Denial::Rules)?;
 let mut hashes=[String::new(),String::new()];
 for (index,input) in inputs.iter().enumerate(){
  let rules=OfflineRules::fixture(identity.clone(),input.venue,generation,&ticks[index]);
  let mut gate=AdmissionReview::fixture(identity,generation,1);
  let price=if input.venue==Venue::OkxSpot{input.book.asks.first()}else{input.book.bids.first()}.ok_or(Denial::Raw)?.0;
  let receipt=gate.review(input.clone(),&rules,receiver,health,ReviewRequest{now_ns:now,wall_ms:wall,quantity,price,real_off:true,kill_engaged:true,owner_healthy:true,storage_healthy:true})?;
  hashes[index]=receipt.rules_digest;
 }
 Ok(hashes)
}
