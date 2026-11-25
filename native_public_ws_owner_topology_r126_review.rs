//! Astra R126: fail-closed OFFLINE proposed public-ticker transport topology.
//! Real transport.rs owns book streams, not these ticker channels.
//! This does not open sockets, verify TLS identities or authorize orders.
use crate::{Venue,owner_index};
use crate::native_public_turnover_ws_r123_review::PublicWsEnvelope;
use std::collections::{BTreeMap,BTreeSet};
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct TickerSubscription{
 pub connection_id:String,pub owner:usize,pub venue:Venue,
 pub base:String,pub url:String,
}
#[derive(Debug,Clone,PartialEq,Eq)]
pub enum TopologyError{Shape,Owner,Duplicate,MissingPair,Source,Epoch}
#[derive(Clone,Debug)]
pub struct PublicTickerTopologyR126{
 owner:usize,owner_count:usize,by_venue:BTreeMap<(String,u8),(String,String)>,
}
fn canonical(base:&str,venue:Venue)->String{
 match venue{
  Venue::OkxSpot=>"wss://ws.okx.com:8443/ws/v5/public".into(),
  Venue::BinanceFutures=>format!("wss://fstream.binance.com/market/ws/{}usdt@ticker",
    base.to_ascii_lowercase())
 }
}
fn venue_id(v:Venue)->u8{match v{Venue::OkxSpot=>1,Venue::BinanceFutures=>2}}
fn good_base(b:&str)->bool{
 !b.is_empty()&&b.len()<=32&&b.bytes().all(|v|v.is_ascii_uppercase()||v.is_ascii_digit())
}
impl PublicTickerTopologyR126{
 /// Legacy R126 tests and review consumers retain exactly three owners.
 pub fn new(owner:usize,subscriptions:&[TickerSubscription])->Result<Self,TopologyError>{
  Self::new_with_owner_count(owner,3,subscriptions)
 }
 /// R152: the candidate R21 book pipeline has 12 owners; a ticker pair
 /// must use that SAME hash modulus. This only validates source-shaped
 /// envelopes; it does not claim real transport ownership.
 pub fn new_with_owner_count(
  owner:usize,owner_count:usize,subscriptions:&[TickerSubscription]
 )->Result<Self,TopologyError>{
  if !(1..=64).contains(&owner_count)||owner>=owner_count
   ||subscriptions.is_empty()||subscriptions.len()>512{
   return Err(TopologyError::Shape)
  }
  let mut by_venue=BTreeMap::new();
  let mut ids=BTreeMap::<String,(u8,String)>::new();
  let mut bases=BTreeSet::new();
  for s in subscriptions{
   if !good_base(&s.base)||s.connection_id.is_empty()||s.connection_id.len()>128
     ||(s.url!=canonical(&s.base,s.venue)
       &&!(s.venue==Venue::OkxSpot
         &&s.url=="wss://ws.okx.com/ws/v5/public")){
      return Err(TopologyError::Source)
    }
   if s.owner!=owner||owner_index(&s.base,owner_count)!=owner{
    return Err(TopologyError::Owner)
   }
   let v=venue_id(s.venue);
   let key=(s.base.clone(),v);
   if by_venue.insert(key,(s.connection_id.clone(),s.url.clone())).is_some(){
    return Err(TopologyError::Duplicate)
   }
   if let Some(old)=ids.get(&s.connection_id){
    if old.0!=v||old.1!=s.url{return Err(TopologyError::Duplicate)}
   }else{
    ids.insert(s.connection_id.clone(),(v,s.url.clone()));
   }
   bases.insert(s.base.clone());
  }
  if bases.iter().any(|b|!by_venue.contains_key(&(b.clone(),1))
   ||!by_venue.contains_key(&(b.clone(),2))){return Err(TopologyError::MissingPair)}
  Ok(Self{owner,owner_count,by_venue})
 }
 pub fn bases(&self)->usize{self.by_venue.len()/2}
 pub fn check_pair(&self,spot_id:&str,fut_id:&str,
  spot:PublicWsEnvelope<'_>,fut:PublicWsEnvelope<'_>)->Result<(),TopologyError>{
  if spot.venue!=Venue::OkxSpot||fut.venue!=Venue::BinanceFutures
   ||spot.base!=fut.base||owner_index(spot.base,self.owner_count)!=self.owner{
   return Err(TopologyError::Owner)
  }
  for (e,id) in [(spot,spot_id),(fut,fut_id)]{
   if e.connection_epoch==0||e.sequence==0{return Err(TopologyError::Epoch)}
   let record=self.by_venue.get(&(e.base.to_owned(),venue_id(e.venue)))
     .ok_or(TopologyError::Source)?;
   if record.0!=id||record.1!=e.url{return Err(TopologyError::Source)}
  }
  Ok(())
 }
}
#[cfg(test)]
mod tests{
 use super::*;
 fn pair()->(usize,Vec<TickerSubscription>){
  let owner=owner_index("BTC",3);
  (owner,vec![
   TickerSubscription{connection_id:"spot-owner".into(),owner,venue:Venue::OkxSpot,
    base:"BTC".into(),url:canonical("BTC",Venue::OkxSpot)},
   TickerSubscription{connection_id:"futures-owner".into(),owner,
    venue:Venue::BinanceFutures,base:"BTC".into(),
    url:canonical("BTC",Venue::BinanceFutures)}
  ])
 }
 fn envelope<'a>(venue:Venue,base:&'a str,url:&'a str)->PublicWsEnvelope<'a>{
  PublicWsEnvelope{venue,url,base,connection_epoch:1,sequence:1,
    received_wall_ms:1_791_000_000_000,body:"{}"}
 }
 #[test]fn r126_topology_per_base_two_trusted_routes_in_review(){
  let (owner,rows)=pair();
  let registry=PublicTickerTopologyR126::new(owner,&rows).unwrap();
  assert_eq!(registry.bases(),1);
  let a=envelope(Venue::OkxSpot,"BTC",&rows[0].url);
  let b=envelope(Venue::BinanceFutures,"BTC",&rows[1].url);
  assert_eq!(registry.check_pair("spot-owner","futures-owner",a,b),Ok(()));
  assert_eq!(registry.check_pair("other","futures-owner",a,b),
   Err(TopologyError::Source));
 }
 #[test]fn r126_rejects_wrong_owner_duplicate_and_partial_pair(){
  let (owner,rows)=pair();
  assert_eq!(PublicTickerTopologyR126::new((owner+1)%3,&rows).err(),
   Some(TopologyError::Owner));
  assert_eq!(PublicTickerTopologyR126::new(owner,&rows[..1]).err(),
   Some(TopologyError::MissingPair));
  let mut duplicated=rows.clone();
  duplicated.push(rows[0].clone());
  assert_eq!(PublicTickerTopologyR126::new(owner,&duplicated).err(),
   Some(TopologyError::Duplicate));
  let mut wrong=rows.clone();
  wrong[1].connection_id=wrong[0].connection_id.clone();
  assert_eq!(PublicTickerTopologyR126::new(owner,&wrong).err(),
   Some(TopologyError::Duplicate));
  let mut wrong=rows.clone();
  wrong[1].url="wss://fstream.binance.com/ws/btcusdt@ticker".into();
  assert_eq!(PublicTickerTopologyR126::new(owner,&wrong).err(),
   Some(TopologyError::Source));
 }
 #[test]fn r152_twelve_owner_candidate_parity_and_invalid_modulus_denied(){
  let owner=owner_index("BTC",12);
  let rows=vec![
   TickerSubscription{connection_id:"12-okx".into(),owner,
    venue:Venue::OkxSpot,base:"BTC".into(),
    url:canonical("BTC",Venue::OkxSpot)},
   TickerSubscription{connection_id:"12-bn".into(),owner,
    venue:Venue::BinanceFutures,base:"BTC".into(),
    url:canonical("BTC",Venue::BinanceFutures)},
  ];
  let topo=PublicTickerTopologyR126::new_with_owner_count(owner,12,&rows).unwrap();
  assert_eq!(topo.bases(),1);
  let spot=envelope(Venue::OkxSpot,"BTC",&rows[0].url);
  let fut=envelope(Venue::BinanceFutures,"BTC",&rows[1].url);
  assert_eq!(topo.check_pair("12-okx","12-bn",spot,fut),Ok(()));
  assert_eq!(topo.check_pair("forged","12-bn",spot,fut),Err(TopologyError::Source));
  assert_eq!(PublicTickerTopologyR126::new_with_owner_count(owner,0,&rows).err(),
   Some(TopologyError::Shape));
  assert_eq!(PublicTickerTopologyR126::new_with_owner_count(owner,65,&rows).err(),
   Some(TopologyError::Shape));
  assert_eq!(PublicTickerTopologyR126::new_with_owner_count(
   (owner+1)%12,12,&rows).err(),Some(TopologyError::Owner));
 }
 #[test]fn r158_new_okx_ws_443_allowed_but_forged_source_denied(){
  let owner=owner_index("BTC",12);
  let mut rows=vec![
   TickerSubscription{connection_id:"okx443".into(),owner,venue:Venue::OkxSpot,
    base:"BTC".into(),url:"wss://ws.okx.com/ws/v5/public".into()},
   TickerSubscription{connection_id:"bnmarket".into(),owner,venue:Venue::BinanceFutures,
    base:"BTC".into(),url:"wss://fstream.binance.com/market/ws/btcusdt@ticker".into()},
  ];
  let topo=PublicTickerTopologyR126::new_with_owner_count(owner,12,&rows).unwrap();
  let a=envelope(Venue::OkxSpot,"BTC",&rows[0].url);
  let b=envelope(Venue::BinanceFutures,"BTC",&rows[1].url);
  assert_eq!(topo.check_pair("okx443","bnmarket",a,b),Ok(()));
  rows[0].url="wss://ws.okx.com:444/ws/v5/public".into();
  assert_eq!(PublicTickerTopologyR126::new_with_owner_count(owner,12,&rows).err(),
   Some(TopologyError::Source));
 }
 #[test]fn r126_rejects_spoofed_source_and_zero_epoch(){
  let (owner,rows)=pair();
  let registry=PublicTickerTopologyR126::new(owner,&rows).unwrap();
  let a=envelope(Venue::OkxSpot,"BTC",&rows[0].url);
  let mut b=envelope(Venue::BinanceFutures,"BTC",&rows[1].url);
  b.connection_epoch=0;
  assert_eq!(registry.check_pair("spot-owner","futures-owner",a,b),
   Err(TopologyError::Epoch));
  let b=envelope(Venue::BinanceFutures,"BTC","wss://other.invalid/");
  assert_eq!(registry.check_pair("spot-owner","futures-owner",a,b),
   Err(TopologyError::Source));
 }
}
