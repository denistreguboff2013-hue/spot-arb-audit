//! R155: exclusive source-owned PAPER research lane; not installed runtime.
 //! A checked public ticker pair, never a caller Decision, gates new modeled
 //! positions. Its only portfolio is SupervisedStrategyPaper::portfolio.
 //! Socket/TLS peer authentication and exchange fills are NOT established.
use crate::{Book,Venue,owner_index};
use crate::decision::RouteSpec;
use crate::native_paper_strategy_frame_review::{
 PublicEntryMarketSnapshot,StrategyFrameError,StrategyFrameEvidence,SupervisedStrategyPaper,
};
use crate::native_public_cold_boot_r130_review::PublicColdBootR130;
use crate::native_public_turnover_ws_r123_review::{PublicWsEnvelope,parse_ws_ticker};
use crate::native_public_ws_owner_topology_r126_review::{
 PublicTickerTopologyR126,TickerSubscription,
};
use std::collections::BTreeMap;
#[derive(Clone,Debug)]
struct Sample{shard:usize,epoch:u64,sequence:u64,received_ms:i64,source_ms:i64,raw:String}
pub struct SourcePaperLaneR155{
 pub strategy:SupervisedStrategyPaper,
 cold:PublicColdBootR130,
 shards:BTreeMap<String,usize>,
 spot:BTreeMap<String,Sample>,
 future:BTreeMap<String,Sample>,
 ready:BTreeMap<String,PublicEntryMarketSnapshot>,
 pub mature_pairs:u64,pub denied_pairs:u64,
}
impl SourcePaperLaneR155{
 pub fn new(strategy:SupervisedStrategyPaper,shards:BTreeMap<String,usize>)
  ->Result<Self,StrategyFrameError>{
  if shards.is_empty()||shards.len()>512||shards.keys().any(|x|
     x.is_empty()||x.len()>32||!x.bytes().all(|b|b.is_ascii_uppercase()||b.is_ascii_digit())){
   return Err(StrategyFrameError::Config)
  }
  Ok(Self{strategy,cold:PublicColdBootR130::default(),shards,
    spot:BTreeMap::new(),future:BTreeMap::new(),ready:BTreeMap::new(),
    mature_pairs:0,denied_pairs:0})
 }
 pub fn ready_bases(&self)->usize{self.ready.len()}
 fn restore_sample(&mut self,base:&str,venue:Venue,old:Option<Sample>){
  let map=match venue{Venue::OkxSpot=>&mut self.spot,
   Venue::BinanceFutures=>&mut self.future};
  if let Some(sample)=old{map.insert(base.to_owned(),sample);}
  else{map.remove(base);}
 }
 /// Bounded caller-provided public WS evidence from the R154 network task.
 /// Rejection revokes entry-ready snapshot and never changes portfolio.
 pub fn on_ticker(
  &mut self,base:&str,venue:Venue,shard:usize,epoch:u64,sequence:u64,
  received_ms:i64,raw:&str,now_ms:i64
 )->bool{
  let deny=|s:&mut Self|{
   s.denied_pairs+=1;s.ready.remove(base);false
  };
  if self.shards.get(base)!=Some(&shard)||epoch==0||sequence==0
    ||raw.len()>32_768||raw.is_empty(){
   return deny(self)
  }
  // R170: never overwrite retained high-water cache before route/payload/clock
  // and old-epoch/sequence validation. R124 continuity checks run later.
  let url=match venue{Venue::OkxSpot=>"wss://ws.okx.com/ws/v5/public".to_owned(),
   Venue::BinanceFutures=>format!("wss://fstream.binance.com/market/ws/{}usdt@ticker",
     base.to_ascii_lowercase())};
  let parsed=parse_ws_ticker(now_ms,PublicWsEnvelope{
   venue,url:&url,base,connection_epoch:epoch,sequence,received_wall_ms:received_ms,body:raw
  });
  let observed=match parsed{Ok(v)=>v,Err(_)=>return deny(self)};
  let previous=match venue{Venue::OkxSpot=>self.spot.get(base),
   Venue::BinanceFutures=>self.future.get(base)}.cloned();
  if let Some(old)=&previous{
   if epoch<old.epoch || (epoch==old.epoch && sequence<old.sequence)
    ||received_ms<old.received_ms || observed.source_ms<old.source_ms{
    self.denied_pairs+=1;return false // old buffered payload NEVER evicts cache
   }
   if epoch==old.epoch && sequence==old.sequence{
    if raw==old.raw && received_ms==old.received_ms{return false}
    return deny(self) // rewritten identical sequence, preserve high-water
   }
   if epoch>old.epoch{self.ready.remove(base);} // reconnect requires rewarm
  }
  let sample=Sample{shard,epoch,sequence,received_ms,
   source_ms:observed.source_ms,raw:raw.into()};
  match venue{Venue::OkxSpot=>{self.spot.insert(base.into(),sample);},
   Venue::BinanceFutures=>{self.future.insert(base.into(),sample);}
  }
  let (Some(a),Some(b))=(self.spot.get(base),self.future.get(base))else{
   self.ready.remove(base);return false
  };
  let owner=owner_index(base,12);
  let spot_url="wss://ws.okx.com/ws/v5/public".to_owned();
  let fut_url=format!("wss://fstream.binance.com/market/ws/{}usdt@ticker",
   base.to_ascii_lowercase());
  let spot_id=format!("okx-shard-{}",a.shard);
  let fut_id=format!("binance-shard-{}",b.shard);
  let topo=PublicTickerTopologyR126::new_with_owner_count(owner,12,&[
   TickerSubscription{connection_id:spot_id.clone(),owner,
    venue:Venue::OkxSpot,base:base.into(),url:spot_url.clone()},
   TickerSubscription{connection_id:fut_id.clone(),owner,
    venue:Venue::BinanceFutures,base:base.into(),url:fut_url.clone()},
  ]);
  let o=PublicWsEnvelope{venue:Venue::OkxSpot,url:&spot_url,base,
   connection_epoch:a.epoch,sequence:a.sequence,received_wall_ms:a.received_ms,
   body:&a.raw};
  let f=PublicWsEnvelope{venue:Venue::BinanceFutures,url:&fut_url,base,
   connection_epoch:b.epoch,sequence:b.sequence,received_wall_ms:b.received_ms,
   body:&b.raw};
  if topo.as_ref().map_or(true,|x|x.check_pair(&spot_id,&fut_id,o,f).is_err()){
   self.restore_sample(base,venue,previous.clone());
   return deny(self)
  }
  match self.cold.observe(now_ms,o,f){
   Ok(Some(pair))=>{
    self.ready.insert(base.into(),pair.snapshot);
    self.mature_pairs+=1;true
   },
   Ok(None)=>{self.ready.remove(base);false},
   Err(_)=>{
    self.restore_sample(base,venue,previous);
    deny(self)
   },
  }
 }
 pub fn invalidate(&mut self,base:&str,venue:Venue){
  self.ready.remove(base);
  match venue{Venue::OkxSpot=>{self.spot.remove(base);},
   Venue::BinanceFutures=>{self.future.remove(base);}
  }
 }
 /// This is the SOLE virtual portfolio updater for the R155 profile.
 /// There is no parallel legacy PaperPortfolio.on_frame or external Decision.
 pub fn on_book_frame(
  &mut self,base:&str,venue:Venue,ns:u64,wall_ms:i64,
  spot:Option<&Book>,fut:Option<&Book>,route:Option<&RouteSpec>
 )->Result<StrategyFrameEvidence,StrategyFrameError>{
  self.strategy.on_source_bound_frame_review_with_market(
   base,venue,ns,wall_ms,spot,fut,route,self.ready.get(base))
 }
}

#[cfg(test)]
mod tests{
 use super::*;
 use crate::native_trading_config::NativeTradingConfig;
 const T:i64=1_791_783_600_000;
 fn cfg()->NativeTradingConfig{
  let original=include_str!("../config/native_trading_operator_baseline_r64.yaml");
  let adjusted=original.replacen("scale_in_hints:\n  enabled: true",
   "scale_in_hints:\n  enabled: false",1)
    .replacen("analytics:\n  enabled: true",
      "analytics:\n  enabled: false",1);
  assert_ne!(original,adjusted);
  NativeTradingConfig::parse(&adjusted).unwrap()
 }
 fn lane()->SourcePaperLaneR155{
  let strategy=SupervisedStrategyPaper::new(cfg(),T).unwrap();
  SourcePaperLaneR155::new(strategy,
   BTreeMap::from([("BTC".into(),4)])).unwrap()
 }
 fn books(ns:u64)->(Book,Book){
  let spot=Book{bids:vec![(99.,1000.)],asks:vec![(100.,1000.)],
   received_ns:ns,received_wall_ms:T,exchange_ts_ms:None,changed_ns:ns,
   epoch:1,sequence:Some(11)};
  let futures=Book{bids:vec![(101.,1000.)],asks:vec![(102.,1000.)],
   received_ns:ns,received_wall_ms:T,exchange_ts_ms:None,changed_ns:ns,
   epoch:1,sequence:Some(11)};
  (spot,futures)
 }
 fn route(ns:u64)->RouteSpec{
  let lim=crate::decision::Limits{
   min_amount:"0.001".into(),step:"0.001".into(),
   min_cost:"1".into(),max_amount:Some("1000".into())};
  RouteSpec{buy_exchange:"okx".into(),sell_exchange:"binance".into(),
   label:"okx-to-binance".into(),buy:lim.clone(),sell:lim,loaded_ns:ns,
   metadata_state:"SIZE_METADATA_READY".into()}
 }
 fn okx(ts:i64)->String{
  format!(r#"{{"arg":{{"channel":"tickers","instId":"BTC-USDT"}},"data":[{{"instType":"SPOT","instId":"BTC-USDT","volCcy24h":"200000","ts":"{ts}"}}]}}"#)
 }
 fn binance(ts:i64)->String{
  format!(r#"{{"e":"24hrTicker","E":{ts},"s":"BTCUSDT","q":"300000","v":"1","O":{},"C":{ts},"st":1}}"#,ts-86_400_000)
 }
 #[test]
 fn r155_no_ticker_cannot_open_warm_two_venue_pair_allows_one_source_entry(){
  let mut lane=lane();let ns=40_000_000_000;
  let (spot,fut)=books(ns);let route=route(ns);
  let before=lane.on_book_frame("BTC",Venue::OkxSpot,ns,T,
   Some(&spot),Some(&fut),Some(&route));
  assert_eq!(before.err(),Some(StrategyFrameError::InvalidFrame));
  assert_eq!(lane.strategy.portfolio.stats.entries,0);
  let a=okx(T-1200);let b=binance(T-1200);
  assert!(!lane.on_ticker("BTC",Venue::OkxSpot,4,3,10,T,&a,T));
  assert!(!lane.on_ticker("BTC",Venue::BinanceFutures,4,5,10,T,&b,T));
  assert_eq!(lane.ready_bases(),0);
  assert_eq!(lane.on_book_frame("BTC",Venue::OkxSpot,ns,T,
   Some(&spot),Some(&fut),Some(&route)).err(),
   Some(StrategyFrameError::InvalidFrame));
  assert_eq!(lane.strategy.portfolio.stats.entries,0);
  let a2=okx(T-800);let b2=binance(T-800);
  assert!(!lane.on_ticker("BTC",Venue::OkxSpot,4,3,11,T,&a2,T));
  assert!(lane.on_ticker("BTC",Venue::BinanceFutures,4,5,11,T,&b2,T));
  assert_eq!(lane.ready_bases(),1);
  let allowed=lane.on_book_frame("BTC",Venue::OkxSpot,ns,T,
   Some(&spot),Some(&fut),Some(&route)).unwrap();
  assert!(allowed.permitted_paper_legacy_model_update);
  assert_eq!(lane.strategy.portfolio.stats.entries,1);
  assert_eq!(lane.strategy.portfolio.max_daily_entries,3000);
  assert!(!allowed.proof_of_real_execution);
  assert_eq!(allowed.config_source_sha256,cfg().sha256);
  lane.invalidate("BTC",Venue::BinanceFutures);
  assert_eq!(lane.ready_bases(),0);
  assert_eq!(lane.strategy.portfolio.stats.entries,1);
 }
 #[test]
 fn r155_wrong_shard_route_or_futures_payload_cannot_open(){
  let mut lane=lane();let a=okx(T-1200);let b=binance(T-1200);
  assert!(!lane.on_ticker("BTC",Venue::OkxSpot,11,3,10,T,&a,T));
  assert_eq!(lane.denied_pairs,1);
  assert!(!lane.on_ticker("BTC",Venue::OkxSpot,4,3,10,T,&a,T));
  let invalid=b.replace("\"q\":\"300000\"",
    "\"v\":\"300000\"");
  assert!(!lane.on_ticker("BTC",Venue::BinanceFutures,4,5,10,T,&invalid,T));
  assert_eq!(lane.ready_bases(),0);
  assert!(lane.denied_pairs>=2);
  let (s,f)=books(40_000_000_000);
  assert_eq!(lane.on_book_frame("BTC",Venue::OkxSpot,40_000_000_000,T,
   Some(&s),Some(&f),Some(&route(40_000_000_000))).err(),
   Some(StrategyFrameError::InvalidFrame));
  assert_eq!(lane.strategy.portfolio.stats.entries,0);
 }
 // R170 regression: state in SourcePaperLaneR155 must not be poisoned
 // BEFORE R124/R130 exchange-ticker continuity sees a delayed envelope.
 fn r170_warm()->SourcePaperLaneR155{
  let mut x=lane();
  let a=okx(T-1200);let b=binance(T-1200);
  let a2=okx(T-800);let b2=binance(T-800);
  assert!(!x.on_ticker("BTC",Venue::OkxSpot,4,3,10,T,&a,T));
  assert!(!x.on_ticker("BTC",Venue::BinanceFutures,4,5,10,T,&b,T));
  assert!(!x.on_ticker("BTC",Venue::OkxSpot,4,3,11,T,&a2,T));
  assert!(x.on_ticker("BTC",Venue::BinanceFutures,4,5,11,T,&b2,T));
  assert_eq!(x.ready_bases(),1);x
 }
 #[test]
 fn r170_old_epoch_or_sequence_does_not_replace_high_water(){
  let mut x=r170_warm();
  let old=okx(T-400);let same=okx(T-800);
  assert!(!x.on_ticker("BTC",Venue::OkxSpot,4,2,99,T,&old,T));
  assert!(!x.on_ticker("BTC",Venue::OkxSpot,4,3,10,T,&same,T));
  assert_eq!(x.spot["BTC"].epoch,3);
  assert_eq!(x.spot["BTC"].sequence,11);
  assert_eq!(x.spot["BTC"].source_ms,T-800);
  assert_eq!(x.ready_bases(),1);
 }
 #[test]
 fn r170_duplicate_exact_does_not_reopen_or_mutate(){
  let mut x=r170_warm();
  let original=okx(T-800);let before=x.mature_pairs;
  assert!(!x.on_ticker("BTC",Venue::OkxSpot,4,3,11,T,&original,T));
  assert_eq!(x.mature_pairs,before);
  assert_eq!(x.ready_bases(),1);
  let forged=original.replace("200000","900000");
  assert!(!x.on_ticker("BTC",Venue::OkxSpot,4,3,11,T,&forged,T));
  assert_eq!(x.spot["BTC"].raw,original);
  assert_eq!(x.ready_bases(),0);
 }
 #[test]
 fn r170_invalid_new_epoch_does_not_poison_valid_cache(){
  let mut x=r170_warm();
  let invalid=r#"{"garbled":true}"#;
  assert!(!x.on_ticker("BTC",Venue::OkxSpot,4,4,1,T,invalid,T));
  assert_eq!(x.spot["BTC"].epoch,3);
  assert_eq!(x.spot["BTC"].sequence,11);
  assert_eq!(x.ready_bases(),0);
 }
 #[test]
 fn r170_new_epoch_demotes_and_two_venues_rewarm(){
  let mut x=r170_warm();
  let a=okx(T-500);let b=binance(T-500);
  assert!(!x.on_ticker("BTC",Venue::OkxSpot,4,4,1,T,&a,T));
  assert_eq!(x.ready_bases(),0);
  assert!(!x.on_ticker("BTC",Venue::BinanceFutures,4,6,1,T,&b,T));
  assert_eq!(x.ready_bases(),0);
  let a2=okx(T-300);let b2=binance(T-300);
  assert!(!x.on_ticker("BTC",Venue::OkxSpot,4,4,2,T,&a2,T));
  assert!(x.on_ticker("BTC",Venue::BinanceFutures,4,6,2,T,&b2,T));
  assert_eq!(x.ready_bases(),1);
  assert_eq!(x.spot["BTC"].epoch,4);
  assert_eq!(x.future["BTC"].epoch,6);
 }
}
