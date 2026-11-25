//! Astra R130: cold-boot public 24h ticker gate, isolated model ONLY.
//! A first caller-supplied frame from each venue is NEVER entry eligible.
//! Need second distinct exchange timestamps, same epochs and rising sequences.
//! No attested external source, TLS peer or persistent/independent replay floor.
use std::collections::BTreeMap;
use crate::native_public_turnover_ws_r123_review::{
 PublicWsEnvelope,PublicWsPair,PublicWsError,
};
use crate::native_public_turnover_ws_continuity_r124_review::WsTurnoverContinuityR124;
#[derive(Clone,Copy,Debug)]
struct PairClock{
 spot_epoch:u64,fut_epoch:u64,
 spot_seq:u64,fut_seq:u64,
 spot_ts:i64,fut_ts:i64,
}
#[derive(Clone,Copy,Debug)]
struct State{last:PairClock,ready:bool}
#[derive(Default)]
pub struct PublicColdBootR130{
 continuity:WsTurnoverContinuityR124,
 state:BTreeMap<String,State>,
}
fn clock(m:&PublicWsPair)->PairClock{
 PairClock{spot_epoch:m.okx.epoch,fut_epoch:m.binance.epoch,
  spot_seq:m.okx.sequence,fut_seq:m.binance.sequence,
  spot_ts:m.okx.source_ms,fut_ts:m.binance.source_ms}
}
impl PublicColdBootR130{
 pub fn observe(
  &mut self,now_ms:i64,spot:PublicWsEnvelope<'_>,fut:PublicWsEnvelope<'_>
 )->Result<Option<PublicWsPair>,PublicWsError>{
  let m=self.continuity.accept(now_ms,spot,fut)?;
  let k=m.snapshot.base.clone();
  let t=clock(&m);
  let previous=self.state.get(&k).copied();
  let permit=match previous{
   None=>false,
   Some(previous) if previous.last.spot_epoch!=t.spot_epoch
    ||previous.last.fut_epoch!=t.fut_epoch=>false,
   Some(previous) if previous.ready=>true,
   Some(previous)=>{
    previous.last.spot_seq<t.spot_seq&&previous.last.fut_seq<t.fut_seq
     &&previous.last.spot_ts<t.spot_ts&&previous.last.fut_ts<t.fut_ts
   }
  };
  if permit{
   self.state.insert(k,State{last:t,ready:true});
   return Ok(Some(m))
  }
  if previous.is_none()||previous.is_some_and(|v|
    v.last.spot_epoch!=t.spot_epoch||v.last.fut_epoch!=t.fut_epoch){
   self.state.insert(k,State{last:t,ready:false});
  }
  Ok(None)
 }
 pub fn ready(&self,base:&str)->bool{
  self.state.get(base).is_some_and(|v|v.ready)
 }
 pub fn tracked(&self)->usize{self.continuity.tracked()}
}
#[cfg(test)]
mod tests{
 use super::*;
 use crate::Venue;
 const N:i64=1_791_783_600_000;
 fn okx(ts:i64)->String{
  format!(r#"{{"arg":{{"channel":"tickers","instId":"BTC-USDT"}},"data":[{{"instType":"SPOT","instId":"BTC-USDT","volCcy24h":"200000","ts":"{ts}"}}]}}"#)
 }
 fn bn(ts:i64)->String{
  format!(r#"{{"e":"24hrTicker","E":{ts},"s":"BTCUSDT","q":"300000","O":{},"C":{ts},"st":1}}"#,ts-86_400_000)
 }
 fn o<'a>(s:&'a str,e:u64,seq:u64)->PublicWsEnvelope<'a>{
  PublicWsEnvelope{venue:Venue::OkxSpot,url:"wss://ws.okx.com:8443/ws/v5/public",
   base:"BTC",connection_epoch:e,sequence:seq,received_wall_ms:N,body:s}
 }
 fn b<'a>(s:&'a str,e:u64,seq:u64)->PublicWsEnvelope<'a>{
  PublicWsEnvelope{venue:Venue::BinanceFutures,
   url:"wss://fstream.binance.com/market/ws/btcusdt@ticker",
   base:"BTC",connection_epoch:e,sequence:seq,received_wall_ms:N,body:s}
 }
 #[test]fn r130_first_pair_never_opens_second_distinct_pair_matures(){
  let mut g=PublicColdBootR130::default();
  let a=okx(N-1200);let f=bn(N-1200);
  assert!(g.observe(N,o(&a,5,12),b(&f,7,14)).unwrap().is_none());
  assert!(!g.ready("BTC"));
  assert!(g.observe(N,o(&a,5,12),b(&f,7,14)).unwrap().is_none());
  assert!(!g.ready("BTC"));
  let a2=okx(N-800);let f2=bn(N-800);
  assert!(g.observe(N,o(&a2,5,13),b(&f2,7,15)).unwrap().is_some());
  assert!(g.ready("BTC"));
  assert_eq!(g.tracked(),1);
 }
 #[test]fn r130_reconnect_demotes_readiness_until_both_venues_advance(){
  let mut g=PublicColdBootR130::default();
  let a=okx(N-2000);let f=bn(N-2000);
  g.observe(N,o(&a,5,12),b(&f,7,14)).unwrap();
  let a2=okx(N-1600);let f2=bn(N-1600);
  g.observe(N,o(&a2,5,13),b(&f2,7,15)).unwrap();
  assert!(g.ready("BTC"));
  let a3=okx(N-1200);let f3=bn(N-1200);
  assert!(g.observe(N,o(&a3,6,1),b(&f3,8,1)).unwrap().is_none());
  assert!(!g.ready("BTC"));
  let a4=okx(N-800);let f4=bn(N-800);
  assert!(g.observe(N,o(&a4,6,2),b(&f4,8,2)).unwrap().is_some());
  assert!(g.ready("BTC"));
 }
 #[test]fn r130_one_updated_venue_and_backdated_replay_cannot_warm(){
  let mut g=PublicColdBootR130::default();
  let a=okx(N-2000);let f=bn(N-2000);
  g.observe(N,o(&a,5,12),b(&f,7,14)).unwrap();
  let a2=okx(N-1600);
  assert!(g.observe(N,o(&a2,5,13),b(&f,7,14)).unwrap().is_none());
  assert!(!g.ready("BTC"));
  let old=okx(N-2200);
  assert_eq!(g.observe(N,o(&old,5,15),b(&f,7,14)).err(),
   Some(PublicWsError::Replay));
  assert!(!g.ready("BTC"));
 }
}
