//! R124: offline same-boot continuity for source-shaped WS 24h turnover.
//! Does not authenticate transport, TLS peers, or restart state.
use crate::native_public_turnover_ws_r123_review::{
 PublicWsEnvelope,PublicWsPair,WsTickerQuote,PublicWsError,bind_public_ws_pair,
};
use std::collections::BTreeMap;
/// Bound retained public JSON and instrument-key memory; no eviction/replay reset.
const MAX_TRACKED_BASES:usize=512;
#[derive(Clone,Debug)]
struct Observed{quote:WsTickerQuote,raw:String}
#[derive(Clone,Debug)]
struct Pair{spot:Observed,futures:Observed,now:i64}
#[derive(Default)]
pub struct WsTurnoverContinuityR124{history:BTreeMap<String,Pair>}
fn next(old:&Observed,now:&Observed)->bool{
 let a=&old.quote;let b=&now.quote;
 if b.source_ms<a.source_ms || b.received_ms<a.received_ms || b.epoch<a.epoch{
  return false
 }
 if b.epoch==a.epoch && b.sequence<a.sequence{return false}
 if b.epoch==a.epoch && b.sequence==a.sequence{
  return b.source_ms==a.source_ms&&now.raw==old.raw
 }
 if b.source_ms==a.source_ms && b.quote_24h!=a.quote_24h{
  return false
 }
 true
}
impl WsTurnoverContinuityR124{
 pub fn accept(&mut self,now:i64,spot:PublicWsEnvelope<'_>,
   futures:PublicWsEnvelope<'_>)->Result<PublicWsPair,PublicWsError>{
  if self.history.len()>=MAX_TRACKED_BASES
   &&!self.history.contains_key(spot.base){return Err(PublicWsError::Replay)}
  let quote=bind_public_ws_pair(now,spot,futures)?;
  let id=quote.snapshot.base.clone();
  let a=Observed{quote:quote.okx.clone(),raw:spot.body.into()};
  let b=Observed{quote:quote.binance.clone(),raw:futures.body.into()};
  if let Some(old)=self.history.get(&id){
   if now<old.now||!next(&old.spot,&a)||!next(&old.futures,&b){
    return Err(PublicWsError::Replay)
   }
  }
  self.history.insert(id,Pair{spot:a,futures:b,now});
  Ok(quote)
 }
 pub fn tracked(&self)->usize{self.history.len()}
}
#[cfg(test)]
mod tests{
 use super::*;
 use crate::Venue;
 const T:i64=1_791_783_600_000;
 fn o(ts:i64,n:&str)->String{
  format!(r#"{{"arg":{{"channel":"tickers","instId":"BTC-USDT"}},"data":[{{"instType":"SPOT","instId":"BTC-USDT","volCcy24h":"{n}","ts":"{ts}"}}]}}"#)
 }
 fn f(ts:i64,n:&str)->String{
  format!(r#"{{"e":"24hrTicker","E":{ts},"s":"BTCUSDT","q":"{n}","O":{},"C":{ts},"st":1}}"#,ts-86_400_000)
 }
 fn a<'a>(raw:&'a str,epoch:u64,seq:u64)->PublicWsEnvelope<'a>{
  PublicWsEnvelope{venue:Venue::OkxSpot,url:"wss://ws.okx.com:8443/ws/v5/public",
    base:"BTC",connection_epoch:epoch,sequence:seq,received_wall_ms:T,body:raw}
 }
 fn b<'a>(raw:&'a str,epoch:u64,seq:u64)->PublicWsEnvelope<'a>{
  PublicWsEnvelope{venue:Venue::BinanceFutures,
    url:"wss://fstream.binance.com/market/ws/btcusdt@ticker",
    base:"BTC",connection_epoch:epoch,sequence:seq,received_wall_ms:T,body:raw}
 }
 #[test]fn r124_reused_quote_plus_new_update_and_reconnect_ok(){
  let mut c=WsTurnoverContinuityR124::default();
  let x=o(T-1000,"200000");let y=f(T-1000,"300000");
  c.accept(T,a(&x,3,20),b(&y,5,21)).unwrap();
  let next=o(T-500,"200001");
  c.accept(T,a(&next,3,21),b(&y,5,21)).unwrap();
  c.accept(T,a(&next,4,1),b(&y,6,1)).unwrap();
  assert_eq!(c.tracked(),1);
 }
 #[test]fn r124_rollback_and_rewritten_same_sequence_rejected(){
  let mut c=WsTurnoverContinuityR124::default();
  let x=o(T-1000,"200000");let y=f(T-1000,"300000");
  c.accept(T,a(&x,3,20),b(&y,5,21)).unwrap();
  let changed=o(T-1000,"999999");
  assert_eq!(c.accept(T,a(&changed,3,20),b(&y,5,21)).err(),Some(PublicWsError::Replay));
  assert_eq!(c.accept(T,a(&x,3,19),b(&y,5,21)).err(),Some(PublicWsError::Replay));
  assert_eq!(c.accept(T,a(&x,2,30),b(&y,5,21)).err(),Some(PublicWsError::Replay));
  assert_eq!(c.tracked(),1);
 }
 #[test]fn r124_rewind_time_or_new_epoch_old_ticker_denied(){
  let mut c=WsTurnoverContinuityR124::default();
  let x=o(T-1000,"200000");let y=f(T-1000,"300000");
  c.accept(T,a(&x,3,20),b(&y,5,21)).unwrap();
  assert_eq!(c.accept(T-1,a(&x,3,20),b(&y,5,21)).err(),
   Some(PublicWsError::Clock));
  let older=o(T-2000,"200000");
  assert_eq!(c.accept(T,a(&older,4,1),b(&y,5,21)).err(),
   Some(PublicWsError::Replay));
 }
 #[test]fn r127_memory_cap_cannot_evict_old_replay_floor(){
  let mut c=WsTurnoverContinuityR124::default();
  let x=o(T-1000,"200000");let y=f(T-1000,"300000");
  c.accept(T,a(&x,3,20),b(&y,5,21)).unwrap();
  for i in 1..MAX_TRACKED_BASES{
   let q=WsTickerQuote{base:format!("R{i}"),quote_24h:100.0,
    source_ms:T-1000,received_ms:T,epoch:1,sequence:1};
   let row=Observed{quote:q,raw:"synthetic-test-only".into()};
   c.history.insert(format!("R{i}"),Pair{spot:row.clone(),futures:row,now:T});
  }
  assert_eq!(c.tracked(),MAX_TRACKED_BASES);
  let new=PublicWsEnvelope{base:"ETH",..a(&x,3,20)};
  assert_eq!(c.accept(T,new,b(&y,5,21)).err(),Some(PublicWsError::Replay));
  assert!(c.accept(T,a(&x,3,20),b(&y,5,21)).is_ok());
  assert_eq!(c.tracked(),MAX_TRACKED_BASES);
 }
}
