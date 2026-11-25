//! R122 Astra: same-boot continuity of **source-shaped** 24h ticker records.
 //! This protects against local replay/clock rollback, but does not verify TLS
 //! peer or network owner. Do NOT use it as standalone real trading evidence.
use std::collections::BTreeMap;
use crate::native_public_turnover_source_review::{
 PublicTickerSource,ReviewedTurnoverPair,TurnoverSourceError,review_public_ticker_pair,
};
#[derive(Clone,Debug)]
struct TickMark{generation:u64,exchange_ts:i64,raw:String}
#[derive(Clone,Debug)]
struct PairMark{spot:TickMark,future:TickMark,now_ms:i64}
#[derive(Default)]
pub struct PublicTurnoverContinuityR122{seen:BTreeMap<String,PairMark>}
fn legal_next(old:&TickMark,new:&TickMark)->bool{
 new.generation>=old.generation && new.exchange_ts>=old.exchange_ts
  &&(new.exchange_ts!=old.exchange_ts||new.raw==old.raw)
}
impl PublicTurnoverContinuityR122{
 pub fn accept_shaped_pair(
  &mut self,base:&str,now_ms:i64,spot:PublicTickerSource<'_>,fut:PublicTickerSource<'_>
 )->Result<ReviewedTurnoverPair,TurnoverSourceError>{
  let evidence=review_public_ticker_pair(base,now_ms,spot,fut)?;
  let s=TickMark{generation:spot.generation,
   exchange_ts:evidence.spot_exchange_ts_ms,raw:spot.json.into()};
  let f=TickMark{generation:fut.generation,
   exchange_ts:evidence.futures_exchange_ts_ms,raw:fut.json.into()};
  if let Some(prior)=self.seen.get(base){
   if now_ms<prior.now_ms || !legal_next(&prior.spot,&s)
    || !legal_next(&prior.future,&f){
    return Err(TurnoverSourceError::Replay)
   }
  }
  self.seen.insert(base.into(),PairMark{spot:s,future:f,now_ms});
  Ok(evidence)
 }
 pub fn observed_bases(&self)->usize{self.seen.len()}
}

#[cfg(test)]
mod tests{
 use super::*;
 use crate::Venue;
 use crate::native_public_turnover_source_review::{OKX_SPOT_TICKER,BINANCE_USDM_24H};
 const NOW:i64=1_791_783_600_000;
 fn o(ts:i64,v:&str)->String{
  format!(r#"{{"code":"0","data":[{{"instType":"SPOT","instId":"BTC-USDT","volCcy24h":"{}","ts":"{}"}}]}}"#,v,ts)
 }
 fn b(ts:i64,v:&str)->String{
  format!(r#"{{"symbol":"BTCUSDT","quoteVolume":"{}","openTime":{},"closeTime":{}}}"#,v,ts-86_400_000,ts)
 }
 fn accept(g:&mut PublicTurnoverContinuityR122,now:i64,os:&str,bs:&str,oe:u64,be:u64)
  ->Result<ReviewedTurnoverPair,TurnoverSourceError>{
  g.accept_shaped_pair("BTC",now,
   PublicTickerSource{venue:Venue::OkxSpot,endpoint:OKX_SPOT_TICKER,symbol:"BTC-USDT",
    generation:oe,received_wall_ms:now,json:os},
   PublicTickerSource{venue:Venue::BinanceFutures,endpoint:BINANCE_USDM_24H,symbol:"BTCUSDT",
    generation:be,received_wall_ms:now,json:bs})
 }
 #[test]fn r122_same_boot_continuity_accepts_same_or_fresher_quote_pair(){
  let mut g=PublicTurnoverContinuityR122::default();
  let a=o(NOW-1000,"500000");
  let b0=b(NOW-1000,"600000");
  assert!(accept(&mut g,NOW,&a,&b0,1,1).is_ok());
  assert!(accept(&mut g,NOW,&a,&b0,1,1).is_ok());
  let newer=o(NOW-500,"500001");
  assert!(accept(&mut g,NOW,&newer,&b0,1,1).is_ok());
  assert_eq!(g.observed_bases(),1);
 }
 #[test]fn r122_source_rollback_stealth_rewrite_and_epoch_rewind_denied(){
  let mut g=PublicTurnoverContinuityR122::default();
  let a=o(NOW-1000,"500000");
  let b0=b(NOW-1000,"600000");
  accept(&mut g,NOW,&a,&b0,10,10).unwrap();
  let changed=o(NOW-1000,"999999");
  assert_eq!(accept(&mut g,NOW,&changed,&b0,10,10).err(),
   Some(TurnoverSourceError::Replay));
  let older=o(NOW-1500,"500000");
  assert_eq!(accept(&mut g,NOW,&older,&b0,10,10).err(),
   Some(TurnoverSourceError::Replay));
  assert_eq!(accept(&mut g,NOW,&a,&b0,9,10).err(),
   Some(TurnoverSourceError::Replay));
  assert_eq!(g.observed_bases(),1);
 }
 #[test]fn r122_currentness_and_clock_rollback_fail_closed(){
  let mut g=PublicTurnoverContinuityR122::default();
  let a=o(NOW-1000,"500000");
  let b0=b(NOW-1000,"600000");
  accept(&mut g,NOW,&a,&b0,1,1).unwrap();
  assert_eq!(accept(&mut g,NOW-1,&a,&b0,1,1).err(),
   Some(TurnoverSourceError::Replay));
  assert_eq!(accept(&mut g,NOW+61_000,&a,&b0,1,1).err(),
   Some(TurnoverSourceError::Time));
 }
}
