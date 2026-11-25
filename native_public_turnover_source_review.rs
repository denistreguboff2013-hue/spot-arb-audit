//! R117 Astra P1-06: source-SHAPED public 24h turnover preview only.
//! No exchange-authenticated transport, no network, NO proof of data origin.
//! OKX SPOT volCcy24h is QUOTE volume; Binance USDT-M quoteVolume is quote.
//! In particular do not use OKX vol24h (BASE) or Binance volume (BASE).
use crate::Venue;
use crate::native_paper_strategy_frame_review::PublicEntryMarketSnapshot;
use serde_json::Value;

pub const OKX_SPOT_TICKER:&str="/api/v5/market/ticker";
pub const BINANCE_USDM_24H:&str="/fapi/v1/ticker/24hr";
#[derive(Debug,Clone,Copy)]
pub struct PublicTickerSource<'a>{
 pub venue:Venue,pub endpoint:&'a str,pub symbol:&'a str,
 pub generation:u64,pub received_wall_ms:i64,pub json:&'a str,
}
#[derive(Debug,Clone,PartialEq,Eq)]
pub enum TurnoverSourceError{Base,Venue,Symbol,Generation,Time,Payload,Volume,Replay}
#[derive(Debug,Clone)]
pub struct ReviewedTurnoverPair{
 pub market:PublicEntryMarketSnapshot,
 pub spot_generation:u64,pub futures_generation:u64,
 pub spot_exchange_ts_ms:i64,pub futures_exchange_ts_ms:i64,
 /// JSON parsed successfully, not exchange/network verified.
 pub exchange_authenticated:bool,
}
fn volume_string(obj:&Value,key:&str)->Result<f64,TurnoverSourceError>{
 let value=obj.get(key).and_then(Value::as_str).ok_or(TurnoverSourceError::Payload)?;
 let n=value.parse::<f64>().map_err(|_|TurnoverSourceError::Volume)?;
 if !n.is_finite()||n<=0.0{return Err(TurnoverSourceError::Volume)}
 Ok(n)
}
fn clock_ok(now_ms:i64,recv_ms:i64,src_ms:i64)->bool{
 now_ms>0&&recv_ms>0&&src_ms>0
  &&now_ms.checked_sub(recv_ms).is_some_and(|a|(0..=60_000).contains(&a))
  &&recv_ms.checked_sub(src_ms).is_some_and(|a|(0..=60_000).contains(&a))
}
/// Strict venue and instrument binding. The caller MUST still establish that
/// the two bodies originated from trusted public market transports.
pub fn review_public_ticker_pair(
 base:&str,now_ms:i64,spot:PublicTickerSource<'_>,fut:PublicTickerSource<'_>
)->Result<ReviewedTurnoverPair,TurnoverSourceError>{
 if base.is_empty()||base.len()>32||!base.bytes().all(|c|c.is_ascii_uppercase()||c.is_ascii_digit()){
  return Err(TurnoverSourceError::Base)
 }
 let okx_symbol=format!("{base}-USDT");
 let binance_symbol=format!("{base}USDT");
 if spot.venue!=Venue::OkxSpot||fut.venue!=Venue::BinanceFutures
  ||spot.endpoint!=OKX_SPOT_TICKER||fut.endpoint!=BINANCE_USDM_24H{
  return Err(TurnoverSourceError::Venue)
 }
 if spot.symbol!=okx_symbol||fut.symbol!=binance_symbol{
  return Err(TurnoverSourceError::Symbol)
 }
 if spot.generation==0||fut.generation==0{
  return Err(TurnoverSourceError::Generation)
 }
 let okx:Value=serde_json::from_str(spot.json).map_err(|_|TurnoverSourceError::Payload)?;
 let okx_data=okx.get("data").and_then(Value::as_array).ok_or(TurnoverSourceError::Payload)?;
 if okx.get("code").and_then(Value::as_str)!=Some("0")||okx_data.len()!=1{
  return Err(TurnoverSourceError::Payload)
 }
 let o=&okx_data[0];
 if o.get("instType").and_then(Value::as_str)!=Some("SPOT")
  ||o.get("instId").and_then(Value::as_str)!=Some(okx_symbol.as_str()){
  return Err(TurnoverSourceError::Symbol)
 }
 let spot_volume=volume_string(o,"volCcy24h")?;
 let spot_ts=o.get("ts").and_then(Value::as_str)
  .and_then(|s|s.parse::<i64>().ok()).ok_or(TurnoverSourceError::Time)?;
 let binance:Value=serde_json::from_str(fut.json).map_err(|_|TurnoverSourceError::Payload)?;
 if !binance.is_object(){return Err(TurnoverSourceError::Payload)}
 if binance.get("symbol").and_then(Value::as_str)!=Some(binance_symbol.as_str()){
  return Err(TurnoverSourceError::Symbol)
 }
 let fut_volume=volume_string(&binance,"quoteVolume")?;
 let fut_ts=binance.get("closeTime").and_then(Value::as_i64)
  .ok_or(TurnoverSourceError::Time)?;
 let opening=binance.get("openTime").and_then(Value::as_i64)
  .ok_or(TurnoverSourceError::Time)?;
 if fut_ts.checked_sub(opening).map_or(true,|span|span<=0||span>90_000_000){
  return Err(TurnoverSourceError::Time)
 }
 if !clock_ok(now_ms,spot.received_wall_ms,spot_ts)
  ||!clock_ok(now_ms,fut.received_wall_ms,fut_ts){
  return Err(TurnoverSourceError::Time)
 }
 Ok(ReviewedTurnoverPair{
  market:PublicEntryMarketSnapshot{base:base.into(),
   spot_volume_24h_quote:spot_volume,futures_volume_24h_quote:fut_volume,
   observed_wall_ms:spot_ts.min(fut_ts)},
  spot_generation:spot.generation,futures_generation:fut.generation,
  spot_exchange_ts_ms:spot_ts,futures_exchange_ts_ms:fut_ts,
  exchange_authenticated:false,
 })
}

#[cfg(test)]
mod tests{
 use super::*;
 const NOW:i64=1_791_783_600_000;
 fn sources()->(String,String){
  (format!(r#"{{"code":"0","data":[{{"instType":"SPOT","instId":"BTC-USDT","volCcy24h":"180000","vol24h":"42","ts":"{}"}}]}}"#,NOW-1000),
   format!(r#"{{"symbol":"BTCUSDT","quoteVolume":"250000","volume":"54","openTime":{},"closeTime":{}}}"#,NOW-86_401_000,NOW-1000))
 }
 fn s<'a>(body:&'a str)->PublicTickerSource<'a>{
  PublicTickerSource{venue:Venue::OkxSpot,endpoint:OKX_SPOT_TICKER,symbol:"BTC-USDT",
   generation:10,received_wall_ms:NOW,json:body}
 }
 fn f<'a>(body:&'a str)->PublicTickerSource<'a>{
  PublicTickerSource{venue:Venue::BinanceFutures,endpoint:BINANCE_USDM_24H,symbol:"BTCUSDT",
   generation:11,received_wall_ms:NOW,json:body}
 }
 #[test]fn r117_source_quote_volume_not_base_units_and_valid_pair(){
  let (o,b)=sources();
  let parsed=review_public_ticker_pair("BTC",NOW,s(&o),f(&b)).unwrap();
  assert_eq!(parsed.market.spot_volume_24h_quote,180_000.0);
  assert_eq!(parsed.market.futures_volume_24h_quote,250_000.0);
  assert_eq!(parsed.market.observed_wall_ms,NOW-1000);
  assert_eq!((parsed.spot_generation,parsed.futures_generation),(10,11));
  assert!(!parsed.exchange_authenticated);
 }
 #[test]fn r117_source_wrong_venue_symbol_generation_and_type_denied(){
  let (o,b)=sources();
  let mut x=s(&o);
  x.endpoint="/api/v5/market/tickers";
  assert_eq!(review_public_ticker_pair("BTC",NOW,x,f(&b)).err(),
   Some(TurnoverSourceError::Venue));
  let mut x=f(&b);
  x.symbol="ETHUSDT";
  assert_eq!(review_public_ticker_pair("BTC",NOW,s(&o),x).err(),
   Some(TurnoverSourceError::Symbol));
  let mut x=f(&b);
  x.generation=0;
  assert_eq!(review_public_ticker_pair("BTC",NOW,s(&o),x).err(),
   Some(TurnoverSourceError::Generation));
  let swapped=o.replace("SPOT","SWAP");
  assert_eq!(review_public_ticker_pair("BTC",NOW,s(&swapped),f(&b)).err(),
   Some(TurnoverSourceError::Symbol));
  assert_eq!(review_public_ticker_pair("BTC-USDT",NOW,s(&o),f(&b)).err(),
   Some(TurnoverSourceError::Base));
 }
 #[test]fn r117_source_stale_future_nan_and_missing_quote_denied(){
  let (o,b)=sources();
  let stale=o.replace(&(NOW-1000).to_string(),&(NOW-61_000).to_string());
  assert_eq!(review_public_ticker_pair("BTC",NOW,s(&stale),f(&b)).err(),
   Some(TurnoverSourceError::Time));
  let future=o.replace(&(NOW-1000).to_string(),&(NOW+1).to_string());
  assert_eq!(review_public_ticker_pair("BTC",NOW,s(&future),f(&b)).err(),
   Some(TurnoverSourceError::Time));
  let nan=b.replace("250000","NaN");
  assert_eq!(review_public_ticker_pair("BTC",NOW,s(&o),f(&nan)).err(),
   Some(TurnoverSourceError::Volume));
  let wrong=b.replace("quoteVolume","baseVolume");
  assert_eq!(review_public_ticker_pair("BTC",NOW,s(&o),f(&wrong)).err(),
   Some(TurnoverSourceError::Payload));
  let wrong_span=b.replace(&(NOW-86_401_000).to_string(),&(NOW-200_000_000).to_string());
  assert_eq!(review_public_ticker_pair("BTC",NOW,s(&o),f(&wrong_span)).err(),
   Some(TurnoverSourceError::Time));
 }
}
