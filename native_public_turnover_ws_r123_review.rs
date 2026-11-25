//! R123: offline public WebSocket ticker-shape parity, not authenticated transport.
//! Reuses the existing OKX ticker validator. No orders or network activity.
use crate::Venue;
use crate::native_public_market_gate_review::parse_public_okx_spot_ticker;
use crate::native_paper_strategy_frame_review::PublicEntryMarketSnapshot;
use serde_json::Value;
use std::collections::BTreeSet;

#[derive(Clone,Copy,Debug)]
pub struct PublicWsEnvelope<'a>{
 pub venue:Venue,pub url:&'a str,pub base:&'a str,
 pub connection_epoch:u64,pub sequence:u64,
 pub received_wall_ms:i64,pub body:&'a str,
}
#[derive(Debug,Clone,Copy,PartialEq,Eq)]
pub enum PublicWsError{Route,Epoch,Payload,Clock,Replay}
#[derive(Debug,Clone,PartialEq)]
pub struct WsTickerQuote{
 pub base:String,pub quote_24h:f64,pub source_ms:i64,
 pub received_ms:i64,pub epoch:u64,pub sequence:u64,
}
#[derive(Debug,Clone)]
pub struct PublicWsPair{
 pub snapshot:PublicEntryMarketSnapshot,
 pub okx:WsTickerQuote,pub binance:WsTickerQuote,
 pub exchange_authenticated:bool,
}
fn check_route(e:PublicWsEnvelope<'_>)->Result<(),PublicWsError>{
 if e.connection_epoch==0||e.sequence==0{return Err(PublicWsError::Epoch)}
 if e.base.is_empty()||e.base.len()>32||
    !e.base.bytes().all(|b|b.is_ascii_uppercase()||b.is_ascii_digit()){
  return Err(PublicWsError::Route)
 }
 let allowed=match e.venue{
  Venue::OkxSpot=>matches!(e.url,
   "wss://ws.okx.com:8443/ws/v5/public" |
   "wss://ws.okx.com/ws/v5/public"),
  Venue::BinanceFutures=>e.url==format!(
   "wss://fstream.binance.com/market/ws/{}usdt@ticker",
    e.base.to_ascii_lowercase()),
 };
 if !allowed{return Err(PublicWsError::Route)}
 Ok(())
}
fn clock_ok(now:i64,recv:i64,src:i64)->bool{
 now>0&&recv>0&&src>0
  &&now.checked_sub(recv).is_some_and(|x|(0..=30_000).contains(&x))
  &&recv.checked_sub(src).is_some_and(|x|(0..=30_000).contains(&x))
}
pub fn parse_ws_ticker(now:i64,e:PublicWsEnvelope<'_>)
 ->Result<WsTickerQuote,PublicWsError>{
 check_route(e)?;
 if e.body.len()>32_768{return Err(PublicWsError::Payload)}
 let (quote,source)=match e.venue{
  Venue::OkxSpot=>{
   let bases=BTreeSet::from([e.base.to_string()]);
   let recv=u64::try_from(e.received_wall_ms).map_err(|_|PublicWsError::Clock)?;
   let parsed=parse_public_okx_spot_ticker(e.body,&bases,recv)
    .map_err(|_|PublicWsError::Payload)?;
   (parsed.volume_24h_quote_e12 as f64/1e12,
    i64::try_from(parsed.exchange_wall_ms).map_err(|_|PublicWsError::Clock)?)
  },
  Venue::BinanceFutures=>{
   let v:Value=serde_json::from_str(e.body).map_err(|_|PublicWsError::Payload)?;
   let symbol=format!("{}USDT",e.base);
   if v["e"]!="24hrTicker"||v["s"].as_str()!=Some(symbol.as_str())
    ||v["st"].as_u64()!=Some(1){return Err(PublicWsError::Payload)}
   let qty=v["q"].as_str().and_then(|x|x.parse::<f64>().ok())
     .ok_or(PublicWsError::Payload)?;
   let close=v["C"].as_i64().ok_or(PublicWsError::Clock)?;
   let open=v["O"].as_i64().ok_or(PublicWsError::Clock)?;
   let evt=v["E"].as_i64().ok_or(PublicWsError::Clock)?;
   if close.checked_sub(open).map_or(true,|x|x<=0||x>90_000_000)
    ||evt<close.saturating_sub(30_000)||evt>close.saturating_add(30_000){
    return Err(PublicWsError::Clock)
   }
   (qty,close)
  }
 };
 if !quote.is_finite()||quote<=0.0{return Err(PublicWsError::Payload)}
 if !clock_ok(now,e.received_wall_ms,source){return Err(PublicWsError::Clock)}
 Ok(WsTickerQuote{base:e.base.into(),quote_24h:quote,source_ms:source,
  received_ms:e.received_wall_ms,epoch:e.connection_epoch,sequence:e.sequence})
}
pub fn bind_public_ws_pair(now:i64,spot:PublicWsEnvelope<'_>,
 future:PublicWsEnvelope<'_>)->Result<PublicWsPair,PublicWsError>{
 if spot.venue!=Venue::OkxSpot||future.venue!=Venue::BinanceFutures
  ||spot.base!=future.base{return Err(PublicWsError::Route)}
 let a=parse_ws_ticker(now,spot)?;
 let b=parse_ws_ticker(now,future)?;
 Ok(PublicWsPair{snapshot:PublicEntryMarketSnapshot{
  base:a.base.clone(),spot_volume_24h_quote:a.quote_24h,
  futures_volume_24h_quote:b.quote_24h,
  observed_wall_ms:a.source_ms.min(b.source_ms)},
  okx:a,binance:b,exchange_authenticated:false})
}
#[cfg(test)]
mod tests{
 use super::*;
 const NOW:i64=1_791_783_600_000;
 fn okx(n:&str)->String{
  format!(r#"{{"arg":{{"channel":"tickers","instId":"BTC-USDT"}},"data":[{{"instType":"SPOT","instId":"BTC-USDT","volCcy24h":"{n}","ts":"{}"}}]}}"#,NOW-1000)
 }
 fn binance(n:&str)->String{
  format!(r#"{{"e":"24hrTicker","E":{},"s":"BTCUSDT","q":"{n}","v":"1","O":{},"C":{},"st":1}}"#,
   NOW-1000,NOW-86_401_000,NOW-1000)
 }
 fn a<'a>(s:&'a str)->PublicWsEnvelope<'a>{
  PublicWsEnvelope{venue:Venue::OkxSpot,url:"wss://ws.okx.com:8443/ws/v5/public",
   base:"BTC",connection_epoch:4,sequence:16,received_wall_ms:NOW,body:s}
 }
 fn b<'a>(s:&'a str)->PublicWsEnvelope<'a>{
  PublicWsEnvelope{venue:Venue::BinanceFutures,
   url:"wss://fstream.binance.com/market/ws/btcusdt@ticker",
   base:"BTC",connection_epoch:7,sequence:21,received_wall_ms:NOW,body:s}
 }
 #[test]fn r123_existing_okx_parser_and_futures_quote_field_work(){
  let x=okx("180000");
  let y=binance("240000");
  let result=bind_public_ws_pair(NOW,a(&x),b(&y)).unwrap();
  assert_eq!(result.snapshot.spot_volume_24h_quote,180000.0);
  assert_eq!(result.snapshot.futures_volume_24h_quote,240000.0);
  assert_eq!(result.snapshot.observed_wall_ms,NOW-1000);
  assert!(!result.exchange_authenticated);
 }
 #[test]fn r158_okx_port_443_source_route_supported_without_source_attestation(){
  let x=okx("180000");let y=binance("240000");
  let mut spot=a(&x);
  spot.url="wss://ws.okx.com/ws/v5/public";
  let v=bind_public_ws_pair(NOW,spot,b(&y)).unwrap();
  assert_eq!(v.snapshot.spot_volume_24h_quote,180_000.0);
  assert!(!v.exchange_authenticated);
  spot.url="wss://ws.okx.com:444/ws/v5/public";
  assert_eq!(bind_public_ws_pair(NOW,spot,b(&y)).err(),Some(PublicWsError::Route));
 }
 #[test]fn r123_source_routes_epoch_and_symbol_mismatch_denied(){
  let x=okx("180000");
  let y=binance("240000");
  let mut f=b(&y);f.url="wss://fstream.binance.com/public/stream";
  assert_eq!(bind_public_ws_pair(NOW,a(&x),f).err(),Some(PublicWsError::Route));
  let mut f=b(&y);f.connection_epoch=0;
  assert_eq!(bind_public_ws_pair(NOW,a(&x),f).err(),Some(PublicWsError::Epoch));
  let mut f=b(&y);f.base="ETH";
  assert_eq!(bind_public_ws_pair(NOW,a(&x),f).err(),Some(PublicWsError::Route));
  let other=y.replace("BTCUSDT","ETHUSDT");
  assert_eq!(bind_public_ws_pair(NOW,a(&x),b(&other)).err(),Some(PublicWsError::Payload));
 }
 #[test]fn r123_stale_nan_and_wrong_quote_unit_denied(){
  let x=okx("180000");
  let y=binance("240000");
  let wrong=y.replace("\"q\":\"240000\"","\"Q\":\"240000\"");
  assert_eq!(bind_public_ws_pair(NOW,a(&x),b(&wrong)).err(),
   Some(PublicWsError::Payload));
  let wrong=y.replace("240000","NaN");
  assert_eq!(bind_public_ws_pair(NOW,a(&x),b(&wrong)).err(),
   Some(PublicWsError::Payload));
  let wrong=y.replace("\"st\":1","\"st\":2");
  assert_eq!(bind_public_ws_pair(NOW,a(&x),b(&wrong)).err(),
   Some(PublicWsError::Payload));
  assert_eq!(bind_public_ws_pair(NOW+31_000,a(&x),b(&y)).err(),
   Some(PublicWsError::Clock));
 }
}
