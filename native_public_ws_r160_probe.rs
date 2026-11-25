//! R160 deliberately isolated, hard-bounded, PUBLIC-ONLY WS observer.
//! One BTC symbol, 3 sockets, 12s. No metadata REST, service interaction,
//! private API, filesystem writes, portfolio, Telegram or order operations.
use futures_util::{SinkExt,StreamExt};
use serde_json::{json,Value};
use spotarb_native_market_core::{Venue};
use spotarb_native_market_core::native_public_turnover_ws_r123_review::{
 parse_ws_ticker,PublicWsEnvelope,
};
use std::time::{Duration,Instant,SystemTime,UNIX_EPOCH};
use tokio_tungstenite::{connect_async,tungstenite::Message};

const OKX:&str="wss://ws.okx.com/ws/v5/public";
const BN_DEPTH:&str="wss://fstream.binance.com/public/stream?streams=btcusdt@depth20@100ms";
const BN_TICKER:&str="wss://fstream.binance.com/market/stream?streams=btcusdt@ticker";
const OBSERVE_SECS:u64=12;
const MAX_MESSAGES:u64=400;
const MAX_TEXT_BYTES:usize=65_536;
#[derive(Clone,Copy)]
enum Feed{Okx,Depth,Ticker}
impl Feed{
 fn name(self)->&'static str{match self{Self::Okx=>"OKX_SPOT_PUBLIC",Self::Depth=>"BINANCE_USDM_PUBLIC_DEPTH",Self::Ticker=>"BINANCE_USDM_MARKET_TICKER"}}
 fn url(self)->&'static str{match self{Self::Okx=>OKX,Self::Depth=>BN_DEPTH,Self::Ticker=>BN_TICKER}}
}
fn wall_ms()->i64{
 SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as i64
}
/// Parse only bounded expected public messages; no raw market payload logging.
fn classify(feed:Feed,raw:&str,now:i64,seq:u64)->Result<&'static str,&'static str>{
 let v:Value=serde_json::from_str(raw).map_err(|_|"non-JSON public frame")?;
 match feed{
  Feed::Okx=>{
   if v["event"]=="subscribe"{
    let channel=v["arg"]["channel"].as_str().unwrap_or("");
    if v["arg"]["instId"]!="BTC-USDT"||
      !matches!(channel,"books5"|"tickers"){return Err("wrong OKX ACK route")}
    return Ok("ack")
   }
   if v["event"]=="error"{return Err("OKX subscribe error")}
   let channel=v["arg"]["channel"].as_str().ok_or("missing OKX channel")?;
   if v["arg"]["instId"]!="BTC-USDT"{return Err("wrong OKX instrument")}
   match channel{
    "books5"=>{
     let a=v["data"].as_array().ok_or("missing OKX books5 array")?;
     if a.len()!=1||a[0]["instId"]!="BTC-USDT"||
      a[0]["bids"].as_array().map_or(true,|x|x.is_empty())||
      a[0]["asks"].as_array().map_or(true,|x|x.is_empty()){
       return Err("invalid OKX books5 data")
     }
     Ok("depth")
    }
    "tickers"=>{
     let env=PublicWsEnvelope{
      venue:Venue::OkxSpot,url:OKX,base:"BTC",
      connection_epoch:1,sequence:seq,received_wall_ms:now,body:raw,
     };
     parse_ws_ticker(now,env).map_err(|_|"invalid OKX ticker/volume/time")?;
     Ok("ticker")
    }
    _=>Err("unexpected OKX channel")
   }
  }
  Feed::Depth=>{
   if v["stream"]!="btcusdt@depth20@100ms"{return Err("wrong Binance depth stream")}
   let data=&v["data"];
   if data["e"]!="depthUpdate"||data["s"]!="BTCUSDT"||
      data["st"].as_u64()!=Some(1)||
      data["U"].as_i64().is_none()||data["u"].as_i64().is_none()||
      data["pu"].as_i64().is_none()||
      data["b"].as_array().map_or(true,|x|x.is_empty())||
      data["a"].as_array().map_or(true,|x|x.is_empty()){
     return Err("invalid Binance partial depth20 / CM filter")
   }
   // The candidate's canonical book decoder must also accept this
   // ACTUAL source schema, not just a purpose-built probe parser.
   let frame=spotarb_native_market_core::RawFrame{source:None,
     venue:Venue::BinanceFutures,epoch:1,received_ns:1,
     received_wall_ms:now,raw:raw.into(),
   };
   if spotarb_native_market_core::parse(&frame).map(|u|u.base)
     .ok().as_deref()!=Some("BTC"){
     return Err("candidate Binance book parser rejected live payload")
   }
   Ok("depth")
  }
  Feed::Ticker=>{
   if v["stream"]!="btcusdt@ticker"{return Err("wrong Binance ticker stream")}
   let body=v["data"].to_string();
   let env=PublicWsEnvelope{venue:Venue::BinanceFutures,
    url:"wss://fstream.binance.com/market/ws/btcusdt@ticker",base:"BTC",
    connection_epoch:1,sequence:seq,received_wall_ms:now,body:&body};
   parse_ws_ticker(now,env).map_err(|_|"invalid Binance ticker/volume/time/UM")?;
   Ok("ticker")
  }
 }
}
async fn observe(feed:Feed)->Value{
 let url=feed.url();
 let mut rec=json!({"feed":feed.name(),"endpoint":url,
    "connected":false,"public_only":true,"subscriptions_ack":0,
    "depth_valid":0,"ticker_valid":0,"invalid":0,
    "received":0,"bytes_total":0,"first_receive_wall_ms":null,
    "last_receive_wall_ms":null,"status":"not_started","first_error":null,
    "remote_peer_independently_attested":false});
 let started=Instant::now();
 let conn=tokio::time::timeout(Duration::from_secs(6),connect_async(url)).await;
 let (mut socket,_)=match conn{
  Ok(Ok(v))=>v,
  Ok(Err(_))=>{rec["status"]=json!("connect_failed");return rec},
  Err(_)=>{rec["status"]=json!("connect_timeout");return rec},
 };
 rec["connected"]=json!(true);
 if matches!(feed,Feed::Okx){
  let sub=json!({"op":"subscribe","args":[
   {"channel":"books5","instId":"BTC-USDT"},
   {"channel":"tickers","instId":"BTC-USDT"}]});
  if socket.send(Message::Text(sub.to_string())).await.is_err(){
   rec["status"]=json!("public_subscribe_send_failed");return rec
  }
 }
 let mut recv=0_u64;let mut valid_depth=0_u64;let mut valid_ticker=0_u64;
 let mut ack=0_u64;let mut invalid=0_u64;let mut bytes=0_u64;
 let mut error:Option<&'static str>=None;
 let deadline=started+Duration::from_secs(OBSERVE_SECS);
 while Instant::now()<deadline &&recv<MAX_MESSAGES{
  let left=deadline.saturating_duration_since(Instant::now()).min(Duration::from_secs(2));
  if left.is_zero(){break}
  match tokio::time::timeout(left,socket.next()).await{
   Ok(Some(Ok(Message::Text(raw))))=>{
    recv+=1;bytes+=raw.len() as u64;
    let now=wall_ms();
    if rec["first_receive_wall_ms"].is_null(){rec["first_receive_wall_ms"]=json!(now)}
    rec["last_receive_wall_ms"]=json!(now);
    if raw.len()>MAX_TEXT_BYTES{
     invalid+=1;error.get_or_insert("oversized public frame");break
    }
    match classify(feed,&raw,now,recv){
     Ok("ack")=>ack+=1,
     Ok("depth")=>valid_depth+=1,
     Ok("ticker")=>valid_ticker+=1,
     Ok(_)=>{},
     Err(e)=>{invalid+=1;error.get_or_insert(e);}
    }
   }
   Ok(Some(Ok(Message::Ping(v))))=>{
    if socket.send(Message::Pong(v)).await.is_err(){
     error.get_or_insert("public pong failed");break
    }
   },
   Ok(Some(Ok(Message::Close(_))))=>{error.get_or_insert("public peer closed");break},
   Ok(Some(Ok(_)))=>{},
   Ok(Some(Err(_)))=>{error.get_or_insert("socket read error");break},
   Ok(None)=>{error.get_or_insert("socket ended");break},
   Err(_)=>{},
  }
 }
 let _=tokio::time::timeout(Duration::from_secs(2),socket.close(None)).await;
 rec["subscriptions_ack"]=json!(ack);
 rec["depth_valid"]=json!(valid_depth);
 rec["ticker_valid"]=json!(valid_ticker);
 rec["invalid"]=json!(invalid);
 rec["received"]=json!(recv);
 rec["bytes_total"]=json!(bytes);
 rec["elapsed_ms"]=json!(started.elapsed().as_millis());
 rec["first_error"]=json!(error);
 rec["status"]=json!(if invalid==0 &&error.is_none() &&
  match feed{Feed::Okx=>ack>=2&&valid_depth>0&&valid_ticker>0,
    Feed::Depth=>valid_depth>0,Feed::Ticker=>valid_ticker>0}{"PASS"}else{"FAIL"});
 rec
}
#[tokio::main(flavor="multi_thread",worker_threads=3)]
async fn main()->Result<(),Box<dyn std::error::Error>>{
 let args=std::env::args().skip(1).collect::<Vec<_>>();
 if args!=["--r160-public-only"]{
  return Err("R160 requires exactly --r160-public-only; no symbols or overrides".into())
 }
 let (okx,depth,ticker)=tokio::join!(
  observe(Feed::Okx),observe(Feed::Depth),observe(Feed::Ticker));
 let pass=[&okx,&depth,&ticker].iter().all(|x|x["status"]=="PASS");
 let report=json!({"schema":"SPOTARB/R160/PUBLIC-ONLY-OBSERVATION/V1",
    "symbol":"BTC","duration_hard_limit_seconds":OBSERVE_SECS,
    "max_messages_per_feed":MAX_MESSAGES,
    "read_only_public_transport":true,
    "real_orders":false,"private_api_calls":0,"installed_paper_modified":false,
    "exchange_peer_independently_attested":false,
    "all_three_public_feeds_valid":pass,"results":[okx,depth,ticker]});
 println!("{}",serde_json::to_string_pretty(&report)?);
 if !pass{std::process::exit(2)}
 Ok(())
}
#[cfg(test)]
mod tests{
 use super::*;
 const NOW:i64=1_791_783_600_000;
 #[test]fn r160_expected_routes_avoid_any_private_or_order_endpoint(){
  for feed in [Feed::Okx,Feed::Depth,Feed::Ticker]{
   let url=feed.url();
   assert!(url.starts_with("wss://"));
   assert!(!url.contains("/private"));
   assert!(!url.contains("/order"));
   assert!(!url.contains(":8443"));
  }
  assert_ne!(BN_DEPTH,BN_TICKER);
  assert!(BN_DEPTH.contains("/public/"));
  assert!(BN_TICKER.contains("/market/"));
 }
 #[test]fn r160_offline_okx_ticker_and_both_books_are_strict(){
  let spot=json!({"arg":{"channel":"tickers","instId":"BTC-USDT"},
    "data":[{"instType":"SPOT","instId":"BTC-USDT",
     "volCcy24h":"250000","ts":(NOW-500).to_string()}]});
  assert_eq!(classify(Feed::Okx,&spot.to_string(),NOW,1),Ok("ticker"));
  let spot_bad=spot.to_string().replace("SPOT","SWAP");
  assert!(classify(Feed::Okx,&spot_bad,NOW,1).is_err());
  let book=json!({"arg":{"channel":"books5","instId":"BTC-USDT"},
   "data":[{"instId":"BTC-USDT","bids":[["100","2"]],
    "asks":[["101","2"]]}]});
  assert_eq!(classify(Feed::Okx,&book.to_string(),NOW,1),Ok("depth"));
  let futures=json!({"stream":"btcusdt@depth20@100ms","data":{
   "e":"depthUpdate","s":"BTCUSDT","E":NOW-100,
   "U":1231,"u":1234,"pu":1230,"st":1,
   "b":[["100","1"]],"a":[["101","1"]]}});
  assert_eq!(classify(Feed::Depth,&futures.to_string(),NOW,1),Ok("depth"));
  let cm=futures.to_string().replace("\"st\":1","\"st\":2");
  assert!(classify(Feed::Depth,&cm,NOW,1).is_err());
 }
 #[test]fn r160_binance_ticker_uses_quote_volume_and_usdm_only(){
  let body=json!({"e":"24hrTicker","E":NOW-500,"s":"BTCUSDT",
   "q":"300000","v":"3","O":NOW-86_400_500,"C":NOW-500,"st":1});
  let wrapped=json!({"stream":"btcusdt@ticker","data":body});
  assert_eq!(classify(Feed::Ticker,&wrapped.to_string(),NOW,1),Ok("ticker"));
  let wrong=wrapped.to_string().replace("\"st\":1","\"st\":2");
  assert!(classify(Feed::Ticker,&wrong,NOW,1).is_err());
  let wrong=wrapped.to_string().replace("\"q\":\"300000\"","\"Q\":\"300000\"");
  assert!(classify(Feed::Ticker,&wrong,NOW,1).is_err());
 }
}
