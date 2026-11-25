//! R162: isolated PUBLIC-only BTC reconnection/queue diagnostic.
//! Explicitly NOT a fix or proof for the blocked R155 source-lane ingress.
//! No secrets, private API, orders, installed service, DB or persistent writes.
use futures_util::{SinkExt,StreamExt};
use serde_json::{json,Value};
use spotarb_native_market_core::{parse,RawFrame,Venue};
use spotarb_native_market_core::native_public_turnover_ws_r123_review::{
    parse_ws_ticker,PublicWsEnvelope,
};
use std::time::{Duration,Instant,SystemTime,UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async,tungstenite::Message};

const OKX:&str="wss://ws.okx.com/ws/v5/public";
const DEPTH:&str="wss://fstream.binance.com/public/stream?streams=btcusdt@depth20@100ms";
const TICKER:&str="wss://fstream.binance.com/market/stream?streams=btcusdt@ticker";
const EPOCH_SECONDS:u64=7;
const MAX_PER_EPOCH:usize=128;
const QUEUE_CAP:usize=8;
const MAX_BYTES:usize=65_536;
#[derive(Clone,Copy)]
enum Feed{Okx,Depth,Ticker}
impl Feed{
 fn url(self)->&'static str{
  match self{Self::Okx=>OKX,Self::Depth=>DEPTH,Self::Ticker=>TICKER}
 }
 fn label(self)->&'static str{
  match self{Self::Okx=>"OKX_SPOT",Self::Depth=>"BINANCE_USDM_DEPTH",
    Self::Ticker=>"BINANCE_USDM_TICKER"}
 }
}
fn wall()->i64{
 SystemTime::now().duration_since(UNIX_EPOCH)
  .unwrap_or_default().as_millis() as i64
}
#[derive(Clone,Copy,Debug)]
struct Header{epoch:u64,sequence:u64,received_ms:i64}
#[derive(Default)]
struct IngressGate{
 epoch:u64,last_sequence:u64,last_received_ms:i64,accepted:u64,denied:u64
}
impl IngressGate{
 fn reset(&mut self,new_epoch:u64)->bool{
  if new_epoch<=self.epoch||new_epoch==0{return false}
  self.epoch=new_epoch;
  self.last_sequence=0;
  self.last_received_ms=0;
  true
 }
 fn accept(&mut self,h:Header)->bool{
  if h.epoch!=self.epoch||h.sequence==0||
    h.sequence<=self.last_sequence||h.received_ms<self.last_received_ms{
   self.denied+=1;return false
  }
  self.last_sequence=h.sequence;
  self.last_received_ms=h.received_ms;
  self.accepted+=1;
  true
 }
}
fn classify(feed:Feed,s:&str,received:i64,seq:u64)->Result<&'static str,&'static str>{
 let v:Value=serde_json::from_str(s).map_err(|_|"not JSON")?;
 match feed{
  Feed::Okx=>{
   if v["event"]=="subscribe"{
    if v["arg"]["instId"]!="BTC-USDT"||
     !matches!(v["arg"]["channel"].as_str(),Some("tickers"|"books5")){
     return Err("unexpected OKX subscription ack")
    }
    return Ok("ack")
   }
   if v["arg"]["instId"]!="BTC-USDT"{return Err("unexpected OKX route")}
   match v["arg"]["channel"].as_str(){
    Some("books5")=>{
     let f=RawFrame{source:None,venue:Venue::OkxSpot,epoch:1,
      received_ns:1,received_wall_ms:received,raw:s.into()};
     if parse(&f).map(|u|u.base).ok().as_deref()!=Some("BTC"){
      return Err("canonical OKX parser denied")
     }
     Ok("depth")
    },
    Some("tickers")=>{
     let e=PublicWsEnvelope{venue:Venue::OkxSpot,url:OKX,base:"BTC",
      connection_epoch:1,sequence:seq,received_wall_ms:received,body:s};
     parse_ws_ticker(received,e).map_err(|_|"canonical OKX ticker denied")?;
     Ok("ticker")
    },
    _=>Err("unexpected OKX channel")
   }
  },
  Feed::Depth=>{
   if v["stream"]!="btcusdt@depth20@100ms"||
      v["data"]["st"].as_u64()!=Some(1){
     return Err("wrong Binance public depth route or contract")
   }
   let f=RawFrame{source:None,venue:Venue::BinanceFutures,
    epoch:1,received_ns:1,received_wall_ms:received,raw:s.into()};
   if parse(&f).map(|u|u.base).ok().as_deref()!=Some("BTC"){
    return Err("canonical Binance depth parser denied")
   }
   Ok("depth")
  },
  Feed::Ticker=>{
   if v["stream"]!="btcusdt@ticker"{return Err("wrong Binance ticker route")}
   let body=v["data"].to_string();
   let e=PublicWsEnvelope{venue:Venue::BinanceFutures,
    url:"wss://fstream.binance.com/market/ws/btcusdt@ticker",
    base:"BTC",connection_epoch:1,sequence:seq,
    received_wall_ms:received,body:&body};
   parse_ws_ticker(received,e).map_err(|_|"canonical USD-M ticker denied")?;
   Ok("ticker")
  }
 }
}
async fn one_epoch(feed:Feed,epoch:u64,gate:&mut IngressGate)->Value{
 let mut report=json!({"epoch":epoch,"connected":false,
    "received":0,"valid":0,"ack":0,"invalid":0,
    "queue_capacity":QUEUE_CAP,"queue_overflows":0,
    "old_epoch_injection_rejected":false,"status":"FAIL","reason":null,
    "first_received_ms":null,"last_received_ms":null});
 if !gate.reset(epoch){
  report["reason"]=json!("nonmonotonic local epoch");
  return report
 }
 // Deliberately inject an OBSERVATION HEADER, not a fabricated exchange
 // message, to prove this isolated queue's previous-epoch denial.
 if epoch==2{
  let old=Header{epoch:1,sequence:1,received_ms:wall()};
  let denied=!gate.accept(old);
  report["old_epoch_injection_rejected"]=json!(denied);
  if !denied{report["reason"]=json!("old epoch admitted");return report}
 }
 let conn=tokio::time::timeout(Duration::from_secs(5),
   connect_async(feed.url())).await;
 let (mut ws,_)=match conn{
  Ok(Ok(v))=>v,
  Ok(Err(_))=>{report["reason"]=json!("connect error");return report},
  Err(_)=>{report["reason"]=json!("connect timeout");return report},
 };
 report["connected"]=json!(true);
 if matches!(feed,Feed::Okx){
  let subscribe=json!({"op":"subscribe","args":[
   {"channel":"books5","instId":"BTC-USDT"},
   {"channel":"tickers","instId":"BTC-USDT"}]});
  if ws.send(Message::Text(subscribe.to_string())).await.is_err(){
   report["reason"]=json!("subscribe send failed");return report
  }
 }
 let (tx,mut rx)=mpsc::channel::<Header>(QUEUE_CAP);
 let mut recv=0_usize;let mut valid=0_usize;let mut invalid=0_usize;
 let mut ack=0_usize;let mut overflows=0_usize;let mut last_reason=None;
 let end=Instant::now()+Duration::from_secs(EPOCH_SECONDS);
 while recv<MAX_PER_EPOCH&&Instant::now()<end{
  let left=end.saturating_duration_since(Instant::now())
    .min(Duration::from_secs(2));
  match tokio::time::timeout(left,ws.next()).await{
   Ok(Some(Ok(Message::Text(raw))))=>{
    recv+=1;
    if raw.len()>MAX_BYTES{invalid+=1;last_reason=Some("oversized frame");break}
    let now=wall();
    if report["first_received_ms"].is_null(){
     report["first_received_ms"]=json!(now)
    }
    report["last_received_ms"]=json!(now);
    match classify(feed,&raw,now,recv as u64){
     Ok("ack")=>{ack+=1;continue},
     Ok(_)=>{},
     Err(e)=>{invalid+=1;last_reason=Some(e);continue},
    }
    // Producer does not wait on a saturated consumer: fail CLOSED.
    if tx.try_send(Header{epoch,sequence:recv as u64,
       received_ms:now}).is_err(){
      overflows+=1;last_reason=Some("bounded queue full");break
    }
    while let Ok(h)=rx.try_recv(){
     if gate.accept(h){valid+=1}else{
      invalid+=1;last_reason=Some("stale, replay or reordered header");
     }
    }
   },
   Ok(Some(Ok(Message::Ping(p))))=>{
    if ws.send(Message::Pong(p)).await.is_err(){
     last_reason=Some("pong failed");break
    }
   },
   Ok(Some(Ok(Message::Close(_))))=>{
    last_reason=Some("peer closed");break
   },
   Ok(Some(Ok(_)))=>{},
   Ok(Some(Err(_)))=>{last_reason=Some("WS error");break},
   Ok(None)=>{last_reason=Some("EOF");break},
   Err(_)=>{},
  }
 }
 let _=tokio::time::timeout(Duration::from_secs(2),ws.close(None)).await;
 report["received"]=json!(recv);
 report["valid"]=json!(valid);
 report["ack"]=json!(ack);
 report["invalid"]=json!(invalid);
 report["queue_overflows"]=json!(overflows);
 report["reason"]=json!(last_reason);
 let success=valid>0&&invalid==0&&overflows==0&&last_reason.is_none()
  &&(!matches!(feed,Feed::Okx)||ack==2)
  &&(epoch!=2||report["old_epoch_injection_rejected"]==true);
 report["status"]=json!(if success{"PASS"}else{"FAIL"});
 report
}
async fn run(feed:Feed)->Value{
 let mut gate=IngressGate::default();
 let first=one_epoch(feed,1,&mut gate).await;
 // Forced close occurs in one_epoch, a fresh TLS WS is then created.
 let second=one_epoch(feed,2,&mut gate).await;
 json!({"feed":feed.label(),"socket":feed.url(),
   "cycles":[first.clone(),second.clone()],
   "forced_disconnect_between_cycles":true,
   "epoch_2_rejected_synthetic_old_header":
      second["old_epoch_injection_rejected"]==true,
   "passed":first["status"]=="PASS"&&second["status"]=="PASS",
   "total_accepted":gate.accepted,"total_denied":gate.denied,
   "installed_paper_covered":false,
   "exchange_origin_independently_attested":false})
}
#[tokio::main(flavor="multi_thread",worker_threads=3)]
async fn main()->Result<(),Box<dyn std::error::Error>>{
 if std::env::args().skip(1).collect::<Vec<_>>()!=["--r162-public-only"]{
  return Err("R162 must be run with exactly --r162-public-only".into())
 }
 let (okx,depth,ticker)=tokio::join!(
  run(Feed::Okx),run(Feed::Depth),run(Feed::Ticker));
 let pass=[&okx,&depth,&ticker].iter().all(|v|v["passed"]==true);
 println!("{}",serde_json::to_string_pretty(&json!({
  "schema":"SPOTARB/R162/ISOLATED-RECONNECT/V1","symbol":"BTC",
  "public_only":true,"private_api_calls":0,"real_orders":false,
  "installed_paper_modified":false,"three_feeds_two_epochs_pass":pass,
  "per_epoch_seconds":EPOCH_SECONDS,"per_epoch_max_messages":MAX_PER_EPOCH,
  "queue_capacity":QUEUE_CAP,"scope":"isolated diagnostic; NOT R155 ingress proof",
  "results":[okx,depth,ticker]
 }))?);
 if !pass{std::process::exit(2)}
 Ok(())
}
#[cfg(test)]
mod tests{
 use super::*;
 #[test]
 fn r162_epoch_replay_sequence_and_wall_clock_are_denied(){
  let mut g=IngressGate::default();
  assert!(g.reset(1));
  assert!(g.accept(Header{epoch:1,sequence:1,received_ms:1000}));
  assert!(!g.accept(Header{epoch:1,sequence:1,received_ms:1001}));
  assert!(!g.accept(Header{epoch:1,sequence:2,received_ms:999}));
  assert!(!g.reset(1));
  assert!(g.reset(2));
  assert!(!g.accept(Header{epoch:1,sequence:999,received_ms:2000}));
  assert!(g.accept(Header{epoch:2,sequence:1,received_ms:2000}));
  assert_eq!(g.accepted,2);
  assert_eq!(g.denied,3);
 }
 #[tokio::test]
 async fn r162_queue_overflow_is_rejected_without_overwriting_old_frame(){
  let (tx,mut rx)=mpsc::channel::<Header>(QUEUE_CAP);
  for seq in 1..=(QUEUE_CAP as u64){
   tx.try_send(Header{epoch:1,sequence:seq,received_ms:1000}).unwrap();
  }
  assert!(tx.try_send(Header{epoch:1,sequence:9,received_ms:1000}).is_err());
  assert_eq!(rx.try_recv().unwrap().sequence,1);
  assert_eq!(rx.try_recv().unwrap().sequence,2);
  assert_eq!(rx.try_recv().unwrap().sequence,3);
 }
 #[test]
 fn r162_public_endpoints_are_static_and_disjoint(){
  assert_ne!(DEPTH,TICKER);
  for url in [OKX,DEPTH,TICKER]{
   assert!(url.starts_with("wss://"));
   assert!(!url.contains("/private"));
   assert!(!url.contains("/order"));
   assert!(!url.contains(":8443"));
  }
 }
}
