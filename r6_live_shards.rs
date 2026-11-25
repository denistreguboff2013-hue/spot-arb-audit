//! R6 live sharded WS gate. Public data only; no credentials/orders/trading.
use futures_util::{SinkExt,StreamExt};use serde_json::{json,Value};use spotarb_native_market_core::{parse,RawFrame,Venue};
use std::{env,collections::HashSet,time::{Duration,Instant,SystemTime,UNIX_EPOCH}};use tokio_tungstenite::{connect_async,tungstenite::Message};
fn wall()->i64{SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64}
async fn b(id:usize,bs:Vec<String>,secs:u64,start:Instant)->Value{
 let s=bs.iter().map(|x|format!("{}usdt@bookTicker",x.to_lowercase())).collect::<Vec<_>>();let u=format!("wss://fstream.binance.com/stream?streams={}",s.join("/"));
 let (mut w,_)=match connect_async(u).await{Ok(x)=>x,Err(e)=>return json!({"id":id,"connect_error":e.to_string()})};let end=Instant::now()+Duration::from_secs(secs);let(mut n,mut p,mut e)=(0u64,0u64,0u64);let mut seen=HashSet::new();
 while Instant::now()<end{match tokio::time::timeout(Duration::from_secs(2),w.next()).await{Ok(Some(Ok(Message::Text(raw))))=>{n+=1;let f=RawFrame{source:None,venue:Venue::BinanceFutures,epoch:1,received_ns:start.elapsed().as_nanos() as u64+1,received_wall_ms:wall(),raw};match parse(&f){Ok(x)=>{p+=1;seen.insert(x.base);},Err(_)=>e+=1}},Ok(Some(Ok(Message::Ping(x))))=>{let _=w.send(Message::Pong(x)).await;},Ok(Some(Ok(_)))|Err(_)=>{},_=>{e+=1;break}}}
 json!({"id":id,"requested":bs.len(),"seen":seen.len(),"received":n,"parsed":p,"errors":e})
}
async fn o(id:usize,bs:Vec<String>,secs:u64,start:Instant)->Value{
 let (mut w,_)=match connect_async("wss://ws.okx.com:8443/ws/v5/public").await{Ok(x)=>x,Err(e)=>return json!({"id":id,"connect_error":e.to_string()})};
 let a=bs.iter().map(|x|json!({"channel":"books5","instId":format!("{x}-USDT")})).collect::<Vec<_>>();if w.send(Message::Text(json!({"op":"subscribe","args":a}).to_string())).await.is_err(){return json!({"id":id,"subscribe_error":true})}
 let end=Instant::now()+Duration::from_secs(secs);let(mut n,mut p,mut e,mut ctl)=(0u64,0u64,0u64,0u64);let mut seen=HashSet::new();
 while Instant::now()<end{match tokio::time::timeout(Duration::from_secs(2),w.next()).await{Ok(Some(Ok(Message::Text(raw))))=>{let v:Value=serde_json::from_str(&raw).unwrap_or(Value::Null);if v.get("event").is_some(){ctl+=1;continue}n+=1;let f=RawFrame{source:None,venue:Venue::OkxSpot,epoch:1,received_ns:start.elapsed().as_nanos() as u64+1,received_wall_ms:wall(),raw};match parse(&f){Ok(x)=>{p+=1;seen.insert(x.base);},Err(_)=>e+=1}},Ok(Some(Ok(Message::Ping(x))))=>{let _=w.send(Message::Pong(x)).await;},Ok(Some(Ok(_)))|Err(_)=>{},_=>{e+=1;break}}}
 json!({"id":id,"requested":bs.len(),"seen":seen.len(),"received":n,"parsed":p,"errors":e,"control":ctl})
}
#[tokio::main(flavor="multi_thread",worker_threads=4)]async fn main(){
 let bs=env::var("R6_BASES").unwrap().split(',').map(|x|x.to_string()).collect::<Vec<_>>();let secs=env::var("R6_SECONDS").ok().and_then(|x|x.parse().ok()).unwrap_or(30);let start=Instant::now();let mut hs=Vec::new();
 for (id,ch) in bs.chunks(32).enumerate(){let v=ch.to_vec();hs.push(tokio::spawn(b(id,v.clone(),secs,start)));hs.push(tokio::spawn(o(id,v,secs,start)));}
 let mut bin=Vec::new();let mut okx=Vec::new();for (i,h) in hs.into_iter().enumerate(){let v=h.await.unwrap();if i%2==0{bin.push(v)}else{okx.push(v)}}
 println!("{}",json!({"r6_live_shards":true,"trading":false,"universe":bs.len(),"shards_per_venue":bin.len(),"binance":bin,"okx":okx}));
}