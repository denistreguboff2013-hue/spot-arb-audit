//! R6 review-only real exchange WebSocket shadow. No credentials, orders, or trading output.
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use spotarb_native_market_core::{parse, RawFrame, Venue};
use std::{collections::HashSet, env, time::{Duration, Instant, SystemTime, UNIX_EPOCH}};
use tokio_tungstenite::{connect_async, tungstenite::Message};

fn wall_ms()->i64 { SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64 }
fn mono_ns(start:Instant)->u64 { start.elapsed().as_nanos() as u64 + 1 }
fn bases()->Vec<String>{
    let raw=env::var("R6_BASES").unwrap_or_else(|_|"BTC,ETH,SOL".into());
    let mut out=Vec::new();
    for x in raw.split(',') {
        let b=x.trim().to_ascii_uppercase();
        assert!(!b.is_empty() && b.bytes().all(|c|c.is_ascii_uppercase()||c.is_ascii_digit()));
        if !out.contains(&b){out.push(b)}
    }
    assert!(!out.is_empty() && out.len()<=256); out
}
async fn binance(bs:Vec<String>,secs:u64,start:Instant)->Value{
    let streams=bs.iter().map(|b|format!("{}usdt@bookTicker",b.to_ascii_lowercase())).collect::<Vec<_>>();
    let url=format!("wss://fstream.binance.com/stream?streams={}",streams.join("/"));
    let deadline=Instant::now()+Duration::from_secs(secs); let mut n=0u64;let mut parsed=0u64;let mut errors=0u64;let mut seen=HashSet::new();
    let mut epoch=0u64;let mut reconnects=0u64;let mut forced=false;
    let (mut ws,_)=match connect_async(&url).await {Ok(x)=>{epoch=1;x},Err(e)=>return json!({"venue":"binance","error":format!("connect:{e}")})};
    let mut error_samples:Vec<String>=Vec::new();
    while Instant::now()<deadline {
        if !forced && start.elapsed()>=Duration::from_secs(secs/2) {
            let _=ws.close(None).await; forced=true;
            if let Ok(x)=connect_async(&url).await { ws=x.0; epoch+=1; reconnects+=1; } else { errors+=1; break; }
        }
        let left=deadline.saturating_duration_since(Instant::now()).min(Duration::from_secs(2));
        match tokio::time::timeout(left,ws.next()).await {
            Ok(Some(Ok(Message::Text(raw))))=>{
                n+=1; let f=RawFrame{source:None,venue:Venue::BinanceFutures,epoch,received_ns:mono_ns(start),received_wall_ms:wall_ms(),raw};
                match parse(&f){Ok(u)=>{parsed+=1;seen.insert(u.base);},Err(e)=>{errors+=1;if error_samples.len()<8{error_samples.push(format!("{e:?}"));}}}
            },
            Ok(Some(Ok(Message::Ping(p))))=>{let _=ws.send(Message::Pong(p)).await;},
            Ok(Some(Ok(_)))=>{}, Ok(Some(Err(_)))|Ok(None)=>{errors+=1;break}, Err(_)=>{}
        }
    }
    let _=ws.close(None).await;
    json!({"venue":"binance","received":n,"parsed":parsed,"errors":errors,"seen":seen.len(),"requested":bs.len(),"error_samples":error_samples,"epoch":epoch,"reconnects":reconnects})
}
async fn okx(bs:Vec<String>,secs:u64,start:Instant)->Value{
    let (mut ws,_)=match connect_async("wss://ws.okx.com:8443/ws/v5/public").await {Ok(x)=>x,Err(e)=>return json!({"venue":"okx","error":format!("connect:{e}")})};
    let args=bs.iter().map(|b|json!({"channel":"books5","instId":format!("{b}-USDT")})).collect::<Vec<_>>();
    if let Err(e)=ws.send(Message::Text(json!({"op":"subscribe","args":args}).to_string())).await{return json!({"venue":"okx","error":format!("subscribe:{e}")})}
    let deadline=Instant::now()+Duration::from_secs(secs);let mut n=0u64;let mut parsed=0u64;let mut control=0u64;let mut errors=0u64;let mut seen=HashSet::new();let mut error_samples:Vec<String>=Vec::new();
    while Instant::now()<deadline {
        let left=deadline.saturating_duration_since(Instant::now()).min(Duration::from_secs(2));
        match tokio::time::timeout(left,ws.next()).await {
            Ok(Some(Ok(Message::Text(raw))))=>{
                let v:Value=serde_json::from_str(&raw).unwrap_or(Value::Null);
                if v.get("event").is_some(){control+=1;continue}
                n+=1;let f=RawFrame{source:None,venue:Venue::OkxSpot,epoch:1,received_ns:mono_ns(start),received_wall_ms:wall_ms(),raw};
                match parse(&f){Ok(u)=>{parsed+=1;seen.insert(u.base);},Err(e)=>{errors+=1;if error_samples.len()<8{error_samples.push(format!("{e:?}"));}}}
            },
            Ok(Some(Ok(Message::Ping(p))))=>{let _=ws.send(Message::Pong(p)).await;},
            Ok(Some(Ok(_)))=>{},Ok(Some(Err(_)))|Ok(None)=>{errors+=1;break},Err(_)=>{}
        }
    }
    let _=ws.close(None).await;
    json!({"venue":"okx","received":n,"parsed":parsed,"control":control,"errors":errors,"seen":seen.len(),"requested":bs.len(),"error_samples":error_samples})
}
#[tokio::main(flavor="multi_thread",worker_threads=2)]
async fn main(){
    let secs: u64=env::var("R6_SECONDS").ok().and_then(|x|x.parse().ok()).unwrap_or(15);
    assert!((5..=120).contains(&secs));let bs=bases();let start=Instant::now();
    let (b,o)=tokio::join!(binance(bs.clone(),secs,start),okx(bs.clone(),secs,start));
    println!("{}",json!({"r6_shadow":true,"duration_s":secs,"bases":bs,"binance":b,"okx":o,"trading":false}));
}
