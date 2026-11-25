//! R6 review-only OKX forced reconnect probe. Public market data only; no trading.
use futures_util::{SinkExt,StreamExt};
use serde_json::{json,Value};
use spotarb_native_market_core::{parse,RawFrame,Venue};
use std::time::{Duration,Instant,SystemTime,UNIX_EPOCH};
use tokio_tungstenite::{connect_async,tungstenite::Message};
fn wall()->i64{SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64}
async fn connect(bs:&[&str])->Result<tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,String>{
 let (mut ws,_)=connect_async("wss://ws.okx.com:8443/ws/v5/public").await.map_err(|e|e.to_string())?;
 let args=bs.iter().map(|b|json!({"channel":"books5","instId":format!("{b}-USDT")})).collect::<Vec<_>>();
 ws.send(Message::Text(json!({"op":"subscribe","args":args}).to_string())).await.map_err(|e|e.to_string())?;
 Ok(ws)
}
async fn phase(bs:&[&str],epoch:u64,seconds:u64,start:Instant)->Result<Value,String>{
 let mut ws=connect(bs).await?;let end=Instant::now()+Duration::from_secs(seconds);
 let mut received=0u64;let mut parsed=0u64;let mut control=0u64;let mut seen=std::collections::HashSet::new();
 while Instant::now()<end{
  match tokio::time::timeout(Duration::from_secs(2),ws.next()).await{
   Ok(Some(Ok(Message::Text(raw))))=>{
    let v:Value=serde_json::from_str(&raw).unwrap_or(Value::Null);
    if v.get("event").is_some(){control+=1;continue}
    received+=1;let f=RawFrame{source:None,venue:Venue::OkxSpot,epoch,received_ns:start.elapsed().as_nanos() as u64+1,received_wall_ms:wall(),raw};
    let u=parse(&f).map_err(|e|format!("{e:?}"))?;parsed+=1;seen.insert(u.base);
   }
   Ok(Some(Ok(Message::Ping(x))))=>{ws.send(Message::Pong(x)).await.map_err(|e|e.to_string())?;}
   Ok(Some(Ok(_)))|Err(_)=>{}
   Ok(Some(Err(e)))=>return Err(e.to_string()),Ok(None)=>break
  }
 }
 let _=ws.close(None).await;
 Ok(json!({"epoch":epoch,"received":received,"parsed":parsed,"control":control,"seen":seen.len()}))
}
#[tokio::main]
async fn main(){
 let bs=["BTC","ETH","SOL","XRP","DOGE","ADA","AVAX","LINK"];let start=Instant::now();
 let a=phase(&bs,1,10,start).await.unwrap();let b=phase(&bs,2,10,start).await.unwrap();
 println!("{}",json!({"r6_okx_forced_reconnect":true,"trading":false,"requested":bs.len(),"phase1":a,"phase2":b,"reconnects":1}));
}