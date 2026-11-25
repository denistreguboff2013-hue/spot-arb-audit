// R1329: isolated public WS BBO observation, no trading/storage integration.
use futures_util::{SinkExt,StreamExt};
use serde_json::{json,Value};
use std::{collections::HashMap,time::{Duration,Instant,SystemTime,UNIX_EPOCH}};
use tokio_tungstenite::{connect_async,tungstenite::Message};
fn now()->u64{SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64}
fn px(v:&Value,k:&str)->Option<f64>{v.get(k)?.as_str()?.parse::<f64>().ok().filter(|x|x.is_finite()&&*x>0.)}
#[derive(Clone,Copy)]
struct Quote{bid:f64,ask:f64,recv:u64,exchange:Option<u64>}
fn rss()->u64{std::fs::read_to_string("/proc/self/status").ok().and_then(|s|s.lines().find(|l|l.starts_with("VmRSS:")).and_then(|l|l.split_whitespace().nth(1)).and_then(|v|v.parse().ok())).unwrap_or(0)}
fn bquote(v:&Value)->Option<(String,Quote)>{
 let d=v.get("data").unwrap_or(v);
 let s=d["s"].as_str()?.strip_suffix("USDT")?.to_string();
 let (bid,ask)=(px(d,"b")?,px(d,"a")?);if bid>ask{return None}
 Some((s,Quote{bid,ask,recv:now(),exchange:d["E"].as_u64()}))
}
fn oquote(v:&Value)->Option<(String,Quote)>{
 if v["arg"]["channel"].as_str()?!="tickers"{return None}
 let d=v["data"].as_array()?.first()?;
 let s=d["instId"].as_str()?.strip_suffix("-USDT")?.to_string();
 let (bid,ask)=(px(d,"bidPx")?,px(d,"askPx")?);if bid>ask{return None}
 Some((s,Quote{bid,ask,recv:now(),exchange:d["ts"].as_str().and_then(|x|x.parse().ok())}))
}
#[tokio::main(flavor="current_thread")]
async fn main(){
 let seconds=std::env::args().nth(1).and_then(|s|s.parse::<u64>().ok()).unwrap_or(30).clamp(5,300);
 let bases=["BTC","ETH","SOL","XRP","DOGE","ADA","LINK","LTC","AVAX","DOT"];
 let streams=bases.iter().map(|s|format!("{}usdt@bookTicker",s.to_lowercase())).collect::<Vec<_>>().join("/");
 let bu=format!("wss://fstream.binance.com/stream?streams={streams}");
 let bn=tokio::time::timeout(Duration::from_secs(12),connect_async(&bu)).await;
 let ok=tokio::time::timeout(Duration::from_secs(12),connect_async("wss://ws.okx.com:8443/ws/v5/public")).await;
 let (mut bn,mut ok)=match(bn,ok){(Ok(Ok((b,_))),Ok(Ok((o,_))))=>(b,o),_=>{println!("{}",json!({"kind":"error","reason":"ws_connect_failed"}));std::process::exit(2)}};
 let args=bases.iter().map(|s|json!({"channel":"tickers","instId":format!("{s}-USDT")})).collect::<Vec<_>>();
 if ok.send(Message::Text(json!({"op":"subscribe","args":args}).to_string())).await.is_err(){std::process::exit(3)}
 let mut b=HashMap::<String,Quote>::new();let mut o=HashMap::<String,Quote>::new();
 let(mut bc,mut oc,mut bad,mut signals)=(0u64,0u64,0u64,0u64);
 let mut seen=HashMap::<String,Instant>::new();
 let start=Instant::now();let deadline=tokio::time::Instant::now()+Duration::from_secs(seconds);
 let mut ticker=tokio::time::interval(Duration::from_secs(5));
 println!("{}",json!({"kind":"start","mode":"PUBLIC_WS_BBO_ONLY","seconds":seconds,"symbols":bases,"orders":false}));
 loop{
  tokio::select!{
   _=tokio::time::sleep_until(deadline)=>break,
   _=ticker.tick()=>{
    let current=now();let fresh=b.iter().filter(|(s,q)|o.get(*s).map_or(false,|p|current.saturating_sub(q.recv)<=800&&current.saturating_sub(p.recv)<=800)).count();
    println!("{}",json!({"kind":"stats","elapsed_ms":start.elapsed().as_millis(),"binance_frames":bc,"okx_frames":oc,"bad":bad,"bn_symbols":b.len(),"okx_symbols":o.len(),"fresh_pairs":fresh,"rss_kb":rss(),"signals":signals}));
   },
   frame=bn.next()=>{
    match frame{Some(Ok(Message::Text(t)))=>{bc+=1;if let Ok(v)=serde_json::from_str::<Value>(&t){if let Some((s,q))=bquote(&v){b.insert(s,q);}else{bad+=1}}else{bad+=1}},
     Some(Ok(Message::Ping(p)))=>{let _=bn.send(Message::Pong(p)).await;},
     Some(Ok(_))=>{},_=>{bad+=1;break}}
   },
   frame=ok.next()=>{
    match frame{Some(Ok(Message::Text(t)))=>{if let Ok(v)=serde_json::from_str::<Value>(&t){if v["event"]=="error"{bad+=1}else if let Some((s,q))=oquote(&v){oc+=1;o.insert(s,q);} }else{bad+=1}},
     Some(Ok(Message::Ping(p)))=>{let _=ok.send(Message::Pong(p)).await;},
     Some(Ok(_))=>{},_=>{bad+=1;break}}
   }
  }
  let current=now();
  for(s,q) in &b{if let Some(p)=o.get(s){
   if current.saturating_sub(q.recv)>800||current.saturating_sub(p.recv)>800{continue}
   if let (Some(bt),Some(ot))=(q.exchange,p.exchange){if current.saturating_sub(bt)>800||current.saturating_sub(ot)>800{continue}}
   let gross=(q.bid/p.ask-1.)*10000.;
   if gross>=10.&&seen.get(s).map_or(true,|t|t.elapsed()>Duration::from_secs(5)){
    seen.insert(s.clone(),Instant::now());signals+=1;
    println!("{}",json!({"kind":"candidate","symbol":s,"gross_bps":gross,"bn_recv_age_ms":current.saturating_sub(q.recv),"okx_recv_age_ms":current.saturating_sub(p.recv),"bn_exchange_age_ms":q.exchange.map(|t|current.saturating_sub(t)),"okx_exchange_age_ms":p.exchange.map(|t|current.saturating_sub(t)),"size_checked":false,"orders":false}));
   }
  }}
 }
 println!("{}",json!({"kind":"summary","elapsed_ms":start.elapsed().as_millis(),"binance_frames":bc,"okx_frames":oc,"bad":bad,"bn_symbols":b.len(),"okx_symbols":o.len(),"signals":signals,"rss_kb":rss(),"orders":false}));
}
