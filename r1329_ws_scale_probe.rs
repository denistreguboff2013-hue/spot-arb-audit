// R1329 isolated full-intersection public BBO observation. No trading or durable writes.
use futures_util::{SinkExt,StreamExt,stream::{select_all,BoxStream}};
use serde_json::{json,Value};
use std::{collections::HashMap,time::{Duration,Instant,SystemTime,UNIX_EPOCH}};
use tokio_tungstenite::{connect_async,tungstenite::Message};
fn now()->u64{SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64}
fn num(v:&Value,k:&str)->Option<f64>{v.get(k)?.as_str()?.parse::<f64>().ok().filter(|x|x.is_finite()&&*x>0.)}
#[derive(Clone,Copy)]struct Q{bid:f64,ask:f64,recv:u64,exchange:Option<u64>}
fn rss()->u64{std::fs::read_to_string("/proc/self/status").ok().and_then(|s|s.lines().find(|l|l.starts_with("VmRSS:")).and_then(|l|l.split_whitespace().nth(1)).and_then(|v|v.parse().ok())).unwrap_or(0)}
fn bq(v:&Value)->Option<(String,Q)>{
 let d=v.get("data").unwrap_or(v);let s=d["s"].as_str()?.strip_suffix("USDT")?.to_string();
 let (bid,ask)=(num(d,"b")?,num(d,"a")?);if bid>ask{return None}
 Some((s,Q{bid,ask,recv:now(),exchange:d["E"].as_u64()}))
}
fn oq(v:&Value)->Option<(String,Q)>{
 if v["arg"]["channel"].as_str()?!="tickers"{return None}
 let d=v["data"].as_array()?.first()?;let s=d["instId"].as_str()?.strip_suffix("-USDT")?.to_string();
 let (bid,ask)=(num(d,"bidPx")?,num(d,"askPx")?);if bid>ask{return None}
 Some((s,Q{bid,ask,recv:now(),exchange:d["ts"].as_str().and_then(|x|x.parse().ok())}))
}
#[tokio::main(flavor="current_thread")]
async fn main(){
 let path=std::env::args().nth(1).expect("symbols file required");
 let seconds=std::env::args().nth(2).and_then(|s|s.parse::<u64>().ok()).unwrap_or(30).clamp(10,300);
 let bases=std::fs::read_to_string(&path).expect("symbols file");
 let bases=bases.lines().filter(|s|!s.is_empty()&&s.len()<20&&s.bytes().all(|b|b.is_ascii_uppercase()||b.is_ascii_digit())).collect::<Vec<_>>();
 if bases.is_empty()||bases.len()>300{panic!("symbol count out of bounds")}
 let mut streams=Vec::<BoxStream<'static,(usize,Result<Message,String>)>>::new();
 let mut connections=0;
 for (chunk_idx,chunk) in bases.chunks(70).enumerate(){
  let streams_path=chunk.iter().map(|s|format!("{}usdt@bookTicker",s.to_lowercase())).collect::<Vec<_>>().join("/");
  let url=format!("wss://fstream.binance.com/stream?streams={streams_path}");
  match tokio::time::timeout(Duration::from_secs(12),connect_async(&url)).await{
   Ok(Ok((ws,_)))=>{streams.push(Box::pin(ws.map(move |r|(chunk_idx,r.map_err(|e|e.to_string())))));connections+=1},
   _=>{println!("{}",json!({"kind":"error","venue":"binance","chunk":chunk_idx}));std::process::exit(2)}
  }
  tokio::time::sleep(Duration::from_millis(150)).await;
 }
 for (chunk_idx,chunk) in bases.chunks(70).enumerate(){
  match tokio::time::timeout(Duration::from_secs(12),connect_async("wss://ws.okx.com:8443/ws/v5/public")).await{
   Ok(Ok((mut ws,_)))=>{
    let args=chunk.iter().map(|s|json!({"channel":"tickers","instId":format!("{s}-USDT")})).collect::<Vec<_>>();
    if ws.send(Message::Text(json!({"op":"subscribe","args":args}).to_string())).await.is_err(){std::process::exit(3)}
    streams.push(Box::pin(ws.map(move |r|(chunk_idx+100,r.map_err(|e|e.to_string())))));connections+=1
   },_=>{println!("{}",json!({"kind":"error","venue":"okx","chunk":chunk_idx}));std::process::exit(4)}
  }
  tokio::time::sleep(Duration::from_millis(150)).await;
 }
 let mut disconnected=false;let mut all=select_all(streams);let(mut b,mut o)=(HashMap::<String,Q>::new(),HashMap::<String,Q>::new());
 let(mut bc,mut oc,mut bad,mut control)=(0u64,0u64,0u64,0u64);
 let start=Instant::now();let deadline=tokio::time::Instant::now()+Duration::from_secs(seconds);
 let mut ticker=tokio::time::interval(Duration::from_secs(5));
 println!("{}",json!({"kind":"start","symbols":bases.len(),"connections":connections,"seconds":seconds,"mode":"PUBLIC_WS_BBO_ONLY","orders":false}));
 loop{tokio::select!{
  _=tokio::time::sleep_until(deadline)=>break,
  _=ticker.tick()=>{
   let t=now();let fresh=b.iter().filter(|(s,q)|o.get(*s).map_or(false,|p|t.saturating_sub(q.recv)<=800&&t.saturating_sub(p.recv)<=800)).count();
   let old=b.iter().filter(|(_,q)|t.saturating_sub(q.recv)>800).count();
   let old_ok=o.iter().filter(|(_,q)|t.saturating_sub(q.recv)>800).count();
   println!("{}",json!({"kind":"stats","elapsed_ms":start.elapsed().as_millis(),"bn_frames":bc,"okx_frames":oc,"bad":bad,"control":control,"bn_symbols":b.len(),"okx_symbols":o.len(),"fresh_pairs":fresh,"stale_bn":old,"stale_okx":old_ok,"rss_kb":rss()}));
  },
  frame=all.next()=>{
   match frame{
    Some((venue,Ok(Message::Text(t))))=>{
     if let Ok(v)=serde_json::from_str::<Value>(&t){
      if venue<100{bc+=1;if let Some((s,q))=bq(&v){b.insert(s,q);}else{bad+=1}}
      else if v.get("event").is_some(){control+=1;if v["event"]=="error"{bad+=1;println!("{}",json!({"kind":"subscription_error","data":v}));}}
      else if let Some((s,q))=oq(&v){oc+=1;o.insert(s,q);}
      else{bad+=1}
     }else{bad+=1}
    },
    Some((_,Ok(Message::Ping(_))))=>{control+=1},
    Some((_,Ok(_)))=>{control+=1},
    Some((venue,Err(e)))=>{println!("{}",json!({"kind":"socket_error","venue":venue,"error":e}));bad+=1;disconnected=true;break},
    None=>{bad+=1;disconnected=true;break}
   }
  }
 }}
 println!("{}",json!({"kind":"summary","elapsed_ms":start.elapsed().as_millis(),"bn_frames":bc,"okx_frames":oc,"bad":bad,"bn_symbols":b.len(),"okx_symbols":o.len(),"rss_kb":rss(),"orders":false,"disconnected":disconnected}));
 if disconnected{std::process::exit(5)}
}
