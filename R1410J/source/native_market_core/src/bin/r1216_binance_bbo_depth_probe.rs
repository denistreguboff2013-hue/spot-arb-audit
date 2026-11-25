use futures_util::{SinkExt,StreamExt};
use serde_json::{json,Value};
use std::{collections::HashMap,time::{Duration,Instant,SystemTime,UNIX_EPOCH}};
use tokio_tungstenite::{connect_async,tungstenite::Message};

fn wall_ms()->i64{SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as i64}
#[derive(Default)]struct Marks{bbo:Vec<(u64,u64)>,depth:Vec<(u64,u64)>,bbo_age:Vec<i64>,depth_age:Vec<i64>,bbo_transaction_age:Vec<i64>,depth_transaction_age:Vec<i64>,invalid:u64,duplicate_or_reordered:u64,last_bbo:Option<u64>,last_depth:Option<u64>}
fn pct(v:&mut [i64],p:usize)->Option<i64>{if v.is_empty(){return None}v.sort_unstable();Some(v[(v.len()-1)*p/100])}
fn matched_deltas_ns(m:&Marks)->Vec<i64>{
 let mut bbo=HashMap::<u64,Vec<u64>>::new();let mut depth=HashMap::<u64,Vec<u64>>::new();
 for (id,rx) in &m.bbo{bbo.entry(*id).or_default().push(*rx)}
 for (id,rx) in &m.depth{depth.entry(*id).or_default().push(*rx)}
 let mut out=Vec::new();for(id,mut a)in bbo{if let Some(mut b)=depth.remove(&id){a.sort_unstable();b.sort_unstable();out.extend(a.into_iter().zip(b).map(|(x,y)|y as i64-x as i64));}}out
}
fn quantiles(mut v:Vec<i64>)->Value{let n=v.len();let max=pct(&mut v,100);json!({"n":n,"p50":pct(&mut v,50),"p95":pct(&mut v,95),"p99":pct(&mut v,99),"max":max})}
#[tokio::main(flavor="multi_thread",worker_threads=2)]
async fn main(){
 let secs=std::env::args().nth(1).and_then(|x|x.parse::<u64>().ok()).unwrap_or(60).clamp(10,300);
 let streams=["btcusdt@bookTicker","btcusdt@depth20@500ms","ethusdt@bookTicker","ethusdt@depth20@500ms"].join("/");
 let url=format!("wss://fstream.binance.com/public/stream?streams={streams}");
 let started=Instant::now();let result=tokio::time::timeout(Duration::from_secs(10),connect_async(url.clone())).await;
 let (mut ws,_)=match result{Ok(Ok(v))=>v,Ok(Err(e))=>{println!("{}",json!({"schema":"R1216_BINANCE_BBO_DEPTH_V1","url":url,"connected":false,"error":e.to_string()}));return},Err(_)=>{println!("{}",json!({"schema":"R1216_BINANCE_BBO_DEPTH_V1","url":url,"connected":false,"error":"connect_timeout"}));return}};
 let connect_ms=started.elapsed().as_millis();
 let mut rows:HashMap<String,Marks>=[("BTC".to_string(),Marks::default()),("ETH".to_string(),Marks::default())].into_iter().collect();
 let run=Instant::now();let mut received=0u64;let mut close_reason="completed".to_string();
 while run.elapsed()<Duration::from_secs(secs){
  match tokio::time::timeout(Duration::from_secs(2),ws.next()).await{
   Err(_)=>continue,
   Ok(Some(Ok(Message::Text(raw))))=>{
    let Ok(v)=serde_json::from_str::<Value>(&raw)else{continue};
    let Some(stream)=v["stream"].as_str()else{continue};
    let Some(d)=v.get("data")else{continue};
    let Some(symbol)=d["s"].as_str().and_then(|s|s.strip_suffix("USDT"))else{continue};
    if symbol!="BTC"&&symbol!="ETH"{continue}
    let Some(seq)=d["u"].as_u64()else{rows.get_mut(symbol).unwrap().invalid+=1;continue};
    let Some(event_ms)=d["E"].as_i64()else{rows.get_mut(symbol).unwrap().invalid+=1;continue};
    let transaction_ms=d["T"].as_i64();
    let now_ms=wall_ms();let rx=run.elapsed().as_nanos().min(u64::MAX as u128)as u64;let age=now_ms-event_ms;let transaction_age=transaction_ms.map(|t|now_ms-t);let row=rows.get_mut(symbol).unwrap();received+=1;
    if stream.ends_with("@bookTicker"){
     if row.last_bbo.is_some_and(|p|seq<=p){row.duplicate_or_reordered+=1}row.last_bbo=Some(seq);row.bbo.push((seq,rx));row.bbo_age.push(age);if let Some(a)=transaction_age{row.bbo_transaction_age.push(a)}
    }else if stream.contains("@depth20@500ms"){
     if row.last_depth.is_some_and(|p|seq<=p){row.duplicate_or_reordered+=1}row.last_depth=Some(seq);row.depth.push((seq,rx));row.depth_age.push(age);if let Some(a)=transaction_age{row.depth_transaction_age.push(a)}
    }
   },
   Ok(Some(Ok(Message::Ping(b))))=>{if ws.send(Message::Pong(b)).await.is_err(){close_reason="pong_send_failed".into();break}},
   Ok(Some(Ok(Message::Close(_))))|Ok(None)=>{close_reason="peer_closed".into();break},
   Ok(Some(Err(e)))=>{close_reason=format!("read_error:{e}");break},
   _=>{}
  }
 }
 let _=tokio::time::timeout(Duration::from_secs(1),ws.close(None)).await;
 let mut results=serde_json::Map::new();
 for (symbol,m) in rows{
  let deltas=matched_deltas_ns(&m);
  let matched=deltas.len();
  let abs=quantiles(deltas.iter().map(|x|x.abs()/1_000_000).collect());
  let bbo_age_outside=m.bbo_age.iter().filter(|x|**x>800||**x<0).count();
  let depth_age_outside=m.depth_age.iter().filter(|x|**x>800||**x<0).count();
  let bbo_age=quantiles(m.bbo_age);
  let depth_age=quantiles(m.depth_age);
  let bbo_transaction_age=quantiles(m.bbo_transaction_age);
  let depth_transaction_age=quantiles(m.depth_transaction_age);
  results.insert(symbol,json!({"bookTicker_messages":m.bbo.len(),"depth20_500ms_messages":m.depth.len(),"matched_update_ids":matched,"absolute_receive_delta_ms":abs,"bookTicker_event_age_ms":bbo_age,"depth20_event_age_ms":depth_age,"bookTicker_transaction_age_ms":bbo_transaction_age,"depth20_transaction_age_ms":depth_transaction_age,"invalid":m.invalid,"bbo_age_outside_0_800ms":bbo_age_outside,"depth_age_outside_0_800ms":depth_age_outside,"duplicate_or_nonincreasing_ids":m.duplicate_or_reordered}));
 }
 println!("{}",json!({"schema":"R1216_BINANCE_BBO_DEPTH_V1","url":url,"public_only":true,"duration_requested_s":secs,"elapsed_s":run.elapsed().as_secs_f64(),"connect_ms":connect_ms,"messages":received,"close_reason":close_reason,"instrument_results":results,"trade":false,"orders":0}));
}
#[cfg(test)]mod r1216_match_tests{use super::*;#[test]fn only_shared_update_ids_are_compared(){let m=Marks{bbo:vec![(10,100),(11,110)],depth:vec![(10,120),(99,130)],..Default::default()};assert_eq!(matched_deltas_ns(&m),vec![20]);}}
