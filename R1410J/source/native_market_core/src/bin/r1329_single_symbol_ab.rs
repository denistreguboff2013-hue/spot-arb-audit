// Standalone read-only public WS experiment: one symbol, 2 sockets per endpoint, no orders.
use futures_util::StreamExt;
use serde_json::{json,Value};
use std::{collections::{HashMap,VecDeque},time::{Duration,Instant,SystemTime,UNIX_EPOCH}};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async,tungstenite::Message};
fn now_ms()->u64{SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64}
#[derive(Clone,Copy)]struct Tick{socket:usize,id:u64,ex:u64,recv:u64,parse_us:u64,send_wait_us:u64}
enum Ev{Tick(Tick),End{socket:usize,why:String}}
async fn socket_task(socket:usize,symbol:String,tx:mpsc::Sender<Ev>){
 let path=if socket<2{"public/stream"}else{"stream"};
 let url=format!("wss://fstream.binance.com/{path}?streams={}usdt@bookTicker",symbol.to_lowercase());
 let connection=tokio::time::timeout(Duration::from_secs(12),connect_async(&url)).await;
 let mut ws=match connection {Ok(Ok((ws,_)))=>ws,_=>{let _=tx.send(Ev::End{socket,why:"connect_failed".into()}).await;return}};
 loop{
  let frame=tokio::time::timeout(Duration::from_secs(15),ws.next()).await;
  let received=now_ms();
  let msg=match frame{Ok(Some(Ok(msg)))=>msg,Ok(None)=>{let _=tx.send(Ev::End{socket,why:"eof".into()}).await;return},Ok(Some(Err(e)))=>{let reason=format!("read_error:{e}");let _=tx.send(Ev::End{socket,why:reason.chars().take(140).collect()}).await;return},Err(_)=>{let _=tx.send(Ev::End{socket,why:"idle_15s".into()}).await;return}};
  match msg {
   Message::Text(t)=>{if let Ok(v)=serde_json::from_str::<Value>(&t){let d=v.get("data").unwrap_or(&v);if let (Some(id),Some(ex))=(d["u"].as_u64(),d["E"].as_u64()){let parsed=now_ms();let before_send=Instant::now();let send=tx.send(Ev::Tick(Tick{socket,id,ex,recv:received,parse_us:(parsed-received)*1000,send_wait_us:0})).await;if send.is_err(){return}let _send_wait=before_send.elapsed();}}},
   Message::Close(_)=>{let _=tx.send(Ev::End{socket,why:"close".into()}).await;return},
   _=>{}
  }
 }
}
fn bucket(ms:u64)->usize{if ms<=1{0}else if ms<=9{1}else if ms<=49{2}else if ms<=199{3}else if ms<=799{4}else if ms<=1999{5}else if ms<=9999{6}else{7}}
#[tokio::main(flavor="multi_thread",worker_threads=4)]
async fn main(){
 let symbol=std::env::args().nth(1).unwrap_or("BTC".into());assert!(symbol.bytes().all(|c|c.is_ascii_uppercase()));
 let seconds=std::env::args().nth(2).and_then(|s|s.parse::<u64>().ok()).unwrap_or(60).clamp(10,900);
 let(tx,mut rx)=mpsc::channel::<Ev>(8192);
 let mut handles=Vec::new();for socket in 0..4{handles.push(tokio::spawn(socket_task(socket,symbol.clone(),tx.clone())))}drop(tx);
 let mut counts=[0u64;4];let mut stale=[0u64;4];let mut max_age=[0u64;4];let mut future=[0u64;4];
 let mut per_sec_counts=[[0u64;4];900];let mut per_sec_stale=[[0u64;4];900];
 let mut per_sec_age_max=[[0u64;4];900];let mut first_age=[None::<u64>;4];let mut last_age=[None::<u64>;4];
 let mut max_parse_us=[0u64;4];let mut max_consumer_queue_us=[0u64;4];
 let mut ends=std::array::from_fn::<String,4,_>(|_|"running".to_string());let mut bins=[[0u64;8];6];let mut matched=[0u64;6];let mut max_delta=[0u64;6];let mut ex_mismatch=[0u64;6];
 let mut first=HashMap::<u64,([Option<(u64,u64)>;4],u64)>::new();let mut fifo=VecDeque::<(u64,u64)>::new();let mut evicted=0u64;let mut drops=0u64;
 let start=Instant::now();let deadline=tokio::time::Instant::now()+Duration::from_secs(seconds);
 loop{tokio::select!{
  _=tokio::time::sleep_until(deadline)=>break,
  e=rx.recv()=>match e{
   Some(Ev::End{socket,why})=>ends[socket]=why,
   Some(Ev::Tick(t))=>{
    counts[t.socket]+=1;
    let sec=(start.elapsed().as_secs() as usize).min(899);per_sec_counts[sec][t.socket]+=1;
    max_parse_us[t.socket]=max_parse_us[t.socket].max(t.parse_us);
    max_consumer_queue_us[t.socket]=max_consumer_queue_us[t.socket].max(now_ms().saturating_sub(t.recv)*1000);
    if t.ex>t.recv{future[t.socket]+=1}else{let age=t.recv-t.ex;
     if first_age[t.socket].is_none(){first_age[t.socket]=Some(age)}last_age[t.socket]=Some(age);
     max_age[t.socket]=max_age[t.socket].max(age);per_sec_age_max[sec][t.socket]=per_sec_age_max[sec][t.socket].max(age);
     if age>800{stale[t.socket]+=1;per_sec_stale[sec][t.socket]+=1}}

    while let Some(&(id,ts))=fifo.front(){if t.recv.saturating_sub(ts)<=15000&&first.len()<3000&&fifo.len()<6000{break}fifo.pop_front();if first.get(&id).is_some_and(|(_,stamp)|*stamp==ts){first.remove(&id);evicted+=1}}
    let entry=first.entry(t.id).or_insert_with(||{fifo.push_back((t.id,t.recv));([None;4],t.recv)});
    for other in 0..4 {if other==t.socket{continue}if let Some((recv,ex))=entry.0[other]{
     let pair=match (other.min(t.socket),other.max(t.socket)){(0,1)=>0,(2,3)=>1,(0,2)=>2,(0,3)=>3,(1,2)=>4,(1,3)=>5,_=>unreachable!()};
     // Each pair counted once per ID: first arrival of each socket only.
     if entry.0[t.socket].is_none(){let delta=t.recv.abs_diff(recv);matched[pair]+=1;bins[pair][bucket(delta)]+=1;max_delta[pair]=max_delta[pair].max(delta);if ex!=t.ex{ex_mismatch[pair]+=1}}
    }}
    if entry.0[t.socket].is_none(){entry.0[t.socket]=Some((t.recv,t.ex))}
   },
   None=>break
  }
 }}
 for h in handles{h.abort()}
 println!("{}",json!({"kind":"summary","symbol":symbol,"seconds":start.elapsed().as_secs(),"endpoints":["public/stream","public/stream","stream","stream"],"counts":counts,"stale_age_over800":stale,"max_age_ms":max_age,"future_ts":future,"socket_end":ends,"matched_pairs_order":["public_0_1","stream_2_3","cross_0_2","cross_0_3","cross_1_2","cross_1_3"],"matched":matched,"delta_buckets":bins,"delta_max_ms":max_delta,"exchange_timestamp_mismatch":ex_mismatch,"unmatched_evicted":evicted,"pending":first.len(),"queue_drops":drops,"per_sec_counts":&per_sec_counts[..(seconds as usize)],"per_sec_stale":&per_sec_stale[..(seconds as usize)],"per_sec_age_max":&per_sec_age_max[..(seconds as usize)],"first_age_ms":first_age,"last_age_ms":last_age,"max_parse_us":max_parse_us,"max_consumer_queue_us":max_consumer_queue_us,"orders":false}));
}
