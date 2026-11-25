//! Bounded public OKX channel comparison. No auth, REST, trading, or filesystem writes.
use futures_util::{SinkExt,StreamExt};
use serde_json::{json,Value};
use std::{collections::HashMap,time::{Duration,Instant,SystemTime,UNIX_EPOCH}};
use tokio_tungstenite::{connect_async,tungstenite::Message};
const URL:&str="wss://ws.okx.com:443/ws/v5/public";
fn wall_ms()->i64{SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as i64}
#[derive(Default)]struct Row{messages:u64,snapshots:u64,deltas:u64,noops:u64,invalid:u64,seq_gaps:u64,seq_resets:u64,stale:u64,ages:Vec<i64>,marks:Vec<(i64,i64,i64)>,last_ns:u128,max_gap_ms:u64,last_seq:Option<i64>}
async fn channel(name:&'static str,secs:u64,origin:Instant)->(Value,HashMap<String,Vec<(i64,i64,i64)>>){
 let mut out=json!({"channel":name,"connected":false,"subscribe_ack":0,"subscribe_error":0,"elapsed_s":0,"btc":{},"eth":{},"status":"not_started"});
 let started=Instant::now();let c=tokio::time::timeout(Duration::from_secs(8),connect_async(URL)).await;
 let (mut ws,_)=match c{Ok(Ok(x))=>x,Ok(Err(e))=>{out["status"]=json!(format!("connect_error:{e}"));return (out,HashMap::new())},Err(_)=>{out["status"]=json!("connect_timeout");return (out,HashMap::new())}};
 out["connected"]=json!(true);
 let sub=json!({"op":"subscribe","args":[{"channel":name,"instId":"BTC-USDT"},{"channel":name,"instId":"ETH-USDT"}]});
 if let Err(e)=ws.send(Message::Text(sub.to_string())).await{out["status"]=json!(format!("subscribe_send:{e}"));return (out,HashMap::new())}
 let mut rows:HashMap<String,Row>=["BTC","ETH"].into_iter().map(|s|(s.to_string(),Row::default())).collect();
 while started.elapsed()<Duration::from_secs(secs){
  match tokio::time::timeout(Duration::from_secs(2),ws.next()).await{
   Err(_)=>continue,
   Ok(Some(Ok(Message::Ping(b))))=>{if ws.send(Message::Pong(b)).await.is_err(){break}},
   Ok(Some(Ok(Message::Text(raw))))=>{
    let Ok(v)=serde_json::from_str::<Value>(&raw)else{continue};
    if v["event"]=="subscribe"{out["subscribe_ack"]=json!(out["subscribe_ack"].as_u64().unwrap_or(0)+1);continue}
    if v["event"]=="error"{out["subscribe_error"]=json!(out["subscribe_error"].as_u64().unwrap_or(0)+1);out["status"]=json!(format!("subscription_error:{}",v["msg"].as_str().unwrap_or("unknown")));continue}
    if v["arg"]["channel"]!=name{continue}
    let Some(inst)=v["arg"]["instId"].as_str()else{continue};
    let symbol=if inst=="BTC-USDT"{"BTC"}else if inst=="ETH-USDT"{"ETH"}else{continue};
    let Some(d)=v["data"].as_array().and_then(|a|a.first())else{continue};
    let Some(ts)=d["ts"].as_str().and_then(|x|x.parse::<i64>().ok())else{rows.get_mut(symbol).unwrap().invalid+=1;continue};
    let now=wall_ms();let row=rows.get_mut(symbol).unwrap();row.messages+=1;let age=now-ts;row.ages.push(age);row.marks.push((ts,d["seqId"].as_i64().unwrap_or(-1),origin.elapsed().as_nanos().min(i64::MAX as u128) as i64));if row.marks.len()>20000{row.marks.remove(0);}if row.ages.len()>20000{row.ages.remove(0);}if !(0..=800).contains(&age){row.stale+=1}
    let action=v["action"].as_str().unwrap_or("snapshot");if action=="snapshot"{row.snapshots+=1}else{row.deltas+=1}
    if d["bids"].as_array().is_some_and(|a|a.is_empty())&&d["asks"].as_array().is_some_and(|a|a.is_empty()){row.noops+=1}
    if row.last_ns!=0{row.max_gap_ms=row.max_gap_ms.max(started.elapsed().as_millis().saturating_sub(row.last_ns.min(started.elapsed().as_millis())) as u64)}
    row.last_ns=started.elapsed().as_millis();
    if let Some(seq)=d["seqId"].as_i64(){if let Some(prev)=d["prevSeqId"].as_i64(){if prev==-1{row.seq_resets+=1}else if row.last_seq.is_some_and(|p|prev!=p){row.seq_gaps+=1}}row.last_seq=Some(seq)}
   },
   Ok(Some(Ok(Message::Close(_))))|Ok(None)=>{out["status"]=json!("peer_closed");break},
   Ok(Some(Err(e)))=>{out["status"]=json!(format!("read_error:{e}"));break},
   _=>{}
  }
 }
 let _=tokio::time::timeout(Duration::from_secs(1),ws.close(None)).await;
 let marks=rows.iter().map(|(s,r)|(s.clone(),r.marks.clone())).collect::<HashMap<_,_>>();
 for s in ["BTC","ETH"]{let r=rows.remove(s).unwrap_or_default();let mut a=r.ages.clone();a.sort_unstable();let q=|p:usize|->Option<i64>{if a.is_empty(){None}else{Some(a[((a.len()-1)*p)/100])}};
 out[s.to_lowercase()]=json!({"messages":r.messages,"snapshots":r.snapshots,"deltas":r.deltas,"empty_book_noops":r.noops,"parse_invalid":r.invalid,"sequence_gaps":r.seq_gaps,"sequence_resets":r.seq_resets,"exchange_age_ms":{"p50":q(50),"p95":q(95),"p99":q(99),"max":a.last().copied()},"over_800ms":r.stale,"max_intermessage_gap_ms":r.max_gap_ms});
 }
 out["elapsed_s"]=json!(started.elapsed().as_secs());if out["status"]=="not_started"{out["status"]=json!("completed")}
 (out,marks)
}
fn matched_deltas_ms(left:&[(i64,i64,i64)],right:&[(i64,i64,i64)])->Vec<i64>{
 let mut a:HashMap<(i64,i64),Vec<i64>>=HashMap::new();let mut b:HashMap<(i64,i64),Vec<i64>>=HashMap::new();
 for (ts,seq,recv) in left{if *seq>=0{a.entry((*ts,*seq)).or_default().push(*recv)}}
 for (ts,seq,recv) in right{if *seq>=0{b.entry((*ts,*seq)).or_default().push(*recv)}}
 let mut out=Vec::new();for(k,mut av)in a{if let Some(mut bv)=b.remove(&k){av.sort_unstable();bv.sort_unstable();out.extend(av.into_iter().zip(bv).map(|(x,y)|y-x));}}out.sort_unstable();out
}
fn compare_marks(left:&[(i64,i64,i64)],right:&[(i64,i64,i64)])->Value{
 let signed=matched_deltas_ms(left,right);let mut abs=signed.iter().map(|x|x.abs()).collect::<Vec<_>>();abs.sort_unstable();let q=|p:usize|if abs.is_empty(){None}else{Some(abs[((abs.len()-1)*p)/100])};
 json!({"matched_exchange_ts_and_seq":signed.len(),"abs_receive_delta_ns":{"p50":q(50),"p95":q(95),"p99":q(99),"max":abs.last().copied()},"books5_received_first":signed.iter().filter(|x|**x>0).count(),"books_received_first":signed.iter().filter(|x|**x<0).count(),"ties":signed.iter().filter(|x|**x==0).count()})
}
#[cfg(test)]mod r1216_channel_match_tests{use super::*;#[test]fn only_exact_exchange_timestamp_and_sequence_are_matched(){let a=vec![(100,7,1000),(100,8,1010),(101,9,1020)];let b=vec![(100,7,1004),(100,99,1012),(101,9,1015)];assert_eq!(matched_deltas_ms(&a,&b),vec![-5,4]);let empty=compare_marks(&a,&[(100,999,1)]);assert_eq!(empty["matched_exchange_ts_and_seq"],0);}}
#[tokio::main(flavor="multi_thread",worker_threads=2)]
async fn main(){let secs=std::env::args().nth(1).and_then(|x|x.parse::<u64>().ok()).unwrap_or(900).clamp(10,3600);let origin=Instant::now();let ((a,am),(b,bm),(c,_cm))=tokio::join!(channel("books5",secs,origin),channel("books",secs,origin),channel("bbo-tbt",secs,origin));let matched=["BTC","ETH"].into_iter().map(|s|(s.to_string(),compare_marks(am.get(s).map(Vec::as_slice).unwrap_or(&[]),bm.get(s).map(Vec::as_slice).unwrap_or(&[])))).collect::<HashMap<_,_>>();println!("{}",json!({"schema":"R1216_OKX_CHANNEL_COMPARE_V1","requested_s":secs,"public_only":true,"trade":false,"results":[a,b,c],"matched_event_receive_comparison":matched}))}
