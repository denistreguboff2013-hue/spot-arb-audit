//! Isolated WS transport/paired-state research. Intentionally no trading API.
//! This runner uses localhost fixture only. History is RAM counted, so this
//! cannot certify durable DB1 or production readiness.
use futures_util::{SinkExt,StreamExt};
use spotarb_native_market_core::{owner_index,CoreError,HistorySink,Owner,RawFrame,Update,Venue};
use spotarb_native_market_core::mpsc_ring::BoundedMpscRing;
#[derive(Clone)] enum Sender { Channel(mpsc::SyncSender<RawFrame>), Ring(Arc<BoundedMpscRing<RawFrame>>) }
impl Sender {
 fn try_send(&self,frame:RawFrame)->Result<(),CoreError>{match self{
  Self::Channel(q)=>q.try_send(frame).map_err(|_|CoreError::HistoryPressure),
  Self::Ring(q)=>q.try_push(frame)
 }}
}
enum Receiver { Channel(mpsc::Receiver<RawFrame>), Ring(Arc<BoundedMpscRing<RawFrame>>) }
impl Receiver {fn recv(&self)->Option<RawFrame>{match self{
 Self::Channel(q)=>q.recv().ok(),Self::Ring(q)=>q.pop()
}}}

use std::{sync::{Arc,atomic::{AtomicBool,AtomicU64,AtomicUsize,Ordering},mpsc},time::{Duration,Instant,SystemTime,UNIX_EPOCH}};
use tokio_tungstenite::{connect_async_tls_with_config,Connector,tungstenite::Message};
#[derive(Default)] struct Counters {
 received:AtomicU64, accepted:AtomicU64, compared:AtomicU64, errors:AtomicU64,
 pressure:AtomicU64, pong:AtomicU64, ping_timeout:AtomicU64,
}
struct CountingHistory<'a>{c:&'a Counters}
impl HistorySink for CountingHistory<'_> {
 fn accept(&mut self,f:&RawFrame,_:&Update)->Result<(),CoreError>{
  self.c.accepted.fetch_add(1,Ordering::Relaxed);
  let _=f; Ok(())
 }
}
fn clock_ms()->i64 {SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64}
fn cpu()->f64{
 let mut u:libc::rusage=unsafe{std::mem::zeroed()};
 unsafe{libc::getrusage(libc::RUSAGE_SELF,&mut u)};
 (u.ru_utime.tv_sec+u.ru_stime.tv_sec) as f64+(u.ru_utime.tv_usec+u.ru_stime.tv_usec) as f64/1e6
}
fn thread_cpu()->f64{
 let mut u:libc::rusage=unsafe{std::mem::zeroed()};
 unsafe{libc::getrusage(libc::RUSAGE_THREAD,&mut u)};
 (u.ru_utime.tv_sec+u.ru_stime.tv_sec) as f64+(u.ru_utime.tv_usec+u.ru_stime.tv_usec) as f64/1e6
}
fn route(raw:&str,venue:Venue)->Option<String>{
 let v:serde_json::Value=serde_json::from_str(raw).ok()?;
 let symbol=match venue{
  Venue::BinanceFutures=>v.get("data")?.get("s")?.as_str()?,
  Venue::OkxSpot=>v.get("arg")?.get("instId")?.as_str()?
 };
 let suffix=if venue==Venue::BinanceFutures {"USDT"} else {"-USDT"};
 let base=symbol.strip_suffix(suffix)?;
 if base.is_empty()||!base.bytes().all(|b|b.is_ascii_uppercase()||b.is_ascii_digit()){return None}
 Some(base.to_string())
}
fn percentile(v:&mut Vec<i64>,p:f64)->i64{
 if v.is_empty(){return -1}
 v.sort_unstable();v[((v.len()-1) as f64*p) as usize]
}
async fn receiver(url:String,venue:Venue,bases:Vec<String>,channels:Vec<Sender>,
 depth:Vec<Arc<AtomicUsize>>,peaks:Vec<Arc<AtomicUsize>>,c:Arc<Counters>,fatal:Arc<AtomicBool>,
 start:Instant)->serde_json::Value {
 let builder=native_tls::TlsConnector::builder().danger_accept_invalid_certs(true).build().unwrap();
 let connector=Connector::NativeTls(builder);
 let (mut ws,_)=match connect_async_tls_with_config(url.as_str(),None,false,Some(connector)).await{
  Ok(x)=>x,Err(e)=>return serde_json::json!({"error":format!("connect: {e}")})
 };
 let mut age=Vec::with_capacity(100_000);
 let mut pong_latency=Vec::new();
 let mut n=0_u64;let mut last_ping:Option<Instant>=None;
 let mut timer=tokio::time::interval(Duration::from_secs(5));
 timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
 let allowed:std::collections::HashSet<_>=bases.iter().cloned().collect();
 loop {
  if fatal.load(Ordering::Acquire){break}
  tokio::select! {
   _=timer.tick()=>{
    if let Some(p)=last_ping{if p.elapsed()>Duration::from_secs(20){
     c.ping_timeout.fetch_add(1,Ordering::Relaxed);fatal.store(true,Ordering::Release);break
    }}
    if last_ping.is_none(){
     if ws.send(Message::Ping(b"native-fixture".to_vec())).await.is_err(){break}
     last_ping=Some(Instant::now());
    }
   },
   incoming=ws.next()=>{
    match incoming{
     Some(Ok(Message::Text(raw)))=>{
      let received_ns=start.elapsed().as_nanos() as u64+1;let wall=clock_ms();
      let base=match route(&raw,venue){Some(s) if allowed.contains(&s)=>s,_=>{
       c.errors.fetch_add(1,Ordering::Relaxed);fatal.store(true,Ordering::Release);break
      }};
      // A receive envelope is accepted exactly once; a full owner queue denies.
      let index=owner_index(&base,channels.len());
      let frame=RawFrame{source:None,venue,epoch:1,received_ns,received_wall_ms:wall,raw};
      let d=depth[index].fetch_add(1,Ordering::AcqRel)+1;
      peaks[index].fetch_max(d,Ordering::Relaxed);
      if channels[index].try_send(frame).is_err(){
       depth[index].fetch_sub(1,Ordering::AcqRel);
       c.pressure.fetch_add(1,Ordering::Relaxed);fatal.store(true,Ordering::Release);break
      }
      c.received.fetch_add(1,Ordering::Relaxed);n+=1;
      if n%32==0{
       // socket arrival age uses exchange E/ts in the owner; this is only
       // an inexpensive receive timestamp distribution.
       age.push(wall);
      }
     },
     Some(Ok(Message::Ping(payload)))=>{if ws.send(Message::Pong(payload)).await.is_err(){break}},
     Some(Ok(Message::Pong(_)))=>{
      c.pong.fetch_add(1,Ordering::Relaxed);
      if let Some(p)=last_ping.take(){pong_latency.push(p.elapsed().as_millis() as i64)}
     },
     Some(Ok(Message::Close(_)))|None=>break,
     Some(Err(_))=>{c.errors.fetch_add(1,Ordering::Relaxed);fatal.store(true,Ordering::Release);break},
     _=>{}
    }
   }
  }
 }
 let _=ws.close(None).await;
 serde_json::json!({"received":n,"pong_p50_ms":percentile(&mut pong_latency.clone(),0.5),
 "pong_p99_ms":percentile(&mut pong_latency,0.99),"error":fatal.load(Ordering::Acquire)})
}
#[tokio::main(flavor="multi_thread",worker_threads=2)]
async fn main(){
 let args:Vec<String>=std::env::args().collect();
 assert!(args.len()==6,"usage: ws_fixture <name> <duration> <b-rates-6> <o-rates-6> <bases.json>");
 let name=&args[1];assert!(name.bytes().all(|b|b.is_ascii_alphanumeric()||b==b'_'));
 let duration:u64=args[2].parse().unwrap();
 let parse_rates=|s:&str|->Vec<u64>{let x:Vec<_>=s.split(',').map(|v|v.parse().unwrap()).collect();assert_eq!(x.len(),6);x};
 let br=parse_rates(&args[3]);let or=parse_rates(&args[4]);
 let groups:Vec<Vec<String>>=serde_json::from_slice(&std::fs::read(&args[5]).unwrap()).unwrap();
 assert_eq!(groups.len(),6);
 let beginning=Instant::now();let initial_cpu=cpu();
 let c=Arc::new(Counters::default());let fatal=Arc::new(AtomicBool::new(false));
 let mut tx=Vec::new();let mut threads=Vec::new();
 let transport=std::env::var("WS_TRANSPORT").unwrap_or_else(|_|"channel".to_owned());
 assert!(transport=="channel"||transport=="ring");
 let mut depth=Vec::new();let mut peaks=Vec::new();
 for i in 0..3{
  let rx=if transport=="ring"{
   let q=Arc::new(BoundedMpscRing::new(4096).unwrap());
   tx.push(Sender::Ring(q.clone()));Receiver::Ring(q)
  }else{
   let (sender,receiver)=mpsc::sync_channel::<RawFrame>(4096);
   tx.push(Sender::Channel(sender));Receiver::Channel(receiver)
  };
  let q=Arc::new(AtomicUsize::new(0));let high=Arc::new(AtomicUsize::new(0));
  depth.push(q.clone());peaks.push(high);
  let count=c.clone();let denied=fatal.clone();
  threads.push(std::thread::spawn(move||{
   let mut owner=Owner::new(i,3,1000).unwrap();
   let mut history=CountingHistory{c:&count};
   let mut source_age:Vec<i64>=Vec::with_capacity(100_000);
   let mut arrival_age:Vec<i64>=Vec::with_capacity(100_000);
   let mut applied=0_u64;let began=thread_cpu();
   while let Some(frame)=rx.recv(){
    q.fetch_sub(1,Ordering::AcqRel);
    if denied.load(Ordering::Acquire){break}
    let timing=spotarb_native_market_core::parse(&frame);
    if let Ok(update)=&timing {
     if let Some(t)=update.book.exchange_ts_ms {
      if applied%32==0 {source_age.push(clock_ms()-t);arrival_age.push(frame.received_wall_ms-t)}
     }
    }
    match owner.ingest(&frame,&mut history){
     Ok(Some(_))=>{applied+=1;count.compared.fetch_add(1,Ordering::Relaxed);},
     Ok(None)=>{applied+=1},
     Err(_)=>{count.errors.fetch_add(1,Ordering::Relaxed);denied.store(true,Ordering::Release);break}
    }
   }
   serde_json::json!({"owner":i,"applied":applied,"cpu_s":thread_cpu()-began,
    "age_p50_ms":percentile(&mut source_age.clone(),0.5),
    "age_p99_ms":percentile(&mut source_age,0.99),
    "arrival_age_p99_ms":percentile(&mut arrival_age,0.99)})
  }));
 }
 let mut tasks=Vec::new();
 for i in 0..6 {
  for (venue,letter,rate) in [(Venue::BinanceFutures,"b",br[i]),(Venue::OkxSpot,"o",or[i])] {
   let url=format!("wss://127.0.0.1:18779/{name}/{letter}/{i}/{rate}/{duration}");
   tasks.push(tokio::spawn(receiver(url,venue,groups[i].clone(),tx.clone(),depth.clone(),
                 peaks.clone(),c.clone(),fatal.clone(),beginning)));
  }
 }
 let mut sockets=Vec::new();
 for task in tasks{sockets.push(task.await.unwrap());}
 for sender in &tx{if let Sender::Ring(q)=sender{q.close()}}
 drop(tx);
 let mut owners=Vec::new();for thread in threads{owners.push(thread.join().unwrap());}
 let result=serde_json::json!({"name":name,"duration_target":duration,
  "wall_s":beginning.elapsed().as_secs_f64(),"cpu_s":cpu()-initial_cpu,
  "received":c.received.load(Ordering::Relaxed),"history_ram_accepted":c.accepted.load(Ordering::Relaxed),
  "compared":c.compared.load(Ordering::Relaxed),"errors":c.errors.load(Ordering::Relaxed),
  "pressure":c.pressure.load(Ordering::Relaxed),"ping_timeout":c.ping_timeout.load(Ordering::Relaxed),
  "pong":c.pong.load(Ordering::Relaxed),"fatal":fatal.load(Ordering::Acquire),
  "owner_peaks":peaks.iter().map(|x|x.load(Ordering::Relaxed)).collect::<Vec<_>>(),
  "owner_ends":depth.iter().map(|x|x.load(Ordering::Relaxed)).collect::<Vec<_>>(),
  "owners":owners,"sockets":sockets,"durable_history":false,"transport":transport});
 println!("{}",result);
}
