//! R1195 bounded PUBLIC-only source demonstration. No PAPER/REAL, databases or orders.
#[path = "../quality_manager_r1195.rs"]
mod quality;
use quality::{Manager,Tier,History};
use std::{path::{Path,PathBuf},time::{Duration,Instant,SystemTime,UNIX_EPOCH},fs};
use futures_util::{StreamExt,SinkExt};
use tokio_tungstenite::{connect_async,tungstenite::Message,MaybeTlsStream};
use spotarb_native_market_core::native_binance_ws_ip_guard_r1110::{r1110_reserve_at_lock,R1110Outcome};
const GUARD:&str="/mnt/spotarb_db_test/astra_paper_continuous_v1/r1110_binance_ws_ip_guard.lock";
const KILL:&str="/home/tmpadmin/spotbot/data/real_kill_switch.json";
const SYMBOLS:[&str;8]=["SEIUSDT","SENTUSDT","SKLUSDT","SKYUSDT","SLPUSDT","SLXUSDT","SNXUSDT","SOLUSDT"];
fn utc()->Result<i64,String> {
 let d=SystemTime::now().duration_since(UNIX_EPOCH).map_err(|e|e.to_string())?;
 i64::try_from(d.as_millis()).map_err(|_|"timestamp overflow".into())
}
fn safety()->Result<(),String> {
 if std::env::var("ASTRA_R1195_SOURCE_ONLY").ok().as_deref()!=Some("YES") {
     return Err("R1195 explicit source-only consent required".into())
 }
 let value:serde_json::Value=serde_json::from_slice(&fs::read(KILL).map_err(|e|e.to_string())?)
     .map_err(|e|e.to_string())?;
 if value["state"]!="ENGAGED" {return Err("REAL kill is not ENGAGED".into())}
 Ok(())
}
async fn permit(deadline:Instant)->Result<(),String>{
 loop{
  let outcome=tokio::task::spawn_blocking(||r1110_reserve_at_lock(Path::new(GUARD)))
   .await.map_err(|e|e.to_string())?.map_err(|e|format!("guard:{e:?}"))?;
  match outcome{
   R1110Outcome::Granted=>return Ok(()),
   R1110Outcome::WaitMs(t)=>{
     if Instant::now()+Duration::from_millis(t)>=deadline {return Err("quota deadline".into());}
     tokio::time::sleep(Duration::from_millis(t)).await;
   }
  }
 }
}
enum Incoming { Connected(u64,String), Data(u64,String,i64,u64), Done(u64,String) }
async fn reader(id:u64,start:Instant,end:Instant,tx:tokio::sync::mpsc::Sender<Incoming>)->Result<(),String>{
 permit(end).await?;
 let query=SYMBOLS.iter().map(|s|format!("{}@depth20@500ms",s.to_ascii_lowercase())).collect::<Vec<_>>().join("/");
 let url=format!("wss://fstream.binance.com/public/stream?streams={query}");
 let (mut ws,_)=tokio::time::timeout(Duration::from_secs(12),connect_async(url.as_str()))
    .await.map_err(|_|"connect timeout".to_string())?.map_err(|e|e.to_string())?;
 let peer=match ws.get_ref(){
  MaybeTlsStream::NativeTls(tls)=>tls.get_ref().get_ref().get_ref().peer_addr()
    .map(|a|a.to_string()).unwrap_or_else(|_|"unknown".into()),
  MaybeTlsStream::Plain(tcp)=>tcp.peer_addr().map(|a|a.to_string()).unwrap_or_else(|_|"unknown".into()),
  _=>"unavailable".into()
 };
 tx.send(Incoming::Connected(id,peer)).await.map_err(|e|e.to_string())?;
 let reason=loop{
  if Instant::now()>=end {break "normal_bound_end".to_string();}
  let timeout=end.saturating_duration_since(Instant::now()).min(Duration::from_secs(2));
  match tokio::time::timeout(timeout,ws.next()).await{
   Ok(Some(Ok(Message::Text(raw))))=>{
    let t=utc()?;let m=start.elapsed().as_millis() as u64;
    if tx.try_send(Incoming::Data(id,raw,t,m)).is_err(){
      break "bounded_telemetry_queue_pressure".to_string();
    }
   },
   Ok(Some(Ok(Message::Ping(_))))=>{let _=ws.flush().await;},
   Ok(Some(Ok(Message::Close(_))))=>break "remote_close".to_string(),
   Ok(Some(Err(e)))=>break format!("ws:{e}"),
   Ok(None)=>break "stream_closed".to_string(),
   Err(_)=>continue,
   _=>{}
  }
 };
 let _=tokio::time::timeout(Duration::from_secs(2),ws.close(None)).await;
 let _=tx.send(Incoming::Done(id,reason)).await;
 Ok(())
}
fn valid_depth(v:&serde_json::Value)->bool {
 let d=&v["data"];
 let levels=|key:&str| d[key].as_array().map(|a| {
   !a.is_empty() && a.iter().all(|p|p.as_array().is_some_and(|v| {
     v.len()==2 && v.iter().all(|x|x.as_str()
       .and_then(|s|s.parse::<f64>().ok()).is_some_and(|n|n.is_finite()&&n>0.0))
   }))
 }).unwrap_or(false);
 levels("b") && levels("a")
}
#[tokio::main(flavor="multi_thread",worker_threads=2)]
async fn main()->Result<(),String>{
 safety()?;
 let dir=PathBuf::from(std::env::args().nth(1).ok_or("scratch path required")?)
    .canonicalize().map_err(|e|e.to_string())?;
 if !dir.to_string_lossy().starts_with(
  "/mnt/spotarb_db_test/astra_p001_r874_test_scratch_20261025/r1195_quality_live_"){
  return Err("output must be approved isolated scratch".into())
 }
 let mut manager=Manager::default();
 manager.define_group("g",SYMBOLS.iter().map(|s|s.to_string()).collect())?;
 let start=Instant::now();
 let end=start+Duration::from_secs(125);
 let (tx,mut rx)=tokio::sync::mpsc::channel::<Incoming>(4096);
 for id in [1_u64,2] {
  let task_tx=tx.clone();
  tokio::spawn(async move{
   if let Err(e)=reader(id,start,end,task_tx.clone()).await {
    let _=task_tx.send(Incoming::Done(id,format!("reader_error:{e}"))).await;
   }
  });
 }
 drop(tx);
 let mut forced=false;let mut initial_mono=None;let mut failover_mono=None;
 let mut live_frames=[0u64;2];let mut aged=[0u64;2];let mut accepted=0u64;
 let mut blocked=0u64;let mut errors=0u64;let mut reason=Vec::new();
 let mut chosen_first=None;let mut epochs=Vec::new();
 loop {
  if Instant::now()>=end {break}
  let m=start.elapsed().as_millis() as u64;
  let now=utc()?;
  if m>=45_000 && !forced {
   forced=true;manager.invalidate(1)?;
   reason.push("synthetic_primary_quarantine_at_45s".to_string());
  }
  if !manager.active.contains_key("g") && !forced && m>5_000 {
   if let Ok(epoch)=manager.promote("g",1,now,m){
    chosen_first=Some(1u64);initial_mono=Some(m);epochs.push(epoch);
   }
  }
  if forced && failover_mono.is_none() {
   if let Ok(epoch)=manager.promote("g",2,now,m){
    failover_mono=Some(m);epochs.push(epoch);
   }
  }
  match tokio::time::timeout(Duration::from_millis(50),rx.recv()).await {
   Ok(Some(Incoming::Connected(id,peer)))=>{
    if manager.add_session(id,&peer,"g").is_err(){errors+=1}
   },
   Ok(Some(Incoming::Data(id,raw,recv_utc,recv_mono)))=>{
    let idx=(id-1) as usize; live_frames[idx]+=1;
    if let Ok(v)=serde_json::from_str::<serde_json::Value>(&raw){
     let sym=v["data"]["s"].as_str().unwrap_or("").to_ascii_uppercase();
     let exchange_ms=v["data"]["E"].as_i64().unwrap_or(0);
     let sequence=v["data"]["u"].as_u64()
       .or_else(||v["data"]["lastUpdateId"].as_u64());
     let good=valid_depth(&v) && exchange_ms!=0;
     if recv_utc.saturating_sub(exchange_ms)>800 {aged[idx]+=1;}
     match manager.observe(id,&sym,exchange_ms,recv_utc,recv_mono,sequence,good) {
      Ok(_)=>{},
      Err(_)=>errors+=1
     }
     if !forced && manager.active.get("g").is_none() && id==1 {
      if let Ok(epoch)=manager.promote("g",1,recv_utc,recv_mono){
       chosen_first=Some(1u64);initial_mono=Some(recv_mono);epochs.push(epoch);
      }
     }
     if forced && failover_mono.is_none() {
      if let Ok(epoch)=manager.promote("g",2,recv_utc,recv_mono){
       failover_mono=Some(recv_mono);epochs.push(epoch);
      }
     }
     if manager.active.get("g").is_some() {
      if manager.publish("g",&sym,recv_utc,recv_mono).is_some(){accepted+=1;}
      else{blocked+=1;}
     }
    }else{errors+=1}
   },
   Ok(Some(Incoming::Done(id,why)))=>{reason.push(format!("lane{id}:{why}"));},
   Ok(None)=>break,
   Err(_)=>{}
  }
 }
 for id in [1,2] {
  let _=manager.record_quality(id,utc()?);
 }
 let h=dir.join("quality_history.json");
 manager.history.save_atomic(&h)?;
 let r=serde_json::json!({
   "schema":"SPOTARB/R1195/2MIN_SOURCE_ONLY_FAST_FAILOVER/V1",
   "real_orders":0,"paper_orders":0,"db_writes":0,
   "num_lanes":2,"symbols":SYMBOLS,
   "frames_received":live_frames,"aged_gt800":aged,
   "published_after_source_selection":accepted,"blocked_candidates":blocked,
   "errors":errors,"first_primary":chosen_first,
   "initial_promotion_mono_ms":initial_mono,
   "synthetic_quarantine_at_mono_ms":45000,
   "backup_promotion_mono_ms":failover_mono,
   "epoch_history":epochs,
   "session_1_tier":format!("{:?}",manager.sessions.get(&1).map(|s|s.tier)),
   "session_2_tier":format!("{:?}",manager.sessions.get(&2).map(|s|s.tier)),
   "history_peers":manager.history.peers,
   "conclusion_limit":"Not PAPER integration and not true network outage; manual quarantine scenario, no durable ACK"
 });
 let evidence=dir.join("r1195_live_report.json");
 let bytes=serde_json::to_vec_pretty(&r).map_err(|e|e.to_string())?;
 fs::write(&evidence,&bytes).map_err(|e|e.to_string())?;
 println!("R1195_RESULT {}",r);
 println!("R1195_REPORT {} reasons={:?}",evidence.display(),reason);
 Ok(())
}
