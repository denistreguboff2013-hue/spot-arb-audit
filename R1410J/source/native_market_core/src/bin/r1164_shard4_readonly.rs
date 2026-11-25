//! R1164_SHARD4: controlled 40-source multiplex vs 1-source PUBLIC WS, same target symbol.
 //! No REST, no private keys, no execution API, no venue reconnect loops.
use futures_util::{StreamExt,SinkExt};
use spotarb_native_market_core::native_binance_ws_ip_guard_r1110::{
    r1110_reserve_at_lock,R1110Outcome};
use std::{path::{Path,PathBuf},time::{Duration,Instant,SystemTime,UNIX_EPOCH},
    collections::{BTreeMap,BTreeSet},fs};
use tokio_tungstenite::{connect_async,tungstenite::Message,MaybeTlsStream};
const GUARD:&str="/mnt/spotarb_db_test/astra_paper_continuous_v1/r1110_binance_ws_ip_guard.lock";
const KILL:&str="/home/tmpadmin/spotbot/data/real_kill_switch.json";
const SINGLE_URL:&str="wss://fstream.binance.com/public/stream?streams=pumpusdt@depth20@500ms";
const SHARD0_BASES:&[&str]=&["PROVE","PUMP","PYTH","QTUM","RE","RENDER","RESOLV","ROBO","RPL","RSR","RVN","S","SAFE","SAHARA","SAND","SCR","SEI","SENT","SKL","SKY","SLP","SLX","SNX","SOL","SONIC","SOPH","SPACE","SPK","SSV","STRK","STX","SUI","SUSHI","T","TAO","THETA","TIA","TNSR","TOSHI","TRB"];
#[derive(Clone,serde::Serialize)]
struct Frame{stream:String,event_ms:i64,wall_ms:i64,relative_ms:u64}
#[derive(serde::Serialize)]
struct ResultLane{
 lane:String,peer:String,started_ms:i64,ended_ms:i64,reason:String,
 frames:Vec<Frame>,errors:u64,pings:u64}
fn wall()->Result<i64,String>{
 let x=SystemTime::now().duration_since(UNIX_EPOCH).map_err(|e|format!("clock:{e}"))?;
 i64::try_from(x.as_millis()).map_err(|_|"timestamp overflow".to_string())
}
fn require_safe()->Result<(),String>{
 if std::env::var("ASTRA_R1164_SHARD4_GROUP40_VS_SINGLE").ok().as_deref()!=Some("YES"){
  return Err("explicit isolated public-only opt-in required".into())
 }
 let k=fs::read(KILL).map_err(|e|format!("kill read:{e}"))?;
 let x:serde_json::Value=serde_json::from_slice(&k).map_err(|e|format!("kill json:{e}"))?;
 if x.get("state").and_then(|v|v.as_str())!=Some("ENGAGED"){
  return Err("REAL kill switch not ENGAGED".into())
 }
 Ok(())
}
async fn permit(end:Instant)->Result<(),String>{
 loop{
  if Instant::now()>=end{return Err("R1110 deadline".into())}
  let v=tokio::task::spawn_blocking(||r1110_reserve_at_lock(Path::new(GUARD)))
    .await.map_err(|e|format!("quota task:{e}"))?
    .map_err(|e|format!("R1110 reject:{e:?}"))?;
  match v{
   R1110Outcome::Granted=>return Ok(()),
   R1110Outcome::WaitMs(ms)=>{
    let wait=Duration::from_millis(ms);
    if wait>=end.saturating_duration_since(Instant::now()){
     return Err(format!("R1110 quota window beyond deadline: {ms}ms"))
    }
    tokio::time::sleep(wait).await;
   }
  }
 }
}
async fn lane(label:&str,end:Instant,start:Instant)->Result<ResultLane,String>{
 require_safe()?;
 permit(end).await?;
 let url=if label=="GROUP40"{
    let query=SHARD0_BASES.iter().map(|b|format!("{}usdt@depth20@500ms",b.to_ascii_lowercase())).collect::<Vec<_>>().join("/");
    format!("wss://fstream.binance.com/public/stream?streams={query}")
 }else{SINGLE_URL.to_string()};
 let (mut ws,_) =tokio::time::timeout(Duration::from_secs(12),connect_async(url.as_str()))
  .await.map_err(|_|format!("{label} WS connect timeout"))?
  .map_err(|e|format!("{label} WS connect failed:{e}"))?;
 let peer=match ws.get_ref(){
   MaybeTlsStream::NativeTls(tls)=>tls.get_ref().get_ref().get_ref().peer_addr()
     .map(|a|a.to_string()).unwrap_or_else(|_|"unknown".into()),
   MaybeTlsStream::Plain(tcp)=>tcp.peer_addr().map(|a|a.to_string())
     .unwrap_or_else(|_|"unknown".into()),
   _=>"unavailable".into()
 };
 let started_ms=wall()?;
 let mut frames=Vec::new();let mut pings=0;let mut errors=0;
 let reason=loop{
  if Instant::now()>=end{break "normal_bound_end".to_string()}
  let timeout=end.saturating_duration_since(Instant::now()).min(Duration::from_secs(2));
  match tokio::time::timeout(timeout,ws.next()).await{
   Ok(Some(Ok(Message::Text(raw))))=>{
    let received=wall()?;
    if let Ok(v)=serde_json::from_str::<serde_json::Value>(&raw){
     if let Some(ts)=v.get("data").and_then(|v|v.get("E")).and_then(|v|v.as_i64()){
      let stream=v.get("stream").and_then(|x|x.as_str()).unwrap_or("unknown")
        .split('@').next().unwrap_or("unknown").to_string();
      frames.push(Frame{stream,event_ms:ts,wall_ms:received,
         relative_ms:start.elapsed().as_millis() as u64});
     }
    }
   },
   Ok(Some(Ok(Message::Ping(_))))=>{pings+=1;if let Err(e)=ws.flush().await{
     errors+=1;break format!("ping_flush:{e}")}},
   Ok(Some(Ok(Message::Close(_))))=>break "remote_ws_close".to_string(),
   Ok(Some(Err(e)))=>{errors+=1;break format!("websocket:{e}")},
   Ok(None)=>break "stream_ended".to_string(),
   Err(_)=>continue,
   _=>{}
  }
 };
 // Closure is best-effort but never reconnect; fail-closed recorded above.
 let _=tokio::time::timeout(Duration::from_secs(2),ws.close(None)).await;
 Ok(ResultLane{lane:label.to_string(),peer,started_ms,ended_ms:wall()?,
    reason,frames,errors,pings})
}
fn summary(v:&ResultLane)->serde_json::Value{
 let mut age:Vec<i64>=v.frames.iter().map(|f|f.wall_ms-f.event_ms).collect();
 age.sort_unstable();let n=age.len();
 let pct=|x:usize|->Option<i64>{if n==0{None}else{Some(age[(n-1)*x/100])}};
 let stale=age.iter().filter(|x|**x>800).count();
 serde_json::json!({"lane":v.lane,"peer":v.peer,"frames":n,
    "reason":v.reason,"errors":v.errors,"pings":v.pings,
    "source_age_p50_ms":pct(50),"source_age_p95_ms":pct(95),
    "source_age_max_ms":age.last(),"aged_gt800":stale,
    "event_time_span_ms":v.frames.last().zip(v.frames.first())
       .map(|(a,b)|a.event_ms-b.event_ms),
    "wall_receive_span_ms":v.frames.last().zip(v.frames.first())
       .map(|(a,b)|a.wall_ms-b.wall_ms)})
}
fn windows(v:&ResultLane,start:Instant)->serde_json::Value{
 let mut slots:BTreeMap<i64,Vec<i64>>=BTreeMap::new();
 for f in &v.frames{
  let k=f.wall_ms.div_euclid(10_000);
  slots.entry(k).or_default().push(f.wall_ms-f.event_ms);
 }
 let a:Vec<_>=slots.into_iter().map(|(bin,mut vals)|{
  vals.sort_unstable();let n=vals.len();
  serde_json::json!({"window_unix_10s":bin,"frames":n,
   "age_p50_ms":vals[n/2],"age_p95_ms":vals[(n-1)*95/100],
   "aged_gt800":vals.iter().filter(|x|**x>800).count()})
 }).collect();
 let _=start;
 serde_json::json!(a)
}
fn paired(a:&ResultLane,b:&ResultLane)->serde_json::Value{
 let m:BTreeMap<i64,i64>=a.frames.iter().filter(|x|x.stream=="pumpusdt").map(|x|(x.event_ms,x.wall_ms)).collect();
 let mut diffs=Vec::new();let mut matched=BTreeSet::new();
 for f in b.frames.iter().filter(|x|x.stream=="pumpusdt"){
  if let Some(w)=m.get(&f.event_ms){
   diffs.push(f.wall_ms-*w);matched.insert(f.event_ms);
  }
 }
 diffs.sort_unstable();let n=diffs.len();
 serde_json::json!({"matched_exact_binance_event_timestamps":matched.len(),
  "pair_observations":n,"receive_wall_B_minus_A_p50_ms":if n>0{Some(diffs[n/2])}else{None},
  "receive_wall_B_minus_A_p95_ms":if n>0{Some(diffs[(n-1)*95/100])}else{None},
  "receive_wall_B_minus_A_min_ms":diffs.first(),"receive_wall_B_minus_A_max_ms":diffs.last()})
}
#[tokio::main(flavor="multi_thread",worker_threads=2)]
async fn main()->Result<(),String>{
 require_safe()?;
 let path=std::env::args().nth(1).ok_or("R1164_SHARD4 private scratch dir argument required")?;
 let dir=PathBuf::from(path).canonicalize().map_err(|e|e.to_string())?;
 if !dir.to_string_lossy().starts_with(
    "/mnt/spotarb_db_test/astra_p001_r874_test_scratch_20261025/r1164_shard4_group40_vs_single_"){
    return Err("R1164_SHARD4 not an isolated approved scratch path".into())
 }
 let before=Instant::now();let end=before+Duration::from_secs(120);
 let (ar,br)=tokio::join!(lane("GROUP40",end,before),lane("SINGLE",end,before));
 let A=ar?;let B=br?;
 let output=serde_json::json!({
   "schema":"SPOTARB/R1164_SHARD4/GROUP40_VS_SINGLE_PUBLIC_WS/V1",
   "symbol":"PUMPUSDT","subscription":"pumpusdt@depth20@500ms",
   "sources":"two Binance PUBLIC WS: original 40-stream shard0 vs single stream",
   "group40_bases":SHARD0_BASES,
   "read_only":true,"rest_requests":0,"private_orders":0,"real_orders":0,
   "reconnects_attempted":0,
   "r1110_connect_guard":"original shared file, 3sec minimum gap, 10/min",
   "duration_bound_seconds":120,"lanes":[summary(&A),summary(&B)],
   "window_summary":{"A":windows(&A,before),"B":windows(&B,before)},
   "same_event_comparison":paired(&A,&B),
   "original_frames":{"A":&A.frames,"B":&B.frames},
   "finished_wall_ms":wall()?
 });
 let file=dir.join("r1164_shard4_group40_vs_single_report.json");
 use std::io::Write;
 let mut f=std::fs::OpenOptions::new().write(true).create_new(true)
   .open(&file).map_err(|e|format!("create new evidence:{e}"))?;
 let encoded=serde_json::to_vec_pretty(&output).map_err(|e|format!("serialize:{e}"))?;
 f.write_all(&encoded).map_err(|e|format!("write:{e}"))?;
 f.sync_all().map_err(|e|format!("fsync:{e}"))?;
 println!("R1164_SHARD4_REPORT {}",file.display());
 println!("R1164_SHARD4_LANES {}",serde_json::json!([summary(&A),summary(&B)]));
 println!("R1164_SHARD4_SAME_E {}",paired(&A,&B));
 Ok(())
}
