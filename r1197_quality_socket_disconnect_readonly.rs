//! R1195 bounded PUBLIC-only source demonstration. No PAPER/REAL, databases or orders.
#[path = "../quality_manager_r1195.rs"]
mod quality;
use quality::{Manager,History};
use std::{path::{Path,PathBuf},time::{Duration,Instant,SystemTime,UNIX_EPOCH},fs};
use futures_util::{StreamExt,SinkExt};
use tokio_tungstenite::{connect_async,tungstenite::Message,MaybeTlsStream};
use spotarb_native_market_core::native_binance_ws_ip_guard_r1110::{r1110_reserve_at_lock,R1110Outcome};
const GUARD:&str="/mnt/spotarb_db_test/astra_paper_continuous_v1/r1110_binance_ws_ip_guard.lock";
const KILL:&str="/home/tmpadmin/spotbot/data/real_kill_switch.json";
const SYMBOLS:[&str;8]=["BTCUSDT","ETHUSDT","SOLUSDT","XRPUSDT","BNBUSDT","DOGEUSDT","ADAUSDT","LINKUSDT"];
fn utc()->Result<i64,String> {
 let d=SystemTime::now().duration_since(UNIX_EPOCH).map_err(|e|e.to_string())?;
 i64::try_from(d.as_millis()).map_err(|_|"timestamp overflow".into())
}
fn safety()->Result<(),String> {
 if std::env::var("ASTRA_R1197_SOURCE_ONLY").ok().as_deref()!=Some("YES") {
     return Err("R1197 explicit source-only opt-in required".into())
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
async fn reader(id:u64,start:Instant,end:Instant,tx:tokio::sync::mpsc::Sender<Incoming>, mut cancel:tokio::sync::watch::Receiver<bool>)->Result<(),String>{
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
  let next=tokio::select! { biased; result=cancel.changed()=>{if result.is_err() || *cancel.borrow(){break "local_socket_shutdown_requested".to_string();} continue}, result=tokio::time::timeout(timeout,ws.next())=>result, };
  match next{
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
 let _=tokio::time::timeout(Duration::from_millis(400),ws.close(None)).await;
 drop(ws); // Actual transport released before signaling Done to selection manager.
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
  "/mnt/spotarb_db_test/astra_p001_r874_test_scratch_20261025/r1197_quality_disconnect_"){
  return Err("output must be approved isolated scratch".into())
 }
 const N:usize=8;
 let mut manager=Manager::default();
 manager.define_group("g",SYMBOLS.iter().map(|s|s.to_string()).collect())?;
 let history_path=dir.join("quality_history.json");
 manager.history=History::load(&history_path,utc()?)?;
 let start=Instant::now();
 let end=start+Duration::from_secs(210);
 let (tx,mut rx)=tokio::sync::mpsc::channel::<Incoming>(4096);
 let mut stops=std::collections::HashMap::<u64,tokio::sync::watch::Sender<bool>>::new();
 for id in 1..=(N as u64){
  let task_tx=tx.clone();
  let (stop_tx,stop_rx)=tokio::sync::watch::channel(false);
  stops.insert(id,stop_tx);
  tokio::spawn(async move{
   if let Err(e)=reader(id,start,end,task_tx.clone(),stop_rx).await {
    let _=task_tx.send(Incoming::Done(id,format!("reader_error:{e}"))).await;
   }
  });
 }
 drop(tx);
 let mut first=None::<(u64,u64,u64)>; // id, epoch, monotonic timestamp
 let mut attempted=false;
 let mut disconnect_observed_mono=None::<u64>;
 let mut active_source_invalidated_on_done=false;
 let mut published_event_watermarks=std::collections::HashMap::<String,i64>::new();
 let mut published_duplicate_or_regression=0u64;
 let mut forced_mono=None::<u64>;
 let mut ready_at_failure=0usize;
 let mut failover=None::<(u64,u64,u64)>; // id, epoch, mono time
 let mut pool_frames=[0u64;N];
 let mut pool_late=[0u64;N];
 let mut published=0u64;
 let mut published_after=0u64;
 let mut blocked_due_to_bad=0u64;
 let mut errors=0u64;
 let mut done=Vec::<String>::new();
 let mut history_saves=0u64;
 let mut last_history_mono=0u64;
 let mut epochs=Vec::<u64>::new();
 let mut max_ready_before=0usize;
 let mut promotions_failed=0u64;
 while Instant::now()<end {
  let m=start.elapsed().as_millis() as u64;
  let now=utc()?;
  // Small, bounded history persistence. The write is NOT a per-frame operation.
  if m>=last_history_mono+30_000 {
   last_history_mono=m;
   for id in 1..=(N as u64){
    if manager.sessions.contains_key(&id) {manager.record_quality(id,now)?;}
   }
   manager.history.save_atomic(&history_path)?;
   history_saves+=1;
  }
  let ready=manager.sessions.values().filter(|s|{
     s.tier==quality::Tier::Hot && manager.suggest_ready_reserve("g",now,m)==Some(s.id)
  }).count();
  max_ready_before=max_ready_before.max(ready);
  if first.is_none() && m>=30_000 {
   if let Some(id)=manager.suggest_ready_reserve("g",now,m){
    if let Ok(epoch)=manager.promote("g",id,now,m){
     first=Some((id,epoch,m));epochs.push(epoch);
    }
   }
  }
  // A deterministic fault time, independent of reserve condition:
  // no valid reserve => stay FAIL_CLOSED; never 'fake' success.
  if !attempted && m>=90_000 {
   attempted=true;
   if let Some((primary,_,_))=first {
    forced_mono=Some(m);
    let mut n=0;
    for candidate in 1..=(N as u64){
     if candidate==primary {continue}
     if manager.sessions.get(&candidate).is_some_and(|s|
          s.tier==quality::Tier::Hot && s.symbols.iter().all(|sym|
          s.quotes.get(sym).is_some_and(|q|
          now>=q.event_ms && now-q.event_ms<=800 &&
          m>=q.receive_mono_ms && m-q.receive_mono_ms<=800))) {
      n+=1;
     }
    }
    ready_at_failure=n;
    stops.get(&primary).ok_or("missing socket control")?.send(true).map_err(|_|"socket reader stopped".to_string())?;
    // Only the WS reader Done event can now invalidate the active source.
   }
  }
  if disconnect_observed_mono.is_some() && first.is_some() && failover.is_none() {
   if let Some(id)=manager.suggest_ready_reserve("g",now,m){
    match manager.promote("g",id,now,m){
     Ok(epoch)=>{failover=Some((id,epoch,m));epochs.push(epoch);},
     Err(_)=>promotions_failed+=1,
    }
   }
  }
  match tokio::time::timeout(Duration::from_millis(25),rx.recv()).await {
   Ok(Some(Incoming::Connected(id,peer)))=>{
    if manager.add_session(id,&peer,"g").is_err(){errors+=1}
   },
   Ok(Some(Incoming::Data(id,raw,recv_utc,recv_mono)))=>{
    let idx=(id-1) as usize;
    pool_frames[idx]+=1;
    if let Ok(v)=serde_json::from_str::<serde_json::Value>(&raw){
     let sym=v["data"]["s"].as_str().unwrap_or("").to_ascii_uppercase();
     let event_ms=v["data"]["E"].as_i64().unwrap_or(0);
     let seq=v["data"]["u"].as_u64().or_else(||v["data"]["lastUpdateId"].as_u64());
     let good=valid_depth(&v) && event_ms!=0;
     if recv_utc.saturating_sub(event_ms)>800{pool_late[idx]+=1;}
     let accepted=match manager.observe(id,&sym,event_ms,recv_utc,recv_mono,seq,good){
      Ok(ok)=>ok,
      Err(_)=>{errors+=1;false}
     };
     if first.is_none() && recv_mono>=30_000 {
      if let Some(best)=manager.suggest_ready_reserve("g",recv_utc,recv_mono){
       if let Ok(epoch)=manager.promote("g",best,recv_utc,recv_mono){
        first=Some((best,epoch,recv_mono));epochs.push(epoch);
       }
      }
     }
     if disconnect_observed_mono.is_some() && first.is_some() && failover.is_none() {
      if let Some(best)=manager.suggest_ready_reserve("g",recv_utc,recv_mono){
       if let Ok(epoch)=manager.promote("g",best,recv_utc,recv_mono){
        failover=Some((best,epoch,recv_mono));epochs.push(epoch);
       }
      }
     }
     // NEVER publish a standby-origin message: only the selected live session.
     if manager.active.get("g").is_some_and(|sel|sel.session_id==id) && accepted {
       if let Some(item)=manager.publish("g",&sym,recv_utc,recv_mono){
        if published_event_watermarks.get(&item.symbol).is_some_and(|old|item.event_ms<=*old){published_duplicate_or_regression+=1;}
        published_event_watermarks.insert(item.symbol,item.event_ms);
        published+=1;
        if forced_mono.is_some(){published_after+=1;}
       }else{blocked_due_to_bad+=1;}
     }else if id==first.map(|x|x.0).unwrap_or(0) && attempted {
       blocked_due_to_bad+=1;
     }
    }else{errors+=1}
   },
   Ok(Some(Incoming::Done(id,why)))=>{
    done.push(format!("session{id}:{why}"));
    if manager.sessions.contains_key(&id) {
     let was_active=manager.active.get("g").is_some_and(|a|a.session_id==id);
     manager.invalidate(id)?;
     if was_active && first.is_some() && attempted {
      disconnect_observed_mono=Some(start.elapsed().as_millis() as u64);
      active_source_invalidated_on_done=true;
     }
    }
   },
   Ok(None)=>break,
   Err(_)=>{}
  }
 }
 let now=utc()?;
 for id in 1..=(N as u64){
  if manager.sessions.contains_key(&id) {manager.record_quality(id,now)?;}
 }
 manager.history.save_atomic(&history_path)?;
 history_saves+=1;
 let reread=History::load(&history_path,now)?;
 let response=serde_json::json!({
  "schema":"SPOTARB/R1197/LOCAL_WEBSOCKET_CLOSE_AUTO_FAILOVER/V1",
  "source":"Binance public futures depth20@500ms","mode":"source_only",
  "real_orders":0,"paper_orders":0,"trading_db_writes":0,
  "symbols":SYMBOLS,"sessions_requested":N,
  "frames_received_by_session":pool_frames,
  "aged_gt800_by_session":pool_late,
  "first_active":first.map(|(id,ep,t)|serde_json::json!({"id":id,"epoch":ep,"at_mono_ms":t})),
  "socket_close_requested_mono_ms":90000,
  "ws_disconnect_observed_mono_ms":disconnect_observed_mono,
  "active_source_invalidated_on_ws_reader_done":active_source_invalidated_on_done,
  "publisher_duplicate_or_event_regression":published_duplicate_or_regression,
  "actual_fault_mono_ms":forced_mono,
  "ready_hot_reserves_at_fault":ready_at_failure,
  "best_backup_promoted":failover.map(|(id,ep,t)|serde_json::json!({"id":id,"epoch":ep,"at_mono_ms":t})),
  "detection_to_promotion_ms":match (disconnect_observed_mono,failover){(Some(t),Some((_,_,f)))=>Some(f.saturating_sub(t)),_=>None},
  "fault_to_promotion_ms":match (forced_mono,failover){
   (Some(t),Some((_,_,f)))=>Some(f.saturating_sub(t)),_=>None},
  "epochs":epochs,"published_total":published,
  "published_post_fault":published_after,
  "blocked_on_stale_or_invalid":blocked_due_to_bad,
  "errors":errors,"promotions_failed":promotions_failed,
  "max_ready_control":max_ready_before,
  "completed_sources":done,
  "history_saves":history_saves,
  "history_reloaded_peer_records":reread.peers.len(),
  "history_profiles":reread.peers,
  "limitations":"Locally requested actual socket closure; NOT a spontaneous Binance/network RST; no DB1/DB2/ACK or 233-symbol certification. No RTT socket sampler."
 });
 let out=dir.join("r1197_quality_disconnect_report.json");
 let bytes=serde_json::to_vec_pretty(&response).map_err(|e|e.to_string())?;
 fs::write(&out,&bytes).map_err(|e|e.to_string())?;
 println!("R1197_REPORT {}",out.display());
 println!("R1197_OUTCOME {}",serde_json::json!({
   "first_active":response["first_active"],
   "ready_hot_reserves_at_fault":ready_at_failure,
   "best_backup_promoted":response["best_backup_promoted"],
   "fault_to_promotion_ms":response["fault_to_promotion_ms"],
   "detection_to_promotion_ms":response["detection_to_promotion_ms"],
   "publisher_duplicate_or_event_regression":published_duplicate_or_regression,
   "received_frames":pool_frames,"late_frames":pool_late,
   "published_after_fault":published_after,"history_saves":history_saves,
   "errors":errors
 }));
 Ok(())
}
