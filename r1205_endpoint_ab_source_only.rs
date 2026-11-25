//! Official endpoints only. Separate Futures partial and SPOT diff cohorts.
use spotarb_native_market_core::{ingress_runtime_r1205::{mono_ns,wall_ms,permit},ingress_transport_r1205::{MeasuredTcp,Trace}};
use std::{sync::{Arc,Mutex},time::Duration,collections::BTreeMap,path::PathBuf,os::fd::AsRawFd};
use tokio_tungstenite::tungstenite::Message;
use futures_util::{StreamExt,SinkExt};
async fn lane(id:usize,url:String,market:&str,start:tokio::time::Instant)->Result<serde_json::Value,String>{
 let end=start+Duration::from_secs(85);permit(end).await?;let parsed=url::Url::parse(&url).map_err(|e|e.to_string())?;let host=parsed.host_str().ok_or("host")?;let port=parsed.port_or_known_default().unwrap_or(443);let dns=tokio::net::lookup_host((host,port)).await.map_err(|e|e.to_string())?.take(16).map(|a|a.to_string()).collect::<Vec<_>>();
 let tcp=tokio::time::timeout(Duration::from_secs(10),tokio::net::TcpStream::connect((host,port))).await.map_err(|e|e.to_string())?.map_err(|e|e.to_string())?;let peer=tcp.peer_addr().map_err(|e|e.to_string())?;let local=tcp.local_addr().map_err(|e|e.to_string())?;let fd=tcp.as_raw_fd();
 let trace=Arc::new(Mutex::new(Trace::default()));let tcp=MeasuredTcp::new(tcp,trace.clone()).map_err(|e|e.to_string())?;let cfg=tokio_tungstenite::tungstenite::protocol::WebSocketConfig{max_message_size:Some(32768),max_frame_size:Some(32768),..Default::default()};let (mut ws,_)=tokio::time::timeout(Duration::from_secs(15),tokio_tungstenite::client_async_tls_with_config(&url,tcp,Some(cfg),None)).await.map_err(|e|e.to_string())?.map_err(|e|e.to_string())?;
 let mut events=Vec::new();let mut tcp_rows=Vec::new();let mut timer=tokio::time::interval(Duration::from_millis(200));let mut total=0u64;let mut reason="bounded_end".to_string();
 while tokio::time::Instant::now()<end{
  tokio::select!{
   _=timer.tick()=>{if tcp_rows.len()<450{tcp_rows.push(spotarb_native_market_core::ingress_runtime_r1205::sample_tcp(fd));}},
   msg=ws.next()=>match msg{
    Some(Ok(Message::Text(raw)))=>{total+=1;let v:serde_json::Value=serde_json::from_str(&raw).map_err(|e|e.to_string())?;let d=v.get("data").unwrap_or(&v);let event=d["E"].as_i64().ok_or("missing E in timestamped diagnostic stream")?;let u=d["u"].as_u64().ok_or("missing u")?;let ns=mono_ns();let wall=wall_ms();if events.len()>=3000{return Err("bounded endpoint telemetry cap".into())}events.push(serde_json::json!({"E":event,"u":u,"receive_ns":ns,"receive_wall_ms":wall,"age_ms":wall-event,"elapsed_ms":start.elapsed().as_millis(),"kind":if market=="SPOT"{"DIFF_DEPTH_NO_BOOK_RECONSTRUCTION"}else{"PARTIAL_DEPTH20_SNAPSHOT"}}));},
    Some(Ok(Message::Ping(_)))=>{ws.flush().await.map_err(|e|e.to_string())?;},
    Some(Ok(Message::Close(_)))|None=>{reason="closed".into();break},Some(Err(e))=>{reason=e.to_string();break},_=>{}
   }
  }
 }
 let _=tokio::time::timeout(Duration::from_millis(200),ws.close(None)).await;
 let transport=trace.lock().map_err(|_|"trace poison")?.summary();Ok(serde_json::json!({"id":id,"market":market,"url":url,"dns":dns,"local":local.to_string(),"peer":peer.to_string(),"frames":total,"reason":reason,"events":events,"tcp":tcp_rows,"transport":transport,"reconnects":0,"orders":0}))
}
#[tokio::main(flavor="multi_thread",worker_threads=2)]async fn main()->Result<(),Box<dyn std::error::Error>>{
 if std::env::var("R1205_SOURCE_ONLY").ok().as_deref()!=Some("YES"){return Err("source-only gate required".into())}
 let output=std::env::args().nth(1).map(PathBuf::from).ok_or("output path")?;if output.exists(){return Err("do not overwrite evidence".into())}
 let endpoints=[("FUTURES","wss://fstream.binance.com/public/stream?streams=btcusdt@depth20@100ms"),("FUTURES","wss://fstream.binance.com/public/ws/btcusdt@depth20@100ms"),("SPOT","wss://stream.binance.com:443/stream?streams=btcusdt@depth@100ms"),("SPOT","wss://data-stream.binance.vision:443/stream?streams=btcusdt@depth@100ms")];
 let start=tokio::time::Instant::now();let mut tasks=Vec::new();for(id,(market,url))in endpoints.into_iter().enumerate(){tasks.push(tokio::spawn(lane(id,url.into(),market,start)));}
 let mut lanes=Vec::new();for t in tasks{match t.await?{Ok(row)=>lanes.push(row),Err(e)=>lanes.push(serde_json::json!({"error":e}))}}
 let result=serde_json::json!({"schema":"R1205/OFFICIAL_ENDPOINT_AB/V1","lanes":lanes,"windows_ms":[[15000,45000],[45000,75000]],"market_cross_comparison":false,"orders":0,"system_tuning":false});std::fs::write(&output,serde_json::to_vec(&result)?)?;println!("{}",serde_json::json!({"output":output,"lanes":result["lanes"].as_array().unwrap().len(),"orders":0}));Ok(())
}
