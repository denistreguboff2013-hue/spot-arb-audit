// Public ticker transport only. Existing R123 validators; no execution capability.
use super::*;
use futures_util::{SinkExt,StreamExt};
use spotarb_native_market_core::native_public_turnover_ws_r123_review::{parse_ws_ticker,PublicWsEnvelope,WsTickerQuote};
#[derive(Default)] struct State {quotes:BTreeMap<String,[Option<(WsTickerQuote,u64)>;2]>,accepted:u64,superseded:u64,rejected:u64,closed:u64,controls:u64,errors:Vec<String>}
#[derive(Default)] pub(super) struct Turnover {state:Mutex<State>}
impl Turnover {
 pub(super) fn configure(&self,bases:&[String])->Result<(),Failure>{let mut s=self.state.lock().map_err(|_|"ticker poison")?;if !s.quotes.is_empty(){return Err("ticker scope already bound".into())}for b in bases{s.quotes.insert(b.clone(),[None,None]);}Ok(())}
 fn put(&self,q:WsTickerQuote,leg:usize)->Result<(),String>{
  let mut s=self.state.lock().map_err(|_|"R1325_TICKER_STATE_POISON")?;
  let pair=s.quotes.get_mut(&q.base).ok_or("R1325_TICKER_OUT_OF_SCOPE")?;
  if pair[leg].as_ref().is_some_and(|(old,_)|old.epoch!=q.epoch||old.sequence>=q.sequence||old.source_ms>q.source_ms){return Err("R1325_TICKER_SEQUENCE".into())}
  let previous=pair[leg].replace((q,core::ingress_runtime_r1205::mono_ns()));s.accepted+=1;if previous.is_some(){s.superseded+=1;}Ok(())
 }
 fn revoke(&self,bases:&[String],leg:usize,error:Option<String>){
  if let Ok(mut s)=self.state.lock(){for b in bases{if let Some(p)=s.quotes.get_mut(b){p[leg]=None;}}s.closed+=1;if let Some(e)=error{s.rejected+=1;if s.errors.len()<64{s.errors.push(e);}}}
 }
 pub(super) fn witness(&self,base:&str,now_ns:u64,now_ms:i64,min:f64)->Result<Value,String>{
  let s=self.state.try_lock().map_err(|_|"R1325_TURNOVER_BUSY")?;
  let p=s.quotes.get(base).ok_or("R1325_TURNOVER_SCOPE")?;
  let mut out=Vec::new();
  for x in p{let(q,recv)=x.as_ref().ok_or("R1325_TURNOVER_MISSING")?;
   if now_ns.checked_sub(*recv).map_or(true,|a|a>800_000_000)||now_ms.checked_sub(q.source_ms).map_or(true,|a|!(0..=800).contains(&a)){return Err("R1325_TURNOVER_STALE".into())}
   if q.quote_24h<min{return Err("R1325_TURNOVER_BELOW_MINIMUM".into())}
   out.push(json!({"base":q.base,"quote_24h":q.quote_24h,"source_ms":q.source_ms,"received_ms":q.received_ms,"generation":q.epoch,"sequence":q.sequence}));
  }Ok(json!({"venue_order":["okx_spot","binance_futures"],"quotes":out,"max_age_ms":800}))
 }
 pub(super) fn report(&self)->Value{match self.state.lock(){Ok(s)=>json!({"accepted":s.accepted,"control_messages":s.controls,"superseded":s.superseded,"rejected_events_or_connections":s.rejected,"revocation_operations":s.closed,"errors":s.errors,"bounded_symbols":s.quotes.len(),"freshness_ms":800}),Err(_)=>json!({"error":"poison"})}}
}
pub(super) struct Runs {tasks:Vec<tokio::task::JoinHandle<()>>,source:Arc<Turnover>,bases:Vec<String>}
impl Drop for Runs {fn drop(&mut self){for t in &self.tasks{t.abort();}self.source.revoke(&self.bases,0,None);self.source.revoke(&self.bases,1,None);}}
impl Runs {pub(super) async fn stop(&mut self){for t in &self.tasks{t.abort();}for t in self.tasks.drain(..){let _=t.await;}self.source.revoke(&self.bases,0,None);self.source.revoke(&self.bases,1,None);}}
pub(super) fn start(source:Arc<Turnover>,bases:&[String],generation:u64,duration_ms:u64)->Result<Runs,Failure>{
 source.configure(bases)?;let mut tasks=Vec::new();
 for group in bases.chunks(core::ingress_runtime_r1205::R1205_MAX_BASES_PER_SHARD){
  for leg in 0..2{let bases=group.to_vec();let source=source.clone();tasks.push(tokio::spawn(async move{
   let result=tokio::time::timeout(Duration::from_millis(duration_ms),run(source.clone(),&bases,leg,generation)).await;
   let error=match result{Ok(Ok(()))|Err(_)=>None,Ok(Err(e))=>Some(e)};source.revoke(&bases,leg,error);
  }));}
 }Ok(Runs{tasks,source,bases:bases.to_vec()})
}
async fn run(source:Arc<Turnover>,bases:&[String],leg:usize,generation:u64)->Result<(),String>{
 use tokio_tungstenite::{connect_async_with_config,tungstenite::{Message,protocol::WebSocketConfig}};
 let url=if leg==0{core::universal_connection_manager_r1209::adapter_for(Venue::OkxSpot).endpoint(bases)}else{format!("wss://fstream.binance.com/market/stream?streams={}",bases.iter().map(|b|format!("{}usdt@ticker",b.to_ascii_lowercase())).collect::<Vec<_>>().join("/"))};
 let cfg=WebSocketConfig{max_message_size:Some(65_536),max_frame_size:Some(65_536),..Default::default()};
 let(mut ws,_)=connect_async_with_config(&url,Some(cfg),false).await.map_err(|e|format!("R1325_TICKER_CONNECT:{leg}:{e}"))?;
 if leg==0{ws.send(Message::Text(json!({"op":"subscribe","args":bases.iter().map(|b|json!({"channel":"tickers","instId":format!("{b}-USDT")})).collect::<Vec<_>>()} ).to_string())).await.map_err(|e|e.to_string())?;}
 let mut sequence=0u64;
 while let Some(message)=ws.next().await{
  match message.map_err(|e|e.to_string())?{
   Message::Ping(p)=>ws.send(Message::Pong(p)).await.map_err(|e|e.to_string())?,
   Message::Pong(_)=>{},Message::Close(_)=>return Err("R1325_TICKER_CLOSED".into()),
   Message::Text(raw)=>{
    if raw=="pong"{continue}let v:Value=serde_json::from_str(&raw).map_err(|e|e.to_string())?;
    if ticker_control(&v)?{source.state.lock().map_err(|_|"ticker poison")?.controls+=1;continue}
    let(base,body,validator_url)=if leg==0{
     let symbol=v["arg"]["instId"].as_str().ok_or_else(||format!("R1325_TICKER_SYMBOL:{leg}:{}",raw.chars().take(512).collect::<String>()))?;
     (symbol.strip_suffix("-USDT").ok_or("ticker quote")?.to_string(),raw.clone(),url.clone())
    }else{
     let data=&v["data"];let symbol=data["s"].as_str().ok_or_else(||format!("R1325_TICKER_SYMBOL:{leg}:{}",raw.chars().take(512).collect::<String>()))?;let base=symbol.strip_suffix("USDT").ok_or("ticker quote")?.to_string();
     let stream=format!("{}usdt@ticker",base.to_ascii_lowercase());if v["stream"].as_str()!=Some(stream.as_str()){return Err("R1325_TICKER_STREAM_BINDING".into())}
     // A verified combined-stream envelope contains the unchanged single-stream payload.
     // The actual fixed public URL and subscribed scope are owned here, not supplied by callers.
     (base,data.to_string(),format!("wss://fstream.binance.com/market/ws/{stream}"))
    };
    if !bases.contains(&base){return Err("R1325_TICKER_SCOPE".into())}sequence=sequence.checked_add(1).ok_or("ticker sequence overflow")?;
    let now=core::ingress_runtime_r1205::wall_ms();let q=match parse_ws_ticker(now,PublicWsEnvelope{venue:if leg==0{Venue::OkxSpot}else{Venue::BinanceFutures},url:&validator_url,base:&base,connection_epoch:generation,sequence,received_wall_ms:now,body:&body}){
     Ok(q)=>q,Err(e)=>{source.revoke(&[base.clone()],leg,Some(format!("R1325_TICKER_VALIDATOR:{leg}:{base}:{e:?}")));continue}
    };
    source.put(q,leg)?;
   },_=>return Err("R1325_TICKER_MESSAGE_TYPE".into())
  }
 }Err("R1325_TICKER_EOF".into())
}
#[cfg(test)] impl Turnover {
 pub(super) fn seed(&self,base:&str,n:u64,w:i64){let mut s=self.state.lock().unwrap();let q=WsTickerQuote{base:base.into(),quote_24h:250000.0,source_ms:w,received_ms:w,epoch:1,sequence:1};s.quotes.insert(base.into(),[Some((q.clone(),n)),Some((q,n))]);}
}
#[cfg(test)] mod tests {
 use super::*;
 #[test] fn r1325_auto_turnover_stale_future_scope_and_revoke(){
  let t=Turnover::default();t.seed("AAA",1_000_000_000,10000);
  assert!(t.witness("AAA",1_000_000_000,10000,150000.0).is_ok());
  assert!(t.witness("AAA",1_000_000_000,9999,150000.0).is_err());
  assert!(t.witness("AAA",1_801_000_000,10801,150000.0).is_err());
  assert!(t.witness("BBB",1_000_000_000,10000,150000.0).is_err());
  assert!(t.witness("AAA",1_000_000_000,10000,300000.0).is_err());
  t.seed("BBB",1_000_000_000,10000);t.revoke(&["AAA".into()],1,None);assert!(t.witness("BBB",1_000_000_000,10000,150000.0).is_ok());assert!(t.witness("AAA",1_000_000_000,10000,150000.0).is_err());
 }
}

fn ticker_control(v:&Value)->Result<bool,String>{
 if v["event"]=="error"{return Err(format!("R1325_TICKER_SUBSCRIBE:{}",v["code"]))}
 Ok(v["event"]=="subscribe"||(v["event"]=="notice"&&v["code"]=="64010"))
}
#[cfg(test)] mod control_tests {
 use super::*;
 #[test] fn r1325_okx_retirement_notice_is_not_market_data(){
  assert!(ticker_control(&json!({"event":"notice","code":"64010","msg":"WebSocket port 8443 will be retired soon. Please migrate to port 443."})).unwrap());
  assert!(ticker_control(&json!({"event":"subscribe","arg":{"channel":"tickers","instId":"BTC-USDT"}})).unwrap());
  assert!(!ticker_control(&json!({"arg":{"channel":"tickers"},"data":[]})).unwrap());
  assert!(ticker_control(&json!({"event":"error","code":"60012"})).is_err());
 }
}
