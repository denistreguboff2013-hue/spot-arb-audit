//! R1187: eight PUBLIC Binance WS shards from the frozen R1186 A2 233-base universe, no DB1/DB2/OKX or orders.
 //! No REST, no private keys, no execution API, no venue reconnect loops.
use futures_util::{StreamExt,SinkExt,future::join_all};
use spotarb_native_market_core::native_binance_ws_ip_guard_r1110::{
    r1110_reserve_at_lock,R1110Outcome};
use std::{path::{Path,PathBuf},time::{Duration,Instant,SystemTime,UNIX_EPOCH},
    collections::{BTreeMap,BTreeSet},fs};
use tokio_tungstenite::{connect_async,tungstenite::Message,MaybeTlsStream};
const SINGLE_SYMBOLS:[&str;3]=["ethusdt","solusdt","xrpusdt"];
const GUARD:&str="/mnt/spotarb_db_test/astra_paper_continuous_v1/r1110_binance_ws_ip_guard.lock";
const KILL:&str="/home/tmpadmin/spotbot/data/real_kill_switch.json";
const SHARD_GROUPS:&[&[&str];8]=&[&["1INCH","2Z","A","AAVE","ACE","ACH","ACT","ADA","AEVO","AGLD","AIXBT","ALGO","ALLO","ANIME","APE","API3","APT","AR","ARB","ARKM","ASTER","ASTR","ATH","ATOM","AUCTION","AVAX","AVNT","AXS","BABY","BANANA","BAND","BARD"],&["BASED","BAT","BCH","BERA","BICO","BIGTIME","BIO","BLUR","BNB","BNT","BOME","BREV","BTC","CATI","CC","CELO","CELR","CETUS","CFG","CFX","CHIP","CHZ","COMP","CRV","CT","CVC","CVX","DASH","DOGE","DOGS","DOOD","DOS"],&["DOT","DYDX","EDGE","EGLD","EIGEN","ENA","ENJ","ENS","ETC","ETH","ETHFI","ETHW","FET","FIL","FLOW","FLUID","FOGO","G","GALA","GAS","GLM","GMT","GMX","GOAT","GRAM","GRASS","GRT","GRVT","HBAR","HMSTR","HUMA","HYPE"],&["ICP","ID","ILV","IMX","INJ","IOST","IOTA","IRYS","JOE","JTO","JUP","KAIA","KAITO","KAT","KITE","KMNO","KNC","KSM","LAYER","LDO","LINEA","LINK","LIT","LPT","LQTY","LSK","LTC","MAGIC","MANA","MASK","ME","MEGA"],&["MEME","MERL","MET","METIS","MEW","MINA","MMT","MON","MOODENG","MORPHO","MOVE","NEAR","NEIRO","NEO","NIGHT","NMR","NOT","ONDO","ONE","ONT","OP","ORDI","PARTI","PAXG","PENDLE","PENGU","PEOPLE","PHA","PIXEL","PNUT","POL","PONS"],&["PROVE","PUMP","PYTH","QTUM","RE","RENDER","RESOLV","ROBO","RPL","RSR","RVN","S","SAFE","SAHARA","SAND","SCR","SEI","SENT","SKL","SKY","SLP","SLX","SNX","SOL","SONIC","SOPH","SPACE","SPK","SSV","STRK","STX","SUI"],&["SUSHI","T","TAO","THETA","TIA","TNSR","TOSHI","TRB","TRUMP","TRX","TURBO","UMA","UNI","USDC","VIRTUAL","VVV","W","WAXP","WCT","WET","WIF","WLD","WLFI","WOO","XAUT","XLM","XPL","XRP","XTZ","YB","YFI","YGG"],&["ZAMA","ZEC","ZEN","ZETA","ZIL","ZK","ZORA","ZRO","ZRX"]];
#[derive(Clone,serde::Serialize)]
struct Frame{stream:String,event_ms:i64,wall_ms:i64,relative_ms:u64,update_id:Option<u64>,poll_gap_us:u64,poll_wait_us:u64,parse_us:u64}
#[derive(serde::Serialize)]
struct ResultLane{
 lane:String,peer:String,started_ms:i64,ended_ms:i64,reason:String,
 frames:Vec<Frame>,errors:u64,pings:u64}
fn wall()->Result<i64,String>{
 let x=SystemTime::now().duration_since(UNIX_EPOCH).map_err(|e|format!("clock:{e}"))?;
 i64::try_from(x.as_millis()).map_err(|_|"timestamp overflow".to_string())
}
fn require_safe()->Result<(),String>{
 if std::env::var("ASTRA_R1187_EIGHT_WS_READONLY").ok().as_deref()!=Some("YES"){
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
async fn lane(label:&str,end:Instant,start:Instant,width:usize,part:usize)->Result<ResultLane,String>{
 require_safe()?;
 permit(end).await?;
 if width!=8 {return Err(format!("R1193 only width8 allowed, got {width}"));}
 let cohort:Vec<&str>=SHARD_GROUPS.iter().flat_map(|g|g.iter().copied()).collect();
 if cohort.len()!=233 {return Err(format!("expected 233 instruments, got {}",cohort.len()))}
 let start_idx=part*8;
 if start_idx>=cohort.len(){return Err(format!("invalid part {part}"))}
 let end_idx=(start_idx+8).min(cohort.len());
 let query=cohort[start_idx..end_idx].iter()
    .map(|b|format!("{}usdt@depth20@500ms",b.to_ascii_lowercase()))
    .collect::<Vec<_>>().join("/");
 let url=format!("wss://fstream.binance.com/public/stream?streams={query}");
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
 let mut prev_poll_return:Option<Instant>=None;
 let reason=loop{
  if Instant::now()>=end{break "normal_bound_end".to_string()}
  let timeout=end.saturating_duration_since(Instant::now()).min(Duration::from_secs(2));
  let poll_gap_us=prev_poll_return.map(|t|Instant::now().duration_since(t).as_micros().min(u64::MAX as u128) as u64).unwrap_or(0);
  let poll_start=Instant::now();
  let next=tokio::time::timeout(timeout,ws.next()).await;
  let poll_wait_us=poll_start.elapsed().as_micros().min(u64::MAX as u128) as u64;
  prev_poll_return=Some(Instant::now());
  match next{
   Ok(Some(Ok(Message::Text(raw))))=>{
    let received=wall()?;
    let parse_started=Instant::now();
    if let Ok(v)=serde_json::from_str::<serde_json::Value>(&raw){
     let parse_us=parse_started.elapsed().as_micros().min(u64::MAX as u128) as u64;
     if let Some(ts)=v.get("data").and_then(|v|v.get("E")).and_then(|v|v.as_i64()){
      let stream=v.get("stream").and_then(|x|x.as_str()).unwrap_or("unknown")
        .split('@').next().unwrap_or("unknown").to_string();
      frames.push(Frame{stream,event_ms:ts,wall_ms:received,
         relative_ms:start.elapsed().as_millis() as u64,update_id:v.get("data").and_then(|d|d.get("u")).and_then(|u|u.as_u64()),poll_gap_us,poll_wait_us,parse_us});
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
    "source_age_max_ms":age.last(),"aged_gt800":stale,"aged_gt3000":age.iter().filter(|x|**x>3000).count(),"aged_gt10000":age.iter().filter(|x|**x>10000).count(),"source_age_p99_ms":pct(99),
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
 let m:BTreeMap<i64,i64>=a.frames.iter().filter(|x|x.stream=="1inchusdt").map(|x|(x.event_ms,x.wall_ms)).collect();
 let mut diffs=Vec::new();let mut matched=BTreeSet::new();
 for f in b.frames.iter().filter(|x|x.stream=="1inchusdt"){
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
fn per_coin(v:&ResultLane)->serde_json::Value {
 let mut coins:BTreeMap<String,Vec<i64>>=BTreeMap::new();
 let mut badbins:BTreeMap<String,BTreeMap<i64,u64>>=BTreeMap::new();
 for f in &v.frames {
  let age=f.wall_ms-f.event_ms;
  coins.entry(f.stream.clone()).or_default().push(age);
  if age>800 {*badbins.entry(f.stream.clone()).or_default().entry(f.wall_ms.div_euclid(10_000)).or_default()+=1;}
 }
 let mut obj=serde_json::Map::new();
 for (name,mut ages) in coins {
  ages.sort_unstable();let n=ages.len();
  let late=ages.iter().filter(|x|**x>800).count();
  obj.insert(name.clone(),serde_json::json!({
   "frames":n,"aged_gt800":late,"aged_gt3000":ages.iter().filter(|x|**x>3000).count(),
   "p50_ms":ages[(n-1)/2],"p95_ms":ages[(n-1)*95/100],"p99_ms":ages[(n-1)*99/100],
   "max_ms":ages[n-1],"bad_10s_bins":badbins.remove(&name).unwrap_or_default()
  }));
 }
 serde_json::Value::Object(obj)
}
fn match_same_event(a:&ResultLane,b:&ResultLane,sym:&str)->serde_json::Value {
 let mut first=BTreeMap::<(i64,u64),i64>::new();
 for f in a.frames.iter().filter(|f|f.stream==sym) {
  first.insert((f.event_ms,f.update_id.unwrap_or(0)),f.wall_ms);
 }
 let mut diffs=Vec::<i64>::new();let mut matched=BTreeSet::new();
 for f in b.frames.iter().filter(|f|f.stream==sym) {
  let key=(f.event_ms,f.update_id.unwrap_or(0));
  if let Some(t)=first.get(&key){diffs.push(f.wall_ms-*t);matched.insert(key);}
 }
 diffs.sort_unstable();let n=diffs.len();
 serde_json::json!({"symbol":sym,"matched_E_u":matched.len(),
  "single_minus_group_p50_ms":if n>0{Some(diffs[(n-1)/2])}else{None},
  "single_minus_group_p95_ms":if n>0{Some(diffs[(n-1)*95/100])}else{None},
  "single_minus_group_min_ms":diffs.first(),"single_minus_group_max_ms":diffs.last(),
  "group_events":first.len(),"single_events":b.frames.iter().filter(|f|f.stream==sym).count()})
}

fn path_windows(v:&ResultLane)->serde_json::Value{
 let mut a:BTreeMap<i64,Vec<&Frame>>=BTreeMap::new();
 for f in &v.frames {a.entry(f.wall_ms.div_euclid(10_000)).or_default().push(f);}
 serde_json::json!(a.into_iter().map(|(bin,fs)|{
   let bad:Vec<&Frame>=fs.iter().copied().filter(|f|f.wall_ms-f.event_ms>800).collect();
   let maxval=|ff:&[&Frame],key:fn(&Frame)->u64|->u64{ff.iter().map(|f|key(f)).max().unwrap_or(0)};
   serde_json::json!({"bin":bin,"frames":fs.len(),"aged_gt800":bad.len(),
     "max_age_ms":fs.iter().map(|f|f.wall_ms-f.event_ms).max().unwrap_or(0),
     "max_poll_gap_us":maxval(&fs,|f|f.poll_gap_us),
     "max_poll_wait_us":maxval(&fs,|f|f.poll_wait_us),
     "max_parse_us":maxval(&fs,|f|f.parse_us),
     "stale_max_poll_gap_us":maxval(&bad,|f|f.poll_gap_us),
     "stale_max_poll_wait_us":maxval(&bad,|f|f.poll_wait_us),
     "stale_max_parse_us":maxval(&bad,|f|f.parse_us)})
 }).collect::<Vec<_>>())
}


fn match_all_events(a:&ResultLane,b:&ResultLane)->serde_json::Value {
 let mut first=BTreeMap::<(String,i64,u64),i64>::new();
 let mut last=BTreeMap::<(String,i64,u64),i64>::new();
 for f in &a.frames {first.insert((f.stream.clone(),f.event_ms,f.update_id.unwrap_or(0)),f.wall_ms);}
 for f in &b.frames {last.insert((f.stream.clone(),f.event_ms,f.update_id.unwrap_or(0)),f.wall_ms);}
 let mut diffs=Vec::<i64>::new();let mut staleA_freshB=0u64;let mut staleB_freshA=0u64;
 let mut matched_each=BTreeMap::<String,(u64,u64,u64)>::new();
 for (k,t) in &first{
  if let Some(u)=last.get(k){
   let diff=*u-*t;diffs.push(diff);
   let aa=*t-k.1;let bb=*u-k.1;
   let entry=matched_each.entry(k.0.clone()).or_default();
   entry.0+=1;
   if aa>800 && bb<=800 {staleA_freshB+=1;entry.1+=1;}
   if bb>800 && aa<=800 {staleB_freshA+=1;entry.2+=1;}
  }
 }
 diffs.sort_unstable();
 let n=diffs.len();
 serde_json::json!({"A":a.lane,"B":b.lane,"A_peer":a.peer,"B_peer":b.peer,
    "A_unique_E_u":first.len(),"B_unique_E_u":last.len(),"matched_E_u":n,
    "B_minus_A_p50_ms":if n>0{Some(diffs[(n-1)/2])}else{None},
    "B_minus_A_p95_ms":if n>0{Some(diffs[(n-1)*95/100])}else{None},
    "B_minus_A_min_ms":diffs.first(),"B_minus_A_max_ms":diffs.last(),
    "matched_A_stale_B_fresh":staleA_freshB,"matched_B_stale_A_fresh":staleB_freshA,
    "per_symbol_matched_and_stale_control":matched_each})
}


fn match_level_to_level(xs:&[&ResultLane],ys:&[&ResultLane])->serde_json::Value {
 let mut a=BTreeMap::<(String,i64,u64),i64>::new();
 let mut b=BTreeMap::<(String,i64,u64),i64>::new();
 for v in xs {for f in &v.frames {a.insert((f.stream.clone(),f.event_ms,f.update_id.unwrap_or(0)),f.wall_ms);}}
 for v in ys {for f in &v.frames {b.insert((f.stream.clone(),f.event_ms,f.update_id.unwrap_or(0)),f.wall_ms);}}
 let mut diffs=Vec::<i64>::new();let mut counts=[0u64;4];let mut samples=Vec::<serde_json::Value>::new();
 for (k,t) in &a{
  if let Some(u)=b.get(k){
   diffs.push(*u-*t);
   let a_fresh=*t-k.1<=800;let b_fresh=*u-k.1<=800;
   counts[if a_fresh {if b_fresh{0}else{1}} else {if b_fresh{2}else{3}}]+=1;
   if !a_fresh || !b_fresh{
    let delta=*u-*t;
    if delta.abs()>2000 && samples.len()<16{
     samples.push(serde_json::json!({"symbol":k.0,"E":k.1,"u":k.2,
       "A_age_ms":*t-k.1,"B_age_ms":*u-k.1,"B_minus_A_ms":delta}));
    }
   }
  }
 }
 diffs.sort_unstable();let n=diffs.len();
 serde_json::json!({"A_events":a.len(),"B_events":b.len(),"matched_E_u":n,
   "both_fresh":counts[0],"A_fresh_B_stale":counts[1],
   "A_stale_B_fresh":counts[2],"both_stale":counts[3],
   "B_minus_A_min_ms":diffs.first(),"B_minus_A_max_ms":diffs.last(),
   "B_minus_A_p50_ms":if n>0{Some(diffs[(n-1)/2])}else{None},
   "B_minus_A_p95_ms":if n>0{Some(diffs[(n-1)*95/100])}else{None},
   "event_samples_over_2s":samples})
}
fn match_merged_levels(a:&ResultLane,bs:&[&ResultLane])->serde_json::Value {
 match_level_to_level(&[a],bs)
}



fn ensemble_matched(lanes:&[ResultLane])->serde_json::Value{
 let n=lanes.len();
 let mut map:BTreeMap<(String,i64,u64),Vec<Option<i64>>>=BTreeMap::new();
 for (i, lane) in lanes.iter().enumerate(){
  for f in &lane.frames{
   let key=(f.stream.clone(),f.event_ms,f.update_id.unwrap_or(0));
   map.entry(key).or_insert_with(||vec![None;n])[i]=Some(f.wall_ms);
  }
 }
 let mut full=0u64;let mut all_fresh=0u64;let mut all_stale=0u64;
 let mut mixed=0u64;let mut worst_skew=0i64;let mut sums=vec![0u64;n];
 let mut examples=Vec::<serde_json::Value>::new();
 for (key, times) in &map{
  if times.iter().any(|x|x.is_none()){continue}
  full+=1;
  let mut min_age=i64::MAX;let mut max_age=i64::MIN;
  for (i,t) in times.iter().enumerate(){
   let age=t.unwrap()-key.1;
   min_age=min_age.min(age);max_age=max_age.max(age);
   if age>800{sums[i]+=1;}
  }
  if max_age<=800{all_fresh+=1}
  else if min_age>800{all_stale+=1}
  else{mixed+=1}
  let skew=max_age-min_age;
  if skew>worst_skew{worst_skew=skew}
  if skew>3000 && examples.len()<12{
   examples.push(serde_json::json!({"symbol":key.0,"E":key.1,
    "skew_ms":skew,"min_age_ms":min_age,"max_age_ms":max_age}));
  }
 }
 serde_json::json!({"lane_count":n,"unique_event_keys":map.len(),
   "matched_all_12_sockets":full,"all_12_fresh":all_fresh,
   "all_12_stale":all_stale,"some_fresh_some_stale":mixed,
   "worst_same_event_skew_ms":worst_skew,
   "stale_count_by_lane_on_full_overlap":sums,
   "examples_over_3000ms":examples})
}

#[tokio::main(flavor="multi_thread",worker_threads=2)]
async fn main()->Result<(),String>{
 require_safe()?;
 let path=std::env::args().nth(1).ok_or("scratch dir required")?;
 let dir=PathBuf::from(path).canonicalize().map_err(|e|e.to_string())?;
 if !dir.to_string_lossy().starts_with(
    "/mnt/spotarb_db_test/astra_p001_r874_test_scratch_20261025/r1194_binance_same8_route_"){
    return Err("not an approved isolated scratch path".into())
 }
 let cohort:Vec<&str>=SHARD_GROUPS.iter().flat_map(|g|g.iter().copied()).collect();
 if cohort.len()!=233{return Err("expected 233 frozen A2 bases".into())}
 let before=Instant::now();let end=before+Duration::from_secs(1200);
 let tasks=(0..12).map(|idx| async move{
   let label=format!("route_{idx:02}");
   lane(&label,end,before,8,22).await
 });
 let results=join_all(tasks).await;
 let lanes:Vec<ResultLane>=results.into_iter().collect::<Result<Vec<_>,_>>()?;
 if lanes.len()!=12{return Err("expected 12 complete lanes".into())}
 let lane_summary:Vec<_>=lanes.iter().map(summary).collect();
 let per_coin:BTreeMap<_,_>=lanes.iter().map(|a|(a.lane.clone(),per_coin(a))).collect();
 let windows:BTreeMap<_,_>=lanes.iter().map(|a|(a.lane.clone(),windows(a,before))).collect();
 let polling:BTreeMap<_,_>=lanes.iter().map(|a|(a.lane.clone(),path_windows(a))).collect();
 let output=serde_json::json!({
   "schema":"SPOTARB/R1194/20MIN_SAME8_ROUTE_CLASS/V1",
   "source":"Binance USD-M PUBLIC depth20@500ms",
   "sources":"12 independent route/socket copies of exactly the same 8-symbol frozen cohort #22",
   "frozen_bases":cohort,"cohort_same8":&cohort[176..184],
   "frozen_a2_base_list_sha256":"a9f88c4ce9dc0528c80da956b8e2585d900e1df26440d8415b598ea3a54b6028",
   "read_only":true,"rest_requests":0,"private_orders":0,"real_orders":0,
   "reconnects_attempted":0,
   "r1110_connect_guard":"original shared file, 3sec minimum gap, 10/min",
   "duration_bound_seconds":1200,
   "lanes":lane_summary,
   "per_coin":per_coin,"window_summary":windows,
   "exact_same_event_ensemble":ensemble_matched(&lanes),
   "read_path_10s":polling,
   "measurement_limit":"source only, no DB/DecisionCore, no kernel ingress, staggered connects",
   "finished_wall_ms":wall()?
 });
 let file=dir.join("r1194_same8_route_report.json");
 use std::io::Write;
 let mut f=std::fs::OpenOptions::new().write(true).create_new(true)
   .open(&file).map_err(|e|format!("create evidence:{e}"))?;
 let enc=serde_json::to_vec_pretty(&output).map_err(|e|format!("serialize:{e}"))?;
 f.write_all(&enc).map_err(|e|format!("write:{e}"))?;
 f.sync_all().map_err(|e|format!("fsync:{e}"))?;
 println!("R1194_REPORT {}",file.display());
 println!("R1194_LANES {}",serde_json::json!(lanes.iter().map(summary).collect::<Vec<_>>()));
 Ok(())
}
