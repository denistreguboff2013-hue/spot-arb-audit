//! Astra P0-01 R1104: 8 production-shaped PUBLIC Binance depth-only WS shards + direct 5-base control.
//! NO PaperPortfolio, DB1/DB2 writes, private API, execution, credential, or installed service.
use futures_util::{future::join_all,SinkExt,StreamExt};
use serde_json::{json,Value};
use std::{collections::{BTreeMap,BTreeSet},fs::{self,OpenOptions},io::Write,path::Path,process::{self,Command},time::{Duration,Instant,SystemTime,UNIX_EPOCH}};
use tokio_tungstenite::{connect_async,tungstenite::Message};
fn deny(s:&str)->!{eprintln!("R1104_DENIED {s}");process::exit(70)}
fn wall()->u64{SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64}
async fn task(id:usize,bases:Vec<String>,seconds:u64)->Value{
 let streams=bases.iter().map(|b|format!("{}usdt@depth20@100ms",b.to_ascii_lowercase())).collect::<Vec<_>>();
 let url=format!("wss://fstream.binance.com/public/stream?streams={}",streams.join("/"));
 let start=wall();let started=Instant::now();
 let mut counts=bases.iter().map(|b|(b.clone(),0u64)).collect::<BTreeMap<_,_>>();
 let mut first=BTreeMap::new();let mut last=BTreeMap::new();
 let mut raw=0u64;let mut malformed=0u64;let mut unexpected=0u64;let mut disconnect=None::<String>;
 let mut ws=match tokio::time::timeout(Duration::from_secs(16),connect_async(url)).await{
   Ok(Ok((ws,_)))=>ws,
   Ok(Err(e))=>return json!({"shard":id,"requested":bases,"connected":false,"connect_error":format!("{e}"),"start_wall_ms":start}),
   Err(_)=>return json!({"shard":id,"requested":bases,"connected":false,"connect_error":"connection timeout","start_wall_ms":start})
 };
 while started.elapsed()<Duration::from_secs(seconds){
   let left=Duration::from_secs(seconds).saturating_sub(started.elapsed()).min(Duration::from_secs(2));
   match tokio::time::timeout(left,ws.next()).await{
      Ok(Some(Ok(Message::Text(s))))=>{
       raw+=1;
       if let Ok(v)=serde_json::from_str::<Value>(&s){
         if let Some(st)=v["stream"].as_str(){
           if let Some(n)=st.strip_suffix("usdt@depth20@100ms"){
             let b=n.to_ascii_uppercase();
             if let Some(cnt)=counts.get_mut(&b){
               *cnt+=1;
               first.entry(b.clone()).or_insert_with(wall);
               last.insert(b,wall());
             }else{unexpected+=1;}
           }else{unexpected+=1;}
         }else{unexpected+=1;}
       }else{malformed+=1;}
      },
      Ok(Some(Ok(Message::Ping(p))))=>{let _=ws.send(Message::Pong(p)).await;},
      Ok(Some(Ok(_)))=>{},
      Ok(Some(Err(e)))=>{disconnect=Some(format!("{e}"));break},
      Ok(None)=>{disconnect=Some("close".into());break},
      Err(_)=>{}
   }
 }
 let _=ws.close(None).await;
 let missing=bases.iter().filter(|b|counts[*b]==0).cloned().collect::<Vec<_>>();
 json!({"shard":id,"requested":bases,"connected":true,"subscription_ack":false,
   "seen_count":counts.len()-missing.len(),"missing":missing,
   "raw_frames":raw,"malformed":malformed,"unexpected":unexpected,
   "disconnect":disconnect,"count_by_base":counts,"first_frame_wall_ms":first,
   "last_frame_wall_ms":last,"start_wall_ms":start,"end_wall_ms":wall()})
}
#[tokio::main]
async fn main(){
 if std::env::var("ASTRA_R1104_PUBLIC_FLEET_ONLY").ok().as_deref()!=Some("YES")
    ||std::env::args().len()!=2{deny("no opt-in or incorrect argument count")}
 let root=std::env::args().nth(1).unwrap();
 let p=Path::new(&root);
 if p.parent()!=Some(Path::new("/mnt/spotarb_db_test/astra_p001_r874_test_scratch_20261025"))
   ||!p.file_name().and_then(|x|x.to_str()).is_some_and(|x|x.starts_with("astra_r1104_fleet_")){
   deny("untrusted scratch path")
 }
 use std::os::unix::fs::{MetadataExt,PermissionsExt};
 let md=fs::symlink_metadata(p).unwrap_or_else(|_|deny("private root missing"));
 if !md.file_type().is_dir()||md.permissions().mode()&0o077!=0
    ||md.uid()!=unsafe{libc::geteuid()}{deny("unsafe scratch permissions")}
 let state=Command::new("/usr/bin/systemctl").args(["show",
  "spotarb-native-paper-continuous.service","-p","ActiveState","-p","MainPID","-p","UnitFileState"])
  .output().unwrap_or_else(|_|deny("service guard inaccessible"));
 let data=String::from_utf8_lossy(&state.stdout);
 if !state.status.success()||!data.lines().any(|x|x=="ActiveState=inactive")
   ||!data.lines().any(|x|x=="MainPID=0")||!data.lines().any(|x|x=="UnitFileState=disabled"){
   deny("installed PAPER must be inactive")
 }
 let kill:Value=serde_json::from_slice(&fs::read("/home/tmpadmin/spotbot/data/real_kill_switch.json")
    .unwrap_or_else(|_|deny("REAL kill missing"))).unwrap_or_else(|_|deny("REAL kill malformed"));
 if kill["state"]!="ENGAGED"{deny("REAL kill must be engaged")}
 let source=p.join("r1104_bases.json");
 let bases:Vec<String>=serde_json::from_slice(&fs::read(&source)
   .unwrap_or_else(|_|deny("base file missing"))).unwrap_or_else(|_|deny("invalid base JSON"));
 if bases.len()!=234||!bases.windows(2).all(|w|w[0]<w[1])
   ||bases.iter().any(|x|x.len()<1||x.len()>24||
     !x.bytes().all(|ch|ch.is_ascii_uppercase()||ch.is_ascii_digit())){
   deny("expected exact sorted 234 supported names, no duplicates")
 }
 let selected=["AUCTION","BICO","JOE","USDC","THETA"];
 if selected.iter().any(|b|!bases.iter().any(|x|x==b)){deny("missing selected controls")}
 let mut futures=Vec::new();
 for (i,chunk) in bases.chunks(32).enumerate(){futures.push(task(i,chunk.to_vec(),55));}
 futures.push(task(8,selected.iter().map(|s|s.to_string()).collect::<Vec<_>>(),55));
 let start=wall();
 let reports=join_all(futures).await;
 let mut observed=BTreeSet::new();let mut missing=Vec::new();
 let mut selected_fleet=BTreeMap::new();let mut faults=0_u64;
 for (idx,v) in reports.iter().enumerate(){
   if v["connected"]!=true||v["disconnect"].is_string()||v["malformed"].as_u64().unwrap_or(0)!=0
        ||v["unexpected"].as_u64().unwrap_or(0)!=0{faults+=1;}
   if idx==8{continue;}
   for (b,c) in v["count_by_base"].as_object().into_iter().flatten(){
     if c.as_u64().unwrap_or(0)>0{observed.insert(b.clone());}
   }
   if let Some(a)=v["missing"].as_array(){for b in a{missing.push(b.clone());}}
   for b in selected{
     if let Some(c)=v["count_by_base"].get(b).and_then(Value::as_u64){
       selected_fleet.insert(b.to_string(),c);
     }
   }
 }
 missing.sort_by_key(|x|x.as_str().unwrap_or_default().to_string());
 let direct=&reports[8];
 let report=json!({"schema":"SPOTARB/ASTRA/R1104/FULL234-PUBLIC-DEPTH-PER-SHARD/V1",
  "scope":"Binance public only, observed raw stream coverage, NOT live integrated PAPER PASS",
  "fleet_requested":234,"shard_count":8,"fleet_unique_raw_observed":observed.len(),
  "fleet_missing":missing,"fleet_selected_counts":selected_fleet,
  "direct_control_requested":selected,
  "direct_control_seen":direct["seen_count"],
  "direct_control_counts":direct["count_by_base"],
  "per_shard":reports,"transport_or_envelope_fault_shards":faults,
  "start_wall_ms":start,"end_wall_ms":wall(),
  "all_fleet_raw_streams_observed":observed.len()==234,
  "subscription_ack_from_binance_per_stream":false,
  "real_orders":0,"private_api":false,"paper_service_started":false});
 let file=p.join("r1104_fleet_source_diagnostic.json");
 let mut h=OpenOptions::new().write(true).create_new(true).open(&file)
   .unwrap_or_else(|_|deny("output already exists"));
 h.write_all(&serde_json::to_vec_pretty(&report).unwrap())
   .unwrap_or_else(|_|deny("cannot write report"));
 h.sync_all().unwrap_or_else(|_|deny("cannot fsync report"));
 println!("{}",json!({"file":file,"fleet_seen":observed.len(),
    "missing":report["fleet_missing"],"direct":report["direct_control_counts"],
    "fleet_selected":report["fleet_selected_counts"],"fault_shards":faults}));
 if faults>0{process::exit(2)}
}
