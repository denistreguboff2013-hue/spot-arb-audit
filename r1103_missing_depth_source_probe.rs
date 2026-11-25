//! Astra R1103: narrow public Binance source diagnostic, no PAPER, private API, or orders.
use futures_util::{SinkExt,StreamExt};
use serde_json::{json,Value};
use std::{collections::BTreeMap,fs::{self,OpenOptions},io::Write,path::Path,process::{self,Command},time::{Duration,Instant,SystemTime,UNIX_EPOCH}};
use tokio_tungstenite::{connect_async,tungstenite::Message};
fn deny(s:&str)->!{eprintln!("R1103_DENIED {s}");process::exit(70)}
fn wall_ms()->u64{SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64}
#[tokio::main]
async fn main(){
 if std::env::var("ASTRA_R1103_PUBLIC_DIAG_ONLY").ok().as_deref()!=Some("YES")
   ||std::env::args().len()!=2{deny("exact standalone public diagnostic opt-in and root required")}
 let root=std::env::args().nth(1).unwrap();
 let root=Path::new(&root);
 let base=Path::new("/mnt/spotarb_db_test/astra_p001_r874_test_scratch_20261025");
 if root.parent()!=Some(base)||!root.file_name().and_then(|s|s.to_str())
   .is_some_and(|s|s.starts_with("astra_r1103_public_probe_")){deny("private root outside scratch")}
 let meta=fs::symlink_metadata(root).unwrap_or_else(|_|deny("root missing"));
 use std::os::unix::fs::{MetadataExt,PermissionsExt};
 if !meta.file_type().is_dir()||meta.permissions().mode()&0o077!=0
   ||meta.uid()!=unsafe{libc::geteuid()}{deny("private root permissions or ownership")}
 let service=Command::new("/usr/bin/systemctl")
   .args(["show","spotarb-native-paper-continuous.service","-p","ActiveState","-p","MainPID","-p","UnitFileState"])
   .output().unwrap_or_else(|_|deny("systemctl inaccessible"));
 let service_txt=String::from_utf8_lossy(&service.stdout);
 if !service.status.success()||
   !service_txt.lines().any(|s|s=="ActiveState=inactive")||
   !service_txt.lines().any(|s|s=="MainPID=0")||
   !service_txt.lines().any(|s|s=="UnitFileState=disabled"){deny("installed PAPER cannot be active")}
 let kill:Value=serde_json::from_slice(&fs::read("/home/tmpadmin/spotbot/data/real_kill_switch.json")
   .unwrap_or_else(|_|deny("kill file unavailable"))).unwrap_or_else(|_|deny("kill file invalid"));
 if kill["state"]!="ENGAGED"{deny("REAL kill not ENGAGED")}
 let bases=["AUCTION","BICO","JOE","USDC","THETA"];
 let mut counts=BTreeMap::new();
 let mut first=BTreeMap::new();let mut last=BTreeMap::new();
 for b in bases{counts.insert(b.to_string(),0_u64);}
 let streams=bases.iter().map(|b|format!("{}usdt@depth20@100ms",b.to_lowercase())).collect::<Vec<_>>();
 let url=format!("wss://fstream.binance.com/public/stream?streams={}",streams.join("/"));
 let start_time=wall_ms();
 let begin=Instant::now();
 let(mut ws,_)=match tokio::time::timeout(Duration::from_secs(15),connect_async(&url)).await{
   Ok(Ok(x))=>x,
   Ok(Err(e))=>deny(&format!("public connect: {e}")),
   Err(_)=>deny("bounded public connect timeout")
 };
 let mut raw=0_u64;let mut unknown=0_u64;let mut malformed=0_u64;
 let mut disconnect=None::<String>;
 let mut pongs=0_u64;
 while begin.elapsed()<Duration::from_secs(50){
   let left=Duration::from_secs(50).saturating_sub(begin.elapsed()).min(Duration::from_secs(2));
   match tokio::time::timeout(left,ws.next()).await{
     Ok(Some(Ok(Message::Text(s))))=>{
       raw+=1;
       if let Ok(v)=serde_json::from_str::<Value>(&s){
         let stream=v["stream"].as_str().unwrap_or_default();
         if let Some(b)=stream.strip_suffix("usdt@depth20@100ms"){
           let b=b.to_ascii_uppercase();
           if let Some(n)=counts.get_mut(&b){
             *n+=1;
             first.entry(b.clone()).or_insert_with(wall_ms);
             last.insert(b,wall_ms());
           }else{unknown+=1}
         }else{unknown+=1}
       }else{malformed+=1}
     },
     Ok(Some(Ok(Message::Ping(payload))))=>{let _=ws.send(Message::Pong(payload)).await;pongs+=1},
     Ok(Some(Ok(_)))=>{},
     Ok(Some(Err(e)))=>{disconnect=Some(format!("{e}"));break},
     Ok(None)=>{disconnect=Some("socket closed".into());break},
     Err(_)=>{}
   }
 }
 let _=ws.close(None).await;
 let missing=bases.iter().filter(|b|counts[**b]==0).copied().collect::<Vec<_>>();
 let report=json!({
   "schema":"SPOTARB/ASTRA/R1103/BINANCE-DEPTH20-PUBLIC-FIRST-FRAME/V1",
   "source":"actual_binance_public_depth20_100ms","version":"public/stream",
   "requested_bases":bases,"requested_exact_streams":streams,
   "connection_established":true,
   "url_subscription_ack_received":false,
   "url_subscription_ack_explanation":"Combined URL handshake is NOT a per-stream server ACK",
   "source_raw_text_frames":raw,"first_wall_ms":first,
   "last_wall_ms":last,"frames_by_requested_base":counts,
   "no_frame_in_window":missing,
   "unrequested_stream_frames":unknown,"malformed_json_frames":malformed,
   "ping_pongs":pongs,"disconnected":disconnect,
   "start_wall_ms":start_time,"end_wall_ms":wall_ms(),
   "duration_elapsed_s":begin.elapsed().as_secs_f64(),
   "real_orders":0,"private_exchange_requests":0,"paper_portfolio_changes":0,
   "source_diagnostic_only":true,"all_expected_bases_observed":missing.is_empty()
 });
 let result=root.join("r1103_binance_source_diagnostic.json");
 let mut file=OpenOptions::new().write(true).create_new(true).open(&result)
   .unwrap_or_else(|_|deny("diagnostic file already exists"));
 let bytes=serde_json::to_vec_pretty(&report).unwrap();
 file.write_all(&bytes).unwrap_or_else(|_|deny("write report"));
 file.sync_all().unwrap_or_else(|_|deny("fsync report"));
 println!("{}",report);
 if unknown>0||malformed>0||raw==0{process::exit(2)}
}
