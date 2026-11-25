//! Bounded PUBLIC WS driver shared by the actual Rust entry point and E2E probe.
use crate::{RawFrame,Venue,R1247FrameTiming};
use crate::market_data_ingress_r1205::{Engine,Output,SharedStore,Authority,key,provenance};
use crate::native_r1173_compact_writer::ingress_store_r1205::StoreWorker;
use crate::universal_connection_manager_r1209::{adapter_for,AdapterControl,UniversalConnectionManager};
use std::{sync::{Arc,Mutex,atomic::{AtomicBool,AtomicUsize,Ordering}},collections::{HashMap,VecDeque},net::SocketAddr,time::{Duration,Instant as StdInstant,SystemTime,UNIX_EPOCH},fs::{File,OpenOptions},io::{BufWriter,Write},path::PathBuf};
use futures_util::{SinkExt,StreamExt};
use tokio::{sync::mpsc,time::Instant};
use tokio_tungstenite::tungstenite::Message;
use serde::{Serialize,Deserialize};
pub fn mono_ns()->u64{let mut t=libc::timespec{tv_sec:0,tv_nsec:0};unsafe{libc::clock_gettime(libc::CLOCK_MONOTONIC,&mut t)};t.tv_sec as u64*1_000_000_000+t.tv_nsec as u64}
pub fn wall_ms()->i64{SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as i64}
const R1268_CAPTURE_PER_SOCKET_MAX:u64=128*1024*1024;
fn r1268_wire_shape(raw:&str)->serde_json::Value{
 let Ok(v)=serde_json::from_str::<serde_json::Value>(raw) else{return serde_json::json!({"json":false,"bytes":raw.len()})};
 let keys=|x:&serde_json::Value|x.as_object().map(|m|m.keys().cloned().collect::<Vec<_>>()).unwrap_or_default();
 let data=v.get("data").and_then(|x|if x.is_array(){x.as_array().and_then(|a|a.first())}else{Some(x)});
 let row=data.unwrap_or(&serde_json::Value::Null);
 serde_json::json!({"json":true,"top_keys":keys(&v),"stream":v.get("stream"),"channel":v.pointer("/arg/channel"),"instrument":v.pointer("/arg/instId").or_else(||row.get("s")),"action":v.get("action"),"event":row.get("e"),"U":row.get("U"),"u":row.get("u"),"pu":row.get("pu"),"lastUpdateId":row.get("lastUpdateId"),"seqId":row.get("seqId"),"prevSeqId":row.get("prevSeqId"),"data_count":v.get("data").and_then(|x|x.as_array()).map(Vec::len),"row_keys":keys(row),"bid_updates":row.get("b").or_else(||row.get("bids")).and_then(|x|x.as_array()).map(Vec::len),"ask_updates":row.get("a").or_else(||row.get("asks")).and_then(|x|x.as_array()).map(Vec::len)})
}
fn r1268_capture_line(w:&mut BufWriter<File>,used:&mut u64,row:serde_json::Value)->Result<bool,String>{
 let mut bytes=serde_json::to_vec(&row).map_err(|e|e.to_string())?;bytes.push(b'\n');
 if used.saturating_add(bytes.len()as u64)>R1268_CAPTURE_PER_SOCKET_MAX{return Ok(false)}
 w.write_all(&bytes).map_err(|e|e.to_string())?;*used+=bytes.len()as u64;Ok(true)
}
static OKX_NEXT_NS:Mutex<u64>=Mutex::new(0);
const GUARD:&str="/mnt/spotarb_db_test/astra_paper_continuous_v1/r1110_binance_ws_ip_guard.lock";
pub const CENTRAL_FRAME_QUEUE_CAPACITY:usize=32;
pub const READER_FRAME_QUEUE_CAPACITY:usize=16;
pub const R1205_OUTPUT_QUEUE_CAPACITY:usize=48;
// Six live source lanes can report connect, subscribe, close, and socket evidence
// in one startup burst. Keep the control path bounded without dropping that burst.
pub const R1205_CONTROL_QUEUE_CAPACITY:usize=64;
pub const R1205_REQUIRED_COPIES:usize=3;
pub const R1205_SOURCE_CONNECTIONS_PER_SHARD:usize=6;
pub const R1205_ENGINE_BATCH_BUFFER_FRAMES:usize=32;
pub const R1205_EVENT_QUEUE_CAPACITY:usize=1024;
pub const R1205_WRITER_BATCH_BUFFER_FRAMES:usize=32;
pub const R1205_MAX_BASES_PER_SHARD:usize=8;
pub const R1205_FRAME_PAYLOAD_MAX_BYTES:usize=32_768;
pub const R1205_QUEUE_PAYLOAD_BUDGET_BYTES:u128=240*1024*1024;
pub fn estimate_r1205_queue_payload_bytes(groups:usize)->u128{
 let per_group=(R1205_SOURCE_CONNECTIONS_PER_SHARD*READER_FRAME_QUEUE_CAPACITY+
  CENTRAL_FRAME_QUEUE_CAPACITY+R1205_OUTPUT_QUEUE_CAPACITY+
  R1205_ENGINE_BATCH_BUFFER_FRAMES)as u128;
 let global=(R1205_EVENT_QUEUE_CAPACITY+R1205_WRITER_BATCH_BUFFER_FRAMES+
  crate::r1241_ingress_pipeline::R1241_CANONICAL_INPUT_CAPACITY)as u128;
 (groups as u128).saturating_mul(per_group).saturating_add(global)
  .saturating_mul(R1205_FRAME_PAYLOAD_MAX_BYTES as u128)
}
#[cfg(test)]
#[derive(Clone)]
struct R1333OfflineEndpoint{url_template:String,connector:native_tls::TlsConnector}
#[cfg(test)]
fn r1334_apply_test_rcvbuf(stream:&tokio::net::TcpStream)->(Option<i32>,Option<i32>){
 use std::os::fd::AsRawFd;
 let requested=std::env::var("R1334_TEST_RCVBUF_BYTES").ok().and_then(|v|v.parse::<i32>().ok()).filter(|v|*v>0);
 let Some(requested)=requested else{return(None,None)};
 let fd=stream.as_raw_fd();
 let _=unsafe{libc::setsockopt(fd,libc::SOL_SOCKET,libc::SO_RCVBUF,&requested as *const _ as *const _,std::mem::size_of_val(&requested) as libc::socklen_t)};
 let mut actual=0i32;let mut len=std::mem::size_of_val(&actual) as libc::socklen_t;
 let rc=unsafe{libc::getsockopt(fd,libc::SOL_SOCKET,libc::SO_RCVBUF,&mut actual as *mut _ as *mut _,&mut len)};
 (Some(requested),(rc==0).then_some(actual))
}
#[derive(Clone)]pub struct Config{pub shard:usize,pub bases:Vec<String>,pub copies:usize,pub duration:Duration,pub controlled_close_after:Option<Duration>,pub endpoint_variant:usize,pub probe_rotation_after:Option<Duration>}
#[derive(Default,Debug,Clone,Serialize,Deserialize)]
pub struct R1382ReaderClassify { pub market_calls:u64, pub control_calls:u64, pub market_ns:u128, pub control_ns:u128, pub max_ns:u64 }
impl R1382ReaderClassify {
 fn record(&mut self,kind:&AdapterControl,elapsed:u64){self.max_ns=self.max_ns.max(elapsed);if *kind==AdapterControl::Market{self.market_calls+=1;self.market_ns+=elapsed as u128;}else{self.control_calls+=1;self.control_ns+=elapsed as u128;}}
}
#[derive(Default,Debug,Clone,Serialize,Deserialize)]pub struct SocketEvidence{pub session:String,pub venue:String,pub url:String,pub connection_epoch:u64,pub local:String,pub peer:String,pub received:u64,pub stale:u64,pub age_samples:Vec<i64>,#[serde(default)]pub age_evictions:u64,pub tcp_samples:Vec<serde_json::Value>,#[serde(default)]pub queue_send_wait_ns:Vec<u64>,#[serde(default)]pub queue_send_wait_evictions:u64,pub reader_queue_push_ns:Vec<u64>,#[serde(default)]pub reader_queue_push_evictions:u64,#[serde(default)]pub reader_queue_depth_max:usize,#[serde(default)]pub reader_queue_overflow:u64,#[serde(default)]pub r1247_intermessage_gap_ns:Vec<u64>,#[serde(default)]pub r1247_intermessage_gap_evictions:u64,#[serde(default)]pub r1247_event_ts_advance_ms:Vec<i64>,#[serde(default)]pub r1247_event_ts_advance_evictions:u64,#[serde(default)]pub r1247_event_ts_nonadvance:u64,#[serde(default)]pub r1247_event_ts_backward:u64,#[serde(default)]pub r1382_reader_classify:R1382ReaderClassify,pub end:String,pub connects:u64,pub established_ns:u64,#[serde(default)]pub tcp_connect_ns:u64,#[serde(default)]pub tls_ws_handshake_ns:u64,#[cfg(test)]#[serde(default)]pub test_rcvbuf_requested:Option<i32>,#[cfg(test)]#[serde(default)]pub test_rcvbuf_actual:Option<i32>,pub dns_ips:Vec<String>,pub transport:serde_json::Value}
#[derive(Default,Debug,Serialize)]pub struct Summary{pub sockets:Vec<SocketEvidence>,pub events:Vec<serde_json::Value>,pub coverage:Vec<serde_json::Value>,pub counts:serde_json::Value,pub engine:serde_json::Value,#[serde(default)]pub durable_writer:serde_json::Value,pub orders:u64,pub synthetic_latency:bool,pub diagnostic_event_evictions:u64,pub source_audit:Vec<serde_json::Value>,pub coverage_time:std::collections::BTreeMap<String,CoverageTime>,pub reader_received_total:u64,pub socket_history_evictions:u64,pub reader_frame_queue_capacity_per_session:usize,pub central_frame_queue_capacity:usize,pub reader_queue_overflow_events:u64,pub peak_reader_frame_queue_depth:usize,#[serde(default)]pub invalid_frame_audit:Vec<serde_json::Value>,#[serde(default)]pub stale_frame_audit:Vec<serde_json::Value>,#[serde(default)]pub okx_books_invalid_audit:Vec<serde_json::Value>,#[serde(default)]pub selection_audit:Vec<serde_json::Value>,#[serde(default)]pub readiness_audit:Vec<serde_json::Value>,#[serde(default)]pub engine_queue_age_samples_ns:Vec<u64>,#[serde(default)]pub engine_queue_age_evictions:u64,#[serde(default)]pub engine_batch_service_samples_ns:Vec<u64>,#[serde(default)]pub engine_batch_service_evictions:u64,#[serde(default)]pub output_send_wait_samples_ns:Vec<u64>,#[serde(default)]pub output_send_wait_evictions:u64,#[serde(default)]pub r1247_frame_stage_samples:Vec<serde_json::Value>,#[serde(default)]pub r1247_frame_stage_evictions:u64,#[serde(default)]pub r1247_selection_evaluations:Vec<serde_json::Value>,#[serde(default)]pub r1247_selection_evaluation_evictions:u64,#[serde(default)]pub engine_receive_to_start_samples_ns:Vec<u64>,#[serde(default)]pub engine_receive_to_start_evictions:u64,#[serde(default)]pub r1272_batch_second_metrics:Vec<serde_json::Value>,#[serde(default)]pub engine_queue_age_sample_origin_counts:serde_json::Value,#[serde(default)]pub r1273_engine_profile:serde_json::Value,#[serde(default)]pub r1279_engine_terminal_trace_samples:Vec<serde_json::Value>,#[serde(default)]pub r1279_engine_trace_dropped:u64,#[serde(default)]pub r1279_engine_trace_sampled_out:u64,#[serde(default)]pub central_queue_depth_samples:Vec<usize>,#[serde(default)]pub central_queue_depth_evictions:u64,#[serde(default)]pub central_queue_depth_max:usize,#[serde(default)]pub r1273_central_queue:serde_json::Value,#[serde(default)]pub r1379_e0_queue_phases:serde_json::Value}
#[derive(Default,Debug,Serialize)]pub struct CoverageTime{pub observed_ns:u64,pub two_hot_ns:u64,pub active_ns:u64,pub all_stale_ns:u64,pub deficit_episodes:u64,pub max_deficit_ns:u64,pub max_observation_gap_ns:u64,pub first_two_hot_ns:Option<u64>,pub after_warmup_ns:u64,pub after_warmup_two_hot_ns:u64,#[serde(skip)]last_ns:u64,#[serde(skip)]last_hot:bool,#[serde(skip)]last_active:bool,#[serde(skip)]last_stale:bool,#[serde(skip)]deficit_start:Option<u64>}
fn observe_coverage(map:&mut std::collections::BTreeMap<String,CoverageTime>,rows:serde_json::Value,now:u64){
 for row in rows.as_array().into_iter().flatten(){let Some(k)=row["key"].as_str()else{continue};let hot=row["hot_ready"].as_array().is_some_and(|a|a.len()>=2);let active=row["active"].is_string();let stale=row["fresh_sources"].as_array().map_or(true,|a|a.is_empty());let c=map.entry(k.into()).or_default();
  if c.last_ns!=0{let dt=now.saturating_sub(c.last_ns);c.observed_ns+=dt;c.max_observation_gap_ns=c.max_observation_gap_ns.max(dt);if c.last_hot{c.two_hot_ns+=dt}if c.last_active{c.active_ns+=dt}if c.last_stale{c.all_stale_ns+=dt}if c.first_two_hot_ns.is_some(){c.after_warmup_ns+=dt;if c.last_hot{c.after_warmup_two_hot_ns+=dt}}}
  if !hot&&c.deficit_start.is_none(){c.deficit_start=Some(now);c.deficit_episodes+=1;}if let Some(start)=c.deficit_start{c.max_deficit_ns=c.max_deficit_ns.max(now.saturating_sub(start));if hot{c.deficit_start=None;}}if hot&&c.first_two_hot_ns.is_none(){c.first_two_hot_ns=Some(now)}
  c.last_ns=now;c.last_hot=hot;c.last_active=active;c.last_stale=stale;
 }
}
fn is_planned_reconnect(reason:&str)->bool{matches!(reason,"PLANNED_PRE_24H_ROTATION"|"PLANNED_ROTATION_PROBE"|"QUALIFIED_COHORT_STALE_ROTATION")}
pub fn reconnect_delay(reason:&str,failures:u64)->Duration{if is_planned_reconnect(reason){Duration::from_secs(1)}else{Duration::from_secs((30*failures.max(1)).min(60))}}
fn bounded_push_u64(values:&mut Vec<u64>,evictions:&mut u64,value:u64,limit:usize){if values.len()>=limit{values.remove(0);*evictions=evictions.saturating_add(1);}values.push(value)}
fn frame_queue_residence_ns(frame:&RawFrame,engine_start_ns:u64)->Option<u64>{
 frame.source.as_ref()?.r1247_timing.as_ref()?.t3_enqueue_mono_ns
  .filter(|enqueued|engine_start_ns>=*enqueued).map(|enqueued|engine_start_ns-enqueued)
}
// R1379 E0B: diagnostics only, no queue/flow-control changes.
#[derive(Default,Serialize)]
struct E0PhaseAccum { count:u64, missing:u64, invalid_order:u64, total_ns:u128, max_ns:u64 }
impl E0PhaseAccum {
 fn add(&mut self,from:Option<u64>,to:Option<u64>)->Option<u64>{
  match(from,to){(Some(a),Some(b))if b>=a=>{let delta=b-a;self.count+=1;self.total_ns+=delta as u128;self.max_ns=self.max_ns.max(delta);Some(delta)},(Some(_),Some(_))=>{self.invalid_order+=1;None},_=>{self.missing+=1;None}}
 }
}
#[derive(Default)]
struct E0QueuePhases{
 frames:u64, without_instrument:u64,
 global:std::collections::BTreeMap<&'static str,E0PhaseAccum>,
 by_venue:std::collections::BTreeMap<String,std::collections::BTreeMap<&'static str,E0PhaseAccum>>,
 by_session:std::collections::BTreeMap<String,std::collections::BTreeMap<&'static str,E0PhaseAccum>>,
 by_instrument:std::collections::BTreeMap<String,std::collections::BTreeMap<&'static str,E0PhaseAccum>>,
 hist:std::collections::BTreeMap<&'static str,Vec<u64>>,
}
impl E0QueuePhases{
 fn observe(&mut self,frame:&RawFrame,engine_begin:u64){
  self.frames+=1;
  let timing=frame.source.as_ref().and_then(|s|s.r1279_timing.as_ref());
  let session=frame.source.as_ref().map(|s|s.session_id.clone()).unwrap_or_else(||"MISSING_SESSION".into());
  let venue=format!("{:?}",frame.venue);
  let instrument=timing.and_then(|t|t.instrument.as_deref()).map(|s|s.to_owned());
  if instrument.is_none(){self.without_instrument+=1;}
  let phases=[
   ("ws_to_reader_enqueue",timing.and_then(|t|t.ws_receive_ns),timing.and_then(|t|t.reader_enqueue_ns)),
   ("reader_queue16",timing.and_then(|t|t.reader_enqueue_ns),timing.and_then(|t|t.reader_dequeue_ns)),
   ("forwarder_to_queue32",timing.and_then(|t|t.reader_dequeue_ns),timing.and_then(|t|t.central_enqueue_ns)),
   // R1395: split permit wait from processing under the reserved Queue32 slot.
   ("forwarder_reserve_wait",timing.and_then(|t|t.reader_dequeue_ns),timing.and_then(|t|t.central_permit_acquired_ns)),
   ("forwarder_process_reserved",timing.and_then(|t|t.central_permit_acquired_ns),timing.and_then(|t|t.central_enqueue_ns)),
   ("central_queue32",timing.and_then(|t|t.central_enqueue_ns),timing.and_then(|t|t.central_dequeue_ns)),
   ("central_dequeue_to_engine",timing.and_then(|t|t.central_dequeue_ns),Some(engine_begin)),
  ];
  for (name,start,end) in phases{
   let delta=self.global.entry(name).or_default().add(start,end);
   self.by_venue.entry(venue.clone()).or_default().entry(name).or_default().add(start,end);
   self.by_session.entry(session.clone()).or_default().entry(name).or_default().add(start,end);
   if let Some(ref instrument)=instrument{self.by_instrument.entry(format!("{}:{}",venue,instrument)).or_default().entry(name).or_default().add(start,end);}
   if let Some(delta)=delta{let sample=self.hist.entry(name).or_default();bounded_push_u64(sample,&mut 0u64,delta,2048);}
  }
 }
 fn snapshot(&self)->serde_json::Value{
  let mut global=serde_json::Map::new();
  for (name,stat) in &self.global{
   let mut sample=self.hist.get(name).cloned().unwrap_or_default();sample.sort_unstable();
   let quantile=|p:usize|->Option<u64>{if sample.is_empty(){None}else{Some(sample[(sample.len()-1)*p/100])}};
   global.insert((*name).into(),serde_json::json!({"count":stat.count,"missing":stat.missing,"invalid_order":stat.invalid_order,"total_ns":stat.total_ns,"max_ns":stat.max_ns,"p50_tail_ns":quantile(50),"p95_tail_ns":quantile(95),"p99_tail_ns":quantile(99),"tail_samples":sample.len()}));
  }
  serde_json::json!({"frames":self.frames,"without_instrument":self.without_instrument,"global":global,"by_venue":self.by_venue,"by_session":self.by_session,"by_instrument":self.by_instrument,"hist_tail_capacity":2048,"time_weighted_queue_occupancy":"NOT_MEASURED"})
 }
}
pub(crate) fn exchange_age_ms(event_timestamp_ms:i64,wall_timestamp_ms:i64)->Option<i64>{
 wall_timestamp_ms.checked_sub(event_timestamp_ms)
}
#[derive(Default)]
struct R1272SecondStats{
 batches:u64,frames:u64,batch_frames_max:u64,service_ns_total:u128,service_ns_max:u64,
 queue_residence_frames:u64,queue_residence_ns_total:u128,queue_residence_ns_max:u64,
 receive_fallback_frames:u64,reader_send_wait_over_10ms:u64,reader_send_wait_ns_max:u64,

}
impl R1272SecondStats{
 fn value(&self,second:u64)->serde_json::Value{
  serde_json::json!({"second":second,"batches":self.batches,"frames":self.frames,
   "batch_frames_max":self.batch_frames_max,"service_mean_ms":if self.batches>0{Some(self.service_ns_total as f64/self.batches as f64/1e6)}else{None},
   "service_max_ms":self.service_ns_max as f64/1e6,"queue_residence_frames":self.queue_residence_frames,
   "queue_residence_mean_ms":if self.queue_residence_frames>0{Some(self.queue_residence_ns_total as f64/self.queue_residence_frames as f64/1e6)}else{None},
   "queue_residence_max_ms":self.queue_residence_ns_max as f64/1e6,"receive_fallback_frames":self.receive_fallback_frames,
   "reader_send_wait_over_10ms":self.reader_send_wait_over_10ms,"reader_send_wait_max_ms":self.reader_send_wait_ns_max as f64/1e6,
   "central_depth_at_dequeue_max":null})
 }
}
#[derive(Default,Serialize)]
struct R1273CentralProfile{enqueue_depth_samples:std::collections::VecDeque<usize>,dequeue_depth_samples:std::collections::VecDeque<usize>,send_wait_ns:std::collections::VecDeque<u64>,evictions:u64,max_depth:usize,max_send_wait_ns:u64,e0_last_ns:u64,e0_last_depth:usize,e0_integral_depth_ns:u128,e0_observed_ns:u64,e0_events:u64}
impl R1273CentralProfile{
 fn e0_account_depth(&mut self,depth:usize){
  let now=mono_ns();
  if self.e0_last_ns>0&&now>=self.e0_last_ns{
   let dt=now-self.e0_last_ns;
   self.e0_observed_ns=self.e0_observed_ns.saturating_add(dt);
   self.e0_integral_depth_ns=self.e0_integral_depth_ns.saturating_add((self.e0_last_depth as u128)*(dt as u128));
  }
  self.e0_last_ns=now;self.e0_last_depth=depth;self.e0_events=self.e0_events.saturating_add(1);
 }
 fn enqueue(&mut self,depth:usize,wait:u64){self.e0_account_depth(depth);self.max_depth=self.max_depth.max(depth);self.max_send_wait_ns=self.max_send_wait_ns.max(wait);if self.enqueue_depth_samples.len()>=8192{self.enqueue_depth_samples.pop_front();self.evictions+=1;}self.enqueue_depth_samples.push_back(depth);if self.send_wait_ns.len()>=8192{self.send_wait_ns.pop_front();self.evictions+=1;}self.send_wait_ns.push_back(wait);}
 fn dequeue(&mut self,depth:usize){self.e0_account_depth(depth);self.max_depth=self.max_depth.max(depth);if self.dequeue_depth_samples.len()>=8192{self.dequeue_depth_samples.pop_front();self.evictions+=1;}self.dequeue_depth_samples.push_back(depth);}
}
fn bounded_push_i64(values:&mut Vec<i64>,evictions:&mut u64,value:i64,limit:usize){if values.len()>=limit{values.remove(0);*evictions=evictions.saturating_add(1);}values.push(value)}
fn quality_rotation_allowed(bad_scopes:usize,total_scopes:usize,fresh_peer_cohorts:usize)->bool{
 if total_scopes==0||bad_scopes==0{false}
 else if bad_scopes==total_scopes{fresh_peer_cohorts>=1}
 else{fresh_peer_cohorts>=2}
}
const QUALITY_ROTATION_DWELL_NS:u64=3_000_000_000;
fn stale_scope_dwell_elapsed(timers:&mut HashMap<(String,String),u64>,session:&str,bases:&[String],ready:&[bool],now_ns:u64)->bool{
 if bases.len()!=ready.len(){return false;}
 let mut elapsed=false;
 for(base,is_ready)in bases.iter().zip(ready){let key=(session.to_owned(),base.clone());if *is_ready{timers.remove(&key);}else{let since=*timers.entry(key).or_insert(now_ns);elapsed|=now_ns.saturating_sub(since)>=QUALITY_ROTATION_DWELL_NS;}}
 timers.retain(|(id,base),_|id.as_str()!=session||bases.iter().any(|candidate|candidate==base));
 elapsed
}
fn quality_rotation_group_key(session:&str)->String{
 let mut parts=session.split('-');
 match(parts.next(),parts.next(),parts.next()){(Some(shard),Some(venue),Some(lane))=>format!("{shard}-{venue}-{lane}"),_=>session.to_owned()}
}
fn quality_rotation_cooldown_elapsed(last_rotation_ns:Option<u64>,now_ns:u64)->bool{
 last_rotation_ns.map_or(true,|last|now_ns.saturating_sub(last)>=30_000_000_000)
}
const STALE_ROUTE_COOLDOWN:Duration=Duration::from_secs(90);
#[derive(Default)]
struct RouteSelector{
 cursor:HashMap<String,usize>,
 cooling_until:HashMap<(String,SocketAddr),StdInstant>,
}
impl RouteSelector{
 fn order(&mut self,host:&str,addresses:&[SocketAddr],now:StdInstant)->Vec<SocketAddr>{
  if addresses.is_empty(){return Vec::new()}
  self.cooling_until.retain(|_,until|*until>now);
  let start=*self.cursor.get(host).unwrap_or(&0)%addresses.len();
  self.cursor.insert(host.to_owned(),(start+1)%addresses.len());
  let mut ready=Vec::with_capacity(addresses.len());
  let mut cooling=Vec::new();
  for offset in 0..addresses.len(){
   let address=addresses[(start+offset)%addresses.len()];
   if let Some(until)=self.cooling_until.get(&(host.to_owned(),address)).copied(){
    cooling.push((until,address));
   }else{ready.push(address);}
  }
  if ready.is_empty(){
   cooling.sort_by_key(|(until,_)|*until);
   cooling.into_iter().map(|(_,address)|address).collect()
  }else{
   ready.extend(cooling.into_iter().map(|(_,address)|address));
   ready
  }
 }
 fn cool(&mut self,host:&str,address:SocketAddr,now:StdInstant){
  let until=now+STALE_ROUTE_COOLDOWN;
  let key=(host.to_owned(),address);
  self.cooling_until.entry(key).and_modify(|old|*old=(*old).max(until)).or_insert(until);
 }
}
pub(crate) async fn enqueue_reader_frame(tx:&mpsc::Sender<RawFrame>,mut frame:RawFrame)->(bool,u64){
 let started=Instant::now();
 match tx.reserve().await{
  Ok(permit)=>{
   let wait=started.elapsed().as_nanos().min(u64::MAX as u128)as u64;
   let queued_ns=mono_ns();
   if let Some(source)=frame.source.as_mut(){
    if let Some(timing)=source.r1247_timing.as_mut(){timing.t3_queue_wait_ns=Some(wait);timing.t3_enqueue_mono_ns=Some(queued_ns);}
    if let Some(timing)=source.r1279_timing.as_mut(){timing.reader_enqueue_ns=Some(queued_ns);timing.reader_queue_capacity=Some(tx.max_capacity());timing.reader_queue_depth=Some(tx.max_capacity().saturating_sub(tx.capacity()));}
   }
   permit.send(frame);(true,wait)
  },
  Err(_)=>(false,started.elapsed().as_nanos().min(u64::MAX as u128)as u64),
 }
}
struct ReaderCleanup{stop:Arc<AtomicBool>,handles:Vec<tokio::task::AbortHandle>}
impl Drop for ReaderCleanup{fn drop(&mut self){self.stop.store(true,Ordering::SeqCst);for task in &self.handles{task.abort();}}}
enum Input{Frame(RawFrame),Connected(String,Venue,u64,Vec<String>),Subscribed(String),Closed(String,String),Socket(SocketEvidence)}
async fn enqueue_reader_input(tx:&mpsc::Sender<Input>,mut input:Input)->(bool,u64){
 let started=Instant::now();match tx.reserve().await{Ok(permit)=>{let wait=started.elapsed().as_nanos().min(u64::MAX as u128)as u64;let queued_ns=mono_ns();if let Input::Frame(frame)=&mut input{if let Some(source)=frame.source.as_mut(){if let Some(timing)=source.r1247_timing.as_mut(){timing.t3_queue_wait_ns=Some(wait);timing.t3_enqueue_mono_ns=Some(queued_ns);}if let Some(timing)=source.r1279_timing.as_mut(){timing.reader_enqueue_ns=Some(queued_ns);timing.reader_queue_capacity=Some(tx.max_capacity());timing.reader_queue_depth=Some(tx.max_capacity().saturating_sub(tx.capacity()));}}}permit.send(input);(true,wait)},Err(_)=>(false,started.elapsed().as_nanos().min(u64::MAX as u128)as u64)}
}
fn send_control(tx:&mpsc::Sender<Input>,stop:&AtomicBool,input:Input)->bool{
 if tx.try_send(input).is_ok(){true}else{stop.store(true,Ordering::SeqCst);false}
}
fn drain_frame_batch(rx:&mut mpsc::Receiver<Input>,first:RawFrame)->(Vec<RawFrame>,Option<Input>){drain_frame_batch_profiled(rx,first,None,None)}
fn drain_frame_batch_profiled(rx:&mut mpsc::Receiver<Input>,first:RawFrame,depth:Option<&AtomicUsize>,profile:Option<&Mutex<R1273CentralProfile>>)->(Vec<RawFrame>,Option<Input>){
 let mut batch=vec![first];
 while batch.len()<32{
  match rx.try_recv(){
   Ok(Input::Frame(mut frame))=>{let dequeue_ns=mono_ns();if let Some(t)=frame.source.as_mut().and_then(|s|s.r1279_timing.as_mut()){t.central_dequeue_ns=Some(dequeue_ns);}if let Some(depth)=depth{let after=depth.fetch_sub(1,Ordering::SeqCst).saturating_sub(1);if let Some(profile)=profile{if let Ok(mut p)=profile.lock(){p.dequeue(after);}}}batch.push(frame)},
   Ok(control)=>return(batch,Some(control)),
   Err(_)=>break,
  }
 }
 (batch,None)
}
pub async fn permit(end:Instant)->Result<(),String>{
 use crate::native_binance_ws_ip_guard_r1110::{r1110_reserve_at_lock,R1110Outcome};
 loop{if Instant::now()>=end{return Err("connect deadline".into())}
 let grant=tokio::task::spawn_blocking(||r1110_reserve_at_lock(std::path::Path::new(GUARD))).await.map_err(|_|"quota worker")?.map_err(|e|format!("R1110 {e:?}"))?;
 match grant{R1110Outcome::Granted=>return Ok(()),R1110Outcome::WaitMs(ms)=>{let d=Duration::from_millis(ms);if Instant::now()+d>=end{return Err("quota beyond deadline".into())}tokio::time::sleep(d).await;}}
 }
}
fn json_integer_field(raw:&str,field:&str,quoted:bool)->Option<i64>{
 let start=raw.find(field)?.checked_add(field.len())?;
 let after=raw.get(start..)?.trim_start().strip_prefix(':')?.trim_start();
 let value=if quoted{after.strip_prefix('"')?}else{after};
 let digits=value.bytes().take_while(|b|b.is_ascii_digit()).count();
 if digits==0{None}else{value.get(..digits)?.parse().ok()}
}
fn market_event_timestamp_ms(venue:Venue,raw:&str)->Option<i64>{match venue{
 Venue::OkxSpot=>json_integer_field(raw,"\"ts\"",true),
 Venue::BinanceFutures=>json_integer_field(raw,"\"E\"",false).or_else(||json_integer_field(raw,"\"T\"",false)),
}}
fn r1247_event_base(venue:Venue,raw:&str,bases:&[String])->Option<String>{
 match venue{
  Venue::OkxSpot=>raw.split("\"instId\":\"").nth(1)?.split('"').next()?.strip_suffix("-USDT").map(str::to_string),
  Venue::BinanceFutures=>{
   let symbol=raw.split("\"stream\":\"").nth(1).and_then(|v|v.split('"').next())
    .and_then(|v|v.split('@').next()).or_else(||raw.split("\"s\":\"").nth(1).and_then(|v|v.split('"').next()));
   symbol.map(|v|v.strip_suffix("usdt").or_else(||v.strip_suffix("USDT")).unwrap_or(v).to_ascii_uppercase())
    .or_else(||if bases.len()==1{Some(bases[0].clone())}else{None})
  }
 }
}
fn session(shard:usize,v:Venue,lane:usize,epoch:u64)->String{let mut b=[0u8;8];let _=openssl::rand::rand_bytes(&mut b);format!("g{shard}-{v:?}-{lane}-{epoch}-{}",b.iter().map(|v|format!("{v:02x}")).collect::<String>())}
fn url(v:Venue,bases:&[String],variant:usize)->String{
 if v==Venue::BinanceFutures&&variant==1&&bases.len()==1{format!("wss://fstream.binance.com/public/ws/{}usdt@{}",bases[0].to_ascii_lowercase(),adapter_for(v).channel())}else{adapter_for(v).endpoint(bases)}
}
pub fn sample_tcp(fd:i32)->serde_json::Value{
 let mut raw=[0u8;256];let mut len=raw.len() as libc::socklen_t;
 let rc=unsafe{libc::getsockopt(fd,libc::IPPROTO_TCP,libc::TCP_INFO,raw.as_mut_ptr()as *mut _,&mut len)};
 let get=|offset:usize|if rc==0&&len as usize>=offset+4{Some(u32::from_ne_bytes(raw[offset..offset+4].try_into().unwrap()))}else{None};
 let mut inq=0i32;let q=unsafe{libc::ioctl(fd,libc::FIONREAD,&mut inq)};let mut rcvbuf=0i32;let mut n=4;
 let br=unsafe{libc::getsockopt(fd,libc::SOL_SOCKET,libc::SO_RCVBUF,&mut rcvbuf as *mut _ as *mut _,&mut n)};
 serde_json::json!({"mono_ns":mono_ns(),"tcp_info_len":len,"rtt_us":get(68),"rto_us":get(8),"local_total_retrans":get(100),"data_segs_in":get(152),"rcv_ooopack":get(224),"ordered_bytes_received":if rc==0&&len>=136{Some(u64::from_ne_bytes(raw[128..136].try_into().unwrap()))}else{None},"inq_bytes":if q==0{Some(inq)}else{None},"rcvbuf":if br==0{Some(rcvbuf)}else{None},"remote_retrans":"UNAVAILABLE","sack_ranges":"ADMIN_CAPTURE_REQUIRED","zero_window":"UNAVAILABLE"})
}
const OKX_WS_OPERATION_CAPACITY:u32=480;
const R1246_OKX_MAX_BASES_PER_CONNECTION:usize=10;
fn r1248_partition_bases(bases:&[String],group_size:usize)->Result<Vec<Vec<String>>,String>{
 if ![10,20,40].contains(&group_size){return Err("R1248 group size must be 10, 20, or 40".into())}
 Ok(bases.chunks(group_size).map(|group|group.to_vec()).collect())
}
fn r1246_connection_base_groups(venue:Venue,bases:&[String])->Vec<Vec<String>>{
 if venue==Venue::OkxSpot&&bases.len()>R1246_OKX_MAX_BASES_PER_CONNECTION{
  bases.chunks(R1246_OKX_MAX_BASES_PER_CONNECTION).map(|group|group.to_vec()).collect()
 }else if venue==Venue::BinanceFutures{
  let requested=std::env::var("R1248_BINANCE_GROUP_SIZE").ok().and_then(|v|v.parse::<usize>().ok()).filter(|n|[10,20,40].contains(n)).unwrap_or(bases.len().max(1));
  r1248_partition_bases(bases,requested).unwrap_or_else(|_|vec![bases.to_vec()])
 }else{vec![bases.to_vec()]}
}
const OKX_WS_OPERATION_REFILL_NS:u128=7_500_000_000;
#[derive(Debug)]
struct OkxWsOperationBudget{tokens:u32,last_refill:StdInstant}
impl OkxWsOperationBudget{
 fn new(now:StdInstant)->Self{Self{tokens:OKX_WS_OPERATION_CAPACITY,last_refill:now}}
 fn try_take(&mut self,cost:u32,now:StdInstant)->bool{
  if cost==0{return true}if cost>OKX_WS_OPERATION_CAPACITY{return false}
  let ticks=now.saturating_duration_since(self.last_refill).as_nanos()/OKX_WS_OPERATION_REFILL_NS;
  if ticks>0{
   let refill=ticks.min(OKX_WS_OPERATION_CAPACITY as u128) as u32;
   self.tokens=self.tokens.saturating_add(refill).min(OKX_WS_OPERATION_CAPACITY);
   if self.tokens==OKX_WS_OPERATION_CAPACITY{self.last_refill=now}
   else{self.last_refill+=Duration::from_nanos((ticks as u64)*7_500_000_000)}
  }
  if self.tokens<cost{return false}self.tokens-=cost;true
 }
}
async fn reader(cfg:Config,venue:Venue,lane:usize,control_tx:mpsc::Sender<Input>,frame_tx:mpsc::Sender<Input>,stop:Arc<AtomicBool>,close:Arc<Mutex<HashMap<String,String>>>,resubscribe:Arc<Mutex<HashMap<String,Vec<String>>>>,routes:Arc<Mutex<RouteSelector>>,#[cfg(test)] offline_endpoint:Option<R1333OfflineEndpoint>){
 let r1247_trace_enabled=std::env::var("R1247_TRACE").ok().as_deref()==Some("1");
 let r1382_trace_enabled=std::env::var("R1382_TRACE").ok().as_deref()==Some("1");
 let end=Instant::now()+cfg.duration;let mut epoch=0u64;let mut failures=0u64;let started=Instant::now();let mut probe_rotated=false;
 while Instant::now()<end&&!stop.load(Ordering::SeqCst){
  let offline_local={#[cfg(test)]{offline_endpoint.is_some()} #[cfg(not(test))]{false}};
  if venue==Venue::BinanceFutures{if !offline_local{if let Err(e)=permit(end).await{let _=send_control(&control_tx,&stop,Input::Closed(String::new(),e));return}}}
  else if !offline_local{
   let wait={let mut next=OKX_NEXT_NS.lock().expect("OKX quota lock");let now=mono_ns();let slot=now.max(*next);*next=slot+500_000_000;Duration::from_nanos(slot-now)};
   if Instant::now()+wait>=end{return}tokio::time::sleep(wait).await;
  }
  if Instant::now()>=end||stop.load(Ordering::SeqCst){return}
  epoch+=1;let id=session(cfg.shard,venue,lane,epoch);let endpoint_url={#[cfg(test)]{offline_endpoint.as_ref().map(|ep|ep.url_template.replace("{venue}",if venue==Venue::OkxSpot{"okx"}else{"binance"}).replace("{lane}",&lane.to_string())).unwrap_or_else(||url(venue,&cfg.bases,cfg.endpoint_variant))} #[cfg(not(test))]{url(venue,&cfg.bases,cfg.endpoint_variant)}};let mut evidence=SocketEvidence{session:id.clone(),venue:format!("{venue:?}"),url:endpoint_url,connection_epoch:epoch,connects:epoch,..Default::default()};
  let config=tokio_tungstenite::tungstenite::protocol::WebSocketConfig{max_message_size:Some(65_536),max_frame_size:Some(65_536),write_buffer_size:16384,max_write_buffer_size:65536,..Default::default()};
  let trace=Arc::new(Mutex::new(crate::ingress_transport_r1205::Trace::default()));
  let parsed=url::Url::parse(&evidence.url).expect("fixed public endpoint URL");
  let host=parsed.host_str().expect("endpoint host").to_string();let port=parsed.port_or_known_default().unwrap_or(443);
  let mut addresses=match tokio::net::lookup_host((host.as_str(),port)).await{
   Ok(addrs)=>addrs.collect::<Vec<_>>(),
   Err(error)=>{evidence.end=format!("dns_resolve:{error}");let _=send_control(&control_tx,&stop,Input::Socket(evidence));failures+=1;let delay=Duration::from_secs((30*failures).min(60));if Instant::now()+delay>=end{return}tokio::time::sleep(delay).await;continue}
  };
  addresses.sort_unstable();addresses.dedup();addresses.truncate(16);
  evidence.dns_ips=addresses.iter().map(|a|a.to_string()).collect();
  let candidates=routes.lock().map(|mut state|state.order(&host,&addresses,StdInstant::now())).unwrap_or_else(|_|addresses.clone());
  let mut selected_peer=None;let tcp_connect_started_ns=mono_ns();let mut tcp_connected_mono_ns=0u64;let mut tls_ws_handshake_ns=0u64;let mut test_rcvbuf_requested:Option<i32>=None;let mut test_rcvbuf_actual:Option<i32>=None;
  let connected=tokio::time::timeout(Duration::from_secs(15),async{
    let mut tcp=None;let mut last_error=None;
    for address in candidates{
     match tokio::time::timeout(Duration::from_secs(4),tokio::net::TcpStream::connect(address)).await{
      Ok(Ok(stream))=>{selected_peer=Some(address);tcp_connected_mono_ns=mono_ns();#[cfg(test)]{(test_rcvbuf_requested,test_rcvbuf_actual)=r1334_apply_test_rcvbuf(&stream);}tcp=Some(stream);break},
      Ok(Err(error))=>last_error=Some(error),
      Err(_)=>last_error=Some(std::io::Error::new(std::io::ErrorKind::TimedOut,"resolved peer connect timeout")),
     }
    }
    let tcp=tcp.ok_or_else(||tokio_tungstenite::tungstenite::Error::Io(last_error.unwrap_or_else(||std::io::Error::new(std::io::ErrorKind::AddrNotAvailable,"DNS returned no connectable peer"))))?;
    let capture_trace=std::env::var("R1246_DISABLE_INGRESS_TRACE").ok().as_deref()!=Some("1");
    let tcp=crate::ingress_transport_r1205::MeasuredTcp::new_with_capture(tcp,trace.clone(),capture_trace).map_err(tokio_tungstenite::tungstenite::Error::Io)?;
    let tls_started_ns=mono_ns();
    let connector={#[cfg(test)]{offline_endpoint.as_ref().map(|ep|tokio_tungstenite::Connector::NativeTls(ep.connector.clone()))} #[cfg(not(test))]{None}};
    let result=tokio_tungstenite::client_async_tls_with_config(&evidence.url,tcp,Some(config),connector).await;
    tls_ws_handshake_ns=mono_ns().saturating_sub(tls_started_ns);result
  }).await;
  let mut ws=match connected{Ok(Ok((ws,_)))=>ws,other=>{evidence.end=match other{Ok(Err(e))=>format!("connect:{e}"),Err(e)=>format!("connect_timeout:{e}"),_=>"connect_unknown".into()};let banned=venue==Venue::BinanceFutures&&["418","429","451"].iter().any(|code|evidence.end.contains(code));let _=send_control(&control_tx,&stop,Input::Socket(evidence));if banned||stop.load(Ordering::SeqCst){return}failures+=1;let delay=Duration::from_secs((30*failures).min(60));if Instant::now()+delay>=end{return}tokio::time::sleep(delay).await;continue}};
  use std::os::fd::AsRawFd;
  let tcp=match ws.get_ref(){tokio_tungstenite::MaybeTlsStream::Plain(s)=>Some(s),tokio_tungstenite::MaybeTlsStream::NativeTls(s)=>Some(s.get_ref().get_ref().get_ref()),_=>None};
  let fd=tcp.map(|t|t.as_raw_fd()).unwrap_or(-1);
  if let Some(t)=tcp{evidence.local=t.local_addr().map(|a|a.to_string()).unwrap_or_default();evidence.peer=t.peer_addr().map(|a|a.to_string()).unwrap_or_default();}
  let mut r1268_capture_bytes=0u64;
  let mut r1268_capture_writer=if std::env::var("R1268_CAPTURE").ok().as_deref()==Some("YES"){
   let root=std::env::var("R1268_CAPTURE_DIR").map(PathBuf::from).map_err(|e|e.to_string());
   match root.and_then(|root|OpenOptions::new().write(true).create_new(true).open(root.join(format!("{id}.jsonl"))).map_err(|e|e.to_string())){
    Ok(file)=>Some(BufWriter::new(file)),Err(error)=>{eprintln!("R1268_CAPTURE_OPEN_ERROR venue={venue:?} generation={epoch} error={error}");let _=send_control(&control_tx,&stop,Input::Socket(evidence));return}
   }
  }else{None};
  if let Some(w)=r1268_capture_writer.as_mut(){
   let row=serde_json::json!({"kind":"socket_connected","venue":format!("{venue:?}"),"source_id":id,"socket_generation":epoch,"bases":cfg.bases,"url":evidence.url,"local":evidence.local,"peer":evidence.peer,"mono_ns":mono_ns(),"wall_ms":wall_ms(),"subscription_channel":adapter_for(venue).channel()});
   if !r1268_capture_line(w,&mut r1268_capture_bytes,row).unwrap_or(false){eprintln!("R1268_CAPTURE_HEADER_FAILED venue={venue:?} generation={epoch}");return}
  }
  failures=0;evidence.established_ns=mono_ns();evidence.tcp_connect_ns=tcp_connected_mono_ns.saturating_sub(tcp_connect_started_ns);evidence.tls_ws_handshake_ns=tls_ws_handshake_ns;#[cfg(test)]{evidence.test_rcvbuf_requested=test_rcvbuf_requested;evidence.test_rcvbuf_actual=test_rcvbuf_actual;}let born=Instant::now();if !enqueue_reader_input(&frame_tx,Input::Connected(id.clone(),venue,epoch,cfg.bases.clone())).await.0{return}
  let mut subscription_failed=false;
  let mut okx_ops=OkxWsOperationBudget::new(StdInstant::now());
  let mut pending_resubscribe=VecDeque::<String>::new();
  let mut r1247_last_by_instrument=HashMap::<String,(u64,Option<i64>)>::new();
  // OKX sends one acknowledgement per subscribed instrument. The manager's
  // session-level mark_subscribed covers all instruments on this socket.
  let mut subscription_ack_reported=false;
  if let Some(subscribe)=adapter_for(venue).subscription(&cfg.bases){
   if (venue!=Venue::OkxSpot||okx_ops.try_take(1,StdInstant::now()))&&ws.send(Message::Text(subscribe)).await.is_ok(){}else{subscription_failed=true}
  }
  let mut last_tcp=Instant::now();let mut idle=Instant::now();
  let reason='connection: loop{
   if subscription_failed{break "subscription_send_failed".into()}
   if Instant::now()>=end||stop.load(Ordering::SeqCst){break "bounded_end".to_string()}
   if !probe_rotated&&lane==0&&cfg.probe_rotation_after.is_some_and(|d|started.elapsed()>=d){probe_rotated=true;break "PLANNED_ROTATION_PROBE".into()}
   if born.elapsed()>=Duration::from_secs(23*3600+50*60-(lane as u64)*60){break "PLANNED_PRE_24H_ROTATION".into()}
   if let Some(reason)=close.lock().ok().and_then(|mut s|s.remove(&id)){if reason=="QUALIFIED_COHORT_STALE_ROTATION"{if let Some(peer)=selected_peer{if let Ok(mut state)=routes.lock(){state.cool(&host,peer,StdInstant::now());}}}let _=tokio::time::timeout(Duration::from_millis(200),ws.close(None)).await;break reason}
   let pending=resubscribe.lock().ok().and_then(|mut m|m.remove(&id)).unwrap_or_default();
   for base in pending{if !pending_resubscribe.contains(&base){pending_resubscribe.push_back(base);}}
   if venue==Venue::OkxSpot&&!pending_resubscribe.is_empty()&&okx_ops.try_take(2,StdInstant::now()){
    let base=pending_resubscribe.pop_front().expect("nonempty OKX resubscribe queue");
    let arg=serde_json::json!({"channel":adapter_for(venue).channel(),"instId":format!("{base}-USDT")});
    let unsub=serde_json::json!({"op":"unsubscribe","args":[arg]});
    let sub=serde_json::json!({"op":"subscribe","args":[arg]});
    if ws.send(Message::Text(unsub.to_string())).await.is_err()||ws.send(Message::Text(sub.to_string())).await.is_err(){break 'connection "okx_resubscribe_send_failed".into();}
   }
   if last_tcp.elapsed()>=Duration::from_millis(200){if evidence.tcp_samples.len()>=512{evidence.tcp_samples.remove(0);}evidence.tcp_samples.push(sample_tcp(fd));last_tcp=Instant::now();}
   match tokio::time::timeout(Duration::from_millis(100),ws.next()).await{
    Err(_)=>{if venue==Venue::OkxSpot&&idle.elapsed()>Duration::from_secs(15){if ws.send(Message::Text("ping".into())).await.is_err(){break "okx ping".into()}idle=Instant::now();}},
    Ok(Some(Ok(Message::Text(raw))))=>{
     let r1379_e0_enabled=std::env::var("R1379_E0").ok().as_deref()==Some("1");
     let r1279_ws_receive_ns=(r1379_e0_enabled||std::env::var("R1279_TRACE").ok().as_deref()==Some("1")).then(mono_ns);
     let t1_ws_text_mono_ns=r1247_trace_enabled.then(mono_ns);
     let t1_ws_text_wall_ms=r1247_trace_enabled.then(wall_ms);
     idle=Instant::now();
     let r1382_classify_started=r1382_trace_enabled.then(mono_ns);
     let control_kind=adapter_for(venue).classify_control(&raw);
     let now=mono_ns();let wall=wall_ms();
     if let Some(start)=r1382_classify_started{evidence.r1382_reader_classify.record(&control_kind,now.saturating_sub(start));}
     if let Some(w)=r1268_capture_writer.as_mut(){
      let disposition=match control_kind{AdapterControl::Market=>"market",AdapterControl::Heartbeat=>"heartbeat",AdapterControl::SubscribeAck=>"subscribe_ack",AdapterControl::SubscribeError=>"subscribe_error",AdapterControl::Other=>"other_control"};
      let row=serde_json::json!({"kind":"ws_text","venue":format!("{venue:?}"),"source_id":id,"socket_generation":epoch,"received_mono_ns":t1_ws_text_mono_ns.unwrap_or(now),"received_wall_ms":t1_ws_text_wall_ms.unwrap_or(wall),"disposition":disposition,"shape":r1268_wire_shape(&raw),"raw":raw});
      match r1268_capture_line(w,&mut r1268_capture_bytes,row){Ok(true)=>{},Ok(false)=>{eprintln!("R1268_CAPTURE_SOCKET_CAP venue={venue:?} generation={epoch} bytes={r1268_capture_bytes}");break 'connection "R1268_CAPTURE_SOCKET_CAP".into()},Err(error)=>{eprintln!("R1268_CAPTURE_WRITE_ERROR venue={venue:?} generation={epoch} error={error}");break 'connection "R1268_CAPTURE_WRITE_ERROR".into()}}
     }
     if control_kind==AdapterControl::Heartbeat{continue}
     if venue==Venue::OkxSpot&&control_kind!=AdapterControl::Market{
      match control_kind{AdapterControl::SubscribeError=>break format!("subscription:{raw}"),AdapterControl::SubscribeAck=>{if !subscription_ack_reported{subscription_ack_reported=true;if !enqueue_reader_input(&frame_tx,Input::Subscribed(id.clone())).await.0{break "subscription_ack_route_closed".into()}}},_=>{}}
      continue
     }
     let mut r1247_current_event_ts_ms=None;
     let r1247_previous_instrument=if r1247_trace_enabled{
      r1247_current_event_ts_ms=market_event_timestamp_ms(venue,&raw);
      r1247_event_base(venue,&raw,&cfg.bases).and_then(|base|{
       let previous=r1247_last_by_instrument.get(&base).copied();
       if let Some((previous_ns,previous_event))=previous{
        if let Some(t1)=t1_ws_text_mono_ns{
         bounded_push_u64(&mut evidence.r1247_intermessage_gap_ns,&mut evidence.r1247_intermessage_gap_evictions,t1.saturating_sub(previous_ns),4096);
        }
        if let (Some(previous_event),Some(event))=(previous_event,r1247_current_event_ts_ms){
         if event>previous_event{
          bounded_push_i64(&mut evidence.r1247_event_ts_advance_ms,&mut evidence.r1247_event_ts_advance_evictions,event.saturating_sub(previous_event),4096);
         }else{
          evidence.r1247_event_ts_nonadvance=evidence.r1247_event_ts_nonadvance.saturating_add(1);
          if event<previous_event{evidence.r1247_event_ts_backward=evidence.r1247_event_ts_backward.saturating_add(1);}
         }
        }
       }
       r1247_last_by_instrument.insert(base,(t1_ws_text_mono_ns.unwrap_or(now),r1247_current_event_ts_ms));
       previous
      })
     }else{None};
     // Normalize documented raw Futures endpoint into the same combined adapter.
     let raw=if venue==Venue::BinanceFutures&&!raw.contains("\"stream\"")&&cfg.bases.len()==1{serde_json::json!({"stream":format!("{}usdt@depth20@100ms",cfg.bases[0].to_ascii_lowercase()),"data":serde_json::from_str::<serde_json::Value>(&raw).unwrap_or(serde_json::Value::Null)}).to_string()}else{raw};
     let mut source=provenance(id.clone(),epoch);source.frame_trace_id=format!("{}:{}:{}",id,epoch,evidence.received.saturating_add(1));source.transport=trace.lock().ok().and_then(|t|t.stamp(now));
     source.r1279_timing=r1279_ws_receive_ns.map(|received|crate::R1279FrameTiming{clock_domain_id:Some(format!("pid:{}",std::process::id())),ws_receive_ns:Some(received),ws_decode_end_ns:Some(now),..Default::default()});
     if r1379_e0_enabled{if let Some(timing)=source.r1279_timing.as_mut(){timing.instrument=r1247_event_base(venue,&raw,&cfg.bases);}}
     source.r1247_timing=r1247_trace_enabled.then(||R1247FrameTiming{
      t1_ws_text_mono_ns,t1_ws_text_wall_ms,
      t1_previous_instrument_mono_ns:r1247_previous_instrument.map(|p|p.0),
      t1_intermessage_gap_ns:r1247_previous_instrument.and_then(|p|t1_ws_text_mono_ns.map(|t|t.saturating_sub(p.0))),
      previous_exchange_event_ts_ms:r1247_previous_instrument.and_then(|p|p.1),
      exchange_event_timestamp_advanced:r1247_previous_instrument.and_then(|p|p.1.zip(r1247_current_event_ts_ms).map(|(prior,current)|current>prior)),
      ..R1247FrameTiming::default()
     });
     let f=RawFrame{venue,epoch:1,received_ns:now,received_wall_ms:wall,raw,source:Some(source)};
     evidence.received+=1;let event_ms=market_event_timestamp_ms(venue,&f.raw);if let Some(e)=event_ms{let age=t1_ws_text_wall_ms.unwrap_or(wall).saturating_sub(e);if age>800||age<0{evidence.stale+=1}if evidence.age_samples.len()>=if cfg.controlled_close_after.is_some(){20000}else{2048}{evidence.age_samples.remove(0);evidence.age_evictions+=1;}evidence.age_samples.push(age);}
     let queued_before=frame_tx.max_capacity().saturating_sub(frame_tx.capacity());let (queued_ok,queue_wait_ns)=enqueue_reader_input(&frame_tx,Input::Frame(f)).await;bounded_push_u64(&mut evidence.reader_queue_push_ns,&mut evidence.reader_queue_push_evictions,queue_wait_ns,2048);let queued_after=frame_tx.max_capacity().saturating_sub(frame_tx.capacity());evidence.reader_queue_depth_max=evidence.reader_queue_depth_max.max(queued_before).max(queued_after);if !queued_ok{break "reader_frame_route_closed".into()}
    },
    Ok(Some(Ok(Message::Ping(_))))=>{if ws.flush().await.is_err(){break "pong flush".into()}},
    Ok(Some(Ok(Message::Close(_))))|Ok(None)=>break "socket_closed".into(),
    Ok(Some(Err(e)))=>break format!("ws:{e}"),_=>{},
   }
  };
  drop(ws);
  if let Some(mut w)=r1268_capture_writer.take(){if let Err(error)=w.flush(){eprintln!("R1268_CAPTURE_FLUSH_ERROR venue={venue:?} generation={epoch} error={error}")}}
  evidence.transport=trace.lock().map(|t|t.summary()).unwrap_or(serde_json::Value::Null);
  evidence.end=reason.clone();let close_ok=enqueue_reader_input(&frame_tx,Input::Closed(id.clone(),reason.clone())).await.0;let evidence_ok=send_control(&control_tx,&stop,Input::Socket(evidence));if !close_ok||!evidence_ok{return}
  // Planned rotations replenish HOT capacity promptly; rate permits still
  // apply on every new connection. Unexpected failures keep exponential backoff.
  let planned=is_planned_reconnect(&reason);
  if planned{failures=0}else{failures+=1;}
  let delay=reconnect_delay(&reason,failures);
  if Instant::now()+delay>=end{return}tokio::time::sleep(delay).await;
 }
}
pub async fn run(cfg:Config,store:SharedStore,authority:Authority,out:mpsc::Sender<Output>)->Result<Summary,String>{
 let store_worker=StoreWorker::start(store.clone())?;
 run_with_store_worker(cfg,store,store_worker,authority,out).await
}
pub async fn run_with_store_worker(cfg:Config,store:SharedStore,store_worker:StoreWorker,authority:Authority,out:mpsc::Sender<Output>)->Result<Summary,String>{
 run_internal(cfg,Some(store),Some(store_worker),authority,out).await
}
pub async fn run_r1246_volatile(cfg:Config,authority:Authority,out:mpsc::Sender<Output>)->Result<Summary,String>{
 run_internal(cfg,None,None,authority,out).await
}
async fn run_internal(cfg:Config,store:Option<SharedStore>,store_worker:Option<StoreWorker>,authority:Authority,out:mpsc::Sender<Output>)->Result<Summary,String>{
 // Explicit opt-in in the existing RAM-only experimental runtime.
 // No automatic public run or durable/PAPER path cutover.
 let worker_requested=std::env::var("R1402_UCM_WORKER").ok().as_deref()==Some("1");
 if worker_requested&&(store.is_some()||store_worker.is_some()){return Err("R1402 UCM worker is restricted to volatile quote runtime".into())}
 #[cfg(test)]{return run_internal_impl(cfg,store,store_worker,authority,out,worker_requested,None).await}
 #[cfg(not(test))]{run_internal_impl(cfg,store,store_worker,authority,out,worker_requested).await}
}
#[cfg(test)]
async fn run_internal_offline(cfg:Config,authority:Authority,out:mpsc::Sender<Output>,endpoint:R1333OfflineEndpoint)->Result<Summary,String>{run_internal_impl(cfg,None,None,authority,out,false,Some(endpoint)).await}
async fn run_internal_impl(cfg:Config,store:Option<SharedStore>,store_worker:Option<StoreWorker>,authority:Authority,out:mpsc::Sender<Output>,worker_requested:bool,#[cfg(test)] offline_endpoint:Option<R1333OfflineEndpoint>)->Result<Summary,String>{
 let volatile=store.is_none()&&store_worker.is_none();
 let max_bases=if volatile{233}else{R1205_MAX_BASES_PER_SHARD};
 let copy_bounds=if volatile{(1..=3).contains(&cfg.copies)}else{(2..=3).contains(&cfg.copies)};
 if cfg.endpoint_variant!=0||cfg.bases.is_empty()||cfg.bases.len()>max_bases||!copy_bounds||cfg.duration>Duration::from_secs(24*3600){return Err("R1205 driver bounds".into())}
 if cfg.probe_rotation_after.is_some_and(|t|cfg.controlled_close_after.is_none()||t<Duration::from_secs(60)||t>=cfg.duration){return Err("R1206 planned probe bounds".into())}
 let keys=cfg.bases.iter().flat_map(|b|[key(Venue::BinanceFutures,b),key(Venue::OkxSpot,b)]).collect();
 let mut engine=if volatile{Engine::new_volatile(authority,keys)?}else{Engine::new_with_store_worker(store.ok_or("R1205 store missing")?,store_worker.clone().ok_or("R1205 worker missing")?,authority,keys)?};
 let ucm_notify=if worker_requested{
  if !volatile{return Err("R1402 worker requires volatile ingress".into())}
  Some(engine.enable_ucm_worker(CENTRAL_FRAME_QUEUE_CAPACITY)?)
 }else{None};
 if cfg.controlled_close_after.is_some(){engine.enable_probe_audit();}
 let (tx,mut rx)=mpsc::channel(CENTRAL_FRAME_QUEUE_CAPACITY);let r1273_enabled=std::env::var("R1273_PROFILE").ok().as_deref()==Some("1");let r1273_central=Arc::new(Mutex::new(R1273CentralProfile::default()));let r1273_depth=Arc::new(AtomicUsize::new(0));let (control_tx,mut control_rx)=mpsc::channel::<Input>(R1205_CONTROL_QUEUE_CAPACITY);let stop=Arc::new(AtomicBool::new(false));let close=Arc::new(Mutex::new(HashMap::new()));let resubscribe=Arc::new(Mutex::new(HashMap::<String,Vec<String>>::new()));let routes=Arc::new(Mutex::new(RouteSelector::default()));let end=Instant::now()+cfg.duration;let started=Instant::now();let diagnostic_start_ns=mono_ns();let mut r1272_by_second=std::collections::BTreeMap::<u64,R1272SecondStats>::new();let mut tasks=Vec::new();let mut cleanup=ReaderCleanup{stop:stop.clone(),handles:Vec::new()};
 for v in [Venue::BinanceFutures,Venue::OkxSpot]{
  for (group_index,group_bases) in r1246_connection_base_groups(v,&cfg.bases).into_iter().enumerate(){
   for copy_index in 0..cfg.copies{
    let lane=group_index*cfg.copies+copy_index;
    let mut reader_cfg=cfg.clone();reader_cfg.bases=group_bases.clone();
    let(frame_tx,mut frame_rx)=mpsc::channel::<Input>(READER_FRAME_QUEUE_CAPACITY);
    let central=tx.clone();let profile=r1273_central.clone();let depth=r1273_depth.clone();let profile_enabled=r1273_enabled;
    let forwarder=tokio::spawn(async move{let mut forwarder_processor=crate::market_data_processor_r1378::MarketDataProcessor::default();while let Some(mut item)=frame_rx.recv().await{if let Input::Frame(frame)=&mut item{let dequeue_ns=mono_ns();if let Some(t)=frame.source.as_mut().and_then(|s|s.r1279_timing.as_mut()){t.reader_dequeue_ns=Some(dequeue_ns);}}let wait_started=Instant::now();let result=central.reserve().await;let wait_ns=wait_started.elapsed().as_nanos().min(u64::MAX as u128)as u64;match result{Ok(permit)=>{let queued=central.max_capacity().saturating_sub(central.capacity());if profile_enabled{depth.fetch_add(1,Ordering::SeqCst);}if let Input::Frame(frame)=&mut item{if let Some(t)=frame.source.as_mut().and_then(|s|s.r1279_timing.as_mut()){t.central_permit_acquired_ns=Some(mono_ns());t.central_queue_capacity=Some(CENTRAL_FRAME_QUEUE_CAPACITY);t.central_queue_depth=Some(queued);}}
 // R1388: reconstruct both exchange protocols in the existing socket Forwarder,
    // after acquiring original bounded central capacity. Never run L2 on WS Reader.
    if let Input::Frame(frame)=&mut item{
     let prepared=tokio::task::block_in_place(||forwarder_processor.prepare_forwarded(frame));
     if let Some(source)=frame.source.as_mut(){source.prepared=Some(Box::new(prepared));}
    }else if let Input::Closed(session,_)=&item{
     forwarder_processor.clear_session(session);
    }
    // The timestamp now marks actual enqueue AFTER Forwarder preparation.
    // The same 32-slot permit continues bounding memory and maintaining FIFO.
    if let Input::Frame(frame)=&mut item{
     if let Some(t)=frame.source.as_mut().and_then(|s|s.r1279_timing.as_mut()){
      t.central_enqueue_ns=Some(mono_ns());
     }
    }
    permit.send(item);if profile_enabled{if let Ok(mut p)=profile.lock(){p.enqueue(queued,wait_ns);}}},Err(_)=>break}}});
    cleanup.handles.push(forwarder.abort_handle());tasks.push(forwarder);
    #[cfg(test)] let task=tokio::spawn(reader(reader_cfg,v,lane,control_tx.clone(),frame_tx,stop.clone(),close.clone(),resubscribe.clone(),routes.clone(),offline_endpoint.clone()));
    #[cfg(not(test))] let task=tokio::spawn(reader(reader_cfg,v,lane,control_tx.clone(),frame_tx,stop.clone(),close.clone(),resubscribe.clone(),routes.clone()));
    cleanup.handles.push(task.abort_handle());tasks.push(task);
   }
  }
 }
 drop(tx);
 let test_engine_delay_ms={#[cfg(test)]{std::env::var("R1333_TEST_ENGINE_DELAY_MS").ok().and_then(|v|v.parse::<u64>().ok()).unwrap_or(0)} #[cfg(not(test))]{0u64}};
 let mut summary=Summary::default();summary.synthetic_latency=test_engine_delay_ms>0;summary.reader_frame_queue_capacity_per_session=READER_FRAME_QUEUE_CAPACITY;summary.central_frame_queue_capacity=CENTRAL_FRAME_QUEUE_CAPACITY;let coverage_end=mono_ns()+cfg.duration.as_nanos()as u64;observe_coverage(&mut summary.coverage_time,engine.coverage(mono_ns(),wall_ms()),mono_ns());let mut tick=tokio::time::interval(Duration::from_millis(50));let mut coverage=Instant::now();let mut connected=HashMap::<String,(Venue,u64,Vec<String>)>::new();let mut degraded_scope_since=HashMap::<(String,String),u64>::new();let mut last_quality_rotation=HashMap::<String,u64>::new();
 // R1351: diagnostic coverage sampling is not a market-data admission gate.
 let mut last_coverage_observation=Instant::now()-Duration::from_millis(250);
 let r1379_e0_enabled=std::env::var("R1379_E0").ok().as_deref()==Some("1");
 let mut r1379_e0_queue=E0QueuePhases::default();
 let mut closed_control=false;let mut closed_okx=false;let mut first_control_ns=None;let mut deferred_input:Option<Input>=None;
 loop{
  let outputs=tokio::select!{
   _=async{if let Some(n)=&ucm_notify{n.notified().await}else{std::future::pending::<()>().await}}=>{
    engine.poll_ucm_worker(mono_ns(),wall_ms())?
   },
   _=tick.tick()=>{
    if coverage.elapsed()>=Duration::from_secs(5){engine.request_ucm_census()?;if summary.coverage.len()>=1200{summary.coverage.remove(0);}summary.coverage.push(serde_json::json!({"mono_ns":mono_ns(),"rows":engine.coverage(mono_ns(),wall_ms())}));summary.events.push(serde_json::json!({"kind":"r1326p_memory_structure","mono_ns":mono_ns(),"engine":engine.memory_structure_counts_r1326p(),"central_queue_depth":r1273_depth.load(Ordering::Relaxed)}));coverage=Instant::now();}
    if !closed_control&&cfg.controlled_close_after.is_some_and(|d|started.elapsed()>=d){
     if let Some(row)=engine.coverage(mono_ns(),wall_ms()).as_array().and_then(|a|a.iter().find(|r|r["key"].as_str().is_some_and(|k|k.starts_with("BinanceFutures"))&&r["active"].is_string()&&r["hot_ready"].as_array().is_some_and(|a|a.len()>=3))){
      if let Some(id)=row["active"].as_str().filter(|_|engine.coverage(mono_ns(),wall_ms()).as_array().is_some_and(|rows|rows.iter().filter(|r|r["key"].as_str().is_some_and(|k|k.starts_with("BinanceFutures"))).all(|r|r["hot_ready"].as_array().is_some_and(|a|a.len()>=3)))){close.lock().map_err(|_|"close poison")?.insert(id.into(),"CONTROLLED_ACTIVE_CLOSE".into());summary.events.push(serde_json::json!({"kind":"close_requested","mono_ns":mono_ns(),"session":id,"hot_before":row,"coverage_before":engine.coverage(mono_ns(),wall_ms())}));closed_control=true;first_control_ns=Some(mono_ns());}
     }
    }
    if closed_control&&!closed_okx&&first_control_ns.is_some_and(|t|mono_ns().saturating_sub(t)>=25_000_000_000){
     if let Some(row)=engine.coverage(mono_ns(),wall_ms()).as_array().and_then(|a|a.iter().find(|r|r["key"].as_str().is_some_and(|k|k.starts_with("OkxSpot"))&&r["active"].is_string()&&r["hot_ready"].as_array().is_some_and(|a|a.len()>=3))){
      if let Some(id)=row["active"].as_str().filter(|_|engine.coverage(mono_ns(),wall_ms()).as_array().is_some_and(|rows|rows.iter().filter(|r|r["key"].as_str().is_some_and(|k|k.starts_with("OkxSpot"))).all(|r|r["hot_ready"].as_array().is_some_and(|a|a.len()>=3)))){close.lock().map_err(|_|"close poison")?.insert(id.into(),"CONTROLLED_ACTIVE_CLOSE".into());summary.events.push(serde_json::json!({"kind":"close_requested","mono_ns":mono_ns(),"session":id,"hot_before":row,"coverage_before":engine.coverage(mono_ns(),wall_ms())}));closed_okx=true;}
     }
    }
    let now=mono_ns();let wall=wall_ms();
    for (id,(venue,born,bases)) in &connected{
     if now.saturating_sub(*born)<10_000_000_000||!engine.ucm_quality_observable(id,*venue,bases){continue}
     let ready_scopes=bases.iter().map(|base|engine.hot_complete(id,*venue,std::slice::from_ref(base),now,wall)).collect::<Vec<_>>();
     let bad=ready_scopes.iter().filter(|ready|!**ready).count();
     let sustained_stale=stale_scope_dwell_elapsed(&mut degraded_scope_since,id,bases,&ready_scopes,now);
     let mut corroborating=0usize;
     for(other,(other_venue,_,other_bases))in &connected{
      if other!=id&&other_venue==venue&&other_bases==bases&&engine.ucm_quality_observable(other,*other_venue,bases)&&engine.hot_complete(other,*other_venue,bases,now,wall){corroborating+=1;}
     }
     if sustained_stale&&quality_rotation_allowed(bad,bases.len(),corroborating){
      let group=quality_rotation_group_key(id);let last=last_quality_rotation.get(&group).copied();
      if quality_rotation_cooldown_elapsed(last,now){
       close.lock().map_err(|_|"close poison")?.insert(id.clone(),"QUALIFIED_COHORT_STALE_ROTATION".into());last_quality_rotation.insert(group.clone(),now);
       summary.events.push(serde_json::json!({"kind":"quality_rotation","session":id,"group":group,"mono_ns":now,"bad_scopes":bad,"fresh_peer_cohorts":corroborating,"scope_bases":bases}));
      }
     }
    }
    tokio::task::block_in_place(||engine.tick(mono_ns(),wall_ms()))?
   },
   incoming=async{if let Some(item)=deferred_input.take(){Some(item)}else{tokio::select!{biased;control=control_rx.recv()=>control,frame=rx.recv()=>frame}}}=>match incoming{
    None=>break,
    Some(Input::Connected(id,venue,generation,bases))=>{if r1273_enabled{let depth=r1273_depth.fetch_sub(1,Ordering::SeqCst).saturating_sub(1);if let Ok(mut p)=r1273_central.lock(){p.dequeue(depth);}summary.central_queue_depth_max=summary.central_queue_depth_max.max(depth);if summary.central_queue_depth_samples.len()>=8192{summary.central_queue_depth_samples.remove(0);summary.central_queue_depth_evictions+=1;}summary.central_queue_depth_samples.push(depth);}engine.register_connection_scoped(&id,venue,generation,&bases)?;connected.insert(id.clone(),(venue,mono_ns(),bases.clone()));summary.events.push(serde_json::json!({"kind":"connected","mono_ns":mono_ns(),"session":id,"venue":format!("{venue:?}"),"generation":generation,"scope_bases":bases,"subscribed":venue==Venue::BinanceFutures}));Vec::new()},
    Some(Input::Subscribed(id))=>{if r1273_enabled{let depth=r1273_depth.fetch_sub(1,Ordering::SeqCst).saturating_sub(1);if let Ok(mut p)=r1273_central.lock(){p.dequeue(depth);}summary.central_queue_depth_max=summary.central_queue_depth_max.max(depth);if summary.central_queue_depth_samples.len()>=8192{summary.central_queue_depth_samples.remove(0);summary.central_queue_depth_evictions+=1;}summary.central_queue_depth_samples.push(depth);}engine.mark_subscribed(&id);summary.events.push(serde_json::json!({"kind":"subscribed","mono_ns":mono_ns(),"session":id}));Vec::new()},
    Some(Input::Closed(id,why))=>{if !id.is_empty()&&r1273_enabled{let depth=r1273_depth.fetch_sub(1,Ordering::SeqCst).saturating_sub(1);if let Ok(mut p)=r1273_central.lock(){p.dequeue(depth);}summary.central_queue_depth_max=summary.central_queue_depth_max.max(depth);if summary.central_queue_depth_samples.len()>=8192{summary.central_queue_depth_samples.remove(0);summary.central_queue_depth_evictions+=1;}summary.central_queue_depth_samples.push(depth);}if why=="RAW_RECEIVED_NOT_ACCEPTED_READER_QUEUE_GAP"{summary.reader_queue_overflow_events+=1;}if !matches!(why.as_str(),"CONTROLLED_ACTIVE_CLOSE"|"PLANNED_PRE_24H_ROTATION"|"PLANNED_ROTATION_PROBE"|"QUALIFIED_COHORT_STALE_ROTATION"|"bounded_end"){engine.mark_reconnecting(&id);}connected.remove(&id);degraded_scope_since.retain(|(session,_),_|session!=&id);summary.events.push(serde_json::json!({"kind":"closed","mono_ns":mono_ns(),"session":id,"reason":why,"coverage_before":engine.coverage(mono_ns(),wall_ms())}));tokio::task::block_in_place(||engine.disconnected(&id,mono_ns(),wall_ms()))?},
    Some(Input::Socket(e))=>{summary.reader_received_total+=e.received;summary.peak_reader_frame_queue_depth=summary.peak_reader_frame_queue_depth.max(e.reader_queue_depth_max);if summary.sockets.len()>=64{summary.sockets.remove(0);summary.socket_history_evictions+=1;}summary.sockets.push(e);Vec::new()},
    Some(Input::Frame(mut f))=>{
     let dequeue_ns=mono_ns();if let Some(t)=f.source.as_mut().and_then(|s|s.r1279_timing.as_mut()){t.central_dequeue_ns=Some(dequeue_ns);}
     // Drain the already bounded ingress queue immediately. The previous fixed 3 ms
     // batching delay added avoidable receive-to-engine latency under every load level.
      // Keep controls behind frames already accepted from the bounded queue.
      // In particular, disconnection cannot invalidate earlier frames in this batch.
       let central_depth_at_dequeue=if r1273_enabled{r1273_depth.fetch_sub(1,Ordering::SeqCst).saturating_sub(1)}else{0};if r1273_enabled{if let Ok(mut p)=r1273_central.lock(){p.dequeue(central_depth_at_dequeue);}summary.central_queue_depth_max=summary.central_queue_depth_max.max(central_depth_at_dequeue);if summary.central_queue_depth_samples.len()>=8192{summary.central_queue_depth_samples.remove(0);summary.central_queue_depth_evictions+=1;}summary.central_queue_depth_samples.push(central_depth_at_dequeue);}
       let (mut batch,deferred)=drain_frame_batch_profiled(&mut rx,f,if r1273_enabled{Some(&r1273_depth)}else{None},if r1273_enabled{Some(&r1273_central)}else{None});
      if deferred_input.is_some(){return Err("R1216 deferred input collision".into())}
      deferred_input=deferred;
     let engine_start_ns=mono_ns();for frame in &mut batch{if let Some(t)=frame.source.as_mut().and_then(|s|s.r1279_timing.as_mut()){t.engine_receive_begin_ns=Some(engine_start_ns);}}
     let second=engine_start_ns.saturating_sub(diagnostic_start_ns)/1_000_000_000;
     let bucket=r1272_by_second.entry(second).or_default();
     bucket.batches=bucket.batches.saturating_add(1);
     bucket.frames=bucket.frames.saturating_add(batch.len() as u64);
     bucket.batch_frames_max=bucket.batch_frames_max.max(batch.len() as u64);
     for frame in &batch{
      if r1379_e0_enabled{r1379_e0_queue.observe(frame,engine_start_ns);}
      let residence=frame_queue_residence_ns(frame,engine_start_ns);
      if let Some(ns)=residence{
       bounded_push_u64(&mut summary.engine_queue_age_samples_ns,&mut summary.engine_queue_age_evictions,ns,4096);
       bucket.queue_residence_frames=bucket.queue_residence_frames.saturating_add(1);
       bucket.queue_residence_ns_total+=ns as u128;
       bucket.queue_residence_ns_max=bucket.queue_residence_ns_max.max(ns);
      }else{
       bucket.receive_fallback_frames=bucket.receive_fallback_frames.saturating_add(1);
      }
      bounded_push_u64(&mut summary.engine_receive_to_start_samples_ns,&mut summary.engine_receive_to_start_evictions,engine_start_ns.saturating_sub(frame.received_ns),4096);
      if let Some(wait)=frame.source.as_ref().and_then(|source|source.r1247_timing.as_ref()).and_then(|timing|timing.t3_queue_wait_ns){
       bucket.reader_send_wait_ns_max=bucket.reader_send_wait_ns_max.max(wait);
       if wait>10_000_000{bucket.reader_send_wait_over_10ms=bucket.reader_send_wait_over_10ms.saturating_add(1);}
      }
     }
     let service_started=Instant::now();
     let batch_result=tokio::task::block_in_place(||{if test_engine_delay_ms>0{std::thread::sleep(Duration::from_millis(test_engine_delay_ms));}engine.receive_batch(batch,engine_start_ns,wall_ms())});
     let service_ns=service_started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
     bucket.service_ns_total+=service_ns as u128;bucket.service_ns_max=bucket.service_ns_max.max(service_ns);
     bounded_push_u64(&mut summary.engine_batch_service_samples_ns,&mut summary.engine_batch_service_evictions,service_ns,4096);
     batch_result?
    }
   }
  };
  engine.check_ucm_owner()?;
  for output in &outputs{match output{Output::Reset{venue,base,epoch}=>summary.events.push(serde_json::json!({"kind":"selection","mono_ns":mono_ns(),"venue":format!("{venue:?}"),"base":base,"epoch":epoch,"coverage":engine.coverage(mono_ns(),wall_ms()),"qualification":engine.qualification_proof(*venue,base,mono_ns(),wall_ms())})),Output::Resubscribe{session,base,reason}=>{resubscribe.lock().map_err(|_|"resubscribe control poison")?.entry(session.clone()).or_default().push(base.clone());summary.events.push(serde_json::json!({"kind":"okx_symbol_resubscribe_requested","mono_ns":mono_ns(),"session":session,"base":base,"reason":reason}));},_=>{}}}
  // R1351: avoid full-universe diagnostic scan after every ingress iteration.
  if last_coverage_observation.elapsed()>=Duration::from_millis(250){
   observe_coverage(&mut summary.coverage_time,engine.coverage(mono_ns(),wall_ms()),mono_ns().min(coverage_end));
   last_coverage_observation=Instant::now();
  }
  if summary.events.len()>4096{summary.events.drain(..1024);summary.diagnostic_event_evictions+=1024;}
  for mut output in outputs {
   if matches!(output,Output::Resubscribe{..}){continue}
   let send_started=Instant::now();
   // Reserve the same bounded capacity with the same 250ms timeout. Timestamp
   // only AFTER capacity exists, so downstream residence excludes producer wait.
   let permit=tokio::time::timeout(Duration::from_millis(250),out.reserve()).await;
   bounded_push_u64(&mut summary.output_send_wait_samples_ns,&mut summary.output_send_wait_evictions,send_started.elapsed().as_nanos()as u64,4096);
   match permit {
    Ok(Ok(permit))=>{
     if let Output::Frame(frame)|Output::Book(frame,_)=&mut output {
      if let Some(source)=frame.source.as_mut(){let timing=source.r1279_timing.get_or_insert_with(Default::default);timing.r1325_output_enqueued_ns=Some(mono_ns());timing.r1324a_output_occupied_slots=Some(out.max_capacity()-out.capacity());}
     }
     permit.send(output);
    },
    _=>{stop.store(true,Ordering::SeqCst);return Err("R1205 accepted pending downstream timeout".into())}
   }
  }
  if stop.load(Ordering::SeqCst){return Err("R1205 raw intake GAP".into())}
  if Instant::now()>end+Duration::from_secs(2){break}
 }
 stop.store(true,Ordering::SeqCst);for task in tasks{let _=tokio::time::timeout(Duration::from_secs(2),task).await;}
 observe_coverage(&mut summary.coverage_time,engine.coverage(mono_ns(),wall_ms()),mono_ns().min(coverage_end));
 if let Some(worker)=store_worker.as_ref(){
  let final_counts=tokio::task::block_in_place(||worker.call(|s,_|s.counts()))?;
  summary.counts=serde_json::to_value(final_counts).map_err(|e|e.to_string())?;
  summary.durable_writer=worker.profile();
 }else{
  summary.counts=serde_json::json!({"volatile":true,"sqlite_ingress_writes":0,"frames_received":engine.metrics.received});
  summary.durable_writer=engine.durable_writer_profile();
 }
 summary.r1273_engine_profile=engine.r1273_profile.lock().map_err(|_|"engine profile poison")?.snapshot();summary.r1273_central_queue=serde_json::to_value(&*r1273_central.lock().map_err(|_|"central profile poison")?).map_err(|e|e.to_string())?;
 summary.r1379_e0_queue_phases=if r1379_e0_enabled{r1379_e0_queue.snapshot()}else{serde_json::Value::Null};
 summary.r1272_batch_second_metrics=r1272_by_second.iter().map(|(second,stats)|stats.value(*second)).collect();summary.engine_queue_age_sample_origin_counts=serde_json::json!({"enqueue_timestamp":"R1247 t3_enqueue_mono_ns","receive_to_start_timestamp":"RawFrame.received_ns","queue_age_capacity":4096,"receive_to_start_capacity":4096});summary.r1279_engine_terminal_trace_samples=engine.r1279_frame_trace_samples();summary.r1279_engine_trace_dropped=engine.r1279_trace_dropped();summary.r1279_engine_trace_sampled_out=engine.r1279_trace_sampled_out();summary.source_audit=engine.audit();summary.invalid_frame_audit=engine.invalid_audit();summary.stale_frame_audit=engine.stale_frame_audit();summary.okx_books_invalid_audit=engine.okx_books_invalid_audit();summary.selection_audit=engine.selection_audit();summary.readiness_audit=engine.readiness_audit();
 summary.r1247_frame_stage_samples=engine.r1247_frame_stage_samples();summary.r1247_frame_stage_evictions=engine.r1247_frame_stage_evictions();summary.r1247_selection_evaluations=engine.r1247_selection_evaluations();summary.r1247_selection_evaluation_evictions=engine.r1247_selection_evaluation_evictions();
 summary.engine=serde_json::to_value(&engine.metrics).map_err(|e|e.to_string())?;
 if let Some(row)=summary.engine.as_object_mut(){row.insert("r1402_ucm_worker".into(),serde_json::json!(worker_requested));}
 Ok(summary)
}
#[cfg(test)]mod okx_ws_operation_budget_tests{
 use super::*;
 #[test]fn r1246_initial_subscription_and_resubscribe_share_documented_per_connection_budget(){
  let start=StdInstant::now();let mut budget=OkxWsOperationBudget::new(start);
  assert!(budget.try_take(1,start),"initial subscribe counts as one operation");
  assert!(budget.try_take(478,start));
  assert!(!budget.try_take(2,start),"unsubscribe plus subscribe require two available operations");
  assert!(budget.try_take(1,start+Duration::from_millis(7_500)));
  assert!(!budget.try_take(2,start+Duration::from_millis(7_500)));
  assert!(budget.try_take(1,start+Duration::from_millis(15_000)));
  assert!(budget.try_take(1,start+Duration::from_secs(3600)));
  assert!(!budget.try_take(481,start+Duration::from_secs(3600)));
 }
}
#[cfg(test)]mod r1246_connection_partition_tests{
 use super::*;
 #[test]fn r1246_splits_okx_400_level_subscriptions_below_documented_limit_only(){
  let bases=(0..40).map(|index|format!("BASE{index:02}")).collect::<Vec<_>>();
  let okx=r1246_connection_base_groups(Venue::OkxSpot,&bases);
  assert_eq!(okx.len(),4);
  assert_eq!(okx.iter().map(Vec::len).collect::<Vec<_>>(),vec![10,10,10,10]);
  assert_eq!(okx.concat(),bases);
  let binance=r1246_connection_base_groups(Venue::BinanceFutures,&bases);
  assert_eq!(binance,vec![bases.clone()]);
  let small=r1246_connection_base_groups(Venue::OkxSpot,&bases[..8]);
  assert_eq!(small,vec![bases[..8].to_vec()]);
 }
}
#[cfg(test)]mod r1246_timestamp_probe_tests{
 use super::*;
 #[test]fn parses_exchange_timestamp_without_decoding_book(){
  assert_eq!(market_event_timestamp_ms(Venue::OkxSpot,r#"{"arg":{"channel":"books5"},"data":[{"ts":"1762520000123","bids":[],"asks":[]}]}"#),Some(1762520000123));
  assert_eq!(market_event_timestamp_ms(Venue::BinanceFutures,r#"{"stream":"btcusdt@depth5@500ms","data":{"E":1762520000456,"T":1762520000400}}"#),Some(1762520000456));
  assert_eq!(market_event_timestamp_ms(Venue::BinanceFutures,r#"{"data":{"T":1762520000400}}"#),Some(1762520000400));
  assert_eq!(market_event_timestamp_ms(Venue::OkxSpot,r#"{"data":[{"bids":[],"asks":[]}]}"#),None);
 }
}
#[cfg(test)]mod r1208_quality_rotation_tests{
 use super::*;
 #[test]fn physical_rotation_helper_waits_for_all_scopes(){assert!(!UniversalConnectionManager::should_rotate_physical(1,2));assert!(UniversalConnectionManager::should_rotate_physical(2,2));assert!(UniversalConnectionManager::should_rotate_physical(1,1));assert!(!UniversalConnectionManager::should_rotate_physical(0,2));}
 #[test]fn rotation_threshold_preserves_hot_scopes_and_repairs_fully_stale_sockets(){assert!(quality_rotation_allowed(2,2,1));assert!(quality_rotation_allowed(2,2,2));assert!(!quality_rotation_allowed(2,2,0));assert!(!quality_rotation_allowed(1,2,1));assert!(quality_rotation_allowed(1,2,2));assert!(!quality_rotation_allowed(0,2,4));assert!(!quality_rotation_allowed(1,0,3));}
 #[test]fn stale_scope_dwell_tracks_each_scope_independently(){let mut timers=HashMap::new();let bases=vec!["BTC".to_owned(),"ETH".to_owned()];assert!(!stale_scope_dwell_elapsed(&mut timers,"lane-1",&bases,&[false,true],1_000_000_000));assert!(!stale_scope_dwell_elapsed(&mut timers,"lane-1",&bases,&[false,true],3_999_999_999));assert!(stale_scope_dwell_elapsed(&mut timers,"lane-1",&bases,&[false,true],4_000_000_000));}
 #[test]fn fresh_scope_resets_only_its_own_stale_dwell(){let mut timers=HashMap::new();let bases=vec!["BTC".to_owned(),"ETH".to_owned()];assert!(!stale_scope_dwell_elapsed(&mut timers,"lane-1",&bases,&[false,true],1_000_000_000));assert!(!stale_scope_dwell_elapsed(&mut timers,"lane-1",&bases,&[true,true],2_000_000_000));assert!(!stale_scope_dwell_elapsed(&mut timers,"lane-1",&bases,&[false,true],3_500_000_000));assert!(stale_scope_dwell_elapsed(&mut timers,"lane-1",&bases,&[false,true],6_500_000_000));}
 #[test]fn cooldown_is_isolated_by_shard_venue_and_lane_but_shared_across_lane_generations(){let lane0=quality_rotation_group_key("g0-BinanceFutures-0-1-a");assert_eq!(lane0,"g0-BinanceFutures-0");assert_eq!(quality_rotation_group_key("g0-BinanceFutures-0-2-b"),lane0);assert_ne!(lane0,quality_rotation_group_key("g0-BinanceFutures-1-1-c"));assert_ne!(lane0,quality_rotation_group_key("g0-OkxSpot-0-1-d"));assert_ne!(lane0,quality_rotation_group_key("g1-BinanceFutures-0-1-e"));}
 #[test]fn rotation_cooldown_is_30_seconds_and_uses_monotonic_time(){let at=10_000;assert!(quality_rotation_cooldown_elapsed(None,at));assert!(!quality_rotation_cooldown_elapsed(Some(at),at+29_999_999_999));assert!(quality_rotation_cooldown_elapsed(Some(at),at+30_000_000_000));}
 #[test]fn qualified_stale_rotation_reconnects_without_failure_backoff(){let reason="QUALIFIED_COHORT_STALE_ROTATION";assert!(is_planned_reconnect(reason));assert_eq!(reconnect_delay(reason,10),Duration::from_secs(1));assert_eq!(reconnect_delay("socket_closed",1),Duration::from_secs(30));assert_eq!(reconnect_delay("socket_closed",10),Duration::from_secs(60));}
 #[test]fn stale_route_is_bypassed_then_restored_after_bounded_cooldown(){let now=StdInstant::now();let host="fstream.binance.com";let a:SocketAddr="192.0.2.1:443".parse().unwrap();let b:SocketAddr="192.0.2.2:443".parse().unwrap();let mut selector=RouteSelector::default();let first=selector.order(host,&[a,b],now);assert_eq!(first.len(),2);selector.cool(host,a,now);let bypass=selector.order(host,&[a,b],now);assert_eq!(bypass[0],b);assert_eq!(bypass[1],a);let recovered=selector.order(host,&[a,b],now+STALE_ROUTE_COOLDOWN+Duration::from_millis(1));assert!(recovered.contains(&a));assert_eq!(recovered.len(),2);}
 #[test]fn all_cooled_routes_fall_back_to_peer_with_earliest_expiry(){let now=StdInstant::now();let host="ws.okx.com";let a:SocketAddr="192.0.2.11:443".parse().unwrap();let b:SocketAddr="192.0.2.12:443".parse().unwrap();let mut selector=RouteSelector::default();selector.cool(host,a,now);selector.cool(host,b,now+Duration::from_secs(10));let ordered=selector.order(host,&[a,b],now+Duration::from_secs(20));assert_eq!(ordered[0],a);assert_eq!(ordered.len(),2);}
}

#[cfg(test)]mod r1216_frame_batch_order_tests{
 use super::*;
 fn frame(received_ns:u64)->RawFrame{RawFrame{venue:Venue::BinanceFutures,epoch:1,received_ns,received_wall_ms:1,raw:"{}".into(),source:None}}
 #[tokio::test]
 async fn frame_batch_defers_close_until_prior_frames_are_returned(){
  let(tx,mut rx)=mpsc::channel(4);
  tx.send(Input::Frame(frame(2))).await.unwrap();
  tx.send(Input::Closed("session-a".into(),"socket_closed".into())).await.unwrap();
  let(batch,deferred)=drain_frame_batch(&mut rx,frame(1));
  assert_eq!(batch.len(),2);
  assert_eq!(batch[0].received_ns,1);
  assert_eq!(batch[1].received_ns,2);
  assert!(matches!(deferred,Some(Input::Closed(id,reason)) if id=="session-a"&&reason=="socket_closed"));
 }
 #[tokio::test]
 async fn frame_batch_keeps_inputs_after_deferred_control_queued(){
  let(tx,mut rx)=mpsc::channel(4);
  tx.send(Input::Closed("session-a".into(),"socket_closed".into())).await.unwrap();
  tx.send(Input::Frame(frame(3))).await.unwrap();
  let(batch,deferred)=drain_frame_batch(&mut rx,frame(1));
  assert_eq!(batch.len(),1);
  assert!(matches!(deferred,Some(Input::Closed(..))));
  assert!(matches!(rx.try_recv(),Ok(Input::Frame(f)) if f.received_ns==3));
 }
}

#[cfg(test)]
#[cfg(test)]
#[path="ingress_runtime_r1333_tests.rs"]
mod r1333_production_ingress_replay_tests;

mod r1239_reader_queue_tests{
 use super::*;
 fn frame(ns:u64)->RawFrame{RawFrame{venue:Venue::OkxSpot,epoch:1,received_ns:ns,received_wall_ms:1_000,raw:String::new(),source:None}}
 #[tokio::test]
 async fn full_reader_queue_backpressures_without_dropping_or_reordering(){
  let(tx_a,mut rx_a)=mpsc::channel::<RawFrame>(1);let(tx_b,mut rx_b)=mpsc::channel::<RawFrame>(1);
  assert!(enqueue_reader_frame(&tx_a,frame(1)).await.0);
  let blocked_tx=tx_a.clone();let mut blocked=tokio::spawn(async move{enqueue_reader_frame(&blocked_tx,frame(2)).await});
  assert!(tokio::time::timeout(Duration::from_millis(20),&mut blocked).await.is_err());
  assert!(enqueue_reader_frame(&tx_b,frame(3)).await.0);
  assert_eq!(rx_b.recv().await.unwrap().received_ns,3);
  assert_eq!(rx_a.recv().await.unwrap().received_ns,1);
  let(sent,wait_ns)=tokio::time::timeout(Duration::from_secs(1),&mut blocked).await.unwrap().unwrap();
  assert!(sent);assert!(wait_ns>=20_000_000);
  assert_eq!(rx_a.recv().await.unwrap().received_ns,2);
  assert!(rx_a.try_recv().is_err());
 }
 #[test]
 fn saturated_control_queue_fails_closed_without_waiting(){
  let(tx,_rx)=mpsc::channel::<Input>(1);let stop=AtomicBool::new(false);
  assert!(send_control(&tx,&stop,Input::Subscribed("one".into())));
  assert!(!send_control(&tx,&stop,Input::Subscribed("two".into())));
  assert!(stop.load(Ordering::SeqCst));
 }
}

#[cfg(test)]
mod r1239_blocking_engine_yield_tests{
 use super::*;
 #[tokio::test(flavor="multi_thread",worker_threads=1)]
 async fn synchronous_engine_work_yields_runtime_worker_to_reader_tasks(){
  let(tx,rx)=std::sync::mpsc::channel::<()>();
  let task=tokio::spawn(async move{tx.send(()).unwrap();});
  let progressed=tokio::task::block_in_place(||rx.recv_timeout(Duration::from_secs(1)).is_ok());
  assert!(progressed);
  task.await.unwrap();
 }
 #[test]
 fn worst_case_frame_queue_budget_is_explicit_and_bounded(){
  assert_eq!(READER_FRAME_QUEUE_CAPACITY,16);
  assert_eq!(CENTRAL_FRAME_QUEUE_CAPACITY,32);
  assert_eq!(R1205_OUTPUT_QUEUE_CAPACITY,48);
  assert_eq!(R1205_CONTROL_QUEUE_CAPACITY,64);
  assert_eq!(R1205_ENGINE_BATCH_BUFFER_FRAMES,32);
  assert_eq!(estimate_r1205_queue_payload_bytes(30),241_172_480);
  assert!(estimate_r1205_queue_payload_bytes(31)<=R1205_QUEUE_PAYLOAD_BUDGET_BYTES);
  assert!(estimate_r1205_queue_payload_bytes(32)>R1205_QUEUE_PAYLOAD_BUDGET_BYTES);
 }
}

#[cfg(test)]mod r1248_topology_partition_tests{
 use super::*;
 #[test]fn forty_symbol_groups_are_exact_and_nonoverlapping(){
  let bases=(0..40).map(|i|format!("BASE{i:02}")).collect::<Vec<_>>();
  for(size,expected)in[(40,vec![40]),(20,vec![20,20]),(10,vec![10,10,10,10])]{let groups=r1248_partition_bases(&bases,size).unwrap();assert_eq!(groups.iter().map(Vec::len).collect::<Vec<_>>(),expected);assert_eq!(groups.concat(),bases);}
  assert!(r1248_partition_bases(&bases,8).is_err());
 }
 #[test]fn unconfigured_binance_partition_remains_one_full_cohort(){
  let bases=(0..40).map(|i|format!("BASE{i:02}")).collect::<Vec<_>>();
  assert_eq!(r1248_partition_bases(&bases,40).unwrap(),vec![bases]);
 }
}

#[cfg(test)]
mod r1272_stage_metric_tests{
 use super::*;
 #[test]
 fn queue_residence_uses_enqueue_clock_and_is_separate_from_receive_age(){
  let mut frame=RawFrame{venue:Venue::OkxSpot,epoch:1,received_ns:1_000_000,received_wall_ms:0,raw:String::new(),source:Some(provenance("s".into(),1))};
  frame.source.as_mut().unwrap().r1247_timing=Some(crate::R1247FrameTiming{t3_enqueue_mono_ns:Some(4_000_000),..Default::default()});
  assert_eq!(frame_queue_residence_ns(&frame,10_000_000),Some(6_000_000));
  assert_eq!(10_000_000-frame.received_ns,9_000_000);
  assert_eq!(frame_queue_residence_ns(&frame,3_000_000),None);
 }
 #[test]
 fn exchange_age_is_ms_and_preserves_future_clock_skew(){
  assert_eq!(exchange_age_ms(1_000,1_800),Some(800));
  assert_eq!(exchange_age_ms(1_801,1_800),Some(-1));
 }
 #[test]
 fn per_second_metrics_keep_counts_and_service_arithmetic(){
  let mut m=R1272SecondStats::default();m.batches=2;m.frames=5;m.service_ns_total=10_000_000;m.queue_residence_frames=5;m.queue_residence_ns_total=25_000_000;
  let v=m.value(3);
  assert_eq!(v["second"],3);assert_eq!(v["frames"],5);
  assert_eq!(v["service_mean_ms"],5.0);assert_eq!(v["queue_residence_mean_ms"],5.0);
 }
}

#[cfg(test)]
mod r1379_e0_queue_phase_tests {
 use super::*;
 #[test]
 fn r1382_reader_classification_market_control_timing_offline(){
  let a=adapter_for(Venue::OkxSpot);
  let mut stats=R1382ReaderClassify::default();
  for (wire,ns) in [
   (r#"{"arg":{"channel":"books"},"data":[{}]}"#,100u64),
   (r#"{"event":"subscribe"}"#,20),
   ("ping",30),
   ("not-json",40),
  ] { stats.record(&a.classify_control(wire),ns); }
  assert_eq!(stats.market_calls,1);
  assert_eq!(stats.control_calls,3);
  assert_eq!(stats.market_ns,100);
  assert_eq!(stats.control_ns,90);
  assert_eq!(stats.max_ns,100);
 }
 #[test]
 fn r1379_e0_queue16_forwarder_queue32_phases_and_missing_are_exact(){
  let mut source=provenance("r1379-OkxSpot-0".to_string(),1);
  source.r1279_timing=Some(crate::R1279FrameTiming{
   instrument:Some("BTC".to_owned()),ws_receive_ns:Some(100),reader_enqueue_ns:Some(120),reader_dequeue_ns:Some(140),central_permit_acquired_ns:Some(150),central_enqueue_ns:Some(160),central_dequeue_ns:Some(190),..Default::default()});
  let frame=RawFrame{venue:Venue::OkxSpot,epoch:1,received_ns:100,received_wall_ms:1000,raw:"{}".to_owned(),source:Some(source)};
  let mut profile=E0QueuePhases::default();profile.observe(&frame,200);
  let row=profile.snapshot();
  assert_eq!(row["frames"],1);
  assert_eq!(row["global"]["ws_to_reader_enqueue"]["total_ns"],20);
  assert_eq!(row["global"]["reader_queue16"]["total_ns"],20);
  assert_eq!(row["global"]["forwarder_to_queue32"]["total_ns"],20);
  assert_eq!(row["global"]["forwarder_reserve_wait"]["total_ns"],10);
  assert_eq!(row["global"]["forwarder_process_reserved"]["total_ns"],10);
  assert_eq!(row["global"]["central_queue32"]["total_ns"],30);
  assert_eq!(row["global"]["central_dequeue_to_engine"]["total_ns"],10);
  assert_eq!(row["by_instrument"]["OkxSpot:BTC"]["central_queue32"]["count"],1);
  let incomplete=RawFrame{venue:Venue::OkxSpot,epoch:1,received_ns:100,received_wall_ms:1000,raw:"{}".to_owned(),source:None};
  profile.observe(&incomplete,201);
  let row=profile.snapshot();
  assert_eq!(row["frames"],2);
  assert_eq!(row["without_instrument"],1);
  assert_eq!(row["global"]["central_queue32"]["missing"],1);
  assert_eq!(row["global"]["forwarder_reserve_wait"]["missing"],1);
  assert_eq!(row["global"]["forwarder_process_reserved"]["missing"],1);
  assert_eq!(row["global"]["central_queue32"]["count"],1);
 }
}
