//! Exchange-neutral connection quality and source lifecycle policy for R1209.
use crate::{RawFrame, Venue, parse};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap,HashSet,VecDeque};
use std::time::Duration;

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SourceId {
    pub exchange: String, pub segment: String, pub instrument: String, pub channel: String,
    pub physical_connection: String, pub logical_session: String, pub connection_group:String, pub generation: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Lifecycle { Connecting, Connected, Subscribed, Warming, HotReady, Active, Degraded, Quarantined, Reconnecting, Closed }
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Transport { Tcp, Tls, WebSocket }
/// Existing per-source lifecycle operations grouped for the single HOT owner.
/// This does not publish authority, resize queues, or change any policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConnectionLifecycleAction { Subscribe, Reconnect, Close }
#[derive(Clone, Debug)]
pub struct QualityPolicy { pub max_receive_age: Duration, pub max_exchange_age_ms: i64, pub min_samples: u32, pub qualify_dwell: Duration, pub recovery_min_samples:u32, pub recovery_qualify_dwell:Duration, pub minimum_score_samples:usize, pub switch_dwell:Duration, pub improvement_percent:u8, pub minimum_improvement_ns:u64 }
impl Default for QualityPolicy { fn default()->Self{Self{max_receive_age:Duration::from_millis(800),max_exchange_age_ms:800,min_samples:3,qualify_dwell:Duration::from_millis(250),recovery_min_samples:1,recovery_qualify_dwell:Duration::from_millis(100),minimum_score_samples:32,switch_dwell:Duration::from_secs(5),improvement_percent:20,minimum_improvement_ns:5_000_000}} }
#[derive(Clone, Debug, Default)]
pub struct SourceHealth {
    pub state: Option<Lifecycle>, pub tcp: bool, pub tls: bool, pub websocket: bool, pub subscribed: bool,
    pub last_receive_ns: Option<u64>, pub last_exchange_ms: Option<i64>, pub last_receive_wall_ms:Option<i64>, pub last_observed_ns:Option<u64>, pub last_valid_receive_ns:Option<u64>, pub last_valid_exchange_ms:Option<i64>, pub good_samples:u32,
    pub first_good_ns:Option<u64>, pub errors:u64, pub stale:u64, pub active:bool, pub hot_ready:bool, pub qualified_once:bool,
    pub processing_delay_ns:VecDeque<u64>, pub event_age_ns:VecDeque<u64>, pub reconnects:u64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AdapterControl { Market, Heartbeat, SubscribeAck, SubscribeError, Other }
pub trait ExchangeAdapter: Send + Sync {
    fn exchange(&self)->&'static str;
    fn venue(&self)->Option<Venue>;
    fn channel(&self)->&'static str;
    fn endpoint(&self, symbols:&[String])->String;
    fn subscription(&self, symbols:&[String])->Option<String>;
    fn classify_control(&self, raw:&str)->AdapterControl;
    fn parse_market(&self, frame:&RawFrame)->Result<NormalizedMarketEvent,String>;
}
pub struct BinanceAdapter;
pub struct OkxAdapter;
pub struct MockExchangeAdapter;
fn r1246_stream_profile(profile:Option<&str>)->(&'static str,&'static str){match profile{
 Some("TOP5_500MS")=>("depth5@500ms","books5"),
 Some("OKX_BOOKS5_BINANCE_DEPTH20_100MS")=>("depth20@100ms","books5"),
 _=>("depth20@100ms","books"),
}}
fn r1246_current_stream_profile()->(&'static str,&'static str){let profile=std::env::var("R1246_BOOK_PROFILE").ok();r1246_stream_profile(profile.as_deref())}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct NormalizedMarketEvent { pub instrument:String, pub exchange_ts_ms:i64, pub sequence:Option<i64>, pub full_snapshot:bool }
impl ExchangeAdapter for BinanceAdapter {
 fn exchange(&self)->&'static str{"binance"} fn venue(&self)->Option<Venue>{Some(Venue::BinanceFutures)} fn channel(&self)->&'static str{r1246_current_stream_profile().0}
 fn endpoint(&self,s:&[String])->String{let suffix=r1246_current_stream_profile().0;format!("wss://fstream.binance.com/public/stream?streams={}",s.iter().map(|b|format!("{}usdt@{}",b.to_ascii_lowercase(),suffix)).collect::<Vec<_>>().join("/"))}
 fn subscription(&self,_:&[String])->Option<String>{None}
 fn classify_control(&self,raw:&str)->AdapterControl{if raw.contains("\"result\"")||raw.contains("\"id\""){AdapterControl::Other}else{AdapterControl::Market}}
 fn parse_market(&self,f:&RawFrame)->Result<NormalizedMarketEvent,String>{if Some(f.venue)!=self.venue(){return Err("adapter venue mismatch".into())}let u=parse(f).map_err(|e|format!("{e:?}"))?;Ok(NormalizedMarketEvent{instrument:u.base,exchange_ts_ms:u.book.exchange_ts_ms.ok_or("missing exchange timestamp")?,sequence:u.book.sequence,full_snapshot:false})}
}
impl ExchangeAdapter for OkxAdapter {
 fn exchange(&self)->&'static str{"okx"} fn venue(&self)->Option<Venue>{Some(Venue::OkxSpot)} fn channel(&self)->&'static str{r1246_current_stream_profile().1}
 fn endpoint(&self,_:&[String])->String{"wss://ws.okx.com/ws/v5/public".into()}
 fn subscription(&self,s:&[String])->Option<String>{let channel=self.channel();Some(serde_json::json!({"op":"subscribe","args":s.iter().map(|b|serde_json::json!({"channel":channel,"instId":format!("{b}-USDT")})).collect::<Vec<_>>()}).to_string())}
 fn classify_control(&self,raw:&str)->AdapterControl{
   if raw=="ping"||raw=="pong"{return AdapterControl::Heartbeat}
   if let Ok(v)=serde_json::from_str::<serde_json::Value>(raw){if v.get("event").and_then(|x|x.as_str())==Some("subscribe"){return AdapterControl::SubscribeAck}if v.get("event").and_then(|x|x.as_str())==Some("error"){return AdapterControl::SubscribeError}if v.get("arg").is_some()&&v.get("data").is_some(){return AdapterControl::Market}}
   AdapterControl::Other
 }
 fn parse_market(&self,f:&RawFrame)->Result<NormalizedMarketEvent,String>{if Some(f.venue)!=self.venue(){return Err("adapter venue mismatch".into())}let v:serde_json::Value=serde_json::from_str(&f.raw).map_err(|e|e.to_string())?;let channel=self.channel();if v["arg"]["channel"]!=channel{return Err("OKX adapter channel mismatch".into())}let instrument=v["arg"]["instId"].as_str().and_then(|s|s.strip_suffix("-USDT")).ok_or("OKX instrument")?.to_string();let row=v["data"].as_array().filter(|a|a.len()==1).and_then(|a|a.first()).ok_or("OKX book row")?;let exchange_ts_ms=row["ts"].as_str().and_then(|s|s.parse().ok()).ok_or("OKX timestamp")?;let sequence=row["seqId"].as_i64().ok_or("OKX sequence")?;Ok(NormalizedMarketEvent{instrument,exchange_ts_ms,sequence:Some(sequence),full_snapshot:channel=="books5"||v["action"]=="snapshot"})}
}
impl ExchangeAdapter for MockExchangeAdapter {
 fn exchange(&self)->&'static str{"mock"} fn venue(&self)->Option<Venue>{None} fn channel(&self)->&'static str{"mock-book"}
 fn endpoint(&self,_:&[String])->String{"mock://market-data".into()}
 fn subscription(&self,s:&[String])->Option<String>{Some(serde_json::json!({"op":"subscribe","symbols":s}).to_string())}
 fn classify_control(&self,raw:&str)->AdapterControl{if let Ok(v)=serde_json::from_str::<serde_json::Value>(raw){if v["op"]=="subscribe_ack"{return AdapterControl::SubscribeAck}if v["symbol"].is_string(){return AdapterControl::Market}}AdapterControl::Other}
 fn parse_market(&self,f:&RawFrame)->Result<NormalizedMarketEvent,String>{let v:serde_json::Value=serde_json::from_str(&f.raw).map_err(|e|e.to_string())?;let instrument=v["symbol"].as_str().ok_or("mock symbol")?;let exchange_ts_ms=v["ts"].as_i64().ok_or("mock ts")?;Ok(NormalizedMarketEvent{instrument:instrument.into(),exchange_ts_ms,sequence:v["seq"].as_i64(),full_snapshot:v["snapshot"]==true})}
}
pub fn adapter_for(venue:Venue)->&'static dyn ExchangeAdapter{match venue{Venue::BinanceFutures=>&BinanceAdapter,Venue::OkxSpot=>&OkxAdapter}}

#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum ConnectionGroupSize { Eight, Sixteen, ThirtyTwo }
impl TryFrom<usize> for ConnectionGroupSize {
 type Error=String;
 fn try_from(v:usize)->Result<Self,Self::Error>{match v{8=>Ok(Self::Eight),16=>Ok(Self::Sixteen),32=>Ok(Self::ThirtyTwo),_=>Err("group size must be 8, 16, or 32 instruments".into())}}
}
impl ConnectionGroupSize { pub fn instruments(self)->usize{match self{Self::Eight=>8,Self::Sixteen=>16,Self::ThirtyTwo=>32}} }
pub fn partition_instruments(symbols:&[String],group:ConnectionGroupSize)->Result<Vec<Vec<String>>,String>{
 if symbols.iter().any(|s|s.is_empty())||symbols.iter().collect::<std::collections::HashSet<_>>().len()!=symbols.len(){return Err("empty or duplicate instrument".into())}
 Ok(symbols.chunks(group.instruments()).map(|c|c.to_vec()).collect())
}
/// HOT state transition only; never grants Engine authority or changes book freshness.
#[derive(Debug,PartialEq,Eq)]
pub(crate) enum DualHotTransition { None, Established, Restored { deficit_duration_ns:Option<u64> }, DeficitOnset }
pub struct UniversalConnectionManager {
 policy:QualityPolicy, sources:HashMap<SourceId,SourceHealth>, active:HashMap<(String,String,String,String),SourceId>,
 challenger:HashMap<(String,String,String,String),(SourceId,u64)>, reconnect_history:HashMap<String,u64>,
 readiness_previous:HashMap<(String,String),u16>, dual_hot_seen:HashSet<String>,
 dual_hot_state:HashMap<String,bool>, dual_hot_deficit_started_ns:HashMap<String,u64>,
}
// Only the actual ingress runtime's stable lane/epoch identity is
// eligible for compaction. Arbitrary legacy identities retain old guards.
pub(crate) fn runtime_lane(id:&SourceId)->Option<&str>{
    if id.physical_connection!=id.logical_session{return None}
    let mut parts=id.physical_connection.rsplitn(3,'-');
    let suffix=parts.next()?;let epoch=parts.next()?;let lane=parts.next()?;
    if suffix.len()!=16||!suffix.bytes().all(|b|b.is_ascii_hexdigit())
        ||epoch.parse::<u64>().ok()!=Some(id.generation)||id.generation==0
        ||lane!=id.connection_group{return None}
    let mut fields=lane.split('-');
    let shard=fields.next()?.strip_prefix('g')?;
    let venue=fields.next()?;let copy=fields.next()?;
    if fields.next().is_some()||shard.parse::<usize>().is_err()||copy.parse::<usize>().is_err(){return None}
    if !matches!((venue,id.exchange.as_str(),id.segment.as_str()),
        ("BinanceFutures","binance","usdm")|("OkxSpot","okx","spot")){return None}
    Some(lane)
}
fn scope(id:&SourceId)->(String,String,String,String){(id.exchange.clone(),id.segment.clone(),id.instrument.clone(),id.channel.clone())}
const QUALITY_WINDOW:usize=128;
fn keep_sample(samples:&mut VecDeque<u64>,value:u64){if samples.len()>=QUALITY_WINDOW{samples.pop_front();}samples.push_back(value)}
fn percentile(samples:&VecDeque<u64>,percent:usize)->Option<u64>{
    let n=samples.len();
    if n==0{return None}
    let rank=((n-1)*percent.min(100))/100;
    // Existing rolling window is bounded. No heap allocation for its
    // temporary selection buffer. Preserve the old oversized behavior.
    if n>QUALITY_WINDOW{
        let mut v=samples.iter().copied().collect::<Vec<_>>();
        return Some(*v.select_nth_unstable(rank).1)
    }
    let mut scratch=[0u64;QUALITY_WINDOW];
    let (a,b)=samples.as_slices();
    scratch[..a.len()].copy_from_slice(a);
    scratch[a.len()..n].copy_from_slice(b);
    Some(*scratch[..n].select_nth_unstable(rank).1)
}
impl UniversalConnectionManager {
 pub fn new(policy:QualityPolicy)->Self{Self{policy,sources:HashMap::new(),active:HashMap::new(),challenger:HashMap::new(),reconnect_history:HashMap::new(),readiness_previous:HashMap::new(),dual_hot_seen:HashSet::new(),dual_hot_state:HashMap::new(),dual_hot_deficit_started_ns:HashMap::new()}}
 /// The Engine calculates the original 14-bit readiness mask (including its
 /// book freshness and final safety veto). UCM owns ONLY its transition history.
 pub(crate) fn observe_readiness_mask(&mut self,scope:&str,session:&str,mask:u16)->(Option<u16>,bool){
  let key=(scope.to_string(),session.to_string());
  let mut evicted=false;
  if !self.readiness_previous.contains_key(&key)&&self.readiness_previous.len()>=4096{
   if let Some(old)=self.readiness_previous.keys().next().cloned(){self.readiness_previous.remove(&old);evicted=true;}
  }
  (self.readiness_previous.insert(key,mask),evicted)
 }
 pub(crate) fn readiness_previous_len(&self)->usize{self.readiness_previous.len()}
 pub(crate) fn readiness_has_session(&self,session:&str)->bool{self.readiness_previous.keys().any(|(_,s)|s==session)}
 pub(crate) fn readiness_previous_payload_capacity_bytes(&self)->usize{
  self.readiness_previous.iter().map(|((scope,session),_)|scope.capacity().saturating_add(session.capacity()).saturating_add(std::mem::size_of::<(String,String,u16)>())).sum()
 }
 pub(crate) fn retire_readiness_session(&mut self,session:&str)->usize{
  let before=self.readiness_previous.len();self.readiness_previous.retain(|(_,s),_|s!=session);before.saturating_sub(self.readiness_previous.len())
 }
 /// Records the existing dual-HOT transitions only; Engine still verifies the
 /// source list, revokes stale authority and renders the original audit JSON.
 pub(crate) fn observe_dual_hot(&mut self,scope:&str,hot:bool,now_ns:u64)->DualHotTransition{
  let prior=self.dual_hot_state.get(scope).copied();
  if hot&&!self.dual_hot_seen.contains(scope){
   self.dual_hot_seen.insert(scope.to_string());self.dual_hot_state.insert(scope.to_string(),true);
   DualHotTransition::Established
  }else if hot&&prior==Some(false){
   self.dual_hot_state.insert(scope.to_string(),true);
   let deficit_duration_ns=self.dual_hot_deficit_started_ns.remove(scope).map(|start|now_ns.saturating_sub(start));
   DualHotTransition::Restored{deficit_duration_ns}
  }else if !hot&&self.dual_hot_seen.contains(scope)&&prior==Some(true){
   self.dual_hot_state.insert(scope.to_string(),false);self.dual_hot_deficit_started_ns.insert(scope.to_string(),now_ns);
   DualHotTransition::DeficitOnset
  }else{
   if !hot&&self.dual_hot_seen.contains(scope){self.dual_hot_state.insert(scope.to_string(),false);}
   DualHotTransition::None
  }
 }
 /// Apply the same existing lifecycle transition to the registered source IDs
 /// of one physical connection; final Engine authority is not touched here.
 pub fn apply_connection_lifecycle(&mut self,ids:&[SourceId],action:ConnectionLifecycleAction){
  for id in ids{match action{
   ConnectionLifecycleAction::Subscribe=>self.subscribed(id),
   ConnectionLifecycleAction::Reconnect=>self.reconnecting(id),
   ConnectionLifecycleAction::Close=>self.close(id),
  }}
 }
 // Existing lane membership is fixed in the runtime reader. Keep its
 // latest generation (including closed records) as the replay fence.
 // Only replace a wholly closed, strictly older, identical scope group.
 fn retired_runtime_sources(&self,ids:&[SourceId])->Result<Vec<SourceId>,String>{
    let Some(first)=ids.first()else{return Ok(Vec::new())};
    let Some(lane)=runtime_lane(first)else{
        if ids.iter().any(|id|runtime_lane(id).is_some()){return Err("UCM mixed runtime identity group".into())}
        return Ok(Vec::new())
    };
    if !ids.iter().all(|id|runtime_lane(id)==Some(lane)&&id.generation==first.generation){
        return Err("UCM inconsistent runtime generation group".into())
    }
    let old=self.sources.iter().filter(|(id,_)|runtime_lane(id)==Some(lane)).collect::<Vec<_>>();
    if old.is_empty(){return Ok(Vec::new())}
    if old.iter().any(|(id,h)|id.generation>=first.generation||h.state!=Some(Lifecycle::Closed)){
        return Err("UCM runtime generation not strictly newer than closed lane".into())
    }
    let previous=old.iter().map(|(id,_)|scope(id)).collect::<HashSet<_>>();
    let next=ids.iter().map(scope).collect::<HashSet<_>>();
    if previous!=next{return Err("UCM runtime lane scope changed".into())}
    Ok(old.into_iter().map(|(id,_)|id.clone()).collect())
 }
 pub fn connect(&mut self,id:SourceId){let mut h=SourceHealth::default();h.tcp=true;h.tls=true;h.websocket=true;h.reconnects=self.reconnect_history.get(&id.connection_group).copied().unwrap_or(0);h.state=Some(Lifecycle::Connected);self.sources.insert(id,h);}
 pub fn connecting(&mut self,id:SourceId){let mut h=SourceHealth::default();h.state=Some(Lifecycle::Connecting);self.sources.insert(id,h);}
 pub fn subscribed(&mut self,id:&SourceId){if let Some(h)=self.sources.get_mut(id){h.subscribed=true;h.state=Some(Lifecycle::Subscribed)}}
 pub fn reconnecting(&mut self,id:&SourceId){let count=if let Some(h)=self.sources.get_mut(id){h.active=false;h.hot_ready=false;h.qualified_once=false;h.good_samples=0;h.first_good_ns=None;h.last_valid_receive_ns=None;h.last_valid_exchange_ms=None;h.reconnects=h.reconnects.saturating_add(1);h.state=Some(Lifecycle::Reconnecting);Some(h.reconnects)}else{None};if let Some(count)=count{self.reconnect_history.insert(id.connection_group.clone(),count);}self.active.retain(|_,active|active!=id)}
 pub fn quarantine(&mut self,id:&SourceId){if let Some(h)=self.sources.get_mut(id){h.active=false;h.hot_ready=false;h.qualified_once=false;h.good_samples=0;h.first_good_ns=None;h.last_valid_receive_ns=None;h.last_valid_exchange_ms=None;h.errors=h.errors.saturating_add(1);h.state=Some(Lifecycle::Quarantined)}self.active.retain(|_,active|active!=id)}
 pub fn transport(&mut self,id:&SourceId,layer:Transport,ok:bool){if let Some(h)=self.sources.get_mut(id){match layer{Transport::Tcp=>h.tcp=ok,Transport::Tls=>h.tls=ok,Transport::WebSocket=>h.websocket=ok}if !ok{h.errors=h.errors.saturating_add(1);h.hot_ready=false;h.qualified_once=false;h.active=false;h.good_samples=0;h.first_good_ns=None;h.state=Some(Lifecycle::Degraded);self.active.retain(|_,v|v!=id);}}}
 pub fn observe(&mut self,id:&SourceId,recv_ns:u64,event_ms:i64,now_ns:u64,now_ms:i64,valid:bool){self.observe_with_receive_wall(id,recv_ns,event_ms,now_ms,now_ns,now_ms,valid);}
 pub fn observe_with_receive_wall(&mut self,id:&SourceId,recv_ns:u64,event_ms:i64,received_wall_ms:i64,now_ns:u64,now_ms:i64,valid:bool){
  let Some(h)=self.sources.get_mut(id)else{return};h.last_receive_ns=Some(recv_ns);h.last_exchange_ms=Some(event_ms);h.last_receive_wall_ms=Some(received_wall_ms);h.last_observed_ns=Some(now_ns);
  if now_ns>=recv_ns{keep_sample(&mut h.processing_delay_ns,now_ns-recv_ns);}
  if now_ms>=event_ms{keep_sample(&mut h.event_age_ns,((now_ms-event_ms)as u64).saturating_mul(1_000_000));}
  let prior_fresh=h.last_valid_receive_ns.is_some_and(|t|now_ns>=t&&now_ns-t<=self.policy.max_receive_age.as_nanos() as u64)&&h.last_valid_exchange_ms.and_then(|t|now_ms.checked_sub(t)).is_some_and(|a|(0..=self.policy.max_exchange_age_ms).contains(&a));
  if h.last_valid_receive_ns.is_some()&&!prior_fresh{h.good_samples=0;h.first_good_ns=None;h.hot_ready=false;}
  let fresh=now_ns>=recv_ns&&now_ns-recv_ns<=self.policy.max_receive_age.as_nanos() as u64&&now_ms.checked_sub(event_ms).is_some_and(|a|(0..=self.policy.max_exchange_age_ms).contains(&a));
  if !valid||!fresh{h.stale=h.stale.saturating_add(1);if !prior_fresh{h.hot_ready=false;h.state=Some(Lifecycle::Degraded)}return}
  h.last_valid_receive_ns=Some(recv_ns);h.last_valid_exchange_ms=Some(event_ms);if h.good_samples==0{h.first_good_ns=Some(recv_ns)}h.good_samples=h.good_samples.saturating_add(1);
  let (minimum_samples,dwell_ns)=if h.qualified_once{(self.policy.recovery_min_samples,self.policy.recovery_qualify_dwell.as_nanos().min(u64::MAX as u128)as u64)}else{(self.policy.min_samples,self.policy.qualify_dwell.as_nanos().min(u64::MAX as u128)as u64)};
  let dwell=h.first_good_ns.is_some_and(|t|recv_ns>=t&&recv_ns-t>=dwell_ns);
  h.hot_ready=h.subscribed&&h.good_samples>=minimum_samples&&dwell;
  if h.hot_ready{h.qualified_once=true;}
  h.state=Some(if h.active{Lifecycle::Active}else if h.hot_ready{Lifecycle::HotReady}else{Lifecycle::Warming});
 }
 /// Record a transport/control frame without advancing market-data freshness.
 pub fn observe_transport_liveness(&mut self,id:&SourceId,recv_ns:u64,received_wall_ms:i64,now_ns:u64)->Result<(),String>{
  let h=self.sources.get_mut(id).ok_or("unknown source")?;
  if h.last_receive_ns.is_some_and(|previous|recv_ns<previous){return Err("transport receive clock moved backward".into())}
  h.last_receive_ns=Some(recv_ns);
  h.last_receive_wall_ms=Some(received_wall_ms);
  h.last_observed_ns=Some(now_ns);
  if now_ns>=recv_ns{keep_sample(&mut h.processing_delay_ns,now_ns-recv_ns);}
  Ok(())
 }
 pub fn set_active(&mut self,id:&SourceId)->bool{
  let key=(id.exchange.clone(),id.segment.clone(),id.instrument.clone(),id.channel.clone());let Some(h)=self.sources.get(id)else{return false};if !h.hot_ready{return false}
  if let Some(previous)=self.active.insert(key, id.clone()){if let Some(old)=self.sources.get_mut(&previous){old.active=false;if old.hot_ready{old.state=Some(Lifecycle::HotReady)}}}
  if let Some(h)=self.sources.get_mut(id){h.active=true;h.state=Some(Lifecycle::Active)}true
 }
 pub fn quality(&self,id:&SourceId)->Option<(u64,u64,usize)>{let h=self.sources.get(id)?;let stability=h.reconnects.min(5)*50_000_000+h.errors.min(10)*10_000_000+h.stale.min(10)*2_000_000;Some((percentile(&h.processing_delay_ns,95)?.saturating_add(stability),percentile(&h.event_age_ns,99)?,h.processing_delay_ns.len().min(h.event_age_ns.len())))}
 fn fresh(&self,id:&SourceId,now_ns:u64,now_ms:i64)->bool{self.sources.get(id).is_some_and(|h|h.hot_ready&&h.subscribed&&h.tcp&&h.tls&&h.websocket&&h.last_valid_receive_ns.is_some_and(|t|now_ns>=t&&now_ns-t<=self.policy.max_receive_age.as_nanos()as u64)&&h.last_valid_exchange_ms.and_then(|t|now_ms.checked_sub(t)).is_some_and(|a|(0..=self.policy.max_exchange_age_ms).contains(&a)))}
 /// The manager proposes one source per instrument scope. A healthy active source
 /// is sticky until a measured challenger wins for the full dwell window; loss of
 /// transport, subscription, freshness, or book qualification fails over immediately.
 pub fn choose_candidate(&mut self,candidates:&[SourceId],now_ns:u64,now_ms:i64)->Option<(SourceId,&'static str)>{
  self.choose_candidate_with_quality(candidates,now_ns,now_ms,|manager,id|manager.quality(id))
 }
 // Same election policy, with an exact owner-maintained quality projection.
 fn choose_candidate_with_quality(&mut self,candidates:&[SourceId],now_ns:u64,now_ms:i64,
   quality:impl Fn(&Self,&SourceId)->Option<(u64,u64,usize)>)->Option<(SourceId,&'static str)>{
  if candidates.is_empty(){return None}
  let key=scope(candidates.first()?);
  let mut ranked=candidates.iter().filter(|id|scope(id)==key&&self.fresh(id,now_ns,now_ms)).filter_map(|id|{let (p95,event_p99,n)=quality(self,id)?;Some((p95.saturating_add(event_p99),n,id.clone()))}).collect::<Vec<_>>();
  ranked.sort_by(|a,b|a.0.cmp(&b.0).then_with(||a.2.physical_connection.cmp(&b.2.physical_connection)));
  let (best_score,best_samples,best)=ranked.first().cloned()?;
  let active=self.active.get(&key).cloned();
  if let Some(current)=active.as_ref().filter(|id|ranked.iter().any(|r|&r.2==*id)){
   if current==&best{self.challenger.remove(&key);return Some((current.clone(),"active_remains_best"))}
   let (current_score,_,current_samples)=quality(self,current)?;
   let required=(current_score.saturating_mul(self.policy.improvement_percent as u64)/100).max(self.policy.minimum_improvement_ns);
   if current_samples>=self.policy.minimum_score_samples&&best_samples>=self.policy.minimum_score_samples&&current_score.saturating_sub(best_score)>=required{
    let started=match self.challenger.get(&key){Some((challenger,at))if challenger==&best=>*at,_=>{self.challenger.insert(key.clone(),(best.clone(),now_ns));now_ns}};
    if now_ns.saturating_sub(started)>=self.policy.switch_dwell.as_nanos()as u64{self.challenger.remove(&key);return Some((best,"sustained_quality_improvement"))}
   }else{self.challenger.remove(&key);}
   return Some((current.clone(),"hysteresis_hold"))
  }
  self.challenger.remove(&key);
  Some((best,if active.is_some(){"active_source_unready_emergency_failover"}else{"initial_manager_choice"}))
 }
 pub fn active_for_scope(&self,id:&SourceId)->Option<&SourceId>{self.active.get(&scope(id))}
 pub fn should_rotate_connection(&self,physical_connection:&str,now_ns:u64,now_ms:i64)->bool{let scopes=self.sources.keys().filter(|id|id.physical_connection==physical_connection).cloned().collect::<Vec<_>>();!scopes.is_empty()&&scopes.iter().all(|id|!self.fresh(id,now_ns,now_ms))}
 pub fn close(&mut self,id:&SourceId){if let Some(h)=self.sources.get_mut(id){h.active=false;h.hot_ready=false;h.qualified_once=false;h.websocket=false;h.state=Some(Lifecycle::Closed)}self.active.retain(|_,v|v!=id);self.challenger.retain(|_,(candidate,_)|candidate!=id)}
 pub fn state(&self,id:&SourceId)->Option<&SourceHealth>{self.sources.get(id)}
 pub fn qualification_requirements(&self,id:&SourceId)->(u32,u64,bool){let recovery=self.sources.get(id).is_some_and(|h|h.qualified_once);let samples=if recovery{self.policy.recovery_min_samples}else{self.policy.min_samples};let dwell=if recovery{self.policy.recovery_qualify_dwell}else{self.policy.qualify_dwell};(samples,dwell.as_nanos().min(u64::MAX as u128)as u64,recovery)}
 fn tick_fields(h:&SourceHealth,now_ns:u64,now_ms:i64,max_receive_age_ns:u64,max_exchange_age_ms:i64)->(u32,Option<u64>,bool,Option<Lifecycle>){
  let fresh=h.last_valid_receive_ns.is_some_and(|t|now_ns>=t&&now_ns-t<=max_receive_age_ns)
   &&h.last_valid_exchange_ms.and_then(|t|now_ms.checked_sub(t)).is_some_and(|a|(0..=max_exchange_age_ms).contains(&a));
  let (mut samples,mut first,mut hot,mut state)=(h.good_samples,h.first_good_ns,h.hot_ready,h.state);
  if h.last_valid_receive_ns.is_some()&&!fresh{samples=0;first=None;if hot{hot=false;state=Some(Lifecycle::Degraded);}}
  if !h.websocket&&state!=Some(Lifecycle::Closed){state=Some(Lifecycle::Reconnecting);}
  (samples,first,hot,state)
 }
 pub fn tick(&mut self,now_ns:u64,now_ms:i64){
  let receive=self.policy.max_receive_age.as_nanos() as u64;let exchange=self.policy.max_exchange_age_ms;
  for h in self.sources.values_mut(){
   (h.good_samples,h.first_good_ns,h.hot_ready,h.state)=Self::tick_fields(h,now_ns,now_ms,receive,exchange);
  }
 }
 pub fn hot_standbys(&self,id:&SourceId)->usize{self.sources.iter().filter(|(k,h)|k.exchange==id.exchange&&k.segment==id.segment&&k.instrument==id.instrument&&k.channel==id.channel&&k.physical_connection!=id.physical_connection&&h.hot_ready).count()}
 pub fn failover_candidate(&self,id:&SourceId,now_ns:u64,now_ms:i64)->Option<&SourceId>{
  let key=(id.exchange.clone(),id.segment.clone(),id.instrument.clone(),id.channel.clone());
  let current_fresh=self.sources.get(id).is_some_and(|h|h.hot_ready&&h.last_valid_receive_ns.is_some_and(|t|now_ns>=t&&now_ns-t<=self.policy.max_receive_age.as_nanos() as u64)&&h.last_valid_exchange_ms.and_then(|t|now_ms.checked_sub(t)).is_some_and(|a|(0..=self.policy.max_exchange_age_ms).contains(&a)));
  if current_fresh{return None}
  self.sources.iter().filter(|(k,h)|k.exchange==key.0&&k.segment==key.1&&k.instrument==key.2&&k.channel==key.3&&k.physical_connection!=id.physical_connection&&h.hot_ready&&h.last_valid_receive_ns.is_some_and(|t|now_ns>=t&&now_ns-t<=self.policy.max_receive_age.as_nanos() as u64)&&h.last_valid_exchange_ms.and_then(|t|now_ms.checked_sub(t)).is_some_and(|a|(0..=self.policy.max_exchange_age_ms).contains(&a))).max_by_key(|(_,h)|h.last_receive_ns).map(|(k,_)|k)
 }
 /// Physical rotation is allowed only when every subscribed scope on the socket is bad.
 pub fn should_rotate_physical(bad_scopes:usize,total_scopes:usize)->bool{total_scopes>0&&bad_scopes==total_scopes}
}


type UcmScopeKey=(String,String,String,String);
// Each source operation affects its exchange, segment, instrument and channel.
// Tick remains a global invalidation barrier; never ignore expiry.
impl UcmWorkerEvent {
 fn affected_scopes(&self)->Option<Vec<UcmScopeKey>>{
  match self{
   Self::Connect(id)|Self::Subscribe(id)|Self::Reconnect(id)
   |Self::Close(id)|Self::Quarantine(id)=>Some(vec![scope(id)]),
   Self::Group{ids,..}=>Some(ids.iter().map(scope).collect()),
   Self::Observe{source,..}|Self::TransportOnly{source,..}
   |Self::CommitActive{source,..}|Self::BookReset{source,..}=>Some(vec![scope(source)]),
   Self::Tick{..}=>None,
  }
 }
}
// R1399: an optional, bounded, single-owner UCM execution boundary. Creating
// this worker is explicit; it is NOT attached to production Engine/authority.
// Engine must validate every input BEFORE submitting it and independently
// revalidate every advisory proposal BEFORE issuing a source Permit.
// No queued operation may be overwritten or silently discarded.
#[derive(Clone,Copy,Debug)]
pub(crate) enum UcmGroupAction {Connect{auto_subscribe:bool},Subscribe,Reconnect,Close,Quarantine}
#[derive(Clone, Debug)]
pub(crate) enum UcmWorkerEvent {
    // One physical connection control already arrives as one Engine input.
    // Preserve its ordered atomic mutation without N mailbox entries.
    Group{ids:Vec<SourceId>,action:UcmGroupAction},
    Connect(SourceId),
    Subscribe(SourceId),
    Reconnect(SourceId),
    Close(SourceId),
    Quarantine(SourceId),
    // OKX L2 reconstruction reset of this book ONLY; the physical session
    // is not retired. Engine still validates every subsequent frame.
    BookReset { source: SourceId, sequence_gap: bool },
    Observe {
        source: SourceId, received_ns: u64, exchange_ms: i64,
        received_wall_ms: i64, now_ns: u64, now_ms: i64, valid: bool,
    },
    TransportOnly {
        source: SourceId, received_ns: u64, received_wall_ms: i64, now_ns: u64,
    },
    Tick { now_ns: u64, now_ms: i64 },
    CommitActive { source: SourceId, now_ns: u64, now_ms: i64 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct UcmWorkerAdvice {
    pub revision: u64,
    pub candidate: Option<(SourceId, &'static str)>,
    // The sole owner certifies a time-independent result only when no
    // challenger dwell is in progress and the chosen source is already active.
    pub stable:Option<UcmStableChoice>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct UcmStableChoice {
    pub candidate:SourceId,
    pub reason:&'static str,
    pub eligible:Vec<(SourceId,Option<(u64,u64,usize)>)>,
}

/// Immutable projection of ONE worker-owned UCM state. Engine may inspect
/// it but never mutate source quality or election history. No snapshot is
/// itself a trading Permit, and a mismatched revision must be discarded.
#[derive(Clone, Debug)]
pub(crate) struct UcmSourceReadView {
    pub health: SourceHealth,
    pub quality: Option<(u64,u64,usize)>,
    pub qualification_requirements: (u32,u64,bool),
}

#[derive(Clone, Debug)]
pub(crate) struct UcmWorkerSnapshot {
    pub event_advice:Option<UcmEventAdvice>,
    pub hot_loss_revision:u64,
    pub revision: u64,
    pub sources: HashMap<SourceId,std::sync::Arc<UcmSourceReadView>>,
    pub active: HashMap<(String,String,String,String),SourceId>,
    max_receive_age_ns: u64,
    max_exchange_age_ms: i64,
}
impl UcmWorkerSnapshot {
    /// Advisory HOT-readiness test at ACTUAL Engine decision time, not the
    /// time when this snapshot was captured. Final book safety is separate.
    pub(crate) fn source_ready_at(&self,id:&SourceId,now_ns:u64,now_ms:i64)->bool {
        self.sources.get(id).is_some_and(|view|{
            let h=&view.health;
            h.hot_ready&&h.subscribed&&h.tcp&&h.tls&&h.websocket
            &&h.last_valid_receive_ns.is_some_and(|t|
                now_ns>=t&&now_ns-t<=self.max_receive_age_ns)
            &&h.last_valid_exchange_ms.and_then(|t|now_ms.checked_sub(t))
                .is_some_and(|age|(0..=self.max_exchange_age_ms).contains(&age))
        })
    }
}

// Read-only latest projection from the sole owner; no new queue or manager.
// Only the owner writes. An Engine read is nonblocking and fenced by the
// producer's submitted revision, including pending global Tick.
// Owner-local exact Tick index. A source has at most one entry on
// each clock, and at most one eager entry. Rescheduling removes old
// entries; no lazy heap tombstones. i128 avoids endpoint overflow.
#[derive(Default)]
struct UcmTickIndex{
    mono:std::collections::BTreeSet<(i128,SourceId)>,
    wall:std::collections::BTreeSet<(i128,SourceId)>,
    indexed:HashMap<SourceId,(i128,i128)>,
    eager:HashSet<SourceId>,
}
impl UcmTickIndex{
    fn sync(&mut self,id:&SourceId,health:Option<&SourceHealth>){
        if let Some((m,w))=self.indexed.remove(id){
            self.mono.remove(&(m,id.clone()));self.wall.remove(&(w,id.clone()));
        }
        self.eager.remove(id);
        let Some(h)=health else{return};
        if !h.websocket&&!matches!(h.state,Some(Lifecycle::Closed|Lifecycle::Reconnecting)){
            self.eager.insert(id.clone());
        }
        // Tick only resets these three fields when last valid data is stale.
        // A fully reset source cannot become HOT by passage of time alone.
        if h.good_samples==0&&h.first_good_ns.is_none()&&!h.hot_ready{return}
        if let Some(m)=h.last_valid_receive_ns{
            if let Some(w)=h.last_valid_exchange_ms{
                let (m,w)=(m as i128,w as i128);
                self.mono.insert((m,id.clone()));self.wall.insert((w,id.clone()));
                self.indexed.insert(id.clone(),(m,w));
            }else{self.eager.insert(id.clone());}
        }
    }
    fn take_due(&self,ns:u64,wall:i64,receive_age:u64,exchange_age:i64)->HashSet<SourceId>{
        let (n,w)=(ns as i128,wall as i128);
        let mut due=self.eager.clone();
        // Inclusive age limit: expire strictly before now-limit. Upper
        // ranges detect future timestamps, including a backward clock jump.
        due.extend(self.mono.iter().take_while(|(at,_)|*at<n-receive_age as i128).map(|(_,id)|id.clone()));
        due.extend(self.mono.iter().rev().take_while(|(at,_)|*at>n).map(|(_,id)|id.clone()));
        due.extend(self.wall.iter().take_while(|(at,_)|*at<w-exchange_age as i128).map(|(_,id)|id.clone()));
        due.extend(self.wall.iter().rev().take_while(|(at,_)|*at>w).map(|(_,id)|id.clone()));
        due
    }
    fn tick(&mut self,manager:&mut UniversalConnectionManager,ns:u64,wall:i64)->Vec<SourceId>{
        let receive=manager.policy.max_receive_age.as_nanos() as u64;
        let exchange=manager.policy.max_exchange_age_ms;
        let due=self.take_due(ns,wall,receive,exchange);
        let mut changed=Vec::new();
        for id in due{
            if let Some(h)=manager.sources.get_mut(&id){
                let before=(h.good_samples,h.first_good_ns,h.hot_ready,h.state);
                let after=UniversalConnectionManager::tick_fields(h,ns,wall,receive,exchange);
                if after!=before{
                    (h.good_samples,h.first_good_ns,h.hot_ready,h.state)=after;
                    changed.push(id.clone());
                }
            }
            self.sync(&id,manager.state(&id));
        }
        changed
    }
}
#[derive(Default)]
struct UcmPublished {
    tick_index:UcmTickIndex,
    challenge_deadlines:std::collections::BTreeSet<(u64,UcmScopeKey)>,
    challenge_matured:std::collections::BTreeSet<(u64,UcmScopeKey)>,
    challenge_times:HashMap<UcmScopeKey,(u64,bool)>,
    applied_revision:u64,
    scopes:HashMap<UcmScopeKey,UcmWorkerSnapshot>,
}
impl UcmPublished {
    fn challenge_deadline(&mut self,key:&UcmScopeKey,at:Option<(u64,bool)>){
        if let Some((old,matured))=self.challenge_times.remove(key){
            if matured{self.challenge_matured.remove(&(old,key.clone()));}else{self.challenge_deadlines.remove(&(old,key.clone()));}
        }
        if let Some((at,matured))=at{
            self.challenge_times.insert(key.clone(),(at,matured));
            if matured{self.challenge_matured.insert((at,key.clone()));}else{self.challenge_deadlines.insert((at,key.clone()));}
        }
    }
    fn refresh_source(&mut self,manager:&UniversalConnectionManager,id:&SourceId,revision:u64){
        self.refresh_source_measured(manager,id,revision,|_|{});
    }
    fn refresh_source_measured(&mut self,manager:&UniversalConnectionManager,id:&SourceId,revision:u64,mut mark:impl FnMut(usize)){
        let Some(health)=manager.state(id)else{return};
        self.tick_index.sync(id,Some(health));mark(3);
        let key=scope(id);
        let view=self.scopes.entry(key.clone()).or_insert_with(||UcmWorkerSnapshot{
            revision:0,event_advice:None,hot_loss_revision:0,sources:HashMap::new(),active:HashMap::new(),
            max_receive_age_ns:manager.policy.max_receive_age.as_nanos().min(u64::MAX as u128) as u64,
            max_exchange_age_ms:manager.policy.max_exchange_age_ms,
        });
        if view.sources.get(id).is_some_and(|old|old.health.hot_ready&&!health.hot_ready){view.hot_loss_revision=revision;}
        let health_copy=health.clone();mark(4);
        let quality=manager.quality(id);mark(5);
        view.sources.insert(id.clone(),std::sync::Arc::new(UcmSourceReadView{
            health:health_copy,quality,
            qualification_requirements:manager.qualification_requirements(id),
        }));
        if let Some(active)=manager.active.get(&key){view.active.insert(key,active.clone());}
        else{view.active.remove(&key);}
        mark(6);
    }
    fn refresh_tick(&mut self,manager:&UniversalConnectionManager,revision:u64,changed:&[SourceId]){
        // The original Tick mutates only these four fields. Preserve exact
        // state without cloning/sorting unchanged quality sample windows.
        for id in changed {
            let Some(health)=manager.state(id)else{continue};
            let key=scope(id);
            if let Some(view)=self.scopes.get_mut(&key){
              if let Some(old)=view.sources.get_mut(id){
                if old.health.hot_ready&&!health.hot_ready{view.hot_loss_revision=revision;}
                if old.health.hot_ready!=health.hot_ready||old.health.state!=health.state
                    ||old.health.good_samples!=health.good_samples||old.health.first_good_ns!=health.first_good_ns{
                    // A changed readiness field invalidates the previously
                    // published decision basis. A proven no-op Tick does not.
                    view.event_advice=None;
                    let next=std::sync::Arc::make_mut(old);
                    next.health.hot_ready=health.hot_ready;
                    next.health.state=health.state;
                    next.health.good_samples=health.good_samples;
                    next.health.first_good_ns=health.first_good_ns;
                }
                continue
              }
            }
            self.refresh_source(manager,id,revision);
            if let Some(view)=self.scopes.get_mut(&key){view.event_advice=None;}
        }
    }
}
#[cfg(test)]
pub(crate) struct UcmPendingPublished {
    requested_at:std::time::Instant,
}

// Ordered diagnostic history operations, NOT source authority or a Permit.
#[derive(Debug)]
pub(crate) enum UcmHistoryEvent {
    Scope { scope: String, masks: Vec<(String,u16)>, dual_hot: bool, now_ns: u64 },
    RetireSession { session: String },
    Census,
}
#[derive(Debug,PartialEq,Eq)]
pub(crate) enum UcmHistoryResult {
    Scope { previous: Vec<(Option<u16>,bool)>, dual_hot: DualHotTransition },
    Retired(usize),
    Census { entries: usize, payload_capacity_bytes: usize },
}
#[derive(Debug,PartialEq,Eq)]
pub(crate) struct UcmHistoryReceipt {
    pub revision: u64,
    pub result: UcmHistoryResult,
}
pub(crate) struct UcmPendingHistory {
    pub(crate) ticket:u64,
    pub(crate) deadline:std::time::Instant,
    expected_revision: u64,
    requested_at: std::time::Instant,
    reply_clock:Option<std::sync::Arc<UcmReplyClock>>,
    reply: std::sync::mpsc::Receiver<Result<UcmHistoryReceipt,&'static str>>,
}
// R1402D: fixed-size RAM-only diagnostic counters. No per-frame log,
// additional channel, mutex, or change to admission/queue policy.
use std::sync::atomic::{AtomicU64,Ordering as UcmOrdering};
const UCM_PROFILE_NAMES:[&str;9]=["observe","transport","group","lifecycle","tick","history","snapshot","proposal","commit"];
struct UcmLatency {
 count:AtomicU64,sum:AtomicU64,max:AtomicU64,buckets:[AtomicU64;32],
}
impl Default for UcmLatency{
 fn default()->Self{Self{count:AtomicU64::new(0),sum:AtomicU64::new(0),max:AtomicU64::new(0),buckets:std::array::from_fn(|_|AtomicU64::new(0))}}
}
impl UcmLatency{
 fn add(&self,ns:u64){
  self.count.fetch_add(1,UcmOrdering::Relaxed);
  self.sum.fetch_add(ns,UcmOrdering::Relaxed);
  self.max.fetch_max(ns,UcmOrdering::Relaxed);
  let bucket=(64-ns.leading_zeros()).min(31) as usize;
  self.buckets[bucket].fetch_add(1,UcmOrdering::Relaxed);
 }
 fn json(&self)->serde_json::Value{
  let count=self.count.load(UcmOrdering::Relaxed);
  let quantile=|p:u64|{
   if count==0{return None}
   let target=(count*p+99)/100;let mut n=0;
   for(i,b)in self.buckets.iter().enumerate(){n+=b.load(UcmOrdering::Relaxed);if n>=target{return Some(if i==31{self.max.load(UcmOrdering::Relaxed)}else{(1u64<<i)-1})}}
   None
  };
  serde_json::json!({"count":count,"sum_ns":self.sum.load(UcmOrdering::Relaxed),"max_ns":self.max.load(UcmOrdering::Relaxed),"p50_upper_ns":quantile(50),"p95_upper_ns":quantile(95),"p99_upper_ns":quantile(99)})
 }
}
#[derive(Default)]
struct UcmOpProfile{
 queue:UcmLatency,service:UcmLatency,reply_wait:UcmLatency,roundtrip:UcmLatency,
 manager_phase:UcmLatency,publication_lock:UcmLatency,projection_refresh:UcmLatency,event_election:UcmLatency,tick_changed:AtomicU64,
 submitted:AtomicU64,full:AtomicU64,closed:AtomicU64,superseded:AtomicU64,timeout:AtomicU64,invalid:AtomicU64,reply_dropped:AtomicU64,
}
struct UcmProfile{ops:[UcmOpProfile;9],owner_tid:AtomicU64,working_marker:AtomicU64}
impl UcmProfile{
 fn new()->Self{Self{ops:std::array::from_fn(|_|UcmOpProfile::default()),owner_tid:AtomicU64::new(0),working_marker:AtomicU64::new(0)}}
 fn json(&self)->serde_json::Value{
  let mut rows=serde_json::Map::new();
  for(i,p)in self.ops.iter().enumerate(){let load=|v:&AtomicU64|v.load(UcmOrdering::Relaxed);rows.insert(UCM_PROFILE_NAMES[i].into(),serde_json::json!({
   "submitted":load(&p.submitted),"queue_full":load(&p.full),"queue_closed":load(&p.closed),"superseded":load(&p.superseded),"timeouts":load(&p.timeout),"invalid_replies":load(&p.invalid),"reply_dropped":load(&p.reply_dropped),
   "manager_phase":p.manager_phase.json(),"publication_lock":p.publication_lock.json(),"projection_refresh":p.projection_refresh.json(),"event_election":p.event_election.json(),"tick_changed_sources":load(&p.tick_changed),
   "mailbox_wait":p.queue.json(),"worker_service":p.service.json(),"reply_to_engine":p.reply_wait.json(),"request_roundtrip":p.roundtrip.json()
  }));}
  serde_json::Value::Object(rows)
 }
}
struct UcmReplyClock{submitted_ns:u64,sent_ns:AtomicU64}
// R1410I: optional owner-local flight recorder. Fixed records, no payload,
// allocation, shared lock or file IO on the command path. Only the diagnostic
// runner enables this and bounds the number of owner rings before spawning.
const UCM_JOURNAL_CAP:usize=1024;
struct UcmJournalRecord{
 ticket:u64,kind:usize,revision:u64,enqueued_ns:u64,
 receive_ns:u64,exchange_ms:i64,generation:u64,venue:u8,
 base:[u8;32],session:[u8;96],identity_truncated:bool,
 stamps:[[u64;3];11],recv_enter_ns:u64,recv_return_ns:u64,completion_before_ns:u64,notify_return_ns:u64,failed:bool,
}
fn journal_clock()->[u64;3]{
 let before=crate::ingress_runtime_r1205::mono_ns();
 let mut t=libc::timespec{tv_sec:0,tv_nsec:0};
 let cpu=if unsafe{libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID,&mut t)}==0{
  t.tv_sec as u64*1_000_000_000+t.tv_nsec as u64
 }else{0};
 [before,cpu,crate::ingress_runtime_r1205::mono_ns()]
}
fn journal_copy<const N:usize>(dst:&mut [u8;N],src:&str)->bool{
 let n=N.min(src.len());dst[..n].copy_from_slice(&src.as_bytes()[..n]);src.len()>N
}
fn journal_text(buf:&[u8])->String{
 String::from_utf8_lossy(&buf[..buf.iter().position(|b|*b==0).unwrap_or(buf.len())]).into_owned()
}
struct UcmJournal{
 path:std::path::PathBuf,rows:VecDeque<UcmJournalRecord>,total:u64,tid:u64,
}
impl UcmJournal{
 fn open()->Option<Self>{
  if std::env::var("R1410I_JOURNAL").ok().as_deref()!=Some("1"){return None}
  let path=std::path::PathBuf::from(std::env::var_os("R1410I_JOURNAL_DIR")?).canonicalize().ok()?;
  if path.parent()?.to_str()? != "/mnt/spotarb_disk2/SPOT_ARB_R1326_TRADE_TRIGGERED_DURABILITY_20261118/R1326E_LIVE_SHADOW"{return None}
  use std::os::unix::fs::MetadataExt;
  let md=std::fs::metadata(&path).ok()?;
  if !md.is_dir()||md.uid()!=unsafe{libc::geteuid()}||md.mode()&0o077!=0{return None}
  let tid=unsafe{libc::syscall(libc::SYS_gettid)} as u64;
  Some(Self{path:path.join(format!("ucm-{}-{tid}.jsonl",std::process::id())),rows:VecDeque::with_capacity(UCM_JOURNAL_CAP),total:0,tid})
 }
 fn flush(&self)->std::io::Result<()>{
  use std::io::Write;
  let file=std::fs::OpenOptions::new().create_new(true).write(true).open(&self.path)?;
  let mut w=std::io::BufWriter::new(file);
  writeln!(w,"{}",serde_json::json!({"schema":"R1410J/UCM/2","tid":self.tid,"total":self.total,"retained":self.rows.len(),"overwritten":self.total.saturating_sub(self.rows.len() as u64),"record_bytes":std::mem::size_of::<UcmJournalRecord>(),"capacity":UCM_JOURNAL_CAP,"stamps":["body_start_after_receive_and_identity_setup","publication_lock_start","projection_start","index_done","health_copy_done","quality_done","source_projection_done","election_start","election_done","reply_start","body_end_before_completion_ticket"],"clock_columns":["mono_before_ns","thread_cpu_ns","mono_after_ns"]}))?;
  for r in &self.rows{
   writeln!(w,"{}",serde_json::json!({"ticket":r.ticket,"kind":UCM_PROFILE_NAMES[r.kind],"revision":r.revision,"enqueued_ns":r.enqueued_ns,"receive_ns":r.receive_ns,"exchange_ms":r.exchange_ms,"generation":r.generation,"venue":r.venue,"base":journal_text(&r.base),"session":journal_text(&r.session),"identity_truncated":r.identity_truncated,"stamps":r.stamps,"recv_enter_ns":r.recv_enter_ns,"recv_return_ns":r.recv_return_ns,"completion_before_ns":r.completion_before_ns,"notify_return_ns":r.notify_return_ns,"failed":r.failed}))?;
  }
  w.flush()
 }
}
impl Drop for UcmJournal{fn drop(&mut self){if let Err(e)=self.flush(){eprintln!("R1410I_JOURNAL_FLUSH_ERROR {:?}: {}",self.path,e);}}}
struct UcmJournalSpan<'a>{journal:&'a mut UcmJournal,row:UcmJournalRecord}
impl<'a> UcmJournalSpan<'a>{
 fn new(journal:&'a mut UcmJournal,e:&UcmEnvelope,recv_enter_ns:u64,recv_return_ns:u64)->Self{
  let mut row=UcmJournalRecord{ticket:e.ticket,kind:e.kind,revision:0,enqueued_ns:e.enqueued_ns,receive_ns:0,exchange_ms:0,generation:0,venue:0,base:[0;32],session:[0;96],identity_truncated:false,stamps:[[0;3];11],recv_enter_ns,recv_return_ns,completion_before_ns:0,notify_return_ns:0,failed:false};
  match &e.command{
   UcmWorkerCommand::Event{revision,event,..}=>{
    row.revision=*revision;
    if let UcmWorkerEvent::Observe{source,received_ns,exchange_ms,..}=event{
     row.receive_ns=*received_ns;row.exchange_ms=*exchange_ms;row.generation=source.generation;
     row.venue=if source.exchange=="okx"{1}else if source.exchange=="binance"{2}else{0};
     row.identity_truncated=journal_copy(&mut row.base,&source.instrument)|journal_copy(&mut row.session,&source.logical_session);
    }
   },
   UcmWorkerCommand::History{revision,..}=>row.revision=*revision,
   UcmWorkerCommand::Snapshot{expected_revision,..}|UcmWorkerCommand::Propose{expected_revision,..}=>row.revision=*expected_revision,
  }
  row.stamps[0]=journal_clock();Self{journal,row}
 }
 fn mark(&mut self,n:usize){self.row.stamps[n]=journal_clock();}
}
impl Drop for UcmJournalSpan<'_>{fn drop(&mut self){
 self.journal.total+=1;
 if self.journal.rows.len()==UCM_JOURNAL_CAP{self.journal.rows.pop_front();}
 // Move the fixed record into the existing owner-local ring.
 let row=std::mem::replace(&mut self.row,UcmJournalRecord{ticket:0,kind:0,revision:0,enqueued_ns:0,receive_ns:0,exchange_ms:0,generation:0,venue:0,base:[0;32],session:[0;96],identity_truncated:false,stamps:[[0;3];11],recv_enter_ns:0,recv_return_ns:0,completion_before_ns:0,notify_return_ns:0,failed:false});
 self.journal.rows.push_back(row);
}}
fn journal_mark(span:&mut Option<UcmJournalSpan<'_>>,n:usize){if let Some(s)=span{s.mark(n);}}

// Immutable Engine eligibility facts carried by the original Observe.
#[derive(Clone,Debug)]
pub(crate) struct UcmBookFact{
    pub source:SourceId,
    pub advances:bool,
    pub selected:bool,
    pub equivalent_prior:bool,
}
#[derive(Clone,Debug)]
pub(crate) struct UcmBookFacts{
    pub anchor:SourceId,
    pub books:Vec<UcmBookFact>,
    pub now_ns:u64,
    pub now_ms:i64,
}
#[derive(Clone,Debug)]
pub(crate) struct UcmEventAdvice{
    pub revision:u64,
    pub ready:Vec<SourceId>,
    pub eligible:Vec<SourceId>,
    pub advice:UcmWorkerAdvice,
}
struct UcmEnvelope{facts:Vec<UcmBookFacts>,ticket:u64,command:UcmWorkerCommand,kind:usize,enqueued_ns:u64,reply_clock:Option<std::sync::Arc<UcmReplyClock>>}
struct UcmWorkSpan<'a>{profile:Option<&'a UcmOpProfile>,start_ns:u64,finished:bool,reply_clock:Option<std::sync::Arc<UcmReplyClock>>}
impl UcmWorkSpan<'_>{
 fn finish(&mut self){
  if self.finished{return}
  if let Some(p)=self.profile{
   let end=crate::ingress_runtime_r1205::mono_ns();
   p.service.add(end.saturating_sub(self.start_ns));
   if let Some(clock)=&self.reply_clock{clock.sent_ns.store(end,UcmOrdering::Release);}
  }
  self.finished=true;
 }
 fn dropped(&self){if let Some(p)=self.profile{p.reply_dropped.fetch_add(1,UcmOrdering::Relaxed);}}
}
impl Drop for UcmWorkSpan<'_>{fn drop(&mut self){self.finish();}}
enum UcmWorkerCommand {
    History {
        revision: u64, event: UcmHistoryEvent,
        reply: std::sync::mpsc::SyncSender<Result<UcmHistoryReceipt,&'static str>>,
    },
    Event {
        revision: u64, event: UcmWorkerEvent,
        ack: Option<std::sync::mpsc::SyncSender<Result<UcmWorkerSnapshot,&'static str>>>,
    },
    Snapshot {
        expected_revision: u64, only_scope:Option<UcmScopeKey>,
        reply: std::sync::mpsc::SyncSender<Result<UcmWorkerSnapshot,&'static str>>,
    },
    Propose {
        expected_revision: u64,only_scope:Option<UcmScopeKey>,
        eligible: Vec<SourceId>, now_ns: u64,
        now_ms: i64, reply: std::sync::mpsc::SyncSender<Result<UcmWorkerAdvice,&'static str>>,
    },
}

impl UcmWorkerCommand{
 fn kind(&self)->usize{
  match self{
   Self::Event{event,..}=>match event{
    UcmWorkerEvent::Observe{..}=>0,UcmWorkerEvent::TransportOnly{..}=>1,UcmWorkerEvent::Group{..}=>2,
    UcmWorkerEvent::Tick{..}=>4,UcmWorkerEvent::CommitActive{..}=>8,_=>3,
   },
   Self::History{..}=>5,Self::Snapshot{..}=>6,Self::Propose{..}=>7,
  }
 }
 fn has_reply(&self)->bool{!matches!(self,Self::Event{ack:None,..})}
}
/// A bounded query's reply may be polled WITHOUT stopping the Engine's
/// central Queue32 consumer. A late revision is never authority.
pub(crate) struct UcmPendingProposal {
    expected_revision: u64,
    scope_fence:Option<(UcmScopeKey,u64)>,
    requested_at: std::time::Instant,
    reply_clock:Option<std::sync::Arc<UcmReplyClock>>,
    reply: std::sync::mpsc::Receiver<Result<UcmWorkerAdvice,&'static str>>,
}
/// NO manager-active or Engine-selected publication before this applied
/// acknowledgment and the final Engine book/generation/dual-HOT safety gate.
pub(crate) struct UcmPendingCommit {
    pub(crate) destructive_revision:u64,
    expected_revision:u64,
    source:SourceId,
    requested_at:std::time::Instant,
    reply_clock:Option<std::sync::Arc<UcmReplyClock>>,
    reply:std::sync::mpsc::Receiver<Result<UcmWorkerSnapshot,&'static str>>,
}
pub(crate) struct UcmPendingSnapshot {
    expected_revision: u64,
    scope_fence: Option<(UcmScopeKey,u64)>,
    requested_at: std::time::Instant,
    reply_clock:Option<std::sync::Arc<UcmReplyClock>>,
    reply: std::sync::mpsc::Receiver<Result<UcmWorkerSnapshot,&'static str>>,
}

/// No detached worker and no unbounded mailbox. A failed enqueue, lost reply,
/// or out-of-order revision permanently invalidates this handle. A future
/// Engine caller MUST fail closed and revoke any source Permit on that error.
/// This is a testable OFFLINE building block, not an active Engine integration.
pub(crate) struct UcmTickPlan{
    ns:u64,wall:i64,revision:u64,
    fence:HashSet<UcmScopeKey>,
    pub(crate) evaluate:HashSet<UcmScopeKey>,
}
pub(crate) struct UcmWorker {
    sender: Option<std::sync::mpsc::SyncSender<UcmEnvelope>>,
    profile:Option<std::sync::Arc<UcmProfile>>,
    published:Option<std::sync::Arc<std::sync::Mutex<UcmPublished>>>,
    thread: Option<std::thread::JoinHandle<()>>,
    last_revision: u64,
    submitted_ticket:u64,
    completed_ticket:std::sync::Arc<AtomicU64>,
    // Producer holds only revision stamps, NOT mutable UCM/SourceHealth.
    scope_revisions: HashMap<UcmScopeKey,u64>,
    // Scope facts are fenced by the actual FIFO command, not by an
    // Engine-created read request. Fixed metadata slots track the existing
    // mailbox's in-flight command deadlines; no commands/frames are stored.
    scope_tickets:HashMap<UcmScopeKey,u64>,
    scope_ticket_order:std::collections::BTreeSet<(u64,UcmScopeKey)>,
    needs_facts:HashSet<UcmScopeKey>,
    receive_age_ns:u64,
    exchange_age_ms:i64,
    destructive_revisions:HashMap<UcmScopeKey,u64>,
    command_deadlines:Vec<Option<(u64,std::time::Instant,usize)>>,
    healthy: bool,
    owner_failed:std::sync::Arc<std::sync::atomic::AtomicBool>,
    response_timeout: Duration,
}

impl UcmWorker {
    pub(crate) fn start(policy: QualityPolicy, capacity: usize) -> Result<Self,String> {
        Self::start_owned(UniversalConnectionManager::new(policy),capacity,None)
    }

    /// Move the existing manager into the sole worker; no second manager.
    pub(crate) fn start_owned(manager:UniversalConnectionManager,capacity:usize,
        notify:Option<std::sync::Arc<tokio::sync::Notify>>)->Result<Self,String>{
        if capacity==0 || capacity>1024 {return Err("UCM worker capacity out of bounds".into());}
        let (tx,rx)=std::sync::mpsc::sync_channel(capacity);
        let owner_failed=std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let thread_failed=owner_failed.clone();
        let profile=(std::env::var("R1402_UCM_PROFILE").ok().as_deref()==Some("1")).then(||std::sync::Arc::new(UcmProfile::new()));
        let worker_profile=profile.clone();
        let receive_age_ns=manager.policy.max_receive_age.as_nanos() as u64;
        let exchange_age_ms=manager.policy.max_exchange_age_ms;
        let mut initial=UcmPublished::default();
        for id in manager.sources.keys(){initial.refresh_source(&manager,id,0);}
        let published=std::sync::Arc::new(std::sync::Mutex::new(initial));
        let owner_published=published.clone();
        let completed_ticket=std::sync::Arc::new(AtomicU64::new(0));
        let owner_completed=completed_ticket.clone();
        let handle=std::thread::Builder::new().name("spotarb-ucm-hot".into())
            .spawn(move || Self::run_owner(manager,rx,notify,thread_failed,worker_profile,owner_published,owner_completed))
            .map_err(|e|format!("UCM worker spawn failed: {e}"))?;
        Ok(Self{sender:Some(tx),profile,published:Some(published),thread:Some(handle),last_revision:0,submitted_ticket:0,completed_ticket,
            scope_revisions:HashMap::new(),scope_tickets:HashMap::new(),scope_ticket_order:Default::default(),needs_facts:HashSet::new(),receive_age_ns,exchange_age_ms,destructive_revisions:HashMap::new(),command_deadlines:vec![None;capacity+2],healthy:true,owner_failed,response_timeout:Duration::from_millis(50)})
    }

    fn source_live_for_event(manager:&UniversalConnectionManager,id:&SourceId,
        local_book_resets:&HashSet<SourceId>)->bool{
        manager.state(id).is_some_and(|h|
            h.websocket && h.state!=Some(Lifecycle::Closed) &&
            (!matches!(h.state,Some(Lifecycle::Reconnecting|Lifecycle::Quarantined))
                || local_book_resets.contains(id)))
    }

    pub(crate) fn revision(&self)->u64{self.last_revision}
    pub(crate) fn submitted_ticket(&self)->u64{self.submitted_ticket}
    pub(crate) fn completed_ticket(&self)->u64{self.completed_ticket.load(UcmOrdering::Acquire)}
    pub(crate) fn is_healthy(&self)->bool{self.healthy&&!self.owner_failed.load(std::sync::atomic::Ordering::Acquire)}
    pub(crate) fn view_current(&self,id:&SourceId,revision:u64)->bool{
        self.is_healthy() && self.scope_revisions.get(&scope(id)).copied().unwrap_or(0)<=revision
    }
    pub(crate) fn scope_ticket(&self,id:&SourceId)->u64{
        self.scope_tickets.get(&scope(id)).copied().unwrap_or(0)
    }
    // Timeout is the age of an actual unfinished owner command, never
    // the age of a scheduled Engine continuation or a moving read target.
    pub(crate) fn check_command_progress(&mut self)->Result<(),&'static str>{
        if !self.is_healthy(){return Err("UCM worker unavailable: fail closed")}
        let done=self.completed_ticket();
        if done>=self.submitted_ticket{return Ok(())}
        let next=done+1;
        let slot=self.command_deadlines[(next%self.command_deadlines.len() as u64)as usize];
        let Some((ticket,deadline,kind))=slot else{self.healthy=false;return Err("UCM command deadline missing: fail closed")};
        if ticket!=next{self.healthy=false;return Err("UCM command deadline mismatch: fail closed")}
        if std::time::Instant::now()>deadline&&self.completed_ticket()<next{
            self.healthy=false;
            // Failure-only evidence. The watchdog decision above is unchanged.
            if let Some(p)=&self.profile{
                // Ticket and phase are one atomic sample, never mixed
                // across two owner commands.
                let marker=p.working_marker.load(UcmOrdering::Acquire);
                let tid=p.owner_tid.load(UcmOrdering::Relaxed);
                let mut bases=self.scope_revisions.keys().map(|(_,_,base,_)|base.as_str()).collect::<Vec<_>>();
                bases.sort_unstable();bases.dedup();bases.truncate(8);
                eprintln!("R1410H_UCM_TIMEOUT {}",serde_json::json!({
                    "ticket":ticket,"kind":UCM_PROFILE_NAMES[kind],"submitted":self.submitted_ticket,"completed_at_check":done,
                    "completed_at_report":self.completed_ticket(),"overdue_us":deadline.elapsed().as_micros(),
                    "worker_ticket":marker>>3,"worker_phase":marker&7,
                    "owner_tid":tid,"owner_schedstat":std::fs::read_to_string(format!("/proc/self/task/{tid}/schedstat")).ok(),
                    "scope_bases_prefix":bases}));
            }
            return Err("UCM actual command timeout: fail closed")
        }
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn begin_published(&self)->UcmPendingPublished{
        UcmPendingPublished{requested_at:std::time::Instant::now()}
    }
    pub(crate) fn read_applied_scope(&mut self,id:&SourceId)
        ->Result<Option<UcmWorkerSnapshot>,&'static str>
    {
        if !self.is_healthy(){return Err("UCM worker unavailable: fail closed");}
        let key=scope(id);
        let required=self.scope_revisions.get(&key).copied().unwrap_or(0);
        let result=match self.published.as_ref().ok_or("UCM publication unavailable")?.try_lock(){
            Ok(p) if p.applied_revision>=required=>{
                match p.scopes.get(&key){
                    Some(view)=>{let mut view=view.clone();view.revision=p.applied_revision;Ok(Some(view))}
                    None=>Err("UCM published scope missing: fail closed"),
                }
            },
            Ok(_)|Err(std::sync::TryLockError::WouldBlock)=>Ok(None),
            Err(std::sync::TryLockError::Poisoned(_))=>Err("UCM publication poisoned: fail closed"),
        };
        if result.is_err(){self.healthy=false;}
        result
    }
    #[cfg(test)]
    pub(crate) fn poll_published(&mut self,id:&SourceId,pending:&UcmPendingPublished)
        ->Result<Option<UcmWorkerSnapshot>,&'static str>{
        let result=self.read_applied_scope(id);
        if matches!(result,Ok(None))&&pending.requested_at.elapsed()>self.response_timeout{
            self.healthy=false;return Err("UCM publication timeout: fail closed")
        }
        result
    }
    fn snapshot_view(manager:&UniversalConnectionManager,revision:u64,only_scope:Option<&UcmScopeKey>)->UcmWorkerSnapshot{
                        let sources=manager.sources.iter()
                            .filter(|(id,_)|only_scope.map_or(true,|wanted|scope(id)==*wanted))
                            .map(|(id,health)|{
                                (id.clone(),std::sync::Arc::new(UcmSourceReadView{
                                    health:health.clone(),
                                    quality:manager.quality(id),
                                    qualification_requirements:manager.qualification_requirements(id)
                                }))
                            }).collect();
                        let active=manager.active.iter()
                            .filter(|(k,_)|only_scope.map_or(true,|wanted|*k==wanted))
                            .map(|(k,v)|(k.clone(),v.clone())).collect();
                        UcmWorkerSnapshot{
                            revision,event_advice:None,hot_loss_revision:0,sources,active,
                            max_receive_age_ns:manager.policy.max_receive_age.as_nanos()
                                .min(u64::MAX as u128) as u64,
                            max_exchange_age_ms:manager.policy.max_exchange_age_ms,
                        }
    }
    fn run_owner(mut manager:UniversalConnectionManager,rx:std::sync::mpsc::Receiver<UcmEnvelope>,
        notify:Option<std::sync::Arc<tokio::sync::Notify>>,owner_failed:std::sync::Arc<std::sync::atomic::AtomicBool>,profile:Option<std::sync::Arc<UcmProfile>>,
        published:std::sync::Arc<std::sync::Mutex<UcmPublished>>,completed:std::sync::Arc<AtomicU64>) {
        struct OwnerExit<'a>{failed:&'a std::sync::atomic::AtomicBool,notify:Option<&'a tokio::sync::Notify>}
        impl Drop for OwnerExit<'_>{fn drop(&mut self){self.failed.store(true,std::sync::atomic::Ordering::Release);if let Some(n)=self.notify{n.notify_one();}}}
        let _exit=OwnerExit{failed:&owner_failed,notify:notify.as_deref()};
        // Wake the existing runtime loop after an applied command, including
        // error exits. Notify coalesces wakeups and carries no market data.
        // Publish completion AFTER the command and any reply send. Notify is
        // only a coalescing wake; the FIFO ticket is the actual progress index.
        struct Wake<'a>{notify:Option<&'a tokio::sync::Notify>,completed:&'a AtomicU64,ticket:u64,detail:Option<UcmJournalSpan<'a>>}
        impl Drop for Wake<'_>{fn drop(&mut self){
            if let Some(d)=&mut self.detail{d.mark(10);d.row.completion_before_ns=crate::ingress_runtime_r1205::mono_ns();}
            self.completed.store(self.ticket,UcmOrdering::Release);
            if let Some(n)=self.notify{n.notify_one();}
            if let Some(d)=&mut self.detail{d.row.notify_return_ns=crate::ingress_runtime_r1205::mono_ns();}
        }}

        let mut revision=0u64;
        let mut poisoned=false;
        // One owner distinguishes same-generation OKX book recovery from
        // physical retirement. Entries cannot outlive a physical lifecycle.
        let mut local_book_resets=HashSet::new();
        if let Some(p)=&profile{p.owner_tid.store(unsafe{libc::syscall(libc::SYS_gettid)} as u64,UcmOrdering::Relaxed);}
        // Optional existing profile: phase codes 1=manager/command body,
        // 2=publication lock, 3=projection, 4=election, 5=reply/completion.
        let mut journal=UcmJournal::open();
        loop {
            let recv_enter_ns=if journal.is_some(){crate::ingress_runtime_r1205::mono_ns()}else{0};
            let Ok(envelope)=rx.recv()else{break};
            let recv_return_ns=if journal.is_some(){crate::ingress_runtime_r1205::mono_ns()}else{0};
            let ticket=envelope.ticket;
            let phase=|n:u64|{if let Some(p)=&profile{p.working_marker.store(ticket.checked_mul(8).and_then(|v|v.checked_add(n)).unwrap_or(0),UcmOrdering::Release);}};
            let mut wake=Wake{notify:notify.as_deref(),completed:&completed,ticket:envelope.ticket,detail:None};
            let op=profile.as_ref().map(|p|&p.ops[envelope.kind]);
            let start_ns=if op.is_some(){crate::ingress_runtime_r1205::mono_ns()}else{0};
            if let Some(p)=op{p.queue.add(start_ns.saturating_sub(envelope.enqueued_ns));}
            phase(1);
            wake.detail=journal.as_mut().map(|j|UcmJournalSpan::new(j,&envelope,recv_enter_ns,recv_return_ns));
            let detail=&mut wake.detail;
            let mut span=UcmWorkSpan{profile:op,start_ns,finished:false,reply_clock:envelope.reply_clock};
            let facts=envelope.facts;
            match envelope.command {
                UcmWorkerCommand::Event{revision:next,event,ack}=>{
                    if poisoned || revision.checked_add(1)!=Some(next) {
                        poisoned=true;owner_failed.store(true,std::sync::atomic::Ordering::Release);
                        if let Some(tx)=ack{span.finish();if tx.send(Err("UCM event rejected: fail closed")).is_err(){span.dropped();}}
                        continue;
                    }
                    let commit_scope=if ack.is_some(){match &event{UcmWorkerEvent::CommitActive{source,..}=>Some(scope(source)),_=>None}}else{None};
                    let publish_ids=match &event{
                        UcmWorkerEvent::Group{ids,..}=>Some(ids.clone()),
                        UcmWorkerEvent::Connect(id)|UcmWorkerEvent::Subscribe(id)|UcmWorkerEvent::Reconnect(id)
                        |UcmWorkerEvent::Close(id)|UcmWorkerEvent::Quarantine(id)=>Some(vec![id.clone()]),
                        UcmWorkerEvent::Observe{source,..}|UcmWorkerEvent::TransportOnly{source,..}|UcmWorkerEvent::BookReset{source,..}=>Some(vec![source.clone()]),
                        UcmWorkerEvent::CommitActive{source,..}=>{
                            let mut ids=vec![source.clone()];
                            if let Some(old)=manager.active_for_scope(source){if old!=source{ids.push(old.clone());}}
                            Some(ids)
                        },
                        UcmWorkerEvent::Tick{..}=>None,
                    };
                    let mut tick_time=None;
                    let mut retired_sources=Vec::new();
                    let phase_start=op.map(|_|crate::ingress_runtime_r1205::mono_ns());
                    let mut result=match event {
                        UcmWorkerEvent::Group{ids,action}=>{
                            let unique=ids.iter().collect::<HashSet<_>>().len()==ids.len();
                            let same_connection=ids.first().is_some_and(|first|ids.iter().all(|id|
                                id.physical_connection==first.physical_connection&&id.generation==first.generation));
                            let valid=unique&&same_connection&&ids.iter().all(|id|match action{
                                UcmGroupAction::Connect{..}=>manager.state(id).is_none(),
                                UcmGroupAction::Subscribe=>Self::source_live_for_event(&manager,id,&local_book_resets),
                                _=>manager.state(id).is_some(),
                            });
                            let retirement=if valid&&matches!(action,UcmGroupAction::Connect{..}){
                                manager.retired_runtime_sources(&ids)
                            }else{Ok(Vec::new())};
                            if !valid{Err("UCM invalid connection group".into())}
                            else if let Err(error)=retirement{Err(error)}
                            else{
                                retired_sources=retirement.unwrap();
                                for id in &retired_sources{manager.sources.remove(id);local_book_resets.remove(id);}
                                for id in ids{match action{
                                    UcmGroupAction::Connect{auto_subscribe}=>{manager.connect(id.clone());if auto_subscribe{manager.subscribed(&id);}},
                                    UcmGroupAction::Subscribe=>manager.subscribed(&id),
                                    UcmGroupAction::Reconnect=>{local_book_resets.remove(&id);manager.reconnecting(&id);},
                                    UcmGroupAction::Close=>{local_book_resets.remove(&id);manager.close(&id);},
                                    UcmGroupAction::Quarantine=>{local_book_resets.remove(&id);manager.quarantine(&id);},
                                }}
                                Ok(())
                            }
                        },
                        UcmWorkerEvent::Connect(id)=>{
                            // A retired generation cannot be silently reopened.
                            if manager.state(&id).is_some(){Err("UCM duplicate source generation".into())}
                            else{match manager.retired_runtime_sources(std::slice::from_ref(&id)){
                                Ok(old)=>{retired_sources=old;for old in &retired_sources{manager.sources.remove(old);local_book_resets.remove(old);}manager.connect(id);Ok(())},
                                Err(error)=>Err(error),
                            }}
                        },
                        UcmWorkerEvent::Subscribe(id)=>{
                            if !Self::source_live_for_event(&manager,&id,&local_book_resets){
                                Err("UCM subscription for retired source".into())
                            }else{manager.subscribed(&id);Ok(())}
                        },
                        UcmWorkerEvent::Reconnect(id)=>{
                            if manager.state(&id).is_none(){Err("UCM unknown reconnect source".into())}
                            else{local_book_resets.remove(&id);manager.reconnecting(&id);Ok(())}
                        },
                        UcmWorkerEvent::Close(id)=>{
                            if manager.state(&id).is_none(){Err("UCM unknown closed source".into())}
                            else{local_book_resets.remove(&id);manager.close(&id);Ok(())}
                        },
                        UcmWorkerEvent::Quarantine(id)=>{
                            if manager.state(&id).is_none(){Err("UCM unknown quarantine source".into())}
                            else{local_book_resets.remove(&id);manager.quarantine(&id);Ok(())}
                        },
                        UcmWorkerEvent::BookReset{source,sequence_gap}=>{
                            if source.exchange!="okx" || source.segment!="spot" || source.channel!="books"
                                || !Self::source_live_for_event(&manager,&source,&local_book_resets) {
                                Err("UCM rejected book reset for nonlive or unsupported source".into())
                            }else{
                                if sequence_gap{manager.reconnecting(&source);}
                                else{manager.quarantine(&source);}
                                local_book_resets.insert(source);Ok(())
                            }
                        },
                        UcmWorkerEvent::Observe{source,received_ns,exchange_ms,
                            received_wall_ms,now_ns,now_ms,valid}=>{
                            if !Self::source_live_for_event(&manager,&source,&local_book_resets){
                                Err("UCM rejected late old-generation market event".into())
                            }else{
                                manager.observe_with_receive_wall(&source,received_ns,exchange_ms,
                                    received_wall_ms,now_ns,now_ms,valid);
                                if manager.state(&source).is_some_and(|h|
                                    !matches!(h.state,Some(Lifecycle::Reconnecting|Lifecycle::Quarantined))){
                                    local_book_resets.remove(&source);
                                }
                                Ok(())
                            }
                        },
                        UcmWorkerEvent::TransportOnly{source,received_ns,received_wall_ms,now_ns}=>{
                            if !Self::source_live_for_event(&manager,&source,&local_book_resets){
                                Err("UCM rejected old-generation transport keepalive".into())
                            }else{
                                manager.observe_transport_liveness(&source,received_ns,received_wall_ms,now_ns)
                            }
                        },
                        UcmWorkerEvent::Tick{now_ns,now_ms}=>{tick_time=Some((now_ns,now_ms));Ok(())},
                        UcmWorkerEvent::CommitActive{source,now_ns,now_ms}=>{
                            if manager.fresh(&source,now_ns,now_ms)&&manager.set_active(&source){
                                let key=scope(&source);
                                if manager.challenger.get(&key).is_some_and(|(id,_)|id==&source){manager.challenger.remove(&key);}
                                Ok(())
                            }else{Err("UCM active source stale or invalid".into())}
                        },
                    };
                    if tick_time.is_none(){if let (Some(p),Some(start))=(op,phase_start){p.manager_phase.add(crate::ingress_runtime_r1205::mono_ns().saturating_sub(start));}}
                    let mut applied_view=None;
                    if result.is_ok(){
                        let lock_start=op.map(|_|crate::ingress_runtime_r1205::mono_ns());
                        phase(2);journal_mark(detail,1);
                        match published.lock(){
                            Ok(mut projection)=>{
                                phase(3);journal_mark(detail,2);
                                if let (Some(p),Some(start))=(op,lock_start){p.publication_lock.add(crate::ingress_runtime_r1205::mono_ns().saturating_sub(start));}
                                let mut refresh_start=op.map(|_|crate::ingress_runtime_r1205::mono_ns());
                                for id in &retired_sources{
                                    projection.tick_index.sync(id,None);
                                    if let Some(view)=projection.scopes.get_mut(&scope(id)){view.sources.remove(id);}
                                }
                                if let Some(ids)=publish_ids{for id in ids{
                                    if envelope.kind==0{
                                        projection.refresh_source_measured(&manager,&id,next,|n|journal_mark(detail,n));
                                    }else{projection.refresh_source(&manager,&id,next);}
                                    let key=scope(&id);
                                    projection.challenge_deadline(&key,None);
                                    if let Some(v)=projection.scopes.get_mut(&key){v.event_advice=None;}
                                }}else{
                                    let (ns,wall)=tick_time.expect("Tick clocks");
                                    let tick_start=op.map(|_|crate::ingress_runtime_r1205::mono_ns());
                                    let tick_changed=projection.tick_index.tick(&mut manager,ns,wall);
                                    if let (Some(p),Some(start))=(op,tick_start){p.manager_phase.add(crate::ingress_runtime_r1205::mono_ns().saturating_sub(start));}
                                    refresh_start=op.map(|_|crate::ingress_runtime_r1205::mono_ns());
                                    projection.refresh_tick(&manager,next,&tick_changed);
                                    if let Some(p)=op{p.tick_changed.fetch_add(tick_changed.len() as u64,UcmOrdering::Relaxed);}
                                }
                                if let (Some(p),Some(start))=(op,refresh_start){p.projection_refresh.add(crate::ingress_runtime_r1205::mono_ns().saturating_sub(start));}
                                phase(4);journal_mark(detail,7);
                                let election_start=op.map(|_|crate::ingress_runtime_r1205::mono_ns());
                                for facts in facts{
                                    let key=scope(&facts.anchor);
                                    let ready=facts.books.iter().filter(|b|manager.fresh(&b.source,facts.now_ns,facts.now_ms)).map(|b|b.source.clone()).collect::<Vec<_>>();
                                    let prior_ready=facts.books.iter().any(|b|b.selected&&ready.contains(&b.source));
                                    let eligible=facts.books.iter().filter(|b|ready.contains(&b.source)&&(b.selected||b.advances||(!prior_ready&&b.equivalent_prior))).map(|b|b.source.clone()).collect::<Vec<_>>();
                                    // A published recommendation is not an applied
                                    // switch. Preserve a matured dwell while the
                                    // exact improvement continues; only actual
                                    // commit (or a new contrary election) ends it.
                                    let prior_challenger=manager.challenger.get(&key).cloned();
                                    // Facts are validated to belong to this exact scope.
                                    // Reuse its lookup for both election and proof.
                                    let view=projection.scopes.get(&key);
                                    let quality=|id:&SourceId|view.and_then(|v|v.sources.get(id)).and_then(|v|v.quality);
                                    let candidate=if ready.len()>=2&&!eligible.is_empty(){
                                        manager.choose_candidate_with_quality(&eligible,facts.now_ns,facts.now_ms,|_,id|
                                            quality(id))
                                    }else{None};
                                    if let Some((id,"sustained_quality_improvement"))=candidate.as_ref(){
                                        if let Some((challenger,at))=prior_challenger.filter(|(challenger,_)|challenger==id){
                                            manager.challenger.insert(key.clone(),(challenger,at));
                                        }
                                    }
                                    let stable=candidate.as_ref().and_then(|(id,reason)|{
                                        if manager.active_for_scope(id)!=Some(id)||manager.challenger.contains_key(&key)
                                            ||!matches!(*reason,"active_remains_best"|"hysteresis_hold"){return None}
                                        Some(UcmStableChoice{candidate:id.clone(),reason:*reason,eligible:eligible.iter().map(|x|(x.clone(),quality(x))).collect()})
                                    });
                                    let deadline=match candidate.as_ref().map(|(_,reason)|*reason){
                                        Some("hysteresis_hold")|Some("sustained_quality_improvement")=>{
                                            manager.challenger.get(&key).map(|(_,at)|(at.saturating_add(manager.policy.switch_dwell.as_nanos().min(u64::MAX as u128)as u64),candidate.as_ref().is_some_and(|(_,r)|*r=="sustained_quality_improvement")))
                                        },_=>None,
                                    };
                                    projection.challenge_deadline(&key,deadline);
                                    let advice=UcmEventAdvice{revision:next,ready,eligible,advice:UcmWorkerAdvice{revision:next,candidate,stable}};
                                    if let Some(view)=projection.scopes.get_mut(&key){view.event_advice=Some(advice);}
                                    else{result=Err("UCM event advice scope missing".into());}
                                }
                                if let (Some(p),Some(start))=(op,election_start){p.event_election.add(crate::ingress_runtime_r1205::mono_ns().saturating_sub(start));}
                                journal_mark(detail,8);
                                projection.applied_revision=next;
                                if let Some(key)=&commit_scope{
                                    applied_view=projection.scopes.get(key).cloned().map(|mut v|{v.revision=next;v});
                                }
                            },
                            Err(_)=>result=Err("UCM publication lock poisoned".into()),
                        }
                    }
                    if result.is_err(){poisoned=true;owner_failed.store(true,std::sync::atomic::Ordering::Release);}else{revision=next;}
                    phase(5);journal_mark(detail,9);
                    if result.is_err(){if let Some(d)=detail{d.row.failed=true;}}
                    if let Some(tx)=ack {
                        let receipt=if result.is_ok(){applied_view.ok_or("UCM commit missing scope: fail closed")}
                            else{Err("UCM commit rejected: fail closed")};
                        span.finish();
                        if tx.send(receipt).is_err(){span.dropped();}
                    }
                }
                UcmWorkerCommand::History{revision:next,event,reply}=>{
                    if poisoned || revision.checked_add(1)!=Some(next) {
                        poisoned=true;owner_failed.store(true,std::sync::atomic::Ordering::Release);
                        span.finish();if reply.send(Err("UCM history revision gap: fail closed")).is_err(){span.dropped();}
                        continue;
                    }
                    let result=match event {
                        UcmHistoryEvent::Scope{scope,masks,dual_hot,now_ns}=>{
                            let previous=masks.into_iter().map(|(session,mask)|
                                manager.observe_readiness_mask(&scope,&session,mask)).collect();
                            UcmHistoryResult::Scope{previous,
                                dual_hot:manager.observe_dual_hot(&scope,dual_hot,now_ns)}
                        },
                        UcmHistoryEvent::RetireSession{session}=>
                            UcmHistoryResult::Retired(manager.retire_readiness_session(&session)),
                        UcmHistoryEvent::Census=>UcmHistoryResult::Census{
                            entries:manager.readiness_previous_len(),
                            payload_capacity_bytes:manager.readiness_previous_payload_capacity_bytes()},
                    };
                    revision=next;
                    // The applied history must never be silently lost.
                    phase(5);journal_mark(detail,9);span.finish();if reply.send(Ok(UcmHistoryReceipt{revision,result})).is_err(){span.dropped();poisoned=true;owner_failed.store(true,std::sync::atomic::Ordering::Release);}
                },
                UcmWorkerCommand::Snapshot{expected_revision,only_scope,reply}=>{
                    let result=if poisoned {
                        Err("UCM worker snapshot poisoned: fail closed")
                    }else if expected_revision!=revision{
                        Err("UCM worker snapshot revision mismatch: fail closed")
                    }else{
                        Ok(Self::snapshot_view(&manager,revision,only_scope.as_ref()))
                    };
                    span.finish();if reply.send(result).is_err(){span.dropped();}
                },
                UcmWorkerCommand::Propose{expected_revision,only_scope,eligible,now_ns,now_ms,reply}=>{
                    let result=if poisoned {
                        Err("UCM worker poisoned: fail closed")
                    }else if revision!=expected_revision {
                        Err("UCM worker revision mismatch: fail closed")
                    }else if only_scope.as_ref().is_some_and(|wanted|
                        eligible.iter().any(|id|scope(id)!=*wanted)){
                        Err("UCM scoped proposal included foreign source")
                    }else{
                        match published.lock(){
                            Ok(mut p)=>{
                                if let Some(key)=only_scope.as_ref(){if let Some(v)=p.scopes.get_mut(key){v.event_advice=None;}}
                                else{for v in p.scopes.values_mut(){v.event_advice=None;}}
                                // The owner updates the quality projection before
                                // processing another command. History, Tick and
                                // election do not change the quality windows.
                                if eligible.iter().any(|id|manager.state(id).is_some()&&p.scopes.get(&scope(id)).and_then(|v|v.sources.get(id)).is_none()){
                                    poisoned=true;owner_failed.store(true,std::sync::atomic::Ordering::Release);
                                    Err("UCM quality projection missing: fail closed")
                                }else{
                                    let candidate=manager.choose_candidate_with_quality(&eligible,now_ns,now_ms,|_,id|
                                        p.scopes.get(&scope(id)).and_then(|v|v.sources.get(id)).and_then(|v|v.quality));
                                    let stable=candidate.as_ref().and_then(|(id,reason)|{
                                        let key=scope(id);
                                        if manager.active_for_scope(id)!=Some(id)||manager.challenger.contains_key(&key)
                                            ||!matches!(*reason,"active_remains_best"|"hysteresis_hold")
                                            ||!eligible.iter().all(|x|manager.fresh(x,now_ns,now_ms)){return None}
                                        Some(UcmStableChoice{candidate:id.clone(),reason:*reason,
                                            eligible:eligible.iter().map(|x|(x.clone(),p.scopes.get(&scope(x))
                                                .and_then(|v|v.sources.get(x)).and_then(|v|v.quality))).collect()})
                                    });
                                    Ok(UcmWorkerAdvice{revision,candidate,stable})
                                }
                            },
                            Err(_)=>{poisoned=true;owner_failed.store(true,std::sync::atomic::Ordering::Release);Err("UCM quality projection poisoned: fail closed")}
                        }
                    };
                    span.finish();if reply.send(result).is_err(){span.dropped();}
                },
            }
        }
    }

    fn submit(&mut self,command:UcmWorkerCommand)->Result<Option<std::sync::Arc<UcmReplyClock>>,&'static str>{
        self.submit_with_facts(command,Vec::new())
    }
    fn submit_with_facts(&mut self,command:UcmWorkerCommand,facts:Vec<UcmBookFacts>)->Result<Option<std::sync::Arc<UcmReplyClock>>,&'static str>{
        if !self.is_healthy(){return Err("UCM worker unavailable: fail closed");}
        let kind=command.kind();
        let Some(sender)=self.sender.as_ref()else{
            self.healthy=false;return Err("UCM worker disconnected: fail closed")
        };
        let op=self.profile.as_ref().map(|p|&p.ops[kind]);
        let enqueued_ns=if op.is_some(){crate::ingress_runtime_r1205::mono_ns()}else{0};
        let reply_clock=(op.is_some()&&command.has_reply()).then(||std::sync::Arc::new(UcmReplyClock{submitted_ns:enqueued_ns,sent_ns:AtomicU64::new(0)}));
        let Some(ticket)=self.submitted_ticket.checked_add(1)else{self.healthy=false;return Err("UCM command ticket exhausted: fail closed")};
        let slot=(ticket%self.command_deadlines.len() as u64)as usize;
        if self.command_deadlines[slot].is_some_and(|(old,_,_)|old>self.completed_ticket()){
            self.healthy=false;return Err("UCM in-flight command bound exceeded: fail closed")
        }
        let deadline=std::time::Instant::now()+self.response_timeout;
        match sender.try_send(UcmEnvelope{facts,ticket,command,kind,enqueued_ns,reply_clock:reply_clock.clone()}){
         Ok(())=>{self.command_deadlines[slot]=Some((ticket,deadline,kind));self.submitted_ticket=ticket;if let Some(p)=op{p.submitted.fetch_add(1,UcmOrdering::Relaxed);}Ok(reply_clock)},
         Err(error)=>{
          if let Some(p)=op{match error{
           std::sync::mpsc::TrySendError::Full(_)=>{p.full.fetch_add(1,UcmOrdering::Relaxed);},
           std::sync::mpsc::TrySendError::Disconnected(_)=>{p.closed.fetch_add(1,UcmOrdering::Relaxed);}
          }}
          self.healthy=false;Err("UCM worker queue full or closed: fail closed")
         }
        }
    }
    fn record_reply(&self,kind:usize,clock:&Option<std::sync::Arc<UcmReplyClock>>){
      if let (Some(profile),Some(clock))=(&self.profile,clock){
       let now=crate::ingress_runtime_r1205::mono_ns();
       let sent=clock.sent_ns.load(UcmOrdering::Acquire);
       if sent!=0{profile.ops[kind].reply_wait.add(now.saturating_sub(sent));}
       profile.ops[kind].roundtrip.add(now.saturating_sub(clock.submitted_ns));
      }
    }
    fn record_reply_outcome(&self,kind:usize,outcome:&str){
     if let Some(p)=self.profile.as_ref().map(|p|&p.ops[kind]){
      match outcome{"superseded"=>&p.superseded,"timeout"=>&p.timeout,_=>&p.invalid}.fetch_add(1,UcmOrdering::Relaxed);
     }
    }

    fn stamp_scope(&mut self,key:UcmScopeKey,revision:u64,destructive:bool){
        if let Some(old)=self.scope_tickets.insert(key.clone(),self.submitted_ticket){self.scope_ticket_order.remove(&(old,key.clone()));}
        self.scope_ticket_order.insert((self.submitted_ticket,key.clone()));
        self.scope_revisions.insert(key.clone(),revision);
        if destructive{self.destructive_revisions.insert(key,revision);}
    }
    // One authoritative published deadline index, no producer scan of
    // SourceHealth. Pending facts are conservatively fenced, as before.
    pub(crate) fn tick_plan(&self,ns:u64,wall:i64)->UcmTickPlan{
        let done=self.completed_ticket();
        let mut fence=self.scope_ticket_order.iter().rev().take_while(|(ticket,_)|*ticket>done).map(|(_,k)|k.clone()).collect::<HashSet<_>>();
        let mut evaluate=self.needs_facts.clone();
        if let Some(p)=self.published.as_ref().and_then(|p|p.try_lock().ok()){
            // Publication may precede the completion notification. The
            // applied revision, not a delayed wake, proves these facts.
            fence.retain(|key|self.scope_revisions.get(key).is_some_and(|at|*at>p.applied_revision));
            fence.extend(p.tick_index.take_due(ns,wall,self.receive_age_ns,self.exchange_age_ms).iter().map(scope));
            evaluate.extend(p.challenge_deadlines.iter().take_while(|(at,_)|*at<=ns).map(|(_,k)|k.clone()));
            evaluate.extend(p.challenge_matured.iter().rev().take_while(|(at,_)|*at>ns).map(|(_,k)|k.clone()));
        }else{
            // Cannot certify the current owner projection. Retain the old
            // conservative fallback, without blocking the central consumer.
            fence.extend(self.scope_revisions.keys().cloned());
        }
        evaluate.extend(fence.iter().cloned());
        UcmTickPlan{ns,wall,revision:self.last_revision,fence,evaluate}
    }
    pub(crate) fn apply_certified_tick(&mut self,revision:u64,now_ns:u64,now_ms:i64)
        ->Result<(),&'static str>
    {
        self.apply_tick_with_facts(revision,now_ns,now_ms,Vec::new())
    }
    pub(crate) fn apply_tick_with_facts(&mut self,revision:u64,now_ns:u64,now_ms:i64,facts:Vec<UcmBookFacts>)
        ->Result<(),&'static str>
    {
        let plan=self.tick_plan(now_ns,now_ms);
        self.apply_planned_tick(revision,plan,facts)
    }
    pub(crate) fn apply_planned_tick(&mut self,revision:u64,plan:UcmTickPlan,facts:Vec<UcmBookFacts>)->Result<(),&'static str>{
        let (now_ns,now_ms)=(plan.ns,plan.wall);
        if self.last_revision!=plan.revision{self.healthy=false;return Err("UCM Tick plan revision mismatch: fail closed")}
        let mut fact_scopes=HashSet::new();
        if !facts.iter().all(|f|f.now_ns==now_ns&&f.now_ms==now_ms&&fact_scopes.insert(scope(&f.anchor))
            &&f.books.iter().all(|b|scope(&b.source)==scope(&f.anchor))
            &&f.books.iter().map(|b|&b.source).collect::<HashSet<_>>().len()==f.books.len()
            &&f.books.iter().filter(|b|b.selected).count()<=1){
            self.healthy=false;return Err("UCM invalid Tick eligibility facts: fail closed")
        }
        if self.last_revision.checked_add(1)!=Some(revision){
            self.healthy=false;return Err("UCM tick producer revision mismatch: fail closed")
        }
        self.submit_with_facts(UcmWorkerCommand::Event{revision,event:UcmWorkerEvent::Tick{now_ns,now_ms},ack:None},facts)?;
        self.last_revision=revision;
        let mut affected=plan.fence.clone();
        affected.extend(fact_scopes.iter().cloned());
        // A planned Tick fences current reads, including uncertain or
        // overdue facts. It is not proof of an applied HOT loss. The owner
        // records actual loss in hot_loss_revision; reconciliation must
        // wait for this Tick's current applied view before granting.
        for key in affected{self.stamp_scope(key,revision,false);}
        for key in plan.evaluate.iter().chain(fact_scopes.iter()){self.needs_facts.remove(key);}
        Ok(())
    }
    pub(crate) fn apply(&mut self,revision:u64,event:UcmWorkerEvent)
        ->Result<(),&'static str>
    {
        self.apply_with_book_facts(revision,event,None)
    }
    pub(crate) fn apply_with_book_facts(&mut self,revision:u64,event:UcmWorkerEvent,facts:Option<UcmBookFacts>)
        ->Result<(),&'static str>
    {
        if let Some(f)=facts.as_ref(){
            let valid=matches!(&event,UcmWorkerEvent::Observe{source,valid:true,now_ns,now_ms,..}
                if source==&f.anchor&&*now_ns==f.now_ns&&*now_ms==f.now_ms)
                &&f.books.iter().all(|b|scope(&b.source)==scope(&f.anchor))
                &&f.books.iter().map(|b|&b.source).collect::<HashSet<_>>().len()==f.books.len()
                &&f.books.iter().filter(|b|b.selected).count()<=1;
            if !valid{self.healthy=false;return Err("UCM invalid ordered book eligibility facts: fail closed")}
        }
        if self.last_revision.checked_add(1)!=Some(revision){
            self.healthy=false;return Err("UCM producer revision mismatch: fail closed");
        }
        let touched=event.affected_scopes();
        let destructive=!matches!(&event,UcmWorkerEvent::Observe{valid:true,..}|UcmWorkerEvent::TransportOnly{..});
        let has_facts=facts.is_some();
        self.submit_with_facts(UcmWorkerCommand::Event{revision,event,ack:None},facts.into_iter().collect())?;
        self.last_revision=revision;
        if let Some(keys)=touched {
            for key in keys{
                if has_facts{self.needs_facts.remove(&key);}else{self.needs_facts.insert(key.clone());}
                self.stamp_scope(key,revision,destructive);
            }
        }else{
            // Original Tick can expire any instrument.
            for key in self.scope_revisions.keys().cloned().collect::<Vec<_>>(){self.stamp_scope(key,revision,true);}
        }
        Ok(())
    }

    /// History shares the same ordered mailbox. It does not affect
    /// freshness, quality, election state or scoped source fences.
    pub(crate) fn begin_history(&mut self,revision:u64,event:UcmHistoryEvent)
        ->Result<UcmPendingHistory,&'static str>
    {
        if self.last_revision.checked_add(1)!=Some(revision){
            self.healthy=false;return Err("UCM history producer revision gap: fail closed");
        }
        let (tx,rx)=std::sync::mpsc::sync_channel(1);
        let reply_clock=self.submit(UcmWorkerCommand::History{revision,event,reply:tx})?;
        self.last_revision=revision;
        Ok(UcmPendingHistory{ticket:self.submitted_ticket,deadline:std::time::Instant::now()+self.response_timeout,expected_revision:revision,
            requested_at:std::time::Instant::now(),reply_clock,reply:rx})
    }

    /// Historical receipts remain valid after subsequent source events:
    /// they describe the earlier applied transition, not current HOT state.
    /// Caller must consume receipts in submission order for audit rendering.
    pub(crate) fn poll_history(&mut self,pending:&mut UcmPendingHistory)
        ->Result<Option<UcmHistoryReceipt>,&'static str>
    {
        if !self.is_healthy(){return Err("UCM worker unavailable: fail closed");}
        match pending.reply.try_recv(){
            Ok(Ok(receipt)) if receipt.revision==pending.expected_revision=>{self.record_reply(5,&pending.reply_clock);Ok(Some(receipt))},
            Ok(_) | Err(std::sync::mpsc::TryRecvError::Disconnected)=>{
                self.record_reply_outcome(5,"invalid");self.healthy=false;Err("UCM history receipt invalid: fail closed")
            },
            Err(std::sync::mpsc::TryRecvError::Empty)=>{
                if pending.requested_at.elapsed()>self.response_timeout{
                    self.record_reply_outcome(5,"timeout");self.healthy=false;Err("UCM history timeout: fail closed")
                }else{Ok(None)}
            },
        }
    }

    /// Submit actual source selection to the ONE manager owner, without
    /// waiting inside Queue32. The pending receipt is not a Permit.
    pub(crate) fn begin_commit(&mut self,revision:u64,source:SourceId,
        now_ns:u64,now_ms:i64)->Result<UcmPendingCommit,&'static str>
    {
        if self.last_revision.checked_add(1)!=Some(revision){
            self.healthy=false;return Err("UCM commit revision gap: fail closed");
        }
        let key=scope(&source);
        let (tx,rx)=std::sync::mpsc::sync_channel(1);
        let reply_clock=self.submit(UcmWorkerCommand::Event{
            revision,event:UcmWorkerEvent::CommitActive{
                source:source.clone(),now_ns,now_ms},ack:Some(tx),
        })?;
        self.last_revision=revision;
        self.needs_facts.insert(key.clone());
        self.stamp_scope(key,revision,false);
        Ok(UcmPendingCommit{
            destructive_revision:self.destructive_revision(&source),
            expected_revision:revision,source,
            requested_at:std::time::Instant::now(),reply_clock,reply:rx})
    }

    /// Pending => NO authority, never wait/spin in Engine's central loop.
    /// Receipt can be used only if no later event invalidated this source
    /// scope and Engine independently rechecks its current book and 800ms.
    pub(crate) fn destructive_revision(&self,source:&SourceId)->u64{
        self.destructive_revisions.get(&scope(source)).copied().unwrap_or(0)
    }
    // Actual receipt is historical evidence, not permission to publish.
    pub(crate) fn poll_applied_commit_receipt(&mut self,pending:&mut UcmPendingCommit)
        ->Result<Option<UcmWorkerSnapshot>,&'static str>
    {
        self.poll_commit_inner(pending,false)
    }
    pub(crate) fn poll_commit(&mut self,pending:&mut UcmPendingCommit)
        ->Result<Option<UcmWorkerSnapshot>,&'static str>
    {
        self.poll_commit_inner(pending,true)
    }
    fn poll_commit_inner(&mut self,pending:&mut UcmPendingCommit,strict:bool)
        ->Result<Option<UcmWorkerSnapshot>,&'static str>
    {
        if !self.is_healthy(){return Err("UCM worker unavailable: fail closed");}
        if strict&&self.scope_revisions.get(&scope(&pending.source)).copied()
            !=Some(pending.expected_revision){
            self.record_reply_outcome(8,"superseded");
            return Err("UCM commit superseded by newer relevant event: reject");
        }
        match pending.reply.try_recv(){
            Ok(Ok(applied)) if applied.revision==pending.expected_revision&&applied.active.get(&scope(&pending.source))==Some(&pending.source)=>{self.record_reply(8,&pending.reply_clock);Ok(Some(applied))},
            Ok(_) | Err(std::sync::mpsc::TryRecvError::Disconnected)=>{
                self.record_reply_outcome(8,"invalid");self.healthy=false;Err("UCM commit rejected or disconnected: fail closed")
            },
            Err(std::sync::mpsc::TryRecvError::Empty)=>{
                if pending.requested_at.elapsed()>self.response_timeout{
                    self.record_reply_outcome(8,"timeout");self.healthy=false;Err("UCM commit timeout: fail closed")
                }else{Ok(None)}
            },
        }
    }

    /// Request a read-only UCM health/quality snapshot. No waiting on
    /// Queue32 consumer: Engine may process new input while the worker fills
    /// the view. The query is serialized BEHIND all preceding UCM events.
    pub(crate) fn begin_snapshot(&mut self,expected_revision:u64)
        ->Result<UcmPendingSnapshot,&'static str>
    {
        self.begin_snapshot_inner(expected_revision,None)
    }
    /// Snapshot of ONE instrument. Other instruments can advance without
    /// revoking its read view; close or tick of this scope still revokes it.
    pub(crate) fn begin_scoped_snapshot(&mut self,expected_revision:u64,id:&SourceId)
        ->Result<UcmPendingSnapshot,&'static str>
    {
        self.begin_snapshot_inner(expected_revision,Some(scope(id)))
    }
    fn begin_snapshot_inner(&mut self,expected_revision:u64,only_scope:Option<UcmScopeKey>)
        ->Result<UcmPendingSnapshot,&'static str>
    {
        if self.last_revision!=expected_revision{
            self.healthy=false;return Err("UCM snapshot query revision mismatch: fail closed");
        }
        let scope_fence=only_scope.as_ref().map(|k|
            (k.clone(),self.scope_revisions.get(k).copied().unwrap_or(0)));
        let (tx,rx)=std::sync::mpsc::sync_channel(1);
        let reply_clock=self.submit(UcmWorkerCommand::Snapshot{
            expected_revision,only_scope,reply:tx})?;
        Ok(UcmPendingSnapshot{
            expected_revision,scope_fence,
            requested_at:std::time::Instant::now(),reply_clock,reply:rx})
    }

    pub(crate) fn poll_snapshot(&mut self,pending:&mut UcmPendingSnapshot)
        ->Result<Option<UcmWorkerSnapshot>,&'static str>
    {
        if !self.is_healthy(){return Err("UCM worker unavailable: fail closed");}
        let superseded=if let Some((k,at))=pending.scope_fence.as_ref(){
            self.scope_revisions.get(k).copied().unwrap_or(0)!=*at
        }else{self.last_revision!=pending.expected_revision};
        if superseded{
            self.record_reply_outcome(6,"superseded");
            return Err("UCM snapshot superseded by relevant event: reject");
        }
        match pending.reply.try_recv(){
            Ok(Ok(view)) if view.revision==pending.expected_revision=>{self.record_reply(6,&pending.reply_clock);Ok(Some(view))},
            Ok(_) | Err(std::sync::mpsc::TryRecvError::Disconnected)=>{
                self.record_reply_outcome(6,"invalid");self.healthy=false;Err("UCM snapshot invalid or disconnected: fail closed")
            },
            Err(std::sync::mpsc::TryRecvError::Empty)=>{
                if pending.requested_at.elapsed()>self.response_timeout {
                    self.record_reply_outcome(6,"timeout");self.healthy=false;Err("UCM snapshot timed out: fail closed")
                }else{Ok(None)}
            },
        }
    }

    /// Production-compatible NONBLOCKING query submission. No awaited worker
    /// reply here: Engine can keep draining/validating the existing Queue32.
    /// UCM owns only the advisory election; Engine MUST recheck source books,
    /// generation, dual-HOT and 800ms freshness before any Permit.
    pub(crate) fn begin_proposal(&mut self,expected_revision:u64,eligible:Vec<SourceId>,
        now_ns:u64,now_ms:i64)->Result<UcmPendingProposal,&'static str>
    {
        self.begin_proposal_inner(expected_revision,None,eligible,now_ns,now_ms)
    }
    pub(crate) fn begin_scoped_proposal(&mut self,expected_revision:u64,
        id:&SourceId,eligible:Vec<SourceId>,now_ns:u64,now_ms:i64)
        ->Result<UcmPendingProposal,&'static str>
    {
        self.begin_proposal_inner(expected_revision,Some(scope(id)),eligible,now_ns,now_ms)
    }
    fn begin_proposal_inner(&mut self,expected_revision:u64,
        only_scope:Option<UcmScopeKey>,eligible:Vec<SourceId>,
        now_ns:u64,now_ms:i64)->Result<UcmPendingProposal,&'static str>
    {
        if self.last_revision!=expected_revision{
            self.healthy=false;return Err("UCM proposal revision mismatch: fail closed");
        }
        if only_scope.as_ref().is_some_and(|wanted|eligible.iter().any(|id|scope(id)!=*wanted)){
            self.healthy=false;return Err("UCM scoped proposal foreign source: fail closed");
        }
        let scope_fence=only_scope.as_ref().map(|k|
            (k.clone(),self.scope_revisions.get(k).copied().unwrap_or(0)));
        let (tx,rx)=std::sync::mpsc::sync_channel(1);
        let reply_clock=self.submit(UcmWorkerCommand::Propose{
            expected_revision,only_scope,eligible,now_ns,now_ms,reply:tx})?;
        Ok(UcmPendingProposal{
            expected_revision,scope_fence,
            requested_at:std::time::Instant::now(),reply_clock,reply:rx})
    }

    /// Ok(None) means PENDING, NEVER an authorized proposal. A future
    /// Engine integration must withhold source admission, not spin or wait.
    pub(crate) fn poll_proposal(&mut self,pending:&mut UcmPendingProposal)
        ->Result<Option<UcmWorkerAdvice>,&'static str>
    {
        if !self.is_healthy(){return Err("UCM worker unavailable: fail closed");}
        let superseded=if let Some((k,at))=pending.scope_fence.as_ref(){
            self.scope_revisions.get(k).copied().unwrap_or(0)!=*at
        }else{self.last_revision!=pending.expected_revision};
        if superseded{
            self.record_reply_outcome(7,"superseded");
            return Err("UCM proposal superseded by relevant event: reject");
        }
        match pending.reply.try_recv() {
            Ok(Ok(advice)) if advice.revision==pending.expected_revision=>{self.record_reply(7,&pending.reply_clock);Ok(Some(advice))},
            Ok(_) | Err(std::sync::mpsc::TryRecvError::Disconnected)=>{
                self.record_reply_outcome(7,"invalid");self.healthy=false;Err("UCM worker invalid or disconnected: fail closed")
            }
            Err(std::sync::mpsc::TryRecvError::Empty)=>{
                if pending.requested_at.elapsed()>self.response_timeout{
                    self.record_reply_outcome(7,"timeout");self.healthy=false;Err("UCM worker proposal timed out: fail closed")
                }else{Ok(None)}
            }
        }
    }

    /// Offline-only blocking parity helper. NEVER call from Engine main loop.
    #[cfg(test)]
    pub(crate) fn propose(&mut self,expected_revision:u64,eligible:Vec<SourceId>,
        now_ns:u64,now_ms:i64)->Result<UcmWorkerAdvice,&'static str>
    {
        let pending=self.begin_proposal(expected_revision,eligible,now_ns,now_ms)?;
        match pending.reply.recv_timeout(self.response_timeout) {
            Ok(Ok(advice)) if advice.revision==expected_revision=>Ok(advice),
            _=>{
                self.healthy=false;Err("UCM proposal missing, late or invalid: fail closed")
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn snapshot_submissions_for_test(&self)->Option<u64>{
        self.profile.as_ref().map(|p|p.ops[6].submitted.load(UcmOrdering::Relaxed))
    }
    #[cfg(test)]
    fn healthy(&self)->bool {self.is_healthy()}
}
impl Drop for UcmWorker {
    fn drop(&mut self) {
        self.sender.take();
        if let Some(thread)=self.thread.take(){let _=thread.join();}
        if let Some(p)=&self.profile{eprintln!("R1402_UCM_PROFILE {}",p.json());}
    }
}

#[cfg(test)]
mod tests {
 use super::*;
 fn r1402i_read_published(w:&mut UcmWorker,id:&SourceId)->UcmWorkerSnapshot{
  let pending=w.begin_published();
  loop{match w.poll_published(id,&pending).unwrap(){Some(view)=>return view,None=>std::thread::yield_now()}}
 }
 // R1402L characterization: closed generations remain in the live
 // owner and published scope map; keep rejection of late events intact.
 #[test]
 fn r1409a_percentile_preserves_rank_and_wrapped_window(){
  let mut samples=VecDeque::new();
  for t in 0..2048u64{
   if t%19==0{samples.clear();}
   keep_sample(&mut samples,match t%7{0=>u64::MAX,1=>0,_=>(t*7919)%307});
   for percent in [0,1,50,95,99,100,101]{
    let mut expected=samples.iter().copied().collect::<Vec<_>>();
    expected.sort_unstable();
    assert_eq!(percentile(&samples,percent),Some(expected[(expected.len()-1)*percent.min(100)/100]));
   }
  }
  samples.clear();
  for t in 0..4096u64{
   keep_sample(&mut samples,(t*7919)%307);
   for percent in [95,99]{
    let mut expected=samples.iter().copied().collect::<Vec<_>>();expected.sort_unstable();
    assert_eq!(percentile(&samples,percent),Some(expected[(expected.len()-1)*percent/100]));
   }
  }
  assert_eq!(samples.len(),128);
  assert_eq!(percentile(&VecDeque::new(),95),None);
  let oversized=(0..129u64).rev().collect::<VecDeque<_>>();
  assert_eq!(percentile(&oversized,95),Some(121));
  println!("R1409A_WITNESS exact percentile rank for partial/full/wrapped windows, extremes and oversized compatibility");
 }
 #[test]
 fn r1408b2_unknown_tick_fences_but_actual_loss_survives_recovery(){
  let mut w=UcmWorker::start(QualityPolicy::default(),32).unwrap();
  let a=id("okx","BTC","a");let b=id("okx","BTC","b");
  let mut rev=0;
  for source in [&a,&b]{
   rev+=1;w.apply(rev,UcmWorkerEvent::Connect(source.clone())).unwrap();
   rev+=1;w.apply(rev,UcmWorkerEvent::Subscribe(source.clone())).unwrap();
  }
  for t in [100u64,400,800]{for source in [&a,&b]{
   rev+=1;w.apply(rev,UcmWorkerEvent::Observe{source:source.clone(),received_ns:t*1_000_000,exchange_ms:10000+t as i64,received_wall_ms:10000+t as i64,now_ns:t*1_000_000,now_ms:10000+t as i64,valid:true}).unwrap();
  }}
  let before=r1402i_read_published(&mut w,&a);
  assert!(before.source_ready_at(&a,800_000_000,10800));
  assert!(before.source_ready_at(&b,800_000_000,10800));
  rev+=1;let mut commit=w.begin_commit(rev,a.clone(),800_000_000,10800).unwrap();
  let limit=std::time::Instant::now()+Duration::from_millis(50);
  while w.completed_ticket()<w.submitted_ticket(){assert!(std::time::Instant::now()<limit);std::thread::yield_now();}
  let shared=w.published.as_ref().unwrap().clone();let lock=shared.lock().unwrap();
  let plan=w.tick_plan(810_000_000,10810);
  assert!(plan.fence.contains(&scope(&a)),"force actual projection contention");
  rev+=1;w.apply_planned_tick(rev,plan,vec![]).unwrap();
  assert_eq!(w.destructive_revision(&a),commit.destructive_revision);
  let receipt=w.poll_applied_commit_receipt(&mut commit).unwrap().unwrap();
  assert!(!w.view_current(&a,receipt.revision),"unknown Tick still fences old receipt");
  assert!(w.read_applied_scope(&a).unwrap().is_none());
  drop(lock);
  let current=r1402i_read_published(&mut w,&a);
  assert_eq!(current.hot_loss_revision,receipt.hot_loss_revision);
  assert_eq!(current.active.get(&scope(&a)),Some(&a));
  assert!(current.source_ready_at(&a,810_000_000,10810)&&current.source_ready_at(&b,810_000_000,10810));
  rev+=1;w.apply_certified_tick(rev,1_601_000_000,11601).unwrap();
  let expired=r1402i_read_published(&mut w,&a);
  assert_ne!(expired.hot_loss_revision,receipt.hot_loss_revision);
  assert!(!expired.source_ready_at(&a,1_601_000_000,11601));
  let loss=expired.hot_loss_revision;
  for t in [1700u64,1900,2100,2300]{for source in [&a,&b]{
   rev+=1;w.apply(rev,UcmWorkerEvent::Observe{source:source.clone(),received_ns:t*1_000_000,exchange_ms:10000+t as i64,received_wall_ms:10000+t as i64,now_ns:t*1_000_000,now_ms:10000+t as i64,valid:true}).unwrap();
  }}
  let recovered=r1402i_read_published(&mut w,&a);
  assert!(recovered.source_ready_at(&a,2_300_000_000,12300)&&recovered.source_ready_at(&b,2_300_000_000,12300));
  assert_eq!(recovered.hot_loss_revision,loss,"actual loss remains after recovery");
  assert_eq!(w.destructive_revision(&a),commit.destructive_revision,"Tick is not lifecycle event");
  rev+=1;w.apply(rev,UcmWorkerEvent::Close(b.clone())).unwrap();
  assert_ne!(w.destructive_revision(&a),commit.destructive_revision,"real lifecycle still immediately fences switch");
  println!("R1408B2_WITNESS contended no-op Tick fences until applied, preserves real ACK; expired HOT loss persists across recovery; close remains destructive");
 }
 #[test]
 fn r1408a_index_matches_full_tick_for_both_clocks_and_lifecycle(){
  let mut reference=UniversalConnectionManager::new(QualityPolicy::default());
  let mut indexed=UniversalConnectionManager::new(QualityPolicy::default());
  let mut index=UcmTickIndex::default();
  let ids=(0..80).map(|n|id("okx",&format!("X{}",n/2),&format!("copy-{n}"))).collect::<Vec<_>>();
  for source in &ids{
   for m in [&mut reference,&mut indexed]{m.connect(source.clone());m.subscribed(source);}
   index.sync(source,indexed.state(source));
  }
  let mut checked=0;
  let mut compare=|reference:&mut UniversalConnectionManager,indexed:&mut UniversalConnectionManager,index:&mut UcmTickIndex,ns:u64,wall:i64|{
   let before=indexed.sources.iter().map(|(id,h)|(id.clone(),(h.good_samples,h.first_good_ns,h.hot_ready,h.state))).collect::<HashMap<_,_>>();
   reference.tick(ns,wall);
   let changed=index.tick(indexed,ns,wall).into_iter().collect::<HashSet<_>>();
   for (id,h) in &reference.sources{
    let actual=indexed.state(id).unwrap();
    let expected=(h.good_samples,h.first_good_ns,h.hot_ready,h.state);
    assert_eq!((actual.good_samples,actual.first_good_ns,actual.hot_ready,actual.state),expected,"Tick parity {id:?} ns={ns} wall={wall}");
    assert_eq!(changed.contains(id),before[id]!=expected,"changed projection census");
   }
   assert_eq!(index.mono.len(),index.indexed.len());
   assert_eq!(index.wall.len(),index.indexed.len());
   assert!(index.indexed.len()<=ids.len()&&index.eager.len()<=ids.len());
   checked+=1;
  };
  // Deterministic irregular updates, partial qualification, silence,
  // transport/reset/close and forward/backward wall/mono clock samples.
  for step in 0..180u64{
   let ns=step*50_000_000;let wall=10_000+step as i64*50;
   for n in 0..ids.len(){
    if (n as u64+step)%7==0{
     let source=&ids[n];
     for m in [&mut reference,&mut indexed]{
      m.observe_with_receive_wall(source,ns,wall-(n%3)as i64,wall,ns,wall,(step+n as u64)%17!=0);
     }
     index.sync(source,indexed.state(source));
    }
   }
   if step==72||step==110{
    for n in (0..ids.len()).step_by(11){
     for m in [&mut reference,&mut indexed]{m.reconnecting(&ids[n]);}
     index.sync(&ids[n],indexed.state(&ids[n]));
    }
   }
   if step==125{
    for n in (0..ids.len()).step_by(13){
     for m in [&mut reference,&mut indexed]{m.close(&ids[n]);}
     index.sync(&ids[n],indexed.state(&ids[n]));
    }
   }
   let (ns_tick,wall_tick)=match step{
    80=>(ns,wall-500), 81=>(ns,wall+5000), 112=>(ns-500_000_000,wall),
    _=>(ns,wall),
   };
   compare(&mut reference,&mut indexed,&mut index,ns_tick,wall_tick);
  }
  // Explicit inclusive boundary, future values, signed overflow endpoints.
  let source=&ids[1];
  for m in [&mut reference,&mut indexed]{
   m.connect(source.clone());m.subscribed(source);
   for n in [0u64,400_000_000,800_000_000]{m.observe(source,n,1000,n,1000,true);}
  }
  index.sync(source,indexed.state(source));
  for (ns,wall) in [(1_600_000_000,1800),(1_600_000_001,1800),(u64::MAX,i64::MAX),(0,i64::MIN)]{
   compare(&mut reference,&mut indexed,&mut index,ns,wall);
  }
  println!("R1408A_WITNESS {checked} full-scan vs indexed Tick comparisons, 80 sources, exact changed-ID census and bounded metadata");
 }
 #[test]
 fn r1408a_index_no_lazy_growth_and_exact_boundaries(){
  let mut m=UniversalConnectionManager::new(QualityPolicy::default());
  let source=id("okx","BTC","a");let mut index=UcmTickIndex::default();
  m.connect(source.clone());m.subscribed(&source);
  for t in 0..10_000u64{
   let ns=t*100_000_000;let wall=1000+t as i64*100;
   m.observe(&source,ns,wall,ns,wall,true);index.sync(&source,m.state(&source));
   assert_eq!(index.indexed.len(),1);assert_eq!(index.mono.len(),1);assert_eq!(index.wall.len(),1);
  }
  let h=m.state(&source).unwrap();let n=h.last_valid_receive_ns.unwrap();let w=h.last_valid_exchange_ms.unwrap();
  assert!(h.hot_ready);
  assert!(index.tick(&mut m,n+800_000_000,w+800).is_empty());
  assert!(m.state(&source).unwrap().hot_ready);
  assert_eq!(index.tick(&mut m,n+800_000_001,w+800),vec![source.clone()]);
  assert!(!m.state(&source).unwrap().hot_ready);assert!(index.indexed.is_empty());
  // Both future time and wall expiry are independently actionable.
  m.observe(&source,n+900_000_000,w+900,n+900_000_000,w+900,true);
  index.sync(&source,m.state(&source));
  assert!(!index.tick(&mut m,n+900_000_000,w+899).is_empty());
  assert!(index.indexed.is_empty());
  println!("R1408A_WITNESS 10000 replacements, constant two clock entries, inclusive 800ms and future wall reset");
 }
 #[test]
 fn r1407b_owner_event_advice_preserves_matured_dwell_until_commit(){
  let mut w=UcmWorker::start(QualityPolicy::default(),32).unwrap();
  let slow=id("okx","BTC","slow");let fast=id("okx","BTC","fast");
  let mut rev=0;
  for source in [&slow,&fast]{
   rev+=1;w.apply(rev,UcmWorkerEvent::Connect(source.clone())).unwrap();
   rev+=1;w.apply(rev,UcmWorkerEvent::Subscribe(source.clone())).unwrap();
  }
  let mut n=0;
  for i in 1..=32u64{
   n=i*100_000_000;
   let wall=10000+(n/1_000_000)as i64;
   for (source,delay) in [(&slow,80_000_000u64),(&fast,1_000_000u64)]{
    rev+=1;w.apply(rev,UcmWorkerEvent::Observe{source:source.clone(),received_ns:n-delay,exchange_ms:wall,received_wall_ms:wall,now_ns:n,now_ms:wall,valid:true}).unwrap();
   }
   let _=r1402i_read_published(&mut w,&fast);
  }
  rev+=1;
  let mut commit=w.begin_commit(rev,slow.clone(),n,13200).unwrap();
  loop{if w.poll_commit(&mut commit).unwrap().is_some(){break}std::thread::yield_now();}
  let mut winner=None;
  for i in 0..=52u64{
   let ns=3_300_000_000+i*100_000_000;
   let wall=10000+(ns/1_000_000)as i64;
   rev+=1;w.apply(rev,UcmWorkerEvent::Observe{source:slow.clone(),received_ns:ns-80_000_000,exchange_ms:wall,received_wall_ms:wall,now_ns:ns,now_ms:wall,valid:true}).unwrap();
   rev+=1;
   let facts=UcmBookFacts{anchor:fast.clone(),books:vec![
    UcmBookFact{source:slow.clone(),advances:true,selected:true,equivalent_prior:false},
    UcmBookFact{source:fast.clone(),advances:true,selected:false,equivalent_prior:false}],now_ns:ns,now_ms:wall};
   w.apply_with_book_facts(rev,UcmWorkerEvent::Observe{source:fast.clone(),received_ns:ns-1_000_000,exchange_ms:wall,received_wall_ms:wall,now_ns:ns,now_ms:wall,valid:true},Some(facts)).unwrap();
   let view=r1402i_read_published(&mut w,&fast);
   let advice=view.event_advice.expect("valid observation must publish choice");
   assert_eq!(advice.revision,rev);
   assert_eq!(advice.eligible,vec![slow.clone(),fast.clone()]);
   assert_eq!(view.active.get(&scope(&slow)),Some(&slow),"advice cannot change owner active");
   if i<50{assert_eq!(advice.advice.candidate,Some((slow.clone(),"hysteresis_hold")));}
   if i==49{
    let key=scope(&fast);
    assert!(!w.tick_plan(ns+99_999_999,wall+99).evaluate.contains(&key),"not before exact dwell deadline");
    assert!(w.tick_plan(ns+100_000_000,wall+100).evaluate.contains(&key),"matured challenger schedules Tick without new market event");
   }
   if i>=50{assert_eq!(advice.advice.candidate,Some((fast.clone(),"sustained_quality_improvement")));winner=Some((ns,wall));}
  }
  let (ns,wall)=winner.unwrap();
  rev+=1;let mut commit=w.begin_commit(rev,fast.clone(),ns,wall).unwrap();
  loop{if let Some(v)=w.poll_commit(&mut commit).unwrap(){assert_eq!(v.active.get(&scope(&fast)),Some(&fast));assert!(v.event_advice.is_none());break}std::thread::yield_now();}
  println!("R1407B_WITNESS 5s dwell exact; three successive matured proposals before actual commit; owner active unchanged until ACK");
 }
 #[test]
 fn r1407_actual_command_progress_and_applied_scope(){
  let mut w=UcmWorker::start(QualityPolicy::default(),2).unwrap();
  let a=id("okx","BTC","a");let b=id("okx","ETH","b");
  let mut rev=0;
  for source in [&a,&b]{
   rev+=1;w.apply(rev,UcmWorkerEvent::Connect(source.clone())).unwrap();
   let ticket=w.scope_ticket(source);
   assert_eq!(ticket,w.submitted_ticket());
   let until=std::time::Instant::now()+Duration::from_millis(50);
   while w.completed_ticket()<ticket{assert!(std::time::Instant::now()<until);std::thread::yield_now();}
   w.check_command_progress().unwrap();
   assert!(w.read_applied_scope(source).unwrap().is_some());
  }
  let old_a=w.scope_ticket(&a);
  // Actual mailbox capacity2 => bounded four metadata slots. Reuse slots
  // repeatedly after owner completion; unrelated scope ticket stays exact.
  for t in 0..24u64{
   rev+=1;w.apply(rev,UcmWorkerEvent::TransportOnly{source:b.clone(),received_ns:t+1,received_wall_ms:t as i64+1,now_ns:t+1}).unwrap();
   let ticket=w.submitted_ticket();
   let until=std::time::Instant::now()+Duration::from_millis(50);
   while w.completed_ticket()<ticket{assert!(std::time::Instant::now()<until);std::thread::yield_now();}
   w.check_command_progress().unwrap();
   assert_eq!(w.scope_ticket(&a),old_a);
   assert_eq!(w.scope_ticket(&b),ticket);
  }
  // A blocked read itself is not an outstanding command. It must never
  // turn the owner unhealthy or invent an applied proof.
  let shared=w.published.as_ref().unwrap().clone();let lock=shared.lock().unwrap();
  assert!(w.read_applied_scope(&a).unwrap().is_none());
  w.check_command_progress().unwrap();
  assert!(w.is_healthy());
  // The actual owner now has a command that cannot publish under this
  // held lock. Deterministically age just its metadata (no wall sleep).
  rev+=1;w.apply(rev,UcmWorkerEvent::Tick{now_ns:1_000_000,now_ms:1}).unwrap();
  let ticket=w.submitted_ticket();let slot=(ticket%w.command_deadlines.len() as u64)as usize;
  w.command_deadlines[slot]=Some((ticket,std::time::Instant::now()-Duration::from_millis(1),4));
  assert_eq!(w.check_command_progress(),Err("UCM actual command timeout: fail closed"));
  assert!(!w.is_healthy());drop(lock);
  println!("R1407_WITNESS scope ticket exact; bounded metadata wraps; contention without command not timeout; actual unfinished command fails closed");
 }
 fn r1409c_group(generation:u64)->Vec<SourceId>{
  let session=format!("g0-OkxSpot-0-{generation}-{generation:016x}");
  ["BTC","ETH"].iter().map(|base|{
   let mut source=id("okx",base,&session);
   source.channel="books".into();source.connection_group="g0-OkxSpot-0".into();source.generation=generation;source
  }).collect()
 }
 #[test]
 fn r1409c_runtime_generation_compaction_and_stale_controls(){
  // Separate owner for each fail-closed injection.
  for fault in 0..6{
   let mut w=UcmWorker::start(QualityPolicy::default(),32).unwrap();
   let mut rev=0;
   let count=if fault==0{64}else{3};
   for generation in 1..=count{
    let group=r1409c_group(generation);
    rev+=1;w.apply(rev,UcmWorkerEvent::Group{ids:group.clone(),action:UcmGroupAction::Connect{auto_subscribe:true}}).unwrap();
    for source in &group{
     let view=r1402i_read_published(&mut w,source);
     assert_eq!(view.sources.len(),1);assert!(view.sources.contains_key(source));
     let mut receipt=w.begin_scoped_snapshot(rev,source).unwrap();
     let actual=loop{if let Some(v)=w.poll_snapshot(&mut receipt).unwrap(){break v}std::thread::yield_now()};
     assert_eq!(actual.sources.len(),1);assert!(actual.sources.contains_key(source));
    }
    if generation<count{
     rev+=1;w.apply(rev,UcmWorkerEvent::Group{ids:group.clone(),action:UcmGroupAction::Close}).unwrap();
     let view=r1402i_read_published(&mut w,&group[0]);
     assert_eq!(view.sources[&group[0]].health.state,Some(Lifecycle::Closed));
    }
   }
   let old=r1409c_group(1);let current=r1409c_group(count);
   let event=match fault{
    0=>UcmWorkerEvent::Group{ids:old.clone(),action:UcmGroupAction::Connect{auto_subscribe:true}},
    1=>UcmWorkerEvent::Observe{source:old[0].clone(),received_ns:1_000_000,exchange_ms:1000,received_wall_ms:1000,now_ns:1_000_000,now_ms:1000,valid:true},
    2=>UcmWorkerEvent::Subscribe(old[0].clone()),
    3=>UcmWorkerEvent::CommitActive{source:old[0].clone(),now_ns:1_000_000,now_ms:1000},
    4=>UcmWorkerEvent::Group{ids:r1409c_group(count+1),action:UcmGroupAction::Connect{auto_subscribe:true}},
    _=>UcmWorkerEvent::Close(old[0].clone()),
   };
   rev+=1;w.apply(rev,event).unwrap();
   assert!(w.propose(rev,vec![current[0].clone()],1_000_000,1000).is_err());
   assert!(!w.is_healthy(),"stale or overlapping control {fault} must fail closed");
  }
  println!("R1409C_WITNESS 64 generations with two scopes bounded at one source per scope; late Connect/Observe/Subscribe/Commit/Close and overlapping Connect rejected");
 }
 #[test]
 fn r1402l_audit_retired_generation_accumulation(){
  let mut w=UcmWorker::start(QualityPolicy::default(),32).unwrap();
  let mut revision=0;
  for generation in 1..=12u64{
   let mut source=id("okx","BTC",&format!("old-{generation}"));source.generation=generation;
   revision+=1;w.apply(revision,UcmWorkerEvent::Connect(source.clone())).unwrap();
   revision+=1;w.apply(revision,UcmWorkerEvent::Close(source)).unwrap();
  }
  let mut current=id("okx","BTC","current");current.generation=13;
  revision+=1;w.apply(revision,UcmWorkerEvent::Connect(current.clone())).unwrap();
  let view=r1402i_read_published(&mut w,&current);
  assert_eq!(view.sources.len(),13);
  assert_eq!(view.sources.values().filter(|v|v.health.state==Some(Lifecycle::Closed)).count(),12);
  let mut snapshot=w.begin_scoped_snapshot(revision,&current).unwrap();
  let owner=loop{match w.poll_snapshot(&mut snapshot).unwrap(){Some(v)=>break v,None=>std::thread::yield_now()}};
  assert_eq!(owner.sources.len(),13,"original owner also retains retired generation records");
  revision+=1;w.apply_certified_tick(revision,1_000_000_000,1000).unwrap();
  let after=r1402i_read_published(&mut w,&current);
  assert_eq!(after.sources.len(),13);
  println!("R1402L_WITNESS 12 retired generations retained in owner and published view; 1 current; Tick does not retire them");
 }
 #[test]
 fn r1402k_certified_tick_preserves_only_proven_noop_scopes(){
  let mut w=UcmWorker::start(QualityPolicy::default(),32).unwrap();
  let a=id("okx","BTC","a");let b=id("okx","ETH","b");
  let mut rev=0;
  for source in [&a,&b]{
   rev+=1;w.apply(rev,UcmWorkerEvent::Connect(source.clone())).unwrap();
   rev+=1;w.apply(rev,UcmWorkerEvent::Subscribe(source.clone())).unwrap();
  }
  for t in [100u64,300,600]{
   for source in [&a,&b]{
    rev+=1;w.apply(rev,UcmWorkerEvent::Observe{source:source.clone(),received_ns:t*1_000_000,exchange_ms:10000+t as i64,received_wall_ms:10000+t as i64,now_ns:t*1_000_000,now_ms:10000+t as i64,valid:true}).unwrap();
   }
  }
  let _=r1402i_read_published(&mut w,&b);
  let first=r1402i_read_published(&mut w,&a);
  rev+=1;w.apply_certified_tick(rev,1_400_000_000,11400).unwrap();
  assert!(w.view_current(&a,first.revision),"inclusive800 unchanged");
  rev+=1;w.apply_certified_tick(rev,1_401_000_000,11401).unwrap();
  assert!(!w.view_current(&a,first.revision),"801 must fence before apply");
  let expired=r1402i_read_published(&mut w,&a);
  assert!(!expired.source_ready_at(&a,1_401_000_000,11401));
  // Contended projection: conservative global fence, never wait on it.
  let shared=w.published.as_ref().unwrap().clone();let lock=shared.lock().unwrap();
  rev+=1;w.apply_certified_tick(rev,1_402_000_000,11402).unwrap();
  assert!(!w.view_current(&a,expired.revision));drop(lock);
  let current=r1402i_read_published(&mut w,&a);
  rev+=1;w.apply(rev,UcmWorkerEvent::Observe{source:a.clone(),received_ns:1_500_000_000,exchange_ms:11500,received_wall_ms:11500,now_ns:1_500_000_000,now_ms:11500,valid:true}).unwrap();
  assert!(!w.view_current(&a,current.revision));
  rev+=1;w.apply_certified_tick(rev,1_501_000_000,11501).unwrap();
  let recovered=r1402i_read_published(&mut w,&a);
  assert_eq!(recovered.sources[&a].health.last_valid_receive_ns,Some(1_500_000_000));
  assert!(w.healthy());
 }
 #[test]
 fn r1402i_publication_fences_tick_quality_and_commit_parity(){
  let mut w=UcmWorker::start(QualityPolicy::default(),32).unwrap();
  let a=id("okx","BTC","a");let b=id("okx","BTC","b");let eth=id("okx","ETH","c");
  let mut revision=0u64;
  for source in [&a,&b,&eth]{
   revision+=1;w.apply(revision,UcmWorkerEvent::Connect(source.clone())).unwrap();
   revision+=1;w.apply(revision,UcmWorkerEvent::Subscribe(source.clone())).unwrap();
  }
  for t in [100u64,300,600]{for source in [&a,&b,&eth]{
   revision+=1;w.apply(revision,UcmWorkerEvent::Observe{source:source.clone(),received_ns:t*1_000_000,
    exchange_ms:10_000+t as i64,received_wall_ms:10_000+t as i64,now_ns:t*1_000_000,now_ms:10_000+t as i64,valid:true}).unwrap();
  }}
  let view=r1402i_read_published(&mut w,&a);
  assert!(view.source_ready_at(&a,600_000_000,10_600));
  assert!(view.source_ready_at(&b,600_000_000,10_600));
  assert!(w.view_current(&a,view.revision));
  assert!(!view.sources.contains_key(&eth));
  let mut query=w.begin_scoped_snapshot(revision,&a).unwrap();
  let direct=loop{match w.poll_snapshot(&mut query).unwrap(){Some(v)=>break v,None=>std::thread::yield_now()}};
  for source in [&a,&b]{
   let x=&view.sources[source];let y=&direct.sources[source];
   assert_eq!(x.quality,y.quality);assert_eq!(x.qualification_requirements,y.qualification_requirements);
   assert_eq!(format!("{:?}",x.health),format!("{:?}",y.health));
  }
  // Unrelated Observe does not invalidate BTC, same-scope event does.
  revision+=1;w.apply(revision,UcmWorkerEvent::Observe{source:eth.clone(),received_ns:650_000_000,
   exchange_ms:10650,received_wall_ms:10650,now_ns:650_000_000,now_ms:10650,valid:true}).unwrap();
  assert!(w.view_current(&a,view.revision));
  let mut proposal=w.begin_scoped_proposal(revision,&a,vec![a.clone(),b.clone()],650_000_000,10650).unwrap();
  let chosen=loop{match w.poll_proposal(&mut proposal).unwrap(){Some(v)=>break v.candidate.unwrap().0,None=>std::thread::yield_now()}};
  assert_eq!(chosen,a,"original quality tie uses physical connection identity");
  revision+=1;let mut commit=w.begin_commit(revision,a.clone(),650_000_000,10650).unwrap();
  let ack=loop{match w.poll_commit(&mut commit).unwrap(){Some(v)=>break v,None=>std::thread::yield_now()}};
  assert_eq!(ack.active.get(&scope(&a)),Some(&a));
  assert!(ack.sources[&a].health.active);
  let old=r1402i_read_published(&mut w,&a);
  // Hold the publication lock: a queued Tick is NOT an applied Tick.
  let shared=w.published.as_ref().unwrap().clone();
  let lock=shared.lock().unwrap();
  revision+=1;w.apply(revision,UcmWorkerEvent::Tick{now_ns:1_400_000_000,now_ms:11400}).unwrap();
  assert!(!w.view_current(&a,old.revision));
  assert!(w.poll_published(&a,&w.begin_published()).unwrap().is_none());
  drop(lock);
  let boundary=r1402i_read_published(&mut w,&a);
  assert!(boundary.source_ready_at(&a,1_400_000_000,11400));
  revision+=1;w.apply(revision,UcmWorkerEvent::Tick{now_ns:1_401_000_000,now_ms:11401}).unwrap();
  let expired=r1402i_read_published(&mut w,&a);
  assert!(!expired.source_ready_at(&a,1_401_000_000,11401));
  assert!(!expired.sources[&a].health.hot_ready);
  assert_eq!(expired.sources[&a].health.good_samples,0);
  revision+=1;w.apply(revision,UcmWorkerEvent::Close(a.clone())).unwrap();
  let closed=r1402i_read_published(&mut w,&a);
  assert_eq!(closed.sources[&a].health.state,Some(Lifecycle::Closed));
  assert!(!closed.active.contains_key(&scope(&a)));
  assert!(!w.view_current(&a,boundary.revision));
  assert!(w.healthy());
 }
 #[test]
 fn r1385_readiness_history_and_dual_hot_transition_owner_parity(){
  let mut manager=UniversalConnectionManager::new(QualityPolicy::default());
  assert_eq!(manager.observe_readiness_mask("BTC","hot-a",0b101), (None,false));
  assert_eq!(manager.observe_readiness_mask("BTC","hot-a",0b101), (Some(0b101),false));
  assert_eq!(manager.observe_readiness_mask("BTC","hot-a",0b001), (Some(0b101),false));
  assert_eq!(manager.readiness_previous_len(),1);
  assert!(manager.readiness_has_session("hot-a"));
  assert!(manager.readiness_previous_payload_capacity_bytes()>0);
  assert_eq!(manager.observe_dual_hot("BTC",false,100),DualHotTransition::None);
  assert_eq!(manager.observe_dual_hot("BTC",true,200),DualHotTransition::Established);
  assert_eq!(manager.observe_dual_hot("BTC",true,250),DualHotTransition::None);
  assert_eq!(manager.observe_dual_hot("BTC",false,300),DualHotTransition::DeficitOnset);
  assert_eq!(manager.observe_dual_hot("BTC",false,350),DualHotTransition::None);
  assert_eq!(manager.observe_dual_hot("BTC",true,450),DualHotTransition::Restored{deficit_duration_ns:Some(150)});
  assert_eq!(manager.observe_dual_hot("BTC",true,500),DualHotTransition::None);
  for i in 0..4095 {assert_eq!(manager.observe_readiness_mask("scope",&format!("session-{i}"),7).1,false);}
  assert_eq!(manager.readiness_previous_len(),4096);
  assert!(manager.observe_readiness_mask("scope","overflow",11).1);
  assert_eq!(manager.readiness_previous_len(),4096);
  let retired=manager.retire_readiness_session("overflow");assert_eq!(retired,1);
  assert!(!manager.readiness_has_session("overflow"));
  assert_eq!(manager.readiness_previous_len(),4095);
 }
 #[test]
 fn r1383_percentile_rank_parity_with_prior_full_sort(){
  assert_eq!(percentile(&VecDeque::new(),95),None);
  // Verify exact rank semantics on empty, singleton, duplicates, reversed, sparse, and ring-wrapped histories.
  for len in 1usize..=128{
   for style in 0usize..4{
    let mut samples=VecDeque::new();
    for i in 0..len{
     let v=match style{0=>i as u64,1=>(len-i)as u64,2=>((i*17+len)%7)as u64,_=>((i*7919+len*31)%1009)as u64};
     samples.push_back(v);
    }
    if len>1{for _ in 0..len/2{let v=samples.pop_front().unwrap();samples.push_back(v);}}
    for percent in [0usize,1,25,50,75,90,95,99,100,101]{
     let mut old=samples.iter().copied().collect::<Vec<_>>();old.sort_unstable();
     let rank=((old.len()-1)*percent.min(100))/100;
     assert_eq!(percentile(&samples,percent),Some(old[rank]),"len={len} style={style} percent={percent}");
    }
   }
  }
 }

 fn id(exchange:&str,symbol:&str,conn:&str)->SourceId{SourceId{exchange:exchange.into(),segment:"spot".into(),instrument:symbol.into(),channel:"books5".into(),physical_connection:conn.into(),logical_session:conn.into(),connection_group:conn.into(),generation:1}}
 #[test]fn r1378_grouped_lifecycle_matches_existing_source_transitions(){
  let mut m=UniversalConnectionManager::new(QualityPolicy::default());
  let a=id("okx","BTC","socket-a");let b=id("okx","ETH","socket-a");let other=id("okx","BTC","socket-b");
  for source in [&a,&b,&other]{m.connect(source.clone());}
  m.apply_connection_lifecycle(&[a.clone(),b.clone()],ConnectionLifecycleAction::Subscribe);
  assert_eq!(m.state(&a).unwrap().state,Some(Lifecycle::Subscribed));
  assert_eq!(m.state(&b).unwrap().state,Some(Lifecycle::Subscribed));
  assert_eq!(m.state(&other).unwrap().state,Some(Lifecycle::Connected));
  m.apply_connection_lifecycle(&[a.clone(),b.clone()],ConnectionLifecycleAction::Reconnect);
  assert_eq!(m.state(&a).unwrap().state,Some(Lifecycle::Reconnecting));
  assert_eq!(m.state(&b).unwrap().state,Some(Lifecycle::Reconnecting));
  assert_eq!(m.state(&other).unwrap().state,Some(Lifecycle::Connected));
  m.apply_connection_lifecycle(&[a.clone(),b.clone()],ConnectionLifecycleAction::Close);
  assert_eq!(m.state(&a).unwrap().state,Some(Lifecycle::Closed));
  assert_eq!(m.state(&b).unwrap().state,Some(Lifecycle::Closed));
  assert_eq!(m.state(&other).unwrap().state,Some(Lifecycle::Connected));
 }
 #[test]fn r1246_book_profiles_keep_okx_spot_and_binance_futures_channels_explicit(){
  assert_eq!(r1246_stream_profile(Some("TOP5_500MS")),("depth5@500ms","books5"));
  assert_eq!(r1246_stream_profile(Some("OKX_BOOKS5_BINANCE_DEPTH20_100MS")),("depth20@100ms","books5"));
  assert_eq!(r1246_stream_profile(Some("FULL_DEPTH_100MS")),("depth20@100ms","books"));
  assert_eq!(r1246_stream_profile(None),("depth20@100ms","books"));
 }
 #[test]fn adapter_registry_has_exchange_specific_wire_contracts(){let a=adapter_for(Venue::BinanceFutures);let o=adapter_for(Venue::OkxSpot);assert_eq!(a.exchange(),"binance");assert_eq!(a.channel(),"depth20@100ms");assert!(a.endpoint(&["BTC".into()]).contains("btcusdt@depth20@100ms"));assert_eq!(o.channel(),"books");let sub=o.subscription(&["BTC".into()]).unwrap();assert!(sub.contains("\"channel\":\"books\""));assert!(sub.contains("BTC-USDT"));let mock=MockExchangeAdapter;let raw=RawFrame{venue:Venue::OkxSpot,epoch:1,received_ns:1,received_wall_ms:1000,raw:r#"{"symbol":"BTC","ts":1000,"seq":1,"snapshot":true}"#.into(),source:None};assert_eq!(mock.parse_market(&raw).unwrap().instrument,"BTC");}
 #[test]fn independent_hot_standby_and_failover(){let mut m=UniversalConnectionManager::new(QualityPolicy::default());let a=id("okx","BTC","a");let b=id("okx","BTC","b");m.connect(a.clone());m.connect(b.clone());m.subscribed(&a);m.subscribed(&b);for t in [0,400_000_000,800_000_000]{for x in [&a,&b]{m.observe(x,t,1_000,t,1_000,true)}}assert!(m.state(&a).unwrap().hot_ready);assert_eq!(m.hot_standbys(&a),1);assert!(m.set_active(&a));m.observe(&b,1_500_000_000,1_500,1_500_000_000,1_500,true);assert_eq!(m.failover_candidate(&a,1_700_000_000,1_700),Some(&b));}
 #[test]fn stale_exchange_age_is_not_hidden_by_live_socket(){let mut m=UniversalConnectionManager::new(QualityPolicy::default());let a=id("okx","ETH","a");m.connect(a.clone());m.subscribed(&a);for t in [0,400_000_000,800_000_000]{m.observe(&a,t,1_000,t,1_000,true)}assert!(m.state(&a).unwrap().hot_ready);m.tick(1_700_000_000,1_700);assert!(m.state(&a).unwrap().websocket);assert!(!m.state(&a).unwrap().hot_ready);}
 #[test]fn transport_loss_immediately_allows_hot_standby_failover(){let mut m=UniversalConnectionManager::new(QualityPolicy::default());let a=id("okx","BTC","primary");let b=id("okx","BTC","standby");for x in [&a,&b]{m.connect(x.clone());m.subscribed(x);for t in [0,400_000_000,800_000_000]{m.observe(x,t,1_000,t,1_000,true)}}assert!(m.set_active(&a));m.transport(&a,Transport::WebSocket,false);assert!(!m.state(&a).unwrap().hot_ready);assert!(!m.state(&a).unwrap().active);assert_eq!(m.failover_candidate(&a,900_000_000,1_000),Some(&b));}
  #[test]fn r1239_idle_stale_recovery_keeps_strict_freshness_and_uses_short_requalification(){let mut m=UniversalConnectionManager::new(QualityPolicy::default());let a=id("okx","ETH","a");m.connect(a.clone());m.subscribed(&a);for t in [0,400_000_000,800_000_000]{m.observe(&a,t,1_000,t,1_000,true)}assert!(m.state(&a).unwrap().hot_ready);m.tick(1_700_000_000,1_700);assert!(!m.state(&a).unwrap().hot_ready);assert!(m.state(&a).unwrap().qualified_once);m.observe(&a,1_700_000_000,1_700,1_700_000_000,1_700,true);assert!(!m.state(&a).unwrap().hot_ready);m.observe(&a,1_800_000_000,1_800,1_800_000_000,1_800,true);assert!(m.state(&a).unwrap().hot_ready);m.tick(2_601_000_000,2_601);assert!(!m.state(&a).unwrap().hot_ready);m.reconnecting(&a);m.connect(a.clone());m.subscribed(&a);for t in [3_000_000_000,3_125_000_000]{m.observe(&a,t,3_000+(t.saturating_sub(3_000_000_000)/1_000_000)as i64,t,3_000+(t.saturating_sub(3_000_000_000)/1_000_000)as i64,true)}assert!(!m.state(&a).unwrap().hot_ready);m.observe(&a,3_250_000_000,3_250,3_250_000_000,3_250,true);assert!(m.state(&a).unwrap().hot_ready);}
 #[test]fn physical_rotation_waits_for_all_scopes(){assert!(!UniversalConnectionManager::should_rotate_physical(1,2));assert!(UniversalConnectionManager::should_rotate_physical(2,2));}
 #[test]fn r1216_qualification_requires_three_samples_and_250ms_dwell(){let mut m=UniversalConnectionManager::new(QualityPolicy::default());let a=id("okx","BTC","dwell250");m.connect(a.clone());m.subscribed(&a);for t in [0,125_000_000]{m.observe(&a,t,1_000,t,1_000,true)}assert!(!m.state(&a).unwrap().hot_ready);m.observe(&a,250_000_000,1_000,250_000_000,1_000,true);assert!(m.state(&a).unwrap().hot_ready);}
 #[test]fn mock_exchange_adapter_uses_full_core_lifecycle(){let adapter=MockExchangeAdapter;let raw=RawFrame{venue:Venue::OkxSpot,epoch:1,received_ns:0,received_wall_ms:1_000,raw:r#"{"symbol":"BTC","ts":1000,"seq":1,"snapshot":true}"#.into(),source:None};let event=adapter.parse_market(&raw).unwrap();assert_eq!(adapter.classify_control(r#"{"op":"subscribe_ack"}"#),AdapterControl::SubscribeAck);let mut m=UniversalConnectionManager::new(QualityPolicy::default());let mock=id(adapter.exchange(),&event.instrument,"m1");m.connect(mock.clone());m.subscribed(&mock);for t in [0,400_000_000,800_000_000]{m.observe(&mock,t,event.exchange_ts_ms,t,1_000,true)}assert!(m.state(&mock).unwrap().hot_ready);assert!(m.set_active(&mock));assert_eq!(m.state(&mock).unwrap().state,Some(Lifecycle::Active));}
 #[test]fn quality_switch_uses_hysteresis_and_dwell(){
  let policy=QualityPolicy{minimum_score_samples:3,switch_dwell:Duration::from_secs(1),improvement_percent:1,minimum_improvement_ns:1,..QualityPolicy::default()};
  let mut m=UniversalConnectionManager::new(policy);let a=id("okx","BTC","slow");let b=id("okx","BTC","fast");
  for source in [&a,&b]{m.connect(source.clone());m.subscribed(source);}
  for t in [0u64,400_000_000,800_000_000]{let event=1_000+(t/1_000_000)as i64;m.observe(&a,t,event,t+100_000_000,event,true);m.observe(&b,t,event,t+1_000_000,event,true);}
  assert!(m.state(&a).unwrap().hot_ready);assert!(m.state(&b).unwrap().hot_ready);assert!(m.set_active(&a));
  assert_eq!(m.choose_candidate(&[a.clone(),b.clone()],1_000_000_000,1_800).map(|x|x.0),Some(a.clone()));
  for t in [1_200_000_000u64,1_600_000_000,2_000_000_000]{let event=1_000+(t/1_000_000)as i64;m.observe(&a,t,event,t+100_000_000,event,true);m.observe(&b,t,event,t+1_000_000,event,true);}
  assert_eq!(m.choose_candidate(&[a,b.clone()],2_100_000_000,3_000).map(|x|x.0),Some(b));
 }
 #[test]fn emergency_failover_ignores_quality_dwell(){
  let mut m=UniversalConnectionManager::new(QualityPolicy::default());let a=id("okx","BTC","primary");let b=id("okx","BTC","reserve");
  for source in [&a,&b]{m.connect(source.clone());m.subscribed(source);for t in [0u64,400_000_000,800_000_000]{m.observe(source,t,1_000,t,1_000,true);}}
  assert!(m.set_active(&a));assert_eq!(m.choose_candidate(&[b.clone()],900_000_000,1_000),Some((b,"active_source_unready_emergency_failover")));
 }
 #[test]fn per_instrument_selection_and_reconnect_history_are_independent(){
  let mut m=UniversalConnectionManager::new(QualityPolicy::default());let btc_a=id("okx","BTC","btc-a");let btc_b=id("okx","BTC","btc-b");let eth=id("okx","ETH","eth-a");
  for source in [&btc_a,&btc_b,&eth]{m.connect(source.clone());m.subscribed(source);for t in [0u64,400_000_000,800_000_000]{m.observe(source,t,1_000,t,1_000,true);}}
  assert!(m.set_active(&btc_a));assert!(m.set_active(&eth));assert_eq!(m.choose_candidate(&[btc_b.clone()],900_000_000,1_000),Some((btc_b.clone(),"active_source_unready_emergency_failover")));assert_eq!(m.active_for_scope(&eth),Some(&eth));
  let mut next=btc_b.clone();next.physical_connection="btc-b-generation-2".into();next.logical_session=next.physical_connection.clone();next.generation=2;next.connection_group=btc_b.connection_group.clone();
  m.reconnecting(&btc_b);m.connect(next.clone());assert_eq!(m.state(&next).unwrap().reconnects,1);
 }
 #[test]fn supported_groups_partition_instruments_only(){let symbols=(0..33).map(|n|format!("S{n}")).collect::<Vec<_>>();let groups=partition_instruments(&symbols,ConnectionGroupSize::Sixteen).unwrap();assert_eq!(groups.iter().map(Vec::len).collect::<Vec<_>>(),vec![16,16,1]);assert!(ConnectionGroupSize::try_from(12).is_err());}

 #[test]fn r1359_dual_source_idle_resume_no_spurious_authority_switch(){
  let mut m=UniversalConnectionManager::new(QualityPolicy::default());
  let a=id("okx","QUIET","primary");let b=id("okx","QUIET","standby");
  for source in [&a,&b]{m.connect(source.clone());m.subscribed(source);for t in [0u64,400_000_000,800_000_000]{m.observe(source,t,1_000+(t/1_000_000)as i64,t,1_000+(t/1_000_000)as i64,true);}}
  assert!(m.set_active(&a));
  m.tick(5_000_000_000,6_000);
  assert!(!m.state(&a).unwrap().hot_ready);
  assert!(!m.state(&b).unwrap().hot_ready);
  assert_eq!(m.choose_candidate(&[a.clone(),b.clone()],5_000_000_000,6_000),None);
  assert_eq!(m.active_for_scope(&a),Some(&a));
  // Both physical sockets still live; a market-data silence is not a transport failure.
  assert!(m.state(&a).unwrap().websocket&&m.state(&b).unwrap().websocket);
  for t in [5_000_000_000u64,5_100_000_000]{let wall=1_000+(t/1_000_000)as i64;for source in [&a,&b]{m.observe(source,t,wall,t,wall,true);}}
  assert!(m.state(&a).unwrap().hot_ready&&m.state(&b).unwrap().hot_ready);
  assert_eq!(m.choose_candidate(&[a.clone(),b.clone()],5_100_000_000,6_100),Some((a.clone(),"active_remains_best")));
  assert_eq!(m.active_for_scope(&a),Some(&a));
  // Actual source failure with qualified standby permits failover.
  m.transport(&a,Transport::WebSocket,false);
  // transport(false) removes active mapping; current reason reports initial choice, not failover.
  assert_eq!(m.choose_candidate(&[a.clone(),b.clone()],5_100_000_000,6_100),Some((b.clone(),"initial_manager_choice")));
  assert!(m.set_active(&b));
  assert_eq!(m.active_for_scope(&b),Some(&b));
 }

 #[test]fn r1359_standby_resumes_first_after_market_silence(){
  let mut m=UniversalConnectionManager::new(QualityPolicy::default());
  let a=id("okx","QUIET","primary");let b=id("okx","QUIET","standby");
  for source in [&a,&b]{m.connect(source.clone());m.subscribed(source);for t in [0u64,400_000_000,800_000_000]{let wall=1_000+(t/1_000_000)as i64;m.observe(source,t,wall,t,wall,true);}}
  assert!(m.set_active(&a));m.tick(5_000_000_000,6_000);
  // The standby sees a new valid update first, while the primary socket is still live.
  for t in [5_000_000_000u64,5_100_000_000]{let wall=1_000+(t/1_000_000)as i64;m.observe(&b,t,wall,t,wall,true);}
  assert!(!m.state(&a).unwrap().hot_ready&&m.state(&a).unwrap().websocket);
  assert!(m.state(&b).unwrap().hot_ready);
  assert_eq!(m.choose_candidate(&[a.clone(),b.clone()],5_100_000_000,6_100),Some((b.clone(),"active_source_unready_emergency_failover")));
  assert!(m.set_active(&b));
  // Primary resumes; stick to now-active standby, rather than ping-pong on first quote.
  for t in [5_200_000_000u64,5_300_000_000]{let wall=1_000+(t/1_000_000)as i64;m.observe(&a,t,wall,t,wall,true);m.observe(&b,t,wall,t,wall,true);}
  assert_eq!(m.choose_candidate(&[a.clone(),b.clone()],5_300_000_000,6_300).map(|x|x.0),Some(b.clone()));
  assert_eq!(m.active_for_scope(&b),Some(&b));
 }
 // R1399: no data is admitted by this worker; it only replays events that
 // the Engine has already ordered/validated and provides advisory HOT choice.
 #[test]
 fn r1399_worker_replays_ordered_ucm_events_identically() {
  let mut reference=UniversalConnectionManager::new(QualityPolicy::default());
  let mut worker=UcmWorker::start(QualityPolicy::default(),16).unwrap();
  let a=id("okx","BTC","a");let b=id("okx","BTC","b");
  let mut revision=0u64;
  macro_rules! push {
   ($event:expr) => {{
    let event=$event;
    match &event {
     UcmWorkerEvent::Group{ids,action}=>{for id in ids{match action{
      UcmGroupAction::Connect{auto_subscribe}=>{reference.connect(id.clone());if *auto_subscribe{reference.subscribed(id);}},
      UcmGroupAction::Subscribe=>reference.subscribed(id),
      UcmGroupAction::Reconnect=>reference.reconnecting(id),
      UcmGroupAction::Close=>reference.close(id),
      UcmGroupAction::Quarantine=>reference.quarantine(id),
     }}},
     UcmWorkerEvent::Connect(id)=>reference.connect(id.clone()),
     UcmWorkerEvent::Subscribe(id)=>reference.subscribed(id),
     UcmWorkerEvent::Reconnect(id)=>reference.reconnecting(id),
     UcmWorkerEvent::Close(id)=>reference.close(id),
     UcmWorkerEvent::Quarantine(id)=>reference.quarantine(id),
     UcmWorkerEvent::BookReset{source,sequence_gap}=>{
      if *sequence_gap{reference.reconnecting(source)}else{reference.quarantine(source)}
     },
     UcmWorkerEvent::Observe{source,received_ns,exchange_ms,received_wall_ms,now_ns,now_ms,valid}
      =>reference.observe_with_receive_wall(source,*received_ns,*exchange_ms,*received_wall_ms,*now_ns,*now_ms,*valid),
     UcmWorkerEvent::TransportOnly{source,received_ns,received_wall_ms,now_ns}
      =>{reference.observe_transport_liveness(source,*received_ns,*received_wall_ms,*now_ns).unwrap();},
     UcmWorkerEvent::Tick{now_ns,now_ms}=>reference.tick(*now_ns,*now_ms),
     UcmWorkerEvent::CommitActive{source,now_ns,now_ms}=>{
      assert!(reference.fresh(source,*now_ns,*now_ms) && reference.set_active(source));
     },
    }
    revision+=1;
    worker.apply(revision,event).unwrap();
   }}
  }
  macro_rules! compare {
   ($at:expr) => {{
    let t=$at as u64;let wall=10_000+t as i64;
    let eligible=vec![a.clone(),b.clone()];
    let expected=reference.choose_candidate(&eligible,t*1_000_000,wall);
    let actual=worker.propose(revision,eligible,t*1_000_000,wall).unwrap();
    assert_eq!(actual.revision,revision);
    assert_eq!(actual.candidate,expected,"worker selection parity t={t}");
   }}
  }
  push!(UcmWorkerEvent::Connect(a.clone()));
  push!(UcmWorkerEvent::Subscribe(a.clone()));
  push!(UcmWorkerEvent::Connect(b.clone()));
  push!(UcmWorkerEvent::Subscribe(b.clone()));
  for t in [100u64,300,600]{
   for source in [&a,&b]{
    push!(UcmWorkerEvent::Observe{source:source.clone(),received_ns:t*1_000_000,
     exchange_ms:10_000+t as i64,received_wall_ms:10_000+t as i64,
     now_ns:t*1_000_000,now_ms:10_000+t as i64,valid:true});
   }
  }
  compare!(600);
  push!(UcmWorkerEvent::CommitActive{source:a.clone(),now_ns:600_000_000,now_ms:10_600});
  compare!(700);
  // Transport-only keeps the physical socket alive but not the book fresh.
  push!(UcmWorkerEvent::TransportOnly{source:a.clone(),received_ns:1_400_000_000,
   received_wall_ms:11_400,now_ns:1_400_000_000});
  compare!(1400); // exactly 800ms book receive age: still eligible
  compare!(1401); // 801ms: no stale HOT candidate
  push!(UcmWorkerEvent::Tick{now_ns:1_401_000_000,now_ms:11_401});
  compare!(1401);
  // Previously active source is closed and must not resurrect on same session.
  push!(UcmWorkerEvent::Close(a.clone()));
  compare!(1402);
  // Exact SourceId generation change, no old-generation candidate admitted.
  let mut next=a.clone();next.generation+=1;
  push!(UcmWorkerEvent::Connect(next.clone()));
  push!(UcmWorkerEvent::Subscribe(next.clone()));
  for t in [1600u64,1800,2000]{
   push!(UcmWorkerEvent::Observe{source:next.clone(),received_ns:t*1_000_000,
    exchange_ms:10_000+t as i64,received_wall_ms:10_000+t as i64,
    now_ns:t*1_000_000,now_ms:10_000+t as i64,valid:true});
  }
  // Old source's candidates remain absent or stale.
  compare!(2000);
  let expected=reference.choose_candidate(&[next.clone()],2_000_000_000,12_000);
  assert_eq!(worker.propose(revision,vec![next],2_000_000_000,12_000).unwrap().candidate,expected);
  assert!(worker.healthy());
 }

 #[test]
 fn r1399_nonblocking_proposal_rejects_superseded_revision() {
  let mut worker=UcmWorker::start(QualityPolicy::default(),8).unwrap();
  // First proposal is issued without waiting for worker, then a newer event
  // arrives before admission; even an already completed reply is stale.
  let mut pending=worker.begin_proposal(0,vec![],0,10_000).unwrap();
  worker.apply(1,UcmWorkerEvent::Tick{now_ns:1,now_ms:10_000}).unwrap();
  assert!(worker.poll_proposal(&mut pending).is_err(),
   "out-of-order stale proposal cannot be accepted");
  assert!(worker.healthy(),"a superseded query is not an owner failure");
  let mut fresh=worker.begin_proposal(1,vec![],1,10_000).unwrap();
  let answer=loop {
   match worker.poll_proposal(&mut fresh).unwrap() {
    Some(advice)=>break advice,
    None=>std::thread::yield_now(),
   }
  };
  assert_eq!(answer.revision,1);
  assert_eq!(answer.candidate,None);
  assert!(worker.healthy());
 }

 #[test]
 fn r1399_worker_revision_gap_and_stale_query_fail_closed() {
  let mut worker=UcmWorker::start(QualityPolicy::default(),4).unwrap();
  let source=id("okx","BTC","a");
  worker.apply(1,UcmWorkerEvent::Connect(source.clone())).unwrap();
  assert_eq!(worker.propose(1,vec![source.clone()],0,10_000).unwrap().candidate,None);
  assert!(worker.apply(3,UcmWorkerEvent::Subscribe(source.clone())).is_err(),
   "skipped lifecycle revision must never be silently discarded");
  assert!(!worker.healthy());
  assert!(worker.propose(1,vec![source],0,10_000).is_err(),
   "after missed event, stale UCM proposal may not be used");
 }


 fn r1402_history_receipt(w:&mut UcmWorker,mut pending:UcmPendingHistory)->UcmHistoryReceipt{
  loop{match w.poll_history(&mut pending).unwrap(){
   Some(receipt)=>return receipt,None=>std::thread::yield_now(),
  }}
 }
 #[test]
 fn r1402_okx_book_reset_same_generation_recovery_parity(){
  for sequence_gap in [true,false] {
   let mut w=UcmWorker::start(QualityPolicy::default(),16).unwrap();
   let mut reference=UniversalConnectionManager::new(QualityPolicy::default());
   let mut source=id("okx","BTC","a");source.channel="books".into();
   let mut rev=0;
   macro_rules! event {($e:expr)=>{{rev+=1;w.apply(rev,$e).unwrap();}}}
   event!(UcmWorkerEvent::Connect(source.clone()));reference.connect(source.clone());
   event!(UcmWorkerEvent::Subscribe(source.clone()));reference.subscribed(&source);
   for t in [100u64,400,700]{
    let ns=t*1_000_000;let wall=10_000+t as i64;
    event!(UcmWorkerEvent::Observe{source:source.clone(),received_ns:ns,
      exchange_ms:wall,received_wall_ms:wall,now_ns:ns,now_ms:wall,valid:true});
    reference.observe_with_receive_wall(&source,ns,wall,wall,ns,wall,true);
   }
   event!(UcmWorkerEvent::CommitActive{source:source.clone(),now_ns:700_000_000,now_ms:10_700});
   assert!(reference.set_active(&source));
   event!(UcmWorkerEvent::BookReset{source:source.clone(),sequence_gap});
   if sequence_gap{reference.reconnecting(&source)}else{reference.quarantine(&source)}
   let mut view=w.begin_scoped_snapshot(rev,&source).unwrap();
   let view=loop{match w.poll_snapshot(&mut view).unwrap(){Some(v)=>break v,None=>std::thread::yield_now()}};
   assert!(!view.source_ready_at(&source,750_000_000,10_750));
   assert!(view.active.is_empty());
   assert_eq!(view.sources.get(&source).unwrap().health.state,reference.state(&source).unwrap().state);
   // The caller supplies only Engine-validated observations; the first is
   // the reconstructed recovery snapshot, then two valid updates.
   for t in [800u64,1000,1200]{
    let ns=t*1_000_000;let wall=10_000+t as i64;
    event!(UcmWorkerEvent::Observe{source:source.clone(),received_ns:ns,
      exchange_ms:wall,received_wall_ms:wall,now_ns:ns,now_ms:wall,valid:true});
    reference.observe_with_receive_wall(&source,ns,wall,wall,ns,wall,true);
   }
   let mut view=w.begin_scoped_snapshot(rev,&source).unwrap();
   let view=loop{match w.poll_snapshot(&mut view).unwrap(){Some(v)=>break v,None=>std::thread::yield_now()}};
   let health=&view.sources.get(&source).unwrap().health;
   assert_eq!(health.good_samples,reference.state(&source).unwrap().good_samples);
   assert_eq!(health.hot_ready,reference.state(&source).unwrap().hot_ready);
   assert_eq!(health.errors,reference.state(&source).unwrap().errors);
   assert_eq!(health.reconnects,reference.state(&source).unwrap().reconnects);
   assert!(view.source_ready_at(&source,2_000_000_000,12_000));
   assert!(!view.source_ready_at(&source,2_001_000_000,12_001));
  }
 }
 #[test]
 fn r1402_okx_book_reset_never_revives_retired_socket(){
  for action in [UcmWorkerEvent::Close as fn(SourceId)->UcmWorkerEvent, UcmWorkerEvent::Reconnect,UcmWorkerEvent::Quarantine]{
   let mut w=UcmWorker::start(QualityPolicy::default(),16).unwrap();
   let mut source=id("okx","BTC","a");source.channel="books".into();
   w.apply(1,UcmWorkerEvent::Connect(source.clone())).unwrap();
   w.apply(2,UcmWorkerEvent::Subscribe(source.clone())).unwrap();
   w.apply(3,UcmWorkerEvent::BookReset{source:source.clone(),sequence_gap:true}).unwrap();
   w.apply(4,action(source.clone())).unwrap();
   w.apply(5,UcmWorkerEvent::Observe{source:source.clone(),received_ns:1_000_000,
     exchange_ms:10_001,received_wall_ms:10_001,now_ns:1_000_000,now_ms:10_001,valid:true}).unwrap();
   assert!(w.propose(5,vec![source],1_000_000,10_001).is_err());
   assert!(!w.healthy());
  }
 }

 #[test]
 fn r1402_history_ordered_parity_and_retirement(){
  let mut w=UcmWorker::start(QualityPolicy::default(),16).unwrap();
  let mut reference=UniversalConnectionManager::new(QualityPolicy::default());
  let source=id("okx","BTC","a");
  w.apply(1,UcmWorkerEvent::Connect(source.clone())).unwrap();
  reference.connect(source.clone());
  let mut receipts=Vec::new();
  for (revision,ns,hot,mask) in [(2,100,false,1),(3,200,true,7),(4,300,false,3),(5,450,true,7)] {
   let masks=vec![("a".to_string(),mask),("b".to_string(),mask)];
   let previous=masks.iter().map(|(session,mask)|
    reference.observe_readiness_mask("BTC",session,*mask)).collect();
   let dual_hot=reference.observe_dual_hot("BTC",hot,ns);
   let pending=w.begin_history(revision,UcmHistoryEvent::Scope{
    scope:"BTC".into(),masks,dual_hot:hot,now_ns:ns}).unwrap();
   receipts.push((pending,UcmHistoryReceipt{revision,
    result:UcmHistoryResult::Scope{previous,dual_hot}}));
  }
  w.apply(6,UcmWorkerEvent::Close(source.clone())).unwrap();
  reference.close(&source);
  let retire=w.begin_history(7,UcmHistoryEvent::RetireSession{session:"a".into()}).unwrap();
  let retired=reference.retire_readiness_session("a");
  let census=w.begin_history(8,UcmHistoryEvent::Census).unwrap();
  // Later close and retirement cannot invalidate earlier historical facts.
  for (pending,expected) in receipts{
   assert_eq!(r1402_history_receipt(&mut w,pending),expected);
  }
  assert_eq!(r1402_history_receipt(&mut w,retire),
    UcmHistoryReceipt{revision:7,result:UcmHistoryResult::Retired(retired)});
  assert_eq!(r1402_history_receipt(&mut w,census),
    UcmHistoryReceipt{revision:8,result:UcmHistoryResult::Census{
     entries:reference.readiness_previous_len(),
     payload_capacity_bytes:reference.readiness_previous_payload_capacity_bytes()}});
  // No history receipt revives the closed source.
  let mut view=w.begin_scoped_snapshot(8,&source).unwrap();
  let view=loop{match w.poll_snapshot(&mut view).unwrap(){
    Some(view)=>break view,None=>std::thread::yield_now(),
  }};
  assert_eq!(view.sources.get(&source).unwrap().health.state,Some(Lifecycle::Closed));
  assert!(!view.source_ready_at(&source,450,10_000));
 }
 #[test]
 fn r1402_history_does_not_revoke_scoped_health_view(){
  let mut w=UcmWorker::start(QualityPolicy::default(),16).unwrap();
  let source=id("okx","BTC","a");
  w.apply(1,UcmWorkerEvent::Connect(source.clone())).unwrap();
  let mut view=w.begin_scoped_snapshot(1,&source).unwrap();
  let history=w.begin_history(2,UcmHistoryEvent::Scope{
   scope:"BTC".into(),masks:vec![("a".into(),1)],dual_hot:false,now_ns:100}).unwrap();
  assert_eq!(r1402_history_receipt(&mut w,history).revision,2);
  let view=loop{match w.poll_snapshot(&mut view).unwrap(){
    Some(view)=>break view,None=>std::thread::yield_now(),
  }};
  assert_eq!(view.revision,1);
 }
 #[test]
 fn r1402_history_full_missing_and_revision_gap_fail_closed(){
  let (tx,_rx)=std::sync::mpsc::sync_channel(1);
  let mut w=UcmWorker{sender:Some(tx),profile:None,published:None,thread:None,submitted_ticket:0,completed_ticket:std::sync::Arc::new(AtomicU64::new(0)),last_revision:0,
   scope_revisions:HashMap::new(),scope_tickets:HashMap::new(),scope_ticket_order:Default::default(),needs_facts:HashSet::new(),receive_age_ns:800_000_000,exchange_age_ms:800,destructive_revisions:HashMap::new(),command_deadlines:vec![None;1026],healthy:true,owner_failed:std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),response_timeout:Duration::from_millis(2)};
  let _pending=w.begin_history(1,UcmHistoryEvent::Census).unwrap();
  assert!(w.begin_history(2,UcmHistoryEvent::Census).is_err());
  assert!(!w.healthy());
  let (tx,_rx)=std::sync::mpsc::sync_channel(1);
  let mut w=UcmWorker{sender:Some(tx),profile:None,published:None,thread:None,submitted_ticket:0,completed_ticket:std::sync::Arc::new(AtomicU64::new(0)),last_revision:0,
   scope_revisions:HashMap::new(),scope_tickets:HashMap::new(),scope_ticket_order:Default::default(),needs_facts:HashSet::new(),receive_age_ns:800_000_000,exchange_age_ms:800,destructive_revisions:HashMap::new(),command_deadlines:vec![None;1026],healthy:true,owner_failed:std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),response_timeout:Duration::from_millis(2)};
  let mut pending=w.begin_history(1,UcmHistoryEvent::Census).unwrap();
  assert!(w.poll_history(&mut pending).unwrap().is_none());
  std::thread::sleep(Duration::from_millis(5));
  assert!(w.poll_history(&mut pending).is_err());
  assert!(!w.healthy());
  let mut w=UcmWorker::start(QualityPolicy::default(),16).unwrap();
  assert!(w.begin_history(2,UcmHistoryEvent::Census).is_err());
  assert!(!w.healthy());
 }

 #[test]
 fn r1401_scope_fence_independent_btc_eth_queries_and_tick_invalidation() {
  let mut w=UcmWorker::start(QualityPolicy::default(),32).unwrap();
  let btc=id("okx","BTC","btcsocket");
  let eth=id("okx","ETH","ethsocket");
  w.apply(1,UcmWorkerEvent::Connect(btc.clone())).unwrap();
  w.apply(2,UcmWorkerEvent::Subscribe(btc.clone())).unwrap();
  w.apply(3,UcmWorkerEvent::Connect(eth.clone())).unwrap();
  w.apply(4,UcmWorkerEvent::Subscribe(eth.clone())).unwrap();
  let mut rev=4;
  for t in [100u64,300,600] {
   let ns=t*1_000_000;let wall=10_000+t as i64;
   for id in [&btc,&eth] {
    rev+=1;
    w.apply(rev,UcmWorkerEvent::Observe{
     source:id.clone(),received_ns:ns,exchange_ms:wall,
     received_wall_ms:wall,now_ns:ns,now_ms:wall,valid:true}).unwrap();
   }
  }
  assert_eq!(rev,10);
  let mut btc_snapshot=w.begin_scoped_snapshot(10,&btc).unwrap();
  let mut btc_proposal=w.begin_scoped_proposal(10,&btc,vec![btc.clone()],
   600_000_000,10_600).unwrap();
  // ETH arrives AFTER BTC requests. This must NOT revoke BTC versions.
  w.apply(11,UcmWorkerEvent::Observe{
   source:eth.clone(),received_ns:650_000_000,exchange_ms:10_650,
   received_wall_ms:10_650,now_ns:650_000_000,now_ms:10_650,valid:true}).unwrap();
  let view=loop {match w.poll_snapshot(&mut btc_snapshot).unwrap(){
   Some(v)=>break v,None=>std::thread::yield_now(),
  }};
  assert_eq!(view.revision,10);
  assert_eq!(view.sources.len(),1);
  assert!(view.sources.contains_key(&btc));
  assert!(!view.sources.contains_key(&eth));
  assert!(view.source_ready_at(&btc,600_000_000,10_600));
  let winner=loop {match w.poll_proposal(&mut btc_proposal).unwrap(){
   Some(v)=>break v,None=>std::thread::yield_now(),
  }};
  assert_eq!(winner.revision,10);
  assert_eq!(winner.candidate.unwrap().0,btc);
  assert!(w.healthy());
  // BTC change invalidates old BTC reply (regardless of ETH).
  let mut stale=w.begin_scoped_snapshot(11,&btc).unwrap();
  w.apply(12,UcmWorkerEvent::Observe{
   source:btc.clone(),received_ns:700_000_000,exchange_ms:10_700,
   received_wall_ms:10_700,now_ns:700_000_000,now_ms:10_700,valid:true}).unwrap();
  assert!(w.poll_snapshot(&mut stale).is_err());
  // A Tick might expire ANY scope and must invalidate both scopes.
  let mut before_tick=w.begin_scoped_snapshot(12,&btc).unwrap();
  w.apply(13,UcmWorkerEvent::Tick{now_ns:1_501_000_000,now_ms:11_501}).unwrap();
  assert!(w.poll_snapshot(&mut before_tick).is_err());
 }

 #[test]
 fn r1401_commit_requires_applied_ack_and_scope_revision() {
  let mut w=UcmWorker::start(QualityPolicy::default(),16).unwrap();
  let id=id("okx","BTC","primary");
  w.apply(1,UcmWorkerEvent::Connect(id.clone())).unwrap();
  w.apply(2,UcmWorkerEvent::Subscribe(id.clone())).unwrap();
  for (rev,t) in [(3u64,100u64),(4,300),(5,600)]{
   let ns=t*1_000_000;let wall=10_000+t as i64;
   w.apply(rev,UcmWorkerEvent::Observe{
    source:id.clone(),received_ns:ns,exchange_ms:wall,
    received_wall_ms:wall,now_ns:ns,now_ms:wall,valid:true}).unwrap();
  }
  let mut commit=w.begin_commit(6,id.clone(),600_000_000,10_600).unwrap();
  let applied=loop {match w.poll_commit(&mut commit).unwrap(){
   Some(v)=>break v,None=>std::thread::yield_now(),
  }};
  assert_eq!(applied.revision,6,"only actual manager set_active returns receipt");
  assert_eq!(applied.active.get(&scope(&id)),Some(&id));
  assert!(applied.source_ready_at(&id,1_400_000_000,11_400));
  assert!(!applied.source_ready_at(&id,1_401_000_000,11_401));
  let mut snap=w.begin_scoped_snapshot(6,&id).unwrap();
  let health=loop{match w.poll_snapshot(&mut snap).unwrap(){
   Some(v)=>break v,None=>std::thread::yield_now(),
  }};
  assert_eq!(health.active.get(&scope(&id)),Some(&id));
  assert!(health.source_ready_at(&id,1_400_000_000,11_400));
  assert!(!health.source_ready_at(&id,1_401_000_000,11_401));
  // Even an ACK computed before Close must be rejected if Close was queued
  // before Engine consumed the old receipt.
  let mut pending=w.begin_commit(7,id.clone(),600_000_000,10_600).unwrap();
  w.apply(8,UcmWorkerEvent::Close(id.clone())).unwrap();
  assert!(w.poll_commit(&mut pending).is_err());
  let mut closed=w.begin_scoped_snapshot(8,&id).unwrap();
  let view=loop{match w.poll_snapshot(&mut closed).unwrap(){
   Some(v)=>break v,None=>std::thread::yield_now(),
  }};
  assert_eq!(view.sources.get(&id).unwrap().health.state,Some(Lifecycle::Closed));
  assert!(!view.source_ready_at(&id,600_000_000,10_600));
  assert!(w.healthy());
 }

 #[test]
 fn r1401_commit_old_quote_or_full_mailbox_fail_closed() {
  let mut w=UcmWorker::start(QualityPolicy::default(),8).unwrap();
  let id=id("okx","BTC","primary");
  w.apply(1,UcmWorkerEvent::Connect(id.clone())).unwrap();
  w.apply(2,UcmWorkerEvent::Subscribe(id.clone())).unwrap();
  for (rev,t) in [(3u64,100u64),(4,300),(5,600)]{
   let ns=t*1_000_000;let wall=10_000+t as i64;
   w.apply(rev,UcmWorkerEvent::Observe{
    source:id.clone(),received_ns:ns,exchange_ms:wall,
    received_wall_ms:wall,now_ns:ns,now_ms:wall,valid:true}).unwrap();
  }
  // 801ms at commit time; worker must reject even if earlier qualified.
  let mut stale=w.begin_commit(6,id.clone(),1_401_000_000,11_401).unwrap();
  assert!(loop {match w.poll_commit(&mut stale){
   Ok(None)=>{std::thread::yield_now();continue},
   Err(_)=>break true,Ok(Some(_))=>break false,
  }});
  assert!(!w.healthy());
  // Bounded unserviced mailbox: commit may not be silently dropped while
  // accepting an imagined success or blocking Queue32.
  let (tx,_rx)=std::sync::mpsc::sync_channel(1);
  let mut offline=UcmWorker{sender:Some(tx),profile:None,published:None,thread:None,submitted_ticket:0,completed_ticket:std::sync::Arc::new(AtomicU64::new(0)),last_revision:0,
   scope_revisions:HashMap::new(),scope_tickets:HashMap::new(),scope_ticket_order:Default::default(),needs_facts:HashSet::new(),receive_age_ns:800_000_000,exchange_age_ms:800,destructive_revisions:HashMap::new(),command_deadlines:vec![None;1026],healthy:true,owner_failed:std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),response_timeout:Duration::from_millis(2)};
  offline.apply(1,UcmWorkerEvent::Tick{now_ns:0,now_ms:0}).unwrap();
  assert!(offline.begin_commit(2,id,600_000_000,10_600).is_err());
  assert!(!offline.healthy());
 }

 #[test]
 fn r1401_scoped_proposal_rejects_foreign_instrument() {
  let mut w=UcmWorker::start(QualityPolicy::default(),4).unwrap();
  let btc=id("okx","BTC","primary");
  let eth=id("okx","ETH","other");
  assert!(w.begin_scoped_proposal(0,&btc,vec![eth],0,10_000).is_err());
  assert!(!w.healthy());
 }

 #[test]
 fn r1400_nonblocking_worker_snapshot_parity_and_800_801_boundary() {
  let mut worker=UcmWorker::start(QualityPolicy::default(),16).unwrap();
  let mut reference=UniversalConnectionManager::new(QualityPolicy::default());
  let a=id("okx","BTC","a");
  worker.apply(1,UcmWorkerEvent::Connect(a.clone())).unwrap();reference.connect(a.clone());
  worker.apply(2,UcmWorkerEvent::Subscribe(a.clone())).unwrap();reference.subscribed(&a);
  for (rev,t) in [(3u64,100u64),(4,300),(5,600)] {
   let ns=t*1_000_000;let wall=10_000+t as i64;
   worker.apply(rev,UcmWorkerEvent::Observe{source:a.clone(),received_ns:ns,
     exchange_ms:wall,received_wall_ms:wall,now_ns:ns,now_ms:wall,valid:true}).unwrap();
   reference.observe_with_receive_wall(&a,ns,wall,wall,ns,wall,true);
  }
  let mut pending=worker.begin_snapshot(5).unwrap();
  let snapshot=loop{match worker.poll_snapshot(&mut pending).unwrap(){
   Some(view)=>break view,None=>std::thread::yield_now(),
  }};
  assert_eq!(snapshot.revision,5);
  assert_eq!(snapshot.sources.len(),1);
  let source=snapshot.sources.get(&a).unwrap();
  assert_eq!(source.health.hot_ready,reference.state(&a).unwrap().hot_ready);
  assert_eq!(source.health.good_samples,reference.state(&a).unwrap().good_samples);
  assert_eq!(source.quality,reference.quality(&a));
  assert_eq!(source.qualification_requirements,reference.qualification_requirements(&a));
  assert_eq!(source.health.last_valid_receive_ns,Some(600_000_000));
  assert!(snapshot.source_ready_at(&a,1_400_000_000,11_400));
  assert!(!snapshot.source_ready_at(&a,1_401_000_000,11_401));
  assert!(!snapshot.source_ready_at(&a,1_400_000_000,11_401));
  assert!(snapshot.active.is_empty());
  // A completed old reply MUST NOT survive a later lifecycle revision.
  let mut old=worker.begin_snapshot(5).unwrap();
  worker.apply(6,UcmWorkerEvent::Close(a.clone())).unwrap();
  assert!(worker.poll_snapshot(&mut old).is_err());
  let mut new=worker.begin_snapshot(6).unwrap();
  let closed=loop{match worker.poll_snapshot(&mut new).unwrap(){
   Some(view)=>break view,None=>std::thread::yield_now(),
  }};
  assert_eq!(closed.revision,6);
  assert_eq!(closed.sources.get(&a).unwrap().health.state,Some(Lifecycle::Closed));
  assert!(!closed.source_ready_at(&a,1_400_000_000,11_400));
  assert!(worker.healthy());
 }

 #[test]
 fn r1400_snapshot_full_mailbox_poison_no_unverified_health() {
  let (tx,_rx)=std::sync::mpsc::sync_channel(1);
  let mut worker=UcmWorker{sender:Some(tx),profile:None,published:None,thread:None,submitted_ticket:0,completed_ticket:std::sync::Arc::new(AtomicU64::new(0)),
   last_revision:0,scope_revisions:HashMap::new(),scope_tickets:HashMap::new(),scope_ticket_order:Default::default(),needs_facts:HashSet::new(),receive_age_ns:800_000_000,exchange_age_ms:800,destructive_revisions:HashMap::new(),command_deadlines:vec![None;1026],healthy:true,owner_failed:std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),response_timeout:Duration::from_millis(2)};
  worker.apply(1,UcmWorkerEvent::Tick{now_ns:0,now_ms:0}).unwrap();
  assert!(worker.begin_snapshot(1).is_err());
  assert!(!worker.healthy());
 }

 #[test]
 fn r1399_worker_old_generation_late_event_poison_is_fail_closed() {
  let mut worker=UcmWorker::start(QualityPolicy::default(),8).unwrap();
  let old=id("okx","BTC","socket");
  let mut newer=old.clone();newer.generation+=1;
  worker.apply(1,UcmWorkerEvent::Connect(old.clone())).unwrap();
  worker.apply(2,UcmWorkerEvent::Subscribe(old.clone())).unwrap();
  worker.apply(3,UcmWorkerEvent::Close(old.clone())).unwrap();
  worker.apply(4,UcmWorkerEvent::Connect(newer.clone())).unwrap();
  worker.apply(5,UcmWorkerEvent::Subscribe(newer.clone())).unwrap();
  // An event from the retired generation MUST poison the worker rather than
  // refreshing SourceHealth after its Close; Engine would also reject it.
  worker.apply(6,UcmWorkerEvent::Observe{source:old,received_ns:600_000_000,
   exchange_ms:10_600,received_wall_ms:10_600,now_ns:600_000_000,
   now_ms:10_600,valid:true}).unwrap();
  assert!(worker.propose(6,vec![newer],600_000_000,10_600).is_err());
  assert!(!worker.healthy());
 }

 #[test]
 fn r1399_worker_full_mailbox_and_no_reply_fail_closed() {
  // A deliberately unserviced bounded mailbox: deterministic pressure fault.
  let (tx,_rx)=std::sync::mpsc::sync_channel(1);
  let mut worker=UcmWorker{sender:Some(tx),profile:None,published:None,thread:None,submitted_ticket:0,completed_ticket:std::sync::Arc::new(AtomicU64::new(0)),
   last_revision:0,scope_revisions:HashMap::new(),scope_tickets:HashMap::new(),scope_ticket_order:Default::default(),needs_facts:HashSet::new(),receive_age_ns:800_000_000,exchange_age_ms:800,destructive_revisions:HashMap::new(),command_deadlines:vec![None;1026],healthy:true,owner_failed:std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),response_timeout:Duration::from_millis(2)};
  worker.apply(1,UcmWorkerEvent::Tick{now_ns:0,now_ms:0}).unwrap();
  assert!(worker.apply(2,UcmWorkerEvent::Tick{now_ns:1,now_ms:1}).is_err());
  assert!(!worker.healthy());
  assert!(worker.propose(1,vec![],1,1).is_err());
  // No worker servicing a query: no infinite wait and never fake success.
  let (tx,_rx)=std::sync::mpsc::sync_channel(1);
  let mut idle=UcmWorker{sender:Some(tx),profile:None,published:None,thread:None,submitted_ticket:0,completed_ticket:std::sync::Arc::new(AtomicU64::new(0)),
   last_revision:0,scope_revisions:HashMap::new(),scope_tickets:HashMap::new(),scope_ticket_order:Default::default(),needs_facts:HashSet::new(),receive_age_ns:800_000_000,exchange_age_ms:800,destructive_revisions:HashMap::new(),command_deadlines:vec![None;1026],healthy:true,owner_failed:std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),response_timeout:Duration::from_millis(2)};
  assert!(idle.propose(0,vec![],0,0).is_err());
  assert!(!idle.healthy());
 }

}
