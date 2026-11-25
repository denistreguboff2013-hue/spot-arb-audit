#![recursion_limit = "256"]
// Isolated R1329 diagnostic: per-connection Tokio task, bounded quote channel, no trading.
use futures_util::{SinkExt,StreamExt};
use serde_json::{json,Value};
use std::{collections::{HashMap,HashSet,VecDeque},sync::{Arc,atomic::{AtomicU64,Ordering}},time::{Duration,Instant,SystemTime,UNIX_EPOCH}};
use tokio::{sync::mpsc,task::JoinSet};
use tokio_tungstenite::{client_async_tls,tungstenite::Message,WebSocketStream};
use tokio::io::{AsyncRead,AsyncWrite,ReadBuf,Interest};
use std::{pin::Pin,task::{Context,Poll}};
use std::net::{IpAddr,SocketAddr};
#[cfg(target_os="linux")]use std::os::fd::AsRawFd;
fn ms()->u64{SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64}
// Diagnostic stage histograms: 0-99us,100-499us,500-999us,1-4.999ms,5-9.999ms,10-49.999ms,>=50ms.
fn stage_bin(us:u64)->usize{if us<100{0}else if us<500{1}else if us<1000{2}else if us<5000{3}else if us<10000{4}else if us<50000{5}else{6}}
#[derive(Default)]struct StageStats{count:AtomicU64,max_us:AtomicU64,bins:[AtomicU64;7]}
impl StageStats{fn add(&self,us:u64){self.count.fetch_add(1,Ordering::Relaxed);self.max_us.fetch_max(us,Ordering::Relaxed);self.bins[stage_bin(us)].fetch_add(1,Ordering::Relaxed);}fn snapshot(&self)->Value{json!({"count":self.count.load(Ordering::Relaxed),"max_us":self.max_us.load(Ordering::Relaxed),"bins":self.bins.iter().map(|v|v.load(Ordering::Relaxed)).collect::<Vec<_>>()})}}
#[derive(Default)]struct ReaderStages{ws_wait:StageStats,json_parse:StageStats,decode:StageStats,enqueue:StageStats,full:AtomicU64,closed:AtomicU64}
impl ReaderStages{fn snapshot(&self)->Value{json!({"ws_wait":self.ws_wait.snapshot(),"json_parse":self.json_parse.snapshot(),"decode":self.decode.snapshot(),"enqueue":self.enqueue.snapshot(),"full":self.full.load(Ordering::Relaxed),"closed":self.closed.load(Ordering::Relaxed)})}}
// TCP ingress tap: poll_read on the actual plaintext TCP socket, before TLS/WS parsing.
fn mono_us()->u64{static ORIGIN:std::sync::OnceLock<Instant>=std::sync::OnceLock::new();ORIGIN.get_or_init(Instant::now).elapsed().as_micros().min(u128::from(u64::MAX-1)) as u64+1}
#[derive(Default)]struct TcpTapStats{read_calls:AtomicU64,read_bytes:AtomicU64,pending:AtomicU64,read_max_us:AtomicU64,first_read_us:AtomicU64,last_read_us:AtomicU64,first_read_wall_ms:AtomicU64,first_read_recvq_bytes:AtomicU64,first_read_recvq_before_bytes:AtomicU64,first_read_recvq_before_errors:AtomicU64,first_read_recvq_errors:AtomicU64,kernel_samples:AtomicU64,kernel_recvq_max:AtomicU64,kernel_last_recv_max_ms:AtomicU64,kernel_last_recv_over800:AtomicU64,kernel_recvq_over0:AtomicU64,kernel_sample_errors:AtomicU64,read_gap_max_us:AtomicU64,read_gap_over_1ms:AtomicU64,read_gap_over_10ms:AtomicU64,read_gap_over_100ms:AtomicU64,last_read_poll_us:AtomicU64,read_size_max:AtomicU64,read_ready_bytes_calls:AtomicU64,gap_queue_samples:AtomicU64,gap_queue_nonzero:AtomicU64,gap_queue_bytes_max:AtomicU64,gap_queue_over100ms_nonzero:AtomicU64,gap_queue_errors:AtomicU64,midgap_samples:AtomicU64,midgap_queue_nonzero:AtomicU64,midgap_queue_over10ms:AtomicU64,midgap_queue_over100ms:AtomicU64,midgap_queue_max:AtomicU64,midgap_age_max_us:AtomicU64,midgap_errors:AtomicU64,frame_kernel_first_wall_ns:AtomicU64,frame_kernel_last_wall_ns:AtomicU64,frame_kernel_first_mono_us:AtomicU64,frame_kernel_last_mono_us:AtomicU64,frame_recvmsg_count:AtomicU64,frame_timestamp_missing:AtomicU64,frame_tcp_bytes:AtomicU64,last_any_tcp_read_mono_us:AtomicU64,last_any_kernel_wall_ns:AtomicU64,last_any_kernel_mono_us:AtomicU64,total_tcp_bytes:AtomicU64}
impl TcpTapStats{fn snapshot(&self)->Value{json!({"calls":self.read_calls.load(Ordering::Relaxed),"bytes":self.read_bytes.load(Ordering::Relaxed),"pending":self.pending.load(Ordering::Relaxed),"poll_max_us":self.read_max_us.load(Ordering::Relaxed),"kernel_samples":self.kernel_samples.load(Ordering::Relaxed),"kernel_recvq_max":self.kernel_recvq_max.load(Ordering::Relaxed),"kernel_last_recv_max_ms":self.kernel_last_recv_max_ms.load(Ordering::Relaxed),"kernel_last_recv_over800":self.kernel_last_recv_over800.load(Ordering::Relaxed),"kernel_recvq_over0":self.kernel_recvq_over0.load(Ordering::Relaxed),"kernel_sample_errors":self.kernel_sample_errors.load(Ordering::Relaxed),"read_gap_max_us":self.read_gap_max_us.load(Ordering::Relaxed),"read_gap_over_1ms":self.read_gap_over_1ms.load(Ordering::Relaxed),"read_gap_over_10ms":self.read_gap_over_10ms.load(Ordering::Relaxed),"read_gap_over_100ms":self.read_gap_over_100ms.load(Ordering::Relaxed),"read_size_max":self.read_size_max.load(Ordering::Relaxed),"read_ready_bytes_calls":self.read_ready_bytes_calls.load(Ordering::Relaxed),"gap_queue_samples":self.gap_queue_samples.load(Ordering::Relaxed),"gap_queue_nonzero":self.gap_queue_nonzero.load(Ordering::Relaxed),"gap_queue_bytes_max":self.gap_queue_bytes_max.load(Ordering::Relaxed),"gap_queue_over100ms_nonzero":self.gap_queue_over100ms_nonzero.load(Ordering::Relaxed),"gap_queue_errors":self.gap_queue_errors.load(Ordering::Relaxed),"midgap_samples":self.midgap_samples.load(Ordering::Relaxed),"midgap_queue_nonzero":self.midgap_queue_nonzero.load(Ordering::Relaxed),"midgap_queue_over10ms":self.midgap_queue_over10ms.load(Ordering::Relaxed),"midgap_queue_over100ms":self.midgap_queue_over100ms.load(Ordering::Relaxed),"midgap_queue_max":self.midgap_queue_max.load(Ordering::Relaxed),"midgap_age_max_us":self.midgap_age_max_us.load(Ordering::Relaxed),"midgap_errors":self.midgap_errors.load(Ordering::Relaxed)})}}
// Linux TCP_INFO tcpi_last_data_recv offset 52 verified against /usr/include/linux/tcp.h on server 2026-11-21.
// This field is time since MOST RECENT TCP data reception, not age of oldest queued byte.
#[cfg(target_os="linux")]
fn sample_kernel_tcp(fd:std::os::fd::RawFd,stats:&TcpTapStats){
 let mut queued:libc::c_int=0;
 let q_ok=unsafe{libc::ioctl(fd,libc::FIONREAD,&mut queued)}==0;
 let mut info=[0u8;64];let mut len=info.len() as libc::socklen_t;
 let t_ok=unsafe{libc::getsockopt(fd,libc::IPPROTO_TCP,libc::TCP_INFO,info.as_mut_ptr().cast(),&mut len)}==0&&len>=56;
 if !q_ok||!t_ok{stats.kernel_sample_errors.fetch_add(1,Ordering::Relaxed);return;}
 stats.kernel_samples.fetch_add(1,Ordering::Relaxed);
 stats.kernel_recvq_max.fetch_max(queued.max(0) as u64,Ordering::Relaxed);
 if queued>0{
  stats.kernel_recvq_over0.fetch_add(1,Ordering::Relaxed);
  let since=u32::from_ne_bytes(info[52..56].try_into().unwrap()) as u64;
  stats.kernel_last_recv_max_ms.fetch_max(since,Ordering::Relaxed);
  if since>800{stats.kernel_last_recv_over800.fetch_add(1,Ordering::Relaxed);}
 }
}
// Independent diagnostic timer: samples FIONREAD while WS reader may not be polling TCP.
// No socket reads; dup fd is closed on timer exit. Samples show queue at timer tick,
// not the age of individual TCP bytes; timer itself may be delayed by runtime scheduling.
#[cfg(target_os="linux")]
fn start_midgap_sampler(tcp:&tokio::net::TcpStream,stats:&Arc<TcpTapStats>){
 use std::os::fd::{FromRawFd,OwnedFd};
 let raw=unsafe{libc::dup(tcp.as_raw_fd())};
 if raw<0{stats.midgap_errors.fetch_add(1,Ordering::Relaxed);return;}
 let owned=unsafe{OwnedFd::from_raw_fd(raw)};
 let weak=Arc::downgrade(stats);
 tokio::spawn(async move{
  let mut timer=tokio::time::interval(Duration::from_millis(10));
  timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
  loop{timer.tick().await;
   let Some(st)=weak.upgrade() else{break};
   let mut queued:libc::c_int=0;
   if unsafe{libc::ioctl(owned.as_raw_fd(),libc::FIONREAD,&mut queued)}!=0{st.midgap_errors.fetch_add(1,Ordering::Relaxed);break;}
   st.midgap_samples.fetch_add(1,Ordering::Relaxed);
   if queued>0{st.midgap_queue_nonzero.fetch_add(1,Ordering::Relaxed);st.midgap_queue_max.fetch_max(queued as u64,Ordering::Relaxed);
    let last=st.last_read_poll_us.load(Ordering::Relaxed);if last>0{let elapsed=mono_us().saturating_sub(last);st.midgap_age_max_us.fetch_max(elapsed,Ordering::Relaxed);
     if elapsed>10000{st.midgap_queue_over10ms.fetch_add(1,Ordering::Relaxed);}
     if elapsed>100000{st.midgap_queue_over100ms.fetch_add(1,Ordering::Relaxed);}
    }
   }
  }
 });
}
struct TcpTap{inner:tokio::net::TcpStream,stats:Arc<TcpTapStats>}
impl TcpTap{
 fn new(inner:tokio::net::TcpStream,stats:Arc<TcpTapStats>)->std::io::Result<Self>{
  #[cfg(target_os="linux")]{
   let enabled:libc::c_int=1;
   let rc=unsafe{libc::setsockopt(inner.as_raw_fd(),libc::SOL_SOCKET,libc::SO_TIMESTAMPNS,(&enabled as *const libc::c_int).cast(),std::mem::size_of_val(&enabled) as libc::socklen_t)};
   if rc!=0{return Err(std::io::Error::last_os_error());}
  }
  Ok(Self{inner,stats})
 }
}
#[cfg(target_os="linux")]
fn recvmsg_timestamped(fd:std::os::fd::RawFd,out:&mut [u8])->std::io::Result<(usize,Option<u64>)>{
 let mut iov=libc::iovec{iov_base:out.as_mut_ptr().cast(),iov_len:out.len()};
 let mut control=[std::mem::MaybeUninit::<u64>::uninit();8];
 let mut msg:libc::msghdr=unsafe{std::mem::zeroed()};
 msg.msg_iov=&mut iov;msg.msg_iovlen=1;msg.msg_control=control.as_mut_ptr().cast();msg.msg_controllen=std::mem::size_of_val(&control);
 let n=unsafe{libc::recvmsg(fd,&mut msg,libc::MSG_DONTWAIT)};
 if n<0{return Err(std::io::Error::last_os_error());}
 let mut ts_ns=None;
 unsafe{
  let mut h=libc::CMSG_FIRSTHDR(&msg);
  while !h.is_null(){
   if (*h).cmsg_level==libc::SOL_SOCKET&&(*h).cmsg_type==libc::SCM_TIMESTAMPNS&&(*h).cmsg_len>=std::mem::size_of::<libc::cmsghdr>()+std::mem::size_of::<libc::timespec>(){
    let ts=std::ptr::read_unaligned(libc::CMSG_DATA(h).cast::<libc::timespec>());
    if ts.tv_sec>=0&&ts.tv_nsec>=0{ts_ns=Some((ts.tv_sec as u64).saturating_mul(1_000_000_000).saturating_add(ts.tv_nsec as u64));}
   }
   h=libc::CMSG_NXTHDR(&msg,h);
  }
 }
 Ok((n as usize,ts_ns))
}
impl AsyncRead for TcpTap{fn poll_read(mut self:Pin<&mut Self>,cx:&mut Context<'_>,buf:&mut ReadBuf<'_>)->Poll<std::io::Result<()>>{
 let start=Instant::now();
 #[cfg(target_os="linux")]
 let ready=match self.inner.poll_read_ready(cx){Poll::Ready(Ok(()))=>true,Poll::Ready(Err(e))=>return Poll::Ready(Err(e)),Poll::Pending=>{self.stats.pending.fetch_add(1,Ordering::Relaxed);return Poll::Pending}};
 #[cfg(not(target_os="linux"))]
 let ready=true;
 let _=ready;
 #[cfg(target_os="linux")]
 {
  let stamp=mono_us();let prev=self.stats.last_read_poll_us.swap(stamp,Ordering::Relaxed);
  if prev>0{let gap=stamp.saturating_sub(prev);self.stats.read_gap_max_us.fetch_max(gap,Ordering::Relaxed);if gap>1000{self.stats.read_gap_over_1ms.fetch_add(1,Ordering::Relaxed);}if gap>10000{self.stats.read_gap_over_10ms.fetch_add(1,Ordering::Relaxed);}if gap>100000{self.stats.read_gap_over_100ms.fetch_add(1,Ordering::Relaxed);}
   if gap>10000{let mut q:libc::c_int=0;if unsafe{libc::ioctl(self.inner.as_raw_fd(),libc::FIONREAD,&mut q)}==0{self.stats.gap_queue_samples.fetch_add(1,Ordering::Relaxed);if q>0{self.stats.gap_queue_nonzero.fetch_add(1,Ordering::Relaxed);self.stats.gap_queue_bytes_max.fetch_max(q as u64,Ordering::Relaxed);if gap>100000{self.stats.gap_queue_over100ms_nonzero.fetch_add(1,Ordering::Relaxed);}}}else{self.stats.gap_queue_errors.fetch_add(1,Ordering::Relaxed);}}
  }
  if std::env::var_os("R1329_DISABLE_KERNEL_SAMPLING").is_none()&&self.stats.read_calls.load(Ordering::Relaxed).wrapping_add(self.stats.pending.load(Ordering::Relaxed))%32==0{sample_kernel_tcp(self.inner.as_raw_fd(),&self.stats);}
  let mut queued:libc::c_int=0;
  let q_ok=unsafe{libc::ioctl(self.inner.as_raw_fd(),libc::FIONREAD,&mut queued)}==0;
  let cap=buf.remaining().min(16*1024);
  if cap==0{return Poll::Ready(Ok(()));}
  let mut tmp=[0u8;16*1024];let mut kernel_wall_ns=None;
  let result=loop{match self.inner.try_io(Interest::READABLE,||recvmsg_timestamped(self.inner.as_raw_fd(),&mut tmp[..cap]).map(|(n,t)|{kernel_wall_ns=t;(n,t)})){Ok(v)=>break v,Err(e) if e.kind()==std::io::ErrorKind::WouldBlock=>{self.stats.pending.fetch_add(1,Ordering::Relaxed);match self.inner.poll_read_ready(cx){Poll::Ready(Ok(()))=>continue,Poll::Pending=>return Poll::Pending,Poll::Ready(Err(e))=>return Poll::Ready(Err(e))}},Err(e)=>return Poll::Ready(Err(e))}};
  let (n,_)=result;
  self.stats.read_max_us.fetch_max(start.elapsed().as_micros() as u64,Ordering::Relaxed);
  self.stats.read_calls.fetch_add(1,Ordering::Relaxed);self.stats.read_bytes.fetch_add(n as u64,Ordering::Relaxed);
  if n>0{
   buf.put_slice(&tmp[..n]);self.stats.read_size_max.fetch_max(n as u64,Ordering::Relaxed);self.stats.read_ready_bytes_calls.fetch_add(1,Ordering::Relaxed);self.stats.frame_tcp_bytes.fetch_add(n as u64,Ordering::Relaxed);
   let read_mono=mono_us();let read_wall_ns=SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64;self.stats.last_any_tcp_read_mono_us.store(read_mono,Ordering::Relaxed);self.stats.total_tcp_bytes.fetch_add(n as u64,Ordering::Relaxed);
   if self.stats.first_read_us.compare_exchange(0,read_mono,Ordering::Relaxed,Ordering::Relaxed).is_ok(){self.stats.first_read_wall_ms.store(read_wall_ns/1_000_000,Ordering::Relaxed);if q_ok{self.stats.first_read_recvq_before_bytes.store(queued.max(0) as u64,Ordering::Relaxed);}else{self.stats.first_read_recvq_before_errors.fetch_add(1,Ordering::Relaxed);}}
   self.stats.last_read_us.store(read_mono,Ordering::Relaxed);
   let mut q_after:libc::c_int=0;if unsafe{libc::ioctl(self.inner.as_raw_fd(),libc::FIONREAD,&mut q_after)}==0{self.stats.first_read_recvq_bytes.store(q_after.max(0) as u64,Ordering::Relaxed);}else{self.stats.first_read_recvq_errors.fetch_add(1,Ordering::Relaxed);}
   self.stats.frame_recvmsg_count.fetch_add(1,Ordering::Relaxed);
   if let Some(ts)=kernel_wall_ns{
    let mono_est=(read_mono as i128-(read_wall_ns as i128-ts as i128)/1000).max(0) as u64;
    self.stats.frame_kernel_first_wall_ns.compare_exchange(0,ts,Ordering::Relaxed,Ordering::Relaxed).ok();
    self.stats.frame_kernel_last_wall_ns.store(ts,Ordering::Relaxed);
    self.stats.frame_kernel_first_mono_us.compare_exchange(0,mono_est,Ordering::Relaxed,Ordering::Relaxed).ok();
    self.stats.frame_kernel_last_mono_us.store(mono_est,Ordering::Relaxed);
    self.stats.last_any_kernel_wall_ns.store(ts,Ordering::Relaxed);self.stats.last_any_kernel_mono_us.store(mono_est,Ordering::Relaxed);
   }else{self.stats.frame_timestamp_missing.fetch_add(1,Ordering::Relaxed);}
  }
  self.stats.read_max_us.fetch_max(start.elapsed().as_micros() as u64,Ordering::Relaxed);
  Poll::Ready(Ok(()))
 }
}
}
impl AsyncWrite for TcpTap{fn poll_write(mut self:Pin<&mut Self>,cx:&mut Context<'_>,buf:&[u8])->Poll<std::io::Result<usize>>{Pin::new(&mut self.inner).poll_write(cx,buf)}
fn poll_flush(mut self:Pin<&mut Self>,cx:&mut Context<'_>)->Poll<std::io::Result<()>>{Pin::new(&mut self.inner).poll_flush(cx)}
fn poll_shutdown(mut self:Pin<&mut Self>,cx:&mut Context<'_>)->Poll<std::io::Result<()>>{Pin::new(&mut self.inner).poll_shutdown(cx)}}
fn rss()->u64{std::fs::read_to_string("/proc/self/status").ok().and_then(|s|s.lines().find(|l|l.starts_with("VmRSS:")).and_then(|l|l.split_whitespace().nth(1)).and_then(|v|v.parse().ok())).unwrap_or(0)}
fn num(v:&Value,k:&str)->Option<f64>{v[k].as_str()?.parse::<f64>().ok().filter(|x|x.is_finite()&&*x>0.)}
#[derive(Clone,Copy)]struct Quote{recv:u64,bid:f64,ask:f64,bid_qty:f64,ask_qty:f64,exchange_ts:Option<u64>,update_id:Option<u64>}
fn qty(v:&Value,k:&str)->Option<f64>{v[k].as_str()?.parse::<f64>().ok().filter(|x|x.is_finite()&&*x>0.)}
fn positive_u64(v:&Value)->Option<u64>{v.as_u64().filter(|x|*x>0).or_else(||v.as_str()?.parse::<u64>().ok().filter(|x|*x>0))}
fn same_bbo(a:Quote,b:Quote)->bool{
 a.bid==b.bid&&a.ask==b.ask&&a.bid_qty==b.bid_qty&&a.ask_qty==b.ask_qty
}
fn newer(previous:Quote,next:Quote)->bool{
 if let (Some(a),Some(b))=(previous.update_id,next.update_id){return b>a}
 if let (Some(a),Some(b))=(previous.exchange_ts,next.exchange_ts){
  return b>a || (b==a&&!same_bbo(previous,next));
 }
 false // never treat local arrival time as exchange identity
}
fn skew_matches(us:u64,identity:bool)->bool{us>=800_000&&identity}
fn skew_log_allowed(us:u64,identity:bool,logged:u64)->bool{skew_matches(us,identity)&&logged<8}
fn skew_event_record(trigger:u64,pair:&Value,prior:&VecDeque<Value>)->Value{json!({"trigger":trigger,"pair":pair,"prior_pairs":prior})}
// R1345: keep highest 8 exact monotonic matched-identity skew events, bounded in memory.
fn record_worst_skew(worst:&mut Vec<(u64,Value)>,delta:u64,pair:&Value,prior:&VecDeque<Value>)->bool {
 if worst.len()>=8 && worst.iter().all(|(v,_)|*v>=delta){return false;}
 let event=skew_event_record(0,pair,prior);
 if worst.len()<8 {worst.push((delta,event));}
 else if let Some((idx,_))=worst.iter().enumerate().min_by_key(|(_,x)|x.0){worst[idx]=(delta,event);}
 true
}
fn exchange_fresh(q:Quote)->bool {
 match q.exchange_ts {
  Some(t)=>q.recv.abs_diff(t)<=800,
  None=>false,
 }
}
fn decode(venue:usize,v:&Value,received_ms:u64)->Option<(String,Quote)>{
 if venue<100{let d=v.get("data").unwrap_or(v);let s=d["s"].as_str()?.strip_suffix("USDT")?;
 let(b,a)=(num(d,"b")?,num(d,"a")?);if b>a{return None}
 let (bid_qty,ask_qty)=(qty(d,"B")?,qty(d,"A")?);
 Some((s.to_string(),Quote{recv:received_ms,bid:b,ask:a,bid_qty,ask_qty,exchange_ts:positive_u64(&d["E"]),update_id:Some(positive_u64(&d["u"])? )}))}
 else{if v["arg"]["channel"]!="tickers"{return None}let d=v["data"].as_array()?.first()?;
 let s=d["instId"].as_str()?.strip_suffix("-USDT")?;let(b,a)=(num(d,"bidPx")?,num(d,"askPx")?);
 if b>a{return None}
 let (bid_qty,ask_qty)=(qty(d,"bidSz")?,qty(d,"askSz")?);
 Some((s.to_string(),Quote{recv:received_ms,bid:b,ask:a,bid_qty,ask_qty,exchange_ts:Some(positive_u64(&d["ts"])?),update_id:None}))}
}
// Historical registry is only a candidate source; never overrides live freshness qualification.
// Opt-in on isolated public-data scanner: R1329_CONNECTION_REGISTRY=/absolute/path/profiles.csv
fn ranked_binance_peers()->Vec<IpAddr>{
 let Ok(path)=std::env::var("R1329_CONNECTION_REGISTRY") else{return Vec::new()};
 let Ok(csv)=std::fs::read_to_string(path) else{return Vec::new()};
 let mut peers=Vec::<(IpAddr,f64)>::new();
 for line in csv.lines().skip(1){
  let cols=line.split(',').collect::<Vec<_>>();
  if cols.len()<19 || cols[1]!="Binance_USDM" || cols[4]!="fstream.binance.com" || cols[16]!="CANDIDATE_GOOD"{continue}
  let (Ok(ip),Ok(conf))=(cols[5].parse::<IpAddr>(),cols[17].parse::<f64>()) else{continue};
  if !ip.is_ipv4() || !conf.is_finite() || conf<=0.0 {continue}
  if !peers.iter().any(|(seen,_)|*seen==ip){peers.push((ip,conf));}
 }
 peers.sort_by(|a,b|b.1.total_cmp(&a.1));
 peers.into_iter().map(|(ip,_)|ip).collect()
}
async fn binance_registry_connect(url:&str,peer:IpAddr,tap:Arc<TcpTapStats>)->Result<WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpTap>>,String>{
 let tcp=tokio::net::TcpStream::connect(SocketAddr::new(peer,443)).await.map_err(|e|e.to_string())?;
 #[cfg(target_os="linux")] if std::env::var_os("R1329_DISABLE_MIDGAP").is_none(){start_midgap_sampler(&tcp,&tap);}
 let (ws,_)=client_async_tls(url,TcpTap::new(tcp,tap).map_err(|e|e.to_string())?).await.map_err(|e|e.to_string())?;
 Ok(ws)
}
async fn fallback_connect(url:&str,tap:Arc<TcpTapStats>)->Result<WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpTap>>,String>{
 let uri:tokio_tungstenite::tungstenite::http::Uri=url.parse().map_err(|e:tokio_tungstenite::tungstenite::http::uri::InvalidUri|e.to_string())?;
 let host=uri.host().ok_or("missing WS hostname")?;
 let port=uri.port_u16().unwrap_or(if uri.scheme_str()==Some("wss"){443}else{80});
 let tcp=tokio::net::TcpStream::connect((host,port)).await.map_err(|e|e.to_string())?;
 #[cfg(target_os="linux")] if std::env::var_os("R1329_DISABLE_MIDGAP").is_none(){start_midgap_sampler(&tcp,&tap);}
 client_async_tls(url,TcpTap::new(tcp,tap).map_err(|e|e.to_string())?).await.map(|(ws,_)|ws).map_err(|e|e.to_string())
}
// HOT selection is RAM-only. Session epochs fence queued events from old sockets.
#[derive(Default)]
struct HotSlot { epoch:u64, live:bool, last_recv_ms:u64 }
#[derive(Default)]
struct HotPair { slots:[HotSlot;2], owner:Option<usize> }
impl HotPair {
 fn connected(&mut self, copy:usize, epoch:u64) {
  let slot=&mut self.slots[copy];
  if epoch>slot.epoch { *slot=HotSlot{epoch,live:true,last_recv_ms:0}; }
 }
 fn disconnected(&mut self, copy:usize, epoch:u64) {
  let slot=&mut self.slots[copy];
  if slot.epoch==epoch { slot.live=false; if self.owner==Some(copy){self.owner=None;} }
 }
 fn admit(&mut self,copy:usize,epoch:u64,recv_ms:u64,now_ms:u64)->bool {
  if copy>1 || now_ms.saturating_sub(recv_ms)>800 {return false}
  let slot=&mut self.slots[copy];
  if !slot.live || slot.epoch!=epoch || recv_ms<slot.last_recv_ms {return false}
  slot.last_recv_ms=recv_ms;
  if let Some(owner)=self.owner {
   if owner!=copy && self.slots[owner].live && now_ms.saturating_sub(self.slots[owner].last_recv_ms)<=800 {return false}
  }
  self.owner=Some(copy);
  true
 }
}
#[derive(Clone,Copy,Default)]
struct ReadWitness {first_tcp_us:u64,last_tcp_us:u64,recvq_before:u64,recvq_after:u64,had_tcp:bool,ws_wait_us:u64,kernel_first_mono_us:u64,kernel_last_mono_us:u64,kernel_first_wall_ns:u64,kernel_last_wall_ns:u64,recvmsg_count:u64,timestamp_missing:u64,tcp_bytes:u64,last_socket_tcp_read_mono_us:u64,last_socket_kernel_mono_us:u64,last_socket_kernel_wall_ns:u64,connection_tcp_bytes:u64}
impl ReadWitness {fn as_json(&self)->Value{json!({"first_tcp_us":self.first_tcp_us,"last_tcp_us":self.last_tcp_us,"recvq_before_first_read":self.recvq_before,"recvq_after_first_read":self.recvq_after,"had_tcp_read":self.had_tcp,"ws_wait_us":self.ws_wait_us,"kernel_first_mono_us_est":self.kernel_first_mono_us,"kernel_last_mono_us_est":self.kernel_last_mono_us,"kernel_first_wall_ns":self.kernel_first_wall_ns,"kernel_last_wall_ns":self.kernel_last_wall_ns,"recvmsg_count":self.recvmsg_count,"timestamp_missing":self.timestamp_missing,"tcp_bytes":self.tcp_bytes,"last_socket_tcp_read_mono_us":self.last_socket_tcp_read_mono_us,"last_socket_kernel_mono_us_est":self.last_socket_kernel_mono_us,"last_socket_kernel_wall_ns":self.last_socket_kernel_wall_ns,"connection_tcp_bytes":self.connection_tcp_bytes})}}
enum Event {
 Quote{venue:usize,copy:usize,epoch:u64,symbol:String,quote:Quote,received_at:Instant,frame_mono_us:u64,parse_us:u64,witness:ReadWitness},
 Disconnected{venue:usize,copy:usize,epoch:u64},
}
async fn reader<S>(venue:usize,copy:usize,epoch:u64,mut ws:WebSocketStream<S>,tx:mpsc::Sender<Event>,drops:Arc<AtomicU64>,stages:Arc<ReaderStages>,tap:Arc<TcpTapStats>)->Result<(),String>
where S:tokio::io::AsyncRead+tokio::io::AsyncWrite+Unpin{
 let mut r1341_stale_logged=0u64;
 let mut interval_start=Instant::now();let mut interval_total=0u64;let mut interval_stale=0u64;let mut interval_age_max=0u64;let mut interval_with_tcp=0u64;let mut interval_buffered=0u64;let mut interval_stale_with_tcp=0u64;let mut interval_stale_buffered=0u64;let mut interval_first_to_frame_max=0u64;let mut interval_last_to_frame_max=0u64;let mut buffered_streak=0u64;let mut interval_buffered_streak_max=0u64;let mut interval_buffered_wait_max_us=0u64;let mut interval_buffered_wait_over_1ms=0u64;let mut interval_buffered_stale_wait_over_1ms=0u64;let mut interval_stale_age_at_tcp_max_ms=0u64;let mut interval_stale_at_tcp=0u64;let mut interval_future_ts=0u64;let mut interval_exchange_ts_backward=0u64;let mut interval_exchange_ts_gap_over800=0u64;let mut interval_update_id_nonincreasing=0u64;let mut previous_exchange_ts=HashMap::<String,u64>::new();let mut previous_update_id=HashMap::<String,u64>::new();
 loop{
  if interval_start.elapsed()>=Duration::from_secs(2){
   eprintln!("SOCKET_AGE venue={venue} copy={copy} epoch={epoch} local_time_ms={} total={interval_total} stale={interval_stale} max_age_ms={interval_age_max} with_tcp={interval_with_tcp} buffered={interval_buffered} stale_with_tcp={interval_stale_with_tcp} stale_buffered={interval_stale_buffered} first_to_frame_max_us={interval_first_to_frame_max} last_to_frame_max_us={interval_last_to_frame_max} buffered_streak_max={interval_buffered_streak_max} buffered_wait_max_us={interval_buffered_wait_max_us} buffered_wait_over_1ms={interval_buffered_wait_over_1ms} buffered_stale_wait_over_1ms={interval_buffered_stale_wait_over_1ms} stale_age_at_tcp_max_ms={interval_stale_age_at_tcp_max_ms} stale_at_tcp={interval_stale_at_tcp} future_ts={interval_future_ts} exchange_ts_backward={interval_exchange_ts_backward} exchange_ts_gap_over800={interval_exchange_ts_gap_over800} update_id_nonincreasing={interval_update_id_nonincreasing} tcp_tap={}",ms(),tap.snapshot());
   interval_start=Instant::now();interval_total=0;interval_stale=0;interval_age_max=0;interval_with_tcp=0;interval_buffered=0;interval_stale_with_tcp=0;interval_stale_buffered=0;interval_first_to_frame_max=0;interval_last_to_frame_max=0;interval_buffered_streak_max=0;interval_buffered_wait_max_us=0;interval_buffered_wait_over_1ms=0;interval_buffered_stale_wait_over_1ms=0;interval_stale_age_at_tcp_max_ms=0;interval_stale_at_tcp=0;interval_future_ts=0;interval_exchange_ts_backward=0;interval_exchange_ts_gap_over800=0;interval_update_id_nonincreasing=0;
  }
  tap.first_read_us.store(0,Ordering::Relaxed);tap.last_read_us.store(0,Ordering::Relaxed);tap.first_read_wall_ms.store(0,Ordering::Relaxed);tap.first_read_recvq_bytes.store(0,Ordering::Relaxed);tap.first_read_recvq_before_bytes.store(0,Ordering::Relaxed);tap.frame_kernel_first_wall_ns.store(0,Ordering::Relaxed);tap.frame_kernel_last_wall_ns.store(0,Ordering::Relaxed);tap.frame_kernel_first_mono_us.store(0,Ordering::Relaxed);tap.frame_kernel_last_mono_us.store(0,Ordering::Relaxed);tap.frame_recvmsg_count.store(0,Ordering::Relaxed);tap.frame_timestamp_missing.store(0,Ordering::Relaxed);tap.frame_tcp_bytes.store(0,Ordering::Relaxed);
  let ws_wait_start=Instant::now();
  let frame=tokio::time::timeout(Duration::from_secs(20),ws.next()).await.map_err(|_|format!("socket {venue}/{copy} idle timeout 20s"))?.ok_or_else(||format!("socket {venue}/{copy} ended"))?.map_err(|e|format!("socket {venue}/{copy}: {e}"))?;
  let received_at=Instant::now();let ws_wait_us=ws_wait_start.elapsed().as_micros() as u64;stages.ws_wait.add(ws_wait_us);let received_ms=ms();
  let first_read=tap.first_read_us.load(Ordering::Relaxed);let last_read=tap.last_read_us.load(Ordering::Relaxed);let frame_stamp=mono_us();let had_tcp_read=first_read>0;
  if had_tcp_read{buffered_streak=0;interval_with_tcp+=1;interval_first_to_frame_max=interval_first_to_frame_max.max(frame_stamp.saturating_sub(first_read));interval_last_to_frame_max=interval_last_to_frame_max.max(frame_stamp.saturating_sub(last_read));}else{interval_buffered+=1;buffered_streak+=1;interval_buffered_streak_max=interval_buffered_streak_max.max(buffered_streak);interval_buffered_wait_max_us=interval_buffered_wait_max_us.max(ws_wait_us);if ws_wait_us>1000{interval_buffered_wait_over_1ms+=1;}}

  match frame{
   Message::Text(t)=>{
    let parse_start=Instant::now();let parsed=serde_json::from_str::<Value>(&t);stages.json_parse.add(parse_start.elapsed().as_micros() as u64);
    if let Ok(v)=parsed{
     if v["event"]=="error"{return Err(format!("socket {venue}/{copy} subscription error"))}
     let decode_start=Instant::now();let decoded=decode(venue,&v,received_ms);stages.decode.add(decode_start.elapsed().as_micros() as u64);
     if let Some((symbol,quote))=decoded{
      interval_total+=1;
      if let Some(ts)=quote.exchange_ts{
       if ts>quote.recv{interval_future_ts+=1;}
       if let Some(prev)=previous_exchange_ts.insert(symbol.clone(),ts){if ts<prev{interval_exchange_ts_backward+=1;}else if ts-prev>800{interval_exchange_ts_gap_over800+=1;}}
       if let Some(uid)=quote.update_id{if let Some(prev)=previous_update_id.insert(symbol.clone(),uid){if uid<=prev{interval_update_id_nonincreasing+=1;}}}
       if had_tcp_read{let tcp_wall=tap.first_read_wall_ms.load(Ordering::Relaxed);if tcp_wall>0{let tcp_age=tcp_wall.saturating_sub(ts);interval_stale_age_at_tcp_max_ms=interval_stale_age_at_tcp_max_ms.max(tcp_age);if tcp_age>800{interval_stale_at_tcp+=1;}}}
       let age=quote.recv.saturating_sub(ts);
       if venue<100 && age>800 && r1341_stale_logged<256 {r1341_stale_logged+=1;
        eprintln!("R1341_STALE_FRAME {}",json!({"venue":venue,"copy":copy,"epoch":epoch,"symbol":symbol,"update_id":quote.update_id,"exchange_ts_ms":ts,"frame_wall_ms":received_ms,"frame_mono_us":frame_stamp,"first_tcp_read_mono_us":first_read,"last_tcp_read_mono_us":last_read,"first_tcp_read_wall_ms":tap.first_read_wall_ms.load(Ordering::Relaxed),"first_tcp_read_recvq_bytes_before_read":tap.first_read_recvq_before_bytes.load(Ordering::Relaxed),"first_tcp_read_recvq_bytes_after_read":tap.first_read_recvq_bytes.load(Ordering::Relaxed),"first_tcp_read_recvq_errors":tap.first_read_recvq_errors.load(Ordering::Relaxed),"had_tcp_read_for_this_ws_frame":had_tcp_read,"ws_next_wait_us":ws_wait_us,"age_ms":age,"kernel_first_mono_us_est":tap.frame_kernel_first_mono_us.load(Ordering::Relaxed),"kernel_last_mono_us_est":tap.frame_kernel_last_mono_us.load(Ordering::Relaxed),"kernel_first_wall_ns":tap.frame_kernel_first_wall_ns.load(Ordering::Relaxed),"kernel_last_wall_ns":tap.frame_kernel_last_wall_ns.load(Ordering::Relaxed),"recvmsg_count":tap.frame_recvmsg_count.load(Ordering::Relaxed),"timestamp_missing":tap.frame_timestamp_missing.load(Ordering::Relaxed),"tcp_bytes":tap.frame_tcp_bytes.load(Ordering::Relaxed),"last_socket_tcp_read_mono_us":tap.last_any_tcp_read_mono_us.load(Ordering::Relaxed),"last_socket_kernel_mono_us_est":tap.last_any_kernel_mono_us.load(Ordering::Relaxed),"last_socket_kernel_wall_ns":tap.last_any_kernel_wall_ns.load(Ordering::Relaxed),"connection_tcp_bytes":tap.total_tcp_bytes.load(Ordering::Relaxed),"note":"SO_TIMESTAMPNS is the kernel timestamp of the last packet passed in each recvmsg; FIONREAD is a byte-count snapshot, not packet age"}));
       }
       interval_age_max=interval_age_max.max(age);if age>800{interval_stale+=1;if had_tcp_read{interval_stale_with_tcp+=1;}else{interval_stale_buffered+=1;if ws_wait_us>1000{interval_buffered_stale_wait_over_1ms+=1;}}}}
      let parse_us=received_at.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
      let send_start=Instant::now();let sent=tx.try_send(Event::Quote{venue,copy,epoch,symbol,quote,received_at,frame_mono_us:frame_stamp,parse_us,witness:ReadWitness{first_tcp_us:first_read,last_tcp_us:last_read,recvq_before:tap.first_read_recvq_before_bytes.load(Ordering::Relaxed),recvq_after:tap.first_read_recvq_bytes.load(Ordering::Relaxed),had_tcp:had_tcp_read,ws_wait_us,kernel_first_mono_us:tap.frame_kernel_first_mono_us.load(Ordering::Relaxed),kernel_last_mono_us:tap.frame_kernel_last_mono_us.load(Ordering::Relaxed),kernel_first_wall_ns:tap.frame_kernel_first_wall_ns.load(Ordering::Relaxed),kernel_last_wall_ns:tap.frame_kernel_last_wall_ns.load(Ordering::Relaxed),recvmsg_count:tap.frame_recvmsg_count.load(Ordering::Relaxed),timestamp_missing:tap.frame_timestamp_missing.load(Ordering::Relaxed),tcp_bytes:tap.frame_tcp_bytes.load(Ordering::Relaxed),last_socket_tcp_read_mono_us:tap.last_any_tcp_read_mono_us.load(Ordering::Relaxed),last_socket_kernel_mono_us:tap.last_any_kernel_mono_us.load(Ordering::Relaxed),last_socket_kernel_wall_ns:tap.last_any_kernel_wall_ns.load(Ordering::Relaxed),connection_tcp_bytes:tap.total_tcp_bytes.load(Ordering::Relaxed)}});stages.enqueue.add(send_start.elapsed().as_micros() as u64);
      if let Err(e)=sent {drops.fetch_add(1,Ordering::Relaxed);match e{tokio::sync::mpsc::error::TrySendError::Full(_)=>{stages.full.fetch_add(1,Ordering::Relaxed);},tokio::sync::mpsc::error::TrySendError::Closed(_)=>{stages.closed.fetch_add(1,Ordering::Relaxed);}}}
     }
    }
   },
   Message::Ping(p)=>{ws.send(Message::Pong(p)).await.map_err(|e|format!("socket {venue}/{copy} pong: {e}"))?;},
   Message::Close(c)=>return Err(format!("socket {venue}/{copy} closed: {c:?}")),
   _=>{}
  }
 }
}
async fn session(venue:usize,copy:usize,chunk:Vec<String>,tx:mpsc::Sender<Event>,drops:Arc<AtomicU64>,stages:Arc<ReaderStages>,url_override:Option<String>){
 let mut epoch=0u64;
 let mut backoff=Duration::from_millis(250);
 // Do not retry an IP that already failed TLS or connection in this session.
 let mut rejected_peers=HashSet::<IpAddr>::new();
 loop{
  epoch+=1;
  let url=if let Some(url)=&url_override {url.clone()} else if venue<100{
   let streams=chunk.iter().map(|s|format!("{}usdt@bookTicker",s.to_lowercase())).collect::<Vec<_>>().join("/");
   format!("wss://fstream.binance.com/public/stream?streams={streams}")
  }else{"wss://ws.okx.com/ws/v5/public".to_string()};
  let peers=if venue<100 && url_override.is_none(){ranked_binance_peers()}else{Vec::new()};
  let candidates=peers.iter().copied().filter(|ip|!rejected_peers.contains(ip)).collect::<Vec<_>>();
  let candidate=if candidates.is_empty(){None}else{Some(candidates[(venue*2+copy+epoch as usize-1)%candidates.len()])};
  let mut failed_peer=None;let tcp_tap=Arc::new(TcpTapStats::default());
  let result=tokio::time::timeout(Duration::from_secs(12),async {
   if let Some(peer)=candidate {
    // A bad historical peer must not consume the entire connect budget.
    match tokio::time::timeout(Duration::from_secs(3),binance_registry_connect(&url,peer,tcp_tap.clone())).await {
     Ok(Ok(ws))=>{eprintln!("PEER venue={venue} copy={copy} epoch={epoch} mode=registry ip={peer} outcome=connected");return Ok((ws,()));},
     Ok(Err(e))=>{failed_peer=Some(peer);eprintln!("PEER venue={venue} copy={copy} epoch={epoch} mode=registry ip={peer} outcome=failed reason={}",e.chars().take(90).collect::<String>());},
     Err(_)=>{failed_peer=Some(peer);eprintln!("PEER venue={venue} copy={copy} epoch={epoch} mode=registry ip={peer} outcome=timeout");},
    }
   }
   let fallback=fallback_connect(&url,tcp_tap.clone()).await.map(|ws|(ws,()));
   eprintln!("PEER venue={venue} copy={copy} epoch={epoch} mode=dns outcome={}",if fallback.is_ok(){"connected"}else{"failed"});
   fallback
  }).await;
  if let Some(ip)=failed_peer{rejected_peers.insert(ip);}
  if let Ok(Ok((mut ws,_)))=result{
   if venue>=100 {
    let args=chunk.iter().map(|s|json!({"channel":"tickers","instId":format!("{s}-USDT")})).collect::<Vec<_>>();
    if ws.send(Message::Text(json!({"op":"subscribe","args":args}).to_string())).await.is_err(){
     let _=tx.send(Event::Disconnected{venue,copy,epoch}).await;
     tokio::time::sleep(backoff).await;backoff=(backoff*2).min(Duration::from_secs(8));continue;
    }
   }
   backoff=Duration::from_millis(250);
   // Per-physical-socket identification for read-only kernel Recv-Q correlation.
   let socket=match ws.get_ref(){
    tokio_tungstenite::MaybeTlsStream::Plain(t)=>t.inner.local_addr().ok().map(|a|a.port()),
    tokio_tungstenite::MaybeTlsStream::NativeTls(t)=>t.get_ref().get_ref().get_ref().inner.local_addr().ok().map(|a|a.port()),
    _=>None,
   };
   eprintln!("SOCKET venue={venue} copy={copy} epoch={epoch} local_port={socket:?}");
   let reader_result=reader(venue,copy,epoch,ws,tx.clone(),drops.clone(),stages.clone(),tcp_tap.clone()).await;
   match reader_result {
    Ok(())=>eprintln!("READER_EXIT venue={venue} copy={copy} epoch={epoch} port={socket:?} outcome=clean"),
    Err(e)=>eprintln!("READER_EXIT venue={venue} copy={copy} epoch={epoch} port={socket:?} outcome=error reason={}",e.chars().take(180).collect::<String>()),
   }
   eprintln!("TCP_TAP venue={venue} copy={copy} epoch={epoch} port={socket:?} stats={}",tcp_tap.snapshot());
  }
  if tx.send(Event::Disconnected{venue,copy,epoch}).await.is_err(){break}
  tokio::time::sleep(backoff).await;
  backoff=(backoff*2).min(Duration::from_secs(8));
 }
}
fn r1347_update_diverse_worst(map:&mut HashMap<String,(u64,Value)>,symbol:&str,delta_us:u64,event:Value){
 if event["identity_equal"]!=true{return}
 let slot=map.entry(symbol.to_string()).or_insert((0,Value::Null));
 if slot.1.is_null() || delta_us>slot.0{*slot=(delta_us,event);}
}
#[tokio::main(flavor="multi_thread",worker_threads=4)]
async fn main(){
 let path=std::env::args().nth(1).expect("symbols file");let seconds=std::env::args().nth(2).and_then(|x|x.parse::<u64>().ok()).unwrap_or(30).clamp(10,3600);
 let input=std::fs::read_to_string(path).expect("symbols");
 let bases=input.lines().filter(|s|!s.is_empty()&&s.len()<20&&s.bytes().all(|b|b.is_ascii_uppercase()||b.is_ascii_digit())).collect::<Vec<_>>();
 assert!(!bases.is_empty()&&bases.len()<=300);
 assert_eq!(bases.len(),bases.iter().collect::<HashSet<_>>().len(),"duplicate symbols");
 let (tx,mut rx)=mpsc::channel::<Event>(4096);
 let drops=Arc::new(AtomicU64::new(0));
 let reader_stages=Arc::new(ReaderStages::default());
 let connections=bases.chunks(70).count()*4;
 let owned_bases=bases.iter().map(|s|s.to_string()).collect::<Vec<_>>();
 let setup_drops=drops.clone();let setup_stages=reader_stages.clone();
 // Start the consumer below immediately, while connections are still being established.
 let mut setup=tokio::spawn(async move {
  let mut tasks=JoinSet::new();
  for (idx,chunk) in owned_bases.chunks(70).enumerate(){
   for copy in 0..2 {
    for venue in [idx,idx+100] {
     let (tx,drops,stages)=(tx.clone(),setup_drops.clone(),setup_stages.clone());
     let chunk=chunk.to_vec();
     tasks.spawn(async move{session(venue,copy,chunk,tx,drops,stages,None).await});
    }
   }
  }
  drop(tx);
  while let Some(done)=tasks.join_next().await {if done.is_err(){return Err("session task panicked".to_string())}}
  Err::<(),String>("all sessions stopped".to_string())
 });
 let(mut bn,mut ok)=(HashMap::<String,Quote>::new(),HashMap::<String,Quote>::new());
 let(mut bf,mut of,mut stale_or_duplicate)=(0u64,0u64,0u64);
 let(mut latency_samples,mut latency_negative,mut latency_over800,mut latency_max)=(0u64,0u64,0u64,0u64);
 let(mut venue_samples,mut venue_over800,mut venue_max)=([0u64;2],[0u64;2],[0u64;2]);
 let(mut queue_samples,mut queue_max_us,mut queue_over_1ms,mut queue_over_10ms)=(0u64,0u64,0u64,0u64);
 let mut hot=HashMap::<(usize,String),HotPair>::new();
 let(mut hot_reject,mut hot_promote,mut hot_disconnect,mut parse_max_us)=(0u64,0u64,0u64,0u64);
 let mut hot_exchange_stale=0u64;
 // Compact per-symbol stage census; diagnostic only, no trading path changes.
 let mut symbol_stages=HashMap::<(usize,String),[u64;5]>::new(); // decoded, fresh, hot-admitted, stored, stale
 let mut symbol_age_max=HashMap::<(usize,String),u64>::new();
 let mut sensor_last=Instant::now();
 let mut queue_stage_bins=[0u64;7];let mut queue_stage_max=0u64;
 let mut hot_promote_by_shard=[0u64;8];
 let mut hot_stale_by_shard=[0u64;8];
 let mut hot_disconnect_by_shard=[0u64;8];
 let mut hot_owner_timeout_promotions=0u64;
 // Raw-only mode bypasses both the freshness gate and HOT owner selection.
 let raw_only=std::env::args().any(|a|a=="--raw-only");
 let mut raw_matched=0u64;let mut raw_copy_lead=[0u64;2];
 let mut raw_match_delay_max_ms=0u64;
 let mut raw_seen=HashMap::<(usize,String,u64),(usize,u64,u64,u64,u64,f64,f64,f64,f64,u64,ReadWitness)>::new();
 let mut raw_pair_rows=0u64;
 // R1338: bounded pre/post context for verified dual-copy monotonic skew.
 // Keep only 32 prior pairs, 16 subsequent pairs and at most 8 triggers per run.
 let mut skew_prior=VecDeque::<Value>::with_capacity(32);
 let mut skew_post_remaining=0usize;
 let mut skew_trigger_count=0u64;
 let mut r1343_skew_events=0u64;
 let mut r1345_worst_skew=Vec::<(u64,Value)>::with_capacity(8);
 let mut r1347_symbol_worst=HashMap::<String,(u64,Value)>::new();
 let pair_log_limit=std::env::var("R1347_PAIR_LOG_LIMIT").ok().and_then(|v|v.parse::<u64>().ok()).unwrap_or(1000).min(50000);
 let mut raw_seen_fifo=VecDeque::<((usize,String,u64),u64)>::new();
 let mut raw_match_bins=[[0u64;8];4]; // <=1,2..9,10..49,50..199,200..799,800..1999,2000..9999,>=10000ms
 let mut raw_match_count=[0u64;4];
 let mut raw_match_evicted=0u64;let mut raw_match_same_copy=0u64;
 let mut raw_match_mismatch_ts=0u64;
 // 8 venue/shards x 2 independent physical HOT copies, measured before any gate.
 let mut raw_copy_samples=[[0u64;2];8];
 let mut raw_copy_age_buckets=[[[0u64;7];2];8]; // <=0,1..199,200..499,500..800,801..2000,2001..10000,>10000 or missing
 let mut raw_copy_max_age=[[0u64;2];8];
 let mut raw_copy_future=[[0u64;2];8];
 let mut raw_copy_missing=[[0u64;2];8];
 let mut raw_copy_last_recv=[[0u64;2];8];
 let mut raw_copy_gap_over800=[[0u64;2];8];
 let mut latency_buckets=[[0u64;7];2]; // 0-9,10-49,50-99,100-199,200-499,500-799,>=800 ms
let mut ticker=tokio::time::interval(Duration::from_secs(5));
 let start=Instant::now();let deadline=tokio::time::Instant::now()+Duration::from_secs(seconds);
 println!("{}",json!({"kind":"start","raw_only":raw_only,"symbols":bases.len(),"connections":connections,"worker_threads":4,"orders":false}));
 let mut failure=None;
 loop{tokio::select!{
  _=tokio::time::sleep_until(deadline)=>break,
  _=ticker.tick()=>{
   if sensor_last.elapsed()>=Duration::from_secs(60) {
    println!("{}",json!({"kind":"stage_timing","elapsed_ms":start.elapsed().as_millis(),"reader":reader_stages.snapshot(),"queue_residence_bins":queue_stage_bins,"queue_residence_max_us":queue_stage_max,"note":"ws_wait includes normal idle; not TCP arrival; cumulative"}));
    sensor_last=Instant::now();
    let mut rows=Vec::with_capacity(bases.len()*2);
    for venue in 0..2 {for base in &bases {
     let key=(venue,(*base).to_string());
     let c=symbol_stages.get(&key).copied().unwrap_or_default();
     rows.push(json!([venue,base,c[0],c[1],c[2],c[3],c[4],symbol_age_max.get(&key).copied().unwrap_or(0)]));
    }}
    println!("{}",json!({"kind":"symbol_freshness_snapshot","elapsed_ms":start.elapsed().as_millis(),"columns":["venue_0_binance_1_okx","symbol","decoded","fresh","hot_admitted","stored","stale","max_exchange_age_ms"],"rows":rows,"channel_drops":drops.load(Ordering::Relaxed),"hot_disconnect":hot_disconnect}));
   }
   let n=ms();let fresh=bn.iter().filter(|(s,q)|ok.get(*s).map_or(false,|p|n.saturating_sub(q.recv)<=800&&n.saturating_sub(p.recv)<=800)).count();
   println!("{}",json!({"kind":"stats","elapsed_ms":start.elapsed().as_millis(),"bn_frames":bf,"okx_frames":of,"bn_symbols":bn.len(),"okx_symbols":ok.len(),"fresh_pairs":fresh,"channel_drops":drops.load(Ordering::Relaxed),"stale_or_duplicate":stale_or_duplicate,"latency_samples":latency_samples,"latency_negative":latency_negative,"latency_over800":latency_over800,"latency_max_ms":latency_max,"latency_buckets":latency_buckets,"venue_samples":venue_samples,"venue_over800":venue_over800,"venue_max_ms":venue_max,"rss_kb":rss(),"queue_samples":queue_samples,"queue_max_us":queue_max_us,"queue_over_1ms":queue_over_1ms,"queue_over_10ms":queue_over_10ms,"hot_reject":hot_reject,"hot_promote":hot_promote,"hot_disconnect":hot_disconnect,"parse_max_us":parse_max_us,"hot_exchange_stale":hot_exchange_stale,"hot_promote_by_shard":hot_promote_by_shard,"hot_stale_by_shard":hot_stale_by_shard,"hot_disconnect_by_shard":hot_disconnect_by_shard,"hot_owner_timeout_promotions":hot_owner_timeout_promotions,"r1347_pair_log_rows":raw_pair_rows,"r1347_pair_log_limit":pair_log_limit,"raw_matched":raw_matched,"raw_copy_lead":raw_copy_lead,"raw_match_delay_max_ms":raw_match_delay_max_ms,"raw_seen_pending":raw_seen.len(),"raw_match_bins":raw_match_bins,"raw_match_count":raw_match_count,"raw_match_evicted":raw_match_evicted,"r1343_skew_events":r1343_skew_events,"r1343_skew_logged":skew_trigger_count,"raw_match_same_copy":raw_match_same_copy,"raw_match_mismatch_ts":raw_match_mismatch_ts,"raw_copy_samples":raw_copy_samples,"raw_copy_age_buckets":raw_copy_age_buckets,"raw_copy_max_age":raw_copy_max_age,"raw_copy_future":raw_copy_future,"raw_copy_missing":raw_copy_missing,"raw_copy_gap_over800":raw_copy_gap_over800}));
  },
  joined=&mut setup=>{
   failure=Some(format!("setup/reader terminated: {joined:?}"));break;
  },
  event=rx.recv()=>{
   if let Some(e)=event{
    match e {
     Event::Disconnected{venue,copy,epoch}=>{
      hot_disconnect+=1;hot_disconnect_by_shard[if venue<100{venue}else{venue-100+4}]+=1;
      for ((v,_),pair) in hot.iter_mut(){if *v==venue{pair.disconnected(copy,epoch)}}
     },
     Event::Quote{venue,copy,epoch,symbol,quote,received_at,frame_mono_us,parse_us,witness}=>{
      let queue_us=received_at.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;
      queue_samples+=1;queue_max_us=queue_max_us.max(queue_us);queue_stage_bins[stage_bin(queue_us)]+=1;queue_stage_max=queue_stage_max.max(queue_us);
      parse_max_us=parse_max_us.max(parse_us);
      if queue_us>1000{queue_over_1ms+=1;}if queue_us>10000{queue_over_10ms+=1;}
      let shard=if venue<100{venue}else{venue-100+4};
      let symbol_venue=if venue<100{0}else{1};
      symbol_stages.entry((symbol_venue,symbol.clone())).or_default()[0]+=1;
      raw_copy_samples[shard][copy]+=1;
      let prev=raw_copy_last_recv[shard][copy];
      if prev>0 && quote.recv.saturating_sub(prev)>800 {raw_copy_gap_over800[shard][copy]+=1;}
      raw_copy_last_recv[shard][copy]=quote.recv;
      match quote.exchange_ts {
       Some(t) if t>quote.recv=>{raw_copy_future[shard][copy]+=1;raw_copy_age_buckets[shard][copy][0]+=1;},
       Some(t)=>{
        let age=quote.recv-t;raw_copy_max_age[shard][copy]=raw_copy_max_age[shard][copy].max(age);
        let age_entry=symbol_age_max.entry((symbol_venue,symbol.clone())).or_default();*age_entry=(*age_entry).max(age);
        let b=if age<=199{1}else if age<=499{2}else if age<=800{3}else if age<=2000{4}else if age<=10000{5}else{6};
        raw_copy_age_buckets[shard][copy][b]+=1;
       },
       None=>{raw_copy_missing[shard][copy]+=1;raw_copy_age_buckets[shard][copy][6]+=1;}
      }
      if raw_only {
       if venue<100 {
        if let Some(id)=quote.update_id {
         let key=(venue,symbol.clone(),id);
         // Expire old unmatched keys (30s), and evict oldest at capacity.
         while let Some((old_key,old_recv))=raw_seen_fifo.front() {
          if quote.recv.saturating_sub(*old_recv)<=30000 && raw_seen.len()<12000 && raw_seen_fifo.len()<24000 {break}
          let old_key=old_key.clone();let old_recv=*old_recv;
          raw_seen_fifo.pop_front();
          if raw_seen.get(&old_key).is_some_and(|(_,t,_,_,_,_,_,_,_,_,_)|*t==old_recv) {raw_seen.remove(&old_key);raw_match_evicted+=1;}
         }
         if let Some((first_copy,first_recv,first_mono,first_ts,first_epoch,first_bid,first_ask,first_bid_qty,first_ask_qty,first_queue_us,first_witness))=raw_seen.get(&key).copied() {
          if first_copy!=copy {
           raw_seen.remove(&key);
           raw_matched+=1;raw_match_count[venue]+=1;raw_copy_lead[first_copy]+=1;
           let delta=quote.recv.abs_diff(first_recv);
           raw_match_delay_max_ms=raw_match_delay_max_ms.max(delta);
           let bin=if delta<=1{0}else if delta<=9{1}else if delta<=49{2}else if delta<=199{3}else if delta<=799{4}else if delta<=1999{5}else if delta<=9999{6}else{7};
           raw_match_bins[venue][bin]+=1;
           if quote.exchange_ts!=Some(first_ts){raw_match_mismatch_ts+=1;}
           let matched_identity=quote.exchange_ts==Some(first_ts) && epoch==first_epoch && quote.bid==first_bid && quote.ask==first_ask && quote.bid_qty==first_bid_qty && quote.ask_qty==first_ask_qty;
           let pair_context=json!({"exchange":"Binance_USDM","venue_shard":venue,"connection_id_a":format!("binance-shard{venue}-copy{first_copy}-epoch{first_epoch}"),"connection_id_b":format!("binance-shard{venue}-copy{copy}-epoch{epoch}"),"symbol":symbol,"update_id":id,"exchange_ts_a_ms":first_ts,"exchange_ts_b_ms":quote.exchange_ts,"wall_recv_a_ms":first_recv,"wall_recv_b_ms":quote.recv,"frame_mono_a_us":first_mono,"frame_mono_b_us":frame_mono_us,"epoch_a":first_epoch,"epoch_b":epoch,"copy_a":first_copy,"copy_b":copy,"mono_delta_us":frame_mono_us.abs_diff(first_mono),"wall_delta_ms":delta,"queue_a_us":first_queue_us,"queue_b_us":queue_us,"tcp_a":first_witness.as_json(),"tcp_b":witness.as_json(),"exchange_ts_equal":quote.exchange_ts==Some(first_ts),"identity_equal":matched_identity});
           if skew_post_remaining>0 {eprintln!("R1338_SKEW_POST {}",pair_context);skew_post_remaining-=1;}
           let mono_delta=frame_mono_us.abs_diff(first_mono);
           if matched_identity {r1347_update_diverse_worst(&mut r1347_symbol_worst,&symbol,mono_delta,pair_context.clone());}
           if skew_matches(mono_delta,matched_identity){
            r1343_skew_events+=1;
            record_worst_skew(&mut r1345_worst_skew,mono_delta,&pair_context,&skew_prior);
           }
           if skew_log_allowed(mono_delta,matched_identity,skew_trigger_count){
            skew_trigger_count+=1;
            eprintln!("R1343_SKEW_EVENT {}",skew_event_record(skew_trigger_count,&pair_context,&skew_prior));
            eprintln!("R1337_SKEW_TRIGGER {}",json!({"symbol":symbol,"update_id":id,"copy_a":first_copy,"copy_b":copy,"epoch":epoch,"mono_delta_us":mono_delta,"wall_delta_ms":quote.recv.abs_diff(first_recv),"first_frame_mono_us":first_mono,"second_frame_mono_us":frame_mono_us,"note":"first 8 only; ws.next completion, not packet arrival"}));
            skew_post_remaining=16;
           }
           if skew_prior.len()==32 {skew_prior.pop_front();}
           skew_prior.push_back(pair_context);
           if raw_pair_rows<pair_log_limit { println!("{}",json!({"kind":"dual_copy_pair","venue":venue,"symbol":symbol,"stream":"bookTicker","update_id":id,"exchange_ts_a_ms":first_ts,"exchange_ts_b_ms":quote.exchange_ts,"exchange_ts_equal":quote.exchange_ts==Some(first_ts),"copy_a":first_copy,"epoch_a":first_epoch,"recv_a_ms":first_recv,"frame_mono_a_us":first_mono,"queue_residence_a_us":first_queue_us,"bid_a":first_bid,"ask_a":first_ask,"bid_qty_a":first_bid_qty,"ask_qty_a":first_ask_qty,"copy_b":copy,"epoch_b":epoch,"recv_b_ms":quote.recv,"frame_mono_b_us":frame_mono_us,"receive_delta_mono_us":frame_mono_us.abs_diff(first_mono),"queue_residence_b_us":queue_us,"bid_b":quote.bid,"ask_b":quote.ask,"bid_qty_b":quote.bid_qty,"ask_qty_b":quote.ask_qty,"receive_delta_ms":quote.recv.abs_diff(first_recv),"quote_fields_equal":quote.bid==first_bid&&quote.ask==first_ask&&quote.bid_qty==first_bid_qty&&quote.ask_qty==first_ask_qty})); raw_pair_rows+=1; }
          } else {raw_match_same_copy+=1;}
         } else {
          raw_seen.insert(key.clone(),(copy,quote.recv,frame_mono_us,quote.exchange_ts.unwrap_or(0),epoch,quote.bid,quote.ask,quote.bid_qty,quote.ask_qty,queue_us,witness));
          raw_seen_fifo.push_back((key,quote.recv));
         }
        }
       }
       continue;
      }
      if !exchange_fresh(quote){symbol_stages.entry((symbol_venue,symbol.clone())).or_default()[4]+=1;hot_exchange_stale+=1;hot_stale_by_shard[if venue<100{venue}else{venue-100+4}]+=1;continue}
      symbol_stages.entry((symbol_venue,symbol.clone())).or_default()[1]+=1;
      let pair=hot.entry((venue,symbol.clone())).or_default();
      pair.connected(copy,epoch);
      let previous_owner=pair.owner;
      if !pair.admit(copy,epoch,quote.recv,ms()){hot_reject+=1;continue}
      symbol_stages.entry((symbol_venue,symbol.clone())).or_default()[2]+=1;
      if previous_owner.is_some()&&previous_owner!=pair.owner{hot_promote+=1;hot_owner_timeout_promotions+=1;hot_promote_by_shard[if venue<100{venue}else{venue-100+4}]+=1}
      let map=if venue<100{bf+=1;&mut bn}else{of+=1;&mut ok};
      if let Some(t)=quote.exchange_ts {
       latency_samples+=1;
       let vi=if venue<100{0}else{1};venue_samples[vi]+=1;
       if t>quote.recv {latency_negative+=1;} else {
        let age=quote.recv-t;
        latency_max=latency_max.max(age);venue_max[vi]=venue_max[vi].max(age);
        if age>800 {latency_over800+=1;venue_over800[vi]+=1;}
        let bucket=if age<10{0}else if age<50{1}else if age<100{2}else if age<200{3}else if age<500{4}else if age<800{5}else{6};
        latency_buckets[if venue<100{0}else{1}][bucket]+=1;
       }
      }
      if map.get(&symbol).map_or(true,|prior|newer(*prior,quote)){symbol_stages.entry((symbol_venue,symbol.clone())).or_default()[3]+=1;map.insert(symbol,quote);}else{stale_or_duplicate+=1;}
     }
    }
   }
   else{failure=Some("channel closed".into());break}
  }
 }}
 setup.abort();
 // An unobserved instrument is not necessarily a failed subscription: bookTicker is change-driven.
 let missing_binance=bases.iter().filter(|s|!bn.contains_key(**s)).copied().collect::<Vec<_>>();
 let missing_okx=bases.iter().filter(|s|!ok.contains_key(**s)).copied().collect::<Vec<_>>();
 let mut rows=Vec::with_capacity(bases.len()*2);
 for venue in 0..2 {for base in &bases {
  let key=(venue,(*base).to_string());let c=symbol_stages.get(&key).copied().unwrap_or_default();
  rows.push(json!([venue,base,c[0],c[1],c[2],c[3],c[4],symbol_age_max.get(&key).copied().unwrap_or(0)]));
 }}
 println!("{}",json!({"kind":"symbol_freshness_snapshot","elapsed_ms":start.elapsed().as_millis(),"final":true,"columns":["venue_0_binance_1_okx","symbol","decoded","fresh","hot_admitted","stored","stale","max_exchange_age_ms"],"rows":rows,"channel_drops":drops.load(Ordering::Relaxed),"hot_disconnect":hot_disconnect}));
 eprintln!("COVERAGE missing_binance={missing_binance:?} missing_okx={missing_okx:?}");
 for venue in 0..2 {
  let mut missing=Vec::new();let mut slow=Vec::new();
  for base in &bases {
   let v=symbol_stages.get(&(venue,(*base).to_string())).copied().unwrap_or_default();
   if v[3]==0 {missing.push(json!({"symbol":base,"decoded":v[0],"fresh":v[1],"hot_admitted":v[2],"stored":v[3],"stale":v[4]}));}
   if v[4]>0 {slow.push((v[4],*base,v[0]));}
  }
  slow.sort_by(|a,b|b.0.cmp(&a.0));
  let top_stale=slow.iter().take(10).map(|(n,s,total)|json!({"symbol":s,"stale":n,"decoded":total})).collect::<Vec<_>>();
  eprintln!("SENSOR {}",json!({"venue":if venue==0{"Binance"}else{"OKX"},"missing_stages":missing,"top_stale":top_stale}));
 }
 r1345_worst_skew.sort_by(|a,b|b.0.cmp(&a.0));
 for (rank,(mono_delta,mut event)) in r1345_worst_skew.into_iter().enumerate(){
  event["rank"]=json!(rank+1);
  event["mono_delta_us"]=json!(mono_delta);
  eprintln!("R1345_WORST_SKEW {}",event);
 }
 let mut diverse=r1347_symbol_worst.into_iter().collect::<Vec<_>>();
 diverse.sort_by(|a,b|b.1.0.cmp(&a.1.0));
 let diverse_rows=diverse.into_iter().map(|(symbol,(delta_us,event))|json!({"symbol":symbol,"mono_delta_us":delta_us,"pair":event})).collect::<Vec<_>>();
 println!("{}",json!({"kind":"r1347_diverse_worst_by_symbol","selection":"maximum exact-identity Binance dual-copy monotonic skew per instrument","symbols_with_matches":diverse_rows.len(),"events":diverse_rows}));
 println!("{}",json!({"kind":"stage_timing","final":true,"elapsed_ms":start.elapsed().as_millis(),"reader":reader_stages.snapshot(),"queue_residence_bins":queue_stage_bins,"queue_residence_max_us":queue_stage_max,"note":"ws_wait includes normal idle; not TCP arrival; cumulative"}));
 println!("{}",json!({"kind":"summary","elapsed_ms":start.elapsed().as_millis(),"bn_frames":bf,"okx_frames":of,"bn_symbols":bn.len(),"okx_symbols":ok.len(),"channel_drops":drops.load(Ordering::Relaxed),"stale_or_duplicate":stale_or_duplicate,"latency_samples":latency_samples,"latency_negative":latency_negative,"latency_over800":latency_over800,"latency_max_ms":latency_max,"latency_buckets":latency_buckets,"venue_samples":venue_samples,"venue_over800":venue_over800,"venue_max_ms":venue_max,"rss_kb":rss(),"failure":failure,"orders":false,"queue_samples":queue_samples,"queue_max_us":queue_max_us,"queue_over_1ms":queue_over_1ms,"queue_over_10ms":queue_over_10ms,"hot_reject":hot_reject,"hot_promote":hot_promote,"hot_disconnect":hot_disconnect,"parse_max_us":parse_max_us,"hot_exchange_stale":hot_exchange_stale,"hot_promote_by_shard":hot_promote_by_shard,"hot_stale_by_shard":hot_stale_by_shard,"hot_disconnect_by_shard":hot_disconnect_by_shard,"hot_owner_timeout_promotions":hot_owner_timeout_promotions,"r1347_pair_log_rows":raw_pair_rows,"r1347_pair_log_limit":pair_log_limit,"raw_matched":raw_matched,"raw_copy_lead":raw_copy_lead,"raw_match_delay_max_ms":raw_match_delay_max_ms,"raw_seen_pending":raw_seen.len(),"raw_match_bins":raw_match_bins,"raw_match_count":raw_match_count,"raw_match_evicted":raw_match_evicted,"r1343_skew_events":r1343_skew_events,"r1343_skew_logged":skew_trigger_count,"raw_match_same_copy":raw_match_same_copy,"raw_match_mismatch_ts":raw_match_mismatch_ts,"raw_copy_samples":raw_copy_samples,"raw_copy_age_buckets":raw_copy_age_buckets,"raw_copy_max_age":raw_copy_max_age,"raw_copy_future":raw_copy_future,"raw_copy_missing":raw_copy_missing,"raw_copy_gap_over800":raw_copy_gap_over800}));
 if failure.is_some()||drops.load(Ordering::Relaxed)>0{std::process::exit(5)}
}

#[cfg(test)]
mod quote_tests {
 use super::*;
 #[test]
 fn r1345_worst_eight_replacement_fixture(){
  let mut worst=Vec::new();let prior=VecDeque::from([json!({"id":17})]);
  for i in 1..=8 {let delta=(i as u64)*1_000_000;assert!(record_worst_skew(&mut worst,delta,&json!({"mono_delta_us":delta,"tcp_a":{"first_tcp_us":i},"tcp_b":{"first_tcp_us":i+10}}),&prior));}
  assert_eq!(worst.len(),8);
  assert!(!record_worst_skew(&mut worst,500_000,&json!({"mono_delta_us":500_000}),&prior));
  assert!(record_worst_skew(&mut worst,12_000_000,&json!({"mono_delta_us":12_000_000,"tcp_a":{"first_tcp_us":99},"tcp_b":{"first_tcp_us":100}}),&prior));
  let mut vals=worst.iter().map(|x|x.0).collect::<Vec<_>>();vals.sort();
  assert_eq!(vals,[2_000_000,3_000_000,4_000_000,5_000_000,6_000_000,7_000_000,8_000_000,12_000_000]);
  assert_eq!(worst.iter().find(|x|x.0==12_000_000).unwrap().1["prior_pairs"][0]["id"],17);
  assert_eq!(worst.iter().find(|x|x.0==12_000_000).unwrap().1["pair"]["tcp_b"]["first_tcp_us"],100);
 }
 #[test]
 fn r1343_exact_trigger_record_and_cap_fixture(){
  let mut prior=VecDeque::new();
  for i in 0..35 {if prior.len()==32{prior.pop_front();}prior.push_back(json!({"id":i}));}
  let first=ReadWitness{first_tcp_us:100,last_tcp_us:105,recvq_before:4096,recvq_after:1024,had_tcp:true,ws_wait_us:12,..ReadWitness::default()};
  let second=ReadWitness{first_tcp_us:800_120,last_tcp_us:800_125,recvq_before:8192,recvq_after:4096,had_tcp:true,ws_wait_us:13,..ReadWitness::default()};
  let pair=json!({"mono_delta_us":800_020,"identity_equal":true,"tcp_a":first.as_json(),"tcp_b":second.as_json()});
  assert!(skew_log_allowed(800_020,true,7));
  let record=skew_event_record(8,&pair,&prior);
  assert_eq!(record["trigger"],8);
  assert_eq!(record["prior_pairs"].as_array().unwrap().len(),32);
  assert_eq!(record["prior_pairs"][0]["id"],3);
  assert_eq!(record["pair"]["tcp_a"]["recvq_before_first_read"],4096);
  assert_eq!(record["pair"]["tcp_b"]["first_tcp_us"],800_120);
  assert!(!skew_log_allowed(800_020,true,8));
  assert!(!skew_log_allowed(799_999,true,0));
  assert!(!skew_log_allowed(800_020,false,0));
 }

 #[test]
 fn r1347_diverse_worst_retains_one_max_exact_pair_per_symbol(){
  let mut rows=HashMap::new();
  r1347_update_diverse_worst(&mut rows,"BTC",100,json!({"identity_equal":true,"update_id":1}));
  r1347_update_diverse_worst(&mut rows,"BTC",250,json!({"identity_equal":true,"update_id":2}));
  r1347_update_diverse_worst(&mut rows,"BTC",200,json!({"identity_equal":true,"update_id":3}));
  r1347_update_diverse_worst(&mut rows,"ETH",175,json!({"identity_equal":true,"update_id":4}));
  r1347_update_diverse_worst(&mut rows,"XRP",900,json!({"identity_equal":false,"update_id":5}));
  assert_eq!(rows.len(),2);assert_eq!(rows["BTC"].0,250);assert_eq!(rows["BTC"].1["update_id"],2);assert_eq!(rows["ETH"].0,175);
 }
 #[test]
 fn decode_minimal_bbo_and_reject_invalid() {
  let b=json!({"data":{"s":"BTCUSDT","b":"100","a":"101","B":"2","A":"3","u":10,"E":1000}});
  let (_,q)=decode(0,&b,10000).expect("binance quote");
  assert_eq!((q.bid,q.ask,q.bid_qty,q.ask_qty,q.update_id),(100.,101.,2.,3.,Some(10)));
  let o=json!({"arg":{"channel":"tickers"},"data":[{"instId":"BTC-USDT","bidPx":"100","askPx":"101","bidSz":"2","askSz":"3","ts":"1000"}]});
  let (_,q)=decode(100,&o,10000).expect("okx quote");
  assert_eq!((q.bid_qty,q.ask_qty,q.exchange_ts),(2.,3.,Some(1000)));
  let mut bad=b.clone();bad["data"]["B"]=json!("0");
  assert!(decode(0,&bad,10000).is_none());
  bad=b.clone();bad["data"]["a"]=json!("99");
  assert!(decode(0,&bad,10000).is_none());
  bad=b.clone();bad["data"].as_object_mut().unwrap().remove("u");
  assert!(decode(0,&bad,10000).is_none());
  bad=o.clone();bad["data"][0].as_object_mut().unwrap().remove("ts");
  assert!(decode(100,&bad,10000).is_none());
 }
 #[test]
 fn hot_fails_closed_on_exchange_age_or_missing_timestamp(){
  let q=Quote{recv:10_000,bid:1.,ask:2.,bid_qty:1.,ask_qty:1.,exchange_ts:Some(9_200),update_id:Some(1)};
  assert!(exchange_fresh(q));
  assert!(!exchange_fresh(Quote{exchange_ts:Some(9_199),..q}));
  assert!(!exchange_fresh(Quote{exchange_ts:Some(10_801),..q}));
  assert!(!exchange_fresh(Quote{exchange_ts:None,..q}));
 }
 #[tokio::test]
 async fn session_reconnects_after_local_ws_close_and_increments_epoch() {
  use tokio::net::TcpListener;
  use tokio_tungstenite::accept_async;
  let listener=TcpListener::bind("127.0.0.1:0").await.unwrap();
  let url=format!("ws://{}",listener.local_addr().unwrap());
  let server=tokio::spawn(async move {
   for i in 0..2 {
    let (stream,_)=listener.accept().await.unwrap();
    let mut ws=accept_async(stream).await.unwrap();
    let payload=json!({"data":{"s":"BTCUSDT","b":"100","a":"101","B":"2","A":"3","u":i+1,"E":1000}}).to_string();
    ws.send(Message::Text(payload)).await.unwrap();
    ws.close(None).await.unwrap();
   }
  });
  let (tx,mut rx)=mpsc::channel(16);
  let client=tokio::spawn(session(0,0,vec!["BTC".into()],tx,Arc::new(AtomicU64::new(0)),Arc::new(ReaderStages::default()),Some(url)));
  let mut epochs=Vec::new();let mut disconnected=0;
  tokio::time::timeout(Duration::from_secs(4),async {
   while epochs.len()<2 || disconnected==0 {
    match rx.recv().await.unwrap() {
     Event::Quote{epoch,..}=>epochs.push(epoch),
     Event::Disconnected{..}=>disconnected+=1,
    }
   }
  }).await.expect("local reconnect timed out");
  assert_eq!(epochs,vec![1,2]);assert!(disconnected>=1);
  client.abort();server.await.unwrap();
 }
 #[tokio::test]
 async fn sixteen_local_sessions_reconnect_and_fence_old_epochs() {
  use tokio::net::TcpListener;
  use tokio_tungstenite::accept_async;
  let listener=TcpListener::bind("127.0.0.1:0").await.unwrap();
  let url=format!("ws://{}",listener.local_addr().unwrap());
  let (tx,mut rx)=mpsc::channel(256);
  let drops=Arc::new(AtomicU64::new(0));
  let mut clients=Vec::new();
  for shard in 0..4 {
   for venue in [shard,shard+100] {
    for copy in 0..2 {
     clients.push(tokio::spawn(session(venue,copy,vec!["BTC".into()],tx.clone(),drops.clone(),Arc::new(ReaderStages::default()),Some(url.clone()))));
    }
   }
  }
  drop(tx);
  let server=tokio::spawn(async move {
   // Every socket receives a quote, then a clean close; every client reconnects.
   for round in 0..2 {
    for _ in 0..16 {
     let (stream,_)=listener.accept().await.unwrap();
     let mut ws=accept_async(stream).await.unwrap();
     // Local fixture supports either venue's format by sending both.
     let b=json!({"data":{"s":"BTCUSDT","b":"100","a":"101","B":"2","A":"3","u":round+1,"E":1000}});
     let o=json!({"arg":{"channel":"tickers"},"data":[{"instId":"BTC-USDT","bidPx":"100","askPx":"101","bidSz":"2","askSz":"3","ts":"1000"}]});
     ws.send(Message::Text(b.to_string())).await.unwrap();
     ws.send(Message::Text(o.to_string())).await.unwrap();
     ws.close(None).await.unwrap();
    }
   }
  });
  let mut first=HashSet::new();let mut second=HashSet::new();let mut disconnected=0u32;
  tokio::time::timeout(Duration::from_secs(8),async {
   while first.len()<16 || second.len()<16 || disconnected<16 {
    match rx.recv().await.expect("channel ended") {
     Event::Quote{venue,copy,epoch,..}=>{
      if epoch==1 {first.insert((venue,copy));}
      if epoch==2 {second.insert((venue,copy));}
     },
     Event::Disconnected{epoch,..}=>{if epoch==1{disconnected+=1}}
    }
   }
  }).await.expect("16-session matrix timeout");
  assert_eq!(first.len(),16);assert_eq!(second.len(),16);
  assert!(disconnected>=16);assert_eq!(drops.load(Ordering::Relaxed),0);
  for task in clients {task.abort()}
  server.await.unwrap();
 }
 #[test]
 fn hot_fails_closed_when_both_copies_disconnected_or_stale(){
  let mut p=HotPair::default();p.connected(0,1);p.connected(1,1);
  assert!(p.admit(0,1,100,100));p.disconnected(0,1);
  assert!(!p.admit(0,1,101,101));
  assert!(!p.admit(1,1,100,901));
  p.disconnected(1,1);assert!(!p.admit(1,1,902,902));
  p.connected(0,2);assert!(!p.admit(0,1,903,903));
  assert!(p.admit(0,2,903,903));
 }
 #[test]
 fn hot_promotes_only_fresh_connected_copy() {
  let mut p=HotPair::default();p.connected(0,1);p.connected(1,1);
  assert!(p.admit(0,1,100,100));
  assert!(!p.admit(1,1,101,102)); // owner remains healthy
  p.disconnected(0,1);
  assert!(p.admit(1,1,103,103)); // pre-connected standby promotion
  assert_eq!(p.owner,Some(1));
 }
 #[test]
 fn hot_rejects_stale_epoch_and_stale_standby() {
  let mut p=HotPair::default();p.connected(0,1);p.connected(1,1);
  assert!(p.admit(0,1,100,100));p.disconnected(0,1);
  assert!(!p.admit(1,1,100,1001)); // old standby frame
  p.connected(0,2);
  assert!(!p.admit(0,1,1002,1002)); // queued old epoch
  assert!(p.admit(0,2,1003,1003));
  p.disconnected(0,1); // stale disconnect cannot kill new session
  assert!(p.admit(0,2,1004,1004));
 }
 #[test]
 fn duplicate_and_out_of_order_are_rejected() {
  let a=Quote{recv:100,bid:100.,ask:101.,bid_qty:2.,ask_qty:3.,exchange_ts:Some(1000),update_id:Some(10)};
  assert!(!skew_matches(799_999,true));
  assert!(!skew_matches(800_000,false));
  assert!(skew_matches(800_000,true));
  assert!(skew_log_allowed(800_000,true,7));
  assert!(!skew_log_allowed(800_000,true,8));
  assert!(!newer(a,Quote{recv:200,update_id:Some(10),..a}));
  assert!(!newer(a,Quote{recv:200,update_id:Some(9),..a}));
  assert!(newer(a,Quote{recv:200,update_id:Some(11),..a}));
  let o=Quote{update_id:None,..a};
  assert!(!newer(o,Quote{recv:200,exchange_ts:Some(1000),..o}));
  assert!(!newer(o,Quote{recv:200,exchange_ts:Some(999),..o}));
  assert!(newer(o,Quote{recv:200,exchange_ts:Some(1001),..o}));
  assert!(newer(o,Quote{recv:200,bid:101.,..o})); // equal-ms but different BBO
  assert!(!newer(o,Quote{recv:200,..o})); // identical duplicate
  assert!(!newer(Quote{exchange_ts:None,update_id:None,..o},Quote{recv:200,exchange_ts:None,update_id:None,..o}));
 }
 #[cfg(target_os="linux")]
 #[tokio::test]
 async fn r1346_recvmsg_pending_then_data_has_kernel_timestamp_and_no_extra_queue(){
  use tokio::net::{TcpListener,TcpStream};
  let listener=TcpListener::bind("127.0.0.1:0").await.unwrap();
  let addr=listener.local_addr().unwrap();
  let (server,client)=tokio::join!(listener.accept(),TcpStream::connect(addr));
  let (server,_)=server.unwrap();let mut client=client.unwrap();
  let stats=Arc::new(TcpTapStats::default());
  let mut tap=TcpTap::new(server,stats.clone()).unwrap();
  let waker=futures_util::task::noop_waker();let mut cx=Context::from_waker(&waker);
  let mut pending_storage=[0u8;4];let mut pending_buf=ReadBuf::new(&mut pending_storage);
  assert!(Pin::new(&mut tap).poll_read(&mut cx,&mut pending_buf).is_pending());
  assert_eq!(stats.first_read_us.load(Ordering::Relaxed),0);
  assert_eq!(stats.frame_recvmsg_count.load(Ordering::Relaxed),0);
  assert_eq!(stats.frame_tcp_bytes.load(Ordering::Relaxed),0);
  tokio::io::AsyncWriteExt::write_all(&mut client,b"ping").await.unwrap();
  let mut received=[0u8;4];
  let n=tokio::time::timeout(Duration::from_secs(1),tokio::io::AsyncReadExt::read(&mut tap,&mut received)).await.unwrap().unwrap();
  assert_eq!(n,4);assert_eq!(&received,b"ping");
  assert!(stats.first_read_us.load(Ordering::Relaxed)>0);
  assert!(stats.frame_recvmsg_count.load(Ordering::Relaxed)>0);
  assert_eq!(stats.frame_tcp_bytes.load(Ordering::Relaxed),4);
  assert_eq!(stats.frame_timestamp_missing.load(Ordering::Relaxed),0);
  assert!(stats.frame_kernel_first_wall_ns.load(Ordering::Relaxed)>0);
  assert!(stats.frame_kernel_last_wall_ns.load(Ordering::Relaxed)>=stats.frame_kernel_first_wall_ns.load(Ordering::Relaxed));
  let mut second_reader=tokio::spawn(async move{
   let mut second=[0u8;4];
   let result=tokio::io::AsyncReadExt::read(&mut tap,&mut second).await;
   (tap,second,result)
  });
  tokio::task::yield_now().await;tokio::time::sleep(Duration::from_millis(10)).await;
  assert!(!second_reader.is_finished());
  tokio::io::AsyncWriteExt::write_all(&mut client,b"pong").await.unwrap();
  tokio::time::sleep(Duration::from_millis(25)).await;
  assert!(second_reader.is_finished(),"socket readiness failed to wake the next AsyncRead after WouldBlock");
  let (tap,second,n)=second_reader.await.unwrap();assert_eq!(n.unwrap(),4);assert_eq!(&second,b"pong");
  assert_eq!(stats.frame_recvmsg_count.load(Ordering::Relaxed),2);
  assert_eq!(stats.frame_tcp_bytes.load(Ordering::Relaxed),8);
  assert_eq!(stats.total_tcp_bytes.load(Ordering::Relaxed),8);
  assert!(stats.last_any_tcp_read_mono_us.load(Ordering::Relaxed)>0);
  assert!(stats.last_any_kernel_wall_ns.load(Ordering::Relaxed)>0);
  assert!(stats.last_any_kernel_mono_us.load(Ordering::Relaxed)>0);
  assert_eq!(stats.frame_timestamp_missing.load(Ordering::Relaxed),0);
 }
}
