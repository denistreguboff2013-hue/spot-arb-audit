//! Unprivileged recvmsg TCP wrapper below native TLS. Keeps only bounded TLS
//! record metadata. A tail skb token is NOT an event's first-byte arrival.
use std::{sync::{Arc,Mutex},pin::Pin,task::{Context,Poll},io,os::fd::{AsRawFd,RawFd},collections::VecDeque};
use tokio::{net::TcpStream,io::{AsyncRead,AsyncWrite,ReadBuf},io::Interest};
use serde::{Serialize,Deserialize};
#[derive(Clone,Debug,Default,Serialize,Deserialize,PartialEq,Eq)]
pub struct Stamp{pub complete_ns:u64,pub wire_end:u64,pub tail_skb_utc_ns:Option<i64>,pub ws_return_ns:u64,pub attribution:String}
#[derive(Clone,Debug,Default,Serialize,Deserialize)]pub struct Record{pub start:u64,pub end:u64,pub complete_ns:u64,pub tail_skb_utc_ns:Option<i64>}
#[derive(Default)]pub struct Trace{pub bytes:u64,pub records:u64,pub reads:u64,pub missing_tokens:u64,pub truncated_tokens:u64,pub recent:VecDeque<Record>,header:Vec<u8>,body_left:usize,start:u64,pub failed:bool}
impl Trace{
 pub fn feed(&mut self,mut b:&[u8],ns:u64,token:Option<i64>,truncated:bool)->io::Result<()>{
  self.reads+=1;if token.is_none(){self.missing_tokens+=1}if truncated{self.truncated_tokens+=1}
  while !b.is_empty(){
   if self.body_left==0&&self.header.len()<5{
    if self.header.is_empty(){self.start=self.bytes;}
    let n=(5-self.header.len()).min(b.len());self.header.extend_from_slice(&b[..n]);self.bytes+=n as u64;b=&b[n..];
    if self.header.len()<5{continue}
    let n=u16::from_be_bytes([self.header[3],self.header[4]])as usize;
    if !(20..=23).contains(&self.header[0])||self.header[1]!=3||n>18432{self.failed=true;return Err(io::Error::new(io::ErrorKind::InvalidData,"TLS record header bound"))}
    self.body_left=n;
   }
   let n=self.body_left.min(b.len());self.body_left-=n;self.bytes+=n as u64;b=&b[n..];
   if self.body_left==0{self.records+=1;self.recent.push_back(Record{start:self.start,end:self.bytes,complete_ns:ns,tail_skb_utc_ns:if truncated{None}else{token}});while self.recent.len()>256{self.recent.pop_front();}self.header.clear();}
  }Ok(())
 }
 pub fn stamp(&self,ws_return_ns:u64)->Option<Stamp>{self.recent.back().map(|r|Stamp{complete_ns:r.complete_ns,wire_end:r.end,tail_skb_utc_ns:r.tail_skb_utc_ns,ws_return_ns,attribution:"latest completed encrypted record at WS return; not exact message-record lineage".into()})}
 pub fn summary(&self)->serde_json::Value{serde_json::json!({"encrypted_bytes":self.bytes,"records":self.records,"reads":self.reads,"missing_ancillary":self.missing_tokens,"truncated_ancillary":self.truncated_tokens,"record_tail":self.recent,"failed":self.failed,"tls_decrypt_internal":"UNAVAILABLE","timestamp_semantics":"last copied skb token, not first ingress or complete event arrival"})}
}
pub struct MeasuredTcp{pub inner:TcpStream,pub trace:Arc<Mutex<Trace>>,capture:bool}
impl MeasuredTcp{
 pub fn new(inner:TcpStream,trace:Arc<Mutex<Trace>>)->io::Result<Self>{Self::new_with_capture(inner,trace,true)}
 pub fn new_with_capture(inner:TcpStream,trace:Arc<Mutex<Trace>>,capture:bool)->io::Result<Self>{if capture{let enabled:libc::c_int=1;if unsafe{libc::setsockopt(inner.as_raw_fd(),libc::SOL_SOCKET,libc::SO_TIMESTAMPNS,&enabled as *const _ as *const _,std::mem::size_of_val(&enabled)as libc::socklen_t)}!=0{return Err(io::Error::last_os_error())}}Ok(Self{inner,trace,capture})}
 pub fn local_addr(&self)->io::Result<std::net::SocketAddr>{self.inner.local_addr()}
 pub fn peer_addr(&self)->io::Result<std::net::SocketAddr>{self.inner.peer_addr()}
}
impl AsRawFd for MeasuredTcp{fn as_raw_fd(&self)->RawFd{self.inner.as_raw_fd()}}
impl AsyncRead for MeasuredTcp{
 fn poll_read(self:Pin<&mut Self>,cx:&mut Context<'_>,buf:&mut ReadBuf<'_>)->Poll<io::Result<()>>{
  let this=self.get_mut();if buf.remaining()==0{return Poll::Ready(Ok(()))}if !this.capture{return Pin::new(&mut this.inner).poll_read(cx,buf)}
  loop{
   match this.inner.poll_read_ready(cx){Poll::Pending=>return Poll::Pending,Poll::Ready(Err(e))=>return Poll::Ready(Err(e)),_=>{}}
   let data=buf.initialize_unfilled();let mut ancillary=[0 as libc::c_long;32];let mut iov=libc::iovec{iov_base:data.as_mut_ptr()as *mut _,iov_len:data.len()};
   let mut msg:libc::msghdr=unsafe{std::mem::zeroed()};msg.msg_iov=&mut iov;msg.msg_iovlen=1;msg.msg_control=ancillary.as_mut_ptr()as *mut _;msg.msg_controllen=std::mem::size_of_val(&ancillary);
   let result=this.inner.try_io(Interest::READABLE,||{let n=unsafe{libc::recvmsg(this.inner.as_raw_fd(),&mut msg,0)};if n<0{Err(io::Error::last_os_error())}else{Ok(n as usize)}});
   match result{
    Err(e)if e.kind()==io::ErrorKind::WouldBlock=>continue,
    Err(e)=>return Poll::Ready(Err(e)),
    Ok(n)=>{
     let mut token=None;unsafe{let mut c=libc::CMSG_FIRSTHDR(&msg);while !c.is_null(){if (*c).cmsg_level==libc::SOL_SOCKET&&(*c).cmsg_type==libc::SO_TIMESTAMPNS&&(*c).cmsg_len>=libc::CMSG_LEN(std::mem::size_of::<libc::timespec>()as u32)as usize{let ts=std::ptr::read_unaligned(libc::CMSG_DATA(c)as *const libc::timespec);token=Some(ts.tv_sec as i64*1_000_000_000+ts.tv_nsec as i64);}c=libc::CMSG_NXTHDR(&msg,c);}}
     let ns=crate::ingress_runtime_r1205::mono_ns();let traced=this.trace.lock().map_err(|_|io::Error::new(io::ErrorKind::Other,"transport trace poisoned")).and_then(|mut t|t.feed(&data[..n],ns,token,msg.msg_flags&libc::MSG_CTRUNC!=0));
     if let Err(e)=traced{return Poll::Ready(Err(e))}buf.advance(n);return Poll::Ready(Ok(()))
    }
   }
  }
 }
}
impl AsyncWrite for MeasuredTcp{
 fn poll_write(self:Pin<&mut Self>,cx:&mut Context<'_>,b:&[u8])->Poll<io::Result<usize>>{Pin::new(&mut self.get_mut().inner).poll_write(cx,b)}
 fn poll_flush(self:Pin<&mut Self>,cx:&mut Context<'_>)->Poll<io::Result<()>>{Pin::new(&mut self.get_mut().inner).poll_flush(cx)}
 fn poll_shutdown(self:Pin<&mut Self>,cx:&mut Context<'_>)->Poll<io::Result<()>>{Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)}
}
#[cfg(test)]mod tests{use super::*;#[test]fn record_split_coalesce_and_timestamp_limit(){let mut t=Trace::default();t.feed(&[23,3],1,Some(7),false).unwrap();t.feed(&[3,0,3,1],2,Some(8),false).unwrap();t.feed(&[2,3,23,3,3,0,1,4],3,Some(9),false).unwrap();assert_eq!(t.records,2);assert_eq!(t.recent[0].start,0);assert_eq!(t.recent[0].end,8);assert_eq!(t.recent[1].end,14);assert_eq!(t.stamp(5).unwrap().tail_skb_utc_ns,Some(9));}#[test]fn oversized_record_rejects(){assert!(Trace::default().feed(&[23,3,3,255,255],1,None,false).is_err());}#[test]fn truncated_token_not_promoted_to_ingress(){let mut t=Trace::default();t.feed(&[23,3,3,0,1,1],1,Some(7),true).unwrap();assert_eq!(t.stamp(2).unwrap().tail_skb_utc_ns,None);}}
