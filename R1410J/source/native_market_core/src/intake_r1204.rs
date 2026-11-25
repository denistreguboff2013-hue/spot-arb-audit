//! R1204 durable intake. No trading publisher; journal receipt != DB1/DB2 ACK.
//! Append-only bounded session journal. Never rotates or deletes audit input.
use serde::{Deserialize, Serialize};
use std::{fs::{File,OpenOptions},io::{Read,Write},path::Path,os::unix::{fs::{OpenOptionsExt,PermissionsExt},io::AsRawFd}};
const MAX_RECORD:usize=131072;
pub const SESSION_CAP:u64=64*1024*1024;
#[derive(Debug,Clone,Serialize,Deserialize,PartialEq,Eq)]
pub struct Receipt {
 pub schema:u8,pub n:u64,pub journal_id:String,pub source:String,pub market:String,pub epoch:u64,
 pub recv_ns:u64,pub recv_wall_ms:i64,pub raw_sha:String,pub raw:String,
 pub previous:String,
}
fn hash(b:&[u8])->String{openssl::sha::sha256(b).iter().map(|v|format!("{v:02x}")).collect()}
pub struct Intake{file:File,bytes:u64,cap:u64,n:u64,previous:String,failed:bool,journal_id:String}
impl Intake{
 /// Caller must create a private session directory. Existing journal is never overwritten.
 pub fn create(path:&Path,cap:u64)->Result<Self,String>{
  if cap<MAX_RECORD as u64||cap>SESSION_CAP{return Err("intake cap invalid".into())}
  let parent=path.parent().ok_or("intake parent")?;
  let md=std::fs::symlink_metadata(parent).map_err(|e|e.to_string())?;
  if !md.is_dir()||md.permissions().mode()&0o077!=0{return Err("intake directory must be private, non-symlink".into())}
  let file=OpenOptions::new().write(true).create_new(true).mode(0o600)
   .custom_flags(libc::O_NOFOLLOW|libc::O_CLOEXEC).open(path).map_err(|e|e.to_string())?;
  if unsafe{libc::flock(file.as_raw_fd(),libc::LOCK_EX|libc::LOCK_NB)}!=0{return Err("intake lock".into())}
  file.sync_all().map_err(|e|e.to_string())?;
  File::open(parent).and_then(|f|f.sync_all()).map_err(|e|e.to_string())?;
  let mut random=[0u8;16];openssl::rand::rand_bytes(&mut random).map_err(|e|e.to_string())?;
  let journal_id=hash(&random);
  Ok(Self{file,bytes:0,cap,n:0,previous:String::new(),failed:false,journal_id})
 }
 /// Success is the acceptance boundary: complete record and fsync succeeded.
 /// Failure is sticky; caller must stop ingress and decisions. Input remains caller-owned.
 pub fn accept(&mut self,source:&str,market:&str,epoch:u64,ns:u64,wall:i64,raw:&str)->Result<u64,String>{
  if self.failed{return Err("intake previously failed".into())}
  let result=(||{
   if raw.len()>65536||source.len()>128||market.len()>32||epoch==0{return Err("intake schema/size".into())}
   let n=self.n.checked_add(1).ok_or("intake counter overflow")?;
   let r=Receipt{schema:1,n,journal_id:self.journal_id.clone(),source:source.into(),market:market.into(),epoch,
    recv_ns:ns,recv_wall_ms:wall,raw_sha:hash(raw.as_bytes()),raw:raw.into(),previous:self.previous.clone()};
   let b=serde_json::to_vec(&r).map_err(|e|e.to_string())?;
   if b.len()>MAX_RECORD||self.bytes.checked_add(b.len() as u64+36).map_or(true,|v|v>self.cap){return Err("intake overflow: GAP, stop source and decisions".into())}
   let digest=openssl::sha::sha256(&b);
   self.file.write_all(&(b.len() as u32).to_be_bytes()).and_then(|_|self.file.write_all(&b))
    .and_then(|_|self.file.write_all(&digest)).and_then(|_|self.file.sync_all()).map_err(|e|format!("intake write uncertain: {e}"))?;
   self.bytes+=b.len() as u64+36;self.n=n;self.previous=hash(&b);Ok(n)
  })();
  if result.is_err(){self.failed=true}result
 }
 pub fn accepted(&self)->u64{self.n}
}
/// Streaming verification, bounded RAM; no old book is published on restart.
/// Torn tail/corruption is explicit error, never silently truncated or skipped.
pub fn verify(path:&Path,cap:u64,mut visit:impl FnMut(&Receipt))->Result<u64,String>{
 let mut f=OpenOptions::new().read(true).custom_flags(libc::O_NOFOLLOW).open(path).map_err(|e|e.to_string())?;
 if cap>SESSION_CAP||f.metadata().map_err(|e|e.to_string())?.len()>cap{return Err("recovery cap".into())}
 let mut n=0;let mut previous=String::new();let mut journal_id=None;
 loop{
  let mut len=[0u8;4];
  let k=f.read(&mut len[..1]).map_err(|e|e.to_string())?;
  if k==0{return Ok(n)}
  f.read_exact(&mut len[1..]).map_err(|_|"torn intake header")?;
  let len=u32::from_be_bytes(len) as usize;
  if len>MAX_RECORD{return Err("invalid record size".into())}
  let mut b=vec![0;len];let mut digest=[0;32];
  f.read_exact(&mut b).and_then(|_|f.read_exact(&mut digest)).map_err(|_|"torn intake record")?;
  if openssl::sha::sha256(&b)!=digest{return Err("intake record hash".into())}
  let r:Receipt=serde_json::from_slice(&b).map_err(|e|e.to_string())?;
  if r.journal_id.len()!=64||journal_id.as_ref().is_some_and(|id|id!=&r.journal_id){return Err("intake session mismatch".into())}
  journal_id=Some(r.journal_id.clone());
  if r.schema!=1||r.n!=n+1||r.previous!=previous||r.raw_sha!=hash(r.raw.as_bytes())||r.epoch==0{return Err("intake lineage".into())}
  n=r.n;previous=hash(&b);visit(&r);
 }
}

/// Fail-closed decision-time fence for a selected partial snapshot. It never
/// merges depth20 frames or applies Futures pu semantics to SPOT.
#[derive(Debug,Clone,Copy,PartialEq,Eq)]
pub enum Kind{FuturesPartial,SpotPartial,FuturesDiff,SpotDiff}
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct Ticket{pub source:u64,pub epoch:u64,pub update:u64,pub event_ms:Option<i64>,pub recv_ms:u64,pub hash:[u8;32],pub kind:Kind}
#[derive(Default)]
pub struct Fence{selected:Option<(u64,u64)>,last:Option<Ticket>,last_switch:Option<u64>}
impl Fence{
 pub fn handover(&mut self,candidate:&Ticket,now:i64,mono:u64)->Result<(),&'static str>{
  if !fresh(candidate,now,mono)||candidate.kind!=Kind::FuturesPartial{return Err("candidate unsynchronized/unsupported")}
  if self.selected.is_some_and(|(_,e)|candidate.epoch<=e){return Err("epoch must advance")}
  if self.last_switch.is_some_and(|t|mono<t||mono-t<1000){return Err("handover hysteresis")}
  if self.last.as_ref().is_some_and(|t|candidate.update<t.update||candidate.event_ms<t.event_ms){return Err("candidate behind high-water")}
  self.selected=Some((candidate.source,candidate.epoch));self.last_switch=Some(mono);Ok(())
 }
 pub fn publish(&mut self,t:&Ticket,now:i64,mono:u64)->Result<(),&'static str>{
  self.check(t,now,mono)?;
  if self.last.as_ref().is_some_and(|p|t.update<=p.update||t.event_ms<p.event_ms){return Err("duplicate/reordered")}
  self.last=Some(t.clone());Ok(())
 }
 pub fn decision(&self,t:&Ticket,now:i64,mono:u64)->Result<(),&'static str>{
  self.check(t,now,mono)?;
  if self.last.as_ref()!=Some(t){return Err("not current publication")}Ok(())
 }
 fn check(&self,t:&Ticket,now:i64,mono:u64)->Result<(),&'static str>{
  if self.selected!=Some((t.source,t.epoch)){return Err("old/unselected epoch")}
  if t.kind!=Kind::FuturesPartial||!fresh(t,now,mono){return Err("stale/unsupported stream")}Ok(())
 }
}
fn fresh(t:&Ticket,now:i64,mono:u64)->bool{
 t.event_ms.and_then(|e|now.checked_sub(e)).is_some_and(|a|(0..=800).contains(&a))&&mono>=t.recv_ms&&mono-t.recv_ms<=800
}
/// Second gate immediately before a modeled action. Cross-venue epoch numbers
/// are independent; require nonzero epochs, never compare them for equality.
pub fn decision_pair_fresh(spot:Option<&spotarb_native_market_core::Book>,future:Option<&spotarb_native_market_core::Book>,now_ns:u64,wall:i64)->bool{
 let (Some(a),Some(b))=(spot,future) else{return false};
 [a,b].iter().all(|book|book.epoch>0&&now_ns>=book.received_ns&&now_ns-book.received_ns<=800_000_000&&book.exchange_ts_ms.and_then(|e|wall.checked_sub(e)).is_some_and(|age|(0..=800).contains(&age)))
}
