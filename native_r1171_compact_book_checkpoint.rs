//! R1171 offline compact checkpoint prototype. NEVER opens original 17-table DBs.
//! Independent mirrored file generations with ACK receipt; no REAL/public network.
//! Restored books are QUARANTINED until a new live source epoch is verified.
use crate::{Book,Venue};
use openssl::sha::sha256;
use serde::{Deserialize,Serialize};
use std::{collections::BTreeSet,fs::{self,File,OpenOptions},io::Write,
 path::{Path,PathBuf},os::unix::{fs::OpenOptionsExt,io::AsRawFd}};

const SCHEMA:&str="SPOTARB/R1171/COMPACT-PUBLIC-PAPER/V1";
const ACK_SCHEMA:&str="SPOTARB/R1171/ACK-RECEIPT/V1";
const MAX_ENTRIES:usize=600;
const MAX_BYTES:usize=1_500_000;
const KEEP_GENERATIONS:u64=2;

#[derive(Clone,Debug,PartialEq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketEntry{
 pub base:String,
 pub venue:Venue,
 pub last_epoch:u64,
 pub last_source_ms:Option<i64>,
 pub book:Option<Book>, // None means invalidated, never an active book.
}
#[derive(Clone,Debug,PartialEq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompactState{
 pub schema:String,
 pub session:String,
 pub generation:u64,
 pub source_batch_floor:u64,
 pub db2_ack_floor:u64,
 pub entries:Vec<MarketEntry>,
 pub paper_only:bool,
}
#[derive(Clone,Debug,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
struct Receipt{
 schema:String,session:String,generation:u64,
 source_batch_floor:u64, db2_ack_floor:u64,
 state_sha256:String,previous_receipt_sha256:String,
}
#[derive(Clone,Debug,PartialEq)]
pub struct QuarantinedRestore{
 pub state:CompactState,
 pub may_resume_decisions:bool,
 pub requires_fresh_live_epoch:bool,
}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum Fault{None,AfterDb1,AfterDb2,AfterAck}

fn hex(bytes:&[u8])->String{
 const HEX:&[u8;16]=b"0123456789abcdef";
 let mut s=String::with_capacity(bytes.len()*2);
 for &b in bytes{s.push(HEX[(b>>4) as usize] as char);s.push(HEX[(b&15) as usize] as char)}
 s
}
fn hash(bytes:&[u8])->String{hex(&sha256(bytes))}
fn safe_session(s:&str)->bool{
 !s.is_empty()&&s.len()<=80&&s.bytes().all(|b|b.is_ascii_alphanumeric()||b==b'_'||b==b'-')
}
fn valid_book(b:&Book,epoch:u64)->bool{
 b.epoch==epoch&&b.received_wall_ms>0&&b.received_ns>0&&
 b.changed_ns<=b.received_ns&&
 b.exchange_ts_ms.is_some_and(|t|t>0)&&
 !b.bids.is_empty()&&!b.asks.is_empty()&&
 b.bids.len()<=20&&b.asks.len()<=20&&
 b.bids.iter().chain(&b.asks).all(|(p,q)|p.is_finite()&&*p>0.&&q.is_finite()&&*q>0.)
}
pub fn validate(s:&CompactState)->Result<(),String>{
 if s.schema!=SCHEMA||!s.paper_only||!safe_session(&s.session)||
   s.generation==0||s.source_batch_floor==0||
   s.source_batch_floor!=s.db2_ack_floor||
   s.entries.is_empty()||s.entries.len()>MAX_ENTRIES{return Err("state_contract_denied".into())}
 let mut keys=BTreeSet::new();let mut previous=None;
 for e in &s.entries{
  if e.base.is_empty()||e.base.len()>30||
   !e.base.bytes().all(|b|b.is_ascii_uppercase()||b.is_ascii_digit())||
   e.last_epoch==0{return Err("bad_market_identity".into())}
  let venue=match e.venue{Venue::OkxSpot=>0,Venue::BinanceFutures=>1};
  let key=(e.base.as_str(),venue);
  if previous.as_ref().is_some_and(|p|*p>=key)||!keys.insert(key){
   return Err("duplicate_or_unsorted_books".into())
  }
  previous=Some(key);
  if let Some(b)=&e.book{
   if !valid_book(b,e.last_epoch)||e.last_source_ms!=b.exchange_ts_ms{
    return Err("bad_or_stale_market_book".into())
   }
  }else if e.last_source_ms.is_some_and(|t|t<=0){
   return Err("bad_invalidated_source_floor".into())
  }
 }
 Ok(())
}
fn path(root:&Path,g:u64,side:&str)->PathBuf{
 root.join(format!("r1171_g{g:020}_{side}.json"))
}
fn receipt_path(root:&Path)->PathBuf{root.join("r1171_ack.json")}
fn dir_ok(root:&Path)->Result<(),String>{
 let m=fs::symlink_metadata(root).map_err(|_|"missing_private_root")?;
 if !m.is_dir()||m.file_type().is_symlink(){return Err("unsafe_checkpoint_root".into())}
 Ok(())
}
fn read_regular(path:&Path)->Result<Vec<u8>,String>{
 let m=fs::symlink_metadata(path).map_err(|_|"missing_checkpoint_file")?;
 if !m.is_file()||m.file_type().is_symlink(){return Err("unsafe_checkpoint_file".into())}
 let b=fs::read(path).map_err(|_|"unreadable_checkpoint_file")?;
 if b.is_empty()||b.len()>MAX_BYTES{return Err("checkpoint_size_denied".into())}
 Ok(b)
}
fn sync_dir(root:&Path)->Result<(),String>{
 File::open(root).and_then(|f|f.sync_all()).map_err(|_|"directory_fsync_failed".into())
}
// Temp file is exclusive; rename is atomic per file, not across DB1/DB2.
// The ACK receipt is the only publication point.
fn publish(root:&Path,dest:&Path,b:&[u8])->Result<(),String>{
 if b.len()>MAX_BYTES{return Err("checkpoint_size_denied".into())}
 let staged=dest.with_extension("stage");
 let mut f=OpenOptions::new().create_new(true).write(true).mode(0o600)
  .open(&staged).map_err(|_|"staging_conflict")?;
 f.write_all(b).and_then(|_|f.sync_all()).map_err(|_|"stage_fsync_failed")?;
 drop(f);
 fs::rename(staged,dest).map_err(|_|"atomic_publish_failed")?;
 sync_dir(root)
}
struct WriterLock(File);
impl Drop for WriterLock{
 fn drop(&mut self){unsafe{libc::flock(self.0.as_raw_fd(),libc::LOCK_UN);}}
}
fn lock(root:&Path)->Result<WriterLock,String>{
 let path=root.join("r1171_writer.lock");
 let f=OpenOptions::new().create(true).read(true).write(true).mode(0o600)
  .custom_flags(libc::O_NOFOLLOW|libc::O_CLOEXEC)
  .open(path).map_err(|_|"writer_lock_open_failed")?;
 if unsafe{libc::flock(f.as_raw_fd(),libc::LOCK_EX|libc::LOCK_NB)}!=0{
  return Err("concurrent_writer_denied".into())
 }
 Ok(WriterLock(f))
}

fn read_receipt(root:&Path)->Result<Option<(Receipt,String)>,String>{
 let f=receipt_path(root);
 if !f.exists(){
  let orphan=fs::read_dir(root).map_err(|_|"checkpoint_dir_read_failed")?
   .filter_map(Result::ok).any(|e|e.file_name().to_string_lossy().starts_with("r1171_g"));
  if orphan{return Err("missing_ack_with_orphans".into())}
  return Ok(None)
 }
 let bytes=read_regular(&f)?;
 let r:Receipt=serde_json::from_slice(&bytes).map_err(|_|"ack_receipt_corrupt")?;
 if r.schema!=ACK_SCHEMA||!safe_session(&r.session)||
   r.generation==0||r.source_batch_floor==0||
   r.db2_ack_floor!=r.source_batch_floor||
   r.state_sha256.len()!=64||r.previous_receipt_sha256.len()!=64{
  return Err("ack_receipt_invalid".into())
 }
 Ok(Some((r,hash(&bytes))))
}
// Verify latest DB1 AND DB2 against the published ACK; never fall back to
// older snapshots on damaged newest generation.
pub fn load_quarantined(root:&Path)->Result<Option<QuarantinedRestore>,String>{
 dir_ok(root)?;
 let (r,_)=match read_receipt(root)?{Some(v)=>v,None=>return Ok(None)};
 let a=read_regular(&path(root,r.generation,"db1"))?;
 let b=read_regular(&path(root,r.generation,"db2"))?;
 let sealed_ack=read_regular(&path(root,r.generation,"ack"))?;
 let published_ack=read_regular(&receipt_path(root))?;
 if sealed_ack!=published_ack{return Err("immutable_ack_mismatch".into())}
 if r.generation==1 {
  if r.previous_receipt_sha256!="0".repeat(64){return Err("bad_genesis_chain".into())}
 }else{
  let previous=read_regular(&path(root,r.generation-1,"ack"))?;
  if hash(&previous)!=r.previous_receipt_sha256{
   return Err("previous_receipt_chain_mismatch".into())
  }
 }
 if a!=b||hash(&a)!=r.state_sha256{return Err("db1_db2_ack_mismatch".into())}
 let s:CompactState=serde_json::from_slice(&a).map_err(|_|"snapshot_json_corrupt")?;
 validate(&s)?;
 if s.session!=r.session||s.generation!=r.generation||
   s.source_batch_floor!=r.source_batch_floor||
   s.db2_ack_floor!=r.db2_ack_floor{return Err("ack_cursor_mismatch".into())}
 Ok(Some(QuarantinedRestore{
   state:s,may_resume_decisions:false,requires_fresh_live_epoch:true}))
}
// NOT the canonical original DB1/DB2 protocol: an independent isolated
// snapshot lab. No authority to skip R1088 pre-decision commits.
pub fn commit(root:&Path,state:&CompactState,fault:Fault)->Result<String,String>{
 dir_ok(root)?;
 validate(state)?;
 let _locked=lock(root)?;
 let prior=read_receipt(root)?;
 if prior.is_some(){let _=load_quarantined(root)?;}
 match &prior{
  None if state.generation!=1=>return Err("missing_genesis".into()),
  Some((r,_)) if state.session!=r.session||
   state.generation!=r.generation+1||
   state.source_batch_floor<=r.source_batch_floor=>return Err("non_monotonic_generation".into()),
  _=>{}
 }
 let bytes=serde_json::to_vec(state).map_err(|_|"serialize_failed")?;
 if bytes.len()>MAX_BYTES{return Err("snapshot_too_large".into())}
 // No attempt to silently overwrite an unacknowledged orphan after crash.
 for side in ["db1","db2"]{
  if path(root,state.generation,side).exists(){
   return Err("unacknowledged_orphan_quarantine".into())
  }
 }
 publish(root,&path(root,state.generation,"db1"),&bytes)?;
 if fault==Fault::AfterDb1{return Err("injected_crash_after_db1".into())}
 publish(root,&path(root,state.generation,"db2"),&bytes)?;
 if fault==Fault::AfterDb2{return Err("injected_crash_after_db2".into())}
 // Both physical copies are stable before the receipt advances.
 let reread_db1=read_regular(&path(root,state.generation,"db1"))?;
 let reread_db2=read_regular(&path(root,state.generation,"db2"))?;
 if reread_db1!=bytes||reread_db2!=bytes{
  return Err("unstable_mirror_before_ack".into())
 }
 let r=Receipt{
  schema:ACK_SCHEMA.to_owned(),session:state.session.clone(),
  generation:state.generation,source_batch_floor:state.source_batch_floor,
  db2_ack_floor:state.db2_ack_floor,
  state_sha256:hash(&bytes),
  previous_receipt_sha256:prior.map_or("0".repeat(64),|(_,h)|h),
 };
 let receipt_bytes=serde_json::to_vec(&r).map_err(|_|"receipt_encode_failed")?;
 // Keep a per-generation immutable receipt so the prior hash is verifiable.
 publish(root,&path(root,state.generation,"ack"),&receipt_bytes)?;
 publish(root,&receipt_path(root),&receipt_bytes)?;
 if fault==Fault::AfterAck{return Err("injected_crash_after_ack".into())}
 // Prune only fully superseded private compact snapshots, never the
 // current or immediate predecessor. An unACKed orphan is not pruned.
 if state.generation>KEEP_GENERATIONS{
  let old=state.generation-KEEP_GENERATIONS;
  for side in ["db1","db2","ack"]{
   let old_file=path(root,old,side);
   if old_file.exists(){
    let _=read_regular(&old_file)?;
    fs::remove_file(&old_file).map_err(|_|"prune_failed")?;
   }
  }
  sync_dir(root)?;
 }
 let verified=load_quarantined(root)?.ok_or("lost_checkpoint_receipt")?;
 if verified.state!=*state{return Err("roundtrip_mismatch".into())}
 Ok(hash(&receipt_bytes))
}

#[cfg(test)]
mod tests{
 use super::*;
 use crate::{Owner,RawFrame,HistorySink,Update,CoreError};
 use std::time::{SystemTime,UNIX_EPOCH};
 use std::os::unix::fs::DirBuilderExt;
 struct Temp(PathBuf);
 impl Temp{
  fn new()->Self{
   let p=std::env::temp_dir().join(format!("r1171_{}_{}",
    std::process::id(),SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()));
   fs::DirBuilder::new().mode(0o700).create(&p).unwrap();Self(p)
  }
 }
 impl Drop for Temp{
  fn drop(&mut self){let _=fs::remove_dir_all(&self.0);}
 }
 struct NoDb;
 impl HistorySink for NoDb{
  fn accept(&mut self,_:&RawFrame,_:&Update)->Result<(),CoreError>{Ok(())}
 }
 fn state(g:u64,seq:u64)->CompactState{
  let book=Book{
   bids:vec![(100.0,2.0),(99.0,3.0)],asks:vec![(101.0,2.0),(102.0,3.0)],
   received_ns:1000+g,received_wall_ms:1_790_000_000_000+g as i64,
   exchange_ts_ms:Some(1_790_000_000_000+g as i64),
   changed_ns:1000+g,epoch:1,sequence:Some(seq as i64)
  };
  CompactState{
   schema:SCHEMA.to_owned(),session:"paper_r1171".into(),
   generation:g,source_batch_floor:seq,db2_ack_floor:seq,paper_only:true,
   entries:vec![
    MarketEntry{base:"BTC".into(),venue:Venue::OkxSpot,
      last_epoch:1,last_source_ms:book.exchange_ts_ms,book:Some(book.clone())},
    MarketEntry{base:"BTC".into(),venue:Venue::BinanceFutures,
      last_epoch:1,last_source_ms:book.exchange_ts_ms,book:Some(book)}
   ]
  }
 }
 #[test]fn r1171_roundtrip_dual_copy_quarantined(){
  let t=Temp::new();let s=state(1,50);
  let ack=commit(&t.0,&s,Fault::None).unwrap();
  assert_eq!(ack.len(),64);
  let restored=load_quarantined(&t.0).unwrap().unwrap();
  assert_eq!(restored.state,s);
  assert!(!restored.may_resume_decisions);
  assert!(restored.requires_fresh_live_epoch);
 }
 #[test]fn r1171_source_before_mirror_ack_never_publishes(){
  for fault in [Fault::AfterDb1,Fault::AfterDb2]{
   let t=Temp::new();let first=state(1,100);
   commit(&t.0,&first,Fault::None).unwrap();
   assert!(commit(&t.0,&state(2,101),fault).is_err());
   assert_eq!(load_quarantined(&t.0).unwrap().unwrap().state,first);
   // Do not overwrite the incomplete evidence; manual quarantine.
   assert_eq!(commit(&t.0,&state(2,101),Fault::None),
      Err("unacknowledged_orphan_quarantine".into()));
  }
 }
 #[test]fn r1171_ack_after_crash_is_valid_commit(){
  let t=Temp::new();commit(&t.0,&state(1,1),Fault::None).unwrap();
  assert!(commit(&t.0,&state(2,2),Fault::AfterAck).is_err());
  assert_eq!(load_quarantined(&t.0).unwrap().unwrap().state,state(2,2));
 }
 #[test]fn r1171_corruption_refuses_latest_without_fallback(){
  let t=Temp::new();commit(&t.0,&state(1,10),Fault::None).unwrap();
  commit(&t.0,&state(2,11),Fault::None).unwrap();
  fs::write(path(&t.0,2,"db2"),b"tampered").unwrap();
  assert_eq!(load_quarantined(&t.0),Err("db1_db2_ack_mismatch".into()));
 }
 #[test]fn r1171_mirrored_checkpoint_bounds_confirmed_generations(){
  let t=Temp::new();
  for g in 1..=4{commit(&t.0,&state(g,g),Fault::None).unwrap();}
  assert!(!path(&t.0,1,"db1").exists());
  assert!(!path(&t.0,2,"db1").exists());
  assert!(path(&t.0,3,"db1").exists());
  assert!(path(&t.0,4,"db2").exists());
  assert_eq!(load_quarantined(&t.0).unwrap().unwrap().state.generation,4);
  let files=fs::read_dir(&t.0).unwrap().filter_map(Result::ok)
   .filter(|p|p.file_name().to_string_lossy().contains("_db")).count();
  assert_eq!(files,4);
 }
 #[test]fn r1171_invalid_entries_and_cursor_fail_closed(){
  let t=Temp::new();let mut s=state(1,1);
  s.entries.push(s.entries[0].clone());
  assert!(commit(&t.0,&s,Fault::None).is_err());
  let mut s=state(1,1);s.db2_ack_floor=0;
  assert!(commit(&t.0,&s,Fault::None).is_err());
  let mut s=state(1,1);s.entries[0].book.as_mut().unwrap().bids[0].0=f64::NAN;
  assert!(commit(&t.0,&s,Fault::None).is_err());
  let mut s=state(1,1);s.entries[0].last_epoch=2;
  assert!(commit(&t.0,&s,Fault::None).is_err());
  let mut s=state(1,1);s.entries[0].book=None;
  commit(&t.0,&s,Fault::None).unwrap();
  assert!(load_quarantined(&t.0).unwrap().unwrap().state.entries[0].book.is_none());
 }
 #[test]fn r1171_live_owner_book_parity_on_fixture(){
  let mut owner=Owner::new(0,1,800).unwrap();
  let mut sink=NoDb;
  let bin=RawFrame{source:None,
   venue:Venue::BinanceFutures,epoch:1,
   received_ns:3_000_000,received_wall_ms:1_790_000_000_100,
   raw:serde_json::json!({"stream":"btcusdt@depth20@500ms","data":{
    "e":"depthUpdate","s":"BTCUSDT","E":1790000000050_i64,"u":17,
    "b":[["100","2"],["99","3"]],"a":[["101","2"],["102","3"]]}}).to_string()
  };
  let okx=RawFrame{source:None,
   venue:Venue::OkxSpot,epoch:1,
   received_ns:3_100_000,received_wall_ms:1_790_000_000_200,
   raw:serde_json::json!({"arg":{"channel":"books5","instId":"BTC-USDT"},
     "data":[{"bids":[["100","2","0","1"]],"asks":[["101","2","0","1"]],
       "ts":"1790000000150","seqId":13}]}).to_string()
  };
  owner.ingest(&bin,&mut sink).unwrap();owner.ingest(&okx,&mut sink).unwrap();
  let original=owner.snapshot_books();
  let (b,spot,future)=original[0].clone();
  let s=CompactState{
   schema:SCHEMA.into(),session:"paper_r1171".into(),generation:1,
   source_batch_floor:1,db2_ack_floor:1,paper_only:true,
   entries:vec![
    MarketEntry{base:b.clone(),venue:Venue::OkxSpot,last_epoch:1,
      last_source_ms:spot.as_ref().unwrap().exchange_ts_ms,book:spot.clone()},
    MarketEntry{base:b,venue:Venue::BinanceFutures,last_epoch:1,
      last_source_ms:future.as_ref().unwrap().exchange_ts_ms,book:future.clone()}
   ]
  };
  let t=Temp::new();commit(&t.0,&s,Fault::None).unwrap();
  let back=load_quarantined(&t.0).unwrap().unwrap();
  assert_eq!(back.state.entries[0].book,spot);
  assert_eq!(back.state.entries[1].book,future);
  assert!(!back.may_resume_decisions);
 }
}

#[cfg(test)]
mod r1162_actual_fixture_test{
 use super::*;
 #[test]fn r1171_actual_r1162_466_books_physical_dual_store(){
  let Some(src)=std::env::var_os("R1171_ACTUAL_R1162_FIXTURE") else{return};
  let data=fs::read(src).unwrap();
  let state:CompactState=serde_json::from_slice(&data).unwrap();
  validate(&state).unwrap();
  assert_eq!(state.entries.len(),466);
  assert_eq!(state.source_batch_floor,2289);
  assert_eq!(state.db2_ack_floor,2289);
  let tmp=std::env::temp_dir().join(format!("r1171_real_r1162_{}",std::process::id()));
  assert!(!tmp.exists());
  fs::create_dir(&tmp).unwrap();
  let receipt=commit(&tmp,&state,Fault::None).unwrap();
  let restored=load_quarantined(&tmp).unwrap().unwrap();
  assert_eq!(restored.state,state);
  assert!(!restored.may_resume_decisions);
  let a=fs::metadata(path(&tmp,1,"db1")).unwrap().len();
  let b=fs::metadata(path(&tmp,1,"db2")).unwrap().len();
  assert!(a<=1_500_000&&b<=1_500_000);
  println!("R1171_R1162_ACTUAL_FIXTURE_PASS entries={} db1_bytes={} db2_bytes={} ack={} quarantine=true",
   restored.state.entries.len(),a,b,receipt);
  fs::remove_dir_all(tmp).unwrap();
 }
}

#[cfg(test)]
mod r1171_negative_ack_tests{
 use super::*;
 use std::time::{SystemTime,UNIX_EPOCH};
 fn temp()->PathBuf{
  let p=std::env::temp_dir().join(format!("r1171_ack_{}_{}",
   std::process::id(),SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()));
  fs::create_dir(&p).unwrap();p
 }
 fn state(gen:u64)->CompactState{
  CompactState{schema:SCHEMA.into(),session:"ack_review".into(),
   generation:gen,source_batch_floor:gen,db2_ack_floor:gen,paper_only:true,
   entries:vec![MarketEntry{
    base:"BTC".into(),venue:Venue::BinanceFutures,
    last_epoch:2,last_source_ms:Some(1_790_000_000_000),
    book:None
   }]}
 }
 #[test]fn r1171_missing_published_head_does_not_silently_reset(){
  let root=temp();commit(&root,&state(1),Fault::None).unwrap();
  fs::remove_file(receipt_path(&root)).unwrap();
  assert_eq!(load_quarantined(&root),Err("missing_ack_with_orphans".into()));
  assert!(commit(&root,&state(1),Fault::None).is_err());
  fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r1171_tampered_immutable_ack_is_rejected(){
  let root=temp();commit(&root,&state(1),Fault::None).unwrap();
  fs::write(path(&root,1,"ack"),b"tampered").unwrap();
  assert_eq!(load_quarantined(&root),Err("immutable_ack_mismatch".into()));
  fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r1171_tampered_predecessor_ack_is_rejected(){
  let root=temp();commit(&root,&state(1),Fault::None).unwrap();
  commit(&root,&state(2),Fault::None).unwrap();
  fs::write(path(&root,1,"ack"),b"tampered").unwrap();
  assert_eq!(load_quarantined(&root),Err("previous_receipt_chain_mismatch".into()));
  fs::remove_dir_all(root).unwrap();
 }
}
