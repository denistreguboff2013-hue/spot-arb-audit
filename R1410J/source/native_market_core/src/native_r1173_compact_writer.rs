//! R1173 native PRIVATE-PAPER compact SQLite writer (isolated experiment).
//! No production sink replacement; no REAL / exchange connectivity.
use crate::{Book,Owner,RawFrame,Update,Venue};
use crate::native_paper_portfolio_review::PaperPortfolio;
use crate::native_paper_market_execution_review::PaperPhase;
use std::{ffi::{CString,c_char,c_int,c_void},fs,os::unix::fs::{MetadataExt,OpenOptionsExt},
 path::{Path,PathBuf},os::unix::io::AsRawFd};
use serde::{Serialize,Deserialize};
use openssl::sha::sha256;
use std::collections::BTreeMap;
// R1183: test-only, aggregate transaction timings. Never logs raw frames,
// never changes production Sqlite durability, queues or trading semantics.
#[cfg(test)]
mod r1183_probe {
 use std::sync::atomic::{AtomicU64,Ordering};
 pub static SRC_DB1_NS:AtomicU64=AtomicU64::new(0);
 pub static SRC_DB2_NS:AtomicU64=AtomicU64::new(0);
 pub static SRC_ACK_NS:AtomicU64=AtomicU64::new(0);
 pub static SEAL_DB1_NS:AtomicU64=AtomicU64::new(0);
 pub static SEAL_DB2_NS:AtomicU64=AtomicU64::new(0);
 pub static SEAL_ACK_NS:AtomicU64=AtomicU64::new(0);
 pub fn add(counter:&AtomicU64,started:std::time::Instant){
  let elapsed=u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
  counter.fetch_add(elapsed,Ordering::Relaxed);
 }
}
const SQLITE_OK:c_int=0;
const SQLITE_DONE:c_int=101;
const SQLITE_ROW:c_int=100;
const READWRITE_EXISTING:c_int=2|0x10000|0x01000000;
const ZERO:&str="0000000000000000000000000000000000000000000000000000000000000000";
const SCHEMA:&str="SPOTARB/R1205/NATIVE-SOURCE-PROVENANCE/V1";
#[link(name="libsqlite3.so.0",kind="dylib",modifiers="+verbatim")]
extern "C" {
 fn sqlite3_open_v2(name:*const c_char,db:*mut *mut c_void,
   flags:c_int,vfs:*const c_char)->c_int;
 fn sqlite3_close(db:*mut c_void)->c_int;
 fn sqlite3_exec(db:*mut c_void,sql:*const c_char,cb:*const c_void,
   arg:*mut c_void,err:*mut *mut c_char)->c_int;
 fn sqlite3_prepare_v2(db:*mut c_void,sql:*const c_char,n:c_int,
   stmt:*mut *mut c_void,tail:*mut *const c_char)->c_int;
 fn sqlite3_bind_text(stmt:*mut c_void,pos:c_int,text:*const c_char,
   len:c_int,destructor:Option<unsafe extern "C" fn(*mut c_void)>)->c_int;
 fn sqlite3_bind_int64(stmt:*mut c_void,pos:c_int,val:i64)->c_int;
 fn sqlite3_step(stmt:*mut c_void)->c_int;
 fn sqlite3_finalize(stmt:*mut c_void)->c_int;
 fn sqlite3_column_count(stmt:*mut c_void)->c_int;
 fn sqlite3_column_text(stmt:*mut c_void,i:c_int)->*const u8;
 fn sqlite3_column_bytes(stmt:*mut c_void,i:c_int)->c_int;
}
pub(crate) struct Db(*mut c_void);
impl Drop for Db{fn drop(&mut self){unsafe{sqlite3_close(self.0);}}}
#[cfg(feature="offline-test-clock")]
fn r1327_should_fail_commit(db:&Db)->bool{
 R1327_FAIL_COMMIT_DB.with(|slot|{if slot.get()==Some(db.0 as usize){slot.set(None);true}else{false}})
}
struct Stmt(*mut c_void);
impl Drop for Stmt{fn drop(&mut self){unsafe{sqlite3_finalize(self.0);}}}
fn hex(bytes:&[u8])->String{
 const A:&[u8;16]=b"0123456789abcdef";
 let mut o=String::with_capacity(bytes.len()*2);
 for x in bytes{o.push(A[(x>>4)as usize]as char);o.push(A[(x&15)as usize]as char)}
 o
}
fn hash(s:&str)->String{hex(&sha256(s.as_bytes()))}
fn receipt_proof(journal:&str,id:u64,raw_hash:&str,frame_hash:&str,source_seq:u64)->String{hash(&format!("{}:{}:{}:{}:{}",journal,id,raw_hash,frame_hash,source_seq))}
#[cfg(test)]
thread_local! { static R1241_COMMIT_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = std::cell::RefCell::new(None); }
#[cfg(feature="offline-test-clock")]
thread_local! { static R1327_FAIL_COMMIT_DB: std::cell::Cell<Option<usize>> = std::cell::Cell::new(None); }
#[cfg(feature="offline-test-clock")]
thread_local! { static R1327_BEFORE_TRADE_EVENT_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = std::cell::RefCell::new(None); }
#[cfg(feature="offline-test-clock")]
pub fn r1327_before_trade_event_hook(hook:Box<dyn FnOnce()>){
 R1327_BEFORE_TRADE_EVENT_HOOK.with(|slot|{*slot.borrow_mut()=Some(hook);});
}
#[cfg(test)]
pub(crate) fn r1241_next_commit_hook(hook: Box<dyn FnOnce()>) {
 R1241_COMMIT_HOOK.with(|slot| { *slot.borrow_mut()=Some(hook); });
}
pub(crate) fn sql(db:&Db,cmd:&str)->Result<(),String>{
 let r1241_started=cmd.trim().trim_end_matches(';').eq_ignore_ascii_case("COMMIT").then(crate::ingress_runtime_r1205::mono_ns);
 #[cfg(test)]
 if cmd.trim().trim_end_matches(';').eq_ignore_ascii_case("COMMIT") {
  let hook=R1241_COMMIT_HOOK.with(|slot|slot.borrow_mut().take());
  if let Some(hook)=hook { hook(); }
 }
 #[cfg(feature="offline-test-clock")]
 if cmd.trim().trim_end_matches(';').eq_ignore_ascii_case("COMMIT")&&r1327_should_fail_commit(db){
  return Err("injected_sqlite_commit_failure".into())
 }
 let c=CString::new(cmd).map_err(|_|"sql_nul")?;
 let rc=unsafe{sqlite3_exec(db.0,c.as_ptr(),std::ptr::null(),
  std::ptr::null_mut(),std::ptr::null_mut())};
 if let Some(start)=r1241_started{crate::custody_executor_r1241::record_sql_commit(start,crate::ingress_runtime_r1205::mono_ns(),rc==SQLITE_OK);}
 if rc!=SQLITE_OK{
  return Err("sqlite_exec_failed".into())
 }
 Ok(())
}

#[derive(Clone)]
pub(crate) enum V{Str(String),Num(i64)}
#[derive(Default)]pub(crate) struct SqlTiming{pub prepare_ns:u64,pub bind_ns:u64,pub step_ns:u64}
pub(crate) fn stmt(db:&Db,cmd:&str,args:&[V])->Result<Vec<Vec<String>>,String>{stmt_measured(db,cmd,args,None)}
pub(crate) fn stmt_measured(db:&Db,cmd:&str,args:&[V],mut timing:Option<&mut SqlTiming>)->Result<Vec<Vec<String>>,String>{
 let started=timing.as_ref().map(|_|std::time::Instant::now());
 let c=CString::new(cmd).map_err(|_|"sql_nul")?;
 let mut raw=std::ptr::null_mut();
 if unsafe{sqlite3_prepare_v2(db.0,c.as_ptr(),-1,
  &mut raw,std::ptr::null_mut())}!=SQLITE_OK||raw.is_null(){
  return Err("prepare_failed".into())
 }
 let st=Stmt(raw);
 if let(Some(t),Some(at))=(timing.as_deref_mut(),started){t.prepare_ns+=at.elapsed().as_nanos()as u64;}
 let binding=timing.as_ref().map(|_|std::time::Instant::now());
 let mut keep=Vec::<CString>::new();
 for (idx,val) in args.iter().enumerate(){
  let pos=(idx+1)as c_int;
  let rc=match val{
   V::Num(v)=>unsafe{sqlite3_bind_int64(st.0,pos,*v)},
   V::Str(s)=>{
    let c=CString::new(s.as_str()).map_err(|_|"bad_value_nul")?;
    let rc=unsafe{sqlite3_bind_text(st.0,pos,c.as_ptr(),
      c.as_bytes().len()as c_int,None)};
    keep.push(c);rc
   }
  };
  if rc!=SQLITE_OK{return Err("bind_failed".into())}
 }
 if let(Some(t),Some(at))=(timing.as_deref_mut(),binding){t.bind_ns+=at.elapsed().as_nanos()as u64;}
 let stepping=timing.as_ref().map(|_|std::time::Instant::now());
 let mut rows=Vec::new();
 loop{
  let rc=unsafe{sqlite3_step(st.0)};
  if rc==SQLITE_DONE{break}
  if rc!=SQLITE_ROW{return Err("sql_step_failed".into())}
  let mut fields=Vec::new();
  for i in 0..unsafe{sqlite3_column_count(st.0)}{
   let ptr=unsafe{sqlite3_column_text(st.0,i)};
   let n=unsafe{sqlite3_column_bytes(st.0,i)};
   if ptr.is_null()||!(0..=2_000_000).contains(&n){
    return Err("bad_result_cell".into())
   }
   let b=unsafe{std::slice::from_raw_parts(ptr,n as usize)};
   fields.push(String::from_utf8(b.to_vec()).map_err(|_|"invalid_cell_utf8")?);
  }
  rows.push(fields);
  if rows.len()>5000{return Err("excessive_result_rows".into())}
 }
 drop(st);
 if let(Some(t),Some(at))=(timing.as_deref_mut(),stepping){t.step_ns+=at.elapsed().as_nanos()as u64;}
 Ok(rows)
}
fn head(db:&Db)->Result<(u64,u64,String),String>{
 let r=stmt(db,"SELECT seq,frames,chain FROM control WHERE id=1",&[])?;
 if r.len()!=1||r[0].len()!=3{return Err("missing_control".into())}
 Ok((r[0][0].parse().map_err(|_|"bad_seq")?,
     r[0][1].parse().map_err(|_|"bad_count")?,r[0][2].clone()))
}
fn transaction<T>(db:&Db,f:impl FnOnce()->Result<T,String>)->Result<T,String>{
 sql(db,"BEGIN IMMEDIATE")?;
 match f(){
  Ok(v)=>match sql(db,"COMMIT"){
   Ok(())=>Ok(v),
   Err(commit_error)=>match sql(db,"ROLLBACK"){
    Ok(())=>Err(format!("commit_failed:{commit_error}; transaction_rolled_back")),
    Err(rollback_error)=>Err(format!("commit_failed:{commit_error}; rollback_failed:{rollback_error}")),
   }
  },
  Err(e)=>{let _=sql(db,"ROLLBACK");Err(e)}
 }
}

const DDL:&str=r#"
CREATE TABLE control(id INTEGER PRIMARY KEY CHECK(id=1),
 seq INTEGER NOT NULL,frames INTEGER NOT NULL,chain TEXT NOT NULL);
CREATE TABLE latest(venue TEXT NOT NULL,base TEXT NOT NULL,
 book TEXT NOT NULL,book_hash TEXT NOT NULL,last_seq INTEGER NOT NULL,
 PRIMARY KEY(venue,base));
CREATE TABLE pending(seq INTEGER PRIMARY KEY,body TEXT NOT NULL,body_hash TEXT NOT NULL);
CREATE TABLE decision_due(id INTEGER PRIMARY KEY CHECK(id=1),
 source_seq INTEGER NOT NULL,hashes_json TEXT NOT NULL,hashes_sha256 TEXT NOT NULL,
 inputs_json TEXT NOT NULL,inputs_sha256 TEXT NOT NULL);
CREATE TABLE decision_totals(id INTEGER PRIMARY KEY CHECK(id=1),
 sealed_batches INTEGER NOT NULL,sealed_frames INTEGER NOT NULL,
 proof_chain TEXT NOT NULL);
CREATE TABLE epochs(venue TEXT NOT NULL,base TEXT NOT NULL,
 epoch INTEGER NOT NULL,source_seq INTEGER NOT NULL,
 PRIMARY KEY(venue,base));
CREATE TABLE portfolio_state(id INTEGER PRIMARY KEY CHECK(id=1),
 body TEXT NOT NULL,body_hash TEXT NOT NULL,
 last_seq INTEGER NOT NULL,source_seq INTEGER NOT NULL);
CREATE TABLE ingress_origin(id INTEGER PRIMARY KEY CHECK(id=1),journal TEXT NOT NULL);
CREATE TABLE ingress_receipts(journal TEXT NOT NULL,id INTEGER NOT NULL,raw_hash TEXT NOT NULL,frame_hash TEXT NOT NULL,source_seq INTEGER NOT NULL,PRIMARY KEY(journal,id));
CREATE TABLE audit(id TEXT PRIMARY KEY,kind TEXT NOT NULL,seq INTEGER NOT NULL,
 body TEXT NOT NULL,body_hash TEXT NOT NULL);
CREATE TRIGGER audit_no_update BEFORE UPDATE ON audit
 BEGIN SELECT RAISE(ABORT,'IMMUTABLE_PAPER_AUDIT'); END;
CREATE TRIGGER audit_no_delete BEFORE DELETE ON audit
 BEGIN SELECT RAISE(ABORT,'IMMUTABLE_PAPER_AUDIT'); END;
"#;
fn open_new(root:&Path,name:&str)->Result<Db,String>{
 if name!="db1.sqlite"&&name!="db2.sqlite"{return Err("unsupported_file".into())}
 let path=root.join(name);
 let f=fs::OpenOptions::new().create_new(true).write(true).mode(0o600)
   .open(&path).map_err(|_|"create_only_denied")?;
 f.sync_all().map_err(|_|"new_db_fsync_failed")?;
 drop(f);
 let c=CString::new(path.to_string_lossy().as_bytes()).map_err(|_|"path_nul")?;
 let mut ptr=std::ptr::null_mut();
 if unsafe{sqlite3_open_v2(c.as_ptr(),&mut ptr,READWRITE_EXISTING,
    std::ptr::null())}!=SQLITE_OK||ptr.is_null(){
  if !ptr.is_null(){unsafe{sqlite3_close(ptr);}}
  return Err("db_open_failed".into())
 }
 let db=Db(ptr);
 sql(&db,"PRAGMA journal_mode=WAL;PRAGMA synchronous=FULL;
  PRAGMA busy_timeout=5000;PRAGMA foreign_keys=ON;")?;
 sql(&db,DDL)?;
 sql(&db,"PRAGMA user_version=1205")?;
 stmt(&db,"INSERT INTO decision_totals VALUES(1,0,0,?)",&[V::Str(ZERO.into())])?;
 stmt(&db,"INSERT INTO control VALUES(1,0,0,?)",&[V::Str(ZERO.into())])?;
 Ok(db)
}
fn open_existing(root:&Path,name:&str)->Result<Db,String>{
 if name!="db1.sqlite"&&name!="db2.sqlite"{return Err("bad_db_name".into())}
 let path=root.join(name);
 let m=fs::symlink_metadata(&path).map_err(|_|"db_missing")?;
 if !m.is_file()||m.nlink()!=1||m.file_type().is_symlink(){
  return Err("unsafe_existing_file".into())
 }
 let c=CString::new(path.to_string_lossy().as_bytes()).map_err(|_|"path_nul")?;
 let mut ptr=std::ptr::null_mut();
 if unsafe{sqlite3_open_v2(c.as_ptr(),&mut ptr,READWRITE_EXISTING,
    std::ptr::null())}!=SQLITE_OK||ptr.is_null(){
  if !ptr.is_null(){unsafe{sqlite3_close(ptr);}}
  return Err("db_open_failed".into())
 }
 let db=Db(ptr);
 sql(&db,"PRAGMA journal_mode=WAL;PRAGMA synchronous=FULL;
  PRAGMA busy_timeout=5000;")?;
 if stmt(&db,"PRAGMA user_version",&[])?!=vec![vec!["1205".to_string()]]{
  return Err("legacy_or_unknown_compact_schema_denied".into())
 }
 Ok(db)
}
fn valid_root(root:&Path)->Result<(),String>{
 if root.parent()!=Some(std::env::temp_dir().as_path())||
  !root.file_name().and_then(|s|s.to_str())
   .is_some_and(|s|s.starts_with("spotarb_r1173_")&&s.len()<=90){
  return Err("nonisolated_root_denied".into())
 }
 let m=fs::symlink_metadata(root).map_err(|_|"root_missing")?;
 if !m.is_dir()||m.file_type().is_symlink()||
   m.uid()!=unsafe{libc::geteuid()}||m.mode()&0o077!=0{
  return Err("unsafe_private_root".into())
 }
 Ok(())
}

#[derive(Clone,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
struct Change{venue:String,base:String,book:String,hash:String}
#[derive(Clone,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
struct ResetMark{venue:String,base:String,epoch:u64}
#[derive(Clone,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
struct Audit{id:String,kind:String,body:String,hash:String}
#[derive(Clone,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
struct PortfolioWrite{
 previous_hash:String,after_hash:String,after_json:String,
 source_seq:u64,
}
#[derive(Clone,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
struct DecisionReceipt{input_hash:String,outcome:String,result_hash:String}
#[derive(Clone,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
struct IngressReceipt{journal:String,id:u64,raw_hash:String,frame_hash:String}
#[derive(Clone,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
struct Batch{
 #[serde(default)] ingress_receipts:Vec<IngressReceipt>,
 #[serde(default)] ingress_reclaim:Vec<(String,u64)>,
 schema:String,seq:u64,count:u64,n:u64,
 prev:String,chain:String,changes:Vec<Change>,
 resets:Vec<ResetMark>,audit:Vec<Audit>,
 #[serde(default)]
 portfolio:Option<PortfolioWrite>,
 #[serde(default)]
 decision_due:Option<Vec<String>>,
 #[serde(default)]
 decision_inputs:Option<Vec<RawFrame>>,
 #[serde(default)]
 decision_seal:Option<Vec<DecisionReceipt>>,
}
#[derive(Clone,Debug)]
pub struct PaperWitness{
 pub id:String,pub kind:String,pub body:serde_json::Value,
}
fn durable_portfolio(input:&PaperPortfolio)->PaperPortfolio{
 // Diagnostics are non-accounting: they may increase on every incoming
 // quote without producing a durable modeled trade.
 let mut p=input.clone();
 p.stats.held_negative_marks=0;
 p.stats.held_positive_marks=0;
 p.stats.denied_metadata=0;
 p.stats.denied_liquidity=0;
 p.stats.denied_stale=0;
 p.stats.denied_entry_guard=0;
 p.stats.denied_daily_limit=0;
 p.stats.denied_concurrent_limit=0;
 p.stats.unknown_unhedged=0;
 // Per-frame hedge exit threshold is re-derived on every observation,
 // not a durable position fact. It changes on HOLD ticks without a leg.
 for m in p.positions.values_mut(){m.config.take_profit_quote=None;}
 p
}
fn validate_modeled_leg(before:&PaperPortfolio,after:&PaperPortfolio,base:&str)
 ->Result<&'static str,String>{
 if before.cold_boot_unknown||after.cold_boot_unknown||
    before.day_yek!=after.day_yek||
    before.started_wall_ms!=after.started_wall_ms||
    before.max_concurrent_positions!=after.max_concurrent_positions||
    before.max_daily_entries!=after.max_daily_entries||
    before.assumptions.quote_budget!=after.assumptions.quote_budget||
    before.positions.len()>before.max_concurrent_positions||
    after.positions.len()>after.max_concurrent_positions{
   return Err("modeled_guard_or_config_mutation".into())
 }
 for (k,v) in &before.positions{
  if k!=base {
   let same=after.positions.get(k).and_then(|n|serde_json::to_value(n).ok())
      ==serde_json::to_value(v).ok();
   if !same{return Err("unrelated_position_mutated".into())}
  }
 }
 for (k,_) in &after.positions{
  if k!=base&&!before.positions.contains_key(k){
   return Err("unrelated_position_added".into())
  }
 }
 let old=before.positions.get(base);
 let new=after.positions.get(base);
 let event=match (old.map(|m|&m.phase),new.map(|m|&m.phase)){
  (None,Some(PaperPhase::AwaitingHedge(_)))=>"SPOT_BUY_MODELED",
  (Some(PaperPhase::AwaitingHedge(_)),Some(PaperPhase::Hedged(_)))=>
    "FUTURES_SHORT_MODELED",
  (Some(PaperPhase::Hedged(_)),Some(PaperPhase::AwaitingSpotExit{..}))=>
    "FUTURES_COVER_MODELED",
  (Some(PaperPhase::AwaitingSpotExit{..}),None)=>"SPOT_SELL_MODELED",
  _=>return Err("modeled_phase_transition_out_of_order".into())
 };
 let cs=&before.stats;let ns=&after.stats;
 let entries=ns.entries.checked_sub(cs.entries).ok_or("modeled_entry_counter_rollback")?;
 let hedges=ns.futures_hedges.checked_sub(cs.futures_hedges).ok_or("modeled_hedge_counter_rollback")?;
 let covers=ns.futures_covers.checked_sub(cs.futures_covers).ok_or("modeled_cover_counter_rollback")?;
 let closed=ns.closed.checked_sub(cs.closed).ok_or("modeled_closed_counter_rollback")?;
 let expected=match event{
  "SPOT_BUY_MODELED"=>(1,0,0,0),
  "FUTURES_SHORT_MODELED"=>(0,1,0,0),
  "FUTURES_COVER_MODELED"=>(0,0,1,0),
  _=>(0,0,0,1)
 };
 if (entries,hedges,covers,closed)!=expected{
  return Err("modeled_four_leg_counters_mismatch".into())
 }
 if !after.stats.modeled_realized_usdt.is_finite()||
  (event!="SPOT_SELL_MODELED"&&
   after.stats.modeled_realized_usdt!=cs.modeled_realized_usdt){
  return Err("unexpected_modeled_realized_change".into())
 }
 if event=="SPOT_SELL_MODELED"{
  if !after.closed_bases.contains(base)||
    !after.closed_at_monotonic_ns.contains_key(base)||
    ns.closed_wins+ns.closed_losses!=cs.closed_wins+cs.closed_losses+1{
   return Err("modeled_close_receipt_incomplete".into())
  }
 }
 Ok(event)
}
#[derive(Clone,Copy,Debug,Eq,PartialEq)]
pub enum Inject{None,AfterDb1,AfterDb2}
/// Consumed proof of durable inbox outcomes, bound to one canonical head.
/// Fields stay private: callers cannot forge or persist a release authority.
pub struct IngressReleaseTicket{root:PathBuf,head:(u64,u64,String),refs:Vec<(String,u64)>}
/// Canonical receipts verified before acquiring the ingress Store mutex.
pub struct VerifiedIngressBatch{root:PathBuf,head:(u64,u64,String),entries:Vec<VerifiedIngressReceipt>}
#[derive(Clone,Debug,Eq,PartialEq)]
pub struct VerifiedIngressReceipt{journal:String,id:u64,raw_hash:String,frame_hash:String,source_seq:u64,proof:String}
/// Constructed only after the FULL inbox transaction commits.
pub struct DurableInboxPublish{root:PathBuf,journal:String,entries:Vec<VerifiedIngressReceipt>}
/// Constructed only after the mirrored canonical reclaim is ACKed.
pub struct AckedIngressReclaim{root:PathBuf,journal:String,entries:Vec<VerifiedIngressReceipt>}
pub struct Writer{
 root:PathBuf,db1:Db,db2:Db,_lock:fs::File,
 receipts:Vec<DecisionReceipt>,
 // R1183: at most one ACK-confirmed guarded batch; never restored cold.
 expected_frame_hashes:Vec<String>,
 modeled_receipt_binding:Option<(usize,String)>,
 cold_unresolved_decision:bool,
}
fn lock(root:&Path)->Result<fs::File,String>{
 let f=fs::OpenOptions::new().create(true).read(true).write(true)
  .mode(0o600).custom_flags(libc::O_NOFOLLOW|libc::O_CLOEXEC)
  .open(root.join("writer.lock")).map_err(|_|"writer_lock_open_failed")?;
 if unsafe{libc::flock(f.as_raw_fd(),libc::LOCK_EX|libc::LOCK_NB)}!=0{
  return Err("writer_already_active".into())
 }
 Ok(f)
}
fn ven(v:Venue)->&'static str{match v{
 Venue::BinanceFutures=>"binance_public_futures",
 Venue::OkxSpot=>"okx_public_spot"
}}
fn allowed_kind(kind:&str)->bool{
 matches!(kind,"SIGNAL"|"OPPORTUNITY_DENIED"|"TRADE_INTENT"|"TRADE_EVENT"|"SPOT_BUY_MODELED"|
  "FUTURES_SHORT_MODELED"|"FUTURES_COVER_MODELED"|"SPOT_SELL_MODELED"|
  "SOURCE_RESET"|"UNKNOWN"|"SESSION_SEAL"|"PORTFOLIO_BASELINE"|
  "PORTFOLIO_TRANSITION"|"LEGACY_RECOVERY")
}
fn to_i(v:u64)->Result<i64,String>{i64::try_from(v).map_err(|_|"integer_overflow".into())}
fn make_batch(frames:&[(RawFrame,Update,usize)],owner_updates:&[Update],
 head:(u64,u64,String),witnesses:&[PaperWitness])->Result<Batch,String>{
 if frames.is_empty()||frames.len()>32||frames.len()!=owner_updates.len(){
  return Err("bad_batch_cardinality".into())
 }
 let mut by_key=BTreeMap::<(String,String),String>::new();
 let mut source_hashes=String::new();
 for ((frame,original,_),validated) in frames.iter().zip(owner_updates){
  if frame.raw.is_empty()||frame.raw.len()>32768||
    original.base!=validated.base||original.venue!=validated.venue||
    frame.venue!=validated.venue||frame.epoch!=validated.book.epoch{
   return Err("source_consistency_denied".into())
  }
  let b=serde_json::to_string(&validated.book).map_err(|_|"serialize_book_failed")?;
  by_key.insert((ven(frame.venue).into(),validated.base.clone()),b);
  source_hashes.push_str(&if frame.source.is_some(){decision_frame_identity(frame)}else{hash(&frame.raw)});
 }
 let mut audits=Vec::new();
 for x in witnesses{
  if !allowed_kind(&x.kind)||x.id.len()>96||
    x.id.is_empty()||!x.id.bytes().all(|b|b.is_ascii_alphanumeric())||
    x.body["paper_only"]!=true||x.body["real_fills_attested"]!=false{
   return Err("paper_witness_denied".into())
  }
  let body=serde_json::to_string(&x.body).map_err(|_|"audit_json_failed")?;
  audits.push(Audit{id:x.id.clone(),kind:x.kind.clone(),
    hash:hash(&body),body});
 }
 let count=head.1.checked_add(frames.len()as u64)
   .ok_or("frame_count_overflow")?;
 Ok(Batch{ingress_receipts:Vec::new(),ingress_reclaim:Vec::new(),schema:SCHEMA.into(),seq:head.0.checked_add(1).ok_or("seq_overflow")?,
   count,n:frames.len()as u64,prev:head.2.clone(),
   chain:hash(&(head.2+&source_hashes)),
   changes:by_key.into_iter().map(|((venue,base),book)|
      Change{venue,base,hash:hash(&book),book}).collect(),
   resets:Vec::new(),audit:audits,portfolio:None,
   decision_due:None,decision_inputs:None,decision_seal:None})
}

fn legacy_source_record_has_identity_and_order(record:&serde_json::Value)->bool{
 let event=&record["event"];let owner=&record["isolated"]["identity"];
 let identity_valid=owner["run_id"].as_str().is_some_and(|v|!v.is_empty())&&
  owner["profile"]["base"].as_str().is_some_and(|v|!v.is_empty());
 let legacy_intent_valid=event["intent"]["instrument"].as_str().is_some_and(|v|!v.is_empty());
 let lifecycle_orders=record["lifecycle"]["orders"].as_object().is_some_and(|v|!v.is_empty());
 let event_order=event["order_id"].as_str().filter(|v|!v.is_empty());
 let intent_order=event_order.is_some_and(|id|event["intent"]["order_id"].as_str()==Some(id));
 let census=event["census_order_ids"].as_array().is_some_and(|a|!a.is_empty());
 event.is_object()&&event["simulated"]==true&&(identity_valid||legacy_intent_valid)&&
  (lifecycle_orders||intent_order||census)
}
fn apply(db:&Db,b:&Batch)->Result<(),String>{
 if b.schema!=SCHEMA||b.seq==0||b.n>32||
  (b.n==0&&b.resets.is_empty()&&b.audit.is_empty()&&b.portfolio.is_none()&&b.decision_seal.is_none()&&b.ingress_reclaim.is_empty())||
  (b.portfolio.is_some()&&(b.n!=0||!b.resets.is_empty()||
      b.audit.len()!=1||b.decision_due.is_some()||b.decision_seal.is_some()))||
  (!b.resets.is_empty()&&(b.n!=0||b.portfolio.is_some()||
      b.decision_due.is_some()||b.decision_seal.is_some()))||
  (b.decision_due.is_some()&&(b.n==0||b.decision_seal.is_some()))||
  (b.decision_due.is_some()!=b.decision_inputs.is_some())||
  (b.decision_seal.is_some()&&(b.n!=0||!b.resets.is_empty()||
      b.portfolio.is_some()||!b.audit.is_empty())){
  return Err("bad_compact_batch".into())}
 if b.ingress_receipts.len()>32||b.ingress_reclaim.len()>1024||
    (!b.ingress_reclaim.is_empty()&&(b.n!=0||!b.resets.is_empty()||b.portfolio.is_some()||b.decision_due.is_some()||b.decision_seal.is_some())){return Err("ingress_receipt_bounds".into())}
 for receipt in &b.ingress_receipts{
  let origin=stmt(db,"SELECT journal FROM ingress_origin WHERE id=1",&[])?;
  if origin.is_empty(){stmt(db,"INSERT INTO ingress_origin VALUES(1,?)",&[V::Str(receipt.journal.clone())])?;}
  else if origin!=vec![vec![receipt.journal.clone()]]{return Err("canonical_ingress_journal_changed".into())}
  if receipt.journal.len()!=64||receipt.id==0||!is_sha256_hex(&receipt.raw_hash)||!is_sha256_hex(&receipt.frame_hash){return Err("ingress_receipt_schema".into())}
  stmt(db,"INSERT INTO ingress_receipts VALUES(?,?,?,?,?)",&[V::Str(receipt.journal.clone()),V::Num(to_i(receipt.id)?),V::Str(receipt.raw_hash.clone()),V::Str(receipt.frame_hash.clone()),V::Num(to_i(b.seq)?)])?;
 }
 for (journal,id) in &b.ingress_reclaim{stmt(db,"DELETE FROM ingress_receipts WHERE journal=? AND id=?",&[V::Str(journal.clone()),V::Num(to_i(*id)?)])?;}
 if !b.ingress_receipts.is_empty(){
  let inputs=b.decision_inputs.as_ref().ok_or("ingress_requires_guarded_raw_inputs")?;
  let actual=inputs.iter().filter_map(|f|f.source.as_ref().map(|p|(p.journal_id.clone(),p.accepted_id,p.payload_sha256.clone(),decision_frame_identity(f)))).collect::<Vec<_>>();
  let declared=b.ingress_receipts.iter().map(|r|(r.journal.clone(),r.id,r.raw_hash.clone(),r.frame_hash.clone())).collect::<Vec<_>>();
  if actual!=declared||actual.len()>b.n as usize||inputs.iter().any(|f|f.source.as_ref().is_some_and(|p|p.payload_sha256!=hash(&f.raw))){return Err("ingress_receipt_raw_binding".into())}
 }
 let h=head(db)?;
 if b.seq!=h.0+1||b.count!=h.1+b.n||b.prev!=h.2{
  return Err("source_cursor_mismatch".into())
 }
 for reset in &b.resets{
  if reset.epoch==0||reset.base.is_empty()||reset.base.len()>30||
    !matches!(reset.venue.as_str(),"binance_public_futures"|"okx_public_spot"){
   return Err("invalid_source_reset".into())
  }
  let prior=one(db,"SELECT epoch FROM epochs WHERE venue=? AND base=?",&[
    V::Str(reset.venue.clone()),V::Str(reset.base.clone())])?;
  if prior.as_ref().is_some_and(|r|r[0].parse::<u64>().is_ok_and(|v|v>=reset.epoch)){
   return Err("source_reset_epoch_nonmonotonic".into())
  }
  stmt(db,"INSERT INTO epochs VALUES(?,?,?,?) ON CONFLICT(venue,base) DO UPDATE SET epoch=excluded.epoch,source_seq=excluded.source_seq",&[
   V::Str(reset.venue.clone()),V::Str(reset.base.clone()),
   V::Num(to_i(reset.epoch)?),V::Num(to_i(b.seq)?)])?;
  stmt(db,"DELETE FROM latest WHERE venue=? AND base=?",&[
   V::Str(reset.venue.clone()),V::Str(reset.base.clone())])?;
 }
 for u in &b.changes{
  if u.base.is_empty()||u.base.len()>30||hash(&u.book)!=u.hash{
   return Err("book_checksum_denied".into())
  }
  let book:Book=serde_json::from_str(&u.book).map_err(|_|"bad_book_json")?;
  if book.bids.is_empty()||book.asks.is_empty()||
    book.changed_ns>book.received_ns||book.exchange_ts_ms.is_none()||
    book.bids.len()>20||book.asks.len()>20{
   return Err("bad_book_fields".into())
  }
  stmt(db,"INSERT INTO latest VALUES(?,?,?,?,?)
    ON CONFLICT(venue,base) DO UPDATE SET
     book=excluded.book,book_hash=excluded.book_hash,
     last_seq=excluded.last_seq",&[
      V::Str(u.venue.clone()),V::Str(u.base.clone()),
      V::Str(u.book.clone()),V::Str(u.hash.clone()),
      V::Num(to_i(b.seq)?)])?;
 }
 if let Some(hashes)=&b.decision_due{
  let inputs=b.decision_inputs.as_ref().ok_or("decision_inputs_missing")?;
  if inputs.len()!=hashes.len()||inputs.iter().zip(hashes).any(|(f,h)|decision_frame_identity(f)!=*h)||
     hashes.len()!=b.n as usize||hashes.iter().any(|h|h.len()!=64)||
     read_due(db)?.is_some(){
    return Err("previous_decision_obligation_unresolved".into())
  }
  let body=serde_json::to_string(hashes).map_err(|_|"due_encoding_failed")?;
  let inputs_json=serde_json::to_string(inputs).map_err(|_|"decision_inputs_encode")?;
  stmt(db,"INSERT INTO decision_due VALUES(1,?,?,?,?,?)",&[
    V::Num(to_i(b.seq)?),V::Str(body.clone()),V::Str(hash(&body)),
    V::Str(inputs_json.clone()),V::Str(hash(&inputs_json))])?;
 }
 if let Some(receipts)=&b.decision_seal{
  let (source,hashes)=read_due(db)?
     .ok_or("decision_seal_without_obligation")?;
  if source>=b.seq||hashes.len()!=receipts.len()||
     !receipts.iter().zip(&hashes).all(|(r,h)|
       r.input_hash==*h&&valid_decision_outcome(&r.outcome)&&
         is_sha256_hex(&r.result_hash)){
   return Err("decision_receipt_order_mismatch".into())
  }
  let control=one(db,
    "SELECT sealed_batches,sealed_frames,proof_chain FROM decision_totals WHERE id=1",&[])?
    .ok_or("missing_decision_totals")?;
  let prior_b=control[0].parse::<u64>().map_err(|_|"invalid_sealed_batches")?;
  let prior_f=control[1].parse::<u64>().map_err(|_|"invalid_sealed_frames")?;
  let proof=serde_json::to_string(receipts).map_err(|_|"receipt_encoding_failed")?;
  let chain=hash(&(control[2].clone()+&hash(&proof)));
  stmt(db,"DELETE FROM decision_due WHERE id=1",&[])?;
  stmt(db,"UPDATE decision_totals SET sealed_batches=?,sealed_frames=?,proof_chain=? WHERE id=1",&[
    V::Num(to_i(prior_b.checked_add(1).ok_or("batch_counter_overflow")?)?),
    V::Num(to_i(prior_f.checked_add(receipts.len()as u64).ok_or("frame_counter_overflow")?)?),
    V::Str(chain)])?;
 }
 for a in &b.audit{
  if !allowed_kind(&a.kind)||hash(&a.body)!=a.hash{return Err("bad_audit".into())}
  let value:serde_json::Value=serde_json::from_str(&a.body)
    .map_err(|_|"bad_audit_json")?;
  if value["paper_only"]!=true||value["real_fills_attested"]!=false{
   return Err("real_audit_denied".into())
  }
  if a.kind=="TRADE_INTENT"{
   let trade_id=value["trade_id"].as_str().ok_or("trade_intent_id_missing")?;
   if value["schema"]!="SPOTARB/R1326/TRADE-INTENT/V1"||
      trade_id.is_empty()||trade_id.len()>128||
      a.id!=format!("R1326T{}",hash(trade_id))||
      value["risk_passed"]!=true||
      value["generation"].as_u64().unwrap_or(0)==0||
      !value["signal_evidence"].is_object()||
      !value["risk_decision"].is_object(){
    return Err("trade_intent_evidence_invalid".into())
   }
  }
  if a.kind=="TRADE_EVENT"{
   let trade_id=value["trade_id"].as_str().ok_or("trade_event_id_missing")?;
   if value["schema"]!="SPOTARB/R1326/TRADE-EVENT/V1"||
      trade_id.is_empty()||trade_id.len()>128||
      !value["event_id"].is_string()||!value["event"].is_object(){
    return Err("trade_event_evidence_invalid".into())
   }
  }
  if a.kind=="LEGACY_RECOVERY"{
   if value["schema"]!="SPOTARB/R1326/LEGACY-RECOVERY/V1"||
      value["status"]!="RECONCILIATION_REQUIRED"||
      value["evidence"]["source_sequence"].as_u64().unwrap_or(0)==0||
      value["evidence"]["source_journal_sha256"].as_str().map_or(true,|v|v.len()!=64)||
      value["evidence"]["source_record_sha256"].as_str().map_or(true,|v|v.len()!=64)||
      !legacy_source_record_has_identity_and_order(&value["evidence"]["source_record"])||
      value.get("risk_passed").is_some()||value.get("signal_evidence").is_some()||
      value.get("risk_decision").is_some()||value.get("trade_id").is_some(){
    return Err("legacy_recovery_evidence_invalid".into())
   }
  }
  stmt(db,"INSERT INTO audit VALUES(?,?,?,?,?)",&[
   V::Str(a.id.clone()),V::Str(a.kind.clone()),V::Num(to_i(b.seq)?),
   V::Str(a.body.clone()),V::Str(a.hash.clone())])?;
 }
 for write in b.portfolio.iter(){
  if hash(&write.after_json)!=write.after_hash||
    write.source_seq.checked_add(1)!=Some(b.seq)||
    write.previous_hash.len()!=64||
    !matches!(b.audit[0].kind.as_str(),"PORTFOLIO_BASELINE"|
      "SPOT_BUY_MODELED"|"FUTURES_SHORT_MODELED"|
      "FUTURES_COVER_MODELED"|"SPOT_SELL_MODELED"){
   return Err("portfolio_atomic_payload_invalid".into())
  }
  let journal:serde_json::Value=serde_json::from_str(&b.audit[0].body)
    .map_err(|_|"portfolio_witness_invalid")?;
  if journal["after"]!=write.after_json||
     journal["after_hash"]!=write.after_hash||
     journal["before_hash"]!=write.previous_hash||
     journal["source_ledger_predecessor"]!=write.source_seq||
     journal["kind"]!=b.audit[0].kind||
     journal["real_fills_attested"]!=false||
     journal["paper_only"]!=true{
   return Err("portfolio_witness_state_mismatch".into())
  }
  let portfolio:PaperPortfolio=serde_json::from_str(&write.after_json)
    .map_err(|_|"portfolio_snapshot_invalid")?;
  if portfolio.max_concurrent_positions==0||
     portfolio.positions.len()>portfolio.max_concurrent_positions||
     portfolio.cold_boot_unknown==false{
   // Deserialized portfolio must be quarantined until operator reconciliation.
   return Err("portfolio_not_quarantined".into())
  }
  let prior=one(db,
    "SELECT body,body_hash FROM portfolio_state WHERE id=1",&[])?;
  let prior_hash=prior.as_ref().map(|x|x[1].as_str()).unwrap_or(ZERO);
  if prior_hash!=write.previous_hash{
   return Err("portfolio_predecessor_hash_mismatch".into())
  }
  if let Some(ref old)=prior{
   if hash(&old[0])!=old[1]||
      journal["before"].as_str()!=Some(old[0].as_str()){
    return Err("portfolio_original_witness_mismatch".into())
   }
   let mut before:PaperPortfolio=serde_json::from_str(&old[0])
      .map_err(|_|"portfolio_prior_invalid")?;
   before.cold_boot_unknown=false;
   let mut next=portfolio.clone();
   next.cold_boot_unknown=false;
   let event=validate_modeled_leg(&before,&next,
      journal["base"].as_str().ok_or("modeled_base_missing")?)?;
   if event!=b.audit[0].kind{
    return Err("modeled_transition_kind_mismatch".into())
   }
  }else if !journal["before"].is_null()||
    b.audit[0].kind!="PORTFOLIO_BASELINE"||
    !portfolio.positions.is_empty()||portfolio.stats.entries!=0{
   return Err("modeled_baseline_witness_invalid".into())
  }
  stmt(db,"INSERT INTO portfolio_state VALUES(1,?,?,?,?)
    ON CONFLICT(id) DO UPDATE SET body=excluded.body,
      body_hash=excluded.body_hash,last_seq=excluded.last_seq,
      source_seq=excluded.source_seq",&[
     V::Str(write.after_json.clone()),V::Str(write.after_hash.clone()),
     V::Num(to_i(b.seq)?),V::Num(to_i(write.source_seq)?)])?;
 }
 stmt(db,"UPDATE control SET seq=?,frames=?,chain=? WHERE id=1",&[
  V::Num(to_i(b.seq)?),V::Num(to_i(b.count)?),V::Str(b.chain.clone())])?;
 Ok(())
}
fn one(db:&Db,q:&str,args:&[V])->Result<Option<Vec<String>>,String>{
 let mut out=stmt(db,q,args)?;
 if out.len()>1{return Err("nonunique_result".into())}
 Ok(out.pop())
}
fn read_due(db:&Db)->Result<Option<(u64,Vec<String>)>,String>{
 let r=one(db,"SELECT source_seq,hashes_json,hashes_sha256,inputs_json,inputs_sha256 FROM decision_due WHERE id=1",&[])?;
 match r{
  None=>Ok(None),
  Some(v)=>{
   if hash(&v[1])!=v[2]||hash(&v[3])!=v[4]{return Err("decision_due_checksum_mismatch".into())}
   let hashes:Vec<String>=serde_json::from_str(&v[1]).map_err(|_|"bad_decision_due")?;
   if hashes.is_empty()||hashes.len()>32||hashes.iter().any(|h|h.len()!=64){
    return Err("invalid_decision_batch".into())
   }
   let inputs:Vec<RawFrame>=serde_json::from_str(&v[3])
      .map_err(|_|"bad_decision_inputs")?;
   if inputs.len()!=hashes.len()||inputs.iter().zip(&hashes).any(|(f,h)|decision_frame_identity(f)!=*h){
    return Err("decision_inputs_identity_mismatch".into())
   }
   Ok(Some((v[0].parse().map_err(|_|"bad_due_seq")?,hashes)))
  }
 }
}
fn decision_frame_identity(frame:&RawFrame)->String{
 let old=format!("{}:{}:{}:{}:{}",ven(frame.venue),frame.epoch,frame.received_ns,frame.received_wall_ms,hash(&frame.raw));
 match &frame.source{
  None=>hash(&old),
  Some(p)=>{
   let mut source=serde_json::to_value(p).expect("source serialization");
   if let Some(fields)=source.as_object_mut(){fields.remove("r1279_timing");}
   hash(&format!("{old}:{}",source))
  }
 }
}

fn is_sha256_hex(s:&str)->bool{
 s.len()==64&&s.bytes().all(|x|x.is_ascii_digit()||(b'a'..=b'f').contains(&x))
}
fn valid_decision_outcome(o:&str)->bool{
 matches!(o,"EVALUATED"|"DECISION_DENIED"|"INPUT_REJECTED")
}
fn verify_changed(db:&Db,b:&Batch)->Result<(),String>{
 for r in &b.ingress_receipts{
  let rows=stmt(db,"SELECT raw_hash,frame_hash,source_seq FROM ingress_receipts WHERE journal=? AND id=?",&[V::Str(r.journal.clone()),V::Num(to_i(r.id)?)])?;
  if rows!=vec![vec![r.raw_hash.clone(),r.frame_hash.clone(),b.seq.to_string()]]{return Err("ingress_mirror_binding".into())}
 }
 for (journal,id) in &b.ingress_reclaim{if !stmt(db,"SELECT id FROM ingress_receipts WHERE journal=? AND id=?",&[V::Str(journal.clone()),V::Num(to_i(*id)?)])?.is_empty(){return Err("ingress_mirror_reclaim".into())}}

 if let Some(hashes)=&b.decision_due{
  let a=read_due(db)?.ok_or("mirrored_decision_due_missing")?;
  if a.0!=b.seq||a.1!=*hashes{return Err("mirrored_decision_due_mismatch".into())}
  let raw=one(db,"SELECT inputs_json FROM decision_due WHERE id=1",&[])?
    .ok_or("mirrored_decision_inputs_missing")?;
  if serde_json::from_str::<Vec<RawFrame>>(&raw[0]).ok()
      .and_then(|v|serde_json::to_string(&v).ok())!=
       b.decision_inputs.as_ref().and_then(|v|serde_json::to_string(v).ok()) {
    return Err("mirrored_decision_inputs_mismatch".into())
  }
 }
 if b.decision_seal.is_some()&&read_due(db)?.is_some(){
  return Err("mirrored_decision_seal_incomplete".into())
 }
 for reset in &b.resets{
  let e=one(db,"SELECT epoch FROM epochs WHERE venue=? AND base=?",&[
   V::Str(reset.venue.clone()),V::Str(reset.base.clone())])?;
  let latest=one(db,"SELECT book FROM latest WHERE venue=? AND base=?",&[
   V::Str(reset.venue.clone()),V::Str(reset.base.clone())])?;
  if e!=Some(vec![reset.epoch.to_string()])||latest.is_some(){
   return Err("mirrored_source_reset_mismatch".into())
  }
 }
 for u in &b.changes{
  let r=one(db,"SELECT book,book_hash FROM latest WHERE venue=? AND base=?",&[
   V::Str(u.venue.clone()),V::Str(u.base.clone())])?;
  if r!=Some(vec![u.book.clone(),u.hash.clone()]){
   return Err("mirrored_book_mismatch".into())
  }
 }
 if let Some(port)=&b.portfolio{
  let row=one(db,
   "SELECT body,body_hash,last_seq,source_seq FROM portfolio_state WHERE id=1",&[])?;
  let target=vec![port.after_json.clone(),port.after_hash.clone(),
                  b.seq.to_string(),port.source_seq.to_string()];
  if row!=Some(target){return Err("mirrored_portfolio_mismatch".into())}
 }
 for x in &b.audit{
  let r=one(db,"SELECT kind,body,body_hash FROM audit WHERE id=?",&[
   V::Str(x.id.clone())])?;
  if r!=Some(vec![x.kind.clone(),x.body.clone(),x.hash.clone()]){
   return Err("mirrored_audit_mismatch".into())
  }
 }
 Ok(())
}

impl Writer{
 pub fn create(root:&Path)->Result<Self,String>{
  valid_root(root)?;
  let lk=lock(root)?;
  let a=open_new(root,"db1.sqlite")?;
  let b=open_new(root,"db2.sqlite")?;
  Ok(Self{root:root.to_owned(),db1:a,db2:b,_lock:lk,receipts:Vec::new(),expected_frame_hashes:Vec::new(),modeled_receipt_binding:None,cold_unresolved_decision:false})
 }
 pub fn resume(root:&Path)->Result<Self,String>{
  valid_root(root)?;
  let lk=lock(root)?;
  let a=open_existing(root,"db1.sqlite")?;
  let b=open_existing(root,"db2.sqlite")?;
  let cold_unresolved_decision=read_due(&a)?.is_some();
  Ok(Self{root:root.to_owned(),db1:a,db2:b,_lock:lk,receipts:Vec::new(),expected_frame_hashes:Vec::new(),modeled_receipt_binding:None,cold_unresolved_decision})
 }
 #[cfg(feature="offline-test-clock")]
 pub fn r1327_fail_next_db1_commit(&self){R1327_FAIL_COMMIT_DB.with(|slot|slot.set(Some(self.db1.0 as usize)));}
 #[cfg(feature="offline-test-clock")]
 pub fn r1327_fail_next_db2_commit(&self){R1327_FAIL_COMMIT_DB.with(|slot|slot.set(Some(self.db2.0 as usize)));}
 pub fn source_reset(
  &mut self,owners:&mut [Owner],venue:Venue,bases:&[String],
  next_epoch:u64,source_ms:i64,provenance:&str,inject:Inject
 )->Result<u64,String>{
  if read_due(&self.db1)?.is_some()||!self.receipts.is_empty(){
   return Err("reset_blocked_by_pending_decisions".into())
  }
  if owners.len()!=12||bases.is_empty()||bases.len()>40||
   next_epoch==0||source_ms<=0||
   !matches!(provenance,"PUBLIC_WS_RESET"|"R1162_INFERRED_OFFLINE"){
   return Err("source_reset_provenance_denied".into())
  }
  if !stmt(&self.db1,"SELECT seq FROM pending",&[])?.is_empty()||
    head(&self.db1)?!=head(&self.db2)?{
   return Err("reset_requires_drained_source_ack".into())
  }
  let mut staged=BTreeMap::<usize,Owner>::new();
  let mut unique=std::collections::BTreeSet::new();
  let mut marks=Vec::new();
  for base in bases{
   if !unique.insert(base.as_str())||base.is_empty()||base.len()>30||
     !base.bytes().all(|c|c.is_ascii_uppercase()||c.is_ascii_digit()){
    return Err("source_reset_bad_bases".into())
   }
   let ix=owners.iter().position(|owner|owner.owns(base))
      .ok_or("reset_owner_not_found")?;
   let candidate=staged.entry(ix).or_insert_with(||owners[ix].clone());
   candidate.reset_venue(base,venue,next_epoch)
     .map_err(|e|format!("source_reset_owner_denied:{e:?}"))?;
   marks.push(ResetMark{venue:ven(venue).into(),base:base.clone(),epoch:next_epoch});
  }
  marks.sort_by(|x,y|x.base.cmp(&y.base));
  let h=head(&self.db1)?;let seq=h.0.checked_add(1).ok_or("reset_seq_overflow")?;
  let evidence=serde_json::json!({
   "paper_only":true,"real_fills_attested":false,
   "source":provenance,"venue":ven(venue),"bases":bases,
   "new_epoch":next_epoch,"observed_ms":source_ms});
  let body=serde_json::to_string(&evidence).map_err(|_|"reset_encode_failed")?;
  let witness=Audit{id:format!("R1173SOURCE{seq}"),kind:"SOURCE_RESET".into(),
   hash:hash(&body),body};
  let b=Batch{ingress_receipts:Vec::new(),ingress_reclaim:Vec::new(),schema:SCHEMA.into(),seq,count:h.1,n:0,prev:h.2.clone(),
   chain:hash(&(h.2+&witness.hash)),changes:Vec::new(),resets:marks,
   audit:vec![witness],portfolio:None,
   decision_due:None,decision_inputs:None,decision_seal:None};
  let body=serde_json::to_string(&b).map_err(|_|"reset_batch_encode_failed")?;
  transaction(&self.db1,||{
   apply(&self.db1,&b)?;
   stmt(&self.db1,"INSERT INTO pending VALUES(?,?,?)",&[
    V::Num(to_i(seq)?),V::Str(body.clone()),V::Str(hash(&body))])?;
   Ok(())
  })?;
  for (ix,new_owner) in staged{owners[ix]=new_owner;}
  if inject==Inject::AfterDb1{return Err("injected_reset_after_db1".into())}
  self.ack_pending(if inject==Inject::AfterDb2{Inject::AfterDb2}else{Inject::None})?;
  Ok(seq)
 }
 pub fn commit_modeled_portfolio(
  &mut self,before:Option<&PaperPortfolio>,after:&PaperPortfolio,
  base:&str,inject:Inject
 )->Result<u64,String>{
  self.commit_modeled_portfolio_inner(before,after,base,None,inject)
 }
 pub fn commit_modeled_portfolio_for_decision(
  &mut self,before:&PaperPortfolio,after:&PaperPortfolio,
  base:&str,frame:&RawFrame,cause:&serde_json::Value,result_hash:&str,inject:Inject
 )->Result<u64,String>{
  self.commit_modeled_portfolio_inner(Some(before),after,base,Some((frame,cause,result_hash)),inject)
 }
 fn commit_modeled_portfolio_inner(
  &mut self,before:Option<&PaperPortfolio>,after:&PaperPortfolio,
  base:&str,cause:Option<(&RawFrame,&serde_json::Value,&str)>,inject:Inject
 )->Result<u64,String>{
  if !base.bytes().all(|c|c.is_ascii_uppercase()||c.is_ascii_digit())||
    base.len()>30||base.is_empty()||after.cold_boot_unknown{
   return Err("unsafe_modeled_transition".into())
  }
  if !stmt(&self.db1,"SELECT seq FROM pending",&[])?.is_empty()||
    head(&self.db1)!=head(&self.db2){
   return Err("model_requires_drained_mirrored_source".into())
  }
  let decision_cause=match read_due(&self.db1)?{
   Some((source_seq,hashes))=>{
    if self.cold_unresolved_decision||before.is_none(){return Err("guarded_model_cold_or_missing_predecessor".into())}
    let (frame,proof,result_hash)=cause.ok_or("guarded_model_decision_proof_required")?;
    let index=self.receipts.len();
    let identity=decision_frame_identity(frame);
    if index>=hashes.len()||hashes[index]!=identity||
       self.expected_frame_hashes.get(index)!=Some(&identity)||
       proof["schema"]!="SPOTARB/R1184/MODELED-DECISION-CAUSE/V1"||
       proof["base"]!=base||proof["source_venue"]!=ven(frame.venue)||
       proof["source_epoch"]!=frame.epoch||
       proof["source_received_ns"]!=frame.received_ns||
       proof["source_received_wall_ms"]!=frame.received_wall_ms||
       proof["decision_rows"].as_array().map_or(true,|r|r.is_empty())||
       proof["decision_config_sha256"].as_str().map_or(true,|h|!is_sha256_hex(h))||
       proof["decision_config_source"].as_str().map_or(true,str::is_empty)||
       proof["metadata_generation"].as_u64().is_none()||
       proof.get("route").is_none()||
       proof.get("paper_model_assumptions").is_none()||
       proof["effective_decision_config"].as_object().is_none()||
       proof["modeled_before"]!=before.unwrap().snapshot()||
       proof.get("spot_book").is_none()||proof.get("futures_book").is_none()||
       !is_sha256_hex(result_hash)||proof.get("result_proof").is_none(){
     return Err("guarded_model_decision_cause_mismatch".into())
    }
    let policy_bytes=serde_json::to_vec(&proof["effective_decision_config"])
      .map_err(|_|"effective_decision_config_encode_failed")?;
    if hex(&sha256(&policy_bytes))!=proof["decision_config_sha256"]{
     return Err("guarded_model_effective_policy_hash_mismatch".into())
    }
    let result_proof=&proof["result_proof"];
    let result_bytes=serde_json::to_vec(result_proof).map_err(|_|"result_proof_encode_failed")?;
    if hex(&sha256(&result_bytes))!=result_hash||
       result_proof["schema"]!="SPOTARB/R1178/DECISION-RESULT/V1"||
       result_proof["decision_rows"]!=proof["decision_rows"]||
       result_proof["source_venue"]!=format!("{:?}",frame.venue)||
       result_proof["decision_time_ns"]!=proof["decision_time_ns"]||
       result_proof["decision_time_wall_ms"]!=proof["decision_time_wall_ms"]||
       result_proof["source_epoch"]!=frame.epoch||
       result_proof["source_received_ns"]!=frame.received_ns||
       result_proof["source_received_wall_ms"]!=frame.received_wall_ms||
       result_proof["modeled_after"]!=after.snapshot(){
      return Err("guarded_model_result_proof_mismatch".into())
    }
    let proof_json=serde_json::to_string(proof).map_err(|_|"decision_cause_encode_failed")?;
    if proof_json.len()>100_000{return Err("decision_cause_oversize".into())}
    let decision_id=hash(&format!("{source_seq}:{index}:{identity}"));
    Some(serde_json::json!({"decision_id":decision_id,"source_seq":source_seq,
      "source_ordinal":index,"source_identity":identity,
      "proof_hash":hash(&proof_json),"result_hash":result_hash,"proof":proof}))
   }
   None=>{
    if cause.is_some(){return Err("orphan_modeled_decision_cause".into())}
    None
   }
  };
  let old_json=before.map(|b|serde_json::to_string(&durable_portfolio(b)))
    .transpose().map_err(|_|"before_portfolio_encode_failed")?;
  let old_hash=old_json.as_ref().map(|s|hash(s))
    .unwrap_or_else(||ZERO.to_owned());
  let previous=one(&self.db1,
    "SELECT body_hash FROM portfolio_state WHERE id=1",&[])?;
  if previous.as_ref().map(|x|x[0].as_str()).unwrap_or(ZERO)!=old_hash{
   return Err("modeled_predecessor_denied".into())
  }
  if before.is_none()&&(!after.positions.is_empty()||
    after.stats.entries!=0||after.stats.futures_hedges!=0||
    after.stats.futures_covers!=0||after.stats.closed!=0){
   return Err("nonfresh_modeled_baseline_denied".into())
  }
  let after_json=serde_json::to_string(&durable_portfolio(after))
   .map_err(|_|"modeled_after_encode_failed")?;
  let after_hash=hash(&after_json);
  if before.is_some()&&after_hash==old_hash{
   return Err("unchanged_modeled_portfolio_denied".into())
  }
  let h=head(&self.db1)?;
  let seq=h.0.checked_add(1).ok_or("modeled_seq_overflow")?;
  let kind=if let Some(prior)=before{
    validate_modeled_leg(prior,after,base)?
  }else{"PORTFOLIO_BASELINE"};
  let mut event_body=serde_json::json!({
   "paper_only":true,"real_fills_attested":false,
   "kind":kind,"base":base,"source_ledger_predecessor":h.0,
   "before":old_json,"before_hash":old_hash,
   "after":after_json,"after_hash":after_hash
  });
  if let Some(cause)=decision_cause{event_body["decision_cause"]=cause;}

  let event_json=serde_json::to_string(&event_body)
    .map_err(|_|"modeled_event_encode_failed")?;
  let audit=Audit{
   id:format!("R1175MODELED{seq}"),kind:kind.into(),
   hash:hash(&event_json),body:event_json,
  };
  let batch=Batch{ingress_receipts:Vec::new(),ingress_reclaim:Vec::new(),schema:SCHEMA.into(),seq,
   count:h.1,n:0,prev:h.2.clone(),
   chain:hash(&(h.2+&audit.hash)),changes:Vec::new(),
   resets:Vec::new(),audit:vec![audit],
   portfolio:Some(PortfolioWrite{previous_hash:old_hash,
      after_hash,after_json,source_seq:h.0}),
   decision_due:None,decision_inputs:None,decision_seal:None};
  let body=serde_json::to_string(&batch)
    .map_err(|_|"modeled_batch_encode_failed")?;
  if body.len()>1_500_000{return Err("modeled_receipt_oversize".into())}
  transaction(&self.db1,||{
   apply(&self.db1,&batch)?;
   stmt(&self.db1,"INSERT INTO pending VALUES(?,?,?)",&[
    V::Num(to_i(seq)?),V::Str(body.clone()),V::Str(hash(&body))])?;
   Ok(())
  })?;
  if inject==Inject::AfterDb1{return Err("injected_modeled_after_db1".into())}
  self.ack_pending(if inject==Inject::AfterDb2{Inject::AfterDb2}else{Inject::None})?;
  if let Some((_,_,result_hash))=cause{
   self.modeled_receipt_binding=Some((self.receipts.len(),result_hash.to_owned()));
  }
  Ok(seq)
 }
 pub fn quarantined_decision_inputs(&self)->Result<Vec<RawFrame>,String>{
  // Forensic only: never grant a replay or trade permit from these bytes.
  self.verify_pair()?;
  if !stmt(&self.db1,"SELECT seq FROM pending",&[])?.is_empty(){
   return Err("unacknowledged_inbox_denied".into())
  }
  let Some((_,hashes))=read_due(&self.db1)? else{return Ok(Vec::new())};
  let raw=one(&self.db1,"SELECT inputs_json FROM decision_due WHERE id=1",&[])?
    .ok_or("decision_inputs_missing")?;
  let frames:Vec<RawFrame>=serde_json::from_str(&raw[0])
    .map_err(|_|"bad_decision_inputs")?;
  if frames.len()!=hashes.len()||frames.iter().zip(&hashes).any(|(f,h)|decision_frame_identity(f)!=*h){
   return Err("decision_inputs_identity_mismatch".into())
  }
  Ok(frames)
 }
 pub fn quarantined_modeled_state(&self)->Result<Option<PaperPortfolio>,String>{
  // Never expose even a quarantined recovery candidate if DB2 differs
  // or the source has an unacknowledged DB1 receipt.
  self.verify_pair()?;
  self.require_decisions_drained()?;
  if !stmt(&self.db1,"SELECT seq FROM pending",&[])?.is_empty(){
   return Err("unresolved_source_receipt_on_cold_restore".into())
  }
  let row=one(&self.db1,
   "SELECT body,body_hash FROM portfolio_state WHERE id=1",&[])?;
  match row{
   None=>Ok(None),
   Some(x)=>{
    if hash(&x[0])!=x[1]{return Err("modeled_hash_corruption".into())}
    let model:PaperPortfolio=serde_json::from_str(&x[0])
      .map_err(|_|"invalid_modeled_snapshot")?;
    if !model.cold_boot_unknown{
     return Err("model_recovery_not_quarantined".into())
    }
    Ok(Some(model))
   }
  }
 }
 pub fn cursor(&self)->Result<(u64,u64,String),String>{head(&self.db1)}
 pub fn source_commit(
  &mut self,owners:&mut [Owner],
  frames:&[(RawFrame,Update,usize)],witness:&[PaperWitness],
  inject:Inject
 )->Result<u64,String>{
  self.source_commit_inner(owners,frames,witness,inject,false)
 }
 pub fn source_commit_guarded(
  &mut self,owners:&mut [Owner],
  frames:&[(RawFrame,Update,usize)],witness:&[PaperWitness],
  inject:Inject
 )->Result<u64,String>{
  self.source_commit_inner(owners,frames,witness,inject,true)
 }
 fn source_commit_inner(
  &mut self,owners:&mut [Owner],
  frames:&[(RawFrame,Update,usize)],witness:&[PaperWitness],
  inject:Inject,guarded:bool
 )->Result<u64,String>{
  if !guarded&&frames.iter().any(|(f,_,_)|f.source.is_some()){return Err("ingress_requires_guarded_source_commit".into())}
  if read_due(&self.db1)?.is_some()||!self.receipts.is_empty()||
     !self.expected_frame_hashes.is_empty(){
   return Err("unresolved_decision_obligation_before_source".into())
  }
  if frames.is_empty()||frames.len()>32||owners.len()!=12{
   return Err("bad_owner_count_or_batch".into())
  }
  if !stmt(&self.db1,"SELECT seq FROM pending",&[])?.is_empty(){
   return Err("unacknowledged_pending_batch".into())
  }
  if head(&self.db1)!=head(&self.db2){
   return Err("db2_catchup_required".into())
  }
  let mut staged=BTreeMap::<usize,Owner>::new();
  let mut validated=Vec::with_capacity(frames.len());
  for (frame,update,index) in frames{
   let current=owners.get(*index).ok_or("wrong_owner_shard")?;
   let cand=staged.entry(*index).or_insert_with(||current.clone());
   let v=cand.prepare_parsed(frame,update.clone())
     .map_err(|e|format!("owner_prepare_failed:{e:?}:base={}:venue={:?}:epoch={}:seq={:?}:exchange_ts={:?}:received_ns={}:source={}",update.base,frame.venue,frame.epoch,update.book.sequence,update.book.exchange_ts_ms,frame.received_ns,frame.source.as_ref().map(|s|s.session_id.as_str()).unwrap_or("none")))?;
   cand.ingest_parsed(frame,v.clone(),&mut crate::history::PreparedSignal)
     .map_err(|e|format!("owner_staging_failed:{e:?}"))?;
   validated.push(v);
  }
  let mut b=make_batch(frames,&validated,head(&self.db1)?,witness)?;
  b.ingress_receipts=frames.iter().filter_map(|(f,_,_)|f.source.as_ref().map(|p|IngressReceipt{
    journal:p.journal_id.clone(),id:p.accepted_id,raw_hash:p.payload_sha256.clone(),frame_hash:decision_frame_identity(f)})).collect();
  if guarded {
   b.decision_due=Some(frames.iter().map(|(f,_,_)|decision_frame_identity(f)).collect());
   b.decision_inputs=Some(frames.iter().map(|(f,_,_)|f.clone()).collect());
  }
  let body=serde_json::to_string(&b).map_err(|_|"batch_encode_failed")?;
  if body.len()>1_500_000{return Err("compact_batch_size_denied".into())}
  #[cfg(test)]
  let r1183_start=std::time::Instant::now();
  let r1325_started=std::time::Instant::now();
  let r1325_result=transaction(&self.db1,||{
   apply(&self.db1,&b)?;
   stmt(&self.db1,"INSERT INTO pending VALUES(?,?,?)",&[
    V::Num(to_i(b.seq)?),V::Str(body.clone()),V::Str(hash(&body))])?;
   Ok(())
  });
  crate::pipeline_diagnostics_r1325::record("DB1Source",r1325_started.elapsed().as_nanos().min(u64::MAX as u128) as u64,r1325_result.is_ok());
  r1325_result?;
  #[cfg(test)]
  r1183_probe::add(&r1183_probe::SRC_DB1_NS,r1183_start);
  // Same R1088 invariant: original owner publication only AFTER DB1 commit.
  for (idx,new_owner) in staged{owners[idx]=new_owner}
  if inject==Inject::AfterDb1{
   return Err("injected_crash_after_db1".into())
  }
  self.ack_pending(if inject==Inject::AfterDb2{Inject::AfterDb2}else{Inject::None})?;
  // Cache only AFTER both DB1/DB2 and source ACK succeeded.
  // A cold-open writer starts empty and cannot seal old obligations.
  if guarded{
   self.expected_frame_hashes=b.decision_due.as_ref()
       .ok_or("guarded_source_due_missing")?.clone();
  }
  Ok(b.seq)
 }
 pub fn note_guarded_decision(
  &mut self,frame:&RawFrame,outcome:&str,result_hash:&str
 )->Result<Option<u64>,String>{
  if self.cold_unresolved_decision{
   return Err("cold_unresolved_decision_requires_independent_reconciliation".into())
  }
  if !valid_decision_outcome(outcome)||!is_sha256_hex(result_hash){
   return Err("invalid_decision_outcome_or_proof".into())
  }
  // No SQLite SELECT/JSON parsing for each of the up to 32 frames.
  // seal_decision_batch still independently re-reads and validates
  // the full persisted raw inbox and source identities.
  let hashes=&self.expected_frame_hashes;
  if hashes.is_empty(){return Err("decision_receipt_without_local_ack".into())}
  let index=self.receipts.len();
  if index>=hashes.len()||decision_frame_identity(frame)!=hashes[index]{
   return Err("decision_frame_identity_or_order_mismatch".into())
  }
  if self.modeled_receipt_binding.as_ref().is_some_and(|(i,h)|*i==index&&h!=result_hash){
   return Err("modeled_receipt_result_mismatch".into())
  }
  self.receipts.push(DecisionReceipt{
   input_hash:decision_frame_identity(frame),outcome:outcome.into(),
   result_hash:result_hash.into()});
  if self.modeled_receipt_binding.as_ref().is_some_and(|(i,_)|*i==index){
   self.modeled_receipt_binding=None;
  }
  if self.receipts.len()!=hashes.len(){return Ok(None)}
  let receipts=self.receipts.clone();
  let seq=self.seal_decision_batch(&receipts,Inject::None)?;
  self.receipts.clear();
  self.expected_frame_hashes.clear();
  Ok(Some(seq))
 }
 fn seal_decision_batch(
  &mut self,receipts:&[DecisionReceipt],inject:Inject
 )->Result<u64,String>{
  let (_,hashes)=read_due(&self.db1)?.ok_or("decision_due_missing")?;
  if receipts.len()!=hashes.len()||
     !receipts.iter().zip(&hashes).all(|(r,h)|
        r.input_hash==*h&&valid_decision_outcome(&r.outcome)&&
         is_sha256_hex(&r.result_hash)){
   return Err("decision_receipts_incomplete".into())
  }
  if !stmt(&self.db1,"SELECT seq FROM pending",&[])?.is_empty()||
      head(&self.db1)!=head(&self.db2){
   return Err("unacknowledged_source_before_seal".into())
  }
  let h=head(&self.db1)?;
  let seq=h.0.checked_add(1).ok_or("seal_seq_overflow")?;
  let proof=serde_json::to_string(receipts).map_err(|_|"seal_json")?;
  let b=Batch{ingress_receipts:Vec::new(),ingress_reclaim:Vec::new(),schema:SCHEMA.into(),seq,
   count:h.1,n:0,prev:h.2.clone(),
   chain:hash(&(h.2+&hash(&proof))),
   changes:Vec::new(),resets:Vec::new(),audit:Vec::new(),
   portfolio:None,decision_due:None,decision_inputs:None,
   decision_seal:Some(receipts.to_vec())};
  let body=serde_json::to_string(&b).map_err(|_|"seal_body_failed")?;
  #[cfg(test)]
  let r1183_start=std::time::Instant::now();
  let r1325_started=std::time::Instant::now();
  let r1325_result=transaction(&self.db1,||{
   apply(&self.db1,&b)?;
   stmt(&self.db1,"INSERT INTO pending VALUES(?,?,?)",&[
    V::Num(to_i(seq)?),V::Str(body.clone()),V::Str(hash(&body))])?;
   Ok(())
  });
  crate::pipeline_diagnostics_r1325::record("DB1Decision",r1325_started.elapsed().as_nanos().min(u64::MAX as u128) as u64,r1325_result.is_ok());
  r1325_result?;
  #[cfg(test)]
  r1183_probe::add(&r1183_probe::SEAL_DB1_NS,r1183_start);
  if inject==Inject::AfterDb1{return Err("injected_seal_after_db1".into())}
  self.ack_pending(if inject==Inject::AfterDb2{Inject::AfterDb2}else{Inject::None})?;
  Ok(seq)
 }
 /// Archives a checksum-verified R1289 source record as a distinct recovery
 /// obligation. This does not attest risk admission, create a TRADE_INTENT,
 /// acknowledge an exchange order, or make the exposure safe to trade.
 pub fn record_legacy_execution_recovery_r1326(
  &mut self,owner:&crate::native_paper_execution_r1289::DurablePaper,
  sequence:u64,source_record:serde_json::Value,inject:Inject
 )->Result<u64,String>{
  if sequence==0{return Err("R1326_LEGACY_SOURCE_SEQUENCE".into())}
  let verified=owner.r1326_records_after(sequence-1).map_err(|_|"R1326_LEGACY_SOURCE_JOURNAL_INVALID")?;
  if !verified.iter().any(|(actual_sequence,actual_record)|*actual_sequence==sequence&&actual_record==&source_record){
   return Err("R1326_LEGACY_SOURCE_RECORD_NOT_VERIFIED".into())
  }
  let journal=owner.journal_path();
  let metadata=fs::metadata(journal).map_err(|_|"R1326_LEGACY_JOURNAL_MISSING")?;
  if metadata.len()>64*1024*1024{return Err("R1326_LEGACY_JOURNAL_BOUND".into())}
  let raw=fs::read(journal).map_err(|_|"R1326_LEGACY_JOURNAL_READ")?;
  let mut prefix_len=0usize;
  for line in raw.split_inclusive(|byte|*byte==b'\n').take(usize::try_from(sequence).unwrap_or(usize::MAX)){
   prefix_len=prefix_len.checked_add(line.len()).ok_or("R1326_LEGACY_PREFIX_BOUND")?;
  }
  if prefix_len==0||raw[..prefix_len].iter().filter(|byte|**byte==b'\n').count()<sequence as usize{
   return Err("R1326_LEGACY_SOURCE_SEQUENCE_NOT_IN_JOURNAL".into())
  }
  if !legacy_source_record_has_identity_and_order(&source_record){
   return Err("R1326_LEGACY_SOURCE_RECORD".into())
  }
  let record_json=serde_json::to_string(&source_record).map_err(|_|"R1326_LEGACY_SOURCE_ENCODE")?;
  let source_journal_sha256=hash(std::str::from_utf8(&raw[..prefix_len]).map_err(|_|"R1326_LEGACY_JOURNAL_UTF8")?);
  let source_record_sha256=hash(&record_json);
  let body=serde_json::json!({
   "schema":"SPOTARB/R1326/LEGACY-RECOVERY/V1",
   "status":"RECONCILIATION_REQUIRED",
   "paper_only":true,
   "real_fills_attested":false,
   "evidence":{
    "source_kind":"R1289_EXECUTION_JOURNAL",
    "source_journal_path":journal.to_string_lossy(),
    "source_journal_sha256":source_journal_sha256,
    "source_sequence":sequence,
    "source_record_sha256":source_record_sha256,
    "source_record":source_record
   }
  });
  let id=format!("R1326L{}",hash(&format!("{}:{sequence}:{source_record_sha256}",body["evidence"]["source_journal_sha256"].as_str().unwrap_or_default())));
  self.commit_legacy_recovery_witness_r1326(PaperWitness{id,kind:"LEGACY_RECOVERY".into(),body},inject)
 }
 fn commit_legacy_recovery_witness_r1326(&mut self,witness:PaperWitness,inject:Inject)->Result<u64,String>{
  let value:&serde_json::Value=&witness.body;
  if witness.kind!="LEGACY_RECOVERY"||value["schema"]!="SPOTARB/R1326/LEGACY-RECOVERY/V1"||
     value["status"]!="RECONCILIATION_REQUIRED"||value["paper_only"]!=true||
     value["real_fills_attested"]!=false{
   return Err("R1326_LEGACY_RECOVERY_KIND_OR_SCHEMA".into())
  }
  let evidence=&value["evidence"];
  let seq_src=evidence["source_sequence"].as_u64().filter(|n|*n>0).ok_or("R1326_LEGACY_SOURCE_SEQUENCE")?;
  let journal_hash=evidence["source_journal_sha256"].as_str().filter(|v|v.len()==64).ok_or("R1326_LEGACY_JOURNAL_HASH")?;
  let record=&evidence["source_record"];
  let record_json=serde_json::to_string(record).map_err(|_|"R1326_LEGACY_RECORD_ENCODE")?;
  let record_hash=hash(&record_json);
  if evidence["source_record_sha256"]!=record_hash||
     !legacy_source_record_has_identity_and_order(record)||
     witness.id!=format!("R1326L{}",hash(&format!("{journal_hash}:{seq_src}:{record_hash}"))){
   return Err("R1326_LEGACY_RECOVERY_PROVENANCE".into())
  }
  let body=serde_json::to_string(value).map_err(|_|"R1326_LEGACY_RECOVERY_ENCODE")?;
  let body_hash=hash(&body);
  if self.cold_unresolved_decision{return Err("R1326_LEGACY_DECISION_RECOVERY_REQUIRED".into())}
  if !stmt(&self.db1,"SELECT seq FROM pending",&[])?.is_empty(){self.ack_pending(Inject::None)?;}
  self.require_decisions_drained()?;
  if head(&self.db1)?!=head(&self.db2)?{return Err("R1326_DB_PAIR_DIVERGENCE".into())}
  if let Some(existing)=one(&self.db1,"SELECT kind,seq,body,body_hash FROM audit WHERE id=?",&[V::Str(witness.id.clone())])?{
   if existing[0]!="LEGACY_RECOVERY"||existing[2]!=body||existing[3]!=body_hash||
      one(&self.db2,"SELECT kind,seq,body,body_hash FROM audit WHERE id=?",&[V::Str(witness.id)])?!=Some(existing.clone()){
    return Err("R1326_LEGACY_RECOVERY_REPLAY_DIVERGENCE".into())
   }
   return existing[1].parse().map_err(|_|"R1326_LEGACY_RECOVERY_SEQUENCE".into())
  }
  let h=head(&self.db1)?;let seq=h.0.checked_add(1).ok_or("R1326_LEGACY_RECOVERY_SEQUENCE_OVERFLOW")?;
  let audit=Audit{id:witness.id,kind:"LEGACY_RECOVERY".into(),body,hash:body_hash.clone()};
  let batch=Batch{ingress_receipts:Vec::new(),ingress_reclaim:Vec::new(),schema:SCHEMA.into(),seq,count:h.1,n:0,
   prev:h.2.clone(),chain:hash(&(h.2+&body_hash)),changes:Vec::new(),resets:Vec::new(),audit:vec![audit],
   portfolio:None,decision_due:None,decision_inputs:None,decision_seal:None};
  let encoded=serde_json::to_string(&batch).map_err(|_|"R1326_LEGACY_RECOVERY_BATCH")?;
  transaction(&self.db1,||{apply(&self.db1,&batch)?;stmt(&self.db1,"INSERT INTO pending VALUES(?,?,?)",&[
   V::Num(to_i(seq)?),V::Str(encoded.clone()),V::Str(hash(&encoded))])?;Ok(())})?;
  if inject==Inject::AfterDb1{return Err("injected_legacy_recovery_after_db1".into())}
  self.ack_pending(if inject==Inject::AfterDb2{Inject::AfterDb2}else{Inject::None})?;
  self.verify_pair()?;Ok(seq)
 }
 pub fn legacy_recovery_count_r1326(&self)->Result<u64,String>{
  self.verify_pair()?;
  let rows=stmt(&self.db1,"SELECT COUNT(*) FROM audit WHERE kind='LEGACY_RECOVERY'",&[])?;
  rows.first().and_then(|r|r.first()).ok_or("legacy recovery count")?.parse().map_err(|_|"legacy recovery count".into())
 }
 /// Durably records one admitted trade identity as a canonical DB1 event,
 /// mirrors it through the existing DB2 transaction protocol, and returns only
 /// after the existing pair verification/ACK path succeeds. Execution outbox
 /// remains separate and must commit its first order intent before dispatch.
 pub fn commit_trade_intent_r1326(
  &mut self,witness:PaperWitness,inject:Inject
 )->Result<u64,String>{
  if witness.kind!="TRADE_INTENT"||witness.id.len()>96||
     !witness.id.bytes().all(|b|b.is_ascii_alphanumeric()){
   return Err("R1326_TRADE_INTENT_KIND_OR_ID".into())
  }
  let value:serde_json::Value=serde_json::from_value(witness.body.clone())
   .map_err(|_|"R1326_TRADE_INTENT_BODY")?;
  let trade_id=value["trade_id"].as_str().ok_or("R1326_TRADE_ID")?;
  if witness.id!=format!("R1326T{}",hash(trade_id))||
     value["schema"]!="SPOTARB/R1326/TRADE-INTENT/V1"||
     value["risk_passed"]!=true||value["paper_only"]!=true||
     value["real_fills_attested"]!=false||
     value["signal_evidence"].as_object().is_none()||
     value["risk_decision"].as_object().is_none()||
     value["generation"].as_u64().unwrap_or(0)==0{
   return Err("R1326_TRADE_INTENT_EVIDENCE".into())
  }
  if self.cold_unresolved_decision{return Err("R1326_LEGACY_DECISION_RECOVERY_REQUIRED".into())}
  if !stmt(&self.db1,"SELECT seq FROM pending",&[])?.is_empty(){
   self.ack_pending(Inject::None)?;
  }
  self.require_decisions_drained()?;
  if head(&self.db1)?!=head(&self.db2)?{return Err("R1326_DB_PAIR_DIVERGENCE".into())}
  let body=serde_json::to_string(&value).map_err(|_|"R1326_TRADE_INTENT_ENCODE")?;
  let audit_hash=hash(&body);
  let id=witness.id.clone();
  if let Some(existing)=one(&self.db1,"SELECT kind,seq,body,body_hash FROM audit WHERE id=?",&[V::Str(id.clone())])?{
   if existing[0]!="TRADE_INTENT"||existing[2]!=body||existing[3]!=audit_hash{
    return Err("R1326_TRADE_ID_COLLISION".into())
   }
   if one(&self.db2,"SELECT kind,seq,body,body_hash FROM audit WHERE id=?",&[V::Str(id)])?!=Some(existing.clone()){
    return Err("R1326_TRADE_INTENT_MIRROR_DIVERGENCE".into())
   }
   return Ok(existing[1].parse().map_err(|_|"R1326_TRADE_INTENT_SEQUENCE".to_string())?)
  }
  let h=head(&self.db1)?;
  let seq=h.0.checked_add(1).ok_or("R1326_TRADE_INTENT_SEQUENCE_OVERFLOW")?;
  let audit=Audit{id,kind:"TRADE_INTENT".into(),body,hash:audit_hash.clone()};
  let b=Batch{
   ingress_receipts:Vec::new(),ingress_reclaim:Vec::new(),schema:SCHEMA.into(),
   seq,count:h.1,n:0,prev:h.2.clone(),chain:hash(&(h.2+&audit_hash)),
   changes:Vec::new(),resets:Vec::new(),audit:vec![audit],portfolio:None,
   decision_due:None,decision_inputs:None,decision_seal:None
  };
  let encoded=serde_json::to_string(&b).map_err(|_|"R1326_TRADE_INTENT_BATCH")?;
  transaction(&self.db1,||{
   apply(&self.db1,&b)?;
   stmt(&self.db1,"INSERT INTO pending VALUES(?,?,?)",&[
    V::Num(to_i(seq)?),V::Str(encoded.clone()),V::Str(hash(&encoded))])?;
   Ok(())
  })?;
  if inject==Inject::AfterDb1{return Err("injected_trade_intent_after_db1".into())}
  self.ack_pending(if inject==Inject::AfterDb2{Inject::AfterDb2}else{Inject::None})?;
  self.verify_pair()?;
  Ok(seq)
 }
 pub fn commit_trade_event_r1326(
  &mut self,witness:PaperWitness,inject:Inject
 )->Result<u64,String>{
  #[cfg(feature="offline-test-clock")]
  R1327_BEFORE_TRADE_EVENT_HOOK.with(|slot|{if let Some(hook)=slot.borrow_mut().take(){hook();}});
  if witness.kind!="TRADE_EVENT"||witness.id.len()>96||
     !witness.id.bytes().all(|b|b.is_ascii_alphanumeric()){
   return Err("R1326_TRADE_EVENT_KIND_OR_ID".into())
  }
  let value:serde_json::Value=serde_json::from_value(witness.body.clone())
   .map_err(|_|"R1326_TRADE_EVENT_BODY")?;
  let trade_id=value["trade_id"].as_str().ok_or("R1326_TRADE_EVENT_ID")?;
  let event_id=value["event_id"].as_str().ok_or("R1326_TRADE_EVENT_ID")?;
  if value["schema"]!="SPOTARB/R1326/TRADE-EVENT/V1"||
     trade_id.is_empty()||event_id.is_empty()||
     witness.id!=format!("R1326E{}",hash(&format!("{trade_id}:{event_id}")))||
     value["event"].as_object().is_none(){
   return Err("R1326_TRADE_EVENT_EVIDENCE".into())
  }
  let intent_id=format!("R1326T{}",hash(trade_id));
  let intent=one(&self.db1,"SELECT kind,body FROM audit WHERE id=?",&[V::Str(intent_id.clone())])?;
  let mirror_intent=one(&self.db2,"SELECT kind,body FROM audit WHERE id=?",&[V::Str(intent_id)])?;
  if intent!=mirror_intent||intent.as_ref().map(|r|r[0].as_str())!=Some("TRADE_INTENT"){
   return Err("R1326_TRADE_EVENT_WITHOUT_INTENT".into())
  }
  if self.cold_unresolved_decision{return Err("R1326_LEGACY_DECISION_RECOVERY_REQUIRED".into())}
  if !stmt(&self.db1,"SELECT seq FROM pending",&[])?.is_empty(){self.ack_pending(Inject::None)?;}
  self.require_decisions_drained()?;
  if head(&self.db1)?!=head(&self.db2)?{return Err("R1326_DB_PAIR_DIVERGENCE".into())}
  let body=serde_json::to_string(&value).map_err(|_|"R1326_TRADE_EVENT_ENCODE")?;
  let body_hash=hash(&body);let id=witness.id.clone();
  if let Some(existing)=one(&self.db1,"SELECT kind,seq,body,body_hash FROM audit WHERE id=?",&[V::Str(id.clone())])?{
   if existing[0]!="TRADE_EVENT"||existing[2]!=body||existing[3]!=body_hash||
      one(&self.db2,"SELECT kind,seq,body,body_hash FROM audit WHERE id=?",&[V::Str(id)])?!=Some(existing.clone()){
    return Err("R1326_TRADE_EVENT_REPLAY_DIVERGENCE".into())
   }
   return Ok(existing[1].parse().map_err(|_|"R1326_TRADE_EVENT_SEQUENCE".to_string())?)
  }
  let h=head(&self.db1)?;let seq=h.0.checked_add(1).ok_or("R1326_TRADE_EVENT_SEQUENCE_OVERFLOW")?;
  let audit=Audit{id,kind:"TRADE_EVENT".into(),body,hash:body_hash.clone()};
  let b=Batch{ingress_receipts:Vec::new(),ingress_reclaim:Vec::new(),schema:SCHEMA.into(),seq,
   count:h.1,n:0,prev:h.2.clone(),chain:hash(&(h.2+&body_hash)),changes:Vec::new(),
   resets:Vec::new(),audit:vec![audit],portfolio:None,decision_due:None,
   decision_inputs:None,decision_seal:None};
  let encoded=serde_json::to_string(&b).map_err(|_|"R1326_TRADE_EVENT_BATCH")?;
  transaction(&self.db1,||{apply(&self.db1,&b)?;stmt(&self.db1,"INSERT INTO pending VALUES(?,?,?)",&[
   V::Num(to_i(seq)?),V::Str(encoded.clone()),V::Str(hash(&encoded))])?;Ok(())})?;
  if inject==Inject::AfterDb1{return Err("injected_trade_event_after_db1".into())}
  self.ack_pending(if inject==Inject::AfterDb2{Inject::AfterDb2}else{Inject::None})?;
  self.verify_pair()?;Ok(seq)
 }
 pub fn require_decisions_drained(&self)->Result<(),String>{
  if read_due(&self.db1)?.is_some()||read_due(&self.db2)?.is_some()||
     !self.receipts.is_empty()||!self.expected_frame_hashes.is_empty()||
     self.modeled_receipt_binding.is_some(){
   return Err("unresolved_decision_obligation_fail_closed".into())
  }
  Ok(())
 }
 pub fn ack_pending(&mut self,inject:Inject)->Result<(),String>{
  let pending=stmt(&self.db1,"SELECT seq,body,body_hash FROM pending",&[])?;
  if pending.len()!=1{return Err("missing_or_multiple_pending".into())}
  let r=&pending[0];
  if hash(&r[1])!=r[2]{return Err("source_pending_hash_corrupted".into())}
  let b:Batch=serde_json::from_str(&r[1]).map_err(|_|"pending_json_bad")?;
  if b.seq.to_string()!=r[0]||b.schema!=SCHEMA||
    head(&self.db1)?!=(b.seq,b.count,b.chain.clone()){
   return Err("source_pending_cursor_mismatch".into())
  }
  let cursor=head(&self.db2)?;
  if cursor.0+1==b.seq{
   #[cfg(test)]
   let r1183_start=std::time::Instant::now();
   let r1325_started=std::time::Instant::now();
   let r1325_result=transaction(&self.db2,||apply(&self.db2,&b));
   crate::pipeline_diagnostics_r1325::record("DB2",r1325_started.elapsed().as_nanos().min(u64::MAX as u128) as u64,r1325_result.is_ok());
   r1325_result?;
   #[cfg(test)]
   r1183_probe::add(if b.decision_seal.is_some(){
     &r1183_probe::SEAL_DB2_NS
   }else{&r1183_probe::SRC_DB2_NS},r1183_start);
  }else if cursor.0!=b.seq{
   return Err("db2_replay_gap".into())
  }
  if head(&self.db1)!=head(&self.db2){
   return Err("db1_db2_cursor_divergence".into())
  }
  verify_changed(&self.db1,&b)?;
  verify_changed(&self.db2,&b)?;
  if inject==Inject::AfterDb2{
   return Err("injected_crash_after_db2".into())
  }
  #[cfg(test)]
  let r1183_start=std::time::Instant::now();
  let r1325_started=std::time::Instant::now();
  let r1325_result=transaction(&self.db1,||{
   stmt(&self.db1,"DELETE FROM pending WHERE seq=?",&[
    V::Num(to_i(b.seq)?)])?;
   Ok(())
  });
  crate::pipeline_diagnostics_r1325::record("ACK",r1325_started.elapsed().as_nanos().min(u64::MAX as u128) as u64,r1325_result.is_ok());
  r1325_result?;
  #[cfg(test)]
  r1183_probe::add(if b.decision_seal.is_some(){
    &r1183_probe::SEAL_ACK_NS
  }else{&r1183_probe::SRC_ACK_NS},r1183_start);
  Ok(())
 }

 pub fn recover(&mut self)->Result<(u64,u64,String),String>{
  let pending=stmt(&self.db1,"SELECT seq FROM pending",&[])?;
  if pending.len()>1{return Err("pending_batch_multiplicity".into())}
  if pending.len()==1{self.ack_pending(Inject::None)?}
  self.verify_pair()?;
  self.require_decisions_drained()?;
  head(&self.db1)
 }
 pub fn trade_intent_context_r1326(&self,trade_id:&str)->Result<Option<(String,u64,u64,u64,String)>,String>{
  if trade_id.is_empty()||trade_id.len()>128{return Err("R1326_TRADE_ID".into())}
  self.verify_pair()?;let id=format!("R1326T{}",hash(trade_id));
  let a=one(&self.db1,"SELECT kind,body FROM audit WHERE id=?",&[V::Str(id.clone())])?;
  let b=one(&self.db2,"SELECT kind,body FROM audit WHERE id=?",&[V::Str(id)])?;
  if a!=b{return Err("R1326_TRADE_INTENT_PAIR_MISMATCH".into())}
  let Some(row)=a else{return Ok(None)};
  if row[0]!="TRADE_INTENT"{return Err("R1326_TRADE_INTENT_KIND_MISMATCH".into())}
  let body:serde_json::Value=serde_json::from_str(&row[1]).map_err(|_|"R1326_TRADE_INTENT_BODY_INVALID")?;
  if body["trade_id"].as_str()!=Some(trade_id)||body["paper_only"]!=true||body["risk_passed"]!=true{return Err("R1326_TRADE_INTENT_PROVENANCE_INVALID".into())}
  let key=body["signal_evidence"]["execution_command_key"].as_str().filter(|v|!v.is_empty()).ok_or("R1326_TRADE_INTENT_COMMAND_KEY_MISSING")?.to_string();
  let metadata_generation=body["signal_evidence"]["generation"].as_u64().filter(|v|*v>0).ok_or("R1326_TRADE_INTENT_GENERATION_MISSING")?;
  let owner_generation=body["signal_evidence"]["owner_generation"].as_u64().filter(|v|*v>0).ok_or("R1326_TRADE_INTENT_OWNER_GENERATION_MISSING")?;
  let revision=body["signal_evidence"]["owner_revision"].as_u64().ok_or("R1326_TRADE_INTENT_OWNER_REVISION_MISSING")?;
  let decision_id=body["signal_evidence"]["decision_id"].as_str().filter(|v|!v.is_empty()).ok_or("R1326_TRADE_INTENT_DECISION_ID_MISSING")?.to_string();
  Ok(Some((key,metadata_generation,owner_generation,revision,decision_id)))
 }
 pub fn trade_intent_ids_r1326(&self)->Result<Vec<String>,String>{
  self.verify_pair()?;
  let a=stmt(&self.db1,"SELECT id,body FROM audit WHERE kind='TRADE_INTENT' ORDER BY id",&[])?;
  let b=stmt(&self.db2,"SELECT id,body FROM audit WHERE kind='TRADE_INTENT' ORDER BY id",&[])?;
  if a!=b{return Err("R1326_TRADE_INTENT_INVENTORY_DIVERGENCE".into())}
  let mut ids=Vec::new();let mut unique=std::collections::BTreeSet::new();
  for row in a{
   if row.len()!=2{return Err("R1326_TRADE_INTENT_INVENTORY_ROW".into())}
   let body:serde_json::Value=serde_json::from_str(&row[1]).map_err(|_|"R1326_TRADE_INTENT_INVENTORY_BODY")?;
   let trade_id=body["trade_id"].as_str().filter(|v|!v.is_empty()).ok_or("R1326_TRADE_INTENT_INVENTORY_ID")?;
   if row[0]!=format!("R1326T{}",hash(trade_id))||!unique.insert(trade_id.to_string()){
    return Err("R1326_TRADE_INTENT_INVENTORY_IDENTITY".into())
   }
   ids.push(trade_id.to_string());
  }
  Ok(ids)
 }
 pub fn trade_counts_r1326(&self)->Result<(u64,u64),String>{
  self.verify_pair()?;
  let intents=stmt(&self.db1,"SELECT COUNT(*) FROM audit WHERE kind='TRADE_INTENT'",&[])?;
  let events=stmt(&self.db1,"SELECT COUNT(*) FROM audit WHERE kind='TRADE_EVENT'",&[])?;
  let i=intents.first().and_then(|r|r.first()).ok_or("trade intent count")?.parse().map_err(|_|"trade intent count")?;
  let e=events.first().and_then(|r|r.first()).ok_or("trade event count")?.parse().map_err(|_|"trade event count")?;
  Ok((i,e))
 }
 pub fn verify_pair(&self)->Result<(),String>{
  if head(&self.db1)?!=head(&self.db2)?{
   return Err("unmirrored_source_control".into())
  }
  if read_due(&self.db1)?!=read_due(&self.db2)?{
   return Err("unmirrored_decision_obligation".into())
  }
  // read_due checks each checksum and frame identity, but omits the
  // original inbox bytes from its projection. Compare the exact mirror.
  if one(&self.db1,"SELECT inputs_json,inputs_sha256 FROM decision_due WHERE id=1",&[])?!=
     one(&self.db2,"SELECT inputs_json,inputs_sha256 FROM decision_due WHERE id=1",&[])?{
   return Err("unmirrored_decision_inputs".into())
  }
  if stmt(&self.db1,"SELECT * FROM decision_totals",&[])?!=
     stmt(&self.db2,"SELECT * FROM decision_totals",&[])?{
   return Err("unmirrored_decision_totals".into())
  }
  for (q,kind) in [
   ("SELECT venue,base,book,book_hash,last_seq FROM latest ORDER BY venue,base",0),
   ("SELECT id,kind,seq,body,body_hash FROM audit ORDER BY id",1),
   ("SELECT venue,base,epoch,source_seq FROM epochs ORDER BY venue,base",2),
   ("SELECT journal,id,raw_hash,frame_hash,source_seq FROM ingress_receipts ORDER BY journal,id",4),
   ("SELECT journal FROM ingress_origin ORDER BY id",4),
   ("SELECT id,body,body_hash,last_seq,source_seq FROM portfolio_state ORDER BY id",3)
  ]{
   let query=format!("{q} LIMIT ? OFFSET ?");
   let mut offset=0i64;
   loop{
    let args=[V::Num(256),V::Num(offset)];
    let a=stmt(&self.db1,&query,&args)?;
    let b=stmt(&self.db2,&query,&args)?;
    if a!=b{return Err("mirrored_content_diverged".into())}
    for r in &a{
     if kind==0&&hash(&r[2])!=r[3]{
      return Err("latest_book_hash_corrupt".into())
     }
     if kind==1&&hash(&r[3])!=r[4]{
      return Err("audit_hash_corrupt".into())
     }
     if kind==3&&hash(&r[1])!=r[2]{
      return Err("portfolio_hash_corrupt".into())
     }
    }
    if a.len()<256{break}
    offset=offset.checked_add(256).ok_or("audit_page_overflow")?;
   }
  }
  Ok(())
 }
 pub fn book_quarantined(&self,venue:Venue,base:&str)->Result<Option<Book>,String>{
  // An old checkpoint never grants an active source/epoch or a trade permit.
  let q=stmt(&self.db1,
    "SELECT book,book_hash FROM latest WHERE venue=? AND base=?",&[
     V::Str(ven(venue).into()),V::Str(base.into())])?;
  if q.len()>1{return Err("duplicate_latest".into())}
  if let Some(row)=q.first(){
   if hash(&row[0])!=row[1]{return Err("book_hash_corrupt".into())}
   Ok(Some(serde_json::from_str(&row[0]).map_err(|_|"book_decode_failed")?))
  }else{Ok(None)}
 }
 /// Recover only mirrored source custody. Unresolved decisions remain fail-closed.
 pub fn recover_ingress_source_only(&mut self)->Result<(),String>{
  let pending=stmt(&self.db1,"SELECT seq FROM pending",&[])?;
  if pending.len()>1{return Err("pending multiplicity".into())}
  if pending.len()==1{self.ack_pending(Inject::None)?;}
  self.verify_pair()
 }
 /// Cold automatic denial is permitted ONLY for an inbox with no modeled
 /// portfolio or non-reset audit. Existing PAPER recovery remains fail-closed.
 pub fn recover_ingress_observer(&mut self,inbox:&ingress_store_r1205::Store)->Result<(),String>{
  self.recover_ingress_source_only()?;
  if let Some((_,hashes))=read_due(&self.db1)?{
   if !stmt(&self.db1,"SELECT id FROM portfolio_state",&[])?.is_empty()||
      !stmt(&self.db1,"SELECT id FROM audit WHERE kind!='SOURCE_RESET'",&[])?.is_empty(){return Err("cold_modeled_state_requires_independent_reconciliation".into())}
   let inputs=self.quarantined_decision_inputs()?;
   if inputs.len()!=hashes.len()||inputs.iter().any(|f|!inbox.recognizes(f).unwrap_or(false)){return Err("cold_ingress_receipt_reconciliation_required".into())}
   for f in &inputs{let p=f.source.as_ref().ok_or("cold provenance")?;self.ingress_receipt(&p.journal_id,p.accepted_id,&p.payload_sha256)?.ok_or("cold mirrored receipt missing")?;}
   let receipts=hashes.into_iter().map(|h|DecisionReceipt{input_hash:h.clone(),outcome:"INPUT_REJECTED".into(),result_hash:hash(&format!("COLD_SOURCE_ONLY_NO_EXECUTION:{h}"))}).collect::<Vec<_>>();
   self.seal_decision_batch(&receipts,Inject::None)?;
   self.cold_unresolved_decision=false;self.receipts.clear();self.expected_frame_hashes.clear();
  }
  self.verify_pair()?;self.require_decisions_drained()
 }
 pub fn source_custody_counts(&self)->Result<serde_json::Value,String>{
  let a=head(&self.db1)?;let b=head(&self.db2)?;let pending=stmt(&self.db1,"SELECT body,body_hash FROM pending",&[])?;let mut outstanding=0u64;
  for row in &pending{if hash(&row[0])!=row[1]{return Err("pending checksum".into())}let batch:Batch=serde_json::from_str(&row[0]).map_err(|_|"pending schema")?;outstanding=outstanding.checked_add(batch.n).ok_or("pending count")?;}
  Ok(serde_json::json!({"db1_source_frames":a.1,"db2_source_frames":b.1,"acked_source_frames":a.1.checked_sub(outstanding).ok_or("ack count")?,"pending_source_batches":pending.len(),"decision_obligations_drained":self.require_decisions_drained().is_ok()}))
 }
 pub fn ingress_receipt_exists(&self,journal:&str,id:u64)->Result<bool,String>{
  self.verify_pair()?;
  if !stmt(&self.db1,"SELECT seq FROM pending",&[])?.is_empty(){return Err("receipt reclamation unacked".into())}
  Ok(!stmt(&self.db1,"SELECT id FROM ingress_receipts WHERE journal=? AND id=?",&[V::Str(journal.into()),V::Num(to_i(id)?)])?.is_empty())
 }
 pub fn sweep_ingress_receipts(&mut self,inbox:&mut ingress_store_r1205::Store)->Result<(),String>{
  let refs=inbox.reclaim_refs()?;if refs.is_empty(){return Ok(())}
  self.release_ingress_receipts(inbox,&refs)?;inbox.forget_reclaimed(self,&refs)
 }
 pub fn ingress_receipt(&self,journal:&str,id:u64,raw_hash:&str)->Result<Option<String>,String>{
  self.verify_pair()?;
  if !stmt(&self.db1,"SELECT seq FROM pending",&[])?.is_empty(){return Err("ingress_source_not_acked".into())}
  let a=stmt(&self.db1,"SELECT raw_hash,frame_hash,source_seq FROM ingress_receipts WHERE journal=? AND id=?",&[V::Str(journal.into()),V::Num(to_i(id)?)])?;
  if a.is_empty(){return Ok(None)}
  if a.len()!=1||a[0][0]!=raw_hash{return Err("ingress_receipt_binding".into())}
  Ok(Some(hash(&format!("{}:{}:{}:{}:{}",journal,id,a[0][0],a[0][1],a[0][2]))))
 }
 /// Fetch and seal canonical ACK proofs before taking the inbox Store mutex.
 pub fn verify_ingress_batch(&self,frames:&[RawFrame])->Result<VerifiedIngressBatch,String>{
  if frames.is_empty()||frames.len()>32{return Err("verified ingress batch bounds".into())}
  self.verify_pair()?;self.require_decisions_drained()?;
  if !stmt(&self.db1,"SELECT seq FROM pending",&[])?.is_empty(){return Err("ingress_source_not_acked".into())}
  let mut ids=std::collections::HashSet::new();let mut entries=Vec::with_capacity(frames.len());
  for f in frames{
   let p=f.source.as_ref().ok_or("R1205 source missing")?;
   if p.journal_id.is_empty()||p.accepted_id==0||!ids.insert((p.journal_id.clone(),p.accepted_id))||hash(&f.raw)!=p.payload_sha256{return Err("verified ingress provenance mismatch".into())}
   let row=stmt(&self.db1,"SELECT raw_hash,frame_hash,source_seq FROM ingress_receipts WHERE journal=? AND id=?",&[V::Str(p.journal_id.clone()),V::Num(to_i(p.accepted_id)?)])?;
   if row.len()!=1||row[0][0]!=p.payload_sha256{return Err("canonical ingress receipt missing or mismatched".into())}
   let source_seq=row[0][2].parse::<u64>().map_err(|_|"canonical ingress sequence")?;
   let proof=hash(&format!("{}:{}:{}:{}:{}",p.journal_id,p.accepted_id,row[0][0],row[0][1],row[0][2]));
   entries.push(VerifiedIngressReceipt{journal:p.journal_id.clone(),id:p.accepted_id,raw_hash:row[0][0].clone(),frame_hash:row[0][1].clone(),source_seq,proof});
  }
  Ok(VerifiedIngressBatch{root:self.root.clone(),head:head(&self.db1)?,entries})
 }
 /// Create a reclaim ticket only after the inbox's FULL terminal commit.
 pub fn prepare_ingress_release_after_publish(&self,verified:&VerifiedIngressBatch,committed:&DurableInboxPublish)->Result<Option<IngressReleaseTicket>,String>{
  if verified.entries.is_empty()||verified.root!=self.root||committed.root!=self.root||committed.journal!=verified.entries[0].journal||committed.entries!=verified.entries{return Err("durable ingress publish binding mismatch".into())}
  self.verify_pair()?;self.require_decisions_drained()?;
  if head(&self.db1)?!=verified.head||!stmt(&self.db1,"SELECT seq FROM pending",&[])?.is_empty(){return Err("canonical ingress head changed or unacked".into())}
  let mut refs=Vec::with_capacity(verified.entries.len());
  for e in &verified.entries{
   let row=stmt(&self.db1,"SELECT raw_hash,frame_hash,source_seq FROM ingress_receipts WHERE journal=? AND id=?",&[V::Str(e.journal.clone()),V::Num(to_i(e.id)?)])?;
   if row.len()!=1||row[0][0]!=e.raw_hash||row[0][1]!=e.frame_hash||row[0][2]!=e.source_seq.to_string()||e.proof!=hash(&format!("{}:{}:{}:{}:{}",e.journal,e.id,e.raw_hash,e.frame_hash,e.source_seq)){return Err("canonical ingress receipt changed".into())}
   refs.push((e.journal.clone(),e.id));
  }
  Ok(Some(IngressReleaseTicket{root:self.root.clone(),head:verified.head.clone(),refs}))
 }
 /// Verify that the ACKed canonical reclaim removed every source receipt.
 pub fn confirm_ingress_reclaimed(&self,committed:&DurableInboxPublish)->Result<AckedIngressReclaim,String>{
  if committed.root!=self.root{return Err("canonical reclaim root mismatch".into())}
  self.verify_pair()?;self.require_decisions_drained()?;
  if !stmt(&self.db1,"SELECT seq FROM pending",&[])?.is_empty(){return Err("canonical reclaim unacked".into())}
  for e in &committed.entries{
   let row=stmt(&self.db1,"SELECT raw_hash FROM ingress_receipts WHERE journal=? AND id=?",&[V::Str(e.journal.clone()),V::Num(to_i(e.id)?)])?;
   if !row.is_empty(){return Err("canonical receipt remains after reclaim ACK".into())}
  }
  Ok(AckedIngressReclaim{root:self.root.clone(),journal:committed.journal.clone(),entries:committed.entries.clone()})
 }
 /// Only call after the corresponding inbox outcomes have committed durably.
 pub fn prepare_ingress_release(&self,inbox:&ingress_store_r1205::Store,refs:&[(String,u64)])->Result<Option<IngressReleaseTicket>,String>{
  if refs.is_empty(){return Ok(None)}
  self.verify_pair()?;self.require_decisions_drained()?;
  if refs.len()>1024{return Err("ingress_reclaim_bounds".into())}
  for (journal,id) in refs{
    let row=stmt(&self.db1,"SELECT raw_hash FROM ingress_receipts WHERE journal=? AND id=?",&[V::Str(journal.clone()),V::Num(to_i(*id)?)])?;
    if let Some(row)=row.first(){let proof=self.ingress_receipt(journal,*id,&row[0])?.ok_or("missing receipt")?;
      if !inbox.published_proof(journal,*id,&proof)?{return Err("ingress_outcome_not_durable".into())}
    }
  }
  Ok(Some(IngressReleaseTicket{root:self.root.clone(),head:head(&self.db1)?,refs:refs.to_vec()}))
 }
 /// Canonical I/O runs after dropping the inbox mutex; immutable durable
 /// reclaim proofs remain until ACK and forget_reclaimed checks the receipt.
 pub fn commit_ingress_release(&mut self,ticket:IngressReleaseTicket)->Result<u64,String>{
  self.commit_ingress_release_inner(ticket,Inject::None)
 }
 #[cfg(test)]
 pub(crate) fn release_ingress_injected(&mut self,ticket:IngressReleaseTicket,inject:Inject)->Result<u64,String>{self.commit_ingress_release_inner(ticket,inject)}
 fn commit_ingress_release_inner(&mut self,ticket:IngressReleaseTicket,inject:Inject)->Result<u64,String>{
  self.verify_pair()?;self.require_decisions_drained()?;
  let h=head(&self.db1)?;
  if self.root!=ticket.root||h!=ticket.head{return Err("ingress_release_ticket_stale".into())}
  let refs=&ticket.refs;let seq=h.0.checked_add(1).ok_or("seq overflow")?;
  let b=Batch{schema:SCHEMA.into(),seq,count:h.1,n:0,prev:h.2.clone(),chain:hash(&format!("{}:INGRESS_RECLAIM:{}",h.2,serde_json::to_string(refs).map_err(|_|"encode")?)),changes:Vec::new(),resets:Vec::new(),audit:Vec::new(),portfolio:None,decision_due:None,decision_inputs:None,decision_seal:None,ingress_receipts:Vec::new(),ingress_reclaim:refs.to_vec()};
  let body=serde_json::to_string(&b).map_err(|_|"encode")?;
  transaction(&self.db1,||{apply(&self.db1,&b)?;stmt(&self.db1,"INSERT INTO pending VALUES(?,?,?)",&[V::Num(to_i(seq)?),V::Str(body.clone()),V::Str(hash(&body))])?;Ok(())})?;
  if inject==Inject::AfterDb1{return Err("injected_reclaim_after_db1".into())}
  self.ack_pending(inject)?;Ok(seq)
 }
 pub fn release_ingress_receipts(&mut self,inbox:&ingress_store_r1205::Store,refs:&[(String,u64)])->Result<Option<u64>,String>{
  match self.prepare_ingress_release(inbox,refs)?{Some(t)=>self.commit_ingress_release(t).map(Some),None=>Ok(None)}
 }
 pub fn may_resume_old_book_as_fresh(&self)->bool{false}
 pub fn may_trade_real(&self)->bool{false}
 pub fn private_root(&self)->&Path{&self.root}
}

#[cfg(test)]
mod tests{
 use super::*;
 use std::time::{SystemTime,UNIX_EPOCH};
 use std::os::unix::fs::DirBuilderExt;
 struct Temp(PathBuf);
 impl Temp{
  fn new()->Self{
   let p=std::env::temp_dir().join(format!("spotarb_r1173_{}_{}",
    std::process::id(),SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()));
   fs::DirBuilder::new().mode(0o700).create(&p).unwrap();Self(p)
  }
 }
 impl Drop for Temp{
  fn drop(&mut self){let _=fs::remove_dir_all(&self.0);}
 }
 fn owners()->Vec<Owner>{(0..12).map(|idx|Owner::new(idx,12,800).unwrap()).collect()}
 fn frame(seq:i64)->RawFrame{
  let ms=1_790_000_000_000+seq*100;
  let raw=serde_json::json!({"stream":"btcusdt@depth20@500ms",
   "data":{"e":"depthUpdate","s":"BTCUSDT","E":ms-50,"u":seq,
    "b":[["100","2"],["99","3"]],
    "a":[["101","2"],["102","3"]]}}).to_string();
  RawFrame{source:None,venue:Venue::BinanceFutures,epoch:1,
   received_ns:(seq as u64)*1000,received_wall_ms:ms,raw}
 }
 fn input(f:RawFrame,owners:&[Owner])->Vec<(RawFrame,Update,usize)>{
  let upd=crate::parse(&f).unwrap();
  let ix=owners.iter().position(|o|o.owns(&upd.base)).unwrap();
  vec![(f,upd,ix)]
 }
 #[test]
 fn r1280_decision_identity_ignores_mutable_trace_timing_but_keeps_frame_trace_id(){
  let mut f=frame(1);
  f.source=Some(crate::SourceProvenance{selected_permit:None,prepared:None,
   frame_trace_id:"trace:session:1".into(),journal_id:"j".into(),accepted_id:1,
   session_id:"session".into(),connection_epoch:1,selection_epoch:1,
   payload_sha256:hash(&f.raw),transport:None,accepted_ns:Some(1),selected_ns:Some(2),
   r1247_timing:None,r1279_timing:Some(crate::R1279FrameTiming{
    clock_domain_id:Some("test".into()),ws_receive_ns:Some(3),..Default::default()
   })
  });
  let before=decision_frame_identity(&f);
  f.source.as_mut().unwrap().r1279_timing.as_mut().unwrap().source_durable_start_ns=Some(4);
  assert_eq!(decision_frame_identity(&f),before);
  f.source.as_mut().unwrap().r1279_timing.as_mut().unwrap().source_durable_end_ns=Some(5);
  assert_eq!(decision_frame_identity(&f),before);
  f.source.as_mut().unwrap().frame_trace_id.push_str(":changed");
  assert_ne!(decision_frame_identity(&f),before);
 }
 fn model_cause(frame:&RawFrame,base:&str,before:&PaperPortfolio,after:&PaperPortfolio)->serde_json::Value{
  let rows=serde_json::json!([{"reason":"TEST_SYNTHETIC"}]);
  let policy=serde_json::json!({"max_age_ms":800,"risk_cap":10.0});
  let result=serde_json::json!({"schema":"SPOTARB/R1178/DECISION-RESULT/V1",
    "source_venue":format!("{:?}",frame.venue),
    "source_epoch":frame.epoch,"source_received_ns":frame.received_ns,
    "source_received_wall_ms":frame.received_wall_ms,
    "decision_time_ns":frame.received_ns+1,"decision_time_wall_ms":frame.received_wall_ms,
    "decision_rows":&rows,"modeled_after":after.snapshot()});
  serde_json::json!({"schema":"SPOTARB/R1184/MODELED-DECISION-CAUSE/V1",
    "base":base,"source_venue":ven(frame.venue),"source_epoch":frame.epoch,
    "source_received_ns":frame.received_ns,
    "source_received_wall_ms":frame.received_wall_ms,
    "decision_time_ns":frame.received_ns+1,
    "decision_config_sha256":hash(&policy.to_string()),
    "decision_config_source":"TEST_SYNTHETIC",
    "effective_decision_config":policy,
    "paper_model_assumptions":{"synthetic":true},"route":null,
    "metadata_generation":1,
    "decision_rows":rows,
    "decision_time_wall_ms":frame.received_wall_ms,
    "modeled_before":before.snapshot(),
    "spot_book":null,"futures_book":null,"result_proof":result})
 }
 fn modeled_step(p:&mut PaperPortfolio,leg:usize){
  use crate::native_paper_market_execution_review::{PaperMachine,PaperPosition};
  match leg{
   0=>{
    let pos=PaperPosition{
     base:"BTC".into(),signal_ns:100,spot_buy_ns:101,
     signal_estimated_net_bps:65.0,spot_gross_base:0.012,
     spot_delivered_base:0.01199,spot_paid_quote:800.0,
     futures_short_ns:None,futures_short_proceeds_quote:None,
     futures_short_base:None,hedge_entry_estimated_net_bps:None,
    };
    p.positions.insert("BTC".into(),PaperMachine{
     config:p.assumptions.clone(),phase:PaperPhase::AwaitingHedge(pos),
    });
    p.stats.entries+=1;
   },
   1=>{
    let machine=p.positions.get_mut("BTC").unwrap();
    let PaperPhase::AwaitingHedge(mut pos)=machine.phase.clone()
      else{panic!("invalid prehedge test phase")};
    pos.futures_short_ns=Some(102);
    pos.futures_short_base=Some(0.01199);
    pos.futures_short_proceeds_quote=Some(815.0);
    pos.hedge_entry_estimated_net_bps=Some(45.0);
    machine.phase=PaperPhase::Hedged(pos);
    p.stats.futures_hedges+=1;
   },
   2=>{
    let machine=p.positions.get_mut("BTC").unwrap();
    let PaperPhase::Hedged(pos)=machine.phase.clone()
      else{panic!("invalid precover test phase")};
    machine.phase=PaperPhase::AwaitingSpotExit{
      pos,cover_quote:795.0,cover_ns:104,close_estimate_quote:10.0};
    p.stats.futures_covers+=1;
   },
   3=>{
    let prior=p.positions.remove("BTC").unwrap();
    assert!(matches!(prior.phase,PaperPhase::AwaitingSpotExit{..}));
    p.closed_bases.insert("BTC".into());
    p.closed_at_monotonic_ns.insert("BTC".into(),106);
    p.stats.closed+=1;
    p.stats.closed_wins+=1;
    p.stats.modeled_realized_usdt+=12.0;
   },
   _=>panic!("unknown modeled test leg")
  }
 }
 #[test]fn native_db1_before_owner_and_mirror(){
  let t=Temp::new();let mut writer=Writer::create(&t.0).unwrap();
  let mut o=owners();
  let frames=input(frame(17),&o);
  let idx=frames[0].2;
  assert!(o[idx].snapshot_venue("BTC",Venue::BinanceFutures).is_none());
  assert_eq!(writer.source_commit(&mut o,&frames,&[],Inject::None),Ok(1));
  assert!(o[idx].snapshot_venue("BTC",Venue::BinanceFutures).is_some());
  assert_eq!(writer.cursor().unwrap().0,1);
  assert_eq!(writer.recover().unwrap().1,1);
  assert!(!writer.may_resume_old_book_as_fresh());
  assert!(!writer.may_trade_real());
 }
 #[test]fn native_db1_crash_then_recovery_exact(){
  let t=Temp::new();let mut w=Writer::create(&t.0).unwrap();
  let mut o=owners();let f=input(frame(17),&o);
  assert!(w.source_commit(&mut o,&f,&[],Inject::AfterDb1).is_err());
  assert_eq!(head(&w.db1).unwrap().0,1);
  assert_eq!(head(&w.db2).unwrap().0,0);
  drop(w);
  let mut w=Writer::resume(&t.0).unwrap();
  assert_eq!(w.recover().unwrap().0,1);
  assert!(w.book_quarantined(Venue::BinanceFutures,"BTC").unwrap().is_some());
  assert!(stmt(&w.db1,"SELECT seq FROM pending",&[]).unwrap().is_empty());
  assert_eq!(w.recover().unwrap().1,1);
 }
 #[test]fn native_db2_crash_avoids_double_source_count(){
  let t=Temp::new();let mut w=Writer::create(&t.0).unwrap();
  let mut o=owners();let f=input(frame(17),&o);
  assert!(w.source_commit(&mut o,&f,&[],Inject::AfterDb2).is_err());
  assert_eq!(head(&w.db2).unwrap().0,1);
  drop(w);
  let mut w=Writer::resume(&t.0).unwrap();
  assert_eq!(w.recover().unwrap().1,1);
  assert_eq!(w.cursor().unwrap().1,1);
  assert_eq!(head(&w.db2).unwrap().1,1);
  assert!(stmt(&w.db1,"SELECT name FROM sqlite_master WHERE name='minutes'",&[]).unwrap().is_empty());
 }

 // R1324A: scenarios are compiled only in Astra; independent execution belongs to Luna.
 fn r1324a_execution(base:&str)->crate::native_paper_execution_r1289::DurablePaper{
  let v:serde_json::Value=serde_json::from_str(include_str!("../fixtures/r1298/full_exit.json")).unwrap();
  let mut id:crate::native_paper_hedge_r1296::Identity=serde_json::from_value(v["identity"].clone()).unwrap();
  id.profile.base=base.into();id.profile.spot_symbol=format!("{base}-USDT");id.profile.futures_symbol=format!("{base}USDT");
  let n=std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
  let path=std::path::PathBuf::from(format!("/mnt/spotarb_db_test/r1273_r1275_engine_latency_20261111/R1301/lab/r1324a-tests/{}-{n}/execution.jsonl",std::process::id()));
  crate::native_paper_execution_r1289::DurablePaper::create_isolated(path,id).unwrap()
 }
 #[test]fn r1324a_crash_reopen_completion_and_duplicate_reconciliation(){
  let t=Temp::new();let mut w=Writer::create(&t.0).unwrap();let mut owners=owners();let f=frame(17);let u=crate::parse(&f).unwrap();let base=u.base.clone();let i=owners.iter().position(|o|o.owns(&base)).unwrap();
  w.source_commit_guarded(&mut owners,&[(f.clone(),u,i)],&[],Inject::None).unwrap();
  let mut execution=r1324a_execution(&base);let boundary=w.decision_boundary_r1324a(&f,1).unwrap();
  execution.complete_decision_r1324a(boundary,hash("evaluated-no-intent")).unwrap();
  let path=execution.journal_path().to_path_buf();let id=execution.isolated_identity().unwrap().clone();drop(execution);drop(w);
  let execution=crate::native_paper_execution_r1289::DurablePaper::recover_isolated(path,&id).unwrap();let mut w=Writer::resume(&t.0).unwrap();
  assert_eq!(w.reconcile_execution_completion_r1324a(&execution).unwrap(),"EXACT_EXECUTION_COMPLETION_SEALED");
  let before=head(&w.db1).unwrap();assert_eq!(w.reconcile_execution_completion_r1324a(&execution).unwrap(),"NO_PENDING_DECISION");assert_eq!(head(&w.db1).unwrap(),before);
 }
 #[test]fn r1324a_journal_seal_mismatch_and_missing_completion_fail_closed(){
  let t=Temp::new();let mut w=Writer::create(&t.0).unwrap();let mut owners=owners();let f=frame(17);let u=crate::parse(&f).unwrap();let base=u.base.clone();let i=owners.iter().position(|o|o.owns(&base)).unwrap();
  w.source_commit_guarded(&mut owners,&[(f.clone(),u,i)],&[],Inject::None).unwrap();let mut e=r1324a_execution(&base);
  assert!(w.reconcile_execution_completion_r1324a(&e).is_err());
  let mut boundary=w.decision_boundary_r1324a(&f,1).unwrap();boundary.source_sequence+=1;
  e.complete_decision_r1324a(boundary,hash("wrong-boundary")).unwrap();
  assert!(w.reconcile_execution_completion_r1324a(&e).unwrap_err().contains("JOURNAL_SEAL_MISMATCH"));assert!(w.require_decisions_drained().is_err());
 }
 #[test]fn r1324a_sealed_canonical_rejects_truncated_or_changed_execution_history(){
  let t=Temp::new();let mut w=Writer::create(&t.0).unwrap();let mut owners=owners();let f=frame(17);let u=crate::parse(&f).unwrap();let base=u.base.clone();let i=owners.iter().position(|o|o.owns(&base)).unwrap();
  w.source_commit_guarded(&mut owners,&[(f.clone(),u,i)],&[],Inject::None).unwrap();let boundary=w.decision_boundary_r1324a(&f,1).unwrap();
  let mut e=r1324a_execution(&base);let proof=hash("completed-original");e.complete_decision_r1324a(boundary.clone(),proof.clone()).unwrap();w.note_guarded_decision(&f,"EVALUATED",&proof).unwrap();
  assert_eq!(w.reconcile_execution_completion_r1324a(&e).unwrap(),"NO_PENDING_DECISION");
  let mut missing=r1324a_execution(&base);assert!(w.reconcile_execution_completion_r1324a(&missing).unwrap_err().contains("SEALED_EXECUTION_HISTORY_DIVERGENCE"));
  missing.complete_decision_r1324a(boundary,hash("different-result")).unwrap();assert!(w.reconcile_execution_completion_r1324a(&missing).unwrap_err().contains("SEALED_EXECUTION_HISTORY_DIVERGENCE"));
 }
 #[test]fn r1324a_partial_db2_and_ack_divergence_no_seal(){
  for inject in [Inject::AfterDb1,Inject::AfterDb2]{
   let t=Temp::new();let mut w=Writer::create(&t.0).unwrap();let mut owners=owners();let f=frame(17);let u=crate::parse(&f).unwrap();let base=u.base.clone();let i=owners.iter().position(|o|o.owns(&base)).unwrap();
   assert!(w.source_commit_guarded(&mut owners,&[(f,u,i)],&[],inject).is_err());let e=r1324a_execution(&base);
   assert!(w.reconcile_execution_completion_r1324a(&e).is_err());assert!(w.require_decisions_drained().is_err());
  }
 }
 #[test]fn r1179_thirty_two_guarded_frames_exact_mirror(){
  let t=Temp::new();let mut w=Writer::create(&t.0).unwrap();
  let mut owners=owners();
  let mut frames=Vec::new();
  for seq in 17..49{
    let f=frame(seq);
    let u=crate::parse(&f).unwrap();
    let i=owners.iter().position(|owner|owner.owns(&u.base)).unwrap();
    frames.push((f,u,i));
  }
  assert_eq!(frames.len(),32);
  assert_eq!(w.source_commit_guarded(&mut owners,&frames,&[],Inject::None),Ok(1));
  assert_eq!(w.cursor().unwrap().1,32);
  let inbox=w.quarantined_decision_inputs().unwrap();
  assert_eq!(inbox.len(),32);
  assert_eq!(inbox[0].raw,frames[0].0.raw);
  assert_eq!(inbox[31].raw,frames[31].0.raw);
  for (idx,(f,_,_)) in frames.iter().enumerate(){
    let r=w.note_guarded_decision(f,"EVALUATED",&hash(&format!("R1179_{idx}"))).unwrap();
    assert_eq!(r,if idx==31{Some(2)}else{None});
  }
  w.require_decisions_drained().unwrap();
  assert!(w.quarantined_decision_inputs().unwrap().is_empty());
  w.verify_pair().unwrap();
  assert_eq!(head(&w.db2).unwrap(),head(&w.db1).unwrap());
  assert_eq!(stmt(&w.db1,"SELECT sealed_batches,sealed_frames FROM decision_totals",&[]).unwrap()[0],vec!["1","32"]);
  assert_eq!(w.book_quarantined(Venue::BinanceFutures,"BTC").unwrap().unwrap().sequence,Some(48));
  assert_eq!(w.recover().unwrap().1,32);
 }
 #[test]fn r1178_output_digest_is_required_and_part_of_seal_chain(){
  let t=Temp::new();let mut w=Writer::create(&t.0).unwrap();
  let mut owners=owners();let frames=input(frame(17),&owners);
  let raw=frames[0].0.clone();
  w.source_commit_guarded(&mut owners,&frames,&[],Inject::None).unwrap();
  assert!(w.note_guarded_decision(&raw,"EVALUATED","").is_err());
  assert!(w.note_guarded_decision(&raw,"EVALUATED","R".repeat(64).as_str()).is_err());
  assert_eq!(w.note_guarded_decision(&raw,"EVALUATED",&hash("actual-result-1")).unwrap(),Some(2));
  let c1=stmt(&w.db1,"SELECT proof_chain FROM decision_totals",&[]).unwrap()[0][0].clone();
  drop(w);
  let t2=Temp::new();let mut w2=Writer::create(&t2.0).unwrap();
  let mut o2=(0..12).map(|idx|Owner::new(idx,12,800).unwrap()).collect::<Vec<_>>();
  let f2=input(frame(17),&o2);
  w2.source_commit_guarded(&mut o2,&f2,&[],Inject::None).unwrap();
  assert_eq!(w2.note_guarded_decision(&raw,"EVALUATED",&hash("different-result")).unwrap(),Some(2));
  let c2=stmt(&w2.db1,"SELECT proof_chain FROM decision_totals",&[]).unwrap()[0][0].clone();
  assert_ne!(c1,c2);
  assert_eq!(w2.recover().unwrap().1,1);
 }
 #[test]
 fn r1186_two_symbol_independent_flat_depth_oracle_with_guarded_model(){
  use crate::decision::{DecisionConfig,Limits,RouteSpec,evaluate};
  use std::collections::HashSet;
  let wall=1_790_000_000_000i64;
  let make_frame=|base:&str,venue:Venue,bid:f64,ask:f64,seq:i64|{
   let raw=if venue==Venue::OkxSpot{
    serde_json::json!({"arg":{"channel":"books5","instId":format!("{base}-USDT")},
      "data":[{"ts":wall.to_string(),"seqId":seq,
       "bids":[[bid.to_string(),"100"]],"asks":[[ask.to_string(),"100"]]}]})
   }else{
    serde_json::json!({"stream":format!("{}usdt@depth20@500ms",base.to_lowercase()),
     "data":{"e":"depthUpdate","s":format!("{base}USDT"),"E":wall,"u":seq,
       "b":[[bid.to_string(),"100"]],"a":[[ask.to_string(),"100"]]}})
   };
   RawFrame{source:None,venue,epoch:1,received_ns:(seq as u64)*1000,
    received_wall_ms:wall,raw:raw.to_string()}
  };
  let raw=vec![
   make_frame("BTC",Venue::OkxSpot,99.9,100.0,1),
   make_frame("BTC",Venue::BinanceFutures,101.0,101.1,2),
   make_frame("ETH",Venue::OkxSpot,99.9,100.0,3),
   make_frame("ETH",Venue::BinanceFutures,100.1,100.2,4)];
  let o=owners();
  let updates:Vec<(RawFrame,Update,usize)>=raw.iter().map(|f|{
   let u=crate::parse(f).unwrap();
   let idx=o.iter().position(|sh|sh.owns(&u.base)).unwrap();
   (f.clone(),u,idx)
  }).collect();
  let size=Limits{min_amount:"0.001".into(),step:"0.001".into(),
   min_cost:"1".into(),max_amount:Some("10".into())};
  let route=RouteSpec{buy_exchange:"okx_spot".into(),
   sell_exchange:"binance_futures".into(),label:"OKX-FUT".into(),
   buy:size.clone(),sell:size,loaded_ns:1,
   metadata_state:"SIZE_METADATA_READY".into()};
  let cfg=DecisionConfig{max_age_ms:800,metadata_ttl_ms:43_200_000,
   base_size:10.,risk_cap:10.,cost:24.03,required_net:42.,
   min_price:0.,max_price:f64::INFINITY,blacklist:HashSet::new()};
  let spot_btc=&updates[0].1.book;let fut_btc=&updates[1].1.book;
  let spot_eth=&updates[2].1.book;let fut_eth=&updates[3].1.book;
  let btc=evaluate("BTC",&route,Some(spot_btc),Some(fut_btc),4001,wall,&cfg);
  let eth=evaluate("ETH",&route,Some(spot_eth),Some(fut_eth),4001,wall,&cfg);
  // This oracle is independent arithmetic on flat levels: no call into
  // DecisionCore for expected gross/net or expected rejection.
  let oracle_btc=(101.0/100.0-1.0)*10_000.0-24.03;
  let oracle_eth=(100.1/100.0-1.0)*10_000.0-24.03;
  assert!(oracle_btc>42.0&&oracle_eth<42.0);
  assert_eq!(btc.reason,"EXECUTABLE_OPPORTUNITY");
  assert!((btc.estimated_net_bps.unwrap()-oracle_btc).abs()<0.01);
  assert_eq!(eth.reason,"EDGE_TOO_LOW");
  // DecisionCore intentionally aborts before depth sizing when the
  // best-price gross bound cannot cover required net plus costs.
  assert!(eth.estimated_net_bps.is_none());
  assert!((eth.gross_upper_bound_bps.unwrap()-(oracle_eth+24.03)).abs()<0.01);
  let baseline=PaperPortfolio::new(wall).unwrap();
  let mut modeled=baseline.clone();
  modeled.on_frame("BTC",Venue::BinanceFutures,4001,wall,
    Some(spot_btc),Some(fut_btc),Some(&btc),Some(&route)).unwrap();
  assert_eq!(modeled.stats.entries,1);
  assert!(matches!(modeled.positions["BTC"].phase,PaperPhase::AwaitingHedge(_)));
  modeled.on_frame("ETH",Venue::BinanceFutures,4002,wall,
    Some(spot_eth),Some(fut_eth),Some(&eth),Some(&route)).unwrap();
  assert_eq!(modeled.stats.entries,1);
  assert!(!modeled.positions.contains_key("ETH"));
  let t=Temp::new();
  let mut writer=Writer::create(&t.0).unwrap();
  let mut owned=owners();
  assert_eq!(writer.commit_modeled_portfolio(None,&baseline,"BOOT",Inject::None),Ok(1));
  assert_eq!(writer.source_commit_guarded(&mut owned,&updates,&[],Inject::None),Ok(2));
  assert!(writer.require_decisions_drained().is_err());
  assert_eq!(writer.note_guarded_decision(&raw[0],"EVALUATED",&hash("btcs-spot-observation")).unwrap(),None);
  let mut cause=model_cause(&raw[1],"BTC",&baseline,&modeled);
  let rows=serde_json::json!([btc]);
  cause["decision_rows"]=rows.clone();
  cause["result_proof"]["decision_rows"]=rows;
  cause["spot_book"]=serde_json::to_value(spot_btc).unwrap();
  cause["futures_book"]=serde_json::to_value(fut_btc).unwrap();
  cause["route"]=serde_json::to_value(&route).unwrap();
  cause["paper_model_assumptions"]=serde_json::to_value(&modeled.assumptions).unwrap();
  cause["decision_config_source"]=serde_json::json!("TWO_SYMBOL_ORACLE_FIXTURE");
  let proof_hash=hash(&cause["result_proof"].to_string());
  assert_eq!(writer.commit_modeled_portfolio_for_decision(
    &baseline,&modeled,"BTC",&raw[1],&cause,&proof_hash,Inject::None),Ok(3));
  assert_eq!(writer.note_guarded_decision(&raw[1],"EVALUATED",&proof_hash).unwrap(),None);
  assert_eq!(writer.note_guarded_decision(&raw[2],"EVALUATED",&hash("eth-spot-observation")).unwrap(),None);
  let eth_proof=hash(&serde_json::to_string(&eth).unwrap());
  assert_eq!(writer.note_guarded_decision(&raw[3],"EVALUATED",&eth_proof).unwrap(),Some(4));
  assert_eq!(writer.recover().unwrap().1,4);
  writer.verify_pair().unwrap();
  let saved=writer.quarantined_modeled_state().unwrap().unwrap();
  assert!(saved.cold_boot_unknown);
  assert_eq!(saved.stats.entries,1);
  assert_eq!(saved.positions.len(),1);
  assert_eq!(stmt(&writer.db1,"SELECT sealed_frames FROM decision_totals",&[]).unwrap()[0][0],"4");
 }
 #[test]fn r1184_guarded_model_requires_exact_current_frame_and_proof(){
  let t=Temp::new();let mut w=Writer::create(&t.0).unwrap();
  let mut o=owners();let baseline=PaperPortfolio::new(1_790_000_000_000).unwrap();
  w.commit_modeled_portfolio(None,&baseline,"BOOT",Inject::None).unwrap();
  let a=input(frame(17),&o);let f=a[0].0.clone();
  w.source_commit_guarded(&mut o,&a,&[],Inject::None).unwrap();
  let mut modeled=baseline.clone();modeled_step(&mut modeled,0);
  assert_eq!(w.commit_modeled_portfolio(Some(&baseline),&modeled,"BTC",Inject::None),
    Err("guarded_model_decision_proof_required".into()));
  let cause=model_cause(&f,"BTC",&baseline,&modeled);
  let mut other=f.clone();other.received_ns+=1;
  assert_eq!(w.commit_modeled_portfolio_for_decision(&baseline,&modeled,"BTC",&other,&cause,&hash(&cause["result_proof"].to_string()),Inject::None),
    Err("guarded_model_decision_cause_mismatch".into()));
  let mut forged=cause.clone();forged["decision_config_sha256"]=serde_json::json!("bad");
  assert_eq!(w.commit_modeled_portfolio_for_decision(&baseline,&modeled,"BTC",&f,&forged,&hash(&cause["result_proof"].to_string()),Inject::None),
    Err("guarded_model_decision_cause_mismatch".into()));
  let mut changed_policy=cause.clone();changed_policy["effective_decision_config"]["risk_cap"]=serde_json::json!(11.0);
  assert_eq!(w.commit_modeled_portfolio_for_decision(&baseline,&modeled,"BTC",&f,&changed_policy,&hash(&cause["result_proof"].to_string()),Inject::None),
    Err("guarded_model_effective_policy_hash_mismatch".into()));
  assert_eq!(w.cursor().unwrap().0,2); // baseline + source only
  assert_eq!(w.commit_modeled_portfolio_for_decision(&baseline,&modeled,"BTC",&f,&cause,&hash(&cause["result_proof"].to_string()),Inject::None),Ok(3));
  let body=stmt(&w.db1,"SELECT body FROM audit WHERE id='R1175MODELED3'",&[]).unwrap();
  let event:serde_json::Value=serde_json::from_str(&body[0][0]).unwrap();
  assert_eq!(event["decision_cause"]["source_identity"],decision_frame_identity(&f));
  assert_eq!(event["decision_cause"]["proof_hash"],hash(&cause.to_string()));
  assert_eq!(w.commit_modeled_portfolio_for_decision(&baseline,&modeled,"BTC",&f,&cause,&hash(&cause["result_proof"].to_string()),Inject::None),
    Err("modeled_predecessor_denied".into()));
  assert_eq!(w.note_guarded_decision(&f,"EVALUATED",&hash("wrong")),
    Err("modeled_receipt_result_mismatch".into()));
  assert_eq!(w.note_guarded_decision(&f,"EVALUATED",&hash(&cause["result_proof"].to_string())).unwrap(),Some(4));
  w.verify_pair().unwrap();
 }
 #[test]fn r1184_model_proof_after_db2_crash_remains_durable_but_cold_denied(){
  let t=Temp::new();let mut w=Writer::create(&t.0).unwrap();
  let mut o=owners();let before=PaperPortfolio::new(1_790_000_000_000).unwrap();
  w.commit_modeled_portfolio(None,&before,"BOOT",Inject::None).unwrap();
  let a=input(frame(17),&o);let f=a[0].0.clone();
  w.source_commit_guarded(&mut o,&a,&[],Inject::None).unwrap();
  let mut after=before.clone();modeled_step(&mut after,0);
  let cause=model_cause(&f,"BTC",&before,&after);
  let digest=hash(&cause["result_proof"].to_string());
  assert_eq!(w.commit_modeled_portfolio_for_decision(&before,&after,"BTC",&f,&cause,&digest,Inject::AfterDb2),
    Err("injected_crash_after_db2".into()));
  drop(w);
  let mut resumed=Writer::resume(&t.0).unwrap();
  assert!(resumed.recover().is_err());
  assert_eq!(resumed.cursor().unwrap().0,3);
  let a=stmt(&resumed.db1,"SELECT body FROM audit WHERE id='R1175MODELED3'",&[]).unwrap();
  let b=stmt(&resumed.db2,"SELECT body FROM audit WHERE id='R1175MODELED3'",&[]).unwrap();
  assert_eq!(a,b);
  let event:serde_json::Value=serde_json::from_str(&a[0][0]).unwrap();
  assert_eq!(event["decision_cause"]["result_hash"],digest);
  assert!(resumed.quarantined_modeled_state().is_err());
  assert!(resumed.note_guarded_decision(&f,"EVALUATED",&digest).is_err());
 }
 #[test]fn r1177_mixed_decision_and_modeled_leg_in_one_source_batch(){
  let t=Temp::new();let mut w=Writer::create(&t.0).unwrap();
  let mut owners=owners();
  let baseline=PaperPortfolio::new(1_790_000_000_000).unwrap();
  assert_eq!(w.commit_modeled_portfolio(None,&baseline,"BOOT",Inject::None),Ok(1));
  let a=input(frame(17),&owners);
  let b=input(frame(18),&owners);
  let mut frames=a.clone();frames.extend(b);
  assert_eq!(w.source_commit_guarded(&mut owners,&frames,&[],Inject::None),Ok(2));
  assert!(w.note_guarded_decision(&frames[1].0,"EVALUATED",&hash("sample-decision-1")).is_err());
  assert_eq!(w.note_guarded_decision(&frames[0].0,"EVALUATED",&hash("sample-decision-0")).unwrap(),None);
  assert!(w.require_decisions_drained().is_err());
  let mut modeled=baseline.clone();modeled_step(&mut modeled,0);
  let cause=model_cause(&frames[1].0,"BTC",&baseline,&modeled);
  assert_eq!(w.commit_modeled_portfolio_for_decision(&baseline,&modeled,"BTC",
     &frames[1].0,&cause,&hash(&cause["result_proof"].to_string()),Inject::None),Ok(3));
  assert_eq!(w.note_guarded_decision(&frames[1].0,"EVALUATED",&hash(&cause["result_proof"].to_string())).unwrap(),Some(4));
  assert_eq!(w.recover().unwrap().0,4);
  assert_eq!(w.cursor().unwrap().1,2);
  assert_eq!(w.quarantined_modeled_state().unwrap().unwrap().stats.entries,1);
  let sealed=stmt(&w.db1,
    "SELECT sealed_batches,sealed_frames FROM decision_totals",&[]).unwrap();
  assert_eq!(sealed[0],vec!["1","2"]);
  w.verify_pair().unwrap();
 }
 #[test]fn r1185_mirrored_due_requires_identical_original_inbox_bytes(){
  let t=Temp::new();let mut w=Writer::create(&t.0).unwrap();
  let mut o=owners();let a=input(frame(17),&o);
  w.source_commit_guarded(&mut o,&a,&[],Inject::None).unwrap();
  let row=one(&w.db2,"SELECT inputs_json FROM decision_due WHERE id=1",&[])
    .unwrap().unwrap();
  // JSON whitespace preserves every RawFrame and its identity, but this
  // is no longer the exact original mirrored inbox representation.
  let changed=format!(" {}",row[0]);
  stmt(&w.db2,"UPDATE decision_due SET inputs_json=?,inputs_sha256=? WHERE id=1",&[
    V::Str(changed.clone()),V::Str(hash(&changed))]).unwrap();
  assert_eq!(read_due(&w.db1).unwrap(),read_due(&w.db2).unwrap());
  assert_eq!(w.verify_pair(),Err("unmirrored_decision_inputs".into()));
  assert_eq!(w.quarantined_decision_inputs().unwrap_err(),"unmirrored_decision_inputs");
  assert!(w.recover().is_err());
 }
 #[test]fn r1183_cached_hashes_still_require_full_inbox_validation(){
  let t=Temp::new();let mut w=Writer::create(&t.0).unwrap();
  let mut owners=owners();
  let mut frames=input(frame(17),&owners);
  frames.extend(input(frame(18),&owners));
  w.source_commit_guarded(&mut owners,&frames,&[],Inject::None).unwrap();
  let receipt_hash=hash("R1183/SYNTHETIC-NO-DECISIONCORE");
  assert!(w.note_guarded_decision(&frames[0].0,"EVALUATED",&receipt_hash)
     .unwrap().is_none());
  // The fast per-frame path uses ACK-certified in-memory identities.
  // Tamper with persisted due between receipts: complete seal MUST fail.
  stmt(&w.db1,"UPDATE decision_due SET inputs_sha256='tampered'",&[]).unwrap();
  let err=w.note_guarded_decision(&frames[1].0,"EVALUATED",&receipt_hash)
    .expect_err("corrupted source inbox must not be silently sealed");
  assert!(err.contains("decision_due_checksum_mismatch"));
  assert!(w.require_decisions_drained().is_err());
  assert_eq!(w.cursor().unwrap().1,2);
  assert!(w.recover().is_err());
 }
 #[test]fn r1181_transient_inbox_survives_crash_and_prunes_after_seal(){
  let t=Temp::new();let mut w=Writer::create(&t.0).unwrap();
  let mut o=owners();
  let mut a=input(frame(17),&o);a.extend(input(frame(18),&o));
  assert_eq!(w.source_commit_guarded(&mut o,&a,&[],Inject::None),Ok(1));
  let forensic=w.quarantined_decision_inputs().unwrap();
  assert_eq!(forensic.len(),2);
  assert_eq!(forensic[0].raw,a[0].0.raw);
  assert_eq!(forensic[1].raw,a[1].0.raw);
  assert!(w.recover().is_err());
  drop(w);
  let mut resumed=Writer::resume(&t.0).unwrap();
  assert!(resumed.recover().is_err());
  assert_eq!(resumed.quarantined_decision_inputs().unwrap()[0].raw,a[0].0.raw);
  assert_eq!(resumed.note_guarded_decision(&a[0].0,"EVALUATED",&hash("fabricated-result")),
    Err("cold_unresolved_decision_requires_independent_reconciliation".into()));
  assert!(resumed.recover().is_err());
  assert_eq!(resumed.quarantined_decision_inputs().unwrap().len(),2);
 }
 #[test]fn r1181_inbox_survives_db1_pre_db2_crash_and_stays_quarantined(){
  let t=Temp::new();let mut w=Writer::create(&t.0).unwrap();
  let mut o=owners();let a=input(frame(17),&o);
  assert_eq!(w.source_commit_guarded(&mut o,&a,&[],Inject::AfterDb1),
    Err("injected_crash_after_db1".into()));
  drop(w);
  let mut resumed=Writer::resume(&t.0).unwrap();
  assert!(resumed.quarantined_decision_inputs().is_err());
  assert!(resumed.recover().is_err());
  assert_eq!(resumed.quarantined_decision_inputs().unwrap()[0].raw,a[0].0.raw);
  assert_eq!(resumed.note_guarded_decision(&a[0].0,"EVALUATED",&hash("fake")),
    Err("cold_unresolved_decision_requires_independent_reconciliation".into()));
  assert_eq!(resumed.cursor().unwrap().1,1);
 }
 #[test]fn r1181_inbox_survives_db2_pre_ack_crash_without_unblocking_replay(){
  let t=Temp::new();let mut w=Writer::create(&t.0).unwrap();
  let mut o=owners();let a=input(frame(17),&o);
  assert_eq!(w.source_commit_guarded(&mut o,&a,&[],Inject::AfterDb2),
    Err("injected_crash_after_db2".into()));
  drop(w);
  let mut resumed=Writer::resume(&t.0).unwrap();
  assert!(resumed.quarantined_decision_inputs().is_err());
  assert!(resumed.recover().is_err());
  assert!(resumed.quarantined_decision_inputs().unwrap().len()==1);
  assert_eq!(resumed.cursor().unwrap().1,1);
  assert!(resumed.recover().is_err());
  assert_eq!(resumed.cursor().unwrap().1,1);
 }
 #[test]fn r1181_transient_inbox_corruption_denies_forensic_read_and_seal(){
  let t=Temp::new();let mut w=Writer::create(&t.0).unwrap();
  let mut o=owners();let a=input(frame(17),&o);
  w.source_commit_guarded(&mut o,&a,&[],Inject::None).unwrap();
  stmt(&w.db1,"UPDATE decision_due SET inputs_json='[]'",&[]).unwrap();
  assert!(w.quarantined_decision_inputs().is_err());
  assert!(w.note_guarded_decision(&a[0].0,"EVALUATED",&hash("result")).is_err());
  assert!(w.recover().is_err());
 }
 #[test]fn r1177_unresolved_source_obligation_denies_cold_restart(){
  let t=Temp::new();let mut w=Writer::create(&t.0).unwrap();
  let mut o=owners();let x=input(frame(17),&o);
  assert_eq!(w.source_commit_guarded(&mut o,&x,&[],Inject::None),Ok(1));
  assert!(w.require_decisions_drained().is_err());
  let newer=input(frame(18),&o);
  assert!(w.source_commit_guarded(&mut o,&newer,&[],Inject::None).is_err());
  drop(w);
  let mut resumed=Writer::resume(&t.0).unwrap();
  assert!(resumed.recover().is_err());
  assert_eq!(resumed.cursor().unwrap().1,1);
  assert!(resumed.quarantined_modeled_state().is_err());
 }
 #[test]fn r1177_ordered_receipt_batch_sealed_once(){
  let t=Temp::new();let mut w=Writer::create(&t.0).unwrap();
  let mut o=owners();
  let frames=input(frame(17),&o);
  let f=frames[0].0.clone();
  w.source_commit_guarded(&mut o,&frames,&[],Inject::None).unwrap();
  let mut incorrect=f.clone();incorrect.received_ns+=1;
  assert!(w.note_guarded_decision(&incorrect,"EVALUATED",&hash("test-decision")).is_err());
  incorrect=f.clone();incorrect.epoch+=1;
  assert!(w.note_guarded_decision(&incorrect,"EVALUATED",&hash("test-decision")).is_err());
  incorrect=f.clone();incorrect.raw.push(' ');
  assert!(w.note_guarded_decision(&incorrect,"EVALUATED",&hash("test-decision")).is_err());
  assert_eq!(w.note_guarded_decision(&f,"EVALUATED",&hash("test-decision")).unwrap(),Some(2));
  w.require_decisions_drained().unwrap();
  assert_eq!(w.recover().unwrap().0,2);
  assert_eq!(w.recover().unwrap().1,1);
  let totals=stmt(&w.db1,"SELECT sealed_batches,sealed_frames FROM decision_totals",&[]).unwrap();
  assert_eq!(totals[0],vec!["1","1"]);
  assert!(w.note_guarded_decision(&f,"EVALUATED",&hash("test-decision")).is_err());
 }
 #[test]
 fn r1280_guarded_decision_accepts_post_commit_trace_completion(){
  let t=Temp::new();let mut w=Writer::create(&t.0).unwrap();let mut o=owners();
  let mut f=frame(17);f.source=Some(crate::SourceProvenance{selected_permit:None,prepared:None,
   frame_trace_id:"trace:session:17".into(),journal_id:hash("journal").into(),
   accepted_id:17,session_id:"session".into(),connection_epoch:1,selection_epoch:1,
   payload_sha256:hash(&f.raw),transport:None,accepted_ns:Some(f.received_ns),
   selected_ns:Some(f.received_ns+1),r1247_timing:None,
   r1279_timing:Some(crate::R1279FrameTiming{
    clock_domain_id:Some("test".into()),ws_receive_ns:Some(f.received_ns),
    source_durable_start_ns:Some(f.received_ns+2),..Default::default()
   })
  });
  let frames=input(f.clone(),&o);w.source_commit_guarded(&mut o,&frames,&[],Inject::None).unwrap();
  let identity=decision_frame_identity(&f);
  let timing=f.source.as_mut().unwrap().r1279_timing.as_mut().unwrap();
  timing.source_durable_end_ns=Some(f.received_ns+3);
  timing.frame_residence_start_ns=Some(f.received_ns+4);
  timing.frame_residence_end_ns=Some(f.received_ns+5);
  timing.frame_residence_status=Some("terminalized".into());
  timing.selection_batch_id=Some(123);
  timing.quarantine_status=Some("complete".into());
  assert_eq!(decision_frame_identity(&f),identity);
  assert_eq!(w.note_guarded_decision(&f,"EVALUATED",&hash("test-decision")).unwrap(),Some(2));
  w.require_decisions_drained().unwrap();w.verify_pair().unwrap();
 }
 #[test]fn r1177_seal_crash_after_db2_is_idempotent(){
  let t=Temp::new();let mut w=Writer::create(&t.0).unwrap();
  let mut o=owners();let frames=input(frame(17),&o);
  let f=frames[0].0.clone();
  w.source_commit_guarded(&mut o,&frames,&[],Inject::None).unwrap();
  let entry=DecisionReceipt{input_hash:decision_frame_identity(&f),outcome:"EVALUATED".into(),result_hash:hash("test-decision")};
  assert!(w.seal_decision_batch(&[entry],Inject::AfterDb2).is_err());
  drop(w);
  let mut w=Writer::resume(&t.0).unwrap();
  assert_eq!(w.recover().unwrap().0,2);
  assert_eq!(stmt(&w.db1,"SELECT sealed_frames FROM decision_totals",&[]).unwrap()[0][0],"1");
  assert_eq!(stmt(&w.db2,"SELECT sealed_frames FROM decision_totals",&[]).unwrap()[0][0],"1");
  assert!(w.recover().is_ok());
  w.require_decisions_drained().unwrap();
 }
 #[test]fn r1176_diagnostics_do_not_break_portfolio_predecessor(){
  let t=Temp::new();let mut w=Writer::create(&t.0).unwrap();
  let mut p=PaperPortfolio::new(1_790_000_000_000).unwrap();
  w.commit_modeled_portfolio(None,&p,"BOOT",Inject::None).unwrap();
  let old=p.clone();modeled_step(&mut p,0);
  w.commit_modeled_portfolio(Some(&old),&p,"BTC",Inject::None).unwrap();
  // Hundreds of non-durable HOLD/deny diagnostics must not break CAS.
  p.stats.denied_stale+=900;p.stats.denied_liquidity+=600;
  p.stats.held_negative_marks+=300;
  p.positions.get_mut("BTC").unwrap().config.take_profit_quote=Some(12.0);
  let before=p.clone();
  modeled_step(&mut p,1);
  w.commit_modeled_portfolio(Some(&before),&p,"BTC",Inject::None).unwrap();
  assert_eq!(w.recover().unwrap().1,0);
  let recovered=w.quarantined_modeled_state().unwrap().unwrap();
  assert_eq!(recovered.stats.futures_hedges,1);
  assert_eq!(recovered.stats.denied_stale,0);
  assert!(matches!(recovered.positions["BTC"].phase,PaperPhase::Hedged(_)));
 }
 #[test]fn r1176_skipped_or_double_four_leg_fails_closed(){
  let t=Temp::new();let mut w=Writer::create(&t.0).unwrap();
  let original=PaperPortfolio::new(1_790_000_000_000).unwrap();
  w.commit_modeled_portfolio(None,&original,"BOOT",Inject::None).unwrap();
  let mut skipped=original.clone();
  skipped.stats.futures_hedges=1;
  assert!(w.commit_modeled_portfolio(Some(&original),&skipped,
    "BTC",Inject::None).is_err());
  let mut opened=original.clone();modeled_step(&mut opened,0);
  let mut doubled=opened.clone();doubled.stats.entries+=1;
  assert!(w.commit_modeled_portfolio(Some(&original),&doubled,
    "BTC",Inject::None).is_err());
  w.commit_modeled_portfolio(Some(&original),&opened,"BTC",Inject::None).unwrap();
  assert_eq!(w.cursor().unwrap().0,2);
  assert_eq!(w.quarantined_modeled_state().unwrap().unwrap().stats.entries,1);
 }
 #[test]fn r1175_modeled_baseline_and_crash_after_db1_recovered(){
  let t=Temp::new();let mut w=Writer::create(&t.0).unwrap();
  let original=PaperPortfolio::new(1_790_000_000_000).unwrap();
  assert_eq!(w.commit_modeled_portfolio(None,&original,"BOOT",Inject::None).unwrap(),1);
  assert_eq!(head(&w.db1).unwrap().1,0);
  let mut next=original.clone();
  modeled_step(&mut next,0);
  assert!(w.commit_modeled_portfolio(Some(&original),&next,
    "BTC",Inject::AfterDb1).is_err());
  assert_eq!(head(&w.db1).unwrap().0,2);
  assert_eq!(head(&w.db2).unwrap().0,1);
  assert!(w.quarantined_modeled_state().is_err());
  drop(w);
  let mut w=Writer::resume(&t.0).unwrap();
  assert_eq!(w.recover().unwrap().0,2);
  let stored=w.quarantined_modeled_state().unwrap().unwrap();
  assert!(stored.cold_boot_unknown);
  assert_eq!(stored.stats.entries,1);
  assert!(w.commit_modeled_portfolio(Some(&original),&next,
    "BTC",Inject::None).is_err());
  assert_eq!(stmt(&w.db1,"SELECT count(*) FROM audit",&[]).unwrap()[0][0],"2");
 }
 #[test]fn r1175_open_modeled_position_preserves_complete_fields_in_quarantine(){
  use crate::native_paper_market_execution_review::{
    PaperMachine,PaperPhase,PaperPosition
  };
  let t=Temp::new();
  let mut w=Writer::create(&t.0).unwrap();
  let initial=PaperPortfolio::new(1_790_000_000_000).unwrap();
  w.commit_modeled_portfolio(None,&initial,"BOOT",Inject::None).unwrap();
  let mut next=initial.clone();
  next.stats.entries+=1;
  let position=PaperPosition{
    base:"BTC".to_owned(),signal_ns:10_000,
    spot_buy_ns:10_002,signal_estimated_net_bps:72.25,
    spot_gross_base:0.012,spot_delivered_base:0.01199,
    spot_paid_quote:800.0,futures_short_ns:None,
    futures_short_proceeds_quote:None,futures_short_base:None,
    hedge_entry_estimated_net_bps:None,
  };
  next.positions.insert("BTC".into(),PaperMachine{
    config:next.assumptions.clone(),
    phase:PaperPhase::AwaitingHedge(position),
  });
  w.commit_modeled_portfolio(Some(&initial),&next,"BTC",Inject::AfterDb2)
    .expect_err("expected modeled DB2 before ACK interruption");
  drop(w);
  let mut w=Writer::resume(&t.0).unwrap();
  assert_eq!(w.recover().unwrap().1,0);
  w.verify_pair().unwrap();
  let cold=w.quarantined_modeled_state().unwrap().unwrap();
  assert!(cold.cold_boot_unknown);
  assert_eq!(cold.positions.len(),1);
  assert_eq!(cold.stats.entries,1);
  match &cold.positions["BTC"].phase{
   PaperPhase::AwaitingHedge(pos)=>{
    assert_eq!(pos.spot_delivered_base,0.01199);
    assert_eq!(pos.spot_paid_quote,800.0);
    assert_eq!(pos.signal_estimated_net_bps,72.25);
    assert!(pos.futures_short_ns.is_none());
   },
   _=>panic!("partial modeled entry recovered as wrong stage"),
  }
  assert!(!w.may_resume_old_book_as_fresh());
  assert!(!w.may_trade_real());
 }
 #[test]fn r1175_modeled_four_transitions_after_db2_fault_once(){
  let t=Temp::new();let mut w=Writer::create(&t.0).unwrap();
  let mut p=PaperPortfolio::new(1_790_000_000_000).unwrap();
  w.commit_modeled_portfolio(None,&p,"BOOT",Inject::None).unwrap();
  for leg in 0..4 {
   let old=p.clone();
   modeled_step(&mut p,leg);
   let inject=if leg==2{Inject::AfterDb2}else{Inject::None};
   let result=w.commit_modeled_portfolio(Some(&old),&p,"BTC",inject);
   if leg==2{
    assert!(result.is_err());w.recover().unwrap();
   }else{result.unwrap();}
   w.verify_pair().unwrap();
  }
  assert_eq!(w.cursor().unwrap().0,5);
  assert_eq!(w.cursor().unwrap().1,0);
  let snapshot=w.quarantined_modeled_state().unwrap().unwrap();
  assert_eq!(snapshot.stats.entries,1);
  assert_eq!(snapshot.stats.futures_hedges,1);
  assert_eq!(snapshot.stats.futures_covers,1);
  assert_eq!(snapshot.stats.closed,1);
  assert_eq!(stmt(&w.db1,"SELECT count(*) FROM audit",&[]).unwrap()[0][0],"5");
  stmt(&w.db2,"UPDATE portfolio_state SET body_hash='corrupt'",&[]).unwrap();
  assert!(w.verify_pair().is_err());
  assert!(w.quarantined_modeled_state().is_err());
 }
 #[test]fn r1174_refuses_legacy_db_on_resume(){
  let t=Temp::new();
  let w=Writer::create(&t.0).unwrap();
  assert_eq!(stmt(&w.db1,"PRAGMA user_version",&[]).unwrap()[0][0],"1205");
  sql(&w.db1,"PRAGMA user_version=1174").unwrap();
  drop(w);
  match Writer::resume(&t.0){
   Err(e)=>assert_eq!(e,"legacy_or_unknown_compact_schema_denied"),
   Ok(_)=>panic!("R1173 schema was incorrectly accepted"),
  }
 }
 #[test]fn r1174_paged_audit_over_5000_records(){
  let t=Temp::new();let mut w=Writer::create(&t.0).unwrap();
  let mut o=owners();let f=input(frame(17),&o);
  w.source_commit(&mut o,&f,&[],Inject::None).unwrap();
  for db in [&w.db1,&w.db2]{
   transaction(db,||{
    for i in 0..5010{
     let id=format!("R1174AUDIT{:05}",i);
     let body=serde_json::json!({
       "paper_only":true,"real_fills_attested":false,"synthetic_test":true,
       "event_number":i}).to_string();
     stmt(db,"INSERT INTO audit VALUES(?,?,?,?,?)",&[
       V::Str(id),V::Str("SIGNAL".into()),V::Num(1),
       V::Str(body.clone()),V::Str(hash(&body))])?;
    }
    Ok(())
   }).unwrap();
  }
  assert_eq!(stmt(&w.db1,"SELECT count(*) FROM audit",&[]).unwrap()[0][0],"5010");
  w.verify_pair().unwrap();
  // A fully valid extra DB2 row beyond the former 5000-row cutoff
  // must still make recovery deny the mirror.
  let body=serde_json::json!({
    "paper_only":true,"real_fills_attested":false,
    "synthetic_test":true,"event_number":5010}).to_string();
  stmt(&w.db2,"INSERT INTO audit VALUES(?,?,?,?,?)",&[
    V::Str("R1174AUDIT05010".into()),V::Str("SIGNAL".into()),
    V::Num(1),V::Str(body.clone()),V::Str(hash(&body))]).unwrap();
  assert!(w.verify_pair().is_err());
  assert!(w.recover().is_err());
 }
 #[test]fn native_rejects_stale_sequence_before_writes(){
  let t=Temp::new();let mut w=Writer::create(&t.0).unwrap();
  let mut o=owners();let f=input(frame(17),&o);
  w.source_commit(&mut o,&f,&[],Inject::None).unwrap();
  let repeated=input(frame(17),&o);
  assert!(w.source_commit(&mut o,&repeated,&[],Inject::None).is_err());
  assert_eq!(w.cursor().unwrap().1,1);
  w.verify_pair().unwrap();
 }
 #[test]fn native_keeps_changed_ns_for_same_depth(){
  let t=Temp::new();let mut w=Writer::create(&t.0).unwrap();
  let mut o=owners();let f=input(frame(17),&o);
  w.source_commit(&mut o,&f,&[],Inject::None).unwrap();
  let first=w.book_quarantined(Venue::BinanceFutures,"BTC").unwrap().unwrap();
  let second=input(frame(18),&o);
  w.source_commit(&mut o,&second,&[],Inject::None).unwrap();
  let next=w.book_quarantined(Venue::BinanceFutures,"BTC").unwrap().unwrap();
  assert_eq!(next.changed_ns,first.changed_ns);
  assert!(next.received_ns>first.received_ns);
  assert_eq!(w.recover().unwrap().1,2);
 }
 #[test]fn native_durable_source_reset_invalidation_crash_recovery(){
  let t=Temp::new();let mut w=Writer::create(&t.0).unwrap();
  let mut o=owners();let f=input(frame(17),&o);
  w.source_commit(&mut o,&f,&[],Inject::None).unwrap();
  let i=f[0].2;
  let bases=vec!["BTC".to_string()];
  assert!(w.source_reset(&mut o,Venue::BinanceFutures,&bases,2,
    1_790_000_010_000,"PUBLIC_WS_RESET",Inject::AfterDb1).is_err());
  assert!(o[i].snapshot_venue("BTC",Venue::BinanceFutures).is_none());
  assert!(w.book_quarantined(Venue::BinanceFutures,"BTC").unwrap().is_none());
  drop(w);
  let mut w=Writer::resume(&t.0).unwrap();
  w.recover().unwrap();
  w.verify_pair().unwrap();
  assert_eq!(stmt(&w.db2,"SELECT epoch FROM epochs",&[]).unwrap()[0][0],"2");
  let mut f2=frame(18);f2.epoch=2;
  let input2=input(f2,&o);
  w.source_commit(&mut o,&input2,&[],Inject::None).unwrap();
  assert_eq!(w.recover().unwrap().1,2);
  assert_eq!(w.book_quarantined(Venue::BinanceFutures,"BTC").unwrap().unwrap().epoch,2);
  assert!(!w.may_resume_old_book_as_fresh());
 }
 #[test]fn native_rejects_corrupt_mirror_and_unauthorized_root(){
  let t=Temp::new();let mut w=Writer::create(&t.0).unwrap();
  let mut o=owners();let f=input(frame(17),&o);
  w.source_commit(&mut o,&f,&[],Inject::None).unwrap();
  stmt(&w.db2,"UPDATE latest SET book_hash='corrupt'",&[]).unwrap();
  assert!(w.verify_pair().is_err());
  assert!(Writer::create(Path::new("/tmp")).is_err());
 }
 #[test]fn native_protects_immutable_modeled_four_leg_audit(){
  let t=Temp::new();let mut w=Writer::create(&t.0).unwrap();
  let mut o=owners();let f=input(frame(17),&o);
  let stages=["SPOT_BUY_MODELED","FUTURES_SHORT_MODELED",
   "FUTURES_COVER_MODELED","SPOT_SELL_MODELED"];
  let proofs:Vec<PaperWitness>=stages.iter().enumerate().map(|(i,k)|
   PaperWitness{
    id:format!("SIMULATEDR1173LEG{}",i+1),
    kind:k.to_string(),
    body:serde_json::json!({"paper_only":true,
     "real_fills_attested":false,"modeled":true,
     "source":"r1173_offline","price":100.0+i as f64,
     "qty":1.25,"fee_bps":5.0})}).collect();
  assert!(w.source_commit(&mut o,&f,&proofs,Inject::AfterDb1).is_err());
  w.recover().unwrap();
  assert_eq!(stmt(&w.db1,"SELECT id FROM audit",&[]).unwrap().len(),4);
  assert_eq!(stmt(&w.db2,"SELECT id FROM audit",&[]).unwrap().len(),4);
  assert!(stmt(&w.db1,"UPDATE audit SET kind='fake'",&[]).is_err());
  assert!(stmt(&w.db2,"DELETE FROM audit",&[]).is_err());
  let rogue=PaperWitness{id:"REALFILL".into(),kind:"REAL_FILL".into(),
    body:serde_json::json!({"paper_only":false,"real_fills_attested":true})};
  let f2=input(frame(18),&o);
  assert!(w.source_commit(&mut o,&f2,&[rogue],Inject::None).is_err());
  assert_eq!(w.cursor().unwrap().1,1);
 }
}

#[cfg(test)]
mod actual_r1162_fixture_test{
 use super::*;
 use std::time::{Instant,SystemTime,UNIX_EPOCH};
 use std::os::unix::fs::DirBuilderExt;
 const SOURCE:&str="/mnt/spotarb_db_test/astra_p001_r874_test_scratch_20261025/astra_r671_file_r1087_R1119_R1162_8JVQgM/r671-private-db1.sqlite";
 const REFERENCE:&str="/mnt/spotarb_db_test/astra_p001_r874_test_scratch_20261025/r1171_compact_market_recovery_fixture_20261028.json";
 // Explicit ignored test because replay includes 59,560 real public frames
 // and 2,289 fsync-confirmed DB1+DB2 transactions.
 #[test]
 #[ignore]
 fn r1173_native_writer_actual_59560_original_frames(){
  // Match original R1119 operator's exact R1121 explicit OKX full
  // books5 snapshot maintenance-reset admission (not a general relaxation).
  std::env::set_var("ASTRA_R1121_BOOKS5_SNAPSHOT_RESET","YES");
  std::env::set_var("ASTRA_R1119_FULL_RUST_PAPER","YES");
  let guarded=std::env::var("SPOTARB_R1182_OFFLINE_GUARDED_MEASURE")
    .ok().as_deref()==Some("YES");
  let path=CString::new(SOURCE).unwrap();let mut ptr=std::ptr::null_mut();
  let flags=1|0x10000|0x01000000;
  assert_eq!(unsafe{sqlite3_open_v2(path.as_ptr(),&mut ptr,flags,
    std::ptr::null())},SQLITE_OK);
  let src=Db(ptr);
  let name=format!("spotarb_r1173_R1162_{}_{}",std::process::id(),
   SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos());
  let root=std::env::temp_dir().join(name);
  fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
  let t0=Instant::now();let mut w=Writer::create(&root).unwrap();
  let mut owners:Vec<Owner>=(0..12).map(|i|Owner::new(i,12,800).unwrap()).collect();
  let mut frames:Vec<(RawFrame,Update,usize)>=Vec::new();
  let mut batches=0u64;let mut processed=0u64;
  let mut inferred_reset_batches=0u64;
  for page in 0..60u64{
   let q=format!("SELECT event_id,payload_json,content_hash FROM events
     ORDER BY event_id LIMIT 1000 OFFSET {}",page*1000);
   let rows=stmt(&src,&q,&[]).unwrap();
   if rows.is_empty(){break}
   for r in rows{
    assert_eq!(hash(&r[1]),r[2]);
    let original:serde_json::Value=serde_json::from_str(&r[1]).unwrap();
    assert_eq!(original["schema"],"SPOTARB/NATIVE/PAPER-HISTORY/R824/V1");
    assert_eq!(original["paper_only"],true);
    let v=match original["venue"].as_str().unwrap(){
      "okx_public_spot"=>Venue::OkxSpot,
      "binance_public_futures"=>Venue::BinanceFutures,
      _=>panic!("unknown venue")};
    let raw=RawFrame{source:None,
      venue:v,epoch:original["epoch"].as_u64().unwrap(),
      received_ns:original["received_ns"].as_u64().unwrap(),
      received_wall_ms:original["received_wall_ms"].as_i64().unwrap(),
      raw:original["raw"].as_str().unwrap().into()};
    let update=crate::parse(&raw).unwrap();
    assert_eq!(update.base,original["base"].as_str().unwrap());
    let index=owners.iter().position(|owner|owner.owns(&update.base)).unwrap();
    let size=original["batch_size"].as_u64().unwrap() as usize;
    let idx=r[0].rsplit('_').next().unwrap().parse::<usize>().unwrap();
    assert_eq!(idx,frames.len());
    frames.push((raw,update,index));
    processed+=1;
    if frames.len()==size{
      // R1162 original events contain frame epochs but omit the
      // corresponding WS Reset event. Reconstruct epoch boundaries
      // for OFFLINE comparison only; label evidence INFERRED.
      let mut groups=BTreeMap::<(String,u64),(Vec<String>,i64)>::new();
      for (f,u,i) in &frames{
       let prior=owners[*i].snapshot_venue(&u.base,f.venue);
       if prior.as_ref().is_some_and(|b|b.epoch<f.epoch){
        let entry=groups.entry((ven(f.venue).to_owned(),f.epoch))
          .or_insert((Vec::new(),f.received_wall_ms));
        if !entry.0.contains(&u.base){entry.0.push(u.base.clone());}
       }
      }
      for ((venue,epoch),(bases,ms)) in groups{
       let v=if venue=="binance_public_futures"{
        Venue::BinanceFutures
       }else{Venue::OkxSpot};
       w.source_reset(&mut owners,v,&bases,epoch,ms,
         "R1162_INFERRED_OFFLINE",Inject::None)
         .unwrap_or_else(|e|panic!("inferred source reset failed: {e}"));
       inferred_reset_batches+=1;
      }
      let committed=if guarded{
       w.source_commit_guarded(&mut owners,&frames,&[],Inject::None)
      }else{
       w.source_commit(&mut owners,&frames,&[],Inject::None)
      };
      if let Err(e)=committed{
        for (f,u,i) in &frames{
         let prior=owners[*i].snapshot_venue(&u.base,f.venue);
         eprintln!("R1173_BATCH_DIAGNOSTIC batch={} base={} venue={:?} now_epoch={} now_seq={:?} now_E={:?} prior_epoch={:?} prior_seq={:?} prior_E={:?}",
           batches+1,u.base,f.venue,f.epoch,u.book.sequence,u.book.exchange_ts_ms,
           prior.as_ref().map(|x|x.epoch),prior.as_ref().and_then(|x|x.sequence),
           prior.as_ref().and_then(|x|x.exchange_ts_ms));
        }
        panic!("R1162 source batch {} failed: {}",batches+1,e);
      }
      if guarded{
       // R1182 STORAGE COST ONLY: synthetic receipt hashes.
       // No DecisionCore or virtual trade is executed or certified.
       let synthetic=hash("R1182/STORAGE-ONLY/NO-DECISION-CALCULATION");
       for (f,_,_) in &frames{
        w.note_guarded_decision(f,"EVALUATED",&synthetic)
          .expect("R1182 synthetic storage receipt");
       }
       w.require_decisions_drained().unwrap();
      }
      batches+=1;frames.clear();
    }
   }
  }
  assert!(frames.is_empty());
  assert_eq!(processed,59560);assert_eq!(batches,2289);
  assert!(inferred_reset_batches>0);
  w.verify_pair().unwrap();assert_eq!(w.recover().unwrap().1,59560);
  if guarded {
   let total=stmt(&w.db1,
     "SELECT sealed_batches,sealed_frames FROM decision_totals",&[]).unwrap();
   assert_eq!(total[0],vec!["2289","59560"]);
   assert!(read_due(&w.db1).unwrap().is_none());
   assert!(read_due(&w.db2).unwrap().is_none());
  }

  let fx:serde_json::Value=serde_json::from_slice(&fs::read(REFERENCE).unwrap()).unwrap();
  let entries=fx["entries"].as_array().unwrap();
  assert_eq!(entries.len(),466);
  let mut matched=0usize;
  for entry in entries{
   let venue=match entry["venue"].as_str().unwrap(){
    "OkxSpot"=>Venue::OkxSpot,"BinanceFutures"=>Venue::BinanceFutures,
    _=>panic!("fixture unknown venue")};
   let base=entry["base"].as_str().unwrap();
   let expected:Book=serde_json::from_value(entry["book"].clone()).unwrap();
   let actual=w.book_quarantined(venue,base).unwrap().unwrap();
   assert_eq!(actual,expected,"R1173 differs from frozen R1171 {base} {:?}",venue);
   matched+=1;
  }
  let physical=fs::read_dir(&root).unwrap().filter_map(Result::ok)
   .filter(|x|x.file_name().to_string_lossy().starts_with("db"))
   .map(|x|x.metadata().unwrap().len()).sum::<u64>();
  sql(&w.db1,"PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
  sql(&w.db2,"PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
  w.verify_pair().unwrap();
  let checkpointed=fs::read_dir(&root).unwrap().filter_map(Result::ok)
   .filter(|x|x.file_name().to_string_lossy().starts_with("db"))
   .map(|x|x.metadata().unwrap().len()).sum::<u64>();
  println!("R1174_ACTUAL_R1162_RUST_PASS frames={} original_batches={} inferred_reset_batches={} native_tx={} books={} active_bytes={} checkpointed_bytes={} elapsed_seconds={:.3}",
   processed,batches,inferred_reset_batches,w.cursor().unwrap().0,matched,physical,checkpointed,t0.elapsed().as_secs_f64());
  if guarded{
   use r1183_probe::*;
   use std::sync::atomic::Ordering;
   println!("R1183_TEST_ONLY_DB_STAGES_MS src_db1={:.3} src_db2={:.3} src_ack={:.3} seal_db1={:.3} seal_db2={:.3} seal_ack={:.3}",
     SRC_DB1_NS.load(Ordering::Relaxed)as f64/1e6,
     SRC_DB2_NS.load(Ordering::Relaxed)as f64/1e6,
     SRC_ACK_NS.load(Ordering::Relaxed)as f64/1e6,
     SEAL_DB1_NS.load(Ordering::Relaxed)as f64/1e6,
     SEAL_DB2_NS.load(Ordering::Relaxed)as f64/1e6,
     SEAL_ACK_NS.load(Ordering::Relaxed)as f64/1e6);
  }
  assert!(!w.may_trade_real()&&!w.may_resume_old_book_as_fresh());
  drop(w);drop(src);
  fs::remove_dir_all(root).unwrap();
 }
}

#[path="ingress_store_r1205.rs"]
pub mod ingress_store_r1205;

impl Writer {
 pub fn decision_boundary_r1324a(&self,frame:&RawFrame,generation:u64)->Result<crate::native_paper_reconciliation_r1324a::DecisionBoundary,String>{
  self.verify_pair()?;
  if generation==0||!stmt(&self.db1,"SELECT seq FROM pending",&[])?.is_empty(){return Err("R1324A_SOURCE_ACK_UNPROVEN".into())}
  let (sequence,hashes)=read_due(&self.db1)?.ok_or("R1324A_NO_DECISION_DUE")?;
  if hashes!=vec![decision_frame_identity(frame)]{return Err("R1324A_SOURCE_IDENTITY_MISMATCH".into())}
  let h=head(&self.db1)?;
  if h.0!=sequence{return Err("R1324A_SOURCE_BOUNDARY_MISMATCH".into())}
  Ok(crate::native_paper_reconciliation_r1324a::DecisionBoundary{canonical:self.root.canonicalize().map_err(|e|e.to_string())?.to_string_lossy().into_owned(),source_sequence:sequence,source_chain:h.2,symbol:decision_symbol_r1324a(frame)?,epoch:frame.epoch,input_hash:hashes[0].clone(),generation})
 }
 pub fn reconcile_execution_completion_r1324a(&mut self,owner:&crate::native_paper_execution_r1289::DurablePaper)->Result<&'static str,String>{
  self.verify_pair().map_err(|e|format!("R1324A_DB_PAIR_DIVERGENCE:{e}"))?;
  if !stmt(&self.db1,"SELECT seq FROM pending",&[])?.is_empty(){return Err("R1324A_ACK_DIVERGENCE".into())}
  let frames=self.quarantined_decision_inputs()?;
  let history=owner.completion_history_r1324a().map_err(|e|format!("R1324A_EXECUTION_HISTORY_UNAVAILABLE:{e:?}"))?;
  let totals=one(&self.db1,"SELECT sealed_batches,sealed_frames,proof_chain FROM decision_totals WHERE id=1",&[])?.ok_or("R1324A_DECISION_TOTALS_MISSING")?;
  let batches=totals[0].parse::<u64>().map_err(|_|"R1324A_BAD_SEALED_BATCHES")?;
  let sealed=totals[1].parse::<u64>().map_err(|_|"R1324A_BAD_SEALED_FRAMES")?;
  if history.uncompleted_effects{return Err("R1324A_UNCOMPLETED_EXECUTION_TAIL".into())}
  if let Some(last)=&history.last{if last.boundary.canonical!=self.root.canonicalize().map_err(|e|e.to_string())?.to_string_lossy(){return Err("R1324A_CANONICAL_SCOPE_MISMATCH".into())}}
  if frames.is_empty(){
   if batches!=history.count||sealed!=history.count||totals[2]!=history.chain{return Err("R1324A_SEALED_EXECUTION_HISTORY_DIVERGENCE".into())}
   self.require_decisions_drained()?;return Ok("NO_PENDING_DECISION")
  }
  if batches.checked_add(1)!=Some(history.count)||sealed!=batches||totals[2]!=history.prior_chain{return Err("R1324A_PENDING_EXECUTION_HISTORY_DIVERGENCE".into())}
  let r=owner.completion_r1324a().map_err(|e|format!("R1324A_EXECUTION_PROOF_UNAVAILABLE:{e:?}"))?;
  if frames.len()!=1{return Err("R1324A_UNSUPPORTED_LEGACY_BATCH".into())}
  let boundary=self.decision_boundary_r1324a(&frames[0],r.boundary.generation)?;
  if boundary!=r.boundary{return Err("R1324A_JOURNAL_SEAL_MISMATCH".into())}
  let receipt=DecisionReceipt{input_hash:r.boundary.input_hash,outcome:"EVALUATED".into(),result_hash:r.result_hash};
  // seal_decision_batch independently revalidates durable DB1/DB2/ACK boundaries.
  self.seal_decision_batch(&[receipt],Inject::None)?;
  self.cold_unresolved_decision=false;self.receipts.clear();self.expected_frame_hashes.clear();
  self.require_decisions_drained()?;Ok("EXACT_EXECUTION_COMPLETION_SEALED")
 }
}

fn decision_symbol_r1324a(frame:&RawFrame)->Result<String,String>{
 if frame.venue==Venue::OkxSpot{
  // A validated L2 delta need not be parseable as a standalone full book.
  // Read only instrument scope; the receipt's hash binds the complete durable frame.
  let value:serde_json::Value=serde_json::from_str(&frame.raw).map_err(|_|"R1324A_SOURCE_JSON")?;
  let base=value["arg"]["instId"].as_str().and_then(|s|s.strip_suffix("-USDT")).ok_or("R1324A_SOURCE_SYMBOL")?;
  if base.is_empty()||!base.bytes().all(|b|b.is_ascii_uppercase()||b.is_ascii_digit()){return Err("R1324A_SOURCE_SYMBOL".into())}Ok(base.into())
 }else{Ok(crate::parse(frame).map_err(|e|format!("R1324A_SOURCE_PARSE:{e:?}"))?.base)}
}

#[cfg(test)]
mod r1326_trade_intent_writer_tests{
 use super::*;
 use std::{os::unix::fs::DirBuilderExt,time::{SystemTime,UNIX_EPOCH}};
 fn root()->PathBuf{
  let n=SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
  let p=std::env::temp_dir().join(format!("spotarb_r1173_r1326_{}_{}",std::process::id(),n));
  fs::DirBuilder::new().mode(0o700).create(&p).unwrap();p
 }
 fn intent(trade_id:&str)->PaperWitness{
  PaperWitness{id:format!("R1326T{}",hash(trade_id)),kind:"TRADE_INTENT".into(),
   body:serde_json::json!({"schema":"SPOTARB/R1326/TRADE-INTENT/V1",
    "trade_id":trade_id,"risk_passed":true,"generation":7,
    "paper_only":true,"real_fills_attested":false,
    "signal_evidence":{"strategy":"fixture","instrument":"AAAUSDT"},
    "risk_decision":{"allowed":true}})}
 }
 fn event(trade_id:&str,event_id:&str)->PaperWitness{
  PaperWitness{id:format!("R1326E{}",hash(&format!("{trade_id}:{event_id}"))),kind:"TRADE_EVENT".into(),
   body:serde_json::json!({"schema":"SPOTARB/R1326/TRADE-EVENT/V1",
    "trade_id":trade_id,"event_id":event_id,"paper_only":true,
    "real_fills_attested":false,"event":{"kind":"OrderSubmitted"}})}
 }
 #[test]fn legacy_partial_unknown_and_census_without_event_instrument_keep_identity(){
  for kind in ["PartialFill","Unknown","OrderCensus"]{
   let record=serde_json::json!({"event":{"kind":kind,"simulated":true,"order_id":"owner",
    "census_order_ids":["legacy-order-1"],"intent":null},
    "lifecycle":{"orders":{"legacy-order-1":{"status":kind}},"state":"RecoveryRequired"},
    "isolated":{"identity":{"run_id":"legacy-run","profile":{"base":"AAA"}}}});
   assert!(legacy_source_record_has_identity_and_order(&record),"{kind}");
  }
  let missing_identity=serde_json::json!({"event":{"kind":"PartialFill","simulated":true,
    "order_id":"legacy-order-1","intent":null},"lifecycle":{"orders":{"legacy-order-1":{}}}});
  assert!(!legacy_source_record_has_identity_and_order(&missing_identity));
 }
 #[test]fn legacy_execution_recovery_is_distinct_and_idempotent_after_db1_crash(){
  let root=root();let journal=root.join("legacy-execution.jsonl");
  fs::write(&journal,b"{\"fixture\":true}\n").unwrap();
  let record=serde_json::json!({"sequence":1,"event":{"kind":"Accepted","simulated":true,"order_id":"legacy-order-1","intent":{"instrument":"AAAUSDT","order_id":"legacy-order-1"}},
   "lifecycle":{"orders":{"legacy-order-1":{"status":"Open"}}},"isolated":{"identity":{"run_id":"legacy-run","profile":{"base":"AAA"}}}});
  let record_json=serde_json::to_string(&record).unwrap();
  let source_journal_sha256=hash("{\"fixture\":true}\n");
  let source_record_sha256=hash(&record_json);
  let body=serde_json::json!({"schema":"SPOTARB/R1326/LEGACY-RECOVERY/V1","status":"RECONCILIATION_REQUIRED",
   "paper_only":true,"real_fills_attested":false,"evidence":{"source_kind":"R1289_EXECUTION_JOURNAL",
   "source_journal_path":journal.to_string_lossy(),"source_journal_sha256":source_journal_sha256,
   "source_sequence":1,"source_record_sha256":source_record_sha256,"source_record":record}});
  let id=format!("R1326L{}",hash(&format!("{}:1:{source_record_sha256}",body["evidence"]["source_journal_sha256"].as_str().unwrap())));
  let witness=PaperWitness{id,kind:"LEGACY_RECOVERY".into(),body};
  {let mut w=Writer::create(&root).unwrap();
   assert!(w.commit_legacy_recovery_witness_r1326(witness.clone(),Inject::AfterDb1).is_err());}
  {let mut w=Writer::resume(&root).unwrap();
   assert_eq!(w.commit_legacy_recovery_witness_r1326(witness.clone(),Inject::None).unwrap(),1);
   assert_eq!(w.commit_legacy_recovery_witness_r1326(witness,Inject::None).unwrap(),1);
   assert_eq!(w.legacy_recovery_count_r1326().unwrap(),1);
   assert_eq!(w.trade_counts_r1326().unwrap(),(0,0));
   let rows=stmt(&w.db1,"SELECT body FROM audit WHERE kind='LEGACY_RECOVERY'",&[]).unwrap();
   let body:serde_json::Value=serde_json::from_str(&rows[0][0]).unwrap();
   assert_eq!(body["status"],"RECONCILIATION_REQUIRED");
   assert!(body.get("trade_id").is_none());
   assert_eq!(head(&w.db1).unwrap(),head(&w.db2).unwrap());
  }
  let verified_root=Path::new("/mnt/spotarb_db_test/r1273_r1275_engine_latency_20261111/R1289/lab")
   .join(format!("r1326-verified-source-{}",std::process::id()));
  fs::create_dir(&verified_root).unwrap();
  let record2=serde_json::json!({"sequence":2,"event":{"kind":"PartialFill","simulated":true,"order_id":"legacy-order-1","intent":null},
   "lifecycle":{"orders":{"legacy-order-1":{"status":"PartiallyFilled"}}},"isolated":{"identity":{"run_id":"legacy-run","profile":{"base":"AAA"}}}});
  let record2_json=serde_json::to_string(&record2).unwrap();
  let source_journal_sha256=hash("{\"fixture\":true}\n{\"fixture2\":true}\n");
  let source_record_sha256=hash(&record2_json);
  let body2=serde_json::json!({"schema":"SPOTARB/R1326/LEGACY-RECOVERY/V1","status":"RECONCILIATION_REQUIRED",
   "paper_only":true,"real_fills_attested":false,"evidence":{"source_kind":"R1289_EXECUTION_JOURNAL",
   "source_journal_path":journal.to_string_lossy(),"source_journal_sha256":source_journal_sha256,
   "source_sequence":2,"source_record_sha256":source_record_sha256,"source_record":record2}});
  let id2=format!("R1326L{}",hash(&format!("{}:2:{source_record_sha256}",body2["evidence"]["source_journal_sha256"].as_str().unwrap())));
  let witness2=PaperWitness{id:id2,kind:"LEGACY_RECOVERY".into(),body:body2};
  {let mut w=Writer::resume(&root).unwrap();
   assert!(w.commit_legacy_recovery_witness_r1326(witness2.clone(),Inject::AfterDb2).is_err());}
  {let mut w=Writer::resume(&root).unwrap();
   assert_eq!(w.commit_legacy_recovery_witness_r1326(witness2,Inject::None).unwrap(),2);
   assert_eq!(w.legacy_recovery_count_r1326().unwrap(),2);
   assert_eq!(w.trade_counts_r1326().unwrap(),(0,0));
   w.verify_pair().unwrap();
  }
  let owner=crate::native_paper_execution_r1289::DurablePaper::create(
   verified_root.join("execution.jsonl"),
   crate::native_paper_risk_lifecycle_r1287::Lifecycle::new(Default::default()).unwrap()).unwrap();
  let forged=serde_json::json!({"event":{"intent":{"instrument":"AAAUSDT"}}});
  let mut w=Writer::resume(&root).unwrap();
  assert!(w.record_legacy_execution_recovery_r1326(&owner,1,forged,Inject::None).unwrap_err().contains("NOT_VERIFIED"));
  drop(w);drop(owner);fs::remove_dir_all(verified_root).unwrap();fs::remove_dir_all(root).unwrap();
 }
 #[test]fn trade_intent_mirrors_after_db1_crash_and_replay_is_idempotent(){
  let root=root();let trade="fixture-trade-0001";
  {let mut w=Writer::create(&root).unwrap();
   assert!(w.commit_trade_intent_r1326(intent(trade),Inject::AfterDb1).is_err());}
  {let mut w=Writer::resume(&root).unwrap();
   assert_eq!(w.commit_trade_intent_r1326(intent(trade),Inject::None).unwrap(),1);
   assert_eq!(w.commit_trade_intent_r1326(intent(trade),Inject::None).unwrap(),1);
   assert_eq!(head(&w.db1).unwrap(),head(&w.db2).unwrap());
   assert_eq!(w.verify_pair().unwrap(),());
   assert_eq!(w.commit_trade_event_r1326(event(trade,"order-submitted-1"),Inject::None).unwrap(),2);
   assert_eq!(w.commit_trade_event_r1326(event(trade,"order-submitted-1"),Inject::None).unwrap(),2);
   assert!(w.commit_trade_event_r1326(event("missing-trade","event-1"),Inject::None).is_err());
  }
  fs::remove_dir_all(root).unwrap();
 }
}
