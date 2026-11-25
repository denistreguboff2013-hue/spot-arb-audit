//! Astra R147–R150: OFFLINE, ISOLATED transactional SQLite mirror model.
//! Uses system libsqlite3.so.0, no Cargo dependency. Only private /tmp test DBs.
//! NOT wired to legacy DB1, active PAPER, exchange, DB2 service or trade permits.
use crate::native_mirror_outbox_protocol_r139_review::{
 audit_outbox_chain,MirrorOutboxRowR139,
};
use std::{ffi::{CString,c_char,c_int,c_void},fs,path::{Path,PathBuf},
 os::unix::fs::MetadataExt};
use serde_json;
const SQLITE_OK:c_int=0;
const SQLITE_ROW:c_int=100;
const SQLITE_DONE:c_int=101;
const READONLY:c_int=1|0x10000|0x01000000;
const READWRITE_CREATE:c_int=2|4|0x10000|0x01000000;
const READWRITE_EXISTING:c_int=2|0x10000|0x01000000;
#[link(name="libsqlite3.so.0",kind="dylib",modifiers="+verbatim")]
extern "C"{
 fn sqlite3_open_v2(filename:*const c_char,db:*mut *mut c_void,
  flags:c_int,zvfs:*const c_char)->c_int;
 fn sqlite3_close(db:*mut c_void)->c_int;
 fn sqlite3_busy_timeout(db:*mut c_void,millis:c_int)->c_int;
 fn sqlite3_exec(db:*mut c_void,sql:*const c_char,cb:*const c_void,
  arg:*mut c_void,err:*mut *mut c_char)->c_int;
 fn sqlite3_prepare_v2(db:*mut c_void,sql:*const c_char,
  len:c_int,stmt:*mut *mut c_void,tail:*mut *const c_char)->c_int;
 fn sqlite3_bind_int64(stmt:*mut c_void,index:c_int,val:i64)->c_int;
 fn sqlite3_bind_text(stmt:*mut c_void,index:c_int,text:*const c_char,
  n:c_int,destructor:Option<unsafe extern "C" fn(*mut c_void)>)->c_int;
 fn sqlite3_step(stmt:*mut c_void)->c_int;
 fn sqlite3_column_int64(stmt:*mut c_void,index:c_int)->i64;
 fn sqlite3_column_text(stmt:*mut c_void,index:c_int)->*const u8;
 fn sqlite3_column_bytes(stmt:*mut c_void,index:c_int)->c_int;
 fn sqlite3_finalize(stmt:*mut c_void)->c_int;
 fn sqlite3_changes(db:*mut c_void)->c_int;
 fn sqlite3_extended_errcode(db:*mut c_void)->c_int;
}
#[derive(Debug,Clone,Copy,PartialEq,Eq)]
pub enum MirrorDbError{Path,Sql,Source,Cursor,Duplicate,Schema,Corrupt}
pub struct SqliteMirrorReview{db1:PathBuf,db2:PathBuf}
#[derive(Debug)]
pub struct MirrorApplyR148{pub seq:u64,pub rows:usize,pub may_trade:bool,pub production_db2:bool}
struct Db(*mut c_void);
impl Drop for Db{fn drop(&mut self){unsafe{sqlite3_close(self.0);}}}
fn name_guard(path:&Path,expected:&str)->Result<(),MirrorDbError>{
 if path.file_name().and_then(|x|x.to_str())!=Some(expected){
  return Err(MirrorDbError::Path)
 }
 let parent=path.parent().ok_or(MirrorDbError::Path)?;
 if parent.parent()!=Some(std::env::temp_dir().as_path())
   ||!parent.file_name().and_then(|x|x.to_str())
     .is_some_and(|x|x.starts_with("astra_r147_isolated_")&&x.len()<=120){
  return Err(MirrorDbError::Path)
 }
 let m=fs::symlink_metadata(parent).map_err(|_|MirrorDbError::Path)?;
 if !m.is_dir()||m.mode()&0o077!=0||m.uid()!=unsafe{libc::geteuid()}{
  return Err(MirrorDbError::Path)
 }
 if let Ok(m)=fs::symlink_metadata(path){
  if !m.file_type().is_file()||m.nlink()!=1||m.uid()!=unsafe{libc::geteuid()}
    ||m.mode()&0o077!=0{return Err(MirrorDbError::Path)}
 }
 Ok(())
}
fn open(path:&Path,flag:c_int)->Result<Db,MirrorDbError>{
 let expected=if path.file_name().and_then(|s|s.to_str())==Some("astra-r147-db1.sqlite"){
  "astra-r147-db1.sqlite"
 }else{"astra-r147-db2.sqlite"};
 name_guard(path,expected)?;
 let s=CString::new(path.to_string_lossy().as_bytes()).map_err(|_|MirrorDbError::Path)?;
 let mut raw=std::ptr::null_mut();
 let rc=unsafe{sqlite3_open_v2(s.as_ptr(),&mut raw,flag,std::ptr::null())};
 if rc!=SQLITE_OK||raw.is_null(){
  if !raw.is_null(){unsafe{sqlite3_close(raw);}}
  return Err(MirrorDbError::Sql)
 }
 let db=Db(raw);
 if unsafe{sqlite3_busy_timeout(db.0,3_000)}!=SQLITE_OK{
  return Err(MirrorDbError::Sql)
 }
 if flag==READWRITE_CREATE{
  exec(&db,"PRAGMA journal_mode=DELETE;PRAGMA synchronous=FULL;")?;
 }
 Ok(db)
}
fn exec(db:&Db,sql:&str)->Result<(),MirrorDbError>{
 let sql=CString::new(sql).map_err(|_|MirrorDbError::Sql)?;
 let rc=unsafe{sqlite3_exec(db.0,sql.as_ptr(),std::ptr::null(),
   std::ptr::null_mut(),std::ptr::null_mut())};
 if rc!=SQLITE_OK{return Err(MirrorDbError::Sql)}
 Ok(())
}
struct Stmt{raw:*mut c_void,texts:Vec<CString>}
impl Drop for Stmt{fn drop(&mut self){unsafe{sqlite3_finalize(self.raw);}}}
impl Stmt{
 fn new(db:&Db,sql:&str)->Result<Self,MirrorDbError>{
  let q=CString::new(sql).map_err(|_|MirrorDbError::Sql)?;
  let mut raw=std::ptr::null_mut();
  let rc=unsafe{sqlite3_prepare_v2(db.0,q.as_ptr(),-1,
    &mut raw,std::ptr::null_mut())};
  if rc!=SQLITE_OK||raw.is_null(){return Err(MirrorDbError::Sql)}
  Ok(Self{raw,texts:vec![]})
 }
 fn number(&mut self,index:i32,n:u64)->Result<(),MirrorDbError>{
  let n=i64::try_from(n).map_err(|_|MirrorDbError::Schema)?;
  if unsafe{sqlite3_bind_int64(self.raw,index,n)}!=SQLITE_OK{
   return Err(MirrorDbError::Sql)
  }
  Ok(())
 }
 fn string(&mut self,index:i32,s:&str)->Result<(),MirrorDbError>{
  self.texts.push(CString::new(s).map_err(|_|MirrorDbError::Schema)?);
  let t=self.texts.last().unwrap();
  let n=i32::try_from(s.len()).map_err(|_|MirrorDbError::Schema)?;
  if unsafe{sqlite3_bind_text(self.raw,index,t.as_ptr(),n,None)}!=SQLITE_OK{
   return Err(MirrorDbError::Sql)
  }
  Ok(())
 }
 fn step(&mut self)->Result<i32,MirrorDbError>{
  let rc=unsafe{sqlite3_step(self.raw)};
  if rc!=SQLITE_DONE&&rc!=SQLITE_ROW{return Err(MirrorDbError::Sql)}
  Ok(rc)
 }
 fn num(&self,index:i32)->Result<u64,MirrorDbError>{
  u64::try_from(unsafe{sqlite3_column_int64(self.raw,index)})
   .map_err(|_|MirrorDbError::Corrupt)
 }
 fn strval(&self,index:i32)->Result<String,MirrorDbError>{
  let p=unsafe{sqlite3_column_text(self.raw,index)};
  if p.is_null(){return Err(MirrorDbError::Corrupt)}
  let n=unsafe{sqlite3_column_bytes(self.raw,index)};
  if n<0{return Err(MirrorDbError::Corrupt)}
  let raw=unsafe{std::slice::from_raw_parts(p,n as usize)};
  Ok(std::str::from_utf8(raw).map_err(|_|MirrorDbError::Corrupt)?.into())
 }
}
impl SqliteMirrorReview{
 pub fn initialize(root:&Path)->Result<Self,MirrorDbError>{
  let db1=root.join("astra-r147-db1.sqlite");
  let db2=root.join("astra-r147-db2.sqlite");
  name_guard(&db1,"astra-r147-db1.sqlite")?;
  name_guard(&db2,"astra-r147-db2.sqlite")?;
  for p in [&db1,&db2]{
   use std::os::unix::fs::OpenOptionsExt;
   let f=fs::OpenOptions::new().write(true).create_new(true)
    .mode(0o600).open(p).map_err(|_|MirrorDbError::Path)?;
   f.sync_all().map_err(|_|MirrorDbError::Sql)?;
  }
  let a=open(&db1,READWRITE_CREATE)?;
  exec(&a,"CREATE TABLE outbox(seq INTEGER PRIMARY KEY,tx_id TEXT UNIQUE NOT NULL,row_json TEXT NOT NULL);    CREATE TABLE control(id INTEGER PRIMARY KEY CHECK(id=1),seq INTEGER NOT NULL);    INSERT INTO control(id,seq) VALUES(1,0);")?;
  let b=open(&db2,READWRITE_CREATE)?;
  exec(&b,"CREATE TABLE events(seq INTEGER PRIMARY KEY,tx_id TEXT UNIQUE NOT NULL,row_json TEXT NOT NULL);    CREATE TABLE cursor(id INTEGER PRIMARY KEY CHECK(id=1),seq INTEGER NOT NULL,     anchor INTEGER NOT NULL,generation TEXT NOT NULL);    INSERT INTO cursor(id,seq,anchor,generation) VALUES(1,0,0,'');")?;
  let dir=fs::File::open(root).map_err(|_|MirrorDbError::Sql)?;
  dir.sync_all().map_err(|_|MirrorDbError::Sql)?;
  Ok(Self{db1,db2})
 }
 pub fn load(root:&Path)->Result<Self,MirrorDbError>{
  let db1=root.join("astra-r147-db1.sqlite");
  let db2=root.join("astra-r147-db2.sqlite");
  let a=open(&db1,READONLY)?;
  let b=open(&db2,READONLY)?;
  // Do not create missing data or treat an empty database as a checkpoint.
  let mut q=Stmt::new(&a,"SELECT seq FROM control WHERE id=1")?;
  if q.step()?!=SQLITE_ROW{return Err(MirrorDbError::Schema)}
  let mut q=Stmt::new(&b,"SELECT seq FROM cursor WHERE id=1")?;
  if q.step()?!=SQLITE_ROW{return Err(MirrorDbError::Schema)}
  Ok(Self{db1,db2})
 }
}
struct Tx<'a>{db:&'a Db,active:bool}
impl<'a> Tx<'a>{
 fn begin(db:&'a Db)->Result<Self,MirrorDbError>{
  exec(db,"BEGIN IMMEDIATE;")?;Ok(Self{db,active:true})
 }
 fn commit(mut self)->Result<(),MirrorDbError>{
  exec(self.db,"COMMIT;")?;self.active=false;Ok(())
 }
}
impl Drop for Tx<'_>{
 fn drop(&mut self){if self.active{let _=exec(self.db,"ROLLBACK;");}}
}
fn cursor(db:&Db,table:&str)->Result<u64,MirrorDbError>{
 let sql=match table{
  "control"=>"SELECT seq FROM control WHERE id=1",
  "cursor"=>"SELECT seq FROM cursor WHERE id=1",
  _=>return Err(MirrorDbError::Schema)
 };
 let mut q=Stmt::new(db,sql)?;
 if q.step()?!=SQLITE_ROW{return Err(MirrorDbError::Schema)}
 let seq=q.num(0)?;
 if q.step()?!=SQLITE_DONE{return Err(MirrorDbError::Corrupt)}
 Ok(seq)
}
fn one_insert(db:&Db,table:&str,row:&MirrorOutboxRowR139)->Result<(),MirrorDbError>{
 let sql=match table{
  "outbox"=>"INSERT INTO outbox(seq,tx_id,row_json) VALUES(?1,?2,?3)",
  "events"=>"INSERT INTO events(seq,tx_id,row_json) VALUES(?1,?2,?3)",
  _=>return Err(MirrorDbError::Schema)
 };
 let mut q=Stmt::new(db,sql)?;
 q.number(1,row.seq)?;q.string(2,&row.tx_id)?;
 q.string(3,&serde_json::to_string(row).map_err(|_|MirrorDbError::Corrupt)?)?;
 if q.step()?!=SQLITE_DONE{return Err(MirrorDbError::Sql)}
 Ok(())
}
impl SqliteMirrorReview{
 /// Synthetic source rows only: strict test-only DB1 filename and parent.
 pub fn append_synthetic_source(&self,row:MirrorOutboxRowR139,now:u64)
  ->Result<(),MirrorDbError>{
  audit_outbox_chain(std::slice::from_ref(&row),row.prev_seq,now)
   .map_err(|_|MirrorDbError::Source)?;
  let db=open(&self.db1,READWRITE_EXISTING)?;
  let tx=Tx::begin(&db)?;
  let current=cursor(&db,"control")?;
  if current!=0&&current!=row.prev_seq{return Err(MirrorDbError::Cursor)}
  if current==0&&row.prev_seq==0{return Err(MirrorDbError::Cursor)}
  one_insert(&db,"outbox",&row)?;
  let mut q=Stmt::new(&db,"UPDATE control SET seq=?1 WHERE id=1 AND seq=?2")?;
  q.number(1,row.seq)?;q.number(2,current)?;
  if q.step()?!=SQLITE_DONE||unsafe{sqlite3_changes(db.0)}!=1{
   return Err(MirrorDbError::Cursor)
  }
  drop(q);tx.commit()
 }
 fn original_row(&self,seq:u64,now:u64)->Result<MirrorOutboxRowR139,MirrorDbError>{
  let db=open(&self.db1,READONLY)?;
  let mut q=Stmt::new(&db,"SELECT row_json FROM outbox WHERE seq=?1")?;
  q.number(1,seq)?;
  if q.step()?!=SQLITE_ROW{return Err(MirrorDbError::Source)}
  let raw=q.strval(0)?;
  if q.step()?!=SQLITE_DONE{return Err(MirrorDbError::Corrupt)}
  let row:MirrorOutboxRowR139=serde_json::from_str(&raw)
   .map_err(|_|MirrorDbError::Corrupt)?;
  if row.seq!=seq{return Err(MirrorDbError::Corrupt)}
  audit_outbox_chain(std::slice::from_ref(&row),row.prev_seq,now)
   .map_err(|_|MirrorDbError::Source)?;
  Ok(row)
 }
}
impl SqliteMirrorReview{
 fn first_available_source_seq(&self)->Result<u64,MirrorDbError>{
  let db=open(&self.db1,READONLY)?;
  let mut q=Stmt::new(&db,"SELECT seq FROM outbox ORDER BY seq ASC LIMIT 1")?;
  if q.step()?!=SQLITE_ROW{return Err(MirrorDbError::Source)}
  q.num(0)
 }
 pub fn apply_offline(&self,seq:u64,now:u64,inject_rollback:bool)
  ->Result<MirrorApplyR148,MirrorDbError>{
  self.apply_offline_phase_r153(seq,now,inject_rollback,None)
 }
 pub fn apply_crash_r153(&self,seq:u64,now:u64,phase:&str)
  ->Result<MirrorApplyR148,MirrorDbError>{
  if !matches!(phase,"after_insert"|"after_cursor"|"after_commit"){
   return Err(MirrorDbError::Schema)
  }
  self.apply_offline_phase_r153(seq,now,false,Some(phase))
 }
 fn apply_offline_phase_r153(&self,seq:u64,now:u64,inject_rollback:bool,
   phase:Option<&str>)->Result<MirrorApplyR148,MirrorDbError>{
  let row=self.original_row(seq,now)?;
  let db=open(&self.db2,READWRITE_EXISTING)?;
  let tx=Tx::begin(&db)?;
  let mut q=Stmt::new(&db,"SELECT seq,anchor,generation FROM cursor WHERE id=1")?;
  if q.step()?!=SQLITE_ROW{return Err(MirrorDbError::Schema)}
  let current=q.num(0)?;let anchor=q.num(1)?;let generation=q.strval(2)?;
  if q.step()?!=SQLITE_DONE{return Err(MirrorDbError::Corrupt)}
  drop(q);
  if current>=row.seq{return Err(MirrorDbError::Duplicate)}
  if current!=0{
   if row.prev_seq!=current||row.feed_generation!=generation
    ||anchor==0{return Err(MirrorDbError::Cursor)}
  }else if anchor!=0||!generation.is_empty()||row.prev_seq==0
    ||self.first_available_source_seq()?!=row.seq{
   // An empty DB2 must begin at the earliest retained DB1 row.
   // This is NOT an independent witness against deleted source rows.
   return Err(MirrorDbError::Cursor)
  }
  one_insert(&db,"events",&row)?;
  r153_pause(phase,"after_insert");
  let mut q=Stmt::new(&db,"UPDATE cursor SET seq=?1,anchor=?2,generation=?3     WHERE id=1 AND seq=?4 AND anchor=?5 AND generation=?6")?;
  q.number(1,row.seq)?;
  q.number(2,if current==0{row.prev_seq}else{anchor})?;
  q.string(3,&row.feed_generation)?;
  q.number(4,current)?;q.number(5,anchor)?;q.string(6,&generation)?;
  if q.step()?!=SQLITE_DONE||unsafe{sqlite3_changes(db.0)}!=1{
   return Err(MirrorDbError::Cursor)
  }
  drop(q);
  r153_pause(phase,"after_cursor");
  if inject_rollback{return Err(MirrorDbError::Sql)}
  tx.commit()?;
  r153_pause(phase,"after_commit");
  let observed=self.review_integrity_r152(now)?;
  Ok(MirrorApplyR148{seq:observed.0,rows:observed.1,
   may_trade:false,production_db2:false})
 }
 pub fn review_destination(&self)->Result<(u64,usize),MirrorDbError>{
  let db=open(&self.db2,READONLY)?;
  let seq=cursor(&db,"cursor")?;
  let mut q=Stmt::new(&db,"SELECT count(*) FROM events")?;
  if q.step()?!=SQLITE_ROW{return Err(MirrorDbError::Schema)}
  let count=usize::try_from(q.num(0)?).map_err(|_|MirrorDbError::Schema)?;
  if count>256{return Err(MirrorDbError::Schema)}
  Ok((seq,count))
 }
}
fn r153_pause(phase:Option<&str>,at:&str){
 if phase==Some(at){
  use std::io::Write;
  println!("R153_PAUSED {at}");
  let _=std::io::stdout().flush();
  loop{std::thread::park();}
 }
}
include!("native_isolated_sqlite_integrity_r152_review.rs");
include!("native_isolated_sqlite_witness_r154_review.rs");
include!("native_isolated_sqlite_batch_r160_review.rs");
include!("native_paper_snapshot_r158_review.rs");
include!("native_paper_sqlite_adapter_r156_review.rs");
include!("native_isolated_sqlite_witness_gate_r164_review.rs");
include!("native_isolated_external_floor_r171_review.rs");
include!("native_isolated_partial_recovery_r180_review.rs");
include!("native_floor_independent_contract_r177_review.rs");
include!("native_session_rollover_r181_review.rs");
include!("native_gate_matrix_r182_review.rs");
include!("native_sigkill_r178_review.rs");
include!("native_isolated_sigkill_integration_r178_review.rs");
include!("native_remote_floor_protocol_r183_review.rs");
include!("native_floor_lock_forensics_r185_review.rs");
include!("native_partial_attack_r186_review.rs");
include!("native_bounded_paper_queue_r187_review.rs");
include!("native_sqlite_perf_profile_r188_review.rs");
include!("native_exchange_evidence_gates_r189_review.rs");
include!("native_attestor_trust_boundary_r191_review.rs");
include!("native_witness_transport_r192_review.rs");
include!("native_floor_lock_identity_r193_review.rs");
include!("native_persisted_paper_queue_r195_review.rs");
include!("native_sqlite_source_batch_r196_review.rs");
include!("native_two_venue_fill_consistency_r197_review.rs");
include!("native_two_venue_zero_window_r198_review.rs");
include!("native_signed_floor_bridge_r199_review.rs");
include!("native_isolated_analytics_collector_r296_review.rs");
include!("native_floor_cold_bridge_r200_review.rs");
include!("native_queue_remote_anchor_r201_review.rs");
include!("native_queue_powerfail_r202_review.rs");
include!("native_floor_queue_ack_r203_review.rs");
include!("native_source_batch_profile_r204_review.rs");
include!("native_order_fill_reconciliation_r205_review.rs");
include!("native_account_page_coverage_r206_review.rs");
#[cfg(test)]
mod tests{
 use super::*;
 use std::{os::unix::fs::DirBuilderExt,sync::atomic::{AtomicUsize,Ordering}};
 static COUNTER:AtomicUsize=AtomicUsize::new(0);
 const NOW:u64=1_791_784_000_000;
 fn setup()->(PathBuf,SqliteMirrorReview){
  let root=std::env::temp_dir().join(format!(
   "astra_r147_isolated_{}_{}",std::process::id(),
   COUNTER.fetch_add(1,Ordering::Relaxed)));
  fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
  let db=SqliteMirrorReview::initialize(&root).unwrap();
  (root,db)
 }
 fn row(seq:u64,prev:u64)->MirrorOutboxRowR139{
  let mut a:MirrorOutboxRowR139=serde_json::from_str(include_str!(
   "../config/native_mirror_outbox_python_fixture_r141_review.json")).unwrap();
  a.seq=seq;a.prev_seq=prev;a.tx_id=format!("tx-{seq:032x}");a
 }
 #[test]fn r147_two_actual_sqlite_files_and_cold_recovery(){
  let (root,db)=setup();
  db.append_synthetic_source(row(43,42),NOW).unwrap();
  db.append_synthetic_source(row(100,43),NOW).unwrap();
  assert_eq!(db.review_destination().unwrap(),(0,0));
  let a=db.apply_offline(43,NOW,false).unwrap();
  assert_eq!((a.seq,a.rows),(43,1));assert!(!a.may_trade&&!a.production_db2);
  assert_eq!(db.apply_offline(100,NOW,false).unwrap().rows,2);
  let cold=SqliteMirrorReview::load(&root).unwrap();
  assert_eq!(cold.review_destination().unwrap(),(100,2));
  fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r148_rollback_before_commit_leaves_both_rows_and_cursor_empty(){
  let (root,db)=setup();
  db.append_synthetic_source(row(43,42),NOW).unwrap();
  assert_eq!(db.apply_offline(43,NOW,true).err(),Some(MirrorDbError::Sql));
  assert_eq!(db.review_destination().unwrap(),(0,0));
  let cold=SqliteMirrorReview::load(&root).unwrap();
  assert_eq!(cold.review_destination().unwrap(),(0,0));
  assert_eq!(cold.apply_offline(43,NOW,false).unwrap().rows,1);
  fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r149_duplicate_and_out_of_order_source_deny(){
  let (root,db)=setup();
  db.append_synthetic_source(row(43,42),NOW).unwrap();
  db.append_synthetic_source(row(100,43),NOW).unwrap();
  assert_eq!(db.apply_offline(100,NOW,false).err(),Some(MirrorDbError::Cursor));
  assert_eq!(db.review_destination().unwrap(),(0,0));
  db.apply_offline(43,NOW,false).unwrap();
  assert_eq!(db.apply_offline(43,NOW,false).err(),Some(MirrorDbError::Duplicate));
  assert_eq!(db.review_destination().unwrap(),(43,1));
  db.apply_offline(100,NOW,false).unwrap();
  fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r149_tampered_original_sqlite_row_denied(){
  let (root,db)=setup();
  db.append_synthetic_source(row(43,42),NOW).unwrap();
  {let a=open(&db.db1,READWRITE_CREATE).unwrap();
   exec(&a,"UPDATE outbox SET row_json='invalid' WHERE seq=43").unwrap();}
  assert_eq!(db.apply_offline(43,NOW,false).err(),Some(MirrorDbError::Corrupt));
  assert_eq!(db.review_destination().unwrap(),(0,0));
  fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r150_missing_or_symlink_db2_does_not_create_fresh_cursor(){
  use std::os::unix::fs::symlink;
  let (root,db)=setup();
  fs::remove_file(&db.db2).unwrap();
  assert!(matches!(SqliteMirrorReview::load(&root),Err(MirrorDbError::Sql)));
  symlink(&db.db1,&db.db2).unwrap();
  assert!(matches!(SqliteMirrorReview::load(&root),Err(MirrorDbError::Path)));
  fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r150_concurrent_db2_transaction_does_not_double_apply(){
  use std::sync::Arc;
  let (root,db)=setup();
  db.append_synthetic_source(row(43,42),NOW).unwrap();
  let shared=Arc::new(db);
  let a=shared.clone();let b=shared.clone();
  let x=std::thread::spawn(move||a.apply_offline(43,NOW,false));
  let y=std::thread::spawn(move||b.apply_offline(43,NOW,false));
  let result=[x.join().unwrap(),y.join().unwrap()];
  assert_eq!(result.iter().filter(|r|r.is_ok()).count(),1,"{result:?}");
  assert_eq!(shared.review_destination().unwrap(),(43,1));
  fs::remove_dir_all(root).unwrap();
 }
}
