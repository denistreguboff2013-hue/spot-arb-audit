//! Durable bounded ingress inbox. All payloads remain until an explicit outcome;
//! published reclamation requires a canonical mirrored source receipt.
use super::{Db,V,sql,stmt,stmt_measured,SqlTiming,sqlite3_open_v2,SQLITE_OK};
use crate::{RawFrame,SourceProvenance};
use std::{path::{Path,PathBuf},fs,ffi::CString,os::unix::{fs::{MetadataExt,OpenOptionsExt},io::AsRawFd}};
use serde::{Serialize,Deserialize};
extern "C"{fn sqlite3_errcode(db:*mut std::ffi::c_void)->i32;}
fn hex(b:&[u8])->String{b.iter().map(|v|format!("{v:02x}")).collect()}
pub fn digest(b:&[u8])->String{hex(&openssl::sha::sha256(b))}
#[derive(Clone,Debug,Default,Serialize,Deserialize,PartialEq,Eq)]
pub struct Counts{pub received:u64,pub accepted:u64,pub published:u64,pub quarantined:u64,pub pending:u64,pub reclaimed:u64,pub payload_bytes:u64,pub rows:u64}
#[derive(Clone,Debug,Default,Serialize)]pub struct AcceptTiming{pub frames:usize,pub ok:bool,pub lock_wait_ns:u64,pub store_lock_acquired_mono_ns:u64,pub limit_checks_ns:u64,pub begin_ns:u64,pub sql_prepare_ns:u64,pub sql_bind_ns:u64,pub sql_write_step_ns:u64,pub commit_ns:u64,pub commit_started_mono_ns:u64,pub commit_finished_mono_ns:u64,pub other_ns:u64,pub total_ns:u64}
pub struct Store{db:Db,_lock:fs::File,pub journal:String,pub path:PathBuf,failed:bool,accept_timings:std::collections::VecDeque<AcceptTiming>,accept_timing_capacity:usize,pub accept_timing_evictions:u64,pub accept_timing_calls:u64,#[cfg(test)]fault_commit_probe:Option<std::sync::Arc<CommitDelayProbe>>}
#[cfg(test)]
#[derive(Default)]
pub struct CommitDelayProbe{pub delay_ms:std::sync::atomic::AtomicU64,pub started:std::sync::atomic::AtomicBool}
fn monotonic_ns()->u64{let mut t=libc::timespec{tv_sec:0,tv_nsec:0};unsafe{libc::clock_gettime(libc::CLOCK_MONOTONIC,&mut t)};t.tv_sec as u64*1_000_000_000+t.tv_nsec as u64}
// SQLite FULLMUTEX connection, exclusively accessed through a Mutex by callers.
unsafe impl Send for Store{}
impl Store{
 fn before_commit(&self){
  #[cfg(test)]
  if let Some(probe)=&self.fault_commit_probe{
   probe.started.store(true,std::sync::atomic::Ordering::Release);
   let delay_ms=probe.delay_ms.load(std::sync::atomic::Ordering::Acquire);
   std::thread::sleep(std::time::Duration::from_millis(delay_ms));
  }
 }
 #[cfg(test)]
 pub fn set_commit_delay_probe(&mut self,probe:std::sync::Arc<CommitDelayProbe>){self.fault_commit_probe=Some(probe);}
 pub fn open(root:&Path)->Result<Self,String>{
  let m=fs::symlink_metadata(root).map_err(|e|e.to_string())?;
  if !m.is_dir()||m.file_type().is_symlink()||m.uid()!=unsafe{libc::geteuid()}||m.mode()&0o077!=0{return Err("R1205 private directory required".into())}
  let lock=fs::OpenOptions::new().create(true).read(true).write(true).mode(0o600).custom_flags(libc::O_NOFOLLOW).open(root.join("ingress.lock")).map_err(|e|e.to_string())?;
  if unsafe{libc::flock(lock.as_raw_fd(),libc::LOCK_EX|libc::LOCK_NB)}!=0{return Err("R1205 inbox locked".into())}
  let path=root.join("ingress.sqlite");let fresh=!path.exists();
  if fresh{fs::OpenOptions::new().create_new(true).write(true).mode(0o600).open(&path).map_err(|e|e.to_string())?;}
  let m=fs::symlink_metadata(&path).map_err(|e|e.to_string())?;
  if !m.is_file()||m.nlink()!=1||m.file_type().is_symlink()||m.uid()!=unsafe{libc::geteuid()}||m.mode()&0o077!=0{return Err("R1205 unsafe inbox".into())}
  let p=CString::new(path.as_os_str().as_encoded_bytes()).map_err(|_|"path nul")?;let mut raw=std::ptr::null_mut();
  if unsafe{sqlite3_open_v2(p.as_ptr(),&mut raw,2|0x10000|0x01000000,std::ptr::null())}!=SQLITE_OK||raw.is_null(){return Err("R1205 SQLite open".into())}
  let db=Db(raw);
  sql(&db,"PRAGMA journal_mode=WAL;PRAGMA synchronous=FULL;PRAGMA foreign_keys=ON;PRAGMA busy_timeout=500;PRAGMA max_page_count=8192;PRAGMA wal_autocheckpoint=1000;")?;
  if fresh{
   sql(&db,"BEGIN IMMEDIATE;
    CREATE TABLE meta(id INTEGER PRIMARY KEY CHECK(id=1),journal TEXT NOT NULL,epoch INTEGER NOT NULL,received INTEGER NOT NULL,accepted INTEGER NOT NULL,published INTEGER NOT NULL,quarantined INTEGER NOT NULL,reclaimed INTEGER NOT NULL,chain TEXT NOT NULL);
    CREATE TABLE reclaim_proofs(id INTEGER PRIMARY KEY,proof TEXT NOT NULL);
    CREATE TABLE binding(id INTEGER PRIMARY KEY CHECK(id=1),canonical_path_hash TEXT NOT NULL);
    CREATE TABLE payload(hash TEXT PRIMARY KEY,raw TEXT NOT NULL);
    CREATE TABLE inbox(id INTEGER PRIMARY KEY,source TEXT NOT NULL,connection_epoch INTEGER NOT NULL,venue TEXT NOT NULL,received_ns INTEGER NOT NULL,received_wall INTEGER NOT NULL,hash TEXT NOT NULL REFERENCES payload(hash),outcome TEXT NOT NULL,reason TEXT NOT NULL,proof TEXT NOT NULL,provenance TEXT NOT NULL);
    CREATE INDEX inbox_status ON inbox(outcome,id);
    CREATE TABLE history(id INTEGER PRIMARY KEY,hash TEXT NOT NULL,outcome TEXT NOT NULL,reason TEXT NOT NULL,proof TEXT NOT NULL);
    PRAGMA user_version=1205;COMMIT;")?;
   let mut nonce=[0u8;32];openssl::rand::rand_bytes(&mut nonce).map_err(|e|e.to_string())?;
   stmt(&db,"INSERT INTO meta VALUES(1,?,0,0,0,0,0,0,'')",&[V::Str(hex(&nonce))])?;
   fs::File::open(root).and_then(|f|f.sync_all()).map_err(|e|e.to_string())?;
  }
  if stmt(&db,"PRAGMA user_version",&[])?.get(0).and_then(|r|r.get(0)).map(String::as_str)!=Some("1205"){return Err("R1205 unknown schema".into())}
  sql(&db,"CREATE TABLE IF NOT EXISTS frame_trace_terminal(id INTEGER PRIMARY KEY,trace_id TEXT NOT NULL,terminal_state TEXT NOT NULL,ack_status TEXT NOT NULL,source_id TEXT NOT NULL,venue TEXT NOT NULL,instrument TEXT NOT NULL,exchange_event_ts_ms TEXT NOT NULL,trace_json TEXT NOT NULL,completion_json TEXT NOT NULL DEFAULT '{}');")?;
  if stmt(&db,"PRAGMA quick_check",&[])?.get(0).and_then(|r|r.get(0)).map(String::as_str)!=Some("ok"){return Err("R1205 corrupt inbox".into())}
  if std::env::var("R1206_RECLAIM_INDEX").ok().as_deref()!=Some("OFF"){sql(&db,"CREATE INDEX IF NOT EXISTS inbox_payload_hash ON inbox(hash)")?;}
  let journal=stmt(&db,"SELECT journal FROM meta WHERE id=1",&[])?.get(0).and_then(|r|r.get(0)).ok_or("R1205 missing meta")?.clone();
  let out=Self{db,_lock:lock,journal,path,failed:false,accept_timings:Default::default(),accept_timing_capacity:1024,accept_timing_evictions:0,accept_timing_calls:0,#[cfg(test)]fault_commit_probe:None};out.counts()?;out.pending()?;Ok(out)
 }
 fn tx<T>(&mut self,f:impl FnOnce(&Db)->Result<T,String>)->Result<T,String>{
  if self.failed{return Err("R1205 inbox fatal".into())}
  sql(&self.db,"BEGIN IMMEDIATE")?;
  match f(&self.db).and_then(|v|{self.before_commit();sql(&self.db,"COMMIT")?;Ok(v)}){
   Ok(v)=>Ok(v),Err(e)=>{let code=unsafe{sqlite3_errcode(self.db.0)};let _=sql(&self.db,"ROLLBACK");self.failed=true;Err(format!("{e}; sqlite_code={code}"))}
  }
 }
 pub fn bind_canonical(&mut self,root:&Path)->Result<(),String>{
  let target=digest(root.canonicalize().map_err(|e|e.to_string())?.as_os_str().as_encoded_bytes());
  self.tx(|db|{let prior=stmt(db,"SELECT canonical_path_hash FROM binding WHERE id=1",&[])?;
    if prior.is_empty(){stmt(db,"INSERT INTO binding VALUES(1,?)",&[V::Str(target)])?;Ok(())}
    else if prior==vec![vec![target]]{Ok(())}else{Err("R1205 canonical binding mismatch".into())}
  })
 }
 pub fn next_epoch(&mut self)->Result<u64,String>{self.tx(|db|{sql(db,"UPDATE meta SET epoch=epoch+1 WHERE id=1 AND epoch<9223372036854775807")?;let r=stmt(db,"SELECT epoch FROM meta WHERE id=1",&[])?;r[0][0].parse().map_err(|_|"epoch overflow".into())})}
 pub fn profile_capacity(&mut self,n:usize)->Result<(),String>{if !(1024..=65536).contains(&n){return Err("R1206 profile capacity".into())}self.accept_timing_capacity=n;Ok(())}
 pub fn accept_profile(&self)->serde_json::Value{serde_json::json!({"capacity":self.accept_timing_capacity,"calls":self.accept_timing_calls,"evictions":self.accept_timing_evictions,"samples":self.accept_timings})}
 pub fn accept_batch(&mut self,frames:&mut [RawFrame])->Result<(),String>{self.accept_batch_with_lock_wait(frames,0)}
 pub fn accept_batch_with_lock_wait(&mut self,frames:&mut [RawFrame],lock_wait_ns:u64)->Result<(),String>{
  let store_lock_acquired_mono_ns=monotonic_ns();let started=std::time::Instant::now();let mut t=AcceptTiming{frames:frames.len(),lock_wait_ns,store_lock_acquired_mono_ns,..Default::default()};
  let result=self.accept_inner(frames,&mut t);t.ok=result.is_ok();t.total_ns=lock_wait_ns+started.elapsed().as_nanos()as u64;
  t.other_ns=t.total_ns.saturating_sub(t.lock_wait_ns+t.limit_checks_ns+t.begin_ns+t.sql_prepare_ns+t.sql_bind_ns+t.sql_write_step_ns+t.commit_ns);
  self.accept_timing_calls+=1;if self.accept_timings.len()>=self.accept_timing_capacity{self.accept_timings.pop_front();self.accept_timing_evictions+=1;}self.accept_timings.push_back(t);result
 }
 fn accept_inner(&mut self,frames:&mut [RawFrame],timing:&mut AcceptTiming)->Result<(),String>{
  let checks=std::time::Instant::now();
  if frames.is_empty()||frames.len()>32{return Err("R1205 batch bounds".into())}
  let parent=self.path.parent().ok_or("inbox parent")?;
  let path=CString::new(parent.as_os_str().as_encoded_bytes()).map_err(|_|"parent path")?;let mut v:libc::statvfs=unsafe{std::mem::zeroed()};
  if unsafe{libc::statvfs(path.as_ptr(),&mut v)}!=0{return Err("R1205 FILESYSTEM_RESERVE_UNKNOWN".into())}
  let free=(v.f_bavail as u128)*(v.f_frsize as u128);
  if free<512*1024*1024+4*frames.iter().map(|f|f.raw.len()as u128).sum::<u128>(){return Err("R1205 FILESYSTEM_RESERVE".into())}
  let c=self.counts()?;
  if c.pending+frames.len()as u64>2048{return Err("R1205 GAP pending capacity".into())}
  if c.payload_bytes+frames.iter().map(|f|f.raw.len()as u64).sum::<u64>()>24*1024*1024{return Err("R1205 GAP byte budget".into())}
  let journal=self.journal.clone();
  timing.limit_checks_ns=checks.elapsed().as_nanos()as u64;
  if self.failed{return Err("R1205 inbox fatal".into())}
  let start=std::time::Instant::now();let begin=sql(&self.db,"BEGIN IMMEDIATE");timing.begin_ns=start.elapsed().as_nanos()as u64;begin?;
  let mut st=SqlTiming::default();let db=&self.db;
  let result:Result<(),String>=(||{
   let mut n=c.accepted;
   for f in frames{
    if f.raw.len()>32768{return Err("R1205 payload cap".into())}
    crate::ensure_frame_trace_id(f);
    let p=f.source.as_mut().ok_or("R1205 source missing")?;n=n.checked_add(1).ok_or("count overflow")?;
    p.journal_id=journal.clone();p.accepted_id=n;p.payload_sha256=digest(f.raw.as_bytes());
    stmt_measured(db,"INSERT OR IGNORE INTO payload VALUES(?,?)",&[V::Str(p.payload_sha256.clone()),V::Str(f.raw.clone())],Some(&mut st))?;
    stmt_measured(db,"INSERT INTO inbox VALUES(?,?,?,?,?,?,?,'retained_pending','','',?)",&[V::Num(n as i64),V::Str(p.session_id.clone()),V::Num(p.connection_epoch as i64),V::Str(format!("{:?}",f.venue)),V::Num(f.received_ns as i64),V::Num(f.received_wall_ms),V::Str(p.payload_sha256.clone()),V::Str(serde_json::to_string(&p).map_err(|_|"provenance encoding")?)],Some(&mut st))?;
   }
   stmt_measured(db,"UPDATE meta SET received=received+?,accepted=? WHERE id=1",&[V::Num((n-c.accepted)as i64),V::Num(n as i64)],Some(&mut st))?;Ok(())
  })();
  timing.sql_prepare_ns=st.prepare_ns;timing.sql_bind_ns=st.bind_ns;timing.sql_write_step_ns=st.step_ns;
  let result=result.and_then(|v|{timing.commit_started_mono_ns=monotonic_ns();self.before_commit();let at=std::time::Instant::now();let commit=sql(db,"COMMIT");timing.commit_ns=at.elapsed().as_nanos()as u64;timing.commit_finished_mono_ns=monotonic_ns();commit?;Ok(v)});
  match result{Ok(())=>Ok(()),Err(e)=>{let code=unsafe{sqlite3_errcode(db.0)};let _=sql(db,"ROLLBACK");self.failed=true;Err(format!("{e}; sqlite_code={code}"))}}
 }
 pub fn finish(&mut self,frame:&RawFrame,outcome:&str,reason:&str,canonical_proof:Option<&str>)->Result<(),String>{
  let journal=self.journal.clone();
  self.tx(|db|{finish_sql(db,&journal,frame,outcome,reason,canonical_proof)?;reclaim_sql(db)})
 }
 /// Terminalize a canonical batch from an opaque Writer token. All canonical
 /// verification was completed before the Store mutex was acquired; this
 /// critical section performs only local inbox reads and one FULL transaction.
 pub fn finish_published_batch_verified(&mut self,frames:&[RawFrame],verified:&super::VerifiedIngressBatch)->Result<super::DurableInboxPublish,String>{
  if frames.is_empty()||frames.len()>32||verified.entries.len()!=frames.len(){return Err("R1205 published batch bounds".into())}
  let canonical=verified.root.canonicalize().map_err(|e|e.to_string())?;
  let binding=digest(canonical.as_os_str().as_encoded_bytes());
  if stmt(&self.db,"SELECT canonical_path_hash FROM binding WHERE id=1",&[])?!=vec![vec![binding]]{return Err("R1205 canonical binding mismatch".into())}
  let mut seen=std::collections::HashSet::new();let mut proofs=Vec::with_capacity(frames.len());
  for(f,e)in frames.iter().zip(&verified.entries){
   let p=f.source.as_ref().ok_or("R1205 source missing")?;
   if p.journal_id!=e.journal||p.accepted_id!=e.id||p.payload_sha256!=e.raw_hash||digest(f.raw.as_bytes())!=p.payload_sha256||e.proof.len()!=64||!seen.insert(e.id)||!self.recognizes(f)?{return Err("R1205 verified publish receipt mismatch".into())}
   let expected=super::receipt_proof(&e.journal,e.id,&e.raw_hash,&e.frame_hash,e.source_seq);
   if expected!=e.proof{return Err("R1205 verified publish proof mismatch".into())}
   proofs.push(e.proof.clone());
  }
  let journal=self.journal.clone();
  self.tx(|db|{for(f,proof)in frames.iter().zip(&proofs){finish_sql(db,&journal,f,"published","canonical_db1_db2_ack",Some(proof))?;}reclaim_sql(db)})?;
  Ok(super::DurableInboxPublish{root:verified.root.clone(),journal:self.journal.clone(),entries:verified.entries.clone()})
 }
 /// Terminalize a sealed canonical batch in one FULL transaction. Every
 /// proof comes from the independently verified writer, never the caller.
 /// On failure all payloads/proofs remain pending or the whole batch commits.
 pub fn finish_published_batch(&mut self,w:&super::Writer,frames:&[RawFrame])->Result<(),String>{
  if frames.is_empty()||frames.len()>32{return Err("R1205 published batch bounds".into())}
  w.verify_pair()?;w.require_decisions_drained()?;
  let mut seen=std::collections::HashSet::new();let mut proofs=Vec::with_capacity(frames.len());
  for f in frames{
   let p=f.source.as_ref().ok_or("R1205 source missing")?;
   if !self.recognizes(f)?||!seen.insert(p.accepted_id){return Err("R1205 published batch receipt mismatch".into())}
   proofs.push(w.ingress_receipt(&p.journal_id,p.accepted_id,&p.payload_sha256)?.ok_or("R1205 canonical receipt missing")?);
  }
  let journal=self.journal.clone();
  self.tx(|db|{for(f,proof)in frames.iter().zip(&proofs){finish_sql(db,&journal,f,"published","canonical_db1_db2_ack",Some(proof))?;}reclaim_sql(db)})
 }
 pub fn quarantine_batch(&mut self,items:&[(RawFrame,String)])->Result<(),String>{
  if items.is_empty(){return Ok(())}if items.len()>256{return Err("quarantine batch bounds".into())}
  let journal=self.journal.clone();self.tx(|db|{for(f,why)in items{finish_sql(db,&journal,f,"quarantined",why,None)?;}reclaim_sql(db)})
 }
 #[cfg(test)]pub fn r1206_remove_index_fixture(&mut self)->Result<(),String>{sql(&self.db,"DROP INDEX IF EXISTS inbox_payload_hash")}
 #[cfg(test)]pub fn r1206_index_fixture_state(&self)->Result<(Vec<Vec<String>>,String),String>{Ok((stmt(&self.db,"EXPLAIN QUERY PLAN SELECT 1 FROM inbox WHERE hash='fixture'",&[])?,stmt(&self.db,"PRAGMA synchronous",&[])?[0][0].clone()))}
 #[cfg(test)]pub fn r1207_durability_fixture_state(&self)->Result<(String,String,String),String>{Ok((stmt(&self.db,"PRAGMA journal_mode",&[])?[0][0].clone(),stmt(&self.db,"PRAGMA synchronous",&[])?[0][0].clone(),stmt(&self.db,"PRAGMA wal_autocheckpoint",&[])?[0][0].clone()))}
 #[cfg(test)]pub fn fault_full_pages(&mut self)->Result<(),String>{let n=stmt(&self.db,"PRAGMA page_count",&[])?[0][0].clone();sql(&self.db,&format!("PRAGMA max_page_count={n}"))}
 pub fn pending(&self)->Result<Vec<RawFrame>,String>{
  let rows=stmt(&self.db,"SELECT i.id,i.source,i.connection_epoch,i.venue,i.received_ns,i.received_wall,i.hash,p.raw,i.provenance FROM inbox i JOIN payload p ON p.hash=i.hash ORDER BY i.id LIMIT 2049",&[])?;
  if rows.len()>2048{return Err("R1205 recovery bounds".into())}
  rows.into_iter().map(|r|{
   if digest(r[7].as_bytes())!=r[6]{return Err("R1205 payload corrupt".into())}
   let venue=match r[3].as_str(){"BinanceFutures"=>crate::Venue::BinanceFutures,"OkxSpot"=>crate::Venue::OkxSpot,_=>return Err("R1205 market".into())};
   Ok(RawFrame{venue,epoch:0,received_ns:r[4].parse().map_err(|_|"clock")?,received_wall_ms:r[5].parse().map_err(|_|"clock")?,raw:r[7].clone(),source:Some({let p:SourceProvenance=serde_json::from_str(&r[8]).map_err(|_|"provenance corrupt")?;if p.journal_id!=self.journal||p.accepted_id.to_string()!=r[0]||p.session_id!=r[1]||p.connection_epoch.to_string()!=r[2]||p.payload_sha256!=r[6]{return Err("provenance binding corrupt".into())}p})})
  }).collect()
 }
 pub fn reclaim_refs(&self)->Result<Vec<(String,u64)>,String>{
  let rows=stmt(&self.db,"SELECT id FROM reclaim_proofs ORDER BY id LIMIT 2049",&[])?;
  if rows.len()>2048{return Err("reclaim proof bounds".into())}
  rows.into_iter().map(|r|Ok((self.journal.clone(),r[0].parse().map_err(|_|"reclaim id")?))).collect()
 }
 pub fn forget_reclaimed_after_ack(&mut self,acked:&super::AckedIngressReclaim)->Result<(),String>{self.forget_reclaimed_after_ack_with_trace(acked,&[])}
 pub fn forget_reclaimed_after_ack_with_trace(&mut self,acked:&super::AckedIngressReclaim,traces:&[(u64,String)])->Result<(),String>{
  let canonical=acked.root.canonicalize().map_err(|e|e.to_string())?;let binding=digest(canonical.as_os_str().as_encoded_bytes());
  if acked.journal!=self.journal||stmt(&self.db,"SELECT canonical_path_hash FROM binding WHERE id=1",&[])?!=vec![vec![binding]]{return Err("R1205 acknowledged reclaim binding mismatch".into())}
  let mut seen=std::collections::HashSet::new();
  for e in &acked.entries{
   if e.journal!=self.journal||!seen.insert(e.id){return Err("R1205 acknowledged reclaim references mismatch".into())}
   let row=stmt(&self.db,"SELECT proof FROM reclaim_proofs WHERE id=?",&[V::Num(e.id as i64)])?;
   if row!=vec![vec![e.proof.clone()]]{return Err("R1205 acknowledged reclaim proof mismatch".into())}
  }
  self.tx(|db|{for e in &acked.entries{if let Some((_,trace))=traces.iter().find(|(id,_)|*id==e.id){stmt(db,"UPDATE frame_trace_terminal SET ack_status='reclaimed_ack_confirmed',completion_json=? WHERE id=? AND terminal_state='published'",&[V::Str(trace.clone()),V::Num(e.id as i64)])?;}else{stmt(db,"UPDATE frame_trace_terminal SET ack_status='reclaimed_ack_confirmed' WHERE id=? AND terminal_state='published'",&[V::Num(e.id as i64)])?;}stmt(db,"DELETE FROM reclaim_proofs WHERE id=?",&[V::Num(e.id as i64)])?;}prune_terminal_details_r1325(db)})
 }
 pub fn forget_reclaimed(&mut self,w:&super::Writer,refs:&[(String,u64)])->Result<(),String>{
  for(journal,id)in refs{if journal!=&self.journal||w.ingress_receipt_exists(journal,*id)?{return Err("canonical receipt not reclaimed".into())}}
  self.tx(|db|{for(_,id)in refs{stmt(db,"DELETE FROM reclaim_proofs WHERE id=?",&[V::Num(*id as i64)])?;}Ok(())})
 }
 pub fn recognizes(&self,frame:&RawFrame)->Result<bool,String>{
  let Some(p)=&frame.source else{return Ok(false)};
  if p.journal_id!=self.journal||digest(frame.raw.as_bytes())!=p.payload_sha256{return Ok(false)}
  let r=stmt(&self.db,"SELECT source,connection_epoch,received_ns,received_wall,hash FROM inbox WHERE id=?",&[V::Num(p.accepted_id as i64)])?;
  Ok(r==vec![vec![p.session_id.clone(),p.connection_epoch.to_string(),frame.received_ns.to_string(),frame.received_wall_ms.to_string(),p.payload_sha256.clone()]])
 }
 pub fn published_proof(&self,journal:&str,id:u64,proof:&str)->Result<bool,String>{
  if journal!=self.journal{return Ok(false)}
  let r=stmt(&self.db,"SELECT 'published',proof FROM reclaim_proofs WHERE id=?",&[V::Num(id as i64)])?;
  Ok(r==vec![vec!["published".to_string(),proof.to_string()]])
 }
 /// Diagnostic detail retention is separate from durable event accounting.
 /// The existing cumulative counters/chain retain every terminal outcome.
 pub fn frame_trace_retention_r1325(&self)->Result<serde_json::Value,String>{
  let r=stmt(&self.db,"SELECT reclaimed,(SELECT COUNT(*) FROM frame_trace_terminal),(SELECT COUNT(*) FROM frame_trace_terminal WHERE ack_status='awaiting_reclaim_ack'),chain FROM meta WHERE id=1",&[])?;
  if r.len()!=1{return Err("R1325 trace retention meta missing".into())}
  let total=r[0][0].parse::<u64>().map_err(|_|"R1325 trace total")?;let retained=r[0][1].parse::<u64>().map_err(|_|"R1325 trace retained")?;
  let missing=total.checked_sub(retained).ok_or("R1325 trace retention residual")?;
  Ok(serde_json::json!({"completed_detail_limit":512,"terminal_outcomes":total,"retained_details":retained,"awaiting_ack_details":r[0][2].parse::<u64>().map_err(|_|"R1325 trace awaiting")?,"terminal_details_not_retained":missing,"terminal_outcome_chain":r[0][3]}))
 }
 pub fn frame_trace_terminal(&self,limit:usize)->Result<Vec<serde_json::Value>,String>{
  if limit==0||limit>4096{return Err("R1279 trace read bound".into())}
  let rows=stmt(&self.db,"SELECT id,trace_id,terminal_state,ack_status,source_id,venue,instrument,exchange_event_ts_ms,trace_json,completion_json FROM frame_trace_terminal ORDER BY id DESC LIMIT ?",&[V::Num(limit as i64)])?;
  Ok(rows.into_iter().map(|r|serde_json::json!({"accepted_id":r[0].parse::<u64>().ok(),"frame_trace_id":r[1],"terminal_state":r[2],"ack_status":r[3],"source":r[4],"venue":r[5],"instrument":if r[6].is_empty(){serde_json::Value::Null}else{serde_json::Value::String(r[6].clone())},"exchange_event_ts_ms":r[7].parse::<i64>().ok(),"trace":serde_json::from_str::<serde_json::Value>(&r[8]).unwrap_or(serde_json::Value::Null),"completion":serde_json::from_str::<serde_json::Value>(&r[9]).unwrap_or(serde_json::Value::Null)})).collect())
 }
 pub fn counts(&self)->Result<Counts,String>{
  let r=stmt(&self.db,"SELECT received,accepted,published,quarantined,reclaimed,(SELECT COUNT(*) FROM inbox),(SELECT COALESCE(SUM(length(CAST(raw AS BLOB))),0) FROM payload),(SELECT COUNT(*) FROM history) FROM meta WHERE id=1",&[])?;
  if r.len()!=1{return Err("R1205 missing counts".into())}
  let n=r[0].iter().map(|s|s.parse::<u64>().map_err(|_|"R1205 invalid count".to_string())).collect::<Result<Vec<_>,_>>()?;
  let c=Counts{received:n[0],accepted:n[1],published:n[2],quarantined:n[3],reclaimed:n[4],pending:n[5],payload_bytes:n[6],rows:n[5]+n[7]};
  if c.accepted!=c.published+c.quarantined+c.pending||c.accepted!=c.received{return Err("R1205 accounting residual".into())}Ok(c)
 }
}

fn finish_sql(db:&Db,journal:&str,frame:&RawFrame,outcome:&str,reason:&str,canonical_proof:Option<&str>)->Result<(),String>{
 if !matches!(outcome,"published"|"quarantined")||reason.len()>160{return Err("R1205 outcome schema".into())}
 if outcome=="published"&&canonical_proof.map_or(true,|p|p.len()!=64){return Err("R1205 canonical ACK proof required".into())}
 let p=frame.source.as_ref().ok_or("R1205 source missing")?;
 if p.journal_id!=journal||digest(frame.raw.as_bytes())!=p.payload_sha256{return Err("R1205 receipt mismatch".into())}
 let id=p.accepted_id;let proof=canonical_proof.unwrap_or("");
 let row=stmt(db,"SELECT hash,outcome,source,connection_epoch FROM inbox WHERE id=?",&[V::Num(id as i64)])?;
 if row.len()!=1||row[0][0]!=p.payload_sha256||row[0][2]!=p.session_id||row[0][3]!=p.connection_epoch.to_string(){return Err("R1205 unknown receipt".into())}
 if row[0][1]!="retained_pending"{return Err("R1205 duplicate terminalization".into())}
 if outcome=="published"{stmt(db,"INSERT INTO reclaim_proofs VALUES(?,?)",&[V::Num(id as i64),V::Str(proof.into())])?;}
 stmt(db,"INSERT INTO history VALUES(?,?,?,?,?)",&[V::Num(id as i64),V::Str(p.payload_sha256.clone()),V::Str(outcome.into()),V::Str(reason.into()),V::Str(proof.into())])?;
 if !p.frame_trace_id.is_empty(){let ack=if outcome=="published"{"awaiting_reclaim_ack"}else{"not_applicable_quarantine"};let timing=p.r1279_timing.as_ref();stmt(db,"INSERT INTO frame_trace_terminal(id,trace_id,terminal_state,ack_status,source_id,venue,instrument,exchange_event_ts_ms,trace_json) VALUES(?,?,?,?,?,?,?,?,?)",&[V::Num(id as i64),V::Str(p.frame_trace_id.clone()),V::Str(outcome.into()),V::Str(ack.into()),V::Str(p.session_id.clone()),V::Str(format!("{:?}",frame.venue)),V::Str(timing.and_then(|t|t.instrument.clone()).unwrap_or_default()),V::Str(timing.and_then(|t|t.exchange_event_ts_ms).map(|v|v.to_string()).unwrap_or_default()),V::Str(serde_json::to_string(&p.r1279_timing).map_err(|_|"R1279 timing encode")?)])?;}
 let previous=stmt(db,"SELECT chain FROM meta WHERE id=1",&[])?[0][0].clone();let chain=digest(format!("{previous}|{id}|{}|{outcome}|{reason}|{proof}",p.payload_sha256).as_bytes());
 let which=if outcome=="published"{"published"}else{"quarantined"};stmt(db,&format!("UPDATE meta SET {which}={which}+1,reclaimed=reclaimed+1,chain=? WHERE id=1"),&[V::Str(chain)])?;
 stmt(db,"DELETE FROM inbox WHERE id=?",&[V::Num(id as i64)])?;Ok(())
}
fn prune_terminal_details_r1325(db:&Db)->Result<(),String>{
 // Never prune awaiting-ACK details or touch inbox/reclaim proofs. Only terminal
 // diagnostics whose custody is settled share the existing 512-row history bound.
 sql(db,"DELETE FROM frame_trace_terminal WHERE ack_status IN ('not_applicable_quarantine','reclaimed_ack_confirmed') AND id NOT IN (SELECT id FROM frame_trace_terminal WHERE ack_status IN ('not_applicable_quarantine','reclaimed_ack_confirmed') ORDER BY id DESC LIMIT 512);")
}
fn reclaim_sql(db:&Db)->Result<(),String>{sql(db,"DELETE FROM payload WHERE NOT EXISTS(SELECT 1 FROM inbox WHERE inbox.hash=payload.hash);DELETE FROM history WHERE id NOT IN(SELECT id FROM history ORDER BY id DESC LIMIT 512);")?;prune_terminal_details_r1325(db)}

type StoreOperation = Box<dyn FnOnce(&mut Store, u64) + Send + 'static>;

#[derive(Default)]
struct StoreWorkerStats {
    outstanding: std::sync::atomic::AtomicU64,
    highwater: std::sync::atomic::AtomicU64,
    submitted: std::sync::atomic::AtomicU64,
    completed: std::sync::atomic::AtomicU64,
    overflow: std::sync::atomic::AtomicU64,
    queue_wait_total_ns: std::sync::atomic::AtomicU64,
    queue_wait_max_ns: std::sync::atomic::AtomicU64,
    store_lock_wait_total_ns: std::sync::atomic::AtomicU64,
    store_lock_wait_max_ns: std::sync::atomic::AtomicU64,
    operation_ns_total: std::sync::atomic::AtomicU64,
    operation_ns_max: std::sync::atomic::AtomicU64,
}

struct StoreWorkerInner {
    tx: std::sync::Mutex<Option<std::sync::mpsc::SyncSender<StoreOperation>>>,
    stats: std::sync::Arc<StoreWorkerStats>,
    join: std::sync::Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for StoreWorkerInner {
    fn drop(&mut self) {
        if let Ok(tx) = self.tx.get_mut() {
            tx.take();
        }
        if let Ok(join) = self.join.get_mut() {
            if let Some(handle) = join.take() {
                let _ = handle.join();
            }
        }
    }
}

/// One serialized SQLite custodian. The bounded command queue never hides
/// overload: a full queue returns an error and callers must fail closed.
#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub struct StoreWorkerCallTiming {
    pub total_ns:u64,
    pub queue_wait_ns:u64,
    pub store_lock_wait_ns:u64,
    pub operation_ns:u64,
    pub other_wait_ns:u64,
}
#[derive(Clone)]
pub struct StoreWorker {
    inner: std::sync::Arc<StoreWorkerInner>,
}

impl StoreWorker {
    pub const QUEUE_CAPACITY: usize = 16;

    pub fn start(shared: std::sync::Arc<std::sync::Mutex<Store>>) -> Result<Self, String> {
        let (tx, rx) = std::sync::mpsc::sync_channel::<StoreOperation>(Self::QUEUE_CAPACITY);
        let stats = std::sync::Arc::new(StoreWorkerStats::default());
        let worker_stats = stats.clone();
        let join = std::thread::Builder::new()
            .name("r1240-sqlite-custodian".into())
            .spawn(move || {
                while let Ok(operation) = rx.recv() {
                    let lock_started = std::time::Instant::now();
                    let mut store = match shared.lock() {
                        Ok(store) => store,
                        Err(_) => break,
                    };
                    let lock_wait_ns = u64::try_from(lock_started.elapsed().as_nanos())
                        .unwrap_or(u64::MAX);
                    worker_stats.store_lock_wait_total_ns.fetch_add(lock_wait_ns, std::sync::atomic::Ordering::Relaxed);
                    worker_stats.store_lock_wait_max_ns.fetch_max(lock_wait_ns, std::sync::atomic::Ordering::Relaxed);
                    let operation_started=std::time::Instant::now();
                    operation(&mut store, lock_wait_ns);
                    let operation_ns=u64::try_from(operation_started.elapsed().as_nanos()).unwrap_or(u64::MAX);
                    worker_stats.operation_ns_total.fetch_add(operation_ns,std::sync::atomic::Ordering::Relaxed);
                    worker_stats.operation_ns_max.fetch_max(operation_ns,std::sync::atomic::Ordering::Relaxed);
                    worker_stats.outstanding.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                    worker_stats.completed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            })
            .map_err(|e| format!("R1240 SQLite worker spawn failed: {e}"))?;
        Ok(Self {
            inner: std::sync::Arc::new(StoreWorkerInner {
                tx: std::sync::Mutex::new(Some(tx)),
                stats,
                join: std::sync::Mutex::new(Some(join)),
            }),
        })
    }

    pub fn call<T, F>(&self, operation: F) -> Result<T, String>
    where
        T: Send + 'static,
        F: FnOnce(&mut Store, u64) -> Result<T, String> + Send + 'static,
    {
        let (reply_tx, reply_rx) = std::sync::mpsc::sync_channel::<Result<T, String>>(1);
        let queued_at = std::time::Instant::now();
        let stats = self.inner.stats.clone();
        let command: StoreOperation = Box::new(move |store, lock_wait_ns| {
            let queue_wait_ns = u64::try_from(queued_at.elapsed().as_nanos()).unwrap_or(u64::MAX);
            stats.queue_wait_total_ns.fetch_add(queue_wait_ns, std::sync::atomic::Ordering::Relaxed);
            stats.queue_wait_max_ns.fetch_max(queue_wait_ns, std::sync::atomic::Ordering::Relaxed);
            let _ = reply_tx.send(operation(store, lock_wait_ns));
        });
        let sender = self.inner.tx.lock().map_err(|_| "R1240 writer sender poisoned")?
            .as_ref().cloned().ok_or("R1240 writer closed")?;
        self.inner.stats.submitted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let outstanding = self.inner.stats.outstanding.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
        match sender.try_send(command) {
            Ok(()) => {
                self.inner.stats.highwater.fetch_max(outstanding, std::sync::atomic::Ordering::Relaxed);
            }
            Err(std::sync::mpsc::TrySendError::Full(_)) => {
                self.inner.stats.outstanding.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                self.inner.stats.overflow.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return Err("R1240 durable writer queue full; fail closed".into());
            }
            Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
                self.inner.stats.outstanding.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                return Err("R1240 durable writer disconnected; fail closed".into());
            }
        }
        reply_rx.recv().map_err(|_| "R1240 durable writer reply lost; fail closed".to_string())?
    }

    /// Synchronous fail-closed call with a per-operation wait/lock/storage split.
    pub fn call_profiled<T, F>(&self, operation: F) -> Result<(T, StoreWorkerCallTiming), String>
    where
        T: Send + 'static,
        F: FnOnce(&mut Store) -> Result<T, String> + Send + 'static,
    {
        let call_started=std::time::Instant::now();
        let queued_at=call_started;
        let (reply_tx,reply_rx)=std::sync::mpsc::sync_channel::<Result<(T,u64,u64,u64),String>>(1);
        let stats=self.inner.stats.clone();
        let command:StoreOperation=Box::new(move |store,lock_wait_ns|{
            let worker_wait_ns=u64::try_from(queued_at.elapsed().as_nanos()).unwrap_or(u64::MAX);
            let queue_wait_ns=worker_wait_ns.saturating_sub(lock_wait_ns);
            stats.queue_wait_total_ns.fetch_add(queue_wait_ns,std::sync::atomic::Ordering::Relaxed);
            stats.queue_wait_max_ns.fetch_max(queue_wait_ns,std::sync::atomic::Ordering::Relaxed);
            let operation_started=std::time::Instant::now();
            let result=operation(store);
            let operation_ns=u64::try_from(operation_started.elapsed().as_nanos()).unwrap_or(u64::MAX);
            stats.operation_ns_total.fetch_add(operation_ns,std::sync::atomic::Ordering::Relaxed);
            stats.operation_ns_max.fetch_max(operation_ns,std::sync::atomic::Ordering::Relaxed);
            let _=reply_tx.send(result.map(|value|(value,queue_wait_ns,lock_wait_ns,operation_ns)));
        });
        let sender=self.inner.tx.lock().map_err(|_|"R1240 writer sender poisoned")?
            .as_ref().cloned().ok_or("R1240 writer closed")?;
        self.inner.stats.submitted.fetch_add(1,std::sync::atomic::Ordering::Relaxed);
        let outstanding=self.inner.stats.outstanding.fetch_add(1,std::sync::atomic::Ordering::Relaxed)+1;
        match sender.try_send(command){
            Ok(())=>{self.inner.stats.highwater.fetch_max(outstanding,std::sync::atomic::Ordering::Relaxed);}
            Err(std::sync::mpsc::TrySendError::Full(_))=>{
                self.inner.stats.outstanding.fetch_sub(1,std::sync::atomic::Ordering::Relaxed);
                self.inner.stats.overflow.fetch_add(1,std::sync::atomic::Ordering::Relaxed);
                return Err("R1240 durable writer queue full; fail closed".into());
            }
            Err(std::sync::mpsc::TrySendError::Disconnected(_))=>{
                self.inner.stats.outstanding.fetch_sub(1,std::sync::atomic::Ordering::Relaxed);
                return Err("R1240 durable writer disconnected; fail closed".into());
            }
        }
        let (value,queue_wait_ns,store_lock_wait_ns,operation_ns)=reply_rx.recv()
            .map_err(|_|"R1240 durable writer reply lost; fail closed".to_string())??;
        let total_ns=u64::try_from(call_started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let accounted=queue_wait_ns.saturating_add(store_lock_wait_ns).saturating_add(operation_ns);
        Ok((value,StoreWorkerCallTiming{total_ns,queue_wait_ns,store_lock_wait_ns,operation_ns,other_wait_ns:total_ns.saturating_sub(accounted)}))
    }

    pub async fn call_async<T, F>(&self, operation: F) -> Result<T, String>
    where
        T: Send + 'static,
        F: FnOnce(&mut Store, u64) -> Result<T, String> + Send + 'static,
    {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel::<Result<T, String>>();
        let queued_at = std::time::Instant::now();
        let stats = self.inner.stats.clone();
        let command: StoreOperation = Box::new(move |store, lock_wait_ns| {
            let queue_wait_ns = u64::try_from(queued_at.elapsed().as_nanos()).unwrap_or(u64::MAX);
            stats.queue_wait_total_ns.fetch_add(queue_wait_ns, std::sync::atomic::Ordering::Relaxed);
            stats.queue_wait_max_ns.fetch_max(queue_wait_ns, std::sync::atomic::Ordering::Relaxed);
            let _ = reply_tx.send(operation(store, lock_wait_ns));
        });
        let sender = self.inner.tx.lock().map_err(|_| "R1240 writer sender poisoned")?
            .as_ref().cloned().ok_or("R1240 writer closed")?;
        self.inner.stats.submitted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let outstanding = self.inner.stats.outstanding.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
        match sender.try_send(command) {
            Ok(()) => {
                self.inner.stats.highwater.fetch_max(outstanding, std::sync::atomic::Ordering::Relaxed);
            }
            Err(std::sync::mpsc::TrySendError::Full(_)) => {
                self.inner.stats.outstanding.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                self.inner.stats.overflow.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return Err("R1240 durable writer queue full; fail closed".into());
            }
            Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
                self.inner.stats.outstanding.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                return Err("R1240 durable writer disconnected; fail closed".into());
            }
        }
        reply_rx.await.map_err(|_| "R1240 durable writer reply lost; fail closed".to_string())?
    }


    pub fn profile(&self) -> serde_json::Value {
        use std::sync::atomic::Ordering::Relaxed;
        serde_json::json!({
            "schema": "SPOTARB/R1240/SQLITE-CUSTODIAN/V1",
            "queue_capacity": Self::QUEUE_CAPACITY,
            "queue_depth_including_active": self.inner.stats.outstanding.load(Relaxed),
            "queue_highwater_including_active": self.inner.stats.highwater.load(Relaxed),
            "submitted": self.inner.stats.submitted.load(Relaxed),
            "completed": self.inner.stats.completed.load(Relaxed),
            "overflow_fail_closed": self.inner.stats.overflow.load(Relaxed),
            "queue_wait_total_ns": self.inner.stats.queue_wait_total_ns.load(Relaxed),
            "queue_wait_max_ns": self.inner.stats.queue_wait_max_ns.load(Relaxed),
            "store_lock_wait_total_ns": self.inner.stats.store_lock_wait_total_ns.load(Relaxed),
            "store_lock_wait_max_ns": self.inner.stats.store_lock_wait_max_ns.load(Relaxed),
            "operation_ns_total": self.inner.stats.operation_ns_total.load(Relaxed),
            "operation_ns_max": self.inner.stats.operation_ns_max.load(Relaxed),
            "writer_thread": "r1240-sqlite-custodian"
        })
    }
}
#[cfg(test)]
#[test]
fn r1325_terminal_detail_retention_reopens_without_losing_pending_or_counts(){
 use std::os::unix::fs::DirBuilderExt;
 let root=std::env::temp_dir().join(format!("r1325-trace-retention-{}-{}",std::process::id(),monotonic_ns()));fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
 let make=|i:u64|RawFrame{venue:crate::Venue::OkxSpot,epoch:1,received_ns:i+1,received_wall_ms:1,raw:format!("fixture-{i}"),source:Some(crate::market_data_ingress_r1205::provenance("r1325-retention".into(),1))};
 let mut store=Store::open(&root).unwrap();let mut pending=make(10000);store.accept_batch(std::slice::from_mut(&mut pending)).unwrap();
 for batch in 0..32{let mut frames=(batch*32..batch*32+32).map(make).collect::<Vec<_>>();for f in &mut frames{f.source.as_mut().unwrap().r1279_timing=Some(Default::default());}store.accept_batch(&mut frames).unwrap();store.quarantine_batch(&frames.into_iter().map(|f|(f,"retention_fixture".into())).collect::<Vec<_>>()).unwrap();}
 let before=store.counts().unwrap();let details=store.frame_trace_retention_r1325().unwrap();assert_eq!(details["retained_details"],512);assert_eq!(details["terminal_details_not_retained"],512);assert_eq!(details["terminal_outcomes"],1024);assert_eq!(details["awaiting_ack_details"],0);assert!(store.recognizes(&pending).unwrap());
 drop(store);let reopened=Store::open(&root).unwrap();assert_eq!(reopened.counts().unwrap(),before);assert_eq!(reopened.frame_trace_retention_r1325().unwrap(),details);assert_eq!(reopened.pending().unwrap().len(),1);assert!(reopened.recognizes(&pending).unwrap());assert_eq!(reopened.frame_trace_terminal(4096).unwrap().len(),512);drop(reopened);fs::remove_dir_all(root).unwrap();
}
