// R506-R515: two independent PRIVATE SQLite files, source->sink->ACK.
// Strict /tmp/astra_r366_isolated_* root, no production DB1/DB2 or exchanges.
fn r506_schema(db:&Db)->Result<(),EventOutboxR366Error>{
 exec(db,"CREATE TABLE r506_source_ack(
 id INTEGER PRIMARY KEY CHECK(id=1),
 seq INTEGER NOT NULL, payload_hash TEXT NOT NULL);
 INSERT INTO r506_source_ack VALUES(1,0,'GENESIS');")
 .map_err(r366_error)
}
fn r506_private_db2_path(root:&Path)->Result<PathBuf,EventOutboxR366Error>{
 r366_path(&root.join("astra-r366-event-outbox.sqlite"))?;
 let p=root.join("astra-r506-private-db2.sqlite");
 if let Ok(m)=fs::symlink_metadata(&p){
  if !m.file_type().is_file()||m.nlink()!=1||
    m.uid()!=unsafe{libc::geteuid()}||m.mode()&0o077!=0{
   return Err(EventOutboxR366Error::Path)
  }
 }
 Ok(p)
}
fn r506_db2_open(root:&Path)->Result<Db,EventOutboxR366Error>{
 let path=r506_private_db2_path(root)?;
 let c=CString::new(path.to_string_lossy().as_bytes())
    .map_err(|_|EventOutboxR366Error::Path)?;
 let mut ptr=std::ptr::null_mut();
 let rc=unsafe{sqlite3_open_v2(c.as_ptr(),&mut ptr,READWRITE_EXISTING,
   std::ptr::null())};
 if rc!=SQLITE_OK||ptr.is_null(){
  if !ptr.is_null(){unsafe{sqlite3_close(ptr);}}
  return Err(EventOutboxR366Error::Sql)
 }
 let db=Db(ptr);
 if unsafe{sqlite3_busy_timeout(db.0,1000)}!=SQLITE_OK{
  return Err(EventOutboxR366Error::Sql)
 }
 exec(&db,"PRAGMA synchronous=FULL;PRAGMA foreign_keys=ON;")
   .map_err(r366_error)?;
 Ok(db)
}
pub struct R506IsolatedReplica{root:PathBuf}
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct R506MirrorState{
 pub source_seq:u64,pub sink_seq:u64,pub ack_seq:u64,
 pub source_durable:bool,pub sink_durable:bool,
 pub real_mirror:bool,
}
impl R506IsolatedReplica{
 pub fn initialize(root:&Path)->Result<Self,EventOutboxR366Error>{
  let path=r506_private_db2_path(root)?;
  use std::os::unix::fs::OpenOptionsExt;
  let f=fs::OpenOptions::new().write(true).create_new(true).mode(0o600)
   .open(&path).map_err(|_|EventOutboxR366Error::Path)?;
  f.sync_all().map_err(|_|EventOutboxR366Error::Sql)?;
  drop(f);
  let db=r506_db2_open(root)?;
  exec(&db,"PRAGMA journal_mode=WAL;PRAGMA synchronous=FULL;
   CREATE TABLE sink_control(id INTEGER PRIMARY KEY CHECK(id=1),
    applied_seq INTEGER NOT NULL);INSERT INTO sink_control VALUES(1,0);
   CREATE TABLE sink_delivery(seq INTEGER PRIMARY KEY,
    prev_seq INTEGER NOT NULL,tx_id TEXT NOT NULL UNIQUE,
    payload_hash TEXT NOT NULL,payload_json TEXT NOT NULL);
   CREATE TABLE sink_rows(table_name TEXT NOT NULL,key_json TEXT NOT NULL,
    row_json TEXT NOT NULL,after_hash TEXT NOT NULL,
    PRIMARY KEY(table_name,key_json));").map_err(r366_error)?;
  drop(db);
  fs::File::open(root).and_then(|f|f.sync_all())
   .map_err(|_|EventOutboxR366Error::Sql)?;
  Ok(Self{root:root.to_path_buf()})
 }
 pub fn reopen(root:&Path)->Result<Self,EventOutboxR366Error>{
  let db=r506_db2_open(root)?;
  for table in ["sink_control","sink_delivery","sink_rows"]{
   let mut q=Stmt::new(&db,"SELECT name FROM sqlite_master
    WHERE type='table' AND name=?1").map_err(r366_error)?;
   q.string(1,table).map_err(r366_error)?;
   if q.step().map_err(r366_error)?!=SQLITE_ROW{
    return Err(EventOutboxR366Error::Schema)
   }
  }
  let this=Self{root:root.to_path_buf()};
  this.audit()?;
  Ok(this)
 }
 pub fn audit(&self)->Result<u64,EventOutboxR366Error>{
  let db=r506_db2_open(&self.root)?;
  r506_sink_audit(&db)
 }
}
fn r506_sink_audit(db:&Db)->Result<u64,EventOutboxR366Error>{
 use serde_json::Value;
 let mut seq=0_u64;
 let mut rows=std::collections::BTreeMap::<(String,String),(String,String)>::new();
 let mut q=Stmt::new(db,"SELECT seq,prev_seq,tx_id,payload_hash,payload_json
   FROM sink_delivery ORDER BY seq").map_err(r366_error)?;
 loop{
  match q.step().map_err(r366_error)?{
   SQLITE_DONE=>break,
   SQLITE_ROW=>{
    let current=q.num(0).map_err(r366_error)?;
    let prev=q.num(1).map_err(r366_error)?;
    let txid=q.strval(2).map_err(r366_error)?;
    let hash=q.strval(3).map_err(r366_error)?;
    let payload=q.strval(4).map_err(r366_error)?;
    let frame:Value=serde_json::from_str(&payload)
      .map_err(|_|EventOutboxR366Error::Corrupt)?;
    if current!=seq+1||prev!=seq||txid.is_empty()||
     hash!=r366_sha(&payload)||frame["protocol_version"]!=1||
     frame["schema_generation"]!=1||frame["tx_kind"]!="event_append"||
     r386_python_mirror_canonical(&frame)
       .map_err(|_|EventOutboxR366Error::Corrupt)?!=payload{
      return Err(EventOutboxR366Error::Corrupt)
    }
    let ops=frame["operations"].as_array().ok_or(EventOutboxR366Error::Corrupt)?;
    if !(2..=3).contains(&ops.len()){return Err(EventOutboxR366Error::Corrupt)}
    for op in ops{
     if op["op"]!="insert"{return Err(EventOutboxR366Error::Corrupt)}
     let table=op["table"].as_str().ok_or(EventOutboxR366Error::Corrupt)?;
     if table!="attempts"&&table!="events"&&
      r426_projection_fields(table).is_none(){return Err(EventOutboxR366Error::Corrupt)}
     let row=&op["row"];
     let key=&op["key"];
     let canonical=r386_python_mirror_canonical(row)
        .map_err(|_|EventOutboxR366Error::Corrupt)?;
     if op["after_hash"]!=r366_sha(&canonical){
      return Err(EventOutboxR366Error::Corrupt)
     }
     let key_json=r386_python_mirror_canonical(key)
       .map_err(|_|EventOutboxR366Error::Corrupt)?;
     if rows.insert((table.to_string(),key_json),(canonical,
       op["after_hash"].as_str().ok_or(EventOutboxR366Error::Corrupt)?.into()))
       .is_some(){return Err(EventOutboxR366Error::Corrupt)}
    }
    seq=current;
   },
   _=>return Err(EventOutboxR366Error::Corrupt)
  }
 }
 if r366_query_num(db,"SELECT applied_seq FROM sink_control WHERE id=1")?!=seq{
  return Err(EventOutboxR366Error::Corrupt)
 }
 let mut physical=Stmt::new(db,"SELECT table_name,key_json,row_json,after_hash
   FROM sink_rows").map_err(r366_error)?;
 let mut n=0;
 loop{
  match physical.step().map_err(r366_error)?{
   SQLITE_DONE=>break,
   SQLITE_ROW=>{
    let table=physical.strval(0).map_err(r366_error)?;
    let key=physical.strval(1).map_err(r366_error)?;
    let value=physical.strval(2).map_err(r366_error)?;
    let hash=physical.strval(3).map_err(r366_error)?;
    if rows.get(&(table,key))!=Some(&(value,hash)){
     return Err(EventOutboxR366Error::Corrupt)
    }
    n+=1;
   },
   _=>return Err(EventOutboxR366Error::Corrupt)
  }
 }
 if n!=rows.len(){return Err(EventOutboxR366Error::Corrupt)}
 Ok(seq)
}
fn r506_source_frame(db:&Db,seq:u64)
 ->Result<(u64,String,String,String),EventOutboxR366Error>{
 let mut q=Stmt::new(db,"SELECT prev_seq,tx_id,payload_hash,payload_json
  FROM mirror_outbox WHERE seq=?1").map_err(r366_error)?;
 q.number(1,seq).map_err(r366_error)?;
 if q.step().map_err(r366_error)?!=SQLITE_ROW{
  return Err(EventOutboxR366Error::Conflict)
 }
 let prev=q.num(0).map_err(r366_error)?;
 let txid=q.strval(1).map_err(r366_error)?;
 let hash=q.strval(2).map_err(r366_error)?;
 let payload=q.strval(3).map_err(r366_error)?;
 if q.step().map_err(r366_error)?!=SQLITE_DONE||
  r366_sha(&payload)!=hash||prev+1!=seq{
  return Err(EventOutboxR366Error::Corrupt)
 }
 Ok((prev,txid,hash,payload))
}
fn r506_source_ack_audit(db:&Db)->Result<u64,EventOutboxR366Error>{
 let seq=r366_query_num(db,"SELECT seq FROM r506_source_ack WHERE id=1")?;
 let mut q=Stmt::new(db,"SELECT payload_hash FROM r506_source_ack WHERE id=1")
  .map_err(r366_error)?;
 if q.step().map_err(r366_error)?!=SQLITE_ROW{
  return Err(EventOutboxR366Error::Corrupt)
 }
 let saved=q.strval(0).map_err(r366_error)?;
 let wanted=if seq==0{"GENESIS".to_string()}else{
  r506_source_frame(db,seq)?.2
 };
 if saved!=wanted||
   seq>r366_query_num(db,"SELECT latest_seq FROM mirror_control WHERE id=1")?{
  return Err(EventOutboxR366Error::Corrupt)
 }
 Ok(seq)
}
impl R506IsolatedReplica{
 /// Apply exactly one source transaction. Failpoint runs inside sink TX.
 /// A previously delivered seq returns Duplicate only after full audit.
 pub fn deliver(&self,source:&IsolatedEventOutboxR366,seq:u64,
  fail_before_commit:bool)->Result<(),EventOutboxR366Error>{
  if source.path.parent()!=Some(self.root.as_path())||seq==0{
   return Err(EventOutboxR366Error::Path)
  }
  source.audit()?;
  let src=r366_open(&source.path)?;
  let (prev,txid,hash,payload)=r506_source_frame(&src,seq)?;
  drop(src);
  let already=self.audit()?;
  let sink=r506_db2_open(&self.root)?;
  if seq<=already{
   let mut q=Stmt::new(&sink,"SELECT payload_hash FROM sink_delivery
    WHERE seq=?1").map_err(r366_error)?;
   q.number(1,seq).map_err(r366_error)?;
   if q.step().map_err(r366_error)?!=SQLITE_ROW||
      q.strval(0).map_err(r366_error)?!=hash{
      return Err(EventOutboxR366Error::Corrupt)
   }
   return Err(EventOutboxR366Error::Duplicate)
  }
  if seq!=already+1||prev!=already{return Err(EventOutboxR366Error::Conflict)}
  let frame:serde_json::Value=serde_json::from_str(&payload)
    .map_err(|_|EventOutboxR366Error::Corrupt)?;
  let ops=frame["operations"].as_array().ok_or(EventOutboxR366Error::Corrupt)?;
  if !(2..=3).contains(&ops.len())||frame["tx_kind"]!="event_append"{
   return Err(EventOutboxR366Error::Corrupt)
  }
  let tx=Tx::begin(&sink).map_err(r366_error)?;
  for op in ops{
   let table=op["table"].as_str().ok_or(EventOutboxR366Error::Corrupt)?;
   if op["op"]!="insert"||
      !(table=="events"||table=="attempts"||
        r426_projection_fields(table).is_some()){
    return Err(EventOutboxR366Error::Corrupt)
   }
   let row=r386_python_mirror_canonical(&op["row"])?;
   let key=r386_python_mirror_canonical(&op["key"])?;
   let after_hash=op["after_hash"].as_str().ok_or(EventOutboxR366Error::Corrupt)?;
   if r366_sha(&row)!=after_hash{
    return Err(EventOutboxR366Error::Corrupt)
   }
   let mut q=Stmt::new(&sink,"INSERT INTO sink_rows(
    table_name,key_json,row_json,after_hash) VALUES(?1,?2,?3,?4)")
     .map_err(r366_error)?;
   for (i,s) in [table,key.as_str(),row.as_str(),after_hash].iter().enumerate(){
    q.string(i as i32+1,s).map_err(r366_error)?;
   }
   if q.step().map_err(r366_error)?!=SQLITE_DONE{
    return Err(EventOutboxR366Error::Conflict)
   }
  }
  let mut q=Stmt::new(&sink,"INSERT INTO sink_delivery(seq,prev_seq,tx_id,
   payload_hash,payload_json) VALUES(?1,?2,?3,?4,?5)").map_err(r366_error)?;
  q.number(1,seq).map_err(r366_error)?;
  q.number(2,prev).map_err(r366_error)?;
  for (i,s) in [txid.as_str(),hash.as_str(),payload.as_str()].iter().enumerate(){
   q.string(i as i32+3,s).map_err(r366_error)?;
  }
  if q.step().map_err(r366_error)?!=SQLITE_DONE{
   return Err(EventOutboxR366Error::Sql)
  }
  drop(q);
  let mut update=Stmt::new(&sink,"UPDATE sink_control SET applied_seq=?1
   WHERE id=1 AND applied_seq=?2").map_err(r366_error)?;
  update.number(1,seq).map_err(r366_error)?;
  update.number(2,prev).map_err(r366_error)?;
  if update.step().map_err(r366_error)?!=SQLITE_DONE||
     unsafe{sqlite3_changes(sink.0)}!=1{
    return Err(EventOutboxR366Error::Conflict)
  }
  drop(update);
  if fail_before_commit{return Err(EventOutboxR366Error::Interrupted)}
  tx.commit().map_err(r366_error)?;
  Ok(())
 }
 pub fn acknowledge(&self,source:&IsolatedEventOutboxR366,
  seq:u64,fail_before_commit:bool)->Result<(),EventOutboxR366Error>{
  if source.path.parent()!=Some(self.root.as_path())||seq==0{
   return Err(EventOutboxR366Error::Path)
  }
  source.audit()?;
  let sink_seq=self.audit()?;
  if seq>sink_seq{return Err(EventOutboxR366Error::Conflict)}
  let sink=r506_db2_open(&self.root)?;
  let mut found=Stmt::new(&sink,"SELECT payload_hash FROM sink_delivery
   WHERE seq=?1").map_err(r366_error)?;
  found.number(1,seq).map_err(r366_error)?;
  if found.step().map_err(r366_error)?!=SQLITE_ROW{
   return Err(EventOutboxR366Error::Corrupt)
  }
  let sink_hash=found.strval(0).map_err(r366_error)?;
  drop(found);
  let db=r366_open(&source.path)?;
  let tx=Tx::begin(&db).map_err(r366_error)?;
  let previous=r506_source_ack_audit(&db)?;
  if seq<=previous{
   return Err(if seq==previous{EventOutboxR366Error::Duplicate}
      else{EventOutboxR366Error::Conflict})
  }
  if seq!=previous+1{return Err(EventOutboxR366Error::Conflict)}
  let source_hash=r506_source_frame(&db,seq)?.2;
  if source_hash!=sink_hash{return Err(EventOutboxR366Error::Corrupt)}
  let mut q=Stmt::new(&db,"UPDATE r506_source_ack SET seq=?1,payload_hash=?2
    WHERE id=1 AND seq=?3").map_err(r366_error)?;
  q.number(1,seq).map_err(r366_error)?;
  q.string(2,&source_hash).map_err(r366_error)?;
  q.number(3,previous).map_err(r366_error)?;
  if q.step().map_err(r366_error)?!=SQLITE_DONE||
    unsafe{sqlite3_changes(db.0)}!=1{
   return Err(EventOutboxR366Error::Conflict)
  }
  drop(q);
  if fail_before_commit{return Err(EventOutboxR366Error::Interrupted)}
  tx.commit().map_err(r366_error)
 }
 pub fn inspect(&self,source:&IsolatedEventOutboxR366)
  ->Result<R506MirrorState,EventOutboxR366Error>{
  if source.path.parent()!=Some(self.root.as_path()){
   return Err(EventOutboxR366Error::Path)
  }
  let audit=source.audit()?;
  let db=r366_open(&source.path)?;
  let ack=r506_source_ack_audit(&db)?;
  let delivered=self.audit()?;
  if ack>delivered{return Err(EventOutboxR366Error::Corrupt)}
  // Independently link source and sink payload hashes at every receipt.
  let sink=r506_db2_open(&self.root)?;
  for seq in 1..=delivered{
   let source_hash=r506_source_frame(&db,seq)?.2;
   let mut q=Stmt::new(&sink,"SELECT payload_hash FROM sink_delivery
     WHERE seq=?1").map_err(r366_error)?;
   q.number(1,seq).map_err(r366_error)?;
   if q.step().map_err(r366_error)?!=SQLITE_ROW||
     q.strval(0).map_err(r366_error)?!=source_hash{
    return Err(EventOutboxR366Error::Corrupt)
   }
  }
  Ok(R506MirrorState{source_seq:audit.last_seq,sink_seq:delivered,
   ack_seq:ack,source_durable:true,sink_durable:true,real_mirror:false})
 }
}
#[cfg(test)]
mod r506_tests{
 use super::*;
 use super::r366_tests::{fixture_root,record};
 const T:u64=1_792_660_000_000;
 fn setup()->(PathBuf,IsolatedEventOutboxR366,R506IsolatedReplica){
  let root=fixture_root();
  let src=IsolatedEventOutboxR366::initialize(&root).unwrap();
  let sink=R506IsolatedReplica::initialize(&root).unwrap();
  (root,src,sink)
 }
 fn append(src:&IsolatedEventOutboxR366,n:u64){
  src.append(&record(&format!("r506_evt_{n}"),"decisions"),
   T+n,false).unwrap();
 }
 #[test]fn r506_two_db_chain_source_then_sink_then_durable_ack(){
  let (root,src,sink)=setup();
  append(&src,1);
  let a=sink.inspect(&src).unwrap();
  assert_eq!((a.source_seq,a.sink_seq,a.ack_seq),(1,0,0));
  assert!(!a.real_mirror);
  sink.deliver(&src,1,false).unwrap();
  let a=sink.inspect(&src).unwrap();
  assert_eq!((a.source_seq,a.sink_seq,a.ack_seq),(1,1,0));
  sink.acknowledge(&src,1,false).unwrap();
  assert_eq!(sink.inspect(&src).unwrap().ack_seq,1);
  assert_eq!((src.audit().unwrap().events,src.audit().unwrap().outbox),(1,1));
  drop(sink);drop(src);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r507_db2_commit_before_ack_lost_is_replay_safe(){
  let (root,src,sink)=setup();
  append(&src,1);sink.deliver(&src,1,false).unwrap();
  assert_eq!(sink.acknowledge(&src,1,true),
    Err(EventOutboxR366Error::Interrupted));
  assert_eq!(sink.inspect(&src).unwrap().ack_seq,0);
  assert_eq!(sink.deliver(&src,1,false),
    Err(EventOutboxR366Error::Duplicate));
  sink.acknowledge(&src,1,false).unwrap();
  assert_eq!(sink.inspect(&src).unwrap().ack_seq,1);
  drop(sink);drop(src);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r508_interrupt_before_db2_commit_rolls_back_every_row(){
  let (root,src,sink)=setup();append(&src,1);
  assert_eq!(sink.deliver(&src,1,true),
    Err(EventOutboxR366Error::Interrupted));
  assert_eq!(sink.audit().unwrap(),0);
  let db=r506_db2_open(&root).unwrap();
  assert_eq!(r366_query_num(&db,"SELECT COUNT(*) FROM sink_rows").unwrap(),0);
  assert_eq!(r366_query_num(&db,"SELECT COUNT(*) FROM sink_delivery").unwrap(),0);
  drop(db);
  sink.deliver(&src,1,false).unwrap();
  assert_eq!(sink.inspect(&src).unwrap().sink_seq,1);
  drop(sink);drop(src);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r509_ack_without_db2_commit_refused(){
  let (root,src,sink)=setup();append(&src,1);
  assert_eq!(sink.acknowledge(&src,1,false),
    Err(EventOutboxR366Error::Conflict));
  assert_eq!(sink.inspect(&src).unwrap().ack_seq,0);
  drop(sink);drop(src);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r510_source_rollback_before_outbox_never_reaches_db2(){
  let (root,src,sink)=setup();
  assert_eq!(src.append(&record("r510_failed","decisions"),T,true),
    Err(EventOutboxR366Error::Interrupted));
  assert_eq!(src.audit().unwrap().last_seq,0);
  assert_eq!(sink.deliver(&src,1,false),Err(EventOutboxR366Error::Conflict));
  assert_eq!(sink.audit().unwrap(),0);
  drop(sink);drop(src);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r511_missing_predecessor_and_unordered_ack_refused(){
  let (root,src,sink)=setup();
  append(&src,1);append(&src,2);
  assert_eq!(sink.deliver(&src,2,false),
    Err(EventOutboxR366Error::Conflict));
  sink.deliver(&src,1,false).unwrap();
  sink.deliver(&src,2,false).unwrap();
  assert_eq!(sink.acknowledge(&src,2,false),
    Err(EventOutboxR366Error::Conflict));
  sink.acknowledge(&src,1,false).unwrap();
  sink.acknowledge(&src,2,false).unwrap();
  assert_eq!(sink.inspect(&src).unwrap().ack_seq,2);
  drop(sink);drop(src);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r512_duplicate_delivery_and_ack_cannot_double_count(){
  let (root,src,sink)=setup();append(&src,1);
  sink.deliver(&src,1,false).unwrap();
  sink.acknowledge(&src,1,false).unwrap();
  assert_eq!(sink.deliver(&src,1,false),
    Err(EventOutboxR366Error::Duplicate));
  assert_eq!(sink.acknowledge(&src,1,false),
    Err(EventOutboxR366Error::Duplicate));
  assert_eq!(r366_query_num(&r506_db2_open(&root).unwrap(),
    "SELECT COUNT(*) FROM sink_delivery").unwrap(),1);
  drop(sink);drop(src);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r513_corrupted_db2_after_image_and_receipt_detected(){
  let (root,src,sink)=setup();append(&src,1);
  sink.deliver(&src,1,false).unwrap();
  let db=r506_db2_open(&root).unwrap();
  exec(&db,"UPDATE sink_rows SET after_hash='altered' WHERE table_name='events'")
   .unwrap();
  drop(db);
  assert_eq!(sink.audit(),Err(EventOutboxR366Error::Corrupt));
  assert!(matches!(sink.acknowledge(&src,1,false),
    Err(EventOutboxR366Error::Corrupt)));
  drop(sink);drop(src);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r514_source_ack_tamper_detected_after_commit(){
  let (root,src,sink)=setup();append(&src,1);
  sink.deliver(&src,1,false).unwrap();
  sink.acknowledge(&src,1,false).unwrap();
  let db=r366_open(&src.path).unwrap();
  exec(&db,"UPDATE r506_source_ack SET payload_hash='altered' WHERE id=1").unwrap();
  drop(db);
  assert_eq!(src.audit(),Err(EventOutboxR366Error::Corrupt));
  assert!(matches!(IsolatedEventOutboxR366::reopen(&root),
   Err(EventOutboxR366Error::Corrupt)));
  drop(sink);drop(src);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r515_both_sqlite_files_reopen_and_reconcile(){
  let (root,src,sink)=setup();append(&src,1);append(&src,2);
  sink.deliver(&src,1,false).unwrap();
  sink.acknowledge(&src,1,false).unwrap();
  sink.deliver(&src,2,false).unwrap();
  drop(src);drop(sink);
  let src=IsolatedEventOutboxR366::reopen(&root).unwrap();
  let sink=R506IsolatedReplica::reopen(&root).unwrap();
  let before=sink.inspect(&src).unwrap();
  assert_eq!((before.source_seq,before.sink_seq,before.ack_seq),(2,2,1));
  sink.acknowledge(&src,2,false).unwrap();
  assert_eq!(sink.inspect(&src).unwrap().ack_seq,2);
  drop(src);drop(sink);fs::remove_dir_all(root).unwrap();
 }
}
