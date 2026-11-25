// R366: fail-closed isolated PAPER event/outbox atomicity witness.
// Python collector-inspired *subset*. Never writes production DB1 or DB2,
// never issues orders, and never treats mirror outbox as DB2 delivery.
#[derive(Debug,Clone,PartialEq,Eq)]
pub enum EventOutboxR366Error{Path,Schema,Sql,Duplicate,Conflict,Interrupted,Corrupt,Pressure}
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct EventOutboxR366Audit{
 pub events:u64,pub category_rows:u64,pub outbox:u64,
 pub last_seq:u64,pub pending_bootstrap:bool,
 pub installed_paper_writer:bool,pub db2_applied:bool,pub real_authority:bool,
}
pub struct IsolatedEventOutboxR366{path:PathBuf}
fn r366_error(_:MirrorDbError)->EventOutboxR366Error{EventOutboxR366Error::Sql}
fn r366_path(path:&Path)->Result<(),EventOutboxR366Error>{
 if path.file_name().and_then(|s|s.to_str())!=Some("astra-r366-event-outbox.sqlite"){
  return Err(EventOutboxR366Error::Path)
 }
 let parent=path.parent().ok_or(EventOutboxR366Error::Path)?;
 if parent.parent()!=Some(std::env::temp_dir().as_path())||
    !parent.file_name().and_then(|s|s.to_str())
      .is_some_and(|s|s.starts_with("astra_r366_isolated_")&&s.len()<=100){
  return Err(EventOutboxR366Error::Path)
 }
 let m=fs::symlink_metadata(parent).map_err(|_|EventOutboxR366Error::Path)?;
 if !m.is_dir()||m.mode()&0o077!=0||m.uid()!=unsafe{libc::geteuid()}{
  return Err(EventOutboxR366Error::Path)
 }
 if let Ok(m)=fs::symlink_metadata(path){
  if !m.file_type().is_file()||m.nlink()!=1||
     m.mode()&0o077!=0||m.uid()!=unsafe{libc::geteuid()}{
    return Err(EventOutboxR366Error::Path)
  }
 }
 Ok(())
}
fn r366_open(path:&Path)->Result<Db,EventOutboxR366Error>{
 r366_path(path)?;
 let name=CString::new(path.to_string_lossy().as_bytes())
   .map_err(|_|EventOutboxR366Error::Path)?;
 let mut p=std::ptr::null_mut();
 let rc=unsafe{sqlite3_open_v2(name.as_ptr(),&mut p,READWRITE_EXISTING,std::ptr::null())};
 if rc!=SQLITE_OK||p.is_null(){
  if !p.is_null(){unsafe{sqlite3_close(p);}}
  return Err(EventOutboxR366Error::Sql)
 }
 let db=Db(p);
 if unsafe{sqlite3_busy_timeout(db.0,1000)}!=SQLITE_OK{return Err(EventOutboxR366Error::Sql)}
 exec(&db,"PRAGMA foreign_keys=ON;PRAGMA synchronous=FULL;").map_err(r366_error)?;
 // max_page_count is connection-scoped: reapply on every private SQLite open.
 r436_install_private_quotas(&db)?;
 Ok(db)
}
fn r366_sha(s:&str)->String{
 openssl::sha::sha256(s.as_bytes()).iter().map(|x|format!("{x:02x}")).collect()
}
fn r366_query_num(db:&Db,sql:&str)->Result<u64,EventOutboxR366Error>{
 let mut q=Stmt::new(db,sql).map_err(r366_error)?;
 if q.step().map_err(r366_error)?!=SQLITE_ROW{return Err(EventOutboxR366Error::Corrupt)}
 let n=q.num(0).map_err(r366_error)?;
 if q.step().map_err(r366_error)?!=SQLITE_DONE{return Err(EventOutboxR366Error::Corrupt)}
 Ok(n)
}
fn r366_field<'a>(v:&'a serde_json::Value,key:&str)->Result<&'a str,EventOutboxR366Error>{
 v.get(key).and_then(serde_json::Value::as_str).ok_or(EventOutboxR366Error::Schema)
}
impl IsolatedEventOutboxR366{
 pub fn initialize(root:&Path)->Result<Self,EventOutboxR366Error>{
  let path=root.join("astra-r366-event-outbox.sqlite");
  r366_path(&path)?;
  use std::os::unix::fs::OpenOptionsExt;
  let f=fs::OpenOptions::new().write(true).create_new(true).mode(0o600)
    .open(&path).map_err(|_|EventOutboxR366Error::Path)?;
  f.sync_all().map_err(|_|EventOutboxR366Error::Sql)?;
  drop(f);
  let db=r366_open(&path)?;
  exec(&db,"PRAGMA journal_mode=WAL;PRAGMA synchronous=FULL;PRAGMA wal_autocheckpoint=32;")
    .map_err(r366_error)?;
  let tx=Tx::begin(&db).map_err(r366_error)?;
  exec(&db,"CREATE TABLE attempts(
     correlation_id TEXT PRIMARY KEY,mode TEXT NOT NULL CHECK(mode='paper'));
     CREATE TABLE events(event_id TEXT PRIMARY KEY,
      schema_version INTEGER NOT NULL CHECK(schema_version=1),
      mode TEXT NOT NULL CHECK(mode='paper'),category TEXT NOT NULL,
      name TEXT NOT NULL,source TEXT NOT NULL,run_id TEXT NOT NULL,
      occurred_at_ms INTEGER NOT NULL,verification_status TEXT NOT NULL,
      is_exchange_verified INTEGER NOT NULL CHECK(is_exchange_verified=0),
      payload_json TEXT NOT NULL,verification_json TEXT NOT NULL,
      source_record_json TEXT NOT NULL,content_hash TEXT NOT NULL,
      received_at_ms INTEGER NOT NULL,
      correlation_id TEXT,
      FOREIGN KEY(correlation_id) REFERENCES attempts(correlation_id));
     CREATE TABLE mirror_outbox(seq INTEGER PRIMARY KEY AUTOINCREMENT,
      prev_seq INTEGER NOT NULL,tx_id TEXT NOT NULL UNIQUE,
      tx_kind TEXT NOT NULL,schema_generation INTEGER NOT NULL,
      payload_json TEXT NOT NULL,payload_hash TEXT NOT NULL,
      payload_bytes INTEGER NOT NULL,committed_at_ms INTEGER NOT NULL,
      feed_generation TEXT NOT NULL);
     CREATE TABLE mirror_control(id INTEGER PRIMARY KEY CHECK(id=1),
      state TEXT NOT NULL CHECK(state IN (
       'BOOTSTRAP_REQUIRED','NORMAL','REBUILD_REQUIRED')),
      latest_seq INTEGER NOT NULL,retained_bytes INTEGER NOT NULL,
      retained_records INTEGER NOT NULL,feed_generation TEXT NOT NULL);
     INSERT INTO mirror_control VALUES(1,'BOOTSTRAP_REQUIRED',0,0,0,'r366-isolated');
     PRAGMA user_version=426;").map_err(r366_error)?;
  r426_category_schema(&db)?;
  r435_schema(&db)?;
  r466_schema(&db)?;
  r476_schema(&db)?;
  r506_schema(&db)?;
  tx.commit().map_err(r366_error)?;
  fs::File::open(root).and_then(|f|f.sync_all())
    .map_err(|_|EventOutboxR366Error::Sql)?;
  Ok(Self{path})
 }
 pub fn reopen(root:&Path)->Result<Self,EventOutboxR366Error>{
  let path=root.join("astra-r366-event-outbox.sqlite");
  let db=r366_open(&path)?;
  if r366_query_num(&db,"PRAGMA user_version")?!=426{
   return Err(EventOutboxR366Error::Schema)
  }
  for table in ["events","attempts","mirror_control","mirror_outbox",
     "collector_runs","collector_run_journal",
     "retention_preparations","retention_preparation_journal",
     "retention_shadow_copies","retention_shadow_tombstones",
     "retention_shadow_rollups","retention_shadow_metadata",
     "retention_shadow_outbox","r506_source_ack"]
    .into_iter().chain(R426_CATEGORY_FIELDS.iter().map(|(name,_)|*name)){
   let mut q=Stmt::new(&db,"SELECT name FROM sqlite_master WHERE type='table' AND name=?1")
    .map_err(r366_error)?;
   q.string(1,table).map_err(r366_error)?;
   if q.step().map_err(r366_error)?!=SQLITE_ROW{return Err(EventOutboxR366Error::Schema)}
  }
  let this=Self{path};
  this.audit()?;
  Ok(this)
 }
 /// Record must be prevalidated by r356_paper_event_record_without_io.
 /// Outbox protocol shape is source inspired; no DB2/remote mirror consumption.
 pub fn append(&self,record:&serde_json::Value,received_at_ms:u64,
   interrupt_before_outbox:bool)->Result<(),EventOutboxR366Error>{
  if record["mode"]!="paper"||record["is_exchange_verified"]!=0||
    record["schema_version"]!=1{return Err(EventOutboxR366Error::Schema)}
  let category=r366_field(record,"category")?;
  if r426_projection_fields(category).is_none(){return Err(EventOutboxR366Error::Schema)}
  let id=r366_field(record,"event_id")?;
  if id.is_empty()||id.len()>160{return Err(EventOutboxR366Error::Schema)}
  let payload:serde_json::Value=serde_json::from_str(r366_field(record,"payload_json")?)
     .map_err(|_|EventOutboxR366Error::Schema)?;
  if !payload.is_object(){return Err(EventOutboxR366Error::Schema)}
  let raw=r386_python_collector_json(record)?;
  if raw.len()>16_384||received_at_ms==0{return Err(EventOutboxR366Error::Schema)}
  let sha=r366_sha(&raw);
  // Fail closed on private volume/WAL pressure and source/outbox corruption.
  r436_admit_private(&self.path)?;
  self.audit()?;
  let db=r366_open(&self.path)?;
  let tx=Tx::begin(&db).map_err(r366_error)?;
  let mut check=Stmt::new(&db,"SELECT content_hash FROM events WHERE event_id=?1")
    .map_err(r366_error)?;
  check.string(1,id).map_err(r366_error)?;
  match check.step().map_err(r366_error)?{
   SQLITE_ROW=>{
    let old=check.strval(0).map_err(r366_error)?;
    return Err(if old==sha{EventOutboxR366Error::Duplicate}
           else{EventOutboxR366Error::Conflict})
   },
   SQLITE_DONE=>{},_=>return Err(EventOutboxR366Error::Corrupt)
  }
  drop(check);
  r430_dedup_before_insert(&db,record,&payload)?;
  let correlation=record["correlation_id"].as_str();
  let mut attempt_inserted=false;
  if let Some(c)=correlation{
   let mut q=Stmt::new(&db,
      "INSERT OR IGNORE INTO attempts(correlation_id,mode) VALUES(?1,'paper')")
     .map_err(r366_error)?;
   q.string(1,c).map_err(r366_error)?;
   if q.step().map_err(r366_error)?!=SQLITE_DONE{return Err(EventOutboxR366Error::Sql)}
   attempt_inserted=unsafe{sqlite3_changes(db.0)}==1;
   drop(q);
  }
  let mut q=Stmt::new(&db,"INSERT INTO events(event_id,schema_version,mode,
   category,name,source,run_id,occurred_at_ms,verification_status,
   is_exchange_verified,payload_json,verification_json,source_record_json,
   content_hash,received_at_ms,correlation_id)
   VALUES(?1,1,'paper',?2,?3,?4,?5,?6,?7,0,?8,?9,?10,?11,?12,?13)")
   .map_err(r366_error)?;
  for (i,k) in ["event_id","category","name","source","run_id"].iter().enumerate(){
   q.string(i as i32+1,r366_field(record,k)?).map_err(r366_error)?;
  }
  q.number(6,record["occurred_at_ms"].as_u64().ok_or(EventOutboxR366Error::Schema)?)
    .map_err(r366_error)?;
  for (i,k) in ["verification_status","payload_json","verification_json"].iter().enumerate(){
   q.string(i as i32+7,r366_field(record,k)?).map_err(r366_error)?;
  }
  q.string(10,&raw).map_err(r366_error)?;
  q.string(11,&sha).map_err(r366_error)?;
  q.number(12,received_at_ms).map_err(r366_error)?;
  if let Some(c)=correlation{
    q.string(13,c).map_err(r366_error)?;
  }
  if q.step().map_err(r366_error)?!=SQLITE_DONE{return Err(EventOutboxR366Error::Sql)}
  drop(q);
  r426_category_insert(&db,category,id,&payload)?;
  if interrupt_before_outbox{return Err(EventOutboxR366Error::Interrupted)}
  #[cfg(test)]
  if std::env::var("ASTRA_R396_PRIVATE_CHILD_SIGKILL").ok().as_deref()
     ==Some("OWNED_SQLITE_TX_TEST_ONLY")
     &&self.path.parent().and_then(|p|p.file_name())
       .and_then(|s|s.to_str()).is_some_and(|s|s.starts_with("astra_r366_isolated_")){
   unsafe{libc::raise(libc::SIGKILL);}
   return Err(EventOutboxR366Error::Interrupted)
  }
  // AFTER images are fetched from real SQLite rows in THIS open transaction.
  // An ignored attempt INSERT has no source mutation and must not be replayed.
  let mut ops=Vec::new();
  if attempt_inserted{
    ops.push(r406_source_after(&db,"attempts",
      correlation.ok_or(EventOutboxR366Error::Corrupt)?,"insert")?);
  }
  ops.push(r406_source_after(&db,"events",id,"insert")?);
  ops.push(r406_source_after(&db,category,id,"insert")?);
  let frame=serde_json::json!({"protocol_version":1,"schema_generation":1,
    "tx_kind":"event_append","operations":ops});
  let serialized=r386_python_mirror_canonical(&frame)?;
  let bytes=serialized.len().saturating_add(384);
  if bytes>4*1024*1024{return Err(EventOutboxR366Error::Schema)}
  let prior=r366_query_num(&db,"SELECT latest_seq FROM mirror_control WHERE id=1")?;
  let seq=prior.checked_add(1).ok_or(EventOutboxR366Error::Schema)?;
  let txid=format!("tx-r366-{seq}-{id}");
  let mut out=Stmt::new(&db,"INSERT INTO mirror_outbox(
   seq,prev_seq,tx_id,tx_kind,schema_generation,payload_json,payload_hash,
   payload_bytes,committed_at_ms,feed_generation
   ) VALUES(?1,?2,?3,'event_append',1,?4,?5,?6,?7,'r366-isolated')")
   .map_err(r366_error)?;
  out.number(1,seq).map_err(r366_error)?;
  out.number(2,prior).map_err(r366_error)?;
  out.string(3,&txid).map_err(r366_error)?;
  out.string(4,&serialized).map_err(r366_error)?;
  out.string(5,&r366_sha(&serialized)).map_err(r366_error)?;
  out.number(6,bytes as u64).map_err(r366_error)?;
  out.number(7,received_at_ms).map_err(r366_error)?;
  if out.step().map_err(r366_error)?!=SQLITE_DONE{return Err(EventOutboxR366Error::Sql)}
  drop(out);
  let mut control=Stmt::new(&db,"UPDATE mirror_control SET latest_seq=?1,
   retained_bytes=retained_bytes+?2,retained_records=retained_records+1 WHERE id=1")
   .map_err(r366_error)?;
  control.number(1,seq).map_err(r366_error)?;
  control.number(2,bytes as u64).map_err(r366_error)?;
  if control.step().map_err(r366_error)?!=SQLITE_DONE{return Err(EventOutboxR366Error::Sql)}
  drop(control);
  tx.commit().map_err(r366_error)
 }
 pub fn audit(&self)->Result<EventOutboxR366Audit,EventOutboxR366Error>{
  let db=r366_open(&self.path)?;
  if r366_query_num(&db,"PRAGMA user_version")?!=426{return Err(EventOutboxR366Error::Schema)}
  r376_integrity(&db)?;
  r435_run_audit(&db)?;
  r466_audit(&db)?;
  r476_shadow_audit(&db)?;
  r506_source_ack_audit(&db)?;
  let events=r366_query_num(&db,"SELECT COUNT(*) FROM events")?;
  let mut categories=0u64;
  for (cat,_) in R426_CATEGORY_FIELDS{
   categories=categories.checked_add(r366_query_num(&db,
     &format!("SELECT COUNT(*) FROM {cat}"))?).ok_or(EventOutboxR366Error::Corrupt)?;
  }
  let outbox=r366_query_num(&db,"SELECT COUNT(*) FROM mirror_outbox")?;
  let control=r366_query_num(&db,"SELECT latest_seq FROM mirror_control WHERE id=1")?;
  let bad=r366_query_num(&db,"SELECT COUNT(*) FROM events e WHERE
    NOT EXISTS(SELECT 1 FROM mirror_outbox o WHERE o.tx_id LIKE ('%-'||e.event_id))")?;
  let bad_link=r366_query_num(&db,"SELECT COUNT(*) FROM mirror_outbox o
    WHERE o.prev_seq != o.seq-1 OR o.payload_hash='' OR o.tx_kind!='event_append'")?;
  if events!=categories||events!=outbox||control!=outbox||bad!=0||bad_link!=0{
   return Err(EventOutboxR366Error::Corrupt)
  }
  Ok(EventOutboxR366Audit{events,category_rows:categories,outbox,
    last_seq:control,pending_bootstrap:true,
    installed_paper_writer:false,db2_applied:false,real_authority:false})
 }
}

#[cfg(test)]
mod r366_tests{
 use super::*;
 use serde_json::{json,Value};
 use std::sync::atomic::{AtomicU64,Ordering};
 static N:AtomicU64=AtomicU64::new(0);
 pub(super) fn fixture_root()->PathBuf{
  use std::os::unix::fs::DirBuilderExt;
  let p=std::env::temp_dir().join(format!("astra_r366_isolated_{}_{}",
   std::process::id(),N.fetch_add(1,Ordering::Relaxed)));
  fs::DirBuilder::new().mode(0o700).create(&p).unwrap();p
 }
 fn event(id:&str,cat:&str)->Value{
  let (stage,payload)=if cat=="errors"{
   ("ERROR",json!({"module":"watcher","error_type":"public_data"}))
  }else{
   ("DECISION_MADE",json!({"decision":"hold","skip_reason":"spread"}))
  };
  json!({"mode":"paper","category":cat,"name":stage,"event_id":id,
   "source":"native.paper","run_id":"test_run",
   "occurred_at_ms":1_792_660_000_000_u64,
   "correlation_id":null,"signal_id":null,"base":"ONE",
   "shadow_group_id":null,"comparison_id":null,"reason_code":null,
   "payload":payload,"verification":{"status":"observed"}})
 }
 pub(super) fn record(id:&str,cat:&str)->Value{
  r356_paper_event_record_without_io(&event(id,cat),&[],1_792_660_000_000).unwrap()
 }
 #[test]fn r366_atomic_source_event_category_outbox_reopen(){
  let root=fixture_root();
  let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
  w.append(&record("r366_001","decisions"),1_792_660_000_000,false).unwrap();
  w.append(&record("r366_002","errors"),1_792_660_000_100,false).unwrap();
  drop(w);
  let a=IsolatedEventOutboxR366::reopen(&root).unwrap().audit().unwrap();
  assert_eq!((a.events,a.category_rows,a.outbox,a.last_seq),(2,2,2,2));
  assert!(a.pending_bootstrap&&!a.db2_applied&&!a.real_authority);
  fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r366_duplicate_and_conflict_do_not_increment_outbox(){
  let root=fixture_root();let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
  let a=record("r366_001","decisions");
  w.append(&a,1_792_660_000_000,false).unwrap();
  assert_eq!(w.append(&a,1_792_660_000_123,false),
   Err(EventOutboxR366Error::Duplicate));
  let mut b=a.clone();b["name"]=json!("PRECHECK_DENY");
  assert_eq!(w.append(&b,1_792_660_000_123,false),
   Err(EventOutboxR366Error::Conflict));
  assert_eq!(w.audit().unwrap().last_seq,1);
  drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r366_failpoint_rolls_back_source_rows_and_outbox(){
  let root=fixture_root();let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
  assert_eq!(w.append(&record("r366_003","decisions"),
      1_792_660_000_000,true),Err(EventOutboxR366Error::Interrupted));
  assert_eq!(w.audit().unwrap().events,0);
  assert_eq!(w.audit().unwrap().outbox,0);
  w.append(&record("r366_003","decisions"),1_792_660_000_000,false).unwrap();
  assert_eq!(w.audit().unwrap().last_seq,1);
  drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r366_refuses_real_and_unimplemented_categories(){
  let root=fixture_root();let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
  let mut v=record("r366_004","decisions");v["mode"]=json!("real");
  assert_eq!(w.append(&v,1_792_660_000_000,false),Err(EventOutboxR366Error::Schema));
  v=record("r366_004","decisions");v["category"]=json!("not_a_category");
  assert_eq!(w.append(&v,1_792_660_000_000,false),Err(EventOutboxR366Error::Schema));
  assert_eq!(w.audit().unwrap().events,0);
  drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r366_refuses_unisolated_existing_db(){
  assert!(matches!(IsolatedEventOutboxR366::reopen(&std::env::temp_dir()),
    Err(EventOutboxR366Error::Path)));
 }
}
