// Astra R296: strictly isolated PAPER-only SQLite event + outbox prototype.
// Included inside R147's private sqlite FFI context to REUSE Db, Stmt, Tx,
// and transactional safety. This is NOT the full Python analytics schema,
// not live DB1/DB2, and never writes existing operator paths.
include!("native_paper_event_scrub_r336_review.rs");
include!("native_paper_event_envelope_r356_review.rs");
include!("native_isolated_event_outbox_r366_review.rs");
include!("native_isolated_event_integrity_r376_review.rs");
include!("native_python_json_canonical_r386_review.rs");
include!("native_python_ordered_inner_r396_review.rs");
include!("native_isolated_sigkill_r396_review.rs");
include!("native_ordered_event_pipeline_r406_review.rs");
include!("native_isolated_after_images_r406_review.rs");
include!("native_python_collector_contract_r426_review.rs");
include!("native_python_collector_dedup_r430_review.rs");
include!("native_isolated_collector_runs_r435_review.rs");
include!("native_isolated_storage_gate_r436_review.rs");
include!("native_isolated_wal_stress_r442_review.rs");
include!("native_isolated_retention_preview_r446_review.rs");
include!("native_isolated_rollup_preview_r456_review.rs");
include!("native_isolated_retention_prepare_r466_review.rs");
include!("native_isolated_retention_shadow_r476_review.rs");
include!("native_python_retention_ops_r486_review.rs");
include!("native_python_rollup_multibatch_r496_review.rs");
include!("native_isolated_db2_ack_r506_review.rs");
include!("native_python_mirror_delivery_r516_review.rs");
include!("native_isolated_cleanup_preview_r526_review.rs");
include!("native_python_mirror_rebaseline_r536_review.rs");
include!("native_isolated_baseline_witness_r546_review.rs");
include!("native_isolated_snapshot_manifest_r556_review.rs");
include!("native_original_collector_schema_r566_review.rs");
include!("native_original_mirror_operations_r586_review.rs");
include!("native_original_17table_ack_r659_review.rs");
include!("native_original_17table_file_r671_review.rs");
include!("native_original_17table_batches_r696_review.rs");
include!("native_original_python_contract_r726_review.rs");
include!("native_original_physical_python_r756_review.rs");
include!("native_original_python_writer_r786_review.rs");
include!("native_python_rust_bootstrap_r790_review.rs");
include!("native_original_python_load_r796_review.rs");
include!("native_original_streaming_parity_r800_review.rs");
include!("native_original_window_service_r805_review.rs");
include!("native_r813_live_paper_status_bridge_review.rs");
include!("native_r819_durable_original_paper_sink_review.rs");
include!("native_r1130_stage_profile_review.rs");
include!("native_r824_batched_paper_history_review.rs");
include!("native_r830_incremental_original_writer_review.rs");
include!("native_r834_private_owner_recovery_review.rs");
include!("native_r841_sigkill_original_owner_review.rs");
include!("native_r845_burst_original_mirror_review.rs");
include!("native_r856_private_drain_worker_review.rs");
include!("native_r860_checkpoint_ack_review.rs");
include!("native_r868_atomic_owner_publication_review.rs");
include!("native_r872_source_drain_capacity_review.rs");
include!("native_r875_stage_profile_review.rs");
include!("native_r876_two_process_paced_review.rs");
include!("native_r879_private_paper_prefix_ack_review.rs");
include!("native_r1066_private_paper_collector_lane_review.rs");
include!("native_r1069_batched_paper_canary_review.rs");
#[derive(Clone,Debug)]
pub struct PaperAnalyticsEventR296 {
 pub event_id:String,
 pub category:String,
 pub payload:serde_json::Value,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum AnalyticsR296Error{Path,Schema,Sql,Duplicate,Conflict,Interrupted,Corrupt}
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct AnalyticsR296Audit{
 pub events:u64,pub outbox:u64,pub mismatched:u64,
 pub real_events:u64,pub may_trade:bool,pub production_writer:bool,
}
pub struct IsolatedAnalyticsR296{path:PathBuf}
fn analytics_r296_path(path:&Path)->Result<(),AnalyticsR296Error>{
 if path.file_name().and_then(|s|s.to_str())!=Some("astra-r296-analytics.sqlite"){
  return Err(AnalyticsR296Error::Path)
 }
 let parent=path.parent().ok_or(AnalyticsR296Error::Path)?;
 if parent.parent()!=Some(std::env::temp_dir().as_path())
 ||!parent.file_name().and_then(|s|s.to_str()).is_some_and(|s|
    s.starts_with("astra_r296_analytics_")&&s.len()<=96){
  return Err(AnalyticsR296Error::Path)
 }
 let m=fs::symlink_metadata(parent).map_err(|_|AnalyticsR296Error::Path)?;
 if !m.file_type().is_dir()||m.mode()&0o077!=0
 ||m.uid()!=unsafe{libc::geteuid()}{
  return Err(AnalyticsR296Error::Path)
 }
 if let Ok(m)=fs::symlink_metadata(path){
  if !m.file_type().is_file()||m.nlink()!=1||m.mode()&0o077!=0
  ||m.uid()!=unsafe{libc::geteuid()}{
   return Err(AnalyticsR296Error::Path)
  }
 }
 Ok(())
}
fn analytics_r296_open(path:&Path)->Result<Db,AnalyticsR296Error>{
 analytics_r296_path(path)?;
 let v=CString::new(path.to_string_lossy().as_bytes())
  .map_err(|_|AnalyticsR296Error::Path)?;
 let mut raw=std::ptr::null_mut();
 let rc=unsafe{sqlite3_open_v2(v.as_ptr(),&mut raw,READWRITE_EXISTING,std::ptr::null())};
 if rc!=SQLITE_OK||raw.is_null(){
  if !raw.is_null(){unsafe{sqlite3_close(raw);}}
  return Err(AnalyticsR296Error::Sql)
 }
 let db=Db(raw);
 if unsafe{sqlite3_busy_timeout(db.0,1000)}!=SQLITE_OK{
  return Err(AnalyticsR296Error::Sql)
 }
 exec(&db,"PRAGMA foreign_keys=ON; PRAGMA synchronous=FULL;")
  .map_err(|_|AnalyticsR296Error::Sql)?;
 Ok(db)
}
fn analytics_r296_count(db:&Db,sql:&str)->Result<u64,AnalyticsR296Error>{
 let mut q=Stmt::new(db,sql).map_err(|_|AnalyticsR296Error::Sql)?;
 if q.step().map_err(|_|AnalyticsR296Error::Sql)?!=SQLITE_ROW{
  return Err(AnalyticsR296Error::Corrupt)
 }
 let n=q.num(0).map_err(|_|AnalyticsR296Error::Corrupt)?;
 if q.step().map_err(|_|AnalyticsR296Error::Sql)?!=SQLITE_DONE{
  return Err(AnalyticsR296Error::Corrupt)
 }
 Ok(n)
}
fn analytics_r296_identity(id:&str)->bool{
 (3..=96).contains(&id.len())&&id.bytes().all(|b|
  b.is_ascii_alphanumeric()||b==b'-'||b==b'_')
}
// R316: validate exact schema shape, not merely table names and user_version.
// Name/type/primary-key and NOT NULL policy are individually checked.
fn analytics_r316_validate_schema(db:&Db)->Result<(),AnalyticsR296Error>{
 if analytics_r296_count(db,"PRAGMA user_version")?!=296{
  return Err(AnalyticsR296Error::Schema)
 }
 let specs=[
  ("events",&[("event_id","TEXT",1u64,0u64),("mode","TEXT",0,1),
    ("category","TEXT",0,1),("payload_json","TEXT",0,1),
    ("content_hash","TEXT",0,1)][..]),
  ("mirror_outbox",&[("seq","INTEGER",1,0),("event_id","TEXT",0,1),
    ("payload_hash","TEXT",0,1)][..]),
 ];
 for (table,expected) in specs{
  let mut q=Stmt::new(db,&format!("PRAGMA table_info({table})"))
    .map_err(|_|AnalyticsR296Error::Schema)?;
  let mut seen=vec![];
  loop{
   match q.step().map_err(|_|AnalyticsR296Error::Schema)?{
    SQLITE_DONE=>break,
    SQLITE_ROW=>{
     seen.push((
       q.strval(1).map_err(|_|AnalyticsR296Error::Schema)?,
       q.strval(2).map_err(|_|AnalyticsR296Error::Schema)?.to_uppercase(),
       q.num(5).map_err(|_|AnalyticsR296Error::Schema)?,
       q.num(3).map_err(|_|AnalyticsR296Error::Schema)?,
     ));
    },
    _=>return Err(AnalyticsR296Error::Schema)
   }
  }
  let reference=expected.iter().map(|(n,t,pk,nn)|
    (n.to_string(),t.to_string(),*pk,*nn)).collect::<Vec<_>>();
  if seen!=reference{return Err(AnalyticsR296Error::Schema)}
 }
 // Dropped/replaced unique constraints can preserve column layout:
 // retain a strict SQLite table-definition contract as well.
 for (table,expected_fragments) in [
  ("events",&["event_id TEXT PRIMARY KEY","mode TEXT NOT NULL CHECK(mode='paper')"][..]),
  ("mirror_outbox",&["seq INTEGER PRIMARY KEY AUTOINCREMENT",
    "event_id TEXT NOT NULL UNIQUE REFERENCES events(event_id)"][..]),
 ]{
  let mut q=Stmt::new(db,
    "SELECT sql FROM sqlite_master WHERE type='table' AND name=?1")
    .map_err(|_|AnalyticsR296Error::Schema)?;
  q.string(1,table).map_err(|_|AnalyticsR296Error::Schema)?;
  if q.step().map_err(|_|AnalyticsR296Error::Schema)?!=SQLITE_ROW{
    return Err(AnalyticsR296Error::Schema)
  }
  let ddl=q.strval(0).map_err(|_|AnalyticsR296Error::Schema)?;
  if expected_fragments.iter().any(|part|!ddl.contains(part)){
    return Err(AnalyticsR296Error::Schema)
  }
 }
 let mut q=Stmt::new(db,"PRAGMA integrity_check")
   .map_err(|_|AnalyticsR296Error::Corrupt)?;
 if q.step().map_err(|_|AnalyticsR296Error::Corrupt)?!=SQLITE_ROW
   ||q.strval(0).map_err(|_|AnalyticsR296Error::Corrupt)?!="ok"
   ||q.step().map_err(|_|AnalyticsR296Error::Corrupt)?!=SQLITE_DONE{
  return Err(AnalyticsR296Error::Corrupt)
 }
 Ok(())
}
fn analytics_r316_digest(event_id:&str,category:&str,payload:&str)->String{
 let canonical=format!("paper\n{}\n{}\n{}",event_id,category,payload);
 openssl::sha::sha256(canonical.as_bytes()).iter()
   .map(|v|format!("{v:02x}")).collect::<String>()
}
fn analytics_r316_verify_rows(db:&Db)->Result<(),AnalyticsR296Error>{
 let mut q=Stmt::new(db,"SELECT e.event_id,e.mode,e.category,e.payload_json,e.content_hash,o.payload_hash FROM events e LEFT JOIN mirror_outbox o ON e.event_id=o.event_id ORDER BY e.event_id")
    .map_err(|_|AnalyticsR296Error::Corrupt)?;
 loop{
  match q.step().map_err(|_|AnalyticsR296Error::Corrupt)?{
   SQLITE_DONE=>break,
   SQLITE_ROW=>{
    let id=q.strval(0).map_err(|_|AnalyticsR296Error::Corrupt)?;
    let mode=q.strval(1).map_err(|_|AnalyticsR296Error::Corrupt)?;
    let cat=q.strval(2).map_err(|_|AnalyticsR296Error::Corrupt)?;
    let payload=q.strval(3).map_err(|_|AnalyticsR296Error::Corrupt)?;
    let hash=q.strval(4).map_err(|_|AnalyticsR296Error::Corrupt)?;
    let outbox_hash=q.strval(5).map_err(|_|AnalyticsR296Error::Corrupt)?;
    let parsed:serde_json::Value=serde_json::from_str(&payload)
       .map_err(|_|AnalyticsR296Error::Corrupt)?;
    if r336_scrub(&parsed,&[]).map_err(|_|AnalyticsR296Error::Corrupt)?!=parsed
       ||["mode","event_id","run_id","is_exchange_verified"].iter()
         .any(|key|parsed.get(*key).is_some())
       ||mode!="paper"||!analytics_r296_identity(&id)||
       !matches!(cat.as_str(),"signals"|"decisions"|"errors")||
       !parsed.is_object()||payload.len()>16_384||
       serde_json::to_string(&parsed).map_err(|_|AnalyticsR296Error::Corrupt)?!=payload||
       analytics_r316_digest(&id,&cat,&payload)!=hash||
       hash!=outbox_hash{
      return Err(AnalyticsR296Error::Corrupt)
    }
   },
   _=>return Err(AnalyticsR296Error::Corrupt)
  }
 }
 drop(q);
 if analytics_r296_count(db,"SELECT COUNT(*) FROM mirror_outbox o LEFT JOIN events e ON o.event_id=e.event_id WHERE e.event_id IS NULL")?!=0{
  return Err(AnalyticsR296Error::Corrupt)
 }
 Ok(())
}
impl IsolatedAnalyticsR296{
 pub fn initialize(root:&Path)->Result<Self,AnalyticsR296Error>{
  let path=root.join("astra-r296-analytics.sqlite");
  analytics_r296_path(&path)?;
  use std::os::unix::fs::OpenOptionsExt;
  let file=fs::OpenOptions::new().write(true).create_new(true)
   .mode(0o600).open(&path).map_err(|_|AnalyticsR296Error::Path)?;
  file.sync_all().map_err(|_|AnalyticsR296Error::Sql)?;
  drop(file);
  let db=analytics_r296_open(&path)?;
  exec(&db,"PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA wal_autocheckpoint=32;")
    .map_err(|_|AnalyticsR296Error::Sql)?;
  exec(&db,"BEGIN IMMEDIATE; CREATE TABLE events(\
    event_id TEXT PRIMARY KEY,mode TEXT NOT NULL CHECK(mode='paper'),\
    category TEXT NOT NULL,payload_json TEXT NOT NULL,content_hash TEXT NOT NULL);\
    CREATE TABLE mirror_outbox(seq INTEGER PRIMARY KEY AUTOINCREMENT,\
    event_id TEXT NOT NULL UNIQUE REFERENCES events(event_id),\
    payload_hash TEXT NOT NULL); PRAGMA user_version=296; COMMIT;")
    .map_err(|_|AnalyticsR296Error::Sql)?;
  fs::File::open(root).and_then(|f|f.sync_all())
    .map_err(|_|AnalyticsR296Error::Sql)?;
  Ok(Self{path})
 }
 pub fn reopen(root:&Path)->Result<Self,AnalyticsR296Error>{
  let path=root.join("astra-r296-analytics.sqlite");
  let db=analytics_r296_open(&path)?;
  analytics_r316_validate_schema(&db)?;
  analytics_r316_verify_rows(&db)?;
  Ok(Self{path})
 }
 /// Failpoint after event insertion: DROP of Tx rolls back both event and outbox.
 pub fn append_paper(&self,event:&PaperAnalyticsEventR296,abort_after_event:bool)
   ->Result<(),AnalyticsR296Error>{
  // Historical isolated test-only path; does NOT attest runtime secret plumbing.
  self.append_paper_with_known_secrets_r346(event,abort_after_event,&[])
 }
 /// Explicit synthetic-secret input for isolated research ONLY. Do not pass
 /// secrets to diagnostics or serialize the raw event. Not installed in PAPER.
 pub fn append_paper_with_known_secrets_r346(
    &self,event:&PaperAnalyticsEventR296,abort_after_event:bool,
    known_secrets:&[&str])->Result<(),AnalyticsR296Error>{
  if !analytics_r296_identity(&event.event_id)
   ||!matches!(event.category.as_str(),"signals"|"decisions"|"errors")
   ||!event.payload.is_object(){
   return Err(AnalyticsR296Error::Schema)
  }
  // Python collector scrubs before serializing or hashing; never journal raw input.
  if ["mode","event_id","run_id","is_exchange_verified"].iter()
      .any(|key|event.payload.get(*key).is_some()){
   return Err(AnalyticsR296Error::Schema)
  }
  let clean=r336_scrub(&event.payload,known_secrets)?;
  let payload=serde_json::to_string(&clean)
    .map_err(|_|AnalyticsR296Error::Schema)?;
  if payload.len()>16_384{return Err(AnalyticsR296Error::Schema)}
  let digest=analytics_r316_digest(&event.event_id,&event.category,&payload);
  let db=analytics_r296_open(&self.path)?;
  analytics_r316_validate_schema(&db)?;
  analytics_r316_verify_rows(&db)?;
  let tx=Tx::begin(&db).map_err(|_|AnalyticsR296Error::Sql)?;
  let mut exists=Stmt::new(&db,"SELECT content_hash FROM events WHERE event_id=?1")
    .map_err(|_|AnalyticsR296Error::Sql)?;
  exists.string(1,&event.event_id).map_err(|_|AnalyticsR296Error::Sql)?;
  let old=match exists.step().map_err(|_|AnalyticsR296Error::Sql)?{
   SQLITE_ROW=>Some(exists.strval(0).map_err(|_|AnalyticsR296Error::Corrupt)?),
   SQLITE_DONE=>None,_=>return Err(AnalyticsR296Error::Corrupt)
  };
  drop(exists);
  if let Some(hash)=old{
   return Err(if hash==digest{AnalyticsR296Error::Duplicate}
              else{AnalyticsR296Error::Conflict});
  }
  let mut q=Stmt::new(&db,"INSERT INTO events(event_id,mode,category,payload_json,content_hash) VALUES(?1,'paper',?2,?3,?4)")
    .map_err(|_|AnalyticsR296Error::Sql)?;
  for (idx,val) in [&event.event_id,&event.category,&payload,&digest].iter().enumerate(){
   q.string(idx as i32+1,val).map_err(|_|AnalyticsR296Error::Sql)?;
  }
  if q.step().map_err(|_|AnalyticsR296Error::Sql)?!=SQLITE_DONE{
   return Err(AnalyticsR296Error::Sql)
  }
  drop(q);
  if abort_after_event{return Err(AnalyticsR296Error::Interrupted)}
  let mut q=Stmt::new(&db,"INSERT INTO mirror_outbox(event_id,payload_hash) VALUES(?1,?2)")
    .map_err(|_|AnalyticsR296Error::Sql)?;
  q.string(1,&event.event_id).map_err(|_|AnalyticsR296Error::Sql)?;
  q.string(2,&digest).map_err(|_|AnalyticsR296Error::Sql)?;
  if q.step().map_err(|_|AnalyticsR296Error::Sql)?!=SQLITE_DONE{
   return Err(AnalyticsR296Error::Sql)
  }
  drop(q);
  tx.commit().map_err(|_|AnalyticsR296Error::Sql)
 }
 pub fn audit(&self)->Result<AnalyticsR296Audit,AnalyticsR296Error>{
  let db=analytics_r296_open(&self.path)?;
  analytics_r316_validate_schema(&db)?;
  analytics_r316_verify_rows(&db)?;
  let events=analytics_r296_count(&db,"SELECT COUNT(*) FROM events")?;
  let outbox=analytics_r296_count(&db,"SELECT COUNT(*) FROM mirror_outbox")?;
  let real_events=analytics_r296_count(&db,"SELECT COUNT(*) FROM events WHERE mode!='paper'")?;
  let mismatched=analytics_r296_count(&db,"SELECT COUNT(*) FROM events e LEFT JOIN mirror_outbox o ON e.event_id=o.event_id WHERE o.event_id IS NULL OR e.content_hash!=o.payload_hash")?;
  if events!=outbox||mismatched!=0||real_events!=0{
   return Err(AnalyticsR296Error::Corrupt)
  }
  Ok(AnalyticsR296Audit{events,outbox,mismatched,real_events,
   may_trade:false,production_writer:false})
 }
}
#[cfg(test)]
mod r296_tests{
 use super::*;
 use std::sync::atomic::{AtomicU64,Ordering};
 static SERIAL:AtomicU64=AtomicU64::new(0);
 fn test_root()->PathBuf{
  use std::os::unix::fs::DirBuilderExt;
  let id=SERIAL.fetch_add(1,Ordering::Relaxed);
  let root=std::env::temp_dir().join(format!(
   "astra_r296_analytics_{}_{}",std::process::id(),id));
  fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
  root
 }
 fn event(id:&str,spread:i64)->PaperAnalyticsEventR296{
  PaperAnalyticsEventR296{event_id:id.into(),category:"decisions".into(),
   payload:serde_json::json!({"spread_bps":spread,"model_only":true})}
 }
 #[test]fn r296_atomic_event_and_outbox_survive_reopen(){
  let root=test_root();
  let w=IsolatedAnalyticsR296::initialize(&root).unwrap();
  w.append_paper(&event("sample_A",42),false).unwrap();
  drop(w);
  let a=IsolatedAnalyticsR296::reopen(&root).unwrap().audit().unwrap();
  assert_eq!((a.events,a.outbox,a.real_events,a.mismatched),(1,1,0,0));
  assert!(!a.may_trade&&!a.production_writer);
  fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r296_idempotent_duplicate_and_collision_fail_closed(){
  let root=test_root();let w=IsolatedAnalyticsR296::initialize(&root).unwrap();
  w.append_paper(&event("sample_A",42),false).unwrap();
  assert_eq!(w.append_paper(&event("sample_A",42),false),Err(AnalyticsR296Error::Duplicate));
  assert_eq!(w.append_paper(&event("sample_A",43),false),Err(AnalyticsR296Error::Conflict));
  let a=w.audit().unwrap();assert_eq!((a.events,a.outbox),(1,1));
  drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r296_rollback_after_event_has_zero_durable_side_effect(){
  let root=test_root();let w=IsolatedAnalyticsR296::initialize(&root).unwrap();
  assert_eq!(w.append_paper(&event("sample_B",99),true),Err(AnalyticsR296Error::Interrupted));
  let a=w.audit().unwrap();assert_eq!((a.events,a.outbox),(0,0));
  drop(w);
  let w=IsolatedAnalyticsR296::reopen(&root).unwrap();
  w.append_paper(&event("sample_B",99),false).unwrap();
  assert_eq!(w.audit().unwrap().outbox,1);
  drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r296_never_accepts_nonpaper_payload_or_unexpected_id(){
  let root=test_root();let w=IsolatedAnalyticsR296::initialize(&root).unwrap();
  let mut e=event("sample_B",42);e.category="fills".into();
  assert_eq!(w.append_paper(&e,false),Err(AnalyticsR296Error::Schema));
  e=event("https://x",42);
  assert_eq!(w.append_paper(&e,false),Err(AnalyticsR296Error::Schema));
  e=event("sample_B",42);e.payload=serde_json::json!({"oversize":"x".repeat(17_000)});
  assert_eq!(w.append_paper(&e,false),Err(AnalyticsR296Error::Schema));
  assert_eq!(w.audit().unwrap().events,0);
  drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r296_refuses_symlink_and_nonisolated_directories(){
  let root=test_root();let out=IsolatedAnalyticsR296::initialize(&root).unwrap();drop(out);
  let bad=std::env::temp_dir().join("astra-r296-analytics.sqlite");
  assert_eq!(IsolatedAnalyticsR296::reopen(&std::env::temp_dir())
     .err(),Some(AnalyticsR296Error::Path));
  assert!(bad.file_name().is_some());
  let other=test_root();
  std::os::unix::fs::symlink(
    root.join("astra-r296-analytics.sqlite"),
    other.join("astra-r296-analytics.sqlite")).unwrap();
  assert_eq!(IsolatedAnalyticsR296::reopen(&other).err(),
    Some(AnalyticsR296Error::Path));
  fs::remove_dir_all(other).unwrap();fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r316_rejects_schema_added_column_before_append(){
  let root=test_root();let w=IsolatedAnalyticsR296::initialize(&root).unwrap();
  let db=analytics_r296_open(&w.path).unwrap();
  exec(&db,"ALTER TABLE events ADD COLUMN unaudited TEXT").unwrap();
  drop(db);
  assert_eq!(IsolatedAnalyticsR296::reopen(&root).err(),Some(AnalyticsR296Error::Schema));
  assert_eq!(w.audit().err(),Some(AnalyticsR296Error::Schema));
  assert_eq!(w.append_paper(&event("sample_A",42),false),Err(AnalyticsR296Error::Schema));
  drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r316_catches_synchronized_event_outbox_hash_forgery(){
  let root=test_root();let w=IsolatedAnalyticsR296::initialize(&root).unwrap();
  w.append_paper(&event("sample_A",42),false).unwrap();
  let db=analytics_r296_open(&w.path).unwrap();
  exec(&db,"UPDATE events SET content_hash='madeup' WHERE event_id='sample_A';\
    UPDATE mirror_outbox SET payload_hash='madeup' WHERE event_id='sample_A'").unwrap();
  drop(db);
  assert_eq!(IsolatedAnalyticsR296::reopen(&root).err(),
     Some(AnalyticsR296Error::Corrupt));
  assert_eq!(w.audit().err(),Some(AnalyticsR296Error::Corrupt));
  assert_eq!(w.append_paper(&event("sample_B",43),false),
     Err(AnalyticsR296Error::Corrupt));
  drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r316_catches_payload_change_even_when_pair_counts_match(){
  let root=test_root();let w=IsolatedAnalyticsR296::initialize(&root).unwrap();
  w.append_paper(&event("sample_A",42),false).unwrap();
  let db=analytics_r296_open(&w.path).unwrap();
  exec(&db,"UPDATE events SET payload_json='{\"spread_bps\":99}' WHERE event_id='sample_A'").unwrap();
  drop(db);
  assert_eq!(w.audit().err(),Some(AnalyticsR296Error::Corrupt));
  drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r336_writer_redacts_before_sqlite_insert_and_outbox_hash(){
  let root=test_root();let w=IsolatedAnalyticsR296::initialize(&root).unwrap();
  let e=PaperAnalyticsEventR296{event_id:"scrub_R336".into(),
    category:"errors".into(),
    payload:serde_json::json!({"token":"FAKE_TOKEN_123",
      "log":"password: madeup","nested":{"api-key":"fake"}})};
  w.append_paper(&e,false).unwrap();
  let db=analytics_r296_open(&w.path).unwrap();
  let mut q=Stmt::new(&db,"SELECT payload_json FROM events WHERE event_id='scrub_R336'").unwrap();
  assert_eq!(q.step().unwrap(),SQLITE_ROW);
  let saved=q.strval(0).unwrap();
  assert!(saved.contains("[REDACTED]"));
  for secret in ["FAKE_TOKEN_123","madeup","fake"]{
   assert!(!saved.contains(secret),"secret appeared in saved row");
  }
  drop(q);drop(db);
  assert_eq!(w.audit().unwrap().events,1);
  assert_eq!(w.audit().unwrap().outbox,1);
  drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r336_writer_rejects_shadowed_envelope_and_overcomplex_payload(){
  let root=test_root();let w=IsolatedAnalyticsR296::initialize(&root).unwrap();
  let e=PaperAnalyticsEventR296{event_id:"shadow_R336".into(),
    category:"decisions".into(),payload:serde_json::json!({"mode":"real"})};
  assert_eq!(w.append_paper(&e,false),Err(AnalyticsR296Error::Schema));
  let e=PaperAnalyticsEventR296{event_id:"oversize_R336".into(),
    category:"decisions".into(),payload:serde_json::json!({"levels":vec![0;65]})};
  assert_eq!(w.append_paper(&e,false),Err(AnalyticsR296Error::Schema));
  assert_eq!(w.audit().unwrap().events,0);
  drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r346_explicit_synthetic_secret_redacts_before_durable_sql(){
  let root=test_root();let w=IsolatedAnalyticsR296::initialize(&root).unwrap();
  let secret="synthetic_OpaqueValue_C9R346";
  let e=PaperAnalyticsEventR296{
   event_id:"secrets_R346".into(),category:"errors".into(),
   payload:serde_json::json!({"note":format!("trace id {secret}")})};
  w.append_paper_with_known_secrets_r346(&e,false,&[secret]).unwrap();
  let db=analytics_r296_open(&w.path).unwrap();
  let mut q=Stmt::new(&db,
    "SELECT payload_json FROM events WHERE event_id='secrets_R346'").unwrap();
  assert_eq!(q.step().unwrap(),SQLITE_ROW);
  let stored=q.strval(0).unwrap();
  assert!(!stored.contains(secret));
  assert_eq!(serde_json::from_str::<serde_json::Value>(&stored).unwrap()["note"],
   "trace id [REDACTED]");
  drop(q);drop(db);
  assert_eq!(w.audit().unwrap().events,1);
  let other=PaperAnalyticsEventR296{
   event_id:"secrets_R346".into(),category:"errors".into(),
   payload:serde_json::json!({"note":"trace id DIFFERENT_FAKE_SECRET"})};
  assert_eq!(w.append_paper_with_known_secrets_r346(
       &other,false,&["DIFFERENT_FAKE_SECRET"]),
       Err(AnalyticsR296Error::Duplicate));
  drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r316_rejects_orphan_outbox_row(){
  let root=test_root();let w=IsolatedAnalyticsR296::initialize(&root).unwrap();
  let db=analytics_r296_open(&w.path).unwrap();
  exec(&db,"PRAGMA foreign_keys=OFF; INSERT INTO mirror_outbox(event_id,payload_hash) VALUES('orphan','bad')").unwrap();
  drop(db);
  assert_eq!(w.audit().err(),Some(AnalyticsR296Error::Corrupt));
  drop(w);fs::remove_dir_all(root).unwrap();
 }
}
