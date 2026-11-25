// R476–R485: atomic SHADOW replay of a prepared retention frame only.
// Never mutates events/categories/attempts/real mirror_outbox, never DB2/REAL.
fn r476_schema(db:&Db)->Result<(),EventOutboxR366Error>{
 exec(db,"CREATE TABLE retention_shadow_copies(
  batch_id TEXT NOT NULL,event_id TEXT NOT NULL,
  category TEXT NOT NULL,source_hash TEXT NOT NULL,
  PRIMARY KEY(batch_id,event_id));
 CREATE TABLE retention_shadow_tombstones(
  event_id TEXT PRIMARY KEY,batch_id TEXT NOT NULL,
  category TEXT NOT NULL,source_hash TEXT NOT NULL);
 CREATE TABLE retention_shadow_rollups(
  batch_id TEXT NOT NULL,position INTEGER NOT NULL,row_json TEXT NOT NULL,
  PRIMARY KEY(batch_id,position));
 CREATE TABLE retention_shadow_metadata(
  batch_id TEXT PRIMARY KEY,rolled_up_raw_events INTEGER NOT NULL);
 CREATE TABLE retention_shadow_outbox(
  seq INTEGER PRIMARY KEY,prev_hash TEXT NOT NULL,
  batch_id TEXT NOT NULL UNIQUE,frame_json TEXT NOT NULL,
  frame_hash TEXT NOT NULL,entry_hash TEXT NOT NULL);")
  .map_err(r366_error)
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct R476ShadowApplied{
 pub batch_id:String,pub tombstones:u64,pub rollups:u64,
 pub source_rows_deleted:bool,pub production_outbox_written:bool,
 pub db2_acknowledged:bool,
}
fn r476_load_frame(db:&Db,id:&str)
 ->Result<serde_json::Value,EventOutboxR366Error>{
 let mut q=Stmt::new(db,
  "SELECT frame_json FROM retention_preparations WHERE batch_id=?1")
  .map_err(r366_error)?;
 q.string(1,id).map_err(r366_error)?;
 if q.step().map_err(r366_error)?!=SQLITE_ROW{
  return Err(EventOutboxR366Error::Schema)
 }
 let raw=q.strval(0).map_err(r366_error)?;
 if q.step().map_err(r366_error)?!=SQLITE_DONE{
  return Err(EventOutboxR366Error::Corrupt)
 }
 serde_json::from_str(&raw).map_err(|_|EventOutboxR366Error::Corrupt)
}
fn r476_shadow_frame(prepared:&serde_json::Value)
 ->Result<String,EventOutboxR366Error>{
 use serde_json::{json,Value};
 let raw=prepared["source_rows"].as_array().ok_or(EventOutboxR366Error::Corrupt)?;
 let rollups=prepared["rollups"].as_array().ok_or(EventOutboxR366Error::Corrupt)?;
 if raw.is_empty()||raw.len()>500||rollups.is_empty()||
   prepared["deletion_authorized"]!=false{return Err(EventOutboxR366Error::Schema)}
 let mut ops=Vec::<Value>::new();
 const KEYS:[&str;8]=["hour_start_ms","mode","base","category","name",
  "reason_code","decision","skip_reason"];
 // Python _rollup_raw_rows emits UPSE(R)Ts before all category DELETEs,
 // all event DELETEs, and metadata UPDATE in this exact order.
 for rollup in rollups{
  let mut key=serde_json::Map::new();
  for name in KEYS{
   let v=rollup.get(name).ok_or(EventOutboxR366Error::Corrupt)?;
   key.insert(name.to_string(),v.clone());
  }
  let canonical=r386_python_mirror_canonical(rollup)?;
  ops.push(json!({"table":"hourly_raw_rollups","op":"upsert",
    "key":key,"row":rollup,"after_hash":r366_sha(&canonical)}));
 }
 for row in raw{
  let id=row["event_id"].as_str().ok_or(EventOutboxR366Error::Corrupt)?;
  let cat=row["category"].as_str().ok_or(EventOutboxR366Error::Corrupt)?;
  if !["signals","decisions"].contains(&cat){return Err(EventOutboxR366Error::Corrupt)}
  ops.push(json!({"table":cat,"op":"delete","key":{"event_id":id}}));
 }
 for row in raw{
  let id=row["event_id"].as_str().ok_or(EventOutboxR366Error::Corrupt)?;
  ops.push(json!({"table":"events","op":"delete","key":{"event_id":id}}));
 }
 let meta=json!({"key":"rolled_up_raw_events","value":raw.len().to_string()});
 ops.push(json!({"table":"metadata","op":"upsert",
   "key":{"key":"rolled_up_raw_events"},
   "after_hash":r366_sha(&r386_python_mirror_canonical(&meta)?),
   "row":meta}));
 let envelope=json!({"protocol_version":1,"schema_generation":1,
  "tx_kind":"PRIVATE_SHADOW_RETENTION_NOT_PRODUCTION",
  "batch_id":prepared["batch_id"],"snapshot_seq":prepared["snapshot_seq"],
  "operations":ops,"db2_acknowledged":false,
  "source_rows_deleted":false});
 let serialized=r386_python_mirror_canonical(&envelope)?;
 if serialized.len()+384>4*1024*1024{
  return Err(EventOutboxR366Error::Schema)
 }
 Ok(serialized)
}
impl IsolatedEventOutboxR366{
 pub fn r476_shadow_apply(&self,batch_id:&str,interrupt_after_delete:bool)
  ->Result<R476ShadowApplied,EventOutboxR366Error>{
  if batch_id.is_empty()||batch_id.len()>80||
     !batch_id.bytes().all(|b|b.is_ascii_alphanumeric()||b==b'_'||b==b'-'){
    return Err(EventOutboxR366Error::Schema)
  }
  r436_admit_private(&self.path)?;
  self.audit()?;
  let db=r366_open(&self.path)?;
  let tx=Tx::begin(&db).map_err(r366_error)?;
  let plan=r476_load_frame(&db,batch_id)?;
  if r366_query_num(&db,"SELECT latest_seq FROM mirror_control WHERE id=1")?
       !=plan["snapshot_seq"].as_u64().ok_or(EventOutboxR366Error::Corrupt)?{
    return Err(EventOutboxR366Error::Conflict)
  }
  let rows=plan["source_rows"].as_array().ok_or(EventOutboxR366Error::Corrupt)?;
  let rollups=plan["rollups"].as_array().ok_or(EventOutboxR366Error::Corrupt)?;
  let envelope=r476_shadow_frame(&plan)?;
  // A duplicate batch may never create a second shadow transaction.
  let mut check=Stmt::new(&db,"SELECT 1 FROM retention_shadow_outbox
    WHERE batch_id=?1").map_err(r366_error)?;
  check.string(1,batch_id).map_err(r366_error)?;
  match check.step().map_err(r366_error)?{
   SQLITE_ROW=>return Err(EventOutboxR366Error::Duplicate),
   SQLITE_DONE=>{},_=>return Err(EventOutboxR366Error::Corrupt)
  }
  drop(check);
  // R486 Python oracle proves metadata after-image only at baseline zero.
  // Until cumulative metadata merge is modeled, refuse a second batch.
  if r366_query_num(&db,"SELECT COUNT(*) FROM retention_shadow_outbox")?!=0{
    return Err(EventOutboxR366Error::Conflict)
  }
  for r in rows{
   let id=r["event_id"].as_str().ok_or(EventOutboxR366Error::Corrupt)?;
   let cat=r["category"].as_str().ok_or(EventOutboxR366Error::Corrupt)?;
   let hash=r["content_hash"].as_str().ok_or(EventOutboxR366Error::Corrupt)?;
   let mut source=Stmt::new(&db,
     "SELECT category,content_hash FROM events WHERE event_id=?1")
     .map_err(r366_error)?;
   source.string(1,id).map_err(r366_error)?;
   if source.step().map_err(r366_error)?!=SQLITE_ROW||
     source.strval(0).map_err(r366_error)?!=cat||
     source.strval(1).map_err(r366_error)?!=hash{
      return Err(EventOutboxR366Error::Conflict)
   }
   drop(source);
   let mut previous=Stmt::new(&db,
     "SELECT 1 FROM retention_shadow_tombstones WHERE event_id=?1")
     .map_err(r366_error)?;
   previous.string(1,id).map_err(r366_error)?;
   match previous.step().map_err(r366_error)?{
    SQLITE_ROW=>return Err(EventOutboxR366Error::Conflict),
    SQLITE_DONE=>{},_=>return Err(EventOutboxR366Error::Corrupt)
   }
   drop(previous);
   let mut copy=Stmt::new(&db,"INSERT INTO retention_shadow_copies
     (batch_id,event_id,category,source_hash) VALUES(?1,?2,?3,?4)")
     .map_err(r366_error)?;
   for (n,s) in [batch_id,id,cat,hash].iter().enumerate(){
    copy.string(n as i32+1,s).map_err(r366_error)?;
   }
   if copy.step().map_err(r366_error)?!=SQLITE_DONE{
    return Err(EventOutboxR366Error::Sql)
   }
   drop(copy);
   // Only the SHADOW copy is removed. A durable tombstone retains hash evidence.
   let mut tomb=Stmt::new(&db,"INSERT INTO retention_shadow_tombstones
    (event_id,batch_id,category,source_hash) VALUES(?1,?2,?3,?4)")
    .map_err(r366_error)?;
   for (n,s) in [id,batch_id,cat,hash].iter().enumerate(){
    tomb.string(n as i32+1,s).map_err(r366_error)?;
   }
   if tomb.step().map_err(r366_error)?!=SQLITE_DONE{
    return Err(EventOutboxR366Error::Conflict)
   }
   drop(tomb);
   let mut del=Stmt::new(&db,"DELETE FROM retention_shadow_copies
    WHERE batch_id=?1 AND event_id=?2").map_err(r366_error)?;
   del.string(1,batch_id).map_err(r366_error)?;
   del.string(2,id).map_err(r366_error)?;
   if del.step().map_err(r366_error)?!=SQLITE_DONE{
    return Err(EventOutboxR366Error::Sql)
   }
   drop(del);
   if unsafe{sqlite3_changes(db.0)}!=1{return Err(EventOutboxR366Error::Corrupt)}
  }
  if interrupt_after_delete{return Err(EventOutboxR366Error::Interrupted)}
  for (i,row) in rollups.iter().enumerate(){
   let mut ins=Stmt::new(&db,"INSERT INTO retention_shadow_rollups
     (batch_id,position,row_json) VALUES(?1,?2,?3)").map_err(r366_error)?;
   ins.string(1,batch_id).map_err(r366_error)?;
   ins.number(2,i as u64).map_err(r366_error)?;
   ins.string(3,&r386_python_mirror_canonical(row)?).map_err(r366_error)?;
   if ins.step().map_err(r366_error)?!=SQLITE_DONE{return Err(EventOutboxR366Error::Sql)}
  }
  let mut m=Stmt::new(&db,"INSERT INTO retention_shadow_metadata
    (batch_id,rolled_up_raw_events) VALUES(?1,?2)").map_err(r366_error)?;
  m.string(1,batch_id).map_err(r366_error)?;
  m.number(2,rows.len() as u64).map_err(r366_error)?;
  if m.step().map_err(r366_error)?!=SQLITE_DONE{
   return Err(EventOutboxR366Error::Sql)
  }
  drop(m);
  let mut last=Stmt::new(&db,"SELECT seq,entry_hash FROM retention_shadow_outbox
    ORDER BY seq DESC LIMIT 1").map_err(r366_error)?;
  let (seq,prev)=match last.step().map_err(r366_error)?{
   SQLITE_DONE=>(1_u64,"GENESIS".to_string()),
   SQLITE_ROW=>(last.num(0).map_err(r366_error)?.checked_add(1)
       .ok_or(EventOutboxR366Error::Corrupt)?,
       last.strval(1).map_err(r366_error)?),
   _=>return Err(EventOutboxR366Error::Corrupt)
  };
  drop(last);
  let hash=r366_sha(&envelope);
  let entry_hash=r366_sha(&format!("{seq}:{prev}:{batch_id}:{hash}"));
  let mut out=Stmt::new(&db,"INSERT INTO retention_shadow_outbox
    (seq,prev_hash,batch_id,frame_json,frame_hash,entry_hash)
    VALUES(?1,?2,?3,?4,?5,?6)").map_err(r366_error)?;
  out.number(1,seq).map_err(r366_error)?;
  for (idx,val) in [&prev,batch_id,&envelope,&hash,&entry_hash].iter().enumerate(){
   out.string(idx as i32+2,val).map_err(r366_error)?;
  }
  if out.step().map_err(r366_error)?!=SQLITE_DONE{
   return Err(EventOutboxR366Error::Sql)
  }
  drop(out);
  tx.commit().map_err(r366_error)?;
  Ok(R476ShadowApplied{batch_id:batch_id.to_string(),
    tombstones:rows.len() as u64,rollups:rollups.len() as u64,
    source_rows_deleted:false,production_outbox_written:false,
    db2_acknowledged:false})
 }
}
fn r476_shadow_audit(db:&Db)->Result<(),EventOutboxR366Error>{
 if r366_query_num(db,"SELECT COUNT(*) FROM retention_shadow_copies")?!=0{
  return Err(EventOutboxR366Error::Corrupt)
 }
 let mut n=0_u64;let mut prev="GENESIS".to_string();
 let mut tombstones=0_u64;let mut rollups_count=0_u64;
 let mut q=Stmt::new(db,"SELECT seq,prev_hash,batch_id,frame_json,frame_hash,
   entry_hash FROM retention_shadow_outbox ORDER BY seq").map_err(r366_error)?;
 loop{
  match q.step().map_err(r366_error)?{
   SQLITE_DONE=>break,
   SQLITE_ROW=>{
    let seq=q.num(0).map_err(r366_error)?;
    let previous=q.strval(1).map_err(r366_error)?;
    let batch=q.strval(2).map_err(r366_error)?;
    let frame=q.strval(3).map_err(r366_error)?;
    let hash=q.strval(4).map_err(r366_error)?;
    let entry=q.strval(5).map_err(r366_error)?;
    if seq!=n+1||previous!=prev||hash!=r366_sha(&frame)||
      entry!=r366_sha(&format!("{seq}:{prev}:{batch}:{hash}")){
      return Err(EventOutboxR366Error::Corrupt)
    }
    let prepared=r476_load_frame(db,&batch)
      .map_err(|_|EventOutboxR366Error::Corrupt)?;
    if frame!=r476_shadow_frame(&prepared)
      .map_err(|_|EventOutboxR366Error::Corrupt)?{
      return Err(EventOutboxR366Error::Corrupt)
    }
    let rows=prepared["source_rows"].as_array().ok_or(EventOutboxR366Error::Corrupt)?;
    let rollups=prepared["rollups"].as_array().ok_or(EventOutboxR366Error::Corrupt)?;
    let mut meta=Stmt::new(db,"SELECT rolled_up_raw_events
      FROM retention_shadow_metadata WHERE batch_id=?1").map_err(r366_error)?;
    meta.string(1,&batch).map_err(r366_error)?;
    if meta.step().map_err(r366_error)?!=SQLITE_ROW||
      meta.num(0).map_err(r366_error)?!=rows.len() as u64||
      meta.step().map_err(r366_error)?!=SQLITE_DONE{
      return Err(EventOutboxR366Error::Corrupt)
    }
    for row in rows{
     let id=row["event_id"].as_str().ok_or(EventOutboxR366Error::Corrupt)?;
     let mut tomb=Stmt::new(db,"SELECT batch_id,category,source_hash
       FROM retention_shadow_tombstones WHERE event_id=?1").map_err(r366_error)?;
     tomb.string(1,id).map_err(r366_error)?;
     if tomb.step().map_err(r366_error)?!=SQLITE_ROW||
       tomb.strval(0).map_err(r366_error)?!=batch||
       tomb.strval(1).map_err(r366_error)?!=row["category"].as_str()
         .ok_or(EventOutboxR366Error::Corrupt)?||
       tomb.strval(2).map_err(r366_error)?!=row["content_hash"].as_str()
         .ok_or(EventOutboxR366Error::Corrupt)?||
       tomb.step().map_err(r366_error)?!=SQLITE_DONE{
      return Err(EventOutboxR366Error::Corrupt)
     }
    }
    for (i,row) in rollups.iter().enumerate(){
     let mut r=Stmt::new(db,"SELECT row_json FROM retention_shadow_rollups
        WHERE batch_id=?1 AND position=?2").map_err(r366_error)?;
     r.string(1,&batch).map_err(r366_error)?;
     r.number(2,i as u64).map_err(r366_error)?;
     if r.step().map_err(r366_error)?!=SQLITE_ROW||
       r.strval(0).map_err(r366_error)!=Ok(r386_python_mirror_canonical(row)
         .map_err(|_|EventOutboxR366Error::Corrupt)?)||
       r.step().map_err(r366_error)?!=SQLITE_DONE{
      return Err(EventOutboxR366Error::Corrupt)
     }
    }
    tombstones=tombstones.checked_add(rows.len() as u64)
      .ok_or(EventOutboxR366Error::Corrupt)?;
    rollups_count=rollups_count.checked_add(rollups.len() as u64)
      .ok_or(EventOutboxR366Error::Corrupt)?;
    n=seq;prev=entry;
   },
   _=>return Err(EventOutboxR366Error::Corrupt)
  }
 }
 for (table,expected) in [
  ("retention_shadow_outbox",n),
  ("retention_shadow_tombstones",tombstones),
  ("retention_shadow_rollups",rollups_count),
  ("retention_shadow_metadata",n)]{
  if r366_query_num(db,&format!("SELECT COUNT(*) FROM {table}"))?!=expected{
   return Err(EventOutboxR366Error::Corrupt)
  }
 }
 Ok(())
}
#[cfg(test)]
mod r476_tests{
 use super::*;
 use super::r366_tests::fixture_root;
 use serde_json::{json,Value};
 const NOW:u64=1_792_660_000_000;
 fn event(id:&str,cat:&str,corr:Option<&str>)->Value{
  let (name,payload)=match cat{
   "signals"=>("SIGNAL_SEEN",json!({"raw_spread_bps":"1.25",
    "net_spread_bps":"0.50","depth_spot_usdt":"10.00"})),
   "decisions"=>("DECISION_MADE",json!({"decision":"skip","skip_reason":"spread"})),
   _=>("ENTRY_ABORT",json!({}))
  };
  r356_paper_event_record_without_io(&json!({"mode":"paper",
   "category":cat,"name":name,"event_id":id,"source":"native.paper",
   "run_id":"r476_private","occurred_at_ms":NOW-8*86_400_000,
   "correlation_id":corr,"base":"SOL","payload":payload,
   "verification":{"status":"observed"}}),&[],NOW).unwrap()
 }
 fn prepare()->(std::path::PathBuf,IsolatedEventOutboxR366){
  let root=fixture_root();
  let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
  let time=NOW-8*86_400_000;
  w.append(&event("r476_sig","signals",None),time,false).unwrap();
  w.append(&event("r476_dec","decisions",None),time+1,false).unwrap();
  w.r466_prepare("r476_batch",NOW,7,false,500,false).unwrap();
  (root,w)
 }
 #[test]fn r476_shadow_apply_preserves_original_source_and_production_outbox(){
  let (root,w)=prepare();
  let applied=w.r476_shadow_apply("r476_batch",false).unwrap();
  assert_eq!((applied.tombstones,applied.rollups),(2,2));
  assert!(!applied.source_rows_deleted&&!applied.production_outbox_written
    &&!applied.db2_acknowledged);
  let db=r366_open(&w.path).unwrap();
  assert_eq!(r366_query_num(&db,"SELECT COUNT(*) FROM retention_shadow_copies").unwrap(),0);
  assert_eq!(r366_query_num(&db,"SELECT COUNT(*) FROM retention_shadow_tombstones").unwrap(),2);
  assert_eq!(r366_query_num(&db,"SELECT COUNT(*) FROM retention_shadow_rollups").unwrap(),2);
  assert_eq!((w.audit().unwrap().events,w.audit().unwrap().outbox),(2,2));
  drop(db);drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r477_shadow_operations_follow_python_original_order(){
  let (root,w)=prepare();
  w.r476_shadow_apply("r476_batch",false).unwrap();
  let db=r366_open(&w.path).unwrap();
  let mut q=Stmt::new(&db,"SELECT frame_json FROM retention_shadow_outbox
   WHERE batch_id='r476_batch'").unwrap();
  assert_eq!(q.step().unwrap(),SQLITE_ROW);
  let frame:Value=serde_json::from_str(&q.strval(0).unwrap()).unwrap();
  let op=frame["operations"].as_array().unwrap();
  assert_eq!(op.len(),7);
  assert_eq!((op[0]["table"].as_str(),op[0]["op"].as_str()),
    (Some("hourly_raw_rollups"),Some("upsert")));
  assert_eq!(op[1]["table"],"hourly_raw_rollups");
  assert_eq!(op[2]["op"],"delete");
  assert_eq!(op[3]["op"],"delete");
  assert_eq!(op[4]["table"],"events");
  assert_eq!(op[5]["table"],"events");
  assert_eq!(op[6]["table"],"metadata");
  assert_eq!(op[6]["op"],"upsert");
  assert_eq!(frame["source_rows_deleted"],false);
  drop(q);drop(db);drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r478_shadow_delete_failpoint_rolls_back_tombstones_and_rollups(){
  let (root,w)=prepare();
  assert_eq!(w.r476_shadow_apply("r476_batch",true),
    Err(EventOutboxR366Error::Interrupted));
  let db=r366_open(&w.path).unwrap();
  for table in ["retention_shadow_copies","retention_shadow_tombstones",
   "retention_shadow_rollups","retention_shadow_metadata",
   "retention_shadow_outbox"]{
   assert_eq!(r366_query_num(&db,&format!("SELECT COUNT(*) FROM {table}")).unwrap(),0);
  }
  assert_eq!((w.audit().unwrap().events,w.audit().unwrap().outbox),(2,2));
  drop(db);drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r479_shadow_retry_after_rollback_reopens_cleanly(){
  let (root,w)=prepare();
  assert_eq!(w.r476_shadow_apply("r476_batch",true),
    Err(EventOutboxR366Error::Interrupted));
  assert_eq!(w.r476_shadow_apply("r476_batch",false).unwrap().tombstones,2);
  drop(w);
  let reopened=IsolatedEventOutboxR366::reopen(&root).unwrap();
  assert_eq!(reopened.audit().unwrap().last_seq,2);
  drop(reopened);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r480_double_apply_returns_duplicate_without_changing_source(){
  let (root,w)=prepare();
  w.r476_shadow_apply("r476_batch",false).unwrap();
  assert_eq!(w.r476_shadow_apply("r476_batch",false),
   Err(EventOutboxR366Error::Duplicate));
  assert_eq!((w.audit().unwrap().events,w.audit().unwrap().outbox),(2,2));
  drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r481_new_execution_renders_preparation_stale(){
  let (root,w)=prepare();
  w.append(&event("r481_entry","entries",Some("corr481")),
    NOW-8*86_400_000+2,false).unwrap();
  assert_eq!(w.r476_shadow_apply("r476_batch",false),
    Err(EventOutboxR366Error::Conflict));
  let db=r366_open(&w.path).unwrap();
  assert_eq!(r366_query_num(&db,"SELECT COUNT(*) FROM retention_shadow_outbox").unwrap(),0);
  assert_eq!((w.audit().unwrap().events,w.audit().unwrap().outbox),(3,3));
  drop(db);drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r482_outbox_chain_tamper_is_corrupt_on_reopen(){
  let (root,w)=prepare();
  w.r476_shadow_apply("r476_batch",false).unwrap();
  let db=r366_open(&w.path).unwrap();
  exec(&db,"UPDATE retention_shadow_outbox SET entry_hash='tampered'").unwrap();
  drop(db);
  assert_eq!(w.audit(),Err(EventOutboxR366Error::Corrupt));
  assert!(matches!(IsolatedEventOutboxR366::reopen(&root),
    Err(EventOutboxR366Error::Corrupt)));
  drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r483_missing_shadow_tombstone_detected(){
  let (root,w)=prepare();
  w.r476_shadow_apply("r476_batch",false).unwrap();
  let db=r366_open(&w.path).unwrap();
  exec(&db,"DELETE FROM retention_shadow_tombstones WHERE event_id='r476_sig'").unwrap();
  drop(db);
  assert_eq!(w.audit(),Err(EventOutboxR366Error::Corrupt));
  drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r484_altered_rollup_and_metadata_detected(){
  let (root,w)=prepare();
  w.r476_shadow_apply("r476_batch",false).unwrap();
  let db=r366_open(&w.path).unwrap();
  exec(&db,"UPDATE retention_shadow_rollups SET row_json='{}' WHERE position=0").unwrap();
  assert_eq!(w.audit(),Err(EventOutboxR366Error::Corrupt));
  exec(&db,"UPDATE retention_shadow_rollups SET row_json=(
   SELECT json_extract(p.frame_json,'$.rollups[0]')
   FROM retention_preparations p WHERE p.batch_id='r476_batch')
   WHERE position=0").unwrap();
  // Canonical serializer may reorder; tampering in metadata independently fails.
  exec(&db,"UPDATE retention_shadow_metadata SET rolled_up_raw_events=99").unwrap();
  assert_eq!(w.audit(),Err(EventOutboxR366Error::Corrupt));
  drop(db);drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r485_second_prepared_batch_for_same_events_cannot_redelete(){
  let (root,w)=prepare();
  w.r466_prepare("r485_second",NOW+1,7,false,500,false).unwrap();
  w.r476_shadow_apply("r476_batch",false).unwrap();
  assert_eq!(w.r476_shadow_apply("r485_second",false),
    Err(EventOutboxR366Error::Conflict));
  let db=r366_open(&w.path).unwrap();
  assert_eq!(r366_query_num(&db,"SELECT COUNT(*) FROM retention_shadow_outbox").unwrap(),1);
  assert_eq!((w.audit().unwrap().events,w.audit().unwrap().outbox),(2,2));
  drop(db);drop(w);fs::remove_dir_all(root).unwrap();
 }
}
