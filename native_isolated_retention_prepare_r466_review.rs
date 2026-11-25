// R466–R475: atomic *PREPARATION* of retention rollups and delete intents.
// All source rows and mirror_outbox stay untouched. Never apply/delete/ACK.
fn r466_schema(db:&Db)->Result<(),EventOutboxR366Error>{
 exec(db,"CREATE TABLE retention_preparations(
 batch_id TEXT PRIMARY KEY,created_at_ms INTEGER NOT NULL,
 snapshot_seq INTEGER NOT NULL,frame_json TEXT NOT NULL,
 frame_hash TEXT NOT NULL);
 CREATE TABLE retention_preparation_journal(
 seq INTEGER PRIMARY KEY,prev_hash TEXT NOT NULL,
 batch_id TEXT NOT NULL UNIQUE REFERENCES retention_preparations(batch_id),
 entry_hash TEXT NOT NULL);").map_err(r366_error)
}
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct R466Prepared{
 pub batch_id:String,pub candidate_count:usize,
 pub rollup_count:usize,pub deletion_authorized:bool,
 pub db2_acknowledged:bool,
}
fn r466_frame(
 db:&Db,batch_id:&str,now:u64,snapshot_seq:u64,
 candidates:&[String],rollups:&[serde_json::Value],
)->Result<String,EventOutboxR366Error>{
 use serde_json::json;
 let mut source=Vec::new();let mut operations=Vec::new();
 for id in candidates{
  let mut q=Stmt::new(db,"SELECT category,content_hash FROM events WHERE event_id=?1")
    .map_err(r366_error)?;
  q.string(1,id).map_err(r366_error)?;
  if q.step().map_err(r366_error)?!=SQLITE_ROW{
   return Err(EventOutboxR366Error::Conflict)
  }
  let category=q.strval(0).map_err(r366_error)?;
  let hash=q.strval(1).map_err(r366_error)?;
  if !["signals","decisions"].contains(&category.as_str())||
     hash.len()!=64{return Err(EventOutboxR366Error::Corrupt)}
  if q.step().map_err(r366_error)?!=SQLITE_DONE{
    return Err(EventOutboxR366Error::Corrupt)
  }
  source.push(json!({"event_id":id,"category":category,"content_hash":hash}));
  operations.push(json!({"planned_op":"delete","table":category,
    "key":{"event_id":id}}));
  operations.push(json!({"planned_op":"delete","table":"events",
    "key":{"event_id":id}}));
 }
 for row in rollups{
  operations.push(json!({"planned_op":"upsert","table":"hourly_raw_rollups",
    "row":row}));
 }
 operations.push(json!({"planned_op":"increment","table":"metadata",
  "key":"rolled_up_raw_events","delta":candidates.len()}));
 let frame=json!({
  "kind":"PRIVATE_RETENTION_PREPARED_NOT_APPLIED",
  "batch_id":batch_id,"now_ms":now,"snapshot_seq":snapshot_seq,
  "source_rows":source,"rollups":rollups,"planned_operations":operations,
  "deletion_authorized":false,"db2_acknowledged":false
 });
 let canonical=r386_python_mirror_canonical(&frame)?;
 if canonical.len()>1_000_000{return Err(EventOutboxR366Error::Schema)}
 Ok(canonical)
}
impl IsolatedEventOutboxR366{
 pub fn r466_prepare(&self,batch_id:&str,now:u64,days:u64,
  pressure:bool,max_batch:usize,fail_before_commit:bool,
 )->Result<R466Prepared,EventOutboxR366Error>{
  if batch_id.is_empty()||batch_id.len()>80||
   !batch_id.bytes().all(|x|x.is_ascii_alphanumeric()||x==b'-'||x==b'_'){
    return Err(EventOutboxR366Error::Schema)
  }
  r436_admit_private(&self.path)?;
  self.audit()?;
  let start=r366_open(&self.path)?;
  let expected_seq=r366_query_num(&start,
    "SELECT latest_seq FROM mirror_control WHERE id=1")?;
  drop(start);
  let selection=self.r446_retention_preview(now,days,pressure,max_batch)?;
  if selection.candidates.is_empty(){return Err(EventOutboxR366Error::Schema)}
  let preview=self.r456_rollup_preview(now,days,pressure,max_batch)?;
  let rolled=preview.rows.iter().map(|v|v["event_count"].as_u64().unwrap_or(0))
    .try_fold(0_u64,|a,b|a.checked_add(b).ok_or(EventOutboxR366Error::Schema))?;
  if preview.deletion_authorized||preview.selected!=selection.candidates.len()||
     rolled!=selection.candidates.len() as u64||
     preview.rows.is_empty(){return Err(EventOutboxR366Error::Schema)}
  let db=r366_open(&self.path)?;
  let tx=Tx::begin(&db).map_err(r366_error)?;
  if r366_query_num(&db,"SELECT latest_seq FROM mirror_control WHERE id=1")?
      !=expected_seq{return Err(EventOutboxR366Error::Conflict)}
  let raw=r466_frame(&db,batch_id,now,expected_seq,
    &selection.candidates,&preview.rows)?;
  let hash=r366_sha(&raw);
  let mut same=Stmt::new(&db,
    "SELECT frame_hash FROM retention_preparations WHERE batch_id=?1")
    .map_err(r366_error)?;
  same.string(1,batch_id).map_err(r366_error)?;
  match same.step().map_err(r366_error)?{
    SQLITE_ROW=>{
     let prev=same.strval(0).map_err(r366_error)?;
     return Err(if prev==hash{EventOutboxR366Error::Duplicate}
       else{EventOutboxR366Error::Conflict});
    }, SQLITE_DONE=>{}, _=>return Err(EventOutboxR366Error::Corrupt)
  }
  drop(same);
  let mut q=Stmt::new(&db,"INSERT INTO retention_preparations
    (batch_id,created_at_ms,snapshot_seq,frame_json,frame_hash)
    VALUES(?1,?2,?3,?4,?5)").map_err(r366_error)?;
  q.string(1,batch_id).map_err(r366_error)?;
  q.number(2,now).map_err(r366_error)?;
  q.number(3,expected_seq).map_err(r366_error)?;
  q.string(4,&raw).map_err(r366_error)?;
  q.string(5,&hash).map_err(r366_error)?;
  if q.step().map_err(r366_error)?!=SQLITE_DONE{
    return Err(EventOutboxR366Error::Sql)
  }
  drop(q);
  if fail_before_commit{return Err(EventOutboxR366Error::Interrupted)}
  r466_journal_add(&db,batch_id,&hash)?;
  tx.commit().map_err(r366_error)?;
  Ok(R466Prepared{batch_id:batch_id.into(),
    candidate_count:selection.candidates.len(),
    rollup_count:preview.rows.len(),
    deletion_authorized:false,db2_acknowledged:false})
 }
}
fn r466_journal_add(db:&Db,batch_id:&str,hash:&str)
 ->Result<(),EventOutboxR366Error>{
 let mut last=Stmt::new(db,"SELECT seq,entry_hash FROM
  retention_preparation_journal ORDER BY seq DESC LIMIT 1").map_err(r366_error)?;
 let (seq,prev)=match last.step().map_err(r366_error)?{
   SQLITE_ROW=>(last.num(0).map_err(r366_error)?.checked_add(1)
      .ok_or(EventOutboxR366Error::Corrupt)?,last.strval(1).map_err(r366_error)?),
   SQLITE_DONE=>(1,"GENESIS".into()),
   _=>return Err(EventOutboxR366Error::Corrupt)
 };
 drop(last);
 let entry_hash=r366_sha(&format!("{seq}:{prev}:{batch_id}:{hash}"));
 let mut q=Stmt::new(db,"INSERT INTO retention_preparation_journal
  (seq,prev_hash,batch_id,entry_hash) VALUES(?1,?2,?3,?4)")
  .map_err(r366_error)?;
 q.number(1,seq).map_err(r366_error)?;
 q.string(2,&prev).map_err(r366_error)?;
 q.string(3,batch_id).map_err(r366_error)?;
 q.string(4,&entry_hash).map_err(r366_error)?;
 if q.step().map_err(r366_error)?!=SQLITE_DONE{
   return Err(EventOutboxR366Error::Sql)
 }
 Ok(())
}
fn r466_audit(db:&Db)->Result<(),EventOutboxR366Error>{
 use serde_json::Value;
 let mut seen=std::collections::BTreeSet::new();
 let mut expected=1_u64;let mut prev="GENESIS".to_owned();
 let mut q=Stmt::new(db,"SELECT seq,prev_hash,batch_id,entry_hash FROM
  retention_preparation_journal ORDER BY seq").map_err(r366_error)?;
 loop{
  match q.step().map_err(r366_error)?{
   SQLITE_DONE=>break,
   SQLITE_ROW=>{
    let seq=q.num(0).map_err(r366_error)?;
    let p=q.strval(1).map_err(r366_error)?;
    let id=q.strval(2).map_err(r366_error)?;
    let hash=q.strval(3).map_err(r366_error)?;
    if seq!=expected||p!=prev||!seen.insert(id.clone()){
     return Err(EventOutboxR366Error::Corrupt)
    }
    let mut b=Stmt::new(db,"SELECT created_at_ms,snapshot_seq,
      frame_json,frame_hash FROM retention_preparations WHERE batch_id=?1")
       .map_err(r366_error)?;
    b.string(1,&id).map_err(r366_error)?;
    if b.step().map_err(r366_error)?!=SQLITE_ROW{
      return Err(EventOutboxR366Error::Corrupt)
    }
    let created=b.num(0).map_err(r366_error)?;
    let snapshot=b.num(1).map_err(r366_error)?;
    let raw=b.strval(2).map_err(r366_error)?;
    let saved=b.strval(3).map_err(r366_error)?;
    if b.step().map_err(r366_error)?!=SQLITE_DONE{
      return Err(EventOutboxR366Error::Corrupt)
    }
    let frame:Value=serde_json::from_str(&raw)
       .map_err(|_|EventOutboxR366Error::Corrupt)?;
    if r386_python_mirror_canonical(&frame)
       .map_err(|_|EventOutboxR366Error::Corrupt)?!=raw||
       saved!=r366_sha(&raw)||
       hash!=r366_sha(&format!("{seq}:{prev}:{id}:{saved}"))||
       frame["kind"]!="PRIVATE_RETENTION_PREPARED_NOT_APPLIED"||
       frame["batch_id"]!=id||frame["now_ms"]!=created||
       frame["snapshot_seq"]!=snapshot||
       frame["deletion_authorized"]!=false||frame["db2_acknowledged"]!=false||
       snapshot>r366_query_num(db,"SELECT latest_seq FROM mirror_control WHERE id=1")?{
      return Err(EventOutboxR366Error::Corrupt)
    }
    let rows=frame["source_rows"].as_array().ok_or(EventOutboxR366Error::Corrupt)?;
    let rollups=frame["rollups"].as_array().ok_or(EventOutboxR366Error::Corrupt)?;
    let ops=frame["planned_operations"].as_array().ok_or(EventOutboxR366Error::Corrupt)?;
    if rows.is_empty()||rows.len()>500||rollups.is_empty()||
      ops.len()!=rows.len()*2+rollups.len()+1{
     return Err(EventOutboxR366Error::Corrupt)
    }
    let mut source_ids=std::collections::BTreeSet::new();
    for (i,row) in rows.iter().enumerate(){
     let source_id=row["event_id"].as_str().ok_or(EventOutboxR366Error::Corrupt)?;
     let cat=row["category"].as_str().ok_or(EventOutboxR366Error::Corrupt)?;
     let expected_hash=row["content_hash"].as_str()
       .ok_or(EventOutboxR366Error::Corrupt)?;
     if !["signals","decisions"].contains(&cat)||
       !source_ids.insert(source_id)||expected_hash.len()!=64||
       ops[i*2]["planned_op"]!="delete"||
       ops[i*2]["table"]!=cat||ops[i*2]["key"]["event_id"]!=source_id||
       ops[i*2+1]["planned_op"]!="delete"||
       ops[i*2+1]["table"]!="events"||
       ops[i*2+1]["key"]["event_id"]!=source_id{
       return Err(EventOutboxR366Error::Corrupt)
     }
     let mut v=Stmt::new(db,
       "SELECT category,content_hash FROM events WHERE event_id=?1")
       .map_err(r366_error)?;
     v.string(1,source_id).map_err(r366_error)?;
     if v.step().map_err(r366_error)?!=SQLITE_ROW||
       v.strval(0).map_err(r366_error)?!=cat||
       v.strval(1).map_err(r366_error)?!=expected_hash||
       v.step().map_err(r366_error)?!=SQLITE_DONE{
      return Err(EventOutboxR366Error::Corrupt)
     }
    }
    let rolled=rollups.iter().try_fold(0_u64,|acc,row|
      acc.checked_add(row["event_count"].as_u64()
        .ok_or(EventOutboxR366Error::Corrupt)?)
       .ok_or(EventOutboxR366Error::Corrupt))?;
    if rolled!=rows.len() as u64{
     return Err(EventOutboxR366Error::Corrupt)
    }
    for (i,row) in rollups.iter().enumerate(){
     if ops[rows.len()*2+i]["planned_op"]!="upsert"||
       ops[rows.len()*2+i]["table"]!="hourly_raw_rollups"||
       ops[rows.len()*2+i]["row"]!=*row{
      return Err(EventOutboxR366Error::Corrupt)
     }
    }
    let tail=&ops[ops.len()-1];
    if tail["planned_op"]!="increment"||tail["table"]!="metadata"||
       tail["key"]!="rolled_up_raw_events"||tail["delta"]!=rows.len(){
      return Err(EventOutboxR366Error::Corrupt)
    }
    expected+=1;prev=hash;
   },
   _=>return Err(EventOutboxR366Error::Corrupt)
  }
 }
 if r366_query_num(db,"SELECT COUNT(*) FROM retention_preparations")?
   !=seen.len() as u64{return Err(EventOutboxR366Error::Corrupt)}
 Ok(())
}
#[cfg(test)]
mod r466_tests{
 use super::*;
 use super::r366_tests::fixture_root;
 use serde_json::{json,Value};
 const NOW:u64=1_792_660_000_000;
 fn raw_event(id:&str,cat:&str,corr:Option<&str>)->Value{
  let name=if cat=="signals"{"SIGNAL_SEEN"}else if cat=="decisions"{
    "DECISION_MADE"}else{"ENTRY_ABORT"};
  let payload=if cat=="signals"{
    json!({"raw_spread_bps":"1.25","net_spread_bps":"0.50",
      "depth_spot_usdt":"15.00"})
  }else if cat=="decisions"{
    json!({"decision":"skip","skip_reason":"spread"})
  }else{json!({})};
  r356_paper_event_record_without_io(&json!({
   "mode":"paper","category":cat,"name":name,"event_id":id,
   "source":"native.paper","run_id":"r466_private",
   "occurred_at_ms":NOW-8*86_400_000,
   "correlation_id":corr,"base":"SOL","payload":payload,
   "verification":{"status":"observed"}}),&[],NOW).unwrap()
 }
 fn setup()->(std::path::PathBuf,IsolatedEventOutboxR366){
  let root=fixture_root();
  let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
  let t=NOW-8*86_400_000;
  w.append(&raw_event("r466_oldsignal","signals",None),t,false).unwrap();
  w.append(&raw_event("r466_olddecision","decisions",None),t+1,false).unwrap();
  (root,w)
 }
 #[test]fn r466_prepared_atomically_keeps_original_rows_and_outbox(){
  let (root,w)=setup();
  let result=w.r466_prepare("r466_batch",NOW,7,false,500,false).unwrap();
  assert_eq!((result.candidate_count,result.rollup_count),(2,2));
  assert!(!result.deletion_authorized&&!result.db2_acknowledged);
  let db=r366_open(&w.path).unwrap();
  assert_eq!(r366_query_num(&db,"SELECT COUNT(*) FROM retention_preparations").unwrap(),1);
  assert_eq!(r366_query_num(&db,"SELECT COUNT(*) FROM retention_preparation_journal").unwrap(),1);
  assert_eq!((w.audit().unwrap().events,w.audit().unwrap().outbox),(2,2));
  drop(db);drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r467_preparation_survives_reopen_without_source_deletion(){
  let (root,w)=setup();
  w.r466_prepare("r467_batch",NOW,7,false,500,false).unwrap();
  drop(w);
  let r=IsolatedEventOutboxR366::reopen(&root).unwrap();
  assert_eq!((r.audit().unwrap().events,r.audit().unwrap().outbox),(2,2));
  assert_eq!(r366_query_num(&r366_open(&r.path).unwrap(),
    "SELECT COUNT(*) FROM retention_preparations").unwrap(),1);
  drop(r);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r468_interrupted_preparation_rolls_back_both_tables(){
  let (root,w)=setup();
  assert_eq!(w.r466_prepare("r468_abort",NOW,7,false,500,true),
     Err(EventOutboxR366Error::Interrupted));
  let db=r366_open(&w.path).unwrap();
  assert_eq!(r366_query_num(&db,"SELECT COUNT(*) FROM retention_preparations").unwrap(),0);
  assert_eq!(r366_query_num(&db,"SELECT COUNT(*) FROM retention_preparation_journal").unwrap(),0);
  assert_eq!(w.audit().unwrap().outbox,2);
  drop(db);drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r469_retry_after_rollback_is_durable(){
  let (root,w)=setup();
  assert_eq!(w.r466_prepare("r469_retry",NOW,7,false,500,true),
     Err(EventOutboxR366Error::Interrupted));
  assert_eq!(w.r466_prepare("r469_retry",NOW,7,false,500,false).unwrap()
    .candidate_count,2);
  drop(w);
  let w=IsolatedEventOutboxR366::reopen(&root).unwrap();
  assert_eq!(r366_query_num(&r366_open(&w.path).unwrap(),
    "SELECT COUNT(*) FROM retention_preparation_journal").unwrap(),1);
  drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r470_identical_batch_is_duplicate_without_write(){
  let (root,w)=setup();
  w.r466_prepare("r470_unchanged",NOW,7,false,500,false).unwrap();
  assert_eq!(w.r466_prepare("r470_unchanged",NOW,7,false,500,false),
    Err(EventOutboxR366Error::Duplicate));
  assert_eq!(r366_query_num(&r366_open(&w.path).unwrap(),
    "SELECT COUNT(*) FROM retention_preparation_journal").unwrap(),1);
  drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r471_same_batch_id_with_changed_conditions_is_conflict(){
  let (root,w)=setup();
  w.r466_prepare("r471_reused",NOW,7,false,500,false).unwrap();
  assert_eq!(w.r466_prepare("r471_reused",NOW+1,7,false,500,false),
    Err(EventOutboxR366Error::Conflict));
  assert_eq!(w.audit().unwrap().outbox,2);
  drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r472_tampered_journal_hash_detected_on_audit_and_reopen(){
  let (root,w)=setup();
  w.r466_prepare("r472_integrity",NOW,7,false,500,false).unwrap();
  let db=r366_open(&w.path).unwrap();
  exec(&db,"UPDATE retention_preparation_journal SET entry_hash='attacker'
    WHERE seq=1").unwrap();
  drop(db);
  assert_eq!(w.audit(),Err(EventOutboxR366Error::Corrupt));
  assert!(matches!(IsolatedEventOutboxR366::reopen(&root),
    Err(EventOutboxR366Error::Corrupt)));
  drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r473_missing_half_transaction_detected(){
  let (root,w)=setup();
  w.r466_prepare("r473_half",NOW,7,false,500,false).unwrap();
  let db=r366_open(&w.path).unwrap();
  exec(&db,"PRAGMA foreign_keys=OFF;
    DELETE FROM retention_preparations WHERE batch_id='r473_half';").unwrap();
  drop(db);
  assert_eq!(w.audit(),Err(EventOutboxR366Error::Corrupt));
  drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r474_stale_plan_never_authorizes_delete_after_new_execution(){
  let (root,w)=setup();
  w.r466_prepare("r474_snapshot",NOW,7,false,500,false).unwrap();
  let t=NOW-8*86_400_000+10;
  w.append(&raw_event("r474_execution","entries",Some("exec_corr")),t,false).unwrap();
  let db=r366_open(&w.path).unwrap();
  let mut q=Stmt::new(&db,"SELECT frame_json FROM retention_preparations
   WHERE batch_id='r474_snapshot'").unwrap();
  assert_eq!(q.step().unwrap(),SQLITE_ROW);
  let frame:Value=serde_json::from_str(&q.strval(0).unwrap()).unwrap();
  assert_eq!(frame["deletion_authorized"],false);
  assert_eq!(frame["db2_acknowledged"],false);
  assert_eq!((w.audit().unwrap().events,w.audit().unwrap().outbox),(3,3));
  drop(q);drop(db);drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r475_invalid_batch_and_no_candidates_rejected(){
  let (root,w)=setup();
  assert_eq!(w.r466_prepare("../bad",NOW,7,false,500,false),
    Err(EventOutboxR366Error::Schema));
  assert_eq!(w.r466_prepare("r475_new",NOW,7,false,0,false),
    Err(EventOutboxR366Error::Schema));
  assert_eq!(w.r466_prepare("r475_not_old",NOW,365,false,500,false),
    Err(EventOutboxR366Error::Schema));
  assert_eq!(w.audit().unwrap().events,2);
  drop(w);fs::remove_dir_all(root).unwrap();
 }
}
