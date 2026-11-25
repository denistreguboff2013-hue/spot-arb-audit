// R435: private /tmp PAPER-only collector_runs lifecycle and LOCAL hash journal.
// Not the production Python mirror_outbox, DB2 ACK, or installed PAPER.
fn r435_schema(db:&Db)->Result<(),EventOutboxR366Error>{
 exec(db,"CREATE TABLE collector_runs(
  run_id TEXT PRIMARY KEY,started_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL,closed_cleanly INTEGER NOT NULL
   CHECK(closed_cleanly IN (0,1)),stats_json TEXT NOT NULL);
 CREATE TABLE collector_run_journal(
  seq INTEGER PRIMARY KEY,prev_hash TEXT NOT NULL,
  frame_json TEXT NOT NULL,frame_hash TEXT NOT NULL);").map_err(r366_error)
}
fn r435_stats(v:&serde_json::Value)->Result<String,EventOutboxR366Error>{
 let map=v.as_object().ok_or(EventOutboxR366Error::Schema)?;
 let valid=["accepted","committed","duplicates","dropped","rejected","write_failures"];
 if map.len()>valid.len()||map.iter().any(|(k,x)|
   !valid.contains(&k.as_str())||x.as_u64().is_none()){
  return Err(EventOutboxR366Error::Schema)
 }
 let canonical=r386_python_mirror_canonical(v)?;
 if canonical.len()>512{return Err(EventOutboxR366Error::Schema)}
 Ok(canonical)
}
fn r435_source_row(db:&Db,run_id:&str)->Result<serde_json::Value,EventOutboxR366Error>{
 let mut q=Stmt::new(db,"SELECT run_id,started_at_ms,updated_at_ms,
 closed_cleanly,stats_json FROM collector_runs WHERE run_id=?1").map_err(r366_error)?;
 q.string(1,run_id).map_err(r366_error)?;
 if q.step().map_err(r366_error)?!=SQLITE_ROW{return Err(EventOutboxR366Error::Corrupt)}
 let row=serde_json::json!({"run_id":q.strval(0).map_err(r366_error)?,
   "started_at_ms":q.num(1).map_err(r366_error)?,
   "updated_at_ms":q.num(2).map_err(r366_error)?,
   "closed_cleanly":q.num(3).map_err(r366_error)?,
   "stats_json":q.strval(4).map_err(r366_error)?});
 if q.step().map_err(r366_error)?!=SQLITE_DONE{return Err(EventOutboxR366Error::Corrupt)}
 Ok(row)
}
fn r435_journal_add(db:&Db,kind:&str,id:&str)->Result<(),EventOutboxR366Error>{
 let mut last=Stmt::new(db,"SELECT seq,frame_hash FROM collector_run_journal
 ORDER BY seq DESC LIMIT 1").map_err(r366_error)?;
 let (seq,prev)=match last.step().map_err(r366_error)?{
  SQLITE_ROW=>(last.num(0).map_err(r366_error)?.checked_add(1)
   .ok_or(EventOutboxR366Error::Corrupt)?,last.strval(1).map_err(r366_error)?),
  SQLITE_DONE=>(1,"GENESIS".to_string()),
  _=>return Err(EventOutboxR366Error::Corrupt)
 };
 drop(last);
 let row=r435_source_row(db,id)?;
 let frame=serde_json::json!({"seq":seq,"kind":kind,"row":row});
 let raw=r386_python_mirror_canonical(&frame)?;
 if raw.len()>2048{return Err(EventOutboxR366Error::Schema)}
 let hash=r366_sha(&format!("{prev}:{raw}"));
 let mut ins=Stmt::new(db,"INSERT INTO collector_run_journal
 (seq,prev_hash,frame_json,frame_hash) VALUES(?1,?2,?3,?4)")
 .map_err(r366_error)?;
 ins.number(1,seq).map_err(r366_error)?;
 ins.string(2,&prev).map_err(r366_error)?;
 ins.string(3,&raw).map_err(r366_error)?;
 ins.string(4,&hash).map_err(r366_error)?;
 if ins.step().map_err(r366_error)?!=SQLITE_DONE{return Err(EventOutboxR366Error::Sql)}
 Ok(())
}
impl IsolatedEventOutboxR366{
 pub fn r435_run_change(&self,kind:&str,id:&str,now:u64,
  stats:&serde_json::Value)->Result<(),EventOutboxR366Error>{
  if !["start","heartbeat","close"].contains(&kind)||now==0||
    now>i64::MAX as u64||id.is_empty()||id.len()>80||
    !id.bytes().all(|b|b.is_ascii_alphanumeric()||b==b'_'||b==b'-'){
   return Err(EventOutboxR366Error::Schema)
  }
  let stats=r435_stats(stats)?;
  r436_admit_private(&self.path)?;
  self.audit()?;
  let db=r366_open(&self.path)?;
  let tx=Tx::begin(&db).map_err(r366_error)?;
  if kind=="start"{
   let mut q=Stmt::new(&db,"INSERT INTO collector_runs
   (run_id,started_at_ms,updated_at_ms,closed_cleanly,stats_json)
   VALUES(?1,?2,?2,0,?3)").map_err(r366_error)?;
   q.string(1,id).map_err(r366_error)?;
   q.number(2,now).map_err(r366_error)?;
   q.string(3,&stats).map_err(r366_error)?;
   if q.step().map_err(r366_error)?!=SQLITE_DONE{return Err(EventOutboxR366Error::Sql)}
  }else{
   let mut q=Stmt::new(&db,"UPDATE collector_runs SET updated_at_ms=?1,
   stats_json=?2,closed_cleanly=?3 WHERE run_id=?4
   AND closed_cleanly=0 AND updated_at_ms<=?1").map_err(r366_error)?;
   q.number(1,now).map_err(r366_error)?;
   q.string(2,&stats).map_err(r366_error)?;
   q.number(3,if kind=="close"{1}else{0}).map_err(r366_error)?;
   q.string(4,id).map_err(r366_error)?;
   if q.step().map_err(r366_error)?!=SQLITE_DONE{return Err(EventOutboxR366Error::Sql)}
   drop(q);
   if unsafe{sqlite3_changes(db.0)}!=1{return Err(EventOutboxR366Error::Schema)}
  }
  r435_journal_add(&db,kind,id)?;
  tx.commit().map_err(r366_error)
 }
}
fn r435_run_audit(db:&Db)->Result<(),EventOutboxR366Error>{
 use serde_json::Value;
 let mut state=std::collections::BTreeMap::<String,(Value,bool)>::new();
 let mut last=0_u64;let mut prev="GENESIS".to_string();
 let mut q=Stmt::new(db,"SELECT seq,prev_hash,frame_json,frame_hash
 FROM collector_run_journal ORDER BY seq").map_err(r366_error)?;
 loop{
  match q.step().map_err(r366_error)?{
   SQLITE_DONE=>break,
   SQLITE_ROW=>{
    let seq=q.num(0).map_err(r366_error)?;
    let p=q.strval(1).map_err(r366_error)?;
    let raw=q.strval(2).map_err(r366_error)?;
    let saved=q.strval(3).map_err(r366_error)?;
    let f:Value=serde_json::from_str(&raw).map_err(|_|EventOutboxR366Error::Corrupt)?;
    if seq!=last+1||p!=prev||saved!=r366_sha(&format!("{prev}:{raw}"))||
      r386_python_mirror_canonical(&f).map_err(|_|EventOutboxR366Error::Corrupt)?!=raw||
      f["seq"]!=seq{return Err(EventOutboxR366Error::Corrupt)}
    let row=&f["row"];
    let id=row["run_id"].as_str().ok_or(EventOutboxR366Error::Corrupt)?;
    let began=row["started_at_ms"].as_u64().ok_or(EventOutboxR366Error::Corrupt)?;
    let updated=row["updated_at_ms"].as_u64().ok_or(EventOutboxR366Error::Corrupt)?;
    let closed=row["closed_cleanly"].as_u64().ok_or(EventOutboxR366Error::Corrupt)?;
    let stats=row["stats_json"].as_str().ok_or(EventOutboxR366Error::Corrupt)?;
    let stats_obj:Value=serde_json::from_str(stats).map_err(|_|EventOutboxR366Error::Corrupt)?;
    if r435_stats(&stats_obj).map_err(|_|EventOutboxR366Error::Corrupt)?!=stats||
      began==0||updated<began||closed>1{return Err(EventOutboxR366Error::Corrupt)}
    match f["kind"].as_str(){
     Some("start") if began==updated&&closed==0&&!state.contains_key(id)=>{
      state.insert(id.into(),(row.clone(),false));
     },
     Some(kind @ ("heartbeat"|"close"))=>{
      let (prior,prior_closed)=state.get_mut(id).ok_or(EventOutboxR366Error::Corrupt)?;
      if *prior_closed||prior["started_at_ms"]!=began||
        updated<prior["updated_at_ms"].as_u64().ok_or(EventOutboxR366Error::Corrupt)?||
        (kind=="close")!=(closed==1){return Err(EventOutboxR366Error::Corrupt)}
      *prior=row.clone();*prior_closed=kind=="close";
     },
     _=>return Err(EventOutboxR366Error::Corrupt)
    }
    last=seq;prev=saved;
   },
   _=>return Err(EventOutboxR366Error::Corrupt)
  }
 }
 drop(q);
 if state.len() as u64!=r366_query_num(db,"SELECT COUNT(*) FROM collector_runs")?{
  return Err(EventOutboxR366Error::Corrupt)
 }
 for (id,(expected,_)) in state{
  if r435_source_row(db,&id).map_err(|_|EventOutboxR366Error::Corrupt)?!=expected{
   return Err(EventOutboxR366Error::Corrupt)
  }
 }
 Ok(())
}
#[cfg(test)]
mod r435_tests{
 use super::*;
 use serde_json::json;
 use super::r366_tests::fixture_root;
 const NOW:u64=1_792_660_000_000;
 #[test]fn r435_start_heartbeat_close_reopen_and_append(){
  let root=fixture_root();let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
  w.r435_run_change("start","r435_test",NOW,&json!({})).unwrap();
  w.r435_run_change("heartbeat","r435_test",NOW+100,
    &json!({"accepted":2,"committed":1})).unwrap();
  w.r435_run_change("close","r435_test",NOW+200,
    &json!({"accepted":2,"committed":2})).unwrap();
  assert_eq!(w.audit().unwrap().outbox,0);
  let db=r366_open(&w.path).unwrap();
  assert_eq!(r366_query_num(&db,"SELECT COUNT(*) FROM collector_run_journal").unwrap(),3);
  assert_eq!(r435_source_row(&db,"r435_test").unwrap()["closed_cleanly"],1);
  drop(db);drop(w);
  let reopened=IsolatedEventOutboxR366::reopen(&root).unwrap();
  reopened.append(&super::r366_tests::record("r435_event","decisions"),
   NOW+300,false).unwrap();
  assert_eq!(reopened.audit().unwrap().events,1);
  drop(reopened);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r435_missing_run_and_closed_lifecycle_fail_closed(){
  let root=fixture_root();let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
  assert_eq!(w.r435_run_change("heartbeat","missing",NOW,&json!({})),
    Err(EventOutboxR366Error::Schema));
  w.r435_run_change("start","r435_test",NOW,&json!({})).unwrap();
  assert_eq!(w.r435_run_change("start","r435_test",NOW+1,&json!({})),
    Err(EventOutboxR366Error::Sql));
  assert_eq!(w.r435_run_change("heartbeat","r435_test",NOW-1,&json!({})),
    Err(EventOutboxR366Error::Schema));
  w.r435_run_change("close","r435_test",NOW+3,&json!({})).unwrap();
  assert_eq!(w.r435_run_change("heartbeat","r435_test",NOW+4,&json!({})),
    Err(EventOutboxR366Error::Schema));
  assert_eq!(w.audit().unwrap().outbox,0);
  let db=r366_open(&w.path).unwrap();
  assert_eq!(r366_query_num(&db,"SELECT COUNT(*) FROM collector_run_journal").unwrap(),2);
  drop(db);drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r435_rejects_unapproved_stats_and_identifiers(){
  let root=fixture_root();let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
  for bad in [json!({"token":"synthetic_secret"}),json!({"accepted":"10"}),
    json!({"accepted":3.5}),json!({"accepted":-1})]{
   assert_eq!(w.r435_run_change("start","r435_test",NOW,&bad),
      Err(EventOutboxR366Error::Schema));
  }
  assert_eq!(w.r435_run_change("start","r435/invalid",NOW,&json!({})),
    Err(EventOutboxR366Error::Schema));
  let db=r366_open(&w.path).unwrap();
  assert_eq!(r366_query_num(&db,"SELECT COUNT(*) FROM collector_runs").unwrap(),0);
  drop(db);drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r435_detects_tampered_historical_journal(){
  let root=fixture_root();let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
  w.r435_run_change("start","r435_run",NOW,&json!({})).unwrap();
  w.r435_run_change("heartbeat","r435_run",NOW+5,&json!({"accepted":2})).unwrap();
  let db=r366_open(&w.path).unwrap();
  exec(&db,"UPDATE collector_run_journal SET frame_hash='tampered' WHERE seq=1").unwrap();
  drop(db);
  assert_eq!(w.audit(),Err(EventOutboxR366Error::Corrupt));
  assert!(matches!(IsolatedEventOutboxR366::reopen(&root),
    Err(EventOutboxR366Error::Corrupt)));
  fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r435_detects_tampered_final_source_row(){
  let root=fixture_root();let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
  w.r435_run_change("start","r435_run",NOW,&json!({})).unwrap();
  let db=r366_open(&w.path).unwrap();
  exec(&db,"UPDATE collector_runs SET closed_cleanly=1").unwrap();
  drop(db);
  assert_eq!(w.audit(),Err(EventOutboxR366Error::Corrupt));
  fs::remove_dir_all(root).unwrap();
 }
}
