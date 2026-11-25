// R1092 PRIVATE research: dynamic (1..16 events/envelope) incremental
// DB1/DB2 ACK by induction from a fully proven EMPTY genesis.
// All original 17 tables are physically immutable except events INSERT.
// Every new row is compared to both DB1 and DB2 and to a canonical
// verified source outbox operation. Receipt tracks cumulative row count.
// NOT compatible with original mixed Python 17-table workload.
fn r1092_guard_ddl(v:&serde_json::Value)
 ->Result<Vec<(String,String)>,EventOutboxR366Error>{
 let tables=v["table_schema"].as_array().ok_or(EventOutboxR366Error::Schema)?;
 if tables.len()!=17{return Err(EventOutboxR366Error::Schema)}
 let mut all=Vec::new();
 for item in tables{
  let name=item["table"].as_str().ok_or(EventOutboxR366Error::Schema)?;
  let tbl=r586_identifier(name)?;
  for action in ["UPDATE","DELETE","INSERT"]{
   if action=="INSERT"&&name=="events"{continue}
   let trigger=format!("r924_guard_{}_{}",name,action.to_ascii_lowercase());
   let ddl=format!("CREATE TRIGGER {} BEFORE {action} ON {tbl} BEGIN SELECT RAISE(ABORT,'r924 original PAPER-only mutation denied'); END",
      r586_identifier(&trigger)?);
   all.push((trigger,ddl));
  }
 }
 if all.len()!=50{return Err(EventOutboxR366Error::Schema)}
 Ok(all)
}
fn r1092_check_guards(db:&Db,v:&serde_json::Value)
 ->Result<(),EventOutboxR366Error>{
 let defs=r1092_guard_ddl(v)?;
 let mut q=Stmt::new(db,"SELECT name,sql FROM sqlite_master
 WHERE type='trigger' AND name LIKE 'r924_guard_%' ORDER BY name")
   .map_err(r366_error)?;
 let mut observed=std::collections::BTreeMap::new();
 loop{
  match q.step().map_err(r366_error)?{
   SQLITE_DONE=>break,
   SQLITE_ROW=>{
    let name=q.strval(0).map_err(r366_error)?;
    let ddl=q.strval(1).map_err(r366_error)?;
    if observed.insert(name,ddl).is_some(){return Err(EventOutboxR366Error::Corrupt)}
   },
   _=>return Err(EventOutboxR366Error::Corrupt)
  }
 }
 if observed.len()!=defs.len(){return Err(EventOutboxR366Error::Corrupt)}
 for (key,ddl) in defs{
  if observed.get(&key)!=Some(&ddl){return Err(EventOutboxR366Error::Corrupt)}
 }
 Ok(())
}
fn r1092_receipt(run:&str,schema:&str,generation:&str,
 seq:u64,events:u64)->Result<String,EventOutboxR366Error>{
 if run.is_empty()||run.len()>80||!run.bytes()
    .all(|c|c.is_ascii_alphanumeric()||c==b'_'||c==b'-'){
  return Err(EventOutboxR366Error::Schema)
 }
 r386_python_mirror_canonical(&serde_json::json!({
  "protocol":"SPOTARB/R1092/GUARDED-DYNAMIC-EVENT-ACK/V1",
  "run":run,"schema_hash":schema,"feed_generation":generation,
  "acked_seq":seq,"events":events
 }))
}
pub fn r1092_private_empty_genesis(root:&Path,run:&str)
 ->Result<(),EventOutboxR366Error>{
 let src=r671_open(root,"r671-private-db1.sqlite",false)?;
 let dst=r671_open(root,"r671-private-db2.sqlite",false)?;
 let v=r586_oracle();
 let (head,c,m)=r805_read_head(root,&src,&dst)?;
 if head.latest!=0||head.applied!=0||head.cleaned!=0||
    head.retained!=0 ||m["last_key_json"]!=serde_json::Value::Null||
    r366_query_num(&src,"SELECT COUNT(*) FROM events")?!=0||
    r366_query_num(&dst,"SELECT COUNT(*) FROM events")?!=0||
    r800_streaming_original_parity(&src,&dst,&v)?!=0{
  return Err(EventOutboxR366Error::Conflict)
 }
 let schema_before=r756_schema_hash(&src)?;
 if schema_before!=r756_schema_hash(&dst)?{return Err(EventOutboxR366Error::Corrupt)}
 let source_tx=Tx::begin(&src).map_err(r366_error)?;
 let mirror_tx=Tx::begin(&dst).map_err(r366_error)?;
 for (_,ddl) in r1092_guard_ddl(&v)?{
  exec(&src,&ddl).map_err(r366_error)?;
  exec(&dst,&ddl).map_err(r366_error)?;
 }
 r1092_check_guards(&src,&v)?;
 r1092_check_guards(&dst,&v)?;
 let schema=r756_schema_hash(&src)?;
 if schema==schema_before||schema!=r756_schema_hash(&dst)?{
  return Err(EventOutboxR366Error::Corrupt)
 }
 let version=r366_query_num(&src,"PRAGMA schema_version")?;
 let receipt=r1092_receipt(run,&schema,r726_s(&c,"feed_generation")?,0,0)?;
 exec(&dst,&format!("UPDATE mirror_meta SET schema_hash={},
    schema_version={version},last_key_json={} WHERE id=1 AND applied_seq=0",
    r586_sql_literal(&serde_json::json!(schema))?,
    r586_sql_literal(&serde_json::json!(receipt))?)).map_err(r366_error)?;
 mirror_tx.commit().map_err(r366_error)?;
 source_tx.commit().map_err(r366_error)?;
 // Mandatory post-bootstrap independent 17-table parity and head/receipt.
 r1092_check_guards(&src,&v)?;
 r1092_check_guards(&dst,&v)?;
 let (_h,_c,meta)=r805_read_head(root,&src,&dst)?;
 if meta["last_key_json"].as_str()!=Some(receipt.as_str())||
    r800_streaming_original_parity(&src,&dst,&v)?!=0{
  return Err(EventOutboxR366Error::Corrupt)
 }
 Ok(())
}
fn r1092_check_new_physical_rows(
 src:&Db,dst:&Db,v:&serde_json::Value,
 expected:&std::collections::BTreeMap<String,serde_json::Value>
)->Result<(),EventOutboxR366Error>{
 // Retained up to 64 envelopes: 16 events by default, or 32 in R1129.
 let ceiling=if r1129_private_paper_32(){2048}else{1024};
 if expected.is_empty()||expected.len()>ceiling{return Err(EventOutboxR366Error::Pressure)}
 let item=v["table_schema"].as_array().ok_or(EventOutboxR366Error::Schema)?
  .iter().find(|x|x["table"]=="events")
  .ok_or(EventOutboxR366Error::Schema)?;
 let cols=item["columns"].as_array().ok_or(EventOutboxR366Error::Schema)?;
 let projection=cols.iter().map(|col|{
  let name=col[1].as_str().ok_or(EventOutboxR366Error::Schema)?;
  Ok(format!("'{}',{}",name,r586_identifier(name)?))
 }).collect::<Result<Vec<String>,EventOutboxR366Error>>()?.join(",");
 let low=r586_sql_literal(&serde_json::json!(
   expected.first_key_value().ok_or(EventOutboxR366Error::Corrupt)?.0))?;
 let high=r586_sql_literal(&serde_json::json!(
   expected.last_key_value().ok_or(EventOutboxR366Error::Corrupt)?.0))?;
 let sql=format!("SELECT json_object({projection}) FROM events
  WHERE event_id>={low} AND event_id<={high} ORDER BY event_id");
 for db in [src,dst]{
  let mut q=Stmt::new(db,&sql).map_err(r366_error)?;
  for (id,row) in expected{
   if q.step().map_err(r366_error)?!=SQLITE_ROW{
    return Err(EventOutboxR366Error::Corrupt)
   }
   let actual:serde_json::Value=serde_json::from_str(&q.strval(0).map_err(r366_error)?)
     .map_err(|_|EventOutboxR366Error::Corrupt)?;
   if &actual!=row||actual["event_id"]!=*id{
    return Err(EventOutboxR366Error::Corrupt)
   }
  }
  if q.step().map_err(r366_error)?!=SQLITE_DONE{
   return Err(EventOutboxR366Error::Corrupt)
  }
 }
 Ok(())
}
fn r1092_private_dynamic_incremental_ack(
 root:&Path,src:&Db,dst:&Db,v:&serde_json::Value,run:&str
)->Result<(u64,u64),EventOutboxR366Error>{
 let r1130_t=std::time::Instant::now();
 let r1131_t=std::time::Instant::now();
 r1092_check_guards(src,v)?;
 r1092_check_guards(dst,v)?;
 r1130_measure(19,r1131_t);
 let r1131_t=std::time::Instant::now();
 // Both locks deliberately held for at most one bounded 64-envelope
 // verification. No source/DB2 state is published without proven parity.
 let source_tx=Tx::begin(src).map_err(r366_error)?;
 let mirror_tx=Tx::begin(dst).map_err(r366_error)?;
 let (h,c,m)=r805_read_head(root,src,dst)?;
 if h.applied!=h.latest||h.applied<=h.cleaned{
  return Err(EventOutboxR366Error::Conflict)
 }
 if h.retained>64{return Err(EventOutboxR366Error::Pressure)}
 r1130_measure(20,r1131_t);
 let r1131_t=std::time::Instant::now();
 r805_verify_chain(src,&h,r726_s(&c,"feed_generation")?)?;
 r1130_measure(21,r1131_t);
 r1130_measure(12,r1130_t);
 let r1130_t=std::time::Instant::now();
 let receipt=m["last_key_json"].as_str().ok_or(EventOutboxR366Error::Corrupt)?;
 let before:serde_json::Value=serde_json::from_str(receipt)
   .map_err(|_|EventOutboxR366Error::Corrupt)?;
 let previous_events=before["events"].as_u64().ok_or(EventOutboxR366Error::Corrupt)?;
 let previous_seq=before["acked_seq"].as_u64().ok_or(EventOutboxR366Error::Corrupt)?;
 // A DB2 receipt may have committed just before SIGKILL while the
 // separate DB1 cleanup transaction rolled back. Never ACK blindly.
 let receipt_ahead=previous_seq==h.applied&&previous_seq>h.cleaned;
 if (previous_seq!=h.cleaned&&!receipt_ahead) ||
   r1092_receipt(run,&r756_schema_hash(src)?,
      r726_s(&c,"feed_generation")?,previous_seq,previous_events)?!=receipt{
  return Err(EventOutboxR366Error::Corrupt)
 }
 if r366_query_num(dst,"SELECT COUNT(*) FROM pragma_foreign_key_check")?!=0{
  return Err(EventOutboxR366Error::Corrupt)
 }
 let query=r805_window_sql(R726_OUTBOX,"ORDER BY seq")?;
 let mut rows=Stmt::new(src,&query).map_err(r366_error)?;
 let mut expected=std::collections::BTreeMap::new();
 let mut count=0u64;let mut bytes=0u64;
 let mut last_hash=String::new();
 let mut prev=h.cleaned;
 loop{
  match rows.step().map_err(r366_error)?{
   SQLITE_DONE=>break,
   SQLITE_ROW=>{
    let raw=rows.strval(0).map_err(r366_error)?;
    let row:serde_json::Value=serde_json::from_str(&raw)
       .map_err(|_|EventOutboxR366Error::Corrupt)?;
    let (seq,size)=r805_validate_row(&row,prev,h.latest,
       r726_s(&c,"feed_generation")?)?;
    if seq!=prev.checked_add(1).ok_or(EventOutboxR366Error::Pressure)?{
     return Err(EventOutboxR366Error::Corrupt)
    }
    prev=seq;count+=1;
    bytes=bytes.checked_add(size).ok_or(EventOutboxR366Error::Pressure)?;
    last_hash=r726_s(&row,"payload_hash")?.to_owned();
    let frame:serde_json::Value=serde_json::from_str(r726_s(&row,"payload_json")?)
      .map_err(|_|EventOutboxR366Error::Corrupt)?;
    let ops=frame["operations"].as_array()
      .ok_or(EventOutboxR366Error::Corrupt)?;
    if ops.is_empty()||ops.len()>(if r1129_private_paper_32(){32}else{16}){
     return Err(EventOutboxR366Error::Corrupt)
    }
    for (idx,op) in ops.iter().enumerate(){
     let event_id=format!("paper_r824_{run}_{seq:020}_{idx:02}");
     if op["table"]!="events"||op["op"]!="insert"||
        op["row"]["event_id"]!=event_id||
        op["row"]["run_id"]!=run||
        op["row"]["mode"]!="paper"||
        op["row"]["verification_status"]!="unverified"||
        op["row"]["is_exchange_verified"]!=0||
        expected.insert(event_id,op["row"].clone()).is_some(){
       return Err(EventOutboxR366Error::Corrupt)
     }
    }
   },
   _=>return Err(EventOutboxR366Error::Corrupt)
  }
 }
 drop(rows);
 r1130_measure(13,r1130_t);
 let r1130_t=std::time::Instant::now();
 if count!=h.retained||prev!=h.latest||
    bytes!=r726_u(&c,"retained_bytes")?||
    m["applied_hash"].as_str()!=Some(last_hash.as_str()){
  return Err(EventOutboxR366Error::Corrupt)
 }
 let new_count=if receipt_ahead{
  // The receipt already accounts for every retained operation.
  // Never double-count or re-apply an already durably mirrored prefix.
  if previous_events<(expected.len() as u64){
   return Err(EventOutboxR366Error::Corrupt)
  }
  previous_events
 }else{
  previous_events.checked_add(expected.len() as u64)
   .ok_or(EventOutboxR366Error::Pressure)?
 };
 // Counts rule out phantom unsourced inserts on either immutable DB;
 // exact new rows rule out equal source/DB2 tampering under old outbox.
 if r366_query_num(src,"SELECT COUNT(*) FROM events")?!=new_count||
    r366_query_num(dst,"SELECT COUNT(*) FROM events")?!=new_count{
  return Err(EventOutboxR366Error::Corrupt)
 }
 r1092_check_new_physical_rows(src,dst,v,&expected)?;
 r1130_measure(14,r1130_t);
 let r1130_t=std::time::Instant::now();
 let latest=r1092_receipt(run,&r756_schema_hash(src)?,
   r726_s(&c,"feed_generation")?,h.applied,new_count)?;
 if receipt_ahead{
  if latest!=receipt||r800_streaming_original_parity(src,dst,v)?!=new_count{
   return Err(EventOutboxR366Error::Corrupt)
  }
 }else{
  exec(dst,&format!("UPDATE mirror_meta SET last_key_json={}
    WHERE id=1",r586_sql_literal(&serde_json::json!(latest))?))
    .map_err(r366_error)?;
 }
 exec(src,&format!("DELETE FROM mirror_outbox WHERE seq<={};
   UPDATE mirror_control SET cleanup_through={},
      retained_records=0,retained_bytes=0 WHERE id=1",
   h.applied,h.applied)).map_err(r366_error)?;
 if r366_query_num(src,"SELECT COUNT(*) FROM mirror_outbox")?!=0{
  return Err(EventOutboxR366Error::Corrupt)
 }
 mirror_tx.commit().map_err(r366_error)?;
 // Test-only crash at the actual cross-file durability boundary.
 // Cannot be activated in a compiled production/installed binary.
 #[cfg(test)]
 if !receipt_ahead &&
    std::env::var("SPOTARB_R1093_TEST_KILL_AFTER_DB2").ok().as_deref()
      ==Some("DISPOSABLE_PRIVATE_CHILD") &&
    root.parent()==Some(std::env::temp_dir().as_path()) &&
    root.file_name().and_then(|s|s.to_str())
      .is_some_and(|s|s.starts_with("astra_r671_file_")){
   unsafe{libc::raise(libc::SIGKILL);}
   panic!("R1093 test-only SIGKILL returned")
 }
 source_tx.commit().map_err(r366_error)?;
 r1130_measure(15,r1130_t);
 Ok((count,0))
}

// Recover a committed DB2 receipt BEFORE any additional DB2 replay.
// An uncertain or unquiesced frontier is quarantined, never advanced.
fn r1093_preflight_receipt_recovery(
 root:&Path,src:&Db,dst:&Db,v:&serde_json::Value,run:&str
)->Result<u64,EventOutboxR366Error>{
 // Source writer is concurrent: control and outbox COUNT must share
 // one SQLite snapshot or a valid committed batch looks corrupt.
 let (h,c,m)={
  exec(src,"BEGIN DEFERRED").map_err(r366_error)?;
  let _snapshot=R805SourceRead(src);
  r805_read_head(root,src,dst)?
 };
 let receipt=m["last_key_json"].as_str().ok_or(EventOutboxR366Error::Corrupt)?;
 let parsed:serde_json::Value=serde_json::from_str(receipt)
  .map_err(|_|EventOutboxR366Error::Corrupt)?;
 let seq=parsed["acked_seq"].as_u64().ok_or(EventOutboxR366Error::Corrupt)?;
 let events=parsed["events"].as_u64().ok_or(EventOutboxR366Error::Corrupt)?;
 if r1092_receipt(run,&r756_schema_hash(src)?,
    r726_s(&c,"feed_generation")?,seq,events)?!=receipt{
  return Err(EventOutboxR366Error::Corrupt)
 }
 if seq==h.cleaned{return Ok(0)}
 r1092_check_guards(src,v)?;
 r1092_check_guards(dst,v)?;
 if seq!=h.applied||h.latest!=h.applied||h.retained==0||h.retained>64{
  return Err(EventOutboxR366Error::Conflict)
 }
 let (recovered,remaining)=
   r1092_private_dynamic_incremental_ack(root,src,dst,v,run)?;
 if remaining!=0{return Err(EventOutboxR366Error::Corrupt)}
 Ok(recovered)
}

#[cfg(test)]
mod r1092_tests{
 use super::*;
 use crate::{Book,RawFrame,Update,Venue,Owner};
 use std::os::unix::fs::DirBuilderExt;
 struct Temp(PathBuf);
 impl Temp{
  fn new()->Self{
   let mut p=std::env::temp_dir();
   let stamp=std::time::SystemTime::now().duration_since(
      std::time::UNIX_EPOCH).unwrap().as_nanos();
   p.push(format!("astra_r671_file_{}_{}_r1092",std::process::id(),stamp));
   fs::DirBuilder::new().mode(0o700).create(&p).unwrap();
   drop(r756_initialize(&p).unwrap());Self(p)
  }
 }
 impl Drop for Temp{fn drop(&mut self){let _=fs::remove_dir_all(&self.0);}}
 fn ev(i:u64)->(RawFrame,Update){
  let ns=1_000_000_000_u64+i*10_000_000;
  let ms=1_800_000_000_000_i64+(i as i64)*10;
  let venue=Venue::OkxSpot;
  let frame=RawFrame{source:None,venue,epoch:1,received_ns:ns,
   received_wall_ms:ms,raw:format!("{{\"source\":\"r1092\",\"seq\":{i}}}")};
  let book=Book{bids:vec![(101.0,1.0)],asks:vec![(102.0,1.0)],
    received_ns:ns,received_wall_ms:ms,
    exchange_ts_ms:Some(ms),changed_ns:ns,epoch:1,sequence:Some(i as i64)};
  (frame,Update{base:"BTC".into(),venue,book})
 }
 #[test]fn r1093_clean_genesis_preflight_does_not_mutate(){
  let t=Temp::new();let v=r586_oracle();
  r1092_private_empty_genesis(&t.0,"batch_1").unwrap();
  let src=r671_open(&t.0,"r671-private-db1.sqlite",false).unwrap();
  let dst=r671_open(&t.0,"r671-private-db2.sqlite",false).unwrap();
  assert_eq!(r1093_preflight_receipt_recovery(
    &t.0,&src,&dst,&v,"batch_1"),Ok(0));
 }
 #[test]fn r1093_concurrent_source_snapshot_preflight_consistent(){
  let t=Temp::new();let v=r586_oracle();
  r1092_private_empty_genesis(&t.0,"batch_1").unwrap();
  let root=t.0.clone();
  let writer=std::thread::spawn(move||{
   let mut h=R819OriginalPaperHistory::new(&root,"batch_1").unwrap();
   let mut o=Owner::new(0,1,800).unwrap();
   for i in 1..=32{
    r824_commit_prepared_batch(&mut h,&mut o,&[ev(i)]).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(1));
   }
  });
  let src=r671_open(&t.0,"r671-private-db1.sqlite",false).unwrap();
  let dst=r671_open(&t.0,"r671-private-db2.sqlite",false).unwrap();
  for _ in 0..120{
   assert_eq!(r1093_preflight_receipt_recovery(
     &t.0,&src,&dst,&v,"batch_1"),Ok(0));
   std::thread::sleep(std::time::Duration::from_millis(1));
  }
  writer.join().unwrap();
  assert_eq!(r366_query_num(&src,
    "SELECT latest_seq FROM mirror_control WHERE id=1"),Ok(32));
 }
 #[test]fn r1093_disposable_test_child(){
  if std::env::var("SPOTARB_R1093_CHILD").ok().as_deref()
      !=Some("DISPOSABLE_PRIVATE_CHILD"){return}
  let root=PathBuf::from(std::env::var("SPOTARB_R1093_CHILD_ROOT").unwrap());
  assert_eq!(root.parent(),Some(std::env::temp_dir().as_path()));
  assert!(root.file_name().and_then(|s|s.to_str())
    .is_some_and(|s|s.starts_with("astra_r671_file_")));
  let src=r671_open(&root,"r671-private-db1.sqlite",false).unwrap();
  let dst=r671_open(&root,"r671-private-db2.sqlite",false).unwrap();
  let _=r1092_private_dynamic_incremental_ack(
    &root,&src,&dst,&r586_oracle(),"batch_1");
  panic!("R1093 crash boundary was not reached")
 }
 fn r1093_killed_gap()->Temp{
  use std::os::unix::process::ExitStatusExt;
  let t=Temp::new();let v=r586_oracle();
  r1092_private_empty_genesis(&t.0,"batch_1").unwrap();
  let mut h=R819OriginalPaperHistory::new(&t.0,"batch_1").unwrap();
  let mut o=Owner::new(0,1,800).unwrap();
  assert_eq!(r824_commit_prepared_batch(&mut h,&mut o,
    &(1..=16).map(ev).collect::<Vec<_>>()),Ok((1,16)));
  assert_eq!(r1090_paper_prepared_burst(&t.0,&h.source,&h.sink,&v,128),Ok(1));
  drop(h);
  let child=std::process::Command::new(std::env::current_exe().unwrap())
   .arg("r1093_disposable_test_child").arg("--nocapture")
   .env("SPOTARB_R1093_CHILD","DISPOSABLE_PRIVATE_CHILD")
   .env("SPOTARB_R1093_CHILD_ROOT",&t.0)
   .env("SPOTARB_R1093_TEST_KILL_AFTER_DB2","DISPOSABLE_PRIVATE_CHILD")
   .output().unwrap();
  assert_eq!(child.status.signal(),Some(libc::SIGKILL),
    "child stderr={}",String::from_utf8_lossy(&child.stderr));
  t
 }
 #[test]fn r1093_sigkill_after_db2_receipt_reconcile_and_continue(){
  use std::os::unix::process::ExitStatusExt;
  let t=Temp::new();let v=r586_oracle();
  assert_eq!(r1092_private_empty_genesis(&t.0,"batch_1"),Ok(()));
  let mut h=R819OriginalPaperHistory::new(&t.0,"batch_1").unwrap();
  let mut owner=Owner::new(0,1,800).unwrap();
  assert_eq!(r824_commit_prepared_batch(&mut h,&mut owner,
     &(1..=16).map(ev).collect::<Vec<_>>()),Ok((1,16)));
  assert_eq!(r1090_paper_prepared_burst(&t.0,&h.source,&h.sink,&v,128),Ok(1));
  drop(h);
  let child=std::process::Command::new(std::env::current_exe().unwrap())
    .arg("r1093_disposable_test_child").arg("--nocapture")
    .env("SPOTARB_R1093_CHILD","DISPOSABLE_PRIVATE_CHILD")
    .env("SPOTARB_R1093_CHILD_ROOT",&t.0)
    .env("SPOTARB_R1093_TEST_KILL_AFTER_DB2","DISPOSABLE_PRIVATE_CHILD")
    .output().unwrap();
  assert_eq!(child.status.signal(),Some(libc::SIGKILL),
    "stdout={} stderr={}",
    String::from_utf8_lossy(&child.stdout),
    String::from_utf8_lossy(&child.stderr));
  let src=r671_open(&t.0,"r671-private-db1.sqlite",false).unwrap();
  let dst=r671_open(&t.0,"r671-private-db2.sqlite",false).unwrap();
  let (before,_,meta)=r805_read_head(&t.0,&src,&dst).unwrap();
  assert_eq!((before.latest,before.applied,before.cleaned,before.retained),
    (1,1,0,1));
  let receipt:serde_json::Value=
    serde_json::from_str(meta["last_key_json"].as_str().unwrap()).unwrap();
  assert_eq!(receipt["acked_seq"],1);
  assert_eq!(receipt["events"],16);
  assert_eq!(r1093_preflight_receipt_recovery(
    &t.0,&src,&dst,&v,"batch_1"),Ok(1));
  assert_eq!(r1093_preflight_receipt_recovery(
    &t.0,&src,&dst,&v,"batch_1"),Ok(0));
  assert_eq!(r366_query_num(&src,
    "SELECT COUNT(*) FROM mirror_outbox"),Ok(0));
  assert_eq!(r800_streaming_original_parity(&src,&dst,&v),Ok(16));
  drop(src);drop(dst);
  let mut resumed=R819OriginalPaperHistory::new(&t.0,"batch_1").unwrap();
  let mut owner=Owner::new(0,1,800).unwrap();
  assert_eq!(r824_commit_prepared_batch(&mut resumed,&mut owner,
    &[ev(17)]),Ok((2,1)));
  assert_eq!(r1090_paper_prepared_burst(
    &t.0,&resumed.source,&resumed.sink,&v,128),Ok(1));
  assert_eq!(r1092_private_dynamic_incremental_ack(
    &t.0,&resumed.source,&resumed.sink,&v,"batch_1"),Ok((1,0)));
  assert_eq!(r800_streaming_original_parity(
    &resumed.source,&resumed.sink,&v),Ok(17));
 }
 #[test]fn r1093_crash_gap_poison_receipt_phantom_and_missing_guard_refused(){
  let v=r586_oracle();
  for case in ["phantom","receipt","guard","future"]{
   let t=r1093_killed_gap();
   let src=r671_open(&t.0,"r671-private-db1.sqlite",false).unwrap();
   let dst=r671_open(&t.0,"r671-private-db2.sqlite",false).unwrap();
   match case{
    "phantom"=>exec(&dst,"INSERT INTO events SELECT
      'r1093_unsourced_ghost',schema_version,mode,category,name,source,run_id,
      occurred_at_ms,received_at_ms,correlation_id,signal_id,base,
      shadow_group_id,comparison_id,reason_code,verification_status,
      is_exchange_verified,payload_json,verification_json,content_hash
      FROM events LIMIT 1").unwrap(),
    "receipt"=>exec(&dst,"UPDATE mirror_meta SET
       last_key_json='{\"acked_seq\":1,\"events\":16}' WHERE id=1").unwrap(),
    "guard"=>exec(&src,"DROP TRIGGER r924_guard_events_update").unwrap(),
    "future"=>{
     drop(src);drop(dst);
     let mut h=R819OriginalPaperHistory::new(&t.0,"batch_1").unwrap();
     let mut o=Owner::new(0,1,800).unwrap();
     assert_eq!(r824_commit_prepared_batch(&mut h,&mut o,&[ev(17)]),
       Ok((2,1)));
     drop(h);
     let a=r671_open(&t.0,"r671-private-db1.sqlite",false).unwrap();
     let b=r671_open(&t.0,"r671-private-db2.sqlite",false).unwrap();
     assert_eq!(r1093_preflight_receipt_recovery(
       &t.0,&a,&b,&v,"batch_1"),Err(EventOutboxR366Error::Conflict));
     assert_eq!(r366_query_num(&a,
       "SELECT cleanup_through FROM mirror_control WHERE id=1"),Ok(0));
     continue
    },
    _=>unreachable!()
   }
   assert_eq!(r1093_preflight_receipt_recovery(
     &t.0,&src,&dst,&v,"batch_1"),Err(EventOutboxR366Error::Corrupt),
     "case={case}");
   assert_eq!(r366_query_num(&src,
      "SELECT cleanup_through FROM mirror_control WHERE id=1"),Ok(0));
   assert_eq!(r366_query_num(&src,
      "SELECT COUNT(*) FROM mirror_outbox"),Ok(1));
  }
 }
 #[test]fn r1092_dynamic_16_plus_5_events_two_exact_proven_acks(){
  let t=Temp::new();let v=r586_oracle();
  assert_eq!(r1092_private_empty_genesis(&t.0,"batch_1"),Ok(()));
  let mut h=R819OriginalPaperHistory::new(&t.0,"batch_1").unwrap();
  let mut o=Owner::new(0,1,800).unwrap();
  assert_eq!(r824_commit_prepared_batch(&mut h,&mut o,
     &(1..=16).map(ev).collect::<Vec<_>>()),Ok((1,16)));
  assert_eq!(r1090_paper_prepared_burst(&t.0,&h.source,&h.sink,&v,128),Ok(1));
  assert_eq!(r1092_private_dynamic_incremental_ack(&t.0,
    &h.source,&h.sink,&v,"batch_1"),Ok((1,0)));
  assert_eq!(r824_commit_prepared_batch(&mut h,&mut o,
     &(17..=21).map(ev).collect::<Vec<_>>()),Ok((2,5)));
  assert_eq!(r1090_paper_prepared_burst(&t.0,&h.source,&h.sink,&v,128),Ok(1));
  assert_eq!(r1092_private_dynamic_incremental_ack(&t.0,
    &h.source,&h.sink,&v,"batch_1"),Ok((1,0)));
  assert_eq!(r800_streaming_original_parity(&h.source,&h.sink,&v),Ok(21));
  assert_eq!(r366_query_num(&h.source,
    "SELECT cleanup_through FROM mirror_control WHERE id=1"),Ok(2));
  assert_eq!(r366_query_num(&h.source,
    "SELECT COUNT(*) FROM mirror_outbox"),Ok(0));
 }
 #[test]fn r1092_cannot_ack_phantom_event_even_when_db2_applied(){
  let t=Temp::new();let v=r586_oracle();
  assert_eq!(r1092_private_empty_genesis(&t.0,"batch_1"),Ok(()));
  let mut h=R819OriginalPaperHistory::new(&t.0,"batch_1").unwrap();
  let mut o=Owner::new(0,1,800).unwrap();
  assert_eq!(r824_commit_prepared_batch(&mut h,&mut o,&[ev(1)]),Ok((1,1)));
  assert_eq!(r1090_paper_prepared_burst(&t.0,&h.source,&h.sink,&v,128),Ok(1));
  exec(&h.sink,"INSERT INTO events SELECT
    'unsourced_ghost',schema_version,mode,category,name,source,run_id,
    occurred_at_ms,received_at_ms,correlation_id,signal_id,base,
    shadow_group_id,comparison_id,reason_code,verification_status,
    is_exchange_verified,payload_json,verification_json,content_hash
    FROM events LIMIT 1").unwrap();
  assert_eq!(r1092_private_dynamic_incremental_ack(&t.0,
    &h.source,&h.sink,&v,"batch_1"),Err(EventOutboxR366Error::Corrupt));
  assert_eq!(r366_query_num(&h.source,
    "SELECT cleanup_through FROM mirror_control WHERE id=1"),Ok(0));
 }
 #[test]fn r1092_original_history_tamper_and_trigger_removal_refused(){
  let t=Temp::new();let v=r586_oracle();
  assert_eq!(r1092_private_empty_genesis(&t.0,"batch_1"),Ok(()));
  let mut h=R819OriginalPaperHistory::new(&t.0,"batch_1").unwrap();
  let mut o=Owner::new(0,1,800).unwrap();
  assert_eq!(r824_commit_prepared_batch(&mut h,&mut o,&[ev(1)]),Ok((1,1)));
  assert_eq!(r1090_paper_prepared_burst(&t.0,&h.source,&h.sink,&v,128),Ok(1));
  assert!(exec(&h.source,"UPDATE events SET category='poison'").is_err());
  exec(&h.source,"DROP TRIGGER r924_guard_events_update").unwrap();
  assert_eq!(r1092_private_dynamic_incremental_ack(&t.0,
     &h.source,&h.sink,&v,"batch_1"),Err(EventOutboxR366Error::Corrupt));
  assert_eq!(r366_query_num(&h.source,
    "SELECT cleanup_through FROM mirror_control WHERE id=1"),Ok(0));
 }
}
