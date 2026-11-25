// R376-R385: independent read-back of isolated event+outbox.
// Never touches DB1, DB2, installed PAPER or private exchange clients.
fn r376_integrity(db:&Db)->Result<(),EventOutboxR366Error>{
 use serde_json::Value;
 if r366_query_num(db,"PRAGMA user_version")?!=426{return Err(EventOutboxR366Error::Schema)}
 let mut check=Stmt::new(db,"PRAGMA integrity_check").map_err(r366_error)?;
 if check.step().map_err(r366_error)?!=SQLITE_ROW||
    check.strval(0).map_err(r366_error)?!="ok"||
    check.step().map_err(r366_error)?!=SQLITE_DONE{
  return Err(EventOutboxR366Error::Corrupt)
 }
 drop(check);
 let mut events=std::collections::BTreeMap::<String,(String,String,Option<String>)>::new();
 let mut q=Stmt::new(db,
 "SELECT event_id,mode,category,name,source,run_id,occurred_at_ms,
  verification_status,is_exchange_verified,payload_json,verification_json,
  source_record_json,content_hash,correlation_id FROM events ORDER BY event_id")
 .map_err(r366_error)?;
 loop{
  match q.step().map_err(r366_error)?{
   SQLITE_DONE=>break,
   SQLITE_ROW=>{
    let id=q.strval(0).map_err(r366_error)?;
    let mode=q.strval(1).map_err(r366_error)?;
    let cat=q.strval(2).map_err(r366_error)?;
    let name=q.strval(3).map_err(r366_error)?;
    let source=q.strval(4).map_err(r366_error)?;
    let run_id=q.strval(5).map_err(r366_error)?;
    let occurred=q.num(6).map_err(r366_error)?;
    let verification=q.strval(7).map_err(r366_error)?;
    let verified=q.num(8).map_err(r366_error)?;
    let payload=q.strval(9).map_err(r366_error)?;
    let evidence=q.strval(10).map_err(r366_error)?;
    let canonical=q.strval(11).map_err(r366_error)?;
    let saved_hash=q.strval(12).map_err(r366_error)?;
    let obj:Value=serde_json::from_str(&canonical)
      .map_err(|_|EventOutboxR366Error::Corrupt)?;
    let correlation=obj["correlation_id"].as_str().map(str::to_string);
    if r386_python_collector_json(&obj).map_err(|_|EventOutboxR366Error::Corrupt)?!=canonical||
       r366_sha(&canonical)!=saved_hash||
       mode!="paper"||verified!=0||obj["schema_version"]!=1||
       obj["mode"]!="paper"||obj["is_exchange_verified"]!=0||
       obj["event_id"]!=id||obj["category"]!=cat||obj["name"]!=name||
       obj["source"]!=source||obj["run_id"]!=run_id||
       obj["occurred_at_ms"].as_u64()!=Some(occurred)||
       obj["verification_status"]!=verification||
       obj["payload_json"]!=payload||obj["verification_json"]!=evidence||
       r426_projection_fields(&cat).is_none(){
     return Err(EventOutboxR366Error::Corrupt)
    }
    if !payload.starts_with('{')||!evidence.starts_with('{'){
     return Err(EventOutboxR366Error::Corrupt)
    }
    // Independently derive category projections from the immutable hashed
    // source payload, not from attacker-editable outbox row/after_hash.
    let decoded:Value=serde_json::from_str(&payload)
      .map_err(|_|EventOutboxR366Error::Corrupt)?;
    let object=decoded.as_object().ok_or(EventOutboxR366Error::Corrupt)?;
    let category_row=r426_category_row(db,&cat,&id)
      .map_err(|_|EventOutboxR366Error::Corrupt)?;
    if category_row["event_id"]!=id||category_row["mode"]!="paper"||
       category_row["is_exchange_verified"]!=0{
      return Err(EventOutboxR366Error::Corrupt)
    }
    for field in r426_projection_fields(&cat).ok_or(EventOutboxR366Error::Corrupt)?{
     let source=object.get(*field).unwrap_or(&Value::Null);
     let expected=r426_projection_string(source)
       .map_err(|_|EventOutboxR366Error::Corrupt)?;
     let observed=category_row.get(*field).ok_or(EventOutboxR366Error::Corrupt)?;
     match expected{
      Some(value) if observed.as_str()!=Some(&value)=>return Err(EventOutboxR366Error::Corrupt),
      None if !observed.is_null()=>return Err(EventOutboxR366Error::Corrupt),
      _=>{}
     }
    }
    // SQLite FK controls correlation state; independent event payload is used
    // for the source/outbox consistency, not for trading assertions.
    if events.insert(id,(cat,saved_hash,correlation)).is_some(){
      return Err(EventOutboxR366Error::Corrupt)
    }
   },
   _=>return Err(EventOutboxR366Error::Corrupt)
  }
 }
 drop(q);
 // Detect missing, mismatched, duplicate or orphan category projections.
 let mut category_presence=std::collections::BTreeMap::<String,u8>::new();
 for (category,_) in R426_CATEGORY_FIELDS{
  let mut rows=Stmt::new(db,&format!("SELECT event_id FROM {category}"))
   .map_err(r366_error)?;
  loop{
   match rows.step().map_err(r366_error)?{
    SQLITE_DONE=>break,
    SQLITE_ROW=>{
     let id=rows.strval(0).map_err(r366_error)?;
     if !events.get(&id).is_some_and(|(cat,_,_)|cat==category){
      return Err(EventOutboxR366Error::Corrupt)
     }
     let count=category_presence.entry(id).or_insert(0);
     *count=count.checked_add(1).ok_or(EventOutboxR366Error::Corrupt)?;
    },
    _=>return Err(EventOutboxR366Error::Corrupt)
   }
  }
 }
 if category_presence.len()!=events.len()||
    category_presence.values().any(|v|*v!=1){
  return Err(EventOutboxR366Error::Corrupt)
 }
 let mut last=0_u64;let mut total_bytes=0_u64;
 let mut seen=std::collections::BTreeSet::<String>::new();
 let mut seen_attempts=std::collections::BTreeSet::<String>::new();
 let mut q=Stmt::new(db,
  "SELECT seq,prev_seq,tx_id,tx_kind,schema_generation,payload_json,
     payload_hash,payload_bytes,committed_at_ms,feed_generation
   FROM mirror_outbox ORDER BY seq").map_err(r366_error)?;
 loop{
  match q.step().map_err(r366_error)?{
   SQLITE_DONE=>break,
   SQLITE_ROW=>{
    let seq=q.num(0).map_err(r366_error)?;
    let prev=q.num(1).map_err(r366_error)?;
    let txid=q.strval(2).map_err(r366_error)?;
    let kind=q.strval(3).map_err(r366_error)?;
    let schema=q.num(4).map_err(r366_error)?;
    let json=q.strval(5).map_err(r366_error)?;
    let hash=q.strval(6).map_err(r366_error)?;
    let bytes=q.num(7).map_err(r366_error)?;
    let stamp=q.num(8).map_err(r366_error)?;
    let generation=q.strval(9).map_err(r366_error)?;
    let frame:Value=serde_json::from_str(&json)
      .map_err(|_|EventOutboxR366Error::Corrupt)?;
    let ops=frame["operations"].as_array()
      .ok_or(EventOutboxR366Error::Corrupt)?;
    if seq!=last.checked_add(1).ok_or(EventOutboxR366Error::Corrupt)?||
       prev!=last||kind!="event_append"||schema!=1||
       generation!="r366-isolated"||stamp==0||
       bytes!=json.len().saturating_add(384) as u64||
       hash!=r366_sha(&json)||
       r386_python_mirror_canonical(&frame).map_err(|_|EventOutboxR366Error::Corrupt)?!=json||
       frame["protocol_version"]!=1||frame["schema_generation"]!=1||
       frame["tx_kind"]!="event_append"||
       !(2..=3).contains(&ops.len()){
      return Err(EventOutboxR366Error::Corrupt)
    }
    let source_id=frame["operations"].as_array().and_then(|v|v.iter()
      .find(|x|x["table"]=="events")).and_then(|v|
       v["key"]["event_id"].as_str()).ok_or(EventOutboxR366Error::Corrupt)?;
    let (cat,event_sha,correlation)=events.get(source_id)
      .ok_or(EventOutboxR366Error::Corrupt)?;
    if txid!=format!("tx-r366-{seq}-{source_id}")||
       !seen.insert(source_id.to_owned()){
       return Err(EventOutboxR366Error::Corrupt)
    }
    let fresh_attempt=correlation.as_ref().is_some_and(|id|
       seen_attempts.insert(id.clone()));
    let mut wanted=std::collections::BTreeSet::new();
    wanted.insert("events");wanted.insert(cat.as_str());
    if fresh_attempt{wanted.insert("attempts");}
    if ops.len()!=wanted.len(){return Err(EventOutboxR366Error::Corrupt)}
    for operation in ops{
      let table=operation["table"].as_str().ok_or(EventOutboxR366Error::Corrupt)?;
      if !wanted.remove(table)||operation["op"]!="insert"{
        return Err(EventOutboxR366Error::Corrupt)
      }
      let key=if table=="attempts"{
        correlation.as_deref().ok_or(EventOutboxR366Error::Corrupt)?
      }else{source_id};
      // Re-read the actual source row rather than trusting a pair of
      // attacker-recomputed payload and after hashes.
      let actual=r406_source_after(db,table,key,"insert")
        .map_err(|_|EventOutboxR366Error::Corrupt)?;
      if *operation!=actual{return Err(EventOutboxR366Error::Corrupt)}
      if table=="events"&&actual["row"]["content_hash"]!=*event_sha{
        return Err(EventOutboxR366Error::Corrupt)
      }
    }
    if !wanted.is_empty(){return Err(EventOutboxR366Error::Corrupt)}
    total_bytes=total_bytes.checked_add(bytes).ok_or(EventOutboxR366Error::Corrupt)?;
    last=seq;
   },
   _=>return Err(EventOutboxR366Error::Corrupt)
  }
 }
 drop(q);
 if seen.len()!=events.len()||
    r366_query_num(db,"SELECT COUNT(*) FROM attempts")?!=seen_attempts.len() as u64{
  return Err(EventOutboxR366Error::Corrupt)
 }
 let mut control=Stmt::new(db,"SELECT state,latest_seq,retained_bytes,
  retained_records,feed_generation FROM mirror_control WHERE id=1").map_err(r366_error)?;
 if control.step().map_err(r366_error)?!=SQLITE_ROW||
    control.strval(0).map_err(r366_error)?!="BOOTSTRAP_REQUIRED"||
    control.num(1).map_err(r366_error)?!=last||
    control.num(2).map_err(r366_error)?!=total_bytes||
    control.num(3).map_err(r366_error)?!=seen.len() as u64||
    control.strval(4).map_err(r366_error)?!="r366-isolated"||
    control.step().map_err(r366_error)?!=SQLITE_DONE{
    return Err(EventOutboxR366Error::Corrupt)
 }
 Ok(())
}
#[cfg(test)]
mod r376_tests{
 use super::*;
 use super::r366_tests::{fixture_root,record};
 #[test]fn r376_valid_two_events_reopen_and_audit(){
   let root=fixture_root();let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
   w.append(&record("r376_001","decisions"),1_792_660_000_000,false).unwrap();
   w.append(&record("r376_002","errors"),1_792_660_000_001,false).unwrap();
   assert_eq!(w.audit().unwrap().last_seq,2);
   drop(w);
   assert_eq!(IsolatedEventOutboxR366::reopen(&root).unwrap().audit().unwrap().events,2);
   fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r376_detects_same_hash_forged_outbox(){
   let root=fixture_root();let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
   w.append(&record("r376_001","decisions"),1_792_660_000_000,false).unwrap();
   let db=r366_open(&w.path).unwrap();
   exec(&db,"UPDATE mirror_outbox SET payload_hash='same-forged-hash'").unwrap();
   drop(db);
   assert_eq!(w.audit(),Err(EventOutboxR366Error::Corrupt));
   assert!(matches!(IsolatedEventOutboxR366::reopen(&root),Err(EventOutboxR366Error::Corrupt)));
   fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r376_detects_forged_event_content_hash(){
   let root=fixture_root();let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
   w.append(&record("r376_001","decisions"),1_792_660_000_000,false).unwrap();
   let db=r366_open(&w.path).unwrap();
   exec(&db,"UPDATE events SET content_hash='fake';UPDATE mirror_outbox SET payload_hash='fake'").unwrap();
   drop(db);
   assert_eq!(w.audit(),Err(EventOutboxR366Error::Corrupt));
   fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r376_detects_broken_prev_seq(){
   let root=fixture_root();let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
   w.append(&record("r376_001","decisions"),1_792_660_000_000,false).unwrap();
   let db=r366_open(&w.path).unwrap();
   exec(&db,"UPDATE mirror_outbox SET prev_seq=8 WHERE seq=1").unwrap();
   drop(db);
   assert_eq!(w.audit(),Err(EventOutboxR366Error::Corrupt));
   fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r376_detects_mismatched_source_category(){
   let root=fixture_root();let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
   w.append(&record("r376_001","decisions"),1_792_660_000_000,false).unwrap();
   let db=r366_open(&w.path).unwrap();
   exec(&db,"DELETE FROM decisions").unwrap();
   drop(db);
   assert_eq!(w.audit(),Err(EventOutboxR366Error::Corrupt));
   fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r376_rejects_corruption_before_append(){
   let root=fixture_root();let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
   w.append(&record("r376_001","decisions"),1_792_660_000_000,false).unwrap();
   let db=r366_open(&w.path).unwrap();
   exec(&db,"UPDATE mirror_outbox SET payload_hash='bad'").unwrap();
   drop(db);
   assert_eq!(w.append(&record("r376_002","decisions"),
        1_792_660_000_001,false),Err(EventOutboxR366Error::Corrupt));
   fs::remove_dir_all(root).unwrap();
 }
}
