// Astra R756+: ORIGINAL PHYSICAL Python mirror_control/outbox/meta names and
// columns in TWO isolated FILE-backed Rust SQLite databases.
// Source Python: ai/mirror_outbox.py, scripts/mirror_service.py.
// NO production DB, installed PAPER, REAL, exchange API or credentials.
fn r756_python_source_schema(src:&Db,dst:&Db,v:&serde_json::Value)
 ->Result<(),EventOutboxR366Error>{
 r671_original_schema(src,v)?;
 r671_original_schema(dst,v)?;
 exec(src,"CREATE TABLE mirror_outbox(
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  prev_seq INTEGER NOT NULL,
  tx_id TEXT NOT NULL UNIQUE,
  tx_kind TEXT NOT NULL,
  schema_generation INTEGER NOT NULL,
  payload_json TEXT NOT NULL,
  payload_hash TEXT NOT NULL,
  payload_bytes INTEGER NOT NULL,
  committed_at_ms INTEGER NOT NULL,
  feed_generation TEXT NOT NULL DEFAULT 'legacy');
 CREATE TABLE mirror_control(
  id INTEGER PRIMARY KEY CHECK(id=1),
  state TEXT NOT NULL CHECK(state IN
   ('BOOTSTRAP_REQUIRED','NORMAL','REBUILD_REQUIRED')),
  reason TEXT,
  latest_seq INTEGER NOT NULL DEFAULT 0,
  cleanup_through INTEGER NOT NULL DEFAULT 0,
  retained_bytes INTEGER NOT NULL DEFAULT 0,
  retained_records INTEGER NOT NULL DEFAULT 0,
  schema_generation INTEGER NOT NULL,
  feed_generation TEXT NOT NULL DEFAULT 'legacy',
  baseline_seq INTEGER NOT NULL DEFAULT 0,
  failure_seq INTEGER,
  failure_reason TEXT,
  baseline_source_identity TEXT,
  baseline_schema_version INTEGER,
  baseline_user_version INTEGER,
  baseline_db1_bytes INTEGER,
  baseline_db1_mtime_ns INTEGER);
 INSERT INTO mirror_control(id,state,reason,latest_seq,cleanup_through,
  retained_bytes,retained_records,schema_generation,feed_generation,baseline_seq)
  VALUES(1,'NORMAL',NULL,0,0,0,0,1,'legacy',0);")
  .map_err(r366_error)?;
 Ok(())
}
fn r756_schema_hash(db:&Db)->Result<String,EventOutboxR366Error>{
 // Exactly scripts/mirror_service.py _schema: 17 collector tables plus
 // indexes/triggers; exclude only original Python control/outbox.
 let mut q=Stmt::new(db,"SELECT type,name,sql FROM sqlite_master
  WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%'
  AND name NOT IN ('mirror_outbox','mirror_control','mirror_meta')
  ORDER BY CASE type WHEN 'table' THEN 0 WHEN 'index' THEN 1 ELSE 2 END,name")
  .map_err(r366_error)?;
 let mut rows=Vec::new();
 loop{
  match q.step().map_err(r366_error)?{
   SQLITE_DONE=>break,
   SQLITE_ROW=>rows.push(serde_json::json!([
    q.strval(0).map_err(r366_error)?,
    q.strval(1).map_err(r366_error)?,
    q.strval(2).map_err(r366_error)?
   ])),
   _=>return Err(EventOutboxR366Error::Sql)
  }
 }
 let table_count=rows.iter().filter(|r|r[0]=="table").count();
 if table_count!=17{return Err(EventOutboxR366Error::Schema)}
 Ok(r366_sha(&r386_python_mirror_canonical(&serde_json::json!(rows))?))
}
fn r756_initialize(root:&Path)
 ->Result<(Db,Db,serde_json::Value),EventOutboxR366Error>{
 let v=r586_oracle();
 r566_check_source_manifest(&v)?;
 let src=r671_open(root,"r671-private-db1.sqlite",true)?;
 let dst=r671_open(root,"r671-private-db2.sqlite",true)?;
 r756_python_source_schema(&src,&dst,&v)?;
 let hash=r756_schema_hash(&src)?;
 let identity=r696_identity(root)?;
 let schema_ver=r366_query_num(&src,"PRAGMA schema_version")?;
 let meta_sql=format!("CREATE TABLE mirror_meta(
  id INTEGER PRIMARY KEY CHECK(id=1), phase TEXT NOT NULL,
  source_identity TEXT NOT NULL, schema_version INTEGER NOT NULL,
  user_version INTEGER NOT NULL, schema_hash TEXT NOT NULL,
  bootstrap_seq INTEGER NOT NULL, applied_seq INTEGER NOT NULL,
  applied_hash TEXT, table_index INTEGER NOT NULL,
  feed_generation TEXT NOT NULL, baseline_seq INTEGER NOT NULL,
  last_key_json TEXT, last_success_ms INTEGER);
  INSERT INTO mirror_meta(id,phase,source_identity,schema_version,
   user_version,schema_hash,bootstrap_seq,applied_seq,applied_hash,
   table_index,feed_generation,baseline_seq,last_key_json,last_success_ms)
   VALUES(1,'SYNCED',{},{schema_ver},4,{},0,0,NULL,17,'legacy',0,NULL,1000);",
  r586_sql_literal(&serde_json::json!(identity))?,
  r586_sql_literal(&serde_json::json!(hash))?);
 exec(&dst,&meta_sql).map_err(r366_error)?;
 let folder=fs::File::open(root).map_err(|_|EventOutboxR366Error::Path)?;
 folder.sync_all().map_err(|_|EventOutboxR366Error::Sql)?;
 Ok((src,dst,v))
}
fn r756_read_json(db:&Db,table:&str,fields:&[&str],tail:&str)
 ->Result<Vec<serde_json::Value>,EventOutboxR366Error>{
 if !matches!(table,"mirror_control"|"mirror_outbox"|"mirror_meta"){
  return Err(EventOutboxR366Error::Schema)
 }
 let mut kv=Vec::new();
 for field in fields{
  let k=r586_identifier(field)?;
  kv.push(format!("'{field}',{k}"));
 }
 let sql=format!("SELECT json_object({}) FROM {table} {tail}",kv.join(","));
 let mut q=Stmt::new(db,&sql).map_err(r366_error)?;
 let mut result=Vec::new();
 loop{
  match q.step().map_err(r366_error)?{
   SQLITE_DONE=>break,
   SQLITE_ROW=>{
    if result.len()>=513{return Err(EventOutboxR366Error::Pressure)}
    let raw=q.strval(0).map_err(r366_error)?;
    result.push(serde_json::from_str(&raw)
     .map_err(|_|EventOutboxR366Error::Corrupt)?);
   },
   _=>return Err(EventOutboxR366Error::Sql)
  }
 }
 Ok(result)
}
fn r756_snapshot(root:&Path,src:&Db,dst:&Db)
 ->Result<(serde_json::Value,serde_json::Value,Vec<serde_json::Value>),EventOutboxR366Error>{
 let controls=r756_read_json(src,"mirror_control",R726_CONTROL,"WHERE id=1")?;
 let metas=r756_read_json(dst,"mirror_meta",R726_META,"WHERE id=1")?;
 let rows=r756_read_json(src,"mirror_outbox",R726_OUTBOX,"ORDER BY seq")?;
 if controls.len()!=1||metas.len()!=1{return Err(EventOutboxR366Error::Corrupt)}
 let control=&controls[0];
 let meta=&metas[0];
 if meta["schema_hash"].as_str()!=Some(r756_schema_hash(src)?.as_str())
  ||r726_u(meta,"schema_version")?!=r366_query_num(src,"PRAGMA schema_version")?
  ||r726_u(meta,"user_version")?!=r366_query_num(src,"PRAGMA user_version")?{
  return Err(EventOutboxR366Error::Schema)
 }
 let identity=r696_identity(root)?;
 r726_python_protocol_contract(control,meta,&rows,
  r366_query_num(src,"PRAGMA schema_version")?,4,&identity)?;
 Ok((control.clone(),meta.clone(),rows))
}

fn r756_append(root:&Path,src:&Db,dst:&Db,v:&serde_json::Value,
 operations:&[serde_json::Value])->Result<u64,EventOutboxR366Error>{
 let (control,meta,_rows)=r756_snapshot(root,src,dst)?;
 if control["state"]!="NORMAL"||meta["phase"]!="SYNCED"||
 operations.is_empty()||operations.len()>17{
  return Err(EventOutboxR366Error::Conflict)
 }
 let seq=r726_u(&control,"latest_seq")?.checked_add(1)
  .ok_or(EventOutboxR366Error::Corrupt)?;
 let prev=seq-1;
 let frame=serde_json::json!({"protocol_version":1,
  "schema_generation":1,"tx_kind":"r756_offline_original",
  "operations":operations});
 let raw=r386_python_mirror_canonical(&frame)?;
 if raw.len()>4*1024*1024{return Err(EventOutboxR366Error::Pressure)}
 let size=(raw.len()+384) as u64;
 if r726_u(&control,"retained_bytes")?.saturating_add(size)>1024*1024*1024{
  return Err(EventOutboxR366Error::Pressure)
 }
 let tx=Tx::begin(src).map_err(r366_error)?;
 for op in operations{r586_apply_original_operation(src,v,op)?;}
 let txid=format!("tx-{seq:032x}");
 let sql=format!("INSERT INTO mirror_outbox(prev_seq,tx_id,tx_kind,
  schema_generation,payload_json,payload_hash,payload_bytes,
  committed_at_ms,feed_generation)
  VALUES({prev},{},'r756_offline_original',1,{},{},{size},{},'legacy');
  UPDATE mirror_control SET latest_seq={seq},
  retained_records=retained_records+1,
  retained_bytes=retained_bytes+{size} WHERE id=1",
  r586_sql_literal(&serde_json::json!(txid))?,
  r586_sql_literal(&serde_json::json!(raw))?,
  r586_sql_literal(&serde_json::json!(r366_sha(&raw)))?,
  1_000_000+seq);
 exec(src,&sql).map_err(r366_error)?;
 tx.commit().map_err(r366_error)?;
 Ok(seq)
}
fn r756_replay(root:&Path,src:&Db,dst:&Db,v:&serde_json::Value,
 limit:usize)->Result<u64,EventOutboxR366Error>{
 if !(1..=32).contains(&limit){return Err(EventOutboxR366Error::Pressure)}
 let (control,meta,rows)=r756_snapshot(root,src,dst)?;
 if control["state"]!="NORMAL"||meta["phase"]!="SYNCED"{
  return Err(EventOutboxR366Error::Conflict)
 }
 if r756_schema_hash(dst)?!=r756_schema_hash(src)?{
  return Err(EventOutboxR366Error::Schema)
 }
 let mut applied=r726_u(&meta,"applied_seq")?;
 let mut count=0_u64;
 let from_applied=applied;
 let tx=Tx::begin(dst).map_err(r366_error)?;
 for row in rows.iter().filter(|row|row["seq"].as_u64().unwrap_or(0)>from_applied)
  .take(limit){
  let seq=r726_u(row,"seq")?;
  if seq!=applied+1||r726_u(row,"prev_seq")?!=applied{
   return Err(EventOutboxR366Error::Corrupt)
  }
  let raw=r726_s(row,"payload_json")?;
  let frame:serde_json::Value=serde_json::from_str(raw)
   .map_err(|_|EventOutboxR366Error::Corrupt)?;
  let ops=frame["operations"].as_array().ok_or(EventOutboxR366Error::Corrupt)?;
  for op in ops{r586_apply_original_operation(dst,v,op)?;}
  applied=seq;count+=1;
  let hash=r726_s(row,"payload_hash")?;
  exec(dst,&format!("UPDATE mirror_meta SET applied_seq={seq},
   applied_hash={},last_success_ms={} WHERE id=1",
   r586_sql_literal(&serde_json::json!(hash))?,
   2_000_000+seq)).map_err(r366_error)?;
 }
 tx.commit().map_err(r366_error)?;
 Ok(count)
}
fn r756_cleanup(root:&Path,src:&Db,dst:&Db,v:&serde_json::Value)
 ->Result<(u64,u64),EventOutboxR366Error>{
 // R803: hold both file DB transaction snapshots across the ACK check
 // and source cleanup, avoiding the historical compare-then-lock race.
 let tx=Tx::begin(src).map_err(r366_error)?;
 let dst_tx=Tx::begin(dst).map_err(r366_error)?;
 let (control,meta,rows)=r756_snapshot(root,src,dst)?;
 if control["state"]!="NORMAL"||meta["phase"]!="SYNCED"
   ||r726_u(&control,"latest_seq")?!=r726_u(&meta,"applied_seq")?{
  return Err(EventOutboxR366Error::Conflict)
 }
 if r756_schema_hash(dst)?!=r756_schema_hash(src)?
   ||r366_query_num(dst,"SELECT COUNT(*) FROM pragma_foreign_key_check")?!=0{
  return Err(EventOutboxR366Error::Schema)
 }
 r800_streaming_original_parity(src,dst,v)?;
 if let Some(last)=rows.last(){
  if meta["applied_hash"]!=last["payload_hash"]{
   return Err(EventOutboxR366Error::Corrupt)
  }
 }
 let batch=&rows[..rows.len().min(128)];
 let deleted=batch.len() as u64;
 let bytes=batch.iter().try_fold(0u64,|acc,r|{
  acc.checked_add(r726_u(r,"payload_bytes")?)
   .ok_or(EventOutboxR366Error::Pressure)
 })?;
 let high=batch.last().and_then(|r|r["seq"].as_u64());
 if let Some(max_seq)=high{
  exec(src,&format!("DELETE FROM mirror_outbox WHERE seq<={max_seq};
   UPDATE mirror_control SET cleanup_through={max_seq},
   retained_records=retained_records-{deleted},
   retained_bytes=retained_bytes-{bytes} WHERE id=1;"))
   .map_err(r366_error)?;
 }
 dst_tx.commit().map_err(r366_error)?;
 tx.commit().map_err(r366_error)?;
 Ok((deleted,rows.len() as u64-deleted))
}

#[cfg(test)]
mod r756_tests{
 use super::*;
 use serde_json::{Value,json};
 use std::os::unix::fs::DirBuilderExt;
 struct Root(PathBuf);
 impl Root{
  fn new()->Self{
   let mut path=std::env::temp_dir();
   let stamp=std::time::SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
   path.push(format!("astra_r671_file_{}_{}_r756",std::process::id(),stamp));
   fs::DirBuilder::new().mode(0o700).create(&path).unwrap();
   Self(path)
  }
 }
 impl Drop for Root{fn drop(&mut self){let _=fs::remove_dir_all(&self.0);}}
 fn setup()->(Root,Db,Db,Value){
  let root=Root::new();
  let (src,dst,v)=r756_initialize(&root.0).unwrap();
  (root,src,dst,v)
 }
 fn frame(v:&Value,name:&str,kind:&str)->Value{
  let row=v["fixture_snapshot"]["tables"].as_array().unwrap().iter()
   .find(|r|r["schema"]["table"]==name).unwrap()["rows"][0]["row"].clone();
  r659_op(v,name,&row,kind).unwrap()
 }
 fn seed()->(Root,Db,Db,Value){
  let (root,src,dst,v)=setup();
  let ops=["attempts","collector_runs","events","decisions",
    "hourly_raw_rollups","metadata"].iter().map(|n|frame(&v,n,"insert"))
   .collect::<Vec<_>>();
  r756_append(&root.0,&src,&dst,&v,&ops).unwrap();
  (root,src,dst,v)
 }
 fn update(v:&Value,n:usize)->Vec<Value>{
  let mut row=v["fixture_snapshot"]["tables"].as_array().unwrap().iter()
   .find(|r|r["schema"]["table"]=="metadata").unwrap()["rows"][0]["row"].clone();
  row["value"]=json!(format!("original-mirror-{n}"));
  vec![r659_op(v,"metadata",&row,"update").unwrap()]
 }
 fn count(db:&Db,t:&str)->u64{
  r366_query_num(db,&format!("SELECT count(*) FROM {t}")).unwrap()
 }
 #[test]fn r756_exact_original_physical_control_outbox_meta_columns(){
  let (root,s,d,_v)=setup();
  let (c,m,rows)=r756_snapshot(&root.0,&s,&d).unwrap();
  assert_eq!(c.as_object().unwrap().len(),17);
  assert_eq!(m.as_object().unwrap().len(),14);
  assert!(rows.is_empty());
  assert_eq!(c["state"],"NORMAL");
  assert_eq!(m["phase"],"SYNCED");
  assert_eq!(count(&s,"mirror_control"),1);
  assert_eq!(count(&s,"mirror_outbox"),0);
  assert_eq!(count(&d,"mirror_meta"),1);
 }
 #[test]fn r757_source_tx_creates_original_python_shape_outbox(){
  let (root,s,d,_v)=seed();
  let (c,m,rows)=r756_snapshot(&root.0,&s,&d).unwrap();
  assert_eq!(rows.len(),1);
  assert_eq!(rows[0].as_object().unwrap().len(),10);
  assert_eq!(rows[0]["tx_id"].as_str().unwrap().len(),35);
  assert_eq!(c["retained_bytes"],rows[0]["payload_bytes"]);
  assert_eq!(c["retained_records"],1);
  assert_eq!(m["applied_seq"],0);
  assert_eq!(count(&s,"events"),1);
  assert_eq!(count(&d,"events"),0);
 }
 #[test]fn r758_original_schema_hash_matches_db1_db2(){
  let (root,s,d,_)=setup();
  assert_eq!(r756_schema_hash(&s),r756_schema_hash(&d));
  assert_eq!(r756_snapshot(&root.0,&s,&d).unwrap().1["schema_hash"],
    json!(r756_schema_hash(&s).unwrap()));
 }
 #[test]fn r759_apply_original_six_rows_then_cleanup_after_ack(){
  let (root,s,d,v)=seed();
  assert_eq!(r756_replay(&root.0,&s,&d,&v,8),Ok(1));
  assert_eq!(count(&d,"events"),1);
  assert_eq!(count(&d,"decisions"),1);
  assert_eq!(r756_cleanup(&root.0,&s,&d,&v),Ok((1,0)));
  assert_eq!(count(&s,"mirror_outbox"),0);
  assert_eq!(r756_snapshot(&root.0,&s,&d).unwrap().0["cleanup_through"],1);
 }
 #[test]fn r760_pending_outbox_must_not_cleanup_early(){
  let (root,s,d,v)=seed();
  assert_eq!(r756_cleanup(&root.0,&s,&d,&v),
    Err(EventOutboxR366Error::Conflict));
  assert_eq!(count(&s,"mirror_outbox"),1);
 }
 #[test]fn r761_reopen_original_python_physical_db_after_source_commit(){
  let (root,s,d,v)=seed();drop(s);drop(d);
  let s=r671_open(&root.0,"r671-private-db1.sqlite",false).unwrap();
  let d=r671_open(&root.0,"r671-private-db2.sqlite",false).unwrap();
  assert_eq!(r756_replay(&root.0,&s,&d,&v,1),Ok(1));
  drop(s);drop(d);
  let s=r671_open(&root.0,"r671-private-db1.sqlite",false).unwrap();
  let d=r671_open(&root.0,"r671-private-db2.sqlite",false).unwrap();
  assert_eq!(r756_cleanup(&root.0,&s,&d,&v),Ok((1,0)));
 }
 #[test]fn r762_duplicate_frame_does_not_duplicate_physical_rows(){
  let (root,s,d,v)=seed();
  assert_eq!(r756_replay(&root.0,&s,&d,&v,1),Ok(1));
  assert_eq!(r756_replay(&root.0,&s,&d,&v,1),Ok(0));
  assert_eq!(count(&d,"events"),1);
 }
 #[test]fn r763_three_frames_in_batches_of_one(){
  let (root,s,d,v)=seed();
  r756_append(&root.0,&s,&d,&v,&update(&v,2)).unwrap();
  r756_append(&root.0,&s,&d,&v,&update(&v,3)).unwrap();
  assert_eq!(r756_replay(&root.0,&s,&d,&v,1),Ok(1));
  assert_eq!(r756_replay(&root.0,&s,&d,&v,1),Ok(1));
  assert_eq!(r756_replay(&root.0,&s,&d,&v,1),Ok(1));
  assert_eq!(r756_cleanup(&root.0,&s,&d,&v),Ok((3,0)));
  assert_eq!(count(&s,"mirror_outbox"),0);
 }
 #[test]fn r764_source_state_rebuild_required_blocks_append(){
  let (root,s,d,v)=seed();
  exec(&s,"UPDATE mirror_control SET state='REBUILD_REQUIRED'").unwrap();
  assert_eq!(r756_append(&root.0,&s,&d,&v,&update(&v,2)),
   Err(EventOutboxR366Error::Conflict));
  assert_eq!(count(&s,"mirror_outbox"),1);
 }
 #[test]fn r765_rebaseline_generation_mismatch_denies_replay(){
  let (root,s,d,v)=seed();
  exec(&s,"UPDATE mirror_control SET feed_generation='rebase-forged'").unwrap();
  assert!(r756_replay(&root.0,&s,&d,&v,1).is_err());
  assert_eq!(count(&d,"events"),0);
 }
 #[test]fn r766_modified_db2_original_schema_rejects_ack(){
  let (root,s,d,v)=seed();
  r756_replay(&root.0,&s,&d,&v,1).unwrap();
  exec(&d,"CREATE INDEX r766_forged ON metadata(value)").unwrap();
  assert!(r756_cleanup(&root.0,&s,&d,&v).is_err());
  assert_eq!(count(&s,"mirror_outbox"),1);
 }
 #[test]fn r767_unlogged_db2_row_change_rejects_cleanup(){
  let (root,s,d,v)=seed();
  r756_replay(&root.0,&s,&d,&v,1).unwrap();
  exec(&d,"UPDATE metadata SET value='UNRECORDED'").unwrap();
  assert_eq!(r756_cleanup(&root.0,&s,&d,&v),
   Err(EventOutboxR366Error::Corrupt));
  assert_eq!(count(&s,"mirror_outbox"),1);
 }
 #[test]fn r768_uncommitted_db1_write_and_feed_rollback_together(){
  let (root,s,d,v)=setup();
  {
   let _tx=Tx::begin(&s).unwrap();
   r586_apply_original_operation(&s,&v,&frame(&v,"metadata","insert")).unwrap();
   assert_eq!(count(&s,"metadata"),1);
  }
  assert_eq!(count(&s,"metadata"),0);
  assert_eq!(count(&s,"mirror_outbox"),0);
  assert!(r756_snapshot(&root.0,&s,&d).is_ok());
 }
 #[test]fn r769_forged_after_image_rejects_db1_tx_atomically(){
  let (root,s,d,v)=seed();
  let mut op=frame(&v,"metadata","update");
  op["after_hash"]=json!("0".repeat(64));
  assert_eq!(r756_append(&root.0,&s,&d,&v,&[op]),
   Err(EventOutboxR366Error::Corrupt));
  assert_eq!(count(&s,"mirror_outbox"),1);
  assert_eq!(r756_snapshot(&root.0,&s,&d).unwrap().0["latest_seq"],1);
 }
 #[test]fn r770_tampered_original_python_feed_hash_blocks_replay(){
  let (root,s,d,v)=seed();
  exec(&s,"UPDATE mirror_outbox SET payload_hash='fake'").unwrap();
  assert!(r756_replay(&root.0,&s,&d,&v,1).is_err());
  assert_eq!(count(&d,"events"),0);
 }
 #[test]fn r771_forged_retained_bytes_blocks_replay(){
  let (root,s,d,v)=seed();
  exec(&s,"UPDATE mirror_control SET retained_bytes=1").unwrap();
  assert!(r756_replay(&root.0,&s,&d,&v,1).is_err());
  assert_eq!(count(&d,"events"),0);
 }
 #[test]fn r772_applied_cursor_above_source_latest_refuses(){
  let (root,s,d,v)=seed();
  exec(&d,"UPDATE mirror_meta SET applied_seq=7").unwrap();
  assert!(r756_replay(&root.0,&s,&d,&v,1).is_err());
 }
 #[test]fn r773_cleanup_watermark_above_applied_refuses(){
  let (root,s,d,v)=seed();
  exec(&s,"UPDATE mirror_control SET cleanup_through=2").unwrap();
  assert!(r756_cleanup(&root.0,&s,&d,&v).is_err());
 }
 #[test]fn r774_original_autoincrement_and_prev_seq_across_frames(){
  let (root,s,d,v)=seed();
  for n in 2..=4 {r756_append(&root.0,&s,&d,&v,&update(&v,n)).unwrap();}
  let (_,_,rows)=r756_snapshot(&root.0,&s,&d).unwrap();
  assert_eq!(rows.len(),4);
  for (i,row) in rows.iter().enumerate(){
   assert_eq!(row["seq"],(i+1) as u64);
   assert_eq!(row["prev_seq"],i as u64);
  }
 }
 #[test]fn r775_repeated_original_insert_denied_without_source_tail(){
  let (root,s,d,v)=seed();
  let mut original=frame(&v,"metadata","insert");
  original["row"]["value"]=json!("different-value");
  original["after_hash"]=json!(r366_sha(
   &r386_python_mirror_canonical(&original["row"]).unwrap()));
  assert!(r756_append(&root.0,&s,&d,&v,&[original]).is_err());
  assert_eq!(count(&s,"mirror_outbox"),1);
 }
 #[test]fn r776_db2_unknown_phase_denies_replay(){
  let (root,s,d,v)=seed();
  exec(&d,"UPDATE mirror_meta SET phase='UNKNOWN'").unwrap();
  assert!(r756_replay(&root.0,&s,&d,&v,8).is_err());
 }
 #[test]fn r777_bootstrap_required_does_not_auto_promote_to_normal(){
  let (root,s,d,v)=seed();
  exec(&s,"UPDATE mirror_control SET state='BOOTSTRAP_REQUIRED'").unwrap();
  assert_eq!(r756_replay(&root.0,&s,&d,&v,1),
   Err(EventOutboxR366Error::Conflict));
  assert_eq!(count(&d,"events"),0);
 }
 #[test]fn r778_forged_db2_applied_hash_blocks_cleanup(){
  let (root,s,d,v)=seed();
  r756_replay(&root.0,&s,&d,&v,1).unwrap();
  exec(&d,"UPDATE mirror_meta SET applied_hash='forged'").unwrap();
  assert_eq!(r756_cleanup(&root.0,&s,&d,&v),
   Err(EventOutboxR366Error::Corrupt));
  assert_eq!(count(&s,"mirror_outbox"),1);
 }
 #[test]fn r779_source_index_drift_blocks_replay(){
  let (root,s,d,v)=seed();
  exec(&s,"CREATE INDEX r779_illicit ON metadata(value)").unwrap();
  assert!(r756_replay(&root.0,&s,&d,&v,1).is_err());
 }
 #[test]fn r780_db2_foreign_key_orphan_blocks_ack(){
  let (root,s,d,v)=seed();
  r756_replay(&root.0,&s,&d,&v,1).unwrap();
  exec(&d,"PRAGMA foreign_keys=OFF;DELETE FROM events;
   PRAGMA foreign_keys=ON;").unwrap();
  assert!(r756_cleanup(&root.0,&s,&d,&v).is_err());
  assert_eq!(count(&s,"mirror_outbox"),1);
 }
 #[test]fn r781_python_original_outbox_cleanup_exact_128_plus_two(){
  let (root,s,d,v)=seed();
  for n in 2..=130 {r756_append(&root.0,&s,&d,&v,&update(&v,n)).unwrap();}
  let mut delivered=0u64;
  while delivered<130 {
   let count=r756_replay(&root.0,&s,&d,&v,32).unwrap();
   assert!(count<=32&&count>0);
   delivered+=count;
  }
  assert_eq!(r756_cleanup(&root.0,&s,&d,&v),Ok((128,2)));
  assert_eq!(count(&s,"mirror_outbox"),2);
  assert_eq!(r756_cleanup(&root.0,&s,&d,&v),Ok((2,0)));
  assert_eq!(count(&s,"mirror_outbox"),0);
  let (control,_,_)=r756_snapshot(&root.0,&s,&d).unwrap();
  assert_eq!(control["retained_bytes"],0);
  assert_eq!(control["cleanup_through"],130);
 }
 #[test]fn r782_forged_source_inode_identity_denies_replay(){
  let (root,s,d,v)=seed();
  exec(&d,"UPDATE mirror_meta SET source_identity='forged-inode'").unwrap();
  assert!(r756_replay(&root.0,&s,&d,&v,1).is_err());
  assert_eq!(count(&d,"events"),0);
 }
 #[test]fn r783_after_db2_commit_before_cleanup_is_idempotent(){
  let (root,s,d,v)=seed();
  r756_replay(&root.0,&s,&d,&v,1).unwrap();
  drop(s);drop(d);
  let s=r671_open(&root.0,"r671-private-db1.sqlite",false).unwrap();
  let d=r671_open(&root.0,"r671-private-db2.sqlite",false).unwrap();
  assert_eq!(r756_replay(&root.0,&s,&d,&v,1),Ok(0));
  assert_eq!(r756_cleanup(&root.0,&s,&d,&v),Ok((1,0)));
  assert_eq!(count(&d,"events"),1);
 }
 #[test]fn r784_no_source_delete_when_mirror_is_behind(){
  let (root,s,d,v)=seed();
  r756_append(&root.0,&s,&d,&v,&update(&v,2)).unwrap();
  assert_eq!(r756_replay(&root.0,&s,&d,&v,1),Ok(1));
  assert_eq!(r756_cleanup(&root.0,&s,&d,&v),
   Err(EventOutboxR366Error::Conflict));
  assert_eq!(count(&s,"mirror_outbox"),2);
 }
 #[test]fn r785_python_named_tables_never_grant_live_or_release(){
  let (root,s,d,v)=seed();
  r756_replay(&root.0,&s,&d,&v,1).unwrap();
  let (c,m,rows)=r756_snapshot(&root.0,&s,&d).unwrap();
  let identity=r696_identity(&root.0).unwrap();
  let proof=r726_python_protocol_contract(&c,&m,&rows,
   r366_query_num(&s,"PRAGMA schema_version").unwrap(),4,&identity).unwrap();
  assert_eq!(proof["installed_paper_wired"],false);
  assert_eq!(proof["real_trading_authorized"],false);
  assert_eq!(proof["source_file_and_account_attested"],false);
 }
}
