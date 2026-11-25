// Astra P0-01 R805: bounded-memory ORIGINAL Python outbox delivery.
// Private fixture-only input; no installed PAPER or production DB paths.
struct R805Head {
 latest:u64,applied:u64,cleaned:u64,retained:u64
}
fn r805_window_sql(fields:&[&str],predicate:&str)->Result<String,EventOutboxR366Error>{
 let mut parts=Vec::new();
 for key in fields{
  let col=r586_identifier(key)?;
  parts.push(format!("'{key}',{col}"));
 }
 Ok(format!("SELECT json_object({}) FROM mirror_outbox {predicate}",parts.join(",")))
}
fn r805_validate_row(row:&serde_json::Value,prev:u64,latest:u64,generation:&str)
 ->Result<(u64,u64),EventOutboxR366Error>{
 r726_shape(row,R726_OUTBOX)?;
 let seq=r726_u(row,"seq")?;
 let previous=r726_u(row,"prev_seq")?;
 let txid=r726_s(row,"tx_id")?;
 let kind=r726_s(row,"tx_kind")?;
 let raw=r726_s(row,"payload_json")?;
 let bytes=r726_u(row,"payload_bytes")?;
 let expected=previous.checked_add(1).ok_or(EventOutboxR366Error::Corrupt)?;
 if previous!=prev||seq!=expected||seq>latest
  ||!txid.starts_with("tx-")||txid.len()!=35
  ||kind.is_empty()||r726_u(row,"schema_generation")?!=1
  ||r726_s(row,"feed_generation")?!=generation
  ||r726_u(row,"committed_at_ms")?==0
  ||raw.len()>4*1024*1024||bytes!=raw.len() as u64+384
  ||r366_sha(raw)!=r726_s(row,"payload_hash")?{
  return Err(EventOutboxR366Error::Corrupt)
 }
 let frame:serde_json::Value=serde_json::from_str(raw)
  .map_err(|_|EventOutboxR366Error::Corrupt)?;
 if r386_python_mirror_canonical(&frame)?!=raw
  ||frame["protocol_version"]!=1||frame["schema_generation"]!=1
  ||frame["tx_kind"]!=kind
  ||frame["operations"].as_array().map_or(true,|v|v.is_empty()){
  return Err(EventOutboxR366Error::Corrupt)
 }
 Ok((seq,bytes))
}
fn r805_read_head(
 root:&Path,src:&Db,dst:&Db
)->Result<(R805Head,serde_json::Value,serde_json::Value),EventOutboxR366Error>{
 let ctrls=r756_read_json(src,"mirror_control",R726_CONTROL,"WHERE id=1")?;
 let metas=r756_read_json(dst,"mirror_meta",R726_META,"WHERE id=1")?;
 if ctrls.len()!=1||metas.len()!=1{return Err(EventOutboxR366Error::Corrupt)}
 let c=ctrls[0].clone();
 let m=metas[0].clone();
 let original=r586_oracle();
 r566_check_source_manifest(&original)?;
 let source_schema=r756_schema_hash(src)?;
 if source_schema!=r756_schema_hash(dst)?
  ||m["schema_hash"].as_str()!=Some(source_schema.as_str())
  ||r726_u(&m,"schema_version")?!=r366_query_num(src,"PRAGMA schema_version")?
  ||r726_u(&m,"user_version")?!=4
  ||r366_query_num(src,"PRAGMA user_version")?!=4
  ||r726_s(&m,"source_identity")?!=r696_identity(root)?
  ||r726_u(&c,"schema_generation")?!=1
  ||c["state"]!="NORMAL"||m["phase"]!="SYNCED"
  ||r726_s(&c,"feed_generation")?.is_empty()
  ||c["feed_generation"]!=m["feed_generation"]
  ||c["baseline_seq"]!=m["bootstrap_seq"]
  ||c["baseline_seq"]!=m["baseline_seq"]{
  return Err(EventOutboxR366Error::Corrupt)
 }
 let latest=r726_u(&c,"latest_seq")?;
 let applied=r726_u(&m,"applied_seq")?;
 let cleaned=r726_u(&c,"cleanup_through")?;
 if cleaned>applied||applied>latest||r726_u(&c,"baseline_seq")?>latest{
  return Err(EventOutboxR366Error::Corrupt)
 }
 let mut q=Stmt::new(src,"SELECT COUNT(*),COALESCE(SUM(payload_bytes),0)
  FROM mirror_outbox").map_err(r366_error)?;
 if q.step().map_err(r366_error)?!=SQLITE_ROW{return Err(EventOutboxR366Error::Corrupt)}
 let count=q.num(0).map_err(r366_error)?;
 let bytes=q.num(1).map_err(r366_error)?;
 if count>1_000_000||r726_u(&c,"retained_records")?!=count
  ||r726_u(&c,"retained_bytes")?!=bytes{
  return Err(EventOutboxR366Error::Corrupt)
 }
 Ok((R805Head{latest,applied,cleaned,retained:count},c,m))
}

// Full outbox chain checked one row at a time, including the older portion
// that a replay window will skip. No retention-size Vec allocation.
fn r805_verify_chain(src:&Db,h:&R805Head,generation:&str)
 ->Result<(),EventOutboxR366Error>{
 let sql=r805_window_sql(R726_OUTBOX,"ORDER BY seq")?;
 let mut statement=Stmt::new(src,&sql).map_err(r366_error)?;
 let mut previous=h.cleaned;
 let mut count=0u64;
 let mut bytes=0u64;
 loop{
  match statement.step().map_err(r366_error)?{
   SQLITE_DONE=>break,
   SQLITE_ROW=>{
    count=count.checked_add(1).ok_or(EventOutboxR366Error::Pressure)?;
    if count>1_000_000{return Err(EventOutboxR366Error::Pressure)}
    let raw=statement.strval(0).map_err(r366_error)?;
    let row:serde_json::Value=serde_json::from_str(&raw)
     .map_err(|_|EventOutboxR366Error::Corrupt)?;
    let (seq,size)=r805_validate_row(&row,previous,h.latest,generation)?;
    previous=seq;
    bytes=bytes.checked_add(size).ok_or(EventOutboxR366Error::Pressure)?;
   },
   _=>return Err(EventOutboxR366Error::Corrupt)
  }
 }
 if count!=h.retained||(count==0&&h.cleaned!=h.latest)
  ||(count>0&&previous!=h.latest)
  ||bytes>1024*1024*1024{
  return Err(EventOutboxR366Error::Corrupt)
 }
 Ok(())
}
struct R805SourceRead<'a>(&'a Db);
impl Drop for R805SourceRead<'_>{
 fn drop(&mut self){let _=exec(self.0,"ROLLBACK");}
}
fn r805_replay_window(root:&Path,src:&Db,dst:&Db,manifest:&serde_json::Value,
 limit:usize)->Result<u64,EventOutboxR366Error>{
 if !(1..=32).contains(&limit){return Err(EventOutboxR366Error::Pressure)}
 exec(src,"BEGIN DEFERRED").map_err(r366_error)?;
 let _source_snapshot=R805SourceRead(src);
 let (h,c,_m)=r805_read_head(root,src,dst)?;
 r805_verify_chain(src,&h,r726_s(&c,"feed_generation")?)?;
 let tx=Tx::begin(dst).map_err(r366_error)?;
 let predicate=format!("WHERE seq>{} ORDER BY seq LIMIT {limit}",h.applied);
 let sql=r805_window_sql(R726_OUTBOX,&predicate)?;
 let mut q=Stmt::new(src,&sql).map_err(r366_error)?;
 let mut applied=h.applied;
 let mut done=0u64;
 loop{
  match q.step().map_err(r366_error)?{
   SQLITE_DONE=>break,
   SQLITE_ROW=>{
    let raw=q.strval(0).map_err(r366_error)?;
    let row:serde_json::Value=serde_json::from_str(&raw)
     .map_err(|_|EventOutboxR366Error::Corrupt)?;
    let (seq,_)=r805_validate_row(&row,applied,h.latest,
      r726_s(&c,"feed_generation")?)?;
    let frame:serde_json::Value=serde_json::from_str(r726_s(&row,"payload_json")?)
     .map_err(|_|EventOutboxR366Error::Corrupt)?;
    for op in frame["operations"].as_array().ok_or(EventOutboxR366Error::Corrupt)?{
     r586_apply_original_operation(dst,manifest,op)?;
    }
    applied=seq;
    done+=1;
    exec(dst,&format!("UPDATE mirror_meta SET applied_seq={seq},
      applied_hash={},last_success_ms={} WHERE id=1",
       r586_sql_literal(&row["payload_hash"])?,
       3_000_000+seq)).map_err(r366_error)?;
   },
   _=>return Err(EventOutboxR366Error::Corrupt)
  }
 }
 drop(q);
 tx.commit().map_err(r366_error)?;
 Ok(done)
}

fn r805_cleanup_window(root:&Path,src:&Db,dst:&Db,manifest:&serde_json::Value)
 ->Result<(u64,u64),EventOutboxR366Error>{
 let tx=Tx::begin(src).map_err(r366_error)?;
 let dst_tx=Tx::begin(dst).map_err(r366_error)?;
 let (h,c,m)=r805_read_head(root,src,dst)?;
 if h.latest!=h.applied{return Err(EventOutboxR366Error::Conflict)}
 r805_verify_chain(src,&h,r726_s(&c,"feed_generation")?)?;
 if r366_query_num(dst,"SELECT COUNT(*) FROM pragma_foreign_key_check")?!=0{
  return Err(EventOutboxR366Error::Corrupt)
 }
 r800_streaming_original_parity(src,dst,manifest)?;
 let sql=r805_window_sql(R726_OUTBOX,"ORDER BY seq DESC LIMIT 1")?;
 let mut last=Stmt::new(src,&sql).map_err(r366_error)?;
 match last.step().map_err(r366_error)?{
  SQLITE_ROW=>{
   let v:serde_json::Value=serde_json::from_str(
    &last.strval(0).map_err(r366_error)?)
     .map_err(|_|EventOutboxR366Error::Corrupt)?;
   if m["applied_hash"]!=v["payload_hash"]{
    return Err(EventOutboxR366Error::Corrupt)
   }
  },
  SQLITE_DONE if h.retained==0=>{},
  _=>return Err(EventOutboxR366Error::Corrupt)
 }
 drop(last);
 let sql=r805_window_sql(R726_OUTBOX,"ORDER BY seq LIMIT 128")?;
 let mut q=Stmt::new(src,&sql).map_err(r366_error)?;
 let mut deleted=0u64;
 let mut end=h.cleaned;
 let mut bytes=0u64;
 loop{
  match q.step().map_err(r366_error)?{
   SQLITE_DONE=>break,
   SQLITE_ROW=>{
    let row:serde_json::Value=serde_json::from_str(
      &q.strval(0).map_err(r366_error)?)
      .map_err(|_|EventOutboxR366Error::Corrupt)?;
    let (seq,size)=r805_validate_row(&row,end,h.latest,
      r726_s(&c,"feed_generation")?)?;
    end=seq;
    deleted+=1;
    bytes=bytes.checked_add(size).ok_or(EventOutboxR366Error::Pressure)?;
   },
   _=>return Err(EventOutboxR366Error::Corrupt)
  }
 }
 drop(q);
 if deleted>0{
  exec(src,&format!("DELETE FROM mirror_outbox WHERE seq<={end};
   UPDATE mirror_control SET cleanup_through={end},
    retained_records=retained_records-{deleted},
    retained_bytes=retained_bytes-{bytes} WHERE id=1"))
    .map_err(r366_error)?;
 }
 dst_tx.commit().map_err(r366_error)?;
 tx.commit().map_err(r366_error)?;
 Ok((deleted,h.retained-deleted))
}

// Public only to a STANDALONE, path-guarded offline mirror worker.
// This entrypoint cannot open paths outside a private astra_r671_file_*
// directory under /tmp, and never touches installed PAPER or REAL.
pub fn r805_private_service_once(root:&Path)
 ->Result<(u64,u64,u64),String>{
 let src=r671_open(root,"r671-private-db1.sqlite",false)
  .map_err(|e|format!("private DB1: {e:?}"))?;
 let dst=r671_open(root,"r671-private-db2.sqlite",false)
  .map_err(|e|format!("private DB2: {e:?}"))?;
 let v=r586_oracle();
 let replayed=r805_replay_window(root,&src,&dst,&v,32)
  .map_err(|e|format!("private replay: {e:?}"))?;
 let (h,_,_)=r805_read_head(root,&src,&dst)
  .map_err(|e|format!("private head: {e:?}"))?;
 let (deleted,remaining)=if h.applied==h.latest{
  r805_cleanup_window(root,&src,&dst,&v)
   .map_err(|e|format!("private ACK: {e:?}"))?
 }else{(0,h.retained)};
 Ok((replayed,deleted,remaining))
}
// Standalone process self-test: fresh private databases, ACTUAL Python
// writer, file-backed Rust receiver, independent --once reopen cycles.
// This is not a production bootstrap or installation.
pub fn r805_private_service_selftest()->Result<(),String>{
 use std::os::unix::fs::DirBuilderExt;
 use std::process::Command;
 struct Temp(PathBuf);
 impl Drop for Temp{
  fn drop(&mut self){let _=fs::remove_dir_all(&self.0);}
 }
 let mut root=std::env::temp_dir();
 let stamp=std::time::SystemTime::now()
  .duration_since(std::time::UNIX_EPOCH)
  .map_err(|e|format!("clock: {e}"))?.as_nanos();
 root.push(format!("astra_r671_file_{}_{}_r809",std::process::id(),stamp));
 fs::DirBuilder::new().mode(0o700).create(&root)
  .map_err(|e|format!("private directory: {e}"))?;
 let _cleanup=Temp(root.clone());
 {
  let pair=r756_initialize(&root)
   .map_err(|e|format!("private source setup: {e:?}"))?;
  drop(pair);
 }
 let script=Path::new(env!("CARGO_MANIFEST_DIR"))
  .join("offline_original_python_producer_r786.py");
 let run=|mode:&str|->Result<(),String>{
  let output=Command::new("/usr/bin/python3")
   .arg(&script).arg(&root).arg(mode).output()
   .map_err(|e|format!("Python worker: {e}"))?;
  if !output.status.success(){
   return Err(format!("original Python writer failed: {mode}"))
  }
  Ok(())
 };
 run("insert")?;
 if r805_private_service_once(&root)?!=(1,1,0){
  return Err("isolated first ACK mismatch".into())
 }
 run("batch160")?;
 let mut delivered=0;
 let mut acknowledged=0;
 let mut remaining=160;
 for _ in 0..7{
  let (n,a,r)=r805_private_service_once(&root)?;
  delivered+=n;acknowledged+=a;remaining=r;
  if remaining==0{break}
 }
 if (delivered,acknowledged,remaining)!=(160,160,0){
  return Err("private multi-window ACK mismatch".into())
 }
 Ok(())
}
#[cfg(test)]
mod r805_tests {
 use super::*;
 use std::os::unix::fs::DirBuilderExt;
 use std::process::Command;
 use serde_json::Value;
 struct Private(PathBuf);
 impl Private {
  fn new()->Self {
   let mut dir=std::env::temp_dir();
   let n=std::time::SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
   dir.push(format!("astra_r671_file_{}_{}_r805",std::process::id(),n));
   fs::DirBuilder::new().mode(0o700).create(&dir).unwrap();
   Self(dir)
  }
 }
 impl Drop for Private{fn drop(&mut self){let _=fs::remove_dir_all(&self.0);}}
 fn python(root:&Path,mode:&str) {
  let script=Path::new(env!("CARGO_MANIFEST_DIR"))
   .join("offline_original_python_producer_r786.py");
  let result=Command::new("/usr/bin/python3")
   .arg(script).arg(root).arg(mode).output().unwrap();
  assert!(result.status.success(),"real isolated Python producer failed");
 }
 fn init()->(Private,Db,Db,Value){
  let root=Private::new();
  let (src,dst,v)=r756_initialize(&root.0).unwrap();
  python(&root.0,"insert");
  r805_replay_window(&root.0,&src,&dst,&v,32).unwrap();
  assert_eq!(r805_cleanup_window(&root.0,&src,&dst,&v),Ok((1,0)));
  (root,src,dst,v)
 }
 fn load(root:&Path) {
  for _ in 0..4{python(root,"batch160");}
 }
 #[test]fn r805_original_python_backlog_640_no_512_ceiling(){
  let (root,src,dst,v)=init();
  load(&root.0);
  assert_eq!(r756_replay(&root.0,&src,&dst,&v,32),
   Err(EventOutboxR366Error::Pressure));
  let mut n=0;
  for _ in 0..21{
   let count=r805_replay_window(&root.0,&src,&dst,&v,32).unwrap();
   assert!(count<=32);
   n+=count;
   if count==0{break}
  }
  assert_eq!(n,640);
  let mut cleaned=0;
  for _ in 0..6{
   let (count,left)=r805_cleanup_window(&root.0,&src,&dst,&v).unwrap();
   cleaned+=count;
   if left==0{break}
  }
  assert_eq!(cleaned,640);
  let (h,_,_)=r805_read_head(&root.0,&src,&dst).unwrap();
  assert_eq!((h.latest,h.applied,h.cleaned,h.retained),(641,641,641,0));
  assert_eq!(r800_streaming_original_parity(&src,&dst,&v),Ok(1));
 }
 #[test]fn r1050_forged_first_unacked_seq_hole_refuses_replay(){
  let (root,src,dst,v)=init();
  python(&root.0,"update"); // original seq=2, prev_seq=1
  exec(&src,"UPDATE mirror_outbox SET seq=3 WHERE seq=2;
    UPDATE mirror_control SET latest_seq=3 WHERE id=1").unwrap();
  // Retained count and byte totals still match. Only strict adjacency
  // catches an outbox gap with a forged final source cursor.
  assert_eq!(r805_replay_window(&root.0,&src,&dst,&v,1),
   Err(EventOutboxR366Error::Corrupt));
  assert_eq!(r366_query_num(&dst,"SELECT applied_seq FROM mirror_meta"),Ok(1));
  assert_eq!(r366_query_num(&src,"SELECT cleanup_through FROM mirror_control"),Ok(1));
 }
 #[test]fn r1051_forged_last_seq_hole_outside_replay_window_denied(){
  let (root,src,dst,v)=init();
  python(&root.0,"batch160"); // source original seq=2..161
  exec(&src,"UPDATE mirror_outbox SET seq=162 WHERE seq=161;
    UPDATE mirror_control SET latest_seq=162 WHERE id=1").unwrap();
  // Complete retained-chain scan rejects tampered tail BEFORE
  // committing any of first 32 destination envelopes.
  assert_eq!(r805_replay_window(&root.0,&src,&dst,&v,32),
   Err(EventOutboxR366Error::Corrupt));
  assert_eq!(r366_query_num(&dst,"SELECT applied_seq FROM mirror_meta"),Ok(1));
 }
 #[test]fn r806_corrupt_tail_outside_first_window_blocks_all_replay(){
  let (root,src,dst,v)=init();
  load(&root.0);
  exec(&src,"UPDATE mirror_outbox SET payload_hash='forged' WHERE seq=580").unwrap();
  assert_eq!(r805_replay_window(&root.0,&src,&dst,&v,32),
   Err(EventOutboxR366Error::Corrupt));
  assert_eq!(r366_query_num(&dst,"SELECT applied_seq FROM mirror_meta").unwrap(),1);
 }
 #[test]fn r807_source_missing_outbox_row_fails_closed(){
  let (root,src,dst,v)=init();
  python(&root.0,"batch160");
  exec(&src,"DELETE FROM mirror_outbox WHERE seq=85").unwrap();
  assert!(r805_replay_window(&root.0,&src,&dst,&v,32).is_err());
  assert_eq!(r366_query_num(&dst,"SELECT applied_seq FROM mirror_meta").unwrap(),1);
 }
 #[test]fn r808_additional_source_frame_after_partial_window_recovers(){
  let (root,src,dst,v)=init();
  python(&root.0,"batch160");
  assert_eq!(r805_replay_window(&root.0,&src,&dst,&v,32),Ok(32));
  python(&root.0,"batch160");
  let mut count=32;
  for _ in 0..12 {
   let n=r805_replay_window(&root.0,&src,&dst,&v,32).unwrap();
   count+=n;
   if n==0{break}
  }
  assert_eq!(count,320);
  let mut removed=0;
  for _ in 0..4 {
   let (n,left)=r805_cleanup_window(&root.0,&src,&dst,&v).unwrap();
   removed+=n;
   if left==0{break}
  }
  assert_eq!(removed,320);
 }
 #[test]fn r809_standalone_entrypoint_reopens_and_drains_private_files(){
  let (root,src,dst,_v)=init();
  python(&root.0,"batch160");
  drop(src);drop(dst);
  let mut delivered=0;
  let mut acknowledged=0;
  for _ in 0..7{
   let (n,a,remaining)=r805_private_service_once(&root.0).unwrap();
   delivered+=n;
   acknowledged+=a;
   if remaining==0{break}
  }
  assert_eq!((delivered,acknowledged),(160,160));
  let (src,dst)=(r671_open(&root.0,"r671-private-db1.sqlite",false).unwrap(),
   r671_open(&root.0,"r671-private-db2.sqlite",false).unwrap());
  let (h,_,_)=r805_read_head(&root.0,&src,&dst).unwrap();
  assert_eq!((h.latest,h.applied,h.cleaned,h.retained),(161,161,161,0));
 }
 #[test]fn r810_standalone_entrypoint_rejects_non_private_root(){
  assert!(r805_private_service_once(Path::new("/home/tmpadmin/spotbot/data")).is_err());
 }
}
