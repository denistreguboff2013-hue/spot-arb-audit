// Astra P0-01 R845: verify full original outbox snapshot ONCE, then
// atomically apply <=128 frames as one destination SQLite transaction.
// Source stays BEGIN DEFERRED until destination commit. Only R671 private
// original 17-table files. Full R805 chain and R800 parity/ACK unchanged.
fn r845_verified_burst(
 root:&Path,src:&Db,dst:&Db,manifest:&serde_json::Value,limit:usize
)->Result<u64,EventOutboxR366Error>{
 if !(1..=128).contains(&limit){return Err(EventOutboxR366Error::Pressure)}
 exec(src,"BEGIN DEFERRED").map_err(r366_error)?;
 let _source_snapshot=R805SourceRead(src);
 let (head,control,_meta)=r805_read_head(root,src,dst)?;
 let generation=r726_s(&control,"feed_generation")?;
 r805_verify_chain(src,&head,generation)?;
 let predicate=format!("WHERE seq>{} ORDER BY seq LIMIT {limit}",head.applied);
 let sql=r805_window_sql(R726_OUTBOX,&predicate)?;
 let mut q=Stmt::new(src,&sql).map_err(r366_error)?;
 let tx=Tx::begin(dst).map_err(r366_error)?;
 let mut applied=head.applied;
 let mut final_hash=None::<String>;
 let mut total=0u64;
 loop{
  match q.step().map_err(r366_error)?{
   SQLITE_DONE=>break,
   SQLITE_ROW=>{
    let raw=q.strval(0).map_err(r366_error)?;
    let row:serde_json::Value=serde_json::from_str(&raw)
     .map_err(|_|EventOutboxR366Error::Corrupt)?;
    let (seq,_bytes)=r805_validate_row(&row,applied,head.latest,generation)?;
    if seq!=applied.checked_add(1).ok_or(EventOutboxR366Error::Corrupt)?{
     return Err(EventOutboxR366Error::Corrupt)
    }
    let frame:serde_json::Value=serde_json::from_str(
     r726_s(&row,"payload_json")?)
     .map_err(|_|EventOutboxR366Error::Corrupt)?;
    for op in frame["operations"].as_array().ok_or(EventOutboxR366Error::Corrupt)?{
     r586_apply_original_operation(dst,manifest,op)?;
    }
    applied=seq;
    total+=1;
    final_hash=Some(r726_s(&row,"payload_hash")?.to_owned());
   },
   _=>return Err(EventOutboxR366Error::Corrupt)
  }
 }
 drop(q);
 if let Some(hash)=final_hash{
  exec(dst,&format!("UPDATE mirror_meta SET applied_seq={applied},
   applied_hash={},last_success_ms={} WHERE id=1",
   r586_sql_literal(&serde_json::json!(hash))?,
   3_000_000+applied)).map_err(r366_error)?;
 }
 tx.commit().map_err(r366_error)?;
 Ok(total)
}

// R1090: R951 prepared-event binding promoted from test-only scope for
// strictly opt-in PRIVATE PAPER event replay. R931 remains test-only.
include!("native_r951_prepared_event_replay_review.rs");
include!("native_r1090_prepared_event_burst_review.rs");
include!("native_r1132_fused_paper_replay_review.rs");
include!("native_r1134_parallel_helper_review.rs");
include!("native_r1092_dynamic_incremental_paper_ack_review.rs");
#[cfg(test)] include!("native_r931_fused_paper_replay_review.rs");

#[cfg(test)]
mod r845_tests {
 use super::*;
 use std::os::unix::fs::DirBuilderExt;
 use std::process::Command;
 struct Temp(PathBuf);
 impl Temp{
  fn new()->Self{
   let mut path=std::env::temp_dir();
   let stamp=std::time::SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
   path.push(format!("astra_r671_file_{}_{}_r845",std::process::id(),stamp));
   fs::DirBuilder::new().mode(0o700).create(&path).unwrap();
   Self(path)
  }
 }
 impl Drop for Temp {fn drop(&mut self){let _=fs::remove_dir_all(&self.0);}}
 fn python(root:&Path,mode:&str) {
  let script=Path::new(env!("CARGO_MANIFEST_DIR"))
   .join("offline_original_python_producer_r786.py");
  let output=Command::new("/usr/bin/python3").arg(script)
   .arg(root).arg(mode).output().unwrap();
  assert!(output.status.success(),"isolated original Python writer failed");
 }
 fn pair()->(Temp,Db,Db,serde_json::Value) {
  let root=Temp::new();
  let (src,dst,v)=r756_initialize(&root.0).unwrap();
  python(&root.0,"insert");
  assert_eq!(r805_replay_window(&root.0,&src,&dst,&v,32),Ok(1));
  assert_eq!(r805_cleanup_window(&root.0,&src,&dst,&v),Ok((1,0)));
  (root,src,dst,v)
 }
 #[test]fn r845_160_real_python_updates_in_two_verified_db2_transactions(){
  let (root,src,dst,v)=pair();
  python(&root.0,"batch160");
  assert_eq!(r845_verified_burst(&root.0,&src,&dst,&v,128),Ok(128));
  assert_eq!(r366_query_num(&dst,"SELECT applied_seq FROM mirror_meta"),Ok(129));
  assert_eq!(r845_verified_burst(&root.0,&src,&dst,&v,128),Ok(32));
  assert_eq!(r845_verified_burst(&root.0,&src,&dst,&v,128),Ok(0));
  assert_eq!(r805_cleanup_window(&root.0,&src,&dst,&v),Ok((128,32)));
  assert_eq!(r805_cleanup_window(&root.0,&src,&dst,&v),Ok((32,0)));
  assert_eq!(r800_streaming_original_parity(&src,&dst,&v),Ok(1));
  assert_eq!(r366_query_num(&src,"SELECT COUNT(*) FROM mirror_outbox"),Ok(0));
  assert_eq!(r366_query_num(&dst,"SELECT applied_seq FROM mirror_meta"),Ok(161));
 }
 #[test]fn r846_corrupt_outbox_tail_after_limit_denies_any_db2_write(){
  let (root,src,dst,v)=pair();
  python(&root.0,"batch160");
  exec(&src,"UPDATE mirror_outbox SET payload_hash='forged' WHERE seq=150").unwrap();
  assert_eq!(r845_verified_burst(&root.0,&src,&dst,&v,128),
   Err(EventOutboxR366Error::Corrupt));
  assert_eq!(r366_query_num(&dst,"SELECT applied_seq FROM mirror_meta"),Ok(1));
 }
 #[test]fn r847_sink_insert_conflict_rolls_back_entire_verified_burst(){
  let (root,src,dst,v)=pair();
  let updated=r659_op(&v,"metadata",&serde_json::json!({
   "key":"rolled_up_raw_events","value":"r847_source_update"}),"update").unwrap();
  let inserted=r659_op(&v,"metadata",&serde_json::json!({
   "key":"r847_conflict","value":"src"}),"insert").unwrap();
  assert_eq!(r830_incremental_original_append(&root.0,&src,&dst,&v,&[updated]),Ok(2));
  assert_eq!(r830_incremental_original_append(&root.0,&src,&dst,&v,&[inserted]),Ok(3));
  exec(&dst,"INSERT INTO metadata VALUES('r847_conflict','dest')").unwrap();
  assert!(r845_verified_burst(&root.0,&src,&dst,&v,128).is_err());
  assert_eq!(r366_query_num(&dst,"SELECT applied_seq FROM mirror_meta"),Ok(1));
  let mut stmt=Stmt::new(&dst,
   "SELECT value FROM metadata WHERE key='rolled_up_raw_events'").unwrap();
  assert_eq!(stmt.step().unwrap(),SQLITE_ROW);
  assert_eq!(stmt.strval(0).unwrap(),"from_original_python");
 }
 #[test]fn r848_invalid_burst_limit_refuses_any_change(){
  let (root,src,dst,v)=pair();
  assert_eq!(r845_verified_burst(&root.0,&src,&dst,&v,129),
   Err(EventOutboxR366Error::Pressure));
  assert_eq!(r366_query_num(&dst,"SELECT applied_seq FROM mirror_meta"),Ok(1));
 }
}
