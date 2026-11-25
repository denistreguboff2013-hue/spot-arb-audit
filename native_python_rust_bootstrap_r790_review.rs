// Astra P0-01 R790+: R786 real Python writer -> consistent DB1 WAL
// snapshot -> Rust 17-table copy -> after-snapshot Python outbox replay.
// Pre-created private test DB2 schema only. This is NOT deployed bootstrap.
fn r790_arm_private_baseline(root:&Path,src:&Db,dst:&Db)
 ->Result<(),EventOutboxR366Error>{
 let (c,m,_)=r756_snapshot(root,src,dst)?;
 if c["state"]!="NORMAL"||m["phase"]!="SYNCED"
  ||r726_u(&c,"latest_seq")?!=1||r726_u(&m,"applied_seq")?!=0{
  return Err(EventOutboxR366Error::Conflict)
 }
 exec(src,"UPDATE mirror_control SET baseline_seq=1 WHERE id=1")
  .map_err(r366_error)?;
 exec(dst,"UPDATE mirror_meta SET phase='BOOTSTRAP',bootstrap_seq=1,
  applied_seq=1,baseline_seq=1,table_index=0,last_key_json=NULL WHERE id=1")
  .map_err(r366_error)?;
 Ok(())
}
fn r790_copy_wal_snapshot<F:FnOnce()>(
 root:&Path,src:&Db,dst:&Db,v:&serde_json::Value,
 after_read_pin:F,fail_before_commit:bool
)->Result<(),EventOutboxR366Error>{
 let (c,m,_)=r756_snapshot(root,src,dst)?;
 if m["phase"]!="BOOTSTRAP"||r726_u(&m,"bootstrap_seq")?!=1
  ||r726_u(&c,"baseline_seq")?!=1||m["feed_generation"]!=c["feed_generation"]{
  return Err(EventOutboxR366Error::Conflict)
 }
 // A DEFERRED source read transaction pins an immutable WAL snapshot,
 // permitting the actual Python writer to commit another source tx.
 exec(src,"BEGIN DEFERRED").map_err(r366_error)?;
 let outcome=(||{
  let pinned=r366_query_num(src,"SELECT latest_seq FROM mirror_control WHERE id=1")?;
  if pinned!=1{return Err(EventOutboxR366Error::Conflict)}
  after_read_pin();
  let tx=Tx::begin(dst).map_err(r366_error)?;
  for entry in v["table_schema"].as_array().ok_or(EventOutboxR366Error::Schema)?{
   let table=entry["table"].as_str().ok_or(EventOutboxR366Error::Schema)?;
   r804_stream_original_table(src,dst,v,table)?;
  }
  if fail_before_commit{return Err(EventOutboxR366Error::Interrupted)}
  exec(dst,"UPDATE mirror_meta SET phase='REPLAY',table_index=17,
   last_key_json=NULL WHERE id=1").map_err(r366_error)?;
  tx.commit().map_err(r366_error)?;
  Ok(())
 })();
 let _=exec(src,"ROLLBACK");
 outcome
}
fn r790_replay_cutover(root:&Path,src:&Db,dst:&Db,v:&serde_json::Value)
 ->Result<u64,EventOutboxR366Error>{
 let (c,m,rows)=r756_snapshot(root,src,dst)?;
 if m["phase"]!="REPLAY"||r726_u(&m,"bootstrap_seq")?!=1
   ||r726_u(&c,"baseline_seq")?!=1||c["state"]!="NORMAL"{
  return Err(EventOutboxR366Error::Conflict)
 }
 if r756_schema_hash(dst)?!=r756_schema_hash(src)?{
  return Err(EventOutboxR366Error::Schema)
 }
 let mut applied=r726_u(&m,"applied_seq")?;
 let starting_applied=applied;
 let mut advanced=0u64;
 let tx=Tx::begin(dst).map_err(r366_error)?;
 for row in rows.iter().filter(|r|r["seq"].as_u64().unwrap_or(0)>starting_applied){
  let seq=r726_u(row,"seq")?;
  if seq!=applied+1||r726_u(row,"prev_seq")?!=applied{
   return Err(EventOutboxR366Error::Corrupt)
  }
  let f:serde_json::Value=serde_json::from_str(r726_s(row,"payload_json")?)
   .map_err(|_|EventOutboxR366Error::Corrupt)?;
  let ops=f["operations"].as_array().ok_or(EventOutboxR366Error::Corrupt)?;
  for op in ops{r586_apply_original_operation(dst,v,op)?;}
  applied=seq;advanced+=1;
  exec(dst,&format!("UPDATE mirror_meta SET applied_seq={seq},applied_hash={}
    WHERE id=1",r586_sql_literal(&row["payload_hash"])?))
   .map_err(r366_error)?;
 }
 if applied!=r726_u(&c,"latest_seq")?{
  return Err(EventOutboxR366Error::Conflict)
 }
 r800_streaming_original_parity(src,dst,v)?;
 if advanced==0 {
  let baseline=rows.iter().find(|r|r["seq"].as_u64()==Some(applied))
   .ok_or(EventOutboxR366Error::Corrupt)?;
  exec(dst,&format!("UPDATE mirror_meta SET applied_hash={} WHERE id=1",
   r586_sql_literal(&baseline["payload_hash"])?)).map_err(r366_error)?;
 }
 exec(dst,"UPDATE mirror_meta SET phase='SYNCED' WHERE id=1")
  .map_err(r366_error)?;
 tx.commit().map_err(r366_error)?;
 Ok(advanced)
}

#[cfg(test)]
mod r790_tests {
 use super::*;
 use std::os::unix::fs::DirBuilderExt;
 use std::process::Command;
 use serde_json::Value;
 struct PrivateRoot(PathBuf);
 impl PrivateRoot{
  fn new()->Self{
   let mut p=std::env::temp_dir();
   let stamp=std::time::SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
   p.push(format!("astra_r671_file_{}_{}_bootstrap",std::process::id(),stamp));
   fs::DirBuilder::new().mode(0o700).create(&p).unwrap();Self(p)
  }
 }
 impl Drop for PrivateRoot{
  fn drop(&mut self){let _=fs::remove_dir_all(&self.0);}
 }
 fn python(root:&Path,mode:&str){
  let p=Path::new(env!("CARGO_MANIFEST_DIR"))
   .join("offline_original_python_producer_r786.py");
  let out=Command::new("/usr/bin/python3").arg(p).arg(root)
   .arg(mode).output().unwrap();
  assert!(out.status.success(),"original Python test source transaction failed");
 }
 fn pair()->(PrivateRoot,Db,Db,Value){
  let root=PrivateRoot::new();
  let (s,d,v)=r756_initialize(&root.0).unwrap();
  python(&root.0,"insert");
  r790_arm_private_baseline(&root.0,&s,&d).unwrap();
  (root,s,d,v)
 }
 fn scalar(db:&Db,sql:&str)->String{
  let mut q=Stmt::new(db,sql).unwrap();
  assert_eq!(q.step().unwrap(),SQLITE_ROW);
  q.strval(0).unwrap()
 }
 fn count(db:&Db,table:&str)->u64{
  r366_query_num(db,&format!("SELECT count(*) FROM {table}")).unwrap()
 }
 #[test]fn r790_real_python_write_during_wal_snapshot_replayed_to_synced(){
  let (root,s,d,v)=pair();
  r790_copy_wal_snapshot(&root.0,&s,&d,&v,
   ||python(&root.0,"update"),false).unwrap();
  assert_eq!(scalar(&d,"SELECT value FROM metadata WHERE key='rolled_up_raw_events'"),
   "from_original_python");
  assert_eq!(scalar(&s,"SELECT value FROM metadata WHERE key='rolled_up_raw_events'"),
   "updated_by_original_python");
  assert_eq!(scalar(&d,"SELECT phase FROM mirror_meta WHERE id=1"),"REPLAY");
  assert_eq!(r790_replay_cutover(&root.0,&s,&d,&v),Ok(1));
  assert_eq!(scalar(&d,"SELECT phase FROM mirror_meta WHERE id=1"),"SYNCED");
  assert_eq!(r756_cleanup(&root.0,&s,&d,&v),Ok((2,0)));
  assert_eq!(count(&s,"mirror_outbox"),0);
  assert_eq!(scalar(&d,"SELECT value FROM metadata"),"updated_by_original_python");
 }
 #[test]fn r791_failed_bootstrap_rolls_back_every_destination_row(){
  let (root,s,d,v)=pair();
  assert_eq!(r790_copy_wal_snapshot(&root.0,&s,&d,&v,||{},true),
    Err(EventOutboxR366Error::Interrupted));
  assert_eq!(count(&d,"metadata"),0);
  assert_eq!(scalar(&d,"SELECT phase FROM mirror_meta"),"BOOTSTRAP");
  assert_eq!(count(&s,"mirror_outbox"),1);
  r790_copy_wal_snapshot(&root.0,&s,&d,&v,||{},false).unwrap();
  assert_eq!(r790_replay_cutover(&root.0,&s,&d,&v),Ok(0));
  assert_eq!(r756_cleanup(&root.0,&s,&d,&v),Ok((1,0)));
 }
 #[test]fn r792_unknown_receiver_phase_fails_before_snapshot(){
  let (root,s,d,v)=pair();
  exec(&d,"UPDATE mirror_meta SET phase='UNATTESTED'").unwrap();
  assert_eq!(r790_copy_wal_snapshot(&root.0,&s,&d,&v,||{},false),
    Err(EventOutboxR366Error::Corrupt));
  assert_eq!(count(&d,"metadata"),0);
 }
 #[test]fn r793_bootstrap_and_replay_are_not_cleanup_eligible(){
  let (root,s,d,v)=pair();
  assert_eq!(r756_cleanup(&root.0,&s,&d,&v),
   Err(EventOutboxR366Error::Conflict));
  r790_copy_wal_snapshot(&root.0,&s,&d,&v,||{},false).unwrap();
  assert_eq!(r756_cleanup(&root.0,&s,&d,&v),
   Err(EventOutboxR366Error::Conflict));
  assert_eq!(count(&s,"mirror_outbox"),1);
 }
 #[test]fn r794_mutated_outbox_prevents_final_synced_transition(){
  let (root,s,d,v)=pair();
  r790_copy_wal_snapshot(&root.0,&s,&d,&v,
   ||python(&root.0,"update"),false).unwrap();
  exec(&s,"UPDATE mirror_outbox SET payload_hash='forged' WHERE seq=2").unwrap();
  assert!(r790_replay_cutover(&root.0,&s,&d,&v).is_err());
  assert_eq!(scalar(&d,"SELECT phase FROM mirror_meta"),"REPLAY");
  assert_eq!(scalar(&d,"SELECT value FROM metadata"),"from_original_python");
 }
 #[test]fn r795_post_bootstrap_new_original_python_frame_uses_r756_replay(){
  let (root,s,d,v)=pair();
  r790_copy_wal_snapshot(&root.0,&s,&d,&v,||{},false).unwrap();
  r790_replay_cutover(&root.0,&s,&d,&v).unwrap();
  r756_cleanup(&root.0,&s,&d,&v).unwrap();
  python(&root.0,"update");
  assert_eq!(r756_replay(&root.0,&s,&d,&v,8),Ok(1));
  assert_eq!(r756_cleanup(&root.0,&s,&d,&v),Ok((1,0)));
  assert_eq!(scalar(&d,"SELECT value FROM metadata"),"updated_by_original_python");
 }
}
