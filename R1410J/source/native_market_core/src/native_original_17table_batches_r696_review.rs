// Astra P0-01 G3/G4 R696+: bounded generation-fenced multi-batch protocol.
// Builds ONLY on the already-isolated R671 file-backed ORIGINAL 17-table
// SQLite pair. All guard/configuration and tests are local review, not PAPER.
fn r696_identity(root:&Path)->Result<String,EventOutboxR366Error>{
 use std::os::unix::fs::MetadataExt;
 let p=r671_guard(root,"r671-private-db1.sqlite")?;
 let meta=fs::metadata(p).map_err(|_|EventOutboxR366Error::Path)?;
 Ok(format!("{}:{}",meta.dev(),meta.ino()))
}
fn r696_init(root:&Path,src:&Db,dst:&Db)
 ->Result<(),EventOutboxR366Error>{
 r671_check_source_witness(root,src,dst)?;
 let identity=r696_identity(root)?;
 // New review-only fields, not migrations for original production DB1/DB2.
 exec(src,"ALTER TABLE r659_source_control ADD COLUMN state TEXT NOT NULL DEFAULT 'NORMAL';
 ALTER TABLE r659_source_control ADD COLUMN generation TEXT NOT NULL DEFAULT 'gen-r696-1';
 ALTER TABLE r659_source_control ADD COLUMN source_identity TEXT NOT NULL DEFAULT '';
 ALTER TABLE r659_source_control ADD COLUMN cleanup_through INTEGER NOT NULL DEFAULT 0;")
  .map_err(r366_error)?;
 exec(dst,"ALTER TABLE r659_sink_control ADD COLUMN state TEXT NOT NULL DEFAULT 'SYNCED';
 ALTER TABLE r659_sink_control ADD COLUMN generation TEXT NOT NULL DEFAULT 'gen-r696-1';
 ALTER TABLE r659_sink_control ADD COLUMN source_identity TEXT NOT NULL DEFAULT '';")
  .map_err(r366_error)?;
 let sql1=format!("UPDATE r659_source_control SET source_identity={}",
  r586_sql_literal(&serde_json::json!(identity))?);
 let sql2=format!("UPDATE r659_sink_control SET source_identity={}",
  r586_sql_literal(&serde_json::json!(identity))?);
 exec(src,&sql1).map_err(r366_error)?;
 exec(dst,&sql2).map_err(r366_error)?;
 Ok(())
}
fn r696_check_fence(root:&Path,src:&Db,dst:&Db)
 ->Result<(),EventOutboxR366Error>{
 let identity=r696_identity(root)?;
 r671_check_source_witness(root,src,dst)?;
 let read=|db:&Db,table:&str|
  ->Result<(String,String,String),EventOutboxR366Error>{
   let sql=format!("SELECT state,generation,source_identity FROM {table} WHERE id=1");
   let mut q=Stmt::new(db,&sql).map_err(r366_error)?;
   if q.step().map_err(r366_error)?!=SQLITE_ROW{
    return Err(EventOutboxR366Error::Corrupt)
   }
   Ok((q.strval(0).map_err(r366_error)?,
      q.strval(1).map_err(r366_error)?,
      q.strval(2).map_err(r366_error)?))
 };
 let (state,gen,src_id)=read(src,"r659_source_control")?;
 let (phase,other_gen,dst_id)=read(dst,"r659_sink_control")?;
 if state!="NORMAL"||phase!="SYNCED"{
  return Err(EventOutboxR366Error::Conflict)
 }
 if gen.is_empty()||gen.len()>80||!gen.bytes().all(|b|
    b.is_ascii_alphanumeric()||b==b'-'||b==b'_')
    ||other_gen!=gen||src_id!=identity||dst_id!=identity{
  return Err(EventOutboxR366Error::Corrupt)
 }
 Ok(())
}
fn r696_append(root:&Path,src:&Db,dst:&Db,v:&serde_json::Value,
 operations:&[serde_json::Value])->Result<u64,EventOutboxR366Error>{
 r696_check_fence(root,src,dst)?;
 let latest=r366_query_num(src,"SELECT latest_seq FROM r659_source_control WHERE id=1")?;
 let applied=r366_query_num(dst,"SELECT applied_seq FROM r659_sink_control WHERE id=1")?;
 let ack=r366_query_num(src,"SELECT ack_seq FROM r659_source_control WHERE id=1")?;
 if latest<applied||applied<ack||latest.saturating_sub(ack)>=512{
  return Err(EventOutboxR366Error::Pressure)
 }
 r659_append(src,v,operations)
}
fn r696_replay_batch(root:&Path,src:&Db,dst:&Db,v:&serde_json::Value,
 max_frames:u64)->Result<u64,EventOutboxR366Error>{
 if !(1..=32).contains(&max_frames){return Err(EventOutboxR366Error::Pressure)}
 r696_check_fence(root,src,dst)?;
 let latest=r366_query_num(src,"SELECT latest_seq FROM r659_source_control WHERE id=1")?;
 let mut applied=r366_query_num(dst,"SELECT applied_seq FROM r659_sink_control WHERE id=1")?;
 if applied>latest{return Err(EventOutboxR366Error::Corrupt)}
 let mut n=0;
 while applied<latest&&n<max_frames{
  r696_check_fence(root,src,dst)?;
  r671_replay_file(root,src,dst,v,applied+1,false)?;
  applied+=1;
  n+=1;
 }
 Ok(n)
}
fn r696_finalize_ack(root:&Path,src:&Db,dst:&Db)
 ->Result<(u64,u64),EventOutboxR366Error>{
 r696_check_fence(root,src,dst)?;
 let source_tx=Tx::begin(src).map_err(r366_error)?;
 let sink_tx=Tx::begin(dst).map_err(r366_error)?;
 let latest=r366_query_num(src,"SELECT latest_seq FROM r659_source_control WHERE id=1")?;
 let applied=r366_query_num(dst,"SELECT applied_seq FROM r659_sink_control WHERE id=1")?;
 let old_ack=r366_query_num(src,"SELECT ack_seq FROM r659_source_control WHERE id=1")?;
 let before=r366_query_num(src,"SELECT cleanup_through FROM r659_source_control WHERE id=1")?;
 if applied!=latest||old_ack>latest||before>old_ack{return Err(EventOutboxR366Error::Conflict)}
 let v=r586_oracle();
 r691_original_rows_match(src,dst,&v)?;
 if old_ack<latest{
  let (_,hash,_)=r659_source_frame(src,latest)?;
  let mut q=Stmt::new(dst,"SELECT payload_hash FROM r659_sink_control WHERE id=1")
   .map_err(r366_error)?;
  if q.step().map_err(r366_error)?!=SQLITE_ROW||
    q.strval(0).map_err(r366_error)?!=hash{
   return Err(EventOutboxR366Error::Corrupt)
  }
 }
 // Original Python outbox cleanup is bounded at 128 records. The review
 // model never deletes unacked records and updates the watermark atomically.
 let max_delete=old_ack.max(latest);
 let sql=format!("DELETE FROM r659_source_feed WHERE seq IN (
   SELECT seq FROM r659_source_feed WHERE seq<={max_delete}
   ORDER BY seq LIMIT 128)");
 exec(src,&sql).map_err(r366_error)?;
 let mut cur=Stmt::new(src,"SELECT COUNT(*),COALESCE(MIN(seq),0)
  FROM r659_source_feed WHERE seq<=?1").map_err(r366_error)?;
 cur.number(1,max_delete).map_err(r366_error)?;
 if cur.step().map_err(r366_error)?!=SQLITE_ROW{
  return Err(EventOutboxR366Error::Corrupt)
 }
 let pending=cur.num(0).map_err(r366_error)?;
 let next=cur.num(1).map_err(r366_error)?;
 drop(cur);
 let through=if pending==0{max_delete}else{next-1};
 if through<before||through>latest{return Err(EventOutboxR366Error::Corrupt)}
 exec(src,&format!("UPDATE r659_source_control SET ack_seq={latest},
   cleanup_through={through} WHERE id=1")).map_err(r366_error)?;
 sink_tx.commit().map_err(r366_error)?;
 source_tx.commit().map_err(r366_error)?;
 Ok((through,pending))
}

#[cfg(test)]
mod r696_tests{
 use super::*;
 use std::os::unix::fs::DirBuilderExt;
 use serde_json::{json,Value};
 struct ReviewRoot(PathBuf);
 impl ReviewRoot{
  fn new()->Self{
   let mut path=std::env::temp_dir();
   let ts=std::time::SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
   path.push(format!("astra_r671_file_{}_{}_r696",std::process::id(),ts));
   fs::DirBuilder::new().mode(0o700).create(&path).unwrap();
   Self(path)
  }
 }
 impl Drop for ReviewRoot{fn drop(&mut self){let _=fs::remove_dir_all(&self.0);}}
 fn base()->(ReviewRoot,Db,Db,Value){
  let root=ReviewRoot::new();
  let (s,d,v)=r671_file_pair(&root.0).unwrap();
  r696_init(&root.0,&s,&d).unwrap();
  (root,s,d,v)
 }
 fn op(v:&Value,name:&str,kind:&str)->Value{
  let row=v["fixture_snapshot"]["tables"].as_array().unwrap().iter()
   .find(|t|t["schema"]["table"]==name).unwrap()["rows"][0]["row"].clone();
  r659_op(v,name,&row,kind).unwrap()
 }
 fn six(v:&Value)->Vec<Value>{
  ["attempts","collector_runs","events","decisions","hourly_raw_rollups","metadata"]
   .iter().map(|n|op(v,n,"insert")).collect()
 }
 fn seeded()->(ReviewRoot,Db,Db,Value){
  let (root,s,d,v)=base();
  assert_eq!(r696_append(&root.0,&s,&d,&v,&six(&v)),Ok(1));
  (root,s,d,v)
 }
 fn meta_change(v:&Value,n:usize)->Vec<Value>{
  let mut o=op(v,"metadata","update");
  o["row"]["value"]=json!(format!("r696-update-{n}"));
  o["after_hash"]=json!(r366_sha(
   &r386_python_mirror_canonical(&o["row"]).unwrap()));
  vec![o]
 }
 fn progress(s:&Db,d:&Db)->(u64,u64,u64){
  (r366_query_num(s,"SELECT latest_seq FROM r659_source_control").unwrap(),
   r366_query_num(d,"SELECT applied_seq FROM r659_sink_control").unwrap(),
   r366_query_num(s,"SELECT ack_seq FROM r659_source_control").unwrap())
 }
 #[test]fn r696_generation_fence_exists_only_in_private_paired_files(){
  let (root,s,d,_)=base();
  r696_check_fence(&root.0,&s,&d).unwrap();
  assert_eq!(progress(&s,&d),(0,0,0));
 }
 #[test]fn r697_two_packet_batch_sends_without_ack_until_finalized(){
  let (root,s,d,v)=seeded();
  r696_append(&root.0,&s,&d,&v,&meta_change(&v,2)).unwrap();
  r696_append(&root.0,&s,&d,&v,&meta_change(&v,3)).unwrap();
  assert_eq!(r696_replay_batch(&root.0,&s,&d,&v,2),Ok(2));
  assert_eq!(progress(&s,&d),(3,2,0));
  assert_eq!(r366_query_num(&s,"SELECT COUNT(*) FROM r659_source_feed").unwrap(),3);
  assert_eq!(r696_replay_batch(&root.0,&s,&d,&v,2),Ok(1));
  assert_eq!(progress(&s,&d),(3,3,0));
  assert_eq!(r696_finalize_ack(&root.0,&s,&d),Ok((3,0)));
  assert_eq!(progress(&s,&d),(3,3,3));
 }
 #[test]fn r698_partial_batch_has_no_cleanup_permission(){
  let (root,s,d,v)=seeded();
  r696_append(&root.0,&s,&d,&v,&meta_change(&v,2)).unwrap();
  assert_eq!(r696_replay_batch(&root.0,&s,&d,&v,1),Ok(1));
  assert_eq!(r696_finalize_ack(&root.0,&s,&d),
    Err(EventOutboxR366Error::Conflict));
  assert_eq!(progress(&s,&d),(2,1,0));
 }
 #[test]fn r699_lost_ack_is_delivered_after_reopen(){
  let (root,s,d,v)=seeded();
  assert_eq!(r696_replay_batch(&root.0,&s,&d,&v,8),Ok(1));
  drop(s);drop(d);
  let (s,d,v)=r671_reopen_pair(&root.0).unwrap();
  assert_eq!(r696_replay_batch(&root.0,&s,&d,&v,8),Ok(0));
  assert_eq!(r696_finalize_ack(&root.0,&s,&d),Ok((1,0)));
  assert_eq!(progress(&s,&d),(1,1,1));
 }
 #[test]fn r700_duplicate_frame_cannot_increment_cursor(){
  let (root,s,d,v)=seeded();
  r696_replay_batch(&root.0,&s,&d,&v,1).unwrap();
  assert_eq!(r671_replay_file(&root.0,&s,&d,&v,1,false),
   Err(EventOutboxR366Error::Duplicate));
  assert_eq!(progress(&s,&d),(1,1,0));
 }
 #[test]fn r701_source_rebuild_required_denies_both_write_and_replay(){
  let (root,s,d,v)=seeded();
  exec(&s,"UPDATE r659_source_control SET state='REBUILD_REQUIRED'").unwrap();
  assert_eq!(r696_replay_batch(&root.0,&s,&d,&v,1),
   Err(EventOutboxR366Error::Conflict));
  assert_eq!(r696_append(&root.0,&s,&d,&v,&meta_change(&v,2)),
   Err(EventOutboxR366Error::Conflict));
  assert_eq!(progress(&s,&d),(1,0,0));
 }
 #[test]fn r702_bootstrap_required_never_auto_promotes_to_normal(){
  let (root,s,d,v)=seeded();
  exec(&s,"UPDATE r659_source_control SET state='BOOTSTRAP_REQUIRED'").unwrap();
  assert_eq!(r696_replay_batch(&root.0,&s,&d,&v,1),
   Err(EventOutboxR366Error::Conflict));
  assert_eq!(r696_finalize_ack(&root.0,&s,&d),
   Err(EventOutboxR366Error::Conflict));
 }
 #[test]fn r703_receiver_in_rebuild_state_blocks_source_ack(){
  let (root,s,d,v)=seeded();
  exec(&d,"UPDATE r659_sink_control SET state='REBUILD_REQUIRED'").unwrap();
  assert_eq!(r696_replay_batch(&root.0,&s,&d,&v,1),
   Err(EventOutboxR366Error::Conflict));
  assert_eq!(progress(&s,&d),(1,0,0));
 }
 #[test]fn r704_generation_mismatch_is_fail_closed(){
  let (root,s,d,v)=seeded();
  exec(&d,"UPDATE r659_sink_control SET generation='new-gen'").unwrap();
  assert_eq!(r696_replay_batch(&root.0,&s,&d,&v,1),
   Err(EventOutboxR366Error::Corrupt));
  assert_eq!(progress(&s,&d),(1,0,0));
 }
 #[test]fn r705_source_generation_mismatch_is_fail_closed(){
  let (root,s,d,v)=seeded();
  exec(&s,"UPDATE r659_source_control SET generation='other-gen'").unwrap();
  assert_eq!(r696_append(&root.0,&s,&d,&v,&meta_change(&v,2)),
   Err(EventOutboxR366Error::Corrupt));
 }
 #[test]fn r706_empty_generation_is_never_accepted(){
  let (root,s,d,v)=seeded();
  exec(&s,"UPDATE r659_source_control SET generation=''").unwrap();
  assert_eq!(r696_replay_batch(&root.0,&s,&d,&v,1),
   Err(EventOutboxR366Error::Corrupt));
 }
 #[test]fn r707_mirror_source_identity_drift_is_rejected(){
  let (root,s,d,v)=seeded();
  exec(&d,"UPDATE r659_sink_control SET source_identity='other-inode'").unwrap();
  assert_eq!(r696_replay_batch(&root.0,&s,&d,&v,1),
   Err(EventOutboxR366Error::Corrupt));
 }
 #[test]fn r708_zero_batch_size_fails_closed(){
  let (root,s,d,v)=seeded();
  assert_eq!(r696_replay_batch(&root.0,&s,&d,&v,0),
   Err(EventOutboxR366Error::Pressure));
 }
 #[test]fn r709_large_batch_size_fails_closed(){
  let (root,s,d,v)=seeded();
  assert_eq!(r696_replay_batch(&root.0,&s,&d,&v,33),
   Err(EventOutboxR366Error::Pressure));
 }
 #[test]fn r710_mirror_cursor_ahead_of_source_fails_closed(){
  let (root,s,d,v)=seeded();
  exec(&d,"UPDATE r659_sink_control SET applied_seq=2").unwrap();
  assert_eq!(r696_replay_batch(&root.0,&s,&d,&v,1),
   Err(EventOutboxR366Error::Corrupt));
 }
 #[test]fn r711_unlogged_destination_mutation_blocks_ack(){
  let (root,s,d,v)=seeded();
  r696_replay_batch(&root.0,&s,&d,&v,8).unwrap();
  exec(&d,"UPDATE metadata SET value='not-from-db1'").unwrap();
  assert_eq!(r696_finalize_ack(&root.0,&s,&d),
   Err(EventOutboxR366Error::Corrupt));
  assert_eq!(progress(&s,&d),(1,1,0));
 }
 #[test]fn r712_untrusted_frame_hash_blocks_multi_batch(){
  let (root,s,d,v)=seeded();
  exec(&s,"UPDATE r659_source_feed SET payload_hash='forged' WHERE seq=1").unwrap();
  assert_eq!(r696_replay_batch(&root.0,&s,&d,&v,8),
   Err(EventOutboxR366Error::Corrupt));
  assert_eq!(progress(&s,&d),(1,0,0));
 }
 #[test]fn r713_cleanup_is_bounded_at_128_and_does_not_skip(){
  let (root,s,d,v)=seeded();
  for n in 2..=130{
   r696_append(&root.0,&s,&d,&v,&meta_change(&v,n)).unwrap();
  }
  let mut total=0;
  loop{
   let n=r696_replay_batch(&root.0,&s,&d,&v,32).unwrap();
   if n==0{break}
   assert!(n<=32);
   total+=n;
  }
  assert_eq!(total,130);
  assert_eq!(progress(&s,&d),(130,130,0));
  assert_eq!(r696_finalize_ack(&root.0,&s,&d),Ok((128,2)));
  assert_eq!(r366_query_num(&s,"SELECT COUNT(*) FROM r659_source_feed").unwrap(),2);
  assert_eq!(progress(&s,&d),(130,130,130));
  assert_eq!(r696_finalize_ack(&root.0,&s,&d),Ok((130,0)));
  assert_eq!(r366_query_num(&s,"SELECT COUNT(*) FROM r659_source_feed").unwrap(),0);
 }
 #[test]fn r714_replay_after_durable_ack_is_idempotent_without_new_cursor(){
  let (root,s,d,v)=seeded();
  r696_replay_batch(&root.0,&s,&d,&v,2).unwrap();
  r696_finalize_ack(&root.0,&s,&d).unwrap();
  drop(s);drop(d);
  let (s,d,v)=r671_reopen_pair(&root.0).unwrap();
  assert_eq!(r696_replay_batch(&root.0,&s,&d,&v,8),Ok(0));
  assert_eq!(progress(&s,&d),(1,1,1));
 }
 #[test]fn r715_forged_source_ack_ahead_of_latest_blocks_cleanup(){
  let (root,s,d,v)=seeded();
  r696_replay_batch(&root.0,&s,&d,&v,8).unwrap();
  exec(&s,"UPDATE r659_source_control SET ack_seq=2").unwrap();
  assert_eq!(r696_finalize_ack(&root.0,&s,&d),
   Err(EventOutboxR366Error::Conflict));
 }
 #[test]fn r716_cleanup_watermark_ahead_of_ack_blocks_cleanup(){
  let (root,s,d,v)=seeded();
  r696_replay_batch(&root.0,&s,&d,&v,8).unwrap();
  exec(&s,"UPDATE r659_source_control SET cleanup_through=2").unwrap();
  assert_eq!(r696_finalize_ack(&root.0,&s,&d),
   Err(EventOutboxR366Error::Conflict));
 }
 #[test]fn r717_source_row_drift_since_record_blocks_ack(){
  let (root,s,d,v)=seeded();
  r696_replay_batch(&root.0,&s,&d,&v,8).unwrap();
  exec(&s,"UPDATE metadata SET value='unrecorded'").unwrap();
  assert_eq!(r696_finalize_ack(&root.0,&s,&d),
   Err(EventOutboxR366Error::Corrupt));
  assert_eq!(progress(&s,&d),(1,1,0));
 }
 #[test]fn r718_unknown_source_db2_phase_never_auto_bootstraps(){
  let (root,s,d,v)=seeded();
  exec(&d,"UPDATE r659_sink_control SET state='BOOTSTRAP_REQUIRED'").unwrap();
  assert_eq!(r696_replay_batch(&root.0,&s,&d,&v,8),
   Err(EventOutboxR366Error::Conflict));
  assert_eq!(r696_finalize_ack(&root.0,&s,&d),
   Err(EventOutboxR366Error::Conflict));
 }
 #[test]fn r719_schema_mutation_blocks_frame_before_new_ack(){
  let (root,s,d,v)=seeded();
  exec(&d,"CREATE INDEX r719_illicit ON metadata(value)").unwrap();
  assert_eq!(r696_replay_batch(&root.0,&s,&d,&v,8),
   Err(EventOutboxR366Error::Corrupt));
  assert_eq!(progress(&s,&d),(1,0,0));
 }
 #[test]fn r720_reopen_preserves_generation_and_source_identity(){
  let (root,s,d,_)=seeded();
  drop(s);drop(d);
  let (s,d,v)=r671_reopen_pair(&root.0).unwrap();
  r696_check_fence(&root.0,&s,&d).unwrap();
  assert_eq!(r696_replay_batch(&root.0,&s,&d,&v,8),Ok(1));
  assert_eq!(r696_finalize_ack(&root.0,&s,&d),Ok((1,0)));
 }
 #[test]fn r721_second_generation_init_is_refused_not_reinitialized(){
  let (root,s,d,_)=base();
  assert!(r696_init(&root.0,&s,&d).is_err());
  r696_check_fence(&root.0,&s,&d).unwrap();
  assert_eq!(progress(&s,&d),(0,0,0));
 }
 #[test]fn r722_forged_source_control_identity_refused(){
  let (root,s,d,v)=seeded();
  exec(&s,"UPDATE r659_source_control SET source_identity='foreign-owner'").unwrap();
  assert_eq!(r696_replay_batch(&root.0,&s,&d,&v,8),
   Err(EventOutboxR366Error::Corrupt));
  assert_eq!(progress(&s,&d),(1,0,0));
 }
 #[test]fn r723_tampered_db2_ack_hash_preserves_source_feed(){
  let (root,s,d,v)=seeded();
  r696_replay_batch(&root.0,&s,&d,&v,8).unwrap();
  exec(&d,"UPDATE r659_sink_control SET payload_hash='wrong-hash'").unwrap();
  assert_eq!(r696_finalize_ack(&root.0,&s,&d),
   Err(EventOutboxR366Error::Corrupt));
  assert_eq!(progress(&s,&d),(1,1,0));
  assert_eq!(r366_query_num(&s,"SELECT COUNT(*) FROM r659_source_feed").unwrap(),1);
 }
 #[test]fn r724_sequential_durable_cohorts_preserve_monotonic_ack(){
  let (root,s,d,v)=seeded();
  r696_replay_batch(&root.0,&s,&d,&v,8).unwrap();
  assert_eq!(r696_finalize_ack(&root.0,&s,&d),Ok((1,0)));
  r696_append(&root.0,&s,&d,&v,&meta_change(&v,2)).unwrap();
  drop(s);drop(d);
  let (s,d,v)=r671_reopen_pair(&root.0).unwrap();
  r696_replay_batch(&root.0,&s,&d,&v,8).unwrap();
  assert_eq!(r696_finalize_ack(&root.0,&s,&d),Ok((2,0)));
  assert_eq!(progress(&s,&d),(2,2,2));
  assert_eq!(r366_query_num(&s,"SELECT COUNT(*) FROM r659_source_feed").unwrap(),0);
 }
 #[test]fn r725_backlog_reserve_512_refuses_new_append(){
  let (root,s,d,v)=seeded();
  exec(&s,"UPDATE r659_source_control SET latest_seq=513").unwrap();
  assert_eq!(r696_append(&root.0,&s,&d,&v,&meta_change(&v,2)),
   Err(EventOutboxR366Error::Pressure));
  assert_eq!(r366_query_num(&s,"SELECT COUNT(*) FROM r659_source_feed").unwrap(),1);
 }
}
