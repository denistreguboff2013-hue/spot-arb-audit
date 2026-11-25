// Astra P0-01 R860: bounded, fully attested, whole-retained-tail ACK.
// One complete source hash-chain validation AND 17-table full parity.
// Atomic source cleanup happens only if DB2 has reached source latest_seq.
// Never skips parity / hashes; only avoids repeating parity N times when
// more than 128 already-applied outbox frames remain. R671 private only.
fn r860_bulk_verified_ack(
 root:&Path,src:&Db,dst:&Db,manifest:&serde_json::Value,max_retained:usize
)->Result<(u64,u64),EventOutboxR366Error>{
 if !(1..=1024).contains(&max_retained){
  return Err(EventOutboxR366Error::Pressure)
 }
 // Source BEGIN IMMEDIATE fences all other source writers across parity,
 // hash verification and cleanup. DB2 remains fenced through read/ACK.
 let source_tx=Tx::begin(src).map_err(r366_error)?;
 let dest_tx=Tx::begin(dst).map_err(r366_error)?;
 let (head,control,meta)=r805_read_head(root,src,dst)?;
 if head.applied!=head.latest{return Err(EventOutboxR366Error::Conflict)}
 if head.retained>max_retained as u64{
  return Err(EventOutboxR366Error::Pressure)
 }
 r805_verify_chain(src,&head,r726_s(&control,"feed_generation")?)?;
 if r366_query_num(dst,"SELECT COUNT(*) FROM pragma_foreign_key_check")?!=0{
  return Err(EventOutboxR366Error::Corrupt)
 }
 r800_streaming_original_parity(src,dst,manifest)?;
 let mut last=Stmt::new(src,
  "SELECT payload_hash FROM mirror_outbox ORDER BY seq DESC LIMIT 1")
   .map_err(r366_error)?;
 match last.step().map_err(r366_error)?{
  SQLITE_ROW=>{
   let hash=last.strval(0).map_err(r366_error)?;
   if meta["applied_hash"].as_str()!=Some(hash.as_str()){
    return Err(EventOutboxR366Error::Corrupt)
   }
  }
  SQLITE_DONE if head.retained==0=>{}
  _=>return Err(EventOutboxR366Error::Corrupt)
 }
 drop(last);
 if head.retained!=0{
  exec(src,&format!("DELETE FROM mirror_outbox WHERE seq<={};
    UPDATE mirror_control SET cleanup_through={},
    retained_records=0,retained_bytes=0 WHERE id=1",
   head.latest,head.latest)).map_err(r366_error)?;
  if r366_query_num(src,"SELECT COUNT(*) FROM mirror_outbox")?!=0{
   return Err(EventOutboxR366Error::Corrupt)
  }
 }
 dest_tx.commit().map_err(r366_error)?;
 source_tx.commit().map_err(r366_error)?;
 Ok((head.retained,0))
}

// R865: separate guarded CLI entrypoint. Existing R856 mode remains
// unchanged for rollback. May ACK <=1024 already-applied envelopes in one
// fully-verified source transaction, never ACK a partial DB2 frontier.
pub fn r865_private_fast_ack_drain_once(root:&Path)
 ->Result<(u64,u64,u64),String>{
 let source=r671_open(root,"r671-private-db1.sqlite",false)
  .map_err(|e|format!("private source: {e:?}"))?;
 let mirror=r671_open(root,"r671-private-db2.sqlite",false)
  .map_err(|e|format!("private mirror: {e:?}"))?;
 let manifest=r586_oracle();
 let copied=r845_verified_burst(root,&source,&mirror,&manifest,128)
  .map_err(|e|format!("full chain verified replay: {e:?}"))?;
 let (head,_,_)=r805_read_head(root,&source,&mirror)
  .map_err(|e|format!("private cursor: {e:?}"))?;
 let (acked,retained)=if head.applied==head.latest{
  match r860_bulk_verified_ack(root,&source,&mirror,&manifest,1024){
   Ok(result)=>result,
   Err(EventOutboxR366Error::Conflict)=>(0,head.retained),
   Err(e)=>return Err(format!("R860 atomic verified ACK: {e:?}"))
  }
 }else{(0,head.retained)};
 Ok((copied,acked,retained))
}
pub fn r865_private_fast_ack_drain_bounded(root:&Path,max_cycles:usize)
 ->Result<(u64,u64,u64),String>{
 if !(1..=256).contains(&max_cycles){
  return Err("private R865 requires 1..256 cycles".into())
 }
 let mut delivered=0_u64;
 let mut acked=0_u64;
 let mut retained=0_u64;
 for _ in 0..max_cycles{
  let (d,a,r)=r865_private_fast_ack_drain_once(root)?;
  delivered=delivered.checked_add(d).ok_or("delivered counter overflow")?;
  acked=acked.checked_add(a).ok_or("acked counter overflow")?;
  retained=r;
  if retained==0||(d==0&&a==0){break}
 }
 Ok((delivered,acked,retained))
}
#[cfg(test)]
mod r860_tests{
 use super::*;
 use std::os::unix::fs::DirBuilderExt;
 use std::process::Command;
 struct Private(PathBuf);
 impl Private{
  fn new()->Self{
   let mut root=std::env::temp_dir();
   let t=std::time::SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
   root.push(format!("astra_r671_file_{}_{}_r860",std::process::id(),t));
   fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
   drop(r756_initialize(&root).unwrap());
   Self(root)
  }
  fn python(&self,mode:&str){
   let script=Path::new(env!("CARGO_MANIFEST_DIR"))
    .join("offline_original_python_producer_r786.py");
   let x=Command::new("/usr/bin/python3").arg(script)
    .arg(&self.0).arg(mode).output().unwrap();
   assert!(x.status.success(),"original Python fixture writer");
  }
  fn handles(&self)->(Db,Db,serde_json::Value){
   let a=r671_open(&self.0,"r671-private-db1.sqlite",false).unwrap();
   let b=r671_open(&self.0,"r671-private-db2.sqlite",false).unwrap();
   (a,b,r586_oracle())
  }
  fn backlog(&self)->(Db,Db,serde_json::Value){
   self.python("insert");
   let (src,dst,v)=self.handles();
   assert_eq!(r845_verified_burst(&self.0,&src,&dst,&v,128),Ok(1));
   assert_eq!(r860_bulk_verified_ack(&self.0,&src,&dst,&v,1024),Ok((1,0)));
   drop(src);drop(dst);
   self.python("batch160");
   self.handles()
  }
 }
 impl Drop for Private{fn drop(&mut self){let _=fs::remove_dir_all(&self.0);}}
 #[test]fn r860_python_160_records_one_ack_after_two_verified_bursts(){
  let p=Private::new();
  let (src,dst,v)=p.backlog();
  assert_eq!(r845_verified_burst(&p.0,&src,&dst,&v,128),Ok(128));
  assert_eq!(r860_bulk_verified_ack(&p.0,&src,&dst,&v,1024),
   Err(EventOutboxR366Error::Conflict));
  assert_eq!(r366_query_num(&src,"SELECT cleanup_through FROM mirror_control"),Ok(1));
  assert_eq!(r845_verified_burst(&p.0,&src,&dst,&v,128),Ok(32));
  assert_eq!(r860_bulk_verified_ack(&p.0,&src,&dst,&v,1024),Ok((160,0)));
  assert_eq!(r366_query_num(&src,"SELECT retained_records FROM mirror_control"),Ok(0));
  assert_eq!(r366_query_num(&src,"SELECT retained_bytes FROM mirror_control"),Ok(0));
  assert_eq!(r366_query_num(&src,"SELECT cleanup_through FROM mirror_control"),Ok(161));
  assert_eq!(r366_query_num(&dst,"SELECT applied_seq FROM mirror_meta"),Ok(161));
  assert_eq!(r800_streaming_original_parity(&src,&dst,&v),Ok(1));
 }
 #[test]fn r861_corrupted_distant_outbox_hash_denies_whole_ack(){
  let p=Private::new();let (src,dst,v)=p.backlog();
  assert_eq!(r845_verified_burst(&p.0,&src,&dst,&v,128),Ok(128));
  assert_eq!(r845_verified_burst(&p.0,&src,&dst,&v,128),Ok(32));
  exec(&src,"UPDATE mirror_outbox SET payload_hash='forged' WHERE seq=50").unwrap();
  assert_eq!(r860_bulk_verified_ack(&p.0,&src,&dst,&v,1024),
   Err(EventOutboxR366Error::Corrupt));
  assert_eq!(r366_query_num(&src,"SELECT retained_records FROM mirror_control"),Ok(160));
  assert_eq!(r366_query_num(&src,"SELECT cleanup_through FROM mirror_control"),Ok(1));
 }
 #[test]fn r862_source_destination_parity_mismatch_never_removes_outbox(){
  let p=Private::new();let (src,dst,v)=p.backlog();
  assert_eq!(r845_verified_burst(&p.0,&src,&dst,&v,128),Ok(128));
  assert_eq!(r845_verified_burst(&p.0,&src,&dst,&v,128),Ok(32));
  exec(&dst,"UPDATE metadata SET value='destination_mutated'
    WHERE key='rolled_up_raw_events'").unwrap();
  assert_eq!(r860_bulk_verified_ack(&p.0,&src,&dst,&v,1024),
   Err(EventOutboxR366Error::Corrupt));
  assert_eq!(r366_query_num(&src,"SELECT retained_records FROM mirror_control"),Ok(160));
  assert_eq!(r366_query_num(&src,"SELECT cleanup_through FROM mirror_control"),Ok(1));
 }
 #[test]fn r863_ack_limit_and_wrong_applied_hash_are_both_denied(){
  let p=Private::new();let (src,dst,v)=p.backlog();
  assert_eq!(r845_verified_burst(&p.0,&src,&dst,&v,128),Ok(128));
  assert_eq!(r845_verified_burst(&p.0,&src,&dst,&v,128),Ok(32));
  assert_eq!(r860_bulk_verified_ack(&p.0,&src,&dst,&v,128),
   Err(EventOutboxR366Error::Pressure));
  exec(&dst,"UPDATE mirror_meta SET applied_hash='incorrect' WHERE id=1").unwrap();
  assert_eq!(r860_bulk_verified_ack(&p.0,&src,&dst,&v,1024),
   Err(EventOutboxR366Error::Corrupt));
  assert_eq!(r366_query_num(&src,"SELECT retained_records FROM mirror_control"),Ok(160));
 }
 #[test]fn r864_large_fixture_12050_rows_two_ack_scans_versus_one(){
  use std::time::Instant;
  for bulk in [false,true]{
   let p=Private::new();
   let (src,dst,v)=p.backlog();
   // Identical immutable baseline-only rows on both isolated private
   // fixture databases. Never present this as live DB1 feed evidence.
   let fill="WITH RECURSIVE n(i) AS (
     SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<12050
    ) INSERT INTO metadata(key,value)
    SELECT printf('r864_key_%06d',i),printf('r864_value_%06d',i) FROM n";
   exec(&src,fill).unwrap();exec(&dst,fill).unwrap();
   assert_eq!(r845_verified_burst(&p.0,&src,&dst,&v,128),Ok(128));
   assert_eq!(r845_verified_burst(&p.0,&src,&dst,&v,128),Ok(32));
   let start=Instant::now();
   if bulk{
    assert_eq!(r860_bulk_verified_ack(&p.0,&src,&dst,&v,1024),Ok((160,0)));
   }else{
    assert_eq!(r805_cleanup_window(&p.0,&src,&dst,&v),Ok((128,32)));
    assert_eq!(r805_cleanup_window(&p.0,&src,&dst,&v),Ok((32,0)));
   }
   let elapsed=start.elapsed().as_secs_f64()*1000.0;
   assert_eq!(r366_query_num(&src,"SELECT retained_records FROM mirror_control"),Ok(0));
   eprintln!("R864 12051-row private parity ack bulk={bulk} elapsed_ms={elapsed:.2} source_cursor=161");
  }
 }
 #[test]fn r865_bounded_separate_worker_replays_and_acks_python_160(){
  let p=Private::new();let (src,dst,v)=p.backlog();drop(src);drop(dst);
  assert_eq!(r865_private_fast_ack_drain_bounded(&p.0,4),
   Ok((160,160,0)));
  let (src,dst,_)=p.handles();
  assert_eq!(r366_query_num(&src,"SELECT retained_records FROM mirror_control"),Ok(0));
  assert_eq!(r366_query_num(&dst,"SELECT applied_seq FROM mirror_meta"),Ok(161));
  assert_eq!(r800_streaming_original_parity(&src,&dst,&v),Ok(1));
 }
 #[test]fn r866_corrupt_source_frame_does_not_enter_receiver_or_ack(){
  let p=Private::new();let (src,dst,_)=p.backlog();
  exec(&src,"UPDATE mirror_outbox SET payload_hash='tampered' WHERE seq=150").unwrap();
  assert!(r865_private_fast_ack_drain_bounded(&p.0,4).is_err());
  assert_eq!(r366_query_num(&src,"SELECT retained_records FROM mirror_control"),Ok(160));
  assert_eq!(r366_query_num(&dst,"SELECT applied_seq FROM mirror_meta"),Ok(1));
 }
 #[test]fn r867_private_worker_rejects_bad_limit_and_production_path(){
  assert!(r865_private_fast_ack_drain_bounded(Path::new("/tmp"),0).is_err());
  assert!(r865_private_fast_ack_drain_bounded(Path::new("/tmp"),257).is_err());
  assert!(r865_private_fast_ack_drain_once(
    Path::new("/home/tmpadmin/spotbot/data")).is_err());
 }
}
