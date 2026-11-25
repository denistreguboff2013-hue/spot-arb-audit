// Astra P0-01 R856: bounded STANDALONE private original DB1-to-DB2
// mirror pump. Reuses R845 full-chain verifier and R805 original ACK.
// The installed PAPER and historical DBs are never opened.
pub fn r856_private_drain_once(root:&Path)
 ->Result<(u64,u64,u64),String>{
 let source=r671_open(root,"r671-private-db1.sqlite",false)
  .map_err(|e|format!("private-source: {e:?}"))?;
 let mirror=r671_open(root,"r671-private-db2.sqlite",false)
  .map_err(|e|format!("private-mirror: {e:?}"))?;
 let manifest=r586_oracle();
 let count=r845_verified_burst(root,&source,&mirror,&manifest,128)
  .map_err(|e|format!("verified-burst: {e:?}"))?;
 let (head,_,_)=r805_read_head(root,&source,&mirror)
  .map_err(|e|format!("private-head: {e:?}"))?;
 // A concurrent source append may extend latest_seq between burst and
 // ACK. Do not acknowledge until the full source snapshot is applied.
 let (acked,retained)=if head.applied==head.latest{
  match r805_cleanup_window(root,&source,&mirror,&manifest){
   Ok(result)=>result,
   Err(EventOutboxR366Error::Conflict)=>(0,head.retained),
   Err(e)=>return Err(format!("verified-ack: {e:?}"))
  }
 }else{(0,head.retained)};
 Ok((count,acked,retained))
}
pub fn r856_private_drain_bounded(root:&Path,max_cycles:usize)
 ->Result<(u64,u64,u64),String>{
 if !(1..=256).contains(&max_cycles){
  return Err("bounded source mirror requires 1..256 cycles".into())
 }
 let mut copied=0_u64;
 let mut acknowledged=0_u64;
 let mut last_retained=0_u64;
 for _ in 0..max_cycles {
  let (c,a,retained)=r856_private_drain_once(root)?;
  copied=copied.checked_add(c).ok_or("copied overflow")?;
  acknowledged=acknowledged.checked_add(a).ok_or("ACK overflow")?;
  last_retained=retained;
  if c==0&&a==0||retained==0{break}
 }
 Ok((copied,acknowledged,last_retained))
}

#[cfg(test)]
mod r856_tests{
 use super::*;
 use std::os::unix::fs::DirBuilderExt;
 use std::process::Command;
 struct Private(PathBuf);
 impl Private{
  fn new()->Self{
   let mut p=std::env::temp_dir();
   let stamp=std::time::SystemTime::now()
     .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
   p.push(format!("astra_r671_file_{}_{}_r856",std::process::id(),stamp));
   fs::DirBuilder::new().mode(0o700).create(&p).unwrap();
   drop(r756_initialize(&p).unwrap());Self(p)
  }
  fn python(&self,mode:&str){
   let script=Path::new(env!("CARGO_MANIFEST_DIR"))
     .join("offline_original_python_producer_r786.py");
   let outcome=Command::new("/usr/bin/python3")
     .arg(script).arg(&self.0).arg(mode).output().unwrap();
   assert!(outcome.status.success(),"private original Python writer failed");
  }
  fn ready(&self){
   self.python("insert");
   assert_eq!(r856_private_drain_bounded(&self.0,4).unwrap(),(1,1,0));
  }
  fn handles(&self)->(Db,Db,serde_json::Value){
   let a=r671_open(&self.0,"r671-private-db1.sqlite",false).unwrap();
   let b=r671_open(&self.0,"r671-private-db2.sqlite",false).unwrap();
   (a,b,r586_oracle())
  }
 }
 impl Drop for Private{fn drop(&mut self){let _=fs::remove_dir_all(&self.0);}}
 #[test]fn r856_original_python_160_frames_two_bursts_two_acks(){
  let p=Private::new();p.ready();
  p.python("batch160");
  let first=r856_private_drain_once(&p.0).unwrap();
  assert_eq!(first,(128,0,160));
  let rest=r856_private_drain_bounded(&p.0,4).unwrap();
  assert_eq!(rest,(32,160,0));
  let (src,dst,v)=p.handles();
  assert_eq!(r800_streaming_original_parity(&src,&dst,&v),Ok(1));
  assert_eq!(r366_query_num(&src,"SELECT retained_records FROM mirror_control"),Ok(0));
  assert_eq!(r366_query_num(&dst,"SELECT applied_seq FROM mirror_meta"),Ok(161));
 }
 #[test]fn r857_late_original_python_update_after_partial_burst_no_early_ack(){
  let p=Private::new();p.ready();p.python("batch160");
  assert_eq!(r856_private_drain_once(&p.0).unwrap(),(128,0,160));
  p.python("update");
  let (copied,acked,left)=r856_private_drain_once(&p.0).unwrap();
  assert_eq!((copied,acked,left),(33,128,33));
  let (copied,acked,left)=r856_private_drain_once(&p.0).unwrap();
  assert_eq!((copied,acked,left),(0,33,0));
  let (src,dst,v)=p.handles();
  assert_eq!(r800_streaming_original_parity(&src,&dst,&v),Ok(1));
  assert_eq!(r366_query_num(&src,"SELECT latest_seq FROM mirror_control"),Ok(162));
 }
 #[test]fn r858_corrupt_original_outbox_tail_fails_before_any_db2_mutation(){
  let p=Private::new();p.ready();p.python("batch160");
  let (src,dst,_)=p.handles();
  exec(&src,"UPDATE mirror_outbox SET payload_hash='tampered' WHERE seq=150").unwrap();
  assert!(r856_private_drain_once(&p.0).is_err());
  assert_eq!(r366_query_num(&dst,"SELECT applied_seq FROM mirror_meta"),Ok(1));
  assert_eq!(r366_query_num(&src,"SELECT cleanup_through FROM mirror_control"),Ok(1));
 }
 #[test]fn r859_bounded_private_worker_never_enters_production_paths(){
  assert!(r856_private_drain_once(
   Path::new("/home/tmpadmin/spotbot/data")).is_err());
  assert!(r856_private_drain_bounded(Path::new("/tmp"),0).is_err());
  assert!(r856_private_drain_bounded(Path::new("/tmp"),257).is_err());
 }
}
