// Astra P0-01 R841-R844: actual SIGKILL of ONLY a Rust test child in
// private 0700 /tmp original-shaped DB1/DB2. Production services untouched.
#[cfg(test)]
mod r841_tests {
 use super::*;
 use crate::{Book,RawFrame,Update,Venue};
 use std::os::unix::fs::DirBuilderExt;
 use std::os::unix::process::ExitStatusExt;
 use std::process::Command;
 struct Private(PathBuf);
 impl Private {
  fn new()->Self{
   let mut p=std::env::temp_dir();
   let n=std::time::SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
   p.push(format!("astra_r671_file_{}_{}_r841",std::process::id(),n));
   fs::DirBuilder::new().mode(0o700).create(&p).unwrap();
   drop(r756_initialize(&p).unwrap());
   Self(p)
  }
 }
 impl Drop for Private {fn drop(&mut self){let _=fs::remove_dir_all(&self.0);}}
 fn fixture(i:u64)->(RawFrame,Update){
  let venue=Venue::OkxSpot;
  let received_ns=1_000_000_000_u64+i*10_000_000;
  let received_wall_ms=1_800_000_000_000_i64+i as i64*10;
  let raw=format!("{{\"source\":\"private_r841\",\"seq\":{i}}}");
  let f=RawFrame{source:None,venue,epoch:1,received_ns,received_wall_ms,raw};
  let book=Book{bids:vec![(101.0,1.0)],asks:vec![(102.0,1.0)],
   received_ns,received_wall_ms,exchange_ts_ms:Some(received_wall_ms),
   changed_ns:received_ns,epoch:1,sequence:Some(i as i64)};
  (f,Update{base:"BTC".into(),venue,book})
 }
 fn open(root:&Path)->(Db,Db){
  (r671_open(root,"r671-private-db1.sqlite",false).unwrap(),
   r671_open(root,"r671-private-db2.sqlite",false).unwrap())
 }
 fn child_only(phase:&str)->bool{
  if std::env::var("SPOTARB_R841_CHILD_PHASE").ok().as_deref()!=Some(phase){
   return false
  }
  let path=PathBuf::from(std::env::var("SPOTARB_R841_ROOT").unwrap());
  let (src,dst)=open(&path);
  let v=r586_oracle();
  let items=(1..=16).map(fixture).collect::<Vec<_>>();
  let operations=items.iter().enumerate().map(|(i,(frame,update))|
   r824_original_paper_op(&v,"sigkill",1,i,items.len(),frame,update)
    .unwrap()).collect::<Vec<_>>();
  if phase=="BEFORE_DB1"{
   let tx=Tx::begin(&src).unwrap();
   r586_apply_original_operation(&src,&v,&operations[0]).unwrap();
   assert_eq!(r366_query_num(&src,"SELECT count(*) FROM events"),Ok(1));
   std::mem::forget(tx); // child deliberately dies before SQLite COMMIT.
  }else {
   assert_eq!(r830_incremental_original_append(
    &path,&src,&dst,&v,&operations),Ok(1));
   if phase=="AFTER_DB2"||phase=="AFTER_ACK"{
    assert_eq!(r805_replay_window(&path,&src,&dst,&v,32),Ok(1));
   }
   if phase=="AFTER_ACK"{
    assert_eq!(r805_cleanup_window(&path,&src,&dst,&v),Ok((1,0)));
   }
  }
  unsafe{libc::raise(libc::SIGKILL);}
  panic!("private SIGKILL child survived");
 }
 fn kill_child(root:&Path,phase:&str,test_name:&str){
  let exe=std::env::current_exe().unwrap();
  let output=Command::new(exe).args(["--exact",test_name,"--nocapture"])
   .env("SPOTARB_R841_CHILD_PHASE",phase)
   .env("SPOTARB_R841_ROOT",root)
   .output().unwrap();
  assert_eq!(output.status.signal(),Some(libc::SIGKILL),
   "R841: child did not terminate by SIGKILL: {:?}",output.status.code());
 }
 fn checked_resume(root:&Path,before:u64,applied:u64,cleaned:u64){
  let (src,dst)=open(root);
  assert_eq!(r366_query_num(&src,
   "SELECT latest_seq FROM mirror_control"),Ok(before));
  assert_eq!(r366_query_num(&dst,
   "SELECT applied_seq FROM mirror_meta"),Ok(applied));
  assert_eq!(r366_query_num(&src,
   "SELECT cleanup_through FROM mirror_control"),Ok(cleaned));
  let (mut owner,n)=r834_recover_private_owner(root,"sigkill",0,1,800).unwrap();
  assert_eq!(n,before*16);
  if before==0{
   assert!(owner.snapshot_venue("BTC",Venue::OkxSpot).is_none());
  }else{
   assert_eq!(owner.snapshot_venue("BTC",Venue::OkxSpot)
    .unwrap().sequence,Some(16));
  }
  let v=r586_oracle();
  let replay=r805_replay_window(root,&src,&dst,&v,32).unwrap();
  assert_eq!(replay,before-applied);
  let (acked,left)=r805_cleanup_window(root,&src,&dst,&v).unwrap();
  assert_eq!(left,0);
  assert_eq!(acked,before-cleaned);
  assert_eq!(r800_streaming_original_parity(&src,&dst,&v),Ok(n));
  drop(src);drop(dst);
  let mut writer=R819OriginalPaperHistory::new(root,"sigkill").unwrap();
  let next=if before==0{(1..=16).map(fixture).collect::<Vec<_>>()}
   else{(17..=32).map(fixture).collect::<Vec<_>>()};
  assert_eq!(r824_commit_prepared_batch(&mut writer,&mut owner,&next),
    Ok((before+1,16)));
  assert_eq!(r366_query_num(&writer.source,
   "SELECT COUNT(*) FROM events"),Ok(n+16));
  assert_eq!(r805_replay_window(root,&writer.source,&writer.sink,&v,32),Ok(1));
  assert_eq!(r805_cleanup_window(root,&writer.source,&writer.sink,&v),Ok((1,0)));
  assert_eq!(r800_streaming_original_parity(
   &writer.source,&writer.sink,&v),Ok(n+16));
 }
 #[test]fn r841_sigkill_before_db1_commit_rolls_back_unpublished_events(){
  if child_only("BEFORE_DB1"){return}
  let p=Private::new();
  kill_child(&p.0,"BEFORE_DB1",
   "native_isolated_sqlite_mirror_r147_review::r841_tests::r841_sigkill_before_db1_commit_rolls_back_unpublished_events");
  checked_resume(&p.0,0,0,0);
 }
 #[test]fn r842_sigkill_after_durable_db1_before_owner_publication(){
  if child_only("AFTER_DB1"){return}
  let p=Private::new();
  kill_child(&p.0,"AFTER_DB1",
   "native_isolated_sqlite_mirror_r147_review::r841_tests::r842_sigkill_after_durable_db1_before_owner_publication");
  checked_resume(&p.0,1,0,0);
 }
 #[test]fn r843_sigkill_after_db2_commit_before_source_ack(){
  if child_only("AFTER_DB2"){return}
  let p=Private::new();
  kill_child(&p.0,"AFTER_DB2",
   "native_isolated_sqlite_mirror_r147_review::r841_tests::r843_sigkill_after_db2_commit_before_source_ack");
  checked_resume(&p.0,1,1,0);
 }
 #[test]fn r844_sigkill_after_ack_before_owner_publication(){
  if child_only("AFTER_ACK"){return}
  let p=Private::new();
  kill_child(&p.0,"AFTER_ACK",
   "native_isolated_sqlite_mirror_r147_review::r841_tests::r844_sigkill_after_ack_before_owner_publication");
  checked_resume(&p.0,1,1,1);
 }
}
