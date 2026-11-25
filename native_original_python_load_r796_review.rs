// Astra P0-01: bounded ORIGINAL Python producer pressure and subprocess
// SIGKILL recovery through Rust ORIGINAL 17-table FILE SQLite receiver.
// Standalone research only. The active PAPER and production DBs are never used.
#[cfg(test)]
mod r796_tests {
 use super::*;
 use std::os::unix::fs::DirBuilderExt;
 use std::os::unix::process::ExitStatusExt;
 use std::process::Command;
 use serde_json::Value;
 struct Root(PathBuf);
 impl Root {
  fn new()->Self{
   let mut path=std::env::temp_dir();
   let stamp=std::time::SystemTime::now()
     .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
   path.push(format!("astra_r671_file_{}_{}_r796",std::process::id(),stamp));
   fs::DirBuilder::new().mode(0o700).create(&path).unwrap();
   Self(path)
  }
 }
 impl Drop for Root {fn drop(&mut self){let _=fs::remove_dir_all(&self.0);}}
 fn python(root:&Path,mode:&str){
  let script=Path::new(env!("CARGO_MANIFEST_DIR"))
   .join("offline_original_python_producer_r786.py");
  let outcome=Command::new("/usr/bin/python3")
   .arg(script).arg(root).arg(mode).output().unwrap();
  assert!(outcome.status.success(),
    "bounded original Python producer failed: {:?}",outcome.status.code());
 }
 fn open(root:&Path)->(Db,Db){
  (r671_open(root,"r671-private-db1.sqlite",false).unwrap(),
   r671_open(root,"r671-private-db2.sqlite",false).unwrap())
 }
 fn setup()->(Root,Db,Db,Value){
  let root=Root::new();
  let (src,dst,v)=r756_initialize(&root.0).unwrap();
  python(&root.0,"insert");
  assert_eq!(r756_replay(&root.0,&src,&dst,&v,8),Ok(1));
  assert_eq!(r756_cleanup(&root.0,&src,&dst,&v),Ok((1,0)));
  assert_eq!(r366_query_num(&src,"SELECT count(*) FROM mirror_outbox").unwrap(),0);
  (root,src,dst,v)
 }
 fn drain(root:&Path,src:&Db,dst:&Db,v:&Value)->u64{
  let mut done=0;
  for _ in 0..18 {
   let count=r756_replay(root,src,dst,v,32).unwrap();
   assert!(count<=32);
   done+=count;
   if count==0{break}
  }
  assert_eq!(r366_query_num(src,"SELECT latest_seq FROM mirror_control").unwrap(),
             r366_query_num(dst,"SELECT applied_seq FROM mirror_meta").unwrap());
  done
 }
 fn cleanup(root:&Path,src:&Db,dst:&Db,v:&Value)->(u64,u64){
  let (a,left)=r756_cleanup(root,src,dst,v).unwrap();
  if left==0{return (a,0)}
  let (b,final_left)=r756_cleanup(root,src,dst,v).unwrap();
  (a+b,final_left)
 }
 fn child(root:&Path,phase:&str,name:&str){
  let current=std::env::current_exe().unwrap();
  let result=Command::new(current).args(["--exact",name,"--nocapture"])
    .env("SPOTARB_R796_CHILD_PHASE",phase)
    .env("SPOTARB_R796_PRIVATE_ROOT",root).output().unwrap();
  assert_eq!(result.status.signal(),Some(libc::SIGKILL),
   "only the isolated child may terminate itself with SIGKILL");
 }
 fn maybe_child(expected:&str)->bool{
  if std::env::var("SPOTARB_R796_CHILD_PHASE").ok().as_deref()!=Some(expected){
   return false
  }
  let root=PathBuf::from(std::env::var("SPOTARB_R796_PRIVATE_ROOT").unwrap());
  let (src,dst)=open(&root);
  let v=r586_oracle();
  let (_,_,rows)=r756_snapshot(&root,&src,&dst).unwrap();
  assert_eq!(rows.len(),160);
  if expected=="MID_DB2"{
   let payload:Value=serde_json::from_str(rows[0]["payload_json"].as_str().unwrap()).unwrap();
   let tx=Tx::begin(&dst).unwrap();
   r586_apply_original_operation(&dst,&v,&payload["operations"][0]).unwrap();
   assert_eq!(r366_query_num(&dst,"SELECT applied_seq FROM mirror_meta").unwrap(),1);
   std::mem::forget(tx); // SIGKILL is the deliberate rollback fault
  } else if expected=="AFTER_DB2" {
   assert_eq!(r756_replay(&root,&src,&dst,&v,32),Ok(32));
   assert_eq!(r366_query_num(&dst,"SELECT applied_seq FROM mirror_meta").unwrap(),33);
  } else {panic!("unrecognized private SIGKILL test phase")}
  unsafe{libc::raise(libc::SIGKILL);}
  panic!("private SIGKILL child unexpectedly survived")
 }
 #[test]fn r796_original_python_160_transactions_bounded_replay_cleanup(){
  let (root,src,dst,v)=setup();
  python(&root.0,"batch160");
  let (ctrl,_,rows)=r756_snapshot(&root.0,&src,&dst).unwrap();
  assert_eq!(rows.len(),160);
  assert_eq!(ctrl["retained_records"],160);
  assert_eq!(ctrl["latest_seq"],161);
  assert_eq!(drain(&root.0,&src,&dst,&v),160);
  assert_eq!(cleanup(&root.0,&src,&dst,&v),(160,0));
  let (ctrl,meta,left)=r756_snapshot(&root.0,&src,&dst).unwrap();
  assert_eq!(left.len(),0);
  assert_eq!(ctrl["retained_bytes"],0);
  assert_eq!(meta["applied_seq"],161);
  assert_eq!(r366_query_num(&dst,
    "SELECT COUNT(*) FROM pragma_foreign_key_check").unwrap(),0);
 }
 #[test]fn r797_source_writes_after_partial_rust_replay_do_not_lose_ack(){
  let (root,src,dst,v)=setup();
  python(&root.0,"batch160");
  assert_eq!(r756_replay(&root.0,&src,&dst,&v,32),Ok(32));
  python(&root.0,"update");
  assert_eq!(r756_cleanup(&root.0,&src,&dst,&v),
   Err(EventOutboxR366Error::Conflict));
  assert_eq!(drain(&root.0,&src,&dst,&v),129);
  assert_eq!(cleanup(&root.0,&src,&dst,&v),(161,0));
  assert_eq!(r366_query_num(&src,"SELECT latest_seq FROM mirror_control").unwrap(),162);
 }
 #[test]fn r798_sigkill_inside_rust_db2_transaction_then_original_replay(){
  if maybe_child("MID_DB2"){return}
  let (root,src,dst,v)=setup();
  python(&root.0,"batch160");
  drop(src);drop(dst);
  child(&root.0,"MID_DB2",
    "native_isolated_sqlite_mirror_r147_review::r796_tests::r798_sigkill_inside_rust_db2_transaction_then_original_replay");
  let (src,dst)=open(&root.0);
  assert_eq!(r366_query_num(&dst,"SELECT applied_seq FROM mirror_meta").unwrap(),1);
  let mut q=Stmt::new(&dst,"SELECT value FROM metadata").unwrap();
  assert_eq!(q.step().unwrap(),SQLITE_ROW);
  assert_eq!(q.strval(0).unwrap(),"from_original_python");
  assert_eq!(drain(&root.0,&src,&dst,&v),160);
  assert_eq!(cleanup(&root.0,&src,&dst,&v),(160,0));
 }
 #[test]fn r799_sigkill_after_durable_db2_before_source_ack(){
  if maybe_child("AFTER_DB2"){return}
  let (root,src,dst,v)=setup();
  python(&root.0,"batch160");
  drop(src);drop(dst);
  child(&root.0,"AFTER_DB2",
    "native_isolated_sqlite_mirror_r147_review::r796_tests::r799_sigkill_after_durable_db2_before_source_ack");
  let (src,dst)=open(&root.0);
  assert_eq!(r366_query_num(&dst,"SELECT applied_seq FROM mirror_meta").unwrap(),33);
  assert_eq!(r366_query_num(&src,"SELECT cleanup_through FROM mirror_control").unwrap(),1);
  assert_eq!(drain(&root.0,&src,&dst,&v),128);
  assert_eq!(cleanup(&root.0,&src,&dst,&v),(160,0));
 }
}
