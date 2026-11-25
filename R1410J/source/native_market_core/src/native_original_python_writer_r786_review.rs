// Astra P0-01 R786+: ACTUAL legacy Python ai.mirror_outbox.record()
// writes a STRICTLY ISOLATED ORIGINAL 17-table SQLite DB1, Rust applies
// its Python-produced frame to ORIGINAL SQL DB2 and ACKs.
// Cross-language forward replay evidence only, no installed PAPER linkage.
#[cfg(test)]
mod r786_tests {
 use super::*;
 use std::os::unix::fs::DirBuilderExt;
 use std::process::Command;
 use serde_json::Value;
 struct Local(PathBuf);
 impl Local {
  fn new()->Self {
   let mut p=std::env::temp_dir();
   let nonce=std::time::SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
   p.push(format!("astra_r671_file_{}_{}_py",std::process::id(),nonce));
   fs::DirBuilder::new().mode(0o700).create(&p).unwrap();
   Self(p)
  }
 }
 impl Drop for Local{
  fn drop(&mut self){let _=fs::remove_dir_all(&self.0);}
 }
 fn pair()->(Local,Db,Db,Value){
  let dir=Local::new();
  let (s,d,v)=r756_initialize(&dir.0).unwrap();
  (dir,s,d,v)
 }
 fn py(dir:&Path,method:&str){
  let script=Path::new(env!("CARGO_MANIFEST_DIR"))
   .join("offline_original_python_producer_r786.py");
  let result=Command::new("/usr/bin/python3")
   .arg(script).arg(dir).arg(method).output().unwrap();
  assert!(result.status.success(),"original Python source writer failed; stderr={} stdout={}",String::from_utf8_lossy(&result.stderr),String::from_utf8_lossy(&result.stdout));
 }
 fn count(db:&Db,table:&str)->u64{
  r366_query_num(db,&format!("SELECT COUNT(*) FROM {table}")).unwrap()
 }
 #[test]fn r786_actual_python_record_to_rust_db2_and_ack(){
  let (root,s,d,v)=pair();
  py(&root.0,"insert");
  let (control,meta,rows)=r756_snapshot(&root.0,&s,&d).unwrap();
  assert_eq!(control["retained_records"],1);
  assert_eq!(meta["applied_seq"],0);
  assert_eq!(rows.len(),1);
  assert_eq!(rows[0]["tx_kind"],"r786_python_real_module");
  assert_eq!(rows[0]["tx_id"].as_str().unwrap().len(),35);
  assert_eq!(count(&s,"metadata"),1);
  assert_eq!(count(&d,"metadata"),0);
  assert_eq!(r756_replay(&root.0,&s,&d,&v,8),Ok(1));
  assert_eq!(count(&d,"metadata"),1);
  assert_eq!(r756_cleanup(&root.0,&s,&d,&v),Ok((1,0)));
  assert_eq!(count(&s,"mirror_outbox"),0);
  assert_eq!(r756_snapshot(&root.0,&s,&d).unwrap().0["cleanup_through"],1);
 }
 #[test]fn r787_actual_python_transaction_rollback_preserves_both(){
  let (root,s,d,v)=pair();
  py(&root.0,"insert");
  py(&root.0,"rollback");
  let (control,_,rows)=r756_snapshot(&root.0,&s,&d).unwrap();
  assert_eq!(control["latest_seq"],1);
  assert_eq!(control["retained_records"],1);
  assert_eq!(rows.len(),1);
  assert_eq!(r756_replay(&root.0,&s,&d,&v,8),Ok(1));
  assert_eq!(r756_cleanup(&root.0,&s,&d,&v),Ok((1,0)));
 }
 #[test]fn r788_two_actual_python_transactions_rust_bounded_batch(){
  let (root,s,d,v)=pair();
  py(&root.0,"insert");
  py(&root.0,"update");
  let (_,_,rows)=r756_snapshot(&root.0,&s,&d).unwrap();
  assert_eq!(rows.len(),2);
  assert_eq!(rows[0]["prev_seq"],0);
  assert_eq!(rows[1]["prev_seq"],1);
  assert_eq!(r756_replay(&root.0,&s,&d,&v,1),Ok(1));
  assert_eq!(r756_cleanup(&root.0,&s,&d,&v),
   Err(EventOutboxR366Error::Conflict));
  assert_eq!(r756_replay(&root.0,&s,&d,&v,1),Ok(1));
  assert_eq!(r756_cleanup(&root.0,&s,&d,&v),Ok((2,0)));
  assert_eq!(count(&d,"metadata"),1);
 }
 #[test]fn r789_python_then_rust_reopen_before_cleanup_is_idempotent(){
  let (root,s,d,v)=pair();
  py(&root.0,"insert");
  r756_replay(&root.0,&s,&d,&v,1).unwrap();
  drop(s);drop(d);
  let s=r671_open(&root.0,"r671-private-db1.sqlite",false).unwrap();
  let d=r671_open(&root.0,"r671-private-db2.sqlite",false).unwrap();
  assert_eq!(r756_replay(&root.0,&s,&d,&v,1),Ok(0));
  assert_eq!(r756_cleanup(&root.0,&s,&d,&v),Ok((1,0)));
 }
 include!("native_r1021_r1033_original_mixed_collector_tests.rs");
 include!("native_r1034_r1039_all17_original_python_tests.rs");
 include!("native_r1056_original_python_mixed_soak_tests.rs");
}
