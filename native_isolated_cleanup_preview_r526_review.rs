// R526-R535: fail-closed ACK-scoped cleanup PREVIEW for private two-file model.
// NEVER deletes production or synthetic source outbox rows; R376 retains history.
#[derive(Debug,Clone,PartialEq,Eq)]
struct R526CleanupPreview{
 ack_seq:u64,through_seq:u64,selected:Vec<u64>,
 retained_bytes:u64,bytes_selected:u64,bootstrap_required:bool,
 deletion_authorized:bool
}
impl R506IsolatedReplica{
 fn r526_cleanup_preview(&self,source:&IsolatedEventOutboxR366)
  ->Result<R526CleanupPreview,EventOutboxR366Error>{
  let witness=self.inspect(source)?;
  // Both source AND receiver audit + source/receiver hash checks required.
  let db=r366_open(&source.path)?;
  let mut control=Stmt::new(&db,"SELECT state,retained_bytes
     FROM mirror_control WHERE id=1").map_err(r366_error)?;
  if control.step().map_err(r366_error)?!=SQLITE_ROW{
   return Err(EventOutboxR366Error::Corrupt)
  }
  let state=control.strval(0).map_err(r366_error)?;
  let retained=control.num(1).map_err(r366_error)?;
  if control.step().map_err(r366_error)?!=SQLITE_DONE||
    !["BOOTSTRAP_REQUIRED","NORMAL","REBUILD_REQUIRED"].contains(&state.as_str()){
    return Err(EventOutboxR366Error::Corrupt)
  }
  drop(control);
  let mut q=Stmt::new(&db,"SELECT seq,payload_bytes FROM mirror_outbox
   WHERE seq<=?1 ORDER BY seq LIMIT 128").map_err(r366_error)?;
  q.number(1,witness.ack_seq).map_err(r366_error)?;
  let mut selected=Vec::<u64>::new();
  let mut bytes=0_u64;
  let mut expected=1_u64;
  loop{
   match q.step().map_err(r366_error)?{
    SQLITE_DONE=>break,
    SQLITE_ROW=>{
     let seq=q.num(0).map_err(r366_error)?;
     let n=q.num(1).map_err(r366_error)?;
     if seq!=expected||n<384{return Err(EventOutboxR366Error::Corrupt)}
     expected+=1;
     bytes=bytes.checked_add(n).ok_or(EventOutboxR366Error::Corrupt)?;
     selected.push(seq);
    },
    _=>return Err(EventOutboxR366Error::Corrupt)
   }
  }
  if bytes>retained||selected.len()>128{
   return Err(EventOutboxR366Error::Corrupt)
  }
  Ok(R526CleanupPreview{
   ack_seq:witness.ack_seq,through_seq:selected.last().copied().unwrap_or(0),
   selected,retained_bytes:retained,bytes_selected:bytes,
   bootstrap_required:state!="NORMAL",deletion_authorized:false
  })
 }
}
#[cfg(test)]
mod r526_tests{
 use super::*;
 use super::r366_tests::{fixture_root,record};
 use serde_json::Value;
 const T:u64=1_792_660_000_000;
 fn oracle()->Value{
  serde_json::from_str(include_str!(
    "../config/native_r526_python_mirror_cleanup_oracle.json")).unwrap()
 }
 fn setup()->(PathBuf,IsolatedEventOutboxR366,R506IsolatedReplica){
  let root=fixture_root();
  let db1=IsolatedEventOutboxR366::initialize(&root).unwrap();
  let db2=R506IsolatedReplica::initialize(&root).unwrap();
  (root,db1,db2)
 }
 fn append(db1:&IsolatedEventOutboxR366,i:u64){
  db1.append(&record(&format!("r526_event_{i:03}"),"decisions"),
   T+i,false).unwrap();
 }
 #[test]fn r526_original_python_cleanup_batch_limit_128(){
  let v=oracle();
  assert_eq!(v["cleanup_batch"],128);
  assert_eq!(v["before"]["latest_seq"],130);
  assert_eq!(v["first"]["deleted"],128);
  assert_eq!(v["first"]["control"]["cleanup_through"],128);
  assert_eq!(v["second"]["deleted"],1);
  assert_eq!(v["third"]["deleted"],1);
  assert_eq!(v["fourth_deleted"],0);
  assert_eq!(v["third"]["control"]["retained_bytes"],0);
 }
 #[test]fn r527_python_cleanup_transaction_rollback_proven(){
  let v=oracle();
  assert_eq!(v["rollback_count"],128);
  assert_eq!(v["no_transaction_error"],
   "cleanup requires an open transaction");
  assert_eq!(v["before"]["retained_records"],130);
  assert_eq!(v["first"]["control"]["retained_records"],2);
  assert_eq!(v["second"]["control"]["retained_records"],1);
  assert_eq!(v["third"]["control"]["retained_records"],0);
 }
 #[test]fn r528_python_rebuild_suppresses_feed(){
  let v=oracle();
  assert_eq!(v["rebuild_before"],v["rebuild_after"]);
  assert_eq!(v["rebuild_after"]["state"],"REBUILD_REQUIRED");
  assert!(v["suppressed"].is_null());
  assert_eq!(v["initial"]["state"],"BOOTSTRAP_REQUIRED");
 }
 #[test]fn r529_python_oversized_frame_forces_rebuild(){
  let v=oracle();
  assert_eq!(v["max_tx_bytes"],4*1024*1024);
  assert_eq!(v["oversize"]["state"],"REBUILD_REQUIRED");
  assert_eq!(v["oversize"]["reason"],"OUTBOX_TRANSACTION_TOO_LARGE");
  assert_eq!(v["oversize"]["failure_seq"],130);
  assert!(v["oversize_suppressed"].is_null());
 }
 #[test]fn r530_no_ack_means_zero_cleanup_candidates(){
  let (root,db1,db2)=setup();append(&db1,1);
  let p=db2.r526_cleanup_preview(&db1).unwrap();
  assert_eq!((p.ack_seq,p.through_seq,p.bytes_selected),(0,0,0));
  assert!(p.selected.is_empty()&&p.bootstrap_required);
  assert!(!p.deletion_authorized);
  drop(db1);drop(db2);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r531_db2_receipt_without_db1_ack_cannot_cleanup(){
  let (root,db1,db2)=setup();append(&db1,1);
  db2.deliver(&db1,1,false).unwrap();
  let p=db2.r526_cleanup_preview(&db1).unwrap();
  assert_eq!(p.ack_seq,0);
  assert!(p.selected.is_empty()&&!p.deletion_authorized);
  drop(db1);drop(db2);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r532_ack_limits_candidates_to_verified_prefix(){
  let (root,db1,db2)=setup();
  append(&db1,1);append(&db1,2);
  db2.deliver(&db1,1,false).unwrap();
  db2.acknowledge(&db1,1,false).unwrap();
  let p=db2.r526_cleanup_preview(&db1).unwrap();
  assert_eq!(p.selected,vec![1]);
  assert_eq!((p.ack_seq,p.through_seq),(1,1));
  assert!(p.retained_bytes>=p.bytes_selected&&p.bytes_selected>=384);
  assert!(!p.deletion_authorized);
  assert_eq!(db1.audit().unwrap().outbox,2);
  drop(db1);drop(db2);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r533_reopen_and_late_ack_extends_safe_preview(){
  let (root,db1,db2)=setup();
  append(&db1,1);append(&db1,2);
  db2.deliver(&db1,1,false).unwrap();
  db2.acknowledge(&db1,1,false).unwrap();
  db2.deliver(&db1,2,false).unwrap();
  drop(db1);drop(db2);
  let db1=IsolatedEventOutboxR366::reopen(&root).unwrap();
  let db2=R506IsolatedReplica::reopen(&root).unwrap();
  assert_eq!(db2.r526_cleanup_preview(&db1).unwrap().selected,vec![1]);
  db2.acknowledge(&db1,2,false).unwrap();
  let p=db2.r526_cleanup_preview(&db1).unwrap();
  assert_eq!(p.selected,vec![1,2]);
  assert_eq!(p.through_seq,2);
  assert!(!p.deletion_authorized);
  drop(db1);drop(db2);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r534_tampered_db2_blocks_preview_before_any_delete(){
  let (root,db1,db2)=setup();append(&db1,1);
  db2.deliver(&db1,1,false).unwrap();
  db2.acknowledge(&db1,1,false).unwrap();
  let db=r506_db2_open(&root).unwrap();
  exec(&db,"UPDATE sink_delivery SET payload_hash='forged' WHERE seq=1")
   .unwrap();
  drop(db);
  assert_eq!(db2.r526_cleanup_preview(&db1),
   Err(EventOutboxR366Error::Corrupt));
  assert_eq!(db1.audit().unwrap().outbox,1);
  drop(db1);drop(db2);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r535_normal_flag_still_has_no_real_deletion_authority(){
  let (root,db1,db2)=setup();append(&db1,1);
  db2.deliver(&db1,1,false).unwrap();
  db2.acknowledge(&db1,1,false).unwrap();
  let db=r366_open(&db1.path).unwrap();
  exec(&db,"UPDATE mirror_control SET state='NORMAL' WHERE id=1").unwrap();
  drop(db);
  // Synthetic R376 audit recognizes an unapproved state change.
  // It must reject the cleanup preview rather than grant authority.
  assert_eq!(db2.r526_cleanup_preview(&db1),
    Err(EventOutboxR366Error::Corrupt));
  assert_eq!(db1.audit(),Err(EventOutboxR366Error::Corrupt));
  drop(db1);drop(db2);fs::remove_dir_all(root).unwrap();
 }
}
