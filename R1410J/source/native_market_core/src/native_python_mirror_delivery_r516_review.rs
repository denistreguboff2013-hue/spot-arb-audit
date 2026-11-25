// R516–R525: original Python mirror protocol oracle and isolated backlog replay.
#[cfg(test)]
mod r516_tests {
 use super::*;
 use super::r366_tests::{fixture_root,record};
 use serde_json::{json,Value};
 const T:u64=1_792_660_000_000;
 fn reference()->Value{
  serde_json::from_str(include_str!(
   "../config/native_r516_python_mirror_feed_oracle.json")).unwrap()
 }
 fn setup()->(PathBuf,IsolatedEventOutboxR366,R506IsolatedReplica){
  let root=fixture_root();
  let db1=IsolatedEventOutboxR366::initialize(&root).unwrap();
  let db2=R506IsolatedReplica::initialize(&root).unwrap();
  (root,db1,db2)
 }
 fn append(db1:&IsolatedEventOutboxR366,number:u64){
  db1.append(&record(&format!("r516_event_{number:03}"),"decisions"),
   T+number,false).unwrap();
 }
 #[test]fn r516_original_python_canonical_and_source_hash_parity(){
  let source=reference();
  assert_eq!(source["protocol_version"],1);
  assert_eq!(source["schema_generation"],1);
  let canonical=r386_python_mirror_canonical(&source["frame"]).unwrap();
  assert_eq!(canonical,source["canonical"]);
  assert_eq!(r366_sha(&canonical),source["sha256"]);
  assert_eq!(source["frame"]["tx_kind"],"event_append");
  assert_eq!(source["operations"].as_array().unwrap().len(),3);
 }
 #[test]fn r517_original_python_after_image_and_budget_bytes(){
  let o=reference();
  let canonical=o["canonical"].as_str().unwrap();
  assert_eq!(o["payload_bytes"].as_u64(),Some(canonical.len() as u64+384));
  assert_eq!(o["record"]["payload_hash"],o["sha256"]);
  assert_eq!(o["record"]["prev_seq"],0);
  assert_eq!(o["control"]["retained_records"],1);
  assert_eq!(o["control"]["retained_bytes"],o["payload_bytes"]);
  assert_eq!(o["control"]["state"],"BOOTSTRAP_REQUIRED");
  for op in o["operations"].as_array().unwrap(){
   let content=r386_python_mirror_canonical(&op["row"]).unwrap();
   assert_eq!(op["after_hash"],r366_sha(&content));
   assert_eq!(op["op"],"insert");
  }
 }
 #[test]fn r518_backlog_16_durable_source_transactions_catches_up(){
  let (root,db1,db2)=setup();
  for seq in 1..=16{append(&db1,seq)}
  let start=db2.inspect(&db1).unwrap();
  assert_eq!((start.source_seq,start.sink_seq,start.ack_seq),(16,0,0));
  for seq in 1..=16{
   db2.deliver(&db1,seq,false).unwrap();
   db2.acknowledge(&db1,seq,false).unwrap();
  }
  let done=db2.inspect(&db1).unwrap();
  assert_eq!((done.source_seq,done.sink_seq,done.ack_seq),(16,16,16));
  let sink=r506_db2_open(&root).unwrap();
  assert_eq!(r366_query_num(&sink,
   "SELECT COUNT(*) FROM sink_delivery").unwrap(),16);
  assert_eq!(r366_query_num(&sink,
   "SELECT COUNT(*) FROM sink_rows").unwrap(),32);
  drop(sink);drop(db1);drop(db2);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r519_lost_ack_then_reopen_sink_ahead_source_is_safe(){
  let (root,db1,db2)=setup();
  append(&db1,1);db2.deliver(&db1,1,false).unwrap();
  assert_eq!(db2.acknowledge(&db1,1,true),
    Err(EventOutboxR366Error::Interrupted));
  drop(db1);drop(db2);
  let db1=IsolatedEventOutboxR366::reopen(&root).unwrap();
  let db2=R506IsolatedReplica::reopen(&root).unwrap();
  assert_eq!((db2.inspect(&db1).unwrap().sink_seq,
    db2.inspect(&db1).unwrap().ack_seq),(1,0));
  assert_eq!(db2.deliver(&db1,1,false),
    Err(EventOutboxR366Error::Duplicate));
  db2.acknowledge(&db1,1,false).unwrap();
  assert_eq!(db2.inspect(&db1).unwrap().ack_seq,1);
  drop(db1);drop(db2);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r520_source_newer_than_sink_while_processing_backlog(){
  let (root,db1,db2)=setup();
  for seq in 1..=6{append(&db1,seq)}
  for seq in 1..=3{db2.deliver(&db1,seq,false).unwrap();}
  let mid=db2.inspect(&db1).unwrap();
  assert_eq!((mid.source_seq,mid.sink_seq,mid.ack_seq),(6,3,0));
  for seq in 7..=8{append(&db1,seq)}
  for seq in 4..=8{db2.deliver(&db1,seq,false).unwrap();}
  for seq in 1..=8{db2.acknowledge(&db1,seq,false).unwrap();}
  assert_eq!((db2.inspect(&db1).unwrap().source_seq,
   db2.inspect(&db1).unwrap().sink_seq,
   db2.inspect(&db1).unwrap().ack_seq),(8,8,8));
  drop(db1);drop(db2);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r521_tampered_source_outbox_hash_never_delivered(){
  let (root,db1,db2)=setup();append(&db1,1);
  let source=r366_open(&db1.path).unwrap();
  exec(&source,"UPDATE mirror_outbox SET payload_hash='forged' WHERE seq=1").unwrap();
  drop(source);
  assert_eq!(db2.deliver(&db1,1,false),
    Err(EventOutboxR366Error::Corrupt));
  assert_eq!(db2.audit().unwrap(),0);
  drop(db1);drop(db2);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r522_corrupted_sink_applied_seq_refused_to_replay(){
  let (root,db1,db2)=setup();append(&db1,1);
  db2.deliver(&db1,1,false).unwrap();
  let sink=r506_db2_open(&root).unwrap();
  exec(&sink,"UPDATE sink_control SET applied_seq=99 WHERE id=1").unwrap();
  drop(sink);
  assert_eq!(db2.audit(),Err(EventOutboxR366Error::Corrupt));
  assert_eq!(db2.deliver(&db1,1,false),
    Err(EventOutboxR366Error::Corrupt));
  drop(db1);drop(db2);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r523_missing_delivered_receipt_detected_after_reopen(){
  let (root,db1,db2)=setup();append(&db1,1);
  db2.deliver(&db1,1,false).unwrap();
  let sink=r506_db2_open(&root).unwrap();
  exec(&sink,"DELETE FROM sink_delivery WHERE seq=1").unwrap();
  drop(sink);
  assert_eq!(db2.audit(),Err(EventOutboxR366Error::Corrupt));
  assert!(matches!(R506IsolatedReplica::reopen(&root),
   Err(EventOutboxR366Error::Corrupt)));
  drop(db1);drop(db2);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r524_invalid_source_tx_kind_is_not_accepted(){
  let (root,db1,db2)=setup();append(&db1,1);
  let source=r366_open(&db1.path).unwrap();
  exec(&source,"UPDATE mirror_outbox SET tx_kind='retention' WHERE seq=1").unwrap();
  drop(source);
  assert_eq!(db2.deliver(&db1,1,false),
   Err(EventOutboxR366Error::Corrupt));
  assert_eq!(db2.audit().unwrap(),0);
  drop(db1);drop(db2);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r525_python_bootstrap_state_requires_external_certification(){
  let oracle=reference();
  assert_eq!(oracle["states"],json!([
    "BOOTSTRAP_REQUIRED","NORMAL","REBUILD_REQUIRED"]));
  assert_eq!(oracle["control"]["state"],"BOOTSTRAP_REQUIRED");
  let (root,db1,db2)=setup();append(&db1,1);
  db2.deliver(&db1,1,false).unwrap();
  db2.acknowledge(&db1,1,false).unwrap();
  assert!(db1.audit().unwrap().pending_bootstrap);
  assert!(!db2.inspect(&db1).unwrap().real_mirror);
  drop(db1);drop(db2);fs::remove_dir_all(root).unwrap();
 }
}
