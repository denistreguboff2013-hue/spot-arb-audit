// R932 Astra P0-01: test-only proof-by-induction, guarded PAPER events
// INSERT. Initial original 17-table parity is certified at EMPTY genesis;
// immutable original 17-table triggers and exact row-bound outbox receipts
// preserve that parity without a costly complete rescan at EVERY ACK.
// Final full parity must still be independently checked by the parent.
// Absolutely not for arbitrary historical Python 17-table mutations.
fn r932_seal(run:&str,schema:&str,generation:&str)
 ->Result<String,EventOutboxR366Error>{
 if run.is_empty()||run.len()>80||
  !run.bytes().all(|c|c.is_ascii_alphanumeric()||c==b'_'||c==b'-'){
  return Err(EventOutboxR366Error::Schema)
 }
 Ok(r366_sha(&r386_python_mirror_canonical(&serde_json::json!({
  "protocol":"r932-private-paper-empty-genesis-v1",
  "run":run,"schema_hash":schema,"feed_generation":generation
 }))?))
}
pub(super) fn r932_certify_private_empty_genesis(
 root:&Path,src:&Db,dst:&Db,v:&serde_json::Value,run:&str
)->Result<(),EventOutboxR366Error>{
 r924_verify_installed_guards(src,v)?;
 r924_verify_installed_guards(dst,v)?;
 let (h,c,m)=r805_read_head(root,src,dst)?;
 if h.latest!=0||h.applied!=0||h.cleaned!=0||h.retained!=0
  ||r366_query_num(src,"SELECT COUNT(*) FROM events")?!=0
  ||r366_query_num(dst,"SELECT COUNT(*) FROM events")?!=0
  ||r800_streaming_original_parity(src,dst,v)?!=0{
  return Err(EventOutboxR366Error::Conflict)
 }
 let schema=r756_schema_hash(src)?;
 let seal=r932_seal(run,&schema,r726_s(&c,"feed_generation")?)?;
 let tx=Tx::begin(dst).map_err(r366_error)?;
 if m["last_key_json"]!=serde_json::Value::Null{
  return Err(EventOutboxR366Error::Conflict)
 }
 exec(dst,&format!("UPDATE mirror_meta SET last_key_json={}
  WHERE id=1 AND applied_seq=0",r586_sql_literal(&serde_json::json!(seal))?))
  .map_err(r366_error)?;
 tx.commit().map_err(r366_error)?;
 Ok(())
}
pub(super) fn r932_guarded_incremental_prefix_ack(
 root:&Path,src:&Db,dst:&Db,v:&serde_json::Value,run:&str
)->Result<(u64,u64),EventOutboxR366Error>{
 r932_guarded_incremental_prefix_ack_impl(root,src,dst,v,run,false)
}
fn r932_guarded_incremental_prefix_ack_impl(
 root:&Path,src:&Db,dst:&Db,v:&serde_json::Value,run:&str,
 fingerprints:bool
)->Result<(u64,u64),EventOutboxR366Error>{
 r924_verify_installed_guards(src,v)?;
 r924_verify_installed_guards(dst,v)?;
 exec(src,"BEGIN DEFERRED").map_err(r366_error)?;
 let snapshot=R805SourceRead(src);
 let dest_tx=Tx::begin(dst).map_err(r366_error)?;
 let (h,c,m)=r805_read_head(root,src,dst)?;
 let seal=r932_seal(run,&r756_schema_hash(src)?,r726_s(&c,"feed_generation")?)?;
 if m["last_key_json"].as_str()!=Some(seal.as_str()){
  return Err(EventOutboxR366Error::Corrupt)
 }
 if h.applied<=h.cleaned{return Err(EventOutboxR366Error::Conflict)}
 if h.retained>64{return Err(EventOutboxR366Error::Pressure)}
 if h.latest.checked_mul(32)!=Some(r366_query_num(src,"SELECT COUNT(*) FROM events")?)
  ||h.applied.checked_mul(32)!=Some(r366_query_num(dst,"SELECT COUNT(*) FROM events")?){
  return Err(EventOutboxR366Error::Corrupt)
 }
 let (acked,bytes,rows,hash,old_fingerprints)=
  r948_retained_with_optional_fingerprints(
   src,&h,r726_s(&c,"feed_generation")?,run,fingerprints)?;
 if m["applied_hash"].as_str()!=Some(hash.as_str())
  ||r366_query_num(dst,"SELECT COUNT(*) FROM pragma_foreign_key_check")?!=0{
  return Err(EventOutboxR366Error::Corrupt)
 }
 // All past rows are immutable under exact pinned triggers and were
 // previously checked. Bind EVERY newly ACKed row to authenticated source
 // envelope, source physical DB1, and independently committed DB2.
 r895_verify_new_paper_rows(src,dst,v,&rows)?;
 let old_fingerprints=if fingerprints{
  Some(old_fingerprints)
 }else{None};
 drop(snapshot);
 let tx=Tx::begin(src).map_err(r366_error)?;
 r924_verify_installed_guards(src,v)?;
 r924_verify_installed_guards(dst,v)?;
 let (now,c2,m2)=r805_read_head(root,src,dst)?;
 if now.cleaned!=h.cleaned||now.applied!=h.applied
  ||now.latest<h.latest||now.retained<h.retained
  ||m2!=m||c2["feed_generation"]!=c["feed_generation"]
  ||c2["baseline_seq"]!=c["baseline_seq"]||c2["state"]!=c["state"]{
  return Err(EventOutboxR366Error::Conflict)
 }
 // No direct non-outbox INSERT may introduce phantom rows.
 if now.latest.checked_mul(32)!=Some(r366_query_num(src,"SELECT COUNT(*) FROM events")?){
  return Err(EventOutboxR366Error::Corrupt)
 }
 // Full retained-chain authenticity is rechecked inside short lock,
 // including concurrently arrived future source envelopes.
 if let Some(ref old)=old_fingerprints{
  r945_verify_fingerprints_and_canonical_new_tail(
   src,&now,r726_s(&c2,"feed_generation")?,old)?;
 }else{
  r805_verify_chain(src,&now,r726_s(&c2,"feed_generation")?)?;
 }
 let mut q=Stmt::new(src,&format!(
  "SELECT payload_hash FROM mirror_outbox WHERE seq={}",h.applied))
  .map_err(r366_error)?;
 if q.step().map_err(r366_error)?!=SQLITE_ROW
  ||q.strval(0).map_err(r366_error)?!=hash{
  return Err(EventOutboxR366Error::Corrupt)
 }
 drop(q);
 let sql=format!("DELETE FROM mirror_outbox WHERE seq<={};
  UPDATE mirror_control SET cleanup_through={},
    retained_records=retained_records-{acked},
    retained_bytes=retained_bytes-{bytes} WHERE id=1",h.applied,h.applied);
 exec(src,&sql).map_err(r366_error)?;
 dest_tx.commit().map_err(r366_error)?;
 tx.commit().map_err(r366_error)?;
 Ok((acked,now.retained-acked))
}
#[test]fn r932_genesis_rejects_existing_events_without_applicable_baseline(){
 let (root,h,_)=pair(1);let v=r586_oracle();
 r924_install_private_paper_only_guards(&h.source,&h.sink,&v).unwrap();
 assert_eq!(r932_certify_private_empty_genesis(&root.0,&h.source,&h.sink,
  &v,"r879_probe"),Err(EventOutboxR366Error::Conflict));
}
#[test]fn r933_empty_genesis_proves_64_batches_exact_physical_parity(){
 let root=Root::new();let v=r586_oracle();
 let src=r671_open(&root.0,"r671-private-db1.sqlite",false).unwrap();
 let dst=r671_open(&root.0,"r671-private-db2.sqlite",false).unwrap();
 r924_install_private_paper_only_guards(&src,&dst,&v).unwrap();
 r932_certify_private_empty_genesis(&root.0,&src,&dst,&v,"r933_probe").unwrap();
 let mut h=R819OriginalPaperHistory::new(&root.0,"r933_probe").unwrap();
 let mut owner=Owner::new(0,1,800).unwrap();
 for i in (1..=2048).step_by(32){
  assert_eq!(r824_commit_prepared_batch(&mut h,&mut owner,&(i..i+32)
   .map(event).collect::<Vec<_>>()),Ok(((i+31)/32,32)));
  assert_eq!(r931_guarded_verified_burst(&root.0,&src,&dst,&v,1),Ok(1));
  assert_eq!(r932_guarded_incremental_prefix_ack(&root.0,&src,&dst,
   &v,"r933_probe"),Ok((1,0)));
 }
 assert_eq!(r800_streaming_original_parity(&src,&dst,&v),Ok(2048));
 assert_eq!(r366_query_num(&src,"SELECT COUNT(*) FROM mirror_outbox"),Ok(0));
}

// R937-R941 negative cases are guarded private original 17-table only.
// The existing independent DB1/DB2 genesis certificate and exact triggers
// are mandatory. All tamper cases must fail before source cleanup.
fn r937_ready_one()->(Root,R819OriginalPaperHistory,serde_json::Value){
 let root=Root::new();let v=r586_oracle();
 {
  let src=r671_open(&root.0,"r671-private-db1.sqlite",false).unwrap();
  let dst=r671_open(&root.0,"r671-private-db2.sqlite",false).unwrap();
  r924_install_private_paper_only_guards(&src,&dst,&v).unwrap();
  r932_certify_private_empty_genesis(&root.0,&src,&dst,&v,"r937_probe").unwrap();
 }
 let mut h=R819OriginalPaperHistory::new(&root.0,"r937_probe").unwrap();
 let mut owner=Owner::new(0,1,800).unwrap();
 let batch=(1..=32).map(event).collect::<Vec<_>>();
 assert_eq!(r824_commit_prepared_batch(&mut h,&mut owner,&batch),Ok((1,32)));
 assert_eq!(r931_guarded_verified_burst(&root.0,&h.source,&h.sink,&v,1),Ok(1));
 (root,h,v)
}
#[test]fn r937_outbox_hash_poison_denies_certified_prefix_ack(){
 let (root,h,v)=r937_ready_one();
 exec(&h.source,"UPDATE mirror_outbox SET payload_hash='poisoned'
   WHERE seq=1").unwrap();
 assert_eq!(r932_guarded_incremental_prefix_ack(&root.0,&h.source,
  &h.sink,&v,"r937_probe"),Err(EventOutboxR366Error::Corrupt));
 assert_eq!(r366_query_num(&h.source,"SELECT cleanup_through FROM mirror_control"),Ok(0));
}
fn r938_inject_unsourced_event(db:&Db,v:&serde_json::Value){
 let table=v["table_schema"].as_array().unwrap().iter()
  .find(|x|x["table"]=="events").unwrap();
 let cols=table["columns"].as_array().unwrap().iter().map(|x|{
  let name=x[1].as_str().unwrap();
  if name=="event_id"{"'ghost_'||event_id".into()}
  else{r586_identifier(name).unwrap()}
 }).collect::<Vec<String>>().join(",");
 let sql=format!("INSERT INTO events SELECT {cols} FROM events LIMIT 1");
 exec(db,&sql).unwrap();
}
#[test]fn r938_extra_unsourced_source_event_denies_ack(){
 let (root,h,v)=r937_ready_one();
 r938_inject_unsourced_event(&h.source,&v);
 assert_eq!(r932_guarded_incremental_prefix_ack(&root.0,&h.source,
  &h.sink,&v,"r937_probe"),Err(EventOutboxR366Error::Corrupt));
 assert_eq!(r366_query_num(&h.source,"SELECT cleanup_through FROM mirror_control"),Ok(0));
}
#[test]fn r939_extra_unreplayed_destination_event_denies_ack(){
 let (root,h,v)=r937_ready_one();
 r938_inject_unsourced_event(&h.sink,&v);
 assert_eq!(r932_guarded_incremental_prefix_ack(&root.0,&h.source,
  &h.sink,&v,"r937_probe"),Err(EventOutboxR366Error::Corrupt));
 assert_eq!(r366_query_num(&h.source,"SELECT cleanup_through FROM mirror_control"),Ok(0));
}
#[test]fn r940_forged_genesis_certificate_denies_ack(){
 let (root,h,v)=r937_ready_one();
 exec(&h.sink,"UPDATE mirror_meta SET last_key_json='forged'
  WHERE id=1").unwrap();
 assert_eq!(r932_guarded_incremental_prefix_ack(&root.0,&h.source,
  &h.sink,&v,"r937_probe"),Err(EventOutboxR366Error::Corrupt));
 assert_eq!(r366_query_num(&h.source,"SELECT cleanup_through FROM mirror_control"),Ok(0));
}
#[test]fn r941_dropped_original_guard_denies_ack(){
 let (root,h,v)=r937_ready_one();
 exec(&h.source,"DROP TRIGGER r924_guard_events_delete").unwrap();
 assert_eq!(r932_guarded_incremental_prefix_ack(&root.0,&h.source,
  &h.sink,&v,"r937_probe"),Err(EventOutboxR366Error::Corrupt));
 assert_eq!(r366_query_num(&h.source,"SELECT cleanup_through FROM mirror_control"),Ok(0));
}

// R942-R943: actual SIGKILL of an independent PRIVATE DB2 receiver
// during a fully guarded original 17-table replay / ACK. Restart in new
// OS PID with identical protected original DB1/DB2 and exact cursor.
#[test]fn r942_sigkill_guarded_worker_child(){
 let Some(root)=std::env::var_os("ASTRA_R942_ROOT").map(PathBuf::from) else{return};
 assert!(r671_guard(&root,"r671-private-db1.sqlite").is_ok());
 let stage=std::env::var("ASTRA_R942_STAGE").unwrap();
 assert!(matches!(stage.as_str(),"after_db2"|"after_ack"));
 let src=r671_open(&root,"r671-private-db1.sqlite",false).unwrap();
 let dst=r671_open(&root,"r671-private-db2.sqlite",false).unwrap();
 let v=r586_oracle();
 assert_eq!(r931_guarded_verified_burst(&root,&src,&dst,&v,1),Ok(1));
 if stage=="after_ack"{
  assert_eq!(r932_guarded_incremental_prefix_ack(
   &root,&src,&dst,&v,"r942_probe"),Ok((1,2)));
 }
 std::fs::write(root.join("r942-stage-reached"),stage.as_bytes()).unwrap();
 std::thread::sleep(std::time::Duration::from_secs(25));
 panic!("R942 test worker must be terminated with SIGKILL");
}
fn r942_crash_guarded_receiver_stage(stage:&str){
 let root=Root::new();let v=r586_oracle();
 {
  let src=r671_open(&root.0,"r671-private-db1.sqlite",false).unwrap();
  let dst=r671_open(&root.0,"r671-private-db2.sqlite",false).unwrap();
  r924_install_private_paper_only_guards(&src,&dst,&v).unwrap();
  r932_certify_private_empty_genesis(&root.0,&src,&dst,&v,"r942_probe").unwrap();
 }
 let mut h=R819OriginalPaperHistory::new(&root.0,"r942_probe").unwrap();
 let mut owner=Owner::new(0,1,800).unwrap();
 for i in (1..=96).step_by(32){
  assert_eq!(r824_commit_prepared_batch(&mut h,&mut owner,
   &(i..i+32).map(event).collect::<Vec<_>>()),Ok(((i+31)/32,32)));
 }
 let exe=std::env::current_exe().unwrap();
 let mut worker=std::process::Command::new(&exe);
 worker.args(["--exact",
  "native_isolated_sqlite_mirror_r147_review::r879_tests::r942_sigkill_guarded_worker_child",
  "--nocapture"])
  .env("ASTRA_R942_ROOT",&root.0).env("ASTRA_R942_STAGE",stage)
  .stdout(std::process::Stdio::null())
  .stderr(std::process::Stdio::null());
 let mut child=worker.spawn().unwrap();
 let marker=root.0.join("r942-stage-reached");
 let mut reached=false;
 for _ in 0..1500{
  if marker.exists(){reached=true;break}
  if child.try_wait().unwrap().is_some(){break}
  std::thread::sleep(std::time::Duration::from_millis(5));
 }
 child.kill().expect("guarded private receiver must be SIGKILLed");
 assert!(!child.wait().unwrap().success());
 assert!(reached,"R942 guarded DB2 stage must occur before SIGKILL");
 assert_eq!(std::fs::read_to_string(marker).unwrap(),stage);
 let cleaned=if stage=="after_ack"{1}else{0};
 {
  let src=r671_open(&root.0,"r671-private-db1.sqlite",false).unwrap();
  let dst=r671_open(&root.0,"r671-private-db2.sqlite",false).unwrap();
  assert_eq!(r366_query_num(&dst,"SELECT applied_seq FROM mirror_meta"),Ok(1));
  assert_eq!(r366_query_num(&src,"SELECT cleanup_through FROM mirror_control"),Ok(cleaned));
  if cleaned==0{
   assert_eq!(r932_guarded_incremental_prefix_ack(
    &root.0,&src,&dst,&v,"r942_probe"),Ok((1,2)));
  }
  assert_eq!(r931_guarded_verified_burst(&root.0,&src,&dst,&v,2),Ok(2));
  assert_eq!(r932_guarded_incremental_prefix_ack(
   &root.0,&src,&dst,&v,"r942_probe"),Ok((2,0)));
  assert_eq!(r800_streaming_original_parity(&src,&dst,&v),Ok(96));
  assert_eq!(r366_query_num(&src,"SELECT COUNT(*) FROM mirror_outbox"),Ok(0));
  assert_eq!(r366_query_num(&src,"SELECT cleanup_through FROM mirror_control"),Ok(3));
  assert_eq!(r366_query_num(&dst,"SELECT applied_seq FROM mirror_meta"),Ok(3));
 }
 let (recovered,n)=r834_recover_private_owner(&root.0,"r942_probe",0,1,800).unwrap();
 assert_eq!(n,96);
 assert_eq!(recovered.snapshot_venue("BTC",Venue::OkxSpot),
            owner.snapshot_venue("BTC",Venue::OkxSpot));
 eprintln!("R942-R943 guarded SIGKILL recovered {} with DB1/DB2 parity and 96 Owner events",stage);
}
#[test]fn r942_sigkill_guarded_after_db2_durable_commit(){
 if std::env::var_os("ASTRA_R942_ROOT").is_none(){
  r942_crash_guarded_receiver_stage("after_db2");
 }
}
#[test]fn r943_sigkill_guarded_after_prefix_ack_commit(){
 if std::env::var_os("ASTRA_R942_ROOT").is_none(){
  r942_crash_guarded_receiver_stage("after_ack");
 }
}
