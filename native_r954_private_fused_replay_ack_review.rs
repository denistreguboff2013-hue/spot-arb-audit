// Astra P0-01 R954: PRIVATE only combined verified DB2 replay + DB1 ACK.
// In ONE WAL source read snapshot, completely validate every retained
// original R805 envelope and build deterministic newly applied PAPER rows.
// DB2 txn applies only missing rows and certifies all awaiting ACK by
// physical exact source+dest checks, then commits durably FIRST.
// Source snapshot is released; a DB1 BEGIN IMMEDIATE write lock checks
// exact old row fingerprints plus any canonically valid concurrently
// appended source tail and removes only the DB2-durable applied prefix.
// A crash between destination commit and source cleanup remains replayable
// by existing R945 independent ACK. No REAL/installed/historic databases.
pub(super) fn r954_private_fused_verified_replay_and_ack(
 root:&Path,src:&Db,dst:&Db,v:&serde_json::Value,run:&str,
 injected_after_db2:impl FnOnce()->Result<(),EventOutboxR366Error>
)->Result<(u64,u64),EventOutboxR366Error>{
 r924_verify_installed_guards(src,v)?;
 r924_verify_installed_guards(dst,v)?;
 exec(src,"BEGIN DEFERRED").map_err(r366_error)?;
 let snapshot=R805SourceRead(src);
 let dst_tx=Tx::begin(dst).map_err(r366_error)?;
 let (head,control,meta)=r805_read_head(root,src,dst)?;
 if head.retained>64||head.latest<=head.applied{
  return Err(EventOutboxR366Error::Conflict)
 }
 let schema=r756_schema_hash(src)?;
 let seal=r932_seal(run,&schema,r726_s(&control,"feed_generation")?)?;
 if meta["last_key_json"].as_str()!=Some(seal.as_str()){
  return Err(EventOutboxR366Error::Corrupt)
 }
 if head.latest.checked_mul(32)!=Some(r366_query_num(src,"SELECT COUNT(*) FROM events")?)
  ||head.applied.checked_mul(32)!=Some(r366_query_num(dst,"SELECT COUNT(*) FROM events")?){
  return Err(EventOutboxR366Error::Corrupt)
 }
 let mut q=Stmt::new(src,&r805_window_sql(R726_OUTBOX,"ORDER BY seq")?)
  .map_err(r366_error)?;
 let mut prev=head.cleaned;let mut count=0u64;let mut bytes=0u64;
 let mut acked_bytes=0u64;let mut expected=std::collections::BTreeMap::new();
 let mut fingerprints=Vec::new();
 let mut last_hash=String::new();let mut applied=head.applied;
 let mut replayed=0u64;
 loop{
  match q.step().map_err(r366_error)?{
   SQLITE_DONE=>break,
   SQLITE_ROW=>{
    let raw=q.strval(0).map_err(r366_error)?;
    let row:serde_json::Value=serde_json::from_str(&raw)
     .map_err(|_|EventOutboxR366Error::Corrupt)?;
    let (seq,size)=r805_validate_row(
     &row,prev,head.latest,r726_s(&control,"feed_generation")?)?;
    if seq!=prev.checked_add(1).ok_or(EventOutboxR366Error::Pressure)?{
     return Err(EventOutboxR366Error::Corrupt)
    }
    count+=1;bytes=bytes.checked_add(size).ok_or(EventOutboxR366Error::Pressure)?;
    if count>64||bytes>1024*1024*1024{return Err(EventOutboxR366Error::Pressure)}
    fingerprints.push((seq,r366_sha(&raw)));prev=seq;
    let frame:serde_json::Value=serde_json::from_str(r726_s(&row,"payload_json")?)
     .map_err(|_|EventOutboxR366Error::Corrupt)?;
    let ops=frame["operations"].as_array().ok_or(EventOutboxR366Error::Corrupt)?;
    if ops.len()!=32{return Err(EventOutboxR366Error::Corrupt)}
    for (idx,op) in ops.iter().enumerate(){
     let id=format!("paper_r824_{run}_{seq:020}_{idx:02}");
     if op["table"]!="events"||op["op"]!="insert"||
       op["row"]["mode"]!="paper"||op["row"]["verification_status"]!="unverified"
       ||op["row"]["is_exchange_verified"]!=0
       ||op["row"]["event_id"]!=id||op["row"]["run_id"]!=run{
      return Err(EventOutboxR366Error::Corrupt)
     }
     if expected.insert(id,op["row"].clone()).is_some(){
      return Err(EventOutboxR366Error::Corrupt)
     }
     if seq>head.applied{
      r586_apply_original_operation(dst,v,op)?;
     }
    }
    if seq>head.applied {replayed+=1;applied=seq;}
    acked_bytes=acked_bytes.checked_add(size).ok_or(EventOutboxR366Error::Pressure)?;
    last_hash=r726_s(&row,"payload_hash")?.to_owned();
   },
   _=>return Err(EventOutboxR366Error::Corrupt)
  }
 }
 drop(q);
 if count!=head.retained||count==0||prev!=head.latest
  ||bytes!=r726_u(&control,"retained_bytes")?
  ||replayed!=head.latest-head.applied
  ||expected.len()!=count as usize*32{
  return Err(EventOutboxR366Error::Corrupt)
 }
 if r366_query_num(dst,"SELECT COUNT(*) FROM pragma_foreign_key_check")?!=0{
  return Err(EventOutboxR366Error::Corrupt)
 }
 if std::env::var("ASTRA_R962_RAW_BINDING").ok().as_deref()==Some("1"){
  r962_verify_new_paper_rows_raw(src,dst,v,&expected)?;
 }else{
  r895_verify_new_paper_rows(src,dst,v,&expected)?;
 }
 if applied.checked_mul(32)!=Some(r366_query_num(dst,"SELECT COUNT(*) FROM events")?){
  return Err(EventOutboxR366Error::Corrupt)
 }
 // Check the authoritative DB2 receipt against exactly the prior
 // applied hash from the original source before updating it.
 let old_applied_hash=if head.applied>head.cleaned{
  // Last currently committed DB2 hash is matched by the source
  // outbox's exact previously-applied receipt.
  let mut check=Stmt::new(src,&format!(
   "SELECT payload_hash FROM mirror_outbox WHERE seq={}",head.applied))
   .map_err(r366_error)?;
  if check.step().map_err(r366_error)?!=SQLITE_ROW{
   return Err(EventOutboxR366Error::Corrupt)
  }
  Some(check.strval(0).map_err(r366_error)?)
 }else{None};
 if head.applied>0 && head.applied>head.cleaned
  &&meta["applied_hash"].as_str()!=old_applied_hash.as_deref(){
  return Err(EventOutboxR366Error::Corrupt)
 }
 let next_receipt=format!("UPDATE mirror_meta SET applied_seq={applied},
 applied_hash={},last_success_ms={} WHERE id=1",
  r586_sql_literal(&serde_json::json!(last_hash))?,3_000_000+applied);
 exec(dst,&next_receipt).map_err(r366_error)?;
 dst_tx.commit().map_err(r366_error)?;
 injected_after_db2()?;
 drop(snapshot);
 // The DB2 durability boundary above MUST precede source cleanup.
 let source_tx=Tx::begin(src).map_err(r366_error)?;
 r924_verify_installed_guards(src,v)?;
 r924_verify_installed_guards(dst,v)?;
 let (now,control2,meta2)=r805_read_head(root,src,dst)?;
 if now.cleaned!=head.cleaned||now.applied!=applied
  ||now.latest<head.latest||now.retained<head.retained
  ||control2["feed_generation"]!=control["feed_generation"]
  ||control2["baseline_seq"]!=control["baseline_seq"]
  ||control2["state"]!=control["state"]
  ||meta2["last_key_json"]!=meta["last_key_json"]
  ||meta2["applied_hash"]!=last_hash{
  return Err(EventOutboxR366Error::Conflict)
 }
 if now.latest.checked_mul(32)!=Some(r366_query_num(src,"SELECT COUNT(*) FROM events")?){
  return Err(EventOutboxR366Error::Corrupt)
 }
 r945_verify_fingerprints_and_canonical_new_tail(
  src,&now,r726_s(&control2,"feed_generation")?,&fingerprints)?;
 let mut check=Stmt::new(src,&format!(
  "SELECT payload_hash FROM mirror_outbox WHERE seq={applied}"))
  .map_err(r366_error)?;
 if check.step().map_err(r366_error)?!=SQLITE_ROW
  ||check.strval(0).map_err(r366_error)?!=last_hash{
  return Err(EventOutboxR366Error::Corrupt)
 }
 drop(check);
 exec(src,&format!("DELETE FROM mirror_outbox WHERE seq<={applied};
 UPDATE mirror_control SET cleanup_through={applied},
 retained_records=retained_records-{count},
 retained_bytes=retained_bytes-{acked_bytes} WHERE id=1"))
 .map_err(r366_error)?;
 source_tx.commit().map_err(r366_error)?;
 Ok((replayed,count))
}
#[test]fn r954_fused_private_64_envelopes_exact_parity_and_owner(){
 let root=Root::new();let v=r586_oracle();
 let src=r671_open(&root.0,"r671-private-db1.sqlite",false).unwrap();
 let dst=r671_open(&root.0,"r671-private-db2.sqlite",false).unwrap();
 r924_install_private_paper_only_guards(&src,&dst,&v).unwrap();
 r932_certify_private_empty_genesis(&root.0,&src,&dst,&v,"r954_probe").unwrap();
 let mut h=R819OriginalPaperHistory::new(&root.0,"r954_probe").unwrap();
 let mut owner=Owner::new(0,1,800).unwrap();
 for i in (1..=2048).step_by(32){
  assert_eq!(r824_commit_prepared_batch(&mut h,&mut owner,
   &(i..i+32).map(event).collect::<Vec<_>>()),Ok(((i+31)/32,32)));
  assert_eq!(r954_private_fused_verified_replay_and_ack(
   &root.0,&src,&dst,&v,"r954_probe",||Ok(())),Ok((1,1)));
 }
 assert_eq!(r800_streaming_original_parity(&src,&dst,&v),Ok(2048));
 assert_eq!(r366_query_num(&src,"SELECT COUNT(*) FROM mirror_outbox"),Ok(0));
 let (restored,n)=r834_recover_private_owner(&root.0,"r954_probe",0,1,800).unwrap();
 assert_eq!(n,2048);
 assert_eq!(restored.snapshot_venue("BTC",Venue::OkxSpot),
            owner.snapshot_venue("BTC",Venue::OkxSpot));
}
#[test]fn r955_fused_abort_after_db2_commit_recovers_ack_without_duplication(){
 let root=Root::new();let v=r586_oracle();
 let src=r671_open(&root.0,"r671-private-db1.sqlite",false).unwrap();
 let dst=r671_open(&root.0,"r671-private-db2.sqlite",false).unwrap();
 r924_install_private_paper_only_guards(&src,&dst,&v).unwrap();
 r932_certify_private_empty_genesis(&root.0,&src,&dst,&v,"r955_probe").unwrap();
 let mut h=R819OriginalPaperHistory::new(&root.0,"r955_probe").unwrap();
 let mut owner=Owner::new(0,1,800).unwrap();
 assert_eq!(r824_commit_prepared_batch(&mut h,&mut owner,
  &(1..=32).map(event).collect::<Vec<_>>()),Ok((1,32)));
 assert_eq!(r954_private_fused_verified_replay_and_ack(
  &root.0,&src,&dst,&v,"r955_probe",
  ||Err(EventOutboxR366Error::Interrupted)),
  Err(EventOutboxR366Error::Interrupted));
 assert_eq!(r366_query_num(&src,"SELECT cleanup_through FROM mirror_control"),Ok(0));
 assert_eq!(r366_query_num(&dst,"SELECT applied_seq FROM mirror_meta"),Ok(1));
 assert_eq!(r945_private_fast_fingerprint_ack(
  &root.0,&src,&dst,&v,"r955_probe"),Ok((1,0)));
 assert_eq!(r800_streaming_original_parity(&src,&dst,&v),Ok(32));
 assert_eq!(r366_query_num(&dst,"SELECT COUNT(*) FROM events"),Ok(32));
}
