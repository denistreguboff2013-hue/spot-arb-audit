// R914-R918 Astra P0-01 strictly private conservative two-phase ACK.
// Phase 1: WAL source DEFERRED read snapshot + DB2 write fence, full R805
// retained hash-chain, 17 original table physical parity and original
// operations bound to BOTH physical copies. Does not lock source writer.
// Phase 2: release snapshot and acquire source BEGIN IMMEDIATE. SQLite
// PRAGMA data_version must be UNCHANGED: any intervening external DB1
// writer (including valid append) forces fail-closed retry. We explicitly
// do not assert this is viable for uninterrupted 900+/s feed. This
// prototype proves the safety boundary; no production promotion.
pub(super) fn r914_private_optimistic_prefix_ack<F>(
 root:&Path,src:&Db,dst:&Db,manifest:&serde_json::Value,run:&str,
 after_snapshot:F
)->Result<(u64,u64),EventOutboxR366Error>
where F:FnOnce()->Result<(),EventOutboxR366Error>{
 let before_version=r366_query_num(src,"PRAGMA data_version")?;
 exec(src,"BEGIN DEFERRED").map_err(r366_error)?;
 let snapshot=R805SourceRead(src);
 let dest_tx=Tx::begin(dst).map_err(r366_error)?;
 let (h,c,m)=r805_read_head(root,src,dst)?;
 if h.applied<=h.cleaned{return Err(EventOutboxR366Error::Conflict)}
 if h.applied>h.latest||h.retained>64{return Err(EventOutboxR366Error::Pressure)}
 let generation=r726_s(&c,"feed_generation")?;
 let (acked,bytes,rows,hash)=r907_verify_retained_and_extract_prefix(
  src,&h,generation,run)?;
 if m["applied_hash"].as_str()!=Some(hash.as_str()){
  return Err(EventOutboxR366Error::Corrupt)
 }
 if r366_query_num(dst,"SELECT COUNT(*) FROM pragma_foreign_key_check")?!=0{
  return Err(EventOutboxR366Error::Corrupt)
 }
 r879_private_paper_prefix_parity(src,dst,manifest,run,h.applied)?;
 r895_verify_new_paper_rows(src,dst,manifest,&rows)?;
 after_snapshot()?;
 drop(snapshot); // mandatory: WAL read-to-write upgrade can return BUSY_SNAPSHOT
 #[cfg(test)] let lock_start=std::time::Instant::now();
 let source_tx=Tx::begin(src).map_err(r366_error)?;
 if r366_query_num(src,"PRAGMA data_version")?!=before_version{
  return Err(EventOutboxR366Error::Conflict)
 }
 // Reassert the exact certified source and DB2 control receipts under
 // the short DB1 write lock. Any changed control/identity is not trusted.
 let (current,c2,m2)=r805_read_head(root,src,dst)?;
 if current.latest!=h.latest||current.applied!=h.applied
  ||current.cleaned!=h.cleaned||current.retained!=h.retained
  ||c2!=c||m2!=m{
  return Err(EventOutboxR366Error::Conflict)
 }
 let mut seal=Stmt::new(src,&format!(
  "SELECT payload_hash FROM mirror_outbox WHERE seq={}",h.applied))
  .map_err(r366_error)?;
 if seal.step().map_err(r366_error)?!=SQLITE_ROW
  ||seal.strval(0).map_err(r366_error)?!=hash{
  return Err(EventOutboxR366Error::Corrupt)
 }
 drop(seal);
 let sql=format!("DELETE FROM mirror_outbox WHERE seq<={};
  UPDATE mirror_control SET cleanup_through={},
  retained_records=retained_records-{acked},
  retained_bytes=retained_bytes-{bytes} WHERE id=1",h.applied,h.applied);
 exec(src,&sql).map_err(r366_error)?;
 dest_tx.commit().map_err(r366_error)?;
 source_tx.commit().map_err(r366_error)?;
 #[cfg(test)] eprintln!("R914 private source WRITE_LOCK_ONLY ms={:.3}",
   lock_start.elapsed().as_secs_f64()*1000.0);
 Ok((acked,h.retained-acked))
}

#[test] fn r914_two_phase_identical_prefix_ack_and_full_recovery(){
 let (root,h,owner)=pair(3);let v=r586_oracle();
 assert_eq!(r845_verified_burst(&root.0,&h.source,&h.sink,&v,1),Ok(1));
 assert_eq!(r914_private_optimistic_prefix_ack(
  &root.0,&h.source,&h.sink,&v,"r879_probe",||Ok(())),Ok((1,2)));
 assert_eq!(r845_verified_burst(&root.0,&h.source,&h.sink,&v,2),Ok(2));
 assert_eq!(r914_private_optimistic_prefix_ack(
  &root.0,&h.source,&h.sink,&v,"r879_probe",||Ok(())),Ok((2,0)));
 assert_eq!(r366_query_num(&h.source,"SELECT retained_records FROM mirror_control"),Ok(0));
 assert_eq!(r800_streaming_original_parity(&h.source,&h.sink,&v),Ok(96));
 let (actual,count)=r834_recover_private_owner(&root.0,"r879_probe",0,1,800).unwrap();
 assert_eq!(count,96);
 assert_eq!(actual.snapshot_venue("BTC",Venue::OkxSpot),
            owner.snapshot_venue("BTC",Venue::OkxSpot));
}

#[test] fn r915_stable_snapshot_does_not_block_source_writer_then_denies_stale_ack(){
 let (root,h,_)=pair(3);let v=r586_oracle();
 assert_eq!(r845_verified_burst(&root.0,&h.source,&h.sink,&v,1),Ok(1));
 // Preopen independent SQL connection; no connector/prod routes.
 let writer=r671_open(&root.0,"r671-private-db1.sqlite",false).unwrap();
 let before=std::time::Instant::now();
 assert_eq!(r914_private_optimistic_prefix_ack(
  &root.0,&h.source,&h.sink,&v,"r879_probe",||{
    exec(&writer,"UPDATE events SET reason_code='source_changed_during_snapshot'
       WHERE event_id LIKE '%_00000000000000000001_00'")
      .map_err(r366_error)?;
    Ok(())
  }),Err(EventOutboxR366Error::Conflict));
 eprintln!("R915 independent source writer DURING snapshot finished in {:.3} ms; ACK denied",before.elapsed().as_secs_f64()*1000.0);
 assert_eq!(r366_query_num(&h.source,"SELECT cleanup_through FROM mirror_control"),Ok(0));
 assert_eq!(r366_query_num(&h.sink,"SELECT applied_seq FROM mirror_meta"),Ok(1));
}

#[test] fn r916_concurrent_valid_append_does_not_block_but_invalidates_snapshot(){
 let (root,h,_)=pair(3);let v=r586_oracle();
 assert_eq!(r845_verified_burst(&root.0,&h.source,&h.sink,&v,1),Ok(1));
 let writer=r671_open(&root.0,"r671-private-db1.sqlite",false).unwrap();
 let writer_sink=r671_open(&root.0,"r671-private-db2.sqlite",false).unwrap();
 let ops=(97..=128).map(|i|{
   let (frame,update)=event(i);
   r824_original_paper_op(&v,"r879_probe",4,(i-97) as usize,32,&frame,&update).unwrap()
 }).collect::<Vec<_>>();
 assert_eq!(r914_private_optimistic_prefix_ack(
  &root.0,&h.source,&h.sink,&v,"r879_probe",||{
    let committed=r830_incremental_original_append(
     &root.0,&writer,&writer_sink,&v,&ops)?;
    assert_eq!(committed,4);
    Ok(())
  }),Err(EventOutboxR366Error::Conflict));
 assert_eq!(r366_query_num(&h.source,"SELECT latest_seq FROM mirror_control"),Ok(4));
 assert_eq!(r366_query_num(&h.source,"SELECT cleanup_through FROM mirror_control"),Ok(0));
 assert_eq!(r366_query_num(&h.sink,"SELECT applied_seq FROM mirror_meta"),Ok(1));
 // Reopen an uncompromised snapshot: ACK the formerly applied prefix.
 assert_eq!(r914_private_optimistic_prefix_ack(
  &root.0,&h.source,&h.sink,&v,"r879_probe",||Ok(())),Ok((1,3)));
 assert_eq!(r845_verified_burst(&root.0,&h.source,&h.sink,&v,3),Ok(3));
 assert_eq!(r914_private_optimistic_prefix_ack(
  &root.0,&h.source,&h.sink,&v,"r879_probe",||Ok(())),Ok((3,0)));
 assert_eq!(r800_streaming_original_parity(&h.source,&h.sink,&v),Ok(128));
}

#[test] fn r917_preexisting_equal_db1_db2_tamper_denies_two_phase_ack(){
 let (root,h,_)=pair(2);let v=r586_oracle();
 assert_eq!(r845_verified_burst(&root.0,&h.source,&h.sink,&v,1),Ok(1));
 for db in [&h.source,&h.sink]{
  exec(db,"UPDATE events SET category='malicious_same_on_both'
     WHERE event_id LIKE '%_00000000000000000001_00'").unwrap();
 }
 assert_eq!(r914_private_optimistic_prefix_ack(
  &root.0,&h.source,&h.sink,&v,"r879_probe",||Ok(())),
  Err(EventOutboxR366Error::Corrupt));
 assert_eq!(r366_query_num(&h.source,"SELECT cleanup_through FROM mirror_control"),Ok(0));
}

#[test] fn r918_no_applied_prefix_never_opens_cleanout(){
 let (root,h,_)=pair(2);let v=r586_oracle();
 assert_eq!(r914_private_optimistic_prefix_ack(
  &root.0,&h.source,&h.sink,&v,"r879_probe",||Ok(())),
  Err(EventOutboxR366Error::Conflict));
 assert_eq!(r366_query_num(&h.source,"SELECT cleanup_through FROM mirror_control"),Ok(0));
}

#[test] fn r919_destination_tamper_refuses_snapshot_confirmation(){
 let (root,h,_)=pair(2);let v=r586_oracle();
 assert_eq!(r845_verified_burst(&root.0,&h.source,&h.sink,&v,1),Ok(1));
 exec(&h.sink,"UPDATE events SET payload_json='{}'
  WHERE event_id LIKE '%_00000000000000000001_05'").unwrap();
 assert_eq!(r914_private_optimistic_prefix_ack(
  &root.0,&h.source,&h.sink,&v,"r879_probe",||Ok(())),
  Err(EventOutboxR366Error::Corrupt));
 assert_eq!(r366_query_num(&h.source,"SELECT cleanup_through FROM mirror_control"),Ok(0));
}
#[test] fn r920_validation_interrupt_rolls_back_without_ack(){
 let (root,h,_)=pair(2);let v=r586_oracle();
 assert_eq!(r845_verified_burst(&root.0,&h.source,&h.sink,&v,1),Ok(1));
 assert_eq!(r914_private_optimistic_prefix_ack(
  &root.0,&h.source,&h.sink,&v,"r879_probe",||{
    Err(EventOutboxR366Error::Interrupted)
  }),Err(EventOutboxR366Error::Interrupted));
 assert_eq!(r366_query_num(&h.source,"SELECT cleanup_through FROM mirror_control"),Ok(0));
 assert_eq!(r366_query_num(&h.sink,"SELECT applied_seq FROM mirror_meta"),Ok(1));
 assert_eq!(r914_private_optimistic_prefix_ack(
  &root.0,&h.source,&h.sink,&v,"r879_probe",||Ok(())),Ok((1,1)));
}
#[test] fn r921_wrong_identity_denied_before_two_phase_ack(){
 let (root,h,_)=pair(2);let v=r586_oracle();
 assert_eq!(r845_verified_burst(&root.0,&h.source,&h.sink,&v,1),Ok(1));
 assert_eq!(r914_private_optimistic_prefix_ack(
  &root.0,&h.source,&h.sink,&v,"../untrusted",||Ok(())),
  Err(EventOutboxR366Error::Schema));
 assert_eq!(r366_query_num(&h.source,"SELECT cleanup_through FROM mirror_control"),Ok(0));
}
#[test] fn r922_outbox_poison_during_snapshot_rejects_then_full_scan_detects(){
 let (root,h,_)=pair(3);let v=r586_oracle();
 assert_eq!(r845_verified_burst(&root.0,&h.source,&h.sink,&v,1),Ok(1));
 let attacker=r671_open(&root.0,"r671-private-db1.sqlite",false).unwrap();
 assert_eq!(r914_private_optimistic_prefix_ack(
  &root.0,&h.source,&h.sink,&v,"r879_probe",||{
    exec(&attacker,"UPDATE mirror_outbox SET payload_hash='poisoned'
      WHERE seq=3").map_err(r366_error)
  }),Err(EventOutboxR366Error::Conflict));
 assert_eq!(r914_private_optimistic_prefix_ack(
  &root.0,&h.source,&h.sink,&v,"r879_probe",||Ok(())),
  Err(EventOutboxR366Error::Corrupt));
 assert_eq!(r366_query_num(&h.source,"SELECT cleanup_through FROM mirror_control"),Ok(0));
 assert_eq!(r366_query_num(&h.sink,"SELECT applied_seq FROM mirror_meta"),Ok(1));
}
#[test] fn r923_full_2048_row_snapshot_short_source_commit(){
 let (root,h,_)=pair(64);let v=r586_oracle();
 assert_eq!(r845_verified_burst(&root.0,&h.source,&h.sink,&v,64),Ok(64));
 let start=std::time::Instant::now();
 assert_eq!(r914_private_optimistic_prefix_ack(
  &root.0,&h.source,&h.sink,&v,"r879_probe",||Ok(())),Ok((64,0)));
 eprintln!("R923 strict full private original-17 snapshot+ACK 2048 events ms={:.2}",
   start.elapsed().as_secs_f64()*1000.0);
 assert_eq!(r800_streaming_original_parity(&h.source,&h.sink,&v),Ok(2048));
 assert_eq!(r366_query_num(&h.source,"SELECT COUNT(*) FROM mirror_outbox"),Ok(0));
}
