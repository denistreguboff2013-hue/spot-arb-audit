// Astra P0-01 R824: ORIGINAL 17-table batch commit on a prepared
// Owner CLONE, followed by canonical Owner publication only after DB1
// durable transaction ACK. Isolated R671 source only; never REAL.
fn r824_original_paper_op(
 manifest:&serde_json::Value,run:&str,seq:u64,index:usize,batch_size:usize,
 frame:&crate::RawFrame,update:&crate::Update
)->Result<serde_json::Value,EventOutboxR366Error>{
 if frame.venue!=update.venue||frame.epoch!=update.book.epoch
  ||frame.received_ns!=update.book.received_ns
  ||frame.received_wall_ms!=update.book.received_wall_ms
  ||frame.received_wall_ms<=0||frame.raw.is_empty()
  ||frame.raw.len()>32_768||update.base.is_empty()||update.base.len()>30{
  return Err(EventOutboxR366Error::Corrupt)
 }
 let id=format!("paper_r824_{run}_{seq:020}_{index:02}");
 let payload=serde_json::json!({
  "schema":"SPOTARB/NATIVE/PAPER-HISTORY/R824/V1",
  "batch_size":batch_size,
  "mode":"paper",
  "venue":match frame.venue{crate::Venue::OkxSpot=>"okx_public_spot",
   crate::Venue::BinanceFutures=>"binance_public_futures"},
  "epoch":frame.epoch,
  "received_ns":frame.received_ns,
  "received_wall_ms":frame.received_wall_ms,
  "base":update.base,
  "raw":frame.raw,
  "bids_json":serde_json::to_string(&update.book.bids)
   .map_err(|_|EventOutboxR366Error::Schema)?,
  "asks_json":serde_json::to_string(&update.book.asks)
   .map_err(|_|EventOutboxR366Error::Schema)?,
  "exchange_ts_ms":update.book.exchange_ts_ms,
  "sequence":update.book.sequence,
  "paper_only":true,"exchange_attested":false
 });
 let canonical=r386_python_mirror_canonical(&payload)?;
 let row=serde_json::json!({
  "event_id":id,"schema_version":1,"mode":"paper",
  "category":"market_data","name":"public_ws_depth_accepted",
  "source":"native_rust_public_paper","run_id":run,
  "occurred_at_ms":frame.received_wall_ms,
  "received_at_ms":frame.received_wall_ms,
  "correlation_id":null,"signal_id":null,"base":update.base,
  "shadow_group_id":null,"comparison_id":null,"reason_code":null,
  "verification_status":"unverified","is_exchange_verified":0,
  "payload_json":canonical,"verification_json":"{}",
  "content_hash":r366_sha(&canonical)
 });
 r659_op(manifest,"events",&row,"insert")
}
fn r824_commit_prepared_batch(
 h:&mut R819OriginalPaperHistory,owner:&mut crate::Owner,
 events:&[(crate::RawFrame,crate::Update)]
)->Result<(u64,usize),crate::CoreError>{
 use crate::CoreError;
 if events.is_empty()||events.len()>32{return Err(CoreError::HistoryPressure)}
 // R871: every potentially fallible counter check happens BEFORE DB1
 // commit and Owner swap. Nothing may return HistoryPressure after ACK.
 let next_event_count=h.committed_events.checked_add(events.len() as u64)
  .ok_or(CoreError::HistoryPressure)?;
 // Prepare *all* input on a private copy. The canonical Owner is not
 // touched until every source event is in the same durable SQLite commit.
 let mut candidate=owner.clone();
 let mut prepared=Vec::with_capacity(events.len());
 for (frame,update) in events{
  let accepted=candidate.prepare_parsed(frame,update.clone())?;
  candidate.ingest_parsed(frame,update.clone(),
   &mut crate::history::PreparedSignal)?;
  prepared.push(accepted);
 }
 let count=r366_query_num(&h.source,
  "SELECT retained_records FROM mirror_control WHERE id=1")
  .map_err(|_|CoreError::HistoryPressure)?;
 if count>=64{return Err(CoreError::HistoryPressure)}
 let next=r366_query_num(&h.source,
  "SELECT latest_seq FROM mirror_control WHERE id=1")
  .map_err(|_|CoreError::HistoryPressure)?
  .checked_add(1).ok_or(CoreError::HistoryPressure)?;
 let mut operations=Vec::with_capacity(events.len());
 for (i,((frame,_),update)) in events.iter().zip(&prepared).enumerate(){
  operations.push(r824_original_paper_op(&h.manifest,&h.run_id,next,i,events.len(),frame,update)
   .map_err(|_|CoreError::HistoryPressure)?);
 }
 let durable=r830_incremental_original_append(&h.root,&h.source,&h.sink,&h.manifest,&operations)
  .map_err(|_|CoreError::HistoryPressure)?;
 if durable!=next{return Err(CoreError::HistoryPressure)}
 // R868: publish the fully prepared Owner in ONE non-fallible swap.
 // Replaying inputs against the original after DB1 commit can fail
 // halfway and leave in-memory books divergent from durable DB1.
 // No canonical mutation is visible before the source transaction ACK.
 *owner=candidate;
 h.accepted=durable;
 h.committed_events=next_event_count;
 Ok((durable,events.len()))
}

// R1088: commit public depth frames from multiple Owner shards in
// ONE durable original-17-table source transaction. No canonical
// Owner state is published until DB1 commit. Private PAPER only.
pub fn r1088_commit_prepared_multishard_batch(
 h:&mut R819OriginalPaperHistory, owners:&mut [crate::Owner],
 frames:&[(crate::RawFrame,crate::Update,usize)]
)->Result<(u64,usize),crate::CoreError>{
 use crate::CoreError;
 if frames.is_empty()||frames.len()>32||owners.len()!=12{
  return Err(CoreError::HistoryPressure)
 }
 let new_events=h.committed_events.checked_add(frames.len() as u64)
   .ok_or(CoreError::HistoryPressure)?;
 let r1130_t=std::time::Instant::now();
 // R1128: stage only touched owners, not all 12 on every 16-frame
 // durable transaction. Never mutate live owners before DB1 commit.
 let mut staged=std::collections::BTreeMap::<usize,crate::Owner>::new();
 let mut prepared=Vec::with_capacity(frames.len());
 for (frame,update,idx) in frames{
   let live=owners.get(*idx).ok_or(CoreError::HistoryPressure)?;
   let candidate=staged.entry(*idx).or_insert_with(||live.clone());
   // R1107: source-owned private PAPER diagnostics only. On refusal,
   // preserve atomic non-commit and expose the exact offending venue,
   // source epoch and source timestamp; never suppress an OutOfOrder.
   let validated=candidate.prepare_parsed(frame,update.clone()).map_err(|e|{
     eprintln!("R1107_SOURCE_ORDER_DENIED base={} venue={:?} epoch={} source_ms={:?} received_ns={} previous={:?} reason={:?}",
       update.base,frame.venue,frame.epoch,update.book.exchange_ts_ms,
       frame.received_ns,candidate.snapshot_venue(&update.base,frame.venue)
         .map(|x|(x.epoch,x.exchange_ts_ms,x.received_ns,x.sequence)),e);
     e
   })?;
   candidate.ingest_parsed(frame,update.clone(),
      &mut crate::history::PreparedSignal)?;
   prepared.push(validated);
 }
 r1130_measure(0,r1130_t);
 let r1130_t=std::time::Instant::now();
 let mut count=r366_query_num(&h.source,
   "SELECT retained_records FROM mirror_control WHERE id=1")
   .map_err(|_|CoreError::HistoryPressure)?;
 if count>=64{
  if std::env::var("PAPER_R1088_PRIVATE_BATCH").ok().as_deref()!=Some("YES"){
   return Err(CoreError::HistoryPressure)
  }
  let deadline=std::time::Instant::now()+std::time::Duration::from_secs(8);
  while count>=64{
   if std::time::Instant::now()>=deadline {
    return Err(CoreError::HistoryPressure)
   }
   std::thread::sleep(std::time::Duration::from_millis(20));
   count=r366_query_num(&h.source,
     "SELECT retained_records FROM mirror_control WHERE id=1")
       .map_err(|_|CoreError::HistoryPressure)?;
  }
 }
 r1130_measure(1,r1130_t);
 let r1130_t=std::time::Instant::now();
 let next=r366_query_num(&h.source,
   "SELECT latest_seq FROM mirror_control WHERE id=1")
   .map_err(|_|CoreError::HistoryPressure)?
   .checked_add(1).ok_or(CoreError::HistoryPressure)?;
 let mut ops=Vec::with_capacity(frames.len());
 for (i,((frame,_,_),update)) in frames.iter().zip(&prepared).enumerate(){
  ops.push(r824_original_paper_op(&h.manifest,&h.run_id,next,i,
     frames.len(),frame,update).map_err(|_|CoreError::HistoryPressure)?);
 }
 r1130_measure(2,r1130_t);
 let r1130_t=std::time::Instant::now();
 let committed=r830_incremental_original_append(
   &h.root,&h.source,&h.sink,&h.manifest,&ops)
   .map_err(|_|CoreError::HistoryPressure)?;
 r1130_measure(3,r1130_t);
 if committed!=next{return Err(CoreError::HistoryPressure)}
 let r1130_t=std::time::Instant::now();
 // Atomic source commit is already complete. Only touched owners
 // are published; untouched owners remain byte-for-byte unchanged.
 for (index,next) in staged{
   owners[index]=next;
 }
 r1130_measure(4,r1130_t);
 h.accepted=next;
 h.committed_events=new_events;
 Ok((next,frames.len()))
}

#[cfg(test)]
mod r824_tests{
 use super::*;
 use crate::{Book,RawFrame,Update,Venue,Owner};
 use std::os::unix::fs::DirBuilderExt;
 struct Private(PathBuf);
 impl Private{
  fn new()->Self{
   let mut dir=std::env::temp_dir();
   let stamp=std::time::SystemTime::now().duration_since(
    std::time::UNIX_EPOCH).unwrap().as_nanos();
   dir.push(format!("astra_r671_file_{}_{}_r824",std::process::id(),stamp));
   fs::DirBuilder::new().mode(0o700).create(&dir).unwrap();
   drop(r756_initialize(&dir).unwrap());
   Self(dir)
  }
 }
 impl Drop for Private{fn drop(&mut self){let _=fs::remove_dir_all(&self.0);}}
 fn event(i:u64)->(RawFrame,Update){
  let ns=1_000_000_000_u64+i*10_000_000;
  let ms=1_800_000_000_000_i64+(i as i64)*10;
  let venue=Venue::OkxSpot;
  let raw=format!("{{\"source\":\"r824-fixture-public\",\"seq\":{i}}}");
  let f=RawFrame{source:None,venue,epoch:1,received_ns:ns,received_wall_ms:ms,raw};
  let b=Book{bids:vec![(101.0,1.0)],asks:vec![(102.0,1.0)],
   received_ns:ns,received_wall_ms:ms,exchange_ts_ms:Some(ms),
   changed_ns:ns,epoch:1,sequence:Some(i as i64)};
  (f,Update{base:"BTC".into(),venue,book:b})
 }
 fn pair()->(Private,R819OriginalPaperHistory,Owner){
  let root=Private::new();
  let h=R819OriginalPaperHistory::new(&root.0,"batch_1").unwrap();
  (root,h,Owner::new(0,1,800).unwrap())
 }
 fn read_count(db:&Db,table:&str)->u64{
  r366_query_num(db,&format!("SELECT COUNT(*) FROM {table}")).unwrap()
 }
 #[test]fn r824_sixteen_events_one_original_python_shaped_durable_tx(){
  let (root,mut h,mut owner)=pair();
  let frames=(1..=16).map(event).collect::<Vec<_>>();
  assert_eq!(r824_commit_prepared_batch(&mut h,&mut owner,&frames),Ok((1,16)));
  assert_eq!(read_count(&h.source,"events"),16);
  assert_eq!(read_count(&h.source,"mirror_outbox"),1);
  let last=owner.snapshot_venue("BTC",Venue::OkxSpot).unwrap();
  assert_eq!(last.sequence,Some(16));
  let v=r586_oracle();
  assert_eq!(r805_replay_window(&root.0,&h.source,&h.sink,&v,32),Ok(1));
  assert_eq!(r805_cleanup_window(&root.0,&h.source,&h.sink,&v),Ok((1,0)));
  assert_eq!(r800_streaming_original_parity(&h.source,&h.sink,&v),Ok(16));
  let mut stmt=Stmt::new(&h.sink,"SELECT mode,is_exchange_verified,payload_json
   FROM events ORDER BY event_id LIMIT 1").unwrap();
  assert_eq!(stmt.step().unwrap(),SQLITE_ROW);
  assert_eq!(stmt.strval(0).unwrap(),"paper");
  assert_eq!(stmt.num(1).unwrap(),0);
  let payload:serde_json::Value=serde_json::from_str(&stmt.strval(2).unwrap()).unwrap();
  assert_eq!(payload["paper_only"],true);
  assert!(payload["raw"].as_str().unwrap().contains("r824-fixture-public"));
 }
 #[test]fn r825_atomic_batch_error_never_mutates_canonical_owner(){
  let (_root,mut h,mut owner)=pair();
  let mut items=(1..=16).map(event).collect::<Vec<_>>();
  items[12].0.raw.clear();
  assert_eq!(r824_commit_prepared_batch(&mut h,&mut owner,&items),
   Err(crate::CoreError::HistoryPressure));
  assert!(owner.snapshot_venue("BTC",Venue::OkxSpot).is_none());
  assert_eq!(read_count(&h.source,"events"),0);
  assert_eq!(read_count(&h.source,"mirror_outbox"),0);
 }
 #[test]fn r826_four_bounded_batches_are_four_durable_frames_not_sixtyfour(){
  let (root,mut h,mut owner)=pair();
  for i in 0..4 {
   let frames=(i*16+1..=(i+1)*16).map(event).collect::<Vec<_>>();
   assert_eq!(r824_commit_prepared_batch(&mut h,&mut owner,&frames),
    Ok((i+1,16)));
  }
  assert_eq!(read_count(&h.source,"events"),64);
  assert_eq!(read_count(&h.source,"mirror_outbox"),4);
  let v=r586_oracle();
  assert_eq!(r805_replay_window(&root.0,&h.source,&h.sink,&v,32),Ok(4));
  assert_eq!(r805_cleanup_window(&root.0,&h.source,&h.sink,&v),Ok((4,0)));
  assert_eq!(r800_streaming_original_parity(&h.source,&h.sink,&v),Ok(64));
 }
 #[test]fn r827_batch_rejects_over_capacity_without_mutation(){
  let (_root,mut h,mut owner)=pair();
  let frames=(1..=33).map(event).collect::<Vec<_>>();
  assert_eq!(r824_commit_prepared_batch(&mut h,&mut owner,&frames),
   Err(crate::CoreError::HistoryPressure));
  assert!(owner.snapshot_venue("BTC",Venue::OkxSpot).is_none());
  assert_eq!(read_count(&h.source,"events"),0);
 }
 #[test]fn r828_sixtyfour_event_one_vs_sixteen_source_commit_measurement(){
  use std::time::Instant;
  for window in [1_u64,16]{
   let (root,mut h,mut owner)=pair();
   let start=Instant::now();
   for from in (1_u64..=64).step_by(window as usize){
    let items=(from..from+window).map(event).collect::<Vec<_>>();
    r824_commit_prepared_batch(&mut h,&mut owner,&items).unwrap();
   }
   let ingest_ms=start.elapsed().as_secs_f64()*1000.0;
   let source_outbox=read_count(&h.source,"mirror_outbox");
   assert_eq!(source_outbox,64/window);
   assert_eq!(read_count(&h.source,"events"),64);
   let mirror_start=Instant::now();
   let v=r586_oracle();
   let mut forwarded=0;
   for _ in 0..3{
    let next=r805_replay_window(&root.0,&h.source,&h.sink,&v,32).unwrap();
    forwarded+=next;
    if next==0{break}
   }
   assert_eq!(forwarded,source_outbox);
   let mut cleaned=0;
   for _ in 0..3{
    let (removed,left)=r805_cleanup_window(&root.0,&h.source,&h.sink,&v).unwrap();
    cleaned+=removed;
    if left==0{break}
   }
   assert_eq!(cleaned,source_outbox);
   assert_eq!(r800_streaming_original_parity(&h.source,&h.sink,&v),Ok(64));
   eprintln!("R828 isolated 64-event window={window} source_ms={ingest_ms:.2} mirror_and_parity_ms={:.2} db1_commits={source_outbox}",
    mirror_start.elapsed().as_secs_f64()*1000.0);
  }
 }
 #[test]fn r829_twohundredfiftysix_events_bounded_source_and_replay(){
  use std::time::Instant;
  let (root,mut h,mut owner)=pair();
  let start=Instant::now();
  for base in (1..=256).step_by(16){
   let rows=(base..base+16).map(event).collect::<Vec<_>>();
   r824_commit_prepared_batch(&mut h,&mut owner,&rows).unwrap();
  }
  let ms=start.elapsed().as_secs_f64()*1000.0;
  assert_eq!(read_count(&h.source,"events"),256);
  assert_eq!(read_count(&h.source,"mirror_outbox"),16);
  let mirror_start=Instant::now();
  let v=r586_oracle();
  assert_eq!(r805_replay_window(&root.0,&h.source,&h.sink,&v,32),Ok(16));
  assert_eq!(r805_cleanup_window(&root.0,&h.source,&h.sink,&v),Ok((16,0)));
  assert_eq!(r800_streaming_original_parity(&h.source,&h.sink,&v),Ok(256));
  eprintln!("R829 isolated 256-event batched source_ms={ms:.2} events_per_s={:.1} replay_ack_parity_ms={:.2} durable_db1_commits=16",
   256000.0/ms,mirror_start.elapsed().as_secs_f64()*1000.0);
 }
 #[test]fn r830_thousand_twenty_four_events_periodically_drained(){
  use std::time::Instant;
  let (root,mut h,mut owner)=pair();
  let begin=Instant::now();
  let v=r586_oracle();
  for cycle in 0..8{
   let start=(cycle*128+1) as u64;
   for n in (start..start+128).step_by(16){
    let events=(n..n+16).map(event).collect::<Vec<_>>();
    assert_eq!(r824_commit_prepared_batch(&mut h,&mut owner,&events),
     Ok(((cycle*8+(n-start)/16+1) as u64,16)));
   }
   assert_eq!(read_count(&h.source,"mirror_outbox"),8);
   assert_eq!(r805_replay_window(&root.0,&h.source,&h.sink,&v,32),Ok(8));
   assert_eq!(r805_cleanup_window(&root.0,&h.source,&h.sink,&v),Ok((8,0)));
   assert_eq!(read_count(&h.source,"mirror_outbox"),0);
   assert_eq!(h.accepted_count(),(cycle+1)*128);
  }
  let elapsed=begin.elapsed().as_secs_f64();
  assert_eq!(read_count(&h.source,"events"),1024);
  assert_eq!(read_count(&h.sink,"events"),1024);
  assert_eq!(r366_query_num(&h.source,
   "SELECT cleanup_through FROM mirror_control"),Ok(64));
  assert_eq!(r366_query_num(&h.sink,
   "SELECT applied_seq FROM mirror_meta"),Ok(64));
  assert_eq!(r800_streaming_original_parity(&h.source,&h.sink,&v),Ok(1024));
  let db1_len=std::fs::metadata(root.0.join("r671-private-db1.sqlite")).unwrap().len();
  let db2_len=std::fs::metadata(root.0.join("r671-private-db2.sqlite")).unwrap().len();
  eprintln!("R830 isolated 1024 events, 64 DB1 transactions, 8 replay/ACK cycles, elapsed_s={elapsed:.3}, end_to_end_events_per_s={:.1}, db1_bytes={db1_len}, db2_bytes={db2_len}",
   1024.0/elapsed);
 }
 #[test]fn r849_thousand_024_original_events_one_verified_burst(){
  use std::time::Instant;
  let (root,mut h,mut owner)=pair();
  let source_start=Instant::now();
  for from in (1_u64..=1024).step_by(16){
   let frames=(from..from+16).map(event).collect::<Vec<_>>();
   r824_commit_prepared_batch(&mut h,&mut owner,&frames).unwrap();
  }
  let source_elapsed=source_start.elapsed().as_secs_f64();
  let control=r366_query_num(&h.source,
   "SELECT retained_records FROM mirror_control").unwrap();
  assert_eq!(control,64);
  let v=r586_oracle();
  let sink_start=Instant::now();
  assert_eq!(r845_verified_burst(&root.0,&h.source,&h.sink,&v,128),Ok(64));
  assert_eq!(r805_cleanup_window(&root.0,&h.source,&h.sink,&v),Ok((64,0)));
  assert_eq!(r800_streaming_original_parity(&h.source,&h.sink,&v),Ok(1024));
  let sink_elapsed=sink_start.elapsed().as_secs_f64();
  let total=source_elapsed+sink_elapsed;
  assert_eq!(r366_query_num(&h.source,"SELECT COUNT(*) FROM events"),Ok(1024));
  assert_eq!(r366_query_num(&h.sink,"SELECT COUNT(*) FROM events"),Ok(1024));
  assert_eq!(r366_query_num(&h.source,
   "SELECT COUNT(*) FROM mirror_outbox"),Ok(0));
  eprintln!("R849 isolated 1024 public PAPER events source_s={source_elapsed:.3} mirror_ack_parity_s={sink_elapsed:.3} total_s={total:.3} events_per_s={:.1}",1024.0/total);
 }
 #[test]fn r850_same_host_1024_event_old_32_vs_new_128_replay_measure(){
  use std::time::Instant;
  for burst in [false,true]{
   let (root,mut h,mut owner)=pair();
   for from in (1_u64..=1024).step_by(16){
    let frames=(from..from+16).map(event).collect::<Vec<_>>();
    r824_commit_prepared_batch(&mut h,&mut owner,&frames).unwrap();
   }
   let v=r586_oracle();
   let start=Instant::now();
   if burst{
    assert_eq!(r845_verified_burst(&root.0,&h.source,&h.sink,&v,128),Ok(64));
   }else{
    assert_eq!(r805_replay_window(&root.0,&h.source,&h.sink,&v,32),Ok(32));
    assert_eq!(r805_replay_window(&root.0,&h.source,&h.sink,&v,32),Ok(32));
   }
   let replay_ms=start.elapsed().as_secs_f64()*1000.0;
   assert_eq!(r805_cleanup_window(&root.0,&h.source,&h.sink,&v),Ok((64,0)));
   assert_eq!(r800_streaming_original_parity(&h.source,&h.sink,&v),Ok(1024));
   eprintln!("R850 same-host 1024-event mirror variant={} replay_ms={:.2} total_mirror_ack_ms={:.2}",
    if burst{"verified_128"}else{"original_32x2"},replay_ms,start.elapsed().as_secs_f64()*1000.0);
  }
 }
 #[test]fn r831_accepted_count_tracks_events_not_outbox_transactions(){
  let (root,mut h,mut owner)=pair();
  let frames=(1..=16).map(event).collect::<Vec<_>>();
  assert_eq!(r824_commit_prepared_batch(&mut h,&mut owner,&frames),Ok((1,16)));
  assert_eq!(h.accepted_count(),16);
  drop(h);
  let mut reopened=R819OriginalPaperHistory::new(&root.0,"batch_1").unwrap();
  assert_eq!(reopened.accepted_count(),0);
  let (frame,update)=event(17);
  crate::HistorySink::accept(&mut reopened,&frame,&update).unwrap();
  assert_eq!(reopened.accepted_count(),1);
  assert_eq!(read_count(&reopened.source,"events"),17);
  let v=r586_oracle();
  assert_eq!(r805_replay_window(&root.0,&reopened.source,&reopened.sink,&v,32),Ok(2));
  assert_eq!(r805_cleanup_window(&root.0,&reopened.source,&reopened.sink,&v),Ok((2,0)));
 }
 #[test]fn r832_corrupt_last_outbox_frame_denies_next_batch(){
  let (_root,mut h,mut owner)=pair();
  let first=(1..=16).map(event).collect::<Vec<_>>();
  assert_eq!(r824_commit_prepared_batch(&mut h,&mut owner,&first),Ok((1,16)));
  exec(&h.source,"UPDATE mirror_outbox SET payload_hash='forged' WHERE seq=1").unwrap();
  let next=(17..=32).map(event).collect::<Vec<_>>();
  assert_eq!(r824_commit_prepared_batch(&mut h,&mut owner,&next),
   Err(crate::CoreError::HistoryPressure));
  assert_eq!(read_count(&h.source,"events"),16);
  assert_eq!(read_count(&h.source,"mirror_outbox"),1);
  assert_eq!(owner.snapshot_venue("BTC",Venue::OkxSpot).unwrap().sequence,Some(16));
 }
 #[test]fn r833_corrupt_older_frame_may_append_but_never_ack(){
  let (root,mut h,mut owner)=pair();
  for start in [1,17,33]{
   let batch=(start..start+16).map(event).collect::<Vec<_>>();
   r824_commit_prepared_batch(&mut h,&mut owner,&batch).unwrap();
  }
  exec(&h.source,"UPDATE mirror_outbox SET payload_hash='forged' WHERE seq=1").unwrap();
  let later=(49..=64).map(event).collect::<Vec<_>>();
  assert_eq!(r824_commit_prepared_batch(&mut h,&mut owner,&later),Ok((4,16)));
  let v=r586_oracle();
  assert_eq!(r805_replay_window(&root.0,&h.source,&h.sink,&v,32),
   Err(EventOutboxR366Error::Corrupt));
  assert_eq!(read_count(&h.sink,"events"),0);
  assert_eq!(r366_query_num(&h.sink,"SELECT applied_seq FROM mirror_meta"),Ok(0));
  assert_eq!(r366_query_num(&h.source,"SELECT cleanup_through FROM mirror_control"),Ok(0));
 }
 #[test]fn r851_thirtytwo_original_events_restore_owner_and_ack(){
  let (root,mut h,mut owner)=pair();
  let frames=(1..=32).map(event).collect::<Vec<_>>();
  assert_eq!(r824_commit_prepared_batch(&mut h,&mut owner,&frames),Ok((1,32)));
  assert_eq!(read_count(&h.source,"events"),32);
  assert_eq!(read_count(&h.source,"mirror_outbox"),1);
  let (recovered,seen)=r834_recover_private_owner(&root.0,"batch_1",0,1,800).unwrap();
  assert_eq!(seen,32);
  assert_eq!(recovered.snapshot_venue("BTC",Venue::OkxSpot),
   owner.snapshot_venue("BTC",Venue::OkxSpot));
  let v=r586_oracle();
  assert_eq!(r845_verified_burst(&root.0,&h.source,&h.sink,&v,128),Ok(1));
  assert_eq!(r805_cleanup_window(&root.0,&h.source,&h.sink,&v),Ok((1,0)));
  assert_eq!(r800_streaming_original_parity(&h.source,&h.sink,&v),Ok(32));
 }
 #[test]fn r852_thirtythree_original_ops_block_at_writer(){
  let (_root,mut h,mut owner)=pair();
  let input=(1..=33).map(event).collect::<Vec<_>>();
  assert_eq!(r824_commit_prepared_batch(&mut h,&mut owner,&input),
   Err(crate::CoreError::HistoryPressure));
  assert_eq!(read_count(&h.source,"events"),0);
  assert_eq!(read_count(&h.source,"mirror_outbox"),0);
  assert!(owner.snapshot_venue("BTC",Venue::OkxSpot).is_none());
  let ops=(0..33).map(|i|r659_op(&h.manifest,"metadata",
   &serde_json::json!({"key":format!("r852_{i}"),"value":"paper_fixture"}),
   "insert").unwrap()).collect::<Vec<_>>();
  assert_eq!(r830_incremental_original_append(
   &h.root,&h.source,&h.sink,&h.manifest,&ops),
   Err(EventOutboxR366Error::Pressure));
  assert_eq!(read_count(&h.source,"metadata"),0);
 }
 #[test]fn r853_thirtytwo_batch_late_invalid_member_atomic_rollback(){
  let (_root,mut h,mut owner)=pair();
  let mut frames=(1..=32).map(event).collect::<Vec<_>>();
  frames[31].0.raw.clear();
  assert_eq!(r824_commit_prepared_batch(&mut h,&mut owner,&frames),
   Err(crate::CoreError::HistoryPressure));
  assert_eq!(read_count(&h.source,"events"),0);
  assert_eq!(read_count(&h.source,"mirror_outbox"),0);
  assert!(owner.snapshot_venue("BTC",Venue::OkxSpot).is_none());
 }
 #[test]fn r854_1024_events_same_host_16_versus_32_batch_throughput(){
  use std::time::Instant;
  for group in [16_u64,32]{
   let (root,mut h,mut owner)=pair();
   let start=Instant::now();
   for from in (1_u64..=1024).step_by(group as usize){
    let batch=(from..from+group).map(event).collect::<Vec<_>>();
    r824_commit_prepared_batch(&mut h,&mut owner,&batch).unwrap();
   }
   let source_ms=start.elapsed().as_secs_f64()*1000.0;
   let expected=1024/group;
   assert_eq!(read_count(&h.source,"events"),1024);
   assert_eq!(read_count(&h.source,"mirror_outbox"),expected);
   let mirror_start=Instant::now();
   let v=r586_oracle();
   assert_eq!(r845_verified_burst(&root.0,&h.source,&h.sink,&v,128),
    Ok(expected));
   let mut deleted=0;
   while deleted<expected{
    let (count,left)=r805_cleanup_window(&root.0,&h.source,&h.sink,&v).unwrap();
    deleted+=count;
    if left==0{break}
   }
   assert_eq!(deleted,expected);
   assert_eq!(r800_streaming_original_parity(&h.source,&h.sink,&v),Ok(1024));
   let mirror_ms=mirror_start.elapsed().as_secs_f64()*1000.0;
   eprintln!("R854 group={group} events=1024 source_commits={expected} source_ms={source_ms:.2} mirror_ms={mirror_ms:.2} throughput_events_sec={:.1}",
    1024.0*1000.0/(source_ms+mirror_ms));
  }
 }
 #[test]fn r1088_multiple_owner_shards_commit_once_and_replay_db2_ack(){
  let root=Private::new();
  let mut h=R819OriginalPaperHistory::new(&root.0,"r1088_multi").unwrap();
  let mut owners=(0..12).map(|i|crate::Owner::new(i,12,800).unwrap())
      .collect::<Vec<_>>();
  let mut batch=Vec::new();
  for i in 1..=16_u64{
    let (frame,mut update)=event(i);
    update.base=format!("BASE{}",i%12);
    let idx=crate::owner_index(&update.base,12);
    batch.push((frame,update,idx));
  }
  assert_eq!(r1088_commit_prepared_multishard_batch(&mut h,&mut owners,&batch),
    Ok((1,16)));
  assert_eq!(read_count(&h.source,"events"),16);
  assert_eq!(read_count(&h.source,"mirror_outbox"),1);
  for (_,u,idx) in &batch{
   assert!(owners[*idx].snapshot_venue(&u.base,Venue::OkxSpot).is_some());
  }
  let manifest=r586_oracle();
  assert_eq!(r845_verified_burst(&root.0,&h.source,&h.sink,&manifest,128),Ok(1));
  assert_eq!(r805_cleanup_window(&root.0,&h.source,&h.sink,&manifest),Ok((1,0)));
  assert_eq!(r800_streaming_original_parity(&h.source,&h.sink,&manifest),Ok(16));
 }
 #[test]fn r1129_private_source_batch32_commits_atomically(){
  let root=Private::new();
  let mut h=R819OriginalPaperHistory::new(&root.0,"r1129_32").unwrap();
  let mut owners=(0..12).map(|i|crate::Owner::new(i,12,800).unwrap())
    .collect::<Vec<_>>();
  let mut batch=Vec::new();
  for i in 1..=32_u64{
    let (frame,mut update)=event(i);
    update.base=format!("BASE{}",i%12);
    let idx=crate::owner_index(&update.base,12);
    batch.push((frame,update,idx));
  }
  assert_eq!(r1088_commit_prepared_multishard_batch(&mut h,&mut owners,&batch),
      Ok((1,32)));
  assert_eq!(read_count(&h.source,"events"),32);
  assert_eq!(read_count(&h.source,"mirror_outbox"),1);
  let manifest=r586_oracle();
  assert_eq!(r845_verified_burst(&root.0,&h.source,&h.sink,&manifest,128),Ok(1));
  assert_eq!(r805_cleanup_window(&root.0,&h.source,&h.sink,&manifest),Ok((1,0)));
  assert_eq!(r800_streaming_original_parity(&h.source,&h.sink,&manifest),Ok(32));
 }
 #[test]fn r1088_multishard_late_bad_frame_refuses_before_db1_or_owner(){
  let root=Private::new();
  let mut h=R819OriginalPaperHistory::new(&root.0,"r1088_fail").unwrap();
  let mut owners=(0..12).map(|i|crate::Owner::new(i,12,800).unwrap())
      .collect::<Vec<_>>();
  let mut batch=Vec::new();
  for i in 1..=16_u64{
    let (frame,mut update)=event(i);
    update.base=format!("BASE{}",i%12);
    let idx=crate::owner_index(&update.base,12);
    batch.push((frame,update,idx));
  }
  batch[15].0.raw.clear();
  assert_eq!(r1088_commit_prepared_multishard_batch(&mut h,&mut owners,&batch),
   Err(crate::CoreError::HistoryPressure));
  assert_eq!(read_count(&h.source,"events"),0);
  assert_eq!(read_count(&h.source,"mirror_outbox"),0);
  assert_eq!(h.accepted_count(),0);
  for (_,u,idx) in &batch{
   assert!(owners[*idx].snapshot_venue(&u.base,Venue::OkxSpot).is_none());
  }
 }
 #[test]fn r1089_below_watermark_ack_deferred_until_sealed(){
  let (root,mut h,mut owner)=pair();
  let batch=(1_u64..=16).map(event).collect::<Vec<_>>();
  r824_commit_prepared_batch(&mut h,&mut owner,&batch).unwrap();
  let oracle=r586_oracle();
  assert_eq!(r1089_bounded_reused_drain(&root.0,&h.source,&h.sink,
     &oracle,false),Ok((1,0,1)));
  assert_eq!(r366_query_num(&h.source,
     "SELECT retained_records FROM mirror_control WHERE id=1"),Ok(1));
  assert_eq!(r1089_bounded_reused_drain(&root.0,&h.source,&h.sink,
     &oracle,true),Ok((0,1,0)));
  assert_eq!(r800_streaming_original_parity(&h.source,&h.sink,&oracle),Ok(16));
 }
 #[test]fn r1089_high_watermark_calls_unchanged_r860_parity(){
  let (root,mut h,mut owner)=pair();
  for i in 1_u64..=48{
   r824_commit_prepared_batch(&mut h,&mut owner,&[event(i)]).unwrap();
  }
  assert_eq!(r366_query_num(&h.source,
    "SELECT retained_records FROM mirror_control WHERE id=1"),Ok(48));
  let oracle=r586_oracle();
  assert_eq!(r1089_bounded_reused_drain(&root.0,&h.source,&h.sink,
      &oracle,false),Ok((48,48,0)));
  assert_eq!(r366_query_num(&h.source,
     "SELECT cleanup_through FROM mirror_control WHERE id=1"),Ok(48));
  assert_eq!(r800_streaming_original_parity(&h.source,&h.sink,&oracle),Ok(48));
 }
 #[test]fn r1090_private_dynamic_16_rows_prepared_db2_and_unchanged_verified_ack(){
  let (root,mut h,mut owner)=pair();
  let batch=(1_u64..=16).map(event).collect::<Vec<_>>();
  assert_eq!(r824_commit_prepared_batch(&mut h,&mut owner,&batch),Ok((1,16)));
  let manifest=r586_oracle();
  assert_eq!(r1090_paper_prepared_burst(&root.0,&h.source,&h.sink,
      &manifest,128),Ok(1));
  assert_eq!(r366_query_num(&h.sink,"SELECT COUNT(*) FROM events"),Ok(16));
  assert_eq!(r1090_paper_prepared_burst(&root.0,&h.source,&h.sink,
      &manifest,128),Ok(0));
  assert_eq!(r860_bulk_verified_ack(&root.0,&h.source,&h.sink,
      &manifest,1024),Ok((1,0)));
  assert_eq!(r800_streaming_original_parity(&h.source,&h.sink,&manifest),Ok(16));
 }
 #[test]fn r1134_parallel_helper_full_chain_commit_and_exact_ack(){
  let (root,mut h,mut owner)=pair();
  let oracle=r586_oracle();
  for i in 1..=3_u64{
    assert_eq!(r824_commit_prepared_batch(&mut h,&mut owner,&[event(i)]),
      Ok((i,1)));
  }
  for seq in 1..=3_u64{
    assert_eq!(r1134_parallel_verified_paper_burst(
      &root.0,&h.source,&h.sink,&oracle,1),Ok(1));
    assert_eq!(r366_query_num(&h.sink,
      "SELECT applied_seq FROM mirror_meta WHERE id=1"),Ok(seq));
  }
  assert_eq!(r1134_parallel_verified_paper_burst(
    &root.0,&h.source,&h.sink,&oracle,1),Ok(0));
  assert_eq!(r860_bulk_verified_ack(
    &root.0,&h.source,&h.sink,&oracle,1024),Ok((3,0)));
  assert_eq!(r800_streaming_original_parity(
    &h.source,&h.sink,&oracle),Ok(3));
 }
 #[test]fn r1134_parallel_helper_refuses_poisoned_already_applied(){
  let (root,mut h,mut owner)=pair();
  let oracle=r586_oracle();
  for i in 1..=2_u64{
    assert_eq!(r824_commit_prepared_batch(&mut h,&mut owner,&[event(i)]),
       Ok((i,1)));
  }
  assert_eq!(r1134_parallel_verified_paper_burst(
      &root.0,&h.source,&h.sink,&oracle,1),Ok(1));
  exec(&h.source,
    "UPDATE mirror_outbox SET payload_hash='tampered' WHERE seq=1").unwrap();
  assert_eq!(r1134_parallel_verified_paper_burst(
      &root.0,&h.source,&h.sink,&oracle,1),
      Err(EventOutboxR366Error::Corrupt));
  assert_eq!(r366_query_num(&h.sink,
      "SELECT applied_seq FROM mirror_meta WHERE id=1"),Ok(1));
  assert_eq!(r366_query_num(&h.sink,
      "SELECT COUNT(*) FROM events"),Ok(1));
 }
 #[test]fn r1134_parallel_helper_refuses_poisoned_future_row(){
  let (root,mut h,mut owner)=pair();
  let oracle=r586_oracle();
  for i in 1..=2_u64{
    assert_eq!(r824_commit_prepared_batch(&mut h,&mut owner,&[event(i)]),
       Ok((i,1)));
  }
  exec(&h.source,
    "UPDATE mirror_outbox SET payload_hash='tampered' WHERE seq=2").unwrap();
  assert_eq!(r1134_parallel_verified_paper_burst(
      &root.0,&h.source,&h.sink,&oracle,1),
      Err(EventOutboxR366Error::Corrupt));
  assert_eq!(r366_query_num(&h.sink,
      "SELECT applied_seq FROM mirror_meta WHERE id=1"),Ok(0));
  assert_eq!(r366_query_num(&h.sink,
      "SELECT COUNT(*) FROM events"),Ok(0));
 }
 #[test]fn r1132_fused_full_chain_bounded_replay_and_exact_ack(){
  let (root,mut h,mut owner)=pair();
  let oracle=r586_oracle();
  for i in 1..=3_u64 {
    assert_eq!(r824_commit_prepared_batch(&mut h,&mut owner,&[event(i)]),
       Ok((i,1)));
  }
  for expected in 1..=3_u64 {
    assert_eq!(r1132_fused_verified_paper_burst(
      &root.0,&h.source,&h.sink,&oracle,1),Ok(1));
    assert_eq!(r366_query_num(&h.sink,
       "SELECT applied_seq FROM mirror_meta WHERE id=1"),Ok(expected));
  }
  assert_eq!(r1132_fused_verified_paper_burst(
    &root.0,&h.source,&h.sink,&oracle,1),Ok(0));
  assert_eq!(r860_bulk_verified_ack(
    &root.0,&h.source,&h.sink,&oracle,1024),Ok((3,0)));
  assert_eq!(r800_streaming_original_parity(
    &h.source,&h.sink,&oracle),Ok(3));
 }
 #[test]fn r1132_fused_refuses_tampered_old_applied_prefix(){
  let (root,mut h,mut owner)=pair();
  let oracle=r586_oracle();
  for i in 1..=2_u64{
    assert_eq!(r824_commit_prepared_batch(&mut h,&mut owner,&[event(i)]),
      Ok((i,1)));
  }
  assert_eq!(r1132_fused_verified_paper_burst(
    &root.0,&h.source,&h.sink,&oracle,1),Ok(1));
  exec(&h.source,
    "UPDATE mirror_outbox SET payload_hash='poison' WHERE seq=1").unwrap();
  assert_eq!(r1132_fused_verified_paper_burst(
    &root.0,&h.source,&h.sink,&oracle,1),
    Err(EventOutboxR366Error::Corrupt));
  assert_eq!(r366_query_num(&h.sink,
    "SELECT applied_seq FROM mirror_meta WHERE id=1"),Ok(1));
  assert_eq!(r366_query_num(&h.sink,
    "SELECT COUNT(*) FROM events"),Ok(1));
 }
 #[test]fn r1132_fused_rolls_back_if_later_row_corrupt(){
  let (root,mut h,mut owner)=pair();
  let oracle=r586_oracle();
  for i in 1..=2_u64{
    assert_eq!(r824_commit_prepared_batch(&mut h,&mut owner,&[event(i)]),
      Ok((i,1)));
  }
  exec(&h.source,
    "UPDATE mirror_outbox SET payload_hash='poison' WHERE seq=2").unwrap();
  assert_eq!(r1132_fused_verified_paper_burst(
    &root.0,&h.source,&h.sink,&oracle,1),
    Err(EventOutboxR366Error::Corrupt));
  assert_eq!(r366_query_num(&h.sink,
    "SELECT applied_seq FROM mirror_meta WHERE id=1"),Ok(0));
  assert_eq!(r366_query_num(&h.sink,
    "SELECT COUNT(*) FROM events"),Ok(0));
 }
 #[test]fn r1090_corrupted_retained_hash_refuses_before_db2(){
  let (root,mut h,mut owner)=pair();
  assert_eq!(r824_commit_prepared_batch(&mut h,&mut owner,&[event(1)]),
      Ok((1,1)));
  exec(&h.source,"UPDATE mirror_outbox SET payload_hash='corrupt' WHERE seq=1").unwrap();
  let manifest=r586_oracle();
  assert_eq!(r1090_paper_prepared_burst(&root.0,&h.source,&h.sink,
      &manifest,128),Err(EventOutboxR366Error::Corrupt));
  assert_eq!(r366_query_num(&h.sink,"SELECT COUNT(*) FROM events"),Ok(0));
  assert_eq!(r366_query_num(&h.sink,
     "SELECT applied_seq FROM mirror_meta WHERE id=1"),Ok(0));
 }
}
