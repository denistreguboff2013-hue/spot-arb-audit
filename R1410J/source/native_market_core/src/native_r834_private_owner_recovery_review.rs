// Astra P0-01 R834: read-back recovery of committed R824 PAPER depth
// from the original isolated 17-table DB1. Rebuild Owner on a PRIVATE clone,
// only publish after every event and the complete source cursor validates.
// Research fixture scope only, no installed PAPER or legacy DB paths.
fn r834_recover_private_owner(
 root:&Path,run:&str,index:usize,total:usize,max_age_ms:u64
)->Result<(crate::Owner,u64),EventOutboxR366Error>{
 if run.is_empty()||run.len()>80||!run.bytes()
   .all(|c|c.is_ascii_alphanumeric()||c==b'_'||c==b'-'){
  return Err(EventOutboxR366Error::Schema)
 }
 let source=r671_open(root,"r671-private-db1.sqlite",false)?;
 let mut owner=crate::Owner::new(index,total,max_age_ms)
  .map_err(|_|EventOutboxR366Error::Schema)?;
 exec(&source,"BEGIN DEFERRED").map_err(r366_error)?;
 // Source snapshot includes the durable control cursor. Do not use the
 // receiver's ACK cursor: cleanup may already have removed the outbox.
 let result=(||{
  let latest=r366_query_num(&source,
   "SELECT latest_seq FROM mirror_control WHERE id=1")?;
  // R960 private-only bounded streaming recovery beyond 10k events.
  const R960_MAX_EVENTS:u64=65_536;
  if latest>R960_MAX_EVENTS/32{return Err(EventOutboxR366Error::Pressure)}
  let mut q=Stmt::new(&source,
   "SELECT event_id,run_id,mode,source,verification_status,    is_exchange_verified,payload_json,content_hash    FROM events ORDER BY event_id").map_err(r366_error)?;
  let mut batch_seq=1_u64;
  let mut index_in_batch=0_u64;
  let mut batch_size=0_u64;
  let mut read=0_u64;
  loop{
   match q.step().map_err(r366_error)?{
    SQLITE_DONE=>break,
    SQLITE_ROW=>{
     read+=1;
     if read>R960_MAX_EVENTS{return Err(EventOutboxR366Error::Pressure)}
     let id=q.strval(0).map_err(r366_error)?;
     let saved_run=q.strval(1).map_err(r366_error)?;
     let mode=q.strval(2).map_err(r366_error)?;
     let source_name=q.strval(3).map_err(r366_error)?;
     let verification=q.strval(4).map_err(r366_error)?;
     let exchange_verified=q.num(5).map_err(r366_error)?;
     let raw=q.strval(6).map_err(r366_error)?;
     let hash=q.strval(7).map_err(r366_error)?;
     if saved_run!=run||mode!="paper"||source_name!="native_rust_public_paper"
      ||verification!="unverified"||exchange_verified!=0
      ||raw.len()>131072||r366_sha(&raw)!=hash{
      return Err(EventOutboxR366Error::Corrupt)
     }
     let payload:serde_json::Value=serde_json::from_str(&raw)
      .map_err(|_|EventOutboxR366Error::Corrupt)?;
     if r386_python_mirror_canonical(&payload)?!=raw
      ||payload["schema"]!="SPOTARB/NATIVE/PAPER-HISTORY/R824/V1"
      ||payload["paper_only"]!=true||payload["exchange_attested"]!=false
      ||payload["mode"]!="paper"{
      return Err(EventOutboxR366Error::Corrupt)
     }
     let size=r726_u(&payload,"batch_size")?;
     if size==0||size>32||batch_seq>latest{
      return Err(EventOutboxR366Error::Corrupt)
     }
     if index_in_batch==0{batch_size=size}
     if batch_size!=size||index_in_batch>=batch_size
      ||id!=format!("paper_r824_{run}_{batch_seq:020}_{index_in_batch:02}"){
      return Err(EventOutboxR366Error::Corrupt)
     }
     let venue=match r726_s(&payload,"venue")?{
      "okx_public_spot"=>crate::Venue::OkxSpot,
      "binance_public_futures"=>crate::Venue::BinanceFutures,
      _=>return Err(EventOutboxR366Error::Corrupt),
     };
     let original=r726_s(&payload,"raw")?;
     if original.is_empty()||original.len()>32768{return Err(EventOutboxR366Error::Corrupt)}
     let base=r726_s(&payload,"base")?;
     if base.is_empty()||base.len()>30{return Err(EventOutboxR366Error::Corrupt)}
     let epoch=r726_u(&payload,"epoch")?;
     let received_ns=r726_u(&payload,"received_ns")?;
     let wall=payload["received_wall_ms"].as_i64()
      .filter(|x|*x>0).ok_or(EventOutboxR366Error::Corrupt)?;
     let bids:Vec<(f64,f64)>=serde_json::from_str(
       r726_s(&payload,"bids_json")?).map_err(|_|EventOutboxR366Error::Corrupt)?;
     let asks:Vec<(f64,f64)>=serde_json::from_str(
       r726_s(&payload,"asks_json")?).map_err(|_|EventOutboxR366Error::Corrupt)?;
     if bids.len()>5000||asks.len()>5000||bids.iter().chain(asks.iter()).any(
      |(price,qty)|!price.is_finite()||!qty.is_finite()||*price<=0.0||*qty<0.0){
      return Err(EventOutboxR366Error::Corrupt)
     }
     let exchange_ts_ms=if payload["exchange_ts_ms"].is_null(){None}else{
      Some(payload["exchange_ts_ms"].as_i64().ok_or(EventOutboxR366Error::Corrupt)?)
     };
     let sequence=if payload["sequence"].is_null(){None}else{
      Some(payload["sequence"].as_i64().ok_or(EventOutboxR366Error::Corrupt)?)
     };
     let frame=crate::RawFrame{source:None,venue,epoch,received_ns,received_wall_ms:wall,
      raw:original.to_string()};
     let book=crate::Book{bids,asks,epoch,received_ns,received_wall_ms:wall,
      exchange_ts_ms,sequence,changed_ns:received_ns};
     let update=crate::Update{base:base.to_string(),venue,book};
     let normalized=owner.prepare_parsed(&frame,update.clone())
       .map_err(|_|EventOutboxR366Error::Corrupt)?;
     let mut durable=crate::history::DurableSignal{expected:&normalized};
     owner.ingest_parsed(&frame,update,&mut durable)
       .map_err(|_|EventOutboxR366Error::Corrupt)?;
     index_in_batch+=1;
     if index_in_batch==batch_size{
      batch_seq+=1;
      index_in_batch=0;
     }
    },
    _=>return Err(EventOutboxR366Error::Corrupt)
   }
  }
  if index_in_batch!=0||batch_seq!=latest+1
   ||(latest==0&&read!=0){
   return Err(EventOutboxR366Error::Corrupt)
  }
  Ok((owner,read))
 })();
 let _=exec(&source,"ROLLBACK");
 result
}

#[cfg(test)]
mod r834_tests {
 use super::*;
 use crate::{Book,RawFrame,Update,Venue,Owner};
 use std::os::unix::fs::DirBuilderExt;
 struct Private(PathBuf);
 impl Private {
  fn new()->Self {
   let mut path=std::env::temp_dir();
   let stamp=std::time::SystemTime::now().duration_since(
    std::time::UNIX_EPOCH).unwrap().as_nanos();
   path.push(format!("astra_r671_file_{}_{}_r834",std::process::id(),stamp));
   fs::DirBuilder::new().mode(0o700).create(&path).unwrap();
   drop(r756_initialize(&path).unwrap());
   Self(path)
  }
 }
 impl Drop for Private{fn drop(&mut self){let _=fs::remove_dir_all(&self.0);}}
 fn fixture(i:u64)->(RawFrame,Update){
  let ns=1_000_000_000+i*10_000_000;
  let ms=1_800_000_000_000_i64+i as i64*10;
  let venue=Venue::OkxSpot;
  let f=RawFrame{source:None,venue,epoch:1,received_ns:ns,received_wall_ms:ms,
   raw:format!("{{\"fixture\":\"r834\",\"sequence\":{i}}}")};
  let book=Book{bids:vec![(101.0,1.0)],asks:vec![(102.0,1.0)],
   epoch:1,received_ns:ns,received_wall_ms:ms,
   exchange_ts_ms:Some(ms),changed_ns:ns,sequence:Some(i as i64)};
  (f,Update{base:"BTC".into(),venue,book})
 }
 fn fixture_pair()->(Private,R819OriginalPaperHistory,Owner){
  let p=Private::new();
  let h=R819OriginalPaperHistory::new(&p.0,"recovery").unwrap();
  (p,h,Owner::new(0,1,800).unwrap())
 }
 fn append_without_publication(
 h:&mut R819OriginalPaperHistory,events:&[(RawFrame,Update)]
 )->u64{
  let seq=r366_query_num(&h.source,
   "SELECT latest_seq FROM mirror_control").unwrap()+1;
  let ops=events.iter().enumerate().map(|(i,(frame,update))|
   r824_original_paper_op(&h.manifest,&h.run_id,seq,i,events.len(),frame,update)
    .unwrap()).collect::<Vec<_>>();
  r830_incremental_original_append(&h.root,&h.source,&h.sink,&h.manifest,&ops).unwrap()
 }
 #[test]fn r834_reconstructs_canonical_owner_after_two_committed_batches(){
  let (p,mut h,mut owner)=fixture_pair();
  for start in [1_u64,17]{
   let batch=(start..start+16).map(fixture).collect::<Vec<_>>();
   assert!(r824_commit_prepared_batch(&mut h,&mut owner,&batch).is_ok());
  }
  let (restored,count)=r834_recover_private_owner(&p.0,"recovery",0,1,800).unwrap();
  assert_eq!(count,32);
  assert_eq!(restored.snapshot_books(),owner.snapshot_books());
 }
 #[test]fn r835_crash_after_db1_commit_before_owner_publish_recovers(){
  let (p,mut h,owner)=fixture_pair();
  let batch=(1..=16).map(fixture).collect::<Vec<_>>();
  assert_eq!(append_without_publication(&mut h,&batch),1);
  assert!(owner.snapshot_venue("BTC",Venue::OkxSpot).is_none());
  // DB2 may already have ACKed before process recovery.
  let v=r586_oracle();
  assert_eq!(r805_replay_window(&p.0,&h.source,&h.sink,&v,32),Ok(1));
  assert_eq!(r805_cleanup_window(&p.0,&h.source,&h.sink,&v),Ok((1,0)));
  let (recovered,count)=r834_recover_private_owner(&p.0,"recovery",0,1,800).unwrap();
  assert_eq!(count,16);
  assert_eq!(recovered.snapshot_venue("BTC",Venue::OkxSpot).unwrap().sequence,Some(16));
  assert_eq!(r800_streaming_original_parity(&h.source,&h.sink,&v),Ok(16));
 }
 #[test]fn r836_last_event_deletion_after_ack_fails_closed(){
  let (p,mut h,mut owner)=fixture_pair();
  let batch=(1..=16).map(fixture).collect::<Vec<_>>();
  r824_commit_prepared_batch(&mut h,&mut owner,&batch).unwrap();
  exec(&h.source,"DELETE FROM events WHERE event_id LIKE '%_15'").unwrap();
  assert!(r834_recover_private_owner(&p.0,"recovery",0,1,800).is_err());
 }
 #[test]fn r837_hash_tampered_event_refuses_owner_reconstruction(){
  let (p,mut h,mut owner)=fixture_pair();
  let batch=(1..=16).map(fixture).collect::<Vec<_>>();
  r824_commit_prepared_batch(&mut h,&mut owner,&batch).unwrap();
  exec(&h.source,"UPDATE events SET payload_json='{}'
   WHERE event_id LIKE '%_08'").unwrap();
  assert!(r834_recover_private_owner(&p.0,"recovery",0,1,800).is_err());
 }
 #[test]fn r838_recovered_owner_accepts_subsequent_batch_once(){
  let (p,mut h,_original)=fixture_pair();
  let batch=(1..=16).map(fixture).collect::<Vec<_>>();
  append_without_publication(&mut h,&batch);
  drop(h);
  let (mut restored,seen)=r834_recover_private_owner(&p.0,"recovery",0,1,800).unwrap();
  assert_eq!(seen,16);
  let mut h=R819OriginalPaperHistory::new(&p.0,"recovery").unwrap();
  let later=(17..=32).map(fixture).collect::<Vec<_>>();
  assert_eq!(r824_commit_prepared_batch(&mut h,&mut restored,&later),Ok((2,16)));
  assert_eq!(r366_query_num(&h.source,"SELECT COUNT(*) FROM events"),Ok(32));
  assert_eq!(restored.snapshot_venue("BTC",Venue::OkxSpot).unwrap().sequence,Some(32));
 }
 #[test]fn r839_missing_entire_last_committed_batch_blocks_restart(){
  let (p,mut h,mut owner)=fixture_pair();
  for from in [1_u64,17]{
   let items=(from..from+16).map(fixture).collect::<Vec<_>>();
   r824_commit_prepared_batch(&mut h,&mut owner,&items).unwrap();
  }
  exec(&h.source,"DELETE FROM events WHERE event_id LIKE '%_00000000000000000002_%'").unwrap();
  assert_eq!(r366_query_num(&h.source,"SELECT COUNT(*) FROM events"),Ok(16));
  assert!(r834_recover_private_owner(&p.0,"recovery",0,1,800).is_err());
 }
 #[test]fn r840_wrong_run_identity_never_recovers_other_source_events(){
  let (p,mut h,mut owner)=fixture_pair();
  let items=(1..=16).map(fixture).collect::<Vec<_>>();
  r824_commit_prepared_batch(&mut h,&mut owner,&items).unwrap();
  assert!(r834_recover_private_owner(&p.0,"different_run",0,1,800).is_err());
 }
}
