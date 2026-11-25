// Astra P0-01 R819: strict, synchronous original 17-table PAPER events
// HistorySink. Research-only original-shaped PRIVATE /tmp file DB1/outbox.
// Commit MUST occur before Owner mutates a parsed book. NO REAL orders,
// no private exchange API, no legacy DB1 writes, no fake fills.
pub struct R819OriginalPaperHistory {
 root:PathBuf,source:Db,sink:Db,manifest:serde_json::Value,
 accepted:u64,committed_events:u64,run_id:String,
}
impl R819OriginalPaperHistory {
 pub fn accepted_count(&self)->u64{self.committed_events}
 pub fn new(root:&Path,run_id:&str)->Result<Self,EventOutboxR366Error>{
  if run_id.is_empty()||run_id.len()>80||!run_id.bytes()
    .all(|c|c.is_ascii_alphanumeric()||c==b'_'||c==b'-'){
   return Err(EventOutboxR366Error::Schema)
  }
  let src=r671_open(root,"r671-private-db1.sqlite",false)?;
  let dst=r671_open(root,"r671-private-db2.sqlite",false)?;
  let v=r586_oracle();
  let (ctrl,meta,_)=r756_snapshot(root,&src,&dst)?;
  if ctrl["state"]!="NORMAL"||meta["phase"]!="SYNCED"{
   return Err(EventOutboxR366Error::Conflict)
  }
  let seq=r726_u(&ctrl,"latest_seq")?;
  Ok(Self{root:root.to_owned(),source:src,sink:dst,manifest:v,
    accepted:seq,committed_events:0,run_id:run_id.to_owned()})
 }
 fn record(&mut self,frame:&crate::RawFrame,update:&crate::Update)
  ->Result<u64,EventOutboxR366Error>{
  if frame.venue!=update.venue||frame.epoch!=update.book.epoch
    ||frame.received_wall_ms!=update.book.received_wall_ms
    ||frame.received_ns!=update.book.received_ns
    ||frame.received_wall_ms<=0
    ||update.base.is_empty()||update.base.len()>30
    ||frame.raw.len()>32_768||frame.raw.is_empty(){
   return Err(EventOutboxR366Error::Corrupt)
  }
  // Protect source disk from a permanently absent receiver. This is
  // backpressure, not a policy to omit or sample accepted frames.
  let mut pending=r366_query_num(&self.source,
    "SELECT retained_records FROM mirror_control WHERE id=1")?;
  if pending>=64{
   // R1087: bounded BACKPRESSURE, only for explicit private PUBLIC WS
   // research. No event is accepted/published until DB2/ACK creates room.
   // Installed PAPER retains fail-fast behavior; no missing receiver
   // can cause unbounded RAM/disk growth or silent frame sampling.
   if std::env::var("PAPER_R1087_BOUNDED_ACK_WAIT")
        .ok().as_deref()!=Some("YES"){
    return Err(EventOutboxR366Error::Pressure)
   }
   let deadline=std::time::Instant::now()+std::time::Duration::from_secs(8);
   while pending>=64{
    if std::time::Instant::now()>=deadline{
     return Err(EventOutboxR366Error::Pressure)
    }
    std::thread::sleep(std::time::Duration::from_millis(20));
    pending=r366_query_num(&self.source,
      "SELECT retained_records FROM mirror_control WHERE id=1")?;
   }
  }
  let seq=r366_query_num(&self.source,
    "SELECT latest_seq FROM mirror_control WHERE id=1")?
    .checked_add(1).ok_or(EventOutboxR366Error::Pressure)?;
  let event_id=format!("paper_r819_{}_{seq:020}",self.run_id);
  let payload=serde_json::json!({
   "schema":"SPOTARB/NATIVE/PAPER-HISTORY/R819/V1",
   "venue":match frame.venue{
    crate::Venue::OkxSpot=>"okx_public_spot",
    crate::Venue::BinanceFutures=>"binance_public_futures"
   },
   "epoch":frame.epoch,
   "received_ns":frame.received_ns,
   "received_wall_ms":frame.received_wall_ms,
   "base":update.base,
   "bid_levels":update.book.bids.len(),
   "ask_levels":update.book.asks.len(),
   "source_frame_digest":r366_sha(&frame.raw),
   "exchange_attested":false,
   "real_orders":false,
  });
  let canonical=r386_python_mirror_canonical(&payload)?;
  let row=serde_json::json!({
   "event_id":event_id,
   "schema_version":1,"mode":"paper",
   "category":"market_data","name":"public_ws_depth_accepted",
   "source":"native_rust_public_paper",
   "run_id":self.run_id,
   "occurred_at_ms":frame.received_wall_ms,
   "received_at_ms":frame.received_wall_ms,
   "correlation_id":null,"signal_id":null,
   "base":update.base,
   "shadow_group_id":null,"comparison_id":null,"reason_code":null,
   "verification_status":"unverified",
   "is_exchange_verified":0,
   "payload_json":canonical,
   "verification_json":"{}",
   "content_hash":r366_sha(&canonical),
  });
  let op=r659_op(&self.manifest,"events",&row,"insert")?;
  let committed=r756_append(&self.root,&self.source,&self.sink,&self.manifest,&[op])?;
  if committed!=seq{return Err(EventOutboxR366Error::Corrupt)}
  self.accepted=committed;
  self.committed_events=self.committed_events.checked_add(1)
   .ok_or(EventOutboxR366Error::Pressure)?;
  Ok(committed)
 }
}
impl crate::HistorySink for R819OriginalPaperHistory {
 fn accept(&mut self,original:&crate::RawFrame,update:&crate::Update)
  ->Result<(),crate::CoreError>{
  self.record(original,update).map(|_|()).map_err(|e|{
    if std::env::var("PAPER_R1087_BOUNDED_ACK_WAIT")
        .ok().as_deref()==Some("YES"){
       eprintln!("R1087 private original DB1 refused: {e:?}");
    }
    crate::CoreError::HistoryPressure
  })
 }
}

#[cfg(test)]
mod r819_tests {
 use super::*;
 use crate::{Book,RawFrame,Update,Venue,Owner,HistorySink};
 use std::os::unix::fs::DirBuilderExt;
 struct Guard(PathBuf);
 impl Guard{
  fn new()->Self {
   let mut p=std::env::temp_dir();
   let n=std::time::SystemTime::now()
     .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
   p.push(format!("astra_r671_file_{}_{}_r819",std::process::id(),n));
   fs::DirBuilder::new().mode(0o700).create(&p).unwrap();
   let pair=r756_initialize(&p).unwrap();drop(pair);
   Self(p)
  }
 }
 impl Drop for Guard {fn drop(&mut self){let _=fs::remove_dir_all(&self.0);}}
 fn event(i:u64)->(RawFrame,Update){
  let venue=Venue::OkxSpot;
  let ns=1_000_000_000+i*10_000_000;
  let ms=1_800_000_000_000_i64+i as i64*10;
  let frame=RawFrame{source:None,venue,epoch:1,received_ns:ns,received_wall_ms:ms,
    raw:format!("{{\"source\":\"test-public-depth\",\"sequence\":{i}}}")};
  let book=Book{bids:vec![(101.0,1.0)],asks:vec![(102.0,1.0)],
    received_ns:ns,received_wall_ms:ms,exchange_ts_ms:Some(ms),
    changed_ns:ns,epoch:1,sequence:Some(i as i64)};
  let update=Update{base:"BTC".to_string(),venue,book};
  (frame,update)
 }
 #[test]fn r819_owner_ingest_requires_committed_private_original_db1_event(){
  let root=Guard::new();
  let mut history=R819OriginalPaperHistory::new(&root.0,"session_1").unwrap();
  let mut owner=Owner::new(0,1,800).unwrap();
  let (frame,update)=event(1);
  assert_eq!(owner.ingest_parsed(&frame,update,&mut history),Ok(None));
  assert_eq!(history.accepted,1);
  let db=r671_open(&root.0,"r671-private-db1.sqlite",false).unwrap();
  assert_eq!(r366_query_num(&db,"SELECT COUNT(*) FROM events"),Ok(1));
  let mut q=Stmt::new(&db,"SELECT mode,is_exchange_verified,name,
    verification_status FROM events").unwrap();
  assert_eq!(q.step().unwrap(),SQLITE_ROW);
  assert_eq!(q.strval(0).unwrap(),"paper");
  assert_eq!(q.num(1).unwrap(),0);
  assert_eq!(q.strval(2).unwrap(),"public_ws_depth_accepted");
  assert_eq!(q.strval(3).unwrap(),"unverified");
  let d=r671_open(&root.0,"r671-private-db2.sqlite",false).unwrap();
  assert_eq!(r366_query_num(&d,"SELECT COUNT(*) FROM events"),Ok(0));
  let v=r586_oracle();
  assert_eq!(r805_replay_window(&root.0,&db,&d,&v,32),Ok(1));
  assert_eq!(r805_cleanup_window(&root.0,&db,&d,&v),Ok((1,0)));
  assert_eq!(r800_streaming_original_parity(&db,&d,&v),Ok(1));
 }
 #[test]fn r820_receiver_restart_preserves_owner_event_identity(){
  let root=Guard::new();
  let mut h=R819OriginalPaperHistory::new(&root.0,"same_run").unwrap();
  let (a,b)=event(1);h.accept(&a,&b).unwrap();drop(h);
  let mut h=R819OriginalPaperHistory::new(&root.0,"same_run").unwrap();
  let (a,b)=event(2);h.accept(&a,&b).unwrap();
  let db=r671_open(&root.0,"r671-private-db1.sqlite",false).unwrap();
  assert_eq!(r366_query_num(&db,"SELECT COUNT(DISTINCT event_id) FROM events"),Ok(2));
  let v=r586_oracle();
  let ds=r671_open(&root.0,"r671-private-db2.sqlite",false).unwrap();
  assert_eq!(r805_replay_window(&root.0,&db,&ds,&v,32),Ok(2));
  assert_eq!(r805_cleanup_window(&root.0,&db,&ds,&v),Ok((2,0)));
 }
 #[test]fn r821_backpressure_rejects_owner_mutation_not_discard_event(){
  let root=Guard::new();
  let mut h=R819OriginalPaperHistory::new(&root.0,"bounded").unwrap();
  let mut owner=Owner::new(0,1,800).unwrap();
  for i in 1..=64{
   let (a,b)=event(i);
   assert!(owner.ingest_parsed(&a,b,&mut h).is_ok());
  }
  let prior=owner.snapshot_venue("BTC",Venue::OkxSpot).unwrap();
  let (a,b)=event(65);
  assert_eq!(owner.ingest_parsed(&a,b,&mut h),
    Err(crate::CoreError::HistoryPressure));
  assert_eq!(owner.snapshot_venue("BTC",Venue::OkxSpot),Some(prior));
  let db=r671_open(&root.0,"r671-private-db1.sqlite",false).unwrap();
  assert_eq!(r366_query_num(&db,"SELECT COUNT(*) FROM events"),Ok(64));
 }
 #[test]fn r822_refuses_mismatched_frame_before_db1_write(){
  let root=Guard::new();
  let mut h=R819OriginalPaperHistory::new(&root.0,"mismatch").unwrap();
  let (a,mut b)=event(1);
  b.book.received_ns+=1;
  assert_eq!(h.accept(&a,&b),Err(crate::CoreError::HistoryPressure));
  let db=r671_open(&root.0,"r671-private-db1.sqlite",false).unwrap();
  assert_eq!(r366_query_num(&db,"SELECT COUNT(*) FROM events"),Ok(0));
 }
}
