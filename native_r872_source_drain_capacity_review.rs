// Astra P0-01 R872: independent interleaved PRIVATE source/DB2
// workload. Not a claim of live admission rate or production deployment.
#[cfg(test)]
mod r872_tests {
 use super::*;
 use crate::{Book,RawFrame,Update,Venue,Owner};
 use std::os::unix::fs::DirBuilderExt;
 use std::time::{Instant,SystemTime,UNIX_EPOCH};
 struct Temp(PathBuf);
 impl Temp {
  fn new()->Self {
   let mut p=std::env::temp_dir();
   let stamp=SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
   p.push(format!("astra_r671_file_{}_{}_r872",std::process::id(),stamp));
   fs::DirBuilder::new().mode(0o700).create(&p).unwrap();
   drop(r756_initialize(&p).unwrap());
   Self(p)
  }
 }
 impl Drop for Temp {fn drop(&mut self){let _=fs::remove_dir_all(&self.0);}}
 fn event(i:u64)->(RawFrame,Update){
  let venue=Venue::OkxSpot;
  let ns=1_000_000_000+i*10_000_000;
  let ms=1_800_000_000_000_i64+i as i64*10;
  let raw=format!("{{\"r872_public_depth_sequence\":{i},\"levels\":2}}");
  let f=RawFrame{source:None,venue,epoch:1,received_ns:ns,received_wall_ms:ms,raw};
  let book=Book{bids:vec![(101.0,1.0)],asks:vec![(102.0,1.0)],
   received_ns:ns,received_wall_ms:ms,exchange_ts_ms:Some(ms),
   changed_ns:ns,epoch:1,sequence:Some(i as i64)};
  (f,Update{base:"BTC".into(),venue,book})
 }
 fn pair()->(Temp,R819OriginalPaperHistory,Owner){
  let root=Temp::new();
  let h=R819OriginalPaperHistory::new(&root.0,"r872_sustained").unwrap();
  (root,h,Owner::new(0,1,800).unwrap())
 }
 fn count(db:&Db,table:&str)->u64{
  r366_query_num(db,&format!("SELECT COUNT(*) FROM {table}")).unwrap()
 }
 fn drain(root:&Path,h:&R819OriginalPaperHistory)->(u64,u64) {
  let v=r586_oracle();
  let mut delivered=0;
  for _ in 0..3 {
   let n=r845_verified_burst(root,&h.source,&h.sink,&v,128).unwrap();
   delivered+=n;
   if n==0{break}
  }
  let (acked,remaining)=r860_bulk_verified_ack(root,&h.source,&h.sink,&v,1024).unwrap();
  assert_eq!(remaining,0);
  (delivered,acked)
 }
 // R872: same scenario uses committed PAPER events, private DB1 and
 // the existing fully-chain-verified DB2 worker, interleaved by 128 events.
 #[test]fn r872_interleaved_1024_events_no_loss_and_recovery(){
  let (root,mut h,mut owner)=pair();
  let start=Instant::now();
  let mut acked=0;
  let mut max_pending=0;
  for first in (1..=1024_u64).step_by(128){
   for batch_first in (first..first+128).step_by(32){
    let batch=(batch_first..batch_first+32).map(event).collect::<Vec<_>>();
    r824_commit_prepared_batch(&mut h,&mut owner,&batch).unwrap();
    max_pending=max_pending.max(count(&h.source,"mirror_outbox"));
   }
   let (applied,cleared)=drain(&root.0,&h);
   assert_eq!((applied,cleared),(4,4));
   acked+=cleared;
  }
  let seconds=start.elapsed().as_secs_f64();
  let rate=1024.0/seconds;
  assert_eq!(acked,32);
  assert_eq!(max_pending,4);
  assert_eq!(count(&h.source,"events"),1024);
  assert_eq!(count(&h.sink,"events"),1024);
  assert_eq!(count(&h.source,"mirror_outbox"),0);
  assert_eq!(r366_query_num(&h.source,"SELECT latest_seq FROM mirror_control"),Ok(32));
  assert_eq!(r366_query_num(&h.sink,"SELECT applied_seq FROM mirror_meta"),Ok(32));
  let (recovered,seen)=r834_recover_private_owner(&root.0,"r872_sustained",0,1,800).unwrap();
  assert_eq!(seen,1024);
  assert_eq!(recovered.snapshot_venue("BTC",Venue::OkxSpot),
             owner.snapshot_venue("BTC",Venue::OkxSpot));
  eprintln!("R872 PRIVATE INTERLEAVED 1024 events elapsed_s={seconds:.3} events_per_s={rate:.1} max_pending_envelopes={max_pending} NO_PRODUCTION_CAPACITY_CERTIFICATE");
 }
 // R873: pending count is 64 *outbox envelopes*, not 64 events;
 // 64 x 32 events are allowed, the next incoming batch fails closed.
 #[test]fn r873_64_pending_envelopes_stop_owner_then_ack_resumes(){
  let (root,mut h,mut owner)=pair();
  for first in (1..=2048_u64).step_by(32){
   let batch=(first..first+32).map(event).collect::<Vec<_>>();
   r824_commit_prepared_batch(&mut h,&mut owner,&batch).unwrap();
  }
  assert_eq!(count(&h.source,"mirror_outbox"),64);
  assert_eq!(count(&h.source,"events"),2048);
  let prior=owner.snapshot_venue("BTC",Venue::OkxSpot);
  let blocked=(2049..=2080_u64).map(event).collect::<Vec<_>>();
  assert_eq!(r824_commit_prepared_batch(&mut h,&mut owner,&blocked),
   Err(crate::CoreError::HistoryPressure));
  assert_eq!(prior,owner.snapshot_venue("BTC",Venue::OkxSpot));
  assert_eq!(count(&h.source,"events"),2048);
  let (copied,acknowledged)=drain(&root.0,&h);
  assert_eq!((copied,acknowledged),(64,64));
  assert_eq!(count(&h.source,"mirror_outbox"),0);
  assert_eq!(r824_commit_prepared_batch(&mut h,&mut owner,&blocked),Ok((65,32)));
  let (copied,acknowledged)=drain(&root.0,&h);
  assert_eq!((copied,acknowledged),(1,1));
  assert_eq!(count(&h.sink,"events"),2080);
  assert_eq!(count(&h.source,"mirror_outbox"),0);
 }
}
