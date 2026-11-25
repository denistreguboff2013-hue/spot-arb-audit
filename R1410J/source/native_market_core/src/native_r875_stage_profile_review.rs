// Astra P0-01 R875: stage-level private source/DB2/ACK profiling.
// Diagnostic only: one test process, synthetic events; NOT a live capacity gate.
#[cfg(test)]
mod r875_tests {
 use super::*;
 use crate::{Book,Owner,RawFrame,Update,Venue};
 use std::os::unix::fs::DirBuilderExt;
 use std::time::{Duration,Instant,SystemTime,UNIX_EPOCH};
 struct Root(PathBuf);
 impl Root {
  fn new()->Self {
   let mut p=std::env::temp_dir();
   let stamp=SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
   p.push(format!("astra_r671_file_{}_{}_r875",std::process::id(),stamp));
   fs::DirBuilder::new().mode(0o700).create(&p).unwrap();
   drop(r756_initialize(&p).unwrap());
   Self(p)
  }
 }
 impl Drop for Root {fn drop(&mut self){let _=fs::remove_dir_all(&self.0);}}
 fn event(i:u64)->(RawFrame,Update) {
  let venue=Venue::OkxSpot;
  let ns=1_000_000_000+i*10_000_000;
  let ms=1_800_000_000_000_i64+i as i64*10;
  let f=RawFrame{source:None, venue,epoch:1,received_ns:ns,received_wall_ms:ms,
    raw:format!("{{\"r875_public_depth_sequence\":{i},\"levels\":2}}") };
  let book=Book { bids:vec![(101.0,1.0)],asks:vec![(102.0,1.0)],
    received_ns:ns,received_wall_ms:ms,exchange_ts_ms:Some(ms),
    changed_ns:ns,epoch:1,sequence:Some(i as i64) };
  (f,Update{base:"BTC".into(),venue,book})
 }
 fn quantiles(samples:&[Duration])->(f64,f64,f64) {
  assert!(!samples.is_empty());
  let mut v=samples.iter().map(|x|x.as_secs_f64()*1000.0).collect::<Vec<_>>();
  v.sort_by(|a,b|a.total_cmp(b));
  let index=|p:usize| (v.len()*p).div_ceil(100).saturating_sub(1);
  (v[index(50)],v[index(95)],v[index(99)])
 }
 #[test]
 fn r875_private_breakdown_source_replay_ack_and_owner_restore() {
  let root=Root::new();
  let mut h=R819OriginalPaperHistory::new(&root.0,"r875_profile").unwrap();
  let mut owner=Owner::new(0,1,800).unwrap();
  let oracle=r586_oracle();
  let mut source_times=Vec::new();
  let mut replay_times=Vec::new();
  let mut ack_times=Vec::new();
  let start=Instant::now();
  for block in (1..=1024_u64).step_by(128) {
   for first in (block..block+128).step_by(32) {
    let batch=(first..first+32).map(event).collect::<Vec<_>>();
    let before=Instant::now();
    assert_eq!(r824_commit_prepared_batch(&mut h,&mut owner,&batch),
      Ok(((first+31)/32,32)));
    source_times.push(before.elapsed());
   }
   let before=Instant::now();
   assert_eq!(r845_verified_burst(&root.0,&h.source,&h.sink,&oracle,128),Ok(4));
   replay_times.push(before.elapsed());
   let before=Instant::now();
   assert_eq!(r860_bulk_verified_ack(&root.0,&h.source,&h.sink,&oracle,1024),Ok((4,0)));
   ack_times.push(before.elapsed());
  }
  let pipeline_s=start.elapsed().as_secs_f64();
  let before=Instant::now();
  let (restored,count)=r834_recover_private_owner(&root.0,"r875_profile",0,1,800).unwrap();
  let restore_ms=before.elapsed().as_secs_f64()*1000.0;
  assert_eq!(count,1024);
  assert_eq!(restored.snapshot_venue("BTC",Venue::OkxSpot),
             owner.snapshot_venue("BTC",Venue::OkxSpot));
  assert_eq!(r366_query_num(&h.source,"SELECT COUNT(*) FROM events"),Ok(1024));
  assert_eq!(r366_query_num(&h.sink,"SELECT COUNT(*) FROM events"),Ok(1024));
  assert_eq!(r366_query_num(&h.source,"SELECT retained_records FROM mirror_control"),Ok(0));
  assert_eq!(r366_query_num(&h.source,"SELECT latest_seq FROM mirror_control"),Ok(32));
  assert_eq!(r366_query_num(&h.sink,"SELECT applied_seq FROM mirror_meta"),Ok(32));
  let (s50,s95,s99)=quantiles(&source_times);
  let (r50,r95,r99)=quantiles(&replay_times);
  let (a50,a95,a99)=quantiles(&ack_times);
  eprintln!("R875 PRIVATE 1024 synthetic events; {:.2} events/s (not capacity certification)",1024.0/pipeline_s);
  eprintln!("R875 DB1 source 32 batches p50/p95/p99 ms {:.2}/{:.2}/{:.2}",s50,s95,s99);
  eprintln!("R875 DB2 verified replay 8 bursts p50/p95/p99 ms {:.2}/{:.2}/{:.2}",r50,r95,r99);
  eprintln!("R875 DB1 verified parity/ACK 8 cycles p50/p95/p99 ms {:.2}/{:.2}/{:.2}",a50,a95,a99);
  eprintln!("R875 Owner restore 1024 accepted event ms {:.2}; PRIVATE only",restore_ms);
 }
}
