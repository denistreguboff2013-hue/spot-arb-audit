// Astra P0-01 R868: canonical Owner publication must be atomic *after*
// durable original DB1/outbox batch ACK. Private test DBs only.
// The production PAPER and original historical DBs are never opened.
#[cfg(test)]
mod r868_tests {
 use super::*;
 use crate::{Book,Owner,RawFrame,Update,Venue};
 use std::os::unix::fs::DirBuilderExt;
 struct Temp(PathBuf);
 impl Temp {
  fn new()->Self{
   let mut root=std::env::temp_dir();
   let stamp=std::time::SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
   root.push(format!("astra_r671_file_{}_{}_r868",std::process::id(),stamp));
   fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
   drop(r756_initialize(&root).unwrap());
   Self(root)
  }
 }
 impl Drop for Temp {
  fn drop(&mut self){let _=fs::remove_dir_all(&self.0);}
 }
 fn event(i:u64,venue:Venue)->(RawFrame,Update){
  let ns=1_000_000_000+i*10_000_000;
  let ms=1_800_000_000_000_i64+(i as i64)*10;
  let label=match venue {
   Venue::OkxSpot=>"okx_public_spot",
   Venue::BinanceFutures=>"binance_public_futures",
  };
  let raw=format!("{{\"source\":\"r868_private\",\"venue\":\"{label}\",\"seq\":{i}}}");
  let f=RawFrame{source:None,venue,epoch:1,received_ns:ns,received_wall_ms:ms,raw};
  let b=Book{bids:vec![(102.0,1.0)],asks:vec![(103.0,1.0)],
   received_ns:ns,received_wall_ms:ms,exchange_ts_ms:Some(ms),
   changed_ns:ns,epoch:1,sequence:Some(i as i64)};
  (f,Update{base:"BTC".to_string(),venue,book:b})
 }
 #[test]fn r868_both_venues_published_atomically_after_one_db1_commit(){
  let root=Temp::new();
  let mut h=R819OriginalPaperHistory::new(&root.0,"atomic_pub").unwrap();
  let mut owner=Owner::new(0,1,800).unwrap();
  let inputs=vec![event(1,Venue::OkxSpot),event(2,Venue::BinanceFutures)];
  assert_eq!(r824_commit_prepared_batch(&mut h,&mut owner,&inputs),Ok((1,2)));
  let src=r671_open(&root.0,"r671-private-db1.sqlite",false).unwrap();
  assert_eq!(r366_query_num(&src,"SELECT COUNT(*) FROM events"),Ok(2));
  assert_eq!(r366_query_num(&src,"SELECT COUNT(*) FROM mirror_outbox"),Ok(1));
  let (recovered,n)=r834_recover_private_owner(&root.0,"atomic_pub",0,1,800).unwrap();
  assert_eq!(n,2);
  for v in [Venue::OkxSpot,Venue::BinanceFutures]{
   assert_eq!(owner.snapshot_venue("BTC",v),recovered.snapshot_venue("BTC",v));
   assert!(owner.snapshot_venue("BTC",v).is_some());
  }
  let dst=r671_open(&root.0,"r671-private-db2.sqlite",false).unwrap();
  let manifest=r586_oracle();
  assert_eq!(r845_verified_burst(&root.0,&src,&dst,&manifest,128),Ok(1));
  assert_eq!(r860_bulk_verified_ack(&root.0,&src,&dst,&manifest,1024),Ok((1,0)));
  assert_eq!(r800_streaming_original_parity(&src,&dst,&manifest),Ok(2));
 }
 #[test]fn r869_invalid_second_venue_never_publishes_first(){
  let root=Temp::new();
  let mut h=R819OriginalPaperHistory::new(&root.0,"deny_pub").unwrap();
  let mut owner=Owner::new(0,1,800).unwrap();
  let mut inputs=vec![event(1,Venue::OkxSpot),event(2,Venue::BinanceFutures)];
  inputs[1].1.book.received_wall_ms+=100;
  assert!(r824_commit_prepared_batch(&mut h,&mut owner,&inputs).is_err());
  for v in [Venue::OkxSpot,Venue::BinanceFutures]{
   assert!(owner.snapshot_venue("BTC",v).is_none());
  }
  assert_eq!(r366_query_num(&h.source,"SELECT COUNT(*) FROM events"),Ok(0));
  assert_eq!(r366_query_num(&h.source,"SELECT COUNT(*) FROM mirror_outbox"),Ok(0));
 }
 #[test]fn r870_explicit_paper_origin_never_exchange_attested(){
  let root=Temp::new();
  let mut h=R819OriginalPaperHistory::new(&root.0,"verify_paper").unwrap();
  let mut owner=Owner::new(0,1,800).unwrap();
  let inputs=(1..=32).map(|i|event(i,Venue::OkxSpot)).collect::<Vec<_>>();
  assert_eq!(r824_commit_prepared_batch(&mut h,&mut owner,&inputs),Ok((1,32)));
  let wrong=r366_query_num(&h.source,"SELECT COUNT(*) FROM events
   WHERE mode!='paper' OR is_exchange_verified!=0 OR verification_status!='unverified'
    OR name!='public_ws_depth_accepted'").unwrap();
  assert_eq!(wrong,0);
  assert_eq!(r366_query_num(&h.source,"SELECT COUNT(*) FROM events"),Ok(32));
 }
 #[test]fn r871_event_counter_overflow_is_denied_before_db1_commit(){
  let root=Temp::new();
  let mut h=R819OriginalPaperHistory::new(&root.0,"overflow_guard").unwrap();
  let mut owner=Owner::new(0,1,800).unwrap();
  h.committed_events=u64::MAX;
  let inputs=vec![event(1,Venue::OkxSpot)];
  assert_eq!(r824_commit_prepared_batch(&mut h,&mut owner,&inputs),
   Err(crate::CoreError::HistoryPressure));
  assert!(owner.snapshot_venue("BTC",Venue::OkxSpot).is_none());
  assert_eq!(r366_query_num(&h.source,"SELECT COUNT(*) FROM events"),Ok(0));
  assert_eq!(r366_query_num(&h.source,"SELECT COUNT(*) FROM mirror_outbox"),Ok(0));
 }
}
