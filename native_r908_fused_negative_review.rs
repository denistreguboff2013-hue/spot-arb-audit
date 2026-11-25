// Astra P0-01 R908–R910: regression of fused full-chain validator,
// continuous verified prefix ACK and exact crash-safe Owner after partial ACK.
#[test] fn r908_nonconsecutive_future_outbox_seq_refuses_any_prefix_ack(){
 let (root,h,_owner)=pair(3);
 let oracle=r586_oracle();
 assert_eq!(r845_verified_burst(&root.0,&h.source,&h.sink,&oracle,2),Ok(2));
 // A fabricated "previously valid" chain with sequence gap (seq=4 instead
 // of 3) must NEVER be accepted, even when row payload hash is unchanged.
 exec(&h.source,"UPDATE mirror_outbox SET seq=4 WHERE seq=3;   UPDATE mirror_control SET latest_seq=4 WHERE id=1").unwrap();
 assert_eq!(r879_private_paper_prefix_ack(
    &root.0,&h.source,&h.sink,&oracle,"r879_probe"),
   Err(EventOutboxR366Error::Corrupt));
 assert_eq!(r366_query_num(&h.source,"SELECT cleanup_through FROM mirror_control"),Ok(0));
 assert_eq!(r366_query_num(&h.sink,"SELECT applied_seq FROM mirror_meta"),Ok(2));
}
#[test] fn r909_continuous_prefixes_with_remaining_future_rows(){
 let (root,h,owner)=pair(4);
 let oracle=r586_oracle();
 for seq in 1..=4_u64{
  assert_eq!(r845_verified_burst(&root.0,&h.source,&h.sink,&oracle,1),Ok(1));
  assert_eq!(r879_private_paper_prefix_ack(
   &root.0,&h.source,&h.sink,&oracle,"r879_probe"),Ok((1,4-seq)));
  assert_eq!(r366_query_num(&h.source,"SELECT cleanup_through FROM mirror_control"),Ok(seq));
  assert_eq!(r366_query_num(&h.sink,"SELECT applied_seq FROM mirror_meta"),Ok(seq));
  assert_eq!(r366_query_num(&h.source,"SELECT retained_records FROM mirror_control"),Ok(4-seq));
 }
 assert_eq!(r800_streaming_original_parity(&h.source,&h.sink,&oracle),Ok(128));
 let (recovered,count)=r834_recover_private_owner(&root.0,"r879_probe",0,1,800).unwrap();
 assert_eq!(count,128);
 assert_eq!(recovered.snapshot_venue("BTC",Venue::OkxSpot),owner.snapshot_venue("BTC",Venue::OkxSpot));
}
#[test] fn r910_mixed_chain64_four_independent_partial_acks(){
 let (root,h,owner)=pair(64);
 let oracle=r586_oracle();
 for cycle in 1..=4_u64{
  assert_eq!(r845_verified_burst(&root.0,&h.source,&h.sink,&oracle,16),Ok(16));
  assert_eq!(r879_private_paper_prefix_ack(
   &root.0,&h.source,&h.sink,&oracle,"r879_probe"),
   Ok((16,64-cycle*16)));
  assert_eq!(r366_query_num(&h.source,"SELECT retained_records FROM mirror_control"),Ok(64-cycle*16));
 }
 assert_eq!(r366_query_num(&h.sink,"SELECT COUNT(*) FROM events"),Ok(2048));
 assert_eq!(r366_query_num(&h.source,"SELECT COUNT(*) FROM mirror_outbox"),Ok(0));
 assert_eq!(r800_streaming_original_parity(&h.source,&h.sink,&oracle),Ok(2048));
 let (recovered,count)=r834_recover_private_owner(&root.0,"r879_probe",0,1,800).unwrap();
 assert_eq!(count,2048);
 assert_eq!(recovered.snapshot_venue("BTC",Venue::OkxSpot),owner.snapshot_venue("BTC",Venue::OkxSpot));
}
