// Astra P0-01 R960: actual >10,000 fully source-durable,
/// independently verified destination/ACKed original PAPER events.
/// PRIVATE only; tests bounded streaming restore, never portfolio cash
/// /orders/Telegram or installed PAPER upgrade.
#[test]fn r960_owner_recovery_12288_durable_events_after_fused_ack(){
 let root=Root::new();let v=r586_oracle();
 let src=r671_open(&root.0,"r671-private-db1.sqlite",false).unwrap();
 let dst=r671_open(&root.0,"r671-private-db2.sqlite",false).unwrap();
 r924_install_private_paper_only_guards(&src,&dst,&v).unwrap();
 r932_certify_private_empty_genesis(&root.0,&src,&dst,&v,"r960_probe").unwrap();
 let mut h=R819OriginalPaperHistory::new(&root.0,"r960_probe").unwrap();
 let mut owner=Owner::new(0,1,800).unwrap();
 let total=12_288u64;
 for start_seq in (1..=total/32).step_by(16){
  for seq in start_seq..start_seq+16{
   let first=(seq-1)*32+1;
   assert_eq!(r824_commit_prepared_batch(&mut h,&mut owner,
    &(first..first+32).map(event).collect::<Vec<_>>()),Ok((seq,32)));
  }
  assert_eq!(r954_private_fused_verified_replay_and_ack(
   &root.0,&src,&dst,&v,"r960_probe",||Ok(())),Ok((16,16)));
 }
 assert_eq!(r366_query_num(&src,"SELECT COUNT(*) FROM mirror_outbox"),Ok(0));
 assert_eq!(r366_query_num(&src,"SELECT COUNT(*) FROM events"),Ok(total));
 assert_eq!(r366_query_num(&dst,"SELECT COUNT(*) FROM events"),Ok(total));
 assert_eq!(r800_streaming_original_parity(&src,&dst,&v),Ok(total));
 let start=std::time::Instant::now();
 let (restored,count)=r834_recover_private_owner(&root.0,"r960_probe",0,1,800)
  .expect("R960 full committed owner must restore >10k without truncation");
 assert_eq!(count,total);
 assert_eq!(restored.snapshot_venue("BTC",Venue::OkxSpot),
   owner.snapshot_venue("BTC",Venue::OkxSpot));
 eprintln!("R960 PRIVATE original 17-table 12288 events exact restore ms={:.2}",
  start.elapsed().as_secs_f64()*1000.0);
}
