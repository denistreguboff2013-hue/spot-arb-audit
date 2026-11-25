// Astra P0-01 R877-R878: independent paced source/verified DB2/ACK soak.
// Short private synthetic acceptance probe only; do NOT install as daemon.
fn r900_private_wal_checkpoint(db:&Db){
 let n=std::env::var("ASTRA_R900_WAL_PAGES").ok()
  .and_then(|s|s.parse::<u64>().ok()).unwrap_or(10);
 assert!(matches!(n,10|100|1000),"R900 test WAL pages allowlist");
 exec(db,&format!("PRAGMA wal_autocheckpoint={n};"))
  .expect("R900 private SQLite WAL setting rejected");
}
// Astra P0-01 R911: optional test-only two-PID DB1/replay/ACK
// latency measurements. No change to production or test capacity limits.
fn r911_quantiles_ms(data:&[f64])->serde_json::Value{
 if data.is_empty(){return serde_json::Value::Null}
 let mut v=data.to_vec();
 v.sort_by(|a,b|a.total_cmp(b));
 let at=|percent:usize|v[(v.len()*percent).div_ceil(100).saturating_sub(1)];
 serde_json::json!({"count":v.len(),"p50_ms":at(50),"p95_ms":at(95),
  "p99_ms":at(99),"max_ms":v[v.len()-1],
  "total_ms":v.iter().sum::<f64>()})
}
fn r911_profile_on()->bool{
 std::env::var("ASTRA_R911_PROFILE").ok().as_deref()==Some("1")
}

// Only PRIVATE R877 test receiver. Codes are SQLite primary BUSY=5,
// LOCKED=6 (extended variants masked). Unknown SQL errors are never retried.
fn r1052_private_sqlite_busy(src:&Db,dst:&Db)->bool{
 [src,dst].iter().any(|db|{
  let code=unsafe{sqlite3_extended_errcode(db.0)}&0xff;
  code==5||code==6
 })
}
fn r877_parameters()->(u64,f64){
 let events=std::env::var("ASTRA_R877_EVENTS").unwrap().parse::<u64>().unwrap();
 let rate=std::env::var("ASTRA_R877_RATE").unwrap().parse::<f64>().unwrap();
 // R965 explicit private endurance opt-in to 65,536 events; ordinary
 // R961 remains bounded <=32,768, 64 original outbox slots unchanged.
 let maximum=if std::env::var("ASTRA_R965_EXTENDED").ok().as_deref()==Some("YES"){
  65_536
 }else{32_768};
 assert!((32..=maximum).contains(&events)&&events%32==0);
 assert!(rate.is_finite()&&(100.0..=1800.0).contains(&rate));
 (events,rate)
}
fn r877_child_root()->Option<PathBuf> {
 let p=PathBuf::from(std::env::var_os("ASTRA_R877_ROOT")?);
 r671_guard(&p,"r671-private-db1.sqlite").ok()?;
 Some(p)
}
#[test] fn r877_paced_writer_child(){
 let Some(root)=r877_child_root() else{return};
 let (events,rate)=r877_parameters();
 let mut h=R819OriginalPaperHistory::new(&root,"r877_soak").unwrap();
 r900_private_wal_checkpoint(&h.source);
 let mut owner=Owner::new(0,1,800).unwrap();
 let start=Instant::now();
 let mut peak=0;let mut committed=0u64;let mut pressure=false;
 let mut db1_commit_ms=Vec::new();
 let mut observed_two_sec=Vec::<u64>::new();
 for first in (1..=events).step_by(32){
  let deadline=if std::env::var("ASTRA_R966_BURST").ok().as_deref()==Some("YES"){
   // Deterministic 4 s cycles: 2 s at 1800/s then 2 s at
   // 600/s. Exactly 4800 events/cycle, 1200/s average.
   assert_eq!(rate,1200.0);
   let i=first-1;let cycle=i/4800;let within=i%4800;
   let offset=if within<3600{within as f64/1800.0}
     else{2.0+(within-3600) as f64/600.0};
   Duration::from_secs_f64(cycle as f64*4.0+offset)
  }else{Duration::from_secs_f64((first-1) as f64/rate)};
  if let Some(delay)=deadline.checked_sub(start.elapsed()){std::thread::sleep(delay)}
  let batch=(first..first+32).map(event).collect::<Vec<_>>();
  let before=Instant::now();
  let committed_result=r824_commit_prepared_batch(&mut h,&mut owner,&batch);
  if r911_profile_on(){db1_commit_ms.push(before.elapsed().as_secs_f64()*1000.0)}
  match committed_result{
   Ok((seq,32))=>{
    committed+=32;
    assert_eq!(seq,committed/32);
    if std::env::var("ASTRA_R967_WINDOW").ok().as_deref()==Some("YES"){
     // Count ONLY completed durable source-transaction events, using
     // real monotonic elapsed time, not the target scheduler deadline.
     let bucket=(start.elapsed().as_millis()/2000) as usize;
     assert!(bucket<=30,"R967 delayed burst window beyond 60 sec");
     if observed_two_sec.len()<=bucket{observed_two_sec.resize(bucket+1,0)}
     observed_two_sec[bucket]+=32;
    }
   },
   Err(crate::CoreError::HistoryPressure)=>{pressure=true;break},
   other=>panic!("R877 writer unexpected error or unverified commit {other:?}"),
  }
  peak=peak.max(r366_query_num(&h.source,
      "SELECT retained_records FROM mirror_control WHERE id=1").unwrap());
 }
 let seconds=start.elapsed().as_secs_f64();
 let result=serde_json::json!({"committed":committed,"pressure":pressure,
   "peak_pending_envelopes":peak,"elapsed_seconds":seconds,
   "target_events":events,"target_rate":rate,
   "observed_two_second_events":observed_two_sec,
   "db1_commit":r911_quantiles_ms(&db1_commit_ms)});
 std::fs::write(root.join("r877-writer-result.json"),serde_json::to_vec(&result).unwrap()).unwrap();
 eprintln!("R877 WRITER pid={} {} committed_rate={:.1}/s",std::process::id(),result,committed as f64/seconds);
}
#[test] fn r877_verified_receiver_child(){
 let Some(root)=r877_child_root() else{return};
 let src=r671_open(&root,"r671-private-db1.sqlite",false).unwrap();
 let dst=r671_open(&root,"r671-private-db2.sqlite",false).unwrap();
 r900_private_wal_checkpoint(&src);
 r900_private_wal_checkpoint(&dst);
 let oracle=r586_oracle();
 let started=Instant::now();
 let use_prefix=std::env::var("ASTRA_R877_PREFIX").ok().as_deref()==Some("1");
 let replay_min=std::env::var("ASTRA_R877_REPLAY_MIN").ok().and_then(|s|s.parse::<u64>().ok()).unwrap_or(1);
 let ack_min=std::env::var("ASTRA_R877_ACK_MIN").ok().and_then(|s|s.parse::<u64>().ok()).unwrap_or(16);
 assert!((1..=32).contains(&replay_min)&&(1..=48).contains(&ack_min));
 let mut copied=0u64;let mut acked=0u64;let mut peak=0u64;
 let mut replay_cycles=0u64;let mut ack_cycles=0u64;let mut idle=0u64;
 let mut db2_replay_ms=Vec::new();let mut ack_ms=Vec::new();
 loop {
  let timeout=if std::env::var("ASTRA_R965_EXTENDED").ok().as_deref()==Some("YES"){
   180
  }else{60};
  assert!(started.elapsed()<Duration::from_secs(timeout),"R877 reader failed progress timeout");
  // These are scheduling hints ONLY. DB2 replay and ACK have their own
  // transactional validation; never ACK based on these unfenced readings.
  let pending=r366_query_num(&src,"SELECT retained_records FROM mirror_control WHERE id=1").unwrap();
  let latest=r366_query_num(&src,"SELECT latest_seq FROM mirror_control WHERE id=1").unwrap();
  let applied=r366_query_num(&dst,"SELECT applied_seq FROM mirror_meta WHERE id=1").unwrap();
  peak=peak.max(pending);
  let done=root.join("r877-writer-result.json").exists();
  if pending==0 && done {break}
  if latest>applied && (latest-applied>=replay_min || done || pending>=48){
   let before=Instant::now();
   let n=if std::env::var("ASTRA_R954_FUSED").ok().as_deref()==Some("1"){
    let (replayed,just_acked)=super::r879_tests::r954_private_fused_verified_replay_and_ack(
      &root,&src,&dst,&oracle,"r877_soak",||Ok(()))
      .expect("R954 fused DB2->DB1 verified durable path failed");
    acked+=just_acked;ack_cycles+=1;
    Ok(replayed)
   }else if std::env::var("ASTRA_R951_PREPARED").ok().as_deref()==Some("1"){
    r951_guarded_verified_burst(&root,&src,&dst,&oracle,128)
   }else if std::env::var("ASTRA_R931_FUSED").ok().as_deref()==Some("1"){
    r931_guarded_verified_burst(&root,&src,&dst,&oracle,128)
   }else{r845_verified_burst(&root,&src,&dst,&oracle,128)}
     .expect("R877 independent DB2 chain/replay failed");
   if r911_profile_on(){db2_replay_ms.push(before.elapsed().as_secs_f64()*1000.0)}
   copied+=n;replay_cycles+=1;
  }
  let now_applied=r366_query_num(&dst,"SELECT applied_seq FROM mirror_meta WHERE id=1").unwrap();
  let now_cleaned=r366_query_num(&src,"SELECT cleanup_through FROM mirror_control WHERE id=1").unwrap();
  // Scheduling hints are deliberately unfenced: the selected ACK routine
  // must independently lock and validate DB1/DB2 before deletion.
  let ack_due=if use_prefix{
   now_applied>=now_cleaned+ack_min || (done&&now_applied>now_cleaned)
     || (pending>=48 && now_applied>now_cleaned)
  }else{latest==applied && (pending>=16||done)};
  if ack_due {
   let before=Instant::now();
   // R1052: retry only when the two private SQLite handles report BUSY/LOCKED.
   // Every attempt reruns full fenced ACK and content validation; bounded retries.
   let mut transient_retries=0u8;
   let result=loop{
    let current=if use_prefix && std::env::var("ASTRA_R945_FINGERPRINT").ok().as_deref()==Some("1"){
    super::r879_tests::r945_private_fast_fingerprint_ack(
     &root,&src,&dst,&oracle,"r877_soak")
   }else if use_prefix && std::env::var("ASTRA_R932_FAST_ACK").ok().as_deref()==Some("1"){
    super::r879_tests::r932_guarded_incremental_prefix_ack(
     &root,&src,&dst,&oracle,"r877_soak")
   }else if use_prefix && std::env::var("ASTRA_R924_GUARD").ok().as_deref()==Some("1"){
    super::r879_tests::r925_guarded_paper_prefix_ack(
     &root,&src,&dst,&oracle,"r877_soak",||Ok(()))
   }else if use_prefix && std::env::var("ASTRA_R914_OPTIMISTIC").ok().as_deref()==Some("1"){
    super::r879_tests::r914_private_optimistic_prefix_ack(
     &root,&src,&dst,&oracle,"r877_soak",||Ok(()))
   }else if use_prefix{
    r879_private_paper_prefix_ack(&root,&src,&dst,&oracle,"r877_soak")
   }else{r860_bulk_verified_ack(&root,&src,&dst,&oracle,1024)};
    if matches!(current,Err(EventOutboxR366Error::Sql))
      && transient_retries<3
      && r1052_private_sqlite_busy(&src,&dst){
      transient_retries+=1;
      std::thread::sleep(Duration::from_millis(25*transient_retries as u64));
      continue
    }
    break current
   };
   if r911_profile_on(){ack_ms.push(before.elapsed().as_secs_f64()*1000.0)}
   match result{
    Ok((a,_))=>{acked+=a;ack_cycles+=1},
    Err(EventOutboxR366Error::Conflict)=>{},
    Err(e)=>panic!("R877 DB1/DB2 fenced ACK failed: {e:?}; SQLite source_ext={} destination_ext={}",
     unsafe{sqlite3_extended_errcode(src.0)},unsafe{sqlite3_extended_errcode(dst.0)}),
   }
  }else{
   idle+=1;
   std::thread::sleep(Duration::from_millis(2));
  }
 }
 let result=serde_json::json!({"copied_envelopes":copied,
  "acked_envelopes":acked,"peak_pending_envelopes":peak,
  "replay_cycles":replay_cycles,"ack_cycles":ack_cycles,"idle_cycles":idle,
  "elapsed_seconds":started.elapsed().as_secs_f64(),
  "db2_replay":r911_quantiles_ms(&db2_replay_ms),
  "db1_ack":r911_quantiles_ms(&ack_ms)});
 std::fs::write(root.join("r877-reader-result.json"),serde_json::to_vec(&result).unwrap()).unwrap();
 eprintln!("R877 RECEIVER pid={} {}",std::process::id(),result);
}
fn r877_run_private(events:u64,rate:f64,require_full:bool,use_prefix:bool,replay_min:u64,ack_min:u64,wal_pages:u64) {
 assert!(r877_child_root().is_none());
 let root=Root::new();
 if std::env::var("ASTRA_R924_GUARD").ok().as_deref()==Some("1"){
  let src=r671_open(&root.0,"r671-private-db1.sqlite",false).unwrap();
  let dst=r671_open(&root.0,"r671-private-db2.sqlite",false).unwrap();
  super::r879_tests::r924_install_private_paper_only_guards(&src,&dst,&r586_oracle()).unwrap();
  if std::env::var("ASTRA_R932_FAST_ACK").ok().as_deref()==Some("1"){
   super::r879_tests::r932_certify_private_empty_genesis(
    &root.0,&src,&dst,&r586_oracle(),"r877_soak").unwrap();
  }
 }
 let exe=std::env::current_exe().unwrap();
 let vars=|cmd:&mut Command| { cmd.env("ASTRA_R877_ROOT",&root.0)
   .env("ASTRA_R877_EVENTS",events.to_string())
   .env("ASTRA_R877_RATE",rate.to_string())
   .env("ASTRA_R877_PREFIX",if use_prefix{"1"}else{"0"})
   .env("ASTRA_R877_REPLAY_MIN",replay_min.to_string())
   .env("ASTRA_R877_ACK_MIN",ack_min.to_string())
   .env("ASTRA_R900_WAL_PAGES",wal_pages.to_string());};
 let mut reader=Command::new(&exe);
 reader.args(["--exact","native_isolated_sqlite_mirror_r147_review::r876_tests::r877_verified_receiver_child","--nocapture"]);
 vars(&mut reader);
 let mut reader=reader.spawn().unwrap();
 let mut writer=Command::new(&exe);
 writer.args(["--exact","native_isolated_sqlite_mirror_r147_review::r876_tests::r877_paced_writer_child","--nocapture"]);
 vars(&mut writer);
 let output=writer.output().unwrap();
 let rxstatus=reader.wait().unwrap();
 eprintln!("R877 writer stdout {} stderr {}",String::from_utf8_lossy(&output.stdout),String::from_utf8_lossy(&output.stderr));
 assert!(output.status.success(),"R877 source child failed");
 assert!(rxstatus.success(),"R877 receiver child failed");
 let wr:serde_json::Value=serde_json::from_slice(&std::fs::read(root.0.join("r877-writer-result.json")).unwrap()).unwrap();
 let rr:serde_json::Value=serde_json::from_slice(&std::fs::read(root.0.join("r877-reader-result.json")).unwrap()).unwrap();
 let committed=wr["committed"].as_u64().unwrap();
 let pressure=wr["pressure"].as_bool().unwrap();
 if r911_profile_on(){
  eprintln!("R911 PROFILE (test-only) DB1 writer {}",wr["db1_commit"]);
  eprintln!("R911 PROFILE (test-only) DB2 replay {}",rr["db2_replay"]);
  eprintln!("R911 PROFILE (test-only) DB1 ACK {}",rr["db1_ack"]);
 }
 // Verify every accepted original row and recovered Owner even if the
 // strict full-acceptance capacity gate eventually fails red.
 if pressure{assert!(committed<events)}
 assert_eq!(committed%32,0);
 let src=r671_open(&root.0,"r671-private-db1.sqlite",false).unwrap();
 let dst=r671_open(&root.0,"r671-private-db2.sqlite",false).unwrap();
 assert_eq!(r366_query_num(&src,"SELECT COUNT(*) FROM events"),Ok(committed));
 assert_eq!(r366_query_num(&dst,"SELECT COUNT(*) FROM events"),Ok(committed));
 assert_eq!(r366_query_num(&src,"SELECT COUNT(*) FROM mirror_outbox"),Ok(0));
 assert_eq!(r366_query_num(&src,"SELECT latest_seq FROM mirror_control"),Ok(committed/32));
 assert_eq!(r366_query_num(&dst,"SELECT applied_seq FROM mirror_meta"),Ok(committed/32));
 assert_eq!(rr["copied_envelopes"].as_u64(),Some(committed/32));
 assert_eq!(rr["acked_envelopes"].as_u64(),Some(committed/32));
 assert_eq!(r800_streaming_original_parity(&src,&dst,&r586_oracle()),Ok(committed));
 let (owner,restored)=r834_recover_private_owner(&root.0,"r877_soak",0,1,800).unwrap();
 assert_eq!(restored,committed);
 if committed>0{assert!(owner.snapshot_venue("BTC",Venue::OkxSpot).is_some())}
 if require_full{assert_eq!(committed,events,"R965 complete intake required");assert!(!pressure)}
 if std::env::var("ASTRA_R967_WINDOW").ok().as_deref()==Some("YES"){
  let windows=wr["observed_two_second_events"].as_array().unwrap();
  let counts=windows.iter().map(|x|x.as_u64().unwrap()).collect::<Vec<_>>();
  assert_eq!(counts.iter().sum::<u64>(),committed);
  assert!(counts.len()>=20,"R967 elapsed time windows truncated");
  let peaks=(0..10).map(|i|counts[2*i]).collect::<Vec<_>>();
  eprintln!("R967 physically committed events in each 2-second window {:?}, high windows {:?}",counts,peaks);
  assert!(peaks.iter().all(|x|*x>=3200),"R967 peak less than 1600/s in a 2-second window");
  for name in ["r671-private-db1.sqlite","r671-private-db2.sqlite"]{
   let base=std::fs::metadata(root.0.join(name)).unwrap().len();
   let wal=std::fs::metadata(root.0.join(format!("{name}-wal")))
    .map(|v|v.len()).unwrap_or(0);
   eprintln!("R967 disk measured {name} base_bytes={base} wal_bytes={wal} total_bytes={}",base+wal);
  }
 }
 let writer_s=wr["elapsed_seconds"].as_f64().unwrap();
 let reader_s=rr["elapsed_seconds"].as_f64().unwrap();
 let peak=rr["peak_pending_envelopes"].as_u64().unwrap();
 assert!(peak<=64,"bounded original outbox violated");
 // R936 is a TRUE capacity gate: merely accepting all packets after a
 // slow writer is insufficient. End-to-end receiver incl drain must keep
 // up with at least 95% of the requested event/s rate in this short probe.
 if std::env::var("ASTRA_R936_RATE").is_ok()
  ||std::env::var("ASTRA_R961_RATE").is_ok()
  ||std::env::var("ASTRA_R965_EXTENDED").ok().as_deref()==Some("YES"){
  let writer_rate=committed as f64/writer_s;
  let receiver_rate=committed as f64/reader_s;
  assert!(writer_rate>=rate*0.95,
   "R936 writer speed FAIL: {writer_rate:.1}/s vs requested {rate:.1}/s");
  assert!(receiver_rate>=rate*0.95,
   "R936 DB2+ACK end-to-end FAIL: {receiver_rate:.1}/s vs requested {rate:.1}/s");
 }
 eprintln!("R877 PRIVATE TWO-PID result committed={committed}/{events} rate_set={rate} pressure={pressure} writer_s={writer_s:.3} reader_s={reader_s:.3} reader_effective_events_s={:.1} peak={peak}; no lost DB1/DB2 ACK; NOT a sustained live capacity certificate",committed as f64/reader_s);
}
#[test] fn r877_4096_paced_450_capacity_or_fail_closed(){r877_run_private(4096,450.0,false,false,1,16,10);}
#[test] fn r878_8192_paced_900_capacity_or_fail_closed(){r877_run_private(8192,900.0,false,false,1,16,10);}
#[test] fn r885_2048_paced_150_must_be_durable(){r877_run_private(2048,150.0,true,false,1,16,10);}
#[test] fn r886_4096_paced_450_prefix_must_be_durable(){r877_run_private(4096,450.0,true,true,1,16,10);}
#[test] fn r887_8192_paced_900_prefix_gate(){r877_run_private(8192,900.0,false,true,1,16,10);}
#[test] fn r888_8192_paced_1200_prefix_gate(){r877_run_private(8192,1200.0,false,true,1,16,10);}
#[test] fn r889_replay8_ack16_paced_1200(){r877_run_private(8192,1200.0,false,true,8,16,10);}
#[test] fn r890_replay16_ack32_paced_1200(){r877_run_private(8192,1200.0,false,true,16,32,10);}
#[test] fn r891_replay8_ack32_paced_1200(){r877_run_private(8192,1200.0,false,true,8,32,10);}
#[test] fn r892_replay16_ack48_paced_1200(){r877_run_private(8192,1200.0,false,true,16,48,10);}
#[test] fn r893_replay16_ack32_paced_900(){r877_run_private(8192,900.0,false,true,16,32,10);}
#[test] fn r894_replay32_ack32_paced_1200(){r877_run_private(8192,1200.0,false,true,32,32,10);}
#[test] fn r900_wal100_replay1_ack16_paced1200(){r877_run_private(8192,1200.0,false,true,1,16,100);}
#[test] fn r901_wal1000_replay1_ack16_paced1200(){r877_run_private(8192,1200.0,false,true,1,16,1000);}
#[test] fn r902_wal1000_replay8_ack16_paced1200(){r877_run_private(8192,1200.0,false,true,8,16,1000);}
#[test] fn r903_wal1000_replay16_ack32_paced1200(){r877_run_private(8192,1200.0,false,true,16,32,1000);}
#[test] fn r904_wal1000_replay32_ack32_paced1200(){r877_run_private(8192,1200.0,false,true,32,32,1000);}
#[test] fn r905_wal1000_replay1_ack16_paced900(){r877_run_private(8192,900.0,false,true,1,16,1000);}

#[test]fn r1052_fenced_ack_busy_is_transient_but_never_skips_parity(){
 let root=Root::new();
 let mut h=R819OriginalPaperHistory::new(&root.0,"r877_soak").unwrap();
 let mut owner=Owner::new(0,1,800).unwrap();
 let batch=(1..=32).map(event).collect::<Vec<_>>();
 assert_eq!(r824_commit_prepared_batch(&mut h,&mut owner,&batch),Ok((1,32)));
 let oracle=r586_oracle();
 assert_eq!(r845_verified_burst(&root.0,&h.source,&h.sink,&oracle,1),Ok(1));
 let competing=r671_open(&root.0,"r671-private-db1.sqlite",false).unwrap();
 let held=Tx::begin(&competing).unwrap();
 assert_eq!(r879_private_paper_prefix_ack(
   &root.0,&h.source,&h.sink,&oracle,"r877_soak"),
   Err(EventOutboxR366Error::Sql));
 assert!(r1052_private_sqlite_busy(&h.source,&h.sink),
   "only actual SQLite BUSY/LOCKED may be retried");
 assert_eq!(r366_query_num(&h.source,"SELECT cleanup_through FROM mirror_control"),Ok(0));
 assert_eq!(r366_query_num(&h.source,"SELECT COUNT(*) FROM mirror_outbox"),Ok(1));
 drop(held);
 assert_eq!(r879_private_paper_prefix_ack(
   &root.0,&h.source,&h.sink,&oracle,"r877_soak"),Ok((1,0)));
 assert_eq!(r366_query_num(&h.source,"SELECT cleanup_through FROM mirror_control"),Ok(1));
 assert_eq!(r366_query_num(&h.sink,"SELECT applied_seq FROM mirror_meta"),Ok(1));
 assert_eq!(r800_streaming_original_parity(&h.source,&h.sink,&oracle),Ok(32));
}
