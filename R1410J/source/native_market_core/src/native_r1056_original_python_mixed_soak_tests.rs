// R1056: opt-in ORIGINAL Python 17-table writer, separate OS PID from
// this Rust DB2/ACK receiver. 12s smoke is NOT a 30min certification.
#[test]
#[ignore = "R1056 explicit 2-PID private original mixed-17-table duration gate"]
fn r1056_actual_python_mixed_17_table_two_pid_soak() {
 use std::time::{Duration, Instant};
 use std::process::Stdio;
 assert_eq!(std::env::var("ASTRA_R1056_PRIVATE_MIXED").as_deref(), Ok("YES"));
 let seconds: u64 = std::env::var("ASTRA_R1056_SECONDS").unwrap().parse().unwrap();
 assert!(seconds == 12 || seconds == 1800, "R1056 allowlist: 12 smoke / 1800 full");
 let (root, src, dst, v) = pair();
 let script = Path::new(env!("CARGO_MANIFEST_DIR"))
   .join("offline_original_python_mixed_soak_r1056.py");
 let writer_log = fs::File::create(root.0.join("r1056-writer.log")).unwrap();
 let mut child = Command::new("/usr/bin/python3")
   .arg(&script).arg(&root.0).arg(seconds.to_string())
   .env("ASTRA_R1056_PRIVATE_MIXED", "YES")
   .stdout(Stdio::from(writer_log.try_clone().unwrap()))
   .stderr(Stdio::from(writer_log))
   .spawn().expect("R1056 private Python source failed to spawn");
 assert_ne!(child.id(), std::process::id(), "writer and receiver must have different OS PIDs");
 let begin = Instant::now();
 let mut replayed = 0u64;
 let mut acked = 0u64;
 let mut max_outbox = 0u64;
 let mut replay_ms = Vec::new();
 let mut ack_ms = Vec::new();
 let mut busy_retry = 0u64;
 let mut conflict_retry = 0u64;
 let mut writer_status = None;
 loop {
   if begin.elapsed() > Duration::from_secs(seconds + 120) {
     let _ = child.kill(); let _ = child.wait();
     panic!("R1056 exceeded exact writer duration + bounded drain time");
   }
   let started = Instant::now();
   match r805_replay_window(&root.0, &src, &dst, &v, 32) {
     Ok(n) => { replayed += n; replay_ms.push(started.elapsed().as_secs_f64()*1000.0); }
     Err(EventOutboxR366Error::Sql)
       if [ &src, &dst ].iter().any(|db| {
          let code = unsafe { sqlite3_extended_errcode(db.0) } & 0xff;
          code == 5 || code == 6
       }) => { busy_retry += 1; }
     Err(e) => { let _ = child.kill(); let _ = child.wait();
       panic!("R1056 original Rust replay error: {e:?}"); }
   }
   let latest = r366_query_num(&src, "SELECT latest_seq FROM mirror_control WHERE id=1").unwrap();
   let applied = r366_query_num(&dst, "SELECT applied_seq FROM mirror_meta WHERE id=1").unwrap();
   let retained = r366_query_num(&src, "SELECT retained_records FROM mirror_control WHERE id=1").unwrap();
   max_outbox = max_outbox.max(retained);
   if latest == applied && retained > 0 {
     let started = Instant::now();
     match r756_cleanup(&root.0, &src, &dst, &v) {
       Ok((n, _)) => { acked += n; ack_ms.push(started.elapsed().as_secs_f64()*1000.0); }
       Err(EventOutboxR366Error::Conflict) => conflict_retry += 1,
       Err(EventOutboxR366Error::Sql)
         if [ &src, &dst ].iter().any(|db| {
            let code = unsafe { sqlite3_extended_errcode(db.0) } & 0xff;
            code == 5 || code == 6
         }) => busy_retry += 1,
       Err(e) => { let _ = child.kill(); let _ = child.wait();
         panic!("R1056 ACK/parity refused: {e:?}"); }
     }
   }
   if writer_status.is_none() { writer_status = child.try_wait().unwrap(); }
   if writer_status.is_some() && latest == applied && retained == 0 { break; }
   std::thread::sleep(Duration::from_millis(20));
 }
 let status = writer_status.unwrap();
 assert!(status.success(), "R1056 Python producer failed: {}",
  fs::read_to_string(root.0.join("r1056-writer.log")).unwrap_or_default());
 let recorded: Value = serde_json::from_slice(
   &fs::read(root.0.join("r1056-producer-result.json")).unwrap()).unwrap();
 assert_eq!(recorded["source_latest_seq"].as_u64(), Some(replayed));
 assert_eq!(recorded["transactions"].as_u64(), Some(acked));
 assert_eq!(recorded["foreign_key_violations"].as_u64(), Some(0));
 for mode in ["insert", "update", "upsert", "delete"] {
   assert!(recorded["operations"][mode].as_u64().unwrap_or(0) > 0,
           "R1056 no original Python {mode} operations");
 }
 assert!(recorded["elapsed_s"].as_f64().unwrap() >= seconds as f64);
 assert_eq!(r366_query_num(&src,"SELECT COUNT(*) FROM mirror_outbox"), Ok(0));
 let final_rows = if recorded["phases"].as_u64().unwrap() % 4 == 0 { 2 } else { 17 };
 assert_eq!(r800_streaming_original_parity(&src, &dst, &v), Ok(final_rows));
 assert_eq!(r366_query_num(&dst,"SELECT applied_seq FROM mirror_meta"),Ok(replayed));
 let wal = |name: &str| -> u64 {
   let path = root.0.join(name);
   fs::metadata(path).map(|x|x.len()).unwrap_or(0)
 };
 let q = |times: &Vec<f64>, pct: usize| -> f64 {
   if times.is_empty(){return 0.0}
   let mut ordered=times.clone();
   ordered.sort_by(|a,b| a.total_cmp(b));
   ordered[(ordered.len()*pct).div_ceil(100).saturating_sub(1)]
 };
 eprintln!("R1056 PRIVATE mixed two-PID duration={seconds}s writer_pid={} receiver_pid={} writer={} replayed={replayed} acked={acked} max_outbox={max_outbox} busy_retry={busy_retry} conflict_retry={conflict_retry} replay_p50_ms={:.3} p95_ms={:.3} p99_ms={:.3} ack_p50_ms={:.3} p95_ms={:.3} p99_ms={:.3} db1_wal_bytes={} db2_wal_bytes={} NOTE: 12s is ONLY smoke; no installed PAPER handoff",
 child.id(),std::process::id(),recorded,
 q(&replay_ms,50),q(&replay_ms,95),q(&replay_ms,99),
 q(&ack_ms,50),q(&ack_ms,95),q(&ack_ms,99),
 wal("r671-private-db1.sqlite-wal"),wal("r671-private-db2.sqlite-wal"));
}

#[test]
fn r1097_disposable_receiver_child_after_original_mixed_db2_commit(){
 use std::os::unix::fs::MetadataExt;
 if std::env::var("SPOTARB_R1097_PRIVATE_CHILD").ok().as_deref()
    !=Some("MIXED_ORIGINAL_DISPOSABLE"){return}
 let root=PathBuf::from(std::env::var("SPOTARB_R1097_PRIVATE_ROOT").unwrap());
 assert!(root.file_name().and_then(|s|s.to_str())
     .is_some_and(|s|s.starts_with("astra_r671_file_")));
 assert_eq!(root.parent(),Some(std::env::temp_dir().as_path()));
 assert_eq!(fs::metadata(&root).unwrap().mode()&0o777,0o700);
 let src=r671_open(&root,"r671-private-db1.sqlite",false).unwrap();
 let dst=r671_open(&root,"r671-private-db2.sqlite",false).unwrap();
 let v=r586_oracle();
 let applied=r805_replay_window(&root,&src,&dst,&v,32).unwrap();
 assert!(applied>0,"child must commit original Python mixed ops before SIGKILL");
 assert_eq!(r366_query_num(&src,
   "SELECT cleanup_through FROM mirror_control WHERE id=1"),Ok(0));
 assert!(r366_query_num(&dst,
   "SELECT applied_seq FROM mirror_meta WHERE id=1").unwrap()>0);
 unsafe {libc::raise(libc::SIGKILL);}
 panic!("R1097 disposable SIGKILL returned unexpectedly")
}

#[test]
#[ignore="R1097 opt-in 12-second ORIGINAL mixed Python writer / killed Rust receiver"]
fn r1097_python_mixed_original_17table_sigkill_then_resume(){
 use std::time::{Duration,Instant};
 use std::os::unix::process::ExitStatusExt;
 use std::process::Stdio;
 assert_eq!(std::env::var("ASTRA_R1097_PRIVATE_MIXED").as_deref(),Ok("YES"));
 assert_eq!(std::env::var("ASTRA_R1056_PRIVATE_MIXED").as_deref(),Ok("YES"));
 let (root,src,dst,v)=pair();
 let script=Path::new(env!("CARGO_MANIFEST_DIR"))
   .join("offline_original_python_mixed_soak_r1056.py");
 let log=fs::File::create(root.0.join("r1097-python-mixed.log")).unwrap();
 let mut writer=Command::new("/usr/bin/python3").arg(&script)
   .arg(&root.0).arg("12")
   .env("ASTRA_R1056_PRIVATE_MIXED","YES")
   .env("PYTHONDONTWRITEBYTECODE","1")
   .stdout(Stdio::from(log.try_clone().unwrap()))
   .stderr(Stdio::from(log)).spawn().unwrap();
 let started=Instant::now();
 loop {
  if r366_query_num(&src,
    "SELECT latest_seq FROM mirror_control WHERE id=1").unwrap()>0{break}
  if started.elapsed()>Duration::from_secs(10){
   let _=writer.kill();let _=writer.wait();
   panic!("R1097 original Python source never committed a mixed operation")
  }
  std::thread::sleep(Duration::from_millis(10));
 }
 let child=Command::new(std::env::current_exe().unwrap())
   .arg("r1097_disposable_receiver_child_after_original_mixed_db2_commit")
   .arg("--nocapture")
   .env("SPOTARB_R1097_PRIVATE_CHILD","MIXED_ORIGINAL_DISPOSABLE")
   .env("SPOTARB_R1097_PRIVATE_ROOT",&root.0)
   .output().unwrap();
 assert_eq!(child.status.signal(),Some(libc::SIGKILL),
   "child failed to SIGKILL, stderr={}",
   String::from_utf8_lossy(&child.stderr));
 let applied=r366_query_num(&dst,
   "SELECT applied_seq FROM mirror_meta WHERE id=1").unwrap();
 let cleaned=r366_query_num(&src,
   "SELECT cleanup_through FROM mirror_control WHERE id=1").unwrap();
 assert!(applied>0);
 assert_eq!(cleaned,0,"DB1 ACK must not precede the crashed DB2 commit");
 // Freshly reopen both SQLite DB handles: the killed process held the old ones.
 drop(src);drop(dst);
 let src=r671_open(&root.0,"r671-private-db1.sqlite",false).unwrap();
 let dst=r671_open(&root.0,"r671-private-db2.sqlite",false).unwrap();
 let mut replayed=0u64;
 let mut acked=0u64;
 let mut busy_retries=0u64;
 let mut conflict_retries=0u64;
 let mut status=None;
 loop{
  if started.elapsed()>Duration::from_secs(55){
   let _=writer.kill();let _=writer.wait();
   panic!("R1097 mixed-source recovery exceeded guarded 55s")
  }
  match r805_replay_window(&root.0,&src,&dst,&v,32){
   Ok(n)=>replayed+=n,
   Err(EventOutboxR366Error::Sql)=>busy_retries+=1,
   Err(e)=>{
    let _=writer.kill();let _=writer.wait();
    panic!("R1097 Rust mixed replay refused: {e:?}")
   }
  }
  let latest=r366_query_num(&src,
   "SELECT latest_seq FROM mirror_control WHERE id=1").unwrap();
  let applied=r366_query_num(&dst,
   "SELECT applied_seq FROM mirror_meta WHERE id=1").unwrap();
  let retained=r366_query_num(&src,
   "SELECT retained_records FROM mirror_control WHERE id=1").unwrap();
  if latest==applied && retained>0{
   match r756_cleanup(&root.0,&src,&dst,&v){
    Ok((n,_))=>acked+=n,
    Err(EventOutboxR366Error::Conflict)=>conflict_retries+=1,
    Err(EventOutboxR366Error::Sql)=>busy_retries+=1,
    Err(e)=>{
     let _=writer.kill();let _=writer.wait();
     panic!("R1097 physical 17-table ACK refused: {e:?}")
    }
   }
  }
  if status.is_none(){status=writer.try_wait().unwrap();}
  let cleaned=r366_query_num(&src,
   "SELECT cleanup_through FROM mirror_control WHERE id=1").unwrap();
  if status.is_some()&&latest==applied&&latest==cleaned&&
     r366_query_num(&src,"SELECT COUNT(*) FROM mirror_outbox").unwrap()==0{
    break
  }
  std::thread::sleep(Duration::from_millis(20));
 }
 assert!(status.unwrap().success(),
   "R1097 genuine Python producer failed: {}",
   fs::read_to_string(root.0.join("r1097-python-mixed.log")).unwrap());
 let original:Value=serde_json::from_slice(
   &fs::read(root.0.join("r1056-producer-result.json")).unwrap()).unwrap();
 let seq=original["transactions"].as_u64().unwrap();
 assert!(seq>0);
 assert_eq!(r366_query_num(&src,"SELECT latest_seq FROM mirror_control"),Ok(seq));
 assert_eq!(r366_query_num(&dst,"SELECT applied_seq FROM mirror_meta"),Ok(seq));
 assert_eq!(r366_query_num(&src,"SELECT cleanup_through FROM mirror_control"),Ok(seq));
 assert_eq!(r366_query_num(&src,"SELECT COUNT(*) FROM mirror_outbox"),Ok(0));
 assert_eq!(original["foreign_key_violations"],0);
 for mode in ["insert","update","upsert","delete"]{
  assert!(original["operations"][mode].as_u64().unwrap_or(0)>0,
   "missing original Python mixed {mode}");
 }
 assert!(original["elapsed_s"].as_f64().unwrap()>=12.0);
 let expected=if original["phases"].as_u64().unwrap()%4==0{2}else{17};
 assert_eq!(r800_streaming_original_parity(&src,&dst,&v),Ok(expected));
 eprintln!("R1097 PASS actual ORIGINAL Python mixed writer + SIGKILL receiver between DB2 commit and DB1 ACK; source_seq={seq} precrash_applied={applied} precrash_ack={cleaned} replay_after_crash={replayed} ack_after_crash={acked} SQL_busy={busy_retries} ack_conflicts={conflict_retries} 17-table original physical parity={expected} no exchange orders");
}
