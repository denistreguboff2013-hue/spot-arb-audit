// Astra P0-01 R876: two separate OS-processes (paced writer + independently
// scheduled verified DB2 drain), private R671 SQLite ONLY. Not a daemon.
#[cfg(test)]
mod r876_tests {
 use super::*;
 use crate::{Book,Owner,RawFrame,Update,Venue};
 use std::os::unix::fs::DirBuilderExt;
 use std::process::Command;
 use std::time::{Duration,Instant,SystemTime,UNIX_EPOCH};
 const EVENTS:u64=1024;
 const EPS:f64=900.0;
 struct Root(PathBuf);
 impl Root {
  fn new()->Self {
   let mut p=std::env::temp_dir();
   let stamp=SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
   p.push(format!("astra_r671_file_{}_{}_r876",std::process::id(),stamp));
   fs::DirBuilder::new().mode(0o700).create(&p).unwrap();
   drop(r756_initialize(&p).unwrap());
   Self(p)
  }
 }
 impl Drop for Root {fn drop(&mut self){let _=fs::remove_dir_all(&self.0);}}
 fn event(i:u64)->(RawFrame,Update){
  let venue=Venue::OkxSpot;
  let ns=1_000_000_000+i*10_000_000;
  let ms=1_800_000_000_000_i64+i as i64*10;
  let f=RawFrame{source:None,venue,epoch:1,received_ns:ns,received_wall_ms:ms,
   raw:format!("{{\"r876_public_depth_sequence\":{i},\"levels\":2}}")};
  let book=Book{bids:vec![(101.0,1.0)],asks:vec![(102.0,1.0)],
   received_ns:ns,received_wall_ms:ms,exchange_ts_ms:Some(ms),
   changed_ns:ns,epoch:1,sequence:Some(i as i64)};
  (f,Update{base:"BTC".into(),venue,book})
 }
 fn worker_root()->Option<PathBuf>{
  let p=PathBuf::from(std::env::var_os("ASTRA_R876_ROOT")?);
  // Reuse original defensive R671 open guard, never use production paths.
  r671_guard(&p,"r671-private-db1.sqlite").ok()?;
  Some(p)
 }
 #[test] fn r876_paced_writer_child(){
  let Some(root)=worker_root() else {return};
  let mut h=R819OriginalPaperHistory::new(&root,"r876_private").unwrap();
  let mut owner=Owner::new(0,1,800).unwrap();
  let start=Instant::now();
  let mut peak=0;
  for first in (1..=EVENTS).step_by(32){
   let schedule=Duration::from_secs_f64((first-1) as f64/EPS);
   if let Some(delay)=schedule.checked_sub(start.elapsed()){std::thread::sleep(delay)}
   let batch=(first..first+32).map(event).collect::<Vec<_>>();
   let result=r824_commit_prepared_batch(&mut h,&mut owner,&batch);
   assert_eq!(result,Ok(((first+31)/32,32)),
    "R876 cannot drop/duplicate/retry unknown DB1 ACK; stop on pressure");
   peak=peak.max(r366_query_num(&h.source,
      "SELECT retained_records FROM mirror_control WHERE id=1").unwrap());
  }
  let seconds=start.elapsed().as_secs_f64();
  std::fs::write(root.join("r876-writer-done"),b"1024").unwrap();
  eprintln!("R876 WRITER pid={} events={} elapsed_s={seconds:.3} rate={:.1} peak_pending_envelopes={peak}",std::process::id(),EVENTS,EVENTS as f64/seconds);
 }
 #[test] fn r876_independent_receiver_child(){
  let Some(root)=worker_root() else {return};
  let src=r671_open(&root,"r671-private-db1.sqlite",false).unwrap();
  let dst=r671_open(&root,"r671-private-db2.sqlite",false).unwrap();
  let oracle=r586_oracle();
  let started=Instant::now();
  let mut copied=0_u64;let mut acked=0_u64;let mut peak=0_u64;
  loop {
   assert!(started.elapsed()<Duration::from_secs(25),"R876 receiver stalled");
   let count=r366_query_num(&src,"SELECT retained_records FROM mirror_control WHERE id=1").unwrap();
   peak=peak.max(count);
   if count==0 {
    if root.join("r876-writer-done").exists(){break}
    std::thread::sleep(Duration::from_millis(3));
    continue
   }
   let n=r845_verified_burst(&root,&src,&dst,&oracle,128)
      .expect("R876 DB2 verified replay failed under concurrent write");
   copied+=n;
   // An unfenced r805_read_head may read source counters then a later
   // outbox revision during a concurrent writer commit. Do NOT trust it.
   // R860 locks both databases and checks one atomic ACK frontier itself.
   match r860_bulk_verified_ack(&root,&src,&dst,&oracle,1024){
    Ok((a,_))=>acked+=a,
    Err(EventOutboxR366Error::Conflict)=>{},
    Err(e)=>panic!("R876 fenced ACK failed: {e:?}"),
   }
   if n==0 {std::thread::sleep(Duration::from_millis(3))}
  }
  eprintln!("R876 RECEIVER pid={} copied_envelopes={copied} acked_envelopes={acked} elapsed_s={:.3} peak_pending_envelopes={peak}",std::process::id(),started.elapsed().as_secs_f64());
 }
 #[test] fn r876_paced_independent_processes_no_loss(){
  if worker_root().is_some(){return}
  let root=Root::new();
  let exe=std::env::current_exe().unwrap();
  let mut reader=Command::new(&exe)
    .args(["--exact","native_isolated_sqlite_mirror_r147_review::r876_tests::r876_independent_receiver_child","--nocapture"])
    .env("ASTRA_R876_ROOT",&root.0)
    .spawn().unwrap();
  let writer=Command::new(&exe)
    .args(["--exact","native_isolated_sqlite_mirror_r147_review::r876_tests::r876_paced_writer_child","--nocapture"])
    .env("ASTRA_R876_ROOT",&root.0).output().unwrap();
  let receiver=reader.wait().unwrap();
  eprintln!("R876 WRITER stdout={} stderr={}",String::from_utf8_lossy(&writer.stdout),String::from_utf8_lossy(&writer.stderr));
  assert!(writer.status.success(),"R876 independent writer failed");
  assert!(receiver.success(),"R876 independent receiver failed");
  let src=r671_open(&root.0,"r671-private-db1.sqlite",false).unwrap();
  let dst=r671_open(&root.0,"r671-private-db2.sqlite",false).unwrap();
  let oracle=r586_oracle();
  assert_eq!(r366_query_num(&src,"SELECT COUNT(*) FROM events"),Ok(EVENTS));
  assert_eq!(r366_query_num(&dst,"SELECT COUNT(*) FROM events"),Ok(EVENTS));
  assert_eq!(r366_query_num(&src,"SELECT COUNT(*) FROM mirror_outbox"),Ok(0));
  assert_eq!(r366_query_num(&src,"SELECT latest_seq FROM mirror_control"),Ok(32));
  assert_eq!(r366_query_num(&dst,"SELECT applied_seq FROM mirror_meta"),Ok(32));
  assert_eq!(r800_streaming_original_parity(&src,&dst,&oracle),Ok(EVENTS));
  let (recovered,count)=r834_recover_private_owner(&root.0,"r876_private",0,1,800).unwrap();
  assert_eq!(count,EVENTS);
  assert!(recovered.snapshot_venue("BTC",Venue::OkxSpot).is_some());
  eprintln!("R876 VERIFIED TWO-PID synthetic events={EVENTS} DB1=DB2, ACK=32, no loss; not production-certified");
 }
 include!("native_r877_soak_review.rs");
 include!("native_r911_profile_routine_review.rs");
}
