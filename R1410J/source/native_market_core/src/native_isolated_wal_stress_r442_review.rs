// R442-R445: private /tmp WAL-reader, writer-contention, append latency probes.
// Testing ONLY: no production DB, no live PAPER, no DB2/ACK, no orders.
#[cfg(test)]
mod r442_tests {
 use super::*;
 use super::r366_tests::{fixture_root,record};
 use std::time::Instant;

 fn checkpoint(db:&Db,kind:&str)->Result<(u64,u64,u64),EventOutboxR366Error>{
  let sql=match kind{
   "passive"=>"PRAGMA wal_checkpoint(PASSIVE)",
   "truncate"=>"PRAGMA wal_checkpoint(TRUNCATE)",
   _=>return Err(EventOutboxR366Error::Schema)
  };
  let mut q=Stmt::new(db,sql).map_err(r366_error)?;
  if q.step().map_err(r366_error)?!=SQLITE_ROW{
   return Err(EventOutboxR366Error::Corrupt)
  }
  let a=q.num(0).map_err(r366_error)?;
  let b=q.num(1).map_err(r366_error)?;
  let c=q.num(2).map_err(r366_error)?;
  if q.step().map_err(r366_error)?!=SQLITE_DONE{
   return Err(EventOutboxR366Error::Corrupt)
  }
  Ok((a,b,c))
 }
 #[test] fn r442_pinned_reader_blocks_wal_checkpoint_but_not_paper_append(){
  let root=fixture_root();
  let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
  w.append(&record("r442_before","decisions"),1_792_660_000_100,false).unwrap();
  let reader=r366_open(&w.path).unwrap();
  exec(&reader,"BEGIN;").unwrap();
  assert_eq!(r366_query_num(&reader,"SELECT COUNT(*) FROM events").unwrap(),1);
  // Snapshot pinned at first durable row; second writer has its own connection.
  w.append(&record("r442_after","decisions"),1_792_660_000_101,false).unwrap();
  let observer=r366_open(&w.path).unwrap();
  let (busy,log,done)=checkpoint(&observer,"passive").unwrap();
  assert_eq!(busy,0);
  assert!(log>done,"the live reader must pin at least one WAL frame");
  assert!(r436_private_sample(&w.path).unwrap().wal>0);
  // Read transaction must see its original snapshot until released.
  assert_eq!(r366_query_num(&reader,"SELECT COUNT(*) FROM events").unwrap(),1);
  exec(&reader,"ROLLBACK;").unwrap();
  let (busy,_,_)=checkpoint(&observer,"truncate").unwrap();
  assert_eq!(busy,0);
  drop(observer);drop(reader);
  assert_eq!((w.audit().unwrap().events,w.audit().unwrap().outbox),(2,2));
  drop(w);
  assert_eq!(IsolatedEventOutboxR366::reopen(&root).unwrap().audit().unwrap().events,2);
  fs::remove_dir_all(root).unwrap();
 }
 #[test] fn r443_competing_writer_rolls_back_without_outbox_gap(){
  let root=fixture_root();
  let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
  let blocker=r366_open(&w.path).unwrap();
  let hold=Tx::begin(&blocker).unwrap();
  let began=Instant::now();
  let err=w.append(&record("r443_blocked","decisions"),
   1_792_660_000_200,false).unwrap_err();
  assert_eq!(err,EventOutboxR366Error::Sql);
  assert!(began.elapsed().as_secs()<5,"bounded private SQLite busy_timeout");
  drop(hold);drop(blocker);
  assert_eq!((w.audit().unwrap().events,w.audit().unwrap().outbox),(0,0));
  w.append(&record("r443_retry","decisions"),
   1_792_660_000_201,false).unwrap();
  let after=w.audit().unwrap();
  assert_eq!((after.events,after.outbox,after.last_seq),(1,1,1));
  drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test] fn r444_append_latency_and_history_growth_local_probe(){
  let root=fixture_root();
  let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
  let mut micros=Vec::<u128>::new();
  for i in 0..40_u64{
   let start=Instant::now();
   w.append(&record(&format!("r444_{i:03}"),"decisions"),
    1_792_660_100_000+i,false).unwrap();
   micros.push(start.elapsed().as_micros());
  }
  let first=micros[..10].iter().sum::<u128>()/10;
  let last=micros[30..].iter().sum::<u128>()/10;
  micros.sort_unstable();
  let median=micros[20];
  let p95=micros[37];
  eprintln!("R444_ISOLATED_ONLY sample=40 first10_avg_us={first} last10_avg_us={last} median_us={median} p95_us={p95}");
  let a=w.audit().unwrap();
  assert_eq!((a.events,a.category_rows,a.outbox,a.last_seq),(40,40,40,40));
  drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test] fn r445_pinned_wal_pressure_admission_stays_fail_closed(){
  let root=fixture_root();
  let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
  w.append(&record("r445_anchor","decisions"),1_792_660_200_000,false).unwrap();
  let pinned=r366_open(&w.path).unwrap();
  exec(&pinned,"BEGIN;").unwrap();
  assert_eq!(r366_query_num(&pinned,"SELECT COUNT(*) FROM events").unwrap(),1);
  w.append(&record("r445_new","decisions"),1_792_660_200_001,false).unwrap();
  let sample=r436_private_sample(&w.path).unwrap();
  assert!(sample.wal>0);
  let artificial=R436StorageSample{wal:R436_WAL_LIMIT-1_048_576,..sample};
  assert_eq!(r436_admit_sample(artificial),Err(EventOutboxR366Error::Pressure));
  assert_eq!(r436_admit_sample(sample),Ok(R436StorageState::Normal));
  exec(&pinned,"ROLLBACK;").unwrap();
  drop(pinned);
  assert_eq!(w.audit().unwrap().outbox,2);
  drop(w);fs::remove_dir_all(root).unwrap();
 }
}
