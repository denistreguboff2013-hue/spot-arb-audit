// R204: compare isolated old per-row DB1 insert to R196 atomic batches.
// Performance evidence ONLY. No Rust hot-path throughput or production claim.
#[cfg(test)]
mod r204_source_benchmark_tests{
 use super::*;
 use std::{time::Instant,os::unix::fs::DirBuilderExt,
  sync::atomic::{AtomicUsize,Ordering}};
 static N:AtomicUsize=AtomicUsize::new(0);
 const NOW:u64=1_792_280_000_000;
 fn setup()->(PathBuf,SqliteMirrorReview){
  let root=std::env::temp_dir().join(format!("astra_r147_isolated_r204_{}_{}",
   std::process::id(),N.fetch_add(1,Ordering::Relaxed)));
  fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
  let db=SqliteMirrorReview::initialize(&root).unwrap();
  (root,db)
 }
 #[test]fn r204_same_128_rows_old_vs_atomic_source_path(){
  let p=r156_r157_tests::projection(128);
  let (aroot,old)=setup();let (broot,new)=setup();
  let start=Instant::now();
  for r in &p.rows{old.append_synthetic_source(r.clone(),NOW).unwrap()}
  let row_micros=start.elapsed().as_micros();
  let start=Instant::now();
  for chunk in p.rows.chunks(16){
   new.append_source_batch_r196(chunk,NOW,false).unwrap();
  }
  let batch_micros=start.elapsed().as_micros();
  assert_eq!(old.review_integrity_r152(NOW).unwrap(),(0,0));
  assert_eq!(new.review_integrity_r152(NOW).unwrap(),(0,0));
  for chunk in p.rows.chunks(16){
   old.apply_batch_r160(chunk,NOW).unwrap();
   new.apply_batch_r160(chunk,NOW).unwrap();
  }
  assert_eq!(old.review_integrity_r152(NOW).unwrap(),(170,128));
  assert_eq!(new.review_integrity_r152(NOW).unwrap(),(170,128));
  eprintln!("R204_OFFLINE_SOURCE_ONLY events=128 old_row_us={} atomic_batches_us={} batches=8",
   row_micros,batch_micros);
  fs::remove_dir_all(aroot).unwrap();
  fs::remove_dir_all(broot).unwrap();
 }
}
