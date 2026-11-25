// R188: bounded OFFLINE cost probe for R157 / R160.
// This measures SQLite research replay only, NOT the Rust market hot-path,
// production DB2 or orders per second.
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct ReplayCostR188{
 pub events:usize,pub batch_size:usize,pub batches:usize,
 pub source_transactions:usize,pub min_integrity_audits:usize,
 pub production_db2:bool,pub may_trade:bool,
}
pub fn expected_replay_cost_r188(events:usize,batch_size:usize)
 ->Result<ReplayCostR188,PaperAdapterErrorR156>{
 if events==0||events>256||batch_size==0||batch_size>16{
  return Err(PaperAdapterErrorR156::Limit)
 }
 let batches=(events+batch_size-1)/batch_size;
 // Each R160 batch cold-audits both DBs before and after the commit.
 // R157 additionally audits once at the end; R154 floor attestations may add more.
 Ok(ReplayCostR188{
  events,batch_size,batches,source_transactions:events,
  min_integrity_audits:2*batches+1,production_db2:false,may_trade:false
 })
}
#[cfg(test)]
mod r188_tests{
 use super::*;
 use std::{time::Instant,os::unix::fs::DirBuilderExt,
 sync::atomic::{AtomicUsize,Ordering}};
 static N:AtomicUsize=AtomicUsize::new(0);
 const NOW:u64=1_792_280_000_000;
 #[test]fn r188_bounded_cost_model_and_live_offline_profile(){
  assert_eq!(expected_replay_cost_r188(64,16).unwrap().min_integrity_audits,9);
  assert_eq!(expected_replay_cost_r188(64,4).unwrap().min_integrity_audits,33);
  assert_eq!(expected_replay_cost_r188(257,16).err(),Some(PaperAdapterErrorR156::Limit));
  for (events,batch) in [(16,4),(32,16),(64,16)]{
   let root=std::env::temp_dir().join(format!("astra_r147_isolated_r188_{}_{}",
    std::process::id(),N.fetch_add(1,Ordering::Relaxed)));
   fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
   let db=SqliteMirrorReview::initialize(&root).unwrap();
   let p=r156_r157_tests::projection(events);
   let started=Instant::now();
   let report=db.replay_modeled_paper_r157(&p,NOW,batch).unwrap();
   let micros=started.elapsed().as_micros();
   assert_eq!(report.newly_applied,events);
   assert_eq!(db.review_integrity_r152(NOW).unwrap().1,events);
   eprintln!("R188_ISOLATED_ONLY events={events} batch={batch} elapsed_us={micros} note=NOT_HOT_PATH");
   fs::remove_dir_all(root).unwrap();
  }
 }
}
