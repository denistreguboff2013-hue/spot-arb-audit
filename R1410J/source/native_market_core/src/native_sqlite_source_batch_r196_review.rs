// R196: atomic <=16 synthetic DB1 source rows per transaction.
// No production DB1, no change to real journal or exchange permissions.
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct SourceBatchR196{
 pub applied:usize,pub last_seq:u64,pub production_db1:bool,pub may_trade:bool,
}
impl SqliteMirrorReview{
 pub fn append_source_batch_r196(&self,rows:&[MirrorOutboxRowR139],now:u64,
  rollback_before_commit:bool)->Result<SourceBatchR196,MirrorDbError>{
  if rows.is_empty()||rows.len()>16{return Err(MirrorDbError::Schema)}
  // Full cold validation prevents appending onto an already corrupt chain.
  self.review_integrity_r152(now)?;
  audit_outbox_chain(rows,rows[0].prev_seq,now)
   .map_err(|_|MirrorDbError::Source)?;
  let db=open(&self.db1,READWRITE_EXISTING)?;
  let tx=Tx::begin(&db)?;
  let current=cursor(&db,"control")?;
  if rows[0].prev_seq!=current&&(current!=0||rows[0].prev_seq==0){
   return Err(MirrorDbError::Cursor)
  }
  if current==0&&rows[0].prev_seq==0{return Err(MirrorDbError::Cursor)}
  if current!=0{
   let mut previous=Stmt::new(&db,"SELECT row_json FROM outbox WHERE seq=?1")?;
   previous.number(1,current)?;
   if previous.step()?!=SQLITE_ROW{return Err(MirrorDbError::Corrupt)}
   let raw=previous.strval(0)?;
   if previous.step()?!=SQLITE_DONE{return Err(MirrorDbError::Corrupt)}
   let prior:MirrorOutboxRowR139=serde_json::from_str(&raw)
    .map_err(|_|MirrorDbError::Corrupt)?;
   if prior.feed_generation!=rows[0].feed_generation{
    return Err(MirrorDbError::Source)
   }
  }
  for row in rows{one_insert(&db,"outbox",row)?;}
  let mut q=Stmt::new(&db,"UPDATE control SET seq=?1 WHERE id=1 AND seq=?2")?;
  q.number(1,rows.last().unwrap().seq)?;q.number(2,current)?;
  if q.step()?!=SQLITE_DONE||unsafe{sqlite3_changes(db.0)}!=1{
   return Err(MirrorDbError::Cursor)
  }
  drop(q);
  if rollback_before_commit{return Err(MirrorDbError::Sql)}
  tx.commit()?;
  // Integrity includes both DB1 and DB2, which may legitimately lag.
  let (dest,_)=self.review_integrity_r152(now)?;
  if dest>rows.last().unwrap().seq{return Err(MirrorDbError::Corrupt)}
  Ok(SourceBatchR196{applied:rows.len(),last_seq:rows.last().unwrap().seq,
   production_db1:false,may_trade:false})
 }
}
#[cfg(test)]
mod r196_tests{
 use super::*;
 use std::{os::unix::fs::DirBuilderExt,sync::atomic::{AtomicUsize,Ordering}};
 static N:AtomicUsize=AtomicUsize::new(0);
 const NOW:u64=1_792_280_000_000;
 fn make()->(PathBuf,SqliteMirrorReview){
  let root=std::env::temp_dir().join(format!("astra_r147_isolated_r196_{}_{}",
   std::process::id(),N.fetch_add(1,Ordering::Relaxed)));
  fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
  let db=SqliteMirrorReview::initialize(&root).unwrap();
  (root,db)
 }
 #[test]fn r196_atomic_source_batch_and_cold_idempotent_dest(){
  let (root,db)=make();
  let p=r156_r157_tests::projection(16);
  let batch=db.append_source_batch_r196(&p.rows,NOW,false).unwrap();
  assert_eq!((batch.applied,batch.last_seq),(16,58));
  assert!(!batch.may_trade&&!batch.production_db1);
  assert_eq!(db.review_integrity_r152(NOW).unwrap(),(0,0));
  let applied=db.apply_batch_r160(&p.rows,NOW).unwrap();
  assert_eq!((applied.applied,applied.final_seq),(16,58));
  let cold=SqliteMirrorReview::load(&root).unwrap();
  assert_eq!(cold.review_integrity_r152(NOW).unwrap(),(58,16));
  assert_eq!(cold.apply_batch_r160(&p.rows,NOW).unwrap().skipped,16);
  fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r196_failure_before_commit_does_not_leave_partial_source(){
  let (root,db)=make();
  let p=r156_r157_tests::projection(16);
  assert_eq!(db.append_source_batch_r196(&p.rows,NOW,true).err(),
    Some(MirrorDbError::Sql));
  assert_eq!(db.review_integrity_r152(NOW).unwrap(),(0,0));
  db.append_source_batch_r196(&p.rows,NOW,false).unwrap();
  assert_eq!(db.append_source_batch_r196(&p.rows,NOW,false).err(),
    Some(MirrorDbError::Cursor));
  fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r196_batched_source_matches_old_per_row_representation(){
  let (a_root,a)=make();let (b_root,b)=make();
  let p=r156_r157_tests::projection(12);
  for r in &p.rows{a.append_synthetic_source(r.clone(),NOW).unwrap()}
  b.append_source_batch_r196(&p.rows,NOW,false).unwrap();
  for r in &p.rows{
   let left=a.original_row(r.seq,NOW).unwrap();
   let right=b.original_row(r.seq,NOW).unwrap();
   assert_eq!(serde_json::to_vec(&left).unwrap(),serde_json::to_vec(&right).unwrap());
  }
  a.apply_batch_r160(&p.rows[..12],NOW).unwrap();
  b.apply_batch_r160(&p.rows[..12],NOW).unwrap();
  assert_eq!(a.review_integrity_r152(NOW).unwrap(),
    b.review_integrity_r152(NOW).unwrap());
  fs::remove_dir_all(a_root).unwrap();fs::remove_dir_all(b_root).unwrap();
 }
}
