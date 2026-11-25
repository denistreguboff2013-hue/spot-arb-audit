// R152: cold readback of EVERY isolated SQLite source/destination row.
// Bounded synthetic model only. Same-store checks are NOT external authorization.
fn r152_sqlite_page_integrity(db:&Db)->Result<(),MirrorDbError>{
 let mut q=Stmt::new(db,"PRAGMA integrity_check")?;
 if q.step()?!=SQLITE_ROW||q.strval(0)?!="ok"||q.step()?!=SQLITE_DONE{
  return Err(MirrorDbError::Corrupt)
 }
 Ok(())
}
impl SqliteMirrorReview {
 pub fn review_integrity_r152(&self, now:u64)->Result<(u64,usize),MirrorDbError>{
  let a=open(&self.db1,READONLY)?;
  let b=open(&self.db2,READONLY)?;
  r152_sqlite_page_integrity(&a)?;
  r152_sqlite_page_integrity(&b)?;
  let source_cursor=cursor(&a,"control")?;
  let mut sq=Stmt::new(&a,"SELECT seq,tx_id,row_json FROM outbox ORDER BY seq")?;
  let mut source=Vec::new();
  while sq.step()?==SQLITE_ROW {
   if source.len()>=256{return Err(MirrorDbError::Schema)}
   let seq=sq.num(0)?;let tx=sq.strval(1)?;let raw=sq.strval(2)?;
   let row:MirrorOutboxRowR139=serde_json::from_str(&raw)
    .map_err(|_|MirrorDbError::Corrupt)?;
   if row.seq!=seq||row.tx_id!=tx{return Err(MirrorDbError::Corrupt)}
   source.push((row,raw));
  }
  if source.last().map_or(0,|x|x.0.seq)!=source_cursor {
   return Err(MirrorDbError::Corrupt)
  }
  if let Some((first,_))=source.first(){
   let chain:Vec<MirrorOutboxRowR139>=source.iter().map(|x|x.0.clone()).collect();
   audit_outbox_chain(&chain,first.prev_seq,now).map_err(|_|MirrorDbError::Corrupt)?;
  }
  let mut cq=Stmt::new(&b,"SELECT seq,anchor,generation FROM cursor WHERE id=1")?;
  if cq.step()?!=SQLITE_ROW{return Err(MirrorDbError::Schema)}
  let last=cq.num(0)?;let anchor=cq.num(1)?;let generation=cq.strval(2)?;
  if cq.step()?!=SQLITE_DONE{return Err(MirrorDbError::Corrupt)}
  let mut dq=Stmt::new(&b,"SELECT seq,tx_id,row_json FROM events ORDER BY seq")?;
  let mut count=0usize;
  while dq.step()?==SQLITE_ROW {
   let (original,raw)=source.get(count).ok_or(MirrorDbError::Corrupt)?;
   if dq.num(0)?!=original.seq||dq.strval(1)?!=original.tx_id
      ||dq.strval(2)?!=*raw{return Err(MirrorDbError::Corrupt)}
   count+=1;
  }
  if last==0 {
   if count!=0||anchor!=0||!generation.is_empty(){
    return Err(MirrorDbError::Corrupt)
   }
  }else {
   if count==0||source[count-1].0.seq!=last
      ||source[0].0.prev_seq!=anchor
      ||source[0].0.feed_generation!=generation {
    return Err(MirrorDbError::Corrupt)
   }
  }
  Ok((last,count))
 }
}
#[cfg(test)]
mod r152_tests {
 use super::*;
 use std::{os::unix::fs::DirBuilderExt,sync::atomic::{AtomicUsize,Ordering}};
 static N:AtomicUsize=AtomicUsize::new(0);
 const NOW:u64=1_791_784_000_000;
 fn setup()->(PathBuf,SqliteMirrorReview){
  let root=std::env::temp_dir().join(format!("astra_r147_isolated_r152_{}_{}",
   std::process::id(),N.fetch_add(1,Ordering::Relaxed)));
  fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
  let db=SqliteMirrorReview::initialize(&root).unwrap();
  (root,db)
 }
 fn row(seq:u64,prev:u64)->MirrorOutboxRowR139{
  let mut x:MirrorOutboxRowR139=serde_json::from_str(include_str!(
   "../config/native_mirror_outbox_python_fixture_r141_review.json")).unwrap();
  x.seq=seq;x.prev_seq=prev;x.tx_id=format!("tx-{seq:032x}");x
 }
 #[test]fn r152_cold_empty_partial_and_full(){
  let (root,db)=setup();
  assert_eq!(db.review_integrity_r152(NOW).unwrap(),(0,0));
  db.append_synthetic_source(row(43,42),NOW).unwrap();
  db.append_synthetic_source(row(100,43),NOW).unwrap();
  db.apply_offline(43,NOW,false).unwrap();
  let cold=SqliteMirrorReview::load(&root).unwrap();
  assert_eq!(cold.review_integrity_r152(NOW).unwrap(),(43,1));
  db.apply_offline(100,NOW,false).unwrap();
  assert_eq!(cold.review_integrity_r152(NOW).unwrap(),(100,2));
  fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r152_db2_valid_json_swap_denied_despite_same_count(){
  let (root,db)=setup();
  db.append_synthetic_source(row(43,42),NOW).unwrap();
  db.apply_offline(43,NOW,false).unwrap();
  let b=open(&db.db2,READWRITE_EXISTING).unwrap();
  exec(&b,"UPDATE events SET tx_id='tx-deadbeef' WHERE seq=43").unwrap();
  drop(b);
  assert_eq!(db.review_destination().unwrap(),(43,1));
  assert_eq!(SqliteMirrorReview::load(&root).unwrap()
   .review_integrity_r152(NOW).err(),Some(MirrorDbError::Corrupt));
  fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r152_swapped_valid_json_row_denied(){
  let (root,db)=setup();
  db.append_synthetic_source(row(43,42),NOW).unwrap();
  db.append_synthetic_source(row(100,43),NOW).unwrap();
  db.apply_offline(43,NOW,false).unwrap();
  db.apply_offline(100,NOW,false).unwrap();
  let b=open(&db.db2,READWRITE_EXISTING).unwrap();
  exec(&b,"UPDATE events SET row_json=(SELECT row_json FROM events WHERE seq=100) WHERE seq=43")
   .unwrap();
  drop(b);
  assert_eq!(db.review_destination().unwrap(),(100,2));
  assert_eq!(db.review_integrity_r152(NOW).err(),Some(MirrorDbError::Corrupt));
  fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r152_cursor_anchor_and_generation_tamper_denied(){
  for sql in ["UPDATE cursor SET anchor=41","UPDATE cursor SET generation='rebase-bad'",
    "UPDATE cursor SET seq=99"] {
   let (root,db)=setup();
   db.append_synthetic_source(row(43,42),NOW).unwrap();
   db.apply_offline(43,NOW,false).unwrap();
   let b=open(&db.db2,READWRITE_EXISTING).unwrap();exec(&b,sql).unwrap();drop(b);
   assert_eq!(db.review_integrity_r152(NOW).err(),Some(MirrorDbError::Corrupt));
   fs::remove_dir_all(root).unwrap();
  }
 }
 #[test]fn r152_embedded_nul_suffix_cannot_hide_corruption(){
  let (root,db)=setup();
  db.append_synthetic_source(row(43,42),NOW).unwrap();
  db.apply_offline(43,NOW,false).unwrap();
  let b=open(&db.db2,READWRITE_EXISTING).unwrap();
  exec(&b,"UPDATE events SET row_json=row_json || char(0) || 'hidden' WHERE seq=43")
   .unwrap();
  drop(b);
  assert_eq!(db.review_integrity_r152(NOW).err(),Some(MirrorDbError::Corrupt));
  fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r152_source_missing_or_rewritten_denied(){
  for sql in ["DELETE FROM outbox WHERE seq=43",
    "UPDATE outbox SET row_json='{}' WHERE seq=43"]{
   let (root,db)=setup();
   db.append_synthetic_source(row(43,42),NOW).unwrap();
   db.apply_offline(43,NOW,false).unwrap();
   let a=open(&db.db1,READWRITE_EXISTING).unwrap();exec(&a,sql).unwrap();drop(a);
   assert_eq!(db.review_integrity_r152(NOW).err(),Some(MirrorDbError::Corrupt));
   fs::remove_dir_all(root).unwrap();
  }
 }
}
