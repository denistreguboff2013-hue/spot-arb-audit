// R160-R161: atomic destination batch for OFFLINE synthetic DB2 only.
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct BatchApplyR160{
 pub applied:usize,pub skipped:usize,pub final_seq:u64,
 pub production_db2:bool,pub may_trade:bool,
}
impl SqliteMirrorReview{
 pub fn apply_batch_r160(&self,rows:&[MirrorOutboxRowR139],now:u64)
  ->Result<BatchApplyR160,MirrorDbError>{
  self.apply_batch_with_fault_r161(rows,now,None)
 }
 pub fn apply_batch_with_fault_r161(&self,rows:&[MirrorOutboxRowR139],
  now:u64,phase:Option<&str>)->Result<BatchApplyR160,MirrorDbError>{
  if rows.is_empty()||rows.len()>16||phase.is_some_and(|x|
    !matches!(x,"rollback_after_insert"|"rollback_after_cas"
      |"after_first_insert"|"after_cas"|"after_commit")){
   return Err(MirrorDbError::Schema)
  }
  // Cold audit before any DB2 mutation: deny corruption even on no-op retry.
  self.review_integrity_r152(now)?;
  // Check source identity BEFORE the DB2 transaction; reject forged rows.
  for row in rows{
   let original=self.original_row(row.seq,now)?;
   if serde_json::to_vec(&original).map_err(|_|MirrorDbError::Corrupt)?
     !=serde_json::to_vec(row).map_err(|_|MirrorDbError::Corrupt)?{
    return Err(MirrorDbError::Source)
   }
  }
  let db=open(&self.db2,READWRITE_EXISTING)?;
  let tx=Tx::begin(&db)?;
  let mut q=Stmt::new(&db,"SELECT seq,anchor,generation FROM cursor WHERE id=1")?;
  if q.step()?!=SQLITE_ROW{return Err(MirrorDbError::Schema)}
  let current=q.num(0)?;let anchor=q.num(1)?;let generation=q.strval(2)?;
  if q.step()!=Ok(SQLITE_DONE){return Err(MirrorDbError::Corrupt)}
  drop(q);
  if rows.windows(2).any(|w|w[0].seq.checked_add(1)!=Some(w[1].seq)
    ||w[1].prev_seq!=w[0].seq||w[1].feed_generation!=w[0].feed_generation){
   return Err(MirrorDbError::Cursor)
  }
  let todo:Vec<_>=rows.iter().filter(|r|r.seq>current).collect();
  let skipped=rows.len()-todo.len();
  if todo.is_empty(){
   drop(tx);
   return Ok(BatchApplyR160{
    applied:0,skipped,final_seq:current,production_db2:false,may_trade:false
   })
  }
  let first=todo[0];
  if current==0{
   if anchor!=0||!generation.is_empty()||first.prev_seq==0||
      self.first_available_source_seq()?!=first.seq{
    return Err(MirrorDbError::Cursor)
   }
  }else if anchor==0||generation!=first.feed_generation||
    first.prev_seq!=current{
   return Err(MirrorDbError::Cursor)
  }
  for row in &todo{
   one_insert(&db,"events",row)?;
   if row.seq==first.seq{r153_pause(phase,"after_first_insert");}
  }
  if phase==Some("rollback_after_insert"){return Err(MirrorDbError::Sql)}
  let mut q=Stmt::new(&db,"UPDATE cursor SET seq=?1,anchor=?2,generation=?3 WHERE id=1 AND seq=?4 AND anchor=?5 AND generation=?6")?;
  q.number(1,todo.last().unwrap().seq)?;
  q.number(2,if current==0{first.prev_seq}else{anchor})?;
  q.string(3,&first.feed_generation)?;
  q.number(4,current)?;q.number(5,anchor)?;q.string(6,&generation)?;
  if q.step()?!=SQLITE_DONE||unsafe{sqlite3_changes(db.0)}!=1{
   return Err(MirrorDbError::Cursor)
  }
  drop(q);
  r153_pause(phase,"after_cas");
  if phase==Some("rollback_after_cas"){return Err(MirrorDbError::Sql)}
  tx.commit()?;
  r153_pause(phase,"after_commit");
  let (observed,_) =self.review_integrity_r152(now)?;
  if observed!=todo.last().unwrap().seq{
   return Err(MirrorDbError::Corrupt)
  }
  Ok(BatchApplyR160{
   applied:todo.len(),skipped,final_seq:observed,
   production_db2:false,may_trade:false
  })
 }
}
