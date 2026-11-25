// R180: explicit operator-only recovery of a signed pending PARTIAL snapshot.
// This is an isolated RESEARCH operation, never automated or attached to live DB2.
impl SqliteMirrorReview{
 pub fn reconcile_partial_model_r180(&self,
  projection:&PaperProjectionR156,path:&Path,now:u64,batch_size:usize,
  owner:&str,key:&[u8],operator_approved:bool)
  ->Result<DetachedWitnessR154,PaperAdapterErrorR156>{
  self.reconcile_partial_with_fault_r194(projection,path,now,batch_size,
    owner,key,operator_approved,None)
 }
 pub fn reconcile_partial_with_fault_r194(&self,
  projection:&PaperProjectionR156,path:&Path,now:u64,batch_size:usize,
  owner:&str,key:&[u8],operator_approved:bool,phase:Option<&str>)
  ->Result<DetachedWitnessR154,PaperAdapterErrorR156>{
  if phase.is_some_and(|p|!matches!(p,"error_after_batch"|"sigkill_after_batch"
    |"sigkill_before_publish"))
    ||!operator_approved||batch_size==0||batch_size>16{
   return Err(PaperAdapterErrorR156::Source)
  }
  let _lock=acquire_floor_lock_r171(path)?;
  verify_projection_r162(projection,now)?;
  if self.inspect_local_floor_r173(path,now,owner,key)!=Ok(FloorReviewR173::Partial){
   return Err(PaperAdapterErrorR156::Source)
  }
  let mut floor=load_floor_r171(path,key)?;
  let intent=floor.pending.as_ref().ok_or(PaperAdapterErrorR156::Source)?;
  let tip=projection.rows.last().ok_or(PaperAdapterErrorR156::Source)?.seq;
  let prev=floor.witness.as_ref().map_or(0,|w|w.seq);
  if intent.expected_previous!=prev||intent.target_seq!=tip
    ||intent.session!=projection.session
    ||intent.projection_hash!=hex_r154(&projection.seal_r162){
   return Err(PaperAdapterErrorR156::Source)
  }
  let (current,_)=self.review_integrity_r152(now).map_err(|_|PaperAdapterErrorR156::Mirror)?;
  if current<=prev||current>=tip{return Err(PaperAdapterErrorR156::Source)}
  // Verify EVERY already-applied row as an immutable exact prefix.
  let present:Vec<_>=projection.rows.iter().filter(|r|r.seq<=current).collect();
  if present.is_empty()||present.last().unwrap().seq!=current {
   return Err(PaperAdapterErrorR156::Source)
  }
  for row in present{
   let old=self.original_row(row.seq,now).map_err(|_|PaperAdapterErrorR156::Source)?;
   if serde_json::to_vec(&old).ok()!=serde_json::to_vec(row).ok(){
    return Err(PaperAdapterErrorR156::Source)
   }
  }
  let pending:Vec<_>=projection.rows.iter().filter(|r|r.seq>current).cloned().collect();
  for batch in pending.chunks(batch_size){
   for row in batch{
    match self.original_row(row.seq,now){
     Ok(old)=>{
      if serde_json::to_vec(&old).ok()!=serde_json::to_vec(row).ok(){
       return Err(PaperAdapterErrorR156::Source)
      }
     },
     Err(MirrorDbError::Source)=>{
      self.append_synthetic_source(row.clone(),now)
       .map_err(|_|PaperAdapterErrorR156::Source)?;
     },
     Err(_)=>return Err(PaperAdapterErrorR156::Source),
    }
   }
   self.apply_batch_r160(batch,now).map_err(|_|PaperAdapterErrorR156::Mirror)?;
   r178_sigkill_at(phase,"sigkill_after_batch")?;
   if phase==Some("error_after_batch"){
    return Err(PaperAdapterErrorR156::Mirror)
   }
  }
  r178_sigkill_at(phase,"sigkill_before_publish")?;
  let witness=self.create_detached_witness_r154(now,owner,key)
   .map_err(|_|PaperAdapterErrorR156::Mirror)?;
  if witness.seq!=tip{return Err(PaperAdapterErrorR156::Mirror)}
  floor.witness=Some(witness.clone());
  floor.pending=None;
  publish_floor_r171(path,floor,key)?;
  Ok(witness)
 }
}

#[cfg(test)]
mod r177_r180_tests{
 use super::*;
 use std::{os::unix::fs::DirBuilderExt,sync::atomic::{AtomicUsize,Ordering}};
 static N:AtomicUsize=AtomicUsize::new(0);
 const NOW:u64=1_792_280_000_000;
 const KEY:&[u8]=b"r177-external-only-synthetic-test-key-value-001";
 const OWNER:&str="research-operator-180";
 fn make()->(PathBuf,PathBuf,SqliteMirrorReview){
  let ident=format!("{}_{}",std::process::id(),N.fetch_add(1,Ordering::Relaxed));
  let root=std::env::temp_dir().join(format!("astra_r147_isolated_r180_{ident}"));
  let wr=std::env::temp_dir().join(format!("astra_r171_isolated_r180_{ident}"));
  fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
  fs::DirBuilder::new().mode(0o700).create(&wr).unwrap();
  let db=SqliteMirrorReview::initialize(&root).unwrap();
  let path=wr.join("external-witness-floor.json");
  db.initialize_local_floor_r171(&path,NOW,OWNER,KEY).unwrap();
  (root,path,db)
 }
 fn clean(root:&Path,path:&Path){
  fs::remove_dir_all(root).unwrap();
  fs::remove_dir_all(path.parent().unwrap()).unwrap();
 }
 fn p(n:usize)->PaperProjectionR156{r156_r157_tests::projection(n)}
 #[test]fn r180_partial_requires_explicit_operator_and_matching_projection(){
  let (root,path,db)=make();
  let original=p(10);
  assert!(db.replay_with_local_floor_r172(&original,&path,NOW,3,OWNER,KEY,
    Some("after_first_batch")).is_err());
  let cold=SqliteMirrorReview::load(&root).unwrap();
  assert_eq!(cold.inspect_local_floor_r173(&path,NOW,OWNER,KEY).unwrap(),
    FloorReviewR173::Partial);
  assert_eq!(cold.reconcile_partial_model_r180(&original,&path,NOW,2,OWNER,KEY,false)
    .err(),Some(PaperAdapterErrorR156::Source));
  assert_eq!(cold.reconcile_partial_model_r180(&p(9),&path,NOW,2,OWNER,KEY,true)
    .err(),Some(PaperAdapterErrorR156::Source));
  assert_eq!(cold.review_integrity_r152(NOW).unwrap(),(45,3));
  let witness=cold.reconcile_partial_model_r180(&original,&path,NOW,3,OWNER,KEY,true)
    .unwrap();
  assert_eq!(witness.seq,52);
  assert_eq!(cold.inspect_local_floor_r173(&path,NOW,OWNER,KEY).unwrap(),
    FloorReviewR173::Clean);
  assert_eq!(cold.review_integrity_r152(NOW).unwrap(),(52,10));
  assert!(cold.reconcile_partial_model_r180(&original,&path,NOW,2,OWNER,KEY,true).is_err());
  clean(&root,&path);
 }
 #[test]fn r180_mismatched_session_cannot_reconcile_partial(){
  let (root,path,db)=make();let original=p(7);
  db.replay_with_local_floor_r172(&original,&path,NOW,2,OWNER,KEY,
    Some("after_first_batch")).unwrap_err();
  let (raw,h)=r156_r157_tests::session_journal_r159("session_201_1792253053",7);
  let other=model_projection_rows_r156(&raw,7,&h,NOW).unwrap();
  let cold=SqliteMirrorReview::load(&root).unwrap();
  assert!(cold.reconcile_partial_model_r180(&other,&path,NOW,2,OWNER,KEY,true).is_err());
  assert_eq!(cold.inspect_local_floor_r173(&path,NOW,OWNER,KEY).unwrap(),
    FloorReviewR173::Partial);
  clean(&root,&path);
 }
}
#[cfg(test)]
mod r194_tests{
 use super::*;
 use std::{os::unix::{fs::DirBuilderExt,process::ExitStatusExt},
  process::Command,sync::atomic::{AtomicUsize,Ordering}};
 static N:AtomicUsize=AtomicUsize::new(0);
 const NOW:u64=1_792_280_000_000;
 const KEY:&[u8]=b"r194-private-research-only-synthetic-key-01";
 const OWNER:&str="r194-operator";
 fn make()->(PathBuf,PathBuf){
  let id=format!("{}_{}",std::process::id(),N.fetch_add(1,Ordering::Relaxed));
  let root=std::env::temp_dir().join(format!("astra_r147_isolated_r194_{id}"));
  let dir=std::env::temp_dir().join(format!("astra_r171_isolated_r194_{id}"));
  fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
  fs::DirBuilder::new().mode(0o700).create(&dir).unwrap();
  let db=SqliteMirrorReview::initialize(&root).unwrap();
  let floor=dir.join("external-witness-floor.json");
  db.initialize_local_floor_r171(&floor,NOW,OWNER,KEY).unwrap();
  db.replay_with_local_floor_r172(&r156_r157_tests::projection(10),
    &floor,NOW,3,OWNER,KEY,Some("after_first_batch")).unwrap_err();
  (root,floor)
 }
 #[test]fn r194_second_simulated_crash_keeps_signed_partial_intent(){
  let (root,floor)=make();let db=SqliteMirrorReview::load(&root).unwrap();
  let p=r156_r157_tests::projection(10);
  assert!(db.reconcile_partial_with_fault_r194(&p,&floor,NOW,2,OWNER,KEY,
    true,Some("error_after_batch")).is_err());
  assert_eq!(db.inspect_local_floor_r173(&floor,NOW,OWNER,KEY).unwrap(),
    FloorReviewR173::Partial);
  assert_eq!(db.review_integrity_r152(NOW).unwrap().0,47);
  let confirmed=db.reconcile_partial_model_r180(&p,&floor,NOW,2,OWNER,KEY,true).unwrap();
  assert_eq!(confirmed.seq,52);
  assert_eq!(db.inspect_local_floor_r173(&floor,NOW,OWNER,KEY).unwrap(),
    FloorReviewR173::Clean);
  fs::remove_dir_all(root).unwrap();fs::remove_dir_all(floor.parent().unwrap()).unwrap();
 }
 #[test]fn r194_child_sigkill_during_operator_repair(){
  if std::env::var("ASTRA_R178_RESEARCH_CHILD").ok().as_deref()!=Some("ISOLATED_ONLY"){
   return
  }
  let root=PathBuf::from(std::env::var("ASTRA_R194_ROOT").unwrap());
  let floor=PathBuf::from(std::env::var("ASTRA_R194_FLOOR").unwrap());
  let phase=std::env::var("ASTRA_R194_PHASE").unwrap();
  assert!(root.to_string_lossy().contains("astra_r147_isolated_r194_"));
  assert!(floor.to_string_lossy().contains("astra_r171_isolated_r194_"));
  SqliteMirrorReview::load(&root).unwrap()
   .reconcile_partial_with_fault_r194(&r156_r157_tests::projection(10),&floor,
     NOW,2,OWNER,KEY,true,Some(&phase)).unwrap_err();
  panic!("isolated repair child unexpectedly survived SIGKILL")
 }
 #[test]fn r194_two_real_child_sigkill_phases_then_forensic_restart(){
  for (phase,expected,seq) in [
    ("sigkill_after_batch",FloorReviewR173::Partial,47u64),
    ("sigkill_before_publish",FloorReviewR173::CommittedUnpublished,52u64),
  ]{
   let (root,floor)=make();
   let child=Command::new(std::env::current_exe().unwrap())
    .arg("r194_child_sigkill_during_operator_repair").arg("--nocapture")
    .env("ASTRA_R178_RESEARCH_CHILD","ISOLATED_ONLY")
    .env("ASTRA_R194_ROOT",&root).env("ASTRA_R194_FLOOR",&floor)
    .env("ASTRA_R194_PHASE",phase).output().unwrap();
   assert_eq!(child.status.signal(),Some(libc::SIGKILL),
    "{phase}: {}",String::from_utf8_lossy(&child.stdout));
   let cold=SqliteMirrorReview::load(&root).unwrap();
   assert_eq!(cold.review_integrity_r152(NOW).unwrap().0,seq);
   assert_eq!(cold.inspect_local_floor_r173(&floor,NOW,OWNER,KEY).unwrap(),expected);
   let evidence=inspect_stable_lock_r193(&floor).unwrap().unwrap();
   assert!(!evidence.auto_release_allowed);
   assert!(cold.reconcile_partial_model_r180(
     &r156_r157_tests::projection(10),&floor,NOW,2,OWNER,KEY,true).is_err());
   // Test-only manual reconciliation: child has exited by SIGKILL and
   // inode is reread immediately before deleting this PRIVATE test lock.
   let second=inspect_stable_lock_r193(&floor).unwrap().unwrap();
   assert_eq!(second.inode,evidence.inode);
   fs::remove_file(floor.with_extension("lock")).unwrap();
   let p=r156_r157_tests::projection(10);
   let witness=if expected==FloorReviewR173::Partial{
    cold.reconcile_partial_model_r180(&p,&floor,NOW,2,OWNER,KEY,true).unwrap()
   }else{
    cold.publish_committed_intent_r173(&p,&floor,NOW,OWNER,KEY).unwrap()
   };
   assert_eq!(witness.seq,52);
   assert_eq!(cold.inspect_local_floor_r173(&floor,NOW,OWNER,KEY).unwrap(),
     FloorReviewR173::Clean);
   fs::remove_dir_all(&root).unwrap();
   fs::remove_dir_all(floor.parent().unwrap()).unwrap();
  }
 }
}
