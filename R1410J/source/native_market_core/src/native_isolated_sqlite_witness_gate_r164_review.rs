// R164-R170: external-caller witness floor gate, RESEARCH-ONLY isolated SQLite.
// The caller must durably store the returned witness OUTSIDE these databases.
// This helper never owns, writes or derives the HMAC key and cannot authorize trading.
#[derive(Debug,Clone)]
pub struct WitnessedPaperReplayR164{
 pub replay:PaperReplayR157,
 pub next_witness:DetachedWitnessR154,
 pub durable_external_publication_required:bool,
}
impl SqliteMirrorReview{
 pub fn replay_modeled_paper_with_floor_r164(&self,
  projection:&PaperProjectionR156,now:u64,batch_size:usize,
  owner:&str,key:&[u8],expected_floor:Option<&DetachedWitnessR154>)
  ->Result<WitnessedPaperReplayR164,PaperAdapterErrorR156>{
  if reject_identity_r154(owner)||key.len()<16{
   return Err(PaperAdapterErrorR156::Source)
  }
  // Audit both databases before comparing the caller-controlled witness.
  let (current,count)=self.review_integrity_r152(now)
   .map_err(|_|PaperAdapterErrorR156::Mirror)?;
  if count==0{
   if current!=0||expected_floor.is_some(){
    return Err(PaperAdapterErrorR156::Source)
   }
  }else{
   let floor=expected_floor.ok_or(PaperAdapterErrorR156::Source)?;
   // Historical witness verification alone does NOT establish a current floor.
   if floor.seq!=current{
    return Err(PaperAdapterErrorR156::Source)
   }
   self.verify_detached_witness_r154(now,floor,owner,key)
    .map_err(|_|PaperAdapterErrorR156::Source)?;
  }
  let replay=self.replay_modeled_paper_r157(projection,now,batch_size)?;
  let next_witness=self.create_detached_witness_r154(now,owner,key)
   .map_err(|_|PaperAdapterErrorR156::Mirror)?;
  if next_witness.seq!=replay.final_seq{
   return Err(PaperAdapterErrorR156::Mirror)
  }
  Ok(WitnessedPaperReplayR164{
   replay,next_witness,durable_external_publication_required:true,
  })
 }
}

#[cfg(test)]
mod r164_r170_witness_tests{
 use super::*;
 use std::{os::unix::fs::DirBuilderExt,sync::atomic::{AtomicUsize,Ordering}};
 static N:AtomicUsize=AtomicUsize::new(0);
 const NOW:u64=1_792_280_000_000;
 const KEY:&[u8]=b"r164-only-external-synthetic-ephemeral-secret-01";
 const OWNER:&str="isolated-research-owner";
 fn setup()->(PathBuf,SqliteMirrorReview){
  let root=std::env::temp_dir().join(format!("astra_r147_isolated_r164_{}_{}",
   std::process::id(),N.fetch_add(1,Ordering::Relaxed)));
  fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
  let db=SqliteMirrorReview::initialize(&root).unwrap();
  (root,db)
 }
 fn projection(n:usize)->PaperProjectionR156{
  super::r156_r157_tests::projection(n)
 }
 #[test]fn r164_first_attested_replay_then_cold_restart_idempotence(){
  let (root,db)=setup();let p=projection(4);
  let first=db.replay_modeled_paper_with_floor_r164(&p,NOW,2,OWNER,KEY,None).unwrap();
  assert_eq!((first.replay.newly_applied,first.next_witness.seq),(4,46));
  assert!(first.durable_external_publication_required);
  let cold=SqliteMirrorReview::load(&root).unwrap();
  let second=cold.replay_modeled_paper_with_floor_r164(&p,NOW,3,OWNER,KEY,
   Some(&first.next_witness)).unwrap();
  assert_eq!((second.replay.newly_applied,second.replay.already_applied),(0,4));
  assert_eq!(second.next_witness.mac,first.next_witness.mac);
  assert!(!second.replay.may_trade);
  fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r165_nonempty_requires_external_floor_and_correct_owner_key(){
  let (root,db)=setup();let p=projection(3);
  let w=db.replay_modeled_paper_with_floor_r164(&p,NOW,2,OWNER,KEY,None)
   .unwrap().next_witness;
  assert_eq!(db.replay_modeled_paper_with_floor_r164(&p,NOW,2,OWNER,KEY,None)
   .err(),Some(PaperAdapterErrorR156::Source));
  assert_eq!(db.replay_modeled_paper_with_floor_r164(&p,NOW,2,"other",KEY,Some(&w))
   .err(),Some(PaperAdapterErrorR156::Source));
  assert_eq!(db.replay_modeled_paper_with_floor_r164(&p,NOW,2,OWNER,b"wrong-key-long-enough-123",Some(&w))
   .err(),Some(PaperAdapterErrorR156::Source));
  assert_eq!(db.review_integrity_r152(NOW).unwrap(),(45,3));
  fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r166_stale_but_cryptographically_valid_floor_denied(){
  let (root,db)=setup();let a=projection(2);let b=projection(4);
  let old=db.replay_modeled_paper_with_floor_r164(&a,NOW,2,OWNER,KEY,None)
   .unwrap().next_witness;
  let current=db.replay_modeled_paper_with_floor_r164(&b,NOW,2,OWNER,KEY,Some(&old))
   .unwrap().next_witness;
  assert!(db.verify_detached_witness_r154(NOW,&old,OWNER,KEY).is_ok());
  assert_eq!(db.replay_modeled_paper_with_floor_r164(&b,NOW,2,OWNER,KEY,Some(&old))
   .err(),Some(PaperAdapterErrorR156::Source));
  assert_eq!(db.replay_modeled_paper_with_floor_r164(&b,NOW,2,OWNER,KEY,Some(&current))
   .unwrap().replay.already_applied,4);
  fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r167_both_sqlite_files_rollback_rejected_by_current_external_floor(){
  let (root,db)=setup();let a=projection(2);let b=projection(4);
  let old=db.replay_modeled_paper_with_floor_r164(&a,NOW,2,OWNER,KEY,None)
   .unwrap().next_witness;
  let old_db1=fs::read(&db.db1).unwrap();let old_db2=fs::read(&db.db2).unwrap();
  let current=db.replay_modeled_paper_with_floor_r164(&b,NOW,2,OWNER,KEY,Some(&old))
   .unwrap().next_witness;
  fs::write(&db.db1,old_db1).unwrap();fs::write(&db.db2,old_db2).unwrap();
  let cold=SqliteMirrorReview::load(&root).unwrap();
  assert_eq!(cold.review_integrity_r152(NOW).unwrap(),(44,2));
  assert_eq!(cold.replay_modeled_paper_with_floor_r164(&b,NOW,2,OWNER,KEY,
    Some(&current)).err(),Some(PaperAdapterErrorR156::Source));
  fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r168_corrupted_hmac_and_false_floor_on_empty_rejected(){
  let (root,db)=setup();let p=projection(2);
  let witness=db.replay_modeled_paper_with_floor_r164(&p,NOW,2,OWNER,KEY,None)
   .unwrap().next_witness;
  let mut bad=witness.clone();bad.mac.replace_range(0..1,"f");
  if bad.mac==witness.mac{bad.mac.replace_range(0..1,"0");}
  assert_eq!(db.replay_modeled_paper_with_floor_r164(&p,NOW,2,OWNER,KEY,Some(&bad))
   .err(),Some(PaperAdapterErrorR156::Source));
  let (root2,empty)=setup();
  assert_eq!(empty.replay_modeled_paper_with_floor_r164(&p,NOW,2,OWNER,KEY,
    Some(&witness)).err(),Some(PaperAdapterErrorR156::Source));
  assert_eq!(empty.review_integrity_r152(NOW).unwrap(),(0,0));
  fs::remove_dir_all(root).unwrap();fs::remove_dir_all(root2).unwrap();
 }
 #[test]fn r169_partial_source_without_destination_stays_fail_closed(){
  let (root,db)=setup();let p=projection(2);
  db.append_synthetic_source(p.rows[0].clone(),NOW).unwrap();
  assert_eq!(db.review_integrity_r152(NOW).unwrap(),(0,0));
  let done=db.replay_modeled_paper_with_floor_r164(&p,NOW,2,OWNER,KEY,None)
   .unwrap();
  assert_eq!(done.replay.final_seq,44);
  let cold=SqliteMirrorReview::load(&root).unwrap();
  assert!(cold.verify_detached_witness_r154(NOW,&done.next_witness,OWNER,KEY).is_ok());
  fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r170_bounded_32_row_snapshot_cold_reopen(){
  let (root,db)=setup();let p=projection(32);
  let first=db.replay_modeled_paper_with_floor_r164(&p,NOW,16,OWNER,KEY,None)
   .unwrap();
  assert_eq!((first.replay.newly_applied,first.replay.final_seq),(32,74));
  let cold=SqliteMirrorReview::load(&root).unwrap();
  let second=cold.replay_modeled_paper_with_floor_r164(&p,NOW,7,OWNER,KEY,
    Some(&first.next_witness)).unwrap();
  assert_eq!((second.replay.already_applied,second.replay.newly_applied),(32,0));
  assert_eq!(cold.review_integrity_r152(NOW).unwrap(),(74,32));
  fs::remove_dir_all(root).unwrap();
 }
}
