// R186: deliberate offline corruption of PRIVATE /tmp research DB1 only.
// The tests must fail closed while the operator floor stays in Partial state.
#[cfg(test)]
mod r186_attack_tests{
 use super::*;
 use std::{os::unix::fs::DirBuilderExt,sync::atomic::{AtomicUsize,Ordering}};
 static N:AtomicUsize=AtomicUsize::new(0);
 const NOW:u64=1_792_280_000_000;
 const KEY:&[u8]=b"synthetic-test-only-r186-key-00001";
 const OWNER:&str="r186-recovery-owner";
 fn setup()->(PathBuf,PathBuf,SqliteMirrorReview){
  let id=format!("{}_{}",std::process::id(),N.fetch_add(1,Ordering::Relaxed));
  let root=std::env::temp_dir().join(format!("astra_r147_isolated_r186_{id}"));
  let wr=std::env::temp_dir().join(format!("astra_r171_isolated_r186_{id}"));
  fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
  fs::DirBuilder::new().mode(0o700).create(&wr).unwrap();
  let db=SqliteMirrorReview::initialize(&root).unwrap();
  let f=wr.join("external-witness-floor.json");
  db.initialize_local_floor_r171(&f,NOW,OWNER,KEY).unwrap();
  (root,f,db)
 }
 fn cleanup(root:&Path,f:&Path){
  fs::remove_dir_all(root).unwrap();fs::remove_dir_all(f.parent().unwrap()).unwrap();
 }
 #[test]fn r186_wrong_generation_projection_denied_without_mutation(){
  let (root,f,db)=setup();let original=r156_r157_tests::projection(10);
  db.replay_with_local_floor_r172(&original,&f,NOW,3,OWNER,KEY,
   Some("after_first_batch")).unwrap_err();
  let before=db.review_integrity_r152(NOW).unwrap();
  let mut changed=original.clone();
  changed.rows[5].feed_generation.push_str("-forged");
  changed.seal_r162=paper_projection_digest_r162(&changed).unwrap();
  assert!(db.reconcile_partial_model_r180(&changed,&f,NOW,3,OWNER,KEY,true).is_err());
  assert_eq!(db.review_integrity_r152(NOW).unwrap(),before);
  assert_eq!(db.inspect_local_floor_r173(&f,NOW,OWNER,KEY).unwrap(),
    FloorReviewR173::Partial);
  cleanup(&root,&f);
 }
 #[test]fn r186_db1_row_corruption_denied_in_partial_recovery(){
  let (root,f,db)=setup();
  let p=r156_r157_tests::projection(9);
  db.replay_with_local_floor_r172(&p,&f,NOW,2,OWNER,KEY,
    Some("after_first_batch")).unwrap_err();
  let source=open(&db.db1,READWRITE_EXISTING).unwrap();
  exec(&source,"UPDATE outbox SET row_json='{}' WHERE seq=43").unwrap();
  drop(source);
  let cold=SqliteMirrorReview::load(&root).unwrap();
  assert!(cold.review_integrity_r152(NOW).is_err());
  assert!(cold.reconcile_partial_model_r180(&p,&f,NOW,2,OWNER,KEY,true).is_err());
  assert!(f.exists());
  cleanup(&root,&f);
 }
 #[test]fn r186_cursor_jump_cannot_be_autoreconciled(){
  let (root,f,db)=setup();
  let p=r156_r157_tests::projection(8);
  db.replay_with_local_floor_r172(&p,&f,NOW,2,OWNER,KEY,
    Some("after_first_batch")).unwrap_err();
  let destination=open(&db.db2,READWRITE_EXISTING).unwrap();
  exec(&destination,"UPDATE cursor SET seq=999 WHERE id=1").unwrap();
  drop(destination);
  let cold=SqliteMirrorReview::load(&root).unwrap();
  assert!(cold.review_integrity_r152(NOW).is_err());
  assert!(cold.reconcile_partial_model_r180(&p,&f,NOW,2,OWNER,KEY,true).is_err());
  cleanup(&root,&f);
 }
}
