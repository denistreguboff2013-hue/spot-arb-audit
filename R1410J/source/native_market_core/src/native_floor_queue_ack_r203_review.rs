// R203: offline queue acknowledgement requires signed Ed25519 floor AND
// cold DB1/DB2 identity, not merely a caller-provided HMAC mock receipt.
impl SqliteMirrorReview{
 pub fn acknowledge_signed_paper_session_r203(
  &self,path:&Path,projection:&PaperProjectionR156,now:u64,
  queue_key:&[u8],local_witness_owner:&str,local_key:&[u8],
  remote:&SignedPaperFloorEnvelopeR199,witness_id:&str,
  trusted_remote_public_hex:&str,fresh_nonce:&str,
  operator_approved:bool)->Result<(),PaperAdapterErrorR156>{
  if !operator_approved{return Err(PaperAdapterErrorR156::Source)}
  verify_projection_r162(projection,now)?;
  if model_stage_r181(projection.rows.last().ok_or(PaperAdapterErrorR156::Source)?)?
     !="SESSION_SEAL"{
   return Err(PaperAdapterErrorR156::Source)
  }
  let last=projection.rows.last().unwrap();
  for row in &projection.rows{
   let stored=self.original_row(row.seq,now)
    .map_err(|_|PaperAdapterErrorR156::Source)?;
   if serde_json::to_vec(&stored).ok()!=serde_json::to_vec(row).ok(){
    return Err(PaperAdapterErrorR156::Source)
   }
  }
  let (seq,rows)=self.inspect_signed_cold_floor_r200(now,
   local_witness_owner,local_key,remote,witness_id,
   trusted_remote_public_hex,fresh_nonce)?;
  if seq!=last.seq||rows!=projection.rows.len()
    ||remote.body.generation!=last.feed_generation{
   return Err(PaperAdapterErrorR156::Source)
  }
  let _lock=queue_lock_r195(path)?;
  let mut q=load_queue_r195(path,queue_key)?;
  let front=q.sessions.first().ok_or(PaperAdapterErrorR156::Source)?;
  if front.session!=projection.session||front.events!=projection.rows.len()
   ||front.last_seq!=seq||front.generation!=last.feed_generation
   ||front.digest!=hex_r154(&projection.seal_r162){
   return Err(PaperAdapterErrorR156::Source)
  }
  q.revision=q.revision.checked_add(1).ok_or(PaperAdapterErrorR156::Limit)?;
  q.accepted_session=Some(front.session.clone());
  q.sessions.remove(0);
  save_queue_r195(path,q,queue_key)
 }
}
#[cfg(test)]
mod r203_tests{
 use super::*;
 use openssl::pkey::PKey;
 use std::{os::unix::fs::DirBuilderExt,sync::atomic::{AtomicUsize,Ordering}};
 static N:AtomicUsize=AtomicUsize::new(0);
 const NOW:u64=1_792_280_000_000;
 const QUEUE:&[u8]=b"r203-queue-local-test-key-00000000000";
 const WITNESS:&[u8]=b"r203-db2-local-test-key-000000000000";
 const OWNER:&str="r203-research-owner";
 const NONCE:&str="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
 fn setup()->(PathBuf,PathBuf,PathBuf,SqliteMirrorReview){
  let id=format!("{}_{}",std::process::id(),N.fetch_add(1,Ordering::Relaxed));
  let dbroot=std::env::temp_dir().join(format!("astra_r147_isolated_r203_{id}"));
  let qroot=std::env::temp_dir().join(format!("astra_r195_isolated_r203_{id}"));
  fs::DirBuilder::new().mode(0o700).create(&dbroot).unwrap();
  fs::DirBuilder::new().mode(0o700).create(&qroot).unwrap();
  let db=SqliteMirrorReview::initialize(&dbroot).unwrap();
  let q=qroot.join("paper-session-queue.json");
  initialize_queue_r195(&q,QUEUE).unwrap();
  (dbroot,qroot,q,db)
 }
 #[test]fn r203_cold_queue_ack_needs_exact_signed_db2_state(){
  let (root,qr,q,db)=setup();
  let p=r181_rollover_tests::project("session_200_1792253053",true);
  enqueue_private_queue_r195(&q,&p,NOW,QUEUE).unwrap();
  db.replay_modeled_paper_r157(&p,NOW,2).unwrap();
  let local=db.create_detached_witness_r154(NOW,OWNER,WITNESS).unwrap();
  let signer=PKey::generate_ed25519().unwrap();
  let pin=hex_r154(&signer.raw_public_key().unwrap());
  let signed=r200_tests::sign(&local,&signer);
  assert!(db.acknowledge_signed_paper_session_r203(&q,&p,NOW,QUEUE,OWNER,WITNESS,
    &signed,"separate-witness-test",&pin,NONCE,false).is_err());
  assert_eq!(private_queue_status_r195(&q,QUEUE).unwrap(),(1,4));
  db.acknowledge_signed_paper_session_r203(&q,&p,NOW,QUEUE,OWNER,WITNESS,
    &signed,"separate-witness-test",&pin,NONCE,true).unwrap();
  assert_eq!(private_queue_status_r195(&q,QUEUE).unwrap(),(0,0));
  assert!(db.acknowledge_signed_paper_session_r203(&q,&p,NOW,QUEUE,OWNER,WITNESS,
    &signed,"separate-witness-test",&pin,NONCE,true).is_err());
  fs::remove_dir_all(root).unwrap();fs::remove_dir_all(qr).unwrap();
 }
 #[test]fn r203_signed_wrong_floor_cannot_remove_queue_front(){
  let (root,qr,q,db)=setup();
  let p=r181_rollover_tests::project("session_200_1792253053",true);
  enqueue_private_queue_r195(&q,&p,NOW,QUEUE).unwrap();
  db.replay_modeled_paper_r157(&p,NOW,2).unwrap();
  let local=db.create_detached_witness_r154(NOW,OWNER,WITNESS).unwrap();
  let signer=PKey::generate_ed25519().unwrap();
  let pin=hex_r154(&signer.raw_public_key().unwrap());
  let mut signed=r200_tests::sign(&local,&signer);
  signed.body.seq-=1;
  assert!(db.acknowledge_signed_paper_session_r203(&q,&p,NOW,QUEUE,OWNER,WITNESS,
    &signed,"separate-witness-test",&pin,NONCE,true).is_err());
  assert_eq!(private_queue_status_r195(&q,QUEUE).unwrap(),(1,4));
  fs::remove_dir_all(root).unwrap();fs::remove_dir_all(qr).unwrap();
 }
}
