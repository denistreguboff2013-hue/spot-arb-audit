// R200: strict cold DB2 comparison with caller-supplied signed remote receipt.
// The external public key pin and nonce must come from a trusted separate source.
// This bridge never claims that signature implies latest remote durability.
impl SqliteMirrorReview{
 pub fn inspect_signed_cold_floor_r200(&self,now:u64,owner:&str,
  local_key:&[u8],signed:&SignedPaperFloorEnvelopeR199,
  witness_id:&str,trusted_public_key_hex:&str,nonce:&str)
  ->Result<(u64,usize),PaperAdapterErrorR156>{
  let (seq,n)=self.review_integrity_r152(now)
   .map_err(|_|PaperAdapterErrorR156::Mirror)?;
  if seq==0||n==0{return Err(PaperAdapterErrorR156::Source)}
  let local=self.create_detached_witness_r154(now,owner,local_key)
   .map_err(|_|PaperAdapterErrorR156::Mirror)?;
  if local.seq!=seq{return Err(PaperAdapterErrorR156::Mirror)}
  verify_floor_ed25519_r199(signed,witness_id,trusted_public_key_hex,nonce,&local)?;
  Ok((seq,n))
 }
}
#[cfg(test)]
mod r200_tests{
 use super::*;
 use openssl::{pkey::PKey,sign::Signer};
 use std::{os::unix::fs::DirBuilderExt,sync::atomic::{AtomicUsize,Ordering}};
 static N:AtomicUsize=AtomicUsize::new(0);
 const NOW:u64=1_792_280_000_000;
 const KEY:&[u8]=b"isolated-r200-key-not-for-production-011";
 const OWNER:&str="r200-test-owner";
 const NONCE:&str="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
 fn root()->(PathBuf,SqliteMirrorReview){
  let r=std::env::temp_dir().join(format!("astra_r147_isolated_r200_{}_{}",
   std::process::id(),N.fetch_add(1,Ordering::Relaxed)));
  fs::DirBuilder::new().mode(0o700).create(&r).unwrap();
  let db=SqliteMirrorReview::initialize(&r).unwrap();
  (r,db)
 }
 pub(super) fn sign(w:&DetachedWitnessR154,k:&PKey<openssl::pkey::Private>)
  ->SignedPaperFloorEnvelopeR199{
  let body=SignedPaperFloorR199{
   version:1,domain:PAPER_FLOOR_DOMAIN_R199.into(),witness_id:"separate-witness-test".into(),
   owner:w.owner.clone(),challenge:NONCE.into(),generation:w.generation.clone(),
   anchor:w.anchor,seq:w.seq,digest:w.digest.clone()};
  let signed=Signer::new_without_digest(k).unwrap()
   .sign_oneshot_to_vec(&floor_message_r199(&body).unwrap()).unwrap();
  SignedPaperFloorEnvelopeR199{body,signature_hex:hex_r154(&signed)}
 }
 #[test]fn r200_cold_db2_matches_signed_remote_and_detects_local_rollback(){
  let (root,db)=root();
  let p=r156_r157_tests::projection(3);
  db.replay_modeled_paper_r157(&p,NOW,2).unwrap();
  let witness=db.create_detached_witness_r154(NOW,OWNER,KEY).unwrap();
  let signer=PKey::generate_ed25519().unwrap();
  let pin=hex_r154(&signer.raw_public_key().unwrap());
  let proof=sign(&witness,&signer);
  assert_eq!(db.inspect_signed_cold_floor_r200(NOW,OWNER,KEY,
    &proof,"separate-witness-test",&pin,NONCE).unwrap(),(45,3));
  let more=r156_r157_tests::projection(5);
  db.replay_modeled_paper_r157(&more,NOW,2).unwrap();
  assert!(db.inspect_signed_cold_floor_r200(NOW,OWNER,KEY,
    &proof,"separate-witness-test",&pin,NONCE).is_err());
  fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r200_empty_db_and_wrong_public_pin_rejected(){
  let (root,db)=root();
  let signer=PKey::generate_ed25519().unwrap();
  let fake=DetachedWitnessR154{owner:OWNER.into(),generation:"rebase-test".into(),
   anchor:42,seq:45,digest:"b".repeat(64),mac:"c".repeat(64)};
  let proof=sign(&fake,&signer);
  let pin=hex_r154(&signer.raw_public_key().unwrap());
  assert!(db.inspect_signed_cold_floor_r200(NOW,OWNER,KEY,
    &proof,"separate-witness-test",&pin,NONCE).is_err());
  fs::remove_dir_all(root).unwrap();
 }
}
