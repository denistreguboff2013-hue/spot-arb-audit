// R201: compare local persisted PAPER queue against a separately signed remote anchor.
// Local HMAC alone cannot prevent whole-machine queue file rollback.
#[derive(Debug,Clone,serde::Serialize,serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignedQueueTipR201{
 pub schema:u32,pub witness_id:String,pub challenge:String,
 pub queue_revision:u64,pub local_manifest_sha256:String,
}
#[derive(Debug,Clone,serde::Serialize,serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignedQueueEnvelopeR201{
 pub tip:SignedQueueTipR201,pub ed25519_signature_hex:String,
}
fn queue_tip_bytes_r201(t:&SignedQueueTipR201)
 ->Result<Vec<u8>,PaperAdapterErrorR156>{
 if t.schema!=1||reject_identity_r154(&t.witness_id)
  ||t.challenge.len()!=64||!valid_hex_r156(&t.challenge)
  ||t.local_manifest_sha256.len()!=64||!valid_hex_r156(&t.local_manifest_sha256){
  return Err(PaperAdapterErrorR156::Source)
 }
 let mut b=b"SPOTARB/QUEUE/REMOTE-SIGNED-ANCHOR/R201/V1\n".to_vec();
 b.extend(serde_json::to_vec(t).map_err(|_|PaperAdapterErrorR156::Source)?);
 Ok(b)
}
fn queue_current_sha256_r201(m:&QueueManifestR195)
 ->Result<String,PaperAdapterErrorR156>{
 use openssl::sha::sha256;
 Ok(hex_r154(&sha256(&serde_json::to_vec(m)
  .map_err(|_|PaperAdapterErrorR156::Source)?)))
}
pub fn verify_queue_against_signed_anchor_r201(
 path:&Path,local_key:&[u8],signed:&SignedQueueEnvelopeR201,
 trusted_witness_id:&str,pinned_public_hex:&str,fresh_challenge:&str
)->Result<u64,PaperAdapterErrorR156>{
 use openssl::{pkey::{Id,PKey},sign::Verifier};
 let m=load_queue_r195(path,local_key)?;
 let tip=&signed.tip;
 let payload=queue_tip_bytes_r201(tip)?;
 if tip.witness_id!=trusted_witness_id
  ||tip.challenge!=fresh_challenge||m.revision!=tip.queue_revision
  ||queue_current_sha256_r201(&m)?!=tip.local_manifest_sha256{
  return Err(PaperAdapterErrorR156::Source)
 }
 let key=decode_hex_r199(pinned_public_hex,32)?;
 let signature=decode_hex_r199(&signed.ed25519_signature_hex,64)?;
 let pk=PKey::public_key_from_raw_bytes(&key,Id::ED25519)
  .map_err(|_|PaperAdapterErrorR156::Source)?;
 let mut verifier=Verifier::new_without_digest(&pk)
  .map_err(|_|PaperAdapterErrorR156::Source)?;
 if !verifier.verify_oneshot(&signature,&payload)
  .map_err(|_|PaperAdapterErrorR156::Source)?{
  return Err(PaperAdapterErrorR156::Source)
 }
 Ok(m.revision)
}
#[cfg(test)]
mod r201_tests{
 use super::*;
 use openssl::{pkey::PKey,sign::Signer};
 use std::{os::unix::fs::DirBuilderExt,sync::atomic::{AtomicUsize,Ordering}};
 static N:AtomicUsize=AtomicUsize::new(0);
 const NOW:u64=1_792_280_000_000;
 const KEY:&[u8]=b"r201-private-test-queue-signing-key-only-00";
 const NONCE:&str="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
 fn setup()->(PathBuf,PathBuf){
  let r=std::env::temp_dir().join(format!("astra_r195_isolated_r201_{}_{}",
   std::process::id(),N.fetch_add(1,Ordering::Relaxed)));
  fs::DirBuilder::new().mode(0o700).create(&r).unwrap();
  let path=r.join("paper-session-queue.json");
  initialize_queue_r195(&path,KEY).unwrap();
  (r,path)
 }
 fn signed(path:&Path,k:&PKey<openssl::pkey::Private>)->SignedQueueEnvelopeR201{
  let m=load_queue_r195(path,KEY).unwrap();
  let tip=SignedQueueTipR201{schema:1,witness_id:"remote-queue-witness".into(),
   challenge:NONCE.into(),queue_revision:m.revision,
   local_manifest_sha256:queue_current_sha256_r201(&m).unwrap()};
  let signature=Signer::new_without_digest(k).unwrap()
   .sign_oneshot_to_vec(&queue_tip_bytes_r201(&tip).unwrap()).unwrap();
  SignedQueueEnvelopeR201{tip,ed25519_signature_hex:hex_r154(&signature)}
 }
 #[test]fn r201_whole_file_rollback_rejected_by_newer_remote_anchor(){
  let (root,path)=setup();let k=PKey::generate_ed25519().unwrap();
  let pubkey=hex_r154(&k.raw_public_key().unwrap());
  let old=fs::read(&path).unwrap();
  enqueue_private_queue_r195(&path,
   &r181_rollover_tests::project("session_200_1792253053",true),NOW,KEY).unwrap();
  let proof=signed(&path,&k);
  assert_eq!(verify_queue_against_signed_anchor_r201(&path,KEY,
   &proof,"remote-queue-witness",&pubkey,NONCE).unwrap(),1);
  fs::write(&path,old).unwrap(); // isolated only: coordinated local rollback
  assert!(load_queue_r195(&path,KEY).is_ok(),"local HMAC still valid");
  assert!(verify_queue_against_signed_anchor_r201(&path,KEY,
   &proof,"remote-queue-witness",&pubkey,NONCE).is_err());
  fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r201_signed_anchor_replay_wrong_nonce_and_bad_signature_denied(){
  let (root,path)=setup();let k=PKey::generate_ed25519().unwrap();
  let pubkey=hex_r154(&k.raw_public_key().unwrap());
  let proof=signed(&path,&k);
  assert!(verify_queue_against_signed_anchor_r201(&path,KEY,
   &proof,"remote-queue-witness",&pubkey,&"b".repeat(64)).is_err());
  let mut forged=proof;forged.ed25519_signature_hex.replace_range(0..2,"ff");
  assert!(verify_queue_against_signed_anchor_r201(&path,KEY,
   &forged,"remote-queue-witness",&pubkey,NONCE).is_err());
  fs::remove_dir_all(root).unwrap();
 }
}
