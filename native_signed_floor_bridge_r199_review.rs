// R199: ED25519 verification bridge for isolated PAPER DB2 floor.
// Reuses crate's established OpenSSL Ed25519 verifier pattern and stable JSON.
// A valid signature DOES NOT establish an off-host latest read or real exchange fills.
#[derive(Clone,Debug,serde::Serialize,serde::Deserialize,PartialEq,Eq)]
#[serde(deny_unknown_fields)]
pub struct SignedPaperFloorR199{
 pub version:u32,pub domain:String,pub witness_id:String,
 pub owner:String,pub challenge:String,pub generation:String,
 pub anchor:u64,pub seq:u64,pub digest:String,
}
#[derive(Clone,Debug,serde::Serialize,serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignedPaperFloorEnvelopeR199{
 pub body:SignedPaperFloorR199,pub signature_hex:String,
}
pub const PAPER_FLOOR_DOMAIN_R199:&str="SPOTARB/PAPER-DB2/ED25519/FLOOR/R199/V1";
fn floor_message_r199(p:&SignedPaperFloorR199)
 ->Result<Vec<u8>,PaperAdapterErrorR156>{
 if p.version!=1||p.domain!=PAPER_FLOOR_DOMAIN_R199
  ||reject_identity_r154(&p.witness_id)||reject_identity_r154(&p.owner)
  ||!valid_hex_r156(&p.challenge)||p.challenge.len()!=64
  ||!valid_hex_r156(&p.digest)||p.digest.len()!=64
  ||p.seq==0||p.anchor==0||p.generation.is_empty(){
  return Err(PaperAdapterErrorR156::Source)
 }
 serde_json::to_vec(p).map_err(|_|PaperAdapterErrorR156::Source)
}
pub fn verify_floor_ed25519_r199(
 signed:&SignedPaperFloorEnvelopeR199,
 trusted_id:&str,trusted_public_key_hex:&str,fresh_challenge:&str,
 local:&DetachedWitnessR154
)->Result<(),PaperAdapterErrorR156>{
 use openssl::{pkey::{Id,PKey},sign::Verifier};
 let p=&signed.body;
 let message=floor_message_r199(p)?;
 if p.witness_id!=trusted_id||p.owner!=local.owner
  ||p.generation!=local.generation||p.seq!=local.seq
  ||p.anchor!=local.anchor||p.digest!=local.digest
  ||p.challenge!=fresh_challenge
  ||trusted_public_key_hex.len()!=64||signed.signature_hex.len()!=128{
  return Err(PaperAdapterErrorR156::Source)
 }
 let pk=decode_hex_r199(trusted_public_key_hex,32)?;
 let signature=decode_hex_r199(&signed.signature_hex,64)?;
 let key=PKey::public_key_from_raw_bytes(&pk,Id::ED25519)
  .map_err(|_|PaperAdapterErrorR156::Source)?;
 let mut checker=Verifier::new_without_digest(&key)
  .map_err(|_|PaperAdapterErrorR156::Source)?;
 if !checker.verify_oneshot(&signature,&message)
  .map_err(|_|PaperAdapterErrorR156::Source)?{
  return Err(PaperAdapterErrorR156::Source)
 }
 Ok(())
}
fn decode_hex_r199(s:&str,byte_count:usize)
 ->Result<Vec<u8>,PaperAdapterErrorR156>{
 if s.len()!=byte_count*2||!s.bytes().all(|b|b.is_ascii_hexdigit()
  &&!b.is_ascii_uppercase()){
  return Err(PaperAdapterErrorR156::Source)
 }
 s.as_bytes().chunks_exact(2).map(|pair|{
  let p=std::str::from_utf8(pair).map_err(|_|PaperAdapterErrorR156::Source)?;
  u8::from_str_radix(p,16).map_err(|_|PaperAdapterErrorR156::Source)
 }).collect()
}
#[cfg(test)]
mod r199_tests{
 use super::*;
 use openssl::{pkey::PKey,sign::Signer};
 const NONCE:&str="abababababababababababababababababababababababababababababababab";
 fn floor(seq:u64)->DetachedWitnessR154{
  DetachedWitnessR154{owner:"isolated_owner_r199".into(),
   generation:"rebase-test".into(),anchor:42,seq,
   digest:"b".repeat(64),mac:"c".repeat(64)}
 }
 fn mk(r:&DetachedWitnessR154,key:&PKey<openssl::pkey::Private>)
  ->SignedPaperFloorEnvelopeR199{
  let body=SignedPaperFloorR199{
   version:1,domain:PAPER_FLOOR_DOMAIN_R199.into(),
   witness_id:"separate-witness-test".into(),
   owner:r.owner.clone(),challenge:NONCE.into(),generation:r.generation.clone(),
   anchor:r.anchor,seq:r.seq,digest:r.digest.clone(),
  };
  let mut s=Signer::new_without_digest(key).unwrap();
  let mac=s.sign_oneshot_to_vec(&floor_message_r199(&body).unwrap()).unwrap();
  SignedPaperFloorEnvelopeR199{body,signature_hex:hex_r154(&mac)}
 }
 #[test]fn r199_valid_signature_and_fresh_scope_only(){
  let key=PKey::generate_ed25519().unwrap();
  let public=hex_r154(&key.raw_public_key().unwrap());
  let w=floor(50);let envelope=mk(&w,&key);
  assert!(verify_floor_ed25519_r199(
   &envelope,"separate-witness-test",&public,NONCE,&w).is_ok());
  assert!(verify_floor_ed25519_r199(
   &envelope,"separate-witness-test",&public,&"d".repeat(64),&w).is_err());
  assert!(verify_floor_ed25519_r199(
   &envelope,"different-id",&public,NONCE,&w).is_err());
 }
 #[test]fn r199_tamper_wrong_key_and_rollback_rejected(){
  let signer=PKey::generate_ed25519().unwrap();
  let other=PKey::generate_ed25519().unwrap();
  let public=hex_r154(&signer.raw_public_key().unwrap());
  let wrong=hex_r154(&other.raw_public_key().unwrap());
  let w=floor(60);
  let signed=mk(&w,&signer);
  assert!(verify_floor_ed25519_r199(
   &signed,"separate-witness-test",&wrong,NONCE,&w).is_err());
  let mut modified=signed.clone();modified.body.seq=59;
  assert!(verify_floor_ed25519_r199(
   &modified,"separate-witness-test",&public,NONCE,&w).is_err());
  let old=floor(59);
  assert!(verify_floor_ed25519_r199(
   &signed,"separate-witness-test",&public,NONCE,&old).is_err());
  modified=signed;modified.signature_hex.replace_range(0..2,"ff");
  assert!(verify_floor_ed25519_r199(
   &modified,"separate-witness-test",&public,NONCE,&w).is_err());
 }
}
