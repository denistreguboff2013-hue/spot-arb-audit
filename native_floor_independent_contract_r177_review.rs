// R177/R179: an independent verifier contract, NOT an actual remote service.
// Caller MUST obtain the external receipt from a separate failure domain.
// Same-host signed files cannot disprove synchronized DB1+DB2+floor rollback.
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct IndependentReceiptR177{
 pub authority:String,pub owner:String,pub challenge:String,pub seq:u64,pub generation:String,
 pub anchor:u64,pub digest:String,pub mac:String,
}
fn independent_message_r177(r:&IndependentReceiptR177)->Vec<u8>{
 format!("ASTRA-R177-INDEPENDENT-v1\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n",
  r.authority,r.owner,r.challenge,r.seq,r.generation,r.anchor,r.digest).into_bytes()
}
pub fn verify_remote_receipt_r177(receipt:&IndependentReceiptR177,
  expected_owner:&str,expected_authority:&str,expected_challenge:&str,remote_verify_key:&[u8],
  local:&DetachedWitnessR154)->Result<(),PaperAdapterErrorR156>{
 if reject_identity_r154(expected_owner)||reject_identity_r154(expected_authority)
  ||!valid_hex_r156(expected_challenge)||receipt.challenge!=expected_challenge
  ||remote_verify_key.len()<16||receipt.owner!=expected_owner
  ||receipt.authority!=expected_authority||local.owner!=expected_owner
  ||receipt.seq!=local.seq||receipt.generation!=local.generation
  ||receipt.anchor!=local.anchor||receipt.digest!=local.digest
  ||receipt.mac.len()!=64{
  return Err(PaperAdapterErrorR156::Source)
 }
 let want=hex_r154(&hmac_r154(remote_verify_key,&independent_message_r177(receipt)));
 let diff=want.bytes().zip(receipt.mac.bytes()).fold(0u8,|v,(a,b)|v|(a^b));
 if diff!=0{return Err(PaperAdapterErrorR156::Source)}
 Ok(())
}

#[cfg(test)]
mod r177_independent_contract_tests{
 use super::*;
 const REMOTE:&[u8]=b"synthetic-independent-provider-only-test-key-r177";
 const CHALLENGE:&str="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
 fn witness(seq:u64)->DetachedWitnessR154{
  DetachedWitnessR154{owner:"r177-test-owner".into(),generation:"rebase-012".into(),
   anchor:42,seq,digest:"d".repeat(64),mac:"f".repeat(64)}
 }
 fn receipt(w:&DetachedWitnessR154)->IndependentReceiptR177{
  let mut r=IndependentReceiptR177{authority:"test-second-domain".into(),
   owner:w.owner.clone(),challenge:CHALLENGE.into(),seq:w.seq,
   generation:w.generation.clone(),anchor:w.anchor,digest:w.digest.clone(),mac:String::new()};
  r.mac=hex_r154(&hmac_r154(REMOTE,&independent_message_r177(&r)));
  r
 }
 #[test]fn r177_remote_challenge_and_signed_floor_identity_verified(){
  let w=witness(60);let r=receipt(&w);
  assert!(verify_remote_receipt_r177(&r,&w.owner,&r.authority,CHALLENGE,REMOTE,&w).is_ok());
  assert!(verify_remote_receipt_r177(&r,&w.owner,&r.authority,&"b".repeat(64),REMOTE,&w).is_err());
  let mut tamper=r.clone();tamper.digest="a".repeat(64);
  assert!(verify_remote_receipt_r177(&tamper,&w.owner,&r.authority,CHALLENGE,REMOTE,&w).is_err());
  assert!(verify_remote_receipt_r177(&r,&w.owner,"other-domain",CHALLENGE,REMOTE,&w).is_err());
 }
 #[test]fn r179_remote_new_floor_rejects_local_coordinated_rollback(){
  let old=witness(45);let new=witness(50);
  let remote=receipt(&new);
  assert!(verify_remote_receipt_r177(&remote,&new.owner,&remote.authority,CHALLENGE,
    REMOTE,&new).is_ok());
  assert!(verify_remote_receipt_r177(&remote,&old.owner,&remote.authority,CHALLENGE,
    REMOTE,&old).is_err());
 }
}
