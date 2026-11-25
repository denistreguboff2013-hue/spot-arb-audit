// R183-R184: synthetic external failure-domain attestor STATE MACHINE.
// No network, no storage, no production authority; key belongs to mock caller.
// Publishing a newer floor is monotonic, but a REAL remote verifier is still missing.
#[derive(Debug,Clone,Copy,PartialEq,Eq)]
pub enum AttestorErrorR183{Unavailable,Identity,Replay,Rollback,Conflict,Missing}
#[derive(Debug,Clone)]
pub struct AttestorMockR183{
 authority:String,owner:String,key:Vec<u8>,online:bool,
 highest:Option<DetachedWitnessR154>,
 challenge:Option<String>,used:Vec<String>,
}
impl AttestorMockR183{
 pub fn synthetic_only(authority:&str,owner:&str,key:&[u8])->Result<Self,AttestorErrorR183>{
  if reject_identity_r154(authority)||reject_identity_r154(owner)||key.len()<16{
   return Err(AttestorErrorR183::Identity)
  }
  Ok(Self{authority:authority.into(),owner:owner.into(),key:key.to_vec(),
   online:true,highest:None,challenge:None,used:vec![]})
 }
 pub fn set_online_mock(&mut self,online:bool){self.online=online}
 pub fn issue_fresh_challenge(&mut self,challenge:&str)->Result<(),AttestorErrorR183>{
  if !self.online{return Err(AttestorErrorR183::Unavailable)}
  if !valid_hex_r156(challenge)||self.challenge.is_some()
    ||self.used.iter().any(|n|n==challenge){
   return Err(AttestorErrorR183::Replay)
  }
  self.challenge=Some(challenge.into());Ok(())
 }
 pub fn publish_monotonic_mock(&mut self,next:DetachedWitnessR154)
  ->Result<(),AttestorErrorR183>{
  if !self.online{return Err(AttestorErrorR183::Unavailable)}
  if next.owner!=self.owner||!valid_hex_r156(&next.digest)||next.seq==0{
   return Err(AttestorErrorR183::Identity)
  }
  if let Some(old)=&self.highest{
   if old.generation!=next.generation||old.anchor!=next.anchor{
    return Err(AttestorErrorR183::Conflict)
   }
   if next.seq<old.seq{return Err(AttestorErrorR183::Rollback)}
   if next.seq==old.seq{
    return if next.digest==old.digest{Ok(())}else{Err(AttestorErrorR183::Conflict)}
   }
  }
  self.highest=Some(next);Ok(())
 }
 pub fn answer_challenge_mock(&mut self)->Result<IndependentReceiptR177,AttestorErrorR183>{
  if !self.online{return Err(AttestorErrorR183::Unavailable)}
  let challenge=self.challenge.take().ok_or(AttestorErrorR183::Missing)?;
  let current=self.highest.as_ref().ok_or(AttestorErrorR183::Missing)?;
  if self.used.len()>=128{return Err(AttestorErrorR183::Replay)}
  self.used.push(challenge.clone());
  let mut r=IndependentReceiptR177{
   authority:self.authority.clone(),owner:self.owner.clone(),
   challenge,seq:current.seq,generation:current.generation.clone(),
   anchor:current.anchor,digest:current.digest.clone(),mac:String::new()
  };
  r.mac=hex_r154(&hmac_r154(&self.key,&independent_message_r177(&r)));
  Ok(r)
 }
}
pub fn attestor_fail_closed_r184(
 receipt:Result<IndependentReceiptR177,AttestorErrorR183>,
 local:&DetachedWitnessR154,authority:&str,challenge:&str,remote_key:&[u8]
)->Result<(),PaperAdapterErrorR156>{
 let proof=receipt.map_err(|_|PaperAdapterErrorR156::Source)?;
 verify_remote_receipt_r177(&proof,&local.owner,authority,challenge,remote_key,local)
}
#[cfg(test)]
mod r183_r184_tests{
 use super::*;
 const KEY:&[u8]=b"synthetic-independent-authority-test-only-183";
 const A:&str="second-domain";
 const O:&str="research-owner";
 const N1:&str="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
 const N2:&str="bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
 fn floor(seq:u64)->DetachedWitnessR154{
  DetachedWitnessR154{owner:O.into(),generation:"rebase-test123".into(),
    anchor:42,seq,digest:format!("{seq:064x}"),mac:"a".repeat(64)}
 }
 #[test]fn r183_attestor_enforces_monotonic_external_floor(){
  let mut server=AttestorMockR183::synthetic_only(A,O,KEY).unwrap();
  server.publish_monotonic_mock(floor(43)).unwrap();
  server.publish_monotonic_mock(floor(50)).unwrap();
  assert_eq!(server.publish_monotonic_mock(floor(44)),
    Err(AttestorErrorR183::Rollback));
  let mut conflicting=floor(50);conflicting.digest="c".repeat(64);
  assert_eq!(server.publish_monotonic_mock(conflicting),
    Err(AttestorErrorR183::Conflict));
  server.issue_fresh_challenge(N1).unwrap();
  let receipt=server.answer_challenge_mock().unwrap();
  assert_eq!(receipt.seq,50);
  assert!(attestor_fail_closed_r184(Ok(receipt),&floor(43),A,N1,KEY).is_err());
 }
 #[test]fn r184_offline_partition_and_nonce_replay_deny(){
  let mut server=AttestorMockR183::synthetic_only(A,O,KEY).unwrap();
  server.publish_monotonic_mock(floor(60)).unwrap();
  server.set_online_mock(false);
  assert_eq!(server.issue_fresh_challenge(N1),Err(AttestorErrorR183::Unavailable));
  assert!(attestor_fail_closed_r184(
    server.answer_challenge_mock(),&floor(60),A,N1,KEY).is_err());
  server.set_online_mock(true);
  server.issue_fresh_challenge(N1).unwrap();
  let proof=server.answer_challenge_mock().unwrap();
  assert!(attestor_fail_closed_r184(Ok(proof.clone()),&floor(60),A,N1,KEY).is_ok());
  assert_eq!(server.issue_fresh_challenge(N1),Err(AttestorErrorR183::Replay));
  assert!(attestor_fail_closed_r184(Ok(proof),&floor(60),A,N2,KEY).is_err());
 }
}
