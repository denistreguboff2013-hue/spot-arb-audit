// R187: bounded in-memory multi-session HANDOFF planning queue.
// Each item is a verified synthetic PAPER projection, never an exchange execution.
#[derive(Debug,Default)]
pub struct BoundedPaperQueueR187{
 queued:std::collections::VecDeque<PaperProjectionR156>,
 total_rows:usize,closed:bool,last_session:Option<String>,
}
impl BoundedPaperQueueR187{
 pub fn counts(&self)->(usize,usize){(self.queued.len(),self.total_rows)}
 pub fn enqueue(&mut self,next:PaperProjectionR156,now:u64)
  ->Result<(),PaperAdapterErrorR156>{
  verify_projection_r162(&next,now)?;
  if self.closed||self.queued.len()>=4||self.total_rows+next.rows.len()>256{
   return Err(PaperAdapterErrorR156::Limit)
  }
  if model_stage_r181(next.rows.first().unwrap())?!="SESSION_START"{
   return Err(PaperAdapterErrorR156::Model)
  }
  let session_order=|s:&str|->Option<(u64,u64)>{
   let fields:Vec<_>=s.split('_').collect();
   if fields.len()!=3||fields[0]!="session"{return None}
   Some((fields[1].parse().ok()?,fields[2].parse().ok()?))
  };
  let newer=session_order(&next.session).ok_or(PaperAdapterErrorR156::Model)?;
  if let Some(last)=&self.last_session{
   let prior=session_order(last).ok_or(PaperAdapterErrorR156::Model)?;
   if newer<=prior{return Err(PaperAdapterErrorR156::Source)}
  }
  if let Some(prior)=self.queued.back(){
   if model_stage_r181(prior.rows.last().unwrap())?!="SESSION_SEAL"{
    return Err(PaperAdapterErrorR156::Model)
   }
   if isolated_session_root_r174(&prior.session)?
     ==isolated_session_root_r174(&next.session)?{
    return Err(PaperAdapterErrorR156::Source)
   }
  }
  self.total_rows+=next.rows.len();
  self.last_session=Some(next.session.clone());
  self.queued.push_back(next);
  Ok(())
 }
 pub fn acknowledge_front(&mut self,expected_session:&str,
    floor:&DetachedWitnessR154,receipt:&IndependentReceiptR177,
    authority:&str,challenge:&str,remote_key:&[u8])
    ->Result<usize,PaperAdapterErrorR156>{
  let p=self.queued.front().ok_or(PaperAdapterErrorR156::Limit)?;
  let tip=p.rows.last().unwrap();
  if p.session!=expected_session||floor.seq!=tip.seq
    ||floor.generation!=tip.feed_generation||floor.anchor!=42
    ||model_stage_r181(tip)?!="SESSION_SEAL"{
   return Err(PaperAdapterErrorR156::Source)
  }
  verify_remote_receipt_r177(receipt,&floor.owner,authority,challenge,remote_key,floor)?;
  let count=p.rows.len();
  self.queued.pop_front();
  self.total_rows-=count;
  Ok(count)
 }
 pub fn forbid_new_sessions(&mut self){self.closed=true}
}
#[cfg(test)]
mod r187_tests{
 use super::*;
 const NOW:u64=1_792_280_000_000;
 const KEY:&[u8]=b"synthetic-multi-session-external-key-r187";
 const NONCE:&str="eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
 fn p(id:usize,sealed:bool)->PaperProjectionR156{
  r181_rollover_tests::project(&format!("session_{id}_1792253053"),sealed)
 }
 fn signed(p:&PaperProjectionR156)->(DetachedWitnessR154,IndependentReceiptR177){
  let tip=p.rows.last().unwrap();
  let f=DetachedWitnessR154{
   owner:"r187-research-owner".into(),generation:tip.feed_generation.clone(),
   anchor:42,seq:tip.seq,digest:"d".repeat(64),mac:"e".repeat(64)
  };
  let mut r=IndependentReceiptR177{
   authority:"research-independent-mock".into(),owner:f.owner.clone(),
   challenge:NONCE.into(),seq:f.seq,generation:f.generation.clone(),
   anchor:f.anchor,digest:f.digest.clone(),mac:String::new()
  };
  r.mac=hex_r154(&hmac_r154(KEY,&independent_message_r177(&r)));
  (f,r)
 }
 #[test]fn r187_four_sealed_sessions_bounded_fifo_and_replay_deny(){
  let mut q=BoundedPaperQueueR187::default();
  for n in 200..204{q.enqueue(p(n,true),NOW).unwrap()}
  assert_eq!(q.counts(),(4,16));
  assert_eq!(q.enqueue(p(204,true),NOW).err(),Some(PaperAdapterErrorR156::Limit));
  assert_eq!(q.enqueue(p(201,true),NOW).err(),Some(PaperAdapterErrorR156::Limit));
  let original=p(200,true);
  let (f,receipt)=signed(&original);
  assert!(q.acknowledge_front("session_201_1792253053",&f,&receipt,
    "research-independent-mock",NONCE,KEY).is_err());
  assert_eq!(q.acknowledge_front(&original.session,&f,&receipt,
    "research-independent-mock",NONCE,KEY).unwrap(),4);
  assert_eq!(q.counts(),(3,12));
  assert!(q.enqueue(p(200,true),NOW).is_err());
  q.enqueue(p(204,true),NOW).unwrap();
  q.forbid_new_sessions();
  assert!(q.enqueue(p(205,true),NOW).is_err());
 }
 #[test]fn r187_unsealed_predecessor_and_stale_attestation_block(){
  let mut q=BoundedPaperQueueR187::default();
  q.enqueue(p(200,false),NOW).unwrap();
  assert!(q.enqueue(p(201,true),NOW).is_err());
  assert_eq!(q.counts(),(1,4));
  let mut q=BoundedPaperQueueR187::default();
  let good=p(200,true);q.enqueue(good.clone(),NOW).unwrap();
  let (f,receipt)=signed(&good);
  assert!(q.acknowledge_front(&good.session,&f,&receipt,
    "research-independent-mock",&"f".repeat(64),KEY).is_err());
  assert_eq!(q.counts(),(1,4));
 }
}
