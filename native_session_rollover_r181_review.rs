// R181: bounded multi-session rollover PLAN ONLY. Source remains modeled PAPER.
// No DB1/DB2 production migration, no unbounded stream, no real trading authority.
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct PaperHandoffR181{
 pub previous:String,pub next:String,pub previous_seq:u64,
 pub next_source_seq:u64,pub previous_root:PathBuf,pub next_root:PathBuf,
 pub may_trade:bool,pub production_db2:bool,
}
fn model_stage_r181(row:&MirrorOutboxRowR139)->Result<String,PaperAdapterErrorR156>{
 let p:serde_json::Value=serde_json::from_str(&row.payload_json)
  .map_err(|_|PaperAdapterErrorR156::Model)?;
 p.get("operations").and_then(|x|x.as_array()).and_then(|a|a.first())
  .and_then(|e|e.get("model_stage")).and_then(|x|x.as_str())
  .map(str::to_owned).ok_or(PaperAdapterErrorR156::Model)
}
pub fn plan_sealed_paper_handoff_r181(prev:&PaperProjectionR156,
 next:&PaperProjectionR156,now:u64,
 confirmed:&DetachedWitnessR154,remote:&IndependentReceiptR177,
 authority:&str,challenge:&str,remote_key:&[u8])
 ->Result<PaperHandoffR181,PaperAdapterErrorR156>{
 verify_projection_r162(prev,now)?;verify_projection_r162(next,now)?;
 let last=prev.rows.last().ok_or(PaperAdapterErrorR156::Source)?;
 let first=next.rows.first().ok_or(PaperAdapterErrorR156::Source)?;
 if prev.session==next.session||last.seq!=confirmed.seq
  ||confirmed.generation!=last.feed_generation||first.seq!=43
  ||model_stage_r181(last)?!="SESSION_SEAL"
  ||model_stage_r181(first)?!="SESSION_START"{
  return Err(PaperAdapterErrorR156::Source)
 }
 verify_remote_receipt_r177(remote,&confirmed.owner,authority,challenge,remote_key,confirmed)?;
 let previous_root=isolated_session_root_r174(&prev.session)?;
 let next_root=isolated_session_root_r174(&next.session)?;
 if previous_root==next_root{return Err(PaperAdapterErrorR156::Source)}
 Ok(PaperHandoffR181{previous:prev.session.clone(),next:next.session.clone(),
  previous_seq:last.seq,next_source_seq:first.seq,previous_root,next_root,
  may_trade:false,production_db2:false})
}

#[cfg(test)]
mod r181_rollover_tests{
 use super::*;
 const NOW:u64=1_792_280_000_000;
 const KEY:&[u8]=b"synthetic-second-domain-rollover-test-key-181";
 const CHALLENGE:&str="eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
 pub(super) fn project(session:&str,seal:bool)->PaperProjectionR156{
  let (raw,_)=r156_r157_tests::session_journal_r159(session,4);
  let mut lines:Vec<serde_json::Value>=raw.split(|b|*b==b'\n')
   .filter(|x|!x.is_empty()).map(|l|serde_json::from_slice(l).unwrap()).collect();
  if seal{
   let mut body:crate::native_virtual_four_leg_provenance_review::ModelAuditPayload=
    serde_json::from_str(lines[3]["body"].as_str().unwrap()).unwrap();
   body.stage="SESSION_SEAL".into();body.base="".into();
   let payload=serde_json::to_string(&body).unwrap();
   let prev=lines[2]["checksum"].as_str().unwrap();
   let hash=model_hash_r156(4,prev,&payload);
   lines[3]["body"]=serde_json::json!(payload);
   lines[3]["checksum"]=serde_json::json!(hash);
  }
  let tip=lines[3]["checksum"].as_str().unwrap().to_string();
  let raw=format!("{}\n",lines.iter().map(|v|v.to_string()).collect::<Vec<_>>().join("\n"));
  model_projection_rows_r156(raw.as_bytes(),4,&tip,NOW).unwrap()
 }
 fn evidence(p:&PaperProjectionR156)->(DetachedWitnessR154,IndependentReceiptR177){
  let row=p.rows.last().unwrap();
  let w=DetachedWitnessR154{owner:"rollover-test-owner".into(),
   generation:row.feed_generation.clone(),anchor:42,seq:row.seq,
   digest:"c".repeat(64),mac:"a".repeat(64)};
  let mut r=IndependentReceiptR177{authority:"second-domain".into(),
   owner:w.owner.clone(),challenge:CHALLENGE.into(),seq:w.seq,
   generation:w.generation.clone(),anchor:w.anchor,digest:w.digest.clone(),mac:String::new()};
  r.mac=hex_r154(&hmac_r154(KEY,&independent_message_r177(&r)));
  (w,r)
 }
 #[test]fn r181_sealed_separate_sessions_have_distinct_safe_roots(){
  let old=project("session_200_1792253053",true);
  let next=project("session_201_1792253053",false);
  let (w,r)=evidence(&old);
  let plan=plan_sealed_paper_handoff_r181(&old,&next,NOW,&w,&r,
    "second-domain",CHALLENGE,KEY).unwrap();
  assert_ne!(plan.previous_root,plan.next_root);
  assert_eq!(plan.previous_seq,46);
  assert_eq!(plan.next_source_seq,43);
  assert!(!plan.may_trade&&!plan.production_db2);
 }
 #[test]fn r181_rejects_unsealed_source_and_wrong_remote_nonce(){
  let unsealed=project("session_200_1792253053",false);
  let good=project("session_200_1792253053",true);
  let next=project("session_201_1792253053",false);
  let (w,r)=evidence(&good);
  assert!(plan_sealed_paper_handoff_r181(&unsealed,&next,NOW,&w,&r,
    "second-domain",CHALLENGE,KEY).is_err());
  assert!(plan_sealed_paper_handoff_r181(&good,&next,NOW,&w,&r,
    "second-domain",&"f".repeat(64),KEY).is_err());
  assert!(plan_sealed_paper_handoff_r181(&good,&good,NOW,&w,&r,
    "second-domain",CHALLENGE,KEY).is_err());
 }
}
