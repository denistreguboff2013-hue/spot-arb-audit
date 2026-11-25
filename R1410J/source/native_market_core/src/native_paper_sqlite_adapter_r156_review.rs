// Astra R156-R157: bounded READ-ONLY modeled PAPER journal -> PRIVATE SQLite research replay.
// Never promotes modeled records to real fills or trading permission.
#[derive(Debug,Clone,Copy,PartialEq,Eq)]
pub enum PaperAdapterErrorR156{Path,Limit,Journal,Model,Source,Mirror}
#[derive(Debug,serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PaperLineR156{seq:u64,previous_hash:String,body:String,checksum:String}
#[derive(Debug,Clone)]
pub struct PaperProjectionR156 {
 pub session:String,pub original_seq:u64,pub journal_hash:String,
 pub rows:Vec<MirrorOutboxRowR139>,pub may_trade:bool,pub actual_fills:bool,
 seal_r162:[u8;32],
}
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct PaperReplayR157{
 pub examined:usize,pub newly_applied:usize,pub already_applied:usize,
 pub final_seq:u64,pub production_db2:bool,pub may_trade:bool,
}
fn hex_paper_r156(raw:&[u8])->String{
 raw.iter().map(|b|format!("{b:02x}")).collect()
}
fn valid_hex_r156(x:&str)->bool{
 x.len()==64&&x.bytes().all(|b|b.is_ascii_digit()||(b'a'..=b'f').contains(&b))
}
fn model_hash_r156(seq:u64,prev:&str,body:&str)->String{
 let v=serde_json::to_vec(&(1u32,seq,prev,body)).expect("primitive tuple");
 hex_paper_r156(&openssl::sha::sha256(&v))
}
// Local in-process tamper guard, NOT an external anti-rollback witness.
fn paper_projection_digest_r162(p:&PaperProjectionR156)->Result<[u8;32],PaperAdapterErrorR156>{
 let v=serde_json::to_vec(&(&p.session,p.original_seq,&p.journal_hash,
  &p.rows,p.may_trade,p.actual_fills))
  .map_err(|_|PaperAdapterErrorR156::Source)?;
 Ok(openssl::sha::sha256(&v))
}
fn verify_projection_r162(p:&PaperProjectionR156,now:u64)
 ->Result<(),PaperAdapterErrorR156>{
 if p.may_trade||p.actual_fills||p.rows.is_empty()||p.rows.len()>256
    ||p.original_seq!=p.rows.len() as u64||!valid_hex_r156(&p.journal_hash)
    ||p.seal_r162!=paper_projection_digest_r162(p)?{
  return Err(PaperAdapterErrorR156::Source)
 }
 audit_outbox_chain(&p.rows,42,now).map_err(|_|PaperAdapterErrorR156::Source)?;
 let feed=format!("rebase-{}",hex_paper_r156(&openssl::sha::sha256(p.session.as_bytes())));
 if p.rows.iter().any(|r|r.feed_generation!=feed||r.tx_kind!="paper_model_snapshot"){
  return Err(PaperAdapterErrorR156::Source)
 }
 Ok(())
}
fn model_projection_rows_r156(raw:&[u8],tip_seq:u64,tip_hash:&str,now:u64)
 ->Result<PaperProjectionR156,PaperAdapterErrorR156>{
 if raw.len()>8*1024*1024||raw.is_empty()||raw.last()!=Some(&b'\n')
    ||!valid_hex_r156(tip_hash){return Err(PaperAdapterErrorR156::Limit)}
 let mut rows=Vec::new();
 let mut prev="0".repeat(64);
 let mut session=String::new();
 let mut start_ms=0u64;
 let mut origin_seq=0;
 for line in raw.split(|c|*c==b'\n').filter(|l|!l.is_empty()){
  if rows.len()>=256{return Err(PaperAdapterErrorR156::Limit)}
  let record:PaperLineR156=serde_json::from_slice(line)
    .map_err(|_|PaperAdapterErrorR156::Journal)?;
  let next=rows.len() as u64+1;
  if record.seq!=next||record.previous_hash!=prev
    ||record.checksum!=model_hash_r156(record.seq,&prev,&record.body){
   return Err(PaperAdapterErrorR156::Journal)
  }
  let body:crate::native_virtual_four_leg_provenance_review::ModelAuditPayload=
   serde_json::from_str(&record.body).map_err(|_|PaperAdapterErrorR156::Model)?;
  if body.schema!="SPOTARB/PUBLIC-PAPER/MODELED-FOUR-LEG/V1"
    ||body.source!="PUBLIC_DEPTH_MODEL_ONLY"
    ||body.exchange_fills_attested||body.real_orders_authorized
    ||body.native_owner_verified_closed
    ||!matches!(body.stage.as_str(),
     "SESSION_START"|"SPOT_BUY_MODELED"|"FUTURES_SHORT_MODELED"
       |"FUTURES_COVER_MODELED"|"SPOT_SELL_MODELED"|"SESSION_SEAL"){
    return Err(PaperAdapterErrorR156::Model)
  }
  if session.is_empty(){
   session=body.session.clone();
   let ts=session.rsplit('_').next().and_then(|s|s.parse::<u64>().ok())
     .ok_or(PaperAdapterErrorR156::Model)?;
   start_ms=ts.checked_mul(1000).ok_or(PaperAdapterErrorR156::Model)?;
   if start_ms==0||start_ms>now.saturating_add(5_000){
    return Err(PaperAdapterErrorR156::Model)
   }
  }
  if session!=body.session{return Err(PaperAdapterErrorR156::Model)}
  let seq=42_u64.checked_add(record.seq).ok_or(PaperAdapterErrorR156::Limit)?;
  let prev_seq=seq-1;
  let payload=serde_json::json!({
   "protocol_version":1,
   "schema_generation":1,
   "tx_kind":"paper_model_snapshot",
   "operations":[{
    "op":"upsert","table":"events",
    "source":"PUBLIC_PAPER_MODELED_ONLY",
    "session":session,"model_event_seq":record.seq,
    "model_stage":body.stage,
    "model_hash":record.checksum,
    "model_body_json":record.body
   }]
  });
  let payload_json=serde_json::to_string(&payload)
    .map_err(|_|PaperAdapterErrorR156::Source)?;
  let id_material=format!("SPOTARB-PAPER-R156|{}|{}|{}",
     session,record.seq,record.checksum);
  let tx_digest=hex_paper_r156(&openssl::sha::sha256(id_material.as_bytes()));
  rows.push(MirrorOutboxRowR139{
   seq,prev_seq,tx_id:format!("tx-{}",&tx_digest[..32]),
   tx_kind:"paper_model_snapshot".to_string(),schema_generation:1,
   payload_hash:hex_paper_r156(&openssl::sha::sha256(payload_json.as_bytes())),
   payload_bytes:payload_json.len() as u64+384,
   payload_json,committed_at_ms:start_ms,
   feed_generation:format!("rebase-{}",
    hex_paper_r156(&openssl::sha::sha256(session.as_bytes()))),
  });
  prev=record.checksum;
  origin_seq=record.seq;
 }
 if rows.is_empty()||origin_seq!=tip_seq||prev!=tip_hash{
  return Err(PaperAdapterErrorR156::Journal)
 }
 audit_outbox_chain(&rows,42,now).map_err(|_|PaperAdapterErrorR156::Source)?;
 let mut p=PaperProjectionR156{
  session,original_seq:origin_seq,journal_hash:prev,rows,
  may_trade:false,actual_fills:false,seal_r162:[0;32],
 };
 p.seal_r162=paper_projection_digest_r162(&p)?;
 Ok(p)
}
pub fn project_verified_paper_journal_r156(
 path:&Path,now:u64)->Result<PaperProjectionR156,PaperAdapterErrorR156>{
 if path.file_name().and_then(|s|s.to_str())!=Some("modeled_four_leg_journal.jsonl"){
  return Err(PaperAdapterErrorR156::Path)
 }
 // R158: original stage/PnL validator and projected rows use identical stable bytes.
 let (raw,tip)=snapshot_paper_source_r158(path)?;
 let p=model_projection_rows_r156(&raw,tip.sequence,&tip.last_hash,now)?;
 if p.session!=tip.session{return Err(PaperAdapterErrorR156::Journal)}
 Ok(p)
}
impl SqliteMirrorReview{
 pub fn replay_modeled_paper_r157(&self,projection:&PaperProjectionR156,
  now:u64,batch_size:usize)->Result<PaperReplayR157,PaperAdapterErrorR156>{
  if projection.may_trade||projection.actual_fills||projection.rows.is_empty()
    ||projection.rows.len()>256||batch_size==0||batch_size>16{
   return Err(PaperAdapterErrorR156::Limit)
  }
  verify_projection_r162(projection,now)?;
  let mut added=0;let mut already=0;
  for chunk in projection.rows.chunks(batch_size){
   // R196: append only the missing SUFFIX of DB1 rows atomically.
   // A missing-prefix/present-suffix anomaly must never be silently repaired.
   let mut missing=Vec::new();
   for row in chunk{
    match self.original_row(row.seq,now){
     Ok(old)=>{
      if !missing.is_empty()||serde_json::to_vec(&old).ok()
        !=serde_json::to_vec(row).ok(){
       return Err(PaperAdapterErrorR156::Source)
      }
     },
     Err(MirrorDbError::Source)=>missing.push(row.clone()),
     Err(_)=>return Err(PaperAdapterErrorR156::Source),
    }
   }
   if !missing.is_empty(){
    self.append_source_batch_r196(&missing,now,false)
      .map_err(|_|PaperAdapterErrorR156::Source)?;
   }
   let outcome=self.apply_batch_r160(chunk,now)
     .map_err(|_|PaperAdapterErrorR156::Mirror)?;
   added+=outcome.applied;already+=outcome.skipped;
  }
  let (seq,_) = self.review_integrity_r152(now)
   .map_err(|_|PaperAdapterErrorR156::Mirror)?;
  Ok(PaperReplayR157{
   examined:projection.rows.len(),newly_applied:added,
   already_applied:already,final_seq:seq,production_db2:false,may_trade:false,
  })
 }
}

#[cfg(test)]
mod r156_r157_tests{
 use super::*;
 use std::{os::unix::fs::DirBuilderExt,sync::atomic::{AtomicUsize,Ordering}};
 static N:AtomicUsize=AtomicUsize::new(0);
 const NOW:u64=1_792_280_000_000;
 pub(super) fn journal(n:usize)->(Vec<u8>,String){
  let mut lines=Vec::new();
  let mut prev="0".repeat(64);
  for i in 1..=n{
   let payload=crate::native_virtual_four_leg_provenance_review::ModelAuditPayload{
    schema:"SPOTARB/PUBLIC-PAPER/MODELED-FOUR-LEG/V1".into(),
    session:"session_200_1792253053".into(),
    stage:if i==1{"SESSION_START"}else{"SPOT_BUY_MODELED"}.into(),
    base:if i==1{""}else{"BTC"}.into(),
    entries:0,closed:0,open:0,modeled_realized_usdt:0.0,
    details:serde_json::json!({}),source:"PUBLIC_DEPTH_MODEL_ONLY".into(),
    exchange_fills_attested:false,real_orders_authorized:false,
    native_owner_verified_closed:false,
   };
   let body=serde_json::to_string(&payload).unwrap();
   let digest=model_hash_r156(i as u64,&prev,&body);
   lines.push(serde_json::json!({"seq":i,"previous_hash":prev,"body":body,
     "checksum":digest}).to_string());
   prev=digest;
  }
  (format!("{}\n",lines.join("\n")).into_bytes(),prev)
 }
 pub(super) fn session_journal_r159(session:&str,n:usize)->(Vec<u8>,String){
  let (base,_)=journal(n);let mut prev="0".repeat(64);let mut out=Vec::new();
  for (i,line) in base.split(|b|*b==b'\n').filter(|x|!x.is_empty()).enumerate(){
   let original:serde_json::Value=serde_json::from_slice(line).unwrap();
   let mut model:crate::native_virtual_four_leg_provenance_review::ModelAuditPayload=
     serde_json::from_str(original["body"].as_str().unwrap()).unwrap();
   model.session=session.into();
   let body=serde_json::to_string(&model).unwrap();
   let seq=i as u64+1;
   let hash=model_hash_r156(seq,&prev,&body);
   out.push(serde_json::json!({"seq":seq,"previous_hash":prev,
    "body":body,"checksum":hash}).to_string());
   prev=hash;
  }
  (format!("{}\n",out.join("\n")).into_bytes(),prev)
 }
 pub(super) fn projection(n:usize)->PaperProjectionR156{
  let (raw,hash)=journal(n);
  model_projection_rows_r156(&raw,n as u64,&hash,NOW).unwrap()
 }
 fn setup()->(PathBuf,SqliteMirrorReview){
  let root=std::env::temp_dir().join(format!("astra_r147_isolated_r157_{}_{}",
   std::process::id(),N.fetch_add(1,Ordering::Relaxed)));
  fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
  let db=SqliteMirrorReview::initialize(&root).unwrap();
  (root,db)
 }
 #[test]fn r156_stable_projection_repeated_identical_bytes(){
  let a=projection(4);let b=projection(4);
  assert_eq!(a.rows.len(),4);
  assert_eq!(a.rows[0].seq,43);
  assert_eq!(a.rows[0].prev_seq,42);
  assert_eq!(a.rows[3].seq,46);
  assert_eq!(a.rows[0].payload_json,b.rows[0].payload_json);
  assert!(!a.may_trade&&!a.actual_fills);
 }
 #[test]fn r156_corrupted_chain_and_attested_flag_denied(){
  let (raw,tip)=journal(2);
  let mut bad=raw.clone();bad[30]=if bad[30]==b'A'{b'B'}else{b'A'};
  assert!(model_projection_rows_r156(&bad,2,&tip,NOW).is_err());
  let mut line:serde_json::Value=serde_json::from_slice(raw.split(|b|*b==b'\n')
    .find(|x|!x.is_empty()).unwrap()).unwrap();
  let mut body:serde_json::Value=
    serde_json::from_str(line["body"].as_str().unwrap()).unwrap();
  body["exchange_fills_attested"]=serde_json::json!(true);
  let body=serde_json::to_string(&body).unwrap();
  let digest=model_hash_r156(1,&"0".repeat(64),&body);
  line["body"]=serde_json::json!(body);
  line["checksum"]=serde_json::json!(digest.clone());
  let one=format!("{}\n",line);
  assert_eq!(model_projection_rows_r156(one.as_bytes(),1,&digest,NOW).err(),
    Some(PaperAdapterErrorR156::Model));
 }
 #[test]fn r157_durable_replay_batch_and_cross_cold_idempotence(){
  let (root,db)=setup();let project=projection(18);
  let first=db.replay_modeled_paper_r157(&project,NOW,4).unwrap();
  assert_eq!((first.newly_applied,first.already_applied,first.final_seq),(18,0,60));
  let cold=SqliteMirrorReview::load(&root).unwrap();
  let second=cold.replay_modeled_paper_r157(&project,NOW,3).unwrap();
  assert_eq!((second.newly_applied,second.already_applied,second.final_seq),(0,18,60));
  assert_eq!(cold.review_integrity_r152(NOW).unwrap(),(60,18));
  fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r156_256_event_limit_boundary(){
  let (raw,hash)=journal(256);
  assert_eq!(model_projection_rows_r156(&raw,256,&hash,NOW).unwrap().rows.len(),256);
  let (raw,hash)=journal(257);
  assert_eq!(model_projection_rows_r156(&raw,257,&hash,NOW).err(),
    Some(PaperAdapterErrorR156::Limit));
 }
 #[test]fn r157_resume_after_source_committed_before_destination(){
  let (root,db)=setup();let p=projection(3);
  db.append_synthetic_source(p.rows[0].clone(),NOW).unwrap();
  assert_eq!(db.review_integrity_r152(NOW).unwrap(),(0,0));
  let report=db.replay_modeled_paper_r157(&p,NOW,2).unwrap();
  assert_eq!((report.newly_applied,report.final_seq),(3,45));
  assert_eq!(SqliteMirrorReview::load(&root).unwrap().review_integrity_r152(NOW)
    .unwrap(),(45,3));
  fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r159_cross_session_source_cannot_be_mixed(){
  let (root,db)=setup();let p=projection(4);
  db.replay_modeled_paper_r157(&p,NOW,4).unwrap();
  let (raw,hash)=session_journal_r159("session_201_1792253053",4);
  let other=model_projection_rows_r156(&raw,4,&hash,NOW).unwrap();
  assert_eq!(db.replay_modeled_paper_r157(&other,NOW,4).err(),
    Some(PaperAdapterErrorR156::Source));
  assert_eq!(db.review_integrity_r152(NOW).unwrap(),(46,4));
  let same=db.replay_modeled_paper_r157(&p,NOW,4).unwrap();
  assert_eq!((same.already_applied,same.newly_applied),(4,0));
  fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r163_oversized_snapshot_denied_without_reading(){
  let (root,db)=setup();
  let large=root.join("modeled_four_leg_journal.jsonl");
  let f=fs::OpenOptions::new().create_new(true).write(true).open(&large).unwrap();
  f.set_len(8*1024*1024+1).unwrap();drop(f);
  assert_eq!(snapshot_paper_source_r158(&large).err(),
    Some(PaperAdapterErrorR156::Path));
  assert_eq!(db.review_integrity_r152(NOW).unwrap(),(0,0));
  fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r160_atomic_batch_rollback_after_insert_and_cas(){
  let (root,db)=setup();let p=projection(8);
  for r in &p.rows{db.append_synthetic_source(r.clone(),NOW).unwrap();}
  for fault in ["rollback_after_insert","rollback_after_cas"]{
   assert_eq!(db.apply_batch_with_fault_r161(&p.rows,NOW,Some(fault)).err(),
     Some(MirrorDbError::Sql));
   assert_eq!(db.review_integrity_r152(NOW).unwrap(),(0,0));
  }
  let good=db.apply_batch_r160(&p.rows,NOW).unwrap();
  assert_eq!((good.applied,good.skipped,good.final_seq),(8,0,50));
  let cold=SqliteMirrorReview::load(&root).unwrap();
  let repeat=cold.apply_batch_r160(&p.rows,NOW).unwrap();
  assert_eq!((repeat.applied,repeat.skipped,repeat.final_seq),(0,8,50));
  fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r158_source_symlink_and_hardlink_denied(){
  use std::os::unix::fs::symlink;
  let (root,db)=setup();
  let plain=root.join("model.txt");fs::write(&plain,b"hello\n").unwrap();
  let symlink_path=root.join("symlink.txt");
  symlink(&plain,&symlink_path).unwrap();
  assert_eq!(snapshot_paper_source_r158(&symlink_path).err(),
    Some(PaperAdapterErrorR156::Path));
  let hard=root.join("hard.txt");fs::hard_link(&plain,&hard).unwrap();
  assert_eq!(snapshot_paper_source_r158(&hard).err(),
    Some(PaperAdapterErrorR156::Path));
  assert_eq!(db.review_integrity_r152(NOW).unwrap(),(0,0));
  fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r162_manual_projection_mutation_rejected(){
  let (root,db)=setup();let mut p=projection(3);
  p.session.push('x');
  assert_eq!(db.replay_modeled_paper_r157(&p,NOW,3).err(),
     Some(PaperAdapterErrorR156::Source));
  assert_eq!(db.review_integrity_r152(NOW).unwrap(),(0,0));
  fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r157_backpressure_and_tampered_source_deny(){
  let (root,db)=setup();let project=projection(2);
  assert_eq!(db.replay_modeled_paper_r157(&project,NOW,0).err(),
    Some(PaperAdapterErrorR156::Limit));
  assert_eq!(db.replay_modeled_paper_r157(&project,NOW,17).err(),
    Some(PaperAdapterErrorR156::Limit));
  db.replay_modeled_paper_r157(&project,NOW,1).unwrap();
  let a=open(&db.db1,READWRITE_EXISTING).unwrap();
  exec(&a,"UPDATE outbox SET row_json='{}' WHERE seq=43").unwrap();drop(a);
  assert!(db.replay_modeled_paper_r157(&project,NOW,1).is_err());
  fs::remove_dir_all(root).unwrap();
 }
}
