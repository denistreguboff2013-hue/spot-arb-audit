// R154: DETACHED witness contract only. Caller owns key and durable external floor.
// Neither DB, checkout, nor service stores any witness key. NEVER trading authority.
#[derive(Debug,Clone,serde::Serialize,serde::Deserialize,PartialEq,Eq)]
#[serde(deny_unknown_fields)]
pub struct DetachedWitnessR154 {
 pub owner:String,pub generation:String,pub anchor:u64,
 pub seq:u64,pub digest:String,pub mac:String,
}
fn hex_r154(b:&[u8])->String{b.iter().map(|v|format!("{v:02x}")).collect()}
fn hmac_r154(key:&[u8],msg:&[u8])->[u8;32]{
 use openssl::sha::sha256;
 let k=if key.len()>64{sha256(key).to_vec()}else{key.to_vec()};
 let mut pad=[0u8;64];pad[..k.len()].copy_from_slice(&k);
 let mut inner=pad;for x in &mut inner{*x^=0x36;}
 let mut outer=pad;for x in &mut outer{*x^=0x5c;}
 let mut b=inner.to_vec();b.extend_from_slice(msg);
 let d=sha256(&b);let mut b=outer.to_vec();b.extend_from_slice(&d);
 sha256(&b)
}
fn mac_message_r154(w:&DetachedWitnessR154)->Vec<u8>{
 format!("ASTRA-R154-v1\n{}\n{}\n{}\n{}\n{}\n",
  w.owner,w.generation,w.anchor,w.seq,w.digest).into_bytes()
}
fn reject_identity_r154(s:&str)->bool{
 s.is_empty()||s.len()>128||!s.bytes().all(|b|b.is_ascii_alphanumeric()
  ||matches!(b,b'-'|b'_'|b'.'))
}
impl SqliteMirrorReview {
 fn prefix_digest_r154(&self,now:u64,upto:u64)
  ->Result<(u64,String,String),MirrorDbError>{
  let (last,_)=self.review_integrity_r152(now)?;
  if upto==0||last<upto{return Err(MirrorDbError::Cursor)}
  let db=open(&self.db2,READONLY)?;
  let mut q=Stmt::new(&db,"SELECT anchor,generation FROM cursor WHERE id=1")?;
  if q.step()?!=SQLITE_ROW{return Err(MirrorDbError::Schema)}
  let anchor=q.num(0)?;let generation=q.strval(1)?;
  let mut q=Stmt::new(&db,
   "SELECT seq,tx_id,row_json FROM events WHERE seq<=?1 ORDER BY seq")?;
  q.number(1,upto)?;
  let mut b=b"ASTRA-R154-ROWS-v1\n".to_vec();let mut end=0;
  while q.step()?==SQLITE_ROW{
   end=q.num(0)?;
   for t in [end.to_string(),q.strval(1)?,q.strval(2)?]{
    b.extend_from_slice(&(t.len() as u64).to_be_bytes());
    b.extend_from_slice(t.as_bytes());
   }
  }
  if end!=upto{return Err(MirrorDbError::Corrupt)}
  Ok((anchor,generation,hex_r154(&openssl::sha::sha256(&b))))
 }
 pub fn create_detached_witness_r154(&self,now:u64,owner:&str,key:&[u8])
  ->Result<DetachedWitnessR154,MirrorDbError>{
  if reject_identity_r154(owner)||key.len()<16{return Err(MirrorDbError::Schema)}
  let (seq,_)=self.review_integrity_r152(now)?;
  let (anchor,generation,digest)=self.prefix_digest_r154(now,seq)?;
  let mut w=DetachedWitnessR154{
   owner:owner.into(),generation,anchor,seq,digest,mac:String::new()
  };
  w.mac=hex_r154(&hmac_r154(key,&mac_message_r154(&w)));
  Ok(w)
 }
 pub fn verify_detached_witness_r154(&self,now:u64,
   witness:&DetachedWitnessR154,expected_owner:&str,key:&[u8])
  ->Result<(),MirrorDbError>{
  if reject_identity_r154(expected_owner)||key.len()<16
    ||witness.owner!=expected_owner{return Err(MirrorDbError::Corrupt)}
  let wanted=hex_r154(&hmac_r154(key,&mac_message_r154(witness)));
  if wanted.len()!=witness.mac.len()
    ||!wanted.bytes().zip(witness.mac.bytes()).fold(0u8,|a,(x,y)|a|(x^y)).eq(&0){
   return Err(MirrorDbError::Corrupt)
  }
  let (anchor,generation,digest)=self.prefix_digest_r154(now,witness.seq)?;
  if anchor!=witness.anchor||generation!=witness.generation||digest!=witness.digest{
   return Err(MirrorDbError::Corrupt)
  }
  Ok(())
 }
}
#[cfg(test)]
mod r154_tests {
 use super::*;
 use std::{os::unix::fs::DirBuilderExt,sync::atomic::{AtomicUsize,Ordering}};
 static N:AtomicUsize=AtomicUsize::new(0);
 const NOW:u64=1_791_784_000_000;
 const KEY:&[u8]=b"synthetic-only-off-server-key-00001";
 fn setup()->(PathBuf,SqliteMirrorReview){
  let root=std::env::temp_dir().join(format!("astra_r147_isolated_r154_{}_{}",
   std::process::id(),N.fetch_add(1,Ordering::Relaxed)));
  fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
  let db=SqliteMirrorReview::initialize(&root).unwrap();
  (root,db)
 }
 fn row(seq:u64,prev:u64)->MirrorOutboxRowR139{
  let mut x:MirrorOutboxRowR139=serde_json::from_str(include_str!(
   "../config/native_mirror_outbox_python_fixture_r141_review.json")).unwrap();
  x.seq=seq;x.prev_seq=prev;x.tx_id=format!("tx-{seq:032x}");x
 }
 #[test]fn r154_detached_witness_valid_and_owner_generation_tamper_deny(){
  let (root,db)=setup();db.append_synthetic_source(row(43,42),NOW).unwrap();
  db.apply_offline(43,NOW,false).unwrap();
  let w=db.create_detached_witness_r154(NOW,"research-owner",KEY).unwrap();
  assert!(db.verify_detached_witness_r154(NOW,&w,"research-owner",KEY).is_ok());
  assert!(db.verify_detached_witness_r154(NOW,&w,"other-owner",KEY).is_err());
  let mut forged=w.clone();forged.generation.push('f');
  assert!(db.verify_detached_witness_r154(NOW,&forged,"research-owner",KEY).is_err());
  fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r154_both_databases_valid_rollback_denied_by_detached_floor(){
  let (root,db)=setup();
  db.append_synthetic_source(row(43,42),NOW).unwrap();
  db.apply_offline(43,NOW,false).unwrap();
  let old1=fs::read(&db.db1).unwrap();let old2=fs::read(&db.db2).unwrap();
  db.append_synthetic_source(row(100,43),NOW).unwrap();
  db.apply_offline(100,NOW,false).unwrap();
  let witness=db.create_detached_witness_r154(NOW,"research-owner",KEY).unwrap();
  // Test-only replacement of two isolated SQLite files, NOT an active database.
  fs::write(&db.db1,old1).unwrap();fs::write(&db.db2,old2).unwrap();
  let cold=SqliteMirrorReview::load(&root).unwrap();
  assert_eq!(cold.review_integrity_r152(NOW).unwrap(),(43,1));
  assert_eq!(cold.verify_detached_witness_r154(NOW,&witness,"research-owner",KEY)
   .err(),Some(MirrorDbError::Cursor));
  fs::remove_dir_all(root).unwrap();
 }
}
