//! R139: offline readonly validation of legacy DB1 mirror_outbox rows.
//! Follows spotbot/ai/mirror_outbox.py protocol v1, NOT executable mirroring.
//! No sqlite writes, no DB2 promotion, no order authorization.
use serde_json::Value;
use openssl::sha::sha256;
use std::collections::BTreeSet;
const MAX_TX_BYTES:u64=4*1024*1024;
const MAX_ROWS:usize=256;
#[derive(Debug,Clone,PartialEq,Eq)]
pub enum MirrorRowError{Shape,Identity,Sequence,Generation,Payload,Clock,Limit}
#[derive(Clone,Debug,serde::Deserialize,serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct MirrorOutboxRowR139{
 pub seq:u64,pub prev_seq:u64,pub tx_id:String,pub tx_kind:String,
 pub schema_generation:u64,pub payload_json:String,pub payload_hash:String,
 pub payload_bytes:u64,pub committed_at_ms:u64,pub feed_generation:String,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct MirrorChainReviewR139{
 pub first_seq:u64,pub last_seq:u64,pub examined:usize,
 pub bytes_accounted:u64,pub feed_generation:String,
 pub can_apply_db2:bool,pub can_trade:bool,
}
fn lower_hex(b:&str)->bool{
 !b.is_empty()&&b.bytes().all(|c|c.is_ascii_digit()||(b'a'..=b'f').contains(&c))
}
fn hash(raw:&str)->String{
 sha256(raw.as_bytes()).iter().map(|b|format!("{b:02x}")).collect()
}
fn validate_row(r:&MirrorOutboxRowR139,now:u64)->Result<(),MirrorRowError>{
 if r.seq==0||r.prev_seq>=r.seq||r.schema_generation!=1{
  return Err(MirrorRowError::Sequence)
 }
 if !r.tx_id.starts_with("tx-")||r.tx_id.len()!=35
  ||!lower_hex(&r.tx_id[3..]){
  return Err(MirrorRowError::Identity)
 }
 if r.tx_kind.is_empty()||r.tx_kind.len()>128
  ||!r.tx_kind.bytes().all(|b|b.is_ascii_alphanumeric()||b==b'_'||b==b'-'){
  return Err(MirrorRowError::Identity)
 }
 if !r.feed_generation.starts_with("rebase-")
  ||r.feed_generation.len()>96||r.feed_generation.len()<=7
  ||!lower_hex(&r.feed_generation[7..]){
  return Err(MirrorRowError::Generation)
 }
 if now==0||r.committed_at_ms==0
  ||r.committed_at_ms>now.saturating_add(5_000){
  return Err(MirrorRowError::Clock)
 }
 if r.payload_json.len()>MAX_TX_BYTES as usize{return Err(MirrorRowError::Limit)}
 let v:Value=serde_json::from_str(&r.payload_json)
  .map_err(|_|MirrorRowError::Payload)?;
 if v["protocol_version"]!=1||v["schema_generation"]!=1
  ||v["tx_kind"].as_str()!=Some(&r.tx_kind)
  ||v["operations"].as_array().map_or(true,|a|a.is_empty()){
  return Err(MirrorRowError::Payload)
 }
 if v.as_object().map_or(true,|m|m.len()!=4)
  ||serde_json::to_string(&v).map_err(|_|MirrorRowError::Payload)?
    !=r.payload_json{
  return Err(MirrorRowError::Payload)
 }
 if r.payload_hash.len()!=64||!lower_hex(&r.payload_hash)
  ||r.payload_hash!=hash(&r.payload_json){
  return Err(MirrorRowError::Payload)
 }
 let expected=(r.payload_json.len() as u64).checked_add(384)
  .ok_or(MirrorRowError::Limit)?;
 if r.payload_bytes!=expected{return Err(MirrorRowError::Payload)}
 Ok(())
}
pub fn audit_outbox_chain(
 rows:&[MirrorOutboxRowR139],anchor_seq:u64,now_ms:u64
)->Result<MirrorChainReviewR139,MirrorRowError>{
 if rows.is_empty()||rows.len()>MAX_ROWS{return Err(MirrorRowError::Limit)}
 let mut prior_seq=anchor_seq;
 let mut last_clock=0;
 let mut gen=None::<String>;
 let mut seen=BTreeSet::new();
 let mut bytes=0u64;
 for row in rows{
  validate_row(row,now_ms)?;
  if row.prev_seq!=prior_seq{return Err(MirrorRowError::Sequence)}
  if row.committed_at_ms<last_clock{return Err(MirrorRowError::Clock)}
  if !seen.insert(row.tx_id.as_str()){return Err(MirrorRowError::Identity)}
  if gen.as_deref().is_some_and(|x|x!=row.feed_generation){
   return Err(MirrorRowError::Generation)
  }
  gen.get_or_insert_with(||row.feed_generation.clone());
  bytes=bytes.checked_add(row.payload_bytes).ok_or(MirrorRowError::Limit)?;
  prior_seq=row.seq;last_clock=row.committed_at_ms;
 }
 Ok(MirrorChainReviewR139{first_seq:rows[0].seq,last_seq:prior_seq,
  examined:rows.len(),bytes_accounted:bytes,
  feed_generation:gen.unwrap(),can_apply_db2:false,can_trade:false})
}
#[cfg(test)]
mod tests{
 use super::*;
 const NOW:u64=1_791_784_000_000;
 fn row(seq:u64,prev:u64)->MirrorOutboxRowR139{
  let raw=serde_json::json!({"protocol_version":1,
   "schema_generation":1,"tx_kind":"collector",
   "operations":[{"op":"upsert","table":"events"}]}).to_string();
  let n=raw.len() as u64;
  MirrorOutboxRowR139{seq,prev_seq:prev,
   tx_id:format!("tx-{seq:032x}"),tx_kind:"collector".into(),
   schema_generation:1,payload_hash:hash(&raw),
   payload_json:raw,payload_bytes:n+384,
   committed_at_ms:NOW-800,feed_generation:
    "rebase-1234567890abcdef1234567890abcdef".into()}
 }
 #[test]fn r139_gapped_sqlite_autoincrement_uses_prev_seq_not_plus_one(){
  let a=row(43,42);
  let mut b=row(100,43);
  b.committed_at_ms=NOW-500;
  let result=audit_outbox_chain(&[a,b],42,NOW).unwrap();
  assert_eq!(result.first_seq,43);
  assert_eq!(result.last_seq,100);
  assert_eq!(result.examined,2);
  assert!(!result.can_apply_db2&&!result.can_trade);
 }
 #[test]fn r139_payload_hash_size_and_canonical_integrity_enforced(){
  let mut a=row(43,42);
  a.payload_json.push(' ');
  assert_eq!(audit_outbox_chain(&[a],42,NOW),Err(MirrorRowError::Payload));
  let mut a=row(43,42);
  a.payload_hash="a".repeat(64);
  assert_eq!(audit_outbox_chain(&[a],42,NOW),Err(MirrorRowError::Payload));
  let mut a=row(43,42);
  a.payload_bytes-=1;
  assert_eq!(audit_outbox_chain(&[a],42,NOW),Err(MirrorRowError::Payload));
 }
 #[test]fn r139_rejects_prev_seq_rewind_and_duplicate_tx_id(){
  let a=row(43,42);
  let b=row(45,44);
  assert_eq!(audit_outbox_chain(&[a.clone(),b],42,NOW),
   Err(MirrorRowError::Sequence));
  let mut b=row(45,43);
  b.tx_id=a.tx_id.clone();
  assert_eq!(audit_outbox_chain(&[a,b],42,NOW),
   Err(MirrorRowError::Identity));
 }
 #[test]fn r139_feed_gen_drift_and_time_rewind_deny(){
  let a=row(43,42);
  let mut b=row(45,43);
  b.feed_generation="rebase-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into();
  assert_eq!(audit_outbox_chain(&[a.clone(),b],42,NOW),
   Err(MirrorRowError::Generation));
  let mut b=row(45,43);
  b.committed_at_ms=NOW-900;
  assert_eq!(audit_outbox_chain(&[a.clone(),b],42,NOW),
   Err(MirrorRowError::Clock));
  let mut b=row(45,43);
  b.committed_at_ms=NOW+5_001;
  assert_eq!(audit_outbox_chain(&[a,b],42,NOW),
   Err(MirrorRowError::Clock));
 }
 #[test]fn r141_python_canonical_utf8_and_reserved_bytes_exact_parity(){
  // Fixture made by Python json.dumps(sort_keys=True, ensure_ascii=False,
  // separators=(',', ':'), allow_nan=False) + hashlib.sha256.
  let x:MirrorOutboxRowR139=serde_json::from_str(include_str!(
   "../config/native_mirror_outbox_python_fixture_r141_review.json")).unwrap();
  assert!(x.payload_json.contains("решение"));
  assert_eq!(x.payload_bytes,x.payload_json.as_bytes().len() as u64+384);
  let report=audit_outbox_chain(&[x],42,1_791_784_000_000).unwrap();
  assert_eq!(report.last_seq,43);
  assert!(!report.can_apply_db2&&!report.can_trade);
 }
 #[test]fn r139_absent_rows_and_unknown_original_generation_cannot_be_accepted(){
  assert_eq!(audit_outbox_chain(&[],42,NOW),Err(MirrorRowError::Limit));
  let mut a=row(43,42);
  a.feed_generation="legacy".into();
  assert_eq!(audit_outbox_chain(&[a],42,NOW),
   Err(MirrorRowError::Generation));
 }
}
