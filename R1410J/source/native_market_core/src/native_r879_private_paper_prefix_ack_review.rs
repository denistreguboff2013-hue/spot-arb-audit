// R895: content-bind every newly acknowledged original PAPER operation
// to BOTH physical DB rows, not just DB1<->DB2 parity. Reject equal
// tampering in both DBs if original outbox still has its integrity chain.
fn r895_verify_new_paper_rows(
 src:&Db,dst:&Db,manifest:&serde_json::Value,
 expected:&std::collections::BTreeMap<String,serde_json::Value>
)->Result<(),EventOutboxR366Error>{
 if expected.is_empty()||expected.len()>2048{return Err(EventOutboxR366Error::Pressure)}
 let tbl=manifest["table_schema"].as_array().ok_or(EventOutboxR366Error::Schema)?
  .iter().find(|x|x["table"]=="events").ok_or(EventOutboxR366Error::Schema)?;
 let cols=tbl["columns"].as_array().ok_or(EventOutboxR366Error::Schema)?;
 let mut projection=Vec::new();
 for col in cols{
  let name=col[1].as_str().ok_or(EventOutboxR366Error::Schema)?;
  projection.push(format!("'{}',{}",name,r586_identifier(name)?));
 }
 let first=expected.first_key_value().ok_or(EventOutboxR366Error::Corrupt)?.0;
 let last=expected.last_key_value().ok_or(EventOutboxR366Error::Corrupt)?.0;
 let low=r586_sql_literal(&serde_json::json!(first))?;
 let high=r586_sql_literal(&serde_json::json!(last))?;
 let sql=format!("SELECT json_object({}) FROM events WHERE event_id>={low} AND event_id<={high} ORDER BY event_id",projection.join(","));
 for db in [src,dst]{
  let mut q=Stmt::new(db,&sql).map_err(r366_error)?;
  for (id,row) in expected{
   if q.step().map_err(r366_error)?!=SQLITE_ROW{return Err(EventOutboxR366Error::Corrupt)}
   let actual:serde_json::Value=serde_json::from_str(&q.strval(0).map_err(r366_error)?)
    .map_err(|_|EventOutboxR366Error::Corrupt)?;
   if &actual!=row || actual["event_id"]!=*id{return Err(EventOutboxR366Error::Corrupt)}
  }
  if q.step().map_err(r366_error)?!=SQLITE_DONE{return Err(EventOutboxR366Error::Corrupt)}
 }
 Ok(())
}

// Astra P0-01 R879: experimental *private PAPER event-only* prefix ACK.
// Unlike R860, this can acknowledge a DB2-proven prefix while DB1's source
// tail grows. It is NOT valid for an arbitrary original 17-table operation,
// not production-ready, and never bypasses source hash, DB2 hash or parity.
fn r879_private_paper_prefix_parity(
 src:&Db,dst:&Db,manifest:&serde_json::Value,run:&str,applied:u64
)->Result<u64,EventOutboxR366Error>{
 if run.is_empty()||run.len()>80||!run.bytes().all(
  |c|c.is_ascii_alphanumeric()||c==b'_'||c==b'-'){
  return Err(EventOutboxR366Error::Schema)
 }
 let limit=format!("paper_r824_{run}_{applied:020}_31");
 let cutoff=r586_sql_literal(&serde_json::json!(limit))?;
 let tables=manifest["table_schema"].as_array().ok_or(EventOutboxR366Error::Schema)?;
 if tables.len()!=17{return Err(EventOutboxR366Error::Schema)}
 let mut total=0u64;let mut event_count=0u64;
 for item in tables{
  let table=item["table"].as_str().ok_or(EventOutboxR366Error::Schema)?;
  let columns=item["columns"].as_array().ok_or(EventOutboxR366Error::Schema)?;
  let keys=manifest["source_keys"][table].as_array().ok_or(EventOutboxR366Error::Schema)?;
  if columns.is_empty()||columns.len()>256||keys.is_empty(){
   return Err(EventOutboxR366Error::Schema)
  }
  let names=columns.iter().map(|col|col[1].as_str()).collect::<Option<Vec<_>>>()
   .ok_or(EventOutboxR366Error::Schema)?;
  let cols=names.iter().map(|col|Ok(format!("quote({})",r586_identifier(col)?)))
   .collect::<Result<Vec<_>,EventOutboxR366Error>>()?.join(",");
  let order=keys.iter().map(|key|r586_identifier(key.as_str().ok_or(EventOutboxR366Error::Schema)?))
   .collect::<Result<Vec<_>,_>>()?.join(",");
  let tbl=r586_identifier(table)?;
  let source_filter=if table=="events"{format!(" WHERE event_id<={cutoff}")}else{String::new()};
  let src_sql=format!("SELECT {cols} FROM {tbl}{source_filter} ORDER BY {order}");
  let dst_sql=format!("SELECT {cols} FROM {tbl} ORDER BY {order}");
  let mut left=Stmt::new(src,&src_sql).map_err(r366_error)?;
  let mut right=Stmt::new(dst,&dst_sql).map_err(r366_error)?;
  loop {
   let a=left.step().map_err(r366_error)?;
   let b=right.step().map_err(r366_error)?;
   if a==SQLITE_DONE&&b==SQLITE_DONE{break}
   if a!=SQLITE_ROW||b!=SQLITE_ROW{return Err(EventOutboxR366Error::Corrupt)}
   total=total.checked_add(1).ok_or(EventOutboxR366Error::Pressure)?;
   if total>10_000_000{return Err(EventOutboxR366Error::Pressure)}
   if table=="events"{event_count+=1;}
   for i in 0..names.len(){
    let l=left.strval(i as i32).map_err(r366_error)?;
    let r=right.strval(i as i32).map_err(r366_error)?;
    if l.len()>4*1024*1024||r.len()>4*1024*1024{
     return Err(EventOutboxR366Error::Pressure)
    }
    if l!=r{return Err(EventOutboxR366Error::Corrupt)}
   }
  }
 }
 if event_count!=applied.checked_mul(32).ok_or(EventOutboxR366Error::Pressure)?{
  return Err(EventOutboxR366Error::Corrupt)
 }
 Ok(total)
}
include!("native_r907_fused_retained_verify_review.rs");
fn r879_private_paper_prefix_ack(
 root:&Path,src:&Db,dst:&Db,manifest:&serde_json::Value,run:&str
)->Result<(u64,u64),EventOutboxR366Error>{
 let source_tx=Tx::begin(src).map_err(r366_error)?;
 let dest_tx=Tx::begin(dst).map_err(r366_error)?;
 let (h,c,m)=r805_read_head(root,src,dst)?;
 if h.applied==h.cleaned{return Err(EventOutboxR366Error::Conflict)}
 if h.applied>h.latest||h.retained>64{return Err(EventOutboxR366Error::Pressure)}
 let generation=r726_s(&c,"feed_generation")?;
 let (acked,bytes,rows,source_hash)=
  r907_verify_retained_and_extract_prefix(src,&h,generation,run)?;
 if m["applied_hash"].as_str()!=Some(source_hash.as_str()){
  return Err(EventOutboxR366Error::Corrupt)
 }
 if r366_query_num(dst,"SELECT COUNT(*) FROM pragma_foreign_key_check")?!=0{
  return Err(EventOutboxR366Error::Corrupt)
 }
 // Verify every original source table, with only future PAPER events
 // excluded from the DB1 side. This cannot be used for non-PAPER writes.
 r879_private_paper_prefix_parity(src,dst,manifest,run,h.applied)?;
 r895_verify_new_paper_rows(src,dst,manifest,&rows)?;
 let sql=format!("DELETE FROM mirror_outbox WHERE seq<={}; UPDATE mirror_control SET cleanup_through={}, retained_records=retained_records-{acked}, retained_bytes=retained_bytes-{bytes} WHERE id=1",h.applied,h.applied);
 exec(src,&sql).map_err(r366_error)?;
 dest_tx.commit().map_err(r366_error)?;
 source_tx.commit().map_err(r366_error)?;
 Ok((acked,h.retained-acked))
}
#[cfg(test)]
mod r879_tests{
 use super::*;
 use crate::{Book,Owner,RawFrame,Update,Venue};
 use std::os::unix::fs::DirBuilderExt;
 use std::time::{SystemTime,UNIX_EPOCH};
 struct Root(PathBuf);
 impl Root{
  fn new()->Self{
   let mut p=std::env::temp_dir();
   p.push(format!("astra_r671_file_{}_{}_r879",std::process::id(),SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()));
   fs::DirBuilder::new().mode(0o700).create(&p).unwrap();
   drop(r756_initialize(&p).unwrap());Self(p)
  }
 }
 impl Drop for Root{fn drop(&mut self){let _=fs::remove_dir_all(&self.0);}}
 fn event(i:u64)->(RawFrame,Update){
  let venue=Venue::OkxSpot;
  let ns=1_000_000_000+i*10_000_000;let ms=1_800_000_000_000+i as i64*10;
  let f=RawFrame{source:None,venue,epoch:1,received_ns:ns,received_wall_ms:ms,raw:format!("{{\"r879\":{i}}}")};
  let book=Book{bids:vec![(101.0,1.0)],asks:vec![(102.0,1.0)],received_ns:ns,received_wall_ms:ms,
   exchange_ts_ms:Some(ms),changed_ns:ns,epoch:1,sequence:Some(i as i64)};
  (f,Update{base:"BTC".into(),venue,book})
 }
 fn pair(batches:u64)->(Root,R819OriginalPaperHistory,Owner){
  let root=Root::new();
  let mut h=R819OriginalPaperHistory::new(&root.0,"r879_probe").unwrap();
  let mut owner=Owner::new(0,1,800).unwrap();
  for i in (1..=32*batches).step_by(32){
   assert_eq!(r824_commit_prepared_batch(&mut h,&mut owner,&(i..i+32).map(event).collect::<Vec<_>>()),
     Ok(((i+31)/32,32)));
  }
  (root,h,owner)
 }
 #[test] fn r879_ack_one_of_three_source_envelopes(){
  let (root,h,owner)=pair(3);
  let v=r586_oracle();
  assert_eq!(r845_verified_burst(&root.0,&h.source,&h.sink,&v,1),Ok(1));
  assert_eq!(r879_private_paper_prefix_ack(&root.0,&h.source,&h.sink,&v,"r879_probe"),Ok((1,2)));
  assert_eq!(r366_query_num(&h.source,"SELECT cleanup_through FROM mirror_control"),Ok(1));
  assert_eq!(r366_query_num(&h.source,"SELECT retained_records FROM mirror_control"),Ok(2));
  assert_eq!(r366_query_num(&h.sink,"SELECT COUNT(*) FROM events"),Ok(32));
  assert_eq!(r845_verified_burst(&root.0,&h.source,&h.sink,&v,128),Ok(2));
  assert_eq!(r860_bulk_verified_ack(&root.0,&h.source,&h.sink,&v,1024),Ok((2,0)));
  assert_eq!(r800_streaming_original_parity(&h.source,&h.sink,&v),Ok(96));
  let (recovered,count)=r834_recover_private_owner(&root.0,"r879_probe",0,1,800).unwrap();
  assert_eq!(count,96);
  assert_eq!(recovered.snapshot_venue("BTC",Venue::OkxSpot),owner.snapshot_venue("BTC",Venue::OkxSpot));
 }
 #[test] fn r880_tampered_db2_event_denies_prefix_ack(){
  let (root,h,_)=pair(2);
  let v=r586_oracle();
  assert_eq!(r845_verified_burst(&root.0,&h.source,&h.sink,&v,1),Ok(1));
  exec(&h.sink,"UPDATE events SET content_hash='FORGED' WHERE event_id=(SELECT min(event_id) FROM events)").unwrap();
  assert_eq!(r879_private_paper_prefix_ack(&root.0,&h.source,&h.sink,&v,"r879_probe"),Err(EventOutboxR366Error::Corrupt));
  assert_eq!(r366_query_num(&h.source,"SELECT cleanup_through FROM mirror_control"),Ok(0));
 }
 #[test] fn r881_future_corrupted_source_chain_denies_prefix_ack(){
  let (root,h,_)=pair(3);
  let v=r586_oracle();
  assert_eq!(r845_verified_burst(&root.0,&h.source,&h.sink,&v,1),Ok(1));
  exec(&h.source,"UPDATE mirror_outbox SET payload_hash='FORGED' WHERE seq=3").unwrap();
  assert_eq!(r879_private_paper_prefix_ack(&root.0,&h.source,&h.sink,&v,"r879_probe"),Err(EventOutboxR366Error::Corrupt));
  assert_eq!(r366_query_num(&h.source,"SELECT cleanup_through FROM mirror_control"),Ok(0));
 }
 #[test] fn r882_mutated_other_original_table_denies_prefix_ack(){
  let (root,h,_)=pair(3);
  let v=r586_oracle();
  assert_eq!(r845_verified_burst(&root.0,&h.source,&h.sink,&v,1),Ok(1));
  exec(&h.source,"INSERT INTO metadata(key,value) VALUES('r882_sneak','not_replayed')").unwrap();
  assert_eq!(r879_private_paper_prefix_ack(&root.0,&h.source,&h.sink,&v,"r879_probe"),Err(EventOutboxR366Error::Corrupt));
  assert_eq!(r366_query_num(&h.source,"SELECT cleanup_through FROM mirror_control"),Ok(0));
 }
 #[test] fn r883_wrong_run_cannot_steal_prefix_ack(){
  let (root,h,_)=pair(2);let v=r586_oracle();
  assert_eq!(r845_verified_burst(&root.0,&h.source,&h.sink,&v,1),Ok(1));
  assert_eq!(r879_private_paper_prefix_ack(&root.0,&h.source,&h.sink,&v,"wrong"),Err(EventOutboxR366Error::Corrupt));
  assert_eq!(r366_query_num(&h.source,"SELECT cleanup_through FROM mirror_control"),Ok(0));
 }
 #[test] fn r884_no_ack_without_db2_applied_prefix(){
  let (root,h,_)=pair(2);let v=r586_oracle();
  assert_eq!(r879_private_paper_prefix_ack(&root.0,&h.source,&h.sink,&v,"r879_probe"),Err(EventOutboxR366Error::Conflict));
  assert_eq!(r366_query_num(&h.source,"SELECT retained_records FROM mirror_control"),Ok(2));
 }
 #[test] fn r895_equal_source_and_db2_tampering_denied_by_outbox_binding(){
  let (root,h,_)=pair(2);let v=r586_oracle();
  assert_eq!(r845_verified_burst(&root.0,&h.source,&h.sink,&v,1),Ok(1));
  let attack="UPDATE events SET content_hash='BOTH_FORGED' WHERE event_id=(SELECT min(event_id) FROM events)";
  exec(&h.source,attack).unwrap();
  exec(&h.sink,attack).unwrap();
  assert_eq!(r879_private_paper_prefix_ack(&root.0,&h.source,&h.sink,&v,"r879_probe"),Err(EventOutboxR366Error::Corrupt));
  assert_eq!(r366_query_num(&h.source,"SELECT cleanup_through FROM mirror_control"),Ok(0));
 }
 #[test] fn r896_equal_source_and_db2_payload_mutation_denied(){
  let (root,h,_)=pair(2);let v=r586_oracle();
  assert_eq!(r845_verified_burst(&root.0,&h.source,&h.sink,&v,1),Ok(1));
  let attack="UPDATE events SET payload_json='FORGED',content_hash='FORGED' WHERE event_id=(SELECT min(event_id) FROM events)";
  exec(&h.source,attack).unwrap();
  exec(&h.sink,attack).unwrap();
  assert_eq!(r879_private_paper_prefix_ack(&root.0,&h.source,&h.sink,&v,"r879_probe"),Err(EventOutboxR366Error::Corrupt));
  assert_eq!(r366_query_num(&h.source,"SELECT cleanup_through FROM mirror_control"),Ok(0));
 }
 #[test] fn r897_invalid_run_identifier_denied_before_cleanup(){
  let (root,h,_)=pair(2);let v=r586_oracle();
  assert_eq!(r845_verified_burst(&root.0,&h.source,&h.sink,&v,1),Ok(1));
  assert_eq!(r879_private_paper_prefix_ack(&root.0,&h.source,&h.sink,&v,"../different_run"),Err(EventOutboxR366Error::Schema));
  assert_eq!(r366_query_num(&h.source,"SELECT cleanup_through FROM mirror_control"),Ok(0));
 }
 include!("native_r898_receiver_sigkill_review.rs");
 include!("native_r906_stage_census_review.rs");
 include!("native_r908_fused_negative_review.rs");
 include!("native_r914_optimistic_snapshot_ack_review.rs");
 include!("native_r924_paper_only_immutable_guard_review.rs");
 include!("native_r932_incremental_parity_ack_review.rs");
 include!("native_r944_private_ack_stage_census_review.rs");
 include!("native_r945_guarded_fingerprint_ack_review.rs");
 include!("native_r951_prepared_event_replay_tests.rs");
 include!("native_r954_private_fused_replay_ack_review.rs");
 include!("native_r956_fused_interrupt_and_append_review.rs");
 include!("native_r960_large_owner_recovery_review.rs");
 include!("native_r962_raw_column_binding_review.rs");
}
