// R546-R555: isolated LOCAL baseline provenance, not a remote trust witness.
// Both DB1 and DB2 must be separate 0600 test SQLite files under R366 root.
#[derive(Clone,Debug,PartialEq,Eq)]
struct R546LocalBaseline {
 boundary:u64,source_identity:String,generation:String,
 db2_snapshot_matched:bool,independent_witness:bool,
 permits_normal:bool,permits_real:bool
}
fn r546_private_generation(g:&str)->bool{
 g.len()==39&&g.starts_with("rebase-")&&
 g[7..].bytes().all(|b|b.is_ascii_hexdigit())
}
fn r546_local_prefix(db1:&Db,db2:&Db,through:u64)
 ->Result<String,EventOutboxR366Error>{
 use serde_json::json;
 if through==0||through>4096{return Err(EventOutboxR366Error::Schema)}
 let mut hashes=Vec::new();
 for seq in 1..=through{
  let (_,txid,source_hash,_)=r506_source_frame(db1,seq)?;
  let mut q=Stmt::new(db2,"SELECT tx_id,payload_hash FROM sink_delivery
    WHERE seq=?1").map_err(r366_error)?;
  q.number(1,seq).map_err(r366_error)?;
  if q.step().map_err(r366_error)?!=SQLITE_ROW||
     q.strval(0).map_err(r366_error)?!=txid||
     q.strval(1).map_err(r366_error)?!=source_hash||
     q.step().map_err(r366_error)?!=SQLITE_DONE{
    return Err(EventOutboxR366Error::Corrupt)
  }
  hashes.push(json!({"seq":seq,"tx_id":txid,"payload_hash":source_hash}));
 }
 let canonical=r386_python_mirror_canonical(&json!({
  "purpose":"R546_ISOLATED_FROZEN_PREFIX_NOT_PRODUCTION",
  "boundary":through,"source_tx":hashes}))?;
 Ok(format!("sha256:{}",r366_sha(&canonical)))
}
fn r546_create_local_schema(db:&Db)->Result<(),EventOutboxR366Error>{
 exec(db,"CREATE TABLE IF NOT EXISTS r546_private_baseline(
  id INTEGER PRIMARY KEY CHECK(id=1),
  boundary INTEGER NOT NULL,source_identity TEXT NOT NULL,
  feed_generation TEXT NOT NULL,proof_hash TEXT NOT NULL);")
 .map_err(r366_error)
}
impl R506IsolatedReplica{
 fn r546_preview_identity(&self,source:&IsolatedEventOutboxR366)
   ->Result<String,EventOutboxR366Error>{
  let s=self.inspect(source)?;
  if s.source_seq==0||s.source_seq!=s.sink_seq||
    s.sink_seq!=s.ack_seq{return Err(EventOutboxR366Error::Conflict)}
  let db1=r366_open(&source.path)?;
  let db2=r506_db2_open(&self.root)?;
  r546_local_prefix(&db1,&db2,s.source_seq)
 }
 fn r546_stage(&self,source:&IsolatedEventOutboxR366,
  frozen_identity:&str,generation:&str)
   ->Result<R546LocalBaseline,EventOutboxR366Error>{
  if !r546_private_generation(generation)||source.path.parent()!=Some(self.root.as_path()){
    return Err(EventOutboxR366Error::Schema)
  }
  let state=self.inspect(source)?;
  if state.source_seq==0||state.source_seq>4096||
    state.source_seq!=state.sink_seq||
    state.source_seq!=state.ack_seq{
   return Err(EventOutboxR366Error::Conflict)
  }
  // Pin DB1 writes while computing source identity and committing private DB2.
  let db1=r366_open(&source.path)?;
  let lock1=Tx::begin(&db1).map_err(r366_error)?;
  let db2=r506_db2_open(&self.root)?;
  let lock2=Tx::begin(&db2).map_err(r366_error)?;
  if r366_query_num(&db1,
    "SELECT latest_seq FROM mirror_control WHERE id=1")?!=state.source_seq||
    r366_query_num(&db2,
    "SELECT applied_seq FROM sink_control WHERE id=1")?!=state.source_seq||
    r506_source_ack_audit(&db1)?!=state.source_seq{
   return Err(EventOutboxR366Error::Conflict)
  }
  let actual=r546_local_prefix(&db1,&db2,state.source_seq)?;
  if actual!=frozen_identity{return Err(EventOutboxR366Error::Conflict)}
  r546_create_local_schema(&db2)?;
  if r366_query_num(&db2,"SELECT COUNT(*) FROM r546_private_baseline")?!=0{
    return Err(EventOutboxR366Error::Duplicate)
  }
  let h=r366_sha(&format!(
    "r546:{generation}:{}:{actual}",state.source_seq));
  let mut q=Stmt::new(&db2,"INSERT INTO r546_private_baseline(
    id,boundary,source_identity,feed_generation,proof_hash)
    VALUES(1,?1,?2,?3,?4)").map_err(r366_error)?;
  q.number(1,state.source_seq).map_err(r366_error)?;
  for (n,s) in [actual.as_str(),generation,&h].iter().enumerate(){
   q.string(n as i32+2,s).map_err(r366_error)?;
  }
  if q.step().map_err(r366_error)?!=SQLITE_DONE{
   return Err(EventOutboxR366Error::Conflict)
  }
  drop(q);
  lock2.commit().map_err(r366_error)?;
  lock1.commit().map_err(r366_error)?;
  Ok(R546LocalBaseline{boundary:state.source_seq,
   source_identity:actual,generation:generation.into(),
   db2_snapshot_matched:true,independent_witness:false,
   permits_normal:false,permits_real:false})
 }
 fn r546_verify(&self,source:&IsolatedEventOutboxR366)
  ->Result<R546LocalBaseline,EventOutboxR366Error>{
  let state=self.inspect(source)?;
  let db1=r366_open(&source.path)?;
  let db2=r506_db2_open(&self.root)?;
  let mut q=Stmt::new(&db2,"SELECT boundary,source_identity,
     feed_generation,proof_hash FROM r546_private_baseline WHERE id=1")
     .map_err(r366_error)?;
  if q.step().map_err(r366_error)?!=SQLITE_ROW{
   return Err(EventOutboxR366Error::Schema)
  }
  let boundary=q.num(0).map_err(r366_error)?;
  let identity=q.strval(1).map_err(r366_error)?;
  let generation=q.strval(2).map_err(r366_error)?;
  let saved=q.strval(3).map_err(r366_error)?;
  if q.step().map_err(r366_error)?!=SQLITE_DONE{
   return Err(EventOutboxR366Error::Corrupt)
  }
  drop(q);
  if boundary==0||!r546_private_generation(&generation)||
    boundary!=state.source_seq||boundary!=state.sink_seq||
    boundary!=state.ack_seq||
    identity!=r546_local_prefix(&db1,&db2,boundary)?||
    saved!=r366_sha(&format!("r546:{generation}:{boundary}:{identity}")){
    return Err(EventOutboxR366Error::Corrupt)
  }
  Ok(R546LocalBaseline{boundary,source_identity:identity,
   generation,db2_snapshot_matched:true,independent_witness:false,
   permits_normal:false,permits_real:false})
 }
}
#[cfg(test)]
mod r546_tests{
 use super::*;
 use super::r366_tests::{fixture_root,record};
 use serde_json::Value;
 const T:u64=1_792_660_000_000;
 fn gen()->String{
  let o:Value=serde_json::from_str(include_str!(
    "../config/native_r536_python_rebaseline_oracle.json")).unwrap();
  o["pending"]["feed_generation"].as_str().unwrap().into()
 }
 fn setup(n:u64,ack:u64)->(PathBuf,IsolatedEventOutboxR366,R506IsolatedReplica){
  let root=fixture_root();
  let src=IsolatedEventOutboxR366::initialize(&root).unwrap();
  let dst=R506IsolatedReplica::initialize(&root).unwrap();
  for i in 1..=n{
   src.append(&record(&format!("r546_event_{i:03}"),"decisions"),
    T+i,false).unwrap();
  }
  for i in 1..=n{dst.deliver(&src,i,false).unwrap();}
  for i in 1..=ack{dst.acknowledge(&src,i,false).unwrap();}
  (root,src,dst)
 }
 #[test]fn r546_attested_local_two_file_prefix_success(){
  let (root,src,dst)=setup(2,2);
  let id=dst.r546_preview_identity(&src).unwrap();
  assert!(id.starts_with("sha256:")&&id.len()==71);
  let got=dst.r546_stage(&src,&id,&gen()).unwrap();
  assert_eq!(got.boundary,2);
  assert_eq!(got.source_identity,id);
  assert!(got.db2_snapshot_matched);
  assert!(!got.independent_witness&&!got.permits_normal&&!got.permits_real);
  assert_eq!(dst.r546_verify(&src).unwrap(),got);
  drop(dst);drop(src);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r547_missing_source_ack_blocks_source_identity_and_staging(){
  let (root,src,dst)=setup(2,1);
  assert_eq!(dst.r546_preview_identity(&src),
    Err(EventOutboxR366Error::Conflict));
  assert_eq!(dst.r546_stage(&src,"sha256:bogus",&gen()),
    Err(EventOutboxR366Error::Conflict));
  drop(dst);drop(src);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r548_wrong_frozen_db1_identity_rejected(){
  let (root,src,dst)=setup(2,2);
  assert_eq!(dst.r546_stage(&src,"sha256:not-the-source",&gen()),
    Err(EventOutboxR366Error::Conflict));
  assert!(dst.r546_preview_identity(&src).is_ok());
  drop(dst);drop(src);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r549_legacy_or_short_generation_rejected(){
  let (root,src,dst)=setup(1,1);
  let id=dst.r546_preview_identity(&src).unwrap();
  for generation in ["legacy","rebase-bad","rebase-0000000000000000000000000000000x"]{
   assert_eq!(dst.r546_stage(&src,&id,generation),
     Err(EventOutboxR366Error::Schema));
  }
  drop(dst);drop(src);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r550_sink_payload_change_blocks_identity(){
  let (root,src,dst)=setup(1,1);
  let db=r506_db2_open(&root).unwrap();
  exec(&db,"UPDATE sink_delivery SET payload_hash='bad' WHERE seq=1").unwrap();
  drop(db);
  assert_eq!(dst.r546_preview_identity(&src),
    Err(EventOutboxR366Error::Corrupt));
  drop(dst);drop(src);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r551_source_outbox_mutation_blocks_stage(){
  let (root,src,dst)=setup(1,1);
  let id=dst.r546_preview_identity(&src).unwrap();
  let db=r366_open(&src.path).unwrap();
  exec(&db,"UPDATE mirror_outbox SET payload_hash='tampered' WHERE seq=1").unwrap();
  drop(db);
  assert_eq!(dst.r546_stage(&src,&id,&gen()),
    Err(EventOutboxR366Error::Corrupt));
  drop(dst);drop(src);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r552_source_changes_after_frozen_boundary_invalidate_local_proof(){
  let (root,src,dst)=setup(1,1);
  let id=dst.r546_preview_identity(&src).unwrap();
  dst.r546_stage(&src,&id,&gen()).unwrap();
  src.append(&record("r552_future","decisions"),T+2,false).unwrap();
  assert_eq!(dst.r546_verify(&src),
    Err(EventOutboxR366Error::Corrupt));
  assert_eq!(src.audit().unwrap().last_seq,2);
  drop(dst);drop(src);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r553_duplicate_local_proof_cannot_override_generation(){
  let (root,src,dst)=setup(1,1);
  let id=dst.r546_preview_identity(&src).unwrap();
  dst.r546_stage(&src,&id,&gen()).unwrap();
  assert_eq!(dst.r546_stage(&src,&id,&gen()),
    Err(EventOutboxR366Error::Duplicate));
  assert_eq!(dst.r546_verify(&src).unwrap().boundary,1);
  drop(dst);drop(src);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r554_two_files_reopen_and_reverify_frozen_identity(){
  let (root,src,dst)=setup(2,2);
  let id=dst.r546_preview_identity(&src).unwrap();
  let prior=dst.r546_stage(&src,&id,&gen()).unwrap();
  drop(dst);drop(src);
  let src=IsolatedEventOutboxR366::reopen(&root).unwrap();
  let dst=R506IsolatedReplica::reopen(&root).unwrap();
  assert_eq!(dst.r546_verify(&src).unwrap(),prior);
  assert!(!prior.independent_witness&&!prior.permits_normal);
  drop(dst);drop(src);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r555_forged_candidate_digest_detected_without_real_authority(){
  let (root,src,dst)=setup(1,1);
  let id=dst.r546_preview_identity(&src).unwrap();
  dst.r546_stage(&src,&id,&gen()).unwrap();
  let db=r506_db2_open(&root).unwrap();
  exec(&db,"UPDATE r546_private_baseline SET proof_hash='forged' WHERE id=1")
    .unwrap();
  drop(db);
  assert_eq!(dst.r546_verify(&src),
    Err(EventOutboxR366Error::Corrupt));
  assert_eq!((src.audit().unwrap().events,dst.audit().unwrap()),(1,1));
  drop(dst);drop(src);fs::remove_dir_all(root).unwrap();
 }
}
