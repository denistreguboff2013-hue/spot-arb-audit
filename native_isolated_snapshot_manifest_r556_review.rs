// R556-R565: three complete SYNTHETIC tables on two isolated SQLite files.
// This is not the original 17-table production DB1->DB2 snapshot.
const R556_TABLES:[&str;3]=[
 "r556_fixture_balances","r556_fixture_events","r556_fixture_metadata"
];
fn r556_fixture_schema(db:&Db)->Result<(),EventOutboxR366Error>{
 exec(db,"CREATE TABLE r556_fixture_balances(
  key TEXT PRIMARY KEY NOT NULL,payload_json TEXT NOT NULL);
 CREATE TABLE r556_fixture_events(
  key TEXT PRIMARY KEY NOT NULL,payload_json TEXT NOT NULL);
 CREATE TABLE r556_fixture_metadata(
  key TEXT PRIMARY KEY NOT NULL,payload_json TEXT NOT NULL);")
 .map_err(r366_error)
}
fn r556_manifest(db:&Db,boundary:u64,generation:&str)
 ->Result<(String,u64),EventOutboxR366Error>{
 use serde_json::json;
 if boundary==0||boundary>4096||!r546_private_generation(generation){
  return Err(EventOutboxR366Error::Schema)
 }
 let mut manifest=Vec::new();
 let mut total=0_u64;
 for table in R556_TABLES{
  // Compile-time fixed table allowlist: NO dynamic user SQL identifiers.
  let mut schema=Stmt::new(db,
   "SELECT sql FROM sqlite_master WHERE type='table' AND name=?1")
    .map_err(r366_error)?;
  schema.string(1,table).map_err(r366_error)?;
  if schema.step().map_err(r366_error)?!=SQLITE_ROW{
   return Err(EventOutboxR366Error::Schema)
  }
  let ddl=schema.strval(0).map_err(r366_error)?;
  if schema.step().map_err(r366_error)?!=SQLITE_DONE{
   return Err(EventOutboxR366Error::Corrupt)
  }
  drop(schema);
  // Table names are selected only from R556_TABLES.
  let sql=format!("SELECT key,payload_json FROM {table} ORDER BY key COLLATE BINARY");
  let mut q=Stmt::new(db,&sql).map_err(r366_error)?;
  let mut rows=Vec::new();let mut bytes=0_usize;
  loop{
   match q.step().map_err(r366_error)?{
    SQLITE_DONE=>break,
    SQLITE_ROW=>{
     let key=q.strval(0).map_err(r366_error)?;
     let raw=q.strval(1).map_err(r366_error)?;
     if key.is_empty()||key.len()>256||raw.len()>65536{
      return Err(EventOutboxR366Error::Schema)
     }
     bytes=bytes.checked_add(key.len()+raw.len()).ok_or(EventOutboxR366Error::Schema)?;
     if bytes>1_000_000||rows.len()>=128{return Err(EventOutboxR366Error::Schema)}
     let parsed:serde_json::Value=serde_json::from_str(&raw)
       .map_err(|_|EventOutboxR366Error::Corrupt)?;
     if !parsed.is_object()||r386_python_mirror_canonical(&parsed)?!=raw{
      return Err(EventOutboxR366Error::Corrupt)
     }
     rows.push(json!({"key":key,"row_json":raw,"row_sha256":r366_sha(&raw)}));
    },
    _=>return Err(EventOutboxR366Error::Corrupt)
   }
  }
  total=total.checked_add(rows.len() as u64).ok_or(EventOutboxR366Error::Schema)?;
  manifest.push(json!({"table":table,"ddl":ddl,"row_count":rows.len(),"rows":rows}));
 }
 let frame=json!({"kind":"R556_SYNTHETIC_FULL_FIXTURE_ONLY",
  "boundary":boundary,"feed_generation":generation,
  "table_count":R556_TABLES.len(),"total_rows":total,"tables":manifest});
 let canonical=r386_python_mirror_canonical(&frame)?;
 Ok((format!("sha256:{}",r366_sha(&canonical)),total))
}
#[derive(Debug,Clone,PartialEq,Eq)]
struct R556LocalSnapshot{
 boundary:u64,table_count:u64,total_rows:u64,
 manifest_sha256:String,independent_witness:bool,
 permits_normal:bool,permits_real:bool
}
impl R506IsolatedReplica{
 fn r556_snapshot_compare(&self,source:&IsolatedEventOutboxR366)
  ->Result<R556LocalSnapshot,EventOutboxR366Error>{
  let proof=self.r546_verify(source)?;
  let db1=r366_open(&source.path)?;
  let db2=r506_db2_open(&self.root)?;
  let (left,n1)=r556_manifest(&db1,proof.boundary,&proof.generation)?;
  let (right,n2)=r556_manifest(&db2,proof.boundary,&proof.generation)?;
  if n1==0||n1!=n2||left!=right{
   return Err(EventOutboxR366Error::Conflict)
  }
  Ok(R556LocalSnapshot{boundary:proof.boundary,table_count:3,
   total_rows:n1,manifest_sha256:left,
   independent_witness:false,permits_normal:false,permits_real:false})
 }
 fn r556_snapshot_stage(&self,source:&IsolatedEventOutboxR366,
  expected:&str,fail_before_commit:bool)
  ->Result<R556LocalSnapshot,EventOutboxR366Error>{
  let checked=self.r556_snapshot_compare(source)?;
  if checked.manifest_sha256!=expected{
   return Err(EventOutboxR366Error::Conflict)
  }
  let db1=r366_open(&source.path)?;
  let frozen=Tx::begin(&db1).map_err(r366_error)?;
  let db2=r506_db2_open(&self.root)?;
  let write=Tx::begin(&db2).map_err(r366_error)?;
  if r366_query_num(&db1,
    "SELECT latest_seq FROM mirror_control WHERE id=1")?!=checked.boundary||
     r366_query_num(&db2,
    "SELECT applied_seq FROM sink_control WHERE id=1")?!=checked.boundary||
     r506_source_ack_audit(&db1)?!=checked.boundary{
   return Err(EventOutboxR366Error::Conflict)
  }
  // No writer can mutate the snapshot rows while both IMMEDIATE TXs are held.
  let proof=self.r546_verify_locked(&db1,&db2,checked.boundary)?;
  let (left,n1)=r556_manifest(&db1,checked.boundary,&proof)?;
  let (right,n2)=r556_manifest(&db2,checked.boundary,&proof)?;
  if n1!=n2||n1!=checked.total_rows||
    left!=right||left!=expected{return Err(EventOutboxR366Error::Conflict)}
  exec(&db2,"CREATE TABLE IF NOT EXISTS r556_private_snapshot_proof(
   id INTEGER PRIMARY KEY CHECK(id=1),boundary INTEGER NOT NULL,
   feed_generation TEXT NOT NULL,total_rows INTEGER NOT NULL,
   manifest_hash TEXT NOT NULL,proof_hash TEXT NOT NULL);")
     .map_err(r366_error)?;
  if r366_query_num(&db2,
    "SELECT COUNT(*) FROM r556_private_snapshot_proof")?!=0{
    return Err(EventOutboxR366Error::Duplicate)
  }
  let digest=r366_sha(&format!(
   "r556:{}:{}:{}:{}",checked.boundary,proof,n1,left));
  let mut q=Stmt::new(&db2,"INSERT INTO r556_private_snapshot_proof
   (id,boundary,feed_generation,total_rows,manifest_hash,proof_hash)
   VALUES(1,?1,?2,?3,?4,?5)").map_err(r366_error)?;
  q.number(1,checked.boundary).map_err(r366_error)?;
  q.string(2,&proof).map_err(r366_error)?;
  q.number(3,n1).map_err(r366_error)?;
  q.string(4,&left).map_err(r366_error)?;
  q.string(5,&digest).map_err(r366_error)?;
  if q.step().map_err(r366_error)?!=SQLITE_DONE{
    return Err(EventOutboxR366Error::Sql)
  }
  drop(q);
  if fail_before_commit{return Err(EventOutboxR366Error::Interrupted)}
  write.commit().map_err(r366_error)?;
  frozen.commit().map_err(r366_error)?;
  Ok(checked)
 }
 fn r546_verify_locked(&self,db1:&Db,db2:&Db,boundary:u64)
  ->Result<String,EventOutboxR366Error>{
  let mut q=Stmt::new(db2,"SELECT boundary,source_identity,
   feed_generation,proof_hash FROM r546_private_baseline WHERE id=1")
   .map_err(r366_error)?;
  if q.step().map_err(r366_error)?!=SQLITE_ROW{
   return Err(EventOutboxR366Error::Corrupt)
  }
  let seq=q.num(0).map_err(r366_error)?;
  let identity=q.strval(1).map_err(r366_error)?;
  let generation=q.strval(2).map_err(r366_error)?;
  let hash=q.strval(3).map_err(r366_error)?;
  if q.step().map_err(r366_error)?!=SQLITE_DONE{
   return Err(EventOutboxR366Error::Corrupt)
  }
  if seq!=boundary||!r546_private_generation(&generation)||
   identity!=r546_local_prefix(db1,db2,boundary)?||
   hash!=r366_sha(&format!("r546:{generation}:{boundary}:{identity}")){
   return Err(EventOutboxR366Error::Corrupt)
  }
  Ok(generation)
 }
 fn r556_snapshot_verify(&self,source:&IsolatedEventOutboxR366)
  ->Result<R556LocalSnapshot,EventOutboxR366Error>{
  let compared=self.r556_snapshot_compare(source)?;
  let db=r506_db2_open(&self.root)?;
  let mut q=Stmt::new(&db,"SELECT boundary,feed_generation,total_rows,
   manifest_hash,proof_hash FROM r556_private_snapshot_proof WHERE id=1")
   .map_err(r366_error)?;
  if q.step().map_err(r366_error)?!=SQLITE_ROW{
   return Err(EventOutboxR366Error::Corrupt)
  }
  let boundary=q.num(0).map_err(r366_error)?;
  let generation=q.strval(1).map_err(r366_error)?;
  let count=q.num(2).map_err(r366_error)?;
  let manifest=q.strval(3).map_err(r366_error)?;
  let hash=q.strval(4).map_err(r366_error)?;
  if q.step().map_err(r366_error)?!=SQLITE_DONE||
   boundary!=compared.boundary||count!=compared.total_rows||
   manifest!=compared.manifest_sha256||
   hash!=r366_sha(&format!("r556:{boundary}:{generation}:{count}:{manifest}")){
    return Err(EventOutboxR366Error::Corrupt)
  }
  let local=self.r546_verify(source)?;
  if generation!=local.generation{return Err(EventOutboxR366Error::Corrupt)}
  Ok(compared)
 }
}
#[cfg(test)]
mod r556_tests{
 use super::*;
 use super::r366_tests::{fixture_root,record};
 const T:u64=1_792_660_000_000;
 const GENERATION:&str="rebase-0123456789abcdef0123456789abcdef";
 fn fixture(db:&Db)->Result<(),EventOutboxR366Error>{
  r556_fixture_schema(db)?;
  exec(db,"INSERT INTO r556_fixture_balances VALUES
     ('ETH','{\"available\":\"1.25\"}'),
     ('SOL','{\"available\":\"11.50\"}');
   INSERT INTO r556_fixture_events VALUES
     ('event-1','{\"kind\":\"paper\",\"status\":\"filled\"}');
   INSERT INTO r556_fixture_metadata VALUES
     ('schema','{\"version\":1}');").map_err(r366_error)
 }
 fn setup()->(PathBuf,IsolatedEventOutboxR366,R506IsolatedReplica){
  let root=fixture_root();
  let src=IsolatedEventOutboxR366::initialize(&root).unwrap();
  let sink=R506IsolatedReplica::initialize(&root).unwrap();
  for i in 1..=2{
   src.append(&record(&format!("r556_evt_{i}"),"decisions"),
     T+i,false).unwrap();
   sink.deliver(&src,i,false).unwrap();
   sink.acknowledge(&src,i,false).unwrap();
  }
  let ident=sink.r546_preview_identity(&src).unwrap();
  sink.r546_stage(&src,&ident,GENERATION).unwrap();
  let db1=r366_open(&src.path).unwrap();
  let db2=r506_db2_open(&root).unwrap();
  fixture(&db1).unwrap();
  fixture(&db2).unwrap();
  drop(db1);drop(db2);
  (root,src,sink)
 }
 fn done(root:PathBuf,src:IsolatedEventOutboxR366,sink:R506IsolatedReplica){
  drop(src);drop(sink);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r556_full_three_table_fixture_and_saved_digest(){
  let (root,src,sink)=setup();
  let preview=sink.r556_snapshot_compare(&src).unwrap();
  assert_eq!(preview.table_count,3);
  assert_eq!(preview.total_rows,4);
  assert_eq!(preview.boundary,2);
  assert_eq!(preview.manifest_sha256.len(),71);
  assert!(!preview.independent_witness&&!preview.permits_normal&&!preview.permits_real);
  let staged=sink.r556_snapshot_stage(&src,&preview.manifest_sha256,false).unwrap();
  assert_eq!(staged,preview);
  assert_eq!(sink.r556_snapshot_verify(&src).unwrap(),preview);
  done(root,src,sink);
 }
 #[test]fn r557_wrong_expected_snapshot_identity_fails_before_write(){
  let (root,src,sink)=setup();
  assert_eq!(sink.r556_snapshot_stage(&src,"sha256:wrong",false),
    Err(EventOutboxR366Error::Conflict));
  assert!(sink.r556_snapshot_compare(&src).is_ok());
  done(root,src,sink);
 }
 #[test]fn r558_missing_db2_row_refuses_snapshot(){
  let (root,src,sink)=setup();
  let db=r506_db2_open(&root).unwrap();
  exec(&db,"DELETE FROM r556_fixture_balances WHERE key='SOL'").unwrap();
  drop(db);
  assert_eq!(sink.r556_snapshot_compare(&src),
    Err(EventOutboxR366Error::Conflict));
  done(root,src,sink);
 }
 #[test]fn r559_extra_db2_row_refuses_snapshot(){
  let (root,src,sink)=setup();
  let db=r506_db2_open(&root).unwrap();
  exec(&db,"INSERT INTO r556_fixture_balances VALUES
   ('USDT','{\"available\":\"9\"}')").unwrap();
  drop(db);
  assert_eq!(sink.r556_snapshot_compare(&src),
    Err(EventOutboxR366Error::Conflict));
  done(root,src,sink);
 }
 #[test]fn r560_changed_payload_equal_counts_refused(){
  let (root,src,sink)=setup();
  let db=r506_db2_open(&root).unwrap();
  exec(&db,"UPDATE r556_fixture_balances
   SET payload_json='{\"available\":\"1.26\"}' WHERE key='ETH'").unwrap();
  drop(db);
  assert_eq!(sink.r556_snapshot_compare(&src),
    Err(EventOutboxR366Error::Conflict));
  done(root,src,sink);
 }
 #[test]fn r561_equal_rows_but_changed_ddl_refused(){
  let (root,src,sink)=setup();
  let db=r506_db2_open(&root).unwrap();
  exec(&db,"ALTER TABLE r556_fixture_metadata ADD COLUMN note TEXT").unwrap();
  drop(db);
  assert_eq!(sink.r556_snapshot_compare(&src),
    Err(EventOutboxR366Error::Conflict));
  done(root,src,sink);
 }
 #[test]fn r562_row_insertion_order_does_not_change_hash(){
  let (root,src,sink)=setup();
  let before=sink.r556_snapshot_compare(&src).unwrap();
  let db=r506_db2_open(&root).unwrap();
  exec(&db,"DELETE FROM r556_fixture_balances;
   INSERT INTO r556_fixture_balances VALUES
   ('SOL','{\"available\":\"11.50\"}'),
   ('ETH','{\"available\":\"1.25\"}');").unwrap();
  drop(db);
  assert_eq!(sink.r556_snapshot_compare(&src).unwrap(),before);
  done(root,src,sink);
 }
 #[test]fn r563_fail_before_commit_rolls_back_local_proof(){
  let (root,src,sink)=setup();
  let expected=sink.r556_snapshot_compare(&src).unwrap();
  assert_eq!(sink.r556_snapshot_stage(&src,&expected.manifest_sha256,true),
    Err(EventOutboxR366Error::Interrupted));
  let db=r506_db2_open(&root).unwrap();
  let tables=r366_query_num(&db,"SELECT COUNT(*) FROM sqlite_master
   WHERE type='table' AND name='r556_private_snapshot_proof'").unwrap();
  assert_eq!(tables,0);
  drop(db);
  let staged=sink.r556_snapshot_stage(&src,&expected.manifest_sha256,false).unwrap();
  assert_eq!(staged,expected);
  done(root,src,sink);
 }
 #[test]fn r564_both_sqlite_files_reopen_and_verify_proof(){
  let (root,src,sink)=setup();
  let expected=sink.r556_snapshot_compare(&src).unwrap();
  sink.r556_snapshot_stage(&src,&expected.manifest_sha256,false).unwrap();
  drop(src);drop(sink);
  let src=IsolatedEventOutboxR366::reopen(&root).unwrap();
  let sink=R506IsolatedReplica::reopen(&root).unwrap();
  assert_eq!(sink.r556_snapshot_verify(&src).unwrap(),expected);
  let db=r506_db2_open(&root).unwrap();
  exec(&db,"UPDATE r556_private_snapshot_proof
     SET proof_hash='forged' WHERE id=1").unwrap();
  drop(db);
  assert_eq!(sink.r556_snapshot_verify(&src),
    Err(EventOutboxR366Error::Corrupt));
  done(root,src,sink);
 }
 #[test]fn r565_source_changes_after_staging_invalidate_proof(){
  let (root,src,sink)=setup();
  let p=sink.r556_snapshot_compare(&src).unwrap();
  sink.r556_snapshot_stage(&src,&p.manifest_sha256,false).unwrap();
  let db=r366_open(&src.path).unwrap();
  exec(&db,"UPDATE r556_fixture_metadata
    SET payload_json='{\"version\":2}' WHERE key='schema'").unwrap();
  drop(db);
  assert_eq!(sink.r556_snapshot_verify(&src),
    Err(EventOutboxR366Error::Conflict));
  assert_eq!((src.audit().unwrap().events,sink.audit().unwrap()),(2,2));
  done(root,src,sink);
 }
}
