// Astra R671+: file-backed ORIGINAL 17-table SQLite DB1/outbox/DB2 research.
// Strict private root /tmp/astra_r671_file_*, two fixed test filenames only.
// Never opens historical/production databases or sends exchange orders.
fn r671_guard(root:&Path,name:&str)->Result<PathBuf,EventOutboxR366Error>{
 use std::os::unix::fs::MetadataExt;
 if name!="r671-private-db1.sqlite"&&name!="r671-private-db2.sqlite"{
  return Err(EventOutboxR366Error::Path)
 }
 if root.parent()!=Some(std::env::temp_dir().as_path())||
    !root.file_name().and_then(|x|x.to_str()).is_some_and(|s|
       s.starts_with("astra_r671_file_")&&s.len()<108){
  return Err(EventOutboxR366Error::Path)
 }
 let m=fs::symlink_metadata(root).map_err(|_|EventOutboxR366Error::Path)?;
 if !m.file_type().is_dir()||m.mode()&0o077!=0||
    m.uid()!=unsafe{libc::geteuid()}{
   return Err(EventOutboxR366Error::Path)
 }
 let path=root.join(name);
 if let Ok(m)=fs::symlink_metadata(&path){
  if !m.file_type().is_file()||m.nlink()!=1||m.mode()&0o077!=0||
    m.uid()!=unsafe{libc::geteuid()}{
   return Err(EventOutboxR366Error::Path)
  }
 }
 Ok(path)
}
fn r671_open(root:&Path,name:&str,create:bool)
 ->Result<Db,EventOutboxR366Error>{
 use std::os::unix::fs::OpenOptionsExt;
 let path=r671_guard(root,name)?;
 if create{
  let f=fs::OpenOptions::new().write(true).create_new(true)
   .mode(0o600).open(&path).map_err(|_|EventOutboxR366Error::Path)?;
  f.sync_all().map_err(|_|EventOutboxR366Error::Sql)?;
  drop(f);
 }
 let name=CString::new(path.to_string_lossy().as_bytes())
  .map_err(|_|EventOutboxR366Error::Path)?;
 let mut ptr=std::ptr::null_mut();
 // SQLite READWRITE_EXISTING: creating a missing DB file is forbidden here.
 let rc=unsafe{sqlite3_open_v2(name.as_ptr(),&mut ptr,READWRITE_EXISTING,
   std::ptr::null())};
 if rc!=SQLITE_OK||ptr.is_null(){
  if !ptr.is_null(){unsafe{sqlite3_close(ptr);}}
  return Err(EventOutboxR366Error::Sql)
 }
 let db=Db(ptr);
 if unsafe{sqlite3_busy_timeout(db.0,1000)}!=SQLITE_OK{
  return Err(EventOutboxR366Error::Sql)
 }
 exec(&db,"PRAGMA journal_mode=WAL;PRAGMA synchronous=FULL;
  PRAGMA foreign_keys=ON;PRAGMA wal_autocheckpoint=10;")
  .map_err(r366_error)?;
 Ok(db)
}
fn r671_original_schema(db:&Db,v:&serde_json::Value)
 ->Result<(),EventOutboxR366Error>{
 r566_check_source_manifest(v)?;
 exec(db,"PRAGMA user_version=4;").map_err(r366_error)?;
 for schema in v["table_schema"].as_array().ok_or(EventOutboxR366Error::Schema)?{
  exec(db,schema["ddl"].as_str().ok_or(EventOutboxR366Error::Schema)?)
   .map_err(r366_error)?;
 }
 for schema in v["table_schema"].as_array().ok_or(EventOutboxR366Error::Schema)?{
  for idx in schema["indexes"].as_array().ok_or(EventOutboxR366Error::Schema)?{
   if let Some(ddl)=idx["definition"].as_str(){
    exec(db,ddl).map_err(r366_error)?;
   }
  }
  for trigger in schema["triggers"].as_array().ok_or(EventOutboxR366Error::Schema)?{
   exec(db,trigger[1].as_str().ok_or(EventOutboxR366Error::Schema)?)
    .map_err(r366_error)?;
  }
 }
 Ok(())
}
fn r671_file_pair(root:&Path)
 ->Result<(Db,Db,serde_json::Value),EventOutboxR366Error>{
 let v=r586_oracle();
 r566_check_source_manifest(&v)?;
 let db1=r671_open(root,"r671-private-db1.sqlite",true)?;
 let db2=r671_open(root,"r671-private-db2.sqlite",true)?;
 r671_original_schema(&db1,&v)?;
 r671_original_schema(&db2,&v)?;
 exec(&db1,"CREATE TABLE r659_source_control(
   id INTEGER PRIMARY KEY CHECK(id=1), latest_seq INTEGER NOT NULL,
   ack_seq INTEGER NOT NULL);
   INSERT INTO r659_source_control VALUES(1,0,0);
   CREATE TABLE r659_source_feed(
   seq INTEGER PRIMARY KEY,prev_seq INTEGER NOT NULL,
   payload_hash TEXT NOT NULL,payload_json TEXT NOT NULL);")
    .map_err(r366_error)?;
 exec(&db2,"CREATE TABLE r659_sink_control(
   id INTEGER PRIMARY KEY CHECK(id=1),applied_seq INTEGER NOT NULL,
   payload_hash TEXT NOT NULL);
   INSERT INTO r659_sink_control VALUES(1,0,'GENESIS');")
    .map_err(r366_error)?;
 // Bind destination to the original source inode and exact source manifest.
 let source_path=r671_guard(root,"r671-private-db1.sqlite")?;
 let m=fs::metadata(source_path).map_err(|_|EventOutboxR366Error::Path)?;
 use std::os::unix::fs::MetadataExt;
 let identity=format!("{}:{}",m.dev(),m.ino());
 exec(&db2,&format!("CREATE TABLE r671_source_witness(
  id INTEGER PRIMARY KEY CHECK(id=1),
  source_identity TEXT NOT NULL,source_schema_digest TEXT NOT NULL);
  INSERT INTO r671_source_witness VALUES(1,{},{});",
  r586_sql_literal(&serde_json::json!(identity))?,
  r586_sql_literal(&serde_json::json!(R566_SOURCE_MANIFEST_DIGEST))?,
 )).map_err(r366_error)?;
 let rootfd=fs::File::open(root).map_err(|_|EventOutboxR366Error::Path)?;
 rootfd.sync_all().map_err(|_|EventOutboxR366Error::Sql)?;
 Ok((db1,db2,v))
}
fn r671_reopen_pair(root:&Path)
 ->Result<(Db,Db,serde_json::Value),EventOutboxR366Error>{
 let v=r586_oracle();
 r566_check_source_manifest(&v)?;
 let src=r671_open(root,"r671-private-db1.sqlite",false)?;
 let dst=r671_open(root,"r671-private-db2.sqlite",false)?;
 r671_check_source_witness(root,&src,&dst)?;
 Ok((src,dst,v))
}
fn r683_verify_original_physical_schema(
 db:&Db,v:&serde_json::Value
)->Result<(),EventOutboxR366Error>{
 use std::collections::{BTreeMap,BTreeSet};
 let tables=v["table_schema"].as_array().ok_or(EventOutboxR366Error::Schema)?;
 let mut mirrored=BTreeSet::<String>::new();
 let mut expected=BTreeMap::<(String,String),String>::new();
 for item in tables{
  let name=item["table"].as_str().ok_or(EventOutboxR366Error::Schema)?;
  if !mirrored.insert(name.to_string()){return Err(EventOutboxR366Error::Schema)}
  expected.insert(("table".into(),name.into()),
   item["ddl"].as_str().ok_or(EventOutboxR366Error::Schema)?.into());
  for index in item["indexes"].as_array().ok_or(EventOutboxR366Error::Schema)?{
   if let Some(ddl)=index["definition"].as_str(){
    let key=index["name"].as_str().ok_or(EventOutboxR366Error::Schema)?;
    expected.insert(("index".into(),key.into()),ddl.into());
   }
  }
  for trigger in item["triggers"].as_array().ok_or(EventOutboxR366Error::Schema)?{
   let key=trigger[0].as_str().ok_or(EventOutboxR366Error::Schema)?;
   let sql=trigger[1].as_str().ok_or(EventOutboxR366Error::Schema)?;
   expected.insert(("trigger".into(),key.into()),sql.into());
  }
 }
 let mut actual=BTreeMap::<(String,String),String>::new();
 let mut q=Stmt::new(db,"SELECT type,name,tbl_name,sql FROM sqlite_master
  WHERE sql IS NOT NULL AND type IN ('table','index','trigger')")
    .map_err(r366_error)?;
 loop{
  match q.step().map_err(r366_error)?{
   SQLITE_DONE=>break,
   SQLITE_ROW=>{
    let kind=q.strval(0).map_err(r366_error)?;
    let name=q.strval(1).map_err(r366_error)?;
    let owner=q.strval(2).map_err(r366_error)?;
    let ddl=q.strval(3).map_err(r366_error)?;
    if mirrored.contains(&owner){
     if actual.insert((kind,name),ddl).is_some(){
      return Err(EventOutboxR366Error::Corrupt)
     }
    }else if kind=="table"&&!matches!(name.as_str(),
       "r659_source_control"|"r659_source_feed"|"r659_sink_control"|
       "r671_source_witness"|"sqlite_sequence"){
     return Err(EventOutboxR366Error::Corrupt)
    }else if kind!="table"{
     return Err(EventOutboxR366Error::Corrupt)
    }
   },
   _=>return Err(EventOutboxR366Error::Corrupt)
  }
 }
 if expected!=actual||r366_query_num(db,
   "SELECT COUNT(*) FROM pragma_foreign_key_check")?!=0{
  return Err(EventOutboxR366Error::Corrupt)
 }
 Ok(())
}
fn r671_check_source_witness(root:&Path,src:&Db,dst:&Db)
 ->Result<(),EventOutboxR366Error>{
 let m=fs::metadata(r671_guard(root,"r671-private-db1.sqlite")?)
  .map_err(|_|EventOutboxR366Error::Path)?;
 use std::os::unix::fs::MetadataExt;
 let identity=format!("{}:{}",m.dev(),m.ino());
 let mut q=Stmt::new(dst,"SELECT source_identity,source_schema_digest
  FROM r671_source_witness WHERE id=1").map_err(r366_error)?;
 if q.step().map_err(r366_error)?!=SQLITE_ROW{
  return Err(EventOutboxR366Error::Corrupt)
 }
 if q.strval(0).map_err(r366_error)?!=identity||
   q.strval(1).map_err(r366_error)?!=R566_SOURCE_MANIFEST_DIGEST{
  return Err(EventOutboxR366Error::Conflict)
 }
 if r366_query_num(src,"PRAGMA user_version")?!=4||
   r366_query_num(dst,"PRAGMA user_version")?!=4{
  return Err(EventOutboxR366Error::Schema)
 }
 let v=r586_oracle();
 r566_check_source_manifest(&v)?;
 r683_verify_original_physical_schema(src,&v)?;
 r683_verify_original_physical_schema(dst,&v)?;
 Ok(())
}

fn r671_replay_file(root:&Path,src:&Db,dst:&Db,
  v:&serde_json::Value,seq:u64,interrupt:bool)
 ->Result<(),EventOutboxR366Error>{
 r671_check_source_witness(root,src,dst)?;
 r659_replay(src,dst,v,seq,interrupt)
}
// R691: bounded, exact, SQL-typed physical row comparison BEFORE DB1
// cleanup. Quote() preserves INTEGER/TEXT/NULL distinctions in this v4
// source schema; deterministic key ordering avoids insertion-order false
// positives. No production path and no unbounded row scan is allowed.
fn r691_original_rows_match(src:&Db,dst:&Db,v:&serde_json::Value)
 ->Result<(),EventOutboxR366Error>{
 let mut total_rows=0_usize;
 let mut total_bytes=0_usize;
 for table in v["table_schema"].as_array().ok_or(EventOutboxR366Error::Schema)?{
  let name=table["table"].as_str().ok_or(EventOutboxR366Error::Schema)?;
  let fields=table["columns"].as_array().ok_or(EventOutboxR366Error::Schema)?;
  let names=fields.iter().map(|f|f[1].as_str())
   .collect::<Option<Vec<_>>>().ok_or(EventOutboxR366Error::Schema)?;
  let key=v["source_keys"][name].as_array().ok_or(EventOutboxR366Error::Schema)?;
  let cols=names.iter().map(|n|Ok(format!("quote({})",r586_identifier(n)?)))
   .collect::<Result<Vec<_>,EventOutboxR366Error>>()?.join(",");
  let order=key.iter().map(|n|r586_identifier(
    n.as_str().ok_or(EventOutboxR366Error::Schema)?))
    .collect::<Result<Vec<_>,_>>()?.join(",");
  let sql=format!("SELECT {cols} FROM {} ORDER BY {order}",r586_identifier(name)?);
  let rows=|db:&Db|->Result<Vec<Vec<String>>,EventOutboxR366Error>{
   let mut q=Stmt::new(db,&sql).map_err(r366_error)?;
   let mut out=Vec::new();
   loop{
    match q.step().map_err(r366_error)?{
     SQLITE_DONE=>break,
     SQLITE_ROW=>{
      if out.len()>=10_000{return Err(EventOutboxR366Error::Pressure)}
      let row=(0..names.len()).map(|i|q.strval(i as i32).map_err(r366_error))
       .collect::<Result<Vec<_>,_>>()?;
      out.push(row);
     },
     _=>return Err(EventOutboxR366Error::Corrupt)
    }
   }
   Ok(out)
  };
  let original=rows(src)?;
  let applied=rows(dst)?;
  total_rows=total_rows.saturating_add(original.len()).saturating_add(applied.len());
  total_bytes=total_bytes.saturating_add(original.iter().chain(applied.iter())
   .flat_map(|r|r.iter()).map(|s|s.len()).sum::<usize>());
  if total_rows>20_000||total_bytes>32*1024*1024{
   return Err(EventOutboxR366Error::Pressure)
  }
  if original!=applied{return Err(EventOutboxR366Error::Corrupt)}
 }
 Ok(())
}
fn r671_ack_file(root:&Path,src:&Db,dst:&Db,seq:u64)
 ->Result<(),EventOutboxR366Error>{
 r671_check_source_witness(root,src,dst)?;
 let v=r586_oracle();
 r691_original_rows_match(src,dst,&v)?;
 r659_ack_cleanup(src,dst,seq)
}
#[cfg(test)]
mod r671_tests{
 use super::*;
 use std::os::unix::fs::{DirBuilderExt,MetadataExt};
 use std::os::unix::process::ExitStatusExt;
 use std::process::Command;
 use serde_json::{json,Value};
 struct OwnerRoot(PathBuf);
 impl OwnerRoot{
  fn new()->Self{
   let mut root=std::env::temp_dir();
   let stamp=std::time::SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
   root.push(format!("astra_r671_file_{}_{}",std::process::id(),stamp));
   fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
   Self(root)
  }
 }
 impl Drop for OwnerRoot{
  fn drop(&mut self){let _=fs::remove_dir_all(&self.0);}
 }
 fn pair()->(OwnerRoot,Db,Db,Value){
  let root=OwnerRoot::new();
  let (src,dst,v)=r671_file_pair(&root.0).unwrap();
  (root,src,dst,v)
 }
 fn op(v:&Value,name:&str,kind:&str)->Value{
  let row=v["fixture_snapshot"]["tables"].as_array().unwrap().iter()
   .find(|t|t["schema"]["table"]==name).unwrap()["rows"][0]["row"].clone();
  r659_op(v,name,&row,kind).unwrap()
 }
 fn six(v:&Value)->Vec<Value>{
  ["attempts","collector_runs","events","decisions",
   "hourly_raw_rollups","metadata"].iter().map(|n|op(v,n,"insert")).collect()
 }
 fn count(db:&Db,table:&str)->u64{
  r366_query_num(db,&format!("SELECT COUNT(*) FROM {table}")).unwrap()
 }
 fn seed()->(OwnerRoot,Db,Db,Value){
  let (root,src,dst,v)=pair();
  r659_append(&src,&v,&six(&v)).unwrap();
  (root,src,dst,v)
 }
 #[test]fn r671_unsafe_path_or_filename_cannot_open(){
  let root=OwnerRoot::new();
  assert!(matches!(r671_open(&root.0,"arb_stats.sqlite",true),
    Err(EventOutboxR366Error::Path)));
  assert!(matches!(r671_open(Path::new("/home/tmpadmin"),"r671-private-db1.sqlite",false),
    Err(EventOutboxR366Error::Path)));
  assert!(matches!(r671_open(&root.0,"r671-private-db2.sqlite",false),
    Err(EventOutboxR366Error::Sql)));
 }
 #[test]fn r672_two_real_file_db_with_wal_full_and_17tables(){
  let (root,src,dst,v)=pair();
  for t in v["table_schema"].as_array().unwrap(){
   let name=t["table"].as_str().unwrap();
   assert_eq!(count(&src,name),0);
   assert_eq!(count(&dst,name),0);
  }
  for name in ["r671-private-db1.sqlite","r671-private-db2.sqlite"]{
   let m=fs::metadata(r671_guard(&root.0,name).unwrap()).unwrap();
   assert_eq!(m.mode()&0o077,0);
   assert!(m.len()>0);
  }
  assert_eq!(r366_query_num(&src,"PRAGMA synchronous").unwrap(),2);
  assert_eq!(r366_query_num(&dst,"PRAGMA synchronous").unwrap(),2);
  let mut q=Stmt::new(&src,"PRAGMA journal_mode").unwrap();
  assert_eq!(q.step().unwrap(),SQLITE_ROW);
  assert_eq!(q.strval(0).unwrap(),"wal");
 }
 #[test]fn r673_reopen_deliver_ack_reopen_original_17tables(){
  let (root,src,dst,v)=seed();
  drop(src);drop(dst);
  let (src,dst,v2)=r671_reopen_pair(&root.0).unwrap();
  assert_eq!(v,v2);
  r671_replay_file(&root.0,&src,&dst,&v,1,false).unwrap();
  drop(src);drop(dst);
  let (src,dst,_)=r671_reopen_pair(&root.0).unwrap();
  assert_eq!(count(&dst,"events"),1);
  assert_eq!(count(&dst,"decisions"),1);
  r671_ack_file(&root.0,&src,&dst,1).unwrap();
  drop(src);drop(dst);
  let (src,dst,_)=r671_reopen_pair(&root.0).unwrap();
  assert_eq!(count(&src,"r659_source_feed"),0);
  assert_eq!(count(&dst,"events"),1);
  assert_eq!(r366_query_num(&src,"SELECT ack_seq FROM r659_source_control").unwrap(),1);
 }
 #[test]fn r674_interrupted_sink_tx_reopen_does_not_ack_or_partial_row(){
  let (root,src,dst,v)=seed();
  assert_eq!(r671_replay_file(&root.0,&src,&dst,&v,1,true),
   Err(EventOutboxR366Error::Interrupted));
  drop(src);drop(dst);
  let (src,dst,v)=r671_reopen_pair(&root.0).unwrap();
  assert_eq!(count(&dst,"events"),0);
  assert_eq!(count(&dst,"decisions"),0);
  assert_eq!(r366_query_num(&dst,"SELECT applied_seq FROM r659_sink_control").unwrap(),0);
  r671_replay_file(&root.0,&src,&dst,&v,1,false).unwrap();
 }
 fn child_phase(test_name:&str,phase:&str,root:&Path){
  let result=Command::new(std::env::current_exe().unwrap())
   .args(["--exact",test_name,"--nocapture"])
   .env("ASTRA_R671_CHILD_PHASE",phase)
   .env("ASTRA_R671_PRIVATE_ROOT",root)
   .output().unwrap();
  assert_eq!(result.status.signal(),Some(libc::SIGKILL),
   "only our own isolated child may SIGKILL itself");
 }
 fn maybe_child(expected:&str)->bool{
  if std::env::var("ASTRA_R671_CHILD_PHASE").ok().as_deref()!=Some(expected){
   return false
  }
  let root=PathBuf::from(std::env::var("ASTRA_R671_PRIVATE_ROOT").unwrap());
  let (src,dst,v)=r671_reopen_pair(&root).unwrap();
  if expected=="MID_DB2"{
   let (_prev,_hash,raw)=r659_source_frame(&src,1).unwrap();
   let frame:Value=serde_json::from_str(&raw).unwrap();
   let _tx=Tx::begin(&dst).unwrap();
   r586_apply_original_operation(&dst,&v,&frame["operations"][0]).unwrap();
   assert_eq!(count(&dst,"attempts"),1);
  }else{
   r671_replay_file(&root,&src,&dst,&v,1,false).unwrap();
   assert_eq!(count(&dst,"events"),1);
  }
  unsafe{libc::raise(libc::SIGKILL);}
  panic!("SIGKILL did not terminate private child")
 }
 #[test]fn r675_sigkill_mid_db2_rolls_back_after_reopen(){
  if maybe_child("MID_DB2"){return}
  let (root,src,dst,_v)=seed();
  drop(src);drop(dst);
  child_phase("native_isolated_sqlite_mirror_r147_review::r671_tests::r675_sigkill_mid_db2_rolls_back_after_reopen",
    "MID_DB2",&root.0);
  let (src,dst,v)=r671_reopen_pair(&root.0).unwrap();
  assert_eq!(count(&dst,"attempts"),0);
  assert_eq!(count(&dst,"events"),0);
  r671_replay_file(&root.0,&src,&dst,&v,1,false).unwrap();
  assert_eq!(count(&dst,"events"),1);
 }
 #[test]fn r676_sigkill_after_db2_commit_replays_ack_only(){
  if maybe_child("AFTER_DB2"){return}
  let (root,src,dst,_)=seed();
  drop(src);drop(dst);
  child_phase("native_isolated_sqlite_mirror_r147_review::r671_tests::r676_sigkill_after_db2_commit_replays_ack_only",
    "AFTER_DB2",&root.0);
  let (src,dst,v)=r671_reopen_pair(&root.0).unwrap();
  assert_eq!(count(&dst,"events"),1);
  assert_eq!(r671_replay_file(&root.0,&src,&dst,&v,1,false),
   Err(EventOutboxR366Error::Duplicate));
  r671_ack_file(&root.0,&src,&dst,1).unwrap();
  assert_eq!(count(&src,"r659_source_feed"),0);
 }
 #[test]fn r677_source_inode_witness_forgery_refuses_reopen(){
  let (root,src,dst,_)=seed();
  exec(&dst,"UPDATE r671_source_witness SET source_identity='fake'").unwrap();
  drop(src);drop(dst);
  assert!(matches!(r671_reopen_pair(&root.0),Err(EventOutboxR366Error::Conflict)));
 }
 #[test]fn r678_unauthorized_user_version_change_fails_closed(){
  let (root,src,dst,_)=seed();
  exec(&dst,"PRAGMA user_version=5").unwrap();
  drop(src);drop(dst);
  assert!(matches!(r671_reopen_pair(&root.0),Err(EventOutboxR366Error::Schema)));
 }
 #[test]fn r679_reopened_source_frame_payload_tamper_blocks_sink(){
  let (root,src,dst,_)=seed();
  exec(&src,"UPDATE r659_source_feed SET payload_hash='0' WHERE seq=1").unwrap();
  drop(src);drop(dst);
  let (src,dst,v)=r671_reopen_pair(&root.0).unwrap();
  assert_eq!(r671_replay_file(&root.0,&src,&dst,&v,1,false),
   Err(EventOutboxR366Error::Corrupt));
  assert_eq!(count(&dst,"events"),0);
 }
 #[test]fn r680_missing_private_db2_refuses_reopen_no_implicit_rebuild(){
  let (root,src,dst,_)=seed();
  drop(src);drop(dst);
  fs::remove_file(r671_guard(&root.0,"r671-private-db2.sqlite").unwrap()).unwrap();
  assert!(r671_reopen_pair(&root.0).is_err());
 }
 #[test]fn r681_duplicate_reopened_delivery_no_extra_rows(){
  let (root,src,dst,v)=seed();
  r671_replay_file(&root.0,&src,&dst,&v,1,false).unwrap();
  drop(src);drop(dst);
  let (src,dst,v)=r671_reopen_pair(&root.0).unwrap();
  assert_eq!(r671_replay_file(&root.0,&src,&dst,&v,1,false),
   Err(EventOutboxR366Error::Duplicate));
  assert_eq!(count(&dst,"events"),1);
 }
 #[test]fn r682_8part_rollup_upsert_survives_wal_reopen(){
  let (root,src,dst,v)=seed();
  r671_replay_file(&root.0,&src,&dst,&v,1,false).unwrap();
  r671_ack_file(&root.0,&src,&dst,1).unwrap();
  let mut row=v["fixture_snapshot"]["tables"].as_array().unwrap().iter()
   .find(|t|t["schema"]["table"]=="hourly_raw_rollups").unwrap()
   ["rows"][0]["row"].clone();
  row["event_count"]=json!(25);
  r659_append(&src,&v,&[r659_op(&v,"hourly_raw_rollups",&row,"upsert").unwrap()])
   .unwrap();
  drop(src);drop(dst);
  let (src,dst,v)=r671_reopen_pair(&root.0).unwrap();
  r671_replay_file(&root.0,&src,&dst,&v,2,false).unwrap();
  assert_eq!(count(&dst,"hourly_raw_rollups"),1);
  assert_eq!(r366_query_num(&dst,"SELECT COUNT(*) FROM pragma_foreign_key_check").unwrap(),0);
 }
 #[test]fn r683_original_stored_sql_matches_exact_17table_manifest(){
  let (root,src,dst,_)=pair();
  r671_check_source_witness(&root.0,&src,&dst).unwrap();
  let v=r586_oracle();
  r683_verify_original_physical_schema(&src,&v).unwrap();
  r683_verify_original_physical_schema(&dst,&v).unwrap();
 }
 #[test]fn r684_added_db2_index_is_never_silent(){
  let (root,src,dst,_)=pair();
  exec(&dst,"CREATE INDEX r684_forged ON metadata(value)").unwrap();
  drop(src);drop(dst);
  assert!(matches!(r671_reopen_pair(&root.0),Err(EventOutboxR366Error::Corrupt)));
 }
 #[test]fn r685_missing_db2_original_index_refuses_reopen(){
  let (root,src,dst,_)=pair();
  exec(&dst,"DROP INDEX advisor_transitions_recommendation").unwrap();
  drop(src);drop(dst);
  assert!(matches!(r671_reopen_pair(&root.0),Err(EventOutboxR366Error::Corrupt)));
 }
 #[test]fn r686_added_db1_index_is_never_silent(){
  let (root,src,dst,_)=pair();
  exec(&src,"CREATE INDEX r686_forged ON metadata(value)").unwrap();
  drop(src);drop(dst);
  assert!(matches!(r671_reopen_pair(&root.0),Err(EventOutboxR366Error::Corrupt)));
 }
 #[test]fn r687_missing_original_immutable_trigger_refuses(){
  let (root,src,dst,_)=pair();
  exec(&dst,"DROP TRIGGER advisor_transitions_no_delete").unwrap();
  drop(src);drop(dst);
  assert!(matches!(r671_reopen_pair(&root.0),Err(EventOutboxR366Error::Corrupt)));
 }
 #[test]fn r688_altered_original_table_definition_refuses(){
  let (root,src,dst,_)=pair();
  exec(&dst,"ALTER TABLE metadata ADD COLUMN unexpected TEXT").unwrap();
  drop(src);drop(dst);
  assert!(matches!(r671_reopen_pair(&root.0),Err(EventOutboxR366Error::Corrupt)));
 }
 #[test]fn r689_foreign_key_violation_written_offline_refuses_reopen(){
  let (root,src,dst,v)=seed();
  r671_replay_file(&root.0,&src,&dst,&v,1,false).unwrap();
  exec(&dst,"PRAGMA foreign_keys=OFF; DELETE FROM events
    WHERE event_id='r566_event'; PRAGMA foreign_keys=ON;").unwrap();
  assert_eq!(r366_query_num(&dst,
   "SELECT COUNT(*) FROM pragma_foreign_key_check").unwrap(),1);
  drop(src);drop(dst);
  assert!(matches!(r671_reopen_pair(&root.0),Err(EventOutboxR366Error::Corrupt)));
 }
 #[test]fn r690_extra_unapproved_sqlite_table_refuses_reopen(){
  let (root,src,dst,_)=pair();
  exec(&dst,"CREATE TABLE r690_extra(value TEXT)").unwrap();
  drop(src);drop(dst);
  assert!(matches!(r671_reopen_pair(&root.0),Err(EventOutboxR366Error::Corrupt)));
 }
 #[test]fn r691_row_equality_required_before_source_cleanup(){
  let (root,src,dst,v)=seed();
  r671_replay_file(&root.0,&src,&dst,&v,1,false).unwrap();
  r691_original_rows_match(&src,&dst,&v).unwrap();
  r671_ack_file(&root.0,&src,&dst,1).unwrap();
  assert_eq!(count(&src,"r659_source_feed"),0);
 }
 #[test]fn r692_db2_row_tamper_after_ack_blocks_cleanup(){
  let (root,src,dst,v)=seed();
  r671_replay_file(&root.0,&src,&dst,&v,1,false).unwrap();
  exec(&dst,"UPDATE metadata SET value='UNATTESTED_DB2_TAMPER'").unwrap();
  assert_eq!(r671_ack_file(&root.0,&src,&dst,1),
   Err(EventOutboxR366Error::Corrupt));
  assert_eq!(count(&src,"r659_source_feed"),1);
 }
 #[test]fn r693_missing_db2_data_row_blocks_cleanup(){
  let (root,src,dst,v)=seed();
  r671_replay_file(&root.0,&src,&dst,&v,1,false).unwrap();
  exec(&dst,"DELETE FROM metadata").unwrap();
  assert_eq!(r671_ack_file(&root.0,&src,&dst,1),
   Err(EventOutboxR366Error::Corrupt));
  assert_eq!(count(&src,"r659_source_feed"),1);
 }
 #[test]fn r694_unlogged_db1_mutation_blocks_cleanup(){
  let (root,src,dst,v)=seed();
  r671_replay_file(&root.0,&src,&dst,&v,1,false).unwrap();
  exec(&src,"UPDATE metadata SET value='UNLOGGED_DB1_CHANGE'").unwrap();
  assert_eq!(r671_ack_file(&root.0,&src,&dst,1),
   Err(EventOutboxR366Error::Corrupt));
  assert_eq!(count(&src,"r659_source_feed"),1);
 }
 #[test]fn r695_rollup_8part_key_value_drift_blocks_cleanup(){
  let (root,src,dst,v)=seed();
  r671_replay_file(&root.0,&src,&dst,&v,1,false).unwrap();
  exec(&dst,"UPDATE hourly_raw_rollups SET event_count=999").unwrap();
  assert_eq!(r671_ack_file(&root.0,&src,&dst,1),
   Err(EventOutboxR366Error::Corrupt));
  assert_eq!(count(&src,"r659_source_feed"),1);
 }
}
