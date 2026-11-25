// Astra P0-01 R800: memory-bounded row-for-row physical comparison of
// the ORIGINAL 17-table schema. No 10,000-row per table / 20,000-row
// overall ceiling of the research-only R691 vector materialization.
// Never opens production SQLite; callers own and lock their connections.
fn r800_streaming_original_parity(
 source:&Db,sink:&Db,manifest:&serde_json::Value
)->Result<u64,EventOutboxR366Error>{
 let tables=manifest["table_schema"].as_array()
  .ok_or(EventOutboxR366Error::Schema)?;
 if tables.len()!=17{return Err(EventOutboxR366Error::Schema)}
 let mut total=0u64;
 for item in tables{
  let table=item["table"].as_str().ok_or(EventOutboxR366Error::Schema)?;
  let columns=item["columns"].as_array().ok_or(EventOutboxR366Error::Schema)?;
  let keys=manifest["source_keys"][table].as_array()
   .ok_or(EventOutboxR366Error::Schema)?;
  if columns.is_empty()||columns.len()>256||keys.is_empty(){
   return Err(EventOutboxR366Error::Schema)
  }
  let mut cols=Vec::new();
  for col in columns{
   let name=col[1].as_str().ok_or(EventOutboxR366Error::Schema)?;
   cols.push(format!("quote({})",r586_identifier(name)?));
  }
  let order=keys.iter().map(|key|r586_identifier(
   key.as_str().ok_or(EventOutboxR366Error::Schema)?))
   .collect::<Result<Vec<_>,_>>()?.join(",");
  let sql=format!("SELECT {} FROM {} ORDER BY {order}",
   cols.join(","),r586_identifier(table)?);
  let mut left=Stmt::new(source,&sql).map_err(r366_error)?;
  let mut right=Stmt::new(sink,&sql).map_err(r366_error)?;
  loop{
   let a=left.step().map_err(r366_error)?;
   let b=right.step().map_err(r366_error)?;
   if a==SQLITE_DONE && b==SQLITE_DONE{break}
   if a!=SQLITE_ROW||b!=SQLITE_ROW{
    return Err(EventOutboxR366Error::Corrupt)
   }
   total=total.checked_add(1).ok_or(EventOutboxR366Error::Pressure)?;
   if total>10_000_000{return Err(EventOutboxR366Error::Pressure)}
   for column in 0..cols.len(){
    let l=left.strval(column as i32).map_err(r366_error)?;
    let r=right.strval(column as i32).map_err(r366_error)?;
    if l.len()>4*1024*1024||r.len()>4*1024*1024{
     return Err(EventOutboxR366Error::Pressure)
    }
    if l!=r{return Err(EventOutboxR366Error::Corrupt)}
   }
  }
 }
 Ok(total)
}
// R804: true bounded-memory snapshot copier for initial bootstrap.
// Caller holds an immutable source WAL snapshot and an uncommitted
// destination transaction. Full table is never materialized in a Vec.
fn r804_stream_original_table(
 src:&Db,dst:&Db,manifest:&serde_json::Value,table:&str
)->Result<u64,EventOutboxR366Error>{
 let schema=manifest["table_schema"].as_array()
  .ok_or(EventOutboxR366Error::Schema)?
  .iter().find(|row|row["table"]==table)
  .ok_or(EventOutboxR366Error::Schema)?;
 let columns=schema["columns"].as_array()
  .ok_or(EventOutboxR366Error::Schema)?;
 let keys=manifest["source_keys"][table].as_array()
  .ok_or(EventOutboxR366Error::Schema)?;
 if columns.is_empty()||columns.len()>256||keys.is_empty(){
  return Err(EventOutboxR366Error::Schema)
 }
 let projection=columns.iter().map(|c|{
   let n=c[1].as_str().ok_or(EventOutboxR366Error::Schema)?;
   Ok(format!("'{}',{}",n,r586_identifier(n)?))
 }).collect::<Result<Vec<_>,EventOutboxR366Error>>()?.join(",");
 let order=keys.iter().map(|key|r586_identifier(
  key.as_str().ok_or(EventOutboxR366Error::Schema)?))
  .collect::<Result<Vec<_>,_>>()?.join(",");
 let sql=format!("SELECT json_object({projection}) FROM {} ORDER BY {order}",
  r586_identifier(table)?);
 let mut statement=Stmt::new(src,&sql).map_err(r366_error)?;
 let mut copied=0u64;
 loop{
  match statement.step().map_err(r366_error)?{
   SQLITE_DONE=>break,
   SQLITE_ROW=>{
    copied=copied.checked_add(1).ok_or(EventOutboxR366Error::Pressure)?;
    if copied>10_000_000{return Err(EventOutboxR366Error::Pressure)}
    let raw=statement.strval(0).map_err(r366_error)?;
    if raw.len()>4*1024*1024{return Err(EventOutboxR366Error::Pressure)}
    let row:serde_json::Value=serde_json::from_str(&raw)
     .map_err(|_|EventOutboxR366Error::Corrupt)?;
    let op=r659_op(manifest,table,&row,"insert")?;
    r586_apply_original_operation(dst,manifest,&op)?;
   }
   _=>return Err(EventOutboxR366Error::Corrupt)
  }
 }
 Ok(copied)
}
#[cfg(test)]
mod r800_tests {
 use super::*;
 use std::os::unix::fs::DirBuilderExt;
 struct Private(PathBuf);
 impl Private {
  fn new()->Self{
   let mut dir=std::env::temp_dir();
   let n=std::time::SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
   dir.push(format!("astra_r671_file_{}_{}_r800",std::process::id(),n));
   fs::DirBuilder::new().mode(0o700).create(&dir).unwrap();
   Self(dir)
  }
 }
 impl Drop for Private{fn drop(&mut self){let _=fs::remove_dir_all(&self.0);}}
 fn pair()->(Private,Db,Db,serde_json::Value){
  let dir=Private::new();
  let (a,b,v)=r756_initialize(&dir.0).unwrap();
  (dir,a,b,v)
 }
 fn fill(db:&Db){
  exec(db,"WITH RECURSIVE n(i) AS (
    SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<12050
   ) INSERT INTO metadata(key,value)
     SELECT printf('r800_key_%06d',i),printf('r800_value_%06d',i)
     FROM n").unwrap();
 }
 #[test]fn r800_more_than_10000_original_rows_stream_identically(){
  let (_dir,src,dst,v)=pair();
  fill(&src);fill(&dst);
  assert_eq!(r800_streaming_original_parity(&src,&dst,&v),Ok(12050));
  assert_eq!(r691_original_rows_match(&src,&dst,&v),
   Err(EventOutboxR366Error::Pressure));
 }
 #[test]fn r801_single_changed_row_in_long_stream_denies_ack(){
  let (_dir,src,dst,v)=pair();
  fill(&src);fill(&dst);
  exec(&dst,"UPDATE metadata SET value='TAMPERED'
    WHERE key='r800_key_012050'").unwrap();
  assert_eq!(r800_streaming_original_parity(&src,&dst,&v),
   Err(EventOutboxR366Error::Corrupt));
 }
 #[test]fn r802_missing_row_in_long_stream_denies_ack(){
  let (_dir,src,dst,v)=pair();
  fill(&src);fill(&dst);
  exec(&dst,"DELETE FROM metadata
    WHERE key='r800_key_012050'").unwrap();
  assert_eq!(r800_streaming_original_parity(&src,&dst,&v),
   Err(EventOutboxR366Error::Corrupt));
 }
 #[test]fn r803_bootstrap_copies_more_than_10000_original_schema_rows(){
  let (dir,src,dst,v)=pair();
  let original=v["fixture_snapshot"]["tables"].as_array().unwrap().iter()
   .find(|x|x["schema"]["table"]=="metadata").unwrap()["rows"][0]["row"].clone();
  let op=r659_op(&v,"metadata",&original,"insert").unwrap();
  assert_eq!(r756_append(&dir.0,&src,&dst,&v,&[op]),Ok(1));
  fill(&src); // intentionally isolated large pre-existing snapshot fixture
  r790_arm_private_baseline(&dir.0,&src,&dst).unwrap();
  r790_copy_wal_snapshot(&dir.0,&src,&dst,&v,||{},false).unwrap();
  assert_eq!(r366_query_num(&dst,"SELECT COUNT(*) FROM metadata").unwrap(),12051);
  assert_eq!(r790_replay_cutover(&dir.0,&src,&dst,&v),Ok(0));
  assert_eq!(r756_cleanup(&dir.0,&src,&dst,&v),Ok((1,0)));
  assert_eq!(r800_streaming_original_parity(&src,&dst,&v),Ok(12051));
 }
}
