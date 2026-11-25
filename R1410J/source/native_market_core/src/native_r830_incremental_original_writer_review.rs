// Astra P0-01 R830 incremental original 17-table PRIVATE SQLite writer.
// Fast source append verifies metadata and the chain TAIL. Full historical
// chain validation MUST run in R805 before applying/ACKing DB2. Research-only
// R671 root, source and destination handles, NEVER historical production DBs.
fn r830_incremental_original_append(
 root:&Path,src:&Db,dst:&Db,manifest:&serde_json::Value,
 operations:&[serde_json::Value]
)->Result<u64,EventOutboxR366Error>{
 if operations.is_empty()||operations.len()>32{
  return Err(EventOutboxR366Error::Pressure)
 }
 let r1130_t=std::time::Instant::now();
 let tx=Tx::begin(src).map_err(r366_error)?;
 let controls=r756_read_json(src,"mirror_control",R726_CONTROL,"WHERE id=1")?;
 let metas=r756_read_json(dst,"mirror_meta",R726_META,"WHERE id=1")?;
 if controls.len()!=1||metas.len()!=1{return Err(EventOutboxR366Error::Corrupt)}
 let control=&controls[0];let meta=&metas[0];
 r726_shape(control,R726_CONTROL)?;
 r726_shape(meta,R726_META)?;
 let latest=r726_u(control,"latest_seq")?;
 let applied=r726_u(meta,"applied_seq")?;
 let cleaned=r726_u(control,"cleanup_through")?;
 let retained=r726_u(control,"retained_records")?;
 let bytes=r726_u(control,"retained_bytes")?;
 if control["state"]!="NORMAL"||meta["phase"]!="SYNCED"
  ||r726_u(control,"id")?!=1||r726_u(meta,"id")?!=1
  ||r726_u(control,"schema_generation")?!=1
  ||r726_s(control,"feed_generation")?!="legacy"
  ||control["feed_generation"]!=meta["feed_generation"]
  ||r726_u(control,"baseline_seq")?!=r726_u(meta,"baseline_seq")?
  ||r726_u(control,"baseline_seq")?!=r726_u(meta,"bootstrap_seq")?
  ||r726_u(control,"baseline_seq")?>latest
  ||cleaned>applied||applied>latest
  ||retained>=64||bytes>1024*1024*1024
  ||r726_s(meta,"source_identity")?!=r696_identity(root)?
  ||r726_s(meta,"schema_hash")?!=r756_schema_hash(src)?
  ||r726_u(meta,"schema_version")?!=r366_query_num(src,"PRAGMA schema_version")?
  ||r726_u(meta,"user_version")?!=4
  ||r366_query_num(src,"PRAGMA user_version")?!=4
  ||r366_query_num(dst,"PRAGMA user_version")?!=4{
  return Err(EventOutboxR366Error::Corrupt)
 }
 // Validate only the immediately preceding source envelope. The receiver
 // independently validates the ENTIRE retained hash chain before DB2 ACK.
 let mut last=Stmt::new(src,"SELECT seq,prev_seq,payload_hash,payload_json,
  feed_generation FROM mirror_outbox ORDER BY seq DESC LIMIT 1").map_err(r366_error)?;
 let exists=match last.step().map_err(r366_error)?{
  SQLITE_ROW=>{
   let seq=last.num(0).map_err(r366_error)?;
   let prev=last.num(1).map_err(r366_error)?;
   let hash=last.strval(2).map_err(r366_error)?;
   let raw=last.strval(3).map_err(r366_error)?;
   let generation=last.strval(4).map_err(r366_error)?;
   if seq!=latest||prev.checked_add(1)!=Some(seq)
    ||r366_sha(&raw)!=hash||generation!="legacy"{
    return Err(EventOutboxR366Error::Corrupt)
   }
   true
  },
  SQLITE_DONE=>false,
  _=>return Err(EventOutboxR366Error::Corrupt)
 };
 drop(last);
 if (!exists&&(retained!=0||cleaned!=latest))||(exists&&retained==0){
  return Err(EventOutboxR366Error::Corrupt)
 }
 let next=latest.checked_add(1).ok_or(EventOutboxR366Error::Pressure)?;
 r1130_measure(5,r1130_t);
 let r1130_t=std::time::Instant::now();
 let frame=serde_json::json!({"protocol_version":1,"schema_generation":1,
  "tx_kind":"r756_offline_original","operations":operations});
 let raw=r386_python_mirror_canonical(&frame)?;
 if raw.len()>4*1024*1024{return Err(EventOutboxR366Error::Pressure)}
 let size=(raw.len()+384) as u64;
 if bytes.checked_add(size).ok_or(EventOutboxR366Error::Pressure)?>1024*1024*1024{
  return Err(EventOutboxR366Error::Pressure)
 }
 r1130_measure(6,r1130_t);
 let r1130_t=std::time::Instant::now();
 for op in operations{r586_apply_original_operation(src,manifest,op)?;}
 r1130_measure(7,r1130_t);
 let r1130_t=std::time::Instant::now();
 let id=format!("tx-{next:032x}");
 let sql=format!("INSERT INTO mirror_outbox(seq,prev_seq,tx_id,tx_kind,
  schema_generation,payload_json,payload_hash,payload_bytes,
  committed_at_ms,feed_generation)
  VALUES({next},{latest},{},'r756_offline_original',1,{},{},{size},{},'legacy');
  UPDATE mirror_control SET latest_seq={next},
   retained_records=retained_records+1,
   retained_bytes=retained_bytes+{size} WHERE id=1",
  r586_sql_literal(&serde_json::json!(id))?,
  r586_sql_literal(&serde_json::json!(raw))?,
  r586_sql_literal(&serde_json::json!(r366_sha(&raw)))?,
  1_000_000+next);
 exec(src,&sql).map_err(r366_error)?;
 tx.commit().map_err(r366_error)?;
 r1130_measure(8,r1130_t);
 Ok(next)
}

#[cfg(test)]
mod r830_fast_writer_guard_tests{
 use super::*;
 use std::os::unix::fs::DirBuilderExt;
 struct Private(PathBuf);
 impl Private{
  fn new()->Self{
   let mut dir=std::env::temp_dir();
   let stamp=std::time::SystemTime::now()
     .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
   dir.push(format!("astra_r671_file_{}_{}_r832",std::process::id(),stamp));
   fs::DirBuilder::new().mode(0o700).create(&dir).unwrap();
   Self(dir)
  }
 }
 impl Drop for Private{
  fn drop(&mut self){let _=fs::remove_dir_all(&self.0);}
 }
 fn insert(v:&serde_json::Value,key:&str)->serde_json::Value{
  r659_op(v,"metadata",&serde_json::json!({"key":key,"value":"paper_only"}),
   "insert").unwrap()
 }
 #[test]fn r832_wrong_schema_hash_refuses_fast_db1_append(){
  let r=Private::new();
  let (src,dst,v)=r756_initialize(&r.0).unwrap();
  exec(&dst,"UPDATE mirror_meta SET schema_hash='forged' WHERE id=1").unwrap();
  assert!(r830_incremental_original_append(
    &r.0,&src,&dst,&v,&[insert(&v,"r832_unchanged")]).is_err());
  assert_eq!(r366_query_num(&src,"SELECT count(*) FROM mirror_outbox"),Ok(0));
  assert_eq!(r366_query_num(&src,
   "SELECT count(*) FROM metadata WHERE key='r832_unchanged'"),Ok(0));
 }
 #[test]fn r833_tail_hash_tampering_blocks_fast_db1_append(){
  let r=Private::new();
  let (src,dst,v)=r756_initialize(&r.0).unwrap();
  assert_eq!(r830_incremental_original_append(
    &r.0,&src,&dst,&v,&[insert(&v,"r833_first")]),Ok(1));
  exec(&src,"UPDATE mirror_outbox SET payload_hash='forged' WHERE seq=1").unwrap();
  assert!(r830_incremental_original_append(
   &r.0,&src,&dst,&v,&[insert(&v,"r833_second")]).is_err());
  assert_eq!(r366_query_num(&src,"SELECT latest_seq FROM mirror_control"),Ok(1));
  assert_eq!(r366_query_num(&src,
   "SELECT count(*) FROM metadata WHERE key='r833_second'"),Ok(0));
 }
}
