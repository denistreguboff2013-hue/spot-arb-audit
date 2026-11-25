// Astra P0-01 R907: perform ONE complete canonical retained-outbox
// hash-chain validation while gathering only already-applied PAPER rows.
// Fuses R805 full-tail verification with R879 ACK row extraction; never
// skips hashes on future rows. Research PRIVATE only, no installed PAPER.
fn r948_retained_with_optional_fingerprints(
 src:&Db,h:&R805Head,generation:&str,run:&str,
 want_fingerprints:bool
)->Result<(u64,u64,std::collections::BTreeMap<String,serde_json::Value>,String,
 Vec<(u64,String)>),
 EventOutboxR366Error>{
 if run.is_empty()||run.len()>80||!run.bytes()
  .all(|c|c.is_ascii_alphanumeric()||c==b'_'||c==b'-'){
  return Err(EventOutboxR366Error::Schema)
 }
 if h.applied<=h.cleaned || h.applied>h.latest || h.retained>64{
  return Err(EventOutboxR366Error::Conflict)
 }
 let sql=r805_window_sql(R726_OUTBOX,"ORDER BY seq")?;
 let mut q=Stmt::new(src,&sql).map_err(r366_error)?;
 let mut previous=h.cleaned;
 let mut count=0_u64;let mut total_bytes=0_u64;
 let mut acked=0_u64;let mut ack_bytes=0_u64;
 let mut ack_hash=String::new();
 let mut rows=std::collections::BTreeMap::new();
 let mut fingerprints=Vec::new();
 loop{
  match q.step().map_err(r366_error)?{
   SQLITE_DONE=>break,
   SQLITE_ROW=>{
    count=count.checked_add(1).ok_or(EventOutboxR366Error::Pressure)?;
    if count>1_000_000{return Err(EventOutboxR366Error::Pressure)}
    let raw=q.strval(0).map_err(r366_error)?;
    let row:serde_json::Value=serde_json::from_str(&raw)
     .map_err(|_|EventOutboxR366Error::Corrupt)?;
    let (seq,size)=r805_validate_row(&row,previous,h.latest,generation)?;
    if seq!=previous.checked_add(1).ok_or(EventOutboxR366Error::Pressure)?{
     return Err(EventOutboxR366Error::Corrupt)
    }
    previous=seq;
    total_bytes=total_bytes.checked_add(size).ok_or(EventOutboxR366Error::Pressure)?;
    if want_fingerprints{fingerprints.push((seq,r366_sha(&raw)));}
    if seq>h.applied {continue}
    acked=acked.checked_add(1).ok_or(EventOutboxR366Error::Pressure)?;
    ack_bytes=ack_bytes.checked_add(size).ok_or(EventOutboxR366Error::Pressure)?;
    if seq==h.applied{
     ack_hash=r726_s(&row,"payload_hash")?.to_owned();
    }
    let payload:serde_json::Value=serde_json::from_str(r726_s(&row,"payload_json")?)
     .map_err(|_|EventOutboxR366Error::Corrupt)?;
    let ops=payload["operations"].as_array().ok_or(EventOutboxR366Error::Corrupt)?;
    if ops.len()!=32{return Err(EventOutboxR366Error::Corrupt)}
    for (idx,op) in ops.iter().enumerate(){
     let event_id=format!("paper_r824_{run}_{seq:020}_{idx:02}");
     if op["table"]!="events"||op["op"]!="insert"
       ||op["row"]["run_id"]!=run||op["row"]["mode"]!="paper"
       ||op["row"]["verification_status"]!="unverified"
       ||op["row"]["is_exchange_verified"]!=0
       ||op["row"]["event_id"]!=event_id
       ||rows.insert(event_id,op["row"].clone()).is_some(){
      return Err(EventOutboxR366Error::Corrupt)
     }
    }
   },
   _=>return Err(EventOutboxR366Error::Corrupt),
  }
 }
 drop(q);
 if count!=h.retained
  || (count==0&&h.cleaned!=h.latest)
  || (count>0&&previous!=h.latest)
  || total_bytes>1024*1024*1024
  || acked!=h.applied-h.cleaned
  || ack_hash.is_empty()
  || rows.len()!=acked as usize*32{
  return Err(EventOutboxR366Error::Corrupt)
 }
 Ok((acked,ack_bytes,rows,ack_hash,fingerprints))
}
fn r907_verify_retained_and_extract_prefix(
 src:&Db,h:&R805Head,generation:&str,run:&str
)->Result<(u64,u64,std::collections::BTreeMap<String,serde_json::Value>,String),
 EventOutboxR366Error>{
 let (n,bytes,rows,hash,_) =
  r948_retained_with_optional_fingerprints(src,h,generation,run,false)?;
 Ok((n,bytes,rows,hash))
}
