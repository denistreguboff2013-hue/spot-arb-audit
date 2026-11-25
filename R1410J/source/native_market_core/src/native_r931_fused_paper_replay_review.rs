// R931: private guarded PAPER-only fused outbox verify + DB2 replay.
// Full canonical retained hash chain is scanned ONCE while replaying only
// the unapplied <=limit contiguous prefix in a single DB2 transaction.
// No source writes, no production service and no original Python receiver.
fn r931_guarded_verified_burst(
 root:&Path,src:&Db,dst:&Db,manifest:&serde_json::Value,limit:usize
)->Result<u64,EventOutboxR366Error>{
 r931_guarded_verified_burst_impl(root,src,dst,manifest,limit,false)
}
fn r951_guarded_verified_burst(
 root:&Path,src:&Db,dst:&Db,manifest:&serde_json::Value,limit:usize
)->Result<u64,EventOutboxR366Error>{
 r931_guarded_verified_burst_impl(root,src,dst,manifest,limit,true)
}
fn r931_guarded_verified_burst_impl(
 root:&Path,src:&Db,dst:&Db,manifest:&serde_json::Value,limit:usize,
 use_prepared:bool
)->Result<u64,EventOutboxR366Error>{
 if !(1..=128).contains(&limit){return Err(EventOutboxR366Error::Pressure)}
 r879_tests::r924_verify_installed_guards(src,manifest)?;
 r879_tests::r924_verify_installed_guards(dst,manifest)?;
 exec(src,"BEGIN DEFERRED").map_err(r366_error)?;
 let _read_snapshot=R805SourceRead(src);
 let (head,control,meta)=r805_read_head(root,src,dst)?;
 if head.retained>64{return Err(EventOutboxR366Error::Pressure)}
 let generation=r726_s(&control,"feed_generation")?;
 let sql=r805_window_sql(R726_OUTBOX,"ORDER BY seq")?;
 let mut q=Stmt::new(src,&sql).map_err(r366_error)?;
 let tx=Tx::begin(dst).map_err(r366_error)?;
 let mut prepared=if use_prepared{
  let cols=r951_original_events_projection(manifest)?;
  let stmt=r951_prepared_insert(dst,&cols)?;
  Some((cols,stmt))
 }else{None};
 let mut prev=head.cleaned;
 let mut count=0u64;let mut total_bytes=0u64;
 let mut applied=head.applied;let mut accepted=0u64;
 let mut last_hash=None::<String>;
 loop{
  match q.step().map_err(r366_error)?{
   SQLITE_DONE=>break,
   SQLITE_ROW=>{
    let raw=q.strval(0).map_err(r366_error)?;
    let row:serde_json::Value=serde_json::from_str(&raw)
     .map_err(|_|EventOutboxR366Error::Corrupt)?;
    let (seq,bytes)=r805_validate_row(&row,prev,head.latest,generation)?;
    if seq!=prev.checked_add(1).ok_or(EventOutboxR366Error::Pressure)?{
     return Err(EventOutboxR366Error::Corrupt)
    }
    prev=seq;
    count=count.checked_add(1).ok_or(EventOutboxR366Error::Pressure)?;
    total_bytes=total_bytes.checked_add(bytes).ok_or(EventOutboxR366Error::Pressure)?;
    if seq>head.applied && accepted < limit as u64{
     if seq!=applied+1{return Err(EventOutboxR366Error::Corrupt)}
     let frame:serde_json::Value=serde_json::from_str(r726_s(&row,"payload_json")?)
      .map_err(|_|EventOutboxR366Error::Corrupt)?;
     let ops=frame["operations"].as_array().ok_or(EventOutboxR366Error::Corrupt)?;
     if ops.len()!=32 || ops.iter().any(|op|
       op["table"]!="events"||op["op"]!="insert"||
       op["row"]["mode"]!="paper"||op["row"]["is_exchange_verified"]!=0){
      return Err(EventOutboxR366Error::Corrupt)
     }
     for op in ops{
      if let Some((cols,stmt))=prepared.as_mut(){
       r951_bind_and_insert(stmt,dst,cols,op)?;
      }else{
       r586_apply_original_operation(dst,manifest,op)?;
      }
     }
     applied=seq;accepted+=1;
     last_hash=Some(r726_s(&row,"payload_hash")?.to_owned());
    }
   },
   _=>return Err(EventOutboxR366Error::Corrupt),
  }
 }
 drop(q);
 if count!=head.retained || (count>0&&prev!=head.latest)
  || (count==0&&head.latest!=head.cleaned)
  || total_bytes!=r726_u(&control,"retained_bytes")?
  || head.applied!=r726_u(&meta,"applied_seq")?{
  return Err(EventOutboxR366Error::Corrupt)
 }
 if let Some(hash)=last_hash{
  exec(dst,&format!("UPDATE mirror_meta SET applied_seq={applied},
    applied_hash={},last_success_ms={} WHERE id=1",
    r586_sql_literal(&serde_json::json!(hash))?,3_000_000+applied))
   .map_err(r366_error)?;
 }
 drop(prepared);
 tx.commit().map_err(r366_error)?;
 Ok(accepted)
}
