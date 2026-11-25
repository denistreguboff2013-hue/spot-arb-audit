// Astra R1134: exactly ONE Rust verifier helper thread, PAPER ONLY.
// Main DB2 writer has the exclusive DB2 transaction. Before DB2 COMMIT,
// the helper proves EVERY outbox row from a single original DB1 source
// snapshot using R805's unchanged exact validation. The worker receives
// references to immutable memory; no stale external/cached proof is trusted.
// A helper panic/error ALWAYS prevents DB2 commit (RAII rollback).
fn r1134_parallel_helper_enabled()->bool{
 r1132_fusion_enabled()&&
 std::env::var("ASTRA_R1134_ONE_VERIFY_HELPER").ok().as_deref()==Some("YES")
}
fn r1134_validate_immutable_window(
 raws:&[String], head:&R805Head,generation:&str,expected_bytes:u64
)->Result<(u64,u64,u64),EventOutboxR366Error>{
 let mut prev=head.cleaned;
 let mut count=0u64;
 let mut bytes=0u64;
 for raw in raws{
   let row:serde_json::Value=serde_json::from_str(raw)
     .map_err(|_|EventOutboxR366Error::Corrupt)?;
   let (seq,size)=r805_validate_row(&row,prev,head.latest,generation)?;
   count=count.checked_add(1).ok_or(EventOutboxR366Error::Pressure)?;
   bytes=bytes.checked_add(size).ok_or(EventOutboxR366Error::Pressure)?;
   prev=seq;
 }
 if count!=head.retained||count>64||
    (count==0&&head.cleaned!=head.latest)||
    (count>0&&prev!=head.latest)||
    bytes!=expected_bytes||bytes>1024*1024*1024{
   return Err(EventOutboxR366Error::Corrupt)
 }
 Ok((count,bytes,prev))
}
fn r1134_parallel_verified_paper_burst(
 root:&Path,src:&Db,dst:&Db,manifest:&serde_json::Value,limit:usize
)->Result<u64,EventOutboxR366Error>{
 if !(1..=128).contains(&limit){return Err(EventOutboxR366Error::Pressure)}
 exec(src,"BEGIN DEFERRED").map_err(r366_error)?;
 let _snapshot=R805SourceRead(src);
 let r1134_read_started=std::time::Instant::now();
 let (head,control,_meta)=r805_read_head(root,src,dst)?;
 if head.retained>64{return Err(EventOutboxR366Error::Pressure)}
 let generation=r726_s(&control,"feed_generation")?.to_owned();
 let bytes_expected=r726_u(&control,"retained_bytes")?;
 // Explicit bounded-memory admission: large windows fail closed, do
 // NOT silently skip any source validation or detached-helper receipt.
 if bytes_expected>64*1024*1024{return Err(EventOutboxR366Error::Pressure)}
 let query=r805_window_sql(R726_OUTBOX,"ORDER BY seq")?;
 let mut query_rows=Stmt::new(src,&query).map_err(r366_error)?;
 let mut raws=Vec::with_capacity(head.retained as usize);
 let mut loaded=0usize;
 loop{
  match query_rows.step().map_err(r366_error)?{
   SQLITE_DONE=>break,
   SQLITE_ROW=>{
    let raw=query_rows.strval(0).map_err(r366_error)?.to_owned();
    loaded=loaded.checked_add(raw.len()).ok_or(EventOutboxR366Error::Pressure)?;
    if loaded>64*1024*1024{return Err(EventOutboxR366Error::Pressure)}
    raws.push(raw);
   },
   _=>return Err(EventOutboxR366Error::Corrupt)
  }
 }
 drop(query_rows);
 r1130_measure(23,r1134_read_started);
 // Immutable "handoff" is Rust's shared borrow, not a JSON receipt
 // detached from an actively mutating source. The original read
 // transaction pins the same exact DB1 snapshot until DB2 completes.
 std::thread::scope(|scope|{
  let worker=scope.spawn(||{
   let from=std::time::Instant::now();
   let result=r1134_validate_immutable_window(
      &raws,&head,&generation,bytes_expected);
   r1130_measure(24,from);
   result
  });
  let write_result=(||{
   let tx=Tx::begin(dst).map_err(r366_error)?;
   let cols=r951_original_events_projection(manifest)?;
   let mut prepared=r951_prepared_insert(dst,&cols)?;
   let r1134_apply_started=std::time::Instant::now();
   let mut applied=head.applied;
   let mut count=0u64;
   let mut hash=None::<String>;
   for raw in &raws{
    let row:serde_json::Value=serde_json::from_str(raw)
      .map_err(|_|EventOutboxR366Error::Corrupt)?;
    let seq=r726_u(&row,"seq")?;
    if seq<=head.applied||count>=limit as u64{continue}
    if seq!=applied.checked_add(1).ok_or(EventOutboxR366Error::Corrupt)?{
      return Err(EventOutboxR366Error::Corrupt)
    }
    let frame:serde_json::Value=serde_json::from_str(
      r726_s(&row,"payload_json")?)
      .map_err(|_|EventOutboxR366Error::Corrupt)?;
    let ops=frame["operations"].as_array()
      .ok_or(EventOutboxR366Error::Corrupt)?;
    if ops.is_empty()||ops.len()>32||
      ops.iter().any(|op|op["table"]!="events"||op["op"]!="insert"||
        op["row"]["mode"]!="paper"||
        op["row"]["verification_status"]!="unverified"||
        op["row"]["is_exchange_verified"]!=0){
      return Err(EventOutboxR366Error::Corrupt)
    }
    for op in ops{r951_bind_and_insert(&mut prepared,dst,&cols,op)?;}
    applied=seq;
    count+=1;
    hash=Some(r726_s(&row,"payload_hash")?.to_owned());
   }
   drop(prepared);
   r1130_measure(25,r1134_apply_started);
   // Wait for exact full retained-chain proof BEFORE mutating the DB2
   // durable frontier or COMMIT. This proof is valid ONLY for the
   // source snapshot and immutable memory borrowed above.
   let r1134_wait_started=std::time::Instant::now();
   let proof=worker.join().map_err(|_|EventOutboxR366Error::Corrupt)??;
   r1130_measure(26,r1134_wait_started);
   if proof.0!=head.retained||proof.1!=bytes_expected||
      proof.2!=head.latest{return Err(EventOutboxR366Error::Corrupt)}
   if let Some(hash)=hash{
    exec(dst,&format!("UPDATE mirror_meta SET applied_seq={applied},
       applied_hash={},last_success_ms={} WHERE id=1",
       r586_sql_literal(&serde_json::json!(hash))?,3_000_000+applied))
       .map_err(r366_error)?;
   }
   tx.commit().map_err(r366_error)?;
   Ok(count)
  })();
  write_result
 })
}
