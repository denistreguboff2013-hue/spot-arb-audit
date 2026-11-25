// Astra R1132 opt-in PRIVATE PUBLIC-PAPER replay experiment.
// Under one consistent SQLite DB1 snapshot, validate the ENTIRE retained
// original hash chain while applying ONLY the next bounded unmirrored slice.
// No verified row is skipped; there is no cached trust, phantom ACK or new
// storage writer. Default R1090 implementation remains untouched.
fn r1132_fusion_enabled()->bool{
 std::env::var("ASTRA_R1132_FUSED_REPLAY").ok().as_deref()==Some("YES")&&
 std::env::var("ASTRA_R1119_FULL_RUST_PAPER").ok().as_deref()==Some("YES")&&
 std::env::var("ASTRA_R1087_PAPER_PUBLIC_ONLY").ok().as_deref()==Some("YES")&&
 std::env::var("ASTRA_R1129_BATCH32_PAPER").ok().as_deref()==Some("YES")&&
 std::env::var("PAPER_R1092_DYNAMIC_ACK").ok().as_deref()==Some("YES")
}
fn r1132_fused_verified_paper_burst(
 root:&Path,src:&Db,dst:&Db,manifest:&serde_json::Value,limit:usize
)->Result<u64,EventOutboxR366Error>{
 if !(1..=128).contains(&limit){return Err(EventOutboxR366Error::Pressure)}
 let total_started=std::time::Instant::now();
 exec(src,"BEGIN DEFERRED").map_err(r366_error)?;
 let _snapshot=R805SourceRead(src);
 let head_started=std::time::Instant::now();
 let (head,control,_meta)=r805_read_head(root,src,dst)?;
 r1130_measure(16,head_started);
 if head.retained>64{return Err(EventOutboxR366Error::Pressure)}
 let generation=r726_s(&control,"feed_generation")?;
 let prepared_started=std::time::Instant::now();
 let query=r805_window_sql(R726_OUTBOX,"ORDER BY seq")?;
 let mut rows=Stmt::new(src,&query).map_err(r366_error)?;
 let tx=Tx::begin(dst).map_err(r366_error)?;
 let cols=r951_original_events_projection(manifest)?;
 let mut stmt=r951_prepared_insert(dst,&cols)?;
 r1130_measure(18,prepared_started);
 r1130_measure(9,total_started);
 let scan_started=std::time::Instant::now();
 let mut previous=head.cleaned;
 let mut seen=0u64;
 let mut bytes=0u64;
 let mut applied=head.applied;
 let mut final_hash=None::<String>;
 let mut copied=0usize;
 loop{
  match rows.step().map_err(r366_error)?{
   SQLITE_DONE=>break,
   SQLITE_ROW=>{
    let raw=rows.strval(0).map_err(r366_error)?;
    let row:serde_json::Value=serde_json::from_str(&raw)
      .map_err(|_|EventOutboxR366Error::Corrupt)?;
    // Exact R805 validator, including source SHA256, canonical JSON,
    // chronological chain, origin identity and original field schema.
    let (seq,size)=r805_validate_row(&row,previous,head.latest,generation)?;
    previous=seq;
    seen=seen.checked_add(1).ok_or(EventOutboxR366Error::Pressure)?;
    bytes=bytes.checked_add(size).ok_or(EventOutboxR366Error::Pressure)?;
    // Fully validate every retained row, INCLUDING already-applied
    // rows and any future rows outside this bounded DB2 apply window.
    if seq<=head.applied||copied>=limit{continue}
    if seq!=applied.checked_add(1).ok_or(EventOutboxR366Error::Corrupt)?{
      return Err(EventOutboxR366Error::Corrupt)
    }
    let frame:serde_json::Value=serde_json::from_str(
       r726_s(&row,"payload_json")?)
      .map_err(|_|EventOutboxR366Error::Corrupt)?;
    let ops=frame["operations"].as_array()
      .ok_or(EventOutboxR366Error::Corrupt)?;
    if ops.is_empty()||ops.len()>32||
      ops.iter().any(|op|op["table"]!="events"||
       op["op"]!="insert"||op["row"]["mode"]!="paper"||
       op["row"]["verification_status"]!="unverified"||
       op["row"]["is_exchange_verified"]!=0){
      return Err(EventOutboxR366Error::Corrupt)
    }
    for op in ops{r951_bind_and_insert(&mut stmt,dst,&cols,op)?;}
    applied=seq;
    copied+=1;
    final_hash=Some(r726_s(&row,"payload_hash")?.to_owned());
   },
   _=>return Err(EventOutboxR366Error::Corrupt)
  }
 }
 drop(stmt);
 drop(rows);
 // Original full-window integrity contract is preserved. Return an
 // error and roll back DB2 if even an OUT-OF-WINDOW row is corrupt.
 if seen!=head.retained || (seen==0&&head.cleaned!=head.latest)
  ||(seen>0&&previous!=head.latest)||bytes>1024*1024*1024
  ||bytes!=r726_u(&control,"retained_bytes")?{
   return Err(EventOutboxR366Error::Corrupt)
 }
 r1130_measure(22,scan_started);
 let commit_started=std::time::Instant::now();
 if let Some(hash)=final_hash{
  exec(dst,&format!("UPDATE mirror_meta SET applied_seq={applied},
   applied_hash={},last_success_ms={} WHERE id=1",
   r586_sql_literal(&serde_json::json!(hash))?,3_000_000+applied))
   .map_err(r366_error)?;
 }
 tx.commit().map_err(r366_error)?;
 r1130_measure(11,commit_started);
 Ok(copied as u64)
}
