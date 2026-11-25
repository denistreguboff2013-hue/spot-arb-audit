// R1129: 32-event envelope is isolated PUBLIC PAPER only; default is 16.
fn r1129_private_paper_32()->bool{
 std::env::var("ASTRA_R1129_BATCH32_PAPER").ok().as_deref()==Some("YES")&&
 std::env::var("ASTRA_R1119_FULL_RUST_PAPER").ok().as_deref()==Some("YES")&&
 std::env::var("ASTRA_R1087_PAPER_PUBLIC_ONLY").ok().as_deref()==Some("YES")&&
 std::env::var("PAPER_R1092_DYNAMIC_ACK").ok().as_deref()==Some("YES")
}
// R1090 private-only fast DB2 event application.
// Reuse R951 prepared INSERT for all events in one DB2 transaction while
// retaining R845 full source chain verification and R860 parity+ACK.
// Requires exact event-only PAPER mode and unverified order provenance;
// mixed 17-table Python source remains on the original R845 path.
fn r1090_paper_prepared_burst(
 root:&Path,src:&Db,dst:&Db,manifest:&serde_json::Value,limit:usize
)->Result<u64,EventOutboxR366Error>{
 if !(1..=128).contains(&limit){return Err(EventOutboxR366Error::Pressure)}
 if r1134_parallel_helper_enabled(){
  return r1134_parallel_verified_paper_burst(root,src,dst,manifest,limit)
 }
 if r1132_fusion_enabled(){
  return r1132_fused_verified_paper_burst(root,src,dst,manifest,limit)
 }
 let r1130_t=std::time::Instant::now();
 exec(src,"BEGIN DEFERRED").map_err(r366_error)?;
 let _snapshot=R805SourceRead(src);
 let r1131_t=std::time::Instant::now();
 let (head,control,_meta)=r805_read_head(root,src,dst)?;
 r1130_measure(16,r1131_t);
 if head.retained>64{return Err(EventOutboxR366Error::Pressure)}
 let generation=r726_s(&control,"feed_generation")?;
 let r1131_t=std::time::Instant::now();
 r805_verify_chain(src,&head,generation)?;
 r1130_measure(17,r1131_t);
 let r1131_t=std::time::Instant::now();
 let predicate=format!("WHERE seq>{} ORDER BY seq LIMIT {limit}",head.applied);
 let sql=r805_window_sql(R726_OUTBOX,&predicate)?;
 let mut rows=Stmt::new(src,&sql).map_err(r366_error)?;
 let tx=Tx::begin(dst).map_err(r366_error)?;
 let cols=r951_original_events_projection(manifest)?;
 let mut stmt=r951_prepared_insert(dst,&cols)?;
 r1130_measure(18,r1131_t);
 let mut applied=head.applied;
 let mut final_hash=None::<String>;
 let mut total=0u64;
 r1130_measure(9,r1130_t);
 let r1130_t=std::time::Instant::now();
 loop{
  match rows.step().map_err(r366_error)?{
   SQLITE_DONE=>break,
   SQLITE_ROW=>{
    let raw=rows.strval(0).map_err(r366_error)?;
    let row:serde_json::Value=serde_json::from_str(&raw)
      .map_err(|_|EventOutboxR366Error::Corrupt)?;
    let (seq,_bytes)=r805_validate_row(&row,applied,head.latest,generation)?;
    if seq!=applied.checked_add(1).ok_or(EventOutboxR366Error::Corrupt)?{
     return Err(EventOutboxR366Error::Corrupt)
    }
    let frame:serde_json::Value=serde_json::from_str(
       r726_s(&row,"payload_json")?)
      .map_err(|_|EventOutboxR366Error::Corrupt)?;
    let ops=frame["operations"].as_array()
      .ok_or(EventOutboxR366Error::Corrupt)?;
    if ops.is_empty()||ops.len()>(if r1129_private_paper_32(){32}else{16}) ||
      ops.iter().any(|op|op["table"]!="events"||
       op["op"]!="insert"||
       op["row"]["mode"]!="paper"||
       op["row"]["verification_status"]!="unverified"||
       op["row"]["is_exchange_verified"]!=0){
      return Err(EventOutboxR366Error::Corrupt)
    }
    for op in ops{
     r951_bind_and_insert(&mut stmt,dst,&cols,op)?;
    }
    applied=seq;
    total+=1;
    final_hash=Some(r726_s(&row,"payload_hash")?.to_owned());
   },
   _=>return Err(EventOutboxR366Error::Corrupt)
  }
 }
 drop(stmt);
 drop(rows);
 r1130_measure(10,r1130_t);
 let r1130_t=std::time::Instant::now();
 if let Some(hash)=final_hash{
  exec(dst,&format!("UPDATE mirror_meta SET applied_seq={applied},
   applied_hash={},last_success_ms={} WHERE id=1",
   r586_sql_literal(&serde_json::json!(hash))?,3_000_000+applied))
   .map_err(r366_error)?;
 }
 tx.commit().map_err(r366_error)?;
 r1130_measure(11,r1130_t);
 Ok(total)
}
