// R1066: tested PAPER-only source->outbox->independent Rust receiver/ACK canary.
// Included in R147 sqlite private module. NO real order path, NO production DB.
pub fn r1066_private_init(root:&Path)->Result<(),String>{
 drop(r756_initialize(root).map_err(|e|format!("R1066 INIT: {e:?}"))?);
 Ok(())
}
fn r1066_numeric(root:&Path, name:&str, sql:&str)->Result<u64,String>{
 let db=r671_open(root,name,false).map_err(|e|format!("R1066 DB OPEN: {e:?}"))?;
 r366_query_num(&db,sql).map_err(|e|format!("R1066 QUERY: {e:?}"))
}
pub fn r1066_private_producer(root:&Path, frames:u64)->Result<serde_json::Value,String>{
 use crate::{Owner,RawFrame,Update,Book,Venue};
 use std::time::{Duration,Instant,SystemTime,UNIX_EPOCH};
 if !(2..=1024).contains(&frames)||frames%2!=0{
  return Err("R1066 only 2..1024 even synthetic frames".into())
 }
 let mut history=R819OriginalPaperHistory::new(root,"r1066_paper_canary")
  .map_err(|e|format!("R1066 R819 history admission: {e:?}"))?;
 let mut owner=Owner::new(0,1,800).map_err(|e|format!("R1066 OWNER: {e:?}"))?;
 let mut accepted=0u64;
 let mut paired=0u64;
 let mut throttled=0u64;
 let begin=Instant::now();
 for i in 0..frames{
  if begin.elapsed()>Duration::from_secs(100){
   return Err("R1066 producer exceeded bounded 100s".into())
  }
  // Drive the real Owner ingest_parsed -> synchronous R819 history
  // acceptance, with fresh mock PUBLIC market books only (never fills).
  let venue=if i%2==0{Venue::OkxSpot}else{Venue::BinanceFutures};
  let mono=crate::transport::monotonic_ns();
  let wall=SystemTime::now().duration_since(UNIX_EPOCH)
   .map_err(|_|"R1066 invalid source wall clock")?.as_millis() as i64;
  let seq=(i/2+1) as i64;
  let price=if i%2==0{101.0}else{102.0};
  let raw=serde_json::json!({"paper_synthetic":true,
    "venue":if i%2==0{"okx_public"}else{"binance_public"},
    "base":"BTC","seq":seq,"bid":price,"ask":price+0.1}).to_string();
  let frame=RawFrame{source:None,venue,epoch:1,received_ns:mono,
    received_wall_ms:wall,raw};
  let book=Book{bids:vec![(price,1.0)],asks:vec![(price+0.1,1.0)],
    received_ns:mono,received_wall_ms:wall,exchange_ts_ms:Some(wall),
    changed_ns:mono,epoch:1,sequence:Some(seq)};
  let update=Update{base:"BTC".into(),venue,book};
  let mut attempt=0;
  loop{
   let pending=r1066_numeric(root,"r671-private-db1.sqlite",
     "SELECT retained_records FROM mirror_control WHERE id=1")?;
   if pending>=48{
    attempt+=1;throttled+=1;
    if attempt>1000{return Err("R1066 bounded producer pressure timeout".into())}
    std::thread::sleep(Duration::from_millis(10));
    continue
   }
   match owner.ingest_parsed(&frame,update.clone(),&mut history){
    Ok(snapshot)=>{accepted+=1;if snapshot.is_some(){paired+=1}break},
    Err(e)=>return Err(format!("R1066 synchronous history/owner rejected: {e:?}"))
   }
  }
 }
 let marker=root.join("r1066-producer-complete.json");
 let data=serde_json::json!({"schema":"r1066_paper_canary_producer_v1",
  "accepted":accepted,"paired":paired,"frames":frames,
  "pressure_wait_cycles":throttled,"exchange_orders":false,
  "source":"SYNTHETIC_PUBLIC_BOOKS_ONLY"});
 let bytes=serde_json::to_vec(&data).map_err(|_|"R1066 JSON")?;
 use std::io::Write;
 let mut f=fs::OpenOptions::new().create_new(true).write(true)
   .open(&marker).map_err(|e|format!("R1066 completion marker: {e}"))?;
 f.write_all(&bytes).map_err(|e|format!("R1066 marker write: {e}"))?;
 f.sync_all().map_err(|e|format!("R1066 marker sync: {e}"))?;
 Ok(data)
}
// The legacy R865 once() reads source head with separate SQLite statements
// after dropping R845's snapshot. A concurrent writer can append between
// mirror_control and COUNT(*) and produce a false Corrupt cursor. R1066
// fences its ENTIRE cursor inspection in one source read snapshot. Real
// corrupted chains still fail in R845 and R860 without suppression.
fn r1066_consistent_drain_once(root:&Path)->Result<(u64,u64,u64),String>{
 let source=r671_open(root,"r671-private-db1.sqlite",false)
   .map_err(|e|format!("R1066 open source: {e:?}"))?;
 let mirror=r671_open(root,"r671-private-db2.sqlite",false)
   .map_err(|e|format!("R1066 open replica: {e:?}"))?;
 let manifest=r586_oracle();
 let copied=r845_verified_burst(root,&source,&mirror,&manifest,128)
   .map_err(|e|format!("R1066 verified frozen burst: {e:?}"))?;
 let (latest,applied,retained)={
  exec(&source,"BEGIN DEFERRED").map_err(|e|format!("R1066 begin head snapshot: {e:?}"))?;
  let _lock=R805SourceRead(&source);
  let (h,_,_)=r805_read_head(root,&source,&mirror)
    .map_err(|e|format!("R1066 consistent cursor: {e:?}"))?;
  (h.latest,h.applied,h.retained)
 };
 let (acked,left)=if latest==applied{
  match r860_bulk_verified_ack(root,&source,&mirror,&manifest,1024){
   Ok((a,r))=>(a,r),
   Err(EventOutboxR366Error::Conflict)=>(0,retained),
   Err(e)=>return Err(format!("R1066 fenced full-state ACK refused: {e:?}"))
  }
 }else{(0,retained)};
 Ok((copied,acked,left))
}
pub fn r1066_private_receiver(root:&Path,expected:u64)
 ->Result<serde_json::Value,String>{
 r1066_private_receiver_exact(root,expected,expected)
}
pub fn r1069_private_batched_receiver(root:&Path,frames:u64)
 ->Result<serde_json::Value,String>{
 if !(16..=1024).contains(&frames)||frames%16!=0{
   return Err("R1069 batched receiver requires multiple of 16".into())
 }
 r1066_private_receiver_exact(root,frames/16,frames)
}
pub fn r1070_private_batched32_receiver(root:&Path,frames:u64)
 ->Result<serde_json::Value,String>{
 if !(32..=1024).contains(&frames)||frames%32!=0{
    return Err("R1070 batched receiver requires multiple of 32".into())
 }
 r1066_private_receiver_exact(root,frames/32,frames)
}
fn r1066_private_receiver_exact(root:&Path,expected_seq:u64,expected_events:u64)
 ->Result<serde_json::Value,String>{
 use std::time::{Instant,Duration};
 if expected_seq==0||expected_events>1024||expected_events<expected_seq{
   return Err("R1066 source-vs-physical count bound".into())
 }
 let begin=Instant::now();
 let mut copied=0u64;
 let mut acked=0u64;
 let mut cycles=0u64;
 let mut pending_peak=0u64;
 loop{
  if begin.elapsed()>Duration::from_secs(110){
   return Err("R1066 receiver exceeded bounded 110s".into())
  }
  let (d,a,_r)=r1066_consistent_drain_once(root)?;
  copied=copied.checked_add(d).ok_or("R1066 delivered overflow")?;
  acked=acked.checked_add(a).ok_or("R1066 ACK overflow")?;
  cycles+=1;
  let retained=r1066_numeric(root,"r671-private-db1.sqlite",
    "SELECT retained_records FROM mirror_control WHERE id=1")?;
  pending_peak=pending_peak.max(retained);
  let latest=r1066_numeric(root,"r671-private-db1.sqlite",
    "SELECT latest_seq FROM mirror_control WHERE id=1")?;
  let applied=r1066_numeric(root,"r671-private-db2.sqlite",
    "SELECT applied_seq FROM mirror_meta WHERE id=1")?;
  let receipt=root.join("r1066-producer-complete.json");
  if receipt.exists()&&latest==expected_seq&&applied==expected_seq&&retained==0{
   break;
  }
  std::thread::sleep(Duration::from_millis(20));
 }
 let a=r671_open(root,"r671-private-db1.sqlite",false)
   .map_err(|e|format!("R1066 final DB1: {e:?}"))?;
 let b=r671_open(root,"r671-private-db2.sqlite",false)
   .map_err(|e|format!("R1066 final DB2: {e:?}"))?;
 let parity=r800_streaming_original_parity(&a,&b,&r586_oracle())
   .map_err(|e|format!("R1066 exact 17-table parity failed: {e:?}"))?;
 let source=r366_query_num(&a,"SELECT latest_seq FROM mirror_control WHERE id=1")
   .map_err(|_|"R1066 source seq")?;
 let ack=r366_query_num(&a,"SELECT cleanup_through FROM mirror_control WHERE id=1")
   .map_err(|_|"R1066 ack seq")?;
 let target=r366_query_num(&b,"SELECT applied_seq FROM mirror_meta WHERE id=1")
   .map_err(|_|"R1066 applied seq")?;
 let kept=r366_query_num(&a,"SELECT COUNT(*) FROM mirror_outbox")
   .map_err(|_|"R1066 outbox count")?;
 if source!=expected_seq||ack!=expected_seq||target!=expected_seq
    ||kept!=0||parity!=expected_events{
  return Err(format!("R1066 exact source/ACK/DB2/physical parity mismatch: {source}/{ack}/{target}/{kept}/{parity}"))
 }
 Ok(serde_json::json!({"schema":"r1066_paper_canary_receiver_v1",
  "ok":true,"source_seq":source,"ack_seq":ack,"db2_applied":target,
  "physical_events_17table":parity,"expected_physical_events":expected_events,
  "source_transactions":expected_seq,"unacked":kept,
  "copied":copied,"acked":acked,"cycles":cycles,
  "peak_pending":pending_peak,"exchange_orders":false,
  "source":"SYNTHETIC_PUBLIC_BOOKS_ONLY"}))
}

// R1087: independent Rust DB2/ACK receiver for a BOUNDED actual-public
// websocket PAPER canary (never private orders or installed source DB).
// Unlike synthetic R1066, real public-frame counts are not preknown.
// An exact producer end marker is mandatory before any final PASS.
// R1089: same R845->R860 integrity gates, but retain DB handles and
// defer the expensive 17-table R860 full parity check until >=48
// durable envelopes, or until the producer has fully sealed.
fn r1089_bounded_reused_drain(
 root:&Path,src:&Db,dst:&Db,manifest:&serde_json::Value,
 stop_marker_exists:bool
)->Result<(u64,u64,u64),String>{
 let prepared=std::env::var("PAPER_R1090_PREPARED_DB2")
   .ok().as_deref()==Some("YES");
 let dynamic=std::env::var("PAPER_R1092_DYNAMIC_ACK")
   .ok().as_deref()==Some("YES");
 let delivered=if prepared{
   r1090_paper_prepared_burst(root,src,dst,manifest,128)
 }else{r845_verified_burst(root,src,dst,manifest,128)}
   .map_err(|e|format!("R1089/R1090 verified DB2 replay: {e:?}"))?;
 let (latest,applied,retained)={
  exec(src,"BEGIN DEFERRED").map_err(|e|format!("R1089 source snapshot: {e:?}"))?;
  let _guard=R805SourceRead(src);
  let (h,_,_)=r805_read_head(root,src,dst)
    .map_err(|e|format!("R1089 consistent snapshot: {e:?}"))?;
  (h.latest,h.applied,h.retained)
 };
 // R1135 bounded actual-public PAPER experiment: ACK earlier while
 // preserving original R1092 exact chain/17-table verification. Default 48.
 let early_ack=std::env::var("ASTRA_R1135_ACK_WATERMARK").ok().as_deref()==Some("24")
    &&r1134_parallel_helper_enabled()
    &&std::env::var("ASTRA_R1127_BOUNDED_PROFILE").ok().as_deref()==Some("YES")
    &&std::env::var("ASTRA_R1130_STAGE_PROFILE").ok().as_deref()==Some("YES");
 let ack_threshold=if early_ack{24}else{48};
 if latest!=applied || (retained<ack_threshold&&!stop_marker_exists){
   return Ok((delivered,0,retained))
 }
 let outcome=if dynamic{
   if !prepared || std::env::var("ASTRA_R1087_PAPER_PUBLIC_ONLY")
     .ok().as_deref()!=Some("YES"){
      return Err("R1092 requires R1090 PRIVATE event-only receiver".into())
   }
   let run=std::env::var("PAPER_VIRTUAL_FOUR_LEG_SESSION_ID")
     .map_err(|_|"R1092 missing source run identity")?;
   r1092_private_dynamic_incremental_ack(root,src,dst,manifest,&run)
 }else{r860_bulk_verified_ack(root,src,dst,manifest,1024)};
 let (acked,left)=match outcome{
   Ok(v)=>v,
   Err(EventOutboxR366Error::Conflict)=>(0,retained),
   Err(e)=>return Err(format!("R1089 unchanged R860 exact parity ACK refused: {e:?}"))
 };
 Ok((delivered,acked,left))
}

pub fn r1087_private_public_receiver(root:&Path,max_s:u64)
 ->Result<serde_json::Value,String>{
 use std::time::{Instant,Duration};
 let observed=std::env::var("ASTRA_R1091_MONITORED_PAPER")
  .ok().as_deref()==Some("YES");
 let operational=std::env::var("ASTRA_R1119_FULL_RUST_PAPER")
   .ok().as_deref()==Some("YES");
 if !(20..=150).contains(&max_s) &&
    !(observed&&(1200..=1500).contains(&max_s)&&
      std::env::var("PAPER_R1090_PREPARED_DB2")
        .ok().as_deref()==Some("YES"))&&
    !(operational&&(21600..=21840).contains(&max_s)&&
      std::env::var("PAPER_R1090_PREPARED_DB2")
        .ok().as_deref()==Some("YES")&&
      std::env::var("PAPER_R1092_DYNAMIC_ACK")
        .ok().as_deref()==Some("YES")&&
      std::env::var("ASTRA_R1087_PAPER_PUBLIC_ONLY")
        .ok().as_deref()==Some("YES")){
  return Err("R1087/R1091 receiver duration guard".into())
 }
 let begin=Instant::now();
 let mut copied=0u64;
 let mut acked=0u64;
 let mut max_pending=0u64;
 let mut cycles=0u64;
 let mut r1132_last_trace=Instant::now();
 let marker=root.join("r1087_public_producer_finished.json");
 let r1089=std::env::var("PAPER_R1089_FAST_RECEIVER")
    .ok().as_deref()==Some("YES");
 let r1090=std::env::var("PAPER_R1090_PREPARED_DB2")
    .ok().as_deref()==Some("YES");
 if r1090&&!r1089 {
  return Err("R1090 prepared DB2 requires private R1089 receiver".into())
 }
 if r1089&&std::env::var("ASTRA_R1087_PAPER_PUBLIC_ONLY")
    .ok().as_deref()!=Some("YES"){
   return Err("R1089 only the separately authorized PUBLIC PAPER canary".into())
 }
 let source=if r1089{
    Some(r671_open(root,"r671-private-db1.sqlite",false)
      .map_err(|e|format!("R1089 DB1 cannot retain handle: {e:?}"))?)
 }else{None};
 let replica=if r1089{
    Some(r671_open(root,"r671-private-db2.sqlite",false)
      .map_err(|e|format!("R1089 DB2 cannot retain handle: {e:?}"))?)
 }else{None};
 let manifest=if r1089{Some(r586_oracle())}else{None};
 // Crash-gap reconciliation runs ONCE per cold receiver process,
 // before any new DB2 replay. This avoids a second hot-path DB1 scan.
 if std::env::var("PAPER_R1092_DYNAMIC_ACK")
    .ok().as_deref()==Some("YES"){
  if !r1090||std::env::var("ASTRA_R1087_PAPER_PUBLIC_ONLY")
      .ok().as_deref()!=Some("YES"){
   return Err("R1093 private event-only receiver required".into())
  }
  let run=std::env::var("PAPER_VIRTUAL_FOUR_LEG_SESSION_ID")
    .map_err(|_|"R1093 missing source run identity")?;
  let (src,dst,v)=match (source.as_ref(),replica.as_ref(),manifest.as_ref()){
    (Some(src),Some(dst),Some(v))=>(src,dst,v),
    _=>return Err("R1093 private source handles unavailable".into())
  };
  let recovered=r1093_preflight_receipt_recovery(root,src,dst,v,&run)
    .map_err(|e|format!("R1093 fail-closed cold preflight: {e:?}"))?;
  acked=acked.checked_add(recovered).ok_or("R1093 ACK count overflow")?;
 }
 loop {
  if begin.elapsed()>Duration::from_secs(max_s){
   return Err("R1087 public receiver timed out waiting for full durable ACK".into())
  }
  let (n,a,_pending)=if let (Some(src),Some(dst),Some(manifest))=
       (source.as_ref(),replica.as_ref(),manifest.as_ref()){
     r1089_bounded_reused_drain(root,src,dst,manifest,marker.exists())?
  }else{r1066_consistent_drain_once(root)?};
  copied=copied.checked_add(n).ok_or("R1087 copied overflow")?;
  acked=acked.checked_add(a).ok_or("R1087 ACK overflow")?;
  cycles+=1;
  let (latest,applied,ack,retained)=if let (Some(src),Some(dst))=
      (source.as_ref(),replica.as_ref()){
   (r366_query_num(src,"SELECT latest_seq FROM mirror_control WHERE id=1")
     .map_err(|e|format!("R1089 latest: {e:?}"))?,
    r366_query_num(dst,"SELECT applied_seq FROM mirror_meta WHERE id=1")
     .map_err(|e|format!("R1089 applied: {e:?}"))?,
    r366_query_num(src,"SELECT cleanup_through FROM mirror_control WHERE id=1")
     .map_err(|e|format!("R1089 ACK: {e:?}"))?,
    r366_query_num(src,"SELECT retained_records FROM mirror_control WHERE id=1")
     .map_err(|e|format!("R1089 retained: {e:?}"))?)
  }else{
   (r1066_numeric(root,"r671-private-db1.sqlite",
      "SELECT latest_seq FROM mirror_control WHERE id=1")?,
    r1066_numeric(root,"r671-private-db2.sqlite",
      "SELECT applied_seq FROM mirror_meta WHERE id=1")?,
    r1066_numeric(root,"r671-private-db1.sqlite",
      "SELECT cleanup_through FROM mirror_control WHERE id=1")?,
    r1066_numeric(root,"r671-private-db1.sqlite",
      "SELECT retained_records FROM mirror_control WHERE id=1")?)
  };
  max_pending=max_pending.max(retained);
  // R1132 timing-only stderr trace, useful even if exchange resets
  // prevent a genuine terminal PASS. No ACK/source mutation.
  if r1132_fusion_enabled()&&r1132_last_trace.elapsed().as_secs()>=20{
    eprintln!("R1132_PROFILE_SNAPSHOT {}",r1130_stage_timing());
    r1132_last_trace=Instant::now();
  }
  if marker.exists()&&latest==applied&&latest==ack&&retained==0{
   let bytes=std::fs::read(&marker).map_err(|_|"R1087 marker missing bytes")?;
   if bytes.len()>4096{return Err("R1087 marker oversized".into())}
   let proof:serde_json::Value=serde_json::from_slice(&bytes)
     .map_err(|_|"R1087 invalid end marker")?;
   if proof["schema"]!="SPOTARB/R1087/PUBLIC-PAPER-SOURCE-FINISHED/V1"||
      proof["live_paper_exit_code"]!=0||
      proof["real_orders"]!=false||
      proof["private_only"]!=true||
      proof["original_archive_untouched"]!=true||
      proof["public_accepted_frames"].as_u64().is_none(){
      return Err("R1087 no authenticated successful source completion".into())
   }
   let orig=r671_open(root,"r671-private-db1.sqlite",false)
      .map_err(|_|"R1087 cannot open source final")?;
   let target=r671_open(root,"r671-private-db2.sqlite",false)
      .map_err(|_|"R1087 cannot open replica final")?;
   let events=r800_streaming_original_parity(&orig,&target,&r586_oracle())
      .map_err(|e|format!("R1087 17-table original parity failure: {e:?}"))?;
   let unique=r366_query_num(&orig,
        "SELECT COUNT(DISTINCT event_id) FROM events")
      .map_err(|_|"R1087 unique source event query")?;
   let kept=r366_query_num(&orig,
        "SELECT COUNT(*) FROM mirror_outbox")
      .map_err(|_|"R1087 final retained")?;
   let accepted=proof["public_accepted_frames"].as_u64().unwrap();
   let batched16=proof["r1088_private_batch16"]==true;
   let batched32=proof["r1129_private_batch32"]==true;
   if batched16&&batched32 || (batched32&&!r1129_private_paper_32()){
      return Err("R1129 unauthorized or conflicting public PAPER envelope".into())
   }
   let batch_limit=if batched32{32}else if batched16{16}else{1};
   let intended_seq=if batched16||batched32{
     proof["durable_source_transactions"].as_u64()
       .ok_or("R1088/R1129 missing source transaction frontier")?
   }else{accepted};
   if intended_seq==0||intended_seq>accepted||
      accepted>intended_seq.saturating_mul(batch_limit)||
      events!=accepted||unique!=accepted||kept!=0||latest!=intended_seq{
     return Err(format!("R1087/R1088 source event/ACK/DB2 mismatch: events={events}, accepted={accepted}, unique={unique}, seq={latest}, wanted={intended_seq}, kept={kept}"))
   }
   return Ok(serde_json::json!({
     "schema":"SPOTARB/R1087/REAL-PUBLIC-PAPER-DB2-ACK/V1",
     "ok":true,"public_accepted":accepted,"source_seq":latest,
     "db2_seq":applied,"ack_seq":ack,"outbox_pending":kept,
     "unique_events":unique,"physical_events_17table":events,
     "cycles":cycles,"copied":copied,"acked":acked,
     "r1130_db2_ack_stage_timing":r1130_stage_timing(),
     "peak_retained":max_pending,"exchange_orders":false,
     "original_paper_service_started":false,
     "data_source":"ACTUAL_PUBLIC_WS_FRAMES"
   }))
  }
  std::thread::sleep(Duration::from_millis(15));
 }
}

#[cfg(test)]
mod r1068_integrated_collector_refusal_tests {
 use super::*;
 use crate::{Owner,Book,RawFrame,Update,Venue};
 use std::os::unix::fs::DirBuilderExt;
 struct Private(PathBuf);
 impl Private{
  fn new()->Self{
   let mut p=std::env::temp_dir();
   p.push(format!("astra_r671_file_r1068_{}_{}",std::process::id(),
    crate::transport::monotonic_ns()));
   fs::DirBuilder::new().mode(0o700).create(&p).unwrap();
   r1066_private_init(&p).unwrap();
   Self(p)
  }
 }
 impl Drop for Private{
  fn drop(&mut self){let _=fs::remove_dir_all(&self.0);}
 }
 #[test]fn r1068_private_history_tampered_chain_denies_db2_and_ack(){
  let private=Private::new();
  let root=&private.0;
  let mut h=R819OriginalPaperHistory::new(root,"r1068_negative").unwrap();
  let mut owner=Owner::new(0,1,800).unwrap();
  let now=std::time::SystemTime::now()
   .duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64;
  let ns=crate::transport::monotonic_ns();
  let venue=Venue::OkxSpot;
  let frame=RawFrame{source:None,venue,epoch:1,received_ns:ns,
   received_wall_ms:now,raw:"{\"synthetic\":true}".into()};
  let book=Book{bids:vec![(101.0,1.0)],asks:vec![(102.0,1.0)],
   received_ns:ns,received_wall_ms:now,exchange_ts_ms:Some(now),
   changed_ns:ns,epoch:1,sequence:Some(1)};
  let update=Update{base:"BTC".into(),venue,book};
  owner.ingest_parsed(&frame,update,&mut h).unwrap();
  drop(h);
  let source=r671_open(root,"r671-private-db1.sqlite",false).unwrap();
  exec(&source,"UPDATE mirror_outbox SET payload_hash='ATTACKED'
     WHERE seq=1").unwrap();
  assert!(r1066_consistent_drain_once(root).is_err(),
   "corrupted source chain cannot be replayed or ACKed");
  let target=r671_open(root,"r671-private-db2.sqlite",false).unwrap();
  assert_eq!(r366_query_num(&target,
   "SELECT applied_seq FROM mirror_meta WHERE id=1"),Ok(0));
  assert_eq!(r366_query_num(&source,
   "SELECT cleanup_through FROM mirror_control WHERE id=1"),Ok(0));
  assert_eq!(r366_query_num(&source,
   "SELECT retained_records FROM mirror_control WHERE id=1"),Ok(1));
 }
 #[test]fn r1068_producer_count_guard_refuses_unsafe_unbounded_input(){
  let private=Private::new();
  assert!(r1066_private_producer(&private.0,1).is_err());
  assert!(r1066_private_producer(&private.0,1025).is_err());
  assert!(r1066_private_producer(&private.0,3).is_err());
  assert_eq!(r366_query_num(&r671_open(&private.0,
    "r671-private-db1.sqlite",false).unwrap(),
    "SELECT latest_seq FROM mirror_control WHERE id=1"),Ok(0));
 }
}
