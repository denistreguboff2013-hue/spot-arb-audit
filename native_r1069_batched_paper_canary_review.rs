// R1069: private 16-events per durable source commit using R824's
// prepare-on-Owner-clone -> original 17-table DB1 fsync -> atomic Owner swap.
// No new live PAPER configuration, no exchange access.
pub fn r1069_private_batched_producer(root:&Path, frames:u64)
 ->Result<serde_json::Value,String>{
 r1069_private_batch_size_producer(root,frames,16)
}
pub fn r1070_private_batched32_producer(root:&Path, frames:u64)
 ->Result<serde_json::Value,String>{
 r1069_private_batch_size_producer(root,frames,32)
}
fn r1069_private_batch_size_producer(root:&Path,frames:u64,batch_size:u64)
 ->Result<serde_json::Value,String>{
 r1081_batch_range(root,0,frames,batch_size,true)
}
/// R1081 ordinary same-session source PID restart. First stop after a
/// COMMITTED source prefix; reconstruct Owner from the full durable DB1,
/// then continue without reusing or losing a journal sequence.
pub fn r1081_private_source_first_half(root:&Path,frames:u64)
 ->Result<serde_json::Value,String>{
 if frames!=1024{return Err("R1081 exact 1024-event scope only".into())}
 r1081_batch_range(root,0,512,32,false)
}
pub fn r1081_private_source_resume(root:&Path,frames:u64)
 ->Result<serde_json::Value,String>{
 if frames!=1024{return Err("R1081 exact 1024-event scope only".into())}
 r1081_batch_range(root,512,1024,32,true)
}
fn r1081_batch_range(root:&Path,start:u64,end:u64,batch_size:u64,
 finalize:bool)->Result<serde_json::Value,String>{
 use crate::{Owner,RawFrame,Book,Update,Venue};
 use std::time::{Duration,Instant,SystemTime,UNIX_EPOCH};
 if !(batch_size==16||batch_size==32)||end>1024||
    end<start||end-start<16||
    start%batch_size!=0||end%batch_size!=0{
    return Err("R1081 bounded batch range invalid".into())
 }
 let (mut owner,already)=if start==0 {
   (Owner::new(0,1,800).map_err(|e|format!("R1081 OWNER: {e:?}"))?,0)
 }else{
   r834_recover_private_owner(root,"r1069_paper_batch",0,1,800)
    .map_err(|e|format!("R1081 replay original committed Owner: {e:?}"))?
 };
 if already!=start{return Err("R1081 durable source prefix does not match restart boundary".into())}
 let mut h=R819OriginalPaperHistory::new(root,"r1069_paper_batch")
  .map_err(|e|format!("R1081 R819 history admission: {e:?}"))?;
 if r1066_numeric(root,"r671-private-db1.sqlite",
      "SELECT latest_seq FROM mirror_control WHERE id=1")?
     !=start/batch_size{
   return Err("R1081 source cursor is not at expected normal restart boundary".into())
 }
 let mut accepted=0u64;
 let mut transactions=0u64;
 let mut waits=0u64;
 let begin=Instant::now();
 for batch in start/batch_size..end/batch_size{
  if begin.elapsed()>Duration::from_secs(100){
   return Err("R1069 batched producer elapsed bound".into())
  }
  let mut events=Vec::with_capacity(batch_size as usize);
  for j in 0..batch_size{
   let i=batch*batch_size+j;
   let venue=if i%2==0{Venue::OkxSpot}else{Venue::BinanceFutures};
   let ns=crate::transport::monotonic_ns();
   let wall=SystemTime::now().duration_since(UNIX_EPOCH)
     .map_err(|_|"R1069 wall clock")?.as_millis() as i64;
   let price=if i%2==0{101.0}else{102.5};
   let seq=(i/2+1) as i64;
   let raw=serde_json::json!({
      "paper_synthetic":true,"batch":batch,"index":j,
      "venue":if i%2==0{"okx_public"}else{"binance_public"},
      "seq":seq,"base":"BTC","price":price
    }).to_string();
   let frame=RawFrame{source:None,venue,epoch:1,received_ns:ns,
      received_wall_ms:wall,raw};
   let book=Book{bids:vec![(price,1.0)],asks:vec![(price+0.1,1.0)],
      received_ns:ns,received_wall_ms:wall,
      exchange_ts_ms:Some(wall),changed_ns:ns,
      epoch:1,sequence:Some(seq)};
   events.push((frame,Update{base:"BTC".into(),venue,book}));
  }
  let mut retries=0u64;
  loop{
   let pending=r1066_numeric(root,"r671-private-db1.sqlite",
     "SELECT retained_records FROM mirror_control WHERE id=1")?;
   if pending>=48{
    retries+=1;waits+=1;
    if retries>1000{return Err("R1069 bounded outbox pressure".into())}
    std::thread::sleep(Duration::from_millis(10));
    continue
   }
   let (seq,events_count)=r824_commit_prepared_batch(&mut h,&mut owner,&events)
     .map_err(|e|format!("R1069 atomic Owner/DB1 commit refused: {e:?}"))?;
   if seq!=batch+1||events_count!=batch_size as usize{
     return Err("R1069 source commit sequence discontinuity".into())
   }
   accepted+=events_count as u64;
   transactions+=1;
   break
  }
 }
 if owner.snapshot_venue("BTC",crate::Venue::OkxSpot).is_none()||
    owner.snapshot_venue("BTC",crate::Venue::BinanceFutures).is_none(){
   return Err("R1081 paired Owner not published".into())
 }
 let receipt=serde_json::json!({
    "schema":"r1081_paper_batch_producer_v1","accepted":accepted,
    "recovered_previous_events":already,
    "durable_transactions_this_pid":transactions,"batch_size":batch_size,
    "expected_physical_events":end,"pressure_wait_cycles":waits,
    "paired_market_present":true,"exchange_orders":false,
    "source":"SYNTHETIC_PUBLIC_BOOKS_ONLY","finalized":finalize
 });
 if !finalize{return Ok(receipt)}
 use std::io::Write;
 let path=root.join("r1066-producer-complete.json");
 let bytes=serde_json::to_vec(&receipt).map_err(|_|"R1069 marker JSON")?;
 let mut f=fs::OpenOptions::new().write(true).create_new(true)
   .open(&path).map_err(|e|format!("R1069 marker create: {e}"))?;
 f.write_all(&bytes).map_err(|e|format!("R1069 marker write: {e}"))?;
 f.sync_all().map_err(|e|format!("R1069 marker fsync: {e}"))?;
 Ok(receipt)
}
