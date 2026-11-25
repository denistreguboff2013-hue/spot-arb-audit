// R1142: strictly opt-in PUBLIC PAPER socket-read diagnostics.
// Observational only. Does not affect admission, WS pacing, books,
// DB1/DB2, ACK, or the immutable 800ms decision-age gate.
struct R1142BinanceProbe{
 enabled:bool,
 begun:Instant,
 frame_count:u64,
 samples:Vec<Vec<i64>>,
 skipped_samples:u64,
 max_sender_block_ms:u64,
 total_sender_block_ms:u64,
 sender_count:u64,
}
impl R1142BinanceProbe{
 fn new()->Self{
  let enabled=std::env::var("ASTRA_R1142_SHARD_DIAGNOSTICS").ok().as_deref()==Some("YES")
    &&std::env::var("ASTRA_R1139_WS_DIAGNOSTICS").ok().as_deref()==Some("YES")
    &&std::env::var("ASTRA_R1119_FULL_RUST_PAPER").ok().as_deref()==Some("YES")
    &&std::env::var("ASTRA_R1087_PAPER_PUBLIC_ONLY").ok().as_deref()==Some("YES")
    &&std::env::var("ASTRA_R1136_RELEASE_PAPER").ok().as_deref()==Some("YES");
  Self{enabled,begun:Instant::now(),frame_count:0,samples:vec![Vec::new();13],
    skipped_samples:0,max_sender_block_ms:0,total_sender_block_ms:0,sender_count:0}
 }
 fn received(&mut self,raw:&str,wall:i64){
  if !self.enabled{return}
  self.frame_count+=1;
  if self.frame_count%16!=0{return}
  let event=serde_json::from_str::<serde_json::Value>(raw).ok()
    .and_then(|v|v.get("data").and_then(|d|d.get("E")).and_then(|e|e.as_i64()));
  if let Some(ts)=event{
   let age=wall.saturating_sub(ts);
   let index=(self.begun.elapsed().as_secs()/10).min(12) as usize;
   self.samples[index].push(age);
  }else{self.skipped_samples+=1}
 }
 fn delivered(&mut self,elapsed:std::time::Duration){
  if !self.enabled{return}
  let ms=elapsed.as_millis().min(u64::MAX as u128) as u64;
  self.sender_count+=1;
  self.total_sender_block_ms=self.total_sender_block_ms.saturating_add(ms);
  self.max_sender_block_ms=self.max_sender_block_ms.max(ms);
 }
 fn report(&self,shard:usize,outcome:&str){
  if !self.enabled{return}
  let mut time_windows=Vec::new();
  for (i,samples) in self.samples.iter().enumerate(){
   if samples.is_empty(){continue}
   let mut ages=samples.clone();ages.sort_unstable();
   let n=ages.len();
   let stale=ages.iter().filter(|x|**x>800).count();
   time_windows.push(serde_json::json!({
     "seconds_from_connect":i*10,"samples":n,"aged_gt_800":stale,
     "age_p50_ms":ages[n/2],
     "age_p95_ms":ages[(n-1)*95/100],
     "age_max_ms":ages[n-1]}));
  }
  eprintln!("R1142_BINANCE_SHARD_DIAGNOSTIC {}",serde_json::json!({
    "schema":"SPOTARB/R1142/BINANCE_READ_ONLY_PAPER/V1",
    "shard":shard,"outcome":outcome,
    "connected_ms":self.begun.elapsed().as_millis(),
    "depth_frames":self.frame_count,"sampling_every":16,
    "samples_skipped_parse":self.skipped_samples,
    "max_sender_block_ms":self.max_sender_block_ms,
    "sender_block_sum_ms":self.total_sender_block_ms,
    "sender_count":self.sender_count,
    "sampled_windows":time_windows,
    "no_market_commands":true,"trading":false
  }));
 }
}
