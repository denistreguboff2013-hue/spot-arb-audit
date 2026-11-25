// R1144: passive, bounded WS read/poll observability on existing PUBLIC PAPER.
// No new network actions, retry scheduling, source durability or trade permission.
#[derive(Default)]
struct R1144Window{
 sample_poll_ms:Vec<u64>,
 sample_read_gap_ms:Vec<u64>,
 read_frames:u64,
 sampled_old:u64,
 min_source_ts:Option<i64>,
 max_source_ts:Option<i64>,
 bytes:u64,
}
struct R1144WsPollProbe{
 enabled:bool,
 connected:Instant,
 last_read:Option<Instant>,
 total_frames:u64,
 max_read_gap_ms:u64,
 max_poll_wait_ms:u64,
 windows:Vec<R1144Window>,
}
impl R1144WsPollProbe{
 fn new()->Self{
   let enabled=std::env::var("ASTRA_R1144_WS_POLL_DIAGNOSTICS").ok().as_deref()==Some("YES")
    &&std::env::var("ASTRA_R1142_SHARD_DIAGNOSTICS").ok().as_deref()==Some("YES")
    &&std::env::var("ASTRA_R1119_FULL_RUST_PAPER").ok().as_deref()==Some("YES")
    &&std::env::var("ASTRA_R1087_PAPER_PUBLIC_ONLY").ok().as_deref()==Some("YES")
    &&std::env::var("ASTRA_R1136_RELEASE_PAPER").ok().as_deref()==Some("YES");
   Self{enabled,connected:Instant::now(),last_read:None,total_frames:0,
       max_read_gap_ms:0,max_poll_wait_ms:0,windows:(0..13).map(|_|R1144Window::default()).collect()}
 }
 fn text_read(&mut self, raw:&str,wall_ms:i64,poll_ms:u64){
  if !self.enabled{return}
  let now=Instant::now();
  let gap=self.last_read.map(|last|now.duration_since(last).as_millis().min(u64::MAX as u128) as u64).unwrap_or(0);
  self.last_read=Some(now);
  self.total_frames+=1;
  self.max_read_gap_ms=self.max_read_gap_ms.max(gap);
  self.max_poll_wait_ms=self.max_poll_wait_ms.max(poll_ms);
  let index=(self.connected.elapsed().as_secs()/10).min(12) as usize;
  let w=&mut self.windows[index];
  w.read_frames+=1;
  w.bytes=w.bytes.saturating_add(raw.len() as u64);
  if self.total_frames%16!=0{return}
  w.sample_poll_ms.push(poll_ms);
  w.sample_read_gap_ms.push(gap);
  if let Ok(value)=serde_json::from_str::<serde_json::Value>(raw){
   if let Some(source)=value.get("data").and_then(|data|data.get("E")).and_then(|ts|ts.as_i64()){
     w.min_source_ts=Some(w.min_source_ts.map_or(source,|e|e.min(source)));
     w.max_source_ts=Some(w.max_source_ts.map_or(source,|e|e.max(source)));
     if wall_ms.saturating_sub(source)>800{w.sampled_old+=1}
   }
  }
 }
 fn report(&self,shard:usize,outcome:&str){
  if !self.enabled{return}
  let quantile=|values:&Vec<u64>,q:usize|->u64{
   if values.is_empty(){return 0}
   let mut a=values.clone();a.sort_unstable();a[(a.len()-1)*q/100]
  };
  let windows=self.windows.iter().enumerate()
    .filter(|(_,w)|w.read_frames>0)
    .map(|(index,w)|serde_json::json!({
      "seconds_from_connect":index*10,
      "read_frames":w.read_frames,
      "read_bytes":w.bytes,
      "sampled":w.sample_poll_ms.len(),
      "sampled_age_gt800":w.sampled_old,
      "sample_poll_wait_p50_ms":quantile(&w.sample_poll_ms,50),
      "sample_poll_wait_p95_ms":quantile(&w.sample_poll_ms,95),
      "sample_read_interarrival_p95_ms":quantile(&w.sample_read_gap_ms,95),
      "source_event_time_span_ms":match(w.min_source_ts.zip(w.max_source_ts)){
         Some((lo,hi))=>Some(hi.saturating_sub(lo)),None=>None}
     })).collect::<Vec<_>>();
  eprintln!("R1144_WS_POLL_DIAGNOSTIC {}",serde_json::json!({
    "schema":"SPOTARB/R1144/WS_READ_PROBE/PAPER/V1",
    "shard":shard,"outcome":outcome,"total_frames":self.total_frames,
    "max_ws_read_interarrival_ms":self.max_read_gap_ms,
    "max_ws_next_poll_wait_ms":self.max_poll_wait_ms,
    "sample_every":16,"window_statistics":windows,
    "read_only":true,"no_exchange_commands":true
  }));
 }
}
