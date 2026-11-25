// R1186: bounded PUBLIC-only source-lag watchdog.
// A guard is not a market-data authenticity oracle. Only prevents stale
// combined streams from being silently used; no API/orders/REAL authority.
struct R1186BinanceStaleGuard {
    enabled:bool,
    frames:u64,
    window_started_ms:u64,
    seen:u64,
    stale:u64,
    max_age_ms:i64,
    stale_streams:std::collections::BTreeSet<String>,
    consecutive_bad:u8,
}
impl R1186BinanceStaleGuard {
 fn new(enabled:bool)->Self{
  Self{enabled,frames:0,window_started_ms:0,seen:0,stale:0,
   max_age_ms:0,stale_streams:Default::default(),consecutive_bad:0}
 }
 fn observe(&mut self,raw:&str,wall_ms:i64,elapsed_ms:u64,
            downstream_healthy:bool)->bool{
  if !self.enabled{return false}
  if !downstream_healthy{
   self.window_started_ms=elapsed_ms;
   self.seen=0;self.stale=0;self.max_age_ms=0;
   self.stale_streams.clear();self.consecutive_bad=0;
   return false
  }
  self.frames=self.frames.saturating_add(1);
  if self.frames%16!=0{return false}
  let event=serde_json::from_str::<serde_json::Value>(raw).ok();
  let e=event.as_ref().and_then(|v|v["data"]["E"].as_i64());
  let stream=event.as_ref().and_then(|v|v["stream"].as_str());
  let (Some(e),Some(stream))=(e,stream) else{return false};
  let age=wall_ms.saturating_sub(e);
  let mut trip=false;
  if elapsed_ms.saturating_sub(self.window_started_ms)>=5_000{
   let bad=self.seen>=16&&self.stale*100>=85*self.seen&&
      self.max_age_ms>=5_000&&self.stale_streams.len()>=3;
   self.consecutive_bad=if bad{self.consecutive_bad.saturating_add(1)}else{0};
   trip=self.consecutive_bad>=2;
   self.seen=0;self.stale=0;self.max_age_ms=0;
   self.stale_streams.clear();self.window_started_ms=elapsed_ms;
  }
  self.seen=self.seen.saturating_add(1);
  self.max_age_ms=self.max_age_ms.max(age);
  if age>3_000{
   self.stale=self.stale.saturating_add(1);
   self.stale_streams.insert(stream.to_owned());
  }
  trip
 }
}

#[cfg(test)]
mod r1186_binance_stale_guard_tests{
 use super::*;
 fn run(enabled:bool,age_ms:i64,distinct:usize,queue_healthy:bool)->bool{
  let mut g=R1186BinanceStaleGuard::new(enabled);
  let mut tripped=false;
  for t in 0..120_u64 {
   let now=1_800_000_000_000_i64+t as i64*100;
   for _j in 0..16_usize {
    let stream=format!("sym{}usdt@depth20@500ms",(t as usize)%distinct);
    let raw=serde_json::json!({
     "stream":stream,"data":{"E":now-age_ms}
    }).to_string();
    tripped|=g.observe(&raw,now,t*100,queue_healthy);
   }
  }
  tripped
 }
 #[test]fn r1186_sustained_multi_symbol_source_lag_trips_once(){
  assert!(run(true,8_000,8,true));
 }
 #[test]fn r1186_fresh_source_never_trips(){
  assert!(!run(true,120,8,true));
 }
 #[test]fn r1186_single_symbol_does_not_trip_group_watchdog(){
  assert!(!run(true,12_000,1,true));
 }
 #[test]fn r1186_explicit_opt_in_required(){
  assert!(!run(false,12_000,8,true));
 }
 #[test]fn r1186_local_queue_backpressure_never_trips(){
  assert!(!run(true,12_000,8,false));
 }
}
