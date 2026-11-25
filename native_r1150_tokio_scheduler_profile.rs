// R1150: PUBLIC-only bounded Release Tokio timer wakeup and DB1 time windows.
// Read-only metric capture; no durability, risk, reconnect, or WS behavior change.
static R1150_PROBE:std::sync::OnceLock<std::sync::Arc<R1150Probe>>=std::sync::OnceLock::new();
struct R1150Bucket {
 ticks:AtomicU64, lag_sum_us:AtomicU64, lag_max_us:AtomicU64,
 late_50ms:AtomicU64, late_250ms:AtomicU64, late_800ms:AtomicU64,
 db1_count:AtomicU64, db1_total_us:AtomicU64, db1_max_us:AtomicU64,
}
impl R1150Bucket {
 fn new()->Self{Self{ticks:AtomicU64::new(0),lag_sum_us:AtomicU64::new(0),
 lag_max_us:AtomicU64::new(0),late_50ms:AtomicU64::new(0),
 late_250ms:AtomicU64::new(0),late_800ms:AtomicU64::new(0),
 db1_count:AtomicU64::new(0),db1_total_us:AtomicU64::new(0),
 db1_max_us:AtomicU64::new(0)}}
}
struct R1150Probe {started:Instant, windows:Vec<R1150Bucket>}
fn r1150_enable()->bool{
 std::env::var("ASTRA_R1150_SCHED_PROBE").ok().as_deref()==Some("YES")
 &&std::env::var("ASTRA_R1119_FULL_RUST_PAPER").ok().as_deref()==Some("YES")
 &&std::env::var("ASTRA_R1087_PAPER_PUBLIC_ONLY").ok().as_deref()==Some("YES")
 &&std::env::var("ASTRA_R1136_RELEASE_PAPER").ok().as_deref()==Some("YES")
 &&std::env::var("ASTRA_R1127_BOUNDED_PROFILE").ok().as_deref()==Some("YES")
}
fn r1150_start(){
 if !r1150_enable(){return}
 let p=Arc::new(R1150Probe{started:Instant::now(),
  windows:(0..16).map(|_|R1150Bucket::new()).collect()});
 if R1150_PROBE.set(p.clone()).is_err(){return}
 tokio::spawn(async move {
  let period=Duration::from_millis(25);
  let mut due=Instant::now()+period;
  loop{
   tokio::time::sleep_until(due.into()).await;
   let now=Instant::now();
   let us=now.saturating_duration_since(due).as_micros()
       .min(u64::MAX as u128) as u64;
   let index=(p.started.elapsed().as_secs()/10).min(15) as usize;
   let w=&p.windows[index];
   w.ticks.fetch_add(1,Ordering::Relaxed);
   w.lag_sum_us.fetch_add(us,Ordering::Relaxed);
   w.lag_max_us.fetch_max(us,Ordering::Relaxed);
   if us>50_000{w.late_50ms.fetch_add(1,Ordering::Relaxed);}
   if us>250_000{w.late_250ms.fetch_add(1,Ordering::Relaxed);}
   if us>800_000{w.late_800ms.fetch_add(1,Ordering::Relaxed);}
   if p.started.elapsed()>Duration::from_secs(145){break}
   due=now+period;
  }
 });
}
fn r1150_db1_observe(ns:u64){
 let Some(p)=R1150_PROBE.get()else{return};
 let index=(p.started.elapsed().as_secs()/10).min(15) as usize;
 let w=&p.windows[index];let us=ns/1000;
 w.db1_count.fetch_add(1,Ordering::Relaxed);
 w.db1_total_us.fetch_add(us,Ordering::Relaxed);
 w.db1_max_us.fetch_max(us,Ordering::Relaxed);
}
fn r1150_snapshot()->serde_json::Value{
 let Some(p)=R1150_PROBE.get()else{return serde_json::Value::Null};
 serde_json::json!({"schema":"SPOTARB/R1150/TOKIO_TIMER_WAKEUP/V1",
 "probe_period_ms":25,"notes":"timer callback scheduling proxy only; not kernel packet timestamp",
 "windows":p.windows.iter().enumerate().map(|(i,w)|serde_json::json!({
   "window_start_s":i*10,
   "ticks":w.ticks.load(Ordering::Relaxed),
   "lag_total_us":w.lag_sum_us.load(Ordering::Relaxed),
   "lag_max_us":w.lag_max_us.load(Ordering::Relaxed),
   "late_50ms":w.late_50ms.load(Ordering::Relaxed),
   "late_250ms":w.late_250ms.load(Ordering::Relaxed),
   "late_800ms":w.late_800ms.load(Ordering::Relaxed),
   "db1_commits":w.db1_count.load(Ordering::Relaxed),
   "db1_duration_total_us":w.db1_total_us.load(Ordering::Relaxed),
   "db1_commit_max_us":w.db1_max_us.load(Ordering::Relaxed),
 })).collect::<Vec<_>>()})
}
