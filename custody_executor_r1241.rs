//! Single-owner canonical custody executor. SQLite never crosses threads.
use crate::native_r1173_compact_writer::Writer;
use std::sync::{Arc, atomic::{AtomicBool, AtomicU64, Ordering}};
use tokio::sync::oneshot;

type Command = Box<dyn FnOnce(&mut Writer) + Send>;
#[derive(Default)]
struct Stats {
    failed: AtomicBool,
    submitted: AtomicU64,
    completed: AtomicU64,
    overflow: AtomicU64,
    cancelled: AtomicU64,
    outstanding: AtomicU64,
    highwater: AtomicU64,
    queue_max_ns: AtomicU64,
    service_max_ns: AtomicU64,
}
/// Dropping an in-flight call must not allow a later command to assume its ACK.
struct InFlight { stats: Arc<Stats>, armed: bool }
impl Drop for InFlight {
    fn drop(&mut self) {
        if self.armed {
            self.stats.cancelled.fetch_add(1, Ordering::SeqCst);
            self.stats.failed.store(true, Ordering::SeqCst);
        }
    }
}
pub struct CustodyWorker {
    tx: Option<std::sync::mpsc::SyncSender<Command>>,
    stopped: Option<oneshot::Receiver<()>>,
    stats: Arc<Stats>,
}
#[derive(Clone)]
pub struct CustodyClient {
    tx: std::sync::mpsc::SyncSender<Command>,
    stats: Arc<Stats>,
}
impl CustodyClient {
    /// Bounded FIFO command: completion means the Writer operation returned
    /// after its durable transaction. Dropping this future fences the worker.
    pub async fn call<T, F>(&self, operation: F) -> Result<T, String>
    where T: Send + 'static, F: FnOnce(&mut Writer) -> Result<T, String> + Send + 'static {
        if self.stats.failed.load(Ordering::SeqCst) { return Err("R1241 custody fenced; reopen and recover required".into()); }
        let (reply_tx, reply_rx) = oneshot::channel();
        let stats = self.stats.clone();
        let queued = std::time::Instant::now();
        let command: Command = Box::new(move |writer| {
            stats.queue_max_ns.fetch_max(queued.elapsed().as_nanos().min(u64::MAX as u128) as u64, Ordering::SeqCst);
            if stats.failed.load(Ordering::SeqCst) || reply_tx.is_closed() {
                stats.failed.store(true, Ordering::SeqCst);
                let _ = reply_tx.send(Err("R1241 custody cancelled/fenced; recovery required".into()));
                return;
            }
            let started = std::time::Instant::now();
            let result = operation(writer);
            stats.service_max_ns.fetch_max(started.elapsed().as_nanos().min(u64::MAX as u128) as u64, Ordering::SeqCst);
            if result.is_err() { stats.failed.store(true, Ordering::SeqCst); }
            if reply_tx.send(result).is_err() { stats.failed.store(true, Ordering::SeqCst); }
        });
        let depth = self.stats.outstanding.fetch_add(1, Ordering::SeqCst) + 1;
        match self.tx.try_send(command) {
            Ok(()) => {
                self.stats.submitted.fetch_add(1, Ordering::SeqCst);
                self.stats.highwater.fetch_max(depth, Ordering::SeqCst);
            }
            Err(error) => {
                self.stats.outstanding.fetch_sub(1, Ordering::SeqCst);
                self.stats.failed.store(true, Ordering::SeqCst);
                if matches!(error, std::sync::mpsc::TrySendError::Full(_)) { self.stats.overflow.fetch_add(1, Ordering::SeqCst); }
                return Err("R1241 custody queue full/disconnected; fail closed".into());
            }
        }
        let mut guard = InFlight { stats: self.stats.clone(), armed: true };
        let result = reply_rx.await.map_err(|_| "R1241 custody reply lost; recovery required".to_string());
        if result.is_err() { self.stats.failed.store(true, Ordering::SeqCst); }
        guard.armed = false;
        let outcome=result?;
        if outcome.is_ok() && self.stats.failed.load(Ordering::SeqCst) {
            return Err("R1241 custody fenced while operation was active; recovery required".into());
        }
        outcome
    }
}
impl CustodyWorker {
    pub const CAPACITY: usize = 16;
    pub async fn open(root: std::path::PathBuf, resume: bool) -> Result<Self, String> {
        Self::start(move || if resume { Writer::resume(&root) } else { Writer::create(&root) }).await
    }
    async fn start<F>(open: F) -> Result<Self, String>
    where F: FnOnce() -> Result<Writer, String> + Send + 'static {
        let (tx, rx) = std::sync::mpsc::sync_channel::<Command>(Self::CAPACITY);
        let (ready_tx, ready_rx) = oneshot::channel();
        let (stop_tx, stop_rx) = oneshot::channel();
        let stats = Arc::new(Stats::default());
        let worker_stats = stats.clone();
        std::thread::Builder::new().name("r1241-canonical-custody".into()).spawn(move || {
            // No unsafe Send implementation for Writer/Db is needed.
            enable_sql_profile();
            let mut writer = match open() {
                Ok(writer) => writer,
                Err(error) => { let _ = ready_tx.send(Err(error)); let _ = stop_tx.send(()); return; }
            };
            if ready_tx.send(Ok(())).is_err() { let _ = stop_tx.send(()); return; }
            while let Ok(command) = rx.recv() {
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| command(&mut writer)));
                if outcome.is_err() { worker_stats.failed.store(true, Ordering::SeqCst); }
                worker_stats.outstanding.fetch_sub(1, Ordering::SeqCst);
                worker_stats.completed.fetch_add(1, Ordering::SeqCst);
            }
            drop(writer); // Release the canonical lock before reporting stopped.
            let _ = stop_tx.send(());
        }).map_err(|e| format!("R1241 custody thread: {e}"))?;
        ready_rx.await.map_err(|_| "R1241 custody initialization lost")??;
        Ok(Self { tx: Some(tx), stopped: Some(stop_rx), stats })
    }
    pub fn client(&self) -> Result<CustodyClient, String> {
        Ok(CustodyClient {
            tx: self.tx.as_ref().ok_or("R1241 custody closed")?.clone(),
            stats: self.stats.clone(),
        })
    }
    /// FIFO, bounded submission. Await suspends this caller only; the custody
    /// owner remains independent and can be shared with ordered ingress actors.
    pub async fn call<T, F>(&self, operation: F) -> Result<T, String>
    where T: Send + 'static, F: FnOnce(&mut Writer) -> Result<T, String> + Send + 'static {
        self.client()?.call(operation).await
    }
    pub fn profile(&self) -> serde_json::Value {
        let s = &self.stats;
        serde_json::json!({"schema":"SPOTARB/R1241/CANONICAL-CUSTODY/V1",
            "queue_capacity":Self::CAPACITY, "failed":s.failed.load(Ordering::SeqCst),
            "submitted":s.submitted.load(Ordering::SeqCst), "completed":s.completed.load(Ordering::SeqCst),
            "outstanding":s.outstanding.load(Ordering::SeqCst), "highwater_including_active":s.highwater.load(Ordering::SeqCst),
            "overflow":s.overflow.load(Ordering::SeqCst), "cancelled":s.cancelled.load(Ordering::SeqCst),
            "queue_max_ns":s.queue_max_ns.load(Ordering::SeqCst), "service_max_ns":s.service_max_ns.load(Ordering::SeqCst)})
    }
    pub async fn shutdown(mut self) -> Result<(), String> {
        self.tx.take();
        self.stopped.take().ok_or("R1241 missing stop acknowledgement")?.await
            .map_err(|_| "R1241 custody termination lost".into())
    }
}

#[derive(Default)]
struct SqlProfile {
 enabled: bool, count: u64, failed: u64, max_ns: u64,
 samples: std::collections::VecDeque<u64>,
 outliers: std::collections::VecDeque<serde_json::Value>,
 outlier_evictions: u64,
}
thread_local! { static SQL_PROFILE: std::cell::RefCell<SqlProfile> = std::cell::RefCell::new(SqlProfile::default()); }
fn enable_sql_profile() { SQL_PROFILE.with(|p|p.borrow_mut().enabled=true); }
pub(crate) fn record_sql_commit(start_ns:u64,end_ns:u64,ok:bool) {
 SQL_PROFILE.with(|p|{
  let mut p=p.borrow_mut();if !p.enabled{return}
  let elapsed=end_ns.saturating_sub(start_ns);p.count+=1;if !ok{p.failed+=1;}p.max_ns=p.max_ns.max(elapsed);
  if p.samples.len()==4096{p.samples.pop_front();}p.samples.push_back(elapsed);
  if elapsed>=50_000_000{
   if p.outliers.len()==256{p.outliers.pop_front();p.outlier_evictions+=1;}
   p.outliers.push_back(serde_json::json!({"start_ns":start_ns,"end_ns":end_ns,"elapsed_ns":elapsed,"success":ok}));
  }
 });
}
pub fn sql_commit_profile()->serde_json::Value {
 SQL_PROFILE.with(|p|{
  let p=p.borrow();let mut samples=p.samples.iter().copied().collect::<Vec<_>>();samples.sort_unstable();
  let at=|percent:usize|samples.get(samples.len().saturating_sub(1)*percent/100).copied();
  serde_json::json!({"clock":"CLOCK_MONOTONIC","count":p.count,"failed":p.failed,"samples_retained":samples.len(),"sample_evictions":p.count.saturating_sub(samples.len()as u64),"p50_ns":at(50),"p95_ns":at(95),"p99_ns":at(99),"max_ns":p.max_ns,"outliers_ge_50ms":p.outliers,"outlier_evictions":p.outlier_evictions})
 })
}
