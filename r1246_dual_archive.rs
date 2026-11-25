//! R1246 isolated RAM-first opportunity archive.
//! The decision loop performs only bounded RAM queue admission. SQLite I/O,
//! batching, WAL commits, replay, and reconciliation run on independent workers.
use openssl::sha::sha256;
use serde_json::{json, Value};
use std::{
    collections::{HashMap, VecDeque},
    ffi::{c_char, c_int, c_void, CString},
    fs,
    path::{Path, PathBuf},
    ptr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Condvar, Mutex,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

const SQLITE_OK: c_int = 0;
const SQLITE_ROW: c_int = 100;
const SQLITE_DONE: c_int = 101;
const SQLITE_OPEN_READWRITE: c_int = 0x0000_0002;
const SQLITE_OPEN_CREATE: c_int = 0x0000_0004;
const SQLITE_OPEN_FULLMUTEX: c_int = 0x0001_0000;
pub const RAM_RING_BYTES: usize = 64 * 1024 * 1024;
pub const ARCHIVE_QUEUE_BYTES: usize = 64 * 1024 * 1024;
const BATCH_MAX: usize = 16;
const FLUSH_DEADLINE: Duration = Duration::from_millis(5);
const METRIC_SAMPLE_CAP: usize = 10_000;
const DB_NAME: &str = "r1246_opportunities.sqlite3";

#[link(name = "libsqlite3.so.0", kind = "dylib", modifiers = "+verbatim")]
extern "C" {
    fn sqlite3_open_v2(name: *const c_char, db: *mut *mut c_void, flags: c_int, vfs: *const c_char) -> c_int;
    fn sqlite3_close(db: *mut c_void) -> c_int;
    fn sqlite3_errmsg(db: *mut c_void) -> *const c_char;
    fn sqlite3_busy_timeout(db: *mut c_void, ms: c_int) -> c_int;
    fn sqlite3_exec(db: *mut c_void, sql: *const c_char, cb: *const c_void, arg: *mut c_void, err: *mut *mut c_char) -> c_int;
    fn sqlite3_prepare_v2(db: *mut c_void, sql: *const c_char, n: c_int, stmt: *mut *mut c_void, tail: *mut *const c_char) -> c_int;
    fn sqlite3_bind_text(stmt: *mut c_void, pos: c_int, text: *const c_char, len: c_int, destructor: Option<unsafe extern "C" fn(*mut c_void)>) -> c_int;
    fn sqlite3_bind_int64(stmt: *mut c_void, pos: c_int, value: i64) -> c_int;
    fn sqlite3_step(stmt: *mut c_void) -> c_int;
    fn sqlite3_finalize(stmt: *mut c_void) -> c_int;
    fn sqlite3_column_count(stmt: *mut c_void) -> c_int;
    fn sqlite3_column_text(stmt: *mut c_void, index: c_int) -> *const u8;
    fn sqlite3_column_bytes(stmt: *mut c_void, index: c_int) -> c_int;
}

struct Db(*mut c_void);
impl Drop for Db {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe { sqlite3_close(self.0); }
        }
    }
}
struct Stmt(*mut c_void);
impl Drop for Stmt {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe { sqlite3_finalize(self.0); }
        }
    }
}

fn cstring(value: &str) -> Result<CString, String> {
    CString::new(value).map_err(|_| "R1246 embedded NUL".to_string())
}
fn db_error(db: *mut c_void, operation: &str, rc: c_int) -> String {
    let detail = if db.is_null() {
        String::new()
    } else {
        unsafe {
            let p = sqlite3_errmsg(db);
            if p.is_null() { String::new() } else { std::ffi::CStr::from_ptr(p).to_string_lossy().into_owned() }
        }
    };
    format!("R1246 SQLite {operation} rc={rc}: {detail}")
}
fn exec(db: &Db, sql: &str) -> Result<(), String> {
    let sql = cstring(sql)?;
    let rc = unsafe { sqlite3_exec(db.0, sql.as_ptr(), ptr::null(), ptr::null_mut(), ptr::null_mut()) };
    if rc == SQLITE_OK { Ok(()) } else { Err(db_error(db.0, "exec", rc)) }
}
fn open_db(path: &Path) -> Result<Db, String> {
    let path = path.to_str().ok_or("R1246 database path is not UTF-8")?;
    let name = cstring(path)?;
    let mut raw = ptr::null_mut();
    let rc = unsafe {
        sqlite3_open_v2(name.as_ptr(), &mut raw, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, ptr::null())
    };
    if rc != SQLITE_OK || raw.is_null() {
        return Err(db_error(raw, "open", rc));
    }
    let db = Db(raw);
    unsafe { sqlite3_busy_timeout(db.0, 5_000); }
    exec(&db, "PRAGMA journal_mode=WAL;")?;
    exec(&db, "PRAGMA synchronous=FULL;")?;
    exec(&db, "PRAGMA wal_autocheckpoint=1000;")?;
    exec(&db, "CREATE TABLE IF NOT EXISTS opportunity_events(event_id TEXT PRIMARY KEY, sequence INTEGER NOT NULL, checksum TEXT NOT NULL, payload TEXT NOT NULL);")?;
    exec(&db, "CREATE INDEX IF NOT EXISTS opportunity_events_sequence ON opportunity_events(sequence);")?;
    Ok(db)
}
fn query(db: &Db, sql: &str, args: &[QueryArg<'_>]) -> Result<Vec<Vec<String>>, String> {
    let sql = cstring(sql)?;
    let mut raw = ptr::null_mut();
    let rc = unsafe { sqlite3_prepare_v2(db.0, sql.as_ptr(), -1, &mut raw, ptr::null_mut()) };
    if rc != SQLITE_OK || raw.is_null() { return Err(db_error(db.0, "prepare", rc)); }
    let stmt = Stmt(raw);
    let mut keep = Vec::with_capacity(args.len());
    for (index, arg) in args.iter().enumerate() {
        let pos = (index + 1) as c_int;
        let rc = match arg {
            QueryArg::Text(value) => {
                let value = cstring(value)?;
                let rc = unsafe { sqlite3_bind_text(stmt.0, pos, value.as_ptr(), value.as_bytes().len() as c_int, None) };
                keep.push(value);
                rc
            }
        };
        if rc != SQLITE_OK { return Err(db_error(db.0, "bind", rc)); }
    }
    let mut rows = Vec::new();
    loop {
        let rc = unsafe { sqlite3_step(stmt.0) };
        if rc == SQLITE_DONE { break; }
        if rc != SQLITE_ROW { return Err(db_error(db.0, "step", rc)); }
        let mut row = Vec::new();
        for index in 0..unsafe { sqlite3_column_count(stmt.0) } {
            let text = unsafe { sqlite3_column_text(stmt.0, index) };
            let len = unsafe { sqlite3_column_bytes(stmt.0, index) };
            if text.is_null() || len < 0 || len as usize > ARCHIVE_QUEUE_BYTES {
                return Err("R1246 invalid SQLite result cell".into());
            }
            let bytes = unsafe { std::slice::from_raw_parts(text, len as usize) };
            row.push(String::from_utf8(bytes.to_vec()).map_err(|_| "R1246 non-UTF8 SQLite value")?);
        }
        rows.push(row);
        if rows.len() > 10_000_000 { return Err("R1246 excessive SQLite result rows".into()); }
    }
    Ok(rows)
}

#[derive(Clone, Debug)]
struct Record {
    event_id: String,
    sequence: u64,
    checksum: String,
    payload: String,
}
impl Record {
    fn bytes(&self) -> usize {
        self.event_id.len().saturating_add(self.checksum.len()).saturating_add(self.payload.len()).saturating_add(8)
    }
    fn verify(&self) -> Result<(), String> {
        let seq = i64::try_from(self.sequence).map_err(|_| "R1246 sequence exceeds SQLite integer")?;
        let _ = seq;
        if self.event_id.is_empty() || self.checksum.len() != 64 {
            return Err("R1246 malformed archive record".into());
        }
        let actual = hex(&sha256(self.payload.as_bytes()));
        if actual != self.checksum { return Err("R1246 payload checksum mismatch".into()); }
        Ok(())
    }
}
enum QueryArg<'a> { Text(&'a str) }

#[derive(Default)]
struct QueueState {
    events: VecDeque<(Record, Instant)>,
    accounted_bytes: usize,
    high_water_bytes: usize,
    closed: bool,
}
struct ByteQueue {
    state: Mutex<QueueState>,
    changed: Condvar,
    max_bytes: usize,
}
impl ByteQueue {
    fn new(max_bytes: usize) -> Self {
        Self { state: Mutex::new(QueueState::default()), changed: Condvar::new(), max_bytes }
    }
    fn try_push(&self, event: Record) -> Result<(), String> {
        event.verify()?;
        let bytes = event.bytes();
        let mut state = self.state.lock().map_err(|_| "R1246 archive queue poisoned")?;
        if state.closed { return Err("R1246 archive queue closed".into()); }
        if bytes > self.max_bytes || state.accounted_bytes > self.max_bytes.saturating_sub(bytes) {
            return Err(format!("R1246 archive byte queue full: {} + {} > {}", state.accounted_bytes, bytes, self.max_bytes));
        }
        state.accounted_bytes += bytes;
        state.high_water_bytes = state.high_water_bytes.max(state.accounted_bytes);
        state.events.push_back((event, Instant::now()));
        self.changed.notify_one();
        Ok(())
    }
    fn pop_batch(&self) -> Option<(Vec<Record>, Vec<Instant>)> {
        let mut state = self.state.lock().ok()?;
        while state.events.is_empty() && !state.closed {
            state = self.changed.wait(state).ok()?;
        }
        if state.events.is_empty() && state.closed { return None; }
        let first_queued = state.events.front().map(|(_, queued)| *queued).unwrap_or_else(Instant::now);
        let deadline = first_queued + FLUSH_DEADLINE;
        while state.events.len() < BATCH_MAX && !state.closed {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() { break; }
            let (next, _) = self.changed.wait_timeout(state, left).ok()?;
            state = next;
        }
        let count = state.events.len().min(BATCH_MAX);
        let drained = state.events.drain(..count).collect::<Vec<_>>();
        let (events, queued_at) = drained.into_iter().unzip();
        Some((events, queued_at))
    }
    fn complete(&self, bytes: usize) {
        if let Ok(mut state) = self.state.lock() {
            state.accounted_bytes = state.accounted_bytes.saturating_sub(bytes);
            self.changed.notify_all();
        }
    }
    fn push_replay(&self, event: Record, timeout: Duration) -> Result<(), String> {
        event.verify()?;
        let bytes = event.bytes();
        if bytes > self.max_bytes {
            return Err("R1246 replay event exceeds archive queue byte limit".into());
        }
        let deadline = Instant::now() + timeout;
        let mut state = self.state.lock().map_err(|_| "R1246 archive queue poisoned")?;
        loop {
            if state.closed { return Err("R1246 archive queue closed during replay".into()); }
            if state.accounted_bytes <= self.max_bytes.saturating_sub(bytes) {
                state.accounted_bytes += bytes;
                state.high_water_bytes = state.high_water_bytes.max(state.accounted_bytes);
                state.events.push_back((event, Instant::now()));
                self.changed.notify_one();
                return Ok(());
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() { return Err("R1246 startup replay queue admission timeout".into()); }
            let (next, timed) = self.changed.wait_timeout(state, left)
                .map_err(|_| "R1246 archive queue poisoned during replay")?;
            state = next;
            if timed.timed_out() && state.accounted_bytes > self.max_bytes.saturating_sub(bytes) {
                return Err("R1246 startup replay queue admission timeout".into());
            }
        }
    }
    fn close(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.closed = true;
            self.changed.notify_all();
        }
    }
    fn bytes(&self) -> usize {
        self.state.lock().map(|s| s.accounted_bytes).unwrap_or(self.max_bytes)
    }
    fn high_water_bytes(&self) -> usize {
        self.state.lock().map(|s| s.high_water_bytes).unwrap_or(self.max_bytes)
    }
    fn wait_empty(&self, timeout: Duration) -> bool {
        let until = Instant::now() + timeout;
        let mut state = match self.state.lock() { Ok(v) => v, Err(_) => return false };
        while state.accounted_bytes != 0 {
            let left = until.saturating_duration_since(Instant::now());
            if left.is_zero() { return false; }
            match self.changed.wait_timeout(state, left) {
                Ok((next, _)) => state = next,
                Err(_) => return false,
            }
        }
        true
    }
}

#[derive(Default)]
struct WorkerStats {
    durable: AtomicU64,
    batches: AtomicU64,
    commit_ns_total: AtomicU64,
    commit_ns_max: AtomicU64,
    ack_latency_ns: Mutex<VecDeque<u64>>,
    commit_latency_ns: Mutex<VecDeque<u64>>,
    failed: Mutex<Option<String>>,
}
impl WorkerStats {
    fn add_sample(samples: &Mutex<VecDeque<u64>>, sample: u64) {
        if let Ok(mut samples) = samples.lock() {
            samples.push_back(sample);
            if samples.len() > METRIC_SAMPLE_CAP { samples.pop_front(); }
        }
    }
    fn summarize(samples: &Mutex<VecDeque<u64>>) -> Value {
        let mut sorted = match samples.lock() {
            Ok(samples) => samples.iter().copied().collect::<Vec<_>>(),
            Err(_) => return json!({"count":0,"error":"poisoned"}),
        };
        if sorted.is_empty() {
            return json!({"count":0,"p50_ns":null,"p95_ns":null,"p99_ns":null,"max_ns":null});
        }
        sorted.sort_unstable();
        let at = |q: f64| -> u64 {
            let i = (((sorted.len() - 1) as f64) * q).ceil() as usize;
            sorted[i]
        };
        json!({"count":sorted.len(),"p50_ns":at(0.50),"p95_ns":at(0.95),
            "p99_ns":at(0.99),"max_ns":sorted[sorted.len()-1]})
    }
    fn fail(&self, error: String) {
        if let Ok(mut slot) = self.failed.lock() {
            if slot.is_none() { *slot = Some(error); }
        }
    }
    fn error(&self) -> Option<String> { self.failed.lock().ok().and_then(|x| x.clone()) }
}
fn insert_batch(db: &Db, events: &[Record]) -> Result<(), String> {
    exec(db, "BEGIN IMMEDIATE;")?;
    let result = (|| {
        for event in events {
            event.verify()?;
            let seq = i64::try_from(event.sequence).map_err(|_| "R1246 sequence exceeds SQLite integer")?;
            let sql = cstring("INSERT OR IGNORE INTO opportunity_events(event_id,sequence,checksum,payload) VALUES(?1,?2,?3,?4)")?;
            let mut raw = ptr::null_mut();
            let rc = unsafe { sqlite3_prepare_v2(db.0, sql.as_ptr(), -1, &mut raw, ptr::null_mut()) };
            if rc != SQLITE_OK || raw.is_null() { return Err(db_error(db.0, "insert prepare", rc)); }
            let stmt = Stmt(raw);
            let id = cstring(&event.event_id)?;
            let checksum = cstring(&event.checksum)?;
            let payload = cstring(&event.payload)?;
            for rc in [
                unsafe { sqlite3_bind_text(stmt.0, 1, id.as_ptr(), id.as_bytes().len() as c_int, None) },
                unsafe { sqlite3_bind_int64(stmt.0, 2, seq) },
                unsafe { sqlite3_bind_text(stmt.0, 3, checksum.as_ptr(), checksum.as_bytes().len() as c_int, None) },
                unsafe { sqlite3_bind_text(stmt.0, 4, payload.as_ptr(), payload.as_bytes().len() as c_int, None) },
            ] {
                if rc != SQLITE_OK { return Err(db_error(db.0, "insert bind", rc)); }
            }
            let rc = unsafe { sqlite3_step(stmt.0) };
            if rc != SQLITE_DONE { return Err(db_error(db.0, "insert step", rc)); }
            drop(stmt);
            let rows = query(db, "SELECT checksum FROM opportunity_events WHERE event_id=?1", &[QueryArg::Text(&event.event_id)])?;
            if rows.len() != 1 || rows[0].len() != 1 || rows[0][0] != event.checksum {
                return Err("R1246 event ID checksum conflict".into());
            }
        }
        Ok(())
    })();
    if let Err(error) = result {
        let _ = exec(db, "ROLLBACK;");
        return Err(error);
    }
    exec(db, "COMMIT;")
}
fn records_at(path: &Path) -> Result<HashMap<String, Record>, String> {
    let db = open_db(path)?;
    let rows = query(&db, "SELECT event_id,sequence,checksum,payload FROM opportunity_events ORDER BY sequence,event_id", &[])?;
    let mut out = HashMap::with_capacity(rows.len());
    for row in rows {
        if row.len() != 4 { return Err("R1246 archive row shape".into()); }
        let event = Record {
            event_id: row[0].clone(),
            sequence: row[1].parse().map_err(|_| "R1246 invalid stored sequence")?,
            checksum: row[2].clone(),
            payload: row[3].clone(),
        };
        event.verify()?;
        if let Some(old) = out.insert(event.event_id.clone(), event.clone()) {
            if old.checksum != event.checksum || old.payload != event.payload {
                return Err("R1246 duplicate event ID checksum conflict".into());
            }
        }
    }
    Ok(out)
}
fn worker_loop(path: PathBuf, queue: Arc<ByteQueue>, stats: Arc<WorkerStats>, ready: std::sync::mpsc::SyncSender<Result<(), String>>) {
    let db = match open_db(&path) {
        Ok(db) => {
            let _ = ready.send(Ok(()));
            db
        }
        Err(error) => {
            stats.fail(error.clone());
            let _ = ready.send(Err(error));
            return;
        }
    };
    while let Some((batch, queued_at)) = queue.pop_batch() {
        let bytes = batch.iter().map(Record::bytes).sum();
        let started = Instant::now();
        match insert_batch(&db, &batch) {
            Ok(()) => {
                let elapsed = started.elapsed().as_nanos().min(u64::MAX as u128) as u64;
                stats.commit_ns_total.fetch_add(elapsed, Ordering::Relaxed);
                stats.commit_ns_max.fetch_max(elapsed, Ordering::Relaxed);
                WorkerStats::add_sample(&stats.commit_latency_ns, elapsed);
                let ack_at = Instant::now();
                for queued in queued_at {
                    WorkerStats::add_sample(
                        &stats.ack_latency_ns,
                        ack_at.saturating_duration_since(queued).as_nanos().min(u64::MAX as u128) as u64,
                    );
                }
                stats.batches.fetch_add(1, Ordering::Relaxed);
                stats.durable.fetch_add(batch.len() as u64, Ordering::Relaxed);
                queue.complete(bytes);
            }
            Err(error) => {
                stats.fail(error);
                queue.close();
                return;
            }
        }
    }
}

struct Worker {
    path: PathBuf,
    queue: Arc<ByteQueue>,
    stats: Arc<WorkerStats>,
    join: Option<JoinHandle<()>>,
}
impl Worker {
    fn start(path: PathBuf, queue: Arc<ByteQueue>, stats: Arc<WorkerStats>) -> Result<Self, String> {
        let worker_path = path.clone();
        let worker_queue = queue.clone();
        let worker_stats = stats.clone();
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        let join = thread::Builder::new().name("r1246-archive-writer".into())
            .spawn(move || worker_loop(worker_path, worker_queue, worker_stats, ready_tx))
            .map_err(|e| format!("R1246 archive thread spawn: {e}"))?;
        match ready_rx.recv_timeout(Duration::from_secs(10)) {
            Ok(Ok(())) => Ok(Self { path, queue, stats, join: Some(join) }),
            Ok(Err(error)) => { let _ = join.join(); Err(error) }
            Err(error) => { queue.close(); let _ = join.join(); Err(format!("R1246 archive writer start timeout: {error}")) }
        }
    }
    fn stop(&mut self) {
        self.queue.close();
        if let Some(join) = self.join.take() { let _ = join.join(); }
    }
}
impl Drop for Worker {
    fn drop(&mut self) { self.stop(); }
}

struct Ring {
    items: VecDeque<Record>,
    bytes: usize,
}
impl Ring {
    fn new() -> Self { Self { items: VecDeque::new(), bytes: 0 } }
    fn push(&mut self, event: Record) -> Result<(), String> {
        let bytes = event.bytes();
        if bytes > RAM_RING_BYTES { return Err("R1246 event exceeds RAM ring capacity".into()); }
        self.bytes += bytes;
        self.items.push_back(event);
        while self.bytes > RAM_RING_BYTES {
            if let Some(old) = self.items.pop_front() { self.bytes = self.bytes.saturating_sub(old.bytes()); }
            else { break; }
        }
        Ok(())
    }
}

pub struct DualArchive {
    session_id: String,
    sequence: AtomicU64,
    ring: Mutex<Ring>,
    workers: Vec<Worker>,
    enqueued: AtomicU64,
    replayed_sdb: u64,
    replayed_sdc: u64,
    queue_admission_failures: AtomicU64,
    fail_closed_pushes: AtomicU64,
}
impl DualArchive {
    pub fn open(sdb_dir: impl AsRef<Path>, sdc_dir: impl AsRef<Path>, session_id: String) -> Result<Self, String> {
        if session_id.is_empty() || session_id.len() > 96 || !session_id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_') {
            return Err("R1246 invalid session ID".into());
        }
        let sdb_dir = sdb_dir.as_ref();
        let sdc_dir = sdc_dir.as_ref();
        fs::create_dir_all(sdb_dir).map_err(|e| format!("R1246 create sdb archive directory: {e}"))?;
        fs::create_dir_all(sdc_dir).map_err(|e| format!("R1246 create sdc archive directory: {e}"))?;
        let sdb_path = sdb_dir.join(DB_NAME);
        let sdc_path = sdc_dir.join(DB_NAME);

        let sdb_existing = records_at(&sdb_path)?;
        let sdc_existing = records_at(&sdc_path)?;
        let mut all = sdb_existing.clone();
        for (id, event) in &sdc_existing {
            if let Some(old) = all.get(id) {
                if old.checksum != event.checksum || old.payload != event.payload {
                    return Err(format!("R1246 cross-copy checksum conflict for {id}"));
                }
            } else {
                all.insert(id.clone(), event.clone());
            }
        }
        let sdb_missing = all.iter().filter(|(id, _)| !sdb_existing.contains_key(*id)).map(|(_, e)| e.clone()).collect::<Vec<_>>();
        let sdc_missing = all.iter().filter(|(id, _)| !sdc_existing.contains_key(*id)).map(|(_, e)| e.clone()).collect::<Vec<_>>();
        let replayed_sdb = u64::try_from(sdb_missing.len()).map_err(|_| "R1246 replay count overflow")?;
        let replayed_sdc = u64::try_from(sdc_missing.len()).map_err(|_| "R1246 replay count overflow")?;
        let expected_prefix = format!("{session_id}-");
        let mut max_sequence = 0_u64;
        for event in all.values() {
            let suffix = event.event_id.strip_prefix(&expected_prefix)
                .ok_or("R1246 archive contains event from another session")?;
            if suffix.len() != 20 || !suffix.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err("R1246 archived event ID has invalid sequence".into());
            }
            let sequence = suffix.parse::<u64>().map_err(|_| "R1246 archived sequence invalid")?;
            if sequence != event.sequence || event.event_id != format!("{session_id}-{sequence:020}") {
                return Err("R1246 archived ID/sequence mismatch".into());
            }
            max_sequence = max_sequence.max(sequence);
        }

        let sdb_queue = Arc::new(ByteQueue::new(ARCHIVE_QUEUE_BYTES));
        let sdc_queue = Arc::new(ByteQueue::new(ARCHIVE_QUEUE_BYTES));
        let mut ring = Ring::new();
        let mut ordered = all.values().cloned().collect::<Vec<_>>();
        ordered.sort_by(|a, b| a.sequence.cmp(&b.sequence).then_with(|| a.event_id.cmp(&b.event_id)));
        for event in ordered { ring.push(event)?; }
        let sdb_stats = Arc::new(WorkerStats::default());
        let sdc_stats = Arc::new(WorkerStats::default());
        let sdb_existing_count = u64::try_from(sdb_existing.len()).map_err(|_| "R1246 archive row count overflow")?;
        let sdc_existing_count = u64::try_from(sdc_existing.len()).map_err(|_| "R1246 archive row count overflow")?;
        sdb_stats.durable.store(sdb_existing_count, Ordering::Relaxed);
        sdc_stats.durable.store(sdc_existing_count, Ordering::Relaxed);
        let total_existing = u64::try_from(all.len()).map_err(|_| "R1246 archive row count overflow")?;
        let sdb_worker = Worker::start(sdb_path, sdb_queue, sdb_stats)?;
        let sdc_worker = Worker::start(sdc_path, sdc_queue, sdc_stats)?;
        for event in sdb_missing { sdb_worker.queue.push_replay(event, Duration::from_secs(300))?; }
        for event in sdc_missing { sdc_worker.queue.push_replay(event, Duration::from_secs(300))?; }
        for worker in [&sdb_worker, &sdc_worker] {
            if !worker.queue.wait_empty(Duration::from_secs(300)) {
                return Err("R1246 startup replay drain timeout".into());
            }
            if let Some(error) = worker.stats.error() {
                return Err(format!("R1246 startup replay writer failed: {error}"));
            }
        }
        Ok(Self {
            session_id,
            sequence: AtomicU64::new(max_sequence),
            ring: Mutex::new(ring),
            workers: vec![sdb_worker, sdc_worker],
            enqueued: AtomicU64::new(total_existing),
            replayed_sdb,
            replayed_sdc,
            queue_admission_failures: AtomicU64::new(0),
            fail_closed_pushes: AtomicU64::new(0),
        })
    }
    pub fn push(&self, kind: &str, fields: Value) -> Result<String, String> {
        if kind.is_empty() || kind.len() > 64 || !kind.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-') {
            return Err("R1246 invalid event kind".into());
        }
        if self.workers.len() != 2 {
            self.fail_closed_pushes.fetch_add(1, Ordering::Relaxed);
            return Err("R1246 dual writer unavailable".into());
        }
        for worker in &self.workers {
            if let Some(error) = worker.stats.error() {
                self.fail_closed_pushes.fetch_add(1, Ordering::Relaxed);
                return Err(format!("R1246 writer failed: {error}"));
            }
        }
        let previous = self.sequence.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| current.checked_add(1))
            .map_err(|_| "R1246 sequence overflow")?;
        let sequence = previous.checked_add(1).ok_or("R1246 sequence overflow")?;
        let event_id = format!("{}-{sequence:020}", self.session_id);
        let value = json!({
            "schema":"SPOTARB/R1246/OPPORTUNITY_ARCHIVE/V1",
            "event_id":event_id,
            "session_id":self.session_id,
            "sequence":sequence,
            "kind":kind,
            "event":fields
        });
        let payload = serde_json::to_string(&value).map_err(|e| format!("R1246 serialize event: {e}"))?;
        let event = Record {
            event_id: event_id.clone(),
            sequence,
            checksum: hex(&sha256(payload.as_bytes())),
            payload,
        };
        event.verify()?;
        self.ring.lock().map_err(|_| "R1246 RAM ring poisoned")?.push(event.clone())?;
        if let Err(error) = self.workers[0].queue.try_push(event.clone()) {
            self.queue_admission_failures.fetch_add(1, Ordering::Relaxed);
            self.fail_closed_pushes.fetch_add(1, Ordering::Relaxed);
            return Err(error);
        }
        self.enqueued.fetch_add(1, Ordering::Relaxed);
        if let Err(error) = self.workers[1].queue.try_push(event) {
            self.queue_admission_failures.fetch_add(1, Ordering::Relaxed);
            self.fail_closed_pushes.fetch_add(1, Ordering::Relaxed);
            return Err(format!("R1246 second-copy admission failed; PAPER entry must fail closed: {error}"));
        }
        Ok(event_id)
    }
    pub fn metrics(&self) -> Value {
        let sdb = &self.workers[0];
        let sdc = &self.workers[1];
        let a = sdb.stats.durable.load(Ordering::Relaxed);
        let b = sdc.stats.durable.load(Ordering::Relaxed);
        json!({
            "ram_ring_bytes":self.ring.lock().map(|r|r.bytes).unwrap_or(RAM_RING_BYTES),
            "ram_ring_capacity_bytes":RAM_RING_BYTES,
            "sdb_queue_bytes":sdb.queue.bytes(),
            "sdc_queue_bytes":sdc.queue.bytes(),
            "sdb_queue_high_water_bytes":sdb.queue.high_water_bytes(),
            "sdc_queue_high_water_bytes":sdc.queue.high_water_bytes(),
            "archive_queue_capacity_bytes":ARCHIVE_QUEUE_BYTES,
            "enqueued":self.enqueued.load(Ordering::Relaxed),
            "durable_sdb":a,
            "durable_sdc":b,
            "both_durable_lower_bound":a.min(b),
            "pending":self.enqueued.load(Ordering::Relaxed).saturating_sub(a.min(b)),
            "sdb_batches":sdb.stats.batches.load(Ordering::Relaxed),
            "sdc_batches":sdc.stats.batches.load(Ordering::Relaxed),
            "sdb_commit_ns_total":sdb.stats.commit_ns_total.load(Ordering::Relaxed),
            "sdc_commit_ns_total":sdc.stats.commit_ns_total.load(Ordering::Relaxed),
            "sdb_commit_ns_max":sdb.stats.commit_ns_max.load(Ordering::Relaxed),
            "sdc_commit_ns_max":sdc.stats.commit_ns_max.load(Ordering::Relaxed),
            "sdb_ack_latency_ns":WorkerStats::summarize(&sdb.stats.ack_latency_ns),
            "sdc_ack_latency_ns":WorkerStats::summarize(&sdc.stats.ack_latency_ns),
            "sdb_commit_latency_ns":WorkerStats::summarize(&sdb.stats.commit_latency_ns),
            "sdc_commit_latency_ns":WorkerStats::summarize(&sdc.stats.commit_latency_ns),
            "queue_admission_failures":self.queue_admission_failures.load(Ordering::Relaxed),
            "fail_closed_pushes":self.fail_closed_pushes.load(Ordering::Relaxed),
            "sdb_failure":sdb.stats.error(),
            "sdc_failure":sdc.stats.error(),
            "replayed_to_sdb":self.replayed_sdb,
            "replayed_to_sdc":self.replayed_sdc,
            "synchronous":"FULL",
            "journal_mode":"WAL",
            "batch_max":BATCH_MAX,
            "flush_deadline_ms":FLUSH_DEADLINE.as_millis()
        })
    }
    pub fn drain_and_verify(&self, timeout: Duration) -> Result<Value, String> {
        let deadline = Instant::now() + timeout;
        for worker in &self.workers {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() || !worker.queue.wait_empty(left) {
                return Err("R1246 archive drain timeout".into());
            }
            if let Some(error) = worker.stats.error() { return Err(format!("R1246 archive writer failed: {error}")); }
        }
        let a = records_at(&self.workers[0].path)?;
        let b = records_at(&self.workers[1].path)?;
        if a.len() != b.len() { return Err(format!("R1246 archive count mismatch sdb={} sdc={}", a.len(), b.len())); }
        for (id, event) in &a {
            let other = b.get(id).ok_or_else(|| format!("R1246 sdc missing {id}"))?;
            if event.checksum != other.checksum || event.payload != other.payload {
                return Err(format!("R1246 archive ID/hash mismatch for {id}"));
            }
        }
        for path in [&self.workers[0].path, &self.workers[1].path] {
            let db = open_db(path)?;
            let rows = query(&db, "PRAGMA integrity_check;", &[])?;
            if rows.len() != 1 || rows[0].first().map(String::as_str) != Some("ok") {
                return Err(format!("R1246 SQLite integrity_check failed for {}", path.display()));
            }
        }
        let mut report = self.metrics();
        report["archive_rows_sdb"] = json!(a.len());
        report["archive_rows_sdc"] = json!(b.len());
        report["cross_copy_id_checksum_match"] = json!(true);
        report["integrity_check"] = json!("ok");
        report["pending"] = json!(0);
        Ok(report)
    }
    pub fn stop(&mut self) {
        for worker in &mut self.workers { worker.stop(); }
    }
}
impl Drop for DualArchive {
    fn drop(&mut self) { self.stop(); }
}
fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8;16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes { out.push(DIGITS[(b >> 4) as usize] as char); out.push(DIGITS[(b & 15) as usize] as char); }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    fn temp_dir(label: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("r1246-{label}-{}-{}", std::process::id(), Instant::now().elapsed().as_nanos()));
        let path = path.with_extension(format!("{}", SystemTimeNanos::now()));
        fs::create_dir_all(&path).unwrap();
        path
    }
    struct SystemTimeNanos;
    impl SystemTimeNanos {
        fn now() -> u128 {
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        }
    }
    fn session() -> String { format!("test-{}", SystemTimeNanos::now()) }
    fn record(id: &str, seq: u64, payload: &str) -> Record {
        Record { event_id:id.into(), sequence:seq, checksum:hex(&sha256(payload.as_bytes())), payload:payload.into() }
    }
    #[test]
    fn byte_queue_enforces_actual_payload_bytes() {
        let queue = ByteQueue::new(160);
        let first = record("a", 1, &"x".repeat(80));
        assert!(queue.try_push(first).is_ok());
        let second = record("b", 2, &"y".repeat(80));
        assert!(queue.try_push(second).is_err());
        assert!(queue.bytes() > 0);
    }
    #[test]
    fn dual_copy_dedup_replay_and_integrity() {
        let root = temp_dir("replay");
        let a = root.join("sdb");
        let b = root.join("sdc");
        let session_id = session();
        {
            let archive = DualArchive::open(&a, &b, session_id.clone()).unwrap();
            for n in 0..12 {
                archive.push("detected", json!({"n":n,"verdict":"rejected"})).unwrap();
            }
            let report = archive.drain_and_verify(Duration::from_secs(10)).unwrap();
            assert_eq!(report["archive_rows_sdb"], 12);
            assert_eq!(report["archive_rows_sdc"], 12);
            assert_eq!(report["sdb_ack_latency_ns"]["count"], 12);
            assert_eq!(report["sdc_ack_latency_ns"]["count"], 12);
            assert!(report["sdb_commit_latency_ns"]["count"].as_u64().unwrap() > 0);
            assert!(report["sdb_queue_high_water_bytes"].as_u64().unwrap() > 0);
            assert_eq!(report["queue_admission_failures"], 0);
            assert_eq!(report["fail_closed_pushes"], 0);
        }
        let sdb_path = a.join(DB_NAME);
        let sdb = records_at(&sdb_path).unwrap();
        {
            let db = open_db(&sdb_path).unwrap();
            let repeated = sdb.values().next().unwrap().clone();
            insert_batch(&db, std::slice::from_ref(&repeated)).unwrap();
            assert_eq!(records_at(&sdb_path).unwrap().len(), 12);
        }
        for suffix in ["", "-wal", "-shm"] {
            let path = b.join(format!("{DB_NAME}{suffix}"));
            let _ = fs::remove_file(path);
        }
        {
            let archive = DualArchive::open(&a, &b, session_id.clone()).unwrap();
            assert_eq!(archive.metrics()["replayed_to_sdb"], 0);
            assert_eq!(archive.metrics()["replayed_to_sdc"], 12);
            assert_eq!(archive.metrics()["pending"], 0);
            let id = archive.push("after-restart", json!({"n":12})).unwrap();
            assert_eq!(id, format!("{session_id}-{:020}", 13));
            let report = archive.drain_and_verify(Duration::from_secs(10)).unwrap();
            assert_eq!(report["archive_rows_sdb"], 13);
            assert_eq!(report["archive_rows_sdc"], 13);
        }
        for suffix in ["", "-wal", "-shm"] {
            let path = a.join(format!("{DB_NAME}{suffix}"));
            let _ = fs::remove_file(path);
        }
        {
            let archive = DualArchive::open(&a, &b, session_id.clone()).unwrap();
            assert_eq!(archive.metrics()["replayed_to_sdb"], 13);
            assert_eq!(archive.metrics()["replayed_to_sdc"], 0);
            let id = archive.push("after-second-restart", json!({"n":13})).unwrap();
            assert_eq!(id, format!("{session_id}-{:020}", 14));
            let report = archive.drain_and_verify(Duration::from_secs(10)).unwrap();
            assert_eq!(report["archive_rows_sdb"], 14);
            assert_eq!(report["archive_rows_sdc"], 14);
        }
        let _ = fs::remove_dir_all(root);
    }
    #[test]
    fn checksum_mismatch_is_rejected() {
        let root = temp_dir("conflict");
        let a = root.join("sdb");
        let b = root.join("sdc");
        fs::create_dir_all(&a).unwrap();
        fs::create_dir_all(&b).unwrap();
        let path_a = a.join(DB_NAME);
        let path_b = b.join(DB_NAME);
        let original = record("same-id", 1, "payload-a");
        let conflict = record("same-id", 1, "payload-b");
        for (path, event) in [(&path_a,&original),(&path_b,&conflict)] {
            let db = open_db(path).unwrap();
            insert_batch(&db,std::slice::from_ref(event)).unwrap();
        }
        let error = match DualArchive::open(&a,&b,session()) { Ok(_) => panic!("checksum conflict admitted"), Err(error) => error };
        assert!(error.contains("checksum conflict"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn stored_payload_checksum_corruption_is_rejected() {
        let root = temp_dir("corrupt-checksum");
        let a = root.join("sdb");
        let b = root.join("sdc");
        fs::create_dir_all(&a).unwrap();
        fs::create_dir_all(&b).unwrap();
        let path = a.join(DB_NAME);
        let event = record("corrupt-id", 1, "intact-payload");
        let db = open_db(&path).unwrap();
        insert_batch(&db, std::slice::from_ref(&event)).unwrap();
        exec(&db, "UPDATE opportunity_events SET checksum='0000000000000000000000000000000000000000000000000000000000000000' WHERE event_id='corrupt-id';").unwrap();
        drop(db);
        let result = DualArchive::open(&a, &b, session());
        let error = match result { Ok(_) => panic!("corrupted payload checksum admitted"), Err(error) => error };
        assert!(error.contains("payload checksum mismatch"));
        let _ = fs::remove_dir_all(root);
    }

    fn wait_durable(archive: &DualArchive, key: &str, target: u64, timeout: Duration) -> bool {
        let until = Instant::now() + timeout;
        while Instant::now() < until {
            if archive.metrics()[key].as_u64().unwrap_or(0) >= target { return true; }
            thread::sleep(Duration::from_millis(5));
        }
        false
    }

    #[test]
    fn one_writer_pause_keeps_other_copy_durable_and_drains_after_release() {
        let root = temp_dir("writer-pause");
        let a = root.join("sdb");
        let b = root.join("sdc");
        let archive = DualArchive::open(&a, &b, session()).unwrap();

        let lock_a = open_db(&a.join(DB_NAME)).unwrap();
        exec(&lock_a, "BEGIN IMMEDIATE;").unwrap();
        for n in 0..8 { archive.push("pause-a", json!({"n":n})).unwrap(); }
        assert!(wait_durable(&archive, "durable_sdc", 8, Duration::from_secs(2)));
        assert_eq!(archive.metrics()["durable_sdb"], 0);
        assert!(archive.metrics()["sdb_failure"].is_null());
        exec(&lock_a, "ROLLBACK;").unwrap();
        drop(lock_a);
        archive.drain_and_verify(Duration::from_secs(10)).unwrap();

        let lock_b = open_db(&b.join(DB_NAME)).unwrap();
        exec(&lock_b, "BEGIN IMMEDIATE;").unwrap();
        for n in 8..16 { archive.push("pause-b", json!({"n":n})).unwrap(); }
        assert!(wait_durable(&archive, "durable_sdb", 16, Duration::from_secs(2)));
        assert_eq!(archive.metrics()["durable_sdc"], 8);
        assert!(archive.metrics()["sdc_failure"].is_null());
        exec(&lock_b, "ROLLBACK;").unwrap();
        drop(lock_b);
        let report = archive.drain_and_verify(Duration::from_secs(10)).unwrap();
        assert_eq!(report["archive_rows_sdb"], 16);
        assert_eq!(report["archive_rows_sdc"], 16);
        assert_eq!(report["cross_copy_id_checksum_match"], true);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn both_writer_failures_never_claim_durable_ack() {
        let root = temp_dir("both-writers-fail");
        let a = root.join("sdb");
        let b = root.join("sdc");
        let archive = DualArchive::open(&a, &b, session()).unwrap();
        let lock_a = open_db(&a.join(DB_NAME)).unwrap();
        let lock_b = open_db(&b.join(DB_NAME)).unwrap();
        exec(&lock_a, "BEGIN IMMEDIATE;").unwrap();
        exec(&lock_b, "BEGIN IMMEDIATE;").unwrap();

        for n in 0..32 {
            if archive.push("both-locked", json!({"n":n})).is_err() { break; }
        }
        let until = Instant::now() + Duration::from_secs(7);
        loop {
            let metrics = archive.metrics();
            let a_failed = !metrics["sdb_failure"].is_null();
            let b_failed = !metrics["sdc_failure"].is_null();
            if a_failed && b_failed { break; }
            assert!(Instant::now() < until, "writer locks did not trip both bounded busy timeouts");
            thread::sleep(Duration::from_millis(10));
        }
        assert!(archive.push("after-both-failed", json!({"n":"denied"})).is_err());
        let metrics = archive.metrics();
        assert_eq!(metrics["durable_sdb"], 0);
        assert_eq!(metrics["durable_sdc"], 0);
        assert_eq!(metrics["both_durable_lower_bound"], 0);
        assert!(metrics["fail_closed_pushes"].as_u64().unwrap() > 0);
        assert!(archive.drain_and_verify(Duration::from_millis(100)).is_err());

        exec(&lock_a, "ROLLBACK;").unwrap();
        exec(&lock_b, "ROLLBACK;").unwrap();
        drop(lock_a);
        drop(lock_b);
        let _ = fs::remove_dir_all(root);
    }
}
