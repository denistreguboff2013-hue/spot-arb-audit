//! R24 non-blocking compressed public market evidence with bounded retention.
//!
//! Hot path: Arc<RawFrame> -> try_send only.
//! Writer, compressor, fsync and retention run off the market thread.

use crate::{async_evidence::{EvidenceLine, EvidenceSubmit}, RawFrame};
use openssl::sha::sha256;
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    ffi::CString,
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, BufWriter, Write},
    os::unix::ffi::OsStrExt,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        mpsc::{self, Receiver, SyncSender, TrySendError},
        Arc,
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

pub const ZSTD_BIN: &str = "/usr/bin/zstd";
pub const DEFAULT_QUEUE_BOUND: usize = 8192;
pub const DEFAULT_COMPRESSION_QUEUE_BOUND: usize = 4;
pub const DEFAULT_SEGMENT_MAX_BYTES: u64 = 64 * 1024 * 1024;
pub const DEFAULT_SYNC_INTERVAL_MS: u64 = 1_000;
pub const DEFAULT_ZSTD_LEVEL: i32 = 3;
pub const DEFAULT_MAX_RETAINED_BYTES: u64 = 8 * 1024 * 1024 * 1024;
pub const DEFAULT_MIN_FREE_BYTES: u64 = 4 * 1024 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct CompressedEvidenceConfig {
    pub queue_bound: usize,
    pub compression_queue_bound: usize,
    pub segment_max_bytes: u64,
    pub sync_interval: Duration,
    pub zstd_level: i32,
    pub max_retained_bytes: u64,
    pub min_free_bytes: u64,
}

impl Default for CompressedEvidenceConfig {
    fn default() -> Self {
        Self {
            queue_bound: DEFAULT_QUEUE_BOUND,
            compression_queue_bound: DEFAULT_COMPRESSION_QUEUE_BOUND,
            segment_max_bytes: DEFAULT_SEGMENT_MAX_BYTES,
            sync_interval: Duration::from_millis(DEFAULT_SYNC_INTERVAL_MS),
            zstd_level: DEFAULT_ZSTD_LEVEL,
            max_retained_bytes: DEFAULT_MAX_RETAINED_BYTES,
            min_free_bytes: DEFAULT_MIN_FREE_BYTES,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompressedEvidenceStats {
    pub offered: u64,
    pub queued: u64,
    pub written: u64,
    pub dropped_full: u64,
    pub dropped_closed: u64,
    pub write_errors: u64,
    pub queue_peak: usize,
    pub outstanding: usize,
    pub raw_bytes_written: u64,
    pub raw_segments_closed: u64,
    pub sync_calls: u64,
    pub last_written_offer_seq: u64,
    pub last_synced_offer_seq: u64,
    pub compression_enqueued: u64,
    pub compression_deferred: u64,
    pub compression_queue_peak: usize,
    pub compression_outstanding: usize,
    pub compressed_segments: u64,
    pub compression_errors: u64,
    pub compressed_bytes_created: u64,
    pub raw_bytes_deleted_after_compress: u64,
    pub retained_segments: u64,
    pub retained_bytes: u64,
    pub retention_runs: u64,
    pub retention_deleted_segments: u64,
    pub retention_deleted_records: u64,
    pub retention_deleted_bytes: u64,
    pub retention_deleted_through_seq: u64,
    pub retention_errors: u64,
    pub retention_pressure_unresolved: u64,
}

#[derive(Default)]
struct Stats {
    offered: AtomicU64,
    queued: AtomicU64,
    written: AtomicU64,
    dropped_full: AtomicU64,
    dropped_closed: AtomicU64,
    write_errors: AtomicU64,
    queue_peak: AtomicUsize,
    outstanding: AtomicUsize,
    raw_bytes_written: AtomicU64,
    raw_segments_closed: AtomicU64,
    sync_calls: AtomicU64,
    last_written_offer_seq: AtomicU64,
    last_synced_offer_seq: AtomicU64,
    compression_enqueued: AtomicU64,
    compression_deferred: AtomicU64,
    compression_queue_peak: AtomicUsize,
    compression_outstanding: AtomicUsize,
    compressed_segments: AtomicU64,
    compression_errors: AtomicU64,
    compressed_bytes_created: AtomicU64,
    raw_bytes_deleted_after_compress: AtomicU64,
    retained_segments: AtomicU64,
    retained_bytes: AtomicU64,
    retention_runs: AtomicU64,
    retention_deleted_segments: AtomicU64,
    retention_deleted_records: AtomicU64,
    retention_deleted_bytes: AtomicU64,
    retention_deleted_through_seq: AtomicU64,
    retention_errors: AtomicU64,
    retention_pressure_unresolved: AtomicU64,
}

impl Stats {
    fn snapshot(&self) -> CompressedEvidenceStats {
        CompressedEvidenceStats {
            offered: self.offered.load(Ordering::Relaxed),
            queued: self.queued.load(Ordering::Relaxed),
            written: self.written.load(Ordering::Relaxed),
            dropped_full: self.dropped_full.load(Ordering::Relaxed),
            dropped_closed: self.dropped_closed.load(Ordering::Relaxed),
            write_errors: self.write_errors.load(Ordering::Relaxed),
            queue_peak: self.queue_peak.load(Ordering::Relaxed),
            outstanding: self.outstanding.load(Ordering::Relaxed),
            raw_bytes_written: self.raw_bytes_written.load(Ordering::Relaxed),
            raw_segments_closed: self.raw_segments_closed.load(Ordering::Relaxed),
            sync_calls: self.sync_calls.load(Ordering::Relaxed),
            last_written_offer_seq: self.last_written_offer_seq.load(Ordering::Relaxed),
            last_synced_offer_seq: self.last_synced_offer_seq.load(Ordering::Relaxed),
            compression_enqueued: self.compression_enqueued.load(Ordering::Relaxed),
            compression_deferred: self.compression_deferred.load(Ordering::Relaxed),
            compression_queue_peak: self.compression_queue_peak.load(Ordering::Relaxed),
            compression_outstanding: self.compression_outstanding.load(Ordering::Relaxed),
            compressed_segments: self.compressed_segments.load(Ordering::Relaxed),
            compression_errors: self.compression_errors.load(Ordering::Relaxed),
            compressed_bytes_created: self.compressed_bytes_created.load(Ordering::Relaxed),
            raw_bytes_deleted_after_compress: self.raw_bytes_deleted_after_compress.load(Ordering::Relaxed),
            retained_segments: self.retained_segments.load(Ordering::Relaxed),
            retained_bytes: self.retained_bytes.load(Ordering::Relaxed),
            retention_runs: self.retention_runs.load(Ordering::Relaxed),
            retention_deleted_segments: self.retention_deleted_segments.load(Ordering::Relaxed),
            retention_deleted_records: self.retention_deleted_records.load(Ordering::Relaxed),
            retention_deleted_bytes: self.retention_deleted_bytes.load(Ordering::Relaxed),
            retention_deleted_through_seq: self.retention_deleted_through_seq.load(Ordering::Relaxed),
            retention_errors: self.retention_errors.load(Ordering::Relaxed),
            retention_pressure_unresolved: self.retention_pressure_unresolved.load(Ordering::Relaxed),
        }
    }
}

struct Job {
    offer_seq: u64,
    frame: Arc<RawFrame>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SegmentMeta {
    segment: u64,
    first_seq: u64,
    last_seq: u64,
    records: u64,
    raw_bytes: u64,
    compressed_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RetentionEntry {
    segment: u64,
    first_seq: u64,
    last_seq: u64,
    records: u64,
    compressed_bytes: u64,
    deleted_wall_ms: u64,
    reason: String,
}

struct CompressionJob {
    segment: u64,
    raw_path: PathBuf,
    first_seq: u64,
    last_seq: u64,
    records: u64,
    raw_bytes: u64,
}

struct Retained {
    segment: u64,
    zst_path: PathBuf,
    meta_path: PathBuf,
    first_seq: u64,
    last_seq: u64,
    records: u64,
    compressed_bytes: u64,
}

pub struct AsyncCompressedEvidenceWriter {
    tx: Option<SyncSender<Job>>,
    stats: Arc<Stats>,
    join: Option<thread::JoinHandle<io::Result<()>>>,
}

impl AsyncCompressedEvidenceWriter {
    pub fn start(root: impl AsRef<Path>, cfg: CompressedEvidenceConfig) -> io::Result<Self> {
        validate_config(&cfg)?;
        if !Path::new(ZSTD_BIN).is_file() {
            return Err(io::Error::new(io::ErrorKind::NotFound, "fixed zstd binary missing"));
        }
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        let canon = root.canonicalize()?;
        if !canon.to_string_lossy().starts_with("/mnt/spotarb_db_test/r24_") {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "R24 evidence root must stay under /mnt/spotarb_db_test/r24_*",
            ));
        }
        if fs::read_dir(&canon)?.next().is_some() {
            return Err(io::Error::new(io::ErrorKind::AlreadyExists, "R24 evidence root must be fresh"));
        }
        let (tx, rx) = mpsc::sync_channel(cfg.queue_bound);
        let stats = Arc::new(Stats::default());
        let worker_stats = stats.clone();
        let join = thread::Builder::new()
            .name("r24-evidence-writer".into())
            .spawn(move || writer_worker(canon, rx, worker_stats, cfg))?;
        Ok(Self { tx: Some(tx), stats, join: Some(join) })
    }

    pub fn try_record(&self, frame: Arc<RawFrame>) -> EvidenceSubmit {
        let offer_seq = self.stats.offered.fetch_add(1, Ordering::Relaxed) + 1;
        let Some(tx) = self.tx.as_ref() else {
            self.stats.dropped_closed.fetch_add(1, Ordering::Relaxed);
            return EvidenceSubmit::DroppedClosed;
        };
        let depth = self.stats.outstanding.fetch_add(1, Ordering::AcqRel) + 1;
        update_peak(&self.stats.queue_peak, depth);
        match tx.try_send(Job { offer_seq, frame }) {
            Ok(()) => {
                self.stats.queued.fetch_add(1, Ordering::Relaxed);
                EvidenceSubmit::Queued
            }
            Err(TrySendError::Full(_)) => {
                self.stats.outstanding.fetch_sub(1, Ordering::AcqRel);
                self.stats.dropped_full.fetch_add(1, Ordering::Relaxed);
                EvidenceSubmit::DroppedFull
            }
            Err(TrySendError::Disconnected(_)) => {
                self.stats.outstanding.fetch_sub(1, Ordering::AcqRel);
                self.stats.dropped_closed.fetch_add(1, Ordering::Relaxed);
                EvidenceSubmit::DroppedClosed
            }
        }
    }

    pub fn snapshot(&self) -> CompressedEvidenceStats {
        self.stats.snapshot()
    }

    pub fn close(mut self) -> io::Result<CompressedEvidenceStats> {
        self.tx.take();
        if let Some(join) = self.join.take() {
            match join.join() {
                Ok(v) => v?,
                Err(_) => return Err(io::Error::other("R24 writer panicked")),
            }
        }
        Ok(self.stats.snapshot())
    }
}

fn validate_config(cfg: &CompressedEvidenceConfig) -> io::Result<()> {
    if cfg.queue_bound == 0
        || cfg.compression_queue_bound == 0
        || cfg.segment_max_bytes < 1024
        || cfg.sync_interval.is_zero()
        || !(1..=19).contains(&cfg.zstd_level)
        || cfg.max_retained_bytes == 0
    {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid R24 evidence config"));
    }
    Ok(())
}

fn update_peak(peak: &AtomicUsize, candidate: usize) {
    let mut current = peak.load(Ordering::Relaxed);
    while candidate > current {
        match peak.compare_exchange_weak(current, candidate, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(actual) => current = actual,
        }
    }
}

fn raw_path(root: &Path, segment: u64) -> PathBuf {
    root.join(format!("market_evidence_{segment:06}.jsonl"))
}
fn zst_path(root: &Path, segment: u64) -> PathBuf {
    root.join(format!("market_evidence_{segment:06}.jsonl.zst"))
}
fn meta_path(root: &Path, segment: u64) -> PathBuf {
    root.join(format!("market_evidence_{segment:06}.meta.json"))
}

fn open_raw(root: &Path, segment: u64) -> io::Result<(File, BufWriter<File>)> {
    let file = OpenOptions::new().create_new(true).write(true).open(raw_path(root, segment))?;
    let sync_file = file.try_clone()?;
    Ok((sync_file, BufWriter::with_capacity(1024 * 1024, file)))
}

fn sync_raw(sync_file: &File, writer: &mut BufWriter<File>, stats: &Stats, last_seq: u64) -> io::Result<()> {
    writer.flush()?;
    sync_file.sync_data()?;
    stats.sync_calls.fetch_add(1, Ordering::Relaxed);
    stats.last_synced_offer_seq.store(last_seq, Ordering::Release);
    Ok(())
}

fn writer_worker(
    root: PathBuf,
    rx: Receiver<Job>,
    stats: Arc<Stats>,
    cfg: CompressedEvidenceConfig,
) -> io::Result<()> {
    let (ctx, crx) = mpsc::sync_channel::<CompressionJob>(cfg.compression_queue_bound);
    let comp_stats = stats.clone();
    let comp_root = root.clone();
    let comp_cfg = cfg.clone();
    let comp_join = thread::Builder::new()
        .name("r24-zstd-retention".into())
        .spawn(move || compressor_worker(comp_root, crx, comp_stats, comp_cfg))?;

    let mut deferred = VecDeque::new();
    let mut segment = 1u64;
    let (mut sync_file, mut writer) = open_raw(&root, segment)?;
    let mut segment_bytes = 0u64;
    let mut segment_records = 0u64;
    let mut segment_first_seq = 0u64;
    let mut segment_last_seq = 0u64;
    let mut last_sync = Instant::now();

    for job in rx {
        let raw_sha256 = hex(&sha256(job.frame.raw.as_bytes()));
        let line = EvidenceLine {
            offer_seq: job.offer_seq,
            venue: job.frame.venue,
            epoch: job.frame.epoch,
            received_ns: job.frame.received_ns,
            received_wall_ms: job.frame.received_wall_ms,
            raw_sha256,
            raw: job.frame.raw.clone(),
        };
        let mut bytes = serde_json::to_vec(&line)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        bytes.push(b'\n');

        if segment_records > 0 && segment_bytes + bytes.len() as u64 > cfg.segment_max_bytes {
            sync_raw(&sync_file, &mut writer, &stats, segment_last_seq)?;
            drop(writer);
            drop(sync_file);
            stats.raw_segments_closed.fetch_add(1, Ordering::Relaxed);
            let cjob = CompressionJob {
                segment,
                raw_path: raw_path(&root, segment),
                first_seq: segment_first_seq,
                last_seq: segment_last_seq,
                records: segment_records,
                raw_bytes: segment_bytes,
            };
            enqueue_compression(&ctx, cjob, &stats, &mut deferred);

            segment += 1;
            (sync_file, writer) = open_raw(&root, segment)?;
            segment_bytes = 0;
            segment_records = 0;
            segment_first_seq = 0;
            last_sync = Instant::now();
        }

        writer.write_all(&bytes).map_err(|e| {
            stats.write_errors.fetch_add(1, Ordering::Relaxed);
            e
        })?;
        if segment_records == 0 {
            segment_first_seq = job.offer_seq;
        }
        segment_last_seq = job.offer_seq;
        segment_records += 1;
        segment_bytes += bytes.len() as u64;
        stats.raw_bytes_written.fetch_add(bytes.len() as u64, Ordering::Relaxed);
        stats.written.fetch_add(1, Ordering::Relaxed);
        stats.last_written_offer_seq.store(job.offer_seq, Ordering::Release);
        stats.outstanding.fetch_sub(1, Ordering::AcqRel);

        if last_sync.elapsed() >= cfg.sync_interval {
            sync_raw(&sync_file, &mut writer, &stats, segment_last_seq).map_err(|e| {
                stats.write_errors.fetch_add(1, Ordering::Relaxed);
                e
            })?;
            last_sync = Instant::now();
        }
    }

    if segment_records > 0 {
        writer.flush()?;
        sync_file.sync_all()?;
        stats.sync_calls.fetch_add(1, Ordering::Relaxed);
        stats.last_synced_offer_seq.store(segment_last_seq, Ordering::Release);
        drop(writer);
        drop(sync_file);
        stats.raw_segments_closed.fetch_add(1, Ordering::Relaxed);
        let cjob = CompressionJob {
            segment,
            raw_path: raw_path(&root, segment),
            first_seq: segment_first_seq,
            last_seq: segment_last_seq,
            records: segment_records,
            raw_bytes: segment_bytes,
        };
        enqueue_compression(&ctx, cjob, &stats, &mut deferred);
    } else {
        drop(writer);
        drop(sync_file);
        let _ = fs::remove_file(raw_path(&root, segment));
    }

    while let Some(job) = deferred.pop_front() {
        let depth = stats.compression_outstanding.fetch_add(1, Ordering::AcqRel) + 1;
        update_peak(&stats.compression_queue_peak, depth);
        if ctx.send(job).is_err() {
            stats.compression_outstanding.fetch_sub(1, Ordering::AcqRel);
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "compressor closed"));
        }
        stats.compression_enqueued.fetch_add(1, Ordering::Relaxed);
    }
    drop(ctx);
    match comp_join.join() {
        Ok(v) => v?,
        Err(_) => return Err(io::Error::other("R24 compressor panicked")),
    }
    Ok(())
}

fn enqueue_compression(
    tx: &SyncSender<CompressionJob>,
    job: CompressionJob,
    stats: &Stats,
    deferred: &mut VecDeque<CompressionJob>,
) {
    flush_deferred_nonblocking(tx, stats, deferred);
    if !deferred.is_empty() {
        stats.compression_deferred.fetch_add(1, Ordering::Relaxed);
        deferred.push_back(job);
        return;
    }
    let depth = stats.compression_outstanding.fetch_add(1, Ordering::AcqRel) + 1;
    update_peak(&stats.compression_queue_peak, depth);
    match tx.try_send(job) {
        Ok(()) => {
            stats.compression_enqueued.fetch_add(1, Ordering::Relaxed);
        }
        Err(TrySendError::Full(job)) | Err(TrySendError::Disconnected(job)) => {
            stats.compression_outstanding.fetch_sub(1, Ordering::AcqRel);
            stats.compression_deferred.fetch_add(1, Ordering::Relaxed);
            deferred.push_back(job);
        }
    }
}

fn flush_deferred_nonblocking(
    tx: &SyncSender<CompressionJob>,
    stats: &Stats,
    deferred: &mut VecDeque<CompressionJob>,
) {
    loop {
        let Some(job) = deferred.pop_front() else { break; };
        let depth = stats.compression_outstanding.fetch_add(1, Ordering::AcqRel) + 1;
        update_peak(&stats.compression_queue_peak, depth);
        match tx.try_send(job) {
            Ok(()) => {
                stats.compression_enqueued.fetch_add(1, Ordering::Relaxed);
            }
            Err(TrySendError::Full(job)) | Err(TrySendError::Disconnected(job)) => {
                stats.compression_outstanding.fetch_sub(1, Ordering::AcqRel);
                deferred.push_front(job);
                break;
            }
        }
    }
}

fn compressor_worker(
    root: PathBuf,
    rx: Receiver<CompressionJob>,
    stats: Arc<Stats>,
    cfg: CompressedEvidenceConfig,
) -> io::Result<()> {
    let mut retained: VecDeque<Retained> = VecDeque::new();
    for job in rx {
        let result = compress_one(&root, &job, cfg.zstd_level);
        stats.compression_outstanding.fetch_sub(1, Ordering::AcqRel);
        let retained_segment = match result {
            Ok(v) => v,
            Err(e) => {
                stats.compression_errors.fetch_add(1, Ordering::Relaxed);
                return Err(e);
            }
        };
        stats.compressed_segments.fetch_add(1, Ordering::Relaxed);
        stats.compressed_bytes_created.fetch_add(retained_segment.compressed_bytes, Ordering::Relaxed);
        stats.raw_bytes_deleted_after_compress.fetch_add(job.raw_bytes, Ordering::Relaxed);
        retained.push_back(retained_segment);
        refresh_retained_stats(&retained, &stats);
        if let Err(e) = apply_retention(&root, &mut retained, &stats, &cfg) {
            stats.retention_errors.fetch_add(1, Ordering::Relaxed);
            return Err(e);
        }
    }
    refresh_retained_stats(&retained, &stats);
    Ok(())
}

fn compress_one(root: &Path, job: &CompressionJob, level: i32) -> io::Result<Retained> {
    let out = zst_path(root, job.segment);
    let status = Command::new(ZSTD_BIN)
        .arg("-q")
        .arg("-T1")
        .arg(format!("-{level}"))
        .arg("-f")
        .arg(&job.raw_path)
        .arg("-o")
        .arg(&out)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()?;
    if !status.success() {
        return Err(io::Error::other(format!("zstd exit status {status}")));
    }
    let compressed_bytes = fs::metadata(&out)?.len();
    File::open(&out)?.sync_all()?;
    let meta = SegmentMeta {
        segment: job.segment,
        first_seq: job.first_seq,
        last_seq: job.last_seq,
        records: job.records,
        raw_bytes: job.raw_bytes,
        compressed_bytes,
    };
    durable_write_json(&meta_path(root, job.segment), &meta)?;
    fs::remove_file(&job.raw_path)?;
    sync_dir(root)?;
    Ok(Retained {
        segment: job.segment,
        zst_path: out,
        meta_path: meta_path(root, job.segment),
        first_seq: job.first_seq,
        last_seq: job.last_seq,
        records: job.records,
        compressed_bytes,
    })
}

fn durable_write_json(path: &Path, value: &impl Serialize) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    let bytes = serde_json::to_vec(value).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    {
        let mut f = OpenOptions::new().create_new(true).write(true).open(&tmp)?;
        f.write_all(&bytes)?;
        f.write_all(b"\n")?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

fn apply_retention(
    root: &Path,
    retained: &mut VecDeque<Retained>,
    stats: &Stats,
    cfg: &CompressedEvidenceConfig,
) -> io::Result<()> {
    stats.retention_runs.fetch_add(1, Ordering::Relaxed);
    loop {
        let total: u64 = retained.iter().map(|x| x.compressed_bytes).sum();
        let free = free_bytes(root)?;
        let Some(reason) = retention_reason(total, free, cfg) else { break; };
        if retained.len() <= 1 {
            stats.retention_pressure_unresolved.fetch_add(1, Ordering::Relaxed);
            break;
        }
        let oldest = retained.pop_front().expect("len checked");
        let tombstone = RetentionEntry {
            segment: oldest.segment,
            first_seq: oldest.first_seq,
            last_seq: oldest.last_seq,
            records: oldest.records,
            compressed_bytes: oldest.compressed_bytes,
            deleted_wall_ms: wall_ms(),
            reason: reason.into(),
        };
        append_retention_log(root, &tombstone)?;
        fs::remove_file(&oldest.zst_path)?;
        fs::remove_file(&oldest.meta_path)?;
        sync_dir(root)?;
        stats.retention_deleted_segments.fetch_add(1, Ordering::Relaxed);
        stats.retention_deleted_records.fetch_add(oldest.records, Ordering::Relaxed);
        stats.retention_deleted_bytes.fetch_add(oldest.compressed_bytes, Ordering::Relaxed);
        stats.retention_deleted_through_seq.store(oldest.last_seq, Ordering::Release);
        refresh_retained_stats(retained, stats);
    }
    refresh_retained_stats(retained, stats);
    Ok(())
}

fn retention_reason(total_bytes:u64, free_bytes_now:u64, cfg:&CompressedEvidenceConfig)->Option<&'static str>{
    match (total_bytes>cfg.max_retained_bytes, free_bytes_now<cfg.min_free_bytes) {
        (true,true)=>Some("max_bytes_and_free_watermark"),
        (true,false)=>Some("max_bytes"),
        (false,true)=>Some("free_watermark"),
        (false,false)=>None,
    }
}

fn refresh_retained_stats(retained: &VecDeque<Retained>, stats: &Stats) {
    stats.retained_segments.store(retained.len() as u64, Ordering::Relaxed);
    stats.retained_bytes.store(retained.iter().map(|x| x.compressed_bytes).sum(), Ordering::Relaxed);
}

fn append_retention_log(root: &Path, row: &RetentionEntry) -> io::Result<()> {
    let path = root.join("retention.jsonl");
    let mut f = OpenOptions::new().create(true).append(true).open(&path)?;
    serde_json::to_writer(&mut f, row).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    f.write_all(b"\n")?;
    f.sync_data()?;
    sync_dir(root)?;
    Ok(())
}

fn free_bytes(path: &Path) -> io::Result<u64> {
    let c = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "nul in path"))?;
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statvfs(c.as_ptr(), &mut stat) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok((stat.f_bavail as u64).saturating_mul(stat.f_frsize as u64))
}

fn sync_dir(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

fn wall_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompressedEvidenceVerify {
    pub retained_segments: u64,
    pub retained_records: u64,
    pub compressed_bytes: u64,
    pub bad_hash: u64,
    pub nonmonotonic: u64,
    pub internal_offer_seq_gaps: u64,
    pub first_retained_offer_seq: u64,
    pub last_retained_offer_seq: u64,
    pub retention_deleted_segments: u64,
    pub retention_deleted_records: u64,
    pub retention_deleted_through_seq: u64,
    pub retention_accounting_ok: bool,
}

pub fn verify_compressed_evidence_dir(root: impl AsRef<Path>) -> io::Result<CompressedEvidenceVerify> {
    let root = root.as_ref();
    let mut paths = fs::read_dir(root)?
        .filter_map(Result::ok)
        .map(|x| x.path())
        .filter(|p| p.file_name().and_then(|x| x.to_str()).is_some_and(|x| x.starts_with("market_evidence_") && x.ends_with(".jsonl.zst")))
        .collect::<Vec<_>>();
    paths.sort();

    let mut retained_records = 0u64;
    let mut compressed_bytes = 0u64;
    let mut bad_hash = 0u64;
    let mut nonmonotonic = 0u64;
    let mut gaps = 0u64;
    let mut first = 0u64;
    let mut previous = 0u64;

    for path in &paths {
        compressed_bytes += fs::metadata(path)?.len();
        let mut child = Command::new(ZSTD_BIN)
            .arg("-q").arg("-dc").arg(path)
            .stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::null()).spawn()?;
        let stdout = child.stdout.take().ok_or_else(|| io::Error::other("zstd stdout"))?;
        for line in BufReader::new(stdout).lines() {
            let line = line?;
            let row: EvidenceLine = serde_json::from_str(&line)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            retained_records += 1;
            if first == 0 { first = row.offer_seq; }
            if hex(&sha256(row.raw.as_bytes())) != row.raw_sha256 { bad_hash += 1; }
            if previous > 0 {
                if row.offer_seq <= previous { nonmonotonic += 1; }
                else if row.offer_seq > previous + 1 { gaps += row.offer_seq - previous - 1; }
            }
            previous = row.offer_seq;
        }
        let status = child.wait()?;
        if !status.success() {
            return Err(io::Error::other(format!("zstd verify exit {status}")));
        }
    }

    let (deleted_segments, deleted_records, deleted_through) = read_retention_prefix(root)?;
    let accounting_ok = if first == 0 {
        deleted_records == 0
    } else {
        deleted_through.saturating_add(1) == first || (deleted_through == 0 && first == 1)
    };

    Ok(CompressedEvidenceVerify {
        retained_segments: paths.len() as u64,
        retained_records,
        compressed_bytes,
        bad_hash,
        nonmonotonic,
        internal_offer_seq_gaps: gaps,
        first_retained_offer_seq: first,
        last_retained_offer_seq: previous,
        retention_deleted_segments: deleted_segments,
        retention_deleted_records: deleted_records,
        retention_deleted_through_seq: deleted_through,
        retention_accounting_ok: accounting_ok,
    })
}

fn read_retention_prefix(root: &Path) -> io::Result<(u64,u64,u64)> {
    let path = root.join("retention.jsonl");
    if !path.exists() { return Ok((0,0,0)); }
    let mut segments=0u64; let mut records=0u64; let mut through=0u64;
    for line in BufReader::new(File::open(path)?).lines() {
        let row:RetentionEntry=serde_json::from_str(&line?)
            .map_err(|e|io::Error::new(io::ErrorKind::InvalidData,e))?;
        segments+=1;
        records=records.saturating_add(row.records);
        if row.first_seq != through.saturating_add(1) {
            return Err(io::Error::new(io::ErrorKind::InvalidData,"retention prefix not contiguous"));
        }
        through=row.last_seq;
    }
    Ok((segments,records,through))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Venue;
    use std::time::SystemTime;

    fn frame(i:u64)->Arc<RawFrame>{
        Arc::new(RawFrame{source:None,venue:Venue::OkxSpot,epoch:1,received_ns:i,received_wall_ms:i as i64,
            raw:format!("{{\"n\":{i},\"padding\":\"{}\"}}","x".repeat(700))})
    }
    fn root(tag:&str)->PathBuf{
        PathBuf::from(format!("/mnt/spotarb_db_test/r24_{}_{}_{}",tag,std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()))
    }

    #[test]
    fn retention_policy_covers_byte_cap_and_free_space_watermark() {
        let mut cfg=CompressedEvidenceConfig::default();
        cfg.max_retained_bytes=100;cfg.min_free_bytes=50;
        assert_eq!(retention_reason(99,50,&cfg),None);
        assert_eq!(retention_reason(101,50,&cfg),Some("max_bytes"));
        assert_eq!(retention_reason(99,49,&cfg),Some("free_watermark"));
        assert_eq!(retention_reason(101,49,&cfg),Some("max_bytes_and_free_watermark"));
    }

    #[test]
    fn outside_r24_test_disk_is_denied() {
        let p=PathBuf::from(format!("/tmp/r24-deny-{}",std::process::id()));
        let err=AsyncCompressedEvidenceWriter::start(&p,CompressedEvidenceConfig::default()).err().unwrap();
        assert_eq!(err.kind(),io::ErrorKind::PermissionDenied);
        let _=fs::remove_dir_all(p);
    }

    #[test]
    fn compression_roundtrip_preserves_hashes_and_sequences() {
        let p=root("roundtrip");
        let mut cfg=CompressedEvidenceConfig::default();
        cfg.segment_max_bytes=8*1024;
        cfg.max_retained_bytes=1024*1024*1024;
        cfg.min_free_bytes=0;
        let w=AsyncCompressedEvidenceWriter::start(&p,cfg).unwrap();
        for i in 1..=120 { assert_eq!(w.try_record(frame(i)),EvidenceSubmit::Queued); }
        let s=w.close().unwrap();
        assert_eq!(s.dropped_full,0);assert_eq!(s.written,120);assert!(s.compressed_segments>1);
        assert_eq!(s.compression_errors,0);assert_eq!(s.retention_deleted_segments,0);
        assert!(!fs::read_dir(&p).unwrap().any(|e|e.unwrap().path().extension().and_then(|x|x.to_str())==Some("jsonl")));
        let v=verify_compressed_evidence_dir(&p).unwrap();
        assert_eq!(v.retained_records,120);assert_eq!(v.bad_hash,0);assert_eq!(v.internal_offer_seq_gaps,0);
        assert!(v.retention_accounting_ok);
        let _=fs::remove_dir_all(p);
    }

    #[test]
    fn retention_deletes_only_oldest_prefix_and_verifier_accounts_for_it() {
        let p=root("retention");
        let mut cfg=CompressedEvidenceConfig::default();
        cfg.segment_max_bytes=8*1024;
        cfg.max_retained_bytes=4*1024;
        cfg.min_free_bytes=0;
        let w=AsyncCompressedEvidenceWriter::start(&p,cfg).unwrap();
        for i in 1..=300 { let _=w.try_record(frame(i)); }
        let s=w.close().unwrap();
        assert_eq!(s.dropped_full,0);assert!(s.retention_deleted_segments>0);
        assert!(s.retained_segments>=1);assert_eq!(s.retention_errors,0);
        let v=verify_compressed_evidence_dir(&p).unwrap();
        assert_eq!(v.bad_hash,0);assert_eq!(v.internal_offer_seq_gaps,0);
        assert!(v.retention_deleted_records>0);assert!(v.retention_accounting_ok);
        assert_eq!(v.first_retained_offer_seq,v.retention_deleted_through_seq+1);
        let _=fs::remove_dir_all(p);
    }

    #[test]
    fn impossible_free_space_watermark_is_explicit_and_keeps_newest_segment() {
        let p=root("watermark");fs::create_dir_all(&p).unwrap();
        let mut retained=VecDeque::new();
        for segment in 1..=3u64 {
            let z=zst_path(&p,segment);let m=meta_path(&p,segment);
            fs::write(&z,b"dummy").unwrap();fs::write(&m,b"{}
").unwrap();
            retained.push_back(Retained{segment,zst_path:z,meta_path:m,first_seq:(segment-1)*10+1,last_seq:segment*10,records:10,compressed_bytes:5});
        }
        let stats=Stats::default();let mut cfg=CompressedEvidenceConfig::default();
        cfg.max_retained_bytes=u64::MAX;cfg.min_free_bytes=u64::MAX;
        apply_retention(&p,&mut retained,&stats,&cfg).unwrap();
        assert_eq!(retained.len(),1);assert_eq!(retained.front().unwrap().segment,3);
        let snap=stats.snapshot();assert_eq!(snap.retention_deleted_segments,2);assert_eq!(snap.retention_deleted_through_seq,20);
        assert!(snap.retention_pressure_unresolved>=1);
        let (segments,records,through)=read_retention_prefix(&p).unwrap();
        assert_eq!((segments,records,through),(2,20,20));
        let _=fs::remove_dir_all(p);
    }

    #[test]
    fn hot_path_try_send_stays_nonblocking_under_tiny_queue() {
        let p=root("nonblock");
        let mut cfg=CompressedEvidenceConfig::default();
        cfg.queue_bound=1;cfg.segment_max_bytes=1024*1024;cfg.min_free_bytes=0;
        let w=AsyncCompressedEvidenceWriter::start(&p,cfg).unwrap();
        let started=Instant::now();
        let mut drops=0u64;
        for i in 1..=20_000 {
            if w.try_record(frame(i))==EvidenceSubmit::DroppedFull {drops+=1;}
        }
        assert!(started.elapsed()<Duration::from_secs(2));
        assert!(drops>0);
        let s=w.close().unwrap();
        assert_eq!(s.offered,20_000);assert_eq!(s.dropped_full,drops);
        let _=fs::remove_dir_all(p);
    }
}
