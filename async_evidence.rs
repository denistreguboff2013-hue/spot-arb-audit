//! R23 non-blocking public market evidence persistence.
//!
//! The market hot path can only try_send an Arc<RawFrame>. It never waits for disk.
//! Queue-full is an explicit evidence loss metric, not backpressure into DecisionCore.

use crate::{RawFrame, Venue};
use openssl::sha::sha256;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        mpsc::{self, Receiver, SyncSender, TrySendError},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

pub const DEFAULT_QUEUE_BOUND: usize = 8192;
pub const DEFAULT_SEGMENT_MAX_BYTES: u64 = 64 * 1024 * 1024;
pub const DEFAULT_SYNC_INTERVAL_MS: u64 = 1_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EvidenceSubmit {
    Queued,
    DroppedFull,
    DroppedClosed,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EvidenceLine {
    pub offer_seq: u64,
    pub venue: Venue,
    pub epoch: u64,
    pub received_ns: u64,
    pub received_wall_ms: i64,
    pub raw_sha256: String,
    pub raw: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct EvidenceStats {
    pub offered: u64,
    pub queued: u64,
    pub written: u64,
    pub dropped_full: u64,
    pub dropped_closed: u64,
    pub write_errors: u64,
    pub queue_peak: usize,
    pub outstanding: usize,
    pub bytes_written: u64,
    pub segments: u64,
    pub sync_calls: u64,
    pub last_written_offer_seq: u64,
    pub last_synced_offer_seq: u64,
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
    bytes_written: AtomicU64,
    segments: AtomicU64,
    sync_calls: AtomicU64,
    last_written_offer_seq: AtomicU64,
    last_synced_offer_seq: AtomicU64,
}

impl Stats {
    fn snapshot(&self) -> EvidenceStats {
        EvidenceStats {
            offered: self.offered.load(Ordering::Relaxed),
            queued: self.queued.load(Ordering::Relaxed),
            written: self.written.load(Ordering::Relaxed),
            dropped_full: self.dropped_full.load(Ordering::Relaxed),
            dropped_closed: self.dropped_closed.load(Ordering::Relaxed),
            write_errors: self.write_errors.load(Ordering::Relaxed),
            queue_peak: self.queue_peak.load(Ordering::Relaxed),
            outstanding: self.outstanding.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            segments: self.segments.load(Ordering::Relaxed),
            sync_calls: self.sync_calls.load(Ordering::Relaxed),
            last_written_offer_seq: self.last_written_offer_seq.load(Ordering::Relaxed),
            last_synced_offer_seq: self.last_synced_offer_seq.load(Ordering::Relaxed),
        }
    }
}

struct Job {
    offer_seq: u64,
    frame: Arc<RawFrame>,
}

pub struct AsyncEvidenceWriter {
    tx: Option<SyncSender<Job>>,
    stats: Arc<Stats>,
    join: Option<thread::JoinHandle<io::Result<()>>>,
}

impl AsyncEvidenceWriter {
    pub fn start(
        root: impl AsRef<Path>,
        queue_bound: usize,
        segment_max_bytes: u64,
        sync_interval: Duration,
    ) -> io::Result<Self> {
        if queue_bound == 0 || segment_max_bytes < 1024 || sync_interval.is_zero() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid evidence bounds"));
        }
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        let canon = root.canonicalize()?;
        let text = canon.to_string_lossy();
        if !text.starts_with("/mnt/spotarb_db_test/r23_") {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "R23 evidence root must stay under /mnt/spotarb_db_test/r23_*",
            ));
        }
        if fs::read_dir(&canon)?.next().is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "evidence root must be fresh",
            ));
        }

        let (tx, rx) = mpsc::sync_channel(queue_bound);
        let stats = Arc::new(Stats::default());
        let worker_stats = stats.clone();
        let join = thread::Builder::new()
            .name("r23-market-evidence".into())
            .spawn(move || worker(canon, rx, worker_stats, segment_max_bytes, sync_interval))?;
        Ok(Self {
            tx: Some(tx),
            stats,
            join: Some(join),
        })
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

    pub fn snapshot(&self) -> EvidenceStats {
        self.stats.snapshot()
    }

    pub fn close(mut self) -> io::Result<EvidenceStats> {
        self.tx.take();
        if let Some(join) = self.join.take() {
            match join.join() {
                Ok(result) => result?,
                Err(_) => return Err(io::Error::other("evidence worker panicked")),
            }
        }
        Ok(self.stats.snapshot())
    }
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

fn segment_path(root: &Path, index: u64) -> PathBuf {
    root.join(format!("market_evidence_{index:06}.jsonl"))
}

fn open_segment(root: &Path, index: u64) -> io::Result<(File, BufWriter<File>)> {
    let path = segment_path(root, index);
    let file = OpenOptions::new().create_new(true).write(true).open(path)?;
    let sync_file = file.try_clone()?;
    Ok((sync_file, BufWriter::with_capacity(1024 * 1024, file)))
}

fn sync_writer(
    sync_file: &File,
    writer: &mut BufWriter<File>,
    stats: &Stats,
    last_written: u64,
) -> io::Result<()> {
    writer.flush()?;
    sync_file.sync_data()?;
    stats.sync_calls.fetch_add(1, Ordering::Relaxed);
    stats.last_synced_offer_seq.store(last_written, Ordering::Release);
    Ok(())
}

fn worker(
    root: PathBuf,
    rx: Receiver<Job>,
    stats: Arc<Stats>,
    segment_max_bytes: u64,
    sync_interval: Duration,
) -> io::Result<()> {
    let mut segment = 1u64;
    let (mut sync_file, mut writer) = open_segment(&root, segment)?;
    stats.segments.store(1, Ordering::Relaxed);
    let mut segment_bytes = 0u64;
    let mut last_sync = Instant::now();
    let mut last_written = 0u64;

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
        let mut bytes = match serde_json::to_vec(&line) {
            Ok(v) => v,
            Err(e) => {
                stats.write_errors.fetch_add(1, Ordering::Relaxed);
                return Err(io::Error::new(io::ErrorKind::InvalidData, e));
            }
        };
        bytes.push(b'\n');

        if segment_bytes > 0 && segment_bytes + bytes.len() as u64 > segment_max_bytes {
            sync_writer(&sync_file, &mut writer, &stats, last_written)?;
            drop(writer);
            drop(sync_file);
            segment += 1;
            (sync_file, writer) = open_segment(&root, segment)?;
            stats.segments.store(segment, Ordering::Relaxed);
            segment_bytes = 0;
            last_sync = Instant::now();
        }

        if let Err(e) = writer.write_all(&bytes) {
            stats.write_errors.fetch_add(1, Ordering::Relaxed);
            return Err(e);
        }
        segment_bytes += bytes.len() as u64;
        stats.bytes_written.fetch_add(bytes.len() as u64, Ordering::Relaxed);
        stats.written.fetch_add(1, Ordering::Relaxed);
        last_written = job.offer_seq;
        stats.last_written_offer_seq.store(last_written, Ordering::Release);
        stats.outstanding.fetch_sub(1, Ordering::AcqRel);

        if last_sync.elapsed() >= sync_interval {
            if let Err(e) = sync_writer(&sync_file, &mut writer, &stats, last_written) {
                stats.write_errors.fetch_add(1, Ordering::Relaxed);
                return Err(e);
            }
            last_sync = Instant::now();
        }
    }

    if last_written > 0 {
        if let Err(e) = writer.flush().and_then(|_| sync_file.sync_all()) {
            stats.write_errors.fetch_add(1, Ordering::Relaxed);
            return Err(e);
        }
        stats.sync_calls.fetch_add(1, Ordering::Relaxed);
        stats.last_synced_offer_seq.store(last_written, Ordering::Release);
    }
    Ok(())
}

pub fn verify_evidence_dir(root: impl AsRef<Path>) -> io::Result<EvidenceVerify> {
    let mut paths = fs::read_dir(root)?
        .filter_map(Result::ok)
        .map(|x| x.path())
        .filter(|p| p.file_name().and_then(|x| x.to_str()).is_some_and(|x| x.starts_with("market_evidence_") && x.ends_with(".jsonl")))
        .collect::<Vec<_>>();
    paths.sort();

    let mut records = 0u64;
    let mut bad_hash = 0u64;
    let mut nonmonotonic = 0u64;
    let mut gaps = 0u64;
    let mut previous = 0u64;
    let mut bytes = 0u64;
    for path in &paths {
        bytes += fs::metadata(path)?.len();
        for line in BufReader::new(File::open(path)?).lines() {
            let line = line?;
            let row: EvidenceLine = serde_json::from_str(&line)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            records += 1;
            if hex(&sha256(row.raw.as_bytes())) != row.raw_sha256 {
                bad_hash += 1;
            }
            if previous > 0 {
                if row.offer_seq <= previous {
                    nonmonotonic += 1;
                } else if row.offer_seq > previous + 1 {
                    gaps += row.offer_seq - previous - 1;
                }
            } else if row.offer_seq > 1 {
                gaps += row.offer_seq - 1;
            }
            previous = row.offer_seq;
        }
    }
    Ok(EvidenceVerify {
        segments: paths.len() as u64,
        records,
        bytes,
        bad_hash,
        nonmonotonic,
        offer_seq_gaps: gaps,
        last_offer_seq: previous,
    })
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct EvidenceVerify {
    pub segments: u64,
    pub records: u64,
    pub bytes: u64,
    pub bad_hash: u64,
    pub nonmonotonic: u64,
    pub offer_seq_gaps: u64,
    pub last_offer_seq: u64,
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{sync::mpsc::sync_channel, time::{SystemTime, UNIX_EPOCH}};

    fn frame(raw: &str) -> Arc<RawFrame> {
        Arc::new(RawFrame{source:None,
            venue: Venue::OkxSpot,
            epoch: 1,
            received_ns: 1,
            received_wall_ms: 2,
            raw: raw.into(),
        })
    }

    #[test]
    fn producer_queue_full_is_drop_not_block() {
        let (tx, _rx) = sync_channel::<Job>(1);
        let stats = Arc::new(Stats::default());
        let writer = AsyncEvidenceWriter { tx: Some(tx), stats, join: None };
        assert_eq!(writer.try_record(frame("a")), EvidenceSubmit::Queued);
        let started = Instant::now();
        assert_eq!(writer.try_record(frame("b")), EvidenceSubmit::DroppedFull);
        assert!(started.elapsed() < Duration::from_millis(50));
        let s = writer.snapshot();
        assert_eq!(s.offered, 2);
        assert_eq!(s.queued, 1);
        assert_eq!(s.dropped_full, 1);
    }

    #[test]
    fn evidence_root_outside_r23_test_disk_is_rejected() {
        let root=PathBuf::from(format!("/tmp/r23-evidence-deny-{}",std::process::id()));
        let err=AsyncEvidenceWriter::start(&root,8,1024,Duration::from_millis(10)).err().unwrap();
        assert_eq!(err.kind(),io::ErrorKind::PermissionDenied);
        let _=fs::remove_dir_all(root);
    }

    #[test]
    fn verifier_detects_hash_corruption_and_sequence_gap() {
        let root=PathBuf::from(format!(
            "/mnt/spotarb_db_test/r23_evidence_corrupt_{}_{}",
            std::process::id(),SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()));
        fs::create_dir_all(&root).unwrap();
        let rows=[EvidenceLine{offer_seq:1,venue:Venue::OkxSpot,epoch:1,received_ns:1,received_wall_ms:1,raw_sha256:hex(&sha256(b"a")),raw:"a".into()},
            EvidenceLine{offer_seq:3,venue:Venue::OkxSpot,epoch:1,received_ns:2,received_wall_ms:2,raw_sha256:"00".repeat(32),raw:"b".into()}];
        let mut f=BufWriter::new(File::create(root.join("market_evidence_000001.jsonl")).unwrap());
        for row in rows{serde_json::to_writer(&mut f,&row).unwrap();f.write_all(b"\n").unwrap();}f.flush().unwrap();
        let v=verify_evidence_dir(&root).unwrap();
        assert_eq!(v.records,2);assert_eq!(v.bad_hash,1);assert_eq!(v.offer_seq_gaps,1);
        let _=fs::remove_dir_all(root);
    }

    #[test]
    fn clean_close_drains_and_verifies_all_records() {
        let root = PathBuf::from(format!(
            "/mnt/spotarb_db_test/r23_evidence_unit_{}_{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
        ));
        fs::create_dir_all(&root).unwrap();
        let writer = AsyncEvidenceWriter::start(&root, 8, 1024, Duration::from_millis(10)).unwrap();
        for i in 0..20 {
            while writer.snapshot().outstanding >= 7 {
                thread::sleep(Duration::from_millis(1));
            }
            assert_eq!(writer.try_record(frame(&format!(r#"{{"n":{i}}}"#))), EvidenceSubmit::Queued);
        }
        let s = writer.close().unwrap();
        assert_eq!(s.dropped_full, 0);
        assert_eq!(s.written, 20);
        assert_eq!(s.last_synced_offer_seq, 20);
        let v = verify_evidence_dir(&root).unwrap();
        assert_eq!(v.records, 20);
        assert_eq!(v.bad_hash, 0);
        assert_eq!(v.nonmonotonic, 0);
        assert_eq!(v.offer_seq_gaps, 0);
        let _ = fs::remove_dir_all(root);
    }
}
