#![recursion_limit = "256"]

//! C27 review-only native durable hot path.
//!
//! Rust owns ingress -> admission -> staging -> TWO-SYNC -> ordered durable ACK.
//! Python may drive this binary as a harness, but is not on the per-event durable path.
use openssl::sha::sha256;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    ffi::CString,
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    os::unix::{fs::MetadataExt, fs::OpenOptionsExt, io::AsRawFd},
    path::{Path, PathBuf},
    sync::{
        mpsc::{sync_channel, Receiver, SyncSender, TryRecvError, TrySendError},
        Arc, Mutex,
    },
    thread,
    time::{Duration, Instant},
};

const MAGIC: &[u8; 8] = b"SPREG001";
const END: &[u8; 8] = b"ENDREG01";
const FLOOR: &[u8; 8] = b"FLOOR001";
const HEADER: usize = 56;
const TRAILER: usize = 40;
const MAX_PAYLOAD: usize = 2_097_152;
const MAX_EVENTS: usize = 65_536;
const MAX_JOURNAL: u64 = 192 * 1024 * 1024;
const PREALLOC: u64 = 16 * 1024 * 1024;
const RESERVE: u64 = 7_868_000_000;
const REVIEW_ROOT: &str = "/home/tmpadmin/native_rust_registrar_review_20261002";

type Result<T> = std::result::Result<T, String>;

fn stamp() -> u64 {
    let mut t = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut t) };
    t.tv_sec as u64 * 1_000_000_000 + t.tv_nsec as u64
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn ioerr(e: io::Error) -> String {
    format!("IO_FAILURE: {e}")
}

fn percentile(mut xs: Vec<u64>, pct: f64) -> u64 {
    if xs.is_empty() {
        return 0;
    }
    xs.sort_unstable();
    let idx = ((xs.len() as f64 - 1.0) * pct).round() as usize;
    xs[idx.min(xs.len() - 1)]
}

fn raw_hash(event_id: &str, name: &str, payload_json: &str) -> String {
    hex(&sha256(format!("{event_id}\n{name}\n{payload_json}").as_bytes()))
}

#[derive(Clone, Deserialize, Serialize)]
struct Event {
    seq: u64,
    event_id: String,
    content_hash: String,
    record_json: String,
    event_created_ns: u64,
    admitted_ns: u64,
}

#[derive(Clone, Serialize)]
struct RawEvent {
    external_id: String,
    name: String,
    payload_json: String,
    created_ns: u64,
}

#[derive(Clone, Serialize)]
struct DurableAck {
    ok: bool,
    seq: u64,
    event_id: String,
    content_hash: String,
    duplicate: bool,
    admitted_ns: u64,
    durable_ns: u64,
    ack_ns: u64,
    through_seq: u64,
    physical_batch_id: u64,
}

#[derive(Clone, Serialize)]
struct OwnerReply {
    ok: bool,
    ack: Option<DurableAck>,
    error: Option<String>,
}

struct IngressRequest {
    raw: RawEvent,
    offered_ns: u64,
    ack_tx: SyncSender<OwnerReply>,
}

#[derive(Clone)]
struct Floor {
    seq: u64,
    offset: u64,
    chain: [u8; 32],
}

struct BatchDurable {
    durable_ns: u64,
    write_start_ns: u64,
    append_ns: u64,
    journal_fsync_ns: u64,
    floor_sync_ns: u64,
    journal_sync_ns: u64,
    through_seq: u64,
    journal_write_syscalls: u64,
    floor_writes: u64,
    sync_calls: u64,
    journal_bytes_written: u64,
    journal_offset: u64,
    chain_hash: String,
}

struct Registrar {
    case: PathBuf,
    file: File,
    floor_file: File,
    events: Vec<Event>,
    ids: HashMap<String, u64>,
    chain: [u8; 32],
    offset: u64,
    journal_writes: u64,
    floor_writes: u64,
    sync_calls: u64,
    journal_bytes: u64,
    tail_removed: u64,
    recovery_ns: u64,
    failpoint: String,
}

fn read_floor(file: &mut File) -> Result<Floor> {
    file.seek(SeekFrom::Start(0)).map_err(ioerr)?;
    let mut b = Vec::new();
    file.read_to_end(&mut b).map_err(ioerr)?;
    if b.len() != 88 || &b[..8] != FLOOR || sha256(&b[..56]) != b[56..88] {
        return Err("corrupt durable floor; fail closed".into());
    }
    Ok(Floor {
        seq: u64::from_le_bytes(b[8..16].try_into().unwrap()),
        offset: u64::from_le_bytes(b[16..24].try_into().unwrap()),
        chain: b[24..56].try_into().unwrap(),
    })
}

fn save_floor(file: &mut File, f: &Floor) -> Result<()> {
    let mut b = FLOOR.to_vec();
    b.extend(f.seq.to_le_bytes());
    b.extend(f.offset.to_le_bytes());
    b.extend(f.chain);
    let digest = sha256(&b);
    b.extend(digest);
    if file.metadata().map_err(ioerr)?.len() != 88 {
        return Err("IO_FAILURE: floor size changed".into());
    }
    file.seek(SeekFrom::Start(0)).map_err(ioerr)?;
    file.write_all(&b).map_err(ioerr)?;
    file.sync_all().map_err(ioerr)
}

fn open_floor(path: &Path, journal_size: u64) -> Result<(File, Floor)> {
    let initial = Floor { seq: 0, offset: 0, chain: [0; 32] };
    let mut created = false;
    let mut file = match OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
    {
        Ok(file) => file,
        Err(e) if e.kind() == io::ErrorKind::NotFound && journal_size == 0 => {
            created = true;
            OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .custom_flags(libc::O_NOFOLLOW)
                .open(path)
                .map_err(ioerr)?
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            return Err("nonempty journal without durable floor; fail closed".into())
        }
        Err(e) => return Err(ioerr(e)),
    };
    let fd_meta = file.metadata().map_err(ioerr)?;
    let path_meta = fs::symlink_metadata(path).map_err(ioerr)?;
    if !fd_meta.file_type().is_file()
        || !path_meta.file_type().is_file()
        || fd_meta.dev() != path_meta.dev()
        || fd_meta.ino() != path_meta.ino()
        || (!created && fd_meta.len() != 88)
    {
        return Err("corrupt durable floor type/inode/size; fail closed".into());
    }
    let floor = if created {
        let mut b = FLOOR.to_vec();
        b.extend(initial.seq.to_le_bytes());
        b.extend(initial.offset.to_le_bytes());
        b.extend(initial.chain);
        b.extend(sha256(&b));
        file.write_all(&b).map_err(ioerr)?;
        file.sync_all().map_err(ioerr)?;
        File::open(path.parent().ok_or("floor parent missing")?)
            .map_err(ioerr)?
            .sync_all()
            .map_err(ioerr)?;
        initial
    } else {
        read_floor(&mut file)?
    };
    Ok((file, floor))
}

fn storage_path(case: &Path, exact: &str, root: &str, legacy: &str, leaf: &str) -> PathBuf {
    if let Some(p) = std::env::var_os(exact) {
        return PathBuf::from(p);
    }
    if let Some(p) = std::env::var_os(legacy) {
        return PathBuf::from(p);
    }
    if let Some(root) = std::env::var_os(root) {
        let name = case.file_name().unwrap_or_default();
        return PathBuf::from(root).join(name).join(leaf);
    }
    case.join(leaf)
}

fn preallocate_journal(file: &File, parent: &Path) -> Result<()> {
    let path = CString::new(parent.as_os_str().as_encoded_bytes()).map_err(|e| e.to_string())?;
    let mut stat = unsafe { std::mem::zeroed::<libc::statvfs>() };
    if unsafe { libc::statvfs(path.as_ptr(), &mut stat) } != 0 {
        return Err("preallocation statvfs failed".into());
    }
    if stat.f_bavail as u64 * stat.f_frsize as u64 <= RESERVE + PREALLOC + 16 * 1024 * 1024 {
        return Err("preallocation disk reserve".into());
    }
    if unsafe { libc::fallocate(file.as_raw_fd(), libc::FALLOC_FL_KEEP_SIZE, 0, PREALLOC as libc::off_t) } != 0 {
        return Err(ioerr(io::Error::last_os_error()));
    }
    let meta = file.metadata().map_err(ioerr)?;
    if meta.len() > PREALLOC || meta.blocks() * 512 < PREALLOC {
        return Err("preallocated journal extent incomplete; fail closed".into());
    }
    Ok(())
}

fn validate_event(e: &Event) -> Result<()> {
    if e.seq == 0
        || e.event_id.is_empty()
        || e.event_id.len() > 100
        || e.record_json.len() > 65_536
        || e.content_hash != hex(&sha256(e.record_json.as_bytes()))
    {
        return Err("event identity/content hash/size mismatch".into());
    }
    let r: Value = serde_json::from_str(&e.record_json).map_err(|e| e.to_string())?;
    if r["event_id"] != e.event_id
        || !r["payload_json"].is_string()
        || !["SIGNAL_SEEN", "DECISION_MADE"].contains(&r["name"].as_str().unwrap_or(""))
    {
        return Err("canonical envelope identity/type mismatch".into());
    }
    Ok(())
}

fn index_events(events: &mut Vec<Event>, ids: &mut HashMap<String, u64>, batch: Vec<Event>) -> Result<()> {
    if events.len() + batch.len() > MAX_EVENTS {
        return Err("bounded journal index full".into());
    }
    for e in batch {
        validate_event(&e)?;
        if e.seq != events.len() as u64 + 1 || ids.contains_key(&e.event_id) {
            return Err("noncontinuous or duplicate journal sequence/identity".into());
        }
        ids.insert(e.event_id.clone(), e.seq);
        events.push(e);
    }
    Ok(())
}

impl Registrar {
    fn open(case: PathBuf, failpoint: String) -> Result<Self> {
        let start = stamp();
        fs::create_dir_all(&case).map_err(ioerr)?;
        let case = fs::canonicalize(case).map_err(ioerr)?;
        if !case.starts_with(REVIEW_ROOT) {
            return Err("explicit isolated registrar root required".into());
        }
        let journal_path = storage_path(
            &case,
            "NATIVE_C27_JOURNAL_PATH",
            "NATIVE_C27_JOURNAL_ROOT",
            "NATIVE_SPLIT_JOURNAL_PATH",
            "registrar.journal",
        );
        let floor_path = storage_path(
            &case,
            "NATIVE_C27_FLOOR_PATH",
            "NATIVE_C27_FLOOR_ROOT",
            "NATIVE_SPLIT_FLOOR_PATH",
            "commit_floor.bin",
        );
        for path in [&journal_path, &floor_path] {
            fs::create_dir_all(path.parent().ok_or("storage parent missing")?).map_err(ioerr)?;
        }
        if !journal_path.exists() && floor_path.exists() {
            return Err("journal missing with durable floor; fail closed".into());
        }
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&journal_path)
            .map_err(ioerr)?;
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err("single registrar journal lock held".into());
        }
        let size = file.metadata().map_err(ioerr)?.len();
        if size > MAX_JOURNAL {
            return Err("journal byte bound exceeded".into());
        }
        preallocate_journal(&file, journal_path.parent().ok_or("journal parent missing")?)?;
        file.sync_all().map_err(ioerr)?;
        File::open(journal_path.parent().ok_or("journal parent missing")?)
            .map_err(ioerr)?
            .sync_all()
            .map_err(ioerr)?;
        let (mut floor_file, floor) = open_floor(&floor_path, size)?;
        if floor.offset > size {
            return Err("journal truncated below durable floor; fail closed".into());
        }
        let mut events = Vec::new();
        let mut ids = HashMap::new();
        let mut chain = [0; 32];
        let mut offset = 0u64;
        let mut floor_seen = floor.seq == 0 && floor.offset == 0 && floor.chain == [0; 32];
        while offset < size {
            if size - offset < HEADER as u64 {
                break;
            }
            let mut header = [0; HEADER];
            file.read_exact(&mut header).map_err(ioerr)?;
            if &header[..8] != MAGIC {
                return Err("corrupt frame header; fail closed".into());
            }
            let length = u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize;
            let first = u64::from_le_bytes(header[12..20].try_into().unwrap());
            let count = u32::from_le_bytes(header[20..24].try_into().unwrap()) as usize;
            if length > MAX_PAYLOAD
                || count == 0
                || count > 32
                || first != events.len() as u64 + 1
                || header[24..56] != chain
            {
                return Err("corrupt frame identity/length/chain; fail closed".into());
            }
            let end = offset + (HEADER + length + TRAILER) as u64;
            if end > size {
                break;
            }
            let mut payload = vec![0; length];
            file.read_exact(&mut payload).map_err(ioerr)?;
            let mut trailer = [0; TRAILER];
            file.read_exact(&mut trailer).map_err(ioerr)?;
            let mut signed = header.to_vec();
            signed.extend(&payload);
            let digest = sha256(&signed);
            if digest != trailer[..32] || &trailer[32..] != END {
                return Err("corrupt complete frame checksum; fail closed".into());
            }
            let batch: Vec<Event> = serde_json::from_slice(&payload).map_err(|e| e.to_string())?;
            if batch.len() != count {
                return Err("frame count mismatch".into());
            }
            index_events(&mut events, &mut ids, batch)?;
            chain = digest;
            offset = end;
            if offset == floor.offset {
                if events.len() as u64 != floor.seq || chain != floor.chain {
                    return Err("durable floor prefix mismatch".into());
                }
                floor_seen = true;
            }
        }
        if !floor_seen || offset < floor.offset {
            return Err("durable floor absent from valid prefix; fail closed".into());
        }
        let tail_removed = size - offset;
        if tail_removed > 0 {
            file.set_len(offset).map_err(ioerr)?;
        }
        file.seek(SeekFrom::Start(offset)).map_err(ioerr)?;
        file.sync_all().map_err(ioerr)?;
        if floor.seq != events.len() as u64 || floor.offset != offset || floor.chain != chain {
            save_floor(&mut floor_file, &Floor { seq: events.len() as u64, offset, chain })?;
        }
        Ok(Self {
            case,
            file,
            floor_file,
            events,
            ids,
            chain,
            offset,
            journal_writes: 0,
            floor_writes: 0,
            sync_calls: 0,
            journal_bytes: 0,
            tail_removed,
            recovery_ns: stamp() - start,
            failpoint,
        })
    }

    fn pressure(&self) -> Result<()> {
        for root in [
            std::env::var_os("NATIVE_C27_JOURNAL_PATH").or_else(|| std::env::var_os("NATIVE_SPLIT_JOURNAL_PATH")),
            std::env::var_os("NATIVE_C27_FLOOR_PATH").or_else(|| std::env::var_os("NATIVE_SPLIT_FLOOR_PATH")),
        ]
        .into_iter()
        .flatten()
        {
            let p = PathBuf::from(root);
            let parent = p.parent().ok_or("storage parent missing")?;
            let path = CString::new(parent.as_os_str().as_encoded_bytes()).map_err(|e| e.to_string())?;
            let mut stat = unsafe { std::mem::zeroed::<libc::statvfs>() };
            if unsafe { libc::statvfs(path.as_ptr(), &mut stat) } != 0 {
                return Err("statvfs failed".into());
            }
            if stat.f_bavail as u64 * stat.f_frsize as u64 <= RESERVE + 16 * 1024 * 1024 {
                return Err("fixture disk reserve".into());
            }
        }
        Ok(())
    }

    fn checkpoint(&self, name: &str) -> Result<()> {
        if self.failpoint == name {
            fs::write(
                self.case.join("registrar_phase.json"),
                json!({"phase":name,"pid":std::process::id(),"at_ns":stamp()}).to_string(),
            )
            .map_err(ioerr)?;
            loop {
                thread::sleep(Duration::from_millis(2));
            }
        }
        Ok(())
    }

    fn write_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        let mut done = 0;
        while done < bytes.len() {
            match self.file.write(&bytes[done..]) {
                Ok(0) => return Err("IO_FAILURE: zero journal write".into()),
                Ok(n) => {
                    done += n;
                    self.journal_writes += 1;
                    self.journal_bytes += n as u64;
                }
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(ioerr(e)),
            }
        }
        Ok(())
    }

    fn batch(&mut self, batch: Vec<Event>) -> Result<BatchDurable> {
        if batch.is_empty() || batch.len() > 32 {
            return Err("bounded batch size 1..32 required".into());
        }
        self.pressure()?;
        let before = self.events.len() as u64;
        let mut expected = before + 1;
        let mut new = Vec::new();
        let mut pending_ids = HashMap::new();
        for (i, e) in batch.iter().enumerate() {
            validate_event(e)?;
            if i > 0 && e.seq != batch[i - 1].seq + 1 {
                return Err("batch sequence gap".into());
            }
            if e.seq <= before {
                let old = &self.events[e.seq as usize - 1];
                if e.event_id != old.event_id || e.content_hash != old.content_hash || e.record_json != old.record_json {
                    return Err("conflicting replay; no ACK".into());
                }
            } else {
                if e.seq != expected || self.ids.contains_key(&e.event_id) || pending_ids.contains_key(&e.event_id) {
                    return Err("new sequence gap or identity conflict".into());
                }
                pending_ids.insert(e.event_id.clone(), e.seq);
                expected += 1;
                new.push(e.clone());
            }
        }
        if self.events.len() + new.len() > MAX_EVENTS {
            return Err("bounded journal index full".into());
        }
        let write_start = stamp();
        let mut journal_sync_ns = 0;
        let mut floor_ns = 0;
        let mut append_ns = 0;
        let mut journal_fsync_ns = 0;
        if !new.is_empty() {
            let payload = serde_json::to_vec(&new).map_err(|e| e.to_string())?;
            if payload.len() > MAX_PAYLOAD {
                return Err("bounded batch bytes".into());
            }
            let mut bytes = MAGIC.to_vec();
            bytes.extend((payload.len() as u32).to_le_bytes());
            bytes.extend(new[0].seq.to_le_bytes());
            bytes.extend((new.len() as u32).to_le_bytes());
            bytes.extend(self.chain);
            bytes.extend(payload);
            let chain = sha256(&bytes);
            bytes.extend(chain);
            bytes.extend(END);
            if self.offset + bytes.len() as u64 > PREALLOC {
                return Err("bounded preallocated journal bytes full".into());
            }
            self.checkpoint("before_write")?;
            if self.failpoint == "partial_write_error" {
                self.write_bytes(&bytes[..bytes.len() / 2])?;
                self.file.sync_all().map_err(ioerr)?;
                return Err("IO_FAILURE: injected partial journal write error".into());
            }
            self.write_bytes(&bytes)?;
            let append_end = stamp();
            append_ns = append_end - write_start;
            self.checkpoint("before_journal_sync")?;
            if self.failpoint == "journal_sync_error" {
                return Err("IO_FAILURE: injected journal fsync error".into());
            }
            self.file.sync_all().map_err(ioerr)?;
            journal_fsync_ns = stamp() - append_end;
            self.sync_calls += 1;
            journal_sync_ns = stamp() - write_start;
            self.checkpoint("after_journal_sync")?;
            let floor_start = stamp();
            let next_floor = Floor { seq: expected - 1, offset: self.offset + bytes.len() as u64, chain };
            if self.failpoint == "floor_partial_write_error" {
                let mut b = FLOOR.to_vec();
                b.extend(next_floor.seq.to_le_bytes());
                self.floor_file.seek(SeekFrom::Start(0)).map_err(ioerr)?;
                self.floor_file.write_all(&b).map_err(ioerr)?;
                return Err("IO_FAILURE: injected partial floor write".into());
            }
            if self.failpoint == "floor_sync_error" {
                self.floor_file.seek(SeekFrom::Start(0)).map_err(ioerr)?;
                let mut b = FLOOR.to_vec();
                b.extend(next_floor.seq.to_le_bytes());
                b.extend(next_floor.offset.to_le_bytes());
                b.extend(next_floor.chain);
                b.extend(sha256(&b));
                self.floor_file.write_all(&b).map_err(ioerr)?;
                return Err("IO_FAILURE: injected floor fsync error".into());
            }
            save_floor(&mut self.floor_file, &next_floor)?;
            self.floor_writes += 1;
            self.sync_calls += 1;
            floor_ns = stamp() - floor_start;
            self.offset += bytes.len() as u64;
            self.chain = chain;
            index_events(&mut self.events, &mut self.ids, new)?;
            self.checkpoint("after_durable_before_ack")?;
        }
        Ok(BatchDurable {
            durable_ns: stamp(),
            write_start_ns: write_start,
            append_ns,
            journal_fsync_ns,
            floor_sync_ns: floor_ns,
            journal_sync_ns,
            through_seq: self.events.len() as u64,
            journal_write_syscalls: self.journal_writes,
            floor_writes: self.floor_writes,
            sync_calls: self.sync_calls,
            journal_bytes_written: self.journal_bytes,
            journal_offset: self.offset,
            chain_hash: hex(&self.chain),
        })
    }

    fn status_json(&self) -> Value {
        json!({
            "ok": true,
            "through_seq": self.events.len(),
            "journal_offset": self.offset,
            "chain_hash": hex(&self.chain),
            "journal_write_syscalls": self.journal_writes,
            "floor_writes": self.floor_writes,
            "sync_calls": self.sync_calls,
            "journal_bytes_written": self.journal_bytes,
            "tail_removed_bytes": self.tail_removed,
            "recovery_ns": self.recovery_ns,
            "queue_bound_events": 128,
            "batch_bound": 32,
            "journal_event_bound": MAX_EVENTS
        })
    }
}

struct IdentityEntry {
    seq: u64,
    raw_hash: String,
    content_hash: String,
    admitted_ns: u64,
    durable_ack: Option<DurableAck>,
    waiters: Vec<SyncSender<OwnerReply>>,
}

struct AdmissionState {
    next_seq: u64,
    durable_frontier: u64,
    entries: HashMap<String, IdentityEntry>,
    fail_closed: Option<String>,
    pending_waiters: usize,
    pending_cap: usize,
}

impl AdmissionState {
    fn from_recovered(events: &[Event], pending_cap: usize) -> Result<Self> {
        let mut entries = HashMap::new();
        for e in events {
            validate_event(e)?;
            let v: Value = serde_json::from_str(&e.record_json).map_err(|e| e.to_string())?;
            let name = v["name"].as_str().ok_or("recovered event missing name")?;
            let payload_json = v["payload_json"].as_str().ok_or("recovered event missing payload_json")?;
            entries.insert(
                e.event_id.clone(),
                IdentityEntry {
                    seq: e.seq,
                    raw_hash: raw_hash(&e.event_id, name, payload_json),
                    content_hash: e.content_hash.clone(),
                    admitted_ns: e.admitted_ns,
                    durable_ack: Some(DurableAck {
                        ok: true,
                        seq: e.seq,
                        event_id: e.event_id.clone(),
                        content_hash: e.content_hash.clone(),
                        duplicate: false,
                        admitted_ns: e.admitted_ns,
                        durable_ns: 0,
                        ack_ns: 0,
                        through_seq: events.len() as u64,
                        physical_batch_id: 0,
                    }),
                    waiters: Vec::new(),
                },
            );
        }
        Ok(Self {
            next_seq: events.len() as u64 + 1,
            durable_frontier: events.len() as u64,
            entries,
            fail_closed: None,
            pending_waiters: 0,
            pending_cap,
        })
    }
}

#[derive(Default)]
struct Metrics {
    offered: u64,
    ingress_received: u64,
    admitted_new: u64,
    duplicate_exact: u64,
    denied: u64,
    conflicts: u64,
    queue_full: u64,
    acked_unique: u64,
    ack_delivered: u64,
    ack_errors: u64,
    physical_groups: u64,
    journal_write_syscalls: u64,
    floor_writes: u64,
    sync_calls: u64,
    journal_bytes_written: u64,
    ingress_depth_cur: u64,
    ingress_depth_peak: u64,
    staging_depth_cur: u64,
    staging_depth_peak: u64,
    pending_depth_cur: u64,
    pending_depth_peak: u64,
    first_offer_ns: u64,
    last_offer_ns: u64,
    first_durable_ns: u64,
    last_durable_ns: u64,
    last_ack_ns: u64,
    planned_window_start_ns: u64,
    planned_window_end_ns: u64,
    durable_batch_samples: Vec<(u64, u64)>,
    producer_block_ns: Vec<u64>,
    admission_latency_ns: Vec<u64>,
    ingress_to_durable_ns: Vec<u64>,
    durable_to_ack_ns: Vec<u64>,
    owner_ack_latency_ns: Vec<u64>,
    journal_fsync_ns: Vec<u64>,
    floor_sync_ns: Vec<u64>,
    physical_batch_sizes: Vec<u64>,
    owner_backlog_samples: Vec<(u64, u64)>,
}

fn inc_ingress(m: &Arc<Mutex<Metrics>>) {
    let mut g = m.lock().unwrap();
    g.ingress_depth_cur += 1;
    g.ingress_depth_peak = g.ingress_depth_peak.max(g.ingress_depth_cur);
}

fn dec_ingress(m: &Arc<Mutex<Metrics>>) {
    let mut g = m.lock().unwrap();
    g.ingress_depth_cur = g.ingress_depth_cur.saturating_sub(1);
    g.ingress_received += 1;
}

fn inc_staging(m: &Arc<Mutex<Metrics>>) {
    let mut g = m.lock().unwrap();
    g.staging_depth_cur += 1;
    g.staging_depth_peak = g.staging_depth_peak.max(g.staging_depth_cur);
}

fn dec_staging(m: &Arc<Mutex<Metrics>>) {
    let mut g = m.lock().unwrap();
    g.staging_depth_cur = g.staging_depth_cur.saturating_sub(1);
}

fn set_pending_peak(m: &Arc<Mutex<Metrics>>, pending: u64) {
    let mut g = m.lock().unwrap();
    g.pending_depth_cur = pending;
    g.pending_depth_peak = g.pending_depth_peak.max(pending);
}

fn make_event(raw: &RawEvent, seq: u64, admitted_ns: u64) -> Result<Event> {
    if raw.external_id.is_empty() || raw.external_id.len() > 100 {
        return Err("event identity/content hash/size mismatch".into());
    }
    if !["SIGNAL_SEEN", "DECISION_MADE"].contains(&raw.name.as_str()) {
        return Err("canonical envelope identity/type mismatch".into());
    }
    let record_json = json!({
        "event_id": raw.external_id,
        "name": raw.name,
        "payload_json": raw.payload_json,
        "seq": seq
    })
    .to_string();
    if record_json.len() > 65_536 {
        return Err("event identity/content hash/size mismatch".into());
    }
    let content_hash = hex(&sha256(record_json.as_bytes()));
    Ok(Event {
        seq,
        event_id: raw.external_id.clone(),
        content_hash,
        record_json,
        event_created_ns: raw.created_ns,
        admitted_ns,
    })
}

fn send_reply(tx: &SyncSender<OwnerReply>, reply: OwnerReply) -> Result<()> {
    match tx.try_send(reply) {
        Ok(()) => Ok(()),
        Err(TrySendError::Full(_)) => Err("ACK consumer stall; fail closed".into()),
        Err(TrySendError::Disconnected(_)) => Err("ACK consumer disconnected; fail closed".into()),
    }
}

fn start_admission(
    ingress_rx: Receiver<IngressRequest>,
    staging_tx: SyncSender<Event>,
    state: Arc<Mutex<AdmissionState>>,
    metrics: Arc<Mutex<Metrics>>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        while let Ok(req) = ingress_rx.recv() {
            dec_ingress(&metrics);
            let admitted_ns = stamp();
            let mut event_to_stage: Option<Event> = None;
            let mut immediate_reply: Option<OwnerReply> = None;
            let request_hash = raw_hash(&req.raw.external_id, &req.raw.name, &req.raw.payload_json);
            {
                let mut st = state.lock().unwrap();
                if let Some(fault) = st.fail_closed.clone() {
                    immediate_reply = Some(OwnerReply { ok: false, ack: None, error: Some(fault) });
                } else if st.entries.contains_key(&req.raw.external_id) {
                    let mut conflict: Option<String> = None;
                    let mut durable_dup: Option<DurableAck> = None;
                    let mut add_waiter = false;
                    let pending_full = st.pending_waiters >= st.pending_cap;
                    {
                        let entry = st.entries.get_mut(&req.raw.external_id).unwrap();
                        if entry.raw_hash != request_hash {
                            conflict = Some("conflicting replay; fail closed".to_string());
                        } else if let Some(ack) = entry.durable_ack.clone() {
                            durable_dup = Some(ack);
                        } else if pending_full {
                            immediate_reply = Some(OwnerReply { ok: false, ack: None, error: Some("bounded pending waiter capacity exceeded".into()) });
                        } else {
                            add_waiter = true;
                            entry.waiters.push(req.ack_tx.clone());
                        }
                    }
                    if let Some(fault) = conflict {
                        st.fail_closed = Some(fault.clone());
                        let mut g = metrics.lock().unwrap();
                        g.conflicts += 1;
                        g.denied += 1;
                        immediate_reply = Some(OwnerReply { ok: false, ack: None, error: Some(fault) });
                    } else if let Some(mut dup) = durable_dup {
                        dup.duplicate = true;
                        dup.ack_ns = stamp();
                        immediate_reply = Some(OwnerReply { ok: true, ack: Some(dup), error: None });
                        metrics.lock().unwrap().duplicate_exact += 1;
                    } else if add_waiter {
                        st.pending_waiters += 1;
                        metrics.lock().unwrap().duplicate_exact += 1;
                    }
                } else {
                    match make_event(&req.raw, st.next_seq, admitted_ns) {
                        Ok(event) => {
                            if st.entries.len() >= MAX_EVENTS {
                                let err = "bounded journal index full".to_string();
                                metrics.lock().unwrap().denied += 1;
                                immediate_reply = Some(OwnerReply { ok: false, ack: None, error: Some(err) });
                            } else if st.pending_waiters >= st.pending_cap {
                                let err = "bounded pending waiter capacity exceeded".to_string();
                                metrics.lock().unwrap().denied += 1;
                                immediate_reply = Some(OwnerReply { ok: false, ack: None, error: Some(err) });
                            } else {
                                st.entries.insert(
                                    event.event_id.clone(),
                                    IdentityEntry {
                                        seq: event.seq,
                                        raw_hash: request_hash,
                                        content_hash: event.content_hash.clone(),
                                        admitted_ns,
                                        durable_ack: None,
                                        waiters: vec![req.ack_tx.clone()],
                                    },
                                );
                                st.pending_waiters += 1;
                                st.next_seq += 1;
                                let pending = st.next_seq - st.durable_frontier - 1;
                                set_pending_peak(&metrics, pending);
                                {
                                    let mut g = metrics.lock().unwrap();
                                    g.admitted_new += 1;
                                    g.admission_latency_ns.push(admitted_ns.saturating_sub(req.offered_ns));
                                }
                                event_to_stage = Some(event);
                            }
                        }
                        Err(err) => {
                            metrics.lock().unwrap().denied += 1;
                            immediate_reply = Some(OwnerReply { ok: false, ack: None, error: Some(err) });
                        }
                    }
                }
            }
            if let Some(reply) = immediate_reply {
                if let Err(err) = send_reply(&req.ack_tx, reply) {
                    let mut st = state.lock().unwrap();
                    st.fail_closed = Some(err.clone());
                    metrics.lock().unwrap().ack_errors += 1;
                }
            }
            if let Some(event) = event_to_stage {
                inc_staging(&metrics);
                if staging_tx.send(event).is_err() {
                    let mut st = state.lock().unwrap();
                    st.fail_closed = Some("staging receiver closed; fail closed".into());
                    metrics.lock().unwrap().denied += 1;
                    break;
                }
            }
        }
    })
}

fn complete_batch(
    state: &Arc<Mutex<AdmissionState>>,
    metrics: &Arc<Mutex<Metrics>>,
    events: &[Event],
    durable: &BatchDurable,
    physical_batch_id: u64,
) -> Result<()> {
    let mut deliveries: Vec<(SyncSender<OwnerReply>, OwnerReply)> = Vec::new();
    {
        let mut st = state.lock().unwrap();
        for e in events {
            if e.seq != st.durable_frontier + 1 {
                let fault = format!("ordered durable frontier gap at seq {}", e.seq);
                st.fail_closed = Some(fault.clone());
                return Err(fault);
            }
            let entry = st.entries.get_mut(&e.event_id).ok_or("completion identity missing; fail closed")?;
            if entry.seq != e.seq || entry.content_hash != e.content_hash {
                let fault = "completion identity/hash mismatch; fail closed".to_string();
                st.fail_closed = Some(fault.clone());
                return Err(fault);
            }
            let ack_ns = stamp();
            let ack = DurableAck {
                ok: true,
                seq: e.seq,
                event_id: e.event_id.clone(),
                content_hash: e.content_hash.clone(),
                duplicate: false,
                admitted_ns: entry.admitted_ns,
                durable_ns: durable.durable_ns,
                ack_ns,
                through_seq: durable.through_seq,
                physical_batch_id,
            };
            entry.durable_ack = Some(ack.clone());
            let waiters = std::mem::take(&mut entry.waiters);
            st.pending_waiters = st.pending_waiters.saturating_sub(waiters.len());
            st.durable_frontier = e.seq;
            for tx in waiters {
                deliveries.push((tx, OwnerReply { ok: true, ack: Some(ack.clone()), error: None }));
            }
            let mut g = metrics.lock().unwrap();
            g.acked_unique += 1;
            g.durable_to_ack_ns.push(ack_ns.saturating_sub(durable.durable_ns));
            g.ingress_to_durable_ns.push(durable.durable_ns.saturating_sub(e.event_created_ns));
            g.last_ack_ns = g.last_ack_ns.max(ack_ns);
        }
        let pending = st.next_seq - st.durable_frontier - 1;
        set_pending_peak(metrics, pending);
    }
    for (tx, reply) in deliveries {
        send_reply(&tx, reply)?;
        metrics.lock().unwrap().ack_delivered += 1;
    }
    Ok(())
}

fn fail_pending(state: &Arc<Mutex<AdmissionState>>, metrics: &Arc<Mutex<Metrics>>, error: String) {
    let mut deliveries: Vec<SyncSender<OwnerReply>> = Vec::new();
    {
        let mut st = state.lock().unwrap();
        st.fail_closed = Some(error.clone());
        for entry in st.entries.values_mut() {
            deliveries.extend(std::mem::take(&mut entry.waiters));
        }
        st.pending_waiters = 0;
    }
    for tx in deliveries {
        let _ = send_reply(&tx, OwnerReply { ok: false, ack: None, error: Some(error.clone()) });
        metrics.lock().unwrap().ack_errors += 1;
    }
}

fn start_writer(
    staging_rx: Receiver<Event>,
    mut registrar: Registrar,
    state: Arc<Mutex<AdmissionState>>,
    metrics: Arc<Mutex<Metrics>>,
    max_batch: usize,
    linger_us: u64,
) -> thread::JoinHandle<Value> {
    thread::spawn(move || {
        let mut physical_batch_id = 0u64;
        loop {
            let first = match staging_rx.recv() {
                Ok(e) => e,
                Err(_) => break,
            };
            dec_staging(&metrics);
            let mut batch = vec![first];
            let mut approx_bytes: usize = batch[0].record_json.len() + 256;
            let deadline = if linger_us > 0 { Some(Instant::now() + Duration::from_micros(linger_us)) } else { None };
            while batch.len() < max_batch.min(32) {
                match staging_rx.try_recv() {
                    Ok(e) => {
                        let next_bytes = e.record_json.len() + 256;
                        if approx_bytes + next_bytes > MAX_PAYLOAD {
                            break;
                        }
                        approx_bytes += next_bytes;
                        batch.push(e);
                        dec_staging(&metrics);
                    }
                    Err(TryRecvError::Empty) => {
                        let Some(deadline) = deadline else { break };
                        let now = Instant::now();
                        if now >= deadline {
                            break;
                        }
                        match staging_rx.recv_timeout(deadline - now) {
                            Ok(e) => {
                                let next_bytes = e.record_json.len() + 256;
                                if approx_bytes + next_bytes > MAX_PAYLOAD {
                                    break;
                                }
                                approx_bytes += next_bytes;
                                batch.push(e);
                                dec_staging(&metrics);
                            }
                            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => break,
                            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                        }
                    }
                    Err(TryRecvError::Disconnected) => break,
                }
            }
            physical_batch_id += 1;
            let durable = match registrar.batch(batch.clone()) {
                Ok(row) => row,
                Err(error) => {
                    fail_pending(&state, &metrics, error.clone());
                    return json!({"ok":false,"fault":error,"registrar":registrar.status_json()});
                }
            };
            {
                let mut g = metrics.lock().unwrap();
                g.physical_groups += 1;
                g.physical_batch_sizes.push(batch.len() as u64);
                g.durable_batch_samples.push((durable.durable_ns, batch.len() as u64));
                g.journal_fsync_ns.push(durable.journal_fsync_ns);
                g.floor_sync_ns.push(durable.floor_sync_ns);
                g.first_durable_ns = if g.first_durable_ns == 0 { durable.durable_ns } else { g.first_durable_ns.min(durable.durable_ns) };
                g.last_durable_ns = g.last_durable_ns.max(durable.durable_ns);
                g.journal_write_syscalls = durable.journal_write_syscalls;
                g.floor_writes = durable.floor_writes;
                g.sync_calls = durable.sync_calls;
                g.journal_bytes_written = durable.journal_bytes_written;
                let _ = durable.write_start_ns;
                let _ = durable.append_ns;
                let _ = durable.journal_sync_ns;
                let _ = durable.journal_offset;
                let _ = &durable.chain_hash;
            }
            if let Err(error) = complete_batch(&state, &metrics, &batch, &durable, physical_batch_id) {
                state.lock().unwrap().fail_closed = Some(error.clone());
                metrics.lock().unwrap().ack_errors += 1;
                return json!({"ok":false,"fault":error,"registrar":registrar.status_json()});
            }
        }
        json!({"ok":true,"fault":Value::Null,"registrar":registrar.status_json()})
    })
}

#[derive(Clone)]
struct PipelineOpts {
    ingress_cap: usize,
    staging_cap: usize,
    pending_cap: usize,
    max_batch: usize,
    linger_us: u64,
    failpoint: String,
}

impl Default for PipelineOpts {
    fn default() -> Self {
        Self { ingress_cap: 1024, staging_cap: 1024, pending_cap: MAX_EVENTS, max_batch: 32, linger_us: 0, failpoint: String::new() }
    }
}

struct Pipeline {
    tx: Option<SyncSender<IngressRequest>>,
    admission: thread::JoinHandle<()>,
    writer: thread::JoinHandle<Value>,
    state: Arc<Mutex<AdmissionState>>,
    metrics: Arc<Mutex<Metrics>>,
}

fn start_pipeline(case: PathBuf, opts: PipelineOpts) -> Result<Pipeline> {
    let registrar = Registrar::open(case, opts.failpoint)?;
    let state = Arc::new(Mutex::new(AdmissionState::from_recovered(&registrar.events, opts.pending_cap)?));
    let metrics = Arc::new(Mutex::new(Metrics::default()));
    let (ingress_tx, ingress_rx) = sync_channel::<IngressRequest>(opts.ingress_cap);
    let (staging_tx, staging_rx) = sync_channel::<Event>(opts.staging_cap);
    let admission = start_admission(ingress_rx, staging_tx, state.clone(), metrics.clone());
    let writer = start_writer(staging_rx, registrar, state.clone(), metrics.clone(), opts.max_batch, opts.linger_us);
    Ok(Pipeline { tx: Some(ingress_tx), admission, writer, state, metrics })
}

fn finish_pipeline(mut p: Pipeline) -> Value {
    p.tx.take();
    let _ = p.admission.join();
    let writer = p.writer.join().unwrap_or_else(|_| json!({"ok":false,"fault":"writer panic"}));
    let fault = p.state.lock().unwrap().fail_closed.clone();
    let m = p.metrics.lock().unwrap();
    let active_ns = if m.first_offer_ns > 0 && m.last_durable_ns > m.first_offer_ns {
        m.last_durable_ns - m.first_offer_ns
    } else {
        0
    };
    let offer_ns = if m.first_offer_ns > 0 && m.last_offer_ns > m.first_offer_ns {
        m.last_offer_ns - m.first_offer_ns
    } else {
        0
    };
    let drain_ns = if m.last_ack_ns > m.last_offer_ns { m.last_ack_ns - m.last_offer_ns } else { 0 };
    let planned_window_ns = if m.planned_window_end_ns > m.planned_window_start_ns {
        m.planned_window_end_ns - m.planned_window_start_ns
    } else {
        0
    };
    let steady_window_start_ns = if planned_window_ns > 0 && m.first_offer_ns > 0 {
        m.first_offer_ns
    } else {
        m.planned_window_start_ns
    };
    let steady_window_end_ns = if planned_window_ns > 0 { steady_window_start_ns + planned_window_ns } else { 0 };
    let window_completed: u64 = if planned_window_ns > 0 {
        m.durable_batch_samples
            .iter()
            .filter(|(durable_ns, _)| *durable_ns >= steady_window_start_ns && *durable_ns <= steady_window_end_ns)
            .map(|(_, count)| *count)
            .sum()
    } else {
        m.acked_unique
    };
    let planned_window_s = planned_window_ns as f64 / 1e9;
    let end_to_end_rate = if active_ns > 0 { m.acked_unique as f64 / (active_ns as f64 / 1e9) } else { 0.0 };
    let steady_rate = if planned_window_s > 0.0 { window_completed as f64 / planned_window_s } else { end_to_end_rate };
    let post_window_drain_ns = if steady_window_end_ns > 0 && m.last_ack_ns > steady_window_end_ns { m.last_ack_ns - steady_window_end_ns } else { 0 };
    let last_durable_after_window_ns = if steady_window_end_ns > 0 && m.last_durable_ns > steady_window_end_ns { m.last_durable_ns - steady_window_end_ns } else { 0 };
    let last_offer_before_window_end_ns = if steady_window_end_ns > m.last_offer_ns { steady_window_end_ns - m.last_offer_ns } else { 0 };
    let first_offer_after_scheduled_start_ns = if m.first_offer_ns > m.planned_window_start_ns { m.first_offer_ns - m.planned_window_start_ns } else { 0 };
    let owner_slope = if let (Some(first), Some(last)) = (m.owner_backlog_samples.first(), m.owner_backlog_samples.last()) {
        let dt = (last.0.saturating_sub(first.0)) as f64 / 1e9;
        if dt > 0.0 { (last.1 as f64 - first.1 as f64) / dt } else { 0.0 }
    } else {
        0.0
    };
    json!({
        "ok": fault.is_none() && writer["ok"].as_bool().unwrap_or(false),
        "fault": fault,
        "writer": writer,
        "offered": m.offered,
        "ingress_received": m.ingress_received,
        "admitted_new": m.admitted_new,
        "duplicate_exact": m.duplicate_exact,
        "denied": m.denied,
        "conflicts": m.conflicts,
        "queue_full": m.queue_full,
        "acked_unique": m.acked_unique,
        "ack_delivered": m.ack_delivered,
        "ack_errors": m.ack_errors,
        "physical_groups": m.physical_groups,
        "sync_calls": m.sync_calls,
        "journal_write_syscalls": m.journal_write_syscalls,
        "floor_writes": m.floor_writes,
        "journal_bytes_written": m.journal_bytes_written,
        "ingress_depth_peak": m.ingress_depth_peak,
        "staging_depth_peak": m.staging_depth_peak,
        "pending_depth_peak": m.pending_depth_peak,
        "duration_target_s": planned_window_s,
        "scheduled_window_start_ns": m.planned_window_start_ns,
        "scheduled_window_end_ns": m.planned_window_end_ns,
        "window_start_ns": steady_window_start_ns,
        "window_end_ns": steady_window_end_ns,
        "first_offer_after_scheduled_start_s": first_offer_after_scheduled_start_ns as f64 / 1e9,
        "window_durable_events": window_completed,
        "offer_duration_s": offer_ns as f64 / 1e9,
        "active_to_last_durable_s": active_ns as f64 / 1e9,
        "end_to_end_durable_events_per_s": end_to_end_rate,
        "steady_durable_events_per_s": steady_rate,
        "drain_s": drain_ns as f64 / 1e9,
        "post_window_drain_s": post_window_drain_ns as f64 / 1e9,
        "last_durable_after_window_s": last_durable_after_window_ns as f64 / 1e9,
        "last_offer_before_window_end_s": last_offer_before_window_end_ns as f64 / 1e9,
        "owner_backlog_slope_per_s": owner_slope,
        "latency": {
            "producer_block_ns": pct_json(&m.producer_block_ns),
            "admission_ns": pct_json(&m.admission_latency_ns),
            "ingress_to_durable_ns": pct_json(&m.ingress_to_durable_ns),
            "durable_to_ack_ns": pct_json(&m.durable_to_ack_ns),
            "owner_ack_latency_ns": pct_json(&m.owner_ack_latency_ns),
            "journal_fsync_ns": pct_json(&m.journal_fsync_ns),
            "floor_sync_ns": pct_json(&m.floor_sync_ns)
        },
        "physical_batch": pct_json(&m.physical_batch_sizes)
    })
}

fn pct_json(xs: &[u64]) -> Value {
    json!({
        "count": xs.len(),
        "p50": percentile(xs.to_vec(), 0.50),
        "p95": percentile(xs.to_vec(), 0.95),
        "p99": percentile(xs.to_vec(), 0.99),
        "max": xs.iter().copied().max().unwrap_or(0)
    })
}

fn submit(tx: &SyncSender<IngressRequest>, metrics: &Arc<Mutex<Metrics>>, raw: RawEvent, ack_cap: usize) -> Result<Receiver<OwnerReply>> {
    let (ack_tx, ack_rx) = sync_channel::<OwnerReply>(ack_cap);
    let offered_ns = stamp();
    {
        let mut g = metrics.lock().unwrap();
        g.offered += 1;
        g.first_offer_ns = if g.first_offer_ns == 0 { offered_ns } else { g.first_offer_ns.min(offered_ns) };
        g.last_offer_ns = g.last_offer_ns.max(offered_ns);
    }
    inc_ingress(metrics);
    let before = stamp();
    match tx.send(IngressRequest { raw, offered_ns, ack_tx }) {
        Ok(()) => {
            let blocked = stamp().saturating_sub(before);
            metrics.lock().unwrap().producer_block_ns.push(blocked);
            Ok(ack_rx)
        }
        Err(_) => {
            dec_ingress(metrics);
            Err("ingress closed; fail closed".into())
        }
    }
}

fn make_raw(prefix: &str, seq: u64) -> RawEvent {
    RawEvent {
        external_id: format!("{prefix}-{seq:08}"),
        name: if seq % 2 == 0 { "DECISION_MADE".into() } else { "SIGNAL_SEEN".into() },
        payload_json: json!({"source":"c27","n":seq,"symbol":"BTC-USDT"}).to_string(),
        created_ns: stamp(),
    }
}

fn run_bench(case: PathBuf, total: u64, rate: f64, producers: usize, owner_window: usize, opts: PipelineOpts) -> Result<Value> {
    let linger_us = opts.linger_us;
    let p = start_pipeline(case, opts)?;
    let tx = p.tx.as_ref().unwrap().clone();
    let metrics = p.metrics.clone();
    let base_instant = Instant::now();
    let base_ns = stamp();
    let start_delay = Duration::from_millis(50);
    let start = base_instant + start_delay;
    let planned_window_ns = ((total as f64 / rate.max(1.0)) * 1e9).round() as u64;
    let window_start_ns = base_ns + start_delay.as_nanos() as u64;
    let window_end_ns = window_start_ns + planned_window_ns;
    {
        let mut g = metrics.lock().unwrap();
        g.planned_window_start_ns = window_start_ns;
        g.planned_window_end_ns = window_end_ns;
    }
    let mut handles = Vec::new();
    for producer in 0..producers.max(1) {
        let tx = tx.clone();
        let metrics = metrics.clone();
        let count = (total as usize + producers.max(1) - 1 - producer) / producers.max(1);
        let per_rate = rate / producers.max(1) as f64;
        handles.push(thread::spawn(move || -> Result<()> {
            let interval = Duration::from_secs_f64(1.0 / per_rate.max(1.0));
            let mut inflight: Vec<Receiver<OwnerReply>> = Vec::new();
            let mut sent = 0usize;
            while sent < count {
                let target = start + interval.mul_f64(sent as f64);
                let now = Instant::now();
                if target > now {
                    thread::sleep(target - now);
                }
                while inflight.len() >= owner_window.max(1) {
                    let rx = inflight.remove(0);
                    collect_ack(rx, &metrics)?;
                }
                let global = (sent * producers.max(1) + producer + 1) as u64;
                let raw = make_raw(&format!("p{producer}"), global);
                let rx = submit(&tx, &metrics, raw, 1)?;
                inflight.push(rx);
                let backlog = inflight.len() as u64;
                metrics.lock().unwrap().owner_backlog_samples.push((stamp(), backlog));
                let mut i = 0;
                while i < inflight.len() {
                    match inflight[i].try_recv() {
                        Ok(reply) => {
                            handle_reply(reply, &metrics)?;
                            inflight.remove(i);
                        }
                        Err(std::sync::mpsc::TryRecvError::Empty) => i += 1,
                        Err(std::sync::mpsc::TryRecvError::Disconnected) => return Err("ACK channel disconnected".into()),
                    }
                }
                sent += 1;
            }
            for rx in inflight {
                collect_ack(rx, &metrics)?;
            }
            Ok(())
        }));
    }
    drop(tx);
    let mut producer_error: Option<String> = None;
    for h in handles {
        match h.join() {
            Ok(Ok(())) => {}
            Ok(Err(e)) => producer_error = Some(e),
            Err(_) => producer_error = Some("producer panic".into()),
        }
    }
    let mut report = finish_pipeline(p);
    if let Some(e) = producer_error {
        report["producer_error"] = json!(e);
        report["ok"] = json!(false);
    }
    report["mode"] = json!("bench");
    report["expected"] = json!(total);
    report["rate_target"] = json!(rate);
    report["producers"] = json!(producers);
    report["owner_window"] = json!(owner_window);
    report["linger_us"] = json!(linger_us);
    Ok(report)
}

fn collect_ack(rx: Receiver<OwnerReply>, metrics: &Arc<Mutex<Metrics>>) -> Result<()> {
    match rx.recv_timeout(Duration::from_secs(30)) {
        Ok(reply) => handle_reply(reply, metrics),
        Err(e) => Err(format!("ACK timeout/disconnect: {e}")),
    }
}

fn handle_reply(reply: OwnerReply, metrics: &Arc<Mutex<Metrics>>) -> Result<()> {
    if !reply.ok {
        metrics.lock().unwrap().ack_errors += 1;
        return Err(reply.error.unwrap_or_else(|| "unknown ACK error".into()));
    }
    if let Some(ack) = reply.ack {
        metrics.lock().unwrap().owner_ack_latency_ns.push(stamp().saturating_sub(ack.admitted_ns));
        Ok(())
    } else {
        Err("ACK missing".into())
    }
}

fn run_manual(case: PathBuf, raws: Vec<RawEvent>, opts: PipelineOpts, ack_cap: usize) -> Result<(Value, Vec<OwnerReply>)> {
    let p = start_pipeline(case, opts)?;
    let tx = p.tx.as_ref().unwrap().clone();
    let metrics = p.metrics.clone();
    let mut replies = Vec::new();
    for raw in raws {
        let rx = submit(&tx, &metrics, raw, ack_cap)?;
        let reply = rx.recv_timeout(Duration::from_secs(30)).map_err(|e| e.to_string())?;
        if reply.ok {
            if let Some(ack) = reply.ack.clone() {
                metrics.lock().unwrap().owner_ack_latency_ns.push(stamp().saturating_sub(ack.admitted_ns));
            }
        }
        replies.push(reply);
    }
    drop(tx);
    let report = finish_pipeline(p);
    Ok((report, replies))
}

fn c28_save(path: &Path, value: &Value) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(ioerr)?;
    }
    fs::write(path, serde_json::to_string_pretty(value).map_err(|e| e.to_string())?).map_err(ioerr)
}

fn c28_raws(prefix: &str, count: u64) -> Vec<RawEvent> {
    (1..=count).map(|i| make_raw(prefix, i)).collect()
}

fn c28_check(ok: bool, msg: &str) -> Result<()> {
    if ok { Ok(()) } else { Err(msg.into()) }
}

fn c28_report_ok(report: &Value, expected: u64) -> Result<()> {
    c28_check(report["ok"].as_bool().unwrap_or(false), "report ok=false")?;
    c28_check(report["fault"].is_null(), "report fault not null")?;
    c28_check(report["offered"].as_u64().unwrap_or(0) == expected, "offered mismatch")?;
    c28_check(report["ingress_received"].as_u64().unwrap_or(0) == expected, "ingress mismatch")?;
    c28_check(report["denied"].as_u64().unwrap_or(0) == 0, "denied nonzero")?;
    c28_check(report["queue_full"].as_u64().unwrap_or(0) == 0, "queue_full nonzero")?;
    c28_check(report["ack_errors"].as_u64().unwrap_or(0) == 0, "ack_errors nonzero")
}

fn c28_recovered_exact(case: &Path, raws: &[RawEvent]) -> Result<Value> {
    let registrar = Registrar::open(case.to_path_buf(), String::new())?;
    c28_check(registrar.events.len() == raws.len(), "recovered event count mismatch")?;
    for (i, raw) in raws.iter().enumerate() {
        let expected = make_event(raw, i as u64 + 1, 0)?;
        let actual = &registrar.events[i];
        c28_check(actual.seq == expected.seq, "recovered seq mismatch")?;
        c28_check(actual.event_id == expected.event_id, "recovered event_id mismatch")?;
        c28_check(actual.content_hash == expected.content_hash, "recovered hash mismatch")?;
        c28_check(actual.record_json == expected.record_json, "recovered record mismatch")?;
    }
    Ok(registrar.status_json())
}

fn c28_run_burst(case: PathBuf, raws: Vec<RawEvent>, opts: PipelineOpts, ack_cap: usize) -> Result<(Value, Vec<OwnerReply>)> {
    let p = start_pipeline(case, opts)?;
    let tx = p.tx.as_ref().unwrap().clone();
    let metrics = p.metrics.clone();
    let mut receivers = Vec::new();
    for raw in raws {
        receivers.push(submit(&tx, &metrics, raw, ack_cap)?);
    }
    drop(tx);
    let mut replies = Vec::new();
    for rx in receivers {
        let reply = rx.recv_timeout(Duration::from_secs(30)).map_err(|e| e.to_string())?;
        if reply.ok {
            if let Some(ack) = reply.ack.clone() {
                metrics.lock().unwrap().owner_ack_latency_ns.push(stamp().saturating_sub(ack.admitted_ns));
            }
        }
        replies.push(reply);
    }
    let report = finish_pipeline(p);
    Ok((report, replies))
}

fn c28_ack_seqs(replies: &[OwnerReply]) -> Result<Vec<u64>> {
    replies
        .iter()
        .map(|r| {
            if !r.ok {
                return Err(r.error.clone().unwrap_or_else(|| "reply not ok".into()));
            }
            r.ack.as_ref().map(|a| a.seq).ok_or_else(|| "missing ack".into())
        })
        .collect()
}

fn c28_dummy_durable(through_seq: u64) -> BatchDurable {
    BatchDurable {
        durable_ns: stamp(),
        write_start_ns: 0,
        append_ns: 0,
        journal_fsync_ns: 0,
        floor_sync_ns: 0,
        journal_sync_ns: 0,
        through_seq,
        journal_write_syscalls: 0,
        floor_writes: 0,
        sync_calls: 0,
        journal_bytes_written: 0,
        journal_offset: 0,
        chain_hash: String::new(),
    }
}

fn c28_seed_state(raws: &[RawEvent]) -> Result<(Arc<Mutex<AdmissionState>>, Arc<Mutex<Metrics>>, Vec<Event>)> {
    let mut st = AdmissionState::from_recovered(&[], MAX_EVENTS)?;
    let mut events = Vec::new();
    for raw in raws {
        let admitted_ns = stamp();
        let event = make_event(raw, st.next_seq, admitted_ns)?;
        st.entries.insert(
            event.event_id.clone(),
            IdentityEntry {
                seq: event.seq,
                raw_hash: raw_hash(&raw.external_id, &raw.name, &raw.payload_json),
                content_hash: event.content_hash.clone(),
                admitted_ns,
                durable_ack: None,
                waiters: Vec::new(),
            },
        );
        st.next_seq += 1;
        events.push(event);
    }
    Ok((Arc::new(Mutex::new(st)), Arc::new(Mutex::new(Metrics::default())), events))
}

fn c28_frontier_gap(case: &Path, prefix: &str, total: u64, mode: u8) -> Result<Value> {
    fs::create_dir_all(case).map_err(ioerr)?;
    let raws = c28_raws(prefix, total);
    let (state, metrics, events) = c28_seed_state(&raws)?;
    let first_err = if mode == 2 {
        complete_batch(&state, &metrics, &events[32..64], &c28_dummy_durable(64), 2).err()
    } else {
        complete_batch(&state, &metrics, &events[64..96], &c28_dummy_durable(96), 3).err()
    };
    c28_check(first_err.as_ref().map(|e| e.contains("frontier gap")).unwrap_or(false), "successor completion did not fail on frontier gap")?;
    c28_check(state.lock().unwrap().durable_frontier == 0, "frontier advanced across gap")?;
    complete_batch(&state, &metrics, &events[0..32], &c28_dummy_durable(32), 1)?;
    if mode == 3 {
        let second_err = complete_batch(&state, &metrics, &events[64..96], &c28_dummy_durable(96), 3).err();
        c28_check(second_err.as_ref().map(|e| e.contains("frontier gap")).unwrap_or(false), "second successor completion did not fail on frontier gap")?;
    }
    complete_batch(&state, &metrics, &events[32..64], &c28_dummy_durable(64), 2)?;
    if mode == 3 {
        complete_batch(&state, &metrics, &events[64..96], &c28_dummy_durable(96), 3)?;
    }
    let frontier = state.lock().unwrap().durable_frontier;
    Ok(json!({"gap_error":first_err,"final_frontier":frontier,"mode":mode,"case_dir":case}))
}

fn c28_spawn_hold(case: &Path, failpoint: &str, prefix: &str, count: u64) -> Result<Value> {
    let exe = std::env::current_exe().map_err(ioerr)?;
    let mut child = std::process::Command::new(exe)
        .arg("hold_failpoint")
        .arg(case)
        .arg(failpoint)
        .arg(prefix)
        .arg(count.to_string())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .map_err(ioerr)?;
    let phase = case.join("registrar_phase.json");
    let deadline = Instant::now() + Duration::from_secs(10);
    while !phase.exists() {
        if Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err("phase wait timeout".into());
        }
        thread::sleep(Duration::from_millis(5));
    }
    child.kill().map_err(ioerr)?;
    let status = child.wait().map_err(ioerr)?;
    let phase_json: Value = serde_json::from_str(&fs::read_to_string(&phase).map_err(ioerr)?).map_err(|e| e.to_string())?;
    Ok(json!({"killed":true,"exit":status.code(),"phase":phase_json}))
}

fn c28_kill_before_write(case: &Path, prefix: &str) -> Result<Value> {
    let raws = c28_raws(prefix, 96);
    let killed = c28_spawn_hold(case, "before_write", prefix, 96)?;
    let before = Registrar::open(case.to_path_buf(), String::new())?.status_json();
    c28_check(before["through_seq"].as_u64().unwrap_or(1) == 0, "pre-write crash recovered durable events")?;
    let (report, replies) = c28_run_burst(case.to_path_buf(), raws.clone(), PipelineOpts::default(), 1)?;
    c28_report_ok(&report, 96)?;
    c28_check(c28_ack_seqs(&replies)? == (1..=96).collect::<Vec<u64>>(), "replay ack order mismatch")?;
    let recovery = c28_recovered_exact(case, &raws)?;
    Ok(json!({"killed":killed,"before_replay":before,"report":report,"recovery":recovery}))
}

fn c28_direct_durable_no_ack(case: &Path) -> Result<Value> {
    let raws = c28_raws("f05", 32);
    let mut registrar = Registrar::open(case.to_path_buf(), String::new())?;
    let events: Vec<Event> = raws.iter().enumerate().map(|(i, raw)| make_event(raw, i as u64 + 1, stamp())).collect::<Result<Vec<_>>>()?;
    let durable = registrar.batch(events)?;
    let before = registrar.status_json();
    drop(registrar);
    let recovered = c28_recovered_exact(case, &raws)?;
    let before_offset = recovered["journal_offset"].as_u64().unwrap_or(0);
    let (report, replies) = c28_run_burst(case.to_path_buf(), raws.clone(), PipelineOpts::default(), 1)?;
    c28_check(report["ok"].as_bool().unwrap_or(false), "duplicate replay report failed")?;
    c28_check(report["admitted_new"].as_u64().unwrap_or(1) == 0, "duplicate replay admitted new events")?;
    c28_check(report["duplicate_exact"].as_u64().unwrap_or(0) == 32, "duplicate replay count mismatch")?;
    for (i, reply) in replies.iter().enumerate() {
        let ack = reply.ack.as_ref().ok_or("missing duplicate ack")?;
        c28_check(reply.ok && ack.duplicate && ack.seq == i as u64 + 1, "duplicate ack mismatch")?;
    }
    let after = Registrar::open(case.to_path_buf(), String::new())?.status_json();
    c28_check(after["journal_offset"].as_u64().unwrap_or(1) == before_offset, "duplicate replay appended journal bytes")?;
    Ok(json!({"durable":durable.through_seq,"before":before,"recovered":recovered,"replay":report,"after":after}))
}

fn c28_failpoint_then_replay(case: &Path, failpoint: &str, prefix: &str) -> Result<Value> {
    let mut opts = PipelineOpts::default();
    opts.failpoint = failpoint.into();
    let first = make_raw(prefix, 1);
    let (failure_report, failure_replies) = run_manual(case.to_path_buf(), vec![first], opts, 1)?;
    c28_check(!failure_replies.first().map(|r| r.ok).unwrap_or(true), "failure produced ok ACK")?;
    let recovered_before = Registrar::open(case.to_path_buf(), String::new()).map(|r| r.status_json()).unwrap_or_else(|e| json!({"ok":false,"error":e}));
    let raws = c28_raws(prefix, 96);
    let replay = if recovered_before["ok"].as_bool().unwrap_or(false) {
        let (report, replies) = c28_run_burst(case.to_path_buf(), raws.clone(), PipelineOpts::default(), 1)?;
        c28_check(c28_ack_seqs(&replies)? == (1..=96).collect::<Vec<u64>>(), "replay seq mismatch")?;
        let recovery = c28_recovered_exact(case, &raws)?;
        json!({"report":report,"recovery":recovery})
    } else {
        json!({"fail_closed":true})
    };
    Ok(json!({"failpoint":failpoint,"failure_report":failure_report,"failure_replies":failure_replies,"recovered_before":recovered_before,"replay":replay}))
}

fn c28_floor_matrix(case: &Path) -> Result<Value> {
    let sync_case = case.join("floor_sync_error");
    let sync = c28_failpoint_then_replay(&sync_case, "floor_sync_error", "f08sync")?;
    let corrupt_case = case.join("corrupt_floor");
    let raws = c28_raws("f08corrupt", 1);
    let (report, _) = c28_run_burst(corrupt_case.clone(), raws, PipelineOpts::default(), 1)?;
    c28_check(report["ok"].as_bool().unwrap_or(false), "corrupt setup write failed")?;
    let floor_path = storage_path(&fs::canonicalize(&corrupt_case).map_err(ioerr)?, "NATIVE_C27_FLOOR_PATH", "NATIVE_C27_FLOOR_ROOT", "NATIVE_SPLIT_FLOOR_PATH", "commit_floor.bin");
    let mut f = OpenOptions::new().write(true).open(&floor_path).map_err(ioerr)?;
    f.seek(SeekFrom::Start(0)).map_err(ioerr)?;
    f.write_all(b"BADFLOOR").map_err(ioerr)?;
    f.sync_all().map_err(ioerr)?;
    let corrupt = Registrar::open(corrupt_case, String::new()).err().unwrap_or_else(|| "corrupt floor accepted".into());
    c28_check(corrupt != "corrupt floor accepted", "corrupt floor accepted")?;
    let stale_case = case.join("stale_floor");
    let stale_raws = c28_raws("f08stale", 32);
    let (stale_report, _) = c28_run_burst(stale_case.clone(), stale_raws.clone(), PipelineOpts::default(), 1)?;
    c28_check(stale_report["ok"].as_bool().unwrap_or(false), "stale setup write failed")?;
    let stale_floor_path = storage_path(&fs::canonicalize(&stale_case).map_err(ioerr)?, "NATIVE_C27_FLOOR_PATH", "NATIVE_C27_FLOOR_ROOT", "NATIVE_SPLIT_FLOOR_PATH", "commit_floor.bin");
    let mut floor_file = OpenOptions::new().read(true).write(true).open(&stale_floor_path).map_err(ioerr)?;
    save_floor(&mut floor_file, &Floor { seq: 0, offset: 0, chain: [0; 32] })?;
    let stale_recovery = c28_recovered_exact(&stale_case, &stale_raws)?;
    Ok(json!({"floor_sync_error":sync,"corrupt_floor_error":corrupt,"stale_floor_recovery":stale_recovery}))
}

fn c28_ack_disconnect(case: &Path, cap: usize, label: &str) -> Result<Value> {
    let p = start_pipeline(case.to_path_buf(), PipelineOpts::default())?;
    let tx = p.tx.as_ref().unwrap().clone();
    let metrics = p.metrics.clone();
    let rx = submit(&tx, &metrics, make_raw(label, 1), cap)?;
    drop(rx);
    drop(tx);
    let report = finish_pipeline(p);
    c28_check(!report["ok"].as_bool().unwrap_or(true), "ACK channel failure did not fail closed")?;
    c28_check(report["fault"].as_str().unwrap_or("").contains("ACK consumer"), "ACK failure reason mismatch")?;
    let recovery = Registrar::open(case.to_path_buf(), String::new())?.status_json();
    Ok(json!({"report":report,"recovery":recovery,"ack_cap":cap}))
}

fn c28_generation_duplicate(case: &Path) -> Result<Value> {
    let raw = make_raw("f12", 1);
    let (first, _) = c28_run_burst(case.to_path_buf(), vec![raw.clone()], PipelineOpts::default(), 1)?;
    c28_check(first["ok"].as_bool().unwrap_or(false), "initial write failed")?;
    let before = Registrar::open(case.to_path_buf(), String::new())?.status_json();
    let (dup_report, dup_replies) = run_manual(case.to_path_buf(), vec![raw], PipelineOpts::default(), 1)?;
    let ack = dup_replies.first().and_then(|r| r.ack.as_ref()).ok_or("missing duplicate ack")?;
    c28_check(ack.duplicate && ack.seq == 1, "exact duplicate not replay-safe")?;
    let after = Registrar::open(case.to_path_buf(), String::new())?.status_json();
    c28_check(before["journal_offset"] == after["journal_offset"], "duplicate changed journal")?;
    Ok(json!({"before":before,"duplicate_report":dup_report,"after":after}))
}

fn c28_epoch_conflict(case: &Path) -> Result<Value> {
    let raw = make_raw("f13", 1);
    let (first, _) = c28_run_burst(case.to_path_buf(), vec![raw.clone()], PipelineOpts::default(), 1)?;
    c28_check(first["ok"].as_bool().unwrap_or(false), "initial write failed")?;
    let mut bad = raw;
    bad.payload_json = json!({"source":"c27","changed_epoch":true}).to_string();
    let (conflict_report, replies) = run_manual(case.to_path_buf(), vec![bad], PipelineOpts::default(), 1)?;
    c28_check(!replies.first().map(|r| r.ok).unwrap_or(true), "conflict replay accepted")?;
    c28_check(conflict_report["fault"].as_str().unwrap_or("").contains("conflicting"), "conflict did not fail closed")?;
    let recovery = Registrar::open(case.to_path_buf(), String::new())?.status_json();
    c28_check(recovery["through_seq"].as_u64().unwrap_or(0) == 1, "conflict changed durable frontier")?;
    Ok(json!({"conflict_report":conflict_report,"recovery":recovery}))
}

fn c28_saturation(case: &Path) -> Result<Value> {
    fs::create_dir_all(case).map_err(ioerr)?;
    let state = Arc::new(Mutex::new(AdmissionState::from_recovered(&[], 4)?));
    let metrics = Arc::new(Mutex::new(Metrics::default()));
    let (ingress_tx, ingress_rx) = sync_channel::<IngressRequest>(2);
    let (staging_tx, staging_rx) = sync_channel::<Event>(16);
    let admission = start_admission(ingress_rx, staging_tx, state.clone(), metrics.clone());
    let mut denied = 0;
    let mut accepted_waiters = 0;
    for raw in c28_raws("f14", 8) {
        let rx = submit(&ingress_tx, &metrics, raw, 1)?;
        match rx.recv_timeout(Duration::from_millis(50)) {
            Ok(reply) if !reply.ok => denied += 1,
            Ok(reply) if reply.ok => return Err(format!("unexpected ok saturation reply: {}", serde_json::to_string(&reply).unwrap_or_default())),
            Ok(_) => return Err("unexpected saturation reply".into()),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => accepted_waiters += 1,
            Err(e) => return Err(e.to_string()),
        }
    }
    drop(ingress_tx);
    let _ = admission.join();
    drop(staging_rx);
    let m = metrics.lock().unwrap();
    c28_check(accepted_waiters == 4 && denied == 4, "pending cap saturation counts mismatch")?;
    c28_check(m.pending_depth_peak == 4 && m.denied == 4, "pending cap metrics mismatch")?;
    let (probe_tx, _probe_rx) = sync_channel::<IngressRequest>(1);
    let (ack_tx, _ack_rx) = sync_channel::<OwnerReply>(1);
    let full1 = probe_tx.try_send(IngressRequest { raw: make_raw("f14q", 1), offered_ns: stamp(), ack_tx: ack_tx.clone() }).is_ok();
    let full2 = probe_tx.try_send(IngressRequest { raw: make_raw("f14q", 2), offered_ns: stamp(), ack_tx }).is_err();
    c28_check(full1 && full2, "bounded ingress try_send did not saturate")?;
    Ok(json!({"accepted_waiters":accepted_waiters,"denied":denied,"pending_depth_peak":m.pending_depth_peak,"staging_depth_peak":m.staging_depth_peak,"bounded_try_send_full":full2}))
}

fn c28_duplicate(case: &Path) -> Result<Value> {
    let raw = make_raw("f15", 1);
    let (r1, a1) = run_manual(case.to_path_buf(), vec![raw.clone(), raw.clone()], PipelineOpts::default(), 1)?;
    c28_check(a1.len() == 2 && a1[0].ok && a1[1].ok, "duplicate replies not ok")?;
    c28_check(a1[0].ack.as_ref().unwrap().seq == a1[1].ack.as_ref().unwrap().seq, "duplicate seq mismatch")?;
    c28_check(a1[1].ack.as_ref().unwrap().duplicate, "post-durable duplicate flag missing")?;
    let before = Registrar::open(case.to_path_buf(), String::new())?.status_json();
    let (r2, a2) = run_manual(case.to_path_buf(), vec![raw], PipelineOpts::default(), 1)?;
    c28_check(a2[0].ack.as_ref().unwrap().duplicate, "restart duplicate flag missing")?;
    let after = Registrar::open(case.to_path_buf(), String::new())?.status_json();
    c28_check(before["journal_offset"] == after["journal_offset"], "restart duplicate appended journal")?;
    Ok(json!({"first":r1,"second":r2,"before":before,"after":after}))
}

fn c28_conflict(case: &Path) -> Result<Value> {
    let raw = make_raw("f16", 1);
    let mut bad = raw.clone();
    bad.payload_json = json!({"source":"c27","conflict":true}).to_string();
    let (report, replies) = run_manual(case.to_path_buf(), vec![raw, bad], PipelineOpts::default(), 1)?;
    c28_check(replies.len() == 2 && replies[0].ok && !replies[1].ok, "conflicting replay result mismatch")?;
    c28_check(report["fault"].as_str().unwrap_or("").contains("conflicting"), "conflict did not set fault")?;
    let recovery = Registrar::open(case.to_path_buf(), String::new())?.status_json();
    c28_check(recovery["through_seq"].as_u64().unwrap_or(0) == 1, "conflict changed durable history")?;
    let raws = c28_raws("f16x", 2);
    let (state, metrics, events) = c28_seed_state(&raws)?;
    let unknown = make_event(&make_raw("unknown", 1), 3, stamp())?;
    let err = complete_batch(&state, &metrics, &[unknown], &c28_dummy_durable(3), 1).err();
    c28_check(err.as_ref().map(|e| e.contains("frontier gap") || e.contains("missing")).unwrap_or(false), "unmatched completion accepted")?;
    complete_batch(&state, &metrics, &events[0..2], &c28_dummy_durable(2), 1)?;
    Ok(json!({"report":report,"recovery":recovery,"unmatched_completion_error":err}))
}

fn c28_restart_identities(case: &Path) -> Result<Value> {
    let raws = c28_raws("f17", 128);
    let (report, replies) = c28_run_burst(case.to_path_buf(), raws.clone(), PipelineOpts::default(), 1)?;
    c28_report_ok(&report, 128)?;
    c28_check(report["physical_groups"].as_u64().unwrap_or(0) >= 4, "not enough physical groups for multi-batch identity test")?;
    c28_check(c28_ack_seqs(&replies)? == (1..=128).collect::<Vec<u64>>(), "initial ACK seq mismatch")?;
    let recovered = c28_recovered_exact(case, &raws)?;
    let (dup_report, dup_replies) = c28_run_burst(case.to_path_buf(), raws.clone(), PipelineOpts::default(), 1)?;
    c28_check(dup_report["admitted_new"].as_u64().unwrap_or(1) == 0, "duplicate replay admitted new events")?;
    c28_check(dup_replies.iter().all(|r| r.ack.as_ref().map(|a| a.duplicate).unwrap_or(false)), "not all replay ACKs duplicate")?;
    let continuation = c28_raws("f17b", 5);
    let (cont_report, cont_replies) = c28_run_burst(case.to_path_buf(), continuation.clone(), PipelineOpts::default(), 1)?;
    c28_check(c28_ack_seqs(&cont_replies)? == (129..=133).collect::<Vec<u64>>(), "continuation seq mismatch")?;
    let mut all = raws;
    all.extend(continuation);
    let final_recovery = c28_recovered_exact(case, &all)?;
    Ok(json!({"initial":report,"recovered":recovered,"duplicate_replay":dup_report,"continuation":cont_report,"final_recovery":final_recovery}))
}

fn run_hold_failpoint(case: PathBuf, failpoint: String, prefix: String, count: u64) -> Result<Value> {
    let mut opts = PipelineOpts::default();
    opts.failpoint = failpoint;
    let raws = c28_raws(&prefix, count);
    let (report, replies) = run_manual(case, raws, opts, 1)?;
    Ok(json!({"report":report,"replies":replies}))
}

fn run_c28_case(index: usize, case: &Path) -> Result<Value> {
    match index {
        1 => {
            let raws = c28_raws("f01", 128);
            let (report, replies) = c28_run_burst(case.to_path_buf(), raws.clone(), PipelineOpts::default(), 1)?;
            c28_report_ok(&report, 128)?;
            c28_check(c28_ack_seqs(&replies)? == (1..=128).collect::<Vec<u64>>(), "F01 ACK seq mismatch")?;
            let recovery = c28_recovered_exact(case, &raws)?;
            Ok(json!({"report":report,"recovery":recovery,"ack_count":replies.len()}))
        }
        2 => c28_frontier_gap(case, "f02", 64, 2),
        3 => c28_frontier_gap(case, "f03", 96, 3),
        4 => c28_kill_before_write(case, "f04"),
        5 => c28_direct_durable_no_ack(case),
        6 => c28_failpoint_then_replay(case, "partial_write_error", "f06"),
        7 => c28_failpoint_then_replay(case, "journal_sync_error", "f07"),
        8 => c28_floor_matrix(case),
        9 => c28_kill_before_write(case, "f09"),
        10 => c28_ack_disconnect(case, 1, "f10"),
        11 => c28_ack_disconnect(case, 0, "f11"),
        12 => c28_generation_duplicate(case),
        13 => c28_epoch_conflict(case),
        14 => c28_saturation(case),
        15 => c28_duplicate(case),
        16 => c28_conflict(case),
        17 => c28_restart_identities(case),
        _ => Err("unknown C28 case".into()),
    }
}

fn run_c28_cert(root: PathBuf) -> Result<Value> {
    fs::create_dir_all(&root).map_err(ioerr)?;
    let names = vec![
        "normal_ordered",
        "N1_completion_before_N",
        "N2_completion_before_N_N1",
        "registrar_death_waiting",
        "death_after_physical_before_release",
        "partial_journal_write",
        "journal_fsync_error",
        "floor_error",
        "assembler_death",
        "coordinator_death",
        "owner_cohort_invalidation",
        "generation_transition",
        "venue_epoch_transition",
        "P_limit_saturation",
        "duplicate_completion_event_replay",
        "conflicting_replay",
        "restart_multiple_identities",
    ];
    let mut cases: Vec<Value> = names
        .iter()
        .enumerate()
        .map(|(i, name)| json!({"name":name,"case":format!("failures/F{:02}", i + 1),"status":"NOT_RUN"}))
        .collect();
    let mut matrix = json!({"pass":false,"stop_on_first_correctness_failure":true,"cases":cases});
    c28_save(&root.join("failure_matrix.json"), &matrix)?;
    for i in 1..=17 {
        let case = root.join(format!("failures/F{:02}", i));
        let stage = json!({"stage":"C28_F01_F17","case":names[i-1],"at_ns":stamp()});
        c28_save(&root.join("current_stage.json"), &stage)?;
        match run_c28_case(i, &case) {
            Ok(detail) => {
                cases[i - 1] = json!({"name":names[i-1],"case":format!("failures/F{:02}", i),"status":"PASS","detail":detail});
                matrix = json!({"pass":false,"stop_on_first_correctness_failure":true,"cases":cases});
                c28_save(&case.join("targeted_result.json"), &cases[i - 1])?;
                c28_save(&root.join("failure_matrix.json"), &matrix)?;
                println!("C28_TARGETED_PASS F{:02} {}", i, names[i - 1]);
            }
            Err(error) => {
                cases[i - 1] = json!({"name":names[i-1],"case":format!("failures/F{:02}", i),"status":"FAIL","error":error});
                matrix = json!({"pass":false,"stop_on_first_correctness_failure":true,"cases":cases});
                c28_save(&case.join("targeted_result.json"), &cases[i - 1])?;
                c28_save(&root.join("failure_matrix.json"), &matrix)?;
                c28_save(&root.join("STOP_correctness.json"), &json!({"case":names[i-1],"error":error,"at_ns":stamp()}))?;
                return Err(format!("C28 F{:02} {} FAIL: {}", i, names[i - 1], error));
            }
        }
    }
    let mb_case = root.join("multi_batch_pipeline");
    let mb = c28_restart_identities(&mb_case)?;
    c28_save(&root.join("MULTI_BATCH_PIPELINE_result.json"), &json!({"status":"PASS","detail":mb}))?;
    matrix = json!({"pass":true,"stop_on_first_correctness_failure":true,"cases":cases,"multi_batch_pipeline":"PASS"});
    c28_save(&root.join("failure_matrix.json"), &matrix)?;
    println!("C28_F01_F17_PASS 17");
    println!("MULTI_BATCH_PIPELINE_PASS");
    Ok(matrix)
}

fn expect(name: &str, ok: bool, detail: Value, results: &mut Vec<Value>) -> bool {
    results.push(json!({"test":name,"pass":ok,"detail":detail}));
    ok
}

fn run_selftest(root: PathBuf) -> Result<Value> {
    fs::create_dir_all(&root).map_err(ioerr)?;
    let mut results = Vec::new();
    let opts = PipelineOpts::default();

    let raws: Vec<RawEvent> = (1..=12).map(|i| make_raw("t1", i)).collect();
    let (r1, a1) = run_manual(root.join("t1_identity"), raws.clone(), opts.clone(), 1)?;
    let t1 = r1["ok"].as_bool().unwrap_or(false)
        && a1.iter().enumerate().all(|(i, r)| r.ack.as_ref().map(|a| a.seq == i as u64 + 1).unwrap_or(false));
    expect("T1 Identity", t1, json!({"report":r1,"acks":a1}), &mut results);

    let raw = make_raw("t2", 1);
    let (r2, a2) = run_manual(root.join("t2_duplicate"), vec![raw.clone(), raw], opts.clone(), 1)?;
    let t2 = a2.len() == 2
        && a2[0].ok
        && a2[1].ok
        && a2[0].ack.as_ref().unwrap().seq == a2[1].ack.as_ref().unwrap().seq
        && a2[1].ack.as_ref().unwrap().duplicate;
    expect("T2 Duplicate", t2, json!({"report":r2,"acks":a2}), &mut results);

    let mut c1 = make_raw("t3", 1);
    c1.external_id = "same-id".into();
    let mut c2 = c1.clone();
    c2.payload_json = json!({"source":"c27","n":999}).to_string();
    let (r3, a3) = run_manual(root.join("t3_conflict"), vec![c1, c2], opts.clone(), 1)?;
    let t3 = a3.len() == 2 && a3[0].ok && !a3[1].ok && r3["fault"].is_string();
    expect("T3 Conflict", t3, json!({"report":r3,"acks":a3}), &mut results);

    let mut oversize = make_raw("t4", 1);
    oversize.payload_json = "x".repeat(70_000);
    let (r4, a4) = run_manual(root.join("t4_bounds"), vec![oversize], opts.clone(), 1)?;
    let t4 = a4.len() == 1 && !a4[0].ok && r4["denied"].as_u64().unwrap_or(0) == 1;
    expect("T4 Bounds", t4, json!({"report":r4,"acks":a4}), &mut results);

    let raws5: Vec<RawEvent> = (1..=128).map(|i| make_raw("t5", i)).collect();
    let (r5, a5) = run_manual(root.join("t5_ordered_ack"), raws5, opts.clone(), 1)?;
    let t5 = r5["ok"].as_bool().unwrap_or(false)
        && a5.iter().enumerate().all(|(i, r)| r.ack.as_ref().map(|a| a.seq == i as u64 + 1 && a.through_seq >= a.seq).unwrap_or(false));
    expect("T5 Ordered ACK", t5, json!({"report":r5}), &mut results);

    let hist_case = root.join("t6_ack_history");
    let hraws: Vec<RawEvent> = (1..=16).map(|i| make_raw("t6", i)).collect();
    let (r6a, a6a) = run_manual(hist_case.clone(), hraws.clone(), opts.clone(), 1)?;
    let (r6b, a6b) = run_manual(hist_case.clone(), vec![hraws[0].clone()], opts.clone(), 1)?;
    let mut conflict = hraws[0].clone();
    conflict.payload_json = json!({"source":"c27","changed":true}).to_string();
    let (r6c, a6c) = run_manual(hist_case, vec![conflict], opts.clone(), 1)?;
    let t6 = r6a["ok"].as_bool().unwrap_or(false)
        && a6a.len() == 16
        && a6b[0].ok
        && a6b[0].ack.as_ref().unwrap().seq == 1
        && a6b[0].ack.as_ref().unwrap().duplicate
        && !a6c[0].ok
        && r6c["fault"].is_string();
    expect("T6 ACK history", t6, json!({"initial":r6a,"duplicate_after_restart":r6b,"conflict_after_restart":r6c}), &mut results);

    let crash_case = root.join("t7_partial_recovery");
    let mut fail_opts = opts.clone();
    fail_opts.failpoint = "partial_write_error".into();
    let partial = run_manual(crash_case.clone(), vec![make_raw("t7", 1)], fail_opts, 1);
    let recovered = Registrar::open(crash_case.clone(), String::new());
    let t7 = partial.is_ok() && recovered.as_ref().map(|r| r.events.is_empty()).unwrap_or(false);
    expect("T7 Crash recovery", t7, json!({"partial_result_ok":partial.is_ok(),"recovered_ok":recovered.is_ok()}), &mut results);

    let corrupt_case = root.join("t8_corruption");
    let (r8a, _) = run_manual(corrupt_case.clone(), vec![make_raw("t8", 1)], opts.clone(), 1)?;
    let floor_path = storage_path(&fs::canonicalize(&corrupt_case).map_err(ioerr)?, "NATIVE_C27_FLOOR_PATH", "NATIVE_C27_FLOOR_ROOT", "NATIVE_SPLIT_FLOOR_PATH", "commit_floor.bin");
    let mut f = OpenOptions::new().write(true).open(&floor_path).map_err(ioerr)?;
    f.seek(SeekFrom::Start(10)).map_err(ioerr)?;
    f.write_all(b"bad").map_err(ioerr)?;
    f.sync_all().map_err(ioerr)?;
    let corrupt = Registrar::open(corrupt_case, String::new());
    let t8 = r8a["ok"].as_bool().unwrap_or(false) && corrupt.is_err();
    expect("T8 Journal/floor corruption", t8, json!({"open_error":corrupt.err()}), &mut results);

    let (sat_tx, _sat_rx) = sync_channel::<IngressRequest>(1);
    let (ack_tx, _ack_rx) = sync_channel::<OwnerReply>(1);
    let sat1 = sat_tx.try_send(IngressRequest { raw: make_raw("t9", 1), offered_ns: stamp(), ack_tx: ack_tx.clone() }).is_ok();
    let sat2 = sat_tx.try_send(IngressRequest { raw: make_raw("t9", 2), offered_ns: stamp(), ack_tx }).is_err();
    expect("T9 Saturation", sat1 && sat2, json!({"bounded_try_send_full":sat2}), &mut results);

    let restart_case = root.join("t10_clean_restart");
    let (r10a, _) = run_manual(restart_case.clone(), (1..=10).map(|i| make_raw("t10a", i)).collect(), opts.clone(), 1)?;
    let (r10b, a10b) = run_manual(restart_case, (1..=5).map(|i| make_raw("t10b", i)).collect(), opts, 1)?;
    let t10 = r10a["ok"].as_bool().unwrap_or(false)
        && r10b["ok"].as_bool().unwrap_or(false)
        && a10b.first().and_then(|r| r.ack.as_ref()).map(|a| a.seq == 11).unwrap_or(false)
        && a10b.last().and_then(|r| r.ack.as_ref()).map(|a| a.seq == 15).unwrap_or(false);
    expect("T10 Clean restart", t10, json!({"first":r10a,"second":r10b,"acks":a10b}), &mut results);

    let pass = results.iter().all(|r| r["pass"].as_bool().unwrap_or(false));
    Ok(json!({"ok":pass,"mode":"selftest","verdict":if pass {"PASS"} else {"FAIL"},"results":results}))
}

fn parse_arg(args: &[String], name: &str, default: &str) -> String {
    args.windows(2).find(|w| w[0] == name).map(|w| w[1].clone()).unwrap_or_else(|| default.to_string())
}

fn run() -> Result<Value> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        return Err("usage: native_hot_path_c27 selftest ROOT | c28_cert ROOT | hold_failpoint CASE FAILPOINT PREFIX COUNT | bench CASE [--events N --rate R --producers N --owner-window N] | status CASE".into());
    }
    match args[1].as_str() {
        "selftest" => run_selftest(PathBuf::from(&args[2])),
        "c28_cert" => run_c28_cert(PathBuf::from(&args[2])),
        "hold_failpoint" => {
            if args.len() != 6 {
                return Err("usage: native_hot_path_c27 hold_failpoint CASE FAILPOINT PREFIX COUNT".into());
            }
            let count = args[5].parse::<u64>().map_err(|e| e.to_string())?;
            run_hold_failpoint(PathBuf::from(&args[2]), args[3].clone(), args[4].clone(), count)
        },
        "bench" => {
            let events = parse_arg(&args, "--events", "3000").parse::<u64>().map_err(|e| e.to_string())?;
            let rate = parse_arg(&args, "--rate", "600").parse::<f64>().map_err(|e| e.to_string())?;
            let producers = parse_arg(&args, "--producers", "3").parse::<usize>().map_err(|e| e.to_string())?;
            let owner_window = parse_arg(&args, "--owner-window", "64").parse::<usize>().map_err(|e| e.to_string())?;
            let opts = PipelineOpts {
                ingress_cap: parse_arg(&args, "--ingress-cap", "1024").parse::<usize>().map_err(|e| e.to_string())?,
                staging_cap: parse_arg(&args, "--staging-cap", "1024").parse::<usize>().map_err(|e| e.to_string())?,
                pending_cap: parse_arg(&args, "--pending-cap", "65536").parse::<usize>().map_err(|e| e.to_string())?,
                max_batch: parse_arg(&args, "--batch", "32").parse::<usize>().map_err(|e| e.to_string())?,
                linger_us: parse_arg(&args, "--linger-us", "0").parse::<u64>().map_err(|e| e.to_string())?,
                failpoint: parse_arg(&args, "--failpoint", ""),
            };
            run_bench(PathBuf::from(&args[2]), events, rate, producers, owner_window, opts)
        }
        "status" => {
            let r = Registrar::open(PathBuf::from(&args[2]), String::new())?;
            Ok(json!({"ok":true,"mode":"status","status":r.status_json()}))
        }
        _ => Err("unknown command".into()),
    }
}

fn main() {
    match run() {
        Ok(value) => println!("{}", serde_json::to_string_pretty(&value).unwrap()),
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(2);
        }
    }
}
