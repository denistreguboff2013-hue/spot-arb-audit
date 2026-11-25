//! Offline Astra P0-02 process-boundary regression (no network or exchange).
//! Separate real PIDs exercise CAS, lost ACK, orphaned lock and torn journal.
//! Loss-of-ACK is AFTER successful file+directory fsync and BEFORE caller response;
//! this does NOT simulate a power failure inside fsync or certify an off-host floor.
use spotarb_native_market_core::{
    native_paper_owner_transaction_review::{OwnerError, PaperOwner},
    native_paper_portfolio_review::PaperPortfolio,
};
use std::{
    collections::BTreeMap,
    env, fs::{self, OpenOptions}, io::Write,
    path::{Path, PathBuf}, process::{Command, Stdio},
    time::{SystemTime, UNIX_EPOCH},
};
const CLOCK_MS: i64 = 1_791_786_000_000;

fn journal(tag: &str) -> PathBuf {
    let ns = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let root = env::var_os("SPOTARB_PROCESS_CAS_EVIDENCE_DIR")
        .map(PathBuf::from).unwrap_or_else(env::temp_dir);
    let dir = root.join(format!("cas-pid-{}-{tag}-{ns}", std::process::id()));
    fs::create_dir_all(&dir).unwrap();
    dir.join("state.jsonl")
}
fn create(path: &Path) -> PaperOwner {
    PaperOwner::start_new(path, CLOCK_MS).unwrap()
}
fn cloned_initial_owner(path: &Path) -> PaperOwner {
    // This is an intentionally STALE in-RAM owner from the same journal parent.
    // It is never reconstructed from the quarantined recovery view.
    PaperOwner {
        portfolio: PaperPortfolio::new(CLOCK_MS).unwrap(),
        gate_by_base: BTreeMap::new(),
        journal: path.to_owned(), last_seq: 1, frozen: false,
    }
}
fn increment(o: &mut PaperOwner) -> Result<u64, OwnerError> {
    o.transact(|p, _| { p.stats.entries += 1; Ok(()) })
}
fn run_child(mode: &str, path: &Path) -> std::process::ExitStatus {
    Command::new(env::current_exe().unwrap())
        .arg("--exact").arg("subprocess_worker").arg("--nocapture")
        .env("SPOTARB_CAS_CHILD_MODE", mode)
        .env("SPOTARB_CAS_CHILD_JOURNAL", path)
        .stdout(Stdio::null()).stderr(Stdio::null())
        .status().unwrap()
}
fn spawn_child(mode: &str, path: &Path) -> std::process::Child {
    Command::new(env::current_exe().unwrap())
        .arg("--exact").arg("subprocess_worker").arg("--nocapture")
        .env("SPOTARB_CAS_CHILD_MODE", mode)
        .env("SPOTARB_CAS_CHILD_JOURNAL", path)
        .stdout(Stdio::null()).stderr(Stdio::null())
        .spawn().unwrap()
}
fn inspect(path: &Path, seq: u64, entries: u64) {
    let restored = PaperOwner::inspect_recovery(path).unwrap().unwrap();
    assert_eq!(restored.sequence, seq);
    assert_eq!(restored.state.entries, entries);
    assert!(!restored.may_resume_paper);
    assert!(!restored.may_trade_real);
}

// Only child subprocesses set this environment variable.
#[test]
fn subprocess_worker() {
    let Ok(mode) = env::var("SPOTARB_CAS_CHILD_MODE") else { return };
    let file = PathBuf::from(env::var_os("SPOTARB_CAS_CHILD_JOURNAL").unwrap());
    match mode.as_str() {
        "compete" => {
            let mut owner = cloned_initial_owner(&file);
            let ok = increment(&mut owner).is_ok();
            if ok {
                assert_eq!(owner.last_seq, 2);
                assert!(!owner.frozen);
                std::process::exit(0);
            }
            assert!(owner.frozen, "CAS/lock rejection must quarantine stale owner");
            std::process::exit(11);
        }
        "die_after_durable_without_ack" => {
            let mut owner = cloned_initial_owner(&file);
            assert_eq!(increment(&mut owner).unwrap(), 2);
            // Process dies without sending its result to the caller.
            // The library's append+file fsync+directory fsync already completed.
            std::process::exit(78);
        }
        "die_while_lock_present" => {
            let lock = file.with_extension("writer_lock");
            let mut f = OpenOptions::new().create_new(true).write(true).open(lock).unwrap();
            f.write_all(b"orphaned-synthetic-writer-lock").unwrap();
            f.sync_all().unwrap();
            std::process::exit(77);
        }
        "die_with_torn_tail" => {
            let mut f = OpenOptions::new().append(true).open(file).unwrap();
            f.write_all(b"{\"incomplete\":true").unwrap(); // no newline
            f.sync_all().unwrap();
            std::process::exit(79);
        }
        _ => panic!("unrecognized child mode"),
    }
}
#[test]
fn competing_independent_pids_have_one_cas_winner() {
    let path = journal("two-writers");
    let _parent = create(&path);
    let a = spawn_child("compete", &path);
    let b = spawn_child("compete", &path);
    let x = a.wait_with_output().unwrap().status.code();
    let y = b.wait_with_output().unwrap().status.code();
    assert!((x == Some(0) && y == Some(11)) ||
            (x == Some(11) && y == Some(0)), "child results: {x:?} {y:?}");
    inspect(&path, 2, 1);
    assert!(matches!(PaperOwner::start_new(&path, CLOCK_MS), Err(OwnerError::Quarantine)));
}
#[test]
fn durable_commit_without_ack_denies_stale_parent_and_restart() {
    let path = journal("lost-ack");
    let mut parent = create(&path);
    assert_eq!(run_child("die_after_durable_without_ack", &path).code(), Some(78));
    inspect(&path, 2, 1);
    assert!(increment(&mut parent).is_err());
    assert!(parent.frozen);
    inspect(&path, 2, 1);
    assert!(matches!(PaperOwner::start_new(&path, CLOCK_MS), Err(OwnerError::Quarantine)));
}
#[test]
fn dead_writer_orphaned_lock_blocks_all_mutations() {
    let path = journal("orphan-lock");
    let mut parent = create(&path);
    assert_eq!(run_child("die_while_lock_present", &path).code(), Some(77));
    assert!(path.with_extension("writer_lock").exists());
    assert!(increment(&mut parent).is_err());
    assert!(parent.frozen);
    inspect(&path, 1, 0);
    // Lock is intentionally NOT cleared: manual, verified recovery is required.
}
#[test]
fn torn_tail_fails_closed_without_truncation_or_retry() {
    let path = journal("torn-tail");
    let mut parent = create(&path);
    assert_eq!(run_child("die_with_torn_tail", &path).code(), Some(79));
    let before = fs::metadata(&path).unwrap().len();
    assert!(PaperOwner::inspect_recovery(&path).is_err());
    assert!(increment(&mut parent).is_err());
    assert!(parent.frozen);
    assert_eq!(fs::metadata(&path).unwrap().len(), before);
}
