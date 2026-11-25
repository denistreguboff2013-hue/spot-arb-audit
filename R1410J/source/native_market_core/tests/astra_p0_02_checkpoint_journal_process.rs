//! Offline P0-02: a second PID must reject independently fsynced
//! PAPER checkpoint/journal crash windows. NOT actual exchange fills.
//! All artifacts live under unique /tmp fixtures; no production service access.
use spotarb_native_market_core::{
    native_paper_continuity_checkpoint_review::{
        inspect_checkpoint_model_alignment, save_checkpoint,
    },
    native_paper_market_execution_review::{
        PaperAssumptions, PaperMachine, PaperPhase, PaperPosition,
    },
    native_paper_portfolio_review::PaperPortfolio,
    native_virtual_four_leg_provenance_review::{inspect_model_journal, ModeledFourLegJournal},
    transport::monotonic_ns,
};
use std::{
    env, fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{SystemTime, UNIX_EPOCH},
};
const SESSION: &str = "session_1";

fn fixture(name: &str) -> PathBuf {
    let ns = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    env::temp_dir().join(format!("paper-crosspid-{}-{name}-{ns}", std::process::id()))
}
fn wall_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}
fn states() -> (PaperPortfolio, PaperPortfolio) {
    let mut before = PaperPortfolio::new(wall_ms()).unwrap();
    before.stats.entries = 1;
    let t = monotonic_ns().saturating_sub(5_000_000);
    let pos = PaperPosition {
        base: "ONE".into(), signal_ns: t, spot_buy_ns: t + 1,
        signal_estimated_net_bps: 60.0, spot_gross_base: 1.0,
        spot_delivered_base: 1.0, spot_paid_quote: 10.0,
        futures_short_ns: Some(t + 2),
        futures_short_proceeds_quote: Some(10.2),
        futures_short_base: Some(1.0),
        hedge_entry_estimated_net_bps: Some(60.0),
    };
    let mut machine = PaperMachine::new(PaperAssumptions::default()).unwrap();
    machine.phase = PaperPhase::Hedged(pos.clone());
    before.positions.insert("ONE".into(), machine);
    let mut after = before.clone();
    after.positions.get_mut("ONE").unwrap().phase = PaperPhase::AwaitingSpotExit {
        pos, cover_quote: 10.0, cover_ns: monotonic_ns(),
        close_estimate_quote: 0.2,
    };
    after.stats.futures_covers += 1;
    (before, after)
}
fn writer(root: &Path, mode: &str) {
    let (before, after) = states();
    let checkpoint = root.join("checkpoint.json");
    let journal = root.join("journal.jsonl");
    let mut j = ModeledFourLegJournal::start(&journal, SESSION, &before, 1).unwrap();
    match mode {
        "checkpoint_ahead" => {
            save_checkpoint(&checkpoint, &after, 2, monotonic_ns(), false).unwrap();
        }
        "journal_ahead" => {
            j.record_change("ONE", &before, &after).unwrap();
            save_checkpoint(&checkpoint, &before, 1, monotonic_ns(), false).unwrap();
        }
        "matched" | "forged_completion" | "sealed" => {
            j.record_change("ONE", &before, &after).unwrap();
            if mode == "sealed" { j.seal(&after).unwrap(); }
            save_checkpoint(&checkpoint, &after, 2, monotonic_ns(),
                mode != "matched").unwrap();
        }
        _ => panic!("invalid test scenario"),
    };
}
fn reader(root: &Path, expected: &str) {
    let checkpoint = root.join("checkpoint.json");
    let journal = root.join("journal.jsonl");
    let cp_before = fs::read(&checkpoint).unwrap();
    let j_before = fs::read(&journal).unwrap();
    let audit = inspect_model_journal(&journal).unwrap();
    assert_eq!(audit.entries, 1);
    assert_eq!(audit.open, 1);
    assert_eq!(audit.closed, 0);
    assert!(audit.model_phase_by_base.contains_key("ONE"));
    // A denial must not erase the still-open virtual position.
    let payload: serde_json::Value = serde_json::from_slice(&cp_before).unwrap();
    assert!(payload["payload"]["portfolio"]["positions"]["ONE"].is_object());
    let verified = inspect_checkpoint_model_alignment(
        &checkpoint, &journal, SESSION, expected == "sealed");
    if expected == "matched" || expected == "sealed" {
        assert!(verified.is_ok(), "consistent cold pair must pass: {verified:?}");
    } else {
        assert!(verified.is_err(), "crash window/false seal must deny");
    }
    assert_eq!(fs::read(&checkpoint).unwrap(), cp_before);
    assert_eq!(fs::read(&journal).unwrap(), j_before);
}

#[test]
fn subprocess_worker() {
    let Ok(mode) = env::var("SPOTARB_CROSSPID_MODE") else { return };
    let root = PathBuf::from(env::var_os("SPOTARB_CROSSPID_ROOT").unwrap());
    if let Some(scenario) = mode.strip_prefix("write:") {
        writer(&root, scenario);
        // Actual PID death after fsync, without any follow-up journal/checkpoint write.
        std::process::exit(if scenario == "checkpoint_ahead" ||
            scenario == "journal_ahead" { 78 } else { 0 });
    }
    if let Some(expected) = mode.strip_prefix("read:") {
        reader(&root, expected);
        std::process::exit(0);
    }
    panic!("invalid cross-PID mode");
}
fn child(root: &Path, mode: &str) -> i32 {
    Command::new(env::current_exe().unwrap())
        .arg("--exact").arg("subprocess_worker").arg("--nocapture")
        .env("SPOTARB_CROSSPID_MODE", mode)
        .env("SPOTARB_CROSSPID_ROOT", root)
        .stdout(Stdio::null()).stderr(Stdio::null())
        .status().unwrap().code().unwrap()
}
#[test]
fn independent_reader_denies_both_fsync_crash_orders() {
    for scenario in ["checkpoint_ahead", "journal_ahead"] {
        let root = fixture(scenario);
        assert_eq!(child(&root, &format!("write:{scenario}")), 78);
        assert_eq!(child(&root, "read:deny"), 0);
        assert_eq!(child(&root, "read:deny"), 0); // fresh third PID
        fs::remove_dir_all(root).unwrap();
    }
}
#[test]
fn independent_reader_requires_full_pair_and_real_journal_seal() {
    for scenario in ["matched", "forged_completion", "sealed"] {
        let root = fixture(scenario);
        assert_eq!(child(&root, &format!("write:{scenario}")), 0);
        let outcome = if scenario == "forged_completion" {
            "deny"
        } else { scenario };
        assert_eq!(child(&root, &format!("read:{outcome}")), 0);
        fs::remove_dir_all(root).unwrap();
    }
}
