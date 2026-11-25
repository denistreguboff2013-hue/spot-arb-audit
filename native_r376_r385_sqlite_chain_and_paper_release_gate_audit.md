# SPOT-ARB R376–R385 — isolated integrity, staged PAPER integration gate
Date: 2026-10-22.
Server spb-3-vm-4r2s; branch review/native-core-v3-r31-entry-permit-dispatch-20261008.
Pre-change HEAD 8e309b4accb2763e6de7500ad9c970674320e8dd.
Original 32 historical dirty/untracked paths SHA256 9d5a1608ce8912435df2269be7eeee6ac17bc178e3442dd137fd56c4beb6ebc4; empty staging.
Installed PAPER supervisor PID 2161721 active, NRestarts 0, child PID 2919927 at preflight, ELF SHA256 10138620181ebfd37dcb76b79c091c70d3315b5c536045fd6ea5c258aa0dbd46; kill switch ENGAGED.
Disk3 5.4 GiB free / 82% used. No production service, DB1, DB2, Telegram, private API or REAL changes.

## Ten review directions — what was actually performed
R376 — Re-read the existing R366 Rust SQLite event/outbox subset. Identified that audit compared counts, sequence hints and row coverage, not independent source/outbox SHA256.
R377 — Added source_record_json TEXT NOT NULL to *private R366 fixture schema* and store source Event.record serialized JSON in the same atomic event/category/outbox transaction. This research schema is NOT Python production schema.
R378 — Added native_isolated_event_integrity_r376_review.rs in the existing R147 SQLite FFI module, checking SQLite integrity_check, event SHA from persisted source JSON, event column correspondence and canonical parse.
R379 — Audit reads outbox payload_json, recalculates SHA256, checks causal prev_seq chain and strictly monotonic seq, tx_id linkage, protocol version, schema_generation, feed generation, transaction size/accounting, source id/hash references.
R380 — Audit verifies category row existence and no opposite-category row, exact control last_seq, retained records/bytes, explicit BOOTSTRAP_REQUIRED state (never SYNCED).
R381 — New append calls fail-closed audit before attempting transaction (intentionally O(events+outbox) per append, NOT production throughput-ready).
R382 — Six targeted Rust tests PASS: two valid entries/reopen, outbox hash tamper, event hash tamper, broken prev_seq, missing category row, and fail-closed append after corruption. First compile exposed ? handling mistakes, corrected before PASS.
R383 — Independent Python analysis of the 5 source Event.record accepted oracle values: Python's json.dumps(record, ensure_ascii=False, sort_keys=True) bytes differ from compact serde_json canonical bytes in all five cases. Thus SHA / exact mirror parity is NOT proven; do not deploy as Python writer replacement. Main source also serializes inner payload_json and verification_json differently.
R384 — Full Rust cargo test --offline --locked --lib and --bins completed with exit code 0. Library 1218 PASS/0 FAIL/2 ignored; binaries 98 PASS/0 FAIL in 81 groups; unique total 1316 PASS/0 FAIL/2 ignored. Six R376 targeted PASS are ALREADY INCLUDED in 1218 library PASS.
R385 — Scoped commit only modified isolated R366 source, R296 include, new R376 source, this audit if full suite passes. Recheck original 32 Git paths and installed PAPER/kill switch. NO deployed PAPER update in this phase.

## PAPER rollout eligibility, staged workflow approved by operator
1. Offline source tests PASS (R376), sufficient as library research gate only.
2. **BLOCKED** canonical Python Event.record/append_record SHA, full row after-images, outbox after_hash and feed generation; precise change-feed cannot replay to DB2.
3. **BLOCKED** compatibility with full Python collector tables (10 categories), source schema, retry/lock/pressure, retention, configured secrets, 160 installed YAML bindings, AI recommendations.
4. **BLOCKED** SIGKILL/crash recovery with durable new/old writer + shadow rehearsal and explicit quarantine handling. Historical four virtual positions remain quarantined.
5. **BLOCKED** installable ELF build/packaging and isolated smoke tests verifying paper-only, exchange private clients absent, REAL kill switch ENGAGED, no production DB changes.
6. Only after all component-specific gates pass: immutable release artifact/hash, rollback image, operational stop/no-open-risk check, controlled PAPER service change and telemetry/Telegram confirmation of actual new ELF. Never automatic per commit, never alter REAL.
Current decision: **NOT ELIGIBLE FOR ACTIVE PAPER DEPLOYMENT**. Proven research != installed runtime. Continue testing and qualifying; do not change PAPER just to advertise progress.

## More limits
Independent SHA recomputation catches accidental corruption and some tampering, but a privileged attacker able to replace whole consistent DB and hashes is not ruled out without an external witness.
Current R376 integrity gate intentionally checks 2 source categories and cannot verify authentic DB1->DB2 logical replication or real exchange PnL.
DB1 -> DB2 long-running mirror stability, P0-02/P0-03, original 13 Astra issues remain open. REAL not authorized.
