# Astra R296–R305 — isolated Rust PAPER analytics writer (2026-10-22)

## Safety baseline
- Server spb-3-vm-4r2s. Parent HEAD dfffbdcb80d0a556d20f5d238102b81ca7d7f860.
- 32 historical Git dirty/untracked paths, porcelain SHA256 9d5a1608ce8912435df2269be7eeee6ac17bc178e3442dd137fd56c4beb6ebc4, staging empty before work.
- Installed PAPER supervisor PID 2161721 ACTIVE, NRestarts=0, child initially 2866189, ELF SHA256 10138620181ebfd37dcb76b79c091c70d3315b5c536045fd6ea5c258aa0dbd46.
- Kill switch ENGAGED. No installed PAPER restart, no production DB1/DB2, no private exchange API, no Telegram, no real/model trading session or external WS.

## Ten review directions

R296: Verify handoff checkpoint and installed process. PASS at start. R285 full postcommit HEAD, 32 unchanged, stage=0 confirmed.

R297: Direct Python collector source: ai/collector.py uses WAL, synchronous=FULL, busy-timeout, foreign_keys, SQLite user_version/schema refusal, max_page_count/journal_size and maintenance_window. An outbox record is produced within the transaction that inserts events and category-specific rows. Python's append_record checks canonical content SHA, same event ID/identical body duplicate and conflicting ID different body; its mirror_outbox protocol uses generation/state/prev sequence and is more sophisticated than the new prototype.

R298: Existing Rust modules native_isolated_sqlite_mirror_r147_review.rs and R139/R147–R199 already expose private SQLite C FFI, Db/Stmt/Tx RAII and fail-closed rollback. REUSED via a single include of new file. Did NOT rewrite SQLite driver or alter existing DB1/DB2 guard.

R299: Added native_isolated_analytics_collector_r296_review.rs, a strictly **isolated, PAPER-only test prototype**. Uses one new database exactly named astra-r296-analytics.sqlite in owned mode-0700 direct /tmp/astra_r296_analytics_* directory, regular single-link mode-0600 file and symlink guard; no production path accepted. SQLite WAL+FULL, FK ON, user_version=296. This is not the Python schema and not an installed writer.

R300: Minimal event/outbox model: event_id unique; mode fixed 'paper'; categories limited to signals, decisions, errors; JSON object payload <=16 KiB; SHA256 computed from deterministic paper/id/category/JSON canonical source. One BEGIN IMMEDIATE transaction inserts event + mirror_outbox row; any failure before COMMIT rolls back automatically by existing Tx Drop. No orders, private API, external output, real PnL or Telegram.

R301: Idempotency: identical event_id and canonical content returns Duplicate with unchanged counts; changed body on same ID returns Conflict, never silently overwrites. NB: this is a bounded PROTOTYPE; not exact Python canonicalization/hash protocol parity and not an externally authenticated mirror.

R302: Recovery test: inject after-event rollback; reopen and confirm events/outbox both zero; append identical event later and verify persisted paired row. This is simulated failure, not SIGKILL/host powerfail proof.

R303: Negative tests: refuse unsupported event categories including fills, malformed/URL event IDs, oversized payload, non-isolated pathname and symlink-backed SQLite db. Path checks are owner/mode/symlink controls on private test directories, not independently verified race-proof file sandboxing.

R304: Five targeted R296 unit tests executed PASS, 0 FAIL. Full offline lib regression 1186 PASS, 0 FAIL, 2 ignored. Full binary regression 98 PASS, 0 FAIL across 81 bin groups. Total UNIQUE 1284 PASS, 0 FAIL, 2 ignored; targeted five already INCLUDED in library count.

R305: Commit *only* the new included test module, its single include in R147 and this audit note after tests pass. Preserve historic 32 paths and PAPER. Do not deploy, grant writer permissions, or claim Python durable AI parity.

## What is NOT closed
- Missing Python exact schema: attempts, events, category tables, collector_runs, hourly rollups, metadata, approval transitions, column policies and exact source content hash.
- Missing R139 mirror_control/outbox generation/prev sequence, causal DB1→DB2 forward replay and independent proof of SYNCED.
- Missing quotas, space headroom, WAL threshold, maintenance_lock, retry/backoff, crash-injected process death / fsync+host powerfail verification, schema migration/version negotiation, external authenticated witness.
- No Rust source PAPER integration of analytics enabled, AI pending-recommendation persistence, Telegram delivery or original 160-field installed runtime attestation.
- No trusted real exchange fills, unknown reconciliation, 4 legs, PnL or trading authorization. REAL NOT READY.

## Next R306+
1. Compare exact Python ai/collector schema, dedup hash and mirror_outbox transaction contract. Extend isolated Rust prototype incrementally without changing operator schemas or suppressing unsupported settings.
2. Add isolated recoverability under true process SIGKILL, adversarial corrupt schema and outbox failure rollback.
3. Audit stable lock, WAL pressure, and storage caps before attempting any PAPER runtime wiring.
4. Only then authorize a bounded isolated source PAPER candidate; never swap installed service by assumption.
