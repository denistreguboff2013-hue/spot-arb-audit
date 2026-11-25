# R435 private collector_runs life-cycle — partial evidence, NOT a PAPER deployment
Date: 2026-10-23. Based on /home/tmpadmin/spotbot/ai/collector.py, _worker and _persist_health.

## Proven implementation in newly-created private /tmp fixture
- Dedicated `collector_runs` table with original five column names and types: run_id, started_at_ms, updated_at_ms, closed_cleanly, stats_json.
- `start`, `heartbeat`, `close` modify the run row and local `collector_run_journal` in one SQLite BEGIN IMMEDIATE/COMMIT; failing a write rolls back.
- Journal stores immutable after-image snapshots, monotonic seq, prev_hash, frame hash, and JSON canonical serialization. Reopen audit replays lifecycle, verifies the full chain, detects orphan/missing/changed final rows.
- Only synthetic `paper` file name under private 0700 /tmp/astra_r366_isolated_* accepted, SQLite 0600 and WAL FULL durability. No system PAPER restart/deploy or production DB1/DB2 writes.
- Source-derived statistic whitelist: accepted, committed, duplicates, dropped, rejected, write_failures, numeric unsigned values only, no arbitrary text, secrets or metadata; run IDs restricted to safe ASCII.
- Five new tests: replay/close/reopen and independent event append, missing/duplicate/closed transition, invalid string secrets and counters, tampered historical hash, tampered final SQL row.
- Original mirror-outbox event chain remains independent and unmodified in semantics.

## Explicit limitations / next tasks
- This journal is **not** Python mirror_outbox.record(collector_run_start/collector_heartbeat/collector_run_close), and is **not** acknowledged by DB2.
- Actual Python run-start stats also include source/pid and health() contains other fields; only a safe small numeric subset is implemented here. No original schema/retention upgrade, no live config-secret attestation.
- No historical antirollback independent witness, no long-duration WAL reader, disk-pressure/70-85-95%, lock-contention or p95/p99 performance certification.
- Original isolated schema user_version 426 retained; newly required run tables mean older 426 fixtures are deliberately rejected, not migrated automatically. A reviewed schema migration/version transition is required before a real release.
- `audit()` still scans history/outbox before each write; not suitable for high-frequency deployment without complexity bounds and load proof.
- P0-01 forward mirror and durable DB2 ACK remain PARTIAL, P0-02 and P0-03 OPEN. 0/160 operator YAML parameters proven used by installed PAPER.
- No REAL orders or exchange APIs permitted. Kill switch must remain ENGAGED and PAPER stays on its installed SHA until independent go/no-go.
