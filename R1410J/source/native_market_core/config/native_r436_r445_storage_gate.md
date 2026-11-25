# R436–R445 partial — isolated PAPER SQLite storage admission gate (2026-10-23)

## Evidence and source contract
- Source of thresholds: `/home/tmpadmin/spotbot/ai/collector.py` `Limits`, `open_store`, `_space_check`, `_refresh_storage_health`.
- Existing Rust `native_ai_storage_review.rs` already provides **read-only** YAML parsing and 70/85/95% classification; no new rewrite of that observer.
- New **separate private test writer** `native_isolated_storage_gate_r436_review.rs`: constants source Python defaults DB 128 MiB, WAL 16 MiB, minimum 512 MiB free space, 15 MiB WAL preflight cutoff.
- Read-only `statvfs` on already path-guarded 0700 /tmp fixture folder, SQLite page_size/page_count/freelist_count, and private WAL symlink metadata with strict owner, mode and link count.
- Guard is applied **before** append() and collector_runs change(). At >=95% DB-used (conservative), WAL >=15 MiB or free bytes below reserve, reject with explicit `Pressure`. Python does not reject at 95%; this is deliberate extra denial, not exact admission parity.
- `PRAGMA max_page_count` is CONNECTION-SCOPED: demonstrated independently using Python sqlite3 (value resets after reopen). The private R366 SQLite wrapper now reapplies and verifies per-connection max_page_count, plus journal_size_limit, on every open.
- WAL sample taken while SQLite connection is alive to avoid misleading last-connection checkpoint.
- Six new offline tests: 70/85/95 boundaries, WAL cutoff, free reserve, connection quota reopen and live private-volume sample, private writer integration+reopen, unowned path denial.

## Release stop / deferred R442–R445
- No actual pinned-reader checkpoint recovery or lock-contention benchmark, and no source Python delayed maintenance lock/backoff logic. Deny-on-WAL is intentionally stricter than Python PASSIVE then TRUNCATE.
- Pure injected pressure tests demonstrate denial thresholds; they do **not** simulate actual external disk exhaustion or live WAL pinning. No attempt to fill real volumes.
- No operator-configured secret attestation or complete YAML consumption, no source-parity schema migration, no hourly rollups/pruning, no full event envelope, no DB2 ACK.
- This is **not installed PAPER**. No DB1 or DB2 mutation; no exchange API, no REAL, no restart, no quarantine change.
- Existing event/outbox audit still checks entire chain before each insert. Need worst-case p95/p99 and O(n²) scaling bounds before continuous ingest.
- Astra P0-01 DB1→DB2 forward durability PARTIAL; P0-02/P0-03 OPEN. Kill switch ENGAGED. No production activation.
