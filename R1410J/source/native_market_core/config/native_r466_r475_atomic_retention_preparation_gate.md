# R466–R475 — private retention PREPARATION, NEVER execution
Date: 2026-10-23. Scope: Rust source and private R366 SQLite in 0700 /tmp fixture.

## Source check
Original Python `/home/tmpadmin/spotbot/ai/collector.py` `prune_raw` runs `_rollup_raw_rows` BEFORE deleting source rows, and mirrors hourly aggregates, category deletes, event deletes, then metadata in a single transaction. Our R366/R376 proof currently requires one permanent source event/category/outbox append frame; naive deletion would violate full-chain audit.

## Narrow implementation
- New `retention_preparations` and `retention_preparation_journal` tables created ONLY in new isolated databases, and included in `reopen` table presence and independent `audit()`.
- R466 accepts strictly safe synthetic batch IDs and calls R446 retention candidate selection plus R456 source-Python-oracle-tested bounded rollup preview. It refuses empty plans, mismatch in event cardinality and unsupported Decimal precision.
- It stores the original candidate event ID/category/content_hash, 24-field rollup preview rows, and an ordered list of *planned* category DELETE, event DELETE, hourly UPSERT and rolled_up_raw_events metadata increment. It explicitly sets `deletion_authorized=false` and `db2_acknowledged=false`.
- BEGIN IMMEDIATE locks the private database and compares earlier mirror_outbox latest_seq to refuse a concurrent changed event snapshot. Prepared frame and chained journal row commit atomically. A failpoint after inserting the preparation but before the journal tests rollback. Same batch ID/same payload returns Duplicate; same ID/different request returns Conflict.
- Prepared-frame canonical hash, sequential journal hash and source event hash are verified by R466 audit and reopen. Orphan/missing/changed half-rows or tampered hash fail closed.
- R366/R376 primary event+category+outbox audit is unchanged; only research PREPARED tables are added. No actual hourly_raw_rollups row is materialized. No DELETE, no production mirror outbox change, no DB2 ACK.

## Ten focused checks
R466 successful atomic preparation, R467 reopen, R468 failpoint rollback, R469 failpoint retry, R470 duplicate, R471 divergent id conflict, R472 journal tamper, R473 missing preparation half, R474 new execution after plan leaves deletion_authorized false, R475 invalid parameters/no eligible candidates.

## Explicit release STOP
- No mutable source Python hourly_raw_rollups merge/upsert, actual deletion protocol frames, metadata update, durable mirror export to DB2, independent witness or source schema migration. Prepared journal is local and does NOT attest complete rollback if both preparation and journal are removed by an attacker.
- The plan becomes stale if a later execution links to a previously unlinked signal; it must **never** be treated as executable evidence without selecting, rolling up and validating again in the *same commit* as real retention.
- R366 audit still scans entire event/outbox chain before each append; R446 caps preview at 4096 rows. Performance and 10-concurrent/500-per-day acceptance remain unproved.
- Astra P0-01 partial, P0-02/P0-03 OPEN; operator YAML application by installed PAPER not proved. REAL kill ENGAGED. No live service restart, deployed ELF, DB1/DB2, disk cleanup or quarantine mutations authorized.
