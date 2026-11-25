# R426–R435 partial delivery: source Python category contract (2026-10-22)

## Proven scope only (source/offline, not installed)
- Read actual `/home/tmpadmin/spotbot/ai/schemas.py`, `ai/collector.py`, `ai/mirror_outbox.py`; source categories and all `FIELDS` projection names are used as the oracle.
- R426 Rust static whitelist now enumerates 10 source categories and their exact named projection columns.
- Synthetic SQLite R366 writer creates all 10 PAPER-only category tables, with TEXT fields and SQL NULL for absent fields. Every event + category + optional first attempt + full after-image outbox belongs to one private transaction.
- R376 audit validates the source JSON event and its category projection separately from the serialized outbox/after_hash, rejecting missing/wrong/multiple/orphan category rows, seq/prev_seq and changed hashes.
- Versioned *isolated* test schema is 426. Older isolated 366 fixtures require deliberate recreation; there is no approved migration of an existing production DB.
- Synthetic oracle is generated independently by Python standard `sqlite3` using an AST-literal extraction of `FIELDS` from the original `collector.py`: `native_r426_python_sqlite_categories_oracle.json`. Ten categories, twelve row/after-hash test cases. Older compact R396 decision oracle is retained separately and not misrepresented as the current full Python category projection.
- Four new tests: all source category names/columns, 10 sequential durable source/after rows compared to Python SQLite, SQL NULL preservation, fail-closed unsupported complex/float projection.
- Tested existing event/outbox audit, two-hash tampering, duplicate attempts and own /tmp SIGKILL WAL recovery.

## Explicit non-parity and release STOP
- Current R426 projection accepts SQL NULL, text, integer and bool adaptation. It DENIES float, list and object column values until exact Python sqlite binding and canonicalization evidence, so this is NOT complete original collector parity.
- Current private event table still has a subset of original Python envelope columns and extra `source_record_json`; SQL schema indexes and complete migration are not source-identical.
- Current isolated Rust append is NOT installed PAPER writer; installed ELF SHA unchanged. There is no DB2 delivery/ACK or production feed generation migration.
- The original Python `collector_runs`, hourly raw rollups, metadata, approval transitions, retention, lock contention, WAL pinning, disk reserve and controlled schema migration are not yet implemented as a full Rust source-parity pipeline.
- Configured-secret admission from original operator settings and 10-category data ownership must be proven before live ingestion; no active service may be connected now.
- `0/160` installed per-field config provenance remains unproven; 149 source research / 11 guard-only. No YAML feature toggles or daily caps silently changed.
- REAL P0-02/P0-03 remain OPEN, kill switch ENGAGED. No private API calls, no orders, no active PAPER restart, no historic position/quarantine mutation.

## Next approved work within R426–R435
1. Complete original Python Event.record → 10-category SQLite adaptors, including float/decimal/array/order/Unicode oracle; ensure fail-closed byte/complexity budgets.
2. Add exact original event envelope, dedup for fills and unique entry/exit commits, source lineage and schema migration tests.
3. Add collector_runs, rollups, approval schema, retention cleanup and synthetic mirror delete/update operations with real source row before/after.
4. Add configured-secret source attestation before ingestion, bound WAL/checkpoint and outbox reserve 70/85/95% failure injection.
5. Only after these and one full locked Rust regression prepare immutable PAPER-only candidate; later operator cutover requires separate go/no-go and rollback.
