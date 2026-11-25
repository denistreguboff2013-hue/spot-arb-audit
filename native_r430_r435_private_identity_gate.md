# R430–R435: isolated PAPER identity checks, partial scope

## Changes measured against Python source
- Original source: `/home/tmpadmin/spotbot/ai/collector.py` `append_record` checks `fills` identity as `(mode, exchange, order_id, fill_id)`, and indexes `ENTRY_COMMIT` / `EXIT_COMMIT` by mode and correlation_id.
- In the synthetic R366/R426 SQLite writer, detect a repeated full fill identity before any source/outbox mutation, returning `Duplicate`, even when `event_id` differs.
- Add three SQLite unique indexes in *new test fixtures*; second ENTRY_COMMIT or EXIT_COMMIT on the same mode/correlation is rejected before source and outbox writing. Distinct partial fills within one order remain distinct.
- Any execution category missing `correlation_id` fails closed before SQLite writing.
- Allow bounded JSON arrays without objects, floats or Decimal as category projections, preserving Python `json.dumps(..., ensure_ascii=False)` separators on demonstrated examples (including Cyrillic, bool and null). Unsupported floats and objects still fail closed, without pretending to be full Python parity.
- New test module `native_python_collector_dedup_r430_review.rs` covers duplicate fills across event IDs/reopen, one entry and exit commit each, missing correlation rollback, exact Unicode nested scalar array after-image, and separate fill IDs.
- Historical `r426` negative test updated so an array containing an object is rejected. Existing Python SQLite fixture and R376 after-image audit remain unchanged.

## What remains OPEN — do not stage or deploy PAPER
- This is NOT full Python collector source parity: general object-key insertion order, floats/Decimal, complete event envelope, Python indexes/migration and source-configured secret provenance are still unproved.
- `collector_runs`, approvals, rollups, retention delete/update mirror outbox, WAL pinned-reader diagnosis, disk-reserve guards and DB1->DB2 durable ACK are not completed in native Rust.
- Current R366 audit scans complete outbox before each insert, with unknown p95/p99 and potential O(n²) batch cost. Do not attach this writer to the running PAPER without bounds/performance proof.
- Test schema retains `user_version=426` (indexes added only for fresh isolated fixtures), no in-place old fixture migration is authorized.
- REAL execution remains forbidden; original Astra P0-02/P0-03 OPEN, P0-01 PARTIAL. Neither private exchange APIs nor installed PAPER service were changed.
- Follow-up: scoped R432–R435 collector_runs/rollups/guard work, source Python oracle for nontrivial numeric/object JSON, failure-injection tests, then independent PAPER deployment gate.
