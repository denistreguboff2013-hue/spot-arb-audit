# R476–R485 isolated retention SHADOW apply — NOT source deletion
23 October 2026 | starting HEAD 067c025

## Original Python evidence
- Original /home/tmpadmin/spotbot/ai/collector.py _rollup_raw_rows + prune_raw and /home/tmpadmin/spotbot/ai/mirror_outbox.py deleted()/after() are the reference.
- In Python, transaction order is hourly_raw_rollups UPSERT before category DELETE before events DELETE before metadata UPDATE. The last mirrors the **updated durable metadata row**; mirror_outbox.record is in the same DB1 transaction as real changes.
- Existing R366/R376 SQLite integrity expects each original event, category row and insert outbox frame to remain permanently available; changing its event rows to simulate source retention is NOT safe or equivalent to Python.

## Narrow demonstration
- R466 prepared frame retained unchanged. R476 constructs a source-order **simulation envelope** with schema version 1 and Python-compatible op/key structure. Explicit tx_kind PRIVATE_SHADOW_RETENTION_NOT_PRODUCTION, source_rows_deleted=false and db2_acknowledged=false.
- Only 0700 /tmp/astra_r366_isolated_* DBs. R476 first requires independent R366 + R435 + R466 audit and R436 storage guard, verifies original mirror_control latest_seq matches prepared snapshot, and rechecks source event hashes.
- Inside one SQLite BEGIN IMMEDIATE: copy each candidate into retention_shadow_copies, record immutable hash tombstone, DELETE **only that temporary shadow copy**, write shadow rollup JSON and shadow metadata, and append one chained retention_shadow_outbox frame. An interrupt-after-delete failpoint must roll back all shadow mutations.
- Event IDs are globally unique in shadow tombstones, preventing a second prepared batch from simulating a second delete of the same event; an explicit preflight returns Conflict, not a generic SQLite error. Identical batch ID returns Duplicate.
- Reopen audit independently checks chain sequence/hash, frame regenerated from immutable prepared source, tombstones matching original source ID/category/hash, rollup rows and metadata counts, absence of residual shadow copies and orphan rows. R366/R376 legacy real event/outbox integrity remains unchanged.
- Ten tests R476–R485: success with immutable source, Python operation ordering, failpoint all-row rollback, retry after rollback + reopen, duplicate, stale snapshot conflict, outbox tamper, tombstone tamper, rollup/metadata tamper, and a second prepared batch for same events conflict.

## Does NOT close original Astra audit
- NO real source deletion, real hourly_raw_rollups SQLite UPSERT, production mirror_outbox.record, mirrored metadata existing-value merge, remote DB2 apply/ACK, durable external witness or approval. Any attempt to treat private shadow success as production retention is prohibited.
- Python Decimal numeric contexts beyond bounded R456 fixture remain unsupported. No actual schema migration from user_version=426, no installed PAPER deployment, no live exchange API or real-trade permit.
- Current R366 full audit per insert remains potentially O(n²) over history and unprofiled at production volume; R446 selection reads at most 4096. Trade-associated event retention is never authorized.
- Original P0-01 DB1->DB2 partial, P0-02/P0-03 OPEN, YAML installed PAPER still 0/160 proven.
