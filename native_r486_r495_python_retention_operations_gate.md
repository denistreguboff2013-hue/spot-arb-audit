# R486–R495: independent original Python retention operation oracle
Date: 2026-10-23. Starting Git HEAD: 1997773cafebc4d7cafbb1929b04c10f9e730d32

## Exact source evidence
- Generator native_r486_python_retention_operations_oracle.py loads UNMODIFIED
  _finite_number, _rollup_raw_rows and prune_raw from original
  /home/tmpadmin/spotbot/ai/collector.py (AST function extraction), and UNMODIFIED
  mirror_outbox.after/deleted/canonical/sha256 from /home/tmpadmin/spotbot/ai/mirror_outbox.py.
- It operates on sqlite3 `:memory:` only, with source-defined hourly_raw_rollups
  schema. It NEVER opens real DB1, DB2 or installed PAPER. Results saved into
  native_r486_python_retention_operations_oracle.json for reproducibility.
- Plain fixture: 2 old unlinked PAPER records -> 2 rollup UPSERT, 2 category DELETE,
  2 events DELETE, 1 metadata UPSERT = 7 operations.
- Linked fixture: old signal sharing correlation with execution retained;
  unlinked old decision pruned -> 1 rollup UPSERT, 1 category DELETE,
  1 events DELETE, 1 metadata UPSERT = 4 operations.
- R486/R487 Rust tests compare the ENTIRE original Python operations array with
  the isolated R476 SHADOW replay: tables, op types, keys, full after-image rows,
  after_hash and ordering. All 10 tests R486–R495 include original canonical
  JSON and digest parity, source-unlinked selection and metadata key/hash.
- Fix discovered by independent Python oracle: mirror_outbox.after for metadata
  returns op='upsert', not 'update'; corrected in private R476 shadow frame.
  Source SQL still uses UPDATE internally before emitting after-image.
- New fail-closed R476 shadow guard refuses applying a *second* batch in the
  private fixture: only zero-baseline rolled_up_raw_events has proven operation
  parity. Original Python would merge cumulative metadata and existing rollups.
  No claim of multibatch or live feed compatibility.

## Explicit blocks not closed
- R376 requires one physical event/category/outbox row per append. It is NOT
  migrated for real source deletion. No source tables are deleted by this work.
- R476 writes ONLY a private shadow outbox, NOT production mirror_outbox.record
  or production hourly_raw_rollups, so no DB1->DB2 ACK, no replica replay,
  no baseline/cleanup ACK, no independent antirollback witness.
- Source rollover with pre-existing hourly aggregate keys and nonzero metadata,
  Python Decimal arbitrary precision, concurrent writers, schema migrations,
  full 160 YAML operational coverage, sustained profitable installed PAPER
  remain unproved. No production deployment or REAL activation.
- Astra P0-01 still partial; P0-02 and P0-03 remain OPEN.
  Existing kill switch ENGAGED, installed PAPER binary unchanged.
