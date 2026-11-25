# R566–R575: Original Python 17-table mirror schema and snapshot fixture

Date: 2026-10-23. Starting HEAD: 5bff78d8a06220328a514fdaee632ddd70f99e11.

## Proven source extraction (not a production scan)
- Source files: /home/tmpadmin/spotbot/ai/collector.py,
  schemas.py, approval_ledger.py, mirror_outbox.py.
- Python generator native_r566_original_collector_schema_oracle.py
  uses AST literal extraction for FIELDS, CATEGORIES,
  approval SCHEMA statements and mirror_outbox.TABLE_KEYS.
  It extracts original collector CREATE TABLE/INDEX declarations
  verbatim, then executes original mirror_outbox.ensure_schema.
  NO original collector connection to production is opened.
- Two independent sqlite3 :memory: DBs are constructed. Exactly
  **17 mirroring tables** are represented: attempts, events,
  10 event-category projections, collector_runs, hourly_raw_rollups,
  metadata, advisor_recommendation_ledger, advisor_approval_transitions.
  mirror_outbox and mirror_control are TWO additional service tables;
  not counted among the 17 table-key contracts.
- SQLite-extracted metadata includes each CREATE TABLE SQL,
  PRAGMA table_info columns and types/constraints and primary key,
  foreign keys, index_list + index_xinfo (including partial indexes),
  SQL index declarations, trigger declarations, mirror TABLE_KEYS.
  The 8-part rollup PK is checked exactly.
- Six synthetic rows populate metadata, attempts, collector_runs,
  events, decisions and hourly_raw_rollups on each :memory: database.
  Others are intentionally empty yet included in the manifest.
  A deterministic sorted-key, whole-schema+whole-row SHA256
  is stored in native_r566_original_collector_schema_oracle.json.
- Original Python fixture verifies both independent database manifests
  match despite changing independent insertion order; an UPDATE with
  unchanged row count, missing row, extra row, DDL change, index removal,
  approval immutability trigger removal or composite-PK change results
  in a different digest. A mutation with rollback restores the exact
  original digest; tests are entirely in-memory.
- Ten Rust tests R566-R575 validate the pinned full snapshot digest,
  17 table definitions + 17 key contracts, 6 row snapshots, composite PK
  and source-related FK/index/trigger evidence, reproduce actual
  original schema and indexes/triggers in another Rust SQLite :memory:,
  and reject tampered rows, DDL, absent tables and mismatched keys.
  Source fixture JSON is an auditable sample, NOT a release certificate.

## Limitations and priority for Astra P0-01
- NOT a scan of any existing production DB1 or DB2, and NOT a new
  comparison of the historical 17 tables with 2,634,367 production rows.
  The true DB1 user_version can have preexisting migration histories,
  field alterations, legacy data, or additional indexes that differ
  from freshly initialized source-expected schema. Such differences
  must be reviewed, not silently harmonized.
- The source SQL extraction is bounded to known literal declarations.
  A source file could change runtime migration paths beyond those
  extracted; therefore source code and generated SHA hashes are pinned.
- No independent signed or remote DB2 witness, authenticated DB1
  file checksum, freeze/ACK/baseline-generation acceptance, sustained
  PAPER replication, production lock, production writes, or trades.
- P0-01 remains PARTIAL/NOT CERTIFIED; P0-02/P0-03 still OPEN.
  No collector cleanup or mirror service start/restart.
  REAL kill stays ENGAGED and installed PAPER binary untouched.
- Next gate should be safe bounded READ-ONLY inventory of genuine
  existing DB1/DB2 schema identities and recorded old stopped
  17-table snapshot evidence, if authorized by file and disk policy,
  followed by isolated replay of original Python mirror operations
  against its real-shaped test schema. Do not create a second server.
