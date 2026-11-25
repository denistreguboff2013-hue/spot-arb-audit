# R556–R565 — P0-01 full *synthetic fixture* DB1/DB2 snapshot integrity

Date: 2026-10-23 | starting Git HEAD 472d22c2a0dd691ff8f1cc21cdf138086cb5b259

## What is truly covered
- One source DB1 and one receiver DB2, separate 0600 SQLite files,
  only inside owned 0700 /tmp/astra_r366_isolated_* test directories.
  Original R366/R376 event and outbox audits, R506 delivery/ACK,
  R546 ordered transaction-prefix proof are preconditions.
- R556 defines THREE synthetic fixture tables:
  r556_fixture_balances, r556_fixture_events, r556_fixture_metadata.
  These contain synthetic strings/JSON, not actual exchange records.
- The full fixture manifest covers ALL THREE allowed tables, including
  empty tables: exact sqlite_master CREATE TABLE DDL per table, sorted
  primary keys (BINARY collation), complete canonical JSON row bodies,
  per-row SHA256, row counts, table count, and global total count.
  It binds the digest to frozen source ACK boundary and test generation.
- Both files must independently produce the SAME manifest digest.
  Private source and sink SQLite BEGIN IMMEDIATE transactions re-check
  prefix generation, ACK, snapshot data and schema immediately before
  the single local DB2 proof row is committed. The failpoint intentionally
  aborts before commit and leaves no partial proof.
- Reopen verifies the local DB2 proof hash and recalculates the whole
  synthetic source/sink manifest, checking stale/altered payloads.
  All positive results hard-code independent_witness=false,
  permits_normal=false and permits_real=false.

## Ten focused Rust tests
R556 3 complete fixture tables, 4 source rows, full digest + durable proof.
R557 incorrect expected snapshot digest rejects staging.
R558 missing DB2 row rejects even if event delivery ACK is complete.
R559 extra DB2 row rejects.
R560 modified DB2 payload with identical row count rejects.
R561 SQL DDL mismatch rejects even when row bodies are unchanged.
R562 different insertion order for identical keys/bodies is accepted.
R563 interrupt before local proof commit fully rolls back and permits retry.
R564 source and sink reopen/reverify, then saved proof tamper detected.
R565 altered source fixture after staging makes proof stale; original
     R376 source history/outbox remains intact.

## HARD NO-GO for actual P0-01
- This is NOT a full copy or certified manifest of the historical 17
  PRODUCTION tables / 2,634,367 rows in stopped DB1/DB2. Only three
  synthetic fixture tables and four synthetic rows are checked.
- The fixture DDL is not the original Python collector schema. No
  authenticated external snapshot provenance, frozen real DB1 writers,
  production DB2 network link, separate independent witness, or replay
  against old mirror multi-generation/snapshot conversion has occurred.
- R556 does NOT migrate original source mirror_control to NORMAL, does
  NOT mutate the real mirror, perform source cleanup, deploy the test
  Rust executable or authorize REAL trading. No second server.
- No 500/day workload certification; tests are tiny and bounded
  (max 128 rows/table; 3 fixed tables; <=4096 event receipts).
- Astra P0-01 remains PARTIAL; P0-02 and P0-03 OPEN.
  Real kill switch stays ENGAGED; PAPER executable remains unchanged.
