# R506–R515 — Astra P0-01 isolated two-SQLite causal delivery gate
Date: 2026-10-23 | Starting Git: 983a3301e5d0545997cde8a85228b4eb9482da4f

## Actual scope and non-authority
- All tests use owned 0700 `/tmp/astra_r366_isolated_*` directories.
- Synthetic DB1 is the existing R366 SQLite with 0600 file, full R376
  event/category/outbox integrity audit; DB2 is a *new, separate* 0600
  astra-r506-private-db2.sqlite file. This is not the production mirror.
- Source-only `r506_source_ack` table is initialized with (seq 0, GENESIS).
  It is checked during source reopen/audit; actual R366 outbox untouched.
- DB2 applies source `event_append` operations as distinct (table,key)
  canonical after-image rows with original row content hash; sink_delivery
  receipt, sink_control sequence and source-row projections are committed
  atomically in DB2 (SQLite WAL/synchronous FULL).
- The explicit DB1 acknowledge step first verifies DB2 audit, source
  sequence and *same source payload_hash*, and then writes one sequential
  source acknowledgment using a separate DB1 transaction. DB2 commit
  before ACK is deliberately possible: it is the lost-ACK recovery window.
- Separate `inspect` rechecks source R376, DB2 audit and every source/sink
  transaction hash. It is not an independent outside-VM witness.

## Negative tests (R506–R515)
R506 source->sink->ACK with intermediate observations.
R507 DB2 durable, ACK interrupted, duplicate delivery rejected, ACK retry.
R508 DB2 rollback failpoint before commit; no partial rows/receipts.
R509 ACK before DB2 delivery refused.
R510 DB1 append fails before outbox; no source or sink event.
R511 gap in delivery and out-of-order ACK refused.
R512 duplicate delivery and ACK no second count.
R513 tampered DB2 after-image fails audit and ACK.
R514 tampered DB1 receipt fails source audit and reopen.
R515 restart with DB2 ahead of ACK, then finish ACK.

## Open P0-01 gates
- This two-file fixture uses a minimal synthetic receiver model and only
  append operations; it does NOT implement original deployed Python
  mirror.service, its generation/rebaseline/rebuild semantics, real 17-table
  schema, ack cursor retention, retries over sustained load, or true
  incremental production source DB1 -> real DB2.
- No rolling deletion/retention mirrored; the R476 shadow is still
  separate and never deletes actual source rows.
- No actual power-cut durability witness, reader-pinned WAL soak, independent
  external copy, full live throughput/p99 limit or deployed ELF attestation.
- No real exchange account GET/POST/order; P0-02 and P0-03 remain open.
  REAL kill stays ENGAGED. Not authorized to run/restart installed PAPER,
  old mirror, or modify live DB1/DB2.
- Source offline PASS only; P0-01 remains PARTIAL/NOT CERTIFIED.
