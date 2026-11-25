# R516–R525: P0-01 Python mirror format and isolated backlog witness

Date: 2026-10-23. Prior HEAD fcadc71cd3145b17a1fa2e88c91c1d7a96590d5d.

## Original Python source / controlled oracle
- `native_r516_python_mirror_feed_oracle.py` imports the ORIGINAL
  `/home/tmpadmin/spotbot/ai/mirror_outbox.py`. It runs `ensure_schema`,
  `after` (real sqlite3 row after-images), `canonical`, `sha256` and
  `record` against a PRIVATE `:memory:` SQLite.
- Only `_storage_reason` was overridden to return None because the fixture
  has no file-backed DB1 and live free-space guard cannot run. This means
  source's disk reserve, 4MiB/frame, 512/768/1024MiB retained-byte thresholds,
  cleanup, generation/rebaseline/failure-transition are **NOT** validated.
- The original Python oracle contains a 3-operation event_append payload
  with exact JSON canonicalization, source row `after_hash`, payload hash,
  payload_bytes (canonical UTF-8 length + 384), control fields and
  BOOTSTRAP_REQUIRED state. It is saved in
  `native_r516_python_mirror_feed_oracle.json` for independent review.
- The original Python `record` creates a random tx_id and current timestamp:
  the oracle compares deterministic payload, key, digest and budget; it
  does not claim deterministic source tx_id, timestamp or feed generation.

## Private Rust R516–R525 tests
- R516 exact original Python canonical frame/digest.
- R517 original source after-image and budget byte accounting.
- R518 16 synthetic durable source messages -> separate SQLite DB2 -> 16 ACK,
  with 1:1 receipts and durable row counts. This is small smoke workload, NOT
  a 500/day soak or performance certification.
- R519 DB2 committed + DB1 ACK lost, reopen both DBs, duplicate delivered
  frame rejected and ACK retry succeeds.
- R520 while DB1 produces more messages with DB2 lagging, catchup remains
  sequential without gaps.
- R521 corrupted source payload_hash cannot reach DB2.
- R522 changed sink applied_seq prevents another delivery.
- R523 missing sink receipt makes reopen fail closed.
- R524 unsupported source tx_kind cannot reach sink.
- R525 mirror BOOTSTRAP_REQUIRED and private real_mirror=false remain
  explicitly unauthoritative even when all source, sink and ACK seq match.

## Open P0-01 release gates
- R506/516 is a synthetic **append-only** source DB and private
  event-append-only DB2 receiver. Actual Python mirror supports 17+ table
  primary-key shapes, upsert/update/delete, source cleanup, bootstrap
  generation, REBUILD_REQUIRED transitions and outbox cleanup. None are
  production-certified here.
- Real DB1/DB2 remain unchanged, old mirror not restarted. This test does
  not authorize replication cutover, use of a new executable in PAPER or
  REAL orders. No production DB2 ACK observed.
- Full R376 scan on each synthetic append/delivery yields possible O(n²)
  cost; no latency/throughput/p99 acceptance at 10 concurrent / 500/day.
- P0-01 PARTIAL; P0-02/P0-03 OPEN; real kill switch remains ENGAGED.
