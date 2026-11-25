# R496–R505: independent TWO-batch Python rollup/metadata oracle — READ-ONLY
Date 2026-10-23; starting HEAD 87cf1910c143f73492592db3426ab077d348caed

## Grounded source evidence
- `native_r496_python_multibatch_oracle.py` AST-loads unmodified original
  `_finite_number`, `_rollup_raw_rows`, `prune_raw` from
  `/home/tmpadmin/spotbot/ai/collector.py`, and original
  `mirror_outbox.py` on an in-memory sqlite3 DB with the source's
  hourly_raw_rollups CREATE TABLE schema.
- Fixture is 3 sequential old PAPER signals in the **same hour**, plus a
  decision in a later hour; prune_batch=2; each original prune_raw executes
  true SQL deletions ONLY in the in-memory Python oracle, never DB1/DB2.
- Starting metadata 0: consecutive source after-images are 2 -> 4.
  Starting metadata 5: source after-images are 7 -> 9.
- Second batch merges into an already existing same-hour SOL SIGNAL_SEEN
  group (count 2 -> 3, raw spread sum 3.50 -> 3.20, min 1.20 -> -0.30,
  max 2.30 unchanged, and previously existing net/age/depth metrics).
  Additional decision group is inserted, metadata incremented.
- The immutable `native_r496_python_multibatch_oracle.json` stores each
  batch's complete source rollup table, mirror_outbox operations, canonical
  Python JSON and SHA256 for independent comparisons.

## Rust implementation limits
- `native_python_rollup_multibatch_r496_review.rs` merges existing and
  incoming 24-column hourly rollup rows in memory using *bounded* exact
  `R456Decimal` (max 6 fraction digits, max 18 significant source digits,
  guard against arithmetic overflow). It preserves string trailing zeros,
  count, min, max and age maxima.
- It constructs in-memory UPSERT after-images and hashes, category/event
  DELETE **intents**, cumulative metadata UPSERT after-image; no DB is
  updated, no real source rows are deleted, and it never emits real outbox.
- It verifies canonical JSON and SHA with original Python; rejects malformed
  keys, duplicates, count/selection mismatches, overflow and unsupported
  numeric precision rather than rounding.
- R476 private shadow replay still **refuses second batch**, because it lacks
  an authenticated per-batch mutation of the eligible candidate set and
  source schema migration. Multi-batch Rust merge parity is a research
  proof, NOT deployment authority.
- Full production mirror_outbox R376 schema transition, DB2 apply/ACK,
  stale history/execution anti-race, durable external witness, performance
  certification, full Python Decimal rules and installed Rust PAPER use
  remain OPEN. Astra P0-01 PARTIAL; P0-02/P0-03 OPEN; REAL ENGAGED.

## Ordering caveat found in independent regression
- Original Python source does NOT specify ORDER BY for rows selected by
  _rollup_raw_rows. In this two-batch sqlite3 fixture, the incoming grouped
  UPSERT operations are visited in the first event_id order, rather than
  ascending hour_start_ms. Rust tests now reproduce that observed fixture
  ordering before comparing entire operations JSON and SHA256.
- This ordering is NOT a guarantee for other SQLite versions/plans; an
  explicit source protocol order or an independent query-plan oracle is
  required before multi-batch shadow/production activation.
