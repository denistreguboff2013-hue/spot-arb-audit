# R526–R535 — P0-01 Python cleanup / source state; private ACK cleanup preview
Date 2026-10-23 | Starting Git HEAD 1fef0f8c4e16648d7810d2c3f68703eb2849e067

## Independent ORIGINAL Python experiment
- The generator `native_r526_python_mirror_cleanup_oracle.py` imports the
  actual /home/tmpadmin/spotbot/ai/mirror_outbox.py, uses
  ensure_schema/record/cleanup and :memory: sqlite3 with transactions.
- Only the on-disk free-space guard was bypassed for 130 NORMAL synthetic
  source records (SQLite :memory: has no database file path). This is not
  validation of free-space protections or deployed source settings.
- Starting with 130 retained records, cleanup LIMIT 128 removes:
  128 + 1 + 1 in three committed transactions; re-run returns zero.
- cleanup without BEGIN rejects; interruption via rollback after selecting
  128 restores all 130 rows and counters. retained_bytes reaches zero
  when all rows are cleaned.
- REBUILD_REQUIRED returns no new outbox frame and retains failure state.
  >4MiB source payload marks REBUILD_REQUIRED with
  OUTBOX_TRANSACTION_TOO_LARGE, no new outbox frame.
- CRITICAL: original Python cleanup(con,applied_seq) itself does NOT attest
  source of applied_seq or verify a DB2 durable receipt. The caller must
  guarantee DB2 commits and ACK provenance. Do not pass an untrusted seq.
- All Python source writes in this oracle are ONLY to :memory: DB,
  never real DB1 or DB2.

## Rust synthetic ACK-scoped cleanup PREVIEW
- New R526 private source test consults full R376 source audit AND R506
  separate test DB2 audit and matches all delivered hashes before reading
  ACK from private DB1. Selects <=128 first source outbox rows with
  seq <= authenticated fixture ACK. Rejects gaps, incomplete source
  accounting or missing receipt. Reports selected seq, expected bytes,
  BOOTSTRAP_REQUIRED vs NORMAL and retained bytes.
- `deletion_authorized` is ALWAYS false, even when state is NORMAL and
  full local source-to-receiver ACK is present. There is no DELETE.
- R526-R535 include four source-Python regression checks and six private
  Rust tests: missing ACK, DB2 ahead of ACK, verified prefix, restart
  extension of ACK prefix, corrupted DB2 and a forged private NORMAL state.
  Source R376 rejects this unauthorized change with Corrupt; it is never
  treated as permission for source cleanup.
- R366 R376 original immutable source/category/outbox assumptions remain
  unchanged; no production mirror outbox cleanup, no remote DB2 ACK.

## Remaining Astra blockers
- Original Python feed state transitions/rebaseline source identities and
  cleanup_through/cursor semantics are NOT implemented in the R506 test
  sink. The R526 private schema has no cleanup_through.
- R506 private ACK can be changed by a privileged process on the same
  server; no independent external witness. No production credentials,
  no real DB1/DB2, no restart/deployment, no 17-table longitudinal apply.
- Original cleanup source does not check DB2; the standalone Python
  trust boundary remains unproven in the deployed system.
- No 500/day throughput or full performance soak: R376 full audit on
  each synthetic event may grow quadratically. Old Python storage guard
  thresholds 512/768/1024MiB and free reserve not certified.
- P0-01 PARTIAL, P0-02 and P0-03 OPEN. REAL kill ENGAGED; installed
  PAPER must not be restarted, replaced or authorized for real trading.
