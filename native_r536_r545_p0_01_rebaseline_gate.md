# R536–R545: Astra P0-01 mirror feed generation/rebaseline oracle
Date: 2026-10-23 | Starting HEAD 2dee77901cb2cbde59abfd72b86c951e2d496c59

## ORIGINAL Python source proof
- `native_r536_python_rebaseline_oracle.py` loads UNMODIFIED original
  /home/tmpadmin/spotbot/ai/mirror_outbox.py and invokes ensure_schema,
  record, prepare_rebaseline, _storage_reason against ONLY sqlite3 :memory:.
- No real DB1/DB2, PAPER service, exchange endpoints or files written except
  the immutable synthetic JSON oracle in this source repository.
- prepare_rebaseline from BOOTSTRAP_REQUIRED refuses. Valid call requires
  REBUILD_REQUIRED state, an active DB1 transaction, nonempty immutable
  source_identity plus typed source schema/user versions, size and mtime.
- A bad metadata call rolls back. Valid REBUILD_REQUIRED -> BOOTSTRAP_REQUIRED
  change records REBASELINE_PENDING, feed_generation='rebase-'+new uuid,
  baseline_seq=prior latest_seq, failure_seq=prior latest_seq,
  failure_reason=previous reason and source identity/version/bytes/mtime.
- Existing source outbox rows and AUTOINCREMENT sequence are preserved:
  fixture retains seq=1 and writes seq=2 in newly armed generation;
  prev_seq remains 1, baseline_seq remains 1. No direct NORMAL cert is
  performed or implied. External baseline handshake and frozen writers
  are NOT proved.
- Synthetic _storage_reason: retained+incoming over 1GiB ->
  OUTBOX_BYTE_LIMIT. Artificial free space < FREE_RESERVE_BYTES ->
  FILESYSTEM_RESERVE; synthetic sufficiently free -> None. This uses
  mocked source path and shutil.disk_usage, NOT actual disk headroom.

## Isolated Rust verification
- R536 pure `r536_check_python_rebaseline` matches previous REBUILD_REQUIRED
  to new BOOTSTRAP_REQUIRED; requires old latest_seq=baseline_seq and new
  frame seq=baseline+1, matching generation and immutable source identity.
- It checks the copied failure reason and typed metadata; rejects state
  forgery, sequence rollback/jump, missing DB1 identity, reused generation,
  missing metadata. Returns **db2_certified=false, real_permit=false**,
  including on valid local evidence.
- R544 verifies the unchanged R376 source audit explicitly refuses an
  unapproved direct switch to NORMAL.
- 10 tests R536–R545 cover original Python source, negative cases and
  the hard authorization boundary. Code/test fixture is NOT production.

## What remains OPEN (not permitted for release)
- No independent source DB1 immutable snapshot identity validated against
  actual DB2 baseline. No frozen-writers handshake, schema baseline digest,
  durable DB2 restored source identity, generation-scoped remote ACK, or
  adversarial REBUILD_REQUIRED recovery test under real load.
- Existing private R366 source schema remains BOOTSTRAP_REQUIRED-only.
  This block does not migrate schema, loosen R376, or enable NORMAL.
- Python storage guard tested with fake disk availability only, not actual
  disk. Existing R506 source and DB2 model covers ONLY append events, not
  Python mirror 17-table upsert/delete and multigeneration replay.
- No installed PAPER binary update, restart, real DB1/DB2 mutation,
  database cleanup or external exchange action. REAL kill ENGAGED.
- Astra P0-01 PARTIAL; P0-02/P0-03 OPEN. Offline PASS != closeout.
