# R416–R425 — isolated SQLite source after-images and PAPER release gate

Date: 2026-10-22. Scope: Native Rust Core research branch; synthetic own `/tmp/astra_r366_isolated_*` databases only.

## Source changes (not installed)
- R296 namespace includes the previously uncompiled R406 after-image draft, now reviewed and tested.
- R366 captures actual SQLite rows inside the same `BEGIN IMMEDIATE` transaction, after the INSERT, before the outbox write and COMMIT.
- Each source operation carries exact `table, op, key, row, after_hash`; hash uses the reviewed R386 Python mirror canonical subset.
- SQLite `INSERT OR IGNORE` for an existing correlation does not create an invented attempts operation. A new attempt emits a single `insert`, verified with sqlite3_changes.
- Events + category + optional new attempt + mirror_outbox + mirror_control remain in one transaction. Existing real/verified category admission blocks remain intact.
- R376 re-reads actual source rows for every outbox operation, checks exact equality including after_hash, and rejects orphan attempts; existing seq/prev_seq/hash/bytes/generation/state guards remain.
- Original Python source of truth: `/home/tmpadmin/spotbot/ai/mirror_outbox.py::after()`; fixture `native_r396_python_ordered_and_after_oracle.json`.
- Implementation is **not** full Python collector parity. Only R366 supported PAPER `decisions` and `errors` category tables; no DB2 delivery.

## Evidence expectations
- Targeted tests: original R366, R376, R406 Python after fixtures; additional shared-correlation, tamper, orphan, and coordinated two-hash forgery.
- Private SIGKILL child: before and after COMMIT, reopened WAL audit with full after-images.
- One `cargo test --offline --locked --lib -q && cargo test --offline --locked --bins -q` after final code.
- Before accepting commit: 32 historical Git paths unchanged, staging scoped to own files only, no cleanup/reset/stash.
- Rust tests confirm SOURCE and OFFLINE TEST; **not** INSTALLED PAPER, DB2 SYNCED, REAL, or full 160-field provenance.

## Known limitations / STOP
- Python mirror allows broader table/category schema than this isolated R366 subset. Do NOT route production DB1/DB2 or Python collector into this writer.
- Exact original Python float/decimal semantics are not implemented; unsupported floats deny. R426–R435 must expand under checked schema and secret admission.
- Isolated R376 full source/outbox scan is O(N); no large-scale p95/p99 performance certification, disk full, pinned WAL reader, ACK-lost replica, or long-duration DB1→DB2 causal replay has been made.
- Source-local checking is not an independent antirollback witness; consistent coordinated source+outbox rewriting requires separate authenticated evidence.
- No installation, PAPER process restart, Telegram external send, REAL private exchange call, or switch change in this block.

## PAPER-only staged release requirements (NOT APPROVED)
1. Finish R426–R435 all 10 Python durable categories, source lineage, secrets, retention, WAL and storage-failure behavior.
2. Prove the original operator YAML 160 per-field source/effective receipt; reconcile 149 source-researched/11 guard-only, 3000/day source versus old ELF 500/day, WS 235 historical versus 234 installed.
3. Independently build an immutable PAPER-only candidate from pinned Cargo inputs and a precise source manifest, record ELF SHA and no real order route.
4. Test isolated source/DB2 replica replay, stale/gapped payload, restart, failpoints, open virtual-position preservation, and Telegram mocks.
5. Record installed old ELF backup SHA, clear rollback plan and non-destructive migration/position handling.
6. Obtain separate operator go/no-go for installed PAPER cutover. Verify exact resulting process ELF, effective YAML provenance and runtime receipts; stop and roll back on mismatch.
7. Keep historic unresolved cross-day hold and four separate storage-guard virtual positions quarantined; never silently auto-release.
8. Keep REAL kill switch ENGAGED and original Astra P0-02/P0-03 unclosed.

Status: source review and targeted tests may pass independently; full regression / exact commit / postflight must be recorded in final operator report.
