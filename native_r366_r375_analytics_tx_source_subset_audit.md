# SPOT-ARB R366–R375: Python collector transactions -> isolated Rust subset (2026-10-22)

Repository /home/tmpadmin/native_rust_registrar_review_20261002/c18_floor_checkpoint_20261006/review
Parent HEAD aa7ab94370d6001b9bebe265f118b2ae1e50d318
32 original Git dirty/untracked paths porcelain SHA256 9d5a1608ce8912435df2269be7eeee6ac17bc178e3442dd137fd56c4beb6ebc4; staged 0.
Installed PAPER supervisor 2161721 ACTIVE, NRestarts 0; binary paper_a342540 SHA256 10138620181ebfd37dcb76b79c091c70d3315b5c536045fd6ea5c258aa0dbd46. Kill switch ENGAGED, not modified. Disk3 free approx 5.4GB at preflight.

## Ten source findings and tasks

R366. Read exact Python ai/collector.py open_store schema: mode, attempts, events, category rows, WAL, foreign key, DB pressure.
R367. Read Python append_record: duplicate/ID conflict via canonical content SHA, correlation_mode uniqueness, atomic event + category insert and ordered operations.
R368. Read Python ai/mirror_outbox.py record source: transactional prev_seq, feed_generation, schema_generation, payload_hash, mirror_control. DB2 never receives a message automatically merely because outbox row exists.
R369. Reused existing Rust R147 Db/Stmt/Tx SQLite FFI and R356 PAPER Event.record pure validation; no second engine or private API.
R370. Introduced src/native_isolated_event_outbox_r366_review.rs as include inside existing R296 research namespace. Only private /tmp/astra_r366_isolated_* parent and astra-r366-event-outbox.sqlite, owned restrictive permissions; no production path.
R371. Isolated test tables attempts, events, decisions, errors, mirror_outbox, mirror_control; user_version=366 is intentionally DIFFERENT from real Python schema, only two categories.
R372. PAPER-only atomic transaction inserts attempt, event, category projection and source-outbox row and updates control sequence. Interruption before outbox rolls back automatically via existing Rust Tx Drop.
R373. Identical duplicate event ID returns Duplicate; changed contents returns Conflict. Neither changes stored count/sequence. Prevalidated R356 source event uses synthetic data.
R374. Five targeted R366 tests PASS/0 FAIL: atomic write/reopen, duplicate/conflict, simulated failpoint, real/unsupported refusal, path denial. Full offline Rust library: 1212 PASS, 0 FAIL, 2 ignored. Full Rust binary suites: 98 PASS, 0 FAIL across 81 binary groups. Unique total: 1310 PASS, 0 FAIL, 2 ignored, exit code 0. The 5 targeted tests are already counted inside the 1212 lib PASS.
R375. Commit exactly new R366 Rust file, R296 include and this audit, if regressions pass and 32 old paths unchanged; no source PAPER runtime wiring, service replacement, DB1/DB2 or REAL.

## Remaining non-parity and release blockers
- Outbox operations currently omit Python mirror_outbox.after full source row image/after_hash. They cannot be applied by DB2 protocol unchanged. Current audit checks counts/sequence/source-ID hints, NOT cryptographically recalculated row/payload chains; further source edit was blocked and not bypassed.
- Rust record digest uses Rust compact JSON; Python content_hash uses sorted keys and default-spaced JSON. Byte-for-byte digest compatibility NOT shown.
- Categories, column projections, schema generation and migration, actual Python transaction limits (MAX_TRANSACTION_BYTES, FREE_RESERVE_BYTES), WAL checkpoint/backpressure, maintenance lock, approval ledger, collector runs, recovery, full 160-field usage remain OPEN.
- Failpoint is a simulated rollback, not a SIGKILL or powerfail. Nothing proves independent DB2 mirrored causal delivery.
- Source R356 accepts synthetic PAPER envelopes; installed PAPER uses an older ELF, new candidate is NOT installed. Kill switch ENGAGED. Four historical unresolved virtual PAPER positions remain quarantined.
- New research reader/writer only, no real PnL, orders, private exchange API or Telegram sends. REAL not authorized.

## Next R376–R385
Independent audit of each outbox payload SHA and causal prev_seq, canonical Python Event.record hashes, full row after-images and REBUILD_REQUIRED protocol. Then process SIGKILL/write-crash test on isolated filesystem, never on installed PAPER or production DB.
