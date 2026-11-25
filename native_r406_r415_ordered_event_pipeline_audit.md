# SPOT-ARB R406–R415 — isolated PAPER source Event.record → SQLite writer (2026-10-22)

Server spb-3-vm-4r2s. Review repo /home/tmpadmin/native_rust_registrar_review_20261002/c18_floor_checkpoint_20261006/review.
Starting HEAD aece1aef9a7c0ad07db9fde10b35ac46f3d940d9.
Original 32 historical dirty/untracked paths SHA256 9d5a1608ce8912435df2269be7eeee6ac17bc178e3442dd137fd56c4beb6ebc4; staging 0.
Original PAPER supervisor 2161721 ACTIVE, no restarts at baseline; child 2944271, installed ELF SHA256 10138620181ebfd37dcb76b79c091c70d3315b5c536045fd6ea5c258aa0dbd46. Real kill switch ENGAGED; disk3 ~5.4 GB free. No real trading or installed PAPER changes.

## Scope, numbered review areas — not claims of ten deployed features
R406 — Rechecked Git baseline and PAPER, kept old 32 paths and service intact. Two untracked draft files were prepared earlier.
R407 — Original Python ai/schemas.py Event.record invoked on TWO fake events (decisions and errors). Exact ordered inner payload/verification, canonical outer JSON and SHA256 saved in config/native_r406_event_pipeline_oracle.json. No secrets or private APIs read.
R408 — Included src/native_ordered_event_pipeline_r406_review.rs in isolated R296 namespace. Verifies raw ordered payload/verification JSON is semantically the same as the supplied event before admission.
R409 — Rejects duplicate raw JSON keys or floats not certified as Python-compatible; PAPER-only Event.record validation through R356, with known synthetic secret redaction before serialization. Checks semantic equivalence with existing R336 scrub.
R410 — Reconstructs inner JSON byte strings with preserved key order, forms outer Python collector record JSON and SHA-256 via R386 bounded nonfloat subset; refuses unsafe envelope sizes.
R411 — Updated isolated R366 research writer event source_record_json/content_hash to Python spaced/sorted canonical JSON rather than Rust compact serialization.
R412 — Updated isolated R366 research outbox payload_json/payload_hash to Python mirror compact/sorted canonical JSON.
R413 — Updated R376 isolated verifier to recalculate both hashes using distinct Python source canonical formats. No DB1/DB2 writer or consumer changed.
R414 — Three R406 targeted Rust tests PASS: two Python-authored fake PAPER events including redaction → SQLite → cold reopen/durable SHA; raw event/verification spoof and REAL denial; duplicate JSON keys and float denial. Historic R366 5 PASS, R376 6 PASS, R396 real child SIGKILL 1 PASS after changes. Targeted results are included in full library suite counts.
R415 — Full offline Rust test completed (exit code 0): library 1229 PASS / 0 FAIL / 2 ignored; binaries 98 PASS / 0 FAIL across 81 bin groups; unique total 1327 PASS / 0 FAIL / 2 ignored. Three new R406 tests are already included in library count. Scoped commit only six tested R406 files, excluding uncompiled draft after_image source, followed by Git/PAPER/hash/disk postflight. No installation into active PAPER.

## Explicit incomplete work
- Separate draft src/native_isolated_after_images_r406_review.rs authored with proposed SQLite row-reader and two tests. Editor BLOCKED include step. It is UNCOMPILED, UNTESTED, UNCOMMITTED and must not be counted as completed or deployed. Leave as untracked candidate for later review.
- Outbox operations reference event IDs and source hash only, with NO complete source row after images/after_hash in the actual R366 transaction. DB2 source-to-sink protocol compatibility is NOT proven.
- R396 ordered scrub handles vetted synthetic source JSON only; no runtime configured-secret collection attestations.
- R386 excludes floats. Python numeric/Decimal/Unicode edge cases remain uncertified.
- R366 full scan before each append is O(N); not production throughput-ready. SQLite directory/symlink race hardening also open.
- Full original 10 Python category tables, mirror control REBUILD_REQUIRED, source DB1→DB2 consumption, long-lived witness, retention and disk pressure are NOT implemented here.
- Isolated SIGKILL does not prove full bot recovery or historical four quarantined virtual position reconciliation.
- Installed PAPER old ELF, source config 160 fields not individually runtime attested, P0-02/P0-03, real fill/close/fees, external witness open. REAL blocked.

## Next R416–R425
- Integrate row-after image draft ONLY through normal authorized editing when permitted. Read full row from private SQLite, use Python after() canonical/hash.
- Have R376 verifier compare per-row after image and SHA256 with actual stored SQLite row including two-hash forged-body attack.
- Add crash / WAL pressure / disk-full controlled private tests and performance; no live binary change without signed immutable PAPER-only release package, rollback and quarantined-position proof.
