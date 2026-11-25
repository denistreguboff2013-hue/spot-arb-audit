# SPOT-ARB R336–R345 — bounded Rust PAPER event scrub, 2026-10-22
Server spb-3-vm-4r2s. Branch review/native-core-v3-r31-entry-permit-dispatch-20261008.
Parent Git HEAD 07fdb0795c7098c2f0d8ec3443862711bfe01412, 32 historical dirty/untracked paths, porcelain SHA256 9d5a1608ce8912435df2269be7eeee6ac17bc178e3442dd137fd56c4beb6ebc4; no staging at preflight.
Installed PAPER Active, supervisor PID 2161721, NRestarts 0; the child had naturally rotated to PID 2902948. ELF SHA256 10138620181ebfd37dcb76b79c091c70d3315b5c536045fd6ea5c258aa0dbd46. Kill switch ENGAGED. No private API, REAL, working PAPER/DB/Telegram changes.

## Actual work, R336–R345
1. Rechecked R325 checkpoint/Git and active PAPER before source edit. Everything matched.
2. Re-read Python ai/schemas.py::scrub and Event.record contracts. Source semantics: sensitive field-name normalized key matching, synthetic auth token formats, in-string inline labels, HTTP URL auth removal, PEM marker, known configured secrets, traversal 512 nodes / max depth 8, 64 array items, 128 object fields, max key length 80, max string length 4096, finite bounded numbers, and event 16KiB cap.
3. Existing native_market_core source contains no shared Rust regex/sanitizer module or Cargo regex dependency. Retained Cargo.lock unchanged and implemented a bounded no-network standalone research helper.
4. Added src/native_paper_event_scrub_r336_review.rs. Recursive typed serde_json Value scrub with explicit known_secrets slice for test use, markers/recognized auth patterns, bounds and fail-closed errors. NOTE: hand-authored ASCII recognition is NOT proven byte-for-byte equivalent to all Python Unicode/regex behavior.
5. Included it in isolated existing R296 analytics writer only, no installed PAPER wiring.
6. Isolated append_paper now refuses envelope shadow keys, scrubs first, then serializes/limits/hashes/writes one atomic event+outbox pair to its private /tmp SQLite. No exchange/client access, no production DB writes.
7. Existing R316 persisted-row verifier additionally refuses unsanitized stored payloads. It remains local-only: an attacker who can rewrite all rows and recompute hashes can bypass local digest checks. No external witness.
8. New Rust tests: sensitive key and nested short-circuit, synthetic Telegram/GitHub token patterns, URL passwords, password/api-key inline labels, PEM markers, explicit synthetic known-secrets, string/list/object/numeric/depth/traversal caps, actual sanitized insert/outbox hash, denial of shadowed envelope/oversized nested input. First expanded targeted run had one failed test due to incorrect fixture counting (expected >512 nodes, actual 511). Corrected to 513 and added 512-node positive boundary. Seven targeted R336 tests PASS after correction. Preliminary error retained in audit history.
9. Full offline regression executed: 1197 library PASS, 0 FAIL, 2 ignored; 98 binaries PASS, 0 FAIL across 81 binary test groups; unique total 1295 PASS, 0 FAIL, 2 ignored. Seven targeted R336 tests already included in library count. Only scoped source/audit commit after preserving old 32 paths.
10. Exact 39 historical fixtures in SPOT_ARB_R326_PYTHON_SANITIZER_FIXTURES_2026-10-22.json were NOT all run under Rust: those were local transcription-based fixtures with explicit rust_validated=false. These 7 tests cover a high-value subset, NOT full Python sanitizer/Event.record parity.

## Remaining risk and release blockers
- The current Rust writer calls r336_scrub with an EMPTY known-secrets slice; the production system's configured secret values are NOT injected. This means arbitrary secrets not matched by implemented patterns may leak. Do not wire into production.
- The source Event envelope is still absent: verification JSON, schema_version=1, cross-references/correlation, exchange verification truth, 10 categories/stages, category tables/collector_runs/retention, Python's mirror-outbox generation and canonical payload source hash are NOT implemented here.
- No complete 39-fixture Rust parity, no Unicode regex fuzzing, no production throughput cost validation (R316 scans all persisted rows per insert), no process SIGKILL crash test, no signed CI/attested runtime and no deployed PAPER per-field YAML evidence.
- R296 writer remains isolated PAPER-only with 3 categories (signals/decisions/errors). It is NOT a replacement for Python AI collector. Old PAPER remains cap 500 vs operator YAML 3000.
- No P0 DB1→DB2 causal forward, no authenticated account census/fills/fees/UNKNOWN/four-leg truth. REAL still strictly forbidden.

## Next R346+
1. Run all 39 expected scrub fixtures against actual compiled Rust scrub and compare expected outputs/rejections (mind float, Unicode and field iteration differences); add mismatches as tests.
2. Add explicit production-secret admission contract (do NOT fetch secret values to logs), strict source envelope validation, verification source gates and category-specific schema before runtime integration.
3. Reuse Python exact mirror outbox schema and transaction model; fail closed on mismatch.
4. True child-process SIGKILL/SQLite WAL recovery only in isolated owned /tmp path; no old supervisor or DB writes.
5. Full cargo --offline --locked lib/bins after code changes; preserve the old 32 Git paths, no service or REAL changes, report each real blocker.
