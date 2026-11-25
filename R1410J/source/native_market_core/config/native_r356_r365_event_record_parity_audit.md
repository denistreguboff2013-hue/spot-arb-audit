# SPOT-ARB Native Rust Core v3 — R356–R365, PAPER Event.record envelope bridge

Date 2026-10-22. Server spb-3-vm-4r2s; review repo /home/tmpadmin/native_rust_registrar_review_20261002/c18_floor_checkpoint_20261006/review; branch review/native-core-v3-r31-entry-permit-dispatch-20261008.

## Safety and preflight
- HEAD before batch: ea5370f002ee7f7626d09d7b77c2db46bc181372.
- Original 32 pre-existing dirty/untracked paths unchanged; historical Git porcelain SHA256: 9d5a1608ce8912435df2269be7eeee6ac17bc178e3442dd137fd56c4beb6ebc4. Initial staging=0.
- Installed PAPER supervisor PID 2161721 ACTIVE, NRestarts=0, current child PID 2919927, installed ELF SHA256 10138620181ebfd37dcb76b79c091c70d3315b5c536045fd6ea5c258aa0dbd46; no installed binary replacement.
- Real kill switch ENGAGED, all private order APIs unused; real trading is NOT authorized.
- Disk3 /mnt/spotarb_disk2 available 5.4GB, 82% used after safe incremental-cache cleanup. No filesystem cleanups in this batch, no service restarts, production DB or PAPER edits.

## Ten review directions
R356 — Read the exact installed Python source /home/tmpadmin/spotbot/ai/schemas.py and compare Event.record: modes, categories, stages, reason codes, event ID validation, optional correlation and comparison dependencies, time, payload/evidence, scrub, verification authority, 16KiB envelope.
R357 — Survey existing Rust native_paper_event_scrub_r336_review.rs and native_isolated_analytics_collector_r296_review.rs; no existing Event.record envelope clone. Reused existing bounded scrub (39 Python fixtures previously matched).
R358 — Added new pure Rust source-only native_paper_event_envelope_r356_review.rs as include in isolated R296 research namespace, NO writer integration, exchange network, Telegram, private APIs, dispatch, or production PAPER modifications.
R359 — Explicitly PAPER-only validation: category and stage allow lists, source/run/event mandatory IDs, optional IDs and comparison dependencies, execution correlation, reason codes, UTC timestamp, payload shadowing and known-secrets in IDs, and status normalization mismatch→contradicted.
R360 — Any PAPER status verified is forbidden, even if crafted evidence claims exchange_reconcile. The source Python demands REAL with exchange-authenticated bilateral fill/state proofs; current research scope refuses REAL entirely. is_exchange_verified always integer 0.
R361 — Existing R336 scrub sanitizes BOTH PAPER payload and verification before their JSON string formation; conservative approximate envelope size bound. No SQL or durable append. No exhaustive encoding parity and not complete Python Event.record.
R362 — Six native Rust targeted safety tests PASS for permitted event/defaults/redaction, invalid stage/reason/mode, future time, identifier leaks, correlation dependencies, shadow comparison and forbidden verified.
R363 — Generated 12 synthetic test Event values by calling the **actual installed Python ai/schemas.py::Event.record** in a read-only Python process (5 accept / 7 reject) and saved the accepted expected record plus all inputs in config/native_r356_python_event_record_oracle.json; no real exchange credentials or order calls. Added seventh targeted Rust test comparing records field-by-field, normalized JSON content for payload/verification. 12/12 parity PASS, including 7 negative cases.
R364 — Full offline Rust lib suite 1207 PASS, 0 FAIL, 2 ignored; all binary suites 98 PASS, 0 FAIL across 81 binary test groups, exit code 0. Unique total 1305 PASS, 0 FAIL, 2 ignored. Seven targeted R356 tests, including 12-case Python oracle inside one test, are INCLUDED in 1207 lib PASS.
R365 — Scoped Git commit only new Rust source include, original R296 research include update, JSON oracle fixture, and this audit, after tests. Verify 32 historical paths and installed PAPER unchanged, kill ENGAGED, staging empty. No production deployment.

## Material gaps
- Pure PAPER Event.record subset: Python supports paper/test/replay/real and REAL verification gates, current module intentionally rejects non-PAPER modes. Source validation is NOT exchange evidence or a trading instruction.
- Rust sanitizer still lacks automatic configured-secret input from production. A research method accepts explicit synthetic known secrets; original path passes empty array; do not wire into PAPER until resolved.
- Source Python Event.record category tables, exact JSON whitespace/byte encoding, all Unicode regex cases, the 16KiB source-envelope boundary and full verification fields remain incompletely audited. Current bound is conservative and JSON-meaning compared for 12 fixtures, not byte-identical.
- Research SQLite R296 schema still 3 categories; the R356 pure validator handles 10 source categories but is NOT a durable event writer.
- True process SIGKILL/powerloss crash test, authentic SQLite outbox protocol, retention and event source-causal recovery NOT closed.
- Installed PAPER still old ELF with source YAML 0/160 positively per-field attested; no REAL authority, no authenticated four-leg exchange truth.
- Historic cross-day quarantine remains durable. Isolated PAPER started automatically after storage guard recovery, but does not settle old four virtual positions.

## Next R366+
1. Add wider original Python Event.record field/UTF8/size parity cases and reject oversized/unsupported event shapes.
2. Model real Python collector transaction and mirror_outbox source schema in isolated Rust, without writing production DB.
3. Add verified isolated-process SIGKILL checkpoint/journal causal recovery test only when safe.
4. Keep all 32 Git paths and installed PAPER preserved; never turn on REAL just because research tests PASS.
