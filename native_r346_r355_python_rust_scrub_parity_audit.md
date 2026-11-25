# SPOT-ARB Astra R346–R355 — executable Python/Rust scrub fixture parity and secret admission (2026-10-22)

Server spb-3-vm-4r2s, branch review/native-core-v3-r31-entry-permit-dispatch-20261008.
Start Git HEAD 98eb03410c22a36bbd837fe61649176cacdcfd3d, original 32 dirty/untracked paths porcelain SHA256 9d5a1608ce8912435df2269be7eeee6ac17bc178e3442dd137fd56c4beb6ebc4, staging=0.
Installed PAPER supervisor 2161721 ACTIVE / NRestarts=0; child 2902948 at preflight, installed paper_a342540 SHA256 10138620181ebfd37dcb76b79c091c70d3315b5c536045fd6ea5c258aa0dbd46, kill switch ENGAGED. No production service, DB, Telegram, exchange private APIs, or REAL order activity changed.

## Ten actual audit directions
R346 — recovered partial source after security-blocked previous attempt. Only src/native_paper_event_scrub_r336_review.rs changed and config/native_r346_python_scrub_fixtures.json untracked; 32 old paths untouched, staged=0.
R347 — read previous fixture JSON from previous handoff via conversation files; exact 39 fixtures, 60KB, copied intact into review config. Source fixture SHA can be determined at next checkpoint. Original fixtures were *transcribed*, not yet guaranteed to match Python execution.
R348 — added Rust test r346_python_scrub_fixture_set_all_39 with expected redaction/rejection comparisons. Initially 39/39 PASS on inherited Rust sanitizer.
R349 — independently imported exact /home/tmpadmin/spotbot/ai/schemas.py (without invoking Python trading operations) and executed its actual scrub() for all 39 synthetic fixtures. Python oracle: 39/39 PASS; no mismatches. This is bounded fixture parity, NOT all-input parity and NOT full Event.record.
R350 — identified missing lower numeric exponent bound in R336 sanitizer and implemented decimal adjusted-digit comparison from serde_json::Number source representation, allowing adjusted exponent inclusive [-30,30] or zero, rejecting other finite values.
R351 — first R346 numeric-boundary test FAIL: default serde_json 1.0.114 parser yielded parsed 9.999999999999999e-31 for lexical 1e-30, unlike Python. This diagnostic was not hidden. Switched *review crate only* Cargo.toml serde_json features to raw_value + float_roundtrip, no Cargo.lock package update or installed PAPER change. R346 numeric boundary, fixture parity and synthetic writer secret test then all PASS.
R352 — added isolated writer method append_paper_with_known_secrets_r346(...,known_secrets). The original append_paper delegates with EMPTY slice; this is a research API, **NOT configured production-secret retrieval**. Uses r336_scrub before JSON serialization and digest, then atomic event+outbox SQLite insert. Tests with *synthetic* unknown-pattern secret proved durable JSON contains [REDACTED], not the raw value; duplicate ID canonicalized under known-secrets results in Duplicate without a second row.
R353 — reject source event envelope collisions continues through existing R336 and R316. No actual Python event envelope implemented: source+run_id, schema version, validation of verification, original category list, event correlation and PnL truth remain OPEN.
R354 — full offline regression after float_roundtrip: Rust library 1200 PASS/0 FAIL/2 ignored, all Rust binaries 98 PASS/0 FAIL across 81 binary groups, total unique 1298 PASS/0 FAIL/2 ignored, exit code 0. Initial targeted R346=3 PASS/0 FAIL; the 39 fixture cases count as ONE Rust unit test and ARE INCLUDED in 1200 library PASS.
R355 — scoped commit only five intended files if full suites PASS; preserve original 32 dirty/untracked paths and staging 0, and postflight PAPER & ENGAGED kill switch. No installed deployment, no source PAPER runtime, no real.

## Safety and important limitations
- Python exact 39-fixture oracle and Rust fixture parity are strong but bounded to prepared synthetic examples, not fuzz/exhaustive Unicode regex or all numeric serialization representations.
- float_roundtrip changes parser behavior broadly across code; full library AND binaries are required for review, and installed PAPER binary hash remains independent.
- Research secret passing only via explicit method parameter. No actual configured credentials are read. Original append_paper still supplies [] and can persist arbitrary unrecognized secret text; do not wire it into live analytics.
- R296/R316/R336 SQLite writer is an isolated PAPER-only prototype with only three categories and simplified outbox, not Python collector parity. No stable source Event.record full envelope and no powerfail / SIGKILL recovery proof.
- REAL remains forbidden; source research does not prove deployed YAML application or authenticated four-leg fills/fees/recovery.

## Next R356+
1. Test original Python Event.record schema/identifiers/verification on intentionally fake and safe test cases and implement a standalone Rust pure envelope validator; no trading.
2. Add arbitrary synthetic-secret property / Unicode regex fuzz and path-case negative tests. Prefer conservative rejection over leaking uncertain credentials.
3. Prove source-owned Paper event safety and durable writer with authentic Python schema/outbox generation protocol and read-only exact input.
4. Separate high-cost process SIGKILL/recovery test using verified isolated directories, not installed PAPER.
5. Run full tests, preserve 32 old paths, never change LIVE or switch kill gate.
