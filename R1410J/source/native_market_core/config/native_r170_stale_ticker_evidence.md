# SPOT-ARB R170 — R155 stale ticker pre-insert guard (research source)

Parent Git: 400729bba512494b6f3e78d2216394679ac205d8
Date: 2026-10-21; device spb-3-vm-4r2s.

## Scope
Only review source `native_r155_source_paper_lane_review.rs` in `native_market_core`.
No deployed PAPER, DB, systemd, production YAML, secrets or private exchange endpoints touched.
This is an offline source-owned ticker lane, not authenticated socket input.

## Before (documented R155 defect)
`SourcePaperLaneR155::on_ticker` inserted untrusted `Sample` into spot/future
cache before running R126 topology and R124/R130 continuity checks.
An out-of-order/older ticker could overwrite valid cache even if R124 later rejected.
R162 reconnect probe did not close this specific R155 caller defect.

## What changed
1. Validate each inbound ticker against R123 `parse_ws_ticker` (payload, route, source time) BEFORE insert.
2. Compare high-water epoch, sequence, receive/source timestamp with previous same venue and base.
3. Reject old epoch, decreasing seq, backdated source/receive timestamps WITHOUT replacing cache.
4. Accept exact duplicate same epoch+seq only as no-op; reject mutated raw with same seq.
5. On new epoch revoke entry readiness before any potential rewarm; R130/R124 remain mandatory.
6. Restore prior source cache if pair topology/continuity fails after provisional insert.
7. Never change portfolio state on ticker rejection and never authorize REAL orders.

## Evidence and boundaries
Four new `r170_*` regressions under native_r155_source_paper_lane_review::tests:
- old epoch or sequence retains high-water and ready;
- exact duplicate no new maturity, rewritten same sequence denied;
- malformed new epoch does not poison cache;
- new epoch degrades ready, both venues advance, rewarms.

Targeted `cargo test --lib r170_`: 11 PASS (four new + seven prior matching tests).
Full library: 1115 PASS, 0 FAIL, 2 ignored.
Binary `cargo test --bin native_paper_live_portfolio_v1`: 12 PASS, 0 FAIL.

Remaining limitations: no production cutover; no independent TLS/socket identity; no
authenticated 235-symbol whole-fleet WS trace; no persisted cross-restart
epoch floor; no proof of installed source YAML application.
P0-02/P0-03 remain OPEN and REAL prohibited.

Current research YAML P1-06 coverage remains 144/160, with 16 not evidenced
after R169. R170 adds no YAML coverage claim.
