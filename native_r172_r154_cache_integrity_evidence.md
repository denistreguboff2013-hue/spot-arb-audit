# Astra R172 — R154 optional public ticker cache integrity

Date: 2026-10-21. Scope: Rust source review; no production cutover.

## Original defect
In `src/bin/native_paper_live_portfolio_v1.rs`, R154 wrote an inbound
`R154TickerSample` to the per-venue cache before R126 topology, R123
payload-clock, and R124/R130 continuity checks finished.
A rejected stale or malformed message could therefore replace last-good cache.

## New decision path
1. Parse via existing R123 `parse_ws_ticker` before mutating cache.
2. Check source timestamp, receive timestamp, connection epoch, sequence.
3. Ignore exact duplicates without replay advancement.
4. Reject older epochs/sequences/timestamps without overwriting last-good.
5. Reject changed payload at the identical sequence.
6. Demote public readiness on an accepted newer connection epoch.
7. Roll back prior cache sample when topology or cold-boot continuity denies.
8. Keep this opt-in R154 diagnostic; no order, private API or trading permit.

## Tests verified
- Bin: 16 PASS, 0 FAIL; four NEW R172 negative/rollback unit tests.
- Lib: 1115 PASS, 0 FAIL, 2 ignored (pre-existing).
- `git diff --check` PASS.
- Historical 32 Git status records unchanged when excluding the single R172
  source modification; baseline SHA256 `9d5a1608ce8912435df2269be7eeee6ac17bc178e3442dd137fd56c4beb6ebc4`.

## Proof boundary
No installed binary or PAPER supervisor was restarted or updated. Neither
market-data peer attestation nor exchange-order truth is established here.
R155 already had the related R170 guard; this patch does not replace it.
144/160 config fields are research-evidenced; installed YAML provenance 0/160.
