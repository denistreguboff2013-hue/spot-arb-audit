# SPOT-ARB Native Rust Core v3 | R216-R235 combined audit, 2026-10-22
Preflight HEAD: 15c4b7af5f1ed1a9db7be924b4bbce752c8c87e2.
Protected 32 historical dirty/untracked paths: porcelain SHA256 9d5a1608ce8912435df2269be7eeee6ac17bc178e3442dd137fd56c4beb6ebc4. Staging initially empty.
Installed PAPER: supervisor 2161721 active, NRestarts=0; paper_a342540 SHA256 10138620181ebfd37dcb76b79c091c70d3315b5c536045fd6ea5c258aa0dbd46. DO NOT change or restart. REAL authorization remains denied.

## 20 audit work units. They are NOT 20 new implementations.
- R216 NEW: R195 persisted research PAPER queue uses checked_add for HMAC-valid event totals. Two locally signed item counts (usize::MAX and 2) must return DENY without overflow/panic.
- R217 NEW: HMAC-valid accepted_session alone, even without queued sessions, requires canonical session_X_Y format. Unparseable accepted identity is DENY.
- R218 NEW: clean HMAC-valid acknowledged older session with empty pending queue can add only a strictly newer session. Same ID replay fails.
- R219 REVIEW: preserve Git source baseline and original 32 historical paths, no reset or blanket stage.
- R220 REVIEW: distinguish installed PAPER ELF hash and source checkout; source changes never imply deployed features.
- R221 REVIEW: original 160 YAML leaves still 149 research source-evidenced and 0/160 per-field installed provenance PROVEN, not zero applied.
- R222 REVIEW: eleven unsupported original knobs remain fail closed under existing R174 guard; no invented consumers.
- R223 REVIEW: compiled legacy PAPER cap 500/day differs from distinct R155 source-owned research model cap 3000/day; no runtime quota cutover.
- R224 REVIEW: R153 source-owned sealed planned handoff, source SHA, fresh boot and R178 source drift guard not restaged.
- R225 REVIEW: R181-R190 legacy PAPER checkpoint FD/lock/sequence/economics guards reused as existing code.
- R226 REVIEW: R167 source WS width=40 research; installed PAPER status historically 234/234 and baseline 235, exact symbol diff not proven.
- R227 REVIEW: R50 AI bridge is read-only advice, not authenticated trade or durable AI owner.
- R228 REVIEW: analytics storage advice has can_write_db=false, does not create a production writer.
- R229 REVIEW: Telegram opt-in outbox, claim unknown and ACK modules exist; no new send or production delivery evidence.
- R230 REVIEW: R196 source DB1 atomic batch and R147/R180 isolated DB1/DB2 replay and partial repair tests remain offline.
- R231 REVIEW: R197/R198/R205/R206 shape-only venue fill/census/zero-window auditing not private authenticated exchange truth.
- R232 REVIEW: R208-R211 source-shaped OKX feeCcy, fee-day and archived receipts are not provider-attested fee completeness.
- R233 REVIEW: R191 mock/same-host witness disallowed; R199-R203 signed mock receipts not independent deployed latest floor.
- R234 REVIEW: kill switch, permissions and no-live-authorization gates unchanged; zero private exchange or Telegram calls.
- R235 RELEASE CHECK: complete Rust cargo test --offline --locked --lib and --bins; commit only two owned files after PASS, preserve 32 paths and installed service.

## Exactly changed files
src/native_persisted_paper_queue_r195_review.rs: checked event sum, validated accepted_session, 3 regression tests R216/R217/R218, synthetic isolated /tmp only.
config/native_r216_r235_batch_audit.md: 20-unit source/deployed evidence ledger.
No other R219-R235 Rust production modules were created. No claim of full closeout on any of the thirteen original Astra findings from merely passing offline tests.

## Next session R236-R255
Use a high-value approach: stop increasing R numbers for trivial source-only tests if no deployed proof improves. Prioritize independent source-to-running-binary and 160-field receipts, actual WS instrument membership diff 234 versus 235, deployed integration/cutover plan in isolated PAPER, unknown/partial fill and fee authority, true external witness, durable AI/analytics owner, and optional Telegram ACK transport with separate operator approval. Ensure REAL remains forbidden.
