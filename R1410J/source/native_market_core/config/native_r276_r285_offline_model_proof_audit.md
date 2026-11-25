# SPOT-ARB Astra R276–R285 — offline applied in-memory model proof, 2026-10-22

## Safe scope and baseline
- Server spb-3-vm-4r2s; parent HEAD 8659f684fc2421ca79916a7f8749bfadc92cecfb; branch review/native-core-v3-r31-entry-permit-dispatch-20261008.
- Old 32 dirty/untracked paths SHA256 git porcelain 9d5a1608ce8912435df2269be7eeee6ac17bc178e3442dd137fd56c4beb6ebc4; preserve untouched, staging initially 0.
- Installed PAPER supervisor 2161721 ACTIVE with 0 restarts; child 2866189 at initial check, executable old paper_a342540 SHA256 10138620181ebfd37dcb76b79c091c70d3315b5c536045fd6ea5c258aa0dbd46. Production paper_max_daily_entries=500.
- Kill switch ENGAGED, real order path forbidden, no private API, DB1/DB2 writes, Telegram, or production service operations.

## Ten review directions and actual evidence levels
1. Existing R155 SupervisedStrategyPaper has *source-applied in-memory* effective_research_config_report, with PaperPortfolio quotas, strategy/market/temporal/fee model fields. The code was reused, not rewritten.
2. Original operator YAML source SHA256 98b377d7d8d98916048279c9c180fa918e4b26d8ea25fd87bc5114edf2f05731 remains not admissible to R155 full source PAPER due to general entry flags, analytics and scale-in guard. Do not bypass the guard.
3. A strictly offline candidate CLI `--r276-offline-model-proof` is gated by an explicit environment opt-in and ENGAGED kill switch. Returns before exchange metadata/network WS, durable checkpoint/journals/virtual entries/private API.
4. The candidate reads the original YAML and verifies exact build SHA against R157 local build proof, then constructs **only an in-memory test copy** by changing exactly six flags: general.trade_enabled true→false, general.paper_entries_enabled false→true, scale_in_hints.enabled true→false, analytics.enabled true→false, real_trial.enabled true→false, real_trial.autonomous_real_enabled true→false. Source file NEVER modified; six feature values changed means it cannot establish full original feature equivalence.
5. Original YAML is rejected by existing R258 admission before test model. The test copy is separately parsed into NativeTradingConfig and admitted by actual SupervisedStrategyPaper::new, which constructs PaperPortfolio and compiles the model/strategy policy.
6. Applied source quota 3000/day and 10 concurrent positions are confirmed both in strategy effective report and actual owned PaperPortfolio public fields. Existing compiled legacy cap is separately 500, parity flag false; no silent fallback.
7. Verified in-memory model values: entry required 42 bps, model costs 24.03 bps, quote budget 10 USDT, minimum lifetime 15s, exit 0.5 bps, net target 12 bps, reentry cooldown 120s, scan cooldown 120s. These are modeled strategy parameters, not exchange outcomes or authenticated trades.
8. Two R276 negative/positive tests protect against unexpected mutated original YAML SHA, forged deployed-ELF claim and false conversion of sandbox evidence into full equivalence; source model starts with entries=0 / open positions=0.
9. Pre-commit offline actual ELF execution output is /tmp/spotarb_r276_in_memory_model_20261022.json. Original source SHA 98b377d7..., sandbox SHA d59103da16a1e2138a07a9fda078bb823f11f0dd34063fa43ec98e9b6b6b36e0; candidate pre-commit ELF SHA 9760c5462af316dc7d31c5fdfee2d5ec1a905b28c279b5648c2d55f5ab0d2f87; receipt SHA256 0869dfe54c3637c651db40a01b815a29189a80df2f0459bc7e32b8d4595c5bea. This is local source/model provenance only; independent signed build and installed runtime application are not attested.
10. Targeted R276 Rust binary tests: 2 PASS/0 FAIL. Full Rust library suite: 1178 PASS/0 FAIL, 2 ignored. A requested full --bins sweep was blocked by tool safety and MUST NOT be described as executed; previous R275 96 binary PASS is historical only. A scoped commit and final Git/PAPER check are required; never install the candidate or turn on REAL.

## Remaining Astra release blockers
- Full original 160-field semantics NOT equivalent. Existing ledger 149 source research, 11 guard-only, 0 positively proven installed field applications (not evidence that 0 are used).
- Original operator AI/analytics/scale-in are NOT implemented as durable/effective source PAPER parity by this test. Telegram external delivery, user notifications and durable AI output remain unverified.
- DB1→DB2 actual causal forward, authentic venue state/fees, four-leg fills and UNKNOWN reconciliation, exact externally attested release remain P0 unfinished.
- A future isolated PUBLIC WS/PAPER candidate run requires a separate isolation contract: concurrency and resource budget, distinct identities/output paths and snapshots, no shared stop/checkpoint/DB paths, rate limits, kill gate, time bounded, no real orders. Do not start merely to produce PASS if source guard rejects original enabled features.

## Follow-up
Move to per-consumer decision and durable ledger receipts for actual full features, not repeating another 160-path source report. Retain original full YAML as the target. Only consider deployment after full parity, durable state and account-truth gates are signed off.
