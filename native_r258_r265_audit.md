# SPOT-ARB R258-R265 — provider exclusion + dual PAPER admission, 2026-10-22

Scope: one substantive new Rust offline diagnostic with three tests; remaining reviewed source/installed evidence. No live trading, private exchange queries, Telegram sends, service restart, deployed binary/DB/config changes.

## R258: public provider-specific exclusion reasons (new factual observation)
At approximately 2026-10-22 04:13:22-04:13:23 UTC, PUBLIC unauthenticated metadata GET only:
- Binance USD-M `/fapi/v1/exchangeInfo` — 931 instrument records.
- OKX `/api/v5/public/instruments?instType=SPOT` — 1183 instrument records.
- CAT: Binance `CATUSDT` TRADING but `contractType=TRADIFI_PERPETUAL` (excluded by current native_market_core::market_metadata::parse_binance). OKX `CAT-USDT` SPOT live.
- PROMPT: Binance `PROMPTUSDT` SETTLING, contractType PERPETUAL (excluded by required TRADING). OKX `PROMPT-USDT` SPOT live.
- CT: Binance `CTUSDT` TRADING, PERPETUAL; OKX `CT-USDT` SPOT live. Remains eligible in current 234 intersection.
- The existing Rust test `same_ticker_tradifi_future_is_excluded_from_crypto_spot_intersection` already covers TradFi; pre-existing parser also excludes SETTLING. Do not redevelop these.
- The R257 historical 235→current 234 diff remains CAT/PROMPT old-only, CT new-only, 233 common.
- Public metadata results are point-in-time; not venue-signed delisting proof and not installed PAPER exact subscription membership.

## R259-R261: NEW Rust dual-profile admission diagnostics — not new trading functionality
- Added `src/native_r258_source_admission_diagnostics.rs`, exported from `src/lib.rs`.
- `review_two_paper_admissions_r258()` inspects original parsed `NativeTradingConfig` without disk/socket writes or secrets.
- It does not replace the old guards: calls actual `SupervisedStrategyPaper::new` on a fresh in-memory portfolio, and actual `admit_public_paper_yaml_review` for the distinct compiled-fallback candidate.
- Source R155 original YAML denied: `general.trade_enabled=true`, `general.paper_entries_enabled=false`, `scale_in_hints.enabled=true`, `analytics.enabled=true`. The R174 freeze remains enforced.
- R95 fixed compiled PAPER additionally denies `real_trial.enabled=true`, `real_trial.autonomous_real_enabled=true`, unsupported `dynamic/velocity`, and source PAPER daily=3000 vs compiled=500.
- A narrowly transformed **in-memory** variant (general trade=false; paper_entries=true; scale_in=false; analytics=false) passes the actual R155 in-memory constructor; R95 remains denied. This is not operator feature parity or a deployable recommendation.
- All decisions tagged as SOURCE-ONLY, installed field applications 0/160 positively proven, automatic cutover and real order authority always false.
- A pre-correction negative test found a previously omitted `general.paper_entries_enabled=false` denial and was fixed before final validation.
- Three new tests: original both lanes reject; in-memory isolated restriction passes R155 only; post-parse `cfg.raw` mutation denies. This new tool is to report blockers, not to silently switch operator preferences.

## R262: installed PAPER receipt gap
- PAPER service continues running old `paper_a342540` ELF SHA256 `10138620181ebfd37dcb76b79c091c70d3315b5c536045fd6ea5c258aa0dbd46`.
- Current installed session summary includes `websocket_seen` counts and model quota but NO `applied_paper_decision_policy`, per-field YAML receipt, active source bundle/ELF lineage.
- Deployed PAPER 234/234 counters match public intersection count; not exact instrument membership/ownership proof.

## R263-R264: existing limits preserved, not invented closure
- 160 original YAML paths: 149 research-evidenced, 11 unsupported/guarded; 0/160 per-field installed positive evidence (not zero actually used).
- AI read-only advisory and analytics `can_write_db=false` do not attest durable writer, and Telegram tests do not attest external delivery.
- DB1/DB2 19 Oct snapshot was positive, but a fresh causal forward replay with traffic not tested in R258-R265.
- Four-leg original exchange account census, fees/funding/exact ACK/UNKNOWN and hosted exact-SHA release provenance remain OPEN. No exchange private GET/POST/CANCEL.

## R265: release/checkpoint
- SOURCE additions only: new diagnostic Rust file, `src/lib.rs` export, this report. No PAPER deployment.
- Tests after change: full Rust lib 1178 PASS/0 FAIL/2 ignored; full Rust bins 93 PASS/0 FAIL. Total 1271 PASS, 2 ignored. Only exact-file commit after scope and PASS checks.
- Preserve 32 historic dirty/untracked paths and exact porcelain SHA256 `9d5a1608ce8912435df2269be7eeee6ac17bc178e3442dd137fd56c4beb6ebc4`, empty staging.
- Keep supervisor MainPID=2161721, NRestarts=0, live child resolved freshly; kill switch ENGAGED.
- No real trading permission; user has not approved deployment of a reduced-feature strategy or signed release.

## Next high-value work
1. A source-profile R155 offline config report must not be treated as installed runtime 160-field attestation.
2. To get actual source-applied receipts, separately evaluate isolated PUBLIC PAPER with resource/network budget and explicit scope; do not change operator YAML in-place or active supervisor.
3. Restore full operator AI, scale-in, analytics semantics only after independent per-field consumer and writer tests; do not disable for production.
4. P0 exchange ledger+DB causal forward replay remain REAL release blockers.
