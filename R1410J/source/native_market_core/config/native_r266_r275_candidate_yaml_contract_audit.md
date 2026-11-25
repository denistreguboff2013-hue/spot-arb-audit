# SPOT-ARB Astra R266-R275 — exact offline candidate YAML contract (2026-10-22)

Server spb-3-vm-4r2s. Branch review/native-core-v3-r31-entry-permit-dispatch-20261008. Start HEAD d2a0f4108802733f22c02bb53a761ead6547c81e. Original worktree porcelain SHA256 9d5a1608ce8912435df2269be7eeee6ac17bc178e3442dd137fd56c4beb6ebc4 (32 pre-existing paths). The running PAPER must NOT be replaced, restarted, or used as test target. Kill switch ENGAGED.

## Ten review directions in this group
1. Source identity: existing R176 already documents original 160-field historical evidence; do not duplicate its parser or report research tests as installed runtime.
2. Prior 160-field source matrix: R166=123; R168=16; R169=5; R173=5; R174 guarded-only=11; research total 149, installed field applications verified 0.
3. Exactly 11 unsupported/guarded: general.bot_name, general.debug_write_liquidity, general.liquidity_bps, general.mode, logging.level, scale_in_hints.enabled, scale_in_hints.min_imbalance, scale_in_hints.min_vel_bps_s, scale_in_hints.send_negative, spread.history_window, velocity.enabled.
4. Current original YAML vs versioned historical R64 baseline: 156/160 identical values, 4 differences (general.paper_entries_enabled, general.trade_enabled, real_trial.autonomous_real_enabled, real_trial.enabled). Both files retained unchanged. R176 embedded source receipt is for R64, not direct evidence of the current original source.
5. Existing R258 admission engine: original R155 and R95 both fail as expected; the new binary reuses actual R258 constructor/admission functions, not a newly invented decision parser. Original R155 blocked by general flags and analytics/scale-in; R95 additionally requires 500/day rather than current 3000.
6. R266 new executable-only --r266-yaml-diagnostic returns BEFORE public metadata GET, WS spawn, model sessions, exchange credentials, Telegram or trading. Requires explicit R266_OFFLINE_REVIEW_AUTHORIZED=YES, exact argument, and kill switch ENGAGED. Without opt-in it denies before any source read.
7. A single offline generated result enumerates all 160 historical field paths and research stage, independently selected current source SHA, local exact /proc/self/exe ELF and R157 source-build fingerprint, and the actual current R258 admission blockers. Every installed field is conservatively NOT_PROVEN.
8. Fail-closed negative Rust tests: source/build YAML SHA drift, forged deployed ELF flag, duplicate 160 fields, false installed field proof and post-parse source mutation. Candidate binary does not expose the internal crate-private source-integrity checker: it calls existing R258 which already checks it. No guard weakening.
9. Local executed candidate receipt: /tmp/spotarb_r266_offline_yaml_160_20261022.json (mode 0600, 49,861 bytes, SHA256 a53835c77575aa854feef240982a94769bd329db6b474f9bd54c5277533b6e94). Pre-commit candidate /proc/self/exe SHA256 81273f3a40c7e6e26c187617d04e5c804116a635d292aad440c6a7e31a0d56ca, compiled from starting HEAD d2a0f410..., research-only. All 160 rows are NOT_ATTESTED as consumer runtime, source 149 research/11 guard. `current_matches_historical_baseline=false`.
10. Offline full Rust lib 1178 PASS / 0 FAIL / 2 ignored, Rust bins 96 PASS / 0 FAIL (81 binary test groups), total 1274 PASS. Stage solely amended candidate binary Rust source and this report, commit once, and reconfirm prior 32 Git paths, service process/PID and kill switch. Rebuild candidate after commit for accurate Git HEAD; do not auto-launch PUBLIC PAPER.

## Reported original YAML blockers from actual executed Rust R258
- source_paper_r155 accepted=false: SOURCE_ANALYTICS_UNSUPPORTED, SOURCE_R174_UNWIRED_FROZEN_OR_SWITCH_CONFLICT, SOURCE_REQUIRES_GENERAL_TRADE_DISABLED, SOURCE_REQUIRES_PAPER_ENTRIES_ENABLED, SOURCE_SCALE_IN_UNSUPPORTED.
- compiled_fallback_r95 accepted=false: COMPILED_PAPER_DAILY_DIFF_FROM_500, COMPILED_R77_QUOTA_OR_POLICY_DENIED, COMPILED_R95_FEATURE_PROFILE_UNSUPPORTED, COMPILED_REQUIRES_AUTONOMOUS_REAL_DISABLED, COMPILED_REQUIRES_GENERAL_TRADE_DISABLED, COMPILED_REQUIRES_PAPER_ENTRIES_ENABLED, COMPILED_REQUIRES_REAL_TRIAL_DISABLED.
- Current original source SHA 98b377d7d8d98916048279c9c180fa918e4b26d8ea25fd87bc5114edf2f05731.
- Source baseline R64 SHA c6e4b7c7f1574cd1831ca4b2d0633d5bd8782adae97f093518342b8bf7cab189.

## Critical limits
This executed candidate check is a local UNSIGNED/SELF-REPORTED build proof. No independent release signature, no positive applied receipt from running `paper_a342540`, no causal per-field consumer trace, no installed YAML proof. It does not make 149 parameter consumers installed, and does not authorize a reduced-feature PAPER profile.
No WS/Telegram/private exchange access in R266. No production DB1/DB2 writes, REAL authority or authenticated fills/PnL.

## Next high-value work, not repeated
- R276+: instrument a separately authorized isolated PUBLIC PAPER run for exact source SHA, ELF, applied decision vs PaperPortfolio model, per-field consumer traces and 3h lifecycle safety; do not misrepresent source-only diagnostics as deployed.
- Work through 11 unsupported/guarded controls by semantics; many are non-decision UI/logging controls, and scale-in/velocity strategy behavior must be preserved if intended.
- AI/analytics/Telegram require actual durable write/delivery evidence; DB1→DB2 forward causal proof and authenticated exchange ledger still remain P0 blockers; REAL is NOT READY.
