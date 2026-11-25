# SPOT-ARB Astra R174 — unsupported legacy YAML admission hardening
Date: 2026-10-21. Source-review ONLY. Installed PAPER unmodified.

## Purpose and result
Investigated all remaining 11/160 YAML fields. No field is credited merely
because it was parsed, inspected, or rejected. Instead, an opt-in source-owned
research PAPER candidate is now rejected if an unsupported legacy value drifts.
Proof remains **149/160 research applied previews, 0/160 installed ELF proven**.

## Evidence-based classification
| Field | Current source finding | R174 disposition |
| --- | --- | --- |
| general.bot_name | No executable getter found in inspected active Python modules | freeze original value in R155 admission |
| general.debug_write_liquidity | No executable getter found | freeze |
| general.liquidity_bps | No executable getter found | freeze |
| general.mode | No general.mode getter; strategy's unrelated position mode differs | freeze |
| logging.level | Active main.py and run_scanner.py call setup_logging() without this argument; utils/logger.py defaults INFO | freeze; not an active Rust logging level |
| scale_in_hints.enabled | Scanner uses min_depth_usdt without consulting this flag; active true cannot be equated with executed scale-in | require false in source-owned research candidate |
| scale_in_hints.min_imbalance | No executable getter found in inspected active Python modules | freeze |
| scale_in_hints.min_vel_bps_s | strategy.py:_vel_cfg fallback only when velocity.hold_pos_bps_s absent; full Rust YAML schema requires primary field | freeze |
| scale_in_hints.send_negative | No executable getter found | freeze |
| spread.history_window | Existing R165 check already rejects non-3 values in optional bridge, not executable history parity | freeze 3 in R155 research admission as well |
| velocity.enabled | Root strategy flag not implemented as OFF in Rust PreparedClose; telemetry.velocity.enabled is an unrelated field | require true only in R155 research profile |

## Source anchors and safeguards
- /home/tmpadmin/spotbot/main.py:97 and run_scanner.py:20,90 use
  setup_logging() without reading logging.level.
- /home/tmpadmin/spotbot/utils/logger.py:5 defaults INFO.
- /home/tmpadmin/spotbot/scanner.py:105-112 accesses min_depth_usdt
  regardless of scale_in_hints.enabled.
- /home/tmpadmin/spotbot/strategy.py:3258-3260 fallback for min_vel.
- src/native_paper_config_bridge.rs:112-132 contains R165 existing freeze.
- src/native_paper_strategy_frame_review.rs:186-210 already rejects
  enabled scale-in or velocity OFF; R174 now ties other unsupported
  fields into the same source-only admission route.
- src/native_public_feed_policy_review.rs uses
  **telemetry.velocity.enabled**, distinct from velocity.enabled.
- This is a bounded active-file/source inspection, not proof that no
  dynamic usage could exist anywhere in the full Python project.

## Tested invariants
- The previously admissible synthetic PAPER config is still admitted.
- Nine frozen legacy fields reject changed source values, without credit.
- Enabled scale-in and disabled velocity are denied for R155 research only.
- In-memory changes to parsed YAML projection fail closed.
- Test integration proves SupervisedStrategyPaper::new invokes the guard.
- No exchange order, network, installed ELF, PaperPortfolio limit, or
  production scheduler was modified by R174.

## Further action
Do not force an artificial 160/160 score. Evaluate which knobs should be
removed/deprecated versus specified with exact Python and Rust semantics.
Most importantly, source-to-installed-PAPER provenance and P1-06 parity
remain OPEN even though offline test suites pass.
