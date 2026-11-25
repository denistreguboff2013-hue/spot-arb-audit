# SPOT-ARB Astra — R173 evidence and remaining 11

Date: 2026-10-21. Candidate only; installed PAPER untouched.

## Proofs completed
- New native_r173_config_operational_preview.rs source-only review.
- 6 distinct new R173 tests PASS; filter r173_ 9 PASS (includes 3 existing tests unrelated to this module).
- Full Rust library: 1121 PASS, 0 FAIL, 2 ignored.
- PAPER binary unit tests: 16 PASS, 0 FAIL.
- Original R166 160-field register plus R168, R169 and R173 overlays reconciled:
  123 baseline + 16 R168 + 5 R169 + 5 R173 = **149/160**, all distinct.
- Installed PAPER field-level YAML source application proven: **0/160**.
  This means evidence is absent, not that the installed executable uses zero values.

## What R173 proves and does not prove
- report_time and report_timezone: local UTC schedule changes for UTC and
  modern Asia/Yekaterinburg within 2020-2035. This does not trigger a scheduler.
  All other IANA/DST timezones explicitly fail closed.
- paths.data_dir and scale_in_hints.telemetry_path: two independent path
  selectors react to source YAML; no file was read/written by the preview.
- telegram.proxy_url: YAML first, then two caller-supplied ENV fixtures.
  No secrets, real environment lookups or HTTP were performed.
- Altering parsed raw YAML without reparsing is rejected.

## Remaining 11 — priority audit targets
1. general.bot_name — no verified active Python consumer; find evidence before implementation.
2. general.debug_write_liquidity — no verified active Python consumer.
3. general.liquidity_bps — no verified active Python consumer.
4. general.mode — no verified getter under general.mode (other mode fields unrelated).
5. logging.level — Python setup_logging defaults INFO; no verified YAML plumbing.
6. scale_in_hints.enabled — enabled runtime behavior not established; do not blindly enable.
7. scale_in_hints.min_imbalance — no verified active consumer.
8. scale_in_hints.min_vel_bps_s — Python fallback only when velocity.hold_pos_bps_s missing;
   baseline primary exists; avoid claiming it is actively applied.
9. scale_in_hints.send_negative — no verified active consumer.
10. spread.history_window — R165 fixed-value rejection only; no dynamic source application.
11. velocity.enabled — current research accepts true, but off-mode semantics unproven.

Active-Python-consumer statements are from inspected source paths, not an
exhaustive negative proof for the whole project.

## Next
- Audit those 11 as implemented / dormant / deprecated / missing wiring.
- Establish installed ELF-to-source YAML provenance through read-only,
  non-secret checks before any future PAPER promotion.
- If an implementation is permitted, add safe offline tests before opt-in
  runtime behavior. No real execution without separate authorization.
