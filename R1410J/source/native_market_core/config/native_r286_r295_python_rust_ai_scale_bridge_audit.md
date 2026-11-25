# Astra R286-R295: Python→Rust actual consumer and AI/Analytics parity audit — 2026-10-22

Scope: READ ONLY for running PAPER; Rust offline research-only code; no REAL, DB, private exchange API, Telegram send, network WS, restart or deployment. Start HEAD 437a0f9a67936505f74a76faf1d0f0df96f318a9; 32 historical dirty/untracked paths SHA256 9d5a1608ce8912435df2269be7eeee6ac17bc178e3442dd137fd56c4beb6ebc4; staging 0.

## Ten verified work directions

R286. Direct legacy Python source traced, not guessed from parameter names:
- /home/tmpadmin/spotbot/scanner.py lines 102–114 and 301–312: scan min_trade_usdt uses first truthy scale_in_hints.min_depth_usdt, adaptive_entry.min_usdt, risk.futures_order_usdt, then 0. Does NOT examine scale_in_hints.enabled. Current requested min-depth is 200 USDT. This sets scanner trade_ok gate, not add-to-position orders.
- /home/tmpadmin/spotbot/strategy.py lines 1475–1486: scale_in_hints.telemetry_path is used to load scanner telemetry.
- /home/tmpadmin/spotbot/strategy.py lines 3257–3283: scale_in_hints.min_vel_bps_s is a fallback for velocity.hold_pos_bps_s. Because hold_pos_bps_s is present (0.06), the current 0.08 fallback is not used in this path.
- The analyzed live Python files had no references to scale_in_hints.enabled / min_imbalance / send_negative. This is not an exhaustive proof all plugins/background systems ignore them; do not invent scale-in order semantics.

R287. Existing Rust native_legacy_strategy_policy_review::inspect_scanner_min_depth_readonly already models first-truthy scanner threshold; its documentation explicitly notes scale_in_hints.enabled is not the scanner selector. Existing R168 tests cover min_depth mutation. Do NOT rewrite it.
R288. Native legacy close preview already covers velocity/dynamic and related guards; R174 intentionally freezes unsupported scale-hints and logging knobs. Do NOT credit guard-only field as implemented runtime consumer.
R289. Native Rust P106 paper_ai_analytics_source_review_without_io already binds all 7 analytics YAML fields and validates source integrity, emits sanitized paper input and pending-only advisor. No duplicated adapter required.
R290. Existing Rust storage controls inspect pressure, WAL and paths but can_write_db=false, can_prune=false; analytics_writer_running_attested=false, analytics_database_opened=false, analytics_database_retention_applied=false. These are NOT durable writer proof.
R291. Python ai/collector.py lines 256 onward and 812–903 is actually a SQLite writer: WAL, collector_runs, append_record, maintenance_window lock, mirror_outbox record and transactional commit/retry. Rust R147 isolated SQLite mirror is research-only DB1/DB2 mirroring, NOT a replacement AI collector. Do not claim writer parity.
R292. Introduced native_r286_python_rust_consumers_review.rs to invoke existing Rust scanner / AI / storage / admission functions in one SHA-labeled result for a deterministic fixture. Fake fixture is explicitly not installed PAPER. No IO/storage/orders/network.
R293. Three new Rust tests check source scanner threshold=200 regardless of scale enabled, 200→250 mutation changes preview but never grants order authority, AI never claims durable writer or strategy auto-tuning. These tests exercise already-existing components; no new order rules.
R294. New opt-in isolated executable CLI --r286-source-consumers-proof requires R286_READONLY_ONLY_ALLOWED=YES, exact argument and kill switch ENGAGED, returns before HTTP metadata, WS, PaperPortfolio, private API, DB, Telegram, orders. Before output validates SHA256 original YAML against local build proof. Local report /tmp/spotarb_r286_source_consumers_20261022.json (SHA256 06095a8e5820a738a2895d9d0aec59302c45409cbf719cd790aeb8c2939dfab3, 12,240 bytes); precommit debug ELF b6f1cc6e9648819cc38457275feb64261b04e5ff908affaa51c7b315c064d1e7, compiled from 437a0f9... . Not a signed/installed build.
R295. Full offline library suite: 1181 PASS, 0 FAIL, 2 ignored; full bin suite: 98 PASS, 0 FAIL (81 binary test groups), total 1279 PASS. Scoped commit only after PASS; unchanged old 32 paths, staging empty, active old PAPER supervisor PID 2161721, NRestarts 0, installed ELF paper_a342540 SHA256 10138620181ebfd37dcb76b79c091c70d3315b5c536045fd6ea5c258aa0dbd46, kill switch ENGAGED.

## What remains open
- Actual source-run AI/analytics durable writer and source feature semantics; R155 still blocks original analytics.enabled/scale_in_hints.enabled. Offline input/read-only preview is NOT activation.
- Exact current installed PAPER YAML field application remains 0/160 positively attested; 149 source research / 11 guarded-only.
- Genuine runtime WS membership and 160-field consumer receipts, AI pending recommendation lifecycle/delivery, Telegram transport delivery, DB1→DB2 forward causal traffic, exchange-authenticated four-leg truth/UNKNOWN and release sign-off.
- REAL not authorized; model PnL does not prove genuine profit.

## Next R296+
Prioritize a signed source-to-ELF–to–durable-writer test design for analytics, grounded in Python collector's actual append+outbox+commit/maintenance semantics. Do not turn on analytics just by flipping guard or invent scale-in trade orders. Integrate only after local isolated fsynced writer tests and no-touch production proof. Reuse existing R147 SQLite FFI infrastructure with appropriate isolation only if transaction semantics fit.
