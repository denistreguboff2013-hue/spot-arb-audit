# R456–R465: hourly raw-rollup read-only preview and independent Python oracle
Date: 2026-10-23. Source: /home/tmpadmin/spotbot/ai/collector.py functions _finite_number and _rollup_raw_rows, and original hourly_raw_rollups CREATE TABLE schema.

## Evidence
- Generator `native_r456_generate_python_oracle.py` AST-extracts the actual Python source functions and schema, runs them in an in-memory Python sqlite3 with Decimal, and writes `native_r456_python_rollup_oracle.json`. No live DB, no network.
- Python oracle: four old PAPER observations (3 SIGNAL_SEEN and 1 DECISION_MADE), 2 groups. First group: raw_spread_count=2, raw_spread_sum=3.50, raw_spread_min=1.20, raw_spread_max=2.30; net sum=0.50, spot depth sum=125.50, future depth sum=125.00. The independent Python function excludes invalid text, NaN and Infinity.
- Rust `native_isolated_rollup_preview_r456_review.rs` groups the exact READ-ONLY retention candidate set from R446–R455 by eight original key fields (hour,mode,base,category,name,reason_code,decision,skip_reason). It produces all 24 source-schema fields as JSON for comparison. It never creates, upserts or deletes a row.
- Bounded custom signed i128 decimal-with-scale preserves tested trailing zeros, sums, min/max and age maximum. Scientific notation supported within bounds; negative zero, arbitrary JSON floats, fraction scale >6, >18 significant source digits, and unsafe aggregate totals are rejected with Schema rather than silently approximated.
- Ten Rust R456–R465 tests: full independent Python SQLite result parity, decimal scale, signed min/max, NaN/non-number ignore, unsupported numeric fail-closed, scientific notation, multi-base/hour groups, all-nonnumeric signal metrics, precision abort without mutation, reopen deterministic.
- The private SQLite rows, R366 event/outbox chain, and installed PAPER remain unchanged. No DB1/DB2, no API or trading.

## Still OPEN (Astra)
- This is **not** complete Python Decimal parity: Python has 28-digit arithmetic-context semantics and broader exponent/numeric-string/float behavior. Source Python stats_merge on preexisting hourly rollups is not implemented. The tests prove only the bounded subset and 4-event independent fixture.
- Actual source-matched hourly_raw_rollups durable upserts, exact deletion-set SQL transaction, category/event delete outbox after-images, metadata changes, DB2 durable mirror ACK, schema migration and restart recovery are not implemented; deletion_authorized always false.
- The preview first executes R366 full audit and R446 full scan <=4096, so continuous ingestion p95/p99 and O(n²) are still not certified. Do not deploy into installed PAPER or remove historical raw data.
- P0-01 forward replication PARTIAL, P0-02/P0-03 OPEN, operator YAML installed-paper coverage 0/160 proven. REAL kill must stay ENGAGED.
