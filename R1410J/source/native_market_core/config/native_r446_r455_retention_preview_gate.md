# R446–R455: PAPER raw retention candidate preview (READ-ONLY)

## Source and proof scope — 23 October 2026
- Python source: `/home/tmpadmin/spotbot/ai/collector.py`, `prune_raw` (~583–612) and `_rollup_raw_rows` (~482–582).
- Python eligibility: category signals/decisions, name SIGNAL_SEEN/DECISION_MADE, received_at_ms strictly older than cutoff, exclusion of any linked entries/exits/fills by mode+correlation_id, by mode+exec.signal_id=raw.event_id or by mode+exec.signal_id=raw.signal_id. Normal age = days*86400000; pressure age = 1800000ms. Batch limit source <=500.
- New Rust `r446_retention_preview` in `native_isolated_retention_preview_r446_review.rs` reads only the private R366 SQLite after full independent audit and calculates this eligibility. Records retain original source envelope (including optional signal_id which is not a physical events column). All mode records restricted to paper. Uses separate BTreeSet indexes for linked execution correlation and signal IDs. Candidate ordering by (received_at_ms,event_id) for deterministic ties; Python SQL ties are unspecified.
- Returned `deletion_authorized=false` always; no actual SQL DELETE, no rollup or mirror deletion frames, and no change to installed PAPER/DB1/DB2. Matching is an **unverified partial-source compatibility candidate gate**, not native collector retention parity.
- Fail-closed: now, days 1–365, batch 1–500, no underflow, maximum 4096 examined SQLite source records per preview to bound memory. Larger histories are refused, NOT silently truncated.
- Ten offline tests R446–R455: eligible old unlinked raw, shared correlation exclusion, signal event-id link, shared signal-id exclusion, 7-day strict boundary + 30-min pressure, batch ordering and no delete, malformed limits, nonraw stage protection, independent attempt not falsely excluded, reopen without mutation.

## Remaining blockers before retention may actually prune
- Original Python executes exact-hour rollup by (mode,base,category,name,reason_code,decision,skip_reason) with Decimal finite precision and metrics spread/net/telemetry age/depth, *before* deletion in same SQL transaction.
- Python writes mirror_outbox `hourly_raw_rollups` after-images, category deletes, events deletes, metadata increment before commit; source counts must match. Current Rust R366/R376 audit assumes one permanent source event/category/outbox row per append. Any naive deletion would break integrity/audit.
- Correct source-protocol schema generation + migration, allowed mirrored DELETE/upsert frames, DB2 transaction and ACK, crash recovery, and independent anti-tamper chain witness are NOT implemented here.
- Read-only candidate preview scans at most 4096 (deliberately fails on large histories) and starts with full R376 audit; it is NOT suitable for continuous PAPER loads as-is. Need bounded incremental audit and p95/p99 scaling proof.
- Original Astra P0-01 partial, P0-02 and P0-03 open. No exchange order authority; REAL kill ENGAGED; installed PAPER SHA untouched.
