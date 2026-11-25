# SPOT-ARB Native Rust Core v3 | R196-R215 batched source-only audit
Date: 2026-10-22. Preflight parent HEAD 1c58042b0f80c7e9a4d32e202c69f37bcc34a58f.
Host spb-3-vm-4r2s. Repo /home/tmpadmin/native_rust_registrar_review_20261002/c18_floor_checkpoint_20261006/review.
Legacy dirty/untracked 32 paths preserved with SHA256 of git status --porcelain=v1:
9d5a1608ce8912435df2269be7eeee6ac17bc178e3442dd137fd56c4beb6ebc4.
Initial staging empty. PAPER supervisor PID 2161721 active and zero NRestarts; installed ELF paper_a342540 SHA256
10138620181ebfd37dcb76b79c091c70d3315b5c536045fd6ea5c258aa0dbd46; kill switch ENGAGED.

## Review of 20 separate stages

| R | What actually existed / was added | Status and limitation |
|---|---|---|
| R196 | native_sqlite_source_batch_r196_review.rs atomic <=16 synthetic DB1 rows per transaction | EXISTING offline; production DB1 not changed |
| R197 | native_two_venue_fill_consistency_r197_review.rs feeCcy/base quantity hedge-mismatch shape | EXISTING offline; not authenticated exchange fills |
| R198 | native_two_venue_zero_window_r198_review.rs two-timepoint zero-exposure shape | EXISTING; no real account census |
| R199 | native_signed_floor_bridge_r199_review.rs ED25519 receipt verification bridge | EXISTING; signature alone does not prove current off-host floor |
| R200 | native_floor_cold_bridge_r200_review.rs signed cold DB2 comparison | EXISTING; no independent witness installed |
| R201 | native_queue_remote_anchor_r201_review.rs signed external queue tip crosscheck | EXISTING research; no actual remote trust configured |
| R202 | native_queue_powerfail_r202_review.rs real SIGKILL around local queue publication | EXISTING test; not a physical power-loss or deployed continuity certification |
| R203 | native_floor_queue_ack_r203_review.rs signed floor + cold DB1/DB2 gate for ACK | EXISTING offline, no exchange authority |
| R204 | native_source_batch_profile_r204_review.rs isolated per-row versus source-batch timing | EXISTING profile only, not production throughput |
| R205 | native_order_fill_reconciliation_r205_review.rs test-supplied order/fill identity | EXISTING test models; external authenticity unproven |
| R206 | native_account_page_coverage_r206_review.rs two-venue pagination completeness shape | EXISTING offline; not exchange-owned account history |
| R207 | R207 order-and-fee consistency tests in native_order_fill_reconciliation_r205_review.rs | EXISTING tests; no runtime private GET |
| R208 | native_okx_fee_day_r208_review.rs UTC+5 feeCcy-aware known lower bound | EXISTING research; not exact full exchange fee |
| R209 | native_okx_fee_checkpoint_bridge_r209_review.rs owner-bound local V2 receipt to fee floor | EXISTING; not real venue receipt |
| R210 | native_okx_close_fee_archive_r210_review.rs archived buy+sell source fee preview | EXISTING; no funding or flatness proof |
| R211 | native_okx_persisted_fee_census_r211_review.rs local persisted owner archive census | EXISTING; not exchange-wide completeness |
| R212 | NEW R195 queue exact FD read O_NOFOLLOW/O_CLOEXEC, capped read, compare dev/inode/size/owner/mode; missing-file symlink at init DENY | NEW Rust source patch and negative test, local /tmp only |
| R213 | NEW R195 symlink/hardlink/permissions read deny; persistent .lock FD identity | NEW Rust source patch and test; local coordination only |
| R214 | NEW QueueLockR195 retains FD, Drop removes path only if exact original inode; replacement .lock stays quarantined | NEW Rust source patch and test; not independent antirollback |
| R215 | NEW R195 dangling .tmp symlink detected by symlink_metadata rather than exists(); empty queue file denied | NEW Rust source patch and test; no auto cleanup |

## Precisely edited files
- src/native_persisted_paper_queue_r195_review.rs: remove read path TOCTOU, harden lock ownership and incomplete tmp detection, reject dangling queue symlink at initialize, 4 targeted tests R212-R215.
- config/native_r196_r215_batch_audit.md: this source-level crosswalk and limits.
Other R196-R211 files were REUSED UNMODIFIED, not reinvented.

## Runtime versus research
- Installed PAPER remains old ELF, unchanged. Rust test PASS never certifies deployed YAML-to-decision usage.
- Original YAML 160 fields: 149/160 research evidenced, 11 unproven, installed 0/160 positively attested by per-field receipts (not zero applied).
- Installed 500/day is distinct from source-owned R155 3000/day; do not silently change quota.
- Live PAPER status historically says websocket_seen 234/234; original test baseline 235, missing symbol not identified.
- AI/analytics writer not attested. Existing Telegram outbox transport is only source-reviewed, no new outbound send.
- REAL execution prohibited; kill switch stays ENGAGED, no venue-private account queries, no production DB writes, no service restart, no external attestor or Telegram token use.

## Next 20-stage packet R216-R235 (not authorized to implement within this batch)
1. Independently verify executed ELF/source build identity before claiming feature parity.
2. Get explicit PAPER per-field YAML-to-consumer runtime receipts, mark negatives honestly.
3. Reconcile 234 vs 235 actual base IDs from trusted read-only feed metadata.
4. Test R195 queue lock identity under post-acquisition path replacement (same uid adversary).
5. Verify queue recovery on incomplete publication without deleting an active writer lock.
6. Separate source PAPER checkpoint and legacy fallback state semantics.
7. Run isolated overnight EKB rollover and quota continuity tests.
8. Verify Rust/Python feeCcy decisions on BASE-fee and QUOTE-fee fixtures.
9. Run local two-venue order/fill/fee mismatch rejection.
10. Verify source R208-R211 ownership bindings and archive completeness limits.
11. Independent external attestor trust design review, not a deployment.
12. DB1/DB2 source replay partial repair guarded by operator receipt.
13. Negative signed queue ACK cases for stale nonce and changed owner.
14. Map PAPER modeled PnL versus eventual exchange-truth evidence gaps.
15. Telemetry and safety budgets in isolated public-only fixtures.
16. Telegram outbox ACK/dedup proof offline without sending.
17. Identify authoritative AI/analytics durable writer before enabling.
18. Review all 13 Astra findings with source versus deployed evidence split.
19. Preserve 32 paths, full Rust suite, review exact staged files.
20. Commit as one bounded unit only with source-only tests and post-commit active PAPER, ELF, kill checks.
