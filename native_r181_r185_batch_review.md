# Astra R181-R185 combined source-only batch
2026-10-22. Parent HEAD b0e7bc7b2babec435cf5d1f30b032899bebb2522.

R181: Existing compiled-fallback virtual continuity checkpoint had pathname race (metadata then read), permissive staging (create+truncate), and no strict 0600 file protection. Fixed via O_NOFOLLOW FD bounded read, inode check, owner-only mode, create_new staging. Negative tests for symlink, hardlink, group readability.

R182: Existing fallback could publish duplicate/stale revisions through simultaneous writers. Added same-path owner-only persistent flock and revision+boot+monotonic checks held through fsync+rename and reader load. Concurrent two-writer fixture confirms exactly one successful next revision.

R183: Same-Yekaterinburg-day planned handoff formerly accepted caller-supplied next_day_entries less than persisted entries. This could artificially free modeled daily quota. Reject reductions on same day, bound by compiled 500; separate new-day source truth still requires operator proof. Test counters 490 -> 0/489 DENY, 490 PASS, 501 DENY.

R184: Crash orphan pending file no longer truncated/reused; symlink and extra hardlinks on local lock fail. Existing AI/analytics remains read-only without certified Rust durable writer. Existing Telegram opt-in outbox and live timers remain separate, no external send attempted. R174 unwired eleven controls and strategy profile split remain OPEN.

R185: PUBLIC-only R167 source WS planning synthetic fixture shows source group_size=40 with 234 bases gives 40/40/40/40/40/34 and 235 gives 40/40/40/40/40/35. Installed PAPER status independently reports websocket_seen binance=234 and okx=234. Absent real instrument ID NOT determined; synthetic fixture does not certify installed membership. Installed ELF source YAML applied 0/160 PROVEN, research 149/160 evidenced, 11 unsupported.

INVARIANTS: Historical 32 Git paths preserved; no staging of them. Installed paper_a342540 SHA 10138620181ebfd37dcb76b79c091c70d3315b5c536045fd6ea5c258aa0dbd46 stays unchanged, supervisor PID 2161721 NRestarts=0 at preflight, kill switch ENGAGED. No new REAL authority, exchange private calls, installed service changes, DB writes, or notifications. Source-only Rust tests do not prove deployed execution.

Next R186-R190: reliable installed-field receipt map; independently sourced symbol-difference report; original Python-vs-Rust executable strategy semantics including unsupported modes; durable AI writer scoping; Telegram end-to-end delivery only with explicit opt-in; controlled source PAPER cold crash proof and separate launch gate.
