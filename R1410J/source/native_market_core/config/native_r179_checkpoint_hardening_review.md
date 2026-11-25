# Astra R179: R153 planned PAPER checkpoint hardening (SOURCE ONLY)

Host: spb-3-vm-4r2s. Parent HEAD: 592ef8180ad9ccbcebb379f23b67546092a6372a.
Branch: review/native-core-v3-r31-entry-permit-dispatch-20261008.
Protected 32 historical paths: SHA256 9d5a1608ce8912435df2269be7eeee6ac17bc178e3442dd137fd56c4beb6ebc4.

1. Preflight: systemd PAPER supervisor active PID 2161721, zero restarts; existing ELF SHA256 10138620181ebfd37dcb76b79c091c70d3315b5c536045fd6ea5c258aa0dbd46. Kill switch ENGAGED.
2. R153 planned same-boot, source-SHA-bound handoff already existed. Its unsealed crash checkpoint cannot be auto-restored, and is still not a private exchange census or crash-proof recovery.
3. Found concrete TOCTOU: symlink_metadata(path) followed by a separate fs::read(path). R179 now opens the exact FD with O_NOFOLLOW, O_CLOEXEC and bounded read; verifies size, owner-only permissions, nlink=1, and path dev/inode consistency.
4. R153 original staging did not request owner-only mode explicitly. R179 uses mode 0600 for create_new stage and continues existing fsync+atomic-rename flow.
5. Negative tests: 0600 and successful clean restore; hardlink denied; group-readable denied; truncation/oversize denied; staging symlink trap denied; Yekaterinburg day rollover preserves entry counters and denies stale replay.
6. Original source quota remains 3000 in the separate review model, while installed PAPER retains compiled 500; R77 fail-closed gate and R178 source-lock remain unchanged.
7. R174 unsupported legacy controls continue failing closed. This change earns no additional source field coverage (149/160 research only; installed 0/160 positively demonstrated).
8. R50 AI and analytics are read-only snapshot advisors without proved durable writer; R38 Telegram has separate opt-in and durable dedup and no external messages were sent. No WS candidate launched and no live trading authorization.

Limits: This hardens local planned checkpoints, not an independent rollback witness, distributed consensus, exchange truth, or safe automatic recovery. Existing 0600 requirement may reject older more permissively written research checkpoints; this is intentionally fail closed. No installed PAPER files updated.

R180 proposed: isolated crash-window fixture; same-day vs next-day quota conservation; independently verified installed YAML-to-decision receipts; actual AI data writer ownership; opt-in Telegram delivery evidence; source-vs-Python strategy parity; public WS universe count; original 13 Astra gaps and installed cutover gate.
