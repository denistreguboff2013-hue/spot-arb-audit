# SPOT-ARB R316–R325 — offline PAPER SQLite integrity gate
Date 2026-10-22; target spb-3-vm-4r2s. No REAL, no installed service or production DB mutation.

## Baseline and scope
Parent HEAD 7a4f1edb6ef4bf09cc85052cd4c42ec233572cb5, branch review/native-core-v3-r31-entry-permit-dispatch-20261008.
32 pre-existing Git dirty/untracked paths, porcelain SHA256 9d5a1608ce8912435df2269be7eeee6ac17bc178e3442dd137fd56c4beb6ebc4; initial staging 0.
Installed PAPER active supervisor 2161721, restarts 0, child 2866189, ELF paper_a342540 SHA256 10138620181ebfd37dcb76b79c091c70d3315b5c536045fd6ea5c258aa0dbd46.
Real kill switch ENGAGED.

## Ten real review directions, NOT ten new features
- R316: verified post-R305 Git, original 32 paths, supervisor process identity, kill gate and disk headroom. PASS before changes.
- R317: read Python ai/collector.py and mirror_outbox.py again: transaction is event + source row + an ordered change-feed; source event SHA and outbox protocol are NOT equivalent to R296 minimal event/outbox prototype.
- R318: review existing R147–R199 SQLite FFI/transaction/crash tests and R296 module. Do not reimplement SQLite or imply old DB1/DB2 mirror is a full AI analytics writer.
- R319: added strict sqlite schema verification on isolated R296 test database (PRAGMA user_version, exact table_info columns/types/PK/NOT NULL, critical sqlite_master DDL constraints), plus SQLite integrity_check. Refuse schema drift.
- R320: added independent recomputation of event SHA256 from stored event_id/mode/category/canonical JSON payload; compare both stored event hash AND outbox hash. Equal but forged event/outbox hash pair is rejected.
- R321: check payload is valid, bounded, canonical JSON object, event identity and category are allowed, and mode remains strictly paper; detect orphan outbox rows.
- R322: fail closed on reopen, audit and append when schema or prior stored data are inconsistent. This is intentionally O(database rows) per write and is NOT approved for high-throughput production.
- R323: four new offline negative tests verify: schema extra-column denial; synchronized two-hash forgery denial; payload alteration denial; outbox orphan denial. Existing five R296 tests are also rerun. The first targeted execution exposed a malformed diagnostic SQL join; it was fixed and all targeted tests then PASS. Do not erase the preliminary failed run from history.
- R324: FULL fresh offline Rust lib: 1190 PASS, 0 FAIL, 2 ignored. FULL binary suite: 98 PASS, 0 FAIL across 81 binary groups. Unique TOTAL 1288 PASS / 0 FAIL / 2 ignored. Four new tests and five historic writer tests are INCLUDED in the 1190 library count; do not double-count them.
- R325: one scoped commit of R296 Rust module + this Markdown audit only if full tests PASS, preserving old 32 paths, no service/config/DB/private API/Telegram change. No SIGKILL crash injection is claimed in this phase.

## Limitations not closed
1. No authenticated tamper witness. Anyone who can edit all rows and recompute matching SHA values can still forge a consistent store; local checksum != cryptographic authenticity.
2. No actual isolated SIGKILL/reboot/powerfail testing in this phase, no independent DB mirror replay proof.
3. No exact Python analytics schema/protocol, schema migration/upgrades, 160-field installed YAML applications, categories, AI durable recommendation writer, retention/maintenance locks, WAL caps or storage limits.
4. The name/mode/symlink path checks are not proven race-proof at openat/dirfd level; isolation is limited to private mode-0700 /tmp fixture directories.
5. No runtime PAPER installation. Kill switch ENGAGED; REAL not authorized. Do not enable analytics.enabled on the operator source until complete functionality and gates are proven.

## Following stage
- R326+ prioritize exact Python collector canonical hash + source table/operational mirror protocol with fail-closed exceptions and constraint tests, and then independent process crash tests if permitted. Avoid fake stage counts.
- Keep explicit separation between isolated prototype readiness, real PAPER deployment, and exchange-authoritative four-leg ledger/DB1→DB2 release gates.
