# R546–R555 — Astra P0-01: LOCAL two-file baseline proof, not DB2 certification
Date: 2026-10-23. Starting verified HEAD 00bda34669bf31ff33ce607c87d3b15789ba367c.

## Scope of implemented evidence
- ONLY test directories from existing R366 0700 /tmp/astra_r366_isolated_*.
  Synthetic source DB1 and synthetic receiver DB2 are separate 0600 SQLite
  files; no production DB1/DB2, deployed PAPER, exchange, or disk cleanup.
- Built from existing R366/R376 source audit, R506 receiver audit/ACK and
  original Python mirror_outbox rebaseline oracle R536.
- New R546 code fingerprints a BOUNDED prefix (max 4096 synthetic append
  transactions): canonical SHA256 of ordered seq, tx_id and payload_hash,
  checked independently from both DB1 original outbox and DB2 receipts.
  R506 audits receiver after-image hash and row projection separately.
- Requires source seq = received DB2 seq = durable source ACK seq, all >0,
  private source path, matching caller-supplied frozen identity, and a
  syntactically valid rebase-32-hex generation.
- Holds source DB1 BEGIN IMMEDIATE and test DB2 BEGIN IMMEDIATE during the
  final prefix verification and a single DB2 local proof insert. This
  prevents a private concurrent source append during the local staging.
- Writes only a PRIVATE local DB2 table r546_private_baseline:
  boundary, SHA identity, generation, proof hash. Does NOT alter source
  mirror_control, mirror_outbox, installed Rust ELF or Python mirror.
- Separate r546_verify() recalculates source->sink prefix and proof hash
  on restart. A changed source count or mismatched receipt fails closed.
  Returns db2_snapshot_matched=true only for this local fixture; hardcodes
  independent_witness=false, permits_normal=false, permits_real=false.

## Ten focused tests
R546 successful two-file matching prefix, source identity and flags.
R547 DB2 receipt ahead of source ACK refuses baseline.
R548 caller-supplied wrong frozen DB1 identity refuses staging.
R549 legacy/invalid feed generation fails closed.
R550 corrupted DB2 receipt fails identity derivation.
R551 source outbox tamper fails the source audit.
R552 source append after staged boundary makes previous local proof stale.
R553 duplicate proof cannot overwrite the saved generation.
R554 DB1 + DB2 reopen preserves independently verifiable local proof.
R555 local proof hash tamper detected, with normal source/sink audit intact.

## Critical unresolved limitations: P0-01 remains PARTIAL
- This fingerprint is a hash of SYNTHETIC ordered event transactions, not a
  physical DB1 snapshot or full source schema/table manifest (17+ tables,
  file bytes/mtime/version, canonical table content digest).
- Original Python R536 has source_identity/schema/user_version/size/mtime
  fields. R546 DOES NOT verify them from a frozen REAL DB1. The caller-
  supplied identity can never itself authenticate a remote source.
- The DB2 proof is written by code on the SAME VM with shared permissions,
  not by an independent remotely authenticated DB2 or witness. If both
  copies roll back together, the local proof may be replayed undetected.
- No proper generation transition in the installed or source R376 scheme:
  it remains BOOTSTRAP_REQUIRED only. No NORMAL flag, DB2 baseline accept
  command, real backfill, replica live ACK or independent deployment witness.
- The source lock/DB2 commit is not a distributed atomic commit. There are
  no crash/power-loss or performance guarantees for production storage.
- R506 handles only synthetic append. Python production upserts/deletes,
  replay after retention, missed generation, 160 YAML effective config,
  installed PAPER build parity and long-running live sync remain open.
- P0-02 actual four-leg execution and P0-03 authenticated balances OPEN.
  REAL kill must stay ENGAGED until independently authorized readiness.
