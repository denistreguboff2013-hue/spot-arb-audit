# SPOT-ARB R386-R395 — Python canonical hash parity and PAPER rollout gate
Date: 2026-10-22. Server spb-3-vm-4r2s.
Review branch review/native-core-v3-r31-entry-permit-dispatch-20261008.
Starting HEAD ccac3c98d9fdf079798ed304b0f9718e85198e52.
Historical 32 dirty/untracked paths SHA256 9d5a1608ce8912435df2269be7eeee6ac17bc178e3442dd137fd56c4beb6ebc4, staging empty at start.
Installed PAPER active supervisor PID 2161721, NRestarts 0; initial child 2919927; installed ELF SHA256 10138620181ebfd37dcb76b79c091c70d3315b5c536045fd6ea5c258aa0dbd46. Real kill switch ENGAGED. No installed service or REAL changes. Disk3 available ~5.4 GiB (82% used).

## Evidence and actual code
- Read original Python ai/schemas.py Event.record: inner payload_json and verification_json use json.dumps(value,ensure_ascii=False,allow_nan=False), default insertion order and whitespace.
- Read original Python ai/collector.py append_record: content_hash SHA256(json.dumps(record,ensure_ascii=False,sort_keys=True).encode()), with default comma+space, colon+space.
- Read original ai/mirror_outbox.py canonical: json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False), used for payload_hash and row after_hash.
- Generated config/native_r386_python_hash_oracle.json using a safe read-only Python script and FIVE accepted synthetic Event.record oracle cases from the prior R356 file. It includes original Python record bytes and SHA256, and a synthetic mirror_outbox frame plus one row after_hash. Never collected real secrets.
- Added isolated, pure Rust source native_python_json_canonical_r386_review.rs; no filesystem, SQLite, network or exchange access in these functions. Python spaced/sorted collector JSON and compact/sorted mirror canonical JSON are separate routines.
- Python ORACLE cross-check r386_original_python_five_record_hashes_and_outbox: 5/5 EVENT source bytes and SHA256 and 1/1 outbox payload bytes and SHA256 matched. The number of Rust tests is not 6; this is ONE test with multiple fixtures. Second test refuses floating-point numeric encoding, tests Unicode and escaped strings.
- IMPORTANT: this is a bounded integer/string/null/bool serialization subset. Unsupported f64 values FAIL CLOSED rather than claiming total Python float parity. Python input map insertion order is not retained by Rust serde_json default Value, so inner payload_json/verification_json still lack exact source byte parity.
- Initially patched isolated R366/R376 writers to use the new serializer and attempted to add complete row-image operations. Five existing R366 tests and six R376 tests passed with first serializer adapter, but the subsequent row-image upgrade was not fully accepted by the remote edit protection; it was NOT committed.
- For safety, reverted the partially integrated changes to R366 and R376 back to committed state. The standalone new Rust R386 module is included in R296 research namespace for its fixture tests. This result is RESEARCH ONLY, not a production writer upgrade.
- Full offline regression completed with exit 0: Rust library 1220 PASS, 0 FAIL, 2 ignored; binary 98 PASS, 0 FAIL across 81 bin groups; unique total 1318 PASS, 0 FAIL, 2 ignored. R386 two targeted Rust PASS are INCLUDED in 1220 library total. Before scoped commit, the original 32 historical paths SHA256 was unchanged and staging=0.
- Scoped commit only source native_python_json_canonical_r386_review.rs, fixture JSON, R296 include change and this audit; never git add -A or reset/stash old work.

## Release decision
NOT READY TO DEPLOY into installed PAPER. New helper does not yet ensure Python byte-exact inner JSON, full before/after row image protocol, DB2 application, real installed YAML parity, configured-secret plumbing, SIGKILL/powerfail restart and historical cross-day position quarantine reconciliation. An independent installed test candidate, immutable hash, rollback and controlled PAPER switch will be required only when component gates are met.
Original Astra P0 issues and all 13 findings remain subject to their individual evidence gates. No live exchange orders, no private-key use, no production DB writes, no Telegram changes, no service restart.
Next R396+: confirm byte-for-byte inner payload/verification bytes including preservation of Python insertion order; Unicode/float/Decimal parity, real Python after() row SHA against SQLite synthetic rows, and complete outbox source protocol, then SIGKILL isolated rehearsals and explicit PAPER release gates. Do not deploy partial R386 helper.
