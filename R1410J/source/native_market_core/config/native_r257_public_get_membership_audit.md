# Astra R257 — exact public market membership, 2026-10-22

Scope: PUBLIC GET-only evidence; NO production PAPER, no real orders, no private exchange GET/POST/CANCEL, no Telegram, no DB1/DB2 writes, no supervisor restart or deployed binary replacement.

## Preflight
- Source parent HEAD: e92dc112b2e892dd7d1f4fb01a5f3fc6030fd294.
- 32 historical dirty/untracked Git paths preserved. Their porcelain SHA256 excluding the R257-owned source path: 9d5a1608ce8912435df2269be7eeee6ac17bc178e3442dd137fd56c4beb6ebc4.
- Active supervisor PID 2161721, NRestarts 0, installed ELF SHA256 10138620181ebfd37dcb76b79c091c70d3315b5c536045fd6ea5c258aa0dbd46.
- Kill switch ENGAGED. No REAL permission.

## One scoped source change
- src/bin/native_paper_live_portfolio_v1.rs: add opt-in --r257-public-membership one-shot branch before the usual metadata→WS/PAPER flow.
- Only invokes the pre-existing fetch_current_metadata(1), which makes two credential-free HTTP GET requests:
  Binance /fapi/v1/exchangeInfo and OKX /api/v5/public/instruments?instType=SPOT.
- Requires exact R257_PUBLIC_ONLY_ALLOWED=YES and kill switch ENGAGED. Without permission it DENIES before HTTP. Other PAPER/REAL paths remain unchanged.
- Reports sorted actual instrument IDs, metadata static_reason, venue statuses, hash, local generation, loaded_wall_ms and self ELF/build identity. Explicitly declares no WS, orders, external attestation or deployed parity.
- Two new offline Rust tests: empty membership denied; synthetic ordered-set digest/eligible exclusion/false authority assertions. Both PASS.

## New public observation (not installed PAPER membership)
- Timestamp UTC: 2026-10-22T03:57:39.396+00:00.
- Current intersection 234 IDs; static eligible 234; static denied 0.
- Current sorted-base SHA256: 2a11356a67e0dd6dc6e0f6dbba6a6fcf209c40b8530f59b481f4cb325f451efa.
- Public Binance crypto active 524; OKX active 442; joint metadata ready 234.
- Historic source Oct 5: /mnt/spotarb_db_test/c15_review/native600_5/metadata_snapshots.json, 235 IDs, sorted-base SHA256 aa5e9c3004cc5ac0904fe1cc5ffab4bbe1336d39005c38675e6f019901e433af.
- Historicals ONLY: CAT, PROMPT. Current ONLY: CT. Intersection: 233 actual identical IDs.
- CT now has OKX CT-USDT live and Binance CTUSDT TRADING with static_reason none.
- CAT/PROMPT missing from current **intersection**; no proven root cause by venue.
- This is a change between two dates/policies, NOT proof that installed PAPER lost a subscribed instrument.

## Executed binary and verification
- Pre-commit candidate ELF SHA256 637b7d50ffb3b1df699d9eb369c948abe20c7bc1642ca418a25e3d3287e02e94.
- Candidate R157 first-party source-bundle SHA256 47f3ea23f5aee6a2b3460d98dbfc99d9d974e4522b669c6230e0f0d9345944db, Git parent dirty=true. This is local self-report, NOT independent release attestation.
- Probe output isolated in /tmp/spotarb_r257_public_membership_20261022.json, with no production writes.
- Targeted bin R257: 2 PASS. Full cargo test --offline --locked --lib: 1175 PASS, 2 ignored. Full --bins: 93 PASS, 0 FAIL. Total 1268 PASS, 2 ignored.
- No claim of all 160 YAML fields applied: 149 source-only research, 11 guarded, 0/160 positive installed per-field proof.

## Next
- Compare historical and current venue-by-venue membership, eligibility and provider delisting statuses if specifically needed. Do not infer cause from diff alone.
- Separate, explicitly scoped candidate PUBLIC-PAPER runtime run must not contend with/replace active supervisor. Original YAML source-mode is still fail-closed for unsupported switches.
- P0-01 forward DB1→DB2 and P0-02/P0-03 authentic exchange truth remain open; no REAL or private calls.
