# SPOT-ARB — architecture summary (source-linked; not a replacement for original specification)
Original global specification dated 2026-10-01: Exchange adapters -> Native ingest -> normalized events -> symbol ownership -> multi-exchange market state -> decision/risk -> execution -> durable history. Rust hot path must not depend on SQLite/Python/Telegram. DB1 canonical, DB2 independent mirror, execution outbox distinct.

Latest explicit user decision 2026-11-18: market data, books, negative signals, pretrade validation failures remain RAM-only. A positive signal goes through final Risk/Safety. After admission to execution, durable TRADE_INTENT must commit before the first order. Record all associated events thereafter, including rejected orders, partial fills, hedges, UNKNOWN, recovery, closure, fees and PnL. A failed or losing trade is not discarded.

Preserve source-quality validation, HOT/WARM, timestamps, checksum, sequence, generation, authority, 800ms freshness, bounded queues and fail-closed behavior. Existing historical ingress receipts remain recoverable.

Primary sources: original global spec (Library), R1320 01_GLOBAL_SPEC_RECOVERY.md and 07_IMPLEMENTATION_ROADMAP.md, R1326 ARCHITECTURE_RECONCILIATION.md, WRITE_PATH_AUDIT.md.
