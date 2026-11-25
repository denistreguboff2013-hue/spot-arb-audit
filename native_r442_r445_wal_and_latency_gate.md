# R442–R445 — isolated WAL, lock contention, latency witness (2026-10-23)

## Ground truth and test-only guarantees
- Starting Git HEAD `77340148737806b03e106ba4b7978ca60e10d21a`.
- Scope: only owned 0700 `/tmp/astra_r366_isolated_*` and SQLite fixtures 0600. Source Python `/home/tmpadmin/spotbot/ai/collector.py` `_space_check` checks WAL and uses PASSIVE→TRUNCATE; no live database is used.
- R442 holds a real SQLite WAL reader transaction at the first committed event. A concurrent PAPER writer appends another event, `PRAGMA wal_checkpoint(PASSIVE)` leaves log frames uncheckpointed, and the reader sees its earlier snapshot. Reader release permits `TRUNCATE`; full R366 audit confirms both events and both outbox frames.
- R443 holds a private `BEGIN IMMEDIATE` writer lock; concurrent append returns bounded SQLite error (1-second busy timeout) without any events/outbox/seq gaps. After rollback, retry succeeds.
- R444 measures 40 serial isolated appends and validates complete chain. Single host sample (microseconds): first 10 mean 29884, last 10 mean 40754, median 36159, p95 49072. The last mean is ~36.4% higher. No SLA or scalability acceptance: this is a tiny sample and R366 runs full integrity audit before every append (likely growing total cost).
- R445 observes a live pinned, small WAL and verifies the independently injected admission sample at 15MiB rejects. It does NOT fill 15MiB on disk. Small WAL itself remains admissible and all events are still durable.

## Release blockers, not cleared by these tests
- Original Python may attempt PASSIVE then TRUNCATE after high WAL: isolated Rust intentionally fails closed at WAL 15MiB rather than attempt checkpoint and does not implement production locked maintenance retries.
- No hour/day/week reader-pin soak test, DB1/DB2 forward replication, source-configured secrets, existing schema migrations, long-duration p95/p99 under 10 concurrent and 500/day or 160-parameter live application.
- No comprehensive bound on append preaudit O(n²) work; need profile 100–1000+ events with appropriate resource caps and bounded incremental integrity with retained anti-corruption proof before installed PAPER adoption.
- REAL remains blocked, P0-02/P0-03 open, P0-01 PARTIAL. This work is source-only and not installed. No service restart, no disk cleanup, no live API calls.
