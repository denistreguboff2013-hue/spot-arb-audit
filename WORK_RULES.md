# SPOT-ARB — MANDATORY WORK RULES
Version 1.0, 2026-11-18. Applies to every assistant, Work session, Astra/Luna audit, and future chat.
This document is operational guidance, NOT a substitute for the original Global Master Spec or permission to change trading policy.

## 1. Truth, evidence and scope
- Never invent Git state, tests, results, source content, process status, permissions, ACKs, or performed actions. Separate observed fact from inference and proposal.
- Verify the exact current source, logs and Git diff before changing code; reports are snapshots, not proof of current state.
- If evidence conflicts, document both and investigate; do not silently choose a convenient interpretation.
- Do not claim the entire cross-chat history was imported: this knowledge base is partial and must cite primary server files.

## 2. Mandatory startup checks
- Read START_HERE.md, this file, CURRENT_STATE.md, MASTER_SPEC.md, DECISIONS.md, DEFECT_REGISTER.md, NEXT_ACTIONS.md, WORK_LOG.md.
- Verify current Git HEAD, branch/status/diff, untracked files, relevant processes, disk free space, and actual safety controls before any mutation.
- Inspect pending work and recent reports; do not redo completed audits or tests without reason.
- Detect concurrent Work/other agent changes. If files have changed unexpectedly, stop and reconcile before editing.
- Keep all pre-existing untracked and modified files unless their owner explicitly authorizes changes.

## 3. Architectural invariants — never silently change
- Market data, raw WS frames, book state, negative signals, and pretrade rejects remain RAM-only for NEW events.
- Positive signal -> final Risk/Safety admission -> durable TRADE_INTENT -> first order -> durable full lifecycle, including failures, partial fills, UNKNOWN, hedges, exits and PnL.
- Never issue an order before confirmed intent durability; commit failure means fail-closed.
- Preserve DB1 canonical, DB2 independent mirror, ACK integrity, execution outbox and recovery of already-durable obligations. Never fake accepted_id, journal_id, source receipt, trade intent, or ACK.
- Preserve source quality, HOT/WARM, checksum, sequence, authority/generation, dedupe, bounded queues and 800 ms freshness threshold.
- Do not introduce mandatory manual instrument manifest, redundant per-frame SQLite writes, duplicate Execution Owner, or silent queue drops.
- Do not solve root architecture problems by simply enlarging queues, batching rejected market records, weakening checks, or raising timeouts.

## 4. Safety gates
- REAL OFF and kill switch ENGAGED. Never disable either.
- Do not mutate Production, DB1, DB2, UNIVERSUM or their active data.
- No public WS/PAPER/REAL run unless separately and explicitly authorized after successful offline acceptance. Offline tests use isolated temp paths/databases.
- Do not delete legacy pending, WAL, UNKNOWN exposure or outbox. Reconcile before changing recovery contracts.
- No unauthorized risk limit, trading strategy, exchange credentials or permission changes.
- Do not assume safety from an old report; check actual controls when necessary.

## 5. Safe execution
- Work in large logical stages using short, bounded server commands. Avoid enormous stdout, dumps, archives or recursive searches of the whole server.
- Keep logs, source, binaries and archives on server; never transfer them through chat/Base64/hex.
- Limit diagnostic excerpts to ~30-50 lines. Filter and summarize locally.
- Prefer focused edits and inspect diff after each change; do not overwrite unrelated work.
- Do not commit untested code as accepted. Never add unrelated/untracked files to a commit.
- Check cargo/test processes before starting new builds; do not rerun a build that is still running.
- Use targeted offline tests first; broader tests when justified. Report PASS/FAIL/SKIPPED/NOT RUN accurately.
- For hangs/tool limits, preserve checkpoint and report exact blocking step; no infinite retry loops.

## 6. Mandatory report AFTER EACH code or configuration edit
- Immediately append a compact entry to CHANGE_LOG.md before the next unrelated edit, even if compilation or testing is pending.
- Include timestamp, work ID, file(s), change purpose, observed pre/post Git state (HEAD and dirty status), exact tests and outcomes (or NOT RUN), safety impact, known risks, evidence paths and next step.
- If a change is reverted or fails, append a new correction entry; never erase history or rewrite failure as success.
- After each logical block, update CURRENT_STATE.md, DEFECT_REGISTER.md, NEXT_ACTIONS.md, WORK_LOG.md and any stage report. Make updates atomic where feasible.
- If a tool disconnects before reporting, next session must inspect Git diff and reconcile unlogged edits before further work.
- Avoid false precision: if a field is unverified, mark UNVERIFIED.

## 7. Commits, handoff and acceptance
- Before a commit: inspect exact staged files, diff --check, relevant compile/tests, and safety implications.
- After a commit: record full Git HEAD, changed files, test outcomes, known gaps, safety state and evidence paths.
- Each handoff starts with START_HERE.md and must use actual current Git, not assume the last reported HEAD.
- Never mark PAPER GO based only on a unit test or writer prototype; require integrated durable-before-order, RAM-only ingress, lifecycle/recovery and independent offline acceptance.
- Treat reports as evidence indexes; original Git history and source logs are authoritative for observed execution.

## 8. Knowledge-base maintenance
- This directory is a stable index, not a mandatory trading runtime manifest. No bot component should depend on these Markdown files for execution.
- Keep a single source of truth for each decision; link original files rather than copying large logs.
- Document the reason behind changes and what failed, so future sessions do not repeat mistakes.
- If updating the knowledge base fails, report it as an explicit handoff blocker rather than claiming documentation complete.

## 9. Division of labor and Work credit economy — user decision 2026-11-19
- Do NOT spend ChatGPT Work time/credits waiting for 5/60/180-minute LIVE SHADOW or other long unattended tests.
- The regular assistant via Desktop Commander prepares safety/resource checks, launches bounded detached server-side tests, and later inspects compact metrics and results when requested. The server process itself handles duration, termination and file output.
- Reserve Work for substantial Rust implementation, P0 durable/recovery fault-injection changes and integration; independent acceptance remains a separate read-only review.
- Work may prepare test code or a launcher when needed, but must not occupy an active Work session just to wait for elapsed test time. If a test is already running, inspect its state; do not duplicate it.
- Public market-data LIVE SHADOW only; REAL OFF, kill switch ENGAGED, no private APIs/orders, no Production/DB1/DB2/UNIVERSUM mutations. Respect bounded duration/resources/logs and PAPER NO-GO until accepted.
- The assistant cannot autonomously return later to inspect results without a new user turn or an explicitly scheduled automation.

## 10. Connected plugin/tool discovery and reuse — 2026-11-21
At the start of a new SPOT-ARB chat, discover and use actually connected tools when appropriate: Desktop Commander (server), Context7 (Rust/Tokio docs), Mermaid Chart (architecture visualization). Superpowers was reported installed by the user; verify availability before claiming use. The active tool list, not this note, establishes connection. Read CONNECTED_PLUGINS_AND_LATENCY_DIAGNOSTICS.md for historical WS/queue diagnosis; do not repeat completed tests. Plugin outputs never replace Git/source/runtime evidence.

## 11. Handoff discipline: reuse existing build; NO unauthorized experiment sprawl — user decision 2026-11-23
- Before proposing or making any code/build changes, recover the current experimental Rust checkout, ALL existing candidate components/tests, Git HEAD + dirty paths, current build/test processes, latest checkpoints and prior decisions. A handoff is NOT permission to restart architecture work or repeat completed tests.
- Maintain ONE agreed experimental implementation in the existing Rust project. Do not create extra checkout copies, parallel Engine variants, standalone prototype crates, build roots, queues, services, or repetitive Cargo builds as a default strategy. First reuse and complete the existing work.
- A necessary targeted compile/test in the EXISTING build context is allowed only after verifying no duplicate running job, capacity, inputs and prior results. New independent implementation/build contexts require a documented concrete blocker with the existing context AND explicit user approval BEFORE creating them.
- Do not introduce queue capacity changes, new queue limits/backpressure semantics, drop/overwrite strategies, execution ownership, HOT authority, trading or architecture changes without comparing historical contracts and obtaining user approval. Existing bounded channels and fail-closed behavior are historical constraints, not authorization to add new ones. Never hide source latency by enlarging or constraining queues.
- First produce a factual component/ownership map and minimal integration plan referencing specific EXISTING files and preserving Engine as the only final safety/authority/decision gate. Integrate HOT/Connection Manager and Market Data Processor into those components only after reconciling confirmed design decisions.
- Every new-chat handoff must identify canonical checkout/build/test paths, existing artifacts and their owner, precise latest HEAD/dirty files/process status, tests PASS/FAIL/NOT RUN, safety, open defects and next minimal step. Clearly separate documented decisions from proposals. Never silently grant the next chat broader authority than the user granted.
- If an earlier handoff conflicts with the user's later correction, flag the conflict and follow the correction. In particular R1377 05_CORRECTION_NO_EXPERIMENT_SPRAWL.md takes priority over R1377 instructions suggesting isolated copies/targets. Read CHAT_HANDOFFS/HANDOFF_RULES.md at every new chat.
- If an unauthorized experimental artifact already exists, inventory and mark it NOT APPROVED; do not delete, merge, or continue it without user instruction. Preserve work of other concurrent sessions.
