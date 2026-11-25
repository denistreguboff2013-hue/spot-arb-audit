# R1402 continuation — actual Engine selection and applied switch boundary
UTC: 2026-11-24T07:36:37.486682+00:00. Status PARTIAL, P0 OPEN, PAPER NO-GO.
Actual existing Engine changed; worker still NOT connected. No claim of latency improvement.

## Source changes
market_data_ingress_r1205.rs: existing select_inner now calls prepare_hot_selection, original synchronous manager choice, finish_hot_selection. HotSelectionInputs retains ordered scopes, readiness, prior, candidates and initial wait. Actual source switch now separates prepare_hot_switch (existing epoch allocation and pre-switch diagnostics), apply_local_hot_switch (real set_active success), and publish_applied_hot_switch (Reset and selected). Added prior selection/session/epoch and current manager-active/candidate-book identity mismatch rejection with scope authority removal. Existing same-source commit and final Permit logic unchanged. No fake worker ACK. No worker invocation in main Engine/runtime.
market_data_ingress_r1205_tests.rs: actual Engine fault test proves no selected/selection audit publication at prepare or apply; closing candidate before publish rejects Reset and removes scope authority. Test-only synthetic clock set to None following established fixture.
Edits A-D recorded individually in CHANGE_LOG.md. Original pre-edit source backups preserved in this handoff.

## Verification
A: R1402B_ENGINE_FULL.exit=0, 104/104 existing tests PASS.
B/C: R1402B_ENGINE_FINAL.exit=101, 104 original tests PASS, new test failed before target fault at synthetic-clock warmup. D corrected only test clock; production code unchanged. R1402B_TARGETED_RETRY.exit=0, new test 1/1 PASS. Combined final production-source evidence 105 distinct Engine tests PASS, not a single green 105-case execution. No needless full rerun after test-only fix.
R1402B_LIB_CHECK.exit=0, cargo check --offline --locked --lib. UCM34/34 prior checkpoint remains unchanged; not rerun. git diff --check PASS.
HEAD 6d4d403c35e4893224c8f0f0541718ec831fa2dd, 32 inherited dirty paths (15 tracked,17 untracked), no commit. Existing Cargo target reused. REAL OFF config false; kill ENGAGED; spotbot and PAPER services inactive; no public WS/PAPER/REAL/order launch or protected path edits.

## Next actual work
Do not repeat worker protocol construction or selection split. Integrate actual Engine one-owner handoff: transfer the existing UCM into the same worker (no concurrent mutable manager), route validated source lifecycle/observation and BookReset events, replace synchronous reads with current immutable projections, and drive existing snapshot/history/proposal/applied-ACK pipeline from runtime without blocking Queue32. Reuse prepare_hot_selection and actual applied-switch publication boundary above. Maintain exact audit context/order, no Permit for missing/superseded ACK, reconcile worker-active versus last Engine selected with explicit failclosed state; final generation/witness/highwater/dual-HOT/800ms gate at real publish. No extra queue/checkout/target or threshold/trading policy change. Need actual integrated Engine and local WSS offline tests before any public quote-only test. P0 OPEN.
