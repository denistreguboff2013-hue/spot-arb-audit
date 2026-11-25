# SYNTHETIC TRANSPORT FIXTURE
No suitable archived capture was selected for reuse. This is explicitly synthetic transport input, never a claimed exchange capture.
native_paper_engine_fixture_r1301a.rs is cfg(test) only. It constructs the existing Engine::new_volatile with unchanged QualityPolicy, actual registered connections, two sources per venue and actual subscription state. It replaces only external transport with generated public-channel JSON.
Three batches carry receive/event times anchored to the current clock: -300ms, -150ms, 0ms. This supplies actual min_samples=3 and >=250ms dwell without sleeping, changing HOT or executing a network client.
OKX: full snapshot sequence100 then updates101 and102. Binance: partial-depth snapshots100/101/102. Both use actual production decode and sequence logic. Levels change at each update.
Only actual Engine Frame/Book outputs are accepted by the unchanged Provider implementation. Fixture asserts two typed frames with genuine capabilities, sends them through the actual bounded queue and keeps Engine alive while consuming.
The fixture does not directly assign validated=true, HOT or production witness. Existing older R1301 contract fixtures remain clearly separate.
Limits: real clocks bound the wall time available to the test (<800ms for freshness, <100ms queue residence). A slow test host may fail closed; do not increase thresholds. No linked binary was executed by Astra.

# R1302 handoff — execution requires its separate authorization
Verify R1301A_GIT_STATE.txt, clean tree, persisted kill ENGAGED, REAL OFF, free space and unchanged 800ms first.
CWD: source/native_market_core. Target: /tmp/spotarb_r1296_compile.
Prepared commands (NOT run by Astra):
CARGO_TARGET_DIR=/tmp/spotarb_r1296_compile cargo test --offline --locked --lib r1301a_ -- --test-threads=1
CARGO_TARGET_DIR=/tmp/spotarb_r1296_compile cargo test --offline --locked --bin native_paper_live_portfolio_v1 r1301a_ -- --test-threads=1
The bridge module is also compiled into the actual entrypoint test target. Record duplicate lib/bin coverage honestly.
Then execute the previously unrun R1301 suite and affected R1298 entrypoint regressions only as authorized in R1302. Do not claim them already passed.
Check actual trace source/instrument/generations and queue IDs against owner receipts. Confirm all twenty negative outcomes, one owner, no orders/fills or legacy mutation. Check CRC and hashes before using artifacts.
Do not connect to exchanges or enable PAPER/REAL. No instrument-rules binding is included in R1301A.
