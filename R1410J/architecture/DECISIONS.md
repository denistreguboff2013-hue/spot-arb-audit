# SPOT-ARB — decision chronology (partial)
2026-10-01: original RAM-first Rust market hot path, durable history separate from market processing (original Global Master Spec).
2026-10-27: historical architecture decisions; see SPOT_ARB_ARCHITECTURE_DECISIONS_AND_NEXT_STEPS_20261027.md for exact wording.
2026-10-31: R1205 source-ingress addendum introduced durable Accepted raw payload and receipt obligations; see source addendum. This is a later architectural deviation relevant to SQLite load.
2026-11-17: R1325 integrated PAPER; evidence shows heavy accepted/quarantined source storage and no orders in PAPER06.
2026-11-18: user explicitly reconfirmed RAM-only market stream and trade-triggered durable lifecycle. Positive signal is the trigger for consideration; durable intent required after final Risk/Safety admission and before order submission. Do not persist rejected pretrade frames as mandatory trading history.

Conflict handling: current direct user decision governs NEW trade flow, while already committed durable records and safety guarantees must remain recoverable. No silent reinterpretation of original documents.

## R1399 FINAL checkpoint — 2026-11-24 UTC
2026-11-24 USER approved trying ONE dedicated UCM/HOT worker with bounded reliable event channel inside existing Rust crate. Engine remains unique final trading safety owner; no permission to change 800ms freshness, Queue16/32 caps, trading rules, PAPER/REAL or Production/DB. R1399 worker currently OPTIONAL offline tested, not yet live integrated.

## R1402D public sensor preflight — 2026-11-24 UTC
R1402D preflight (2026-11-24T08:04:28.249012+00:00): user authorized 900s public quote-only test first; existing ingress sensors verified, new UCM internal/roundtrip counters missing. Source unchanged, run not launched. Add minimal bounded diagnostics and rebuild existing quote runner before guarded 900s; no full-universe offline replay prerequisite. See CHAT_HANDOFFS/R1377_ENGINE_SPLIT_20261123/38_R1402D_PUBLIC_900S_SENSOR_PREFLIGHT_20261124.md.

## R1410B checkpoint
R1410B 2026-11-24T17:27:38.049285+00:00: user approved analysis/authorization separation, verified Python observation-only shadow path; narrow ordinary Binance observation path and duplicate prepare removal implemented. Existing execution gate unchanged, no new queue. Build PID2377273. Full validation pending. Report /mnt/spotarb_disk2/SPOT_ARB_KNOWLEDGE_BASE/CHAT_HANDOFFS/R1377_ENGINE_SPLIT_20261123/82_R1410B_OBSERVATION_EXECUTION_CHECKPOINT_20261124.md.

## R1410B validation / full235 gate — 2026-11-24T17:35:23.880328+00:00
User authorized real public quotes all235 AFTER fixes/offline gate, no orders/PAPER/REAL. Final lineage PASS; 158 distinct functional PASS across typed/Bridge/Engine/UCM, first failure test clock corrected only, no runtime edits this turn. Point5 PARTIAL, actual integrated census/startup/4096 and ordinary race checks, previous overflow/diagnostic gates open. R1347 exact235 archive set verified, current40 manifest unchanged; existing volatile bound233 and runner40 require explicit test-only existing shard topology, no limit/manifest changes. Report /mnt/spotarb_disk2/SPOT_ARB_KNOWLEDGE_BASE/CHAT_HANDOFFS/R1377_ENGINE_SPLIT_20261123/83_R1410B_LINEAGE_RESULT_AND_235_GATE_20261124.md. HEAD6d4d403c35e4893224c8f0f0541718ec831fa2dd. REAL OFF kill ENGAGED PAPER NO-GO.

## Dynamic universe correction
235 — результат предыдущего сканирования бирж штатным ботом, НЕ фиксированный список и НЕ требуемое число. Итоговая проверка должна использовать весь актуальный набор, найденный штатным сканером по существующим правилам; число может быть больше или меньше 235. Не заменять сканирование архивом R1347, историческим fixture40, произвольным top-N или ручной выборкой. Проверить фактический путь сканера и его утверждённые eligibility rules; не менять их или UNIVERSUM. Сохранить отдельный read-only снимок найденного набора и критерии отбора, фактический count, затем подписать все найденные инструменты в разрешённом тестовом контуре одновременно. Если текущий доступный сканер/лимит не соответствует утверждённому контракту, выявить причину, не обходить молча. Предыдущее разрешение пользователя на public quotes относится к всему актуальному universe после исправлений/offline gate. REAL OFF, kill ENGAGED, PAPER NO-GO. Это уточнение имеет приоритет над формулировками о фиксированных235 в предыдущих handoff и плане, остальные шесть пунктов без изменений.
