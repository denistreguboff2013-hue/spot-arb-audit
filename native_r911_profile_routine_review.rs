// Astra P0-01 R911-R913: ordinary independent two-PID correctness
// tests, never installed PAPER, no ORIGINAL production DBs.
#[test] fn r911_two_pid_8192_at_900_replay1_ack16_wal1000(){
 r877_run_private(8192,900.0,false,true,1,16,1000);
}
#[test] fn r912_two_pid_8192_at_1200_replay16_ack32_wal1000(){
 r877_run_private(8192,1200.0,false,true,16,32,1000);
}
#[test] fn r913_two_pid_4096_at_450_replay1_ack16_wal1000(){
 r877_run_private(4096,450.0,false,true,1,16,1000);
}

// R928-R935: opt-in benchmark gate; NEVER promote a pressure-truncated
// correctness PASS to throughput certification. Kept IGNORED by default
// solely because it requires explicit guarded private harness and rate.
// Run with --ignored and ASTRA_R924_GUARD=1 ASTRA_R932_FAST_ACK=1,
// ASTRA_R936_RATE=450|900|1200, optionally ASTRA_R936_REPLAY_MIN and
// ASTRA_R936_ACK_MIN. Any lost acceptance or HistoryPressure is a FAIL.
#[test]
#[ignore = "private opt-in capacity benchmark; requires ASTRA_R924_GUARD and ASTRA_R932_FAST_ACK"]
fn r936_guarded_private_full_acceptance_capacity_gate(){
 assert_eq!(std::env::var("ASTRA_R924_GUARD").as_deref(),Ok("1"));
 assert_eq!(std::env::var("ASTRA_R932_FAST_ACK").as_deref(),Ok("1"));
 let rate:usize=std::env::var("ASTRA_R936_RATE").unwrap()
  .parse().expect("R936 450|900|1200");
 assert!(matches!(rate,450|900|1200));
 let replay:usize=std::env::var("ASTRA_R936_REPLAY_MIN")
  .unwrap_or_else(|_|"1".into()).parse().unwrap();
 let ack:usize=std::env::var("ASTRA_R936_ACK_MIN")
  .unwrap_or_else(|_|"16".into()).parse().unwrap();
 assert!((1..=32).contains(&replay)&&(1..=48).contains(&ack));
 let events=if rate==450{4096}else{8192};
 r877_run_private(events,rate as f64,true,true,replay as u64,ack as u64,1000);
}

// R961: optional 32768 event two-PID private original 17-table endurance.
// Guarantees all events accepted and full independent DB1/DB2/Owner parity;
// does NOT qualify the installed PAPER without 30min sustained evidence.
#[test]
#[ignore = "explicit guarded R961 endurance opt-in"]
fn r961_two_process_32768_exact_endurance_gate(){
 assert_eq!(std::env::var("ASTRA_R924_GUARD").as_deref(),Ok("1"));
 assert_eq!(std::env::var("ASTRA_R932_FAST_ACK").as_deref(),Ok("1"));
 assert_eq!(std::env::var("ASTRA_R954_FUSED").as_deref(),Ok("1"));
 let rate:usize=std::env::var("ASTRA_R961_RATE").unwrap()
  .parse().expect("R961 900|1200 events/s");
 assert!(matches!(rate,900|1200));
 r877_run_private(32768,rate as f64,true,true,16,16,1000);
}


// R965-R966 PRIVATE only, explicit 64-slot outbox release build endurance.
// R965 doubles the previously certified 32768 event history and enforces
// complete original 17-table physical DB1/DB2 and Owner restore <=65536.
// R966 applies deterministic 2-second 1800/s spikes / 2-second 600/s
// recovery, mean 1200/s; any refused envelope or under-speed fails red.
#[test]
#[ignore = "R965 explicit two-PID 65536 durable PAPER event endurance"]
fn r965_private_65536_1200_complete_parity_owner(){
 assert_eq!(std::env::var("ASTRA_R924_GUARD").as_deref(),Ok("1"));
 assert_eq!(std::env::var("ASTRA_R932_FAST_ACK").as_deref(),Ok("1"));
 assert_eq!(std::env::var("ASTRA_R954_FUSED").as_deref(),Ok("1"));
 assert_eq!(std::env::var("ASTRA_R965_EXTENDED").as_deref(),Ok("YES"));
 assert!(std::env::var_os("ASTRA_R966_BURST").is_none());
 r877_run_private(65536,1200.0,true,true,16,16,1000);
}
#[test]
#[ignore = "R966 explicit two-PID 1800/s spike, 600/s recovery gate"]
fn r966_private_48000_burst_1200_average_gate(){
 assert_eq!(std::env::var("ASTRA_R924_GUARD").as_deref(),Ok("1"));
 assert_eq!(std::env::var("ASTRA_R932_FAST_ACK").as_deref(),Ok("1"));
 assert_eq!(std::env::var("ASTRA_R954_FUSED").as_deref(),Ok("1"));
 assert_eq!(std::env::var("ASTRA_R965_EXTENDED").as_deref(),Ok("YES"));
 assert_eq!(std::env::var("ASTRA_R966_BURST").as_deref(),Ok("YES"));
 r877_run_private(48000,1200.0,true,true,16,16,1000);
}

// R967 strict empirically observed 2-second peaks, unlike scheduled R966.
// Every one of ten 2s high windows must actually commit at least 3200
// source events (>=1600/s), 48k all accepted, FULL DB1/DB2/Owner parity.
#[test]
#[ignore = "private measured 2-second arrival/commit burst gate"]
fn r967_measured_2second_10_cycle_burst_gate(){
 assert_eq!(std::env::var("ASTRA_R924_GUARD").as_deref(),Ok("1"));
 assert_eq!(std::env::var("ASTRA_R932_FAST_ACK").as_deref(),Ok("1"));
 assert_eq!(std::env::var("ASTRA_R954_FUSED").as_deref(),Ok("1"));
 assert_eq!(std::env::var("ASTRA_R965_EXTENDED").as_deref(),Ok("YES"));
 assert_eq!(std::env::var("ASTRA_R966_BURST").as_deref(),Ok("YES"));
 assert_eq!(std::env::var("ASTRA_R967_WINDOW").as_deref(),Ok("YES"));
 r877_run_private(48000,1200.0,true,true,16,16,1000);
}
