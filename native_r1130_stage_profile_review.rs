// R1130 timing-only PRIVATE PAPER audit. No persistence/ACK decisions change.
// Wall-clock durations are accumulated per-process. Outer spans overlap
// inner spans; never sum the whole vector as total pipeline latency.
const R1130_LABELS:[&str;27]=[
 "src_owner_prepare","src_backpressure_wait","src_build_envelope",
 "src_db1_total","src_owner_publish","db1_preflight",
 "db1_json_canonical","db1_apply_rows","db1_sqlite_commit",
 "db2_preflight","db2_apply_rows","db2_sqlite_commit",
 "ack_preflight","ack_verify_outbox","ack_parity_rows","ack_commit",
 "db2_read_head","db2_chain_verify","db2_stmt_prepare",
 "ack_guard_checks","ack_read_head","ack_chain_verify",
 "db2_fused_validate_and_apply",
 "r1134_immutable_read","r1134_worker_full_verify",
 "r1134_db2_apply","r1134_wait_helper"
];
static R1130_NS:std::sync::OnceLock<[std::sync::atomic::AtomicU64;27]>=std::sync::OnceLock::new();
static R1130_CALLS:std::sync::OnceLock<[std::sync::atomic::AtomicU64;27]>=std::sync::OnceLock::new();
fn r1130_ns()->&'static [std::sync::atomic::AtomicU64;27]{
 R1130_NS.get_or_init(||std::array::from_fn(|_|std::sync::atomic::AtomicU64::new(0)))
}
fn r1130_calls()->&'static [std::sync::atomic::AtomicU64;27]{
 R1130_CALLS.get_or_init(||std::array::from_fn(|_|std::sync::atomic::AtomicU64::new(0)))
}
static R1130_ENABLED:std::sync::OnceLock<bool>=std::sync::OnceLock::new();
fn r1130_enabled()->bool{
 *R1130_ENABLED.get_or_init(||{
   std::env::var("ASTRA_R1130_STAGE_PROFILE").ok().as_deref()==Some("YES")&&
   std::env::var("ASTRA_R1119_FULL_RUST_PAPER").ok().as_deref()==Some("YES")&&
   std::env::var("ASTRA_R1087_PAPER_PUBLIC_ONLY").ok().as_deref()==Some("YES")
 })
}
fn r1130_measure(idx:usize,from:std::time::Instant){
 if !r1130_enabled(){return}
 let ns=from.elapsed().as_nanos().min(u64::MAX as u128) as u64;
 r1130_ns()[idx].fetch_add(ns,std::sync::atomic::Ordering::Relaxed);
 r1130_calls()[idx].fetch_add(1,std::sync::atomic::Ordering::Relaxed);
}
pub fn r1130_stage_timing()->serde_json::Value{
 use std::sync::atomic::Ordering;
 serde_json::json!({
  "schema":"SPOTARB/R1130/STAGE-TIMING/V1",
  "enabled":r1130_enabled(),
  "nested_spans_not_additive":true,
  "labels":R1130_LABELS,
  "sum_ns":r1130_ns().iter().map(|x|x.load(Ordering::Relaxed))
     .collect::<Vec<_>>(),
  "samples":r1130_calls().iter().map(|x|x.load(Ordering::Relaxed))
     .collect::<Vec<_>>()
 })
}
