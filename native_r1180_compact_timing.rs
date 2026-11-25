// Opt-in measurement only. No market payloads, socket operations or trading authority.
static R1180_SOURCE_NS:AtomicU64=AtomicU64::new(0);
static R1180_SOURCE_MAX_NS:AtomicU64=AtomicU64::new(0);
static R1180_SOURCE_COUNT:AtomicU64=AtomicU64::new(0);
static R1180_FRAME_NS:AtomicU64=AtomicU64::new(0);
static R1180_FRAME_MAX_NS:AtomicU64=AtomicU64::new(0);
static R1180_FRAME_COUNT:AtomicU64=AtomicU64::new(0);
static R1180_SEAL_NS:AtomicU64=AtomicU64::new(0);
static R1180_SEAL_MAX_NS:AtomicU64=AtomicU64::new(0);
static R1180_SEAL_COUNT:AtomicU64=AtomicU64::new(0);
fn r1180_enabled()->bool{
    std::env::var("ASTRA_R1180_COMPACT_TIMING").ok().as_deref()==Some("YES")&&
    std::env::var("PAPER_R1177_DECISION_GATE").ok().as_deref()==Some("YES")&&
    std::env::var("PAPER_R1173_EXPERIMENTAL").ok().as_deref()==Some("YES")&&
    std::env::var("ASTRA_R1087_PAPER_PUBLIC_ONLY").ok().as_deref()==Some("YES")
}
fn r1180_record(start:Instant,sum:&AtomicU64,max:&AtomicU64,count:&AtomicU64){
    if !r1180_enabled(){return}
    let ns=u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX);
    sum.fetch_add(ns,Ordering::Relaxed);
    max.fetch_max(ns,Ordering::Relaxed);
    count.fetch_add(1,Ordering::Relaxed);
}
fn r1180_compact_snapshot()->serde_json::Value{
    let metric=|sum:&AtomicU64,max:&AtomicU64,count:&AtomicU64|serde_json::json!({
        "count":count.load(Ordering::Relaxed),
        "sum_us":sum.load(Ordering::Relaxed)/1000,
        "max_us":max.load(Ordering::Relaxed)/1000
    });
    serde_json::json!({"enabled":r1180_enabled(),
        "source_db1_db2_ack":metric(&R1180_SOURCE_NS,&R1180_SOURCE_MAX_NS,&R1180_SOURCE_COUNT),
        "frame_owner_decision_proof_seal":metric(&R1180_FRAME_NS,&R1180_FRAME_MAX_NS,&R1180_FRAME_COUNT),
        "decision_receipt_db1_db2_ack":metric(&R1180_SEAL_NS,&R1180_SEAL_MAX_NS,&R1180_SEAL_COUNT),
        "nested_seal_in_frame":true,"histogram_unavailable":true})
}
