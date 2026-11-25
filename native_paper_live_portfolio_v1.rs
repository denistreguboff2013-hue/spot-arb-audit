//! R21 unified live-public native shadow.
//! Public market data + read-only production safety facts. No credentials, private REST or order transport.

#![recursion_limit = "256"]

// Never produce the trading entrypoint with injectable Engine time.
#[cfg(all(feature="offline-test-clock",not(test)))]
compile_error!("offline-test-clock is only permitted in the test harness, not the trading runtime");

#[path="../intake_r1204.rs"]
mod intake_r1204;
use futures_util::{SinkExt, StreamExt};
use spotarb_native_market_core::native_paper_portfolio_review::PaperPortfolio;
use spotarb_native_market_core::r1246_dual_archive::DualArchive;
use spotarb_native_market_core::native_isolated_sqlite_mirror_r147_review::{R819OriginalPaperHistory,r1088_commit_prepared_multishard_batch,r1130_stage_timing};
use spotarb_native_market_core::native_r1173_compact_writer::{Inject as R1173Inject};
use spotarb_native_market_core::native_r1173_compact_writer::ingress_store_r1205::StoreWorker;
use spotarb_native_market_core::r1241_ingress_pipeline as r1241_pipeline;
use spotarb_native_market_core::native_r155_source_paper_lane_review::SourcePaperLaneR155;
use spotarb_native_market_core::native_r167_source_ws_shards_review::{split_source_groups_for_ingress,SourceWsShardPlanR167};
use spotarb_native_market_core::native_binance_ws_ip_guard_r1110::{
    r1110_reserve_at_lock,R1110Outcome,
};
use spotarb_native_market_core::native_build_provenance_r157_review::candidate_build_provenance;
use spotarb_native_market_core::native_paper_strategy_frame_review::SupervisedStrategyPaper;
use spotarb_native_market_core::native_source_paper_checkpoint_r153_review::
    save_source_paper_review;
use spotarb_native_market_core::native_r178_candidate_source_identity_review::
    CandidateSourceIdentityR178;
use spotarb_native_market_core::native_public_turnover_ws_r123_review::{
    PublicWsEnvelope,parse_ws_ticker,
};
use spotarb_native_market_core::native_public_cold_boot_r130_review::PublicColdBootR130;
use spotarb_native_market_core::native_public_ws_owner_topology_r126_review::{
    PublicTickerTopologyR126,TickerSubscription,
};
use spotarb_native_market_core::native_trading_config::NativeTradingConfig;
use spotarb_native_market_core::native_r176_source_runtime_manifest_review::embedded_r176;
use spotarb_native_market_core::native_r258_source_admission_diagnostics::review_two_paper_admissions_r258;
use spotarb_native_market_core::native_r286_python_rust_consumers_review::audit_existing_consumer_boundaries_r286;
use spotarb_native_market_core::native_paper_config_bridge::{
    admit_public_paper_yaml_review,bind_public_paper_temporal_policy_review,
    bind_public_paper_decision_config,verify_paper_portfolio_quota_parity,
    PaperTemporalPolicyReview,verify_public_paper_decision_model_parity,
};
use spotarb_native_market_core::native_paper_continuity_checkpoint_review::{
    save_checkpoint,load_planned_handoff,
};
use spotarb_native_market_core::native_virtual_four_leg_provenance_review::
    ModeledFourLegJournal;
use spotarb_native_market_core::native_r1116_durable_model_notice::
    r1116_commit;
use serde_json::{json, Value};
use spotarb_native_market_core::{
    decision::{Decision, DecisionConfig, DecisionCore},
    market_metadata::{fetch_current_metadata, NativeMetadataSnapshot},
    owner_index, CoreError, HistorySink, Owner, RawFrame, Update, Venue,
    safety::{evaluate as evaluate_safety, SafetyInput},
    execution::{Coordinator, CoordinatorState},
    transport::monotonic_ns,
};
use spotarb_native_market_core::{parse as parse_market_frame, owner_index as market_owner_index};
use spotarb_native_market_core::decision::{Limits as DecisionLimits, RouteSpec};
use spotarb_native_market_core::native_paper_execution_r1289::{
    DurablePaper,
};
use spotarb_native_market_core::native_paper_risk_lifecycle_r1287 as paper_risk;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs::{self, File},
    io::{BufWriter, Write},
    path::PathBuf,
    sync::{Arc, atomic::{AtomicBool, AtomicU64, Ordering}},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

const OWNERS:usize=12;
const SHARD:usize=32;
const R1125_QUEUE_CAP:usize=16_384;
const R1246_COHORT_WIDTH:usize=40;

#[derive(Clone,Debug,PartialEq,Eq)]
enum RuntimeMode {
    Legacy,
    PublicObservation { manifest:PathBuf,journal_root:PathBuf },
    IntegratedPaper { manifest:Option<PathBuf>,journal_root:PathBuf },
    ExecutionIsolated { input:PathBuf, journal_root:PathBuf },
}
#[derive(Clone,Debug,PartialEq,Eq)]
struct RuntimePlan { mode:RuntimeMode }

fn parse_runtime_plan(args:&[String], conflicting_flags:&[&str])->Result<RuntimePlan,String>{
    let selectors=args.iter().filter(|a|a.starts_with("--paper-runtime=")).collect::<Vec<_>>();
    if selectors.is_empty(){return Ok(RuntimePlan{mode:RuntimeMode::Legacy})}
    if selectors.len()!=1{return Err("duplicate PAPER runtime selector".into())}
    let mode=selectors[0].strip_prefix("--paper-runtime=").unwrap_or("");
    match mode {
        "legacy"=>{
            if args.len()!=1{return Err("legacy runtime selector conflicts with extra arguments".into())}
            Ok(RuntimePlan{mode:RuntimeMode::Legacy})
        },
        "public-observation"=>parse_observation_args_r1301(args,conflicting_flags),
        "integrated-paper"=>{
            if !conflicting_flags.is_empty(){return Err("integrated PAPER conflicts with legacy flags".into())}
            let mut manifest=None;let mut root=None;
            for arg in args {
                if arg=="--paper-runtime=integrated-paper"{continue}
                if let Some(v)=arg.strip_prefix("--integration-manifest="){if manifest.replace(PathBuf::from(v)).is_some(){return Err("duplicate manifest".into())}}
                else if let Some(v)=arg.strip_prefix("--journal-root="){if root.replace(PathBuf::from(v)).is_some(){return Err("duplicate root".into())}}
                else{return Err("integrated PAPER unknown argument".into())}
            }
            Ok(RuntimePlan{mode:RuntimeMode::IntegratedPaper{manifest,journal_root:root.ok_or("missing journal root")?}})
        },
        "execution-isolated"=>{
            if let Some(flag)=conflicting_flags.first(){return Err(format!("isolated runtime conflicts with {flag}"))}
            let mut input=None;let mut journal_root=None;
            for arg in args.iter().skip(1){
                if let Some(v)=arg.strip_prefix("--offline-input="){
                    if input.replace(PathBuf::from(v)).is_some(){return Err("duplicate offline input".into())}
                }else if let Some(v)=arg.strip_prefix("--journal-root="){
                    if journal_root.replace(PathBuf::from(v)).is_some(){return Err("duplicate journal root".into())}
                }else{return Err("execution-isolated accepts only offline input and journal root".into())}
            }
            let input=input.ok_or("execution-isolated requires --offline-input")?;
            let journal_root=journal_root.ok_or("execution-isolated requires --journal-root")?;
            if input.as_os_str().is_empty()||journal_root.as_os_str().is_empty(){return Err("empty isolated path".into())}
            Ok(RuntimePlan{mode:RuntimeMode::ExecutionIsolated{input,journal_root}})
        },
        _=>Err("unknown PAPER runtime; expected legacy or execution-isolated".into())
    }
}

fn runtime_conflicting_flags()->Vec<&'static str>{
    [
      "PAPER_NATIVE_RUST_CONFIG_REVIEW","PAPER_R1088_PRIVATE_BATCH",
      "ASTRA_R1205_INGRESS","PAPER_VIRTUAL_FOUR_LEG_JOURNAL_PATH",
      "PAPER_PLANNED_RESTORE","PAPER_CONTINUITY_PATH",
      "PAPER_GRACEFUL_STOP_PATH","PAPER_R1173_EXPERIMENTAL",
      "PAPER_R819_PRIVATE_ROOT","PAPER_R1119_PRIVATE_ROOT",
      "R1266_PUBLIC_PAPER_SOAK","R1266_PUBLIC_PAPER_PREFLIGHT",
      "ASTRA_R1116_PRIVATE_NOTICE_ROOT"
    ].into_iter().filter(|name|std::env::var_os(name).is_some()).collect()
}
include!("../native_r1150_tokio_scheduler_profile.rs");
include!("../native_r1180_compact_timing.rs");
// R1127: timing only, no alteration to source/DB2 commit or ordering.
static R1127_DB1_TX_NS:AtomicU64=AtomicU64::new(0);
static R1127_DB1_TX_COUNT:AtomicU64=AtomicU64::new(0);
static R1127_DB1_TX_MAX_NS:AtomicU64=AtomicU64::new(0);

fn wall_ms()->i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as i64
}
fn cpu_s()->f64 {
    let mut u:libc::rusage=unsafe{std::mem::zeroed()};
    unsafe{libc::getrusage(libc::RUSAGE_SELF,&mut u)};
    (u.ru_utime.tv_sec+u.ru_stime.tv_sec) as f64+
        (u.ru_utime.tv_usec+u.ru_stime.tv_usec) as f64/1e6
}
fn rss_kib()->u64 {
    fs::read_to_string("/proc/self/status").ok().and_then(|s|
        s.lines().find(|l|l.starts_with("VmRSS:"))
            .and_then(|l|l.split_whitespace().nth(1))
            .and_then(|x|x.parse().ok())).unwrap_or(0)
}

struct NoHistory{accepted:u64}
impl HistorySink for NoHistory {
    fn accept(&mut self,_:&RawFrame,_:&Update)->Result<(),CoreError>{self.accepted+=1;Ok(())}
}
// R823: candidate-only gated integration of the ORIGINAL Rust 17-table
// DB1/outbox sink, never enabled for installed continuous PAPER by default.
// No sample/drop allowed when opt-in history cannot durably commit.
enum PaperHistoryR823 {
    Legacy(NoHistory),
    PrivateOriginal(R819OriginalPaperHistory),
    // R1173: explicitly opted-in bounded experimental PUBLIC-PAPER only.
    PrivateCompact(spotarb_native_market_core::custody_executor_r1241::CustodyWorker),
}
impl PaperHistoryR823 {
    async fn from_env()->Result<Self,String>{
        match std::env::var("PAPER_R1173_PRIVATE_ROOT"){
            Ok(root)=>{
                if std::env::var_os("PAPER_R819_PRIVATE_ROOT").is_some()||
                   std::env::var("PAPER_R1173_EXPERIMENTAL").ok().as_deref()!=Some("YES")||
                   std::env::var("PAPER_R1175_MODELED_DURABLE").ok().as_deref()!=Some("YES")||
                   std::env::var("PAPER_R1177_DECISION_GATE").ok().as_deref()!=Some("YES")||
                   std::env::var("ASTRA_R1087_PAPER_PUBLIC_ONLY").ok().as_deref()!=Some("YES")||
                   std::env::var("PAPER_R1088_PRIVATE_BATCH").ok().as_deref()!=Some("YES")||
                   std::env::var("PAPER_PLANNED_RESTORE").ok().as_deref()!=Some("0"){
                    return Err("R1173 isolated compact PAPER opt-in denied".into())
                }
                return spotarb_native_market_core::custody_executor_r1241::CustodyWorker::open(PathBuf::from(root),false).await
                    .map(Self::PrivateCompact)
                    .map_err(|e|format!("R1173 private compact source refused: {e}"))
            },
            Err(std::env::VarError::NotPresent)=>{},
            Err(_)=>return Err("R1173 private root non-UTF8 denied".into()),
        }
        match std::env::var("PAPER_R819_PRIVATE_ROOT"){
            Err(std::env::VarError::NotPresent)=>Ok(Self::Legacy(NoHistory{accepted:0})),
            Err(_)=>Err("R819 invalid Unicode private fixture root".into()),
            Ok(root)=>{
                if std::env::var("PAPER_R819_REVIEW_ONLY").ok().as_deref()!=Some("YES"){
                    return Err("R819 private DB1 source requires exact review-only opt-in".into())
                }
                let run=std::env::var("PAPER_VIRTUAL_FOUR_LEG_SESSION_ID")
                    .unwrap_or_else(|_|"paper_private_review".into());
                R819OriginalPaperHistory::new(std::path::Path::new(&root),&run)
                    .map(Self::PrivateOriginal)
                    .map_err(|e|format!("R819 private history source denied: {e:?}"))
            }
        }
    }
    fn is_private(&self)->bool{matches!(self,Self::PrivateOriginal(_)|Self::PrivateCompact(_))}
    async fn accepted_count(&self)->Result<u64,String>{match self{
        Self::Legacy(h)=>Ok(h.accepted),
        Self::PrivateOriginal(h)=>Ok(h.accepted_count()),
        Self::PrivateCompact(h)=>h.call(|w|Ok(w.cursor()?.1)).await,
    }}
}
impl HistorySink for PaperHistoryR823{
    fn accept(&mut self,frame:&RawFrame,update:&Update)->Result<(),CoreError>{
        match self{
            Self::Legacy(v)=>v.accept(frame,update),
            Self::PrivateOriginal(v)=>v.accept(frame,update),
            // R1173 only supports vetted atomic source batches.
            Self::PrivateCompact(_)=>Err(CoreError::HistoryPressure),
        }
    }
}

// R1088/R1129: DB1 commit PRECEDES decision publication.
// The 32-event envelope is authorized ONLY by isolated public PAPER opt-in.
fn r1129_paper_batch32()->bool{
    let original=std::env::var("ASTRA_R1129_BATCH32_PAPER").ok().as_deref()==Some("YES")&&
        std::env::var("ASTRA_R1119_FULL_RUST_PAPER").ok().as_deref()==Some("YES")&&
        std::env::var("ASTRA_R1087_PAPER_PUBLIC_ONLY").ok().as_deref()==Some("YES");
    // R1179 isolated public PAPER comparison only: never silently
    // change the installed R1119 or original 17-table source.
    let compact=std::env::var("ASTRA_R1179_COMPACT_BATCH32").ok().as_deref()==Some("YES")&&
        std::env::var("ASTRA_R1119_FULL_RUST_PAPER").is_err()&&
        std::env::var("PAPER_R1173_PRIVATE_ROOT").is_ok()&&
        std::env::var("PAPER_R1173_EXPERIMENTAL").ok().as_deref()==Some("YES")&&
        std::env::var("PAPER_R1175_MODELED_DURABLE").ok().as_deref()==Some("YES")&&
        std::env::var("PAPER_R1177_DECISION_GATE").ok().as_deref()==Some("YES")&&
        std::env::var("PAPER_R1088_PRIVATE_BATCH").ok().as_deref()==Some("YES")&&
        std::env::var("ASTRA_R1087_PAPER_PUBLIC_ONLY").ok().as_deref()==Some("YES")&&
        std::env::var("PAPER_PLANNED_RESTORE").ok().as_deref()==Some("0")&&
        std::env::var("R21_SECONDS").ok().as_deref()==Some("120");
    original||compact
}
async fn r1088_flush_queued_frames(
 pending:&mut Vec<(RawFrame,Update,usize)>,owners:&mut Vec<Owner>,
 history:&mut PaperHistoryR823,
 durable:&mut VecDeque<Event>,tx_count:&mut u64
)->Result<(),String>{
 if pending.is_empty(){return Ok(())}
 if pending.len()>(if r1129_paper_batch32(){32}else{16}){
     return Err("R1088/R1129 batch overflow".into())
 }
 let compact=matches!(history,PaperHistoryR823::PrivateCompact(_));
 let started=Instant::now();
 let (seq,committed)=match history{
   PaperHistoryR823::PrivateOriginal(h)=>
     r1088_commit_prepared_multishard_batch(h,owners,pending)
       .map_err(|e|format!("R1088 original DB1 failed: {e:?}"))?,
   PaperHistoryR823::PrivateCompact(w)=>{
     let r1180_started=Instant::now();
     let mut owned=std::mem::take(owners);let frames=pending.clone();
     let (seq,returned)=w.call(move|writer|{
       let seq=writer.source_commit_guarded(&mut owned,&frames,&[],R1173Inject::None)?;
       Ok((seq,owned))
     }).await.map_err(|e|format!("R1173 compact DB1 DB2 ACK denied: {e}"))?;
     *owners=returned;
     r1180_record(r1180_started,&R1180_SOURCE_NS,&R1180_SOURCE_MAX_NS,&R1180_SOURCE_COUNT);
     (seq,pending.len())
   },
   PaperHistoryR823::Legacy(_)=>return Err("private durable source required".into()),
 };
 let ns=u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
 // R1173 source+mirror wall time is NOT the R1127 DB1-only stage.
 if !compact{
  R1127_DB1_TX_NS.fetch_add(ns,Ordering::Relaxed);
  R1127_DB1_TX_COUNT.fetch_add(1,Ordering::Relaxed);
  R1127_DB1_TX_MAX_NS.fetch_max(ns,Ordering::Relaxed);
  r1150_db1_observe(ns);
 }
 if committed!=pending.len()||seq!=*tx_count+1{
   return Err("R1088 durable cursor/count mismatch".into())
 }
 *tx_count=seq;
 for (frame,_,_) in pending.drain(..){
   if compact{
     durable.push_back(Event::R1177DurableFrame(frame));
   }else{
     durable.push_back(Event::R1088DurableFrame(frame));
   }
 }
 Ok(())
}

#[derive(Debug)]
enum Event {
    Frame(RawFrame),
    NormalizedFrame(RawFrame,spotarb_native_market_core::Update),
    R1088DurableFrame(RawFrame),
    R1177DurableFrame(RawFrame),
    R1241BatchDurableFrame{batch_id:u64,batch_index:usize,batch_count:usize,frame:RawFrame,update:Update},
    Canonical(r1241_pipeline::Event),
    Ticker{venue:Venue,base:String,shard:usize,epoch:u64,sequence:u64,
           received_wall_ms:i64,raw:String},
    Reset{venue:Venue,bases:Vec<String>,epoch:u64},
    R1246TelemetryTick,
    TaskError{venue:&'static str,shard:usize,error:String},
    TaskDone{venue:&'static str,shard:usize,reconnects:u64,frames:u64},
}
// R1106: a pre-disconnect quote is NEVER proof of this venue's new WS epoch.
// Only genuinely processed new-epoch book frames can restore coverage.
fn r1106_clear_stale_coverage(seen:&mut HashSet<String>,bases:&[String])->usize{
    let mut evicted=0;
    for base in bases{
        if seen.remove(base){evicted+=1;}
    }
    evicted
}

fn cfg()->DecisionConfig {
    DecisionConfig{
        max_age_ms:800,metadata_ttl_ms:43_200_000,base_size:10.0,risk_cap:10.0,
        cost:24.03,required_net:42.0,min_price:0.0,max_price:f64::INFINITY,
        blacklist:HashSet::new(),
    }
}

/// Report only the applied, non-secret decision policy for subsequent cutover proof.
fn paper_config_evidence(cfg:&DecisionConfig,source:&str,yaml_sha:Option<&str>)->Value{
    let mut blacklist=cfg.blacklist.iter().cloned().collect::<Vec<_>>();
    blacklist.sort();
    let canonical=json!({
        "max_age_ms":cfg.max_age_ms,"metadata_ttl_ms":cfg.metadata_ttl_ms,
        "base_size":cfg.base_size,"risk_cap":cfg.risk_cap,
        "cost":cfg.cost,"required_net":cfg.required_net,
        "min_price":cfg.min_price,
        "max_price":if cfg.max_price.is_infinite(){"UNBOUNDED".to_owned()}else{cfg.max_price.to_string()},
        "blacklist":blacklist,
    });
    let bytes=serde_json::to_vec(&canonical).expect("canonical policy serialize");
    let digest=openssl::sha::sha256(&bytes);
    let mut fingerprint=String::with_capacity(64);
    for b in digest{fingerprint.push_str(&format!("{b:02x}"))}
    let modeled=spotarb_native_market_core::native_paper_market_execution_review::
        PaperAssumptions::default();
    let parity=verify_public_paper_decision_model_parity(cfg,&modeled).is_ok();
    json!({"source":source,"decision_config_sha256":fingerprint,
        "source_yaml_sha256":yaml_sha,
        "effective_decision_config":canonical,
        "paper_model_source":"COMPILED_PAPER_ASSUMPTIONS_DEFAULT",
        "paper_model_assumptions":modeled,
        "decision_and_model_parity_at_start":parity,
        "paper_executable_sha256_independently_attested":false,
        "real_order_authority":false,
        "output_is_runtime_observation":true})
}

fn safety_input(kill:&Value,preflight:&Value)->SafetyInput {
    let kill_state=kill.get("state").and_then(Value::as_str).unwrap_or("UNKNOWN");
    let kill_schema_ok=kill.get("schema").and_then(Value::as_str).is_some_and(|x|x.starts_with("spotbot-real-kill-switch-"));
    let kill_operator_ok=kill.get("operator").and_then(Value::as_str).is_some_and(|x|!x.trim().is_empty());
    let drilled=kill.get("drill_result").and_then(Value::as_str)==Some("PASS");
    let preflight_present=!preflight.is_null();
    let preflight_allow=preflight.get("verdict").and_then(Value::as_str)==Some("ALLOW");
    let preflight_has_blockers=preflight.get("blocker_codes").and_then(Value::as_array).is_some_and(|x|!x.is_empty());
    let operator_approved=preflight.get("real_launch_authorized").and_then(Value::as_bool).unwrap_or(false);
    SafetyInput{
        trade_enabled:true,trial_enabled:true,operator_approved,max_trades_per_day:0,
        entry_cooldown_sec:10800,max_open_positions:1,deny_on_unknown:true,deny_on_untracked_assets:true,
        position_state_valid:true,active_position:false,paper_position:false,unknown_or_manual_state:false,
        preflight_present,preflight_path_ok:true,preflight_read_ok:true,preflight_size_ok:true,
        preflight_valid:preflight_present,preflight_max_age_ok:true,preflight_schema_ok:true,preflight_fresh:true,
        preflight_allow,preflight_has_blockers,exchange_polled:true,live_checks_complete:true,
        live_reconciliation_complete:true,outbox_unresolved:0,spot_inventory_policy_valid:true,
        inventory_policy_match:true,untracked_spot:false,untracked_futures:false,untracked_orders:false,
        untracked_exchange_assets:false,lifecycle_verified:true,bootstrap_policy_valid:true,bootstrap_valid:true,
        prior_incident_valid:true,cooldown_evidence_present:true,cooldown_state_present:true,
        cooldown_evidence_valid:true,cooldown_remaining_sec:0,auto_preflight_refresh_failed:false,
        kill_path_ok:true,kill_read_ok:true,kill_size_ok:true,kill_payload_ok:true,kill_switch_present:!kill.is_null(),
        kill_switch_schema_ok:kill_schema_ok,kill_switch_operator_ok:kill_operator_ok,
        kill_switch_state:kill_state.into(),kill_drill_age_ok:true,kill_drilled:drilled,
        kill_switch_drill_fresh:true,kill_max_age_ok:true,kill_switch_disengagement_fresh:kill_state=="DISENGAGED",
    }
}

fn read_json(path:&str)->Value {
    fs::read(path).ok().and_then(|b|serde_json::from_slice(&b).ok()).unwrap_or(Value::Null)
}

/// Read a bounded, per-session, PUBLIC PAPER-only stop request. Never
/// interpret it as an exchange order or as authorization for real trading.
fn graceful_stop_requested(path:&std::path::Path,session:&str)
 ->Result<bool,Box<dyn std::error::Error>>{
    let meta=match fs::symlink_metadata(path){
       Ok(x)=>x,
       Err(e) if e.kind()==std::io::ErrorKind::NotFound=>return Ok(false),
       Err(e)=>return Err(e.into()),
    };
    if !meta.file_type().is_file()||meta.len()==0||meta.len()>4096{
        return Err("invalid bounded PAPER graceful-stop request".into())
    }
    let request:Value=serde_json::from_slice(&fs::read(path)?)?;
    if request["schema"]!="SPOTARB/PAPER/GRACEFUL-STOP/V1"||
       request["session"].as_str()!=Some(session)||
       request["scope"]!="public-paper-only"||
       request["real_trading"]!=false{
        return Err("wrong PAPER graceful-stop session or safety scope".into())
    }
    Ok(true)
}

/// R154: bounded public ticker demultiplexing from the KNOWN book socket
/// task only. Logical URL is reviewed downstream and never attests TLS peer.
fn r154_binance_ticker(raw:&str,bases:&[String])->Option<(String,String)>{
    if raw.len()>32_768{return None}
    let v:Value=serde_json::from_str(raw).ok()?;
    let stream=v.get("stream")?.as_str()?;
    let symbol=stream.strip_suffix("@ticker")?.to_ascii_uppercase();
    let base=symbol.strip_suffix("USDT")?.to_string();
    if !bases.contains(&base)||v["data"]["s"].as_str()!=Some(symbol.as_str())||
        v["data"]["e"]!="24hrTicker"||v["data"]["st"]!=1{return None}
    Some((base,v["data"].to_string()))
}
fn r154_okx_ticker(raw:&str,bases:&[String])->Option<(String,String)>{
    if raw.len()>32_768{return None}
    let v:Value=serde_json::from_str(raw).ok()?;
    if v["arg"]["channel"]!="tickers"{return None}
    let instrument=v["arg"]["instId"].as_str()?;
    let base=instrument.strip_suffix("-USDT")?.to_owned();
    if !bases.contains(&base)||v["data"].as_array()?.len()!=1||
        v["data"][0]["instId"].as_str()!=Some(instrument)||
        v["data"][0]["instType"]!="SPOT"{return None}
    Some((base,raw.to_owned()))
}
/// R159: both opt-in and default PUBLIC depth-only candidate sessions
/// use the supported high-frequency endpoint. This is NOT installed PAPER.
fn r156_binance_combined_url(streams:&[String],_ticker_review:bool)->String{
    format!("wss://fstream.binance.com/public/stream?streams={}",
        streams.join("/"))
}
// R1124: genuine PUBLIC depth20 snapshot stream, never frame sampling.
// PAPER-only operator opt-in changes the Binance source cadence;
// downstream source checks, 800ms book age and safety gates stay strict.
fn r1124_binance_depth_stream(base:&str,five_hundred_ms:bool)->String{
    format!("{}usdt@depth20@{}ms",base.to_ascii_lowercase(),
        if five_hundred_ms{500}else{100})
}
fn r158_binance_ticker_url(bases:&[String])->String{
    let streams=bases.iter().map(|b|format!("{}usdt@ticker",
        b.to_ascii_lowercase())).collect::<Vec<_>>();
    format!("wss://fstream.binance.com/market/stream?streams={}",
        streams.join("/"))
}
#[derive(Clone)]
struct R154TickerSample{shard:usize,epoch:u64,sequence:u64,wall:i64,source_ms:i64,raw:String}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
enum R154CacheAction{Accepted,NewEpoch,Duplicate,Stale,Rejected}
/// R172: inspect public ticker provenance before changing R154 diagnostic cache.
/// No entry authorization and no network activity happen in this helper.
fn r154_cache_ticker(cache:&mut HashMap<String,R154TickerSample>,base:&str,
 venue:Venue,shard:usize,epoch:u64,sequence:u64,wall:i64,
 raw:String,now:i64)->R154CacheAction{
 let url=match venue{
  Venue::OkxSpot=>"wss://ws.okx.com/ws/v5/public".into(),
  Venue::BinanceFutures=>format!("wss://fstream.binance.com/market/ws/{}usdt@ticker",
   base.to_ascii_lowercase()),
 };
 let parsed=match parse_ws_ticker(now,PublicWsEnvelope{
  venue,url:&url,base,connection_epoch:epoch,sequence,
  received_wall_ms:wall,body:&raw}){
   Ok(x)=>x,Err(_)=>return R154CacheAction::Rejected,
 };
 let mut action=R154CacheAction::Accepted;
 if let Some(old)=cache.get(base){
  if epoch<old.epoch||(epoch==old.epoch&&sequence<old.sequence)
    ||wall<old.wall||parsed.source_ms<old.source_ms{
   return R154CacheAction::Stale
  }
  if epoch==old.epoch&&sequence==old.sequence{
   return if wall==old.wall&&raw==old.raw{R154CacheAction::Duplicate}
    else{R154CacheAction::Rejected}
  }
  if epoch>old.epoch{action=R154CacheAction::NewEpoch}
 }
 cache.insert(base.to_owned(),R154TickerSample{
  shard,epoch,sequence,wall,source_ms:parsed.source_ms,raw});
 action
}
fn r154_restore_ticker(cache:&mut HashMap<String,R154TickerSample>,
 base:&str,previous:Option<R154TickerSample>){
 if let Some(old)=previous{cache.insert(base.to_owned(),old);}
 else{cache.remove(base);}
}
// R1109: Binance IP-protection policy. No immediate reconnect loops.
// Maximum TWO reconnects per source task, with strictly increasing cooldown.
// A failed/denied circuit is terminal; the public PAPER gate remains FAIL.
fn r1109_binance_cooldown(reconnects:u64)->Option<Duration>{
    match reconnects{
        1=>Some(Duration::from_secs(30)),
        2=>Some(Duration::from_secs(60)),
        _=>None
    }
}
// R1111 service-path reconciliation: installed service sandbox has
// ReadWritePaths=/mnt/spotarb_db_test/astra_paper_continuous_v1 ONLY.
// Do not use test-scratch quota path when building eventual installed PAPER.
const R1110_BINANCE_CONNECT_GUARD:&str=
    "/mnt/spotarb_db_test/astra_paper_continuous_v1/r1110_binance_ws_ip_guard.lock";
/// Protect all Binance PUBLIC WebSocket CONNECT attempts in this binary.
/// Other programs must use the SAME shared guard to benefit from host pacing.
/// A denied/corrupt guard fails closed and does NOT open a network connection.
async fn r1110_wait_public_ws_permit(end:Instant)->Result<(),String>{
    loop {
        if Instant::now()>=end{
            return Err("R1110 public Binance connection session deadline".into())
        }
        // R1143: capture wall time AFTER the shared gate's flock,
        // not in Tokio before worker scheduling and lock acquisition.
        let res=tokio::task::spawn_blocking(move||
            r1110_reserve_at_lock(std::path::Path::new(R1110_BINANCE_CONNECT_GUARD)))
            .await.map_err(|_|"R1110 guard worker unavailable")?
            .map_err(|e|format!("R1110 shared Binance quota fail-closed: {e:?}"))?;
        match res{
            R1110Outcome::Granted=>return Ok(()),
            R1110Outcome::WaitMs(ms)=>{
                let wait=Duration::from_millis(ms);
                if wait>=end.saturating_duration_since(Instant::now()){
                    return Err("R1110 shared Binance quota beyond session".into())
                }
                tokio::time::sleep(wait).await;
            }
        }
    }
}
include!("../native_r1142_binance_shard_diagnostics.rs");
include!("../native_r1144_ws_poll_diagnostics.rs");
include!("../native_r1157_ws_epoch_observe.rs");
include!("../native_r1158_ws_ping_probe.rs");
include!("../native_r1186_binance_stale_guard.rs");
// R1139: read-only diagnostics attached to existing Binance error notes.
// Never changes re-connect budgets, WS messages, source/ACK or trade gates.
fn r1139_binance_ws_error_suffix(
    connected:Instant,last_frame:Option<Instant>,last_ping:Option<Instant>,
    max_sender_block_ms:u64,connected_frames:u64
)->String{
    if std::env::var("ASTRA_R1139_WS_DIAGNOSTICS").ok().as_deref()!=Some("YES")
      ||std::env::var("ASTRA_R1119_FULL_RUST_PAPER").ok().as_deref()!=Some("YES")
      ||std::env::var("ASTRA_R1087_PAPER_PUBLIC_ONLY").ok().as_deref()!=Some("YES"){
        return String::new()
    }
    let idle=|last:Option<Instant>|last.map(|x|x.elapsed().as_millis() as u64);
    format!(";r1139={{connection_ms:{},last_frame_idle_ms:{:?},last_ping_idle_ms:{:?},max_sender_block_ms:{},connection_frames:{}}}",
        connected.elapsed().as_millis(),idle(last_frame),idle(last_ping),
        max_sender_block_ms,connected_frames)
}
// Opt-in bounded handoff; a timeout aborts the entire PAPER candidate.
async fn r1187_send_frame(tx:&mpsc::Sender<Event>,frame:RawFrame,
    fatal:&AtomicBool,enabled:bool,budget:Duration)->Result<Duration,&'static str>{
    let started=Instant::now();
    if enabled {
        match tokio::time::timeout(budget,tx.send(Event::Frame(frame))).await {
            Ok(Ok(()))=>Ok(started.elapsed()),
            Ok(Err(_))=>Err("binance_ingest_receiver_closed"),
            Err(_)=>{fatal.store(true,Ordering::SeqCst);
                Err("binance_ingest_send_timeout_fail_closed")}
        }
    }else{
        tx.send(Event::Frame(frame)).await
            .map_err(|_|"binance_ingest_receiver_closed")?;
        Ok(started.elapsed())
    }
}

async fn binance_task(
    shard:usize,bases:Vec<String>,duration:Duration,force_reconnect:bool,
    ticker_review:bool,tx:mpsc::Sender<Event>,counters:Arc<Counters>,
    r1187_fail_closed:bool,r1187_fatal:Arc<AtomicBool>
){
    // R1204: a private NEW per-run intake directory is mandatory.
    // Never resume a prior journal as live state, overwrite it, or rotate it here.
    let intake_path=std::env::var_os("ASTRA_R1204_INTAKE_ROOT")
        .map(std::path::PathBuf::from)
        .map(|p|p.join(format!("binance-shard-{shard}.intake")));
    let mut r1204_intake=match intake_path.ok_or_else(||"R1204 intake root missing".to_string())
        .and_then(|p|intake_r1204::Intake::create(&p,intake_r1204::SESSION_CAP)) {
        Ok(j)=>j,
        Err(e)=>{
            r1187_fatal.store(true,Ordering::SeqCst);
            let _=tx.send(Event::TaskError{venue:"binance",shard,error:e}).await;
            return
        }
    };
    let end=Instant::now()+duration;
    let r1124_paper_500ms=std::env::var("ASTRA_R1124_BINANCE_500MS")
        .ok().as_deref()==Some("YES")&&
        std::env::var("ASTRA_R1119_FULL_RUST_PAPER")
          .ok().as_deref()==Some("YES");
    let streams=bases.iter().map(|b|
        r1124_binance_depth_stream(b,r1124_paper_500ms)
    ).collect::<Vec<_>>();
    let url=r156_binance_combined_url(&streams,ticker_review);
    let mut epoch=1u64;let mut reconnects=0u64;let mut frames=0u64;let mut forced=false;
    // Strict bounded-session budget: at most ONE lag-driven reconnect per shard.
    let mut r1186_reconnect_used=false;
    loop {
        if let Err(e)=r1110_wait_public_ws_permit(end).await{
            let _=tx.send(Event::TaskError{venue:"binance",shard,error:e}).await;
            break
        }
        let ws_config=tokio_tungstenite::tungstenite::protocol::WebSocketConfig {
            max_message_size:Some(65_536),max_frame_size:Some(65_536),
            ..Default::default()
        };
        let (mut ws,_)=match tokio_tungstenite::connect_async_with_config(
            &url,Some(ws_config),false).await {
            Ok(v)=>v,
            Err(e)=>{let _=tx.send(Event::TaskError{venue:"binance",shard,error:format!("connect:{e}")}).await;break}
        };
        // R1145: map the pre-existing PUBLIC socket to Linux TCP/Recv-Q
        // passive observation. Never changes its buffering or connects.
        if std::env::var("ASTRA_R1145_TCP_OBSERVE").ok().as_deref()==Some("YES")
            &&std::env::var("ASTRA_R1144_WS_POLL_DIAGNOSTICS").ok().as_deref()==Some("YES")
            &&std::env::var("ASTRA_R1087_PAPER_PUBLIC_ONLY").ok().as_deref()==Some("YES"){
            use tokio_tungstenite::MaybeTlsStream;
            let addr=match ws.get_ref(){
                MaybeTlsStream::Plain(tcp)=>tcp.local_addr().ok(),
                MaybeTlsStream::NativeTls(tls)=>
                    tls.get_ref().get_ref().get_ref().local_addr().ok(),
                _=>None
            };
            if let Some(local)=addr{
                eprintln!("R1145_BINANCE_TCP_SOCKET {}",serde_json::json!({
                    "shard":shard,"epoch":epoch,"local_port":local.port(),
                    "connection":"existing public WS","real_orders":false
                }));
            }
        }
        // R1157: local port identifies the EXISTING socket for passive ss/TCP_INFO.
        // This gate does not connect, route, send or alter the WS stream.
        let r1157_local_port=if std::env::var("ASTRA_R1157_WS_EPOCH_OBSERVE")
            .ok().as_deref()==Some("YES"){
            use tokio_tungstenite::MaybeTlsStream;
            match ws.get_ref(){
                MaybeTlsStream::Plain(tcp)=>tcp.local_addr().ok().map(|a|a.port()),
                MaybeTlsStream::NativeTls(tls)=>
                    tls.get_ref().get_ref().get_ref().local_addr().ok().map(|a|a.port()),
                _=>None
            }
        }else{None};
        let r1157_connection_epoch=epoch;
        let mut r1157_probe=R1157EpochProbe::new(r1157_local_port);
        r1157_probe.report(shard,r1157_connection_epoch,"connected");
        let mut r1158_ping_probe=R1158WsPingProbe::new();
        // Only local monotonic observations, no outbound probes or API calls.
        let connected=Instant::now();
        let mut last_frame=None::<Instant>;
        let mut last_ping=None::<Instant>;
        let mut connected_frames=0u64;
        let mut max_sender_block_ms=0u64;
        let mut r1142_probe=R1142BinanceProbe::new();
        let mut r1144_probe=R1144WsPollProbe::new();
        let mut r1186_guard=R1186BinanceStaleGuard::new(
          std::env::var("ASTRA_R1186_BINANCE_STALE_RECOVERY").ok().as_deref()==Some("YES")&&
          std::env::var("ASTRA_R1087_PAPER_PUBLIC_ONLY").ok().as_deref()==Some("YES")&&
          std::env::var("ASTRA_R1136_RELEASE_PAPER").ok().as_deref()==Some("YES"));

        // R1106: Reset was sent on DISCONNECT, never delayed to
        // this successful reconnection (a stale book must not survive outage).
        loop {
            if Instant::now()>=end {
                r1142_probe.report(shard,"normal_session_end");
                r1144_probe.report(shard,"normal_session_end");
                r1157_probe.report(shard,r1157_connection_epoch,"normal_session_end");
                if r1158_ping_probe.pending.is_some(){
                    r1158_ping_probe.log(shard,r1157_connection_epoch,r1157_local_port,
                        "session_end_pending_pong",None);
                }
                let _=ws.close(None).await;
                let _=tx.send(Event::TaskDone{venue:"binance",shard,reconnects,frames}).await;
                return
            }
            if force_reconnect&&!forced&&Instant::now()+Duration::from_millis(50)>=end-duration/2 {
                forced=true;reconnects+=1;epoch+=1;
                if tx.send(Event::Reset{venue:Venue::BinanceFutures,
                    bases:bases.clone(),epoch}).await.is_err(){return}
                let _=ws.close(None).await;break;
            }
            // R1158: optional one outstanding client Ping every >=30s, diagnostic only.
            // A missing Pong is logged; it never initiates an early reconnect.
            r1158_ping_probe.check_overdue(shard,r1157_connection_epoch,r1157_local_port);
            if r1158_ping_probe.due(){
                let payload=r1158_ping_probe.make_payload(shard,r1157_connection_epoch);
                let started=Instant::now();
                let result=tokio::time::timeout(Duration::from_secs(2),
                    ws.send(Message::Ping(payload.clone()))).await;
                let elapsed=started.elapsed().as_millis().min(u64::MAX as u128) as u64;
                if matches!(result,Ok(Ok(()))){
                    r1158_ping_probe.sent(payload,shard,r1157_connection_epoch,r1157_local_port,elapsed);
                }else{
                    r1158_ping_probe.send_error(shard,r1157_connection_epoch,r1157_local_port,elapsed);
                    reconnects+=1;epoch+=1;
                    if tx.send(Event::Reset{venue:Venue::BinanceFutures,
                        bases:bases.clone(),epoch}).await.is_err(){return}
                    let _=tx.send(Event::TaskError{venue:"binance",shard,
                        error:"R1158 diagnostic public Ping send/timeout failure".into()}).await;
                    break
                }
            }
            let left=end.saturating_duration_since(Instant::now()).min(Duration::from_secs(2));
            r1157_probe.next_poll_started();
            let r1144_poll_started=Instant::now();
            let r1157_result=tokio::time::timeout(left,ws.next()).await;
            r1157_probe.poll_return(r1144_poll_started.elapsed(),r1157_result.is_err());
            r1157_probe.maybe_report(shard,r1157_connection_epoch);
            match r1157_result {
                Ok(Some(Ok(Message::Text(raw))))=>{
                    let r1204_received_ns=monotonic_ns();
                    let r1204_received_wall=wall_ms();
                    if let Err(e)=r1204_intake.accept(
                        &format!("binance:shard:{shard}"),"USD-M",epoch,
                        r1204_received_ns,r1204_received_wall,&raw) {
                        r1187_fatal.store(true,Ordering::SeqCst);
                        eprintln!("R1204_INTAKE_GAP {}",json!({
                            "shard":shard,"epoch":epoch,"reason":e,
                            "accepted":r1204_intake.accepted(),
                            "raw_sha256":openssl::sha::sha256(raw.as_bytes()).iter()
                                .map(|b|format!("{b:02x}")).collect::<String>(),
                            "no_ack_claim":true,"real_orders":false}));
                        return
                    }

                    if ticker_review&&raw.contains("@ticker"){
                        // Strictly forbid 24h ticker data on a depth-only task.
                        let _=tx.send(Event::TaskError{venue:"binance",shard,
                            error:"market ticker on public depth socket".into()}).await;
                        return
                    }
                    frames+=1;connected_frames+=1;
                    counters.bin_frames.fetch_add(1,Ordering::Relaxed);
                    last_frame=Some(Instant::now());
                    let received_wall=r1204_received_wall;
                    r1157_probe.text_read(&raw,received_wall);
                    r1144_probe.text_read(&raw,received_wall,
                       r1144_poll_started.elapsed().as_millis().min(u64::MAX as u128) as u64);
                    // R1186: act only on sustained multi-symbol stale E;
                    // never weaken DecisionCore 800ms freshness or R1109.
                    // Never misdiagnose a saturated LOCAL ingest queue
                    // as an upstream Binance stall. Require >=95% free.
                    let local_queue_healthy=tx.capacity()*100>=
                        R1125_QUEUE_CAP*95;
                    if !r1186_reconnect_used&&r1186_guard.observe(
                        &raw,received_wall,
                        connected.elapsed().as_millis().min(u64::MAX as u128) as u64,
                        local_queue_healthy){
                        r1186_reconnect_used=true;
                        r1157_probe.report(shard,r1157_connection_epoch,
                            "sustained_stale_source_guard");
                        eprintln!("R1186_BINANCE_GUARD {}",serde_json::json!({
                            "shard":shard,"epoch":epoch,"reason":"sustained_multi_symbol_source_e_lag",
                            "reconnect_attempt_budget":1,"real_orders":false
                        }));
                        reconnects+=1;epoch+=1;
                        if tx.send(Event::Reset{venue:Venue::BinanceFutures,
                            bases:bases.clone(),epoch}).await.is_err(){return}
                        let _=ws.close(None).await;break;
                    }
                    r1142_probe.received(&raw,received_wall);
                    let f=RawFrame{source:None,venue:Venue::BinanceFutures,epoch,received_ns:r1204_received_ns,received_wall_ms:received_wall,raw};
                    let raw_sha=if r1187_fail_closed {Some(openssl::sha::sha256(f.raw.as_bytes()))}else{None};
                    let full_at_start=tx.capacity()==0;
                    let started=Instant::now();
                    let delivered=r1187_send_frame(&tx,f,&r1187_fatal,r1187_fail_closed,
                        Duration::from_millis(250)).await;
                    let send_elapsed=started.elapsed();
                    r1157_probe.sender_sent(send_elapsed,full_at_start);
                    r1142_probe.delivered(send_elapsed);
                    max_sender_block_ms=max_sender_block_ms.max(
                        send_elapsed.as_millis().min(u64::MAX as u128) as u64);
                    if let Err(reason)=delivered{
                        if r1187_fail_closed {
                            eprintln!("R1187_BINANCE_INGEST_FAIL_CLOSED {}",json!({
                                "shard":shard,"epoch":epoch,"reason":reason,
                                "source_received_wall_ms":received_wall,
                                "raw_sha256":raw_sha.map(|h|h.iter().map(|b|format!("{b:02x}")).collect::<String>()),
                                "sender_block_ms":send_elapsed.as_millis(),
                                "mpsc_capacity":tx.capacity(),"no_ack_claim":true,
                                "real_orders":false
                            }));
                        }
                        return
                    }
                }
                // R1123: tungstenite 0.21 automatically queues a Pong.
                // Flush it promptly instead of replacing it with manual Pong.
                Ok(Some(Ok(Message::Ping(_))))=>{
                    last_ping=Some(Instant::now());
                    let flush_started=Instant::now();
                    let flush_result=ws.flush().await;
                    r1157_probe.pong_flush(flush_started.elapsed(),flush_result.is_ok());
                    if let Err(e)=flush_result{
                        reconnects+=1;epoch+=1;
                        if tx.send(Event::Reset{venue:Venue::BinanceFutures,
                            bases:bases.clone(),epoch}).await.is_err(){return}
                        let _=tx.send(Event::TaskError{venue:"binance",shard,
                            error:format!("ping_auto_pong_flush:{e}{}",r1139_binance_ws_error_suffix(
                                connected,last_frame,last_ping,max_sender_block_ms,connected_frames))}).await;
                        break
                    }
                }
                Ok(Some(Ok(Message::Pong(payload))))=>{
                    r1158_ping_probe.pong(&payload,shard,r1157_connection_epoch,r1157_local_port);
                }
                Ok(Some(Ok(_)))=>{}
                Ok(Some(Err(e)))=>{
                    reconnects+=1;epoch+=1;
                    if tx.send(Event::Reset{venue:Venue::BinanceFutures,bases:bases.clone(),epoch}).await.is_err(){return}
                    let _=tx.send(Event::TaskError{venue:"binance",shard,
                        error:format!("ws:{e}{}",r1139_binance_ws_error_suffix(
                            connected,last_frame,last_ping,max_sender_block_ms,connected_frames))}).await;
                    break
                }
                Ok(None)=>{
                    reconnects+=1;epoch+=1;
                    if tx.send(Event::Reset{venue:Venue::BinanceFutures,bases:bases.clone(),epoch}).await.is_err(){return}
                    break
                }
                Err(_)=>{}
            }
        }
        r1142_probe.report(shard,"connection_loop_ended");
        r1144_probe.report(shard,"connection_loop_ended");
        r1157_probe.report(shard,r1157_connection_epoch,"connection_loop_ended");
        if r1158_ping_probe.pending.is_some(){
            r1158_ping_probe.log(shard,r1157_connection_epoch,r1157_local_port,
                "connection_ended_pending_pong",None);
        }
        if Instant::now()>=end {break}
        // Binance WS safety: a protocol reset NEVER creates a hot retry loop.
        // Do not reconnect if the bounded session cannot accommodate cooldown.
        let Some(delay)=r1109_binance_cooldown(reconnects) else{
            let _=tx.send(Event::TaskError{venue:"binance",shard,
                error:"R1109 IP protection: retry budget exhausted".into()}).await;
            break
        };
        if delay>=end.saturating_duration_since(Instant::now()){
            let _=tx.send(Event::TaskError{venue:"binance",shard,
                error:"R1109 IP protection: cooling down beyond session".into()}).await;
            break
        }
        tokio::time::sleep(delay).await;
    }
    let _=tx.send(Event::TaskDone{venue:"binance",shard,reconnects,frames}).await;
}

/// R158: public 24-hour turnover stream has a DIFFERENT Binance
/// endpoint from the public depth books. No private API or order channel.
async fn binance_ticker_task(
    shard:usize,bases:Vec<String>,duration:Duration,force_reconnect:bool,
    tx:mpsc::Sender<Event>
){
    let end=Instant::now()+duration;
    let url=r158_binance_ticker_url(&bases);
    let mut epoch=1u64;let mut reconnects=0u64;let mut frames=0u64;let mut forced=false;
    loop {
        if let Err(e)=r1110_wait_public_ws_permit(end).await{
            let _=tx.send(Event::TaskError{venue:"binance-ticker",shard,error:e}).await;
            break
        }
        let (mut ws,_)=match connect_async(&url).await{
            Ok(v)=>v,
            Err(e)=>{
                let _=tx.send(Event::TaskError{venue:"binance-ticker",shard,
                    error:format!("connect:{e}")}).await;break
            }
        };
        if epoch>1{
            let _=tx.send(Event::Reset{venue:Venue::BinanceFutures,
                bases:bases.clone(),epoch}).await;
        }
        let mut sequences=HashMap::<String,u64>::new();
        loop{
            if Instant::now()>=end{
                let _=ws.close(None).await;
                let _=tx.send(Event::TaskDone{venue:"binance-ticker",
                    shard,reconnects,frames}).await;
                return
            }
            if force_reconnect&&!forced&&
                Instant::now()+Duration::from_millis(50)>=end-duration/2{
                forced=true;reconnects+=1;epoch+=1;
                let _=ws.close(None).await;break
            }
            let left=end.saturating_duration_since(Instant::now())
                .min(Duration::from_secs(2));
            match tokio::time::timeout(left,ws.next()).await{
                Ok(Some(Ok(Message::Text(raw))))=>{
                    let Some((base,body))=r154_binance_ticker(&raw,&bases) else{
                        let _=tx.send(Event::TaskError{
                            venue:"binance-ticker",shard,
                            error:"invalid or unexpected ticker envelope".into(),
                        }).await;
                        return
                    };
                    let seq=sequences.entry(base.clone()).or_default();
                    *seq=seq.saturating_add(1);
                    frames+=1;
                    if tx.send(Event::Ticker{
                        venue:Venue::BinanceFutures,base,shard,epoch,
                        sequence:*seq,received_wall_ms:wall_ms(),raw:body,
                    }).await.is_err(){return}
                }
                Ok(Some(Ok(Message::Ping(data))))=>{
                    let _=ws.send(Message::Pong(data)).await;
                }
                Ok(Some(Ok(_)))=>{},
                Ok(Some(Err(e)))=>{
                    reconnects+=1;epoch+=1;
                    let _=tx.send(Event::TaskError{
                        venue:"binance-ticker",shard,error:format!("ws:{e}"),
                    }).await;break
                }
                Ok(None)=>{reconnects+=1;epoch+=1;break},
                Err(_)=>{},
            }
        }
        if Instant::now()>=end{break}
        // Same IP budget applies if optional public ticker review is enabled.
        let Some(delay)=r1109_binance_cooldown(reconnects) else{
            let _=tx.send(Event::TaskError{venue:"binance-ticker",shard,
                error:"R1109 IP protection: retry budget exhausted".into()}).await;
            break
        };
        if delay>=end.saturating_duration_since(Instant::now()){
            let _=tx.send(Event::TaskError{venue:"binance-ticker",shard,
                error:"R1109 IP protection: cooling down beyond session".into()}).await;
            break
        }
        tokio::time::sleep(delay).await;
    }
    let _=tx.send(Event::TaskDone{venue:"binance-ticker",
        shard,reconnects,frames}).await;
}
async fn okx_task(
    shard:usize,bases:Vec<String>,duration:Duration,force_reconnect:bool,
    ticker_review:bool,tx:mpsc::Sender<Event>,counters:Arc<Counters>
){
    let end=Instant::now()+duration;
    let mut epoch=1u64;let mut reconnects=0u64;let mut frames=0u64;let mut forced=false;
    loop {
        // R159: default and opt-in source review use future-safe port 443.
        let okx_url="wss://ws.okx.com/ws/v5/public";
        let (mut ws,_)=match connect_async(okx_url).await {
            Ok(v)=>v,
            Err(e)=>{let _=tx.send(Event::TaskError{venue:"okx",shard,error:format!("connect:{e}")}).await;break}
        };
        let mut args=bases.iter().map(|b|json!({"channel":"books5",
            "instId":format!("{b}-USDT")})).collect::<Vec<_>>();
        if ticker_review {
            args.extend(bases.iter().map(|b|json!({"channel":"tickers",
                "instId":format!("{b}-USDT")})));
        }
        if let Err(e)=ws.send(Message::Text(json!({"op":"subscribe","args":args}).to_string())).await {
            let _=tx.send(Event::TaskError{venue:"okx",shard,error:format!("subscribe:{e}")}).await;break
        }
        // R1106: outage reset already emitted before reconnect, never
        // allow pre-disconnect spot books to masquerade as a new epoch.
        let mut ticker_sequences=HashMap::<String,u64>::new();
        loop {
            if Instant::now()>=end {let _=ws.close(None).await;let _=tx.send(Event::TaskDone{venue:"okx",shard,reconnects,frames}).await;return}
            if force_reconnect&&!forced&&Instant::now()+Duration::from_millis(50)>=end-duration/2 {
                forced=true;reconnects+=1;epoch+=1;
                if tx.send(Event::Reset{venue:Venue::OkxSpot,bases:bases.clone(),epoch}).await.is_err(){return}
                let _=ws.close(None).await;break;
            }
            let left=end.saturating_duration_since(Instant::now()).min(Duration::from_secs(2));
            match tokio::time::timeout(left,ws.next()).await {
                Ok(Some(Ok(Message::Text(raw))))=>{
                    let parsed:Value=serde_json::from_str(&raw).unwrap_or(Value::Null);
                    if parsed.get("event").is_some(){continue}
                    if ticker_review&&parsed["arg"]["channel"]=="tickers" {
                        if let Some((base,body))=r154_okx_ticker(&raw,&bases){
                            let seq=ticker_sequences.entry(base.clone()).or_default();
                            *seq=seq.saturating_add(1);
                            if tx.send(Event::Ticker{venue:Venue::OkxSpot,
                                base,shard,epoch,sequence:*seq,received_wall_ms:wall_ms(),raw:body})
                                .await.is_err(){return}
                        }else{
                            let _=tx.send(Event::TaskError{venue:"okx",shard,
                                error:"invalid subscribed ticker envelope".into()}).await;
                        }
                        continue
                    }
                    frames+=1;counters.okx_frames.fetch_add(1,Ordering::Relaxed);
                    let f=RawFrame{source:None,venue:Venue::OkxSpot,epoch,received_ns:monotonic_ns(),received_wall_ms:wall_ms(),raw};
                    if tx.send(Event::Frame(f)).await.is_err(){return}
                }
                Ok(Some(Ok(Message::Ping(p))))=>{let _=ws.send(Message::Pong(p)).await;}
                Ok(Some(Ok(_)))=>{}
                Ok(Some(Err(e)))=>{
                    reconnects+=1;epoch+=1;
                    if tx.send(Event::Reset{venue:Venue::OkxSpot,bases:bases.clone(),epoch}).await.is_err(){return}
                    let _=tx.send(Event::TaskError{venue:"okx",shard,error:format!("ws:{e}")}).await;
                    break
                }
                Ok(None)=>{
                    reconnects+=1;epoch+=1;
                    if tx.send(Event::Reset{venue:Venue::OkxSpot,bases:bases.clone(),epoch}).await.is_err(){return}
                    break
                }
                Err(_)=>{}
            }
        }
        if Instant::now()>=end {break}
    }
    let _=tx.send(Event::TaskDone{venue:"okx",shard,reconnects,frames}).await;
}

#[derive(Default)]
struct Counters{bin_frames:AtomicU64,okx_frames:AtomicU64}

fn decision_is_opportunity(d:&Decision)->bool { d.reason=="EXECUTABLE_OPPORTUNITY" }
fn r1246_archive_candidate(d:&Decision)->bool {
    decision_is_opportunity(d)||d.gross_upper_bound_bps
        .filter(|gross|gross.is_finite())
        .is_some_and(|gross|gross-d.cost_bps>=d.required_net_bps)
}
fn r1246_latency_summary(samples:&VecDeque<u64>)->Value{
    if samples.is_empty(){return json!({"count":0,"p50_ns":null,"p95_ns":null,"p99_ns":null,"max_ns":null})}
    let mut sorted=samples.iter().copied().collect::<Vec<_>>();
    sorted.sort_unstable();
    let at=|q:f64|->u64{let i=((sorted.len()-1) as f64*q).ceil() as usize;sorted[i]};
    json!({"count":sorted.len(),"p50_ns":at(0.50),"p95_ns":at(0.95),
           "p99_ns":at(0.99),"max_ns":sorted[sorted.len()-1]})
}
fn open_r1246_archive()->Result<Option<DualArchive>,String>{
    if std::env::var("R1246_DUAL_ARCHIVE").ok().as_deref()!=Some("YES"){
        return Ok(None)
    }
    let sdb=PathBuf::from(std::env::var("R1246_SDB_ARCHIVE_DIR")
        .map_err(|_|"R1246 sdb archive directory is required")?);
    let sdc=PathBuf::from(std::env::var("R1246_SDC_ARCHIVE_DIR")
        .map_err(|_|"R1246 sdc archive directory is required")?);
    let sdb_root=PathBuf::from("/mnt/spotarb_db_test/r1246_runs");
    let sdc_root=PathBuf::from("/mnt/spotarb_disk2/r1246_runs");
    if sdb==sdc||!sdb.starts_with(&sdb_root)||sdb==sdb_root||
        !sdc.starts_with(&sdc_root)||sdc==sdc_root{
        return Err("R1246 archive path is outside its isolated disk root".into())
    }
    let session=std::env::var("R1246_SESSION_ID")
        .map_err(|_|"R1246 session identity is required")?;
    let run=PathBuf::from("/mnt/spotarb_db_test/r1246_runs").join(&session);
    let expected_sdb=run.join("archive_sdb");
    let expected_sdc=PathBuf::from("/mnt/spotarb_disk2/r1246_runs")
        .join(&session).join("archive_sdc");
    if sdb!=expected_sdb||sdc!=expected_sdc{
        return Err("R1246 archive path/session mismatch".into())
    }
    fs::create_dir_all(&sdb).map_err(|e|format!("R1246 create sdb dir: {e}"))?;
    fs::create_dir_all(&sdc).map_err(|e|format!("R1246 create sdc dir: {e}"))?;
    let canonical_sdb=fs::canonicalize(&sdb).map_err(|e|format!("R1246 canonical sdb dir: {e}"))?;
    let canonical_sdc=fs::canonicalize(&sdc).map_err(|e|format!("R1246 canonical sdc dir: {e}"))?;
    let canonical_sdb_root=fs::canonicalize("/mnt/spotarb_db_test/r1246_runs")
        .map_err(|e|format!("R1246 canonical sdb root: {e}"))?;
    let canonical_sdc_root=fs::canonicalize("/mnt/spotarb_disk2/r1246_runs")
        .map_err(|e|format!("R1246 canonical sdc root: {e}"))?;
    if !canonical_sdb.starts_with(canonical_sdb_root)||
       !canonical_sdc.starts_with(canonical_sdc_root){
        return Err("R1246 archive symlink escaped isolated run roots".into())
    }
    DualArchive::open(canonical_sdb,canonical_sdc,session).map(Some)
}

/// A fixed, pre-existing, ordinary directory is required. A syntactically
/// scoped path must not use ../ or symlinked parent directories.
fn r155_checkpoint_path(raw:&str)->Result<PathBuf,&'static str>{
    const ROOT:&str="/mnt/spotarb_db_test/astra_p1_06_config_audit_20261018/r155_review_checkpoints";
    let path=PathBuf::from(raw);
    let filename=path.file_name().and_then(|f|f.to_str())
        .ok_or("R155 checkpoint filename missing")?;
    if path.parent()!=Some(std::path::Path::new(ROOT))||
        !filename.ends_with(".json")||
        filename.len()>80||filename.len()<6||
        !filename.bytes().all(|x|x.is_ascii_alphanumeric()||x==b'.'||x==b'_'||x==b'-')||
        filename.starts_with('.'){
        return Err("R155 checkpoint path outside fixed research directory")
    }
    Ok(path)
}
/// R155 separate research-only branch: one source-owned virtual portfolio.
/// It deliberately DOES NOT construct DecisionCore, PaperPortfolio or a
/// parallel virtual owner. No private exchange or installed PAPER calls.
/// On any interrupted episode its R153 snapshot remains UNSEALED.
async fn r155_source_paper_run(
    secs:u64,force_reconnect:bool,meta:NativeMetadataSnapshot,
    bases:Vec<String>,kill_path:&str,output:Option<PathBuf>
)->Result<(),Box<dyn std::error::Error>>{
    let yaml_path=std::env::var("R155_SOURCE_PAPER_YAML_PATH")
        .map_err(|_|"R155 requires an explicit source YAML path")?;
    let checkpoint=r155_checkpoint_path(
        &std::env::var("R155_REVIEW_CHECKPOINT_PATH")
          .map_err(|_|"R155 requires an isolated source checkpoint path")?)?;
    let parent=checkpoint.parent().ok_or("R155 parent missing")?;
    let m=fs::symlink_metadata(parent)
        .map_err(|_|"R155 checkpoint directory must already exist")?;
    if !m.file_type().is_dir()||fs::canonicalize(parent)?!=parent{
        return Err("R155 checkpoint directory contains a symlink".into())
    }
    if std::env::var("PAPER_NATIVE_RUST_CONFIG_REVIEW").is_ok()
      ||std::env::var("PAPER_PLANNED_RESTORE").is_ok()
      ||std::env::var("PAPER_CONTINUITY_PATH").is_ok()
      ||std::env::var("PAPER_VIRTUAL_FOUR_LEG_JOURNAL_PATH").is_ok(){
        return Err("R155 and legacy PAPER configuration/recovery must not mix".into())
    }
    if read_json(kill_path)["state"]!="ENGAGED"{
        return Err("R155 source review requires ENGAGED kill switch".into())
    }
    let yaml_path=PathBuf::from(yaml_path);
    let source=NativeTradingConfig::load(&yaml_path)
        .map_err(|e|format!("R155 source YAML unavailable: {e:?}"))?;
    // R178: a source YAML replacement after parse cannot be silently
    // attributed to this candidate's later checkpoints or result.
    let source_identity=CandidateSourceIdentityR178::admit(&yaml_path,&source)
        .map_err(|e|format!("R178 source admission denied: {e:?}"))?;
    // R167: the explicitly supplied R155 YAML owns the PUBLIC WS fleet
    // width and start staggering. Never change legacy compiled fallback.
    let plan=SourceWsShardPlanR167::from_source(&source,&bases)
        .map_err(|e|format!("R167 source fleet rejected: {e}"))?;
    let strategy=SupervisedStrategyPaper::new(source,wall_ms())
        .map_err(|e|format!("R155 unsupported YAML refused: {e:?}"))?;
    let mut lane=SourcePaperLaneR155::new(strategy,plan.owner_shards())
        .map_err(|e|format!("R155 source lane denied: {e:?}"))?;
    let report=lane.strategy.effective_research_config_report()
        .map_err(|e|format!("R155 config proof denied: {e:?}"))?;
    if report["source_sha256"].as_str()!=Some(plan.config_sha256.as_str()){
        return Err("R167 source SHA changed during candidate admission".into())
    }
    let applied_ws_groups=plan.groups.len();
    let applied_ws_group_size=plan.group_size;
    let applied_ws_stagger_ms=plan.start_interval_ms;
    let max_age_ms=report["decision_max_book_age_ms"].as_u64()
        .ok_or("R155 source decision age absent")?;
    let mut owners=(0..OWNERS).map(|i|Owner::new(i,OWNERS,max_age_ms).unwrap())
        .collect::<Vec<_>>();
    let mut history=NoHistory{accepted:0};
    let mut revision=1_u64;
    source_identity.before_checkpoint(&lane.strategy)
        .map_err(|e|format!("R178 initial source drift denied: {e:?}"))?;
    save_source_paper_review(&checkpoint,&lane.strategy,revision,monotonic_ns(),false)
        .map_err(|e|format!("R155 initial fsync denied: {e:?}"))?;
    let (tx,mut rx)=mpsc::channel::<Event>(16_384);
    let counters=Arc::new(Counters::default());
    let duration=Duration::from_secs(secs);
    let tasks=plan.task_count()
        .map_err(|e|format!("R167 source task count denied: {e}"))?;
    // Do not block the receiver while pacing connects: delays are inside
    // each PUBLIC WS task. A 1-second Python-style fleet stagger therefore
    // cannot accumulate frames while the consumer is asleep.
    for (shard,bs) in plan.groups.iter().cloned().enumerate(){
        let pause=plan.delay_before(shard)
            .map_err(|e|format!("R167 stagger rejected: {e}"))?;
        let bin=bs.clone();let ticker=bs.clone();
        let bin_tx=tx.clone();let bin_counters=counters.clone();
        tokio::spawn(async move {
            tokio::time::sleep(pause).await;
            binance_task(shard,bin,duration,force_reconnect,
                true,bin_tx,bin_counters,false,Arc::new(AtomicBool::new(false))).await;
        });
        let ticker_tx=tx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(pause).await;
            binance_ticker_task(shard,ticker,duration,
                force_reconnect,ticker_tx).await;
        });
        let okx_tx=tx.clone();let okx_counters=counters.clone();
        tokio::spawn(async move {
            tokio::time::sleep(pause).await;
            okx_task(shard,bs,duration,force_reconnect,
                true,okx_tx,okx_counters).await;
        });
    }
    drop(tx);
    let mut done=0_usize;let mut books=0_u64;let mut source_denies=0_u64;
    let mut last_safety=Instant::now();
    while let Some(event)=rx.recv().await{
        if last_safety.elapsed()>Duration::from_secs(10){
            if read_json(kill_path)["state"]!="ENGAGED"{
                return Err("R155 review kill switch is not ENGAGED".into())
            }
            // R178: also reject on-disk operator YAML drift even when no
            // modeled position changed and no lifecycle save is pending.
            source_identity.before_checkpoint(&lane.strategy)
                .map_err(|e|format!("R178 periodic source drift denied: {e:?}"))?;
            last_safety=Instant::now();
        }
        let (event,normalized_update)=match event{Event::NormalizedFrame(f,u)=>(Event::Frame(f),Some(u)),other=>(other,None)};
        match event{
            Event::NormalizedFrame(_,_)=>unreachable!("normalized above"),
            Event::Frame(frame)=>{
                let u=match normalized_update{Some(u)=>u,None=>spotarb_native_market_core::parse(&frame).map_err(|e|format!("R155 malformed book frame: {e:?}"))?};
                let base=u.base.clone();let idx=owner_index(&base,OWNERS);
                owners[idx].ingest_parsed(&frame,u,&mut history)
                    .map_err(|e|format!("R155 source book ingest denied: {e:?}"))?;
                books+=1;
                let (spot,fut)=owners[idx].snapshot_pair(&base);
                let route=meta.routes.get(&base).and_then(|rows|rows.first());
                let before=(
                    lane.strategy.portfolio.stats.entries,
                    lane.strategy.portfolio.stats.futures_hedges,
                    lane.strategy.portfolio.stats.futures_covers,
                    lane.strategy.portfolio.stats.closed,
                    lane.strategy.portfolio.positions.len());
                match lane.on_book_frame(&base,frame.venue,frame.received_ns,
                   frame.received_wall_ms,spot.as_ref(),fut.as_ref(),route){
                    Ok(_)=>{},
                    Err(spotarb_native_market_core::native_paper_strategy_frame_review::
                        StrategyFrameError::InvalidFrame)=>{source_denies+=1;},
                    Err(e)=>return Err(format!("R155 source strategy refused: {e:?}").into()),
                }
                let after=(
                    lane.strategy.portfolio.stats.entries,
                    lane.strategy.portfolio.stats.futures_hedges,
                    lane.strategy.portfolio.stats.futures_covers,
                    lane.strategy.portfolio.stats.closed,
                    lane.strategy.portfolio.positions.len());
                if after!=before{
                    revision=revision.checked_add(1)
                        .ok_or("R155 checkpoint revision overflow")?;
                    source_identity.before_checkpoint(&lane.strategy)
                        .map_err(|e|format!("R178 lifecycle source drift denied: {e:?}"))?;
                    save_source_paper_review(&checkpoint,&lane.strategy,
                        revision,monotonic_ns(),false)
                        .map_err(|e|format!("R155 lifecycle fsync denied: {e:?}"))?;
                }
            }
            Event::Ticker{venue,base,shard,epoch,sequence,received_wall_ms,raw}=>{
                lane.on_ticker(&base,venue,shard,epoch,sequence,
                    received_wall_ms,&raw,wall_ms());
            }
            Event::R1088DurableFrame(_)|Event::R1177DurableFrame(_)|Event::R1241BatchDurableFrame{..}=>return Err(
                "R155 source review cannot receive private committed frame".into()),
            Event::Canonical(_)=>return Err("R155 source review cannot receive canonical pipeline control".into()),
            Event::Reset{..}=>return Err(
                "R155 WS reconnect: review aborts unsealed, no automatic position restore".into()),
            Event::R1246TelemetryTick=>return Err("R155 source review cannot receive R1246 telemetry".into()),
            Event::TaskError{venue,shard,error}=>return Err(
                format!("R155 public WS {venue}/{shard}: {error}").into()),
            Event::TaskDone{reconnects,..}=>{
                if reconnects!=0{return Err("R155 reconnect during research".into())}
                done+=1;if done==tasks{break}
            },
        }
    }
    if done!=tasks||read_json(kill_path)["state"]!="ENGAGED"{
        return Err("R155 incomplete or unsafe session remains unsealed".into())
    }
    revision=revision.checked_add(1).ok_or("R155 final revision overflow")?;
    source_identity.before_checkpoint(&lane.strategy)
        .map_err(|e|format!("R178 final source drift denied: {e:?}"))?;
    save_source_paper_review(&checkpoint,&lane.strategy,
        revision,monotonic_ns(),true)
        .map_err(|e|format!("R155 final fsync denied: {e:?}"))?;
    let result=json!({
        "profile":"R155_SOURCE_OWNED_PUBLIC_PAPER_REVIEW",
        "exclusive_virtual_paper_owner":true,
        "candidate_build_provenance":candidate_build_provenance()?,
        "r167_source_ws_fleet":{
            "width_source":"fast_market_shadow.ws_fleet.group_size",
            "start_interval_source":"fast_market_shadow.ws_fleet.start_interval_sec",
            "source_config_sha256":plan.config_sha256,
            "effective_group_size":applied_ws_group_size,
            "effective_groups":applied_ws_groups,
            "ws_task_count":tasks,
            "start_stagger_ms":applied_ws_stagger_ms,
            "twelve_decision_owners_unchanged":true,
            "python_eligibility_parity_proven":false,
            "installed_paper_applied":false
        },
        "source_sha256":report["source_sha256"],
        "effective_research_config":report,
        "portfolio":lane.strategy.portfolio.snapshot(),
        "public_book_frames":books,
        "public_ticker_ready_bases":lane.ready_bases(),
        "warm_public_pair_events":lane.mature_pairs,
        "denied_public_pair_events":lane.denied_pairs,
        "denied_invalid_source_frames":source_denies,
        "checkpoint_revision":revision,
        "checkpoint_source_locked_and_sealed":true,
        "r178_file_source_identity_checked_at_admission_lifecycle_and_seal":true,
        "r178_source_sha256":source_identity.source_sha256(),
        "r178_source_daily_quota":source_identity.quotas().0,
        "r178_continuous_unchanged_file_attested":false,
        "no_automatic_crash_recovery":true,
        "real_orders":false,"installed_paper_modified":false,
        "public_transport_independently_attested":false
    });
    if let Some(path)=output{fs::write(path,serde_json::to_vec_pretty(&result)?)?}
    println!("{}",serde_json::to_string(&result)?);
    Ok(())
}

fn r1246_read_cohort(path:&PathBuf,available:&[String])->Result<Vec<String>,String>{
    let raw=fs::read_to_string(path).map_err(|e|format!("R1246 cohort read: {e}"))?;
    let available=available.iter().map(String::as_str).collect::<HashSet<_>>();
    let mut seen=HashSet::<String>::new();
    let mut selected=Vec::new();
    for base in raw.lines().map(str::trim).filter(|base|!base.is_empty()){
        if base.len()>32||!base.bytes().all(|c|c.is_ascii_uppercase()||c.is_ascii_digit())
            ||!available.contains(base)||!seen.insert(base.to_owned()){
            return Err("R1246 cohort contains invalid, duplicate, or out-of-universe base".into())
        }
        selected.push(base.to_owned());
    }
    if selected.len()!=R1246_COHORT_WIDTH{
        return Err(format!("R1246 cohort width {} != {}",selected.len(),R1246_COHORT_WIDTH))
    }
    Ok(selected)
}

fn r1266_exact_soak_gate(secs:u64,requested:bool,r1119:bool,monitored:bool)->bool{
    requested&&secs==10_800&&!r1119&&!monitored
}
fn r1266_preflight_gate(secs:u64,requested:bool,r1119:bool,monitored:bool)->bool{
    requested&&secs==60&&!r1119&&!monitored
}
fn r1268_receiver_diag_gate(secs:u64,requested:bool,r1119:bool,monitored:bool)->bool{
    requested&&(300..=900).contains(&secs)&&!r1119&&!monitored
}

fn r1266_read_cohort(path:&PathBuf,available:&[String])->Result<Vec<String>,String>{
    let raw=fs::read_to_string(path).map_err(|e|format!("R1266 cohort read: {e}"))?;
    let available=available.iter().map(String::as_str).collect::<HashSet<_>>();
    let mut seen=HashSet::<String>::new();
    let mut selected=Vec::new();
    for base in raw.lines().map(str::trim).filter(|base|!base.is_empty()){
        if base.len()>32||!base.bytes().all(|c|c.is_ascii_uppercase()||c.is_ascii_digit())
            ||!available.contains(base)||!seen.insert(base.to_owned()){
            return Err("R1266 cohort contains invalid, duplicate, or out-of-universe base".into())
        }
        selected.push(base.to_owned());
    }
    if selected.len()!=8{
        return Err(format!("R1266 cohort width {} != 8",selected.len()))
    }
    Ok(selected)
}

/// R257: one-shot PUBLIC GET-only inventory. No WS tasks, positions, journals,
/// private exchange requests, Telegram, or installed service interaction.
fn r257_membership_receipt(meta:&NativeMetadataSnapshot)->Result<Value,&'static str>{
    let bases=meta.bases();
    if bases.is_empty()||bases.len()>512||
       bases.iter().any(|b|b.is_empty()||b.len()>32||
         !b.bytes().all(|c|c.is_ascii_uppercase()||c.is_ascii_digit())){
        return Err("R257 malformed public intersection")
    }
    let canonical_sha=|ids:&[String]| -> String {
        openssl::sha::sha256(ids.join("\n").as_bytes()).iter()
            .map(|x|format!("{x:02x}")).collect()
    };
    let eligible=bases.iter().filter(|b|meta.markets.get(*b)
        .is_some_and(|m|m.static_reason.is_none())).cloned().collect::<Vec<_>>();
    let rows=bases.iter().map(|base|{
        let m=&meta.markets[base];
        json!({"base":base,"okx_instrument":m.spot_symbol,
            "binance_instrument":m.future_symbol,
            "okx_status":m.spot_status,"binance_status":m.future_status,
            "static_reason":m.static_reason})
    }).collect::<Vec<_>>();
    Ok(json!({
        "schema":"SPOTARB/R257/PUBLIC-GET-MEMBERSHIP/V1",
        "source":"native_market_core::market_metadata::fetch_current_metadata",
        "local_generation_not_exchange_epoch":meta.generation,
        "loaded_wall_ms":meta.loaded_wall_ms,
        "public_intersection_count":bases.len(),
        "public_intersection_sha256":canonical_sha(&bases),
        "public_intersection_bases":bases,
        "eligible_count":eligible.len(),"eligible_sha256":canonical_sha(&eligible),
        "eligible_bases":eligible,"rows":rows,
        "binance_active":meta.binance_active,"okx_active":meta.okx_active,
        "metadata_ready":meta.metadata_ready,"static_denied":meta.static_denied,
        "exchange_private_api_used":false,"network_mutation_possible":false,
        "websocket_connections_started":false,"paper_entries_created":false,
        "installed_paper_ws_membership_attested":false,
        "historical_eligibility_parity_attested":false,
        "real_trading_authorized":false
    }))
}

/// R266: an offline executed candidate-ELF admission and 160-field
/// evidence GATE. Existing R176 reports remain historical SOURCE-ONLY.
fn r266_candidate_yaml_report(cfg:&NativeTradingConfig,historical:&Value,
    build:&Value)->Result<Value,&'static str>{
    if historical["field_count"]!=160||
        historical["total_research_reported"]!=149||
        historical["r174_unsupported"]!=11||
        historical["installed_paper_applied_proven"]!=0||
        historical["source_only"]!=true||
        build["build_snapshot"]["operator_yaml_sha256"].as_str()!=Some(cfg.sha256.as_str())||
        build["limits"]["operator_source_applied_at_runtime"]!=false||
        build["limits"]["deployed_elf_attested"]!=false{
        return Err("R266 source or build identity mismatch")
    }
    let fields=historical["fields"].as_array()
        .ok_or("R266 missing historical field rows")?;
    if fields.len()!=160{return Err("R266 wrong field count")}
    let mut unique=std::collections::BTreeSet::new();
    let mut guarded=0usize;
    let mut research=0usize;
    for entry in fields{
        let id=entry["field"].as_str().ok_or("R266 absent field id")?;
        if !unique.insert(id)||entry["installed_paper_applied"]!="NOT_PROVEN"
            ||entry["actual_consumer_runtime"]!="NOT_ATTESTED"{
            return Err("R266 invented or duplicate field proof")
        }
        if entry["research_stage"]=="UNWIRED_R174_GUARD_ONLY"{
            guarded+=1
        }else{research+=1}
    }
    if guarded!=11||research!=149{return Err("R266 inconsistent classifications")}
    let diagnostic=review_two_paper_admissions_r258(cfg,wall_ms());
    if diagnostic["source_yaml_sha256"].as_str()!=Some(cfg.sha256.as_str())||
       diagnostic["source_paper_r155"]["preflight_blockers"].as_array()
         .is_some_and(|v|v.iter().any(|x|x=="SOURCE_PROJECTION_DRIFT"))||
       diagnostic["compiled_fallback_r95"]["preflight_blockers"].as_array()
         .is_some_and(|v|v.iter().any(|x|x=="SOURCE_PROJECTION_DRIFT"))||
       diagnostic["installed_field_applications_proven"]!=0||
       diagnostic["automatic_cutover_authorized"]!=false{
        return Err("R266 diagnostic unsafe claim")
    }
    Ok(json!({
        "schema":"SPOTARB/R266/OFFLINE-EXECUTED-YAML-CONTRACT/V1",
        "candidate_elf_build_proof":build,
        "current_operator_source_sha256":cfg.sha256,
        "historical_r64_baseline_sha256":historical["operator_yaml_sha256"],
        "current_matches_historical_baseline":
            historical["operator_yaml_sha256"].as_str()==Some(cfg.sha256.as_str()),
        "historical_source_only_field_inventory":fields,
        "source_research_count":research,"source_guard_only_count":guarded,
        "actual_source_admission":diagnostic,
        "installed_per_field_proven":0,
        "installed_source_runtime_applied":false,
        "executed_candidate_is_not_installed_paper":true,
        "websocket_connections_started":false,
        "virtual_trades_created":false,
        "private_exchange_api_used":false,
        "real_trading_authorized":false,
    }))
}

/// R276: explicitly restricted test-copy ONLY. Execute real source-paper
/// constructor and confirm applied *in-memory* limits against a replay-safe
/// modeled portfolio. Never copy this profile to production.
fn r276_offline_model_receipt(original:&str,build:&Value)->Result<Value,&'static str>{
    let raw=NativeTradingConfig::parse(original).map_err(|_|"R276 original YAML invalid")?;
    if build["build_snapshot"]["operator_yaml_sha256"].as_str()!=Some(raw.sha256.as_str())||
       raw.raw["general"]["trade_enabled"].as_bool().is_none()||
       raw.raw["general"]["paper_entries_enabled"]!=false||
       raw.raw["scale_in_hints"]["enabled"]!=true||
       raw.raw["analytics"]["enabled"]!=true||
       raw.raw["real_trial"]["enabled"].as_bool().is_none()||
       raw.raw["real_trial"]["autonomous_real_enabled"].as_bool().is_none()||
       build["limits"]["deployed_elf_attested"]!=false{
        return Err("R276 original test-source boundary changed")
    }
    let edits=[
        ("  trade_enabled: true","  trade_enabled: false"),
        ("  paper_entries_enabled: false","  paper_entries_enabled: true"),
        ("scale_in_hints:\n  enabled: true","scale_in_hints:\n  enabled: false"),
        ("analytics:\n  enabled: true","analytics:\n  enabled: false"),
        ("real_trial:\n  enabled: true","real_trial:\n  enabled: false"),
        ("  autonomous_real_enabled: true","  autonomous_real_enabled: false"),
    ];
    let mut variant=original.to_owned();
    for (from,to) in edits {
        if variant.matches(from).count()==1{variant=variant.replacen(from,to,1);}
        else if variant.matches(from).count()!=0||variant.matches(to).count()!=1{return Err("R276 non-unique sandbox toggle")}
    }
    let sandbox=NativeTradingConfig::parse(&variant)
        .map_err(|_|"R276 sandbox source invalid")?;
    let strategy=SupervisedStrategyPaper::new(sandbox.clone(),wall_ms())
        .map_err(|_|"R276 source strategy refuses sandbox")?;
    let applied=strategy.effective_research_config_report()
        .map_err(|_|"R276 incomplete research source application")?;
    let current_guard=review_two_paper_admissions_r258(&raw,wall_ms());
    if current_guard["source_paper_r155"]["actual_in_memory_constructor_accepted"]!=false||
       applied["source_sha256"].as_str()!=Some(sandbox.sha256.as_str())||
       applied["paper_max_daily_entries"]!=3000||
       applied["paper_max_concurrent_positions"]!=10||
       applied["decision_entry_required_net_bps"]!=42.0||
       applied["model_quote_budget"]!=10.0||
       applied["compiled_legacy_default_daily_cap"]!=500||
       applied["review_source_quota_matches_compiled_legacy_default"]!=false||
       strategy.portfolio.max_daily_entries!=3000||
       strategy.portfolio.max_concurrent_positions!=10||
       strategy.portfolio.stats.entries!=0||
       !strategy.portfolio.positions.is_empty(){
        return Err("R276 in-memory effective model/source mismatch")
    }
    Ok(json!({
        "schema":"SPOTARB/R276/ISOLATED-IN-MEMORY-SOURCE-MODEL/V1",
        "original_yaml_sha256":raw.sha256,
        "sandbox_yaml_sha256":sandbox.sha256,
        "sandbox_differs_from_original":true,
        "sandbox_only_switches_changed":[
          "general.trade_enabled","general.paper_entries_enabled",
          "scale_in_hints.enabled","analytics.enabled",
          "real_trial.enabled","real_trial.autonomous_real_enabled"],
        "operator_features_temporarily_disabled":["scale_in_hints","analytics"],
        "original_admitted":false,"sandbox_source_model_admitted":true,
        "applied_in_memory_source_model":applied,
        "portfolio_runtime_counters":{
          "daily_entry_limit":strategy.portfolio.max_daily_entries,
          "open_position_limit":strategy.portfolio.max_concurrent_positions,
          "entries":strategy.portfolio.stats.entries,
          "open_positions":strategy.portfolio.positions.len()},
        "candidate_elf_build_proof":build,
        "original_160_field_semantics_equivalent":false,
        "installed_paper_applied":false,
        "deployed_binary_attested":false,
        "public_ws_started":false,"private_api_used":false,
        "db_or_journal_written":false,"real_order_authority":false
    }))
}

include!("../native_paper_isolated_r1296.rs");
#[cfg(test)]
use spotarb_native_market_core::{Book,SourceProvenance,fill_economics,R1279FrameTiming,r1310_stage_event,pipeline_diagnostics_r1325,market_data_ingress_r1205,ingress_runtime_r1205,native_paper_hedge_r1296,universal_connection_manager_r1209,parse};
#[cfg(test)]
#[path="../native_paper_bridge_r1301.rs"]
mod bridge1301;
include!("../native_paper_observation_r1301.rs");
#[cfg(test)]
#[path="../native_paper_observation_r1301_tests.rs"]
mod r1301_entrypoint_tests;

#[tokio::main(flavor="multi_thread",worker_threads=4)]
async fn main()->Result<(),Box<dyn std::error::Error>>{
    let args=std::env::args().skip(1).collect::<Vec<_>>();
    let runtime_plan=parse_runtime_plan(&args,&runtime_conflicting_flags())
        .map_err(|e|std::io::Error::new(std::io::ErrorKind::InvalidInput,e))?;
    if !matches!(runtime_plan.mode,RuntimeMode::Legacy) {
        return dispatch_runtime_plan(runtime_plan).await
    }
    // R1205: actual entry point, before any Portfolio/safety/private-path setup.
    // This branch cannot construct a PAPER portfolio or invoke an order API.
    if std::env::args().nth(1).as_deref()==Some("--r1205-ingress-source-only"){
        let root=std::env::args().nth(2).map(PathBuf::from).ok_or("R1205 private output root required")?;
        spotarb_native_market_core::ingress_source_only_r1205::run_source_only_with_config(root,cfg()).await?;
        return Ok(())
    }

    // R157 local proof route returns BEFORE metadata, exchange sockets,
    // account safety reads, virtual positions or any PAPER runtime state.
    if std::env::args_os().nth(1).as_deref()==Some(std::ffi::OsStr::new("--r157-build-proof")){
        if std::env::args_os().len()!=2{
            return Err("R157 proof accepts no extra arguments".into())
        }
        let proof=candidate_build_provenance()?;
        println!("{}",serde_json::to_string_pretty(&proof)?);
        return Ok(())
    }
    if std::env::args_os().nth(1).as_deref()==Some(std::ffi::OsStr::new("--r286-source-consumers-proof")){
        if std::env::args_os().len()!=2||
           std::env::var("R286_READONLY_ONLY_ALLOWED").ok().as_deref()!=Some("YES"){
            return Err("R286 requires exact read-only opt-in".into())
        }
        let kill_path=std::env::var("R21_KILL_SWITCH")
            .unwrap_or_else(|_|"/home/tmpadmin/spotbot/data/real_kill_switch.json".into());
        if read_json(&kill_path)["state"]!="ENGAGED"{
            return Err("R286 refuses offline preview unless kill switch ENGAGED".into())
        }
        let cfg=NativeTradingConfig::load(
            &PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config/native_trading.yaml"))
            .map_err(|e|format!("R286 YAML parse denied: {e:?}"))?;
        let build=candidate_build_provenance()?;
        if build["build_snapshot"]["operator_yaml_sha256"].as_str()!=Some(cfg.sha256.as_str())||
           build["limits"]["deployed_elf_attested"]!=false{
           return Err("R286 candidate build/source mismatch".into())
        }
        let report=audit_existing_consumer_boundaries_r286(&cfg)
            .map_err(|e|format!("R286 source-only report denied: {e:?}"))?;
        println!("{}",serde_json::to_string_pretty(
            &json!({"source_consumers":report,"candidate_build_provenance":build}))?);
        return Ok(())
    }
    if std::env::args_os().nth(1).as_deref()==Some(std::ffi::OsStr::new("--r276-offline-model-proof")){
        if std::env::args_os().len()!=2 ||
           std::env::var("R276_OFFLINE_ONLY_ALLOWED").ok().as_deref()!=Some("YES"){
            return Err("R276 requires explicit offline-only opt-in".into())
        }
        let kill_path=std::env::var("R21_KILL_SWITCH")
            .unwrap_or_else(|_|"/home/tmpadmin/spotbot/data/real_kill_switch.json".into());
        if read_json(&kill_path)["state"]!="ENGAGED"{
            return Err("R276 refuses test unless REAL kill switch ENGAGED".into())
        }
        let original=fs::read_to_string(
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config/native_trading.yaml"))?;
        let proof=r276_offline_model_receipt(&original,&candidate_build_provenance()?)?;
        println!("{}",serde_json::to_string_pretty(&proof)?);
        return Ok(())
    }
    if std::env::args_os().nth(1).as_deref()==Some(std::ffi::OsStr::new("--r266-yaml-diagnostic")){
        if std::env::args_os().len()!=2||
          std::env::var("R266_OFFLINE_REVIEW_AUTHORIZED").ok().as_deref()!=Some("YES"){
            return Err("R266 exact argument and explicit offline review gate required".into())
        }
        // Fail-closed gate, with NO sockets, private API, PaperPortfolio
        // session or installed service operations.
        let kill_path=std::env::var("R21_KILL_SWITCH")
          .unwrap_or_else(|_|"/home/tmpadmin/spotbot/data/real_kill_switch.json".into());
        if read_json(&kill_path)["state"]!="ENGAGED"{
            return Err("R266 offline review requires ENGAGED REAL kill switch".into())
        }
        let cfg=NativeTradingConfig::load(
           &PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config/native_trading.yaml"))
           .map_err(|e|format!("R266 current source parsing denied: {e:?}"))?;
        let historical=embedded_r176()
           .map_err(|e|format!("R266 historical source identity denied: {e:?}"))?;
        let report=r266_candidate_yaml_report(&cfg,&historical,
           &candidate_build_provenance()?)?;
        println!("{}",serde_json::to_string_pretty(&report)?);
        return Ok(())
    }
    if std::env::args_os().nth(1).as_deref()==Some(std::ffi::OsStr::new("--r257-public-membership")){
        if std::env::args_os().len()!=2 ||
            std::env::var("R257_PUBLIC_ONLY_ALLOWED").ok().as_deref()!=Some("YES"){
            return Err("R257 requires explicit PUBLIC-only permission and exact argument".into())
        }
        let kill_path=std::env::var("R21_KILL_SWITCH")
            .unwrap_or_else(|_|"/home/tmpadmin/spotbot/data/real_kill_switch.json".into());
        if read_json(&kill_path)["state"]!="ENGAGED"{
            return Err("R257 refuses public probe unless REAL kill switch ENGAGED".into())
        }
        // Only existing public Binance/OKX metadata GET builders; no WS or PAPER.
        let snapshot=fetch_current_metadata(1)
            .map_err(|e|format!("R257 public metadata failed: {e:?}"))?;
        let receipt=r257_membership_receipt(&snapshot)?;
        println!("{}",serde_json::to_string_pretty(&json!({
            "membership":receipt,
            "candidate_elf_build_proof":candidate_build_provenance()?
        }))?);
        return Ok(())
    }
    let secs:u64=std::env::var("R21_SECONDS").ok().and_then(|x|x.parse().ok()).unwrap_or(30);
    assert!((10..=21600).contains(&secs));
    let r1246_enabled=std::env::var("R1246_DUAL_ARCHIVE").ok().as_deref()==Some("YES");
    if r1246_enabled && std::env::var("R1246_UCM_MODE").ok().as_deref()!=Some("YES"){
        return Err("R1246 requires integrated volatile UCM mode".into())
    }
    if r1246_enabled {
        if !matches!(secs,60|300|3600) ||
            std::env::var_os("PAPER_R819_PRIVATE_ROOT").is_some() ||
            std::env::var_os("PAPER_R1173_PRIVATE_ROOT").is_some() ||
            std::env::var_os("PAPER_R1088_PRIVATE_BATCH").is_some() ||
            std::env::var_os("ASTRA_R1119_FULL_RUST_PAPER").is_some() ||
            std::env::var_os("ASTRA_R1205_INGRESS").is_some() ||
            std::env::var_os("ASTRA_R1116_PRIVATE_NOTICE_ROOT").is_some() ||
            std::env::var("PAPER_PLANNED_RESTORE").ok().as_deref()!=Some("0") ||
            std::env::var_os("PAPER_NATIVE_RUST_CONFIG_REVIEW").is_some() {
            return Err("R1246 requires isolated full-path PAPER without DB1/DB2/private ingress".into())
        }
        let session=std::env::var("R1246_SESSION_ID")
            .map_err(|_|"R1246 session identity is required")?;
        if std::env::var("PAPER_VIRTUAL_FOUR_LEG_SESSION_ID").ok().as_deref()!=Some(session.as_str()){
            return Err("R1246 PAPER journal/session identity mismatch".into())
        }
        let run_root=PathBuf::from("/mnt/spotarb_db_test/r1246_runs").join(&session);
        for required in ["PAPER_CONTINUITY_PATH","PAPER_VIRTUAL_FOUR_LEG_JOURNAL_PATH",
                         "PAPER_VIRTUAL_FOUR_LEG_SESSION_ID","R1246_TELEMETRY_PATH",
                         "PAPER_STATUS_JSON","R21_OUTPUT","R1246_UCM_SUMMARY_PATH",
                         "R1246_BASES_FILE"] {
            if std::env::var_os(required).is_none(){return Err(format!("R1246 missing isolated PAPER path: {required}").into())}
        }
        for key in ["PAPER_CONTINUITY_PATH","PAPER_VIRTUAL_FOUR_LEG_JOURNAL_PATH",
                    "R1246_TELEMETRY_PATH","PAPER_STATUS_JSON","R21_OUTPUT","R1246_UCM_SUMMARY_PATH",
                    "R1246_BASES_FILE"] {
            let value=PathBuf::from(std::env::var(key).map_err(|_|format!("R1246 invalid path: {key}"))?);
            if !value.is_absolute()||!value.starts_with(&run_root)||value==run_root{
                return Err(format!("R1246 state/log path escapes isolated run root: {key}").into())
            }
        }
    }
    let force_reconnect=std::env::var("R21_FORCE_RECONNECT").ok().as_deref()==Some("1");
    // Diagnostic-only public quote turnover. Default book pipeline unchanged.
    let ticker_review=match std::env::var("R154_PUBLIC_TICKERS_REVIEW").ok().as_deref(){
        None=>false,Some("1")=>true,_=>return Err("R154_PUBLIC_TICKERS_REVIEW must be unset or 1".into())
    };
    let capture=std::env::var("R21_CAPTURE").ok().map(PathBuf::from);
    let output=std::env::var("R21_OUTPUT").ok().map(PathBuf::from);
    let kill_path=std::env::var("R21_KILL_SWITCH").unwrap_or_else(|_|"/home/tmpadmin/spotbot/data/real_kill_switch.json".into());
    let preflight_path=std::env::var("R21_PREFLIGHT").unwrap_or_else(|_|"/home/tmpadmin/spotbot/reports/real_micro_preflight_latest.json".into());

    let meta:NativeMetadataSnapshot=fetch_current_metadata(1).map_err(|e|format!("metadata:{e:?}"))?;
    if r1246_enabled && meta.markets.values().any(|m|
        m.buy_exchange!="okx_spot"||m.sell_exchange!="binance_futures"||
        m.route_label!="OKX Spot BUY -> Binance Futures SHORT"){
        return Err("R1246 metadata route is not OKX Spot BUY + Binance Futures SHORT".into())
    }
    // R1119: expressly authorized full-market operational Rust PAPER.
    // Existing independent original 17-table history, strict R1116 queue,
    // shared Binance IP guard and REAL kill remain fail-closed.
    let r1119_operational=std::env::var("ASTRA_R1119_FULL_RUST_PAPER")
       .ok().as_deref()==Some("YES");
    if r1119_operational {
       // R1127 one-time actual-public 120s profile, with all existing
       // REAL kill, service, original DB and identity gates unchanged.
       let r1127_bounded=secs==120&&
           std::env::var("ASTRA_R1127_BOUNDED_PROFILE")
             .ok().as_deref()==Some("YES");
       if (!(300..=21600).contains(&secs)&&!r1127_bounded)||
          std::env::var("ASTRA_R1087_PAPER_PUBLIC_ONLY").ok().as_deref()!=Some("YES")||
          std::env::var("PAPER_R819_REVIEW_ONLY").ok().as_deref()!=Some("YES")||
          std::env::var("PAPER_R1087_BOUNDED_ACK_WAIT").ok().as_deref()!=Some("YES")||
          std::env::var("PAPER_R819_PRIVATE_ROOT").is_err()||
          std::env::var("PAPER_R1088_PRIVATE_BATCH").ok().as_deref()!=Some("YES")||
          std::env::var("PAPER_NATIVE_RUST_CONFIG_REVIEW").ok().as_deref()!=Some("2")||
          std::env::var("ASTRA_R1116_PRIVATE_NOTICE_ROOT").is_err()||
          std::env::var("ASTRA_R1121_BOOKS5_SNAPSHOT_RESET").ok().as_deref()!=Some("YES")||
          std::env::var("R1087_PUBLIC_CANARY_BASE_LIMIT").is_ok()||
          std::env::var("PAPER_PLANNED_RESTORE").ok().as_deref()!=Some("0")||
          read_json(&kill_path)["state"]!="ENGAGED" {
           return Err("R1119 full Rust PAPER operator safety not met".into())
       }
       let service=std::process::Command::new("/usr/bin/systemctl")
          .args(["show","spotarb-native-paper-continuous.service",
                 "-p","ActiveState","-p","UnitFileState","-p","MainPID"])
          .output().map_err(|e|format!("R1119 service state: {e}"))?;
       let status=String::from_utf8_lossy(&service.stdout);
       if !service.status.success()||
          !status.contains("ActiveState=inactive")||
          !status.contains("UnitFileState=disabled")||
          !status.contains("MainPID=0"){
          return Err("R1119 legacy PAPER must remain stopped".into())
       }
    }
    // R1091: consented monitored 8-base public-PAPER observation only.
    // Keep the wider 234-base published default and REAL unchanged.
    let monitored_paper=std::env::var("ASTRA_R1091_MONITORED_PAPER")
      .ok().as_deref()==Some("YES");
    let r1266_requested=std::env::var("ASTRA_R1266_ISOLATED_180M_SOAK")
      .ok().as_deref()==Some("YES");
    let r1266_exact_soak=r1266_exact_soak_gate(
      secs,r1266_requested,r1119_operational,monitored_paper);
    let r1266_preflight_requested=std::env::var("ASTRA_R1266_ISOLATED_PREFLIGHT")
      .ok().as_deref()==Some("YES");
    let r1266_preflight=r1266_preflight_gate(
      secs,r1266_preflight_requested,r1119_operational,monitored_paper);
    let r1268_diag_requested=std::env::var("R1268_RECEIVER_DIAGNOSTIC").ok().as_deref()==Some("YES")&&
      std::env::var("R1268_CAPTURE").ok().as_deref()==Some("YES");
    let r1268_diagnostic=r1268_receiver_diag_gate(secs,r1268_diag_requested,r1119_operational,monitored_paper);
    if r1268_diag_requested&&!r1268_diagnostic&&!r1266_preflight{ return Err("R1268 diagnostic requires 300..900s and isolated non-operational mode".into()) }
    if r1266_preflight_requested&&!r1266_preflight{
      return Err("R1266 preflight requires exactly 60s and cannot combine with R1119/monitored mode".into())
    }
    if r1266_requested&&!r1266_exact_soak{
      return Err("R1266 isolated soak requires exactly 10800s and cannot combine with R1119/monitored mode".into())
    }
    if r1266_exact_soak&&std::env::var("R1087_PUBLIC_CANARY_BASE_LIMIT")
       .ok().as_deref()!=Some("8"){
      return Err("R1266 isolated soak is restricted to an explicit eight-base cohort".into())
    }
    if monitored_paper && (secs<300||secs>900 ||
      std::env::var("R1087_PUBLIC_CANARY_BASE_LIMIT").ok().as_deref()!=Some("8") ||
      std::env::var("ASTRA_R1087_PAPER_PUBLIC_ONLY").ok().as_deref()!=Some("YES") ||
      std::env::var("PAPER_R819_REVIEW_ONLY").ok().as_deref()!=Some("YES") ||
      std::env::var("PAPER_R1087_BOUNDED_ACK_WAIT").ok().as_deref()!=Some("YES") ||
      std::env::var("PAPER_R819_PRIVATE_ROOT").is_err()){
      return Err("R1091 monitored public PAPER requires private 300..900 second admission".into())
    }
    // R1087: tiny bounded public-WS candidate canary only. The ordinary
    // installed PAPER continues using the full public universe.
    let mut bases=match std::env::var("R1087_PUBLIC_CANARY_BASE_LIMIT").ok().as_deref(){
       None=>meta.bases(),
       Some("8") if std::env::var("ASTRA_R1087_PAPER_PUBLIC_ONLY").ok().as_deref()==Some("YES")
          &&std::env::var("PAPER_R819_REVIEW_ONLY").ok().as_deref()==Some("YES")
          &&std::env::var("PAPER_R1087_BOUNDED_ACK_WAIT").ok().as_deref()==Some("YES")
          &&(std::env::var("PAPER_R819_PRIVATE_ROOT").is_ok() ||
             ((r1266_exact_soak||r1266_preflight||r1268_diagnostic)&&std::env::var("PAPER_R1173_PRIVATE_ROOT").is_ok()))
          &&(secs<=60 || monitored_paper || r1266_exact_soak || r1266_preflight || r1268_diagnostic)=>{
         let mut sample=if r1266_exact_soak||r1266_preflight||r1268_diagnostic{
             let path=PathBuf::from(std::env::var("R1266_BASES_FILE")
                 .map_err(|_|"R1266 explicit eight-base cohort file required")?);
             r1266_read_cohort(&path,&meta.bases())?
         }else{
             let mut available=meta.bases();
             available.sort();
             available.truncate(8);
             available
         };
         if sample.len()!=8 {return Err("R1087 insufficient public bases".into())}
         sample
       },
       _=>return Err("R1087 sample mode requires private bounded public-PAPER opt-in".into())
    };
    if r1246_enabled{
        let path=PathBuf::from(std::env::var("R1246_BASES_FILE")
            .map_err(|_|"R1246 selected-bases file is required")?);
        bases=r1246_read_cohort(&path,&meta.bases())?;
    }
    let expected=bases.len();
    let r1246_selected_bases_sha256=if r1246_enabled{
        Some(openssl::sha::sha256(bases.join("\n").as_bytes()).iter()
            .map(|x|format!("{x:02x}")).collect::<String>())
    }else{None};
    let r1266_selected_bases_sha256=if r1266_exact_soak||r1266_preflight||r1268_diagnostic{
        Some(openssl::sha::sha256(bases.join("\n").as_bytes()).iter()
            .map(|x|format!("{x:02x}")).collect::<String>())
    }else{None};
    let safety=evaluate_safety(&safety_input(&read_json(&kill_path),&read_json(&preflight_path)));
    if safety.real_launch_authorized {return Err("shadow invariant: launch authority true".into())}
    let source_mode=match std::env::var("R155_SOURCE_PAPER_REVIEW").ok().as_deref(){
        None=>false,Some("1")=>true,_=>return Err(
          "R155_SOURCE_PAPER_REVIEW must be unset or 1".into())
    };
    if source_mode{
        if !ticker_review{
            return Err("R155 source-owned mode requires R154_PUBLIC_TICKERS_REVIEW=1".into())
        }
        if capture.is_some(){
            return Err("R155 source-owned mode forbids legacy capture output".into())
        }
        return r155_source_paper_run(secs,force_reconnect,meta,bases,
            &kill_path,output).await;
    }
    // The fixed 32-base routing map exists ONLY for the legacy compiled
    // fallback branch. R155 builds the source-selected 40-wide map itself.
    let mut ticker_shards=HashMap::<String,usize>::new();
    for (shard,chunk) in bases.chunks(SHARD).enumerate(){
        for base in chunk {ticker_shards.insert(base.clone(),shard);}
    }

    // Explicit opt-in only. The already running production PAPER process
    // keeps its original compiled decision parameters without restart.
    // This bridge never enables private APIs or real trade execution.
    let fallback=PaperPortfolio::new(wall_ms())
      .map_err(|e|format!("paper temporal fallback: {e:?}"))?;
    let fallback_temporal=PaperTemporalPolicyReview{
        min_lifetime_ns:fallback.min_lifetime_ns,
        exit_spread_bps:fallback.exit_spread_bps,
        reentry_cooldown_ns:fallback.reentry_cooldown_ns,
        net_target_bps:fallback.net_target_bps,
    };
    let (paper_cfg,config_source,yaml_sha,temporal,source_ws_plan)=match std::env::var("PAPER_NATIVE_RUST_CONFIG_REVIEW").ok().as_deref(){
        None=>(cfg(),"COMPILED_FALLBACK",None,fallback_temporal,None),
        Some("1")=>{
            let yaml=NativeTradingConfig::load(
              &PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config/native_trading.yaml"))
              .map_err(|e|format!("paper Rust source YAML denied: {e:?}"))?;
            // P1-06: ALL three existing checks plus no-REAL switches
            // are now one fail-closed opt-in admission. Decision-only
            // YAML parsing must never bypass quota/unsupported consumers.
            let decision=admit_public_paper_yaml_review(&yaml)
              .map_err(|e|format!("paper Rust YAML admission denied: {e:?}"))?;
            let temporal=bind_public_paper_temporal_policy_review(&yaml)
              .map_err(|e|format!("paper temporal YAML denied: {e:?}"))?;
            (decision,"RUST_YAML_REVIEW_OPT_IN",Some(yaml.sha256),temporal,None)
        },
        Some("2")=>{
            // R1100: isolated <=60s REAL-disabled PUBLIC-PAPER probe only.
            // The installed PAPER service and original YAML are never changed.
            if std::env::var("ASTRA_R1100_SAFE_CONFIG_PROBE").ok().as_deref()!=Some("YES")
              ||(secs>60&&!r1119_operational)
              ||std::env::var("ASTRA_R1087_PAPER_PUBLIC_ONLY").ok().as_deref()!=Some("YES")
              ||std::env::var("PAPER_R819_REVIEW_ONLY").ok().as_deref()!=Some("YES")
              ||std::env::var("PAPER_R1087_BOUNDED_ACK_WAIT").ok().as_deref()!=Some("YES")
              ||std::env::var("PAPER_R819_PRIVATE_ROOT").is_err()
              ||std::env::var("PAPER_PLANNED_RESTORE").ok().as_deref()!=Some("0"){
                return Err("R1100 candidate requires private short PUBLIC-PAPER only".into())
            }
            let status=std::process::Command::new("/usr/bin/systemctl")
                .args(["show","spotarb-native-paper-continuous.service",
                       "-p","ActiveState","-p","UnitFileState","-p","MainPID"])
                .output().map_err(|e|format!("R1100 service guard unavailable: {e}"))?;
            let unit=String::from_utf8(status.stdout)
                .map_err(|_|"R1100 service status invalid UTF-8")?;
            if !status.status.success()||
               !unit.lines().any(|l|l=="ActiveState=inactive")||
               !unit.lines().any(|l|l=="UnitFileState=disabled")||
               !unit.lines().any(|l|l=="MainPID=0"){
                return Err("R1100 requires original PAPER inactive and disabled".into())
            }
            let candidate=PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("config/native_trading_paper_only_r1076.yaml");
            let bytes=fs::read(&candidate)
                .map_err(|e|format!("R1100 candidate source read denied: {e}"))?;
            let digest=openssl::sha::sha256(&bytes).iter()
                .map(|b|format!("{b:02x}")).collect::<String>();
            if digest!="38a71a9c457c00c686c6f7c4c808d05cfe37e43cc760fc8591eac960d101ec4a"{
                return Err("R1100 versioned PAPER candidate SHA mismatch".into())
            }
            let yaml=NativeTradingConfig::load(&candidate)
                .map_err(|e|format!("R1100 isolated YAML source refused: {e:?}"))?;
            let decision=admit_public_paper_yaml_review(&yaml)
                .map_err(|e|format!("R1100 isolated YAML runtime admission denied: {e:?}"))?;
            verify_paper_portfolio_quota_parity(&yaml)
                .map_err(|e|format!("R1100 paper quota mismatch: {e:?}"))?;
            let temporal=bind_public_paper_temporal_policy_review(&yaml)
                .map_err(|e|format!("R1100 PAPER temporal YAML refused: {e:?}"))?;
            let plan=SourceWsShardPlanR167::from_source(&yaml,&bases)
                .map_err(|e|format!("R1108 source-owned public WS plan denied: {e}"))?;
            (decision,"R1100_ISOLATED_PAPER_YAML_CANDIDATE",Some(yaml.sha256),temporal,Some(plan))
        },
        _=>return Err("PAPER_NATIVE_RUST_CONFIG_REVIEW must be unset, 1, or strictly gated 2".into()),
    };
    // R21 public portfolio models ENTRY independently of DecisionCore.
    // Require identical size, assumed fees, threshold and time bounds in
    // either compiled or opt-in YAML mode. No new running service is changed.
    verify_public_paper_decision_model_parity(
        &paper_cfg,
        &spotarb_native_market_core::native_paper_market_execution_review::
            PaperAssumptions::default()
    ).map_err(|e|format!("PAPER decision/model split-brain denied: {e:?}"))?;
    let mut applied_config=paper_config_evidence(&paper_cfg,config_source,yaml_sha.as_deref());
    if r1266_preflight{
        applied_config["r1266_isolated_preflight"]=json!({"duration_seconds":60,"explicit_operator_opt_in":true,"selected_bases":&bases,"selected_bases_sha256":r1266_selected_bases_sha256,"real_order_authority":false});
    }
    if r1266_exact_soak{
        applied_config["r1266_isolated_soak"]=json!({
            "duration_seconds":10_800,
            "explicit_operator_opt_in":true,
            "selected_bases":&bases,
            "selected_bases_sha256":r1266_selected_bases_sha256,
            "real_order_authority":false
        });
    }
    let max_age_ms=paper_cfg.max_age_ms;
    let mut owners=(0..OWNERS).map(|i|Owner::new(i,OWNERS,max_age_ms).unwrap()).collect::<Vec<_>>();
    let mut cores=(0..OWNERS).map(|i|DecisionCore::new(i,OWNERS,paper_cfg.clone(),HashMap::new()).unwrap()).collect::<Vec<_>>();
    for core in &mut cores {core.replace_routes(meta.generation,meta.routes.clone()).map_err(|e|format!("routes:{e:?}"))?}
    let mut history=PaperHistoryR823::from_env().await
        .map_err(|e|format!("PAPER R819 private history guard: {e}"))?;
    let r1173_compact=matches!(history,PaperHistoryR823::PrivateCompact(_));
    // R1173 is a *separate bounded candidate*, not R1119 operational
    // and never authorized to inherit unknown modeled positions.
    let batch_mode=match std::env::var("PAPER_R1088_PRIVATE_BATCH").ok().as_deref(){
        None=>false,
        Some("YES") if std::env::var("ASTRA_R1087_PAPER_PUBLIC_ONLY")
             .ok().as_deref()==Some("YES")&&history.is_private()&&
          (if r1173_compact{
            (secs<=120 || r1266_exact_soak || r1266_preflight || r1268_diagnostic) && !r1119_operational && !monitored_paper &&
            std::env::var("PAPER_R1173_EXPERIMENTAL").ok().as_deref()==Some("YES")
          }else{
            std::env::var("PAPER_R819_REVIEW_ONLY")
              .ok().as_deref()==Some("YES")&&
            (secs<=60||monitored_paper||r1119_operational)
          })=>true,
        _=>return Err("R1088/R1173 isolated PUBLIC PAPER gate denied".into())
    };
    if r1173_compact&&!batch_mode{
        return Err("R1173 compact source refuses unbatched model".into())
    }
    let mut decision_owners=owners.clone();
    let mut pending_batch=Vec::<(RawFrame,Update,usize)>::new();
    let r1129_batch_target=if r1129_paper_batch32(){32}else{16};
    let mut durable_frames=VecDeque::<Event>::new();
    let mut durable_tx=0_u64;

    let cap_writer=if let Some(path)=capture {
        Some(BufWriter::new(File::create(path)?))
    } else {None};
    let mut cap_writer=cap_writer;

    let r1205_enabled=std::env::var("ASTRA_R1205_INGRESS").ok().as_deref()==Some("YES");
    if r1205_enabled&&(!r1173_compact||!batch_mode||ticker_review){
        return Err("R1205 requires canonical compact depth-only guarded path".into())
    }
    let r1205_authority:spotarb_native_market_core::market_data_ingress_r1205::Authority=Arc::new(std::sync::RwLock::new(HashMap::new()));
    let r1205_store=if r1205_enabled{
        let root=std::env::var_os("ASTRA_R1205_INGRESS_ROOT").map(PathBuf::from).ok_or("R1205 private ingress root required")?;
        let store=Arc::new(std::sync::Mutex::new(spotarb_native_market_core::native_r1173_compact_writer::ingress_store_r1205::Store::open(&root)?));
        if let PaperHistoryR823::PrivateCompact(ref mut w)=history{
            let recovery_store=store.clone();
            w.call(move|writer|{
                recovery_store.lock().map_err(|_|"R1205 store poison")?.bind_canonical(writer.private_root())?;
                writer.recover_ingress_source_only()?;
                spotarb_native_market_core::market_data_ingress_r1205::reconcile(&recovery_store,writer)?;
                writer.require_decisions_drained()?;
                writer.sweep_ingress_receipts(&mut *recovery_store.lock().map_err(|_|"R1205 store poison")?)?;
                Ok(())
            }).await?;
        }else{return Err("R1205 canonical writer required".into())}
        Some(store)
    }else{None};
    // Start the single bounded SQLite custodian only after startup recovery/reconcile.
    let r1205_store_worker=if let Some(ref store)=r1205_store{Some(StoreWorker::start(store.clone())?)}else{None};
    let event_queue_cap=if r1205_enabled{spotarb_native_market_core::ingress_runtime_r1205::R1205_EVENT_QUEUE_CAPACITY}else{R1125_QUEUE_CAP};
    let (tx,mut rx)=mpsc::channel::<Event>(event_queue_cap);
    let r1187_fatal=Arc::new(AtomicBool::new(false));
    let (mut r1241_pipeline_rx,r1241_pipeline_tx,r1241_pipeline_control_tx)=if r1205_enabled{
        let client=match &history{
            PaperHistoryR823::PrivateCompact(worker)=>worker.client()?,
            _=>return Err("R1241 canonical pipeline requires compact Writer".into())
        };
        let source_owners=std::mem::take(&mut owners);
        let store_worker=r1205_store_worker.as_ref().ok_or("R1241 SQLite worker missing")?.clone();
        let (command_tx,control_tx,event_rx)=r1241_pipeline::spawn(
            client,source_owners,store_worker,r1205_authority.clone());
        (Some(event_rx),Some(command_tx),Some(control_tx))
    }else{(None,None,None)};
    let r1187_requested=std::env::var("ASTRA_R1187_BINANCE_INGEST_FAIL_CLOSED")
        .ok().as_deref()==Some("YES");
    let r1187_fail_closed=r1187_requested&&r1173_compact&&batch_mode&&
        std::env::var("ASTRA_R1087_PAPER_PUBLIC_ONLY").ok().as_deref()==Some("YES")&&
        std::env::var("ASTRA_R1136_RELEASE_PAPER").ok().as_deref()==Some("YES");
    if r1187_requested&&!r1187_fail_closed{
        return Err("R1187 opt-in requires isolated compact PUBLIC Release PAPER".into())
    }
    // Weak sender lets us observe queue saturation without keeping it open.
    let r1125_queue_witness=tx.downgrade();
    let mut r1246_archive=if r1246_enabled{open_r1246_archive()?}else{None};
    let counters=Arc::new(Counters::default());
    let duration=Duration::from_secs(secs);
    // R1108: reuse the already validated R167 YAML source WS plan
    // ONLY in opt-in separate candidate PAPER. Installed/default R21
    // keeps its original compiled 32-wide simultaneous WS launch.
    let mut ws_groups=if let Some(ref plan)=source_ws_plan{
        if Some(plan.config_sha256.as_str())!=yaml_sha.as_deref()
           ||plan.selected_bases!=bases.len(){
            return Err("R1108 WS plan/source YAML identity mismatch".into())
        }
        plan.groups.clone()
    }else{
        bases.chunks(SHARD).map(|chunk|chunk.to_vec()).collect::<Vec<_>>()
    };
    if r1246_enabled{ws_groups=vec![bases.clone()];}
    let configured_ws_group_count=ws_groups.len();
    let configured_ws_group_size=source_ws_plan.as_ref().map(|p|p.group_size).unwrap_or(SHARD);
    let r1205_start_interval_ms=source_ws_plan.as_ref().map(|p|p.start_interval_ms).unwrap_or(1_000);
    if r1205_enabled{
        ws_groups=split_source_groups_for_ingress(
            &ws_groups,
            spotarb_native_market_core::ingress_runtime_r1205::R1205_MAX_BASES_PER_SHARD
        ).map_err(|e|format!("R1205 source group split denied: {e}"))?;
        let last_delay_ms=(ws_groups.len().saturating_sub(1) as u64)
            .saturating_mul(r1205_start_interval_ms);
        if last_delay_ms>60_000{return Err("R1205 source group stagger exceeds 60 seconds".into())}
    }
    let estimated_queue_payload_bytes=if r1205_enabled{
        spotarb_native_market_core::ingress_runtime_r1205::estimate_r1205_queue_payload_bytes(ws_groups.len())
    }else{0};
    if r1205_enabled&&estimated_queue_payload_bytes>
        spotarb_native_market_core::ingress_runtime_r1205::R1205_QUEUE_PAYLOAD_BUDGET_BYTES{
        return Err(format!("R1205 bounded frame queues exceed 240 MiB payload budget: {} bytes",
            estimated_queue_payload_bytes).into())
    }
    if r1205_enabled{
        applied_config["effective_r1205_ingress_plan"]=json!({
            "runtime_group_size":spotarb_native_market_core::ingress_runtime_r1205::R1205_MAX_BASES_PER_SHARD,
            "runtime_groups":ws_groups.len(),
            "source_connections":ws_groups.len().saturating_mul(
                spotarb_native_market_core::ingress_runtime_r1205::R1205_SOURCE_CONNECTIONS_PER_SHARD),
            "estimated_queue_payload_bytes":estimated_queue_payload_bytes as u64,
            "queue_payload_budget_bytes":spotarb_native_market_core::ingress_runtime_r1205::R1205_QUEUE_PAYLOAD_BUDGET_BYTES as u64,
            "frame_payload_max_bytes":spotarb_native_market_core::ingress_runtime_r1205::R1205_FRAME_PAYLOAD_MAX_BYTES,
            "installed_paper_applied":false
        });
    }
    if let Some(ref plan)=source_ws_plan{
        applied_config["effective_public_ws_plan"]=json!({
            "origin":"VERSIONED_NATIVE_PAPER_ONLY_YAML",
            "source_sha256":plan.config_sha256,
            "configured_group_size":plan.group_size,
            "configured_groups":configured_ws_group_count,
            "runtime_group_size":if r1205_enabled{spotarb_native_market_core::ingress_runtime_r1205::R1205_MAX_BASES_PER_SHARD}else{configured_ws_group_size},
            "runtime_groups":ws_groups.len(),
            "r1205_connections":if r1205_enabled{Some(ws_groups.len().saturating_mul(spotarb_native_market_core::ingress_runtime_r1205::R1205_SOURCE_CONNECTIONS_PER_SHARD))}else{None},
            "start_interval_ms":r1205_start_interval_ms,
            "installed_paper_applied":false
        });
    }
    let ws_group_count=ws_groups.len();
    let mut tasks=0usize;
    let r1246_ucm_summary_path=if r1246_enabled{Some(PathBuf::from(std::env::var("R1246_UCM_SUMMARY_PATH")?))}else{None};
    let r1246_ucm_copies=std::env::var("R1246_UCM_COPIES").ok().and_then(|v|v.parse::<usize>().ok()).filter(|n|(2..=3).contains(n)).unwrap_or(2);
    for (shard,bs) in ws_groups.into_iter().enumerate(){
        let pause=if r1205_enabled{
            Duration::from_millis(r1205_start_interval_ms.saturating_mul(shard as u64))
        }else{
            source_ws_plan.as_ref().map(|plan|
                plan.delay_before(shard).expect("validated R167 shard index"))
                .unwrap_or(Duration::ZERO)
        };
        if r1246_enabled{
            tasks+=1;
            let central=tx.clone();
            let authority=r1205_authority.clone();
            let fatal=r1187_fatal.clone();
            let summary_path=r1246_ucm_summary_path.clone().ok_or("R1246 UCM summary path missing")?;
            let probe_close=if duration==Duration::from_secs(60)&&r1246_ucm_copies>=3{Some(Duration::from_secs(10))}else{None};
            tokio::spawn(async move{
                let (out_tx,mut out_rx)=mpsc::channel(
                    spotarb_native_market_core::ingress_runtime_r1205::R1205_OUTPUT_QUEUE_CAPACITY);
                let run=tokio::spawn(
                    spotarb_native_market_core::ingress_runtime_r1205::run_r1246_volatile(
                        spotarb_native_market_core::ingress_runtime_r1205::Config{
                            shard,bases:bs,copies:r1246_ucm_copies,duration,controlled_close_after:probe_close,
                            endpoint_variant:0,probe_rotation_after:None
                        },authority,out_tx));
                while let Some(output)=out_rx.recv().await{
                    let event=match output{
                        spotarb_native_market_core::market_data_ingress_r1205::Output::Frame(frame)=>
                            Some(Event::Frame(frame)),
                        spotarb_native_market_core::market_data_ingress_r1205::Output::Book(frame,update)=>
                            Some(Event::NormalizedFrame(frame,update)),
                        spotarb_native_market_core::market_data_ingress_r1205::Output::Reset{venue,base,epoch}=>
                            Some(Event::Reset{venue,bases:vec![base],epoch}),
                        spotarb_native_market_core::market_data_ingress_r1205::Output::Blocked{venue,base,reason}=>{
                            // The UCM has already revoked authority for this scope. Preserve the
                            // diagnostic and continue collecting evidence; a per-symbol HOT deficit
                            // is not a Tokio/MPSC overload or a reason to erase the run summary.
                            Some(Event::TaskError{venue:"ucm",shard,error:format!("scope_blocked:{venue:?}:{base}:{reason}")})
                        },
                        spotarb_native_market_core::market_data_ingress_r1205::Output::Resubscribe{..}=>None,
                    };
                    if let Some(event)=event{
                        if central.send(event).await.is_err(){fatal.store(true,Ordering::SeqCst);run.abort();return}
                    }
                }
                match run.await{
                    Ok(Ok(summary))=>{
                        let result=serde_json::to_vec(&summary)
                            .map_err(|e|format!("R1246 UCM summary serialization: {e}"))
                            .and_then(|bytes|fs::write(&summary_path,bytes)
                                .map_err(|e|format!("R1246 UCM summary write: {e}")));
                        match result{
                            Ok(())=>{let _=central.send(Event::TaskDone{
                                venue:"ucm",shard,reconnects:0,frames:summary.reader_received_total
                            }).await;},
                            Err(error)=>{
                                if central.send(Event::TaskError{venue:"ucm",shard,error}).await.is_err(){
                                    fatal.store(true,Ordering::SeqCst);return
                                }
                                let _=central.send(Event::TaskDone{venue:"ucm",shard,reconnects:0,frames:0}).await;
                            }
                        }
                    },
                    Ok(Err(error))=>{
                        if central.send(Event::TaskError{venue:"ucm",shard,error}).await.is_err(){
                            fatal.store(true,Ordering::SeqCst);return
                        }
                        let _=central.send(Event::TaskDone{venue:"ucm",shard,reconnects:0,frames:0}).await;
                    },
                    Err(error)=>{
                        if central.send(Event::TaskError{venue:"ucm",shard,error:format!("driver join: {error}")}).await.is_err(){
                            fatal.store(true,Ordering::SeqCst);return
                        }
                        let _=central.send(Event::TaskDone{venue:"ucm",shard,reconnects:0,frames:0}).await;
                    }
                }
            });
            continue
        }
        if let (Some(store),Some(store_worker))=(r1205_store.clone(),r1205_store_worker.clone()){
            tasks+=1;
            let central=tx.clone();let authority=r1205_authority.clone();let fatal=r1187_fatal.clone();let source_counters=counters.clone();
            tokio::spawn(async move{
                tokio::time::sleep(pause).await;
                let (output,mut source)=mpsc::channel(spotarb_native_market_core::ingress_runtime_r1205::R1205_OUTPUT_QUEUE_CAPACITY);
                let run=tokio::spawn(spotarb_native_market_core::ingress_runtime_r1205::run_with_store_worker(
                    spotarb_native_market_core::ingress_runtime_r1205::Config{shard,bases:bs,copies:spotarb_native_market_core::ingress_runtime_r1205::R1205_REQUIRED_COPIES,duration,controlled_close_after:None,endpoint_variant:0,probe_rotation_after:None},store,store_worker,authority,output));
                while let Some(event)=source.recv().await{
                    let event=match event{
                        spotarb_native_market_core::market_data_ingress_r1205::Output::Frame(f)=>{match f.venue{Venue::BinanceFutures=>{source_counters.bin_frames.fetch_add(1,Ordering::Relaxed);},Venue::OkxSpot=>{source_counters.okx_frames.fetch_add(1,Ordering::Relaxed);}}Event::Frame(f)},spotarb_native_market_core::market_data_ingress_r1205::Output::Book(f,u)=>{source_counters.okx_frames.fetch_add(1,Ordering::Relaxed);Event::NormalizedFrame(f,u)},
                        spotarb_native_market_core::market_data_ingress_r1205::Output::Reset{venue,base,epoch}=>Event::Reset{venue,bases:vec![base],epoch},
                        spotarb_native_market_core::market_data_ingress_r1205::Output::Resubscribe{..}=>continue,
                        spotarb_native_market_core::market_data_ingress_r1205::Output::Blocked{venue,base,reason}=>{
                            eprintln!("R1205_SCOPE_FAIL_CLOSED {}",json!({"venue":format!("{venue:?}"),"base":base,"reason":reason}));continue
                        }
                    };
                    if central.send(event).await.is_err(){fatal.store(true,Ordering::SeqCst);run.abort();return}
                }
                match run.await{
                    Ok(Ok(summary))=>{
                        let summary_write=std::env::var("R1268_INGRESS_SUMMARY_PATH").ok().map(|path|{
                            serde_json::to_vec(&summary).map_err(|e|e.to_string()).and_then(|bytes|std::fs::write(path,bytes).map_err(|e|e.to_string()))
                        }).unwrap_or(Ok(()));
                        if let Err(error)=summary_write{
                            eprintln!("R1268_INGRESS_SUMMARY_WRITE_ERROR {error}");
                            let _=central.send(Event::TaskError{venue:"ingress",shard,error}).await;
                        }
                        eprintln!("R1205_INGRESS_SUMMARY {}",json!({"counts":summary.counts,"engine":summary.engine,"socket_generations":summary.sockets.len(),"orders":0}));
                    },
                    other=>{fatal.store(true,Ordering::SeqCst);let _=central.send(Event::TaskError{venue:"ingress",shard,error:format!("R1205 driver:{other:?}")}).await;}
                }
                let _=central.send(Event::TaskDone{venue:"ingress",shard,reconnects:0,frames:0}).await;
            });
            continue
        }
        tasks+=if ticker_review{3}else{2};
        let bin=bs.clone();
        let bin_tx=tx.clone();let bin_counters=counters.clone();
        let bin_fatal=r1187_fatal.clone();
        tokio::spawn(async move{
            tokio::time::sleep(pause).await;
            binance_task(shard,bin,duration,force_reconnect,
                ticker_review,bin_tx,bin_counters,r1187_fail_closed,bin_fatal).await
        });
        if ticker_review{
            let ticker_tx=tx.clone();let ticker_bs=bs.clone();
            tokio::spawn(async move{
                tokio::time::sleep(pause).await;
                binance_ticker_task(shard,ticker_bs,duration,
                    force_reconnect,ticker_tx).await
            });
        }
        let okx_tx=tx.clone();let okx_counters=counters.clone();
        tokio::spawn(async move{
            tokio::time::sleep(pause).await;
            okx_task(shard,bs,duration,force_reconnect,
                ticker_review,okx_tx,okx_counters).await
        });
    }
    drop(tx);

    let started_wall_ms=wall_ms();
    let started_monotonic_ns=monotonic_ns();
    if let Some(archive)=r1246_archive.as_ref(){
        archive.push("paper_session_open",json!({
            "route_contract":"OKX Spot BUY -> Binance Futures SHORT -> Binance Futures COVER -> OKX Spot SELL",
            "started_wall_ms":started_wall_ms,"started_monotonic_ns":started_monotonic_ns,
            "universe_bases":expected,"market_data":"live_public_exchange_streams",
            "storage_mode":"volatile_ram_ingress_plus_dual_async_archive",
            "real_orders":false,"private_order_path_linked":false
        })).map_err(|e|format!("R1246 PAPER session-open archive denied: {e}"))?;
    }
    let started=Instant::now();let cpu0=cpu_s();let rss0=rss_kib();let mut rss_peak=rss0;
    // R1150: optional 25ms scheduler wakeup-only passive probe.
    r1150_start();
    let mut done=0usize;let mut resets=0u64;let mut task_errors=Vec::new();
    let mut invalidated_coverage_binance=0u64;
    let mut invalidated_coverage_okx=0u64;
    let mut r154_boot=PublicColdBootR130::default();
    let mut r154_okx=HashMap::<String,R154TickerSample>::new();
    let mut r154_binance=HashMap::<String,R154TickerSample>::new();
    let mut r154_ready=HashSet::<String>::new();
    let mut r154_seen=0_u64;let mut r154_denied=0_u64;
    let mut parse_errors=0u64;let mut ingest_errors=0u64;let mut decision_errors=0u64;
    // R1125: passive queue/age telemetry for ACTUAL public PAPER only.
    let mut r1125_queue_peak=0usize;
    let mut r1125_last_bin_lag=0i64;let mut r1125_last_okx_lag=0i64;
    let mut r1125_peak_bin_lag=0i64;let mut r1125_peak_okx_lag=0i64;
    let mut r1126_last_queue_delay_ms=0u64;
    let mut r1126_peak_queue_delay_ms=0u64;
    let mut decision_error_samples=Vec::<serde_json::Value>::new();
    let r1247_trace_enabled=std::env::var("R1247_TRACE").ok().as_deref()==Some("1");
    let mut r1247_decision_trace=Vec::<Value>::new();let mut r1247_decision_trace_evictions=0u64;
    let mut accepted=0u64;let mut decisions=0u64;let mut opportunities=0u64;
    let mut safety_denies=0u64;let mut rehearsal_plans=0u64;
    let mut reasons:HashMap<String,u64>=HashMap::new();let mut seen_b=HashSet::new();let mut seen_o=HashSet::new();
    let mut opportunity_samples=Vec::new();
    let mut r1246_archived_candidates=0_u64;
    let mut r1246_archived_transitions=0_u64;
    let mut r1246_enqueue_samples=VecDeque::<u64>::new();
    let mut r1246_decision_samples=VecDeque::<u64>::new();
    let mut ingest_error_samples:Vec<Value>=Vec::new();
    let mut portfolio=PaperPortfolio::new(wall_ms()).map_err(|e|format!("paper-init: {e:?}"))?;
    // Apply REAL YAML-derived temporal policy to the research in-memory
    // PAPER model, not just to a sidecar diagnosis or DecisionCore.
    portfolio.apply_public_paper_temporal_policy_review(
        temporal.min_lifetime_ns,temporal.exit_spread_bps,temporal.reentry_cooldown_ns,
        temporal.net_target_bps)
      .map_err(|e|format!("paper temporal application denied: {e:?}"))?;
    // For supervised short public-data sessions, count *all* modeled entries
    // already started during this EKB calendar day. An unknown/crashed
    // predecessor must pass 500 (fail-closed until next day).
    let previous_entries=std::env::var("PAPER_PREVIOUS_DAY_ENTRIES").ok()
        .map(|raw|raw.parse::<u64>()).transpose()
        .map_err(|_|"invalid supervised paper daily ledger")?.unwrap_or(0);
    if previous_entries>spotarb_native_market_core::native_paper_portfolio_review::MAX_DAILY_PAPER as u64{
        return Err("supervised paper prior daily count exceeds cap".into())
    }
    portfolio.stats.entries=previous_entries;
    if r1173_compact&&previous_entries!=0{
        return Err("R1175 compact model requires independently fresh day ledger".into())
    }
    let checkpoint_path=std::env::var("PAPER_CONTINUITY_PATH").ok().map(PathBuf::from);
    let stop_path=std::env::var("PAPER_GRACEFUL_STOP_PATH").ok().map(PathBuf::from);
    let stop_session=std::env::var("PAPER_GRACEFUL_SESSION_ID").ok();
    if stop_path.is_some()!=stop_session.is_some(){
        return Err("PAPER graceful stop path and identity must be paired".into())
    }
    let restoration_requested=std::env::var("PAPER_PLANNED_RESTORE").ok().as_deref()==Some("1");
    if restoration_requested&&checkpoint_path.is_none(){
        return Err("planned virtual handoff requires durable checkpoint path".into())
    }
    let mut checkpoint_revision=0_u64;
    let mut carried_positions=0_usize;
    if restoration_requested{
        let previous=load_planned_handoff(
           checkpoint_path.as_ref().unwrap(),monotonic_ns(),previous_entries)
           .map_err(|e|format!("virtual PAPER planned handoff denied: {e:?}"))?;
        checkpoint_revision=previous.previous_revision;
        carried_positions=previous.carried_open_positions;
        portfolio=previous.portfolio;
    }
    // A recovered virtual owner retains its original persisted temporal
    // policy. A new YAML revision must not silently rewrite the old exit.
    portfolio.verify_public_paper_temporal_policy_review(
        temporal.min_lifetime_ns,temporal.exit_spread_bps,temporal.reentry_cooldown_ns,
        temporal.net_target_bps)
      .map_err(|e|format!("paper temporal restore mismatch: {e:?}"))?;
    applied_config["effective_paper_temporal_policy"]=json!({
      "min_lifetime_ns":portfolio.min_lifetime_ns,
      "exit_spread_bps":portfolio.exit_spread_bps,
      "net_target_bps":portfolio.net_target_bps,
      "reentry_cooldown_ns":portfolio.reentry_cooldown_ns,
      "matches_source_and_persisted_portfolio":true,
      "source":config_source,
      "deployed_binary_attested":false,
      "may_trade_real":false
    });
    // This initial unsealed checkpoint makes abnormal exits distinguishable
    // from a fully completed virtual-paper session.
    if let Some(ref path)=checkpoint_path{
        checkpoint_revision=checkpoint_revision.checked_add(1)
           .ok_or("paper checkpoint revision overflow")?;
        save_checkpoint(path,&portfolio,checkpoint_revision,monotonic_ns(),false)
          .map_err(|e|format!("paper initial durable checkpoint: {e:?}"))?;
    }
    // R1175: durable fresh modeled baseline in the SAME compact
    // DB1->DB2 ledger, before any position or decision transition.
    if let PaperHistoryR823::PrivateCompact(ref mut w)=history{
        let baseline_portfolio=portfolio.clone();
        let seq=w.call(move|writer|writer.commit_modeled_portfolio(None,&baseline_portfolio,"BOOT",R1173Inject::None)).await
           .map_err(|e|format!("R1175 model baseline denied: {e}"))?;
        if seq!=durable_tx+1{
            return Err("R1175 baseline source cursor discontinuity".into())
        }
        durable_tx=seq;
    }
    // A separate, disk-fsynced provenance stream of modeled transitions.
    // This MUST NEVER be fed to Native PaperOwner as authenticated fills.
    let model_journal_path=std::env::var("PAPER_VIRTUAL_FOUR_LEG_JOURNAL_PATH")
        .ok().map(PathBuf::from);
    let model_session_id=std::env::var("PAPER_VIRTUAL_FOUR_LEG_SESSION_ID").ok();
    if model_journal_path.is_some()!=model_session_id.is_some(){
        return Err("modeled audit journal needs exact session identity".into())
    }
    // R1116: source-owned journal -> R37/R40 durable local queue.
    // NO Telegram transport. Strictly opt-in PRIVATE R1088 and exact
    // versioned PAPER-only candidate, not installed/default PAPER.
    let r1116_root=std::env::var("ASTRA_R1116_PRIVATE_NOTICE_ROOT")
       .ok().map(PathBuf::from);
    let r1116_cfg=if let Some(ref root)=r1116_root{
       if !batch_mode||std::env::var("PAPER_NATIVE_RUST_CONFIG_REVIEW")
           .ok().as_deref()!=Some("2")||
          std::env::var("ASTRA_R1087_PAPER_PUBLIC_ONLY")
           .ok().as_deref()!=Some("YES")||
          model_journal_path.as_ref()!=Some(&root.join("model.jsonl"))||
          model_session_id.is_none(){
          return Err("R1116 isolated private model/queue config denied".into())
       }
       let cfg_file=PathBuf::from(env!("CARGO_MANIFEST_DIR"))
          .join("config/native_trading_paper_only_r1076.yaml");
       let candidate=NativeTradingConfig::load(&cfg_file)
         .map_err(|e|format!("R1116 source YAML denied: {e:?}"))?;
       if candidate.sha256!="38a71a9c457c00c686c6f7c4c808d05cfe37e43cc760fc8591eac960d101ec4a"||
          candidate.policy.real_execution_authorized{
          return Err("R1116 exact no-REAL source fingerprint denied".into())
       }
       Some(candidate)
    }else{None};
    let mut model_journal=match model_journal_path {
       Some(ref path)=>Some(ModeledFourLegJournal::start(path,
             model_session_id.as_deref().unwrap(),&portfolio,carried_positions)
             .map_err(|e|format!("initial modeled four-leg provenance: {e:?}"))?),
       None=>None,
    };
    if let (Some(ref root),Some(ref cfg),Some(ref path))=
         (&r1116_root,&r1116_cfg,&model_journal_path){
       let (seq,created)=r1116_commit(root,path,cfg,u64::try_from(wall_ms())
                                 .map_err(|_|"R1116 wall clock invalid")?,false)
         .map_err(|e|format!("R1116 initial no-replay durable notice baseline: {e}"))?;
       if seq!=1||created!=0{
          return Err("R1116 unsafe initial modeled queue baseline".into())
       }
    }
    let status_path=std::env::var("PAPER_STATUS_JSON").ok().map(PathBuf::from);
    let mut r1246_telemetry=if r1246_enabled{
        let path=PathBuf::from(std::env::var("R1246_TELEMETRY_PATH")?);
        if let Some(parent)=path.parent(){fs::create_dir_all(parent)?;}
        Some(BufWriter::new(File::create(path)?))
    }else{None};
    let mut last_status=Instant::now()-Duration::from_secs(60);
    let mut stop_tick=tokio::time::interval(Duration::from_millis(250));
    stop_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut r1246_tick=tokio::time::interval(Duration::from_secs(60));
    r1246_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut batch_tick=tokio::time::interval(Duration::from_millis(75));
    batch_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut graceful_early_stop=false;
    let mut r1241_stop_drain_requested=false;
    let mut r1241_next_reset_id=0_u64;
    let mut r1241_active_batch:Option<(u64,usize,usize,Vec<r1241_pipeline::DecisionSeal>)>=None;
    let mut r1241_pending_custody_commit:Option<r1241_pipeline::DeferredCustodyCommit>=None;
    let mut r1241_finalize_pending:Option<(u64,usize,usize)>=None;
    let mut r1241_shutdown_requested=false;
    let mut r1241_pipeline_stopped=false;
    let mut r1241_pipeline_profile=Value::Null;
    let mut raw_event_channel_closed=false;

    loop {
        if r1187_fatal.load(Ordering::SeqCst){
            return Err("R1187 Binance MPSC overload; source and decision custody quarantined".into())
        }
        // Poll independently of incoming quote frames. At a valid request,
        // stop accepting further frames, finish the current state update,
        // seal the exact virtual PAPER checkpoint and write a normal final.
        let next=if let Some(done)=durable_frames.pop_front(){
            Some(done)
        }else{
            tokio::select!{
                biased;
                _=stop_tick.tick(), if stop_path.is_some()&&!r1241_stop_drain_requested=>{
                    if graceful_stop_requested(stop_path.as_ref().unwrap(),
                           stop_session.as_deref().unwrap())?{
                        if batch_mode && !pending_batch.is_empty(){
                           r1088_flush_queued_frames(&mut pending_batch,&mut owners,
                              &mut history,&mut durable_frames,&mut durable_tx).await?;
                           continue
                        }
                        if r1205_enabled {
                            // R1241 sources own fixed-duration tasks; stop intake only after they finish,
                            // then drain the canonical sequencer before producing a final record.
                            graceful_early_stop=true;
                            r1241_stop_drain_requested=true;
                            continue
                        }
                        graceful_early_stop=true;
                        break
                    }
                    continue
                }
                _=batch_tick.tick(), if batch_mode&&!r1205_enabled=>{
                    if !pending_batch.is_empty(){
                        r1088_flush_queued_frames(&mut pending_batch,&mut owners,
                            &mut history,&mut durable_frames,&mut durable_tx).await?;
                    }
                    continue
                }
                _=r1246_tick.tick(), if r1246_enabled=>Some(Event::R1246TelemetryTick),
                canonical=async{
                    match r1241_pipeline_rx.as_mut(){
                        Some(events)=>events.recv().await,
                        None=>std::future::pending::<Option<r1241_pipeline::Event>>().await,
                    }
                }, if r1205_enabled => canonical.map(Event::Canonical),
                item=rx.recv(), if !raw_event_channel_closed => {
                    match item {
                        Some(value)=>Some(value),
                        None=>{
                            raw_event_channel_closed=true;
                            if r1205_enabled {continue;} else {None}
                        }
                    }
                },
            }
        };
        if r1187_fatal.load(Ordering::SeqCst){
            return Err("R1187 Binance MPSC overload before next decision; fail closed".into())
        }
        let Some(event)=next else{
            if r1205_enabled&&!r1241_pipeline_stopped{
                return Err("R1241 canonical event channel closed before durable drain".into())
            }
            if batch_mode && !pending_batch.is_empty(){
                r1088_flush_queued_frames(&mut pending_batch,&mut owners,
                    &mut history,&mut durable_frames,&mut durable_tx).await?;
                continue
            }
            break
        };
        rss_peak=rss_peak.max(rss_kib());
        r1125_queue_peak=r1125_queue_peak.max(
            r1125_queue_witness.upgrade().map_or(0,
                |sender|event_queue_cap.saturating_sub(sender.capacity())));
        let (event,normalized_update,guarded_meta,canonical_reset)=match event{
            Event::NormalizedFrame(f,u)=>(Event::Frame(f),Some(u),None,None),
            Event::R1241BatchDurableFrame{batch_id,batch_index,batch_count,frame,update}=>
                (Event::R1177DurableFrame(frame),Some(update),Some((batch_id,batch_index,batch_count)),None),
            Event::Canonical(r1241_pipeline::Event::ResetReady{request_id,seq,venue,bases,epoch})=>{
                if seq!=durable_tx+1{return Err("R1241 reset transaction cursor gap".into())}
                durable_tx=seq;
                (Event::Reset{venue,bases,epoch},None,None,Some(request_id))
            }
            other=>(other,None,None,None)
        };
        let guarded_decision=matches!(&event,Event::R1177DurableFrame(_));
        match event {
            Event::NormalizedFrame(_,_)=>unreachable!("normalized above"),
            Event::Frame(frame) if batch_mode=>{
                let update=match normalized_update{Some(v)=>v,None=>match spotarb_native_market_core::parse(&frame){Ok(v)=>v,Err(_)=>{parse_errors+=1;continue}}};
                if r1205_enabled{
                    if !spotarb_native_market_core::market_data_ingress_r1205::authorization_at_normalized(
                        &r1205_authority,&frame,&update,
                        spotarb_native_market_core::ingress_runtime_r1205::mono_ns(),wall_ms()).is_some(){
                        r1241_pipeline::submit(
                            r1241_pipeline_tx.as_ref().ok_or("R1241 pipeline missing")?,
                            r1241_pipeline::Command::Quarantine{
                                frame,reason:"selection_fenced_before_owner".into()
                            })?;
                        continue
                    }
                }else if r1205_store.is_some(){
                    if !spotarb_native_market_core::market_data_ingress_r1205::authorization_at_normalized(&r1205_authority,&frame,&update,spotarb_native_market_core::ingress_runtime_r1205::mono_ns(),wall_ms()).is_some(){
                        let writer=r1205_store_worker.as_ref().ok_or("R1240 SQLite worker missing")?.clone();
                        writer.call_async(move|store,_|store.finish(&frame,"quarantined","selection_fenced_before_owner",None)).await?;
                        continue
                    }
                }
                if let Some(exchange_ts)=update.book.exchange_ts_ms{
                    let age=frame.received_wall_ms.saturating_sub(exchange_ts);
                    match frame.venue{
                        Venue::BinanceFutures=>{
                            r1125_last_bin_lag=age;
                            r1125_peak_bin_lag=r1125_peak_bin_lag.max(age);
                        }
                        Venue::OkxSpot=>{
                            r1125_last_okx_lag=age;
                            r1125_peak_okx_lag=r1125_peak_okx_lag.max(age);
                        }
                    }
                }
                let idx=owner_index(&update.base,OWNERS);
                if r1205_enabled{
                    r1241_pipeline::submit(
                        r1241_pipeline_tx.as_ref().ok_or("R1241 pipeline missing")?,
                        r1241_pipeline::Command::Frame(r1241_pipeline::CanonicalFrame{
                            frame,update,owner_index:idx
                        }))?;
                    continue
                }
                pending_batch.push((frame,update,idx));
                if pending_batch.len()>=r1129_batch_target{
                   r1088_flush_queued_frames(&mut pending_batch,&mut owners,
                     &mut history,&mut durable_frames,&mut durable_tx).await?;
                }
                continue
            }
            Event::Frame(mut frame)|Event::R1088DurableFrame(mut frame)|
                Event::R1177DurableFrame(mut frame)=>{
                let r1180_frame_started=Instant::now();
                let update=match normalized_update{Some(v)=>v,None=>match spotarb_native_market_core::parse(&frame){Ok(v)=>v,Err(_)=>{
                    if guarded_decision {return Err("R1177 committed source reparsing denied".into())}
                    parse_errors+=1;continue
                }}};
                let base=update.base.clone();let idx=owner_index(&base,OWNERS);
                let r1246_source_sequence=update.book.sequence;
                let r1246_source_changed_ns=update.book.changed_ns;
                let r1246_source_exchange_ts_ms=update.book.exchange_ts_ms;
                match frame.venue {Venue::BinanceFutures=>{seen_b.insert(base.clone());},Venue::OkxSpot=>{seen_o.insert(base.clone());}}
                let ingest_result=if batch_mode{
                    decision_owners[idx].ingest_parsed(&frame,update,
                        &mut spotarb_native_market_core::history::PreparedSignal)
                }else{
                    owners[idx].ingest_parsed(&frame,update,&mut history)
                };
                if let Err(e)=ingest_result{
                    if guarded_decision{
                        return Err(format!("R1177 committed frame ingest denied: {e:?}").into())
                    }
                    if history.is_private()&&e==CoreError::HistoryPressure{
                        return Err("R819/R1088 private original DB1 history not durable; PAPER frame admission halted".into())
                    }
                    ingest_errors+=1;
                    if ingest_error_samples.len()<20{
                        ingest_error_samples.push(json!({"base":base,"venue":format!("{:?}",frame.venue),
                          "epoch":frame.epoch,"received_ns":frame.received_ns,"stage":"ingest_parsed",
                          "error":format!("{e:?}")}));
                    }
                    continue
                }
                let view_owner=if batch_mode{&decision_owners[idx]}else{&owners[idx]};
                let book=match view_owner.snapshot_venue(&base,frame.venue){Some(v)=>v,None=>{
                    if guarded_decision{return Err("R1177 committed frame missing current book".into())}
                    ingest_errors+=1;
                    if ingest_error_samples.len()<20{
                        ingest_error_samples.push(json!({"base":base,"venue":format!("{:?}",frame.venue),
                          "epoch":frame.epoch,"received_ns":frame.received_ns,"stage":"missing_book"}));
                    }
                    continue
                }};
                let exchange=match frame.venue{Venue::OkxSpot=>"okx_spot",Venue::BinanceFutures=>"binance_futures"};
                // R1122: only AFTER canonical R1088 DB1 source commit
                // and Owner::ingest_parsed validated the entire original
                // OKX books5 snapshot (R1121 raw+time+epoch guard), permit
                // its equivalent source-verified sequence semantics in
                // DecisionCore. No Binance or REAL source exception.
                // R1177 private candidate also uses the existing
                // R1121 exact OKX snapshot rule, only after Owner verified
                // the committed source. This does NOT relax generic order.
                let native_books5_verified=(r1119_operational||r1173_compact)&&
                    batch_mode&&frame.venue==Venue::OkxSpot&&
                    (r1205_enabled||std::env::var("ASTRA_R1121_BOOKS5_SNAPSHOT_RESET")
                      .ok().as_deref()==Some("YES"));
                // R1126: PAPER must decide at PROCESSING clock, not stale
                // WS-reception clock when bounded intake is backlogged.
                // Book provenance and DB1 records retain original timestamps.
                let decision_now_ns=monotonic_ns();
                let paper_trace_start=decision_now_ns;
                let decision_now_wall_ms=wall_ms();
                r1126_last_queue_delay_ms=decision_now_ns
                    .saturating_sub(frame.received_ns)/1_000_000;
                r1126_peak_queue_delay_ms=r1126_peak_queue_delay_ms
                    .max(r1126_last_queue_delay_ms);
                let rows=match cores[idx].update_at_native_verified_books5(
                   &base,exchange,book,decision_now_ns,
                   decision_now_wall_ms,native_books5_verified){
                    Ok(v)=>v,Err(e)=>{
                        if guarded_decision{return Err(format!("R1177 decision core error after source ACK: {e:?}").into())}
                        decision_errors+=1;
                        if decision_error_samples.len()<20{
                           decision_error_samples.push(json!({
                             "base":base,"venue":format!("{:?}",frame.venue),
                             "epoch":frame.epoch,"received_ns":frame.received_ns,
                             "source":"R1088_DB1_COMMITTED_NATIVE_PAPER",
                             "r1122_validated_books5":native_books5_verified,
                             "reason":format!("{e:?}")}));
                        }
                        continue
                    }
                };
                let t6_decision_end_ns=monotonic_ns();
                let decision_trace_id=frame.source.as_ref().map(|s|format!("decision:{}:{}",s.frame_trace_id,decision_now_ns));
                if let Some(t)=frame.source.as_mut().and_then(|s|s.r1279_timing.as_mut()){t.paper_decision_start_ns=Some(paper_trace_start);t.paper_decision_end_ns=Some(t6_decision_end_ns);t.decision_trace_id=decision_trace_id;}
                accepted+=1;
                if let Some(w)=cap_writer.as_mut(){serde_json::to_writer(&mut *w,&frame)?;w.write_all(b"\n")?}
                let mut r1247_archive_enqueue_ids=Vec::<String>::new();
                for (decision_index,d) in rows.iter().enumerate() {
                    decisions+=1;*reasons.entry(d.reason.clone()).or_default()+=1;
                    if r1246_archive_candidate(d) {
                        if let Some(archive)=r1246_archive.as_ref(){
                            let started_push=Instant::now();
                            let event_id=archive.push("opportunity_evaluated",json!({
                                "route_contract":"OKX Spot BUY + Binance Futures SHORT",
                                "outcome":if decision_is_opportunity(d){"ACCEPTED_FOR_PAPER"}else{"REJECTED"},
                                "rejection_reason":if decision_is_opportunity(d){Value::Null}else{json!(d.reason)},
                                "source":{"venue":format!("{:?}",frame.venue),"instrument":base,
                                    "epoch":frame.epoch,"sequence":r1246_source_sequence,
                                    "book_changed_ns":r1246_source_changed_ns,
                                    "exchange_ts_ms":r1246_source_exchange_ts_ms,
                                    "received_ns":frame.received_ns,
                                    "received_wall_ms":frame.received_wall_ms},
                                "decision_lineage":{"index":decision_index,
                                    "metadata_generation":meta.generation,
                                    "decision_time_ns":decision_now_ns,
                                    "decision_time_wall_ms":decision_now_wall_ms},
                                "decision":d
                            })).map_err(|e|format!("R1246 opportunity archive fail-closed: {e}"))?;
                            let ns=started_push.elapsed().as_nanos().min(u64::MAX as u128) as u64;
                            r1247_archive_enqueue_ids.push(event_id.clone());
                            r1246_enqueue_samples.push_back(ns);
                            if r1246_enqueue_samples.len()>10_000{r1246_enqueue_samples.pop_front();}
                            r1246_archived_candidates+=1;
                            if event_id.is_empty(){return Err("R1246 archive returned empty event ID".into())}
                        }
                    }
                    if decision_is_opportunity(&d) {
                        opportunities+=1;
                        if !safety.allowed {safety_denies+=1}
                        let mut c=Coordinator::new(meta.generation);
                        c.prepare_spot(meta.generation).map_err(|e|format!("rehearsal:{e:?}"))?;
                        if c.state==CoordinatorState::SpotPending {rehearsal_plans+=1}
                        if opportunity_samples.len()<20 {
                            opportunity_samples.push(json!({"base":d.base,"net_bps":d.estimated_net_bps,
                                "capacity_usdt":d.max_profitable_capacity_usdt,"risk_result":d.risk_result,
                                "safety_verdict":safety.verdict}));
                        }
                    }
                }
                let candidate=rows.iter().find(|d|decision_is_opportunity(d));
                let decision_delay_ns=decision_now_ns.saturating_sub(frame.received_ns);
                r1246_decision_samples.push_back(decision_delay_ns);
                if r1246_decision_samples.len()>10_000{r1246_decision_samples.pop_front();}
                let (spot,fut)=view_owner.snapshot_pair(&base);
                let route=meta.routes.get(&base).and_then(|r|r.first());
                let model_before=if (model_journal.is_some()||r1173_compact)&&
                      (candidate.is_some()||portfolio.positions.contains_key(&base)){
                    Some(portfolio.clone())
                }else{None};
                let previous_lifecycle=(
                    portfolio.stats.entries,portfolio.stats.futures_hedges,
                    portfolio.stats.futures_covers,portfolio.stats.closed,
                    portfolio.open_count());
                // R1204: re-read both clocks immediately before modeled action.
                let portfolio_now_ns=monotonic_ns();
                let portfolio_now_wall_ms=wall_ms();
                if !r1187_fatal.load(Ordering::SeqCst) &&
                    (!r1205_enabled||spotarb_native_market_core::market_data_ingress_r1205::owner_pair_authorized(
                        &r1205_authority,view_owner,&base,portfolio_now_ns,portfolio_now_wall_ms)) &&
                    intake_r1204::decision_pair_fresh(spot.as_ref(),fut.as_ref(),
                    portfolio_now_ns,portfolio_now_wall_ms) {
                    portfolio.on_frame(&base,frame.venue,portfolio_now_ns,portfolio_now_wall_ms,
                        spot.as_ref(),fut.as_ref(),candidate,route)
                        .map_err(|e|format!("paper fail-closed: {e:?}"))?;
                }else{
                    portfolio.stats.denied_stale+=1;
                }
                let lifecycle=(
                    portfolio.stats.entries,portfolio.stats.futures_hedges,
                    portfolio.stats.futures_covers,portfolio.stats.closed,
                    portfolio.open_count());
                let result_proof=if guarded_decision {Some(json!({
                    "schema":"SPOTARB/R1178/DECISION-RESULT/V1",
                    "source_venue":format!("{:?}",frame.venue),
                    "source_epoch":frame.epoch,
                    "source_received_ns":frame.received_ns,
                    "source_received_wall_ms":frame.received_wall_ms,
                    "decision_time_ns":decision_now_ns,
                    "decision_time_wall_ms":decision_now_wall_ms,
                    "decision_rows":&rows,
                    "modeled_after":portfolio.snapshot()
                }))}else{None};
                let result_hash=if let Some(ref proof)=result_proof{
                    let bytes=serde_json::to_vec(proof)
                        .map_err(|e|format!("R1178 result witness serialization: {e}"))?;
                    Some(openssl::sha::sha256(&bytes).iter()
                        .map(|b|format!("{b:02x}")).collect::<String>())
                }else{None};
                if lifecycle!=previous_lifecycle {
                    // R1175: publish the complete virtual-portfolio
                    // predecessor/successor and immutable witness to
                    // DB1 then mirror DB2 before any sidecar checkpoint.
                    if r1205_enabled && (!guarded_decision || !matches!(&history,PaperHistoryR823::PrivateCompact(_))) {
                        return Err("R1241 modeled transition lacks guarded compact custody".into())
                    }
                    if r1205_enabled && r1241_pending_custody_commit.is_some() {
                        return Err("R1241 duplicate deferred modeled transition".into())
                    }
                    let prior=model_before.as_ref()
                       .ok_or("R1175 missing modeled predecessor")?;
                    // The significant model leg carries its exact source
                    // and DecisionCore context in the immutable DB audit.
                    let source_venue=match frame.venue{
                        Venue::OkxSpot=>"okx_public_spot",
                        Venue::BinanceFutures=>"binance_public_futures"};
                    let cause=json!({
                        "schema":"SPOTARB/R1184/MODELED-DECISION-CAUSE/V1",
                        "base":base,"source_venue":source_venue,
                        "source_epoch":frame.epoch,
                        "source_received_ns":frame.received_ns,
                        "source_received_wall_ms":frame.received_wall_ms,
                        "decision_time_ns":decision_now_ns,
                        "decision_time_wall_ms":decision_now_wall_ms,
                        "decision_config_sha256":applied_config["decision_config_sha256"],
                        "decision_config_source":applied_config["source"],
                        "effective_decision_config":applied_config["effective_decision_config"],
                        "paper_model_assumptions":applied_config["paper_model_assumptions"],
                        "route":route,
                        "metadata_generation":meta.generation,
                        "decision_rows":&rows,
                        "spot_book":&spot,"futures_book":&fut,
                        "modeled_before":prior.snapshot(),
                        "result_proof":&result_proof
                    });
                    let prior=prior.clone();let after=portfolio.clone();
                    let transition_base=base.clone();let transition_frame=frame.clone();let transition_hash=result_hash.clone();
                    if r1205_enabled {
                        r1241_pending_custody_commit=Some(Box::new(move |writer|{
                            writer.commit_modeled_portfolio_for_decision(
                                &prior,&after,&transition_base,&transition_frame,&cause,
                                transition_hash.as_deref().ok_or("missing guarded result hash")?,R1173Inject::None)
                        }));
                    } else if let PaperHistoryR823::PrivateCompact(ref mut w)=history {
                        let seq=w.call(move|writer|if guarded_decision{
                            writer.commit_modeled_portfolio_for_decision(
                                &prior,&after,&transition_base,&transition_frame,&cause,
                                transition_hash.as_deref().ok_or("missing guarded result hash")?,R1173Inject::None)
                        }else{
                            writer.commit_modeled_portfolio(Some(&prior),&after,&transition_base,R1173Inject::None)
                        }).await.map_err(|e|format!("R1184 modeled transition denied: {e}"))?;
                        if seq!=durable_tx+1{
                            return Err("R1175 transition ledger cursor discontinuity".into())
                        }
                        durable_tx=seq;
                    }
                    if let Some(ref path)=checkpoint_path {
                        checkpoint_revision=checkpoint_revision.checked_add(1)
                            .ok_or("paper checkpoint revision overflow")?;
                        save_checkpoint(path,&portfolio,checkpoint_revision,monotonic_ns(),false)
                            .map_err(|e|format!("paper lifecycle checkpoint failed: {e:?}"))?;
                    }
                    if let Some(ref mut modeled)=model_journal{
                        let prior=model_before.as_ref()
                            .ok_or("missing model-before on a lifecycle transition")?;
                        modeled.record_change(&base,prior,&portfolio)
                            .map_err(|e|format!("modeled four-leg write denied: {e:?}"))?;
                        // R1116: commit R37/R40 cursor AFTER fsynced model
                        // stage; no Telegram send. Never acknowledge a
                        // lifecycle while private queue publication failed.
                        if let (Some(ref root),Some(ref cfg),Some(ref path))=
                              (&r1116_root,&r1116_cfg,&model_journal_path){
                            r1116_commit(root,path,cfg,u64::try_from(wall_ms())
                                 .map_err(|_|"R1116 wall clock invalid")?,false)
                               .map_err(|e|format!("R1116 modeled event queue fail-closed: {e}"))?;
                        }
                    }
                    if let Some(archive)=r1246_archive.as_ref(){
                        let prior=model_before.as_ref().ok_or("R1246 transition predecessor missing")?;
                        let paper_event_id=archive.push("paper_transition",json!({
                            "route_contract":"OKX Spot BUY -> Binance Futures SHORT -> Binance Futures COVER -> OKX Spot SELL",
                            "base":base,"source_venue":source_venue,
                            "source_epoch":frame.epoch,"source_sequence":r1246_source_sequence,
                            "source_received_ns":frame.received_ns,
                            "source_received_wall_ms":frame.received_wall_ms,
                            "decision_time_ns":decision_now_ns,
                            "metadata_generation":meta.generation,
                            "transition_delta":{"spot_entries":portfolio.stats.entries.saturating_sub(prior.stats.entries),
                                "futures_hedges":portfolio.stats.futures_hedges.saturating_sub(prior.stats.futures_hedges),
                                "futures_covers":portfolio.stats.futures_covers.saturating_sub(prior.stats.futures_covers),
                                "spot_closes":portfolio.stats.closed.saturating_sub(prior.stats.closed)},
                            "modeled_before":prior.snapshot(),"modeled_after":portfolio.snapshot(),
                            "model_journal_sequence":model_journal.as_ref().map(|m|m.sequence()),
                            "durable_paper_state":{"model_journal_fsynced":model_journal.is_some(),
                                "checkpoint_written":checkpoint_path.is_some()},
                            "real_orders":false
                        })).map_err(|e|format!("R1246 PAPER transition archive fail-closed: {e}"))?;
                        r1247_archive_enqueue_ids.push(paper_event_id.clone());
                        r1246_archived_transitions+=1;
                        if paper_event_id.is_empty(){return Err("R1246 archive returned empty transition ID".into())}
                    }
                }
                // R1178: bind the guarded source envelope to its exact
                // DecisionCore result and modeled PAPER after-state. These
                // hashes are witnesses, NOT exchange-signed fills or a
                // replayable copy of every market frame.
                if guarded_decision {
                    let proof_hash=result_hash.as_deref()
                        .ok_or("missing guarded result proof")?;
                    if r1205_enabled{
                        let (batch_id,batch_index,batch_count)=guarded_meta
                            .ok_or("R1241 durable frame missing batch identity")?;
                        let (active_id,expected,next,seals)=r1241_active_batch.as_mut()
                            .ok_or("R1241 decision arrived without active source batch")?;
                        if *active_id!=batch_id||*expected!=batch_count||*next!=batch_index{
                            return Err("R1241 decision batch order mismatch".into())
                        }
                        seals.push(r1241_pipeline::DecisionSeal{
                            frame:frame.clone(),result_hash:proof_hash.to_string(),
                            pre_decision_commit:r1241_pending_custody_commit.take()
                        });
                        *next+=1;
                        if *next==*expected{
                            let (final_id,final_count,_,decisions)=r1241_active_batch.take()
                                .ok_or("R1241 active batch disappeared")?;
                            if final_id!=batch_id||decisions.len()!=final_count{
                                return Err("R1241 incomplete decision batch".into())
                            }
                            let transition_count=decisions.iter()
                                .filter(|seal|seal.pre_decision_commit.is_some()).count();
                            r1241_pipeline_control_tx.as_ref().ok_or("R1241 control channel missing")?
                                .try_send(r1241_pipeline::Control::FinalizeBatch{
                                    batch_id,decisions
                                }).map_err(|_|"R1241 decision control queue full; fail closed")?;
                            r1241_finalize_pending=Some((batch_id,final_count,transition_count));
                        }
                    }else if let PaperHistoryR823::PrivateCompact(ref mut w)=history {
                        let r1180_seal_started=Instant::now();
                        let seal_frame=frame.clone();let seal_hash=proof_hash.to_string();
                        let sealed=w.call(move|writer|writer.note_guarded_decision(&seal_frame,"EVALUATED",&seal_hash)).await
                            .map_err(|e|format!("R1178 decision seal denied: {e}"))?;
                        r1180_record(r1180_seal_started,&R1180_SEAL_NS,&R1180_SEAL_MAX_NS,&R1180_SEAL_COUNT);
                        if let Some(seq)=sealed {
                            if seq!=durable_tx+1 {
                                return Err("R1177 decision seal cursor gap".into())
                            }
                            durable_tx=seq;
                        }
                    }else{
                        return Err("R1177 guarded frame without compact writer".into())
                    }
                    r1180_record(r1180_frame_started,&R1180_FRAME_NS,&R1180_FRAME_MAX_NS,&R1180_FRAME_COUNT);
                }
                if r1247_trace_enabled&&frame.source.as_ref().is_some_and(|source|source.accepted_id>0&&source.accepted_id%64==0){
                    let source=frame.source.as_ref().ok_or("R1247 sampled source provenance missing")?;
                    let timing=source.r1247_timing.as_ref();
                    let trace_id=format!("{}:{}:{}",source.session_id,source.connection_epoch,source.accepted_id);
                    let event_age_t1=timing.and_then(|t|t.t1_ws_text_wall_ms).zip(r1246_source_exchange_ts_ms)
                        .and_then(|(received,event)|received.checked_sub(event));
                    let event_age_t4=timing.and_then(|t|t.t4_engine_wall_ms).zip(r1246_source_exchange_ts_ms)
                        .and_then(|(processed,event)|processed.checked_sub(event));
                    let event_age_t6=r1246_source_exchange_ts_ms.and_then(|event|decision_now_wall_ms.checked_sub(event));
                    let t5_ns=source.selected_ns;
                    let lifecycle_changed=lifecycle!=previous_lifecycle;
                    let decision_rows=rows.iter().map(|d|json!({
                        "base":d.base,"reason":d.reason,"executable_opportunity":decision_is_opportunity(d),
                        "gross_upper_bound_bps":d.gross_upper_bound_bps,"estimated_net_bps":d.estimated_net_bps,
                        "risk_result":d.risk_result
                    })).collect::<Vec<_>>();
                    let trace_row=json!({
                        "trace_id":trace_id,"accepted_id":source.accepted_id,
                        "source":source.session_id,"connection_epoch":source.connection_epoch,
                        "selection_epoch":source.selection_epoch,"venue":format!("{:?}",frame.venue),
                        "instrument":base,"sequence":r1246_source_sequence,
                        "exchange_event_ts_ms":r1246_source_exchange_ts_ms,
                        "exchange_age_at_t1_ms":event_age_t1,"exchange_age_at_t4_ms":event_age_t4,
                        "exchange_age_at_t6_ms":event_age_t6,
                        "previous_exchange_event_ts_ms":timing.and_then(|t|t.previous_exchange_event_ts_ms),
                        "exchange_event_timestamp_advanced":timing.and_then(|t|t.exchange_event_timestamp_advanced),
                        "exchange_timestamp_delta_ms":timing.and_then(|t|t.previous_exchange_event_ts_ms.zip(r1246_source_exchange_ts_ms).map(|(prior,current)|current.saturating_sub(prior))),
                        "t1_ws_text_mono_ns":timing.and_then(|t|t.t1_ws_text_mono_ns),
                        "t1_ws_text_wall_ms":timing.and_then(|t|t.t1_ws_text_wall_ms),
                        "time_since_last_local_ws_message_ns":timing.and_then(|t|t.t1_intermessage_gap_ns),
                        "t2_parse_start_ns":timing.and_then(|t|t.t2_parse_start_ns),
                        "t2_parse_end_ns":timing.and_then(|t|t.t2_parse_end_ns),
                        "t3_enqueue_mono_ns":timing.and_then(|t|t.t3_enqueue_mono_ns),
                        "t3_queue_wait_ns":timing.and_then(|t|t.t3_queue_wait_ns),
                        "t4_engine_start_ns":timing.and_then(|t|t.t4_engine_start_ns),
                        "t4_validation_end_ns":timing.and_then(|t|t.t4_validation_end_ns),
                        "t4_engine_wall_ms":timing.and_then(|t|t.t4_engine_wall_ms),
                        "t5_selected_frame_ns":t5_ns,
                        "t6_decision_start_ns":decision_now_ns,"t6_decision_end_ns":t6_decision_end_ns,
                        "t1_to_t2_ns":timing.and_then(|t|t.t2_parse_start_ns.zip(t.t1_ws_text_mono_ns).map(|(a,b)|a.saturating_sub(b))),
                        "t1_to_t3_ns":timing.and_then(|t|t.t3_enqueue_mono_ns.zip(t.t1_ws_text_mono_ns).map(|(a,b)|a.saturating_sub(b))),
                        "t3_to_t4_ns":timing.and_then(|t|t.t4_engine_start_ns.zip(t.t3_enqueue_mono_ns).map(|(a,b)|a.saturating_sub(b))),
                        "t4_validation_to_t5_ns":timing.and_then(|t|t5_ns.zip(t.t4_validation_end_ns).map(|(a,b)|a.saturating_sub(b))),
                        "t5_to_t6_ns":t5_ns.map(|t|decision_now_ns.saturating_sub(t)),
                        "t4_validation_to_t6_ns":timing.and_then(|t|t.t4_validation_end_ns.map(|v|decision_now_ns.saturating_sub(v))),
                        "t1_to_t6_ns":timing.and_then(|t|t.t1_ws_text_mono_ns.map(|v|decision_now_ns.saturating_sub(v))),
                        "t6_decision_duration_ns":t6_decision_end_ns.saturating_sub(decision_now_ns),
                        "frame_received_ns":frame.received_ns,"frame_received_wall_ms":frame.received_wall_ms,
                        "paper_lifecycle_before":previous_lifecycle,"paper_lifecycle_after":lifecycle,
                        "paper_lifecycle_transition":lifecycle_changed,
                        "paper_transition_delta":{
                            "spot_entries":lifecycle.0.saturating_sub(previous_lifecycle.0),
                            "futures_hedges":lifecycle.1.saturating_sub(previous_lifecycle.1),
                            "futures_covers":lifecycle.2.saturating_sub(previous_lifecycle.2),
                            "spot_closes":lifecycle.3.saturating_sub(previous_lifecycle.3),
                            "open_positions_before":previous_lifecycle.4,"open_positions_after":lifecycle.4
                        },
                        "t6_decision_rows":decision_rows,
                        "t7_archive_enqueue_event_ids":r1247_archive_enqueue_ids,
                        "t7_archive_durable_ack":"run_level_dual_copy_verification_only"
                    });
                    if r1247_decision_trace.len()>=512{r1247_decision_trace.remove(0);r1247_decision_trace_evictions=r1247_decision_trace_evictions.saturating_add(1);}
                    r1247_decision_trace.push(trace_row);
                }
            }
            Event::R1241BatchDurableFrame{..}=>{
                return Err("R1241 durable batch normalization failed".into())
            }
            Event::Ticker{venue,base,shard,epoch,sequence,received_wall_ms,raw}=>{
                // Every ticker must belong to the shard's actual public
                // transport task. No ticker or quote enters the model here.
                if !ticker_review||ticker_shards.get(&base)!=Some(&shard)||
                    epoch==0||sequence==0||raw.len()>32_768{
                    r154_denied+=1;continue
                }
                let previous=match venue{
                    Venue::OkxSpot=>r154_okx.get(&base),
                    Venue::BinanceFutures=>r154_binance.get(&base),
                }.cloned();
                let cache=match venue{
                    Venue::OkxSpot=>&mut r154_okx,
                    Venue::BinanceFutures=>&mut r154_binance,
                };
                let result=r154_cache_ticker(cache,&base,venue,shard,epoch,sequence,
                    received_wall_ms,raw,wall_ms());
                match result{
                    R154CacheAction::Duplicate=>continue,
                    R154CacheAction::Stale=>{r154_denied+=1;continue},
                    R154CacheAction::Rejected=>{
                        r154_denied+=1;r154_ready.remove(&base);continue
                    },
                    R154CacheAction::NewEpoch=>{r154_ready.remove(&base);},
                    R154CacheAction::Accepted=>{},
                }
                let (Some(o),Some(b))=(r154_okx.get(&base),r154_binance.get(&base))
                    else{r154_ready.remove(&base);continue};
                let owner=owner_index(&base,OWNERS);
                let spot_url="wss://ws.okx.com/ws/v5/public".to_string();
                let future_url=format!(
                    "wss://fstream.binance.com/market/ws/{}usdt@ticker",
                    base.to_ascii_lowercase());
                let spot_id=format!("okx-shard-{}",o.shard);
                let future_id=format!("binance-shard-{}",b.shard);
                let topo=PublicTickerTopologyR126::new_with_owner_count(
                    owner,OWNERS,&[
                        TickerSubscription{connection_id:spot_id.clone(),
                            owner,venue:Venue::OkxSpot,base:base.clone(),url:spot_url.clone()},
                        TickerSubscription{connection_id:future_id.clone(),
                            owner,venue:Venue::BinanceFutures,base:base.clone(),
                            url:future_url.clone()},
                    ]);
                let spot_env=PublicWsEnvelope{
                    venue:Venue::OkxSpot,url:&spot_url,base:&base,
                    connection_epoch:o.epoch,sequence:o.sequence,
                    received_wall_ms:o.wall,body:&o.raw,
                };
                let future_env=PublicWsEnvelope{
                    venue:Venue::BinanceFutures,url:&future_url,base:&base,
                    connection_epoch:b.epoch,sequence:b.sequence,
                    received_wall_ms:b.wall,body:&b.raw,
                };
                if topo.as_ref().map_or(true,|x|
                    x.check_pair(&spot_id,&future_id,spot_env,future_env).is_err()){
                    let cache=match venue{
                        Venue::OkxSpot=>&mut r154_okx,
                        Venue::BinanceFutures=>&mut r154_binance,
                    };
                    r154_restore_ticker(cache,&base,previous);
                    r154_denied+=1;r154_ready.remove(&base);continue
                }
                match r154_boot.observe(wall_ms(),spot_env,future_env){
                    Ok(Some(_))=>{r154_seen+=1;r154_ready.insert(base);},
                    Ok(None)=>{r154_ready.remove(&base);},
                    Err(_)=>{
                        let cache=match venue{
                            Venue::OkxSpot=>&mut r154_okx,
                            Venue::BinanceFutures=>&mut r154_binance,
                        };
                        r154_restore_ticker(cache,&base,previous);
                        r154_denied+=1;r154_ready.remove(&base);
                    },
                }
            }
            Event::Reset{venue,bases,epoch}=>{
                if r1205_enabled&&canonical_reset.is_none(){
                    r1241_next_reset_id=r1241_next_reset_id.checked_add(1)
                        .ok_or("R1241 reset id overflow")?;
                    r1241_pipeline::submit(
                        r1241_pipeline_tx.as_ref().ok_or("R1241 pipeline missing")?,
                        r1241_pipeline::Command::Reset{
                            request_id:r1241_next_reset_id,venue,bases,epoch
                        })?;
                    continue
                }
                if batch_mode && !pending_batch.is_empty(){
                    // Preserve source order: pre-reconnect accepted quotes
                    // must commit and be processed BEFORE clearing books.
                    r1088_flush_queued_frames(&mut pending_batch,&mut owners,
                        &mut history,&mut durable_frames,&mut durable_tx).await?;
                    durable_frames.push_back(Event::Reset{venue,bases,epoch});
                    continue
                }
                // R1173: disconnect/reset must be committed in DB1+DB2
                // BEFORE invalidating the canonical Owner. Not reconstructed
                // from a later quote's epoch after crash.
                let mut compact_reset_applied=r1205_enabled&&canonical_reset.is_some();
                if !r1205_enabled{
                    if let PaperHistoryR823::PrivateCompact(ref mut w)=history{
                        let mut owned=std::mem::take(&mut owners);let reset_bases=bases.clone();
                        let (seq,returned)=w.call(move|writer|{
                            let seq=writer.source_reset(&mut owned,venue,&reset_bases,epoch,wall_ms(),"PUBLIC_WS_RESET",R1173Inject::None)?;
                            Ok((seq,owned))
                        }).await.map_err(|e|format!("R1173 WS source reset denied: {e}"))?;
                        owners=returned;
                        if seq!=durable_tx+1{
                            return Err("R1173 source reset transaction cursor gap".into())
                        }
                        durable_tx=seq;
                        compact_reset_applied=true;
                    }
                }
                resets+=1;
                // R1106: remove coverage from the DISCONNECTED epoch
                // before any newly received quote can restore eligibility.
                match venue{
                    Venue::BinanceFutures=>{
                        invalidated_coverage_binance+=
                            r1106_clear_stale_coverage(&mut seen_b,&bases) as u64;
                    },
                    Venue::OkxSpot=>{
                        invalidated_coverage_okx+=
                            r1106_clear_stale_coverage(&mut seen_o,&bases) as u64;
                    }
                }
                if ticker_review{
                    for base in &bases{
                        r154_ready.remove(base);
                        match venue{
                            Venue::OkxSpot=>{r154_okx.remove(base);},
                            Venue::BinanceFutures=>{r154_binance.remove(base);},
                        }
                    }
                }
                let exchange=match venue{Venue::OkxSpot=>"okx_spot",Venue::BinanceFutures=>"binance_futures"};
                for base in bases {
                    let idx=owner_index(&base,OWNERS);
                    if !compact_reset_applied {
                        if let Err(e)=owners[idx].reset_venue(&base,venue,epoch){
                            task_errors.push(format!("owner_reset:{base}:{e:?}"))
                        }
                    }
                    if batch_mode {
                        if let Err(e)=decision_owners[idx].reset_venue(&base,venue,epoch){
                            return Err(format!("R1088 replay owner reset refused: {e:?}").into())
                        }
                    }
                    if let Err(e)=cores[idx].reset_exchange(&base,exchange,epoch){task_errors.push(format!("core_reset:{base}:{e:?}"))}
                }
                if let Some(request_id)=canonical_reset{
                    r1241_pipeline_control_tx.as_ref().ok_or("R1241 control channel missing")?
                        .try_send(r1241_pipeline::Control::ResetApplied{request_id})
                        .map_err(|_|"R1241 reset control queue full; fail closed")?;
                }
            }
            Event::Canonical(r1241_pipeline::Event::BatchReady{batch_id,source_seq,frames})=>{
                if source_seq!=durable_tx+1||frames.is_empty(){
                    return Err("R1241 source batch cursor/count mismatch".into())
                }
                if r1241_active_batch.is_some()||r1241_finalize_pending.is_some(){
                    return Err("R1241 overlapping canonical batches".into())
                }
                durable_tx=source_seq;
                let batch_count=frames.len();
                for(batch_index,item)in frames.into_iter().enumerate(){
                    durable_frames.push_back(Event::R1241BatchDurableFrame{
                        batch_id,batch_index,batch_count,frame:item.frame,update:item.update
                    });
                }
                r1241_active_batch=Some((batch_id,batch_count,0,Vec::with_capacity(batch_count)));
            }
            Event::Canonical(r1241_pipeline::Event::BatchAcked{batch_id,transition_seqs,decision_seq,release_seq,frame_count})=>{
                let (expected_id,expected_count,expected_transitions)=r1241_finalize_pending.take()
                    .ok_or("R1241 unexpected canonical ACK")?;
                if batch_id!=expected_id||frame_count!=expected_count||transition_seqs.len()!=expected_transitions{
                    return Err("R1241 canonical ACK identity/count mismatch".into())
                }
                for seq in transition_seqs {
                    if seq!=durable_tx+1{return Err("R1241 modeled transition cursor discontinuity".into())}
                    durable_tx=seq;
                }
                let decision_seq=decision_seq.ok_or("R1241 decision seal missing")?;
                if decision_seq!=durable_tx+1{return Err("R1241 decision cursor discontinuity".into())}
                durable_tx=decision_seq;
                let release_seq=release_seq.ok_or("R1241 release ACK missing")?;
                if release_seq!=durable_tx+1{return Err("R1241 release cursor discontinuity".into())}
                durable_tx=release_seq;
            }
            Event::Canonical(r1241_pipeline::Event::Stopped{profile})=>{
                r1241_pipeline_profile=profile;
                r1241_pipeline_stopped=true;
                r1241_pipeline_rx=None;
                if done==tasks{break}
            }
            Event::Canonical(r1241_pipeline::Event::Failed(error))=>{
                return Err(format!("R1241 canonical ingress failed; recovery required: {error}").into())
            }
            Event::Canonical(r1241_pipeline::Event::ResetReady{..})=>{
                return Err("R1241 reset event normalization failed".into())
            }
            Event::R1246TelemetryTick=>{},
            Event::TaskError{venue,shard,error}=>task_errors.push(format!("{venue}[{shard}]:{error}")),
            Event::TaskDone{venue,shard,reconnects,frames}=>{
                done+=1;
                if reconnects>0 {task_errors.push(format!("{venue}[{shard}]:reconnects={reconnects}:frames={frames}"))}
                if done==tasks&&r1205_enabled&&!r1241_shutdown_requested{
                    r1241_pipeline_tx.as_ref().ok_or("R1241 pipeline missing")?
                        .send(r1241_pipeline::Command::Shutdown).await
                        .map_err(|_|"R1241 shutdown could not enter bounded queue")?;
                    r1241_shutdown_requested=true;
                }
                if done==tasks&&!batch_mode{break}
                if done==tasks&&r1241_pipeline_stopped{break}
            }
        }
        if last_status.elapsed()>=Duration::from_secs(60){
            if read_json(&kill_path).get("state").and_then(Value::as_str)!=Some("ENGAGED"){
                return Err("paper observer real kill-switch no longer ENGAGED".into())
            }
            if let Some(ref path)=checkpoint_path {
                checkpoint_revision=checkpoint_revision.checked_add(1)
                    .ok_or("paper checkpoint revision overflow")?;
                save_checkpoint(path,&portfolio,checkpoint_revision,monotonic_ns(),false)
                    .map_err(|e|format!("paper periodic checkpoint failed: {e:?}"))?;
            }
            if let Some(ref path)=status_path{
                let snapshot=json!({
                    "updated_wall_ms":wall_ms(),"elapsed_seconds":started.elapsed().as_secs(),
                    "frames":accepted,"decisions":decisions,"opportunities":opportunities,
                    "errors":{"parse":parse_errors,"ingest":ingest_errors,"decision":decision_errors},
                    "ingest_error_samples":ingest_error_samples,
                    "decision_error_samples":decision_error_samples,
                    "r1130_source_stage_timing":r1130_stage_timing(),
                    "r1150_tokio_scheduler_probe":r1150_snapshot(),
                    "r1180_compact_timing":r1180_compact_snapshot(),
                    "r1241_async_canonical_ingress":r1241_pipeline_profile,
                    "r1127_db1_durable_timing":{
                        "transactions":R1127_DB1_TX_COUNT.load(Ordering::Relaxed),
                        "cumulative_ms":R1127_DB1_TX_NS.load(Ordering::Relaxed)/1_000_000,
                        "max_transaction_ms":R1127_DB1_TX_MAX_NS.load(Ordering::Relaxed)/1_000_000
                    },
                    "r1125_public_ingest_pressure":{
                        "rx_queue_depth":r1125_queue_witness.upgrade().map_or(0,
                            |sender|event_queue_cap.saturating_sub(sender.capacity())),
                        "rx_queue_capacity":event_queue_cap,
                        "rx_queue_peak":r1125_queue_peak,
                        "durable_pending":durable_frames.len(),
                        "last_binance_source_lag_ms":r1125_last_bin_lag,
                        "last_okx_source_lag_ms":r1125_last_okx_lag,
                        "peak_binance_source_lag_ms":r1125_peak_bin_lag,
                        "peak_okx_source_lag_ms":r1125_peak_okx_lag,
                        "last_receive_to_decision_ms":r1126_last_queue_delay_ms,
                        "peak_receive_to_decision_ms":r1126_peak_queue_delay_ms
                    },
                    "websocket_seen":{"binance":seen_b.len(),"okx":seen_o.len()},
                    "r1246":{"enabled":r1246_enabled,
                        "session_id":std::env::var("R1246_SESSION_ID").ok(),
                        "route_contract":"OKX Spot BUY + Binance Futures SHORT",
                        "archive_candidates":r1246_archived_candidates,
                        "archive_paper_transitions":r1246_archived_transitions,
                        "decision_latency_ns":r1246_latency_summary(&r1246_decision_samples),
                        "enqueue_latency_ns":r1246_latency_summary(&r1246_enqueue_samples),
                        "archive_metrics":r1246_archive.as_ref().map(|a|a.metrics()).unwrap_or(Value::Null),
                        "real_orders":false},
                    "r1120_source_health":{"expected_bases":expected,
                       "binance_connection_resets":invalidated_coverage_binance,
                       "okx_connection_resets":invalidated_coverage_okx,
                       "task_notes_recent":task_errors.iter().rev().take(8)
                           .cloned().collect::<Vec<_>>(),
                       "binance_missing_count":expected.saturating_sub(seen_b.len()),
                       "okx_missing_count":expected.saturating_sub(seen_o.len()),
                       "terminal_trade_readiness":seen_b.len()==expected&&seen_o.len()==expected,
                       "real_orders":false},
                    "paper_portfolio":portfolio.snapshot(),
                    "applied_paper_decision_policy":applied_config,
                    "planned_virtual_positions_carried":carried_positions,
                    "virtual_continuity_checkpoint_only":checkpoint_path.is_some(),
                    "r154_public_ticker_probe":{"enabled":ticker_review,
                        "cold_ready_bases":r154_ready.len(),
                        "warm_pair_events":r154_seen,"denied_pair_events":r154_denied,
                        "public_ticker_changes_used_for_entries":false,
                        "network_peer_independently_attested":false},
                    "live_trading":false,"private_api":false
                });
                if let Some(writer)=r1246_telemetry.as_mut(){
                    serde_json::to_writer(&mut *writer,&snapshot)?;
                    writer.write_all(b"\n")?;
                    writer.flush()?;
                }
                let tmp=path.with_extension("tmp");
                fs::write(&tmp,serde_json::to_vec_pretty(&snapshot)?)?;
                fs::rename(tmp,path)?;
            }
            last_status=Instant::now();
        }
    }
    if let PaperHistoryR823::PrivateCompact(ref w)=history {
        w.call(|writer|{
            writer.require_decisions_drained().map_err(|e|format!("R1177 final decision gate not sealed: {e}"))?;
            writer.verify_pair().map_err(|e|format!("R1177 final DB1 DB2 mismatch: {e}"))
        }).await?;
    }
    if let Some(w)=cap_writer.as_mut(){w.flush()?}
    if let Some(ref path)=checkpoint_path {
        checkpoint_revision=checkpoint_revision.checked_add(1)
            .ok_or("paper checkpoint revision overflow")?;
        save_checkpoint(path,&portfolio,checkpoint_revision,monotonic_ns(),true)
            .map_err(|e|format!("paper final checkpoint failed: {e:?}"))?;
    }
    if let Some(archive)=r1246_archive.as_ref(){
        archive.push("paper_session_seal",json!({
            "route_contract":"OKX Spot BUY -> Binance Futures SHORT -> Binance Futures COVER -> OKX Spot SELL",
            "sealed_wall_ms":wall_ms(),"sealed_monotonic_ns":monotonic_ns(),
            "elapsed_s":started.elapsed().as_secs_f64(),"decisions":decisions,
            "opportunities":opportunities,"paper_portfolio":portfolio.snapshot(),
            "decision_errors":decision_errors,"ingest_errors":ingest_errors,
            "parse_errors":parse_errors,"task_error_count":task_errors.len(),
            "real_orders":false,"private_order_path_linked":false
        })).map_err(|e|format!("R1246 PAPER session-seal archive denied: {e}"))?;
    }
    let r1246_archive_result=if let Some(ref mut archive)=r1246_archive{
        let verified=archive.drain_and_verify(Duration::from_secs(30))?;
        archive.stop();
        Some(verified)
    }else{None};
    let ended_wall_ms=wall_ms();
    let ended_monotonic_ns=monotonic_ns();
    let model_audit_result=if let Some(ref mut modeled)=model_journal{
        let finished=modeled.seal(&portfolio)
            .map_err(|e|format!("modeled four-leg journal seal denied: {e:?}"))?;
        // R1116: independently advance local R37 queue to the
        // exact fsynced SESSION_SEAL journal watermark, no resend.
        if let (Some(ref root),Some(ref cfg),Some(ref path))=
              (&r1116_root,&r1116_cfg,&model_journal_path){
            let (queued_floor,_)=r1116_commit(root,path,cfg,
                 u64::try_from(wall_ms()).map_err(|_|"R1116 final clock")?,false)
               .map_err(|e|format!("R1116 final modeled queue custody: {e}"))?;
            if queued_floor!=finished.sequence{
                return Err("R1116 final modeled watermark mismatched".into())
            }
        }
        json!({"enabled":true,"sealed":finished.sealed,
            "sequence":finished.sequence,"last_hash":finished.last_hash,
            "exchange_fills_attested":false,"real_order_authority":false})
    }else{
        json!({"enabled":false,"sealed":false,
            "exchange_fills_attested":false,"real_order_authority":false})
    };
    let elapsed=started.elapsed().as_secs_f64();let cpu=cpu_s()-cpu0;
    let hard_errors=task_errors.iter().filter(|x|!x.contains(":reconnects=")).count();
    // R1102: preserve the exact fail-closed coverage rule and reveal
    // which public source failed to produce ANY book update during the window.
    // No dummy frame, optimistic seen flag, excluded base, or trading authority.
    let mut missing_binance=bases.iter().filter(|b|!seen_b.contains(*b))
        .cloned().collect::<Vec<_>>();
    let mut missing_okx=bases.iter().filter(|b|!seen_o.contains(*b))
        .cloned().collect::<Vec<_>>();
    missing_binance.sort();missing_okx.sort();
    let result=json!({
        "r21_live_public_shadow":hard_errors==0&&parse_errors==0&&ingest_errors==0&&decision_errors==0&&seen_b.len()==expected&&seen_o.len()==expected,
        "paper_portfolio":portfolio.snapshot(),
        "applied_paper_decision_policy":applied_config,
        "modeled_four_leg_journal":model_audit_result,
        "r154_public_ticker_probe":{"enabled":ticker_review,
            "cold_ready_bases":r154_ready.len(),
            "warm_pair_events":r154_seen,"denied_pair_events":r154_denied,
            "public_ticker_changes_used_for_entries":false,
            "network_peer_independently_attested":false,
            "real_order_authority":false},
        "virtual_paper_continuity":{
            "planned_positions_carried":carried_positions,
            "durable_local_checkpoint_only":checkpoint_path.is_some(),
            "checkpoint_completed":checkpoint_path.is_some(),
            "checkpoint_revision":checkpoint_revision,
            "graceful_early_stop":graceful_early_stop,
            "real_execution_evidence":false,
            "real_orders":false
        },
        "ingest_error_samples":ingest_error_samples,
        "decision_error_samples":decision_error_samples,
        "duration_s":secs,"actual_duration_s":elapsed,
        "run_window":{"start_utc_epoch_ms":started_wall_ms,"end_utc_epoch_ms":ended_wall_ms,
            "start_monotonic_ns":started_monotonic_ns,"end_monotonic_ns":ended_monotonic_ns,
            "graceful_early_stop":graceful_early_stop},
        "r1246":{"enabled":r1246_enabled,
            "session_id":std::env::var("R1246_SESSION_ID").ok(),
            "route_contract":"OKX Spot BUY -> Binance Futures SHORT -> Binance Futures COVER -> OKX Spot SELL",
            "spot_spot_route_used":false,
            "market_scope":{"metadata_intersection_bases":meta.bases().len(),
                "runtime_bases":expected,"cohort_width":R1246_COHORT_WIDTH,
                "selection_method":"top-40 by min(OKX Spot, Binance USD-M 24h quote volume); aligned to websocket.top_max=40; not legacy scanner-rank parity",
                "selected_bases_sha256":r1246_selected_bases_sha256},
            "archived_opportunity_candidates":r1246_archived_candidates,
            "archived_paper_transitions":r1246_archived_transitions,
            "decision_latency_ns":r1246_latency_summary(&r1246_decision_samples),
            "enqueue_latency_ns":r1246_latency_summary(&r1246_enqueue_samples),
            "archive_metrics":r1246_archive.as_ref().map(|a|a.metrics()).unwrap_or(Value::Null),
            "archive_verification":r1246_archive_result,
            "real_orders":false},
        "r1247":{
            "enabled":r1247_trace_enabled,
            "t1_definition":"first timestamp after ws.next() yields Message::Text, before control classification; not a TCP-byte timestamp",
            "exchange_age_clock":"host wall-clock milliseconds minus exchange event timestamp; local intervals use monotonic time",
            "sample_policy":{"accepted_id_modulo":64,"selection_evaluation_interval_ns":2000000000,
                "frame_stage_ring_capacity":1024,"decision_trace_capacity":512,"selection_evaluation_capacity":3072},
            "decision_trace_sample_count":r1247_decision_trace.len(),
            "decision_trace_evictions":r1247_decision_trace_evictions,
            "decision_trace":r1247_decision_trace,
            "t7_per_event_durable_ack":"not joined per event; final dual-copy archive verification is run-level"
        },
        "graceful_early_stop":graceful_early_stop,
        "force_reconnect":force_reconnect,"trading":false,"private_order_path_linked":false,
        "metadata":{"source":"pure_native_https","generation":meta.generation,"universe":expected,
            "binance_crypto_active":meta.binance_active,"binance_tradifi_excluded":meta.binance_tradifi_excluded,
            "okx_active":meta.okx_active,"ready":meta.metadata_ready},
        "r1130_source_stage_timing":r1130_stage_timing(),
        "r1150_tokio_scheduler_probe":r1150_snapshot(),
        "r1180_compact_timing":r1180_compact_snapshot(),
        "r1241_async_canonical_ingress":r1241_pipeline_profile,
        "ws":{"shards_per_venue":ws_group_count,"binance_frames":counters.bin_frames.load(Ordering::Relaxed),
            "okx_frames":counters.okx_frames.load(Ordering::Relaxed),"binance_seen":seen_b.len(),"okx_seen":seen_o.len(),
            "missing_binance_bases":missing_binance,"missing_okx_bases":missing_okx,
            "prior_epoch_binance_coverage_invalidated":invalidated_coverage_binance,
            "prior_epoch_okx_coverage_invalidated":invalidated_coverage_okx,
            "coverage_requires_new_epoch_real_book":true,
            "reset_at_disconnect_before_ws_reconnect":true,
            "resets":resets,"task_notes":task_errors,
            "r1125_max_ingest_queue":r1125_queue_peak,
            "r1125_max_queue_capacity":event_queue_cap,
            "r1125_peak_binance_source_lag_ms":r1125_peak_bin_lag,
            "r1125_peak_okx_source_lag_ms":r1125_peak_okx_lag,
            "r1126_peak_receive_to_decision_ms":r1126_peak_queue_delay_ms,
            "r1127_db1_timing_transactions":R1127_DB1_TX_COUNT.load(Ordering::Relaxed),
            "r1127_db1_timing_cumulative_ms":R1127_DB1_TX_NS.load(Ordering::Relaxed)/1_000_000,
            "r1127_db1_timing_max_ms":R1127_DB1_TX_MAX_NS.load(Ordering::Relaxed)/1_000_000},
        "pipeline":{"accepted_frames":accepted,"history_interface_accepts":history.accepted_count().await?,
            "r1088_live_batch16":batch_mode&&r1129_batch_target==16,
            "r1129_live_batch32":batch_mode&&r1129_batch_target==32,
            "r1088_durable_transactions":durable_tx,"decisions":decisions,
            "reasons":reasons,"opportunities":opportunities,"parse_errors":parse_errors,"ingest_errors":ingest_errors,
            "decision_errors":decision_errors},
        "hard_safety":{"allowed":safety.allowed,"verdict":safety.verdict,"blocker_codes":safety.blocker_codes,
            "real_launch_authorized":safety.real_launch_authorized,"opportunities_denied":safety_denies},
        "execution_rehearsal":{"in_memory_only":true,"planned_spot_transitions":rehearsal_plans,
            "durable_intent_created":false,"network_mutation_possible":false},
        "opportunity_samples":opportunity_samples,
        "resources":{"wall_s":elapsed,"cpu_s":cpu,"cpu_pct_one_core":if elapsed>0.0{cpu/elapsed*100.0}else{0.0},
            "rss_start_kib":rss0,"rss_peak_kib":rss_peak,"frames_per_s":if elapsed>0.0{accepted as f64/elapsed}else{0.0}}
    });
    if let Some(path)=output{fs::write(path,serde_json::to_vec_pretty(&result)?)?}
    println!("{}",serde_json::to_string(&result)?);
    if result["r21_live_public_shadow"]!=true {std::process::exit(2)}
    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;
    fn r266_original()->NativeTradingConfig{
        NativeTradingConfig::parse(include_str!("../../config/native_trading.yaml")).unwrap()
    }
    #[test]
    fn r276_restricted_source_paper_in_memory_applies_actual_3000(){
        let raw=include_str!("../../config/native_trading.yaml");
        let proof=candidate_build_provenance().unwrap();
        let v=r276_offline_model_receipt(raw,&proof).unwrap();
        assert_eq!(v["original_admitted"],false);
        assert_eq!(v["sandbox_source_model_admitted"],true);
        assert_eq!(v["applied_in_memory_source_model"]["paper_max_daily_entries"],3000);
        assert_eq!(v["applied_in_memory_source_model"]["compiled_legacy_default_daily_cap"],500);
        assert_eq!(v["portfolio_runtime_counters"]["open_position_limit"],10);
        assert_eq!(v["portfolio_runtime_counters"]["open_positions"],0);
        assert_eq!(v["real_order_authority"],false);
        assert_eq!(v["original_160_field_semantics_equivalent"],false);
    }
    #[test]
    fn r276_denies_wrong_original_yaml_or_false_elf_identity(){
        let raw=include_str!("../../config/native_trading.yaml");
        let build=candidate_build_provenance().unwrap();
        let malformed=raw.replace("paper_max_trades_per_day: 3000",
                                  "paper_max_trades_per_day: 3001");
        assert!(r276_offline_model_receipt(&malformed,&build).is_err());
        let mut fake=build.clone();
        fake["limits"]["deployed_elf_attested"]=json!(true);
        assert!(r276_offline_model_receipt(raw,&fake).is_err());
    }
    #[test]
    fn r266_exact_candidate_source_160_field_report_never_claims_installed(){
        let cfg=r266_original();
        let historical=embedded_r176().unwrap();
        let build=candidate_build_provenance().unwrap();
        let v=r266_candidate_yaml_report(&cfg,&historical,&build).unwrap();
        assert_eq!(v["source_research_count"],149);
        assert_eq!(v["source_guard_only_count"],11);
        assert_eq!(v["historical_source_only_field_inventory"].as_array().unwrap().len(),160);
        assert_eq!(v["actual_source_admission"]["source_paper_r155"]
            ["actual_in_memory_constructor_accepted"],false);
        assert_eq!(v["installed_per_field_proven"],0);
        assert_eq!(v["real_trading_authorized"],false);
    }
    #[test]
    fn r266_build_yaml_digest_mismatch_and_fake_deploy_are_denied(){
        let cfg=r266_original();
        let historical=embedded_r176().unwrap();
        let mut build=candidate_build_provenance().unwrap();
        build["build_snapshot"]["operator_yaml_sha256"]=json!("bad");
        assert!(r266_candidate_yaml_report(&cfg,&historical,&build).is_err());
        let mut build=candidate_build_provenance().unwrap();
        build["limits"]["deployed_elf_attested"]=json!(true);
        assert!(r266_candidate_yaml_report(&cfg,&historical,&build).is_err());
    }
    #[test]
    fn r266_forged_field_and_duplicate_are_denied(){
        let cfg=r266_original();
        let mut historical=embedded_r176().unwrap();
        let build=candidate_build_provenance().unwrap();
        historical["fields"][0]["installed_paper_applied"]=json!("PROVEN");
        assert!(r266_candidate_yaml_report(&cfg,&historical,&build).is_err());
        let mut historical=embedded_r176().unwrap();
        historical["fields"][0]["field"]=historical["fields"][1]["field"].clone();
        assert!(r266_candidate_yaml_report(&cfg,&historical,&build).is_err());
        let mut changed=r266_original();
        changed.raw["analytics"]["enabled"]=serde_yaml::Value::Bool(false);
        let historical=embedded_r176().unwrap();
        assert!(r266_candidate_yaml_report(&changed,&historical,&build).is_err());
    }
    #[test]
    fn r257_empty_public_membership_is_denied() {
        let m=NativeMetadataSnapshot{generation:1,loaded_ns:1,loaded_wall_ms:1,
            binance_active:0,binance_tradifi_excluded:0,okx_active:0,
            metadata_ready:0,static_denied:0,markets:Default::default(),
            routes:Default::default()};
        assert!(r257_membership_receipt(&m).is_err());
    }
    #[test]
    fn r257_public_membership_is_sorted_hashed_and_never_installed_claim(){
        use spotarb_native_market_core::market_metadata::NativeMarketMetadata;
        let mut m=NativeMetadataSnapshot{generation:1,loaded_ns:1,loaded_wall_ms:42,
            binance_active:2,binance_tradifi_excluded:0,okx_active:2,
            metadata_ready:1,static_denied:1,markets:Default::default(),
            routes:Default::default()};
        for (base,reason) in [("ETH",Some("MARKET_INACTIVE")),("BTC",None)]{
            m.markets.insert(base.into(),NativeMarketMetadata{
                base:base.into(),spot_symbol:format!("{base}-USDT"),
                future_symbol:format!("{base}USDT"),spot_min_amount:None,
                future_min_base:None,spot_min_cost:"1".into(),
                future_min_cost:"1".into(),spot_step:None,future_step_base:None,
                contract_size:"1".into(),buy_exchange:"okx_spot".into(),
                sell_exchange:"binance_futures".into(),
                route_label:"OKX Spot BUY -> Binance Futures SHORT".into(),
                static_reason:reason.map(str::to_string),
                loaded_ns:1,loaded_wall_ms:42,spot_tick:None,future_tick:None,
                spot_status:"live".into(),future_status:"TRADING".into(),
                spot_max_amount:None,future_max_base:None,
            });
        }
        let v=r257_membership_receipt(&m).unwrap();
        assert_eq!(v["public_intersection_bases"],json!(["BTC","ETH"]));
        assert_eq!(v["eligible_bases"],json!(["BTC"]));
        let expected=openssl::sha::sha256(b"BTC\nETH").iter()
            .map(|x|format!("{x:02x}")).collect::<String>();
        assert_eq!(v["public_intersection_sha256"],expected);
        assert_eq!(v["installed_paper_ws_membership_attested"],false);
        assert_eq!(v["network_mutation_possible"],false);
        assert_eq!(v["real_trading_authorized"],false);
    }
    const R172_NOW:i64=1_791_783_600_000;
    fn r172_okx(ts:i64)->String{
        format!(r#"{{"arg":{{"channel":"tickers","instId":"BTC-USDT"}},"data":[{{"instType":"SPOT","instId":"BTC-USDT","volCcy24h":"200000","ts":"{ts}"}}]}}"#)
    }
    fn r172_binance(ts:i64)->String{
        format!(r#"{{"e":"24hrTicker","E":{ts},"s":"BTCUSDT","q":"300000","O":{},"C":{ts},"st":1}}"#,ts-86_400_000)
    }
    #[test]
    fn r172_invalid_or_wrong_ticker_cannot_poison_cache(){
        let mut cache=HashMap::new();
        let good=r172_okx(R172_NOW-1_200);
        assert_eq!(r154_cache_ticker(&mut cache,"BTC",Venue::OkxSpot,
            0,2,11,R172_NOW,good.clone(),R172_NOW),R154CacheAction::Accepted);
        for bad in ["not-json".to_owned(),good.replace("BTC-USDT","ETH-USDT"),
                    r172_okx(R172_NOW-40_000)]{
            assert_eq!(r154_cache_ticker(&mut cache,"BTC",Venue::OkxSpot,
                0,2,12,R172_NOW,bad,R172_NOW),R154CacheAction::Rejected);
            assert_eq!(cache["BTC"].sequence,11);
            assert_eq!(cache["BTC"].raw,good);
        }
        assert_eq!(r154_cache_ticker(&mut cache,"BTC",Venue::OkxSpot,
            0,0,12,R172_NOW,good.clone(),R172_NOW),R154CacheAction::Rejected);
        assert_eq!(cache["BTC"].sequence,11);
    }
    #[test]
    fn r172_old_epoch_sequence_and_source_clock_do_not_evict(){
        let mut cache=HashMap::new();
        let initial=r172_binance(R172_NOW-1_200);
        assert_eq!(r154_cache_ticker(&mut cache,"BTC",Venue::BinanceFutures,
            1,4,18,R172_NOW,initial.clone(),R172_NOW),R154CacheAction::Accepted);
        for (epoch,seq,raw) in [(3,21,initial.clone()),
            (4,17,initial.clone()),(4,19,r172_binance(R172_NOW-1_500))]{
            assert_eq!(r154_cache_ticker(&mut cache,"BTC",Venue::BinanceFutures,
                1,epoch,seq,R172_NOW,raw,R172_NOW),R154CacheAction::Stale);
            assert_eq!(cache["BTC"].epoch,4);
            assert_eq!(cache["BTC"].sequence,18);
            assert_eq!(cache["BTC"].raw,initial);
        }
    }
    #[test]
    fn r172_exact_duplicate_noop_but_same_sequence_mutation_denied(){
        let mut cache=HashMap::new();
        let raw=r172_okx(R172_NOW-900);
        assert_eq!(r154_cache_ticker(&mut cache,"BTC",Venue::OkxSpot,
            0,5,21,R172_NOW,raw.clone(),R172_NOW),R154CacheAction::Accepted);
        assert_eq!(r154_cache_ticker(&mut cache,"BTC",Venue::OkxSpot,
            0,5,21,R172_NOW,raw.clone(),R172_NOW),R154CacheAction::Duplicate);
        let mutation=raw.replace("200000","201000");
        assert_eq!(r154_cache_ticker(&mut cache,"BTC",Venue::OkxSpot,
            0,5,21,R172_NOW,mutation,R172_NOW),R154CacheAction::Rejected);
        assert_eq!(cache["BTC"].raw,raw);
        assert_eq!(cache["BTC"].sequence,21);
    }
    #[test]
    fn r172_epoch_advance_and_postcheck_rollback_restore_last_good(){
        let mut cache=HashMap::new();
        let first=r172_okx(R172_NOW-1_300);
        assert_eq!(r154_cache_ticker(&mut cache,"BTC",Venue::OkxSpot,
            0,4,25,R172_NOW,first.clone(),R172_NOW),R154CacheAction::Accepted);
        let prior=cache.get("BTC").cloned();
        assert_eq!(r154_cache_ticker(&mut cache,"BTC",Venue::OkxSpot,
            0,5,1,R172_NOW,r172_okx(R172_NOW-600),R172_NOW),
            R154CacheAction::NewEpoch);
        assert_eq!(cache["BTC"].epoch,5);
        r154_restore_ticker(&mut cache,"BTC",prior);
        assert_eq!(cache["BTC"].epoch,4);
        assert_eq!(cache["BTC"].sequence,25);
        assert_eq!(cache["BTC"].raw,first);
        let mut empty=HashMap::new();
        assert_eq!(r154_cache_ticker(&mut empty,"BTC",Venue::OkxSpot,
            0,1,1,R172_NOW,r172_okx(R172_NOW-600),R172_NOW),
            R154CacheAction::Accepted);
        r154_restore_ticker(&mut empty,"BTC",None);
        assert!(empty.is_empty());
    }
    #[test]
    fn r167_candidate_source_only_fleet_40_does_not_rewrite_compiled_32(){
        let source=NativeTradingConfig::parse(
            include_str!("../../config/native_trading.yaml")).unwrap();
        let bases=(0..235).rev().map(|i|format!("SIM{i:04}"))
            .collect::<Vec<_>>();
        let selected=SourceWsShardPlanR167::from_source(&source,&bases).unwrap();
        assert_eq!(selected.config_sha256,source.sha256);
        assert_eq!(selected.group_size,40);
        assert_eq!(selected.groups.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![40,40,40,40,40,35]);
        assert_eq!(selected.task_count(),Ok(18));
        let shards=selected.owner_shards();
        assert_eq!(shards.len(),235);
        assert_eq!(shards["SIM0000"],0);
        assert_eq!(shards["SIM0234"],5);
        assert_eq!(SHARD,32,"compiled fallback profile must remain intact");
        assert_eq!(bases.chunks(SHARD).count(),8);
        assert!(!source.policy.real_execution_authorized);
        // Source YAML is still NOT admissible to supervised strategy
        // because enabled analytics/scale-in and other gates are open.
        assert!(SupervisedStrategyPaper::new(source,wall_ms()).is_err());
    }
    #[test]
    fn r1124_paper_depth_500ms_preserves_actual_public_snapshot_source(){
        assert_eq!(r1124_binance_depth_stream("BTC",false),
                   "btcusdt@depth20@100ms");
        assert_eq!(r1124_binance_depth_stream("BTC",true),
                   "btcusdt@depth20@500ms");
        assert_eq!(r156_binance_combined_url(&[
            r1124_binance_depth_stream("ETH",true)],false),
            "wss://fstream.binance.com/public/stream?streams=ethusdt@depth20@500ms");
    }
    #[test]
    fn r159_default_and_optin_depth_plus_market_use_supported_endpoints(){
        let depth=vec!["btcusdt@depth20@100ms".to_owned()];
        let default=r156_binance_combined_url(&depth,false);
        assert_eq!(default,
            "wss://fstream.binance.com/public/stream?streams=btcusdt@depth20@100ms");
        let optin=r156_binance_combined_url(&depth,true);
        assert_eq!(optin,default);
        let ticker=r158_binance_ticker_url(&["BTC".into()]);
        assert_eq!(ticker,
          "wss://fstream.binance.com/market/stream?streams=btcusdt@ticker");
        assert!(!optin.contains("@ticker"));
        assert!(!ticker.contains("@depth"));
        assert!(!default.contains("fstream.binance.com/stream?streams="));
    }
    #[test]
    fn r155_source_checkpoint_strict_path_no_parent_traversal(){
        let root="/mnt/spotarb_db_test/astra_p1_06_config_audit_20261018/r155_review_checkpoints";
        let good=format!("{root}/session_001.json");
        assert_eq!(r155_checkpoint_path(&good).unwrap(),PathBuf::from(&good));
        assert!(r155_checkpoint_path(&format!("{root}/../escape.json")).is_err());
        assert!(r155_checkpoint_path(&format!("{root}/nested/file.json")).is_err());
        assert!(r155_checkpoint_path(&format!("{root}/.hidden.json")).is_err());
        assert!(r155_checkpoint_path("/home/tmpadmin/spotbot/data/real_kill_switch.json").is_err());
    }
    #[test]
    fn r155_operator_source_with_unsupported_analytics_is_refused(){
        let c=NativeTradingConfig::parse(
            include_str!("../../config/native_trading_operator_baseline_r64.yaml")).unwrap();
        assert!(SupervisedStrategyPaper::new(c,wall_ms()).is_err());
    }
    #[test]
    fn r154_combined_public_binance_ticker_uses_usdm_quote_not_depth(){
        let bs=vec!["BTC".to_owned()];
        let raw=json!({"stream":"btcusdt@ticker","data":{
            "e":"24hrTicker","E":1791783600000_i64,"s":"BTCUSDT",
            "q":"250000","v":"3","O":1791697200000_i64,
            "C":1791783600000_i64,"st":1,
        }}).to_string();
        let (base,body)=r154_binance_ticker(&raw,&bs).unwrap();
        assert_eq!(base,"BTC");
        assert_eq!(serde_json::from_str::<Value>(&body).unwrap()["q"],"250000");
        assert!(r154_binance_ticker(
            &raw.replace("@ticker","@depth20@100ms"),&bs).is_none());
        assert!(r154_binance_ticker(&raw.replace("\"st\":1",
            "\"st\":2"),&bs).is_none());
        assert!(r154_binance_ticker(&raw,&["ETH".into()]).is_none());
        assert!(r154_binance_ticker(&raw.replace("BTCUSDT","ETHUSDT"),&bs).is_none());
    }
    #[test]
    fn r154_okx_ticker_route_never_becomes_book_frame(){
        let bs=vec!["BTC".to_owned()];
        let raw=json!({"arg":{"channel":"tickers","instId":"BTC-USDT"},
            "data":[{"instType":"SPOT","instId":"BTC-USDT",
                "volCcy24h":"300000","ts":"1791783600000"}]}).to_string();
        assert!(r154_okx_ticker(&raw,&bs).is_some());
        assert!(r154_okx_ticker(
            &raw.replace("tickers","books5"),&bs).is_none());
        assert!(r154_okx_ticker(
            &raw.replace("SPOT","SWAP"),&bs).is_none());
        assert!(r154_okx_ticker(&raw,&["ETH".into()]).is_none());
    }
    #[test]
    fn applied_config_fingerprint_records_true_decision_source(){
        let legacy=paper_config_evidence(&cfg(),"COMPILED_FALLBACK",None);
        assert_eq!(legacy["source"],"COMPILED_FALLBACK");
        assert_eq!(legacy["source_yaml_sha256"],Value::Null);
        assert_eq!(legacy["effective_decision_config"]["max_price"],"UNBOUNDED");
        assert_eq!(legacy["real_order_authority"],false);
        assert_eq!(legacy["decision_and_model_parity_at_start"],true);
        assert_eq!(legacy["paper_model_assumptions"]["quote_budget"],10.0);
        assert_eq!(legacy["paper_model_assumptions"]["min_entry_net_bps"],42.0);
        assert_eq!(legacy["paper_model_assumptions"]["other_cost_bps"],24.03);
        assert_eq!(legacy["paper_executable_sha256_independently_attested"],false);
        let mut wrong_model_input=cfg();
        wrong_model_input.cost=22.0;
        let mismatch=paper_config_evidence(&wrong_model_input,
           "COMPILED_FALLBACK",None);
        assert_eq!(mismatch["decision_and_model_parity_at_start"],false);
        let yaml=NativeTradingConfig::parse(
            include_str!("../../config/native_trading_operator_baseline_r64.yaml")).unwrap();
        // Source evidence is independent of runtime quota promotion.
        assert!(verify_paper_portfolio_quota_parity(&yaml).is_err());
        let bridged=bind_public_paper_decision_config(&yaml).unwrap();
        let modern=paper_config_evidence(&bridged,"RUST_YAML_REVIEW_OPT_IN",Some(&yaml.sha256));
        assert_eq!(modern["source"],"RUST_YAML_REVIEW_OPT_IN");
        assert_eq!(modern["source_yaml_sha256"],yaml.sha256);
        assert_eq!(modern["effective_decision_config"]["min_price"],0.03);
        assert_ne!(legacy["decision_config_sha256"],modern["decision_config_sha256"]);
    }
    #[test]
    fn applied_config_fingerprint_is_stable_across_blacklist_order(){
        let mut a=cfg();a.blacklist.insert("ZEC".into());a.blacklist.insert("ONE".into());
        let mut b=cfg();b.blacklist.insert("ONE".into());b.blacklist.insert("ZEC".into());
        assert_eq!(
            paper_config_evidence(&a,"COMPILED_FALLBACK",None)["decision_config_sha256"],
            paper_config_evidence(&b,"COMPILED_FALLBACK",None)["decision_config_sha256"]
        );
    }
    #[test]
    fn live_guard_denies_engaged_kill_and_denied_preflight() {
        let kill=json!({"schema":"spotbot-real-kill-switch-v1.2d","state":"ENGAGED","operator":"Denis","drill_result":"PASS"});
        let pre=json!({"verdict":"DENY","real_launch_authorized":false,"blocker_codes":["X"]});
        let r=evaluate_safety(&safety_input(&kill,&pre));
        assert!(!r.allowed);assert!(!r.real_launch_authorized);
        assert!(r.blocker_codes.contains(&"BLOCKER_KILL_SWITCH_ENGAGED".into()));
        assert!(r.blocker_codes.contains(&"BLOCKER_PREFLIGHT_DENY".into()));
        assert!(r.blocker_codes.contains(&"BLOCKER_OPERATOR_APPROVAL_MISSING".into()));
    }
    #[test]
    fn missing_safety_files_fail_closed() {
        let r=evaluate_safety(&safety_input(&Value::Null,&Value::Null));
        assert!(!r.allowed);assert!(!r.real_launch_authorized);
        assert!(r.blocker_codes.contains(&"BLOCKER_KILL_SWITCH_MISSING".into()));
        assert!(r.blocker_codes.contains(&"BLOCKER_PREFLIGHT_MISSING".into()));
    }
    #[test]
    fn per_session_graceful_stop_is_exact_and_fails_closed_on_wrong_identity(){
        let root=std::env::temp_dir().join(format!(
           "paper_graceful_{}_{}",std::process::id(),monotonic_ns()));
        fs::create_dir_all(&root).unwrap();
        let path=root.join("stop.json");
        assert!(!graceful_stop_requested(&path,"session_1").unwrap());
        fs::write(&path,serde_json::to_vec(&json!({
           "schema":"SPOTARB/PAPER/GRACEFUL-STOP/V1",
           "session":"session_1","scope":"public-paper-only",
           "real_trading":false})).unwrap()).unwrap();
        assert!(graceful_stop_requested(&path,"session_1").unwrap());
        assert!(graceful_stop_requested(&path,"session_2").is_err());
        fs::write(&path,b"{bad").unwrap();
        assert!(graceful_stop_requested(&path,"session_1").is_err());
        fs::write(&path,vec![b'a';4097]).unwrap();
        assert!(graceful_stop_requested(&path,"session_1").is_err());
        fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn in_memory_rehearsal_has_no_durable_or_network_step() {
        let mut c=Coordinator::new(1);c.prepare_spot(1).unwrap();
        assert_eq!(c.state,CoordinatorState::SpotPending);
    }
    #[test]
    fn r1106_disconnect_evicts_every_old_epoch_book_in_affected_shard(){
        let mut binance=["BTC","ETH","USDC"].iter()
            .map(|s|s.to_string()).collect::<HashSet<_>>();
        let to_reset=vec!["BTC".to_owned(),"ETH".to_owned()];
        assert_eq!(r1106_clear_stale_coverage(&mut binance,&to_reset),2);
        assert!(!binance.contains("BTC")&&!binance.contains("ETH"));
        assert!(binance.contains("USDC"));
        assert_eq!(r1106_clear_stale_coverage(&mut binance,&to_reset),0);
        // Only a NEW observed frame after reset may satisfy coverage again.
        binance.insert("BTC".into());
        assert!(!binance.contains("ETH"));
        assert!(binance.contains("USDC"));
    }
    #[test]
    fn r1106_disconnect_cannot_reuse_pre_reset_cross_venue_coverage(){
        let mut binance=["BTC","ETH"].iter().map(|x|x.to_string())
            .collect::<HashSet<_>>();
        let okx=binance.clone();
        let bases=vec!["BTC".into(),"ETH".into()];
        assert_eq!(r1106_clear_stale_coverage(&mut binance,&bases),2);
        assert!(binance.is_empty());
        assert_eq!(okx.len(),2,"other venue never fake-reset");
        assert_ne!(binance.len(),bases.len(),"pre-epoch 2/2 must not pass");
    }
    #[tokio::test]
    async fn r1187_overload_fails_closed_without_claiming_ack(){
        let (tx,mut rx)=mpsc::channel(1);
        let fatal=AtomicBool::new(false);
        let f=RawFrame{source:None,venue:Venue::BinanceFutures,epoch:1,
            received_ns:1,received_wall_ms:1,raw:"first".into()};
        assert!(r1187_send_frame(&tx,f.clone(),&fatal,true,Duration::from_millis(20)).await.is_ok());
        assert_eq!(r1187_send_frame(&tx,f,&fatal,true,Duration::from_millis(20)).await,
            Err("binance_ingest_send_timeout_fail_closed"));
        assert!(fatal.load(Ordering::SeqCst));
        assert!(matches!(rx.try_recv(),Ok(Event::Frame(_))));
        assert!(rx.try_recv().is_err());
    }
    #[tokio::test]
    async fn r1187_healthy_delivery_is_unchanged(){
        let (tx,mut rx)=mpsc::channel(1);
        let fatal=AtomicBool::new(false);
        let f=RawFrame{source:None,venue:Venue::BinanceFutures,epoch:1,
            received_ns:1,received_wall_ms:1,raw:"healthy".into()};
        assert!(r1187_send_frame(&tx,f,&fatal,true,Duration::from_millis(20)).await.is_ok());
        assert!(!fatal.load(Ordering::SeqCst));
        assert!(matches!(rx.recv().await,Some(Event::Frame(_))));
    }
    #[test]
    fn r1109_binance_never_hot_retries_or_reconnects_unbounded(){
        assert_eq!(r1109_binance_cooldown(0),None,
            "no retry without preceding disconnect");
        assert_eq!(r1109_binance_cooldown(1),Some(Duration::from_secs(30)));
        assert_eq!(r1109_binance_cooldown(2),Some(Duration::from_secs(60)));
        assert_eq!(r1109_binance_cooldown(3),None);
        assert_eq!(r1109_binance_cooldown(u64::MAX),None);
    }
    #[test]
    fn r1109_does_not_allow_cooldown_beyond_bounded_trial(){
        let remaining=Duration::from_secs(25);
        assert!(r1109_binance_cooldown(1).unwrap()>=remaining);
        assert!(r1109_binance_cooldown(2).unwrap()>=remaining);
        // This test performs no network I/O and no authorized exchange call.
    }
}
#[cfg(test)]
mod r1246_cohort_selection_tests{
    use super::*;
    #[test]
    fn r1266_soak_admits_only_exact_explicit_isolated_three_hours(){
        assert!(r1266_exact_soak_gate(10_800,true,false,false));
        assert!(r1266_preflight_gate(60,true,false,false));
        assert!(!r1266_preflight_gate(59,true,false,false));
        assert!(!r1266_preflight_gate(61,true,false,false));
        assert!(!r1266_preflight_gate(60,false,false,false));
        assert!(!r1266_preflight_gate(60,true,true,false));
        assert!(!r1266_preflight_gate(60,true,false,true));
        assert!(!r1266_exact_soak_gate(10_799,true,false,false));
        assert!(!r1266_exact_soak_gate(10_801,true,false,false));
        assert!(!r1266_exact_soak_gate(10_800,false,false,false));
        assert!(!r1266_exact_soak_gate(10_800,true,true,false));
        assert!(!r1266_exact_soak_gate(10_800,true,false,true));
    }
    fn available()->Vec<String>{
        (0..40).map(|i|format!("BASE{i:02}")).collect()
    }
    fn check(raw:&str,available:&[String])->Result<Vec<String>,String>{
        let path=std::env::temp_dir().join(format!(
            "r1246-cohort-{}-{}.txt",std::process::id(),monotonic_ns()));
        fs::write(&path,raw).unwrap();
        let result=r1246_read_cohort(&path,available);
        let _=fs::remove_file(path);
        result
    }
    #[test]
    fn r1266_cohort_requires_eight_unique_current_metadata_members(){
        let available=available();
        let exact=available[..8].join("\n");
        let path=std::env::temp_dir().join(format!(
            "r1266-cohort-{}-{}.txt",std::process::id(),monotonic_ns()));
        fs::write(&path,&exact).unwrap();
        assert_eq!(r1266_read_cohort(&path,&available).unwrap(),available[..8]);
        assert!(r1266_read_cohort(&path,&available[..7]).is_err());
        fs::write(&path,available[..7].join("\n")).unwrap();
        assert!(r1266_read_cohort(&path,&available).is_err());
        let mut duplicate=available[..8].to_vec();
        duplicate[7]=duplicate[0].clone();
        fs::write(&path,duplicate.join("\n")).unwrap();
        assert!(r1266_read_cohort(&path,&available).is_err());
        let _=fs::remove_file(path);
    }
    #[test]
    fn accepts_exact_unique_metadata_member_cohort(){
        let available=available();
        let raw=available.join("\n");
        let selected=check(&raw,&available).unwrap();
        assert_eq!(selected,available);
    }
    #[test]
    fn rejects_wrong_width_duplicate_unknown_and_invalid_base(){
        let available=available();
        let short=available[..39].join("\n");
        assert!(check(&short,&available).is_err());
        let mut duplicate=available.clone();
        duplicate[39]=duplicate[0].clone();
        assert!(check(&duplicate.join("\n"),&available).is_err());
        let mut unknown=available.clone();
        unknown[0]="NOT_LISTED".into();
        assert!(check(&unknown.join("\n"),&available).is_err());
        let mut invalid=available.clone();
        invalid[0]="lowercase".into();
        assert!(check(&invalid.join("\n"),&available).is_err());
    }
}
