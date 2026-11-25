//! R1077: versioned PAPER-only YAML candidate; independent Rust admission.
//! Offline-only; service MUST be stopped, REAL kill engaged, no IO mutation.
use std::{fs,path::Path,process::{Command,exit}};
use serde_json::{json,Value};
use spotarb_native_market_core::{
 native_trading_config::NativeTradingConfig,
 native_paper_config_bridge::{admit_public_paper_yaml_review,
   bind_public_paper_temporal_policy_review,verify_paper_portfolio_quota_parity},
 native_paper_portfolio_review::PaperPortfolio,
};
const CANDIDATE_SHA:&str="38a71a9c457c00c686c6f7c4c808d05cfe37e43cc760fc8591eac960d101ec4a";
const SOURCE_SHA:&str="b3b8a760387886f297668a1791ab1cc358386aeb3c82550f8df6bf151bbbe437";
fn denied(s:&str)->!{eprintln!("R1077 PAPER CANDIDATE DENIED: {s}");exit(70)}
fn sha(path:&Path)->String{
 openssl::sha::sha256(&fs::read(path).unwrap_or_else(|_|denied("read yaml")))
  .iter().map(|b|format!("{b:02x}")).collect()
}
fn main(){
 if std::env::args_os().len()!=1{denied("no flags accepted")}
 let state=Command::new("/usr/bin/systemctl")
  .args(["show","spotarb-native-paper-continuous.service",
         "-p","ActiveState","-p","UnitFileState","-p","MainPID"])
  .output().unwrap_or_else(|_|denied("systemctl unavailable"));
 let fields=String::from_utf8_lossy(&state.stdout);
 if !state.status.success()||
   !fields.contains("ActiveState=inactive\n")||
   !fields.contains("UnitFileState=disabled\n")||
   !fields.contains("MainPID=0\n") {denied("PAPER service not stopped")}
 let kill:Value=serde_json::from_slice(
   &fs::read("/home/tmpadmin/spotbot/data/real_kill_switch.json")
    .unwrap_or_else(|_|denied("REAL kill missing")))
    .unwrap_or_else(|_|denied("REAL kill malformed"));
 if kill["state"]!="ENGAGED"{denied("REAL kill not engaged")}
 let dir=Path::new(env!("CARGO_MANIFEST_DIR"));
 let source=dir.join("config/native_trading.yaml");
 let candidate=dir.join("config/native_trading_paper_only_r1076.yaml");
 if sha(&source)!=SOURCE_SHA||sha(&candidate)!=CANDIDATE_SHA{
  denied("source or candidate hash changed since R1076")
 }
 let cfg=NativeTradingConfig::load(&candidate)
   .unwrap_or_else(|_|denied("candidate YAML load refused"));
 let decisions=admit_public_paper_yaml_review(&cfg)
   .unwrap_or_else(|e|denied(&format!("Rust admission refused {e:?}")));
 verify_paper_portfolio_quota_parity(&cfg)
   .unwrap_or_else(|_|denied("PAPER quota mismatch"));
 let temporal=bind_public_paper_temporal_policy_review(&cfg)
   .unwrap_or_else(|_|denied("PAPER temporal policy refused"));
 let checkpoint:Value=serde_json::from_slice(
   &fs::read("/mnt/spotarb_db_test/astra_paper_continuous_v1/portfolio_checkpoint.json")
    .unwrap_or_else(|_|denied("checkpoint missing")))
    .unwrap_or_else(|_|denied("checkpoint malformed"));
 if checkpoint["payload"]["completed"]!=true{denied("checkpoint unsealed")}
 let paper:PaperPortfolio=serde_json::from_value(
     checkpoint["payload"]["portfolio"].clone())
   .unwrap_or_else(|_|denied("checkpoint paper model invalid"));
 paper.verify_public_paper_temporal_policy_review(
    temporal.min_lifetime_ns,temporal.exit_spread_bps,
    temporal.reentry_cooldown_ns,temporal.net_target_bps)
   .unwrap_or_else(|_|denied("CANDIDATE CHANGES OLD PAPER EXIT ECONOMICS"));
 if paper.positions.len()!=3||paper.stats.entries!=37||
   ["DOS","PHA","WLD"].iter().any(|base|!paper.positions.contains_key(*base)){
    denied("frozen portfolio has changed")
 }
 let g=&cfg.raw["general"];
 let r=&cfg.raw["real_trial"];
 let f=&cfg.raw["futures_emergency_stop"];
 let n=&cfg.raw["native_runtime"];
 if g["trade_enabled"]!=false||
    r["enabled"]!=false||r["operator_approved"]!=false||
    r["autonomous_real_enabled"]!=false||
    r["bootstrap_first_trade"]!=false||
    r["supervised_retrial_enabled"]!=false||
    f["place_in_real"]!=false||
    n["real_execution_authorized"]!=false||
    n["mutation_capability_enabled"]!=false||
    n["production_cutover_authorized"]!=false{
    denied("candidate REAL flag not disabled")
 }
 println!("{}",json!({
    "schema":"SPOTARB/R1077/STOPPED-PAPER-ONLY-CANDIDATE-ADMISSION/V1",
    "ok":true,"rust_paper_yaml_admission":"PASS",
    "source_sha256":SOURCE_SHA,"candidate_sha256":CANDIDATE_SHA,
    "real_flags_verified_off":10,
    "decision_net_bps":decisions.required_net,
    "daily_paper_cap":cfg.policy.paper_daily_trades,
    "concurrent_paper_cap":cfg.policy.general_max_positions,
    "original_virtual_open":paper.positions.len(),
    "original_virtual_entries":paper.stats.entries,
    "cold_checkpoint_revision":checkpoint["payload"]["revision"],
    "original_exit_timing_economics_match":true,
    "installed_paper_running":false,
    "installed_paper_config_activated":false,
    "all_160_fields_runtime_wired":false,
    "real_orders_authorized":false,
    "warning":"CANDIDATE ADMISSION ONLY; no collector/portfolio cold restart certified"
 }))
}
