//! R1073: stopped PAPER source YAML admission versus sealed virtual positions.
//! STRICTLY read-only audit. Never enables REAL, restarts PAPER or mutates config.
use serde_json::{json,Value};
use std::{fs,path::Path,process::{Command,exit}};
use spotarb_native_market_core::{
 native_trading_config::NativeTradingConfig,
 native_paper_config_bridge::{admit_public_paper_yaml_review,
    bind_public_paper_temporal_policy_review},
 native_paper_portfolio_review::PaperPortfolio,
};
const ROOT:&str="/mnt/spotarb_db_test/astra_paper_continuous_v1";
fn fail(reason:&str)->!{eprintln!("R1073 DENIED {reason}");exit(70)}
fn state(key:&str,expected:&str){
 let p=Command::new("/usr/bin/systemctl")
    .args(["show","spotarb-native-paper-continuous.service","-p",key])
    .output().unwrap_or_else(|_|fail("cannot inspect PAPER systemd"));
 if !p.status.success()||p.stdout!=format!("{key}={expected}\n").as_bytes(){
  fail("installed PAPER must remain stopped and disabled")
 }
}
fn main(){
 if std::env::args_os().len()!=1{fail("no runtime flags permitted")}
 state("ActiveState","inactive");state("UnitFileState","disabled");state("MainPID","0");
 let kill:Value=serde_json::from_slice(&fs::read(
   "/home/tmpadmin/spotbot/data/real_kill_switch.json")
   .unwrap_or_else(|_|fail("kill state missing")))
   .unwrap_or_else(|_|fail("kill JSON malformed"));
 if kill["state"]!="ENGAGED"{fail("REAL kill not engaged")}
 let config=NativeTradingConfig::load(&Path::new(env!("CARGO_MANIFEST_DIR"))
   .join("config/native_trading.yaml"))
   .unwrap_or_else(|_|fail("source YAML cannot be parsed"));
 let admitted=admit_public_paper_yaml_review(&config).is_ok();
 let temporal=bind_public_paper_temporal_policy_review(&config);
 let checkpoint:Value=serde_json::from_slice(&fs::read(
   Path::new(ROOT).join("portfolio_checkpoint.json")
 ).unwrap_or_else(|_|fail("sealed checkpoint not readable")))
  .unwrap_or_else(|_|fail("checkpoint JSON corrupted"));
 if checkpoint["payload"]["completed"]!=true{
  fail("checkpoint is not sealed")
 }
 let paper:PaperPortfolio=serde_json::from_value(
   checkpoint["payload"]["portfolio"].clone())
   .unwrap_or_else(|_|fail("typed stopped portfolio invalid"));
 let temporal_match=temporal.as_ref().is_ok_and(|t|
   paper.verify_public_paper_temporal_policy_review(
     t.min_lifetime_ns,t.exit_spread_bps,
     t.reentry_cooldown_ns,t.net_target_bps).is_ok());
 let policy_general=&config.raw["general"];
 let trial=&config.raw["real_trial"];
 let real_flags=json!({
  "general_trade_enabled":policy_general["trade_enabled"],
  "real_trial_enabled":trial["enabled"],
  "real_trial_autonomous_real_enabled":trial["autonomous_real_enabled"]
 });
 let bases=paper.positions.keys().cloned().collect::<Vec<_>>();
 println!("{}",json!({
  "schema":"SPOTARB/R1073/READONLY-STOPPED-YAML-ADMISSION/V1",
  "source_yaml_sha256":config.sha256,
  "source_yaml_paper_admitted":admitted,
  "frozen_temporal_source_binding_valid":temporal.is_ok(),
  "stopped_portfolio_temporal_parameters_match":temporal_match,
  "source_yaml_real_flags":real_flags,
  "stopped_model_open":bases.len(),"stopped_model_bases":bases,
  "stopped_model_entries":paper.stats.entries,
  "checkpoint_revision":checkpoint["payload"]["revision"],
  "ready_to_deploy_unified_paper":false,
  "public_paper_running":false,"real_orders_enabled":false,
  "changes_made":false,
  "warning":"YAML admission does not assert all 160 fields are wired"
 }));
}
