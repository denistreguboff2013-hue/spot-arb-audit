//! R1078: cross-EKB-day sealed positions + PAPER YAML, READ ONLY.
//! Verifies exact previous model, only in-memory clone/clock advance,
//! no live quote decisions, exchange/private writes, or systemd activation.
use serde_json::{json,Value};
use std::{fs,path::Path,process::{Command,exit}};
use spotarb_native_market_core::{
 native_paper_continuity_checkpoint_review::
    r1078_stopped_cross_day_quarantine_preview,
 native_trading_config::NativeTradingConfig,
 native_paper_config_bridge::{admit_public_paper_yaml_review,
  bind_public_paper_temporal_policy_review,verify_paper_portfolio_quota_parity},
 transport::monotonic_ns,
 decision::{DecisionCore,DecisionConfig,RouteSpec,Limits},
 Book,Venue
};
use std::collections::{HashMap,HashSet};
const PAPER:&str="/mnt/spotarb_db_test/astra_paper_continuous_v1";
const ARCHIVE:&str="/mnt/spotarb_db_test/astra_p001_r874_test_scratch_20261025/r1061_sealed_stopped_paper_20261025";
const RECEIPT:&str="/mnt/spotarb_db_test/astra_collector_original_shadow_v1/privileged_stop_receipt.json";
const YAML_SHA:&str="38a71a9c457c00c686c6f7c4c808d05cfe37e43cc760fc8591eac960d101ec4a";
fn fail(s:&str)->!{eprintln!("R1078 STOPPED QUARANTINE DENIED {s}");exit(70)}
fn stopped(){
 let p=Command::new("/usr/bin/systemctl").args(
   ["show","spotarb-native-paper-continuous.service","-p","ActiveState",
    "-p","MainPID","-p","UnitFileState"]).output()
   .unwrap_or_else(|_|fail("cannot read PAPER service"));
 let o=String::from_utf8_lossy(&p.stdout);
 if !p.status.success()||!o.lines().any(|x|x=="ActiveState=inactive")||
   !o.lines().any(|x|x=="MainPID=0")||
   !o.lines().any(|x|x=="UnitFileState=disabled"){
   fail("PAPER must remain inactive, PID 0 and disabled")
 }
 let kill:Value=serde_json::from_slice(
  &fs::read("/home/tmpadmin/spotbot/data/real_kill_switch.json")
  .unwrap_or_else(|_|fail("kill unreadable")))
   .unwrap_or_else(|_|fail("kill invalid"));
 if kill["state"]!="ENGAGED"{fail("REAL kill not engaged")}
}
fn main(){
 if std::env::args_os().len()!=1{fail("no arguments authorized")}
 stopped();
 let path=Path::new(env!("CARGO_MANIFEST_DIR"))
  .join("config/native_trading_paper_only_r1076.yaml");
 let cfg=NativeTradingConfig::load(&path)
   .unwrap_or_else(|_|fail("cannot load PAPER YAML"));
 if cfg.sha256!=YAML_SHA{fail("candidate hash changed")}
 admit_public_paper_yaml_review(&cfg)
   .unwrap_or_else(|_|fail("PAPER YAML admission denied"));
 verify_paper_portfolio_quota_parity(&cfg)
   .unwrap_or_else(|_|fail("PAPER quota parity mismatch"));
 let t=bind_public_paper_temporal_policy_review(&cfg)
   .unwrap_or_else(|_|fail("PAPER temporal binding refused"));
 let checkpoint=Path::new(PAPER).join("portfolio_checkpoint.json");
 let original_sha=openssl::sha::sha256(&fs::read(&checkpoint)
   .unwrap_or_else(|_|fail("checkpoint unreadable")));
 let mut restored=r1078_stopped_cross_day_quarantine_preview(
  &checkpoint,Path::new(PAPER),Path::new(ARCHIVE),
  Path::new(RECEIPT),monotonic_ns())
  .unwrap_or_else(|e|fail(&format!("cross-day archive proof {e:?}")));
 restored.portfolio.verify_public_paper_temporal_policy_review(
   t.min_lifetime_ns,t.exit_spread_bps,t.reentry_cooldown_ns,
   t.net_target_bps).unwrap_or_else(|_|fail("frozen temporal policy drift"));
 restored.portfolio.verify_source_paper_quotas_review(
    cfg.policy.paper_daily_trades,cfg.policy.general_max_positions as usize)
  .unwrap_or_else(|_|fail("frozen quota drift"));
 if !restored.portfolio.cold_boot_unknown||
   restored.portfolio.stats.entries!=37||
   restored.portfolio.stats.closed!=34||
   restored.carried_open_positions!=3{
   fail("quarantine model is wrong")
 }
 let original_bases=restored.portfolio.positions.keys().cloned().collect::<Vec<_>>();
 if original_bases!=vec!["DOS","PHA","WLD"]{fail("frozen symbols changed")}
 // This explicitly exercises on_frame with NO books or decisions: exact
 // close-only research clone remains unchanged, no account/event action.
 let before=serde_json::to_vec(&restored.portfolio)
   .unwrap_or_else(|_|fail("cannot serialize frozen positions"));
 let now_ns=monotonic_ns();
 let now_ms=std::time::SystemTime::now()
  .duration_since(std::time::UNIX_EPOCH)
  .unwrap_or_else(|_|fail("wall clock")).as_millis() as i64;
 for base in &original_bases{
  restored.portfolio.on_frame(base,Venue::OkxSpot,
       now_ns,now_ms,None,None,None,None)
   .unwrap_or_else(|_|fail("in-memory empty public frame refused"));
 }
 let after=serde_json::to_vec(&restored.portfolio)
   .unwrap_or_else(|_|fail("cannot serialize checked in-memory model"));
 if before!=after{fail("empty book call mutated cold model")}
 // R1080: a correctly formed, profitable-looking NEW entry proposal
 // must still be denied by the deserialized cold_boot_unknown guard.
 // This deliberately mutates ONLY an in-memory clone's denied counter.
 let base="NEWT";
 let route=RouteSpec{
   buy_exchange:"okx_spot".into(),sell_exchange:"binance_futures".into(),
   label:"Spot-Future".into(),
   buy:Limits{min_amount:"0.001".into(),step:"0.001".into(),
    min_cost:"0".into(),max_amount:None},
   sell:Limits{min_amount:"0.001".into(),step:"0.001".into(),
    min_cost:"5".into(),max_amount:None},
   loaded_ns:now_ns,metadata_state:"SIZE_METADATA_READY".into(),
 };
 let config=DecisionConfig{
   max_age_ms:800,metadata_ttl_ms:43_200_000,base_size:10.0,
   risk_cap:10.0,cost:24.03,required_net:42.0,min_price:0.0,
   max_price:f64::INFINITY,blacklist:HashSet::new(),
 };
 let mut core=DecisionCore::new(0,1,config,HashMap::new())
  .unwrap_or_else(|_|fail("R1080 core refused"));
 core.replace_routes(1,HashMap::from([(base.to_owned(),vec![route.clone()])]))
  .unwrap_or_else(|_|fail("R1080 routes refused"));
 let mkbook=|ns,bid,ask|Book{bids:vec![(bid,100.0)],
    asks:vec![(ask,100.0)],received_ns:ns,received_wall_ms:now_ms,
    exchange_ts_ms:Some(now_ms),changed_ns:ns,epoch:1,
    sequence:Some(ns as i64)};
 let spot=mkbook(now_ns,99.9,100.0);
 let future=mkbook(now_ns+1,101.0,101.1);
 core.update_at(base,"okx_spot",spot.clone(),now_ns,now_ms)
  .unwrap_or_else(|_|fail("R1080 spot ingestion refused"));
 let proposed=core.update_at(base,"binance_futures",future.clone(),
     now_ns+1,now_ms)
  .unwrap_or_else(|_|fail("R1080 future ingestion refused"));
 let opportunity=proposed.iter().find(|x|x.reason=="EXECUTABLE_OPPORTUNITY")
  .unwrap_or_else(||fail("R1080 no valid executable proposed entry"));
 let positions_before=serde_json::to_vec(&restored.portfolio.positions)
  .unwrap_or_else(|_|fail("R1080 cannot copy phases"));
 let entries_before=restored.portfolio.stats.entries;
 let rejects_before=restored.portfolio.stats.denied_entry_guard;
 restored.portfolio.on_frame(base,Venue::BinanceFutures,
  now_ns+2,now_ms,Some(&spot),Some(&future),Some(opportunity),Some(&route))
  .unwrap_or_else(|_|fail("R1080 in-memory entry gate failed"));
 if restored.portfolio.stats.entries!=entries_before||
    restored.portfolio.stats.denied_entry_guard!=rejects_before+1||
    serde_json::to_vec(&restored.portfolio.positions)
      .unwrap_or_else(|_|fail("R1080 phase reserialize failed"))!=positions_before{
    fail("R1080 valid new entry altered cold restored position model")
 }
 let sha=openssl::sha::sha256(&fs::read(&checkpoint)
   .unwrap_or_else(|_|fail("checkpoint disappeared")));
 if sha!=original_sha{fail("source checkpoint changed")}
 println!("{}",json!({
  "schema":"SPOTARB/R1078/CROSS-DAY-QUARANTINE/OFFLINE/V1",
  "ok":true,"source_day":restored.previous_day,
  "in_memory_day":restored.portfolio.day_yek,
  "day_advanced_in_memory_only":true,
  "historic_counters_not_reset":true,
  "open_bases":original_bases,"open_count":3,
  "entries":37,"closed":34,"checkpoint_revision":restored.previous_revision,
  "new_entries_permitted":false,"cold_boot_unknown":true,
  "original_book_frame_actions":0,
  "model_after_empty_frames_unchanged":true,
  "valid_synthetic_entry_proposed":true,
  "valid_synthetic_entry_denied_due_to_cold_boot":true,
  "existing_three_model_phases_unchanged":true,
  "original_checkpoint_bytes_unchanged":true,
  "real_orders":false,"paper_trading_deployed":false,
  "close_only_live_execution_authorized":false,
  "fee_funding_reconciliation_complete":false,
  "telegram_reconciliation_complete":false,
  "candidate_yaml_sha256":cfg.sha256
 }))
}
