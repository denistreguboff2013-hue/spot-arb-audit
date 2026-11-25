//! R1082: genuine two-OS-process normal PAPER virtual portfolio stop/start.
//! Fully SYNTHETIC local PAPER states; no installed service, no REAL/private API.
use spotarb_native_market_core::{
 native_paper_continuity_checkpoint_review::{save_checkpoint,load_planned_handoff,
   inspect_checkpoint_model_alignment},
 native_paper_portfolio_review::PaperPortfolio,
 native_paper_market_execution_review::{PaperAssumptions,PaperMachine,PaperPhase,PaperPosition},
 native_virtual_four_leg_provenance_review::ModeledFourLegJournal,
 transport::monotonic_ns,
};
use std::{env,fs,path::Path,process};
use serde_json::json;
fn fail(s:&str)->!{eprintln!("R1082_DENIED {s}");process::exit(70)}
fn main(){
 let a=env::args().collect::<Vec<_>>();
 if a.len()!=3 || env::var("ASTRA_R1082_PRIVATE_ONLY").as_deref()!=Ok("YES"){
  fail("explicit private mode and path required")
 }
 let base=Path::new(&a[2]);
 if !base.to_string_lossy().starts_with(
    "/mnt/spotarb_db_test/astra_p001_r874_test_scratch_20261025/astra_r1082_private_"){
    fail("not an R1082 isolated test path")
 }
 let md=fs::symlink_metadata(base).unwrap_or_else(|_|fail("private root missing"));
 use std::os::unix::fs::{MetadataExt,PermissionsExt};
 if !md.file_type().is_dir()||md.permissions().mode()&0o077!=0||
   md.uid()!=unsafe{libc::geteuid()} {fail("unsafe private root")}
 let k:serde_json::Value=serde_json::from_slice(
    &fs::read("/home/tmpadmin/spotbot/data/real_kill_switch.json")
     .unwrap_or_else(|_|fail("REAL kill not available")))
    .unwrap_or_else(|_|fail("REAL kill corrupted"));
 if k["state"]!="ENGAGED"{fail("REAL kill not engaged")}
 let checkpoint=base.join("paper_checkpoint.json");
 let journal=base.join("modeled_four_leg.jsonl");
 let session="session_1082_normal";
 match a[1].as_str(){
 "--seal"=>{
   if checkpoint.exists()||journal.exists(){fail("source session already exists")}
   let wall=std::time::SystemTime::now().duration_since(
       std::time::UNIX_EPOCH).unwrap().as_millis() as i64;
   let mut p=PaperPortfolio::new(wall).unwrap_or_else(|_|fail("portfolio new"));
   // Mirror only the ORIGINAL STOPPED COUNTERS for a synthetic copy.
   // Values are not broker PnL attestation or reused position balances.
   p.stats.entries=37;
   p.stats.closed=34;
   p.stats.closed_wins=27;
   p.stats.closed_losses=7;
   p.stats.futures_hedges=37;
   p.stats.futures_covers=34;
   p.stats.modeled_realized_usdt=1.6578974787089982;
   let mono=monotonic_ns();
   let mut t=mono.checked_sub(20_000_000).unwrap_or_else(||fail("bad mono"));
   for base in ["DOS","PHA","WLD"]{
    let mut machine=PaperMachine::new(PaperAssumptions::default())
      .unwrap_or_else(|_|fail("paper machine new"));
    machine.phase=PaperPhase::Hedged(PaperPosition{
       base:base.into(),signal_ns:t,spot_buy_ns:t+1,
       signal_estimated_net_bps:60.0,spot_gross_base:1.0,
       spot_delivered_base:1.0,spot_paid_quote:10.0,
       futures_short_ns:Some(t+2),
       futures_short_proceeds_quote:Some(10.2),
       futures_short_base:Some(1.0),
       hedge_entry_estimated_net_bps:Some(60.0),
    });
    p.positions.insert(base.into(),machine);
    t=t.checked_add(3).unwrap_or_else(||fail("mono overflow"));
   }
   let mut j=ModeledFourLegJournal::start(&journal,session,&p,0)
      .unwrap_or_else(|_|fail("modeled journal init"));
   save_checkpoint(&checkpoint,&p,1,monotonic_ns(),false)
      .unwrap_or_else(|_|fail("unsealed checkpoint creation"));
   save_checkpoint(&checkpoint,&p,2,monotonic_ns(),true)
      .unwrap_or_else(|_|fail("checkpoint seal"));
   j.seal(&p).unwrap_or_else(|_|fail("modeled four-leg journal seal"));
   inspect_checkpoint_model_alignment(&checkpoint,&journal,session,true)
      .unwrap_or_else(|_|fail("sealed cross-file parity"));
   println!("{}",json!({"schema":"R1082/NORMAL-STOP/V1","ok":true,
      "synthetic_only":true,"revision":2,"entries":37,"closed":34,
      "open":3,"bases":["DOS","PHA","WLD"],"real_orders":false}));
 },
 "--restore"=>{
   let saved=fs::read(&checkpoint).unwrap_or_else(|_|fail("checkpoint absent"));
   let before=openssl::sha::sha256(&saved);
   inspect_checkpoint_model_alignment(&checkpoint,&journal,session,true)
      .unwrap_or_else(|_|fail("saved modeled checkpoint vs journal mismatch"));
   let restored=load_planned_handoff(&checkpoint,monotonic_ns(),37)
      .unwrap_or_else(|_|fail("normal planned restart refused"));
   if restored.previous_revision!=2||restored.previous_day!=restored.portfolio.day_yek||
      restored.carried_open_positions!=3||restored.portfolio.stats.entries!=37||
      restored.portfolio.stats.closed!=34||!restored.portfolio.cold_boot_unknown {
     fail("restored virtual portfolio identity/guards differ")
   }
   let bases=restored.portfolio.positions.keys().cloned().collect::<Vec<_>>();
   if bases!=["DOS","PHA","WLD"]{fail("lost or reordered modeled positions")}
   inspect_checkpoint_model_alignment(&checkpoint,&journal,session,true)
      .unwrap_or_else(|_|fail("post-restore parity mismatch"));
   if openssl::sha::sha256(&fs::read(&checkpoint).unwrap())!=before{
     fail("original sealed checkpoint mutated on restore")
   }
   println!("{}",json!({"schema":"R1082/NORMAL-START/V1","ok":true,
     "original_revision":2,"previous_day_matches":true,
     "positions_recovered":3,"modeled_entries":37,
     "modeled_closed":34,"bases":bases,
     "original_checkpoint_unchanged":true,
     "modeled_four_leg_journal_verified":true,
     "cold_new_entries_disabled":true,
     "no_real_orders":true,"no_production_writes":true}));
 },
 _=>fail("unknown mode")
 }
}
