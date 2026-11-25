//! One-time PAPER-only migration across the pre-raw-checksum Rust supervisor.
//! Must run while the OLD service is STOPPED. Does not contact private APIs,
//! create orders, attest actual fills, or turn model closes into native truth.
use serde_json::{json,Value};
use spotarb_native_market_core::{
    native_paper_continuity_checkpoint_review::load_planned_handoff,
    transport::monotonic_ns,
};
use std::{env,fs::{self,File,OpenOptions},io::{self,Write},
    path::Path,time::{SystemTime,UNIX_EPOCH}};
const ROOT:&str="/mnt/spotarb_db_test/astra_paper_continuous_v1";
const MAX_DAILY:u64=500;
fn err(s:&str)->io::Error{io::Error::new(io::ErrorKind::Other,s.to_owned())}
fn read(p:&Path)->io::Result<Value>{
    serde_json::from_slice(&fs::read(p)?).map_err(|_|err("invalid JSON"))
}
fn now()->u64{SystemTime::now().duration_since(UNIX_EPOCH)
    .unwrap_or_default().as_secs()}
fn day(t:u64)->u64{(t+5*3600)/86_400}
fn atomic(root:&Path,name:&str,v:&Value)->io::Result<()>{
    let dst=root.join(name);
    let temp=root.join(format!("{name}.legacy_pending"));
    let bytes=serde_json::to_vec_pretty(v)?;
    let mut f=OpenOptions::new().write(true).create_new(true).open(&temp)?;
    f.write_all(&bytes)?;f.sync_all()?;drop(f);
    fs::rename(&temp,&dst)?;
    File::open(root)?.sync_all()?;
    Ok(())
}
#[derive(Debug)]
struct Checked{
    day:u64,entries:u64,open:u64,session:String,
    checkpoint_revision:u64,budget_was:u64,
}
fn inspect(root:&Path)->io::Result<Checked>{
    if root.join("active.json").exists(){
        return Err(err("current public paper episode still active"))
    }
    if root.join("planned_handoff.json").exists(){
        return Err(err("prior unconsumed planned handoff exists"))
    }
    let current=read(&root.join("current_session.json"))?;
    let name=current["session"].as_str()
       .ok_or_else(||err("missing current session identity"))?;
    if !name.starts_with("session_")||name.len()>90||
       !name.bytes().all(|v|v.is_ascii_alphanumeric()||v==b'_'){
        return Err(err("malformed session identity"))
    }
    let sess=root.join("sessions").join(name);
    let summary=read(&sess.join("supervisor_summary.json"))?;
    let final_report=read(&sess.join("final.json"))?;
    let day_num=current["day"].as_u64().ok_or_else(||err("missing day"))?;
    let baseline=current["baseline"].as_u64().ok_or_else(||err("missing baseline"))?;
    let reported=final_report["paper_portfolio"]["counters"]["entries"]
         .as_u64().ok_or_else(||err("missing final paper entry count"))?;
    let virtual_open=final_report["paper_portfolio"]["open_positions"]
         .as_u64().ok_or_else(||err("missing open count"))?;
    let old_budget=read(&root.join("daily_budget.json"))?;
    let old_used=old_budget["entries"].as_u64()
         .ok_or_else(||err("missing saved daily budget"))?;
    if day_num!=day(now())||old_budget["day"].as_u64()!=Some(day_num)||
       summary["day"].as_u64()!=Some(day_num)||
       summary["session"].as_str()!=Some(name)||
       summary["started_with_entries"].as_u64()!=Some(baseline)||
       baseline>reported||reported>MAX_DAILY||old_used>MAX_DAILY{
        return Err(err("wrong day or inconsistent budget/session"))
    }
    let exit=summary["exit_code"].as_i64();
    if !matches!(exit,Some(0)|Some(2))||
       summary["killed_for_safety"]!=false||
       summary["real_trading"]!=false||
       final_report["private_order_path_linked"]!=false||
       final_report["trading"]!=false||
       final_report["hard_safety"]["allowed"]!=false||
       final_report["hard_safety"]["real_launch_authorized"]!=false||
       final_report["virtual_paper_continuity"]["checkpoint_completed"]!=true||
       final_report["virtual_paper_continuity"]["real_orders"]!=false{
        return Err(err("source session did not end safely in public PAPER"))
    }
    if summary["finished_with_entries"].as_u64()!=Some(reported){
        // The legacy supervisor can set the budget to 500 only on its
        // invalid re-serialization SHA check. Allow this narrow case,
        // but only if exact source/ledger quarantine statements agree.
        if summary["reason"]!="unrecovered_virtual_positions_paper_entries_quarantined"||
           old_budget["reason"]!="unrecovered_virtual_positions_paper_entries_quarantined"||
           summary["finished_with_entries"].as_u64()!=Some(MAX_DAILY)||
           old_used!=MAX_DAILY||virtual_open==0{
            return Err(err("inconsistent supervisor quarantine budget"))
        }
    }else if old_used!=reported{
        return Err(err("unexpected budget change since completed session"))
    }
    if final_report["paper_portfolio"]["day_yekt"].as_i64()!=Some(day_num as i64){
        return Err(err("final paper EKB day mismatch"))
    }
    let checkpoint=root.join("portfolio_checkpoint.json");
    let restored=load_planned_handoff(&checkpoint,monotonic_ns(),reported)
        .map_err(|_|err("recent completed local checkpoint invalid or stale"))?;
    if restored.previous_day!=day_num as i64||
       restored.portfolio.stats.entries!=reported||
       restored.carried_open_positions as u64!=virtual_open{
        return Err(err("checkpoint vs completed model state diverged"))
    }
    if !root.join("sessions").join(name).is_dir(){
        return Err(err("session no longer exists"))
    }
    Ok(Checked{day:day_num,entries:reported,open:virtual_open,
        session:name.to_owned(),
        checkpoint_revision:restored.previous_revision,budget_was:old_used})
}
fn arm(root:&Path,ck:&Checked)->io::Result<()>{
    // Only a stopped supervisor + separately validated on-disk checkpoint
    // may reach this path. Failures must be quarantined by the wrapper.
    let report=json!({
       "schema":"native-public-paper-legacy-transition-v1",
       "session":ck.session,"day":ck.day,"entries":ck.entries,
       "open_virtual_positions":ck.open,
       "checkpoint_revision":ck.checkpoint_revision,
       "old_ledger_entries":ck.budget_was,
       "real_trade_authority":false,"attested_exchange_fills":false,
       "completed_at_unix_s":now(),
    });
    atomic(root,"migration_rawhash_receipt_20261013.json",&report)?;
    // The new supervisor consumes this marker on the next launch and
    // independently revalidates the exact on-disk sealed PAPER checkpoint.
    atomic(root,"daily_budget.json",&json!({
       "day":ck.day,"entries":ck.entries,"paper_only":true,
       "reason":"validated_same_boot_legacy_handoff",
       "wall_s":now(),
    }))?;
    atomic(root,"planned_handoff.json",&json!({
       "day":ck.day,"entries":ck.entries,"last_session":ck.session,
       "validated_seal":true,"no_real_orders":true,
       "legacy_bridge_single_use":true,
    }))?;
    Ok(())
}
fn quarantine(root:&Path)->io::Result<()>{
    // Used only after confirmed inactive old service. An uncertain boundary
    // burns the remaining modeled entry budget; WS intake stays permitted.
    match fs::remove_file(root.join("planned_handoff.json")){
        Ok(())=>{},
        Err(e) if e.kind()==io::ErrorKind::NotFound=>{},
        Err(e)=>return Err(e),
    }
    atomic(root,"daily_budget.json",&json!({
        "day":day(now()),"entries":MAX_DAILY,
        "paper_only":true,"reason":"uncertain_legacy_handoff_quarantined",
        "wall_s":now(),
    }))?;
    atomic(root,"legacy_migration_quarantine_20261013.json",&json!({
        "day":day(now()),"wall_s":now(),
        "no_model_entries_until_next_day":true,
        "unresolved_virtual_positions_may_exist":true,
        "no_real_orders":true,
    }))?;
    Ok(())
}
fn main()->io::Result<()>{
    let arg=env::args().nth(1).ok_or_else(||err("expected inspect/apply/quarantine"))?;
    if !matches!(arg.as_str(),"inspect"|"apply"|"quarantine"){
        return Err(err("expected inspect/apply/quarantine"))
    }
    let root=Path::new(ROOT);
    if arg=="quarantine"{
        quarantine(root)?;
        println!("LOCAL_PAPER_QUARANTINED entries=500 no_real_orders");
        return Ok(())
    }
    // The wrapper must have STOPPED the old supervisor and verified no
    // unfinished active.json. This tool never stops or starts services.
    let ck=inspect(root)?;
    println!("LOCAL_PAPER_MIGRATION_CHECK {:?}",ck);
    if arg=="apply"{
        arm(root,&ck)?;
        println!("LOCAL_PAPER_MIGRATION_ARMED model_positions={} no_real_orders",ck.open);
    }
    Ok(())
}
#[cfg(test)]
mod tests{
 use super::*;
 #[test]fn unsafe_missing_paths_denied_without_mutating_anything(){
    let root=std::env::temp_dir().join(format!("paper_legacy_migration_{}",
        std::process::id()));
    fs::create_dir_all(&root).unwrap();
    assert!(inspect(&root).is_err());
    assert!(!root.join("planned_handoff.json").exists());
    fs::remove_dir_all(root).unwrap();
 }
 #[test]fn validated_old_supervisor_quarantine_budget_can_be_recovered_on_synthetic_fixture(){
    use spotarb_native_market_core::{
      native_paper_portfolio_review::PaperPortfolio,
      native_paper_market_execution_review::{
        PaperAssumptions,PaperMachine,PaperPhase,PaperPosition,
      },
      native_paper_continuity_checkpoint_review::save_checkpoint,
    };
    let root=std::env::temp_dir().join(format!(
        "paper_legacy_positive_{}_{}",std::process::id(),monotonic_ns()));
    let name=format!("session_{}_{}",day(now()),now());
    let sess=root.join("sessions").join(&name);
    fs::create_dir_all(&sess).unwrap();
    let mut p=PaperPortfolio::new(now() as i64*1000).unwrap();
    p.stats.entries=20;
    let ns=monotonic_ns()-100_000;
    p.positions.insert("ONE".into(),PaperMachine{
      config:PaperAssumptions::default(),
      phase:PaperPhase::AwaitingHedge(PaperPosition{
         base:"ONE".into(),signal_ns:ns,spot_buy_ns:ns+1,
         signal_estimated_net_bps:55.0,
         spot_gross_base:4.0,spot_delivered_base:4.0,
         spot_paid_quote:10.0,
         futures_short_ns:None,futures_short_proceeds_quote:None,
         futures_short_base:None,hedge_entry_estimated_net_bps:None,
      }),
    });
    save_checkpoint(&root.join("portfolio_checkpoint.json"),&p,
        99,monotonic_ns(),true).unwrap();
    fs::write(root.join("current_session.json"),
        serde_json::to_vec(&json!({
           "session":name,"day":day(now()),"baseline":17
        })).unwrap()).unwrap();
    fs::write(sess.join("final.json"),
        serde_json::to_vec(&json!({
           "paper_portfolio":{
               "counters":{"entries":20},
               "day_yekt":day(now()),"open_positions":1
           },
           "virtual_paper_continuity":{
               "checkpoint_completed":true,"real_orders":false
           },
           "trading":false,"private_order_path_linked":false,
           "hard_safety":{"allowed":false,"real_launch_authorized":false}
        })).unwrap()).unwrap();
    fs::write(sess.join("supervisor_summary.json"),
        serde_json::to_vec(&json!({
           "day":day(now()),"session":name,
           "started_with_entries":17,"finished_with_entries":500,
           "exit_code":2,"killed_for_safety":false,
           "real_trading":false,
           "reason":"unrecovered_virtual_positions_paper_entries_quarantined"
        })).unwrap()).unwrap();
    fs::write(root.join("daily_budget.json"),
        serde_json::to_vec(&json!({
           "day":day(now()),"entries":500,
           "reason":"unrecovered_virtual_positions_paper_entries_quarantined"
        })).unwrap()).unwrap();
    let validated=inspect(&root).unwrap();
    assert_eq!(validated.entries,20);
    assert_eq!(validated.open,1);
    assert_eq!(validated.checkpoint_revision,99);
    assert_eq!(validated.budget_was,500);
    assert!(!root.join("planned_handoff.json").exists());
    // A bad old exit is never a planned migration, even with an
    // intact disk image and matching model entry count.
    let summary=sess.join("supervisor_summary.json");
    let mut bad=read(&summary).unwrap();
    bad["exit_code"]=json!(1);
    fs::write(&summary,serde_json::to_vec(&bad).unwrap()).unwrap();
    assert!(inspect(&root).is_err());
    bad["exit_code"]=json!(2);
    fs::write(&summary,serde_json::to_vec(&bad).unwrap()).unwrap();
    assert!(inspect(&root).is_ok());
    arm(&root,&validated).unwrap();
    assert_eq!(read(&root.join("daily_budget.json")).unwrap()["entries"],20);
    assert_eq!(read(&root.join("planned_handoff.json")).unwrap()["entries"],20);
    assert_eq!(read(&root.join("planned_handoff.json")).unwrap()["no_real_orders"],true);
    assert!(inspect(&root).is_err(),"marker is single use");
    quarantine(&root).unwrap();
    assert_eq!(read(&root.join("daily_budget.json")).unwrap()["entries"],MAX_DAILY);
    assert!(!root.join("planned_handoff.json").exists());
    assert_eq!(read(&root.join("legacy_migration_quarantine_20261013.json"))
        .unwrap()["no_real_orders"],true);
    fs::remove_dir_all(root).unwrap();
 }
 #[test]fn no_live_service_or_native_fill_permission_paths(){
    assert_ne!(ROOT,"/home/tmpadmin/spotbot");
    assert_eq!(MAX_DAILY,500);
 }
}
