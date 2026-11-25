//! R32D6C8B2A ordinary-linked, isolated paper accounting only.
use std::{fs,path::PathBuf};
use spotarb_native_market_core::{
 authoritative_state::{NativeAuthoritativeState,NativeStateStore,NativeStateError},
 native_daily_risk::{yekt_day_index,RiskWrite,UTC_DAY_MS,YEKT_OFFSET_MS},
 native_trading_config::NativeTradingConfig,
};
const CFG:&str=include_str!("../config/native_trading.yaml");
fn path(tag:&str)->PathBuf{
 let n=std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
 std::env::temp_dir().join(format!("r32d6c8b2a-{tag}-{}-{n}",std::process::id()))
}
fn config(paper_daily:u64,slots:u64)->String{
 CFG.replace("  paper_entries_enabled: false\n","  paper_entries_enabled: true\n")
    .replace("  paper_max_trades_per_day: 3000\n",
        &format!("  paper_max_trades_per_day: {paper_daily}\n"))
    .replace("  max_open_positions: 10\n",&format!("  max_open_positions: {slots}\n"))
}
fn new_store(tag:&str,cfg:&str)->(PathBuf,PathBuf,NativeStateStore){
 let root=path(tag);
 fs::create_dir_all(&root).unwrap();
 let c=root.join("risk.yaml");
 fs::write(&c,cfg).unwrap();
 let store=NativeStateStore::open(root.join("state.json")).unwrap();
 store.create(&NativeAuthoritativeState::empty_from_legacy("11".repeat(32),0)).unwrap();
 (root,c,store)
}
fn clock()->u64{21_000u64*UTC_DAY_MS-YEKT_OFFSET_MS+1000}
#[test]
fn r32d6c8b2a_original_python_modes_closed_no_state_mutation(){
 let (root,c,store)=new_store("disabled",CFG);
 let bytes=fs::read(root.join("state.json")).unwrap();
 assert!(store.reserve_paper_daily_slot(1,&c,clock(),"paper-1").is_err());
 assert!(store.reject_real_daily_slot(1,&c,clock()).is_err());
 assert_eq!(fs::read(root.join("state.json")).unwrap(),bytes);
 assert!(store.load().unwrap().unwrap().paper_risk_ledger.is_none());
 let p=NativeTradingConfig::load(&c).unwrap().policy;
 assert_eq!(p.general_daily_trades,300);
 assert_eq!(p.paper_daily_trades,3000);
 assert_eq!(p.real_daily_trades,0);
 assert!(!p.paper_entries_enabled);
 fs::remove_dir_all(root).unwrap();
}
#[test]
fn r32d6c8b2a_daily_cap_not_refunded_and_yekt_midnight_rollover(){
 let (root,c,store)=new_store("rollover",&config(2,2));
 let t=clock();
 assert_eq!(yekt_day_index(t+UTC_DAY_MS-1001).unwrap(),yekt_day_index(t).unwrap());
 assert_eq!(yekt_day_index(t+UTC_DAY_MS).unwrap(),yekt_day_index(t).unwrap()+1);
 let (s,w)=store.reserve_paper_daily_slot(1,&c,t,"paper-1").unwrap();
 assert_eq!(w,RiskWrite::Recorded);
 let (s,w)=store.reserve_paper_daily_slot(s.generation,&c,t+1,"paper-2").unwrap();
 assert_eq!(w,RiskWrite::Recorded);
 assert_eq!(s.paper_risk_ledger.as_ref().unwrap().open_paper_slots(),2);
 assert_eq!(s.paper_risk_ledger.as_ref().unwrap().used_paper_today(t+1).unwrap(),2);
 let before=fs::read(root.join("state.json")).unwrap();
 assert!(store.reserve_paper_daily_slot(s.generation,&c,t+2,"paper-3").is_err());
 assert_eq!(fs::read(root.join("state.json")).unwrap(),before);
 let (s,w)=store.release_paper_daily_slot(s.generation,&c,t+3,"paper-1","close-1").unwrap();
 assert_eq!(w,RiskWrite::Recorded);
 assert_eq!(s.paper_risk_ledger.as_ref().unwrap().open_paper_slots(),1);
 assert!(store.reserve_paper_daily_slot(s.generation,&c,t+4,"paper-3").is_err());
 let (s,w)=store.reserve_paper_daily_slot(s.generation,&c,t+UTC_DAY_MS,"paper-3").unwrap();
 assert_eq!(w,RiskWrite::Recorded);
 assert_eq!(s.paper_risk_ledger.as_ref().unwrap().open_paper_slots(),2);
 assert_eq!(s.paper_risk_ledger.as_ref().unwrap().used_paper_today(t+UTC_DAY_MS).unwrap(),1);
 let (same,w)=store.reserve_paper_daily_slot(s.generation,&c,t+UTC_DAY_MS+1,"paper-1").unwrap();
 assert_eq!(w,RiskWrite::Idempotent);
 assert_eq!(same.generation,s.generation);
 let (same,w)=store.release_paper_daily_slot(same.generation,&c,t+UTC_DAY_MS+1,"paper-1","close-1").unwrap();
 assert_eq!(w,RiskWrite::Idempotent);
 assert_eq!(same.generation,s.generation);
 let reopened=NativeStateStore::open(root.join("state.json")).unwrap();
 assert_eq!(reopened.load().unwrap().unwrap(),s);
 fs::remove_dir_all(root).unwrap();
}
#[test]
fn r32d6c8b2a_config_sha_drift_reverse_clock_and_zero_real_fail_closed(){
 let (root,c,store)=new_store("drift",&config(2,2));
 let t=clock();
 let (s,_)=store.reserve_paper_daily_slot(1,&c,t,"paper-a").unwrap();
 let old=fs::read(root.join("state.json")).unwrap();
 let changed=config(2,2).replace("  heartbeat_interval_sec: 900\n",
                                  "  heartbeat_interval_sec: 901\n");
 fs::write(&c,changed).unwrap();
 assert!(store.reserve_paper_daily_slot(s.generation,&c,t+1,"paper-b").is_err());
 assert!(store.release_paper_daily_slot(s.generation,&c,t+1,"paper-a","close-a").is_err());
 fs::write(&c,config(2,2)).unwrap();
 assert!(store.reserve_paper_daily_slot(s.generation,&c,t-1,"paper-b").is_err());
 assert!(store.reject_real_daily_slot(s.generation,&c,t+1).is_err());
 assert_eq!(fs::read(root.join("state.json")).unwrap(),old);
 fs::remove_dir_all(root).unwrap();
}
#[test]
fn r32d6c8b2a_conflicting_close_receipt_or_unknown_identity_denied(){
 let (root,c,store)=new_store("receipt",&config(4,2));
 let t=clock();
 let (s,_)=store.reserve_paper_daily_slot(1,&c,t,"paper-a").unwrap();
 let (s,_)=store.reserve_paper_daily_slot(s.generation,&c,t+1,"paper-b").unwrap();
 let (s,_)=store.release_paper_daily_slot(s.generation,&c,t+2,"paper-a","close-a").unwrap();
 let old=fs::read(root.join("state.json")).unwrap();
 assert!(store.release_paper_daily_slot(s.generation,&c,t+3,"paper-a","other-close").is_err());
 assert!(store.release_paper_daily_slot(s.generation,&c,t+3,"paper-b","close-a").is_err());
 assert!(store.release_paper_daily_slot(s.generation,&c,t+3,"unknown","close-x").is_err());
 assert_eq!(fs::read(root.join("state.json")).unwrap(),old);
 fs::remove_dir_all(root).unwrap();
}
#[test]
fn r32d6c8b2a_parallel_reservations_one_cas_wins_and_capacity_enforced(){
 let (root,c,store)=new_store("race",&config(2,1));
 let t=clock();
 let a=store.clone();let b=store.clone();
 let pa=c.clone();let pb=c.clone();
 let x=std::thread::spawn(move||a.reserve_paper_daily_slot(1,&pa,t,"paper-a"));
 let y=std::thread::spawn(move||b.reserve_paper_daily_slot(1,&pb,t,"paper-b"));
 let results=[x.join().unwrap(),y.join().unwrap()];
 assert_eq!(results.iter().filter(|r|r.is_ok()).count(),1);
 assert_eq!(results.iter().filter(|r|matches!(r,Err(NativeStateError::Generation))).count(),1);
 let one=store.load().unwrap().unwrap();
 assert_eq!(one.generation,2);
 assert_eq!(one.paper_risk_ledger.as_ref().unwrap().open_paper_slots(),1);
 assert!(store.reserve_paper_daily_slot(2,&c,t+1,"paper-c").is_err());
 fs::remove_dir_all(root).unwrap();
}
#[test]
fn r32d6c8b2a_tampered_count_or_receipt_and_blocked_tmp_preserve_old_disk(){
 let (root,c,store)=new_store("tamper",&config(2,2));
 let t=clock();
 let (s,_)=store.reserve_paper_daily_slot(1,&c,t,"paper-a").unwrap();
 let (s,_)=store.reserve_paper_daily_slot(s.generation,&c,t+1,"paper-b").unwrap();
 let mut forged=s.clone();
 forged.paper_risk_ledger.as_mut().unwrap().paper_daily_limit=1;
 assert!(forged.validate().is_err());
 let mut forged=s.clone();
 forged.paper_risk_ledger.as_mut().unwrap().config_sha256="fake".into();
 assert!(forged.validate().is_err());
 let before=fs::read(root.join("state.json")).unwrap();
 let blocker=root.join("state.tmp");
 fs::create_dir(&blocker).unwrap();
 assert!(store.release_paper_daily_slot(s.generation,&c,t+2,"paper-a","close-a").is_err());
 assert_eq!(fs::read(root.join("state.json")).unwrap(),before);
 fs::remove_dir(&blocker).unwrap();
 let (saved,_) =store.release_paper_daily_slot(s.generation,&c,t+2,"paper-a","close-a").unwrap();
 assert_eq!(saved.generation,s.generation+1);
 fs::remove_dir_all(root).unwrap();
}
#[test]
fn r32d6c8b2a_child_exit_before_or_after_durable_risk_record(){
 let Ok(dest)=std::env::var("R32D6C8B2_CHILD_ROOT")else{return};
 let phase=std::env::var("R32D6C8B2_CHILD_PHASE").unwrap();
 let (root,c,_store)=new_store("child",&config(2,2));
 let target=PathBuf::from(dest);
 fs::create_dir_all(&target).unwrap();
 fs::write(target.join("state.json"),fs::read(root.join("state.json")).unwrap()).unwrap();
 let cfg_path=target.join("risk.yaml");
 fs::write(&cfg_path,fs::read(&c).unwrap()).unwrap();
 fs::File::open(&target).unwrap().sync_all().unwrap();
 let other=NativeStateStore::open(target.join("state.json")).unwrap();
 if phase=="after"{
  let (s,w)=other.reserve_paper_daily_slot(1,&cfg_path,clock(),"paper-recovery").unwrap();
  assert_eq!(w,RiskWrite::Recorded);
  assert_eq!(s.generation,2);
 }else{assert_eq!(phase,"before")}
 std::process::exit(84);
}
#[test]
fn r32d6c8b2a_forced_child_crash_reopens_either_old_or_whole_risk_record(){
 for phase in ["before","after"]{
  let dir=path(&format!("crash-{phase}"));
  fs::create_dir_all(&dir).unwrap();
  let status=std::process::Command::new(std::env::current_exe().unwrap())
    .arg("--exact").arg("r32d6c8b2a_child_exit_before_or_after_durable_risk_record")
    .env("R32D6C8B2_CHILD_ROOT",&dir)
    .env("R32D6C8B2_CHILD_PHASE",phase)
    .status().unwrap();
  assert_eq!(status.code(),Some(84));
  let store=NativeStateStore::open(dir.join("state.json")).unwrap();
  let state=store.load().unwrap().unwrap();
  assert_eq!(state.paper_risk_ledger.is_some(),phase=="after");
  assert_eq!(state.generation,if phase=="after"{2}else{1});
  assert!(state.entry.is_none()&&state.active.is_none());
  fs::remove_dir_all(dir).unwrap();
 }
}


#[test]
fn r32d6c8b2a_3000_paper_daily_is_independent_of_general_300_cap(){
 use spotarb_native_market_core::native_daily_risk::{NativeDailyRiskLedger,NativePaperRiskRecord,RiskError};
 let cfg=NativeTradingConfig::parse(&config(3000,10)).unwrap();
 assert_eq!(cfg.policy.general_daily_trades,300);
 assert_eq!(cfg.policy.paper_daily_trades,3000);
 let start=clock();
 let mut ledger=NativeDailyRiskLedger::new(&cfg,start).unwrap();
 let day=yekt_day_index(start).unwrap();
 for idx in 0..3000u64{
  let id=format!("paper-{idx}");
  let stamp=start+idx;
  ledger.records.insert(id.clone(),NativePaperRiskRecord{
   entry_id:id,day_yekt:day,reserved_at_ms:stamp,
   released_at_ms:if idx<2999{Some(stamp+1)}else{None},
   close_receipt_id:if idx<2999{Some(format!("close-{idx}"))}else{None},
  });
 }
 ledger.last_seen_at_ms=start+3000;
 ledger.validate().unwrap();
 assert_eq!(ledger.open_paper_slots(),1);
 assert_eq!(ledger.used_paper_today(start+3000).unwrap(),3000);
 assert!(matches!(ledger.can_reserve_paper(&cfg,start+3001,"paper-3000"),
    Err(RiskError::DailyLimit)));
 assert!(ledger.can_reserve_real(&cfg,start+3001).is_err());
}
