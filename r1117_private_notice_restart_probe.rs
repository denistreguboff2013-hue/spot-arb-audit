//! R1117 offline multi-process private PAPER notice restart witness.
//! No network / live orders / Telegram send. Never targets installed service.
use spotarb_native_market_core::{
 native_paper_portfolio_review::PaperPortfolio,
 native_paper_market_execution_review::{PaperAssumptions,PaperMachine,PaperPhase,PaperPosition},
 native_virtual_four_leg_provenance_review::{ModeledFourLegJournal,inspect_model_trade_notices},
 native_trading_config::NativeTradingConfig,
 native_r1116_durable_model_notice::r1116_commit,
};
use serde_json::{json,Value};
use std::{fs::{self,File},path::{Path,PathBuf},
 os::unix::fs::{MetadataExt,PermissionsExt,OpenOptionsExt},time::{SystemTime,UNIX_EPOCH}};
const T:&str="/mnt/spotarb_db_test/astra_p001_r874_test_scratch_20261025";
const ORIGINAL:&str="423ba8e64e003bc36e73bf5fda4313d3d9ac8b781ecdfb82761d0e9cd7a9f6f9";
fn sha(path:&Path)->Result<String,String>{
 let bytes=fs::read(path).map_err(|e|format!("R1117 read: {e}"))?;
 Ok(openssl::sha::sha256(&bytes).iter().map(|b|format!("{b:02x}")).collect())
}
fn now()->u64{
 SystemTime::now().duration_since(UNIX_EPOCH).expect("system wall").as_millis() as u64
}
fn preflight(root:&Path)->Result<(),String>{
 if std::env::var("ASTRA_R1117_OFFLINE").ok().as_deref()!=Some("YES"){
    return Err("R1117 must be explicitly OFFLINE".into())
 }
 let name=root.file_name().and_then(|v|v.to_str()).unwrap_or("");
 if root.parent()!=Some(Path::new(T))||
    !name.starts_with("astra_r1116_R1117_")||name.len()<26{
    return Err("R1117 private root identity denied".into())
 }
 let stat=fs::symlink_metadata(root).map_err(|_|"R1117 missing root")?;
 if !stat.file_type().is_dir()||stat.uid()!=unsafe{libc::geteuid()}||
    stat.permissions().mode()&0o077!=0{
    return Err("R1117 directory owner/mode refused".into())
 }
 let service=std::process::Command::new("/usr/bin/systemctl")
    .args(["show","spotarb-native-paper-continuous.service",
        "-p","ActiveState","-p","MainPID","-p","UnitFileState"])
    .output().map_err(|_|"R1117 installed status missing")?;
 let status=String::from_utf8_lossy(&service.stdout);
 if !service.status.success()||
   !status.lines().any(|x|x=="ActiveState=inactive")||
   !status.lines().any(|x|x=="MainPID=0")||
   !status.lines().any(|x|x=="UnitFileState=disabled"){
    return Err("R1117 installed PAPER is not stopped".into())
 }
 let kill:Value=serde_json::from_slice(&fs::read(
    "/home/tmpadmin/spotbot/data/real_kill_switch.json")
    .map_err(|_|"R1117 kill file unavailable")?)
    .map_err(|_|"R1117 kill JSON invalid")?;
 if kill["state"]!="ENGAGED"{
    return Err("R1117 real kill switch not ENGAGED".into())
 }
 if sha(Path::new("/mnt/spotarb_db_test/astra_paper_continuous_v1/portfolio_checkpoint.json"))?!=ORIGINAL{
    return Err("R1117 original operator portfolio changed".into())
 }
 Ok(())
}
fn config()->Result<NativeTradingConfig,String>{
 let cfg=NativeTradingConfig::load(&PathBuf::from(env!("CARGO_MANIFEST_DIR"))
   .join("config/native_trading_paper_only_r1076.yaml"))
   .map_err(|e|format!("R1117 source YAML: {e:?}"))?;
 if cfg.sha256!="38a71a9c457c00c686c6f7c4c808d05cfe37e43cc760fc8591eac960d101ec4a"||
   cfg.policy.real_execution_authorized{
    return Err("R1117 unexpected real/YAML source".into())
 }
 Ok(cfg)
}
fn pos()->PaperPosition{PaperPosition{
 base:"ONE".into(),signal_ns:1,spot_buy_ns:1,signal_estimated_net_bps:52.3,
 spot_gross_base:4.0,spot_delivered_base:4.0,spot_paid_quote:10.0,
 futures_short_ns:None,futures_short_proceeds_quote:None,
 futures_short_base:None,hedge_entry_estimated_net_bps:None,
}}
fn run(mode:&str,root:&Path)->Result<Value,String>{
 preflight(root)?;
 let cfg=config()?;let model=root.join("model.jsonl");
 let queue=root.join("r1116_durable_model_notice.json");
 if mode=="seed"{
   if model.exists()||queue.exists(){
     return Err("R1117 previously used private root".into())
   }
   let at=now();
   let mut p=PaperPortfolio::new(at as i64)
       .map_err(|e|format!("R1117 model: {e:?}"))?;
   let session=format!("session_{}_{}",(at+18_000_000)/86_400_000,at/1000);
   let mut j=ModeledFourLegJournal::start(&model,&session,&p,0)
       .map_err(|e|format!("R1117 new journal: {e:?}"))?;
   fs::set_permissions(&model,fs::Permissions::from_mode(0o600))
       .map_err(|_|"R1117 secure journal chmod denied")?;
   if r1116_commit(root,&model,&cfg,at+1000,false)?!=(1,0){
     return Err("R1117 private baseline refused".into())
   }
   let mut prev=p.clone();p.stats.entries=1;
   p.positions.insert("ONE".into(),PaperMachine{
     config:PaperAssumptions::default(),phase:PaperPhase::AwaitingHedge(pos())
   });
   j.record_change("ONE",&prev,&p)
     .map_err(|e|format!("R1117 spot buy: {e:?}"))?;
   if r1116_commit(root,&model,&cfg,at+2000,false)?!=(2,0){
      return Err("R1117 unexpected spot buy notice".into())
   }
   prev=p.clone();let mut h=pos();
   h.futures_short_ns=Some(2);h.futures_short_base=Some(4.0);
   h.futures_short_proceeds_quote=Some(10.2);
   h.hedge_entry_estimated_net_bps=Some(180.0);
   p.stats.futures_hedges=1;
   p.positions.get_mut("ONE").unwrap().phase=PaperPhase::Hedged(h.clone());
   j.record_change("ONE",&prev,&p)
      .map_err(|e|format!("R1117 future short: {e:?}"))?;
   if r1116_commit(root,&model,&cfg,at+3000,false)?!=(3,1){
      return Err("R1117 missing source POPEN".into())
   }
   prev=p.clone();p.stats.futures_covers=1;
   p.positions.get_mut("ONE").unwrap().phase=PaperPhase::AwaitingSpotExit{
      pos:h,cover_quote:10.0,cover_ns:3,close_estimate_quote:0.2
   };
   j.record_change("ONE",&prev,&p)
     .map_err(|e|format!("R1117 cover: {e:?}"))?;
   if r1116_commit(root,&model,&cfg,at+4000,false)?!=(4,0){
     return Err("R1117 cover notice unexpected".into())
   }
   prev=p.clone();p.stats.closed=1;p.stats.modeled_realized_usdt=0.3;
   p.positions.remove("ONE");
   j.record_change("ONE",&prev,&p)
     .map_err(|e|format!("R1117 final spot sell: {e:?}"))?;
   if r1116_commit(root,&model,&cfg,at+5000,false)?!=(5,1){
     return Err("R1117 missing PCLOSE".into())
   }
   j.seal(&p).map_err(|e|format!("R1117 journal seal: {e:?}"))?;
   if r1116_commit(root,&model,&cfg,at+6000,false)?!=(6,0){
      return Err("R1117 seal cursor not durable".into())
   }
   let proof=json!({"mode":"seed","ok":true,"model_seq":6,
      "modeled_entries":1,"modeled_closed":1,"modeled_open":0,
      "notices":2,"telegram_sent":false,"real_orders":0,
      "model_sha256":sha(&model)?,"queue_sha256":sha(&queue)?});
   let report=root.join("r1117_seed_receipt.json");
   let mut fd=std::fs::OpenOptions::new().write(true).create_new(true)
      .mode(0o600).open(&report)
      .map_err(|_|"R1117 prior seed receipt blocked")?;
   use std::io::Write;
   fd.write_all(&serde_json::to_vec_pretty(&proof).unwrap())
      .map_err(|_|"R1117 seed write failed")?;
   fd.sync_all().map_err(|_|"R1117 seed sync failed")?;
   File::open(root).and_then(|d|d.sync_all())
      .map_err(|_|"R1117 seed dir sync failed")?;
   return Ok(proof)
 }
 let seed:Value=serde_json::from_slice(
    &fs::read(root.join("r1117_seed_receipt.json"))
      .map_err(|_|"R1117 seed receipt absent")?)
      .map_err(|_|"R1117 seed receipt malformed")?;
 if seed["ok"]!=true||seed["notices"]!=2||
    seed["model_sha256"]!=sha(&model)?{
    return Err("R1117 source model mismatch across processes".into())
 }
 if mode=="reopen"{
    let (_,rows)=inspect_model_trade_notices(&model)
       .map_err(|_|"R1117 cold reader refused")?;
    if rows.len()!=2||rows[0].stage!="FUTURES_SHORT_MODELED"||
       rows[1].stage!="SPOT_SELL_MODELED"{
       return Err("R1117 modeled journal stages wrong".into())
    }
    if r1116_commit(root,&model,&cfg,now()+1000,false)?!=(6,0){
       return Err("R1117 cold queue created duplicates".into())
    }
    let read=fs::read(&queue).map_err(|_|"R1117 missing queue")?;
    let state:Value=serde_json::from_slice(&read)
       .map_err(|_|"R1117 queue JSON invalid")?;
    let pending=state["state"]["pending"].as_object()
       .ok_or("R1117 missing pending")?;
    let opens=pending.keys().filter(|k|k.starts_with("POPEN-")).count();
    let closes=pending.keys().filter(|k|k.starts_with("PCLOSE-")).count();
    if pending.len()!=2||opens!=1||closes!=1{return Err("R1117 invalid notices".into())}
    return Ok(json!({"mode":"reopen","ok":true,"verified_seq":6,
        "pending":2,"new_events":0,"telegram_sent":false,"real_orders":0}))
 }
 if mode=="inject-stage"{
    let prior=sha(&queue)?;
    if r1116_commit(root,&model,&cfg,now()+1000,true).is_ok(){
      return Err("R1117 staged fault not rejected".into())
    }
    if sha(&queue)?!=prior{return Err("R1117 queue changed".into())}
    return Ok(json!({"mode":"inject-stage","ok":true,"old_queue_sha256":prior}))
 }
 Err("R1117 unsupported mode".into())
}
fn main(){
 let args=std::env::args().collect::<Vec<_>>();
 let value=if args.len()==3{run(&args[1],Path::new(&args[2]))}
 else{Err("R1117 missing arguments".into())};
 match value{
  Ok(v)=>println!("{}",serde_json::to_string(&v).unwrap()),
  Err(e)=>{eprintln!("R1117 DENIED: {e}");std::process::exit(70)}
 }
}
