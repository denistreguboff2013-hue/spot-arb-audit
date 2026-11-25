//! R1115: hash-checked model stage journal to no-network R37/R40 PAPER queue.
use crate::{
 native_paper_notice_queue_review::{advance_native_paper_watch_oldstyle,PaperWatchState,PaperWatchStep},
 native_paper_telegram_observer::PaperPoint,
 native_trading_config::NativeTradingConfig,
 native_virtual_four_leg_provenance_review::inspect_model_trade_notices,
};
use std::path::Path;
const DAY:u64=86_400_000;
/// No HTTP or exchange access. This only PLANS events. Caller must fsync
/// the resulting queue and separately verify its journal floor before delivery.
pub fn r1115_plan(
 path:&Path,previous:Option<&PaperWatchState>,
 config:&NativeTradingConfig,now_ms:u64
)->Result<PaperWatchStep,String>{
 if now_ms==0||config.policy.real_execution_authorized||
   config.policy.real_daily_trades!=0{
   return Err("R1115 real-order policy forbidden".into())
 }
 let (tip,rows)=inspect_model_trade_notices(path)
   .map_err(|e|format!("R1115 source journal invalid: {e:?}"))?;
 let start_ms=tip.session.rsplit('_').next()
   .and_then(|x|x.parse::<u64>().ok())
   .and_then(|x|x.checked_mul(1000))
   .ok_or("R1115 source session is not dated")?;
 if !tip.session.starts_with("session_")||
    now_ms<start_ms||now_ms-start_ms>30*DAY||
    tip.sequence==0||tip.entries>500||tip.open>10||
    tip.closed>tip.entries||tip.closed+tip.open as u64>tip.entries{
    return Err("R1115 model provenance/time/limits denied".into())
 }
 let realized=f64::from_bits(tip.modeled_realized_bits);
 if !realized.is_finite()||realized.abs()>1_000_000.0{
    return Err("R1115 model PnL denied".into())
 }
 let day=now_ms.checked_add(18_000_000).ok_or("R1115 day overflow")?/DAY;
 let point=PaperPoint{
   session:tip.session.clone(),day,updated_wall_ms:now_ms,
   entries:tip.entries,closes:tip.closed,open_positions:tip.open as u64,
   faults:0,unknown_unhedged:0,
   modeled_milli_usdt:(realized*1000.0).round() as i64,
 };
 let step=advance_native_paper_watch_oldstyle(
   previous,point,config,&tip,&rows,now_ms)
   .map_err(|e|format!("R1115 R37 oldstyle gate: {e:?}"))?;
 if step.telegram_network_called||step.real_orders_permitted||
   step.next.oldstyle.as_ref().map(|x|x.verified_journal_seq)!=Some(tip.sequence)||
   step.next.pending.values().any(|r|
      r.send_attempted||r.notice.external_delivery||
      r.notice.real_pnl_attested||r.notice.may_trigger_real_trade){
   return Err("R1115 unverified notice authority".into())
 }
 Ok(step)
}
#[cfg(test)]
mod tests{
 use super::*;
 use crate::{
  native_paper_market_execution_review::{
    PaperAssumptions,PaperMachine,PaperPhase,PaperPosition,
  },
  native_paper_portfolio_review::PaperPortfolio,
  native_virtual_four_leg_provenance_review::{
    ModeledFourLegJournal,inspect_model_trade_notices,
  },
 };
 use std::{fs,path::PathBuf};
 fn cfg()->NativeTradingConfig{
  NativeTradingConfig::parse(
   include_str!("../config/native_trading_paper_only_r1076.yaml")).unwrap()
 }
 fn root()->PathBuf{
  let p=std::env::temp_dir().join(format!("r1115_{}_{}",
   std::process::id(),crate::transport::monotonic_ns()));
  fs::create_dir(&p).unwrap();p
 }
 fn pos()->PaperPosition{PaperPosition{
    base:"ONE".into(),signal_ns:1,spot_buy_ns:1,
    signal_estimated_net_bps:52.3,spot_gross_base:4.0,
    spot_delivered_base:4.0,spot_paid_quote:10.0,
    futures_short_ns:None,futures_short_proceeds_quote:None,
    futures_short_base:None,hedge_entry_estimated_net_bps:None,
 }}
 const NOW:u64=1_792_080_231_000;
 #[test]
 fn r1115_real_model_writer_hashchain_open_then_close_no_replay(){
  let dir=root();let file=dir.join("model.jsonl");
  let mut p=PaperPortfolio::new(NOW as i64).unwrap();
  let session=format!("session_{}_{}",(NOW+18_000_000)/DAY,NOW/1000);
  let mut journal=ModeledFourLegJournal::start(&file,&session,&p,0).unwrap();
  let c=cfg();
  let baseline=r1115_plan(&file,None,&c,NOW+1000).unwrap();
  assert!(baseline.baseline_created&&baseline.newly_queued.is_empty());
  let mut before=p.clone();p.stats.entries=1;
  p.positions.insert("ONE".into(),PaperMachine{
    config:PaperAssumptions::default(),phase:PaperPhase::AwaitingHedge(pos())
  });
  journal.record_change("ONE",&before,&p).unwrap();
  before=p.clone();
  let mut hedge=pos();
  hedge.futures_short_ns=Some(2);
  hedge.futures_short_base=Some(4.0);
  hedge.futures_short_proceeds_quote=Some(10.2);
  hedge.hedge_entry_estimated_net_bps=Some(180.0);
  p.stats.futures_hedges=1;
  p.positions.get_mut("ONE").unwrap().phase=PaperPhase::Hedged(hedge.clone());
  journal.record_change("ONE",&before,&p).unwrap();
  let opened=r1115_plan(&file,Some(&baseline.next),&c,NOW+2000).unwrap();
  assert_eq!(opened.newly_queued.len(),1);
  assert!(opened.newly_queued[0].starts_with("POPEN-"));
  let recovered:PaperWatchState=serde_json::from_slice(
   &serde_json::to_vec(&opened.next).unwrap()).unwrap();
  let duplicate=r1115_plan(&file,Some(&recovered),&c,NOW+2500).unwrap();
  assert!(duplicate.newly_queued.is_empty());
  before=p.clone();p.stats.futures_covers=1;
  p.positions.get_mut("ONE").unwrap().phase=PaperPhase::AwaitingSpotExit{
   pos:hedge,cover_quote:10.0,cover_ns:3,close_estimate_quote:0.2
  };
  journal.record_change("ONE",&before,&p).unwrap();
  before=p.clone();p.stats.closed=1;
  p.stats.modeled_realized_usdt=0.3;p.positions.remove("ONE");
  journal.record_change("ONE",&before,&p).unwrap();
  let closed=r1115_plan(&file,Some(&duplicate.next),&c,NOW+3000).unwrap();
  assert_eq!(closed.newly_queued.len(),1);
  assert!(closed.newly_queued[0].starts_with("PCLOSE-"));
  // First activation AFTER historical modeled trades is BASELINE ONLY.
  let historical=r1115_plan(&file,None,&c,NOW+3100).unwrap();
  assert!(historical.baseline_created);
  assert!(historical.newly_queued.is_empty());
  assert!(historical.next.pending.is_empty());
  assert_eq!(closed.next.pending.len(),2);
  let encoded=serde_json::to_vec(&closed.next).unwrap();
  fs::write(dir.join("queue.json"),&encoded).unwrap();
  let restored:PaperWatchState=serde_json::from_slice(
   &fs::read(dir.join("queue.json")).unwrap()).unwrap();
  let again=r1115_plan(&file,Some(&restored),&c,NOW+3500).unwrap();
  assert!(again.newly_queued.is_empty());
  assert_eq!(again.next.pending.len(),2);
  assert_eq!(inspect_model_trade_notices(&file).unwrap().1.len(),2);
  let mut damaged=fs::read(&file).unwrap();
  damaged.push(b'x');fs::write(&file,&damaged).unwrap();
  assert!(r1115_plan(&file,Some(&again.next),&c,NOW+4000).is_err());
  fs::remove_dir_all(dir).unwrap();
 }
 #[test]
 fn r1115_original_stop_seal_cannot_impersonate_new_live_trade(){
  let dir=root();let file=dir.join("model.jsonl");
  let p=PaperPortfolio::new(NOW as i64).unwrap();
  let mut j=ModeledFourLegJournal::start(
    &file,"session_1084_private",&p,0).unwrap();
  j.seal(&p).unwrap();
  assert_eq!(inspect_model_trade_notices(&file).unwrap().1.len(),0);
  assert!(r1115_plan(&file,None,&cfg(),NOW+1000).is_err());
  fs::remove_dir_all(dir).unwrap();
 }
}
