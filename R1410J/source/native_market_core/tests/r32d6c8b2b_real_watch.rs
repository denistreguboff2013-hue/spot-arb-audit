//! R32D6C8B2B1: mock-only CLOSED-CYCLE mathematical scenarios.
//! No authenticated live exchange close, no mutation network or real permit.
use spotarb_native_market_core::{
 native_trading_config::NativeTradingConfig,
 native_real_risk_watch::{
  SimulatedClosedCycle,simulate_read_only_real_budgets,RealWatchError,
 },
 native_daily_risk::{UTC_DAY_MS,YEKT_OFFSET_MS,yekt_day_index},
 fill_economics::{decimal_to_atoms,signed_decimal_to_atoms},
};
const YAML:&str=include_str!("../config/native_trading.yaml");
fn config()->NativeTradingConfig{NativeTradingConfig::parse(YAML).unwrap()}
fn now()->u64{22_000*UTC_DAY_MS-YEKT_OFFSET_MS+120_000}
fn row(id:&str,time:u64,net:&str,fees:&str,okx:&str)->SimulatedClosedCycle{
 SimulatedClosedCycle{
  id:id.into(),completed_at_ms:time,
  quote_net_result_e12:Some(signed_decimal_to_atoms(net).unwrap()),
  all_fees_e12:Some(decimal_to_atoms(fees).unwrap()),
  okx_fees_e12:Some(decimal_to_atoms(okx).unwrap()),
  funding_complete:true,both_closes_confirmed:true,
 }
}
#[test]
fn r32d6c8b2b1_zero_real_allowed_even_if_budget_and_cooldown_pass(){
 let cfg=config();
 let t=now();
 let v=simulate_read_only_real_budgets(&cfg,t,&[]).unwrap();
 assert_eq!(v.finished_circuits_today,0);
 assert_eq!(v.loss_total_e12,0);
 assert_eq!(v.fees_total_e12,0);
 assert_eq!(v.cooldown_remaining_ms,0);
 assert!(!v.over_loss_cap&&!v.over_fee_cap);
 assert!(!v.may_open_real);
 assert_eq!(cfg.policy.real_daily_trades,0);
}
#[test]
fn r32d6c8b2b1_real_cooldown_three_hours_and_yekt_rollover_dont_reset(){
 let cfg=config();let t=now();
 let prior=row("prior-close",t-1_000,"-0.1","0.02","0.01");
 let v=simulate_read_only_real_budgets(&cfg,t,&[prior.clone()]).unwrap();
 assert_eq!(v.cooldown_remaining_ms,10_799_000);
 assert_eq!(v.finished_circuits_today,1);
 assert!(!v.may_open_real);
 let later=simulate_read_only_real_budgets(&cfg,t+10_801_000,&[prior]).unwrap();
 assert_eq!(later.cooldown_remaining_ms,0);
 assert!(!later.may_open_real);
 let t2=22_001*UTC_DAY_MS-YEKT_OFFSET_MS+3_000;
 let prev=row("prev-close",t2-4_000,"-9","1.0","0.5");
 assert_ne!(yekt_day_index(t2).unwrap(),yekt_day_index(prev.completed_at_ms).unwrap());
 let next=simulate_read_only_real_budgets(&cfg,t2,&[prev]).unwrap();
 assert_eq!(next.finished_circuits_today,0);
 assert_eq!(next.loss_total_e12,0);
 assert_eq!(next.fees_total_e12,0);
 assert_eq!(next.cooldown_remaining_ms,10_796_000);
 assert!(!next.may_open_real);
}
#[test]
fn r32d6c8b2b1_exact_10_usdt_loss_7_okx_fees_and_total_fees_distinct(){
 let cfg=config();let t=now();
 let v=simulate_read_only_real_budgets(&cfg,t,&[
  row("first",t-2_000,"-6","5","4"),
  row("second",t-1_000,"-4","4","3"),
 ]).unwrap();
 assert_eq!(v.finished_circuits_today,2);
 assert_eq!(v.loss_total_e12,decimal_to_atoms("10").unwrap());
 assert_eq!(v.fees_total_e12,decimal_to_atoms("9").unwrap());
 assert_eq!(v.okx_fees_total_e12,decimal_to_atoms("7").unwrap());
 assert!(v.over_loss_cap&&v.over_fee_cap);
 assert!(!v.may_open_real);
}
#[test]
fn r32d6c8b2b1_profit_wont_refund_gross_daily_losses(){
 let cfg=config();let t=now();
 let v=simulate_read_only_real_budgets(&cfg,t,&[
  row("loss",t-2_000,"-11","5","3"),
  row("profit",t-1_000,"20","1","0.5"),
 ]).unwrap();
 assert_eq!(v.loss_total_e12,decimal_to_atoms("11").unwrap());
 assert!(v.over_loss_cap);
 assert!(!v.over_fee_cap);
 assert!(!v.may_open_real);
}
#[test]
fn r32d6c8b2b1_missing_fees_funding_or_both_close_truth_denied(){
 let cfg=config();let t=now();
 let a=row("a",t-1,"0","0.1","0.01");
 let mut bad=a.clone();bad.quote_net_result_e12=None;
 assert!(matches!(simulate_read_only_real_budgets(&cfg,t,&[bad]),Err(RealWatchError::Unsupported)));
 let mut bad=a.clone();bad.all_fees_e12=None;
 assert!(simulate_read_only_real_budgets(&cfg,t,&[bad]).is_err());
 let mut bad=a.clone();bad.okx_fees_e12=None;
 assert!(simulate_read_only_real_budgets(&cfg,t,&[bad]).is_err());
 let mut bad=a.clone();bad.funding_complete=false;
 assert!(simulate_read_only_real_budgets(&cfg,t,&[bad]).is_err());
 let mut bad=a.clone();bad.both_closes_confirmed=false;
 assert!(simulate_read_only_real_budgets(&cfg,t,&[bad]).is_err());
 let mut bad=a.clone();bad.okx_fees_e12=Some(decimal_to_atoms("100").unwrap());
 assert!(matches!(simulate_read_only_real_budgets(&cfg,t,&[bad]),Err(RealWatchError::Invalid)));
}
#[test]
fn r32d6c8b2b1_duplicate_history_future_timestamp_and_overlong_ids_denied(){
 let cfg=config();let t=now();let good=row("dup",t-1,"-1","1","0.5");
 assert!(simulate_read_only_real_budgets(&cfg,t,&[good.clone(),good.clone()]).is_err());
 let mut future=good.clone();future.completed_at_ms=t+1;
 assert!(simulate_read_only_real_budgets(&cfg,t,&[future]).is_err());
 let mut id=good.clone();id.id="x".repeat(97);
 assert!(simulate_read_only_real_budgets(&cfg,t,&[id]).is_err());
 assert!(simulate_read_only_real_budgets(&cfg,0,&[]).is_err());
}
