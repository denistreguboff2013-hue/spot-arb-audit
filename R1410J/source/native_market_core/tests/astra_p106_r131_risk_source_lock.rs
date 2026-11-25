//! Astra P1-06 R131: mutation-rejection across risk accounting, retry
//! diagnostics and simulated financial limits. Never a real order permit.
use spotarb_native_market_core::{
 native_trading_config::{NativeTradingConfig,ConfigError},
 native_daily_risk::{NativeDailyRiskLedger,RiskError},
 native_real_risk_watch::{simulate_read_only_real_budgets,RealWatchError,
   SimulatedClosedCycle},
};
const YAML:&str=include_str!("../config/native_trading_operator_baseline_r64.yaml");
const NOW:u64=1_792_320_000_000;
fn config()->NativeTradingConfig{NativeTradingConfig::parse(YAML).unwrap()}
fn source_mismatch_denied(cfg:&NativeTradingConfig,ledger:&NativeDailyRiskLedger){
 assert_eq!(NativeDailyRiskLedger::new(cfg,NOW).err(),Some(RiskError::Policy));
 assert_eq!(ledger.ensure_config(cfg),Err(RiskError::Policy));
 assert_eq!(ledger.can_reserve_paper(cfg,NOW,"r131_test"),
   Err(RiskError::Policy));
 assert_eq!(ledger.can_reserve_real(cfg,NOW),Err(RiskError::Policy));
 assert_eq!(simulate_read_only_real_budgets(cfg,NOW,&[]).err(),
   Some(RealWatchError::Policy));
 assert_eq!(cfg.inspect_execution_retry_bounds_read_only().err(),
   Some(ConfigError::Policy));
}
#[test]
fn r131_positive_original_snapshot_remains_readonly(){
 let cfg=config();
 let ledger=NativeDailyRiskLedger::new(&cfg,NOW).unwrap();
 assert_eq!(ledger.ensure_config(&cfg),Ok(()));
 assert_eq!(ledger.used_paper_today(NOW).unwrap(),0);
 let retries=cfg.inspect_execution_retry_bounds_read_only().unwrap();
 assert_eq!(retries.spot_max_attempts,4);
 assert_eq!(retries.spot_max_wait_before_last_ms,750);
 assert_eq!(retries.futures_max_attempts,2);
 assert_eq!(retries.futures_max_wait_before_last_ms,500);
 assert!(!retries.may_resubmit_after_unknown_ack);
 assert!(!retries.may_send_or_cancel_order);
 let view=simulate_read_only_real_budgets(&cfg,NOW,&[]).unwrap();
 assert!(!view.may_open_real);
}
#[test]
fn r131_raw_sha_or_typed_risk_mutation_never_reused_as_original(){
 let original=config();
 let ledger=NativeDailyRiskLedger::new(&original,NOW).unwrap();
 let mut changed=config();
 changed.raw["limits"]["max_daily_loss_usdt"]=serde_yaml::Value::from(8);
 source_mismatch_denied(&changed,&ledger);
 let mut changed=config();
 changed.raw["limits"]["max_okx_fees_per_day_usdt"]=serde_yaml::Value::from(5);
 source_mismatch_denied(&changed,&ledger);
 let mut changed=config();
 changed.raw["execution"]["spot_retry_n"]=serde_yaml::Value::from(5);
 source_mismatch_denied(&changed,&ledger);
 let mut changed=config();
 changed.raw["real_trial"]["deny_on_unknown"]=serde_yaml::Value::from(false);
 source_mismatch_denied(&changed,&ledger);
 let mut changed=config();
 changed.raw["real_trial"]["deny_on_untracked_assets"]=serde_yaml::Value::from(false);
 source_mismatch_denied(&changed,&ledger);
 let mut changed=config();
 changed.policy.daily_loss_cap_e18=1;
 source_mismatch_denied(&changed,&ledger);
 let mut changed=config();
 changed.policy.daily_okx_fee_cap_e18=1;
 source_mismatch_denied(&changed,&ledger);
 let mut changed=config();
 changed.policy.spot_retry_attempts+=1;
 source_mismatch_denied(&changed,&ledger);
 let mut changed=config();
 changed.sha256="a".repeat(64);
 source_mismatch_denied(&changed,&ledger);
}
#[test]
fn r131_valid_reparse_may_change_review_math_but_never_trade(){
 let new_text=YAML.replacen("spot_retry_n: 4","spot_retry_n: 5",1)
   .replacen("max_daily_loss_usdt: 10.0","max_daily_loss_usdt: 8.0",1)
   .replacen("max_okx_fees_per_day_usdt: 7.0",
            "max_okx_fees_per_day_usdt: 5.0",1);
 assert_ne!(new_text,YAML);
 let updated=NativeTradingConfig::parse(&new_text).unwrap();
 assert_eq!(updated.inspect_execution_retry_bounds_read_only()
     .unwrap().spot_max_attempts,5);
 let old=NativeDailyRiskLedger::new(&config(),NOW).unwrap();
 assert_eq!(old.ensure_config(&updated),Err(RiskError::Policy));
 let row=SimulatedClosedCycle{
   id:"mock-closed".into(),completed_at_ms:NOW-1_000,
   quote_net_result_e12:Some(-9_000_000_000_000),
   all_fees_e12:Some(6_000_000_000_000),
   okx_fees_e12:Some(5_000_000_000_000),
   funding_complete:true,both_closes_confirmed:true,
 };
 let simulated=simulate_read_only_real_budgets(&updated,NOW,&[row]).unwrap();
 assert!(simulated.over_loss_cap&&simulated.over_fee_cap);
 assert!(!simulated.may_open_real);
}
