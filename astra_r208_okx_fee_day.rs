//! Astra R208: fill-timestamp fee floor in isolated OFFLINE review.
use spotarb_native_market_core::{
 native_trading_config::NativeTradingConfig,
 native_okx_fee_day_r208_review::{
  inspect_okx_fee_day_lower_bound_r208,SourcedOkxFeeFillR208,OkxFeeDayError},
 native_daily_risk::{UTC_DAY_MS,YEKT_OFFSET_MS,yekt_day_index},
};
const YAML:&str=include_str!("../config/native_trading.yaml");
const E12:u128=1_000_000_000_000;
fn cfg()->NativeTradingConfig{NativeTradingConfig::parse(YAML).unwrap()}
fn now()->u64{22_000*UTC_DAY_MS-YEKT_OFFSET_MS+120_000}
fn fee(id:&str,at:u64,amount:u128)->SourcedOkxFeeFillR208{
 SourcedOkxFeeFillR208{
  account_owner:"research-owner".into(),order_id:"o1".into(),
  trade_id:id.into(),symbol:"BTC-USDT".into(),
  fee_ccy:"USDT".into(),execution_at_ms:at,
  quoted_fee_cost_e12:amount,source_row_is_paper:false,
 }
}
#[test]fn r208_midnight_fee_charged_on_execution_day(){
 let t=now();
 let prior=fee("prior",t-180_000,5*E12);
 let current=fee("current",t-60_000,2*E12);
 assert_ne!(yekt_day_index(prior.execution_at_ms).unwrap(),
            yekt_day_index(current.execution_at_ms).unwrap());
 let a=inspect_okx_fee_day_lower_bound_r208(
     &cfg(),t,&[prior,current],&[]).unwrap();
 assert_eq!(a.known_today_fill_count,1);
 assert_eq!(a.known_earlier_fill_count,1);
 assert_eq!(a.known_quote_okx_fees_e12,2*E12);
 assert_eq!(a.quote_fee_cap_e12,7*E12);
 assert!(!a.cap_reached_by_known_fees);
 assert!(!a.below_cap_authoritatively_proven);
 assert!(!a.complete_day_fee_history_proven);
 assert!(!a.exchange_source_authenticated&&!a.may_open_real);
}
#[test]fn r208_open_unknown_not_silent_zero_fee(){
 let t=now();let unknown=vec!["unknown-order".to_string()];
 let fills=[fee("first",t-100,4*E12),fee("second",t-200,3*E12)];
 let a=inspect_okx_fee_day_lower_bound_r208(
    &cfg(),t,&fills,&unknown).unwrap();
 assert_eq!(a.known_quote_okx_fees_e12,7*E12);
 assert!(a.cap_reached_by_known_fees);
 assert_eq!(a.open_or_unknown_order_count,1);
 assert!(!a.may_open_real);
 let empty=inspect_okx_fee_day_lower_bound_r208(&cfg(),t,&[],&unknown).unwrap();
 assert_eq!(empty.known_quote_okx_fees_e12,0);
 assert!(!empty.below_cap_authoritatively_proven);
}
#[test]fn r208_duplicate_fee_fill_or_unknown_denied(){
 let t=now();let original=fee("t1",t-1,E12);
 let mut forged=original.clone();forged.order_id="o2".into();
 assert_eq!(inspect_okx_fee_day_lower_bound_r208(
   &cfg(),t,&[original,forged],&[]),Err(OkxFeeDayError::Duplicate));
 let unknown=vec!["u1".into(),"u1".into()];
 assert_eq!(inspect_okx_fee_day_lower_bound_r208(
   &cfg(),t,&[],&unknown),Err(OkxFeeDayError::Duplicate));
 let mut other=fee("t1",t-2,E12);other.symbol="ETH-USDT".into();
 assert_eq!(inspect_okx_fee_day_lower_bound_r208(
   &cfg(),t,&[fee("t1",t-1,E12),other],&[])
   .unwrap().known_today_fill_count,2);
}
#[test]fn r208_unsupported_fee_assets_and_model_rows_denied(){
 let t=now();
 for cc in ["BTC","OKB","BNB",""]{
  let mut a=fee("x",t-100,E12);a.fee_ccy=cc.into();
  assert_eq!(inspect_okx_fee_day_lower_bound_r208(
    &cfg(),t,&[a],&[]),Err(OkxFeeDayError::Unsupported));
 }
 let mut a=fee("x",t-100,E12);a.source_row_is_paper=true;
 assert_eq!(inspect_okx_fee_day_lower_bound_r208(
   &cfg(),t,&[a],&[]),Err(OkxFeeDayError::Unsupported));
}
#[test]fn r208_tampered_config_future_fee_and_sum_overflow_denied(){
 let t=now();
 let mut changed=cfg();
 changed.policy.daily_okx_fee_cap_e18=1;
 assert_eq!(inspect_okx_fee_day_lower_bound_r208(
   &changed,t,&[],&[]),Err(OkxFeeDayError::Config));
 let future=fee("future",t+1,E12);
 assert_eq!(inspect_okx_fee_day_lower_bound_r208(
   &cfg(),t,&[future],&[]),Err(OkxFeeDayError::Unsupported));
 let a=fee("large",t-2,u128::MAX);
 let b=fee("small",t-1,1);
 assert_eq!(inspect_okx_fee_day_lower_bound_r208(
   &cfg(),t,&[a,b],&[]),Err(OkxFeeDayError::Overflow));
 assert_eq!(inspect_okx_fee_day_lower_bound_r208(
   &cfg(),0,&[],&[]),Err(OkxFeeDayError::Timestamp));
}
