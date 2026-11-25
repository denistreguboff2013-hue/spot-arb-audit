//! TRADE1 read-only 10-USDT quote cap for OKX spot + Binance USDM.
//! No native order placement, credentials, production configuration or live data.
use spotarb_native_market_core::{
 market_metadata::NativeMarketMetadata,
 native_micro_trade_size::{
   preview_micro_pair,MicroSizeDeny,HARD_MICRO_CAP_E12,
   MAX_METADATA_AGE_MS,
 },
 fill_economics::decimal_to_atoms,
};
fn n(v:&str)->u128{decimal_to_atoms(v).unwrap()}
fn metadata()->NativeMarketMetadata{
    NativeMarketMetadata{
        base:"BNT".to_owned(),
        spot_symbol:"BNT-USDT".to_owned(),future_symbol:"BNTUSDT".to_owned(),
        spot_min_amount:Some("0.1".to_owned()),
        future_min_base:Some("0.1".to_owned()),
        spot_min_cost:"0".to_owned(),future_min_cost:"5".to_owned(),
        spot_step:Some("0.1".to_owned()),future_step_base:Some("0.1".to_owned()),
        contract_size:"1".into(),
        buy_exchange:"okx_spot".into(),sell_exchange:"binance_futures".into(),
        route_label:spotarb_native_market_core::market_metadata::ROUTE_LABEL.into(),
        static_reason:None,
        loaded_ns:1234,loaded_wall_ms:1_780_000_000_000,
        spot_tick:Some("0.01".into()),future_tick:Some("0.01".into()),
        spot_status:"live".into(),future_status:"TRADING".into(),
        spot_max_amount:Some("1000".into()),future_max_base:Some("500".into()),
    }
}
#[test]
fn trade1_exact_common_lot_both_notional_under_ten_and_no_real_permit(){
 let m=metadata();let cap=HARD_MICRO_CAP_E12;
 let x=preview_micro_pair(&m,m.loaded_wall_ms+3000,n("20"),n("21"),cap).unwrap();
 assert_eq!(x.qty_e12,n("0.4"));
 assert_eq!(x.estimated_spot_cost_e12,n("8"));
 assert_eq!(x.estimated_futures_notional_e12,n("8.4"));
 assert_eq!(x.spot_symbol,"BNT-USDT");
 assert_eq!(x.futures_symbol,"BNTUSDT");
 assert_eq!(x.metadata_age_ms,3000);
 assert!(!x.may_send_order&&!x.execution_liquidity_proven);
 assert!(!x.execution_fees_proven&&!x.may_mark_hedged);
}
#[test]
fn trade1_incompatible_steps_are_not_rounded_leg_by_leg(){
 let mut m=metadata();m.spot_step=Some("0.1".into());
 m.future_step_base=Some("0.25".into());
 let r=preview_micro_pair(&m,m.loaded_wall_ms+1,n("15"),n("16"),n("10")).unwrap();
 assert_eq!(r.qty_e12,n("0.5"));
 assert_eq!(r.estimated_spot_cost_e12,n("7.5"));
 assert_eq!(r.estimated_futures_notional_e12,n("8"));
 assert!(!r.may_send_order);
 let z=preview_micro_pair(&m,m.loaded_wall_ms+1,n("20"),n("21"),n("10"));
 assert_eq!(z,Err(MicroSizeDeny::GridImpossible));
}
#[test]
fn trade1_ten_dollars_below_binance_min_notional_denied(){
 let mut m=metadata();m.future_min_cost="11".into();
 assert_eq!(preview_micro_pair(&m,m.loaded_wall_ms,n("20"),n("21"),n("10")),
    Err(MicroSizeDeny::TooSmall));
 m.future_min_cost="5".into();m.spot_min_cost="9".into();
 assert_eq!(preview_micro_pair(&m,m.loaded_wall_ms,n("20"),n("21"),n("10")),
    Err(MicroSizeDeny::TooSmall));
 m.spot_min_cost="0".into();m.future_min_base=Some("1".into());
 assert_eq!(preview_micro_pair(&m,m.loaded_wall_ms,n("20"),n("21"),n("10")),
    Err(MicroSizeDeny::TooSmall));
}
#[test]
fn trade1_stale_future_metadata_and_malformed_exchange_lots_denied(){
 let mut m=metadata();let t=m.loaded_wall_ms;
 assert_eq!(preview_micro_pair(&m,t+MAX_METADATA_AGE_MS+1,n("20"),n("21"),n("10")),
    Err(MicroSizeDeny::Stale));
 m.loaded_wall_ms=t+100_000;
 assert_eq!(preview_micro_pair(&m,t,n("20"),n("21"),n("10")),
    Err(MicroSizeDeny::Stale));
 m=metadata();m.future_status="BREAK".into();
 assert_eq!(preview_micro_pair(&m,t,n("20"),n("21"),n("10")),
    Err(MicroSizeDeny::Inactive));
 m=metadata();m.contract_size="0.1".into();
 assert_eq!(preview_micro_pair(&m,t,n("20"),n("21"),n("10")),
    Err(MicroSizeDeny::UnsupportedContract));
 m=metadata();m.future_step_base=Some("oops".into());
 assert_eq!(preview_micro_pair(&m,t,n("20"),n("21"),n("10")),
    Err(MicroSizeDeny::MissingMetadata));
 m=metadata();m.spot_step=None;
 assert_eq!(preview_micro_pair(&m,t,n("20"),n("21"),n("10")),
    Err(MicroSizeDeny::MissingMetadata));
 m=metadata();m.spot_tick=None;
 assert_eq!(preview_micro_pair(&m,t,n("20"),n("21"),n("10")),
    Err(MicroSizeDeny::MissingMetadata));
 m=metadata();m.spot_tick=Some("0.03".into());
 assert_eq!(preview_micro_pair(&m,t,n("20"),n("21"),n("10")),
    Err(MicroSizeDeny::Invalid));
 m=metadata();m.static_reason=Some("MARKET_INACTIVE".into());
 assert_eq!(preview_micro_pair(&m,t,n("20"),n("21"),n("10")),
    Err(MicroSizeDeny::MissingMetadata));
}
#[test]
fn trade1_max_amount_cap_and_quote_over_ten_denied(){
 let mut m=metadata();let t=m.loaded_wall_ms;
 assert_eq!(preview_micro_pair(&m,t,n("20"),n("21"),n("10.01")),
    Err(MicroSizeDeny::Invalid));
 assert_eq!(preview_micro_pair(&m,t,n("20"),n("21"),0),
    Err(MicroSizeDeny::Invalid));
 assert_eq!(preview_micro_pair(&m,t,0,n("21"),n("10")),
    Err(MicroSizeDeny::Invalid));
 m.spot_max_amount=Some("0.3".into());
 assert_eq!(preview_micro_pair(&m,t,n("20"),n("21"),n("10")),
    Err(MicroSizeDeny::OverMaximum));
 m=metadata();m.future_max_base=Some("0.3".into());
 assert_eq!(preview_micro_pair(&m,t,n("20"),n("21"),n("10")),
    Err(MicroSizeDeny::OverMaximum));
}
#[test]
fn trade1_large_inputs_protected_from_math_overflow(){
 let m=metadata();let t=m.loaded_wall_ms;
 assert_eq!(preview_micro_pair(&m,t,u128::MAX,n("21"),n("10")),
    Err(MicroSizeDeny::Invalid));
 let mut m=metadata();m.spot_step=Some("999999999999999".into());
 m.future_step_base=Some("999999999999998".into());
 assert!(matches!(preview_micro_pair(&m,t,n("1"),n("1"),n("10")),
    Err(MicroSizeDeny::Arithmetic)|Err(MicroSizeDeny::GridImpossible)));
}
