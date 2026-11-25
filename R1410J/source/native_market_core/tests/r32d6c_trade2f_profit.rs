//! TRADE2F1, pure synthetic exact four-leg economics; no live private GET,
//! no order/cancel/POST, no account permissions and no Native write.
use spotarb_native_market_core::fill_economics::{
 decimal_to_atoms as a,signed_decimal_to_atoms as signed,
 LegFillLedger,FillEvent,
};
use spotarb_native_market_core::native_trade2f_economics::{
 calculate_bounded_trade_result,ObservedMoneyMovements,Trade2FError,
};
fn leg(venue:&str,side:&str,symbol:&str,id:&str,
       qty:&str,px:&str,fee:&str,time:u64)->LegFillLedger{
    let q=a(qty).unwrap();let price=a(px).unwrap();
    let mut l=LegFillLedger::new(venue,symbol,side,id,id,q).unwrap();
    l.apply_cumulative_truth(q,Some(price),true,
        Some("FILLED"),Some(id),Some(time),1).unwrap();
    l.apply_fill(FillEvent{
        fill_id:format!("{id}-trade"),price_atoms:price,qty_atoms:q,
        fee_cost_quote_atoms:Some(signed(fee).unwrap()),
        fee_currency:Some("USDT".into()),trade_time_ms:Some(time),
    },1).unwrap();
    assert!(l.fee_evidence_complete);
    l
}
fn four()->[LegFillLedger;4]{
 [
   leg("okx","buy","BNT-USDT","spot-open","1","20","0.01",1000),
   leg("okx","sell","BNT-USDT","spot-close","1","20.2","0.01",2000),
   leg("binance","sell","BNTUSDT","fut-open","1","20.1","0.01",1100),
   leg("binance","buy","BNTUSDT","fut-close","1","20.15","0.01",2100),
 ]
}
fn m()->ObservedMoneyMovements{
 ObservedMoneyMovements{
    bounded_funding_usdt_e12:None,
    observed_external_flow_usdt_e12:None,
    combined_spot_and_futures_usdt_wallet_delta_e12:None,
    internal_transfer_volume_usdt_e12:None,
 }
}
fn calc(
   legs:&[LegFillLedger;4],fut_pnl:&str,money:&ObservedMoneyMovements,
)->Result<spotarb_native_market_core::native_trade2f_economics::NonAuthorizingTrade2F,
 Trade2FError>{
 calculate_bounded_trade_result(
    &legs[0],&legs[1],&legs[2],&legs[3],signed(fut_pnl).unwrap(),money)
}
#[test]
fn trade2f_four_actual_prices_four_fees_net_before_missing_funding_not_lifetime_profit(){
 let o=calc(&four(),"-0.05",&m()).unwrap();
 assert_eq!(o.matched_base_quantity_e12,a("1").unwrap());
 assert_eq!(o.spot_buy_usdt_e12,signed("20").unwrap());
 assert_eq!(o.spot_sell_usdt_e12,signed("20.2").unwrap());
 assert_eq!(o.futures_short_open_usdt_e12,signed("20.1").unwrap());
 assert_eq!(o.futures_short_cover_usdt_e12,signed("20.15").unwrap());
 assert_eq!(o.spot_gross_e12,signed("0.2").unwrap());
 assert_eq!(o.futures_recomputed_gross_e12,signed("-0.05").unwrap());
 assert_eq!(o.four_trade_fees_usdt_e12,signed("0.04").unwrap());
 assert_eq!(o.trade_only_net_before_funding_e12,signed("0.11").unwrap());
 assert_eq!(o.provisional_net_with_observed_bounded_funding_e12,None);
 assert_eq!(o.definitive_lifetime_net_pnl_e12,None);
 assert!(!o.complete_live_funding_history_proven
   &&!o.complete_external_flow_history_proven
   &&!o.source_signed_exchange_attestation_proven
   &&!o.may_send_cancel_retry_or_verify_closed);
}
#[test]
fn trade2f_transfer_is_separate_from_profit_and_internal_transfers_are_zero_net(){
 let mut v=m();
 v.bounded_funding_usdt_e12=Some(signed("-0.03").unwrap());
 v.observed_external_flow_usdt_e12=Some(signed("10").unwrap());
 v.combined_spot_and_futures_usdt_wallet_delta_e12=Some(signed("10.08").unwrap());
 v.internal_transfer_volume_usdt_e12=Some(a("5").unwrap());
 let r=calc(&four(),"-0.05",&v).unwrap();
 assert_eq!(r.trade_only_net_before_funding_e12,signed("0.11").unwrap());
 assert_eq!(r.provisional_net_with_observed_bounded_funding_e12,
      Some(signed("0.08").unwrap()));
 assert_eq!(r.combined_wallet_delta_excluding_external_flow_e12,
      Some(signed("0.08").unwrap()));
 assert_eq!(r.unexplained_or_unbounded_account_difference_e12,Some(0));
 assert_eq!(r.internal_transfer_volume_usdt_e12,Some(a("5").unwrap()));
 assert!(!r.external_flow_counted_as_trading_profit);
 assert_eq!(r.definitive_lifetime_net_pnl_e12,None);
}
#[test]
fn trade2f_unexplained_wallet_difference_shown_as_gap_never_as_pnl(){
 let mut v=m();
 v.bounded_funding_usdt_e12=Some(0);
 v.observed_external_flow_usdt_e12=Some(signed("10").unwrap());
 v.combined_spot_and_futures_usdt_wallet_delta_e12=Some(signed("10.5").unwrap());
 let r=calc(&four(),"-0.05",&v).unwrap();
 assert_eq!(r.unexplained_or_unbounded_account_difference_e12,
   Some(signed("0.39").unwrap()));
 assert_eq!(r.trade_only_net_before_funding_e12,signed("0.11").unwrap());
 assert_eq!(r.definitive_lifetime_net_pnl_e12,None);
}
#[test]
fn trade2f_missing_external_flow_or_wallet_no_implied_zero(){
 let mut v=m();
 v.bounded_funding_usdt_e12=Some(0);
 v.combined_spot_and_futures_usdt_wallet_delta_e12=Some(signed("100").unwrap());
 let r=calc(&four(),"-0.05",&v).unwrap();
 assert_eq!(r.combined_wallet_delta_excluding_external_flow_e12,None);
 assert_eq!(r.unexplained_or_unbounded_account_difference_e12,None);
}
#[test]
fn trade2f_binance_reported_close_realized_must_match_recomputed_open_sell_minus_cover_buy(){
 assert!(matches!(calc(&four(),"0.05",&m()),Err(Trade2FError::Mismatch(_))));
 assert!(matches!(calc(&four(),"-0.06",&m()),Err(Trade2FError::Mismatch(_))));
}

#[test]
fn trade2f_unmatched_futures_hedge_and_unfinished_order_refuse_complete_roundtrip(){
 let mut x=four();
 x[2]=leg("binance","sell","BNTUSDT","fut-open","0.4","20.1","0.01",1100);
 x[3]=leg("binance","buy","BNTUSDT","fut-close","0.4","20.15","0.01",2100);
 assert!(matches!(calc(&x,"-0.02",&m()),Err(Trade2FError::Mismatch(_))));
 let mut x=four();x[1].terminal=false;
 assert!(matches!(calc(&x,"-0.05",&m()),Err(Trade2FError::Missing(_))));
}
#[test]
fn trade2f_fee_asset_unknown_fee_negative_rebate_and_trade_without_time_fail_closed(){
 for fault in 0..6{
    let mut x=four();
    let fee=x[1].fills.get_mut("spot-close-trade").unwrap();
    match fault{
       0=>fee.fee_currency=Some("BNT".into()),
       1=>fee.fee_currency=None,
       2=>fee.fee_cost_quote_atoms=None,
       3=>fee.fee_cost_quote_atoms=Some(-1),
       4=>fee.trade_time_ms=None,
       5=>fee.trade_time_ms=Some(0),
       _=>unreachable!(),
    }
    assert!(calc(&x,"-0.05",&m()).is_err(),"bad fee/time {fault} accepted");
 }
}
#[test]
fn trade2f_unreconciled_fill_volume_vwap_fee_or_wrong_exchange_rejected(){
 for fault in 0..7{
    let mut x=four();
    match fault{
       0=>x[1].fills.get_mut("spot-close-trade").unwrap().qty_atoms=a("0.9").unwrap(),
       1=>x[1].fills.get_mut("spot-close-trade").unwrap().price_atoms=a("20.1").unwrap(),
       2=>x[1].fills.get_mut("spot-close-trade").unwrap().fee_cost_quote_atoms=Some(a("0.02").unwrap() as i128),
       3=>x[1].fills.get_mut("spot-close-trade").unwrap().fill_id="other-trade".into(),
       4=>x[1].venue="binance".into(),
       5=>x[1].side="buy".into(),
       6=>x[2].symbol="ZECUSDT".into(),
       _=>unreachable!(),
    }
    assert!(calc(&x,"-0.05",&m()).is_err(),"tampered fill {fault} accepted");
 }
}
#[test]
fn trade2f_reused_order_ids_and_closing_fill_predating_entry_rejected(){
 let mut x=four();
 x[1].client_order_id=x[0].client_order_id.clone();
 assert!(matches!(calc(&x,"-0.05",&m()),Err(Trade2FError::Mismatch(_))));
 let mut x=four();
 x[1].fills.get_mut("spot-close-trade").unwrap().trade_time_ms=Some(999);
 assert!(matches!(calc(&x,"-0.05",&m()),Err(Trade2FError::Mismatch(_))));
}
#[test]
fn trade2f_tiny_subquantum_usdt_value_rejected_instead_of_rounding_profit(){
 let tiny=leg("okx","buy","BNT-USDT","spot-open",
      "0.000000000001","0.000000000001","0",1000);
 let mut x=four();x[0]=tiny;
 assert!(matches!(calc(&x,"-0.05",&m()),Err(Trade2FError::Unsupported(_))));
}
#[test]
fn trade2f_arithmetic_overflow_rejected_not_wrapped(){
 let mut x=four();
 x[0].fills.get_mut("spot-open-trade").unwrap().price_atoms=u128::MAX;
 assert!(matches!(calc(&x,"-0.05",&m()),Err(Trade2FError::Overflow)));
 let mut x=four();let mut v=m();
 v.bounded_funding_usdt_e12=Some(i128::MAX);
 assert!(matches!(calc(&x,"-0.05",&v),Err(Trade2FError::Overflow)));
 // Do not infer that a change in wallets alone is profit.
 let mut v=m();v.combined_spot_and_futures_usdt_wallet_delta_e12=Some(i128::MIN);
 v.observed_external_flow_usdt_e12=Some(1);
 assert!(matches!(calc(&x,"-0.05",&v),Err(Trade2FError::Overflow)));
}

#[test]
fn trade2f_wrong_cross_venue_symbol_and_missing_true_exchange_order_id_deny(){
 let mut l=four();l[2].symbol="ZECUSDT".into();
 l[3].symbol="ZECUSDT".into();
 assert!(matches!(calc(&l,"-0.05",&m()),Err(Trade2FError::Mismatch(_))));
 let mut l=four();l[3].exchange_order_id=None;
 assert!(matches!(calc(&l,"-0.05",&m()),Err(Trade2FError::Mismatch(_))));
 let mut l=four();l[0].client_order_id=String::new();
 assert!(matches!(calc(&l,"-0.05",&m()),Err(Trade2FError::Mismatch(_))));
}
#[test]
fn trade2f_two_fills_per_leg_use_individual_quote_cashflow_and_commissions(){
 let mut legs=four();
 let mut opening=LegFillLedger::new(
    "okx","BNT-USDT","buy","spot-open","spot-open",a("1").unwrap()).unwrap();
 opening.apply_cumulative_truth(a("1").unwrap(),Some(a("20.2").unwrap()),
    true,Some("FILLED"),Some("spot-open"),Some(1002),1).unwrap();
 for (trade_id,qty,price,fee,t) in [
    ("original-part-1","0.5","20","0.005",1000),
    ("original-part-2","0.5","20.4","0.005",1001)
 ]{
   opening.apply_fill(FillEvent{
     fill_id:trade_id.into(),price_atoms:a(price).unwrap(),qty_atoms:a(qty).unwrap(),
     fee_cost_quote_atoms:Some(signed(fee).unwrap()),fee_currency:Some("USDT".into()),
     trade_time_ms:Some(t),
   },1).unwrap();
 }
 legs[0]=opening;
 let result=calc(&legs,"-0.05",&m()).unwrap();
 assert_eq!(result.spot_buy_usdt_e12,signed("20.2").unwrap());
 assert_eq!(result.spot_gross_e12,0);
 assert_eq!(result.four_trade_fees_usdt_e12,signed("0.04").unwrap());
 assert_eq!(result.trade_only_net_before_funding_e12,signed("-0.09").unwrap());
 assert_eq!(result.definitive_lifetime_net_pnl_e12,None);
}
