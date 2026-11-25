//! Astra P0-02 / P1-02 / P1-03: normal four-leg fee-adjusted OFFLINE only.
use spotarb_native_market_core::{
    fill_economics::{decimal_to_atoms as atom,signed_decimal_to_atoms as signed,FillEvent,LegFillLedger},
    native_trade2f_asset_fee::SpotFeeFill,
    native_trade2f3_fee_adjusted_cycle_gate::{
        inspect_fee_adjusted_ordinary_four_leg_cycle as review,CycleGateError,
    },
    native_hard_safety::{require_native_mutation_egress,NativeHardSafetyError},
};
// Source-shaped test fill has negative fee value as reported by OKX.
fn sf(side:&str,order:&str,trade:&str,qty:&str,fee_ccy:&str,fee:&str,ts:u64)->SpotFeeFill{
    SpotFeeFill{order_id:order.into(),trade_id:trade.into(),at_ms:ts,
        side:side.into(),inst_id:"BNT-USDT".into(),
        price_quote_e12:atom("20").unwrap(),gross_base_qty_e12:atom(qty).unwrap(),
        fee_ccy:fee_ccy.into(),signed_fee_e12:signed(fee).unwrap()}
}
fn leg(venue:&str,side:&str,order:&str,trade:&str,qty:&str,
       fee_ccy:&str,fee_quote:Option<&str>,t:u64)->LegFillLedger{
    let symbol=if venue=="okx"{"BNT/USDT"}else{"BNTUSDT"};
    let q=atom(qty).unwrap();
    let mut l=LegFillLedger::new(venue,symbol,side,order,order,q).unwrap();
    l.apply_cumulative_truth(q,Some(atom("20").unwrap()),true,
        Some("FILLED"),Some(order),Some(t),1).unwrap();
    l.apply_fill(FillEvent{fill_id:trade.into(),qty_atoms:q,price_atoms:atom("20").unwrap(),
        fee_currency:Some(fee_ccy.into()),
        fee_cost_quote_atoms:fee_quote.map(|v|signed(v).unwrap()),
        trade_time_ms:Some(t)},1).unwrap();
    l
}
#[derive(Clone)]
struct Case {
    spot_buy:LegFillLedger,
    spot_sell:LegFillLedger,
    fut_short:LegFillLedger,
    fut_cover:LegFillLedger,
    fees:Vec<SpotFeeFill>,
}
impl Case{
 fn with_base_buy_fee()->Self{
    let fees=vec![
        sf("buy","okx-buy","okx-t1","1","BNT","-0.001",1000),
        sf("sell","okx-sell","okx-t2","0.999","USDT","-0.01",4000),
    ];
    Self{
       spot_buy:leg("okx","buy","okx-buy","okx-t1","1","BNT",None,1000),
       spot_sell:leg("okx","sell","okx-sell","okx-t2","0.999","USDT",Some("0.01"),4000),
       fut_short:leg("binance","sell","bn-short","bn-t3","0.999","USDT",Some("0.01"),2000),
       fut_cover:leg("binance","buy","bn-cover","bn-t4","0.999","USDT",Some("0.01"),3000),
       fees,
    }
 }
 fn try_gate(&self)->Result<spotarb_native_market_core::native_trade2f3_fee_adjusted_cycle_gate::FeeAdjustedOrdinaryCycleReview,CycleGateError>{
    review(&self.spot_buy,&self.spot_sell,&self.fut_short,&self.fut_cover,&self.fees)
 }
}
#[test]
fn astra_fee_adjusted_native_buy_one_base_net_short_point999_is_consistent_not_live_ready(){
    let c=Case::with_base_buy_fee();
    let r=c.try_gate().unwrap();
    assert_eq!(r.spot_buy_gross_atoms_e12,atom("1").unwrap());
    assert_eq!(r.spot_inventory_delivered_atoms_e12,atom("0.999").unwrap());
    assert_eq!(r.spot_sell_gross_atoms_e12,atom("0.999").unwrap());
    assert_eq!(r.futures_short_open_atoms_e12,atom("0.999").unwrap());
    assert_eq!(r.futures_short_close_atoms_e12,atom("0.999").unwrap());
    assert_eq!(r.base_fee_atoms_e12,atom("0.001").unwrap());
    assert!(r.zero_owned_spot_inventory_residual&&r.exact_qty_hedge_after_native_fee);
    assert!(!r.independent_exchange_account_truth_proven);
    assert!(!r.late_funding_history_proven);
    assert!(!r.may_send_cancel_retry_or_verify_closed);
    assert_eq!(r.definitive_net_pnl_atoms_e12,None);
    // Even a fully self-consistent offline report cannot authorize network send.
    assert_eq!(require_native_mutation_egress(),
        Err(NativeHardSafetyError::MutationCapabilityNotCertified));
}
#[test]
fn astra_all_quote_fees_original_equal_gross_leg_quantities_still_allowed_offline(){
    let mut c=Case::with_base_buy_fee();
    c.fees[0].fee_ccy="USDT".into();
    c.fees[0].signed_fee_e12=signed("-0.01").unwrap();
    c.spot_buy.fills.get_mut("okx-t1").unwrap().fee_currency=Some("USDT".into());
    c.spot_buy.fills.get_mut("okx-t1").unwrap().fee_cost_quote_atoms=Some(signed("0.01").unwrap());
    // Fresh ledger for full-size ordinary equal-gross case
    c.spot_sell=leg("okx","sell","okx-sell","okx-t2","1","USDT",Some("0.01"),4000);
    c.fut_short=leg("binance","sell","bn-short","bn-t3","1","USDT",Some("0.01"),2000);
    c.fut_cover=leg("binance","buy","bn-cover","bn-t4","1","USDT",Some("0.01"),3000);
    c.fees[1].gross_base_qty_e12=atom("1").unwrap();
    let r=c.try_gate().unwrap();
    assert_eq!(r.base_fee_atoms_e12,0);
    assert_eq!(r.spot_inventory_delivered_atoms_e12,atom("1").unwrap());
}
#[test]
fn astra_future_open_gross_size_one_overhedges_net_point999_denied(){
    let mut c=Case::with_base_buy_fee();
    c.fut_short=leg("binance","sell","bn-short","bn-t3","1","USDT",Some("0.01"),2000);
    assert!(c.try_gate().is_err());
}
#[test]
fn astra_future_close_only_point9_when_short_point999_denied(){
    let mut c=Case::with_base_buy_fee();
    c.fut_cover=leg("binance","buy","bn-cover","bn-t4","0.9","USDT",Some("0.01"),3000);
    assert!(c.try_gate().is_err());
}
#[test]
fn astra_sell_requires_unsourced_inventory_after_buy_base_fee_denied(){
    let mut c=Case::with_base_buy_fee();
    c.spot_sell=leg("okx","sell","okx-sell","okx-t2","1","USDT",Some("0.01"),4000);
    c.fees[1].gross_base_qty_e12=atom("1").unwrap();
    assert!(c.try_gate().is_err());
}
#[test]
fn astra_partial_sell_leaves_owned_inventory_dust_denied_as_fully_flat(){
    let mut c=Case::with_base_buy_fee();
    c.spot_sell=leg("okx","sell","okx-sell","okx-t2","0.8","USDT",Some("0.01"),4000);
    c.fees[1].gross_base_qty_e12=atom("0.8").unwrap();
    assert!(c.try_gate().is_err());
}
#[test]
fn astra_sell_spot_before_futures_open_is_not_a_hedged_ordinary_cycle(){
    let mut c=Case::with_base_buy_fee();
    c.spot_sell.fills.get_mut("okx-t2").unwrap().trade_time_ms=Some(1500);
    c.spot_sell.first_fill_time_ms=Some(1500);
    c.spot_sell.last_fill_time_ms=Some(1500);
    c.fees[1].at_ms=1500;
    assert!(c.try_gate().is_err());
}
#[test]
fn astra_futures_cover_before_short_is_not_an_ordinary_cycle(){
    let mut c=Case::with_base_buy_fee();
    c.fut_cover.fills.get_mut("bn-t4").unwrap().trade_time_ms=Some(1500);
    c.fut_cover.first_fill_time_ms=Some(1500);
    c.fut_cover.last_fill_time_ms=Some(1500);
    assert!(c.try_gate().is_err());
}
#[test]
fn astra_unknown_binance_cover_fee_or_partial_terminal_rejected(){
    for fault in 0..3{
        let mut c=Case::with_base_buy_fee();
        match fault{
            0=>c.fut_cover.fills.get_mut("bn-t4").unwrap().fee_cost_quote_atoms=None,
            1=>c.fut_cover.fills.get_mut("bn-t4").unwrap().fee_currency=Some("BNB".into()),
            2=>c.fut_cover.terminal=false,
            _=>unreachable!()
        }
        assert!(c.try_gate().is_err(),"partial/unknown fee case {fault}");
    }
}
#[test]
fn astra_duplicate_futures_order_id_not_accepted_as_original_close(){
    let mut c=Case::with_base_buy_fee();
    c.fut_cover.exchange_order_id=c.fut_short.exchange_order_id.clone();
    assert!(c.try_gate().is_err());
}
#[test]
fn astra_stale_order_timestamp_report_is_rejected(){
    let mut c=Case::with_base_buy_fee();
    c.fut_cover.last_fill_time_ms=Some(3111);
    assert!(c.try_gate().is_err());
}
#[test]
fn astra_wrong_market_side_or_order_identity_denied(){
    for fault in 0..3{
        let mut c=Case::with_base_buy_fee();
        match fault{
            0=>c.fut_cover.side="sell".into(),
            1=>c.fut_cover.symbol="ZECUSDT".into(),
            2=>c.fut_cover.client_order_id=c.fut_short.client_order_id.clone(),
            _=>unreachable!()
        }
        assert!(c.try_gate().is_err());
    }
}
