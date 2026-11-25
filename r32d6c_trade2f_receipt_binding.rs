//! TRADE2F2B: fully synthetic four-leg source-shaped transaction matching.
//! Pure offline data consistency only. No signed private HTTP, no trades.
use spotarb_native_market_core::{
 fill_economics::{LegFillLedger,FillEvent,decimal_to_atoms as a,signed_decimal_to_atoms as s},
 native_trade2f_income_window::{ObservedBinanceIncome,ObservedOkxBill},
 native_trade2f_receipt_binding::{reconcile_supplied_four_leg_receipts,ReceiptBindError,BoundedReceiptBinding},
};
fn leg(venue:&str,symbol:&str,side:&str,id:&str,
 qty:&str,price:&str,fee:&str,t:u64)->LegFillLedger{
    let qty=a(qty).unwrap();let px=a(price).unwrap();
    let mut x=LegFillLedger::new(venue,symbol,side,id,id,qty).unwrap();
    x.apply_cumulative_truth(qty,Some(px),true,Some("FILLED"),Some(id),Some(t),1).unwrap();
    x.apply_fill(FillEvent{
      fill_id:format!("{id}-fill"),qty_atoms:qty,price_atoms:px,
      fee_currency:Some("USDT".into()),fee_cost_quote_atoms:Some(s(fee).unwrap()),
      trade_time_ms:Some(t),
    },1).unwrap();
    x
}
#[derive(Clone)]
struct Toy{
    legs:[LegFillLedger;4],
    bin:Vec<ObservedBinanceIncome>,base:Vec<ObservedOkxBill>,
    quote:Vec<ObservedOkxBill>,
}
impl Toy{
 fn new()->Self{
  let legs=[
   leg("okx","BNT-USDT","buy","sp-open","1","20","0.01",101_000),
   leg("okx","BNT-USDT","sell","sp-close","1","20.2","0.01",102_000),
   leg("binance","BNTUSDT","sell","fut-open","1","20.1","0.01",101_050),
   leg("binance","BNTUSDT","buy","fut-close","1","20.15","0.01",102_050),
  ];
  let bin=vec![
    bi("COMMISSION","11","BNTUSDT","fut-open-fill","-0.01",101_060),
    bi("COMMISSION","12","BNTUSDT","fut-close-fill","-0.01",102_060),
    bi("REALIZED_PNL","13","BNTUSDT","fut-close-fill","-0.05",102_065),
    bi("FUNDING_FEE","14","BNTUSDT","","-0.03",101_550),
    bi("TRANSFER","14","","","10",101_500),
  ];
  let base=vec![];
  let quote=vec![
     bill("9101","USDT","sp-open","sp-open-fill","-20.01","79.99",101_030,"1"),
     bill("9102","USDT","sp-close","sp-close-fill","20.19","100.18",102_040,"2"),
  ];
  Self{legs,bin,base,quote}
 }
 fn go(&self)->Result<BoundedReceiptBinding,ReceiptBindError>{
   reconcile_supplied_four_leg_receipts(
       &self.legs[0],&self.legs[1],&self.legs[2],&self.legs[3],
       s("-0.05").unwrap(),(100_000,103_000),
       &self.bin,&self.base,&self.quote)
 }
}
fn bi(kind:&str,id:&str,symbol:&str,trade:&str,amount:&str,at:u64)->ObservedBinanceIncome{
 ObservedBinanceIncome{
    income_type:kind.into(),tran_id:id.into(),symbol:symbol.into(),asset:"USDT".into(),
    signed_amount_e12:s(amount).unwrap(),at_ms:at,trade_id:trade.into(),
 }
}
#[allow(clippy::too_many_arguments)]
fn bill(id:&str,ccy:&str,ord:&str,trade:&str,change:&str,balance:&str,
          at:u64,sub:&str)->ObservedOkxBill{
 ObservedOkxBill{
    bill_id:id.into(),currency:ccy.into(),signed_change_e12:s(change).unwrap(),
    cash_after_e12:s(balance).unwrap(),at_ms:at,
    bill_type:"2".into(),bill_sub_type:sub.into(),
    order_id:ord.into(),trade_id:trade.into(),
    observed_signed_fee_without_sourced_ccy_e12:None,
 }
}
#[test]
fn p104_native_owner_cash_accepts_exactly_one_original_bill_per_spot_fill(){
 let mut t=Toy::new();
 // Real original billId lives in quote ccy for both fills; the BASE
 // movement is inferred from original fill, not invented as another bill.
 t.base.clear();
 let r=t.go().unwrap();
 assert_eq!(r.spot_fill_ids.len(),2);
 assert_eq!(r.spot_base_delta_e12,0);
 assert_eq!(r.spot_quote_delta_e12,s("0.18").unwrap());
 assert!(!r.independent_live_source_attested&&!r.may_send_cancel_retry_or_verify_closed);
}
#[test]
fn trade2f2b_exact_one_original_spot_bill_per_fill_and_binance_income_no_double_count(){
 let r=Toy::new().go().unwrap();
 assert!(r.supplied_trade_and_cash_receipts_reconcile);
 assert_eq!(r.spot_fill_ids,vec!["sp-close-fill".to_string(),"sp-open-fill".into()]);
 assert_eq!(r.futures_fill_ids,vec!["fut-close-fill".to_string(),"fut-open-fill".into()]);
 assert_eq!(r.spot_base_delta_e12,0);
 assert_eq!(r.spot_quote_delta_e12,s("0.18").unwrap());
 assert_eq!(r.binance_commissions_e12,s("0.02").unwrap());
 assert_eq!(r.futures_realized_income_e12,s("-0.05").unwrap());
 assert_eq!(r.observed_bounded_funding_e12,s("-0.03").unwrap());
 assert_eq!(r.observed_unclassified_transfer_e12,s("10").unwrap());
 assert_eq!(r.trade_only_net_e12,s("0.11").unwrap());
 assert_eq!(r.window_adjusted_net_e12,s("0.08").unwrap());
 assert!(!r.independent_live_source_attested
       &&!r.complete_retained_income_history_proven
       &&!r.complete_wallet_transfer_history_proven
       &&!r.all_lifetime_funding_proven
       &&!r.may_send_cancel_retry_or_verify_closed);
 assert_eq!(r.definitive_lifetime_net_pnl_e12,None);
}
#[test]
fn p104_native_original_base_and_mixed_cash_bill_shapes(){
 for mode in 0..2{
  let mut t=Toy::new();
  // Replace one or both original USDT bills with actual BASE bills
  // (different asset ccy, SAME original fill identity, no second bill).
  let opening=t.quote.remove(0);
  t.base.push(bill(&opening.bill_id,"BNT","sp-open","sp-open-fill",
       "1","1",101_030,"1"));
  if mode==1{
   let closing=t.quote.remove(0);
   t.base.push(bill(&closing.bill_id,"BNT","sp-close","sp-close-fill",
       "-1","0",102_040,"2"));
  }
  let r=t.go().unwrap();
  assert_eq!(r.spot_base_delta_e12,0);
  assert_eq!(r.spot_quote_delta_e12,s("0.18").unwrap());
  assert_eq!(r.definitive_lifetime_net_pnl_e12,None);
  assert!(!r.independent_live_source_attested);
 }
}
#[test]
fn p104_native_fails_closed_cross_asset_reused_bill_id_and_noncanonical_id(){
 let mut t=Toy::new();
 t.base.push(bill("9101","BNT","sp-close","sp-close-fill",
         "-1","0",102_040,"2"));
 t.quote.remove(1);
 assert!(t.go().is_err(),"one bill ID cannot belong to two original trades");
 let mut t=Toy::new();
 t.quote[0].bill_id="009101".into();
 assert!(t.go().is_err(),"noncanonical original billId denied");
 let mut t=Toy::new();
 t.quote[1].signed_change_e12=s("20.2").unwrap();
 assert!(t.go().is_err(),"wrong source amount denied");
 let mut t=Toy::new();
 t.legs[0].fills.get_mut("sp-open-fill").unwrap().fee_currency=Some("BNT".into());
 assert!(t.go().is_err(),"narrow USDT-only ledger cannot accept BASE fee");
}
#[test]
fn trade2f2b_no_funding_rows_is_observed_zero_not_complete_funding_history(){
 let mut t=Toy::new();t.bin.retain(|x|x.income_type!="FUNDING_FEE");
 let r=t.go().unwrap();
 assert_eq!(r.observed_bounded_funding_e12,0);
 assert_eq!(r.window_adjusted_net_e12,s("0.11").unwrap());
 assert!(!r.all_lifetime_funding_proven);
 assert_eq!(r.definitive_lifetime_net_pnl_e12,None);
}

#[test]
fn trade2f2b_missing_duplicate_extra_wrong_okx_bill_rejected_without_backfilled_profit(){
 for fault in 0..14{
   let mut t=Toy::new();
   match fault{
    0=>{t.quote.remove(0);},
    1=>{t.quote.pop();},
    2=>{t.quote.push(t.quote[0].clone());},
    3=>{t.quote[1].bill_id=t.quote[0].bill_id.clone();},
    4=>{t.quote[0].signed_change_e12=s("-20").unwrap();},
    5=>{t.quote[1].signed_change_e12=s("20.2").unwrap();},
    6=>{t.base.push(bill("9001","BNT","sp-open","sp-open-fill",
             "1","1",101_030,"1"));},
    7=>{t.quote[1].order_id="foreign-order".into();},
    8=>{t.quote[0].trade_id="foreign-trade".into();},
    9=>{t.quote[0].currency="ZEC".into();},
    10=>{t.quote[1].bill_sub_type="1".into();},
    11=>{t.quote[0].at_ms=99_000;},
    12=>{t.quote[1].at_ms=104_000;},
    13=>{t.quote[0].bill_type="1".into();},
    _=>unreachable!(),
   }
   assert!(t.go().is_err(),"unsafe OKX bill case {fault} accepted");
 }
}
#[test]
fn trade2f2b_wrong_future_income_fee_pnl_duplicates_and_transfer_rejected(){
 for fault in 0..19{
   let mut t=Toy::new();
   match fault{
    0=>{t.bin.retain(|x|x.trade_id!="fut-open-fill");},
    1=>{t.bin.retain(|x|x.income_type!="REALIZED_PNL");},
    2=>{t.bin.push(t.bin[0].clone());},
    3=>{t.bin[1].tran_id=t.bin[0].tran_id.clone();},
    4=>{t.bin[0].signed_amount_e12=s("-0.02").unwrap();},
    5=>{t.bin[2].signed_amount_e12=s("-0.06").unwrap();},
    6=>{t.bin[0].trade_id="missing-original-fill".into();},
    7=>{t.bin[2].trade_id="fut-open-fill".into();},
    8=>{t.bin[1].symbol="ZECUSDT".into();},
    9=>{t.bin[3].symbol="ZECUSDT".into();},
    10=>{t.bin[4].symbol="BNTUSDT".into();},
    11=>{t.bin[4].trade_id="fut-open-fill".into();},
    12=>{t.bin[1].asset="BNB".into();},
    13=>{t.bin[1].at_ms=100_000;},
    14=>{t.bin[3].at_ms=99_999;},
    15=>{t.bin[0].income_type="REFERRAL_KICKBACK".into();},
    16=>{t.bin[0].tran_id="not-numeric".into();},
    17=>{t.bin[0].signed_amount_e12=1;},
    18=>{t.bin.push(bi("COMMISSION","999","BNTUSDT",
                             "fut-close-fill","-0.01",102_060));},
    _=>unreachable!(),
   }
   assert!(t.go().is_err(),"unsafe Binance income case {fault} accepted");
 }
}
#[test]
fn trade2f2b_missing_lifetime_source_and_foreign_four_leg_amount_refused(){
 for fault in 0..5{
    let mut t=Toy::new();
    match fault{
      0=>t.legs[0].fills.get_mut("sp-open-fill").unwrap().fee_currency=Some("BNT".into()),
      1=>t.legs[1].fills.get_mut("sp-close-fill").unwrap().qty_atoms=a("0.9").unwrap(),
      2=>t.legs[3].symbol="ZECUSDT".into(),
      3=>t.legs[2].fills.get_mut("fut-open-fill").unwrap().price_atoms=a("22").unwrap(),
      4=>t.legs[0].exchange_order_id=None,
      _=>unreachable!(),
    }
    assert!(t.go().is_err(),"untrusted original four-leg source {fault} passed");
 }
 let t=Toy::new();
 assert!(reconcile_supplied_four_leg_receipts(
    &t.legs[0],&t.legs[1],&t.legs[2],&t.legs[3],
    s("-0.05").unwrap(),(0,103000),&t.bin,&t.base,&t.quote).is_err());
}

#[test]
fn trade2f2d_multiple_independent_binance_fee_and_pnl_receipts_aggregate_exactly(){
 // Each income class + tranId is unique; one execution can have
 // multiple distinct receipts, but exact original fee and PnL MUST match.
 let mut t=Toy::new();
 t.bin[0].signed_amount_e12=s("-0.003").unwrap();
 t.bin.push(bi("COMMISSION","101","BNTUSDT","fut-open-fill",
     "-0.007",101_075));
 t.bin[2].signed_amount_e12=s("-0.02").unwrap();
 t.bin.push(bi("REALIZED_PNL","102","BNTUSDT","fut-close-fill",
     "-0.03",102_075));
 let r=t.go().unwrap();
 assert_eq!(r.binance_commissions_e12,s("0.02").unwrap());
 assert_eq!(r.futures_realized_income_e12,s("-0.05").unwrap());
 assert_eq!(r.trade_only_net_e12,s("0.11").unwrap());
 assert_eq!(r.window_adjusted_net_e12,s("0.08").unwrap());
 assert_eq!(r.definitive_lifetime_net_pnl_e12,None);
 assert!(!r.may_send_cancel_retry_or_verify_closed);
}
#[test]
fn trade2f2d_multiple_fee_components_duplicate_identity_overbill_and_foreign_trade_deny(){
 for mode in 0..7{
    let mut t=Toy::new();
    t.bin[0].signed_amount_e12=s("-0.003").unwrap();
    t.bin.push(bi("COMMISSION","101","BNTUSDT","fut-open-fill",
        "-0.007",101_075));
    match mode{
      0=>t.bin.last_mut().unwrap().signed_amount_e12=s("-0.008").unwrap(),
      1=>t.bin.last_mut().unwrap().tran_id="11".into(),
      2=>t.bin.last_mut().unwrap().signed_amount_e12=s("0.007").unwrap(),
      3=>t.bin.last_mut().unwrap().trade_id="foreign-fill".into(),
      4=>t.bin.last_mut().unwrap().asset="BNB".into(),
      5=>t.bin.last_mut().unwrap().at_ms=101_049,
      6=>t.bin.last_mut().unwrap().income_type="COMMISSION_REBATE".into(),
      _=>unreachable!(),
    }
    assert!(t.go().is_err(),"commission partial receipt unsafe fault={mode}");
 }
}
#[test]
fn trade2f2d_bill_only_nonzero_fee_denied_without_transaction_fee_currency(){
 let mut t=Toy::new();
 t.quote[0].observed_signed_fee_without_sourced_ccy_e12=Some(0);
 t.quote[1].observed_signed_fee_without_sourced_ccy_e12=Some(0);

 assert!(t.go().unwrap().supplied_trade_and_cash_receipts_reconcile);
 // A nonzero fee copied from an OKX bill is NOT safely
 // denominated in that bill's balance currency; feeCcy comes
 // from a separate transaction detail by the same billId.
 t.quote[0].observed_signed_fee_without_sourced_ccy_e12=Some(s("-0.01").unwrap());
 assert!(t.go().is_err());
 for mode in 0..4{
    let mut t=Toy::new();
    match mode{
      0=>t.quote[0].observed_signed_fee_without_sourced_ccy_e12=Some(s("-0.02").unwrap()),
      1=>t.quote[1].observed_signed_fee_without_sourced_ccy_e12=Some(s("0.01").unwrap()),
      2=>t.quote[0].observed_signed_fee_without_sourced_ccy_e12=Some(s("0.0001").unwrap()),
      3=>t.quote[1].observed_signed_fee_without_sourced_ccy_e12=Some(s("-0.005").unwrap()),
      _=>unreachable!(),
    }
    assert!(t.go().is_err(),"OKX bill-only fee fault={mode}");
 }
}
