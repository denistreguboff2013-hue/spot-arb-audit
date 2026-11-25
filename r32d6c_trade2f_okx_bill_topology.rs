//! TRADE2F2F official OKX transaction-detail billId / currency model
//! on fully fake API-shaped JSON. No real REST/exchange keys/trading.
use serde_json::{json,Value};
use spotarb_native_market_core::{
    fill_economics::{decimal_to_atoms as a,signed_decimal_to_atoms as s},
    native_trade2f_okx_bill_topology::{
      inspect_documented_okx_trade_detail_to_bill_id as inspect,
      OkxBillTopologyError,
    },
};
#[derive(Clone)]
struct Fixture{
    fills:Vec<Value>,
    bills:Vec<Value>,
}
impl Fixture{
    fn new()->Self{
       Self{
          fills:vec![
            json!({"instType":"SPOT","instId":"BNT-USDT",
              "tradeQuoteCcy":"USDT","side":"buy","subType":"1",
              "ordId":"701","tradeId":"901","billId":"801",
              "fillSz":"1","fillPx":"20","fee":"-0.001",
              "feeCcy":"BNT","fillTime":"2000","ts":"2001"}),
            json!({"instType":"SPOT","instId":"BNT-USDT",
              "tradeQuoteCcy":"USDT","side":"sell","subType":"2",
              "ordId":"702","tradeId":"902","billId":"802",
              "fillSz":"0.999","fillPx":"20.2","fee":"-0.01",
              "feeCcy":"USDT","fillTime":"3000","ts":"3001"}),
          ],
          // Official bills-archive ccy is the BALANCE asset. It does
          // not tell the fee currency, which is from fills-history.
          // Newest bill first; cash after - change = prior cash.
          bills:vec![
            json!({"instType":"SPOT","instId":"BNT-USDT",
              "type":"2","subType":"2","ccy":"USDT","billId":"802",
              "ordId":"702","tradeId":"902","balChg":"20.1698",
              "bal":"100.1698","fee":"-0.01","ts":"3005"}),
            json!({"instType":"SPOT","instId":"BNT-USDT",
              "type":"2","subType":"1","ccy":"USDT","billId":"801",
              "ordId":"701","tradeId":"901","balChg":"-20",
              "bal":"80","fee":"-0.001","ts":"2005"}),
          ],
       }
    }
    fn check(&self)->Result<
       spotarb_native_market_core::native_trade2f_okx_bill_topology::DiagnosticOkxTopology,
       OkxBillTopologyError>{
       inspect("BNT/USDT",a("1").unwrap(),1000,4000,&self.fills,&self.bills)
    }
}
#[test]
fn p104_one_source_bill_two_derived_currency_movements_without_fake_bill(){
 let p=Fixture::new().check().unwrap();
 assert_eq!(p.matched_bill_ids.len(),2);
 assert_eq!(p.per_fill_derived_cash_not_source_bills.len(),2);
 let buy=p.per_fill_derived_cash_not_source_bills.iter()
   .find(|r|r.original_bill_id=="801").unwrap();
 assert_eq!(buy.original_trade_id,"901");
 assert_eq!(buy.original_bill_ccy,"USDT");
 assert_eq!(buy.original_bill_balance_change_e12,s("-20").unwrap());
 assert_eq!(buy.derived_base_delta_e12,s("0.999").unwrap());
 assert_eq!(buy.derived_quote_delta_e12,s("-20").unwrap());
 let sell=p.per_fill_derived_cash_not_source_bills.iter()
   .find(|r|r.original_bill_id=="802").unwrap();
 assert_eq!(sell.original_trade_id,"902");
 assert_eq!(sell.original_bill_ccy,"USDT");
 assert_eq!(sell.original_bill_balance_change_e12,s("20.1698").unwrap());
 assert_eq!(sell.derived_base_delta_e12,s("-0.999").unwrap());
 assert_eq!(sell.derived_quote_delta_e12,s("20.1698").unwrap());
 assert_eq!(p.per_fill_derived_cash_not_source_bills.iter()
   .map(|r|r.derived_base_delta_e12).sum::<i128>(),0);
 assert_eq!(p.per_fill_derived_cash_not_source_bills.iter()
   .map(|r|r.derived_quote_delta_e12).sum::<i128>(),s("0.1698").unwrap());
 assert_eq!(p.exact_detail_bill_witnesses.len(),2);
 assert!(!p.all_base_quote_account_movements_proven_complete);
}

#[test]
fn trade2f2f_official_bill_usdt_balance_can_have_bnt_fee_currency_from_fills(){
 let p=Fixture::new().check().unwrap();
 assert_eq!(p.matched_bill_ids,vec!["801".to_string(),"802".to_string()]);
 assert_eq!(p.exact_detail_bill_witnesses.len(),2);
 let buy=p.exact_detail_bill_witnesses.iter()
      .find(|v|v.trade_id=="901").unwrap();
 assert_eq!(buy.bill_balance_currency,"USDT");
 assert_eq!(buy.fee_currency_from_trade_detail,"BNT");
 assert_eq!(buy.signed_fee_e12,s("-0.001").unwrap());
 assert_eq!(p.observed_bill_delta_by_ccy_e12.get("USDT").copied(),
      Some(s("0.1698").unwrap()));
 assert_eq!(p.fee_adjusted_inventory.base_fee_debited_e12,a("0.001").unwrap());
 assert_eq!(p.fee_adjusted_inventory.buy_delivered_base_after_native_fee_e12,
      a("0.999").unwrap());
 assert_eq!(p.fee_adjusted_inventory.aggregate_base_wallet_delta_e12,0);
 assert!(!p.fee_adjusted_inventory.original_future_short_matches_delivered_spot);
 assert!(!p.fee_currency_inferred_from_bill_ccy
      &&!p.one_bill_per_trade_is_general_exchange_guarantee
      &&!p.all_base_quote_account_movements_proven_complete
      &&!p.independent_authenticated_signed_history
      &&!p.real_wallet_flatness_proven
      &&!p.quote_valuation_of_base_fee_proven
      &&!p.may_send_cancel_retry_or_mark_verified_closed);
 assert_eq!(p.full_lifetime_net_pnl_e12,None);
}
#[test]
fn trade2f2f_two_usdt_fee_details_still_no_lifetime_authority(){
 let mut f=Fixture::new();
 f.fills[0]["feeCcy"]=json!("USDT");
 f.fills[0]["fee"]=json!("-0.01");
 f.fills[1]["fillSz"]=json!("1");
 f.bills[1]["fee"]=json!("-0.01");
 f.bills[1]["balChg"]=json!("-20.01");
 f.bills[1]["bal"]=json!("79.99");
 f.bills[0]["balChg"]=json!("20.19");
 f.bills[0]["bal"]=json!("100.18");
 let p=f.check().unwrap();
 assert_eq!(p.fee_adjusted_inventory.base_fee_debited_e12,0);
 assert_eq!(p.fee_adjusted_inventory.original_future_short_matches_delivered_spot,true);
 assert_eq!(p.observed_bill_delta_by_ccy_e12.get("USDT").copied(),
    Some(s("0.18").unwrap()));
 assert_eq!(p.full_lifetime_net_pnl_e12,None);
}
#[test]
fn trade2f2f_invalid_trade_detail_identity_instrument_fee_asset_side_or_order_denied(){
 for mode in 0..23{
    let mut f=Fixture::new();
    match mode{
      0=>f.fills[0]["instType"]=json!("SWAP"),
      1=>f.fills[0]["instId"]=json!("ZEC-USDT"),
      2=>f.fills[0]["tradeQuoteCcy"]=json!("BNB"),
      3=>f.fills[0]["side"]=json!("sell"),
      4=>f.fills[0]["subType"]=json!("2"),
      5=>f.fills[0]["ordId"]=json!(""),
      6=>f.fills[0]["tradeId"]=json!("0"),
      7=>f.fills[0]["billId"]=json!("not numeric"),
      8=>f.fills[0]["billId"]=json!("802"),
      9=>f.fills[0]["tradeId"]=json!("902"),
      10=>f.fills[0]["fillPx"]=json!("0"),
      11=>f.fills[0]["fillSz"]=json!("0"),
      12=>f.fills[0]["fillSz"]=json!("0.0000000000001"),
      13=>f.fills[0]["fee"]=json!("0.001"),
      14=>f.fills[0]["feeCcy"]=json!("BNB"),
      15=>f.fills[0]["feeCcy"]=Value::Null,
      16=>f.fills[0]["fillTime"]=json!("5000"),
      17=>f.fills[0]["ts"]=json!("1999"),
      18=>f.fills[0]["fee"]=json!("invalid"),
      19=>f.fills[0]["fee"]=json!("-1.1"),
      20=>f.fills[1]["fillSz"]=json!("1"),
      21=>f.fills[0]["ordId"]=json!("702"),
      22=>f.fills[1]["fillTime"]=json!("999"),
      _=>unreachable!(),
    }
    assert!(f.check().is_err(),"bad original OKX fill {mode} accepted");
 }
}
#[test]
fn trade2f2f_cash_bill_type_identity_amount_time_fee_or_missing_page_denied(){
 for mode in 0..21{
    let mut f=Fixture::new();
    match mode{
      0=>f.bills[0]["billId"]=json!("801"),
      1=>f.bills[0]["tradeId"]=json!("901"),
      2=>f.bills[1]["ordId"]=json!("702"),
      3=>f.bills[0]["subType"]=json!("1"),
      4=>f.bills[0]["type"]=json!("1"),
      5=>f.bills[0]["instType"]=json!("FUTURES"),
      6=>f.bills[0]["instId"]=json!("ZEC-USDT"),
      7=>f.bills[0]["ccy"]=json!("BNB"),
      8=>f.bills[0]["fee"]=json!("0.01"),
      9=>f.bills[1]["fee"]=json!("-0.02"),
      10=>f.bills[1]["fee"]=Value::Null,
      11=>f.bills[1]["bal"]=json!("99"),
      12=>f.bills[1]["balChg"]=json!("not decimal"),
      13=>f.bills[1]["ts"]=json!("1999"),
      14=>f.bills[0]["ts"]=json!("6000"),
      15=>f.bills[0]["ts"]=json!("2999"),
      16=>f.bills[0]["billId"]=json!("999"),
      17=>{f.bills.pop();},
      18=>{f.bills.push(f.bills[1].clone());},
      19=>f.bills[0]["bal"]=json!("999999999999999999999999999999999"),
      20=>f.bills[1]["ccy"]=Value::Null,
      _=>unreachable!(),
    }
    assert!(f.check().is_err(),"bad OKX fill-linked cash bill {mode} accepted");
 }
}
#[test]
fn trade2f2f_missing_original_window_or_unlisted_fee_inventory_rejected(){
 let f=Fixture::new();
 assert!(inspect("BNT/USDT",0,1000,4000,&f.fills,&f.bills).is_err());
 assert!(inspect("ZEC/USDT",a("1").unwrap(),1000,4000,&f.fills,&f.bills).is_err());
 assert!(inspect("BNT/USDT",a("1").unwrap(),0,4000,&f.fills,&f.bills).is_err());
 assert!(inspect("BNT/USDT",a("1").unwrap(),5000,4000,&f.fills,&f.bills).is_err());
 assert!(inspect("BNT/USDT",a("1").unwrap(),1000,4000,&[],&f.bills).is_err());
 assert!(inspect("BNT/USDT",a("1").unwrap(),1000,4000,&f.fills,&[]).is_err());
}
