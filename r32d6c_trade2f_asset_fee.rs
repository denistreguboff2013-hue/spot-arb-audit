//! TRADE2F2E fixed-point signed native-asset fee vs inventory.
//! Only artificial source-shaped fills, no exchange credentials / HTTP.
use spotarb_native_market_core::{
 fill_economics::{LegFillLedger,FillEvent,decimal_to_atoms as a,signed_decimal_to_atoms as s},
 native_trade2f_asset_fee::{
   inspect_native_spot_asset_fee_inventory as assess,
   inspect_asset_fees_for_original_spot_ledgers as anchored,
   SpotFeeFill,AssetFeeError,
 },
 native_trade2f_receipt_binding::{
   inspect_original_okx_asset_fee_bill_linkage as link_bills,
   require_b8_original_bill_parity_without_network as compare_b8,
   LocalB8OriginalBillShape as B8Bill,LocalB8BillLane as B8Lane},
 native_trade2f_income_window::ObservedOkxBill,
 native_trade2f_okx_owner_mock::MockGetStep,
 native_trade2f3_source_provenance::require_original_okx_b1_b8_bill_parity_without_network,
};
fn f(id:&str,order:&str,side:&str,q:&str,px:&str,asset:&str,fee:&str,t:u64)->SpotFeeFill{
 SpotFeeFill{
   order_id:order.into(),trade_id:id.into(),at_ms:t,side:side.into(),
   inst_id:"BNT-USDT".into(),gross_base_qty_e12:a(q).unwrap(),
   price_quote_e12:a(px).unwrap(),fee_ccy:asset.into(),
   signed_fee_e12:s(fee).unwrap(),
 }
}
fn native_case()->Vec<SpotFeeFill>{
 vec![
  f("801","701","buy","1","20","BNT","-0.001",1000),
  f("802","702","sell","0.999","20.2","USDT","-0.01",2000),
 ]
}
fn usdt_case()->Vec<SpotFeeFill>{
 vec![
  f("801","701","buy","1","20","USDT","-0.01",1000),
  f("802","702","sell","1","20.2","USDT","-0.01",2000),
 ]
}
#[test]
fn trade2f2e_base_buy_fee_reduces_delivered_inventory_and_shows_hedge_gap(){
 let x=assess("BNT/USDT",a("1").unwrap(),&native_case()).unwrap();
 assert_eq!(x.buy_gross_base_e12,a("1").unwrap());
 assert_eq!(x.buy_delivered_base_after_native_fee_e12,a("0.999").unwrap());
 assert_eq!(x.sell_gross_base_e12,a("0.999").unwrap());
 assert_eq!(x.base_fee_debited_e12,a("0.001").unwrap());
 assert_eq!(x.quote_fee_debited_e12,a("0.01").unwrap());
 assert_eq!(x.aggregate_base_wallet_delta_e12,0);
 assert_eq!(x.aggregate_quote_wallet_delta_e12,s("0.1698").unwrap());
 assert!(!x.original_future_short_matches_delivered_spot);
 assert!(x.completely_self_funded_spot_unwind);
 assert!(!x.base_fee_converted_to_quote_by_unverified_mark_price
   &&!x.source_signed_exchange_attestation_proven
   &&!x.complete_hedged_exit_proven
   &&!x.full_income_or_wallet_bill_history_proven
   &&!x.may_send_cancel_retry_or_verify_closed);
 assert_eq!(x.definitive_lifetime_net_pnl_quote_e12,None);
}
#[test]
fn trade2f2e_attempt_sell_full_one_bnt_after_buy_fee_denied_without_external_topup(){
 let mut x=native_case();x[1].gross_base_qty_e12=a("1").unwrap();
 assert!(matches!(assess("BNT-USDT",a("1").unwrap(),&x),
   Err(AssetFeeError::Invalid(_))));
}
#[test]
fn trade2f2e_all_usdt_commissions_do_not_reduce_base_qty_or_add_fake_pnl(){
 let x=assess("BNT-USDT",a("1").unwrap(),&usdt_case()).unwrap();
 assert!(x.completely_self_funded_spot_unwind);
 assert!(x.original_future_short_matches_delivered_spot);
 assert_eq!(x.base_fee_debited_e12,0);
 assert_eq!(x.quote_fee_debited_e12,a("0.02").unwrap());
 assert_eq!(x.aggregate_base_wallet_delta_e12,0);
 assert_eq!(x.aggregate_quote_wallet_delta_e12,s("0.18").unwrap());
 assert_eq!(x.definitive_lifetime_net_pnl_quote_e12,None);
}
#[test]
fn trade2f2e_sell_fee_in_base_requires_extra_inventory_and_quote_unconverted(){
 let rows=vec![
   f("801","701","buy","1","20","USDT","0",1000),
   f("802","702","sell","0.999","20.2","BNT","-0.001",2000),
 ];
 let x=assess("BNT-USDT",a("1").unwrap(),&rows).unwrap();
 assert_eq!(x.base_fee_debited_e12,a("0.001").unwrap());
 assert_eq!(x.aggregate_base_wallet_delta_e12,0);
 assert_eq!(x.aggregate_quote_wallet_delta_e12,s("0.1798").unwrap());
 assert!(!x.complete_hedged_exit_proven);
}
#[test]
fn trade2f2e_partial_sell_leaves_base_residual_not_verified_flat(){
 let mut rows=native_case();rows[1].gross_base_qty_e12=a("0.8").unwrap();
 let x=assess("BNT-USDT",a("1").unwrap(),&rows).unwrap();
 assert_eq!(x.aggregate_base_wallet_delta_e12,s("0.199").unwrap());
 assert!(!x.completely_self_funded_spot_unwind);
 assert_eq!(x.definitive_lifetime_net_pnl_quote_e12,None);
}

fn ledger(side:&str,order:&str,q:&str,px:&str,source:&SpotFeeFill)->LegFillLedger{
 let (v,symbol)=("okx","BNT/USDT");
 let mut l=LegFillLedger::new(v,symbol,side,order,order,a(q).unwrap()).unwrap();
 l.apply_cumulative_truth(a(q).unwrap(),Some(a(px).unwrap()),true,
   Some("FILLED"),Some(order),Some(source.at_ms),1).unwrap();
 let fee=if source.fee_ccy=="USDT"{
   Some(source.signed_fee_e12.checked_neg().unwrap())
 }else{None};
 l.apply_fill(FillEvent{
   fill_id:source.trade_id.clone(),price_atoms:a(px).unwrap(),
   qty_atoms:a(q).unwrap(),fee_cost_quote_atoms:fee,
   fee_currency:Some(source.fee_ccy.clone()),
   trade_time_ms:Some(source.at_ms),
 },1).unwrap();
 l
}
fn hedged_open()->LegFillLedger{
 let mut l=LegFillLedger::new("binance","BNTUSDT","sell","fut-open","fut-open",
    a("1").unwrap()).unwrap();
 l.apply_cumulative_truth(a("1").unwrap(),Some(a("20.1").unwrap()),true,
     Some("FILLED"),Some("910"),Some(1100),1).unwrap();
 l.apply_fill(FillEvent{fill_id:"810".into(),price_atoms:a("20.1").unwrap(),
    qty_atoms:a("1").unwrap(),fee_currency:Some("USDT".into()),
    fee_cost_quote_atoms:Some(s("0.01").unwrap()),trade_time_ms:Some(1100)
 },1).unwrap();
 l
}
fn anchored_case()->(LegFillLedger,LegFillLedger,LegFillLedger,Vec<SpotFeeFill>){
 let rows=native_case();
 let buy=ledger("buy","701","1","20",&rows[0]);
 let sell=ledger("sell","702","0.999","20.2",&rows[1]);
 (buy,sell,hedged_open(),rows)
}
#[test]
fn trade2f2e_original_venue_ids_and_fills_bound_to_fee_currency_but_not_fake_pnl(){
 let (buy,sell,short,rows)=anchored_case();
 let r=anchored(&buy,&sell,&short,&rows).unwrap();
 assert_eq!(r.base_fee_debited_e12,a("0.001").unwrap());
 assert_eq!(r.aggregate_base_wallet_delta_e12,0);
 assert!(!r.original_future_short_matches_delivered_spot);
 assert!(!r.source_signed_exchange_attestation_proven
    &&!r.full_income_or_wallet_bill_history_proven
    &&!r.complete_hedged_exit_proven
    &&!r.may_send_cancel_retry_or_verify_closed);
 assert_eq!(r.definitive_lifetime_net_pnl_quote_e12,None);
}
#[test]
fn trade2f2e_original_order_fill_fee_size_time_and_currency_tampering_denied(){
 for fault in 0..14{
  let (mut buy,mut sell,mut short,mut rows)=anchored_case();
  match fault{
   0=>rows[0].trade_id="foreign".into(),
   1=>rows[0].order_id="foreign".into(),
   2=>rows[0].price_quote_e12=a("21").unwrap(),
   3=>rows[0].gross_base_qty_e12=a("0.99").unwrap(),
   4=>rows[0].at_ms=1001,
   5=>rows[0].fee_ccy="USDT".into(),
   6=>buy.fills.get_mut("801").unwrap().fee_cost_quote_atoms=Some(s("0.02").unwrap()),
   7=>rows[1].signed_fee_e12=s("-0.02").unwrap(),
   8=>sell.exchange_order_id=None,
   9=>buy.venue="binance".into(),
   10=>short.symbol="ZECUSDT".into(),
   11=>buy.terminal=false,
   12=>short.cumulative_truth_present=false,
   13=>{rows.pop();},
   _=>unreachable!(),
  }
  assert!(anchored(&buy,&sell,&short,&rows).is_err(),
    "forged source original fill {fault} accepted");
 }
}
#[test]
fn trade2f2e_fee_fx_asset_rebate_invalid_identity_and_underflow_fail_closed(){
 for fault in 0..16{
   let mut rows=native_case();
   match fault{
    0=>rows[0].fee_ccy="BNB".into(),
    1=>rows[0].signed_fee_e12=s("0.001").unwrap(),
    2=>rows[0].trade_id=rows[1].trade_id.clone(),
    3=>rows[0].order_id.clear(),
    4=>rows[1].inst_id="ZEC-USDT".into(),
    5=>rows[0].gross_base_qty_e12=0,
    6=>rows[0].price_quote_e12=0,
    7=>rows[0].at_ms=0,
    8=>rows[1].at_ms=999,
    9=>rows[1].side="buy".into(),
    10=>rows[0].side="hold".into(),
    11=>rows[0].signed_fee_e12=i128::MIN,
    12=>rows[0].signed_fee_e12=s("-1.01").unwrap(),
    13=>rows[0].price_quote_e12=u128::MAX,
    14=>rows[0].gross_base_qty_e12=u128::MAX,
    15=>rows[0].signed_fee_e12=s("-0.999999999999").unwrap(),
    _=>unreachable!(),
   }
   let r=assess("BNT-USDT",a("1").unwrap(),&rows);
   if fault==15{
     // 1 - fee still leaves a tiny quantity but SELL .999 exceeds it.
     assert!(r.is_err(),"sell cannot consume sourced base BNT dust");
   }else{
     assert!(r.is_err(),"unsafe fee scenario {fault} accepted");
   }
 }
}
#[test]
fn trade2f2e_binance_non_usdt_commission_or_missing_fee_refuses_apparent_hedge_proof(){
 for mode in 0..3{
    let (buy,sell,mut short,rows)=anchored_case();
    let f=short.fills.get_mut("810").unwrap();
    match mode{
      0=>f.fee_currency=Some("BNB".into()),
      1=>f.fee_cost_quote_atoms=None,
      2=>f.fee_cost_quote_atoms=Some(-1),
      _=>unreachable!()
    }
    assert!(anchored(&buy,&sell,&short,&rows).is_err(),
       "Binance third-currency/missing fee mode {mode} was accepted");
 }
}
fn bill(source:&SpotFeeFill,id:&str,ccy:&str,delta:&str)->ObservedOkxBill{
 ObservedOkxBill{
  bill_id:id.into(),currency:ccy.into(),signed_change_e12:s(delta).unwrap(),
  cash_after_e12:0,at_ms:source.at_ms+100,bill_type:"2".into(),
  bill_sub_type:if source.side=="buy"{"1"}else{"2"}.into(),
  order_id:source.order_id.clone(),trade_id:source.trade_id.clone(),
  observed_signed_fee_without_sourced_ccy_e12:Some(source.signed_fee_e12),
 }
}
#[test]
fn trade2f2b_native_base_fee_one_original_bill_per_fill_no_fake_usdt_pnl(){
 let (buy,sell,short,rows)=anchored_case();
 let bills=vec![bill(&rows[0],"1001","BNT","0.999"),
     bill(&rows[1],"1002","USDT","20.1698")];
 let r=link_bills(&buy,&sell,&short,&rows,&bills,(900,2300)).unwrap();
 assert_eq!(r.base_fee_debited_e12,a("0.001").unwrap());
 assert_eq!(r.quote_fee_debited_e12,a("0.01").unwrap());
 assert_eq!(r.aggregate_base_wallet_delta_e12,0);
 // The observed ONE original bill may be QUOTE even if feeCcy is BASE.
 let reversed=vec![bill(&rows[0],"1001","USDT","-20"),
     bill(&rows[1],"1002","BNT","-0.999")];
 assert!(link_bills(&buy,&sell,&short,&rows,&reversed,(900,2300)).is_ok());
 assert_eq!(r.definitive_lifetime_net_pnl_quote_e12,None);
 assert!(!r.source_signed_exchange_attestation_proven);
 assert!(!r.may_send_cancel_retry_or_verify_closed);
}
#[test]
fn trade2f2b_original_okx_fee_bill_mismatch_and_fabricated_second_bill_denied(){
 for fault in 0..10{
   let (buy,sell,short,rows)=anchored_case();
   let mut bills=vec![bill(&rows[0],"1001","BNT","0.999"),
       bill(&rows[1],"1002","USDT","20.1698")];
   match fault{
      0=>bills[0].currency="USDT".into(),
      1=>bills[0].signed_change_e12=s("1").unwrap(),
      2=>bills[1].signed_change_e12=s("20.1798").unwrap(),
      3=>bills[1].trade_id="foreign".into(),
      4=>bills[0].order_id="wrong".into(),
      5=>bills[0].bill_id="01001".into(),
      6=>bills[1].bill_id="1001".into(),
      7=>bills[0].at_ms=13000,
      8=>bills[0].observed_signed_fee_without_sourced_ccy_e12=Some(0),
      9=>bills.push(bill(&rows[0],"1003","USDT","-20")),
      _=>unreachable!(),
   }
   assert!(link_bills(&buy,&sell,&short,&rows,&bills,(900,3300)).is_err(),
      "invalid original OKX bill scenario {fault} accepted");
 }
}
#[test]
fn p104_local_native_to_b8_original_bill_shape_keeps_one_source_per_fill(){
 let (buy,sell,short,originals)=anchored_case();
 let receipts=vec![bill(&originals[0],"1001","BNT","0.999"),
   bill(&originals[1],"1002","USDT","20.1698")];
 assert!(link_bills(&buy,&sell,&short,&originals,&receipts,(900,2300)).is_ok());
 let rows=vec![
  B8Bill{bill_id:"1001".into(),trade_id:"801".into(),
    asset:"BNT".into(),signed_delta_e12:s("0.999").unwrap(),
    lane:B8Lane::BaseOriginal},
  B8Bill{bill_id:"1002".into(),trade_id:"802".into(),
    asset:"USDT".into(),signed_delta_e12:s("20.1698").unwrap(),
    lane:B8Lane::QuoteOriginal},
 ];
 assert!(compare_b8("BNT",&receipts,&rows).is_ok());
 for variant in 0..7{
    let mut tampered=rows.clone();
    match variant{
       0=>tampered[0].bill_id="1002".into(),
       1=>tampered[0].trade_id="802".into(),
       2=>tampered[1].asset="BNT".into(),
       3=>tampered[1].signed_delta_e12+=1,
       4=>tampered[0].lane=B8Lane::BaseDelayed,
       5=>tampered[0].lane=B8Lane::QuoteOriginal,
       6=>tampered.push(tampered[0].clone()),
       _=>unreachable!(),
    }
    assert!(compare_b8("BNT",&receipts,&tampered).is_err(),
       "B8 duplicate/tampered original source accepted {variant}");
 }
 let mut duplicate_trade=receipts.clone();
 duplicate_trade[1].trade_id=duplicate_trade[0].trade_id.clone();
 assert!(compare_b8("BNT",&duplicate_trade,&rows).is_err(),
    "Native original trade identity duplicated");
 let mut b8_double_trade=rows.clone();
 b8_double_trade[1].trade_id=b8_double_trade[0].trade_id.clone();
 assert!(compare_b8("BNT",&receipts,&b8_double_trade).is_err(),
    "B8 repeated original trade must be refused");
 assert!(compare_b8("BNB",&receipts,&rows).is_err(),
    "foreign BASE denomination wrongly allowed");
 assert!(compare_b8("BNT",&receipts,&[]).is_err());
}
#[test]
fn p101_raw_b1_original_okx_pages_must_equal_native_and_b8_source_bills(){
 let (buy,sell,short,fills)=anchored_case();
 let receipts=vec![bill(&fills[0],"1001","BNT","0.999"),
   bill(&fills[1],"1002","USDT","20.1698")];
 assert!(link_bills(&buy,&sell,&short,&fills,&receipts,(900,2300)).is_ok());
 let rows=vec![
  B8Bill{bill_id:"1001".into(),trade_id:"801".into(),asset:"BNT".into(),
   signed_delta_e12:s("0.999").unwrap(),lane:B8Lane::BaseOriginal},
  B8Bill{bill_id:"1002".into(),trade_id:"802".into(),asset:"USDT".into(),
   signed_delta_e12:s("20.1698").unwrap(),lane:B8Lane::QuoteOriginal},
 ];
 let mk=|ccy:&str,id:&str,trade:&str,order:&str,delta:&str|MockGetStep{
   path_and_query:format!("/api/v5/account/bills-archive?ccy={ccy}&begin=900&end=2300&limit=100"),
   http_status:200,
   body:serde_json::json!({"code":"0","data":[{"ccy":ccy,"billId":id,
     "tradeId":trade,"ordId":order,"balChg":delta}]}).to_string(),
 };
 let raw=vec![mk("BNT","1001","801","701","0.999"),
    mk("USDT","1002","802","702","20.1698")];
 assert!(require_original_okx_b1_b8_bill_parity_without_network(
   "BNT",&raw,&receipts,&rows).is_ok());
 for fault in 0..13{
   let mut wrong=raw.clone();
   match fault{
    0=>wrong[0].body=wrong[0].body.replace("0.999","0.998"),
    1=>wrong[0].body=wrong[0].body.replace("\"701\"","\"999\""),
    2=>wrong[0].body=wrong[0].body.replace("\"1001\"","\"1002\""),
    3=>{wrong.pop();},
    4=>wrong.push(wrong[0].clone()),
    5=>wrong[0].body=wrong[0].body.replace("\"801\"","\"999\""),
    6=>wrong[0].body=wrong[0].body.replace("\"BNT\"","\"USDT\""),
    7=>wrong[0].path_and_query=wrong[0].path_and_query.replace("ccy=BNT","ccy=BNB"),
    8=>wrong[0].body=wrong[0].body.replace("\"code\":\"0\"","\"code\":\"1\""),
    9=>wrong[0].path_and_query=wrong[0].path_and_query.replace("begin=900","begin=950"),
    10=>wrong[1].path_and_query=wrong[1].path_and_query.replace("end=2300","end=2200"),
    11=>wrong[0].path_and_query.push_str("&after=999"),
    12=>wrong[1].path_and_query=wrong[1].path_and_query.replace("limit=100","limit=1"),
    _=>unreachable!(),
   }
   assert!(require_original_okx_b1_b8_bill_parity_without_network(
      "BNT",&wrong,&receipts,&rows).is_err(),"tampered raw case {fault}");
 }
 // Fully occupied first page needs a cursor-linked, short terminal page
 // for EACH original currency slot, rather than pretending one page is complete.
 let mut tiny=raw.clone();
 for page in &mut tiny{page.path_and_query=page.path_and_query.replace("limit=100","limit=1");}
 for (page,id) in [(&raw[0],"1001"),(&raw[1],"1002")]{
    let mut terminal=page.clone();
    terminal.path_and_query=terminal.path_and_query.replace("limit=100","limit=1");
    terminal.path_and_query.push_str(&format!("&after={id}"));
    terminal.body=serde_json::json!({"code":"0","data":[]}).to_string();
    tiny.push(terminal);
 }
 assert!(require_original_okx_b1_b8_bill_parity_without_network(
     "BNT",&tiny,&receipts,&rows).is_ok());
 let mut forged=tiny.clone();
 forged[2].path_and_query=forged[2].path_and_query.replace("after=1001","after=999");
 assert!(require_original_okx_b1_b8_bill_parity_without_network(
     "BNT",&forged,&receipts,&rows).is_err());
 assert!(require_original_okx_b1_b8_bill_parity_without_network(
     "BNT",&tiny[..3],&receipts,&rows).is_err());
}
#[test]
fn trade2f2e_empty_missing_symbol_and_large_graph_refused(){
 let mut rows=native_case();
 assert!(assess("BNT-USDT",a("1").unwrap(),&[]).is_err());
 assert!(assess("BNT-USDT",0,&rows).is_err());
 assert!(assess("ZEC-USDT",a("1").unwrap(),&rows).is_err());
 assert!(assess("BNT-BTC",a("1").unwrap(),&rows).is_err());
 assert!(assess("",a("1").unwrap(),&rows).is_err());
 rows[0].trade_id.clear();
 assert!(assess("BNT-USDT",a("1").unwrap(),&rows).is_err());
 let mut rows=native_case();
 for i in 0..64{
   let mut f=rows[0].clone();f.trade_id=format!("extra{i}");
   rows.push(f);
 }
 assert!(assess("BNT-USDT",a("1").unwrap(),&rows).is_err());
}
