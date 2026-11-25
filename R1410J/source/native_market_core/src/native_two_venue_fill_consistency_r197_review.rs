// R197: offline two-venue fill consistency check, NOT exchange attestation.
// Real OKX feeCcy may be BASE or QUOTE; never silently treat a BASE fee as USDT.
#[derive(Debug,Clone,PartialEq)]
pub struct HedgeFillShapeR197{
 pub spot_base_qty:f64,pub spot_gross_base_qty:f64,
 pub spot_fee_paid_base:f64,pub short_base_qty:f64,
 pub unmatched_base_qty:f64,pub fee_conversion_required:bool,
 pub shape_matched:bool,pub exchange_attested:bool,pub may_trade:bool,
}
pub fn preflight_two_venue_fill_consistency_r197(
 rows:&[FillCandidateR189],now:u64,base:&str,quote:&str,
 max_unmatched_base:f64)->Result<HedgeFillShapeR197,PaperAdapterErrorR156>{
 let shape=preflight_fill_shape_r189(rows,now)?;
 if !shape.has_both_venues||base.is_empty()||quote.is_empty()
  ||base==quote||!max_unmatched_base.is_finite()||max_unmatched_base<0.0{
  return Err(PaperAdapterErrorR156::Source)
 }
 let ticker=format!("{base}{quote}");
 let mut spot=0.0f64;let mut short=0.0f64;
 let mut gross_spot=0.0f64;let mut fee_base_spot=0.0f64;
 let mut conversion=false;
 for row in rows{
  if row.symbol!=ticker||row.fee_currency!=base&&row.fee_currency!=quote{
   return Err(PaperAdapterErrorR156::Source)
  }
  // USDT-M futures BASE-fee rows cannot be valued in quote or adjusted
  // by this quantity-only diagnostic. The historical R197 loop used to
  // flag conversion but still count the gross futures short as net.
  // Refuse unsupported futures feeCcy rather than silently mark MATCH.
  if row.venue==EvidenceVenueR189::BinanceUsdtMFutures
     &&row.fee_currency!=quote{
    return Err(PaperAdapterErrorR156::Source)
  }
  // A quote fee cannot exceed this fill's notional in a finite,
  // nonnegative source-shaped row. Unknown fee valuation DENIES.
  let notional=row.executed_qty*row.executed_price;
  if !notional.is_finite()||notional<=0.0 ||
    (row.fee_currency==quote&&row.fee_amount>notional){
    return Err(PaperAdapterErrorR156::Source)
  }
  match row.venue{
   EvidenceVenueR189::OkxSpot=>{
    gross_spot+=row.executed_qty;
    if row.fee_currency==base{
     if row.fee_amount>=row.executed_qty{return Err(PaperAdapterErrorR156::Source)}
     if row.fee_amount>0.0{conversion=true}
     fee_base_spot+=row.fee_amount;
     spot+=row.executed_qty-row.fee_amount;
    }else{spot+=row.executed_qty}
   },
   EvidenceVenueR189::BinanceUsdtMFutures=>short+=row.executed_qty,
  }
 }
 if !spot.is_finite()||!short.is_finite(){
  return Err(PaperAdapterErrorR156::Source)
 }
 let delta=(spot-short).abs();
 if delta>max_unmatched_base{
  return Err(PaperAdapterErrorR156::Source)
 }
 Ok(HedgeFillShapeR197{
  spot_base_qty:spot,spot_gross_base_qty:gross_spot,
  spot_fee_paid_base:fee_base_spot,short_base_qty:short,unmatched_base_qty:delta,
  fee_conversion_required:conversion,shape_matched:true,
  exchange_attested:false,may_trade:false
 })
}
#[cfg(test)]
mod r197_tests{
 use super::*;
 const NOW:u64=1_792_280_000_000;
 fn pair()->Vec<FillCandidateR189>{
  [EvidenceVenueR189::OkxSpot,EvidenceVenueR189::BinanceUsdtMFutures]
    .iter().enumerate().map(|(i,v)|FillCandidateR189{
      venue:*v,account_owner:"research-owner".into(),symbol:"BTCUSDT".into(),
      order_id:format!("mock-order-{i}"),fill_id:format!("mock-fill-{i}"),
      fee_currency:if i==0{"BTC"}else{"USDT"}.into(),
      executed_qty:0.25,executed_price:100_000.0,
      fee_amount:0.0001,exchange_timestamp_ms:NOW-100,modeled_paper:false,
    }).collect()
 }
 #[test]fn r197_okx_base_fee_currency_flag_and_hedge_delta(){
  let mut rows=pair();
  rows[1].executed_qty=0.2499;
  let shape=preflight_two_venue_fill_consistency_r197(&rows,NOW,
    "BTC","USDT",0.000001).unwrap();
  assert!(shape.unmatched_base_qty<0.000001);
  assert!((shape.spot_gross_base_qty-0.25).abs()<0.000001);
  assert!((shape.spot_fee_paid_base-0.0001).abs()<0.000001);
  assert!(shape.fee_conversion_required);
  assert!(shape.shape_matched);
  assert!(!shape.exchange_attested&&!shape.may_trade);
 }
 #[test]fn r198_futures_base_fee_cannot_be_silently_ignored_in_net_hedge(){
  let mut p=pair();p[1].executed_qty=0.2499;
  p[1].fee_currency="BTC".into();
  assert!(preflight_two_venue_fill_consistency_r197(
    &p,NOW,"BTC","USDT",0.000001).is_err(),
    "Binance USDT-M nonquote fee must not silently count gross short");
  let mut p=pair();p[0].fee_currency="USDT".into();
  p[0].fee_amount=10.0;p[1].executed_qty=0.25;
  let a=preflight_two_venue_fill_consistency_r197(
    &p,NOW,"BTC","USDT",0.0).unwrap();
  assert_eq!(a.spot_fee_paid_base,0.0);
  assert!(!a.fee_conversion_required);
  assert!(!a.exchange_attested&&!a.may_trade);
  let mut p=pair();p[0].fee_amount=0.0;p[1].executed_qty=0.25;
  let a=preflight_two_venue_fill_consistency_r197(
    &p,NOW,"BTC","USDT",0.0).unwrap();
  assert!(!a.fee_conversion_required,"zero BASE fee costs no conversion");
 }
 #[test]fn r198_impossible_quote_fee_valuation_denied(){
   let mut p=pair();p[1].executed_qty=0.2499;
   p[1].fee_amount=100_000.0;
   assert!(preflight_two_venue_fill_consistency_r197(
      &p,NOW,"BTC","USDT",0.000001).is_err());
   let mut p=pair();p[0].fee_currency="USDT".into();
   p[0].fee_amount=1_000_000.0;p[1].executed_qty=0.25;
   assert!(preflight_two_venue_fill_consistency_r197(
      &p,NOW,"BTC","USDT",0.0).is_err());
   let mut p=pair();p[1].executed_qty=0.2499;
   p[0].executed_price=f64::MAX;p[0].executed_qty=f64::MAX;
   assert!(preflight_two_venue_fill_consistency_r197(
      &p,NOW,"BTC","USDT",f64::MAX).is_err(),
      "finite inputs cannot overflow source quote valuation");
 }
 #[test]fn r198_multiple_okx_base_fee_fills_sum_net_inventory_before_hedge(){
  let mut p=pair();
  p[0].executed_qty=0.15;p[0].fee_amount=0.00005;
  let mut extra=p[0].clone();extra.fill_id="mock-fill-spot-extra".into();
  extra.executed_qty=0.10;extra.fee_amount=0.00005;
  p[1].executed_qty=0.2499;
  p.push(extra.clone());
  let matched=preflight_two_venue_fill_consistency_r197(
    &p,NOW,"BTC","USDT",0.000001).unwrap();
  assert!((matched.spot_gross_base_qty-0.25).abs()<1e-10);
  assert!((matched.spot_fee_paid_base-0.0001).abs()<1e-10);
  assert!((matched.spot_base_qty-0.2499).abs()<1e-10);
  assert!(matched.fee_conversion_required);
  assert!(!matched.exchange_attested&&!matched.may_trade);
  p[1].executed_qty=0.25;
  assert!(preflight_two_venue_fill_consistency_r197(
    &p,NOW,"BTC","USDT",0.000001).is_err(),
    "original gross spot qty must not replace BASE-fee net qty");
  p[1].executed_qty=0.2499;
  p.push(extra);
  assert!(preflight_two_venue_fill_consistency_r197(
    &p,NOW,"BTC","USDT",0.000001).is_err(),"duplicated mock spot fill");
 }
 #[test]fn r197_mismatched_venue_size_fee_ccy_and_duplicates_deny(){
  let mut p=pair();
  assert!(preflight_two_venue_fill_consistency_r197(&p,NOW,
    "BTC","USDT",0.000001).is_err(),"base-denominated OKX fee changes net hedge");
  p[1].executed_qty=0.2;
  assert!(preflight_two_venue_fill_consistency_r197(&p,NOW,
    "BTC","USDT",0.000001).is_err());
  p=pair();p[0].fee_currency="DOGE".into();
  assert!(preflight_two_venue_fill_consistency_r197(&p,NOW,
    "BTC","USDT",0.000001).is_err());
  p=pair();p[1].venue=EvidenceVenueR189::OkxSpot;
  assert!(preflight_two_venue_fill_consistency_r197(&p,NOW,
    "BTC","USDT",0.000001).is_err());
  p=pair();p[0].modeled_paper=true;
  assert!(preflight_two_venue_fill_consistency_r197(&p,NOW,
    "BTC","USDT",0.000001).is_err());
 }
}
