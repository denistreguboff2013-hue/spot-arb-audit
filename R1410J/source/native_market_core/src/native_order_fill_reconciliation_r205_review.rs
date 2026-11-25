// R205: validate that OFFLINE candidate fills match two declared orders.
// Orders and fills are supplied by test caller; no provider authenticity inferred.
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum OrderSideR205{Buy,Sell}
#[derive(Clone,Debug)]
pub struct OrderClaimR205{
 pub venue:EvidenceVenueR189,pub side:OrderSideR205,
 pub owner:String,pub symbol:String,pub order_id:String,
 pub reported_executed_base:f64,pub allow_partial:bool,
 pub reported_at_ms:u64,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct OrderReconciliationR205{
 pub order_count:usize,pub fill_count:usize,
 pub actual_provider_attested:bool,pub may_trade:bool,
}
pub fn check_order_fill_claims_r205(
 orders:&[OrderClaimR205],fills:&[FillCandidateR189],
 now:u64,qty_tolerance:f64
)->Result<OrderReconciliationR205,PaperAdapterErrorR156>{
 if orders.len()!=2||!qty_tolerance.is_finite()||qty_tolerance<0.0{
  return Err(PaperAdapterErrorR156::Source)
 }
 preflight_fill_shape_r189(fills,now)?;
 for (idx,o) in orders.iter().enumerate(){
  let expected=if idx==0{EvidenceVenueR189::OkxSpot}
    else{EvidenceVenueR189::BinanceUsdtMFutures};
  let side=if idx==0{OrderSideR205::Buy}else{OrderSideR205::Sell};
  // R207: source-shaped order report cannot absorb arbitrary quantity
  // drift. A caller-chosen huge tolerance was previously accepted and
  // could conceal mismatched execution totals in a research report.
  let max_relative_tolerance=o.reported_executed_base*0.000001;
  if o.venue!=expected||o.side!=side||o.owner!=orders[0].owner
   ||o.symbol!=orders[0].symbol||o.order_id.is_empty()
   ||!o.reported_executed_base.is_finite()||o.reported_executed_base<=0.0
   ||!max_relative_tolerance.is_finite()
   ||qty_tolerance>max_relative_tolerance
   ||o.reported_at_ms==0
   ||o.reported_at_ms>now.saturating_add(1000)
   ||now.saturating_sub(o.reported_at_ms)>86_400_000{
   return Err(PaperAdapterErrorR156::Source)
  }
  let owned:Vec<_>=fills.iter().filter(|f|f.venue==o.venue).collect();
  if owned.is_empty()||owned.iter().any(|f|
    f.order_id!=o.order_id||f.symbol!=o.symbol||f.account_owner!=o.owner
    ||f.exchange_timestamp_ms>o.reported_at_ms){
   return Err(PaperAdapterErrorR156::Source)
  }
  let sum: f64=owned.iter().map(|f|f.executed_qty).sum();
  if !sum.is_finite()||(sum-o.reported_executed_base).abs()>qty_tolerance{
   return Err(PaperAdapterErrorR156::Source)
  }
  if o.allow_partial{return Err(PaperAdapterErrorR156::Source)}
 }
 Ok(OrderReconciliationR205{order_count:2,fill_count:fills.len(),
  actual_provider_attested:false,may_trade:false})
}
/// R207: joint OFFLINE review of the two existing source-shaped guards.
/// Local agreement of order claims and BASE-fee-adjusted hedge quantity
/// does NOT certify the original orders, API account or any financial PnL.
#[derive(Clone,Debug,PartialEq)]
pub struct JoinedOrderFeeHedgeReviewR207{
 pub orders:OrderReconciliationR205,
 pub hedge:HedgeFillShapeR197,
 pub original_order_truth_authenticated:bool,
 pub same_day_fee_history_complete:bool,
 pub unknown_acks_reconciled:bool,
 pub may_send_order_or_release_recovery:bool,
}
pub fn inspect_joined_order_fee_hedge_r207(
 orders:&[OrderClaimR205],fills:&[FillCandidateR189],
 now:u64,base:&str,quote:&str,
 order_qty_tolerance:f64,max_unmatched_base:f64
)->Result<JoinedOrderFeeHedgeReviewR207,PaperAdapterErrorR156>{
 // Both caller-chosen tolerances must be small relative to the
 // claimed contract size, independent of asset scale (BTC vs DOGE).
 let min_qty=orders.iter().map(|o|o.reported_executed_base)
   .fold(f64::INFINITY,f64::min);
 let upper=min_qty*0.000001;
 if orders.len()!=2||!upper.is_finite()||upper<=0.0
   ||!order_qty_tolerance.is_finite()||order_qty_tolerance<0.0
   ||!max_unmatched_base.is_finite()||max_unmatched_base<0.0
   ||order_qty_tolerance>upper||max_unmatched_base>upper{
  return Err(PaperAdapterErrorR156::Source)
 }
 let order_review=check_order_fill_claims_r205(
   orders,fills,now,order_qty_tolerance)?;
 let hedge_review=preflight_two_venue_fill_consistency_r197(
   fills,now,base,quote,max_unmatched_base)?;
 if !hedge_review.shape_matched||order_review.may_trade
    ||order_review.actual_provider_attested||hedge_review.may_trade
    ||hedge_review.exchange_attested{
   return Err(PaperAdapterErrorR156::Source)
 }
 Ok(JoinedOrderFeeHedgeReviewR207{
   orders:order_review,hedge:hedge_review,
   original_order_truth_authenticated:false,
   same_day_fee_history_complete:false,
   unknown_acks_reconciled:false,
   may_send_order_or_release_recovery:false,
 })
}
#[cfg(test)]
mod r205_tests{
 use super::*;
 const NOW:u64=1_792_280_000_000;
 fn claims()->(Vec<OrderClaimR205>,Vec<FillCandidateR189>){
  let mut orders=Vec::new();let mut fills=Vec::new();
  for (i,venue) in [EvidenceVenueR189::OkxSpot,
    EvidenceVenueR189::BinanceUsdtMFutures].iter().enumerate(){
   let oid=format!("mock-order-{i}");
   orders.push(OrderClaimR205{venue:*venue,
    side:if i==0{OrderSideR205::Buy}else{OrderSideR205::Sell},
    owner:"test-account-owner".into(),symbol:"BTCUSDT".into(),
    order_id:oid.clone(),reported_executed_base:0.25,
    allow_partial:false,reported_at_ms:NOW-1000});
   for j in 0..2{
    fills.push(FillCandidateR189{venue:*venue,
     account_owner:"test-account-owner".into(),symbol:"BTCUSDT".into(),
     order_id:oid.clone(),fill_id:format!("{i}-{j}"),
     fee_currency:"USDT".into(),executed_qty:0.125,
     executed_price:100_000.0,fee_amount:0.01,
     exchange_timestamp_ms:NOW-1000,modeled_paper:false});
   }
  }
  (orders,fills)
 }
 #[test]fn r207_order_and_net_fee_hedge_agree(){
  let (mut orders,mut fills)=claims();
  fills[0].fee_currency="BTC".into();
  fills[1].fee_currency="BTC".into();
  fills[0].fee_amount=0.000025;
  fills[1].fee_amount=0.000025;
  fills[2].executed_qty=0.124975;
  fills[3].executed_qty=0.124975;
  orders[1].reported_executed_base=0.24995;
  let joined=inspect_joined_order_fee_hedge_r207(
    &orders,&fills,NOW,"BTC","USDT",0.0,0.0).unwrap();
  assert_eq!((joined.orders.order_count,joined.orders.fill_count),(2,4));
  assert!((joined.hedge.spot_base_qty-0.24995).abs()<1e-10);
  assert!(joined.hedge.fee_conversion_required);
  assert!(!joined.original_order_truth_authenticated);
  assert!(!joined.same_day_fee_history_complete);
  assert!(!joined.unknown_acks_reconciled);
  assert!(!joined.may_send_order_or_release_recovery);
  fills[3].executed_qty=0.125025;
  orders[1].reported_executed_base=0.25;
  assert!(inspect_joined_order_fee_hedge_r207(
    &orders,&fills,NOW,"BTC","USDT",0.0,0.0).is_err());
 }
 #[test]fn r207_unbounded_qty_tolerance_cannot_hide_mismatch(){
  let (mut orders,fills)=claims();
  orders[0].reported_executed_base=0.5;
  assert!(check_order_fill_claims_r205(&orders,&fills,NOW,100.0).is_err());
  assert!(inspect_joined_order_fee_hedge_r207(
    &orders,&fills,NOW,"BTC","USDT",100.0,100.0).is_err());
  let (orders,fills)=claims();
  assert!(inspect_joined_order_fee_hedge_r207(
    &orders,&fills,NOW,"BTC","USDT",0.0,1.0).is_err());
  assert!(inspect_joined_order_fee_hedge_r207(
    &orders,&fills,NOW,"BTC","USDT",0.0,0.0).is_ok());
 }
 #[test]fn r207_partial_unknown_late_report_and_invalid_fee_denied(){
  let (mut orders,fills)=claims();
  orders[0].allow_partial=true;
  assert!(inspect_joined_order_fee_hedge_r207(
    &orders,&fills,NOW,"BTC","USDT",0.0,0.0).is_err());
  let (mut orders,fills)=claims();
  orders[1].reported_at_ms=NOW-1001;
  assert!(inspect_joined_order_fee_hedge_r207(
    &orders,&fills,NOW,"BTC","USDT",0.0,0.0).is_err());
  let (orders,mut fills)=claims();
  fills[2].fee_currency="BTC".into();
  assert!(inspect_joined_order_fee_hedge_r207(
    &orders,&fills,NOW,"BTC","USDT",0.0,0.0).is_err());
  let (orders,mut fills)=claims();
  fills[1].fill_id=fills[0].fill_id.clone();
  assert!(inspect_joined_order_fee_hedge_r207(
    &orders,&fills,NOW,"BTC","USDT",0.0,0.0).is_err());
 }
 #[test]fn r205_offline_order_ids_and_aggregate_quantities(){
  let (orders,fills)=claims();
  let report=check_order_fill_claims_r205(&orders,&fills,NOW,0.00000001).unwrap();
  assert_eq!((report.order_count,report.fill_count),(2,4));
  assert!(!report.actual_provider_attested&&!report.may_trade);
 }
 #[test]fn r205_mismatched_order_id_partial_unfilled_or_wrong_direction_denied(){
  let (mut orders,mut fills)=claims();
  fills[3].order_id="wrong-order".into();
  assert!(check_order_fill_claims_r205(&orders,&fills,NOW,0.00000001).is_err());
  let (o,f)=claims();orders=o;fills=f;orders[1].side=OrderSideR205::Buy;
  assert!(check_order_fill_claims_r205(&orders,&fills,NOW,0.00000001).is_err());
  orders[1].side=OrderSideR205::Sell;orders[1].allow_partial=true;
  assert!(check_order_fill_claims_r205(&orders,&fills,NOW,0.00000001).is_err());
 }
}
