//! Astra P1-02: feeCcy-aware, fixed-point proposed PAPER futures hedge sizing.
//! Uses already validated source-shaped first-leg fee preview and public symbol rules.
//! No network calls, no mutable order transport, no live hedge authority.
use crate::fill_economics::{decimal_to_atoms,atoms_to_string,SCALE_12};
use crate::market_metadata::NativeMarketMetadata;
use crate::native_trade2f3_fee_aware_first_leg_v2::FirstLegFeePreviewV2;

#[derive(Clone,Debug,PartialEq,Eq)]
pub enum HedgeSizeError{
 Missing(&'static str),BadMetadata(&'static str),BadFeeTruth(&'static str),
 Overflow,StaleMetadata,StaleQuote,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum HedgeSizeStatus{
 SizedWithNoDust,SizedWithResidualExposure,BlockedMinAmount,
 BlockedMinNotional,BlockedStepOrZero,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct NativePaperHedgeSize{
 pub symbol:String,
 pub status:HedgeSizeStatus,
 pub gross_spot_atoms_e12:u128,
 pub okx_base_fee_atoms_e12:u128,
 pub delivered_spot_atoms_e12:u128,
 pub proposed_futures_short_atoms_e12:u128,
 pub residual_unhedged_spot_atoms_e12:u128,
 pub proposed_future_notional_floor_atoms_e12:u128,
 pub min_future_notional_atoms_e12:u128,
 pub future_step_atoms_e12:u128,
 pub fully_hedged_by_proposal:bool,
 pub source_shape_fee_checked:bool,
 pub exchange_fill_signed_and_authenticated:bool,
 pub may_send_paper_or_real_orders:bool,
 pub may_mark_hedged:bool,
}
impl NativePaperHedgeSize{
 pub fn proposed_short_quantity(&self)->String{
   atoms_to_string(self.proposed_futures_short_atoms_e12)
 }
 pub fn dust_quantity(&self)->String{
   atoms_to_string(self.residual_unhedged_spot_atoms_e12)
 }
}
fn parsed(s:&str)->Result<u128,HedgeSizeError>{
 decimal_to_atoms(s).map_err(|_|HedgeSizeError::BadMetadata("public decimal invalid or >12 digits"))
}
fn positive_opt(s:Option<&String>,field:&'static str)->Result<u128,HedgeSizeError>{
 let val=s.ok_or(HedgeSizeError::BadMetadata(field))?;
 let atoms=parsed(val)?;
 if atoms==0{return Err(HedgeSizeError::BadMetadata(field))}
 Ok(atoms)
}
/// Price is a PUBLIC quote observation in USDT/base (fixed e12), not execution
/// truth. Min-notional is checked against rounded-DOWN notional, fail closed.
pub fn plan_fee_adjusted_paper_short(
 fee:&FirstLegFeePreviewV2,
 metadata:&NativeMarketMetadata,
 futures_bid_price_e12:u128,
 future_book_received_ns:u64,
 now_monotonic_ns:u64,
 max_book_age_ns:u64,
 max_metadata_age_ns:u64,
)->Result<NativePaperHedgeSize,HedgeSizeError>{
 if !fee.base_fee_is_explicitly_source_shaped
    ||fee.persisted_owner_v2_seal_available
    ||fee.independent_live_exchange_attestation
    ||fee.trading_or_close_authorized{
    return Err(HedgeSizeError::BadFeeTruth("unsupported authority or missing native fee source"))
 }
 let gross=fee.original_spot_gross_e12;
 let base_fee=fee.original_base_buy_fee_e12;
 let owned=fee.original_delivered_base_e12;
 if gross==0||owned==0||base_fee>gross
   ||gross-base_fee!=owned||futures_bid_price_e12==0{
   return Err(HedgeSizeError::BadFeeTruth("inconsistent fee-adjusted inventory or price"))
 }
 if metadata.metadata_state()!="SIZE_METADATA_READY"||
    metadata.spot_status!="live"||metadata.future_status!="TRADING"||
    metadata.spot_symbol!=format!("{}-USDT",metadata.base)||
    metadata.future_symbol!=format!("{}USDT",metadata.base){
   return Err(HedgeSizeError::BadMetadata("pair inactive, mismatched, or incomplete"))
 }
 if future_book_received_ns==0||max_book_age_ns==0
   ||now_monotonic_ns<future_book_received_ns
   ||now_monotonic_ns-future_book_received_ns>max_book_age_ns{
   return Err(HedgeSizeError::StaleQuote)
 }
 if metadata.loaded_ns==0||now_monotonic_ns<metadata.loaded_ns
   ||max_metadata_age_ns==0
   ||now_monotonic_ns-metadata.loaded_ns>max_metadata_age_ns{
   return Err(HedgeSizeError::StaleMetadata)
 }
 let step=positive_opt(metadata.future_step_base.as_ref(),"Binance market lot step missing")?;
 let min_amount=positive_opt(metadata.future_min_base.as_ref(),"Binance min quantity missing")?;
 let min_cost=parsed(&metadata.future_min_cost)?;
 let max_amount=match metadata.future_max_base.as_ref(){
   None=>None,
   Some(s)=>Some(positive_opt(Some(s),"Binance max quantity invalid")?),
 };
 // Never round the hedge UP, including after native BASE-denominated fees.
 let stepped=(owned/step)*step;
 let capped=match max_amount{
   Some(max)=>stepped.min((max/step)*step),
   None=>stepped,
 };
 let quote_floor=capped.checked_mul(futures_bid_price_e12)
    .ok_or(HedgeSizeError::Overflow)?/SCALE_12;
 let status=if capped==0{
    HedgeSizeStatus::BlockedStepOrZero
 }else if capped<min_amount{
    HedgeSizeStatus::BlockedMinAmount
 }else if quote_floor<min_cost{
    HedgeSizeStatus::BlockedMinNotional
 }else if capped==owned{
    HedgeSizeStatus::SizedWithNoDust
 }else{
    HedgeSizeStatus::SizedWithResidualExposure
 };
 let qty=match status{
    HedgeSizeStatus::SizedWithNoDust|HedgeSizeStatus::SizedWithResidualExposure=>capped,
    _=>0,
 };
 let residual=owned.checked_sub(qty).ok_or(HedgeSizeError::Overflow)?;
 Ok(NativePaperHedgeSize{
   symbol:metadata.future_symbol.clone(),status,
   gross_spot_atoms_e12:gross,okx_base_fee_atoms_e12:base_fee,
   delivered_spot_atoms_e12:owned,proposed_futures_short_atoms_e12:qty,
   residual_unhedged_spot_atoms_e12:residual,
   proposed_future_notional_floor_atoms_e12:
     qty.checked_mul(futures_bid_price_e12).ok_or(HedgeSizeError::Overflow)?/SCALE_12,
   min_future_notional_atoms_e12:min_cost,
   future_step_atoms_e12:step,
   fully_hedged_by_proposal:qty==owned && qty>0,
   source_shape_fee_checked:true,
   exchange_fill_signed_and_authenticated:false,
   may_send_paper_or_real_orders:false,may_mark_hedged:false,
 })
}
#[cfg(test)]
mod tests{
 use super::*;
 use crate::fill_economics::decimal_to_atoms as a;
 fn atoms(s:&str)->u128{a(s).unwrap()}
 fn original(gross:&str,base_fee:&str)->FirstLegFeePreviewV2{
   let g=atoms(gross);let f=atoms(base_fee);
   FirstLegFeePreviewV2{
     original_spot_gross_e12:g,
     original_base_buy_fee_e12:f,
     original_quote_buy_fee_e12:0,
     original_delivered_base_e12:g-f,
     quote_spent_with_quote_fees_e12:atoms("10"),
     base_fee_is_explicitly_source_shaped:true,
     persisted_owner_v2_seal_available:false,
     independent_live_exchange_attestation:false,
     trading_or_close_authorized:false,
   }
 }
 fn market(step:&str,min_amount:&str,min_cost:&str)->NativeMarketMetadata{
   NativeMarketMetadata{
     base:"ONE".into(),spot_symbol:"ONE-USDT".into(),
     future_symbol:"ONEUSDT".into(),
     spot_min_amount:Some("1".into()),
     future_min_base:Some(min_amount.into()),
     spot_min_cost:"0".into(),future_min_cost:min_cost.into(),
     spot_step:Some("0.01".into()),
     future_step_base:Some(step.into()),contract_size:"1".into(),
     buy_exchange:"okx_spot".into(),sell_exchange:"binance_futures".into(),
     route_label:crate::market_metadata::ROUTE_LABEL.into(),
     static_reason:None,loaded_ns:1_000,loaded_wall_ms:12_345,
     spot_tick:Some("0.00001".into()),future_tick:Some("0.00001".into()),
     spot_status:"live".into(),future_status:"TRADING".into(),
     spot_max_amount:None,future_max_base:None,
   }
 }
 fn size(f:&FirstLegFeePreviewV2,m:&NativeMarketMetadata)
   ->Result<NativePaperHedgeSize,HedgeSizeError>{
   plan_fee_adjusted_paper_short(f,m,atoms("0.02"),1_001,1_001,100,100)
 }
 #[test]fn base_fee_then_floor_never_overhedges(){
    let f=original("1000","1");
    let r=size(&f,&market("1","1","5")).unwrap();
    assert_eq!(r.proposed_short_quantity(),"999");
    assert_eq!(r.dust_quantity(),"0");
    assert!(r.fully_hedged_by_proposal);
    assert!(!r.may_send_paper_or_real_orders);
    assert!(!r.exchange_fill_signed_and_authenticated);
    assert!(!r.may_mark_hedged);
 }
 #[test]fn sub_step_fee_causes_explicit_dust(){
    let f=original("1000","1.123");
    let r=size(&f,&market("1","1","5")).unwrap();
    assert_eq!(r.proposed_short_quantity(),"998");
    assert_eq!(r.dust_quantity(),"0.877");
    assert_eq!(r.status,HedgeSizeStatus::SizedWithResidualExposure);
    assert!(!r.fully_hedged_by_proposal);
 }
 #[test]fn quote_ccy_fee_does_not_reduce_base(){
    let f=original("1000","0");
    let r=size(&f,&market("1","1","5")).unwrap();
    assert_eq!(r.proposed_short_quantity(),"1000");
    assert_eq!(r.okx_base_fee_atoms_e12,0);
 }
 #[test]fn minimum_notional_fails_closed_with_entire_exposure(){
    let f=original("100","0");
    let r=size(&f,&market("1","1","5")).unwrap();
    assert_eq!(r.status,HedgeSizeStatus::BlockedMinNotional);
    assert_eq!(r.proposed_short_quantity(),"0");
    assert_eq!(r.dust_quantity(),"100");
 }
 #[test]fn below_minimum_quantity_does_not_hedge(){
    let f=original("2","0.4");
    let r=size(&f,&market("1","2","0")).unwrap();
    assert_eq!(r.status,HedgeSizeStatus::BlockedMinAmount);
    assert_eq!(r.proposed_short_quantity(),"0");
    assert_eq!(r.dust_quantity(),"1.6");
 }
 #[test]fn below_step_does_not_hedge(){
    let f=original("0.005","0");
    let r=size(&f,&market("0.01","0.001","0")).unwrap();
    assert_eq!(r.status,HedgeSizeStatus::BlockedStepOrZero);
    assert_eq!(r.dust_quantity(),"0.005");
 }
 #[test]fn max_future_amount_caps_without_exceeding(){
    let f=original("1000","0");
    let mut m=market("1","1","0");
    m.future_max_base=Some("300".into());
    let r=size(&f,&m).unwrap();
    assert_eq!(r.proposed_short_quantity(),"300");
    assert_eq!(r.dust_quantity(),"700");
    assert!(!r.fully_hedged_by_proposal);
 }
 #[test]fn exact_step_with_large_decimal_supported(){
    let f=original("0.123456789123","0.000000000001");
    let r=size(&f,&market("0.000000000001","0.000000000001","0")).unwrap();
    assert_eq!(r.proposed_short_quantity(),"0.123456789122");
    assert_eq!(r.dust_quantity(),"0");
 }
 #[test]fn stale_public_book_price_fails_closed(){
    let result=plan_fee_adjusted_paper_short(
        &original("1000","1"),&market("1","1","0"),
        atoms("1"),1_001,1_500,100,1_000);
    assert_eq!(result,Err(HedgeSizeError::StaleQuote));
 }
 #[test]fn future_dated_public_book_price_fails_closed(){
    let result=plan_fee_adjusted_paper_short(
        &original("1000","1"),&market("1","1","0"),
        atoms("1"),1_501,1_500,100,1_000);
    assert_eq!(result,Err(HedgeSizeError::StaleQuote));
 }
 #[test]fn stale_metadata_denied(){
    assert_eq!(
      plan_fee_adjusted_paper_short(&original("1000","1"),
        &market("1","1","0"),atoms("1"),1_500,1_500,100,100),
      Err(HedgeSizeError::StaleMetadata)
    );
 }
 #[test]fn missing_market_step_denied(){
    let mut m=market("1","1","0");m.future_step_base=None;
    assert!(matches!(size(&original("1000","1"),&m),
       Err(HedgeSizeError::BadMetadata(_))));
 }
 #[test]fn mismatched_symbol_denied(){
    let mut m=market("1","1","0");m.future_symbol="OTHERUSDT".into();
    assert!(matches!(size(&original("1000","1"),&m),
       Err(HedgeSizeError::BadMetadata(_))));
 }
 #[test]fn noncanonical_source_rejected(){
    let mut f=original("1000","1");
    f.base_fee_is_explicitly_source_shaped=false;
    assert!(matches!(size(&f,&market("1","1","0")),
       Err(HedgeSizeError::BadFeeTruth(_))));
 }
 #[test]fn inconsistent_gross_minus_fee_denied(){
    let mut f=original("1000","1");
    f.original_delivered_base_e12=atoms("1000");
    assert!(matches!(size(&f,&market("1","1","0")),
       Err(HedgeSizeError::BadFeeTruth(_))));
 }
 #[test]fn third_asset_unrepresentable_fee_denied_upstream(){
    // Caller must supply first-leg source-shaped BASE/USDT bridge.
    // Third-asset fees cannot be silently converted to a ZERO BASE debit.
    let mut f=original("1000","0");
    f.trading_or_close_authorized=true;
    assert!(matches!(size(&f,&market("1","1","0")),
       Err(HedgeSizeError::BadFeeTruth(_))));
 }
 #[test]fn overflow_fails_closed(){
    let f=original("1000","0");
    let err=plan_fee_adjusted_paper_short(&f,&market("1","1","0"),
        u128::MAX,1_001,1_001,100,100);
    assert_eq!(err,Err(HedgeSizeError::Overflow));
 }
}
