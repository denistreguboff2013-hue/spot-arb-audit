// R198: two-timepoint zero-exposure CONSISTENCY shape, still UNATTESTED.
// This does not query real balances, futures positions or private exchanges.
#[derive(Debug,Clone)]
pub struct ExposureFrameR198{
 pub account:ZeroPositionCandidateR190,
 pub spot_base_inventory:f64,
 pub pending_transfer_base:f64,
 pub unresolved_fee_base:f64,
}
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct ZeroWindowR198{
 pub venues:usize,pub sample_count:usize,
 pub all_claims_consistent:bool,pub independently_attested:bool,pub may_trade:bool,
}
pub fn preflight_zero_window_r198(
 first:&[ExposureFrameR198],second:&[ExposureFrameR198],now:u64)
 ->Result<ZeroWindowR198,PaperAdapterErrorR156>{
 if first.len()!=2||second.len()!=2{return Err(PaperAdapterErrorR156::Source)}
 let mut earliest=u64::MAX;let mut latest=0u64;
 for frames in [first,second]{
  let claims:Vec<_>=frames.iter().map(|x|x.account.clone()).collect();
  preflight_zero_claim_r190(&claims,now)?;
  for frame in frames{
   let amounts=[frame.spot_base_inventory,frame.pending_transfer_base,
    frame.unresolved_fee_base];
   if amounts.iter().any(|v|!v.is_finite()||*v!=0.0){
    return Err(PaperAdapterErrorR156::Source)
   }
   earliest=earliest.min(frame.account.reported_at_ms);
   latest=latest.max(frame.account.reported_at_ms);
  }
 }
 if latest.saturating_sub(earliest)<1000||
   latest.saturating_sub(earliest)>9000{
  return Err(PaperAdapterErrorR156::Source)
 }
 // Require same owner and matching venue identity at both boundaries.
 for a in first{
  if !second.iter().any(|b|b.account.venue==a.account.venue
    &&b.account.account_owner==a.account.account_owner
    &&b.account.reported_at_ms>a.account.reported_at_ms){
   return Err(PaperAdapterErrorR156::Source)
  }
 }
 Ok(ZeroWindowR198{
  venues:2,sample_count:2,all_claims_consistent:true,
  independently_attested:false,may_trade:false
 })
}
#[cfg(test)]
mod r198_tests{
 use super::*;
 const NOW:u64=1_792_280_000_000;
 fn frames(at:u64)->Vec<ExposureFrameR198>{
  [EvidenceVenueR189::OkxSpot,EvidenceVenueR189::BinanceUsdtMFutures]
   .iter().map(|v|ExposureFrameR198{
    account:ZeroPositionCandidateR190{
     venue:*v,account_owner:"research-owner".into(),
     open_positions:0,open_orders:0,unsettled_fills:0,
     reported_at_ms:at,reported_by_model:false,account_scope_complete:true,
    },
    spot_base_inventory:0.0,pending_transfer_base:0.0,unresolved_fee_base:0.0,
   }).collect()
 }
 #[test]fn r198_consistent_two_venue_two_sample_window_is_not_attestation(){
  let a=frames(NOW-5000);let b=frames(NOW-2000);
  let result=preflight_zero_window_r198(&a,&b,NOW).unwrap();
  assert!(result.all_claims_consistent);
  assert_eq!((result.venues,result.sample_count),(2,2));
  assert!(!result.independently_attested&&!result.may_trade);
 }
 #[test]fn r198_pending_inventory_stale_and_incomplete_coverage_deny(){
  let a=frames(NOW-5000);let mut b=frames(NOW-2000);
  b[0].pending_transfer_base=0.001;
  assert!(preflight_zero_window_r198(&a,&b,NOW).is_err());
  b=frames(NOW-2000);b[1].spot_base_inventory=0.01;
  assert!(preflight_zero_window_r198(&a,&b,NOW).is_err());
  b=frames(NOW-2000);b[1].unresolved_fee_base=0.0001;
  assert!(preflight_zero_window_r198(&a,&b,NOW).is_err());
  b=frames(NOW-2000);b[1].account.account_scope_complete=false;
  assert!(preflight_zero_window_r198(&a,&b,NOW).is_err());
  b=frames(NOW-5000);
  assert!(preflight_zero_window_r198(&a,&b,NOW).is_err());
 }
}
