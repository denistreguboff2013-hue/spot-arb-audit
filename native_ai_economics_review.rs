//! R48: read-only deterministic economics helpers from Python ai/analyzer.py.
 //! No exchange verification is implied by caller-supplied evidence flags.
use crate::fill_economics::decimal_to_atoms;

#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum ExecutionSide{Buy,Sell}
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum EconomicsReviewError{ArithmeticOverflow}

/// Positive is adverse to the trader. Output is signed millionths of 1 bp.
/// Missing/invalid/non-positive price yields None; no imputation to zero.
/// Arithmetic overflow is an explicit error, never zero or an allowed trade.
pub fn slippage_micro_bps(expected:Option<&str>,actual:Option<&str>,
 side:ExecutionSide)->Result<Option<i128>,EconomicsReviewError>{
 let price=|v:Option<&str>|v.and_then(|s|decimal_to_atoms(s).ok()).filter(|v|*v>0);
 let (expected,actual)=match (price(expected),price(actual)){
  (Some(e),Some(a))=>(e,a),_=>return Ok(None)
 };
 let expected=i128::try_from(expected).map_err(|_|EconomicsReviewError::ArithmeticOverflow)?;
 let actual=i128::try_from(actual).map_err(|_|EconomicsReviewError::ArithmeticOverflow)?;
 let delta=actual.checked_sub(expected).ok_or(EconomicsReviewError::ArithmeticOverflow)?;
 let numerator=delta.checked_mul(10_000_000_000)
  .ok_or(EconomicsReviewError::ArithmeticOverflow)?;
 let bps_micro=numerator.checked_div(expected).ok_or(EconomicsReviewError::ArithmeticOverflow)?;
 Ok(Some(if side==ExecutionSide::Buy{bps_micro}else{
  bps_micro.checked_neg().ok_or(EconomicsReviewError::ArithmeticOverflow)?}))
}

#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum AnalyticsMode{Paper,Real,Test,Replay}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct ScreenedPnl{
 pub net_atoms_e12:i128,
 pub source_claims_verified_real:bool,
 /// Actual exchange authenticity cannot be established from a local claim.
 pub independently_exchange_authenticated:bool,
 pub may_trade_or_release:bool,
}
/// Rust equivalent of ai/analyzer.py _pnl evidence filter, not a proof engine.
pub fn screen_claimed_net_pnl(mode:AnalyticsMode,net:Option<&str>,
 is_exchange_verified:bool,fees_complete:bool,pnl_source:Option<&str>)
 ->Option<ScreenedPnl>{
 if mode==AnalyticsMode::Real &&
   (!is_exchange_verified||!fees_complete||pnl_source!=Some("exchange_fills")){
  return None
 }
 let value=crate::fill_economics::signed_decimal_to_atoms(net?).ok()?;
 Some(ScreenedPnl{net_atoms_e12:value,
  source_claims_verified_real:mode==AnalyticsMode::Real,
  independently_exchange_authenticated:false,may_trade_or_release:false})
}

#[cfg(test)]
mod tests{
 use super::*;
 #[test]
 fn slippage_direction_and_missing_prices(){
  assert_eq!(slippage_micro_bps(Some("100"),Some("101"),ExecutionSide::Buy),
    Ok(Some(100_000_000)));
  assert_eq!(slippage_micro_bps(Some("100"),Some("99"),ExecutionSide::Buy),
    Ok(Some(-100_000_000)));
  assert_eq!(slippage_micro_bps(Some("100"),Some("99"),ExecutionSide::Sell),
    Ok(Some(100_000_000)));
  assert_eq!(slippage_micro_bps(Some("100"),Some("101"),ExecutionSide::Sell),
    Ok(Some(-100_000_000)));
  assert_eq!(slippage_micro_bps(Some("100"),None,ExecutionSide::Buy),Ok(None));
  assert_eq!(slippage_micro_bps(Some("0"),Some("1"),ExecutionSide::Buy),Ok(None));
  assert_eq!(slippage_micro_bps(Some("bad"),Some("1"),ExecutionSide::Buy),Ok(None));
 }
 #[test]
 fn exact_fraction_and_overflow_deny(){
  assert_eq!(slippage_micro_bps(Some("100"),Some("100.001"),ExecutionSide::Buy),
    Ok(Some(100_000)));
  let huge="100000000000000000000";
  assert_eq!(slippage_micro_bps(Some("1"),Some(huge),ExecutionSide::Buy),
    Err(EconomicsReviewError::ArithmeticOverflow));
 }
 #[test]
 fn real_pnl_requires_all_three_claimed_evidence_fields(){
  for (verified,complete,source) in [
   (false,true,Some("exchange_fills")),
   (true,false,Some("exchange_fills")),
   (true,true,Some("paper_model")),
   (true,true,None)]{
   assert_eq!(screen_claimed_net_pnl(AnalyticsMode::Real,
     Some("1.20"),verified,complete,source),None);
  }
  let accepted=screen_claimed_net_pnl(AnalyticsMode::Real,
   Some("-0.25"),true,true,Some("exchange_fills")).unwrap();
  assert_eq!(accepted.net_atoms_e12,-250_000_000_000);
  assert!(accepted.source_claims_verified_real);
  assert!(!accepted.independently_exchange_authenticated);
  assert!(!accepted.may_trade_or_release);
  let paper=screen_claimed_net_pnl(AnalyticsMode::Paper,
   Some("0.125"),false,false,None).unwrap();
  assert!(!paper.source_claims_verified_real);
  assert_eq!(paper.net_atoms_e12,125_000_000_000);
  assert!(screen_claimed_net_pnl(AnalyticsMode::Real,
   None,true,true,Some("exchange_fills")).is_none());
 }
}
