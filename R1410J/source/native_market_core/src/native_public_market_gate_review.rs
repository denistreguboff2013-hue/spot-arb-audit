//! P1-06 public market screening, NEVER a live execution permit.
//! All values below come from untrusted public market frames. No account
//! positions, executable depth, fills, fees or real PnL are verified.
use crate::fill_economics::{decimal_to_atoms,SCALE_12};
use crate::native_trading_config::NativeTradingConfig;
use serde_json::Value;
use std::collections::BTreeSet;

#[derive(Debug,Clone,PartialEq,Eq)]
pub enum PublicMarketGateError {Malformatted,UntrustedSource,Overflow,Policy}
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct PublicOkxSpotTicker {
 pub base:String,
 pub volume_24h_quote_e12:u128,
 pub exchange_wall_ms:u64,
 pub received_wall_ms:u64,
}
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct NativePublicMarketGateReview {
 pub gross_spread_milli_bps:i128,
 pub reserved_cost_milli_bps:u128,
 pub indicative_net_spread_milli_bps:i128,
 pub local_screen_pass:bool,
 pub reason:&'static str,
 pub exchange_authenticated:bool,
 pub executable_depth_verified:bool,
 pub actual_fee_and_funding_proven:bool,
 pub may_open_close_cancel_or_resubmit:bool,
}
/// Accept only OKX public SPOT tickers and quote-currency volCcy24h.
/// Invalid/ambiguous timestamps, decimal notation or venue are DENIED.
pub fn parse_public_okx_spot_ticker(
 raw:&str,requested_bases:&BTreeSet<String>,receive_ms:u64,
)->Result<PublicOkxSpotTicker,PublicMarketGateError>{
 if raw.len()>32*1024||receive_ms==0||requested_bases.is_empty()
  ||requested_bases.len()>32{return Err(PublicMarketGateError::Malformatted)}
 let v:Value=serde_json::from_str(raw)
   .map_err(|_|PublicMarketGateError::Malformatted)?;
 if v.get("event").is_some()||v.get("op").is_some()
  ||v.get("arg").and_then(|a|a.get("channel")).and_then(Value::as_str)
    !=Some("tickers") {return Err(PublicMarketGateError::UntrustedSource)}
 let array=v.get("data").and_then(Value::as_array)
  .ok_or(PublicMarketGateError::Malformatted)?;
 if array.len()!=1{return Err(PublicMarketGateError::Malformatted)}
 let row=&array[0];
 let inst=row.get("instId").and_then(Value::as_str)
  .ok_or(PublicMarketGateError::Malformatted)?;
 if row.get("instType").and_then(Value::as_str)!=Some("SPOT")
  ||v.get("arg").and_then(|a|a.get("instId")).and_then(Value::as_str)!=Some(inst)
  ||!inst.ends_with("-USDT"){return Err(PublicMarketGateError::UntrustedSource)}
 let base=inst.strip_suffix("-USDT").unwrap_or("");
 if !requested_bases.contains(base)||base.len()>32||base.is_empty()
  ||!base.bytes().all(|b|b.is_ascii_uppercase()||b.is_ascii_digit()){
  return Err(PublicMarketGateError::UntrustedSource)
 }
 let volume=row.get("volCcy24h").and_then(Value::as_str)
  .ok_or(PublicMarketGateError::Malformatted)?;
 // Public turnover can have more fractional digits than order quantities.
 // Truncate only surplus turnover decimals: never round eligibility upward.
 let bounded=if let Some((whole,fraction))=volume.split_once('.'){
  if fraction.len()>12{
   if !fraction.bytes().all(|b|b.is_ascii_digit()){return Err(PublicMarketGateError::Malformatted)}
   format!("{whole}.{}",&fraction[..12])
  }else{volume.to_string()}
 }else{volume.to_string()};
 let volume_e12=decimal_to_atoms(&bounded)
  .map_err(|_|PublicMarketGateError::Malformatted)?;
 if volume_e12==0||volume_e12>1_000_000_000_000*SCALE_12{
  return Err(PublicMarketGateError::Malformatted)
 }
 let ts=row.get("ts").and_then(Value::as_str)
  .ok_or(PublicMarketGateError::Malformatted)?;
 if ts.len()!=13||!ts.bytes().all(|b|b.is_ascii_digit()){
  return Err(PublicMarketGateError::Malformatted)
 }
 let exchange_wall_ms=ts.parse::<u64>()
  .map_err(|_|PublicMarketGateError::Malformatted)?;
 // A very old or future-dated ticker must not rescue a stale book.
 if exchange_wall_ms>receive_ms.saturating_add(2_000)
  ||receive_ms.saturating_sub(exchange_wall_ms)>30_000{
  return Err(PublicMarketGateError::UntrustedSource)
 }
 Ok(PublicOkxSpotTicker{
  base:base.into(),volume_24h_quote_e12:volume_e12,
  exchange_wall_ms,received_wall_ms:receive_ms,
 })
}
/// Conservative local edge estimate. No executable quote-size depth, or
/// source authentication. A positive diagnostic never authorizes a trade.
pub fn inspect_public_spread_gate(
 cfg:&NativeTradingConfig,spot_ask_e12:u128,futures_bid_e12:u128,
 volume_quote_e12:Option<u128>,
 spot_age_ms:u64,futures_age_ms:u64,ticker_age_ms:Option<u64>,
)->Result<NativePublicMarketGateReview,PublicMarketGateError>{
 let p=&cfg.policy;
 if p.real_execution_authorized||cfg.sha256.len()!=64
  ||spot_ask_e12==0||futures_bid_e12==0
  ||spot_ask_e12>1_000_000*SCALE_12
  ||futures_bid_e12>1_000_000*SCALE_12
  ||p.entry_min_milli_bps<42_000||p.fee_reserve_milli_bps>1_000_000{
  return Err(PublicMarketGateError::Policy)
 }
 let ask=i128::try_from(spot_ask_e12).map_err(|_|PublicMarketGateError::Overflow)?;
 let bid=i128::try_from(futures_bid_e12).map_err(|_|PublicMarketGateError::Overflow)?;
 let gross=(bid-ask).checked_mul(10_000_000)
  .and_then(|n|n.checked_div(ask))
  .ok_or(PublicMarketGateError::Overflow)?;
 let fees=i128::try_from(p.fee_reserve_milli_bps)
  .map_err(|_|PublicMarketGateError::Overflow)?;
 let net=gross.checked_sub(fees).ok_or(PublicMarketGateError::Overflow)?;
 let mut screened=false;
 let reason=if spot_age_ms>800||futures_age_ms>800{
  "STALE_BOOK"
 }else if volume_quote_e12.is_none()||ticker_age_ms.is_none(){
  "MISSING_VERIFIED_PUBLIC_TICKER"
 }else if ticker_age_ms.unwrap()>30_000{
  "STALE_PUBLIC_TICKER"
 }else{
  let price_e18=spot_ask_e12.checked_mul(1_000_000)
   .ok_or(PublicMarketGateError::Overflow)?;
  let volume_e18=volume_quote_e12.unwrap().checked_mul(1_000_000)
   .ok_or(PublicMarketGateError::Overflow)?;
  let local=cfg.inspect_market_filters_read_only(price_e18,volume_e18,gross)
   .map_err(|_|PublicMarketGateError::Policy)?;
  if !local.passes_legacy_filters_locally{local.reason}
  else if net<i128::try_from(p.entry_min_milli_bps)
   .map_err(|_|PublicMarketGateError::Overflow)?{
   "BELOW_NET_ENTRY_THRESHOLD"
  }else{
   screened=true;
   "LOCAL_PUBLIC_MARKET_SIGNAL_ONLY_NOT_EXECUTABLE"
  }
 };
 Ok(NativePublicMarketGateReview{
  gross_spread_milli_bps:gross,
  reserved_cost_milli_bps:p.fee_reserve_milli_bps,
  indicative_net_spread_milli_bps:net,
  local_screen_pass:screened,reason,exchange_authenticated:false,
  executable_depth_verified:false,actual_fee_and_funding_proven:false,
  may_open_close_cancel_or_resubmit:false,
 })
}

#[cfg(test)]
mod tests {
 use super::*;
 const C:&str=include_str!("../config/native_trading.yaml");
 fn config()->NativeTradingConfig{NativeTradingConfig::parse(C).unwrap()}
 fn ticker(base:&str,itype:&str,volume:&str,ts:&str)->String{
  format!(r#"{{"arg":{{"channel":"tickers","instId":"{base}-USDT"}},"data":[{{"instType":"{itype}","instId":"{base}-USDT","volCcy24h":"{volume}","ts":"{ts}"}}]}}"#)
 }
 #[test]
 fn public_ticker_exact_quote_volume_and_identity(){
  let bases=["BTC".to_owned(),"ETH".to_owned()].into_iter().collect();
  let ms=1_750_000_000_000u64;
  let ok=parse_public_okx_spot_ticker(
   &ticker("BTC","SPOT","200000.123456789","1750000000000"),&bases,ms).unwrap();
  assert_eq!(ok.base,"BTC");
  assert_eq!(ok.volume_24h_quote_e12,200_000_123_456_789_000);
  let precise=parse_public_okx_spot_ticker(&ticker("BTC","SPOT","6167.9336216367027","1750000000000"),&bases,ms).unwrap();
  assert_eq!(precise.volume_24h_quote_e12,6_167_933_621_636_702);
  assert!(parse_public_okx_spot_ticker(&ticker("BTC","SPOT","6167.933621636702x","1750000000000"),&bases,ms).is_err());

  for invalid in [
   ticker("BTC","SWAP","200000","1750000000000"),
   ticker("SOL","SPOT","200000","1750000000000"),
   ticker("BTC","SPOT","-200000","1750000000000"),
   ticker("BTC","SPOT","NaN","1750000000000"),
   ticker("BTC","SPOT","1e90","1750000000000"),
   ticker("BTC","SPOT","200000","1750000300000"),
   ticker("BTC","SPOT","200000","1749999900000"),
   ticker("BTC","SPOT","0","1750000000000"),
   ticker("BTC","SPOT","200000","0"),
  ]{
   assert!(parse_public_okx_spot_ticker(&invalid,&bases,ms).is_err(),
    "must reject: {invalid}");
  }
  let mismatch=ticker("BTC","SPOT","200000","1750000000000")
   .replace(r#""instId":"BTC-USDT"},"#,
            r#""instId":"ETH-USDT"},"#);
  assert!(parse_public_okx_spot_ticker(&mismatch,&bases,ms).is_err());
  let book=ticker("BTC","SPOT","200000","1750000000000")
   .replace(r#""channel":"tickers""#,r#""channel":"books5""#);
  assert!(parse_public_okx_spot_ticker(&book,&bases,ms).is_err());
 }
 #[test]
 fn signal_threshold_net_not_gross_and_real_order_always_denied(){
  let c=config();
  // 100.000 -> 100.6603 => 66.03 gross - 24.03 cost = 42.000 net
  let ok=inspect_public_spread_gate(&c,100*SCALE_12,100_660_300_000_000,
   Some(200_000*SCALE_12),50,40,Some(2_000)).unwrap();
  assert!(ok.local_screen_pass);
  assert_eq!(ok.gross_spread_milli_bps,66_030);
  assert_eq!(ok.indicative_net_spread_milli_bps,42_000);
  assert_eq!(ok.reason,"LOCAL_PUBLIC_MARKET_SIGNAL_ONLY_NOT_EXECUTABLE");
  assert!(!ok.exchange_authenticated&&!ok.executable_depth_verified);
  assert!(!ok.actual_fee_and_funding_proven);
  assert!(!ok.may_open_close_cancel_or_resubmit);
  let low=inspect_public_spread_gate(&c,100*SCALE_12,100_660_299_999_999,
   Some(200_000*SCALE_12),50,40,Some(2_000)).unwrap();
  assert!(!low.local_screen_pass);
  assert_eq!(low.reason,"BELOW_NET_ENTRY_THRESHOLD");
  let neg=inspect_public_spread_gate(&c,100*SCALE_12,99*SCALE_12,
   Some(200_000*SCALE_12),50,40,Some(1)).unwrap();
  assert!(neg.indicative_net_spread_milli_bps<0&&!neg.local_screen_pass);
 }
 #[test]
 fn fail_closed_without_volume_or_freshness(){
  let c=config();
  let inspect=|ask,fut,vol,spot,future,ticker|inspect_public_spread_gate(
   &c,ask,fut,vol,spot,future,ticker).unwrap();
  let a=100*SCALE_12;
  let b=101*SCALE_12;
  assert_eq!(inspect(a,b,None,0,0,None).reason,
   "MISSING_VERIFIED_PUBLIC_TICKER");
  assert_eq!(inspect(a,b,Some(200_000*SCALE_12),801,0,Some(0)).reason,
   "STALE_BOOK");
  assert_eq!(inspect(a,b,Some(200_000*SCALE_12),0,0,Some(30_001)).reason,
   "STALE_PUBLIC_TICKER");
  assert_eq!(inspect(a,b,Some(149_999*SCALE_12),0,0,Some(0)).reason,
   "BELOW_24H_VOLUME");
  assert_eq!(inspect(SCALE_12/100,b,Some(200_000*SCALE_12),0,0,Some(0)).reason,
   "BELOW_MIN_PRICE");
  assert!(inspect_public_spread_gate(&c,0,b,Some(200_000*SCALE_12),0,0,Some(0)).is_err());
  let mut altered=config();
  altered.policy.real_execution_authorized=true;
  assert!(inspect_public_spread_gate(&altered,a,b,Some(200_000*SCALE_12),0,0,Some(0)).is_err());
 }
}
