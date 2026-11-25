//! Astra R210: SOURCE-SHAPED archived OKX BUY + SELL fee day preview.
//! Reuses the existing owner-bound four-leg archive reader and source
//! identity census. Never asserts live exchange ownership, completeness,
//! funding settlement, verified flatness, or real order permission.
use crate::{
 native_okx_fee_day_r208_review::{
  inspect_okx_fee_day_lower_bound_r208,OkxFeeDayError,OkxDayFeeFloorR208,
  SourcedOkxFeeFillR208,
 },
 native_paper_v2_close_source_archive_review::{
  inspect_supplied_local_archived_close_census,read_local_close_source,
  LocalArchivedCloseCandidate,ArchiveError,
 },
 native_trading_config::NativeTradingConfig,
 native_daily_risk::yekt_day_index,
};
#[derive(Debug,Clone,PartialEq,Eq)]
pub enum OkxArchiveFeeError{
 Config,Owner,Fee(OkxFeeDayError),Archive(ArchiveError),
}
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct OkxArchiveFeeDayReviewR210{
 pub quote_fee_day:OkxDayFeeFloorR208,
 pub included_buy_quote_fee_fills:usize,
 pub included_sell_quote_fee_fills:usize,
 pub unvalued_buy_base_fee_fills:usize,
 pub unvalued_buy_base_fee_asset:Option<String>,
 pub unvalued_buy_base_fee_atoms_e12:u128,
 pub unvalued_today_buy_base_fee_fills:usize,
 pub unvalued_today_buy_base_fee_atoms_e12:u128,
 pub claimed_local_closed_cycles:usize,
 pub complete_quote_and_base_fee_valuation_proven:bool,
 pub exchange_account_authenticated:bool,
 pub independently_verified_closed:bool,
 pub may_trade_real:bool,
}
// A scalar BASE-fee amount is meaningful only for ONE concrete asset.
// Never sum independently native-denominated fees (e.g. BTC + ETH).
fn require_same_unvalued_asset(
 current:&mut Option<String>,base:&str
)->Result<(),OkxArchiveFeeError>{
 if base.is_empty(){return Err(OkxArchiveFeeError::Owner)}
 match current{
  Some(previous) if previous.as_str()!=base=>Err(OkxArchiveFeeError::Owner),
  Some(_)=>Ok(()),
  None=>{*current=Some(base.to_owned());Ok(())}
 }
}
/// One caller-supplied account-owner scope; no claim the source list covers
/// OTHER accounts, active positions, UNKNOWN orders or omitted archives.
/// Opening fee in BASE is retained as UNVALUED asset quantity, NOT zero USDT.
pub fn inspect_archived_okx_buy_sell_fee_day_r210(
 cfg:&NativeTradingConfig,now_ms:u64,account_owner:&str,
 sources:&[LocalArchivedCloseCandidate<'_>],
 unresolved_orders:&[String],
)->Result<OkxArchiveFeeDayReviewR210,OkxArchiveFeeError>{
 if !cfg.source_projection_unchanged()
   ||cfg.policy.real_execution_authorized
   ||cfg.policy.real_daily_trades!=0
   ||account_owner.is_empty()||account_owner.len()>96
   ||sources.is_empty()||sources.len()>128{
    return Err(OkxArchiveFeeError::Config)
 }
 let census=inspect_supplied_local_archived_close_census(sources)
   .map_err(OkxArchiveFeeError::Archive)?;
 if census.exchange_account_authenticated||census.complete_owner_universe_proven
   ||census.independently_verified_closed||census.may_trade_real{
   return Err(OkxArchiveFeeError::Owner)
 }
 let mut rows=Vec::new();
 let mut unvalued_count=0usize;
 let mut unvalued_base=0u128;
 let mut unvalued_asset=None::<String>;
 let mut unvalued_today=0usize;
 let mut unvalued_today_atoms=0u128;
 let day=yekt_day_index(now_ms).map_err(|_|OkxArchiveFeeError::Fee(OkxFeeDayError::Timestamp))?;
 let mut buy_count=0usize;
 let mut sell_count=0usize;
 for src in sources{
  let owner=src.preclose_v2;
  let origin=&owner.genesis.checkpoint;
  if origin.policy.config_sha256!=cfg.sha256
    ||origin.original_owner_v2_seal_migrated
    ||origin.exchange_global_fees_attested
    ||origin.may_send_or_verify_closed{
    return Err(OkxArchiveFeeError::Owner)
  }
  // Existing reader verifies local SHA, linked immutable revision-4
  // owner, exact quantity/fill/bill/cash identities and nonauthority.
  let close=read_local_close_source(src.archive_path,owner)
    .map_err(OkxArchiveFeeError::Archive)?;
  let base=origin.plan.base.as_str();
  for f in &origin.source_shaped_fee_fills{
   if f.fee_ccy==base{
    require_same_unvalued_asset(&mut unvalued_asset,base)?;
    let charged=f.signed_fee_e12.checked_neg()
      .and_then(|v|u128::try_from(v).ok())
      .ok_or(OkxArchiveFeeError::Owner)?;
    unvalued_base=unvalued_base.checked_add(charged)
      .ok_or(OkxArchiveFeeError::Owner)?;
    unvalued_count+=1;
    let fee_day=yekt_day_index(f.at_ms).map_err(|_|OkxArchiveFeeError::Fee(OkxFeeDayError::Timestamp))?;
    if fee_day==day{
      unvalued_today+=1;
      unvalued_today_atoms=unvalued_today_atoms.checked_add(charged)
        .ok_or(OkxArchiveFeeError::Owner)?;
    }
   }else if f.fee_ccy=="USDT"{
    let fee=f.signed_fee_e12.checked_neg()
      .and_then(|v|u128::try_from(v).ok())
      .ok_or(OkxArchiveFeeError::Owner)?;
    rows.push(SourcedOkxFeeFillR208{
      account_owner:account_owner.into(),order_id:f.order_id.clone(),
      trade_id:f.trade_id.clone(),symbol:f.inst_id.replace('/',"-"),
      fee_ccy:"USDT".into(),execution_at_ms:f.at_ms,
      quoted_fee_cost_e12:fee,source_row_is_paper:false,
    });
    buy_count+=1;
   }else{return Err(OkxArchiveFeeError::Owner)}
  }
  for f in &close.spot_sell_rows{
   if f.fee_currency!="USDT"{
    return Err(OkxArchiveFeeError::Owner)
   }
   let fee=f.signed_fee_quote_atoms_e12.checked_neg()
     .and_then(|v|u128::try_from(v).ok())
     .ok_or(OkxArchiveFeeError::Owner)?;
   rows.push(SourcedOkxFeeFillR208{
     account_owner:account_owner.into(),order_id:f.order_id.clone(),
     trade_id:f.trade_id.clone(),symbol:f.symbol.clone(),
     fee_ccy:"USDT".into(),execution_at_ms:f.occurred_at_ms,
     quoted_fee_cost_e12:fee,source_row_is_paper:false,
   });
   sell_count+=1;
  }
 }
 let day=inspect_okx_fee_day_lower_bound_r208(
   cfg,now_ms,&rows,unresolved_orders).map_err(OkxArchiveFeeError::Fee)?;
 Ok(OkxArchiveFeeDayReviewR210{
  quote_fee_day:day,included_buy_quote_fee_fills:buy_count,
  included_sell_quote_fee_fills:sell_count,
  unvalued_buy_base_fee_fills:unvalued_count,
  unvalued_buy_base_fee_atoms_e12:unvalued_base,
  unvalued_buy_base_fee_asset:unvalued_asset,
  unvalued_today_buy_base_fee_fills:unvalued_today,
  unvalued_today_buy_base_fee_atoms_e12:unvalued_today_atoms,
  claimed_local_closed_cycles:sources.len(),
  complete_quote_and_base_fee_valuation_proven:false,
  exchange_account_authenticated:false,
  independently_verified_closed:false,
  may_trade_real:false,
 })
}

#[cfg(test)]
mod r211_asset_mix_tests{
 use super::*;
 #[test]
 fn native_base_fee_units_must_never_mix_across_assets(){
  let mut one=None;
  require_same_unvalued_asset(&mut one,"BTC").unwrap();
  require_same_unvalued_asset(&mut one,"BTC").unwrap();
  assert_eq!(one.as_deref(),Some("BTC"));
  assert_eq!(require_same_unvalued_asset(&mut one,"ETH"),
    Err(OkxArchiveFeeError::Owner));
  assert_eq!(one.as_deref(),Some("BTC"),
   "rejected currency must not rewrite existing fee-unit identity");
  let mut none=None;
  assert_eq!(require_same_unvalued_asset(&mut none,""),
    Err(OkxArchiveFeeError::Owner));
  assert_eq!(none,None);
 }
}
