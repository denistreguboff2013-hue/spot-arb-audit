//! Astra R208 / P1-06: offline, source-shaped OKX fill-fee DAY accounting.
//! A completed trade's day != the day its entry fee was charged. This
//! computes only a known lower bound from quote-denominated OKX fills.
//! No exchange authentication, completeness, refund, or real authorization.
use std::collections::BTreeSet;
use crate::{
 native_daily_risk::yekt_day_index,
 native_trading_config::NativeTradingConfig,
};
const MAX_ROWS:usize=30_000;
const MAX_ID_LEN:usize=96;
const QUOTE_SCALE:u128=1_000_000; // policy e18 -> monetary e12
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum OkxFeeDayError{Config,Timestamp,Duplicate,Unsupported,Overflow}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct SourcedOkxFeeFillR208{
 pub account_owner:String,pub order_id:String,pub trade_id:String,
 pub symbol:String,pub fee_ccy:String,
 pub execution_at_ms:u64,pub quoted_fee_cost_e12:u128,
 /// If any original OKX feeCcy is BASE or third asset, its quote
 /// valuation must NOT be assumed from this field: reject that row.
 pub source_row_is_paper:bool,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct OkxDayFeeFloorR208{
 pub yekt_day:u64,
 pub known_quote_okx_fees_e12:u128,
 pub quote_fee_cap_e12:u128,
 pub known_today_fill_count:usize,
 pub known_earlier_fill_count:usize,
 pub open_or_unknown_order_count:usize,
 pub cap_reached_by_known_fees:bool,
 /// Lower-bound math cannot prove that there are NO omitted fills,
 /// BASE-denominated fees, rebates, transfers, funding or late posts.
 pub complete_day_fee_history_proven:bool,
 pub below_cap_authoritatively_proven:bool,
 pub exchange_source_authenticated:bool,
 pub may_open_real:bool,
}
fn safe_id(s:&str)->bool{
 !s.is_empty()&&s.len()<=MAX_ID_LEN&&s.bytes().all(|b|
   b.is_ascii_alphanumeric()||b"._-:/".contains(&b))
}
/// Treat all supplied rows as untrusted. Duplicate trade IDs across
/// separate fills/close cycles are rejected instead of double counted.
/// Unknown / open orders contribute no invented zero fee or closure.
pub fn inspect_okx_fee_day_lower_bound_r208(
 cfg:&NativeTradingConfig,now_ms:u64,
 rows:&[SourcedOkxFeeFillR208],unresolved_order_ids:&[String]
)->Result<OkxDayFeeFloorR208,OkxFeeDayError>{
 if !cfg.source_projection_unchanged()
   ||cfg.policy.real_execution_authorized||cfg.policy.real_daily_trades!=0
   ||rows.len()>MAX_ROWS||unresolved_order_ids.len()>MAX_ROWS{
    return Err(OkxFeeDayError::Config)
 }
 let day=yekt_day_index(now_ms).map_err(|_|OkxFeeDayError::Timestamp)?;
 let cap=cfg.policy.daily_okx_fee_cap_e18/QUOTE_SCALE;
 if cap==0{return Err(OkxFeeDayError::Config)}
 let mut owner=None::<&str>;
 let mut seen=BTreeSet::new();
 let mut earlier=0usize;let mut today=0usize;
 let mut fees=0u128;
 for r in rows{
  if !safe_id(&r.account_owner)||!safe_id(&r.order_id)
   ||!safe_id(&r.trade_id)||!safe_id(&r.symbol)
   ||r.source_row_is_paper||r.fee_ccy!="USDT"
   ||r.execution_at_ms==0||r.execution_at_ms>now_ms{
    return Err(OkxFeeDayError::Unsupported)
  }
  if let Some(prev)=owner{
   if prev!=r.account_owner{return Err(OkxFeeDayError::Unsupported)}
  }else{owner=Some(&r.account_owner)}
  // An OKX trade ID is bound to its instrument. Checking orderId as
  // well would allow a forged order identity to replay the same fill.
  if !seen.insert((r.symbol.as_str(),r.trade_id.as_str())){return Err(OkxFeeDayError::Duplicate)}
  let at=yekt_day_index(r.execution_at_ms)
    .map_err(|_|OkxFeeDayError::Timestamp)?;
  if at==day{
   today+=1;
   fees=fees.checked_add(r.quoted_fee_cost_e12)
      .ok_or(OkxFeeDayError::Overflow)?;
  }else{
   earlier+=1;
  }
 }
 let mut unknowns=BTreeSet::new();
 for id in unresolved_order_ids{
  if !safe_id(id){return Err(OkxFeeDayError::Unsupported)}
  if !unknowns.insert(id){return Err(OkxFeeDayError::Duplicate)}
 }
 // Reject accidental double-accounting of a known fill/order owner as
 // an UNKNOWN result? No: an order may have partial fills AND unknown
 // terminal status. Keep it unresolved and deny completeness instead.
 Ok(OkxDayFeeFloorR208{
  yekt_day:day,known_quote_okx_fees_e12:fees,
  quote_fee_cap_e12:cap,
  known_today_fill_count:today,known_earlier_fill_count:earlier,
  open_or_unknown_order_count:unknowns.len(),
  cap_reached_by_known_fees:fees>=cap,
  complete_day_fee_history_proven:false,
  below_cap_authoritatively_proven:false,
  exchange_source_authenticated:false,
  may_open_real:false,
 })
}

#[derive(Clone,Debug,PartialEq,Eq)]
pub enum NativePendingFeeJoinErrorR135{
 Config,
 Native(crate::authoritative_state::NativeStateError),
 Fee(OkxFeeDayError),
 Drift,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct NativePendingFeeJoinR135{
 pub native_generation:u64,
 pub native_unresolved_spot_count:usize,
 pub native_unresolved_futures_count:usize,
 pub local_unresolved_client_id:String,
 pub fee_day:OkxDayFeeFloorR208,
 /// This is one *local* state owner, not the exchange account universe.
 pub account_wide_inventory_complete:bool,
 pub source_exchange_authenticated:bool,
 pub may_open_real:bool,
}
/// R135: read an EXISTING durable native pending intent and inject its
/// client-order identity into the local OKX fee-day view. No caller can
/// omit the local SPOT pending identity from the supplied unknown-id list.
/// Futures pending stays a separate Binance risk barrier; it is NOT an OKX
/// order and must not manufacture an OKX fee. This remains read-only and
/// cannot establish account-wide order completeness or a real permit.
pub fn inspect_persisted_native_pending_fee_day_r135(
 store:&crate::authoritative_state::NativeStateStore,
 expected_generation:u64,cfg:&NativeTradingConfig,now_ms:u64,
 known_okx_fills:&[SourcedOkxFeeFillR208],
)->Result<NativePendingFeeJoinR135,NativePendingFeeJoinErrorR135>{
 use crate::authoritative_state::NativeEntryLeg;
 if !cfg.source_projection_unchanged()
   ||cfg.policy.real_execution_authorized||cfg.policy.real_daily_trades!=0
   ||cfg.raw["real_trial"]["deny_on_unknown"].as_bool()!=Some(true){
    return Err(NativePendingFeeJoinErrorR135::Config)
 }
 let pending=store.pending_native_entry_query(expected_generation)
   .map_err(NativePendingFeeJoinErrorR135::Native)?;
 if pending.generation!=expected_generation
   ||pending.created_at_ms==0||pending.created_at_ms>now_ms{
    return Err(NativePendingFeeJoinErrorR135::Drift)
 }
 let (spot,futures,unresolved_okx):(_,_,Vec<String>)=match pending.leg{
  NativeEntryLeg::Spot=>(1,0,vec![format!("client:{}",pending.client_id)]),
  NativeEntryLeg::Futures=>(0,1,Vec::new()),
 };
 // R208 remains conditional on user-supplied fill rows and cannot
 // infer absent invoices, late fills, commissions or funding.
 let day=inspect_okx_fee_day_lower_bound_r208(
   cfg,now_ms,known_okx_fills,&unresolved_okx)
   .map_err(NativePendingFeeJoinErrorR135::Fee)?;
 // Catch changes that occur while inspecting local evidence. This
 // double read is NOT atomicity or external antirollback protection.
 let after=store.pending_native_entry_query(expected_generation)
   .map_err(NativePendingFeeJoinErrorR135::Native)?;
 if after!=pending{return Err(NativePendingFeeJoinErrorR135::Drift)}
 if day.may_open_real||day.exchange_source_authenticated
   ||day.complete_day_fee_history_proven||day.below_cap_authoritatively_proven{
    return Err(NativePendingFeeJoinErrorR135::Drift)
 }
 Ok(NativePendingFeeJoinR135{
  native_generation:pending.generation,
  native_unresolved_spot_count:spot,
  native_unresolved_futures_count:futures,
  local_unresolved_client_id:pending.client_id,
  fee_day:day,
  account_wide_inventory_complete:false,
  source_exchange_authenticated:false,
  may_open_real:false,
 })
}
