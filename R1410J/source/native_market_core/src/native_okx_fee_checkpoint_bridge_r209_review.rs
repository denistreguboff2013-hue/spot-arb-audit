//! Astra R209/P1-06: local write-once V2 OKX first-leg receipt -> R208
//! per-fill UTC+5 fee accounting. This is NOT signed exchange attestation.
//! Reuse load_v2's SHA, durable marker, source ledger and original owner
//! checks. No private API requests, money movements or real trade permit.
use std::path::Path;
use crate::{
 authoritative_state::NativeEntryPlan,
 native_spot_prehedge::NativeSpotPreHedgeEvidence,
 native_trading_config::{NativeEntryPolicyBinding,NativeTradingConfig},
 native_trade2f3_first_leg_v2_checkpoint::load_v2,
 native_okx_fee_day_r208_review::{
  inspect_okx_fee_day_lower_bound_r208,OkxDayFeeFloorR208,
  SourcedOkxFeeFillR208,OkxFeeDayError,
 },
};
#[derive(Clone,Debug)]
pub struct V2FeeReceiptInputR209<'a>{
 pub path:&'a Path,
 pub original_generation:u64,
 pub policy:&'a NativeEntryPolicyBinding,
 pub plan:&'a NativeEntryPlan,
 pub first:&'a NativeSpotPreHedgeEvidence,
 /// Operator-supplied ACCOUNT SCOPE, not independent authentication.
 pub account_owner:&'a str,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum V2FeeDayBridgeError{
 Config,Original,Unsupported,
 Fee(OkxFeeDayError),
}
pub fn inspect_v2_receipts_okx_fee_day_r209(
 cfg:&NativeTradingConfig,now_ms:u64,
 sources:&[V2FeeReceiptInputR209<'_>],
 unresolved_order_ids:&[String],
)->Result<OkxDayFeeFloorR208,V2FeeDayBridgeError>{
 if !cfg.source_projection_unchanged()
    ||cfg.policy.real_execution_authorized
    ||sources.is_empty()||sources.len()>128{
  return Err(V2FeeDayBridgeError::Config)
 }
 let mut rows=Vec::new();
 for s in sources{
  if s.policy.config_sha256!=cfg.sha256
   ||s.original_generation==0||s.account_owner.is_empty(){
   return Err(V2FeeDayBridgeError::Config)
  }
  // Exact persisted original: local checksum alone does not grant source
  // authenticity or rollback protection.
  let (checkpoint,preview)=load_v2(s.path,s.original_generation,
   s.policy,s.plan,s.first).map_err(|_|V2FeeDayBridgeError::Original)?;
  if preview.independent_live_exchange_attestation
   ||preview.persisted_owner_v2_seal_available
   ||preview.trading_or_close_authorized
   ||checkpoint.exchange_global_fees_attested
   ||checkpoint.may_send_or_verify_closed
   ||checkpoint.original_owner_v2_seal_migrated{
   return Err(V2FeeDayBridgeError::Unsupported)
  }
  for f in &checkpoint.source_shaped_fee_fills{
   // R208 deliberately cannot value native BASE commission without
   // an independently verified price and asset-cash contract.
   if f.fee_ccy!="USDT"||f.signed_fee_e12>0{
    return Err(V2FeeDayBridgeError::Unsupported)
   }
   let cost=f.signed_fee_e12.checked_neg()
    .and_then(|x|u128::try_from(x).ok())
    .ok_or(V2FeeDayBridgeError::Unsupported)?;
   rows.push(SourcedOkxFeeFillR208{
    account_owner:s.account_owner.into(),order_id:f.order_id.clone(),
    trade_id:f.trade_id.clone(),symbol:f.inst_id.replace('/',"-"),
    fee_ccy:f.fee_ccy.clone(),execution_at_ms:f.at_ms,
    quoted_fee_cost_e12:cost,source_row_is_paper:false,
   });
  }
 }
 // One join across ALL sidecars rejects replayed (symbol,tradeId) from
 // different local checkpoints instead of double-counting old fees.
 inspect_okx_fee_day_lower_bound_r208(cfg,now_ms,&rows,
  unresolved_order_ids).map_err(V2FeeDayBridgeError::Fee)
}
