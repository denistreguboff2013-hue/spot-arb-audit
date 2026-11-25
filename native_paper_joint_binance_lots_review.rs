//! Astra P1-02/P0-02: offline Binance MARKET futures two-filter lot compatibility.
//! Provenance is untrusted public exchangeInfo JSON, NEVER signed account truth.
//! Fail-closed on unknown filters; no exchange orders, authorizations or callbacks.
use crate::{
 fill_economics::{decimal_to_atoms,atoms_to_string},
 market_metadata::NativeMarketMetadata,
 native_paper_fee_hedge_sizing_review::{
   plan_fee_adjusted_paper_short,HedgeSizeError,NativePaperHedgeSize,
 },
 native_trade2f3_fee_aware_first_leg_v2::FirstLegFeePreviewV2,
};
use serde_json::Value;
#[derive(Debug,Clone,PartialEq,Eq)]
pub enum JointLotError{
 Json,Missing(&'static str),Mismatched(&'static str),
 Decimal(&'static str), Overflow, Sizing(HedgeSizeError),
}
fn missing(s:&'static str)->JointLotError{JointLotError::Missing(s)}
fn mismatch(s:&'static str)->JointLotError{JointLotError::Mismatched(s)}
fn val<'a>(obj:&'a Value,k:&'static str)->Result<&'a str,JointLotError>{
 obj.get(k).and_then(Value::as_str).filter(|v|!v.is_empty())
   .ok_or(missing(k))
}
fn amount(raw:&str,k:&'static str)->Result<u128,JointLotError>{
 decimal_to_atoms(raw).map_err(|_|JointLotError::Decimal(k))
}
fn positive(obj:&Value,k:&'static str)->Result<u128,JointLotError>{
 let v=amount(val(obj,k)?,k)?;
 if v==0{return Err(JointLotError::Decimal(k))}
 Ok(v)
}
fn gcd(mut a:u128,mut b:u128)->u128{
 while b>0{let r=a%b;a=b;b=r;}a
}
fn lcm(a:u128,b:u128)->Result<u128,JointLotError>{
 (a/gcd(a,b)).checked_mul(b).ok_or(JointLotError::Overflow)
}
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct JointMarketFilterProof{
 pub symbol:String,
 pub lot_step_e12:u128,
 pub market_step_e12:u128,
 pub joint_step_e12:u128,
 pub lot_min_e12:u128,
 pub market_min_e12:u128,
 pub joint_min_e12:u128,
 pub lot_max_e12:u128,
 pub market_max_e12:u128,
 pub joint_max_e12:u128,
 pub min_notional_e12:u128,
 pub strict_raw_filters_bound:bool,
 pub private_order_or_fill_attested:bool,
}
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct JointFeeAdjustedHedgeReview{
 pub filters:JointMarketFilterProof,
 pub proposal:NativePaperHedgeSize,
 pub public_metadata_consistent:bool,
 pub full_inventory_hedge_proven:bool,
 pub private_exchange_fill_attested:bool,
 pub may_send_order_or_mark_hedged:bool,
}
/// Bind strict raw LOT_SIZE and MARKET_LOT_SIZE. Conservative intersection:
/// step phases must be zero-aligned; non-zero-offset rule variants fail closed.
pub fn inspect_two_binance_market_lots(
 original_public_exchange_info:&str,
 future_symbol:&str,
)->Result<JointMarketFilterProof,JointLotError>{
 if !future_symbol.ends_with("USDT")||future_symbol.len()>80{
   return Err(mismatch("USDT-M futures symbol required"))
 }
 let root:Value=serde_json::from_str(original_public_exchange_info)
    .map_err(|_|JointLotError::Json)?;
 let rows=root.get("symbols").and_then(Value::as_array)
    .ok_or(missing("symbols"))?;
 let mut found=None;
 for row in rows{
   if row.get("symbol").and_then(Value::as_str)==Some(future_symbol){
      if found.is_some(){return Err(mismatch("duplicate exchange instrument"))}
      found=Some(row)
   }
 }
 let row=found.ok_or(missing("exact future symbol not found"))?;
 if val(row,"status")?!="TRADING"
   ||val(row,"baseAsset")?!=future_symbol.strip_suffix("USDT").unwrap_or("")
   ||val(row,"quoteAsset")?!="USDT"
   ||val(row,"contractType")?!="PERPETUAL"
   ||row.get("symbol").and_then(Value::as_str)!=Some(future_symbol){
   return Err(mismatch("invalid or inactive public futures instrument"))
 }
 let filters=row.get("filters").and_then(Value::as_array)
   .ok_or(missing("filters"))?;
 let mut lot=None;let mut market=None;let mut min_cost=None;
 for f in filters{
   match f.get("filterType").and_then(Value::as_str){
    Some("LOT_SIZE")=>{
      if lot.replace(f).is_some(){return Err(mismatch("duplicate LOT_SIZE"))}
    }
    Some("MARKET_LOT_SIZE")=>{
      if market.replace(f).is_some(){return Err(mismatch("duplicate MARKET_LOT_SIZE"))}
    }
    Some("MIN_NOTIONAL")|Some("NOTIONAL")=>{
      let src=f.get("notional").and_then(Value::as_str)
        .or_else(||f.get("minNotional").and_then(Value::as_str))
        .ok_or(missing("minNotional"))?;
      let value=amount(src,"minNotional")?;
      if value==0{return Err(JointLotError::Decimal("minNotional"))}
      if min_cost.replace(value).is_some(){
          return Err(mismatch("ambiguous duplicate minimum notional"))
      }
    }
    _=>{}
   }
 }
 let lot=lot.ok_or(missing("LOT_SIZE"))?;
 let market=market.ok_or(missing("MARKET_LOT_SIZE"))?;
 let lstep=positive(lot,"stepSize")?;
 let mstep=positive(market,"stepSize")?;
 let lmin=positive(lot,"minQty")?;
 let mmin=positive(market,"minQty")?;
 let lmax=positive(lot,"maxQty")?;
 let mmax=positive(market,"maxQty")?;
 if lmin>lmax||mmin>mmax{
    return Err(mismatch("invalid quantity range"))
 }
 // Binance step is modeled as zero-origin multiples. Reject exotic phases:
 // compatible LOT and MARKET quantity phases require explicit CRT support.
 if lmin%lstep!=0||mmin%mstep!=0{
    return Err(mismatch("quantity min not step-aligned; CRT not yet proven"))
 }
 let step=lcm(lstep,mstep)?;
 let min=lmin.max(mmin);
 let max=lmax.min(mmax);
 if max<min{return Err(mismatch("no overlapping quantity range"))}
 Ok(JointMarketFilterProof{
   symbol:future_symbol.into(),
   lot_step_e12:lstep,market_step_e12:mstep,
   joint_step_e12:step,lot_min_e12:lmin,market_min_e12:mmin,
   joint_min_e12:min,lot_max_e12:lmax,market_max_e12:mmax,
   joint_max_e12:max,
   min_notional_e12:min_cost.ok_or(missing("Binance notional filter"))?,
   strict_raw_filters_bound:true,private_order_or_fill_attested:false,
 })
}
/// Combine source-shaped fee accounting with BOTH public Binance lot filters.
/// This only returns a PAPER sizing proposal; no private order authority.
pub fn inspect_joint_fee_adjusted_paper_short(
 fee:&FirstLegFeePreviewV2,
 metadata:&NativeMarketMetadata,
 original_public_exchange_info:&str,
 public_futures_bid_e12:u128,
 public_book_received_ns:u64,
 now_monotonic_ns:u64,
 max_book_age_ns:u64,
 max_metadata_age_ns:u64,
)->Result<JointFeeAdjustedHedgeReview,JointLotError>{
 let src=inspect_two_binance_market_lots(
   original_public_exchange_info,&metadata.future_symbol)?;
 if metadata.metadata_state()!="SIZE_METADATA_READY"{
   return Err(mismatch("metadata invalid"))
 }
 // Verify the aggregate public metadata is exactly derived from these raw
 // two filters. Different snapshot generations or stale raw data must fail.
 let mstep=metadata.future_step_base.as_deref()
   .ok_or(missing("canonical futures step"))?;
 let mmin=metadata.future_min_base.as_deref()
   .ok_or(missing("canonical futures min"))?;
 let mmax=metadata.future_max_base.as_deref()
   .ok_or(missing("canonical futures max"))?;
 if amount(mstep,"canonical step")?!=src.lot_step_e12.max(src.market_step_e12)
   ||amount(mmin,"canonical min")?!=src.joint_min_e12
   ||amount(mmax,"canonical max")?!=src.joint_max_e12
   ||amount(&metadata.future_min_cost,"canonical notional")?
      !=src.min_notional_e12{
   return Err(mismatch("public snapshot does not match Binance raw filters"))
 }
 // The canonical max(stepA,stepB) might be invalid for one venue filter.
 // Re-synthesize a strictly valid zero-phase common step without touching
 // the canonical exchange universe.
 let mut constrained=metadata.clone();
 constrained.future_step_base=Some(atoms_to_string(src.joint_step_e12));
 constrained.future_min_base=Some(atoms_to_string(src.joint_min_e12));
 constrained.future_max_base=Some(atoms_to_string(src.joint_max_e12));
 constrained.future_min_cost=atoms_to_string(src.min_notional_e12);
 let proposal=plan_fee_adjusted_paper_short(
    fee,&constrained,public_futures_bid_e12,public_book_received_ns,
    now_monotonic_ns,max_book_age_ns,max_metadata_age_ns)
   .map_err(JointLotError::Sizing)?;
 let q=proposal.proposed_futures_short_atoms_e12;
 if q>0 && (q%src.lot_step_e12!=0||q%src.market_step_e12!=0||
    q<src.joint_min_e12||q>src.joint_max_e12){
   return Err(mismatch("rounded quantity violates original LOT/MARKET filters"))
 }
 if proposal.residual_unhedged_spot_atoms_e12
   .checked_add(q)!=Some(fee.original_delivered_base_e12){
    return Err(mismatch("unaccounted inventory remainder"))
 }
 Ok(JointFeeAdjustedHedgeReview{
    filters:src,proposal,public_metadata_consistent:true,
    full_inventory_hedge_proven:false,
    private_exchange_fill_attested:false,
    may_send_order_or_mark_hedged:false,
 })
}
#[cfg(test)]
mod tests{
 use super::*;
 use crate::fill_economics::decimal_to_atoms;
 fn a(v:&str)->u128{decimal_to_atoms(v).unwrap()}
 fn fee(gross:&str,base_fee:&str)->FirstLegFeePreviewV2{
   let gross=a(gross);let base_fee=a(base_fee);
   FirstLegFeePreviewV2{
    original_spot_gross_e12:gross,
    original_base_buy_fee_e12:base_fee,
    original_quote_buy_fee_e12:0,
    original_delivered_base_e12:gross-base_fee,
    quote_spent_with_quote_fees_e12:a("10"),
    base_fee_is_explicitly_source_shaped:true,
    persisted_owner_v2_seal_available:false,
    independent_live_exchange_attestation:false,
    trading_or_close_authorized:false,
   }
 }
 fn source(lstep:&str,mstep:&str,lmin:&str,mmin:&str)->String{
   format!(r#"{{"symbols":[{{"symbol":"ONEUSDT","baseAsset":"ONE",
    "quoteAsset":"USDT","contractType":"PERPETUAL","status":"TRADING",
    "filters":[
       {{"filterType":"LOT_SIZE","minQty":"{lmin}","maxQty":"100","stepSize":"{lstep}"}},
       {{"filterType":"MARKET_LOT_SIZE","minQty":"{mmin}","maxQty":"50","stepSize":"{mstep}"}},
       {{"filterType":"MIN_NOTIONAL","notional":"5"}}
    ]}}]}}"#)
 }
 fn metadata(lstep:&str,mstep:&str,lmin:&str,mmin:&str)->NativeMarketMetadata{
  let v=a(lstep).max(a(mstep));
  let m=a(lmin).max(a(mmin));
  NativeMarketMetadata{
   base:"ONE".into(),spot_symbol:"ONE-USDT".into(),
   future_symbol:"ONEUSDT".into(),
   spot_min_amount:Some("0.1".into()),
   future_min_base:Some(atoms_to_string(m)),
   spot_min_cost:"0".into(),future_min_cost:"5".into(),
   spot_step:Some("0.001".into()),
   future_step_base:Some(atoms_to_string(v)),contract_size:"1".into(),
   buy_exchange:"okx_spot".into(),sell_exchange:"binance_futures".into(),
   route_label:crate::market_metadata::ROUTE_LABEL.into(),
   static_reason:None,loaded_ns:1000,loaded_wall_ms:1_790_000_000_000,
   spot_tick:Some("0.01".into()),future_tick:Some("0.01".into()),
   spot_status:"live".into(),future_status:"TRADING".into(),
   spot_max_amount:None,future_max_base:Some("50".into()),
  }
 }
 fn calc(f:&FirstLegFeePreviewV2,m:&NativeMarketMetadata,src:&str)
 ->Result<JointFeeAdjustedHedgeReview,JointLotError>{
    inspect_joint_fee_adjusted_paper_short(f,m,src,a("1"),1001,1001,100,100)
 }
 #[test]fn both_lot_filters_use_lcm_not_greatest(){
   let src=source("0.2","0.5","0.2","0.5");
   let m=metadata("0.2","0.5","0.2","0.5");
   let r=calc(&fee("12.2","0.1"),&m,&src).unwrap();
   assert_eq!(atoms_to_string(r.filters.joint_step_e12),"1");
   assert_eq!(r.proposal.proposed_short_quantity(),"12");
   assert_eq!(r.proposal.dust_quantity(),"0.1");
   assert_eq!(r.filters.joint_max_e12,a("50"));
   assert!(!r.proposal.fully_hedged_by_proposal);
   assert!(!r.may_send_order_or_mark_hedged);
 }
 #[test]fn compatible_exact_joint_hedge_is_still_not_a_fill(){
   let src=source("0.2","0.5","0.2","0.5");
   let r=calc(&fee("12.1","0.1"),&metadata("0.2","0.5","0.2","0.5"),&src).unwrap();
   assert_eq!(r.proposal.proposed_short_quantity(),"12");
   assert_eq!(r.proposal.dust_quantity(),"0");
   assert!(r.proposal.fully_hedged_by_proposal);
   assert!(!r.full_inventory_hedge_proven);
   assert!(!r.private_exchange_fill_attested);
 }
 #[test]fn incommensurate_decimal_steps_need_integer_lcm(){
   let src=source("0.03","0.05","0.03","0.05");
   let m=metadata("0.03","0.05","0.03","0.05");
   let r=calc(&fee("9.2","0"),&m,&src).unwrap();
   assert_eq!(atoms_to_string(r.filters.joint_step_e12),"0.15");
   assert_eq!(r.proposal.proposed_short_quantity(),"9.15");
   assert_eq!(r.proposal.dust_quantity(),"0.05");
 }
 #[test]fn under_notional_keeps_entire_base_unhedged(){
   let src=source("0.2","0.5","0.2","0.5");
   let r=inspect_joint_fee_adjusted_paper_short(&fee("10","0"),
    &metadata("0.2","0.5","0.2","0.5"),&src,
    a("0.1"),1001,1001,100,100).unwrap();
   assert_eq!(r.proposal.proposed_short_quantity(),"0");
   assert_eq!(r.proposal.dust_quantity(),"10");
 }
 #[test]fn raw_filter_absent_blocks(){
   let src=source("0.2","0.5","0.2","0.5")
      .replace("MARKET_LOT_SIZE","OTHER_FILTER");
   assert!(matches!(calc(&fee("12","0"),
      &metadata("0.2","0.5","0.2","0.5"),&src),
      Err(JointLotError::Missing("MARKET_LOT_SIZE"))));
 }
 #[test]fn public_metadata_not_same_as_raw_filters_rejected(){
   let src=source("0.2","0.5","0.2","0.5");
   let mut m=metadata("0.2","0.5","0.2","0.5");
   m.future_step_base=Some("0.2".into());
   assert!(matches!(calc(&fee("12","0"),&m,&src),Err(JointLotError::Mismatched(_))));
 }
 #[test]fn zero_step_blocked(){
   let src=source("0.2","0","0.2","0.5");
   assert!(matches!(calc(&fee("12","0"),
      &metadata("0.2","0.5","0.2","0.5"),&src),Err(JointLotError::Decimal("stepSize"))));
 }
 #[test]fn unsupported_offset_minimum_blocks_without_crt(){
   let src=source("0.2","0.5","0.3","0.5");
   assert!(matches!(calc(&fee("12","0"),
      &metadata("0.2","0.5","0.3","0.5"),&src),Err(JointLotError::Mismatched(_))));
 }
 #[test]fn duplicate_lot_filter_blocks(){
   let src=source("0.2","0.5","0.2","0.5");
   let src=src.replace("\"filterType\":\"MARKET_LOT_SIZE\"",
    "\"filterType\":\"LOT_SIZE\"");
   assert!(matches!(calc(&fee("12","0"),
      &metadata("0.2","0.5","0.2","0.5"),&src),Err(JointLotError::Mismatched(_))));
 }
 #[test]fn inactive_contract_blocks(){
   let src=source("0.2","0.5","0.2","0.5").replace(
      "\"status\":\"TRADING\"","\"status\":\"SETTLING\"");
   assert!(matches!(calc(&fee("12","0"),
      &metadata("0.2","0.5","0.2","0.5"),&src),Err(JointLotError::Mismatched(_))));
 }
 #[test]fn invalid_or_missing_notional_filter_blocks(){
   let src=source("0.2","0.5","0.2","0.5").replace(
      "\"filterType\":\"MIN_NOTIONAL\"","\"filterType\":\"OTHER\"");
   assert!(matches!(calc(&fee("12","0"),
      &metadata("0.2","0.5","0.2","0.5"),&src),Err(JointLotError::Missing(_))));
 }
 #[test]fn stale_price_propagates(){
   let src=source("0.2","0.5","0.2","0.5");
   let e=inspect_joint_fee_adjusted_paper_short(&fee("12","0"),
      &metadata("0.2","0.5","0.2","0.5"),&src,a("1"),1001,1200,100,500);
   assert!(matches!(e,Err(JointLotError::Sizing(HedgeSizeError::StaleQuote))));
 }
 #[test]fn stale_metadata_propagates(){
   let src=source("0.2","0.5","0.2","0.5");
   let e=inspect_joint_fee_adjusted_paper_short(&fee("12","0"),
      &metadata("0.2","0.5","0.2","0.5"),&src,a("1"),1200,1200,500,100);
   assert!(matches!(e,Err(JointLotError::Sizing(HedgeSizeError::StaleMetadata))));
 }
 #[test]fn base_fee_does_not_allow_rounding_up(){
   let src=source("0.2","0.5","0.2","0.5");
   let r=calc(&fee("6.01","0.02"),&metadata("0.2","0.5","0.2","0.5"),&src).unwrap();
   assert_eq!(r.proposal.proposed_short_quantity(),"5");
   assert_eq!(r.proposal.dust_quantity(),"0.99");
   assert!(!r.proposal.fully_hedged_by_proposal);
 }
 #[test]fn false_raw_base_asset_blocks(){
    let src=source("0.2","0.5","0.2","0.5")
      .replace("\"baseAsset\":\"ONE\"","\"baseAsset\":\"OTHER\"");
    assert!(matches!(calc(&fee("12","0"),
      &metadata("0.2","0.5","0.2","0.5"),&src),Err(JointLotError::Mismatched(_))));
 }
 #[test]fn inverted_market_qty_range_blocks(){
    let src=source("0.2","0.5","0.2","0.5")
      .replace("\"maxQty\":\"50\"","\"maxQty\":\"0.1\"");
    assert!(matches!(calc(&fee("12","0"),
      &metadata("0.2","0.5","0.2","0.5"),&src),Err(JointLotError::Mismatched(_))));
 }
 #[test]fn bad_source_type_blocks(){
   let src="{}";
   assert!(matches!(calc(&fee("12","0"),
      &metadata("0.2","0.5","0.2","0.5"),src),Err(JointLotError::Missing(_))));
 }
}

/// Owner-bound READ-ONLY feeCcy proof from individually identified OKX fills.
/// This cannot write a hedge into PaperOwner; its legacy fee gate intentionally
/// lacks an authoritative BASE fee seal. Durable matching is local-only.
pub fn inspect_owner_bound_joint_paper_hedge(
 owner:&crate::native_paper_owner_transaction_review::PaperOwner,
 base:&str,
 fee:&FirstLegFeePreviewV2,
 original_okx_fills:&[crate::native_trade2f_asset_fee::SpotFeeFill],
 metadata:&NativeMarketMetadata,
 original_public_exchange_info:&str,
 public_futures_bid_e12:u128,
 public_book_received_ns:u64,
 now_monotonic_ns:u64,
 max_book_age_ns:u64,
 max_metadata_age_ns:u64,
)->Result<JointFeeAdjustedHedgeReview,JointLotError>{
 use crate::native_paper_market_execution_review::PaperPhase;
 if owner.frozen||owner.last_seq==0||
    base!=metadata.base||base.is_empty(){
    return Err(mismatch("owner frozen or base mismatch"))
 }
 let machine=owner.portfolio.positions.get(base)
   .ok_or(missing("active paper position missing"))?;
 if !matches!(machine.phase,PaperPhase::AwaitingHedge(_)){
   return Err(mismatch("owner must await initial hedge"))
 }
 let gate=owner.gate_by_base.get(base)
    .ok_or(missing("owned first-leg ledger missing"))?;
 let exposure=gate.exposure()
   .map_err(|_|mismatch("first leg ledger invalid"))?;
 let ledger=&gate.spot;
 if exposure.unknown_or_quarantine||!exposure.spot_fee_complete||!ledger.terminal||
    !ledger.cumulative_truth_present||
    !gate.shorts.is_empty()||
    ledger.venue!="okx"||ledger.side!="buy"||
    ledger.symbol.replace('-',"")!=metadata.future_symbol||
    ledger.exchange_order_id.is_none()||
    ledger.filled_qty_atoms()!=fee.original_spot_gross_e12{
    return Err(mismatch("pending/unknown/foreign first-leg ownership"))
 }
 // Only original durable owner ledger can qualify: no imaginary
 // fill tapes supplied to the sizing function without a journal match.
 let recovery=crate::native_paper_durable_journal_review::read_journal(&owner.journal)
    .map_err(|_|mismatch("local durable owner journal not readable"))?
    .ok_or(missing("local durable owner checkpoint"))?;
 let disk_row=recovery.state.positions.get(base)
    .ok_or(missing("durable position missing"))?;
 if recovery.sequence!=owner.last_seq
    ||disk_row.phase!="AWAITING_HEDGE"
    ||disk_row.fills.spot!=*ledger
    ||disk_row.fills.native_spot_fee!=gate.native_spot_fee
    ||disk_row.fills.shorts!=gate.shorts
    ||disk_row.fills.unknown!=gate.unknown
    ||disk_row.fills.generation!=gate.generation
    ||disk_row.fills.owner_id!=gate.owner_id{

     return Err(mismatch("paper memory vs durable fill owner changed"))
 }
 if original_okx_fills.len()!=ledger.fills.len()
    ||original_okx_fills.is_empty()||original_okx_fills.len()>64{
    return Err(mismatch("source rows do not cover original fills"))
 }
 let order_id=ledger.exchange_order_id.as_deref().unwrap();
 let mut seen=std::collections::BTreeSet::new();
 let mut base_fees=0u128;let mut quote_fees=0u128;
 let mut spent=0u128;
 for row in original_okx_fills{
    if row.order_id!=order_id||row.inst_id!=metadata.spot_symbol||
       row.side!="buy"||row.at_ms==0||!seen.insert(&row.trade_id)||
       row.signed_fee_e12>0{
       return Err(mismatch("OKX source identity/rebate not allowed"))
    }
    let fill=ledger.fills.get(&row.trade_id)
       .ok_or(missing("trade ID absent from durable fill ledger"))?;
    if fill.qty_atoms!=row.gross_base_qty_e12
       ||fill.price_atoms!=row.price_quote_e12
       ||fill.trade_time_ms!=Some(row.at_ms)
       ||fill.fee_currency.as_deref()!=Some(row.fee_ccy.as_str()){
       return Err(mismatch("OKX fee record differs from durable fill"))
    }
    let fee_atoms=u128::try_from(row.signed_fee_e12.checked_neg()
       .ok_or(JointLotError::Overflow)?).map_err(|_|JointLotError::Overflow)?;
    match row.fee_ccy.as_str(){
       c if c==base=>{
          if fill.fee_cost_quote_atoms.is_some(){
              return Err(mismatch("BASE fee disguised as quote fee"))
          }
          base_fees=base_fees.checked_add(fee_atoms)
             .ok_or(JointLotError::Overflow)?;
       }
       "USDT"=>{
          if fill.fee_cost_quote_atoms!=Some(fee_atoms as i128){
             return Err(mismatch("USDT fee amount does not match trade fill"))
          }
          quote_fees=quote_fees.checked_add(fee_atoms)
             .ok_or(JointLotError::Overflow)?;
       }
       _=>return Err(mismatch("unsupported third-asset OKX fee")),
    }
    let q=fill.qty_atoms.checked_mul(fill.price_atoms)
       .ok_or(JointLotError::Overflow)?;
    let notional=q.checked_add(crate::fill_economics::SCALE_12-1)
       .ok_or(JointLotError::Overflow)?
       /crate::fill_economics::SCALE_12;
    spent=spent.checked_add(notional)
       .and_then(|s|s.checked_add(if row.fee_ccy=="USDT"{fee_atoms}else{0}))
       .ok_or(JointLotError::Overflow)?;
 }
 let mut source_native:Vec<_>=original_okx_fills.iter().filter(|r|r.fee_ccy==base).cloned().collect();
 let mut sealed_native=gate.native_spot_fee.as_ref().map(|v|v.rows.clone()).unwrap_or_default();
 source_native.sort_by(|a,b|a.trade_id.cmp(&b.trade_id));
 sealed_native.sort_by(|a,b|a.trade_id.cmp(&b.trade_id));
 if source_native!=sealed_native{
   return Err(mismatch("OKX BASE fee source rows not sealed in durable owner"))
 }
 if base_fees!=exposure.spot_base_fee_atoms
    ||fee.original_delivered_base_e12!=exposure.spot_net_delivered_atoms{
    return Err(mismatch("durable fee/quantity mismatch"))
 }
 if base_fees!=fee.original_base_buy_fee_e12||
    quote_fees!=fee.original_quote_buy_fee_e12||
    spent!=fee.quote_spent_with_quote_fees_e12{
    return Err(mismatch("feeCcy totals do not match owner source preview"))
 }
 inspect_joint_fee_adjusted_paper_short(
   fee,metadata,original_public_exchange_info,public_futures_bid_e12,
   public_book_received_ns,now_monotonic_ns,max_book_age_ns,max_metadata_age_ns)
}
#[cfg(test)]
mod owner_joint_tests{
 use super::*;
 use crate::{
  fill_economics::{FillEvent,decimal_to_atoms},
  native_paper_market_execution_review::{PaperPhase,PaperPosition,PaperMachine,PaperAssumptions},
  native_paper_owner_transaction_review::PaperOwner,
  native_paper_partial_recovery_gate::PaperPartialGate,
  native_trade2f_asset_fee::SpotFeeFill,
 };
 use std::time::{SystemTime,UNIX_EPOCH};
 fn a(s:&str)->u128{decimal_to_atoms(s).unwrap()}
 fn fixture()->(PaperOwner,Vec<SpotFeeFill>,FirstLegFeePreviewV2,NativeMarketMetadata,String){
    let ns=SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let path=std::env::temp_dir()
      .join(format!("spotarb-paper-fee-owner-{ns}")).join("journal.jsonl");
    let mut owner=PaperOwner::start_new(&path,1_791_786_000_000).unwrap();
    owner.transact(|p,g|{
      p.stats.entries=1;
      p.positions.insert("ONE".into(),PaperMachine{
       config:PaperAssumptions::default(),
       phase:PaperPhase::AwaitingHedge(PaperPosition{
         base:"ONE".into(),signal_ns:1,spot_buy_ns:1,
         signal_estimated_net_bps:50.0,spot_gross_base:10.0,
         spot_delivered_base:9.0,spot_paid_quote:10.0,
         futures_short_ns:None,futures_short_proceeds_quote:None,
         futures_short_base:None,hedge_entry_estimated_net_bps:None,
       }),
      });
      let mut gate=PaperPartialGate::new(1,"owner_ONE","ONEUSDT","intent1",
         "client1",a("10")).unwrap();
      gate.spot.apply_cumulative_truth(
         a("10"),Some(a("1")),true,
         Some("filled"),Some("1001"),Some(1001),1).unwrap();
      gate.spot.apply_fill(FillEvent{
         fill_id:"trade1".into(),price_atoms:a("1"),qty_atoms:a("10"),
         fee_cost_quote_atoms:None,fee_currency:Some("ONE".into()),
         trade_time_ms:Some(1001),
      },1).unwrap();
      gate.record_native_spot_fee(vec![SpotFeeFill{
        order_id:"1001".into(),trade_id:"trade1".into(),
        at_ms:1001,side:"buy".into(),inst_id:"ONE-USDT".into(),
        price_quote_e12:a("1"),gross_base_qty_e12:a("10"),
        fee_ccy:"ONE".into(),signed_fee_e12:-(a("1") as i128),
      }]).unwrap();
      g.insert("ONE".into(),gate);
      Ok(())
    }).unwrap();
    let fees=vec![SpotFeeFill{
       order_id:"1001".into(),trade_id:"trade1".into(),
       at_ms:1001,side:"buy".into(),inst_id:"ONE-USDT".into(),
       price_quote_e12:a("1"),gross_base_qty_e12:a("10"),
       fee_ccy:"ONE".into(),signed_fee_e12:-(a("1") as i128),
    }];
    let fee=FirstLegFeePreviewV2{
       original_spot_gross_e12:a("10"),
       original_base_buy_fee_e12:a("1"),
       original_quote_buy_fee_e12:0,
       original_delivered_base_e12:a("9"),
       quote_spent_with_quote_fees_e12:a("10"),
       base_fee_is_explicitly_source_shaped:true,
       persisted_owner_v2_seal_available:false,
       independent_live_exchange_attestation:false,
       trading_or_close_authorized:false,
    };
    let metadata=NativeMarketMetadata{
       base:"ONE".into(),spot_symbol:"ONE-USDT".into(),future_symbol:"ONEUSDT".into(),
       spot_min_amount:Some("1".into()),
       future_min_base:Some("0.5".into()),
       spot_min_cost:"0".into(),future_min_cost:"5".into(),
       spot_step:Some("0.1".into()),
       future_step_base:Some("0.5".into()),contract_size:"1".into(),
       buy_exchange:"okx_spot".into(),sell_exchange:"binance_futures".into(),
       route_label:crate::market_metadata::ROUTE_LABEL.into(),
       static_reason:None,loaded_ns:1000,loaded_wall_ms:1_790_000_000_000,
       spot_tick:Some("0.00001".into()),future_tick:Some("0.00001".into()),
       spot_status:"live".into(),future_status:"TRADING".into(),
       spot_max_amount:None,future_max_base:Some("50".into()),
    };
    let raw=r#"{"symbols":[{"symbol":"ONEUSDT","baseAsset":"ONE",
       "quoteAsset":"USDT","contractType":"PERPETUAL","status":"TRADING",
       "filters":[{"filterType":"LOT_SIZE","minQty":"0.2","maxQty":"100","stepSize":"0.2"},
          {"filterType":"MARKET_LOT_SIZE","minQty":"0.5","maxQty":"50","stepSize":"0.5"},
          {"filterType":"MIN_NOTIONAL","notional":"5"}]}]}"#.to_owned();
    (owner,fees,fee,metadata,raw)
 }
 fn bound(owner:&PaperOwner,fees:&[SpotFeeFill],fee:&FirstLegFeePreviewV2,
    meta:&NativeMarketMetadata,raw:&str)->Result<JointFeeAdjustedHedgeReview,JointLotError>{
   inspect_owner_bound_joint_paper_hedge(owner,"ONE",fee,fees,meta,raw,a("1"),
      1001,1001,100,100)
 }
 #[test]fn owner_durable_base_fee_joint_lots_read_only(){
   let (owner,fees,fee,meta,raw)=fixture();
   let review=bound(&owner,&fees,&fee,&meta,&raw).unwrap();
   assert_eq!(review.proposal.proposed_short_quantity(),"9");
   assert_eq!(review.proposal.dust_quantity(),"0");
   assert!(!review.full_inventory_hedge_proven);
   assert!(!review.may_send_order_or_mark_hedged);
   assert_eq!(owner.last_seq,2);
   let _=std::fs::remove_dir_all(owner.journal.parent().unwrap());
 }
 #[test]fn owner_refuses_foreign_trade_id(){
   let (owner,mut fees,fee,meta,raw)=fixture();
   fees[0].trade_id="forged".into();
   assert!(matches!(bound(&owner,&fees,&fee,&meta,&raw),
       Err(JointLotError::Missing(_))));
   let _=std::fs::remove_dir_all(owner.journal.parent().unwrap());
 }
 #[test]fn owner_refuses_forged_native_fee_total(){
   let (owner,fees,mut fee,meta,raw)=fixture();
   fee.original_base_buy_fee_e12=a("0.5");
   assert!(matches!(bound(&owner,&fees,&fee,&meta,&raw),
       Err(JointLotError::Mismatched(_))));
   let _=std::fs::remove_dir_all(owner.journal.parent().unwrap());
 }
 #[test]fn owner_refuses_uncommitted_fill_mutation(){
   let (mut owner,fees,fee,meta,raw)=fixture();
   owner.gate_by_base.get_mut("ONE").unwrap().spot
      .exchange_order_id=Some("forged".into());
   assert!(matches!(bound(&owner,&fees,&fee,&meta,&raw),
       Err(JointLotError::Mismatched(_))));
   let _=std::fs::remove_dir_all(owner.journal.parent().unwrap());
 }
 #[test]fn owner_refuses_unknown_after_timeout(){
   let (mut owner,fees,fee,meta,raw)=fixture();
   owner.gate_by_base.get_mut("ONE").unwrap().unknown=Some(
     crate::native_paper_partial_recovery_gate::PaperUnknown{
       intent_id:"intent1".into(),client_order_id:"client1".into(),
       explanation:"unknown response".into(),
     });
   assert!(matches!(bound(&owner,&fees,&fee,&meta,&raw),
       Err(JointLotError::Mismatched(_))));
   let _=std::fs::remove_dir_all(owner.journal.parent().unwrap());
 }
 #[test]fn owner_refuses_third_asset_fee_claim(){
   let (owner,mut fees,fee,meta,raw)=fixture();
   fees[0].fee_ccy="BNB".into();
   assert!(matches!(bound(&owner,&fees,&fee,&meta,&raw),
       Err(JointLotError::Mismatched(_))));
   let _=std::fs::remove_dir_all(owner.journal.parent().unwrap());
 }
 #[test]fn prepared_paper_short_fsyncs_exact_native_fee_qty_before_publish(){
   let (mut owner,fees,fee,meta,raw)=fixture();
   let outcome=prepare_durable_paper_hedge(&mut owner,"ONE",&fee,&fees,
      &meta,&raw,a("1"),1001,1001,100,100,"fut1","fut_client1").unwrap();
   assert_eq!(outcome.committed_seq,3);
   assert_eq!(outcome.proposed_short_atoms_e12,a("9"));
   assert_eq!(outcome.residual_base_atoms_e12,0);
   assert!(!outcome.may_send_exchange_order);
   assert!(!outcome.may_mark_hedged);
   assert_eq!(owner.gate_by_base["ONE"].shorts.len(),1);
   assert_eq!(owner.gate_by_base["ONE"].shorts[0].requested_qty_atoms,a("9"));
   assert_eq!(owner.gate_by_base["ONE"].exposure().unwrap().futures_filled_atoms,0);
   let rec=PaperOwner::inspect_recovery(&owner.journal).unwrap().unwrap();
   assert_eq!(rec.sequence,3);
   assert_eq!(rec.state.positions["ONE"].fills.shorts[0].requested_qty_atoms,a("9"));
   assert!(rec.state.positions["ONE"].fills.restart_quarantine);
   assert!(!rec.may_resume_paper && !rec.may_trade_real);
   let _=std::fs::remove_dir_all(owner.journal.parent().unwrap());
 }
 #[test]fn repeated_paper_preparation_never_creates_second_short(){
   let (mut owner,fees,fee,meta,raw)=fixture();
   prepare_durable_paper_hedge(&mut owner,"ONE",&fee,&fees,&meta,&raw,
      a("1"),1001,1001,100,100,"fut1","fut_client1").unwrap();
   assert!(prepare_durable_paper_hedge(&mut owner,"ONE",&fee,&fees,&meta,&raw,
      a("1"),1001,1001,100,100,"fut2","fut_client2").is_err());
   assert_eq!(owner.last_seq,3);
   assert_eq!(owner.gate_by_base["ONE"].shorts.len(),1);
   let _=std::fs::remove_dir_all(owner.journal.parent().unwrap());
 }
 #[test]fn stale_owner_after_external_commit_cannot_prepare(){
   let (mut owner,fees,fee,meta,raw)=fixture();
   let mut stale=PaperOwner{portfolio:owner.portfolio.clone(),
       gate_by_base:owner.gate_by_base.clone(),journal:owner.journal.clone(),
       last_seq:owner.last_seq,frozen:false};
   owner.transact(|_,_|Ok(())).unwrap();
   assert_eq!(owner.last_seq,3);
   assert!(prepare_durable_paper_hedge(&mut stale,"ONE",&fee,&fees,&meta,&raw,
      a("1"),1001,1001,100,100,"fut1","fut_client1").is_err());
   assert_eq!(stale.last_seq,2);
   assert!(stale.gate_by_base["ONE"].shorts.is_empty());
   assert_eq!(PaperOwner::inspect_recovery(&owner.journal).unwrap().unwrap().sequence,3);
   let _=std::fs::remove_dir_all(owner.journal.parent().unwrap());
 }
 #[test]fn unsupported_fee_or_no_min_notional_must_not_write_intent(){
   let (mut owner,mut fees,fee,meta,raw)=fixture();
   fees[0].signed_fee_e12=0;
   assert!(prepare_durable_paper_hedge(&mut owner,"ONE",&fee,&fees,
      &meta,&raw,a("1"),1001,1001,100,100,"fut1","fut_client1").is_err());
   assert_eq!(owner.last_seq,2);
   fees[0].signed_fee_e12=-(a("1") as i128);
   assert!(prepare_durable_paper_hedge(&mut owner,"ONE",&fee,&fees,
      &meta,&raw,a("0.01"),1001,1001,100,100,"fut1","fut_client1").is_err());
   assert_eq!(owner.last_seq,2);
   assert!(owner.gate_by_base["ONE"].shorts.is_empty());
   let _=std::fs::remove_dir_all(owner.journal.parent().unwrap());
 }
 #[test]fn prepared_short_partial_fill_and_restart_stays_unhedged(){
   use crate::native_paper_partial_recovery_gate::PaperOrderReport;
   let (mut owner,fees,fee,meta,raw)=fixture();
   prepare_durable_paper_hedge(&mut owner,"ONE",&fee,&fees,&meta,&raw,
      a("1"),1001,1001,100,100,"fut1","fut_client1").unwrap();
   owner.transact(|_,g|{
     let report=PaperOrderReport{
       intent_id:"fut1".into(),client_order_id:"fut_client1".into(),
       exchange_order_id:"futures100".into(),
       cumulative_qty_atoms:a("5"),average_price_atoms:Some(a("1")),
       terminal:false,status:"partially_filled".into(),observed_at_ms:1002,
       fills:vec![FillEvent{fill_id:"futtrade1".into(),qty_atoms:a("5"),
         price_atoms:a("1"),fee_cost_quote_atoms:Some(0),
         fee_currency:Some("USDT".into()),trade_time_ms:Some(1002)}],
     };
     g.get_mut("ONE").unwrap().observe_short(&report)
       .map_err(|_|crate::native_paper_owner_transaction_review::OwnerError::Invalid("mock partial"))?;
     Ok(())
   }).unwrap();
   let recovered=PaperOwner::inspect_recovery(&owner.journal).unwrap().unwrap();
   assert_eq!(recovered.sequence,4);
   let status=recovered.state.positions["ONE"].fills.exposure().unwrap();
   assert_eq!(status.spot_filled_atoms,a("10"));
   assert_eq!(status.spot_base_fee_atoms,a("1"));
   assert_eq!(status.futures_filled_atoms,a("5"));
   assert_eq!(status.unhedged_base_atoms,a("4"));
   assert!(status.unknown_or_quarantine);
   assert!(!recovered.may_resume_paper);
   assert!(prepare_durable_paper_hedge(&mut owner,"ONE",&fee,&fees,&meta,&raw,
      a("1"),1001,1001,100,100,"fut2","fut_client2").is_err());
   assert_eq!(owner.last_seq,4);
   let _=std::fs::remove_dir_all(owner.journal.parent().unwrap());
 }
}
/// PAPER-only two-phase preparation. Proves the proposal against sealed OKX
/// fill/fee rows, Binance raw LOT/MARKET filters and an owner CAS journal.
/// Never sends an order; caller must separately obtain fresh execution truth.
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct DurablePaperHedgePreparation{
 pub committed_seq:u64,
 pub proposed_short_atoms_e12:u128,
 pub residual_base_atoms_e12:u128,
 pub may_send_exchange_order:bool,
 pub may_mark_hedged:bool,
}
pub fn prepare_durable_paper_hedge(
 owner:&mut crate::native_paper_owner_transaction_review::PaperOwner,
 base:&str,
 fee:&FirstLegFeePreviewV2,
 original_okx_fills:&[crate::native_trade2f_asset_fee::SpotFeeFill],
 metadata:&NativeMarketMetadata,
 raw_public_exchange_info:&str,
 futures_bid_e12:u128,
 book_ns:u64,now_ns:u64,max_book_age_ns:u64,max_meta_age_ns:u64,
 paper_intent_id:&str,paper_client_id:&str,
)->Result<DurablePaperHedgePreparation,JointLotError>{
 let inspected=inspect_owner_bound_joint_paper_hedge(
   owner,base,fee,original_okx_fills,metadata,raw_public_exchange_info,
   futures_bid_e12,book_ns,now_ns,max_book_age_ns,max_meta_age_ns)?;
 let qty=inspected.proposal.proposed_futures_short_atoms_e12;
 if qty==0||inspected.proposal.residual_unhedged_spot_atoms_e12
    .checked_add(qty)!=Some(fee.original_delivered_base_e12){
     return Err(mismatch("invalid or blocked paper hedge amount"))
 }
 // Sizing was read-only. This CAS append atomically seals the exact proposed
 // short intent without publishing anything until file+dir durability returns.
 let seq=owner.transact(|_,gates|{
    let gate=gates.get_mut(base)
       .ok_or(crate::native_paper_owner_transaction_review::OwnerError::Invalid("no gate"))?;
    gate.begin_paper_short(paper_intent_id,paper_client_id,qty)
       .map_err(|_|crate::native_paper_owner_transaction_review::OwnerError::Invalid("paper short stage denied"))?;
    Ok(())
 }).map_err(|_|mismatch("paper owner CAS/durable or stage denial"))?;
 Ok(DurablePaperHedgePreparation{
   committed_seq:seq,proposed_short_atoms_e12:qty,
   residual_base_atoms_e12:inspected.proposal.residual_unhedged_spot_atoms_e12,
   may_send_exchange_order:false,may_mark_hedged:false,
 })
}
