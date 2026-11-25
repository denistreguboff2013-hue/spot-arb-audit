//! Astra P1-02/P1-04: offline V2 C2 source/bill ancestry review.
 //! One supplied OKX BASE account bill per original BUY fill; never invent a
 //! second QUOTE bill. This is not authenticated GET or durable owner proof.
 use std::collections::{BTreeMap,BTreeSet};
 use serde::{Serialize,Deserialize};
 use crate::{
     native_trade2f3_first_leg_v2_checkpoint::FirstLegV2CheckpointPayload,
     native_trade2f3_v2_net_hedge_contract::{
         inspect_v2_net_hedge_proposal,V2NetHedgeError,
     },
 };
 #[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
 #[serde(deny_unknown_fields)]
 pub struct SourceShapedOkxBaseBillV2 {
     pub bill_id:String,
     pub trade_id:String,
     pub order_id:String,
     pub inst_id:String,
     pub ccy:String,
     pub sub_type:String,
     pub event_at_ms:u64,
     pub gross_base_qty_e12:u128,
     pub fee_ccy:String,
     pub signed_fee_e12:i128,
     pub balance_change_base_e12:i128,
     pub balance_after_base_e12:u128,
 }
 #[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
 #[serde(deny_unknown_fields)]
 pub struct BoundedSourceBillWindowV2 {
     pub query_from_ms:u64,
     pub query_through_ms:u64,
     pub captured_at_ms:u64,
     pub page_count:u16,
     pub empty_terminal_page_observed:bool,
     /// Supplied descending by billId and timestamp, including *all* BASE
     /// currency activity in this synthetic bounded window, not filtered by order.
     pub source_shaped_base_bills_desc:Vec<SourceShapedOkxBaseBillV2>,
 }
 #[derive(Clone,Debug,PartialEq,Eq)]
 pub enum BillAncestryV2Error {
     Incomplete(&'static str),Mismatch(&'static str),Overflow,Net(V2NetHedgeError),
 }
 #[derive(Clone,Debug,PartialEq,Eq)]
 pub struct V2BillAncestryOfflineReview {
     pub generation:u64,
     pub original_order_id:String,
     pub confirmed_source_bill_ids:Vec<String>,
     pub delivered_base_e12:u128,
     pub proposed_futures_short_e12:u128,
     pub source_window_locally_consistent:bool,
     pub independent_exchange_bills_authenticated:bool,
     pub account_global_history_proven:bool,
     pub persisted_owner_v2_seal_proven:bool,
     pub actual_futures_filled_proven:bool,
     pub may_send_or_verify_closed:bool,
 }
 fn mismatch(s:&'static str)->BillAncestryV2Error{BillAncestryV2Error::Mismatch(s)}
 fn missing(s:&'static str)->BillAncestryV2Error{BillAncestryV2Error::Incomplete(s)}
 const MICRO:u128=1_000_000;
 /// Verify supplied raw-like BASE account rows and original FILL feeCcy:
 /// balance-after(next) - own delta = prior balance, in reverse chronology.
 /// SOURCE-SHAPED alone can never certify remote authenticity or full pagination.
 pub fn inspect_v2_supplied_bill_ancestry(
     checkpoint:&FirstLegV2CheckpointPayload,
     expected_generation:u64,
     window:&BoundedSourceBillWindowV2,
     proposed_futures_short_e12:u128,
 )->Result<V2BillAncestryOfflineReview,BillAncestryV2Error>{
     let preview=inspect_v2_net_hedge_proposal(
         checkpoint,expected_generation,"v2-futures-intent","v2-futures-client",
         proposed_futures_short_e12,
     ).map_err(BillAncestryV2Error::Net)?;
     let original=&checkpoint.original;
     let symbol=checkpoint.plan.spot_symbol.replace('/',"-");
     let base=&checkpoint.plan.base;
     if window.query_from_ms!=checkpoint.plan.created_at_ms
         ||window.query_through_ms!=original.balance_updated_at_ms
         ||window.query_from_ms>=window.query_through_ms
         ||window.captured_at_ms<original.observed_at_ms
         ||window.captured_at_ms<window.query_through_ms
         ||window.captured_at_ms.saturating_sub(original.observed_at_ms)>60_000
         ||!window.empty_terminal_page_observed
         ||window.page_count==0||window.page_count>16{
         return Err(missing("bounded source window, freshness or terminal empty page absent"))
     }
     let source=&checkpoint.source_shaped_fee_fills;
     let records=&window.source_shaped_base_bills_desc;
     if records.is_empty()||records.len()!=source.len()
         ||records.len()!=original.fill_bill_ids.len()
         ||records.len()>64{
         return Err(missing("raw base bills must cover each original fill exactly once"))
     }
     let fee_by_trade: BTreeMap<_,_>=source.iter().map(|s|(s.trade_id.as_str(),s)).collect();
     if fee_by_trade.len()!=source.len(){
         return Err(mismatch("duplicate source trade identities"))
     }
     let expected_bill_ids:BTreeSet<_>=original.fill_bill_ids.iter().map(String::as_str).collect();
     if expected_bill_ids.len()!=records.len(){
         return Err(mismatch("duplicate or absent original bill IDs"))
     }
     let baseline=checkpoint.plan.spot_baseline_atoms_e18;
     let observed=original.observed_base_cash_atoms_e18;
     if baseline%MICRO!=0||observed%MICRO!=0 {
         return Err(mismatch("unrepresentable original base cash precision"))
     }
     let mut rolling=observed/MICRO;
     let mut used_bill_ids=BTreeSet::new();
     let mut used_trade_ids=BTreeSet::new();
     let mut previous_bill:Option<u128>=None;
     let mut previous_ts:Option<u64>=None;
     for bill in records{
         let id=bill.bill_id.parse::<u128>().ok().filter(|v|*v>0)
             .ok_or(mismatch("bill ID must be canonical positive decimal"))?;
         if bill.bill_id!=id.to_string()
             ||previous_bill.is_some_and(|old|id>=old)
             ||previous_ts.is_some_and(|old|bill.event_at_ms>old)
             ||!expected_bill_ids.contains(bill.bill_id.as_str())
             ||!used_bill_ids.insert(bill.bill_id.as_str())
             ||!used_trade_ids.insert(bill.trade_id.as_str()){
             return Err(mismatch("duplicate, foreign or unordered original bill"))
         }
         previous_bill=Some(id);
         previous_ts=Some(bill.event_at_ms);
         let fee=fee_by_trade.get(bill.trade_id.as_str())
             .ok_or(missing("source bill trade ID missing from original fill"))?;
         let origin_fill=original.exact_ledger.fills.get(&bill.trade_id)
             .ok_or(missing("original immutable ledger fill missing"))?;
         if bill.order_id!=original.spot_order_id
             ||bill.inst_id!=symbol
             ||bill.ccy!=*base
             ||bill.sub_type!="1"
             ||bill.event_at_ms<window.query_from_ms
             ||bill.event_at_ms>window.query_through_ms
             ||bill.event_at_ms<fee.at_ms
             ||bill.event_at_ms.saturating_sub(fee.at_ms)>10_000
             ||bill.gross_base_qty_e12!=fee.gross_base_qty_e12
             ||bill.gross_base_qty_e12!=origin_fill.qty_atoms
             ||bill.fee_ccy!=fee.fee_ccy
             ||bill.signed_fee_e12!=fee.signed_fee_e12{
             return Err(mismatch("raw base bill differs from original source fill/feeCcy"))
         }
         let fee_native_base=if bill.fee_ccy==*base{
             bill.signed_fee_e12
         }else if bill.fee_ccy=="USDT"{0}else{
             return Err(mismatch("unsupported third asset fee"))
         };
         let delta=i128::try_from(bill.gross_base_qty_e12)
             .map_err(|_|BillAncestryV2Error::Overflow)?
             .checked_add(fee_native_base)
             .ok_or(BillAncestryV2Error::Overflow)?;
         if delta<=0||bill.balance_change_base_e12!=delta
             ||bill.balance_after_base_e12!=rolling{
             return Err(mismatch("bill balance delta/reverse-running balance mismatch"))
         }
         rolling=rolling.checked_sub(u128::try_from(delta)
             .map_err(|_|BillAncestryV2Error::Overflow)?)
             .ok_or(mismatch("negative baseline during reverse bill walk"))?;
     }
     if rolling!=baseline/MICRO||used_trade_ids.len()!=source.len()
         ||used_bill_ids!=expected_bill_ids {
         return Err(mismatch("unexplained base balance movement or missing bill"))
     }
     Ok(V2BillAncestryOfflineReview {
         generation:expected_generation,
         original_order_id:original.spot_order_id.clone(),
         confirmed_source_bill_ids:records.iter().map(|x|x.bill_id.clone()).collect(),
         delivered_base_e12:preview.spot_delivered_net_atoms_e12,
         proposed_futures_short_e12:preview.proposed_futures_short_atoms_e12,
         source_window_locally_consistent:true,
         independent_exchange_bills_authenticated:false,
         account_global_history_proven:false,
         persisted_owner_v2_seal_proven:false,
         actual_futures_filled_proven:false,
         may_send_or_verify_closed:false,
     })
 }
