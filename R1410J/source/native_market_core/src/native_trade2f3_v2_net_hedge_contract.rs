//! Astra P0-02/P1-02: additive, OFFLINE ONLY V2 C2->C3 net-hedge proposal.
 //! This does NOT read authoritative NativeStateStore, sign exchange bills,
 //! authorize HTTP, seal V1/V2 owner, prove wallet flatness, or mutate anything.
 //! Caller-provided generation and bill IDs are not an owner CAS or exchange truth.
 use std::collections::BTreeSet;
 use openssl::sha::sha256;
 use crate::{
     native_trade2f3_first_leg_v2_checkpoint::FirstLegV2CheckpointPayload,
     native_trade2f3_fee_aware_first_leg_v2::inspect_first_leg_fee_preview_v2,
 };
 #[derive(Clone,Debug,PartialEq,Eq)]
 pub enum V2NetHedgeError {
     Missing(&'static str),Mismatch(&'static str),Source,Encode,
 }
 #[derive(Clone,Debug,PartialEq,Eq)]
 pub struct V2NetHedgePreview {
     pub schema_version:u32,
     pub proposed_native_generation:u64,
     pub source_payload_sha256:String,
     pub original_okx_order_id:String,
     pub original_trade_ids:Vec<String>,
     pub original_bill_ids:Vec<String>,
     pub future_client_id:String,
     pub proposed_futures_intent_id:String,
     pub futures_symbol:String,
     pub spot_buy_gross_atoms_e12:u128,
     pub spot_base_fee_atoms_e12:u128,
     pub spot_delivered_net_atoms_e12:u128,
     pub proposed_futures_short_atoms_e12:u128,
     pub full_exchange_bill_ancestry_proven:bool,
     pub persisted_owner_v2_seal_available:bool,
     pub owner_generation_cas_proven:bool,
     pub may_send_or_cancel_or_verify_closed:bool,
 }
 fn deny(s:&'static str)->V2NetHedgeError{V2NetHedgeError::Mismatch(s)}
 /// Strict fee-aware target only; accepting this typed offline plan does
 /// not claim the exchange actually filled the hedge or paid any bill.
 pub fn inspect_v2_net_hedge_proposal(
     checkpoint:&FirstLegV2CheckpointPayload,
     expected_owner_generation:u64,
     futures_intent_id:&str,
     futures_client_id:&str,
     proposed_short_atoms_e12:u128,
 ) -> Result<V2NetHedgePreview,V2NetHedgeError> {
     if checkpoint.schema_version!=2
         ||expected_owner_generation==0
         ||checkpoint.original_native_generation!=expected_owner_generation
         ||checkpoint.original_owner_v2_seal_migrated
         ||checkpoint.exchange_global_fees_attested
         ||checkpoint.may_send_or_verify_closed {
         return Err(deny("unsupported V2 checkpoint or stale/overclaimed owner generation"))
     }
     let first=&checkpoint.original;
     if futures_intent_id.is_empty()||futures_client_id.is_empty()
         ||futures_intent_id==first.spot_intent_id
         ||futures_client_id==first.spot_client_id
         ||checkpoint.plan.futures_symbol.is_empty()
         ||checkpoint.plan.safety_generation!=first.safety_generation {
         return Err(deny("missing or reused original/futures identity"))
     }
     let actual=inspect_first_leg_fee_preview_v2(
         &checkpoint.policy,&checkpoint.plan,first,&checkpoint.source_shaped_fee_fills
     ).map_err(|_|V2NetHedgeError::Source)?;
     if checkpoint.quote_spent_atoms_e12!=actual.quote_spent_with_quote_fees_e12.to_string()
         ||checkpoint.net_base_after_fee_atoms_e12!=actual.original_delivered_base_e12.to_string() {
         return Err(deny("checkpoint cached quantity differs from source"))
     }
     if proposed_short_atoms_e12==0
         ||proposed_short_atoms_e12!=actual.original_delivered_base_e12 {
         return Err(deny("futures short proposal must match exact delivered BASE, not gross"))
     }
     // Local syntactic source/bill identity checks do NOT prove source
     // bill completeness, missing pages or account-wide funding history.
     if first.fill_bill_ids.len()!=first.exact_ledger.fills.len()
         ||first.fill_bill_ids.is_empty()
         ||first.fill_bill_ids.iter().any(|x|x.is_empty())
         ||first.fill_bill_ids.iter().collect::<BTreeSet<_>>().len()!=first.fill_bill_ids.len() {
         return Err(V2NetHedgeError::Missing("one unique source bill ID per original fill is required"))
     }
     let trade_ids=first.exact_ledger.fills.keys().cloned().collect::<Vec<_>>();
     let bytes=serde_json::to_vec(checkpoint).map_err(|_|V2NetHedgeError::Encode)?;
     let fingerprint=sha256(&bytes).iter().map(|x|format!("{x:02x}")).collect();
     Ok(V2NetHedgePreview {
         schema_version:2,
         proposed_native_generation:expected_owner_generation,
         source_payload_sha256:fingerprint,
         original_okx_order_id:first.spot_order_id.clone(),
         original_trade_ids:trade_ids,
         original_bill_ids:first.fill_bill_ids.clone(),
         future_client_id:futures_client_id.to_string(),
         proposed_futures_intent_id:futures_intent_id.to_string(),
         futures_symbol:checkpoint.plan.futures_symbol.clone(),
         spot_buy_gross_atoms_e12:actual.original_spot_gross_e12,
         spot_base_fee_atoms_e12:actual.original_base_buy_fee_e12,
         spot_delivered_net_atoms_e12:actual.original_delivered_base_e12,
         proposed_futures_short_atoms_e12:proposed_short_atoms_e12,
         full_exchange_bill_ancestry_proven:false,
         persisted_owner_v2_seal_available:false,
         owner_generation_cas_proven:false,
         may_send_or_cancel_or_verify_closed:false,
     })
 }
