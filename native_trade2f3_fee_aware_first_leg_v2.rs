//! Astra P0-02/P1-02: V2 read-only first-leg BASE/QUOTE fee source diagnostic.
 //! Does not change legacy LegFillLedger::fee_evidence_complete semantics,
 //! persisted owner schema, old seals, risk authorization or exchange egress.
 use std::collections::BTreeSet;
 use crate::{
     authoritative_state::NativeEntryPlan,
     fill_economics::{LegFillLedger,SCALE_12},
     native_spot_prehedge::NativeSpotPreHedgeEvidence,
     native_trading_config::NativeEntryPolicyBinding,
     native_trade2f_asset_fee::SpotFeeFill,
 };
 #[derive(Clone,Debug,PartialEq,Eq)]
 pub enum FirstLegV2Error{
     Incomplete(&'static str),Inconsistent(&'static str),Overflow,
 }
 #[derive(Clone,Debug,PartialEq,Eq)]
 pub struct FirstLegFeePreviewV2{
     pub original_spot_gross_e12:u128,
     pub original_base_buy_fee_e12:u128,
     pub original_quote_buy_fee_e12:u128,
     pub original_delivered_base_e12:u128,
     pub quote_spent_with_quote_fees_e12:u128,
     pub base_fee_is_explicitly_source_shaped:bool,
     pub persisted_owner_v2_seal_available:bool,
     pub independent_live_exchange_attestation:bool,
     pub trading_or_close_authorized:bool,
 }
 fn deny(s:&'static str)->FirstLegV2Error{FirstLegV2Error::Inconsistent(s)}
 fn missing(s:&'static str)->FirstLegV2Error{FirstLegV2Error::Incomplete(s)}
 /// Standalone proposal review using a caller-supplied source-shaped tape.
 /// A passing diagnostic is NEVER a signature, account truth, nor permission.
 pub fn inspect_first_leg_fee_preview_v2(
     policy:&NativeEntryPolicyBinding,
     plan:&NativeEntryPlan,
     original:&NativeSpotPreHedgeEvidence,
     source_rows:&[SpotFeeFill],
 )->Result<FirstLegFeePreviewV2,FirstLegV2Error>{
     policy.validate_in_state(plan)
         .map_err(|_|deny("original risk policy/plan invalid"))?;
     let led:&LegFillLedger=&original.exact_ledger;
     if led.venue!="okx"||led.side!="buy"||led.symbol!=plan.spot_symbol
         ||led.intent_id!=original.spot_intent_id
         ||led.client_order_id!=original.spot_client_id
         ||led.exchange_order_id.as_deref()!=Some(original.spot_order_id.as_str())
         ||led.fills.is_empty()||source_rows.is_empty()||source_rows.len()!=led.fills.len()
         ||source_rows.len()>64||!led.terminal||!led.cumulative_truth_present
         ||led.filled_qty_atoms()==0||led.filled_qty_atoms()!=led.requested_qty_atoms
         ||led.filled_qty_atoms().checked_mul(1_000_000)
             .filter(|q|*q<=plan.planned_qty_atoms_e18).is_none()
         ||original.signed_base_fee_atoms_e18>0
         ||!original.cash_bracket_matched
         ||original.exchange_global_completeness_proven
         ||original.exchange_bill_history_proven||original.may_send_order
     {return Err(missing("original terminal buy, source or policy mismatch"))}
     let norm_symbol=led.symbol.replace('/',"-");
     let base=norm_symbol.split('-').next().unwrap_or("");
     if base.is_empty()||!norm_symbol.ends_with("-USDT"){
         return Err(deny("BASE/USDT pair required"))
     }
     let mut quote_spent=0u128;
     let mut base_fee=0u128;
     let mut quote_fee=0u128;
     let mut seen=BTreeSet::new();
     let mut accounted_qty=0u128;
     for fee in source_rows{
         if fee.side!="buy"||fee.order_id!=original.spot_order_id
             ||fee.inst_id.replace('/',"-")!=norm_symbol
             ||fee.trade_id.is_empty()||fee.at_ms==0||fee.price_quote_e12==0
             ||fee.gross_base_qty_e12==0||fee.signed_fee_e12>0
             ||!seen.insert(fee.trade_id.as_str()){
             return Err(deny("fee row source identity/rebate not allowed"))
         }
         let fill=led.fills.get(&fee.trade_id)
             .ok_or(missing("source trade not present in original ledger"))?;
         if fill.qty_atoms!=fee.gross_base_qty_e12
             ||fill.price_atoms!=fee.price_quote_e12
             ||fill.trade_time_ms!=Some(fee.at_ms)
             ||fill.fee_currency.as_deref()!=Some(fee.fee_ccy.as_str()){
             return Err(deny("source row differs from immutable original fill"))
         }
         let fee_cost=fee.signed_fee_e12.checked_neg()
             .ok_or(FirstLegV2Error::Overflow)?;
         let cost=u128::try_from(fee_cost).map_err(|_|FirstLegV2Error::Overflow)?;
         let notional=fill.qty_atoms.checked_mul(fill.price_atoms)
             .ok_or(FirstLegV2Error::Overflow)?
             .checked_add(SCALE_12-1)
             .ok_or(FirstLegV2Error::Overflow)?/SCALE_12;
         let quote_total=match fee.fee_ccy.as_str(){
             "USDT"=>{
                 if fill.fee_cost_quote_atoms!=Some(fee_cost){
                     return Err(deny("original USDT fee cost differs from source"))
                 }
                 quote_fee=quote_fee.checked_add(cost)
                     .ok_or(FirstLegV2Error::Overflow)?;
                 notional.checked_add(cost).ok_or(FirstLegV2Error::Overflow)?
             },
             c if c==base=>{
                 if fill.fee_cost_quote_atoms.is_some(){
                     return Err(deny("BASE fee falsely converted to USDT fee"))
                 }
                 base_fee=base_fee.checked_add(cost)
                     .ok_or(FirstLegV2Error::Overflow)?;
                 notional
             },
             _=>return Err(deny("unsupported third asset fee")),
         };
         quote_spent=quote_spent.checked_add(quote_total)
             .ok_or(FirstLegV2Error::Overflow)?;
         accounted_qty=accounted_qty.checked_add(fill.qty_atoms)
             .ok_or(FirstLegV2Error::Overflow)?;
     }
     let gross=led.filled_qty_atoms();
     let net=gross.checked_sub(base_fee)
         .ok_or(deny("BASE fees consume more than original BUY"))?;
     if net==0||accounted_qty!=gross
         ||original.signed_base_fee_atoms_e18!=
             -i128::try_from(base_fee.checked_mul(1_000_000)
                 .ok_or(FirstLegV2Error::Overflow)?).map_err(|_|FirstLegV2Error::Overflow)?
         ||plan.spot_baseline_atoms_e18.checked_add(net.checked_mul(1_000_000)
              .ok_or(FirstLegV2Error::Overflow)?)!=Some(original.observed_base_cash_atoms_e18)
         ||quote_spent.checked_mul(1_000_000).filter(|x|
             *x<=policy.user_quote_cap_e18
             &&*x<=policy.config_order_quote_e18
             &&*x<=plan.order_quote_atoms_e18).is_none(){
         return Err(deny("BASE net inventory, cash bracket or quote budget mismatched"))
     }
     Ok(FirstLegFeePreviewV2{
         original_spot_gross_e12:gross,
         original_base_buy_fee_e12:base_fee,
         original_quote_buy_fee_e12:quote_fee,
         original_delivered_base_e12:net,
         quote_spent_with_quote_fees_e12:quote_spent,
         base_fee_is_explicitly_source_shaped:true,
         persisted_owner_v2_seal_available:false,
         independent_live_exchange_attestation:false,
         trading_or_close_authorized:false,
     })
 }
