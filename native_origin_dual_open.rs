//! R32D6C8B2B2B4B: policy-bound, dual-venue opening-chain diagnostic.
//! Consumes EXISTING persisted R32D6C2/C3/C5/C6 read-only evidence. This
//! proves internal consistency only for the bounded audited source windows.
//! No genuine exchange-global history, no atomic venue pair, no Native Hedged,
//! no VerifiedClosed, no executable permit and no production mutations.
use crate::{
    authoritative_state::{
        NativeAuthoritativeState,NativeEntryAuthority,NativeEntryLeg,NativeEntryPhase,
        NativeEntryRecoveryQuality,NativeStateError,NativeStateStore,
    },
    execution::IntentState,
};
use openssl::sha::sha256;
use serde::{Deserialize,Serialize};

fn hash<T:Serialize>(x:&T)->Result<String,NativeStateError>{
    let v=serde_json::to_vec(x).map_err(|_|NativeStateError::Json)?;
    Ok(sha256(&v).iter().map(|b|format!("{b:02x}")).collect())
}
fn deny(s:&'static str)->NativeStateError{NativeStateError::Invalid(s)}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
pub struct NativeOriginDualOpeningLink {
    origin_key_sha256:String,
    original_policy_sha256:String,
    spot_order_id:String,
    spot_trade_ids:Vec<String>,
    spot_bill_ids:Vec<String>,
    futures_intent_id:String,
    futures_client_id:String,
    futures_order_id:String,
    futures_trade_ids:Vec<String>,
    spot_qty_e18:u128,
    futures_short_e18:u128,
    remaining_spot_exposure_e18:u128,
    futures_fill_fee_e12:i128,
    audited_joint_generation:u64,
    spot_source_sha256:String,
    okx_bills_sha256:String,
    futures_checkpoint_sha256:String,
    futures_income_sha256:String,
    two_venue_window_sha256:String,
    may_send_order:bool,
    may_promote_hedged:bool,
    may_verify_closed:bool,
}
impl NativeOriginDualOpeningLink {
    pub fn position_key(&self)->&str{&self.origin_key_sha256}
    pub fn futures_order_id(&self)->&str{&self.futures_order_id}
    pub fn futures_trade_ids(&self)->&[String]{&self.futures_trade_ids}
    pub fn exposure_remaining_e18(&self)->u128{self.remaining_spot_exposure_e18}
    pub fn futures_short_e18(&self)->u128{self.futures_short_e18}
    pub fn source_generation(&self)->u64{self.audited_joint_generation}
    pub fn may_send_order(&self)->bool{self.may_send_order}
    pub fn may_promote_hedged(&self)->bool{self.may_promote_hedged}
    pub fn may_verify_closed(&self)->bool{self.may_verify_closed}
    pub fn exchange_global_history_complete(&self)->bool{false}
    pub fn all_venue_snapshots_atomic(&self)->bool{false}
    pub fn closing_order_ancestry_proven(&self)->bool{false}
    fn derive(e:&NativeEntryAuthority,generation:u64)->Result<Self,NativeStateError>{
        let origin=e.origin.as_ref().ok_or(deny("original configured position key absent"))?;
        origin.verify_entry(e,generation)?;
        let spot=e.origin_spot_link.as_ref()
            .ok_or(deny("original C2 spot link not sealed"))?;
        spot.verify(e,generation)?;
        let first=e.spot_prehedge_evidence.as_ref().ok_or(deny("C2 missing"))?;
        let bills=e.spot_prehedge_bills_evidence.as_ref().ok_or(deny("C3 OKX all-base bills missing"))?;
        let cp=e.recovery_checkpoint.as_ref().ok_or(deny("original futures unknown checkpoint missing"))?;
        let f=e.futures_entry_window_evidence.as_ref().ok_or(deny("C5 Binance income/trades missing"))?;
        let joint=e.prospective_joint_evidence.as_ref().ok_or(deny("C6 same-entry joint receipts missing"))?;
        let policy=e.policy_binding.as_ref().ok_or(deny("original policy missing"))?;
        let open=e.economics.entry.futures.order(&cp.intent_id)
            .map_err(|_|deny("original futures order not durably prepared"))?;
        let ids:Vec<String>=cp.ledger.fills.keys().cloned().collect();
        let order_qty=cp.ledger.filled_qty_atoms().checked_mul(1_000_000)
            .ok_or(deny("Binance filled quantity e12/e18 overflow"))?;
        let net=bills.observed_base_cash_atoms_e18
            .checked_sub(e.plan.spot_baseline_atoms_e18)
            .ok_or(deny("spot net below first baseline"))?;
        let residual=net.checked_sub(f.observed_short_atoms_e18)
            .ok_or(deny("futures short exceeds available spot"))?;
        // Bound actual futures e12 VWAP×fills/fees to the original, immutable
        // configured risk cap. Not a guarantee of complete account-wide fees.
        let notional=cp.ledger.quote_notional_atoms().ok_or(deny("futures fill price missing"))?
            .checked_mul(1_000_000).ok_or(deny("Binance e18 notional overflow"))?;
        let quote_fee=u128::try_from(cp.ledger.fee_cost_quote_atoms)
            .map_err(|_|deny("unsupported negative Binance fee/rebate"))?
            .checked_mul(1_000_000).ok_or(deny("Binance e18 fee overflow"))?;
        let total=notional.checked_add(quote_fee).ok_or(deny("Binance quote amount overflow"))?;
        if e.phase!=NativeEntryPhase::RecoveryRequired||generation<=joint.source_generation
            ||joint.source_generation<=f.source_generation
            ||f.source_generation<=bills.source_generation
            ||bills.source_generation<=first.source_generation
            ||first.source_generation<origin.initial_generation()
            ||joint.spot_first_source_generation!=first.source_generation
            ||joint.spot_bills_source_generation!=bills.source_generation
            ||joint.futures_window_source_generation!=f.source_generation
            ||bills.spot_first_source_generation!=first.source_generation
            ||spot.position_key()!=origin.position_key()
            ||spot.policy_sha256()!=origin.policy_sha256()
            ||spot.opening_order_id()!=first.spot_order_id
            ||spot.trade_ids()!=first.exact_ledger.fills.keys().cloned().collect::<Vec<_>>()
            ||joint.spot_intent_id!=first.spot_intent_id
            ||joint.spot_client_id!=first.spot_client_id
            ||joint.spot_order_id!=first.spot_order_id
            ||joint.spot_trade_ids!=spot.trade_ids()
            ||joint.spot_bill_ids!=bills.verified_bill_ids
            ||joint.spot_available_net_atoms_e18!=net
            ||joint.spot_cash_e18!=bills.observed_base_cash_atoms_e18
            ||joint.spot_gross_atoms_e18!=spot.filled_qty_atoms_e18()
            ||cp.leg!=NativeEntryLeg::Futures
            ||cp.quality!=NativeEntryRecoveryQuality::FuturesPriorSpotUnverified
            ||cp.order_id.is_empty()||cp.client_id.is_empty()
            ||cp.ledger.exchange_order_id.as_deref()!=Some(cp.order_id.as_str())
            ||cp.ledger.client_order_id!=cp.client_id||cp.ledger.intent_id!=cp.intent_id
            ||cp.ledger.venue!="binance"||cp.ledger.side!="sell"
            ||cp.ledger.symbol!=e.plan.futures_symbol
            ||!cp.ledger.terminal||!cp.ledger.fee_evidence_complete
            ||ids.is_empty()||ids.iter().any(|id|id.is_empty())
            ||cp.ledger.fills.values().any(|x|x.trade_time_ms.is_none()
                ||x.fee_cost_quote_atoms.is_none()||x.qty_atoms==0||x.price_atoms==0)
            ||open.client_order_id!=cp.client_id||open.venue!="binance"
            ||open.symbol!=e.plan.futures_symbol||open.side!="sell"
            ||open.intent_id!=cp.intent_id
            ||e.intents.iter().filter(|i|i.id==cp.intent_id
                &&i.client_order_id.as_deref()==Some(cp.client_id.as_str())
                &&matches!(i.state,IntentState::Prepared|IntentState::Unknown)).count()!=1
            ||e.intents.iter().any(|i|i.id==first.spot_intent_id && i.client_order_id.as_deref()==Some(cp.client_id.as_str()))
            ||cp.ledger.requested_qty_atoms!=open.requested_qty_atoms
            ||order_qty==0||order_qty>net
            ||order_qty!=f.fill_qty_atoms_e18
            ||f.observed_short_atoms_e18!=order_qty
            ||f.futures_order_id!=cp.order_id||f.futures_client_id!=cp.client_id
            ||f.futures_intent_id!=cp.intent_id
            ||f.trade_ids!=ids
            ||cp.ledger.fee_cost_quote_atoms!=f.signed_quote_fee_cost_atoms_e12
            ||joint.futures_intent_id!=cp.intent_id||joint.futures_client_id!=cp.client_id
            ||joint.futures_order_id!=cp.order_id||joint.futures_trade_ids!=ids
            ||joint.futures_short_atoms_e18!=order_qty
            ||joint.futures_fee_quote_atoms_e12!=cp.ledger.fee_cost_quote_atoms
            ||joint.exposed_spot_unhedged_atoms_e18!=residual
            ||!joint.okx_balance_bracket_matched
            ||!joint.binance_income_and_position_bracket_matched
            ||!joint.foreign_symbol_trades_absent_in_bounded_window
            ||!bills.exact_bills_for_first_leg_matched
            ||!bills.base_cash_ancestry_to_original_baseline_matched
            ||!bills.source_balance_bracket_matched
            ||!f.exact_fills_commissions_and_symbol_history_matched
            ||!f.income_position_wallet_bracket_matched
            ||!first.cash_bracket_matched
            ||joint.exchange_global_history_complete
            ||joint.cross_venue_exchange_snapshot_atomicity_proven
            ||bills.account_history_globally_complete||f.exchange_global_history_complete
            ||f.cross_venue_atomicity_proven||bills.cross_venue_atomicity_proven
            ||joint.may_send_order||joint.may_promote_hedged
            ||bills.may_send_order||f.may_send_order||f.may_promote_hedged
            ||policy.config_sha256!=origin.policy_sha256()
            ||total>policy.user_quote_cap_e18
            ||total>policy.config_order_quote_e18
            ||total>policy.futures_notional_e18 // conservative: no quote-fee budget release
        {
            return Err(deny("original C2/C3/C5/C6 dual opening identity/economics mismatch"))
        }
        Ok(Self{
            origin_key_sha256:origin.position_key().into(),
            original_policy_sha256:origin.policy_sha256().into(),
            spot_order_id:spot.opening_order_id().into(),
            spot_trade_ids:spot.trade_ids().to_vec(),
            spot_bill_ids:bills.verified_bill_ids.clone(),
            futures_intent_id:cp.intent_id.clone(),futures_client_id:cp.client_id.clone(),
            futures_order_id:cp.order_id.clone(),futures_trade_ids:ids,
            spot_qty_e18:net,futures_short_e18:order_qty,
            remaining_spot_exposure_e18:residual,
            futures_fill_fee_e12:cp.ledger.fee_cost_quote_atoms,
            audited_joint_generation:joint.source_generation,
            spot_source_sha256:hash(first)?,okx_bills_sha256:hash(bills)?,
            futures_checkpoint_sha256:hash(cp)?,futures_income_sha256:hash(f)?,
            two_venue_window_sha256:hash(joint)?,
            may_send_order:false,may_promote_hedged:false,may_verify_closed:false,
        })
    }
    pub fn verify(&self,e:&NativeEntryAuthority,generation:u64)->Result<(),NativeStateError>{
        if self.may_send_order||self.may_promote_hedged||self.may_verify_closed
            ||Self::derive(e,generation)?!=*self{
            return Err(deny("original two-leg evidence seal changed"))
        }
        Ok(())
    }
}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum OriginDualOpeningWrite{Recorded,Idempotent}
impl NativeStateStore {
    /// This stores an extra review-only CHECKED snapshot; no new signed GET.
    /// Earlier C2/C3/C5/C6 collectors are separate and bounded. Their receipts
    /// do not prove exchange-account lifetime completeness.
    pub fn record_origin_dual_opening_link(
        &self,expected_generation:u64,
    )->Result<(NativeAuthoritativeState,OriginDualOpeningWrite),NativeStateError>{
        let snapshot=self.load()?.ok_or(NativeStateError::Generation)?;
        if snapshot.generation!=expected_generation{
            return Err(NativeStateError::Generation)
        }
        if let Some(e)=snapshot.entry.as_ref(){
            if let Some(old)=e.origin_dual_opening_link.as_ref(){
                old.verify(e,snapshot.generation)?;
                return Ok((snapshot,OriginDualOpeningWrite::Idempotent))
            }
        }
        let (state,_)=self.transact(expected_generation,|state|{
            if state.active.is_some(){return Err(deny("Native active denied"))}
            let e=state.entry.as_mut().ok_or(deny("original entry missing"))?;
            if e.origin_dual_opening_link.is_some(){
                return Err(deny("concurrent dual link already recorded"))
            }
            e.origin_dual_opening_link=Some(NativeOriginDualOpeningLink::derive(e,state.generation)?);
            Ok(())
        })?;
        Ok((state,OriginDualOpeningWrite::Recorded))
    }
}
