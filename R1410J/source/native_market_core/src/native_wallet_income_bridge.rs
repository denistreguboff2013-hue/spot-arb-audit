//! MICRO5B1 — bounded, read-only original Binance wallet vs signed C5/C6
//! entry commission comparison. No global income/transfer/lifetime claim.
//! Requires genuine pre-entry MICRO5A source, C2/C3 spot lineage, durable
//! MICRO1/MICRO3 original futures and independently rechecked C5/C6 proofs.
use crate::authoritative_state::{
    NativeAuthoritativeState,NativeEntryAuthority,NativeStateError,NativeStateStore,
    NativeEntryPhase,
};
use serde::{Serialize,Deserialize};
use openssl::sha::sha256;

fn deny(x:&'static str)->NativeStateError{NativeStateError::Invalid(x)}
fn digest<T:Serialize>(x:&T)->Result<String,NativeStateError>{
    Ok(sha256(&serde_json::to_vec(x).map_err(|_|NativeStateError::Json)?)
        .iter().map(|b|format!("{b:02x}")).collect())
}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
pub struct NativeWalletIncomeBridge {
    source_generation:u64,
    pre_entry_hash:String,
    original_spot_hash:String,
    original_futures_hash:String,
    independent_binance_hash:String,
    independent_joint_hash:String,
    start_wallet_e12:u128,
    observed_wallet_e12:u128,
    verified_futures_commissions_e12:i128,
    observed_wallet_delta_e12:i128,
    observed_wallet_matches_commissions:bool,
    start_to_first_trade_global_income_history_complete:bool,
    usdt_internal_external_transfer_history_complete:bool,
    funding_history_through_close_complete:bool,
    global_account_profit_proven:bool,
    independent_anti_rollback_proven:bool,
    may_send_order:bool,
    may_verify_hedged:bool,
    may_verify_closed:bool,
}
impl NativeWalletIncomeBridge {
    pub fn start_wallet_e12(&self)->u128{self.start_wallet_e12}
    pub fn observed_wallet_e12(&self)->u128{self.observed_wallet_e12}
    pub fn observed_wallet_delta_e12(&self)->i128{self.observed_wallet_delta_e12}
    pub fn known_futures_commissions_e12(&self)->i128{
        self.verified_futures_commissions_e12
    }
    pub fn may_send_order(&self)->bool{self.may_send_order}
    pub fn is_lifetime_complete(&self)->bool{
        self.global_account_profit_proven
    }
    fn identity(s:&NativeAuthoritativeState,e:&NativeEntryAuthority)
      ->Result<(String,String,String,String,String,u128,u128,i128),NativeStateError>{
        let opening=s.pre_entry_binance_baseline.as_ref()
            .ok_or(deny("MICRO5A original Binance wallet baseline not persisted"))?;
        opening.validate(s)?;
        let spot=e.spot_prehedge_evidence.as_ref()
            .ok_or(deny("original OKX signed spot fills missing"))?;
        let bills=e.spot_prehedge_bills_evidence.as_ref()
            .ok_or(deny("original OKX currency-wide bills missing"))?;
        let q=e.futures_uncertain_quarantine.as_ref()
            .ok_or(deny("original Binance futures quarantine missing"))?;
        q.verify(e,s.generation)?;
        let cp=e.recovery_checkpoint.as_ref()
            .ok_or(deny("original Binance per-trade checkpoint missing"))?;
        q.validate_get_checkpoint(e,cp)?;
        let bin=e.futures_entry_window_evidence.as_ref()
            .ok_or(deny("Binance income and whole-symbol audit missing"))?;
        let joint=e.prospective_joint_evidence.as_ref()
            .ok_or(deny("fresh two-exchange signed audit missing"))?;
        if e.phase!=NativeEntryPhase::RecoveryRequired
            ||s.active.is_some()
            ||bin.futures_intent_id!=cp.intent_id
            ||bin.futures_client_id!=cp.client_id
            ||bin.futures_order_id!=cp.order_id
            ||bin.trade_ids!=cp.ledger.fills.keys().cloned().collect::<Vec<_>>()
            ||bin.signed_quote_fee_cost_atoms_e12!=cp.ledger.fee_cost_quote_atoms
            ||joint.futures_client_id!=cp.client_id
            ||joint.futures_order_id!=cp.order_id
            ||joint.futures_wallet_usdt_e18!=bin.observed_wallet_usdt_atoms_e18
            ||joint.futures_fee_quote_atoms_e12!=bin.signed_quote_fee_cost_atoms_e12
            ||!bin.income_position_wallet_bracket_matched
            ||!bin.exact_fills_commissions_and_symbol_history_matched
            ||!joint.okx_balance_bracket_matched
            ||!joint.binance_income_and_position_bracket_matched
            ||!joint.foreign_symbol_trades_absent_in_bounded_window
            ||opening.captured_at_ms()>e.plan.created_at_ms
            ||e.plan.created_at_ms-opening.captured_at_ms()>60_000
            ||bin.may_send_order||bin.may_promote_hedged
            ||joint.may_send_order||joint.may_promote_hedged{
            return Err(deny("original wallet bridge provenance inconsistent"))
        }
        let observed=bin.observed_wallet_usdt_atoms_e18;
        if observed%1_000_000!=0{
            return Err(deny("observed Binance wallet e18 not exactly representable e12"))
        }
        let fee=cp.ledger.fee_cost_quote_atoms;
        if fee<0{return Err(deny("original Binance fee sign wrong"))}
        Ok((digest(opening)?,digest(&(spot,bills))?,digest(&(q,cp))?,
            digest(bin)?,digest(joint)?,
            opening.wallet_usdt_e12(),observed/1_000_000,fee))
    }
    pub fn verify(&self,s:&NativeAuthoritativeState)->Result<(),NativeStateError>{
        let e=s.entry.as_ref().ok_or(deny("wallet bridge native entry erased"))?;
        let (opening,spot,futures,bin,joint,start,current,fee)=Self::identity(s,e)?;
        let actual=i128::try_from(current).ok().and_then(|c|
            i128::try_from(start).ok().and_then(|o|c.checked_sub(o)))
            .ok_or(deny("wallet delta precision overflow"))?;
        if s.generation<=self.source_generation
            ||self.pre_entry_hash!=opening||self.original_spot_hash!=spot
            ||self.original_futures_hash!=futures
            ||self.independent_binance_hash!=bin||self.independent_joint_hash!=joint
            ||self.start_wallet_e12!=start||self.observed_wallet_e12!=current
            ||self.verified_futures_commissions_e12!=fee
            ||self.observed_wallet_delta_e12!=actual
            ||actual!=fee.checked_neg().ok_or(deny("wallet fee negation overflow"))?
            ||!self.observed_wallet_matches_commissions
            ||self.start_to_first_trade_global_income_history_complete
            ||self.usdt_internal_external_transfer_history_complete
            ||self.funding_history_through_close_complete
            ||self.global_account_profit_proven
            ||self.independent_anti_rollback_proven
            ||self.may_send_order||self.may_verify_hedged||self.may_verify_closed{
            return Err(deny("wallet bridge forged, erased or missing unexplained USDT delta"))
        }
        Ok(())
    }
}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum NativeWalletBridgeWrite{Recorded,Idempotent}
impl NativeStateStore {
    /// Local CAS/fsync. Never query an exchange, change Native quantities,
    /// or enable execution. A mismatch is a hard DENY with no disk mutation.
    pub fn record_quarantined_wallet_income_bridge(
        &self,expected_generation:u64,
    )->Result<(NativeAuthoritativeState,NativeWalletBridgeWrite),NativeStateError>{
        let previous=self.load()?.ok_or(NativeStateError::Generation)?;
        if previous.generation!=expected_generation{return Err(NativeStateError::Generation)}
        if let Some(proof)=previous.entry.as_ref().and_then(|e|e.wallet_income_bridge.as_ref()){
            proof.verify(&previous)?;
            return Ok((previous,NativeWalletBridgeWrite::Idempotent))
        }
        let (saved,_)=self.transact(expected_generation,|s|{
            let (opening,spot,futures,bin,joint,start,current,fee)={
                let e=s.entry.as_ref().ok_or(deny("native entry missing"))?;
                NativeWalletIncomeBridge::identity(s,e)?
            };
            if s.entry.as_ref().unwrap().wallet_income_bridge.is_some(){
                return Err(deny("wallet proof race"))
            }
            let delta=i128::try_from(current).ok().and_then(|c|
                i128::try_from(start).ok().and_then(|o|c.checked_sub(o)))
                .ok_or(deny("wallet delta precision overflow"))?;
            if delta!=fee.checked_neg().ok_or(deny("fee cost overflow"))?{
                return Err(deny("Binance wallet differs from known commission cashflows"))
            }
            s.entry.as_mut().unwrap().wallet_income_bridge=Some(NativeWalletIncomeBridge{
                source_generation:expected_generation,
                pre_entry_hash:opening,original_spot_hash:spot,
                original_futures_hash:futures,independent_binance_hash:bin,
                independent_joint_hash:joint,
                start_wallet_e12:start,observed_wallet_e12:current,
                verified_futures_commissions_e12:fee,observed_wallet_delta_e12:delta,
                observed_wallet_matches_commissions:true,
                start_to_first_trade_global_income_history_complete:false,
                usdt_internal_external_transfer_history_complete:false,
                funding_history_through_close_complete:false,
                global_account_profit_proven:false,independent_anti_rollback_proven:false,
                may_send_order:false,may_verify_hedged:false,may_verify_closed:false,
            });
            Ok(())
        })?;
        Ok((saved,NativeWalletBridgeWrite::Recorded))
    }
}
