//! R32D6C8B2B2B4A: exact OKX FIRST opening leg evidence -> original
//! policy/intent origin seal. Pure review owner-CAS diagnostic; NO permission,
//! NO futures/close proof, NO complete account history. The trusted C2
//! per-fill receipt must already be persisted via its signed mock-only GET.
use crate::authoritative_state::{
    NativeEntryAuthority,NativeEntryPhase,NativeStateError,NativeStateStore,
    NativeAuthoritativeState,
};
use crate::native_spot_prehedge::NativeSpotPreHedgeEvidence;
use openssl::sha::sha256;
use serde::{Serialize,Deserialize};

fn hex(bytes:&[u8])->String{
    const H:&[u8;16]=b"0123456789abcdef";
    let mut s=String::with_capacity(bytes.len()*2);
    for &b in bytes{s.push(H[(b>>4)as usize]as char);s.push(H[(b&15)as usize]as char)}
    s
}
fn fingerprint<T:Serialize>(x:&T)->Result<String,NativeStateError>{
    Ok(hex(&sha256(&serde_json::to_vec(x).map_err(|_|NativeStateError::Json)?)))
}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
pub struct NativeOriginSpotLink {
    origin_position_key:String,
    original_config_sha256:String,
    source_generation:u64,
    observed_at_ms:u64,
    spot_intent_id:String,
    spot_client_id:String,
    spot_order_id:String,
    trade_ids:Vec<String>,
    fill_bill_ids:Vec<String>,
    exact_ledger_sha256:String,
    signed_base_fee_atoms_e18:i128,
    filled_qty_atoms_e18:u128,
    settled_cash_atoms_e18:u128,
    /// None of these may ever become true in the review record.
    may_send_order:bool,
    may_promote_hedged:bool,
    may_verify_closed:bool,
}
impl NativeOriginSpotLink {
    pub fn position_key(&self)->&str{&self.origin_position_key}
    pub fn policy_sha256(&self)->&str{&self.original_config_sha256}
    pub fn opening_order_id(&self)->&str{&self.spot_order_id}
    pub fn trade_ids(&self)->&[String]{&self.trade_ids}
    pub fn filled_qty_atoms_e18(&self)->u128{self.filled_qty_atoms_e18}
    pub fn may_send_order(&self)->bool{self.may_send_order}
    pub fn may_promote_hedged(&self)->bool{self.may_promote_hedged}
    pub fn may_verify_closed(&self)->bool{self.may_verify_closed}
    pub fn proves_both_opening_legs(&self)->bool{false}
    pub fn proves_account_wide_history(&self)->bool{false}
    pub fn source_generation(&self)->u64{self.source_generation}

    fn derive(entry:&NativeEntryAuthority,now_generation:u64)
        ->Result<Self,NativeStateError>{
        let origin=entry.origin.as_ref().ok_or(NativeStateError::Invalid(
            "configured original entry anchor absent"))?;
        origin.verify_entry(entry,now_generation)?;
        let bind=entry.policy_binding.as_ref().ok_or(NativeStateError::Invalid(
            "bound original opening policy absent"))?;
        let first:&NativeSpotPreHedgeEvidence=entry.spot_prehedge_evidence.as_ref()
            .ok_or(NativeStateError::Invalid("signed spot first-leg proof absent"))?;
        let led=&first.exact_ledger;
        let original=entry.economics.entry.spot.order(&first.spot_intent_id)
            .map_err(|_|NativeStateError::Invalid("persisted original ledger absent"))?;
        let q=led.filled_qty_atoms().checked_mul(1_000_000)
            .ok_or(NativeStateError::Invalid("spot amount precision overflow"))?;
        bind.verify_exact_spot_fills(&entry.plan,led)
            .map_err(|_|NativeStateError::Invalid("opening fill/fee breaches config quote cap"))?;
        let fills:Vec<String>=led.fills.keys().cloned().collect();
        if now_generation<=first.source_generation
            ||first.source_generation<origin.initial_generation()
            ||entry.phase!=NativeEntryPhase::SpotExposed
                &&entry.phase!=NativeEntryPhase::RecoveryRequired
                &&entry.phase!=NativeEntryPhase::FuturesPrepared
            ||first.spot_client_id!=origin.spot_client_id()
            ||first.spot_intent_id!=led.intent_id
            ||first.spot_client_id!=led.client_order_id
            ||first.exact_ledger!=*original
            ||first.spot_order_id!=led.exchange_order_id.as_deref().unwrap_or("")
            ||first.spot_order_id.is_empty()
            ||!first.spot_order_id.bytes().all(|b|b.is_ascii_digit())
            ||!led.terminal||!led.fee_evidence_complete||led.fills.is_empty()
            ||fills.iter().any(|id|id.is_empty())
            ||first.fill_bill_ids.len()!=fills.len()
            ||q==0||q!=entry.spot_filled_atoms_e18
            ||q>entry.plan.planned_qty_atoms_e18
            ||first.safety_generation!=entry.plan.safety_generation
            ||first.observed_at_ms<entry.plan.created_at_ms
            ||first.observed_at_ms==0
            ||first.signed_base_fee_atoms_e18>0
            ||first.observed_base_cash_atoms_e18
                !=entry.plan.spot_baseline_atoms_e18
                    .checked_add_signed(
                        i128::try_from(q).ok().and_then(|v|
                            v.checked_add(first.signed_base_fee_atoms_e18))
                        .ok_or(NativeStateError::Invalid("base fee/cash overflow"))?
                    ).ok_or(NativeStateError::Invalid("base cash underflow/overflow"))?
            ||!first.cash_bracket_matched
            ||first.exchange_global_completeness_proven
            ||first.exchange_bill_history_proven
            ||first.may_send_order
            ||led.fills.values().any(|f|f.trade_time_ms.is_none()||
                f.fee_cost_quote_atoms.is_none()||f.price_atoms==0||f.qty_atoms==0){
                return Err(NativeStateError::Invalid("first opening leg not bound to original identity"))
        }
        Ok(Self{
            origin_position_key:origin.position_key().to_owned(),
            original_config_sha256:origin.policy_sha256().to_owned(),
            source_generation:first.source_generation,
            observed_at_ms:first.observed_at_ms,
            spot_intent_id:first.spot_intent_id.clone(),
            spot_client_id:first.spot_client_id.clone(),
            spot_order_id:first.spot_order_id.clone(),
            trade_ids:fills,
            fill_bill_ids:first.fill_bill_ids.clone(),
            exact_ledger_sha256:fingerprint(led)?,
            signed_base_fee_atoms_e18:first.signed_base_fee_atoms_e18,
            filled_qty_atoms_e18:q,
            settled_cash_atoms_e18:first.observed_base_cash_atoms_e18,
            may_send_order:false,may_promote_hedged:false,may_verify_closed:false,
        })
    }
    pub fn verify(&self,entry:&NativeEntryAuthority,now_generation:u64)
        ->Result<(),NativeStateError>{
        if self.may_send_order||self.may_promote_hedged||self.may_verify_closed
            ||Self::derive(entry,now_generation)?!=*self {
            return Err(NativeStateError::Invalid("origin-spot seal changed/authority overclaim"))
        }
        Ok(())
    }
}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum OriginSpotLinkWrite{Recorded,Idempotent}
impl NativeStateStore {
    /// Owner CAS, atomic NativeStateStore fsync/rename/dir fsync. No credential
    /// access, no real orders. Reuses previously sealed signed-first-leg proof.
    pub fn record_origin_spot_link(
        &self,expected_generation:u64,
    )->Result<(NativeAuthoritativeState,OriginSpotLinkWrite),NativeStateError>{
        let current=self.load()?.ok_or(NativeStateError::Generation)?;
        if let Some(e)=current.entry.as_ref(){
            if let Some(link)=&e.origin_spot_link{
                link.verify(e,current.generation)?;
                if expected_generation==current.generation
                    ||expected_generation.checked_add(1)==Some(current.generation){
                    return Ok((current,OriginSpotLinkWrite::Idempotent))
                }
            }
        }
        let (next,_)=self.transact(expected_generation,|state|{
            if state.active.is_some(){
                return Err(NativeStateError::Invalid("active origin cannot be staged"))
            }
            let e=state.entry.as_mut().ok_or(NativeStateError::Invalid(
                "no existing prepared opening"))?;
            if e.origin_spot_link.is_some(){
                return Err(NativeStateError::Invalid("conflicting origin-spot link"))
            }
            let seal=NativeOriginSpotLink::derive(e,state.generation)?;
            e.origin_spot_link=Some(seal);
            Ok(())
        })?;
        Ok((next,OriginSpotLinkWrite::Recorded))
    }
}
