//! R32D6C8B2B2B4C1: persisted permitless SHADOW close-order identity fence.
//! Review-only proposal AFTER a C2/C3/C5/C6 opening-origin diagnostic.
//! Does NOT create a real PendingCloseIntent, executable mutation token,
//! exchange order, leg receipt, live flat account or a verified close.
//! Incomplete original hedge remains RecoveryRequired, including residual spot.
use crate::authoritative_state::{
    NativeEntryAuthority,NativeEntryPhase,NativeAuthoritativeState,
    NativeStateError,NativeStateStore,
};
use crate::native_close_readonly::{CloseOrderIdentity,DualCloseCandidate};
use openssl::sha::sha256;
use serde::{Serialize,Deserialize};

fn deny(msg:&'static str)->NativeStateError{NativeStateError::Invalid(msg)}
fn valid_client(s:&str)->bool{
    !s.is_empty()&&s.len()<=96
        &&s.bytes().all(|b|b.is_ascii_alphanumeric()||b==b'-'||b==b'_'||b==b'.')
}
fn order_id(s:&str)->bool{
    !s.is_empty()&&s.len()<=40&&s.bytes().all(|b|b.is_ascii_digit())
}
fn digest<T:Serialize>(data:&T)->Result<String,NativeStateError>{
    let raw=serde_json::to_vec(data).map_err(|_|NativeStateError::Json)?;
    Ok(sha256(&raw).iter().map(|b|format!("{b:02x}")).collect())
}

#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
pub struct NativeShadowCloseIdentityFence {
    original_position_key_sha256:String,
    original_policy_sha256:String,
    opening_dual_link_sha256:String,
    source_native_generation:u64,
    observed_at_ms:u64,
    requested_qty_e12:u128,
    original_spot_open_id:String,
    original_futures_open_id:String,
    spot_close_client_id:String,
    spot_close_order_id:String,
    futures_close_client_id:String,
    futures_close_order_id:String,
    leftover_spot_atoms_e18:u128,
    cannot_fully_close_original_spot:bool,
    verified_close_order_receipts:bool,
    may_send_order:bool,
    may_verify_closed:bool,
    may_release_cooldown:bool,
}
impl NativeShadowCloseIdentityFence {
    pub fn position_key(&self)->&str{&self.original_position_key_sha256}
    pub fn request_quantity_e12(&self)->u128{self.requested_qty_e12}
    pub fn residual_spot_e18(&self)->u128{self.leftover_spot_atoms_e18}
    pub fn spot_client_id(&self)->&str{&self.spot_close_client_id}
    pub fn futures_client_id(&self)->&str{&self.futures_close_client_id}
    pub fn source_generation(&self)->u64{self.source_native_generation}
    pub fn may_send_order(&self)->bool{self.may_send_order}
    pub fn may_verify_closed(&self)->bool{self.may_verify_closed}
    pub fn may_release_cooldown(&self)->bool{self.may_release_cooldown}
    pub fn close_receipts_verified(&self)->bool{self.verified_close_order_receipts}
    pub fn is_real_pending_close_intent(&self)->bool{false}
    pub fn opening_proof_is_exchange_global(&self)->bool{false}
    fn derive(
        entry:&NativeEntryAuthority,
        source_generation:u64,
        candidate:&DualCloseCandidate
    )->Result<Self,NativeStateError>{
        let original=entry.origin.as_ref().ok_or(deny("original configured origin absent"))?;
        original.verify_entry(entry,source_generation)?;
        let link=entry.origin_dual_opening_link.as_ref()
            .ok_or(deny("original two-venue opening proof absent"))?;
        link.verify(entry,source_generation)?;
        let opened=entry.prospective_joint_evidence.as_ref()
            .ok_or(deny("original opening source window missing"))?;
        let spot=entry.spot_prehedge_evidence.as_ref()
            .ok_or(deny("original OKX opening missing"))?;
        let future=entry.recovery_checkpoint.as_ref()
            .ok_or(deny("original Binance opening missing"))?;
        let expected_e18=candidate.expected_qty_e12.checked_mul(1_000_000)
            .ok_or(deny("close proposed size overflow"))?;
        let original_short_e18=link.futures_short_e18();
        let remaining=link.exposure_remaining_e18();
        let confirmed_spot_e18=opened.spot_available_net_atoms_e18;
        if entry.phase!=NativeEntryPhase::RecoveryRequired
            || candidate.position_identity!=original.position_key()
            || candidate.position_identity!=link.position_key()
            || candidate.base!=entry.plan.base
            || candidate.spot.symbol!=entry.plan.spot_symbol
            || candidate.futures.symbol!=entry.plan.futures_symbol
            || !valid_client(&candidate.spot.client_id)
            || !valid_client(&candidate.futures.client_id)
            || !order_id(&candidate.spot.exchange_order_id)
            || !order_id(&candidate.futures.exchange_order_id)
            || candidate.spot.client_id==candidate.futures.client_id
            || candidate.spot.client_id==spot.spot_client_id
            || candidate.futures.client_id==future.client_id
            || candidate.spot.client_id==future.client_id
            || candidate.futures.client_id==spot.spot_client_id
            || entry.intents.iter().any(|i|i.client_order_id.as_deref()
                ==Some(candidate.spot.client_id.as_str())
                ||i.client_order_id.as_deref()==Some(candidate.futures.client_id.as_str()))
            || candidate.spot.exchange_order_id==spot.spot_order_id
            || candidate.futures.exchange_order_id==future.order_id
            || candidate.expected_qty_e12==0
            || expected_e18!=original_short_e18
            || original_short_e18==0
            || confirmed_spot_e18!=original_short_e18.checked_add(remaining)
                .ok_or(deny("unhedged residual overflow"))?
            || opened.exposed_spot_unhedged_atoms_e18!=remaining
            || candidate.created_at_ms<=opened.captured_at_ms
            || candidate.created_at_ms.saturating_sub(opened.captured_at_ms)>600_000
            || candidate.created_at_ms<entry.plan.created_at_ms{
            return Err(deny("shadow close IDs not bound to original partial opening"))
        }
        Ok(Self{
            original_position_key_sha256:original.position_key().into(),
            original_policy_sha256:original.policy_sha256().into(),
            opening_dual_link_sha256:digest(link)?,
            source_native_generation:source_generation,
            observed_at_ms:candidate.created_at_ms,
            requested_qty_e12:candidate.expected_qty_e12,
            original_spot_open_id:spot.spot_order_id.clone(),
            original_futures_open_id:future.order_id.clone(),
            spot_close_client_id:candidate.spot.client_id.clone(),
            spot_close_order_id:candidate.spot.exchange_order_id.clone(),
            futures_close_client_id:candidate.futures.client_id.clone(),
            futures_close_order_id:candidate.futures.exchange_order_id.clone(),
            leftover_spot_atoms_e18:remaining,
            cannot_fully_close_original_spot:remaining>0,
            verified_close_order_receipts:false,
            may_send_order:false,may_verify_closed:false,may_release_cooldown:false,
        })
    }
    pub(crate) fn to_candidate(&self,e:&NativeEntryAuthority)->DualCloseCandidate{
        DualCloseCandidate{
            position_identity:self.original_position_key_sha256.clone(),
            base:e.plan.base.clone(),
            created_at_ms:self.observed_at_ms,
            expected_qty_e12:self.requested_qty_e12,
            spot:CloseOrderIdentity{
                symbol:e.plan.spot_symbol.clone(),
                client_id:self.spot_close_client_id.clone(),
                exchange_order_id:self.spot_close_order_id.clone(),
            },
            futures:CloseOrderIdentity{
                symbol:e.plan.futures_symbol.clone(),
                client_id:self.futures_close_client_id.clone(),
                exchange_order_id:self.futures_close_order_id.clone(),
            }
        }
    }
    pub fn verify(
        &self,e:&NativeEntryAuthority,current_generation:u64
    )->Result<(),NativeStateError>{
        if current_generation<=self.source_native_generation
            ||self.original_position_key_sha256.len()!=64
            ||self.original_policy_sha256!=e.origin.as_ref()
                .ok_or(deny("origin erased"))?.policy_sha256()
            ||self.cannot_fully_close_original_spot
                !=(self.leftover_spot_atoms_e18>0)
            ||self.verified_close_order_receipts
            ||self.may_send_order||self.may_verify_closed||self.may_release_cooldown
            ||Self::derive(e,self.source_native_generation,&self.to_candidate(e))?!=*self{
            return Err(deny("shadow close seal forgery/overclaimed authority"))
        }
        Ok(())
    }
}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum ShadowCloseFenceWrite {Recorded,Idempotent}
impl NativeStateStore {
    /// Stores only untrusted proposed CLOSE order identities behind the
    /// original configured Native-entry state. No lifecycle mutation/permit.
    pub fn record_shadow_close_identity(
        &self,expected_generation:u64,candidate:&DualCloseCandidate
    )->Result<(NativeAuthoritativeState,ShadowCloseFenceWrite),NativeStateError>{
        let snapshot=self.load()?.ok_or(NativeStateError::Generation)?;
        if let Some(e)=snapshot.entry.as_ref(){
            if let Some(existing)=e.shadow_close_identity.as_ref(){
                existing.verify(e,snapshot.generation)?;
                let prior=existing.to_candidate(e);
                if prior==*candidate
                    &&(snapshot.generation==expected_generation
                        ||expected_generation.checked_add(1)==Some(snapshot.generation)){
                    return Ok((snapshot,ShadowCloseFenceWrite::Idempotent))
                }
                return Err(deny("shadow close candidate conflicts/stale"))
            }
        }
        if snapshot.generation!=expected_generation{
            return Err(NativeStateError::Generation)
        }
        let (saved,_)=self.transact(expected_generation,|state|{
            if state.active.is_some(){return Err(deny("cannot use native active authority"))}
            let e=state.entry.as_mut().ok_or(deny("configured opening not present"))?;
            if e.shadow_close_identity.is_some(){
                return Err(deny("duplicate shadow close intent"))
            }
            e.shadow_close_identity=Some(NativeShadowCloseIdentityFence::derive(
                e,state.generation,candidate)?);
            Ok(())
        })?;
        Ok((saved,ShadowCloseFenceWrite::Recorded))
    }
}
