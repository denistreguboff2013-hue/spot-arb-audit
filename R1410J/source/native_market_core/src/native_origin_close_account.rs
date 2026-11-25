//! R32D6C8B2B2B4C3: read-only BOUNDED post-close account window chained
//! to B3/B4B/B4C1/B4C2 in the same native owner-CAS durable entry.
//! This is NOT a proof of complete lifetime funding, cross-venue atomicity
//! or an actionable VerifiedClosed. No real order submission.
use crate::{
    authoritative_state::{NativeEntryAuthority,NativeEntryPhase,
        NativeAuthoritativeState,NativeStateError,NativeStateStore},
    native_close_readonly::DualCloseReadOnlyCandidate,
    native_close_window::{audit_close_source_window_once,CloseSourceError,CloseSourceObservation},
    private_https::HttpExecutor,
    private_rest::{OkxCredentials,BinanceCredentials,OkxDomain},
};
use openssl::sha::sha256;
use serde::{Serialize,Deserialize};

fn no(s:&'static str)->NativeStateError{NativeStateError::Invalid(s)}
fn hash<T:Serialize>(d:&T)->Result<String,NativeStateError>{
    Ok(sha256(&serde_json::to_vec(d).map_err(|_|NativeStateError::Json)?)
        .iter().map(|b|format!("{b:02x}")).collect())
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum OriginCloseAccountError {
    State(NativeStateError),
    Source(CloseSourceError),
    NonFlatOriginalOpening,
    SourceMismatch,
}
impl From<NativeStateError> for OriginCloseAccountError{
    fn from(v:NativeStateError)->Self{Self::State(v)}
}
impl From<CloseSourceError> for OriginCloseAccountError{
    fn from(v:CloseSourceError)->Self{Self::Source(v)}
}

/// An informational PASS of the bounded audit window, with explicit
/// irreducible NOT-PROVEN flags. The original partial scenario cannot
/// produce this (residual BNT is nonzero).
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
pub struct NativeShadowCloseAccountWindow {
    original_position_key_sha256:String,
    original_policy_sha256:String,
    original_dual_opening_sha256:String,
    original_close_receipt_sha256:String,
    source_generation:u64,
    observed_at_ms:u64,
    bounds_sha256:String,
    spot_base_bills:usize,
    spot_quote_bills:usize,
    futures_income_rows:usize,
    futures_funding_rows:usize,
    futures_close_trade_rows:usize,
    account_snapshot_reads:usize,
    observed_funding_e12:i128,
    original_unmatched_spot_e18:u128,
    original_unmatched_futures_e18:u128,
    /// These remain *false* even if the bounded source audit succeeds.
    original_exchange_lifetime_history_complete:bool,
    original_funding_lifetime_complete:bool,
    cross_venue_atomicity_proven:bool,
    may_send_order:bool,
    may_mark_verified_closed:bool,
    may_release_real_cooldown:bool,
}
impl NativeShadowCloseAccountWindow{
    pub fn position_key(&self)->&str{&self.original_position_key_sha256}
    pub fn source_generation(&self)->u64{self.source_generation}
    pub(crate) fn observation_time_ms(&self)->u64{self.observed_at_ms}
    pub fn account_reads(&self)->usize{self.account_snapshot_reads}
    pub fn funding_observed_e12(&self)->i128{self.observed_funding_e12}
    pub fn counts(&self)->(usize,usize,usize,usize,usize){
        (self.spot_base_bills,self.spot_quote_bills,self.futures_income_rows,
         self.futures_funding_rows,self.futures_close_trade_rows)
    }
    pub fn may_send_order(&self)->bool{self.may_send_order}
    pub fn verified_closed(&self)->bool{self.may_mark_verified_closed}
    pub fn may_release_cooldown(&self)->bool{self.may_release_real_cooldown}
    pub fn entire_lifetime_funding_proven(&self)->bool{self.original_funding_lifetime_complete}
    pub fn exchange_global_complete(&self)->bool{self.original_exchange_lifetime_history_complete}
    fn validate_ancestry(e:&NativeEntryAuthority,generation:u64)
        ->Result<(),NativeStateError>{
        let dual=e.origin_dual_opening_link.as_ref()
            .ok_or(no("original native two-leg opening evidence absent"))?;
        let receipt=e.shadow_close_receipt.as_ref()
            .ok_or(no("persisted close receipts absent"))?;
        let proposal=e.shadow_close_identity.as_ref()
            .ok_or(no("original shadow close proposal absent"))?;
        dual.verify(e,generation)?;
        receipt.verify(e,generation)?;
        proposal.verify(e,generation)?;
        let open=e.prospective_joint_evidence.as_ref().ok_or(no("opening window absent"))?;
        let cap=proposal.request_quantity_e12()
            .checked_mul(1_000_000).ok_or(no("close volume overflow"))?;
        // Conservative true-flat prerequisite. B4B synthetic 0.09 OKX /
        // 0.04 Binance MUST be rejected before any exchange GET.
        if e.phase!=NativeEntryPhase::RecoveryRequired
            ||dual.exposure_remaining_e18()!=0
            ||receipt.residual_spot_e18()!=0
            ||proposal.residual_spot_e18()!=0
            ||open.exposed_spot_unhedged_atoms_e18!=0
            ||dual.futures_short_e18()!=cap
            ||open.spot_available_net_atoms_e18!=cap
            ||e.spot_filled_atoms_e18!=cap
            ||e.futures_filled_atoms_e18!=0 // source used recovery checkpoint, not hedged authority
        {
            return Err(no("opening not certified full-hedge and flat-capable"))
        }
        Ok(())
    }
    fn make(e:&NativeEntryAuthority,generation:u64,now:u64,
            source:&CloseSourceObservation)->Result<Self,NativeStateError>{
        Self::validate_ancestry(e,generation)?;
        let origin=e.origin.as_ref().ok_or(no("origin missing"))?;
        let dual=e.origin_dual_opening_link.as_ref().ok_or(no("dual missing"))?;
        let receipt=e.shadow_close_receipt.as_ref().ok_or(no("close receipts missing"))?;
        if now==0
            ||now<e.shadow_close_identity.as_ref().ok_or(no("close proposal missing"))?
                .to_candidate(e).created_at_ms
            ||source.account_reads()!=8
            ||source.counts().0==0||source.counts().1==0
            ||source.counts().2==0||source.counts().3==0||source.counts().4==0
            ||source.full_account_history_proven()
            ||source.original_native_position_ancestry_proven()
            ||source.funding_lifetime_complete()
            ||source.cross_venue_atomicity_proven()
            ||source.may_mark_verified_closed()||source.may_send_order()
            ||source.may_start_real_cooldown(){
            return Err(no("bounded close source observation overclaims authority"))
        }
        let counts=source.counts();
        let receipt_sha=hash(receipt)?;
        let dual_sha=hash(dual)?;
        let bounds_sha256=hash(&("B4C3_BOUNDED_ACCOUNT_WINDOW_V1",
            origin.position_key(),origin.policy_sha256(),&dual_sha,&receipt_sha,
            generation,now,counts,source.account_reads(),source.observed_funding_e12()))?;
        Ok(Self{
            original_position_key_sha256:origin.position_key().to_owned(),
            original_policy_sha256:origin.policy_sha256().to_owned(),
            original_dual_opening_sha256:dual_sha,
            original_close_receipt_sha256:receipt_sha,
            source_generation:generation,
            observed_at_ms:now,
            bounds_sha256,
            spot_base_bills:counts.0,spot_quote_bills:counts.1,
            futures_income_rows:counts.2,futures_funding_rows:counts.3,
            futures_close_trade_rows:counts.4,
            account_snapshot_reads:source.account_reads(),
            observed_funding_e12:source.observed_funding_e12(),
            original_unmatched_spot_e18:0,original_unmatched_futures_e18:0,
            original_exchange_lifetime_history_complete:false,
            original_funding_lifetime_complete:false,cross_venue_atomicity_proven:false,
            may_send_order:false,may_mark_verified_closed:false,may_release_real_cooldown:false,
        })
    }
    pub fn verify(&self,e:&NativeEntryAuthority,generation:u64)
        ->Result<(),NativeStateError>{
        Self::validate_ancestry(e,generation)?;
        let origin=e.origin.as_ref().ok_or(no("origin missing"))?;
        let dual=e.origin_dual_opening_link.as_ref().ok_or(no("dual missing"))?;
        let receipt=e.shadow_close_receipt.as_ref().ok_or(no("close receipts missing"))?;
        let counts=self.counts();
        let sha=hash(&("B4C3_BOUNDED_ACCOUNT_WINDOW_V1",
            origin.position_key(),origin.policy_sha256(),hash(dual)?,hash(receipt)?,
            self.source_generation,self.observed_at_ms,counts,
            self.account_snapshot_reads,self.observed_funding_e12))?;
        if generation<=self.source_generation
            ||self.source_generation<=receipt.source_generation()
            ||self.original_position_key_sha256!=origin.position_key()
            ||self.original_policy_sha256!=origin.policy_sha256()
            ||self.original_dual_opening_sha256!=hash(dual)?
            ||self.original_close_receipt_sha256!=hash(receipt)?
            ||self.bounds_sha256!=sha
            ||self.account_snapshot_reads!=8
            ||counts.0==0||counts.1==0||counts.2==0||counts.3==0||counts.4==0
            ||self.original_unmatched_spot_e18!=0
            ||self.original_unmatched_futures_e18!=0
            ||self.original_exchange_lifetime_history_complete
            ||self.original_funding_lifetime_complete
            ||self.cross_venue_atomicity_proven
            ||self.may_send_order||self.may_mark_verified_closed||self.may_release_real_cooldown{
            return Err(no("bounded close source proof altered or overclaims authority"))
        }
        Ok(())
    }
}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum OriginCloseAccountWrite{Recorded,Idempotent}
impl NativeStateStore{
    #[allow(clippy::too_many_arguments)]
    pub fn audit_and_record_shadow_close_account_window<E:HttpExecutor>(
        &self,expected_generation:u64,receipt:&DualCloseReadOnlyCandidate,
        http:&E,okx:&OkxCredentials,bin:&BinanceCredentials,
        now_ms:u64,domain:OkxDomain,recv_window_ms:u64,page_limit:u16,
    )->Result<(NativeAuthoritativeState,OriginCloseAccountWrite),OriginCloseAccountError>{
        let old=self.load()?.ok_or(OriginCloseAccountError::SourceMismatch)?;
        if old.generation!=expected_generation{
            return Err(OriginCloseAccountError::State(NativeStateError::Generation))
        }
        let e=old.entry.as_ref().ok_or(OriginCloseAccountError::SourceMismatch)?;
        // Must precede ALL network GET calls, even mocked ones.
        match NativeShadowCloseAccountWindow::validate_ancestry(e,old.generation){
            Ok(())=>{},
            Err(_)=>return Err(OriginCloseAccountError::NonFlatOriginalOpening),
        }
        let closed=e.shadow_close_receipt.as_ref()
            .ok_or(OriginCloseAccountError::SourceMismatch)?;
        if !closed.exact_receipt_matches(receipt){
            return Err(OriginCloseAccountError::SourceMismatch)
        }
        if let Some(previous)=e.shadow_close_account_window.as_ref(){
            previous.verify(e,old.generation)?;
            return Ok((old,OriginCloseAccountWrite::Idempotent))
        }
        let proposed=e.shadow_close_identity.as_ref()
            .ok_or(OriginCloseAccountError::SourceMismatch)?;
        let candidate=proposed.to_candidate(e);
        // The old bounded GET validator rejects foreign trades, incomplete
        // pagination, inconsistent funding, nonflat balances, stale/moving
        // snapshots and all open futures orders.
        let observed=audit_close_source_window_once(
            http,&candidate,receipt,okx,bin,
            now_ms,domain,recv_window_ms,page_limit)?;
        // Audit completed outside the owner lock. CAS and exact original
        // evidence still required; stale generation fails without writes.
        let (next,_)=self.transact(expected_generation,|state|{
            if state.active.is_some(){return Err(no("native active still forbidden"))}
            let e=state.entry.as_mut().ok_or(no("original entry absent"))?;
            let proposal=e.shadow_close_identity.as_ref()
                .ok_or(no("original shadow close intent absent"))?;
            if proposal.to_candidate(e)!=candidate{
                return Err(no("source changed during bounded GET window"))
            }
            let proof=NativeShadowCloseAccountWindow::make(
                e,state.generation,now_ms,&observed)?;
            if let Some(previous)=e.shadow_close_account_window.as_ref(){
                if previous!=&proof{
                    return Err(no("previous bounded account proof conflict"))
                }
            }else{
                e.shadow_close_account_window=Some(proof)
            }
            Ok(())
        })?;
        Ok((next,OriginCloseAccountWrite::Recorded))
    }
}
