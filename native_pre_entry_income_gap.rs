//! MICRO5B2A — bounded Binance USD-M income provenance gap between the
//! original saved PRE-ENTRY wallet snapshot and first Native entry creation.
//! Two independent signed, unfiltered GET /fapi/v1/income requests must BOTH
//! return a valid empty JSON array. This does NOT cover every Binance SAPI
//! transfer, off-Futures account activity, posting delays or income AFTER
//! the entry. Neither balance equality nor this audit is live trade authority.
use crate::authoritative_state::{
    NativeAuthoritativeState,NativeEntryPhase,NativeStateError,NativeStateStore,
};
use crate::private_rest::{
    BinanceCredentials,build_binance_all_income_page_request,
    HttpMethod,RestBuildError,
};
use crate::private_https::{HttpExecutor,HttpsRequest,TransportError};
use openssl::sha::sha256;
use serde::{Serialize,Deserialize};
use serde_json::Value;

fn deny(x:&'static str)->NativeStateError{NativeStateError::Invalid(x)}
fn hash<T:Serialize>(v:&T)->Result<String,NativeStateError>{
    Ok(sha256(&serde_json::to_vec(v).map_err(|_|NativeStateError::Json)?)
        .iter().map(|b|format!("{b:02x}")).collect())
}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
pub struct NativePreEntryIncomeGapProof {
    source_generation:u64,
    baseline_sha256:String,
    wallet_bridge_sha256:String,
    window_begin_ms:u64,
    window_end_ms:u64,
    captured_at_ms:u64,
    unfiltered_income_snapshots:u8,
    both_empty_json_arrays:bool,
    income_api_access_verified:bool,
    all_futures_income_types_covered_by_query:bool,
    global_internal_external_transfers_complete:bool,
    cross_wallet_activity_proven_absent:bool,
    income_posting_finality_proven:bool,
    all_account_history_complete:bool,
    external_anti_rollback_proven:bool,
    may_send_order:bool,
    may_release_recovery:bool,
    may_promote_hedged:bool,
}
impl NativePreEntryIncomeGapProof {
    pub fn window(&self)->(u64,u64){(self.window_begin_ms,self.window_end_ms)}
    pub fn source_generation(&self)->u64{self.source_generation}
    pub fn may_send_order(&self)->bool{self.may_send_order}
    pub fn all_account_history_complete(&self)->bool{self.all_account_history_complete}
    pub fn transfer_history_proven(&self)->bool{self.global_internal_external_transfers_complete}
    fn source(s:&NativeAuthoritativeState)->Result<(String,String,u64,u64),NativeStateError>{
        let b=s.pre_entry_binance_baseline.as_ref()
            .ok_or(deny("pre-entry Binance signed wallet baseline missing"))?;
        b.validate(s)?;
        let e=s.entry.as_ref().ok_or(deny("original Native entry missing"))?;
        if e.phase!=NativeEntryPhase::RecoveryRequired||s.active.is_some(){
            return Err(deny("income gap only applies to a quarantined opening"))
        }
        let w=e.wallet_income_bridge.as_ref()
            .ok_or(deny("MICRO5B1 independently sealed wallet fee bridge missing"))?;
        w.verify(s)?;
        if e.futures_uncertain_quarantine.is_none()
            ||e.recovery_checkpoint.is_none()
            ||e.futures_entry_window_evidence.is_none()
            ||e.prospective_joint_evidence.is_none(){
            return Err(deny("missing original signed futures or cross-venue source"))
        }
        let begin=b.captured_at_ms();
        let end=e.plan.created_at_ms;
        if begin==0||end<begin||end-begin>60_000{
            return Err(deny("original Binance opening gap out of bounds"))
        }
        Ok((hash(b)?,hash(w)?,begin,end))
    }
    pub fn verify(&self,s:&NativeAuthoritativeState)->Result<(),NativeStateError>{
        let (baseline,bridge,start,end)=Self::source(s)?;
        if self.source_generation==0||self.source_generation>=s.generation
            ||self.baseline_sha256!=baseline||self.wallet_bridge_sha256!=bridge
            ||self.window_begin_ms!=start||self.window_end_ms!=end
            ||self.captured_at_ms<end
            ||self.captured_at_ms.saturating_sub(end)>180_000
            ||self.unfiltered_income_snapshots!=2
            ||!self.both_empty_json_arrays||!self.income_api_access_verified
            ||!self.all_futures_income_types_covered_by_query
            ||self.global_internal_external_transfers_complete
            ||self.cross_wallet_activity_proven_absent||self.income_posting_finality_proven
            ||self.all_account_history_complete||self.external_anti_rollback_proven
            ||self.may_send_order||self.may_release_recovery||self.may_promote_hedged{
            return Err(deny("original pre-entry Binance income gap source altered or authority forged"))
        }
        Ok(())
    }
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum NativePreEntryGapError{
    State(NativeStateError),
    Build(RestBuildError),
    Transport(TransportError),
    Invalid(&'static str),
}
impl From<NativeStateError> for NativePreEntryGapError{
    fn from(x:NativeStateError)->Self{Self::State(x)}
}
fn get_empty_income<E:HttpExecutor>(
    exec:&E,credentials:&BinanceCredentials,
    now_ms:u64,recv_ms:u64,begin_ms:u64,end_ms:u64,
)->Result<(),NativePreEntryGapError>{
    // First empty page is the endpoint's empty-result marker; no pagination
    // continuation is possible if zero rows were returned.
    let signed=build_binance_all_income_page_request(
        credentials,now_ms,recv_ms,begin_ms,end_ms,1,1000)
        .map_err(NativePreEntryGapError::Build)?;
    if signed.method!=HttpMethod::Get||signed.is_mutation{
        return Err(NativePreEntryGapError::Invalid("pre-entry income check not signed read-only"))
    }
    let response=exec.execute_request(&HttpsRequest::from_signed(&signed))
        .map_err(NativePreEntryGapError::Transport)?;
    if response.status!=200{
        return Err(NativePreEntryGapError::Invalid("Binance income API rejected"))
    }
    let parsed:Value=serde_json::from_str(&response.body)
        .map_err(|_|NativePreEntryGapError::Invalid("income response not valid JSON"))?;
    let list=parsed.as_array().ok_or(
        NativePreEntryGapError::Invalid("income response not an array"))?;
    if !list.is_empty(){
        // Includes all FUTURES income types, not just commissions or BNT.
        // Even a zero-amount or an apparently unrelated entry requires review.
        return Err(NativePreEntryGapError::Invalid(
            "some income/funding/transfer event exists before native opening"))
    }
    Ok(())
}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum NativePreEntryGapWrite {Recorded,Idempotent}
impl NativeStateStore {
    /// Independent DOUBLE signed GET on the exact original [wallet capture,
    /// Native entry] interval. Fail-closed on any event; local fsynced proof
    /// only; NEVER a live send/recovery/hedge/cooldown permit.
    pub fn audit_and_record_pre_entry_income_gap_once<E:HttpExecutor>(
        &self,exec:&E,expected_generation:u64,
        binance:&BinanceCredentials,now_ms:u64,recv_window_ms:u64,
    )->Result<(NativeAuthoritativeState,NativePreEntryGapWrite),NativePreEntryGapError>{
        let original=self.load()?.ok_or(NativeStateError::Generation)?;
        if original.generation!=expected_generation{
            return Err(NativePreEntryGapError::State(NativeStateError::Generation))
        }
        if let Some(proof)=original.entry.as_ref()
            .and_then(|e|e.pre_entry_income_gap_proof.as_ref()){
            proof.verify(&original)?;
            return Ok((original,NativePreEntryGapWrite::Idempotent))
        }
        let (original_baseline,original_bridge,begin,end)=
            NativePreEntryIncomeGapProof::source(&original)?;
        if now_ms<end||now_ms.saturating_sub(end)>180_000
            ||recv_window_ms==0||recv_window_ms>60_000{
            return Err(NativePreEntryGapError::Invalid(
                "stale clock, original opening, or recvWindow"))
        }
        get_empty_income(exec,binance,now_ms,recv_window_ms,begin,end)?;
        get_empty_income(exec,binance,now_ms,recv_window_ms,begin,end)?;
        let (saved,_)=self.transact(expected_generation,|s|{
            let (b,w,actual_begin,actual_end)=NativePreEntryIncomeGapProof::source(s)?;
            if b!=original_baseline||w!=original_bridge
                ||actual_begin!=begin||actual_end!=end
                ||s.entry.as_ref().unwrap().pre_entry_income_gap_proof.is_some(){
                return Err(deny("opening income gap source changed during signed GET"))
            }
            s.entry.as_mut().unwrap().pre_entry_income_gap_proof=Some(
                NativePreEntryIncomeGapProof{
                    source_generation:expected_generation,baseline_sha256:b,
                    wallet_bridge_sha256:w,
                    window_begin_ms:begin,window_end_ms:end,
                    captured_at_ms:now_ms,unfiltered_income_snapshots:2,
                    both_empty_json_arrays:true,income_api_access_verified:true,
                    all_futures_income_types_covered_by_query:true,
                    global_internal_external_transfers_complete:false,
                    cross_wallet_activity_proven_absent:false,
                    income_posting_finality_proven:false,
                    all_account_history_complete:false,external_anti_rollback_proven:false,
                    may_send_order:false,may_release_recovery:false,may_promote_hedged:false,
                });
            Ok(())
        })?;
        Ok((saved,NativePreEntryGapWrite::Recorded))
    }
}
