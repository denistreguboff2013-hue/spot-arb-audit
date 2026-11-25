//! MICRO1: quarantine ambiguous Binance futures execution after a timeout or
//! partial/untrusted HTTP response. No real network, fills or new permits.
use crate::authoritative_state::{NativeAuthoritativeState,NativeEntryAuthority,
    NativeEntryPhase,NativeStateError,NativeStateStore};
use crate::execution::IntentState;
use crate::private_rest::{OrderObservation,OkxCredentials,BinanceCredentials,OkxDomain};
use crate::private_https::HttpExecutor;
use crate::native_entry_recovery::{
    probe_native_entry_readonly_once,probe_native_entry_fills_readonly_once,
    EntryProbeReport,EntryFillDetailReport,EntryProbeVerdict,EntryProbeError,
};
use openssl::sha::sha256;
use serde::{Serialize,Deserialize};

fn deny(msg:&'static str)->NativeStateError{NativeStateError::Invalid(msg)}
fn digest<T:Serialize>(v:&T)->Result<String,NativeStateError>{
    let data=serde_json::to_vec(v).map_err(|_|NativeStateError::Json)?;
    Ok(sha256(&data).iter().map(|x|format!("{x:02x}")).collect())
}
fn id_ok(x:&str)->bool{!x.is_empty()&&x.len()<=96&&x.bytes()
    .all(|b|b.is_ascii_alphanumeric()||b==b'-'||b==b'_'||b==b'.')}
fn order_ok(x:&str)->bool{!x.is_empty()&&x.len()<=40&&x.bytes().all(|b|b.is_ascii_digit())}

#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
pub struct UncertainFuturesResponse {
    pub intent_id:String,
    pub client_id:String,
    pub exchange_order_id:Option<String>,
    pub reported_qty_e18:Option<u128>,
    pub reported_avg_price_e12:Option<u128>,
    pub terminal_reported:bool,
    pub observed_at_ms:u64,
}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
pub struct NativeFuturesUncertainQuarantine {
    source_generation:u64,
    source_identity_hash:String,
    original_safety_generation:u64,
    response:UncertainFuturesResponse,
    spot_original_filled_e18:u128,
    futures_original_confirmed_e18:u128,
    overhedge_suspected:bool,
    exchange_trade_ids_confirmed:bool,
    futures_fees_confirmed:bool,
    may_send_order:bool,
    may_credit_fill:bool,
    may_promote_hedged:bool,
    may_verify_closed:bool,
}
impl NativeFuturesUncertainQuarantine {
    pub fn reported_qty_e18(&self)->Option<u128>{self.response.reported_qty_e18}
    pub fn source_generation(&self)->u64{self.source_generation}
    pub fn overhedge_suspected(&self)->bool{self.overhedge_suspected}
    pub fn may_send_order(&self)->bool{self.may_send_order}
    pub fn may_credit_fill(&self)->bool{self.may_credit_fill}
    pub fn confirmed_trades(&self)->bool{self.exchange_trade_ids_confirmed}
    pub fn may_promote_hedged(&self)->bool{self.may_promote_hedged}
    pub fn reported_order_id(&self)->Option<&str>{self.response.exchange_order_id.as_deref()}
    pub fn recorded_client_id(&self)->&str{&self.response.client_id}
    pub(crate) fn validate_get_checkpoint(
        &self,e:&NativeEntryAuthority,
        cp:&crate::authoritative_state::NativeEntryRecoveryCheckpoint,
    )->Result<(),NativeStateError>{
        use crate::authoritative_state::{NativeEntryLeg,NativeEntryRecoveryQuality};
        let qty=cp.ledger.filled_qty_atoms().checked_mul(1_000_000)
            .ok_or(deny("checkpoint futures quantity overflow"))?;
        let suspected_order=self.response.exchange_order_id.as_deref();
        if cp.leg!=NativeEntryLeg::Futures
            ||cp.quality!=NativeEntryRecoveryQuality::FuturesPriorSpotUnverified
            ||cp.client_id!=self.response.client_id
            ||cp.intent_id!=self.response.intent_id
            ||suspected_order.map_or(false,|id|id!=cp.order_id)
            ||cp.ledger.client_order_id!=self.response.client_id
            ||cp.ledger.exchange_order_id.as_deref()!=Some(cp.order_id.as_str())
            ||cp.ledger.venue!="binance"||cp.ledger.side!="sell"
            ||cp.ledger.symbol!=e.plan.futures_symbol
            ||!cp.ledger.terminal||!cp.ledger.fee_evidence_complete
            ||cp.ledger.fills.is_empty()||qty==0
            ||qty>self.spot_original_filled_e18
            ||cp.ledger.fee_cost_quote_atoms<0
            ||cp.ledger.fills.values().any(|fill|
                 fill.trade_time_ms.is_none()||fill.fee_cost_quote_atoms.is_none()
                 ||fill.qty_atoms==0||fill.price_atoms==0)
{
            return Err(deny("quarantined signed GET checkpoint foreign or unsafe"))
        }
        Ok(())
    }

    fn source_hash(e:&NativeEntryAuthority)->Result<String,NativeStateError>{
        let spot=e.spot_prehedge_evidence.as_ref().ok_or(deny("signed spot source missing"))?;
        let bills=e.spot_prehedge_bills_evidence.as_ref().ok_or(deny("spot cash bills missing"))?;
        digest(&(&e.plan,&e.policy_binding,&e.origin,spot,bills,
            &e.intents.iter().map(|i|(&i.id,&i.client_order_id)).collect::<Vec<_>>()))
    }
    fn response_valid(e:&NativeEntryAuthority,r:&UncertainFuturesResponse)
        ->Result<(),NativeStateError>{
        if !id_ok(&r.intent_id)||!id_ok(&r.client_id)
            ||r.exchange_order_id.as_deref().map_or(false,|x|!order_ok(x))
            ||r.reported_qty_e18==Some(0)||r.reported_avg_price_e12==Some(0)
            ||r.observed_at_ms==0||r.observed_at_ms<e.plan.created_at_ms
            ||e.spot_filled_atoms_e18==0||e.futures_filled_atoms_e18!=0{
            return Err(deny("malformed or unbound uncertain futures response"))
        }
        Ok(())
    }
    fn create(e:&NativeEntryAuthority,gen:u64,r:&UncertainFuturesResponse)
        ->Result<Self,NativeStateError>{
        Self::response_valid(e,r)?;
        if e.phase!=NativeEntryPhase::FuturesPrepared
            ||e.intents.iter().filter(|i|i.id==r.intent_id
                &&i.client_order_id.as_deref()==Some(r.client_id.as_str())
                &&i.state==IntentState::Prepared).count()!=1{
            return Err(deny("no matching prepared futures leg to quarantine"))
        }
        let exposure=e.spot_filled_atoms_e18.checked_sub(e.futures_filled_atoms_e18)
            .ok_or(deny("entry already overhedged"))?;
        Ok(Self{
            source_generation:gen,source_identity_hash:Self::source_hash(e)?,
            original_safety_generation:e.plan.safety_generation,
            response:r.clone(),spot_original_filled_e18:e.spot_filled_atoms_e18,
            futures_original_confirmed_e18:e.futures_filled_atoms_e18,
            overhedge_suspected:r.reported_qty_e18.map_or(false,|q|q>exposure),
            exchange_trade_ids_confirmed:false,futures_fees_confirmed:false,
            may_send_order:false,may_credit_fill:false,
            may_promote_hedged:false,may_verify_closed:false,
        })
    }
    pub fn verify(&self,e:&NativeEntryAuthority,gen:u64)->Result<(),NativeStateError>{
        Self::response_valid(e,&self.response)?;
        let intent=e.intents.iter().find(|i|i.id==self.response.intent_id
            &&i.client_order_id.as_deref()==Some(self.response.client_id.as_str()));
        if gen<=self.source_generation||e.phase!=NativeEntryPhase::RecoveryRequired
            ||intent.map(|x|x.state)!=Some(IntentState::Unknown)
            ||self.source_identity_hash!=Self::source_hash(e)?
            ||self.original_safety_generation!=e.plan.safety_generation
            ||self.spot_original_filled_e18!=e.spot_filled_atoms_e18
            ||self.futures_original_confirmed_e18!=e.futures_filled_atoms_e18
            ||self.futures_original_confirmed_e18!=0
            ||self.overhedge_suspected!=self.response.reported_qty_e18
                .map_or(false,|q|q>self.spot_original_filled_e18)
            ||self.exchange_trade_ids_confirmed||self.futures_fees_confirmed
            ||self.may_send_order||self.may_credit_fill
            ||self.may_promote_hedged||self.may_verify_closed{
            return Err(deny("quarantined response forged or credited as confirmed fill"))
        }
        Ok(())
    }
}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum FuturesQuarantineWrite {Recorded,Idempotent}
impl NativeStateStore {
    /// Fail-stop: save response uncertainty under owner flock + CAS + fsync.
    /// Never treat reported quantities as confirmed or send further orders.
    pub fn quarantine_uncertain_futures_execution(
        &self,expected_generation:u64,r:&UncertainFuturesResponse
    )->Result<(NativeAuthoritativeState,FuturesQuarantineWrite),NativeStateError>{
        let previous=self.load()?.ok_or(NativeStateError::Generation)?;
        if previous.generation!=expected_generation{return Err(NativeStateError::Generation)}
        if let Some(e)=previous.entry.as_ref(){
            if let Some(saved)=e.futures_uncertain_quarantine.as_ref(){
                saved.verify(e,previous.generation)?;
                if &saved.response==r{return Ok((previous,FuturesQuarantineWrite::Idempotent))}
                return Err(deny("conflicting futures uncertainty report"))
            }
        }
        let (next,_)=self.transact(expected_generation,|state|{
            if state.active.is_some(){return Err(deny("cannot quarantine active native position"))}
            let e=state.entry.as_mut().ok_or(deny("native entry missing"))?;
            if e.futures_uncertain_quarantine.is_some(){
                return Err(deny("racing futures uncertainty report"))
            }
            let proof=NativeFuturesUncertainQuarantine::create(e,state.generation,r)?;
            let intent=e.intents.iter_mut().find(|x|x.id==r.intent_id
                &&x.client_order_id.as_deref()==Some(r.client_id.as_str()))
                .ok_or(deny("prepared futures intent missing"))?;
            intent.state=IntentState::Unknown;
            e.phase=NativeEntryPhase::RecoveryRequired;
            e.futures_uncertain_quarantine=Some(proof);
            Ok(())
        })?;
        Ok((next,FuturesQuarantineWrite::Recorded))
    }
}

/// Converts a placed futures order's UNTRUSTED response to a durable UNKNOWN
/// under original prepared intent/client ID. Reported fill/avg/order ID are
/// only hints; never exchange evidence and never credited to economics.
/// A malformed/mismatching reply is conservatively recorded as UNKNOWN.
impl NativeStateStore {
    pub fn quarantine_futures_dispatch_observation(
        &self,expected_generation:u64,reply:&OrderObservation,now_ms:u64,
    )->Result<(NativeAuthoritativeState,FuturesQuarantineWrite),NativeStateError>{
        let target=self.pending_native_entry_query(expected_generation)?;
        if target.leg!=crate::authoritative_state::NativeEntryLeg::Futures {
            return Err(deny("dispatch quarantine requires original Binance futures intent"))
        }
        let matched=reply.client_order_id.as_deref()==Some(target.client_id.as_str());
        let parsed=if matched{
            reply.filled.as_deref()
                .and_then(|q|crate::fill_economics::decimal_to_atoms(q).ok())
                .and_then(|qty|qty.checked_mul(1_000_000))
                .filter(|q|*q>0)
        }else{None};
        let order=if matched{
            reply.order_id.as_deref().filter(|s|order_ok(s)).map(str::to_owned)
        }else{None};
        let uncertain=UncertainFuturesResponse{
            intent_id:target.intent_id,client_id:target.client_id,
            exchange_order_id:order,reported_qty_e18:parsed,
            reported_avg_price_e12:None,terminal_reported:reply.terminal,
            observed_at_ms:now_ms,
        };
        self.quarantine_uncertain_futures_execution(expected_generation,&uncertain)
    }
}

/// Results of existing signed GET account+order and (conditionally) trade/
/// commission history, *after* MICRO1 quarantine. Merely diagnostic.
#[derive(Debug)]
pub struct FuturesQuarantineGetReport{
    pub order_account:EntryProbeReport,
    pub detailed_trades:Option<EntryFillDetailReport>,
    pub may_send_order:bool,
    pub may_promote_hedged:bool,
}
#[derive(Debug)]
pub enum FuturesQuarantineGetError{
    State(NativeStateError),
    Read(EntryProbeError),
    Invalid(&'static str),
}
impl From<NativeStateError> for FuturesQuarantineGetError{
    fn from(x:NativeStateError)->Self{Self::State(x)}
}
impl From<EntryProbeError> for FuturesQuarantineGetError{
    fn from(x:EntryProbeError)->Self{Self::Read(x)}
}
/// Never sends POST. Reuses the ten signed, independent account/order GETs
/// and the separate trade-by-trade GET from R32D2. All private requests are
/// already constrained to signed HttpMethod::Get + !is_mutation.
/// Does NOT persist fills, dismiss uncertainty, reissue the futures order,
/// create a native Hedged position, or accept incomplete account provenance.
#[allow(clippy::too_many_arguments)]
pub fn probe_quarantined_futures_entry_readonly_once<E:HttpExecutor>(
    exec:&E,store:&NativeStateStore,expected_generation:u64,
    okx:&OkxCredentials,binance:&BinanceCredentials,now_ms:u64,
    okx_domain:OkxDomain,binance_recv_window_ms:u64,
)->Result<FuturesQuarantineGetReport,FuturesQuarantineGetError>{
    let snapshot=store.load()?.ok_or(NativeStateError::Generation)?;
    if snapshot.generation!=expected_generation||snapshot.active.is_some(){
        return Err(FuturesQuarantineGetError::State(NativeStateError::Generation))
    }
    let entry=snapshot.entry.as_ref().ok_or(FuturesQuarantineGetError::Invalid(
        "original entry absent"))?;
    let witness=entry.futures_uncertain_quarantine.as_ref().ok_or(
        FuturesQuarantineGetError::Invalid("no durable futures quarantine"))?;
    witness.verify(entry,snapshot.generation)?;
    let original=store.pending_native_entry_query(expected_generation)?;
    if original.leg!=crate::authoritative_state::NativeEntryLeg::Futures
        ||original.client_id!=witness.recorded_client_id(){
        return Err(FuturesQuarantineGetError::Invalid(
            "quarantined Binance client id differs from unresolved order"))
    }
    let report=probe_native_entry_readonly_once(
        exec,store,expected_generation,okx,binance,now_ms,
        okx_domain,binance_recv_window_ms)?;
    if report.target!=original || report.may_send_order{
        return Err(FuturesQuarantineGetError::Invalid(
            "quarantine source moved during signed GET"))
    }
    let detail=if report.verdict==EntryProbeVerdict::TerminalCandidateNeedsFillFeesAndInventoryTruth{
        Some(probe_native_entry_fills_readonly_once(
            exec,store,&report,okx,binance,now_ms,
            okx_domain,binance_recv_window_ms)?)
    }else{None};
    let after=store.load()?.ok_or(NativeStateError::Generation)?;
    if after.generation!=expected_generation
        ||after.entry.as_ref().and_then(|e|e.futures_uncertain_quarantine.as_ref())!=Some(witness){
        return Err(FuturesQuarantineGetError::State(NativeStateError::Generation))
    }
    Ok(FuturesQuarantineGetReport{
        order_account:report,detailed_trades:detail,
        may_send_order:false,may_promote_hedged:false,
    })
}


/// Advice is deliberately NOT executable. The journaled checkpoint is a
/// read-only verified candidate; it never credits position or mints a permit.
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum FuturesQuarantineRecoveryAdvice {
    ManualReviewRequired,
    PartialHedgeManualDecision{
        verified_short_e18:u128,
        unmatched_spot_e18:u128,
    },
    FullHedgeNeedsIndependentAccountAndFundingAudit{
        candidate_qty_e18:u128,
    },
}
#[derive(Debug)]
pub enum FuturesQuarantineCheckpointError {
    State(NativeStateError),
    Unverified(&'static str),
}
impl From<NativeStateError> for FuturesQuarantineCheckpointError{
    fn from(e:NativeStateError)->Self{Self::State(e)}
}
impl NativeStateStore {
    /// Durable checkpoint from MICRO2's bounded read-only GET. The existing
    /// R32D3 owner lock commits the native checkpoint atomically. No new
    /// network requests or position mutation are issued.
    pub fn record_quarantined_futures_get_checkpoint(
        &self,report:FuturesQuarantineGetReport,
    )->Result<(NativeAuthoritativeState,crate::authoritative_state::NativeEntryRecoveryWrite,
        FuturesQuarantineRecoveryAdvice),FuturesQuarantineCheckpointError>{
        use crate::native_entry_recovery::EntryFillVerdict;
        if report.may_send_order||report.may_promote_hedged
            ||report.order_account.verdict
                !=EntryProbeVerdict::TerminalCandidateNeedsFillFeesAndInventoryTruth {
            return Err(FuturesQuarantineCheckpointError::Unverified(
                "account/order not verified for futures recovery"))
        }
        let detail=report.detailed_trades.ok_or(
            FuturesQuarantineCheckpointError::Unverified("no trade and fee evidence"))?;
        if detail.verdict!=EntryFillVerdict::PriorSpotFeeOriginUnverified
            ||detail.may_send_order{
            return Err(FuturesQuarantineCheckpointError::Unverified(
                "unsigned or incomplete futures fee evidence"))
        }
        let proof=detail.sealed_checkpoint.as_ref().ok_or(
            FuturesQuarantineCheckpointError::Unverified("signed GET checkpoint not sealed"))?;
        let current=self.load()?.ok_or(NativeStateError::Generation)?;
        if current.generation!=detail.source_generation
            ||current.generation!=report.order_account.target.generation
            ||current.active.is_some(){
            return Err(FuturesQuarantineCheckpointError::State(NativeStateError::Generation))
        }
        let e=current.entry.as_ref().ok_or(
            FuturesQuarantineCheckpointError::Unverified("native entry absent"))?;
        let quarantine=e.futures_uncertain_quarantine.as_ref().ok_or(
            FuturesQuarantineCheckpointError::Unverified("MICRO1 original missing"))?;
        quarantine.verify(e,current.generation)?;
        quarantine.validate_get_checkpoint(e,proof)?;
        if report.order_account.target.intent_id!=proof.intent_id
            ||report.order_account.target.client_id!=proof.client_id{
            return Err(FuturesQuarantineCheckpointError::Unverified(
                "signed GET target changed"))
        }
        let (saved,status)=self.record_native_entry_recovery_checkpoint(detail)?;
        let decision=saved.quarantined_futures_recovery_advice()?;
        Ok((saved,status,decision))
    }
}
impl NativeAuthoritativeState {
    pub fn quarantined_futures_recovery_advice(
        &self,
    )->Result<FuturesQuarantineRecoveryAdvice,NativeStateError>{
        let e=self.entry.as_ref().ok_or(deny("original entry absent"))?;
        let witness=e.futures_uncertain_quarantine.as_ref()
            .ok_or(deny("original uncertain future absent"))?;
        witness.verify(e,self.generation)?;
        // An anomalous placement overhedge hint remains a manual emergency,
        // even if a bounded query sees fewer fills for this one order.
        if witness.overhedge_suspected(){
            return Ok(FuturesQuarantineRecoveryAdvice::ManualReviewRequired)
        }
        if let Some(cp)=e.recovery_checkpoint.as_ref(){
            witness.validate_get_checkpoint(e,cp)?;
            let qty=cp.ledger.filled_qty_atoms().checked_mul(1_000_000)
                .ok_or(deny("futures candidate overflow"))?;
            let spot=e.spot_filled_atoms_e18;
            if qty<spot{
                Ok(FuturesQuarantineRecoveryAdvice::PartialHedgeManualDecision{
                    verified_short_e18:qty,unmatched_spot_e18:spot-qty,
                })
            }else{
                Ok(FuturesQuarantineRecoveryAdvice::
                    FullHedgeNeedsIndependentAccountAndFundingAudit{
                        candidate_qty_e18:qty,
                    })
            }
        }else{Ok(FuturesQuarantineRecoveryAdvice::ManualReviewRequired)}
    }
}


// MICRO4: combine existing independently implemented C5 and C6 GET-source
// audits around the exact durable MICRO1->MICRO3 quarantined Binance order.
// Each stage commits its own owner-CAS fsynced checkpoint. A failed second
// stage leaves the first recorded but cannot mint any order/hedged authority.
#[derive(Debug)]
pub enum QuarantinedSupervisedAuditError {
    State(NativeStateError),
    Futures(crate::native_futures_entry_audit::FuturesEntryWindowError),
    Joint(crate::native_prospective_joint::ProspectiveJointError),
    Invalid(&'static str),
}
impl From<NativeStateError> for QuarantinedSupervisedAuditError{
    fn from(e:NativeStateError)->Self{Self::State(e)}
}
impl From<crate::native_futures_entry_audit::FuturesEntryWindowError>
    for QuarantinedSupervisedAuditError{
    fn from(e:crate::native_futures_entry_audit::FuturesEntryWindowError)->Self{
        Self::Futures(e)
    }
}
impl From<crate::native_prospective_joint::ProspectiveJointError>
    for QuarantinedSupervisedAuditError{
    fn from(e:crate::native_prospective_joint::ProspectiveJointError)->Self{
        Self::Joint(e)
    }
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum QuarantinedOperatorReview {
    ManualRecoveryRequired{
        observed_short_e18:u128,
        unmatched_spot_e18:u128,
        confirmed_trade_count:usize,
        confirmed_fee_e12:i128,
    },
    FurtherAccountAndLifetimeReviewRequired{
        observed_short_e18:u128,
        confirmed_trade_count:usize,
        confirmed_fee_e12:i128,
    },
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct QuarantinedSupervisedAssessment{
    pub generation:u64,
    pub review:QuarantinedOperatorReview,
    pub binance_account_brackets:u8,
    pub cross_venue_account_brackets:u8,
    pub may_send_order:bool,
    pub may_auto_hedge:bool,
    pub may_auto_unwind:bool,
    pub may_promote_hedged:bool,
    pub may_verify_closed:bool,
    pub globally_complete:bool,
    pub cross_venue_atomicity_proven:bool,
}
fn micro4_preflight(
    store:&NativeStateStore,expected_generation:u64,
    expect_futures_window:bool,
)->Result<NativeAuthoritativeState,QuarantinedSupervisedAuditError>{
    let s=store.load()?.ok_or(NativeStateError::Generation)?;
    if s.generation!=expected_generation||s.active.is_some(){
        return Err(QuarantinedSupervisedAuditError::State(NativeStateError::Generation))
    }
    let e=s.entry.as_ref()
        .ok_or(QuarantinedSupervisedAuditError::Invalid("native entry absent"))?;
    let q=e.futures_uncertain_quarantine.as_ref()
        .ok_or(QuarantinedSupervisedAuditError::Invalid("original quarantine missing"))?;
    q.verify(e,s.generation)?;
    let cp=e.recovery_checkpoint.as_ref()
        .ok_or(QuarantinedSupervisedAuditError::Invalid("MICRO3 fills/fees not sealed"))?;
    q.validate_get_checkpoint(e,cp)?;
    if e.phase!=NativeEntryPhase::RecoveryRequired
        ||e.spot_prehedge_evidence.is_none()
        ||e.spot_prehedge_bills_evidence.is_none()
        ||e.futures_entry_window_evidence.is_some()!=expect_futures_window
        ||e.prospective_joint_evidence.is_some(){
        return Err(QuarantinedSupervisedAuditError::Invalid(
            "quarantined recovery sources missing, repeated or out of order"))
    }
    if expect_futures_window{
        let c5=e.futures_entry_window_evidence.as_ref().unwrap();
        if c5.futures_intent_id!=cp.intent_id
            ||c5.futures_client_id!=cp.client_id
            ||c5.futures_order_id!=cp.order_id
            ||c5.trade_ids!=cp.ledger.fills.keys().cloned().collect::<Vec<_>>()
            ||c5.signed_quote_fee_cost_atoms_e12!=cp.ledger.fee_cost_quote_atoms
            ||c5.may_send_order||c5.may_promote_hedged{
            return Err(QuarantinedSupervisedAuditError::Invalid(
                "Binance independently audited C5 origin changed"))
        }
    }
    Ok(s)
}
impl NativeStateStore {
    /// Stage 1: 27 signed synthetic GETs in existing R32D6C5 auditor
    /// (Binance symbol-wide executions between two account/income checks).
    /// No mutation HTTP; only the C5 proof is fsynced locally.
    pub fn audit_and_record_quarantined_binance_window_once<E:HttpExecutor>(
        &self,exec:&E,expected_generation:u64,
        binance:&BinanceCredentials,now_ms:u64,recv_window_ms:u64,
        income_limit:u16,trade_limit:u16,
    )->Result<NativeAuthoritativeState,QuarantinedSupervisedAuditError>{
        micro4_preflight(self,expected_generation,false)?;
        let audit=crate::native_futures_entry_audit::audit_futures_entry_window_once(
            exec,self,expected_generation,binance,now_ms,recv_window_ms,
            income_limit,trade_limit)?;
        let (saved,_)=self.record_native_futures_entry_window(audit)?;
        micro4_preflight(self,saved.generation,true)?;
        Ok(saved)
    }

    /// Stage 2: 29 more synthetic signed GETs: fresh OKX available cash
    /// both sides of two Binance account/income reads and symbol-wide trades.
    /// Can be reattempted after a C6 failure without re-doing/potentially
    /// overwriting the already durable C5 Binance proof.
    #[allow(clippy::too_many_arguments)]
    pub fn audit_and_record_quarantined_joint_window_once<E:HttpExecutor>(
        &self,exec:&E,expected_generation:u64,
        okx:&OkxCredentials,binance:&BinanceCredentials,now_ms:u64,
        okx_domain:OkxDomain,recv_window_ms:u64,
        income_limit:u16,trade_limit:u16,
    )->Result<(NativeAuthoritativeState,QuarantinedSupervisedAssessment),
        QuarantinedSupervisedAuditError>{
        micro4_preflight(self,expected_generation,true)?;
        let audit=crate::native_prospective_joint::audit_prospective_joint_chain_once(
            exec,self,expected_generation,okx,binance,now_ms,okx_domain,
            recv_window_ms,income_limit,trade_limit)?;
        let (saved,_)=self.record_native_prospective_joint_window(audit)?;
        let review=saved.quarantined_supervised_assessment()?;
        Ok((saved,review))
    }

    /// Exactly stages 1 then 2; second-stage failures preserve stage 1.
    #[allow(clippy::too_many_arguments)]
    pub fn audit_and_record_quarantined_full_window_once<E:HttpExecutor>(
        &self,exec:&E,expected_generation:u64,
        okx:&OkxCredentials,binance:&BinanceCredentials,now_ms:u64,
        okx_domain:OkxDomain,recv_window_ms:u64,
        income_limit:u16,trade_limit:u16,
    )->Result<(NativeAuthoritativeState,QuarantinedSupervisedAssessment),
        QuarantinedSupervisedAuditError>{
        let first=self.audit_and_record_quarantined_binance_window_once(
            exec,expected_generation,binance,now_ms,recv_window_ms,income_limit,trade_limit)?;
        self.audit_and_record_quarantined_joint_window_once(
            exec,first.generation,okx,binance,now_ms,okx_domain,
            recv_window_ms,income_limit,trade_limit)
    }
}
impl NativeAuthoritativeState {
    /// NO-AUTO-ORDER advisory; quantities merely correlate in finite GET
    /// windows. Never a native Hedged position or lifetime funding evidence.
    pub fn quarantined_supervised_assessment(
        &self,
    )->Result<QuarantinedSupervisedAssessment,NativeStateError>{
        let e=self.entry.as_ref().ok_or(deny("original entry missing"))?;
        let q=e.futures_uncertain_quarantine.as_ref()
            .ok_or(deny("quarantined source missing"))?;
        q.verify(e,self.generation)?;
        let cp=e.recovery_checkpoint.as_ref()
            .ok_or(deny("original per-trade checkpoint missing"))?;
        q.validate_get_checkpoint(e,cp)?;
        let fut=e.futures_entry_window_evidence.as_ref()
            .ok_or(deny("independent Binance audit missing"))?;
        let joint=e.prospective_joint_evidence.as_ref()
            .ok_or(deny("independent paired account audit missing"))?;
        let quantity=cp.ledger.filled_qty_atoms()
            .checked_mul(1_000_000).ok_or(deny("futures qty overflow"))?;
        let net=joint.spot_available_net_atoms_e18;
        if e.phase!=NativeEntryPhase::RecoveryRequired
            ||joint.source_generation>=self.generation
            ||fut.source_generation>=joint.source_generation
            ||joint.futures_window_source_generation!=fut.source_generation
            ||joint.futures_client_id!=cp.client_id
            ||joint.futures_intent_id!=cp.intent_id
            ||joint.futures_order_id!=cp.order_id
            ||joint.futures_trade_ids!=cp.ledger.fills.keys().cloned().collect::<Vec<_>>()
            ||joint.futures_fee_quote_atoms_e12!=cp.ledger.fee_cost_quote_atoms
            ||joint.futures_short_atoms_e18!=quantity
            ||joint.futures_short_atoms_e18!=fut.observed_short_atoms_e18
            ||net<quantity||joint.exposed_spot_unhedged_atoms_e18!=net-quantity
            ||!joint.okx_balance_bracket_matched
            ||!joint.binance_income_and_position_bracket_matched
            ||!joint.foreign_symbol_trades_absent_in_bounded_window
            ||!fut.exact_fills_commissions_and_symbol_history_matched
            ||!fut.income_position_wallet_bracket_matched
            ||joint.may_send_order||joint.may_promote_hedged
            ||fut.may_send_order||fut.may_promote_hedged
            ||joint.exchange_global_history_complete
            ||joint.cross_venue_exchange_snapshot_atomicity_proven
            ||fut.exchange_global_history_complete||fut.cross_venue_atomicity_proven{
            return Err(deny("MICRO4 signed multi-window original lineage mismatch"))
        }
        let review=if q.overhedge_suspected()||net>quantity{
            QuarantinedOperatorReview::ManualRecoveryRequired{
                observed_short_e18:quantity,unmatched_spot_e18:net-quantity,
                confirmed_trade_count:cp.ledger.fills.len(),
                confirmed_fee_e12:cp.ledger.fee_cost_quote_atoms,
            }
        }else{
            QuarantinedOperatorReview::FurtherAccountAndLifetimeReviewRequired{
                observed_short_e18:quantity,
                confirmed_trade_count:cp.ledger.fills.len(),
                confirmed_fee_e12:cp.ledger.fee_cost_quote_atoms,
            }
        };
        Ok(QuarantinedSupervisedAssessment{
            generation:self.generation,review,
            binance_account_brackets:fut.account_read_repetitions,
            cross_venue_account_brackets:2,
            may_send_order:false,may_auto_hedge:false,may_auto_unwind:false,
            may_promote_hedged:false,may_verify_closed:false,
            globally_complete:false,cross_venue_atomicity_proven:false,
        })
    }
}
