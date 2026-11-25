use crate::{
    execution::{ExecutionState,Intent,IntentState},
    execution_adapter::{authorize_exact,OrderIntent,Venue},
    fill_economics::{PositionEconomicsState,LegFillLedger},
    position_lifecycle::{BasicTruth,CloseLeg,CloseNetworkPlan,LifecycleState,MutationObservation,
        PositionLifecycle,PreparedClose,RecoveryEvidence,ExitTruth},
};
use openssl::sha::sha256;
use serde::{Deserialize,Serialize};
use serde_json::Value;
use std::{
    fs::{self,File,OpenOptions},
    io::{self,Read,Write},
    os::fd::AsRawFd,
    path::{Path,PathBuf},
};

pub const NATIVE_STATE_SCHEMA_VERSION:u32=1;
pub const NATIVE_QTY_SCALE_DIGITS:i32=18;

#[derive(Clone,Copy,Debug,PartialEq,Eq,Serialize,Deserialize)]
pub enum PositionProvenance{Native,LegacyPythonImport}

#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
pub struct NativePositionCore{
    pub base:String,
    pub spot_symbol:String,
    pub futures_symbol:String,
    pub opened_at_ms:u64,
    pub spot_baseline_atoms_e18:u128,
    pub spot_net_atoms_e18:u128,
    pub futures_target_atoms_e18:u128,
    pub order_quote_atoms_e18:u128,
}

#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
pub struct NativePositionBundle{
    pub core:NativePositionCore,
    pub lifecycle:PositionLifecycle,
    pub intents:Vec<Intent>,
    pub economics:Option<PositionEconomicsState>,
    pub provenance:PositionProvenance,
    pub reconciliation_required:bool,
    #[serde(default,skip_serializing_if="Option::is_none")]
    pub origin:Option<crate::native_entry_origin::NativeEntryOriginAnchor>,
}

#[derive(Clone,Copy,Debug,PartialEq,Eq,Serialize,Deserialize)]
pub enum NativeEntryPhase{
    SpotPrepared,SpotExposed,FuturesPrepared,RecoveryRequired,UnwindRequired,ManualRequired,
}

#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
pub struct NativeEntryPlan{
    pub base:String,
    pub spot_symbol:String,
    pub futures_symbol:String,
    pub created_at_ms:u64,
    pub planned_qty_atoms_e18:u128,
    pub order_quote_atoms_e18:u128,
    pub spot_baseline_atoms_e18:u128,
    pub safety_generation:u64,
}

/// R32D3: durable recovery EVIDENCE only, never a network order permit.
#[derive(Clone,Copy,Debug,PartialEq,Eq,Serialize,Deserialize)]
pub enum NativeEntryRecoveryQuality{SpotCashMatched,FuturesPriorSpotUnverified}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
pub struct NativeEntryRecoveryCheckpoint{
    pub leg:NativeEntryLeg,pub intent_id:String,pub client_id:String,
    pub order_id:String,pub ledger:LegFillLedger,
    pub quality:NativeEntryRecoveryQuality,
    pub observed_spot_cash_atoms_e18:Option<u128>,
    pub signed_base_fee_atoms_e18:Option<i128>,
}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum NativeEntryRecoveryWrite{Recorded,Idempotent}

/// R32D6B: two-leg native-state consistency snapshot only.
/// The settled spot leg is sourced from persisted native economics, NOT
/// independently verified OKX bills/trades/account history.
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
pub struct NativeEntryDualLegEvidence{
    pub native_generation_at_capture:u64,
    pub created_at_ms:u64,
    pub safety_generation:u64,
    pub settled_spot_intent_id:String,
    pub settled_spot_client_id:String,
    pub settled_spot_order_id:String,
    pub settled_spot_ledger:LegFillLedger,
    pub pending_futures_checkpoint:NativeEntryRecoveryCheckpoint,
    pub independent_spot_exchange_history_verified:bool,
    pub combined_exchange_snapshot_verified:bool,
    pub may_send_order:bool,
}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum NativeDualLegWrite{Recorded,Idempotent}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum NativeSpotAuditWrite{Recorded,Idempotent}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum NativeJointWindowWrite{Recorded,Idempotent}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum NativeSpotFirstLegWrite{Recorded,Idempotent}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum NativeSpotBillsWrite{Recorded,Idempotent}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum NativeFuturesEntryWindowWrite{Recorded,Idempotent}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum NativeProspectiveJointWrite{Recorded,Idempotent}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
pub struct NativeEntryAuthority{
    pub plan:NativeEntryPlan,
    #[serde(default,skip_serializing_if="Option::is_none")]
    pub policy_binding:Option<crate::native_trading_config::NativeEntryPolicyBinding>,
    #[serde(default,skip_serializing_if="Option::is_none")]
    pub origin:Option<crate::native_entry_origin::NativeEntryOriginAnchor>,
    pub phase:NativeEntryPhase,
    pub spot_filled_atoms_e18:u128,
    pub futures_filled_atoms_e18:u128,
    pub intents:Vec<Intent>,
    pub economics:PositionEconomicsState,
    #[serde(default,skip_serializing_if="Option::is_none")]
    pub recovery_checkpoint:Option<NativeEntryRecoveryCheckpoint>,
    #[serde(default,skip_serializing_if="Option::is_none")]
    pub dual_leg_evidence:Option<NativeEntryDualLegEvidence>,
    #[serde(default,skip_serializing_if="Option::is_none")]
    pub settled_spot_window_evidence:Option<crate::native_settled_spot_audit::NativeSpotWindowEvidence>,
    #[serde(default,skip_serializing_if="Option::is_none")]
    pub joint_window_evidence:Option<crate::native_joint_window::NativeJointWindowEvidence>,
    #[serde(default,skip_serializing_if="Option::is_none")]
    pub spot_prehedge_evidence:Option<crate::native_spot_prehedge::NativeSpotPreHedgeEvidence>,
    #[serde(default,skip_serializing_if="Option::is_none")]
    pub origin_spot_link:Option<crate::native_origin_spot_link::NativeOriginSpotLink>,
    #[serde(default,skip_serializing_if="Option::is_none")]
    pub origin_dual_opening_link:Option<crate::native_origin_dual_open::NativeOriginDualOpeningLink>,
    #[serde(default,skip_serializing_if="Option::is_none")]
    pub shadow_close_identity:Option<crate::native_origin_close_fence::NativeShadowCloseIdentityFence>,
    #[serde(default,skip_serializing_if="Option::is_none")]
    pub shadow_close_receipt:Option<crate::native_origin_close_receipt::NativeShadowCloseReceipt>,
    #[serde(default,skip_serializing_if="Option::is_none")]
    pub shadow_close_account_window:Option<crate::native_origin_close_account::NativeShadowCloseAccountWindow>,
    #[serde(default,skip_serializing_if="Option::is_none")]
    pub shadow_lifetime_economics:Option<crate::native_origin_lifetime_economics::NativeShadowLifetimeEconomicsGap>,
    #[serde(default,skip_serializing_if="Option::is_none")]
    pub futures_uncertain_quarantine:Option<crate::native_futures_uncertain_quarantine::NativeFuturesUncertainQuarantine>,
    #[serde(default,skip_serializing_if="Option::is_none")]
    pub wallet_income_bridge:Option<crate::native_wallet_income_bridge::NativeWalletIncomeBridge>,
    #[serde(default,skip_serializing_if="Option::is_none")]
    pub pre_entry_income_gap_proof:Option<crate::native_pre_entry_income_gap::NativePreEntryIncomeGapProof>,
    #[serde(default,skip_serializing_if="Option::is_none")]
    pub dryrun_close_latch:Option<crate::native_recovery_dryrun_latch::NativeRecoveryDryRunLatch>,
    #[serde(default,skip_serializing_if="Option::is_none")]
    pub spot_prehedge_bills_evidence:Option<crate::native_spot_prehedge_bills::NativeSpotBillsEvidence>,
    #[serde(default,skip_serializing_if="Option::is_none")]
    pub futures_entry_window_evidence:Option<crate::native_futures_entry_audit::NativeFuturesEntryWindowEvidence>,
    #[serde(default,skip_serializing_if="Option::is_none")]
    pub prospective_joint_evidence:Option<crate::native_prospective_joint::NativeProspectiveJointEvidence>,
    #[serde(default,skip_serializing_if="Option::is_none")]
    pub real_risk_observation:Option<crate::native_real_risk_watch::NativeRealRiskObservation>,
}

#[derive(Clone,Copy,Debug,PartialEq,Eq,Serialize,Deserialize)]
pub enum NativeEntryLeg{Spot,Futures}

#[derive(Debug)]
pub struct EntryNetworkPermit{
    generation:u64,
    intent_id:String,
    leg:NativeEntryLeg,
}
impl EntryNetworkPermit{
    pub fn generation(&self)->u64{self.generation}
    pub fn intent_id(&self)->&str{&self.intent_id}
    pub fn leg(&self)->NativeEntryLeg{self.leg}
}

#[derive(Debug)]
pub struct AuthorizedEntryOrder{
    permit:EntryNetworkPermit,
    intent:OrderIntent,
    safety_generation:u64,
}
impl AuthorizedEntryOrder{
    pub fn intent(&self)->&OrderIntent{&self.intent}
    pub fn leg(&self)->NativeEntryLeg{self.permit.leg}
    pub fn generation(&self)->u64{self.permit.generation}
    pub fn intent_id(&self)->&str{&self.permit.intent_id}
    pub fn safety_generation(&self)->u64{self.safety_generation}
    pub fn into_permit(self)->EntryNetworkPermit{self.permit}
}

/// R32D: read-only provenance for an unresolved opening order. This
/// snapshot is not a network mutation permission and cannot be promoted
/// into an EntryNetworkPermit after a restart.
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct NativePendingEntryQuery{
    pub generation:u64,
    pub leg:NativeEntryLeg,
    pub symbol:String,
    pub created_at_ms:u64,
    pub spot_symbol:String,
    pub futures_symbol:String,
    pub base:String,
    pub intent_id:String,
    pub client_id:String,
    pub requested_atoms_e18:u128,
    pub spot_baseline_atoms_e18:u128,
    pub confirmed_spot_atoms_e18:u128,
    pub confirmed_futures_atoms_e18:u128,
}

/// Non-cloneable, non-serializable permission bound to a physically persisted
/// native closing intent and its exact authoritative state generation.
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct NativePendingCloseQuery{
    pub generation:u64,pub venue:Venue,pub symbol:String,
    pub client_id:String,pub requested_atoms_e18:u128,
}

#[derive(Debug)]
pub struct NativeClosePermit{
    generation:u64,intent_id:String,client_id:String,leg:CloseLeg,safety_generation:u64,
}
impl NativeClosePermit{
    pub fn generation(&self)->u64{self.generation}
    pub fn intent_id(&self)->&str{&self.intent_id}
    pub fn leg(&self)->CloseLeg{self.leg}
}

#[derive(Debug)]
pub struct AuthorizedCloseOrder{
    permit:NativeClosePermit,
    plan:CloseNetworkPlan,
}
impl AuthorizedCloseOrder{
    pub fn plan(&self)->&CloseNetworkPlan{&self.plan}
    pub fn into_permit(self)->NativeClosePermit{self.permit}
}

#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
pub struct NativeAuthoritativeState{
    pub schema_version:u32,
    pub generation:u64,
    #[serde(default,skip_serializing_if="Option::is_none")]
    pub entry:Option<NativeEntryAuthority>,
    pub active:Option<NativePositionBundle>,
    #[serde(default,skip_serializing_if="Option::is_none")]
    pub paper_risk_ledger:Option<crate::native_daily_risk::NativeDailyRiskLedger>,
    #[serde(default,skip_serializing_if="Option::is_none")]
    pub pre_entry_binance_baseline:Option<crate::native_pre_entry_binance_baseline::NativePreEntryBinanceBaseline>,
    pub legacy_source_sha256:Option<String>,
    pub legacy_closed_positions_ignored:u64,
}

#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
struct StateEnvelope{
    payload:NativeAuthoritativeState,
    payload_sha256:String,
}

#[derive(Clone,Debug,PartialEq,Eq)]
pub enum NativeStateError{
    Io(io::ErrorKind),Json,Checksum,Schema,Generation,Invalid(&'static str),
    Legacy(&'static str),Economics,
}
impl From<io::Error> for NativeStateError{
    fn from(v:io::Error)->Self{Self::Io(v.kind())}
}

fn hex(bytes:&[u8])->String{
    const H:&[u8;16]=b"0123456789abcdef";
    let mut s=String::with_capacity(bytes.len()*2);
    for &b in bytes{s.push(H[(b>>4)as usize]as char);s.push(H[(b&15)as usize]as char);}
    s
}
fn pow10(n:u32)->Result<i128,NativeStateError>{
    let mut v=1i128;
    for _ in 0..n{v=v.checked_mul(10).ok_or(NativeStateError::Legacy("decimal overflow"))?;}
    Ok(v)
}
fn parse_decimal_e18_text(s:&str)->Result<i128,NativeStateError>{
    let t=s.trim();
    if t.is_empty(){return Err(NativeStateError::Legacy("empty decimal"))}
    let (neg,u)=if let Some(x)=t.strip_prefix('-'){(true,x)}
        else if let Some(x)=t.strip_prefix('+'){(false,x)}else{(false,t)};
    if u.is_empty(){return Err(NativeStateError::Legacy("bad decimal"))}
    let mut ep=u.split(|c|c=='e'||c=='E');
    let mant=ep.next().unwrap_or("");
    let exp_txt=ep.next();
    if ep.next().is_some(){return Err(NativeStateError::Legacy("bad decimal"))}
    let exp:i32=match exp_txt{
        Some(x) if !x.is_empty()=>x.parse().map_err(|_|NativeStateError::Legacy("bad exponent"))?,
        Some(_)=>return Err(NativeStateError::Legacy("bad exponent")),
        None=>0,
    };
    if !(-36..=36).contains(&exp){return Err(NativeStateError::Legacy("exponent range"))}
    let mut p=mant.split('.');
    let whole=p.next().unwrap_or("");let frac=p.next().unwrap_or("");
    if p.next().is_some()||whole.is_empty()||
        !whole.bytes().all(|b|b.is_ascii_digit())||!frac.bytes().all(|b|b.is_ascii_digit()){
        return Err(NativeStateError::Legacy("bad decimal"))
    }
    let digits: i128=format!("{whole}{frac}").parse()
        .map_err(|_|NativeStateError::Legacy("decimal overflow"))?;
    let power=NATIVE_QTY_SCALE_DIGITS+exp-frac.len() as i32;
    let scaled=if power>=0{
        digits.checked_mul(pow10(power as u32)?).ok_or(NativeStateError::Legacy("decimal overflow"))?
    }else{
        let d=pow10((-power)as u32)?;
        if digits%d!=0{return Err(NativeStateError::Legacy("precision > e18"))}
        digits/d
    };
    Ok(if neg{-scaled}else{scaled})
}
fn value_u128_e18(v:&Value)->Result<u128,NativeStateError>{
    let n=if let Some(s)=v.as_str(){parse_decimal_e18_text(s)?}
        else if v.is_number(){parse_decimal_e18_text(&v.to_string())?}
        else{return Err(NativeStateError::Legacy("quantity type"))};
    u128::try_from(n).map_err(|_|NativeStateError::Legacy("negative quantity"))
}
fn value_seconds_to_ms(v:&Value)->Result<u64,NativeStateError>{
    let atoms=value_u128_e18(v)?;
    let ms=atoms/1_000_000_000_000_000u128;
    u64::try_from(ms).map_err(|_|NativeStateError::Legacy("timestamp overflow"))
}
fn payload_hash(v:&NativeAuthoritativeState)->Result<String,NativeStateError>{
    let bytes=serde_json::to_vec(v).map_err(|_|NativeStateError::Json)?;
    Ok(hex(&sha256(&bytes)))
}
pub fn legacy_source_sha256(raw:&[u8])->String{hex(&sha256(raw))}
pub fn verify_legacy_source_unchanged(
    raw:&[u8],state:&NativeAuthoritativeState
)->Result<bool,NativeStateError>{
    let expected=state.legacy_source_sha256.as_ref()
        .ok_or(NativeStateError::Legacy("legacy source hash missing"))?;
    Ok(&legacy_source_sha256(raw)==expected)
}

impl NativeAuthoritativeState{
    pub fn empty_from_legacy(hash:String,closed_ignored:u64)->Self{
        Self{schema_version:NATIVE_STATE_SCHEMA_VERSION,generation:1,entry:None,active:None,
            paper_risk_ledger:None,pre_entry_binance_baseline:None,
            legacy_source_sha256:Some(hash),legacy_closed_positions_ignored:closed_ignored}
    }
    pub fn validate(&self)->Result<(),NativeStateError>{
        if self.schema_version!=NATIVE_STATE_SCHEMA_VERSION{return Err(NativeStateError::Schema)}
        if self.generation==0{return Err(NativeStateError::Generation)}
        if self.entry.is_some()&&self.active.is_some(){
            return Err(NativeStateError::Invalid("entry and active both present"))
        }
        if let Some(opening)=self.pre_entry_binance_baseline.as_ref(){
            opening.validate(self)?;
        }
        if let Some(bridge)=self.entry.as_ref().and_then(|e|e.wallet_income_bridge.as_ref()){
            bridge.verify(self)?;
        }
        if let Some(gap)=self.entry.as_ref().and_then(|e|e.pre_entry_income_gap_proof.as_ref()){
            gap.verify(self)?;
        }
        if let Some(mock)=self.entry.as_ref().and_then(|e|e.dryrun_close_latch.as_ref()){
            mock.verify(self)?;
        }
        if let Some(risk)=&self.paper_risk_ledger{
            risk.validate().map_err(|_|NativeStateError::Invalid(
                "R32D6C8B2A durable paper daily risk ledger invalid"))?;
            if risk.open_paper_slots()>0&&(self.entry.is_some()||self.active.is_some()){
                return Err(NativeStateError::Invalid(
                    "R32D6C8B2A paper reservation cannot coexist with native entry/active"))
            }
        }
        if let Some(e)=&self.entry{
            e.validate()?;
            if let Some(origin)=&e.origin{
                origin.verify_entry(e,self.generation)?;
            }
            if let Some(link)=&e.origin_spot_link{
                link.verify(e,self.generation)?;
            }
            if let Some(dual)=&e.origin_dual_opening_link{
                dual.verify(e,self.generation)?;
            }
            if let Some(close)=&e.shadow_close_identity{
                close.verify(e,self.generation)?;
            }
            if let Some(close_receipt)=&e.shadow_close_receipt{
                close_receipt.verify(e,self.generation)?;
            }
            if let Some(account)=&e.shadow_close_account_window{
                account.verify(e,self.generation)?;
            }
            if let Some(econ)=&e.shadow_lifetime_economics{
                econ.verify(e,self.generation)?;
            }
            if let Some(uncertain)=&e.futures_uncertain_quarantine{
                uncertain.verify(e,self.generation)?;
                if let Some(cp)=&e.recovery_checkpoint{
                    uncertain.validate_get_checkpoint(e,cp)?;
                }
            }
            if let Some(dual)=&e.dual_leg_evidence{
                if dual.native_generation_at_capture==0
                    ||dual.native_generation_at_capture>=self.generation{
                    return Err(NativeStateError::Invalid("dual proof native generation fence"))
                }
            }
            if let Some(external)=&e.settled_spot_window_evidence{
                if external.source_generation==0
                    ||external.source_generation>=self.generation{
                    return Err(NativeStateError::Invalid("settled spot audit generation fence"))
                }
            }
            if let Some(joint)=&e.joint_window_evidence{
                if joint.source_generation==0||joint.source_generation>=self.generation{
                    return Err(NativeStateError::Invalid("joint diagnostic generation fence"))
                }
            }
            if let Some(first)=&e.spot_prehedge_evidence{
                if first.source_generation==0||first.source_generation>=self.generation{
                    return Err(NativeStateError::Invalid("first leg provenance generation fence"))
                }
            }
            if let Some(bills)=&e.spot_prehedge_bills_evidence{
                if bills.source_generation==0||bills.source_generation>=self.generation{
                    return Err(NativeStateError::Invalid("currency-wide bills generation fence"))
                }
            }
            if let Some(fut)=&e.futures_entry_window_evidence{
                if fut.source_generation==0||fut.source_generation>=self.generation{
                    return Err(NativeStateError::Invalid("futures entry window generation fence"))
                }
            }
            if let Some(joint)=&e.prospective_joint_evidence{
                if joint.source_generation==0||joint.source_generation>=self.generation{
                    return Err(NativeStateError::Invalid("prospective joint evidence generation fence"))
                }
            }
            if let Some(risk)=&e.real_risk_observation{
                risk.validate_against(e,self.generation).map_err(|_|NativeStateError::Invalid(
                    "R32D6C8B2B1 real risk diagnostic cannot certify unknown exit"))?;
            }
        }
        if let Some(p)=&self.active{
            p.validate()?;
            // R32D6C4: the ordinary linked review library has NO certified
            // way to prove that a Native Hedged position reflects actual
            // exchange inventory. This guard sits in the common persisted
            // STATE validator, not just the install/entry transition APIs:
            // create, replace, arbitrary transact, reopen and direct state
            // restoration cannot synthesize trading authority.
            #[cfg(not(test))]
            if p.provenance==PositionProvenance::Native{
                return Err(NativeStateError::Invalid(
                    "R32D6C4 native active authority not externally certified"))
            }
        }
        Ok(())
    }
}
impl NativePositionBundle{
    pub fn validate(&self)->Result<(),NativeStateError>{
        let c=&self.core;
        if c.base.is_empty()||c.base!=c.base.to_ascii_uppercase(){return Err(NativeStateError::Invalid("base"))}
        if c.spot_symbol.is_empty()||c.futures_symbol.is_empty(){return Err(NativeStateError::Invalid("symbols"))}
        if c.spot_net_atoms_e18==0||c.futures_target_atoms_e18==0||c.order_quote_atoms_e18==0{
            return Err(NativeStateError::Invalid("zero quantity"))
        }
        if self.lifecycle.target_units!=c.futures_target_atoms_e18{
            return Err(NativeStateError::Invalid("lifecycle target mismatch"))
        }
        if matches!(self.lifecycle.state,LifecycleState::Closed|LifecycleState::VerifiedClosed){
            return Err(NativeStateError::Invalid("closed lifecycle in active position"))
        }
        let unresolved=self.intents.iter().filter(|x|matches!(x.state,IntentState::Prepared|IntentState::Unknown)).count();
        if unresolved>1{return Err(NativeStateError::Invalid("multiple unresolved intents"))}
        if let Some(pending)=&self.lifecycle.pending{
            if pending.intent_id.is_empty()||pending.client_id.is_empty()||pending.requested_units==0{
                return Err(NativeStateError::Invalid("invalid close pending identity"))
            }
            if !self.intents.iter().any(|i|i.id==pending.intent_id
                && i.client_order_id.as_deref()==Some(pending.client_id.as_str())){
                return Err(NativeStateError::Invalid("close intent not in authoritative bundle"))
            }
        }
        if let Some(origin)=&self.origin{
            origin.verify_active_plan(self)?;
        }
        if self.provenance==PositionProvenance::LegacyPythonImport{
            if self.origin.is_some(){return Err(NativeStateError::Invalid("legacy native origin spoof"))}
            if !self.reconciliation_required{return Err(NativeStateError::Invalid("legacy reconciliation flag"))}
            if !matches!(self.lifecycle.state,LifecycleState::RecoveryRequired|LifecycleState::ManualRequired){
                return Err(NativeStateError::Invalid("legacy lifecycle must recover"))
            }
        }
        if let Some(e)=&self.economics{e.validate().map_err(|_|NativeStateError::Economics)?;}
        Ok(())
    }
}

/// Only trusted owner-CAS paths may carry a configured opening origin
/// forward. A new anchor cannot be added to an older unbound entry; an
/// existing one cannot be erased/replaced while entry or Native-active
/// still exists. Terminal clear is permitted but NOT a verified close.
fn preserve_origin_transition(
    previous:&NativeAuthoritativeState,next:&NativeAuthoritativeState
)->Result<(),NativeStateError>{
    let before=previous.entry.as_ref().and_then(|e|e.origin.as_ref())
        .or_else(||previous.active.as_ref().and_then(|p|p.origin.as_ref()));
    let after=next.entry.as_ref().and_then(|e|e.origin.as_ref())
        .or_else(||next.active.as_ref().and_then(|p|p.origin.as_ref()));
    let prev_occupied=previous.entry.is_some()||previous.active.is_some();
    let next_occupied=next.entry.is_some()||next.active.is_some();
    // B4B: a linked actual OKX fill is exposure; it must not silently
    // disappear even if a caller tries to erase the *whole* entry instead
    // of only its proof. No native close certification exists to release it.
    if previous.entry.as_ref().map_or(false,|e|
        e.origin_spot_link.is_some()||e.origin_dual_opening_link.is_some())
        &&next.entry.is_none(){
        return Err(NativeStateError::Invalid(
            "native source entry with signed opening fills cannot be dropped"))
    }
    // MICRO5B2A: double signed original opening income-gap observation is
    // immutable; prior state cannot be rolled back via ordinary owner CAS.
    // No duplicate mock CLOSE identities or unknown-response rollback.
    if let Some(old)=previous.entry.as_ref().and_then(|e|e.dryrun_close_latch.as_ref()){
        let next_latch=next.entry.as_ref().and_then(|e|e.dryrun_close_latch.as_ref());
        if !next_latch.map(|x|old.valid_transition(x)).unwrap_or(false){
            return Err(NativeStateError::Invalid("mock close ID erased, rebound or retry enabled"))
        }
    }
    if let Some(old)=previous.entry.as_ref().and_then(|e|e.pre_entry_income_gap_proof.as_ref()){
        if next.entry.as_ref().and_then(|e|e.pre_entry_income_gap_proof.as_ref())!=Some(old){
            return Err(NativeStateError::Invalid(
                "original pre-entry Binance income-gap proof erased/rebound"))
        }
    }
    // MICRO5B1: do not ever lose an already-sealed original wallet bridge.
    if let Some(bridge)=previous.entry.as_ref().and_then(|e|e.wallet_income_bridge.as_ref()){
        if next.entry.as_ref().and_then(|e|e.wallet_income_bridge.as_ref())!=Some(bridge){
            return Err(NativeStateError::Invalid(
                "MICRO5B pre-entry USDT bridge erased or rewritten"))
        }
    }
    // MICRO5A: a genuinely PRE-entry exchange wallet snapshot must remain
    // exactly frozen, even across follow-up orders or generic CAS updates.
    if let Some(original)=previous.pre_entry_binance_baseline.as_ref(){
        if next.pre_entry_binance_baseline.as_ref()!=Some(original){
            return Err(NativeStateError::Invalid(
                "original Binance pre-entry account baseline cannot be erased/rebound"))
        }
    }
    // MICRO4: an independently witnessed Binance account/income bracket,
    // and subsequent cross-venue audit must be sticky while the original
    // futures uncertainty remains. Both are deny-only external read proofs.
    if previous.entry.as_ref().map_or(false,|e|e.futures_uncertain_quarantine.is_some()){
        if let Some(old)=previous.entry.as_ref()
            .and_then(|e|e.futures_entry_window_evidence.as_ref()){
            if next.entry.as_ref().and_then(|e|e.futures_entry_window_evidence.as_ref())!=Some(old){
                return Err(NativeStateError::Invalid(
                    "quarantined Binance signed account proof cannot be deleted/replaced"))
            }
        }
        if let Some(old)=previous.entry.as_ref()
            .and_then(|e|e.prospective_joint_evidence.as_ref()){
            if next.entry.as_ref().and_then(|e|e.prospective_joint_evidence.as_ref())!=Some(old){
                return Err(NativeStateError::Invalid(
                    "quarantined cross-venue signed proof cannot be deleted/replaced"))
            }
        }
    }
    // MICRO3: once original quarantined signed GET fills/fees are sealed,
    // their trade IDs, client/order and fee ledger must never be erased or
    // substituted by an unrelated owner-CAS state update.
    if let Some(old_cp)=previous.entry.as_ref().and_then(|e|
        if e.futures_uncertain_quarantine.is_some(){e.recovery_checkpoint.as_ref()}
        else{None}){
        if next.entry.as_ref().and_then(|e|e.recovery_checkpoint.as_ref())!=Some(old_cp){
            return Err(NativeStateError::Invalid(
                "quarantined futures signed trades/fees cannot be erased or rewritten"))
        }
    }
    // MICRO1: unknown futures may be a live exchange exposure, so neither
    // the entry nor the original uncertainty snapshot can be discarded.
    if let Some(original)=previous.entry.as_ref()
        .and_then(|e|e.futures_uncertain_quarantine.as_ref()){
        if next.entry.as_ref().and_then(|e|e.futures_uncertain_quarantine.as_ref())!=Some(original){
            return Err(NativeStateError::Invalid(
                "suspected futures execution cannot be silently erased or changed"))
        }
    }
    // C8B2B2B4A: once the original C2 spot fill is linked to the
    // configured-origin seal, an active entry snapshot must NOT silently
    // erase or substitute that link through replace/transact.
    if let Some(econ)=previous.entry.as_ref()
        .and_then(|e|e.shadow_lifetime_economics.as_ref()){
        if next.entry.as_ref().and_then(|e|e.shadow_lifetime_economics.as_ref())!=Some(econ){
            return Err(NativeStateError::Invalid(
                "frozen four-leg economics may not be erased or replaced"))
        }
    }
    if let Some(account)=previous.entry.as_ref()
        .and_then(|e|e.shadow_close_account_window.as_ref()){
        if next.entry.as_ref().and_then(|e|e.shadow_close_account_window.as_ref())!=Some(account){
            return Err(NativeStateError::Invalid(
                "sealed bounded close account source cannot be erased/rebound"))
        }
    }
    if let Some(receipt)=previous.entry.as_ref()
        .and_then(|e|e.shadow_close_receipt.as_ref()){
        if next.entry.as_ref().and_then(|e|e.shadow_close_receipt.as_ref())!=Some(receipt){
            return Err(NativeStateError::Invalid(
                "persisted bounded close receipts may not be erased/rebound"))
        }
    }
    if let Some(previous_close)=previous.entry.as_ref()
        .and_then(|e|e.shadow_close_identity.as_ref()){
        if next.entry.as_ref().and_then(|e|e.shadow_close_identity.as_ref())!=Some(previous_close){
            return Err(NativeStateError::Invalid(
                "shadow close IDs cannot be erased or replaced by an owner CAS update"))
        }
    }
    if let Some(previous_dual)=previous.entry.as_ref()
        .and_then(|e|e.origin_dual_opening_link.as_ref()){
        if let Some(current_entry)=next.entry.as_ref(){
            if current_entry.origin_dual_opening_link.as_ref()!=Some(previous_dual){
                return Err(NativeStateError::Invalid(
                    "signed opening two-leg ancestry cannot be erased/replaced"))
            }
        }
    }
    if let Some(old_link)=previous.entry.as_ref()
        .and_then(|e|e.origin_spot_link.as_ref()){
        if let Some(next_entry)=next.entry.as_ref(){
            if next_entry.origin_spot_link.as_ref()!=Some(old_link){
                return Err(NativeStateError::Invalid(
                    "original opening spot fill link cannot be erased or changed"))
            }
        }
    }
    if let Some(origin)=before{
        if next_occupied&&after!=Some(origin){
            return Err(NativeStateError::Invalid("configured native origin cannot change/disappear"))
        }
    }else if let Some(origin)=after{
        if prev_occupied||origin.initial_generation()!=next.generation{
            return Err(NativeStateError::Invalid("cannot graft origin onto old position"))
        }
    }
    Ok(())
}

#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum StatePersistFailpoint{None,BeforeWrite,AfterWrite,AfterFileSync,AfterRename,AfterDirSync}

struct FileLock{file:File}
impl Drop for FileLock{
    fn drop(&mut self){unsafe{libc::flock(self.file.as_raw_fd(),libc::LOCK_UN);}}
}

#[derive(Debug,Clone)]
pub struct NativeStateStore{path:PathBuf}
impl NativeStateStore{
    fn lock_exclusive(&self)->Result<FileLock,NativeStateError>{
        let lock_path=self.path.with_extension("lock");
        let file=OpenOptions::new().read(true).write(true).create(true).open(lock_path)?;
        if unsafe{libc::flock(file.as_raw_fd(),libc::LOCK_EX)}!=0{
            return Err(NativeStateError::Io(io::Error::last_os_error().kind()))
        }
        Ok(FileLock{file})
    }
    pub fn open(path:impl AsRef<Path>)->Result<Self,NativeStateError>{
        let path=path.as_ref().to_path_buf();
        if let Some(parent)=path.parent(){fs::create_dir_all(parent)?;}
        Ok(Self{path})
    }
    pub fn load(&self)->Result<Option<NativeAuthoritativeState>,NativeStateError>{
        // A pending V2 snapshot must not be mistaken for an empty legacy owner.
        if crate::native_trade2f3_v2_versioned_store::pending_path(&self.path)
            .symlink_metadata().is_ok(){
            return Err(NativeStateError::Invalid("unfinished V2 owner revision"))
        }
        if !self.path.exists(){return Ok(None)}
        let mut bytes=Vec::new();File::open(&self.path)?.read_to_end(&mut bytes)?;
        if bytes.is_empty(){return Err(NativeStateError::Json)}
        let env:StateEnvelope=serde_json::from_slice(&bytes).map_err(|_|NativeStateError::Json)?;
        env.payload.validate()?;
        let expected=payload_hash(&env.payload)?;
        if expected!=env.payload_sha256{return Err(NativeStateError::Checksum)}
        Ok(Some(env.payload))
    }
    pub fn create(&self,state:&NativeAuthoritativeState)->Result<(),NativeStateError>{
        let _lock=self.lock_exclusive()?;
        if crate::native_trade2f3_v2_versioned_store::pending_path(&self.path)
            .symlink_metadata().is_ok(){
            return Err(NativeStateError::Invalid("unfinished V2 owner creation"))
        }
        if self.path.exists(){return Err(NativeStateError::Generation)}
        if state.generation!=1{return Err(NativeStateError::Generation)}
        self.persist_internal(state,StatePersistFailpoint::None)
    }
    pub fn replace(&self,expected_generation:u64,state:&NativeAuthoritativeState)->Result<(),NativeStateError>{
        let _lock=self.lock_exclusive()?;
        let current=self.load()?.ok_or(NativeStateError::Generation)?;
        if current.generation!=expected_generation||state.generation!=expected_generation.saturating_add(1){
            return Err(NativeStateError::Generation)
        }
        preserve_origin_transition(&current,state)?;
        self.persist_internal(state,StatePersistFailpoint::None)
    }
    pub fn persist_with_failpoint(
        &self,state:&NativeAuthoritativeState,fp:StatePersistFailpoint
    )->Result<(),NativeStateError>{
        let _lock=self.lock_exclusive()?;
        // This diagnostic test writer predates the anchored entry. Never
        // let its generation-bypass route replace an existing sealed origin.
        // For an anchored state it may only re-persist identical bytes.
        if let Some(old)=self.load()?{
            if let Some(baseline)=old.pre_entry_binance_baseline.as_ref(){
                if state.pre_entry_binance_baseline.as_ref()!=Some(baseline){
                    return Err(NativeStateError::Invalid(
                        "debug failpoint writer cannot erase Binance baseline"))
                }
            }
            if let Some(bridge)=old.entry.as_ref().and_then(|e|e.wallet_income_bridge.as_ref()){
                if state.entry.as_ref().and_then(|e|e.wallet_income_bridge.as_ref())!=Some(bridge){
                    return Err(NativeStateError::Invalid(
                        "debug failpoint writer cannot erase/rebind wallet bridge"))
                }
            }
            if let Some(gap)=old.entry.as_ref().and_then(|e|e.pre_entry_income_gap_proof.as_ref()){
                if state.entry.as_ref().and_then(|e|e.pre_entry_income_gap_proof.as_ref())!=Some(gap){
                    return Err(NativeStateError::Invalid(
                        "debug failpoint writer cannot erase/rebind income-gap witness"))
                }
            }
            if let Some(mock)=old.entry.as_ref().and_then(|e|e.dryrun_close_latch.as_ref()){
                if state.entry.as_ref().and_then(|e|e.dryrun_close_latch.as_ref())!=Some(mock){
                    return Err(NativeStateError::Invalid(
                        "diagnostic writer cannot erase/change mock close seal"))
                }
            }
            let anchored=old.entry.as_ref().and_then(|e|e.origin.as_ref()).is_some()
                ||old.active.as_ref().and_then(|p|p.origin.as_ref()).is_some();
            if anchored&&old!=*state{
                return Err(NativeStateError::Invalid(
                    "origin state replacement through raw failpoint writer denied"))
            }
        }
        self.persist_internal(state,fp)
    }
    pub fn transact<T,F>(
        &self,expected_generation:u64,f:F
    )->Result<(NativeAuthoritativeState,T),NativeStateError>
    where F:FnOnce(&mut NativeAuthoritativeState)->Result<T,NativeStateError>{
        let _lock=self.lock_exclusive()?;
        let mut current=self.load()?.ok_or(NativeStateError::Generation)?;
        if current.generation!=expected_generation{return Err(NativeStateError::Generation)}
        let original_generation=current.generation;
        let before=current.clone();
        let output=f(&mut current)?;
        if current.generation!=original_generation{return Err(NativeStateError::Generation)}
        current.generation=original_generation.checked_add(1).ok_or(NativeStateError::Generation)?;
        current.validate()?;
        preserve_origin_transition(&before,&current)?;
        self.persist_internal(&current,StatePersistFailpoint::None)?;
        Ok((current,output))
    }
    fn persist_internal(
        &self,state:&NativeAuthoritativeState,fp:StatePersistFailpoint
    )->Result<(),NativeStateError>{
        state.validate()?;
        let parent=self.path.parent().ok_or(NativeStateError::Invalid("missing parent"))?;
        let env=StateEnvelope{payload:state.clone(),payload_sha256:payload_hash(state)?};
        let bytes=serde_json::to_vec(&env).map_err(|_|NativeStateError::Json)?;
        let tmp=self.path.with_extension("tmp");
        if fp==StatePersistFailpoint::BeforeWrite{return Err(NativeStateError::Io(io::ErrorKind::Other))}
        let mut file=OpenOptions::new().write(true).create(true).truncate(true).open(&tmp)?;
        file.write_all(&bytes)?;
        if fp==StatePersistFailpoint::AfterWrite{return Err(NativeStateError::Io(io::ErrorKind::Other))}
        file.sync_all()?;
        if fp==StatePersistFailpoint::AfterFileSync{return Err(NativeStateError::Io(io::ErrorKind::Other))}
        fs::rename(&tmp,&self.path)?;
        if fp==StatePersistFailpoint::AfterRename{return Err(NativeStateError::Io(io::ErrorKind::Other))}
        File::open(parent)?.sync_all()?;
        if fp==StatePersistFailpoint::AfterDirSync{return Err(NativeStateError::Io(io::ErrorKind::Other))}
        Ok(())
    }
}

/// Explicit V1/V2 disk dispatch for review. Legacy V1 validation is untouched;
/// V2 never produces a trading permit or a VerifiedClosed result.
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum NativeVersionedReview {
    LegacyV1(NativeAuthoritativeState),
    FeeAwareV2(crate::native_trade2f3_v2_versioned_store::V2ReviewState),
}
impl NativeStateStore {
    /// Uses the SAME owner lock and SAME on-disk path as the V1 store.
    /// No silent downgrade of V2, or migration of legacy sealed entries.
    pub fn load_versioned_review(&self)
        ->Result<Option<NativeVersionedReview>,NativeStateError>{
        use crate::native_trade2f3_v2_versioned_store as v2;
        let _guard=self.lock_exclusive()?;
        let version=v2::detect_version(&self.path)
            .map_err(|_|NativeStateError::Invalid("owner version dispatch denied"))?;
        match version{
            None=>Ok(None),
            Some(1)=>self.load().map(|s|s.map(NativeVersionedReview::LegacyV1)),
            Some(2)=>v2::load_v2(&self.path)
                .map(NativeVersionedReview::FeeAwareV2).map(Some)
                .map_err(|_|NativeStateError::Invalid("V2 review source/hash/cash invalid")),
            _=>Err(NativeStateError::Schema),
        }
    }
    /// OFFLINE ONLY: V2 owner lock -> freshly read revision-4 source ->
    /// PAPER CAS durable append. No other V2 writer using this flock can
    /// mutate the source between validation and the journal commit.
    ///
    /// Lock ordering is fixed: V2 .lock first, PAPER .writer_lock second.
    /// No callback can acquire this lock in the opposite order.
    /// The two files are NOT a single atomic transaction against power loss,
    /// off-host rollback, or a non-cooperating filesystem writer.
    pub fn attach_paper_v2_alignment_review(
        &self,
        paper:&mut crate::native_paper_owner_transaction_review::PaperOwner,
        base:&str,expected_paper_seq:u64,
        close:&crate::native_trade2f3_v2_four_leg_close_review::V2FourLegCloseObservation,
    )->Result<u64,NativeStateError>{
        use crate::native_trade2f3_v2_versioned_store as v2;
        let _owner_guard=self.lock_exclusive()?;
        let exact=v2::load_v2(&self.path).map_err(|_|
            NativeStateError::Invalid("V2 source missing, replaced, pending or corrupt"))?;
        if exact.revision!=4||
           exact.phase!=v2::V2ReviewPhase::FuturesEvidenceRecorded{
            return Err(NativeStateError::Invalid("V2 must be exact review revision 4"))
        }
        paper.attach_v2_local_alignment(base,expected_paper_seq,&exact,close)
            .map_err(|_|NativeStateError::Invalid("PAPER CAS alignment refused"))
    }
    /// Cold offline archive read and PAPER CAS within one V2 owner flock.
    pub fn attach_paper_v2_archived_alignment_review(
        &self,paper:&mut crate::native_paper_owner_transaction_review::PaperOwner,
        base:&str,expected_paper_seq:u64,archive:&std::path::Path,
    )->Result<u64,NativeStateError>{
        use crate::native_trade2f3_v2_versioned_store as v2;
        let _guard=self.lock_exclusive()?;
        let current=v2::load_v2(&self.path).map_err(|_|
            NativeStateError::Invalid("V2 owner cold read denied"))?;
        let close=crate::native_paper_v2_close_source_archive_review::
            read_local_close_source(archive,&current).map_err(|_|
                NativeStateError::Invalid("local archive evidence denied"))?;
        paper.attach_v2_local_alignment(base,expected_paper_seq,&current,&close)
            .map_err(|_|NativeStateError::Invalid("PAPER CAS from archive denied"))
    }
    /// Creates a nonauthorizing V2 *review-only* owner on a fresh path.
    /// Existing V1 state cannot be replaced or rewritten by this method.
    pub fn create_v2_review(&self,
        genesis:crate::native_trade2f3_v2_owner_genesis::V2OwnerGenesisReview,
        fp:crate::native_trade2f3_v2_versioned_store::V2ReviewFailpoint,
    )->Result<(),NativeStateError>{
        use crate::native_trade2f3_v2_versioned_store as v2;
        let _guard=self.lock_exclusive()?;
        let state=v2::V2ReviewState::from_genesis(genesis)
            .map_err(|_|NativeStateError::Invalid("invalid V2 owner genesis"))?;
        v2::create_v2(&self.path,&state,fp)
            .map_err(|_|NativeStateError::Invalid("V2 create denied/unfinished"))
    }
    /// True local generation CAS under original NativeStateStore flock.
    /// Source root immutable; only offline review phase advances.
    pub fn advance_v2_review(&self,expected_revision:u64,
        next:crate::native_trade2f3_v2_versioned_store::V2ReviewPhase,
        fp:crate::native_trade2f3_v2_versioned_store::V2ReviewFailpoint,
    )->Result<crate::native_trade2f3_v2_versioned_store::V2ReviewState,NativeStateError>{
        use crate::native_trade2f3_v2_versioned_store as v2;
        let _guard=self.lock_exclusive()?;
        v2::advance_v2(&self.path,expected_revision,next,fp)
            .map_err(|_|NativeStateError::Invalid("V2 revision CAS/transition denied"))
    }
    /// Persist a single V2 C3 futures observation under original native
    /// flock and expected V2 review revision 3. Does NOT set Hedged/permit.
    pub fn record_v2_c3_review(&self,expected_revision:u64,
        observation:crate::native_trade2f3_v2_c3_futures_evidence::V2C3FuturesObservation,
        fp:crate::native_trade2f3_v2_versioned_store::V2ReviewFailpoint,
    )->Result<crate::native_trade2f3_v2_versioned_store::V2ReviewState,NativeStateError>{
        use crate::native_trade2f3_v2_versioned_store as v2;
        let _guard=self.lock_exclusive()?;
        v2::record_c3_v2(&self.path,expected_revision,observation,fp)
            .map_err(|_|NativeStateError::Invalid("V2 C3 source/CAS/transition denied"))
    }
    /// C5/C6/B4 source-shaped fee-aware close diagnostics only.
    /// Uses the original owner CAS; NEVER sets native VerifiedClosed.
    pub fn record_v2_four_leg_close_review(&self,expected_revision:u64,
        observation:crate::native_trade2f3_v2_four_leg_close_review::V2FourLegCloseObservation,
        fp:crate::native_trade2f3_v2_versioned_store::V2ReviewFailpoint,
    )->Result<crate::native_trade2f3_v2_versioned_store::V2ReviewState,NativeStateError>{
        use crate::native_trade2f3_v2_versioned_store as v2;
        let _lock=self.lock_exclusive()?;
        v2::record_four_leg_close_v2(&self.path,expected_revision,observation,fp)
            .map_err(|_|NativeStateError::Invalid("V2 C5/C6/B4 source/CAS denied"))
    }
    /// Offline, owner-generation bound emergency recovery witness.
    /// Unknown ≠ zero. Partial ≠ flat. No order retry/sending permitted.
    pub fn record_v2_emergency_close_review(&self,expected_revision:u64,
        evidence:crate::native_trade2f3_v2_emergency_close::V2EmergencyCloseEvidence,
        fp:crate::native_trade2f3_v2_versioned_store::V2ReviewFailpoint,
    )->Result<crate::native_trade2f3_v2_versioned_store::V2ReviewState,NativeStateError>{
        use crate::native_trade2f3_v2_versioned_store as v2;
        let _guard=self.lock_exclusive()?;
        v2::record_emergency_close_v2(&self.path,expected_revision,evidence,fp)
            .map_err(|_|NativeStateError::Invalid("V2 emergency close/source/CAS denied"))
    }
    /// Immutable caller-shaped, zero-network C3 opening recovery review.
    /// No order resend, Hedged, or permission to use private exchange API.
    pub fn record_v2_c3_opening_recovery_review(&self,expected_revision:u64,
        evidence:crate::native_trade2f3_v2_c3_opening_recovery::V2C3OpeningRecoveryEvidence,
        fp:crate::native_trade2f3_v2_versioned_store::V2ReviewFailpoint,
    )->Result<crate::native_trade2f3_v2_versioned_store::V2ReviewState,NativeStateError>{
        use crate::native_trade2f3_v2_versioned_store as v2;
        let _guard=self.lock_exclusive()?;
        v2::record_opening_recovery_v2(&self.path,expected_revision,evidence,fp)
            .map_err(|_|NativeStateError::Invalid("V2 C3 opening recovery/CAS denied"))
    }
    /// REVIEW-ONLY co-located append-only journal under original owner flock.
    /// Must be manually called after each V2 revision in this experiment;
    /// it is NOT atomic with original owner CAS, nor independent storage.
    pub fn append_v2_revision_journal_review(&self,
        fp:crate::native_trade2f3_v2_revision_journal::V2JournalFailpoint,
    )->Result<crate::native_trade2f3_v2_revision_journal::V2JournalEntry,NativeStateError>{
        use crate::native_trade2f3_v2_versioned_store as v2;
        use crate::native_trade2f3_v2_revision_journal as journal;
        let _guard=self.lock_exclusive()?;
        let owner=v2::load_v2(&self.path)
            .map_err(|_|NativeStateError::Invalid("V2 owner invalid for journal append"))?;
        journal::append_current_revision_offline(&self.path,&owner,fp)
            .map_err(|_|NativeStateError::Invalid("V2 journal gap/torn stage denied"))
    }
    /// Check local hash chain + caller-held floor *under owner flock*.
    /// Supplied floor is NOT externally verified by this API.
    /// Does not authorize dispatch or mutate owner.
    pub fn verify_v2_revision_journal_review(&self,
        floor:Option<&crate::native_trade2f3_v2_revision_journal::V2CallerHeldFloor>,
    )->Result<crate::native_trade2f3_v2_revision_journal::V2JournalReview,NativeStateError>{
        use crate::native_trade2f3_v2_versioned_store as v2;
        use crate::native_trade2f3_v2_revision_journal as journal;
        let _guard=self.lock_exclusive()?;
        let owner=v2::load_v2(&self.path)
            .map_err(|_|NativeStateError::Invalid("V2 owner invalid for journal preflight"))?;
        journal::review_disk_local_journal(&self.path,&owner,floor)
            .map_err(|_|NativeStateError::Invalid("V2 journal/owner/floor mismatch denied"))
    }
    /// Offline cryptographic verification of a signed witness FLOOR, under
    /// original V2 owner flock. The caller MUST separately provision the pin,
    /// nonce and off-host currentness. This cannot issue execution permits.
    pub fn verify_v2_signed_floor_review(&self,
        signed:&crate::native_trade2f3_v2_signed_floor::V2SignedFloorEnvelope,
        config:&crate::native_trade2f3_v2_signed_floor::V2ExternallyProvisionedVerifier,
    )->Result<crate::native_trade2f3_v2_signed_floor::V2SignedFloorReview,NativeStateError>{
        use crate::native_trade2f3_v2_versioned_store as v2;
        use crate::native_trade2f3_v2_revision_journal as journal;
        use crate::native_trade2f3_v2_signed_floor as witness;
        let _guard=self.lock_exclusive()?;
        let owner=v2::load_v2(&self.path)
            .map_err(|_|NativeStateError::Invalid("V2 owner unavailable for signed floor"))?;
        let entries=journal::read_local_journal(&self.path)
            .map_err(|_|NativeStateError::Invalid("V2 journal unavailable for signed floor"))?;
        witness::review_signed_floor_against_owner_journal(&owner,&entries,signed,config)
            .map_err(|_|NativeStateError::Invalid("V2 signed floor mismatch or untrusted"))
    }
    /// Classify owner/journal/signed witness crash windows under owner lock.
    /// Diagnostic only. All outcomes, even aligned, DENY trade or durable ACK.
    pub fn classify_v2_three_way_review(&self,
        signed:&crate::native_trade2f3_v2_signed_floor::V2SignedFloorEnvelope,
        config:&crate::native_trade2f3_v2_signed_floor::V2ExternallyProvisionedVerifier,
    )->Result<crate::native_trade2f3_v2_three_way_gate::V2ThreeWayReview,NativeStateError>{
        use crate::native_trade2f3_v2_versioned_store as v2;
        use crate::native_trade2f3_v2_revision_journal as journal;
        use crate::native_trade2f3_v2_three_way_gate as gate;
        let _guard=self.lock_exclusive()?;
        let owner=v2::load_v2(&self.path)
            .map_err(|_|NativeStateError::Invalid("V2 owner failed three-way preflight"))?;
        let entries=journal::read_local_journal(&self.path)
            .map_err(|_|NativeStateError::Invalid("V2 journal failed three-way preflight"))?;
        gate::classify_v2_three_way(&owner,&entries,signed,config)
            .map_err(|_|NativeStateError::Invalid("V2 three-way witness identity or source denied"))
    }
    /// Isolated durable review-only intent. Original owner flock is retained
    /// while reading original V2 owner and journal. Does not promote execution.
    pub fn prepare_v2_ack_intent_review(&self,
        ack:&crate::native_trade2f3_v2_durable_ack_review::V2DurableAckReview,
        mock_witness_prior:Option<&crate::native_trade2f3_v2_revision_journal::V2JournalEntry>,
        fp:crate::native_trade2f3_v2_durable_ack_review::V2AckReviewFailpoint,
    )->Result<crate::native_trade2f3_v2_durable_ack_review::V2AckReviewOutcome,NativeStateError>{
        use crate::native_trade2f3_v2_versioned_store as v2;
        use crate::native_trade2f3_v2_revision_journal as journal;
        let _lock=self.lock_exclusive()?;
        let owner=v2::load_v2(&self.path)
            .map_err(|_|NativeStateError::Invalid("ACK prep V2 owner denied"))?;
        let entries=journal::read_local_journal(&self.path)
            .map_err(|_|NativeStateError::Invalid("ACK prep journal denied"))?;
        ack.prepare_review(&owner,&entries,mock_witness_prior,fp)
            .map_err(|_|NativeStateError::Invalid("ACK prep wrong predecessor or pending"))
    }
    /// Store ONLY a local diagnostic signed-receipt checkpoint; no trading
    /// or independent remote-durability authority follows from success.
    pub fn confirm_v2_ack_intent_review(&self,
        ack:&crate::native_trade2f3_v2_durable_ack_review::V2DurableAckReview,
        signed:&crate::native_trade2f3_v2_signed_floor::V2SignedFloorEnvelope,
        pin:&crate::native_trade2f3_v2_signed_floor::V2ExternallyProvisionedVerifier,
        fp:crate::native_trade2f3_v2_durable_ack_review::V2AckReviewFailpoint,
    )->Result<crate::native_trade2f3_v2_durable_ack_review::V2AckReviewOutcome,NativeStateError>{
        use crate::native_trade2f3_v2_versioned_store as v2;
        use crate::native_trade2f3_v2_revision_journal as journal;
        let _lock=self.lock_exclusive()?;
        let owner=v2::load_v2(&self.path)
            .map_err(|_|NativeStateError::Invalid("ACK check V2 owner denied"))?;
        let entries=journal::read_local_journal(&self.path)
            .map_err(|_|NativeStateError::Invalid("ACK check journal denied"))?;
        ack.confirm_review(&owner,&entries,signed,pin,fp)
            .map_err(|_|NativeStateError::Invalid("ACK check signed witness or pending denied"))
    }
}

fn e12_to_e18(v:u128)->Result<u128,NativeStateError>{
    v.checked_mul(1_000_000).ok_or(NativeStateError::Invalid("quantity scale overflow"))
}
fn e18_to_e12(v:u128)->Result<u128,NativeStateError>{
    if v%1_000_000!=0{return Err(NativeStateError::Invalid("quantity precision exceeds economics scale"))}
    Ok(v/1_000_000)
}
impl NativeEntryAuthority{
    /// Assemble BOTH native ledgers from ONE persisted entry generation.
    /// This is native-state ancestry, NOT independent exchange spot proof.
    fn dual_leg_evidence_candidate(
        &self,native_generation_at_capture:u64
    )->Result<NativeEntryDualLegEvidence,NativeStateError>{
        if native_generation_at_capture==0
            ||self.phase!=NativeEntryPhase::RecoveryRequired
            ||self.spot_filled_atoms_e18==0
            ||self.economics.entry.spot.orders.len()!=1{
            return Err(NativeStateError::Invalid("dual leg native recovery not eligible"))
        }
        let fut=self.recovery_checkpoint.as_ref()
            .ok_or(NativeStateError::Invalid("future fill checkpoint absent"))?;
        if fut.quality!=NativeEntryRecoveryQuality::FuturesPriorSpotUnverified
            ||fut.leg!=NativeEntryLeg::Futures
            ||!fut.ledger.cumulative_truth_present
            ||!fut.ledger.terminal||!fut.ledger.fee_evidence_complete{
            return Err(NativeStateError::Invalid("unverified futures recovery checkpoint"))
        }
        let unresolved=self.intents.iter().filter(|i|
            matches!(i.state,IntentState::Prepared|IntentState::Unknown)
            && i.id==fut.intent_id
            && i.client_order_id.as_deref()==Some(fut.client_id.as_str()))
            .count();
        if unresolved!=1{
            return Err(NativeStateError::Invalid("dual pending futures identity mismatch"))
        }
        let (spot_id,spot)=self.economics.entry.spot.orders.iter().next()
            .ok_or(NativeStateError::Invalid("native prior spot ledger missing"))?;
        let settled=self.intents.iter().find(|i|
            i.id==*spot_id
            && i.client_order_id.as_deref()==Some(spot.client_order_id.as_str())
            && i.state==IntentState::Acknowledged)
            .ok_or(NativeStateError::Invalid("spot previous intent not acknowledged"))?;
        if settled.id==fut.intent_id ||spot_id==&fut.intent_id
            ||spot.venue!="okx"||spot.symbol!=self.plan.spot_symbol
            ||spot.side!="buy"
            ||spot.exchange_order_id.as_deref().unwrap_or("").is_empty()
            ||spot.client_order_id.is_empty()
            ||!spot.cumulative_truth_present||!spot.terminal
            ||!spot.fee_evidence_complete||spot.fills.is_empty()
            ||spot.fills.values().any(|f|f.trade_time_ms.is_none())
            ||e12_to_e18(spot.filled_qty_atoms())?!=self.spot_filled_atoms_e18{
            return Err(NativeStateError::Invalid("settled spot per-trade provenance incomplete"))
        }
        if e12_to_e18(fut.ledger.filled_qty_atoms())?>self.spot_filled_atoms_e18
            ||fut.ledger.intent_id!=fut.intent_id
            ||fut.ledger.client_order_id!=fut.client_id
            ||fut.ledger.exchange_order_id.as_deref()!=Some(fut.order_id.as_str()){
            return Err(NativeStateError::Invalid("futures diagnostic overfill or ID mismatch"))
        }
        Ok(NativeEntryDualLegEvidence{
            native_generation_at_capture,
            created_at_ms:self.plan.created_at_ms,
            safety_generation:self.plan.safety_generation,
            settled_spot_intent_id:spot_id.clone(),
            settled_spot_client_id:spot.client_order_id.clone(),
            settled_spot_order_id:spot.exchange_order_id.clone().unwrap(),
            settled_spot_ledger:spot.clone(),
            pending_futures_checkpoint:fut.clone(),
            independent_spot_exchange_history_verified:false,
            combined_exchange_snapshot_verified:false,
            may_send_order:false,
        })
    }

    /// R32D6C2: an original ACKed SPOT leg is eligible for native futures
    /// preparation ONLY after every per-fill fee/ID/time has been durably
    /// captured and exactly matches a signed read-only reconciliation.
    /// No historic cumulative-only fill may be inferred from balances.
    pub fn require_spot_prehedge_provenance(&self)->Result<(),NativeStateError>{
        let proof=self.spot_prehedge_evidence.as_ref()
            .ok_or(NativeStateError::Invalid("first-leg full fill lineage absent"))?;
        let original=self.economics.entry.spot.order(&proof.spot_intent_id)
            .map_err(|_|NativeStateError::Economics)?;
        if self.phase!=NativeEntryPhase::SpotExposed
            ||self.futures_filled_atoms_e18!=0
            ||!original.terminal||!original.cumulative_truth_present
            ||!original.fee_evidence_complete||original.fills.is_empty()
            ||original.fills.values().any(|f|f.trade_time_ms.is_none())
            ||proof.exact_ledger!=*original
            ||proof.safety_generation!=self.plan.safety_generation
            ||proof.may_send_order||proof.exchange_global_completeness_proven
            ||proof.exchange_bill_history_proven||!proof.cash_bracket_matched
            ||proof.spot_client_id!=original.client_order_id
            ||proof.spot_order_id!=original.exchange_order_id.as_deref().unwrap_or("")
            ||self.economics.entry.spot.orders.len()!=1
            ||self.economics.entry.futures.orders.len()!=0
            ||self.intents.len()!=1
            ||!self.intents.iter().any(|i|i.id==proof.spot_intent_id
                &&i.client_order_id.as_deref()==Some(proof.spot_client_id.as_str())
                &&i.state==IntentState::Acknowledged){
            return Err(NativeStateError::Invalid("first-leg provenance cannot prepare futures"))
        }
        Ok(())
    }

    /// R32D6C3: no Futures preparation without a separate currency-wide
    /// first-leg account-bill ancestry proof in the same native state.
    pub fn require_spot_prehedge_bills_provenance(&self)->Result<(),NativeStateError>{
        self.require_spot_prehedge_provenance()?;
        let source=self.spot_prehedge_evidence.as_ref()
            .ok_or(NativeStateError::Invalid("first leg trade source absent"))?;
        let bill=self.spot_prehedge_bills_evidence.as_ref()
            .ok_or(NativeStateError::Invalid("currency-wide account bills absent"))?;
        if bill.source_generation<=source.source_generation
            ||bill.spot_first_source_generation!=source.source_generation
            ||bill.spot_intent_id!=source.spot_intent_id
            ||bill.spot_client_id!=source.spot_client_id
            ||bill.spot_order_id!=source.spot_order_id
            ||!bill.exact_bills_for_first_leg_matched
            ||!bill.base_cash_ancestry_to_original_baseline_matched
            ||!bill.source_balance_bracket_matched
            ||bill.account_history_globally_complete
            ||bill.cross_venue_atomicity_proven||bill.may_send_order{
            return Err(NativeStateError::Invalid("cash lineage cannot prepare futures"))
        }
        Ok(())
    }

    /// R32D6C4: a conserved account balance/older signed GET result must
    /// never become a fresh authority merely because state generation did
    /// not change. The supplied clock is explicitly a local bounded fence,
    /// NOT a cross-venue exchange-atomic timestamp.
    pub fn require_recent_spot_bills_provenance(
        &self,observed_now_ms:u64
    )->Result<(),NativeStateError>{
        self.require_spot_prehedge_bills_provenance()?;
        let source=self.spot_prehedge_evidence.as_ref()
            .ok_or(NativeStateError::Invalid("spot fill clock absent"))?;
        let bills=self.spot_prehedge_bills_evidence.as_ref()
            .ok_or(NativeStateError::Invalid("cash bill clock absent"))?;
        const WINDOW_MAX_MS:u64=60_000;
        const FUTURE_ALLOWANCE_MS:u64=5_000;
        for stamp in [source.observed_at_ms,bills.observed_at_ms]{
            if stamp==0||stamp>observed_now_ms.saturating_add(FUTURE_ALLOWANCE_MS)
                ||observed_now_ms.saturating_sub(stamp)>WINDOW_MAX_MS{
                return Err(NativeStateError::Invalid(
                    "R32D6C4 first-leg source observation stale"))
            }
        }
        Ok(())
    }

    pub fn validate(&self)->Result<(),NativeStateError>{
        let p=&self.plan;
        if self.origin.is_some()&&self.policy_binding.is_none(){
            return Err(NativeStateError::Invalid("origin lost required policy binding"))
        }
        if let Some(binding)=&self.policy_binding{
            binding.validate_in_state(p).map_err(|_|NativeStateError::Invalid(
                "R32D6C8B1 policy binding invalid or nominal quote cap exceeded"))?;
            if let Some(first)=&self.spot_prehedge_evidence{
                binding.verify_exact_spot_fills(p,&first.exact_ledger)
                    .map_err(|_|NativeStateError::Invalid(
                        "R32D6C8B1 signed spot fills exceed planned quote cap or fees"))?;
            }
        }
        if p.base.is_empty()||p.base!=p.base.to_ascii_uppercase(){
            return Err(NativeStateError::Invalid("entry base"))
        }
        if p.spot_symbol.is_empty()||p.futures_symbol.is_empty()||
            p.planned_qty_atoms_e18==0||p.order_quote_atoms_e18==0{
            return Err(NativeStateError::Invalid("entry plan"))
        }
        if self.spot_filled_atoms_e18>p.planned_qty_atoms_e18||
            self.futures_filled_atoms_e18>self.spot_filled_atoms_e18{
            return Err(NativeStateError::Invalid("entry exposure quantities"))
        }
        if self.economics.exit.is_some(){return Err(NativeStateError::Invalid("entry has exit economics"))}
        self.economics.validate().map_err(|_|NativeStateError::Economics)?;
        let spot_e18=e12_to_e18(self.economics.entry.spot.filled_qty_atoms()
            .map_err(|_|NativeStateError::Economics)?)?;
        let fut_e18=e12_to_e18(self.economics.entry.futures.filled_qty_atoms()
            .map_err(|_|NativeStateError::Economics)?)?;
        if spot_e18!=self.spot_filled_atoms_e18||fut_e18!=self.futures_filled_atoms_e18{
            return Err(NativeStateError::Invalid("entry economics quantity mismatch"))
        }
        if let Some(c)=&self.recovery_checkpoint{
            if self.phase!=NativeEntryPhase::RecoveryRequired{
                return Err(NativeStateError::Invalid("checkpoint only in recovery phase"))
            }
            let i=self.intents.iter().find(|i|i.id==c.intent_id
                && i.client_order_id.as_deref()==Some(c.client_id.as_str()))
                .ok_or(NativeStateError::Invalid("checkpoint intent mismatch"))?;
            if !matches!(i.state,IntentState::Prepared|IntentState::Unknown){
                return Err(NativeStateError::Invalid("checkpoint intent already resolved"))
            }
            let original=match c.leg{
                NativeEntryLeg::Spot=>self.economics.entry.spot.order(&c.intent_id),
                NativeEntryLeg::Futures=>self.economics.entry.futures.order(&c.intent_id),
            }.map_err(|_|NativeStateError::Economics)?;
            if c.order_id.is_empty()||c.client_id.is_empty()
                ||c.ledger.intent_id!=c.intent_id||c.ledger.client_order_id!=c.client_id
                ||c.ledger.exchange_order_id.as_deref()!=Some(c.order_id.as_str())
                ||c.ledger.requested_qty_atoms!=original.requested_qty_atoms
                ||!c.ledger.terminal||!c.ledger.cumulative_truth_present
                ||!c.ledger.fee_evidence_complete
                ||c.ledger.filled_qty_atoms()>c.ledger.requested_qty_atoms{
                return Err(NativeStateError::Invalid("checkpoint evidence invalid"))
            }
            if c.ledger.fills.is_empty(){
                return Err(NativeStateError::Invalid("checkpoint no fills"))
            }
            if c.quality==NativeEntryRecoveryQuality::SpotCashMatched{
                let qty=e12_to_e18(c.ledger.filled_qty_atoms())?;
                let expected=i128::try_from(self.plan.spot_baseline_atoms_e18)
                    .ok().and_then(|v|v.checked_add(i128::try_from(qty).ok()?))
                    .and_then(|v|v.checked_add(c.signed_base_fee_atoms_e18?));
                let actual=c.observed_spot_cash_atoms_e18.and_then(|v|i128::try_from(v).ok());
                if c.leg!=NativeEntryLeg::Spot||expected.is_none()||expected!=actual{
                    return Err(NativeStateError::Invalid("spot checkpoint net cash mismatch"))
                }
            }
            if c.quality==NativeEntryRecoveryQuality::FuturesPriorSpotUnverified
                && (c.leg!=NativeEntryLeg::Futures||c.signed_base_fee_atoms_e18.is_some()){
                return Err(NativeStateError::Invalid("futures checkpoint quality invalid"))
            }
        }
        if let Some(first)=&self.spot_prehedge_evidence{
            let saved=self.economics.entry.spot.order(&first.spot_intent_id)
                .map_err(|_|NativeStateError::Economics)?;
            let gross=e12_to_e18(first.exact_ledger.filled_qty_atoms())?;
            let expected_cash=i128::try_from(self.plan.spot_baseline_atoms_e18).ok()
                .and_then(|base|base.checked_add(i128::try_from(gross).ok()?))
                .and_then(|base|base.checked_add(first.signed_base_fee_atoms_e18));
            let mut seen=std::collections::BTreeSet::new();
            if first.source_generation==0
                ||first.safety_generation!=self.plan.safety_generation
                ||first.observed_at_ms<self.plan.created_at_ms
                ||first.balance_updated_at_ms==0
                ||first.balance_updated_at_ms>first.observed_at_ms.saturating_add(5_000)
                ||first.observed_at_ms.saturating_sub(first.balance_updated_at_ms)>180_000
                ||first.exact_ledger!=*saved
                ||first.spot_order_id!=saved.exchange_order_id.as_deref().unwrap_or("")
                ||first.spot_client_id!=saved.client_order_id
                ||saved.fills.is_empty()||!saved.fee_evidence_complete
                ||saved.fills.values().any(|f|f.trade_time_ms.is_none())
                ||first.fill_bill_ids.len()!=saved.fills.len()
                ||first.fill_bill_ids.iter().any(|x|x.is_empty()
                    ||!x.bytes().all(|b|b.is_ascii_digit())||!seen.insert(x))
                ||first.filled_pages<2||first.filled_pages>16
                ||expected_cash!=i128::try_from(first.observed_base_cash_atoms_e18).ok()
                ||!first.cash_bracket_matched
                ||first.exchange_bill_history_proven
                ||first.exchange_global_completeness_proven||first.may_send_order{
                return Err(NativeStateError::Invalid("prehedge spot evidence tampered"))
            }
        }
        if let Some(bills)=&self.spot_prehedge_bills_evidence{
            let first=self.spot_prehedge_evidence.as_ref()
                .ok_or(NativeStateError::Invalid("bills require first-leg per-fill proof"))?;
            let ids:std::collections::BTreeSet<_>=bills.verified_bill_ids.iter().collect();
            let trade_ids:Vec<_>=first.exact_ledger.fills.keys().cloned().collect();
            let gross=e12_to_e18(first.exact_ledger.filled_qty_atoms())?;
            let expected_delta=i128::try_from(gross).ok()
                .and_then(|qty|qty.checked_add(first.signed_base_fee_atoms_e18));
            let expected_cash=i128::try_from(self.plan.spot_baseline_atoms_e18).ok()
                .and_then(|v|v.checked_add(expected_delta?));
            if bills.source_generation<=first.source_generation
                ||bills.spot_first_source_generation!=first.source_generation
                ||bills.observed_at_ms<first.observed_at_ms
                ||bills.observed_at_ms
                    .saturating_sub(bills.observed_base_cash_updated_at_ms)>180_000
                ||bills.observed_base_cash_updated_at_ms!=first.balance_updated_at_ms
                ||bills.safety_generation!=self.plan.safety_generation
                ||bills.spot_intent_id!=first.spot_intent_id
                ||bills.spot_client_id!=first.spot_client_id
                ||bills.spot_order_id!=first.spot_order_id
                ||bills.original_base_cash_atoms_e18!=self.plan.spot_baseline_atoms_e18
                ||bills.observed_base_cash_atoms_e18!=first.observed_base_cash_atoms_e18
                ||bills.aggregate_signed_base_fee_atoms_e18!=first.signed_base_fee_atoms_e18
                ||Some(bills.aggregate_base_delta_atoms_e18)!=expected_delta
                ||expected_cash!=i128::try_from(bills.observed_base_cash_atoms_e18).ok()
                ||bills.verified_bill_ids!=first.fill_bill_ids
                ||ids.len()!=bills.verified_bill_ids.len()
                ||bills.billed_trade_ids!=trade_ids
                ||bills.account_bill_pages<2||bills.account_bill_pages>16
                ||!bills.exact_bills_for_first_leg_matched
                ||!bills.base_cash_ancestry_to_original_baseline_matched
                ||!bills.source_balance_bracket_matched
                ||bills.account_history_globally_complete
                ||bills.cross_venue_atomicity_proven||bills.may_send_order{
                return Err(NativeStateError::Invalid("currency-wide cash movement evidence mismatch"))
            }
        }
        if let Some(proof)=&self.futures_entry_window_evidence{
            let cp=self.recovery_checkpoint.as_ref()
                .ok_or(NativeStateError::Invalid("futures entry snapshot missing detailed checkpoint"))?;
            let ledger=&cp.ledger;
            let trade_ids:Vec<_>=ledger.fills.keys().cloned().collect();
            let gross=e12_to_e18(ledger.filled_qty_atoms())?;
            let expected=self.futures_filled_atoms_e18.checked_add(gross)
                .ok_or(NativeStateError::Invalid("futures entry quantity overflow"))?;
            if self.phase!=NativeEntryPhase::RecoveryRequired
                ||cp.leg!=NativeEntryLeg::Futures
                ||cp.quality!=NativeEntryRecoveryQuality::FuturesPriorSpotUnverified
                ||ledger.fills.is_empty()||!ledger.fee_evidence_complete
                ||proof.safety_generation!=self.plan.safety_generation
                ||proof.futures_intent_id!=cp.intent_id
                ||proof.futures_client_id!=cp.client_id
                ||proof.futures_order_id!=cp.order_id
                ||proof.trade_ids!=trade_ids
                ||proof.fill_qty_atoms_e18!=gross
                ||proof.signed_quote_fee_cost_atoms_e12!=ledger.fee_cost_quote_atoms
                ||proof.observed_short_atoms_e18!=expected
                ||expected>self.spot_filled_atoms_e18
                ||proof.income_pages<2||proof.income_pages>16
                ||proof.trade_windows==0||proof.trade_windows>12
                ||proof.account_read_repetitions!=2
                ||!proof.exact_fills_commissions_and_symbol_history_matched
                ||!proof.income_position_wallet_bracket_matched
                ||proof.exchange_global_history_complete
                ||proof.cross_venue_atomicity_proven
                ||proof.may_send_order||proof.may_promote_hedged
                ||proof.captured_at_ms<self.plan.created_at_ms
                ||proof.observed_short_updated_at_ms==0
                ||proof.observed_wallet_updated_at_ms==0
                ||proof.captured_at_ms.saturating_sub(proof.observed_short_updated_at_ms)>180_000
                ||proof.captured_at_ms.saturating_sub(proof.observed_wallet_updated_at_ms)>180_000
                ||proof.observed_short_updated_at_ms>proof.captured_at_ms.saturating_add(5_000)
                ||proof.observed_wallet_updated_at_ms>proof.captured_at_ms.saturating_add(5_000){
                return Err(NativeStateError::Invalid("futures entry evidence mismatch"))
            }
            // Historical native checkpoint fixtures under cfg(test) have
            // no prospective OKX first-leg proof. In ordinary linked builds,
            // never accept such a futures-window evidence record.
            #[cfg(not(test))]
            if self.spot_prehedge_evidence.is_none()
                ||self.spot_prehedge_bills_evidence.is_none(){
                return Err(NativeStateError::Invalid("unproven first leg before futures audit"))
            }
        }
        if let Some(chain)=&self.prospective_joint_evidence{
            let spot=self.spot_prehedge_evidence.as_ref()
                .ok_or(NativeStateError::Invalid("prospective spot first source absent"))?;
            let bills=self.spot_prehedge_bills_evidence.as_ref()
                .ok_or(NativeStateError::Invalid("prospective spot currency bills absent"))?;
            let futures=self.futures_entry_window_evidence.as_ref()
                .ok_or(NativeStateError::Invalid("prospective futures source absent"))?;
            let cp=self.recovery_checkpoint.as_ref()
                .ok_or(NativeStateError::Invalid("prospective futures checkpoint absent"))?;
            let net=bills.observed_base_cash_atoms_e18
                .checked_sub(self.plan.spot_baseline_atoms_e18)
                .ok_or(NativeStateError::Invalid("prospective base cash less than baseline"))?;
            let residual=net.checked_sub(futures.observed_short_atoms_e18)
                .ok_or(NativeStateError::Invalid("prospective short exceeds spot net"))?;
            let flags_ok=chain.okx_balance_bracket_matched
                &&chain.binance_income_and_position_bracket_matched
                &&chain.foreign_symbol_trades_absent_in_bounded_window
                &&!chain.cross_venue_exchange_snapshot_atomicity_proven
                &&!chain.exchange_global_history_complete
                &&!chain.may_send_order&&!chain.may_promote_hedged;
            if self.phase!=NativeEntryPhase::RecoveryRequired
                ||chain.source_generation<=futures.source_generation
                ||chain.spot_first_source_generation!=spot.source_generation
                ||chain.spot_bills_source_generation!=bills.source_generation
                ||chain.futures_window_source_generation!=futures.source_generation
                ||spot.source_generation>=bills.source_generation
                ||bills.source_generation>=futures.source_generation
                ||chain.safety_generation!=self.plan.safety_generation
                ||chain.spot_intent_id!=spot.spot_intent_id
                ||chain.spot_client_id!=spot.spot_client_id
                ||chain.spot_order_id!=spot.spot_order_id
                ||chain.spot_trade_ids!=spot.exact_ledger.fills.keys().cloned().collect::<Vec<_>>()
                ||chain.spot_bill_ids!=spot.fill_bill_ids
                ||chain.spot_gross_atoms_e18!=self.spot_filled_atoms_e18
                ||chain.spot_available_net_atoms_e18!=net
                ||chain.spot_fee_quote_atoms_e12!=spot.exact_ledger.fee_cost_quote_atoms
                ||chain.spot_cash_e18!=bills.observed_base_cash_atoms_e18
                ||chain.spot_cash_updated_at_ms!=bills.observed_base_cash_updated_at_ms
                ||chain.futures_intent_id!=cp.intent_id
                ||chain.futures_client_id!=cp.client_id
                ||chain.futures_order_id!=cp.order_id
                ||chain.futures_trade_ids!=futures.trade_ids
                ||chain.futures_short_atoms_e18!=futures.observed_short_atoms_e18
                ||chain.futures_fee_quote_atoms_e12!=futures.signed_quote_fee_cost_atoms_e12
                ||chain.futures_wallet_usdt_e18!=futures.observed_wallet_usdt_atoms_e18
                ||chain.futures_position_updated_at_ms!=futures.observed_short_updated_at_ms
                ||chain.futures_wallet_updated_at_ms!=futures.observed_wallet_updated_at_ms
                ||chain.exposed_spot_unhedged_atoms_e18!=residual
                ||!flags_ok
                ||chain.captured_at_ms<spot.observed_at_ms
                ||chain.captured_at_ms<bills.observed_at_ms
                ||chain.captured_at_ms<futures.captured_at_ms
                ||[spot.observed_at_ms,bills.observed_at_ms,futures.captured_at_ms]
                    .iter().any(|t|chain.captured_at_ms.saturating_sub(*t)>60_000
                        ||*t>chain.captured_at_ms.saturating_add(5_000)){
                return Err(NativeStateError::Invalid("prospective two-venue receipt chain tampered"))
            }
        }
        if let Some(joint)=&self.dual_leg_evidence{
            if joint.independent_spot_exchange_history_verified
                ||joint.combined_exchange_snapshot_verified||joint.may_send_order{
                return Err(NativeStateError::Invalid("diagnostic dual leg cannot certify release"))
            }
            let reconstructed=self.dual_leg_evidence_candidate(
                joint.native_generation_at_capture)?;
            if joint!=&reconstructed{
                return Err(NativeStateError::Invalid("dual leg proof differs from native history"))
            }
        }
        if let Some(window)=&self.settled_spot_window_evidence{
            let dual=self.dual_leg_evidence.as_ref()
                .ok_or(NativeStateError::Invalid("external spot audit needs dual snapshot"))?;
            if window.source_generation<=dual.native_generation_at_capture
                ||window.original_dual_capture_generation!=dual.native_generation_at_capture
                ||window.spot_intent_id!=dual.settled_spot_intent_id
                ||window.spot_client_id!=dual.settled_spot_client_id
                ||window.spot_order_id!=dual.settled_spot_order_id
                ||window.spot_ledger!=dual.settled_spot_ledger
                ||!window.all_order_fills_and_bills_in_window_matched
                ||window.exchange_global_completeness_proven
                ||window.cross_venue_atomicity_proven||window.may_send_order
                ||window.observed_cash_updated_at_ms==0
                ||window.spot_ledger.fills.len()!=window.checked_bill_ids.len()
                ||window.fills_history_pages<2||window.cash_bill_pages<2{
                return Err(NativeStateError::Invalid("independent spot audit invalid"))
            }
            let spot=e12_to_e18(dual.settled_spot_ledger.filled_qty_atoms())?;
            let expected_cash=i128::try_from(self.plan.spot_baseline_atoms_e18).ok()
                .and_then(|v|v.checked_add(i128::try_from(spot).ok()?))
                .and_then(|v|v.checked_add(window.signed_base_fee_atoms_e18));
            if window.gross_fill_atoms_e18!=spot
                ||expected_cash!=i128::try_from(window.observed_cash_atoms_e18).ok(){
                return Err(NativeStateError::Invalid("external spot audit cash doesn't balance"))
            }
            let mut bills=std::collections::BTreeSet::new();
            for id in &window.checked_bill_ids{
                if id.is_empty()||!id.bytes().all(|c|c.is_ascii_digit())
                    ||!bills.insert(id){
                    return Err(NativeStateError::Invalid("duplicate/invalid audit bill ID"))
                }
            }
        }
        if let Some(combined)=&self.joint_window_evidence{
            let original=self.settled_spot_window_evidence.as_ref()
                .ok_or(NativeStateError::Invalid("joint snapshot needs prior spot evidence"))?;
            let dual=self.dual_leg_evidence.as_ref()
                .ok_or(NativeStateError::Invalid("joint snapshot needs exact dual evidence"))?;
            let fut=&dual.pending_futures_checkpoint;
            let mut comparable=combined.spot.clone();
            comparable.source_generation=original.source_generation;
            let fills:Vec<String>=fut.ledger.fills.keys().cloned().collect();
            let expected_short=self.futures_filled_atoms_e18.checked_add(
                e12_to_e18(fut.ledger.filled_qty_atoms())?)
                .ok_or(NativeStateError::Invalid("joint short overflow"))?;
            if combined.source_generation<=original.source_generation
                ||combined.spot.source_generation!=combined.source_generation
                ||comparable!=*original
                ||combined.safety_generation!=self.plan.safety_generation
                ||combined.futures_intent_id!=fut.intent_id
                ||combined.futures_client_id!=fut.client_id
                ||combined.futures_order_id!=fut.order_id
                ||combined.futures_trade_ids!=fills
                ||combined.futures_short_atoms_e18!=expected_short
                ||combined.futures_fee_quote_atoms_e12!=fut.ledger.fee_cost_quote_atoms
                ||combined.futures_income_pages<2||combined.futures_trade_windows==0
                ||combined.income_check_repetitions!=2
                ||!combined.spot_window_rechecked
                ||!combined.futures_income_bracket_matched
                ||!combined.futures_symbol_trade_window_matched
                ||combined.exchange_global_history_complete
                ||combined.exchange_cross_venue_atomicity_proven
                ||combined.may_send_order
                ||combined.requested_at_ms<self.plan.created_at_ms
                ||combined.requested_at_ms<combined.spot.observed_cash_updated_at_ms
                ||combined.requested_at_ms<combined.futures_position_updated_at_ms
                ||combined.requested_at_ms<combined.usdt_wallet_updated_at_ms
                ||combined.requested_at_ms
                    .saturating_sub(combined.spot.observed_cash_updated_at_ms)>180_000
                ||combined.requested_at_ms
                    .saturating_sub(combined.futures_position_updated_at_ms)>180_000
                ||combined.requested_at_ms
                    .saturating_sub(combined.usdt_wallet_updated_at_ms)>180_000{
                return Err(NativeStateError::Invalid("joint evidence not exact native diagnostic"))
            }
        }
        let unresolved=self.intents.iter().filter(|x|matches!(x.state,IntentState::Prepared|IntentState::Unknown)).count();
        let prepared=self.intents.iter().filter(|x|x.state==IntentState::Prepared).count();
        if unresolved>1{return Err(NativeStateError::Invalid("entry multiple unresolved intents"))}
        match self.phase{
            NativeEntryPhase::SpotPrepared=>{
                if self.futures_filled_atoms_e18!=0{return Err(NativeStateError::Invalid("spot phase futures exposure"))}
                if unresolved!=1||prepared!=1{return Err(NativeStateError::Invalid("spot prepared intent invariant"))}
            }
            NativeEntryPhase::SpotExposed=>{
                if self.spot_filled_atoms_e18==0||self.futures_filled_atoms_e18>=self.spot_filled_atoms_e18{
                    return Err(NativeStateError::Invalid("spot exposed phase"))
                }
                if unresolved!=0{return Err(NativeStateError::Invalid("spot exposed unresolved intent"))}
            }
            NativeEntryPhase::FuturesPrepared=>{
                // Existing pre-C2 unit fixtures have no per-fill lineage;
                // normal binaries MUST reject such a futures-ready state.
                #[cfg(not(test))]
                if self.spot_prehedge_evidence.is_none()
                    ||self.spot_prehedge_bills_evidence.is_none(){
                    return Err(NativeStateError::Invalid("futures phase missing first-leg cash ancestry"))
                }
                if self.spot_filled_atoms_e18==0||self.futures_filled_atoms_e18>=self.spot_filled_atoms_e18{
                    return Err(NativeStateError::Invalid("futures prepared phase"))
                }
                if unresolved!=1||prepared!=1{return Err(NativeStateError::Invalid("futures prepared intent invariant"))}
            }
            NativeEntryPhase::RecoveryRequired|NativeEntryPhase::UnwindRequired|NativeEntryPhase::ManualRequired=>{}
        }
        Ok(())
    }
}

fn entry_intent_mut<'a>(
    entry:&'a mut NativeEntryAuthority,id:&str
)->Result<&'a mut Intent,NativeStateError>{
    entry.intents.iter_mut().find(|x|x.id==id)
        .ok_or(NativeStateError::Invalid("entry intent missing"))
}

impl NativeStateStore{
    fn verify_policy_binding_at_path(
        entry:&NativeEntryAuthority,config_path:Option<&std::path::Path>
    )->Result<(),NativeStateError>{
        let Some(binding)=entry.policy_binding.as_ref() else{
            if config_path.is_some(){return Err(NativeStateError::Invalid(
                "R32D6C8B1 config route requires a policy-bound entry"))}
            return Ok(())
        };
        let path=config_path.ok_or(NativeStateError::Invalid(
            "R32D6C8B1 raw route forbidden on config-bound entry"))?;
        let fresh=crate::native_trading_config::NativeTradingConfig::load(path)
            .map_err(|_|NativeStateError::Invalid(
                "R32D6C8B1 current YAML missing/invalid"))?;
        fresh.validate_binding(&entry.plan,binding).map_err(|_|NativeStateError::Invalid(
            "R32D6C8B1 YAML hash or quote policy changed since original intent"))
    }
    fn verify_current_entry_policy(
        &self,entry:&NativeEntryAuthority,config_path:Option<&std::path::Path>
    )->Result<(),NativeStateError>{
        Self::verify_policy_binding_at_path(entry,config_path)
    }
    pub fn load_for_recovery(&self)->Result<Option<NativeAuthoritativeState>,NativeStateError>{
        let mut state=self.load()?;
        if let Some(s)=state.as_mut(){
            if let Some(e)=s.entry.as_mut(){
                if e.intents.iter().any(|x|matches!(x.state,IntentState::Prepared|IntentState::Unknown)){
                    e.phase=NativeEntryPhase::RecoveryRequired;
                }
            }
            if let Some(p)=s.active.as_mut(){
                if p.intents.iter().any(|x|matches!(x.state,IntentState::Prepared|IntentState::Unknown)){
                    p.lifecycle.state=LifecycleState::RecoveryRequired;
                }
            }
        }
        Ok(state)
    }

    /// Read only. Recovering any unresolved opening requires exchange
    /// orders, fills, inventory and futures positions. No shortcut to a
    /// new entry/hedge permit or state transition is granted here.
    pub fn pending_native_entry_query(
        &self,expected_generation:u64
    )->Result<NativePendingEntryQuery,NativeStateError>{
        let state=self.load()?.ok_or(NativeStateError::Generation)?;
        if state.generation!=expected_generation{return Err(NativeStateError::Generation)}
        if state.active.is_some(){return Err(NativeStateError::Invalid("active already installed"))}
        let e=state.entry.as_ref().ok_or(NativeStateError::Invalid("entry missing"))?;
        let mut unresolved=e.intents.iter().filter(|x|
            matches!(x.state,IntentState::Prepared|IntentState::Unknown));
        let item=unresolved.next().ok_or(NativeStateError::Invalid("entry is not unresolved"))?;
        if unresolved.next().is_some(){return Err(NativeStateError::Invalid("multiple entry unresolved"))}
        let client_id=item.client_order_id.as_deref()
            .filter(|x|!x.is_empty())
            .ok_or(NativeStateError::Invalid("entry client ID missing"))?;
        let (leg,symbol,atoms)=if e.economics.entry.spot.orders.contains_key(&item.id){
            let o=e.economics.entry.spot.order(&item.id).map_err(|_|NativeStateError::Economics)?;
            (NativeEntryLeg::Spot,e.plan.spot_symbol.clone(),e12_to_e18(o.requested_qty_atoms)?)
        }else if e.economics.entry.futures.orders.contains_key(&item.id){
            let o=e.economics.entry.futures.order(&item.id).map_err(|_|NativeStateError::Economics)?;
            (NativeEntryLeg::Futures,e.plan.futures_symbol.clone(),e12_to_e18(o.requested_qty_atoms)?)
        }else{
            return Err(NativeStateError::Invalid("entry intent has no economic order"))
        };
        if atoms==0{return Err(NativeStateError::Invalid("zero entry requested"))}
        Ok(NativePendingEntryQuery{
            generation:state.generation,leg,symbol,
            created_at_ms:e.plan.created_at_ms,
            spot_symbol:e.plan.spot_symbol.clone(),
            futures_symbol:e.plan.futures_symbol.clone(),base:e.plan.base.clone(),
            intent_id:item.id.clone(),client_id:client_id.into(),
            requested_atoms_e18:atoms,
            spot_baseline_atoms_e18:e.plan.spot_baseline_atoms_e18,
            confirmed_spot_atoms_e18:e.spot_filled_atoms_e18,
            confirmed_futures_atoms_e18:e.futures_filled_atoms_e18,
        })
    }

    /// Legacy synthetic entry constructor remains for compatibility with
    /// pre-R32D6C8B fixtures. It DOES NOT prove a production risk authority,
    /// and ordinary builds still have hard-denied mutation egress.
    pub fn begin_native_entry(
        &self,expected_generation:u64,plan:NativeEntryPlan,
        economics:PositionEconomicsState,spot_intent_id:&str,spot_client_id:&str
    )->Result<(NativeAuthoritativeState,EntryNetworkPermit),NativeStateError>{
        self.begin_native_entry_impl(expected_generation,plan,economics,
            spot_intent_id,spot_client_id,None)
    }

    /// R32D6C8B1: read the original YAML at an explicitly supplied review
    /// path, recompute BOTH leg prices + fees under the loaded $10 quote
    /// policy and bind its SHA256 DURABLY to the first Prepared intent.
    /// No permission to trade follows from a passing risk admission.
    #[allow(clippy::too_many_arguments)]
    pub fn begin_native_entry_with_config(
        &self,expected_generation:u64,plan:NativeEntryPlan,
        economics:PositionEconomicsState,spot_intent_id:&str,spot_client_id:&str,
        spot_limit_e12:u128,futures_reference_e12:u128,
        current_safety_generation:u64,config_path:&std::path::Path
    )->Result<(NativeAuthoritativeState,EntryNetworkPermit),NativeStateError>{
        let cfg=crate::native_trading_config::NativeTradingConfig::load(config_path)
            .map_err(|_|NativeStateError::Invalid("R32D6C8B1 YAML missing or invalid"))?;
        let binding=cfg.bind_entry(&plan,spot_limit_e12,futures_reference_e12,
            current_safety_generation).map_err(|_|NativeStateError::Invalid(
                "R32D6C8B1 quote + fee budget or safety generation DENY"))?;
        self.begin_native_entry_impl(expected_generation,plan,economics,
            spot_intent_id,spot_client_id,Some((binding,config_path.to_path_buf())))
    }

    fn begin_native_entry_impl(
        &self,expected_generation:u64,plan:NativeEntryPlan,
        mut economics:PositionEconomicsState,spot_intent_id:&str,spot_client_id:&str,
        config_source:Option<(crate::native_trading_config::NativeEntryPolicyBinding,
                              std::path::PathBuf)>
    )->Result<(NativeAuthoritativeState,EntryNetworkPermit),NativeStateError>{
        if spot_intent_id.is_empty()||spot_client_id.is_empty(){
            return Err(NativeStateError::Invalid("spot intent identity"))
        }
        if economics.exit.is_some(){return Err(NativeStateError::Invalid("entry economics has exit"))}
        economics.validate().map_err(|_|NativeStateError::Economics)?;
        let planned_e12=e18_to_e12(plan.planned_qty_atoms_e18)?;
        if economics.entry.spot.target_qty_atoms!=planned_e12||
            economics.entry.futures.target_qty_atoms!=planned_e12{
            return Err(NativeStateError::Invalid("entry economics target mismatch"))
        }
        if economics.entry.spot.filled_qty_atoms().map_err(|_|NativeStateError::Economics)?!=0||
            economics.entry.futures.filled_qty_atoms().map_err(|_|NativeStateError::Economics)?!=0||
            !economics.entry.spot.orders.is_empty()||!economics.entry.futures.orders.is_empty(){
            return Err(NativeStateError::Invalid("entry economics not pristine"))
        }
        if economics.entry.spot.symbol!=plan.spot_symbol||
            economics.entry.futures.symbol!=plan.futures_symbol||
            !economics.entry.spot.venue.eq_ignore_ascii_case("okx")||
            !economics.entry.futures.venue.eq_ignore_ascii_case("binance")||
            !economics.entry.spot.side.eq_ignore_ascii_case("buy")||
            !economics.entry.futures.side.eq_ignore_ascii_case("sell"){
            return Err(NativeStateError::Invalid("entry economics route mismatch"))
        }
        economics.entry.spot.add_order(spot_intent_id,spot_client_id,planned_e12)
            .map_err(|_|NativeStateError::Economics)?;
        let intent_id=spot_intent_id.to_string();
        let policy_binding=config_source.as_ref().map(|(v,_)|v.clone());
        let origin=if let Some(binding)=policy_binding.as_ref(){
            Some(crate::native_entry_origin::NativeEntryOriginAnchor::seal_configured_prepared(
                expected_generation.checked_add(1).ok_or(NativeStateError::Generation)?,
                &plan,binding,spot_intent_id,spot_client_id)?)
        }else{None};
        let entry=NativeEntryAuthority{
            plan,policy_binding,origin,phase:NativeEntryPhase::SpotPrepared,
            spot_filled_atoms_e18:0,futures_filled_atoms_e18:0,
            intents:vec![Intent{id:intent_id.clone(),client_order_id:Some(spot_client_id.into()),state:IntentState::Prepared}],
            economics,recovery_checkpoint:None,dual_leg_evidence:None,
            settled_spot_window_evidence:None,joint_window_evidence:None,
            spot_prehedge_evidence:None,origin_spot_link:None,
            origin_dual_opening_link:None,shadow_close_identity:None,
            shadow_close_receipt:None,shadow_close_account_window:None,
            shadow_lifetime_economics:None,futures_uncertain_quarantine:None,
            wallet_income_bridge:None,pre_entry_income_gap_proof:None,
            dryrun_close_latch:None,
            spot_prehedge_bills_evidence:None,
            futures_entry_window_evidence:None,prospective_joint_evidence:None,
            real_risk_observation:None,
        };
        entry.validate()?;
        let (next,_)=self.transact(expected_generation,move|state|{
            if state.entry.is_some()||state.active.is_some(){
                return Err(NativeStateError::Invalid("position authority already occupied"))
            }
            if let Some((binding,path))=&config_source{
                let fresh=crate::native_trading_config::NativeTradingConfig::load(path)
                    .map_err(|_|NativeStateError::Invalid("R32D6C8B1 changed/unavailable YAML"))?;
                fresh.validate_binding(&entry.plan,binding).map_err(|_|NativeStateError::Invalid(
                    "R32D6C8B1 policy changed before Prepared durable write"))?;
            }
            state.entry=Some(entry);
            state.legacy_source_sha256=None;
            Ok(())
        })?;
        let permit=EntryNetworkPermit{generation:next.generation,intent_id,leg:NativeEntryLeg::Spot};
        Ok((next,permit))
    }

    pub fn authorize_entry_order(
        &self,permit:EntryNetworkPermit,intent:&OrderIntent,current_safety_generation:u64
    )->Result<AuthorizedEntryOrder,NativeStateError>{
        self.authorize_entry_order_impl(permit,intent,current_safety_generation,None)
    }

    /// R32D6C8B1: pinned policy digest must still match the current YAML
    /// when binding an EntryNetworkPermit to an exact durable order intent.
    pub fn authorize_entry_order_with_config(
        &self,permit:EntryNetworkPermit,intent:&OrderIntent,current_safety_generation:u64,
        config_path:&std::path::Path
    )->Result<AuthorizedEntryOrder,NativeStateError>{
        self.authorize_entry_order_impl(
            permit,intent,current_safety_generation,Some(config_path))
    }

    fn authorize_entry_order_impl(
        &self,permit:EntryNetworkPermit,intent:&OrderIntent,current_safety_generation:u64,
        config_path:Option<&std::path::Path>
    )->Result<AuthorizedEntryOrder,NativeStateError>{
        let state=self.load()?.ok_or(NativeStateError::Generation)?;
        if state.generation!=permit.generation{return Err(NativeStateError::Generation)}
        let e=state.entry.as_ref().ok_or(NativeStateError::Invalid("entry missing"))?;
        if e.plan.safety_generation!=current_safety_generation{
            return Err(NativeStateError::Invalid("stale safety generation"))
        }
        self.verify_current_entry_policy(e,config_path)?;
        let stored=e.intents.iter().find(|x|x.id==permit.intent_id)
            .ok_or(NativeStateError::Invalid("entry intent missing"))?;
        if stored.state!=IntentState::Prepared{return Err(NativeStateError::Invalid("entry intent not prepared"))}
        let expected_client=stored.client_order_id.as_deref()
            .ok_or(NativeStateError::Invalid("entry client id missing"))?;
        authorize_exact(intent,true,expected_client,e.plan.safety_generation,current_safety_generation)
            .map_err(|_|NativeStateError::Invalid("entry adapter authorization"))?;
        let (expected_venue,expected_symbol,expected_side,requested_e18)=match permit.leg{
            NativeEntryLeg::Spot=>{
                if e.phase!=NativeEntryPhase::SpotPrepared{
                    return Err(NativeStateError::Invalid("spot permit wrong phase"))
                }
                let order=e.economics.entry.spot.order(&permit.intent_id)
                    .map_err(|_|NativeStateError::Economics)?;
                (Venue::OkxSpot,e.plan.spot_symbol.as_str(),"buy",e12_to_e18(order.requested_qty_atoms)?)
            }
            NativeEntryLeg::Futures=>{
                if e.phase!=NativeEntryPhase::FuturesPrepared{
                    return Err(NativeStateError::Invalid("futures permit wrong phase"))
                }
                let order=e.economics.entry.futures.order(&permit.intent_id)
                    .map_err(|_|NativeStateError::Economics)?;
                (Venue::BinanceUsdm,e.plan.futures_symbol.as_str(),"sell",e12_to_e18(order.requested_qty_atoms)?)
            }
        };
        if intent.venue!=expected_venue||intent.symbol!=expected_symbol||
            !intent.side.eq_ignore_ascii_case(expected_side){
            return Err(NativeStateError::Invalid("entry intent route mismatch"))
        }
        let amount_e18=parse_decimal_e18_text(&intent.amount)?;
        if amount_e18<0||amount_e18 as u128!=requested_e18{
            return Err(NativeStateError::Invalid("entry intent amount mismatch"))
        }
        Ok(AuthorizedEntryOrder{
            permit,intent:intent.clone(),safety_generation:current_safety_generation,
        })
    }

    /// Hold the native writer lock across the final state proof and the
    /// single attempt to send. A concurrent native state transition cannot
    /// change the durable generation in that interval.
    pub(crate) fn with_authorized_entry_send<T>(
        &self, authorized:&AuthorizedEntryOrder, current_safety_generation:u64,
        send:impl FnOnce()->T,
    )->Result<T,NativeStateError>{
        crate::native_hard_safety::require_native_mutation_egress()
            .map_err(|_|NativeStateError::Invalid("R32D6C1 review mutation egress locked"))?;
        let _lock=self.lock_exclusive()?;
        if authorized.safety_generation()!=current_safety_generation{
            return Err(NativeStateError::Invalid("stale safety generation"))
        }
        self.revalidate_authorized_entry(authorized)?;
        Ok(send())
    }

    pub fn revalidate_authorized_entry(
        &self,authorized:&AuthorizedEntryOrder
    )->Result<(),NativeStateError>{
        let state=self.load()?.ok_or(NativeStateError::Generation)?;
        if state.generation!=authorized.generation(){return Err(NativeStateError::Generation)}
        let e=state.entry.as_ref().ok_or(NativeStateError::Invalid("entry missing"))?;
        if e.plan.safety_generation!=authorized.safety_generation{
            return Err(NativeStateError::Invalid("stale safety generation"))
        }
        let stored=e.intents.iter().find(|x|x.id==authorized.intent_id())
            .ok_or(NativeStateError::Invalid("entry intent missing"))?;
        if stored.state!=IntentState::Prepared{return Err(NativeStateError::Invalid("entry intent not prepared"))}
        if stored.client_order_id.as_deref()!=Some(authorized.intent.client_id.as_str()){
            return Err(NativeStateError::Invalid("entry client id mismatch"))
        }
        let (venue,symbol,side,requested_e18)=match authorized.leg(){
            NativeEntryLeg::Spot=>{
                if e.phase!=NativeEntryPhase::SpotPrepared{return Err(NativeStateError::Invalid("spot permit wrong phase"))}
                let order=e.economics.entry.spot.order(authorized.intent_id()).map_err(|_|NativeStateError::Economics)?;
                (Venue::OkxSpot,e.plan.spot_symbol.as_str(),"buy",e12_to_e18(order.requested_qty_atoms)?)
            }
            NativeEntryLeg::Futures=>{
                if e.phase!=NativeEntryPhase::FuturesPrepared{return Err(NativeStateError::Invalid("futures permit wrong phase"))}
                let order=e.economics.entry.futures.order(authorized.intent_id()).map_err(|_|NativeStateError::Economics)?;
                (Venue::BinanceUsdm,e.plan.futures_symbol.as_str(),"sell",e12_to_e18(order.requested_qty_atoms)?)
            }
        };
        let intent=&authorized.intent;
        if intent.venue!=venue||intent.symbol!=symbol||!intent.side.eq_ignore_ascii_case(side){
            return Err(NativeStateError::Invalid("entry intent route mismatch"))
        }
        let amount_e18=parse_decimal_e18_text(&intent.amount)?;
        if amount_e18<0||amount_e18 as u128!=requested_e18{
            return Err(NativeStateError::Invalid("entry intent amount mismatch"))
        }
        Ok(())
    }

    pub fn record_spot_entry_observation(
        &self,permit:EntryNetworkPermit,cumulative_fill_atoms_e18:u128,
        avg_price_atoms_e12:Option<u128>,terminal:bool,rejected:bool,unknown:bool
    )->Result<NativeAuthoritativeState,NativeStateError>{
        if permit.leg!=NativeEntryLeg::Spot{return Err(NativeStateError::Invalid("wrong entry permit leg"))}
        let intent_id=permit.intent_id;
        let (next,_)=self.transact(permit.generation,move|state|{
            let mut clear_entry=false;
            {
                let e=state.entry.as_mut().ok_or(NativeStateError::Invalid("entry missing"))?;
                if e.phase!=NativeEntryPhase::SpotPrepared{
                    return Err(NativeStateError::Invalid("spot observation wrong phase"))
                }
                if entry_intent_mut(e,&intent_id)?.state!=IntentState::Prepared{
                    return Err(NativeStateError::Invalid("spot intent not prepared"))
                }
                if unknown{
                    entry_intent_mut(e,&intent_id)?.state=IntentState::Unknown;
                    e.phase=NativeEntryPhase::RecoveryRequired;
                }else if rejected{
                    if !terminal||cumulative_fill_atoms_e18!=0{
                        return Err(NativeStateError::Invalid("rejected spot with fill/nonterminal"))
                    }
                    entry_intent_mut(e,&intent_id)?.state=IntentState::Rejected;
                    clear_entry=true;
                }else{
                    let q12=e18_to_e12(cumulative_fill_atoms_e18)?;
                    e.economics.entry.spot.order_mut(&intent_id)
                        .map_err(|_|NativeStateError::Economics)?
                        .apply_cumulative_truth(q12,avg_price_atoms_e12,terminal,
                            Some(if terminal{"filled"}else{"open"}),None,None,1)
                        .map_err(|_|NativeStateError::Economics)?;
                    e.spot_filled_atoms_e18=e12_to_e18(
                        e.economics.entry.spot.filled_qty_atoms().map_err(|_|NativeStateError::Economics)?
                    )?;
                    if terminal{
                        entry_intent_mut(e,&intent_id)?.state=IntentState::Acknowledged;
                        if e.spot_filled_atoms_e18==0{clear_entry=true}
                        else{e.phase=NativeEntryPhase::SpotExposed}
                    }
                }
            }
            if clear_entry{state.entry=None;}
            Ok(())
        })?;
        Ok(next)
    }

    pub fn prepare_futures_entry_hedge(
        &self,expected_generation:u64,current_safety_generation:u64,
        intent_id:&str,client_id:&str
    )->Result<(NativeAuthoritativeState,EntryNetworkPermit),NativeStateError>{
        self.prepare_futures_entry_hedge_impl(expected_generation,current_safety_generation,
            intent_id,client_id,None)
    }

    pub fn prepare_futures_entry_hedge_with_config(
        &self,expected_generation:u64,current_safety_generation:u64,
        intent_id:&str,client_id:&str,config_path:&std::path::Path
    )->Result<(NativeAuthoritativeState,EntryNetworkPermit),NativeStateError>{
        self.prepare_futures_entry_hedge_impl(expected_generation,current_safety_generation,
            intent_id,client_id,Some(config_path.to_path_buf()))
    }
    fn prepare_futures_entry_hedge_impl(
        &self,expected_generation:u64,current_safety_generation:u64,
        intent_id:&str,client_id:&str,config_path:Option<std::path::PathBuf>
    )->Result<(NativeAuthoritativeState,EntryNetworkPermit),NativeStateError>{
        if intent_id.is_empty()||client_id.is_empty(){return Err(NativeStateError::Invalid("futures intent identity"))}
        let iid=intent_id.to_string();
        let cid=client_id.to_string();
        let (next,_)=self.transact(expected_generation,move|state|{
            let e=state.entry.as_mut().ok_or(NativeStateError::Invalid("entry missing"))?;
            Self::verify_policy_binding_at_path(e,config_path.as_deref())?;
            if e.phase!=NativeEntryPhase::SpotExposed{
                return Err(NativeStateError::Invalid("futures prepare wrong phase"))
            }
            // Existing cumulative-only historical unit fixtures are preserved
            // in #[cfg(test)], but production-linked builds must have an
            // independently audited, fsynced first-leg fill/fee ledger.
            #[cfg(not(test))]
            e.require_recent_spot_bills_provenance(crate::private_https::wall_ms())?;
            if e.plan.safety_generation!=current_safety_generation{
                return Err(NativeStateError::Invalid("stale safety generation"))
            }
            if e.intents.iter().any(|x|x.id==iid||x.client_order_id.as_deref()==Some(cid.as_str())){
                return Err(NativeStateError::Invalid("duplicate futures intent"))
            }
            let remaining=e.spot_filled_atoms_e18.checked_sub(e.futures_filled_atoms_e18)
                .ok_or(NativeStateError::Invalid("futures overhedged"))?;
            if remaining==0{return Err(NativeStateError::Invalid("no hedge remaining"))}
            let requested_e12=e18_to_e12(remaining)?;
            e.economics.entry.futures.add_order(&iid,&cid,requested_e12)
                .map_err(|_|NativeStateError::Economics)?;
            e.intents.push(Intent{id:iid.clone(),client_order_id:Some(cid.clone()),state:IntentState::Prepared});
            e.phase=NativeEntryPhase::FuturesPrepared;
            Ok(())
        })?;
        let permit=EntryNetworkPermit{generation:next.generation,intent_id:intent_id.into(),leg:NativeEntryLeg::Futures};
        Ok((next,permit))
    }

    pub fn record_futures_entry_observation(
        &self,permit:EntryNetworkPermit,order_cumulative_fill_atoms_e18:u128,
        avg_price_atoms_e12:Option<u128>,terminal:bool,rejected:bool,unknown:bool
    )->Result<NativeAuthoritativeState,NativeStateError>{
        if permit.leg!=NativeEntryLeg::Futures{return Err(NativeStateError::Invalid("wrong entry permit leg"))}
        // No normal linked review build may ACK even a PARTIAL futures
        // position from unsigned aggregate size. Record UNKNOWN and use the
        // exact signed order/fill checkpoint + account audits in recovery.
        #[cfg(not(test))]
        if order_cumulative_fill_atoms_e18>0&&!unknown{
            return Err(NativeStateError::Invalid(
                "R32D6C5 futures position needs individual fills/fees before ACK"))
        }
        let intent_id=permit.intent_id;
        let (next,_)=self.transact(permit.generation,move|state|{
            let mut promote=false;
            {
                let e=state.entry.as_mut().ok_or(NativeStateError::Invalid("entry missing"))?;
                if e.phase!=NativeEntryPhase::FuturesPrepared{
                    return Err(NativeStateError::Invalid("futures observation wrong phase"))
                }
                if entry_intent_mut(e,&intent_id)?.state!=IntentState::Prepared{
                    return Err(NativeStateError::Invalid("futures intent not prepared"))
                }
                if unknown{
                    entry_intent_mut(e,&intent_id)?.state=IntentState::Unknown;
                    e.phase=NativeEntryPhase::RecoveryRequired;
                }else if rejected{
                    if !terminal||order_cumulative_fill_atoms_e18!=0{
                        return Err(NativeStateError::Invalid("rejected futures with fill/nonterminal"))
                    }
                    entry_intent_mut(e,&intent_id)?.state=IntentState::Rejected;
                    e.phase=NativeEntryPhase::SpotExposed;
                }else{
                    let q12=e18_to_e12(order_cumulative_fill_atoms_e18)?;
                    e.economics.entry.futures.order_mut(&intent_id)
                        .map_err(|_|NativeStateError::Economics)?
                        .apply_cumulative_truth(q12,avg_price_atoms_e12,terminal,
                            Some(if terminal{"filled"}else{"open"}),None,None,1)
                        .map_err(|_|NativeStateError::Economics)?;
                    e.futures_filled_atoms_e18=e12_to_e18(
                        e.economics.entry.futures.filled_qty_atoms().map_err(|_|NativeStateError::Economics)?
                    )?;
                    if e.futures_filled_atoms_e18>e.spot_filled_atoms_e18{
                        return Err(NativeStateError::Invalid("futures overhedged"))
                    }
                    if terminal{
                        entry_intent_mut(e,&intent_id)?.state=IntentState::Acknowledged;
                        if e.futures_filled_atoms_e18==e.spot_filled_atoms_e18{promote=true}
                        else{e.phase=NativeEntryPhase::SpotExposed}
                    }
                }
            }
            if promote{
                // R32D6C4: cumulative exchange quantity alone is never
                // proof of per-fill futures commissions, trade IDs or the
                // complete foreign-order/account history. No normal linked
                // review build may turn it into native Hedged authority.
                // Unit-only synthetic scenarios retain the old transition
                // for isolated economics/lifecycle test coverage.
                #[cfg(not(test))]
                return Err(NativeStateError::Invalid(
                    "R32D6C4 cannot promote futures cumulative truth to native Hedged"));
                #[cfg(test)]
                {
                let mut e=state.entry.take().ok_or(NativeStateError::Invalid("entry missing at promote"))?;
                let locked=e.economics.entry.lock_actual_entry()
                    .map_err(|_|NativeStateError::Invalid("entry economics not fully hedged"))?;
                let spot_e18=e12_to_e18(locked.actual_spot_filled_qty_atoms)?;
                let fut_e18=e12_to_e18(locked.actual_futures_filled_qty_atoms)?;
                if spot_e18!=e.spot_filled_atoms_e18||fut_e18!=e.futures_filled_atoms_e18{
                    return Err(NativeStateError::Invalid("promote quantity mismatch"))
                }
                let core=NativePositionCore{
                    base:e.plan.base.clone(),spot_symbol:e.plan.spot_symbol.clone(),
                    futures_symbol:e.plan.futures_symbol.clone(),opened_at_ms:e.plan.created_at_ms,
                    spot_baseline_atoms_e18:e.plan.spot_baseline_atoms_e18,
                    spot_net_atoms_e18:e.spot_filled_atoms_e18,
                    futures_target_atoms_e18:e.futures_filled_atoms_e18,
                    order_quote_atoms_e18:e.plan.order_quote_atoms_e18,
                };
                let lifecycle=PositionLifecycle::new_hedged(
                    e.futures_filled_atoms_e18,e.plan.safety_generation
                ).map_err(|_|NativeStateError::Invalid("promote lifecycle init"))?;
                let bundle=NativePositionBundle{
                    core,lifecycle,intents:e.intents,economics:Some(e.economics),
                    provenance:PositionProvenance::Native,reconciliation_required:false,
                    origin:e.origin,
                };
                bundle.validate()?;
                state.active=Some(bundle);
                }
            }
            Ok(())
        })?;
        Ok(next)
    }

    pub fn install_native_hedged(
        &self,expected_generation:u64,core:NativePositionCore,
        mut economics:PositionEconomicsState,safety_generation:u64
    )->Result<NativeAuthoritativeState,NativeStateError>{
        if economics.exit.is_some(){return Err(NativeStateError::Invalid("entry install already has exit"))}
        let locked=economics.entry.lock_actual_entry()
            .map_err(|_|NativeStateError::Invalid("entry economics not fully hedged"))?;
        if e12_to_e18(locked.actual_spot_filled_qty_atoms)?!=core.spot_net_atoms_e18||
            e12_to_e18(locked.actual_futures_filled_qty_atoms)?!=core.futures_target_atoms_e18{
            return Err(NativeStateError::Invalid("core/economics quantity mismatch"))
        }
        let lifecycle=PositionLifecycle::new_hedged(core.futures_target_atoms_e18,safety_generation)
            .map_err(|_|NativeStateError::Invalid("lifecycle init"))?;
        let bundle=NativePositionBundle{
            core,lifecycle,intents:Vec::new(),economics:Some(economics),
            provenance:PositionProvenance::Native,reconciliation_required:false,
            origin:None,
        };
        bundle.validate()?;
        let (next,_)=self.transact(expected_generation,move|state|{
            if state.entry.is_some()||state.active.is_some(){
                return Err(NativeStateError::Invalid("position authority already occupied"))
            }
            state.active=Some(bundle);
            state.legacy_source_sha256=None;
            Ok(())
        })?;
        Ok(next)
    }

    pub fn start_native_close(
        &self,expected_generation:u64
    )->Result<NativeAuthoritativeState,NativeStateError>{
        let (next,_)=self.transact(expected_generation,|state|{
            let p=state.active.as_mut().ok_or(NativeStateError::Invalid("active position missing"))?;
            if p.provenance!=PositionProvenance::Native||p.reconciliation_required{
                return Err(NativeStateError::Invalid("position not native-authoritative"))
            }
            p.lifecycle.start_close().map_err(|_|NativeStateError::Invalid("start close transition"))?;
            Ok(())
        })?;
        Ok(next)
    }

    pub fn observe_native_basic_truth(
        &self,expected_generation:u64,truth:BasicTruth
    )->Result<NativeAuthoritativeState,NativeStateError>{
        let (next,_)=self.transact(expected_generation,move|state|{
            let p=state.active.as_mut().ok_or(NativeStateError::Invalid("active position missing"))?;
            if p.provenance!=PositionProvenance::Native||p.reconciliation_required{
                return Err(NativeStateError::Invalid("position not native-authoritative"))
            }
            p.lifecycle.observe_basic_truth(truth)
                .map_err(|_|NativeStateError::Invalid("basic truth transition"))?;
            Ok(())
        })?;
        Ok(next)
    }

    pub fn finalize_verified_native_close(
        &self,expected_generation:u64
    )->Result<NativeAuthoritativeState,NativeStateError>{
        let (next,_)=self.transact(expected_generation,|state|{
            {
                let p=state.active.as_mut().ok_or(NativeStateError::Invalid("active position missing"))?;
                if p.provenance!=PositionProvenance::Native||p.reconciliation_required{
                    return Err(NativeStateError::Invalid("position not native-authoritative"))
                }
                if p.lifecycle.state!=LifecycleState::VerifyingExitTruth{
                    return Err(NativeStateError::Invalid("not verifying exit truth"))
                }
                if p.intents.iter().any(|x|matches!(x.state,IntentState::Prepared|IntentState::Unknown)){
                    return Err(NativeStateError::Invalid("unresolved execution intent"))
                }
                let economics=p.economics.as_ref().ok_or(NativeStateError::Invalid("economics missing"))?;
                let exit=economics.exit.as_ref().ok_or(NativeStateError::Invalid("exit economics missing"))?;
                exit.finalize_against_entry(&economics.entry)
                    .map_err(|_|NativeStateError::Invalid("exit economics not finalizable"))?;
                p.lifecycle.apply_exit_truth(ExitTruth::VerifiedClosed)
                    .map_err(|_|NativeStateError::Invalid("exit truth transition"))?;
                p.lifecycle.finalize()
                    .map_err(|_|NativeStateError::Invalid("lifecycle finalize"))?;
            }
            state.active=None;
            Ok(())
        })?;
        Ok(next)
    }
}

/// Convert exact native e18 atoms to an exchange quantity without f64.
fn close_quantity_decimal_e18(atoms:u128)->String{
    const SCALE:u128=1_000_000_000_000_000_000;
    let whole=atoms/SCALE;
    let remainder=atoms%SCALE;
    if remainder==0{return whole.to_string()}
    let fraction=format!("{remainder:018}");
    format!("{whole}.{}",fraction.trim_end_matches('0'))
}

// R31B: one physically durable native bundle owns BOTH the close lifecycle
// and its execution intents. No independent in-memory owner may authorize send.
impl NativeStateStore{
    pub fn prepare_native_close_intent(
        &self,expected_generation:u64,current_safety_generation:u64,
        intent_id:&str,client_id:&str,
    )->Result<(NativeAuthoritativeState,NativeClosePermit),NativeStateError>{
        if intent_id.is_empty()||client_id.is_empty(){
            return Err(NativeStateError::Invalid("close identity missing"))
        }
        let (next,prepared)=self.transact(expected_generation,|state|{
            let p=state.active.as_mut().ok_or(NativeStateError::Invalid("active missing"))?;
            if p.provenance!=PositionProvenance::Native||p.reconciliation_required{
                return Err(NativeStateError::Invalid("close authority not native"))
            }
            if p.intents.iter().any(|i|i.id==intent_id
                ||i.client_order_id.as_deref()==Some(client_id)){
                return Err(NativeStateError::Invalid("duplicate close intent identity"))
            }
            let symbol=match p.lifecycle.state{
                LifecycleState::ClosingFutures=>p.core.futures_symbol.clone(),
                LifecycleState::ClosingSpot=>p.core.spot_symbol.clone(),
                _=>return Err(NativeStateError::Invalid("not in closing state")),
            };
            let mut execution=ExecutionState{intents:p.intents.clone()};
            let prepared=p.lifecycle.prepare_close_atomic(
                &mut execution,intent_id,client_id,&symbol,current_safety_generation)
                .map_err(|_|NativeStateError::Invalid("close prepare denied"))?;
            p.intents=execution.intents;
            Ok(prepared)
        })?;
        let permit=NativeClosePermit{
            generation:next.generation,intent_id:prepared.intent_id,
            client_id:prepared.client_id,leg:prepared.leg,
            safety_generation:current_safety_generation,
        };
        Ok((next,permit))
    }

    pub fn authorize_native_close_order(
        &self,permit:NativeClosePermit,current_safety_generation:u64,
    )->Result<AuthorizedCloseOrder,NativeStateError>{
        let plan=self.revalidate_native_close_permit(&permit,current_safety_generation)?;
        Ok(AuthorizedCloseOrder{permit,plan})
    }

    fn revalidate_native_close_permit(
        &self,permit:&NativeClosePermit,current_safety_generation:u64,
    )->Result<CloseNetworkPlan,NativeStateError>{
        if permit.safety_generation!=current_safety_generation{
            return Err(NativeStateError::Invalid("stale close safety"))
        }
        let state=self.load()?.ok_or(NativeStateError::Generation)?;
        if state.generation!=permit.generation{return Err(NativeStateError::Generation)}
        let p=state.active.as_ref().ok_or(NativeStateError::Invalid("active missing"))?;
        if p.provenance!=PositionProvenance::Native||p.reconciliation_required{
            return Err(NativeStateError::Invalid("close authority not native"))
        }
        if p.lifecycle.safety_generation!=current_safety_generation{
            return Err(NativeStateError::Invalid("stale close safety"))
        }
        let pending=p.lifecycle.pending.as_ref()
            .ok_or(NativeStateError::Invalid("close pending missing"))?;
        if pending.intent_id!=permit.intent_id||pending.client_id!=permit.client_id
            ||pending.leg!=permit.leg{
            return Err(NativeStateError::Invalid("close pending identity mismatch"))
        }
        let symbol=match permit.leg{
            CloseLeg::Futures=>&p.core.futures_symbol,
            CloseLeg::Spot=>&p.core.spot_symbol,
        };
        // Derive intent from authoritative state, not from caller-provided bytes.
        let (intent,reduce_only,kind,exec_leg)=match permit.leg{
            CloseLeg::Futures=>{
                let un=crate::execution_adapter::futures_unwind();
                (crate::execution_adapter::OrderIntent{
                    venue:Venue::BinanceUsdm,symbol:symbol.clone(),side:un.side.into(),
                    amount:pending.requested_units.to_string(),client_id:permit.client_id.clone()
                },un.reduce_only,un.kind,crate::execution::Leg::Futures)
            },
            CloseLeg::Spot=>{
                let un=crate::execution_adapter::spot_unwind();
                (crate::execution_adapter::OrderIntent{
                    venue:Venue::OkxSpot,symbol:symbol.clone(),side:un.side.into(),
                    amount:pending.requested_units.to_string(),client_id:permit.client_id.clone()
                },un.reduce_only,un.kind,crate::execution::Leg::Spot)
            }
        };
        let prepared=PreparedClose{
            intent_id:permit.intent_id.clone(),client_id:permit.client_id.clone(),
            leg:permit.leg,intent,reduce_only,kind,requested_units:pending.requested_units,
        };
        let exec=ExecutionState{intents:p.intents.clone()};
        let mut plan=p.lifecycle.authorize_prepared_close_atomic(
            &exec,&prepared,current_safety_generation)
            .map_err(|_|NativeStateError::Invalid("close send authorization denied"))?;
        // Legacy R15 order_for() uses numeric units. The authoritative native
        // lifecycle uses e18 atoms: never send the raw atom count to an exchange.
        plan.intent.amount=close_quantity_decimal_e18(pending.requested_units);
        if plan.network_action!=(crate::execution::Action::NetworkMutation{
            intent_id:permit.intent_id.clone(),leg:exec_leg
        }){return Err(NativeStateError::Invalid("close wrong network leg"))}
        Ok(plan)
    }

    pub(crate) fn with_authorized_close_send<T>(
        &self,authorized:&AuthorizedCloseOrder,current_safety_generation:u64,
        send:impl FnOnce(&CloseNetworkPlan)->T,
    )->Result<T,NativeStateError>{
        crate::native_hard_safety::require_native_mutation_egress()
            .map_err(|_|NativeStateError::Invalid("R32D6C1 review mutation egress locked"))?;
        let _lock=self.lock_exclusive()?;
        let actual=self.revalidate_native_close_permit(
            &authorized.permit,current_safety_generation)?;
        if actual!=authorized.plan{
            return Err(NativeStateError::Invalid("close plan changed before send"))
        }
        Ok(send(&actual))
    }

    pub fn record_native_close_observation(
        &self,permit:NativeClosePermit,observation:MutationObservation,
    )->Result<NativeAuthoritativeState,NativeStateError>{
        let (next,_)=self.transact(permit.generation,|state|{
            let p=state.active.as_mut().ok_or(NativeStateError::Invalid("active missing"))?;
            if p.provenance!=PositionProvenance::Native||p.reconciliation_required{
                return Err(NativeStateError::Invalid("close authority not native"))
            }
            if p.lifecycle.safety_generation!=permit.safety_generation{
                return Err(NativeStateError::Invalid("stale close safety"))
            }
            let pending=p.lifecycle.pending.as_ref()
                .ok_or(NativeStateError::Invalid("close pending missing"))?;
            if pending.intent_id!=permit.intent_id||pending.client_id!=permit.client_id
                ||pending.leg!=permit.leg{
                return Err(NativeStateError::Invalid("close observation identity mismatch"))
            }
            let mut exec=ExecutionState{intents:p.intents.clone()};
            p.lifecycle.apply_network_observation_atomic(&mut exec,&permit.intent_id,observation)
                .map_err(|_|NativeStateError::Invalid("close observation denied"))?;
            p.intents=exec.intents;
            Ok(())
        })?;
        Ok(next)
    }

    /// Read-only snapshot of a durable unresolved native close for an exact
    /// credentialed exchange GET. Never exposes a new network mutation permit.
    pub fn pending_native_close_query(
        &self,expected_generation:u64,
    )->Result<NativePendingCloseQuery,NativeStateError>{
        let state=self.load()?.ok_or(NativeStateError::Generation)?;
        if state.generation!=expected_generation{return Err(NativeStateError::Generation)}
        let p=state.active.as_ref().ok_or(NativeStateError::Invalid("no active position"))?;
        if p.provenance!=PositionProvenance::Native||p.reconciliation_required{
            return Err(NativeStateError::Invalid("non-native recovery query"))
        }
        let pending=p.lifecycle.pending.as_ref()
            .ok_or(NativeStateError::Invalid("no pending close"))?;
        if pending.requested_units==0{return Err(NativeStateError::Invalid("zero pending"))}
        let intent=p.intents.iter().find(|x|x.id==pending.intent_id
            && x.client_order_id.as_deref()==Some(pending.client_id.as_str()))
            .ok_or(NativeStateError::Invalid("pending identity invalid"))?;
        if !matches!(intent.state,IntentState::Prepared|IntentState::Unknown|IntentState::Acknowledged){
            return Err(NativeStateError::Invalid("pending not recovery-eligible"))
        }
        let (venue,symbol)=match pending.leg{
            CloseLeg::Futures=>(Venue::BinanceUsdm,p.core.futures_symbol.clone()),
            CloseLeg::Spot=>(Venue::OkxSpot,p.core.spot_symbol.clone()),
        };
        Ok(NativePendingCloseQuery{generation:state.generation,
            venue,symbol,client_id:pending.client_id.clone(),
            requested_atoms_e18:pending.requested_units})
    }

    /// Post-restart recovery requires verified exchange evidence supplied
    /// by a trusted reconciliation caller; it can never generate a send permit.
    pub(crate) fn recover_native_close(
        &self,expected_generation:u64,evidence_present:bool,evidence:RecoveryEvidence,
    )->Result<NativeAuthoritativeState,NativeStateError>{
        let (next,_)=self.transact(expected_generation,|state|{
            let p=state.active.as_mut().ok_or(NativeStateError::Invalid("active missing"))?;
            if p.provenance!=PositionProvenance::Native||p.reconciliation_required{
                return Err(NativeStateError::Invalid("close authority not native"))
            }
            let mut exec=ExecutionState{intents:p.intents.clone()};
            p.lifecycle.recover_pending_atomic(&mut exec,evidence_present,evidence)
                .map_err(|_|NativeStateError::Invalid("close recovery denied"))?;
            p.intents=exec.intents;
            Ok(())
        })?;
        Ok(next)
    }
}

fn legacy_mode(pos:&serde_json::Map<String,Value>)->Option<String>{
    pos.get("_meta").and_then(Value::as_object).and_then(|m|m.get("open_mode")).and_then(Value::as_str)
        .or_else(||pos.get("entry").and_then(Value::as_object).and_then(|m|m.get("mode")).and_then(Value::as_str))
        .map(|s|s.to_ascii_lowercase())
}
fn legacy_string<'a>(pos:&'a serde_json::Map<String,Value>,a:&str,b:&str,c:&str)->Option<&'a str>{
    pos.get(a).and_then(Value::as_str)
        .or_else(||pos.get(b).and_then(Value::as_object).and_then(|x|x.get(c)).and_then(Value::as_str))
}
/// R32D3: persist a sealed, fully cross-checked read-only fill/fee
/// checkpoint. This is NOT an exposure mutation or recovery approval:
/// existing intent remains Prepared/Unknown; RecoveryRequired remains.
impl NativeStateStore{
    pub fn record_native_entry_recovery_checkpoint(
        &self, detail:crate::native_entry_recovery::EntryFillDetailReport,
    )->Result<(NativeAuthoritativeState,NativeEntryRecoveryWrite),NativeStateError>{
        if detail.may_send_order{
            return Err(NativeStateError::Invalid("recovery may not send"))
        }
        let proof=detail.sealed_checkpoint
            .ok_or(NativeStateError::Invalid("incomplete recovery evidence"))?;
        if detail.fill_count!=proof.ledger.fills.len()
            ||detail.filled_atoms_e18!=e12_to_e18(proof.ledger.filled_qty_atoms())?
            ||detail.quoted_fee_cost_atoms_e12!=proof.ledger.fee_cost_quote_atoms
            ||detail.signed_base_fee_atoms_e18!=proof.signed_base_fee_atoms_e18{
            return Err(NativeStateError::Invalid("recovery detail/proof mismatch"))
        }
        // Single-writer flock holds through generation check and durable
        // atomic rename/fsync. A replay can never insert duplicate trade IDs.
        let _lock=self.lock_exclusive()?;
        let mut state=self.load()?.ok_or(NativeStateError::Generation)?;
        let entry=state.entry.as_mut().ok_or(NativeStateError::Invalid("entry missing"))?;
        if state.active.is_some()||entry.phase!=NativeEntryPhase::RecoveryRequired{
            return Err(NativeStateError::Invalid("entry recovery gate missing"))
        }
        let intent=entry.intents.iter().find(|i|i.id==proof.intent_id
            && i.client_order_id.as_deref()==Some(proof.client_id.as_str()))
            .ok_or(NativeStateError::Invalid("checkpoint intent mismatch"))?;
        if !matches!(intent.state,IntentState::Prepared|IntentState::Unknown){
            return Err(NativeStateError::Invalid("checkpoint intent resolved"))
        }
        if let Some(q)=entry.futures_uncertain_quarantine.as_ref(){
            q.validate_get_checkpoint(entry,&proof)?;
        }
        if let Some(old)=&entry.recovery_checkpoint{
            if old==&proof{
                return Ok((state,NativeEntryRecoveryWrite::Idempotent))
            }
            return Err(NativeStateError::Invalid("conflicting recovery evidence; manual review"))
        }
        if state.generation!=detail.source_generation{
            return Err(NativeStateError::Generation)
        }
        entry.recovery_checkpoint=Some(proof);
        state.generation=state.generation.checked_add(1)
            .ok_or(NativeStateError::Generation)?;
        state.validate()?;
        self.persist_internal(&state,StatePersistFailpoint::None)?;
        Ok((state,NativeEntryRecoveryWrite::Recorded))
    }
}

/// R32D6B: captures TWO native leg ledgers under one exclusive flock, exact
/// state generation, checksum, temp fsync, rename and parent dir fsync.
/// Provenance remains native-only; no external cross-exchange approval.
impl NativeStateStore{
    pub fn record_native_dual_leg_evidence(
        &self,expected_generation:u64
    )->Result<(NativeAuthoritativeState,NativeDualLegWrite),NativeStateError>{
        let _lock=self.lock_exclusive()?;
        let mut state=self.load()?.ok_or(NativeStateError::Generation)?;
        if state.generation!=expected_generation{
            return Err(NativeStateError::Generation)
        }
        if state.active.is_some(){
            return Err(NativeStateError::Invalid("entry dual proof cannot touch active"))
        }
        let entry=state.entry.as_mut()
            .ok_or(NativeStateError::Invalid("native entry absent"))?;
        if entry.dual_leg_evidence.is_some(){
            // Reread and revalidate per-leg economic ledger against checkpoint.
            // Exact replay does not increment a generation or overwrite bytes.
            return Ok((state,NativeDualLegWrite::Idempotent))
        }
        let evidence=entry.dual_leg_evidence_candidate(expected_generation)?;
        entry.dual_leg_evidence=Some(evidence);
        state.generation=state.generation.checked_add(1)
            .ok_or(NativeStateError::Generation)?;
        state.validate()?;
        self.persist_internal(&state,StatePersistFailpoint::None)?;
        Ok((state,NativeDualLegWrite::Recorded))
    }
}

/// R32D6B2A: preserve an independently collected but WINDOW-BOUNDED
/// spot history/bills/cash correlation. This is NEVER a trade permission.
impl NativeStateStore{
    pub fn record_settled_spot_window_evidence(
        &self,report:crate::native_settled_spot_audit::SettledSpotAuditReport
    )->Result<(NativeAuthoritativeState,NativeSpotAuditWrite),NativeStateError>{
        if report.may_send_order||report.exchange_global_completeness_proven
            ||report.cross_venue_atomicity_proven||!report.candidate_window_matched{
            return Err(NativeStateError::Invalid("spot audit report cannot release trading"))
        }
        let proof=report.sealed
            .ok_or(NativeStateError::Invalid("spot window evidence unsealed"))?;
        if !proof.all_order_fills_and_bills_in_window_matched
            ||proof.may_send_order||proof.exchange_global_completeness_proven
            ||proof.cross_venue_atomicity_proven
            ||report.fill_rows!=proof.spot_ledger.fills.len()
            ||report.bill_rows!=proof.checked_bill_ids.len()
            ||report.fills_pages!=proof.fills_history_pages
            ||report.bills_pages!=proof.cash_bill_pages
            ||report.cash_e18!=proof.observed_cash_atoms_e18{
            return Err(NativeStateError::Invalid("sealed spot audit/report mismatch"))
        }
        let _lock=self.lock_exclusive()?;
        let mut state=self.load()?.ok_or(NativeStateError::Generation)?;
        if state.active.is_some(){
            return Err(NativeStateError::Invalid("spot audit cannot touch active position"))
        }
        let entry=state.entry.as_mut()
            .ok_or(NativeStateError::Invalid("native entry absent"))?;
        if entry.phase!=NativeEntryPhase::RecoveryRequired{
            return Err(NativeStateError::Invalid("pending recovery no longer required"))
        }
        if let Some(old)=&entry.settled_spot_window_evidence{
            if old==&proof{
                return Ok((state,NativeSpotAuditWrite::Idempotent))
            }
            return Err(NativeStateError::Invalid("conflicting exchange-window audit"))
        }
        if state.generation!=proof.source_generation{
            return Err(NativeStateError::Generation)
        }
        if entry.dual_leg_evidence.as_ref().map_or(true,|dual|
            dual.native_generation_at_capture!=proof.original_dual_capture_generation){
            return Err(NativeStateError::Invalid("spot audit not anchored to dual generation"))
        }
        entry.settled_spot_window_evidence=Some(proof);
        state.generation=state.generation.checked_add(1)
            .ok_or(NativeStateError::Generation)?;
        state.validate()?;
        self.persist_internal(&state,StatePersistFailpoint::None)?;
        Ok((state,NativeSpotAuditWrite::Recorded))
    }
}

/// R32D6B2B: immutable combined observational window, NO release authority.
/// This does not re-execute network calls. Only accepts an internal report
/// sealed by the coordinated signed GET audit; raw reports never authorize
/// order network mutations.
impl NativeStateStore{
    pub fn record_native_joint_window(
        &self,report:crate::native_joint_window::JointWindowReport,
    )->Result<(NativeAuthoritativeState,NativeJointWindowWrite),NativeStateError>{
        if !report.read_only_candidate||report.may_send_order
            ||report.exchange_global_history_complete
            ||report.exchange_cross_venue_atomicity_proven
            ||report.income_checks!=2{
            return Err(NativeStateError::Invalid("joint report cannot certify trade"))
        }
        let proof=report.sealed
            .ok_or(NativeStateError::Invalid("missing internally sealed joint diagnostic"))?;
        if proof.may_send_order||proof.exchange_global_history_complete
            ||proof.exchange_cross_venue_atomicity_proven
            ||!proof.spot_window_rechecked
            ||!proof.futures_income_bracket_matched
            ||!proof.futures_symbol_trade_window_matched
            ||proof.income_check_repetitions!=2
            ||report.okx_fills_count!=proof.spot.spot_ledger.fills.len()
            ||report.okx_bills_count!=proof.spot.checked_bill_ids.len()
            ||report.futures_trades_count!=proof.futures_trade_ids.len()
            ||report.futures_income_commissions_count!=proof.futures_trade_ids.len(){
            return Err(NativeStateError::Invalid("joint report and sealed facts differ"))
        }
        let _lock=self.lock_exclusive()?;
        let mut state=self.load()?.ok_or(NativeStateError::Generation)?;
        if state.active.is_some(){return Err(NativeStateError::Invalid("joint evidence cannot change active position"))}
        let entry=state.entry.as_mut().ok_or(NativeStateError::Invalid("no native entry"))?;
        if entry.phase!=NativeEntryPhase::RecoveryRequired{
            return Err(NativeStateError::Invalid("joint evidence only for unresolved recovery"))
        }
        if let Some(previous)=&entry.joint_window_evidence{
            if previous==&proof
                && state.generation==proof.source_generation.checked_add(1)
                    .ok_or(NativeStateError::Generation)?{
                return Ok((state,NativeJointWindowWrite::Idempotent))
            }
            return Err(NativeStateError::Invalid("conflicting/stale joint evidence"))
        }
        if state.generation!=proof.source_generation{return Err(NativeStateError::Generation)}
        entry.joint_window_evidence=Some(proof);
        state.generation=state.generation.checked_add(1)
            .ok_or(NativeStateError::Generation)?;
        state.validate()?;
        self.persist_internal(&state,StatePersistFailpoint::None)?;
        Ok((state,NativeJointWindowWrite::Recorded))
    }
}

/// R32D6C2: one durable acknowledgement of a READ-ONLY fully paginated
/// settled SPOT fill history, atomic with replacing the cumulative-only native
/// first-leg ledger. Never manufactures fill IDs and never grants send rights.
impl NativeStateStore{
    pub fn record_native_spot_first_leg_evidence(
        &self,report:crate::native_spot_prehedge::SpotPreHedgeReport,
    )->Result<(NativeAuthoritativeState,NativeSpotFirstLegWrite),NativeStateError>{
        if !report.is_complete_readonly_candidate||report.may_send_order{
            return Err(NativeStateError::Invalid("first-leg report is not a no-send candidate"))
        }
        let proof=report.sealed
            .ok_or(NativeStateError::Invalid("missing sealed first-leg evidence"))?;
        if proof.may_send_order||proof.exchange_bill_history_proven
            ||proof.exchange_global_completeness_proven
            ||!proof.cash_bracket_matched||proof.fill_bill_ids.len()!=proof.exact_ledger.fills.len()
            ||report.source_generation!=proof.source_generation
            ||report.pages!=proof.filled_pages||report.fills!=proof.exact_ledger.fills.len()
            ||report.fee_quote_atoms_e12!=proof.exact_ledger.fee_cost_quote_atoms
            ||report.cash_atoms_e18!=proof.observed_base_cash_atoms_e18{
            return Err(NativeStateError::Invalid("first-leg diagnostic report disagrees with seal"))
        }
        let _lock=self.lock_exclusive()?;
        let mut state=self.load()?.ok_or(NativeStateError::Generation)?;
        if state.active.is_some(){
            return Err(NativeStateError::Invalid("cannot capture first leg of active"))
        }
        let entry=state.entry.as_mut()
            .ok_or(NativeStateError::Invalid("first-leg entry absent"))?;
        if let Some(previous)=&entry.spot_prehedge_evidence{
            if previous==&proof && state.generation
                ==proof.source_generation.checked_add(1).ok_or(NativeStateError::Generation)?{
                return Ok((state,NativeSpotFirstLegWrite::Idempotent))
            }
            return Err(NativeStateError::Invalid("conflicting first-leg evidence"))
        }
        if state.generation!=proof.source_generation{
            return Err(NativeStateError::Generation)
        }
        if entry.phase!=NativeEntryPhase::SpotExposed
            ||entry.plan.safety_generation!=proof.safety_generation
            ||entry.economics.entry.spot.orders.len()!=1
            ||entry.economics.entry.futures.orders.len()!=0{
            return Err(NativeStateError::Invalid("first-leg phase/safety mismatch"))
        }
        if let Some(binding)=&entry.policy_binding{
            binding.verify_exact_spot_fills(&entry.plan,&proof.exact_ledger)
                .map_err(|_|NativeStateError::Invalid(
                    "R32D6C8B1 original signed fills/fees exceed bound quote limit"))?;
        }
        let existing=entry.economics.entry.spot.order(&proof.spot_intent_id)
            .map_err(|_|NativeStateError::Economics)?;
        if existing.venue!="okx"||existing.side!="buy"
            ||existing.symbol!=entry.plan.spot_symbol
            ||!existing.terminal||!existing.cumulative_truth_present
            ||existing.filled_qty_atoms()!=proof.exact_ledger.filled_qty_atoms()
            ||existing.vwap_atoms()!=proof.exact_ledger.vwap_atoms()
            ||existing.client_order_id!=proof.spot_client_id
            ||existing.requested_qty_atoms!=proof.exact_ledger.requested_qty_atoms
            ||existing.intent_id!=proof.spot_intent_id
            ||existing.fills.len()>0{
            return Err(NativeStateError::Invalid("first-leg native/exchange originals differ"))
        }
        let id=proof.spot_intent_id.clone();
        let saved=entry.economics.entry.spot.order_mut(&id)
            .map_err(|_|NativeStateError::Economics)?;
        *saved=proof.exact_ledger.clone();
        entry.spot_prehedge_evidence=Some(proof);
        entry.require_spot_prehedge_provenance()?;
        state.generation=state.generation.checked_add(1).ok_or(NativeStateError::Generation)?;
        state.validate()?;
        self.persist_internal(&state,StatePersistFailpoint::None)?;
        Ok((state,NativeSpotFirstLegWrite::Recorded))
    }
}

/// R32D6C3: independent currency-wide account-bills proof, committed only
/// when exact native generation/leg/previous per-fill source still match.
/// This is a diagnostic prerequisite, NEVER a private-network order permit.
impl NativeStateStore{
    pub fn record_native_spot_prehedge_bills_evidence(
        &self,report:crate::native_spot_prehedge_bills::SpotBillsReport,
    )->Result<(NativeAuthoritativeState,NativeSpotBillsWrite),NativeStateError>{
        if !report.window_matched||report.account_history_globally_complete
            ||report.cross_venue_atomicity_proven||report.may_send_order{
            return Err(NativeStateError::Invalid("cash report cannot unlock network mutation"))
        }
        let proof=report.sealed
            .ok_or(NativeStateError::Invalid("missing sealed currency-wide source"))?;
        if proof.source_generation!=report.source_generation
            ||proof.verified_bill_ids.len()!=report.rows
            ||proof.account_bill_pages!=report.pages
            ||proof.observed_base_cash_atoms_e18!=report.observed_cash_e18
            ||proof.aggregate_base_delta_atoms_e18!=report.base_delta_e18
            ||!proof.exact_bills_for_first_leg_matched
            ||!proof.base_cash_ancestry_to_original_baseline_matched
            ||!proof.source_balance_bracket_matched
            ||proof.account_history_globally_complete
            ||proof.cross_venue_atomicity_proven||proof.may_send_order{
            return Err(NativeStateError::Invalid("sealed bills and summary conflict"))
        }
        let _lock=self.lock_exclusive()?;
        let mut state=self.load()?.ok_or(NativeStateError::Generation)?;
        if state.active.is_some(){
            return Err(NativeStateError::Invalid("can't audit bills of active position"))
        }
        let entry=state.entry.as_mut()
            .ok_or(NativeStateError::Invalid("no pending native entry"))?;
        if entry.phase!=NativeEntryPhase::SpotExposed{
            return Err(NativeStateError::Invalid("first leg no longer spot-only"))
        }
        if let Some(previous)=&entry.spot_prehedge_bills_evidence{
            if previous==&proof && state.generation
                ==proof.source_generation.checked_add(1)
                    .ok_or(NativeStateError::Generation)?{
                return Ok((state,NativeSpotBillsWrite::Idempotent))
            }
            return Err(NativeStateError::Invalid("conflicting currency-wide cash proof"))
        }
        if state.generation!=proof.source_generation{
            return Err(NativeStateError::Generation)
        }
        entry.require_spot_prehedge_provenance()?;
        if entry.spot_prehedge_evidence.as_ref()
            .map(|first|first.exact_ledger.fee_cost_quote_atoms)
            !=Some(report.quote_fee_e12){
            return Err(NativeStateError::Invalid("bills quote fees do not match previous fills"))
        }
        entry.spot_prehedge_bills_evidence=Some(proof);
        entry.require_spot_prehedge_bills_provenance()?;
        state.generation=state.generation.checked_add(1)
            .ok_or(NativeStateError::Generation)?;
        state.validate()?;
        self.persist_internal(&state,StatePersistFailpoint::None)?;
        Ok((state,NativeSpotBillsWrite::Recorded))
    }
}

/// R32D6C5: durable signed-GET FUTURES entry account/trade/income
/// corroboration. This is NOT a Hedged-position authority or network permit.
impl NativeStateStore{
    pub fn record_native_futures_entry_window(
        &self,report:crate::native_futures_entry_audit::FuturesEntryWindowReport,
    )->Result<(NativeAuthoritativeState,NativeFuturesEntryWindowWrite),NativeStateError>{
        if !report.exact_read_only_candidate||report.account_checks!=2
            ||report.may_send_order||report.may_promote_hedged{
            return Err(NativeStateError::Invalid("futures diagnostic report cannot release"))
        }
        let proof=report.sealed
            .ok_or(NativeStateError::Invalid("missing sealed futures entry evidence"))?;
        if report.trade_count!=proof.trade_ids.len()
            ||report.commission_count!=proof.trade_ids.len()
            ||proof.may_send_order||proof.may_promote_hedged
            ||proof.exchange_global_history_complete||proof.cross_venue_atomicity_proven
            ||!proof.exact_fills_commissions_and_symbol_history_matched
            ||!proof.income_position_wallet_bracket_matched
            ||proof.account_read_repetitions!=2{
            return Err(NativeStateError::Invalid("sealed futures evidence not matching report"))
        }
        let _lock=self.lock_exclusive()?;
        let mut state=self.load()?.ok_or(NativeStateError::Generation)?;
        if state.active.is_some(){return Err(NativeStateError::Invalid("active already exists"))}
        let entry=state.entry.as_mut()
            .ok_or(NativeStateError::Invalid("futures native entry missing"))?;
        if entry.phase!=NativeEntryPhase::RecoveryRequired{
            return Err(NativeStateError::Invalid("futures entry not in recovery"))
        }
        if let Some(previous)=&entry.futures_entry_window_evidence{
            if previous==&proof && state.generation
                ==proof.source_generation.checked_add(1)
                    .ok_or(NativeStateError::Generation)?{
                return Ok((state,NativeFuturesEntryWindowWrite::Idempotent))
            }
            return Err(NativeStateError::Invalid("conflicting futures audit"))
        }
        if state.generation!=proof.source_generation{return Err(NativeStateError::Generation)}
        let cp=entry.recovery_checkpoint.as_ref()
            .ok_or(NativeStateError::Invalid("missing detailed futures checkpoint"))?;
        if cp.leg!=NativeEntryLeg::Futures||cp.intent_id!=proof.futures_intent_id
            ||cp.client_id!=proof.futures_client_id||cp.order_id!=proof.futures_order_id
            ||cp.ledger.fills.keys().cloned().collect::<Vec<_>>()!=proof.trade_ids
            ||cp.ledger.fee_cost_quote_atoms!=proof.signed_quote_fee_cost_atoms_e12{
            return Err(NativeStateError::Invalid("futures checkpoint IDs or fees changed"))
        }
        entry.futures_entry_window_evidence=Some(proof);
        state.generation=state.generation.checked_add(1)
            .ok_or(NativeStateError::Generation)?;
        state.validate()?;
        self.persist_internal(&state,StatePersistFailpoint::None)?;
        Ok((state,NativeFuturesEntryWindowWrite::Recorded))
    }
}

/// R32D6C6: record strictly observational two-venue prospective lineage.
/// Must NOT change entry exposure, intent state, phase or mint a network token.
impl NativeStateStore{
    pub fn record_native_prospective_joint_window(
        &self,report:crate::native_prospective_joint::ProspectiveJointReport,
    )->Result<(NativeAuthoritativeState,NativeProspectiveJointWrite),NativeStateError>{
        if !report.candidate_bound_to_same_native_entry
            ||report.may_send_order||report.may_promote_hedged
            ||report.okx_balances_read!=2||report.binance_account_checks!=2{
            return Err(NativeStateError::Invalid("not a read-only prospective candidate"))
        }
        let proof=report.sealed
            .ok_or(NativeStateError::Invalid("prospective joint seal missing"))?;
        if proof.source_generation!=report.source_generation
            ||proof.spot_trade_ids.len()!=report.spot_fill_count
            ||proof.futures_trade_ids.len()!=report.futures_fill_count
            ||proof.exposed_spot_unhedged_atoms_e18!=report.spot_unhedged_atoms_e18
            ||proof.may_send_order||proof.may_promote_hedged
            ||proof.exchange_global_history_complete
            ||proof.cross_venue_exchange_snapshot_atomicity_proven
            ||!proof.okx_balance_bracket_matched
            ||!proof.binance_income_and_position_bracket_matched
            ||!proof.foreign_symbol_trades_absent_in_bounded_window{
            return Err(NativeStateError::Invalid("prospective result/receipt disagree"))
        }
        let _lock=self.lock_exclusive()?;
        let mut state=self.load()?.ok_or(NativeStateError::Generation)?;
        if state.active.is_some(){return Err(NativeStateError::Invalid("active native pos forbidden"))}
        let entry=state.entry.as_mut()
            .ok_or(NativeStateError::Invalid("prospective entry not found"))?;
        if entry.phase!=NativeEntryPhase::RecoveryRequired{
            return Err(NativeStateError::Invalid("cannot certify non-recovery entry"))
        }
        if let Some(saved)=&entry.prospective_joint_evidence{
            if saved==&proof && state.generation
                ==proof.source_generation.checked_add(1)
                    .ok_or(NativeStateError::Generation)?{
                return Ok((state,NativeProspectiveJointWrite::Idempotent))
            }
            return Err(NativeStateError::Invalid("conflicting prospective joint source"))
        }
        if state.generation!=proof.source_generation{return Err(NativeStateError::Generation)}
        entry.prospective_joint_evidence=Some(proof);
        state.generation=state.generation.checked_add(1)
            .ok_or(NativeStateError::Generation)?;
        state.validate()?;
        self.persist_internal(&state,StatePersistFailpoint::None)?;
        Ok((state,NativeProspectiveJointWrite::Recorded))
    }
}

/// R32D6C8B2A: separate durable PAPER accounting, no private exchange
/// mutation, no real entry permit, no automatic Hedged promotion. An entry
/// reservation is a bounded synthetic slot, NOT a verified exchange fill.
impl NativeStateStore{
    pub fn reserve_paper_daily_slot(
        &self,expected_generation:u64,config_path:&std::path::Path,
        now_ms:u64,entry_id:&str,
    )->Result<(NativeAuthoritativeState,crate::native_daily_risk::RiskWrite),NativeStateError>{
        use crate::native_daily_risk::{NativeDailyRiskLedger,NativePaperRiskRecord,RiskWrite};
        let cfg=crate::native_trading_config::NativeTradingConfig::load(config_path)
            .map_err(|_|NativeStateError::Invalid("R32D6C8B2A invalid/missing YAML"))?;
        let _lock=self.lock_exclusive()?;
        let fresh=crate::native_trading_config::NativeTradingConfig::load(config_path)
            .map_err(|_|NativeStateError::Invalid("R32D6C8B2A stale/missing YAML"))?;
        if cfg.sha256!=fresh.sha256{
            return Err(NativeStateError::Invalid("R32D6C8B2A YAML changed before CAS"))
        }
        let mut state=self.load()?.ok_or(NativeStateError::Generation)?;
        if state.generation!=expected_generation{return Err(NativeStateError::Generation)}
        if state.entry.is_some()||state.active.is_some(){
            return Err(NativeStateError::Invalid("R32D6C8B2A native position already occupied"))
        }
        let ledger=if let Some(existing)=&state.paper_risk_ledger{
            existing.clone()
        }else{NativeDailyRiskLedger::new(&fresh,now_ms)
            .map_err(|_|NativeStateError::Invalid("R32D6C8B2A missing safe paper policy"))?
        };
        ledger.validate().map_err(|_|NativeStateError::Invalid("R32D6C8B2A risk ledger tampered"))?;
        ledger.ensure_config(&fresh).map_err(|_|NativeStateError::Invalid("R32D6C8B2A config digest drift"))?;
        if let Some(old)=ledger.records.get(entry_id){
            if now_ms<ledger.last_seen_at_ms{
                return Err(NativeStateError::Invalid("R32D6C8B2A clock moved backward"))
            }
            if old.entry_id==entry_id{
                return Ok((state,RiskWrite::Idempotent))
            }
            return Err(NativeStateError::Invalid("R32D6C8B2A conflicting paper identity"))
        }
        ledger.can_reserve_paper(&fresh,now_ms,entry_id).map_err(|_|NativeStateError::Invalid(
            "R32D6C8B2A daily limit, position occupancy, disabled mode or monotonic clock"))?;
        let mut ledger=ledger;
        let day=crate::native_daily_risk::yekt_day_index(now_ms)
            .map_err(|_|NativeStateError::Invalid("R32D6C8B2A invalid YEKT day"))?;
        ledger.records.insert(entry_id.into(),NativePaperRiskRecord{
            entry_id:entry_id.into(),day_yekt:day,reserved_at_ms:now_ms,
            released_at_ms:None,close_receipt_id:None,
        });
        ledger.last_seen_at_ms=now_ms;
        ledger.validate().map_err(|_|NativeStateError::Invalid("R32D6C8B2A ledger capacity corrupt"))?;
        state.paper_risk_ledger=Some(ledger);
        state.generation=state.generation.checked_add(1).ok_or(NativeStateError::Generation)?;
        state.validate()?;
        self.persist_internal(&state,StatePersistFailpoint::None)?;
        Ok((state,RiskWrite::Recorded))
    }

    pub fn release_paper_daily_slot(
        &self,expected_generation:u64,config_path:&std::path::Path,
        now_ms:u64,entry_id:&str,close_receipt_id:&str,
    )->Result<(NativeAuthoritativeState,crate::native_daily_risk::RiskWrite),NativeStateError>{
        use crate::native_daily_risk::RiskWrite;
        let _lock=self.lock_exclusive()?;
        let cfg=crate::native_trading_config::NativeTradingConfig::load(config_path)
            .map_err(|_|NativeStateError::Invalid("R32D6C8B2A YAML missing on paper close"))?;
        let mut state=self.load()?.ok_or(NativeStateError::Generation)?;
        if state.generation!=expected_generation{return Err(NativeStateError::Generation)}
        let ledger=state.paper_risk_ledger.as_mut()
            .ok_or(NativeStateError::Invalid("R32D6C8B2A no paper journal"))?;
        ledger.validate().map_err(|_|NativeStateError::Invalid("R32D6C8B2A invalid paper journal"))?;
        ledger.ensure_config(&cfg).map_err(|_|NativeStateError::Invalid("R32D6C8B2A config drift"))?;
        if now_ms<ledger.last_seen_at_ms{
            return Err(NativeStateError::Invalid("R32D6C8B2A backward clock"))
        }
        if close_receipt_id.is_empty()||close_receipt_id.len()>96
          ||!close_receipt_id.bytes().all(|c|c.is_ascii_alphanumeric()||c==b'-'||c==b'_'||c==b'.'){
            return Err(NativeStateError::Invalid("R32D6C8B2A close identity invalid"))
        }
        let record=ledger.records.get(entry_id).ok_or(NativeStateError::Invalid(
            "R32D6C8B2A original paper reservation not found"))?;
        if record.released_at_ms.is_some(){
            if record.close_receipt_id.as_deref()==Some(close_receipt_id){
                return Ok((state,RiskWrite::Idempotent))
            }
            return Err(NativeStateError::Invalid("R32D6C8B2A conflicting close receipt"))
        }
        let start=record.reserved_at_ms;
        if now_ms<start||ledger.records.values().any(|v|
            v.close_receipt_id.as_deref()==Some(close_receipt_id)){
            return Err(NativeStateError::Invalid("R32D6C8B2A close time/identity conflict"))
        }
        let record=ledger.records.get_mut(entry_id).ok_or(NativeStateError::Generation)?;
        record.released_at_ms=Some(now_ms);
        record.close_receipt_id=Some(close_receipt_id.into());
        ledger.last_seen_at_ms=now_ms;
        ledger.validate().map_err(|_|NativeStateError::Invalid("R32D6C8B2A invalid close journal"))?;
        state.generation=state.generation.checked_add(1).ok_or(NativeStateError::Generation)?;
        state.validate()?;
        self.persist_internal(&state,StatePersistFailpoint::None)?;
        Ok((state,RiskWrite::Recorded))
    }

    pub fn reject_real_daily_slot(
        &self,expected_generation:u64,config_path:&std::path::Path,
        now_ms:u64,
    )->Result<(),NativeStateError>{
        let cfg=crate::native_trading_config::NativeTradingConfig::load(config_path)
            .map_err(|_|NativeStateError::Invalid("R32D6C8B2A invalid YAML"))?;
        let state=self.load()?.ok_or(NativeStateError::Generation)?;
        if state.generation!=expected_generation{return Err(NativeStateError::Generation)}
        if let Some(ledger)=&state.paper_risk_ledger{
            ledger.can_reserve_real(&cfg,now_ms).map_err(|_|NativeStateError::Invalid(
                "R32D6C8B2A REAL daily limit is ZERO"))?;
        }
        Err(NativeStateError::Invalid("R32D6C8B2A REAL daily limit is ZERO"))
    }
}

/// R32D6C8B2B1: record read-only, source-bound and categorically incomplete
/// risk observation, with original joint C6 proof and CAS+fsync lineage.
/// Does NOT count an exchange-verified close, daily net PNL or issue a permit.
impl NativeStateStore{
    pub fn record_real_risk_diagnostic(
        &self,expected_generation:u64,config_path:&std::path::Path,
        now_ms:u64,
    )->Result<(NativeAuthoritativeState,crate::native_real_risk_watch::RealWatchWrite),NativeStateError>{
        use crate::native_real_risk_watch::{
            NativeRealRiskObservation,RealWatchWrite,
        };
        let before_cfg=crate::native_trading_config::NativeTradingConfig::load(config_path)
            .map_err(|_|NativeStateError::Invalid("R32D6C8B2B1 config absent or invalid"))?;
        let _lock=self.lock_exclusive()?;
        let cfg=crate::native_trading_config::NativeTradingConfig::load(config_path)
            .map_err(|_|NativeStateError::Invalid("R32D6C8B2B1 config drift/absent"))?;
        if before_cfg.sha256!=cfg.sha256{
            return Err(NativeStateError::Invalid("R32D6C8B2B1 config drift before CAS"))
        }
        let mut state=self.load()?.ok_or(NativeStateError::Generation)?;
        if state.generation!=expected_generation{return Err(NativeStateError::Generation)}
        if state.active.is_some(){return Err(NativeStateError::Invalid(
            "R32D6C8B2B1 no real risk diagnostic on active position"))}
        let entry=state.entry.as_mut().ok_or(NativeStateError::Invalid(
            "R32D6C8B2B1 no native recovery entry"))?;
        if let Some(saved)=&entry.real_risk_observation{
            if saved.config_sha256!=cfg.sha256{
                return Err(NativeStateError::Invalid("R32D6C8B2B1 immutable config changed"))
            }
            saved.validate_against(entry,state.generation)
                .map_err(|_|NativeStateError::Invalid("R32D6C8B2B1 corrupt observation"))?;
            if now_ms<saved.captured_at_ms{
                return Err(NativeStateError::Invalid("R32D6C8B2B1 clock moved backward"))
            }
            return Ok((state,RealWatchWrite::Idempotent))
        }
        let obs=NativeRealRiskObservation::from_bounded_entry(
            entry,state.generation,now_ms,&cfg)
            .map_err(|_|NativeStateError::Invalid(
                "R32D6C8B2B1 no complete bounded opening proof or stale source"))?;
        if let Some(original)=&entry.policy_binding{
            if original.config_sha256!=cfg.sha256{
                return Err(NativeStateError::Invalid("R32D6C8B2B1 bound YAML mismatched"))
            }
        }
        entry.real_risk_observation=Some(obs);
        state.generation=state.generation.checked_add(1).ok_or(NativeStateError::Generation)?;
        state.validate()?;
        self.persist_internal(&state,StatePersistFailpoint::None)?;
        Ok((state,RealWatchWrite::Recorded))
    }
}

pub fn import_legacy_python_state(raw:&[u8])->Result<NativeAuthoritativeState,NativeStateError>{
    let root:Value=serde_json::from_slice(raw).map_err(|_|NativeStateError::Json)?;
    let positions=root.get("positions").and_then(Value::as_object)
        .ok_or(NativeStateError::Legacy("positions missing"))?;
    let closed_ignored=root.get("closed_positions").and_then(Value::as_object)
        .map(|m|m.values().map(|v|v.as_array().map(|a|a.len()as u64).unwrap_or(0)).sum()).unwrap_or(0);
    let source_hash=hex(&sha256(raw));
    if positions.is_empty(){return Ok(NativeAuthoritativeState::empty_from_legacy(source_hash,closed_ignored))}
    if positions.len()!=1{return Err(NativeStateError::Legacy("multiple active positions"))}
    let (base_raw,value)=positions.iter().next().unwrap();
    let pos=value.as_object().ok_or(NativeStateError::Legacy("position object"))?;
    let mode=legacy_mode(pos).ok_or(NativeStateError::Legacy("open mode missing"))?;
    if mode=="paper"{return Err(NativeStateError::Legacy("active paper position"))}
    if mode!="live"&&mode!="real"{return Err(NativeStateError::Legacy("unsupported open mode"))}
    let base=base_raw.to_ascii_uppercase();
    let spot_symbol=legacy_string(pos,"o_symbol","spot","symbol")
        .ok_or(NativeStateError::Legacy("spot symbol missing"))?.to_string();
    let futures_symbol=legacy_string(pos,"b_symbol","fut","symbol")
        .ok_or(NativeStateError::Legacy("futures symbol missing"))?.to_string();
    let baseline=value_u128_e18(pos.get("spot_baseline_amount").ok_or(NativeStateError::Legacy("spot baseline missing"))?)?;
    let spot_net=value_u128_e18(pos.get("spot_net_amount").ok_or(NativeStateError::Legacy("spot net missing"))?)?;
    let fut_obj=pos.get("fut").and_then(Value::as_object).ok_or(NativeStateError::Legacy("fut missing"))?;
    let fut_amount=value_u128_e18(fut_obj.get("amount").ok_or(NativeStateError::Legacy("fut amount missing"))?)?;
    let order_quote=value_u128_e18(pos.get("order_usdt").ok_or(NativeStateError::Legacy("order_usdt missing"))?)?;
    let opened_at_ms=value_seconds_to_ms(pos.get("opened_at").ok_or(NativeStateError::Legacy("opened_at missing"))?)?;
    if spot_net==0||fut_amount==0{return Err(NativeStateError::Legacy("zero active amount"))}
    let mut lifecycle=PositionLifecycle::new_hedged(fut_amount,0)
        .map_err(|_|NativeStateError::Legacy("lifecycle init"))?;
    lifecycle.state=LifecycleState::RecoveryRequired;
    let bundle=NativePositionBundle{
        core:NativePositionCore{base,spot_symbol,futures_symbol,opened_at_ms,
            spot_baseline_atoms_e18:baseline,spot_net_atoms_e18:spot_net,
            futures_target_atoms_e18:fut_amount,order_quote_atoms_e18:order_quote},
        lifecycle,intents:Vec::new(),economics:None,
        provenance:PositionProvenance::LegacyPythonImport,reconciliation_required:true,
        origin:None,
    };
    bundle.validate()?;
    Ok(NativeAuthoritativeState{
        schema_version:NATIVE_STATE_SCHEMA_VERSION,generation:1,entry:None,active:Some(bundle),
        paper_risk_ledger:None,pre_entry_binance_baseline:None,
        legacy_source_sha256:Some(source_hash),legacy_closed_positions_ignored:closed_ignored,
    })
}

#[cfg(test)]
mod tests{
    use super::*;
    use crate::fill_economics::{
        decimal_to_atoms,EntryEconomicsState,EntryTiming,ExitEconomicsState,LegFillAggregate,
    };
    use std::time::{SystemTime,UNIX_EPOCH};

    fn root(tag:&str)->PathBuf{
        std::env::temp_dir().join(format!("spotarb-r28-{tag}-{}",
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()))
    }
    fn empty()->NativeAuthoritativeState{
        NativeAuthoritativeState::empty_from_legacy("11".repeat(32),0)
    }
    fn live_fixture()->Vec<u8>{
        br#"{"positions":{"BNT":{"status":"opened","opened_at":1780000000.125,
        "spot_baseline_amount":0.125,"spot_net_amount":1.25,"order_usdt":10.0,
        "o_symbol":"BNT/USDT","b_symbol":"BNT/USDT:USDT",
        "fut":{"symbol":"BNT/USDT:USDT","amount":1.25},
        "_meta":{"open_mode":"live"}}},"closed_positions":{"LSK":[{"status":"closed"}]}}"#.to_vec()
    }
    fn agg(venue:&str,symbol:&str,side:&str,id:&str,qty:&str,px:&str)->LegFillAggregate{
        let q=decimal_to_atoms(qty).unwrap();
        let p=decimal_to_atoms(px).unwrap();
        let mut a=LegFillAggregate::new(venue,symbol,side,q).unwrap();
        let cid=format!("c-{id}");
        let oid=format!("o-{id}");
        a.add_order(id,&cid,q).unwrap();
        a.apply_cumulative_truth(q,Some(p),true,Some("filled"),Some(&oid),Some(10),1).unwrap();
        a
    }
    fn pristine_entry_economics()->PositionEconomicsState{
        let q=decimal_to_atoms("1").unwrap();
        PositionEconomicsState{
            entry:EntryEconomicsState{
                signal_gross_microbps:100_000_000,
                signal_expected_net_microbps:80_000_000,
                signal_spot_price_atoms:Some(decimal_to_atoms("100").unwrap()),
                signal_futures_price_atoms:Some(decimal_to_atoms("101").unwrap()),
                projected_cost_reserve_microbps:20_000_000,
                spot:LegFillAggregate::new("okx","BNT/USDT","buy",q).unwrap(),
                futures:LegFillAggregate::new("binance","BNTUSDT","sell",q).unwrap(),
                timing:EntryTiming::default(),
                post_leg1_projected_gross_microbps:None,
                post_leg1_projected_net_microbps:None,
                locked_actual_gross_microbps:None,
                locked_actual_net_microbps:None,
                signal_to_leg1_slippage_microbps:None,
                signal_to_hedge_decay_microbps:None,
                execution_cost_vs_signal_microbps:None,
            },
            exit:None,
        }
    }
    fn entry_economics()->PositionEconomicsState{
        PositionEconomicsState{
            entry:EntryEconomicsState{
                signal_gross_microbps:100_000_000,
                signal_expected_net_microbps:80_000_000,
                signal_spot_price_atoms:Some(decimal_to_atoms("100").unwrap()),
                signal_futures_price_atoms:Some(decimal_to_atoms("101").unwrap()),
                projected_cost_reserve_microbps:20_000_000,
                spot:agg("okx","BNT/USDT","buy","es","1","100"),
                futures:agg("binance","BNTUSDT","sell","ef","1","101"),
                timing:EntryTiming::default(),
                post_leg1_projected_gross_microbps:None,
                post_leg1_projected_net_microbps:None,
                locked_actual_gross_microbps:None,
                locked_actual_net_microbps:None,
                signal_to_leg1_slippage_microbps:None,
                signal_to_hedge_decay_microbps:None,
                execution_cost_vs_signal_microbps:None,
            },
            exit:None,
        }
    }
    fn exit_economics()->ExitEconomicsState{
        ExitEconomicsState{
            spot:agg("okx","BNT/USDT","sell","xs","1","102"),
            futures:agg("binance","BNTUSDT","buy","xf","1","99"),
        }
    }
    fn native_core()->NativePositionCore{
        NativePositionCore{
            base:"BNT".into(),spot_symbol:"BNT/USDT".into(),futures_symbol:"BNTUSDT".into(),
            opened_at_ms:1_780_000_000_125,
            spot_baseline_atoms_e18:125_000_000_000_000_000,
            spot_net_atoms_e18:1_000_000_000_000_000_000,
            futures_target_atoms_e18:1_000_000_000_000_000_000,
            order_quote_atoms_e18:10_000_000_000_000_000_000,
        }
    }
    fn entry_plan()->NativeEntryPlan{
        NativeEntryPlan{
            base:"BNT".into(),spot_symbol:"BNT/USDT".into(),futures_symbol:"BNTUSDT".into(),
            created_at_ms:1_780_000_000_125,
            planned_qty_atoms_e18:1_000_000_000_000_000_000,
            order_quote_atoms_e18:10_000_000_000_000_000_000,
            spot_baseline_atoms_e18:125_000_000_000_000_000,
            safety_generation:7,
        }
    }

    #[test]
    fn empty_legacy_import_is_atomic_and_reopens(){
        let raw=br#"{"positions":{},"closed_positions":{"LSK":[{},{}]}}"#;
        let state=import_legacy_python_state(raw).unwrap();
        assert!(state.active.is_none());
        assert_eq!(state.generation,1);
        assert_eq!(state.legacy_closed_positions_ignored,2);
        let dir=root("empty");let store=NativeStateStore::open(dir.join("state.json")).unwrap();
        store.create(&state).unwrap();
        let reopened=store.load().unwrap().unwrap();
        assert_eq!(reopened,state);
        let _=fs::remove_dir_all(dir);
    }

    #[test]
    fn live_legacy_import_is_recovery_required_not_trusted_native(){
        let state=import_legacy_python_state(&live_fixture()).unwrap();
        let p=state.active.unwrap();
        assert_eq!(p.provenance,PositionProvenance::LegacyPythonImport);
        assert!(p.reconciliation_required);
        assert_eq!(p.lifecycle.state,LifecycleState::RecoveryRequired);
        assert_eq!(p.core.base,"BNT");
        assert_eq!(p.core.spot_net_atoms_e18,1_250_000_000_000_000_000);
        assert_eq!(p.core.futures_target_atoms_e18,1_250_000_000_000_000_000);
        assert!(p.economics.is_none());
        assert!(p.intents.is_empty());
    }

    #[test]
    fn active_paper_or_multiple_positions_fail_closed(){
        let paper=br#"{"positions":{"BNT":{"entry":{"mode":"paper"}}}}"#;
        assert_eq!(import_legacy_python_state(paper),Err(NativeStateError::Legacy("active paper position")));
        let multi=br#"{"positions":{"A":{},"B":{}}}"#;
        assert_eq!(import_legacy_python_state(multi),Err(NativeStateError::Legacy("multiple active positions")));
    }

    #[test]
    fn checksum_corruption_is_detected(){
        let dir=root("checksum");let path=dir.join("state.json");
        let store=NativeStateStore::open(&path).unwrap();let state=empty();store.create(&state).unwrap();
        let mut env:Value=serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        env["payload"]["generation"]=Value::from(2u64);
        fs::write(&path,serde_json::to_vec(&env).unwrap()).unwrap();
        assert_eq!(store.load(),Err(NativeStateError::Checksum));
        let _=fs::remove_dir_all(dir);
    }

    #[test]
    fn stale_generation_cannot_overwrite_newer_state(){
        let dir=root("cas");let store=NativeStateStore::open(dir.join("state.json")).unwrap();
        let s1=empty();store.create(&s1).unwrap();
        let mut s2=s1.clone();s2.generation=2;store.replace(1,&s2).unwrap();
        let mut stale=s2.clone();stale.generation=2;
        assert_eq!(store.replace(1,&stale),Err(NativeStateError::Generation));
        assert_eq!(store.load().unwrap().unwrap().generation,2);
        let _=fs::remove_dir_all(dir);
    }

    #[test]
    fn invalid_active_bundle_is_rejected(){
        let raw=live_fixture();let mut state=import_legacy_python_state(&raw).unwrap();
        let p=state.active.as_mut().unwrap();
        p.lifecycle.state=LifecycleState::Hedged;
        assert_eq!(state.validate(),Err(NativeStateError::Invalid("legacy lifecycle must recover")));
    }

    #[test]
    fn crash_windows_never_create_unparseable_authoritative_state(){
        for fp in [
            StatePersistFailpoint::BeforeWrite,StatePersistFailpoint::AfterWrite,
            StatePersistFailpoint::AfterFileSync,StatePersistFailpoint::AfterRename,
            StatePersistFailpoint::AfterDirSync
        ]{
            let dir=root(&format!("fp-{fp:?}"));let store=NativeStateStore::open(dir.join("state.json")).unwrap();
            let state=empty();let _=store.persist_with_failpoint(&state,fp);
            if store.path.exists(){
                match store.load(){
                    Ok(Some(v))=>assert_eq!(v,state),
                    Err(NativeStateError::Json)=>panic!("authoritative path became partial JSON"),
                    Err(e)=>panic!("unexpected authoritative state error: {e:?}"),
                    Ok(None)=>panic!("path exists but load returned none"),
                }
            }
            let _=fs::remove_dir_all(dir);
        }
    }

    #[test]
    fn replace_requires_exact_next_generation(){
        let dir=root("next");let store=NativeStateStore::open(dir.join("state.json")).unwrap();
        let s1=empty();store.create(&s1).unwrap();
        let mut bad=s1.clone();bad.generation=3;
        assert_eq!(store.replace(1,&bad),Err(NativeStateError::Generation));
        let _=fs::remove_dir_all(dir);
    }

    #[test]
    fn concurrent_same_generation_writers_cannot_both_commit(){
        use std::sync::{Arc,Barrier};
        use std::thread;
        let dir=root("race");let path=dir.join("state.json");
        let store=NativeStateStore::open(&path).unwrap();let s1=empty();store.create(&s1).unwrap();
        let mut s2=s1.clone();s2.generation=2;
        let barrier=Arc::new(Barrier::new(3));
        let mut handles=Vec::new();
        for _ in 0..2{
            let st=NativeStateStore::open(&path).unwrap();
            let next=s2.clone();let b=barrier.clone();
            handles.push(thread::spawn(move||{b.wait();st.replace(1,&next)}));
        }
        barrier.wait();
        let results:Vec<_>=handles.into_iter().map(|h|h.join().unwrap()).collect();
        assert_eq!(results.iter().filter(|r|r.is_ok()).count(),1);
        assert_eq!(results.iter().filter(|r|**r==Err(NativeStateError::Generation)).count(),1);
        assert_eq!(store.load().unwrap().unwrap().generation,2);
        let _=fs::remove_dir_all(dir);
    }

    #[test]
    fn legacy_source_hash_barrier_detects_drift(){
        let raw=br#"{"positions":{},"closed_positions":{}}"#;
        let state=import_legacy_python_state(raw).unwrap();
        assert!(verify_legacy_source_unchanged(raw,&state).unwrap());
        let changed=br#"{"positions":{},"closed_positions":{"BNT":[]}}"#;
        assert!(!verify_legacy_source_unchanged(changed,&state).unwrap());
    }

    #[test]
    fn transact_advances_exactly_one_generation(){
        let dir=root("txn-ok");let store=NativeStateStore::open(dir.join("state.json")).unwrap();
        let s1=empty();store.create(&s1).unwrap();
        let (s2,out)=store.transact(1,|s|{
            s.legacy_closed_positions_ignored=7;
            Ok::<_,NativeStateError>("ok")
        }).unwrap();
        assert_eq!(out,"ok");
        assert_eq!(s2.generation,2);
        assert_eq!(s2.legacy_closed_positions_ignored,7);
        assert_eq!(store.load().unwrap().unwrap(),s2);
        let _=fs::remove_dir_all(dir);
    }

    #[test]
    fn failed_transaction_is_not_persisted(){
        let dir=root("txn-fail");let store=NativeStateStore::open(dir.join("state.json")).unwrap();
        let s1=empty();store.create(&s1).unwrap();
        let r=store.transact(1,|s|{
            s.legacy_closed_positions_ignored=99;
            Err::<(),_>(NativeStateError::Invalid("synthetic"))
        });
        assert_eq!(r,Err(NativeStateError::Invalid("synthetic")));
        assert_eq!(store.load().unwrap().unwrap(),s1);
        let _=fs::remove_dir_all(dir);
    }

    #[test]
    fn transaction_rejects_stale_or_manual_generation_edit(){
        let dir=root("txn-gen");let store=NativeStateStore::open(dir.join("state.json")).unwrap();
        let s1=empty();store.create(&s1).unwrap();
        assert_eq!(store.transact(2,|_|Ok::<_,NativeStateError>(())).unwrap_err(),NativeStateError::Generation);
        let r=store.transact(1,|s|{s.generation=9;Ok::<_,NativeStateError>(())});
        assert_eq!(r,Err(NativeStateError::Generation));
        assert_eq!(store.load().unwrap().unwrap(),s1);
        let _=fs::remove_dir_all(dir);
    }

    #[test]
    fn native_hedged_install_and_close_are_atomic_generations(){
        let dir=root("native-life");let store=NativeStateStore::open(dir.join("state.json")).unwrap();
        store.create(&empty()).unwrap();

        let s2=store.install_native_hedged(1,native_core(),entry_economics(),7).unwrap();
        assert_eq!(s2.generation,2);
        let p=s2.active.as_ref().unwrap();
        assert_eq!(p.provenance,PositionProvenance::Native);
        assert!(!p.reconciliation_required);
        assert_eq!(p.lifecycle.state,LifecycleState::Hedged);
        assert!(s2.legacy_source_sha256.is_none());

        let s3=store.start_native_close(2).unwrap();
        assert_eq!(s3.generation,3);
        assert_eq!(s3.active.as_ref().unwrap().lifecycle.state,LifecycleState::ClosingFutures);

        let s4=store.observe_native_basic_truth(3,BasicTruth::Closed).unwrap();
        assert_eq!(s4.active.as_ref().unwrap().lifecycle.state,LifecycleState::AwaitingFlatConfirmation);
        let s5=store.observe_native_basic_truth(4,BasicTruth::Closed).unwrap();
        assert_eq!(s5.active.as_ref().unwrap().lifecycle.state,LifecycleState::AwaitingFlatConfirmation);
        let s6=store.observe_native_basic_truth(5,BasicTruth::Closed).unwrap();
        assert_eq!(s6.active.as_ref().unwrap().lifecycle.state,LifecycleState::VerifyingExitTruth);

        let (s7,_)=store.transact(6,|state|{
            state.active.as_mut().unwrap().economics.as_mut().unwrap().exit=Some(exit_economics());
            Ok::<_,NativeStateError>(())
        }).unwrap();
        assert_eq!(s7.generation,7);

        let s8=store.finalize_verified_native_close(7).unwrap();
        assert_eq!(s8.generation,8);
        assert!(s8.active.is_none());
        assert_eq!(store.load().unwrap().unwrap(),s8);
        let _=fs::remove_dir_all(dir);
    }

    #[test]
    fn verified_close_without_exit_economics_is_not_persisted(){
        let dir=root("no-exit-econ");let store=NativeStateStore::open(dir.join("state.json")).unwrap();
        store.create(&empty()).unwrap();
        store.install_native_hedged(1,native_core(),entry_economics(),7).unwrap();
        store.start_native_close(2).unwrap();
        store.observe_native_basic_truth(3,BasicTruth::Closed).unwrap();
        store.observe_native_basic_truth(4,BasicTruth::Closed).unwrap();
        let s6=store.observe_native_basic_truth(5,BasicTruth::Closed).unwrap();
        let before=store.load().unwrap().unwrap();
        assert_eq!(s6.generation,6);
        assert_eq!(
            store.finalize_verified_native_close(6),
            Err(NativeStateError::Invalid("exit economics missing"))
        );
        assert_eq!(store.load().unwrap().unwrap(),before);
        let _=fs::remove_dir_all(dir);
    }

    #[test]
    fn native_install_rejects_core_economics_quantity_mismatch(){
        let dir=root("qty-mismatch");let store=NativeStateStore::open(dir.join("state.json")).unwrap();
        let initial=empty();store.create(&initial).unwrap();
        let mut core=native_core();core.futures_target_atoms_e18+=1;
        assert_eq!(
            store.install_native_hedged(1,core,entry_economics(),7),
            Err(NativeStateError::Invalid("core/economics quantity mismatch"))
        );
        assert_eq!(store.load().unwrap().unwrap(),initial);
        let _=fs::remove_dir_all(dir);
    }

    #[test]
    fn unresolved_intent_blocks_verified_close(){
        let dir=root("intent-block");let store=NativeStateStore::open(dir.join("state.json")).unwrap();
        store.create(&empty()).unwrap();
        store.install_native_hedged(1,native_core(),entry_economics(),7).unwrap();
        store.start_native_close(2).unwrap();
        store.observe_native_basic_truth(3,BasicTruth::Closed).unwrap();
        store.observe_native_basic_truth(4,BasicTruth::Closed).unwrap();
        store.observe_native_basic_truth(5,BasicTruth::Closed).unwrap();
        let (s7,_)=store.transact(6,|state|{
            let p=state.active.as_mut().unwrap();
            p.economics.as_mut().unwrap().exit=Some(exit_economics());
            p.intents.push(Intent{
                id:"close-fut".into(),client_order_id:Some("c-close-fut".into()),
                state:IntentState::Unknown,
            });
            Ok::<_,NativeStateError>(())
        }).unwrap();
        assert_eq!(s7.generation,7);
        assert_eq!(
            store.finalize_verified_native_close(7),
            Err(NativeStateError::Invalid("unresolved execution intent"))
        );
        assert_eq!(store.load().unwrap().unwrap(),s7);
        let _=fs::remove_dir_all(dir);
    }

    #[test]
    fn legacy_import_cannot_use_native_transitions(){
        let dir=root("legacy-transition");let store=NativeStateStore::open(dir.join("state.json")).unwrap();
        let legacy=import_legacy_python_state(&live_fixture()).unwrap();
        store.create(&legacy).unwrap();
        assert_eq!(
            store.start_native_close(1),
            Err(NativeStateError::Invalid("position not native-authoritative"))
        );
        assert_eq!(store.load().unwrap().unwrap(),legacy);
        let _=fs::remove_dir_all(dir);
    }

    // Review-only executor: no DNS, HTTPS or private exchange mutations.
    struct R31FakeSend(std::sync::atomic::AtomicUsize);
    impl crate::private_https::HttpExecutor for R31FakeSend{
        fn execute_request(&self,_req:&crate::private_https::HttpsRequest)
            ->Result<crate::private_https::HttpResponse,crate::private_https::TransportError>{
            self.0.fetch_add(1,std::sync::atomic::Ordering::SeqCst);
            Ok(crate::private_https::HttpResponse{
                status:200,headers:vec![],
                body:r#"{"code":"0","data":[{"sCode":"0","ordId":"123"}]}"#.into()
            })
        }
    }
    fn r31_spot_order(cid:&str)->crate::execution_adapter::OrderIntent{
        crate::execution_adapter::OrderIntent{
            venue:crate::execution_adapter::Venue::OkxSpot,
            symbol:"BNT/USDT".into(),side:"buy".into(),amount:"1".into(),
            client_id:cid.into(),
        }
    }
    fn r31_futures_order(cid:&str)->crate::execution_adapter::OrderIntent{
        crate::execution_adapter::OrderIntent{
            venue:crate::execution_adapter::Venue::BinanceUsdm,
            symbol:"BNTUSDT".into(),side:"sell".into(),amount:"1".into(),
            client_id:cid.into(),
        }
    }

    #[test]
    fn r31_exact_spot_authority_and_pre_send_revalidation(){
        use crate::private_rest::{build_signed_entry_order_from_native_authority,
            OkxCredentials,CredentialsRef,OkxDomain,BinancePositionMode,
            BINANCE_DEFAULT_RECV_WINDOW_MS,RestBuildError};
        use crate::execution_adapter::OrderKind;
        use crate::private_https::{dispatch_authorized_entry_once,NativeEntryDispatchError};
        use std::sync::atomic::{AtomicUsize,Ordering};
        let dir=root("r31-spot");
        let store=NativeStateStore::open(dir.join("state.json")).unwrap();
        store.create(&empty()).unwrap();
        let (_state,permit)=store.begin_native_entry(
            1,entry_plan(),pristine_entry_economics(),"r31-s","sab00000000000000000000000000031").unwrap();
        let make=||EntryNetworkPermit{
            generation:permit.generation,intent_id:permit.intent_id.clone(),leg:NativeEntryLeg::Spot
        };
        let original=r31_spot_order("sab00000000000000000000000000031");
        for (bad,safety) in [
            (r31_spot_order("wrong-client"),7),
            (crate::execution_adapter::OrderIntent{symbol:"WRONG/USDT".into(),..original.clone()},7),
            (crate::execution_adapter::OrderIntent{side:"sell".into(),..original.clone()},7),
            (crate::execution_adapter::OrderIntent{venue:crate::execution_adapter::Venue::BinanceUsdm,..original.clone()},7),
            (crate::execution_adapter::OrderIntent{amount:"2".into(),..original.clone()},7),
            (crate::execution_adapter::OrderIntent{amount:"0.999".into(),..original.clone()},7),
            (original.clone(),8),
        ]{
            assert!(store.authorize_entry_order(make(),&bad,safety).is_err());
        }
        assert!(store.authorize_entry_order(
            EntryNetworkPermit{leg:NativeEntryLeg::Futures,..make()},&original,7).is_err());
        let creds=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
        let auth=store.authorize_entry_order(make(),&original,7).unwrap();
        assert_eq!(build_signed_entry_order_from_native_authority(
            auth,OrderKind::Market,None,CredentialsRef::Okx(&creds),
            1_700_000_000_123,OkxDomain::Global,BinancePositionMode::OneWay,
            BINANCE_DEFAULT_RECV_WINDOW_MS).unwrap_err(),RestBuildError::UnsupportedOrderKind);
        let auth=store.authorize_entry_order(make(),&original,7).unwrap();
        let prepared=build_signed_entry_order_from_native_authority(
            auth,OrderKind::AggressiveLimitFok,Some("0.75"),CredentialsRef::Okx(&creds),
            1_700_000_000_123,OkxDomain::Global,BinancePositionMode::OneWay,
            BINANCE_DEFAULT_RECV_WINDOW_MS).unwrap();
        assert!(prepared.request.body.as_deref().unwrap().contains("sab00000000000000000000000000031"));
        assert!(!format!("{prepared:?}").contains("FAKESECRET"));
        let fake=R31FakeSend(AtomicUsize::new(0));
        assert!(dispatch_authorized_entry_once(&fake,&store,prepared,8).is_err());
        assert_eq!(fake.0.load(Ordering::SeqCst),0);
        let auth=store.authorize_entry_order(make(),&original,7).unwrap();
        let prepared=build_signed_entry_order_from_native_authority(
            auth,OrderKind::AggressiveLimitFok,Some("0.75"),CredentialsRef::Okx(&creds),
            1_700_000_000_123,OkxDomain::Global,BinancePositionMode::OneWay,
            BINANCE_DEFAULT_RECV_WINDOW_MS).unwrap();
        store.transact(2,|_|Ok(())).unwrap();
        assert!(matches!(dispatch_authorized_entry_once(&fake,&store,prepared,7),
            Err(NativeEntryDispatchError::State(NativeStateError::Generation))));
        assert_eq!(fake.0.load(Ordering::SeqCst),0);
        let _=fs::remove_dir_all(dir);
    }

    #[test]
    fn r31_spot_and_futures_permits_reach_fake_http_only_when_valid(){
        use crate::private_rest::{build_signed_entry_order_from_native_authority,
            OkxCredentials,BinanceCredentials,CredentialsRef,OkxDomain,BinancePositionMode,
            BINANCE_DEFAULT_RECV_WINDOW_MS,RestBuildError};
        use crate::execution_adapter::OrderKind;
        use crate::private_https::dispatch_authorized_entry_once;
        use std::sync::atomic::{AtomicUsize,Ordering};
        let dir=root("r31-valid");
        let store=NativeStateStore::open(dir.join("state.json")).unwrap();
        store.create(&empty()).unwrap();
        let (_state,spot_permit)=store.begin_native_entry(
            1,entry_plan(),pristine_entry_economics(),"r31-spot","sab00000000000000000000000000032").unwrap();
        let spot_authorized=store.authorize_entry_order(
            spot_permit,&r31_spot_order("sab00000000000000000000000000032"),7).unwrap();
        let oc=OkxCredentials::new("FAKE_OKX","FAKE_SECRET","FAKE_PASS").unwrap();
        let prepared=build_signed_entry_order_from_native_authority(
            spot_authorized,OrderKind::AggressiveLimitFok,Some("0.75"),
            CredentialsRef::Okx(&oc),1_700_000_000_123,OkxDomain::Global,
            BinancePositionMode::OneWay,BINANCE_DEFAULT_RECV_WINDOW_MS).unwrap();
        let fake=R31FakeSend(AtomicUsize::new(0));
        let (_observation,returned_spot_permit)=
            dispatch_authorized_entry_once(&fake,&store,prepared,7).unwrap();
        assert_eq!(returned_spot_permit.leg(),NativeEntryLeg::Spot);
        assert_eq!(fake.0.load(Ordering::SeqCst),1);
        let state=store.record_spot_entry_observation(
            returned_spot_permit,1_000_000_000_000_000_000,Some(decimal_to_atoms("100").unwrap()),
            true,false,false).unwrap();
        let (_s4,fut_permit)=store.prepare_futures_entry_hedge(
            state.generation,7,"r31-fut","sab00000000000000000000000000033").unwrap();
        let make=||EntryNetworkPermit{generation:4,intent_id:"r31-fut".into(),leg:NativeEntryLeg::Futures};
        let wrong=r31_futures_order("sab00000000000000000000000000033");
        assert!(store.authorize_entry_order(make(),&crate::execution_adapter::OrderIntent{
            amount:"2".into(),..wrong.clone()},7).is_err());
        let bc=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        for mode in [BinancePositionMode::Unknown,BinancePositionMode::Hedge]{
            let authorized=store.authorize_entry_order(make(),&wrong,7).unwrap();
            assert_eq!(build_signed_entry_order_from_native_authority(
                authorized,OrderKind::Market,None,CredentialsRef::Binance(&bc),
                1_700_000_000_123,OkxDomain::Global,mode,BINANCE_DEFAULT_RECV_WINDOW_MS
            ).unwrap_err(),RestBuildError::UnsupportedBinancePositionMode);
        }
        for window in [0,60001]{
            let authorized=store.authorize_entry_order(make(),&wrong,7).unwrap();
            assert_eq!(build_signed_entry_order_from_native_authority(
                authorized,OrderKind::Market,None,CredentialsRef::Binance(&bc),
                1_700_000_000_123,OkxDomain::Global,BinancePositionMode::OneWay,window
            ).unwrap_err(),RestBuildError::InvalidRecvWindow);
        }
        let authorized=store.authorize_entry_order(make(),&wrong,7).unwrap();
        assert_eq!(build_signed_entry_order_from_native_authority(
            authorized,OrderKind::Market,None,CredentialsRef::Okx(&oc),
            1_700_000_000_123,OkxDomain::Global,BinancePositionMode::OneWay,BINANCE_DEFAULT_RECV_WINDOW_MS
        ).unwrap_err(),RestBuildError::CredentialVenueMismatch);
        let authorized=store.authorize_entry_order(fut_permit,&wrong,7).unwrap();
        let prepared=build_signed_entry_order_from_native_authority(
            authorized,OrderKind::Market,None,CredentialsRef::Binance(&bc),
            1_700_000_000_123,OkxDomain::Global,BinancePositionMode::OneWay,BINANCE_DEFAULT_RECV_WINDOW_MS
        ).unwrap();
        assert!(prepared.request.body.as_deref().unwrap().contains("side=SELL&type=MARKET"));
        let (_observation,returned_futures_permit)=
            dispatch_authorized_entry_once(&fake,&store,prepared,7).unwrap();
        assert_eq!(returned_futures_permit.leg(),NativeEntryLeg::Futures);
        assert_eq!(fake.0.load(Ordering::SeqCst),2);
        // The signed request was consumed; after restart there is no
        // serialized EntryNetworkPermit. Unresolved Prepared remains recovery-only.
        let reopened=NativeStateStore::open(dir.join("state.json")).unwrap();
        assert_eq!(reopened.load_for_recovery().unwrap().unwrap().entry.unwrap().phase,
            NativeEntryPhase::RecoveryRequired);
        let _=fs::remove_dir_all(dir);
    }

    fn r31b_close_store(tag:&str)->(PathBuf,NativeStateStore,NativeAuthoritativeState){
        let root=root(tag);
        let store=NativeStateStore::open(root.join("state.json")).unwrap();
        store.create(&empty()).unwrap();
        let installed=store.install_native_hedged(1,native_core(),entry_economics(),7).unwrap();
        let closing=store.start_native_close(installed.generation).unwrap();
        assert_eq!(closing.generation,3);
        (root,store,closing)
    }

    #[test]
    fn r31b_close_durable_prepare_crash_unknown_recover_and_spot(){
        use crate::execution_adapter::{ResponseClass,OrderKind};
        let (root,store,closing)=r31b_close_store("r31b-full");
        let (prepared,permit)=store.prepare_native_close_intent(
            closing.generation,7,"close-f1","sab00000000000000000000000000041").unwrap();
        assert_eq!(prepared.generation,4);
        assert_eq!(prepared.active.as_ref().unwrap().lifecycle.pending.as_ref().unwrap().intent_id,"close-f1");
        assert_eq!(prepared.active.as_ref().unwrap().intents.last().unwrap().state,IntentState::Prepared);
        let reopened=NativeStateStore::open(root.join("state.json")).unwrap();
        let restart=reopened.load_for_recovery().unwrap().unwrap();
        assert_eq!(restart.active.as_ref().unwrap().lifecycle.state,LifecycleState::RecoveryRequired);
        assert!(reopened.prepare_native_close_intent(4,7,
            "wrong-next","sab00000000000000000000000000042").is_err());
        let authorized=store.authorize_native_close_order(
            NativeClosePermit{
                generation:permit.generation,intent_id:permit.intent_id.clone(),
                client_id:permit.client_id.clone(),leg:permit.leg,
                safety_generation:permit.safety_generation
            },7).unwrap();
        assert_eq!(authorized.plan().intent.amount,"1");
        assert_eq!(authorized.plan().intent.side,"buy");
        assert_eq!(authorized.plan().intent.venue,Venue::BinanceUsdm);
        assert!(authorized.plan().reduce_only);
        assert_eq!(authorized.plan().kind,OrderKind::Market);
        assert_eq!(close_quantity_decimal_e18(600_000_000_000_000_000),"0.6");
        assert_eq!(close_quantity_decimal_e18(12_000_000_000_000_000),"0.012");
        assert_eq!(close_quantity_decimal_e18(1),"0.000000000000000001");

        let unknown=store.record_native_close_observation(permit,MutationObservation{
            class:ResponseClass::Unknown,terminal:false,filled_units:0}).unwrap();
        assert_eq!(unknown.generation,5);
        let active=unknown.active.as_ref().unwrap();
        assert_eq!(active.intents.last().unwrap().state,IntentState::Unknown);
        assert_eq!(active.lifecycle.state,LifecycleState::RecoveryRequired);
        assert!(reopened.recover_native_close(5,false,RecoveryEvidence::ConfirmedFill{
            filled_units:1_000_000_000_000_000_000,terminal:true}).is_err());
        assert_eq!(reopened.load().unwrap().unwrap().generation,5);
        let still=reopened.recover_native_close(5,true,RecoveryEvidence::ConfirmedFill{
            filled_units:300_000_000_000_000_000,terminal:false}).unwrap();
        assert_eq!(still.active.as_ref().unwrap().intents.last().unwrap().state,IntentState::Unknown);
        assert_eq!(still.active.as_ref().unwrap().lifecycle.state,LifecycleState::RecoveryRequired);
        let recovered=reopened.recover_native_close(6,true,RecoveryEvidence::ConfirmedFill{
            filled_units:1_000_000_000_000_000_000,terminal:true}).unwrap();
        assert_eq!(recovered.generation,7);
        let active=recovered.active.as_ref().unwrap();
        assert_eq!(active.intents.last().unwrap().state,IntentState::Reconciled);
        assert!(active.lifecycle.pending.is_none());
        assert_eq!(active.lifecycle.state,LifecycleState::ClosingSpot);
        let (_,spot_permit)=reopened.prepare_native_close_intent(
            7,7,"close-s1","sab00000000000000000000000000043").unwrap();
        let spot=reopened.authorize_native_close_order(
            NativeClosePermit{
                generation:spot_permit.generation,intent_id:spot_permit.intent_id.clone(),
                client_id:spot_permit.client_id.clone(),leg:spot_permit.leg,
                safety_generation:spot_permit.safety_generation
            },7).unwrap();
        assert_eq!(spot.plan().intent.amount,"1");
        assert_eq!(spot.plan().intent.side,"sell");
        assert_eq!(spot.plan().intent.venue,Venue::OkxSpot);
        assert!(!spot.plan().reduce_only);
        let final_state=reopened.record_native_close_observation(spot_permit,MutationObservation{
            class:ResponseClass::Acknowledged,terminal:true,
            filled_units:1_000_000_000_000_000_000}).unwrap();
        assert_eq!(final_state.active.unwrap().lifecycle.state,LifecycleState::VerifyingExitTruth);
        let _=fs::remove_dir_all(root);
    }

    #[test]
    fn r31b_close_partial_exact_remaining_and_failed_transaction(){
        use crate::execution_adapter::ResponseClass;
        let (root,store,closing)=r31b_close_store("r31b-partial");
        let (_,permit)=store.prepare_native_close_intent(closing.generation,7,
            "close-f1","sab00000000000000000000000000044").unwrap();
        assert!(store.authorize_native_close_order(NativeClosePermit{
            generation:permit.generation,intent_id:permit.intent_id.clone(),
            client_id:permit.client_id.clone(),leg:permit.leg,safety_generation:7
        },8).is_err());
        let partial=store.record_native_close_observation(permit,MutationObservation{
            class:ResponseClass::Acknowledged,terminal:true,
            filled_units:400_000_000_000_000_000}).unwrap();
        assert_eq!(partial.active.as_ref().unwrap().lifecycle.state,LifecycleState::ClosingFutures);
        assert_eq!(partial.active.as_ref().unwrap().lifecycle.futures_filled_units,400_000_000_000_000_000);
        assert!(store.prepare_native_close_intent(partial.generation,7,
            "close-duplicate","sab00000000000000000000000000044").is_err());
        assert_eq!(store.load().unwrap().unwrap().generation,5);
        let (_,permit2)=store.prepare_native_close_intent(partial.generation,7,
            "close-f2","sab00000000000000000000000000045").unwrap();
        let auth=store.authorize_native_close_order(NativeClosePermit{
            generation:permit2.generation,intent_id:permit2.intent_id.clone(),
            client_id:permit2.client_id.clone(),leg:permit2.leg,safety_generation:7
        },7).unwrap();
        assert_eq!(auth.plan().intent.amount,"0.6");
        let invalid=store.record_native_close_observation(NativeClosePermit{
            generation:permit2.generation,intent_id:permit2.intent_id.clone(),
            client_id:permit2.client_id.clone(),leg:permit2.leg,safety_generation:7
        },MutationObservation{class:ResponseClass::Acknowledged,terminal:true,
            filled_units:700_000_000_000_000_000});
        assert!(invalid.is_err());
        assert_eq!(store.load().unwrap().unwrap().generation,6);
        assert_eq!(store.load().unwrap().unwrap().active.as_ref().unwrap()
            .intents.last().unwrap().state,IntentState::Prepared);
        let finished=store.record_native_close_observation(permit2,MutationObservation{
            class:ResponseClass::Acknowledged,terminal:true,
            filled_units:600_000_000_000_000_000}).unwrap();
        assert_eq!(finished.active.as_ref().unwrap().lifecycle.state,LifecycleState::ClosingSpot);
        assert!(store.record_native_close_observation(NativeClosePermit{
            generation:6,intent_id:"close-f2".into(),
            client_id:"sab00000000000000000000000000045".into(),
            leg:CloseLeg::Futures,safety_generation:7
        },MutationObservation{class:ResponseClass::Acknowledged,terminal:true,filled_units:1}).is_err());
        let _=fs::remove_dir_all(root);
    }

    #[test]
    fn r31b_close_mock_transport_permit_required_and_stale_denied(){
        use crate::private_rest::{BinanceCredentials,CredentialsRef,OkxDomain,BinancePositionMode,
            BINANCE_DEFAULT_RECV_WINDOW_MS};
        use crate::private_https::{dispatch_authorized_close_once,NativeCloseDispatchError};
        use std::sync::atomic::{AtomicUsize,Ordering};
        let (root,store,closing)=r31b_close_store("r31b-http");
        let (_,permit)=store.prepare_native_close_intent(closing.generation,7,
            "close-f1","sab00000000000000000000000000046").unwrap();
        let copy=||NativeClosePermit{
            generation:permit.generation,intent_id:permit.intent_id.clone(),
            client_id:permit.client_id.clone(),leg:permit.leg,safety_generation:7
        };
        let fake=R31FakeSend(AtomicUsize::new(0));
        let creds=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        let stale=store.authorize_native_close_order(copy(),7).unwrap();
        assert!(matches!(dispatch_authorized_close_once(
            &fake,&store,stale,8,CredentialsRef::Binance(&creds),
            1_700_000_000_123,OkxDomain::Global,
            BinancePositionMode::OneWay,BINANCE_DEFAULT_RECV_WINDOW_MS
        ),Err(NativeCloseDispatchError::State(_))));
        assert_eq!(fake.0.load(Ordering::SeqCst),0);
        let wrong_mode=store.authorize_native_close_order(copy(),7).unwrap();
        assert!(matches!(dispatch_authorized_close_once(
            &fake,&store,wrong_mode,7,CredentialsRef::Binance(&creds),
            1_700_000_000_123,OkxDomain::Global,
            BinancePositionMode::Hedge,BINANCE_DEFAULT_RECV_WINDOW_MS
        ),Err(NativeCloseDispatchError::Sign(_))));
        assert_eq!(fake.0.load(Ordering::SeqCst),0);
        let good=store.authorize_native_close_order(permit,7).unwrap();
        let (observation,returned_permit)=dispatch_authorized_close_once(
            &fake,&store,good,7,CredentialsRef::Binance(&creds),
            1_700_000_000_123,OkxDomain::Global,
            BinancePositionMode::OneWay,BINANCE_DEFAULT_RECV_WINDOW_MS
        ).unwrap();
        assert_eq!(fake.0.load(Ordering::SeqCst),1);
        assert_eq!(returned_permit.leg(),CloseLeg::Futures);
        assert_eq!(observation.class,crate::private_rest::RestOutcomeClass::Unknown);
        let _=fs::remove_dir_all(root);
    }

    #[test]
    fn r31b_close_rejection_nonterminal_and_not_found_matrix(){
        use crate::execution_adapter::ResponseClass;
        let (root,store,closing)=r31b_close_store("r31b-reject");
        let (_,permit)=store.prepare_native_close_intent(
            closing.generation,7,"rej-f","sab00000000000000000000000000047").unwrap();
        let rejected=store.record_native_close_observation(permit,MutationObservation{
            class:ResponseClass::Rejected,terminal:true,filled_units:0}).unwrap();
        assert_eq!(rejected.active.as_ref().unwrap().lifecycle.state,LifecycleState::ManualRequired);
        assert_eq!(rejected.active.as_ref().unwrap().intents.last().unwrap().state,IntentState::Rejected);
        assert!(store.prepare_native_close_intent(rejected.generation,7,
            "again","sab00000000000000000000000000048").is_err());
        let _=fs::remove_dir_all(root);

        let (root,store,closing)=r31b_close_store("r31b-nonterminal");
        let (_,permit)=store.prepare_native_close_intent(
            closing.generation,7,"part-f","sab00000000000000000000000000049").unwrap();
        let observing=store.record_native_close_observation(permit,MutationObservation{
            class:ResponseClass::Acknowledged,terminal:false,
            filled_units:300_000_000_000_000_000}).unwrap();
        assert_eq!(observing.active.as_ref().unwrap().lifecycle.state,LifecycleState::RecoveryRequired);
        assert_eq!(observing.active.as_ref().unwrap().intents.last().unwrap().state,
            IntentState::Acknowledged);
        assert!(store.prepare_native_close_intent(observing.generation,7,
            "wrong-next","sab00000000000000000000000000050").is_err());
        let terminal=store.recover_native_close(observing.generation,true,
            RecoveryEvidence::ConfirmedFill{filled_units:300_000_000_000_000_000,
                terminal:true}).unwrap();
        assert_eq!(terminal.active.as_ref().unwrap().lifecycle.state,LifecycleState::ClosingFutures);
        assert_eq!(terminal.active.as_ref().unwrap().lifecycle.futures_filled_units,
            300_000_000_000_000_000);
        assert!(terminal.active.as_ref().unwrap().lifecycle.pending.is_none());
        let _=fs::remove_dir_all(root);

        let (root,store,closing)=r31b_close_store("r31b-notfound");
        let (_,permit)=store.prepare_native_close_intent(
            closing.generation,7,"nf-f","sab00000000000000000000000000051").unwrap();
        let observation=store.record_native_close_observation(permit,MutationObservation{
            class:ResponseClass::Unknown,terminal:false,filled_units:0}).unwrap();
        let next=store.recover_native_close(observation.generation,true,
            RecoveryEvidence::ConfirmedNotFound).unwrap();
        assert!(next.active.as_ref().unwrap().lifecycle.pending.is_none());
        assert_eq!(next.active.as_ref().unwrap().intents.last().unwrap().state,IntentState::Reconciled);
        let (_next,_permit)=store.prepare_native_close_intent(
            next.generation,7,"nf-f2","sab00000000000000000000000000052").unwrap();
        assert_eq!(store.load().unwrap().unwrap().active.as_ref().unwrap().intents.len(),2);
        let _=fs::remove_dir_all(root);
    }

    #[test]
    fn r32a_binance_rest_response_maps_into_atomic_native_close_e18(){
        use crate::private_rest::{parse_binance_order_http,to_native_close_observation_e18,
            RestOutcomeClass};
        let (root,store,closing)=r31b_close_store("r32a-rest-state");
        let client_id="sab00000000000000000000000000062";
        let (_,permit)=store.prepare_native_close_intent(closing.generation,7,
            "r32a-f1",client_id).unwrap();
        let body=format!(
            r#"{{"orderId":123,"clientOrderId":"{}","status":"FILLED","executedQty":"0.4"}}"#,
            client_id
        );
        let observed=parse_binance_order_http(200,&body);
        assert_eq!(observed.class,RestOutcomeClass::Acknowledged);
        assert!(observed.terminal);
        let native=to_native_close_observation_e18(
            &observed,client_id,1_000_000_000_000_000_000);
        assert_eq!(native.filled_units,400_000_000_000_000_000);
        let state=store.record_native_close_observation(permit,native).unwrap();
        let p=state.active.as_ref().unwrap();
        assert_eq!(p.lifecycle.futures_filled_units,400_000_000_000_000_000);
        assert_eq!(p.lifecycle.state,LifecycleState::ClosingFutures);
        assert_eq!(p.intents.last().unwrap().state,IntentState::Acknowledged);
        let (_,next)=store.prepare_native_close_intent(
            state.generation,7,"r32a-f2",
            "sab00000000000000000000000000063").unwrap();
        let authorized=store.authorize_native_close_order(next,7).unwrap();
        assert_eq!(authorized.plan().intent.amount,"0.6");
        let _=fs::remove_dir_all(root);
    }

    struct R32Http{
        status:u16,body:String,calls:std::sync::atomic::AtomicUsize,
    }
    impl R32Http{
        fn new(status:u16,body:String)->Self{
            Self{status,body,calls:std::sync::atomic::AtomicUsize::new(0)}
        }
        fn calls(&self)->usize{self.calls.load(std::sync::atomic::Ordering::SeqCst)}
    }
    impl crate::private_https::HttpExecutor for R32Http{
        fn execute_request(&self,_request:&crate::private_https::HttpsRequest)
            ->Result<crate::private_https::HttpResponse,crate::private_https::TransportError>{
            self.calls.fetch_add(1,std::sync::atomic::Ordering::SeqCst);
            Ok(crate::private_https::HttpResponse{
                status:self.status,headers:vec![],body:self.body.clone(),
            })
        }
    }
    #[test]
    fn r32b_close_http_fill_is_durable_and_next_leg_uses_remainder(){
        use crate::private_https::dispatch_record_native_close_once;
        use crate::private_rest::{BinanceCredentials,CredentialsRef,OkxDomain,
            BinancePositionMode,BINANCE_DEFAULT_RECV_WINDOW_MS};
        let (root,store,closing)=r31b_close_store("r32b-close-fill");
        let cid="sab00000000000000000000000000071";
        let (_,permit)=store.prepare_native_close_intent(
            closing.generation,7,"r32b-f1",cid).unwrap();
        let authorized=store.authorize_native_close_order(permit,7).unwrap();
        let creds=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        let fake=R32Http::new(200,format!(
            r#"{{"orderId":101,"clientOrderId":"{cid}","status":"FILLED","executedQty":"0.4"}}"#
        ));
        let result=dispatch_record_native_close_once(
            &fake,&store,authorized,7,CredentialsRef::Binance(&creds),
            1_700_000_000_123,OkxDomain::Global,BinancePositionMode::OneWay,
            BINANCE_DEFAULT_RECV_WINDOW_MS).unwrap();
        assert_eq!(fake.calls(),1);
        assert_eq!(result.durable_state.generation,5);
        let reopened=NativeStateStore::open(root.join("state.json")).unwrap();
        let loaded=reopened.load().unwrap().unwrap();
        assert_eq!(loaded,result.durable_state);
        let p=loaded.active.as_ref().unwrap();
        assert_eq!(p.lifecycle.futures_filled_units,400_000_000_000_000_000);
        assert_eq!(p.lifecycle.state,LifecycleState::ClosingFutures);
        assert!(p.lifecycle.pending.is_none());
        assert_eq!(p.intents.last().unwrap().state,IntentState::Acknowledged);
        let (_,next)=reopened.prepare_native_close_intent(
            loaded.generation,7,"r32b-f2","sab00000000000000000000000000072").unwrap();
        assert_eq!(reopened.authorize_native_close_order(next,7)
            .unwrap().plan().intent.amount,"0.6");
        let _=fs::remove_dir_all(root);
    }

    #[test]
    fn r32b_close_bad_identity_and_503_remain_durable_unknown(){
        use crate::private_https::dispatch_record_native_close_once;
        use crate::private_rest::{BinanceCredentials,CredentialsRef,OkxDomain,
            BinancePositionMode,BINANCE_DEFAULT_RECV_WINDOW_MS};
        let creds=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        for (tag,status,body) in [
            ("r32b-503",503,r#"{"code":-1000,"msg":"Unknown error, please check your request or try again later."}"#.to_string()),
            ("r32b-wrong-id",200,r#"{"orderId":122,"clientOrderId":"wrong","status":"FILLED","executedQty":"1"}"#.to_string()),
            ("r32b-missing-qty",200,r#"{"orderId":123,"clientOrderId":"sab00000000000000000000000000073","status":"FILLED"}"#.to_string()),
        ]{
            let (root,store,closing)=r31b_close_store(tag);
            let (_,permit)=store.prepare_native_close_intent(closing.generation,7,
                "r32b-f1","sab00000000000000000000000000073").unwrap();
            let authorized=store.authorize_native_close_order(permit,7).unwrap();
            let fake=R32Http::new(status,body);
            let outcome=dispatch_record_native_close_once(
                &fake,&store,authorized,7,CredentialsRef::Binance(&creds),
                1_700_000_000_123,OkxDomain::Global,BinancePositionMode::OneWay,
                BINANCE_DEFAULT_RECV_WINDOW_MS).unwrap();
            assert_eq!(fake.calls(),1);
            assert_eq!(outcome.durable_state.generation,5);
            let reload=NativeStateStore::open(root.join("state.json")).unwrap();
            let active=reload.load_for_recovery().unwrap().unwrap().active.unwrap();
            assert_eq!(active.lifecycle.state,LifecycleState::RecoveryRequired);
            assert_eq!(active.intents.last().unwrap().state,IntentState::Unknown);
            assert_eq!(active.lifecycle.futures_filled_units,0);
            assert!(reload.prepare_native_close_intent(5,7,
                "retry","sab00000000000000000000000000074").is_err());
            let _=fs::remove_dir_all(root);
        }
    }

    #[test]
    fn r32b_http_return_then_crash_before_persist_remains_prepared(){
        use crate::private_https::dispatch_authorized_close_once;
        use crate::private_rest::{BinanceCredentials,CredentialsRef,OkxDomain,
            BinancePositionMode,BINANCE_DEFAULT_RECV_WINDOW_MS};
        let (root,store,closing)=r31b_close_store("r32b-crash");
        let cid="sab00000000000000000000000000075";
        let (_,permit)=store.prepare_native_close_intent(
            closing.generation,7,"r32b-f1",cid).unwrap();
        let authorized=store.authorize_native_close_order(permit,7).unwrap();
        let creds=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        let fake=R32Http::new(200,format!(
            r#"{{"orderId":101,"clientOrderId":"{cid}","status":"FILLED","executedQty":"1"}}"#
        ));
        // Simulate process dying immediately after the mock exchange answered,
        // before the caller can commit the response to its durable store.
        let (observed,unrecorded_permit)=dispatch_authorized_close_once(
            &fake,&store,authorized,7,CredentialsRef::Binance(&creds),
            1_700_000_000_123,OkxDomain::Global,BinancePositionMode::OneWay,
            BINANCE_DEFAULT_RECV_WINDOW_MS).unwrap();
        assert_eq!(fake.calls(),1);
        assert_eq!(observed.class,crate::private_rest::RestOutcomeClass::Acknowledged);
        drop(unrecorded_permit);
        let reopened=NativeStateStore::open(root.join("state.json")).unwrap();
        let snapshot=reopened.load_for_recovery().unwrap().unwrap();
        assert_eq!(snapshot.generation,4);
        let active=snapshot.active.unwrap();
        assert_eq!(active.intents.last().unwrap().state,IntentState::Prepared);
        assert_eq!(active.lifecycle.state,LifecycleState::RecoveryRequired);
        assert_eq!(active.lifecycle.futures_filled_units,0);
        assert!(reopened.prepare_native_close_intent(4,7,
            "blind-retry","sab00000000000000000000000000076").is_err());
        let recovered=reopened.recover_native_close(4,true,
            RecoveryEvidence::ConfirmedFill{
                filled_units:1_000_000_000_000_000_000,terminal:true}).unwrap();
        assert_eq!(recovered.active.unwrap().lifecycle.state,LifecycleState::ClosingSpot);
        let _=fs::remove_dir_all(root);
    }

    #[test]
    fn r32b_spot_entry_place_ack_persists_unknown_and_never_opens_futures(){
        use crate::private_https::dispatch_record_native_entry_once;
        use crate::private_rest::{build_signed_entry_order_from_native_authority,
            OkxCredentials,CredentialsRef,OkxDomain,BinancePositionMode,
            BINANCE_DEFAULT_RECV_WINDOW_MS};
        use crate::execution_adapter::OrderKind;
        let root=root("r32b-spot-entry");
        let store=NativeStateStore::open(root.join("state.json")).unwrap();
        store.create(&empty()).unwrap();
        let cid="sab00000000000000000000000000077";
        let (_,permit)=store.begin_native_entry(1,entry_plan(),pristine_entry_economics(),
            "r32b-spot",cid).unwrap();
        let auth=store.authorize_entry_order(permit,&r31_spot_order(cid),7).unwrap();
        let creds=OkxCredentials::new("FAKE_OKX","FAKE_SECRET","FAKE_PASS").unwrap();
        let prepared=build_signed_entry_order_from_native_authority(auth,
            OrderKind::AggressiveLimitFok,Some("1"),
            CredentialsRef::Okx(&creds),1_700_000_000_123,
            OkxDomain::Global,BinancePositionMode::OneWay,
            BINANCE_DEFAULT_RECV_WINDOW_MS).unwrap();
        let fake=R32Http::new(200,format!(
            r#"{{"code":"0","data":[{{"sCode":"0","clOrdId":"{cid}","ordId":"111"}}]}}"#
        ));
        let result=dispatch_record_native_entry_once(&fake,&store,prepared,7).unwrap();
        assert_eq!(fake.calls(),1);
        assert_eq!(result.observation.reason,"OKX_PLACE_ACK_NOT_FILL_TRUTH");
        let reopened=NativeStateStore::open(root.join("state.json")).unwrap();
        let loaded=reopened.load().unwrap().unwrap();
        assert_eq!(loaded.generation,3);
        let entry=loaded.entry.unwrap();
        assert_eq!(entry.phase,NativeEntryPhase::RecoveryRequired);
        assert_eq!(entry.intents[0].state,IntentState::Unknown);
        assert_eq!(entry.spot_filled_atoms_e18,0);
        assert!(reopened.prepare_futures_entry_hedge(3,7,"blind-hedge","blind-cid").is_err());
        let _=fs::remove_dir_all(root);
    }

    #[test]
    fn r32b_child_kill_after_http_before_durable_record(){
        // Only the parent test arms this branch. Otherwise this test is inert.
        let Ok(root_path)=std::env::var("SPOTARB_R32B_MOCK_KILL_ROOT")else{return};
        use crate::private_https::dispatch_authorized_close_once;
        use crate::private_rest::{BinanceCredentials,CredentialsRef,OkxDomain,
            BinancePositionMode,BINANCE_DEFAULT_RECV_WINDOW_MS};
        let root=PathBuf::from(root_path);
        let store=NativeStateStore::open(root.join("state.json")).unwrap();
        store.create(&empty()).unwrap();
        let active=store.install_native_hedged(
            1,native_core(),entry_economics(),7).unwrap();
        let closing=store.start_native_close(active.generation).unwrap();
        let client="sab00000000000000000000000000081";
        let (_,permit)=store.prepare_native_close_intent(
            closing.generation,7,"kill-f1",client).unwrap();
        let authorized=store.authorize_native_close_order(permit,7).unwrap();
        let creds=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        let fake=R32Http::new(200,format!(
            r#"{{"orderId":810,"clientOrderId":"{client}","status":"FILLED","executedQty":"1"}}"#
        ));
        let (observation,_unrecorded_permit)=dispatch_authorized_close_once(
            &fake,&store,authorized,7,CredentialsRef::Binance(&creds),
            1_700_000_000_123,OkxDomain::Global,BinancePositionMode::OneWay,
            BINANCE_DEFAULT_RECV_WINDOW_MS).unwrap();
        assert_eq!(fake.calls(),1);
        assert_eq!(observation.class,crate::private_rest::RestOutcomeClass::Acknowledged);
        // Real child-process termination, after an HTTP mock returned and
        // before any acknowledgement was committed. No real exchange touched.
        unsafe{libc::raise(libc::SIGKILL)};
        unreachable!("SIGKILL must terminate child")
    }

    #[test]
    fn r32b_process_kill_window_reopens_prepared_recovery_only(){
        use std::os::unix::process::ExitStatusExt;
        let root=root("r32b-process-kill");
        fs::create_dir_all(&root).unwrap();
        let status=std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("authoritative_state::tests::r32b_child_kill_after_http_before_durable_record")
            .arg("--nocapture")
            .env("SPOTARB_R32B_MOCK_KILL_ROOT",&root)
            .status().unwrap();
        assert_eq!(status.signal(),Some(libc::SIGKILL));
        let store=NativeStateStore::open(root.join("state.json")).unwrap();
        let live=store.load().unwrap().unwrap();
        assert_eq!(live.generation,4);
        let recovered=store.load_for_recovery().unwrap().unwrap();
        let active=recovered.active.unwrap();
        assert_eq!(active.lifecycle.state,LifecycleState::RecoveryRequired);
        assert_eq!(active.intents.last().unwrap().state,IntentState::Prepared);
        assert_eq!(active.lifecycle.futures_filled_units,0);
        assert!(store.prepare_native_close_intent(4,7,
            "blind-resend","sab00000000000000000000000000082").is_err());
        let _=fs::remove_dir_all(root);
    }

    #[test]
    fn r32b_binance_futures_entry_fill_reply_is_not_trusted_without_fill_truth(){
        use crate::private_https::dispatch_record_native_entry_once;
        use crate::private_rest::{build_signed_entry_order_from_native_authority,
            BinanceCredentials,CredentialsRef,OkxDomain,BinancePositionMode,
            BINANCE_DEFAULT_RECV_WINDOW_MS};
        use crate::execution_adapter::OrderKind;
        let root=root("r32b-futures-entry");
        let store=NativeStateStore::open(root.join("state.json")).unwrap();
        store.create(&empty()).unwrap();
        let (_,spot_permit)=store.begin_native_entry(
            1,entry_plan(),pristine_entry_economics(),
            "spot","sab00000000000000000000000000083").unwrap();
        let exposed=store.record_spot_entry_observation(
            spot_permit,1_000_000_000_000_000_000,
            Some(decimal_to_atoms("100").unwrap()),true,false,false).unwrap();
        let cid="sab00000000000000000000000000084";
        let (_,futures_permit)=store.prepare_futures_entry_hedge(
            exposed.generation,7,"futures",cid).unwrap();
        let auth=store.authorize_entry_order(futures_permit,&r31_futures_order(cid),7).unwrap();
        let creds=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        let prepared=build_signed_entry_order_from_native_authority(
            auth,OrderKind::Market,None,CredentialsRef::Binance(&creds),
            1_700_000_000_123,OkxDomain::Global,BinancePositionMode::OneWay,
            BINANCE_DEFAULT_RECV_WINDOW_MS).unwrap();
        let fake=R32Http::new(200,format!(
            r#"{{"orderId":10,"clientOrderId":"{cid}","status":"FILLED","executedQty":"1"}}"#
        ));
        let result=dispatch_record_native_entry_once(&fake,&store,prepared,7).unwrap();
        assert_eq!(fake.calls(),1);
        assert_eq!(result.durable_state.generation,5);
        assert!(result.durable_state.active.is_none());
        let e=result.durable_state.entry.as_ref().unwrap();
        assert_eq!(e.phase,NativeEntryPhase::RecoveryRequired);
        assert_eq!(e.intents.last().unwrap().state,IntentState::Unknown);
        assert_eq!(e.futures_filled_atoms_e18,0);
        assert_eq!(e.spot_filled_atoms_e18,1_000_000_000_000_000_000);
        let _=fs::remove_dir_all(root);
    }

    #[test]
    fn r32b_after_send_persist_blocked_keeps_prepared(){
        use crate::private_https::{dispatch_record_native_close_once,NativeRecordDispatchError};
        use crate::private_rest::{BinanceCredentials,CredentialsRef,OkxDomain,
            BinancePositionMode,BINANCE_DEFAULT_RECV_WINDOW_MS};
        use std::sync::atomic::{AtomicUsize,Ordering};
        struct BlockPersist{path:PathBuf,cid:String,calls:AtomicUsize}
        impl crate::private_https::HttpExecutor for BlockPersist{
            fn execute_request(&self,_:&crate::private_https::HttpsRequest)
                ->Result<crate::private_https::HttpResponse,crate::private_https::TransportError>{
                self.calls.fetch_add(1,Ordering::SeqCst);
                fs::create_dir(&self.path).unwrap();
                Ok(crate::private_https::HttpResponse{status:200,headers:vec![],
                    body:format!(r#"{{"orderId":99,"clientOrderId":"{}","status":"FILLED","executedQty":"1"}}"#,self.cid)})
            }
        }
        let (root,store,state)=r31b_close_store("r32b-persist-fault");
        let cid="sab00000000000000000000000000086";
        let (_,permit)=store.prepare_native_close_intent(state.generation,7,"r32b-f1",cid).unwrap();
        let auth=store.authorize_native_close_order(permit,7).unwrap();
        let fake=BlockPersist{path:root.join("state.tmp"),cid:cid.into(),calls:AtomicUsize::new(0)};
        let creds=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        let result=dispatch_record_native_close_once(&fake,&store,auth,7,
            CredentialsRef::Binance(&creds),1_700_000_000_123,
            OkxDomain::Global,BinancePositionMode::OneWay,
            BINANCE_DEFAULT_RECV_WINDOW_MS);
        assert!(matches!(result,Err(NativeRecordDispatchError::State(NativeStateError::Io(_)))));
        assert_eq!(fake.calls.load(Ordering::SeqCst),1);
        let recovered=store.load_for_recovery().unwrap().unwrap();
        assert_eq!(recovered.generation,4);
        let active=recovered.active.unwrap();
        assert_eq!(active.lifecycle.state,LifecycleState::RecoveryRequired);
        assert_eq!(active.intents.last().unwrap().state,IntentState::Prepared);
        assert!(store.prepare_native_close_intent(4,7,"retry",
            "sab00000000000000000000000000087").is_err());
        let _=fs::remove_dir_all(root);
    }

    #[test]
    fn r32c_binance_exact_terminal_query_recovers_durably(){
        use crate::private_https::query_native_close_recovery_once;
        use crate::private_rest::{BinanceCredentials,CredentialsRef,OkxDomain};
        let (root,store,closing)=r31b_close_store("r32c-bn-fill");
        let cid="sab00000000000000000000000000091";
        let (_,permit)=store.prepare_native_close_intent(
            closing.generation,7,"r32c-f1",cid).unwrap();
        // After a possible send, the permit is gone. Query the durable client ID.
        drop(permit);
        let creds=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        let fake=R32Http::new(200,format!(
            r#"{{"orderId":990,"clientOrderId":"{cid}","status":"FILLED","executedQty":"0.4"}}"#
        ));
        let result=query_native_close_recovery_once(&fake,&store,4,
            CredentialsRef::Binance(&creds),1_700_000_000_123,
            OkxDomain::Global,10_000).unwrap();
        assert!(result.evidence_applied);
        assert_eq!(fake.calls(),1);
        assert_eq!(result.state.generation,5);
        let reopened=NativeStateStore::open(root.join("state.json")).unwrap();
        let active=reopened.load().unwrap().unwrap().active.unwrap();
        assert_eq!(active.lifecycle.futures_filled_units,400_000_000_000_000_000);
        assert_eq!(active.lifecycle.state,LifecycleState::ClosingFutures);
        assert_eq!(active.intents.last().unwrap().state,IntentState::Reconciled);
        let (_,next)=reopened.prepare_native_close_intent(
            5,7,"r32c-f2","sab00000000000000000000000000092").unwrap();
        assert_eq!(reopened.authorize_native_close_order(next,7)
            .unwrap().plan().intent.amount,"0.6");
        let _=fs::remove_dir_all(root);
    }

    #[test]
    fn r32c_single_not_found_wrong_client_and_unknown_never_clear_pending(){
        use crate::private_https::query_native_close_recovery_once;
        use crate::private_rest::{BinanceCredentials,CredentialsRef,OkxDomain};
        let creds=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        for (tag,status,body) in [
            ("not-found",400,r#"{"code":-2013,"msg":"Order does not exist"}"#.to_string()),
            ("wrong-client",200,r#"{"orderId":9,"clientOrderId":"WRONG","status":"FILLED","executedQty":"1"}"#.into()),
            ("transport-unknown",503,r#"{"code":-1000,"msg":"Unknown error"}"#.into()),
            ("missing-fill",200,r#"{"orderId":9,"clientOrderId":"sab00000000000000000000000000093","status":"FILLED"}"#.into()),
        ]{
            let (root,store,closing)=r31b_close_store(tag);
            let (_,permit)=store.prepare_native_close_intent(
                closing.generation,7,"r32c-f1",
                "sab00000000000000000000000000093").unwrap();
            drop(permit);
            let fake=R32Http::new(status,body);
            let result=query_native_close_recovery_once(&fake,&store,4,
                CredentialsRef::Binance(&creds),1_700_000_000_123,
                OkxDomain::Global,10_000).unwrap();
            assert!(!result.evidence_applied);
            assert_eq!(fake.calls(),1);
            assert_eq!(result.state.generation,4);
            assert_eq!(result.state.active.as_ref().unwrap().lifecycle.state,
                LifecycleState::RecoveryRequired);
            let original=store.load().unwrap().unwrap().active.unwrap();
            assert_eq!(original.intents.last().unwrap().state,IntentState::Prepared);
            assert!(store.prepare_native_close_intent(4,7,"bad-next",
                "sab00000000000000000000000000094").is_err());
            let _=fs::remove_dir_all(root);
        }
    }

    #[test]
    fn r32c_okx_terminal_close_query_uses_spot_identity(){
        use crate::private_https::query_native_close_recovery_once;
        use crate::private_rest::{OkxCredentials,CredentialsRef,OkxDomain};
        use crate::execution_adapter::ResponseClass;
        let (root,store,closing)=r31b_close_store("r32c-okx");
        let (_,permit)=store.prepare_native_close_intent(closing.generation,7,
            "fut","sab00000000000000000000000000095").unwrap();
        let finished=store.record_native_close_observation(permit,
            MutationObservation{class:ResponseClass::Acknowledged,
                terminal:true,filled_units:1_000_000_000_000_000_000}).unwrap();
        assert_eq!(finished.active.as_ref().unwrap().lifecycle.state,LifecycleState::ClosingSpot);
        let cid="sab00000000000000000000000000096";
        let (_,permit)=store.prepare_native_close_intent(
            finished.generation,7,"spot",cid).unwrap();
        drop(permit);
        let creds=OkxCredentials::new("FAKE_OKX","FAKE_SECRET","FAKE_PASS").unwrap();
        let fake=R32Http::new(200,format!(
            r#"{{"code":"0","data":[{{"ordId":"99","clOrdId":"{cid}","state":"filled","accFillSz":"1"}}]}}"#
        ));
        let result=query_native_close_recovery_once(&fake,&store,6,
            CredentialsRef::Okx(&creds),1_700_000_000_123,
            OkxDomain::Global,10_000).unwrap();
        assert!(result.evidence_applied);
        assert_eq!(fake.calls(),1);
        let active=result.state.active.unwrap();
        assert_eq!(active.lifecycle.spot_filled_units,1_000_000_000_000_000_000);
        assert_eq!(active.lifecycle.state,LifecycleState::VerifyingExitTruth);
        assert!(active.lifecycle.pending.is_none());
        let _=fs::remove_dir_all(root);
    }

    #[test]
    fn r32c_terminal_zero_and_still_open_preserve_correct_recovery(){
        use crate::private_https::query_native_close_recovery_once;
        use crate::private_rest::{BinanceCredentials,CredentialsRef,OkxDomain};
        let creds=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        let cid="sab00000000000000000000000000097";
        let (root,store,closing)=r31b_close_store("r32c-open-cancel");
        let (_,permit)=store.prepare_native_close_intent(
            closing.generation,7,"r32c-f1",cid).unwrap();
        drop(permit);
        let open=R32Http::new(200,format!(
            r#"{{"orderId":1,"clientOrderId":"{cid}","status":"NEW","executedQty":"0"}}"#
        ));
        let before=query_native_close_recovery_once(&open,&store,4,
            CredentialsRef::Binance(&creds),1_700_000_000_123,
            OkxDomain::Global,10_000).unwrap();
        assert!(before.evidence_applied);
        assert_eq!(before.state.generation,5);
        assert_eq!(before.state.active.as_ref().unwrap().lifecycle.state,LifecycleState::RecoveryRequired);
        assert_eq!(before.state.active.as_ref().unwrap().intents.last().unwrap().state,IntentState::Prepared);
        assert!(store.prepare_native_close_intent(5,7,"not-yet","sab00000000000000000000000000098").is_err());
        let canceled=R32Http::new(200,format!(
            r#"{{"orderId":1,"clientOrderId":"{cid}","status":"CANCELED","executedQty":"0"}}"#
        ));
        let after=query_native_close_recovery_once(&canceled,&store,5,
            CredentialsRef::Binance(&creds),1_700_000_000_123,
            OkxDomain::Global,10_000).unwrap();
        assert!(after.evidence_applied);
        assert_eq!(after.state.active.as_ref().unwrap().lifecycle.state,LifecycleState::ClosingFutures);
        assert!(after.state.active.as_ref().unwrap().lifecycle.pending.is_none());
        assert_eq!(after.state.active.as_ref().unwrap().intents.last().unwrap().state,IntentState::Reconciled);
        let (_,next)=store.prepare_native_close_intent(after.state.generation,7,
            "new-id","sab00000000000000000000000000099").unwrap();
        assert_eq!(store.authorize_native_close_order(next,7).unwrap().plan().intent.amount,"1");
        let _=fs::remove_dir_all(root);
    }

    #[test]
    fn r32c_query_wrong_credentials_and_stale_generation_do_not_read_network(){
        use crate::private_https::{query_native_close_recovery_once,NativeQueryRecoveryError};
        use crate::private_rest::{OkxCredentials,BinanceCredentials,CredentialsRef,OkxDomain};
        let (root,store,closing)=r31b_close_store("r32c-credentials");
        let (_,permit)=store.prepare_native_close_intent(
            closing.generation,7,"r32c-f1","sab00000000000000000000000000100").unwrap();
        drop(permit);
        let fake=R32Http::new(200,"{}".into());
        let okx=OkxCredentials::new("FAKE_OKX","FAKE_SECRET","FAKE_PASS").unwrap();
        assert!(matches!(query_native_close_recovery_once(&fake,&store,4,
            CredentialsRef::Okx(&okx),1_700_000_000_123,
            OkxDomain::Global,10_000),Err(NativeQueryRecoveryError::CredentialVenue)));
        let bin=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        assert!(matches!(query_native_close_recovery_once(&fake,&store,3,
            CredentialsRef::Binance(&bin),1_700_000_000_123,
            OkxDomain::Global,10_000),Err(NativeQueryRecoveryError::State(NativeStateError::Generation))));
        assert_eq!(fake.calls(),0);
        assert_eq!(store.load().unwrap().unwrap().generation,4);
        let _=fs::remove_dir_all(root);
    }

    #[derive(Clone,Copy,Debug)]
    enum R32DCase{Candidate,WrongClient,MissingOrder,BalanceConflict,
        PendingOrder,HedgeMode,BadAccount,FutureSideMismatch,UnexpectedShort,OverfillOrder,NonterminalOrder,TerminalZeroFill,TerminalMissingQty,FeeMissing,FeeOtherCurrency,FillPartial,SpotNetMismatch,PartialTerminalTwoFills,DuplicateFillId,DifferentFee,LineageTwoFills}
    struct R32DReadOnlyFixture{
        client_id:String,futures:bool,case:R32DCase,
        calls:std::sync::atomic::AtomicUsize,
    }
    impl R32DReadOnlyFixture{
        fn new(id:&str,futures:bool,case:R32DCase)->Self{
            Self{client_id:id.into(),futures,case,calls:std::sync::atomic::AtomicUsize::new(0)}
        }
        fn count(&self)->usize{self.calls.load(std::sync::atomic::Ordering::SeqCst)}
    }
    impl crate::private_https::HttpExecutor for R32DReadOnlyFixture{
        fn execute_request(&self,req:&crate::private_https::HttpsRequest)
            ->Result<crate::private_https::HttpResponse,crate::private_https::TransportError>{
            use crate::private_rest::HttpMethod;
            assert_eq!(req.method,HttpMethod::Get);
            assert!(!req.mutation,"R32D never sends exchange mutations");
            self.calls.fetch_add(1,std::sync::atomic::Ordering::SeqCst);
            let url=req.url.as_str();
            let id=if matches!(self.case,R32DCase::WrongClient){"BADID"}
                else{self.client_id.as_str()};
            let status=if matches!(self.case,R32DCase::MissingOrder)
                && (url.contains("/fapi/v1/order?")||url.contains("/api/v5/trade/order?")){
                    if self.futures{400}else{200}
                }else{200};
            let balance=if matches!(self.case,R32DCase::BalanceConflict){"0.1"}
                else if matches!(self.case,R32DCase::SpotNetMismatch){"0.525"}
                else if matches!(self.case,R32DCase::DifferentFee){"0.5245"}
                else if self.futures{"1.125"}else{"0.5246"};
            let body=if url.contains("/fapi/v1/order?"){
                if matches!(self.case,R32DCase::MissingOrder){
                    r#"{"code":-2013,"msg":"Order does not exist"}"#.into()
                }else{format!(
                    r#"{{"orderId":124,"clientOrderId":"{id}","status":"{}","executedQty":"{}","avgPrice":"101","cumQuote":"{}"}}"#,
                    if matches!(self.case,R32DCase::NonterminalOrder){"NEW"}
                    else if matches!(self.case,R32DCase::TerminalZeroFill|R32DCase::TerminalMissingQty|R32DCase::PartialTerminalTwoFills|R32DCase::DuplicateFillId|R32DCase::LineageTwoFills){"CANCELED"}
                    else{"FILLED"},
                    if matches!(self.case,R32DCase::OverfillOrder){"2"}
                    else if matches!(self.case,R32DCase::TerminalZeroFill){"0"}
                    else if matches!(self.case,R32DCase::TerminalMissingQty){""}
                    else{"0.4"},
                    if matches!(self.case,R32DCase::OverfillOrder){"202"}
                    else if matches!(self.case,R32DCase::TerminalZeroFill|R32DCase::TerminalMissingQty){"0"}
                    else{"40.4"}
                )}
            }else if url.contains("/fapi/v1/positionSide/dual"){
                if matches!(self.case,R32DCase::HedgeMode){
                    r#"{"dualSidePosition":true}"#.into()
                }else{r#"{"dualSidePosition":false}"#.into()}
            }else if url.contains("/fapi/v1/accountConfig"){
                r#"{"multiAssetsMargin":false}"#.into()
            }else if url.contains("/fapi/v3/balance"){
                r#"[{"asset":"USDT","balance":"100","availableBalance":"100"}]"#.into()
            }else if url.contains("/fapi/v3/positionRisk"){
                if matches!(self.case,R32DCase::BadAccount){
                    r#"{"invalid":true}"#.into()
                }else if matches!(self.case,R32DCase::FutureSideMismatch){
                    r#"[{"symbol":"BNTUSDT","positionAmt":"0.4","positionSide":"LONG"}]"#.into()
                }else if matches!(self.case,R32DCase::UnexpectedShort){
                    if self.futures {
                        r#"[{"symbol":"BNTUSDT","positionAmt":"-0.7","positionSide":"BOTH"}]"#.into()
                    }else{
                        r#"[{"symbol":"BNTUSDT","positionAmt":"-0.4","positionSide":"BOTH"}]"#.into()
                    }
                }else if self.futures{
                    r#"[{"symbol":"BNTUSDT","positionAmt":"-0.4","positionSide":"BOTH"}]"#.into()
                }else{
                    r#"[{"symbol":"BNTUSDT","positionAmt":"0","positionSide":"BOTH"}]"#.into()
                }
            }else if url.contains("/fapi/v1/openOrders"){
                if matches!(self.case,R32DCase::PendingOrder)&&self.futures{
                    format!(r#"[{{"symbol":"BNTUSDT","orderId":1,"clientOrderId":"{}","side":"SELL","positionSide":"BOTH","status":"NEW","reduceOnly":false}}]"#,self.client_id)
                }else{"[]".into()}
            }else if url.contains("/api/v5/trade/order?"){
                if matches!(self.case,R32DCase::MissingOrder){
                    r#"{"code":"51000","data":[]}"#.into()
                }else{format!(
                    r#"{{"code":"0","data":[{{"ordId":"123","clOrdId":"{id}","state":"{}","accFillSz":"{}","avgPx":"100"}}]}}"#,
                    if matches!(self.case,R32DCase::NonterminalOrder){"live"}
                    else if matches!(self.case,R32DCase::TerminalZeroFill|R32DCase::TerminalMissingQty|R32DCase::PartialTerminalTwoFills|R32DCase::DuplicateFillId|R32DCase::LineageTwoFills){"canceled"}
                    else{"filled"},
                    if matches!(self.case,R32DCase::OverfillOrder){"2"}
                    else if matches!(self.case,R32DCase::TerminalZeroFill){"0"}
                    else if matches!(self.case,R32DCase::TerminalMissingQty){""}
                    else{"0.4"}
                )}
            }else if url.contains("/api/v5/account/config"){
                r#"{"code":"0","data":[{"acctLv":"2","posMode":"net_mode"}]}"#.into()
            }else if url.contains("/api/v5/account/balance"){
                format!(
                    r#"{{"code":"0","data":[{{"details":[{{"ccy":"BNT","cashBal":"{balance}","availBal":"{balance}","frozenBal":"0"}}]}}]}}"#
                )
            }else if url.contains("/api/v5/account/positions"){
                r#"{"code":"0","data":[]}"#.into()
            }else if url.contains("/api/v5/trade/orders-pending"){
                if matches!(self.case,R32DCase::PendingOrder)&&!self.futures{
                    format!(
                        r#"{{"code":"0","data":[{{"instId":"BNT-USDT","ordId":"123","clOrdId":"{}","state":"live","side":"buy","sz":"1","accFillSz":"0.4"}}]}}"#,self.client_id)
                }else{r#"{"code":"0","data":[]}"#.into()}
            }else if url.contains("/api/v5/trade/fills-history?"){
                if matches!(self.case,R32DCase::PartialTerminalTwoFills|R32DCase::DuplicateFillId|R32DCase::LineageTwoFills){
                    let id2=if matches!(self.case,R32DCase::DuplicateFillId){"1001"}else{"1002"};
                    let (t1,t2)=if matches!(self.case,R32DCase::LineageTwoFills){
                        ("1780000000220","1780000000320")
                    }else{("10","11")};
                    format!(r#"{{"code":"0","data":[{{"ordId":"123","tradeId":"1001","fillPx":"100","fillSz":"0.2","fee":"-0.0002","feeCcy":"BNT","fillTime":"{t1}"}},{{"ordId":"123","tradeId":"{id2}","fillPx":"100","fillSz":"0.2","fee":"-0.0002","feeCcy":"BNT","fillTime":"{t2}"}}]}}"#)
                }else{
                let fee=match self.case{
                    R32DCase::FeeMissing=>"null",
                    R32DCase::FeeOtherCurrency=>r#""BNB""#,
                    _=>r#""BNT""#,
                };
                let qty=if matches!(self.case,R32DCase::FillPartial){"0.2"}else{"0.4"};
                let fee_value=if matches!(self.case,R32DCase::FeeMissing){"null"}
                    else if matches!(self.case,R32DCase::DifferentFee){r#""-0.0005""#}
                    else{r#""-0.0004""#};
                format!(
                    r#"{{"code":"0","data":[{{"ordId":"123","tradeId":"1001","fillPx":"100","fillSz":"{qty}","fee":{fee_value},"feeCcy":{fee},"fillTime":"10"}}]}}"#
                )
                }
            }else if url.contains("/fapi/v1/userTrades?"){
                if matches!(self.case,R32DCase::PartialTerminalTwoFills|R32DCase::DuplicateFillId|R32DCase::LineageTwoFills){
                    let id2=if matches!(self.case,R32DCase::DuplicateFillId){1001}else{1002};
                    let (t1,t2)=if matches!(self.case,R32DCase::LineageTwoFills){
                        ("1780000000220","1780000000320")
                    }else{("10","11")};
                    format!(r#"[{{"orderId":124,"id":1001,"price":"101","qty":"0.2","commission":"0.02","commissionAsset":"USDT","time":{t1}}},{{"orderId":124,"id":{id2},"price":"101","qty":"0.2","commission":"0.02","commissionAsset":"USDT","time":{t2}}}]"#)
                }else{
                let qty=if matches!(self.case,R32DCase::FillPartial){"0.2"}else{"0.4"};
                let fee_value=if matches!(self.case,R32DCase::FeeMissing){"null"}
                    else if matches!(self.case,R32DCase::DifferentFee){r#""0.05""#}
                    else{r#""0.04""#};
                let currency=if matches!(self.case,R32DCase::FeeOtherCurrency){"BNB"}else{"USDT"};
                format!(
                    r#"[{{"orderId":124,"id":1002,"price":"101","qty":"{qty}","commission":{fee_value},"commissionAsset":"{currency}","time":10}}]"#
                )
                }
            }else{panic!("R32D unexpected signed GET route")}
            ;
            Ok(crate::private_https::HttpResponse{status,headers:vec![],body})
        }
    }
    fn r32d_entry_test_store(futures:bool)
        ->(PathBuf,NativeStateStore,String,u64){
        let root=root(if futures{"r32d-futures"}else{"r32d-spot"});
        let store=NativeStateStore::open(root.join("state.json")).unwrap();
        store.create(&empty()).unwrap();
        let client="sab00000000000000000000000000111".to_string();
        let (_,spot_permit)=store.begin_native_entry(
            1,entry_plan(),pristine_entry_economics(),"r32d-spot",&client).unwrap();
        if futures{
            let exposed=store.record_spot_entry_observation(
                spot_permit,1_000_000_000_000_000_000,
                Some(decimal_to_atoms("100").unwrap()),true,false,false).unwrap();
            let (_,futures_permit)=store.prepare_futures_entry_hedge(
                exposed.generation,7,"r32d-futures",&client.replace("111","112")).unwrap();
            let cid=client.replace("111","112");
            let unknown=store.record_futures_entry_observation(
                futures_permit,0,None,false,false,true).unwrap();
            (root,store,cid,unknown.generation)
        }else{
            let unknown=store.record_spot_entry_observation(
                spot_permit,0,None,false,false,true).unwrap();
            (root,store,client,unknown.generation)
        }
    }

    #[test]
    fn r32d_spot_futures_evidence_probe_is_read_only_and_never_resumes(){
        use crate::native_entry_recovery::{probe_native_entry_readonly_once,EntryProbeVerdict};
        use crate::private_rest::{OkxCredentials,BinanceCredentials,OkxDomain};
        let okx=OkxCredentials::new("FAKE_OKX","FAKE_SECRET","FAKE_PASS").unwrap();
        let bn=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        for futures in [false,true]{
            let (root,store,cid,generation)=r32d_entry_test_store(futures);
            let raw=fs::read(root.join("state.json")).unwrap();
            let fake=R32DReadOnlyFixture::new(&cid,futures,R32DCase::Candidate);
            let proof=probe_native_entry_readonly_once(
                &fake,&store,generation,&okx,&bn,1_700_000_000_123,OkxDomain::Global,10_000
            ).unwrap();
            assert_eq!(fake.count(),10);
            assert!(!proof.may_send_order);
            assert_eq!(proof.target.generation,generation);
            assert_eq!(proof.target.leg,if futures{NativeEntryLeg::Futures}else{NativeEntryLeg::Spot});
            assert_eq!(proof.verdict,EntryProbeVerdict::TerminalCandidateNeedsFillFeesAndInventoryTruth);
            assert_eq!(proof.order.cumulative_qty_atoms,Some(decimal_to_atoms("0.4").unwrap()));
            assert_eq!(fs::read(root.join("state.json")).unwrap(),raw);
            assert!(store.prepare_futures_entry_hedge(generation,7,"wrong-next","new-id").is_err());
            let _=fs::remove_dir_all(root);
        }
    }

    #[test]
    fn r32d_bad_exchange_evidence_cannot_rearm_mutation(){
        use crate::native_entry_recovery::{probe_native_entry_readonly_once,EntryProbeVerdict,EntryProbeError};
        use crate::private_rest::{OkxCredentials,BinanceCredentials,OkxDomain};
        let okx=OkxCredentials::new("FAKE_OKX","FAKE_SECRET","FAKE_PASS").unwrap();
        let bn=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        for futures in [false,true]{
            for case in [
                R32DCase::WrongClient,R32DCase::MissingOrder,
                R32DCase::BalanceConflict,R32DCase::PendingOrder,
                R32DCase::HedgeMode,R32DCase::BadAccount,
                R32DCase::FutureSideMismatch,R32DCase::UnexpectedShort,R32DCase::OverfillOrder,R32DCase::NonterminalOrder,R32DCase::TerminalZeroFill,R32DCase::TerminalMissingQty,
            ]{
                let (root,store,cid,generation)=r32d_entry_test_store(futures);
                let raw=fs::read(root.join("state.json")).unwrap();
                let fake=R32DReadOnlyFixture::new(&cid,futures,case);
                let probe=probe_native_entry_readonly_once(
                    &fake,&store,generation,&okx,&bn,1_700_000_000_123,
                    OkxDomain::Global,10_000);
                if matches!(case,R32DCase::BadAccount){
                    assert!(matches!(probe,Err(EntryProbeError::Truth(_))));
                }else{
                    let report=probe.unwrap();
                    assert!(!report.may_send_order);
                    let expected=match case{
                        R32DCase::WrongClient|R32DCase::MissingOrder=>EntryProbeVerdict::OrderUnverified,
                        R32DCase::BalanceConflict|R32DCase::HedgeMode
                            |R32DCase::FutureSideMismatch|R32DCase::UnexpectedShort
                            =>EntryProbeVerdict::AccountConflict,
                        R32DCase::PendingOrder=>EntryProbeVerdict::OpenOrdersPresent,
                        R32DCase::OverfillOrder|R32DCase::NonterminalOrder
                            |R32DCase::TerminalZeroFill|R32DCase::TerminalMissingQty=>EntryProbeVerdict::OrderUnverified,
                        _=>unreachable!(),
                    };
                    assert_eq!(report.verdict,expected,"case={case:?} futures={futures}");
                }
                assert_eq!(fs::read(root.join("state.json")).unwrap(),raw);
                assert_eq!(store.load_for_recovery().unwrap().unwrap().entry.unwrap().phase,
                    NativeEntryPhase::RecoveryRequired);
                let _=fs::remove_dir_all(root);
            }
        }
    }

    #[test]
    fn r133_terminal_zero_or_missing_original_fill_cannot_trigger_second_private_get(){
        use crate::native_entry_recovery::{
            probe_native_entry_readonly_once,probe_native_entry_fills_readonly_once,
            EntryProbeVerdict,EntryProbeError,
        };
        use crate::private_rest::{OkxCredentials,BinanceCredentials,OkxDomain};
        let okx=OkxCredentials::new("FAKE_OKX","FAKE_SECRET","FAKE_PASS").unwrap();
        let bn=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        for futures in [false,true]{
            for case in [R32DCase::TerminalZeroFill,R32DCase::TerminalMissingQty]{
                let (root,store,cid,generation)=r32d_entry_test_store(futures);
                let before=fs::read(root.join("state.json")).unwrap();
                let fake=R32DReadOnlyFixture::new(&cid,futures,case);
                let report=probe_native_entry_readonly_once(
                    &fake,&store,generation,&okx,&bn,1_700_000_000_123,
                    OkxDomain::Global,10_000).unwrap();
                assert_eq!(report.verdict,EntryProbeVerdict::OrderUnverified);
                assert!(!report.may_send_order);
                assert_eq!(fake.count(),10);
                let second=probe_native_entry_fills_readonly_once(
                    &fake,&store,&report,&okx,&bn,1_700_000_000_123,
                    OkxDomain::Global,10_000);
                assert!(matches!(second,Err(EntryProbeError::ForbiddenNetworkShape)));
                assert_eq!(fake.count(),10,"secondary signed GET blocked");
                assert_eq!(fs::read(root.join("state.json")).unwrap(),before);
                assert_eq!(store.load_for_recovery().unwrap().unwrap().entry.unwrap().phase,
                    NativeEntryPhase::RecoveryRequired);
                let _=fs::remove_dir_all(root);
            }
        }
    }

    #[test]
    fn r134_paginated_history_denies_unverified_original_without_new_get(){
        use crate::native_entry_recovery::{probe_native_entry_readonly_once,EntryProbeVerdict};
        use crate::native_history_audit::{audit_native_entry_paginated_history,HistoryAuditError};
        use crate::private_rest::{OkxCredentials,BinanceCredentials,OkxDomain};
        let okx=OkxCredentials::new("FAKE_OKX","FAKE_SECRET","FAKE_PASS").unwrap();
        let bn=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        for futures in [false,true]{
            for case in [R32DCase::TerminalZeroFill,R32DCase::TerminalMissingQty]{
                let (root,store,cid,generation)=r32d_entry_test_store(futures);
                let original=fs::read(root.join("state.json")).unwrap();
                let fake=R32DReadOnlyFixture::new(&cid,futures,case);
                let proof=probe_native_entry_readonly_once(
                    &fake,&store,generation,&okx,&bn,1_700_000_000_123,
                    OkxDomain::Global,10_000).unwrap();
                assert_eq!(proof.verdict,EntryProbeVerdict::OrderUnverified);
                assert_eq!(fake.count(),10);
                let history=audit_native_entry_paginated_history(
                    &fake,&store,&proof,&okx,&bn,1_700_000_000_123,
                    OkxDomain::Global,10_000,100);
                assert!(matches!(history,Err(HistoryAuditError::Invalid(_))));
                assert_eq!(fake.count(),10,"unverified original must cause zero extra history GET");
                assert_eq!(fs::read(root.join("state.json")).unwrap(),original);
                assert_eq!(store.load_for_recovery().unwrap().unwrap().entry.unwrap().phase,
                    NativeEntryPhase::RecoveryRequired);
                let _=fs::remove_dir_all(root);
            }
        }
    }

    #[test]
    fn r32d_invalid_generation_and_signing_rejected_before_network(){
        use crate::native_entry_recovery::{probe_native_entry_readonly_once,EntryProbeError};
        use crate::private_rest::{OkxCredentials,BinanceCredentials,OkxDomain};
        let (root,store,cid,generation)=r32d_entry_test_store(false);
        let okx=OkxCredentials::new("FAKE_OKX","FAKE_SECRET","FAKE_PASS").unwrap();
        let bn=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        let fake=R32DReadOnlyFixture::new(&cid,false,R32DCase::Candidate);
        let stale=probe_native_entry_readonly_once(
            &fake,&store,generation-1,&okx,&bn,1_700_000_000_123,
            OkxDomain::Global,10_000);
        assert!(matches!(stale,Err(EntryProbeError::State(NativeStateError::Generation))));
        let bad=probe_native_entry_readonly_once(
            &fake,&store,generation,&okx,&bn,1_700_000_000_123,
            OkxDomain::Global,60_001);
        assert!(matches!(bad,Err(EntryProbeError::Build(_))));
        assert_eq!(fake.count(),0);
        let _=fs::remove_dir_all(root);
    }

    #[test]
    fn r32d_generation_changing_mid_query_fails_closed(){
        use crate::native_entry_recovery::{probe_native_entry_readonly_once,EntryProbeError};
        use crate::private_rest::{OkxCredentials,BinanceCredentials,OkxDomain};
        use std::sync::atomic::{AtomicBool,Ordering};
        struct ConcurrentGeneration{
            inner:R32DReadOnlyFixture,store:NativeStateStore,
            old_generation:u64,changed:AtomicBool,
        }
        impl crate::private_https::HttpExecutor for ConcurrentGeneration{
            fn execute_request(&self,req:&crate::private_https::HttpsRequest)
                ->Result<crate::private_https::HttpResponse,crate::private_https::TransportError>{
                if self.inner.count()==5&&!self.changed.swap(true,Ordering::SeqCst){
                    self.store.transact(self.old_generation,|_|Ok(())).unwrap();
                }
                self.inner.execute_request(req)
            }
        }
        let (root,store,cid,generation)=r32d_entry_test_store(false);
        let fake=ConcurrentGeneration{
            inner:R32DReadOnlyFixture::new(&cid,false,R32DCase::Candidate),
            store:store.clone(),old_generation:generation,
            changed:AtomicBool::new(false),
        };
        let okx=OkxCredentials::new("FAKE_OKX","FAKE_SECRET","FAKE_PASS").unwrap();
        let bn=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        let result=probe_native_entry_readonly_once(
            &fake,&store,generation,&okx,&bn,1_700_000_000_123,
            OkxDomain::Global,10_000);
        assert!(matches!(result,Err(EntryProbeError::State(NativeStateError::Generation))));
        assert_eq!(fake.inner.count(),10);
        assert_eq!(store.load().unwrap().unwrap().generation,generation+1);
        assert_eq!(store.load_for_recovery().unwrap().unwrap().entry.unwrap().phase,
            NativeEntryPhase::RecoveryRequired);
        let _=fs::remove_dir_all(root);
    }

    #[test]
    fn r32d2_quote_and_base_fees_are_verified_without_resuming_trading(){
        use crate::native_entry_recovery::{probe_native_entry_readonly_once,
            probe_native_entry_fills_readonly_once,EntryFillVerdict};
        use crate::private_rest::{OkxCredentials,BinanceCredentials,OkxDomain};
        let okx=OkxCredentials::new("FAKE_OKX","FAKE_SECRET","FAKE_PASS").unwrap();
        let bn=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        for futures in [false,true]{
            let (root,store,cid,generation)=r32d_entry_test_store(futures);
            let before=fs::read(root.join("state.json")).unwrap();
            let fake=R32DReadOnlyFixture::new(&cid,futures,R32DCase::Candidate);
            let first=probe_native_entry_readonly_once(&fake,&store,generation,
                &okx,&bn,1_700_000_000_123,OkxDomain::Global,10_000).unwrap();
            assert_eq!(fake.count(),10);
            let detail=probe_native_entry_fills_readonly_once(&fake,&store,&first,
                &okx,&bn,1_700_000_000_123,OkxDomain::Global,10_000).unwrap();
            assert_eq!(fake.count(),11);
            assert_eq!(detail.fill_count,1);
            assert_eq!(detail.filled_atoms_e18,400_000_000_000_000_000);
            assert_eq!(detail.quoted_fee_cost_atoms_e12,40_000_000_000);
            assert!(!detail.may_send_order);
            if futures{
                assert_eq!(detail.verdict,EntryFillVerdict::PriorSpotFeeOriginUnverified);
                assert_eq!(detail.signed_base_fee_atoms_e18,None);
            }else{
                assert_eq!(detail.verdict,EntryFillVerdict::VerifiedCandidateStillBlocked);
                assert_eq!(detail.signed_base_fee_atoms_e18,
                    Some(-400_000_000_000_000));
                assert_eq!(first.observed_spot_cash_atoms_e18,
                    Some(524_600_000_000_000_000));
            }
            assert_eq!(fs::read(root.join("state.json")).unwrap(),before);
            assert!(store.prepare_futures_entry_hedge(generation,7,"blind",
                "sab00000000000000000000000000211").is_err());
            let _=fs::remove_dir_all(root);
        }
    }

    #[test]
    fn r32d2_missing_fee_unsupported_ccy_partial_and_net_mismatch_hold(){
        use crate::native_entry_recovery::{probe_native_entry_readonly_once,
            probe_native_entry_fills_readonly_once,EntryFillVerdict};
        use crate::private_rest::{OkxCredentials,BinanceCredentials,OkxDomain};
        let okx=OkxCredentials::new("FAKE_OKX","FAKE_SECRET","FAKE_PASS").unwrap();
        let bn=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        for futures in [false,true]{
            for case in [R32DCase::FeeMissing,R32DCase::FeeOtherCurrency,
                R32DCase::FillPartial,R32DCase::SpotNetMismatch]{
                let (root,store,cid,generation)=r32d_entry_test_store(futures);
                let before=fs::read(root.join("state.json")).unwrap();
                let fake=R32DReadOnlyFixture::new(&cid,futures,case);
                let first=probe_native_entry_readonly_once(&fake,&store,generation,
                    &okx,&bn,1_700_000_000_123,OkxDomain::Global,10_000).unwrap();
                let detail=probe_native_entry_fills_readonly_once(&fake,&store,&first,
                    &okx,&bn,1_700_000_000_123,OkxDomain::Global,10_000).unwrap();
                assert_eq!(fake.count(),11);
                assert!(!detail.may_send_order);
                let expected=match case{
                    R32DCase::FeeMissing|R32DCase::FeeOtherCurrency=>
                        EntryFillVerdict::IncompleteFeeEvidence,
                    R32DCase::FillPartial=>EntryFillVerdict::IncompleteFills,
                    R32DCase::SpotNetMismatch=>if futures{
                        EntryFillVerdict::PriorSpotFeeOriginUnverified
                    }else{EntryFillVerdict::NetSpotBalanceMismatch},
                    _=>unreachable!(),
                };
                assert_eq!(detail.verdict,expected,"futures={futures} case={case:?}");
                assert_eq!(fs::read(root.join("state.json")).unwrap(),before);
                let _=fs::remove_dir_all(root);
            }
        }
    }

    #[test]
    fn r32d2_no_fill_query_for_unverified_order_and_stale_generation(){
        use crate::native_entry_recovery::{probe_native_entry_readonly_once,
            probe_native_entry_fills_readonly_once,EntryProbeError};
        use crate::private_rest::{OkxCredentials,BinanceCredentials,OkxDomain};
        let okx=OkxCredentials::new("FAKE_OKX","FAKE_SECRET","FAKE_PASS").unwrap();
        let bn=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        let (root,store,cid,generation)=r32d_entry_test_store(false);
        let bad=R32DReadOnlyFixture::new(&cid,false,R32DCase::WrongClient);
        let first=probe_native_entry_readonly_once(&bad,&store,generation,
            &okx,&bn,1_700_000_000_123,OkxDomain::Global,10_000).unwrap();
        let result=probe_native_entry_fills_readonly_once(&bad,&store,&first,
            &okx,&bn,1_700_000_000_123,OkxDomain::Global,10_000);
        assert!(matches!(result,Err(EntryProbeError::ForbiddenNetworkShape)));
        assert_eq!(bad.count(),10);
        let good=R32DReadOnlyFixture::new(&cid,false,R32DCase::Candidate);
        let report=probe_native_entry_readonly_once(&good,&store,generation,
            &okx,&bn,1_700_000_000_123,OkxDomain::Global,10_000).unwrap();
        store.transact(generation,|_|Ok(())).unwrap();
        let stale=probe_native_entry_fills_readonly_once(&good,&store,&report,
            &okx,&bn,1_700_000_000_123,OkxDomain::Global,10_000);
        assert!(matches!(stale,Err(EntryProbeError::State(NativeStateError::Generation))));
        assert_eq!(good.count(),10);
        let _=fs::remove_dir_all(root);
    }

    #[test]
    fn r32d2_two_partial_fills_on_canceled_entry_match_full_history(){
        use crate::native_entry_recovery::{probe_native_entry_readonly_once,
            probe_native_entry_fills_readonly_once,EntryFillVerdict};
        use crate::private_rest::{OkxCredentials,BinanceCredentials,OkxDomain};
        let okx=OkxCredentials::new("FAKE_OKX","FAKE_SECRET","FAKE_PASS").unwrap();
        let bn=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        for futures in [false,true]{
            let (root,store,cid,generation)=r32d_entry_test_store(futures);
            let before=fs::read(root.join("state.json")).unwrap();
            let fake=R32DReadOnlyFixture::new(&cid,futures,R32DCase::PartialTerminalTwoFills);
            let first=probe_native_entry_readonly_once(&fake,&store,generation,
                &okx,&bn,1_700_000_000_123,OkxDomain::Global,10_000).unwrap();
            assert_eq!(first.order.status.as_deref(),Some("canceled"));
            let detail=probe_native_entry_fills_readonly_once(&fake,&store,&first,
                &okx,&bn,1_700_000_000_123,OkxDomain::Global,10_000).unwrap();
            assert_eq!(fake.count(),11);
            assert_eq!(detail.fill_count,2);
            assert_eq!(detail.filled_atoms_e18,400_000_000_000_000_000);
            assert_eq!(detail.quoted_fee_cost_atoms_e12,40_000_000_000);
            assert!(!detail.may_send_order);
            assert_eq!(detail.verdict,if futures{
                EntryFillVerdict::PriorSpotFeeOriginUnverified
            }else{EntryFillVerdict::VerifiedCandidateStillBlocked});
            assert_eq!(fs::read(root.join("state.json")).unwrap(),before);
            assert!(store.prepare_futures_entry_hedge(generation,7,"blind",
                "sab00000000000000000000000000222").is_err());
            let _=fs::remove_dir_all(root);
        }
    }

    #[test]
    fn r32d2_duplicate_trade_id_fails_closed_without_state_mutation(){
        use crate::native_entry_recovery::{probe_native_entry_readonly_once,
            probe_native_entry_fills_readonly_once,EntryProbeError};
        use crate::private_rest::{OkxCredentials,BinanceCredentials,OkxDomain};
        let okx=OkxCredentials::new("FAKE_OKX","FAKE_SECRET","FAKE_PASS").unwrap();
        let bn=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        for futures in [false,true]{
            let (root,store,cid,generation)=r32d_entry_test_store(futures);
            let before=fs::read(root.join("state.json")).unwrap();
            let fake=R32DReadOnlyFixture::new(&cid,futures,R32DCase::DuplicateFillId);
            let first=probe_native_entry_readonly_once(&fake,&store,generation,
                &okx,&bn,1_700_000_000_123,OkxDomain::Global,10_000).unwrap();
            let detail=probe_native_entry_fills_readonly_once(&fake,&store,&first,
                &okx,&bn,1_700_000_000_123,OkxDomain::Global,10_000);
            assert!(matches!(detail,Err(EntryProbeError::ForbiddenNetworkShape)));
            assert_eq!(fake.count(),11);
            assert_eq!(fs::read(root.join("state.json")).unwrap(),before);
            let _=fs::remove_dir_all(root);
        }
    }

    #[test]
    fn r32d2_native_generation_changes_during_fill_get_fails_closed(){
        use crate::native_entry_recovery::{probe_native_entry_readonly_once,
            probe_native_entry_fills_readonly_once,EntryProbeError};
        use crate::private_rest::{OkxCredentials,BinanceCredentials,OkxDomain};
        use std::sync::atomic::{AtomicBool,Ordering};
        struct MutatingFillRead{
            inner:R32DReadOnlyFixture,store:NativeStateStore,
            expected:u64,once:AtomicBool,
        }
        impl crate::private_https::HttpExecutor for MutatingFillRead{
            fn execute_request(&self,req:&crate::private_https::HttpsRequest)
                ->Result<crate::private_https::HttpResponse,crate::private_https::TransportError>{
                let response=self.inner.execute_request(req);
                if !self.once.swap(true,Ordering::SeqCst){
                    // The local native state changed while an authenticated
                    // read-only fill-history response was being collected.
                    self.store.transact(self.expected,|_|Ok(())).unwrap();
                }
                response
            }
        }
        let okx=OkxCredentials::new("FAKE_OKX","FAKE_SECRET","FAKE_PASS").unwrap();
        let bn=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        let (root,store,cid,generation)=r32d_entry_test_store(false);
        let normal=R32DReadOnlyFixture::new(&cid,false,R32DCase::Candidate);
        let report=probe_native_entry_readonly_once(&normal,&store,generation,
            &okx,&bn,1_700_000_000_123,OkxDomain::Global,10_000).unwrap();
        let during=MutatingFillRead{
            inner:R32DReadOnlyFixture::new(&cid,false,R32DCase::Candidate),
            store:store.clone(),expected:generation,once:AtomicBool::new(false),
        };
        let out=probe_native_entry_fills_readonly_once(&during,&store,&report,
            &okx,&bn,1_700_000_000_123,OkxDomain::Global,10_000);
        assert!(matches!(out,Err(EntryProbeError::State(NativeStateError::Generation))));
        assert_eq!(during.inner.count(),1);
        assert_eq!(store.load().unwrap().unwrap().generation,generation+1);
        assert_eq!(store.load_for_recovery().unwrap().unwrap().entry.unwrap().phase,
            NativeEntryPhase::RecoveryRequired);
        let _=fs::remove_dir_all(root);
    }

    #[test]
    fn r32d3_durable_checkpoint_reopen_and_idempotent_replay_for_both_legs(){
        use crate::native_entry_recovery::{probe_native_entry_readonly_once,
            probe_native_entry_fills_readonly_once};
        use crate::private_rest::{OkxCredentials,BinanceCredentials,OkxDomain};
        let okx=OkxCredentials::new("FAKE_OKX","FAKE_SECRET","FAKE_PASS").unwrap();
        let bin=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        for futures in [false,true]{
            let (root,store,cid,generation)=r32d_entry_test_store(futures);
            let fake=R32DReadOnlyFixture::new(&cid,futures,R32DCase::Candidate);
            let first=probe_native_entry_readonly_once(&fake,&store,generation,
                &okx,&bin,1_700_000_000_123,OkxDomain::Global,10_000).unwrap();
            let report=probe_native_entry_fills_readonly_once(&fake,&store,&first,
                &okx,&bin,1_700_000_000_123,OkxDomain::Global,10_000).unwrap();
            let (persisted,kind)=store.record_native_entry_recovery_checkpoint(report).unwrap();
            assert_eq!(kind,NativeEntryRecoveryWrite::Recorded);
            assert_eq!(persisted.generation,generation+1);
            let reopened=NativeStateStore::open(root.join("state.json")).unwrap();
            assert_eq!(reopened.load().unwrap().unwrap(),persisted);
            let e=persisted.entry.as_ref().unwrap();
            assert_eq!(e.phase,NativeEntryPhase::RecoveryRequired);
            assert_eq!(e.intents.last().unwrap().state,IntentState::Unknown);
            assert_eq!(e.spot_filled_atoms_e18,if futures{1_000_000_000_000_000_000}else{0});
            assert_eq!(e.futures_filled_atoms_e18,0);
            let proof=e.recovery_checkpoint.as_ref().unwrap();
            assert_eq!(proof.ledger.fills.len(),1);
            assert!(proof.ledger.terminal);
            assert!(proof.ledger.fee_evidence_complete);
            assert_eq!(proof.ledger.filled_qty_atoms(),decimal_to_atoms("0.4").unwrap());
            assert_eq!(proof.ledger.fee_cost_quote_atoms,40_000_000_000);
            assert_eq!(proof.quality,if futures{NativeEntryRecoveryQuality::FuturesPriorSpotUnverified}
                else{NativeEntryRecoveryQuality::SpotCashMatched});
            assert_eq!(proof.signed_base_fee_atoms_e18,
                if futures{None}else{Some(-400_000_000_000_000)});
            // Replay after process reopen: issue authenticated mock reads afresh
            // at current generation but do not append fills twice.
            let before=fs::read(root.join("state.json")).unwrap();
            let evidence_again=probe_native_entry_readonly_once(&fake,&reopened,persisted.generation,
                &okx,&bin,1_700_000_000_123,OkxDomain::Global,10_000).unwrap();
            let detail_again=probe_native_entry_fills_readonly_once(&fake,&reopened,&evidence_again,
                &okx,&bin,1_700_000_000_123,OkxDomain::Global,10_000).unwrap();
            let (unchanged,replay)=reopened.record_native_entry_recovery_checkpoint(detail_again).unwrap();
            assert_eq!(replay,NativeEntryRecoveryWrite::Idempotent);
            assert_eq!(unchanged,persisted);
            assert_eq!(fs::read(root.join("state.json")).unwrap(),before);
            assert_eq!(fake.count(),22);
            assert!(reopened.prepare_futures_entry_hedge(persisted.generation,7,
                "forbidden","sab00000000000000000000000000301").is_err());
            let _=fs::remove_dir_all(root);
        }
    }

    #[test]
    fn r32d3_conflicting_fee_evidence_cannot_replace_checkpoint(){
        use crate::native_entry_recovery::{probe_native_entry_readonly_once,
            probe_native_entry_fills_readonly_once};
        use crate::private_rest::{OkxCredentials,BinanceCredentials,OkxDomain};
        let okx=OkxCredentials::new("FAKE_OKX","FAKE_SECRET","FAKE_PASS").unwrap();
        let bin=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        for futures in [false,true]{
            let (root,store,cid,generation)=r32d_entry_test_store(futures);
            let first_fake=R32DReadOnlyFixture::new(&cid,futures,R32DCase::Candidate);
            let first=probe_native_entry_readonly_once(&first_fake,&store,generation,
                &okx,&bin,1_700_000_000_123,OkxDomain::Global,10_000).unwrap();
            let details=probe_native_entry_fills_readonly_once(&first_fake,&store,&first,
                &okx,&bin,1_700_000_000_123,OkxDomain::Global,10_000).unwrap();
            let (saved,_)=store.record_native_entry_recovery_checkpoint(details).unwrap();
            let before=fs::read(root.join("state.json")).unwrap();
            let changed=R32DReadOnlyFixture::new(&cid,futures,R32DCase::DifferentFee);
            let different=probe_native_entry_readonly_once(&changed,&store,saved.generation,
                &okx,&bin,1_700_000_000_123,OkxDomain::Global,10_000).unwrap();
            let inconsistent=probe_native_entry_fills_readonly_once(&changed,&store,&different,
                &okx,&bin,1_700_000_000_123,OkxDomain::Global,10_000).unwrap();
            assert_eq!(store.record_native_entry_recovery_checkpoint(inconsistent),
                Err(NativeStateError::Invalid("conflicting recovery evidence; manual review")));
            assert_eq!(fs::read(root.join("state.json")).unwrap(),before);
            assert_eq!(store.load().unwrap().unwrap(),saved);
            let _=fs::remove_dir_all(root);
        }
    }

    #[test]
    fn r32d3_durable_write_failure_keeps_unknown_and_retry_is_exact(){
        use crate::native_entry_recovery::{probe_native_entry_readonly_once,
            probe_native_entry_fills_readonly_once};
        use crate::private_rest::{OkxCredentials,BinanceCredentials,OkxDomain};
        let okx=OkxCredentials::new("FAKE_OKX","FAKE_SECRET","FAKE_PASS").unwrap();
        let bin=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        let (root,store,cid,generation)=r32d_entry_test_store(false);
        let fake=R32DReadOnlyFixture::new(&cid,false,R32DCase::Candidate);
        let first=probe_native_entry_readonly_once(&fake,&store,generation,
            &okx,&bin,1_700_000_000_123,OkxDomain::Global,10_000).unwrap();
        let details=probe_native_entry_fills_readonly_once(&fake,&store,&first,
            &okx,&bin,1_700_000_000_123,OkxDomain::Global,10_000).unwrap();
        let before=fs::read(root.join("state.json")).unwrap();
        // State tmp path blocked after read-only evidence but before fsync:
        // NO partial success, original checksum and unresolved intent survive.
        let obstacle=root.join("state.tmp");
        fs::create_dir(&obstacle).unwrap();
        let outcome=store.record_native_entry_recovery_checkpoint(details);
        assert!(matches!(outcome,Err(NativeStateError::Io(_))));
        assert_eq!(fs::read(root.join("state.json")).unwrap(),before);
        let reopened=NativeStateStore::open(root.join("state.json")).unwrap();
        let old=reopened.load_for_recovery().unwrap().unwrap();
        assert_eq!(old.generation,generation);
        assert_eq!(old.entry.unwrap().phase,NativeEntryPhase::RecoveryRequired);
        fs::remove_dir(&obstacle).unwrap();
        let recheck=probe_native_entry_readonly_once(&fake,&reopened,generation,
            &okx,&bin,1_700_000_000_123,OkxDomain::Global,10_000).unwrap();
        let verified=probe_native_entry_fills_readonly_once(&fake,&reopened,&recheck,
            &okx,&bin,1_700_000_000_123,OkxDomain::Global,10_000).unwrap();
        let (committed,status)=reopened.record_native_entry_recovery_checkpoint(verified).unwrap();
        assert_eq!(status,NativeEntryRecoveryWrite::Recorded);
        assert_eq!(committed.generation,generation+1);
        assert!(committed.entry.unwrap().recovery_checkpoint.is_some());
        let _=fs::remove_dir_all(root);
    }

    #[test]
    fn r32d3_incomplete_or_conflicting_input_does_not_write(){
        use crate::native_entry_recovery::{probe_native_entry_readonly_once,
            probe_native_entry_fills_readonly_once};
        use crate::private_rest::{OkxCredentials,BinanceCredentials,OkxDomain};
        let okx=OkxCredentials::new("FAKE_OKX","FAKE_SECRET","FAKE_PASS").unwrap();
        let bin=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        for case in [R32DCase::FeeMissing,R32DCase::FeeOtherCurrency,
            R32DCase::FillPartial,R32DCase::SpotNetMismatch]{
            let (root,store,cid,generation)=r32d_entry_test_store(false);
            let original=fs::read(root.join("state.json")).unwrap();
            let fake=R32DReadOnlyFixture::new(&cid,false,case);
            let account=probe_native_entry_readonly_once(&fake,&store,generation,
                &okx,&bin,1_700_000_000_123,OkxDomain::Global,10_000).unwrap();
            let detail=probe_native_entry_fills_readonly_once(&fake,&store,&account,
                &okx,&bin,1_700_000_000_123,OkxDomain::Global,10_000).unwrap();
            assert_eq!(store.record_native_entry_recovery_checkpoint(detail),
                Err(NativeStateError::Invalid("incomplete recovery evidence")));
            assert_eq!(fs::read(root.join("state.json")).unwrap(),original);
            let _=fs::remove_dir_all(root);
        }
    }

    #[test]
    fn r32d3_child_exit_before_or_after_durable_checkpoint(){
        let Ok(root_path)=std::env::var("R32D3_CHILD_ROOT")else{return};
        use crate::native_entry_recovery::{
            probe_native_entry_readonly_once,probe_native_entry_fills_readonly_once};
        use crate::private_rest::{OkxCredentials,BinanceCredentials,OkxDomain};
        let root=PathBuf::from(root_path);
        let store=NativeStateStore::open(root.join("state.json")).unwrap();
        store.create(&empty()).unwrap();
        let cid="sab00000000000000000000000000401";
        let (_,permit)=store.begin_native_entry(1,entry_plan(),
            pristine_entry_economics(),"child-spot",cid).unwrap();
        store.record_spot_entry_observation(permit,0,None,false,false,true).unwrap();
        let okx=OkxCredentials::new("FAKE_OKX","FAKE_SECRET","FAKE_PASS").unwrap();
        let bin=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        let fake=R32DReadOnlyFixture::new(cid,false,R32DCase::Candidate);
        let first=probe_native_entry_readonly_once(&fake,&store,3,
            &okx,&bin,1_700_000_000_123,OkxDomain::Global,10_000).unwrap();
        let proof=probe_native_entry_fills_readonly_once(&fake,&store,&first,
            &okx,&bin,1_700_000_000_123,OkxDomain::Global,10_000).unwrap();
        if std::env::var("R32D3_CHILD_STAGE").as_deref()==Ok("after"){
            store.record_native_entry_recovery_checkpoint(proof).unwrap();
        }
        // Abrupt process exit, no Rust stack unwinding or cleanup.
        std::process::exit(73);
    }

    #[test]
    fn r32d3_real_subprocess_exit_boundary_before_and_after_checkpoint(){
        use crate::native_entry_recovery::{
            probe_native_entry_readonly_once,probe_native_entry_fills_readonly_once};
        use crate::private_rest::{OkxCredentials,BinanceCredentials,OkxDomain};
        let okx=OkxCredentials::new("FAKE_OKX","FAKE_SECRET","FAKE_PASS").unwrap();
        let bin=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        for stage in ["before","after"]{
            let root=root(&format!("r32d3-process-{stage}"));
            fs::create_dir_all(&root).unwrap();
            let status=std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("authoritative_state::tests::r32d3_child_exit_before_or_after_durable_checkpoint")
                .env("R32D3_CHILD_ROOT",&root)
                .env("R32D3_CHILD_STAGE",stage)
                .status().unwrap();
            assert_eq!(status.code(),Some(73));
            let store=NativeStateStore::open(root.join("state.json")).unwrap();
            let before=store.load().unwrap().unwrap();
            assert_eq!(before.generation,if stage=="before"{3}else{4});
            let entry=before.entry.as_ref().unwrap();
            assert_eq!(entry.phase,NativeEntryPhase::RecoveryRequired);
            assert_eq!(entry.spot_filled_atoms_e18,0);
            assert_eq!(entry.intents[0].state,IntentState::Unknown);
            assert_eq!(entry.recovery_checkpoint.is_some(),stage=="after");
            let old_bytes=fs::read(root.join("state.json")).unwrap();
            let fake=R32DReadOnlyFixture::new(
                "sab00000000000000000000000000401",false,R32DCase::Candidate);
            let first=probe_native_entry_readonly_once(&fake,&store,before.generation,
                &okx,&bin,1_700_000_000_123,OkxDomain::Global,10_000).unwrap();
            let detail=probe_native_entry_fills_readonly_once(&fake,&store,&first,
                &okx,&bin,1_700_000_000_123,OkxDomain::Global,10_000).unwrap();
            let (after,outcome)=store.record_native_entry_recovery_checkpoint(detail).unwrap();
            assert_eq!(after.generation,4);
            if stage=="after"{
                assert_eq!(outcome,NativeEntryRecoveryWrite::Idempotent);
                assert_eq!(fs::read(root.join("state.json")).unwrap(),old_bytes);
            }else{
                assert_eq!(outcome,NativeEntryRecoveryWrite::Recorded);
            }
            assert_eq!(fake.count(),11);
            assert!(store.prepare_futures_entry_hedge(4,7,"forbidden",
                "sab00000000000000000000000000402").is_err());
            let _=fs::remove_dir_all(root);
        }
    }

    #[test]
    fn r32d3_stale_capture_and_tampered_net_fee_do_not_commit(){
        use crate::native_entry_recovery::{
            probe_native_entry_readonly_once,probe_native_entry_fills_readonly_once};
        use crate::private_rest::{OkxCredentials,BinanceCredentials,OkxDomain};
        let okx=OkxCredentials::new("FAKE_OKX","FAKE_SECRET","FAKE_PASS").unwrap();
        let bin=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        for tamper in [false,true]{
            let (root,store,cid,generation)=r32d_entry_test_store(false);
            let fake=R32DReadOnlyFixture::new(&cid,false,R32DCase::Candidate);
            let probe=probe_native_entry_readonly_once(&fake,&store,generation,
                &okx,&bin,1_700_000_000_123,OkxDomain::Global,10_000).unwrap();
            let mut detail=probe_native_entry_fills_readonly_once(&fake,&store,&probe,
                &okx,&bin,1_700_000_000_123,OkxDomain::Global,10_000).unwrap();
            if tamper{
                detail.signed_base_fee_atoms_e18=Some(0);
                detail.sealed_checkpoint.as_mut().unwrap().signed_base_fee_atoms_e18=Some(0);
            }else{
                store.transact(generation,|_|Ok(())).unwrap();
            }
            let old=fs::read(root.join("state.json")).unwrap();
            let result=store.record_native_entry_recovery_checkpoint(detail);
            if tamper{
                assert!(matches!(result,Err(NativeStateError::Invalid(
                    "spot checkpoint net cash mismatch"))));
            }else{
                assert!(matches!(result,Err(NativeStateError::Generation)));
            }
            assert_eq!(fs::read(root.join("state.json")).unwrap(),old);
            assert!(store.load().unwrap().unwrap().entry.unwrap().recovery_checkpoint.is_none());
            let _=fs::remove_dir_all(root);
        }
    }

    #[derive(Clone,Copy,Debug)]
    enum R32D4Case {Full,RepeatedCursor,MissingSecondPage,OtherOrder,BadTime,NoEnd}
    struct R32D4HistoryMock {
        futures:bool,
        case:R32D4Case,
        calls:std::sync::atomic::AtomicUsize,
    }
    impl R32D4HistoryMock{
        fn new(futures:bool,case:R32D4Case)->Self{
            Self{futures,case,calls:std::sync::atomic::AtomicUsize::new(0)}
        }
        fn calls(&self)->usize{
            self.calls.load(std::sync::atomic::Ordering::SeqCst)
        }
    }
    impl crate::private_https::HttpExecutor for R32D4HistoryMock {
        fn execute_request(&self,req:&crate::private_https::HttpsRequest)
            ->Result<crate::private_https::HttpResponse,crate::private_https::TransportError>{
            use crate::private_rest::HttpMethod;
            assert_eq!(req.method,HttpMethod::Get);
            assert!(!req.mutation);
            let parsed=url::Url::parse(&req.url).unwrap();
            let path=parsed.path();
            let query=parsed.query_pairs().collect::<std::collections::BTreeMap<_,_>>();
            let spot=path=="/api/v5/trade/fills-history";
            let fut=path=="/fapi/v1/userTrades";
            assert!(spot||fut);
            assert_eq!(fut,self.futures);
            assert_eq!(query.get("limit").map(|x|x.as_ref()),Some("1"));
            let n=self.calls.fetch_add(1,std::sync::atomic::Ordering::SeqCst);
            if matches!(self.case,R32D4Case::NoEnd){
                let body=if spot{
                    let expected=if n==0{None}else{Some((201-n).to_string())};
                    assert_eq!(query.get("after").map(|s|s.to_string()),expected);
                    let bill=200-n;
                    let trade=5000+n;
                    format!(r#"{{"code":"0","data":[{{"ordId":"123","billId":"{bill}","tradeId":"{trade}","fillPx":"100","fillSz":"0.00001","fee":"0","feeCcy":"BNT","fillTime":"1780000000225"}}]}}"#)
                }else{
                    let from=if n==0{0}else{1001+n};
                    assert_eq!(query.get("fromId").map(|s|s.to_string()),Some(from.to_string()));
                    let trade=1001+n;
                    format!(r#"[{{"orderId":124,"id":{trade},"price":"101","qty":"0.00001","commission":"0","commissionAsset":"USDT","time":1780000000225}}]"#)
                };
                return Ok(crate::private_https::HttpResponse{
                    status:200,headers:vec![],body
                })
            }
            let (idx,ord)=if spot{
                assert_eq!(query.get("ordId").map(|x|x.as_ref()),Some("123"));
                (match query.get("after").map(|x|x.as_ref()){
                    None=>0,
                    Some("200")=>1,
                    Some("199")=>2,
                    Some(x)=>panic!("bad OKX page cursor {x}"),
                },"123")
            }else{
                assert_eq!(query.get("orderId").map(|x|x.as_ref()),Some("124"));
                (match query.get("fromId").map(|x|x.as_ref()){
                    Some("0")=>0,
                    Some("1002")=>1,
                    Some("1003")=>2,
                    Some(x)=>panic!("bad Binance page cursor {x}"),
                    None=>panic!("missing Binance fromId"),
                },"124")
            };
            assert_eq!(n,idx,"unexpected missing/extra history page");
            let actual_idx=if matches!(self.case,R32D4Case::MissingSecondPage)&&idx==1{
                2
            }else{idx};
            let cursor_idx=if matches!(self.case,R32D4Case::RepeatedCursor)&&idx==1{
                0
            }else{actual_idx};
            let wrong_order=matches!(self.case,R32D4Case::OtherOrder) && idx==1;
            let ord=if wrong_order{"999"}else{ord};
            let time=if matches!(self.case,R32D4Case::BadTime){
                "1770000000000"
            }else{"1780000000225"};
            let body=if actual_idx>=2 && !matches!(self.case,R32D4Case::NoEnd){
                if spot{r#"{"code":"0","data":[]}"#.to_string()}else{"[]".into()}
            }else if spot{
                let bill=if cursor_idx==0{"200"}else{"199"};
                let trade=if cursor_idx==0{"1001"}else{"1002"};
                format!(r#"{{"code":"0","data":[{{"ordId":"{ord}","billId":"{bill}","tradeId":"{trade}","fillPx":"100","fillSz":"0.2","fee":"-0.0002","feeCcy":"BNT","fillTime":"{time}"}}]}}"#)
            }else{
                let trade=if cursor_idx==0{1001}else{1002};
                format!(r#"[{{"orderId":{ord},"id":{trade},"price":"101","qty":"0.2","commission":"0.02","commissionAsset":"USDT","time":{time}}}]"#)
            };
            Ok(crate::private_https::HttpResponse{status:200,headers:vec![],body})
        }
    }

    #[test]
    fn r32d4_both_venues_paginated_history_requires_explicit_empty_tail(){
        use crate::native_history_audit::{audit_native_entry_paginated_history,HistoryAuditVerdict};
        use crate::native_entry_recovery::probe_native_entry_readonly_once;
        use crate::private_rest::{OkxCredentials,BinanceCredentials,OkxDomain};
        let okx=OkxCredentials::new("FAKE_OKX","FAKE_SECRET","FAKE_PASS").unwrap();
        let bin=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        for futures in [false,true]{
            let (root,store,cid,generation)=r32d_entry_test_store(futures);
            let before=fs::read(root.join("state.json")).unwrap();
            let account_fake=R32DReadOnlyFixture::new(&cid,futures,R32DCase::Candidate);
            let probe=probe_native_entry_readonly_once(&account_fake,&store,generation,
                &okx,&bin,1_700_000_000_123,OkxDomain::Global,10_000).unwrap();
            let paging=R32D4HistoryMock::new(futures,R32D4Case::Full);
            let audited=audit_native_entry_paginated_history(
                &paging,&store,&probe,&okx,&bin,1_780_000_010_125,
                OkxDomain::Global,10_000,1).unwrap();
            assert_eq!(account_fake.count(),10);
            assert_eq!(paging.calls(),3);
            assert_eq!(audited.page_count,3);
            assert_eq!(audited.trade_count,2);
            assert_eq!(audited.gross_filled_atoms_e18,400_000_000_000_000_000);
            assert_eq!(audited.fee_quote_atoms_e12,40_000_000_000);
            assert_eq!(audited.base_fee_atoms_e18,
                if futures{None}else{Some(-400_000_000_000_000)});
            assert_eq!(audited.verdict,if futures{
                HistoryAuditVerdict::PriorSpotFeeOriginUnverified
            }else{
                HistoryAuditVerdict::FullPageExhaustionCandidateAccountFreshnessUnverified
            });
            assert!(audited.page_exhaustion_observed);
            assert!(!audited.account_snapshot_freshness_proven);
            assert!(!audited.may_send_order);
            assert_eq!(fs::read(root.join("state.json")).unwrap(),before);
            assert!(store.prepare_futures_entry_hedge(generation,7,"forbidden",
                "sab00000000000000000000000000501").is_err());
            let _=fs::remove_dir_all(root);
        }
    }

    #[test]
    fn r32d4_missing_page_repeated_cursor_wrong_order_and_stale_time_fail(){
        use crate::native_history_audit::{audit_native_entry_paginated_history,HistoryAuditError};
        use crate::native_entry_recovery::probe_native_entry_readonly_once;
        use crate::private_rest::{OkxCredentials,BinanceCredentials,OkxDomain};
        let okx=OkxCredentials::new("FAKE_OKX","FAKE_SECRET","FAKE_PASS").unwrap();
        let bin=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        for futures in [false,true]{
            for case in [R32D4Case::MissingSecondPage,R32D4Case::RepeatedCursor,
                R32D4Case::OtherOrder,R32D4Case::BadTime]{
                let (root,store,cid,generation)=r32d_entry_test_store(futures);
                let before=fs::read(root.join("state.json")).unwrap();
                let account_fake=R32DReadOnlyFixture::new(&cid,futures,R32DCase::Candidate);
                let probe=probe_native_entry_readonly_once(&account_fake,&store,generation,
                    &okx,&bin,1_700_000_000_123,OkxDomain::Global,10_000).unwrap();
                let paging=R32D4HistoryMock::new(futures,case);
                let outcome=audit_native_entry_paginated_history(
                    &paging,&store,&probe,&okx,&bin,1_780_000_010_125,
                    OkxDomain::Global,10_000,1);
                assert!(matches!(outcome,Err(HistoryAuditError::Invalid(_))),
                    "futures={futures} case={case:?} outcome={outcome:?}");
                assert!(paging.calls()>=1);
                assert_eq!(fs::read(root.join("state.json")).unwrap(),before);
                let _=fs::remove_dir_all(root);
            }
        }
    }

    #[test]
    fn r32d4_expired_or_invalid_history_window_denied_before_get(){
        use crate::native_history_audit::{audit_native_entry_paginated_history,HistoryAuditError};
        use crate::native_entry_recovery::probe_native_entry_readonly_once;
        use crate::private_rest::{OkxCredentials,BinanceCredentials,OkxDomain};
        let okx=OkxCredentials::new("FAKE_OKX","FAKE_SECRET","FAKE_PASS").unwrap();
        let bin=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        let (root,store,cid,generation)=r32d_entry_test_store(false);
        let account_fake=R32DReadOnlyFixture::new(&cid,false,R32DCase::Candidate);
        let probe=probe_native_entry_readonly_once(&account_fake,&store,generation,
            &okx,&bin,1_700_000_000_123,OkxDomain::Global,10_000).unwrap();
        let paging=R32D4HistoryMock::new(false,R32D4Case::Full);
        for asof in [1_779_999_999_999,1_780_000_000_125+71*24*60*60*1000]{
            let outcome=audit_native_entry_paginated_history(
                &paging,&store,&probe,&okx,&bin,asof,OkxDomain::Global,10_000,1);
            assert!(matches!(outcome,Err(HistoryAuditError::HistoryWindowUnverifiable)));
        }
        assert_eq!(paging.calls(),0);
        let _=fs::remove_dir_all(root);
    }

    #[test]
    fn r32d4_unbounded_history_limited_and_never_claims_exhaustion(){
        use crate::native_history_audit::{audit_native_entry_paginated_history,HistoryAuditError};
        use crate::native_entry_recovery::probe_native_entry_readonly_once;
        use crate::private_rest::{OkxCredentials,BinanceCredentials,OkxDomain};
        let okx=OkxCredentials::new("FAKE_OKX","FAKE_SECRET","FAKE_PASS").unwrap();
        let bn=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        for futures in [false,true]{
            let (root,store,cid,generation)=r32d_entry_test_store(futures);
            let original=fs::read(root.join("state.json")).unwrap();
            let account_fake=R32DReadOnlyFixture::new(&cid,futures,R32DCase::Candidate);
            let probe=probe_native_entry_readonly_once(&account_fake,&store,generation,
                &okx,&bn,1_700_000_000_123,OkxDomain::Global,10_000).unwrap();
            let history=R32D4HistoryMock::new(futures,R32D4Case::NoEnd);
            let outcome=audit_native_entry_paginated_history(
                &history,&store,&probe,&okx,&bn,1_780_000_010_125,
                OkxDomain::Global,10_000,1);
            assert!(matches!(outcome,Err(HistoryAuditError::PageLimitExceeded)),
                "futures={futures} outcome={outcome:?}");
            assert_eq!(history.calls(),16);
            assert_eq!(fs::read(root.join("state.json")).unwrap(),original);
            let _=fs::remove_dir_all(root);
        }
    }

    #[test]
    fn r32d4_concurrent_native_state_drift_invalidates_finished_pages(){
        use crate::native_history_audit::{audit_native_entry_paginated_history,HistoryAuditError};
        use crate::native_entry_recovery::probe_native_entry_readonly_once;
        use crate::private_rest::{OkxCredentials,BinanceCredentials,OkxDomain};
        struct DuringPages{
            inner:R32D4HistoryMock,store:NativeStateStore,
            generation:u64,once:std::sync::atomic::AtomicBool,
        }
        impl crate::private_https::HttpExecutor for DuringPages{
            fn execute_request(&self,request:&crate::private_https::HttpsRequest)
                ->Result<crate::private_https::HttpResponse,crate::private_https::TransportError>{
                use std::sync::atomic::Ordering;
                let response=self.inner.execute_request(request);
                if self.inner.calls()==2 && !self.once.swap(true,Ordering::SeqCst){
                    self.store.transact(self.generation,|_|Ok(())).unwrap();
                }
                response
            }
        }
        let okx=OkxCredentials::new("FAKE_OKX","FAKE_SECRET","FAKE_PASS").unwrap();
        let bin=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        for futures in [false,true]{
            let (root,store,cid,generation)=r32d_entry_test_store(futures);
            let account_fake=R32DReadOnlyFixture::new(&cid,futures,R32DCase::Candidate);
            let probe=probe_native_entry_readonly_once(&account_fake,&store,generation,
                &okx,&bin,1_700_000_000_123,OkxDomain::Global,10_000).unwrap();
            let pages=DuringPages{
                inner:R32D4HistoryMock::new(futures,R32D4Case::Full),
                store:store.clone(),generation,
                once:std::sync::atomic::AtomicBool::new(false),
            };
            let audited=audit_native_entry_paginated_history(
                &pages,&store,&probe,&okx,&bin,1_780_000_010_125,
                OkxDomain::Global,10_000,1);
            assert!(matches!(audited,Err(HistoryAuditError::State(NativeStateError::Generation))));
            assert_eq!(pages.inner.calls(),3);
            assert_eq!(store.load_for_recovery().unwrap().unwrap().entry.unwrap().phase,
                NativeEntryPhase::RecoveryRequired);
            assert!(store.prepare_futures_entry_hedge(generation+1,7,"unsafe",
                "sab00000000000000000000000000502").is_err());
            let _=fs::remove_dir_all(root);
        }
    }

    #[derive(Clone,Copy,Debug)]
    enum R32D5Case {
        Good,ForeignOrder,OffsettingForeignTransfer,StaleUpdate,
        BalanceChanged,BadBillBalance,BadFee,WrongTrade,
        OldBillTime,RepeatedCursor,TruncatedArchive,MissingUpdate,
    }
    struct R32D5CashMock {
        case:R32D5Case,calls:std::sync::atomic::AtomicUsize,
    }
    impl R32D5CashMock{
        fn new(case:R32D5Case)->Self{
            Self{case,calls:std::sync::atomic::AtomicUsize::new(0)}
        }
        fn calls(&self)->usize{
            self.calls.load(std::sync::atomic::Ordering::SeqCst)
        }
    }
    impl crate::private_https::HttpExecutor for R32D5CashMock{
        fn execute_request(&self,req:&crate::private_https::HttpsRequest)
            ->Result<crate::private_https::HttpResponse,crate::private_https::TransportError>{
            use crate::private_rest::HttpMethod;
            assert_eq!(req.method,HttpMethod::Get);
            assert!(!req.mutation,"R32D5 no mutations");
            let parsed=url::Url::parse(&req.url).unwrap();
            let q=parsed.query_pairs().collect::<std::collections::BTreeMap<_,_>>();
            let n=self.calls.fetch_add(1,std::sync::atomic::Ordering::SeqCst);
            let body=if parsed.path()=="/api/v5/account/balance"{
                assert!(n==0 || n==4);
                let cash=if n==4&&matches!(self.case,R32D5Case::BalanceChanged){"0.5247"}
                    else{"0.5246"};
                let utime=if matches!(self.case,R32D5Case::StaleUpdate){"1770000000000"}
                    else{"1780000004000"};
                if matches!(self.case,R32D5Case::MissingUpdate){
                    format!(r#"{{"code":"0","data":[{{"details":[{{"ccy":"BNT","cashBal":"{cash}"}}]}}]}}"#)
                }else{
                    format!(r#"{{"code":"0","data":[{{"details":[{{"ccy":"BNT","cashBal":"{cash}","uTime":"{utime}"}}]}}]}}"#)
                }
            }else if parsed.path()=="/api/v5/account/bills-archive"{
                assert!(n>=1 && n<=3);
                assert_eq!(q.get("ccy").map(|v|v.as_ref()),Some("BNT"));
                assert_eq!(q.get("begin").map(|v|v.as_ref()),Some("1780000000125"));
                assert_eq!(q.get("end").map(|v|v.as_ref()),Some("1780000010125"));
                assert_eq!(q.get("limit").map(|v|v.as_ref()),Some("1"));
                assert!(!q.contains_key("instType"),"transfer bills must not be filtered away");
                let after=q.get("after").map(|v|v.as_ref());
                if n==1{assert_eq!(after,None);}
                if n==2{assert_eq!(after,Some("200"));}
                if n==3{assert_eq!(after,Some("199"));}
                if n==3||n==2&&matches!(self.case,R32D5Case::TruncatedArchive){
                    r#"{"code":"0","data":[]}"#.into()
                }else{
                    let first=n==1;
                    let bill=if first||matches!(self.case,R32D5Case::RepeatedCursor){
                        "200"
                    }else{"199"};
                    let trade=if first{"1002"}else{"1001"};
                    let trade=if matches!(self.case,R32D5Case::WrongTrade){"999"}else{trade};
                    let ord=if matches!(self.case,R32D5Case::ForeignOrder
                        |R32D5Case::OffsettingForeignTransfer){"999"}else{"123"};
                    let cash=if first{"0.5246"}
                        else if matches!(self.case,R32D5Case::BadBillBalance){"0.3333"}
                        else{"0.3248"};
                    let change=if first&&matches!(self.case,R32D5Case::OffsettingForeignTransfer){
                        "-0.1"
                    }else{"0.1998"};
                    let fee=if first&&matches!(self.case,R32D5Case::BadFee){"-0.0003"}
                        else{"-0.0002"};
                    let ts=if first&&matches!(self.case,R32D5Case::OldBillTime){
                        "1770000000000"
                    }else if first{"1780000000320"}else{"1780000000220"};
                    format!(r#"{{"code":"0","data":[{{"ccy":"BNT","instId":"BNT-USDT","subType":"1","ordId":"{ord}","tradeId":"{trade}","billId":"{bill}","bal":"{cash}","balChg":"{change}","sz":"0.2","fee":"{fee}","ts":"{ts}"}}]}}"#)
                }
            }else{panic!("R32D5 unexpected GET path")}
            ;
            Ok(crate::private_https::HttpResponse{status:200,headers:vec![],body})
        }
    }

    fn r32d5_seed_recovery_checkpoint()->(PathBuf,NativeStateStore,u64){
        use crate::native_entry_recovery::{probe_native_entry_readonly_once,
            probe_native_entry_fills_readonly_once};
        use crate::private_rest::{OkxCredentials,BinanceCredentials,OkxDomain};
        let (root,store,cid,generation)=r32d_entry_test_store(false);
        let okx=OkxCredentials::new("FAKE_OKX","FAKE_SECRET","FAKE_PASS").unwrap();
        let bin=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        let fake=R32DReadOnlyFixture::new(&cid,false,R32DCase::LineageTwoFills);
        let probe=probe_native_entry_readonly_once(&fake,&store,generation,
            &okx,&bin,1_700_000_000_123,OkxDomain::Global,10_000).unwrap();
        let details=probe_native_entry_fills_readonly_once(&fake,&store,&probe,
            &okx,&bin,1_700_000_000_123,OkxDomain::Global,10_000).unwrap();
        assert_eq!(details.fill_count,2);
        assert_eq!(fake.count(),11);
        let (saved,kind)=store.record_native_entry_recovery_checkpoint(details).unwrap();
        assert_eq!(kind,NativeEntryRecoveryWrite::Recorded);
        (root,store,saved.generation)
    }

    #[test]
    fn r32d5_okx_all_bill_movement_chain_matches_exact_trades_readonly(){
        use crate::native_cash_lineage::audit_okx_cash_lineage_once;
        use crate::private_rest::{OkxCredentials,OkxDomain};
        let (root,store,generation)=r32d5_seed_recovery_checkpoint();
        let bytes=fs::read(root.join("state.json")).unwrap();
        let okx=OkxCredentials::new("FAKE_OKX","FAKE_SECRET","FAKE_PASS").unwrap();
        let mock=R32D5CashMock::new(R32D5Case::Good);
        let result=audit_okx_cash_lineage_once(
            &mock,&store,generation,&okx,1_780_000_010_125,OkxDomain::Global,1).unwrap();
        assert_eq!(mock.calls(),5);
        assert_eq!(result.account_bill_count,2);
        assert_eq!(result.pages,3);
        assert_eq!(result.observed_cash_e18,524_600_000_000_000_000);
        assert_eq!(result.accounted_trade_delta_e18,399_600_000_000_000_000);
        assert_eq!(result.aggregate_base_fee_e18,-400_000_000_000_000);
        assert!(result.base_currency_movement_chain_matched);
        assert!(!result.account_history_global_completeness_proven);
        assert!(!result.cross_venue_snapshot_atomicity_proven);
        assert!(!result.may_send_order);
        assert_eq!(fs::read(root.join("state.json")).unwrap(),bytes);
        assert_eq!(store.load_for_recovery().unwrap().unwrap().entry.unwrap().phase,
            NativeEntryPhase::RecoveryRequired);
        let _=fs::remove_dir_all(root);
    }

    #[test]
    fn r32d5_foreign_movements_stale_balance_fee_cursor_and_missing_bill_rejected(){
        use crate::native_cash_lineage::{audit_okx_cash_lineage_once,CashLineageError};
        use crate::private_rest::{OkxCredentials,OkxDomain};
        let okx=OkxCredentials::new("FAKE_OKX","FAKE_SECRET","FAKE_PASS").unwrap();
        for case in [
            R32D5Case::ForeignOrder,R32D5Case::OffsettingForeignTransfer,
            R32D5Case::StaleUpdate,R32D5Case::BalanceChanged,
            R32D5Case::BadBillBalance,R32D5Case::BadFee,R32D5Case::WrongTrade,
            R32D5Case::OldBillTime,R32D5Case::RepeatedCursor,
            R32D5Case::TruncatedArchive,R32D5Case::MissingUpdate,
        ]{
            let (root,store,generation)=r32d5_seed_recovery_checkpoint();
            let old=fs::read(root.join("state.json")).unwrap();
            let mock=R32D5CashMock::new(case);
            let res=audit_okx_cash_lineage_once(
                &mock,&store,generation,&okx,1_780_000_010_125,
                OkxDomain::Global,1);
            if matches!(case,R32D5Case::StaleUpdate){
                assert!(matches!(res,Err(CashLineageError::NotFresh)),
                    "case={case:?} result={res:?}");
            }else{
                assert!(matches!(res,Err(CashLineageError::Invalid(_))),
                    "case={case:?} result={res:?}");
            }
            assert!(mock.calls()>=1);
            assert_eq!(fs::read(root.join("state.json")).unwrap(),old);
            assert_eq!(store.load().unwrap().unwrap().entry.unwrap().phase,
                NativeEntryPhase::RecoveryRequired);
            let _=fs::remove_dir_all(root);
        }
    }

    #[test]
    fn r32d5_no_verified_spot_checkpoint_or_future_leg_yields_zero_gets(){
        use crate::native_cash_lineage::{audit_okx_cash_lineage_once,CashLineageError};
        use crate::private_rest::{OkxCredentials,OkxDomain};
        let okx=OkxCredentials::new("FAKE_OKX","FAKE_SECRET","FAKE_PASS").unwrap();
        for futures in [false,true]{
            let (root,store,_,generation)=r32d_entry_test_store(futures);
            let fake=R32D5CashMock::new(R32D5Case::Good);
            let res=audit_okx_cash_lineage_once(
                &fake,&store,generation,&okx,1_780_000_010_125,
                OkxDomain::Global,1);
            assert!(matches!(res,Err(CashLineageError::Invalid(_))));
            assert_eq!(fake.calls(),0);
            let stale=audit_okx_cash_lineage_once(
                &fake,&store,generation-1,&okx,1_780_000_010_125,
                OkxDomain::Global,1);
            assert!(matches!(stale,Err(CashLineageError::State(NativeStateError::Generation))));
            assert_eq!(fake.calls(),0);
            let _=fs::remove_dir_all(root);
        }
    }

    #[test]
    fn r32d5_concurrent_native_change_during_account_bill_scan_invalidates_result(){
        use crate::native_cash_lineage::{audit_okx_cash_lineage_once,CashLineageError};
        use crate::private_rest::{OkxCredentials,OkxDomain};
        use std::sync::atomic::{AtomicBool,Ordering};
        struct RaceLineage{
            inner:R32D5CashMock,
            store:NativeStateStore,generation:u64,once:AtomicBool,
        }
        impl crate::private_https::HttpExecutor for RaceLineage{
            fn execute_request(&self,req:&crate::private_https::HttpsRequest)
                ->Result<crate::private_https::HttpResponse,crate::private_https::TransportError>{
                let res=self.inner.execute_request(req);
                if self.inner.calls()==3&&!self.once.swap(true,Ordering::SeqCst){
                    self.store.transact(self.generation,|_|Ok(())).unwrap();
                }
                res
            }
        }
        let (root,store,generation)=r32d5_seed_recovery_checkpoint();
        let okx=OkxCredentials::new("FAKE_OKX","FAKE_SECRET","FAKE_PASS").unwrap();
        let mock=RaceLineage{
            inner:R32D5CashMock::new(R32D5Case::Good),
            store:store.clone(),generation,once:AtomicBool::new(false),
        };
        let res=audit_okx_cash_lineage_once(
            &mock,&store,generation,&okx,1_780_000_010_125,
            OkxDomain::Global,1);
        assert!(matches!(res,Err(CashLineageError::State(NativeStateError::Generation))));
        assert_eq!(mock.inner.calls(),5);
        let after=store.load().unwrap().unwrap();
        assert_eq!(after.generation,generation+1);
        assert_eq!(after.entry.unwrap().phase,NativeEntryPhase::RecoveryRequired);
        let _=fs::remove_dir_all(root);
    }

    #[derive(Clone,Copy,Debug)]
    enum R32D5BCase{
        Good,ForeignTransfer,FundingFee,WrongTradeId,WrongCommission,
        DuplicateIncome,MissingCommission,OtherFeeAsset,
        PositionChanged,WalletChanged,StalePosition,StaleWallet,
        HedgeMode,OpenOrders,WrongPosition,ExtraPosition,MissingIncomeTime,
        MultiAssetsMode,LateOpenOrders,LateMultiAssetsMode,
    }
    struct R32D5BMock{
        case:R32D5BCase,
        requests:std::sync::atomic::AtomicUsize,
        pos_reads:std::sync::atomic::AtomicUsize,
        wallet_reads:std::sync::atomic::AtomicUsize,
        income_reads:std::sync::atomic::AtomicUsize,
        config_reads:std::sync::atomic::AtomicUsize,
        open_reads:std::sync::atomic::AtomicUsize,
    }
    impl R32D5BMock{
        fn new(case:R32D5BCase)->Self{
            Self{case,requests:std::sync::atomic::AtomicUsize::new(0),
                pos_reads:std::sync::atomic::AtomicUsize::new(0),
                wallet_reads:std::sync::atomic::AtomicUsize::new(0),
                income_reads:std::sync::atomic::AtomicUsize::new(0),
                config_reads:std::sync::atomic::AtomicUsize::new(0),
                open_reads:std::sync::atomic::AtomicUsize::new(0)}
        }
        fn count(&self)->usize{
            self.requests.load(std::sync::atomic::Ordering::SeqCst)
        }
        fn income_count(&self)->usize{
            self.income_reads.load(std::sync::atomic::Ordering::SeqCst)
        }
    }
    impl crate::private_https::HttpExecutor for R32D5BMock{
        fn execute_request(&self,req:&crate::private_https::HttpsRequest)
            ->Result<crate::private_https::HttpResponse,crate::private_https::TransportError>{
            use crate::private_rest::HttpMethod;
            use std::sync::atomic::Ordering;
            assert_eq!(req.method,HttpMethod::Get,"R32D5B mock forbids mutation");
            assert!(!req.mutation);
            self.requests.fetch_add(1,Ordering::SeqCst);
            let parsed=url::Url::parse(&req.url).unwrap();
            let url=parsed.path();
            let q=parsed.query_pairs().collect::<std::collections::BTreeMap<_,_>>();
            let body=if url=="/fapi/v1/positionSide/dual"{
                if matches!(self.case,R32D5BCase::HedgeMode){
                    r#"{"dualSidePosition":true}"#.to_string()
                }else{r#"{"dualSidePosition":false}"#.to_string()}
            }else if url=="/fapi/v1/accountConfig"{
                let n=self.config_reads.fetch_add(1,Ordering::SeqCst);
                if matches!(self.case,R32D5BCase::MultiAssetsMode)
                    ||matches!(self.case,R32D5BCase::LateMultiAssetsMode)&&n==1{
                    r#"{"multiAssetsMargin":true}"#.to_string()
                }else{r#"{"multiAssetsMargin":false}"#.to_string()}
            }else if url=="/fapi/v1/openOrders"{
                let n=self.open_reads.fetch_add(1,Ordering::SeqCst);
                if matches!(self.case,R32D5BCase::OpenOrders)
                    ||matches!(self.case,R32D5BCase::LateOpenOrders)&&n==1{
                    r#"[{"symbol":"BNTUSDT","orderId":22,"clientOrderId":"other","side":"SELL","positionSide":"BOTH","status":"NEW","reduceOnly":false}]"#.to_string()
                }else{"[]".into()}
            }else if url=="/fapi/v3/positionRisk"{
                let n=self.pos_reads.fetch_add(1,Ordering::SeqCst);
                let amount=if matches!(self.case,R32D5BCase::WrongPosition)
                    ||matches!(self.case,R32D5BCase::PositionChanged)&&n==1{
                    "-0.7"
                }else{"-0.4"};
                let updated=if matches!(self.case,R32D5BCase::StalePosition){
                    "1770000000000"
                }else{"1780000000400"};
                if matches!(self.case,R32D5BCase::ExtraPosition){
                    format!(r#"[{{"symbol":"BNTUSDT","positionAmt":"{amount}","positionSide":"BOTH","marginAsset":"USDT","updateTime":{updated}}},{{"symbol":"ETHUSDT","positionAmt":"-0.1","positionSide":"BOTH"}}]"#)
                }else{
                    format!(r#"[{{"symbol":"BNTUSDT","positionAmt":"{amount}","positionSide":"BOTH","marginAsset":"USDT","updateTime":{updated}}}]"#)
                }
            }else if url=="/fapi/v3/balance"{
                let n=self.wallet_reads.fetch_add(1,Ordering::SeqCst);
                let amount=if matches!(self.case,R32D5BCase::WalletChanged)&&n==1{
                    "99.9"
                }else{"100"};
                let updated=if matches!(self.case,R32D5BCase::StaleWallet){
                    "1770000000000"
                }else{"1780000000600"};
                format!(r#"[{{"asset":"USDT","balance":"{amount}","availableBalance":"99","updateTime":{updated}}}]"#)
            }else if url=="/fapi/v1/income"{
                self.income_reads.fetch_add(1,Ordering::SeqCst);
                assert_eq!(q.get("limit").map(|s|s.as_ref()),Some("1"));
                assert_eq!(q.get("startTime").map(|s|s.as_ref()),Some("1780000000125"));
                assert_eq!(q.get("endTime").map(|s|s.as_ref()),Some("1780000010125"));
                assert!(!q.contains_key("symbol"),"all-symbol evidence required");
                assert!(!q.contains_key("incomeType"),"all income classes must be visible");
                let page=q.get("page").unwrap().parse::<u16>().unwrap();
                if page==3||page==2&&matches!(self.case,R32D5BCase::MissingCommission){
                    "[]".into()
                }else{
                    let type_name=if page==1&&matches!(self.case,R32D5BCase::FundingFee){
                        "FUNDING_FEE"
                    }else if page==1&&matches!(self.case,R32D5BCase::ForeignTransfer){
                        "TRANSFER"
                    }else{"COMMISSION"};
                    let trade=if page==1&&matches!(self.case,R32D5BCase::WrongTradeId){
                        "9999"
                    }else if page==2&&matches!(self.case,R32D5BCase::DuplicateIncome){
                        "1001"
                    }else if page==1{"1001"}else{"1002"};
                    let txn=if page==2&&matches!(self.case,R32D5BCase::DuplicateIncome){
                        "5001"
                    }else if page==1{"5001"}else{"5002"};
                    let fee=if page==1&&matches!(self.case,R32D5BCase::WrongCommission){
                        "-0.03"
                    }else{"-0.02"};
                    let asset=if page==1&&matches!(self.case,R32D5BCase::OtherFeeAsset){
                        "BNB"
                    }else{"USDT"};
                    let time=if page==1{"1780000000220"}else{"1780000000320"};
                    if page==1&&matches!(self.case,R32D5BCase::MissingIncomeTime){
                        format!(r#"[{{"symbol":"BNTUSDT","incomeType":"{type_name}","income":"{fee}","asset":"{asset}","tranId":{txn},"tradeId":"{trade}"}}]"#)
                    }else{
                        format!(r#"[{{"symbol":"BNTUSDT","incomeType":"{type_name}","income":"{fee}","asset":"{asset}","time":{time},"tranId":{txn},"tradeId":"{trade}"}}]"#)
                    }
                }
            }else{panic!("R32D5B unexpected GET route {url}")};
            Ok(crate::private_https::HttpResponse{status:200,headers:vec![],body})
        }
    }
    fn r32d5b_seed_futures_checkpoint()->(PathBuf,NativeStateStore,u64){
        use crate::native_entry_recovery::{
            probe_native_entry_readonly_once,probe_native_entry_fills_readonly_once};
        use crate::private_rest::{OkxCredentials,BinanceCredentials,OkxDomain};
        let (root,store,cid,generation)=r32d_entry_test_store(true);
        let okx=OkxCredentials::new("FAKE_OKX","FAKE_SECRET","FAKE_PASS").unwrap();
        let bn=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        let fake=R32DReadOnlyFixture::new(&cid,true,R32DCase::LineageTwoFills);
        let first=probe_native_entry_readonly_once(&fake,&store,generation,
            &okx,&bn,1_700_000_000_123,OkxDomain::Global,10_000).unwrap();
        let second=probe_native_entry_fills_readonly_once(&fake,&store,&first,
            &okx,&bn,1_700_000_000_123,OkxDomain::Global,10_000).unwrap();
        assert_eq!(second.fill_count,2);
        let (saved,write)=store.record_native_entry_recovery_checkpoint(second).unwrap();
        assert_eq!(write,NativeEntryRecoveryWrite::Recorded);
        assert_eq!(saved.entry.as_ref().unwrap().phase,NativeEntryPhase::RecoveryRequired);
        (root,store,saved.generation)
    }
    #[test]
    fn r32d5b_exact_income_commissions_and_signed_oneway_short_pass_only_diagnostic(){
        use crate::native_binance_lineage::audit_binance_futures_lineage_once;
        use crate::private_rest::BinanceCredentials;
        let (root,store,generation)=r32d5b_seed_futures_checkpoint();
        let original=fs::read(root.join("state.json")).unwrap();
        let bin=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        let fake=R32D5BMock::new(R32D5BCase::Good);
        let result=audit_binance_futures_lineage_once(
            &fake,&store,generation,&bin,1_780_000_010_125,10_000,1).unwrap();
        assert_eq!(fake.count(),13);
        assert_eq!(result.income_pages,3);
        assert_eq!(result.commission_rows,2);
        assert_eq!(result.total_commission_usdt_e12,40_000_000_000);
        assert_eq!(result.position_short_atoms_e18,400_000_000_000_000_000);
        assert_eq!(result.usdt_wallet_atoms_e18,100_000_000_000_000_000_000);
        assert!(result.income_window_exhaustion_seen);
        assert!(!result.prior_spot_fee_origin_proven);
        assert!(!result.all_account_activity_proven);
        assert!(!result.exchange_snapshots_atomicity_proven);
        assert!(!result.may_send_order);
        assert_eq!(fs::read(root.join("state.json")).unwrap(),original);
        assert!(store.prepare_futures_entry_hedge(generation,7,"blind",
            "sab00000000000000000000000000601").is_err());
        let _=fs::remove_dir_all(root);
    }
    #[test]
    fn r32d5b_transfer_funding_bad_fee_duplicate_missing_and_stale_fail_closed(){
        use crate::native_binance_lineage::{
            audit_binance_futures_lineage_once,BinanceLineageError};
        use crate::private_rest::BinanceCredentials;
        let bin=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        for case in [
            R32D5BCase::ForeignTransfer,R32D5BCase::FundingFee,
            R32D5BCase::WrongTradeId,R32D5BCase::WrongCommission,
            R32D5BCase::DuplicateIncome,R32D5BCase::MissingCommission,
            R32D5BCase::OtherFeeAsset,R32D5BCase::PositionChanged,
            R32D5BCase::WalletChanged,R32D5BCase::StalePosition,
            R32D5BCase::StaleWallet,R32D5BCase::HedgeMode,
            R32D5BCase::OpenOrders,R32D5BCase::WrongPosition,
            R32D5BCase::ExtraPosition,R32D5BCase::MissingIncomeTime,
            R32D5BCase::MultiAssetsMode,
            R32D5BCase::LateOpenOrders,R32D5BCase::LateMultiAssetsMode,
        ]{
            let (root,store,generation)=r32d5b_seed_futures_checkpoint();
            let prior=fs::read(root.join("state.json")).unwrap();
            let fake=R32D5BMock::new(case);
            let result=audit_binance_futures_lineage_once(
                &fake,&store,generation,&bin,1_780_000_010_125,10_000,1);
            if matches!(case,R32D5BCase::StalePosition|R32D5BCase::StaleWallet){
                assert!(matches!(result,Err(BinanceLineageError::NotFresh)),
                    "case={case:?} result={result:?}");
            }else{
                assert!(matches!(result,Err(BinanceLineageError::Invalid(_))),
                    "case={case:?} result={result:?}");
            }
            assert_eq!(fs::read(root.join("state.json")).unwrap(),prior);
            assert_eq!(store.load_for_recovery().unwrap().unwrap().entry.unwrap().phase,
                NativeEntryPhase::RecoveryRequired);
            let _=fs::remove_dir_all(root);
        }
    }
    #[test]
    fn r32d5b_missing_checkpoint_spot_leg_stale_state_no_http(){
        use crate::native_binance_lineage::{
            audit_binance_futures_lineage_once,BinanceLineageError};
        use crate::private_rest::BinanceCredentials;
        let bin=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        for futures in [false,true]{
            let (root,store,_,generation)=r32d_entry_test_store(futures);
            let fake=R32D5BMock::new(R32D5BCase::Good);
            let result=audit_binance_futures_lineage_once(
                &fake,&store,generation,&bin,1_780_000_010_125,10_000,1);
            assert!(matches!(result,Err(BinanceLineageError::Invalid(_))));
            assert_eq!(fake.count(),0);
            let stale=audit_binance_futures_lineage_once(
                &fake,&store,generation-1,&bin,1_780_000_010_125,10_000,1);
            assert!(matches!(stale,Err(BinanceLineageError::State(NativeStateError::Generation))));
            assert_eq!(fake.count(),0);
            let _=fs::remove_dir_all(root);
        }
    }
    #[test]
    fn r32d5b_native_generation_drift_mid_income_scan_fails_closed(){
        use crate::native_binance_lineage::{
            audit_binance_futures_lineage_once,BinanceLineageError};
        use crate::private_rest::BinanceCredentials;
        use std::sync::atomic::{AtomicBool,Ordering};
        struct RaceBinance{
            inner:R32D5BMock,store:NativeStateStore,
            generation:u64,once:AtomicBool,
        }
        impl crate::private_https::HttpExecutor for RaceBinance{
            fn execute_request(&self,req:&crate::private_https::HttpsRequest)
                ->Result<crate::private_https::HttpResponse,crate::private_https::TransportError>{
                let result=self.inner.execute_request(req);
                if self.inner.income_count()==2&&!self.once.swap(true,Ordering::SeqCst){
                    self.store.transact(self.generation,|_|Ok(())).unwrap();
                }
                result
            }
        }
        let (root,store,generation)=r32d5b_seed_futures_checkpoint();
        let bin=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        let fake=RaceBinance{
            inner:R32D5BMock::new(R32D5BCase::Good),
            store:store.clone(),generation,once:AtomicBool::new(false),
        };
        let result=audit_binance_futures_lineage_once(
            &fake,&store,generation,&bin,1_780_000_010_125,10_000,1);
        assert!(matches!(result,Err(BinanceLineageError::State(NativeStateError::Generation))));
        assert_eq!(fake.inner.count(),13);
        assert_eq!(store.load().unwrap().unwrap().generation,generation+1);
        assert_eq!(store.load_for_recovery().unwrap().unwrap().entry.unwrap().phase,
            NativeEntryPhase::RecoveryRequired);
        let _=fs::remove_dir_all(root);
    }

    #[test]
    fn r32d5b_invalid_age_page_and_signing_rejected_before_http(){
        use crate::native_binance_lineage::{
            audit_binance_futures_lineage_once,BinanceLineageError};
        use crate::private_rest::BinanceCredentials;
        let (root,store,generation)=r32d5b_seed_futures_checkpoint();
        let before=fs::read(root.join("state.json")).unwrap();
        let bin=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        let fake=R32D5BMock::new(R32D5BCase::Good);
        for (now,recv,limit) in [
            (1_780_000_000_124,10_000,1u16),
            (1_780_000_000_125+71*24*60*60*1000,10_000,1),
            (1_780_000_010_125,10_000,0),
            (1_780_000_010_125,10_000,1001),
            (1_780_000_010_125,60_001,1),
        ]{
            let result=audit_binance_futures_lineage_once(
                &fake,&store,generation,&bin,now,recv,limit);
            assert!(matches!(result,
                Err(BinanceLineageError::Invalid(_))|Err(BinanceLineageError::Build(_))),
                "case=({now},{recv},{limit}) result={result:?}");
        }
        assert_eq!(fake.count(),0);
        assert_eq!(fs::read(root.join("state.json")).unwrap(),before);
        let _=fs::remove_dir_all(root);
    }

    #[derive(Clone,Copy,Debug)]
    enum R32D6Case{
        Complete,ForeignRoundTrip,MissingFill,WrongFee,WrongSide,
        DuplicateTrade,FullPage,OutOfWindow,WrongSymbol,RealizedPnl,
    }
    struct R32D6SymbolTrades{
        case:R32D6Case,
        calls:std::sync::atomic::AtomicUsize,
    }
    impl R32D6SymbolTrades{
        fn new(case:R32D6Case)->Self{
            Self{case,calls:std::sync::atomic::AtomicUsize::new(0)}
        }
        fn count(&self)->usize{
            self.calls.load(std::sync::atomic::Ordering::SeqCst)
        }
    }
    impl crate::private_https::HttpExecutor for R32D6SymbolTrades{
        fn execute_request(&self,req:&crate::private_https::HttpsRequest)
            ->Result<crate::private_https::HttpResponse,crate::private_https::TransportError>{
            use crate::private_rest::HttpMethod;
            assert_eq!(req.method,HttpMethod::Get);
            assert!(!req.mutation);
            let parsed=url::Url::parse(&req.url).unwrap();
            assert_eq!(parsed.path(),"/fapi/v1/userTrades");
            let pairs=parsed.query_pairs().collect::<std::collections::BTreeMap<_,_>>();
            assert_eq!(pairs.get("symbol").map(|s|s.as_ref()),Some("BNTUSDT"));
            assert_eq!(pairs.get("startTime").map(|s|s.as_ref()),Some("1779999700125"));
            assert_eq!(pairs.get("endTime").map(|s|s.as_ref()),Some("1780000010125"));
            assert!(!pairs.contains_key("orderId"));
            assert!(!pairs.contains_key("fromId"));
            self.calls.fetch_add(1,std::sync::atomic::Ordering::SeqCst);
            let mut rows=vec![];
            for i in 0..2u64{
                if matches!(self.case,R32D6Case::MissingFill)&&i==1{continue}
                let fee=if i==0&&matches!(self.case,R32D6Case::WrongFee){"0.03"}else{"0.02"};
                let side=if i==0&&matches!(self.case,R32D6Case::WrongSide){"BUY"}else{"SELL"};
                let id=if i==1&&matches!(self.case,R32D6Case::DuplicateTrade){1001}else{1001+i};
                let symbol=if i==0&&matches!(self.case,R32D6Case::WrongSymbol){"ETHUSDT"}else{"BNTUSDT"};
                let t=if i==0&&matches!(self.case,R32D6Case::OutOfWindow){1770000000000u64}
                    else{1780000000220u64+i*100};
                let pnl=if i==0&&matches!(self.case,R32D6Case::RealizedPnl){"1"}else{"0"};
                rows.push(serde_json::json!({
                    "symbol":symbol,"id":id,"orderId":124,
                    "side":side,"positionSide":"BOTH","marginAsset":"USDT",
                    "buyer":false,"realizedPnl":pnl,
                    "price":"101","qty":"0.2","commission":fee,
                    "commissionAsset":"USDT","time":t,
                }));
            }
            if matches!(self.case,R32D6Case::ForeignRoundTrip){
                // Own target short remains -0.4. Foreign BUY and SELL
                // completely cancel each other but MUST cause denial.
                for (i,side) in ["BUY","SELL"].iter().enumerate(){
                    rows.push(serde_json::json!({
                        "symbol":"BNTUSDT","id":2001+i,"orderId":999,
                        "side":side,"positionSide":"BOTH","marginAsset":"USDT",
                        "buyer":i==0,"realizedPnl":"0",
                        "price":"101","qty":"0.1","commission":"0.01",
                        "commissionAsset":"USDT","time":1780000000400u64+i as u64*100,
                    }));
                }
            }
            Ok(crate::private_https::HttpResponse{
                status:200,headers:vec![],body:serde_json::to_string(&rows).unwrap(),
            })
        }
    }

    #[test]
    fn r32d6a_symbol_wide_history_matching_two_fills_still_cannot_release(){
        use crate::native_futures_trade_scope::audit_binance_symbol_wide_trade_scope;
        use crate::private_rest::BinanceCredentials;
        let (root,store,generation)=r32d5b_seed_futures_checkpoint();
        let before=fs::read(root.join("state.json")).unwrap();
        let bin=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        let fake=R32D6SymbolTrades::new(R32D6Case::Complete);
        let evidence=audit_binance_symbol_wide_trade_scope(
            &fake,&store,generation,&bin,1_780_000_010_125,10_000,10).unwrap();
        assert_eq!(fake.count(),1);
        assert_eq!(evidence.exact_order_fill_events,2);
        assert_eq!(evidence.windows_scanned,1);
        assert_eq!(evidence.sum_signed_position_change_atoms_e12,-400_000_000_000);
        assert_eq!(evidence.foreign_symbol_order_count,0);
        assert!(!evidence.global_trade_history_proven);
        assert!(!evidence.cross_venue_recovery_proven);
        assert!(!evidence.may_send_order);
        assert_eq!(fs::read(root.join("state.json")).unwrap(),before);
        assert_eq!(store.load_for_recovery().unwrap().unwrap().entry.unwrap().phase,
            NativeEntryPhase::RecoveryRequired);
        let _=fs::remove_dir_all(root);
    }

    #[test]
    fn r32d6a_foreign_zero_net_roundtrip_and_corrupted_history_fail_closed(){
        use crate::native_futures_trade_scope::{
            audit_binance_symbol_wide_trade_scope,FuturesTradeScopeError};
        use crate::private_rest::BinanceCredentials;
        let bin=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        for case in [
            R32D6Case::ForeignRoundTrip,R32D6Case::MissingFill,
            R32D6Case::WrongFee,R32D6Case::WrongSide,
            R32D6Case::DuplicateTrade,R32D6Case::OutOfWindow,
            R32D6Case::WrongSymbol,R32D6Case::RealizedPnl,
        ]{
            let (root,store,generation)=r32d5b_seed_futures_checkpoint();
            let before=fs::read(root.join("state.json")).unwrap();
            let fake=R32D6SymbolTrades::new(case);
            let res=audit_binance_symbol_wide_trade_scope(
                &fake,&store,generation,&bin,1_780_000_010_125,10_000,10);
            assert!(matches!(res,Err(FuturesTradeScopeError::Invalid(_))),
                "case={case:?} result={res:?}");
            assert_eq!(fake.count(),1);
            assert_eq!(fs::read(root.join("state.json")).unwrap(),before);
            let _=fs::remove_dir_all(root);
        }
    }

    #[test]
    fn r32d6a_exact_limit_page_blocks_possible_hidden_trade(){
        use crate::native_futures_trade_scope::{
            audit_binance_symbol_wide_trade_scope,FuturesTradeScopeError};
        use crate::private_rest::BinanceCredentials;
        let (root,store,generation)=r32d5b_seed_futures_checkpoint();
        let bin=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        let fake=R32D6SymbolTrades::new(R32D6Case::FullPage);
        let res=audit_binance_symbol_wide_trade_scope(
            &fake,&store,generation,&bin,1_780_000_010_125,10_000,2);
        assert!(matches!(res,Err(FuturesTradeScopeError::PotentialTruncation)));
        assert_eq!(fake.count(),1);
        let _=fs::remove_dir_all(root);
    }

    #[test]
    fn r32d6a_absent_checkpoint_or_invalid_time_rejects_before_any_get(){
        use crate::native_futures_trade_scope::{
            audit_binance_symbol_wide_trade_scope,FuturesTradeScopeError};
        use crate::private_rest::BinanceCredentials;
        let bin=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        let (root,store,_,generation)=r32d_entry_test_store(true);
        let fake=R32D6SymbolTrades::new(R32D6Case::Complete);
        let no_proof=audit_binance_symbol_wide_trade_scope(
            &fake,&store,generation,&bin,1_780_000_010_125,10_000,10);
        assert!(matches!(no_proof,Err(FuturesTradeScopeError::Invalid(_))));
        assert_eq!(fake.count(),0);
        let _=fs::remove_dir_all(root);
        let (root,store,generation)=r32d5b_seed_futures_checkpoint();
        for now in [1_780_000_000_124,1_780_000_000_125+71*24*60*60*1000]{
            let wrong=audit_binance_symbol_wide_trade_scope(
                &fake,&store,generation,&bin,now,10_000,10);
            assert!(matches!(wrong,Err(FuturesTradeScopeError::Invalid(_))));
        }
        assert_eq!(fake.count(),0);
        let _=fs::remove_dir_all(root);
    }

    #[test]
    fn r32d6a_local_generation_drift_during_symbol_get_fails_closed(){
        use crate::native_futures_trade_scope::{
            audit_binance_symbol_wide_trade_scope,FuturesTradeScopeError};
        use crate::private_rest::BinanceCredentials;
        use std::sync::atomic::{AtomicBool,Ordering};
        struct MutatingHistory{
            inner:R32D6SymbolTrades,store:NativeStateStore,
            generation:u64,once:AtomicBool,
        }
        impl crate::private_https::HttpExecutor for MutatingHistory{
            fn execute_request(&self,req:&crate::private_https::HttpsRequest)
                ->Result<crate::private_https::HttpResponse,crate::private_https::TransportError>{
                let resp=self.inner.execute_request(req);
                if !self.once.swap(true,Ordering::SeqCst){
                    self.store.transact(self.generation,|_|Ok(())).unwrap();
                }
                resp
            }
        }
        let (root,store,generation)=r32d5b_seed_futures_checkpoint();
        let bin=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        let mock=MutatingHistory{
            inner:R32D6SymbolTrades::new(R32D6Case::Complete),
            store:store.clone(),generation,once:AtomicBool::new(false),
        };
        let result=audit_binance_symbol_wide_trade_scope(
            &mock,&store,generation,&bin,1_780_000_010_125,10_000,10);
        assert!(matches!(result,
            Err(FuturesTradeScopeError::State(NativeStateError::Generation))));
        assert_eq!(mock.inner.count(),1);
        assert_eq!(store.load().unwrap().unwrap().generation,generation+1);
        assert_eq!(store.load_for_recovery().unwrap().unwrap().entry.unwrap().phase,
            NativeEntryPhase::RecoveryRequired);
        let _=fs::remove_dir_all(root);
    }

    #[test]
    fn r32d6a_two_time_windows_do_not_skip_or_duplicate_fills(){
        use crate::native_futures_trade_scope::audit_binance_symbol_wide_trade_scope;
        use crate::private_rest::BinanceCredentials;
        use std::sync::atomic::{AtomicUsize,Ordering};
        struct TwoWindows{
            calls:AtomicUsize,
        }
        impl crate::private_https::HttpExecutor for TwoWindows{
            fn execute_request(&self,req:&crate::private_https::HttpsRequest)
                ->Result<crate::private_https::HttpResponse,crate::private_https::TransportError>{
                let url=url::Url::parse(&req.url).unwrap();
                assert_eq!(url.path(),"/fapi/v1/userTrades");
                assert_eq!(req.method,crate::private_rest::HttpMethod::Get);
                assert!(!req.mutation);
                let q=url.query_pairs().collect::<std::collections::BTreeMap<_,_>>();
                assert!(!q.contains_key("orderId"));
                let call=self.calls.fetch_add(1,Ordering::SeqCst);
                let first_start=1_779_999_700_125u64;
                let first_end=first_start+7*24*60*60*1000-1;
                let start=q.get("startTime").unwrap().parse::<u64>().unwrap();
                let end=q.get("endTime").unwrap().parse::<u64>().unwrap();
                if call==0{
                    assert_eq!(start,first_start);
                    assert_eq!(end,first_end);
                }else{
                    assert_eq!(call,1);
                    assert_eq!(start,first_end+1);
                    assert_eq!(end,1_780_000_000_125+8*24*60*60*1000);
                }
                let rows=if call==0{
                    serde_json::json!([
                        {"symbol":"BNTUSDT","id":1001,"orderId":124,
                         "side":"SELL","positionSide":"BOTH","marginAsset":"USDT",
                         "buyer":false,"realizedPnl":"0","price":"101","qty":"0.2",
                         "commission":"0.02","commissionAsset":"USDT","time":1780000000220u64},
                        {"symbol":"BNTUSDT","id":1002,"orderId":124,
                         "side":"SELL","positionSide":"BOTH","marginAsset":"USDT",
                         "buyer":false,"realizedPnl":"0","price":"101","qty":"0.2",
                         "commission":"0.02","commissionAsset":"USDT","time":1780000000320u64}
                    ])
                }else{serde_json::json!([])};
                Ok(crate::private_https::HttpResponse{
                    status:200,headers:vec![],body:rows.to_string(),
                })
            }
        }
        let (root,store,generation)=r32d5b_seed_futures_checkpoint();
        let bin=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        let mock=TwoWindows{calls:AtomicUsize::new(0)};
        let report=audit_binance_symbol_wide_trade_scope(
            &mock,&store,generation,&bin,
            1_780_000_000_125+8*24*60*60*1000,10_000,10).unwrap();
        assert_eq!(mock.calls.load(Ordering::SeqCst),2);
        assert_eq!(report.windows_scanned,2);
        assert_eq!(report.matching_trade_ids,2);
        assert!(!report.global_trade_history_proven);
        assert!(!report.cross_venue_recovery_proven);
        assert!(!report.may_send_order);
        let _=fs::remove_dir_all(root);
    }

    fn r32d6b_complete_two_leg_native_fixture()->(
        PathBuf,NativeStateStore,u64
    ){
        use crate::fill_economics::{FillEvent,DEFAULT_AVG_TOLERANCE_ATOMS};
        let (root,store,generation)=r32d5b_seed_futures_checkpoint();
        let (prepared,_)=store.transact(generation,|state|{
            let e=state.entry.as_mut()
                .ok_or(NativeStateError::Invalid("missing entry"))?;
            let prior=e.economics.entry.spot.order_mut("r32d-spot")
                .map_err(|_|NativeStateError::Economics)?;
            prior.exchange_order_id=Some("789".into());
            prior.apply_fill(FillEvent{
                fill_id:"prior-spot-fill-789".into(),
                price_atoms:decimal_to_atoms("100").unwrap(),
                qty_atoms:decimal_to_atoms("1").unwrap(),
                fee_cost_quote_atoms:Some(decimal_to_atoms("0.1").unwrap() as i128),
                fee_currency:Some("USDT".into()),
                trade_time_ms:Some(1_780_000_000_220),
            },DEFAULT_AVG_TOLERANCE_ATOMS).map_err(|_|NativeStateError::Economics)?;
            assert!(prior.fee_evidence_complete);
            Ok(())
        }).unwrap();
        assert_eq!(prepared.generation,generation+1);
        (root,store,prepared.generation)
    }

    #[test]
    fn r32d6b_native_two_leg_checkpoint_atomic_reopen_and_idempotent(){
        let (root,store,generation)=r32d6b_complete_two_leg_native_fixture();
        let before=store.load().unwrap().unwrap();
        assert!(before.entry.as_ref().unwrap().dual_leg_evidence.is_none());
        let (committed,result)=store.record_native_dual_leg_evidence(generation).unwrap();
        assert_eq!(result,NativeDualLegWrite::Recorded);
        assert_eq!(committed.generation,generation+1);
        let entry=committed.entry.as_ref().unwrap();
        assert_eq!(entry.phase,NativeEntryPhase::RecoveryRequired);
        assert_eq!(entry.spot_filled_atoms_e18,1_000_000_000_000_000_000);
        assert_eq!(entry.futures_filled_atoms_e18,0);
        assert_eq!(entry.intents.last().unwrap().state,IntentState::Unknown);
        let proof=entry.dual_leg_evidence.as_ref().unwrap();
        assert_eq!(proof.native_generation_at_capture,generation);
        assert_eq!(proof.safety_generation,7);
        assert_eq!(proof.settled_spot_intent_id,"r32d-spot");
        assert_eq!(proof.settled_spot_order_id,"789");
        assert_eq!(proof.settled_spot_ledger.fills.len(),1);
        assert_eq!(proof.settled_spot_ledger.fee_cost_quote_atoms,
            decimal_to_atoms("0.1").unwrap() as i128);
        assert_eq!(proof.pending_futures_checkpoint.order_id,"124");
        assert_eq!(proof.pending_futures_checkpoint.ledger.fills.len(),2);
        assert_eq!(proof.pending_futures_checkpoint.ledger.fee_cost_quote_atoms,
            decimal_to_atoms("0.04").unwrap() as i128);
        assert_eq!(proof.pending_futures_checkpoint.quality,
            NativeEntryRecoveryQuality::FuturesPriorSpotUnverified);
        assert!(!proof.independent_spot_exchange_history_verified);
        assert!(!proof.combined_exchange_snapshot_verified);
        assert!(!proof.may_send_order);
        let after=fs::read(root.join("state.json")).unwrap();
        let reopened=NativeStateStore::open(root.join("state.json")).unwrap();
        assert_eq!(reopened.load().unwrap().unwrap(),committed);
        let (stable,replay)=reopened.record_native_dual_leg_evidence(committed.generation).unwrap();
        assert_eq!(replay,NativeDualLegWrite::Idempotent);
        assert_eq!(stable,committed);
        assert_eq!(fs::read(root.join("state.json")).unwrap(),after);
        assert!(reopened.prepare_futures_entry_hedge(committed.generation,7,
            "forbidden","sab00000000000000000000000000701").is_err());
        assert!(reopened.record_native_dual_leg_evidence(generation)
            .is_err(),"stale CAS may not replay");
        let _=fs::remove_dir_all(root);
    }

    #[test]
    fn r32d6b_incomplete_settled_spot_fills_or_spot_pending_rejected(){
        for futures in [false,true]{
            let (root,store,generation)=if futures{
                r32d5b_seed_futures_checkpoint()
            }else{
                // R32D3 on unresolved spot has no settled prior leg.
                let (root,store,_,g)=r32d_entry_test_store(false);
                (root,store,g)
            };
            let before=fs::read(root.join("state.json")).unwrap();
            assert!(matches!(
                store.record_native_dual_leg_evidence(generation),
                Err(NativeStateError::Invalid(_))
            ));
            assert_eq!(fs::read(root.join("state.json")).unwrap(),before);
            assert_eq!(store.load_for_recovery().unwrap().unwrap().entry.unwrap().phase,
                NativeEntryPhase::RecoveryRequired);
            let _=fs::remove_dir_all(root);
        }
    }

    #[test]
    fn r32d6b_missing_futures_checkpoint_rejected_and_stale_generation(){
        let (root,store,_,generation)=r32d_entry_test_store(true);
        let raw=fs::read(root.join("state.json")).unwrap();
        assert!(matches!(store.record_native_dual_leg_evidence(generation),
            Err(NativeStateError::Invalid(_))));
        assert!(matches!(store.record_native_dual_leg_evidence(generation-1),
            Err(NativeStateError::Generation)));
        assert_eq!(fs::read(root.join("state.json")).unwrap(),raw);
        let _=fs::remove_dir_all(root);
    }

    #[test]
    fn r32d6b_durable_write_failure_preserves_old_state_then_one_exact_retry(){
        let (root,store,generation)=r32d6b_complete_two_leg_native_fixture();
        let bytes=fs::read(root.join("state.json")).unwrap();
        let tmp=root.join("state.tmp");
        fs::create_dir(&tmp).unwrap();
        let rejected=store.record_native_dual_leg_evidence(generation);
        assert!(matches!(rejected,Err(NativeStateError::Io(_))));
        assert_eq!(fs::read(root.join("state.json")).unwrap(),bytes);
        assert!(store.load().unwrap().unwrap().entry.unwrap().dual_leg_evidence.is_none());
        fs::remove_dir(&tmp).unwrap();
        let (saved,r)=store.record_native_dual_leg_evidence(generation).unwrap();
        assert_eq!(r,NativeDualLegWrite::Recorded);
        assert_eq!(saved.generation,generation+1);
        assert!(saved.entry.unwrap().dual_leg_evidence.is_some());
        let _=fs::remove_dir_all(root);
    }

    #[test]
    fn r32d6b_tampered_dual_snapshot_and_fake_release_flags_rejected(){
        let (root,store,generation)=r32d6b_complete_two_leg_native_fixture();
        let (recorded,_)=store.record_native_dual_leg_evidence(generation).unwrap();
        for i in 0..5{
            let mut fake=recorded.clone();
            let dual=fake.entry.as_mut().unwrap().dual_leg_evidence.as_mut().unwrap();
            match i{
                0=>dual.may_send_order=true,
                1=>dual.independent_spot_exchange_history_verified=true,
                2=>dual.combined_exchange_snapshot_verified=true,
                3=>dual.settled_spot_order_id="FORGED".into(),
                4=>dual.native_generation_at_capture=fake.generation,
                _=>unreachable!(),
            };
            assert!(fake.validate().is_err(),"tampered case {i} accepted");
            assert!(store.replace(recorded.generation,&fake).is_err());
        }
        assert_eq!(store.load().unwrap().unwrap(),recorded);
        let _=fs::remove_dir_all(root);
    }

    #[test]
    fn r32d6b_concurrent_writers_one_generation_one_commit(){
        use std::thread;
        let (root,store,generation)=r32d6b_complete_two_leg_native_fixture();
        let first=store.clone();
        let second=store.clone();
        let a=thread::spawn(move||first.record_native_dual_leg_evidence(generation));
        let b=thread::spawn(move||second.record_native_dual_leg_evidence(generation));
        let outcomes=[a.join().unwrap(),b.join().unwrap()];
        let committed=outcomes.iter().filter(|r|
            matches!(r,Ok((_,NativeDualLegWrite::Recorded)))).count();
        let stale=outcomes.iter().filter(|r|
            matches!(r,Err(NativeStateError::Generation))).count();
        assert_eq!(committed,1);
        assert_eq!(stale,1);
        let after=store.load().unwrap().unwrap();
        assert_eq!(after.generation,generation+1);
        assert!(after.entry.as_ref().unwrap().dual_leg_evidence.is_some());
        assert_eq!(after.entry.unwrap().phase,NativeEntryPhase::RecoveryRequired);
        let _=fs::remove_dir_all(root);
    }

    #[test]
    fn r32d6b_child_exit_before_or_after_durable_dual_fsync(){
        let Ok(path)=std::env::var("R32D6B_CHILD_ROOT")else{return};
        let stage=std::env::var("R32D6B_CHILD_STAGE").unwrap();
        let (root,_store,generation)=r32d6b_complete_two_leg_native_fixture();
        // The existing state is moved atomically in-process to the specific
        // parent-visible test path without touching unrelated directories.
        let dest=PathBuf::from(path);
        fs::create_dir_all(&dest).unwrap();
        let raw=fs::read(root.join("state.json")).unwrap();
        fs::write(dest.join("state.json"),raw).unwrap();
        File::open(dest.as_path()).unwrap().sync_all().unwrap();
        let stage_store=NativeStateStore::open(dest.join("state.json")).unwrap();
        if stage=="after"{
            let (saved,wrote)=stage_store.record_native_dual_leg_evidence(generation).unwrap();
            assert_eq!(wrote,NativeDualLegWrite::Recorded);
            assert_eq!(saved.generation,generation+1);
        }else{assert_eq!(stage,"before");}
        std::process::exit(74);
    }

    #[test]
    fn r32d6b_real_child_exit_before_after_dual_checkpoint_reopen_safe(){
        for stage in ["before","after"]{
            let root=root(&format!("r32d6b-child-{stage}"));
            fs::create_dir_all(&root).unwrap();
            let status=std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("authoritative_state::tests::r32d6b_child_exit_before_or_after_durable_dual_fsync")
                .env("R32D6B_CHILD_ROOT",&root)
                .env("R32D6B_CHILD_STAGE",stage)
                .status().unwrap();
            assert_eq!(status.code(),Some(74));
            let store=NativeStateStore::open(root.join("state.json")).unwrap();
            let persisted=store.load().unwrap().unwrap();
            let has=persisted.entry.as_ref().unwrap().dual_leg_evidence.is_some();
            assert_eq!(has,stage=="after");
            assert_eq!(persisted.entry.as_ref().unwrap().phase,
                NativeEntryPhase::RecoveryRequired);
            assert_eq!(persisted.entry.as_ref().unwrap().futures_filled_atoms_e18,0);
            let bytes=fs::read(root.join("state.json")).unwrap();
            let (final_state,write)=store.record_native_dual_leg_evidence(persisted.generation).unwrap();
            if stage=="after"{
                assert_eq!(write,NativeDualLegWrite::Idempotent);
                assert_eq!(fs::read(root.join("state.json")).unwrap(),bytes);
            }else{assert_eq!(write,NativeDualLegWrite::Recorded);}
            assert_eq!(final_state.entry.unwrap().phase,NativeEntryPhase::RecoveryRequired);
            let _=fs::remove_dir_all(root);
        }
    }

    #[derive(Clone,Copy,Debug)]
    enum R32D6B2Case {
        Good,WrongTradeId,ForeignBill,MissingBill,RepeatBillId,
        FillFeeChanged,WrongBillFee,BadBillBalance,CashChanged,
        StaleCash,WrongBillTime,ExtraFill,NoFills,CashUpdateDrift,
    }
    struct R32D6B2Mock {
        case:R32D6B2Case,
        calls:std::sync::atomic::AtomicUsize,
        cash_reads:std::sync::atomic::AtomicUsize,
    }
    impl R32D6B2Mock {
        fn new(case:R32D6B2Case)->Self{
            Self{case,calls:std::sync::atomic::AtomicUsize::new(0),
                cash_reads:std::sync::atomic::AtomicUsize::new(0)}
        }
        fn count(&self)->usize{
            self.calls.load(std::sync::atomic::Ordering::SeqCst)
        }
    }
    impl crate::private_https::HttpExecutor for R32D6B2Mock {
        fn execute_request(&self,req:&crate::private_https::HttpsRequest)
            ->Result<crate::private_https::HttpResponse,crate::private_https::TransportError>{
            use crate::private_rest::HttpMethod;
            use std::sync::atomic::Ordering;
            assert_eq!(req.method,HttpMethod::Get);
            assert!(!req.mutation);
            let url=url::Url::parse(&req.url).unwrap();
            let qs=url.query_pairs().collect::<std::collections::BTreeMap<_,_>>();
            self.calls.fetch_add(1,Ordering::SeqCst);
            let body=if url.path()=="/api/v5/account/balance"{
                let n=self.cash_reads.fetch_add(1,Ordering::SeqCst);
                let cash=if matches!(self.case,R32D6B2Case::CashChanged)&&n==1{
                    "1.225"
                }else{"1.125"};
                let updated=if matches!(self.case,R32D6B2Case::StaleCash){
                    "1770000000000"
                }else if matches!(self.case,R32D6B2Case::CashUpdateDrift)&&n==1{
                    "1780000004000"
                }else{"1780000002000"};
                format!(r#"{{"code":"0","data":[{{"details":[{{"ccy":"BNT","cashBal":"{cash}","uTime":"{updated}"}}]}}]}}"#)
            }else if url.path()=="/api/v5/trade/fills-history"{
                assert_eq!(qs.get("ordId").map(|s|s.as_ref()),Some("789"));
                assert_eq!(qs.get("instId").map(|s|s.as_ref()),Some("BNT-USDT"));
                assert_eq!(qs.get("limit").map(|s|s.as_ref()),Some("1"));
                let cursor=qs.get("after").map(|s|s.as_ref());
                if matches!(self.case,R32D6B2Case::NoFills) && cursor.is_none(){
                    r#"{"code":"0","data":[]}"#.into()
                }else if cursor==Some("200")
                    && !matches!(self.case,R32D6B2Case::ExtraFill){
                    r#"{"code":"0","data":[]}"#.into()
                }else{
                    let (bill,trade)=if cursor.is_some(){
                        ("199","foreign-spot-trade")
                    }else{
                        ("200",if matches!(self.case,R32D6B2Case::WrongTradeId){
                            "FORGED"
                        }else{"prior-spot-fill-789"})
                    };
                    let fee=if matches!(self.case,R32D6B2Case::FillFeeChanged){
                        "-0.2"
                    }else{"-0.1"};
                    format!(r#"{{"code":"0","data":[{{"ordId":"789","billId":"{bill}","tradeId":"{trade}","fillPx":"100","fillSz":"1","fee":"{fee}","feeCcy":"USDT","fillTime":"1780000000220"}}]}}"#)
                }
            }else if url.path()=="/api/v5/account/bills-archive"{
                assert_eq!(qs.get("ccy").map(|s|s.as_ref()),Some("BNT"));
                assert_eq!(qs.get("begin").map(|s|s.as_ref()),Some("1780000000125"));
                assert_eq!(qs.get("end").map(|s|s.as_ref()),Some("1780000010125"));
                assert_eq!(qs.get("limit").map(|s|s.as_ref()),Some("1"));
                assert!(!qs.contains_key("instType"));
                let cursor=qs.get("after").map(|s|s.as_ref());
                if matches!(self.case,R32D6B2Case::MissingBill)
                    ||(cursor==Some("200")&&!matches!(self.case,R32D6B2Case::RepeatBillId)){
                    r#"{"code":"0","data":[]}"#.into()
                }else{
                    let ord=if matches!(self.case,R32D6B2Case::ForeignBill){
                        "999"
                    }else{"789"};
                    let cash=if matches!(self.case,R32D6B2Case::BadBillBalance){
                        "1.225"
                    }else{"1.125"};
                    let fee=if matches!(self.case,R32D6B2Case::WrongBillFee){
                        "-0.2"
                    }else{"-0.1"};
                    let ts=if matches!(self.case,R32D6B2Case::WrongBillTime){
                        "1780000009999"
                    }else{"1780000000250"};
                    format!(r#"{{"code":"0","data":[{{"ccy":"BNT","ordId":"{ord}","instId":"BNT-USDT","subType":"1","tradeId":"prior-spot-fill-789","billId":"200","bal":"{cash}","balChg":"1","sz":"1","fee":"{fee}","feeCcy":"USDT","ts":"{ts}"}}]}}"#)
                }
            }else{panic!("R32D6B2 unexpected GET {}",url.path())};
            Ok(crate::private_https::HttpResponse{status:200,headers:vec![],body})
        }
    }
    fn r32d6b2_dual_state()->(PathBuf,NativeStateStore,u64){
        let (root,store,generation)=r32d6b_complete_two_leg_native_fixture();
        let (dual,write)=store.record_native_dual_leg_evidence(generation).unwrap();
        assert_eq!(write,NativeDualLegWrite::Recorded);
        (root,store,dual.generation)
    }
    #[test]
    fn r32d6b2_exact_okx_order_fills_bills_cash_immutable_persist(){
        use crate::native_settled_spot_audit::audit_prior_settled_spot_window_once;
        use crate::private_rest::{OkxCredentials,OkxDomain};
        let okx=OkxCredentials::new("FAKE_OKX","FAKE_SECRET","FAKE_PASS").unwrap();
        let (root,store,generation)=r32d6b2_dual_state();
        let raw=fs::read(root.join("state.json")).unwrap();
        let exchange=R32D6B2Mock::new(R32D6B2Case::Good);
        let report=audit_prior_settled_spot_window_once(
            &exchange,&store,generation,&okx,1_780_000_010_125,
            OkxDomain::Global,1).unwrap();
        assert_eq!(exchange.count(),6);
        assert_eq!(report.fill_rows,1);
        assert_eq!(report.bill_rows,1);
        assert_eq!(report.fills_pages,2);
        assert_eq!(report.bills_pages,2);
        assert_eq!(report.cash_e18,1_125_000_000_000_000_000);
        assert!(report.candidate_window_matched);
        assert!(!report.exchange_global_completeness_proven);
        assert!(!report.cross_venue_atomicity_proven);
        assert!(!report.may_send_order);
        assert_eq!(fs::read(root.join("state.json")).unwrap(),raw);
        let (saved,kind)=store.record_settled_spot_window_evidence(report.clone()).unwrap();
        assert_eq!(kind,NativeSpotAuditWrite::Recorded);
        assert_eq!(saved.generation,generation+1);
        let entry=saved.entry.as_ref().unwrap();
        let proof=entry.settled_spot_window_evidence.as_ref().unwrap();
        assert_eq!(proof.spot_order_id,"789");
        assert_eq!(proof.spot_ledger.fills.len(),1);
        assert_eq!(proof.checked_bill_ids,vec!["200".to_string()]);
        assert_eq!(proof.signed_base_fee_atoms_e18,0);
        assert_eq!(proof.original_dual_capture_generation,
            entry.dual_leg_evidence.as_ref().unwrap().native_generation_at_capture);
        assert!(!proof.exchange_global_completeness_proven);
        assert!(!proof.cross_venue_atomicity_proven);
        assert!(!proof.may_send_order);
        assert_eq!(entry.phase,NativeEntryPhase::RecoveryRequired);
        let bytes=fs::read(root.join("state.json")).unwrap();
        let reopened=NativeStateStore::open(root.join("state.json")).unwrap();
        assert_eq!(reopened.load().unwrap().unwrap(),saved);
        let (again,replayed)=reopened.record_settled_spot_window_evidence(report).unwrap();
        assert_eq!(replayed,NativeSpotAuditWrite::Idempotent);
        assert_eq!(again,saved);
        assert_eq!(fs::read(root.join("state.json")).unwrap(),bytes);
        assert!(reopened.prepare_futures_entry_hedge(saved.generation,7,"forbidden",
            "sab00000000000000000000000000711").is_err());
        let _=fs::remove_dir_all(root);
    }
    #[test]
    fn r32d6b2_foreign_bill_missing_trade_fee_time_balance_fail_closed(){
        use crate::native_settled_spot_audit::{
            audit_prior_settled_spot_window_once,SettledSpotAuditError};
        use crate::private_rest::{OkxCredentials,OkxDomain};
        let okx=OkxCredentials::new("FAKE_OKX","FAKE_SECRET","FAKE_PASS").unwrap();
        for case in [
            R32D6B2Case::WrongTradeId,R32D6B2Case::ForeignBill,
            R32D6B2Case::MissingBill,R32D6B2Case::RepeatBillId,
            R32D6B2Case::FillFeeChanged,R32D6B2Case::WrongBillFee,
            R32D6B2Case::BadBillBalance,R32D6B2Case::CashChanged,
            R32D6B2Case::StaleCash,R32D6B2Case::WrongBillTime,
            R32D6B2Case::ExtraFill,R32D6B2Case::NoFills,
            R32D6B2Case::CashUpdateDrift,
        ]{
            let (root,store,generation)=r32d6b2_dual_state();
            let raw=fs::read(root.join("state.json")).unwrap();
            let exchange=R32D6B2Mock::new(case);
            let res=audit_prior_settled_spot_window_once(
                &exchange,&store,generation,&okx,1_780_000_010_125,
                OkxDomain::Global,1);
            assert!(matches!(res,Err(SettledSpotAuditError::Invalid(_))
                |Err(SettledSpotAuditError::NotFresh)
                |Err(SettledSpotAuditError::HistoryNotExhausted)),
                "case={case:?} result={res:?}");
            assert!(exchange.count()>=1);
            assert_eq!(fs::read(root.join("state.json")).unwrap(),raw);
            assert_eq!(store.load_for_recovery().unwrap().unwrap().entry.unwrap().phase,
                NativeEntryPhase::RecoveryRequired);
            let _=fs::remove_dir_all(root);
        }
    }
    #[test]
    fn r32d6b2_absent_dual_or_stale_generation_no_okx_get(){
        use crate::native_settled_spot_audit::{
            audit_prior_settled_spot_window_once,SettledSpotAuditError};
        use crate::private_rest::{OkxCredentials,OkxDomain};
        let okx=OkxCredentials::new("FAKE_OKX","FAKE_SECRET","FAKE_PASS").unwrap();
        let (root,store,generation)=r32d5b_seed_futures_checkpoint();
        let exchange=R32D6B2Mock::new(R32D6B2Case::Good);
        let res=audit_prior_settled_spot_window_once(
            &exchange,&store,generation,&okx,1_780_000_010_125,
            OkxDomain::Global,1);
        assert!(matches!(res,Err(SettledSpotAuditError::Invalid(_))));
        assert_eq!(exchange.count(),0);
        let stale=audit_prior_settled_spot_window_once(
            &exchange,&store,generation-1,&okx,1_780_000_010_125,
            OkxDomain::Global,1);
        assert!(matches!(stale,Err(SettledSpotAuditError::State(NativeStateError::Generation))));
        assert_eq!(exchange.count(),0);
        let _=fs::remove_dir_all(root);
    }

    fn r32d6b2_report(
        store:&NativeStateStore,generation:u64,
    )->crate::native_settled_spot_audit::SettledSpotAuditReport{
        use crate::native_settled_spot_audit::audit_prior_settled_spot_window_once;
        use crate::private_rest::{OkxCredentials,OkxDomain};
        let okx=OkxCredentials::new("FAKE_OKX","FAKE_SECRET","FAKE_PASS").unwrap();
        audit_prior_settled_spot_window_once(
            &R32D6B2Mock::new(R32D6B2Case::Good),
            store,generation,&okx,1_780_000_010_125,OkxDomain::Global,1,
        ).unwrap()
    }

    #[test]
    fn r32d6b2_immutable_spot_window_concurrent_cas_and_exact_replay(){
        use std::thread;
        let (root,store,generation)=r32d6b2_dual_state();
        let report=r32d6b2_report(&store,generation);
        let first=store.clone();
        let second=store.clone();
        let copy=report.clone();
        let a=thread::spawn(move||first.record_settled_spot_window_evidence(report));
        let b=thread::spawn(move||second.record_settled_spot_window_evidence(copy));
        let outcomes=[a.join().unwrap(),b.join().unwrap()];
        assert_eq!(outcomes.iter().filter(|r|
            matches!(r,Ok((_,NativeSpotAuditWrite::Recorded)))).count(),1);
        assert_eq!(outcomes.iter().filter(|r|
            matches!(r,Ok((_,NativeSpotAuditWrite::Idempotent)))).count(),1);
        let after=store.load().unwrap().unwrap();
        assert_eq!(after.generation,generation+1);
        assert!(after.entry.as_ref().unwrap().settled_spot_window_evidence.is_some());
        assert_eq!(after.entry.unwrap().phase,NativeEntryPhase::RecoveryRequired);
        let _=fs::remove_dir_all(root);
    }

    #[test]
    fn r32d6b2_stale_native_generation_after_exchange_read_refuses_commit(){
        let (root,store,generation)=r32d6b2_dual_state();
        let report=r32d6b2_report(&store,generation);
        store.transact(generation,|_|Ok(())).unwrap();
        let saved=fs::read(root.join("state.json")).unwrap();
        assert!(matches!(store.record_settled_spot_window_evidence(report),
            Err(NativeStateError::Generation)));
        assert_eq!(fs::read(root.join("state.json")).unwrap(),saved);
        assert!(store.load().unwrap().unwrap().entry.unwrap()
            .settled_spot_window_evidence.is_none());
        let _=fs::remove_dir_all(root);
    }

    #[test]
    fn r32d6b2_io_failure_before_rename_preserves_checkpoint_then_retries(){
        let (root,store,generation)=r32d6b2_dual_state();
        let report=r32d6b2_report(&store,generation);
        let before=fs::read(root.join("state.json")).unwrap();
        let tmp=root.join("state.tmp");
        fs::create_dir(&tmp).unwrap();
        let rejected=store.record_settled_spot_window_evidence(report.clone());
        assert!(matches!(rejected,Err(NativeStateError::Io(_))));
        assert_eq!(fs::read(root.join("state.json")).unwrap(),before);
        assert_eq!(store.load().unwrap().unwrap().generation,generation);
        fs::remove_dir(&tmp).unwrap();
        let (saved,wrote)=store.record_settled_spot_window_evidence(report).unwrap();
        assert_eq!(wrote,NativeSpotAuditWrite::Recorded);
        assert_eq!(saved.generation,generation+1);
        assert!(saved.entry.unwrap().settled_spot_window_evidence.is_some());
        let _=fs::remove_dir_all(root);
    }

    #[test]
    fn r32d6b2_forged_cross_venue_release_proof_and_identity_fail_validation(){
        let (root,store,generation)=r32d6b2_dual_state();
        let report=r32d6b2_report(&store,generation);
        let (saved,_)=store.record_settled_spot_window_evidence(report.clone()).unwrap();
        for attack in 0..7{
            let mut state=saved.clone();
            let e=state.entry.as_mut().unwrap();
            let window=e.settled_spot_window_evidence.as_mut().unwrap();
            match attack {
                0=>window.may_send_order=true,
                1=>window.cross_venue_atomicity_proven=true,
                2=>window.exchange_global_completeness_proven=true,
                3=>window.spot_order_id="FAKE-ORD".into(),
                4=>window.source_generation=state.generation,
                5=>window.checked_bill_ids.push("200".into()),
                6=>window.gross_fill_atoms_e18=1,
                _=>unreachable!(),
            };
            assert!(state.validate().is_err(),"attack {attack} changed state");
        }
        let mut fake=report;
        fake.may_send_order=true;
        assert!(matches!(store.record_settled_spot_window_evidence(fake),
            Err(NativeStateError::Invalid(_))));
        assert_eq!(store.load().unwrap().unwrap(),saved);
        let _=fs::remove_dir_all(root);
    }

    #[test]
    fn r32d6b2_native_change_during_mock_exchange_audit_fails_closed(){
        use crate::native_settled_spot_audit::{
            audit_prior_settled_spot_window_once,SettledSpotAuditError};
        use crate::private_rest::{OkxCredentials,OkxDomain};
        use std::sync::atomic::{AtomicBool,Ordering};
        struct MutatingCash {
            inner:R32D6B2Mock,store:NativeStateStore,
            generation:u64,done:AtomicBool,
        }
        impl crate::private_https::HttpExecutor for MutatingCash{
            fn execute_request(&self,req:&crate::private_https::HttpsRequest)
                ->Result<crate::private_https::HttpResponse,crate::private_https::TransportError>{
                let resp=self.inner.execute_request(req);
                if self.inner.count()==3&&!self.done.swap(true,Ordering::SeqCst){
                    self.store.transact(self.generation,|_|Ok(())).unwrap();
                }
                resp
            }
        }
        let (root,store,generation)=r32d6b2_dual_state();
        let okx=OkxCredentials::new("FAKE_OKX","FAKE_SECRET","FAKE_PASS").unwrap();
        let exchange=MutatingCash{
            inner:R32D6B2Mock::new(R32D6B2Case::Good),
            store:store.clone(),generation,done:AtomicBool::new(false),
        };
        let result=audit_prior_settled_spot_window_once(
            &exchange,&store,generation,&okx,1_780_000_010_125,
            OkxDomain::Global,1);
        assert!(matches!(result,Err(SettledSpotAuditError::State(NativeStateError::Generation))));
        assert_eq!(exchange.inner.count(),6);
        assert_eq!(store.load().unwrap().unwrap().generation,generation+1);
        assert!(store.load().unwrap().unwrap().entry.unwrap().settled_spot_window_evidence.is_none());
        let _=fs::remove_dir_all(root);
    }

    #[test]
    fn r32d6b2_child_early_or_after_spot_window_save(){
        let Ok(path)=std::env::var("R32D6B2_CHILD_DEST")else{return};
        let stage=std::env::var("R32D6B2_CHILD_STAGE").unwrap();
        let (root,store,generation)=r32d6b2_dual_state();
        let report=r32d6b2_report(&store,generation);
        let dest=PathBuf::from(path);
        fs::create_dir_all(&dest).unwrap();
        fs::write(dest.join("state.json"),fs::read(root.join("state.json")).unwrap()).unwrap();
        File::open(dest.as_path()).unwrap().sync_all().unwrap();
        let authoritative=NativeStateStore::open(dest.join("state.json")).unwrap();
        if stage=="after"{
            let (_,outcome)=authoritative.record_settled_spot_window_evidence(report).unwrap();
            assert_eq!(outcome,NativeSpotAuditWrite::Recorded);
        }else{assert_eq!(stage,"before");}
        std::process::exit(75);
    }

    #[test]
    fn r32d6b2_real_subprocess_exit_before_after_spot_record_reopen(){
        for stage in ["before","after"]{
            let root=root(&format!("r32d6b2-exit-{stage}"));
            fs::create_dir_all(&root).unwrap();
            let status=std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("authoritative_state::tests::r32d6b2_child_early_or_after_spot_window_save")
                .env("R32D6B2_CHILD_DEST",&root)
                .env("R32D6B2_CHILD_STAGE",stage)
                .status().unwrap();
            assert_eq!(status.code(),Some(75));
            let store=NativeStateStore::open(root.join("state.json")).unwrap();
            let recovered=store.load().unwrap().unwrap();
            assert_eq!(recovered.entry.as_ref().unwrap().phase,NativeEntryPhase::RecoveryRequired);
            assert_eq!(recovered.entry.as_ref().unwrap()
                .settled_spot_window_evidence.is_some(),stage=="after");
            assert!(!recovered.entry.as_ref().unwrap().dual_leg_evidence.as_ref().unwrap()
                .combined_exchange_snapshot_verified);
            if stage=="before"{
                let report=r32d6b2_report(&store,recovered.generation);
                let (new,wrote)=store.record_settled_spot_window_evidence(report).unwrap();
                assert_eq!(wrote,NativeSpotAuditWrite::Recorded);
                assert_eq!(new.generation,recovered.generation+1);
                assert!(new.entry.unwrap().settled_spot_window_evidence.is_some());
            }
            let _=fs::remove_dir_all(root);
        }
    }

    struct R32D6B2BJointMock{
        spot:R32D6B2Mock,binance:R32D5BMock,trades:R32D6SymbolTrades,
        alter_second_binance_wallet:bool,
        wallet_reads:std::sync::atomic::AtomicUsize,
    }
    impl R32D6B2BJointMock{
        fn new(spot:R32D6B2Case,bin:R32D5BCase,trades:R32D6Case,alter:bool)->Self{
            Self{spot:R32D6B2Mock::new(spot),binance:R32D5BMock::new(bin),
                trades:R32D6SymbolTrades::new(trades),alter_second_binance_wallet:alter,
                wallet_reads:std::sync::atomic::AtomicUsize::new(0)}
        }
        fn count(&self)->usize{
            self.spot.count()+self.binance.count()+self.trades.count()
        }
    }
    impl crate::private_https::HttpExecutor for R32D6B2BJointMock{
        fn execute_request(&self,req:&crate::private_https::HttpsRequest)
            ->Result<crate::private_https::HttpResponse,crate::private_https::TransportError>{
            use crate::private_rest::HttpMethod;
            use std::sync::atomic::Ordering;
            assert_eq!(req.method,HttpMethod::Get);
            assert!(!req.mutation);
            let path=url::Url::parse(&req.url).unwrap().path().to_string();
            if path.starts_with("/api/v5/"){
                self.spot.execute_request(req)
            }else if path=="/fapi/v1/userTrades"{
                self.trades.execute_request(req)
            }else{
                let mut response=self.binance.execute_request(req)?;
                if path=="/fapi/v3/balance"{
                    let n=self.wallet_reads.fetch_add(1,Ordering::SeqCst);
                    if self.alter_second_binance_wallet&&n>=2{
                        response.body=response.body.replace(
                            r#""balance":"100""#,r#""balance":"99""#,
                        );
                    }
                }
                Ok(response)
            }
        }
    }
    fn r32d6b2b_ready()->(PathBuf,NativeStateStore,u64){
        let (root,store,gen)=r32d6b2_dual_state();
        let old=r32d6b2_report(&store,gen);
        let (result,write)=store.record_settled_spot_window_evidence(old).unwrap();
        assert_eq!(write,NativeSpotAuditWrite::Recorded);
        (root,store,result.generation)
    }
    fn r32d6b2b_read(
        executor:&R32D6B2BJointMock,store:&NativeStateStore,gen:u64
    )->Result<crate::native_joint_window::JointWindowReport,
        crate::native_joint_window::JointWindowError>{
        use crate::native_joint_window::audit_joint_recovery_window_once;
        use crate::private_rest::{OkxCredentials,BinanceCredentials,OkxDomain};
        let okx=OkxCredentials::new("FAKE_OKX","FAKE_SECRET","FAKE_PASS").unwrap();
        let bin=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        audit_joint_recovery_window_once(
            executor,store,gen,&okx,&bin,1_780_000_010_125,
            OkxDomain::Global,10_000,1,1,10,
        )
    }
    #[test]
    fn r32d6b2b_joint_okx_binance_bracket_durable_no_release(){
        let (root,store,generation)=r32d6b2b_ready();
        let old=fs::read(root.join("state.json")).unwrap();
        let mock=R32D6B2BJointMock::new(
            R32D6B2Case::Good,R32D5BCase::Good,R32D6Case::Complete,false);
        let report=r32d6b2b_read(&mock,&store,generation).unwrap();
        assert_eq!(mock.spot.count(),6);
        assert_eq!(mock.binance.count(),26);
        assert_eq!(mock.trades.count(),1);
        assert_eq!(mock.count(),33);
        assert_eq!(report.okx_fills_count,1);
        assert_eq!(report.okx_bills_count,1);
        assert_eq!(report.futures_trades_count,2);
        assert_eq!(report.futures_income_commissions_count,2);
        assert_eq!(report.income_checks,2);
        assert!(report.read_only_candidate);
        assert!(!report.exchange_global_history_complete);
        assert!(!report.exchange_cross_venue_atomicity_proven);
        assert!(!report.may_send_order);
        assert_eq!(fs::read(root.join("state.json")).unwrap(),old);
        let (saved,write)=store.record_native_joint_window(report.clone()).unwrap();
        assert_eq!(write,NativeJointWindowWrite::Recorded);
        assert_eq!(saved.generation,generation+1);
        let entry=saved.entry.as_ref().unwrap();
        let proof=entry.joint_window_evidence.as_ref().unwrap();
        assert_eq!(proof.source_generation,generation);
        assert_eq!(proof.requested_at_ms,1_780_000_010_125);
        assert_eq!(proof.spot.spot_order_id,"789");
        assert_eq!(proof.futures_order_id,"124");
        assert_eq!(proof.futures_trade_ids,vec!["1001","1002"]);
        assert_eq!(proof.futures_short_atoms_e18,400_000_000_000_000_000);
        assert_eq!(proof.usdt_wallet_atoms_e18,100_000_000_000_000_000_000);
        assert_eq!(proof.futures_fee_quote_atoms_e12,40_000_000_000);
        assert_eq!(proof.income_check_repetitions,2);
        assert!(!proof.exchange_global_history_complete);
        assert!(!proof.exchange_cross_venue_atomicity_proven);
        assert!(!proof.may_send_order);
        assert_eq!(entry.phase,NativeEntryPhase::RecoveryRequired);
        assert_eq!(entry.spot_filled_atoms_e18,1_000_000_000_000_000_000);
        assert_eq!(entry.futures_filled_atoms_e18,0);
        let bytes=fs::read(root.join("state.json")).unwrap();
        let reopened=NativeStateStore::open(root.join("state.json")).unwrap();
        assert_eq!(reopened.load().unwrap().unwrap(),saved);
        let (stable,again)=reopened.record_native_joint_window(report).unwrap();
        assert_eq!(again,NativeJointWindowWrite::Idempotent);
        assert_eq!(stable,saved);
        assert_eq!(fs::read(root.join("state.json")).unwrap(),bytes);
        assert!(reopened.prepare_futures_entry_hedge(saved.generation,7,"forbidden",
            "sab00000000000000000000000000721").is_err());
        let _=fs::remove_dir_all(root);
    }
    #[test]
    fn r32d6b2b_missing_prior_spot_window_rejects_without_network(){
        use crate::native_joint_window::JointWindowError;
        let (root,store,generation)=r32d6b2_dual_state();
        let mock=R32D6B2BJointMock::new(
            R32D6B2Case::Good,R32D5BCase::Good,R32D6Case::Complete,false);
        let result=r32d6b2b_read(&mock,&store,generation);
        assert!(matches!(result,Err(JointWindowError::Invalid(_))));
        assert_eq!(mock.count(),0);
        let _=fs::remove_dir_all(root);
    }
    #[test]
    fn r32d6b2b_foreign_zero_net_trade_income_and_balance_drift_fail_closed(){
        use crate::native_joint_window::JointWindowError;
        for (spot,bin,trades,alter) in [
            (R32D6B2Case::CashChanged,R32D5BCase::Good,R32D6Case::Complete,false),
            (R32D6B2Case::Good,R32D5BCase::FundingFee,R32D6Case::Complete,false),
            (R32D6B2Case::Good,R32D5BCase::Good,R32D6Case::ForeignRoundTrip,false),
            (R32D6B2Case::Good,R32D5BCase::Good,R32D6Case::Complete,true),
        ]{
            let (root,store,generation)=r32d6b2b_ready();
            let bytes=fs::read(root.join("state.json")).unwrap();
            let mock=R32D6B2BJointMock::new(spot,bin,trades,alter);
            let result=r32d6b2b_read(&mock,&store,generation);
            assert!(matches!(result,
                Err(JointWindowError::Spot(_))|
                Err(JointWindowError::Binance(_))|
                Err(JointWindowError::Trades(_))|
                Err(JointWindowError::Invalid(_))),
                "case={spot:?}/{bin:?}/{trades:?}/{alter} result={result:?}");
            assert_eq!(fs::read(root.join("state.json")).unwrap(),bytes);
            assert!(store.load().unwrap().unwrap().entry.unwrap().joint_window_evidence.is_none());
            let _=fs::remove_dir_all(root);
        }
    }
    #[test]
    fn r32d6b2b_corrupt_proof_flag_gen_futures_fee_and_spot_origin_denied(){
        let (root,store,generation)=r32d6b2b_ready();
        let mock=R32D6B2BJointMock::new(
            R32D6B2Case::Good,R32D5BCase::Good,R32D6Case::Complete,false);
        let report=r32d6b2b_read(&mock,&store,generation).unwrap();
        let (saved,_)=store.record_native_joint_window(report).unwrap();
        for case in 0..9{
            let mut fake=saved.clone();
            let window=fake.entry.as_mut().unwrap().joint_window_evidence.as_mut().unwrap();
            match case{
                0=>window.may_send_order=true,
                1=>window.exchange_global_history_complete=true,
                2=>window.exchange_cross_venue_atomicity_proven=true,
                3=>window.futures_order_id="FORGED".into(),
                4=>window.source_generation=saved.generation,
                5=>window.futures_fee_quote_atoms_e12=1,
                6=>window.spot.spot_order_id="OTHER".into(),
                7=>window.futures_trade_ids.push("DUP".into()),
                8=>window.income_check_repetitions=1,
                _=>unreachable!(),
            }
            assert!(fake.validate().is_err(),"joint tamper case {case} allowed");
        }
        assert_eq!(store.load().unwrap().unwrap(),saved);
        let _=fs::remove_dir_all(root);
    }

    #[test]
    fn r32d6b2b_concurrent_native_joint_writers_are_single_durable_commit(){
        use std::thread;
        let (root,store,generation)=r32d6b2b_ready();
        let fake=R32D6B2BJointMock::new(
            R32D6B2Case::Good,R32D5BCase::Good,R32D6Case::Complete,false);
        let report=r32d6b2b_read(&fake,&store,generation).unwrap();
        let s1=store.clone();let s2=store.clone();let same=report.clone();
        let a=thread::spawn(move||s1.record_native_joint_window(report));
        let b=thread::spawn(move||s2.record_native_joint_window(same));
        let answers=[a.join().unwrap(),b.join().unwrap()];
        assert_eq!(answers.iter().filter(|r|
            matches!(r,Ok((_,NativeJointWindowWrite::Recorded)))).count(),1);
        assert_eq!(answers.iter().filter(|r|
            matches!(r,Ok((_,NativeJointWindowWrite::Idempotent)))).count(),1);
        let saved=store.load().unwrap().unwrap();
        assert_eq!(saved.generation,generation+1);
        assert!(saved.entry.as_ref().unwrap().joint_window_evidence.is_some());
        assert_eq!(saved.entry.unwrap().phase,NativeEntryPhase::RecoveryRequired);
        let _=fs::remove_dir_all(root);
    }
    #[test]
    fn r32d6b2b_stale_generation_after_all_gets_cannot_store_joint(){
        let (root,store,generation)=r32d6b2b_ready();
        let fake=R32D6B2BJointMock::new(
            R32D6B2Case::Good,R32D5BCase::Good,R32D6Case::Complete,false);
        let report=r32d6b2b_read(&fake,&store,generation).unwrap();
        store.transact(generation,|_|Ok(())).unwrap();
        let bytes=fs::read(root.join("state.json")).unwrap();
        assert!(matches!(store.record_native_joint_window(report),
            Err(NativeStateError::Generation)));
        assert_eq!(fs::read(root.join("state.json")).unwrap(),bytes);
        assert!(store.load().unwrap().unwrap().entry.unwrap().joint_window_evidence.is_none());
        let _=fs::remove_dir_all(root);
    }
    #[test]
    fn r32d6b2b_disk_write_blocker_keeps_old_proof_then_exact_retry(){
        let (root,store,generation)=r32d6b2b_ready();
        let fake=R32D6B2BJointMock::new(
            R32D6B2Case::Good,R32D5BCase::Good,R32D6Case::Complete,false);
        let report=r32d6b2b_read(&fake,&store,generation).unwrap();
        let original=fs::read(root.join("state.json")).unwrap();
        let tmp=root.join("state.tmp");
        fs::create_dir(&tmp).unwrap();
        assert!(matches!(store.record_native_joint_window(report.clone()),
            Err(NativeStateError::Io(_))));
        assert_eq!(fs::read(root.join("state.json")).unwrap(),original);
        fs::remove_dir(&tmp).unwrap();
        let (saved,write)=store.record_native_joint_window(report).unwrap();
        assert_eq!(write,NativeJointWindowWrite::Recorded);
        assert_eq!(saved.generation,generation+1);
        assert!(saved.entry.unwrap().joint_window_evidence.is_some());
        let _=fs::remove_dir_all(root);
    }
    #[test]
    fn r32d6b2b_child_exit_without_cleanup_before_or_after_commit(){
        let Ok(destination)=std::env::var("R32D6B2B_CHILD_DEST")else{return};
        let phase=std::env::var("R32D6B2B_CHILD_STAGE").unwrap();
        let (root,store,generation)=r32d6b2b_ready();
        let fake=R32D6B2BJointMock::new(
            R32D6B2Case::Good,R32D5BCase::Good,R32D6Case::Complete,false);
        let report=r32d6b2b_read(&fake,&store,generation).unwrap();
        let dest=PathBuf::from(destination);
        fs::create_dir_all(&dest).unwrap();
        fs::write(dest.join("state.json"),fs::read(root.join("state.json")).unwrap()).unwrap();
        File::open(dest.as_path()).unwrap().sync_all().unwrap();
        let checkpoint=NativeStateStore::open(dest.join("state.json")).unwrap();
        if phase=="after"{
            let (saved,r)=checkpoint.record_native_joint_window(report).unwrap();
            assert_eq!(r,NativeJointWindowWrite::Recorded);
            assert_eq!(saved.generation,generation+1);
        }else{assert_eq!(phase,"before");}
        std::process::exit(76);
    }
    #[test]
    fn r32d6b2b_abrupt_subprocess_exit_reopens_old_or_new_complete_state(){
        for stage in ["before","after"]{
            let root=root(&format!("r32d6b2b-exit-{stage}"));
            fs::create_dir_all(&root).unwrap();
            let result=std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("authoritative_state::tests::r32d6b2b_child_exit_without_cleanup_before_or_after_commit")
                .env("R32D6B2B_CHILD_DEST",&root)
                .env("R32D6B2B_CHILD_STAGE",stage)
                .status().unwrap();
            assert_eq!(result.code(),Some(76));
            let store=NativeStateStore::open(root.join("state.json")).unwrap();
            let persisted=store.load().unwrap().unwrap();
            assert_eq!(persisted.entry.as_ref().unwrap().phase,NativeEntryPhase::RecoveryRequired);
            assert_eq!(persisted.entry.as_ref().unwrap().joint_window_evidence.is_some(),
                stage=="after");
            if stage=="before"{
                let mock=R32D6B2BJointMock::new(
                    R32D6B2Case::Good,R32D5BCase::Good,R32D6Case::Complete,false);
                let replay=r32d6b2b_read(&mock,&store,persisted.generation).unwrap();
                let (saved,wrote)=store.record_native_joint_window(replay).unwrap();
                assert_eq!(wrote,NativeJointWindowWrite::Recorded);
                assert_eq!(saved.generation,persisted.generation+1);
            }else{
                assert_eq!(persisted.entry.as_ref().unwrap()
                    .joint_window_evidence.as_ref().unwrap().income_check_repetitions,2);
            }
            let _=fs::remove_dir_all(root);
        }
    }

    #[test]
    fn r32d6b2b_native_generation_drift_during_futures_trade_scan_denies_joint(){
        use crate::native_joint_window::{audit_joint_recovery_window_once,JointWindowError};
        use crate::private_rest::{BinanceCredentials,OkxCredentials,OkxDomain};
        use std::sync::atomic::{AtomicBool,Ordering};
        struct ConcurrentStateDrift{
            inner:R32D6B2BJointMock,
            store:NativeStateStore,generation:u64,done:AtomicBool,
        }
        impl crate::private_https::HttpExecutor for ConcurrentStateDrift{
            fn execute_request(&self,req:&crate::private_https::HttpsRequest)
                ->Result<crate::private_https::HttpResponse,crate::private_https::TransportError>{
                let result=self.inner.execute_request(req);
                if req.url.contains("/fapi/v1/userTrades")
                    &&!self.done.swap(true,Ordering::SeqCst){
                    self.store.transact(self.generation,|_|Ok(())).unwrap();
                }
                result
            }
        }
        let (root,store,generation)=r32d6b2b_ready();
        let initial=store.load().unwrap().unwrap();
        let mock=ConcurrentStateDrift{
            inner:R32D6B2BJointMock::new(
                R32D6B2Case::Good,R32D5BCase::Good,R32D6Case::Complete,false),
            store:store.clone(),generation,done:AtomicBool::new(false),
        };
        let okx=OkxCredentials::new("FAKE_OKX","FAKE_SECRET","FAKE_PASS").unwrap();
        let bin=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        let result=audit_joint_recovery_window_once(
            &mock,&store,generation,&okx,&bin,1_780_000_010_125,
            OkxDomain::Global,10_000,1,1,10);
        assert!(matches!(result,Err(JointWindowError::Trades(
            crate::native_futures_trade_scope::FuturesTradeScopeError::State(
                NativeStateError::Generation)))),
            "unexpected CAS response={result:?}");
        let after=store.load().unwrap().unwrap();
        assert_eq!(after.generation,generation+1);
        assert_eq!(initial.entry.as_ref().unwrap().phase,NativeEntryPhase::RecoveryRequired);
        assert_eq!(after.entry.as_ref().unwrap().phase,NativeEntryPhase::RecoveryRequired);
        assert!(after.entry.unwrap().joint_window_evidence.is_none());
        assert_eq!(mock.inner.spot.count(),6);
        assert_eq!(mock.inner.trades.count(),1);
        let _=fs::remove_dir_all(root);
    }

    #[test]
    fn astra_p106_r135_native_pending_is_mandatory_in_read_only_okx_fee_gate(){
        use crate::native_trading_config::NativeTradingConfig;
        use crate::native_okx_fee_day_r208_review::{
            inspect_persisted_native_pending_fee_day_r135,NativePendingFeeJoinErrorR135,
        };
        let cfg=NativeTradingConfig::parse(
            include_str!("../config/native_trading.yaml")).unwrap();
        let root=root("astra-r135-spot-pending");
        let store=NativeStateStore::open(root.join("state.json")).unwrap();
        store.create(&empty()).unwrap();
        let (state,_permit)=store.begin_native_entry(
            1,entry_plan(),pristine_entry_economics(),"r135-spot",
            "sab00000000000000000000000000135").unwrap();
        let now=entry_plan().created_at_ms+100_000;
        let one=inspect_persisted_native_pending_fee_day_r135(
            &store,state.generation,&cfg,now,&[]).unwrap();
        assert_eq!(one.native_unresolved_spot_count,1);
        assert_eq!(one.native_unresolved_futures_count,0);
        assert_eq!(one.fee_day.open_or_unknown_order_count,1);
        assert_eq!(one.fee_day.known_quote_okx_fees_e12,0);
        // R208 with an incomplete caller-provided list would show zero
        // locally unresolved orders: R135 corrects that from cold NativeState.
        let naive=crate::native_okx_fee_day_r208_review::
           inspect_okx_fee_day_lower_bound_r208(&cfg,now,&[],&[]).unwrap();
        assert_eq!(naive.open_or_unknown_order_count,0);
        let row=crate::native_okx_fee_day_r208_review::SourcedOkxFeeFillR208{
           account_owner:"review-owner".into(),order_id:"123456".into(),
           trade_id:"review-r135-trade".into(),symbol:"BNT-USDT".into(),
           fee_ccy:"USDT".into(),execution_at_ms:now-100,
           quoted_fee_cost_e12:7_000_000_000_000,
           source_row_is_paper:false,
        };
        let hit=inspect_persisted_native_pending_fee_day_r135(
            &store,state.generation,&cfg,now,&[row]).unwrap();
        assert_eq!(hit.fee_day.open_or_unknown_order_count,1);
        assert!(hit.fee_day.cap_reached_by_known_fees);
        assert!(!hit.may_open_real);
        assert!(!one.account_wide_inventory_complete);
        assert!(!one.may_open_real);
        assert!(inspect_persisted_native_pending_fee_day_r135(
            &store,state.generation+1,&cfg,now,&[]).is_err());
        let restarted=NativeStateStore::open(root.join("state.json")).unwrap();
        assert_eq!(inspect_persisted_native_pending_fee_day_r135(
            &restarted,state.generation,&cfg,now,&[]).unwrap(),one);
        let mut tampered=cfg.clone();
        tampered.raw["real_trial"]["deny_on_unknown"]=serde_yaml::Value::Bool(false);
        assert_eq!(inspect_persisted_native_pending_fee_day_r135(
            &restarted,state.generation,&tampered,now,&[]).err(),
            Some(NativePendingFeeJoinErrorR135::Config));
        let _=fs::remove_dir_all(root);

        let (root,store,generation)=r32d6b2b_ready();
        let pending=store.pending_native_entry_query(generation).unwrap();
        assert_eq!(pending.leg,NativeEntryLeg::Futures);
        let future=inspect_persisted_native_pending_fee_day_r135(
            &store,generation,&cfg,1_780_000_100_000,&[]).unwrap();
        assert_eq!(future.native_unresolved_futures_count,1);
        assert_eq!(future.native_unresolved_spot_count,0);
        // A Binance UNKNOWN must never masquerade as an OKX fee/order.
        assert_eq!(future.fee_day.open_or_unknown_order_count,0);
        assert!(!future.account_wide_inventory_complete&&!future.may_open_real);
        let _=fs::remove_dir_all(root);
    }
    #[test]
    fn r32d6c1_durable_joint_diagnostic_never_rearms_prepared_entry(){
        use crate::execution_adapter::{OrderIntent,Venue};
        let (root,store,generation)=r32d6b2b_ready();
        let mock=R32D6B2BJointMock::new(
            R32D6B2Case::Good,R32D5BCase::Good,R32D6Case::Complete,false);
        let report=r32d6b2b_read(&mock,&store,generation).unwrap();
        let (saved,kind)=store.record_native_joint_window(report).unwrap();
        assert_eq!(kind,NativeJointWindowWrite::Recorded);
        let reader=NativeStateStore::open(root.join("state.json")).unwrap();
        let target=reader.pending_native_entry_query(saved.generation).unwrap();
        let intent=OrderIntent{
            venue:Venue::BinanceUsdm,
            symbol:target.futures_symbol.clone(),side:"sell".into(),
            amount:"1".into(),client_id:target.client_id.clone(),
        };
        for safety_gen in [7,8]{
            let simulated=EntryNetworkPermit{
                generation:saved.generation,
                intent_id:target.intent_id.clone(),
                leg:NativeEntryLeg::Futures,
            };
            // Even a test-scoped forged, exact-looking permit cannot
            // resurrect the pending UNKNOWN futures entry after restart.
            assert!(reader.authorize_entry_order(simulated,&intent,safety_gen).is_err());
            assert!(reader.prepare_futures_entry_hedge(
                saved.generation,safety_gen,"new-hedge-id","sab00000000000000000000000000801",
            ).is_err());
        }
        let old=EntryNetworkPermit{
            generation,
            intent_id:target.intent_id.clone(),leg:NativeEntryLeg::Futures,
        };
        assert!(reader.authorize_entry_order(old,&intent,7).is_err());
        let reloaded=reader.load_for_recovery().unwrap().unwrap();
        assert_eq!(reloaded.entry.as_ref().unwrap().phase,NativeEntryPhase::RecoveryRequired);
        assert!(reloaded.entry.as_ref().unwrap().joint_window_evidence.is_some());
        assert_eq!(reloaded.entry.as_ref().unwrap()
            .joint_window_evidence.as_ref().unwrap().may_send_order,false);
        let _=fs::remove_dir_all(root);
    }

    fn r32d6c2_spot_exposed()->(PathBuf,NativeStateStore,u64){
        let root=root("r32d6c2-first-leg");
        let store=NativeStateStore::open(root.join("state.json")).unwrap();
        store.create(&empty()).unwrap();
        let cid="sab00000000000000000000000000791";
        let (_opened,permit)=store.begin_native_entry(
            1,entry_plan(),pristine_entry_economics(),"r32d6c2-spot",cid).unwrap();
        let settled=store.record_spot_entry_observation(
            permit,1_000_000_000_000_000_000,
            Some(decimal_to_atoms("100").unwrap()),true,false,false).unwrap();
        assert_eq!(settled.entry.as_ref().unwrap().phase,NativeEntryPhase::SpotExposed);
        (root,store,settled.generation)
    }
    #[derive(Clone,Copy,Debug)]
    enum R32D6C2Case {
        Good,MissingFill,FeeUnknown,DuplicateTrade,ForeignOrder,
        BadPrice,ChangedCash,WrongOrder,RepeatedCursor,OldBalance,
    }
    struct R32D6C2Mock{
        kind:R32D6C2Case,
        count:std::sync::atomic::AtomicUsize,
        balance_reads:std::sync::atomic::AtomicUsize,
    }
    impl R32D6C2Mock{
        fn new(kind:R32D6C2Case)->Self{
            Self{kind,count:std::sync::atomic::AtomicUsize::new(0),
                balance_reads:std::sync::atomic::AtomicUsize::new(0)}
        }
        fn calls(&self)->usize{
            self.count.load(std::sync::atomic::Ordering::SeqCst)
        }
    }
    impl crate::private_https::HttpExecutor for R32D6C2Mock{
        fn execute_request(&self,req:&crate::private_https::HttpsRequest)
            ->Result<crate::private_https::HttpResponse,crate::private_https::TransportError>{
            use crate::private_rest::HttpMethod;
            use std::sync::atomic::Ordering;
            assert_eq!(req.method,HttpMethod::Get);
            assert!(!req.mutation);
            let parsed=url::Url::parse(&req.url).unwrap();
            let qs=parsed.query_pairs().collect::<std::collections::BTreeMap<_,_>>();
            self.count.fetch_add(1,Ordering::SeqCst);
            let body=match parsed.path(){
                "/api/v5/account/balance"=>{
                    let n=self.balance_reads.fetch_add(1,Ordering::SeqCst);
                    let cash=if matches!(self.kind,R32D6C2Case::ChangedCash)&&n==1{
                        "1.1"
                    }else{"1.125"};
                    let updated=if matches!(self.kind,R32D6C2Case::OldBalance){
                        "1770000000000"
                    }else{"1780000002000"};
                    format!(r#"{{"code":"0","data":[{{"details":[{{"ccy":"BNT","cashBal":"{cash}","uTime":"{updated}"}}]}}]}}"#)
                },
                "/api/v5/trade/order"=>{
                    assert_eq!(qs.get("clOrdId").map(|s|s.as_ref()),
                        Some("sab00000000000000000000000000791"));
                    let client=if matches!(self.kind,R32D6C2Case::WrongOrder){
                        "WRONG-CLIENT"
                    }else{"sab00000000000000000000000000791"};
                    format!(r#"{{"code":"0","data":[{{"ordId":"789","clOrdId":"{client}","state":"filled","accFillSz":"1","avgPx":"100","fillTime":"1780000000320"}}]}}"#)
                },
                "/api/v5/trade/fills-history"=>{
                    assert_eq!(qs.get("instId").map(|s|s.as_ref()),Some("BNT-USDT"));
                    assert_eq!(qs.get("ordId").map(|s|s.as_ref()),Some("789"));
                    assert_eq!(qs.get("limit").map(|s|s.as_ref()),Some("1"));
                    let after=qs.get("after").map(|s|s.as_ref());
                    if after==Some("300")
                        &&!matches!(self.kind,R32D6C2Case::RepeatedCursor){
                        r#"{"code":"0","data":[]}"#.into()
                    }else if after==Some("301")
                        &&matches!(self.kind,R32D6C2Case::MissingFill){
                        r#"{"code":"0","data":[]}"#.into()
                    }else{
                        let next=after.is_some();
                        let bill=if next && matches!(self.kind,R32D6C2Case::RepeatedCursor){
                            "301"
                        }else if next{"300"}else{"301"};
                        let id=if matches!(self.kind,R32D6C2Case::DuplicateTrade){
                            "T1"
                        }else if next{"T2"}else{"T1"};
                        let order=if next&&matches!(self.kind,R32D6C2Case::ForeignOrder){
                            "999"
                        }else{"789"};
                        let fee=if next&&matches!(self.kind,R32D6C2Case::FeeUnknown){
                            "BNB"
                        }else{"USDT"};
                        let price=if next&&matches!(self.kind,R32D6C2Case::BadPrice){
                            "101"
                        }else{"100"};
                        let qty=if next{"0.6"}else{"0.4"};
                        let commission=if next{"-0.06"}else{"-0.04"};
                        let t=if next{"1780000000320"}else{"1780000000220"};
                        format!(r#"{{"code":"0","data":[{{"ordId":"{order}","billId":"{bill}","tradeId":"{id}","fillPx":"{price}","fillSz":"{qty}","fee":"{commission}","feeCcy":"{fee}","fillTime":"{t}"}}]}}"#)
                    }
                },
                other=>panic!("R32D6C2 unexpected read endpoint {other}"),
            };
            Ok(crate::private_https::HttpResponse{status:200,headers:vec![],body})
        }
    }
    fn r32d6c2_audit(store:&NativeStateStore,generation:u64,
        mock:&R32D6C2Mock,
    )->Result<crate::native_spot_prehedge::SpotPreHedgeReport,
             crate::native_spot_prehedge::SpotPreHedgeError>{
        use crate::native_spot_prehedge::audit_okx_first_leg_before_hedge;
        use crate::private_rest::{OkxCredentials,OkxDomain};
        let creds=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
        audit_okx_first_leg_before_hedge(
            mock,store,generation,&creds,1_780_000_010_125,OkxDomain::Global,1,
        )
    }
    #[test]
    fn r32d6c2_first_leg_per_trade_fee_capture_fsync_reopen_hedge_gate(){
        let (root,store,generation)=r32d6c2_spot_exposed();
        let before=fs::read(root.join("state.json")).unwrap();
        assert!(store.load().unwrap().unwrap().entry.unwrap()
            .require_spot_prehedge_provenance().is_err());
        let mock=R32D6C2Mock::new(R32D6C2Case::Good);
        let result=r32d6c2_audit(&store,generation,&mock).unwrap();
        assert_eq!(mock.calls(),6);
        assert_eq!(result.fills,2);
        assert_eq!(result.pages,3);
        assert_eq!(result.fee_quote_atoms_e12,100_000_000_000);
        assert_eq!(result.cash_atoms_e18,1_125_000_000_000_000_000);
        assert!(!result.may_send_order);
        assert_eq!(fs::read(root.join("state.json")).unwrap(),before);
        let (recorded,write)=store.record_native_spot_first_leg_evidence(result.clone()).unwrap();
        assert_eq!(write,NativeSpotFirstLegWrite::Recorded);
        assert_eq!(recorded.generation,generation+1);
        let first=recorded.entry.as_ref().unwrap();
        first.require_spot_prehedge_provenance().unwrap();
        assert_eq!(first.phase,NativeEntryPhase::SpotExposed);
        assert_eq!(first.spot_filled_atoms_e18,1_000_000_000_000_000_000);
        assert_eq!(first.economics.entry.spot.orders.len(),1);
        let ledger=first.economics.entry.spot.order("r32d6c2-spot").unwrap();
        assert!(ledger.fee_evidence_complete);
        assert_eq!(ledger.fills.len(),2);
        assert_eq!(ledger.exchange_order_id.as_deref(),Some("789"));
        assert_eq!(ledger.fee_cost_quote_atoms,100_000_000_000);
        assert!(first.spot_prehedge_evidence.as_ref().unwrap().may_send_order==false);
        assert!(!first.spot_prehedge_evidence.as_ref().unwrap().exchange_bill_history_proven);
        let saved=fs::read(root.join("state.json")).unwrap();
        let reopened=NativeStateStore::open(root.join("state.json")).unwrap();
        assert_eq!(reopened.load().unwrap().unwrap(),recorded);
        let (stable,again)=reopened.record_native_spot_first_leg_evidence(result).unwrap();
        assert_eq!(again,NativeSpotFirstLegWrite::Idempotent);
        assert_eq!(stable,recorded);
        assert_eq!(fs::read(root.join("state.json")).unwrap(),saved);
        assert!(r32d6c2_audit(&reopened,recorded.generation,
            &R32D6C2Mock::new(R32D6C2Case::Good)).is_err());
        let (_,fut_permit)=reopened.prepare_futures_entry_hedge(
            recorded.generation,7,"r32d6c2-futures",
            "sab00000000000000000000000000792").unwrap();
        assert_eq!(fut_permit.leg(),NativeEntryLeg::Futures);
        let fut=reopened.load().unwrap().unwrap();
        assert!(fut.entry.as_ref().unwrap().spot_prehedge_evidence.is_some());
        assert_eq!(fut.entry.unwrap().phase,NativeEntryPhase::FuturesPrepared);
        let _=fs::remove_dir_all(root);
    }
    #[test]
    fn r32d6c2_missing_duplicate_foreign_fee_cash_and_average_deny_without_write(){
        use crate::native_spot_prehedge::SpotPreHedgeError;
        for case in [
            R32D6C2Case::MissingFill,R32D6C2Case::FeeUnknown,
            R32D6C2Case::DuplicateTrade,R32D6C2Case::ForeignOrder,
            R32D6C2Case::BadPrice,R32D6C2Case::ChangedCash,
            R32D6C2Case::WrongOrder,R32D6C2Case::RepeatedCursor,
            R32D6C2Case::OldBalance,
        ]{
            let (root,store,generation)=r32d6c2_spot_exposed();
            let bytes=fs::read(root.join("state.json")).unwrap();
            let mock=R32D6C2Mock::new(case);
            let result=r32d6c2_audit(&store,generation,&mock);
            assert!(matches!(result,
                Err(SpotPreHedgeError::Invalid(_))
                |Err(SpotPreHedgeError::Stale)
                |Err(SpotPreHedgeError::UnexhaustedHistory)),
                "case={case:?}: {result:?}");
            assert!(mock.calls()>0);
            assert_eq!(fs::read(root.join("state.json")).unwrap(),bytes);
            assert!(store.load().unwrap().unwrap().entry.unwrap()
                .require_spot_prehedge_provenance().is_err());
            let _=fs::remove_dir_all(root);
        }
    }
    #[test]
    fn r32d6c2_stale_fence_before_get_and_after_get_prevents_record(){
        use crate::native_spot_prehedge::SpotPreHedgeError;
        let (root,store,generation)=r32d6c2_spot_exposed();
        let mock=R32D6C2Mock::new(R32D6C2Case::Good);
        assert!(matches!(r32d6c2_audit(&store,generation-1,&mock),
            Err(SpotPreHedgeError::State(NativeStateError::Generation))));
        assert_eq!(mock.calls(),0);
        let report=r32d6c2_audit(&store,generation,&mock).unwrap();
        store.transact(generation,|_|Ok(())).unwrap();
        let bytes=fs::read(root.join("state.json")).unwrap();
        assert!(matches!(store.record_native_spot_first_leg_evidence(report),
            Err(NativeStateError::Generation)));
        assert_eq!(fs::read(root.join("state.json")).unwrap(),bytes);
        let _=fs::remove_dir_all(root);
    }
    #[test]
    fn r32d6c2_immutable_first_leg_fees_cannot_be_forged(){
        let (root,store,generation)=r32d6c2_spot_exposed();
        let report=r32d6c2_audit(&store,generation,
            &R32D6C2Mock::new(R32D6C2Case::Good)).unwrap();
        let (state,_)=store.record_native_spot_first_leg_evidence(report).unwrap();
        for case in 0..7{
            let mut forged=state.clone();
            let e=forged.entry.as_mut().unwrap();
            let proof=e.spot_prehedge_evidence.as_mut().unwrap();
            match case{
                0=>proof.may_send_order=true,
                1=>proof.exchange_global_completeness_proven=true,
                2=>proof.exchange_bill_history_proven=true,
                3=>proof.spot_order_id="FORGED".into(),
                4=>proof.source_generation=state.generation,
                5=>proof.fill_bill_ids.push("301".into()),
                6=>proof.observed_base_cash_atoms_e18=1,
                _=>unreachable!(),
            }
            assert!(forged.validate().is_err(),"forged case={case} was accepted");
        }
        assert_eq!(store.load().unwrap().unwrap(),state);
        let _=fs::remove_dir_all(root);
    }

    #[test]
    fn r32d6c2_two_competing_writers_cannot_duplicate_first_leg_fees(){
        use std::thread;
        let (root,store,generation)=r32d6c2_spot_exposed();
        let report=r32d6c2_audit(&store,generation,
            &R32D6C2Mock::new(R32D6C2Case::Good)).unwrap();
        let writer=store.clone();
        let other=store.clone();
        let second=report.clone();
        let a=thread::spawn(move||writer.record_native_spot_first_leg_evidence(report));
        let b=thread::spawn(move||other.record_native_spot_first_leg_evidence(second));
        let joined=[a.join().unwrap(),b.join().unwrap()];
        assert_eq!(joined.iter().filter(|r|matches!(
            r,Ok((_,NativeSpotFirstLegWrite::Recorded)))).count(),1);
        assert_eq!(joined.iter().filter(|r|matches!(
            r,Ok((_,NativeSpotFirstLegWrite::Idempotent)))).count(),1);
        let final_state=store.load().unwrap().unwrap();
        assert_eq!(final_state.generation,generation+1);
        assert_eq!(final_state.entry.unwrap().economics.entry.spot.order(
            "r32d6c2-spot").unwrap().fee_cost_quote_atoms,100_000_000_000);
        let _=fs::remove_dir_all(root);
    }

    #[test]
    fn r32d6c2_failed_tmp_write_preserves_cumulative_only_old_file(){
        let (root,store,generation)=r32d6c2_spot_exposed();
        let report=r32d6c2_audit(&store,generation,
            &R32D6C2Mock::new(R32D6C2Case::Good)).unwrap();
        let raw=fs::read(root.join("state.json")).unwrap();
        let tmp=root.join("state.tmp");
        fs::create_dir(&tmp).unwrap();
        let failed=store.record_native_spot_first_leg_evidence(report.clone());
        assert!(matches!(failed,Err(NativeStateError::Io(_))));
        assert_eq!(fs::read(root.join("state.json")).unwrap(),raw);
        let previous=store.load().unwrap().unwrap();
        assert_eq!(previous.generation,generation);
        assert!(previous.entry.unwrap().spot_prehedge_evidence.is_none());
        fs::remove_dir(&tmp).unwrap();
        let (committed,kind)=store.record_native_spot_first_leg_evidence(report).unwrap();
        assert_eq!(kind,NativeSpotFirstLegWrite::Recorded);
        assert_eq!(committed.generation,generation+1);
        assert_eq!(committed.entry.unwrap().economics.entry.spot.order(
            "r32d6c2-spot").unwrap().fills.len(),2);
        let _=fs::remove_dir_all(root);
    }

    #[test]
    fn r32d6c2_child_abrupt_exit_before_or_after_first_leg_fsync(){
        let Ok(folder)=std::env::var("R32D6C2_CHILD_ROOT")else{return};
        let mode=std::env::var("R32D6C2_CHILD_MODE").unwrap();
        let (root,store,generation)=r32d6c2_spot_exposed();
        let report=r32d6c2_audit(&store,generation,
            &R32D6C2Mock::new(R32D6C2Case::Good)).unwrap();
        let destination=PathBuf::from(folder);
        fs::create_dir_all(&destination).unwrap();
        fs::write(destination.join("state.json"),
            fs::read(root.join("state.json")).unwrap()).unwrap();
        File::open(destination.as_path()).unwrap().sync_all().unwrap();
        let durable=NativeStateStore::open(destination.join("state.json")).unwrap();
        if mode=="after"{
            let (saved,kind)=durable.record_native_spot_first_leg_evidence(report).unwrap();
            assert_eq!(kind,NativeSpotFirstLegWrite::Recorded);
            assert_eq!(saved.generation,generation+1);
        }else{assert_eq!(mode,"before");}
        std::process::exit(77);
    }

    #[test]
    fn r32d6c2_forced_process_exit_reloads_old_or_full_first_leg(){
        for mode in ["before","after"]{
            let root=root(&format!("r32d6c2-child-{mode}"));
            fs::create_dir_all(&root).unwrap();
            let outcome=std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("authoritative_state::tests::r32d6c2_child_abrupt_exit_before_or_after_first_leg_fsync")
                .env("R32D6C2_CHILD_ROOT",&root)
                .env("R32D6C2_CHILD_MODE",mode)
                .status().unwrap();
            assert_eq!(outcome.code(),Some(77));
            let store=NativeStateStore::open(root.join("state.json")).unwrap();
            let state=store.load().unwrap().unwrap();
            let entry=state.entry.as_ref().unwrap();
            assert_eq!(entry.phase,NativeEntryPhase::SpotExposed);
            let pre=entry.spot_prehedge_evidence.as_ref();
            let fills=entry.economics.entry.spot.order("r32d6c2-spot").unwrap();
            assert_eq!(pre.is_some(),mode=="after");
            assert_eq!(fills.fills.len(),if mode=="after"{2}else{0});
            if mode=="after"{
                assert_eq!(fills.fee_cost_quote_atoms,100_000_000_000);
                entry.require_spot_prehedge_provenance().unwrap();
            }else{assert!(entry.require_spot_prehedge_provenance().is_err());}
            let _=fs::remove_dir_all(root);
        }
    }

    #[derive(Clone,Copy,Debug)]
    enum R32D6C3Case{
        Good,ForeignZeroNetTransfer,ForeignManualOrder,MissingBill,
        ChangedFee,ChangedQty,ChangedCash,BadBalance,WrongTrade,
        WrongTime,RepeatedBill,OldCash,UpdatedDuringRead,
    }
    struct R32D6C3Mock{
        case:R32D6C3Case,
        calls:std::sync::atomic::AtomicUsize,
        cash_reads:std::sync::atomic::AtomicUsize,
    }
    impl R32D6C3Mock{
        fn new(case:R32D6C3Case)->Self{
            Self{case,calls:std::sync::atomic::AtomicUsize::new(0),
                cash_reads:std::sync::atomic::AtomicUsize::new(0)}
        }
        fn count(&self)->usize{
            self.calls.load(std::sync::atomic::Ordering::SeqCst)
        }
    }
    impl crate::private_https::HttpExecutor for R32D6C3Mock{
        fn execute_request(&self,req:&crate::private_https::HttpsRequest)
            ->Result<crate::private_https::HttpResponse,crate::private_https::TransportError>{
            use std::sync::atomic::Ordering;
            assert_eq!(req.method,crate::private_rest::HttpMethod::Get);
            assert!(!req.mutation);
            let url=url::Url::parse(&req.url).unwrap();
            let qs=url.query_pairs().collect::<std::collections::BTreeMap<_,_>>();
            self.calls.fetch_add(1,Ordering::SeqCst);
            let payload=match url.path(){
                "/api/v5/account/balance"=>{
                    let n=self.cash_reads.fetch_add(1,Ordering::SeqCst);
                    let cash=if matches!(self.case,R32D6C3Case::ChangedCash)&&n==1{
                        "1.1"
                    }else{"1.125"};
                    let t=if matches!(self.case,R32D6C3Case::OldCash){
                        "1770000000000"
                    }else if matches!(self.case,R32D6C3Case::UpdatedDuringRead)&&n==1{
                        "1780000002001"
                    }else{"1780000002000"};
                    format!(r#"{{"code":"0","data":[{{"details":[{{"ccy":"BNT","cashBal":"{cash}","availBal":"{cash}","frozenBal":"0","uTime":"{t}"}}]}}]}}"#)
                },
                "/api/v5/account/bills-archive"=>{
                    assert_eq!(qs.get("ccy").map(|s|s.as_ref()),Some("BNT"));
                    assert_eq!(qs.get("begin").map(|s|s.as_ref()),Some("1780000000125"));
                    assert_eq!(qs.get("end").map(|s|s.as_ref()),Some("1780000010125"));
                    assert_eq!(qs.get("limit").map(|s|s.as_ref()),Some("1"));
                    assert!(!qs.contains_key("instType"));
                    let cursor=qs.get("after").map(|x|x.as_ref());
                    let with_zero_net=matches!(self.case,R32D6C3Case::ForeignZeroNetTransfer);
                    let position=if with_zero_net{
                        match cursor{
                            None=>0,
                            Some("303")=>1,Some("302")=>2,
                            Some("301")=>3,Some("300")=>4,
                            other=>panic!("unexpected injected bill cursor {other:?}"),
                        }
                    }else{
                        match cursor{
                            None=>2,Some("301")=>3,Some("300")=>4,
                            other=>panic!("unexpected bill cursor {other:?}"),
                        }
                    };
                    if position==4
                        ||(matches!(self.case,R32D6C3Case::MissingBill)&&position==3){
                        r#"{"code":"0","data":[]}"#.into()
                    }else{
                        let (bill,trade,amount,base,ts)=match position{
                            0=>("303","external-credit","0.25","1.125","1780000000800"),
                            1=>("302","external-debit","-0.25","0.875","1780000000700"),
                            2=>("301","T1","0.4","1.125","1780000000600"),
                            3=>("300","T2","0.6","0.725","1780000000500"),
                            _=>unreachable!(),
                        };
                        let foreign=position<2;
                        let order=if foreign
                            ||(position==2&&matches!(self.case,R32D6C3Case::ForeignManualOrder)){
                            "MANUAL"
                        }else{"789"};
                        let subtype=if foreign{"2"}else{"1"};
                        let id=if position==2&&matches!(self.case,R32D6C3Case::WrongTrade){
                            "WRONG"
                        }else{trade};
                        let bill=if position==3&&matches!(self.case,R32D6C3Case::RepeatedBill){
                            "301"
                        }else{bill};
                        let quantity=if position==2&&matches!(self.case,R32D6C3Case::ChangedQty){
                            "0.3"
                        }else if foreign{"0.25"}else if position==2{"0.4"}else{"0.6"};
                        let fee=if foreign{"0"}
                            else if position==2&&matches!(self.case,R32D6C3Case::ChangedFee){
                                "-0.05"
                            }else if position==2{"-0.04"}else{"-0.06"};
                        let bal=if position==2&&matches!(self.case,R32D6C3Case::BadBalance){
                            "1.135"
                        }else{base};
                        let ts=if position==2&&matches!(self.case,R32D6C3Case::WrongTime){
                            "1780000000000"
                        }else{ts};
                        format!(r#"{{"code":"0","data":[{{"ccy":"BNT","ordId":"{order}","instId":"BNT-USDT","subType":"{subtype}","tradeId":"{id}","billId":"{bill}","bal":"{bal}","balChg":"{amount}","sz":"{quantity}","fee":"{fee}","feeCcy":"USDT","ts":"{ts}"}}]}}"#)
                    }
                },
                other=>panic!("unexpected GET {other}"),
            };
            Ok(crate::private_https::HttpResponse{status:200,headers:vec![],body:payload})
        }
    }
    fn r32d6c3_with_first_leg()->(PathBuf,NativeStateStore,u64){
        let (root,store,gen)=r32d6c2_spot_exposed();
        let report=r32d6c2_audit(&store,gen,&R32D6C2Mock::new(R32D6C2Case::Good)).unwrap();
        let (state,write)=store.record_native_spot_first_leg_evidence(report).unwrap();
        assert_eq!(write,NativeSpotFirstLegWrite::Recorded);
        (root,store,state.generation)
    }
    fn r32d6c3_scan(store:&NativeStateStore,generation:u64,
                     exchange:&R32D6C3Mock)
    ->Result<crate::native_spot_prehedge_bills::SpotBillsReport,
             crate::native_spot_prehedge_bills::SpotBillsError>{
        use crate::native_spot_prehedge_bills::audit_okx_first_leg_cash_bills_window;
        use crate::private_rest::{OkxCredentials,OkxDomain};
        let creds=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
        audit_okx_first_leg_cash_bills_window(
            exchange,store,generation,&creds,1_780_000_010_125,OkxDomain::Global,1,
        )
    }
    #[test]
    fn r32d6c3_currency_wide_bill_chain_cash_fee_ids_durable_and_idempotent(){
        let (root,store,generation)=r32d6c3_with_first_leg();
        assert!(store.load().unwrap().unwrap().entry.unwrap()
            .require_spot_prehedge_bills_provenance().is_err());
        let before=fs::read(root.join("state.json")).unwrap();
        let mock=R32D6C3Mock::new(R32D6C3Case::Good);
        let report=r32d6c3_scan(&store,generation,&mock).unwrap();
        assert_eq!(mock.count(),5);
        assert_eq!(report.rows,2);
        assert_eq!(report.pages,3);
        assert_eq!(report.observed_cash_e18,1_125_000_000_000_000_000);
        assert_eq!(report.base_delta_e18,1_000_000_000_000_000_000);
        assert_eq!(report.quote_fee_e12,100_000_000_000);
        assert!(report.window_matched);
        assert!(!report.account_history_globally_complete);
        assert!(!report.cross_venue_atomicity_proven);
        assert!(!report.may_send_order);
        assert_eq!(fs::read(root.join("state.json")).unwrap(),before);
        let (saved,write)=store.record_native_spot_prehedge_bills_evidence(report.clone()).unwrap();
        assert_eq!(write,NativeSpotBillsWrite::Recorded);
        assert_eq!(saved.generation,generation+1);
        let entry=saved.entry.as_ref().unwrap();
        entry.require_spot_prehedge_bills_provenance().unwrap();
        let proof=entry.spot_prehedge_bills_evidence.as_ref().unwrap();
        assert_eq!(proof.verified_bill_ids,vec!["300".to_string(),"301".to_string()]);
        assert_eq!(proof.billed_trade_ids,vec!["T1".to_string(),"T2".to_string()]);
        assert_eq!(proof.account_bill_pages,3);
        assert!(!proof.account_history_globally_complete);
        assert!(!proof.may_send_order);
        assert_eq!(entry.phase,NativeEntryPhase::SpotExposed);
        assert_eq!(entry.futures_filled_atoms_e18,0);
        let bytes=fs::read(root.join("state.json")).unwrap();
        let reopened=NativeStateStore::open(root.join("state.json")).unwrap();
        assert_eq!(reopened.load().unwrap().unwrap(),saved);
        let (again,r)=reopened.record_native_spot_prehedge_bills_evidence(report).unwrap();
        assert_eq!(r,NativeSpotBillsWrite::Idempotent);
        assert_eq!(again,saved);
        assert_eq!(fs::read(root.join("state.json")).unwrap(),bytes);
        let _=fs::remove_dir_all(root);
    }
    #[test]
    fn r32d6c3_zero_net_external_transfers_missing_bill_fee_and_cash_drift_fail_closed(){
        use crate::native_spot_prehedge_bills::SpotBillsError;
        for case in [
            R32D6C3Case::ForeignZeroNetTransfer,R32D6C3Case::ForeignManualOrder,
            R32D6C3Case::MissingBill,R32D6C3Case::ChangedFee,
            R32D6C3Case::ChangedQty,R32D6C3Case::ChangedCash,
            R32D6C3Case::BadBalance,R32D6C3Case::WrongTrade,
            R32D6C3Case::WrongTime,R32D6C3Case::RepeatedBill,
            R32D6C3Case::OldCash,R32D6C3Case::UpdatedDuringRead,
        ]{
            let (root,store,generation)=r32d6c3_with_first_leg();
            let original=fs::read(root.join("state.json")).unwrap();
            let mock=R32D6C3Mock::new(case);
            let audit=r32d6c3_scan(&store,generation,&mock);
            assert!(matches!(audit,
                Err(SpotBillsError::Invalid(_))
                |Err(SpotBillsError::NotFresh)
                |Err(SpotBillsError::HistoryNotExhausted)),
                "unexpected {case:?}: {audit:?}");
            assert!(mock.count()>0);
            assert_eq!(fs::read(root.join("state.json")).unwrap(),original);
            assert!(store.load().unwrap().unwrap().entry.unwrap()
                .require_spot_prehedge_bills_provenance().is_err());
            let _=fs::remove_dir_all(root);
        }
    }
    #[test]
    fn r32d6c3_missing_full_spot_or_wrong_native_generation_zero_private_gets(){
        use crate::native_spot_prehedge_bills::SpotBillsError;
        let (root,store,generation)=r32d6c2_spot_exposed();
        let mock=R32D6C3Mock::new(R32D6C3Case::Good);
        assert!(matches!(r32d6c3_scan(&store,generation,&mock),
            Err(SpotBillsError::State(_))));
        assert!(matches!(r32d6c3_scan(&store,generation-1,&mock),
            Err(SpotBillsError::State(NativeStateError::Generation))));
        assert_eq!(mock.count(),0);
        let _=fs::remove_dir_all(root);
    }
    #[test]
    fn r32d6c3_native_state_drift_after_complete_bills_refuses_commit(){
        let (root,store,generation)=r32d6c3_with_first_leg();
        let mock=R32D6C3Mock::new(R32D6C3Case::Good);
        let report=r32d6c3_scan(&store,generation,&mock).unwrap();
        store.transact(generation,|_|Ok(())).unwrap();
        let bytes=fs::read(root.join("state.json")).unwrap();
        assert!(matches!(store.record_native_spot_prehedge_bills_evidence(report),
            Err(NativeStateError::Generation)));
        assert_eq!(fs::read(root.join("state.json")).unwrap(),bytes);
        assert!(store.load().unwrap().unwrap().entry.unwrap()
            .spot_prehedge_bills_evidence.is_none());
        let _=fs::remove_dir_all(root);
    }
    #[test]
    fn r32d6c3_forged_bills_flags_generation_cash_and_ids_rejected(){
        let (root,store,generation)=r32d6c3_with_first_leg();
        let report=r32d6c3_scan(&store,generation,
            &R32D6C3Mock::new(R32D6C3Case::Good)).unwrap();
        let (saved,_)=store.record_native_spot_prehedge_bills_evidence(report).unwrap();
        for c in 0..10{
            let mut changed=saved.clone();
            let evidence=changed.entry.as_mut().unwrap().spot_prehedge_bills_evidence
                .as_mut().unwrap();
            match c{
                0=>evidence.may_send_order=true,
                1=>evidence.account_history_globally_complete=true,
                2=>evidence.cross_venue_atomicity_proven=true,
                3=>evidence.source_generation=saved.generation,
                4=>evidence.spot_order_id="MANUAL".into(),
                5=>evidence.original_base_cash_atoms_e18=1,
                6=>evidence.aggregate_base_delta_atoms_e18=1,
                7=>evidence.verified_bill_ids.push("301".into()),
                8=>evidence.billed_trade_ids.pop().map(|_|()).unwrap(),
                9=>evidence.account_bill_pages=1,
                _=>unreachable!(),
            }
            assert!(changed.validate().is_err(),"forged case {c} accepted");
        }
        assert_eq!(store.load().unwrap().unwrap(),saved);
        let _=fs::remove_dir_all(root);
    }

    #[test]
    fn r32d6c3_two_simultaneous_writers_single_cash_proof_commit(){
        use std::thread;
        let (root,store,generation)=r32d6c3_with_first_leg();
        let proof=r32d6c3_scan(&store,generation,
            &R32D6C3Mock::new(R32D6C3Case::Good)).unwrap();
        let a_store=store.clone();let b_store=store.clone();let second=proof.clone();
        let a=thread::spawn(move||a_store.record_native_spot_prehedge_bills_evidence(proof));
        let b=thread::spawn(move||b_store.record_native_spot_prehedge_bills_evidence(second));
        let results=[a.join().unwrap(),b.join().unwrap()];
        assert_eq!(results.iter().filter(|x|matches!(
            x,Ok((_,NativeSpotBillsWrite::Recorded)))).count(),1);
        assert_eq!(results.iter().filter(|x|matches!(
            x,Ok((_,NativeSpotBillsWrite::Idempotent)))).count(),1);
        let actual=store.load().unwrap().unwrap();
        assert_eq!(actual.generation,generation+1);
        assert!(actual.entry.as_ref().unwrap().spot_prehedge_bills_evidence.is_some());
        actual.entry.unwrap().require_spot_prehedge_bills_provenance().unwrap();
        let _=fs::remove_dir_all(root);
    }
    #[test]
    fn r32d6c3_failed_fsync_temp_write_no_partial_account_bill_proof(){
        let (root,store,generation)=r32d6c3_with_first_leg();
        let proof=r32d6c3_scan(&store,generation,
            &R32D6C3Mock::new(R32D6C3Case::Good)).unwrap();
        let original=fs::read(root.join("state.json")).unwrap();
        let tmp=root.join("state.tmp");
        fs::create_dir(&tmp).unwrap();
        assert!(matches!(store.record_native_spot_prehedge_bills_evidence(proof.clone()),
            Err(NativeStateError::Io(_))));
        assert_eq!(fs::read(root.join("state.json")).unwrap(),original);
        assert_eq!(store.load().unwrap().unwrap().generation,generation);
        fs::remove_dir(&tmp).unwrap();
        let (new,wrote)=store.record_native_spot_prehedge_bills_evidence(proof).unwrap();
        assert_eq!(wrote,NativeSpotBillsWrite::Recorded);
        assert_eq!(new.generation,generation+1);
        assert!(new.entry.unwrap().spot_prehedge_bills_evidence.is_some());
        let _=fs::remove_dir_all(root);
    }
    #[test]
    fn r32d6c3_child_process_exits_before_or_after_cash_bill_commit(){
        let Ok(dest)=std::env::var("R32D6C3_CHILD_ROOT")else{return};
        let phase=std::env::var("R32D6C3_CHILD_PHASE").unwrap();
        let (root,store,generation)=r32d6c3_with_first_leg();
        let report=r32d6c3_scan(&store,generation,
            &R32D6C3Mock::new(R32D6C3Case::Good)).unwrap();
        let folder=PathBuf::from(dest);
        fs::create_dir_all(&folder).unwrap();
        fs::write(folder.join("state.json"),
            fs::read(root.join("state.json")).unwrap()).unwrap();
        File::open(folder.as_path()).unwrap().sync_all().unwrap();
        let copied=NativeStateStore::open(folder.join("state.json")).unwrap();
        if phase=="after"{
            let (saved,r)=copied.record_native_spot_prehedge_bills_evidence(report).unwrap();
            assert_eq!(r,NativeSpotBillsWrite::Recorded);
            assert_eq!(saved.generation,generation+1);
        }else{assert_eq!(phase,"before");}
        std::process::exit(78);
    }
    #[test]
    fn r32d6c3_real_abrupt_process_exit_restores_old_or_new_cash_state(){
        for phase in ["before","after"]{
            let dir=root(&format!("r32d6c3-child-{phase}"));
            fs::create_dir_all(&dir).unwrap();
            let result=std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("authoritative_state::tests::r32d6c3_child_process_exits_before_or_after_cash_bill_commit")
                .env("R32D6C3_CHILD_ROOT",&dir)
                .env("R32D6C3_CHILD_PHASE",phase)
                .status().unwrap();
            assert_eq!(result.code(),Some(78));
            let store=NativeStateStore::open(dir.join("state.json")).unwrap();
            let recovered=store.load().unwrap().unwrap();
            assert_eq!(recovered.entry.as_ref().unwrap().phase,NativeEntryPhase::SpotExposed);
            let present=recovered.entry.as_ref().unwrap().spot_prehedge_bills_evidence.is_some();
            assert_eq!(present,phase=="after");
            if present{
                recovered.entry.as_ref().unwrap()
                    .require_spot_prehedge_bills_provenance().unwrap();
            }else{
                let report=r32d6c3_scan(&store,recovered.generation,
                    &R32D6C3Mock::new(R32D6C3Case::Good)).unwrap();
                let (saved,r)=store.record_native_spot_prehedge_bills_evidence(report).unwrap();
                assert_eq!(r,NativeSpotBillsWrite::Recorded);
                assert_eq!(saved.generation,recovered.generation+1);
            }
            let _=fs::remove_dir_all(dir);
        }
    }
    #[test]
    fn r32d6c3_native_generation_drifts_during_bill_read_abort(){
        use crate::native_spot_prehedge_bills::{audit_okx_first_leg_cash_bills_window,SpotBillsError};
        use crate::private_rest::{OkxCredentials,OkxDomain};
        use std::sync::atomic::{AtomicBool,Ordering};
        struct InjectDrift{
            mock:R32D6C3Mock,store:NativeStateStore,generation:u64,changed:AtomicBool,
        }
        impl crate::private_https::HttpExecutor for InjectDrift{
            fn execute_request(&self,req:&crate::private_https::HttpsRequest)
                ->Result<crate::private_https::HttpResponse,crate::private_https::TransportError>{
                let result=self.mock.execute_request(req);
                if req.url.contains("/api/v5/account/bills-archive")
                    &&!self.changed.swap(true,Ordering::SeqCst){
                    self.store.transact(self.generation,|_|Ok(())).unwrap();
                }
                result
            }
        }
        let (root,store,generation)=r32d6c3_with_first_leg();
        let exec=InjectDrift{
            mock:R32D6C3Mock::new(R32D6C3Case::Good),
            store:store.clone(),generation,changed:AtomicBool::new(false),
        };
        let creds=OkxCredentials::new("FAKEKEY","FAKESECRET","FAKEPASS").unwrap();
        let audit=audit_okx_first_leg_cash_bills_window(
            &exec,&store,generation,&creds,1_780_000_010_125,OkxDomain::Global,1);
        assert!(matches!(audit,Err(SpotBillsError::State(NativeStateError::Generation))));
        assert_eq!(exec.mock.count(),5);
        let latest=store.load().unwrap().unwrap();
        assert_eq!(latest.generation,generation+1);
        assert!(latest.entry.unwrap().spot_prehedge_bills_evidence.is_none());
        let _=fs::remove_dir_all(root);
    }

    fn r32d6c5_audit(
        store:&NativeStateStore,generation:u64,
        fake:&R32D6B2BJointMock
    )->Result<crate::native_futures_entry_audit::FuturesEntryWindowReport,
            crate::native_futures_entry_audit::FuturesEntryWindowError>{
        use crate::native_futures_entry_audit::audit_futures_entry_window_once;
        let creds=crate::private_rest::BinanceCredentials::new(
            "FAKE_BIN","FAKE_SECRET").unwrap();
        audit_futures_entry_window_once(
            fake,store,generation,&creds,1_780_000_010_125,10_000,1,10,
        )
    }
    #[test]
    fn r32d6c5_two_binance_account_checks_symbol_trades_durable_reopen(){
        let (root,store,generation)=r32d5b_seed_futures_checkpoint();
        let fake=R32D6B2BJointMock::new(
            R32D6B2Case::Good,R32D5BCase::Good,R32D6Case::Complete,false);
        let original=fs::read(root.join("state.json")).unwrap();
        let report=r32d6c5_audit(&store,generation,&fake).unwrap();
        assert_eq!(fake.count(),27);
        assert_eq!(fake.binance.count(),26);
        assert_eq!(fake.trades.count(),1);
        assert_eq!(fake.spot.count(),0);
        assert_eq!(report.trade_count,2);
        assert_eq!(report.commission_count,2);
        assert_eq!(report.account_checks,2);
        assert!(report.exact_read_only_candidate);
        assert!(!report.may_send_order);
        assert!(!report.may_promote_hedged);
        assert_eq!(fs::read(root.join("state.json")).unwrap(),original);
        let (saved,write)=store.record_native_futures_entry_window(report.clone()).unwrap();
        assert_eq!(write,NativeFuturesEntryWindowWrite::Recorded);
        assert_eq!(saved.generation,generation+1);
        let entry=saved.entry.as_ref().unwrap();
        assert_eq!(entry.phase,NativeEntryPhase::RecoveryRequired);
        assert_eq!(entry.futures_filled_atoms_e18,0);
        let p=entry.futures_entry_window_evidence.as_ref().unwrap();
        assert_eq!(p.futures_order_id,"124");
        assert_eq!(p.trade_ids,vec!["1001","1002"]);
        assert_eq!(p.fill_qty_atoms_e18,400_000_000_000_000_000);
        assert_eq!(p.observed_short_atoms_e18,400_000_000_000_000_000);
        assert_eq!(p.signed_quote_fee_cost_atoms_e12,40_000_000_000);
        assert_eq!(p.account_read_repetitions,2);
        assert!(!p.may_send_order);
        assert!(!p.may_promote_hedged);
        assert!(!p.exchange_global_history_complete);
        assert!(!p.cross_venue_atomicity_proven);
        let data=fs::read(root.join("state.json")).unwrap();
        let reopened=NativeStateStore::open(root.join("state.json")).unwrap();
        assert_eq!(reopened.load().unwrap().unwrap(),saved);
        let (again,status)=reopened.record_native_futures_entry_window(report).unwrap();
        assert_eq!(status,NativeFuturesEntryWindowWrite::Idempotent);
        assert_eq!(again,saved);
        assert_eq!(fs::read(root.join("state.json")).unwrap(),data);
        assert!(reopened.prepare_futures_entry_hedge(
            saved.generation,7,"forbidden","sab00000000000000000000000000851",
        ).is_err());
        let _=fs::remove_dir_all(root);
    }
    #[test]
    fn r32d6c5_absent_checkpoint_or_wrong_generation_preflight_zero_binance_get(){
        use crate::native_futures_entry_audit::FuturesEntryWindowError;
        let (root,store,_,generation)=r32d_entry_test_store(true);
        let fake=R32D6B2BJointMock::new(
            R32D6B2Case::Good,R32D5BCase::Good,R32D6Case::Complete,false);
        assert!(matches!(r32d6c5_audit(&store,generation,&fake),
            Err(FuturesEntryWindowError::Invalid(_))));
        assert!(matches!(r32d6c5_audit(&store,generation-1,&fake),
            Err(FuturesEntryWindowError::State(NativeStateError::Generation))));
        assert_eq!(fake.count(),0);
        let _=fs::remove_dir_all(root);
    }
    #[test]
    fn r32d6c5_foreign_zero_net_trade_funding_wallet_changes_and_missing_income_deny(){
        use crate::native_futures_entry_audit::FuturesEntryWindowError;
        for (bin,trades,alter) in [
            (R32D5BCase::Good,R32D6Case::ForeignRoundTrip,false),
            (R32D5BCase::FundingFee,R32D6Case::Complete,false),
            (R32D5BCase::Good,R32D6Case::MissingFill,false),
            (R32D5BCase::Good,R32D6Case::WrongFee,false),
            (R32D5BCase::WrongCommission,R32D6Case::Complete,false),
            (R32D5BCase::Good,R32D6Case::Complete,true),
            (R32D5BCase::OpenOrders,R32D6Case::Complete,false),
            (R32D5BCase::MissingCommission,R32D6Case::Complete,false),
        ]{
            let (root,store,generation)=r32d5b_seed_futures_checkpoint();
            let bytes=fs::read(root.join("state.json")).unwrap();
            let fake=R32D6B2BJointMock::new(R32D6B2Case::Good,bin,trades,alter);
            let result=r32d6c5_audit(&store,generation,&fake);
            assert!(matches!(result,
                Err(FuturesEntryWindowError::Account(_))
                |Err(FuturesEntryWindowError::Trades(_))
                |Err(FuturesEntryWindowError::Invalid(_))),
                "unexpected bin={bin:?} trades={trades:?} alter={alter}: {result:?}");
            assert_eq!(fs::read(root.join("state.json")).unwrap(),bytes);
            assert!(store.load().unwrap().unwrap().entry.unwrap()
                .futures_entry_window_evidence.is_none());
            let _=fs::remove_dir_all(root);
        }
    }
    #[test]
    fn r32d6c5_forged_proof_id_fee_flags_gen_and_qty_denied(){
        let (root,store,generation)=r32d5b_seed_futures_checkpoint();
        let mock=R32D6B2BJointMock::new(
            R32D6B2Case::Good,R32D5BCase::Good,R32D6Case::Complete,false);
        let report=r32d6c5_audit(&store,generation,&mock).unwrap();
        let (state,_) =store.record_native_futures_entry_window(report).unwrap();
        for i in 0..11{
            let mut tampered=state.clone();
            let p=tampered.entry.as_mut().unwrap()
                .futures_entry_window_evidence.as_mut().unwrap();
            match i{
                0=>p.may_send_order=true,
                1=>p.may_promote_hedged=true,
                2=>p.exchange_global_history_complete=true,
                3=>p.cross_venue_atomicity_proven=true,
                4=>p.source_generation=state.generation,
                5=>p.futures_order_id="WRONG".into(),
                6=>p.trade_ids.push("1002".into()),
                7=>p.signed_quote_fee_cost_atoms_e12=1,
                8=>p.fill_qty_atoms_e18=1,
                9=>p.observed_short_atoms_e18=1,
                10=>p.account_read_repetitions=1,
                _=>unreachable!(),
            }
            assert!(tampered.validate().is_err(),"futures forgery case={i} allowed");
        }
        assert_eq!(store.load().unwrap().unwrap(),state);
        let _=fs::remove_dir_all(root);
    }
    #[test]
    fn r32d6c5_concurrent_writers_only_one_futures_fee_proof_and_idempotent(){
        use std::thread;
        let (root,store,generation)=r32d5b_seed_futures_checkpoint();
        let mock=R32D6B2BJointMock::new(
            R32D6B2Case::Good,R32D5BCase::Good,R32D6Case::Complete,false);
        let report=r32d6c5_audit(&store,generation,&mock).unwrap();
        let s1=store.clone();let s2=store.clone();
        let copy=report.clone();
        let a=thread::spawn(move||s1.record_native_futures_entry_window(report));
        let b=thread::spawn(move||s2.record_native_futures_entry_window(copy));
        let r=[a.join().unwrap(),b.join().unwrap()];
        assert_eq!(r.iter().filter(|x|matches!(
            x,Ok((_,NativeFuturesEntryWindowWrite::Recorded)))).count(),1);
        assert_eq!(r.iter().filter(|x|matches!(
            x,Ok((_,NativeFuturesEntryWindowWrite::Idempotent)))).count(),1);
        assert_eq!(store.load().unwrap().unwrap().generation,generation+1);
        let _=fs::remove_dir_all(root);
    }
    #[test]
    fn r32d6c5_failed_disk_write_old_checkpoint_preserved_then_exact_retry(){
        let (root,store,generation)=r32d5b_seed_futures_checkpoint();
        let mock=R32D6B2BJointMock::new(
            R32D6B2Case::Good,R32D5BCase::Good,R32D6Case::Complete,false);
        let report=r32d6c5_audit(&store,generation,&mock).unwrap();
        let bytes=fs::read(root.join("state.json")).unwrap();
        let blocker=root.join("state.tmp");
        fs::create_dir(&blocker).unwrap();
        assert!(matches!(store.record_native_futures_entry_window(report.clone()),
            Err(NativeStateError::Io(_))));
        assert_eq!(fs::read(root.join("state.json")).unwrap(),bytes);
        fs::remove_dir(&blocker).unwrap();
        let (saved,status)=store.record_native_futures_entry_window(report).unwrap();
        assert_eq!(status,NativeFuturesEntryWindowWrite::Recorded);
        assert_eq!(saved.generation,generation+1);
        let _=fs::remove_dir_all(root);
    }
    #[test]
    fn r32d6c5_stale_generation_after_27_gets_refuses_durable_result(){
        let (root,store,generation)=r32d5b_seed_futures_checkpoint();
        let mock=R32D6B2BJointMock::new(
            R32D6B2Case::Good,R32D5BCase::Good,R32D6Case::Complete,false);
        let report=r32d6c5_audit(&store,generation,&mock).unwrap();
        store.transact(generation,|_|Ok(())).unwrap();
        let data=fs::read(root.join("state.json")).unwrap();
        assert!(matches!(store.record_native_futures_entry_window(report),
            Err(NativeStateError::Generation)));
        assert_eq!(fs::read(root.join("state.json")).unwrap(),data);
        let _=fs::remove_dir_all(root);
    }

    #[test]
    fn r32d6c5_native_generation_changes_mid_binance_scope_scan_rejected(){
        use std::sync::atomic::{AtomicBool,Ordering};
        use crate::native_futures_entry_audit::{audit_futures_entry_window_once,FuturesEntryWindowError};
        struct DriftingFutures{
            fake:R32D6B2BJointMock,store:NativeStateStore,
            generation:u64,changed:AtomicBool,
        }
        impl crate::private_https::HttpExecutor for DriftingFutures{
            fn execute_request(&self,req:&crate::private_https::HttpsRequest)
                ->Result<crate::private_https::HttpResponse,crate::private_https::TransportError>{
                let response=self.fake.execute_request(req);
                if req.url.contains("/fapi/v1/userTrades")
                    &&!self.changed.swap(true,Ordering::SeqCst){
                    self.store.transact(self.generation,|_|Ok(())).unwrap();
                }
                response
            }
        }
        let (root,store,generation)=r32d5b_seed_futures_checkpoint();
        let audit=DriftingFutures{
            fake:R32D6B2BJointMock::new(
                R32D6B2Case::Good,R32D5BCase::Good,R32D6Case::Complete,false),
            store:store.clone(),generation,changed:AtomicBool::new(false),
        };
        let creds=crate::private_rest::BinanceCredentials::new(
            "FAKE_BIN","FAKE_SECRET").unwrap();
        let result=audit_futures_entry_window_once(
            &audit,&store,generation,&creds,1_780_000_010_125,10_000,1,10);
        assert!(matches!(result,Err(FuturesEntryWindowError::Trades(
            crate::native_futures_trade_scope::FuturesTradeScopeError::State(
                NativeStateError::Generation)))));
        assert_eq!(audit.fake.spot.count(),0);
        assert_eq!(audit.fake.trades.count(),1);
        let current=store.load().unwrap().unwrap();
        assert_eq!(current.generation,generation+1);
        assert!(current.entry.unwrap().futures_entry_window_evidence.is_none());
        let _=fs::remove_dir_all(root);
    }
    #[test]
    fn r32d6c5_abrupt_child_exit_before_or_after_durable_futures_proof(){
        let Ok(target)=std::env::var("R32D6C5_CHILD_ROOT")else{return};
        let phase=std::env::var("R32D6C5_CHILD_PHASE").unwrap();
        let (root,store,generation)=r32d5b_seed_futures_checkpoint();
        let mock=R32D6B2BJointMock::new(
            R32D6B2Case::Good,R32D5BCase::Good,R32D6Case::Complete,false);
        let report=r32d6c5_audit(&store,generation,&mock).unwrap();
        let dest=PathBuf::from(target);
        fs::create_dir_all(&dest).unwrap();
        fs::write(dest.join("state.json"),
            fs::read(root.join("state.json")).unwrap()).unwrap();
        File::open(dest.as_path()).unwrap().sync_all().unwrap();
        let writer=NativeStateStore::open(dest.join("state.json")).unwrap();
        if phase=="after"{
            let (saved,r)=writer.record_native_futures_entry_window(report).unwrap();
            assert_eq!(r,NativeFuturesEntryWindowWrite::Recorded);
            assert_eq!(saved.generation,generation+1);
        }else{assert_eq!(phase,"before");}
        std::process::exit(79);
    }
    #[test]
    fn r32d6c5_real_child_process_exit_returns_complete_old_or_new_journal(){
        for phase in ["before","after"]{
            let dir=root(&format!("r32d6c5-crash-{phase}"));
            fs::create_dir_all(&dir).unwrap();
            let status=std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("authoritative_state::tests::r32d6c5_abrupt_child_exit_before_or_after_durable_futures_proof")
                .env("R32D6C5_CHILD_ROOT",&dir)
                .env("R32D6C5_CHILD_PHASE",phase)
                .status().unwrap();
            assert_eq!(status.code(),Some(79));
            let store=NativeStateStore::open(dir.join("state.json")).unwrap();
            let reopened=store.load().unwrap().unwrap();
            assert_eq!(reopened.entry.as_ref().unwrap().phase,NativeEntryPhase::RecoveryRequired);
            assert!(reopened.active.is_none());
            assert_eq!(reopened.entry.as_ref().unwrap()
                .futures_entry_window_evidence.is_some(),phase=="after");
            if phase=="before"{
                let mock=R32D6B2BJointMock::new(
                    R32D6B2Case::Good,R32D5BCase::Good,R32D6Case::Complete,false);
                let again=r32d6c5_audit(&store,reopened.generation,&mock).unwrap();
                let (saved,r)=store.record_native_futures_entry_window(again).unwrap();
                assert_eq!(r,NativeFuturesEntryWindowWrite::Recorded);
                assert_eq!(saved.generation,reopened.generation+1);
            }else{
                assert_eq!(reopened.entry.as_ref().unwrap()
                    .futures_entry_window_evidence.as_ref().unwrap().account_read_repetitions,2);
            }
            let _=fs::remove_dir_all(dir);
        }
    }
    fn r32d6c6_seed_prospective_native_two_legs()->(PathBuf,NativeStateStore,u64){
        use crate::native_entry_recovery::{probe_native_entry_readonly_once,probe_native_entry_fills_readonly_once};
        use crate::private_rest::{OkxCredentials,BinanceCredentials,OkxDomain};
        // New prospective Native entry, not historical cumulative-only Spot.
        let (root,store,initial)=r32d6c3_with_first_leg();
        let spot=store.load().unwrap().unwrap();
        assert_eq!(spot.generation,initial);
        let full_bills=r32d6c3_scan(&store,initial,
            &R32D6C3Mock::new(R32D6C3Case::Good)).unwrap();
        let (certified,_) =store.record_native_spot_prehedge_bills_evidence(full_bills).unwrap();
        assert!(certified.entry.as_ref().unwrap().spot_prehedge_bills_evidence.is_some());
        let client="sab00000000000000000000000000871";
        let (prepared,permit)=store.prepare_futures_entry_hedge(
            certified.generation,7,"r32d6c6-futures",client).unwrap();
        let unknown=store.record_futures_entry_observation(
            permit,0,None,false,false,true).unwrap();
        assert_eq!(unknown.entry.as_ref().unwrap().phase,NativeEntryPhase::RecoveryRequired);
        let okx=OkxCredentials::new("FAKE_OKX","FAKE_SECRET","FAKE_PASS").unwrap();
        let bin=BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap();
        let fake=R32DReadOnlyFixture::new(client,true,R32DCase::LineageTwoFills);
        let order=probe_native_entry_readonly_once(
            &fake,&store,unknown.generation,&okx,&bin,
            1_700_000_000_123,OkxDomain::Global,10_000).unwrap();
        let fills=probe_native_entry_fills_readonly_once(
            &fake,&store,&order,&okx,&bin,
            1_700_000_000_123,OkxDomain::Global,10_000).unwrap();
        assert_eq!(fills.fill_count,2);
        let (cp,w)=store.record_native_entry_recovery_checkpoint(fills).unwrap();
        assert_eq!(w,NativeEntryRecoveryWrite::Recorded);
        assert_eq!(cp.entry.as_ref().unwrap().phase,NativeEntryPhase::RecoveryRequired);
        let mock=R32D6B2BJointMock::new(
            R32D6B2Case::Good,R32D5BCase::Good,R32D6Case::Complete,false);
        let report=r32d6c5_audit(&store,cp.generation,&mock).unwrap();
        let (scanned,write)=store.record_native_futures_entry_window(report).unwrap();
        assert_eq!(write,NativeFuturesEntryWindowWrite::Recorded);
        assert_eq!(scanned.entry.as_ref().unwrap().phase,NativeEntryPhase::RecoveryRequired);
        assert!(scanned.entry.as_ref().unwrap().spot_prehedge_evidence.is_some());
        assert!(scanned.entry.as_ref().unwrap().spot_prehedge_bills_evidence.is_some());
        assert!(scanned.entry.as_ref().unwrap().futures_entry_window_evidence.is_some());
        assert_eq!(prepared.entry.as_ref().unwrap().phase,NativeEntryPhase::FuturesPrepared);
        (root,store,scanned.generation)
    }

    #[test]
    fn r32d6c6_prospective_c2_c3_checkpoint_c5_same_native_entry_fixture(){
        let (root,store,generation)=r32d6c6_seed_prospective_native_two_legs();
        let snapshot=store.load().unwrap().unwrap();
        assert_eq!(snapshot.generation,generation);
        assert_eq!(snapshot.entry.unwrap().phase,NativeEntryPhase::RecoveryRequired);
        let _=fs::remove_dir_all(root);
    }

    struct R32D6C6Mock{
        okx:R32D6C3Mock,
        futures:R32D6B2BJointMock,
    }
    impl R32D6C6Mock{
        fn new(okx:R32D6C3Case,bin:R32D5BCase,
            trades:R32D6Case,late_wallet_change:bool)->Self{
            Self{okx:R32D6C3Mock::new(okx),
                futures:R32D6B2BJointMock::new(
                    R32D6B2Case::Good,bin,trades,late_wallet_change)}
        }
        fn count(&self)->usize{self.okx.count()+self.futures.count()}
    }
    impl crate::private_https::HttpExecutor for R32D6C6Mock{
        fn execute_request(&self,req:&crate::private_https::HttpsRequest)
            ->Result<crate::private_https::HttpResponse,crate::private_https::TransportError>{
            assert_eq!(req.method,crate::private_rest::HttpMethod::Get);
            assert!(!req.mutation);
            let url=url::Url::parse(&req.url).unwrap();
            if url.path().starts_with("/api/v5/"){
                self.okx.execute_request(req)
            }else{self.futures.execute_request(req)}
        }
    }
    fn r32d6c6_audit(store:&NativeStateStore,generation:u64,exec:&R32D6C6Mock,
        now_ms:u64,
    )->Result<crate::native_prospective_joint::ProspectiveJointReport,
             crate::native_prospective_joint::ProspectiveJointError>{
        use crate::native_prospective_joint::audit_prospective_joint_chain_once;
        let okx=crate::private_rest::OkxCredentials::new(
            "FAKE_OKX","FAKE_SECRET","FAKE_PASS").unwrap();
        let bin=crate::private_rest::BinanceCredentials::new(
            "FAKE_BIN","FAKE_SECRET").unwrap();
        audit_prospective_joint_chain_once(
            exec,store,generation,&okx,&bin,now_ms,
            crate::private_rest::OkxDomain::Global,10_000,1,10,
        )
    }
    #[test]
    fn r32d6c6_two_venues_same_native_entry_29_signed_gets_exact_residual_durable(){
        let (root,store,generation)=r32d6c6_seed_prospective_native_two_legs();
        let mock=R32D6C6Mock::new(
            R32D6C3Case::Good,R32D5BCase::Good,R32D6Case::Complete,false);
        let original=fs::read(root.join("state.json")).unwrap();
        let report=r32d6c6_audit(&store,generation,&mock,1_780_000_010_125).unwrap();
        assert_eq!(mock.count(),29);
        assert_eq!(mock.okx.count(),2);
        assert_eq!(mock.futures.binance.count(),26);
        assert_eq!(mock.futures.trades.count(),1);
        assert_eq!(report.spot_fill_count,2);
        assert_eq!(report.futures_fill_count,2);
        assert_eq!(report.okx_balances_read,2);
        assert_eq!(report.binance_account_checks,2);
        assert!(report.candidate_bound_to_same_native_entry);
        assert_eq!(report.spot_unhedged_atoms_e18,600_000_000_000_000_000);
        assert!(!report.may_send_order);
        assert!(!report.may_promote_hedged);
        assert_eq!(fs::read(root.join("state.json")).unwrap(),original);
        let (saved,write)=store.record_native_prospective_joint_window(report.clone()).unwrap();
        assert_eq!(write,NativeProspectiveJointWrite::Recorded);
        assert_eq!(saved.generation,generation+1);
        assert!(saved.active.is_none());
        let entry=saved.entry.as_ref().unwrap();
        assert_eq!(entry.phase,NativeEntryPhase::RecoveryRequired);
        assert_eq!(entry.futures_filled_atoms_e18,0);
        let proof=entry.prospective_joint_evidence.as_ref().unwrap();
        assert_eq!(proof.spot_trade_ids,vec!["T1","T2"]);
        assert_eq!(proof.spot_bill_ids,vec!["300","301"]);
        assert_eq!(proof.futures_trade_ids,vec!["1001","1002"]);
        assert_eq!(proof.futures_order_id,"124");
        assert_eq!(proof.exposed_spot_unhedged_atoms_e18,600_000_000_000_000_000);
        assert!(!proof.may_promote_hedged);
        assert!(!proof.may_send_order);
        assert!(!proof.exchange_global_history_complete);
        assert!(!proof.cross_venue_exchange_snapshot_atomicity_proven);
        let bytes=fs::read(root.join("state.json")).unwrap();
        let reopened=NativeStateStore::open(root.join("state.json")).unwrap();
        assert_eq!(reopened.load().unwrap().unwrap(),saved);
        let (again,kind)=reopened.record_native_prospective_joint_window(report).unwrap();
        assert_eq!(kind,NativeProspectiveJointWrite::Idempotent);
        assert_eq!(again,saved);
        assert_eq!(fs::read(root.join("state.json")).unwrap(),bytes);
        assert!(reopened.prepare_futures_entry_hedge(saved.generation,7,
            "forbidden","sab00000000000000000000000000888").is_err());
        let _=fs::remove_dir_all(root);
    }
    #[test]
    fn r32d6c6_missing_okx_sources_and_stale_generation_zero_external_gets(){
        use crate::native_prospective_joint::ProspectiveJointError;
        let (root,store,generation)=r32d5b_seed_futures_checkpoint();
        let mock=R32D6C6Mock::new(
            R32D6C3Case::Good,R32D5BCase::Good,R32D6Case::Complete,false);
        assert!(matches!(r32d6c6_audit(&store,generation,&mock,1_780_000_010_125),
            Err(ProspectiveJointError::Invalid(_))));
        assert!(matches!(r32d6c6_audit(&store,generation-1,&mock,1_780_000_010_125),
            Err(ProspectiveJointError::State(NativeStateError::Generation))));
        assert_eq!(mock.count(),0);
        let _=fs::remove_dir_all(root);
    }
    #[test]
    fn r32d6c6_foreign_trades_income_wallet_and_base_balance_changes_fail_closed(){
        use crate::native_prospective_joint::ProspectiveJointError;
        for (okx,bin,trades,late) in [
            (R32D6C3Case::ChangedCash,R32D5BCase::Good,R32D6Case::Complete,false),
            (R32D6C3Case::UpdatedDuringRead,R32D5BCase::Good,R32D6Case::Complete,false),
            (R32D6C3Case::Good,R32D5BCase::FundingFee,R32D6Case::Complete,false),
            (R32D6C3Case::Good,R32D5BCase::Good,R32D6Case::ForeignRoundTrip,false),
            (R32D6C3Case::Good,R32D5BCase::WrongCommission,R32D6Case::Complete,false),
            (R32D6C3Case::Good,R32D5BCase::Good,R32D6Case::Complete,true),
            (R32D6C3Case::Good,R32D5BCase::OpenOrders,R32D6Case::Complete,false),
        ]{
            let (root,store,generation)=r32d6c6_seed_prospective_native_two_legs();
            let original=fs::read(root.join("state.json")).unwrap();
            let mock=R32D6C6Mock::new(okx,bin,trades,late);
            let result=r32d6c6_audit(&store,generation,&mock,1_780_000_010_125);
            assert!(matches!(result,
                Err(ProspectiveJointError::Invalid(_))
                |Err(ProspectiveJointError::Income(_))
                |Err(ProspectiveJointError::Trades(_))),
                "case {okx:?} {bin:?} {trades:?} {late}={result:?}");
            assert!(mock.count()>0);
            assert_eq!(fs::read(root.join("state.json")).unwrap(),original);
            assert!(store.load().unwrap().unwrap().entry.unwrap()
                .prospective_joint_evidence.is_none());
            let _=fs::remove_dir_all(root);
        }
    }
    #[test]
    fn r32d6c6_source_receipts_expire_before_any_get(){
        use crate::native_prospective_joint::ProspectiveJointError;
        let (root,store,generation)=r32d6c6_seed_prospective_native_two_legs();
        let fake=R32D6C6Mock::new(
            R32D6C3Case::Good,R32D5BCase::Good,R32D6Case::Complete,false);
        let stale=r32d6c6_audit(&store,generation,&fake,1_780_000_010_125+60_001);
        assert!(matches!(stale,Err(ProspectiveJointError::Invalid(_))));
        assert_eq!(fake.count(),0);
        let _=fs::remove_dir_all(root);
    }
    #[test]
    fn r32d6c6_joint_receipt_forged_flags_fee_order_and_unhedged_rejected(){
        let (root,store,generation)=r32d6c6_seed_prospective_native_two_legs();
        let mock=R32D6C6Mock::new(
            R32D6C3Case::Good,R32D5BCase::Good,R32D6Case::Complete,false);
        let report=r32d6c6_audit(&store,generation,&mock,1_780_000_010_125).unwrap();
        let (state,_) =store.record_native_prospective_joint_window(report).unwrap();
        for case in 0..12{
            let mut forged=state.clone();
            let proof=forged.entry.as_mut().unwrap()
                .prospective_joint_evidence.as_mut().unwrap();
            match case{
                0=>proof.may_send_order=true,
                1=>proof.may_promote_hedged=true,
                2=>proof.exchange_global_history_complete=true,
                3=>proof.cross_venue_exchange_snapshot_atomicity_proven=true,
                4=>proof.source_generation=state.generation,
                5=>proof.futures_order_id="FAKE".into(),
                6=>proof.spot_order_id="FAKE".into(),
                7=>proof.spot_fee_quote_atoms_e12=1,
                8=>proof.futures_fee_quote_atoms_e12=1,
                9=>proof.exposed_spot_unhedged_atoms_e18=0,
                10=>proof.futures_trade_ids.push("1002".into()),
                11=>proof.spot_bill_ids.push("301".into()),
                _=>unreachable!(),
            }
            assert!(forged.validate().is_err(),"forged case={case} accepted");
        }
        assert_eq!(store.load().unwrap().unwrap(),state);
        let _=fs::remove_dir_all(root);
    }
    #[test]
    fn r32d6c6_concurrent_writers_only_one_cas_revision_no_duplicate_evidence(){
        use std::thread;
        let (root,store,generation)=r32d6c6_seed_prospective_native_two_legs();
        let mock=R32D6C6Mock::new(
            R32D6C3Case::Good,R32D5BCase::Good,R32D6Case::Complete,false);
        let report=r32d6c6_audit(&store,generation,&mock,1_780_000_010_125).unwrap();
        let s1=store.clone();let s2=store.clone();
        let other=report.clone();
        let a=thread::spawn(move||s1.record_native_prospective_joint_window(report));
        let b=thread::spawn(move||s2.record_native_prospective_joint_window(other));
        let outcomes=[a.join().unwrap(),b.join().unwrap()];
        assert_eq!(outcomes.iter().filter(|o|matches!(
            o,Ok((_,NativeProspectiveJointWrite::Recorded)))).count(),1);
        assert_eq!(outcomes.iter().filter(|o|matches!(
            o,Ok((_,NativeProspectiveJointWrite::Idempotent)))).count(),1);
        assert_eq!(store.load().unwrap().unwrap().generation,generation+1);
        let _=fs::remove_dir_all(root);
    }
    #[test]
    fn r32d6c6_failed_temp_write_does_not_modify_old_recovery_checkpoint(){
        let (root,store,generation)=r32d6c6_seed_prospective_native_two_legs();
        let mock=R32D6C6Mock::new(
            R32D6C3Case::Good,R32D5BCase::Good,R32D6Case::Complete,false);
        let report=r32d6c6_audit(&store,generation,&mock,1_780_000_010_125).unwrap();
        let bytes=fs::read(root.join("state.json")).unwrap();
        let blocker=root.join("state.tmp");
        fs::create_dir(&blocker).unwrap();
        assert!(matches!(store.record_native_prospective_joint_window(report.clone()),
            Err(NativeStateError::Io(_))));
        assert_eq!(fs::read(root.join("state.json")).unwrap(),bytes);
        fs::remove_dir(&blocker).unwrap();
        let (saved,w)=store.record_native_prospective_joint_window(report).unwrap();
        assert_eq!(w,NativeProspectiveJointWrite::Recorded);
        assert_eq!(saved.generation,generation+1);
        let _=fs::remove_dir_all(root);
    }
    #[test]
    fn r32d6c6_native_generation_drift_after_read_refuses_to_record(){
        let (root,store,generation)=r32d6c6_seed_prospective_native_two_legs();
        let mock=R32D6C6Mock::new(
            R32D6C3Case::Good,R32D5BCase::Good,R32D6Case::Complete,false);
        let report=r32d6c6_audit(&store,generation,&mock,1_780_000_010_125).unwrap();
        store.transact(generation,|_|Ok(())).unwrap();
        let old=fs::read(root.join("state.json")).unwrap();
        assert!(matches!(store.record_native_prospective_joint_window(report),
            Err(NativeStateError::Generation)));
        assert_eq!(fs::read(root.join("state.json")).unwrap(),old);
        let _=fs::remove_dir_all(root);
    }
    #[test]
    fn r32d6c6_native_generation_drift_during_binance_trades_deny_before_record(){
        use std::sync::atomic::{AtomicBool,Ordering};
        use crate::native_prospective_joint::{audit_prospective_joint_chain_once,ProspectiveJointError};
        struct DriftingJoint{
            mock:R32D6C6Mock,store:NativeStateStore,gen:u64,done:AtomicBool,
        }
        impl crate::private_https::HttpExecutor for DriftingJoint{
            fn execute_request(&self,req:&crate::private_https::HttpsRequest)
                ->Result<crate::private_https::HttpResponse,crate::private_https::TransportError>{
                let response=self.mock.execute_request(req);
                if req.url.contains("/fapi/v1/userTrades")
                    &&!self.done.swap(true,Ordering::SeqCst){
                    self.store.transact(self.gen,|_|Ok(())).unwrap();
                }
                response
            }
        }
        let (root,store,generation)=r32d6c6_seed_prospective_native_two_legs();
        let mock=DriftingJoint{
            mock:R32D6C6Mock::new(
                R32D6C3Case::Good,R32D5BCase::Good,R32D6Case::Complete,false),
            store:store.clone(),gen:generation,done:AtomicBool::new(false),
        };
        let okx=crate::private_rest::OkxCredentials::new(
            "FAKE_OKX","FAKE_SECRET","FAKE_PASS").unwrap();
        let bin=crate::private_rest::BinanceCredentials::new(
            "FAKE_BIN","FAKE_SECRET").unwrap();
        let scanned=audit_prospective_joint_chain_once(
            &mock,&store,generation,&okx,&bin,1_780_000_010_125,
            crate::private_rest::OkxDomain::Global,10_000,1,10);
        assert!(matches!(scanned,Err(ProspectiveJointError::Trades(
            crate::native_futures_trade_scope::FuturesTradeScopeError::State(
                NativeStateError::Generation)))));
        let loaded=store.load().unwrap().unwrap();
        assert_eq!(loaded.generation,generation+1);
        assert!(loaded.entry.unwrap().prospective_joint_evidence.is_none());
        let _=fs::remove_dir_all(root);
    }
    #[test]
    fn r32d6c6_child_exit_before_after_durable_joint_commit(){
        let Ok(destination)=std::env::var("R32D6C6_CHILD_ROOT")else{return};
        let phase=std::env::var("R32D6C6_CHILD_PHASE").unwrap();
        let (root,store,generation)=r32d6c6_seed_prospective_native_two_legs();
        let mock=R32D6C6Mock::new(
            R32D6C3Case::Good,R32D5BCase::Good,R32D6Case::Complete,false);
        let proof=r32d6c6_audit(&store,generation,&mock,1_780_000_010_125).unwrap();
        let target=PathBuf::from(destination);
        fs::create_dir_all(&target).unwrap();
        fs::write(target.join("state.json"),fs::read(root.join("state.json")).unwrap()).unwrap();
        File::open(target.as_path()).unwrap().sync_all().unwrap();
        let copied=NativeStateStore::open(target.join("state.json")).unwrap();
        if phase=="after"{
            let (saved,w)=copied.record_native_prospective_joint_window(proof).unwrap();
            assert_eq!(w,NativeProspectiveJointWrite::Recorded);
            assert_eq!(saved.generation,generation+1);
        }else{assert_eq!(phase,"before")}
        std::process::exit(80);
    }
    #[test]
    fn r32d6c6_subprocess_crash_recovers_old_or_full_cross_venue_proof(){
        for phase in ["before","after"]{
            let root=root(&format!("r32d6c6-crash-{phase}"));
            fs::create_dir_all(&root).unwrap();
            let outcome=std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("authoritative_state::tests::r32d6c6_child_exit_before_after_durable_joint_commit")
                .env("R32D6C6_CHILD_ROOT",&root)
                .env("R32D6C6_CHILD_PHASE",phase)
                .status().unwrap();
            assert_eq!(outcome.code(),Some(80));
            let store=NativeStateStore::open(root.join("state.json")).unwrap();
            let state=store.load().unwrap().unwrap();
            assert_eq!(state.entry.as_ref().unwrap().phase,NativeEntryPhase::RecoveryRequired);
            assert!(state.active.is_none());
            assert_eq!(state.entry.as_ref().unwrap()
                .prospective_joint_evidence.is_some(),phase=="after");
            if phase=="after"{
                let evidence=state.entry.as_ref().unwrap()
                    .prospective_joint_evidence.as_ref().unwrap();
                assert_eq!(evidence.exposed_spot_unhedged_atoms_e18,600_000_000_000_000_000);
                assert!(!evidence.may_send_order);
                assert!(!evidence.may_promote_hedged);
            }
            let _=fs::remove_dir_all(root);
        }
    }

    #[test]
    fn r30_full_entry_handshake_promotes_atomically_to_hedged(){
        let dir=root("r30-happy");let store=NativeStateStore::open(dir.join("state.json")).unwrap();
        store.create(&empty()).unwrap();

        let (s2,spot_permit)=store.begin_native_entry(
            1,entry_plan(),pristine_entry_economics(),"spot-entry-1","c-spot-entry-1"
        ).unwrap();
        assert_eq!(s2.generation,2);
        assert_eq!(s2.entry.as_ref().unwrap().phase,NativeEntryPhase::SpotPrepared);
        assert_eq!(spot_permit.generation(),2);
        assert_eq!(spot_permit.leg(),NativeEntryLeg::Spot);

        let recovery=store.load_for_recovery().unwrap().unwrap();
        assert_eq!(recovery.entry.as_ref().unwrap().phase,NativeEntryPhase::RecoveryRequired);
        assert_eq!(store.load().unwrap().unwrap().entry.as_ref().unwrap().phase,NativeEntryPhase::SpotPrepared);

        let s3=store.record_spot_entry_observation(
            spot_permit,1_000_000_000_000_000_000,
            Some(decimal_to_atoms("100").unwrap()),true,false,false
        ).unwrap();
        assert_eq!(s3.generation,3);
        assert_eq!(s3.entry.as_ref().unwrap().phase,NativeEntryPhase::SpotExposed);

        let (s4,fut_permit)=store.prepare_futures_entry_hedge(
            3,7,"fut-entry-1","c-fut-entry-1"
        ).unwrap();
        assert_eq!(s4.generation,4);
        assert_eq!(s4.entry.as_ref().unwrap().phase,NativeEntryPhase::FuturesPrepared);

        let s5=store.record_futures_entry_observation(
            fut_permit,1_000_000_000_000_000_000,
            Some(decimal_to_atoms("101").unwrap()),true,false,false
        ).unwrap();
        assert_eq!(s5.generation,5);
        assert!(s5.entry.is_none());
        let active=s5.active.as_ref().unwrap();
        assert_eq!(active.lifecycle.state,LifecycleState::Hedged);
        assert_eq!(active.core.spot_net_atoms_e18,1_000_000_000_000_000_000);
        assert_eq!(active.core.futures_target_atoms_e18,1_000_000_000_000_000_000);
        assert_eq!(active.intents.len(),2);
        assert!(active.intents.iter().all(|x|x.state==IntentState::Acknowledged));
        let _=fs::remove_dir_all(dir);
    }

    #[test]
    fn r30_partial_spot_and_two_futures_hedges_only_remaining_exposure(){
        let dir=root("r30-partial");let store=NativeStateStore::open(dir.join("state.json")).unwrap();
        store.create(&empty()).unwrap();
        let (_,spot_permit)=store.begin_native_entry(
            1,entry_plan(),pristine_entry_economics(),"spot-p","c-spot-p"
        ).unwrap();
        let s3=store.record_spot_entry_observation(
            spot_permit,500_000_000_000_000_000,
            Some(decimal_to_atoms("100").unwrap()),true,false,false
        ).unwrap();
        assert_eq!(s3.entry.as_ref().unwrap().spot_filled_atoms_e18,500_000_000_000_000_000);

        let (_,f1)=store.prepare_futures_entry_hedge(3,7,"fut-p1","c-fut-p1").unwrap();
        let s5=store.record_futures_entry_observation(
            f1,300_000_000_000_000_000,Some(decimal_to_atoms("101").unwrap()),true,false,false
        ).unwrap();
        let e5=s5.entry.as_ref().unwrap();
        assert_eq!(e5.phase,NativeEntryPhase::SpotExposed);
        assert_eq!(e5.futures_filled_atoms_e18,300_000_000_000_000_000);

        let (_,f2)=store.prepare_futures_entry_hedge(5,7,"fut-p2","c-fut-p2").unwrap();
        let s7=store.record_futures_entry_observation(
            f2,200_000_000_000_000_000,Some(decimal_to_atoms("101").unwrap()),true,false,false
        ).unwrap();
        assert!(s7.entry.is_none());
        let active=s7.active.unwrap();
        assert_eq!(active.core.spot_net_atoms_e18,500_000_000_000_000_000);
        assert_eq!(active.core.futures_target_atoms_e18,500_000_000_000_000_000);
        let _=fs::remove_dir_all(dir);
    }

    #[test]
    fn r30_unknown_spot_is_recovery_and_cannot_prepare_futures(){
        let dir=root("r30-unknown");let store=NativeStateStore::open(dir.join("state.json")).unwrap();
        store.create(&empty()).unwrap();
        let (_,permit)=store.begin_native_entry(
            1,entry_plan(),pristine_entry_economics(),"spot-u","c-spot-u"
        ).unwrap();
        let s3=store.record_spot_entry_observation(permit,0,None,false,false,true).unwrap();
        assert_eq!(s3.entry.as_ref().unwrap().phase,NativeEntryPhase::RecoveryRequired);
        assert_eq!(s3.entry.as_ref().unwrap().intents[0].state,IntentState::Unknown);
        assert_eq!(
            store.prepare_futures_entry_hedge(3,7,"fut-u","c-fut-u").unwrap_err(),
            NativeStateError::Invalid("futures prepare wrong phase")
        );
        let _=fs::remove_dir_all(dir);
    }

    #[test]
    fn r30_confirmed_spot_reject_clears_entry_without_exposure(){
        let dir=root("r30-reject");let store=NativeStateStore::open(dir.join("state.json")).unwrap();
        store.create(&empty()).unwrap();
        let (_,permit)=store.begin_native_entry(
            1,entry_plan(),pristine_entry_economics(),"spot-r","c-spot-r"
        ).unwrap();
        let s3=store.record_spot_entry_observation(permit,0,None,true,true,false).unwrap();
        assert!(s3.entry.is_none());
        assert!(s3.active.is_none());
        let _=fs::remove_dir_all(dir);
    }

    #[test]
    fn r30_stale_safety_blocks_futures_prepare_without_state_change(){
        let dir=root("r30-safety");let store=NativeStateStore::open(dir.join("state.json")).unwrap();
        store.create(&empty()).unwrap();
        let (_,permit)=store.begin_native_entry(
            1,entry_plan(),pristine_entry_economics(),"spot-s","c-spot-s"
        ).unwrap();
        let s3=store.record_spot_entry_observation(
            permit,1_000_000_000_000_000_000,Some(decimal_to_atoms("100").unwrap()),true,false,false
        ).unwrap();
        assert_eq!(
            store.prepare_futures_entry_hedge(3,8,"fut-s","c-fut-s").unwrap_err(),
            NativeStateError::Invalid("stale safety generation")
        );
        assert_eq!(store.load().unwrap().unwrap(),s3);
        let _=fs::remove_dir_all(dir);
    }

    #[test]
    fn r30_old_permit_is_invalid_after_generation_changes(){
        let dir=root("r30-permit");let store=NativeStateStore::open(dir.join("state.json")).unwrap();
        store.create(&empty()).unwrap();
        let (_,permit)=store.begin_native_entry(
            1,entry_plan(),pristine_entry_economics(),"spot-old","c-spot-old"
        ).unwrap();
        let (s3,_)=store.transact(2,|_|Ok::<_,NativeStateError>(())).unwrap();
        assert_eq!(s3.generation,3);
        assert_eq!(
            store.record_spot_entry_observation(
                permit,1_000_000_000_000_000_000,Some(decimal_to_atoms("100").unwrap()),true,false,false
            ).unwrap_err(),
            NativeStateError::Generation
        );
        assert_eq!(store.load().unwrap().unwrap(),s3);
        let _=fs::remove_dir_all(dir);
    }

    #[test]
    fn r30_nonterminal_spot_fill_cannot_start_futures(){
        let dir=root("r30-spot-open");let store=NativeStateStore::open(dir.join("state.json")).unwrap();
        store.create(&empty()).unwrap();
        let (_,permit)=store.begin_native_entry(
            1,entry_plan(),pristine_entry_economics(),"spot-open","c-spot-open"
        ).unwrap();
        let s3=store.record_spot_entry_observation(
            permit,400_000_000_000_000_000,Some(decimal_to_atoms("100").unwrap()),false,false,false
        ).unwrap();
        assert_eq!(s3.entry.as_ref().unwrap().phase,NativeEntryPhase::SpotPrepared);
        assert_eq!(
            store.prepare_futures_entry_hedge(3,7,"fut-too-early","c-fut-too-early").unwrap_err(),
            NativeStateError::Invalid("futures prepare wrong phase")
        );
        let _=fs::remove_dir_all(dir);
    }

    #[test]
    fn r30_unknown_futures_is_recovery_only(){
        let dir=root("r30-fut-unknown");let store=NativeStateStore::open(dir.join("state.json")).unwrap();
        store.create(&empty()).unwrap();
        let (_,sp)=store.begin_native_entry(
            1,entry_plan(),pristine_entry_economics(),"spot-fu","c-spot-fu"
        ).unwrap();
        store.record_spot_entry_observation(
            sp,1_000_000_000_000_000_000,Some(decimal_to_atoms("100").unwrap()),true,false,false
        ).unwrap();
        let (_,fp)=store.prepare_futures_entry_hedge(3,7,"fut-u2","c-fut-u2").unwrap();
        let s5=store.record_futures_entry_observation(fp,0,None,false,false,true).unwrap();
        assert_eq!(s5.entry.as_ref().unwrap().phase,NativeEntryPhase::RecoveryRequired);
        assert!(s5.active.is_none());
        let _=fs::remove_dir_all(dir);
    }

    #[test]
    fn r30_confirmed_futures_reject_returns_to_spot_exposed(){
        let dir=root("r30-fut-reject");let store=NativeStateStore::open(dir.join("state.json")).unwrap();
        store.create(&empty()).unwrap();
        let (_,sp)=store.begin_native_entry(
            1,entry_plan(),pristine_entry_economics(),"spot-fr","c-spot-fr"
        ).unwrap();
        store.record_spot_entry_observation(
            sp,1_000_000_000_000_000_000,Some(decimal_to_atoms("100").unwrap()),true,false,false
        ).unwrap();
        let (_,fp)=store.prepare_futures_entry_hedge(3,7,"fut-r1","c-fut-r1").unwrap();
        let s5=store.record_futures_entry_observation(fp,0,None,true,true,false).unwrap();
        assert_eq!(s5.entry.as_ref().unwrap().phase,NativeEntryPhase::SpotExposed);
        let (s6,_)=store.prepare_futures_entry_hedge(5,7,"fut-r2","c-fut-r2").unwrap();
        assert_eq!(s6.entry.as_ref().unwrap().phase,NativeEntryPhase::FuturesPrepared);
        let _=fs::remove_dir_all(dir);
    }

    #[test]
    fn r30_overhedge_is_rejected_without_persist(){
        let dir=root("r30-overhedge");let store=NativeStateStore::open(dir.join("state.json")).unwrap();
        store.create(&empty()).unwrap();
        let (_,sp)=store.begin_native_entry(
            1,entry_plan(),pristine_entry_economics(),"spot-oh","c-spot-oh"
        ).unwrap();
        store.record_spot_entry_observation(
            sp,500_000_000_000_000_000,Some(decimal_to_atoms("100").unwrap()),true,false,false
        ).unwrap();
        let (s4,fp)=store.prepare_futures_entry_hedge(3,7,"fut-oh","c-fut-oh").unwrap();
        assert_eq!(
            store.record_futures_entry_observation(
                fp,600_000_000_000_000_000,Some(decimal_to_atoms("101").unwrap()),true,false,false
            ).unwrap_err(),
            NativeStateError::Economics
        );
        assert_eq!(store.load().unwrap().unwrap(),s4);
        let _=fs::remove_dir_all(dir);
    }

    #[test]
    fn r30_reads_r29_checksum_without_entry_field(){
        #[derive(Serialize)]
        struct OldState{
            schema_version:u32,
            generation:u64,
            active:Option<NativePositionBundle>,
            legacy_source_sha256:Option<String>,
            legacy_closed_positions_ignored:u64,
        }
        #[derive(Serialize)]
        struct OldEnvelope{payload:OldState,payload_sha256:String}

        let dir=root("r29-compat");let path=dir.join("state.json");
        fs::create_dir_all(&dir).unwrap();
        let old=OldState{
            schema_version:1,generation:1,active:None,
            legacy_source_sha256:Some("22".repeat(32)),legacy_closed_positions_ignored:17,
        };
        let payload_bytes=serde_json::to_vec(&old).unwrap();
        let env=OldEnvelope{payload:old,payload_sha256:hex(&sha256(&payload_bytes))};
        fs::write(&path,serde_json::to_vec(&env).unwrap()).unwrap();
        let loaded=NativeStateStore::open(&path).unwrap().load().unwrap().unwrap();
        assert!(loaded.entry.is_none());
        assert!(loaded.active.is_none());
        assert_eq!(loaded.generation,1);
        assert_eq!(loaded.legacy_closed_positions_ignored,17);
        let _=fs::remove_dir_all(dir);
    }
}
