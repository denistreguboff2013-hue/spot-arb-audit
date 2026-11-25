//! R15 review-only native position lifecycle / close / recovery.
//!
//! Contract source: audited Python strategy.py / position_state.py /
//! ai/observe.py / ai/exchange_truth.py. No real REST transport is present.
//! Every close mutation is still fenced by the certified R14 durable gate.

use crate::execution::{Action as ExecAction, ExecError, ExecutionState, IntentState, Leg, Resolution};
use crate::execution_adapter::{
    authorize_exact, futures_unwind, spot_unwind, AdapterError, OrderIntent, OrderKind,
    ResponseClass, Venue,
};
use crate::execution_store::DurableMutationGate;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
};

pub const CLOSED_CONFIRMATIONS_REQUIRED: u8 = 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LifecycleState {
    Hedged,
    ClosingFutures,
    ClosingSpot,
    AwaitingFlatConfirmation,
    VerifyingExitTruth,
    RecoveryRequired,
    VerifiedClosed,
    Closed,
    ManualRequired,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LifecycleAction {
    None,
    CloseFuturesReduceOnly,
    CloseSpotMarket,
    CheckCloseTruth,
    VerifyExitTruth,
    ReconcilePending,
    Finalize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CloseLeg {
    Futures,
    Spot,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BasicTruth {
    Hedged,
    Closed,
    OrphanSpot,
    OrphanFutures,
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExitTruth {
    VerifiedClosed,
    PartialObserved,
    Mismatch,
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ManualReason {
    OrphanFutures,
    TruthUnknown,
    TruthMismatch,
    TruthPartial,
    MutationRejected,
    MutationUnknown,
    NonTerminalAck,
    ZeroFill,
    Overfill,
    StaleSafety,
    DurableFailure,
    InvalidRecoveryEvidence,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingCloseIntent {
    pub intent_id: String,
    pub client_id: String,
    pub leg: CloseLeg,
    pub requested_units: u128,
    // Durable terminal source quantity, written before intent Reconciled.
    // None remains compatible with previously persisted pending snapshots.
    #[serde(default)]
    pub recovery_terminal_filled_units: Option<u128>,
    // Caller-supplied local order ID, persisted write-once while quarantined.
    // It does NOT authenticate the exchange or grant retry permission.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_original_exchange_order_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PositionLifecycle {
    pub state: LifecycleState,
    pub target_units: u128,
    pub futures_filled_units: u128,
    pub spot_filled_units: u128,
    pub safety_generation: u64,
    pub closed_observations: u8,
    pub pending: Option<PendingCloseIntent>,
    pub manual_reason: Option<ManualReason>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LifecycleError {
    BadAmount,
    WrongState,
    StaleSafety,
    PendingMutation,
    MissingEvidence,
    IntentMismatch,
    InvalidObservation,
    Adapter(AdapterError),
    Execution(ExecError),
    Io(io::ErrorKind),
}

impl From<AdapterError> for LifecycleError {
    fn from(value: AdapterError) -> Self {
        Self::Adapter(value)
    }
}
impl From<ExecError> for LifecycleError {
    fn from(value: ExecError) -> Self {
        Self::Execution(value)
    }
}

impl From<io::Error> for LifecycleError {
    fn from(value: io::Error) -> Self {
        Self::Io(value.kind())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloseNetworkPlan {
    pub network_action: ExecAction,
    pub intent: OrderIntent,
    pub reduce_only: bool,
    pub kind: OrderKind,
    pub requested_units: u128,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedClose {
    pub intent_id:String,
    pub client_id:String,
    pub leg:CloseLeg,
    pub intent:OrderIntent,
    pub reduce_only:bool,
    pub kind:OrderKind,
    pub requested_units:u128,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MutationObservation {
    pub class: ResponseClass,
    pub terminal: bool,
    pub filled_units: u128,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveryEvidence {
    ConfirmedNotFound,
    ConfirmedNoFill,
    ConfirmedRejected,
    ConfirmedFill { filled_units: u128, terminal: bool },
    StillOpen,
    Unknown,
    Mismatch,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CloseTriggerInput {
    pub emergency: bool,
    pub amount_units: u128,
    pub lifetime_sec: u64,
    pub min_lifetime_sec: u64,
    pub close_spread_bps: Option<f64>,
    pub exit_bps: f64,
    pub velocity_bps_s: Option<f64>,
    pub hold_positive_bps_s: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CloseDecision {
    Wait,
    Hold,
    CloseNormal,
    CloseEmergency,
    ManualRequired,
}

pub fn evaluate_close_trigger(i: CloseTriggerInput) -> CloseDecision {
    // R96: a zero-sized or unknown position must never acquire close authority.
    if i.amount_units == 0 {
        return CloseDecision::ManualRequired;
    }
    // Emergency can bypass a missing/stale book, but not an absent exposure.
    if i.emergency {
        return CloseDecision::CloseEmergency;
    }
    // Fail closed on invalid public numerical signals and configured targets.
    // NaN in spread previously bypassed the comparison and caused CloseNormal.
    if !i.exit_bps.is_finite() || !i.hold_positive_bps_s.is_finite()
        || i.close_spread_bps.is_some_and(|s| !s.is_finite())
        || i.velocity_bps_s.is_some_and(|v| !v.is_finite())
    {
        return CloseDecision::ManualRequired;
    }
    if i.lifetime_sec < i.min_lifetime_sec {
        return CloseDecision::Wait;
    }
    let Some(spread) = i.close_spread_bps else {
        return CloseDecision::Wait;
    };
    if spread > i.exit_bps {
        return CloseDecision::Wait;
    }
    if i.velocity_bps_s
        .map(|v| v >= i.hold_positive_bps_s && spread >= i.exit_bps - 3.0)
        .unwrap_or(false)
    {
        return CloseDecision::Hold;
    }
    CloseDecision::CloseNormal
}

impl PositionLifecycle {
    pub fn new_hedged(target_units: u128, safety_generation: u64) -> Result<Self, LifecycleError> {
        if target_units == 0 {
            return Err(LifecycleError::BadAmount);
        }
        Ok(Self {
            state: LifecycleState::Hedged,
            target_units,
            futures_filled_units: 0,
            spot_filled_units: 0,
            safety_generation,
            closed_observations: 0,
            pending: None,
            manual_reason: None,
        })
    }

    pub fn start_close(&mut self) -> Result<(), LifecycleError> {
        if self.pending.is_some() {
            return Err(LifecycleError::PendingMutation);
        }
        if self.state != LifecycleState::Hedged {
            return Err(LifecycleError::WrongState);
        }
        self.state = LifecycleState::ClosingFutures;
        self.closed_observations = 0;
        Ok(())
    }

    pub fn next_action(&self) -> LifecycleAction {
        if self.pending.is_some() || self.state == LifecycleState::RecoveryRequired {
            return LifecycleAction::ReconcilePending;
        }
        match self.state {
            LifecycleState::Hedged | LifecycleState::Closed | LifecycleState::ManualRequired => {
                LifecycleAction::None
            }
            LifecycleState::ClosingFutures => LifecycleAction::CloseFuturesReduceOnly,
            LifecycleState::ClosingSpot => LifecycleAction::CloseSpotMarket,
            LifecycleState::AwaitingFlatConfirmation => LifecycleAction::CheckCloseTruth,
            LifecycleState::VerifyingExitTruth => LifecycleAction::VerifyExitTruth,
            LifecycleState::VerifiedClosed => LifecycleAction::Finalize,
            LifecycleState::RecoveryRequired => LifecycleAction::ReconcilePending,
        }
    }

    pub fn remaining_units(&self, leg: CloseLeg) -> u128 {
        let filled = match leg {
            CloseLeg::Futures => self.futures_filled_units,
            CloseLeg::Spot => self.spot_filled_units,
        };
        self.target_units.saturating_sub(filled)
    }

    pub fn observe_basic_truth(&mut self, truth: BasicTruth) -> Result<(), LifecycleError> {
        if self.pending.is_some() {
            self.state = LifecycleState::RecoveryRequired;
            return Err(LifecycleError::PendingMutation);
        }
        match truth {
            BasicTruth::Hedged => {
                self.closed_observations = 0;
                match self.state {
                    LifecycleState::Hedged | LifecycleState::ClosingFutures => Ok(()),
                    _ => {
                        self.set_manual(ManualReason::TruthMismatch);
                        Err(LifecycleError::InvalidObservation)
                    }
                }
            }
            BasicTruth::Closed => {
                self.closed_observations = self.closed_observations.saturating_add(1);
                if self.closed_observations >= CLOSED_CONFIRMATIONS_REQUIRED {
                    self.state = LifecycleState::VerifyingExitTruth;
                } else {
                    self.state = LifecycleState::AwaitingFlatConfirmation;
                }
                Ok(())
            }
            BasicTruth::OrphanSpot => {
                self.closed_observations = 0;
                self.futures_filled_units = self.target_units;
                self.state = LifecycleState::ClosingSpot;
                Ok(())
            }
            BasicTruth::OrphanFutures => {
                self.set_manual(ManualReason::OrphanFutures);
                Ok(())
            }
            BasicTruth::Unknown => {
                self.set_manual(ManualReason::TruthUnknown);
                Ok(())
            }
        }
    }

    pub fn apply_exit_truth(&mut self, truth: ExitTruth) -> Result<(), LifecycleError> {
        if self.pending.is_some() {
            self.state = LifecycleState::RecoveryRequired;
            return Err(LifecycleError::PendingMutation);
        }
        match truth {
            ExitTruth::VerifiedClosed => {
                self.state = LifecycleState::VerifiedClosed;
                self.manual_reason = None;
            }
            ExitTruth::PartialObserved => self.set_manual(ManualReason::TruthPartial),
            ExitTruth::Mismatch => self.set_manual(ManualReason::TruthMismatch),
            ExitTruth::Unknown => self.set_manual(ManualReason::TruthUnknown),
        }
        Ok(())
    }

    pub fn finalize(&mut self) -> Result<(), LifecycleError> {
        if self.state != LifecycleState::VerifiedClosed || self.pending.is_some() {
            return Err(LifecycleError::WrongState);
        }
        self.state = LifecycleState::Closed;
        Ok(())
    }

    fn current_leg(&self) -> Result<CloseLeg, LifecycleError> {
        match self.state {
            LifecycleState::ClosingFutures => Ok(CloseLeg::Futures),
            LifecycleState::ClosingSpot => Ok(CloseLeg::Spot),
            _ => Err(LifecycleError::WrongState),
        }
    }

    fn order_for(
        &self,
        leg: CloseLeg,
        symbol: &str,
        client_id: &str,
        requested_units: u128,
    ) -> (OrderIntent, bool, OrderKind, Leg) {
        match leg {
            CloseLeg::Futures => {
                let p = futures_unwind();
                (
                    OrderIntent {
                        venue: Venue::BinanceUsdm,
                        symbol: symbol.into(),
                        side: p.side.into(),
                        amount: requested_units.to_string(),
                        client_id: client_id.into(),
                    },
                    p.reduce_only,
                    p.kind,
                    Leg::Futures,
                )
            }
            CloseLeg::Spot => {
                let p = spot_unwind();
                (
                    OrderIntent {
                        venue: Venue::OkxSpot,
                        symbol: symbol.into(),
                        side: p.side.into(),
                        amount: requested_units.to_string(),
                        client_id: client_id.into(),
                    },
                    p.reduce_only,
                    p.kind,
                    Leg::Spot,
                )
            }
        }
    }

    pub(crate) fn prepare_close_atomic(
        &mut self,
        execution:&mut ExecutionState,
        intent_id:&str,
        client_id:&str,
        symbol:&str,
        generation_before_prepare:u64,
    )->Result<PreparedClose,LifecycleError>{
        if self.pending.is_some(){return Err(LifecycleError::PendingMutation)}
        if generation_before_prepare!=self.safety_generation{
            self.set_manual(ManualReason::StaleSafety);
            return Err(LifecycleError::StaleSafety)
        }
        let leg=self.current_leg()?;
        let requested_units=self.remaining_units(leg);
        if requested_units==0{return Err(LifecycleError::WrongState)}
        let (intent,reduce_only,kind,_)=self.order_for(leg,symbol,client_id,requested_units);
        authorize_exact(&intent,true,client_id,self.safety_generation,generation_before_prepare)?;
        execution.prepare(intent_id,true,Some(client_id))?;
        self.pending=Some(PendingCloseIntent{
            intent_id:intent_id.into(),client_id:client_id.into(),leg,requested_units,
            recovery_terminal_filled_units:None,
            recovery_original_exchange_order_id:None,
        });
        Ok(PreparedClose{
            intent_id:intent_id.into(),client_id:client_id.into(),leg,intent,
            reduce_only,kind,requested_units,
        })
    }

    pub(crate) fn authorize_prepared_close_atomic(
        &self,
        execution:&ExecutionState,
        prepared:&PreparedClose,
        generation_at_send:u64,
    )->Result<CloseNetworkPlan,LifecycleError>{
        let pending=self.pending.as_ref().ok_or(LifecycleError::PendingMutation)?;
        if pending.intent_id!=prepared.intent_id||pending.client_id!=prepared.client_id||
            pending.leg!=prepared.leg||pending.requested_units!=prepared.requested_units{
            return Err(LifecycleError::IntentMismatch)
        }
        let durable=execution.intents.iter().find(|x|x.id==prepared.intent_id)
            .ok_or(LifecycleError::IntentMismatch)?;
        if durable.state!=IntentState::Prepared||
            durable.client_order_id.as_deref()!=Some(prepared.client_id.as_str()){
            return Err(LifecycleError::IntentMismatch)
        }
        authorize_exact(&prepared.intent,true,&prepared.client_id,self.safety_generation,generation_at_send)?;
        let (expected,expected_reduce,expected_kind,_)=self.order_for(
            pending.leg,&prepared.intent.symbol,&pending.client_id,pending.requested_units);
        if prepared.intent!=expected || prepared.reduce_only!=expected_reduce
            || prepared.kind!=expected_kind{
            return Err(LifecycleError::IntentMismatch)
        }
        let exec_leg=match prepared.leg{CloseLeg::Futures=>Leg::Futures,CloseLeg::Spot=>Leg::Spot};
        Ok(CloseNetworkPlan{
            network_action:ExecAction::NetworkMutation{intent_id:prepared.intent_id.clone(),leg:exec_leg},
            intent:prepared.intent.clone(),reduce_only:prepared.reduce_only,
            kind:prepared.kind,requested_units:prepared.requested_units,
        })
    }

    pub(crate) fn apply_network_observation_atomic(
        &mut self,
        execution:&mut ExecutionState,
        intent_id:&str,
        obs:MutationObservation,
    )->Result<(),LifecycleError>{
        let pending=self.pending.clone().ok_or(LifecycleError::PendingMutation)?;
        if pending.intent_id!=intent_id{return Err(LifecycleError::IntentMismatch)}
        match obs.class{
            ResponseClass::Unknown=>{
                execution.mark_unknown(intent_id)?;
                self.state=LifecycleState::RecoveryRequired;
                self.manual_reason=Some(ManualReason::MutationUnknown);
            }
            ResponseClass::Rejected=>{
                execution.acknowledge(intent_id,true)?;
                self.pending=None;
                self.set_manual(ManualReason::MutationRejected);
            }
            ResponseClass::Acknowledged=>{
                execution.acknowledge(intent_id,false)?;
                if !obs.terminal{
                    self.state=LifecycleState::RecoveryRequired;
                    self.manual_reason=Some(ManualReason::NonTerminalAck);
                    return Ok(())
                }
                if obs.filled_units==0{
                    self.state=LifecycleState::RecoveryRequired;
                    self.manual_reason=Some(ManualReason::ZeroFill);
                    return Ok(())
                }
                self.apply_confirmed_fill(pending.leg,pending.requested_units,obs.filled_units)?;
                self.pending=None;self.manual_reason=None;
            }
        }
        Ok(())
    }

    pub(crate) fn recover_pending_atomic(
        &mut self,
        execution:&mut ExecutionState,
        evidence_present:bool,
        evidence:RecoveryEvidence,
    )->Result<(),LifecycleError>{
        if !evidence_present{return Err(LifecycleError::MissingEvidence)}
        let pending=self.pending.clone().ok_or(LifecycleError::PendingMutation)?;
        let intent=execution.intents.iter().find(|x|x.id==pending.intent_id)
            .ok_or(LifecycleError::IntentMismatch)?.clone();
        if matches!(intent.state,IntentState::Prepared|IntentState::Unknown){
            // A still-open or zero-fill observation is not final exchange truth.
            // Do not mark the durable intent Reconciled before terminal evidence.
            if let RecoveryEvidence::ConfirmedFill{terminal:false,..}=evidence{
                self.state=LifecycleState::RecoveryRequired;
                self.manual_reason=Some(ManualReason::NonTerminalAck);
                return Ok(())
            }
            if let RecoveryEvidence::ConfirmedFill{filled_units:0,..}=evidence{
                self.state=LifecycleState::RecoveryRequired;
                self.manual_reason=Some(ManualReason::ZeroFill);
                return Ok(())
            }
            if matches!(evidence,RecoveryEvidence::Unknown|RecoveryEvidence::Mismatch|RecoveryEvidence::StillOpen){
                if matches!(evidence,RecoveryEvidence::StillOpen){
                    self.state=LifecycleState::RecoveryRequired;self.manual_reason=None;
                }else{
                    self.set_manual(ManualReason::InvalidRecoveryEvidence);
                }
                return Ok(())
            }
            let resolution=match evidence{
                RecoveryEvidence::ConfirmedNotFound|RecoveryEvidence::ConfirmedNoFill=>Resolution::NotFound,
                RecoveryEvidence::ConfirmedRejected=>Resolution::Rejected,
                RecoveryEvidence::ConfirmedFill{..}=>Resolution::Confirmed,
                _=>Resolution::Confirmed,
            };
            execution.reconcile(&pending.intent_id,resolution,true)?;
        }
        match evidence{
            RecoveryEvidence::ConfirmedNotFound|RecoveryEvidence::ConfirmedNoFill=>{
                self.pending=None;self.manual_reason=None;
                self.state=match pending.leg{
                    CloseLeg::Futures=>LifecycleState::ClosingFutures,
                    CloseLeg::Spot=>LifecycleState::ClosingSpot,
                };
            }
            RecoveryEvidence::ConfirmedRejected=>{
                self.pending=None;self.set_manual(ManualReason::MutationRejected);
            }
            RecoveryEvidence::ConfirmedFill{filled_units,terminal}=>{
                if !terminal{
                    self.state=LifecycleState::RecoveryRequired;
                    self.manual_reason=Some(ManualReason::NonTerminalAck);
                    return Ok(())
                }
                if filled_units==0{
                    self.state=LifecycleState::RecoveryRequired;
                    self.manual_reason=Some(ManualReason::ZeroFill);
                    return Ok(())
                }
                self.apply_confirmed_fill(pending.leg,pending.requested_units,filled_units)?;
                self.pending=None;self.manual_reason=None;
            }
            RecoveryEvidence::StillOpen=>{
                self.state=LifecycleState::RecoveryRequired;self.manual_reason=None;
            }
            RecoveryEvidence::Unknown=>self.set_manual(ManualReason::TruthUnknown),
            RecoveryEvidence::Mismatch=>self.set_manual(ManualReason::TruthMismatch),
        }
        Ok(())
    }

    pub fn prepare_close_mutation(
        &mut self,
        lifecycle_store: &DurableLifecycleStore,
        gate: &mut DurableMutationGate,
        intent_id: &str,
        client_id: &str,
        symbol: &str,
        generation_before_prepare: u64,
        generation_at_send: u64,
    ) -> Result<CloseNetworkPlan, LifecycleError> {
        if self.pending.is_some() {
            return Err(LifecycleError::PendingMutation);
        }
        if generation_before_prepare != self.safety_generation {
            self.set_manual(ManualReason::StaleSafety);
            return Err(LifecycleError::StaleSafety);
        }
        let leg = self.current_leg()?;
        let requested_units = self.remaining_units(leg);
        if requested_units == 0 {
            return Err(LifecycleError::WrongState);
        }
        let (intent, reduce_only, kind, exec_leg) =
            self.order_for(leg, symbol, client_id, requested_units);

        // Preflight identity/venue syntax before creating a durable PREPARED.
        authorize_exact(
            &intent,
            true,
            client_id,
            self.safety_generation,
            generation_before_prepare,
        )?;

        // R14 physical PREPARED is written first. Network permission is still denied.
        gate.begin(intent_id, exec_leg, client_id)?;

        // Persist R15 pending intent before any network permission can be returned.
        self.pending = Some(PendingCloseIntent {
            intent_id: intent_id.into(),
            client_id: client_id.into(),
            leg,
            requested_units,
            recovery_terminal_filled_units: None,
            recovery_original_exchange_order_id: None,
        });
        if let Err(e) = lifecycle_store.persist(self) {
            self.state = LifecycleState::RecoveryRequired;
            self.manual_reason = Some(ManualReason::DurableFailure);
            return Err(LifecycleError::Io(e.kind()));
        }

        let network_action = gate.after_durable_prepare(intent_id)?;

        // Revalidate safety generation at the send boundary. If it changed, the
        // durable PREPARED + lifecycle pending record force recovery; no send.
        if let Err(e) = authorize_exact(
            &intent,
            true,
            client_id,
            self.safety_generation,
            generation_at_send,
        ) {
            self.state = LifecycleState::RecoveryRequired;
            self.manual_reason = Some(ManualReason::StaleSafety);
            let _ = lifecycle_store.persist(self);
            return Err(LifecycleError::Adapter(e));
        }

        Ok(CloseNetworkPlan {
            network_action,
            intent,
            reduce_only,
            kind,
            requested_units,
        })
    }

    pub fn apply_network_observation(
        &mut self,
        lifecycle_store: &DurableLifecycleStore,
        gate: &mut DurableMutationGate,
        intent_id: &str,
        obs: MutationObservation,
    ) -> Result<(), LifecycleError> {
        let pending = self
            .pending
            .clone()
            .ok_or(LifecycleError::PendingMutation)?;
        if pending.intent_id != intent_id {
            return Err(LifecycleError::IntentMismatch);
        }

        match obs.class {
            ResponseClass::Unknown => {
                if let Err(e) = gate.ambiguous(intent_id) {
                    self.state = LifecycleState::RecoveryRequired;
                    self.manual_reason = Some(ManualReason::DurableFailure);
                    let _ = lifecycle_store.persist(self);
                    return Err(LifecycleError::Io(e.kind()));
                }
                self.state = LifecycleState::RecoveryRequired;
                self.manual_reason = Some(ManualReason::MutationUnknown);
                lifecycle_store.persist(self)?;
                Ok(())
            }
            ResponseClass::Rejected => {
                // A non-terminal "rejection" or one accompanied by fills
                // cannot prove that the exchange left no position. Keep the
                // original intent unresolved and require cold reconciliation.
                if !obs.terminal || obs.filled_units != 0 {
                    if let Err(e) = gate.ambiguous(intent_id) {
                        self.state = LifecycleState::RecoveryRequired;
                        self.manual_reason = Some(ManualReason::DurableFailure);
                        let _ = lifecycle_store.persist(self);
                        return Err(LifecycleError::Io(e.kind()));
                    }
                    self.state = LifecycleState::RecoveryRequired;
                    self.manual_reason = Some(ManualReason::InvalidRecoveryEvidence);
                    lifecycle_store.persist(self)?;
                    return Ok(());
                }
                if let Err(e) = gate.acknowledge(intent_id, true) {
                    self.state = LifecycleState::RecoveryRequired;
                    self.manual_reason = Some(ManualReason::DurableFailure);
                    let _ = lifecycle_store.persist(self);
                    return Err(LifecycleError::Io(e.kind()));
                }
                self.pending = None;
                self.set_manual(ManualReason::MutationRejected);
                lifecycle_store.persist(self)?;
                Ok(())
            }
            ResponseClass::Acknowledged => {
                if let Err(e) = gate.acknowledge(intent_id, false) {
                    self.state = LifecycleState::RecoveryRequired;
                    self.manual_reason = Some(ManualReason::DurableFailure);
                    let _ = lifecycle_store.persist(self);
                    return Err(LifecycleError::Io(e.kind()));
                }

                // ACK is not fill truth. Non-terminal or zero-fill responses are
                // held for exchange-truth recovery instead of blind re-submit.
                if !obs.terminal {
                    self.state = LifecycleState::RecoveryRequired;
                    self.manual_reason = Some(ManualReason::NonTerminalAck);
                    lifecycle_store.persist(self)?;
                    return Ok(());
                }
                if obs.filled_units == 0 {
                    self.state = LifecycleState::RecoveryRequired;
                    self.manual_reason = Some(ManualReason::ZeroFill);
                    lifecycle_store.persist(self)?;
                    return Ok(());
                }
                // A durable ACK alone is not a completed cross-file mutation:
                // first pin exact terminal quantity, then reconcile the intent.
                // If the process dies between either fsync, cold recovery stays
                // pending and cannot send a new leg.
                self.stage_recovery_terminal_fill(lifecycle_store, obs.filled_units)?;
                gate.store.persist_reconciled(intent_id, true)?;
                self.apply_confirmed_fill(pending.leg, pending.requested_units, obs.filled_units)?;
                self.pending = None;
                self.manual_reason = None;
                lifecycle_store.persist(self)?;
                Ok(())
            }
        }
    }

    /// Read-only research recovery: pin one locally supplied order ID before
    /// comparing R15 to V2 and PAPER. The supplied ID is NOT authenticated
    /// exchange evidence. Persisted snapshots have no external CAS/antirollback.
    pub fn stage_recovery_original_exchange_order_id_review(
        &mut self, store:&DurableLifecycleStore, order_id:&str,
    )->Result<(),LifecycleError>{
        if self.state!=LifecycleState::RecoveryRequired||
           order_id.is_empty()||order_id.len()>128||
           !order_id.bytes().all(|b|b.is_ascii_alphanumeric()||b==b'-'||b==b'_'){
            return Err(LifecycleError::InvalidObservation)
        }
        let p=self.pending.as_ref().ok_or(LifecycleError::PendingMutation)?;
        if let Some(existing)=p.recovery_original_exchange_order_id.as_deref(){
            return if existing==order_id {Ok(())}
                else {Err(LifecycleError::IntentMismatch)}
        }
        let mut staged=self.clone();
        staged.pending.as_mut().unwrap().recovery_original_exchange_order_id=
            Some(order_id.to_owned());
        store.persist(&staged)?;
        *self=staged;
        Ok(())
    }

    fn stage_recovery_terminal_fill(&mut self, store:&DurableLifecycleStore, qty:u128) -> Result<(),LifecycleError> {
        let p=self.pending.as_ref().ok_or(LifecycleError::PendingMutation)?;
        if qty==0 || qty>p.requested_units { return Err(LifecycleError::InvalidObservation) }
        if let Some(old)=p.recovery_terminal_filled_units {
            return if old==qty {Ok(())} else {Err(LifecycleError::InvalidObservation)};
        }
        let mut staged=self.clone();
        staged.pending.as_mut().unwrap().recovery_terminal_filled_units=Some(qty);
        staged.state=LifecycleState::RecoveryRequired;
        store.persist(&staged)?;
        *self=staged;
        Ok(())
    }

    pub fn recover_pending(
        &mut self,
        lifecycle_store: &DurableLifecycleStore,
        gate: &mut DurableMutationGate,
        evidence_present: bool,
        evidence: RecoveryEvidence,
    ) -> Result<(), LifecycleError> {
        if !evidence_present {
            return Err(LifecycleError::MissingEvidence);
        }
        let pending = self
            .pending
            .clone()
            .ok_or(LifecycleError::PendingMutation)?;

        let intents = gate.store.load()?;
        let intent = intents
            .iter()
            .find(|x| x.id == pending.intent_id)
            .ok_or(LifecycleError::IntentMismatch)?;
        let recorded=pending.recovery_terminal_filled_units;
        if recorded.is_some() && !matches!(evidence,RecoveryEvidence::ConfirmedFill{filled_units,terminal:true} if Some(filled_units)==recorded) {
            return Err(LifecycleError::InvalidObservation);
        }

        if intent.state==IntentState::Reconciled && recorded.is_none() &&
            matches!(evidence,RecoveryEvidence::ConfirmedFill{..}) {
            return Err(LifecycleError::MissingEvidence);
        }

        // A non-terminal or zero-fill observation is NOT final exchange truth.
        // Preserve the durable PREPARED/UNKNOWN barrier across cold restart;
        // never persist Reconciled before terminal confirmation.
        if let RecoveryEvidence::ConfirmedFill { filled_units, terminal } = evidence {
            if !terminal || filled_units == 0 {
                self.state = LifecycleState::RecoveryRequired;
                self.manual_reason = Some(if !terminal {
                    ManualReason::NonTerminalAck
                } else {
                    ManualReason::ZeroFill
                });
                lifecycle_store.persist(self)?;
                return Ok(());
            }
        }
        if let RecoveryEvidence::ConfirmedFill{filled_units,terminal:true}=evidence {
            if filled_units>0 { self.stage_recovery_terminal_fill(lifecycle_store,filled_units)?; }
        }
        if matches!(intent.state, IntentState::Prepared | IntentState::Acknowledged | IntentState::Unknown) {
            if matches!(
                evidence,
                RecoveryEvidence::Unknown
                    | RecoveryEvidence::Mismatch
                    | RecoveryEvidence::StillOpen
            ) {
                if matches!(evidence, RecoveryEvidence::StillOpen) {
                    self.state = LifecycleState::RecoveryRequired;
                    self.manual_reason = None;
                } else {
                    self.set_manual(ManualReason::InvalidRecoveryEvidence);
                }
                lifecycle_store.persist(self)?;
                return Ok(());
            }
            gate.store
                .persist_reconciled(&pending.intent_id, true)
                .map_err(LifecycleError::from)?;
        }

        match evidence {
            RecoveryEvidence::ConfirmedNotFound | RecoveryEvidence::ConfirmedNoFill => {
                self.pending = None;
                self.manual_reason = None;
                self.state = match pending.leg {
                    CloseLeg::Futures => LifecycleState::ClosingFutures,
                    CloseLeg::Spot => LifecycleState::ClosingSpot,
                };
            }
            RecoveryEvidence::ConfirmedRejected => {
                self.pending = None;
                self.set_manual(ManualReason::MutationRejected);
            }
            RecoveryEvidence::ConfirmedFill {
                filled_units,
                terminal,
            } => {
                if !terminal {
                    self.state = LifecycleState::RecoveryRequired;
                    self.manual_reason = Some(ManualReason::NonTerminalAck);
                    lifecycle_store.persist(self)?;
                    return Ok(());
                }
                if filled_units == 0 {
                    self.state = LifecycleState::RecoveryRequired;
                    self.manual_reason = Some(ManualReason::ZeroFill);
                    lifecycle_store.persist(self)?;
                    return Ok(());
                }
                self.apply_confirmed_fill(pending.leg, pending.requested_units, filled_units)?;
                self.pending = None;
                self.manual_reason = None;
            }
            RecoveryEvidence::StillOpen => {
                self.state = LifecycleState::RecoveryRequired;
                self.manual_reason = None;
            }
            RecoveryEvidence::Unknown => {
                self.set_manual(ManualReason::TruthUnknown);
            }
            RecoveryEvidence::Mismatch => {
                self.set_manual(ManualReason::TruthMismatch);
            }
        }
        lifecycle_store.persist(self)?;
        Ok(())
    }

    fn apply_confirmed_fill(
        &mut self,
        leg: CloseLeg,
        requested_units: u128,
        filled_units: u128,
    ) -> Result<(), LifecycleError> {
        if filled_units > requested_units {
            self.set_manual(ManualReason::Overfill);
            return Err(LifecycleError::InvalidObservation);
        }
        match leg {
            CloseLeg::Futures => {
                let next = self
                    .futures_filled_units
                    .checked_add(filled_units)
                    .ok_or(LifecycleError::InvalidObservation)?;
                if next > self.target_units {
                    self.set_manual(ManualReason::Overfill);
                    return Err(LifecycleError::InvalidObservation);
                }
                self.futures_filled_units = next;
                self.state = if next == self.target_units {
                    LifecycleState::ClosingSpot
                } else {
                    LifecycleState::ClosingFutures
                };
            }
            CloseLeg::Spot => {
                let next = self
                    .spot_filled_units
                    .checked_add(filled_units)
                    .ok_or(LifecycleError::InvalidObservation)?;
                if next > self.target_units {
                    self.set_manual(ManualReason::Overfill);
                    return Err(LifecycleError::InvalidObservation);
                }
                self.spot_filled_units = next;
                self.state = if next == self.target_units {
                    LifecycleState::VerifyingExitTruth
                } else {
                    LifecycleState::ClosingSpot
                };
            }
        }
        Ok(())
    }

    fn set_manual(&mut self, reason: ManualReason) {
        self.state = LifecycleState::ManualRequired;
        self.manual_reason = Some(reason);
    }
}

#[derive(Debug)]
pub struct DurableLifecycleStore {
    path: PathBuf,
}

impl DurableLifecycleStore {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
            File::open(parent)?.sync_all()?;
        }
        Ok(Self { path })
    }

    pub fn load(&self) -> io::Result<Option<PositionLifecycle>> {
        if !self.path.exists() {
            return Ok(None);
        }
        let mut raw = String::new();
        File::open(&self.path)?.read_to_string(&mut raw)?;
        if raw.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "empty lifecycle store",
            ));
        }
        let value = serde_json::from_str(&raw)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Ok(Some(value))
    }

    pub fn load_for_recovery(&self) -> io::Result<Option<PositionLifecycle>> {
        let mut value = self.load()?;
        if let Some(ref mut lifecycle) = value {
            if lifecycle.pending.is_some()
                && !matches!(
                    lifecycle.state,
                    LifecycleState::Closed | LifecycleState::ManualRequired
                )
            {
                lifecycle.state = LifecycleState::RecoveryRequired;
            }
        }
        Ok(value)
    }

    pub fn persist(&self, value: &PositionLifecycle) -> io::Result<()> {
        let parent = self
            .path
            .parent()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "missing parent"))?;
        let tmp = self.path.with_extension("tmp");
        let bytes = serde_json::to_vec(value)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(&tmp, &self.path)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "spotarb-r15-{name}-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    fn ids(n: u8) -> (String, String) {
        (
            format!("close-{n}"),
            format!("sab{:029}", n),
        )
    }

    fn setup(
        name: &str,
    ) -> (
        PathBuf,
        DurableLifecycleStore,
        DurableMutationGate,
        PositionLifecycle,
    ) {
        let root = dir(name);
        let lifecycle_store = DurableLifecycleStore::open(root.join("lifecycle.json")).unwrap();
        let gate = DurableMutationGate::open(root.join("intents.json")).unwrap();
        let mut lifecycle = PositionLifecycle::new_hedged(10, 7).unwrap();
        lifecycle.start_close().unwrap();
        lifecycle_store.persist(&lifecycle).unwrap();
        (root, lifecycle_store, gate, lifecycle)
    }

    #[test]
    fn normal_close_is_futures_then_spot_then_verified_truth() {
        let (root, store, mut gate, mut p) = setup("happy");
        let (i1, c1) = ids(1);
        let a = p
            .prepare_close_mutation(&store, &mut gate, &i1, &c1, "BNT/USDT:USDT", 7, 7)
            .unwrap();
        assert!(matches!(a.network_action, ExecAction::NetworkMutation { leg: Leg::Futures, .. }));
        assert_eq!(a.intent.side, "buy");
        assert!(a.reduce_only);
        p.apply_network_observation(
            &store,
            &mut gate,
            &i1,
            MutationObservation {
                class: ResponseClass::Acknowledged,
                terminal: true,
                filled_units: 10,
            },
        )
        .unwrap();
        assert_eq!(p.next_action(), LifecycleAction::CloseSpotMarket);

        let (i2, c2) = ids(2);
        let b = p
            .prepare_close_mutation(&store, &mut gate, &i2, &c2, "BNT/USDT", 7, 7)
            .unwrap();
        assert!(matches!(b.network_action, ExecAction::NetworkMutation { leg: Leg::Spot, .. }));
        assert_eq!(b.intent.side, "sell");
        assert!(!b.reduce_only);
        p.apply_network_observation(
            &store,
            &mut gate,
            &i2,
            MutationObservation {
                class: ResponseClass::Acknowledged,
                terminal: true,
                filled_units: 10,
            },
        )
        .unwrap();
        assert_eq!(p.next_action(), LifecycleAction::VerifyExitTruth);
        p.apply_exit_truth(ExitTruth::VerifiedClosed).unwrap();
        assert_eq!(p.next_action(), LifecycleAction::Finalize);
        p.finalize().unwrap();
        assert_eq!(p.state, LifecycleState::Closed);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn partial_futures_never_opens_spot_close() {
        let (root, store, mut gate, mut p) = setup("partial-fut");
        let (i1, c1) = ids(3);
        p.prepare_close_mutation(&store, &mut gate, &i1, &c1, "BNT/USDT:USDT", 7, 7)
            .unwrap();
        p.apply_network_observation(
            &store,
            &mut gate,
            &i1,
            MutationObservation {
                class: ResponseClass::Acknowledged,
                terminal: true,
                filled_units: 4,
            },
        )
        .unwrap();
        assert_eq!(p.futures_filled_units, 4);
        assert_eq!(p.next_action(), LifecycleAction::CloseFuturesReduceOnly);
        assert_eq!(p.remaining_units(CloseLeg::Futures), 6);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn partial_spot_keeps_finishing_spot() {
        let (root, store, mut gate, mut p) = setup("partial-spot");
        p.futures_filled_units = 10;
        p.state = LifecycleState::ClosingSpot;
        store.persist(&p).unwrap();
        let (i, c) = ids(4);
        p.prepare_close_mutation(&store, &mut gate, &i, &c, "BNT/USDT", 7, 7)
            .unwrap();
        p.apply_network_observation(
            &store,
            &mut gate,
            &i,
            MutationObservation {
                class: ResponseClass::Acknowledged,
                terminal: true,
                filled_units: 3,
            },
        )
        .unwrap();
        assert_eq!(p.next_action(), LifecycleAction::CloseSpotMarket);
        assert_eq!(p.remaining_units(CloseLeg::Spot), 7);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn unknown_response_is_durable_recovery_and_never_blind_retry() {
        let (root, store, mut gate, mut p) = setup("unknown");
        let (i, c) = ids(5);
        p.prepare_close_mutation(&store, &mut gate, &i, &c, "BNT/USDT:USDT", 7, 7)
            .unwrap();
        p.apply_network_observation(
            &store,
            &mut gate,
            &i,
            MutationObservation {
                class: ResponseClass::Unknown,
                terminal: false,
                filled_units: 0,
            },
        )
        .unwrap();
        assert_eq!(p.state, LifecycleState::RecoveryRequired);
        assert_eq!(p.next_action(), LifecycleAction::ReconcilePending);
        drop(gate);
        let reopened_gate = DurableMutationGate::open(root.join("intents.json")).unwrap();
        let reopened = store.load_for_recovery().unwrap().unwrap();
        assert_eq!(reopened.state, LifecycleState::RecoveryRequired);
        assert_eq!(
            reopened_gate.store.load().unwrap()[0].state,
            IntentState::Unknown
        );
        assert!(DurableMutationGate::open(root.join("intents.json"))
            .unwrap()
            .begin("new", Leg::Futures, "sab00000000000000000000000000099")
            .is_err());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn crash_after_ack_before_lifecycle_update_cannot_resend() {
        let (root, store, mut gate, mut p) = setup("ack-crash");
        let (i, c) = ids(6);
        p.prepare_close_mutation(&store, &mut gate, &i, &c, "BNT/USDT:USDT", 7, 7)
            .unwrap();
        // Simulate network ACK and durable R14 ACK, then process death before
        // the R15 lifecycle snapshot can clear pending/apply fill.
        gate.acknowledge(&i, false).unwrap();
        drop(gate);
        let mut reopened_gate = DurableMutationGate::open(root.join("intents.json")).unwrap();
        let mut reopened = store.load_for_recovery().unwrap().unwrap();
        assert_eq!(reopened.state, LifecycleState::RecoveryRequired);
        assert_eq!(reopened.next_action(), LifecycleAction::ReconcilePending);
        assert_eq!(
            reopened_gate.store.load().unwrap()[0].state,
            IntentState::Acknowledged
        );
        assert_eq!(
            reopened_gate.begin("second", Leg::Futures, "sab00000000000000000000000000099")
                .unwrap_err().kind(), io::ErrorKind::WouldBlock
        );
        reopened
            .recover_pending(
                &store,
                &mut reopened_gate,
                true,
                RecoveryEvidence::ConfirmedFill {
                    filled_units: 10,
                    terminal: true,
                },
            )
            .unwrap();
        assert_eq!(reopened.state, LifecycleState::ClosingSpot);
        assert!(reopened.pending.is_none());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn acknowledged_terminal_quantity_fsync_crash_stays_blocked_until_cold_reconcile() {
        let (root, store, mut gate, mut p) = setup("ack-qty-fsync-crash");
        let (id, client) = ids(25);
        p.prepare_close_mutation(&store, &mut gate, &id, &client, "BNT/USDT:USDT", 7, 7).unwrap();
        // Separate fsync of placement ACK and exact receipt, then crash before
        // the intent journal's Reconciled write or lifecycle fill application.
        gate.acknowledge(&id, false).unwrap();
        p.stage_recovery_terminal_fill(&store, 4).unwrap();
        drop(gate);
        let mut cold_gate = DurableMutationGate::open(root.join("intents.json")).unwrap();
        let mut cold = store.load_for_recovery().unwrap().unwrap();
        assert_eq!(cold.pending.as_ref().unwrap().recovery_terminal_filled_units, Some(4));
        assert_eq!(cold.futures_filled_units, 0);
        assert_eq!(cold_gate.store.load().unwrap()[0].state, IntentState::Acknowledged);
        assert_eq!(cold_gate.begin("next", Leg::Futures, "sab00000000000000000000000000098")
            .unwrap_err().kind(), io::ErrorKind::WouldBlock);
        assert_eq!(cold.recover_pending(&store, &mut cold_gate, true,
            RecoveryEvidence::ConfirmedFill { filled_units: 5, terminal: true }),
            Err(LifecycleError::InvalidObservation));
        assert_eq!(cold_gate.store.load().unwrap()[0].state, IntentState::Acknowledged);
        cold.recover_pending(&store, &mut cold_gate, true,
            RecoveryEvidence::ConfirmedFill { filled_units: 4, terminal: true }).unwrap();
        assert_eq!(cold.futures_filled_units, 4);
        assert_eq!(cold.remaining_units(CloseLeg::Futures), 6);
        assert!(cold.pending.is_none());
        assert_eq!(cold_gate.store.load().unwrap()[0].state, IntentState::Reconciled);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn crash_after_prepare_reopens_recovery_only() {
        let (root, store, mut gate, mut p) = setup("prepare-crash");
        let (i, c) = ids(7);
        p.prepare_close_mutation(&store, &mut gate, &i, &c, "BNT/USDT:USDT", 7, 7)
            .unwrap();
        drop(gate);
        let reopened = store.load_for_recovery().unwrap().unwrap();
        let reopened_gate = DurableMutationGate::open(root.join("intents.json")).unwrap();
        assert_eq!(reopened.state, LifecycleState::RecoveryRequired);
        assert_eq!(
            reopened_gate.store.load().unwrap()[0].state,
            IntentState::Prepared
        );
        assert!(reopened_gate.after_durable_prepare(&i).is_err());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn safety_flip_after_prepare_denies_send_and_forces_recovery() {
        let (root, store, mut gate, mut p) = setup("safety-flip");
        let (i, c) = ids(8);
        let e = p
            .prepare_close_mutation(&store, &mut gate, &i, &c, "BNT/USDT:USDT", 7, 8)
            .unwrap_err();
        assert_eq!(e, LifecycleError::Adapter(AdapterError::StaleSafety));
        assert_eq!(p.state, LifecycleState::RecoveryRequired);
        assert_eq!(gate.store.load().unwrap()[0].state, IntentState::Prepared);
        assert_eq!(p.next_action(), LifecycleAction::ReconcilePending);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn stale_safety_before_prepare_creates_no_intent() {
        let (root, store, mut gate, mut p) = setup("stale-before");
        let (i, c) = ids(9);
        assert_eq!(
            p.prepare_close_mutation(&store, &mut gate, &i, &c, "BNT/USDT:USDT", 8, 8),
            Err(LifecycleError::StaleSafety)
        );
        assert!(gate.store.load().unwrap().is_empty());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn orphan_spot_skips_futures_mutation_and_closes_only_spot() {
        let (root, store, mut gate, mut p) = setup("orphan-spot");
        p.observe_basic_truth(BasicTruth::OrphanSpot).unwrap();
        assert_eq!(p.futures_filled_units, 10);
        assert_eq!(p.next_action(), LifecycleAction::CloseSpotMarket);
        let (i, c) = ids(10);
        let plan = p
            .prepare_close_mutation(&store, &mut gate, &i, &c, "BNT/USDT", 7, 7)
            .unwrap();
        assert_eq!(plan.intent.venue, Venue::OkxSpot);
        assert_eq!(plan.intent.side, "sell");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn orphan_futures_is_manual_and_never_auto_unwinds() {
        let (root, _store, _gate, mut p) = setup("orphan-fut");
        p.observe_basic_truth(BasicTruth::OrphanFutures).unwrap();
        assert_eq!(p.state, LifecycleState::ManualRequired);
        assert_eq!(p.manual_reason, Some(ManualReason::OrphanFutures));
        assert_eq!(p.next_action(), LifecycleAction::None);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn unknown_truth_is_manual() {
        let (root, _store, _gate, mut p) = setup("truth-unknown");
        p.observe_basic_truth(BasicTruth::Unknown).unwrap();
        assert_eq!(p.state, LifecycleState::ManualRequired);
        assert_eq!(p.manual_reason, Some(ManualReason::TruthUnknown));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn three_flat_observations_are_required_before_detailed_exit_truth() {
        let (root, _store, _gate, mut p) = setup("flat-three");
        for n in 1..=2 {
            p.observe_basic_truth(BasicTruth::Closed).unwrap();
            assert_eq!(p.closed_observations, n);
            assert_eq!(p.next_action(), LifecycleAction::CheckCloseTruth);
        }
        p.observe_basic_truth(BasicTruth::Closed).unwrap();
        assert_eq!(p.closed_observations, 3);
        assert_eq!(p.next_action(), LifecycleAction::VerifyExitTruth);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn flat_snapshot_alone_can_never_finalize() {
        let (root, _store, _gate, mut p) = setup("flat-not-final");
        p.observe_basic_truth(BasicTruth::Closed).unwrap();
        assert_ne!(p.next_action(), LifecycleAction::Finalize);
        assert_eq!(p.finalize(), Err(LifecycleError::WrongState));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn exit_truth_partial_mismatch_and_unknown_are_manual() {
        for (name, truth, reason) in [
            ("partial", ExitTruth::PartialObserved, ManualReason::TruthPartial),
            ("mismatch", ExitTruth::Mismatch, ManualReason::TruthMismatch),
            ("unknown", ExitTruth::Unknown, ManualReason::TruthUnknown),
        ] {
            let (root, _store, _gate, mut p) = setup(name);
            p.futures_filled_units = 10;
            p.spot_filled_units = 10;
            p.state = LifecycleState::VerifyingExitTruth;
            p.apply_exit_truth(truth).unwrap();
            assert_eq!(p.state, LifecycleState::ManualRequired);
            assert_eq!(p.manual_reason, Some(reason));
            let _ = fs::remove_dir_all(root);
        }
    }

    #[test]
    fn zero_fill_ack_requires_recovery_not_retry() {
        let (root, store, mut gate, mut p) = setup("zero-fill");
        let (i, c) = ids(11);
        p.prepare_close_mutation(&store, &mut gate, &i, &c, "BNT/USDT:USDT", 7, 7)
            .unwrap();
        p.apply_network_observation(
            &store,
            &mut gate,
            &i,
            MutationObservation {
                class: ResponseClass::Acknowledged,
                terminal: true,
                filled_units: 0,
            },
        )
        .unwrap();
        assert_eq!(p.state, LifecycleState::RecoveryRequired);
        assert_eq!(p.manual_reason, Some(ManualReason::ZeroFill));
        assert_eq!(p.next_action(), LifecycleAction::ReconcilePending);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn nonterminal_ack_requires_recovery_not_retry() {
        let (root, store, mut gate, mut p) = setup("nonterminal");
        let (i, c) = ids(12);
        p.prepare_close_mutation(&store, &mut gate, &i, &c, "BNT/USDT:USDT", 7, 7)
            .unwrap();
        p.apply_network_observation(
            &store,
            &mut gate,
            &i,
            MutationObservation {
                class: ResponseClass::Acknowledged,
                terminal: false,
                filled_units: 4,
            },
        )
        .unwrap();
        assert_eq!(p.state, LifecycleState::RecoveryRequired);
        assert_eq!(p.manual_reason, Some(ManualReason::NonTerminalAck));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn r15_reconciled_before_lifecycle_fsync_cold_not_found_no_fill_rejected_matrix() {
        // Unlike the existing terminal FILLED crash scenario, the 3 zero /
        // rejected outcomes have no immutable quantity to stage. If the
        // intent store fsync happens first, cold lifecycle still owns an
        // unresolved intent. Replay MUST query authoritative truth, not
        // issue a new order or infer that position is flat.
        for (index,evidence) in [
            RecoveryEvidence::ConfirmedNotFound,
            RecoveryEvidence::ConfirmedNoFill,
            RecoveryEvidence::ConfirmedRejected,
        ].into_iter().enumerate() {
            let (root,store,mut gate,mut lifecycle)=setup("r15-zero-cross-file");
            let (id,client)=ids(70+index as u8);
            lifecycle.prepare_close_mutation(
                &store,&mut gate,&id,&client,"BNT/USDT:USDT",7,7).unwrap();
            lifecycle.apply_network_observation(
                &store,&mut gate,&id,
                MutationObservation{
                    class:ResponseClass::Unknown,terminal:false,filled_units:0,
                }).unwrap();
            assert_eq!(lifecycle.next_action(),LifecycleAction::ReconcilePending);
            assert_eq!(gate.store.load().unwrap()[0].state,IntentState::Unknown);
            // Crash boundary: durable intent is Reconciled; lifecycle
            // snapshot has NOT been committed. Drop all runtime state.
            gate.store.persist_reconciled(&id,true).unwrap();
            drop(gate);
            drop(lifecycle);
            let mut cold=store.load_for_recovery().unwrap().unwrap();
            let mut reopened=DurableMutationGate::open(root.join("intents.json")).unwrap();
            assert_eq!(cold.next_action(),LifecycleAction::ReconcilePending);
            assert!(cold.pending.is_some());
            assert_eq!(cold.futures_filled_units,0);
            assert_eq!(reopened.store.load().unwrap()[0].state,IntentState::Reconciled);
            assert_eq!(cold.recover_pending(&store,&mut reopened,false,evidence),
                Err(LifecycleError::MissingEvidence));
            assert!(store.load_for_recovery().unwrap().unwrap().pending.is_some());
            assert_eq!(cold.next_action(),LifecycleAction::ReconcilePending);
            // New independent evidence from source is still required.
            cold.recover_pending(&store,&mut reopened,true,evidence).unwrap();
            let reread=store.load_for_recovery().unwrap().unwrap();
            assert!(reread.pending.is_none());
            assert_eq!(reread.futures_filled_units,0);
            match evidence {
                RecoveryEvidence::ConfirmedRejected=>{
                    assert_eq!(reread.state,LifecycleState::ManualRequired);
                    assert_eq!(reread.manual_reason,Some(ManualReason::MutationRejected));
                    assert_eq!(reread.next_action(),LifecycleAction::None);
                },
                RecoveryEvidence::ConfirmedNotFound|RecoveryEvidence::ConfirmedNoFill=>{
                    assert_eq!(reread.state,LifecycleState::ClosingFutures);
                    assert_eq!(reread.next_action(),LifecycleAction::CloseFuturesReduceOnly);
                },
                _=>unreachable!(),
            }
            // A second reconciliation cannot synthesize fills or a second
            // intent from the same already reconciled exchange outcome.
            let mut again=reread;
            assert_eq!(again.recover_pending(&store,&mut reopened,true,evidence),
                Err(LifecycleError::PendingMutation));
            fs::remove_dir_all(root).unwrap();
        }
    }

    #[test]
    fn evidence_not_found_releases_recovery_for_same_leg() {
        let (root, store, mut gate, mut p) = setup("not-found");
        let (i, c) = ids(13);
        p.prepare_close_mutation(&store, &mut gate, &i, &c, "BNT/USDT:USDT", 7, 7)
            .unwrap();
        p.apply_network_observation(
            &store,
            &mut gate,
            &i,
            MutationObservation {
                class: ResponseClass::Unknown,
                terminal: false,
                filled_units: 0,
            },
        )
        .unwrap();
        p.recover_pending(
            &store,
            &mut gate,
            true,
            RecoveryEvidence::ConfirmedNotFound,
        )
        .unwrap();
        assert!(p.pending.is_none());
        assert_eq!(p.state, LifecycleState::ClosingFutures);
        assert_eq!(p.next_action(), LifecycleAction::CloseFuturesReduceOnly);
        assert_eq!(gate.store.load().unwrap()[0].state, IntentState::Reconciled);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn confirmed_terminal_no_fill_releases_same_leg_without_blind_resend() {
        let (root, store, mut gate, mut p) = setup("no-fill-terminal");
        let (i, c) = ids(19);
        p.prepare_close_mutation(&store, &mut gate, &i, &c, "BNT/USDT:USDT", 7, 7)
            .unwrap();
        p.apply_network_observation(
            &store,
            &mut gate,
            &i,
            MutationObservation {
                class: ResponseClass::Acknowledged,
                terminal: false,
                filled_units: 0,
            },
        )
        .unwrap();
        assert_eq!(p.state, LifecycleState::RecoveryRequired);
        p.recover_pending(&store, &mut gate, true, RecoveryEvidence::ConfirmedNoFill)
            .unwrap();
        assert!(p.pending.is_none());
        assert_eq!(p.state, LifecycleState::ClosingFutures);
        assert_eq!(p.next_action(), LifecycleAction::CloseFuturesReduceOnly);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn still_open_recovery_keeps_pending_and_never_reissues() {
        let (root, store, mut gate, mut p) = setup("still-open");
        let (i, c) = ids(20);
        p.prepare_close_mutation(&store, &mut gate, &i, &c, "BNT/USDT:USDT", 7, 7)
            .unwrap();
        p.apply_network_observation(
            &store,
            &mut gate,
            &i,
            MutationObservation {
                class: ResponseClass::Acknowledged,
                terminal: false,
                filled_units: 0,
            },
        )
        .unwrap();
        p.recover_pending(&store, &mut gate, true, RecoveryEvidence::StillOpen)
            .unwrap();
        assert!(p.pending.is_some());
        assert_eq!(p.state, LifecycleState::RecoveryRequired);
        assert_eq!(p.next_action(), LifecycleAction::ReconcilePending);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn reconciled_crash_window_keeps_exact_fill_amount_until_cold_apply() {
        let (root, store, mut gate, mut p)=setup("reconciled-crash");
        let (id,client)=ids(22);
        p.prepare_close_mutation(&store,&mut gate,&id,&client,"BNT/USDT:USDT",7,7).unwrap();
        p.apply_network_observation(&store,&mut gate,&id,MutationObservation{
            class:ResponseClass::Unknown,terminal:false,filled_units:0
        }).unwrap();
        p.stage_recovery_terminal_fill(&store,4).unwrap();
        assert_eq!(store.load().unwrap().unwrap().pending.unwrap().recovery_terminal_filled_units,Some(4));
        gate.store.persist_reconciled(&id,true).unwrap();
        drop(gate);
        let mut cold=store.load_for_recovery().unwrap().unwrap();
        let mut reopen=DurableMutationGate::open(root.join("intents.json")).unwrap();
        assert_eq!(cold.futures_filled_units,0);
        assert_eq!(cold.next_action(),LifecycleAction::ReconcilePending);
        assert!(cold.pending.is_some());
        assert_eq!(cold.recover_pending(&store,&mut reopen,true,RecoveryEvidence::ConfirmedFill{
            filled_units:4,terminal:true}),Ok(()));
        assert_eq!(cold.futures_filled_units,4);
        assert_eq!(cold.remaining_units(CloseLeg::Futures),6);
        assert_eq!(cold.state,LifecycleState::ClosingFutures);
        assert!(cold.pending.is_none());
        let mut reread=store.load_for_recovery().unwrap().unwrap();
        assert_eq!(reread.futures_filled_units,4);
        assert!(reread.pending.is_none());
        assert_eq!(reread.recover_pending(&store,&mut reopen,true,RecoveryEvidence::ConfirmedFill{
            filled_units:4,terminal:true}),Err(LifecycleError::PendingMutation));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn cold_recovery_rejects_changed_terminal_quantity_before_or_after_reconcile() {
        for reconciled in [false,true] {
            let (root,store,mut gate,mut p)=setup("recovery-mismatch");
            let (id,client)=ids(23);
            p.prepare_close_mutation(&store,&mut gate,&id,&client,"BNT/USDT:USDT",7,7).unwrap();
            p.apply_network_observation(&store,&mut gate,&id,MutationObservation{
                class:ResponseClass::Unknown,terminal:false,filled_units:0
            }).unwrap();
            p.stage_recovery_terminal_fill(&store,4).unwrap();
            if reconciled {gate.store.persist_reconciled(&id,true).unwrap();}
            drop(gate);
            let mut cold=store.load_for_recovery().unwrap().unwrap();
            let mut reopen=DurableMutationGate::open(root.join("intents.json")).unwrap();
            assert_eq!(cold.recover_pending(&store,&mut reopen,true,RecoveryEvidence::ConfirmedFill{
                filled_units:6,terminal:true}),Err(LifecycleError::InvalidObservation));
            assert_eq!(cold.futures_filled_units,0);
            assert_eq!(cold.pending.as_ref().unwrap().recovery_terminal_filled_units,Some(4));
            let disk=store.load_for_recovery().unwrap().unwrap();
            assert_eq!(disk.futures_filled_units,0);
            assert_eq!(disk.pending.unwrap().recovery_terminal_filled_units,Some(4));
            cold.recover_pending(&store,&mut reopen,true,RecoveryEvidence::ConfirmedFill{
                filled_units:4,terminal:true}).unwrap();
            assert_eq!(cold.futures_filled_units,4);
            fs::remove_dir_all(root).unwrap();
        }
    }

    #[test]
    fn failed_terminal_receipt_fsync_does_not_promote_memory_or_intent() {
        let (root,store,gate,mut p)=setup("receipt-io-failure");
        let (id,client)=ids(24);
        let mut gate=gate;
        p.prepare_close_mutation(&store,&mut gate,&id,&client,"BNT/USDT:USDT",7,7).unwrap();
        fs::create_dir(root.join("lifecycle.tmp")).unwrap();
        assert!(matches!(p.stage_recovery_terminal_fill(&store,4),Err(LifecycleError::Io(_))));
        assert_eq!(p.pending.as_ref().unwrap().recovery_terminal_filled_units,None);
        assert_eq!(store.load().unwrap().unwrap().pending.unwrap().recovery_terminal_filled_units,None);
        assert_eq!(gate.store.load().unwrap()[0].state,IntentState::Prepared);
        fs::remove_dir(root.join("lifecycle.tmp")).unwrap();
        p.stage_recovery_terminal_fill(&store,4).unwrap();
        assert_eq!(store.load().unwrap().unwrap().pending.unwrap().recovery_terminal_filled_units,Some(4));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn nonterminal_recovery_cannot_clear_durable_unknown_or_prepared() {
        for unknown in [false, true] {
            for (terminal, filled_units, reason) in [
                (false, 4, ManualReason::NonTerminalAck),
                (true, 0, ManualReason::ZeroFill),
            ] {
                let (root, store, mut gate, mut p) = setup("nonterminal-recovery");
                let (id, client) = ids(21);
                p.prepare_close_mutation(&store, &mut gate, &id, &client, "BNT/USDT:USDT", 7, 7)
                    .unwrap();
                if unknown {
                    p.apply_network_observation(
                        &store, &mut gate, &id,
                        MutationObservation {
                            class: ResponseClass::Unknown,
                            terminal: false, filled_units: 0,
                        },
                    ).unwrap();
                }
                p.recover_pending(
                    &store, &mut gate, true,
                    RecoveryEvidence::ConfirmedFill { filled_units, terminal },
                ).unwrap();
                assert_eq!(p.state, LifecycleState::RecoveryRequired);
                assert_eq!(p.manual_reason, Some(reason));
                assert!(p.pending.is_some());
                let expected = if unknown { IntentState::Unknown } else { IntentState::Prepared };
                assert_eq!(gate.store.load().unwrap()[0].state, expected);
                drop(gate);
                let mut reopened_gate = DurableMutationGate::open(root.join("intents.json")).unwrap();
                assert_eq!(reopened_gate.store.load().unwrap()[0].state, expected);
                assert_eq!(
                    reopened_gate.begin("second", Leg::Futures, "sab00000000000000000000000000022")
                        .unwrap_err().kind(),
                    io::ErrorKind::WouldBlock,
                );
                let recovered = store.load_for_recovery().unwrap().unwrap();
                assert!(recovered.pending.is_some());
                assert_eq!(recovered.next_action(), LifecycleAction::ReconcilePending);
                fs::remove_dir_all(root).unwrap();
            }
        }
    }

    #[test]
    fn recovery_requires_evidence() {
        let (root, store, mut gate, mut p) = setup("evidence");
        let (i, c) = ids(14);
        p.prepare_close_mutation(&store, &mut gate, &i, &c, "BNT/USDT:USDT", 7, 7)
            .unwrap();
        assert_eq!(
            p.recover_pending(
                &store,
                &mut gate,
                false,
                RecoveryEvidence::ConfirmedNotFound
            ),
            Err(LifecycleError::MissingEvidence)
        );
        assert!(p.pending.is_some());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn malformed_lifecycle_store_fails_closed() {
        let root = dir("corrupt");
        let path = root.join("lifecycle.json");
        let store = DurableLifecycleStore::open(&path).unwrap();
        fs::write(&path, b"{bad").unwrap();
        assert_eq!(store.load().unwrap_err().kind(), io::ErrorKind::InvalidData);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn nonterminal_or_nonzero_rejection_remains_unknown_after_cold_restart() {
        for (terminal, filled_units) in [(false, 0), (true, 4)] {
            let (root, store, mut gate, mut p) = setup("ambiguous-rejection");
            let (id, client) = ids(26);
            p.prepare_close_mutation(&store, &mut gate, &id, &client,
                "BNT/USDT:USDT", 7, 7).unwrap();
            p.apply_network_observation(&store, &mut gate, &id, MutationObservation {
                class: ResponseClass::Rejected, terminal, filled_units
            }).unwrap();
            drop(gate);
            let mut cold_gate = DurableMutationGate::open(root.join("intents.json")).unwrap();
            let cold = store.load_for_recovery().unwrap().unwrap();
            assert!(cold.pending.is_some());
            assert_eq!(cold.next_action(), LifecycleAction::ReconcilePending);
            assert_eq!(cold.futures_filled_units, 0);
            assert_eq!(cold_gate.store.load().unwrap()[0].state, IntentState::Unknown);
            assert_eq!(cold_gate.begin("next", Leg::Futures, "sab00000000000000000000000000097")
                .unwrap_err().kind(), io::ErrorKind::WouldBlock);
            fs::remove_dir_all(root).unwrap();
        }
    }

    #[test]
    fn rejected_close_is_terminal_manual_and_never_retried_blindly() {
        let (root, store, mut gate, mut p) = setup("rejected");
        let (i, c) = ids(15);
        p.prepare_close_mutation(&store, &mut gate, &i, &c, "BNT/USDT:USDT", 7, 7)
            .unwrap();
        p.apply_network_observation(
            &store,
            &mut gate,
            &i,
            MutationObservation {
                class: ResponseClass::Rejected,
                terminal: true,
                filled_units: 0,
            },
        )
        .unwrap();
        assert_eq!(p.state, LifecycleState::ManualRequired);
        assert_eq!(p.manual_reason, Some(ManualReason::MutationRejected));
        assert!(p.pending.is_none());
        assert_eq!(gate.store.load().unwrap()[0].state, IntentState::Rejected);
        assert_eq!(p.next_action(), LifecycleAction::None);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn crash_between_legs_reopens_at_spot_only() {
        let (root, store, mut gate, mut p) = setup("between-legs");
        let (i, c) = ids(16);
        p.prepare_close_mutation(&store, &mut gate, &i, &c, "BNT/USDT:USDT", 7, 7)
            .unwrap();
        p.apply_network_observation(
            &store,
            &mut gate,
            &i,
            MutationObservation {
                class: ResponseClass::Acknowledged,
                terminal: true,
                filled_units: 10,
            },
        )
        .unwrap();
        assert_eq!(p.state, LifecycleState::ClosingSpot);
        drop(gate);
        let reopened = store.load_for_recovery().unwrap().unwrap();
        assert_eq!(reopened.state, LifecycleState::ClosingSpot);
        assert!(reopened.pending.is_none());
        assert_eq!(reopened.next_action(), LifecycleAction::CloseSpotMarket);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn hedged_truth_after_futures_done_is_contradiction_manual() {
        let (root, _store, _gate, mut p) = setup("truth-reappeared");
        p.futures_filled_units = 10;
        p.state = LifecycleState::ClosingSpot;
        assert_eq!(
            p.observe_basic_truth(BasicTruth::Hedged),
            Err(LifecycleError::InvalidObservation)
        );
        assert_eq!(p.state, LifecycleState::ManualRequired);
        assert_eq!(p.manual_reason, Some(ManualReason::TruthMismatch));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn recovery_confirmed_partial_fill_resumes_same_futures_leg() {
        let (root, store, mut gate, mut p) = setup("recovery-partial");
        let (i, c) = ids(17);
        p.prepare_close_mutation(&store, &mut gate, &i, &c, "BNT/USDT:USDT", 7, 7)
            .unwrap();
        p.apply_network_observation(
            &store,
            &mut gate,
            &i,
            MutationObservation {
                class: ResponseClass::Unknown,
                terminal: false,
                filled_units: 0,
            },
        )
        .unwrap();
        p.recover_pending(
            &store,
            &mut gate,
            true,
            RecoveryEvidence::ConfirmedFill {
                filled_units: 4,
                terminal: true,
            },
        )
        .unwrap();
        assert_eq!(p.futures_filled_units, 4);
        assert_eq!(p.state, LifecycleState::ClosingFutures);
        assert_eq!(p.remaining_units(CloseLeg::Futures), 6);
        assert_eq!(p.next_action(), LifecycleAction::CloseFuturesReduceOnly);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn spot_second_leg_revalidates_safety_generation() {
        let (root, store, mut gate, mut p) = setup("spot-safety");
        p.futures_filled_units = 10;
        p.state = LifecycleState::ClosingSpot;
        store.persist(&p).unwrap();
        let (i, c) = ids(18);
        assert_eq!(
            p.prepare_close_mutation(&store, &mut gate, &i, &c, "BNT/USDT", 8, 8),
            Err(LifecycleError::StaleSafety)
        );
        assert!(gate.store.load().unwrap().is_empty());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn r96_close_trigger_rejects_missing_exposure_and_nonfinite_public_signals() {
        let base = CloseTriggerInput {
            emergency: false,
            amount_units: 10,
            lifetime_sec: 120,
            min_lifetime_sec: 60,
            close_spread_bps: Some(2.0),
            exit_bps: 3.0,
            velocity_bps_s: Some(0.0),
            hold_positive_bps_s: 0.5,
        };
        // Even an emergency label cannot close a zero-size or unknown position.
        assert_eq!(evaluate_close_trigger(CloseTriggerInput {
            emergency: true, amount_units: 0, ..base
        }), CloseDecision::ManualRequired);
        // NaN would previously bypass "spread > exit" and trigger CloseNormal.
        for spread in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert_eq!(evaluate_close_trigger(CloseTriggerInput {
                close_spread_bps: Some(spread), ..base
            }), CloseDecision::ManualRequired);
        }
        // Invalid velocity must never manufacture a positive-velocity hold.
        for velocity in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert_eq!(evaluate_close_trigger(CloseTriggerInput {
                velocity_bps_s: Some(velocity), ..base
            }), CloseDecision::ManualRequired);
        }
        assert_eq!(evaluate_close_trigger(CloseTriggerInput {
            exit_bps: f64::NAN, ..base
        }), CloseDecision::ManualRequired);
        assert_eq!(evaluate_close_trigger(CloseTriggerInput {
            hold_positive_bps_s: f64::NAN, ..base
        }), CloseDecision::ManualRequired);
        // A valid emergency can skip unavailable market quotes, but not size.
        assert_eq!(evaluate_close_trigger(CloseTriggerInput {
            emergency: true, close_spread_bps: None, ..base
        }), CloseDecision::CloseEmergency);
    }

    #[test]
    fn close_trigger_matches_python_normal_hold_and_emergency_contract() {
        let base = CloseTriggerInput {
            emergency: false,
            amount_units: 10,
            lifetime_sec: 120,
            min_lifetime_sec: 60,
            close_spread_bps: Some(2.0),
            exit_bps: 3.0,
            velocity_bps_s: Some(0.0),
            hold_positive_bps_s: 0.5,
        };
        assert_eq!(evaluate_close_trigger(base), CloseDecision::CloseNormal);
        assert_eq!(
            evaluate_close_trigger(CloseTriggerInput {
                velocity_bps_s: Some(1.0),
                ..base
            }),
            CloseDecision::Hold
        );
        assert_eq!(
            evaluate_close_trigger(CloseTriggerInput {
                lifetime_sec: 30,
                ..base
            }),
            CloseDecision::Wait
        );
        assert_eq!(
            evaluate_close_trigger(CloseTriggerInput {
                emergency: true,
                lifetime_sec: 0,
                close_spread_bps: None,
                ..base
            }),
            CloseDecision::CloseEmergency
        );
    }
}
