//! Astra P0-03/P1-07: fail-closed three-way owner/journal/witness
//! crash-window classifier, for OFFLINE V2 REVIEW ONLY.
//! A valid signature cannot authenticate independent witness custody,
//! non-rollbackability or latest remote history. No execution permits.
use openssl::sha::sha256;
use crate::{
    native_trade2f3_v2_versioned_store::V2ReviewState,
    native_trade2f3_v2_revision_journal::{
        V2JournalEntry,verify_mock_witness_revision_frame,
        review_owner_against_local_journal,
    },
    native_trade2f3_v2_signed_floor::{
        V2SignedFloorEnvelope,V2ExternallyProvisionedVerifier,
        verify_signed_floor_cryptography,
    },
};
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum V2ThreeWayCondition{
    /// Three TEST-SHAPED views agree. Does NOT mean a trusted external ACK.
    AlignedDiagnosticOnly,
    /// Owner successfully persisted rev N+1 but local journal is at N.
    OwnerAheadOfJournal,
    /// Local journal at N+1 but owner is at N (possible owner rollback).
    OwnerBehindJournal,
    /// Both local owner/journal at N+1; signed witness at N.
    LocalAheadOfWitness,
    /// Signed witness ahead of local revisions: rollback or lost local writes.
    WitnessAheadOfLocal,
    /// Same revision but different hash, or conflicting ancestry/scope.
    DivergentFork,
    /// Nonadjacent revision gap, invalid local owner or incomplete local log.
    IncompleteOrCorruptLocal,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum V2ThreeWayError{
    BadWitnessSignatureOrChallenge,
    WrongWitnessOwnerOrGenesis,
    InvalidLocalStateOrJournal,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct V2ThreeWayReview{
    pub condition:V2ThreeWayCondition,
    pub owner_revision:u64,
    pub journal_revision:u64,
    pub signed_witness_revision:u64,
    pub signed_witness_valid:bool,
    /// No authoritative remote custody or currentness checker in this crate.
    pub witness_is_independent_and_latest:bool,
    pub may_repair_replay_or_retry_automatically:bool,
    pub may_issue_exchange_commands:bool,
    pub may_ack_durable_external_commit:bool,
}
fn result(c:V2ThreeWayCondition,o:u64,j:u64,w:u64)->V2ThreeWayReview{
    V2ThreeWayReview{
        condition:c,owner_revision:o,journal_revision:j,
        signed_witness_revision:w,signed_witness_valid:true,
        witness_is_independent_and_latest:false,
        may_repair_replay_or_retry_automatically:false,
        may_issue_exchange_commands:false,
        may_ack_durable_external_commit:false,
    }
}
/// Inspects three *provided* snapshots, verifying Ed25519 receipt first.
/// All results have absolutely no trading/remote-ACK authority. Caller must
/// obtain an independently fresh signed tip; this code does not fetch it.
pub fn classify_v2_three_way(
    owner:&V2ReviewState,
    journal:&[V2JournalEntry],
    signed:&V2SignedFloorEnvelope,
    provisioned:&V2ExternallyProvisionedVerifier,
)->Result<V2ThreeWayReview,V2ThreeWayError>{
    verify_signed_floor_cryptography(signed,provisioned)
        .map_err(|_|V2ThreeWayError::BadWitnessSignatureOrChallenge)?;
    owner.validate()
        .map_err(|_|V2ThreeWayError::InvalidLocalStateOrJournal)?;
    if journal.is_empty()||journal.len()>512{
        return Err(V2ThreeWayError::InvalidLocalStateOrJournal)
    }
    for (idx,e) in journal.iter().enumerate(){
        let prior=if idx==0{None}else{Some(&journal[idx-1])};
        verify_mock_witness_revision_frame(e,prior)
            .map_err(|_|V2ThreeWayError::InvalidLocalStateOrJournal)?;
    }
    let tip=journal.last().ok_or(V2ThreeWayError::InvalidLocalStateOrJournal)?;
    let p=&signed.payload;
    let owner_genesis_bytes=serde_json::to_vec(&owner.genesis)
        .map_err(|_|V2ThreeWayError::InvalidLocalStateOrJournal)?;
    let owner_genesis_sha=sha256(&owner_genesis_bytes).iter()
        .map(|b|format!("{b:02x}")).collect::<String>();
    if owner.genesis.original_owner_generation!=tip.original_owner_generation
        ||owner_genesis_sha!=tip.immutable_genesis_sha256
        ||p.original_owner_generation!=tip.original_owner_generation
        ||p.immutable_genesis_sha256!=tip.immutable_genesis_sha256{
        return Err(V2ThreeWayError::WrongWitnessOwnerOrGenesis)
    }
    // An owner snapshot with a different immutable genesis must never be
    // treated as an ordinary crash window, even when its generation matches.
    let same_owner_genesis=owner.revision==tip.revision
        &&review_owner_against_local_journal(owner,journal,None).is_ok();
    // Cases with a revision gap are diagnosable but never accepted.  A
    // mismatched owner genesis will be quarantined as "incomplete" below.
    let o=owner.revision;
    let j=tip.revision;
    let w=p.latest_committed_revision;
    let adjacent=o.abs_diff(j)==1;
    let local_condition=if o==j{
        if same_owner_genesis{None}else{Some(V2ThreeWayCondition::DivergentFork)}
    }else if adjacent{
        Some(if o>j{
            V2ThreeWayCondition::OwnerAheadOfJournal
        }else{V2ThreeWayCondition::OwnerBehindJournal})
    }else{Some(V2ThreeWayCondition::IncompleteOrCorruptLocal)};
    // A matching signed latest hash must belong to a known frame. If witness
    // is at or below local journal, require the exact historical hash:
    // an alternate same-revision signed fork must never look "stale-safe".
    if w<=j{
        let idx=usize::try_from(w.saturating_sub(1))
            .map_err(|_|V2ThreeWayError::InvalidLocalStateOrJournal)?;
        let historical=journal.get(idx)
            .ok_or(V2ThreeWayError::InvalidLocalStateOrJournal)?;
        if historical.revision!=w||historical.entry_sha256!=p.latest_entry_sha256{
            return Ok(result(V2ThreeWayCondition::DivergentFork,o,j,w))
        }
    }
    if let Some(local)=local_condition{
        // If signed witness is ahead as well, the local crash discrepancy is
        // still quarantined. Do not guess who committed which operation.
        return Ok(result(local,o,j,w))
    }
    if w>j{
        return Ok(result(V2ThreeWayCondition::WitnessAheadOfLocal,o,j,w))
    }
    if w<j{
        return Ok(result(V2ThreeWayCondition::LocalAheadOfWitness,o,j,w))
    }
    Ok(result(V2ThreeWayCondition::AlignedDiagnosticOnly,o,j,w))
}
