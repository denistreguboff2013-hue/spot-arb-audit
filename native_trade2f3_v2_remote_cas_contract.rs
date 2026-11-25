//! P0-03 / P1-07: zero-network, transport-neutral OFFLINE signed CAS
//! contract for a future physically independent monotonic witness.
//! Authentic Ed25519 signatures alone do NOT prove independent custody,
//! WORM semantics, freshness of a remote latest read, or remote deployment.
//! No exchange capabilities, no key material stored and no auto-recovery.
use openssl::{pkey::{Id,PKey},sign::Verifier,sha::sha256};
use serde::{Serialize,Deserialize};
use crate::native_trade2f3_v2_revision_journal::{
    V2JournalEntry,verify_mock_witness_revision_frame,
};
pub const CAS_REQUEST_DOMAIN:&str="SPOTARB/V2/WITNESS-CAS-REQUEST/ED25519/V1";
pub const CAS_REPLY_DOMAIN:&str="SPOTARB/V2/WITNESS-CAS-REPLY/ED25519/V1";
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V2CasWriteRequest{
    pub version:u32,
    pub domain:String,
    pub witness_id:String,
    pub client_id:String,
    pub operation_id_hex:String,
    pub challenge_hex:String,
    pub candidate:V2JournalEntry,
    pub expected_predecessor_revision:u64,
    pub expected_predecessor_entry_sha256:Option<String>,
}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V2SignedCasWrite{
    pub request:V2CasWriteRequest,
    pub client_ed25519_signature_hex:String,
}
#[derive(Clone,Copy,Debug,PartialEq,Eq,Serialize,Deserialize)]
pub enum V2CasResultKind{Committed,AlreadyCommitted,Rejected}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V2CasReceipt{
    pub version:u32,
    pub domain:String,
    pub witness_id:String,
    pub client_id:String,
    pub operation_id_hex:String,
    pub challenge_hex:String,
    pub request_sha256:String,
    pub original_owner_generation:u64,
    pub immutable_genesis_sha256:String,
    pub latest_committed_revision:u64,
    pub latest_entry_sha256:String,
    pub result:V2CasResultKind,
}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V2SignedCasReceipt{
    pub receipt:V2CasReceipt,
    pub witness_ed25519_signature_hex:String,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct V2CasPinnedIdentity{
    pub witness_id:String,
    pub client_id:String,
    pub client_public_key_hex:String,
    pub witness_public_key_hex:String,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum V2CasContractError{
    Schema(&'static str),
    MissingIndependentPin,
    WrongIdentity,
    InvalidSignature,
    ReplayOrDifferentRequest,
    DeniedOrUnverifiableCommit,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct V2CasReceiptDiagnostic{
    pub revision:u64,
    pub signed_request_valid:bool,
    pub signed_reply_valid:bool,
    pub result:V2CasResultKind,
    /// This layer cannot prove durable readback of an off-host CAS commit.
    pub independently_durable_remote_commit_proven:bool,
    pub remote_currentness_proven:bool,
    pub trading_authorized:bool,
}
fn deny(msg:&'static str)->V2CasContractError{V2CasContractError::Schema(msg)}
fn lower_hex(s:&str,n:usize)->bool{
    s.len()==2*n&&s.bytes().all(|b|b.is_ascii_hexdigit()&&!b.is_ascii_uppercase())
}
fn unhex(s:&str,n:usize)->Result<Vec<u8>,V2CasContractError>{
    if !lower_hex(s,n){return Err(deny("bad hexadecimal encoding"))}
    let mut v=Vec::with_capacity(n);
    for part in s.as_bytes().chunks_exact(2){
        let h=(part[0] as char).to_digit(16).ok_or(deny("bad hex"))?;
        let l=(part[1] as char).to_digit(16).ok_or(deny("bad hex"))?;
        v.push(((h<<4)|l) as u8);
    }
    Ok(v)
}
fn identifier(s:&str)->bool{
    !s.is_empty()&&s.len()<=128&&s.bytes().all(|v|
        v.is_ascii_alphanumeric()||v==b'-'||v==b'_'||v==b'.')
}
fn verify_raw(pub_key:&str,signature:&str,message:&[u8])
    ->Result<(),V2CasContractError>{
    let key=unhex(pub_key,32)?;
    let sig=unhex(signature,64)?;
    let pkey=PKey::public_key_from_raw_bytes(&key,Id::ED25519)
        .map_err(|_|deny("bad raw Ed25519 public key"))?;
    let mut verifier=Verifier::new_without_digest(&pkey)
        .map_err(|_|deny("Ed25519 verifier unavailable"))?;
    if !verifier.verify_oneshot(&sig,message)
        .map_err(|_|deny("Ed25519 verifier failed"))?{
        return Err(V2CasContractError::InvalidSignature)
    }
    Ok(())
}
pub fn cas_request_message(r:&V2CasWriteRequest)
    ->Result<Vec<u8>,V2CasContractError>{
    if r.version!=1||r.domain!=CAS_REQUEST_DOMAIN
        ||!identifier(&r.witness_id)||!identifier(&r.client_id)
        ||!lower_hex(&r.operation_id_hex,32)||!lower_hex(&r.challenge_hex,32){
        return Err(deny("invalid CAS request scope or operation challenge"))
    }
    let prior=if r.expected_predecessor_revision==0{
        if r.expected_predecessor_entry_sha256.is_some(){
            return Err(deny("genesis cannot cite predecessor hash"))
        }
        None
    }else{
        let hash=r.expected_predecessor_entry_sha256.as_deref()
            .ok_or(deny("predecessor hash missing"))?;
        if !lower_hex(hash,32){return Err(deny("predecessor hash invalid"))}
        Some(hash)
    };
    if r.candidate.revision!=r.expected_predecessor_revision.checked_add(1)
        .ok_or(deny("CAS revision overflow"))?{
        return Err(deny("CAS not exact successor"))
    }
    // Validate candidate checksum even without the entire previous frame.
    // Exact full predecessor identity and generation must be rechecked by
    // the remote durable CAS engine, never by this request parser alone.
    if r.candidate.schema_version!=1
        ||!lower_hex(&r.candidate.immutable_genesis_sha256,32)
        ||!lower_hex(&r.candidate.owner_state_sha256,32)
        ||!lower_hex(&r.candidate.entry_sha256,32)
        ||!lower_hex(&r.candidate.previous_entry_sha256,32)
        ||r.candidate.original_owner_generation==0{
        return Err(deny("candidate shape invalid"))
    }
    let expected_link=prior.unwrap_or(
        "0000000000000000000000000000000000000000000000000000000000000000");
    if r.candidate.previous_entry_sha256!=expected_link{
        return Err(deny("request predecessor differs from candidate"))
    }
    // Only genesis frame is independently validated without prior.
    if r.expected_predecessor_revision==0{
        verify_mock_witness_revision_frame(&r.candidate,None)
            .map_err(|_|deny("bad genesis checksum"))?;
    }
    serde_json::to_vec(r).map_err(|_|deny("encode request"))
}
pub fn verify_signed_cas_command(cmd:&V2SignedCasWrite,
    pinned:&V2CasPinnedIdentity)->Result<(),V2CasContractError>{
    if pinned.client_public_key_hex.is_empty(){
        return Err(V2CasContractError::MissingIndependentPin)
    }
    if pinned.client_id!=cmd.request.client_id
        ||pinned.witness_id!=cmd.request.witness_id{
        return Err(V2CasContractError::WrongIdentity)
    }
    verify_raw(&pinned.client_public_key_hex,
        &cmd.client_ed25519_signature_hex,
        &cas_request_message(&cmd.request)?)
}

pub fn cas_receipt_message(r:&V2CasReceipt)->Result<Vec<u8>,V2CasContractError>{
    if r.version!=1||r.domain!=CAS_REPLY_DOMAIN
        ||!identifier(&r.witness_id)||!identifier(&r.client_id)
        ||!lower_hex(&r.operation_id_hex,32)
        ||!lower_hex(&r.challenge_hex,32)
        ||!lower_hex(&r.request_sha256,32)
        ||!lower_hex(&r.immutable_genesis_sha256,32)
        ||!lower_hex(&r.latest_entry_sha256,32)
        ||r.original_owner_generation==0||r.latest_committed_revision==0{
        return Err(deny("invalid CAS reply domain or receipt fields"))
    }
    serde_json::to_vec(r).map_err(|_|deny("encode CAS receipt"))
}
/// This is cryptographic and request-correlation verification ONLY.
/// A verified signed Committed status is not a durable remote ACK without
/// independently trusted key custody and a fresh off-host readback.
pub fn verify_signed_cas_response(
    cmd:&V2SignedCasWrite,reply:&V2SignedCasReceipt,
    pinned:&V2CasPinnedIdentity,
)->Result<V2CasReceiptDiagnostic,V2CasContractError>{
    verify_signed_cas_command(cmd,pinned)?;
    if pinned.witness_public_key_hex.is_empty(){
        return Err(V2CasContractError::MissingIndependentPin)
    }
    let r=&cmd.request;
    let response=&reply.receipt;
    let req_bytes=cas_request_message(r)?;
    let request_digest=sha256(&req_bytes);
    let req_hash=request_digest.iter().map(|b|format!("{b:02x}")).collect::<String>();
    if response.witness_id!=pinned.witness_id
        ||response.client_id!=pinned.client_id
        ||response.operation_id_hex!=r.operation_id_hex
        ||response.challenge_hex!=r.challenge_hex
        ||response.request_sha256!=req_hash{
        return Err(V2CasContractError::ReplayOrDifferentRequest)
    }
    verify_raw(&pinned.witness_public_key_hex,
        &reply.witness_ed25519_signature_hex,
        &cas_receipt_message(response)?)?;
    if response.result==V2CasResultKind::Rejected{
        return Err(V2CasContractError::DeniedOrUnverifiableCommit)
    }
    if response.latest_committed_revision!=r.candidate.revision
        ||response.latest_entry_sha256!=r.candidate.entry_sha256
        ||response.original_owner_generation!=r.candidate.original_owner_generation
        ||response.immutable_genesis_sha256!=r.candidate.immutable_genesis_sha256{
        return Err(V2CasContractError::DeniedOrUnverifiableCommit)
    }
    Ok(V2CasReceiptDiagnostic{
        revision:r.candidate.revision,
        signed_request_valid:true,
        signed_reply_valid:true,
        result:response.result,
        independently_durable_remote_commit_proven:false,
        remote_currentness_proven:false,
        trading_authorized:false,
    })
}
/// Creates a canonical receipt frame for offline fixture signing. This does
/// NOT represent network I/O or an actual remote commit. It requires the
/// caller to separately run the witness CAS and collect its result.
pub fn offline_receipt_frame(cmd:&V2SignedCasWrite,
    result:V2CasResultKind)->Result<V2CasReceipt,V2CasContractError>{
    let r=&cmd.request;
    let digest=sha256(&cas_request_message(r)?);
    Ok(V2CasReceipt{
        version:1,domain:CAS_REPLY_DOMAIN.to_owned(),
        witness_id:r.witness_id.clone(),
        client_id:r.client_id.clone(),
        operation_id_hex:r.operation_id_hex.clone(),
        challenge_hex:r.challenge_hex.clone(),
        request_sha256:digest.iter().map(|b|format!("{b:02x}")).collect(),
        original_owner_generation:r.candidate.original_owner_generation,
        immutable_genesis_sha256:r.candidate.immutable_genesis_sha256.clone(),
        latest_committed_revision:r.candidate.revision,
        latest_entry_sha256:r.candidate.entry_sha256.clone(),
        result,
    })
}
