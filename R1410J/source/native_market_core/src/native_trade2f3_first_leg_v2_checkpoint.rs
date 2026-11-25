//! Astra P0-02/P1-02 — immutable, sidecar V2 first-leg fee evidence.
//! This is LOCAL SOURCE-SHAPED offline evidence ONLY: SHA256 is an integrity
//! checksum, not independent exchange attestation or external antirollback.
//! Separate from old Native owner JSON and old C2/C3/C5/C6/B4 seals.
//! A checkpoint never authorizes a trade or claims a full owner migration.
use std::{fs::{File,OpenOptions},io::{Read,Write,ErrorKind},path::Path};
use serde::{Serialize,Deserialize};
use openssl::sha::sha256;
use crate::{
    authoritative_state::NativeEntryPlan,
    native_spot_prehedge::NativeSpotPreHedgeEvidence,
    native_trade2f_asset_fee::SpotFeeFill,
    native_trading_config::NativeEntryPolicyBinding,
    native_trade2f3_fee_aware_first_leg_v2::{
        inspect_first_leg_fee_preview_v2,FirstLegV2Error,FirstLegFeePreviewV2,
    },
};
pub const SCHEMA_VERSION:u32=2;
const MAX_BYTES:usize=131_072;

#[derive(Clone,Debug,PartialEq,Eq)]
pub enum FirstLegCheckpointError{
    Io(ErrorKind),Decode,Checksum,Schema,Source,Denied(&'static str),
}
impl From<std::io::Error> for FirstLegCheckpointError{
    fn from(e:std::io::Error)->Self{Self::Io(e.kind())}
}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum CheckpointFailpoint{
    None,BeforeCreate,AfterIncompleteWrite,AfterFileSyncBeforeDirSync,
}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FirstLegV2CheckpointPayload{
    pub schema_version:u32,
    pub original_native_generation:u64,
    pub policy:NativeEntryPolicyBinding,
    pub plan:NativeEntryPlan,
    pub original:NativeSpotPreHedgeEvidence,
    pub source_shaped_fee_fills:Vec<SpotFeeFill>,
    pub quote_spent_atoms_e12:String,
    pub net_base_after_fee_atoms_e12:String,
    /// Optional LOCAL exact V1 snapshot bytes pin used by the strict offline
    /// two-file reader. Old sidecars without this field remain valid only for
    /// legacy single-sidecar diagnostics, never full pair certification.
    #[serde(default,skip_serializing_if="Option::is_none")]
    pub original_v1_owner_file_sha256:Option<String>,
    /// The legacy Native owner has NOT accepted or signed this V2 evidence.
    pub original_owner_v2_seal_migrated:bool,
    pub exchange_global_fees_attested:bool,
    pub may_send_or_verify_closed:bool,
}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckpointEnvelope{
    pub payload:FirstLegV2CheckpointPayload,
    pub local_checksum_sha256:String,
}
fn digest(payload:&FirstLegV2CheckpointPayload)->Result<String,FirstLegCheckpointError>{
    let bytes=serde_json::to_vec(payload).map_err(|_|FirstLegCheckpointError::Decode)?;
    Ok(sha256(&bytes).iter().map(|v|format!("{v:02x}")).collect::<String>())
}
fn revalidate(p:&FirstLegV2CheckpointPayload)
    ->Result<FirstLegFeePreviewV2,FirstLegCheckpointError>{
    if p.schema_version!=SCHEMA_VERSION || p.original_native_generation==0 {
        return Err(FirstLegCheckpointError::Schema)
    }
    if p.original_v1_owner_file_sha256.as_ref().is_some_and(|sha|
        sha.len()!=64||sha.bytes().any(|b|!b.is_ascii_hexdigit()||b.is_ascii_uppercase())
    ){
        return Err(FirstLegCheckpointError::Denied("invalid original V1 local snapshot digest"))
    }
    if p.original_owner_v2_seal_migrated||p.exchange_global_fees_attested
        ||p.may_send_or_verify_closed{
        return Err(FirstLegCheckpointError::Denied("offline V2 source must never claim authority"))
    }
    let proof=inspect_first_leg_fee_preview_v2(
        &p.policy,&p.plan,&p.original,&p.source_shaped_fee_fills
    ).map_err(|_e:FirstLegV2Error|FirstLegCheckpointError::Source)?;
    if p.quote_spent_atoms_e12!=proof.quote_spent_with_quote_fees_e12.to_string()
        ||p.net_base_after_fee_atoms_e12!=proof.original_delivered_base_e12.to_string(){
        return Err(FirstLegCheckpointError::Denied("cached V2 quantity inconsistent with original source"))
    }
    Ok(proof)
}
/// Pure constructor. The caller generation is ONLY a binding token; not
/// a trusted owner CAS or proof that those fills exist in an exchange.
pub fn prepare_first_leg_v2(
    generation:u64,
    policy:&NativeEntryPolicyBinding,
    plan:&NativeEntryPlan,
    first:&NativeSpotPreHedgeEvidence,
    rows:&[SpotFeeFill],
)->Result<FirstLegV2CheckpointPayload,FirstLegCheckpointError>{
    let proof=inspect_first_leg_fee_preview_v2(policy,plan,first,rows)
        .map_err(|_|FirstLegCheckpointError::Source)?;
    let payload=FirstLegV2CheckpointPayload{
        schema_version:SCHEMA_VERSION,original_native_generation:generation,
        policy:policy.clone(),plan:plan.clone(),original:first.clone(),
        source_shaped_fee_fills:rows.to_vec(),
        quote_spent_atoms_e12:proof.quote_spent_with_quote_fees_e12.to_string(),
        net_base_after_fee_atoms_e12:proof.original_delivered_base_e12.to_string(),
        original_v1_owner_file_sha256:None,
        original_owner_v2_seal_migrated:false,
        exchange_global_fees_attested:false,
        may_send_or_verify_closed:false,
    };
    revalidate(&payload)?;
    Ok(payload)
}
// The directory-commit witness is deliberately separate from the V2
// envelope checksum. A complete fsynced file is NOT safely published until
// the parent's directory entry has also reached the durability barrier.
// Presence of this marker is an irreversible (manual-only) recovery quarantine.
fn incomplete_path(path:&Path)->std::path::PathBuf{
    let mut name=path.as_os_str().to_os_string();
    name.push(".incomplete");
    std::path::PathBuf::from(name)
}
/// Write-once, O_EXCL. Existing path is NEVER overwritten or interpreted
/// as idempotently authorized. Caller must supply a dedicated test directory.
pub fn persist_v2_once(
    path:&Path,payload:&FirstLegV2CheckpointPayload,
    failpoint:CheckpointFailpoint,
)->Result<(),FirstLegCheckpointError>{
    revalidate(payload)?;
    let parent=path.parent().filter(|p|!p.as_os_str().is_empty())
        .ok_or(FirstLegCheckpointError::Denied("missing checkpoint parent"))?;
    if !parent.is_dir()||parent.symlink_metadata()?.file_type().is_symlink(){
        return Err(FirstLegCheckpointError::Denied("checkpoint parent must be real directory"))
    }
    if failpoint==CheckpointFailpoint::BeforeCreate{
        return Err(FirstLegCheckpointError::Io(ErrorKind::Interrupted))
    }
    let env=CheckpointEnvelope{payload:payload.clone(),
        local_checksum_sha256:digest(payload)?};
    let bytes=serde_json::to_vec(&env).map_err(|_|FirstLegCheckpointError::Decode)?;
    if bytes.len()>MAX_BYTES{return Err(FirstLegCheckpointError::Schema)}
    // Publish a durable pending marker *before* the checkpoint file can
    // appear. Any interrupted write is quarantined; no blind retry.
    if path.symlink_metadata().is_ok(){
        return Err(FirstLegCheckpointError::Io(ErrorKind::AlreadyExists))
    }
    let marker=incomplete_path(path);
    let guard=OpenOptions::new().write(true).create_new(true).open(&marker)?;
    guard.sync_all()?;
    File::open(parent)?.sync_all()?;
    let mut f=OpenOptions::new().write(true).create_new(true).open(path)?;
    if failpoint==CheckpointFailpoint::AfterIncompleteWrite{
        // Crash simulation leaves a nonempty but invalid blob. NEVER
        // overwrite it automatically on retry: quarantine requires review.
        f.write_all(&bytes[..bytes.len()/2])?;
        return Err(FirstLegCheckpointError::Io(ErrorKind::Interrupted))
    }
    f.write_all(&bytes)?;
    f.sync_all()?;
    if failpoint==CheckpointFailpoint::AfterFileSyncBeforeDirSync{
        return Err(FirstLegCheckpointError::Io(ErrorKind::Interrupted))
    }
    // Durable target entry precedes marker removal. A crash before removal
    // leaves quarantine; successful removal is fsynced before return.
    File::open(parent)?.sync_all()?;
    std::fs::remove_file(&marker)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}
/// On every reopen: exact version, checksum, source fee IDs/currency/cash,
/// policy $10 cap, fixed-point amounts, and nonauthorizing fields.
pub fn load_v2(
    path:&Path,expected_generation:u64,
    expected_policy:&NativeEntryPolicyBinding,
    expected_plan:&NativeEntryPlan,
    expected_first:&NativeSpotPreHedgeEvidence,
)->Result<(FirstLegV2CheckpointPayload,FirstLegFeePreviewV2),FirstLegCheckpointError>{
    // An unacknowledged file can have a completely valid SHA envelope.
    // Never treat it as durable while the write-once marker still exists.
    if incomplete_path(path).symlink_metadata().is_ok(){
        return Err(FirstLegCheckpointError::Denied("unfinished V2 checkpoint requires manual review"))
    }
    let f=File::open(path)?;
    let mut bytes=Vec::new();
    f.take((MAX_BYTES+1) as u64).read_to_end(&mut bytes)?;
    if bytes.len()>MAX_BYTES{return Err(FirstLegCheckpointError::Schema)}
    let env:CheckpointEnvelope=serde_json::from_slice(&bytes)
        .map_err(|_|FirstLegCheckpointError::Decode)?;
    if digest(&env.payload)?!=env.local_checksum_sha256{
        return Err(FirstLegCheckpointError::Checksum)
    }
    if env.payload.original_native_generation!=expected_generation
        ||env.payload.policy!=*expected_policy
        ||env.payload.plan!=*expected_plan
        ||env.payload.original!=*expected_first{
        return Err(FirstLegCheckpointError::Denied("original owner input/generation no longer matches V2 checkpoint"))
    }
    let proof=revalidate(&env.payload)?;
    Ok((env.payload,proof))
}
