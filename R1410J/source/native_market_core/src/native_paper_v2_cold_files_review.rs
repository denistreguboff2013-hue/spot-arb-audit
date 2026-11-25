//! Astra P0-02 offline-only cold match of sealed local PAPER CAS,
//! immutable V2 owner, and either raw TEST source or fsynced local archive.
//! LOCAL checks cannot authenticate exchange truth or externally anchored
//! antirollback. Never grant a live order, retry, or VerifiedClosed.
use crate::{
    native_paper_durable_journal_review::{
        inspect_journal_against_supplied_floor,RecoveredPaper,JournalError,
    },
    native_paper_v2_close_source_archive_review::{
        read_local_close_source,ArchiveError,
    },
    native_paper_v2_closure_bridge_review::{
        inspect_recovered_alignment_consistency,BridgeError,
    },
    native_trade2f3_v2_versioned_store::{
        load_v2,V2ReviewState,V2ReviewError,V2ReviewPhase,
    },
    native_trade2f3_v2_four_leg_close_review::V2FourLegCloseObservation,
};
use openssl::sha::sha256;
use serde_json::from_slice;
use std::{fs::{self,File},io::{Read,ErrorKind},path::Path};
const MAX_CLOSE_OBSERVATION_BYTES:u64=256*1024;

#[derive(Debug,Clone,PartialEq,Eq)]
pub enum ColdP002Error{
    Invalid(&'static str),
    LocalPaper(JournalError),
    LocalV2(V2ReviewError),
    ArchivedSource(ArchiveError),
    MismatchedEvidence(BridgeError),
    Io(ErrorKind),
}
impl From<std::io::Error> for ColdP002Error{
    fn from(value:std::io::Error)->Self{Self::Io(value.kind())}
}
fn deny(message:&'static str)->ColdP002Error{ColdP002Error::Invalid(message)}
fn hex(bytes:&[u8])->String{
    sha256(bytes).iter().map(|b|format!("{b:02x}")).collect()
}
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct ColdP002Diagnostic{
    pub paper_sequence:u64,
    pub paper_chain_sha256:String,
    pub source_v2_revision:u64,
    /// SHA256 of raw fixture bytes OR canonical decoded observation bytes
    /// from the verified archive, as indicated by source_archive_verified.
    pub source_observation_sha256:String,
    pub source_archive_verified:bool,
    pub source_matches_local_paper_cas:bool,
    pub local_paper_legs_fee_and_qty_consistent:bool,
    pub exchange_account_truth_authenticated:bool,
    pub external_floor_authenticated_and_fresh:bool,
    pub verified_closed:bool,
    pub may_resume_or_retry_paper:bool,
    pub may_trade_or_cancel_real:bool,
}
fn bounded_source(path:&Path)->Result<Vec<u8>,ColdP002Error>{
    let metadata=fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file()||metadata.len()==0||
       metadata.len()>MAX_CLOSE_OBSERVATION_BYTES{
        return Err(deny("close-observation source not a bounded ordinary file"))
    }
    let mut data=Vec::new();
    File::open(path)?.take(MAX_CLOSE_OBSERVATION_BYTES+1)
        .read_to_end(&mut data)?;
    if data.is_empty()||data.len() as u64>MAX_CLOSE_OBSERVATION_BYTES{
        return Err(deny("close-observation record oversized or empty"))
    }
    Ok(data)
}
fn cold_owner(
    paper_journal:&Path,v2_owner_file:&Path,
    base:&str,expected_sequence:u64,expected_hash:&str,
)->Result<(RecoveredPaper,V2ReviewState),ColdP002Error>{
    if base.is_empty()||base.len()>64||
       !base.bytes().all(|x|x.is_ascii_uppercase()||x.is_ascii_digit()){
        return Err(deny("malformed source position identity"))
    }
    if !fs::symlink_metadata(paper_journal)?.file_type().is_file(){
        return Err(deny("paper cold journal must be a regular file, not a symlink"))
    }
    let recovered=inspect_journal_against_supplied_floor(
        paper_journal,expected_sequence,expected_hash)
        .map_err(ColdP002Error::LocalPaper)?;
    let v2=load_v2(v2_owner_file).map_err(ColdP002Error::LocalV2)?;
    if v2.revision!=4||v2.phase!=V2ReviewPhase::FuturesEvidenceRecorded||
       v2.production_authorized||v2.may_send_or_cancel_or_verify_closed{
       return Err(deny("V2 source must be immutable offline rev4 pre-close"))
    }
    Ok((recovered,v2))
}
fn finish(
    recovered:&RecoveredPaper,base:&str,v2:&V2ReviewState,
    obs:&V2FourLegCloseObservation,source_hash:String,
    archive_verified:bool,
)->Result<ColdP002Diagnostic,ColdP002Error>{
    if obs.source_v2_revision!=4||
       obs.source_original_generation!=v2.genesis.original_owner_generation||
       obs.trading_or_verified_closed_authorized||
       obs.independent_account_history_authenticated||
       obs.late_funding_and_transfers_complete{
        return Err(deny("invalid source identities or falsely claimed authority"))
    }
    let comparison=inspect_recovered_alignment_consistency(recovered,base,v2,obs)
        .map_err(ColdP002Error::MismatchedEvidence)?;
    if !comparison.source_agrees_with_local_journal||
       comparison.may_resume_paper||comparison.may_trade_real||
       comparison.verified_closed{
        return Err(deny("recovered local owner unexpectedly granted authority"))
    }
    Ok(ColdP002Diagnostic{
        paper_sequence:recovered.sequence,
        paper_chain_sha256:recovered.hash.clone(),
        source_v2_revision:v2.revision,
        source_observation_sha256:source_hash,
        source_archive_verified:archive_verified,
        source_matches_local_paper_cas:true,
        local_paper_legs_fee_and_qty_consistent:true,
        exchange_account_truth_authenticated:false,
        external_floor_authenticated_and_fresh:false,
        verified_closed:false,
        may_resume_or_retry_paper:false,
        may_trade_or_cancel_real:false,
    })
}
/// Legacy RAW TEST fixture path. Never interpret a loose JSON file as the
/// fsynced immutable source archive. No exchange authenticated evidence.
pub fn inspect_cold_p002_four_leg_files(
    paper_journal:&Path,v2_owner_file:&Path,
    close_observation_file:&Path,base:&str,
    expected_paper_sequence:u64,expected_paper_chain_sha256:&str,
)->Result<ColdP002Diagnostic,ColdP002Error>{
    let (recovered,v2)=cold_owner(paper_journal,v2_owner_file,base,
        expected_paper_sequence,expected_paper_chain_sha256)?;
    let bytes=bounded_source(close_observation_file)?;
    let obs:V2FourLegCloseObservation=from_slice(&bytes)
        .map_err(|_|deny("source-shaped closing evidence failed strict JSON decode"))?;
    finish(&recovered,base,&v2,&obs,hex(&bytes),false)
}
/// NEW strict cold recovery: the source MUST be the verified one-shot fsynced
/// typed archive. Raw source JSON, symlink alias or tampered envelope fails.
/// Supplied journal floor is still NOT an independent remote witness.
pub fn inspect_cold_p002_archived_four_leg_files(
    paper_journal:&Path,v2_owner_file:&Path,
    close_archive_file:&Path,base:&str,
    expected_paper_sequence:u64,expected_paper_chain_sha256:&str,
)->Result<ColdP002Diagnostic,ColdP002Error>{
    let (recovered,v2)=cold_owner(paper_journal,v2_owner_file,base,
       expected_paper_sequence,expected_paper_chain_sha256)?;
    let obs=read_local_close_source(close_archive_file,&v2)
        .map_err(ColdP002Error::ArchivedSource)?;
    let bytes=serde_json::to_vec(&obs)
        .map_err(|_|deny("source observation canonical serialization failed"))?;
    finish(&recovered,base,&v2,&obs,hex(&bytes),true)
}
#[cfg(test)]
mod tests{
    use super::*;
    #[test]fn bad_base_and_missing_input_do_not_create_paper_or_exchange_rights(){
        let path=Path::new("/dev/null");
        assert_eq!(inspect_cold_p002_four_leg_files(
            path,path,path,"one",1,&"0".repeat(64)),
            Err(ColdP002Error::Invalid("malformed source position identity")));
        assert!(inspect_cold_p002_archived_four_leg_files(
            path,path,path,"ONE",1,&"0".repeat(64)).is_err());
    }
    #[test]fn forged_or_empty_source_bytes_refused(){
        let root=std::env::temp_dir().join(format!(
            "astra_cold_four_leg_{}_{}",
            std::process::id(),crate::transport::monotonic_ns()));
        fs::create_dir_all(&root).unwrap();
        let file=root.join("observation.json");
        fs::write(&file,b"").unwrap();
        assert_eq!(bounded_source(&file),
            Err(ColdP002Error::Invalid("close-observation source not a bounded ordinary file")));
        fs::write(&file,vec![b'0';MAX_CLOSE_OBSERVATION_BYTES as usize+1]).unwrap();
        assert!(bounded_source(&file).is_err());
        fs::remove_dir_all(root).unwrap();
    }
}
