//! Astra P0-03/P1-07 OFFLINE ONLY: append-only V2 local journal and
//! caller-held floor comparison. It DOES NOT authenticate the caller or
//! provide independent antirollback: journal and owner can be rolled back
//! together; a trusted floor must originate OUTSIDE this host/failure domain.
use std::{
    fs::{self,File,OpenOptions},io::{self,Read,Write,ErrorKind},
    path::{Path,PathBuf},
};
use serde::{Deserialize,Serialize};
use openssl::sha::sha256;
use crate::native_trade2f3_v2_versioned_store::V2ReviewState;

const LOG_SCHEMA:u32=1;
const MAX_LOG_BYTES:usize=2_097_152;
const MAX_ENTRIES:usize=2048;
const ZERO_HASH:&str="0000000000000000000000000000000000000000000000000000000000000000";
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V2JournalEntry {
    pub schema_version:u32,
    pub original_owner_generation:u64,
    pub revision:u64,
    pub immutable_genesis_sha256:String,
    pub owner_state_sha256:String,
    pub previous_entry_sha256:String,
    pub entry_sha256:String,
}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V2CallerHeldFloor {
    pub original_owner_generation:u64,
    pub minimum_revision:u64,
    pub exact_entry_sha256:String,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct V2JournalReview {
    pub current_revision:u64,
    pub journal_entries:usize,
    pub current_entry_sha256:String,
    pub local_chain_consistent:bool,
    pub supplied_floor_matches:bool,
    /// No trusted external floor transport/counter implemented in this stage.
    pub external_independence_authenticated:bool,
    pub may_send_retry_cancel_verify_closed:bool,
}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum V2JournalFailpoint{None,BeforeAppend,PartialTail,AfterFileSync,AfterDirSync}
#[derive(Debug,Clone,PartialEq,Eq)]
pub enum V2JournalError{
    Invalid(&'static str),Io(ErrorKind),Decode,Overflow,
}
impl From<io::Error> for V2JournalError{
    fn from(e:io::Error)->Self{Self::Io(e.kind())}
}
fn no(s:&'static str)->V2JournalError{V2JournalError::Invalid(s)}
fn digest(bytes:&[u8])->String{
    sha256(bytes).iter().map(|v|format!("{v:02x}")).collect()
}
fn is_hex_sha(s:&str)->bool{
    s.len()==64&&s.bytes().all(|b|b.is_ascii_hexdigit()&&!b.is_ascii_uppercase())
}
fn genesis_hash(owner:&V2ReviewState)->Result<String,V2JournalError>{
    Ok(digest(&serde_json::to_vec(&owner.genesis).map_err(|_|V2JournalError::Decode)?))
}
fn owner_hash(owner:&V2ReviewState)->Result<String,V2JournalError>{
    Ok(digest(&serde_json::to_vec(owner).map_err(|_|V2JournalError::Decode)?))
}
#[derive(Serialize)]
struct JournalHashInput<'a>{
    schema_version:u32,
    original_owner_generation:u64,
    revision:u64,
    immutable_genesis_sha256:&'a str,
    owner_state_sha256:&'a str,
    previous_entry_sha256:&'a str,
}
fn entry_hash(e:&V2JournalEntry)->Result<String,V2JournalError>{
    let h=JournalHashInput{
        schema_version:e.schema_version,
        original_owner_generation:e.original_owner_generation,
        revision:e.revision,
        immutable_genesis_sha256:&e.immutable_genesis_sha256,
        owner_state_sha256:&e.owner_state_sha256,
        previous_entry_sha256:&e.previous_entry_sha256,
    };
    Ok(digest(&serde_json::to_vec(&h).map_err(|_|V2JournalError::Decode)?))
}
pub fn journal_path(owner_path:&Path)->PathBuf{
    owner_path.with_extension("v2_revision_journal")
}
fn parent(owner_path:&Path)->Result<&Path,V2JournalError>{
    let p=owner_path.parent().filter(|p|!p.as_os_str().is_empty())
        .ok_or(no("missing local owner parent"))?;
    if !p.symlink_metadata()?.file_type().is_dir(){return Err(no("owner parent symlink"))}
    Ok(p)
}
fn check_log_regular(path:&Path)->Result<bool,V2JournalError>{
    match path.symlink_metadata(){
        Ok(m)=>{
            if !m.file_type().is_file(){return Err(no("journal not regular file"))}
            if m.len() as usize>MAX_LOG_BYTES{return Err(no("journal max size exceeded"))}
            Ok(true)
        }
        Err(e) if e.kind()==ErrorKind::NotFound=>Ok(false),
        Err(e)=>Err(e.into()),
    }
}
fn checked_entry(e:&V2JournalEntry,last:Option<&V2JournalEntry>)
    ->Result<(),V2JournalError>{
    if e.schema_version!=LOG_SCHEMA
        ||e.revision==0||e.original_owner_generation==0
        ||!is_hex_sha(&e.immutable_genesis_sha256)
        ||!is_hex_sha(&e.owner_state_sha256)
        ||!is_hex_sha(&e.previous_entry_sha256)
        ||!is_hex_sha(&e.entry_sha256)
        ||entry_hash(e)?!=e.entry_sha256{
        return Err(no("journal invalid schema/hash"))
    }
    match last{
        None if e.revision!=1||e.previous_entry_sha256!=ZERO_HASH=>{
            Err(no("journal must start at revision 1 without skipped events"))
        }
        Some(p) if e.revision!=p.revision.checked_add(1).ok_or(V2JournalError::Overflow)?
            ||e.original_owner_generation!=p.original_owner_generation
            ||e.immutable_genesis_sha256!=p.immutable_genesis_sha256
            ||e.previous_entry_sha256!=p.entry_sha256=>{
            Err(no("journal chain broken, source replaced, or generation jumped"))
        }
        _=>Ok(()),
    }
}
/// One revision frame validator reusable by the **offline** witness simulator.
 /// Frame checksum and exact predecessor are verified without trusting caller.
 pub fn verify_mock_witness_revision_frame(
     candidate:&V2JournalEntry,previous:Option<&V2JournalEntry>,
 )->Result<(),V2JournalError>{
     checked_entry(candidate,previous)
 }
 pub fn read_local_journal(owner_path:&Path)->Result<Vec<V2JournalEntry>,V2JournalError>{
    parent(owner_path)?;
    let path=journal_path(owner_path);
    if !check_log_regular(&path)?{
        return Err(no("missing original V2 revision journal"))
    }
    let mut bytes=Vec::new();
    File::open(&path)?.take((MAX_LOG_BYTES+1) as u64).read_to_end(&mut bytes)?;
    if bytes.is_empty()||bytes.len()>MAX_LOG_BYTES||bytes.last()!=Some(&b'\n'){
        return Err(no("missing, oversized, or torn journal tail"))
    }
    let mut out=Vec::new();
    for line in bytes.split(|b|*b==b'\n').filter(|l|!l.is_empty()){
        if out.len()>=MAX_ENTRIES||line.len()>4096{
            return Err(no("too many journal entries/oversized frame"))
        }
        let entry:V2JournalEntry=serde_json::from_slice(line)
            .map_err(|_|V2JournalError::Decode)?;
        checked_entry(&entry,out.last())?;
        out.push(entry);
    }
    if out.is_empty(){return Err(no("journal has no committed event"))}
    Ok(out)
}
/// Caller MUST already hold the original NativeStateStore flock. This append
/// follows (is not atomic WITH) owner CAS: a crash in-between requires
/// quarantined review. Not called from production mutation entry points.
pub fn append_current_revision_offline(
    owner_path:&Path,owner:&V2ReviewState,fp:V2JournalFailpoint,
)->Result<V2JournalEntry,V2JournalError>{
    owner.validate().map_err(|_|no("invalid V2 owner"))?;
    let parent=parent(owner_path)?;
    let path=journal_path(owner_path);
    let exists=check_log_regular(&path)?;
    let prior=if exists{read_local_journal(owner_path)?}else{Vec::new()};
    let expected=prior.last().map_or(1,|r|r.revision.saturating_add(1));
    if owner.revision!=expected{
        return Err(no("V2 owner/journal revision gap, duplicate or rollback: quarantine"))
    }
    if owner.revision>MAX_ENTRIES as u64{
        return Err(no("journal bounded capacity reached"))
    }
    let generation=owner.genesis.original_owner_generation;
    let genesis=genesis_hash(owner)?;
    if prior.last().is_some_and(|p|p.immutable_genesis_sha256!=genesis
        ||p.original_owner_generation!=generation){
        return Err(no("owner genesis differs from previous journal"))
    }
    let mut entry=V2JournalEntry{
        schema_version:LOG_SCHEMA,
        original_owner_generation:generation,
        revision:owner.revision,
        immutable_genesis_sha256:genesis,
        owner_state_sha256:owner_hash(owner)?,
        previous_entry_sha256:prior.last()
            .map(|e|e.entry_sha256.clone()).unwrap_or_else(||ZERO_HASH.to_owned()),
        entry_sha256:String::new(),
    };
    entry.entry_sha256=entry_hash(&entry)?;
    checked_entry(&entry,prior.last())?;
    let mut bytes=serde_json::to_vec(&entry).map_err(|_|V2JournalError::Decode)?;
    bytes.push(b'\n');
    let existing_len=fs::metadata(&path).map(|m|m.len() as usize).unwrap_or(0);
    if existing_len.checked_add(bytes.len()).ok_or(V2JournalError::Overflow)?>MAX_LOG_BYTES{
        return Err(no("journal log cap"))
    }
    if fp==V2JournalFailpoint::BeforeAppend{
        return Err(V2JournalError::Io(ErrorKind::Interrupted))
    }
    let mut file=if exists{
        OpenOptions::new().append(true).write(true).open(&path)?
    }else{
        OpenOptions::new().append(true).write(true).create_new(true).open(&path)?
    };
    if fp==V2JournalFailpoint::PartialTail{
        file.write_all(&bytes[..bytes.len()/2])?;
        return Err(V2JournalError::Io(ErrorKind::Interrupted))
    }
    file.write_all(&bytes)?;
    file.sync_all()?;
    if fp==V2JournalFailpoint::AfterFileSync{
        return Err(V2JournalError::Io(ErrorKind::Interrupted))
    }
    File::open(parent)?.sync_all()?;
    if fp==V2JournalFailpoint::AfterDirSync{
        return Err(V2JournalError::Io(ErrorKind::Interrupted))
    }
    Ok(entry)
}
/// Caller-held floor is an INPUT to test a prospective external anchor
/// contract. NO identity, signature, monotonicity or storage independence
/// validation exists yet. It is never an exchange/trading authorization.
pub fn review_owner_against_local_journal(
    owner:&V2ReviewState,
    journal:&[V2JournalEntry],
    claimed_floor:Option<&V2CallerHeldFloor>,
)->Result<V2JournalReview,V2JournalError>{
    owner.validate().map_err(|_|no("invalid V2 owner"))?;
    if journal.is_empty()||journal.len()>MAX_ENTRIES{
        return Err(no("journal absent/invalid count"))
    }
    for (idx,e) in journal.iter().enumerate(){
        checked_entry(e,if idx==0{None}else{Some(&journal[idx-1])})?;
    }
    let end=journal.last().ok_or(no("journal missing tip"))?;
    if end.revision!=owner.revision
        ||end.original_owner_generation!=owner.genesis.original_owner_generation
        ||end.immutable_genesis_sha256!=genesis_hash(owner)?
        ||end.owner_state_sha256!=owner_hash(owner)?{
        return Err(no("journal current revision/state differs from V2 owner"))
    }
    let mut matched=false;
    if let Some(floor)=claimed_floor{
        if floor.original_owner_generation!=owner.genesis.original_owner_generation
            ||floor.minimum_revision==0||floor.minimum_revision>owner.revision
            ||!is_hex_sha(&floor.exact_entry_sha256){
            return Err(no("floor generation/revision invalid or from future"))
        }
        let anchored=journal.get((floor.minimum_revision-1) as usize)
            .ok_or(no("missing externally supplied floor revision"))?;
        if anchored.revision!=floor.minimum_revision
            ||anchored.entry_sha256!=floor.exact_entry_sha256{
            return Err(no("owner/local journal below or divergent from supplied floor"))
        }
        matched=true;
    }
    Ok(V2JournalReview{
        current_revision:owner.revision,
        journal_entries:journal.len(),
        current_entry_sha256:end.entry_sha256.clone(),
        local_chain_consistent:true,
        supplied_floor_matches:matched,
        external_independence_authenticated:false,
        may_send_retry_cancel_verify_closed:false,
    })
}
pub fn review_disk_local_journal(
    owner_path:&Path,owner:&V2ReviewState,claimed_floor:Option<&V2CallerHeldFloor>,
)->Result<V2JournalReview,V2JournalError>{
    let journal=read_local_journal(owner_path)?;
    review_owner_against_local_journal(owner,&journal,claimed_floor)
}
