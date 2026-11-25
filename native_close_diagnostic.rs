//! R32D6C8B2B2B2: durable UNBOUND close diagnostic, NEVER a trading authority.
//!
//! IMPORTANT: NativePositionBundle currently lacks immutable initial position
//! ID + policy digest. Therefore this journal records only *observations* and
//! deliberately rejects a Native-active/legacy-active source on ordinary Rust.
//! It does not release cooldown, mark VerifiedClosed, change native state, or
//! claim exchange-global completeness.
//!
//! Independent journal lock, generation/CAS, checksum, file fsync, rename,
//! directory fsync; crash failpoints are restricted to this diagnostic store.
//! It is NOT an ACK-history floor and cannot prove protection against an
//! external rollback of all files. This sidecar cannot substitute for the
//! authoritative state transaction boundary.
use crate::{
    authoritative_state::{NativeStateError, NativeStateStore},
    native_close_readonly::{DualCloseCandidate, DualCloseReadOnlyCandidate},
    native_close_window::CloseSourceObservation,
};
use openssl::sha::sha256;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    os::fd::AsRawFd,
    path::{Path, PathBuf},
};

const SCHEMA:u32=1;
const MAX_RECORDS:usize=256;
const MAX_BYTES:u64=2*1024*1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CloseDiagnosticError {
    Io(io::ErrorKind),
    Json, HashMismatch, Invalid(&'static str),
    Generation, State(NativeStateError),
}
impl From<io::Error> for CloseDiagnosticError {
    fn from(v:io::Error)->Self{Self::Io(v.kind())}
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CloseDiagnosticWrite { Recorded, Idempotent }
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CloseDiagnosticFailpoint {
    None, BeforeWrite, AfterWrite, AfterFileSync, AfterRename,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnboundCloseRecord {
    observation_id:String,
    /// Sourced from caller, and explicitly *NOT* a durable position identity.
    caller_position_label:String,
    source_state_generation:u64,
    source_state_sha256:String,
    source_window_at_ms:u64,
    candidate_sha256:String,
    original_position_ancestry_proven:bool,
    funding_lifetime_complete:bool,
    cross_venue_atomicity_proven:bool,
    verified_closed:bool,
    may_send_order:bool,
    may_release_real_cooldown:bool,
}
impl UnboundCloseRecord {
    pub fn observation_id(&self)->&str{&self.observation_id}
    pub fn candidate_digest(&self)->&str{&self.candidate_sha256}
    pub fn source_generation(&self)->u64{self.source_state_generation}
    pub fn ancestry_proven(&self)->bool{self.original_position_ancestry_proven}
    pub fn verified_closed(&self)->bool{self.verified_closed}
    pub fn may_send_order(&self)->bool{self.may_send_order}
    pub fn may_release_real_cooldown(&self)->bool{self.may_release_real_cooldown}
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CloseDiagnosticSnapshot {
    schema_version:u32,
    generation:u64,
    records:Vec<UnboundCloseRecord>,
}
impl CloseDiagnosticSnapshot {
    pub fn generation(&self)->u64{self.generation}
    pub fn records(&self)->&[UnboundCloseRecord]{&self.records}
    fn new()->Self{Self{schema_version:SCHEMA,generation:0,records:Vec::new()}}
    fn validate(&self)->Result<(),CloseDiagnosticError>{
        if self.schema_version!=SCHEMA || self.generation!=self.records.len() as u64 ||
            self.records.len()>MAX_RECORDS {
            return Err(CloseDiagnosticError::Invalid("diagnostic schema/generation/size"))
        }
        let mut ids=BTreeSet::new();
        for rec in &self.records {
            if rec.observation_id.is_empty()||rec.observation_id.len()>96
                || !rec.observation_id.bytes().all(|b|b.is_ascii_alphanumeric()||b==b'-'||b==b'_')
                || !ids.insert(&rec.observation_id)
                || rec.caller_position_label.is_empty()
                || rec.caller_position_label.len()>96
                || rec.source_state_generation==0
                || rec.source_window_at_ms==0
                || !sha_string_valid(&rec.source_state_sha256)
                || !sha_string_valid(&rec.candidate_sha256)
                || rec.original_position_ancestry_proven
                || rec.funding_lifetime_complete
                || rec.cross_venue_atomicity_proven
                || rec.verified_closed||rec.may_send_order||rec.may_release_real_cooldown {
                    return Err(CloseDiagnosticError::Invalid("diagnostic authority/record invalid"))
            }
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Envelope { payload:CloseDiagnosticSnapshot,sha256:String }
fn hex(bytes:&[u8])->String{
    const H:&[u8;16]=b"0123456789abcdef";
    let mut s=String::with_capacity(bytes.len()*2);
    for &b in bytes{s.push(H[(b>>4) as usize]as char);s.push(H[(b&15) as usize]as char)}
    s
}
fn sha_string_valid(s:&str)->bool{s.len()==64&&s.bytes().all(|x|x.is_ascii_hexdigit()&&!x.is_ascii_uppercase())}
fn digest<T:Serialize>(input:&T)->Result<String,CloseDiagnosticError>{
    let v=serde_json::to_vec(input).map_err(|_|CloseDiagnosticError::Json)?;
    Ok(hex(&sha256(&v)))
}
fn witness(
    id:&str,source_state_generation:u64,source_state_sha256:String,now_ms:u64,
    c:&DualCloseCandidate,receipt:&DualCloseReadOnlyCandidate,window:&CloseSourceObservation,
)->Result<UnboundCloseRecord,CloseDiagnosticError>{
    if id.is_empty()||id.len()>96||
        !id.bytes().all(|x|x.is_ascii_alphanumeric()||x==b'-'||x==b'_')
        ||now_ms==0||source_state_generation==0
        ||receipt.position_identity()!=c.position_identity
        ||receipt.spot_ledger().client_order_id!=c.spot.client_id
        ||receipt.futures_ledger().client_order_id!=c.futures.client_id
        ||receipt.spot_ledger().exchange_order_id.as_deref()!=Some(c.spot.exchange_order_id.as_str())
        ||receipt.futures_ledger().exchange_order_id.as_deref()!=Some(c.futures.exchange_order_id.as_str())
        ||receipt.spot_ledger().filled_qty_atoms()!=c.expected_qty_e12
        ||receipt.futures_ledger().filled_qty_atoms()!=c.expected_qty_e12
        ||c.created_at_ms>now_ms
        ||receipt.close_verified()||receipt.may_send_order()
        ||window.may_mark_verified_closed()||window.may_send_order()
        ||window.may_start_real_cooldown()||window.original_native_position_ancestry_proven()
        ||window.full_account_history_proven()
        ||window.funding_lifetime_complete()||window.cross_venue_atomicity_proven() {
            return Err(CloseDiagnosticError::Invalid("unsealed or overclaimed diagnostic"))
    }
    // serde_json::Value cannot hold some u128 e12*e12 notionals (>u64).
    // Serialize the typed tuple *directly* with serde_json::Serializer so
    // u128 values remain exact integers, never f64 or lossy JSON Value.
    let evidence=(
        "R32D6C8B2B2B2_UNBOUND",
        &c.position_identity,&c.base,
        &c.spot.symbol,&c.spot.client_id,&c.spot.exchange_order_id,
        &c.futures.symbol,&c.futures.client_id,&c.futures.exchange_order_id,
        c.expected_qty_e12,c.created_at_ms,
        receipt.spot_ledger(),receipt.futures_ledger(),
        window.counts(),window.account_reads(),window.observed_funding_e12(),
    );
    Ok(UnboundCloseRecord{
        observation_id:id.into(),caller_position_label:c.position_identity.clone(),
        source_state_generation,source_state_sha256,source_window_at_ms:now_ms,
        candidate_sha256:digest(&evidence)?,
        original_position_ancestry_proven:false,
        funding_lifetime_complete:false,cross_venue_atomicity_proven:false,
        verified_closed:false,may_send_order:false,may_release_real_cooldown:false,
    })
}
struct Lock(File);
impl Drop for Lock {
    fn drop(&mut self){unsafe{libc::flock(self.0.as_raw_fd(),libc::LOCK_UN);}}
}
#[derive(Clone,Debug)]
pub struct NativeCloseDiagnosticStore { path:PathBuf }
impl NativeCloseDiagnosticStore {
    pub fn open(path:impl AsRef<Path>)->Result<Self,CloseDiagnosticError>{
        let path=path.as_ref().to_path_buf();
        if !path.is_absolute()||path.file_name().is_none(){
            return Err(CloseDiagnosticError::Invalid("diagnostic path must be absolute"))
        }
        let parent=path.parent().ok_or(CloseDiagnosticError::Invalid("missing parent"))?;
        fs::create_dir_all(parent)?;
        Ok(Self{path})
    }
    fn lock(&self)->Result<Lock,CloseDiagnosticError>{
        let f=OpenOptions::new().read(true).write(true).create(true)
            .open(self.path.with_extension("lock"))?;
        if unsafe{libc::flock(f.as_raw_fd(),libc::LOCK_EX)}!=0{
            return Err(CloseDiagnosticError::Io(io::Error::last_os_error().kind()))
        }
        Ok(Lock(f))
    }
    pub fn load(&self)->Result<CloseDiagnosticSnapshot,CloseDiagnosticError>{
        if !self.path.exists(){return Ok(CloseDiagnosticSnapshot::new())}
        if fs::metadata(&self.path)?.len()>MAX_BYTES{
            return Err(CloseDiagnosticError::Invalid("diagnostic exceeded maximum bytes"))
        }
        let mut bytes=Vec::new();
        File::open(&self.path)?.read_to_end(&mut bytes)?;
        let env:Envelope=serde_json::from_slice(&bytes).map_err(|_|CloseDiagnosticError::Json)?;
        env.payload.validate()?;
        if digest(&env.payload)?!=env.sha256{return Err(CloseDiagnosticError::HashMismatch)}
        Ok(env.payload)
    }
    fn persist(&self,s:&CloseDiagnosticSnapshot,point:CloseDiagnosticFailpoint)
         ->Result<(),CloseDiagnosticError>{
        s.validate()?;
        let payload=serde_json::to_vec(&Envelope{
            payload:s.clone(),sha256:digest(s)?
        }).map_err(|_|CloseDiagnosticError::Json)?;
        if payload.len() as u64>MAX_BYTES{return Err(CloseDiagnosticError::Invalid("journal exceeds bytes"))}
        if point==CloseDiagnosticFailpoint::BeforeWrite{
            return Err(CloseDiagnosticError::Io(io::ErrorKind::Other))
        }
        let tmp=self.path.with_extension("tmp");
        let mut f=OpenOptions::new().write(true).create(true).truncate(true).open(&tmp)?;
        f.write_all(&payload)?;
        if point==CloseDiagnosticFailpoint::AfterWrite{
            return Err(CloseDiagnosticError::Io(io::ErrorKind::Other))
        }
        f.sync_all()?;
        if point==CloseDiagnosticFailpoint::AfterFileSync{
            return Err(CloseDiagnosticError::Io(io::ErrorKind::Other))
        }
        fs::rename(&tmp,&self.path)?;
        if point==CloseDiagnosticFailpoint::AfterRename{
            return Err(CloseDiagnosticError::Io(io::ErrorKind::Other))
        }
        File::open(self.path.parent().ok_or(CloseDiagnosticError::Invalid("parent"))?)?.sync_all()?;
        Ok(())
    }
    /// The state has no active Native position or original policy identity.
    /// Thus this deliberately stores a diagnostic-only UNBOUND proof, and
    /// never an authority; if state is active, fail closed until properly
    /// certified NativePosition ancestry is added.
    #[allow(clippy::too_many_arguments)]
    pub fn record_unbound(
        &self,source:&NativeStateStore,source_generation:u64,
        expected_journal_generation:u64,observation_id:&str,observed_at_ms:u64,
        candidate:&DualCloseCandidate,receipt:&DualCloseReadOnlyCandidate,
        window:&CloseSourceObservation,fp:CloseDiagnosticFailpoint,
    )->Result<(CloseDiagnosticSnapshot,CloseDiagnosticWrite),CloseDiagnosticError>{
        let read_source=||->Result<String,CloseDiagnosticError>{
            let state=source.load().map_err(CloseDiagnosticError::State)?
                .ok_or(CloseDiagnosticError::Invalid("source state absent"))?;
            if state.generation!=source_generation{
                return Err(CloseDiagnosticError::Generation)
            }
            if state.active.is_some()||state.entry.is_some(){
                return Err(CloseDiagnosticError::Invalid("active/entry source cannot be recorded unbound"))
            }
            digest(&state)
        };
        let source_sha=read_source()?;
        let rec=witness(observation_id,source_generation,source_sha,
            observed_at_ms,candidate,receipt,window)?;
        let _lock=self.lock()?;
        let mut ledger=self.load()?;
        if let Some(old)=ledger.records.iter().find(|x|x.observation_id==rec.observation_id){
            if old==&rec && (expected_journal_generation==ledger.generation
                 ||expected_journal_generation.checked_add(1)==Some(ledger.generation)){
                return Ok((ledger,CloseDiagnosticWrite::Idempotent))
            }
            return Err(CloseDiagnosticError::Invalid("observation identity conflict"))
        }
        if ledger.generation!=expected_journal_generation{
            return Err(CloseDiagnosticError::Generation)
        }
        if ledger.records.len()>=MAX_RECORDS{
            return Err(CloseDiagnosticError::Invalid("bounded diagnostics full"))
        }
        // Recheck source state after acquiring journal owner lock, but do
        // NOT claim joint lock/atomicity with NativeStateStore or exchanges.
        if read_source()!=Ok(rec.source_state_sha256.clone()){
            return Err(CloseDiagnosticError::Generation)
        }
        ledger.records.push(rec);
        ledger.generation=ledger.generation.checked_add(1)
            .ok_or(CloseDiagnosticError::Generation)?;
        self.persist(&ledger,fp)?;
        Ok((ledger,CloseDiagnosticWrite::Recorded))
    }
}
