//! Astra P0-03/P1-07 OFFLINE review: durable local witness commit intent.
//! Three independent disk commits do NOT form an atomic transaction.
//! Confirmed here = only local diagnostic receipt, NEVER external durability,
//! independently latest witness, Hedged or permission for exchange mutations.
use std::{
    fs::{self,File,OpenOptions},
    io::{self,ErrorKind,Read,Write},
    os::unix::io::AsRawFd,
    path::{Path,PathBuf},
};
use serde::{Serialize,Deserialize};
use openssl::sha::sha256;
use crate::{
    native_trade2f3_v2_versioned_store::V2ReviewState,
    native_trade2f3_v2_revision_journal::{
        V2JournalEntry,review_owner_against_local_journal,
    },
    native_trade2f3_v2_signed_floor::{
        V2SignedFloorEnvelope,V2ExternallyProvisionedVerifier,
        review_signed_floor_against_owner_journal,
    },
};
const MAX_STATE:usize=8192;
#[derive(Clone,Copy,Debug,PartialEq,Eq,Serialize,Deserialize)]
pub enum V2AckReviewPhase{Prepared,LocallyConfirmed}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum V2AckReviewFailpoint{None,BeforeWrite,PartialWrite,AfterFileSync,AfterRename,AfterDirSync}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum V2AckReviewOutcome{Created,AlreadyPrepared,LocallyConfirmed}
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum V2AckReviewError{Invalid(&'static str),Io(ErrorKind),Decode}
impl From<io::Error> for V2AckReviewError{
    fn from(e:io::Error)->Self{Self::Io(e.kind())}
}
fn deny(s:&'static str)->V2AckReviewError{V2AckReviewError::Invalid(s)}
fn hex(bytes:&[u8])->String{bytes.iter().map(|b|format!("{b:02x}")).collect()}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct V2AckReviewRecord{
    pub schema:u32,
    pub original_owner_generation:u64,
    pub immutable_genesis_sha256:String,
    pub revision:u64,
    pub target_entry_sha256:String,
    pub predecessor_entry_sha256:Option<String>,
    pub phase:V2AckReviewPhase,
}
#[derive(Clone,Debug,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
struct V2AckEnvelope{record:V2AckReviewRecord,sha256:String}
struct Lock{file:File}
impl Drop for Lock{
    fn drop(&mut self){unsafe{libc::flock(self.file.as_raw_fd(),libc::LOCK_UN)};}
}
pub struct V2DurableAckReview{root:PathBuf}
impl V2DurableAckReview{
    /// Existing isolated review fixture directory; same VM, NOT off-host.
    pub fn open(root:impl AsRef<Path>)->Result<Self,V2AckReviewError>{
        let p=root.as_ref();
        if !p.symlink_metadata()?.file_type().is_dir(){return Err(deny("ack directory not real"))}
        Ok(Self{root:p.to_path_buf()})
    }
    fn record_path(&self)->PathBuf{self.root.join("v2_ack_record.json")}
    fn pending_path(&self)->PathBuf{self.root.join("v2_ack_record.pending")}
    fn lock_path(&self)->PathBuf{self.root.join(".v2_ack.lock")}
    fn lock(&self)->Result<Lock,V2AckReviewError>{
        if !self.root.symlink_metadata()?.file_type().is_dir(){return Err(deny("ack root replaced"))}
        let p=self.lock_path();
        match p.symlink_metadata(){
            Ok(m) if !m.file_type().is_file()=>return Err(deny("ack lock not regular")),
            Err(e) if e.kind()!=ErrorKind::NotFound=>return Err(e.into()),
            _=>{},
        }
        let f=OpenOptions::new().read(true).write(true).create(true).open(p)?;
        if unsafe{libc::flock(f.as_raw_fd(),libc::LOCK_EX)}!=0{
            return Err(io::Error::last_os_error().into())
        }
        Ok(Lock{file:f})
    }
    fn read_locked(&self)->Result<Option<V2AckReviewRecord>,V2AckReviewError>{
        if self.pending_path().symlink_metadata().is_ok(){
            return Err(deny("interrupted local ACK write: quarantine"))
        }
        let p=self.record_path();
        match p.symlink_metadata(){
            Err(e) if e.kind()==ErrorKind::NotFound=>return Ok(None),
            Err(e)=>return Err(e.into()),
            Ok(m) if !m.file_type().is_file()||m.len()>MAX_STATE as u64=>
                return Err(deny("ack state type or size invalid")),
            _=>{},
        }
        let mut bytes=Vec::new();
        File::open(&p)?.take((MAX_STATE+1) as u64).read_to_end(&mut bytes)?;
        if bytes.is_empty()||bytes.len()>MAX_STATE{return Err(deny("ack empty or oversized"))}
        let envelope:V2AckEnvelope=serde_json::from_slice(&bytes)
            .map_err(|_|V2AckReviewError::Decode)?;
        let r=envelope.record;
        if r.schema!=1||r.revision==0||r.original_owner_generation==0{
            return Err(deny("ack schema or generation invalid"))
        }
        if envelope.sha256!=hex(&sha256(
            &serde_json::to_vec(&r).map_err(|_|V2AckReviewError::Decode)?)){
            return Err(deny("ack local checksum invalid"))
        }
        Ok(Some(r))
    }
    fn save_locked(&self,r:&V2AckReviewRecord,fp:V2AckReviewFailpoint)
        ->Result<(),V2AckReviewError>{
        let envelope=V2AckEnvelope{record:r.clone(),
            sha256:hex(&sha256(&serde_json::to_vec(r)
                .map_err(|_|V2AckReviewError::Decode)?))};
        let bytes=serde_json::to_vec(&envelope).map_err(|_|V2AckReviewError::Decode)?;
        if bytes.len()>MAX_STATE{return Err(deny("ack record too large"))}
        if fp==V2AckReviewFailpoint::BeforeWrite{
            return Err(V2AckReviewError::Io(ErrorKind::Interrupted))
        }
        let p=self.pending_path();
        let mut f=OpenOptions::new().write(true).create_new(true).open(&p)?;
        if fp==V2AckReviewFailpoint::PartialWrite{
            f.write_all(&bytes[..bytes.len()/2])?;
            return Err(V2AckReviewError::Io(ErrorKind::Interrupted))
        }
        f.write_all(&bytes)?;
        f.sync_all()?;
        if fp==V2AckReviewFailpoint::AfterFileSync{
            return Err(V2AckReviewError::Io(ErrorKind::Interrupted))
        }
        fs::rename(&p,self.record_path())?;
        if fp==V2AckReviewFailpoint::AfterRename{
            return Err(V2AckReviewError::Io(ErrorKind::Interrupted))
        }
        File::open(&self.root)?.sync_all()?;
        if fp==V2AckReviewFailpoint::AfterDirSync{
            return Err(V2AckReviewError::Io(ErrorKind::Interrupted))
        }
        Ok(())
    }
    pub fn read_review(&self)->Result<Option<V2AckReviewRecord>,V2AckReviewError>{
        let _lock=self.lock()?;
        self.read_locked()
    }
    fn proposal(owner:&V2ReviewState,frames:&[V2JournalEntry],
        prior:Option<&V2JournalEntry>)->Result<V2AckReviewRecord,V2AckReviewError>{
        review_owner_against_local_journal(owner,frames,None)
            .map_err(|_|deny("owner and local journal not aligned"))?;
        let tip=frames.last().ok_or(deny("missing local tip"))?;
        let expected_prior=if frames.len()==1{None}else{frames.get(frames.len()-2)};
        if prior!=expected_prior{
            return Err(deny("witness prior is not exact durable predecessor"))
        }
        Ok(V2AckReviewRecord{
            schema:1,original_owner_generation:tip.original_owner_generation,
            immutable_genesis_sha256:tip.immutable_genesis_sha256.clone(),
            revision:tip.revision,
            target_entry_sha256:tip.entry_sha256.clone(),
            predecessor_entry_sha256:prior.map(|p|p.entry_sha256.clone()),
            phase:V2AckReviewPhase::Prepared,
        })
    }
    /// Caller MUST hold original NativeStateStore owner flock. No network.
    /// Begin only when mock witness previously held exact revision N-1.
    /// This writes a local recoverable intent AFTER owner and journal CAS.
    pub fn prepare_review(&self,owner:&V2ReviewState,frames:&[V2JournalEntry],
        witnessed_prior:Option<&V2JournalEntry>,fp:V2AckReviewFailpoint)
        ->Result<V2AckReviewOutcome,V2AckReviewError>{
        let _lock=self.lock()?;
        let candidate=Self::proposal(owner,frames,witnessed_prior)?;
        if let Some(existing)=self.read_locked()?{
            let mut normalized=existing.clone();
            normalized.phase=V2AckReviewPhase::Prepared;
            if normalized!=candidate{
                return Err(deny("another or divergent transaction already staged"))
            }
            return Ok(V2AckReviewOutcome::AlreadyPrepared)
        }
        self.save_locked(&candidate,fp)?;
        Ok(V2AckReviewOutcome::Created)
    }
    /// Reads signed mock witness tip (supplied by test), verifies original
    /// owner/journal scope and exact intent; only writes local "confirmed".
    /// NO assertion remote receipt is current, independently trusted, or
    /// safe for a trading permit.
    pub fn confirm_review(&self,owner:&V2ReviewState,frames:&[V2JournalEntry],
        signed:&V2SignedFloorEnvelope,pin:&V2ExternallyProvisionedVerifier,
        fp:V2AckReviewFailpoint)
        ->Result<V2AckReviewOutcome,V2AckReviewError>{
        let _lock=self.lock()?;
        let mut stored=self.read_locked()?.ok_or(deny("missing prepared intent"))?;
        let target=Self::proposal(owner,frames,
            if frames.len()==1{None}else{frames.get(frames.len()-2)})?;
        let mut matching=stored.clone();
        matching.phase=V2AckReviewPhase::Prepared;
        if matching!=target{return Err(deny("intent source or revision differs from owner"))}
        review_signed_floor_against_owner_journal(owner,frames,signed,pin)
            .map_err(|_|deny("signed witness is stale, forged, ahead or divergent"))?;
        // Even already LocallyConfirmed must freshly recheck witness signed
        // response; do not trust an old local ACK on reboot.
        if stored.phase==V2AckReviewPhase::LocallyConfirmed{
            return Ok(V2AckReviewOutcome::LocallyConfirmed)
        }
        stored.phase=V2AckReviewPhase::LocallyConfirmed;
        self.save_locked(&stored,fp)?;
        Ok(V2AckReviewOutcome::LocallyConfirmed)
    }
}
