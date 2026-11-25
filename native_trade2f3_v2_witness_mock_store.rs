//! Astra P0-03/P1-07 — persistent, zero-network witness SERVICE SIMULATOR.
 //! Independent application-level CAS lock + fsync + signed nonce-scoped
 //! read receipt. All paths are on SAME VM as the bot in this stage:
 //! NOT external durability, immutable WORM storage, trusted pin or currentness.
 //! Test-injected Ed25519 private key exists in test memory ONLY.
 //! No live services, trades, account API, network or production authorization.
 use std::{
     fs::{self,File,OpenOptions},
     io::{self,Read,Write,ErrorKind},
     os::unix::io::AsRawFd,
     path::{Path,PathBuf},
 };
 use serde::{Serialize,Deserialize};
 use openssl::{pkey::{PKey,Private},sign::Signer};
 use crate::{
     native_trade2f3_v2_revision_journal::{
         V2JournalEntry,verify_mock_witness_revision_frame,
     },
     native_trade2f3_v2_signed_floor::{
         V2SignedFloorPayload,V2SignedFloorEnvelope,FLOOR_PROTOCOL_VERSION,
         FLOOR_DOMAIN,signed_floor_message,
     },
     native_trade2f3_v2_remote_cas_contract::{
         V2SignedCasWrite,V2CasPinnedIdentity,verify_signed_cas_command,
     },
 };
 const MAX_LEDGER_BYTES:usize=512*1024;
 const MAX_LEDGER_ENTRIES:usize=512;
 const MAX_RECEIPT_NONCE_BYTES:usize=32;
 #[derive(Clone,Copy,Debug,PartialEq,Eq)]
 pub enum V2MockWitnessFp{None,BeforeWrite,PartialStage,AfterFileSync,AfterRename,AfterDirSync}
 #[derive(Clone,Copy,Debug,PartialEq,Eq)]
 pub enum V2MockCommitOutcome{Committed,AlreadyCommitted}
 #[derive(Clone,Debug,PartialEq,Eq)]
 pub enum V2MockWitnessError{
     Invalid(&'static str),Io(ErrorKind),Encode,
 }
 impl From<io::Error> for V2MockWitnessError{
     fn from(e:io::Error)->Self{Self::Io(e.kind())}
 }
 fn err(s:&'static str)->V2MockWitnessError{V2MockWitnessError::Invalid(s)}
 fn hex(bytes:&[u8])->String{
     bytes.iter().map(|v|format!("{v:02x}")).collect()
 }
 #[derive(Clone,Debug,Serialize,Deserialize)]
 #[serde(deny_unknown_fields)]
 struct V2MockLedger{
     schema_version:u32,
     /// Test-only state. No network, no secrets stored.
     entries:Vec<V2JournalEntry>,
 }
 struct HeldLock{file:File}
 impl Drop for HeldLock{
     fn drop(&mut self){unsafe{libc::flock(self.file.as_raw_fd(),libc::LOCK_UN);}}
 }
 #[derive(Clone,Debug)]
 pub struct V2OfflineWitnessMockStore{root:PathBuf}
 impl V2OfflineWitnessMockStore{
     /// ROOT must be an existing real directory, selected only by offline tests.
     pub fn open(root:impl AsRef<Path>)->Result<Self,V2MockWitnessError>{
         let root=root.as_ref();
         if !root.symlink_metadata()?.file_type().is_dir(){
             return Err(err("mock witness directory cannot be symlink"))
         }
         Ok(Self{root:root.to_path_buf()})
     }
     fn state_path(&self)->PathBuf{self.root.join("witness_state.json")}
     fn pending_path(&self)->PathBuf{self.root.join("witness_state.pending")}
     fn lock_path(&self)->PathBuf{self.root.join(".witness_cas.lock")}
     fn locked(&self)->Result<HeldLock,V2MockWitnessError>{
         if !self.root.symlink_metadata()?.file_type().is_dir(){
             return Err(err("witness root moved or replaced"))
         }
         let path=self.lock_path();
         // Tests control isolated temp directories; symlink even to /tmp denied.
         match path.symlink_metadata(){
             Ok(m) if !m.file_type().is_file()=>return Err(err("witness lock not regular")),
             Ok(_)=>{},
             Err(e) if e.kind()==ErrorKind::NotFound=>{},
             Err(e)=>return Err(e.into()),
         }
         let f=OpenOptions::new().read(true).write(true).create(true).open(path)?;
         if unsafe{libc::flock(f.as_raw_fd(),libc::LOCK_EX)}!=0{
             return Err(io::Error::last_os_error().into())
         }
         Ok(HeldLock{file:f})
     }
     fn load_locked(&self)->Result<Option<V2MockLedger>,V2MockWitnessError>{
         if self.pending_path().symlink_metadata().is_ok(){
             return Err(err("ambiguous pending mock witness commit quarantined"))
         }
         let path=self.state_path();
         match path.symlink_metadata(){
             Err(e) if e.kind()==ErrorKind::NotFound=>return Ok(None),
             Err(e)=>return Err(e.into()),
             Ok(m) if !m.file_type().is_file()||m.len()>MAX_LEDGER_BYTES as u64=>
                 return Err(err("witness state not regular or over budget")),
             _=>{},
         }
         let mut buf=Vec::new();
         File::open(path)?.take((MAX_LEDGER_BYTES+1) as u64).read_to_end(&mut buf)?;
         if buf.is_empty()||buf.len()>MAX_LEDGER_BYTES{
             return Err(err("corrupt or truncated witness state"))
         }
         let ledger:V2MockLedger=serde_json::from_slice(&buf)
             .map_err(|_|V2MockWitnessError::Encode)?;
         if ledger.schema_version!=1||ledger.entries.is_empty()
             ||ledger.entries.len()>MAX_LEDGER_ENTRIES{
             return Err(err("witness schema/empty/overflow"))
         }
         for (idx,e) in ledger.entries.iter().enumerate(){
             let previous=if idx==0{None}else{Some(&ledger.entries[idx-1])};
             verify_mock_witness_revision_frame(e,previous)
                 .map_err(|_|err("persisted witness hash/predecessor invalid"))?;
         }
         Ok(Some(ledger))
     }
     pub fn latest_offline(&self)->Result<Option<V2JournalEntry>,V2MockWitnessError>{
         let _lock=self.locked()?;
         Ok(self.load_locked()?.and_then(|s|s.entries.last().cloned()))
     }
     /// Separate witness-local CAS operation. EXACT next original journal
     /// revision, exact genesis and prior frame, no gaps or fork. Duplicate
     /// of exact durable tip returns AlreadyCommitted (caller must fresh-read).
     pub fn commit_offline(
         &self,entry:&V2JournalEntry,fp:V2MockWitnessFp,
     )->Result<V2MockCommitOutcome,V2MockWitnessError>{
         let _lock=self.locked()?;
         let old=self.load_locked()?;
         if let Some(state)=old.as_ref(){
             if let Some(last)=state.entries.last(){
                 if last==entry{
                     return Ok(V2MockCommitOutcome::AlreadyCommitted)
                 }
             }
         }
         let prior=old.as_ref().and_then(|r|r.entries.last());
         verify_mock_witness_revision_frame(entry,prior)
             .map_err(|_|err("failed exact witness CAS revision+hash chain"))?;
         let mut state=old.unwrap_or_else(||V2MockLedger{
             schema_version:1,entries:Vec::new(),
         });
         if state.entries.len()>=MAX_LEDGER_ENTRIES{
             return Err(err("mock witness capacity exhausted"))
         }
         state.entries.push(entry.clone());
         let bytes=serde_json::to_vec(&state).map_err(|_|V2MockWitnessError::Encode)?;
         if bytes.len()>MAX_LEDGER_BYTES{
             return Err(err("mock witness durable state byte cap"))
         }
         if fp==V2MockWitnessFp::BeforeWrite{
             return Err(V2MockWitnessError::Io(ErrorKind::Interrupted))
         }
         let stage=self.pending_path();
         let mut f=OpenOptions::new().write(true).create_new(true).open(&stage)?;
         if fp==V2MockWitnessFp::PartialStage{
             f.write_all(&bytes[..bytes.len()/2])?;
             return Err(V2MockWitnessError::Io(ErrorKind::Interrupted))
         }
         f.write_all(&bytes)?;
         f.sync_all()?;
         if fp==V2MockWitnessFp::AfterFileSync{
             return Err(V2MockWitnessError::Io(ErrorKind::Interrupted))
         }
         fs::rename(&stage,self.state_path())?;
         if fp==V2MockWitnessFp::AfterRename{
             // A later cold read may see new tip; no durable ACK was returned.
             return Err(V2MockWitnessError::Io(ErrorKind::Interrupted))
         }
         File::open(&self.root)?.sync_all()?;
         if fp==V2MockWitnessFp::AfterDirSync{
             return Err(V2MockWitnessError::Io(ErrorKind::Interrupted))
         }
         Ok(V2MockCommitOutcome::Committed)
     }
     /// Offline stand-in for a future authenticated remote CAS endpoint.
     /// Rejects unsigned/foreign commands and verifies exact previous tip.
     /// Existing commit_offline is a test fixture API, not a network route.
     /// No trusted remote identity/pin or external durability proven here.
     pub fn accept_signed_cas_offline(
         &self,cmd:&V2SignedCasWrite,pin:&V2CasPinnedIdentity,
         fp:V2MockWitnessFp,
     )->Result<V2MockCommitOutcome,V2MockWitnessError>{
         verify_signed_cas_command(cmd,pin)
             .map_err(|_|err("signed mock CAS client admission rejected"))?;
         let request=&cmd.request;
         let current=self.latest_offline()?;
         if current.as_ref()==Some(&request.candidate){
             return Ok(V2MockCommitOutcome::AlreadyCommitted)
         }
         match current{
             None if request.expected_predecessor_revision==0
                 &&request.expected_predecessor_entry_sha256.is_none()=>{},
             Some(prev) if prev.revision==request.expected_predecessor_revision
                 &&request.expected_predecessor_entry_sha256.as_deref()
                     ==Some(prev.entry_sha256.as_str())=>{},
             _=>return Err(err("signed mock CAS predecessor not current")),
         }
         // commit_offline itself rechecks exact tip under exclusive flock
         // to preserve CAS under concurrent contenders.
         self.commit_offline(&request.candidate,fp)
     }
     /// Signed latest read from DURABLE LOCAL mock ledger. Caller provides
     /// freshly generated random challenge and separately pinned public key.
     /// This is not proof of remote independent custody or latest globally.
     pub fn latest_signed_offline(
         &self,witness_id:&str,request_nonce_hex:&str,key:&PKey<Private>,
     )->Result<V2SignedFloorEnvelope,V2MockWitnessError>{
         let _lock=self.locked()?;
         let state=self.load_locked()?.ok_or(err("no witness genesis recorded"))?;
         let last=state.entries.last().ok_or(err("witness no tip"))?;
         let payload=V2SignedFloorPayload{
             schema_version:FLOOR_PROTOCOL_VERSION,
             domain:FLOOR_DOMAIN.to_owned(),
             witness_id:witness_id.to_owned(),
             request_nonce_hex:request_nonce_hex.to_owned(),
             original_owner_generation:last.original_owner_generation,
             immutable_genesis_sha256:last.immutable_genesis_sha256.clone(),
             latest_committed_revision:last.revision,
             latest_entry_sha256:last.entry_sha256.clone(),
         };
         if request_nonce_hex.len()!=MAX_RECEIPT_NONCE_BYTES*2{
             return Err(err("nonce wrong byte count"))
         }
         let bytes=signed_floor_message(&payload)
             .map_err(|_|err("witness receipt identity/nonce invalid"))?;
         let mut signer=Signer::new_without_digest(key)
             .map_err(|_|err("Ed25519 signer unavailable"))?;
         let signature=signer.sign_oneshot_to_vec(&bytes)
             .map_err(|_|err("Ed25519 signing failed"))?;
         Ok(V2SignedFloorEnvelope{
             payload,
             ed25519_signature_hex:hex(&signature),
         })
     }
 }
