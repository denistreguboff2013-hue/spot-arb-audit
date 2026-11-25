//! Astra P0-02/P1-02 — write-once, OFFLINE review-only V2 owner genesis.
 //! The entire V2 original fee source, OKX bill window, and net SHORT proposal
 //! are in ONE sealed object. This is NOT an upgrade of NativeStateStore V1,
 //! a live owner CAS, a source authenticator, or an HTTP/order capability.
 use std::{
     fs::{self,File,OpenOptions},io::{self,Read,Write,ErrorKind},
     path::Path,
 };
 use serde::{Serialize,Deserialize};
 use openssl::sha::sha256;
 use crate::{
     native_trade2f3_first_leg_v2_checkpoint::FirstLegV2CheckpointPayload,
     native_trade2f3_v2_bill_ancestry::{
         BoundedSourceBillWindowV2,inspect_v2_supplied_bill_ancestry,
     },
     native_trade2f3_v2_net_hedge_contract::inspect_v2_net_hedge_proposal,
 };
 const MAX_BYTES:usize=262_144;
 #[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
 #[serde(deny_unknown_fields)]
 pub struct V2OwnerGenesisReview {
     pub schema_version:u32,
     pub owner_revision:u64,
     pub original_owner_generation:u64,
     pub checkpoint:FirstLegV2CheckpointPayload,
     pub original_okx_base_bill_window:BoundedSourceBillWindowV2,
     pub futures_intent_id:String,
     pub futures_client_id:String,
     pub planned_net_short_atoms_e12:u128,
     /// This does not represent an executed Binance short.
     pub original_owner_cas_proven:bool,
     pub complete_exchange_account_authenticated:bool,
     pub trading_or_close_authorized:bool,
 }
 #[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
 #[serde(deny_unknown_fields)]
 struct Envelope {
     owner:V2OwnerGenesisReview,
     local_checksum_sha256:String,
 }
 #[derive(Debug,Clone,PartialEq,Eq)]
 pub enum V2GenesisError {
     Io(ErrorKind),Schema,Decode,Checksum,Untrusted(&'static str),
 }
 impl From<io::Error> for V2GenesisError {
     fn from(e:io::Error)->Self{Self::Io(e.kind())}
 }
 fn deny(s:&'static str)->V2GenesisError{V2GenesisError::Untrusted(s)}
 fn hash(v:&V2OwnerGenesisReview)->Result<String,V2GenesisError>{
     let bytes=serde_json::to_vec(v).map_err(|_|V2GenesisError::Decode)?;
     Ok(sha256(&bytes).iter().map(|v|format!("{v:02x}")).collect())
 }
 fn check_location(path:&Path)->Result<&Path,V2GenesisError>{
     let parent=path.parent().filter(|x|!x.as_os_str().is_empty())
         .ok_or(deny("missing owner review directory"))?;
     if !parent.symlink_metadata()?.file_type().is_dir(){
         return Err(deny("owner review parent must be real directory"))
     }
     Ok(parent)
 }
 fn staging(path:&Path)->Result<std::path::PathBuf,V2GenesisError>{
     let name=path.file_name().and_then(|n|n.to_str())
         .ok_or(deny("invalid genesis file name"))?;
     if !name.ends_with(".json"){return Err(deny("genesis file extension must be json"))}
     Ok(path.with_file_name(format!("{name}.incomplete")))
 }
 pub(crate) fn revalidate(p:&V2OwnerGenesisReview,expected_generation:u64)
     ->Result<(),V2GenesisError>{
     if p.schema_version!=2||p.owner_revision!=1||expected_generation==0
         ||p.original_owner_generation!=expected_generation
         ||p.checkpoint.original_native_generation!=expected_generation{
         return Err(V2GenesisError::Schema)
     }
     if p.original_owner_cas_proven||p.complete_exchange_account_authenticated
         ||p.trading_or_close_authorized {
         return Err(deny("review owner cannot attest account, CAS or trading"))
     }
     if p.futures_intent_id.is_empty()||p.futures_client_id.is_empty()
         ||p.futures_intent_id==p.futures_client_id {
         return Err(deny("distinct futures identities required"))
     }
     inspect_v2_net_hedge_proposal(&p.checkpoint,expected_generation,
         &p.futures_intent_id,&p.futures_client_id,p.planned_net_short_atoms_e12)
         .map_err(|_|deny("V2 source/hedge identity or net quantity invalid"))?;
     inspect_v2_supplied_bill_ancestry(&p.checkpoint,expected_generation,
         &p.original_okx_base_bill_window,p.planned_net_short_atoms_e12)
         .map_err(|_|deny("V2 source/bill cash bracket incomplete or incompatible"))?;
     Ok(())
 }
 /// This is a typed genesis *proposal*, not an authoritative owner transition.
 /// Original generation remains caller supplied until future V1/V2 CAS wiring.
 pub fn prepare_v2_genesis(
     checkpoint:FirstLegV2CheckpointPayload,
     bills:BoundedSourceBillWindowV2,
     futures_intent_id:&str,futures_client_id:&str,
     planned_net_short_atoms_e12:u128,
 ) -> Result<V2OwnerGenesisReview,V2GenesisError> {
     let p=V2OwnerGenesisReview{
         schema_version:2,owner_revision:1,
         original_owner_generation:checkpoint.original_native_generation,
         checkpoint,original_okx_base_bill_window:bills,
         futures_intent_id:futures_intent_id.into(),
         futures_client_id:futures_client_id.into(),
         planned_net_short_atoms_e12,
         original_owner_cas_proven:false,
         complete_exchange_account_authenticated:false,
         trading_or_close_authorized:false,
     };
     revalidate(&p,p.original_owner_generation)?;
     Ok(p)
 }
 #[derive(Clone,Copy,Debug,PartialEq,Eq)]
 pub enum GenesisFailpoint {
     None,BeforeStage,PartialStage,AfterStageFileSync,
     AfterPublishBeforeDirSync,AfterParentSyncBeforeStageRemove,
 }
 /// Write *one* immutable schema-V2 snapshot in dedicated OFFLINE directory.
 /// Staging and final are the same inode via atomic hardlink O_EXCL publish.
 /// No ACK before both directory fsyncs; pending stage => recovery quarantine.
 /// Local tamper+hash rewrite / whole-directory rollback cannot be detected.
 pub fn persist_genesis_once(
     path:&Path,p:&V2OwnerGenesisReview,fp:GenesisFailpoint,
 ) -> Result<(),V2GenesisError>{
     let parent=check_location(path)?;
     revalidate(p,p.original_owner_generation)?;
     if fp==GenesisFailpoint::BeforeStage{
         return Err(V2GenesisError::Io(ErrorKind::Interrupted))
     }
     if path.symlink_metadata().is_ok(){
         return Err(V2GenesisError::Io(ErrorKind::AlreadyExists))
     }
     let stage=staging(path)?;
     let env=Envelope{owner:p.clone(),local_checksum_sha256:hash(p)?};
     let bytes=serde_json::to_vec(&env).map_err(|_|V2GenesisError::Decode)?;
     if bytes.len()>MAX_BYTES{return Err(V2GenesisError::Schema)}
     let mut f=OpenOptions::new().write(true).create_new(true).open(&stage)?;
     if fp==GenesisFailpoint::PartialStage{
         f.write_all(&bytes[..bytes.len()/2])?;
         return Err(V2GenesisError::Io(ErrorKind::Interrupted))
     }
     f.write_all(&bytes)?;
     f.sync_all()?;
     if fp==GenesisFailpoint::AfterStageFileSync{
         return Err(V2GenesisError::Io(ErrorKind::Interrupted))
     }
     // If a competitor published the final record while our stage existed,
     // no completed review transition may be overwritten by this genesis.
     if path.symlink_metadata().is_ok(){
         fs::remove_file(&stage)?;
         File::open(parent)?.sync_all()?;
         return Err(V2GenesisError::Io(ErrorKind::AlreadyExists))
     }
     fs::hard_link(&stage,path)?;
     if fp==GenesisFailpoint::AfterPublishBeforeDirSync{
         return Err(V2GenesisError::Io(ErrorKind::Interrupted))
     }
     File::open(parent)?.sync_all()?;
     if fp==GenesisFailpoint::AfterParentSyncBeforeStageRemove{
         return Err(V2GenesisError::Io(ErrorKind::Interrupted))
     }
     fs::remove_file(&stage)?;
     File::open(parent)?.sync_all()?;
     Ok(())
 }
 /// A valid local hash protects against accidental torn/corrupt records,
 /// not coherent adversarial rollback or forged exchange source content.
 /// An incomplete stage always quarantines the owner even if final exists.
 pub fn load_genesis(
     path:&Path,expected_generation:u64
 ) -> Result<V2OwnerGenesisReview,V2GenesisError> {
     check_location(path)?;
     let stage=staging(path)?;
     if stage.symlink_metadata().is_ok(){
         return Err(deny("pending owner stage demands manual recovery"))
     }
     if !path.symlink_metadata()?.file_type().is_file(){
         return Err(deny("owner genesis must be real regular file"))
     }
     let mut buf=Vec::new();
     File::open(path)?.take((MAX_BYTES+1) as u64).read_to_end(&mut buf)?;
     if buf.len()>MAX_BYTES||buf.is_empty(){return Err(V2GenesisError::Schema)}
     let envelope:Envelope=serde_json::from_slice(&buf).map_err(|_|V2GenesisError::Decode)?;
     if hash(&envelope.owner)?!=envelope.local_checksum_sha256{
         return Err(V2GenesisError::Checksum)
     }
     revalidate(&envelope.owner,expected_generation)?;
     Ok(envelope.owner)
 }
