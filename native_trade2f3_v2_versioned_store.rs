//! Astra review: version-aware V1/V2 NativeStateStore **non-trading** path.
 //! V1 owner bytes/serializer remain untouched; V2 snapshots share its file
 //! path and exclusive flock/CAS, but never acquire execution permissions.
 //! WARNING: local SHA256 is not an external rollback/retention witness.
 use std::{fs::{self,File,OpenOptions},io::{self,Read,Write,ErrorKind},path::{Path,PathBuf}};
 use serde::{Serialize,Deserialize};
 use openssl::sha::sha256;
 use crate::native_trade2f3_v2_owner_genesis::{revalidate,V2OwnerGenesisReview};
 pub const VERSION:u32=2;
 const MAX_BYTES:usize=524_288;
 #[derive(Clone,Copy,Debug,PartialEq,Eq,Serialize,Deserialize)]
 pub enum V2ReviewPhase{Genesis,BillsReviewed,HedgeSizeReviewed,RecoveryOnly,FuturesEvidenceRecorded,FourLegCloseReviewed,EmergencyCloseRecorded,OpeningRecoveryRecorded}
 #[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
 #[serde(deny_unknown_fields)]
 pub struct V2ReviewState{
     pub schema_version:u32,
     pub revision:u64,
     pub genesis:V2OwnerGenesisReview,
     pub phase:V2ReviewPhase,
     /// Optional in old V2 review records; absent serializes byte-identically.
     #[serde(default,skip_serializing_if="Option::is_none")]
     pub c3_futures:Option<crate::native_trade2f3_v2_c3_futures_evidence::V2C3FuturesObservation>,
     /// Optional C5/C6/B4 synthetic close, never authoritative VerifiedClosed.
     #[serde(default,skip_serializing_if="Option::is_none")]
     pub close_four_leg:Option<crate::native_trade2f3_v2_four_leg_close_review::V2FourLegCloseObservation>,
     /// Mutually exclusive alternative for UNKNOWN / partial C5-C6, offline.
     #[serde(default,skip_serializing_if="Option::is_none")]
     pub emergency_close:Option<crate::native_trade2f3_v2_emergency_close::V2EmergencyCloseEvidence>,
     /// One-time offline opening C3 recovery for original PARTIAL or UNKNOWN.
     #[serde(default,skip_serializing_if="Option::is_none")]
     pub opening_recovery:Option<crate::native_trade2f3_v2_c3_opening_recovery::V2C3OpeningRecoveryEvidence>,
     pub may_send_or_cancel_or_verify_closed:bool,
     pub production_authorized:bool,
 }
 #[derive(Clone,Copy,Debug,PartialEq,Eq)]
 pub enum V2ReviewFailpoint{None,BeforeWrite,PartialWrite,AfterFileSync,AfterRename}
 #[derive(Debug,Clone,PartialEq,Eq)]
 pub enum V2ReviewError{Io(ErrorKind),Decode,Checksum,Schema,Cas,Denied(&'static str)}
 impl From<io::Error> for V2ReviewError{
     fn from(e:io::Error)->Self{Self::Io(e.kind())}
 }
 fn deny(s:&'static str)->V2ReviewError{V2ReviewError::Denied(s)}
 impl V2ReviewState{
     pub fn from_genesis(genesis:V2OwnerGenesisReview)->Result<Self,V2ReviewError>{
         let s=Self{schema_version:VERSION,revision:1,genesis,
             phase:V2ReviewPhase::Genesis,c3_futures:None,close_four_leg:None,
             emergency_close:None,opening_recovery:None,
             may_send_or_cancel_or_verify_closed:false,production_authorized:false};
         s.validate()?;Ok(s)
     }
     pub fn validate(&self)->Result<(),V2ReviewError>{
         if self.schema_version!=VERSION||self.revision==0{
             return Err(V2ReviewError::Schema)
         }
         if self.may_send_or_cancel_or_verify_closed||self.production_authorized{
             return Err(deny("V2 review must never authorize exchange mutations"))
         }
         if (self.phase==V2ReviewPhase::Genesis&&self.revision!=1)
             ||(self.phase==V2ReviewPhase::BillsReviewed&&self.revision!=2)
             ||(self.phase==V2ReviewPhase::HedgeSizeReviewed&&self.revision!=3)
             ||(self.phase==V2ReviewPhase::FuturesEvidenceRecorded&&self.revision!=4)
             ||(self.phase==V2ReviewPhase::FourLegCloseReviewed&&self.revision!=5)
             ||(self.phase==V2ReviewPhase::EmergencyCloseRecorded&&self.revision!=5)
             ||(self.phase==V2ReviewPhase::OpeningRecoveryRecorded&&self.revision!=5)
             ||(self.phase==V2ReviewPhase::RecoveryOnly&&!(2..=6).contains(&self.revision)){
             return Err(deny("invalid review phase/revision sequence"))
         }
         revalidate(&self.genesis,self.genesis.original_owner_generation)
             .map_err(|_|deny("original fee source/bills/hedge mutated or untrusted"))?;
         if let Some(c3)=self.c3_futures.as_ref(){
             if !matches!(self.phase,
                 V2ReviewPhase::FuturesEvidenceRecorded|
                 V2ReviewPhase::FourLegCloseReviewed|
                 V2ReviewPhase::EmergencyCloseRecorded|
                 V2ReviewPhase::OpeningRecoveryRecorded|V2ReviewPhase::RecoveryOnly)
                 ||(self.phase==V2ReviewPhase::FuturesEvidenceRecorded&&self.revision!=4)
                 ||(matches!(self.phase,V2ReviewPhase::FourLegCloseReviewed|
                     V2ReviewPhase::EmergencyCloseRecorded|
                     V2ReviewPhase::OpeningRecoveryRecorded)&&self.revision!=5)
                 ||(self.phase==V2ReviewPhase::RecoveryOnly&&!(5..=6).contains(&self.revision)){
                 return Err(deny("C3 evidence cannot predate hedge-size review"))
             }
             crate::native_trade2f3_v2_c3_futures_evidence::inspect_v2_c3_futures_observation(
                 &self.genesis,c3
             ).map_err(|_|deny("invalid original futures C3 observation"))?;
         }else if matches!(self.phase,
             V2ReviewPhase::FuturesEvidenceRecorded|V2ReviewPhase::FourLegCloseReviewed|
             V2ReviewPhase::EmergencyCloseRecorded|
             V2ReviewPhase::OpeningRecoveryRecorded)
             ||(self.phase==V2ReviewPhase::RecoveryOnly&&self.revision>=5){
             return Err(deny("C3 review stage requires exact owned evidence"))
         }
         if let Some(close)=self.close_four_leg.as_ref(){
             if !((self.phase==V2ReviewPhase::FourLegCloseReviewed&&self.revision==5)
                 ||(self.phase==V2ReviewPhase::RecoveryOnly&&self.revision==6)){
                 return Err(deny("four-leg evidence outside closure ancestry"))
             }
             let mut predecessor=self.clone();
             predecessor.phase=V2ReviewPhase::FuturesEvidenceRecorded;
             predecessor.revision=4;
             predecessor.close_four_leg=None;
             predecessor.validate()?;
             crate::native_trade2f3_v2_four_leg_close_review::inspect_v2_four_leg_close(
                 &predecessor,close
             ).map_err(|_|deny("C5/C6/B4 original four-leg source invalid"))?;
         }else if self.phase==V2ReviewPhase::FourLegCloseReviewed{
             return Err(deny("four-leg phase without original close history"))
         }
         if let Some(emergency)=self.emergency_close.as_ref(){
             if self.close_four_leg.is_some()
                 ||!((self.phase==V2ReviewPhase::EmergencyCloseRecorded&&self.revision==5)
                     ||(self.phase==V2ReviewPhase::RecoveryOnly&&self.revision==6)){
                 return Err(deny("emergency witness overlaps normal closure or wrong phase"))
             }
             let mut predecessor=self.clone();
             predecessor.phase=V2ReviewPhase::FuturesEvidenceRecorded;
             predecessor.revision=4;
             predecessor.close_four_leg=None;
             predecessor.emergency_close=None;
             predecessor.validate()?;
             crate::native_trade2f3_v2_emergency_close::inspect_v2_emergency_close(
                 &predecessor,emergency
             ).map_err(|_|deny("untrusted V2 emergency closure witness"))?;
         }else if self.phase==V2ReviewPhase::EmergencyCloseRecorded{
             return Err(deny("emergency phase without original risk history"))
         }
         if let Some(recovery)=self.opening_recovery.as_ref(){
             if self.close_four_leg.is_some()||self.emergency_close.is_some()
                 ||!((self.phase==V2ReviewPhase::OpeningRecoveryRecorded&&self.revision==5)
                     ||(self.phase==V2ReviewPhase::RecoveryOnly&&self.revision==6)){
                 return Err(deny("opening recovery overlaps closing ownership"))
             }
             let mut predecessor=self.clone();
             predecessor.phase=V2ReviewPhase::FuturesEvidenceRecorded;
             predecessor.revision=4;
             predecessor.opening_recovery=None;
             predecessor.validate()?;
             crate::native_trade2f3_v2_c3_opening_recovery::inspect_v2_c3_opening_recovery(
                 &predecessor,recovery
             ).map_err(|_|deny("V2 opening C3 recovery ancestry invalid"))?;
         }else if self.phase==V2ReviewPhase::OpeningRecoveryRecorded{
             return Err(deny("opening recovery phase without original source"))
         }
         if self.phase==V2ReviewPhase::RecoveryOnly&&self.revision==6
             &&self.close_four_leg.is_none()&&self.emergency_close.is_none()
             &&self.opening_recovery.is_none(){
             return Err(deny("recovery revision six without owner close/open ancestry"))
         }
         Ok(())
     }
     pub fn next(&self,next:V2ReviewPhase)->Result<Self,V2ReviewError>{
         let allowed=matches!((self.phase,next),
             (V2ReviewPhase::Genesis,V2ReviewPhase::BillsReviewed)
             |(V2ReviewPhase::BillsReviewed,V2ReviewPhase::HedgeSizeReviewed))
             ||(next==V2ReviewPhase::RecoveryOnly&&self.phase!=V2ReviewPhase::RecoveryOnly);
         if !allowed{return Err(deny("invalid review transition, no recovery exit"))}
         let mut s=self.clone();
         s.revision=self.revision.checked_add(1).ok_or(V2ReviewError::Schema)?;
         s.phase=next;s.validate()?;Ok(s)
     }
 }
 #[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
 #[serde(deny_unknown_fields)]
 struct Envelope{
     v2_schema:u32,
     review:V2ReviewState,
     local_checksum_sha256:String,
 }
 fn digest(p:&V2ReviewState)->Result<String,V2ReviewError>{
     Ok(sha256(&serde_json::to_vec(p).map_err(|_|V2ReviewError::Decode)?)
         .iter().map(|v|format!("{v:02x}")).collect())
 }
 pub fn pending_path(path:&Path)->PathBuf{path.with_extension("v2_pending")}
 fn parent(path:&Path)->Result<&Path,V2ReviewError>{
     let parent=path.parent().filter(|p|!p.as_os_str().is_empty())
         .ok_or(deny("missing owner parent"))?;
     if !parent.symlink_metadata()?.file_type().is_dir(){
         return Err(deny("owner parent must be real directory"))
     }
     Ok(parent)
 }
 fn encode(state:&V2ReviewState)->Result<Vec<u8>,V2ReviewError>{
     state.validate()?;
     let env=Envelope{v2_schema:VERSION,review:state.clone(),
         local_checksum_sha256:digest(state)?};
     let bytes=serde_json::to_vec(&env).map_err(|_|V2ReviewError::Decode)?;
     if bytes.len()>MAX_BYTES{return Err(V2ReviewError::Schema)}
     Ok(bytes)
 }
 fn decode(bytes:&[u8])->Result<V2ReviewState,V2ReviewError>{
     if bytes.is_empty()||bytes.len()>MAX_BYTES{return Err(V2ReviewError::Schema)}
     let env:Envelope=serde_json::from_slice(bytes).map_err(|_|V2ReviewError::Decode)?;
     if env.v2_schema!=VERSION{return Err(V2ReviewError::Schema)}
     if digest(&env.review)?!=env.local_checksum_sha256{
         return Err(V2ReviewError::Checksum)
     }
     env.review.validate()?;Ok(env.review)
 }
 /// Only for exact version dispatch: no coercion, migration, or deserializing
 /// through floating point. Legacy values are parsed by untouched V1 load().
 pub fn detect_version(path:&Path)->Result<Option<u32>,V2ReviewError>{
     if pending_path(path).symlink_metadata().is_ok(){
         return Err(deny("unfinished V2 transition is quarantined"))
     }
     if !path.exists(){return Ok(None)}
     if !path.symlink_metadata()?.file_type().is_file(){
         return Err(deny("owner must be regular file"))
     }
     let mut bytes=Vec::new();
     File::open(path)?.take((MAX_BYTES+1) as u64).read_to_end(&mut bytes)?;
     if bytes.len()>MAX_BYTES||bytes.is_empty(){return Err(V2ReviewError::Schema)}
     let v:serde_json::Value=serde_json::from_slice(&bytes).map_err(|_|V2ReviewError::Decode)?;
     let object=v.as_object().ok_or(V2ReviewError::Decode)?;
     if object.contains_key("v2_schema") {
         if object.len()!=3||!object.contains_key("review")
             ||!object.contains_key("local_checksum_sha256"){
             return Err(deny("invalid V2 envelope field set"))
         }
         Ok(Some(VERSION))
     }else if object.len()==2&&object.contains_key("payload")
         &&object.contains_key("payload_sha256"){
         Ok(Some(1))
     }else{Err(deny("unknown or mixed owner envelope"))}
 }
 pub fn load_v2(path:&Path)->Result<V2ReviewState,V2ReviewError>{
     parent(path)?;
     if detect_version(path)?!=Some(VERSION){
         return Err(V2ReviewError::Schema)
     }
     let mut bytes=Vec::new();
     File::open(path)?.take((MAX_BYTES+1) as u64).read_to_end(&mut bytes)?;
     decode(&bytes)
 }
 /// Caller must hold original NativeStateStore flock. In the same file, V1
 /// create/replace cannot silently parse V2; this API cannot write over V1.
 pub fn create_v2(
     path:&Path,state:&V2ReviewState,fp:V2ReviewFailpoint,
 )->Result<(),V2ReviewError>{
     let p=parent(path)?;
     if state.phase!=V2ReviewPhase::Genesis||state.revision!=1{
         return Err(V2ReviewError::Schema)
     }
     let bytes=encode(state)?;
     if path.symlink_metadata().is_ok()||pending_path(path).symlink_metadata().is_ok(){
         return Err(V2ReviewError::Cas)
     }
     if fp==V2ReviewFailpoint::BeforeWrite{return Err(V2ReviewError::Io(ErrorKind::Interrupted))}
     let stage=pending_path(path);
     let mut f=OpenOptions::new().write(true).create_new(true).open(&stage)?;
     if fp==V2ReviewFailpoint::PartialWrite{
         f.write_all(&bytes[..bytes.len()/2])?;
         return Err(V2ReviewError::Io(ErrorKind::Interrupted))
     }
     f.write_all(&bytes)?;f.sync_all()?;
     if fp==V2ReviewFailpoint::AfterFileSync{
         return Err(V2ReviewError::Io(ErrorKind::Interrupted))
     }
     fs::hard_link(&stage,path)?;
     if fp==V2ReviewFailpoint::AfterRename{
         return Err(V2ReviewError::Io(ErrorKind::Interrupted))
     }
     File::open(p)?.sync_all()?;
     fs::remove_file(stage)?;
     File::open(p)?.sync_all()?;
     Ok(())
 }
 /// Caller must hold original NativeStateStore flock across load+compare+
 /// commit; CAS = exact local revision under flock, NOT remote account proof.
 pub fn advance_v2(
     path:&Path,expected_revision:u64,next:V2ReviewPhase,fp:V2ReviewFailpoint,
 )->Result<V2ReviewState,V2ReviewError>{
     let p=parent(path)?;
     let current=load_v2(path)?;
     if current.revision!=expected_revision{return Err(V2ReviewError::Cas)}
     let updated=current.next(next)?;
     persist_revision(path,p,&updated,fp)?;
     Ok(updated)
 }
 /// Called under the exact NativeStateStore flock. C3 is a one-time
 /// immutable record of synthetic account observations, NOT a future permit.
 pub fn record_c3_v2(
     path:&Path,expected_revision:u64,
     observation:crate::native_trade2f3_v2_c3_futures_evidence::V2C3FuturesObservation,
     fp:V2ReviewFailpoint,
 )->Result<V2ReviewState,V2ReviewError>{
     let p=parent(path)?;
     let current=load_v2(path)?;
     if current.revision!=expected_revision
         ||current.revision!=3
         ||current.phase!=V2ReviewPhase::HedgeSizeReviewed
         ||current.c3_futures.is_some(){
         return Err(V2ReviewError::Cas)
     }
     let mut updated=current.clone();
     updated.revision=4;
     updated.phase=V2ReviewPhase::FuturesEvidenceRecorded;
     updated.c3_futures=Some(observation);
     updated.validate()?;
     persist_revision(path,p,&updated,fp)?;
     Ok(updated)
 }
 /// Same NativeStateStore flock, revision 4 -> 5 only. OFFLINE CLOSE REVIEW
 /// is NOT position VerifiedClosed and never grants network capability.
 pub fn record_four_leg_close_v2(
     path:&Path,expected_revision:u64,
     close:crate::native_trade2f3_v2_four_leg_close_review::V2FourLegCloseObservation,
     fp:V2ReviewFailpoint,
 )->Result<V2ReviewState,V2ReviewError>{
     let p=parent(path)?;
     let current=load_v2(path)?;
     if current.revision!=4||expected_revision!=4
         ||current.phase!=V2ReviewPhase::FuturesEvidenceRecorded
         ||current.c3_futures.is_none()||current.close_four_leg.is_some(){
         return Err(V2ReviewError::Cas)
     }
     crate::native_trade2f3_v2_four_leg_close_review::inspect_v2_four_leg_close(&current,&close)
         .map_err(|_|deny("four-leg proof/identity/fees/qty denied"))?;
     let mut next=current.clone();
     next.revision=5;
     next.phase=V2ReviewPhase::FourLegCloseReviewed;
     next.close_four_leg=Some(close);
     next.validate()?;
     persist_revision(path,p,&next,fp)?;
     Ok(next)
 }
 /// Offline-only, mutually exclusive with full close. Local revision CAS
 /// is held by caller NativeStateStore original flock.
 pub fn record_emergency_close_v2(
     path:&Path,expected_revision:u64,
     evidence:crate::native_trade2f3_v2_emergency_close::V2EmergencyCloseEvidence,
     fp:V2ReviewFailpoint,
 )->Result<V2ReviewState,V2ReviewError>{
     let p=parent(path)?;
     let current=load_v2(path)?;
     if current.revision!=4||expected_revision!=4
         ||current.phase!=V2ReviewPhase::FuturesEvidenceRecorded
         ||current.c3_futures.is_none()
         ||current.close_four_leg.is_some()||current.emergency_close.is_some(){
         return Err(V2ReviewError::Cas)
     }
     crate::native_trade2f3_v2_emergency_close::inspect_v2_emergency_close(
         &current,&evidence
     ).map_err(|_|deny("emergency close invalid or unbound"))?;
     let mut next=current.clone();
     next.revision=5;
     next.phase=V2ReviewPhase::EmergencyCloseRecorded;
     next.emergency_close=Some(evidence);
     next.validate()?;
     persist_revision(path,p,&next,fp)?;
     Ok(next)
 }
 /// One-shot, nonauthorizing synthetic C3 opening recovery. Same
 /// owner file and original NativeStateStore flock (held by caller).
 pub fn record_opening_recovery_v2(
     path:&Path,expected_revision:u64,
     evidence:crate::native_trade2f3_v2_c3_opening_recovery::V2C3OpeningRecoveryEvidence,
     fp:V2ReviewFailpoint,
 )->Result<V2ReviewState,V2ReviewError>{
     let p=parent(path)?;
     let current=load_v2(path)?;
     if current.revision!=4||expected_revision!=4
         ||current.phase!=V2ReviewPhase::FuturesEvidenceRecorded
         ||current.c3_futures.is_none()
         ||current.close_four_leg.is_some()
         ||current.emergency_close.is_some()
         ||current.opening_recovery.is_some(){
         return Err(V2ReviewError::Cas)
     }
     crate::native_trade2f3_v2_c3_opening_recovery::inspect_v2_c3_opening_recovery(
         &current,&evidence
     ).map_err(|_|deny("opening C3 proof/owner/timing refused"))?;
     let mut next=current.clone();
     next.revision=5;
     next.phase=V2ReviewPhase::OpeningRecoveryRecorded;
     next.opening_recovery=Some(evidence);
     next.validate()?;
     persist_revision(path,p,&next,fp)?;
     Ok(next)
 }
 fn persist_revision(
     path:&Path,p:&Path,updated:&V2ReviewState,fp:V2ReviewFailpoint,
 )->Result<(),V2ReviewError>{
     let bytes=encode(updated)?;
     if fp==V2ReviewFailpoint::BeforeWrite{
         return Err(V2ReviewError::Io(ErrorKind::Interrupted))
     }
     let stage=pending_path(path);
     let mut f=OpenOptions::new().write(true).create_new(true).open(&stage)?;
     if fp==V2ReviewFailpoint::PartialWrite{
         f.write_all(&bytes[..bytes.len()/2])?;
         return Err(V2ReviewError::Io(ErrorKind::Interrupted))
     }
     f.write_all(&bytes)?;f.sync_all()?;
     if fp==V2ReviewFailpoint::AfterFileSync{
         return Err(V2ReviewError::Io(ErrorKind::Interrupted))
     }
     fs::rename(stage,path)?;
     if fp==V2ReviewFailpoint::AfterRename{
         // Never ACK an unsynced directory rename; no trading capability.
         return Err(V2ReviewError::Io(ErrorKind::Interrupted))
     }
     File::open(p)?.sync_all()?;
     Ok(())
 }
