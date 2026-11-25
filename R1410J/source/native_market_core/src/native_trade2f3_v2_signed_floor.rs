//! Astra P0-03/P1-07: OFFLINE signature-aware monotonic floor contract.
 //! Only verifies Ed25519 cryptography, a pinned public key supplied by an
 //! external operator, caller nonce and the actual V2 journal tip.
 //! DOES NOT prove a signer lives in another failure domain, or that a
 //! signed value is the latest durable remote witness state.
 //! NO network I/O, external key, credential, trade authority or promotion.
 use openssl::{pkey::{Id,PKey},sign::Verifier};
 use serde::{Serialize,Deserialize};
 use crate::{
     native_trade2f3_v2_versioned_store::V2ReviewState,
     native_trade2f3_v2_revision_journal::{
         V2JournalEntry,review_owner_against_local_journal,
     },
 };

 pub const FLOOR_PROTOCOL_VERSION:u32=1;
 pub const FLOOR_DOMAIN:&str="SPOTARB/V2/REVIEW-FLOOR/ED25519/V1";
 #[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
 #[serde(deny_unknown_fields)]
 pub struct V2SignedFloorPayload{
     pub schema_version:u32,
     pub domain:String,
     pub witness_id:String,
     pub request_nonce_hex:String,
     pub original_owner_generation:u64,
     pub immutable_genesis_sha256:String,
     pub latest_committed_revision:u64,
     pub latest_entry_sha256:String,
 }
 #[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
 #[serde(deny_unknown_fields)]
 pub struct V2SignedFloorEnvelope{
     pub payload:V2SignedFloorPayload,
     pub ed25519_signature_hex:String,
 }
 /// Pin MUST originate from verified trusted provisioning, NOT from envelope,
 /// owner, repo, or this host's writable disk. This stage cannot attest that.
 #[derive(Clone,Debug,PartialEq,Eq)]
 pub struct V2ExternallyProvisionedVerifier{
     pub witness_id:String,
     pub ed25519_public_key_hex:String,
     pub expected_nonce_hex:String,
 }
 #[derive(Clone,Copy,Debug,PartialEq,Eq)]
 pub enum V2WitnessQuarantine{
     LocalOwnerJournalDivergence,
     LocalBehindSignedFloor,
     AwaitingSignedFloorAdvance,
     EqualRevisionDifferentHash,
 }
 #[derive(Clone,Debug,PartialEq,Eq)]
 pub enum V2WitnessError{
     Malformed(&'static str),
     InvalidSignature,
     IdentityOrChallengeMismatch,
     NoTrustedKeyProvisioning,
     Quarantined(V2WitnessQuarantine),
 }
 #[derive(Clone,Debug,PartialEq,Eq)]
 pub struct V2SignedFloorReview{
     pub owner_revision:u64,
     pub signed_floor_revision:u64,
     pub signature_valid:bool,
     pub matches_local_tip:bool,
     /// Cryptographic validity of one response ≠ externally authenticated
     /// append-only monotonicity or durable fetch of CURRENT latest value.
     pub independent_monotonic_witness_proven:bool,
     pub remote_currentness_proven:bool,
     pub trading_authorized:bool,
 }
 fn no(s:&'static str)->V2WitnessError{V2WitnessError::Malformed(s)}
 fn lower_hex(s:&str,n:usize)->bool{
     s.len()==n*2&&s.bytes().all(|c|c.is_ascii_hexdigit()&&!c.is_ascii_uppercase())
 }
 fn from_hex(s:&str,n:usize)->Result<Vec<u8>,V2WitnessError>{
     if !lower_hex(s,n){return Err(no("hex field wrong size or non-canonical"))}
     let mut out=Vec::with_capacity(n);
     for c in s.as_bytes().chunks_exact(2){
         let value=(c[0] as char).to_digit(16).ok_or(no("hex malformed"))?*16
             +(c[1] as char).to_digit(16).ok_or(no("hex malformed"))?;
         out.push(value as u8);
     }
     Ok(out)
 }
 /// Canonical domain-separated bytes to be signed by OFF-HOST witness.
 /// Includes witness ID, per-request nonce, original V2 owner + SHA256 tip.
 pub fn signed_floor_message(p:&V2SignedFloorPayload)->Result<Vec<u8>,V2WitnessError>{
     if p.schema_version!=FLOOR_PROTOCOL_VERSION
         ||p.domain!=FLOOR_DOMAIN
         ||p.witness_id.is_empty()||p.witness_id.len()>128
         ||!p.witness_id.bytes().all(|v|v.is_ascii_alphanumeric()
             ||v==b'-'||v==b'_'||v==b'.'||v==b'/')
         ||!lower_hex(&p.request_nonce_hex,32)
         ||!lower_hex(&p.immutable_genesis_sha256,32)
         ||!lower_hex(&p.latest_entry_sha256,32)
         ||p.original_owner_generation==0||p.latest_committed_revision==0{
         return Err(no("invalid signed witness scope/domain/nonce/revision"))
     }
     serde_json::to_vec(p).map_err(|_|no("cannot encode signed floor payload"))
 }
 /// This only verifies a supplied signature against a pinned raw public key.
 /// The public key itself is NOT trusted unless provisioned out-of-band.
 pub fn verify_signed_floor_cryptography(
     signed:&V2SignedFloorEnvelope,
     config:&V2ExternallyProvisionedVerifier,
 )->Result<(),V2WitnessError>{
     let payload=signed_floor_message(&signed.payload)?;
     if config.witness_id!=signed.payload.witness_id
         ||config.witness_id.is_empty()
         ||config.expected_nonce_hex!=signed.payload.request_nonce_hex{
         return Err(V2WitnessError::IdentityOrChallengeMismatch)
     }
     if config.ed25519_public_key_hex.is_empty(){
         return Err(V2WitnessError::NoTrustedKeyProvisioning)
     }
     let key=from_hex(&config.ed25519_public_key_hex,32)?;
     let signature=from_hex(&signed.ed25519_signature_hex,64)?;
     let key=PKey::public_key_from_raw_bytes(&key,Id::ED25519)
         .map_err(|_|no("invalid Ed25519 public key"))?;
     let mut verifier=Verifier::new_without_digest(&key)
         .map_err(|_|no("OpenSSL Ed25519 unavailable"))?;
     if !verifier.verify_oneshot(&signature,&payload)
         .map_err(|_|no("Ed25519 signature decoding failed"))?{
         return Err(V2WitnessError::InvalidSignature)
     }
     Ok(())
 }
 /// Verified signature means signed payload integrity, NOT a latest-commit
 /// remote read. Always require exact signed floor equality to local tip
 /// before returning a non-trading diagnostic PASS.
 pub fn review_signed_floor_against_owner_journal(
     owner:&V2ReviewState,
     journal:&[V2JournalEntry],
     signed:&V2SignedFloorEnvelope,
     config:&V2ExternallyProvisionedVerifier,
 )->Result<V2SignedFloorReview,V2WitnessError>{
     verify_signed_floor_cryptography(signed,config)?;
     let local=review_owner_against_local_journal(owner,journal,None)
         .map_err(|_|V2WitnessError::Quarantined(
             V2WitnessQuarantine::LocalOwnerJournalDivergence))?;
     let tip=journal.last().ok_or(no("journal tip missing"))?;
     let p=&signed.payload;
     if p.original_owner_generation!=tip.original_owner_generation
         ||p.immutable_genesis_sha256!=tip.immutable_genesis_sha256{
         return Err(V2WitnessError::IdentityOrChallengeMismatch)
     }
     if p.latest_committed_revision>local.current_revision{
         return Err(V2WitnessError::Quarantined(
             V2WitnessQuarantine::LocalBehindSignedFloor))
     }
     if p.latest_committed_revision<local.current_revision{
         return Err(V2WitnessError::Quarantined(
             V2WitnessQuarantine::AwaitingSignedFloorAdvance))
     }
     if p.latest_entry_sha256!=local.current_entry_sha256{
         return Err(V2WitnessError::Quarantined(
             V2WitnessQuarantine::EqualRevisionDifferentHash))
     }
     Ok(V2SignedFloorReview{
         owner_revision:local.current_revision,
         signed_floor_revision:p.latest_committed_revision,
         signature_valid:true,matches_local_tip:true,
         independent_monotonic_witness_proven:false,
         remote_currentness_proven:false,
         trading_authorized:false,
     })
 }
 /// Diagnostic only: indicates which local crash window needs a separate
 /// verified external history read. Neither snapshot is ever repaired here.
 pub fn classify_owner_journal_crash_window(
     owner_revision:u64,
     journal_last_revision:u64,
 )->Option<V2WitnessQuarantine>{
     if owner_revision==journal_last_revision{None}
     else{Some(V2WitnessQuarantine::LocalOwnerJournalDivergence)}
 }
