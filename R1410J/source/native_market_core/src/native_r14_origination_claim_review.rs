//! R90: optional write-once local provenance declaration for one R14
//! closing intent. It is not part of R14 execution authority. Not exchange
//! authentication, not a cross-store atomic CAS and never a resend permit.
use std::{fs::{self,File,OpenOptions},io::{Read,Write,ErrorKind},path::Path};
use serde::{Deserialize,Serialize};
use openssl::sha::sha256;
use crate::{
 execution::{Intent,IntentState},
 execution_store::DurableIntentStore,
 position_lifecycle::{CloseLeg,PositionLifecycle,LifecycleState},
 native_r15_original_owner_binding_review::{
  R15OriginalOwnerBinding,read_r15_original_owner_binding,
 },
 native_r14_r15_cross_store_cold_review::inspect_r89_r14_r15_v2_paper_cold_ack,
};
const MAX:usize=16_384;
#[derive(Debug,Clone,PartialEq,Eq)]
pub enum R90Error{Denied(&'static str),Io(ErrorKind)}
impl From<std::io::Error> for R90Error{
 fn from(e:std::io::Error)->Self{Self::Io(e.kind())}
}
fn deny(s:&'static str)->R90Error{R90Error::Denied(s)}
fn checksum<T:Serialize>(v:&T)->Result<String,R90Error>{
 let bytes=serde_json::to_vec(v).map_err(|_|deny("R90 serialization"))?;
 Ok(sha256(&bytes).iter().map(|n|format!("{n:02x}")).collect())
}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct R14OriginalCloseClaim{
 pub schema:u32,
 pub intent_id:String,pub client_id:String,
 pub close_leg:CloseLeg,pub venue:String,
 pub owner_generation:u64,pub owner_target_units_e12:u128,
 pub requested_close_units_e12:u128,
 pub original_okx_order_id:String,
 pub original_v2_genesis_sha256:String,
 pub original_owner_sidecar_sha256:String,
 pub local_only:bool,
 pub exchange_authenticated:bool,
 pub atomic_cross_store_proven:bool,
 pub may_send_or_retry:bool,
}
#[derive(Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
struct Sealed{claim:R14OriginalCloseClaim,sha256:String}
// Literal venue string deserialization must be owned, never &'static.

fn valid(c:&R14OriginalCloseClaim)->Result<(),R90Error>{
 if c.schema!=1||c.intent_id.is_empty()||c.client_id.is_empty()||
    c.intent_id.len()>256||c.client_id.len()>256||
    c.owner_generation==0||c.owner_target_units_e12==0||
    c.requested_close_units_e12==0||
    c.requested_close_units_e12>c.owner_target_units_e12||
    c.original_okx_order_id.is_empty()||
    c.original_okx_order_id.len()>256||
    c.original_v2_genesis_sha256.len()!=64||
    c.original_owner_sidecar_sha256.len()!=64||
    !c.local_only||c.exchange_authenticated||
    c.atomic_cross_store_proven||c.may_send_or_retry{
  return Err(deny("invalid or authoritative R14 original claim"))
 }
 for x in [&c.original_v2_genesis_sha256,&c.original_owner_sidecar_sha256]{
  if !x.bytes().all(|b|b.is_ascii_hexdigit()&&!b.is_ascii_uppercase()){
   return Err(deny("invalid local original fingerprint"))
  }
 }
 let correct=match c.close_leg{
  CloseLeg::Futures=>"BINANCE_USDT_M_BUY_COVER",
  CloseLeg::Spot=>"OKX_SPOT_SELL_CLOSE",
 };
 if c.venue!=correct{return Err(deny("R14 venue does not match close leg"))}
 Ok(())
}
pub fn derive_r14_origination_claim(
 intent:&Intent,r15:&PositionLifecycle,owner:&R15OriginalOwnerBinding
)->Result<R14OriginalCloseClaim,R90Error>{
 let p=r15.pending.as_ref().ok_or(deny("R15 has no prepared close"))?;
 if matches!(r15.state,LifecycleState::Closed|LifecycleState::ManualRequired)||
    !matches!(intent.state,IntentState::Prepared|IntentState::Acknowledged|IntentState::Unknown)||
    intent.id!=p.intent_id||intent.client_order_id.as_deref()!=Some(p.client_id.as_str())||
    owner.safety_generation!=r15.safety_generation||
    owner.target_units_e12!=r15.target_units||
    owner.original_spot_order_id.is_empty()||
    owner.may_send_or_retry||owner.independently_authenticated||
    owner.externally_antirollback_proven||
    p.requested_units==0||p.requested_units>r15.target_units{
  return Err(deny("R14 intent, R15 pending or R87 original owner differs"))
 }
 let venue=match p.leg{
  CloseLeg::Futures=>"BINANCE_USDT_M_BUY_COVER",
  CloseLeg::Spot=>"OKX_SPOT_SELL_CLOSE",
 }.to_string();
 let claim=R14OriginalCloseClaim{
  schema:1,intent_id:p.intent_id.clone(),client_id:p.client_id.clone(),
  close_leg:p.leg,venue,
  owner_generation:r15.safety_generation,
  owner_target_units_e12:r15.target_units,
  requested_close_units_e12:p.requested_units,
  original_okx_order_id:owner.original_spot_order_id.clone(),
  original_v2_genesis_sha256:owner.v2_immutable_genesis_sha256.clone(),
  original_owner_sidecar_sha256:checksum(owner)?,
  local_only:true,exchange_authenticated:false,
  atomic_cross_store_proven:false,may_send_or_retry:false,
 };
 valid(&claim)?;
 Ok(claim)
}
pub fn check_r14_origination_claim(
 claim:&R14OriginalCloseClaim,intent:&Intent,
 r15:&PositionLifecycle,owner:&R15OriginalOwnerBinding
)->Result<(),R90Error>{
 valid(claim)?;
 if *claim!=derive_r14_origination_claim(intent,r15,owner)?{
  return Err(deny("R14 same intent has foreign leg/generation/owner"))
 }
 Ok(())
}
fn directory(path:&Path)->Result<&Path,R90Error>{
 let parent=path.parent().ok_or(deny("R90 path has no parent"))?;
 if !parent.symlink_metadata()?.file_type().is_dir(){
  return Err(deny("R90 parent not directory"))
 }
 Ok(parent)
}
/// Crash-safe write-once local declaration, not an exchange ACK.
pub fn publish_r14_origination_claim_once(
 path:&Path,claim:&R14OriginalCloseClaim
)->Result<(),R90Error>{
 valid(claim)?;
 let parent=directory(path)?;
 let stage=path.with_extension("json.incomplete");
 if fs::symlink_metadata(path).is_ok()||fs::symlink_metadata(&stage).is_ok(){
  return Err(R90Error::Io(ErrorKind::AlreadyExists))
 }
 let bytes=serde_json::to_vec(&Sealed{
   claim:claim.clone(),sha256:checksum(claim)?
 }).map_err(|_|deny("R90 serialization"))?;
 if bytes.is_empty()||bytes.len()>MAX{return Err(deny("R90 large claim"))}
 let mut f=OpenOptions::new().write(true).create_new(true).open(&stage)?;
 f.write_all(&bytes)?;f.sync_all()?;
 fs::hard_link(&stage,path)?;
 File::open(parent)?.sync_all()?;
 fs::remove_file(&stage)?;
 File::open(parent)?.sync_all()?;
 Ok(())
}
pub fn read_r14_origination_claim(path:&Path)->Result<R14OriginalCloseClaim,R90Error>{
 directory(path)?;
 if fs::symlink_metadata(path.with_extension("json.incomplete")).is_ok(){
  return Err(deny("interrupted R14 origination publication"))
 }
 let meta=fs::symlink_metadata(path)?;
 if !meta.file_type().is_file()||meta.len()==0||meta.len()>MAX as u64{
  return Err(deny("R90 sidecar not bounded regular file"))
 }
 let mut raw=Vec::new();
 File::open(path)?.take(MAX as u64+1).read_to_end(&mut raw)?;
 let envelope:Sealed=serde_json::from_slice(&raw)
    .map_err(|_|deny("R90 corrupt claim JSON"))?;
 valid(&envelope.claim)?;
 if checksum(&envelope.claim)?!=envelope.sha256{
  return Err(deny("R90 claim checksum mismatch"))
 }
 Ok(envelope.claim)
}
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct R90ColdAudit{
 pub r14_state:IntentState,
 pub same_original_local_close_owner:bool,
 pub exchange_authenticated:bool,
 pub cross_store_atomic_proven:bool,
 pub may_send_retry_or_resume:bool,
 pub verified_closed:bool,
}
/// Read-only extension of the full R89 six-store cold check.
pub fn inspect_r90_seven_source_cold_claim(
 claim_path:&Path,r14_file:&Path,r15_file:&Path,r87_file:&Path,
 v2_file:&Path,close_archive:&Path,paper_file:&Path,
 base:&str,paper_seq:u64,paper_hash:&str
)->Result<R90ColdAudit,R90Error>{
 let previous=inspect_r89_r14_r15_v2_paper_cold_ack(
  r14_file,r15_file,r87_file,v2_file,close_archive,paper_file,
  base,paper_seq,paper_hash)
   .map_err(|_|deny("R89 six-store cold source does not agree"))?;
 if previous.may_send_or_retry_or_resume||previous.verified_closed_or_flat{
  return Err(deny("R89 source incorrectly authorized mutation"))
 }
 let claim=read_r14_origination_claim(claim_path)?;
 let owner=read_r15_original_owner_binding(r87_file)
   .map_err(|_|deny("R87 entry owner sidecar unreadable"))?;
 let r15_bytes=fs::read(r15_file)?;
 let r15:PositionLifecycle=serde_json::from_slice(&r15_bytes)
   .map_err(|_|deny("R15 source invalid"))?;
 let journal=DurableIntentStore::open(r14_file)?;
 let rows=journal.load()?;
 let matching:Vec<_>=rows.iter().filter(|x|x.id==claim.intent_id).collect();
 if matching.len()!=1{return Err(deny("R14 owner intent not unique"))}
 check_r14_origination_claim(&claim,matching[0],&r15,&owner)?;
 // Catch ordinary revisions of the sidecar during read, but not
 // malicious replacement or rollback of the full local directory.
 if read_r14_origination_claim(claim_path)?!=claim{
  return Err(deny("R90 certificate changed during cold review"))
 }
 Ok(R90ColdAudit{
  r14_state:previous.original_pending_state,
  same_original_local_close_owner:true,
  exchange_authenticated:false,cross_store_atomic_proven:false,
  may_send_retry_or_resume:false,verified_closed:false,
 })
}

/// P0-02: stricter READ-ONLY R90 terminal-v2/R15 owner join.
/// Cannot reconcile, send, retry, resume, finalize, or claim real exchange truth.
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct R90TerminalV2ColdAudit{
 pub terminal_v2_matches_staged_r15_close:bool,
 pub original_local_owner_matches:bool,
 pub r14_ack_only_not_authenticated_fill:bool,
 pub pending_exchange_reconciliation_required:bool,
 pub cross_store_atomic_proven:bool,
 pub exchange_authenticated:bool,
 pub may_send_retry_or_resume:bool,
 pub verified_closed:bool,
}
fn cold_source_digest(path:&Path)->Result<[u8;32],R90Error>{
 let m=fs::symlink_metadata(path)?;
 if !m.file_type().is_file()||m.len()==0||m.len()>16*1024*1024{
   return Err(deny("R90 terminal source is not bounded regular file"))
 }
 let mut bytes=Vec::new();
 File::open(path)?.take(16*1024*1024+1).read_to_end(&mut bytes)?;
 if bytes.is_empty()||bytes.len()>16*1024*1024{
   return Err(deny("R90 terminal source changed size during read"))
 }
 Ok(sha256(&bytes))
}
/// An exact high-watermark supplied by the caller from a separate
/// failure domain. This data is NOT independently authenticated here.
#[derive(Debug,Clone,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct R90CallerSuppliedFloor{
 pub schema:u32,
 pub base:String,
 pub paper_sequence:u64,
 pub paper_hash:String,
 /// Exact SHA256 of claim, R14, R15, R87, V2, close archive, PAPER.
 pub seven_file_sha256:[String;7],
}
fn sha_hex(bytes:&[u8])->String{
 bytes.iter().map(|n|format!("{n:02x}")).collect()
}
fn lower_sha(s:&str)->bool{
 s.len()==64&&s.bytes().all(|b|b.is_ascii_hexdigit()&&!b.is_ascii_uppercase())
}
/// Only equality to caller-supplied values is verified. Authentication of
/// the floor and currentness must occur OUTSIDE this host/failure domain.
/// No local file is promoted into an independent floor by this method.
pub fn inspect_r90_terminal_v2_at_caller_floor(
 claim_path:&Path,r14_file:&Path,r15_file:&Path,r87_file:&Path,
 v2_file:&Path,close_archive:&Path,paper_file:&Path,
 base:&str,floor:&R90CallerSuppliedFloor
)->Result<R90TerminalV2ColdAudit,R90Error>{
 if floor.schema!=1||base.is_empty()||floor.base!=base||
    floor.paper_sequence==0||!lower_sha(&floor.paper_hash)||
    floor.seven_file_sha256.iter().any(|s|!lower_sha(s)){
   return Err(deny("invalid caller-supplied seven-source high-watermark"))
 }
 let paths=[claim_path,r14_file,r15_file,r87_file,
     v2_file,close_archive,paper_file];
 for (path,expected) in paths.iter().zip(floor.seven_file_sha256.iter()){
   if sha_hex(&cold_source_digest(path)?)!=*expected{
     return Err(deny("seven-source bytes differ from caller-supplied floor"))
   }
 }
 let result=inspect_r90_terminal_v2_r15_cold(
   claim_path,r14_file,r15_file,r87_file,v2_file,close_archive,paper_file,
   base,floor.paper_sequence,&floor.paper_hash)?;
 for (path,expected) in paths.iter().zip(floor.seven_file_sha256.iter()){
   if sha_hex(&cold_source_digest(path)?)!=*expected{
     return Err(deny("seven-source changed across floor review"))
   }
 }
 if result.may_send_retry_or_resume||result.verified_closed||
    result.exchange_authenticated||result.cross_store_atomic_proven{
    return Err(deny("caller-supplied floor cannot elevate review authority"))
 }
 Ok(result)
}
/// Signed seven-store latest-state response (offline verifier only).
pub const R90_LATEST_DOMAIN:&str="SPOTARB/R90/SEVEN-STORE/LATEST/V1";
#[derive(Debug,Clone,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct R90SignedSevenFloor{
 pub domain:String,pub witness_id:String,pub nonce_hex:String,
 pub witness_revision:u64,pub floor:R90CallerSuppliedFloor,
}
#[derive(Debug,Clone,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct R90SignedSevenReply{
 pub payload:R90SignedSevenFloor,pub signature_hex:String,
}
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct R90PinnedSevenLatest{
 pub trusted_id:String,pub public_key_hex:String,
 pub fresh_nonce_hex:String,pub minimum_revision:u64,
}
fn decode_hex_r90(s:&str,n:usize)->Result<Vec<u8>,R90Error>{
 if s.len()!=n*2||!s.bytes().all(|b|
     b.is_ascii_hexdigit()&&!b.is_ascii_uppercase()){
   return Err(deny("R90 latest witness hex invalid"))
 }
 s.as_bytes().chunks_exact(2).map(|pair|{
   let strval=std::str::from_utf8(pair)
     .map_err(|_|deny("R90 latest witness hex invalid"))?;
   u8::from_str_radix(strval,16).map_err(|_|deny("R90 latest hex invalid"))
 }).collect()
}
pub fn r90_signed_seven_message(t:&R90SignedSevenFloor)
 ->Result<Vec<u8>,R90Error>{
 if t.domain!=R90_LATEST_DOMAIN||t.witness_id.is_empty()||
    t.witness_id.len()>128||!t.witness_id.bytes().all(|c|
       c.is_ascii_alphanumeric()||matches!(c,b'_'|b'-'|b'.'))||
    t.nonce_hex.len()!=64||!lower_sha(&t.nonce_hex)||
    t.witness_revision==0||t.floor.schema!=1||
    t.floor.base.is_empty()||t.floor.paper_sequence==0||
    !lower_sha(&t.floor.paper_hash)||
    t.floor.seven_file_sha256.iter().any(|h|!lower_sha(h)){
   return Err(deny("R90 latest seven-store payload malformed"))
 }
 let mut message=R90_LATEST_DOMAIN.as_bytes().to_vec();
 message.push(b'\n');
 message.extend(serde_json::to_vec(t)
   .map_err(|_|deny("R90 seven-store signing encoding"))?);
 Ok(message)
}
/// Require a caller-pinned identity/key, fresh nonce and minimum
/// revision BEFORE comparing all seven stores. Never a trading permit.
/// Signature verification cannot prove remote durability or actual latest.
pub fn inspect_r90_terminal_v2_signed_latest(
 claim_path:&Path,r14_file:&Path,r15_file:&Path,r87_file:&Path,
 v2_file:&Path,close_archive:&Path,paper_file:&Path,
 base:&str,signed:&R90SignedSevenReply,pin:&R90PinnedSevenLatest
)->Result<R90TerminalV2ColdAudit,R90Error>{
 use openssl::{pkey::{PKey,Id},sign::Verifier};
 let msg=r90_signed_seven_message(&signed.payload)?;
 if pin.trusted_id!=signed.payload.witness_id||
    pin.fresh_nonce_hex!=signed.payload.nonce_hex||
    pin.minimum_revision==0||
    signed.payload.witness_revision<pin.minimum_revision{
   return Err(deny("R90 latest witness id/nonce/revision mismatch"))
 }
 let key=decode_hex_r90(&pin.public_key_hex,32)?;
 let sig=decode_hex_r90(&signed.signature_hex,64)?;
 let public=PKey::public_key_from_raw_bytes(&key,Id::ED25519)
   .map_err(|_|deny("R90 latest witness key invalid"))?;
 let mut verify=Verifier::new_without_digest(&public)
   .map_err(|_|deny("R90 latest verifier invalid"))?;
 if !verify.verify_oneshot(&sig,&msg)
   .map_err(|_|deny("R90 latest signature verification failure"))?{
    return Err(deny("R90 latest signature mismatch"))
 }
 inspect_r90_terminal_v2_at_caller_floor(
   claim_path,r14_file,r15_file,r87_file,v2_file,close_archive,
   paper_file,base,&signed.payload.floor)
}
pub fn inspect_r90_terminal_v2_r15_cold(
 claim_path:&Path,r14_file:&Path,r15_file:&Path,r87_file:&Path,
 v2_file:&Path,close_archive:&Path,paper_file:&Path,
 base:&str,paper_seq:u64,paper_hash:&str
)->Result<R90TerminalV2ColdAudit,R90Error>{
 let paths=[claim_path,r14_file,r15_file,r87_file,
   v2_file,close_archive,paper_file];
 let before:Vec<_>=paths.iter().map(|p|cold_source_digest(p))
   .collect::<Result<_,_>>()?;
 let earlier=inspect_r90_seven_source_cold_claim(
   claim_path,r14_file,r15_file,r87_file,v2_file,close_archive,
   paper_file,base,paper_seq,paper_hash)?;
 if earlier.r14_state!=IntentState::Acknowledged||
    earlier.may_send_retry_or_resume||earlier.verified_closed||
    earlier.exchange_authenticated||earlier.cross_store_atomic_proven{
   return Err(deny("R14 terminal ACK absent or R90 claims authority"))
 }
 let cold=crate::native_paper_durable_journal_review::
   inspect_journal_against_supplied_floor(paper_file,paper_seq,paper_hash)
   .map_err(|_|deny("terminal V2 journal floor absent or altered"))?;
 if cold.may_resume_paper||cold.may_trade_real||
    cold.state.closed!=0||cold.state.modeled_pnl_usdt!=0.0{
   return Err(deny("terminal PAPER must remain quarantined, not Closed"))
 }
 let seal=cold.sealed_terminal_v2.get(base)
    .ok_or(deny("terminal V2 source is not durable for this owner"))?;
 if cold.sealed_terminal_v2.len()!=1||
    seal.verified_closed||seal.may_send_or_retry||!seal.local_source_only||
    seal.base!=base{
   return Err(deny("terminal V2 other owner or forbidden authority"))
 }
 let life:PositionLifecycle=serde_json::from_slice(&fs::read(r15_file)?)
   .map_err(|_|deny("R15 terminal lifecycle bytes invalid"))?;
 if matches!(life.state,LifecycleState::Closed|LifecycleState::VerifiedClosed|
   LifecycleState::ManualRequired){
   return Err(deny("R15 terminal is not unresolved recovery"))
 }
 let pending=life.pending.as_ref().ok_or(deny("R15 pending close absent"))?;
 let staged=pending.recovery_terminal_filled_units
   .ok_or(deny("R15 terminal fill not durably staged"))?;
 let ledger=match pending.leg{
   CloseLeg::Futures=>&seal.futures_cover,
   CloseLeg::Spot=>&seal.spot_sell,
 };
 if !ledger.terminal||!ledger.cumulative_truth_present||
    !ledger.fee_evidence_complete||ledger.fills.is_empty()||
    ledger.exchange_order_id.as_deref().unwrap_or("").is_empty()||
    ledger.intent_id!=pending.intent_id||ledger.client_order_id!=pending.client_id||
    ledger.requested_qty_atoms!=pending.requested_units||
    staged==0||staged!=ledger.filled_qty_atoms()||
    staged!=pending.requested_units||
    life.safety_generation!=seal.original_generation||
    life.target_units!=seal.futures_cover.filled_qty_atoms()||
    life.target_units!=seal.spot_sell.filled_qty_atoms(){
   return Err(deny("R15 staged terminal quantity/identity differs from V2 seal"))
 }
 let owner=read_r15_original_owner_binding(r87_file)
   .map_err(|_|deny("R87 original owner cannot be reopened"))?;
 if owner.safety_generation!=seal.original_generation||
    owner.original_spot_order_id!=seal.original_spot_order_id||
    owner.target_units_e12!=life.target_units||
    owner.original_spot_order_id.is_empty(){
   return Err(deny("R87 original spot owner differs from terminal seal"))
 }
 for (path,expected) in paths.iter().zip(before.iter()){
   if cold_source_digest(path)?!=*expected{
     return Err(deny("R90 terminal source changed during cross-store audit"))
   }
 }
 Ok(R90TerminalV2ColdAudit{
   terminal_v2_matches_staged_r15_close:true,
   original_local_owner_matches:true,
   r14_ack_only_not_authenticated_fill:true,
   pending_exchange_reconciliation_required:true,
   cross_store_atomic_proven:false,exchange_authenticated:false,
   may_send_retry_or_resume:false,verified_closed:false,
 })
}
