//! R87: write-once, OFFLINE, local R15 original-order identity sidecar.
//! Not an exchange signature, live CAS, external floor, or trading permission.
use std::{fs::{self,File,OpenOptions},io::{self,Read,Write,ErrorKind},path::Path};
use serde::{Serialize,Deserialize};
use openssl::sha::sha256;
use crate::{
 position_lifecycle::PositionLifecycle,
 native_trade2f3_v2_versioned_store::V2ReviewState,
 native_trade2f3_v2_four_leg_close_review::V2FourLegCloseObservation,
 native_paper_durable_journal_review::RecoveredPaper,
 native_paper_v2_closure_bridge_review::{
   inspect_r15_v2_paper_recovery_join,R15V2PaperRecoveryJoin,
 },
};
const SCHEMA:u32=1;
const MAX:usize=16_384;
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct R15OriginalOwnerBinding{
 pub schema:u32,
 pub safety_generation:u64,
 pub target_units_e12:u128,
 pub original_spot_intent_id:String,
 pub original_spot_client_id:String,
 pub original_spot_order_id:String,
 pub original_futures_intent_id:String,
 pub original_futures_client_id:String,
 pub v2_immutable_genesis_sha256:String,
 pub local_source_only:bool,
 pub independently_authenticated:bool,
 pub externally_antirollback_proven:bool,
 pub may_send_or_retry:bool,
}
#[derive(Clone,Debug,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope{
 owner:R15OriginalOwnerBinding,local_checksum_sha256:String,
}
#[derive(Debug,Clone,PartialEq,Eq)]
pub enum R87Error{
 Io(ErrorKind),Untrusted(&'static str),Encoding,
}
impl From<io::Error> for R87Error{
 fn from(e:io::Error)->Self{Self::Io(e.kind())}
}
fn deny(s:&'static str)->R87Error{R87Error::Untrusted(s)}
fn digest<T:Serialize>(value:&T)->Result<String,R87Error>{
 let bytes=serde_json::to_vec(value).map_err(|_|R87Error::Encoding)?;
 Ok(sha256(&bytes).iter().map(|n|format!("{n:02x}")).collect())
}
fn shape(c:&R15OriginalOwnerBinding)->Result<(),R87Error>{
 if c.schema!=SCHEMA||c.safety_generation==0||c.target_units_e12==0||
    !c.local_source_only||c.independently_authenticated||
    c.externally_antirollback_proven||c.may_send_or_retry||
    c.v2_immutable_genesis_sha256.len()!=64||
    !c.v2_immutable_genesis_sha256.bytes()
      .all(|n|n.is_ascii_hexdigit()&&!n.is_ascii_uppercase()){
   return Err(deny("R15 original-owner certificate may not assert authority"))
 }
 for field in [&c.original_spot_intent_id,&c.original_spot_client_id,
    &c.original_spot_order_id,&c.original_futures_intent_id,
    &c.original_futures_client_id]{
   if field.is_empty()||field.len()>256{return Err(deny("owner ID invalid"))}
 }
 Ok(())
}
pub fn prepare_r15_original_owner_binding(
 r15:&PositionLifecycle,v2:&V2ReviewState
)->Result<R15OriginalOwnerBinding,R87Error>{
 v2.validate().map_err(|_|deny("V2 source-shaped genesis not valid"))?;
 if r15.safety_generation!=v2.genesis.original_owner_generation||
    r15.target_units!=v2.genesis.planned_net_short_atoms_e12||
    r15.target_units==0{
   return Err(deny("R15 generation/qty differs from V2"))
 }
 let original=&v2.genesis.checkpoint.original;
 let p=R15OriginalOwnerBinding{
  schema:SCHEMA,safety_generation:r15.safety_generation,
  target_units_e12:r15.target_units,
  original_spot_intent_id:original.spot_intent_id.clone(),
  original_spot_client_id:original.spot_client_id.clone(),
  original_spot_order_id:original.spot_order_id.clone(),
  original_futures_intent_id:v2.genesis.futures_intent_id.clone(),
  original_futures_client_id:v2.genesis.futures_client_id.clone(),
  v2_immutable_genesis_sha256:digest(&v2.genesis)?,
  local_source_only:true,independently_authenticated:false,
  externally_antirollback_proven:false,may_send_or_retry:false,
 };
 shape(&p)?;Ok(p)
}
pub fn check_r15_original_owner_binding(
 p:&R15OriginalOwnerBinding,r15:&PositionLifecycle,v2:&V2ReviewState
)->Result<(),R87Error>{
 shape(p)?;
 let expected=prepare_r15_original_owner_binding(r15,v2)?;
 if *p!=expected{
  return Err(deny("R15 original source owner differs despite matching generation/qty"))
 }
 Ok(())
}
fn parent(path:&Path)->Result<&Path,R87Error>{
 let parent=path.parent().filter(|v|!v.as_os_str().is_empty())
   .ok_or(deny("owner binding parent missing"))?;
 if !parent.symlink_metadata()?.file_type().is_dir(){
   return Err(deny("owner binding parent is not real directory"))
 }
 Ok(parent)
}
/// A local write-once review artifact. Failed writes retain .incomplete,
/// forcing explicit review; never silently replace an old owner source.
pub fn persist_r15_original_owner_binding_once(
 path:&Path,p:&R15OriginalOwnerBinding
)->Result<(),R87Error>{
 shape(p)?;let directory=parent(path)?;
 let stage=path.with_extension("json.incomplete");
 if path.symlink_metadata().is_ok()||stage.symlink_metadata().is_ok(){
  return Err(R87Error::Io(ErrorKind::AlreadyExists))
 }
 let env=Envelope{owner:p.clone(),local_checksum_sha256:digest(p)?};
 let bytes=serde_json::to_vec(&env).map_err(|_|R87Error::Encoding)?;
 if bytes.len()>MAX{return Err(deny("local owner binding too large"))}
 let mut f=OpenOptions::new().write(true).create_new(true).open(&stage)?;
 f.write_all(&bytes)?;f.sync_all()?;
 fs::hard_link(&stage,path)?;
 File::open(directory)?.sync_all()?;
 fs::remove_file(stage)?;
 File::open(directory)?.sync_all()?;
 Ok(())
}
pub fn read_r15_original_owner_binding(
 path:&Path
)->Result<R15OriginalOwnerBinding,R87Error>{
 parent(path)?;
 let stage=path.with_extension("json.incomplete");
 if stage.symlink_metadata().is_ok(){
  return Err(deny("owner sidecar interrupted; manual recovery only"))
 }
 let m=path.symlink_metadata()?;
 if !m.file_type().is_file()||m.len()==0||m.len()>MAX as u64{
  return Err(deny("owner binding not safe regular file"))
 }
 let f=File::open(path)?;
 let mut raw=Vec::new();f.take(MAX as u64+1).read_to_end(&mut raw)?;
 let env:Envelope=serde_json::from_slice(&raw).map_err(|_|R87Error::Encoding)?;
 shape(&env.owner)?;
 if digest(&env.owner)?!=env.local_checksum_sha256{
  return Err(deny("owner binding checksum invalid"))
 }
 Ok(env.owner)
}
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct R87RecoveryReview{
 pub local_original_order_identity_matches:bool,
 pub r86:R15V2PaperRecoveryJoin,
 pub independent_exchange_proof:bool,
 pub externally_antirollback_proven:bool,
 pub may_retry_or_resume:bool,
}
/// Only for an already quarantined R86 local recovery; never grants orders.
pub fn inspect_r87_cold_owner_join(
 path:&Path,r15:&PositionLifecycle,recovered:&RecoveredPaper,
 base:&str,v2:&V2ReviewState,obs:&V2FourLegCloseObservation
)->Result<R87RecoveryReview,R87Error>{
 let certificate=read_r15_original_owner_binding(path)?;
 check_r15_original_owner_binding(&certificate,r15,v2)?;
 let r86=inspect_r15_v2_paper_recovery_join(r15,recovered,base,v2,obs)
   .map_err(|_|deny("R86 pending close differs from PAPER/V2 source"))?;
 if r86.may_retry_or_resume||r86.verified_closed||
    r86.independent_exchange_account_truth{
  return Err(deny("local owner certificate must not authorize trade"))
 }
 Ok(R87RecoveryReview{
  local_original_order_identity_matches:true,r86,
  independent_exchange_proof:false,externally_antirollback_proven:false,
  may_retry_or_resume:false,
 })
}
