//! TRADE2D2A. Persisted MOCK-only close replay latch; NEVER an order permit.
//! A simulated timeout permanently requires manual reconciliation.
use crate::authoritative_state::{NativeAuthoritativeState,NativeStateError};
use serde::{Serialize,Deserialize};
use openssl::sha::sha256;
fn deny(m:&'static str)->NativeStateError{NativeStateError::Invalid(m)}
fn digest<T:Serialize>(v:&T)->Result<String,NativeStateError>{
 let b=serde_json::to_vec(v).map_err(|_|NativeStateError::Json)?;
 Ok(sha256(&b).iter().map(|x|format!("{x:02x}")).collect())
}
fn valid(x:&str)->bool{(1..=48).contains(&x.len())&&x.bytes().all(|b|
 b.is_ascii_alphanumeric()||b==b'-'||b==b'_')}
#[derive(Clone,Copy,Debug,PartialEq,Eq,Serialize,Deserialize)]
pub enum DryRunClosePhase{PreparedMockOnly,UnknownManualOnly}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
pub struct NativeRecoveryDryRunLatch{
 source_generation:u64,source_hash:String,
 spot_client_id:String,future_client_id:String,
 spot_qty_e12:u128,short_qty_e12:u128,unmatched_e12:u128,
 phase:DryRunClosePhase,
 actual_order_sent:bool,may_send_order:bool,may_retry_order:bool,
 may_verify_closed:bool,may_release_cooldown:bool,
}
impl NativeRecoveryDryRunLatch{
 pub fn phase(&self)->DryRunClosePhase{self.phase}
 pub fn ids(&self)->(&str,&str){(&self.spot_client_id,&self.future_client_id)}
 pub fn amounts(&self)->(u128,u128,u128){
  (self.spot_qty_e12,self.short_qty_e12,self.unmatched_e12)
 }
 pub fn can_send(&self)->bool{self.may_send_order}
 pub fn can_retry(&self)->bool{self.may_retry_order}
 pub fn is_real_intent(&self)->bool{false}
 fn lineage_hash(s:&NativeAuthoritativeState,gen:u64)->Result<String,NativeStateError>{
  let e=s.entry.as_ref().ok_or(deny("mock close entry absent"))?;
  let sp=e.spot_prehedge_evidence.as_ref().ok_or(deny("mock spot absent"))?;
  let cp=e.recovery_checkpoint.as_ref().ok_or(deny("mock futures absent"))?;
  let q=e.futures_uncertain_quarantine.as_ref().ok_or(deny("mock quarantine absent"))?;
  let c5=e.futures_entry_window_evidence.as_ref().ok_or(deny("mock C5 absent"))?;
  let c6=e.prospective_joint_evidence.as_ref().ok_or(deny("mock C6 absent"))?;
  digest(&(&e.plan,sp,&cp.ledger,q,c5,c6,gen))
 }
 fn new_mock(
  s:&NativeAuthoritativeState,expected_gen:u64,source_hash:&str,
  spot_id:&str,future_id:&str,spot:u128,short:u128,
 )->Result<Self,NativeStateError>{
  let e=s.entry.as_ref().ok_or(deny("mock close no entry"))?;
  let original=e.spot_prehedge_evidence.as_ref().ok_or(deny("mock no original OKX"))?;
  let hedge=e.recovery_checkpoint.as_ref().ok_or(deny("mock no original Binance"))?;
  let c6=e.prospective_joint_evidence.as_ref().ok_or(deny("mock no C6"))?;
  let os=original.exact_ledger.filled_qty_atoms();
  let fs=hedge.ledger.filled_qty_atoms();
  let left=os.checked_sub(fs).ok_or(deny("mock overhedge"))?;
  if s.generation!=expected_gen||s.active.is_some()
    ||e.phase!=crate::authoritative_state::NativeEntryPhase::RecoveryRequired
    ||e.dryrun_close_latch.is_some()
    ||e.shadow_close_identity.is_some()
    ||spot!=os||short!=fs||spot==0||short==0
    ||c6.spot_available_net_atoms_e18!=spot.checked_mul(1_000_000)
      .ok_or(deny("spot overflow"))?
    ||c6.futures_short_atoms_e18!=short.checked_mul(1_000_000)
      .ok_or(deny("short overflow"))?
    ||source_hash!=Self::lineage_hash(s,expected_gen)?
    ||!valid(spot_id)||!valid(future_id)||spot_id==future_id
    ||spot_id==original.spot_client_id||spot_id==hedge.client_id
    ||future_id==original.spot_client_id||future_id==hedge.client_id
    ||e.intents.iter().any(|x|
      x.client_order_id.as_deref()==Some(spot_id)
       ||x.client_order_id.as_deref()==Some(future_id)){
      return Err(deny("mock close forged or unsafe identity"))
  }
  Ok(Self{
    source_generation:expected_gen,source_hash:source_hash.into(),
    spot_client_id:spot_id.into(),future_client_id:future_id.into(),
    spot_qty_e12:spot,short_qty_e12:short,unmatched_e12:left,
    phase:DryRunClosePhase::PreparedMockOnly,
    actual_order_sent:false,may_send_order:false,may_retry_order:false,
    may_verify_closed:false,may_release_cooldown:false,
  })
 }
 pub fn verify(&self,s:&NativeAuthoritativeState)->Result<(),NativeStateError>{
  let e=s.entry.as_ref().ok_or(deny("mock close entry deleted"))?;
  let sp=e.spot_prehedge_evidence.as_ref().ok_or(deny("mock spot deleted"))?;
  let cp=e.recovery_checkpoint.as_ref().ok_or(deny("mock future deleted"))?;
  if s.generation<=self.source_generation||s.active.is_some()
    ||e.phase!=crate::authoritative_state::NativeEntryPhase::RecoveryRequired
    ||e.shadow_close_identity.is_some()
    ||self.source_hash!=Self::lineage_hash(s,self.source_generation)?
    ||self.spot_qty_e12!=sp.exact_ledger.filled_qty_atoms()
    ||self.short_qty_e12!=cp.ledger.filled_qty_atoms()
    ||self.unmatched_e12!=self.spot_qty_e12
       .checked_sub(self.short_qty_e12).ok_or(deny("mock negative remaining"))?
    ||!valid(&self.spot_client_id)||!valid(&self.future_client_id)
    ||self.spot_client_id==self.future_client_id
    ||self.spot_client_id==sp.spot_client_id
    ||self.spot_client_id==cp.client_id
    ||self.future_client_id==sp.spot_client_id
    ||self.future_client_id==cp.client_id
    ||e.intents.iter().any(|x|
       x.client_order_id.as_deref()==Some(self.spot_client_id.as_str())
       ||x.client_order_id.as_deref()==Some(self.future_client_id.as_str()))
    ||self.actual_order_sent||self.may_send_order||self.may_retry_order
    ||self.may_verify_closed||self.may_release_cooldown{
    return Err(deny("mock close seal forged or authority escalated"))
  }
  Ok(())
 }
 pub fn valid_transition(&self,next:&Self)->bool{
  let mut a=self.clone();a.phase=next.phase;
  a==*next && matches!((self.phase,next.phase),
   (DryRunClosePhase::PreparedMockOnly,DryRunClosePhase::PreparedMockOnly)
   |(DryRunClosePhase::PreparedMockOnly,DryRunClosePhase::UnknownManualOnly)
   |(DryRunClosePhase::UnknownManualOnly,DryRunClosePhase::UnknownManualOnly))
 }
 pub(crate) fn mark_uncertain(&mut self){self.phase=DryRunClosePhase::UnknownManualOnly;}
}

use crate::{
 native_supervised_close_plan::{preview_full_inventory_close,ClosePlanError},
 market_metadata::NativeMarketMetadata,
 private_https::HttpExecutor,
 private_rest::{BinanceCredentials,OkxCredentials,OkxDomain},
 authoritative_state::NativeStateStore,
};
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum DryRunLatchWrite{Recorded,Idempotent}
impl NativeStateStore{
 /// Initial recording first re-verifies original exchange READ-ONLY proof.
 /// No API POST, approval, signed close request or order intent is created.
 #[allow(clippy::too_many_arguments)]
 pub fn record_mock_close_pair<E:HttpExecutor>(
  &self,http:&E,g:u64,market:&NativeMarketMetadata,
  now:u64,spot_bid:u128,future_ask:u128,
  okx:&OkxCredentials,bn:&BinanceCredentials,domain:OkxDomain,recv:u64,
  spot_id:&str,future_id:&str,
 )->Result<(NativeAuthoritativeState,DryRunLatchWrite),ClosePlanError>{
  let old=self.load().map_err(ClosePlanError::State)?
    .ok_or(ClosePlanError::State(NativeStateError::Generation))?;
  if old.generation!=g{
   return Err(ClosePlanError::State(NativeStateError::Generation))
  }
  if !valid(spot_id)||!valid(future_id)||spot_id==future_id{
   return Err(ClosePlanError::Mismatch)
  }
  if let Some(p)=old.entry.as_ref().and_then(|e|e.dryrun_close_latch.as_ref()){
   p.verify(&old).map_err(ClosePlanError::State)?;
   if p.ids()==(spot_id,future_id){
    return Ok((old,DryRunLatchWrite::Idempotent))
   }
   return Err(ClosePlanError::Mismatch)
  }
  let plan=preview_full_inventory_close(
    http,self,g,market,now,spot_bid,future_ask,okx,bn,domain,recv)?;
  let proof=NativeRecoveryDryRunLatch::new_mock(
    &old,g,&plan.source_identity_sha256,spot_id,future_id,
    plan.total_spot_to_sell_e12,plan.binance_short_to_buy_reduce_only_e12,
  ).map_err(ClosePlanError::State)?;
  let (new,_)=self.transact(g,|state|{
    if *state!=old{return Err(deny("mock source changed before CAS fsync"))}
    state.entry.as_mut().ok_or(deny("mock entry vanished"))?
      .dryrun_close_latch=Some(proof);
    Ok(())
  }).map_err(ClosePlanError::State)?;
  Ok((new,DryRunLatchWrite::Recorded))
 }
 /// An UNKNOWN synthetic response can only transition to manual review.
 /// This is not a network action and cannot be undone or retried as an order.
 pub fn record_mock_close_timeout(
  &self,g:u64,spot_id:&str,future_id:&str,
 )->Result<(NativeAuthoritativeState,DryRunLatchWrite),NativeStateError>{
  let old=self.load()?.ok_or(NativeStateError::Generation)?;
  if old.generation!=g{return Err(NativeStateError::Generation)}
  let p=old.entry.as_ref().and_then(|e|e.dryrun_close_latch.as_ref())
    .ok_or(deny("mock pair not durably recorded"))?;
  p.verify(&old)?;
  if p.ids()!=(spot_id,future_id){return Err(deny("wrong mock close ID"))}
  if p.phase()==DryRunClosePhase::UnknownManualOnly{
    return Ok((old,DryRunLatchWrite::Idempotent))
  }
  let (state,_)=self.transact(g,|state|{
    state.entry.as_mut().and_then(|e|e.dryrun_close_latch.as_mut())
     .ok_or(deny("mock close latch vanished"))?.mark_uncertain();
    Ok(())
  })?;
  Ok((state,DryRunLatchWrite::Recorded))
 }
}
