//! Astra P0-02: zero-network paper fill/hedge and restart safety gate.
//! Authenticated exchange fill truth is NOT established by caller fixtures.
//! This code never authorizes real orders, cancel, hedge or VerifyClosed.
//! Use separately from the long-running public paper observer; no production IO.
use serde::{Serialize,Deserialize};
use std::collections::BTreeSet;
use crate::fill_economics::{LegFillLedger,FillEvent,EconError,ApplyFillResult};
use crate::native_trade2f_asset_fee::SpotFeeFill;

#[derive(Clone,Debug,PartialEq,Eq)]
pub enum PaperGateError {
 BadIdentity,WrongStage,Unresolved,FeeIncomplete,Overhedge,
 Source(EconError),BadSnapshot,Overflow,
}
impl From<EconError> for PaperGateError{
 fn from(e:EconError)->Self{Self::Source(e)}
}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaperOrderReport{
 pub intent_id:String,
 pub client_order_id:String,
 pub exchange_order_id:String,
 pub cumulative_qty_atoms:u128,
 pub average_price_atoms:Option<u128>,
 pub terminal:bool,
 pub status:String,
 pub observed_at_ms:u64,
 pub fills:Vec<FillEvent>,
}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaperUnknown {
 pub intent_id:String,
 pub client_order_id:String,
 pub explanation:String,
}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaperSpotNativeFeeProof{
 pub rows:Vec<SpotFeeFill>,
 pub total_debited_base_atoms:u128,
}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaperPartialGate{
 /// Source-shaped native BASE fees bound to original OKX fills; NOT signed account truth.
 #[serde(default,skip_serializing_if="Option::is_none")]
 pub native_spot_fee:Option<PaperSpotNativeFeeProof>,
 pub schema_version:u32,
 pub generation:u64,
 pub owner_id:String,
 pub spot:LegFillLedger,
 pub shorts:Vec<LegFillLedger>,
 /// Offline typed closing legs, persisted in the SAME CAS paper journal.
 /// Source-shaped mock fills only; NEVER actual verified exchange settlement.
 #[serde(default,skip_serializing_if="Option::is_none")]
 pub cover:Option<LegFillLedger>,
 #[serde(default,skip_serializing_if="Option::is_none")]
 pub sell:Option<LegFillLedger>,
 /// CAS-persisted digest of independently supplied MOCK V2 cross-check.
 /// This is evidence of local agreement only, NEVER real source authority.
 #[serde(default,skip_serializing_if="Option::is_none")]
 pub v2_local_alignment:Option<crate::native_paper_v2_closure_bridge_review::PaperV2LocalAlignment>,
 pub unknown:Option<PaperUnknown>,
 /// Active paper positions must be reconciled before resuming after restart.
 pub restart_quarantine:bool,
 /// Only the mock scenario may set this. Not account-wide exchange proof.
 pub exchange_truth_authenticated:bool,
 pub may_send_real_or_cancel:bool,
}
#[derive(Clone,Debug,PartialEq,Eq,Serialize)]
pub struct PaperExposure{
 pub spot_filled_atoms:u128,
 pub spot_net_delivered_atoms:u128,
 pub spot_base_fee_atoms:u128,
 pub futures_filled_atoms:u128,
 pub unhedged_base_atoms:u128,
 pub spot_terminal:bool,
 pub spot_fee_complete:bool,
 pub all_futures_terminal:bool,
 pub unknown_or_quarantine:bool,
 pub safe_to_propose_next_paper_hedge:bool,
 pub may_send_real_or_cancel:bool,
}
/// Quantity-only closing review: mock values, never exchange-authorized flat.
#[derive(Clone,Debug,PartialEq,Eq,Serialize)]
pub struct PaperCloseExposure{
 pub original_net_base_atoms:u128,
 pub opened_short_atoms:u128,
 pub cover_filled_atoms:u128,
 pub spot_sold_atoms:u128,
 pub remaining_short_atoms:u128,
 pub remaining_spot_base_atoms:u128,
 pub mock_both_closing_legs_complete:bool,
 pub independently_verified_closed:bool,
 pub real_execution_authorized:bool,
}
fn good_id(s:&str)->bool{
 !s.is_empty()&&s.len()<=96&&s.bytes().all(|b|
  b.is_ascii_alphanumeric()||b"._-".contains(&b))
}
fn validate_report(order:&LegFillLedger,r:&PaperOrderReport)->Result<(),PaperGateError>{
 if r.intent_id!=order.intent_id||r.client_order_id!=order.client_order_id
    ||r.exchange_order_id.is_empty()||r.observed_at_ms==0
    ||r.fills.len()>256||r.status.is_empty()
    ||r.cumulative_qty_atoms>order.requested_qty_atoms
    ||!good_id(&r.exchange_order_id)
    ||r.fills.iter().any(|f|{
       let base=order.symbol.strip_suffix("USDT").unwrap_or("");
       let quote=f.fee_currency.as_deref()==Some("USDT")
         && f.fee_cost_quote_atoms.is_some_and(|v|v>=0);
       let native=order.venue=="okx"&&order.side=="buy"
         && f.fee_currency.as_deref()==Some(base)
         && !base.is_empty()&&f.fee_cost_quote_atoms.is_none();
       !quote&&!native
    }){
   return Err(PaperGateError::BadIdentity)
 }
 if r.terminal && !matches!(r.status.as_str(),"filled"|"canceled"|"expired"){
   return Err(PaperGateError::BadIdentity)
 }
 if !r.terminal && !matches!(r.status.as_str(),"open"|"partially_filled"){
   return Err(PaperGateError::BadIdentity)
 }
 if order.terminal && (!r.terminal
    ||r.cumulative_qty_atoms!=order.cumulative_qty_atoms
    ||r.average_price_atoms!=order.vwap_atoms()){
    return Err(PaperGateError::WrongStage)
 }
 if r.status=="filled" && r.cumulative_qty_atoms!=order.requested_qty_atoms{
    return Err(PaperGateError::BadIdentity)
 }
 if order.exchange_order_id.as_deref().is_some_and(|prior|prior!=r.exchange_order_id){
   return Err(PaperGateError::BadIdentity)
 }
 Ok(())
}
fn apply_report(ledger:&LegFillLedger,r:&PaperOrderReport)->Result<LegFillLedger,PaperGateError>{
 validate_report(ledger,r)?;
 let mut proposed=ledger.clone();
 // Each fill must be idempotent, full duplicate payload or exact conflict.
 proposed.apply_cumulative_truth(r.cumulative_qty_atoms,r.average_price_atoms,
    r.terminal,Some(&r.status),Some(&r.exchange_order_id),
    Some(r.observed_at_ms),1)?;
 for f in &r.fills{
  match proposed.apply_fill(f.clone(),1)?{
    ApplyFillResult::Added|ApplyFillResult::DuplicateNoOp=>{}
  }
 }
 if r.terminal&&proposed.filled_qty_atoms()>0&&!proposed.fee_evidence_complete{
  // Terminal alone never proves complete fee/individual fill ancestry.
  // Keep the ledger but do not grant the next-leg permit.
 }
 Ok(proposed)
}
impl PaperPartialGate{
 /// Derive native fee truth from individually identified original OKX fills.
 /// Mixed USDT/BASE per-fill fees supported; other fee assets fail closed.
 fn fee_effect(&self)->Result<(u128,bool),PaperGateError>{
   let b=self.spot.symbol.strip_suffix("USDT").ok_or(PaperGateError::BadSnapshot)?;
   if b.is_empty(){return Err(PaperGateError::BadSnapshot)}
   let mut native_ids=BTreeSet::new();
   let mut filled_total=0u128;
   for (id,f) in &self.spot.fills{
     filled_total=filled_total.checked_add(f.qty_atoms).ok_or(PaperGateError::Overflow)?;
     match f.fee_currency.as_deref(){
       Some("USDT") if f.fee_cost_quote_atoms.is_some_and(|v|v>=0)=>{}
       Some(asset) if asset==b && f.fee_cost_quote_atoms.is_none()=>{
          native_ids.insert(id.as_str());
       }
       _=>return Err(PaperGateError::BadSnapshot),
     }
   }
   if native_ids.is_empty(){
     if self.native_spot_fee.is_some(){return Err(PaperGateError::BadSnapshot)}
     return Ok((0,self.spot.fee_evidence_complete))
   }
   let Some(proof)=self.native_spot_fee.as_ref() else {
     return Ok((0,false))
   };
   if !self.spot.terminal||!self.spot.cumulative_truth_present
      ||filled_total!=self.spot.filled_qty_atoms()
      ||proof.rows.len()!=native_ids.len()
      ||proof.rows.is_empty()||proof.rows.len()>256{
      return Err(PaperGateError::BadSnapshot)
   }
   let order=self.spot.exchange_order_id.as_deref().ok_or(PaperGateError::BadSnapshot)?;
   let mut seen=BTreeSet::new();
   let mut total=0u128;
   for row in &proof.rows{
      let fill=self.spot.fills.get(&row.trade_id).ok_or(PaperGateError::BadSnapshot)?;
      if !native_ids.contains(row.trade_id.as_str())||!seen.insert(row.trade_id.as_str())
        ||row.order_id!=order||row.side!="buy"||
        row.inst_id!=format!("{b}-USDT")||row.fee_ccy!=b||
        row.signed_fee_e12>0||row.at_ms==0||
        fill.qty_atoms!=row.gross_base_qty_e12||
        fill.price_atoms!=row.price_quote_e12||
        fill.trade_time_ms!=Some(row.at_ms)||
        fill.fee_currency.as_deref()!=Some(b)||
        fill.fee_cost_quote_atoms.is_some(){
         return Err(PaperGateError::BadSnapshot)
      }
      let debit=row.signed_fee_e12.checked_neg().ok_or(PaperGateError::Overflow)?;
      total=total.checked_add(u128::try_from(debit).map_err(|_|PaperGateError::Overflow)?)
          .ok_or(PaperGateError::Overflow)?;
   }
   if total!=proof.total_debited_base_atoms||
      total>=self.spot.filled_qty_atoms(){
      return Err(PaperGateError::BadSnapshot)
   }
   Ok((total,true))
 }
 /// Seal exact native fee by a separate durable PAPER owner transaction.
 /// Neither an unsigned source-shaped row nor its local journal is actual
 /// exchange attestation.
 pub fn record_native_spot_fee(&mut self,rows:Vec<SpotFeeFill>)
    ->Result<(),PaperGateError>{
    self.check()?;
    if self.restart_quarantine||self.unknown.is_some()||!self.shorts.is_empty()||
       !self.spot.terminal||rows.is_empty(){
       return Err(PaperGateError::Unresolved)
    }
    let total=rows.iter().try_fold(0u128,|n,r|{
       let neg=r.signed_fee_e12.checked_neg().ok_or(PaperGateError::Overflow)?;
       n.checked_add(u128::try_from(neg).map_err(|_|PaperGateError::Overflow)?)
         .ok_or(PaperGateError::Overflow)
    })?;
    let proof=PaperSpotNativeFeeProof{rows,total_debited_base_atoms:total};
    if let Some(old)=self.native_spot_fee.as_ref(){
       if old==&proof{return Ok(())}
       return Err(PaperGateError::WrongStage)
    }
    let mut candidate=self.clone();
    candidate.native_spot_fee=Some(proof);
    candidate.check()?;
    if !candidate.fee_effect()?.1{return Err(PaperGateError::FeeIncomplete)}
    *self=candidate;
    Ok(())
 }
 pub fn new(generation:u64,owner:&str,symbol:&str,intent:&str,client:&str,
       requested_spot_qty_atoms:u128)->Result<Self,PaperGateError>{
  if generation==0||![owner,symbol,intent,client].iter().all(|s|good_id(s)){
      return Err(PaperGateError::BadIdentity)
  }
  let spot=LegFillLedger::new("okx",symbol,"buy",intent,client,
       requested_spot_qty_atoms)?;
  Ok(Self{native_spot_fee:None,schema_version:1,generation,owner_id:owner.into(),spot,
       shorts:Vec::new(),cover:None,sell:None,v2_local_alignment:None,
       unknown:None,restart_quarantine:false,
       exchange_truth_authenticated:false,may_send_real_or_cancel:false})
 }
 fn check(&self)->Result<(),PaperGateError>{
  if self.schema_version!=1||self.generation==0||!good_id(&self.owner_id)
    ||self.exchange_truth_authenticated||self.may_send_real_or_cancel
    ||self.shorts.len()>32||self.spot.venue!="okx"||self.spot.side!="buy"
    ||self.spot.requested_qty_atoms==0{return Err(PaperGateError::BadSnapshot)}
  if self.shorts.iter().any(|s|s.venue!="binance"||s.side!="sell"
      ||s.symbol!=self.spot.symbol||s.requested_qty_atoms==0){
    return Err(PaperGateError::BadSnapshot)
  }
  let mut intents=BTreeSet::new();let mut clients=BTreeSet::new();
  for leg in std::iter::once(&self.spot).chain(self.shorts.iter())
      .chain(self.cover.iter()).chain(self.sell.iter()){
   if !good_id(&leg.intent_id)||!good_id(&leg.client_order_id)
      ||!intents.insert(&leg.intent_id)||!clients.insert(&leg.client_order_id)
      ||leg.filled_qty_atoms()>leg.requested_qty_atoms{
     return Err(PaperGateError::BadSnapshot)
   }
  }
  let summed=self.sum_short()?;
  let (fee_atoms,fee_complete)=self.fee_effect()?;
  let delivered=self.spot.filled_qty_atoms().checked_sub(fee_atoms)
      .ok_or(PaperGateError::BadSnapshot)?;
  if summed>delivered{return Err(PaperGateError::Overhedge)}
  if !self.shorts.is_empty() && !fee_complete{return Err(PaperGateError::FeeIncomplete)}
  if let Some(c)=&self.cover{
    if c.venue!="binance"||c.side!="buy"||c.symbol!=self.spot.symbol||
       !self.spot.terminal||!fee_complete||self.shorts.is_empty()||
       self.shorts.iter().any(|s|!s.terminal||!s.fee_evidence_complete)||
       summed!=delivered||c.requested_qty_atoms!=summed||
       c.filled_qty_atoms()>summed{return Err(PaperGateError::BadSnapshot)}
  }
  if let Some(s)=&self.sell{
    let c=self.cover.as_ref().ok_or(PaperGateError::BadSnapshot)?;
    if s.venue!="okx"||s.side!="sell"||s.symbol!=self.spot.symbol||
       s.requested_qty_atoms!=delivered||!c.terminal||
       !c.fee_evidence_complete||c.filled_qty_atoms()!=summed||
       s.filled_qty_atoms()>c.filled_qty_atoms(){
       return Err(PaperGateError::BadSnapshot)
    }
  }
  if let Some(m)=&self.v2_local_alignment{
    let digest=m.source_digest_sha256.as_bytes();
    let exact_cover=self.cover.as_ref().is_some_and(|c|
       c.terminal&&c.fee_evidence_complete&&c.filled_qty_atoms()==summed);
    let exact_sell=self.sell.as_ref().is_some_and(|s|
       s.terminal&&s.fee_evidence_complete&&s.filled_qty_atoms()==delivered);
    if m.original_generation!=self.generation||m.paper_parent_seq==0||
       m.v2_source_revision!=4||digest.len()!=64||
       !digest.iter().all(|b|b.is_ascii_hexdigit()&&!b.is_ascii_uppercase())||
       m.independent_exchange_truth||m.verified_closed||m.may_send_or_retry||
       !exact_cover||!exact_sell||self.unknown.is_some(){
       return Err(PaperGateError::BadSnapshot)
    }
  }
  if let Some(unknown)=&self.unknown{
    if unknown.explanation.is_empty()
       ||!std::iter::once(&self.spot).chain(self.shorts.iter())
          .chain(self.cover.iter()).chain(self.sell.iter()).any(|s|
          s.intent_id==unknown.intent_id&&s.client_order_id==unknown.client_order_id){
     return Err(PaperGateError::BadSnapshot)
    }
  }
  Ok(())
 }
 fn sum_short(&self)->Result<u128,PaperGateError>{
  self.shorts.iter().try_fold(0u128,|n,s|
    n.checked_add(s.filled_qty_atoms()).ok_or(PaperGateError::Overflow))
 }
 pub fn observe_spot(&mut self,r:&PaperOrderReport)->Result<(),PaperGateError>{
  self.check()?;
  if self.restart_quarantine||self.unknown.is_some()||!self.shorts.is_empty()
       ||self.native_spot_fee.is_some(){
    return Err(PaperGateError::Unresolved)
  }
  self.spot=apply_report(&self.spot,r)?;
  self.check()
 }
 pub fn begin_paper_short(&mut self,intent:&str,client:&str,qty:u128)
      ->Result<(),PaperGateError>{
  self.check()?;
  if self.restart_quarantine||self.unknown.is_some(){return Err(PaperGateError::Unresolved)}
  if !self.spot.terminal{return Err(PaperGateError::WrongStage)}
  let (fee_atoms,complete)=self.fee_effect()?;
  if !complete{return Err(PaperGateError::FeeIncomplete)}
  if !good_id(intent)||!good_id(client)||
    self.shorts.len()>=32||qty==0||
    self.shorts.iter().any(|s|!s.terminal||
      s.intent_id==intent||s.client_order_id==client)
    ||intent==self.spot.intent_id||client==self.spot.client_order_id{
    return Err(PaperGateError::WrongStage)
  }
  let current_short=self.sum_short()?;
  let available=self.spot.filled_qty_atoms()
     .checked_sub(fee_atoms).and_then(|v|v.checked_sub(current_short))
     .ok_or(PaperGateError::Overhedge)?;
  if qty>available{return Err(PaperGateError::Overhedge)}
  let short=LegFillLedger::new("binance",&self.spot.symbol,"sell",
    intent,client,qty)?;
  self.shorts.push(short);
  self.check()
 }
 pub fn observe_short(&mut self,r:&PaperOrderReport)->Result<(),PaperGateError>{
  self.check()?;
  if self.restart_quarantine||self.unknown.is_some(){return Err(PaperGateError::Unresolved)}
  let idx=self.shorts.iter().position(|s|s.intent_id==r.intent_id)
       .ok_or(PaperGateError::BadIdentity)?;
  let fresh=apply_report(&self.shorts[idx],r)?;
  let old=std::mem::replace(&mut self.shorts[idx],fresh);
  if let Err(e)=self.check(){
    self.shorts[idx]=old;return Err(e)
  }
  Ok(())
 }
 /// Begin Binance BUY cover only after fully owned, fully fee-proven opening.
 /// Offline only: this records an intent; it does not send an exchange order.
 pub fn begin_paper_cover(&mut self,intent:&str,client:&str,qty:u128)
    ->Result<(),PaperGateError>{
   let e=self.exposure()?;
   if e.unknown_or_quarantine||!e.spot_terminal||!e.spot_fee_complete||
      !e.all_futures_terminal||self.shorts.is_empty()||
      self.shorts.iter().any(|s|!s.fee_evidence_complete)||
      e.unhedged_base_atoms!=0||qty==0||qty!=e.futures_filled_atoms||
      self.cover.is_some()||self.sell.is_some(){
      return Err(PaperGateError::Unresolved)
   }
   if !good_id(intent)||!good_id(client){return Err(PaperGateError::BadIdentity)}
   let mut next=self.clone();
   next.cover=Some(LegFillLedger::new("binance",&self.spot.symbol,"buy",
      intent,client,qty)?);
   next.check()?;
   *self=next;Ok(())
 }
 pub fn observe_paper_cover(&mut self,r:&PaperOrderReport)
    ->Result<(),PaperGateError>{
   self.check()?;
   if self.restart_quarantine||self.unknown.is_some()||
      self.sell.is_some(){return Err(PaperGateError::Unresolved)}
   let mut next=self.clone();
   let c=next.cover.as_mut().ok_or(PaperGateError::WrongStage)?;
   *c=apply_report(c,r)?;
   next.check()?;*self=next;Ok(())
 }
 pub fn begin_paper_sell(&mut self,intent:&str,client:&str,qty:u128)
    ->Result<(),PaperGateError>{
   self.check()?;
   if self.restart_quarantine||self.unknown.is_some()||self.sell.is_some(){
      return Err(PaperGateError::Unresolved)
   }
   let c=self.cover.as_ref().ok_or(PaperGateError::WrongStage)?;
   if !c.terminal||!c.fee_evidence_complete||
      c.filled_qty_atoms()!=c.requested_qty_atoms||qty!=c.filled_qty_atoms(){
      return Err(PaperGateError::Unresolved)
   }
   if !good_id(intent)||!good_id(client){return Err(PaperGateError::BadIdentity)}
   let mut next=self.clone();
   next.sell=Some(LegFillLedger::new("okx",&self.spot.symbol,"sell",
      intent,client,qty)?);
   next.check()?;*self=next;Ok(())
 }
 pub fn observe_paper_sell(&mut self,r:&PaperOrderReport)
    ->Result<(),PaperGateError>{
   self.check()?;
   if self.restart_quarantine||self.unknown.is_some(){
      return Err(PaperGateError::Unresolved)
   }
   let mut next=self.clone();
   let s=next.sell.as_mut().ok_or(PaperGateError::WrongStage)?;
   *s=apply_report(s,r)?;
   next.check()?;*self=next;Ok(())
 }
 /// Only a local mock coverage summary; never release trading or VerifiedClosed.
 pub fn paper_close_exposure(&self)->Result<PaperCloseExposure,PaperGateError>{
   let e=self.exposure()?;
   let cover=self.cover.as_ref().map_or(0,|c|c.filled_qty_atoms());
   let sell=self.sell.as_ref().map_or(0,|s|s.filled_qty_atoms());
   let complete=self.sell.as_ref().is_some_and(|s|s.terminal&&s.fee_evidence_complete
        &&s.filled_qty_atoms()==e.spot_net_delivered_atoms)
        &&self.cover.as_ref().is_some_and(|c|c.terminal&&c.fee_evidence_complete
        &&c.filled_qty_atoms()==e.futures_filled_atoms)
        &&!e.unknown_or_quarantine;
   Ok(PaperCloseExposure{
      original_net_base_atoms:e.spot_net_delivered_atoms,
      opened_short_atoms:e.futures_filled_atoms,
      cover_filled_atoms:cover,spot_sold_atoms:sell,
      remaining_short_atoms:e.futures_filled_atoms-cover,
      remaining_spot_base_atoms:e.spot_net_delivered_atoms-sell,
      mock_both_closing_legs_complete:complete,
      independently_verified_closed:false,real_execution_authorized:false,
   })
 }
 pub fn mark_unknown(&mut self,intent:&str,client:&str,why:&str)
      ->Result<(),PaperGateError>{
  self.check()?;
  if self.unknown.is_some(){return Err(PaperGateError::Unresolved)}
  let matches=std::iter::once(&self.spot).chain(self.shorts.iter())
   .chain(self.cover.iter()).chain(self.sell.iter())
   .any(|leg|leg.intent_id==intent&&leg.client_order_id==client&&!leg.terminal);
  if !matches||why.trim().is_empty(){return Err(PaperGateError::BadIdentity)}
  self.unknown=Some(PaperUnknown{intent_id:intent.into(),
    client_order_id:client.into(),explanation:why.into()});
  self.check()
 }
 pub fn exposure(&self)->Result<PaperExposure,PaperGateError>{
  self.check()?;
  let spot=self.spot.filled_qty_atoms();let hedge=self.sum_short()?;
  let (base_fee,fee_complete)=self.fee_effect()?;
  let net=spot.checked_sub(base_fee).ok_or(PaperGateError::BadSnapshot)?;
  let blocked=self.restart_quarantine||self.unknown.is_some();
  Ok(PaperExposure{
    spot_filled_atoms:spot,spot_net_delivered_atoms:net,
    spot_base_fee_atoms:base_fee,
    futures_filled_atoms:hedge,
    unhedged_base_atoms:net-hedge,
    spot_terminal:self.spot.terminal,
    spot_fee_complete:fee_complete,
    all_futures_terminal:self.shorts.iter().all(|s|s.terminal),
    unknown_or_quarantine:blocked,
    safe_to_propose_next_paper_hedge:!blocked&&self.spot.terminal
      &&fee_complete&&
      self.shorts.iter().all(|s|s.terminal)
      &&net>hedge,
    may_send_real_or_cancel:false,
  })
 }
 pub fn restart_from_checkpoint(bytes:&[u8])->Result<Self,PaperGateError>{
  let mut saved:Self=serde_json::from_slice(bytes).map_err(|_|PaperGateError::BadSnapshot)?;
  saved.check()?;
  saved.restart_quarantine=true;
  saved.check()?;
  Ok(saved)
 }
 pub fn paper_checkpoint(&self)->Result<Vec<u8>,PaperGateError>{
  self.check()?;
  serde_json::to_vec(self).map_err(|_|PaperGateError::BadSnapshot)
 }
 // No method can release a recovery quarantine based on public market data.
 // Actual order confirmations and fee-bill ancestry are Astra P0-02/P0-03.
 pub fn qty_as_base(&self)->Result<String,PaperGateError>{
  Ok(crate::fill_economics::atoms_to_string(self.exposure()?.unhedged_base_atoms))
 }
}
#[cfg(test)]
mod tests{
 use super::*;
 use crate::fill_economics::decimal_to_atoms as atoms;
 fn new()->PaperPartialGate{
  PaperPartialGate::new(7,"owner1","ONEUSDT","spot1","spot_client1",atoms("10").unwrap()).unwrap()
 }
 fn f(id:&str,qty:&str)->FillEvent{
  FillEvent{
   fill_id:id.into(),price_atoms:atoms("100").unwrap(),
   qty_atoms:atoms(qty).unwrap(),fee_cost_quote_atoms:Some(0),
   fee_currency:Some("USDT".into()),trade_time_ms:Some(1234),
  }
 }
 fn spot_report(qty:&str,term:bool,rows:Vec<FillEvent>)->PaperOrderReport{
  PaperOrderReport{
   intent_id:"spot1".into(),client_order_id:"spot_client1".into(),
   exchange_order_id:"12345".into(),
   cumulative_qty_atoms:atoms(qty).unwrap(),
   average_price_atoms:if qty=="0"{None}else{Some(atoms("100").unwrap())},
   terminal:term,
   status:if term{
      if qty=="10"{"filled".into()}else{"canceled".into()}
   }else{"partially_filled".into()},
   observed_at_ms:1234,fills:rows,
  }
 }
 fn short_report(id:&str,client:&str,qty:&str,requested:&str,rows:Vec<FillEvent>)
    ->PaperOrderReport{
  PaperOrderReport{intent_id:id.into(),client_order_id:client.into(),
    exchange_order_id:format!("num{id}"),
    cumulative_qty_atoms:atoms(qty).unwrap(),
    average_price_atoms:Some(atoms("100").unwrap()),
    terminal:true,status:if qty==requested{"filled".into()}else{"canceled".into()},
    observed_at_ms:1235,fills:rows}
 }
 fn spot_partial_terminal()->PaperPartialGate{
  let mut p=new();
  p.observe_spot(&spot_report("4",true,vec![f("s1","4")])).unwrap();p
 }
 #[test]fn spot_partial_nonterminal_blocks_hedge(){
  let mut p=new();
  p.observe_spot(&spot_report("4",false,vec![f("s1","4")])).unwrap();
  assert!(matches!(p.begin_paper_short("f1","fc1",atoms("4").unwrap()),Err(PaperGateError::WrongStage)));
  let s=p.exposure().unwrap();
  assert_eq!(s.unhedged_base_atoms,atoms("4").unwrap());
  assert!(!s.safe_to_propose_next_paper_hedge);
 }
 #[test]fn terminal_partial_spot_allows_only_actual_amount(){
  let mut p=spot_partial_terminal();
  assert!(p.exposure().unwrap().safe_to_propose_next_paper_hedge);
  assert_eq!(p.begin_paper_short("f1","fc1",atoms("10").unwrap()),
   Err(PaperGateError::Overhedge));
  p.begin_paper_short("f1","fc1",atoms("4").unwrap()).unwrap();
  assert_eq!(p.shorts[0].requested_qty_atoms,atoms("4").unwrap());
 }
 #[test]fn futures_partial_then_followup_for_only_remaining(){
  let mut p=spot_partial_terminal();
  p.begin_paper_short("f1","fc1",atoms("4").unwrap()).unwrap();
  p.observe_short(&short_report("f1","fc1","3","4",vec![f("h1","3")])).unwrap();
  assert_eq!(p.exposure().unwrap().unhedged_base_atoms,atoms("1").unwrap());
  assert_eq!(p.begin_paper_short("f2","fc2",atoms("2").unwrap()),
    Err(PaperGateError::Overhedge));
  p.begin_paper_short("f2","fc2",atoms("1").unwrap()).unwrap();
  p.observe_short(&short_report("f2","fc2","1","1",vec![f("h2","1")])).unwrap();
  let e=p.exposure().unwrap();
  assert_eq!(e.unhedged_base_atoms,0);
  assert!(!e.safe_to_propose_next_paper_hedge);
  assert!(!e.may_send_real_or_cancel);
 }
 #[test]fn incremental_cumulative_and_fills_are_atomic(){
  let mut p=new();
  p.observe_spot(&spot_report("4",false,vec![f("s1","4")])).unwrap();
  p.observe_spot(&spot_report("10",true,vec![f("s2","6")])).unwrap();
  assert_eq!(p.spot.filled_qty_atoms(),atoms("10").unwrap());
  assert_eq!(p.spot.fills.len(),2);
  assert!(p.spot.fee_evidence_complete);
 }
 #[test]fn duplicate_identical_fill_is_noop(){
  let mut p=spot_partial_terminal();
  let before=p.spot.fills.len();
  p.observe_spot(&spot_report("4",true,vec![f("s1","4")])).unwrap();
  assert_eq!(p.spot.fills.len(),before);
  assert_eq!(p.spot.filled_qty_atoms(),atoms("4").unwrap());
 }
 #[test]fn conflicting_replayed_fill_rejected_without_mutation(){
  let mut p=spot_partial_terminal();
  let before=p.clone();
  let mut bad=f("s1","4");bad.price_atoms=atoms("101").unwrap();
  assert_eq!(p.observe_spot(&spot_report("4",true,vec![bad])),
    Err(PaperGateError::Source(EconError::DuplicateFillConflict)));
  assert_eq!(p,before);
 }
 #[test]fn reported_terminal_without_fee_fill_details_blocks_hedge(){
  let mut p=new();
  p.observe_spot(&spot_report("4",true,vec![])).unwrap();
  assert!(!p.spot.fee_evidence_complete);
  assert_eq!(p.begin_paper_short("f1","fc1",atoms("4").unwrap()),
     Err(PaperGateError::FeeIncomplete));
 }
 #[test]fn fee_paid_in_base_is_not_silently_called_quote_fee(){
  let mut p=new();
  let mut bad=f("s1","4");bad.fee_currency=Some("ONE".into());
  assert_eq!(p.observe_spot(&spot_report("4",true,vec![bad])),
      Err(PaperGateError::BadIdentity));
 }
 #[test]fn unknown_spot_keeps_both_legs_blocked(){
  let mut p=new();
  p.mark_unknown("spot1","spot_client1","no ack").unwrap();
  assert_eq!(p.observe_spot(&spot_report("4",true,vec![f("s1","4")])),
      Err(PaperGateError::Unresolved));
  assert_eq!(p.begin_paper_short("f1","fc1",atoms("4").unwrap()),
      Err(PaperGateError::Unresolved));
  assert!(p.exposure().unwrap().unknown_or_quarantine);
 }
 #[test]fn unknown_futures_blocks_hedge_retry(){
  let mut p=spot_partial_terminal();
  p.begin_paper_short("f1","fc1",atoms("4").unwrap()).unwrap();
  p.mark_unknown("f1","fc1","socket timeout").unwrap();
  assert_eq!(p.begin_paper_short("f2","fc2",atoms("4").unwrap()),
      Err(PaperGateError::Unresolved));
  assert_eq!(p.observe_short(&short_report("f1","fc1","4","4",vec![f("h1","4")])),
      Err(PaperGateError::Unresolved));
 }
 #[test]fn nonterminal_short_cannot_duplicate_hedge_intent(){
  let mut p=spot_partial_terminal();
  p.begin_paper_short("f1","fc1",atoms("4").unwrap()).unwrap();
  assert_eq!(p.begin_paper_short("f2","fc2",atoms("4").unwrap()),
      Err(PaperGateError::WrongStage));
 }
 #[test]fn restart_serialized_hedged_does_not_grant_order_permission(){
  let mut p=spot_partial_terminal();
  p.begin_paper_short("f1","fc1",atoms("4").unwrap()).unwrap();
  p.observe_short(&short_report("f1","fc1","4","4",vec![f("h1","4")])).unwrap();
  let bytes=p.paper_checkpoint().unwrap();
  let reopened=PaperPartialGate::restart_from_checkpoint(&bytes).unwrap();
  let e=reopened.exposure().unwrap();
  assert_eq!(e.unhedged_base_atoms,0);
  assert!(e.unknown_or_quarantine);
  assert!(!e.safe_to_propose_next_paper_hedge);
  assert!(!e.may_send_real_or_cancel);
 }
 #[test]fn restart_partial_spot_never_assumes_no_exposure(){
  let mut p=new();
  p.observe_spot(&spot_report("4",false,vec![f("s1","4")])).unwrap();
  let bytes=p.paper_checkpoint().unwrap();
  let mut reopened=PaperPartialGate::restart_from_checkpoint(&bytes).unwrap();
  assert_eq!(reopened.exposure().unwrap().unhedged_base_atoms,atoms("4").unwrap());
  assert_eq!(reopened.observe_spot(&spot_report("10",true,vec![f("s2","6")])),
      Err(PaperGateError::Unresolved));
 }
 #[test]fn tampering_snapshot_to_authorized_is_rejected(){
  let mut p=spot_partial_terminal();
  p.may_send_real_or_cancel=true;
  let raw=serde_json::to_vec(&p).unwrap();
  assert!(matches!(PaperPartialGate::restart_from_checkpoint(&raw),
      Err(PaperGateError::BadSnapshot)));
 }
 #[test]fn terminal_order_cannot_gain_more_fills_later(){
  let mut p=spot_partial_terminal();
  assert_eq!(p.observe_spot(&spot_report("10",true,vec![f("s2","6")])),
      Err(PaperGateError::WrongStage));
 }
 #[test]fn unexpected_client_or_order_id_fails_closed(){
  let mut p=new();
  let mut r=spot_report("4",true,vec![f("s1","4")]);
  r.client_order_id="other".into();
  assert_eq!(p.observe_spot(&r),Err(PaperGateError::BadIdentity));
  assert_eq!(p.spot.filled_qty_atoms(),0);
 }
 #[test]fn no_signals_can_authorize_real_exchange_order(){
  let p=spot_partial_terminal();
  assert!(!p.exchange_truth_authenticated);
  assert!(!p.exposure().unwrap().may_send_real_or_cancel);
 }
}
#[cfg(test)]
mod base_fee_durable_tests{
 use super::*;
 use crate::fill_economics::decimal_to_atoms as a;
 fn n(s:&str)->u128{a(s).unwrap()}
 fn src(id:&str,qty:&str,fee:&str)->SpotFeeFill{
  SpotFeeFill{
   order_id:"12345".into(),trade_id:id.into(),at_ms:1234,
   side:"buy".into(),inst_id:"ONE-USDT".into(),
   price_quote_e12:n("1"),gross_base_qty_e12:n(qty),
   fee_ccy:"ONE".into(),signed_fee_e12:-(n(fee) as i128),
  }
 }
 fn base_fill(id:&str,qty:&str)->FillEvent{
   FillEvent{fill_id:id.into(),price_atoms:n("1"),qty_atoms:n(qty),
       fee_cost_quote_atoms:None,fee_currency:Some("ONE".into()),
       trade_time_ms:Some(1234)}
 }
 fn fut_fill(id:&str,qty:&str)->FillEvent{
   FillEvent{fill_id:id.into(),price_atoms:n("1"),qty_atoms:n(qty),
       fee_cost_quote_atoms:Some(0),fee_currency:Some("USDT".into()),
       trade_time_ms:Some(1235)}
 }
 fn spot(rows:Vec<FillEvent>,amount:&str)->PaperOrderReport{
   PaperOrderReport{intent_id:"spot1".into(),
      client_order_id:"spot_client1".into(),exchange_order_id:"12345".into(),
      cumulative_qty_atoms:n(amount),average_price_atoms:Some(n("1")),
      terminal:true,status:"filled".into(),observed_at_ms:1234,fills:rows}
 }
 fn future(intent:&str,client:&str,qty:&str,terminal:bool,status:&str,
   fills:Vec<FillEvent>)->PaperOrderReport{
   PaperOrderReport{intent_id:intent.into(),client_order_id:client.into(),
      exchange_order_id:format!("order_{intent}"),
      cumulative_qty_atoms:n(qty),average_price_atoms:Some(n("1")),
      terminal,status:status.into(),observed_at_ms:1235,fills}
 }
 fn pending()->PaperPartialGate{
    let mut g=PaperPartialGate::new(3,"owner_ONE","ONEUSDT",
       "spot1","spot_client1",n("10")).unwrap();
    g.observe_spot(&spot(vec![base_fill("buy1","10")],"10")).unwrap();
    g
 }
 #[test]fn base_fee_must_not_be_silently_spendable(){
   let mut g=pending();let e=g.exposure().unwrap();
   assert_eq!(e.spot_filled_atoms,n("10"));
   assert!(!e.spot_fee_complete);
   assert_eq!(g.begin_paper_short("f1","fc1",n("9")),
     Err(PaperGateError::FeeIncomplete));
 }
 #[test]fn native_fee_proof_reduces_net_inventory(){
   let mut g=pending();
   g.record_native_spot_fee(vec![src("buy1","10","1")]).unwrap();
   let e=g.exposure().unwrap();
   assert_eq!(e.spot_base_fee_atoms,n("1"));
   assert_eq!(e.spot_net_delivered_atoms,n("9"));
   assert_eq!(e.unhedged_base_atoms,n("9"));
   assert!(e.spot_fee_complete);
   assert_eq!(g.begin_paper_short("bad","badc",n("10")),
     Err(PaperGateError::Overhedge));
   g.begin_paper_short("f1","fc1",n("9")).unwrap();
   assert_eq!(g.shorts[0].requested_qty_atoms,n("9"));
 }
 #[test]fn native_fee_duplicate_noop_conflict_denied(){
   let mut g=pending();let row=src("buy1","10","1");
   g.record_native_spot_fee(vec![row.clone()]).unwrap();
   let saved=g.clone();
   g.record_native_spot_fee(vec![row]).unwrap();
   assert_eq!(g,saved);
   assert_eq!(g.record_native_spot_fee(vec![src("buy1","10","2")]),
       Err(PaperGateError::WrongStage));
   assert_eq!(g,saved);
 }
 #[test]fn forged_trade_id_or_fee_qty_fails_atomically(){
   let mut g=pending();let old=g.clone();
   assert_eq!(g.record_native_spot_fee(vec![src("wrong","10","1")]),
       Err(PaperGateError::BadSnapshot));
   assert_eq!(g.record_native_spot_fee(vec![src("buy1","9","1")]),
       Err(PaperGateError::BadSnapshot));
   assert_eq!(g,old);
 }
 #[test]fn fee_overdraw_denied(){
   let mut g=pending();
   assert_eq!(g.record_native_spot_fee(vec![src("buy1","10","10")]),
      Err(PaperGateError::BadSnapshot));
   assert!(g.native_spot_fee.is_none());
 }
 #[test]fn partial_futures_stays_exposed_then_followup_only_remainder(){
   let mut g=pending();
   g.record_native_spot_fee(vec![src("buy1","10","1")]).unwrap();
   g.begin_paper_short("f1","fc1",n("9")).unwrap();
   g.observe_short(&future("f1","fc1","5",false,"partially_filled",
      vec![fut_fill("short1","5")])).unwrap();
   assert_eq!(g.exposure().unwrap().unhedged_base_atoms,n("4"));
   assert_eq!(g.begin_paper_short("f2","fc2",n("4")),
      Err(PaperGateError::WrongStage));
   g.observe_short(&future("f1","fc1","8",true,"canceled",
      vec![fut_fill("short2","3")])).unwrap();
   assert_eq!(g.exposure().unwrap().unhedged_base_atoms,n("1"));
   assert_eq!(g.begin_paper_short("f2","fc2",n("2")),
      Err(PaperGateError::Overhedge));
   g.begin_paper_short("f2","fc2",n("1")).unwrap();
   g.observe_short(&future("f2","fc2","1",true,"filled",
      vec![fut_fill("short3","1")])).unwrap();
   assert_eq!(g.exposure().unwrap().unhedged_base_atoms,0);
   assert_eq!(g.exposure().unwrap().futures_filled_atoms,n("9"));
 }
 #[test]fn restart_preserves_native_fee_and_quarantines(){
   let mut g=pending();
   g.record_native_spot_fee(vec![src("buy1","10","1")]).unwrap();
   let raw=g.paper_checkpoint().unwrap();
   let restored=PaperPartialGate::restart_from_checkpoint(&raw).unwrap();
   assert_eq!(restored.exposure().unwrap().spot_net_delivered_atoms,n("9"));
   assert_eq!(restored.exposure().unwrap().unhedged_base_atoms,n("9"));
   assert!(restored.exposure().unwrap().unknown_or_quarantine);
   assert!(!restored.exposure().unwrap().safe_to_propose_next_paper_hedge);
 }
 #[test]fn altered_native_fee_in_snapshot_is_denied(){
   let mut g=pending();g.record_native_spot_fee(vec![src("buy1","10","1")]).unwrap();
   g.native_spot_fee.as_mut().unwrap().total_debited_base_atoms=n("2");
   assert!(matches!(g.paper_checkpoint(),Err(PaperGateError::BadSnapshot)));
   assert!(PaperPartialGate::restart_from_checkpoint(&serde_json::to_vec(&g).unwrap()).is_err());
 }
 #[test]fn legacy_no_native_fee_keeps_old_json_shape(){
   let g=PaperPartialGate::new(1,"owner","ONEUSDT","spot1","client1",n("10")).unwrap();
   let bytes=g.paper_checkpoint().unwrap();
   assert!(!String::from_utf8(bytes.clone()).unwrap().contains("native_spot_fee"));
   assert_eq!(PaperPartialGate::restart_from_checkpoint(&bytes)
      .unwrap().native_spot_fee,None);
 }
 #[test]fn unknown_future_disallows_followup_even_with_native_fee(){
   let mut g=pending();g.record_native_spot_fee(vec![src("buy1","10","1")]).unwrap();
   g.begin_paper_short("f1","fc1",n("9")).unwrap();
   g.observe_short(&future("f1","fc1","5",false,"partially_filled",
      vec![fut_fill("short1","5")])).unwrap();
   g.mark_unknown("f1","fc1","timeout").unwrap();
   assert_eq!(g.begin_paper_short("f2","fc2",n("4")),
      Err(PaperGateError::Unresolved));
   assert!(g.exposure().unwrap().unknown_or_quarantine);
 }
 #[test]fn mixed_base_quote_fee_needs_only_native_rows_in_native_proof(){
   let mut g=PaperPartialGate::new(3,"owner_ONE","ONEUSDT",
       "spot1","spot_client1",n("10")).unwrap();
   let mut quote=fut_fill("buy2","6");
   quote.trade_time_ms=Some(1234);
   quote.fee_cost_quote_atoms=Some(n("0.01") as i128);
   g.observe_spot(&spot(vec![base_fill("buy1","4"),quote],"10")).unwrap();
   let proof=src("buy1","4","0.5");
   g.record_native_spot_fee(vec![proof]).unwrap();
   assert_eq!(g.exposure().unwrap().spot_net_delivered_atoms,n("9.5"));
   assert_eq!(g.exposure().unwrap().spot_base_fee_atoms,n("0.5"));
   assert!(g.exposure().unwrap().spot_fee_complete);
 }
}
