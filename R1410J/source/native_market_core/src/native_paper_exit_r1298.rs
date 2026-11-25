//! Explicit synthetic exit policy; no portfolio mutation or transport.
use crate::{Book,native_paper_hedge_r1296 as hedge,native_paper_risk_lifecycle_r1287 as risk};
use serde::{Serialize,Deserialize};
#[derive(Clone,Debug,Serialize,Deserialize,PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ExitRules {
 pub sequence:String,
 pub max_unhedged_base:String,
 pub max_exit_duration_ns:u64,
 pub quantity_units:String,
 pub fee_model:String,
 pub allow_next_generation:bool,
}
impl ExitRules {
 pub fn validate(&self)->Result<(),String>{
  if self.sequence!="SYNTHETIC_COVER_THEN_SPOT"||self.quantity_units!="BASE"||self.fee_model!="QUOTE_SIMULATED"||
    self.max_exit_duration_ns==0||crate::fill_economics::decimal_to_atoms(&self.max_unhedged_base).map_err(|_|"exit cap")?==0{
   return Err("unsupported exit rules".into())
  }Ok(())
 }
}
#[derive(Clone,Debug,Serialize,Deserialize)]
pub struct ExitRequest {pub trigger_id:String,pub started_ns:u64,pub deadline_ns:u64}
#[derive(Clone,Debug,Serialize,PartialEq)]
pub enum ExitDecision {Hold,WaitPending,FuturesCover{quantity:f64},SpotExit{quantity:f64},Census,RiskExit(String),FailClosed(String)}
pub fn decide(id:&hedge::Identity,l:&risk::Lifecycle,request:Option<&ExitRequest>,
 spot:&Book,futures:&Book,c:&hedge::MarketContext)->ExitDecision{
 let Some(rules)=id.exit.as_ref() else{return ExitDecision::Hold};
 let Some(request)=request else{return ExitDecision::Hold};
 let check=(||->Result<ExitDecision,String>{
  rules.validate()?;hedge::validate_pair(id,spot,futures,c)?;
  let v=hedge::exposure(l,0);
  if !v.known||l.state==risk::LifecycleState::Closed{return Err("recovery/closed exit blocked".into())}
  if c.now_ns<request.started_ns{return Err("exit clock regression".into())}
  if c.now_ns>request.deadline_ns{return Ok(ExitDecision::RiskExit("exit deadline requires recovery".into()))}
  if l.orders.values().any(|o|o.uncertain){return Err("uncertain order".into())}
  if l.orders.values().any(|o|!matches!(o.status,risk::OrderStatus::Filled|risk::OrderStatus::Cancelled|risk::OrderStatus::Rejected)){
   return Ok(ExitDecision::WaitPending)
  }
  let spot_atoms=hedge::quantity_atoms(v.spot_base)?;let future_atoms=hedge::quantity_atoms(v.futures_base)?;
  let cap=crate::fill_economics::decimal_to_atoms(&rules.max_unhedged_base).map_err(|_|"exit cap")?;
  if spot_atoms>cap{return Err("cover-first unhedged inventory cap".into())}
  let step=crate::fill_economics::decimal_to_atoms(&id.profile.step).map_err(|_|"step")?;
  let rounded=|a:u128|hedge::as_quantity(a/step*step);
  if future_atoms>0{
   let quantity=rounded(future_atoms)?;
   id.profile.validate_quantity(quantity,futures.asks[0].0,false)?;
   return Ok(ExitDecision::FuturesCover{quantity})
  }
  if spot_atoms>0{
   let quantity=rounded(spot_atoms)?;
   id.profile.validate_quantity(quantity,spot.bids.last().ok_or("spot depth")?.0,false)?;
   return Ok(ExitDecision::SpotExit{quantity})
  }
  Ok(ExitDecision::Census)
 })();
 check.unwrap_or_else(ExitDecision::FailClosed)
}
