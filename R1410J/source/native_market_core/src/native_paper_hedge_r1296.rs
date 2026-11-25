//! Typed, synthetic-only isolated PAPER policy. No transport or mutable portfolio.
use crate::{Book, native_paper_risk_lifecycle_r1287 as risk};
use serde::{Serialize, Deserialize};
use std::collections::BTreeMap;
use crate::fill_economics::{decimal_to_atoms, atoms_to_string, SCALE_12};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct InstrumentProfile {
    pub synthetic: bool,
    pub base: String,
    pub spot_symbol: String,
    pub futures_symbol: String,
    pub product: String,
    pub quantity_units: String,
    pub multiplier: String,
    pub hedge_ratio: String,
    pub step: String,
    pub minimum_quantity: String,
    pub maximum_quantity: String,
    pub minimum_notional: String,
    pub fee_model: String,
    pub fee_bps: String,
    pub maximum_unhedged_base: String,
    pub maximum_dust_base: String,
    pub metadata_loaded_ns: u64,
    pub metadata_ttl_ns: u64,
    pub hedge_timeout_ns: u64,
    pub stop_loss_bps: f64,
    pub max_hold_ns: u64,
}
fn atoms(s:&str)->Result<u128,String> {
    decimal_to_atoms(s).map_err(|_|"invalid exact decimal (maximum 12 fractional digits)".into())
}
pub fn quantity_atoms(q:f64)->Result<u128,String> {
    if !q.is_finite() || q<0.0 {return Err("invalid quantity".into())}
    let scaled=q*SCALE_12 as f64;
    if scaled>u128::MAX as f64 {return Err("quantity overflow".into())}
    let rounded=scaled.round();
    if (scaled-rounded).abs()>0.01 {return Err("quantity exceeds exact decimal precision".into())}
    Ok(rounded as u128)
}
pub fn as_quantity(a:u128)->Result<f64,String>{
    let q=atoms_to_string(a).parse::<f64>().map_err(|_|"quantity conversion")?;
    if quantity_atoms(q)?!=a {return Err("quantity cannot round trip".into())}
    Ok(q)
}
pub fn digest<T:Serialize>(v:&T)->Result<String,String>{
    let b=serde_json::to_vec(v).map_err(|e|e.to_string())?;
    Ok(openssl::sha::sha256(&b).iter().map(|x|format!("{x:02x}")).collect())
}
impl InstrumentProfile {
    pub fn validate(&self)->Result<(),String>{
        if !self.synthetic || self.product!="LINEAR_USDT_PERPETUAL" ||
           self.quantity_units!="BASE" || atoms(&self.multiplier)?!=SCALE_12 ||
           atoms(&self.hedge_ratio)?!=SCALE_12 {
            return Err("unsupported instrument/units: isolated synthetic base-unit linear profile required".into())
        }
        if self.base.is_empty() || self.base.len()>30 ||
           !self.base.bytes().all(|b|b.is_ascii_uppercase()||b.is_ascii_digit()) ||
           self.spot_symbol!=format!("{}-USDT",self.base) ||
           self.futures_symbol!=format!("{}USDT",self.base) {return Err("instrument identity".into())}
        let step=atoms(&self.step)?; let min=atoms(&self.minimum_quantity)?;
        let max=atoms(&self.maximum_quantity)?; let cap=atoms(&self.maximum_unhedged_base)?;
        let dust=atoms(&self.maximum_dust_base)?;
        if step==0 || min==0 || max<min || cap==0 || dust>cap ||
           self.fee_model!="QUOTE_SIMULATED" || atoms(&self.fee_bps)? >= 10_000*SCALE_12 ||
           self.metadata_loaded_ns==0 || self.metadata_ttl_ns==0 ||
           self.hedge_timeout_ns==0 || self.max_hold_ns==0 ||
           !self.stop_loss_bps.is_finite() || self.stop_loss_bps>=0.0 {
            return Err("missing/invalid economic policy".into())
        }
        atoms(&self.minimum_notional)?;
        Ok(())
    }
    pub fn risk_config(&self)->Result<risk::RiskConfig,String>{
        self.validate()?;
        Ok(risk::RiskConfig{hedge_timeout_ns:self.hedge_timeout_ns,
            stop_loss_bps:self.stop_loss_bps,max_hold_ns:self.max_hold_ns,
            futures_contract_multiplier:1.0,
            fee_bps:self.fee_bps.parse().map_err(|_|"fee decimal")?})
    }
    pub fn validate_quantity(&self,q:f64,price:f64,entry:bool)->Result<(),String>{
        self.validate()?;
        let a=quantity_atoms(q)?;
        if a==0 || a%atoms(&self.step)?!=0 || a<atoms(&self.minimum_quantity)? ||
           a>atoms(&self.maximum_quantity)? ||
           (entry && a>atoms(&self.maximum_unhedged_base)?) {return Err("quantity/step/cap denied".into())}
        if !price.is_finite()||price<=0.0 {return Err("invalid price".into())}
        let p=quantity_atoms(price)?;
        let notional=a.checked_mul(p).ok_or("notional overflow")?/SCALE_12;
        if notional<atoms(&self.minimum_notional)? {return Err("minimum notional".into())}
        Ok(())
    }
}
#[derive(Clone,Debug,Serialize,Deserialize,PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Identity {
    pub run_id:String,
    pub clock_domain:String,
    pub profile:InstrumentProfile,
    #[serde(default,skip_serializing_if="Option::is_none")]
    pub exit:Option<crate::native_paper_exit_r1298::ExitRules>,
}
impl Identity {
    pub fn validate(&self)->Result<(),String>{
        self.profile.validate()?;
        if let Some(r)=&self.exit{r.validate()?;}
        if self.run_id.is_empty()||self.run_id.len()>96 ||
           !self.run_id.bytes().all(|b|b.is_ascii_alphanumeric()||b"-_".contains(&b)) ||
           self.clock_domain.is_empty()||self.clock_domain.len()>96 {
            return Err("invalid run/clock identity".into())
        }
        Ok(())
    }
    pub fn hash(&self)->Result<String,String>{self.validate()?;digest(self)}
}
#[derive(Clone,Debug,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketContext {
    pub input_id:String,
    pub clock_domain:String,
    pub now_ns:u64,
    pub wall_ms:i64,
    pub spot_hot:bool,
    pub futures_hot:bool,
}
pub fn validate_pair(id:&Identity,spot:&Book,futures:&Book,c:&MarketContext)->Result<(),String>{
    id.validate()?;
    if c.input_id.is_empty()||c.clock_domain!=id.clock_domain {return Err("clock/input identity".into())}
    if !c.spot_hot||!c.futures_hot {return Err("HOT loss".into())}
    if c.now_ns<id.profile.metadata_loaded_ns ||
       c.now_ns-id.profile.metadata_loaded_ns>id.profile.metadata_ttl_ns {return Err("stale metadata".into())}
    for b in [spot,futures] {
        if c.now_ns<b.received_ns || c.now_ns-b.received_ns>800_000_000 ||
           b.exchange_ts_ms.map_or(true,|t| c.wall_ms.checked_sub(t).map_or(true,|a| !(-1000..=800).contains(&a))) ||
           b.bids.is_empty() || b.asks.is_empty() {return Err("stale/absent book".into())}
        if b.bids[0].0>=b.asks[0].0 {return Err("crossed book".into())}
        for (levels,bids) in [(&b.bids,true),(&b.asks,false)] {
            if levels.iter().any(|(p,q)|!p.is_finite()||*p<=0.0||!q.is_finite()||*q<=0.0) ||
               levels.windows(2).any(|w|if bids{w[0].0<=w[1].0}else{w[0].0>=w[1].0}) {
                return Err("invalid book shape".into())
            }
        }
    }
    Ok(())
}
#[derive(Clone,Debug,Serialize)]
pub struct ExposureView {
    pub revision:u64, pub state:risk::LifecycleState,
    pub spot_base:f64, pub futures_base:f64, pub residual_base:f64,
    pub known:bool, pub pending_futures:Vec<String>, pub latest_spot_fill_ns:u64,
}
pub fn exposure(l:&risk::Lifecycle,revision:u64)->ExposureView{
    let (s,f)=l.ledger.as_ref().map(|x|(x.spot_exposure_base(),x.futures_short_exposure_base(l.config.futures_contract_multiplier))).unwrap_or((0.,0.));
    ExposureView{revision,state:l.state,spot_base:s,futures_base:f,residual_base:s-f,
      known:l.state!=risk::LifecycleState::RecoveryRequired && l.ledger.as_ref().map_or(true,|x|!x.unknown_execution_status) && !l.orders.values().any(|o|o.uncertain),
      pending_futures:l.orders.values().filter(|o|o.leg==risk::FillLeg::Futures &&
        !matches!(o.status,risk::OrderStatus::Filled|risk::OrderStatus::Cancelled|risk::OrderStatus::Rejected)).map(|o|o.order_id.clone()).collect(),
      latest_spot_fill_ns:l.processed_events.values().filter(|e|e.leg==risk::FillLeg::Spot && e.side==risk::FillSide::Buy).map(|e|e.event_ns).max().unwrap_or(0)}
}
#[derive(Clone,Debug,Serialize,PartialEq)]
pub enum HedgeDecision { NoAction, WaitExisting, NeedNewerFutures, Short{quantity:f64,key:String}, Block(String) }
pub fn plan_hedge(id:&Identity,l:&risk::Lifecycle,revision:u64,spot:&Book,fut:&Book,c:&MarketContext)->HedgeDecision{
    let v=exposure(l,revision);
    if !v.known {return HedgeDecision::Block("unknown exposure/recovery".into())}
    if !v.pending_futures.is_empty(){return HedgeDecision::WaitExisting}
    if !matches!(v.state,risk::LifecycleState::AwaitingHedge|risk::LifecycleState::PartiallyFilled){return HedgeDecision::NoAction}
    if let Err(e)=validate_pair(id,spot,fut,c){return HedgeDecision::Block(e)}
    if v.residual_base< -1e-10{return HedgeDecision::Block("overhedged; cover outside scope".into())}
    if v.residual_base<=1e-10{return HedgeDecision::NoAction}
    if fut.received_ns<=v.latest_spot_fill_ns{return HedgeDecision::NeedNewerFutures}
    if l.ledger.as_ref().is_some_and(|x|c.now_ns>=x.hedge_deadline_ns){return HedgeDecision::Block("hedge timeout".into())}
    let size=(||->Result<f64,String>{
        let r=quantity_atoms(v.residual_base)?; let step=atoms(&id.profile.step)?;
        let q=(r/step*step).min(atoms(&id.profile.maximum_quantity)?/step*step);
        if r-q>atoms(&id.profile.maximum_dust_base)? {return Err("residual exceeds explicit dust policy".into())}
        let qty=as_quantity(q)?;
        // Conservative notional: last visible bid rather than optimistic top quote.
        id.profile.validate_quantity(qty,fut.bids.last().ok_or("no bids")?.0,false)?;
        Ok(qty)
    })();
    match size {Err(e)=>HedgeDecision::Block(e),Ok(quantity)=>{
        let frontier:BTreeMap<_,_>=l.processed_events.iter().map(|(k,v)|(k,v)).collect();
        match digest(&(id,frontier)){Ok(key)=>HedgeDecision::Short{quantity,key},Err(e)=>HedgeDecision::Block(e)}
    }}
}
