//! R19 actual fill economics / spread revaluation / hedge decision.
//!
//! Authoritative quantities and prices are fixed-point 12-decimal integers.
//! No binary float is stored in durable fill/economic state.

use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
};

pub const SCALE_12: u128 = 1_000_000_000_000;
pub const BPS_MICRO_SCALE: i128 = 1_000_000;
pub const DEFAULT_AVG_TOLERANCE_ATOMS: u128 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EconError {
    InvalidDecimal,
    Overflow,
    ZeroQuantity,
    Overfill,
    NonMonotonicCumulative,
    DuplicateFillConflict,
    AverageMismatch,
    InsufficientDepth,
    FuturesOverHedged,
    MissingActualPrice,
    MissingTruth,
    Io(io::ErrorKind),
    InvalidState,
}
impl From<io::Error> for EconError {
    fn from(value: io::Error) -> Self { Self::Io(value.kind()) }
}

pub fn decimal_to_atoms(value: &str) -> Result<u128, EconError> {
    let raw=value.trim();
    if raw.is_empty()||raw.starts_with('-')||raw.starts_with('+'){return Err(EconError::InvalidDecimal)}
    let mut p=raw.split('.');
    let whole=p.next().ok_or(EconError::InvalidDecimal)?;
    let frac=p.next().unwrap_or("");
    if p.next().is_some()||whole.is_empty()||!whole.bytes().all(|b|b.is_ascii_digit())
        ||!frac.bytes().all(|b|b.is_ascii_digit())||frac.len()>12{return Err(EconError::InvalidDecimal)}
    let w:u128=whole.parse().map_err(|_|EconError::InvalidDecimal)?;
    let mut f=frac.to_owned(); while f.len()<12{f.push('0');}
    let fv=if f.is_empty(){0}else{f.parse::<u128>().map_err(|_|EconError::InvalidDecimal)?};
    w.checked_mul(SCALE_12).and_then(|x|x.checked_add(fv)).ok_or(EconError::Overflow)
}
pub fn signed_decimal_to_atoms(value:&str)->Result<i128,EconError>{
    let raw=value.trim();
    let (neg,body)=if let Some(v)=raw.strip_prefix('-'){(true,v)}else if let Some(v)=raw.strip_prefix('+'){(false,v)}else{(false,raw)};
    let u=decimal_to_atoms(body)?;
    if u>i128::MAX as u128{return Err(EconError::Overflow)}
    Ok(if neg{-(u as i128)}else{u as i128})
}
pub fn atoms_to_string(v:u128)->String{
    let w=v/SCALE_12; let f=v%SCALE_12;
    if f==0{return w.to_string()}
    let mut s=format!("{f:012}"); while s.ends_with('0'){s.pop();}
    format!("{w}.{s}")
}
pub fn signed_atoms_to_string(v:i128)->String{
    if v<0{format!("-{}",atoms_to_string(v.unsigned_abs()))}else{atoms_to_string(v as u128)}
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FillEvent {
    pub fill_id:String,
    pub price_atoms:u128,
    pub qty_atoms:u128,
    /// Positive cost in quote-currency atoms. None means fee economics incomplete.
    pub fee_cost_quote_atoms:Option<i128>,
    pub fee_currency:Option<String>,
    pub trade_time_ms:Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApplyFillResult { Added, DuplicateNoOp }

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegFillLedger {
    pub venue:String,
    pub symbol:String,
    pub side:String,
    pub intent_id:String,
    pub client_order_id:String,
    pub exchange_order_id:Option<String>,
    pub requested_qty_atoms:u128,
    pub cumulative_qty_atoms:u128,
    pub cumulative_notional_atoms2:u128,
    pub cumulative_truth_present:bool,
    pub terminal:bool,
    pub status:Option<String>,
    pub first_fill_time_ms:Option<u64>,
    pub last_fill_time_ms:Option<u64>,
    pub fee_cost_quote_atoms:i128,
    pub fee_evidence_complete:bool,
    pub fills:BTreeMap<String,FillEvent>,
    fill_qty_atoms:u128,
    fill_notional_atoms2:u128,
}
impl LegFillLedger {
    pub fn new(venue:&str,symbol:&str,side:&str,intent_id:&str,client_order_id:&str,requested_qty_atoms:u128)->Result<Self,EconError>{
        if requested_qty_atoms==0{return Err(EconError::ZeroQuantity)}
        Ok(Self{venue:venue.into(),symbol:symbol.into(),side:side.into(),intent_id:intent_id.into(),
            client_order_id:client_order_id.into(),exchange_order_id:None,requested_qty_atoms,
            cumulative_qty_atoms:0,cumulative_notional_atoms2:0,cumulative_truth_present:false,terminal:false,status:None,
            first_fill_time_ms:None,last_fill_time_ms:None,fee_cost_quote_atoms:0,
            fee_evidence_complete:false,fills:BTreeMap::new(),fill_qty_atoms:0,fill_notional_atoms2:0})
    }
    fn refresh_fee_completeness(&mut self){
        let effective=self.filled_qty_atoms();
        self.fee_evidence_complete=self.terminal && effective>0 && self.fill_qty_atoms==effective
            && !self.fills.is_empty() && self.fills.values().all(|f|f.fee_cost_quote_atoms.is_some());
    }
    pub fn apply_cumulative_truth(&mut self,qty_atoms:u128,avg_price_atoms:Option<u128>,terminal:bool,
        status:Option<&str>,exchange_order_id:Option<&str>,observed_fill_time_ms:Option<u64>,
        avg_tolerance_atoms:u128)->Result<(),EconError>{
        if qty_atoms>self.requested_qty_atoms{return Err(EconError::Overfill)}
        if self.cumulative_truth_present && qty_atoms<self.cumulative_qty_atoms{return Err(EconError::NonMonotonicCumulative)}
        let notional=if qty_atoms==0{0}else{
            let px=avg_price_atoms.filter(|x|*x>0).ok_or(EconError::MissingActualPrice)?;
            px.checked_mul(qty_atoms).ok_or(EconError::Overflow)?
        };
        if self.cumulative_truth_present && qty_atoms==self.cumulative_qty_atoms && self.cumulative_qty_atoms>0 {
            let prior=self.cumulative_notional_atoms2/self.cumulative_qty_atoms;
            let next=avg_price_atoms.ok_or(EconError::MissingActualPrice)?;
            if prior.abs_diff(next)>avg_tolerance_atoms{return Err(EconError::AverageMismatch)}
        }
        if self.fill_qty_atoms>qty_atoms{return Err(EconError::AverageMismatch)}
        if self.fill_qty_atoms==qty_atoms && qty_atoms>0 {
            let event_avg=self.fill_notional_atoms2/qty_atoms;
            let reported=avg_price_atoms.ok_or(EconError::MissingActualPrice)?;
            if event_avg.abs_diff(reported)>avg_tolerance_atoms{return Err(EconError::AverageMismatch)}
        }
        let increased=qty_atoms>self.cumulative_qty_atoms;
        self.cumulative_qty_atoms=qty_atoms;
        self.cumulative_notional_atoms2=notional;
        self.cumulative_truth_present=true;
        self.terminal=terminal;
        self.status=status.map(str::to_owned);
        if let Some(id)=exchange_order_id{
            if let Some(prior)=&self.exchange_order_id{if prior!=id{return Err(EconError::InvalidState)}}
            self.exchange_order_id=Some(id.into());
        }
        if increased {
            if self.first_fill_time_ms.is_none(){self.first_fill_time_ms=observed_fill_time_ms;}
            if observed_fill_time_ms.is_some(){self.last_fill_time_ms=observed_fill_time_ms;}
        }
        self.refresh_fee_completeness();
        Ok(())
    }
    pub fn apply_fill(&mut self,fill:FillEvent,avg_tolerance_atoms:u128)->Result<ApplyFillResult,EconError>{
        if fill.fill_id.is_empty()||fill.price_atoms==0||fill.qty_atoms==0{return Err(EconError::InvalidState)}
        if let Some(old)=self.fills.get(&fill.fill_id){
            if old==&fill{return Ok(ApplyFillResult::DuplicateNoOp)}
            return Err(EconError::DuplicateFillConflict)
        }
        let next_qty=self.fill_qty_atoms.checked_add(fill.qty_atoms).ok_or(EconError::Overflow)?;
        if next_qty>self.requested_qty_atoms{return Err(EconError::Overfill)}
        if self.cumulative_truth_present && next_qty>self.cumulative_qty_atoms{return Err(EconError::AverageMismatch)}
        let add=fill.price_atoms.checked_mul(fill.qty_atoms).ok_or(EconError::Overflow)?;
        let next_notional=self.fill_notional_atoms2.checked_add(add).ok_or(EconError::Overflow)?;
        if self.cumulative_truth_present && next_qty==self.cumulative_qty_atoms {
            let event_avg=next_notional/next_qty;
            let reported=self.cumulative_notional_atoms2/self.cumulative_qty_atoms;
            if event_avg.abs_diff(reported)>avg_tolerance_atoms{return Err(EconError::AverageMismatch)}
        }
        self.fill_qty_atoms=next_qty; self.fill_notional_atoms2=next_notional;
        if let Some(fee)=fill.fee_cost_quote_atoms{
            self.fee_cost_quote_atoms=self.fee_cost_quote_atoms.checked_add(fee).ok_or(EconError::Overflow)?;
        }
        if self.first_fill_time_ms.is_none(){self.first_fill_time_ms=fill.trade_time_ms;}
        if fill.trade_time_ms.is_some(){self.last_fill_time_ms=fill.trade_time_ms;}
        self.fills.insert(fill.fill_id.clone(),fill);
        self.refresh_fee_completeness();
        Ok(ApplyFillResult::Added)
    }
    pub fn filled_qty_atoms(&self)->u128{if self.cumulative_truth_present{self.cumulative_qty_atoms}else{self.fill_qty_atoms}}
    pub fn remaining_qty_atoms(&self)->u128{self.requested_qty_atoms.saturating_sub(self.filled_qty_atoms())}
    pub fn vwap_atoms(&self)->Option<u128>{
        let q=self.filled_qty_atoms(); if q==0{return None}
        let n=if self.cumulative_truth_present{self.cumulative_notional_atoms2}else{self.fill_notional_atoms2};
        Some(n/q)
    }
    pub fn quote_notional_atoms(&self)->Option<u128>{
        let q=self.filled_qty_atoms(); if q==0{return None}
        let n=if self.cumulative_truth_present{self.cumulative_notional_atoms2}else{self.fill_notional_atoms2};
        Some(n/SCALE_12)
    }
}


#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegFillAggregate {
    pub venue:String,
    pub symbol:String,
    pub side:String,
    pub target_qty_atoms:u128,
    pub orders:BTreeMap<String,LegFillLedger>,
}
impl LegFillAggregate {
    pub fn new(venue:&str,symbol:&str,side:&str,target_qty_atoms:u128)->Result<Self,EconError>{
        if target_qty_atoms==0{return Err(EconError::ZeroQuantity)}
        Ok(Self{venue:venue.into(),symbol:symbol.into(),side:side.into(),target_qty_atoms,orders:BTreeMap::new()})
    }
    pub fn add_order(&mut self,intent_id:&str,client_order_id:&str,requested_qty_atoms:u128)->Result<(),EconError>{
        if intent_id.is_empty()||client_order_id.is_empty()||requested_qty_atoms==0{return Err(EconError::InvalidState)}
        if self.orders.contains_key(intent_id)||self.orders.values().any(|x|x.client_order_id==client_order_id){return Err(EconError::InvalidState)}
        let ledger=LegFillLedger::new(&self.venue,&self.symbol,&self.side,intent_id,client_order_id,requested_qty_atoms)?;
        self.orders.insert(intent_id.into(),ledger);Ok(())
    }
    pub fn order(&self,intent_id:&str)->Result<&LegFillLedger,EconError>{self.orders.get(intent_id).ok_or(EconError::InvalidState)}
    pub fn order_mut(&mut self,intent_id:&str)->Result<&mut LegFillLedger,EconError>{self.orders.get_mut(intent_id).ok_or(EconError::InvalidState)}
    pub fn filled_qty_atoms(&self)->Result<u128,EconError>{
        let mut total=0u128;for o in self.orders.values(){total=total.checked_add(o.filled_qty_atoms()).ok_or(EconError::Overflow)?;}if total>self.target_qty_atoms{return Err(EconError::Overfill)}Ok(total)
    }
    pub fn remaining_qty_atoms(&self)->Result<u128,EconError>{Ok(self.target_qty_atoms-self.filled_qty_atoms()?)}
    fn total_notional_atoms2(&self)->Result<u128,EconError>{
        let mut n=0u128;for o in self.orders.values(){let q=o.filled_qty_atoms();if q==0{continue}let px=o.vwap_atoms().ok_or(EconError::MissingActualPrice)?;n=n.checked_add(px.checked_mul(q).ok_or(EconError::Overflow)?).ok_or(EconError::Overflow)?;}Ok(n)
    }
    pub fn vwap_atoms(&self)->Option<u128>{let q=self.filled_qty_atoms().ok()?;if q==0{return None}self.total_notional_atoms2().ok().map(|n|n/q)}
    pub fn quote_notional_atoms(&self)->Option<u128>{self.filled_qty_atoms().ok()?;self.total_notional_atoms2().ok().map(|n|n/SCALE_12)}
    pub fn fee_cost_quote_atoms(&self)->Result<i128,EconError>{self.orders.values().try_fold(0i128,|a,o|a.checked_add(o.fee_cost_quote_atoms).ok_or(EconError::Overflow))}
    pub fn fee_evidence_complete(&self)->bool{
        let filled:Vec<_>=self.orders.values().filter(|o|o.filled_qty_atoms()>0).collect();
        !filled.is_empty()&&filled.iter().all(|o|o.fee_evidence_complete)
    }
    pub fn all_orders_terminal(&self)->bool{!self.orders.is_empty()&&self.orders.values().all(|o|o.terminal)}
    pub fn first_fill_time_ms(&self)->Option<u64>{self.orders.values().filter_map(|o|o.first_fill_time_ms).min()}
    pub fn last_fill_time_ms(&self)->Option<u64>{self.orders.values().filter_map(|o|o.last_fill_time_ms).max()}
    pub fn apply_fill(&mut self,fill:FillEvent,avg_tolerance_atoms:u128)->Result<ApplyFillResult,EconError>{
        if self.orders.len()!=1{return Err(EconError::InvalidState)}
        let key=self.orders.keys().next().cloned().ok_or(EconError::InvalidState)?;
        self.order_mut(&key)?.apply_fill(fill,avg_tolerance_atoms)
    }
    pub fn apply_cumulative_truth(&mut self,qty_atoms:u128,avg_price_atoms:Option<u128>,terminal:bool,status:Option<&str>,exchange_order_id:Option<&str>,observed_fill_time_ms:Option<u64>,avg_tolerance_atoms:u128)->Result<(),EconError>{
        if self.orders.len()!=1{return Err(EconError::InvalidState)}
        let key=self.orders.keys().next().cloned().ok_or(EconError::InvalidState)?;
        self.order_mut(&key)?.apply_cumulative_truth(qty_atoms,avg_price_atoms,terminal,status,exchange_order_id,observed_fill_time_ms,avg_tolerance_atoms)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DepthLevel { pub price_atoms:u128, pub qty_atoms:u128 }
impl DepthLevel {
    pub fn from_str(price:&str,qty:&str)->Result<Self,EconError>{
        Ok(Self{price_atoms:decimal_to_atoms(price)?,qty_atoms:decimal_to_atoms(qty)?})
    }
}
pub fn executable_vwap(levels:&[DepthLevel],required_qty_atoms:u128)->Result<u128,EconError>{
    if required_qty_atoms==0{return Err(EconError::ZeroQuantity)}
    let mut left=required_qty_atoms; let mut notional:u128=0;
    for l in levels{
        if l.price_atoms==0||l.qty_atoms==0{continue}
        let take=left.min(l.qty_atoms);
        notional=notional.checked_add(l.price_atoms.checked_mul(take).ok_or(EconError::Overflow)?).ok_or(EconError::Overflow)?;
        left-=take; if left==0{break}
    }
    if left!=0{return Err(EconError::InsufficientDepth)}
    Ok(notional/required_qty_atoms)
}
pub fn spread_microbps(sell_price_atoms:u128,buy_price_atoms:u128)->Result<i128,EconError>{
    if sell_price_atoms==0||buy_price_atoms==0{return Err(EconError::MissingActualPrice)}
    if sell_price_atoms>i128::MAX as u128||buy_price_atoms>i128::MAX as u128{return Err(EconError::Overflow)}
    let delta=sell_price_atoms as i128-buy_price_atoms as i128;
    delta.checked_mul(10_000).and_then(|x|x.checked_mul(BPS_MICRO_SCALE))
        .and_then(|x|x.checked_div(buy_price_atoms as i128)).ok_or(EconError::Overflow)
}
pub fn bps_micro_to_f64(v:i128)->f64{v as f64/BPS_MICRO_SCALE as f64}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntryTiming {
    pub signal_time_ms:Option<u64>,pub leg1_prepare_time_ms:Option<u64>,pub leg1_send_time_ms:Option<u64>,
    pub leg1_first_fill_time_ms:Option<u64>,pub leg1_terminal_fill_time_ms:Option<u64>,
    pub leg2_prepare_time_ms:Option<u64>,pub leg2_send_time_ms:Option<u64>,
    pub leg2_first_fill_time_ms:Option<u64>,pub leg2_terminal_fill_time_ms:Option<u64>,
}
impl Default for EntryTiming{fn default()->Self{Self{signal_time_ms:None,leg1_prepare_time_ms:None,leg1_send_time_ms:None,
    leg1_first_fill_time_ms:None,leg1_terminal_fill_time_ms:None,leg2_prepare_time_ms:None,leg2_send_time_ms:None,
    leg2_first_fill_time_ms:None,leg2_terminal_fill_time_ms:None}}}
impl EntryTiming {
    pub fn time_to_first_fill_ms(&self)->Option<u64>{Some(self.leg1_first_fill_time_ms?.checked_sub(self.signal_time_ms?)?)}
    pub fn time_leg1_to_leg2_send_ms(&self)->Option<u64>{Some(self.leg2_send_time_ms?.checked_sub(self.leg1_first_fill_time_ms?)?)}
    pub fn time_to_fully_hedged_ms(&self)->Option<u64>{Some(self.leg2_terminal_fill_time_ms?.checked_sub(self.signal_time_ms?)?)}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EntryTelemetrySnapshot {
    pub signal_gross_microbps:i128,
    pub signal_expected_net_microbps:i128,
    pub signal_to_leg1_slippage_microbps:Option<i128>,
    pub post_leg1_projected_gross_microbps:Option<i128>,
    pub post_leg1_projected_net_microbps:Option<i128>,
    pub signal_to_hedge_decay_microbps:Option<i128>,
    pub locked_actual_gross_microbps:Option<i128>,
    pub locked_actual_net_microbps:Option<i128>,
    pub execution_cost_vs_signal_microbps:Option<i128>,
    pub time_to_first_fill_ms:Option<u64>,
    pub time_leg1_to_leg2_send_ms:Option<u64>,
    pub time_to_fully_hedged_ms:Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntryEconomicsState {
    pub signal_gross_microbps:i128,
    pub signal_expected_net_microbps:i128,
    pub signal_spot_price_atoms:Option<u128>,
    pub signal_futures_price_atoms:Option<u128>,
    pub projected_cost_reserve_microbps:i128,
    pub spot:LegFillAggregate,
    pub futures:LegFillAggregate,
    pub timing:EntryTiming,
    pub post_leg1_projected_gross_microbps:Option<i128>,
    pub post_leg1_projected_net_microbps:Option<i128>,
    pub locked_actual_gross_microbps:Option<i128>,
    pub locked_actual_net_microbps:Option<i128>,
    pub signal_to_leg1_slippage_microbps:Option<i128>,
    pub signal_to_hedge_decay_microbps:Option<i128>,
    pub execution_cost_vs_signal_microbps:Option<i128>,
}
impl EntryEconomicsState {
    pub fn validate(&self)->Result<(),EconError>{
        let s=self.spot.filled_qty_atoms()?;let f=self.futures.filled_qty_atoms()?;
        if f>s{return Err(EconError::FuturesOverHedged)}
        if self.spot.venue.is_empty()||self.futures.venue.is_empty()||self.spot.symbol.is_empty()||self.futures.symbol.is_empty(){return Err(EconError::InvalidState)}
        Ok(())
    }
    pub fn exposed_qty_atoms(&self)->Result<u128,EconError>{
        let s=self.spot.filled_qty_atoms()?; let f=self.futures.filled_qty_atoms()?;
        if f>s{return Err(EconError::FuturesOverHedged)} Ok(s-f)
    }
    pub fn telemetry(&self)->EntryTelemetrySnapshot{EntryTelemetrySnapshot{
        signal_gross_microbps:self.signal_gross_microbps,signal_expected_net_microbps:self.signal_expected_net_microbps,
        signal_to_leg1_slippage_microbps:self.signal_to_leg1_slippage_microbps,
        post_leg1_projected_gross_microbps:self.post_leg1_projected_gross_microbps,
        post_leg1_projected_net_microbps:self.post_leg1_projected_net_microbps,
        signal_to_hedge_decay_microbps:self.signal_to_hedge_decay_microbps,
        locked_actual_gross_microbps:self.locked_actual_gross_microbps,locked_actual_net_microbps:self.locked_actual_net_microbps,
        execution_cost_vs_signal_microbps:self.execution_cost_vs_signal_microbps,
        time_to_first_fill_ms:self.timing.time_to_first_fill_ms(),time_leg1_to_leg2_send_ms:self.timing.time_leg1_to_leg2_send_ms(),
        time_to_fully_hedged_ms:self.timing.time_to_fully_hedged_ms()}}
    pub fn refresh_leg1_slippage(&mut self)->Result<Option<i128>,EconError>{
        let Some(signal)=self.signal_spot_price_atoms else{return Ok(None)};
        let Some(actual)=self.spot.vwap_atoms() else{return Ok(None)};
        let slip=spread_microbps(actual,signal)?;
        self.signal_to_leg1_slippage_microbps=Some(slip);Ok(Some(slip))
    }
    pub fn projected_hedge(&mut self,futures_bids:&[DepthLevel])->Result<Revaluation,EconError>{
        let exposure=self.exposed_qty_atoms()?; if exposure==0{return Err(EconError::ZeroQuantity)}
        let spot=self.spot.vwap_atoms().ok_or(EconError::MissingActualPrice)?;
        let fut=executable_vwap(futures_bids,exposure)?;
        let gross=spread_microbps(fut,spot)?;
        let net=gross-self.projected_cost_reserve_microbps;
        self.post_leg1_projected_gross_microbps=Some(gross);self.post_leg1_projected_net_microbps=Some(net);
        self.signal_to_hedge_decay_microbps=Some(gross-self.signal_gross_microbps);
        let _=self.refresh_leg1_slippage()?;
        Ok(Revaluation{exposed_qty_atoms:exposure,actual_spot_vwap_atoms:spot,projected_futures_vwap_atoms:fut,
            projected_gross_microbps:gross,projected_net_microbps:net,
            signal_to_hedge_decay_microbps:gross-self.signal_gross_microbps})
    }
    pub fn lock_actual_entry(&mut self)->Result<LockedEntry,EconError>{
        let sq=self.spot.filled_qty_atoms()?;let fq=self.futures.filled_qty_atoms()?;
        if sq==0||sq!=fq||!self.spot.all_orders_terminal()||!self.futures.all_orders_terminal(){return Err(EconError::InvalidState)}
        let sp=self.spot.vwap_atoms().ok_or(EconError::MissingActualPrice)?;
        let fp=self.futures.vwap_atoms().ok_or(EconError::MissingActualPrice)?;
        let gross=spread_microbps(fp,sp)?;
        let mut fee_microbps=0i128;
        let fee_complete=self.spot.fee_evidence_complete()&&self.futures.fee_evidence_complete();
        if fee_complete{
            let notional=self.spot.quote_notional_atoms().ok_or(EconError::MissingActualPrice)?;
            let fees=self.spot.fee_cost_quote_atoms()?.checked_add(self.futures.fee_cost_quote_atoms()?).ok_or(EconError::Overflow)?;
            if notional>0 {
                fee_microbps=fees.checked_mul(10_000).and_then(|x|x.checked_mul(BPS_MICRO_SCALE))
                    .and_then(|x|x.checked_div(notional as i128)).ok_or(EconError::Overflow)?;
            }
        }
        let total_fee=if fee_complete{Some(self.spot.fee_cost_quote_atoms()?.checked_add(self.futures.fee_cost_quote_atoms()?).ok_or(EconError::Overflow)?)}else{None};
        let net=if fee_complete{Some(gross-fee_microbps)}else{None};
        self.locked_actual_gross_microbps=Some(gross);self.locked_actual_net_microbps=net;
        self.execution_cost_vs_signal_microbps=Some(self.signal_gross_microbps-gross);
        let _=self.refresh_leg1_slippage()?;
        Ok(LockedEntry{actual_spot_filled_qty_atoms:sq,actual_futures_filled_qty_atoms:fq,
            actual_spot_entry_vwap_atoms:sp,actual_futures_entry_vwap_atoms:fp,
            actual_entry_gross_microbps:gross,actual_entry_net_microbps:net,
            actual_fee_cost_quote_atoms:total_fee,actual_fee_cost_microbps:if fee_complete{Some(fee_microbps)}else{None},
            signal_to_locked_decay_microbps:gross-self.signal_gross_microbps})
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Revaluation {
    pub exposed_qty_atoms:u128,pub actual_spot_vwap_atoms:u128,pub projected_futures_vwap_atoms:u128,
    pub projected_gross_microbps:i128,pub projected_net_microbps:i128,pub signal_to_hedge_decay_microbps:i128,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LockedEntry {
    pub actual_spot_filled_qty_atoms:u128,
    pub actual_futures_filled_qty_atoms:u128,
    pub actual_spot_entry_vwap_atoms:u128,
    pub actual_futures_entry_vwap_atoms:u128,
    pub actual_entry_gross_microbps:i128,
    pub actual_entry_net_microbps:Option<i128>,
    pub actual_fee_cost_quote_atoms:Option<i128>,
    pub actual_fee_cost_microbps:Option<i128>,
    pub signal_to_locked_decay_microbps:i128,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExposureAction {
    HedgeNow,HedgeRemaining,WaitForExistingLiveOrder,CancelAndRevalue,UnwindFirstLeg,
    RecoveryRequired,ManualRequired,NoExposure,
}
#[derive(Clone, Debug)]
pub struct HedgeDecisionInput<'a>{
    pub state:&'a EntryEconomicsState,
    pub second_leg_truth_known:bool,
    pub existing_second_leg_live:bool,
    pub live_order_age_ms:u64,
    pub max_live_wait_ms:u64,
    pub cancel_confirmed:bool,
    pub book_fresh:bool,
    pub hedge_bids:&'a [DepthLevel],
    pub unwind_spot_bids:&'a [DepthLevel],
    pub hedge_cost_microbps:i128,
    pub unwind_cost_microbps:i128,
    pub allow_unwind_if_better:bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HedgeDecision {
    pub action:ExposureAction,pub exposed_qty_atoms:u128,
    pub hedge_net_microbps:Option<i128>,pub unwind_net_microbps:Option<i128>,
}
pub fn decide_exposure(i:HedgeDecisionInput<'_>)->HedgeDecision{
    let exposure=match i.state.exposed_qty_atoms(){Ok(v)=>v,Err(_)=>return HedgeDecision{action:ExposureAction::ManualRequired,exposed_qty_atoms:0,hedge_net_microbps:None,unwind_net_microbps:None}};
    if exposure==0{return HedgeDecision{action:ExposureAction::NoExposure,exposed_qty_atoms:0,hedge_net_microbps:None,unwind_net_microbps:None}}
    if !i.second_leg_truth_known{return HedgeDecision{action:ExposureAction::RecoveryRequired,exposed_qty_atoms:exposure,hedge_net_microbps:None,unwind_net_microbps:None}}
    if i.existing_second_leg_live {
        if i.live_order_age_ms<=i.max_live_wait_ms{return HedgeDecision{action:ExposureAction::WaitForExistingLiveOrder,exposed_qty_atoms:exposure,hedge_net_microbps:None,unwind_net_microbps:None}}
        return HedgeDecision{action:if i.cancel_confirmed{ExposureAction::CancelAndRevalue}else{ExposureAction::RecoveryRequired},
            exposed_qty_atoms:exposure,hedge_net_microbps:None,unwind_net_microbps:None}
    }
    if !i.book_fresh{return HedgeDecision{action:ExposureAction::RecoveryRequired,exposed_qty_atoms:exposure,hedge_net_microbps:None,unwind_net_microbps:None}}
    let spot=match i.state.spot.vwap_atoms(){Some(x)=>x,None=>return HedgeDecision{action:ExposureAction::ManualRequired,exposed_qty_atoms:exposure,hedge_net_microbps:None,unwind_net_microbps:None}};
    let hedge=executable_vwap(i.hedge_bids,exposure).ok().and_then(|px|spread_microbps(px,spot).ok()).map(|x|x-i.hedge_cost_microbps);
    let unwind=executable_vwap(i.unwind_spot_bids,exposure).ok().and_then(|px|spread_microbps(px,spot).ok()).map(|x|x-i.unwind_cost_microbps);
    let action=match (hedge,unwind){
        (None,None)=>ExposureAction::RecoveryRequired,
        (None,Some(_))=>if i.allow_unwind_if_better{ExposureAction::UnwindFirstLeg}else{ExposureAction::RecoveryRequired},
        (Some(_),None)=>if i.state.futures.filled_qty_atoms().is_ok_and(|x|x>0){ExposureAction::HedgeRemaining}else{ExposureAction::HedgeNow},
        (Some(h),Some(u)) if i.allow_unwind_if_better && u>h=>ExposureAction::UnwindFirstLeg,
        (Some(_),Some(_))=>if i.state.futures.filled_qty_atoms().is_ok_and(|x|x>0){ExposureAction::HedgeRemaining}else{ExposureAction::HedgeNow},
    };
    HedgeDecision{action,exposed_qty_atoms:exposure,hedge_net_microbps:hedge,unwind_net_microbps:unwind}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RealizedPnl {
    pub gross_quote_atoms:i128,pub fee_cost_quote_atoms:i128,pub net_quote_atoms:i128,
}
pub fn realized_round_trip_pnl(entry_spot:u128,entry_futures:u128,exit_spot:u128,exit_futures:u128,
    qty_atoms:u128,total_fee_cost_quote_atoms:i128)->Result<RealizedPnl,EconError>{
    for x in [entry_spot,entry_futures,exit_spot,exit_futures]{if x>i128::MAX as u128{return Err(EconError::Overflow)}}
    if qty_atoms>i128::MAX as u128{return Err(EconError::Overflow)}
    let spot=(exit_spot as i128-entry_spot as i128).checked_mul(qty_atoms as i128).and_then(|x|x.checked_div(SCALE_12 as i128)).ok_or(EconError::Overflow)?;
    let fut=(entry_futures as i128-exit_futures as i128).checked_mul(qty_atoms as i128).and_then(|x|x.checked_div(SCALE_12 as i128)).ok_or(EconError::Overflow)?;
    let gross=spot.checked_add(fut).ok_or(EconError::Overflow)?;
    let net=gross.checked_sub(total_fee_cost_quote_atoms).ok_or(EconError::Overflow)?;
    Ok(RealizedPnl{gross_quote_atoms:gross,fee_cost_quote_atoms:total_fee_cost_quote_atoms,net_quote_atoms:net})
}

#[derive(Debug,Clone)]pub struct EconomicsStore{path:PathBuf}
impl EconomicsStore{
    pub fn open(path:impl AsRef<Path>)->io::Result<Self>{let path=path.as_ref().to_path_buf();if let Some(p)=path.parent(){fs::create_dir_all(p)?;}Ok(Self{path})}
    pub fn persist(&self,state:&EntryEconomicsState)->io::Result<()>{
        state.validate().map_err(|_|io::Error::new(io::ErrorKind::InvalidData,"invalid economics state"))?;
        let parent=self.path.parent().ok_or_else(||io::Error::new(io::ErrorKind::InvalidInput,"parent"))?;
        let tmp=self.path.with_extension("tmp");
        let bytes=serde_json::to_vec(state).map_err(|e|io::Error::new(io::ErrorKind::InvalidData,e))?;
        let mut f=OpenOptions::new().write(true).create(true).truncate(true).open(&tmp)?;f.write_all(&bytes)?;f.sync_all()?;
        fs::rename(&tmp,&self.path)?;File::open(parent)?.sync_all()
    }
    pub fn load(&self)->io::Result<Option<EntryEconomicsState>>{
        if !self.path.exists(){return Ok(None)}
        let mut b=Vec::new();File::open(&self.path)?.read_to_end(&mut b)?;
        let s:EntryEconomicsState=serde_json::from_slice(&b).map_err(|e|io::Error::new(io::ErrorKind::InvalidData,e))?;
        s.validate().map_err(|_|io::Error::new(io::ErrorKind::InvalidData,"invalid economics state"))?;Ok(Some(s))
    }
}

#[cfg(test)]
mod tests{
    use super::*;
    use std::time::{SystemTime,UNIX_EPOCH};
    fn a(x:&str)->u128{decimal_to_atoms(x).unwrap()}
    fn ledger(side:&str,qty:&str)->LegFillAggregate{let mut x=LegFillAggregate::new(if side=="buy"{"okx"}else{"binance"},"TEST",side,a(qty)).unwrap();x.add_order("i","c",a(qty)).unwrap();x}
    fn fill(id:&str,px:&str,qty:&str,fee:Option<&str>,ts:u64)->FillEvent{FillEvent{fill_id:id.into(),price_atoms:a(px),qty_atoms:a(qty),
        fee_cost_quote_atoms:fee.map(|x|a(x) as i128),fee_currency:fee.map(|_|"USDT".into()),trade_time_ms:Some(ts)}}
    fn state()->EntryEconomicsState{EntryEconomicsState{signal_gross_microbps:60_000_000,signal_expected_net_microbps:36_000_000,
        signal_spot_price_atoms:Some(a("100")),signal_futures_price_atoms:Some(a("100.6")),projected_cost_reserve_microbps:24_000_000,spot:ledger("buy","10"),futures:ledger("sell","10"),timing:EntryTiming::default(),
        post_leg1_projected_gross_microbps:None,post_leg1_projected_net_microbps:None,locked_actual_gross_microbps:None,locked_actual_net_microbps:None,
        signal_to_leg1_slippage_microbps:None,signal_to_hedge_decay_microbps:None,execution_cost_vs_signal_microbps:None}}
    fn depth(px:&str,qty:&str)->Vec<DepthLevel>{vec![DepthLevel::from_str(px,qty).unwrap()]}
    #[test]fn a_single_fill_leg1_revalues(){let mut s=state();s.spot.apply_fill(fill("1","100.08","10",Some("0.01"),10),1).unwrap();
        let r=s.projected_hedge(&depth("100.45","10")).unwrap();assert_eq!(r.exposed_qty_atoms,a("10"));assert!(bps_micro_to_f64(r.projected_gross_microbps)>36.9);}
    #[test]fn b_multiple_fills_exact_vwap(){let mut l=ledger("buy","10");l.apply_fill(fill("1","100.05","4",Some("0.004"),1),1).unwrap();
        l.apply_fill(fill("2","100.10","6",Some("0.006"),2),1).unwrap();assert_eq!(atoms_to_string(l.vwap_atoms().unwrap()),"100.08");}
    #[test]fn c_partial_leg1_hedges_exact_exposure(){let mut s=state();s.spot.apply_fill(fill("1","100","4",Some("0"),1),1).unwrap();
        let d=decide_exposure(HedgeDecisionInput{state:&s,second_leg_truth_known:true,existing_second_leg_live:false,live_order_age_ms:0,max_live_wait_ms:100,
            cancel_confirmed:false,book_fresh:true,hedge_bids:&depth("100.2","4"),unwind_spot_bids:&depth("99.9","4"),hedge_cost_microbps:0,unwind_cost_microbps:0,allow_unwind_if_better:false});
        assert_eq!(d.action,ExposureAction::HedgeNow);assert_eq!(d.exposed_qty_atoms,a("4"));}
    #[test]fn d_additional_leg1_fill_updates_vwap_and_exposure(){let mut s=state();s.spot.apply_fill(fill("1","100","4",Some("0"),1),1).unwrap();
        s.spot.apply_fill(fill("2","101","2",Some("0"),2),1).unwrap();assert_eq!(s.exposed_qty_atoms().unwrap(),a("6"));assert_eq!(atoms_to_string(s.spot.vwap_atoms().unwrap()),"100.333333333333");}
    #[test]fn e_partial_leg2_only_remaining_is_hedged(){let mut s=state();s.spot.apply_fill(fill("s","100","10",Some("0"),1),1).unwrap();
        s.futures.apply_fill(fill("f","100.4","3",Some("0"),2),1).unwrap();let d=decide_exposure(HedgeDecisionInput{state:&s,second_leg_truth_known:true,existing_second_leg_live:false,
            live_order_age_ms:0,max_live_wait_ms:10,cancel_confirmed:false,book_fresh:true,hedge_bids:&depth("100.3","7"),unwind_spot_bids:&[],hedge_cost_microbps:0,unwind_cost_microbps:0,allow_unwind_if_better:false});
        assert_eq!(d.action,ExposureAction::HedgeRemaining);assert_eq!(d.exposed_qty_atoms,a("7"));}
    #[test]fn f_below_original_threshold_does_not_abandon_exposure(){let mut s=state();s.spot.apply_fill(fill("s","100","10",Some("0"),1),1).unwrap();
        let d=decide_exposure(HedgeDecisionInput{state:&s,second_leg_truth_known:true,existing_second_leg_live:false,live_order_age_ms:0,max_live_wait_ms:10,cancel_confirmed:false,
            book_fresh:true,hedge_bids:&depth("100.30","10"),unwind_spot_bids:&depth("99.0","10"),hedge_cost_microbps:24_000_000,unwind_cost_microbps:5_000_000,allow_unwind_if_better:false});
        assert_eq!(d.action,ExposureAction::HedgeNow);assert!(d.hedge_net_microbps.unwrap()<42_000_000);}
    #[test]fn g_profitable_hedge(){let mut s=state();s.spot.apply_fill(fill("s","100","10",Some("0"),1),1).unwrap();
        let d=decide_exposure(HedgeDecisionInput{state:&s,second_leg_truth_known:true,existing_second_leg_live:false,live_order_age_ms:0,max_live_wait_ms:10,cancel_confirmed:false,
            book_fresh:true,hedge_bids:&depth("101","10"),unwind_spot_bids:&depth("99","10"),hedge_cost_microbps:10_000_000,unwind_cost_microbps:0,allow_unwind_if_better:false});assert_eq!(d.action,ExposureAction::HedgeNow);}
    #[test]fn h_mild_loss_hedge_better_than_unwind(){let mut s=state();s.spot.apply_fill(fill("s","100","10",Some("0"),1),1).unwrap();
        let d=decide_exposure(HedgeDecisionInput{state:&s,second_leg_truth_known:true,existing_second_leg_live:false,live_order_age_ms:0,max_live_wait_ms:10,cancel_confirmed:false,
            book_fresh:true,hedge_bids:&depth("99.95","10"),unwind_spot_bids:&depth("99.0","10"),hedge_cost_microbps:0,unwind_cost_microbps:0,allow_unwind_if_better:false});assert_eq!(d.action,ExposureAction::HedgeNow);}
    #[test]fn i_unwind_cheaper_than_bad_hedge(){let mut s=state();s.spot.apply_fill(fill("s","100","10",Some("0"),1),1).unwrap();
        let d=decide_exposure(HedgeDecisionInput{state:&s,second_leg_truth_known:true,existing_second_leg_live:false,live_order_age_ms:0,max_live_wait_ms:10,cancel_confirmed:false,
            book_fresh:true,hedge_bids:&depth("98","10"),unwind_spot_bids:&depth("99.8","10"),hedge_cost_microbps:0,unwind_cost_microbps:0,allow_unwind_if_better:true});assert_eq!(d.action,ExposureAction::UnwindFirstLeg);}
    #[test]fn j_stale_book_forces_recovery(){let mut s=state();s.spot.apply_fill(fill("s","100","10",Some("0"),1),1).unwrap();
        let d=decide_exposure(HedgeDecisionInput{state:&s,second_leg_truth_known:true,existing_second_leg_live:false,live_order_age_ms:0,max_live_wait_ms:10,cancel_confirmed:false,
            book_fresh:false,hedge_bids:&depth("101","10"),unwind_spot_bids:&depth("100","10"),hedge_cost_microbps:0,unwind_cost_microbps:0,allow_unwind_if_better:false});assert_eq!(d.action,ExposureAction::RecoveryRequired);}
    #[test]fn k_unknown_order_truth_recovery_only(){let mut s=state();s.spot.apply_fill(fill("s","100","10",Some("0"),1),1).unwrap();
        let d=decide_exposure(HedgeDecisionInput{state:&s,second_leg_truth_known:false,existing_second_leg_live:false,live_order_age_ms:0,max_live_wait_ms:10,cancel_confirmed:false,
            book_fresh:true,hedge_bids:&depth("101","10"),unwind_spot_bids:&depth("100","10"),hedge_cost_microbps:0,unwind_cost_microbps:0,allow_unwind_if_better:false});assert_eq!(d.action,ExposureAction::RecoveryRequired);}
    #[test]fn l_duplicate_fill_is_idempotent(){let mut l=ledger("buy","10");let f=fill("x","100","3",Some("0.1"),1);assert_eq!(l.apply_fill(f.clone(),1).unwrap(),ApplyFillResult::Added);
        assert_eq!(l.apply_fill(f,1).unwrap(),ApplyFillResult::DuplicateNoOp);assert_eq!(l.filled_qty_atoms().unwrap(),a("3"));assert_eq!(l.fee_cost_quote_atoms().unwrap(),a("0.1") as i128);}
    #[test]fn m_crash_restart_preserves_exact_economics(){let mut s=state();s.spot.apply_fill(fill("s","100.08","10",Some("0.01"),1),1).unwrap();
        let root=std::env::temp_dir().join(format!("r19-{}",SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()));let store=EconomicsStore::open(root.join("e.json")).unwrap();
        store.persist(&s).unwrap();let mut r=store.load().unwrap().unwrap();assert_eq!(r,s);
        let before_fee=r.spot.fee_cost_quote_atoms().unwrap();assert_eq!(r.spot.apply_fill(fill("s","100.08","10",Some("0.01"),1),1).unwrap(),ApplyFillResult::DuplicateNoOp);
        assert_eq!(r.spot.fee_cost_quote_atoms().unwrap(),before_fee);let _=fs::remove_dir_all(root);}
    #[test]fn n_reported_average_mismatch_fails_closed(){let mut l=ledger("buy","10");l.apply_fill(fill("a","100","4",Some("0"),1),1).unwrap();
        l.apply_fill(fill("b","102","6",Some("0"),2),1).unwrap();assert_eq!(l.apply_cumulative_truth(a("10"),Some(a("100")),true,Some("filled"),Some("o"),Some(2),1),Err(EconError::AverageMismatch));}
    #[test]fn o_quote_fee_is_accumulated(){let mut l=ledger("buy","2");l.apply_fill(fill("1","100","1",Some("0.02"),1),1).unwrap();l.apply_fill(fill("2","100","1",Some("0.03"),2),1).unwrap();l.apply_cumulative_truth(a("2"),Some(a("100")),true,Some("filled"),Some("o"),Some(2),1).unwrap();
        assert_eq!(l.fee_cost_quote_atoms().unwrap(),a("0.05") as i128);assert!(l.fee_evidence_complete());}
    #[test]fn p_missing_fee_marks_incomplete(){let mut l=ledger("buy","1");l.apply_fill(fill("1","100","1",None,1),1).unwrap();l.apply_cumulative_truth(a("1"),Some(a("100")),true,Some("filled"),Some("o"),Some(1),1).unwrap();assert!(!l.fee_evidence_complete());}
    #[test]fn q_locked_actual_two_leg_spread(){let mut s=state();s.spot.apply_fill(fill("s","100.08","10",Some("0.01"),1),1).unwrap();s.spot.apply_cumulative_truth(a("10"),Some(a("100.08")),true,Some("filled"),Some("os"),Some(1),1).unwrap();
        s.futures.apply_fill(fill("f","100.43","10",Some("0.01"),2),1).unwrap();s.futures.apply_cumulative_truth(a("10"),Some(a("100.43")),true,Some("filled"),Some("of"),Some(2),1).unwrap();let x=s.lock_actual_entry().unwrap();assert_eq!(x.actual_spot_filled_qty_atoms,a("10"));assert!(bps_micro_to_f64(x.actual_entry_gross_microbps)>34.9);assert!(x.actual_entry_net_microbps.is_some());}
    #[test]fn p2_missing_fee_keeps_actual_net_unknown(){let mut s=state();s.spot.apply_fill(fill("s","100","10",None,1),1).unwrap();s.spot.apply_cumulative_truth(a("10"),Some(a("100")),true,Some("filled"),Some("os"),Some(1),1).unwrap();
        s.futures.apply_fill(fill("f","100.5","10",Some("0.01"),2),1).unwrap();s.futures.apply_cumulative_truth(a("10"),Some(a("100.5")),true,Some("filled"),Some("of"),Some(2),1).unwrap();let x=s.lock_actual_entry().unwrap();assert!(x.actual_entry_net_microbps.is_none());assert!(s.locked_actual_net_microbps.is_none());}
    #[test]fn q2_nonterminal_fill_cannot_be_locked_as_final_entry(){let mut s=state();s.spot.apply_fill(fill("s","100","10",Some("0"),1),1).unwrap();s.futures.apply_fill(fill("f","100.5","10",Some("0"),2),1).unwrap();assert_eq!(s.lock_actual_entry(),Err(EconError::InvalidState));}
    #[test]fn r_actual_close_realized_pnl(){let p=realized_round_trip_pnl(a("100"),a("101"),a("102"),a("99"),a("10"),a("0.2") as i128).unwrap();
        assert_eq!(signed_atoms_to_string(p.gross_quote_atoms),"40");assert_eq!(signed_atoms_to_string(p.net_quote_atoms),"39.8");}
    #[test]fn s_timing_and_decay_telemetry(){let mut s=state();s.timing=EntryTiming{signal_time_ms:Some(1000),leg1_first_fill_time_ms:Some(1010),leg2_send_time_ms:Some(1015),leg2_terminal_fill_time_ms:Some(1025),..EntryTiming::default()};
        s.spot.apply_fill(fill("s","100","10",Some("0"),1010),1).unwrap();let r=s.projected_hedge(&depth("100.4","10")).unwrap();
        assert_eq!(s.timing.time_to_first_fill_ms(),Some(10));assert_eq!(s.timing.time_leg1_to_leg2_send_ms(),Some(5));assert_eq!(s.timing.time_to_fully_hedged_ms(),Some(25));assert!(r.signal_to_hedge_decay_microbps<0);assert_eq!(s.signal_to_leg1_slippage_microbps,Some(0));assert_eq!(s.signal_to_hedge_decay_microbps,Some(r.signal_to_hedge_decay_microbps));}
    #[test]fn t_existing_live_order_wait_and_cancel_truth_are_bounded(){let mut s=state();s.spot.apply_fill(fill("s","100","10",Some("0"),1),1).unwrap();
        let mk=|age,cancel|decide_exposure(HedgeDecisionInput{state:&s,second_leg_truth_known:true,existing_second_leg_live:true,live_order_age_ms:age,max_live_wait_ms:100,cancel_confirmed:cancel,
            book_fresh:true,hedge_bids:&[],unwind_spot_bids:&[],hedge_cost_microbps:0,unwind_cost_microbps:0,allow_unwind_if_better:false}).action;
        assert_eq!(mk(50,false),ExposureAction::WaitForExistingLiveOrder);assert_eq!(mk(101,false),ExposureAction::RecoveryRequired);assert_eq!(mk(101,true),ExposureAction::CancelAndRevalue);}
    #[test]fn unwind_choice_requires_explicit_policy(){let mut s=state();s.spot.apply_fill(fill("s","100","10",Some("0"),1),1).unwrap();
        let d=decide_exposure(HedgeDecisionInput{state:&s,second_leg_truth_known:true,existing_second_leg_live:false,live_order_age_ms:0,max_live_wait_ms:100,cancel_confirmed:false,
            book_fresh:true,hedge_bids:&depth("98","10"),unwind_spot_bids:&depth("99.9","10"),hedge_cost_microbps:0,unwind_cost_microbps:0,allow_unwind_if_better:false});
        assert_eq!(d.action,ExposureAction::HedgeNow);}
    #[test]fn telemetry_snapshot_exposes_signal_projected_locked_and_timing(){let mut s=state();s.timing.signal_time_ms=Some(100);s.timing.leg1_first_fill_time_ms=Some(110);s.timing.leg2_send_time_ms=Some(115);s.timing.leg2_terminal_fill_time_ms=Some(130);
        s.spot.apply_fill(fill("s","100.1","10",Some("0.01"),110),1).unwrap();s.spot.apply_cumulative_truth(a("10"),Some(a("100.1")),true,Some("filled"),Some("os"),Some(110),1).unwrap();s.projected_hedge(&depth("100.5","10")).unwrap();s.futures.apply_fill(fill("f","100.4","10",Some("0.01"),130),1).unwrap();s.futures.apply_cumulative_truth(a("10"),Some(a("100.4")),true,Some("filled"),Some("of"),Some(130),1).unwrap();s.lock_actual_entry().unwrap();
        let t=s.telemetry();assert_eq!(t.time_to_first_fill_ms,Some(10));assert_eq!(t.time_leg1_to_leg2_send_ms,Some(5));assert_eq!(t.time_to_fully_hedged_ms,Some(30));assert!(t.locked_actual_gross_microbps.is_some());assert!(t.execution_cost_vs_signal_microbps.is_some());}

    #[test]fn aggregate_multi_order_overfill_is_fail_closed(){let mut x=LegFillAggregate::new("binance","TEST","sell",a("4")).unwrap();x.add_order("a","ca",a("4")).unwrap();x.add_order("b","cb",a("2")).unwrap();
        x.order_mut("a").unwrap().apply_fill(fill("fa","100","4",Some("0"),1),1).unwrap();x.order_mut("b").unwrap().apply_fill(fill("fb","100","1",Some("0"),2),1).unwrap();assert_eq!(x.filled_qty_atoms(),Err(EconError::Overfill));}

    #[test]fn cumulative_truth_is_monotonic_and_idempotent(){let mut l=ledger("buy","10");l.apply_cumulative_truth(a("4"),Some(a("100")),false,Some("partially_filled"),Some("o"),Some(1),1).unwrap();
        l.apply_cumulative_truth(a("4"),Some(a("100")),false,Some("partially_filled"),Some("o"),Some(1),1).unwrap();assert_eq!(l.apply_cumulative_truth(a("3"),Some(a("100")),false,None,Some("o"),Some(2),1),Err(EconError::NonMonotonicCumulative));}
    #[test]fn insufficient_hedge_depth_prefers_known_unwind(){let mut s=state();s.spot.apply_fill(fill("s","100","10",Some("0"),1),1).unwrap();
        let d=decide_exposure(HedgeDecisionInput{state:&s,second_leg_truth_known:true,existing_second_leg_live:false,live_order_age_ms:0,max_live_wait_ms:100,cancel_confirmed:false,book_fresh:true,
            hedge_bids:&depth("100","2"),unwind_spot_bids:&depth("99.5","10"),hedge_cost_microbps:0,unwind_cost_microbps:0,allow_unwind_if_better:true});assert_eq!(d.action,ExposureAction::UnwindFirstLeg);}
}


#[cfg(test)]
mod r19_integrated_actual_execution {
    use super::*;
    use crate::execution::{CoordError, Coordinator, CoordinatorState, EntryFillProgress};
    use crate::private_rest::{
        apply_order_economic_truth, parse_binance_economic_truth_http,
        parse_binance_fill_events_http, parse_okx_economic_truth_http,
        parse_okx_fill_events_http,
    };
    use std::time::{SystemTime, UNIX_EPOCH};

    fn a(v:&str)->u128{decimal_to_atoms(v).unwrap()}

    #[test]
    fn terminal_partial_then_multi_order_hedge_locks_actual_economics_and_survives_restart() {
        let mut spot=LegFillAggregate::new("okx","TEST-USDT","buy",a("10")).unwrap();
        spot.add_order("spot-1","spot-client-1",a("10")).unwrap();
        let futures=LegFillAggregate::new("binance","TESTUSDT","sell",a("10")).unwrap();
        let mut econ=EntryEconomicsState{
            signal_gross_microbps:60_000_000,
            signal_expected_net_microbps:36_000_000,
            signal_spot_price_atoms:Some(a("100")),
            signal_futures_price_atoms:Some(a("100.6")),
            projected_cost_reserve_microbps:24_000_000,
            spot,futures,timing:EntryTiming{signal_time_ms:Some(1000),..EntryTiming::default()},
            post_leg1_projected_gross_microbps:None,post_leg1_projected_net_microbps:None,
            locked_actual_gross_microbps:None,locked_actual_net_microbps:None,
            signal_to_leg1_slippage_microbps:None,signal_to_hedge_decay_microbps:None,
            execution_cost_vs_signal_microbps:None,
        };
        let mut coord=Coordinator::new(7);
        coord.prepare_spot(7).unwrap();

        // First leg: requested 10, but terminal exchange truth proves only 4 filled.
        let spot_truth=parse_okx_economic_truth_http(200,
            r#"{"code":"0","data":[{"ordId":"os1","clOrdId":"spot-client-1","state":"canceled","accFillSz":"4","avgPx":"100.05","fillTime":"1010"}],"msg":""}"#);
        apply_order_economic_truth(econ.spot.order_mut("spot-1").unwrap(),&spot_truth).unwrap();
        let spot_fills=parse_okx_fill_events_http(200,
            r#"{"code":"0","data":[{"tradeId":"st1","ordId":"os1","fillPx":"100.05","fillSz":"4","fee":"-0.04","feeCcy":"USDT","fillTime":"1010"}]}"#,
            "os1","TEST","USDT").unwrap();
        for f in spot_fills{econ.spot.order_mut("spot-1").unwrap().apply_fill(f,DEFAULT_AVG_TOLERANCE_ATOMS).unwrap();}
        econ.timing.leg1_first_fill_time_ms=Some(1010);econ.timing.leg1_terminal_fill_time_ms=Some(1010);
        assert_eq!(coord.spot_result_revaluable(4,10,true,false),Ok(EntryFillProgress::Exposure{remaining:4}));
        assert_eq!(econ.exposed_qty_atoms().unwrap(),a("4"));

        // Original 42 bps admission threshold is no longer the exposure decision.
        let projected=econ.projected_hedge(&[DepthLevel::from_str("100.30","4").unwrap()]).unwrap();
        assert!(projected.projected_net_microbps < 42_000_000);
        let action=decide_exposure(HedgeDecisionInput{
            state:&econ,second_leg_truth_known:true,existing_second_leg_live:false,
            live_order_age_ms:0,max_live_wait_ms:100,cancel_confirmed:false,book_fresh:true,
            hedge_bids:&[DepthLevel::from_str("100.30","4").unwrap()],
            unwind_spot_bids:&[DepthLevel::from_str("99.0","4").unwrap()],
            hedge_cost_microbps:24_000_000,unwind_cost_microbps:5_000_000,
            allow_unwind_if_better:false,
        });
        assert_eq!(action.action,ExposureAction::HedgeNow);
        assert_eq!(action.exposed_qty_atoms,a("4"));

        // Futures order #1: 2/4 is observed but order is still live.
        econ.futures.add_order("fut-1","fut-client-1",a("4")).unwrap();
        coord.prepare_futures(7).unwrap();
        let f1_live=parse_binance_economic_truth_http(200,
            r#"{"orderId":501,"clientOrderId":"fut-client-1","status":"PARTIALLY_FILLED","executedQty":"2","avgPrice":"100.25","cumQuote":"200.5","updateTime":1015}"#);
        apply_order_economic_truth(econ.futures.order_mut("fut-1").unwrap(),&f1_live).unwrap();
        let f1_fill=parse_binance_fill_events_http(200,
            r#"[{"id":7001,"orderId":501,"price":"100.25","qty":"2","quoteQty":"200.5","commission":"0.02","commissionAsset":"USDT","time":1015}]"#,
            "501","USDT").unwrap().remove(0);
        econ.futures.order_mut("fut-1").unwrap().apply_fill(f1_fill,DEFAULT_AVG_TOLERANCE_ATOMS).unwrap();
        assert_eq!(coord.futures_result_revaluable(2,false,false),Ok(EntryFillProgress::Exposure{remaining:2}));
        assert_eq!(coord.state,CoordinatorState::FuturesPending);
        assert_eq!(coord.prepare_futures(7),Err(CoordError::WrongState));

        // Same order becomes terminal partial. Only now may a new remaining hedge intent exist.
        let f1_terminal=parse_binance_economic_truth_http(200,
            r#"{"orderId":501,"clientOrderId":"fut-client-1","status":"CANCELED","executedQty":"2","avgPrice":"100.25","cumQuote":"200.5","updateTime":1020}"#);
        apply_order_economic_truth(econ.futures.order_mut("fut-1").unwrap(),&f1_terminal).unwrap();
        assert_eq!(coord.futures_result_revaluable(2,true,false),Ok(EntryFillProgress::Exposure{remaining:2}));
        assert_eq!(coord.state,CoordinatorState::SpotFilled);
        let remain=decide_exposure(HedgeDecisionInput{
            state:&econ,second_leg_truth_known:true,existing_second_leg_live:false,
            live_order_age_ms:0,max_live_wait_ms:100,cancel_confirmed:false,book_fresh:true,
            hedge_bids:&[DepthLevel::from_str("100.35","2").unwrap()],
            unwind_spot_bids:&[],hedge_cost_microbps:24_000_000,unwind_cost_microbps:0,
            allow_unwind_if_better:false,
        });
        assert_eq!(remain.action,ExposureAction::HedgeRemaining);
        assert_eq!(remain.exposed_qty_atoms,a("2"));

        // Futures order #2 fills exactly the remaining 2.
        econ.futures.add_order("fut-2","fut-client-2",a("2")).unwrap();
        coord.prepare_futures(7).unwrap();
        let f2_truth=parse_binance_economic_truth_http(200,
            r#"{"orderId":502,"clientOrderId":"fut-client-2","status":"FILLED","executedQty":"2","avgPrice":"100.35","cumQuote":"200.7","updateTime":1025}"#);
        apply_order_economic_truth(econ.futures.order_mut("fut-2").unwrap(),&f2_truth).unwrap();
        let f2_fill=parse_binance_fill_events_http(200,
            r#"[{"id":7002,"orderId":502,"price":"100.35","qty":"2","quoteQty":"200.7","commission":"0.02","commissionAsset":"USDT","time":1025}]"#,
            "502","USDT").unwrap().remove(0);
        econ.futures.order_mut("fut-2").unwrap().apply_fill(f2_fill.clone(),DEFAULT_AVG_TOLERANCE_ATOMS).unwrap();
        econ.timing.leg2_first_fill_time_ms=Some(1015);econ.timing.leg2_terminal_fill_time_ms=Some(1025);
        assert_eq!(coord.futures_result_revaluable(4,true,false),Ok(EntryFillProgress::Hedged));
        assert_eq!(coord.state,CoordinatorState::Hedged);
        assert_eq!(econ.exposed_qty_atoms().unwrap(),0);
        assert_eq!(atoms_to_string(econ.futures.vwap_atoms().unwrap()),"100.3");

        let locked=econ.lock_actual_entry().unwrap();
        assert_eq!(locked.actual_spot_filled_qty_atoms,a("4"));
        assert_eq!(locked.actual_futures_filled_qty_atoms,a("4"));
        assert_eq!(atoms_to_string(locked.actual_spot_entry_vwap_atoms),"100.05");
        assert_eq!(atoms_to_string(locked.actual_futures_entry_vwap_atoms),"100.3");
        assert!(locked.actual_entry_net_microbps.is_some());
        assert!(locked.signal_to_locked_decay_microbps<0);

        // Durable restart reproduces the same state; repeated trade is an exact no-op.
        let root=std::env::temp_dir().join(format!("r19-e2e-{}",SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()));
        let store=EconomicsStore::open(root.join("economics.json")).unwrap();
        store.persist(&econ).unwrap();
        let mut reopened=store.load().unwrap().unwrap();
        assert_eq!(reopened,econ);
        let before_qty=reopened.futures.filled_qty_atoms().unwrap();
        let before_fee=reopened.futures.fee_cost_quote_atoms().unwrap();
        assert_eq!(reopened.futures.order_mut("fut-2").unwrap().apply_fill(f2_fill,DEFAULT_AVG_TOLERANCE_ATOMS).unwrap(),ApplyFillResult::DuplicateNoOp);
        assert_eq!(reopened.futures.filled_qty_atoms().unwrap(),before_qty);
        assert_eq!(reopened.futures.fee_cost_quote_atoms().unwrap(),before_fee);
        let _=fs::remove_dir_all(root);
    }
}


#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExitEconomicsState {
    pub spot:LegFillAggregate,
    pub futures:LegFillAggregate,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClosedEconomics {
    pub qty_atoms:u128,
    pub actual_spot_entry_vwap_atoms:u128,
    pub actual_futures_entry_vwap_atoms:u128,
    pub actual_spot_exit_vwap_atoms:u128,
    pub actual_futures_exit_vwap_atoms:u128,
    pub entry_fee_cost_quote_atoms:Option<i128>,
    pub exit_fee_cost_quote_atoms:Option<i128>,
    pub trade_fees_complete:bool,
    pub execution_gross_pnl_quote_atoms:i128,
    pub execution_pnl_after_trade_fees_quote_atoms:Option<i128>,
    pub funding_included:bool,
}
impl ExitEconomicsState {
    pub fn validate(&self)->Result<(),EconError>{
        self.spot.filled_qty_atoms()?;
        self.futures.filled_qty_atoms()?;
        if self.spot.venue.is_empty()||self.futures.venue.is_empty(){return Err(EconError::InvalidState)}
        Ok(())
    }
    pub fn finalize_against_entry(&self,entry:&EntryEconomicsState)->Result<ClosedEconomics,EconError>{
        self.validate()?;
        let mut e=entry.clone();
        let locked=e.lock_actual_entry()?;
        let sq=self.spot.filled_qty_atoms()?;
        let fq=self.futures.filled_qty_atoms()?;
        if sq!=locked.actual_spot_filled_qty_atoms||fq!=locked.actual_futures_filled_qty_atoms||sq!=fq
            ||!self.spot.all_orders_terminal()||!self.futures.all_orders_terminal(){return Err(EconError::InvalidState)}
        let sx=self.spot.vwap_atoms().ok_or(EconError::MissingActualPrice)?;
        let fx=self.futures.vwap_atoms().ok_or(EconError::MissingActualPrice)?;
        let gross=realized_round_trip_pnl(
            locked.actual_spot_entry_vwap_atoms,locked.actual_futures_entry_vwap_atoms,
            sx,fx,sq,0)?.gross_quote_atoms;
        let exit_fee_complete=self.spot.fee_evidence_complete()&&self.futures.fee_evidence_complete();
        let exit_fee=if exit_fee_complete{Some(self.spot.fee_cost_quote_atoms()?.checked_add(self.futures.fee_cost_quote_atoms()?).ok_or(EconError::Overflow)?)}else{None};
        let complete=locked.actual_fee_cost_quote_atoms.is_some()&&exit_fee.is_some();
        let net=if complete{
            let fees=locked.actual_fee_cost_quote_atoms.unwrap().checked_add(exit_fee.unwrap()).ok_or(EconError::Overflow)?;
            Some(gross.checked_sub(fees).ok_or(EconError::Overflow)?)
        }else{None};
        Ok(ClosedEconomics{qty_atoms:sq,
            actual_spot_entry_vwap_atoms:locked.actual_spot_entry_vwap_atoms,
            actual_futures_entry_vwap_atoms:locked.actual_futures_entry_vwap_atoms,
            actual_spot_exit_vwap_atoms:sx,actual_futures_exit_vwap_atoms:fx,
            entry_fee_cost_quote_atoms:locked.actual_fee_cost_quote_atoms,
            exit_fee_cost_quote_atoms:exit_fee,trade_fees_complete:complete,
            execution_gross_pnl_quote_atoms:gross,
            execution_pnl_after_trade_fees_quote_atoms:net,funding_included:false})
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PositionEconomicsState {
    pub entry:EntryEconomicsState,
    pub exit:Option<ExitEconomicsState>,
}
impl PositionEconomicsState {
    pub fn validate(&self)->Result<(),EconError>{self.entry.validate()?;if let Some(x)=&self.exit{x.validate()?;}Ok(())}
}

#[derive(Debug,Clone)]pub struct PositionEconomicsStore{path:PathBuf}
impl PositionEconomicsStore{
    pub fn open(path:impl AsRef<Path>)->io::Result<Self>{let path=path.as_ref().to_path_buf();if let Some(p)=path.parent(){fs::create_dir_all(p)?;}Ok(Self{path})}
    pub fn persist(&self,state:&PositionEconomicsState)->io::Result<()>{
        state.validate().map_err(|_|io::Error::new(io::ErrorKind::InvalidData,"invalid position economics"))?;
        let parent=self.path.parent().ok_or_else(||io::Error::new(io::ErrorKind::InvalidInput,"parent"))?;
        let tmp=self.path.with_extension("tmp");let bytes=serde_json::to_vec(state).map_err(|e|io::Error::new(io::ErrorKind::InvalidData,e))?;
        let mut f=OpenOptions::new().write(true).create(true).truncate(true).open(&tmp)?;f.write_all(&bytes)?;f.sync_all()?;fs::rename(&tmp,&self.path)?;File::open(parent)?.sync_all()
    }
    pub fn load(&self)->io::Result<Option<PositionEconomicsState>>{
        if !self.path.exists(){return Ok(None)}let mut b=Vec::new();File::open(&self.path)?.read_to_end(&mut b)?;
        let s:PositionEconomicsState=serde_json::from_slice(&b).map_err(|e|io::Error::new(io::ErrorKind::InvalidData,e))?;
        s.validate().map_err(|_|io::Error::new(io::ErrorKind::InvalidData,"invalid position economics"))?;Ok(Some(s))
    }
}


#[cfg(test)]
mod r19_exit_economics_tests {
    use super::*;
    use std::time::{SystemTime,UNIX_EPOCH};
    fn a(v:&str)->u128{decimal_to_atoms(v).unwrap()}
    fn terminal_leg(venue:&str,side:&str,intent:&str,client:&str,qty:&str,px:&str,fee:Option<&str>)->LegFillAggregate{
        let mut x=LegFillAggregate::new(venue,"TEST",side,a(qty)).unwrap();
        x.add_order(intent,client,a(qty)).unwrap();
        x.order_mut(intent).unwrap().apply_fill(FillEvent{fill_id:format!("fill-{intent}"),price_atoms:a(px),qty_atoms:a(qty),
            fee_cost_quote_atoms:fee.map(|f|a(f) as i128),fee_currency:fee.map(|_|"USDT".into()),trade_time_ms:Some(10)},1).unwrap();
        x.order_mut(intent).unwrap().apply_cumulative_truth(a(qty),Some(a(px)),true,Some("filled"),Some(&format!("order-{intent}")),Some(10),1).unwrap();
        x
    }
    fn entry()->EntryEconomicsState{
        EntryEconomicsState{signal_gross_microbps:100_000_000,signal_expected_net_microbps:80_000_000,
            signal_spot_price_atoms:Some(a("100")),signal_futures_price_atoms:Some(a("101")),projected_cost_reserve_microbps:20_000_000,
            spot:terminal_leg("okx","buy","es","ces","10","100",Some("0.1")),
            futures:terminal_leg("binance","sell","ef","cef","10","101",Some("0.1")),
            timing:EntryTiming::default(),post_leg1_projected_gross_microbps:None,post_leg1_projected_net_microbps:None,
            locked_actual_gross_microbps:None,locked_actual_net_microbps:None,signal_to_leg1_slippage_microbps:None,
            signal_to_hedge_decay_microbps:None,execution_cost_vs_signal_microbps:None}
    }
    #[test]
    fn actual_exit_vwaps_and_trade_fees_produce_round_trip_pnl(){
        let e=entry();
        let x=ExitEconomicsState{spot:terminal_leg("okx","sell","xs","cxs","10","102",Some("0.1")),
            futures:terminal_leg("binance","buy","xf","cxf","10","99",Some("0.1"))};
        let c=x.finalize_against_entry(&e).unwrap();
        assert_eq!(atoms_to_string(c.actual_spot_exit_vwap_atoms),"102");
        assert_eq!(atoms_to_string(c.actual_futures_exit_vwap_atoms),"99");
        assert_eq!(signed_atoms_to_string(c.execution_gross_pnl_quote_atoms),"40");
        assert_eq!(signed_atoms_to_string(c.execution_pnl_after_trade_fees_quote_atoms.unwrap()),"39.6");
        assert!(c.trade_fees_complete);assert!(!c.funding_included);
    }
    #[test]
    fn incomplete_exit_fee_keeps_net_unknown_but_gross_known(){
        let e=entry();
        let x=ExitEconomicsState{spot:terminal_leg("okx","sell","xs","cxs","10","102",None),
            futures:terminal_leg("binance","buy","xf","cxf","10","99",Some("0.1"))};
        let c=x.finalize_against_entry(&e).unwrap();
        assert_eq!(signed_atoms_to_string(c.execution_gross_pnl_quote_atoms),"40");
        assert_eq!(c.execution_pnl_after_trade_fees_quote_atoms,None);assert!(!c.trade_fees_complete);
    }
    #[test]
    fn wrong_exit_quantity_or_nonterminal_truth_cannot_finalize(){
        let e=entry();
        let x=ExitEconomicsState{spot:terminal_leg("okx","sell","xs","cxs","9","102",Some("0.1")),
            futures:terminal_leg("binance","buy","xf","cxf","9","99",Some("0.1"))};
        assert_eq!(x.finalize_against_entry(&e),Err(EconError::InvalidState));
        let mut x=ExitEconomicsState{spot:terminal_leg("okx","sell","xs2","cxs2","10","102",Some("0.1")),
            futures:terminal_leg("binance","buy","xf2","cxf2","10","99",Some("0.1"))};
        x.futures.order_mut("xf2").unwrap().terminal=false;
        assert_eq!(x.finalize_against_entry(&e),Err(EconError::InvalidState));
    }
    #[test]
    fn position_economics_with_exit_is_durable_across_restart(){
        let state=PositionEconomicsState{entry:entry(),exit:Some(ExitEconomicsState{
            spot:terminal_leg("okx","sell","xs","cxs","10","102",Some("0.1")),
            futures:terminal_leg("binance","buy","xf","cxf","10","99",Some("0.1"))})};
        let root=std::env::temp_dir().join(format!("r19-pos-{}",SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()));
        let store=PositionEconomicsStore::open(root.join("position_economics.json")).unwrap();
        store.persist(&state).unwrap();let restored=store.load().unwrap().unwrap();assert_eq!(restored,state);
        assert!(restored.exit.as_ref().unwrap().finalize_against_entry(&restored.entry).unwrap().execution_pnl_after_trade_fees_quote_atoms.is_some());
        let _=fs::remove_dir_all(root);
    }
}
