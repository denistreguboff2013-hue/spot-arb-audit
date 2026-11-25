//! R1287 deterministic PAPER risk lifecycle model.
//! Simulated fills only: this module has no exchange order transport and is not wired to REAL.
use crate::Book;
use serde::{Deserialize,Serialize};
use std::collections::BTreeMap;

pub const FRESHNESS_LIMIT_NS:u64=800_000_000;
const MAX_FUTURE_SKEW_MS:i64=1_000;
const QTY_EPS:f64=1e-10;

#[derive(Clone,Copy,Debug,Eq,PartialEq,Ord,PartialOrd,Serialize,Deserialize)]
pub enum FillLeg { Spot, Futures }
#[derive(Clone,Copy,Debug,Eq,PartialEq,Serialize,Deserialize)]
pub enum FillSide { Buy, Sell, Short, Cover }
#[derive(Clone,Copy,Debug,Eq,PartialEq,Serialize,Deserialize)]
pub enum FillStatus { Partial, Complete, Unknown }
#[derive(Clone,Copy,Debug,Eq,PartialEq,Serialize,Deserialize)]
pub enum LifecycleState {
    Flat, EntryPending, PartiallyFilled, AwaitingHedge, Hedged,
    ExitPending, ExitPartiallyFilled, RecoveryRequired, Closed, FailedClosed,
}
#[derive(Clone,Copy,Debug,Eq,PartialEq,Serialize,Deserialize)]
pub enum ExitReason { StopLoss, MaxHold, HedgeTimeout, HotLost, ManualRecovery }
#[derive(Clone,Copy,Debug,Eq,PartialEq,Serialize,Deserialize)]
pub enum EventOutcome { Applied, Duplicate, Rejected, RecoveryRequired }

#[derive(Clone,Debug,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RiskConfig {
    pub hedge_timeout_ns:u64,
    pub stop_loss_bps:f64,
    pub max_hold_ns:u64,
    pub futures_contract_multiplier:f64,
    pub fee_bps:f64,
}
impl Default for RiskConfig {
    fn default()->Self{Self{hedge_timeout_ns:2_000_000_000,stop_loss_bps:-100.0,
        max_hold_ns:900_000_000_000,futures_contract_multiplier:1.0,fee_bps:0.0}}
}
#[derive(Clone,Debug,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketGate {
    pub spot_hot:bool,pub futures_hot:bool,
    pub spot_book_valid:bool,pub futures_book_valid:bool,
}
#[derive(Clone,Copy,Debug,Eq,PartialEq,Serialize,Deserialize)]
pub enum OrderStatus { New, Pending, PartiallyFilled, Filled, CancelPending, Cancelled, Rejected, Unknown }
impl OrderStatus { fn terminal(self)->bool { matches!(self,Self::Filled|Self::Cancelled|Self::Rejected) } }
#[derive(Clone,Debug,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimulatedOrder {
    pub order_id:String,pub leg:FillLeg,pub side:FillSide,pub original_quantity:f64,
    pub confirmed_filled_quantity:f64,pub remaining_quantity:f64,pub status:OrderStatus,
    pub execution_event_ids:Vec<String>,pub last_confirmed_change_ns:u64,
    pub last_request_ns:Option<u64>,pub uncertain:bool,
}
#[derive(Clone,Copy,Debug,Eq,PartialEq,Serialize,Deserialize)]
pub enum OrderEventKind { Accepted, CancelRequested, Cancelled, Rejected, Unknown }
#[derive(Clone,Debug,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrderEvent { pub event_id:String,pub order_id:String,pub kind:OrderEventKind,pub event_ns:u64,pub simulated:bool }
#[derive(Clone,Debug,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FillEvent {
    pub event_id:String,pub order_id:String,pub leg:FillLeg,pub side:FillSide,
    /// Spot: BASE units. Futures: contract units.
    pub quantity:f64,pub price:f64,pub contract_multiplier:f64,
    pub sequence:u64,pub event_ns:u64,pub status:FillStatus,
    /// Must remain true: this is a deterministic simulated event, never venue proof.
    pub simulated:bool,
}
#[derive(Clone,Debug,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PositionLedger {
    pub target_base:f64,
    pub spot_bought_base:f64,pub spot_sold_base:f64,
    pub futures_short_contracts:f64,pub futures_covered_contracts:f64,
    pub spot_buy_quote:f64,pub spot_sell_quote:f64,
    pub futures_short_quote:f64,pub futures_cover_quote:f64,
    pub fees_quote:f64,pub opened_ns:u64,pub hedge_deadline_ns:u64,
    pub exit_reason:Option<ExitReason>,
    pub unknown_execution_status:bool,pub valuation_unknown:bool,
}
impl PositionLedger {
    pub fn spot_exposure_base(&self)->f64{self.spot_bought_base-self.spot_sold_base}
    pub fn futures_short_exposure_base(&self,multiplier:f64)->f64{
        (self.futures_short_contracts-self.futures_covered_contracts)*multiplier
    }
    pub fn residual_base(&self,multiplier:f64)->f64{
        self.spot_exposure_base()-self.futures_short_exposure_base(multiplier)
    }
    pub fn realized_quote(&self)->f64{
        self.spot_sell_quote-self.spot_buy_quote+
        self.futures_short_quote-self.futures_cover_quote-self.fees_quote
    }
}
#[derive(Clone,Debug,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Lifecycle {
    pub config:RiskConfig,pub state:LifecycleState,
    pub ledger:Option<PositionLedger>,
    pub orders:BTreeMap<String,SimulatedOrder>,
    pub processed_events:BTreeMap<String,FillEvent>,
    pub processed_order_events:BTreeMap<String,OrderEvent>,
    #[serde(default)] pub order_census_complete:bool,
    pub last_sequence:BTreeMap<FillLeg,u64>,
    pub last_reason:Option<String>,
    pub last_transition_ns:Option<u64>,
}
#[derive(Clone,Debug,Eq,PartialEq)]
pub enum LifecycleError { InvalidConfig, InvalidMarket, MissingTimestamp, StaleExchange,
    StaleReceive, NotHot, InvalidFill, WrongFillSide, MultiplierMismatch, EventOrder,
    UnknownExposure, NotExitable }
#[derive(Clone,Debug,PartialEq)]
pub enum LifecycleAction {
    DenyEntry(&'static str),
    SimulatedEntryIntents{spot_buy_base:f64},
    SimulatedHedgeIntent{futures_short_contracts:f64,futures_cover_contracts:f64},
    NoAction,
    SimulatedRiskReducingExit{spot_sell_base:f64,futures_cover_contracts:f64,reason:ExitReason},
}
fn positive(v:f64)->bool{v.is_finite()&&v>0.0}
fn finite_nonnegative(v:f64)->bool{v.is_finite()&&v>=0.0}
fn book_shape(book:&Book,bids:bool)->bool{
    let side=if bids{&book.bids}else{&book.asks};
    if side.is_empty(){return false}
    if side.iter().any(|(p,q)|!positive(*p)||!positive(*q)){return false}
    if bids { !side.windows(2).any(|w|w[0].0<=w[1].0) }
    else { !side.windows(2).any(|w|w[0].0>=w[1].0) }
}
fn market_ok(spot:&Book,fut:&Book,gate:&MarketGate,now_ns:u64,now_wall_ms:i64)->Result<(),LifecycleError>{
    if !gate.spot_hot||!gate.futures_hot{return Err(LifecycleError::NotHot)}
    if !gate.spot_book_valid||!gate.futures_book_valid||
       !book_shape(spot,true)||!book_shape(spot,false)||
       !book_shape(fut,true)||!book_shape(fut,false)||
       spot.bids[0].0>=spot.asks[0].0||fut.bids[0].0>=fut.asks[0].0{
        return Err(LifecycleError::InvalidMarket)
    }
    for book in [spot,fut]{
        let ts=book.exchange_ts_ms.ok_or(LifecycleError::MissingTimestamp)?;
        let age=now_wall_ms.checked_sub(ts).ok_or(LifecycleError::StaleExchange)?;
        if age < -MAX_FUTURE_SKEW_MS||age>800{return Err(LifecycleError::StaleExchange)}
        if now_ns<book.received_ns||now_ns-book.received_ns>FRESHNESS_LIMIT_NS{
            return Err(LifecycleError::StaleReceive)
        }
    }
    Ok(())
}
impl Lifecycle {
    pub fn new(config:RiskConfig)->Result<Self,LifecycleError>{
        if config.hedge_timeout_ns==0||config.max_hold_ns==0||
           !config.stop_loss_bps.is_finite()||config.stop_loss_bps>=0.0||
           !positive(config.futures_contract_multiplier)||
           !finite_nonnegative(config.fee_bps){return Err(LifecycleError::InvalidConfig)}
        Ok(Self{config,state:LifecycleState::Flat,ledger:None,
            orders:BTreeMap::new(),processed_events:BTreeMap::new(),
            processed_order_events:BTreeMap::new(),order_census_complete:true,
            last_sequence:BTreeMap::new(),
            last_reason:None,last_transition_ns:None})
    }
    /// Restart is conservative: any non-flat, non-closed inventory requires reconciliation.
    pub fn cold_restart(&mut self,now_ns:u64){
        if !matches!(self.state,LifecycleState::Flat|LifecycleState::Closed){
            if matches!(self.state,LifecycleState::EntryPending|LifecycleState::PartiallyFilled|
                LifecycleState::AwaitingHedge|LifecycleState::ExitPending|LifecycleState::ExitPartiallyFilled){
                if let Some(l)=self.ledger.as_mut(){l.unknown_execution_status=true}
            }
            for order in self.orders.values_mut(){
                if !order.status.terminal(){order.status=OrderStatus::Unknown;order.uncertain=true;}
            }
            self.order_census_complete=false;
            self.state=LifecycleState::RecoveryRequired;
            self.last_reason=Some("cold_restart_requires_reconciliation".into());
            self.last_transition_ns=Some(now_ns);
        }
    }
    pub fn begin_entry(&mut self,spot:&Book,fut:&Book,gate:&MarketGate,
        target_base:f64,now_ns:u64,now_wall_ms:i64)->Result<LifecycleAction,LifecycleError>{
        if self.state!=LifecycleState::Flat{return Ok(LifecycleAction::DenyEntry("not_flat"))}
        if !positive(target_base){return Err(LifecycleError::InvalidFill)}
        market_ok(spot,fut,gate,now_ns,now_wall_ms)?;
        crate::native_paper_market_execution_review::buy_base(&spot.asks,target_base)
            .map_err(|_|LifecycleError::InvalidMarket)?;
        crate::native_paper_market_execution_review::sell_base(&fut.bids,target_base)
            .map_err(|_|LifecycleError::InvalidMarket)?;
        let deadline=now_ns.checked_add(self.config.hedge_timeout_ns)
            .ok_or(LifecycleError::InvalidConfig)?;
        self.ledger=Some(PositionLedger{target_base,spot_bought_base:0.,spot_sold_base:0.,
            futures_short_contracts:0.,futures_covered_contracts:0.,spot_buy_quote:0.,
            spot_sell_quote:0.,futures_short_quote:0.,futures_cover_quote:0.,fees_quote:0.,
            opened_ns:now_ns,hedge_deadline_ns:deadline,exit_reason:None,
            unknown_execution_status:false,valuation_unknown:false});
        self.state=LifecycleState::EntryPending;
        self.last_reason=Some("fresh_hot_entry_authorized_simulation_only".into());
        self.last_transition_ns=Some(now_ns);
        Ok(LifecycleAction::SimulatedEntryIntents{spot_buy_base:target_base})
    }
    pub fn register_simulated_order(&mut self,order_id:&str,leg:FillLeg,side:FillSide,
        original_quantity:f64,now_ns:u64)->Result<(),LifecycleError>{
        if order_id.is_empty()||!positive(original_quantity)||self.orders.contains_key(order_id){
            return Err(LifecycleError::InvalidFill)
        }
        self.orders.insert(order_id.into(),SimulatedOrder{order_id:order_id.into(),leg,side,
            original_quantity,confirmed_filled_quantity:0.,remaining_quantity:original_quantity,
            status:OrderStatus::New,execution_event_ids:Vec::new(),last_confirmed_change_ns:now_ns,
            last_request_ns:None,uncertain:false});
        Ok(())
    }
    pub fn request_cancel(&mut self,order_id:&str,now_ns:u64)->Result<(),LifecycleError>{
        let o=self.orders.get_mut(order_id).ok_or(LifecycleError::EventOrder)?;
        if o.status.terminal(){return Err(LifecycleError::EventOrder)}
        o.status=OrderStatus::CancelPending;o.last_request_ns=Some(now_ns);
        Ok(())
    }
    pub fn apply_order_event(&mut self,event:OrderEvent)->Result<EventOutcome,LifecycleError>{
        if !event.simulated||event.event_id.is_empty(){return Err(LifecycleError::InvalidFill)}
        if let Some(old)=self.processed_order_events.get(&event.event_id){
            if old.order_id==event.order_id&&old.kind==event.kind&&old.event_ns==event.event_ns{return Ok(EventOutcome::Duplicate)}
            self.unknown_recovery(event.event_ns,"order_event_id_collision");return Ok(EventOutcome::RecoveryRequired)
        }
        let Some(order)=self.orders.get_mut(&event.order_id) else {
            self.unknown_recovery(event.event_ns,"event_for_unknown_order");return Ok(EventOutcome::RecoveryRequired)
        };
        if event.event_ns<order.last_confirmed_change_ns {
            order.uncertain=true;order.status=OrderStatus::Unknown;
            self.unknown_recovery(event.event_ns,"out_of_order_order_event");
            self.processed_order_events.insert(event.event_id.clone(),event);return Ok(EventOutcome::RecoveryRequired)
        }
        if order.status.terminal() && !matches!(event.kind,OrderEventKind::Unknown){
            order.uncertain=true;self.unknown_recovery(event.event_ns,"event_after_terminal_order_status");
            self.processed_order_events.insert(event.event_id.clone(),event);return Ok(EventOutcome::RecoveryRequired)
        }
        let is_unknown=event.kind==OrderEventKind::Unknown;
        match event.kind {
            OrderEventKind::Accepted=>order.status=OrderStatus::Pending,
            OrderEventKind::CancelRequested=>order.status=OrderStatus::CancelPending,
            OrderEventKind::Cancelled=>{order.status=OrderStatus::Cancelled;order.remaining_quantity=order.original_quantity-order.confirmed_filled_quantity;},
            OrderEventKind::Rejected=>{order.status=OrderStatus::Rejected;order.remaining_quantity=order.original_quantity-order.confirmed_filled_quantity;},
            OrderEventKind::Unknown=>{order.status=OrderStatus::Unknown;order.uncertain=true;}
        }
        order.last_confirmed_change_ns=event.event_ns;
        let event_ns=event.event_ns;
        self.processed_order_events.insert(event.event_id.clone(),event);
        if is_unknown { self.unknown_recovery(event_ns,"unknown_order_status"); }
        Ok(if self.state==LifecycleState::RecoveryRequired{EventOutcome::RecoveryRequired}else{EventOutcome::Applied})
    }
    pub fn apply_fill(&mut self,event:FillEvent)->Result<EventOutcome,LifecycleError>{
        if !event.simulated{return Err(LifecycleError::InvalidFill)}
        if event.event_id.is_empty()||!positive(event.quantity)||!positive(event.price)||
           !positive(event.contract_multiplier){return Err(LifecycleError::InvalidFill)}
        if let Some(old)=self.processed_events.get(&event.event_id){
            if old.order_id==event.order_id&&old.leg==event.leg&&old.side==event.side&&old.quantity.to_bits()==event.quantity.to_bits()
               &&old.price.to_bits()==event.price.to_bits()&&
               old.contract_multiplier.to_bits()==event.contract_multiplier.to_bits()&&
               old.sequence==event.sequence&&old.event_ns==event.event_ns&&
               old.status==event.status&&old.simulated==event.simulated{
                return Ok(EventOutcome::Duplicate)
            }
            self.unknown_recovery(event.event_ns,"execution_id_collision");
            return Ok(EventOutcome::RecoveryRequired)
        }
        if !self.orders.contains_key(&event.order_id) {
            if event.status!=FillStatus::Complete { self.unknown_recovery(event.event_ns,"partial_fill_without_registered_order"); return Ok(EventOutcome::RecoveryRequired); }
            self.orders.insert(event.order_id.clone(),SimulatedOrder{order_id:event.order_id.clone(),leg:event.leg,side:event.side,
                original_quantity:event.quantity,confirmed_filled_quantity:0.,remaining_quantity:event.quantity,
                status:OrderStatus::Pending,execution_event_ids:Vec::new(),last_confirmed_change_ns:event.event_ns,
                last_request_ns:None,uncertain:false});
        }
        if let Some(o)=self.orders.get(&event.order_id) {
            if o.leg!=event.leg||o.side!=event.side||o.status.terminal() {
                self.unknown_recovery(event.event_ns,"fill_conflicts_with_order_state");
                if let Some(o)=self.orders.get_mut(&event.order_id){o.uncertain=true;o.status=OrderStatus::Unknown;}
                self.processed_events.insert(event.event_id.clone(),event.clone());return Ok(EventOutcome::RecoveryRequired)
            }
            if o.confirmed_filled_quantity+event.quantity>o.original_quantity+QTY_EPS {
                self.unknown_recovery(event.event_ns,"fill_exceeds_order_quantity");return Ok(EventOutcome::RecoveryRequired)
            }
        }
        if (event.leg==FillLeg::Futures)&&
            event.contract_multiplier.to_bits()!=self.config.futures_contract_multiplier.to_bits(){
            self.unknown_recovery(event.event_ns,"contract_multiplier_mismatch");
            return Err(LifecycleError::MultiplierMismatch)
        }
        if self.ledger.is_none(){return Err(LifecycleError::EventOrder)}
        if matches!(self.state,LifecycleState::Closed|LifecycleState::FailedClosed){
            if let Some(l)=self.ledger.as_mut(){l.unknown_execution_status=true}
            self.require_recovery(event.event_ns,"execution_arrived_after_terminal_state");
            return Ok(EventOutcome::RecoveryRequired)
        }
        if self.ledger.as_ref().is_some_and(|l|event.event_ns<l.opened_ns){
            self.unknown_recovery(event.event_ns,"execution_before_entry_time");
            return Ok(EventOutcome::RecoveryRequired)
        }
        if self.last_sequence.get(&event.leg).is_some_and(|n|event.sequence<=*n){
            self.unknown_recovery(event.event_ns,"out_of_order_execution");
            return Ok(EventOutcome::RecoveryRequired)
        }
        if event.status==FillStatus::Unknown{
            self.processed_events.insert(event.event_id.clone(),event.clone());
            self.last_sequence.insert(event.leg,event.sequence);
            self.unknown_recovery(event.event_ns,"unknown_execution_status");
            return Ok(EventOutcome::RecoveryRequired)
        }
        let leg_side_ok=matches!((event.leg,event.side),
            (FillLeg::Spot,FillSide::Buy|FillSide::Sell)|
            (FillLeg::Futures,FillSide::Short|FillSide::Cover));
        if !leg_side_ok{return Err(LifecycleError::WrongFillSide)}
        let new_order_filled=self.orders.get(&event.order_id).map(|o|o.confirmed_filled_quantity+event.quantity).unwrap_or(event.quantity);
        let completion_mismatch=self.orders.get(&event.order_id).is_some_and(|o|match event.status{
            FillStatus::Complete=>new_order_filled+QTY_EPS<o.original_quantity,
            FillStatus::Partial=>new_order_filled+QTY_EPS>=o.original_quantity,
            FillStatus::Unknown=>false,
        });
        let was_flat=self.ledger.as_ref().is_some_and(|l|
            l.spot_exposure_base().abs()<=QTY_EPS&&
            l.futures_short_exposure_base(self.config.futures_contract_multiplier).abs()<=QTY_EPS);
        let l=self.ledger.as_mut().unwrap();
        if was_flat{
            l.opened_ns=event.event_ns;
            l.hedge_deadline_ns=event.event_ns.saturating_add(self.config.hedge_timeout_ns);
        }
        let base_qty=if event.leg==FillLeg::Futures{
            event.quantity*event.contract_multiplier
        }else{event.quantity};
        if !positive(base_qty){return Err(LifecycleError::InvalidFill)}
        match (event.leg,event.side){
            (FillLeg::Spot,FillSide::Buy)=>{
                if self.state==LifecycleState::ExitPending||self.state==LifecycleState::ExitPartiallyFilled{
                    l.unknown_execution_status=true;
                    self.require_recovery(event.event_ns,"risk_increasing_fill_during_exit");
                    return Ok(EventOutcome::RecoveryRequired)
                }
                if l.spot_bought_base+base_qty>l.target_base+QTY_EPS{return Err(LifecycleError::InvalidFill)}
                l.spot_bought_base+=base_qty;l.spot_buy_quote+=base_qty*event.price;
                l.fees_quote+=base_qty*event.price*self.config.fee_bps/10_000.;
            }
            (FillLeg::Spot,FillSide::Sell)=>{
                if base_qty>l.spot_exposure_base()+QTY_EPS{return Err(LifecycleError::InvalidFill)}
                l.spot_sold_base+=base_qty;l.spot_sell_quote+=base_qty*event.price;
                l.fees_quote+=base_qty*event.price*self.config.fee_bps/10_000.;
            }
            (FillLeg::Futures,FillSide::Short)=>{
                if self.state==LifecycleState::ExitPending||self.state==LifecycleState::ExitPartiallyFilled{
                    l.unknown_execution_status=true;
                    self.require_recovery(event.event_ns,"risk_increasing_fill_during_exit");
                    return Ok(EventOutcome::RecoveryRequired)
                }
                if (l.futures_short_contracts+event.quantity)*event.contract_multiplier>l.target_base+QTY_EPS{return Err(LifecycleError::InvalidFill)}
                l.futures_short_contracts+=event.quantity;
                l.futures_short_quote+=base_qty*event.price;
                l.fees_quote+=base_qty*event.price*self.config.fee_bps/10_000.;
            }
            (FillLeg::Futures,FillSide::Cover)=>{
                if event.quantity>l.futures_short_contracts-l.futures_covered_contracts+QTY_EPS{
                    return Err(LifecycleError::InvalidFill)
                }
                l.futures_covered_contracts+=event.quantity;
                l.futures_cover_quote+=base_qty*event.price;
                l.fees_quote+=base_qty*event.price*self.config.fee_bps/10_000.;
            }
            _=>return Err(LifecycleError::WrongFillSide)
        }
        self.processed_events.insert(event.event_id.clone(),event.clone());
        if let Some(o)=self.orders.get_mut(&event.order_id){
            o.confirmed_filled_quantity+=event.quantity;
            o.remaining_quantity=(o.original_quantity-o.confirmed_filled_quantity).max(0.);
            o.execution_event_ids.push(event.event_id.clone());
            o.last_confirmed_change_ns=event.event_ns;
            o.status=if completion_mismatch{OrderStatus::Unknown}else if o.confirmed_filled_quantity+QTY_EPS>=o.original_quantity{OrderStatus::Filled}else{OrderStatus::PartiallyFilled};
            if event.status==FillStatus::Unknown||completion_mismatch{o.uncertain=true;}
        }
        self.last_sequence.insert(event.leg,event.sequence);
        if completion_mismatch{self.unknown_recovery(event.event_ns,"fill_status_quantity_conflict");return Ok(EventOutcome::RecoveryRequired)}
        self.recompute_state(event.event_ns);
        Ok(EventOutcome::Applied)
    }
    fn recompute_state(&mut self,now_ns:u64){
        let Some(l)=self.ledger.as_ref() else{return};
        if l.unknown_execution_status{self.state=LifecycleState::RecoveryRequired;return}
        let spot=l.spot_exposure_base();let fut=l.futures_short_exposure_base(self.config.futures_contract_multiplier);
        if spot.abs()<=QTY_EPS&&fut.abs()<=QTY_EPS{
            let orders_settled=self.order_census_complete&&self.orders.values().all(|o|o.status.terminal()&&!o.uncertain);
            if orders_settled && (l.exit_reason.is_some()||matches!(self.state,LifecycleState::RecoveryRequired|
                LifecycleState::ExitPending|LifecycleState::ExitPartiallyFilled)){self.state=LifecycleState::Closed}
            else if self.state==LifecycleState::EntryPending{self.state=LifecycleState::EntryPending}
            else{self.state=LifecycleState::PartiallyFilled}
            self.last_transition_ns=Some(now_ns);return
        }
        if self.state==LifecycleState::RecoveryRequired{
            self.last_transition_ns=Some(now_ns);return
        }
        if matches!(self.state,LifecycleState::ExitPending|LifecycleState::ExitPartiallyFilled){
            self.state=LifecycleState::ExitPartiallyFilled;
        }else if (spot-fut).abs()<=QTY_EPS && spot>QTY_EPS && fut>QTY_EPS{
            self.state=LifecycleState::Hedged;
        }else if spot>QTY_EPS&&fut<=QTY_EPS{
            self.state=if spot+QTY_EPS<l.target_base{LifecycleState::PartiallyFilled}
                else{LifecycleState::AwaitingHedge};
        }else{self.state=LifecycleState::PartiallyFilled}
        self.last_transition_ns=Some(now_ns);
    }
    fn require_recovery(&mut self,now_ns:u64,reason:&str){
        self.state=LifecycleState::RecoveryRequired;
        self.last_reason=Some(reason.into());self.last_transition_ns=Some(now_ns);
    }
    pub(crate) fn isolated_recovery_incident(&mut self,now_ns:u64,reason:&str){self.unknown_recovery(now_ns,reason)}
    fn unknown_recovery(&mut self,now_ns:u64,reason:&str){
        self.order_census_complete=false;
        if let Some(l)=self.ledger.as_mut(){l.unknown_execution_status=true}
        self.require_recovery(now_ns,reason);
    }
    pub fn on_clock(&mut self,now_ns:u64,gate:&MarketGate)->LifecycleAction{
        let Some(l)=self.ledger.as_ref() else{return LifecycleAction::NoAction};
        if !gate.spot_hot||!gate.futures_hot{
            self.require_recovery(now_ns,"hot_lost_open_exposure_preserved");
            return LifecycleAction::DenyEntry("hot_lost_recovery_required")
        }
        let unhedged=l.residual_base(self.config.futures_contract_multiplier).abs()>QTY_EPS;
        if unhedged&&now_ns>=l.hedge_deadline_ns&&
            !matches!(self.state,LifecycleState::ExitPending|LifecycleState::ExitPartiallyFilled){
            if let Some(l)=self.ledger.as_mut(){l.exit_reason=Some(ExitReason::HedgeTimeout)}
            self.require_recovery(now_ns,"hedge_timeout");
            return LifecycleAction::DenyEntry("hedge_timeout_requires_fresh_market_recovery")
        }
        if matches!(self.state,LifecycleState::Hedged|LifecycleState::ExitPending|
            LifecycleState::ExitPartiallyFilled|LifecycleState::RecoveryRequired)&&now_ns.saturating_sub(l.opened_ns)>=self.config.max_hold_ns{
            if let Some(l)=self.ledger.as_mut(){l.exit_reason=Some(ExitReason::MaxHold)}
            self.require_recovery(now_ns,"max_hold_requires_fresh_market_exit");
            return LifecycleAction::DenyEntry("max_hold_requires_fresh_market_exit")
        }
        LifecycleAction::NoAction
    }
    /// Authorize only a fresh, HOT, risk-reducing hedge adjustment. Fills still require
    /// separate simulated execution events; the returned request is not a fill.
    pub fn authorize_hedge(&mut self,spot:&Book,fut:&Book,gate:&MarketGate,
        now_ns:u64,now_wall_ms:i64)->Result<LifecycleAction,LifecycleError>{
        if !matches!(self.state,LifecycleState::AwaitingHedge|LifecycleState::PartiallyFilled|
            LifecycleState::RecoveryRequired){return Ok(LifecycleAction::NoAction)}
        if let Err(e)=market_ok(spot,fut,gate,now_ns,now_wall_ms){
            self.require_recovery(now_ns,"hedge_authorization_market_untrusted");return Err(e)
        }
        let l=self.ledger.as_ref().ok_or(LifecycleError::UnknownExposure)?;
        if l.unknown_execution_status{return Err(LifecycleError::UnknownExposure)}
        let residual=l.residual_base(self.config.futures_contract_multiplier);
        let action=if residual>QTY_EPS{
            LifecycleAction::SimulatedHedgeIntent{futures_short_contracts:
                residual/self.config.futures_contract_multiplier,futures_cover_contracts:0.}
        }else if residual< -QTY_EPS{
            LifecycleAction::SimulatedHedgeIntent{futures_short_contracts:0.,
                futures_cover_contracts:(-residual)/self.config.futures_contract_multiplier}
        }else{return Ok(LifecycleAction::NoAction)};
        Ok(action)
    }
    pub fn evaluate_risk(&mut self,spot:&Book,fut:&Book,gate:&MarketGate,
        now_ns:u64,now_wall_ms:i64)->Result<LifecycleAction,LifecycleError>{
        if !matches!(self.state,LifecycleState::Hedged|LifecycleState::RecoveryRequired){
            return Ok(LifecycleAction::NoAction)
        }
        if let Err(e)=market_ok(spot,fut,gate,now_ns,now_wall_ms){
            self.require_recovery(now_ns,"risk_mark_untrusted_exposure_preserved");
            return Err(e)
        }
        let l=self.ledger.as_ref().ok_or(LifecycleError::UnknownExposure)?;
        if l.unknown_execution_status||l.valuation_unknown{return Ok(LifecycleAction::NoAction)}
        let sq=l.spot_exposure_base();let fq=l.futures_short_exposure_base(self.config.futures_contract_multiplier);
        if (sq-fq).abs()>QTY_EPS{return Err(LifecycleError::UnknownExposure)}
        let spot_value=spot.bids.iter().map(|(p,q)|p*q).sum::<f64>();
        let fut_cover=fut.asks.iter().map(|(p,q)|p*q).sum::<f64>();
        if !positive(spot_value)||!positive(fut_cover){return Err(LifecycleError::InvalidMarket)}
        let spot_exit=crate::native_paper_market_execution_review::sell_base(&spot.bids,sq)
            .map_err(|_|LifecycleError::InvalidMarket)?;
        let fut_exit=crate::native_paper_market_execution_review::buy_base(&fut.asks,fq)
            .map_err(|_|LifecycleError::InvalidMarket)?;
        let mark=spot_exit-l.spot_buy_quote+l.futures_short_quote-fut_exit-l.fees_quote;
        let basis=l.spot_buy_quote+l.futures_short_quote;
        let pnl_bps=mark/basis.max(f64::MIN_POSITIVE)*10_000.;
        if pnl_bps<=self.config.stop_loss_bps{
            return Ok(self.begin_exit(ExitReason::StopLoss,now_ns))
        }
        if now_ns.saturating_sub(l.opened_ns)>=self.config.max_hold_ns{
            return Ok(self.begin_exit(ExitReason::MaxHold,now_ns))
        }
        Ok(LifecycleAction::NoAction)
    }
    /// Reconciliation input must come from an explicit authoritative review. It supplies
    /// confirmed quantities only; missing prices stay unknown and cannot produce PnL.
    pub fn reconcile_confirmed_exposure(&mut self,spot_base:f64,futures_contracts:f64,now_ns:u64)->Result<(),LifecycleError>{
        if self.state!=LifecycleState::RecoveryRequired||!finite_nonnegative(spot_base)||
           !finite_nonnegative(futures_contracts){return Err(LifecycleError::UnknownExposure)}
        let l=self.ledger.as_mut().ok_or(LifecycleError::UnknownExposure)?;
        let fut_base=futures_contracts*self.config.futures_contract_multiplier;
        if !finite_nonnegative(fut_base)||spot_base>l.target_base+QTY_EPS||fut_base>l.target_base+QTY_EPS{
            return Err(LifecycleError::UnknownExposure)
        }
        l.spot_bought_base=l.spot_sold_base+spot_base;
        l.futures_short_contracts=l.futures_covered_contracts+futures_contracts;
        l.unknown_execution_status=false;l.valuation_unknown=true;
        l.exit_reason=Some(l.exit_reason.unwrap_or(ExitReason::ManualRecovery));
        let orders_settled=self.order_census_complete&&self.orders.values().all(|o|o.status.terminal()&&!o.uncertain);
        self.state=if spot_base<=QTY_EPS&&fut_base<=QTY_EPS&&orders_settled{LifecycleState::Closed}else{LifecycleState::RecoveryRequired};
        self.last_reason=Some("confirmed_quantities_reconciled_values_unknown".into());
        self.last_transition_ns=Some(now_ns);Ok(())
    }
    /// Caller asserts that the modeled execution adapter has reconciled its complete
    /// order inventory. This is not durable persistence and must never be inferred from
    /// an empty in-memory map after restart.
    pub fn confirm_order_census_complete(&mut self,now_ns:u64)->Result<(),LifecycleError>{
        if self.orders.values().any(|o|!o.status.terminal()||o.uncertain){return Err(LifecycleError::UnknownExposure)}
        self.order_census_complete=true;
        if let Some(l)=self.ledger.as_ref(){
            let zero=l.spot_exposure_base().abs()<=QTY_EPS&&
                l.futures_short_exposure_base(self.config.futures_contract_multiplier).abs()<=QTY_EPS;
            let all_terminal=self.orders.values().all(|o|o.status.terminal()&&!o.uncertain);
            if zero&&all_terminal&&!l.unknown_execution_status&&
                (l.exit_reason.is_some()||self.state==LifecycleState::RecoveryRequired||
                 self.state==LifecycleState::ExitPending||self.state==LifecycleState::ExitPartiallyFilled){
                self.state=LifecycleState::Closed;
            }
        }
        self.last_reason=Some("complete_order_census_confirmed_by_simulated_adapter".into());
        self.last_transition_ns=Some(now_ns);Ok(())
    }
    pub fn request_recovery_exit(&mut self,spot:&Book,fut:&Book,gate:&MarketGate,
        now_ns:u64,now_wall_ms:i64)->Result<LifecycleAction,LifecycleError>{
        if self.state!=LifecycleState::RecoveryRequired{return Ok(LifecycleAction::NoAction)}
        market_ok(spot,fut,gate,now_ns,now_wall_ms)?;
        let l=self.ledger.as_ref().ok_or(LifecycleError::UnknownExposure)?;
        if l.unknown_execution_status{return Err(LifecycleError::UnknownExposure)}
        if l.spot_exposure_base().abs()<=QTY_EPS&&
           l.futures_short_exposure_base(self.config.futures_contract_multiplier).abs()<=QTY_EPS&&
           self.order_census_complete&&self.orders.values().all(|o|o.status.terminal()&&!o.uncertain){
            self.state=LifecycleState::Closed;
            self.last_transition_ns=Some(now_ns);
            return Ok(LifecycleAction::NoAction)
        }
        if !self.order_census_complete||!self.orders.values().all(|o|o.status.terminal()&&!o.uncertain){
            self.require_recovery(now_ns,"open_or_unknown_orders_require_terminal_reconciliation");
            return Ok(LifecycleAction::DenyEntry("open_or_unknown_orders"))
        }
        let reason=l.exit_reason.unwrap_or(ExitReason::ManualRecovery);
        self.state=LifecycleState::ExitPending;
        self.last_transition_ns=Some(now_ns);
        Ok(self.risk_reducing_action(reason))
    }
    pub fn request_exit(&mut self,spot:&Book,fut:&Book,gate:&MarketGate,reason:ExitReason,
        now_ns:u64,now_wall_ms:i64)->Result<LifecycleAction,LifecycleError>{
        if let Err(e)=market_ok(spot,fut,gate,now_ns,now_wall_ms){
            self.require_recovery(now_ns,"exit_authorization_market_untrusted");return Err(e)
        }
        if self.state!=LifecycleState::Hedged{return Err(LifecycleError::NotExitable)}
        if self.ledger.as_ref().is_some_and(|l|l.unknown_execution_status){
            self.require_recovery(now_ns,"unknown_exposure_blocks_exit_estimate");return Err(LifecycleError::UnknownExposure)
        }
        Ok(self.begin_exit(reason,now_ns))
    }
    fn begin_exit(&mut self,reason:ExitReason,now_ns:u64)->LifecycleAction{
        if self.ledger.as_ref().is_some_and(|l|l.unknown_execution_status){
            self.require_recovery(now_ns,"unknown_exposure_blocks_exit_estimate");
            return LifecycleAction::DenyEntry("unknown_exposure_reconciliation_required")
        }
        if let Some(l)=self.ledger.as_mut(){l.exit_reason=Some(reason)}
        self.state=LifecycleState::ExitPending;self.last_reason=Some(format!("exit:{reason:?}"));
        self.last_transition_ns=Some(now_ns);
        self.risk_reducing_action(reason)
    }
    fn risk_reducing_action(&self,reason:ExitReason)->LifecycleAction{
        let Some(l)=self.ledger.as_ref() else{return LifecycleAction::NoAction};
        if l.unknown_execution_status{return LifecycleAction::DenyEntry("unknown_exposure_reconciliation_required")}
        LifecycleAction::SimulatedRiskReducingExit{
            spot_sell_base:l.spot_exposure_base().max(0.),
            futures_cover_contracts:(l.futures_short_contracts-l.futures_covered_contracts).max(0.),
            reason}
    }
    pub fn fail_exit(&mut self,now_ns:u64,reason:&str){
        self.require_recovery(now_ns,reason);
    }
    pub fn exposure_known(&self)->bool{
        self.ledger.as_ref().map_or(true,|l|!l.unknown_execution_status)
    }
    pub fn can_open_risk(&self)->bool{self.state==LifecycleState::Flat}
    pub fn outstanding_exposure(&self)->Option<(f64,f64)>{
        let l=self.ledger.as_ref()?;
        if l.unknown_execution_status{return None}
        Some((l.spot_exposure_base(),
            l.futures_short_exposure_base(self.config.futures_contract_multiplier)))
    }
}

#[cfg(test)]
mod tests{
 use super::*;
 fn book(ns:u64,wall:i64,bid:f64,ask:f64)->Book{
  Book{bids:vec![(bid,10.)],asks:vec![(ask,10.)],received_ns:ns,
   received_wall_ms:wall,exchange_ts_ms:Some(wall),changed_ns:ns,epoch:1,sequence:Some(ns as i64)}
 }
 fn gate()->MarketGate{MarketGate{spot_hot:true,futures_hot:true,spot_book_valid:true,futures_book_valid:true}}
 fn machine()->Lifecycle{Lifecycle::new(RiskConfig{hedge_timeout_ns:20,stop_loss_bps:-10.,
  max_hold_ns:100,futures_contract_multiplier:0.1,fee_bps:0.}).unwrap()}
 fn start()->Lifecycle{
  let mut m=machine();let s=book(100,100_000,99.,100.);let f=book(100,100_000,101.,102.);
  assert_eq!(m.begin_entry(&s,&f,&gate(),1.,100,100_000).unwrap(),
   LifecycleAction::SimulatedEntryIntents{spot_buy_base:1.});m
 }
 fn ev(id:&str,leg:FillLeg,side:FillSide,qty:f64,price:f64,seq:u64,ns:u64)->FillEvent{
  FillEvent{event_id:id.into(),order_id:format!("order-{id}"),leg,side,quantity:qty,price,
   contract_multiplier:if leg==FillLeg::Futures{0.1}else{1.},
   sequence:seq,event_ns:ns,status:FillStatus::Complete,simulated:true}
 }
 fn spot_fill(m:&mut Lifecycle,qty:f64,seq:u64,ns:u64)->EventOutcome{
  m.apply_fill(ev(&format!("sb{seq}"),FillLeg::Spot,FillSide::Buy,qty,100.,seq,ns)).unwrap()
 }
 fn future_fill(m:&mut Lifecycle,contracts:f64,seq:u64,ns:u64)->EventOutcome{
  m.apply_fill(ev(&format!("fs{seq}"),FillLeg::Futures,FillSide::Short,contracts,101.,seq,ns)).unwrap()
 }
 fn hedged()->Lifecycle{
  let mut m=start();spot_fill(&mut m,1.,1,101);future_fill(&mut m,10.,1,102);
  assert_eq!(m.state,LifecycleState::Hedged);m
 }
 fn exit_fills(m:&mut Lifecycle){
  m.apply_fill(ev("sell1",FillLeg::Spot,FillSide::Sell,1.,102.,2,120)).unwrap();
  m.apply_fill(ev("cover1",FillLeg::Futures,FillSide::Cover,10.,100.,2,121)).unwrap();
 }
 #[test]fn normal_entry_and_exit_closes_only_after_both_confirmed_fills(){
  let mut m=hedged();assert_eq!(m.begin_exit(ExitReason::ManualRecovery,115),
   LifecycleAction::SimulatedRiskReducingExit{spot_sell_base:1.,futures_cover_contracts:10.,reason:ExitReason::ManualRecovery});
  m.apply_fill(ev("sell",FillLeg::Spot,FillSide::Sell,1.,102.,2,116)).unwrap();
  assert_ne!(m.state,LifecycleState::Closed);
  m.apply_fill(ev("cover",FillLeg::Futures,FillSide::Cover,10.,100.,2,117)).unwrap();
  assert_eq!(m.state,LifecycleState::Closed);assert_eq!(m.outstanding_exposure(),Some((0.,0.)));
 }
 #[test]fn spot_full_futures_zero_is_awaiting_hedge_with_open_delta(){
  let mut m=start();spot_fill(&mut m,1.,1,101);
  assert_eq!(m.state,LifecycleState::AwaitingHedge);assert_eq!(m.outstanding_exposure(),Some((1.,0.)));
 }
 #[test]fn spot_partial_futures_zero_is_partially_filled(){
  let mut m=start();spot_fill(&mut m,0.5,1,101);
  assert_eq!(m.state,LifecycleState::PartiallyFilled);assert_eq!(m.outstanding_exposure(),Some((0.5,0.)));
 }
 #[test]fn spot_full_futures_half_retains_unhedged_residual(){
  let mut m=start();spot_fill(&mut m,1.,1,101);future_fill(&mut m,5.,1,102);
  assert_eq!(m.state,LifecycleState::PartiallyFilled);assert_eq!(m.outstanding_exposure(),Some((1.,0.5)));
 }
 #[test]fn matched_half_fills_are_hedged_only_to_confirmed_quantity(){
  let mut m=start();spot_fill(&mut m,0.5,1,101);future_fill(&mut m,5.,1,102);
  assert_eq!(m.state,LifecycleState::Hedged);assert_eq!(m.outstanding_exposure(),Some((0.5,0.5)));
 }
 #[test]fn futures_full_spot_zero_is_not_closed(){
  let mut m=start();future_fill(&mut m,10.,1,101);
  assert_eq!(m.state,LifecycleState::PartiallyFilled);assert_eq!(m.outstanding_exposure(),Some((0.,1.)));
 }
 #[test]fn hedge_timeout_marks_recovery_but_never_fabricates_exit_fill(){
  let mut m=start();spot_fill(&mut m,1.,1,101);
  assert_eq!(m.on_clock(121,&gate()),LifecycleAction::DenyEntry("hedge_timeout_requires_fresh_market_recovery"));
  assert_eq!(m.state,LifecycleState::RecoveryRequired);assert_eq!(m.outstanding_exposure(),Some((1.,0.)));
 }
 #[test]fn late_hedge_fill_after_timeout_is_applied_once_and_balances_position(){
  let mut m=start();spot_fill(&mut m,1.,1,101);m.on_clock(121,&gate());
  assert_eq!(future_fill(&mut m,10.,1,122),EventOutcome::Applied);
  assert_eq!(m.state,LifecycleState::RecoveryRequired);assert_eq!(m.outstanding_exposure(),Some((1.,1.)));
 }
 #[test]fn duplicate_execution_id_never_doubles_fill(){
  let mut m=start();let e=ev("once",FillLeg::Spot,FillSide::Buy,0.5,100.,1,101);
  assert_eq!(m.apply_fill(e.clone()),Ok(EventOutcome::Applied));
  assert_eq!(m.apply_fill(e),Ok(EventOutcome::Duplicate));
  assert_eq!(m.ledger.as_ref().unwrap().spot_exposure_base(),0.5);
 }
 #[test]fn out_of_order_execution_makes_exposure_unknown_and_recovery_only(){
  let mut m=start();spot_fill(&mut m,0.5,2,102);
  assert_eq!(m.apply_fill(ev("late-old",FillLeg::Spot,FillSide::Buy,0.5,100.,1,103)),
   Ok(EventOutcome::RecoveryRequired));
  assert_eq!(m.state,LifecycleState::RecoveryRequired);assert_eq!(m.outstanding_exposure(),None);
  assert!(!m.can_open_risk());
 }
 #[test]fn hot_loss_before_entry_denies_entry(){
  let mut m=machine();let s=book(100,100_000,99.,100.);let f=book(100,100_000,101.,102.);
  let mut g=gate();g.futures_hot=false;
  assert_eq!(m.begin_entry(&s,&f,&g,1.,100,100_000),Err(LifecycleError::NotHot));
  assert_eq!(m.state,LifecycleState::Flat);
 }
 #[test]fn hot_loss_during_hedge_preserves_exposure_and_blocks_entry(){
  let mut m=start();spot_fill(&mut m,0.5,1,101);let mut g=gate();g.spot_hot=false;
  assert!(matches!(m.on_clock(105,&g),LifecycleAction::DenyEntry(_)));
  assert_eq!(m.state,LifecycleState::RecoveryRequired);assert_eq!(m.outstanding_exposure(),Some((0.5,0.)));
  assert!(!m.can_open_risk());
 }
 #[test]fn hot_loss_during_exit_does_not_erase_position(){
  let mut m=hedged();m.begin_exit(ExitReason::ManualRecovery,110);
  let mut g=gate();g.futures_hot=false;m.on_clock(111,&g);
  assert_eq!(m.state,LifecycleState::RecoveryRequired);assert_eq!(m.outstanding_exposure(),Some((1.,1.)));
 }
 #[test]fn missing_exchange_timestamp_blocks_entry(){
  let mut m=machine();let s=book(100,100_000,99.,100.);let mut f=book(100,100_000,101.,102.);
  f.exchange_ts_ms=None;
  assert_eq!(m.begin_entry(&s,&f,&gate(),1.,100,100_000),Err(LifecycleError::MissingTimestamp));
  assert_eq!(m.state,LifecycleState::Flat);
 }
 #[test]fn stale_exchange_timestamp_during_risk_moves_to_recovery(){
  let mut m=hedged();let s=book(110,100_000,99.,100.);let f=book(110,100_000,101.,102.);
  assert_eq!(m.evaluate_risk(&s,&f,&gate(),110,100_801),Err(LifecycleError::StaleExchange));
  assert_eq!(m.state,LifecycleState::RecoveryRequired);assert_eq!(m.outstanding_exposure(),Some((1.,1.)));
 }
 #[test]fn stop_loss_uses_fresh_executable_books_and_starts_simulated_exit(){
  let mut m=hedged();let s=book(110,100_000,98.,99.);let f=book(110,100_000,102.,103.);
  assert!(matches!(m.evaluate_risk(&s,&f,&gate(),110,100_000).unwrap(),
   LifecycleAction::SimulatedRiskReducingExit{reason:ExitReason::StopLoss,..}));
  assert_eq!(m.state,LifecycleState::ExitPending);assert_eq!(m.outstanding_exposure(),Some((1.,1.)));
 }
 #[test]fn max_hold_is_separate_and_requires_fresh_market_before_exit_intent(){
  let mut m=hedged();
  assert_eq!(m.on_clock(201,&gate()),LifecycleAction::DenyEntry("max_hold_requires_fresh_market_exit"));
  assert_eq!(m.state,LifecycleState::RecoveryRequired);
  let s=book(201,100_000,99.,100.);let f=book(201,100_000,101.,102.);
  assert!(matches!(m.request_recovery_exit(&s,&f,&gate(),201,100_000).unwrap(),
   LifecycleAction::SimulatedRiskReducingExit{reason:ExitReason::MaxHold,..}));
 }
 #[test]fn negative_mark_does_not_hold_forever_past_max_hold(){
  let mut m=hedged();let s=book(150,100_000,98.,99.);let f=book(150,100_000,102.,103.);
  assert!(matches!(m.evaluate_risk(&s,&f,&gate(),150,100_000).unwrap(),
    LifecycleAction::SimulatedRiskReducingExit{reason:ExitReason::StopLoss,..}));
  assert_eq!(m.state,LifecycleState::ExitPending);
 }
 #[test]fn partial_exit_keeps_residual_and_never_reports_closed(){
  let mut m=hedged();m.begin_exit(ExitReason::ManualRecovery,110);
  m.apply_fill(ev("s-part",FillLeg::Spot,FillSide::Sell,0.5,102.,2,111)).unwrap();
  assert_eq!(m.state,LifecycleState::ExitPartiallyFilled);
  assert_eq!(m.outstanding_exposure(),Some((0.5,1.)));
  assert_ne!(m.state,LifecycleState::Closed);
 }
 #[test]fn failed_exit_moves_open_position_to_recovery(){
  let mut m=hedged();m.begin_exit(ExitReason::StopLoss,110);m.fail_exit(111,"simulated_exit_rejected");
  assert_eq!(m.state,LifecycleState::RecoveryRequired);assert_eq!(m.outstanding_exposure(),Some((1.,1.)));
 }
 #[test]fn restart_with_open_exposure_requires_recovery(){
  let mut m=hedged();m.cold_restart(110);
  assert_eq!(m.state,LifecycleState::RecoveryRequired);assert_eq!(m.outstanding_exposure(),Some((1.,1.)));
 }
 #[test]fn unknown_execution_status_is_not_treated_as_zero(){
  let mut m=start();let mut e=ev("unknown",FillLeg::Spot,FillSide::Buy,1.,100.,1,101);
  e.status=FillStatus::Unknown;
  assert_eq!(m.apply_fill(e),Ok(EventOutcome::RecoveryRequired));
  assert_eq!(m.outstanding_exposure(),None);assert!(!m.exposure_known());
 }
 #[test]fn unknown_status_can_recover_only_from_explicit_quantity_reconciliation(){
  let mut m=start();let mut e=ev("unknown",FillLeg::Spot,FillSide::Buy,1.,100.,1,101);e.status=FillStatus::Unknown;
  m.apply_fill(e).unwrap();assert!(m.reconcile_confirmed_exposure(1.,10.,105).is_ok());
  assert_eq!(m.state,LifecycleState::RecoveryRequired);assert_eq!(m.outstanding_exposure(),Some((1.,1.)));
  assert!(m.ledger.as_ref().unwrap().valuation_unknown);
 }
 #[test]fn repeated_recovery_event_does_not_reapply(){
  let mut m=start();let e=ev("once",FillLeg::Spot,FillSide::Buy,0.5,100.,1,101);
  m.apply_fill(e.clone()).unwrap();m.cold_restart(102);
  assert_eq!(m.apply_fill(e),Ok(EventOutcome::Duplicate));
  assert_eq!(m.ledger.as_ref().unwrap().spot_exposure_base(),0.5);
 }
 #[test]fn contract_multiplier_mismatch_fails_closed(){
  let mut m=start();let mut e=ev("bad-mult",FillLeg::Futures,FillSide::Short,10.,101.,1,102);
  e.contract_multiplier=1.;
  assert_eq!(m.apply_fill(e),Err(LifecycleError::MultiplierMismatch));
  assert_eq!(m.state,LifecycleState::RecoveryRequired);assert_eq!(m.outstanding_exposure(),None);
 }
 #[test]fn no_false_closed_if_one_leg_remains(){
  let mut m=hedged();m.begin_exit(ExitReason::ManualRecovery,110);
  m.apply_fill(ev("sell",FillLeg::Spot,FillSide::Sell,1.,102.,2,111)).unwrap();
  assert_eq!(m.outstanding_exposure(),Some((0.,1.)));assert_ne!(m.state,LifecycleState::Closed);
 }
 #[test]fn recovery_never_emits_entry_or_risk_increase(){
  let mut m=start();spot_fill(&mut m,1.,1,101);m.on_clock(121,&gate());
  assert!(!m.can_open_risk());
  let action=m.request_recovery_exit(&book(121,100_000,99.,100.),
    &book(121,100_000,101.,102.),&gate(),121,100_000).unwrap();
  if let LifecycleAction::SimulatedRiskReducingExit{spot_sell_base,futures_cover_contracts,..}=action {
    assert!((spot_sell_base-1.0).abs()<f64::EPSILON);
    assert!(futures_cover_contracts.abs()<f64::EPSILON);
  } else { panic!("expected a simulated risk-reducing exit"); }
 }
 #[test]fn recovery_exit_needs_fresh_hot_valid_books(){
  let mut m=hedged();m.fail_exit(110,"injected");
  let mut g=gate();g.spot_hot=false;
  assert_eq!(m.request_recovery_exit(&book(111,100_000,99.,100.),&book(111,100_000,101.,102.),&g,111,100_000),
   Err(LifecycleError::NotHot));
  assert_eq!(m.state,LifecycleState::RecoveryRequired);
 }
 #[test]fn receive_age_and_invalid_book_fail_closed(){
  let mut m=machine();let stale=book(100,100_000,99.,100.);let f=book(100,100_000,101.,102.);
  assert_eq!(m.begin_entry(&stale,&f,&gate(),1.,FRESHNESS_LIMIT_NS+101,100_000),Err(LifecycleError::StaleReceive));
  let mut crossed=book(100,100_000,101.,100.);
  assert_eq!(m.begin_entry(&crossed,&f,&gate(),1.,100,100_000),Err(LifecycleError::InvalidMarket));
 }
 #[test]fn hedge_timeout_is_not_the_800ms_freshness_limit(){
  let mut m=machine();assert_eq!(m.config.hedge_timeout_ns,20);
  assert_eq!(FRESHNESS_LIMIT_NS,800_000_000);assert_ne!(m.config.hedge_timeout_ns,FRESHNESS_LIMIT_NS);
 }
 #[test]fn cancel_request_is_not_cancel_confirmation_and_fill_may_arrive(){
  let mut m=start();m.register_simulated_order("o1",FillLeg::Spot,FillSide::Buy,1.,100).unwrap();
  m.apply_order_event(OrderEvent{event_id:"ack1".into(),order_id:"o1".into(),kind:OrderEventKind::Accepted,event_ns:101,simulated:true}).unwrap();
  m.request_cancel("o1",102).unwrap();assert_eq!(m.orders["o1"].status,OrderStatus::CancelPending);
  assert_eq!(m.orders["o1"].last_confirmed_change_ns,101);assert_eq!(m.orders["o1"].last_request_ns,Some(102));
  let mut e=ev("fill-after-cancel",FillLeg::Spot,FillSide::Buy,0.5,100.,1,103);e.order_id="o1".into();e.status=FillStatus::Partial;
  assert_eq!(m.apply_fill(e).unwrap(),EventOutcome::Applied);
  assert_eq!(m.orders["o1"].confirmed_filled_quantity,0.5);assert_eq!(m.orders["o1"].remaining_quantity,0.5);
  m.apply_order_event(OrderEvent{event_id:"cancelled".into(),order_id:"o1".into(),kind:OrderEventKind::Cancelled,event_ns:104,simulated:true}).unwrap();
  assert_eq!(m.orders["o1"].status,OrderStatus::Cancelled);assert_eq!(m.orders["o1"].confirmed_filled_quantity,0.5);
  assert_eq!(m.outstanding_exposure(),Some((0.5,0.)));
 }
 #[test]fn fill_after_confirmed_cancel_enters_recovery_without_double_count(){
  let mut m=start();m.register_simulated_order("o2",FillLeg::Spot,FillSide::Buy,1.,100).unwrap();
  m.request_cancel("o2",101).unwrap();m.apply_order_event(OrderEvent{event_id:"c2".into(),order_id:"o2".into(),kind:OrderEventKind::Cancelled,event_ns:102,simulated:true}).unwrap();
  let mut e=ev("late",FillLeg::Spot,FillSide::Buy,0.2,100.,1,103);e.order_id="o2".into();e.status=FillStatus::Partial;
  assert_eq!(m.apply_fill(e).unwrap(),EventOutcome::RecoveryRequired);
  assert_eq!(m.state,LifecycleState::RecoveryRequired);assert_eq!(m.outstanding_exposure(),None);
 }
 #[test]fn zero_position_with_open_order_cannot_be_closed_by_reconciliation(){
  let mut m=start();m.register_simulated_order("open",FillLeg::Spot,FillSide::Buy,1.,100).unwrap();
  m.state=LifecycleState::RecoveryRequired;
  m.reconcile_confirmed_exposure(0.,0.,101).unwrap();
  assert_eq!(m.state,LifecycleState::RecoveryRequired);
 }
 #[test]fn restart_with_cancel_pending_or_unknown_order_requires_unknown_recovery(){
  let mut m=start();m.register_simulated_order("cancel",FillLeg::Spot,FillSide::Buy,1.,100).unwrap();
  m.request_cancel("cancel",101).unwrap();m.cold_restart(102);
  assert_eq!(m.state,LifecycleState::RecoveryRequired);assert_eq!(m.orders["cancel"].status,OrderStatus::Unknown);
  assert!(m.orders["cancel"].uncertain);assert!(!m.can_open_risk());
 }
 #[test]fn lost_cancel_ack_does_not_make_order_terminal(){
  let mut m=start();m.register_simulated_order("lost",FillLeg::Spot,FillSide::Buy,1.,100).unwrap();
  m.request_cancel("lost",101).unwrap();m.on_clock(10_000,&gate());
  assert_eq!(m.orders["lost"].status,OrderStatus::CancelPending);
  assert_ne!(m.state,LifecycleState::Closed);
 }
 #[test]fn conflicting_event_after_terminal_order_is_incident(){
  let mut m=start();m.register_simulated_order("term",FillLeg::Spot,FillSide::Buy,1.,100).unwrap();
  m.apply_order_event(OrderEvent{event_id:"rej".into(),order_id:"term".into(),kind:OrderEventKind::Rejected,event_ns:101,simulated:true}).unwrap();
  let out=m.apply_order_event(OrderEvent{event_id:"acc-late".into(),order_id:"term".into(),kind:OrderEventKind::Accepted,event_ns:102,simulated:true}).unwrap();
  assert_eq!(out,EventOutcome::RecoveryRequired);assert_eq!(m.orders["term"].status,OrderStatus::Rejected);
  assert!(m.orders["term"].uncertain);assert_eq!(m.state,LifecycleState::RecoveryRequired);
 }
 #[test]fn repeated_execution_id_remains_idempotent_per_order(){
  let mut m=start();m.register_simulated_order("repeat",FillLeg::Spot,FillSide::Buy,1.,100).unwrap();
  let mut e=ev("same",FillLeg::Spot,FillSide::Buy,0.5,100.,1,101);e.order_id="repeat".into();e.status=FillStatus::Partial;
  assert_eq!(m.apply_fill(e.clone()).unwrap(),EventOutcome::Applied);
  assert_eq!(m.apply_fill(e).unwrap(),EventOutcome::Duplicate);
  assert_eq!(m.orders["repeat"].confirmed_filled_quantity,0.5);
 }
 #[test]fn out_of_order_order_event_is_preserved_as_recovery_incident(){
  let mut m=start();m.register_simulated_order("ooo",FillLeg::Spot,FillSide::Buy,1.,100).unwrap();
  m.apply_order_event(OrderEvent{event_id:"newer".into(),order_id:"ooo".into(),kind:OrderEventKind::Accepted,event_ns:110,simulated:true}).unwrap();
  assert_eq!(m.apply_order_event(OrderEvent{event_id:"older".into(),order_id:"ooo".into(),kind:OrderEventKind::CancelRequested,event_ns:109,simulated:true}).unwrap(),EventOutcome::RecoveryRequired);
  assert_eq!(m.orders["ooo"].status,OrderStatus::Unknown);assert!(m.orders["ooo"].uncertain);
 }
 #[test]fn recovery_exit_waits_for_all_orders_terminal(){
  let mut m=hedged();m.fail_exit(110,"recovery");
  m.register_simulated_order("orphan",FillLeg::Spot,FillSide::Buy,0.1,111).unwrap();
  let s=book(112,100_000,99.,100.);let f=book(112,100_000,101.,102.);
  assert_eq!(m.request_recovery_exit(&s,&f,&gate(),112,100_000).unwrap(),LifecycleAction::DenyEntry("open_or_unknown_orders"));
  assert_eq!(m.state,LifecycleState::RecoveryRequired);
 }
 #[test]fn empty_order_map_after_restart_is_not_proof_of_cancellation(){
  let mut m=start();m.state=LifecycleState::RecoveryRequired;m.cold_restart(101);
  m.reconcile_confirmed_exposure(0.,0.,102).unwrap();
  assert!(!m.order_census_complete);assert_eq!(m.state,LifecycleState::RecoveryRequired);
  m.confirm_order_census_complete(103).unwrap();assert_eq!(m.state,LifecycleState::Closed);
 }
 #[test]fn model_fill_events_are_explicitly_simulated(){
  let m=hedged();assert!(m.state==LifecycleState::Hedged);
  assert!(m.processed_events.values().all(|e|e.simulated));
 }
}
