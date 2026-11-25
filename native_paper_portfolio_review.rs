//! Research-only public-book paper portfolio, 10 concurrent / 500 per Yekaterinburg day.
//! It is NOT a fill engine and never sends/cancels/queries private orders.
//! Native config of actual venue min/steps is required before any paper entry.
use crate::{Book,Venue};
use crate::decision::{Decision,RouteSpec};
use crate::native_paper_market_execution_review::{
    PaperMachine,PaperAssumptions,PaperPhase,PaperEvent,PaperError,buy_base,sell_base,
};
use serde::{Serialize,Deserialize};
use std::collections::{BTreeMap,BTreeSet};

pub const MAX_CONCURRENT_PAPER:usize=10;
pub const MAX_DAILY_PAPER:usize=500;
const LEGACY_REENTRY_COOLDOWN_NS:u64=120_000_000_000;
const COMPILED_MIN_LIFETIME_NS:u64=15_000_000_000;
const COMPILED_EXIT_SPREAD_BPS:f64=0.5;
const COMPILED_NET_TARGET_BPS:f64=12.0;
fn default_reentry_cooldown_ns()->u64{LEGACY_REENTRY_COOLDOWN_NS}
fn default_min_lifetime_ns()->u64{COMPILED_MIN_LIFETIME_NS}
fn default_exit_spread_bps()->f64{COMPILED_EXIT_SPREAD_BPS}
fn default_net_target_bps()->f64{COMPILED_NET_TARGET_BPS}
fn default_daily_quota()->u64{MAX_DAILY_PAPER as u64}
fn default_concurrent_quota()->usize{MAX_CONCURRENT_PAPER}
fn cold_boot_needs_reconciliation()->bool{true}
const YEK_OFFSET_MS:i64=5*3600*1000;
const DAY_MS:i64=86_400_000;

#[derive(Debug,Clone)]
pub enum PortfolioError { BadClock, InvalidMetadata, Book(PaperError) }
#[derive(Debug,Default,Serialize,Deserialize,Clone)]
#[serde(deny_unknown_fields)]
pub struct PortfolioCounters{
 pub entries:u64,pub futures_hedges:u64,pub futures_covers:u64,pub closed:u64,
 pub modeled_realized_usdt:f64,pub closed_wins:u64,pub closed_losses:u64,
 pub held_negative_marks:u64,pub held_positive_marks:u64,
 pub denied_metadata:u64,pub denied_liquidity:u64,pub denied_stale:u64,
 pub denied_entry_guard:u64,pub denied_daily_limit:u64,pub denied_concurrent_limit:u64,
 pub unknown_unhedged:u64,
}
#[derive(Debug,Clone,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaperPortfolio {
    pub positions:BTreeMap<String,PaperMachine>,
    pub closed_bases:BTreeSet<String>,
    #[serde(default)]
    pub closed_at_monotonic_ns:BTreeMap<String,u64>,
    #[serde(default="default_reentry_cooldown_ns")]
    pub reentry_cooldown_ns:u64,
    #[serde(default="default_reentry_cooldown_ns")]
    pub scan_cooldown_ns:u64,
    #[serde(default="default_min_lifetime_ns")]
    pub min_lifetime_ns:u64,
    #[serde(default="default_exit_spread_bps")]
    pub exit_spread_bps:f64,
    #[serde(default="default_net_target_bps")]
    pub net_target_bps:f64,
    #[serde(default="default_daily_quota")]
    pub max_daily_entries:u64,
    #[serde(default="default_concurrent_quota")]
    pub max_concurrent_positions:usize,
    #[serde(default)]
    pub scan_last_mark_ns:BTreeMap<String,u64>,
    #[serde(skip,default="cold_boot_needs_reconciliation")]
    pub cold_boot_unknown:bool,
    pub stats:PortfolioCounters,
    pub assumptions:PaperAssumptions,
    pub day_yek:i64,
    pub started_wall_ms:i64,
}
fn valid_temporal(min_lifetime_ns:u64,exit_bps:f64,cooldown_ns:u64,target_bps:f64)->bool{
 (1_000_000_000..=21_600_000_000_000).contains(&min_lifetime_ns)
  &&exit_bps.is_finite()&&(0.0..=100.0).contains(&exit_bps)
  &&(1_000_000_000..=86_400_000_000_000).contains(&cooldown_ns)
  &&target_bps.is_finite()&&(0.0..=1000.0).contains(&target_bps)
}
fn temporal_exit_eligible(min_lifetime_ns:u64,exit_bps:f64,
    elapsed_ns:u64,spread_bps:f64)->bool{
 (1_000_000_000..=21_600_000_000_000).contains(&min_lifetime_ns)
  &&exit_bps.is_finite()&&(0.0..=100.0).contains(&exit_bps)
  &&spread_bps.is_finite()
  &&elapsed_ns>=min_lifetime_ns&&spread_bps<=exit_bps
}
fn positive(s:&str)->Option<f64>{
 let v=s.parse::<f64>().ok()?;
 if v.is_finite()&&v>0.0{Some(v)}else{None}
}
fn nonnegative(s:&str)->Option<f64>{
 let n=s.parse::<f64>().ok()?;
 if n.is_finite()&&n>=0.0{Some(n)}else{None}
}
fn decimal_step(qty:f64,step:f64)->bool{
 if qty<=0.0||!qty.is_finite()||!step.is_finite()||step<=0.0{return false}
 let n=qty/step;
 // allow only tiny floating-point serialization error, not size rounding.
 n>=1.0-1e-8&&(n-n.round()).abs()<1e-7
}
fn source_valid(qty:f64,route:&RouteSpec,spot_notional:f64,short_notional:f64)->bool{
 if route.metadata_state!="SIZE_METADATA_READY"{return false}
 if !positive(&route.buy.step).is_some_and(|v|decimal_step(qty,v))||
    !positive(&route.sell.step).is_some_and(|v|decimal_step(qty,v)){return false}
 for (side,notional) in [(&route.buy,spot_notional),(&route.sell,short_notional)]{
  let Some(min_amt)=positive(&side.min_amount) else{return false};
  let Some(min_cost)=nonnegative(&side.min_cost) else{return false};
  if qty+qty*1e-10<min_amt || notional+notional*1e-10<min_cost{return false}
  if side.max_amount.as_ref().is_some_and(|s|
    positive(s).map_or(true,|max|qty>max+qty*1e-10)){return false}
 }
 true
}
/// R129 / Astra P1-06: bind a proposed research opportunity to its
/// actual symbol/route before the PAPER entry accounting path. This is a
/// local shape and identity guard, not an authenticated execution decision.
fn paper_entry_decision_matches(base:&str,d:&Decision,route:&RouteSpec)->bool{
 d.reason=="EXECUTABLE_OPPORTUNITY"
  &&!base.is_empty()&&d.base==base
  &&d.route==route.label&&d.buy_exchange==route.buy_exchange
  &&d.sell_exchange==route.sell_exchange
  &&d.metadata_state=="SIZE_METADATA_READY"
  &&route.metadata_state=="SIZE_METADATA_READY"
  &&d.quantity.is_some_and(|q|q.is_finite()&&q>0.0)
  &&d.cost_bps.is_finite()&&d.cost_bps>=0.0
  &&d.required_net_bps.is_finite()&&d.required_net_bps>=0.0
  &&d.risk_cap_usdt.is_finite()&&d.risk_cap_usdt>0.0
  &&d.estimated_net_bps.is_some_and(|v|v.is_finite()
     &&v+1e-6>=d.required_net_bps)
  &&d.spot_notional_usdt.is_some_and(|v|v.is_finite()&&v>0.0)
  &&d.futures_notional_usdt.is_some_and(|v|v.is_finite()&&v>0.0)
}
fn day(ms:i64)->Result<i64,PortfolioError>{
 if ms<=0{return Err(PortfolioError::BadClock)}
 Ok((ms+YEK_OFFSET_MS).div_euclid(DAY_MS))
}
impl PaperPortfolio{
 pub fn new(now_ms:i64)->Result<Self,PortfolioError>{
    Ok(Self{positions:BTreeMap::new(),closed_bases:BTreeSet::new(),
        closed_at_monotonic_ns:BTreeMap::new(),
        reentry_cooldown_ns:LEGACY_REENTRY_COOLDOWN_NS,
        scan_cooldown_ns:LEGACY_REENTRY_COOLDOWN_NS,
        min_lifetime_ns:COMPILED_MIN_LIFETIME_NS,
        exit_spread_bps:COMPILED_EXIT_SPREAD_BPS,
        net_target_bps:COMPILED_NET_TARGET_BPS,
        max_daily_entries:MAX_DAILY_PAPER as u64,
        max_concurrent_positions:MAX_CONCURRENT_PAPER,
        scan_last_mark_ns:BTreeMap::new(),cold_boot_unknown:false,
        stats:PortfolioCounters::default(),
        assumptions:PaperAssumptions::default(),day_yek:day(now_ms)?,
        started_wall_ms:now_ms})
 }
 /// R review only: apply optional YAML temporal knobs to a FRESH
 /// research portfolio. Existing or cold restored portfolios are immutable.
 pub fn apply_public_paper_temporal_policy_review(
    &mut self,min_lifetime_ns:u64,exit_spread_bps:f64,reentry_cooldown_ns:u64,
    net_target_bps:f64
 )->Result<(),PortfolioError>{
    if self.cold_boot_unknown||!self.positions.is_empty()||
       self.stats.entries!=0||!self.closed_bases.is_empty()||
       !self.scan_last_mark_ns.is_empty()||
       !valid_temporal(min_lifetime_ns,exit_spread_bps,reentry_cooldown_ns,net_target_bps){
       return Err(PortfolioError::InvalidMetadata)
    }
    self.min_lifetime_ns=min_lifetime_ns;
    self.exit_spread_bps=exit_spread_bps;
    self.reentry_cooldown_ns=reentry_cooldown_ns;
    self.net_target_bps=net_target_bps;
    Ok(())
 }
 /// Restore is strictly compare-only; NEVER rewrite an already sealed
 /// virtual position's timing policy under a different YAML revision.
 pub fn verify_public_paper_temporal_policy_review(
    &self,min_lifetime_ns:u64,exit_spread_bps:f64,reentry_cooldown_ns:u64,
    net_target_bps:f64
 )->Result<(),PortfolioError>{
    if !valid_temporal(min_lifetime_ns,exit_spread_bps,reentry_cooldown_ns,net_target_bps)||
       self.net_target_bps.to_bits()!=net_target_bps.to_bits()||
       self.min_lifetime_ns!=min_lifetime_ns||
       self.exit_spread_bps.to_bits()!=exit_spread_bps.to_bits()||
       self.reentry_cooldown_ns!=reentry_cooldown_ns{
       return Err(PortfolioError::InvalidMetadata)
    }
    Ok(())
 }
 /// A3: apply source PAPER quotas only before any modeled activity.
 /// This is a research-model limit, not supervisor policy or REAL permit.
 pub fn apply_source_paper_quotas_review(
   &mut self,daily:u64,concurrent:usize
 )->Result<(),PortfolioError>{
   if !(1..=10_000).contains(&daily)||!(1..=10).contains(&concurrent)
     ||self.cold_boot_unknown||!self.positions.is_empty()
     ||self.stats.entries!=0||!self.closed_bases.is_empty()
     ||!self.scan_last_mark_ns.is_empty(){
     return Err(PortfolioError::InvalidMetadata)
   }
   self.max_daily_entries=daily;
   self.max_concurrent_positions=concurrent;
   Ok(())
 }
 /// Cold restarts may compare quotas but may not rewrite prior session policy.
 pub fn verify_source_paper_quotas_review(
   &self,daily:u64,concurrent:usize
 )->Result<(),PortfolioError>{
   if !(1..=10_000).contains(&daily)||!(1..=10).contains(&concurrent)
     ||self.max_daily_entries!=daily||self.max_concurrent_positions!=concurrent{
      return Err(PortfolioError::InvalidMetadata)
   }
   Ok(())
 }
 pub fn temporal_exit_eligible_review(&self,elapsed_ns:u64,spread_bps:f64)->bool{
    temporal_exit_eligible(self.min_lifetime_ns,self.exit_spread_bps,elapsed_ns,spread_bps)
 }
 pub fn open_count(&self)->usize{self.positions.len()}
 pub fn day_check(&mut self,now_ms:i64)->Result<(),PortfolioError>{
    let current=day(now_ms)?;
    if current<self.day_yek{return Err(PortfolioError::BadClock)}
    if current!=self.day_yek{
       // Do not silently roll to the new day with existing positions and
       // reset trade allowance; requires a new supervised test session.
       return Err(PortfolioError::BadClock)
    }
    Ok(())
 }
 pub fn on_frame(&mut self,
    base:&str,venue:Venue,now_ns:u64,now_wall_ms:i64,
    spot:Option<&Book>,fut:Option<&Book>,
    decision:Option<&Decision>,route:Option<&RouteSpec>
 )->Result<(),PortfolioError>{
    self.on_frame_with_checked_exit_policy(base,venue,now_ns,now_wall_ms,
      spot,fut,decision,route,None)
 }
 pub(crate) fn on_frame_with_checked_exit_policy(&mut self,
    base:&str,venue:Venue,now_ns:u64,now_wall_ms:i64,
    spot:Option<&Book>,fut:Option<&Book>,
    decision:Option<&Decision>,route:Option<&RouteSpec>,
    _exit:Option<(u64,f64)>
 )->Result<(),PortfolioError>{
    if _exit.is_some_and(|(life,bps)|!temporal_exit_eligible(life,bps,life,bps)){
       return Err(PortfolioError::InvalidMetadata)
    }
    if !valid_temporal(self.min_lifetime_ns,self.exit_spread_bps,
       self.reentry_cooldown_ns,self.net_target_bps){
       return Err(PortfolioError::InvalidMetadata)
    }
    self.day_check(now_wall_ms)?;
    let Some(spot)=spot else{return Ok(())};
    let Some(fut)=fut else{return Ok(())};
    if self.positions.contains_key(base){
        let mut complete=None;
        let mut signal=PaperEvent::NoAction;
        let machine=self.positions.get_mut(base).unwrap();
        match &machine.phase{
            PaperPhase::AwaitingHedge(_) if venue==Venue::BinanceFutures=>{
                match machine.observe_future_short(base,fut,now_ns){
                    Ok(e)=>signal=e,
                    Err(PaperError::InsufficientDepth)=>self.stats.denied_liquidity+=1,
                    Err(PaperError::StaleBook)=>self.stats.denied_stale+=1,
                    Err(e)=>return Err(PortfolioError::Book(e))
                }
            }
            PaperPhase::Hedged(pos)=>{
                // Use monotonic clock delta, NOT a conversion to wall time.
                let elapsed_ns=now_ns.saturating_sub(pos.spot_buy_ns);
                let spread=fut.asks.first().and_then(|(a,_)|
                    spot.bids.first().map(|(b,_)| (a/b-1.0)*10_000.0));
                let (life,close_bps)=_exit.unwrap_or((self.min_lifetime_ns,self.exit_spread_bps));
                let eligible=spread.is_some_and(|s|temporal_exit_eligible(
                    life,close_bps,elapsed_ns,s));
                // Deliberately no synthetic waiting.
                machine.config.take_profit_quote=if eligible{
                    Some(machine.config.quote_budget*self.net_target_bps/10_000.0)
                }else{None};
                match machine.observe_hedged_exit(base,spot,fut,now_ns){
                    Ok((e,m))=>{
                        signal=e;
                        if let Some(value)=m{
                            if value>0.0{self.stats.held_positive_marks+=1}
                            else{self.stats.held_negative_marks+=1}
                        }
                    }
                    Err(PaperError::StaleBook)=>self.stats.denied_stale+=1,
                    Err(PaperError::InsufficientDepth)=>self.stats.denied_liquidity+=1,
                    Err(e)=>return Err(PortfolioError::Book(e))
                }
            }
            PaperPhase::AwaitingSpotExit{..} if venue==Venue::OkxSpot=>{
                match machine.observe_spot_exit(base,spot,now_ns){
                    Ok(e)=>signal=e,
                    Err(PaperError::StaleBook)=>self.stats.denied_stale+=1,
                    Err(PaperError::InsufficientDepth)=>self.stats.denied_liquidity+=1,
                    Err(e)=>return Err(PortfolioError::Book(e))
                }
                if let PaperPhase::Closed(ref close)=machine.phase{
                   complete=Some(close.modeled_net_quote)
                }
            }
            _=>{}
        }
        match signal{
            PaperEvent::FutureShort=>self.stats.futures_hedges+=1,
            PaperEvent::TakeProfitFuturesCover=>self.stats.futures_covers+=1,
            _=>{}
        }
        if let Some(net)=complete{
            self.stats.closed+=1;
            self.stats.modeled_realized_usdt+=net;
            if net>0.0{self.stats.closed_wins+=1}
            else{self.stats.closed_losses+=1}
            self.positions.remove(base);
            self.closed_bases.insert(base.into());
            self.closed_at_monotonic_ns.insert(base.into(),now_ns);
        }
        return Ok(())
    }
    if decision.map(|d|d.reason.as_str())!=Some("EXECUTABLE_OPPORTUNITY"){
        return Ok(())
    }
    // R129: never convert another symbol's (or another route's) positive
    // decision into a new PAPER open. Existing virtual closes above must
    // remain operable even if a new entry decision is absent or corrupt.
    let (Some(proposed),Some(proposed_route))=(decision,route) else {
        self.stats.denied_metadata+=1;return Ok(())
    };
    if !paper_entry_decision_matches(base,proposed,proposed_route){
        self.stats.denied_entry_guard+=1;return Ok(())
    }
    // Cold-deserialized monotonic timestamps are foreign to this boot.
    // Existing positions above still progress; only NEW entries are denied.
    if self.cold_boot_unknown{
        self.stats.denied_entry_guard+=1;return Ok(())
    }
    // Separate scanner cooldown tracks the last examined entry attempt.
    if self.scan_last_mark_ns.get(base).is_some_and(|last|
      now_ns.checked_sub(*last)
        .map_or(true,|n|n<self.scan_cooldown_ns)){
        self.stats.denied_entry_guard+=1;return Ok(())
    }
    // In-memory same-boot CLOCK_MONOTONIC Python cooldown equivalent.
    // Missing/rollback timestamps deny; never fabricate a closed time.
    if self.closed_bases.contains(base){
        let allowed=self.closed_at_monotonic_ns.get(base)
          .and_then(|at|now_ns.checked_sub(*at))
          .is_some_and(|elapsed|elapsed>=self.reentry_cooldown_ns);
        if !allowed{
            self.stats.denied_entry_guard+=1;return Ok(())
        }
    }
    // Reject corrupted or untrusted restored quotas, never enlarge limits.
    if !(1..=10_000).contains(&self.max_daily_entries)
      ||!(1..=10).contains(&self.max_concurrent_positions){
        self.stats.denied_entry_guard+=1;return Ok(())
    }
    if self.stats.entries>=self.max_daily_entries{
        self.stats.denied_daily_limit+=1;return Ok(())
    }
    if self.positions.len()>=self.max_concurrent_positions{
        self.stats.denied_concurrent_limit+=1;return Ok(())
    }
    let Some(qty)=decision.and_then(|d|d.quantity) else{
        self.stats.denied_entry_guard+=1;return Ok(())
    };
    let Some(route)=route else{self.stats.denied_metadata+=1;return Ok(())};
    if now_ns<spot.received_ns||now_ns< fut.received_ns
        ||now_ns-spot.received_ns>self.assumptions.max_book_age_ns
        ||now_ns-fut.received_ns>self.assumptions.max_book_age_ns {
        self.stats.denied_stale+=1;return Ok(())
    }
    let paid=match buy_base(&spot.asks,qty){
        Ok(v)=>v,Err(_)=>{self.stats.denied_liquidity+=1;return Ok(())}
    };
    let gross=match sell_base(&fut.bids,qty){
        Ok(v)=>v,Err(_)=>{self.stats.denied_liquidity+=1;return Ok(())}
    };
    if !source_valid(qty,route,paid,gross){
        self.stats.denied_metadata+=1;return Ok(())
    }
    if paid>self.assumptions.quote_budget*(1.0+1e-9){
        self.stats.denied_entry_guard+=1;return Ok(())
    }
    // An unprofitable scan candidate is not an attempted opening.
    let signal_net=(gross-paid-paid*self.assumptions.other_cost_bps/10_000.0)
        /paid*10_000.0;
    if signal_net<self.assumptions.min_entry_net_bps{return Ok(())}
    let mut machine=PaperMachine::new(self.assumptions.clone()).map_err(PortfolioError::Book)?;
    let event=machine.observe_entry_exact(base,spot,fut,now_ns,qty)
        .map_err(PortfolioError::Book)?;
    if event==PaperEvent::SignalSpotBuy{
        // Python marks a PAPER open, not a merely examined candidate.
        self.scan_last_mark_ns.insert(base.to_owned(),now_ns);
        self.closed_bases.remove(base);
        self.closed_at_monotonic_ns.remove(base);
        self.stats.entries+=1;
        self.positions.insert(base.to_owned(),machine);
    }
    Ok(())
 }
 pub fn snapshot(&self)->serde_json::Value{
    let pending=self.positions.iter().map(|(base,m)|{
        let status=match m.phase {
            PaperPhase::Flat=>"FLAT",
            PaperPhase::AwaitingHedge(_)=>"UNHEDGED",
            PaperPhase::Hedged(_)=>"HEDGED",
            PaperPhase::AwaitingSpotExit{..}=>"EXIT_PENDING",
            PaperPhase::Closed(_)=>"CLOSED",
        };
        let signal=match &m.phase{
            PaperPhase::AwaitingHedge(p)|PaperPhase::Hedged(p)=>p.signal_estimated_net_bps,
            PaperPhase::AwaitingSpotExit{pos,..}=>pos.signal_estimated_net_bps,
            _=>0.0,
        };
        serde_json::json!({"base":base,"status":status,"signal_net_bps":signal})
    }).collect::<Vec<_>>();
    serde_json::json!({
        "status":"RESEARCH_PUBLIC_PAPER_ONLY",
        "max_concurrent":self.max_concurrent_positions,
        "max_daily_entries":self.max_daily_entries,
        "day_yekt":self.day_yek,
        "open_positions":self.positions.len(),
        "pending_positions":pending,
        "counters":self.stats,
        "no_real_or_private_api":true,
        "venue_rules_from_public_metadata_only":true,
        "fees_model_assumed_not_certified":true,
        "paper_fill_attested":false,
        "real_profit_proven":false
    })
 }
}

#[cfg(test)]
mod tests{
 use super::*;
 use crate::decision::{DecisionConfig,DecisionCore,Limits};
 use std::collections::{HashMap,HashSet};
 fn book(ns:u64,bid:f64,ask:f64)->Book{
   Book{bids:vec![(bid,100.0)],asks:vec![(ask,100.0)],
     received_ns:ns,received_wall_ms:1_791_783_600_000,
     exchange_ts_ms:None,changed_ns:ns,epoch:1,sequence:Some(ns as i64)}
 }
 fn route()->RouteSpec{
   RouteSpec{buy_exchange:"okx_spot".into(),
   sell_exchange:"binance_futures".into(),label:"Spot-Future".into(),
   buy:Limits{min_amount:"0.001".into(),step:"0.001".into(),
     min_cost:"0".into(),max_amount:None},
   sell:Limits{min_amount:"0.001".into(),step:"0.001".into(),
     min_cost:"5".into(),max_amount:None},
   loaded_ns:1,metadata_state:"SIZE_METADATA_READY".into()}
 }
 fn signal()->Decision{
    let mut c=DecisionCore::new(0,1,DecisionConfig{
      max_age_ms:800,metadata_ttl_ms:43_200_000,
      base_size:10.0,risk_cap:10.0,cost:24.03,required_net:42.0,
      min_price:0.0,max_price:f64::INFINITY,
      blacklist:HashSet::new()},HashMap::new()).unwrap();
    c.replace_routes(1,HashMap::from([("X".into(),vec![route()])])).unwrap();
    c.update_at("X","okx_spot",book(1,99.9,100.0),1,1_791_783_600_000).unwrap();
    let d=c.update_at("X","binance_futures",book(2,101.0,101.1),
        2,1_791_783_600_000).unwrap().remove(0);
    assert_eq!(d.reason,"EXECUTABLE_OPPORTUNITY");
    d
 }
 #[test]fn r129_paper_entry_binds_symbol_route_and_policy_shape(){
    let original=signal();
    assert!(paper_entry_decision_matches("X",&original,&route()));
    // Corrupt one proposal field at a time; PAPER must not turn it
    // into a modeled first leg or increment a virtual daily budget.
    for idx in 0..15{
        let mut d=original.clone();
        let mut r=route();
        match idx{
          0=>d.base="Y".into(),
          1=>d.route="other route".into(),
          2=>d.buy_exchange="other_spot".into(),
          3=>d.sell_exchange="other_futures".into(),
          4=>d.metadata_state="SIZE_METADATA_STALE".into(),
          5=>r.metadata_state="SIZE_METADATA_MISSING".into(),
          6=>d.quantity=Some(f64::NAN),
          7=>d.quantity=Some(0.0),
          8=>d.cost_bps=f64::INFINITY,
          9=>d.required_net_bps=f64::NAN,
         10=>d.risk_cap_usdt=0.0,
         11=>d.estimated_net_bps=None,
         12=>d.estimated_net_bps=Some(-100.0),
         13=>d.spot_notional_usdt=Some(f64::NAN),
          _=>d.futures_notional_usdt=Some(-1.0),
        }
        assert!(!paper_entry_decision_matches("X",&d,&r),"mutation {idx}");
        let mut paper=PaperPortfolio::new(1_791_783_600_000).unwrap();
        paper.on_frame("X",Venue::OkxSpot,2,1_791_783_600_000,
           Some(&book(2,99.9,100.0)),Some(&book(2,101.0,101.1)),
           Some(&d),Some(&r)).unwrap();
        assert_eq!(paper.stats.entries,0,"mutation {idx}");
        assert_eq!(paper.open_count(),0,"mutation {idx}");
        assert_eq!(paper.stats.denied_entry_guard,1,"mutation {idx}");
    }
    let mut paper=PaperPortfolio::new(1_791_783_600_000).unwrap();
    paper.on_frame("X",Venue::OkxSpot,2,1_791_783_600_000,
       Some(&book(2,99.9,100.0)),Some(&book(2,101.0,101.1)),
       Some(&original),Some(&route())).unwrap();
    assert_eq!(paper.stats.entries,1,"intact signal must still model opening");
    assert_eq!(paper.open_count(),1);
 }
 #[test]fn p106_actual_hedged_exit_uses_yaml_bound_lifetime_and_exit_spread(){
    let d=signal();
    let mut p=PaperPortfolio::new(1_791_783_600_000).unwrap();
    p.apply_public_paper_temporal_policy_review(
        30_000_000_000,1.0,60_000_000_000,12.0).unwrap();
    p.on_frame("X",Venue::OkxSpot,2,1_791_783_600_000,
       Some(&book(2,99.9,100.0)),Some(&book(2,101.0,101.1)),
       Some(&d),Some(&route())).unwrap();
    p.on_frame("X",Venue::BinanceFutures,3,1_791_783_600_000,
       Some(&book(2,99.9,100.0)),Some(&book(3,101.0,101.1)),
       None,None).unwrap();
    assert!(matches!(p.positions["X"].phase,PaperPhase::Hedged(_)));
    let mut too_soon=p.clone();
    let now=29_000_000_002u64;
    too_soon.on_frame("X",Venue::OkxSpot,now,1_791_783_600_000,
       Some(&book(now,100.0,100.0)),Some(&book(now,99.99,100.009)),
       None,None).unwrap();
    assert_eq!(too_soon.stats.futures_covers,0);
    assert!(matches!(too_soon.positions["X"].phase,PaperPhase::Hedged(_)));
    let mut too_wide=p.clone();
    let now=30_000_000_002u64;
    too_wide.on_frame("X",Venue::OkxSpot,now,1_791_783_600_000,
       Some(&book(now,100.0,100.0)),Some(&book(now,99.99,100.011)),
       None,None).unwrap();
    assert_eq!(too_wide.stats.futures_covers,0);
    p.on_frame("X",Venue::OkxSpot,now,1_791_783_600_000,
       Some(&book(now,100.0,100.0)),Some(&book(now,99.99,100.009)),
       None,None).unwrap();
    assert_eq!(p.stats.futures_covers,1);
    assert!(matches!(p.positions["X"].phase,PaperPhase::AwaitingSpotExit{..}));
 }
 #[test]fn a8_prepared_frame_lifetime_controls_modeled_cover_without_rewriting_source(){
    let d=signal();
    let mut p=PaperPortfolio::new(1_791_783_600_000).unwrap();
    p.apply_public_paper_temporal_policy_review(
        30_000_000_000,1.0,120_000_000_000,12.0).unwrap();
    p.on_frame("X",Venue::OkxSpot,2,1_791_783_600_000,
      Some(&book(2,99.9,100.0)),Some(&book(2,101.0,101.1)),
      Some(&d),Some(&route())).unwrap();
    p.on_frame("X",Venue::BinanceFutures,3,1_791_783_600_000,
      Some(&book(2,99.9,100.0)),Some(&book(3,101.0,101.1)),
      None,None).unwrap();
    let now=20_000_000_002;
    let spot=book(now,100.0,100.0);
    let fut=book(now,99.99,100.009);
    let mut static_gate=p.clone();
    static_gate.on_frame("X",Venue::OkxSpot,now,1_791_783_600_000,
      Some(&spot),Some(&fut),None,None).unwrap();
    assert_eq!(static_gate.stats.futures_covers,0);
    let mut dynamic=p.clone();
    dynamic.on_frame_with_checked_exit_policy("X",Venue::OkxSpot,
      now,1_791_783_600_000,Some(&spot),Some(&fut),None,None,
      Some((15_000_000_000,1.0))).unwrap();
    assert_eq!(dynamic.stats.futures_covers,1);
    assert_eq!(dynamic.min_lifetime_ns,30_000_000_000);
    assert_eq!(dynamic.exit_spread_bps,1.0);
    let mut invalid=p;
    assert!(invalid.on_frame_with_checked_exit_policy("X",Venue::OkxSpot,
      now,1_791_783_600_000,Some(&spot),Some(&fut),None,None,
      Some((0,1.0))).is_err());
    assert_eq!(invalid.stats.futures_covers,0);
 }
 #[test]fn p106_model_exit_target_consumes_real_portfolio_value(){
    let signal=signal();
    let make=|target:f64|{
       let mut p=PaperPortfolio::new(1_791_783_600_000).unwrap();
       p.apply_public_paper_temporal_policy_review(
         15_000_000_000,1.0,120_000_000_000,target).unwrap();
       p.on_frame("X",Venue::OkxSpot,2,1_791_783_600_000,
         Some(&book(2,99.9,100.0)),Some(&book(2,101.0,101.1)),
         Some(&signal),Some(&route())).unwrap();
       p.on_frame("X",Venue::BinanceFutures,3,1_791_783_600_000,
         Some(&book(2,99.9,100.0)),Some(&book(3,101.0,101.1)),
         None,None).unwrap();
       assert!(matches!(p.positions["X"].phase,PaperPhase::Hedged(_)));
       p
    };
    let now=30_000_000_002u64;
    let mut low=make(5.0);
    let mut high=make(500.0);
    for p in [&mut low,&mut high]{
      p.on_frame("X",Venue::OkxSpot,now,1_791_783_600_000,
        Some(&book(now,100.0,100.0)),Some(&book(now,99.99,100.009)),
        None,None).unwrap();
    }
    assert_eq!(low.stats.futures_covers,1);
    assert!(matches!(low.positions["X"].phase,PaperPhase::AwaitingSpotExit{..}));
    assert_eq!(high.stats.futures_covers,0);
    assert!(matches!(high.positions["X"].phase,PaperPhase::Hedged(_)));
    assert_eq!(high.positions["X"].config.take_profit_quote,Some(0.5));
    assert_eq!(low.positions["X"].config.take_profit_quote,Some(0.005));
 }
 #[test]fn r129_existing_paper_position_advances_when_entry_proposal_is_corrupt(){
    let d=signal();
    let mut paper=PaperPortfolio::new(1_791_783_600_000).unwrap();
    paper.on_frame("X",Venue::OkxSpot,2,1_791_783_600_000,
       Some(&book(2,99.9,100.0)),Some(&book(2,101.0,101.1)),
       Some(&d),Some(&route())).unwrap();
    assert_eq!(paper.stats.entries,1);
    let mut bad=d.clone();bad.base="OTHER".into();
    paper.on_frame("X",Venue::BinanceFutures,3,1_791_783_600_000,
       Some(&book(2,99.9,100.0)),Some(&book(3,100.5,101.1)),
       Some(&bad),Some(&route())).unwrap();
    assert_eq!(paper.stats.futures_hedges,1);
    assert_eq!(paper.stats.entries,1);
    assert!(matches!(paper.positions["X"].phase,PaperPhase::Hedged(_)));
 }
 #[test]fn zero_min_quote_is_supported_not_negative(){
    assert!(source_valid(0.099,&route(),9.9,9.999));
    let mut bad=route();bad.buy.min_cost="-1".into();
    assert!(!source_valid(0.099,&bad,9.9,9.999));
    bad=route();bad.buy.step="0.01".into();
    assert!(!source_valid(0.099,&bad,9.9,9.999));
 }
 #[test]fn max_ten_positions_is_enforced_before_entering(){
    let mut p=PaperPortfolio::new(1_791_783_600_000).unwrap();
    let config=p.assumptions.clone();
    for i in 0..MAX_CONCURRENT_PAPER{
      p.positions.insert(format!("OLD{i}"),PaperMachine::new(config.clone()).unwrap());
    }
    let d=signal();
    p.on_frame("X",Venue::OkxSpot,2,1_791_783_600_000,
       Some(&book(2,99.9,100.0)),Some(&book(2,101.0,101.1)),
       Some(&d),Some(&route())).unwrap();
    assert_eq!(p.open_count(),10);
    assert_eq!(p.stats.entries,0);
    assert_eq!(p.stats.denied_concurrent_limit,1);
 }
 #[test]fn r112_exact_120_seconds_per_base_reopen_no_day_lock(){
    let mut p=PaperPortfolio::new(1_791_783_600_000).unwrap();
    let last=50u64;
    p.closed_bases.insert("X".into());
    p.closed_at_monotonic_ns.insert("X".into(),last);
    p.stats.entries=1;p.stats.closed=1;
    let d=signal();
    for now in [last-1,last,last+119_999_999_999]{
      p.on_frame("X",Venue::OkxSpot,now,1_791_783_600_000,
        Some(&book(now,99.9,100.0)),Some(&book(now,101.0,101.1)),
        Some(&d),Some(&route())).unwrap();
      assert_eq!(p.stats.entries,1);
      assert!(p.positions.is_empty());
    }
    assert_eq!(p.stats.denied_entry_guard,3);
    let now=last+120_000_000_000;
    p.on_frame("X",Venue::OkxSpot,now,1_791_783_600_000,
       Some(&book(now,99.9,100.0)),Some(&book(now,101.0,101.1)),
       Some(&d),Some(&route())).unwrap();
    assert_eq!(p.stats.entries,2);
    assert!(!p.closed_bases.contains("X"));
    assert!(!p.closed_at_monotonic_ns.contains_key("X"));
 }
 #[test]fn r112_missing_cold_clock_history_denies_reentry(){
   let mut p=PaperPortfolio::new(1_791_783_600_000).unwrap();
   p.closed_bases.insert("X".into());
   p.stats.entries=1;p.stats.closed=1;
   let mut raw=serde_json::to_value(&p).unwrap();
   raw.as_object_mut().unwrap().remove("closed_at_monotonic_ns");
   let mut cold:PaperPortfolio=serde_json::from_value(raw).unwrap();
   let now=200_000_000_000u64;
   let d=signal();
   cold.on_frame("X",Venue::OkxSpot,now,1_791_783_600_000,
     Some(&book(now,99.9,100.0)),Some(&book(now,101.0,101.1)),
     Some(&d),Some(&route())).unwrap();
   assert_eq!(cold.stats.denied_entry_guard,1);
   assert!(cold.positions.is_empty());
 }
 #[test]fn r115_cold_json_roundtrip_must_not_resume_new_entry(){
    let src=PaperPortfolio::new(1_791_783_600_000).unwrap();
    let raw=serde_json::to_string(&src).unwrap();
    assert!(!raw.contains("cold_boot_unknown"));
    let mut cold:PaperPortfolio=serde_json::from_str(&raw).unwrap();
    assert!(cold.cold_boot_unknown);
    let now=200_000_000_000u64;
    let d=signal();
    cold.on_frame("X",Venue::OkxSpot,now,1_791_783_600_000,
      Some(&book(now,99.9,100.0)),Some(&book(now,101.0,101.1)),
      Some(&d),Some(&route())).unwrap();
    assert_eq!(cold.stats.entries,0);
    assert_eq!(cold.stats.denied_entry_guard,1);
 }
 #[test]fn r116_scanner_cooldown_and_close_cooldown_are_independent(){
    let mut p=PaperPortfolio::new(1_791_783_600_000).unwrap();
    p.reentry_cooldown_ns=30_000_000_000;
    p.scan_cooldown_ns=70_000_000_000;
    p.closed_bases.insert("X".into());
    p.closed_at_monotonic_ns.insert("X".into(),5);
    p.scan_last_mark_ns.insert("X".into(),10);
    p.stats.entries=1;p.stats.closed=1;
    let d=signal();
    let now=45_000_000_010u64;
    p.on_frame("X",Venue::OkxSpot,now,1_791_783_600_000,
       Some(&book(now,99.9,100.0)),Some(&book(now,101.0,101.1)),
       Some(&d),Some(&route())).unwrap();
    assert_eq!(p.stats.entries,1);
    assert_eq!(p.stats.denied_entry_guard,1);
    let later=70_000_000_010u64;
    p.on_frame("X",Venue::OkxSpot,later,1_791_783_600_000,
       Some(&book(later,99.9,100.0)),Some(&book(later,101.0,101.1)),
       Some(&d),Some(&route())).unwrap();
    assert_eq!(p.stats.entries,2);
    assert_eq!(p.scan_last_mark_ns["X"],later);
 }
 #[test]fn r120_unprofitable_candidate_does_not_start_scanner_cooldown(){
   let mut p=PaperPortfolio::new(1_791_783_600_000).unwrap();
   p.scan_cooldown_ns=120_000_000_000;
   p.assumptions.min_entry_net_bps=1_000_000.0;
   let d=signal();
   p.on_frame("X",Venue::OkxSpot,20,1_791_783_600_000,
       Some(&book(20,99.9,100.0)),Some(&book(20,101.0,101.1)),
       Some(&d),Some(&route())).unwrap();
   assert_eq!(p.stats.entries,0);
   assert!(p.scan_last_mark_ns.get("X").is_none());
   p.assumptions.min_entry_net_bps=0.0;
   p.on_frame("X",Venue::OkxSpot,21,1_791_783_600_000,
       Some(&book(21,99.9,100.0)),Some(&book(21,101.0,101.1)),
       Some(&d),Some(&route())).unwrap();
   assert_eq!(p.stats.entries,1);
   assert_eq!(p.scan_last_mark_ns["X"],21);
 }
 #[test]fn r115_cold_virtual_hedge_progression_still_allowed(){
  let mut p=PaperPortfolio::new(1_791_783_600_000).unwrap();
  let d=signal();
  p.on_frame("X",Venue::OkxSpot,2,1_791_783_600_000,
    Some(&book(2,99.9,100.0)),Some(&book(2,101.0,101.1)),
    Some(&d),Some(&route())).unwrap();
  assert_eq!(p.stats.entries,1);
  let mut data=serde_json::to_value(&p).unwrap();
  assert!(data.get("cold_boot_unknown").is_none());
  let mut cold:PaperPortfolio=serde_json::from_value(data.clone()).unwrap();
  assert!(cold.cold_boot_unknown);
  cold.on_frame("X",Venue::BinanceFutures,3,1_791_783_600_000,
    Some(&book(2,99.9,100.0)),Some(&book(3,100.5,101.1)),
    None,None).unwrap();
  assert_eq!(cold.stats.futures_hedges,1);
  assert!(matches!(cold.positions["X"].phase,PaperPhase::Hedged(_)));
  data["cold_boot_unknown"]=serde_json::Value::Bool(false);
  assert!(serde_json::from_value::<PaperPortfolio>(data).is_err());
 }
 #[test]fn a3_original_yaml_quota_applied_to_actual_portfolio_entry_gate(){
    use crate::native_trading_config::NativeTradingConfig;
    use crate::native_paper_config_bridge::bind_source_paper_quotas_review;
    let cfg=NativeTradingConfig::parse(
       include_str!("../config/native_trading.yaml")).unwrap();
    let quotas=bind_source_paper_quotas_review(&cfg).unwrap();
    assert_eq!(quotas.daily,3000);
    assert_eq!(quotas.concurrent,10);
    let mut p=PaperPortfolio::new(1_791_783_600_000).unwrap();
    p.apply_source_paper_quotas_review(quotas.daily,quotas.concurrent).unwrap();
    assert_eq!(p.snapshot()["max_daily_entries"],3000);
    p.stats.entries=500;
    let d=signal();
    p.on_frame("X",Venue::OkxSpot,2,1_791_783_600_000,
       Some(&book(2,99.9,100.0)),Some(&book(2,101.0,101.1)),
       Some(&d),Some(&route())).unwrap();
    assert_eq!(p.stats.entries,501);
    assert_eq!(p.stats.denied_daily_limit,0);
    let cold:PaperPortfolio=serde_json::from_str(
       &serde_json::to_string(&p).unwrap()).unwrap();
    assert!(cold.verify_source_paper_quotas_review(3000,10).is_ok());
    assert!(cold.verify_source_paper_quotas_review(500,10).is_err());
    assert!(cold.clone().apply_source_paper_quotas_review(500,10).is_err());
 }
 #[test]fn a3_corrupt_cold_checkpoint_quotas_do_not_admit_new_entries(){
    let mut p=PaperPortfolio::new(1_791_783_600_000).unwrap();
    p.max_daily_entries=u64::MAX;
    let mut cold:PaperPortfolio=serde_json::from_str(
      &serde_json::to_string(&p).unwrap()).unwrap();
    assert!(cold.cold_boot_unknown);
    // An explicit cold-boot denial remains active.
    cold.on_frame("X",Venue::OkxSpot,2,1_791_783_600_000,
      Some(&book(2,99.9,100.0)),Some(&book(2,101.0,101.1)),
      Some(&signal()),Some(&route())).unwrap();
    assert_eq!(cold.stats.entries,0);
    let mut active=p.clone();
    active.cold_boot_unknown=false;
    active.on_frame("X",Venue::OkxSpot,2,1_791_783_600_000,
      Some(&book(2,99.9,100.0)),Some(&book(2,101.0,101.1)),
      Some(&signal()),Some(&route())).unwrap();
    assert_eq!(active.stats.entries,0);
    assert_eq!(active.stats.denied_entry_guard,1);
 }
 #[test]fn daily_500_enforced_and_no_auto_rollover(){
    let mut p=PaperPortfolio::new(1_791_783_600_000).unwrap();
    p.stats.entries=500;
    let d=signal();
    p.on_frame("X",Venue::OkxSpot,2,1_791_783_600_000,
       Some(&book(2,99.9,100.0)),Some(&book(2,101.0,101.1)),
       Some(&d),Some(&route())).unwrap();
    assert_eq!(p.stats.denied_daily_limit,1);
    assert_eq!(p.open_count(),0);
    assert!(p.day_check(1_791_783_600_000+DAY_MS).is_err());
 }
 #[test]fn entry_and_next_frame_hedge_are_distinct(){
    let mut p=PaperPortfolio::new(1_791_783_600_000).unwrap();
    let d=signal();
    p.on_frame("X",Venue::OkxSpot,2,1_791_783_600_000,
       Some(&book(2,99.9,100.0)),Some(&book(2,101.0,101.1)),
       Some(&d),Some(&route())).unwrap();
    assert_eq!(p.stats.entries,1);
    assert_eq!(p.stats.futures_hedges,0);
    p.on_frame("X",Venue::BinanceFutures,3,1_791_783_600_000,
       Some(&book(2,99.9,100.0)),Some(&book(3,100.5,101.1)),
       Some(&d),Some(&route())).unwrap();
    assert_eq!(p.stats.entries,1);
    assert_eq!(p.stats.futures_hedges,1);
    assert!(matches!(p.positions["X"].phase,PaperPhase::Hedged(_)));
 }
}
