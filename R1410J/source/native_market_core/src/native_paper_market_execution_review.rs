//! Astra P0-02/P1-02/P1-03/P1-09: event-driven zero-network PAPER review.
//! Uses only successive observed books. DOES NOT prove real order fills, queue
//! priority, exchange latency, fee schedule, funding, funding cutoff or PnL.
//! No sleep, timers, keys, transport, exchange mutations or real permissions.
use crate::Book;
use serde::{Serialize,Deserialize};

#[derive(Clone,Debug,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaperAssumptions {
    pub quote_budget: f64,
    pub min_entry_net_bps: f64,
    /// Assumed remaining total round-trip quote costs, EXCLUDING any
    /// spot BUY fee explicitly deducted in BASE below.
    pub other_cost_bps: f64,
    pub spot_buy_fee_base_bps: f64,
    /// None: no autonomously invented take-profit policy; mark/hold only.
    /// Some(x): take profit when executable paper exit value strictly exceeds x.
    pub take_profit_quote: Option<f64>,
    pub max_book_age_ns: u64,
}
impl Default for PaperAssumptions {
    fn default()->Self{Self{
        quote_budget:10.0,min_entry_net_bps:42.0,other_cost_bps:24.03,
        spot_buy_fee_base_bps:0.0,take_profit_quote:None,
        max_book_age_ns:800_000_000,
    }}
}
#[derive(Clone,Debug,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaperPosition {
    pub base:String,
    pub signal_ns:u64,
    pub spot_buy_ns:u64,
    pub signal_estimated_net_bps:f64,
    pub spot_gross_base:f64,
    pub spot_delivered_base:f64,
    pub spot_paid_quote:f64,
    pub futures_short_ns:Option<u64>,
    pub futures_short_proceeds_quote:Option<f64>,
    pub futures_short_base:Option<f64>,
    pub hedge_entry_estimated_net_bps:Option<f64>,
}
#[derive(Clone,Debug,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaperClosed{
    pub base:String,
    pub spot_buy_ns:u64,
    pub futures_short_ns:u64,
    pub futures_cover_ns:u64,
    pub spot_sell_ns:u64,
    pub signal_estimated_net_bps:f64,
    pub spot_paid_quote:f64,
    pub futures_short_quote:f64,
    pub futures_cover_quote:f64,
    pub spot_sell_quote:f64,
    pub assumed_other_fees_quote:f64,
    pub modeled_net_quote:f64,
    pub modeled_net_bps:f64,
    pub close_decision_estimated_quote:f64,
}
#[derive(Clone,Debug,Serialize,Deserialize)]
pub enum PaperPhase{
    Flat,
    AwaitingHedge(PaperPosition),
    Hedged(PaperPosition),
    AwaitingSpotExit{pos:PaperPosition,cover_quote:f64,cover_ns:u64,close_estimate_quote:f64},
    Closed(PaperClosed),
}
#[derive(Clone,Debug,PartialEq)]
pub enum PaperError{InvalidConfig,InvalidBook,StaleBook,InsufficientDepth,EventOrder}
#[derive(Clone,Debug,PartialEq)]
pub enum PaperEvent{NoAction,SignalSpotBuy,FutureShort,Hold,TakeProfitFuturesCover,SpotExitComplete}
#[derive(Clone,Debug,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PaperMachine{pub config:PaperAssumptions,pub phase:PaperPhase}
fn valid(n:f64)->bool{n.is_finite()&&n>0.0}
fn fresh(book:&Book,now:u64,max_age:u64)->bool{
    now>=book.received_ns&&now-book.received_ns<=max_age
}
/// Orderbook VWAP for a buy of BASE quantity (asks).
pub fn buy_base(asks:&[(f64,f64)],qty:f64)->Result<f64,PaperError>{
    sweep(asks,qty)
}
/// Orderbook VWAP for a sell of BASE quantity (bids).
pub fn sell_base(bids:&[(f64,f64)],qty:f64)->Result<f64,PaperError>{
    sweep(bids,qty)
}
fn sweep(levels:&[(f64,f64)],qty:f64)->Result<f64,PaperError>{
    if !valid(qty){return Err(PaperError::InvalidBook)}
    let mut left=qty;let mut quote=0.0;
    for &(price,volume) in levels{
        if !valid(price)||!valid(volume){return Err(PaperError::InvalidBook)}
        let n=left.min(volume);
        quote+=price*n;left-=n;
        if left<=qty*1e-10{break}
    }
    if left>qty*1e-10{return Err(PaperError::InsufficientDepth)}
    if !valid(quote){return Err(PaperError::InvalidBook)}
    Ok(quote)
}
/// Consume a fixed quote budget against asks, never more quote than budget.
pub fn buy_with_quote(asks:&[(f64,f64)],budget:f64)
    ->Result<(f64,f64),PaperError>{
    if !valid(budget){return Err(PaperError::InvalidConfig)}
    let mut left=budget;let mut qty=0.0;let mut quote=0.0;
    for &(price,volume) in asks{
        if !valid(price)||!valid(volume){return Err(PaperError::InvalidBook)}
        let q=(left/price).min(volume);
        qty+=q;let spent=q*price;quote+=spent;left-=spent;
        if left<=budget*1e-10{break}
    }
    if left>budget*1e-10{return Err(PaperError::InsufficientDepth)}
    if !valid(qty)||quote>budget*(1.0+1e-9){
        return Err(PaperError::InvalidBook)
    }
    Ok((qty,quote))
}
impl PaperMachine{
    pub fn new(config:PaperAssumptions)->Result<Self,PaperError>{
        if !valid(config.quote_budget)||config.min_entry_net_bps<0.0
           ||!config.min_entry_net_bps.is_finite()||config.other_cost_bps<0.0
           ||!config.other_cost_bps.is_finite()
           ||!(0.0..10000.0).contains(&config.spot_buy_fee_base_bps)
           ||config.take_profit_quote.is_some_and(|x|!x.is_finite()||x<0.0)
           ||config.max_book_age_ns==0{return Err(PaperError::InvalidConfig)}
        Ok(Self{config,phase:PaperPhase::Flat})
    }
    fn fees(&self,spot_paid:f64)->f64{
        spot_paid*self.config.other_cost_bps/10_000.0
    }
    /// Signal and paper spot buy at the book CURRENTLY observed. No future
    /// frames inspected. Sufficient depth and both books must be fresh.
    pub fn observe_entry(&mut self,base:&str,spot:&Book,future:&Book,now:u64)
        ->Result<PaperEvent,PaperError>{
        if !matches!(self.phase,PaperPhase::Flat){return Ok(PaperEvent::NoAction)}
        if !fresh(spot,now,self.config.max_book_age_ns)
            ||!fresh(future,now,self.config.max_book_age_ns){
            return Err(PaperError::StaleBook)
        }
        let (gross,paid)=buy_with_quote(&spot.asks,self.config.quote_budget)?;
        let delivered=gross*(1.0-self.config.spot_buy_fee_base_bps/10000.0);
        if !valid(delivered){return Err(PaperError::InvalidBook)}
        let sell=sell_base(&future.bids,delivered)?;
        let signal_bps=(sell-paid-self.fees(paid))/paid*10_000.0;
        if signal_bps<self.config.min_entry_net_bps{
            return Ok(PaperEvent::NoAction)
        }
        self.phase=PaperPhase::AwaitingHedge(PaperPosition{
            base:base.into(),signal_ns:now,spot_buy_ns:now,
            signal_estimated_net_bps:signal_bps,spot_gross_base:gross,
            spot_delivered_base:delivered,spot_paid_quote:paid,
            futures_short_ns:None,futures_short_proceeds_quote:None,
            futures_short_base:None,hedge_entry_estimated_net_bps:None,
        });
        Ok(PaperEvent::SignalSpotBuy)
    }
    /// Only a NEWER Binance observation can paper-fill the second leg.
    /// A worse future price DOES NOT rewrite the signal as guaranteed PnL.
    pub fn observe_future_short(&mut self,base:&str,future:&Book,now:u64)
        ->Result<PaperEvent,PaperError>{
        let PaperPhase::AwaitingHedge(ref pos)=self.phase else{
            return Ok(PaperEvent::NoAction)
        };
        if base!=pos.base||now<=pos.spot_buy_ns||
           future.received_ns<=pos.spot_buy_ns{return Ok(PaperEvent::NoAction)}
        if !fresh(future,now,self.config.max_book_age_ns){
            return Err(PaperError::StaleBook)
        }
        let proceeds=sell_base(&future.bids,pos.spot_delivered_base)?;
        let mut p=pos.clone();
        p.futures_short_ns=Some(now);
        p.futures_short_proceeds_quote=Some(proceeds);
        p.futures_short_base=Some(p.spot_delivered_base);
        p.hedge_entry_estimated_net_bps=Some(
            (proceeds-p.spot_paid_quote-self.fees(p.spot_paid_quote))
            /p.spot_paid_quote*10_000.0);
        self.phase=PaperPhase::Hedged(p);
        Ok(PaperEvent::FutureShort)
    }
    /// Mark using executable exit bids/asks. Negative mark is HOLD, not
    /// automatic loss liquidation. Explicit risk exits are separate P0-02 work.
    pub fn observe_hedged_exit(&mut self,base:&str,spot:&Book,future:&Book,now:u64)
        ->Result<(PaperEvent,Option<f64>),PaperError>{
        let PaperPhase::Hedged(ref pos)=self.phase else{
            return Ok((PaperEvent::NoAction,None))
        };
        if base!=pos.base{return Ok((PaperEvent::NoAction,None))}
        if !fresh(spot,now,self.config.max_book_age_ns)
            ||!fresh(future,now,self.config.max_book_age_ns){
            return Err(PaperError::StaleBook)
        }
        let spot_sale=sell_base(&spot.bids,pos.spot_delivered_base)?;
        let fut_cover=buy_base(&future.asks,pos.spot_delivered_base)?;
        let marked=spot_sale-pos.spot_paid_quote
            +pos.futures_short_proceeds_quote.ok_or(PaperError::EventOrder)?
            -fut_cover-self.fees(pos.spot_paid_quote);
        if marked<=self.config.take_profit_quote.unwrap_or(f64::INFINITY){
            return Ok((PaperEvent::Hold,Some(marked)))
        }
        let p=pos.clone();
        self.phase=PaperPhase::AwaitingSpotExit{
            pos:p,cover_quote:fut_cover,cover_ns:now,
            close_estimate_quote:marked,
        };
        Ok((PaperEvent::TakeProfitFuturesCover,Some(marked)))
    }
    /// Spot sale MUST be valued from NEXT spot observation, not a book
    /// already seen at the time futures cover was hypothetically filled.
    pub fn observe_spot_exit(&mut self,base:&str,spot:&Book,now:u64)
        ->Result<PaperEvent,PaperError>{
        let PaperPhase::AwaitingSpotExit{
            ref pos,cover_quote,cover_ns,close_estimate_quote
        }=self.phase else{return Ok(PaperEvent::NoAction)};
        if base!=pos.base||now<=cover_ns||spot.received_ns<=cover_ns{
            return Ok(PaperEvent::NoAction)
        }
        if !fresh(spot,now,self.config.max_book_age_ns){
            return Err(PaperError::StaleBook)
        }
        let sold=sell_base(&spot.bids,pos.spot_delivered_base)?;
        let shorts=pos.futures_short_proceeds_quote.ok_or(PaperError::EventOrder)?;
        let cost=self.fees(pos.spot_paid_quote);
        let net=sold-pos.spot_paid_quote+shorts-cover_quote-cost;
        let result=PaperClosed{
            base:pos.base.clone(),spot_buy_ns:pos.spot_buy_ns,
            futures_short_ns:pos.futures_short_ns.ok_or(PaperError::EventOrder)?,
            futures_cover_ns:cover_ns,spot_sell_ns:now,
            signal_estimated_net_bps:pos.signal_estimated_net_bps,
            spot_paid_quote:pos.spot_paid_quote,
            futures_short_quote:shorts,futures_cover_quote:cover_quote,
            spot_sell_quote:sold,assumed_other_fees_quote:cost,
            modeled_net_quote:net,modeled_net_bps:net/pos.spot_paid_quote*10_000.0,
            close_decision_estimated_quote:close_estimate_quote,
        };
        self.phase=PaperPhase::Closed(result);
        Ok(PaperEvent::SpotExitComplete)
    }
}
#[cfg(test)]
mod tests{
    use super::*;
    fn book(ns:u64,bid:f64,ask:f64,bidqty:f64,askqty:f64)->Book{
        Book{bids:vec![(bid,bidqty)],asks:vec![(ask,askqty)],
            received_ns:ns,received_wall_ms:1800000000000,exchange_ts_ms:None,
            changed_ns:ns,epoch:1,sequence:Some(ns as i64)}
    }
    fn machine()->PaperMachine{
        let mut c=PaperAssumptions::default();
        c.take_profit_quote=Some(0.0);
        PaperMachine::new(c).unwrap()
    }
    fn begin()->PaperMachine{
        let mut p=machine();
        assert_eq!(p.observe_entry("X",&book(10,99.9,100.,1.,1.),
            &book(10,101.,101.1,1.,1.),10),Ok(PaperEvent::SignalSpotBuy));
        p
    }
    fn hedge()->PaperMachine{
        let mut p=begin();
        assert_eq!(p.observe_future_short("X",&book(12,100.7,101.2,1.,1.),12),
            Ok(PaperEvent::FutureShort));
        p
    }
    #[test]fn full_depth_sell_buy_vwap_not_top_only(){
        assert_eq!(sell_base(&[(100.,0.05),(90.,0.05)],0.1),Ok(9.5));
        assert_eq!(buy_base(&[(100.,0.05),(110.,0.05)],0.1),Ok(10.5));
        assert_eq!(buy_with_quote(&[(100.,0.05),(110.,1.)],10.),Ok((0.05+5./110.,10.)));
    }
    #[test]fn liquidity_shortfall_fails_closed(){
        assert_eq!(sell_base(&[(101.,0.01)],0.1),Err(PaperError::InsufficientDepth));
        assert_eq!(buy_with_quote(&[(100.,0.01)],10.),Err(PaperError::InsufficientDepth));
    }
    #[test]fn first_leg_requires_fresh_books_and_minimum_estimated_net(){
        let mut p=machine();
        assert_eq!(p.observe_entry("X",&book(10,99.,100.,1.,1.),
            &book(10,100.2,100.3,1.,1.),10),Ok(PaperEvent::NoAction));
        assert_eq!(p.observe_entry("X",&book(10,99.,100.,1.,1.),
            &book(10,101.,101.2,1.,1.),900_000_011),
            Err(PaperError::StaleBook));
        assert!(matches!(p.phase,PaperPhase::Flat));
    }
    #[test]fn same_timestamp_futures_cannot_be_future_hedge(){
        let mut p=begin();
        assert_eq!(p.observe_future_short("X",&book(10,101.,102.,1.,1.),10),
            Ok(PaperEvent::NoAction));
        assert!(matches!(p.phase,PaperPhase::AwaitingHedge(_)));
    }
    #[test]fn changed_future_price_replaces_theoretical_signal_spread(){
        let p=hedge();
        let PaperPhase::Hedged(pos)=p.phase else{panic!("expected hedge")};
        assert!(pos.signal_estimated_net_bps>42.0);
        assert!(pos.hedge_entry_estimated_net_bps.unwrap()<pos.signal_estimated_net_bps);
        assert_eq!(pos.futures_short_ns,Some(12));
    }
    #[test]fn deterioration_to_negative_mark_must_not_force_loss_exit(){
        let mut p=hedge();
        let (e,mark)=p.observe_hedged_exit("X",&book(13,99.7,100.,1.,1.),
            &book(13,100.1,100.2,1.,1.),13).unwrap();
        assert_eq!(e,PaperEvent::Hold);assert!(mark.unwrap()<0.0);
        assert!(matches!(p.phase,PaperPhase::Hedged(_)));
    }
    #[test]fn positive_exit_first_closes_futures_then_next_spot_observation(){
        let mut p=hedge();
        let (event,mark)=p.observe_hedged_exit("X",&book(14,100.2,100.3,1.,1.),
            &book(14,99.7,99.8,1.,1.),14).unwrap();
        assert_eq!(event,PaperEvent::TakeProfitFuturesCover);assert!(mark.unwrap()>0.0);
        assert_eq!(p.observe_spot_exit("X",&book(14,100.2,100.3,1.,1.),14),
            Ok(PaperEvent::NoAction));
        assert_eq!(p.observe_spot_exit("X",&book(15,100.0,100.1,1.,1.),15),
            Ok(PaperEvent::SpotExitComplete));
        let PaperPhase::Closed(c)=p.phase else{panic!("expected close")};
        assert!(c.modeled_net_quote.is_finite());
        assert_ne!(c.spot_buy_ns,c.spot_sell_ns);
        assert_ne!(c.spot_sell_quote,c.close_decision_estimated_quote);
    }
    #[test]fn profit_forecast_may_turn_into_realized_loss_after_futures_cover(){
        let mut p=hedge();
        assert_eq!(p.observe_hedged_exit("X",&book(14,100.2,100.3,1.,1.),
            &book(14,99.7,99.8,1.,1.),14).unwrap().0,
            PaperEvent::TakeProfitFuturesCover);
        p.observe_spot_exit("X",&book(15,95.,95.1,1.,1.),15).unwrap();
        let PaperPhase::Closed(c)=p.phase else{panic!("expected close")};
        assert!(c.close_decision_estimated_quote>0.0);
        assert!(c.modeled_net_quote<0.0);
    }
    #[test]fn base_asset_fee_reduces_futures_hedge_size(){
        let mut config=PaperAssumptions::default();
        config.spot_buy_fee_base_bps=10.0;
        let mut p=PaperMachine::new(config).unwrap();
        p.observe_entry("X",&book(10,99.,100.,1.,1.),
            &book(10,101.5,102.,1.,1.),10).unwrap();
        let PaperPhase::AwaitingHedge(pos)=p.phase else{panic!("expected spot")};
        assert!((pos.spot_delivered_base-pos.spot_gross_base*0.999).abs()<1e-9);
        assert!(pos.spot_delivered_base<pos.spot_gross_base);
    }
    #[test]fn positive_mark_without_exit_policy_does_not_force_close(){
        let mut p=hedge();
        p.config.take_profit_quote=None;
        let (action,mark)=p.observe_hedged_exit("X",
            &book(14,100.2,100.3,1.,1.),
            &book(14,99.7,99.8,1.,1.),14).unwrap();
        assert_eq!(action,PaperEvent::Hold);
        assert!(mark.unwrap()>0.0);
        assert!(matches!(p.phase,PaperPhase::Hedged(_)));
    }
    #[test]fn repeat_entry_does_not_open_two_positions(){
        let mut p=begin();
        assert_eq!(p.observe_entry("Y",&book(11,99.,100.,1.,1.),
            &book(11,101.,102.,1.,1.),11),Ok(PaperEvent::NoAction));
    }
    #[test]fn missing_spot_exit_book_prevents_false_closed(){
        let mut p=hedge();
        p.observe_hedged_exit("X",&book(14,100.2,100.3,1.,1.),
            &book(14,99.7,99.8,1.,1.),14).unwrap();
        assert_eq!(p.observe_spot_exit("X",&book(15,100.,100.1,0.001,1.),15),
            Err(PaperError::InsufficientDepth));
        assert!(matches!(p.phase,PaperPhase::AwaitingSpotExit{..}));
    }
}

impl PaperMachine{
    /// Canonical sized paper entry; quantity must already pass public venue
    /// lot/step/notional validation in the separate portfolio policy.
    pub fn observe_entry_exact(&mut self,base:&str,spot:&Book,future:&Book,now:u64,qty:f64)
        ->Result<PaperEvent,PaperError>{
        if !matches!(self.phase,PaperPhase::Flat){return Ok(PaperEvent::NoAction)}
        if !fresh(spot,now,self.config.max_book_age_ns)
           ||!fresh(future,now,self.config.max_book_age_ns){
            return Err(PaperError::StaleBook)
        }
        let paid=buy_base(&spot.asks,qty)?;
        if paid>self.config.quote_budget*(1.0+1e-9){
            return Err(PaperError::InvalidConfig)
        }
        let net_qty=qty*(1.0-self.config.spot_buy_fee_base_bps/10_000.0);
        if !valid(net_qty){return Err(PaperError::InvalidBook)}
        let future_bid=sell_base(&future.bids,net_qty)?;
        let net_bps=(future_bid-paid-self.fees(paid))/paid*10_000.0;
        if net_bps<self.config.min_entry_net_bps{return Ok(PaperEvent::NoAction)}
        self.phase=PaperPhase::AwaitingHedge(PaperPosition{
            base:base.into(),signal_ns:now,spot_buy_ns:now,
            signal_estimated_net_bps:net_bps,spot_gross_base:qty,
            spot_delivered_base:net_qty,spot_paid_quote:paid,
            futures_short_ns:None,futures_short_proceeds_quote:None,
            futures_short_base:None,hedge_entry_estimated_net_bps:None,
        });
        Ok(PaperEvent::SignalSpotBuy)
    }
}
