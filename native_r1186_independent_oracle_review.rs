//! R1186 cross-language oracle: Python-created original R1162-depth fixture
//! with clearly labeled synthetic subsequent prices. Independent arithmetic
//! is frozen in tests/fixtures, not computed by DecisionCore or PaperMachine.
#![cfg(test)]
use crate::Book;
use crate::native_paper_market_execution_review::{
    PaperAssumptions,PaperMachine,PaperPhase,PaperEvent,buy_with_quote,sell_base
};
use serde_json::Value;
const FIXTURE:&str=include_str!("../tests/fixtures/r1186_independent_two_market_oracle.json");
fn oracle()->Value {
    let f:Value=serde_json::from_str(FIXTURE).unwrap();
    assert_eq!(f["schema"],"SPOTARB/R1186/INDEPENDENT-FOUR-LEG-ORACLE/V1");
    assert_eq!(f["provenance"]["actual_fills"],false);
    assert_eq!(f["provenance"]["original_decisions_attested"],false);
    assert_eq!(f["provenance"]["r1171_fixture_sha256"],
      "3487feadf2a424fc364224e950603a92ef9b8f6562d9c02410b59536be51d137");
    f
}
fn num(x:&Value,key:&str)->f64{x[key].as_f64().unwrap()}
fn close(got:f64,want:f64,ctx:&str){
    let tolerance=1e-9_f64.max(want.abs()*1e-8);
    assert!((got-want).abs()<=tolerance,
     "{ctx}: got {got:.14}, independent {want:.14}, tol={tolerance:.12}");
}
fn book(x:&Value)->Book{serde_json::from_value(x.clone()).unwrap()}
fn machine(cfg:&Value)->PaperMachine{
    let v:PaperAssumptions=serde_json::from_value(cfg.clone()).unwrap();
    assert_eq!(v.max_book_age_ns,800_000_000);
    assert_eq!(v.take_profit_quote,Some(0.0));
    PaperMachine::new(v).unwrap()
}
#[test]
fn original_r1162_books_do_not_prove_profit_or_accept_entry(){
    let f=oracle();
    for e in f["negative_controls"].as_array().unwrap(){
        let mut spot=book(&e["raw_spot"]);
        let mut fut=book(&e["raw_future"]);
        // Explicit counterfactual co-timestamp: original frozen
        // snapshots are NOT contemporaneous proof of live liquidity.
        spot.received_ns=1_000_000_000;
        fut.received_ns=1_000_000_000;
        let mut p=machine(&f["model"]);
        let base=e["base"].as_str().unwrap();
        assert_eq!(p.observe_entry(base,&spot,&fut,1_000_000_000),
                   Ok(PaperEvent::NoAction));
        assert!(matches!(p.phase,PaperPhase::Flat));
        let (gross,paid)=buy_with_quote(&spot.asks,10.0).unwrap();
        let delivered=gross*(1.0-p.config.spot_buy_fee_base_bps/10000.0);
        let short=sell_base(&fut.bids,delivered).unwrap();
        let score=(short-paid-paid*p.config.other_cost_bps/10000.0)/paid*10000.0;
        close(score,num(e,"expected_entry_net_bps"),base);
    }
}
#[test]
fn two_symbols_four_modeled_legs_match_independent_python_math(){
    let f=oracle();
    let cases=f["cases"].as_array().unwrap();
    assert_eq!(cases.len(),2);
    for e in cases{
        let base=e["base"].as_str().unwrap();
        let b=&e["books"];let want=&e["expected"];
        let mut m=machine(&f["model"]);
        let entry_ns=want["entry_ns"].as_u64().unwrap();
        let hedge_ns=want["hedge_ns"].as_u64().unwrap();
        let cover_ns=want["cover_ns"].as_u64().unwrap();
        let exit_ns=want["spot_exit_ns"].as_u64().unwrap();
        assert_eq!(m.observe_entry(base,&book(&b["entry_spot"]),
                    &book(&b["entry_future"]),entry_ns),
                   Ok(PaperEvent::SignalSpotBuy));
        let PaperPhase::AwaitingHedge(ref pos)=m.phase else{
            panic!("{base}: missing modeled spot leg");
        };
        close(pos.spot_gross_base,num(want,"gross_base"),base);
        close(pos.spot_delivered_base,num(want,"delivered_base"),base);
        close(pos.spot_paid_quote,num(want,"spot_paid_quote"),base);
        close(pos.signal_estimated_net_bps,num(want,"estimated_signal_bps"),base);
        let entry_before=serde_json::to_string(&m.phase).unwrap();
        // Fail-closed: hedge from already-seen timestamp is never a fill.
        assert_eq!(m.observe_future_short(base,&book(&b["entry_future"]),entry_ns),
                   Ok(PaperEvent::NoAction));
        assert_eq!(serde_json::to_string(&m.phase).unwrap(),entry_before);
        assert_eq!(m.observe_future_short(base,&book(&b["hedge_future"]),hedge_ns),
                   Ok(PaperEvent::FutureShort));
        let PaperPhase::Hedged(ref pos)=m.phase else{
            panic!("{base}: hedge missing");
        };
        close(pos.futures_short_proceeds_quote.unwrap(),
              num(want,"hedge_proceeds_quote"),base);
        close(pos.hedge_entry_estimated_net_bps.unwrap(),
              num(want,"hedge_entry_bps"),base);
        let (event,marked)=m.observe_hedged_exit(base,
                 &book(&b["cover_spot"]),&book(&b["cover_future"]),cover_ns)
                 .expect("modeled cover evaluation");
        assert_eq!(event,PaperEvent::TakeProfitFuturesCover);
        close(marked.unwrap(),num(want,"cover_mark_quote"),base);
        let PaperPhase::AwaitingSpotExit{cover_quote,close_estimate_quote,..}=m.phase else{
            panic!("{base}: futures cover did not precede spot sell");
        };
        close(cover_quote,num(want,"cover_quote"),base);
        close(close_estimate_quote,num(want,"cover_mark_quote"),base);
        assert_eq!(m.observe_spot_exit(base,&book(&b["cover_spot"]),cover_ns),
                   Ok(PaperEvent::NoAction));
        assert_eq!(m.observe_spot_exit(base,&book(&b["exit_spot"]),exit_ns),
                   Ok(PaperEvent::SpotExitComplete));
        let PaperPhase::Closed(ref closed)=m.phase else{
            panic!("{base}: modeled spot sale missing");
        };
        close(closed.spot_paid_quote,num(want,"spot_paid_quote"),base);
        close(closed.futures_short_quote,num(want,"hedge_proceeds_quote"),base);
        close(closed.futures_cover_quote,num(want,"cover_quote"),base);
        close(closed.spot_sell_quote,num(want,"spot_sell_quote"),base);
        close(closed.assumed_other_fees_quote,num(want,"fee_quote"),base);
        close(closed.modeled_net_quote,num(want,"modeled_net_quote"),base);
        close(closed.modeled_net_bps,num(want,"modeled_net_bps"),base);
        close(closed.close_decision_estimated_quote,num(want,"cover_mark_quote"),base);
        assert_eq!(closed.spot_buy_ns,entry_ns);
        assert_eq!(closed.futures_short_ns,hedge_ns);
        assert_eq!(closed.futures_cover_ns,cover_ns);
        assert_eq!(closed.spot_sell_ns,exit_ns);
        assert_eq!(closed.modeled_net_quote>0.0,
                   e["scenario"]=="synthetic_profit");
        assert!(closed.close_decision_estimated_quote>0.0,
                "positive cover forecast is not a guaranteed modeled outcome");
    }
}
#[test]
fn invalid_inputs_cannot_be_mislabeled_as_four_leg_fills(){
    let f=oracle();let e=&f["cases"][0];
    let b=&e["books"];
    let mut m=machine(&f["model"]);
    let stale_time=1_800_000_001_u64;
    assert_eq!(m.observe_entry("SPACE",&book(&b["entry_spot"]),
                  &book(&b["entry_future"]),stale_time),
               Err(crate::native_paper_market_execution_review::PaperError::StaleBook));
    assert!(matches!(m.phase,PaperPhase::Flat));
    let mut dry=book(&b["entry_spot"]);
    dry.asks.truncate(1);
    dry.asks[0].1=0.00001;
    assert_eq!(m.observe_entry("SPACE",&dry,&book(&b["entry_future"]),1_000_000_000),
        Err(crate::native_paper_market_execution_review::PaperError::InsufficientDepth));
    assert!(matches!(m.phase,PaperPhase::Flat));
}
