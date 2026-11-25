//! Zero-network one-position paper replay of authentic previously captured
//! PUBLIC Binance and OKX orderbook messages. No HTTP, credentials, trade API
//! or invented sleep. Quote/fee/fill assumptions are NOT actual exchange fills.
use serde_json::json;
use spotarb_native_market_core::{
    RawFrame,Owner,CoreError,HistorySink,Update,Venue,parse,
    native_paper_market_execution_review::{
        PaperMachine,PaperAssumptions,PaperPhase,PaperEvent,PaperError,
    }
};
use std::{fs::File,io::{BufRead,BufReader},collections::BTreeMap};
struct NoHistory;
impl HistorySink for NoHistory {
    fn accept(&mut self,_:&RawFrame,_:&Update)->Result<(),CoreError>{Ok(())}
}
fn main()->Result<(),Box<dyn std::error::Error>> {
    let argv:Vec<_>=std::env::args().collect();
    if argv.len()!=2 {return Err("usage: native_paper_capture_replay <recorded-public-frames.jsonl>".into())}
    let file=BufReader::new(File::open(&argv[1])?);
    let mut owner=Owner::new(0,1,800).map_err(|e|format!("owner init: {e:?}"))?;
    let config=PaperAssumptions::default();
    let mut paper=PaperMachine::new(config.clone()).map_err(|e|format!("{e:?}"))?;
    let mut frames=0usize;
    let mut parse_or_ingest_errors=0usize;
    let mut paper_liquidity_failures=0usize;
    let mut paper_stale_denials=0usize;
    let mut new_spot=0usize;
    let mut short=0usize;
    let mut futures_cover=0usize;
    let mut realized=0usize;
    let mut negative_marks=0usize;
    let mut positive_marks=0usize;
    let mut max_negative_mark:Option<f64>=None;
    let mut reasons:BTreeMap<&'static str,usize>=BTreeMap::new();
    let mut last_ns=0u64;
    let mut raw_clock_inversions=0usize;
    for line in file.lines(){
        let line=line?;
        frames+=1;
        let frame:RawFrame=serde_json::from_str(&line)?;
        // Preserve EXACT capture order. Concurrent WS shards can enqueue a
        // physically earlier timestamp later. Never reorder or look ahead.
        if frame.received_ns<last_ns{raw_clock_inversions+=1}
        let now=last_ns.max(frame.received_ns);
        last_ns=now;
        let venue=frame.venue;
        let update=match parse(&frame){
            Ok(v)=>v,Err(_)=>{parse_or_ingest_errors+=1;continue}
        };
        let base=update.base.clone();
        let fresh_pair=match owner.ingest_parsed(&frame,update,&mut NoHistory){
            Ok(v)=>v,Err(_)=>{parse_or_ingest_errors+=1;continue}
        };
        let current=paper.phase.clone();
        let result=match current {
            PaperPhase::Flat=>{
                if let Some(ref snapshot)=fresh_pair{
                    paper.observe_entry(&base,&snapshot.spot,&snapshot.future,now)
                        .map(|e|(e,None))
                }else{Ok((PaperEvent::NoAction,None))}
            }
            PaperPhase::AwaitingHedge(pos) if pos.base==base
               &&venue==Venue::BinanceFutures=>{
                if let Some(book)=owner.snapshot_venue(&base,venue){
                    paper.observe_future_short(&base,&book,now)
                        .map(|e|(e,None))
                }else{Ok((PaperEvent::NoAction,None))}
            }
            PaperPhase::Hedged(pos) if pos.base==base=>{
                if let Some(ref snapshot)=fresh_pair{
                    paper.observe_hedged_exit(&base,&snapshot.spot,&snapshot.future,now)
                }else{Ok((PaperEvent::NoAction,None))}
            }
            PaperPhase::AwaitingSpotExit{pos,..} if pos.base==base
               &&venue==Venue::OkxSpot=>{
                if let Some(book)=owner.snapshot_venue(&base,venue){
                    paper.observe_spot_exit(&base,&book,now).map(|e|(e,None))
                }else{Ok((PaperEvent::NoAction,None))}
            }
            _=>Ok((PaperEvent::NoAction,None)),
        };
        match result{
            Ok((event,mark))=>{
                if let Some(m)=mark{
                    if m<=0.0{
                        negative_marks+=1;
                        max_negative_mark=Some(max_negative_mark.map_or(m,|x|x.min(m)));
                    }else{positive_marks+=1}
                }
                match event{
                    PaperEvent::SignalSpotBuy=>new_spot+=1,
                    PaperEvent::FutureShort=>short+=1,
                    PaperEvent::TakeProfitFuturesCover=>futures_cover+=1,
                    PaperEvent::SpotExitComplete=>realized+=1,
                    _=>{},
                }
            }
            Err(PaperError::StaleBook)=>paper_stale_denials+=1,
            Err(PaperError::InsufficientDepth)=>paper_liquidity_failures+=1,
            Err(e)=>return Err(format!("paper invariant denied at frame {frames}: {e:?}").into()),
        }
    }
    let (status,record)=match &paper.phase {
        PaperPhase::Flat=>("FLAT_NO_PAPER_OPEN",json!(null)),
        PaperPhase::AwaitingHedge(p)=>("UNHEDGED_PAPER_EXPOSURE",json!(p)),
        PaperPhase::Hedged(p)=>("HEDGED_PAPER_OPEN",json!(p)),
        PaperPhase::AwaitingSpotExit{pos,cover_quote,cover_ns,close_estimate_quote}=>(
            "UNHEDGED_PAPER_EXIT_PENDING",json!({"position":pos,"cover_quote":cover_quote,
                "cover_ns":cover_ns,"close_estimate_quote":close_estimate_quote})),
        PaperPhase::Closed(p)=>("PAPER_CLOSED",json!(p)),
    };
    if new_spot==0{*reasons.entry("no_open_signal_qualified").or_default()+=1}
    let output=json!({
        "report":"NATIVE_EVENT_DRIVEN_PUBLIC_PAPER_REVIEW",
        "offline":true,"real_orders":0,"paper_fill_attestation":false,
        "public_recorded_market_source":argv[1],
        "no_artificial_latency_or_sleep":true,
        "single_paper_position_max":1,
        "venue_lot_step_min_notional_validated":false,
        "exchange_matching_or_queue_priority_proven":false,
        "result_not_an_executable_order_or_verified_pnl":true,
        "assumptions":{
            "quote_budget_usdt":config.quote_budget,
            "min_entry_net_bps":config.min_entry_net_bps,
            "other_roundtrip_cost_bps":config.other_cost_bps,
            "spot_buy_fee_base_bps":config.spot_buy_fee_base_bps,
            "take_profit_quote":config.take_profit_quote,
            "note":"scenario costs, not live exchange fee/position truth"
        },
        "frames":frames,"raw_cross_shard_clock_inversions":raw_clock_inversions,
        "parse_or_ingest_errors":parse_or_ingest_errors,
        "paper_stale_denials":paper_stale_denials,
        "paper_liquidity_failures":paper_liquidity_failures,
        "spot_open_count":new_spot,"futures_short_count":short,
        "futures_cover_count":futures_cover,"spot_exit_count":realized,
        "negative_hedged_exit_marks_held":negative_marks,
        "positive_hedged_exit_marks":positive_marks,
        "worst_hedged_exit_mark_quote":max_negative_mark,
        "status":status,"record":record,"notes":reasons,
        "trading_authorized":false,
    });
    println!("{}",serde_json::to_string_pretty(&output)?);
    if parse_or_ingest_errors>0{return Err("capture parsing or input continuity invalid".into())}
    Ok(())
}
