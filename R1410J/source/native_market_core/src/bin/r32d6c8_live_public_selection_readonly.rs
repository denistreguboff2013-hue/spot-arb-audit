//! P1-06 public market proof. Exchange-public WebSockets only: NO API keys,
//! account state, order transport, cancellation, subscriptions to private data,
//! safety switches, live paper state, or production config writes.
use futures_util::{SinkExt,StreamExt};
use serde_json::{json,Value};
use spotarb_native_market_core::{owner_index,parse,RawFrame,Update,Venue,HistorySink,Owner,CoreError};
use spotarb_native_market_core::native_trading_config::NativeTradingConfig;
use spotarb_native_market_core::native_public_market_gate_review::{
 parse_public_okx_spot_ticker,inspect_public_spread_gate,PublicOkxSpotTicker};
use spotarb_native_market_core::fill_economics::SCALE_12;
use spotarb_native_market_core::native_observability_review::{
 NativeObservePolicy,OfflineObservationReplayFence,OfflineNativeSample,ObservationKind};
use std::{collections::{BTreeMap,BTreeSet},path::Path,time::{Duration,Instant,SystemTime,UNIX_EPOCH}};
use tokio_tungstenite::{connect_async,tungstenite::Message};

struct RamOnly { accepted:u64 }
impl HistorySink for RamOnly {
 fn accept(&mut self,_:&RawFrame,_:&Update)->Result<(),CoreError>{
  self.accepted+=1;Ok(())
 }
}
fn clock_ms()->i64 { SystemTime::now().duration_since(UNIX_EPOCH)
 .unwrap_or_default().as_millis() as i64 }
fn accept_frame(
 venue:Venue,raw:String,start:Instant,owners:&mut [Owner],
 history:&mut RamOnly,failures:&mut u64,
){
 let frame=RawFrame{source:None,
  venue,epoch:1,received_ns:start.elapsed().as_nanos() as u64+1,
  received_wall_ms:clock_ms(),raw
 };
 match parse(&frame){
  Ok(update)=>{
   let idx=owner_index(&update.base,owners.len());
   if owners[idx].ingest_parsed(&frame,update,history).is_err(){*failures+=1;}
  },
  Err(_)=>*failures+=1,
 }
}
fn die(msg:&str)->!{eprintln!("P106_PUBLIC_READONLY_DENY:{msg}");std::process::exit(2)}
fn bases()->Vec<String>{
 let source=std::env::var("P106_BASES").unwrap_or_else(|_|"BTC,ETH,SOL,XRP".into());
 let mut unique=BTreeSet::new();
 let bases=source.split(',').map(|s|s.trim().to_ascii_uppercase())
  .collect::<Vec<_>>();
 if bases.is_empty()||bases.len()>8||bases.iter().any(|b|
  b.is_empty()||b.len()>16
  ||!b.bytes().all(|c|c.is_ascii_uppercase()||c.is_ascii_digit())
  ||!unique.insert(b.clone())){die("invalid_bases")}
 bases
}
#[tokio::main(flavor="multi_thread",worker_threads=2)]
async fn main(){
 let cfg=NativeTradingConfig::load(&Path::new(env!("CARGO_MANIFEST_DIR"))
  .join("config/native_trading.yaml"))
  .unwrap_or_else(|_|die("review_config_failed_closed"));
 let bs=bases();
 let secs=std::env::var("P106_SECONDS").ok()
  .and_then(|s|s.parse::<u64>().ok()).unwrap_or(18);
 if !(10..=30).contains(&secs){die("duration_outside_10_to_30_sec")}
 let streams=bs.iter().map(|base|
  format!("{}usdt@bookTicker",base.to_ascii_lowercase()))
  .collect::<Vec<_>>().join("/");
 let binance_url=format!("wss://fstream.binance.com/stream?streams={streams}");
 let (mut binance,_)=match tokio::time::timeout(
  Duration::from_secs(7),connect_async(binance_url)).await{
  Ok(Ok(ws))=>ws,_=>die("public_binance_connect_unavailable")
 };
 let (mut okx,_)=match tokio::time::timeout(Duration::from_secs(7),
  connect_async("wss://ws.okx.com:8443/ws/v5/public")).await{
  Ok(Ok(ws))=>ws,_=>die("public_okx_connect_unavailable")
 };
 let args=bs.iter().flat_map(|b|["books5","tickers"].into_iter()
  .map(move |channel|json!({
   "channel":channel,"instId":format!("{b}-USDT")
  }))).collect::<Vec<_>>();
 if okx.send(Message::Text(json!({"op":"subscribe","args":args}).to_string()))
  .await.is_err(){die("public_okx_subscribe_unavailable")}
 let start=Instant::now();
 let deadline=start+Duration::from_secs(secs);
 let mut owners=(0..12).map(|i|Owner::new(i,12,800).unwrap())
  .collect::<Vec<_>>();
 let mut history=RamOnly{accepted:0};
 let (mut okx_frames,mut binance_frames,mut bad_frames,mut controls)=(0u64,0u64,0u64,0u64);
 let mut disconnects=0u64;
 let mut public_transport_errors=Vec::<String>::new();
 let allowed_bases=bs.iter().cloned().collect::<BTreeSet<_>>();
 let mut okx_tickers=BTreeMap::<String,PublicOkxSpotTicker>::new();
 let (mut ticker_frames,mut ticker_invalid)=(0u64,0u64);
 while Instant::now()<deadline {
  tokio::select! {
   biased;
   _=tokio::time::sleep_until(tokio::time::Instant::from_std(deadline))=>break,
   msg=okx.next()=>{
    match msg{
     Some(Ok(Message::Text(raw)))=>{
      if let Ok(v)=serde_json::from_str::<Value>(&raw){
       if let Some(event)=v.get("event").and_then(Value::as_str){
        controls+=1;
        if event=="error"{
         bad_frames+=1;
         public_transport_errors.push(v.to_string().chars().take(240).collect());
        }
        continue;
       }
       match v.get("arg").and_then(|x|x.get("channel")).and_then(Value::as_str){
        Some("tickers")=>{
         ticker_frames+=1;
         match parse_public_okx_spot_ticker(&raw,&allowed_bases,clock_ms() as u64){
          Ok(ticker)=>{
           let replace=okx_tickers.get(&ticker.base).map_or(true,
            |old|ticker.exchange_wall_ms>=old.exchange_wall_ms);
           if replace{okx_tickers.insert(ticker.base.clone(),ticker);}
          },
          Err(_)=>{ticker_invalid+=1;bad_frames+=1;}
         }
         continue;
        },
        Some("books5")=>{},
        _=>{bad_frames+=1;continue;}
       }
      }else{bad_frames+=1;continue;}
      okx_frames+=1;
      accept_frame(Venue::OkxSpot,raw,start,&mut owners,
       &mut history,&mut bad_frames);
     },
     Some(Ok(Message::Ping(payload)))=>{
      if okx.send(Message::Pong(payload)).await.is_err(){disconnects+=1;break;}
     },
     Some(Ok(_))=>{},
     other=>{
      disconnects+=1;
      public_transport_errors.push(
       format!("okx_public_ws:{other:?}").chars().take(240).collect());
      break;
     }
    }
   },
   msg=binance.next()=>{
    match msg{
     Some(Ok(Message::Text(raw)))=>{
      binance_frames+=1;
      accept_frame(Venue::BinanceFutures,raw,start,&mut owners,
       &mut history,&mut bad_frames);
     },
     Some(Ok(Message::Ping(payload)))=>{
      if binance.send(Message::Pong(payload)).await.is_err(){
       disconnects+=1;break;
      }
     },
     Some(Ok(_))=>{},
     other=>{
      disconnects+=1;
      public_transport_errors.push(
       format!("binance_public_ws:{other:?}").chars().take(240).collect());
      break;
     }
    }
   }
  }
 }
 let _=binance.close(None).await;
 let _=okx.close(None).await;
 let now_ns=start.elapsed().as_nanos() as u64+1;
 let mut quotes=Vec::new();
 let mut candidates=Vec::new();
 let mut qualified_local_candidates=Vec::new();
 let mut public_market_gate_checks=Vec::new();
 let mut stale=Vec::new();
 let observe_policy=NativeObservePolicy::from_config(&cfg)
  .unwrap_or_else(|_|die("rust_observability_config_denied"));
 let mut preview_fence=OfflineObservationReplayFence::new(&observe_policy);
 let now_wall=clock_ms() as u64;
 for base in &bs{
  let idx=owner_index(base,owners.len());
  let (spot,fut)=owners[idx].snapshot_pair(base);
  let (Some(spot),Some(fut))=(spot,fut)else{
   stale.push(json!({"base":base,"reason":"MISSING_PAIRED_BOOK"}));
   continue;
  };
  let age_spot=now_ns.saturating_sub(spot.received_ns)/1_000_000;
  let age_fut=now_ns.saturating_sub(fut.received_ns)/1_000_000;
  if age_spot>800||age_fut>800{
   stale.push(json!({"base":base,"reason":"BOOK_OVER_800MS",
    "spot_age_ms":age_spot,"futures_age_ms":age_fut}));
   continue;
  }
  let (Some(&(ask,_)),Some(&(bid,_)))=(spot.asks.first(),fut.bids.first())
  else{
   stale.push(json!({"base":base,"reason":"MISSING_BEST_BID_ASK"}));
   continue;
  };
  if !ask.is_finite()||!bid.is_finite()||ask<=0.0||bid<=0.0{
   stale.push(json!({"base":base,"reason":"NONFINITE_OR_ZERO_PRICE"}));
   continue;
  }
  let gross=(bid/ask-1.0)*10_000.0;
  if !gross.is_finite()||gross.abs()>20_000.0{
   stale.push(json!({"base":base,"reason":"SPREAD_OUT_OF_DIAGNOSTIC_BOUNDS"}));
   continue;
  }
  let scaled=(gross*1000.0).round() as i64;
  candidates.push((base.as_str(),scaled));
  // Owner stores public prices as f64. Round BUY up and SELL down in
  // this untrusted diagnostic, never elevate this into exact fills/PnL.
  let ask_e12=(ask*SCALE_12 as f64).ceil() as u128;
  let bid_e12=(bid*SCALE_12 as f64).floor() as u128;
  let ticker=okx_tickers.get(base);
  let ticker_age_ms=ticker.map(|t|now_wall.saturating_sub(t.exchange_wall_ms));
  let gate=inspect_public_spread_gate(&cfg,ask_e12,bid_e12,
   ticker.map(|t|t.volume_24h_quote_e12),
   age_spot,age_fut,ticker_age_ms)
    .unwrap_or_else(|_|die("public_volume_price_risk_gate_denied"));
  let mut russian_preview=None;
  if gate.local_screen_pass{
   qualified_local_candidates.push((base.as_str(),scaled));
   let sample=OfflineNativeSample{
    observed_ms:now_wall,owner_generation:1,symbol:Some(base.clone()),
    net_spread_milli_bps:Some(gate.indicative_net_spread_milli_bps as u128),
    book_age_ms:age_spot.max(age_fut),decisions:history.accepted,
    ws_to_decision_p99_micros:0,worker_queue_p99_micros:0,
    ws_reconnects:disconnects,worker_drops:bad_frames,
    fault_code:None,
   };
   let preview=preview_fence.inspect_once(&observe_policy,
    ObservationKind::Candidate,&sample,now_wall)
    .unwrap_or_else(|_|die("russian_preview_policy_denied"));
   russian_preview=preview.russian_telegram_preview;
  }
  public_market_gate_checks.push(json!({
   "base":base,"public_ticker_present":ticker.is_some(),
   "public_ticker_age_ms":ticker_age_ms,
   "public_24h_quote_volume_e12":ticker.map(|t|t.volume_24h_quote_e12.to_string()),
   "gross_spread_milli_bps":gate.gross_spread_milli_bps.to_string(),
   "reserved_cost_milli_bps":gate.reserved_cost_milli_bps.to_string(),
   "indicative_net_spread_milli_bps":gate.indicative_net_spread_milli_bps.to_string(),
   "local_screen_pass":gate.local_screen_pass,"reason":gate.reason,
   "source_authenticated":gate.exchange_authenticated,
   "executable_depth_verified":gate.executable_depth_verified,
   "real_fees_and_funding_verified":gate.actual_fee_and_funding_proven,
   "may_trade":gate.may_open_close_cancel_or_resubmit,
   "russian_telegram_preview_not_sent":russian_preview,
   "telegram_delivery":false
  }));
  quotes.push(json!({
   "base":base,"spot_ask_usdt":ask,"futures_bid_usdt":bid,
   "indicative_gross_bps":(gross*1000.0).round()/1000.0,
   "spot_age_ms":age_spot,"futures_age_ms":age_fut,
   "price_source":"public_ws_books5_and_bookTicker",
   "executable_depth_verified":false
  }));
 }
 // The live market data is public, but it is NOT the legacy scanner's
 // independently calculated ranked feed. A locally derived ranking proxy
 // is used for this bounded public-data selector check and labelled so.
 // Keep raw candidate encounter order for the Rust selector to do the
 // stable spread ranking itself; build an independent scanner proxy.
 let mut scanner_rank_proxy=candidates.clone();
 scanner_rank_proxy.sort_by(|a,b|b.1.cmp(&a.1).then_with(||a.0.cmp(b.0)));
 let scanner_leader_proxy=scanner_rank_proxy.iter().map(|(base,_)|*base)
  .collect::<Vec<_>>();
 let actual=cfg.inspect_candidate_and_ws_selection_read_only(
  &candidates,&scanner_leader_proxy,&[])
  .unwrap_or_else(|_|die("live_selection_review_denied"));
 let qualified_leader_proxy=scanner_leader_proxy.iter().copied()
  .filter(|base|qualified_local_candidates.iter().any(|(q,_)|q==base))
  .collect::<Vec<_>>();
 let qualified=cfg.inspect_candidate_and_ws_selection_read_only(
  &qualified_local_candidates,&qualified_leader_proxy,&[])
  .unwrap_or_else(|_|die("qualified_local_selection_denied"));
 // Diagnostic cap only: NOT a config file edit, and ZZZ is simulated
 // already-open owner, NOT fetched from the exchange/account.
 let mut narrow=cfg.clone();
 narrow.policy.top_n_per_scan=2;
 narrow.policy.websocket_top_max=2;
 let probe=narrow.inspect_candidate_and_ws_selection_read_only(
  &candidates,&scanner_leader_proxy,&["ZZZ"])
  .unwrap_or_else(|_|die("bounded_diagnostic_denied"));
 let fresh_count=quotes.len();
 println!("{}",json!({
  "source":"OKX public books5 + Binance Futures public bookTicker",
  "trading":false,"private_api_access":false,
  "production_ws_subscription_mutation":false,
  "exchange_source_authenticated":false,
  "requested_bases":bs,"duration_sec":secs,
  "capture_wall_ms":now_wall,
  "total_ram_accepted_frames":history.accepted,
  "binance_public_frames":binance_frames,"okx_public_frames":okx_frames,
  "control_messages":controls,"bad_frames":bad_frames,
  "public_spot_ticker_frames":ticker_frames,
  "public_spot_ticker_invalid":ticker_invalid,
  "public_spot_ticker_bases":okx_tickers.len(),
  "unexpected_disconnects":disconnects,
  "public_transport_errors":public_transport_errors,
  "fresh_paired_count":quotes.len(),"fresh_pairs":quotes,
  "unusable_pairs":stale,
  "public_market_gate_checks":public_market_gate_checks,
  "qualified_local_public_market_selection":{
   "top_candidates":qualified.top_candidate_bases,
   "ws_target_preview":qualified.ws_target_bases,
   "may_trade":qualified.may_trade,
   "volume_quote_currency":"USDT",
   "net_spread_based_on_configured_cost_reserve":true,
   "depth_and_actual_fees_proven":false
  },
  "legacy_scanner_feed_authenticated":false,
  "scanner_leaders_input":"derived_public_book_spread_proxy_not_legacy_scanner",
  "config_sha256":cfg.sha256,
  "config_top_n":cfg.policy.top_n_per_scan,
  "config_ws_top_max":cfg.policy.websocket_top_max,
  "current_config_selection":{
   "top_candidates":actual.top_candidate_bases,
   "ws_targets":actual.ws_target_bases,
   "may_trade":actual.may_trade
  },
  "diagnostic_override_no_config_write":{
   "candidate_top_n":2,"ws_leader_top_max":2,
   "owner_base":"ZZZ_SIMULATED_NO_ACCOUNT_ACCESS",
   "top_candidates":probe.top_candidate_bases,
   "ws_targets":probe.ws_target_bases,
   "retained_open_position_bases":probe.retained_open_position_bases,
   "may_update_live_ws_subscriptions":probe.may_update_live_ws_subscriptions,
   "may_trade":probe.may_trade
  }
 }));
 if history.accepted==0||okx_frames==0||fresh_count==0||ticker_frames==0
  ||bad_frames>0||ticker_invalid>0||disconnects>0{std::process::exit(3)}
}
