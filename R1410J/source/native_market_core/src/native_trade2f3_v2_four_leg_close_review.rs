//! Astra C5/C6/B4: offline ONLY fee-adjusted four-leg close review.
 //! Links OKX BASE-fee BUY -> Binance SELL SHORT -> Binance BUY COVER ->
 //! OKX SELL to one V2 owner; NEVER claims live flatness or final PnL.
 use std::collections::BTreeSet;
 use serde::{Serialize,Deserialize};
 use crate::{
     fill_economics::SCALE_12,
     native_trade2f3_v2_versioned_store::{V2ReviewPhase,V2ReviewState},
     native_trade2f3_v2_c3_futures_evidence::{
         inspect_v2_c3_futures_observation,V2FuturesObservationKind,
     },
 };
 #[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
 #[serde(deny_unknown_fields)]
 pub struct V2SpotSellFill {
     pub trade_id:String,pub bill_id:String,pub order_id:String,
     pub symbol:String,pub side:String,pub occurred_at_ms:u64,
     pub base_qty_atoms_e12:u128,pub price_quote_atoms_e12:u128,
     pub fee_currency:String,pub signed_fee_quote_atoms_e12:i128,
     pub base_balance_delta_e12:i128,pub base_balance_after_e12:u128,
 }
 #[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
 #[serde(deny_unknown_fields)]
 pub struct V2FuturesCoverFill {
     pub trade_id:String,pub order_id:String,pub symbol:String,
     pub side:String,pub position_side:String,pub occurred_at_ms:u64,
     pub base_qty_atoms_e12:u128,pub price_quote_atoms_e12:u128,
     pub fee_currency:String,pub signed_fee_quote_atoms_e12:i128,
     pub realized_pnl_quote_atoms_e12:i128,
 }
 #[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
 #[serde(deny_unknown_fields)]
 pub struct V2CloseFlatObservation {
     pub captured_at_ms:u64,pub futures_symbol:String,
     pub short_qty_atoms_e12:u128,
     pub claimed_prior_short_atoms_e12:u128,
 }
 #[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
 #[serde(deny_unknown_fields)]
 pub struct V2FourLegCloseObservation {
     pub source_original_generation:u64,
     pub source_v2_revision:u64,
     pub futures_cover_intent_id:String,pub futures_cover_client_id:String,
     pub futures_cover_order_id:String,
     pub spot_sell_intent_id:String,pub spot_sell_client_id:String,
     pub spot_sell_order_id:String,
     pub window_from_ms:u64,pub window_through_ms:u64,
     pub captured_at_ms:u64,
     pub spot_sell_rows:Vec<V2SpotSellFill>,
     pub futures_cover_rows:Vec<V2FuturesCoverFill>,
     pub first_futures_flat:V2CloseFlatObservation,
     pub second_futures_flat:V2CloseFlatObservation,
     pub spot_bill_window_complete:bool,pub spot_terminal_page_seen:bool,
     pub futures_trade_window_complete:bool,pub futures_terminal_page_seen:bool,
     pub both_closing_orders_terminal:bool,
     pub independent_account_history_authenticated:bool,
     pub late_funding_and_transfers_complete:bool,
     pub trading_or_verified_closed_authorized:bool,
 }
 #[derive(Clone,Debug,PartialEq,Eq)]
 pub enum V2FourLegError{Denied(&'static str),Overflow}
 #[derive(Clone,Debug,PartialEq,Eq)]
 pub struct V2FourLegPreview{
     pub original_spot_buy_gross_e12:u128,pub original_base_fee_e12:u128,
     pub owned_net_base_e12:u128,pub binance_short_open_e12:u128,
     pub binance_short_cover_e12:u128,pub okx_spot_sell_e12:u128,
     pub local_spot_base_remaining_e12:u128,pub local_future_short_remaining_e12:u128,
     pub trade_only_pnl_before_funding_e12:i128,
     pub definitive_lifetime_net_pnl_e12:Option<i128>,
     pub independently_verified_flat:bool,
     pub may_send_retry_or_verified_closed:bool,
 }
 fn bad(s:&'static str)->V2FourLegError{V2FourLegError::Denied(s)}
 fn canonical_id(s:&str)->bool{
     s.parse::<u128>().ok().filter(|v|*v>0).map_or(false,|v|v.to_string()==s)
 }
 fn notional(q:u128,p:u128)->Result<i128,V2FourLegError>{
     let raw=q.checked_mul(p).ok_or(V2FourLegError::Overflow)?;
     if raw%SCALE_12!=0{return Err(bad("unrepresentable sub-e12 quote notional"))}
     i128::try_from(raw/SCALE_12).map_err(|_|V2FourLegError::Overflow)
 }
 fn sum(a:i128,b:i128)->Result<i128,V2FourLegError>{
     a.checked_add(b).ok_or(V2FourLegError::Overflow)
 }
 /// A positive review means ONLY locally consistent caller-supplied raw-like
 /// fills/bills. It cannot prove exchange signature, isolated account history,
 /// external rollback floor, absent late trade or definitive funding/PnL.
 pub fn inspect_v2_four_leg_close(
     owner:&V2ReviewState,obs:&V2FourLegCloseObservation,
 )->Result<V2FourLegPreview,V2FourLegError>{
     // Caller must supply the previously validated immutable revision 4.
     // This function does not recursively validate a revision-5 owner.
     if owner.revision!=4||owner.phase!=V2ReviewPhase::FuturesEvidenceRecorded
         ||owner.close_four_leg.is_some()
         ||obs.source_original_generation!=owner.genesis.original_owner_generation
         ||obs.source_v2_revision!=4
         ||obs.trading_or_verified_closed_authorized
         ||obs.independent_account_history_authenticated
         ||obs.late_funding_and_transfers_complete{
         return Err(bad("source owner revision or trading/attestation authority invalid"))
     }
     let c3=owner.c3_futures.as_ref().ok_or(bad("C3 absent"))?;
     let c3r=inspect_v2_c3_futures_observation(&owner.genesis,c3)
         .map_err(|_|bad("original futures C3 invalid"))?;
     if c3.kind!=V2FuturesObservationKind::ExactTargetShort
         ||c3r.observed_short_atoms_e12!=Some(owner.genesis.planned_net_short_atoms_e12)
         ||c3r.remaining_exposure_atoms_e12!=Some(0){
         return Err(bad("C3 partial or UNKNOWN cannot prove normal four-leg cycle"))
     }
     let original=&owner.genesis.checkpoint;
     let source_gross=original.source_shaped_fee_fills.iter().try_fold(0u128,|acc,x|
         acc.checked_add(x.gross_base_qty_e12).ok_or(V2FourLegError::Overflow))?;
     let net=owner.genesis.planned_net_short_atoms_e12;
     if source_gross<net{return Err(bad("original BASE fee/qty invalid"))}
     if obs.futures_cover_intent_id.is_empty()||obs.futures_cover_client_id.is_empty()
         ||obs.spot_sell_intent_id.is_empty()||obs.spot_sell_client_id.is_empty()
         ||!canonical_id(&obs.futures_cover_order_id)
         ||!canonical_id(&obs.spot_sell_order_id)
         ||obs.futures_cover_intent_id==owner.genesis.futures_intent_id
         ||obs.futures_cover_client_id==owner.genesis.futures_client_id
         ||obs.spot_sell_intent_id==original.original.spot_intent_id
         ||obs.spot_sell_client_id==original.original.spot_client_id
         ||obs.spot_sell_order_id==original.original.spot_order_id
         ||c3.order_id.as_deref()==Some(obs.futures_cover_order_id.as_str())
         ||obs.futures_cover_client_id==obs.spot_sell_client_id
         ||obs.futures_cover_intent_id==obs.spot_sell_intent_id{
         return Err(bad("closing identities reused or absent"))
     }
     if obs.window_from_ms<c3.captured_at_ms
         ||obs.window_from_ms>obs.window_through_ms
         ||obs.window_through_ms!=obs.captured_at_ms
         ||obs.window_through_ms.saturating_sub(obs.window_from_ms)>7*24*60*60*1000
         ||!obs.spot_bill_window_complete||!obs.spot_terminal_page_seen
         ||!obs.futures_trade_window_complete||!obs.futures_terminal_page_seen
         ||!obs.both_closing_orders_terminal
         ||obs.spot_sell_rows.is_empty()||obs.futures_cover_rows.is_empty()
         ||obs.spot_sell_rows.len()>128||obs.futures_cover_rows.len()>128{
         return Err(bad("bounded close windows or terminal evidence incomplete"))
     }
     let mut used=BTreeSet::new();
     for r in &original.source_shaped_fee_fills{used.insert(("OKX_TRADE",r.trade_id.as_str().to_owned()));}
     for id in &original.original.fill_bill_ids{used.insert(("OKX_BILL",id.clone()));}
     for r in &c3.rows{used.insert(("FUT_TRADE",r.trade_id.clone()));}
     let base=&original.plan.base;
     let spot_symbol=original.plan.spot_symbol.replace('/',"-");
     let mut owned=net;
     let mut spot_sold=0u128;
     let mut spot_income=0i128;
     let mut closing_fee=0i128;
     let mut spot_time=0u64;
     for r in &obs.spot_sell_rows{
         if !canonical_id(&r.trade_id)||!canonical_id(&r.bill_id)
             ||!used.insert(("OKX_TRADE",r.trade_id.clone()))
             ||!used.insert(("OKX_BILL",r.bill_id.clone()))
             ||r.order_id!=obs.spot_sell_order_id
             ||r.symbol!=spot_symbol||r.side!="sell"
             ||r.fee_currency!="USDT"||r.signed_fee_quote_atoms_e12>0
             ||r.base_qty_atoms_e12==0||r.price_quote_atoms_e12==0
             ||r.occurred_at_ms<obs.window_from_ms||r.occurred_at_ms>obs.window_through_ms
             ||r.occurred_at_ms<spot_time {
             return Err(bad("OKX closing trade/bill/fee/identity invalid"))
         }
         spot_time=r.occurred_at_ms;
         let d=i128::try_from(r.base_qty_atoms_e12).map_err(|_|V2FourLegError::Overflow)?;
         if r.base_balance_delta_e12!=-d||owned<r.base_qty_atoms_e12{
             return Err(bad("OKX sell consumes foreign or insufficient original BASE"))
         }
         owned-=r.base_qty_atoms_e12;
         if owned!=r.base_balance_after_e12{
             return Err(bad("OKX after-bill BASE balance mismatch"))
         }
         spot_sold=spot_sold.checked_add(r.base_qty_atoms_e12).ok_or(V2FourLegError::Overflow)?;
         spot_income=sum(spot_income,notional(r.base_qty_atoms_e12,r.price_quote_atoms_e12)?)?;
         closing_fee=sum(closing_fee,r.signed_fee_quote_atoms_e12)?;
     }
     let mut cover_qty=0u128;
     let mut cover_cost=0i128;
     let mut cover_time=0u64;
     let mut realized=0i128;
     for r in &obs.futures_cover_rows{
         if !canonical_id(&r.trade_id)||!used.insert(("FUT_TRADE",r.trade_id.clone()))
             ||r.order_id!=obs.futures_cover_order_id
             ||r.symbol!=original.plan.futures_symbol
             ||r.side!="BUY"||r.position_side!="BOTH"
             ||r.fee_currency!="USDT"||r.signed_fee_quote_atoms_e12>0
             ||r.base_qty_atoms_e12==0||r.price_quote_atoms_e12==0
             ||r.occurred_at_ms<obs.window_from_ms||r.occurred_at_ms>obs.window_through_ms
             ||r.occurred_at_ms<cover_time {
             return Err(bad("Binance cover trade/fee/identity invalid"))
         }
         cover_time=r.occurred_at_ms;
         cover_qty=cover_qty.checked_add(r.base_qty_atoms_e12).ok_or(V2FourLegError::Overflow)?;
         if cover_qty>net{return Err(bad("futures cover overfilled"))}
         cover_cost=sum(cover_cost,notional(r.base_qty_atoms_e12,r.price_quote_atoms_e12)?)?;
         closing_fee=sum(closing_fee,r.signed_fee_quote_atoms_e12)?;
         realized=sum(realized,r.realized_pnl_quote_atoms_e12)?;
     }
     let c3_last=c3.rows.iter().map(|r|r.occurred_at_ms).max()
         .ok_or(bad("missing futures open fill"))?;
     if c3_last>=cover_time||cover_time>=spot_time{
         return Err(bad("ordinary closing sequence must be SHORT then COVER then SPOT SELL"))
     }
     if spot_sold!=net||cover_qty!=net||owned!=0{
         return Err(bad("incomplete four-leg fee-adjusted close"))
     }
     let o=&obs;
     let first=&o.first_futures_flat;
     let second=&o.second_futures_flat;
     if first.futures_symbol!=original.plan.futures_symbol
         ||second.futures_symbol!=original.plan.futures_symbol
         ||first.claimed_prior_short_atoms_e12!=net
         ||second.claimed_prior_short_atoms_e12!=net
         ||first.short_qty_atoms_e12!=0||second.short_qty_atoms_e12!=0
         ||first.captured_at_ms<cover_time
         ||first.captured_at_ms>=second.captured_at_ms
         ||second.captured_at_ms>obs.captured_at_ms {
         return Err(bad("two bounded futures flat snapshots inconsistent"))
     }
     let mut short_open=0i128;
     let mut short_fee=0i128;
     for r in &c3.rows{
         short_open=sum(short_open,notional(r.base_qty_atoms_e12,r.price_quote_atoms_e12)?)?;
         short_fee=sum(short_fee,r.signed_fee_quote_atoms_e12)?;
     }
     let recomputed_futures=short_open.checked_sub(cover_cost).ok_or(V2FourLegError::Overflow)?;
     if realized!=recomputed_futures{return Err(bad("Binance realized PnL differs from exact short/cover fills"))}
     let buy_spent:u128=original.quote_spent_atoms_e12.parse()
         .map_err(|_|bad("original OKX quote spend malformed"))?;
     let buy_spent=i128::try_from(buy_spent).map_err(|_|V2FourLegError::Overflow)?;
     let pnl=spot_income.checked_sub(buy_spent)
         .and_then(|x|x.checked_add(recomputed_futures))
         .and_then(|x|x.checked_add(closing_fee))
         .and_then(|x|x.checked_add(short_fee))
         .ok_or(V2FourLegError::Overflow)?;
     let _=base; // explicitly counted in original BASE fee cash ancestry.
     Ok(V2FourLegPreview{
         original_spot_buy_gross_e12:source_gross,original_base_fee_e12:source_gross-net,
         owned_net_base_e12:net,binance_short_open_e12:net,
         binance_short_cover_e12:cover_qty,okx_spot_sell_e12:spot_sold,
         local_spot_base_remaining_e12:owned,local_future_short_remaining_e12:net-cover_qty,
         trade_only_pnl_before_funding_e12:pnl,definitive_lifetime_net_pnl_e12:None,
         independently_verified_flat:false,may_send_retry_or_verified_closed:false,
     })
 }
