//! TRADE2D1: never-executable full-inventory recovery close BLUEPRINT.
//! A partial opening may need THREE economic actions: cover original short,
//! sell original spot matching that short, AND sell unmatched additional spot.
//! They are modelled as ONE indicative total spot SELL and one futures BUY
//! reduceOnly, but are NOT exchange requests or safe automatic sequencing.
//! Read-only TRADE2C 50 fake-able signed GET prerequisite, no real POST.
use std::collections::BTreeSet;
use crate::{
 authoritative_state::{NativeStateStore,NativeStateError},
 fill_economics::{decimal_to_atoms,SCALE_12},
 market_metadata::NativeMarketMetadata,
 native_supervised_recovery::{RecoveryTriage,MAX_RECOVERY_SOURCE_AGE_MS},
 native_supervised_recovery_fills::{
   audit_fresh_supervised_recovery_fills_once,Trade2CFillError,
 },
 private_https::HttpExecutor,
 private_rest::{BinanceCredentials,OkxCredentials,OkxDomain},
};
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum ClosePlanError{
 Source(Trade2CFillError),State(NativeStateError),
 Mismatch,MissingFilters,Expired,Untradeable,Overflow,Unsupported,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct RecoveryFullCloseBlueprint{
    pub source_native_generation:u64,
    pub source_identity_sha256:String,
    pub original_okx_spot_client_id:String,
    pub original_binance_futures_client_id:String,
    pub base:String,
    pub spot_symbol:String,
    pub futures_symbol:String,
    pub total_spot_to_sell_e12:u128,
    pub matched_spot_e12:u128,
    pub unmatched_spot_to_remove_e12:u128,
    pub binance_short_to_buy_reduce_only_e12:u128,
    pub estimated_total_spot_sale_usdt_e12:u128,
    pub estimated_binance_cover_usdt_e12:u128,
    pub exceeds_ten_usdt_in_one_leg:bool,
    pub split_unmatched_spot_leg_feasible:bool,
    pub operator_must_select_order_sequence:bool,
    pub fresh_position_and_order_pre_send_recheck_required:bool,
    pub all_recovery_order_clients_durable_before_send:bool,
    pub simulated_not_real:bool,
    pub may_create_signed_order:bool,
    pub may_send_order:bool,
    pub may_auto_close:bool,
    pub may_verify_closed:bool,
}
fn dec(s:Option<&str>)->Result<u128,ClosePlanError>{
    let x=decimal_to_atoms(s.ok_or(ClosePlanError::MissingFilters)?)
        .map_err(|_|ClosePlanError::MissingFilters)?;
    if x==0{Err(ClosePlanError::MissingFilters)}else{Ok(x)}
}
fn amount(q:u128,p:u128)->Result<u128,ClosePlanError>{
    Ok(q.checked_mul(p).ok_or(ClosePlanError::Overflow)?/SCALE_12)
}
fn complete_market(m:&NativeMarketMetadata,now:u64)->Result<(),ClosePlanError>{
    if now<m.loaded_wall_ms||m.loaded_wall_ms==0||m.loaded_ns==0
        ||now-m.loaded_wall_ms>MAX_RECOVERY_SOURCE_AGE_MS{
        return Err(ClosePlanError::Expired)
    }
    if m.static_reason.is_some()||m.metadata_state()!="SIZE_METADATA_READY"
        ||m.spot_status!="live"||m.future_status!="TRADING"
        ||m.contract_size!="1"{
        return Err(ClosePlanError::Unsupported)
    }
    Ok(())
}
/// Performs a FULL new TRADE2C signed-GET mockable audit itself. A caller
/// cannot pass a fabricated "PASS" report as trade authority. The result
/// is an untrusted offline advisory: never translate directly into a POST.
#[allow(clippy::too_many_arguments)]
pub fn preview_full_inventory_close<E:HttpExecutor>(
    http:&E,store:&NativeStateStore,gen:u64,market:&NativeMarketMetadata,
    now:u64,spot_bid_e12:u128,futures_ask_e12:u128,
    okx:&OkxCredentials,bn:&BinanceCredentials,domain:OkxDomain,recv:u64,
)->Result<RecoveryFullCloseBlueprint,ClosePlanError>{
    complete_market(market,now)?;
    if spot_bid_e12==0||futures_ask_e12==0{return Err(ClosePlanError::MissingFilters)}
    let spot_tick=dec(market.spot_tick.as_deref())?;
    let future_tick=dec(market.future_tick.as_deref())?;
    if spot_bid_e12%spot_tick!=0||futures_ask_e12%future_tick!=0{
        return Err(ClosePlanError::Untradeable)
    }
    let audit=audit_fresh_supervised_recovery_fills_once(
        http,store,gen,market,spot_bid_e12,okx,bn,now,domain,recv)
        .map_err(ClosePlanError::Source)?;
    if audit.before!=audit.after
        ||!audit.every_original_fill_price_qty_fee_time_matches
        ||audit.before.previous.may_send_order
        ||audit.before.previous.may_auto_unwind
        ||audit.may_send_order||audit.may_auto_unwind||audit.may_verify_closed{
        return Err(ClosePlanError::Mismatch)
    }
    let parent=&audit.before.previous;
    let spot_total=audit.after.observed_net_available_spot_e12;
    let futures=audit.after.observed_futures_short_e12;
    let residual=parent.unhedged_spot_e12;
    if parent.native_generation!=gen
        ||market.base.is_empty()||spot_total==0||futures==0
        ||futures!=parent.observed_short_e12
        ||spot_total!=parent.observed_short_e12.checked_add(residual)
            .ok_or(ClosePlanError::Overflow)?
        ||audit.after.original_spot_order_filled_e12!=spot_total
        ||audit.after.original_futures_order_filled_e12!=futures
        ||(residual==0&&parent.triage!=RecoveryTriage::EqualLegsManualCloseReview)
        ||(residual>0&&!matches!(parent.triage,
            RecoveryTriage::PartialSpotReduceOperatorOnly
            |RecoveryTriage::SmallOrOffGridResidualManual)){
        return Err(ClosePlanError::Mismatch)
    }
    let spot_step=dec(market.spot_step.as_deref())?;
    let future_step=dec(market.future_step_base.as_deref())?;
    let spot_min=dec(market.spot_min_amount.as_deref())?;
    let future_min=dec(market.future_min_base.as_deref())?;
    let spot_min_cost=decimal_to_atoms(&market.spot_min_cost)
        .map_err(|_|ClosePlanError::MissingFilters)?;
    let future_min_cost=decimal_to_atoms(&market.future_min_cost)
        .map_err(|_|ClosePlanError::MissingFilters)?;
    if spot_total%spot_step!=0||futures%future_step!=0
        ||spot_total<spot_min||futures<future_min{
        return Err(ClosePlanError::Untradeable)
    }
    if market.spot_max_amount.as_ref().map(|s|dec(Some(s))
        .map(|max|spot_total>max)).transpose()?.unwrap_or(false)
        ||market.future_max_base.as_ref().map(|s|dec(Some(s))
            .map(|max|futures>max)).transpose()?.unwrap_or(false){
        return Err(ClosePlanError::Untradeable)
    }
    let estimated_spot=amount(spot_total,spot_bid_e12)?;
    let estimated_future=amount(futures,futures_ask_e12)?;
    if estimated_spot<spot_min_cost||estimated_future<future_min_cost{
        return Err(ClosePlanError::Untradeable)
    }
    let split=if residual==0{true}else{
        let separate_notional=amount(residual,spot_bid_e12)?;
        residual>=spot_min&&residual%spot_step==0
            &&separate_notional>=spot_min_cost
            &&market.spot_max_amount.as_ref().map(|s|dec(Some(s))
                .map(|max|residual<=max)).transpose()?.unwrap_or(true)
    };
    let fresh=store.load().map_err(ClosePlanError::State)?
        .ok_or(ClosePlanError::State(NativeStateError::Generation))?;
    if fresh.generation!=gen||fresh.active.is_some()
        ||fresh.entry.as_ref().map(|e|e.plan.base.as_str())!=Some(market.base.as_str())
        ||fresh.entry.as_ref().map(|e|e.plan.spot_symbol.replace('/',"-"))
            !=Some(market.spot_symbol.clone())
        ||fresh.entry.as_ref().map(|e|e.plan.futures_symbol.as_str())
            !=Some(market.future_symbol.as_str()){
        return Err(ClosePlanError::Mismatch)
    }
    Ok(RecoveryFullCloseBlueprint{
        source_native_generation:gen,
        source_identity_sha256:parent.original_source_sha256.clone(),
        original_okx_spot_client_id:parent.original_spot_client_id.clone(),
        original_binance_futures_client_id:parent.original_futures_client_id.clone(),
        base:market.base.clone(),spot_symbol:market.spot_symbol.clone(),
        futures_symbol:market.future_symbol.clone(),
        total_spot_to_sell_e12:spot_total,
        matched_spot_e12:futures,
        unmatched_spot_to_remove_e12:residual,
        binance_short_to_buy_reduce_only_e12:futures,
        estimated_total_spot_sale_usdt_e12:estimated_spot,
        estimated_binance_cover_usdt_e12:estimated_future,
        exceeds_ten_usdt_in_one_leg:estimated_spot>10*SCALE_12
            ||estimated_future>10*SCALE_12,
        split_unmatched_spot_leg_feasible:split,
        operator_must_select_order_sequence:true,
        fresh_position_and_order_pre_send_recheck_required:true,
        all_recovery_order_clients_durable_before_send:false,
        simulated_not_real:true,
        may_create_signed_order:false,may_send_order:false,
        may_auto_close:false,may_verify_closed:false,
    })
}

#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum HypotheticalCloseLeg{OkxSellBase,BinanceBuyReduceOnly}
/// Simulated hypothetical exchange fill; NEVER authoritative order evidence.
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct HypotheticalFill{
    pub leg:HypotheticalCloseLeg,
    pub trade_id:String,
    pub quantity_e12:u128,
    pub fee_usdt_e12:u128,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct HypotheticalCloseProgress {
    pub spot_sold_e12:u128,pub futures_reduced_e12:u128,
    pub spot_left_e12:u128,pub short_left_e12:u128,
    pub directional_unhedged_spot_e12:i128,
    pub observed_hypothetical_fees_usdt_e12:u128,
    pub hypothetical_flat_only:bool,
    pub actual_exchange_flatness_proven:bool,
    pub may_send_order:bool,pub may_verify_closed:bool,
}
/// A simulation/what-if calculator, NOT a receipt recorder. Strictly
/// unique mocked fill IDs prevent double-counted replay.
pub fn simulate_close_fill_progress(
    plan:&RecoveryFullCloseBlueprint,fills:&[HypotheticalFill]
)->Result<HypotheticalCloseProgress,ClosePlanError>{
    if !plan.simulated_not_real||plan.may_send_order||plan.may_verify_closed
        ||plan.may_create_signed_order||plan.may_auto_close
        ||plan.total_spot_to_sell_e12==0||plan.binance_short_to_buy_reduce_only_e12==0{
        return Err(ClosePlanError::Mismatch)
    }
    let mut ids=BTreeSet::new();
    let (mut spot,mut future,mut fee)=(0u128,0u128,0u128);
    for f in fills{
        if f.trade_id.is_empty()||f.trade_id.len()>128||f.quantity_e12==0
            ||!ids.insert((f.leg as u8,f.trade_id.clone())){
            return Err(ClosePlanError::Mismatch)
        }
        match f.leg{
            HypotheticalCloseLeg::OkxSellBase=>
                spot=spot.checked_add(f.quantity_e12).ok_or(ClosePlanError::Overflow)?,
            HypotheticalCloseLeg::BinanceBuyReduceOnly=>
                future=future.checked_add(f.quantity_e12).ok_or(ClosePlanError::Overflow)?,
        }
        fee=fee.checked_add(f.fee_usdt_e12).ok_or(ClosePlanError::Overflow)?;
    }
    let spot_left=plan.total_spot_to_sell_e12.checked_sub(spot)
        .ok_or(ClosePlanError::Untradeable)?;
    let future_left=plan.binance_short_to_buy_reduce_only_e12.checked_sub(future)
        .ok_or(ClosePlanError::Untradeable)?;
    let net=i128::try_from(spot_left).ok()
        .and_then(|s|i128::try_from(future_left).ok().and_then(|f|s.checked_sub(f)))
        .ok_or(ClosePlanError::Overflow)?;
    Ok(HypotheticalCloseProgress{
        spot_sold_e12:spot,futures_reduced_e12:future,
        spot_left_e12:spot_left,short_left_e12:future_left,
        directional_unhedged_spot_e12:net,
        observed_hypothetical_fees_usdt_e12:fee,
        hypothetical_flat_only:spot_left==0&&future_left==0,
        actual_exchange_flatness_proven:false,
        may_send_order:false,may_verify_closed:false,
    })
}
