//! TRADE2D2E3A: one OKX SPOT parent -> exactly 2 or 3 ordinary
//! children -> each exact original per-trade fill ID and USDT fee.
//! NON-EXECUTABLE and MANUAL-ONLY, not an E1 veto release.
//! SubAlgoIdList and other possible child generations hard-denied.
use std::collections::BTreeSet;
use crate::{
 authoritative_state::{NativeStateStore,NativeStateError},
 native_dryrun_child_lineage::{
    ChildLineageError,id,valid_id,str_field,fixed,get,one_object,bad,
    exact_okx_fills,ensure_supported_spot_algo_shape
 },
 native_recovery_dryrun_latch::DryRunClosePhase,
 private_https::HttpExecutor,
 private_rest::{
    OkxCredentials,OkxDomain,
    build_okx_algo_by_id_request,build_okx_child_by_order_id_request,
 },
};

#[derive(Clone,Debug,PartialEq,Eq)]
pub struct MultiChildWitness{
    pub child_order_id:String,
    pub executed_qty_e12:u128,
    pub fill_ids:Vec<String>,
    pub fee_usdt_e12:i128,
}
#[derive(Clone,Debug,PartialEq,Eq)]
struct MultiSnapshot{
    parent_id:String,trigger_time_ms:u64,
    all_children:Vec<MultiChildWitness>,
    total_qty_e12:u128,total_fee_usdt_e12:i128,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct MockMultiChildReport{
    pub native_generation:u64,
    pub original_parent_algo_id:String,
    pub observed_children:Vec<MultiChildWitness>,
    pub observed_parent_total_qty_e12:u128,
    pub observed_total_quote_fee_e12:i128,
    pub total_mock_signed_get:u8,
    pub matching_scans:u8,
    pub all_declared_simple_ordinary_children_seen_in_mock:bool,
    pub nested_algos_proven_exhausted:bool,
    pub account_wide_historical_algo_completeness_proven:bool,
    pub futures_suborders_complete:bool,
    pub fee_and_lifetime_pnl_complete:bool,
    pub real_exchange_execution_proven:bool,
    pub final_live_positions_flat_proven:bool,
    pub may_cancel_retry_send_or_verify_closed:bool,
}
#[allow(clippy::too_many_arguments)]
fn one<E:HttpExecutor>(
    http:&E,okx:&OkxCredentials,domain:OkxDomain,
    now:u64,base:&str,symbol:&str,
    algo_id:&str,expected_qty:u128,
)->Result<MultiSnapshot,ChildLineageError>{
    let req=build_okx_algo_by_id_request(okx,now,domain,algo_id)
      .map_err(ChildLineageError::Build)?;
    let raw=get(http,&req)?;
    let parent=one_object(&raw)?;
    ensure_supported_spot_algo_shape(parent)?;
    if id(parent,"algoId")?!=algo_id
        ||str_field(parent,"instId")?!=symbol
        ||str_field(parent,"tdMode")?!="cash"
        ||str_field(parent,"side")?!="sell"
        ||str_field(parent,"state")?!="effective"
        ||fixed(parent,"sz")?!=expected_qty{
        return Err(bad("OKX branching algo original parent mismatch"))
    }
    let trigger=id(parent,"triggerTime")?.parse::<u64>()
       .ok().filter(|t|*t>0&&*t<=now&&now-*t<=600_000)
       .ok_or(bad("OKX branching algo trigger time missing or stale"))?;
    let nested=parent.get("subAlgoIdList").and_then(|v|v.as_array())
       .ok_or(bad("OKX subAlgoIdList missing"))?;
    if !nested.is_empty(){return Err(bad(
       "nested OKX spawned algorithm requires separate recursive audit"))}
    let children=parent.get("ordIdList").and_then(|v|v.as_array())
       .ok_or(bad("OKX ordinary child list absent"))?;
    if !(2..=3).contains(&children.len()){
        return Err(bad("only bounded two/three-child ordinary OKX graph reviewed"))
    }
    let mut order_ids=BTreeSet::new();
    for child in children {
        let child_id=child.as_str().ok_or(bad("OKX child ID not text"))?;
        if !valid_id(child_id)||!order_ids.insert(child_id.to_string()){
            return Err(bad("OKX duplicate/malformed ordinary child order ID"))
        }
    }
    let mut total_qty=0u128;
    let mut total_fee=0i128;
    let mut trade_ids=BTreeSet::new();
    let mut audited=Vec::with_capacity(order_ids.len());
    // Canonical numeric lexical order. Both reads must be exactly identical.
    for child_id in order_ids {
        let req=build_okx_child_by_order_id_request(
            okx,now,domain,symbol,&child_id)
            .map_err(ChildLineageError::Build)?;
        let v=get(http,&req)?;
        let ordinary=one_object(&v)?;
        if id(ordinary,"ordId")?!=child_id
          ||id(ordinary,"algoId")?!=algo_id
          ||str_field(ordinary,"instId")?!=symbol
          ||str_field(ordinary,"side")?!="sell"
          ||str_field(ordinary,"tdMode")?!="cash"
          ||str_field(ordinary,"state")?!="filled"{
            return Err(bad("ordinary child ID, side or reverse parent mismatch"))
        }
        let qty=fixed(ordinary,"sz")?;
        if qty==0||fixed(ordinary,"accFillSz")?!=qty{
            return Err(bad("OKX child not fully filled at declared amount"))
        }
        total_qty=total_qty.checked_add(qty)
           .ok_or(bad("sum of child quantities overflow"))?;
        if total_qty>expected_qty{
            return Err(bad("child graph over-sells original spot inventory"))
        }
        let (fills,fee)=exact_okx_fills(
            http,okx,domain,now,symbol,&child_id,base,trigger,qty)?;
        for trade in &fills{
            if !trade_ids.insert(trade.clone()){
                return Err(bad("same exchange trade ID counted across two children"))
            }
        }
        total_fee=total_fee.checked_add(fee)
            .ok_or(bad("sum of child commission overflow"))?;
        audited.push(MultiChildWitness{
            child_order_id:child_id,executed_qty_e12:qty,
            fill_ids:fills,fee_usdt_e12:fee,
        });
    }
    if total_qty!=expected_qty{
        return Err(bad("multi-child quantities do not close all original spot"))
    }
    Ok(MultiSnapshot{
        parent_id:algo_id.to_string(),trigger_time_ms:trigger,
        all_children:audited,total_qty_e12:total_qty,
        total_fee_usdt_e12:total_fee,
    })
}
/// Two complete read-only scans. For N=2 children 14 GET in all;
/// N=3 gives 20 GET. Not a parent-graph exhaustion certificate
/// for other symbols, other parents, subalgos or later effects.
pub fn inspect_mock_multi_okx_ordinary_children<E:HttpExecutor>(
    http:&E,store:&NativeStateStore,gen:u64,
    okx:&OkxCredentials,now:u64,domain:OkxDomain,algo_id:&str,
)->Result<MockMultiChildReport,ChildLineageError>{
    let original=store.load().map_err(ChildLineageError::State)?
      .ok_or(ChildLineageError::State(NativeStateError::Generation))?;
    if gen==0||original.generation!=gen||original.active.is_some()
        ||!valid_id(algo_id)||now==0{
        return Err(bad("invalid original Native owner or OKX parent before GET"))
    }
    let entry=original.entry.as_ref().ok_or(bad("original Native entry not found"))?;
    let latch=entry.dryrun_close_latch.as_ref()
      .ok_or(bad("original mock unknown close latch missing"))?;
    latch.verify(&original).map_err(ChildLineageError::State)?;
    if latch.phase()!=DryRunClosePhase::UnknownManualOnly{
        return Err(bad("only original manually quarantined mock close is inspectable"))
    }
    let (expected,_short,_residual)=latch.amounts();
    let symbol=entry.plan.spot_symbol.replace('/',"-");
    let base=&entry.plan.base;
    let first=one(http,okx,domain,now,base,&symbol,algo_id,expected)?;
    let second=one(http,okx,domain,now,base,&symbol,algo_id,expected)?;
    if first!=second{
        return Err(bad("OKX ordinary child order graph or fills/fees drifted between reads"))
    }
    if store.load().map_err(ChildLineageError::State)?.as_ref()!=Some(&original){
        return Err(bad("Native owner CAS advanced during multi-child audit"))
    }
    let get_count=u8::try_from(2*(1+3*first.all_children.len()))
       .map_err(|_|bad("GET count overflow"))?;
    Ok(MockMultiChildReport{
        native_generation:gen,original_parent_algo_id:algo_id.into(),
        observed_children:first.all_children,
        observed_parent_total_qty_e12:first.total_qty_e12,
        observed_total_quote_fee_e12:first.total_fee_usdt_e12,
        total_mock_signed_get:get_count,matching_scans:2,
        all_declared_simple_ordinary_children_seen_in_mock:true,
        nested_algos_proven_exhausted:false,
        account_wide_historical_algo_completeness_proven:false,
        futures_suborders_complete:false,fee_and_lifetime_pnl_complete:false,
        real_exchange_execution_proven:false,
        final_live_positions_flat_proven:false,
        may_cancel_retry_send_or_verify_closed:false,
    })
}
