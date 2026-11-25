//! TRADE1: READ-ONLY sizing and recovery feasibility for first <=10 USDT
//! microtrade. Fixed-point e12. No orders, permissions or execution claims.
use crate::{
    fill_economics::{decimal_to_atoms,SCALE_12},
    market_metadata::NativeMarketMetadata,
    authoritative_state::NativeAuthoritativeState,
};
pub const HARD_MICRO_CAP_E12:u128=10*SCALE_12;
pub const MAX_METADATA_AGE_MS:u64=30_000;
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum MicroSizeDeny{Invalid,Stale,Inactive,UnsupportedContract,
    MissingMetadata,Arithmetic,TooSmall,GridImpossible,OverMaximum}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct MicroSizePreview{
    pub base:String,pub spot_symbol:String,pub futures_symbol:String,
    pub qty_e12:u128,pub estimated_spot_cost_e12:u128,
    pub estimated_futures_notional_e12:u128,pub budget_e12:u128,
    pub metadata_age_ms:u64,pub may_send_order:bool,
    pub execution_liquidity_proven:bool,pub execution_fees_proven:bool,
    pub may_mark_hedged:bool,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum ResidualSpotStatus{
    NoResidual,ExactLotCandidateOnly,ManualDustOrBelowMinimum,
    ManualGridMismatch,ManualSuspectedOverhedge,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct ResidualSpotPreview{
    pub spot_total_e12:u128,pub futures_short_e12:u128,
    pub unmatched_spot_e12:u128,pub estimated_spot_sell_quote_e12:u128,
    pub status:ResidualSpotStatus,pub may_send_order:bool,
    pub may_auto_close:bool,pub may_mark_verified_closed:bool,
}
fn number(s:&str,zero_ok:bool)->Result<u128,MicroSizeDeny>{
    let n=decimal_to_atoms(s).map_err(|_|MicroSizeDeny::MissingMetadata)?;
    if n==0&&!zero_ok{return Err(MicroSizeDeny::MissingMetadata)}
    Ok(n)
}
fn cost(q:u128,p:u128)->Result<u128,MicroSizeDeny>{
    if p==0{return Err(MicroSizeDeny::Invalid)}
    q.checked_mul(p).and_then(|x|x.checked_add(SCALE_12-1))
        .map(|x|x/SCALE_12).ok_or(MicroSizeDeny::Arithmetic)
}
fn gcd(mut a:u128,mut b:u128)->u128{
    while b!=0{let n=a%b;a=b;b=n;}a
}
fn validate(m:&NativeMarketMetadata,now:u64)->Result<u64,MicroSizeDeny>{
    if m.base.is_empty()||m.base.len()>16||!m.base.bytes().all(|x|x.is_ascii_uppercase())
       ||m.spot_symbol!=format!("{}-USDT",m.base)
       ||m.future_symbol!=format!("{}USDT",m.base){
        return Err(MicroSizeDeny::Invalid)
    }
    if m.spot_status!="live"||m.future_status!="TRADING"{
        return Err(MicroSizeDeny::Inactive)
    }
    if m.static_reason.is_some()||m.metadata_state()!="SIZE_METADATA_READY"{
        return Err(MicroSizeDeny::MissingMetadata)
    }
    if m.contract_size!="1"{return Err(MicroSizeDeny::UnsupportedContract)}
    if m.loaded_ns==0||m.loaded_wall_ms==0||now<m.loaded_wall_ms
       ||now-m.loaded_wall_ms>MAX_METADATA_AGE_MS{
        return Err(MicroSizeDeny::Stale)
    }
    Ok(now-m.loaded_wall_ms)
}
/// Both legs get exactly the SAME underlying quantity on the common exchange
/// step grid. Price inputs are only observations; real fill VWAP can differ.
pub fn preview_micro_pair(
    m:&NativeMarketMetadata,now:u64,spot_ask_e12:u128,
    futures_bid_e12:u128,cap_e12:u128,
)->Result<MicroSizePreview,MicroSizeDeny>{
    let age=validate(m,now)?;
    if cap_e12==0||cap_e12>HARD_MICRO_CAP_E12
       ||spot_ask_e12==0||futures_bid_e12==0{
        return Err(MicroSizeDeny::Invalid)
    }
    let spot_tick=number(m.spot_tick.as_deref()
        .ok_or(MicroSizeDeny::MissingMetadata)?,false)?;
    let future_tick=number(m.future_tick.as_deref()
        .ok_or(MicroSizeDeny::MissingMetadata)?,false)?;
    if spot_ask_e12%spot_tick!=0||futures_bid_e12%future_tick!=0{
        return Err(MicroSizeDeny::Invalid)
    }
    let a=number(m.spot_step.as_deref().ok_or(MicroSizeDeny::MissingMetadata)?,false)?;
    let b=number(m.future_step_base.as_deref().ok_or(MicroSizeDeny::MissingMetadata)?,false)?;
    let grid=(a/gcd(a,b)).checked_mul(b).ok_or(MicroSizeDeny::Arithmetic)?;
    let q_spot=cap_e12.checked_mul(SCALE_12)
        .ok_or(MicroSizeDeny::Arithmetic)?/spot_ask_e12;
    let q_fut=cap_e12.checked_mul(SCALE_12)
        .ok_or(MicroSizeDeny::Arithmetic)?/futures_bid_e12;
    let qty=(q_spot.min(q_fut)/grid)*grid;
    if qty==0{return Err(MicroSizeDeny::GridImpossible)}
    let spot_min=number(m.spot_min_amount.as_deref()
        .ok_or(MicroSizeDeny::MissingMetadata)?,false)?;
    let fut_min=number(m.future_min_base.as_deref()
        .ok_or(MicroSizeDeny::MissingMetadata)?,false)?;
    let spot_mincost=number(&m.spot_min_cost,true)?;
    let fut_mincost=number(&m.future_min_cost,true)?;
    if qty<spot_min||qty<fut_min{return Err(MicroSizeDeny::TooSmall)}
    if let Some(max)=m.spot_max_amount.as_ref(){
        if qty>number(max,false)?{return Err(MicroSizeDeny::OverMaximum)}
    }
    if let Some(max)=m.future_max_base.as_ref(){
        if qty>number(max,false)?{return Err(MicroSizeDeny::OverMaximum)}
    }
    let spot_cost=cost(qty,spot_ask_e12)?;
    let fut_cost=cost(qty,futures_bid_e12)?;
    if spot_cost>cap_e12||fut_cost>cap_e12{return Err(MicroSizeDeny::OverMaximum)}
    if spot_cost<spot_mincost||fut_cost<fut_mincost{
        return Err(MicroSizeDeny::TooSmall)
    }
    Ok(MicroSizePreview{base:m.base.clone(),spot_symbol:m.spot_symbol.clone(),
        futures_symbol:m.future_symbol.clone(),qty_e12:qty,
        estimated_spot_cost_e12:spot_cost,
        estimated_futures_notional_e12:fut_cost,budget_e12:cap_e12,
        metadata_age_ms:age,may_send_order:false,
        execution_liquidity_proven:false,execution_fees_proven:false,
        may_mark_hedged:false})
}
/// Operator-only diagnostic from ORIGINAL independently persisted C5/C6
/// Binance/OKX proof. Does NOT create an order or decide when to execute.
pub fn preview_residual_spot_reduction(
    s:&NativeAuthoritativeState,m:&NativeMarketMetadata,now:u64,
    spot_bid_e12:u128,
)->Result<ResidualSpotPreview,MicroSizeDeny>{
    validate(m,now)?;
    let spot_tick=number(m.spot_tick.as_deref()
        .ok_or(MicroSizeDeny::MissingMetadata)?,false)?;
    if spot_bid_e12==0||spot_bid_e12%spot_tick!=0{
        return Err(MicroSizeDeny::Invalid)
    }
    let advice=s.quarantined_supervised_assessment()
        .map_err(|_|MicroSizeDeny::Invalid)?;
    let e=s.entry.as_ref().ok_or(MicroSizeDeny::Invalid)?;
    if e.plan.base!=m.base||e.plan.spot_symbol.replace('/',"-")!=m.spot_symbol
       ||e.plan.futures_symbol!=m.future_symbol
       ||advice.may_send_order||advice.may_auto_hedge||advice.may_auto_unwind{
        return Err(MicroSizeDeny::Invalid)
    }
    let (short_e18,residual_e18)=match advice.review{
        crate::native_futures_uncertain_quarantine::QuarantinedOperatorReview::
            ManualRecoveryRequired{observed_short_e18,unmatched_spot_e18,..}=>
            (observed_short_e18,unmatched_spot_e18),
        crate::native_futures_uncertain_quarantine::QuarantinedOperatorReview::
            FurtherAccountAndLifetimeReviewRequired{observed_short_e18,..}=>
            (observed_short_e18,0)
    };
    if short_e18%1_000_000!=0||residual_e18%1_000_000!=0{
        return Err(MicroSizeDeny::Arithmetic)
    }
    let short=short_e18/1_000_000;
    let residual=residual_e18/1_000_000;
    let total=short.checked_add(residual).ok_or(MicroSizeDeny::Arithmetic)?;
    let quote=cost(residual,spot_bid_e12)?;
    let min=number(m.spot_min_amount.as_deref()
        .ok_or(MicroSizeDeny::MissingMetadata)?,false)?;
    let step=number(m.spot_step.as_deref()
        .ok_or(MicroSizeDeny::MissingMetadata)?,false)?;
    let mincost=number(&m.spot_min_cost,true)?;
    let overmax=if let Some(max)=m.spot_max_amount.as_ref(){
        residual>number(max,false)?
    }else{false};
    let suspected=e.futures_uncertain_quarantine.as_ref()
        .ok_or(MicroSizeDeny::Invalid)?.overhedge_suspected();
    let status=if suspected{ResidualSpotStatus::ManualSuspectedOverhedge}
    else if residual==0{ResidualSpotStatus::NoResidual}
    else if residual<min||quote<mincost||overmax{
        ResidualSpotStatus::ManualDustOrBelowMinimum
    }else if residual%step!=0{ResidualSpotStatus::ManualGridMismatch}
    else{ResidualSpotStatus::ExactLotCandidateOnly};
    Ok(ResidualSpotPreview{spot_total_e12:total,
        futures_short_e12:short,unmatched_spot_e12:residual,
        estimated_spot_sell_quote_e12:quote,status,
        may_send_order:false,may_auto_close:false,may_mark_verified_closed:false})
}
