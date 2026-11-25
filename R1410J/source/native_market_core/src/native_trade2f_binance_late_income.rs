//! TRADE2F2I: owner-bound, read-only SIGNED-IN-MEMORY-MOCK Binance income.
//! No HttpExecutor, real credentials, network calls, orders or profit authority.
//! The unfiltered /fapi/v1/income stream may contain external wallet flows;
//! a stable bounded interval NEVER proves the absence of future postings.
use std::collections::{BTreeMap,BTreeSet};
use serde_json::Value;
use crate::{
 authoritative_state::{NativeStateError,NativeStateStore},
 fill_economics::{signed_decimal_to_atoms,LegFillLedger},
 native_trade2f_owner_binding::original_four_leg_preflight,
 private_rest::{BinanceCredentials,HttpMethod,RestBuildError,SignedRequest,
   build_binance_all_income_page_request},
 native_trade2f_okx_owner_mock::MockGetStep,
};
const PAGE:u16=1000;
const MAX_PAGES:u16=4;
const MAX_WINDOW:u64=6*24*60*60*1000;
const MIN_LATE_GAP:u64=60_000; // diagnostic fixture only, NOT an exchange SLA
const MAX_LATE_GAP:u64=24*60*60*1000;
const FAKE_KEY:&str="F2I_OFFLINE_BINANCE_KEY";
const FAKE_SECRET:&str="F2I_OFFLINE_BINANCE_SECRET";
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum F2IError{State(NativeStateError),Build(RestBuildError),Deny(&'static str)}
fn no(reason:&'static str)->F2IError{F2IError::Deny(reason)}
fn field<'a>(r:&'a Value,k:&str)->Result<&'a str,F2IError>{
 r.get(k).and_then(Value::as_str).ok_or(no("Binance income missing mandatory string"))
}
fn u64_field(r:&Value,k:&str)->Result<u64,F2IError>{
 let v=r.get(k).ok_or(no("Binance income mandatory numeric field missing"))?;
 let n=if let Some(u)=v.as_u64(){Some(u)}
  else{v.as_str().and_then(|x|x.parse::<u64>().ok())};
 n.filter(|x|*x>0).ok_or(no("Binance income numeric field invalid"))
}
fn money(r:&Value)->Result<i128,F2IError>{
 signed_decimal_to_atoms(field(r,"income")?)
     .map_err(|_|no("Binance income overprecision or overflow"))
}
#[derive(Clone,Debug,PartialEq,Eq)]
struct Row{
 kind:String,id:u64,trade_id:String,symbol:String,asset:String,
 amount:i128,time:u64,
}
type Rows=BTreeMap<(String,u64),Row>;
struct Tape<'a>{items:&'a [MockGetStep],next:usize}
impl Tape<'_>{
 fn read(&mut self,req:&SignedRequest)->Result<Vec<Value>,F2IError>{
  if req.method!=HttpMethod::Get||req.is_mutation||req.body.is_some()
      ||!req.headers.iter().any(|(k,v)|k=="X-MBX-APIKEY"&&v==FAKE_KEY){
      return Err(no("Binance income collector is mock-only signed GET"))}
  let request_path=req.url.strip_prefix("https://fapi.binance.com")
      .ok_or(no("Binance mock URL not futures API"))?;
  if !request_path.starts_with("/fapi/v1/income?")
    ||request_path.contains("incomeType=")
    ||request_path.contains("symbol="){
      return Err(no("Binance income query unexpectedly filtered"))}
  let step=self.items.get(self.next).ok_or(no("Binance mock transcript incomplete"))?;
  if step.path_and_query!=request_path{
      return Err(no("Binance income signed query or pagination differs"))}
  self.next+=1;
  if step.http_status!=200{return Err(no("Binance income GET HTTP error"))}
  let rows:Value=serde_json::from_str(&step.body)
     .map_err(|_|no("Binance income malformed JSON"))?;
  let array=rows.as_array().ok_or(no("Binance income response not an array"))?;
  if array.len()>PAGE as usize{return Err(no("Binance income page exceeds configured limit"))}
  Ok(array.clone())
 }
}
fn scan(tape:&mut Tape<'_>,creds:&BinanceCredentials,
 start:u64,end:u64)->Result<Rows,F2IError>{
 let mut rows=Rows::new();
 for page in 1..=MAX_PAGES{
  let req=build_binance_all_income_page_request(
     creds,end,10_000,start,end,page,PAGE).map_err(F2IError::Build)?;
  let response=tape.read(&req)?;
  if response.is_empty(){return Ok(rows)}
  for v in response{
   let kind=field(&v,"incomeType")?.to_string();
   let id=u64_field(&v,"tranId")?;
   let time=u64_field(&v,"time")?;
   let asset=field(&v,"asset")?.to_string();
   let symbol=field(&v,"symbol")?.to_string();
   let trade_id=field(&v,"tradeId")?.to_string();
   if time<start||time>end{return Err(no("Binance income outside explicit window"))}
   if asset!="USDT"{
      return Err(no("Binance non-USDT BNB/BNT/other fee cannot be quote-valued without independent FX source"))}
   let amount=money(&v)?;
   // We deliberately do not exclude incomeType or symbols from the GET.
   // Unknown classes, deposits, transfers and rebates are *not* trading PnL.
   if !matches!(kind.as_str(),"COMMISSION"|"REALIZED_PNL"|"FUNDING_FEE"){
      return Err(no("unsupported Binance income class or transfer requires manual cash reconciliation"))}
   if symbol.is_empty(){return Err(no("unscoped Binance income cannot be attributed to position"))}
   if kind=="FUNDING_FEE"{
      if !trade_id.is_empty(){return Err(no("Binance funding has unexpected tradeId"))}
   }else if trade_id.is_empty(){
      return Err(no("Binance execution income missing tradeId"))
   }
   let record=Row{kind:kind.clone(),id,trade_id,symbol,asset,amount,time};
   if rows.insert((kind,id),record).is_some(){
      return Err(no("duplicate Binance (incomeType,tranId)"))}
  }
 }
 Err(no("Binance income pagination not exhausted within 4 pages"))
}
fn bind_futures_income(rows:&Rows,open:&LegFillLedger,close:&LegFillLedger,
 source_close_pnl:i128,prior_funding:i128)->Result<usize,F2IError>{
 if open.venue!="binance"||close.venue!="binance"
    ||open.side!="sell"||close.side!="buy"
    ||open.symbol!=close.symbol||!open.terminal||!close.terminal{
    return Err(no("original future pair not sealed and terminal"))}
 let mut fills=BTreeMap::new();
 for (ledger,is_close) in [(open,false),(close,true)]{
   for (id,fill) in &ledger.fills{
    if id!=&fill.fill_id||fills.insert(id.as_str(),(fill,is_close)).is_some(){
       return Err(no("nonunique original Binance fills"))}
   }
 }
 if fills.is_empty(){return Err(no("missing Binance source fills"))}
 let mut fees=BTreeMap::<String,i128>::new();
 let mut realized=BTreeMap::<String,i128>::new();
 let mut total_funding=0i128;
 let mut funding_rows=0usize;
 for r in rows.values(){
  if r.symbol!=open.symbol||r.asset!="USDT"{
     return Err(no("Binance income for different instrument/account asset"))}
  if r.kind=="FUNDING_FEE"{
    funding_rows+=1;
    total_funding=total_funding.checked_add(r.amount)
       .ok_or(no("funding total overflow"))?;
    continue
  }
  let (fill,is_close)=fills.get(r.trade_id.as_str())
    .ok_or(no("Binance income from foreign / duplicate trade"))?;
  let t=fill.trade_time_ms.ok_or(no("original Binance trade lacks fill time"))?;
  if r.time<t||r.time-t>180_000{
     return Err(no("Binance income time outside original execution bracket"))}
  if r.kind=="COMMISSION"{
    if r.amount>=0{return Err(no("Binance commission rebate cannot be netted as loss"))}
    let sum=fees.entry(r.trade_id.clone()).or_default();
    *sum=sum.checked_add(r.amount).ok_or(no("commission total overflow"))?;
  }else{
    if !is_close{return Err(no("realized PnL on original opening fill"))}
    let sum=realized.entry(r.trade_id.clone()).or_default();
    *sum=sum.checked_add(r.amount).ok_or(no("realized PnL total overflow"))?;
  }
 }
 if total_funding!=prior_funding{
    return Err(no("late Binance funding disagrees with original Native bounded seal"))}
 let mut sum_pnl=0i128;
 for (id,(f,is_close)) in &fills{
  let want=f.fee_cost_quote_atoms
     .ok_or(no("original Binance fee asset/amount missing"))?;
  if want<=0 || fees.get(*id).copied()!=want.checked_neg(){
     return Err(no("Binance per-trade commission differs from Native fill"))}
  if *is_close{
    let p=realized.get(*id).ok_or(no("missing realized PnL closing leg"))?;
    sum_pnl=sum_pnl.checked_add(*p).ok_or(no("Binance PnL overflow"))?;
  }else if realized.contains_key(*id){
    return Err(no("unexpected realized PnL for opening future"))}
 }
 if fees.len()!=fills.len()
   ||realized.len()!=close.fills.len()
   ||sum_pnl!=source_close_pnl{
   return Err(no("Binance closing original PnL/fill identity mismatch"))}
 Ok(funding_rows)
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct F2IOfflineReview{
 pub native_owner_generation:u64,
 pub signed_mock_gets:usize,
 pub source_income_rows:usize,
 pub observed_bounded_funding_rows:usize,
 pub two_original_and_two_late_mock_scans_stable:bool,
 pub late_income_rows_in_observed_mock_window:usize,
 pub third_asset_usdt_fx_independently_proven:bool,
 pub all_real_binance_income_proven:bool,
 pub account_wide_history_and_late_finality_proven:bool,
 pub definitive_lifetime_net_pnl_e12:Option<i128>,
 pub verified_closed:bool,pub real_ready:bool,pub may_trade_cancel_transfer_or_release:bool,
}
/// Scoped by full sealed Native four legs and original bounded funding.
/// A signed fake GET makes no network call and cannot certify real full PnL.
pub fn inspect_owner_bound_binance_late_income_offline(
 store:&NativeStateStore,generation:u64,initial_end:u64,late_end:u64,
 tape:&[MockGetStep],
)->Result<F2IOfflineReview,F2IError>{
 inspect_owner_bound_binance_late_income_inner(
   store,generation,initial_end,late_end,tape,||{})
}
#[cfg(debug_assertions)]
#[doc(hidden)]
pub fn inspect_owner_bound_binance_late_income_debug_after_initial<F:FnOnce()>(
 store:&NativeStateStore,generation:u64,initial_end:u64,late_end:u64,
 tape:&[MockGetStep],hook:F,
)->Result<F2IOfflineReview,F2IError>{
 inspect_owner_bound_binance_late_income_inner(
    store,generation,initial_end,late_end,tape,hook)
}
fn inspect_owner_bound_binance_late_income_inner<F:FnOnce()>(
 store:&NativeStateStore,generation:u64,initial_end:u64,late_end:u64,
 tape:&[MockGetStep],hook:F,
)->Result<F2IOfflineReview,F2IError>{
 if initial_end==0||late_end<initial_end
    ||late_end-initial_end<MIN_LATE_GAP
    ||late_end-initial_end>MAX_LATE_GAP{
    return Err(no("Binance late income observation horizon invalid"))}
 let state=store.load().map_err(F2IError::State)?
    .ok_or(F2IError::State(NativeStateError::Generation))?;
 if state.generation!=generation{return Err(no("Native generation stale before GET"))}
 original_four_leg_preflight(&state)
   .map_err(|_|no("original four-leg Native owner preflight denied BEFORE GET"))?;
 let e=state.entry.as_ref().ok_or(no("original Native entry absent"))?;
 let start=e.plan.created_at_ms;
 if start==0||initial_end<start||late_end-start>MAX_WINDOW{
    return Err(no("Binance bounded income exceeds supported time"))}
 let open=&e.recovery_checkpoint.as_ref()
   .ok_or(no("original futures open missing"))?.ledger;
 let closing=e.shadow_close_receipt.as_ref()
   .ok_or(no("original futures closing receipt missing"))?;
 let close=closing.bounded_economic_ledgers().1;
 let funding=e.shadow_lifetime_economics.as_ref()
   .ok_or(no("original bounded funding source seal missing"))?.bounded_funding_e12();
 let creds=BinanceCredentials::new(FAKE_KEY,FAKE_SECRET).map_err(F2IError::Build)?;
 let mut mock=Tape{items:tape,next:0};
 let a=scan(&mut mock,&creds,start,initial_end)?;
 let b=scan(&mut mock,&creds,start,initial_end)?;
 if a!=b{return Err(no("Binance initial money snapshots disagreed"))}
 hook();
 if store.load().map_err(F2IError::State)?.as_ref()!=Some(&state){
    return Err(no("Native owner CAS advanced after initial Binance scan"))}
 let c=scan(&mut mock,&creds,start,late_end)?;
 let d=scan(&mut mock,&creds,start,late_end)?;
 if c!=d{return Err(no("Binance delayed money snapshots disagreed"))}
 if mock.next!=tape.len(){return Err(no("unexpected additional signed Binance mock traffic"))}
 // Every earlier income record must persist bit-for-bit in the widened
 // window, and every new record is *not* silently promoted to profit.
 for (k,v) in &a{
   if c.get(k)!=Some(v){return Err(no("old Binance money disappeared or was rewritten"))}
 }
 if c.len()!=a.len(){return Err(no("new late Binance income is unverified and manual only"))}
 let count=bind_futures_income(&c,open,close,
   closing.futures_reported_close_pnl_e12(),funding)?;
 if store.load().map_err(F2IError::State)?.as_ref()!=Some(&state){
    return Err(no("Native owner CAS advanced during late Binance income reads"))}
 Ok(F2IOfflineReview{
  native_owner_generation:generation,signed_mock_gets:mock.next,
  source_income_rows:c.len(),observed_bounded_funding_rows:count,
  two_original_and_two_late_mock_scans_stable:true,
  late_income_rows_in_observed_mock_window:0,
  third_asset_usdt_fx_independently_proven:false,
  all_real_binance_income_proven:false,
  account_wide_history_and_late_finality_proven:false,
  definitive_lifetime_net_pnl_e12:None,
  verified_closed:false,real_ready:false,may_trade_cancel_transfer_or_release:false,
 })
}

/// Astra P1-05: a Binance income `time` is a POSTED timestamp. It is NOT
/// by itself evidence that funding was effective while OUR short was held.
/// External effective-time proof is still missing; caller-supplied witness is
/// diagnostically useful but never an exchange-attested position entitlement.
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct FundingTimeWitness{
 pub income_tran_id:u64,
 pub source_owner_generation:u64,
 pub symbol:String,
 pub signed_income_atoms_e12:i128,
 pub effective_at_ms:u64,
 pub posted_at_ms:u64,
 pub captured_at_ms:u64,
 pub independent_effective_time_proven:bool,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct FundingExposureOfflineReview{
 pub original_owner_generation:u64,
 pub short_fully_open_at_ms:u64,
 pub short_first_cover_at_ms:u64,
 pub funding_income_transactions:usize,
 pub effective_within_full_short_exposure:bool,
 pub posted_and_captured_times_distinguished:bool,
 pub independent_effective_time_proven:bool,
 pub entire_exchange_history_complete:bool,
 pub definitive_lifetime_pnl_e12:Option<i128>,
 pub may_trade_or_mark_verified_closed:bool,
}
pub fn inspect_owner_funding_effective_vs_posted_offline(
 store:&NativeStateStore,generation:u64,first_end:u64,late_end:u64,
 source:&[MockGetStep],witnesses:&[FundingTimeWitness],
)->Result<FundingExposureOfflineReview,F2IError>{
 // Full persisted four-leg owner, original money identity, duplicated
 // original/late page stability, and total sealed funding are established
 // first by the existing verifier, not reimplemented.
 let checked=inspect_owner_bound_binance_late_income_offline(
    store,generation,first_end,late_end,source)?;
 if checked.observed_bounded_funding_rows==0
    ||witnesses.len()!=checked.observed_bounded_funding_rows
    ||witnesses.len()>32||checked.all_real_binance_income_proven
    ||checked.verified_closed||checked.real_ready{
    return Err(no("funding row coverage or source authority is unproven"))
 }
 // This separate phase keeps the deep F2I and large Native owner stack
 // frames nonoverlapping, including on default Rust test stacks.
 inspect_funding_time_witnesses_against_owner(
    store,generation,late_end,source,witnesses)
}
// P1-05: reconstruct the modelled short at the caller-supplied effective
// cutoff from each ORIGINAL fill, not merely cached first/last timestamps.
// Still no independent Binance funding clock or account-scoped position proof.
fn short_full_qty_at_effective_offline(open:&LegFillLedger,close:&LegFillLedger,
 cutoff:u64)->Result<u128,F2IError>{
 fn fold(ledger:&LegFillLedger,cutoff:u64)->Result<(u128,u128,u64,u64),F2IError>{
  if !ledger.terminal||ledger.fills.is_empty(){
   return Err(no("funding cutoff needs terminal original fill ledger"))
  }
  let (mut full,mut before,mut earliest,mut latest)=(0u128,0u128,u64::MAX,0u64);
  for (id,fill) in &ledger.fills{
   let t=fill.trade_time_ms.filter(|t|*t>0)
     .ok_or(no("original futures fill time missing"))?;
   if fill.fill_id!=*id||fill.qty_atoms==0||t==cutoff{
    return Err(no("ambiguous futures fill identity or funding cutoff"))
   }
   full=full.checked_add(fill.qty_atoms).ok_or(no("short fill quantity overflow"))?;
   if t<cutoff{
    before=before.checked_add(fill.qty_atoms)
      .ok_or(no("short cutoff quantity overflow"))?;
   }
   earliest=earliest.min(t);
   latest=latest.max(t);
  }
  if full!=ledger.filled_qty_atoms()
    ||ledger.first_fill_time_ms!=Some(earliest)
    ||ledger.last_fill_time_ms!=Some(latest){
    return Err(no("cached futures fill quantity or timestamps differ from original fills"))
  }
  Ok((full,before,earliest,latest))
 }
 if open.venue!="binance"||close.venue!="binance"
    ||open.side!="sell"||close.side!="buy"||open.symbol!=close.symbol{
    return Err(no("original short/cover venue or owner symbol mismatch"))
 }
 let (full,held,_,last_open)=fold(open,cutoff)?;
 let (total_cover,already_covered,first_cover,_)=fold(close,cutoff)?;
 if full==0||full!=total_cover||held!=full||already_covered!=0
    ||cutoff<=last_open||cutoff>=first_cover{
    return Err(no("funding cutoff not fully held by original owner short"))
 }
 Ok(full)
}
fn inspect_funding_time_witnesses_against_owner(
 store:&NativeStateStore,generation:u64,late_end:u64,
 source:&[MockGetStep],witnesses:&[FundingTimeWitness],
)->Result<FundingExposureOfflineReview,F2IError>{
 let before=store.load().map_err(F2IError::State)?
    .ok_or(no("owner missing for funding exposure"))?;
 if before.generation!=generation{return Err(no("stale funding source generation"))}
 let e=before.entry.as_ref().ok_or(no("original short owner absent"))?;
 let open=&e.recovery_checkpoint.as_ref()
    .ok_or(no("original future short not filled"))?.ledger;
 let close=e.shadow_close_receipt.as_ref()
    .ok_or(no("original short cover missing"))?.bounded_economic_ledgers().1;
 let open_last=open.last_fill_time_ms.ok_or(no("last short fill time missing"))?;
 let close_first=close.first_fill_time_ms.ok_or(no("first short cover time missing"))?;
 if open_last>=close_first||open.fills.is_empty()||close.fills.is_empty(){
    return Err(no("no provable full-short ownership interval"))
 }
 // The same signed in-memory income snapshots can be replayed four times.
 // They are not four independent funding events.
 let mut recorded=BTreeMap::<u64,(String,i128,u64)>::new();
 for step in source{
    let body:Value=serde_json::from_str(&step.body)
       .map_err(|_|no("funding transcript malformed"))?;
    let rows=body.as_array().ok_or(no("funding transcript is not an array"))?;
    for row in rows{
       if row.get("incomeType").and_then(Value::as_str)!=Some("FUNDING_FEE"){continue}
       let id=u64_field(row,"tranId")?;
       let asset=field(row,"asset")?;
       let symbol=field(row,"symbol")?;
       let post=u64_field(row,"time")?;
       if asset!="USDT"||symbol!=open.symbol{
          return Err(no("funding income asset or short owner mismatch"))
       }
       let value=money(row)?;
       let old=recorded.insert(id,(symbol.to_string(),value,post));
       if old.is_some_and(|old|old!=(symbol.to_string(),value,post)){
          return Err(no("funding time or amount rewritten across snapshots"))
       }
    }
 }
 if recorded.len()!=witnesses.len(){
    return Err(no("missing or duplicate original funding effective-time evidence"))
 }
 let mut used=BTreeSet::new();
 for w in witnesses{
    if w.income_tran_id==0||!used.insert(w.income_tran_id)
       ||w.source_owner_generation!=generation
       ||w.symbol!=open.symbol||w.independent_effective_time_proven
       ||w.effective_at_ms<=open_last||w.effective_at_ms>=close_first
       ||w.posted_at_ms<w.effective_at_ms
       ||w.captured_at_ms<w.posted_at_ms||w.captured_at_ms>late_end{
       return Err(no("funding effective/posted/captured times not owner bounded"))
    }
    short_full_qty_at_effective_offline(open,&close,w.effective_at_ms)?;
    let expected=recorded.get(&w.income_tran_id)
       .ok_or(no("foreign funding transaction witness"))?;
    if expected.0!=w.symbol||expected.1!=w.signed_income_atoms_e12
       ||expected.2!=w.posted_at_ms{
       return Err(no("funding witness differs from original posted income row"))
    }
 }
 if store.load().map_err(F2IError::State)?.as_ref()!=Some(&before){
    return Err(no("Native owner changed during funding attribution"))
 }
 Ok(FundingExposureOfflineReview{
    original_owner_generation:generation,
    short_fully_open_at_ms:open_last,
    short_first_cover_at_ms:close_first,
    funding_income_transactions:recorded.len(),
    effective_within_full_short_exposure:true,
    posted_and_captured_times_distinguished:true,
    independent_effective_time_proven:false,
    entire_exchange_history_complete:false,
    definitive_lifetime_pnl_e12:None,
    may_trade_or_mark_verified_closed:false,
 })
}

#[cfg(test)]
mod p105_exposure_gate_tests{
 use super::*;
 use crate::fill_economics::{FillEvent,decimal_to_atoms,DEFAULT_AVG_TOLERANCE_ATOMS};
 fn leg(side:&str,events:&[(&str,&str,u64)])->LegFillLedger{
  let qty=decimal_to_atoms("0.09").unwrap();
  let mut ledger=LegFillLedger::new("binance","BNTUSDT",side,"intent","client",qty).unwrap();
  for (id,amount,time) in events{
   ledger.apply_fill(FillEvent{
    fill_id:(*id).into(),price_atoms:decimal_to_atoms("100").unwrap(),
    qty_atoms:decimal_to_atoms(amount).unwrap(),fee_cost_quote_atoms:Some(100),
    fee_currency:Some("USDT".into()),trade_time_ms:Some(*time),
   },DEFAULT_AVG_TOLERANCE_ATOMS).unwrap();
  }
  ledger.apply_cumulative_truth(qty,Some(decimal_to_atoms("100").unwrap()),
    true,Some("filled"),Some("mock-order"),events.last().map(|v|v.2),
    DEFAULT_AVG_TOLERANCE_ATOMS).unwrap();
  ledger
 }
 #[test]
 fn p105_full_short_cutoff_uses_individual_fills_not_mutable_cached_bounds(){
  let original=leg("sell",&[("F1","0.04",3000),("F2","0.05",3100)]);
  let close=leg("buy",&[("C1","0.09",7000)]);
  assert_eq!(short_full_qty_at_effective_offline(&original,&close,5000).unwrap(),
     decimal_to_atoms("0.09").unwrap());
  let mut false_open=original.clone();
  false_open.last_fill_time_ms=Some(3000); // cache says full but F2 is later
  assert!(short_full_qty_at_effective_offline(&false_open,&close,3050).is_err());
  let mut missing=original.clone();
  missing.fills.get_mut("F2").unwrap().trade_time_ms=None;
  assert!(short_full_qty_at_effective_offline(&missing,&close,5000).is_err());
  let early_cover=leg("buy",&[("C1","0.04",4500),("C2","0.05",7000)]);
  let mut false_close=early_cover;
  false_close.first_fill_time_ms=Some(7000); // actual partial cover began earlier
  assert!(short_full_qty_at_effective_offline(&original,&false_close,5000).is_err());
  let mut inconsistent=original.clone();
  inconsistent.cumulative_truth_present=true;
  inconsistent.cumulative_qty_atoms=decimal_to_atoms("0.08").unwrap();
  assert!(short_full_qty_at_effective_offline(&inconsistent,&close,5000).is_err());
 }
}
