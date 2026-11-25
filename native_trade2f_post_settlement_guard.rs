//! TRADE2F2H: conservative *offline* settlement-lag mirror of unfiltered
//! OKX BNT/USDT account bills. Uses ONLY scripted in-memory signed GETs.
//! Two stable scans of a bounded window DO NOT prove future postings absent.
use std::collections::{BTreeMap,BTreeSet};
use serde_json::Value;
use crate::{
 authoritative_state::{NativeStateStore,NativeStateError},
 fill_economics::signed_decimal_to_atoms,
 private_rest::{OkxCredentials,OkxDomain,HttpMethod,RestBuildError,
    build_okx_base_currency_bills_page_request},
 native_trade2f_okx_owner_mock::{
    F2GMockReview,F2GError,MockGetStep,inspect_owner_bound_okx_offline_mock},
};
const PAGE:u16=100;
const MAX_PAGES:u16=4;
const MIN_OBSERVATION_LAG:u64=60_000; // review test horizon, NOT exchange SLA
const MAX_OBSERVATION_LAG:u64=24*60*60*1000;
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum F2HError{Native(NativeStateError),First(F2GError),Build(RestBuildError),Deny(&'static str)}
fn deny(s:&'static str)->F2HError{F2HError::Deny(s)}
fn str_f<'a>(v:&'a Value,k:&str)->Result<&'a str,F2HError>{
 v.get(k).and_then(Value::as_str).ok_or(deny("late account bill field absent or nonstring"))
}
fn id(v:&Value,k:&str)->Result<u128,F2HError>{
 let s=str_f(v,k)?;
 if s.is_empty()||s.starts_with('0')||!s.bytes().all(|c|c.is_ascii_digit()){
    return Err(deny("noncanonical late OKX bill ID"))}
 s.parse::<u128>().ok().filter(|x|*x>0).ok_or(deny("late bill ID overflow"))
}
fn decimal(v:&Value,k:&str)->Result<i128,F2HError>{
 signed_decimal_to_atoms(str_f(v,k)?)
    .map_err(|_|deny("late account money has missing/unsafe fixed decimal"))
}
struct Tape<'a>{steps:&'a [MockGetStep],pos:usize,base:&'static str}
impl Tape<'_>{
 fn get(&mut self,path:&str,req:&crate::private_rest::SignedRequest)
 ->Result<Vec<Value>,F2HError>{
  if req.method!=HttpMethod::Get||req.is_mutation||req.body.is_some()
    ||!req.headers.iter().any(|(k,v)|k=="OK-ACCESS-SIGN"&&!v.is_empty())
    ||req.url.strip_prefix(self.base)!=Some(path){
     return Err(deny("settlement review only supports correctly signed mock GET"))}
  let r=self.steps.get(self.pos).ok_or(deny("settlement mock GET tape short"))?;
  if r.path_and_query!=path{return Err(deny("settlement mock GET path or cursor drift"))}
  self.pos+=1;
  if r.http_status!=200{return Err(deny("settlement account GET status not 200"))}
  let j:Value=serde_json::from_str(&r.body)
     .map_err(|_|deny("settlement GET malformed JSON"))?;
  if j.get("code").and_then(Value::as_str)!=Some("0"){
     return Err(deny("settlement GET rejected by exchange code"))}
  let rows=j.get("data").and_then(Value::as_array)
     .ok_or(deny("settlement bill list missing"))?;
  if rows.len()>PAGE as usize{return Err(deny("settlement bill page >max"))}
  Ok(rows.clone())
 }
}
fn one_currency(tape:&mut Tape<'_>,creds:&OkxCredentials,domain:OkxDomain,
 ccy:&str,start:u64,end:u64)->Result<Vec<Value>,F2HError>{
 let mut all=Vec::new();
 let mut after=None::<String>;
 let mut last_id=None::<u128>;
 let mut last_ts=None::<u64>;
 let mut expected_prior_balance=None::<i128>;
 for _ in 0..MAX_PAGES{
   let req=build_okx_base_currency_bills_page_request(
     creds,end,domain,ccy,start,end,after.as_deref(),PAGE)
     .map_err(F2HError::Build)?;
   let path=req.url.strip_prefix(domain.base_url())
      .ok_or(deny("settlement host invalid"))?;
   let page=tape.get(path,&req)?;
   if page.is_empty(){return Ok(all)}
   for row in page{
     let n=id(&row,"billId")?;
     let ts=str_f(&row,"ts")?.parse::<u64>()
        .map_err(|_|deny("settlement bill timestamp invalid"))?;
     if ts<start||ts>end||last_id.is_some_and(|prev|n>=prev)
       ||last_ts.is_some_and(|prev|ts>prev){
        return Err(deny("settlement cursor/time order or original window broken"))}
     if str_f(&row,"ccy")?!=ccy{
        return Err(deny("settlement bill unexpected account currency"))}
     let change=decimal(&row,"balChg")?;
     let bal=decimal(&row,"bal")?;
     if expected_prior_balance.is_some_and(|prev|prev!=bal){
        return Err(deny("settlement account balance chain discontinuity"))}
     expected_prior_balance=Some(bal.checked_sub(change)
        .ok_or(deny("settlement cash chain arithmetic overflow"))?);
     last_id=Some(n);last_ts=Some(ts);
     after=Some(n.to_string());
     all.push(row);
   }
 }
 Err(deny("settlement bill pagination lacks explicit empty tail"))
}
#[derive(Clone,Debug,PartialEq,Eq)]
struct Snapshot{base:Vec<Value>,quote:Vec<Value>}
fn scan(tape:&mut Tape<'_>,creds:&OkxCredentials,domain:OkxDomain,
 base:&str,start:u64,end:u64)->Result<Snapshot,F2HError>{
 let b=one_currency(tape,creds,domain,base,start,end)?;
 let q=one_currency(tape,creds,domain,"USDT",start,end)?;
 Ok(Snapshot{base:b,quote:q})
}
fn reconcile_followup(snapshot:&Snapshot,prior:&F2GMockReview,first_end:u64,
 original_pair:&str,original_buy_ids:&BTreeSet<String>,original_sell_ids:&BTreeSet<String>,
 original_fill_times:&BTreeMap<String,u64>)
 ->Result<usize,F2HError>{
 let witnesses=&prior.narrow_detail_bill_topology.exact_detail_bill_witnesses;
 if witnesses.is_empty(){return Err(deny("no original F2G linked account bills"))}
 let mut expected=BTreeMap::new();
 for w in witnesses{
   if expected.insert(w.bill_id.as_str(),w).is_some(){
       return Err(deny("duplicate original F2G bill witness"))}
 }
 let mut used=BTreeSet::new();
 let mut totals=BTreeMap::<String,i128>::new();
 for v in snapshot.base.iter().chain(&snapshot.quote){
   let bill=str_f(v,"billId")?;
   let w=expected.get(bill).ok_or(
       deny("unexplained/late account bill: transfer, fee, interest or foreign event"))?;
   if !used.insert(bill){return Err(deny("same bill in multiple account currency scans"))}
   if str_f(v,"type")?!="2"||str_f(v,"instType")?!="SPOT"{
       return Err(deny("non-trade account class requires manual classification"))}
   let expected_side=if original_buy_ids.contains(&w.trade_id){"1"}
      else if original_sell_ids.contains(&w.trade_id){"2"}
      else{return Err(deny("bill cannot be matched to original owner side"))};
   if str_f(v,"subType")?!=expected_side
      ||str_f(v,"ordId")?!=w.order_id
      ||str_f(v,"tradeId")?!=w.trade_id
      ||str_f(v,"instId")?!=original_pair
      ||str_f(v,"ccy")?!=w.bill_balance_currency{
       return Err(deny("late bill direction, trade or account identity drift"))}
   let ts=str_f(v,"ts")?.parse::<u64>()
      .map_err(|_|deny("late bill time malformed"))?;
   if ts>first_end{return Err(deny("bill posted after original close window; manual"))}
   let fill_time=*original_fill_times.get(&w.trade_id)
       .ok_or(deny("original Native execution time missing for bill"))?;
   let expected_ts=fill_time.checked_add(w.bill_posted_after_trade_ms)
       .ok_or(deny("original bill posted time overflow"))?;
   if ts!=expected_ts{
      return Err(deny("late scan rewrote original persisted source bill timestamp"))}

   if decimal(v,"fee")?!=w.signed_fee_e12
      ||decimal(v,"balChg")?!=w.signed_bill_balance_change_e12{
      return Err(deny("late commission or balance change contradicts first receipt"))}
   let total=totals.entry(w.bill_balance_currency.clone()).or_insert(0);
   *total=total.checked_add(w.signed_bill_balance_change_e12)
      .ok_or(deny("late cash total overflow"))?;
 }
 if used.len()!=expected.len()||used.iter().any(|s|!expected.contains_key(s)){
     return Err(deny("known original cash bill disappeared from wider settlement scan"))}
 if totals!=prior.narrow_detail_bill_topology.observed_bill_delta_by_ccy_e12{
     return Err(deny("wider settlement account delta disagrees with original F2G"))}
 Ok(used.len())
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct F2HOfflineReview{
 pub original_generation:u64,
 pub signed_mock_gets_total:usize,
 pub original_bill_ids_crosschecked:usize,
 pub two_late_mock_scans_identical:bool,
 pub no_additional_bills_in_mock_window:bool,
 pub all_real_account_bill_classes_proven:bool,
 pub independent_real_source_attested:bool,
 pub all_delayed_postings_absent_proven:bool,
 pub all_binance_late_income_absent_proven:bool,
 pub entire_lifetime_net_pnl_e12:Option<i128>,
 pub real_ready:bool,
 pub verified_closed:bool,
 pub may_trade_cancel_transfer_or_release:bool,
}
/// Calls G's full persisted four-leg preflight first. Post-settlement
/// pages are scripted fake source only, never live private exchange reads.
pub fn inspect_owner_bound_late_okx_bills_offline(
 store:&NativeStateStore,expected_generation:u64,
 first_end:u64,late_end:u64,domain:OkxDomain,
 original_source:&[MockGetStep],late_source:&[MockGetStep],
)->Result<F2HOfflineReview,F2HError>{
 inspect_owner_bound_late_okx_bills_offline_inner(store,expected_generation,
  first_end,late_end,domain,original_source,late_source,||{})
}
/// Debug-only deterministic race injection. No mutation capability in
/// regular or release F2H entrypoint.
#[cfg(debug_assertions)]
#[doc(hidden)]
pub fn inspect_owner_bound_late_okx_bills_debug_after_first<F:FnOnce()>(
 store:&NativeStateStore,expected_generation:u64,
 first_end:u64,late_end:u64,domain:OkxDomain,
 original_source:&[MockGetStep],late_source:&[MockGetStep],after_first:F,
)->Result<F2HOfflineReview,F2HError>{
 inspect_owner_bound_late_okx_bills_offline_inner(store,expected_generation,
  first_end,late_end,domain,original_source,late_source,after_first)
}
fn inspect_owner_bound_late_okx_bills_offline_inner<F:FnOnce()>(
 store:&NativeStateStore,expected_generation:u64,
 first_end:u64,late_end:u64,domain:OkxDomain,
 original_source:&[MockGetStep],late_source:&[MockGetStep],after_first:F,
)->Result<F2HOfflineReview,F2HError>{
 if first_end==0||late_end<first_end
   ||late_end-first_end<MIN_OBSERVATION_LAG
   ||late_end-first_end>MAX_OBSERVATION_LAG{
   return Err(deny("settlement observation time too short/long for diagnostic"))}
 let initial=store.load().map_err(F2HError::Native)?
    .ok_or(F2HError::Native(NativeStateError::Generation))?;
 if initial.generation!=expected_generation{return Err(deny("original Native generation stale BEFORE GET"))}
 let prior=inspect_owner_bound_okx_offline_mock(
    store,expected_generation,first_end,domain,original_source)
    .map_err(F2HError::First)?;
 let entry=initial.entry.as_ref().ok_or(deny("original entry missing"))?;
 let start=entry.plan.created_at_ms;
 if late_end<start||late_end-start>6*24*60*60*1000{
    return Err(deny("settlement history exceeds bounded retention budget"))}
 let buy=&entry.spot_prehedge_evidence.as_ref()
    .ok_or(deny("Native opening fill absent"))?.exact_ledger;
 let close=entry.shadow_close_receipt.as_ref()
    .ok_or(deny("Native closing fill absent"))?;
 let sell=close.bounded_economic_ledgers().0;
 let buy_ids=buy.fills.keys().cloned().collect::<BTreeSet<_>>();
 let sell_ids=sell.fills.keys().cloned().collect::<BTreeSet<_>>();
 let mut original_times=BTreeMap::new();
 for (id,f) in buy.fills.iter().chain(sell.fills.iter()){
   let t=f.trade_time_ms.ok_or(deny("persisted original fill time absent"))?;
   if original_times.insert(id.clone(),t).is_some(){
      return Err(deny("duplicate original Native trade ID in settlement proof"))}
 }
 let creds=OkxCredentials::new("F2G_MOCK_ONLY_KEY","F2G_MOCK_ONLY_SECRET",
   "F2G_MOCK_ONLY_PASSPHRASE").map_err(F2HError::Build)?;
 let mut tape=Tape{steps:late_source,pos:0,base:domain.base_url()};
 let first=scan(&mut tape,&creds,domain,&entry.plan.base,start,late_end)?;
 let original_pair=entry.plan.spot_symbol.replace('/',"-");
 let count=reconcile_followup(&first,&prior,first_end,&original_pair,
     &buy_ids,&sell_ids,&original_times)?;
 after_first();
 if store.load().map_err(F2HError::Native)?.as_ref()!=Some(&initial){
     return Err(deny("Native owner CAS drift after first settlement scan"))}
 let second=scan(&mut tape,&creds,domain,&entry.plan.base,start,late_end)?;
 if first!=second{return Err(deny("new/delayed account bill appeared between settlement scans"))}
 if tape.pos!=late_source.len(){return Err(deny("extra unmatched settlement MOCK traffic"))}
 if store.load().map_err(F2HError::Native)?.as_ref()!=Some(&initial){
     return Err(deny("Native owner CAS drift after settlement scans"))}
 Ok(F2HOfflineReview{
  original_generation:expected_generation,
  signed_mock_gets_total:prior.signed_mock_gets+tape.pos,
  original_bill_ids_crosschecked:count,
  two_late_mock_scans_identical:true,
  no_additional_bills_in_mock_window:true,
  all_real_account_bill_classes_proven:false,
  independent_real_source_attested:false,
  all_delayed_postings_absent_proven:false,
  all_binance_late_income_absent_proven:false,
  entire_lifetime_net_pnl_e12:None,
  real_ready:false,verified_closed:false,may_trade_cancel_transfer_or_release:false,
 })
}
