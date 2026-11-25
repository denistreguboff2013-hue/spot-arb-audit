//! TRADE2F2G: signed OKX GET builders against an IN-MEMORY MOCK TAPE.
//! No HttpExecutor, network client, real credentials, orders or authority.
//! The tape proves only deterministic local source coherence, not that the
//! complete account history or any actual exchange response was obtained.
use std::collections::{BTreeMap,BTreeSet};
use serde_json::Value;
use crate::{
 authoritative_state::{NativeStateStore,NativeStateError},
 fill_economics::{LegFillLedger,decimal_to_atoms,signed_decimal_to_atoms},
 native_trade2f_owner_binding::original_four_leg_preflight,
 native_trade2f_okx_bill_topology::{inspect_documented_okx_trade_detail_to_bill_id,DiagnosticOkxTopology,OkxBillTopologyError},
 native_trade2f_asset_fee::{inspect_asset_fees_for_original_spot_ledgers,SpotFeeFill},
 private_rest::{OkxCredentials,OkxDomain,RestBuildError,HttpMethod,SignedRequest,
   build_okx_fills_history_page_request,build_okx_base_currency_bills_page_request},
};
const MAX_PAGES:u16=4;
const PAGE:u16=100;
const WINDOW:u64=6*24*60*60*1000;
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum F2GError{State(NativeStateError),Build(RestBuildError),Topology(OkxBillTopologyError),Deny(&'static str)}
fn no(s:&'static str)->F2GError{F2GError::Deny(s)}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct MockGetStep{
    /// Exact path AND query expected from the signed builder.
    pub path_and_query:String,
    pub http_status:u16,
    pub body:String,
}
struct Tape<'a>{steps:&'a [MockGetStep],next:usize,base:&'static str}
impl Tape<'_>{
 fn get(&mut self,request:&SignedRequest)->Result<Vec<Value>,F2GError>{
    if request.method!=HttpMethod::Get||request.is_mutation||request.body.is_some()
      ||!request.headers.iter().any(|(k,v)|k=="OK-ACCESS-SIGN"&&!v.is_empty()){
       return Err(no("not a read-only signed mock GET"))
    }
    let p=request.url.strip_prefix(self.base).ok_or(no("foreign exchange mock URL"))?;
    let step=self.steps.get(self.next).ok_or(no("mock response tape exhausted"))?;
    if step.path_and_query!=p{return Err(no("mock requested unexpected path or cursor"))}
    self.next+=1;
    if step.http_status!=200{return Err(no("mock HTTP status not 200"))}
    let j:Value=serde_json::from_str(&step.body).map_err(|_|no("mock REST response invalid JSON"))?;
    if j.get("code").and_then(Value::as_str)!=Some("0"){return Err(no("mock OKX nonzero business code"))}
    let rows=j.get("data").and_then(Value::as_array).ok_or(no("mock OKX response data missing"))?;
    if rows.len()>PAGE as usize{return Err(no("mock OKX page size exceeded"))}
    Ok(rows.clone())
 }
}
fn id(row:&Value,k:&str)->Result<u128,F2GError>{
 let s=row.get(k).and_then(Value::as_str).ok_or(no("source identity missing"))?;
 if s.is_empty()||!s.bytes().all(|c|c.is_ascii_digit())||s.starts_with('0'){
     return Err(no("source identity not canonical numeric"))}
 s.parse::<u128>().ok().filter(|x|*x>0).ok_or(no("source identity invalid"))
}
fn str_field<'a>(row:&'a Value,k:&str)->Result<&'a str,F2GError>{
 row.get(k).and_then(Value::as_str).ok_or(no("source field absent or not string"))
}
fn page_ordered(tape:&mut Tape<'_>, mut build:impl FnMut(Option<&str>)->Result<SignedRequest,RestBuildError>)
    ->Result<Vec<Value>,F2GError>{
 let mut rows=Vec::new();
 let mut cursor=None::<String>;
 let mut last=None::<u128>;
 let mut seen=BTreeSet::new();
 for _ in 0..MAX_PAGES{
    let response=tape.get(&build(cursor.as_deref()).map_err(F2GError::Build)?)?;
    if response.is_empty(){return Ok(rows)}
    for r in response{
       let n=id(&r,"billId")?;
       if last.is_some_and(|prev|n>=prev)||!seen.insert(n){
          return Err(no("mock cursor duplicated, reversed, or nonmonotonic"))}
       last=Some(n);
       cursor=Some(n.to_string());
       rows.push(r);
    }
 }
 Err(no("mock history pagination budget exhausted without empty tail"))
}
fn sort_bill_id_desc(rows:&mut [Value])->Result<(),F2GError>{
 let mut ids=Vec::with_capacity(rows.len());
 for v in rows.iter(){ids.push(id(v,"billId")?)}
 // Using a numeric key avoids the lexical order "9" > "10" pitfall.
 rows.sort_by(|a,b|{
    let a=a.get("billId").and_then(Value::as_str).unwrap_or("0").parse::<u128>().unwrap_or(0);
    let b=b.get("billId").and_then(Value::as_str).unwrap_or("0").parse::<u128>().unwrap_or(0);
    b.cmp(&a)
 });
 if ids.len()!=ids.iter().collect::<BTreeSet<_>>().len(){
    return Err(no("duplicate bill identity across source scopes"))}
 Ok(())
}
#[derive(Clone,Debug,PartialEq,Eq)]
struct MockSnapshot{details:Vec<Value>,bills:Vec<Value>}
fn scan(tape:&mut Tape<'_>,creds:&OkxCredentials,now:u64,domain:OkxDomain,
     symbol:&str,base:&str,start:u64,orders:&[String;2])->Result<MockSnapshot,F2GError>{
 let mut details=Vec::new();
 for order in orders{
    let found=page_ordered(tape,|after|build_okx_fills_history_page_request(
       creds,now,domain,symbol,order,after,PAGE))?;
    for f in &found{
       if str_field(f,"ordId")?!=order{return Err(no("OKX detail belongs to other order"))}
    }
    details.extend(found);
 }
 let mut bills=Vec::new();
 for currency in [base,"USDT"]{
    let found=page_ordered(tape,|after|build_okx_base_currency_bills_page_request(
       creds,now,domain,currency,start,now,after,PAGE))?;
    for b in &found{
       if str_field(b,"ccy")?!=currency{return Err(no("OKX bill belongs to other currency"))}
    }
    bills.extend(found);
 }
 sort_bill_id_desc(&mut details)?;
 sort_bill_id_desc(&mut bills)?;
 Ok(MockSnapshot{details,bills})
}
fn prove_native_trade_ids(snapshot:&MockSnapshot,buy:&LegFillLedger,sell:&LegFillLedger)
    ->Result<(),F2GError>{
 let mut expected=BTreeMap::new();
 for (ledger,side) in [(buy,"buy"),(sell,"sell")]{
   let ord=ledger.exchange_order_id.as_deref().ok_or(no("original spot order ID absent"))?;
   for (k,f) in &ledger.fills{
     if k!=&f.fill_id||expected.insert(k.as_str(),(ord,side,f)).is_some(){
        return Err(no("original native fill IDs missing or duplicated"))}
   }
 }
 if expected.is_empty()||snapshot.details.len()!=expected.len(){
    return Err(no("OKX transaction count does not match persisted native fills"))}
 for row in &snapshot.details{
   let tid=str_field(row,"tradeId")?;
   let (order,side,f)=expected.remove(tid).ok_or(no("foreign OKX fill not in Native owner"))?;
   if str_field(row,"ordId")?!=order||str_field(row,"side")?!=side{
      return Err(no("Native original order/side mismatched"))}
   if decimal_to_atoms(str_field(row,"fillSz")?).ok()!=Some(f.qty_atoms)
     ||decimal_to_atoms(str_field(row,"fillPx")?).ok()!=Some(f.price_atoms)
     ||str_field(row,"fillTime")?.parse::<u64>().ok()!=f.trade_time_ms{
      return Err(no("OKX execution differs from persisted Native price/qty/time"))}
   let fee=signed_decimal_to_atoms(str_field(row,"fee")?)
      .map_err(|_|no("OKX original source fee not exact"))?;
   if str_field(row,"feeCcy")?=="USDT" && f.fee_cost_quote_atoms!=fee.checked_neg(){
       return Err(no("original Native USDT commission differs from sourced feeCcy"))}
 }
 if !expected.is_empty(){return Err(no("missing original Native fills"))}
 Ok(())
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct F2GMockReview{
 pub original_owner_generation:u64,
 pub two_full_identical_mock_scans:bool,
 pub signed_mock_gets:usize,
 pub narrow_detail_bill_topology:DiagnosticOkxTopology,
 pub complete_account_history_proven:bool,
 pub independent_real_source_attested:bool,
 pub full_lifetime_pnl_e12:Option<i128>,
 pub verified_closed:bool,
 pub real_ready:bool,
 pub may_send_cancel_retry_or_transfer:bool,
}
/// Immutable persisted owner + 2 source-shaped, exactly scripted signed MOCK
/// history rounds. No real HTTP facility is accepted by this function.
pub fn inspect_owner_bound_okx_offline_mock(
 store:&NativeStateStore,expected_generation:u64,
 now:u64,domain:OkxDomain,tape_steps:&[MockGetStep],
)->Result<F2GMockReview,F2GError>{
 inspect_owner_bound_okx_offline_mock_inner(
   store,expected_generation,now,domain,tape_steps,||{})
}
/// Debug-only injected mutation between scan rounds proves the CAS guard
/// fails closed. Not present in release/production binaries.
#[cfg(debug_assertions)]
#[doc(hidden)]
pub fn inspect_owner_bound_okx_offline_mock_debug_after_first<F:FnOnce()>(
 store:&NativeStateStore,expected_generation:u64,
 now:u64,domain:OkxDomain,tape_steps:&[MockGetStep],after_first:F,
)->Result<F2GMockReview,F2GError>{
 inspect_owner_bound_okx_offline_mock_inner(
   store,expected_generation,now,domain,tape_steps,after_first)
}
fn inspect_owner_bound_okx_offline_mock_inner<F:FnOnce()>(
 store:&NativeStateStore,expected_generation:u64,
 now:u64,domain:OkxDomain,tape_steps:&[MockGetStep],after_first:F,
)->Result<F2GMockReview,F2GError>{
 let initial=store.load().map_err(F2GError::State)?
    .ok_or(F2GError::State(NativeStateError::Generation))?;
 if initial.generation!=expected_generation{return Err(no("stale original generation before GET"))}
 original_four_leg_preflight(&initial)
    .map_err(|_|no("four-leg original owner preflight denied before GET"))?;
 let entry=initial.entry.as_ref().ok_or(no("Native entry missing"))?;
 let start=entry.plan.created_at_ms;
 if start==0||now<start||now-start>WINDOW||now-start>89*24*60*60*1000{
    return Err(no("source window outside supported 3-month/6-day bounds"))}
 let open=&entry.spot_prehedge_evidence.as_ref()
    .ok_or(no("original spot buy absent"))?.exact_ledger;
 let close=entry.shadow_close_receipt.as_ref().ok_or(no("original spot sell absent"))?;
 let (sell,futures)=close.bounded_economic_ledgers();
 let orders=[
   open.exchange_order_id.clone().ok_or(no("opening exchange order ID absent"))?,
   sell.exchange_order_id.clone().ok_or(no("closing exchange order ID absent"))?,
 ];
 for order in &orders{
   if order=="0"||order.is_empty()||!order.bytes().all(|b|b.is_ascii_digit()){
      return Err(no("non-numeric persisted OKX order ID before GET"))}
 }
 for ledger in [open,sell]{
   if ledger.fills.is_empty()||ledger.fills.keys().any(|id|
      id=="0"||id.is_empty()||!id.bytes().all(|b|b.is_ascii_digit())){
       return Err(no("non-numeric persisted OKX trade ID before GET"))}
 }
 // Hardwired mock-only credentials cannot coincide with live keys.
 let creds=OkxCredentials::new("F2G_MOCK_ONLY_KEY","F2G_MOCK_ONLY_SECRET",
    "F2G_MOCK_ONLY_PASSPHRASE").map_err(F2GError::Build)?;
 let mut tape=Tape{steps:tape_steps,next:0,base:domain.base_url()};
 let first=scan(&mut tape,&creds,now,domain,&entry.plan.spot_symbol,
    &entry.plan.base,start,&orders)?;
 after_first();
 if store.load().map_err(F2GError::State)?.as_ref()!=Some(&initial){
    return Err(no("Native owner changed during first MOCK read"))}
 let second=scan(&mut tape,&creds,now,domain,&entry.plan.spot_symbol,
    &entry.plan.base,start,&orders)?;
 if first!=second{return Err(no("two complete signed MOCK rounds disagreed"))}
 if tape.next!=tape.steps.len(){return Err(no("unexpected unconsumed mock traffic"))}
 prove_native_trade_ids(&first,open,sell)?;
 // The opening per-fill billId had already been persisted and sealed by
 // the earlier first-leg source auditor. Require the F2G response to
 // corroborate that exact ancestry, not just arbitrary fresh bill IDs.
 let prior_bills=&entry.spot_prehedge_evidence.as_ref()
    .ok_or(no("opening Native bill lineage absent"))?.fill_bill_ids;
 let mut now_open_bills=BTreeSet::new();
 for row in &first.details{
    if str_field(row,"side")?=="buy"{
      now_open_bills.insert(str_field(row,"billId")?.to_string());
    }
 }
 let prior_set:BTreeSet<String>=prior_bills.iter().cloned().collect();
 if prior_set.is_empty()||prior_set.len()!=prior_bills.len()
    ||prior_set!=now_open_bills{
    return Err(no("OKX billId no longer matches persisted first-leg source ancestry"))}
 let report=inspect_documented_okx_trade_detail_to_bill_id(
    &entry.plan.spot_symbol,futures.filled_qty_atoms(),start,now,
    &first.details,&first.bills).map_err(F2GError::Topology)?;
 // Match fee denomination with the persisted BUY/SELL and actual original
 // Binance SHORT, not merely the caller's artificial JSON or bill ccy.
 let mut source_fees=Vec::with_capacity(first.details.len());
 for row in &first.details{
    let at_ms=str_field(row,"fillTime")?.parse::<u64>()
       .map_err(|_|no("OKX fill time not numeric"))?;
    source_fees.push(SpotFeeFill{
       order_id:str_field(row,"ordId")?.into(),
       trade_id:str_field(row,"tradeId")?.into(),
       at_ms,side:str_field(row,"side")?.into(),
       inst_id:str_field(row,"instId")?.into(),
       price_quote_e12:decimal_to_atoms(str_field(row,"fillPx")?)
          .map_err(|_|no("bad OKX source fillPx"))?,
       gross_base_qty_e12:decimal_to_atoms(str_field(row,"fillSz")?)
          .map_err(|_|no("bad OKX source fillSz"))?,
       fee_ccy:str_field(row,"feeCcy")?.into(),
       signed_fee_e12:signed_decimal_to_atoms(str_field(row,"fee")?)
          .map_err(|_|no("bad OKX sourced fee"))?,
    });
 }
 let original_short=&entry.recovery_checkpoint.as_ref()
    .ok_or(no("original future short source absent"))?.ledger;
 let independent=inspect_asset_fees_for_original_spot_ledgers(
   open,sell,original_short,&source_fees)
    .map_err(|_|no("original Native fee-currency/position conservation violated"))?;
 if independent!=report.fee_adjusted_inventory{
    return Err(no("two independent source-asset validators disagree"))}
 if !report.fee_adjusted_inventory.original_future_short_matches_delivered_spot
  ||!report.fee_adjusted_inventory.completely_self_funded_spot_unwind{
    return Err(no("spot asset fees create unhedged or externally funded residual"))}
 if store.load().map_err(F2GError::State)?.as_ref()!=Some(&initial){
    return Err(no("Native owner CAS changed after source reconciliation"))}
 Ok(F2GMockReview{
    original_owner_generation:expected_generation,
    two_full_identical_mock_scans:true,signed_mock_gets:tape.next,
    narrow_detail_bill_topology:report,
    complete_account_history_proven:false,
    independent_real_source_attested:false,
    full_lifetime_pnl_e12:None,verified_closed:false,real_ready:false,
    may_send_cancel_retry_or_transfer:false,
 })
}


#[cfg(test)]
mod tests{
 use super::*;
 use crate::fill_economics::{FillEvent,LegFillLedger};
 use serde_json::json;
 fn ok(data:Vec<Value>)->String{json!({"code":"0","data":data}).to_string()}
 fn step(path:String,data:Vec<Value>)->MockGetStep{
    MockGetStep{path_and_query:path,http_status:200,body:ok(data)}
 }
 fn row_fill(ord:&str,tid:&str,bill:&str,side:&str,sz:&str,price:&str,time:&str)->Value{
    json!({"instType":"SPOT","instId":"BNT-USDT","tradeQuoteCcy":"USDT",
      "side":side,"subType":if side=="buy"{"1"}else{"2"},
      "ordId":ord,"tradeId":tid,"billId":bill,"fillSz":sz,"fillPx":price,
      "fee":"-0.01","feeCcy":"USDT","fillTime":time,
      "ts":if side=="buy"{"2001"}else{"3001"}})
 }
 fn row_bill(ord:&str,tid:&str,bill:&str,side:&str,bal:&str,chg:&str,ts:&str)->Value{
   json!({"instType":"SPOT","instId":"BNT-USDT","type":"2",
    "subType":if side=="buy"{"1"}else{"2"},"ccy":"USDT",
    "ordId":ord,"tradeId":tid,"billId":bill,"fee":"-0.01",
    "bal":bal,"balChg":chg,"ts":ts})
 }
 fn original(side:&str,ord:&str,id:&str,px:&str,qty:&str,time:u64)->LegFillLedger{
   let mut l=LegFillLedger::new("okx","BNT/USDT",side,"i","c",decimal_to_atoms(qty).unwrap()).unwrap();
   l.exchange_order_id=Some(ord.into());
   l.apply_fill(FillEvent{fill_id:id.into(),price_atoms:decimal_to_atoms(px).unwrap(),
     qty_atoms:decimal_to_atoms(qty).unwrap(),fee_cost_quote_atoms:Some(10_000_000_000),
     fee_currency:Some("USDT".into()),trade_time_ms:Some(time)},0).unwrap();
   l
 }
 fn path_fill(ord:&str,after:Option<&str>)->String{
   let mut p=format!("/api/v5/trade/fills-history?instType=SPOT&instId=BNT-USDT&ordId={ord}&limit=100");
   if let Some(id)=after{p.push_str("&after=");p.push_str(id);}
   p
 }
 fn path_bill(ccy:&str,after:Option<&str>)->String{
   let mut p=format!("/api/v5/account/bills-archive?ccy={ccy}&begin=1000&end=4000&limit=100");
   if let Some(id)=after{p.push_str("&after=");p.push_str(id);}
   p
 }
 fn scripted()->Vec<MockGetStep>{
   let mut v=Vec::new();
   for _ in 0..2{
     v.push(step(path_fill("701",None),
         vec![row_fill("701","901","801","buy","1","20","2000")]));
     v.push(step(path_fill("701",Some("801")),vec![]));
     v.push(step(path_fill("702",None),
         vec![row_fill("702","902","802","sell","1","20.2","3000")]));
     v.push(step(path_fill("702",Some("802")),vec![]));
     v.push(step(path_bill("BNT",None),vec![]));
     v.push(step(path_bill("USDT",None),vec![
       row_bill("702","902","802","sell","100.18","20.19","3005"),
       row_bill("701","901","801","buy","79.99","-20.01","2005"),
     ]));
     v.push(step(path_bill("USDT",Some("801")),vec![]));
   }
   v
 }
 fn simulate(tape_steps:&[MockGetStep])->Result<DiagnosticOkxTopology,F2GError>{
   let cred=OkxCredentials::new("F2G_MOCK_ONLY_KEY","F2G_MOCK_ONLY_SECRET",
       "F2G_MOCK_ONLY_PASSPHRASE").unwrap();
   let mut tape=Tape{steps:tape_steps,next:0,base:OkxDomain::Global.base_url()};
   let ord=["701".into(),"702".into()];
   let a=scan(&mut tape,&cred,4000,OkxDomain::Global,"BNT/USDT","BNT",1000,&ord)?;
   let b=scan(&mut tape,&cred,4000,OkxDomain::Global,"BNT/USDT","BNT",1000,&ord)?;
   if a!=b||tape.next!=tape_steps.len(){return Err(no("source drift or trailing mock reads"))}
   prove_native_trade_ids(&a,
       &original("buy","701","901","20","1",2000),
       &original("sell","702","902","20.2","1",3000))?;
   inspect_documented_okx_trade_detail_to_bill_id(
      "BNT/USDT",decimal_to_atoms("1").unwrap(),1000,4000,
      &a.details,&a.bills).map_err(F2GError::Topology)
 }
 #[test]
 fn trade2f2g_offline_signed_mock_pages_match_exact_native_ids_and_fee_ccy(){
    let report=simulate(&scripted()).unwrap();
    assert!(report.fee_adjusted_inventory.original_future_short_matches_delivered_spot);
    assert!(report.fee_adjusted_inventory.completely_self_funded_spot_unwind);
    assert_eq!(report.matched_bill_ids,vec!["801".to_string(),"802".to_string()]);
    assert!(!report.independent_authenticated_signed_history);
    assert!(!report.may_send_cancel_retry_or_mark_verified_closed);
    assert_eq!(report.full_lifetime_net_pnl_e12,None);
 }
 #[test]
 fn trade2f2g_hostile_missing_drift_status_cursor_source_and_fee_all_deny(){
    for fault in 0..12{
       let mut tape=scripted();
       match fault{
         0=>{tape.remove(6);},
         1=>{tape[0].http_status=403;},
         2=>{tape[0].http_status=429;},
         3=>{tape[0].body="{".into();},
         4=>{tape[0].body=ok(vec![row_fill("701","901","801","buy","1","20","2000"),
              row_fill("701","901","801","buy","1","20","2000")]);},
         5=>{tape[0].body=ok(vec![row_fill("701","901","801","buy","1","20","2000"),
              row_fill("701","903","803","buy","1","20","2000")]);},
         6=>{tape[0].path_and_query.push_str("&before=999");},
         7=>{tape[7].body=ok(vec![row_fill("701","901","801","buy","1.01","20","2000")]);},
         8=>{let mut j:Value=serde_json::from_str(&tape[0].body).unwrap();
              j["data"][0]["feeCcy"]=json!("BNB");tape[0].body=j.to_string();},
         9=>{let mut j:Value=serde_json::from_str(&tape[5].body).unwrap();
              j["data"][0]["balChg"]=json!("123");tape[5].body=j.to_string();},
         10=>{let mut j:Value=serde_json::from_str(&tape[5].body).unwrap();
              j["data"][0]["billId"]=json!("999");tape[5].body=j.to_string();},
         _=>{tape[0].body=ok(vec![]);},
       }
       assert!(simulate(&tape).is_err(),"fault {fault} must DENY");
    }
 }
}
