//! R32D6C8B2B2A: read-only TWO-close-order proof candidate, NEVER closure.
//! GET only; requires explicitly identified close order IDs and client IDs.
//! The caller cannot turn these signed responses into a verified flat account.
//! One bounded fills page plus an explicitly empty tail are required; if a
//! history may exceed one page this deliberately fails and needs B2B2B.
use crate::{
 fill_economics::{LegFillLedger,DEFAULT_AVG_TOLERANCE_ATOMS,signed_decimal_to_atoms},
 private_https::{HttpExecutor,HttpsRequest,TransportError},
 private_rest::{
  BinanceCredentials,OkxCredentials,OkxDomain,RestBuildError,HttpMethod,SignedRequest,
  build_okx_query_by_client_id,build_binance_query_by_client_id,
  build_okx_fills_history_page_request,build_binance_user_trades_page_request,
  parse_okx_economic_truth_http,parse_binance_economic_truth_http,
  parse_okx_fill_events_http,parse_binance_fill_events_http,
  apply_order_economic_truth,EconomicTruthClass,
 },
};
use serde_json::Value;
use std::collections::BTreeSet;

#[derive(Clone,Debug,PartialEq,Eq)]
pub enum DualCloseError {
 Invalid(&'static str),Build(RestBuildError),Transport(TransportError),
 UnexhaustedHistory,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct CloseOrderIdentity {
 pub symbol:String,pub client_id:String,pub exchange_order_id:String,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct DualCloseCandidate {
 pub position_identity:String,pub base:String,
 pub created_at_ms:u64,pub expected_qty_e12:u128,
 pub spot:CloseOrderIdentity,pub futures:CloseOrderIdentity,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct DualCloseReadOnlyCandidate {
 source_position_identity:String,
 spot:LegFillLedger,futures:LegFillLedger,
 okx_fill_count:usize,futures_fill_count:usize,
 /// Binance's reported PnL for these CLOSE trades only, NOT whole-cycle net.
 futures_reported_realized_pnl_e12:i128,
 order_reads:u8,fill_reads:u8,
 full_account_cash_bracket_proven:bool,
 futures_position_flat_proven:bool,
 funding_and_all_income_complete:bool,
 full_currency_bills_history_complete:bool,
 exchange_global_history_complete:bool,
 cross_venue_atomicity_proven:bool,
 may_mark_verified_closed:bool,may_send_order:bool,
}
// Opaque candidate: no caller may set safety booleans or invent a proof.
// In particular do not use the returned leg ledgers as closure authority.
impl DualCloseReadOnlyCandidate {
 pub fn position_identity(&self)->&str{&self.source_position_identity}
 pub fn spot_ledger(&self)->&LegFillLedger{&self.spot}
 pub fn futures_ledger(&self)->&LegFillLedger{&self.futures}
 pub fn fill_counts(&self)->(usize,usize){
  (self.okx_fill_count,self.futures_fill_count)
 }
 pub fn read_counts(&self)->(u8,u8){(self.order_reads,self.fill_reads)}
 pub fn futures_reported_realized_pnl_e12(&self)->i128{
  self.futures_reported_realized_pnl_e12
 }
 pub fn close_verified(&self)->bool{self.may_mark_verified_closed}
 pub fn may_send_order(&self)->bool{self.may_send_order}
 pub fn all_wallet_balance_brackets_proven(&self)->bool{self.full_account_cash_bracket_proven}
 pub fn futures_flat_proven(&self)->bool{self.futures_position_flat_proven}
 pub fn funding_complete(&self)->bool{self.funding_and_all_income_complete}
 pub fn currency_bills_complete(&self)->bool{self.full_currency_bills_history_complete}
 pub fn global_history_complete(&self)->bool{self.exchange_global_history_complete}
 pub fn cross_venue_atomicity_proven(&self)->bool{self.cross_venue_atomicity_proven}
}
fn number(s:&str)->bool{!s.is_empty()&&s.len()<=40&&s.bytes().all(|b|b.is_ascii_digit())}
fn cid(s:&str)->bool{
 !s.is_empty()&&s.len()<=96&&s.bytes().all(|b|
 b.is_ascii_alphanumeric()||b==b'-'||b==b'_'||b==b'.')
}
fn field<'a>(v:&'a Value,key:&str)->Result<&'a str,DualCloseError>{
 v.get(key).and_then(Value::as_str).ok_or(DualCloseError::Invalid("close field absent"))
}
fn idfield(v:&Value,key:&str)->Result<String,DualCloseError>{
 v.get(key).and_then(|x|x.as_str().map(str::to_owned).or_else(||
  x.as_u64().map(|x|x.to_string())))
  .ok_or(DualCloseError::Invalid("close numeric ID missing"))
}
fn query<E:HttpExecutor>(exec:&E,req:&SignedRequest)->Result<Value,DualCloseError>{
 if req.method!=HttpMethod::Get||req.is_mutation||req.body.is_some(){
  return Err(DualCloseError::Invalid("not a read-only GET"))
 }
 let reply=exec.execute_request(&HttpsRequest::from_signed(req))
    .map_err(DualCloseError::Transport)?;
 if reply.status!=200{return Err(DualCloseError::Invalid("GET HTTP status not 200"))}
 serde_json::from_str(&reply.body).map_err(|_|DualCloseError::Invalid("GET JSON invalid"))
}
fn encode(v:&Value)->Result<String,DualCloseError>{
 serde_json::to_string(v).map_err(|_|DualCloseError::Invalid("JSON serialization"))
}
fn okx_rows(v:&Value)->Result<&Vec<Value>,DualCloseError>{
 if v.get("code").and_then(Value::as_str)!=Some("0"){
  return Err(DualCloseError::Invalid("OKX business code not zero"))
 }
 v.get("data").and_then(Value::as_array)
   .ok_or(DualCloseError::Invalid("OKX data missing"))
}
fn binance_rows(v:&Value)->Result<&Vec<Value>,DualCloseError>{
 v.as_array().ok_or(DualCloseError::Invalid("Binance trades not array"))
}
fn validate_request(c:&DualCloseCandidate,now:u64,okx_limit:u16,bin_limit:u16,window:u64)->Result<(),DualCloseError>{
 if !cid(&c.position_identity)||c.base.is_empty()||c.base.len()>15
   ||!c.base.bytes().all(|b|b.is_ascii_uppercase())
   ||!cid(&c.spot.client_id)||!cid(&c.futures.client_id)
   ||c.spot.client_id==c.futures.client_id
   ||!number(&c.spot.exchange_order_id)||!number(&c.futures.exchange_order_id)
   ||c.spot.symbol!=format!("{}/USDT",c.base)
   ||c.futures.symbol!=format!("{}USDT",c.base)
   ||c.created_at_ms==0||now<c.created_at_ms
   ||now-c.created_at_ms>600_000
   ||c.expected_qty_e12==0||okx_limit<2||okx_limit>100
   ||bin_limit<2||bin_limit>1000||window==0||window>60_000{
   return Err(DualCloseError::Invalid("dual close identities/window invalid"))
 }
 Ok(())
}
#[allow(clippy::too_many_arguments)]
pub fn audit_dual_close_order_receipts_once<E:HttpExecutor>(
 exec:&E,c:&DualCloseCandidate,okx:&OkxCredentials,bin:&BinanceCredentials,
 now_ms:u64,domain:OkxDomain,recv_window_ms:u64,okx_limit:u16,bin_limit:u16,
)->Result<DualCloseReadOnlyCandidate,DualCloseError>{
 validate_request(c,now_ms,okx_limit,bin_limit,recv_window_ms)?;
 let spot_req=build_okx_query_by_client_id(
   okx,now_ms,domain,&c.spot.symbol,&c.spot.client_id).map_err(DualCloseError::Build)?;
 let future_req=build_binance_query_by_client_id(
   bin,now_ms,recv_window_ms,&c.futures.symbol,&c.futures.client_id)
   .map_err(DualCloseError::Build)?;
 let spot_json=query(exec,&spot_req)?;
 let okx_order=okx_rows(&spot_json)?;
 if okx_order.len()!=1{return Err(DualCloseError::Invalid("OKX close order ambiguous"))}
 let spot_row=&okx_order[0];
 if field(spot_row,"side")?!="sell"
   ||field(spot_row,"instId")?!=c.spot.symbol.replace('/',"-")
   ||idfield(spot_row,"ordId")?!=c.spot.exchange_order_id
   ||field(spot_row,"clOrdId")?!=c.spot.client_id
   ||field(spot_row,"state")?!="filled"{
  return Err(DualCloseError::Invalid("OKX close direction/identity/status"))
 }
 let spot_truth=parse_okx_economic_truth_http(200,&encode(&spot_json)?);
 if spot_truth.class!=EconomicTruthClass::Known||!spot_truth.terminal
  ||spot_truth.cumulative_qty_atoms!=Some(c.expected_qty_e12){
  return Err(DualCloseError::Invalid("OKX close total not confirmed"))
 }
 let fut_json=query(exec,&future_req)?;
 if field(&fut_json,"side")?!="BUY"
  ||field(&fut_json,"symbol")?!=c.futures.symbol
  ||field(&fut_json,"positionSide")?!="BOTH"
  ||fut_json.get("reduceOnly").and_then(Value::as_bool)!=Some(true)
  ||idfield(&fut_json,"orderId")?!=c.futures.exchange_order_id
  ||field(&fut_json,"clientOrderId")?!=c.futures.client_id
  ||field(&fut_json,"status")?!="FILLED"{
   return Err(DualCloseError::Invalid("Binance close direction/reduceOnly/identity/status"))
 }
 let fut_truth=parse_binance_economic_truth_http(200,&encode(&fut_json)?);
 if fut_truth.class!=EconomicTruthClass::Known||!fut_truth.terminal
  ||fut_truth.cumulative_qty_atoms!=Some(c.expected_qty_e12){
  return Err(DualCloseError::Invalid("Binance close total not confirmed"))
 }
 let mut spot=LegFillLedger::new("okx",&c.spot.symbol,"sell",&c.position_identity,
   &c.spot.client_id,c.expected_qty_e12)
    .map_err(|_|DualCloseError::Invalid("OKX close ledger init"))?;
 let mut fut=LegFillLedger::new("binance",&c.futures.symbol,"buy",&c.position_identity,
   &c.futures.client_id,c.expected_qty_e12)
    .map_err(|_|DualCloseError::Invalid("Binance close ledger init"))?;
 apply_order_economic_truth(&mut spot,&spot_truth)
   .map_err(|_|DualCloseError::Invalid("OKX close cumulative economics invalid"))?;
 apply_order_economic_truth(&mut fut,&fut_truth)
   .map_err(|_|DualCloseError::Invalid("Binance close cumulative economics invalid"))?;
 let spot_req=build_okx_fills_history_page_request(
   okx,now_ms,domain,&c.spot.symbol,&c.spot.exchange_order_id,None,okx_limit)
    .map_err(DualCloseError::Build)?;
 let spot_raw=query(exec,&spot_req)?;
 let spot_rows=okx_rows(&spot_raw)?;
 if spot_rows.is_empty()||spot_rows.len()>=okx_limit as usize{
  return Err(DualCloseError::UnexhaustedHistory)
 }
 let mut cursor=None::<String>;
 let mut prev_bill=None::<u128>;
 let mut ids=BTreeSet::new();
 let fill_events=parse_okx_fill_events_http(200,&encode(&spot_raw)?,
   &c.spot.exchange_order_id,&c.base,"USDT")
    .map_err(|_|DualCloseError::Invalid("OKX individual close fills invalid"))?;
 if fill_events.len()!=spot_rows.len(){return Err(DualCloseError::Invalid("foreign OKX close order fill"))}
 for (row,event) in spot_rows.iter().zip(fill_events){
  if idfield(row,"ordId")?!=c.spot.exchange_order_id
   ||field(row,"feeCcy")?!="USDT"
   ||event.fee_cost_quote_atoms.map_or(true,|x|x<0)
   ||event.trade_time_ms.map_or(true,|x|x<c.created_at_ms||x>now_ms.saturating_add(5_000))
   ||!ids.insert(event.fill_id.clone()){
   return Err(DualCloseError::Invalid("OKX close fee/time/order/ID mismatch"))
  }
  let bill=idfield(row,"billId")?;
  if !number(&bill){return Err(DualCloseError::Invalid("OKX bill cursor invalid"))}
  let v=bill.parse::<u128>().map_err(|_|DualCloseError::Invalid("OKX bill ID overflow"))?;
  if v==0||prev_bill.is_some_and(|p|v>=p){
   return Err(DualCloseError::Invalid("OKX descending bill IDs invalid"))
  }
  prev_bill=Some(v);cursor=Some(bill);
  spot.apply_fill(event,DEFAULT_AVG_TOLERANCE_ATOMS)
     .map_err(|_|DualCloseError::Invalid("OKX close cumulative fill mismatch"))?;
 }
 let tail_req=build_okx_fills_history_page_request(
   okx,now_ms,domain,&c.spot.symbol,&c.spot.exchange_order_id,
   cursor.as_deref(),okx_limit).map_err(DualCloseError::Build)?;
 if !okx_rows(&query(exec,&tail_req)?)?.is_empty(){return Err(DualCloseError::UnexhaustedHistory)}
 let fut_req=build_binance_user_trades_page_request(
   bin,now_ms,recv_window_ms,&c.futures.symbol,&c.futures.exchange_order_id,0,bin_limit)
    .map_err(DualCloseError::Build)?;
 let fut_raw=query(exec,&fut_req)?;
 let fut_rows=binance_rows(&fut_raw)?;
 if fut_rows.is_empty()||fut_rows.len()>=bin_limit as usize{
  return Err(DualCloseError::UnexhaustedHistory)
 }
 let events=parse_binance_fill_events_http(200,&encode(&fut_raw)?,
    &c.futures.exchange_order_id,"USDT")
    .map_err(|_|DualCloseError::Invalid("Binance individual close fills invalid"))?;
 if events.len()!=fut_rows.len(){return Err(DualCloseError::Invalid("foreign futures close order fill"))}
 let mut prev_id=None::<u64>;let mut ids=BTreeSet::new();
 let mut reported_pnl=0i128;
 for (row,event) in fut_rows.iter().zip(events){
  let id=idfield(row,"id")?.parse::<u64>()
    .map_err(|_|DualCloseError::Invalid("Binance numeric close fill ID invalid"))?;
  if id==0||prev_id.is_some_and(|p|id<=p)
   ||idfield(row,"orderId")?!=c.futures.exchange_order_id
   ||field(row,"symbol")?!=c.futures.symbol
   ||field(row,"side")?!="BUY"
   ||field(row,"positionSide")?!="BOTH"
   ||field(row,"marginAsset")?!="USDT"
   ||row.get("buyer").and_then(Value::as_bool)!=Some(true)
   ||field(row,"commissionAsset")?!="USDT"
   ||event.fee_cost_quote_atoms.map_or(true,|x|x<0)
   ||event.trade_time_ms.map_or(true,|x|x<c.created_at_ms||x>now_ms.saturating_add(5_000))
   ||!ids.insert(event.fill_id.clone()){
   return Err(DualCloseError::Invalid("Binance close fee/qty/side/time/id invalid"))
  }
  let realized=signed_decimal_to_atoms(field(row,"realizedPnl")?)
   .map_err(|_|DualCloseError::Invalid("Binance per-trade realizedPnL malformed"))?;
  reported_pnl=reported_pnl.checked_add(realized)
   .ok_or(DualCloseError::Invalid("Binance realizedPnL overflow"))?;
  prev_id=Some(id);
  fut.apply_fill(event,DEFAULT_AVG_TOLERANCE_ATOMS)
    .map_err(|_|DualCloseError::Invalid("Binance close cumulative fill mismatch"))?;
 }
 let from_id=prev_id.ok_or(DualCloseError::UnexhaustedHistory)?
   .checked_add(1).ok_or(DualCloseError::Invalid("Binance cursor overflow"))?;
 let tail_req=build_binance_user_trades_page_request(
   bin,now_ms,recv_window_ms,&c.futures.symbol,&c.futures.exchange_order_id,
   from_id,bin_limit).map_err(DualCloseError::Build)?;
 if !binance_rows(&query(exec,&tail_req)?)?.is_empty(){return Err(DualCloseError::UnexhaustedHistory)}
 if !spot.fee_evidence_complete||!fut.fee_evidence_complete
  ||spot.filled_qty_atoms()!=c.expected_qty_e12
  ||fut.filled_qty_atoms()!=c.expected_qty_e12
  ||spot.exchange_order_id.as_deref()!=Some(c.spot.exchange_order_id.as_str())
  ||fut.exchange_order_id.as_deref()!=Some(c.futures.exchange_order_id.as_str()){
  return Err(DualCloseError::Invalid("close fee/total/identity incomplete"))
 }
 Ok(DualCloseReadOnlyCandidate{
  source_position_identity:c.position_identity.clone(),
  okx_fill_count:spot.fills.len(),futures_fill_count:fut.fills.len(),
  futures_reported_realized_pnl_e12:reported_pnl,
  spot,futures: fut,order_reads:2,fill_reads:4,
  full_account_cash_bracket_proven:false,futures_position_flat_proven:false,
  funding_and_all_income_complete:false,full_currency_bills_history_complete:false,
  exchange_global_history_complete:false,cross_venue_atomicity_proven:false,
  may_mark_verified_closed:false,may_send_order:false,
 })
}
