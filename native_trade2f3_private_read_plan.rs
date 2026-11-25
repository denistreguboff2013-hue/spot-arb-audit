//! TRADE2F3 Stage B2: zero-network PRIVATE READ PREPARATION ONLY.
//!
//! Binds required exchange history GET scopes to the original sealed Native
//! owner and inspects the existing HTTPS policy without DNS/HTTP/credentials.
//! It never creates a SignedRequest, connects to either exchange, grants
//! private account access, accepts API keys or marks real trading ready.
use openssl::sha::sha256;
use crate::{
 authoritative_state::{NativeStateStore,NativeStateError},
 execution_adapter::Venue,
 native_trade2f_owner_binding::original_four_leg_preflight,
 private_https::{HttpsRequest,inspect_readonly_get_policy_without_network},
 private_rest::{OkxDomain,HttpMethod},
};
const MAX_SPAN_MS:u64=6*24*60*60*1000;
const MIN_LATE_MS:u64=60_000;
const MAX_LATE_MS:u64=24*60*60*1000;
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum PlanningError{State(NativeStateError),Deny(&'static str)}
fn deny(s:&'static str)->PlanningError{PlanningError::Deny(s)}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum PlannedWindow{Original,Delayed}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum PlannedKind{OkxBuyFills,OkxSellFills,BinanceShortOpenFills,BinanceShortCloseFills,OkxAccountBills,BinanceIncome}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct UnapprovedHistorySlot{
 venue:Venue,window:PlannedWindow,kind:PlannedKind,
 endpoint:String,selector:String,query_symbol:Option<String>,
 page_size:u16,max_pages:u16,
 transport_get_currently_allowlisted:bool,
}
impl UnapprovedHistorySlot{
 pub fn venue(&self)->Venue{self.venue}
 pub fn window(&self)->PlannedWindow{self.window}
 pub fn kind(&self)->PlannedKind{self.kind}
 pub fn endpoint(&self)->&str{&self.endpoint}
 pub fn selector(&self)->&str{&self.selector}
 /// Required Binance futures symbol for order-scoped userTrades. The
 /// selector alone is never an executable or authenticated GET request.
 pub fn query_symbol(&self)->Option<&str>{self.query_symbol.as_deref()}
 pub fn page_size(&self)->u16{self.page_size}
 pub fn max_pages(&self)->u16{self.max_pages}
 pub fn transport_get_currently_allowlisted(&self)->bool{
    self.transport_get_currently_allowlisted}
}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum PrivateReadDisposition{UnapprovedDesignOnly}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct HistoryReadBlueprint{
 owner_generation:u64,owner_sha256:String,
 first_end:u64,late_end:u64,
 slots:Vec<UnapprovedHistorySlot>,
}
impl HistoryReadBlueprint{
 pub fn owner_generation(&self)->u64{self.owner_generation}
 pub fn owner_sha256(&self)->&str{&self.owner_sha256}
 pub fn slots(&self)->&[UnapprovedHistorySlot]{&self.slots}
 pub fn first_end(&self)->u64{self.first_end}
 pub fn late_end(&self)->u64{self.late_end}
 pub fn disposition(&self)->PrivateReadDisposition{
     PrivateReadDisposition::UnapprovedDesignOnly}
 pub fn can_connect_private_exchange(&self)->bool{false}
 pub fn api_key_permissions_verified(&self)->bool{false}
 pub fn independently_authenticated_account_data(&self)->bool{false}
 pub fn external_antirollback_proven(&self)->bool{false}
 pub fn may_trade_cancel_transfer_or_mark_closed(&self)->bool{false}
}
fn fake_scoped_request(
 venue:Venue,host:&str,path:&str,
)->HttpsRequest{
 // Only an inert policy-inspection value: its fake key is never signed,
 // passed to HttpsClient::execute or sent over a socket.
 // Signed-shape DUMMY values only. Matching a structural policy
 // does not make these credentials valid or permit network access.
 let (url,headers)=match venue{
   Venue::OkxSpot=>(
     format!("{host}{path}"),
     vec![
       ("OK-ACCESS-KEY".into(),"TRADE2F3_B2_NOT_A_REAL_KEY".into()),
       ("OK-ACCESS-PASSPHRASE".into(),"FAKE_PASS".into()),
       ("OK-ACCESS-TIMESTAMP".into(),"2026-10-15T12:00:00.000Z".into()),
       ("OK-ACCESS-SIGN".into(),format!("{}=", "A".repeat(43))),
     ],
   ),
   Venue::BinanceUsdm=>(
     format!("{host}{path}?timestamp=1700000000000&recvWindow=10000&signature={}",
       "a".repeat(64)),
     vec![("X-MBX-APIKEY".into(),"TRADE2F3_B2_NOT_A_REAL_KEY".into())],
   ),
 };
 HttpsRequest{
    method:HttpMethod::Get,url,headers,
    body:None,credential_bearing:true,
    credential_scope:Some(venue),mutation:false,
 }
}
fn slot(
 venue:Venue,window:PlannedWindow,kind:PlannedKind,
 host:&str,path:&str,selector:String,page_size:u16,
)->UnapprovedHistorySlot{
 let req=fake_scoped_request(venue,host,path);
 // This policy inspection never allocates sockets or touches the network;
 // allowlisted != operator-approved nor exchange-account authenticated.
 let allowed=inspect_readonly_get_policy_without_network(&req).is_ok();
 UnapprovedHistorySlot{
    venue,window,kind,endpoint:path.into(),selector,query_symbol:None,
    page_size,max_pages:4,transport_get_currently_allowlisted:allowed,
 }
}
/// Inert Binance source slot: original persisted orderId PLUS exact original
/// symbol. Existing HTTPS GET allowlisting is not operator authorization.
/// The B2 plan never signs, sends or schedules a private exchange call.
fn binance_original_fill_slot(kind:PlannedKind,order_id:&str,symbol:&str)
 ->UnapprovedHistorySlot{
 let mut s=slot(Venue::BinanceUsdm,PlannedWindow::Original,kind,
   "https://fapi.binance.com","/fapi/v1/userTrades",order_id.into(),1000);
 s.query_symbol=Some(symbol.into());
 s
}
fn numeric_id(v:&str)->bool{
 !v.is_empty()&&!v.starts_with('0')
    &&v.bytes().all(|b|b.is_ascii_digit())
    &&v.parse::<u128>().is_ok_and(|x|x>0)
}
/// Design the exact original-owner-bound per-endpoint collection slots.
/// Never exports secrets, raw account data or an executable GET capability.
/// Requires *all* existing immutable four-leg mock owner seals before any plan.
pub fn inspect_private_history_plan_without_network(
 store:&NativeStateStore,expected_generation:u64,
 first_end:u64,late_end:u64,domain:OkxDomain,
)->Result<HistoryReadBlueprint,PlanningError>{
 let before=store.load().map_err(PlanningError::State)?
    .ok_or(deny("Native original owner missing before read-plan"))?;
 if before.generation!=expected_generation{
    return Err(deny("stale Native generation before private read-plan"))}
 original_four_leg_preflight(&before)
    .map_err(|_|deny("unsealed original four-leg owner cannot plan private history"))?;
 let e=before.entry.as_ref().ok_or(deny("original entry missing"))?;
 let start=e.plan.created_at_ms;
 if start==0||first_end<start||late_end<first_end
    ||late_end-start>MAX_SPAN_MS
    ||late_end-first_end<MIN_LATE_MS
    ||late_end-first_end>MAX_LATE_MS{
     return Err(deny("invalid bounded original and delayed history windows"))}
 let open=e.spot_prehedge_evidence.as_ref()
    .ok_or(deny("Native first spot owner absent"))?;
 let close=e.shadow_close_receipt.as_ref()
    .ok_or(deny("Native closing owner absent"))?;
 let sell=&close.bounded_economic_ledgers().0;
 let buy=&open.exact_ledger;
 let buy_id=buy.exchange_order_id.as_deref().ok_or(deny("original buy order absent"))?;
 let sell_id=sell.exchange_order_id.as_deref().ok_or(deny("original sell order absent"))?;
 if !numeric_id(buy_id)||!numeric_id(sell_id)||buy_id==sell_id
    ||buy.fills.keys().chain(sell.fills.keys()).any(|id|!numeric_id(id)){
      return Err(deny("noncanonical persisted original OKX order/trade ID"))}
 if e.plan.base.is_empty()||e.plan.base=="USDT"
    ||e.plan.base.contains(['/',':','&','?',' '])
    ||e.plan.spot_symbol.replace('/',"-")!=format!("{}-USDT",e.plan.base){
     return Err(deny("original spot base/quote scope not supported"))}
 // Both original Binance IDs MUST be taken from original sealed persisted
 // ledgers, never injected by a caller or guessed from the income ledger.
 let original_short=e.recovery_checkpoint.as_ref()
    .ok_or(deny("original Binance short checkpoint absent"))?;
 let short_open_id=original_short.ledger.exchange_order_id.as_deref()
    .ok_or(deny("original Binance short order ID absent"))?;
 let (_,futures_close_ledger)=close.bounded_economic_ledgers();
 let short_close_id=futures_close_ledger.exchange_order_id.as_deref()
    .ok_or(deny("original Binance short close order ID absent"))?;
 if !numeric_id(short_open_id)||!numeric_id(short_close_id)
    ||short_open_id==short_close_id
    ||original_short.order_id!=short_open_id
    ||original_short.ledger.symbol!=e.plan.futures_symbol
    ||futures_close_ledger.symbol!=e.plan.futures_symbol
    ||original_short.ledger.venue!="binance"
    ||futures_close_ledger.venue!="binance"
    ||original_short.ledger.side!="sell"
    ||futures_close_ledger.side!="buy"{
    return Err(deny("original Binance order identity or side mismatch"))}
 let host=domain.base_url();
 let mut slots=vec![
   slot(Venue::OkxSpot,PlannedWindow::Original,
       PlannedKind::OkxBuyFills,host,
       "/api/v5/trade/fills-history",buy_id.into(),100),
   slot(Venue::OkxSpot,PlannedWindow::Original,
       PlannedKind::OkxSellFills,host,
       "/api/v5/trade/fills-history",sell_id.into(),100),
   binance_original_fill_slot(
       PlannedKind::BinanceShortOpenFills,short_open_id,&e.plan.futures_symbol),
   binance_original_fill_slot(
       PlannedKind::BinanceShortCloseFills,short_close_id,&e.plan.futures_symbol),
 ];
 for (window,when) in [
   (PlannedWindow::Original,"original"),
   (PlannedWindow::Delayed,"delayed"),
 ]{
   for currency in [&e.plan.base,"USDT"]{
    slots.push(slot(Venue::OkxSpot,window,PlannedKind::OkxAccountBills,
        host,"/api/v5/account/bills-archive",
        format!("{when}:{currency}"),100));
   }
 }
 for window in [PlannedWindow::Original,PlannedWindow::Delayed]{
   slots.push(slot(Venue::BinanceUsdm,window,PlannedKind::BinanceIncome,
       "https://fapi.binance.com","/fapi/v1/income",
       "UNFILTERED_ALL_INCOME_TYPES".into(),1000));
 }
 // Pure hash here binds the plan to the owner snapshot, but cannot serve
 // as independent durability, exchange authenticity, antirollback or PnL.
 let bytes=serde_json::to_vec(&before)
    .map_err(|_|deny("Native owner hash serialization failed"))?;
 let digest=sha256(&bytes).iter().map(|x|format!("{x:02x}")).collect();
 if store.load().map_err(PlanningError::State)?.as_ref()!=Some(&before){
    return Err(deny("Native owner changed during read-only plan inspection"))}
 Ok(HistoryReadBlueprint{
   owner_generation:expected_generation,
   owner_sha256:digest,first_end,late_end,slots,
 })
}

/// P1-01 separate late-only B6 query DESIGN. H/I widened reads really
/// start at owner.created_at; they cannot be relabelled as late-only.
/// No credentials, signatures, network requests, or executable URLs.
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum DelayedOnlyLane{OkxBaseBills,OkxQuoteBills,BinanceIncome}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum DelayedOnlyPagination{OkxAfterBillId,BinanceOneBasedPage}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct InertLateOnlyReadSlot{
 lane:DelayedOnlyLane,
 endpoint:String,
 source_currency:Option<String>,
 start_ms:u64,
 end_ms:u64,
 limit:u16,
 max_pages:u16,
 pagination:DelayedOnlyPagination,
}
impl InertLateOnlyReadSlot{
 pub fn lane(&self)->DelayedOnlyLane{self.lane}
 pub fn endpoint(&self)->&str{&self.endpoint}
 pub fn source_currency(&self)->Option<&str>{self.source_currency.as_deref()}
 pub fn start_ms(&self)->u64{self.start_ms}
 pub fn end_ms(&self)->u64{self.end_ms}
 pub fn limit(&self)->u16{self.limit}
 pub fn max_pages(&self)->u16{self.max_pages}
 pub fn pagination(&self)->DelayedOnlyPagination{self.pagination}
 pub fn authorized_to_read(&self)->bool{false}
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct InertLateOnlyHistoryPlan{
 owner_generation:u64,
 native_owner_sha256:String,
 slots:[InertLateOnlyReadSlot;3],
}
impl InertLateOnlyHistoryPlan{
 pub fn owner_generation(&self)->u64{self.owner_generation}
 pub fn native_owner_sha256(&self)->&str{&self.native_owner_sha256}
 pub fn slots(&self)->&[InertLateOnlyReadSlot;3]{&self.slots}
 pub fn may_send_private_get(&self)->bool{false}
 pub fn proves_exchange_origin_or_account_scope(&self)->bool{false}
 pub fn can_mark_verified_closed_or_trade(&self)->bool{false}
}
/// Derive three true (first_end,late_end] read scopes exclusively from
/// persisted original owner and existing B2 delayed slots. Design only.
pub fn inspect_late_only_b6_blueprint_without_network(
 store:&NativeStateStore,generation:u64,first_end:u64,late_end:u64,
 domain:OkxDomain,
)->Result<InertLateOnlyHistoryPlan,PlanningError>{
 let before=store.load().map_err(PlanningError::State)?
   .ok_or(deny("late-only Native owner missing"))?;
 let plan=inspect_private_history_plan_without_network(
   store,generation,first_end,late_end,domain)?;
 if before.generation!=generation||plan.slots.len()!=10{
   return Err(deny("late-only B2 owner or slot count drift"))
 }
 let entry=before.entry.as_ref().ok_or(deny("late-only owner entry absent"))?;
 let base=&entry.plan.base;
 let slots:[InertLateOnlyReadSlot;3]=std::array::from_fn(|i|{
   let b2=&plan.slots[[6,7,9][i]];
   InertLateOnlyReadSlot{
      lane:match i{0=>DelayedOnlyLane::OkxBaseBills,
        1=>DelayedOnlyLane::OkxQuoteBills,
        _=>DelayedOnlyLane::BinanceIncome},
      endpoint:b2.endpoint.clone(),
      source_currency:if i==0{Some(base.clone())}
         else if i==1{Some("USDT".into())}else{None},
      start_ms:first_end,end_ms:late_end,
      limit:b2.page_size,max_pages:b2.max_pages,
      pagination:if i<2{DelayedOnlyPagination::OkxAfterBillId}
        else{DelayedOnlyPagination::BinanceOneBasedPage},
   }
 });
 for (i,actual) in [6usize,7,9].iter().map(|n|&plan.slots[*n]).enumerate(){
   let (venue,kind,endpoint,selector,page_size)=if i<2{
      (Venue::OkxSpot,PlannedKind::OkxAccountBills,
       "/api/v5/account/bills-archive",
       format!("delayed:{}",if i==0{base.as_str()}else{"USDT"}),100)
   }else{
      (Venue::BinanceUsdm,PlannedKind::BinanceIncome,
       "/fapi/v1/income","UNFILTERED_ALL_INCOME_TYPES".into(),1000)
   };
   if actual.window!=PlannedWindow::Delayed
      ||actual.venue!=venue||actual.kind!=kind
      ||actual.endpoint!=endpoint||actual.selector!=selector
      ||actual.page_size!=page_size||actual.max_pages!=4
      ||actual.query_symbol.is_some(){
     return Err(deny("late-only B2 delayed source selector drift"))
   }
 }
 if first_end<=entry.plan.created_at_ms
    ||slots.iter().any(|s|s.start_ms!=first_end
      ||s.end_ms!=late_end||s.start_ms>=s.end_ms){
    return Err(deny("late-only source interval not disjoint from original"))
 }
 if store.load().map_err(PlanningError::State)?.as_ref()!=Some(&before){
   return Err(deny("late-only owner changed during inert planning"))
 }
 Ok(InertLateOnlyHistoryPlan{
   owner_generation:generation,native_owner_sha256:plan.owner_sha256,
   slots,
 })
}
