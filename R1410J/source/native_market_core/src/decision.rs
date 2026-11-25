//! Generic, execution-free decision hot path. Exchange transport stays in adapters.
use crate::{Book, CoreError, owner_index};
use serde::{Deserialize,Serialize};
use std::collections::{HashMap,HashSet};
const SCALE:i128=1_000_000_000_000;
const CEILING:f64=500.0;
const BUCKETS:[f64;6]=[10.,25.,50.,100.,200.,500.];
#[derive(Clone,Debug,Deserialize,Serialize)]
pub struct Limits {
 pub min_amount:String,pub step:String,pub min_cost:String,
 pub max_amount:Option<String>,
}
#[derive(Clone,Debug,Deserialize,Serialize)]
pub struct RouteSpec {
 pub buy_exchange:String,pub sell_exchange:String,pub label:String,
 pub buy:Limits,pub sell:Limits,pub loaded_ns:u64,
 pub metadata_state:String,
}
#[derive(Clone,Debug)]
pub struct DecisionConfig {
 pub max_age_ms:u64,pub metadata_ttl_ms:u64,pub base_size:f64,
 pub risk_cap:f64,pub cost:f64,pub required_net:f64,
 pub min_price:f64,pub max_price:f64,pub blacklist:HashSet<String>,
}
#[derive(Clone,Debug,Serialize)]
pub struct Sample {
 pub quantity:f64,pub reason:String,pub executable:bool,
 pub spot_vwap:Option<f64>,pub futures_vwap:Option<f64>,
 pub spot_notional_usdt:Option<f64>,pub futures_notional_usdt:Option<f64>,
 pub gross_executable_bps:Option<f64>,pub estimated_net_bps:Option<f64>,
 pub requested_size_usdt:f64,pub estimated_cost_bps:f64,pub ladder:bool,
}
#[derive(Clone,Debug,Serialize)]
pub struct Decision {
 pub base:String,pub route:String,pub buy_exchange:String,pub sell_exchange:String,
 pub reason:String,pub metadata_state:String,pub cost_bps:f64,
 pub required_net_bps:f64,pub risk_cap_usdt:f64,
 pub spot_book_age_ms:Option<f64>,pub futures_book_age_ms:Option<f64>,
 pub buy_changed_ns:Option<u64>,pub sell_changed_ns:Option<u64>,
 pub buy_epoch:Option<u64>,pub sell_epoch:Option<u64>,
 pub gross_upper_bound_bps:Option<f64>,pub minimum_executable_size_usdt:Option<f64>,
 pub discovery_size_usdt:Option<f64>,pub quantity:Option<f64>,
 pub requested_size_usdt:Option<f64>,pub spot_vwap:Option<f64>,
 pub futures_vwap:Option<f64>,pub spot_notional_usdt:Option<f64>,
 pub futures_notional_usdt:Option<f64>,pub gross_executable_bps:Option<f64>,
 pub estimated_net_bps:Option<f64>,pub tested_size_usdt:Option<f64>,
 pub depth_state:Option<String>,pub max_profitable_capacity_usdt:Option<f64>,
 pub capacity_at_least_500:Option<bool>,pub risk_result:Option<String>,
 pub real_execution_status:Option<String>,pub samples:Vec<Sample>,
}
impl Decision {
 fn new(base:&str,route:&RouteSpec,state:String,cfg:&DecisionConfig)->Self{
  Self{base:base.into(),route:route.label.clone(),buy_exchange:route.buy_exchange.clone(),
   sell_exchange:route.sell_exchange.clone(),reason:String::new(),
   metadata_state:state,cost_bps:cfg.cost,required_net_bps:cfg.required_net,risk_cap_usdt:cfg.risk_cap,spot_book_age_ms:None,futures_book_age_ms:None,
   buy_changed_ns:None,sell_changed_ns:None,buy_epoch:None,sell_epoch:None,
   gross_upper_bound_bps:None,minimum_executable_size_usdt:None,
   discovery_size_usdt:None,quantity:None,requested_size_usdt:None,
   spot_vwap:None,futures_vwap:None,spot_notional_usdt:None,
   futures_notional_usdt:None,gross_executable_bps:None,estimated_net_bps:None,
   tested_size_usdt:None,depth_state:None,max_profitable_capacity_usdt:None,
   capacity_at_least_500:None,risk_result:None,real_execution_status:None,samples:Vec::new()}
 }
}
pub struct DecisionCore {
 pub index:usize,pub total:usize,pub cfg:DecisionConfig,
 state:HashMap<String,HashMap<String,Book>>,
 epochs:HashMap<(String,String),u64>,
 routes:HashMap<String,Vec<RouteSpec>>,
 metadata_generation:u64,
 staged:Option<(u64,HashMap<String,Vec<RouteSpec>>)>,
 paused:bool,
}
fn validate_routes(routes:&HashMap<String,Vec<RouteSpec>>)->Result<(),CoreError>{
 for (symbol,rows) in routes{
  if symbol.is_empty()||rows.is_empty()||rows.len()>16{return Err(CoreError::Invalid("routes"))}
  let mut labels=HashSet::new();
  for r in rows{
   if r.buy_exchange.is_empty()||r.sell_exchange.is_empty()||r.buy_exchange==r.sell_exchange
    ||r.label.is_empty()||!labels.insert(&r.label){return Err(CoreError::Invalid("route identity"))}
   if r.metadata_state=="SIZE_METADATA_READY"{
    for side in [&r.buy,&r.sell]{
     if positive(&side.min_amount).is_none()||positive(&side.step).is_none()
      ||scaled(&side.min_cost).is_none()||side.max_amount.as_deref().is_some_and(|v|positive(v).is_none()){
      return Err(CoreError::Invalid("route limits"))
     }
    }
   }
  }
 }
 Ok(())
}
impl DecisionCore {
 pub fn new(index:usize,total:usize,cfg:DecisionConfig,routes:HashMap<String,Vec<RouteSpec>>)->Result<Self,CoreError>{
  if total==0||index>=total{return Err(CoreError::Invalid("owner count"))}
  validate_routes(&routes)?;
  Ok(Self{index,total,cfg,state:HashMap::new(),epochs:HashMap::new(),routes,
   metadata_generation:0,staged:None,paused:false})
 }
 pub fn replace_routes(&mut self,generation:u64,routes:HashMap<String,Vec<RouteSpec>>)->Result<(),CoreError>{
  // Single-owner setup/replay only. Cross-process refresh uses stage/pause/commit/resume.
  if self.paused||self.staged.is_some(){return Err(CoreError::Invalid("barrier active"))}
  if generation<=self.metadata_generation{return Err(CoreError::OutOfOrder)}
  if routes.is_empty(){return Err(CoreError::Invalid("empty metadata"))}
  validate_routes(&routes)?;
  self.routes=routes;
  self.metadata_generation=generation;
  Ok(())
 }
 pub fn metadata_generation(&self)->u64{self.metadata_generation}
 /// Read-only metadata binding for an already selected symbol; no recomputation.
 pub fn routes_for(&self,base:&str)->Option<&[RouteSpec]>{self.routes.get(base).map(Vec::as_slice)}
 pub fn stage_routes(&mut self,generation:u64,routes:HashMap<String,Vec<RouteSpec>>)->Result<(),CoreError>{
  if generation<=self.metadata_generation{return Err(CoreError::OutOfOrder)}
  if routes.is_empty(){return Err(CoreError::Invalid("empty metadata"))}
  validate_routes(&routes)?;
  self.staged=Some((generation,routes));Ok(())
 }
 pub fn pause_decisions(&mut self){self.paused=true}
 pub fn abort_staged(&mut self){self.staged=None}
 pub fn commit_staged(&mut self,generation:u64)->Result<(),CoreError>{
  if !self.paused{return Err(CoreError::Invalid("not paused"))}
  if self.staged.as_ref().map(|(g,_)|*g)!=Some(generation){return Err(CoreError::OutOfOrder)}
  let (_,routes)=self.staged.take().unwrap();
  self.routes=routes;self.metadata_generation=generation;Ok(())
 }
 pub fn resume_decisions(&mut self,generation:u64)->Result<(),CoreError>{
  if self.staged.is_some()||self.metadata_generation!=generation{return Err(CoreError::OutOfOrder)}
  self.paused=false;Ok(())
 }
 pub fn decisions_paused(&self)->bool{self.paused}
 pub fn reset_exchange(&mut self,symbol:&str,exchange:&str,next_epoch:u64)->Result<(),CoreError>{
  if owner_index(symbol,self.total)!=self.index{return Err(CoreError::WrongOwner)}
  if exchange.is_empty(){return Err(CoreError::Invalid("exchange"))}
  let key=(symbol.to_owned(),exchange.to_owned());
  if self.epochs.get(&key).is_some_and(|prior|next_epoch<=*prior){return Err(CoreError::StaleEpoch)}
  self.epochs.insert(key,next_epoch);
  if let Some(books)=self.state.get_mut(symbol){books.remove(exchange);}
  Ok(())
 }
 /// Transport recovery invalidates both sides without resetting epoch fences.
 pub fn invalidate_books(&mut self,symbol:&str)->Result<(),CoreError>{
  if owner_index(symbol,self.total)!=self.index{return Err(CoreError::WrongOwner)}
  self.state.remove(symbol);Ok(())
 }
 pub fn update(&mut self,symbol:&str,exchange:&str,book:Book)->Result<Vec<Decision>,CoreError>{
  let now_ns=book.received_ns;let now_wall_ms=book.received_wall_ms;
  self.update_at(symbol,exchange,book,now_ns,now_wall_ms)
 }
 /// Live transport evaluates freshness at processing time, including queue age.
 pub fn update_at(&mut self,symbol:&str,exchange:&str,book:Book,now_ns:u64,now_wall_ms:i64)->Result<Vec<Decision>,CoreError>{
  self.update_at_native_verified_books5(
    symbol,exchange,book,now_ns,now_wall_ms,false)
 }
 /// R1122: only native Rust source that has ALREADY admitted + persisted
 /// an exact OKX books5 full snapshot may opt into its source-verified
 /// snapshot sequence reset. All default/incremental/real paths stay strict.
 pub fn update_at_native_verified_books5(
  &mut self,symbol:&str,exchange:&str,book:Book,
  now_ns:u64,now_wall_ms:i64,source_books5_verified:bool
 )->Result<Vec<Decision>,CoreError>{
  if self.paused{return Err(CoreError::Invalid("metadata transition"))}
  if owner_index(symbol,self.total)!=self.index{return Err(CoreError::WrongOwner)}
  if exchange.is_empty(){return Err(CoreError::Invalid("exchange"))}
  if book.bids.iter().chain(&book.asks).any(|(p,q)|!p.is_finite()||!q.is_finite()||*p<=0.||*q<=0.){
   return Err(CoreError::NonFinite)
  }
  let key=(symbol.to_owned(),exchange.to_owned());
  let expected=self.epochs.entry(key).or_insert(book.epoch);
  if *expected!=book.epoch{return Err(CoreError::StaleEpoch)}
  let books=self.state.entry(symbol.into()).or_default();
  if let Some(prior)=books.get(exchange){
   let valid_snapshot_rebase=source_books5_verified&&exchange=="okx_spot"&&
     book.epoch==prior.epoch&&
     book.received_ns>prior.received_ns&&
     book.received_wall_ms>=prior.received_wall_ms&&
     !book.bids.is_empty()&&!book.asks.is_empty()&&
     book.exchange_ts_ms.zip(prior.exchange_ts_ms)
       .is_some_and(|(next,old)|next>old&&
          book.received_wall_ms.saturating_sub(next)<=10_000&&
          next.saturating_sub(book.received_wall_ms)<=1_000);
   if book.received_ns<prior.received_ns ||
    (book.sequence.is_some()&&prior.sequence.is_some()&&
      book.sequence<=prior.sequence&&!valid_snapshot_rebase){
    return Err(CoreError::OutOfOrder)
   }
  }
  books.insert(exchange.into(),book.clone());
  let Some(routes)=self.routes.get(symbol) else {return Ok(Vec::new())};
  Ok(routes.iter().map(|route|{
   let books=&self.state[symbol];
   evaluate(symbol,route,books.get(&route.buy_exchange),books.get(&route.sell_exchange),
            now_ns,now_wall_ms,&self.cfg)
  }).collect())
 }
 pub fn exchange_count(&self,symbol:&str)->usize{self.state.get(symbol).map_or(0,HashMap::len)}
}
fn round_even(value:f64,digits:i32)->f64{
 if !value.is_finite(){return value}
 format!("{value:.precision$}",precision=digits as usize).parse::<f64>().unwrap_or(value)
}
fn scaled(s:&str)->Option<i128>{
 let x=s.trim();if x.is_empty()||x.starts_with('-')||x.starts_with('+'){return None}
 let (mantissa,exponent)=if let Some((a,b))=x.split_once(['e','E']){
  (a,b.parse::<i32>().ok()?)
 }else{(x,0)};
 let (whole,frac)=mantissa.split_once('.').unwrap_or((mantissa,""));
 if whole.is_empty()||!whole.bytes().all(|c|c.is_ascii_digit())
  ||!frac.bytes().all(|c|c.is_ascii_digit()){return None}
 let digits=whole.parse::<i128>().ok()?
  .checked_mul(10_i128.checked_pow(frac.len() as u32)?)?
  .checked_add(if frac.is_empty(){0}else{frac.parse::<i128>().ok()?})?;
 let shift=12_i32-(frac.len() as i32)+exponent;
 if !(0..=38).contains(&shift){return None}
 digits.checked_mul(10_i128.checked_pow(shift as u32)?)
}
fn positive(s:&str)->Option<i128>{scaled(s).filter(|x|*x>0)}
fn from_float(f:f64)->Option<i128>{
 if !f.is_finite()||f<=0.0{return None}
 positive(&f.to_string())
}
fn gcd(mut a:i128,mut b:i128)->i128{while b!=0{let r=a%b;a=b;b=r}a}
fn ceil_div(a:i128,b:i128)->Option<i128>{if b<=0{return None} a.checked_add(b-1).map(|n|n/b)}
fn floor_div(a:i128,b:i128)->Option<i128>{if b<=0{None}else{Some(a/b)}}
#[derive(Clone,Copy,Debug)]struct Size {amount:f64,minimum:f64,step:f64}
fn size_for(route:&RouteSpec,ask:f64,bid:f64,usd:f64)->Result<Size,&'static str>{
 let mut maximum_deny=false;
 let result=(||->Option<Size>{
 let ask=from_float(ask)?;let bid=from_float(bid)?;let cap=from_float(usd)?;
 let bs=positive(&route.buy.step)?;let ss=positive(&route.sell.step)?;
 let step=bs.checked_div(gcd(bs,ss))?.checked_mul(ss)?;
 let bmin=positive(&route.buy.min_amount)?;let smin=positive(&route.sell.min_amount)?;
 let bcost=scaled(&route.buy.min_cost)?;let scost=scaled(&route.sell.min_cost)?;
 let mut first=ceil_div(bmin,step)?.max(ceil_div(smin,step)?);
 first=first.max(ceil_div(bcost.checked_mul(SCALE)?,ask.checked_mul(step)?)?);
 first=first.max(ceil_div(scost.checked_mul(SCALE)?,bid.checked_mul(step)?)?);
 let mut max_units:Option<i128>=None;
 for raw in [route.buy.max_amount.as_deref(),route.sell.max_amount.as_deref()].into_iter().flatten(){
  let units=floor_div(positive(raw)?,step)?;
  max_units=Some(max_units.map_or(units,|prior|prior.min(units)));
 }
 if max_units.is_some_and(|m|first>m){maximum_deny=true;return None}
 let price=ask.max(bid);
 let min_quantity=first.checked_mul(step)?;
 let min_usd_scaled=min_quantity.checked_mul(price)?/SCALE;
 let min_usd=min_usd_scaled as f64/SCALE as f64;
 let target=cap.max(min_usd_scaled);
 let mut units=floor_div(target.checked_mul(SCALE)?,price.checked_mul(step)?)?;
 if let Some(m)=max_units{units=units.min(m)}
 units=units.max(first);
 Some(Size{amount:(units*step) as f64/SCALE as f64,
   minimum:min_usd,step:step as f64/SCALE as f64})
 })();
 result.ok_or(if maximum_deny{"EXCHANGE_MAXIMUM_DENY"}else{"SIZE_METADATA_INVALID"})
}
fn vwap(levels:&[(f64,f64)],quantity:f64)->Option<f64>{
 let mut left=quantity;let mut total=0.;
 for &(price,available) in levels{
  let taken=left.min(available);total+=taken*price;left-=taken;
  if left<=quantity*1e-9{return Some(total/quantity)}
 }
 None
}
fn sample(buy:&Book,sell:&Book,quantity:f64,cfg:&DecisionConfig,ladder:bool)->Sample{
 let requested=round_even(quantity*buy.asks[0].0.max(sell.bids[0].0),6);
 let mut out=Sample{quantity,reason:"DEPTH_DENY".into(),executable:false,
  spot_vwap:None,futures_vwap:None,spot_notional_usdt:None,
  futures_notional_usdt:None,gross_executable_bps:None,estimated_net_bps:None,
  requested_size_usdt:requested,estimated_cost_bps:cfg.cost,ladder};
 let Some(bv)=vwap(&buy.asks,quantity) else{return out};
 let Some(sv)=vwap(&sell.bids,quantity) else{return out};
 let bn=bv*quantity;let sn=sv*quantity;let gross=(sv/bv-1.)*10000.;let net=gross-cfg.cost;
 out.reason=if bn.max(sn)>CEILING+1e-7{"ABOVE_RESEARCH_CEILING"}
  else if net>=cfg.required_net{"EXECUTABLE_OPPORTUNITY"}else{"EDGE_TOO_LOW"}.into();
 out.executable=out.reason=="EXECUTABLE_OPPORTUNITY";
 out.spot_vwap=Some(round_even(bv,10));out.futures_vwap=Some(round_even(sv,10));
 out.spot_notional_usdt=Some(round_even(bn,6));out.futures_notional_usdt=Some(round_even(sn,6));
 out.gross_executable_bps=Some(round_even(gross,4));out.estimated_net_bps=Some(round_even(net,4));
 out
}
fn capacity(buy:&Book,sell:&Book,route:&RouteSpec,minimum:Size,cfg:&DecisionConfig)
 ->(Vec<Sample>,f64,bool){
 let step=minimum.step;
 let first=(minimum.amount/step).round() as i64;
 let price=buy.asks[0].0.max(sell.bids[0].0);
 let mut last=first.max(((CEILING/price)/step+1e-9).floor() as i64);
 let research_last=last;
 for maxstr in [route.buy.max_amount.as_deref(),route.sell.max_amount.as_deref()].into_iter().flatten(){
  if let Some(v)=positive(maxstr){last=last.min(((v as f64/SCALE as f64)/step+1e-9).floor() as i64)}
 }
 let mut tested:HashMap<i64,Sample>=HashMap::new();
 fn check(tested:&mut HashMap<i64,Sample>,buy:&Book,sell:&Book,step:f64,cfg:&DecisionConfig,unit:i64,ladder:bool)->Sample{
  let row=tested.entry(unit).or_insert_with(||sample(buy,sell,unit as f64*step,cfg,ladder));
  if ladder{row.ladder=true};row.clone()
 }
 let mut best=first;check(&mut tested,buy,sell,step,cfg,first,true);
 for budget in BUCKETS{
  let units=last.min(((budget/price)/step+1e-9).floor() as i64);
  if units<first{continue}
  if check(&mut tested,buy,sell,step,cfg,units,true).executable{best=best.max(units)}
 }
 if !tested.contains_key(&last)&&check(&mut tested,buy,sell,step,cfg,last,false).executable{best=best.max(last)}
 let mut high=tested.iter().filter(|(u,s)|**u>best&&!s.executable)
  .map(|(u,_)|*u).min().unwrap_or(last+1);
 for _ in 0..12{
  if high-best<=1{break}
  let middle=(best+high)/2;
  if check(&mut tested,buy,sell,step,cfg,middle,false).executable{best=middle}else{high=middle}
 }
 let winning=check(&mut tested,buy,sell,step,cfg,best,false);
 let value=winning.spot_notional_usdt.unwrap_or(0.).max(winning.futures_notional_usdt.unwrap_or(0.));
 let ceiling=best==last&&last==research_last&&value>=CEILING-price*step-1e-6&&winning.executable;
 let mut samples:Vec<_>=tested.into_values().collect();
 samples.sort_by(|a,b|a.quantity.total_cmp(&b.quantity));
 (samples,round_even(value.min(CEILING),2),ceiling)
}
pub fn evaluate(base:&str,route:&RouteSpec,buy:Option<&Book>,sell:Option<&Book>,
 now_ns:u64,now_wall_ms:i64,cfg:&DecisionConfig)->Decision{
 #[cfg(test)]
 crate::r1323_whole_universe::record_evaluation(base);
 let meta=if route.metadata_state!="SIZE_METADATA_READY"{route.metadata_state.clone()}
  else if route.loaded_ns==0{"SIZE_METADATA_INVALID".into()}
  else if now_ns.saturating_sub(route.loaded_ns)>cfg.metadata_ttl_ms*1_000_000{"SIZE_METADATA_STALE".into()}
  else{"SIZE_METADATA_READY".into()};
 let mut d=Decision::new(base,route,meta.clone(),cfg);
 let (Some(buy),Some(sell))=(buy,sell) else{d.reason="SYSTEM_NOT_READY".into();return d};
 if buy.asks.is_empty()||sell.bids.is_empty(){d.reason="SYSTEM_NOT_READY".into();return d}
 d.buy_changed_ns=Some(buy.changed_ns);d.sell_changed_ns=Some(sell.changed_ns);
 d.buy_epoch=Some(buy.epoch);d.sell_epoch=Some(sell.epoch);
 let ba=(now_ns as i128-buy.received_ns as i128) as f64/1e6;
 let sa=(now_ns as i128-sell.received_ns as i128) as f64/1e6;
 d.spot_book_age_ms=Some(round_even(ba,3));d.futures_book_age_ms=Some(round_even(sa,3));
 let ask=buy.asks[0].0;let bid=sell.bids[0].0;
 let upper=(bid/ask-1.)*10000.;
 d.gross_upper_bound_bps=Some(round_even(upper,4));
 let fresh=|b:&Book|b.exchange_ts_ms.is_some_and(|ts|{
  now_wall_ms.checked_sub(ts).is_some_and(|age|age>=-1000&&age<=cfg.max_age_ms as i64)
 });
 d.reason=if ba.min(sa)<0.||ba.max(sa)>cfg.max_age_ms as f64||!fresh(buy)||!fresh(sell){"STALE_BOOK"}
 else if cfg.blacklist.contains(base){"BLACKLIST"}
 else if ask<cfg.min_price||ask>cfg.max_price{"PRICE_FILTER"}
 else if upper-cfg.cost<cfg.required_net{"EDGE_TOO_LOW"}
 else if meta!="SIZE_METADATA_READY"{
  if meta=="SIZE_METADATA_MISSING"{"SIZE_METADATA_NOT_READY"}else{&meta}
 }else{""}.into();
 if !d.reason.is_empty(){return d}
 let minimum=match size_for(route,ask,bid,cfg.base_size){
  Ok(v)=>v,Err(_)=>{d.reason="SIZE_METADATA_INVALID".into();return d}
 };
 d.minimum_executable_size_usdt=Some(round_even(minimum.minimum,6));
 let discovery=cfg.base_size.max(minimum.minimum);d.discovery_size_usdt=Some(discovery);
 if minimum.minimum>CEILING{d.reason="ABOVE_RESEARCH_CEILING".into();return d}
 let sizing=match size_for(route,ask,bid,discovery){
  Ok(v)=>v,Err(_)=>{d.reason="SIZE_METADATA_INVALID".into();return d}
 };
 let row=sample(buy,sell,sizing.amount,cfg,false);
 d.reason=row.reason.clone();d.quantity=Some(row.quantity);
 d.requested_size_usdt=Some(row.requested_size_usdt);
 d.spot_vwap=row.spot_vwap;d.futures_vwap=row.futures_vwap;
 d.spot_notional_usdt=row.spot_notional_usdt;d.futures_notional_usdt=row.futures_notional_usdt;
 d.gross_executable_bps=row.gross_executable_bps;d.estimated_net_bps=row.estimated_net_bps;
 d.tested_size_usdt=Some(row.spot_notional_usdt.unwrap_or(0.).max(row.futures_notional_usdt.unwrap_or(0.)));
 d.depth_state=Some(if row.spot_vwap.is_some(){"DEPTH_OK"}else{"DEPTH_DENY"}.into());
 if row.executable{
  let Ok(min_sizing)=size_for(route,ask,bid,minimum.minimum) else{return d};
  let (samples,capacity,ceiling)=capacity(buy,sell,route,min_sizing,cfg);
  d.samples=samples;d.max_profitable_capacity_usdt=Some(capacity);
  d.capacity_at_least_500=Some(ceiling);
  d.real_execution_status=Some(if minimum.minimum>cfg.risk_cap{"ABOVE_CURRENT_REAL_CAP"}else{"SHADOW_ONLY"}.into());
  d.risk_result=Some(if d.tested_size_usdt.unwrap_or(0.)>cfg.risk_cap{"RISK_SIZE_DENY"}else{"RISK_SIZE_ALLOW"}.into());
 }
 d
}
#[cfg(test)]mod tests{
 use super::*;
 #[test]fn multiple_exchanges_and_wrong_owner(){
  let cfg=DecisionConfig{max_age_ms:800,metadata_ttl_ms:1000,base_size:10.,risk_cap:10.,
   cost:24.,required_net:42.,min_price:0.,max_price:f64::INFINITY,blacklist:HashSet::new()};
  let mut core=DecisionCore::new(owner_index("BTC",3),3,cfg,HashMap::new()).unwrap();
  let book=Book{bids:vec![(1.,2.)],asks:vec![(1.,2.)],received_ns:1,received_wall_ms:1,
   exchange_ts_ms:None,changed_ns:1,epoch:1,sequence:None};
  for venue in ["okx","binance","gate"]{core.update("BTC",venue,book.clone()).unwrap();}
  assert_eq!(core.exchange_count("BTC"),3);
  assert!(matches!(core.update("ETH","gate",book),Err(CoreError::WrongOwner)));
 }
 #[test]fn decimal_steps_and_fail_closed(){
  let limits=Limits{min_amount:"0.002".into(),step:"0.003".into(),min_cost:"1".into(),max_amount:Some("10".into())};
  let route=RouteSpec{buy_exchange:"a".into(),sell_exchange:"b".into(),label:"a/b".into(),
   buy:limits,sell:Limits{min_amount:"0.004".into(),step:"0.002".into(),min_cost:"1".into(),max_amount:None},
   loaded_ns:1,metadata_state:"SIZE_METADATA_READY".into()};
  let v=size_for(&route,100.,101.,10.).unwrap();assert!((v.step-0.006).abs()<1e-12);
  let mut broken=route.clone();broken.buy.step="NaN".into();assert!(size_for(&broken,100.,101.,10.).is_err());
  broken.buy.step="0.0000000000001".into();assert!(size_for(&broken,100.,101.,10.).is_err());
 }
 fn fixture()->(RouteSpec,DecisionConfig,Book,Book){
  let limits=Limits{min_amount:"0.001".into(),step:"0.001".into(),
   min_cost:"1".into(),max_amount:Some("100000".into())};
  let route=RouteSpec{buy_exchange:"spot".into(),sell_exchange:"future".into(),
   label:"Spot BUY -> Future SHORT".into(),buy:limits.clone(),sell:limits,
   loaded_ns:1,metadata_state:"SIZE_METADATA_READY".into()};
  let cfg=DecisionConfig{max_age_ms:800,metadata_ttl_ms:43_200_000,base_size:10.,risk_cap:10.,
   cost:24.,required_net:42.,min_price:0.,max_price:f64::INFINITY,blacklist:HashSet::new()};
  let buy=Book{bids:vec![(0.99,20.)],asks:vec![(1.,20.)],received_ns:1,
   received_wall_ms:1000,exchange_ts_ms:Some(1000),changed_ns:1,epoch:1,sequence:None};
  let sell=Book{bids:vec![(1.01,20.)],asks:vec![(1.02,20.)],..buy.clone()};
  (route,cfg,buy,sell)
 }
 #[test]fn readiness_freshness_and_fee_boundary(){
  let (route,mut cfg,mut buy,mut sell)=fixture();
  assert_eq!(evaluate("BTC",&route,None,Some(&sell),1,1000,&cfg).reason,"SYSTEM_NOT_READY");
  assert_eq!(evaluate("BTC",&route,Some(&buy),Some(&sell),1,1000,&cfg).reason,"EXECUTABLE_OPPORTUNITY");
  buy.exchange_ts_ms=Some(0);
  assert_eq!(evaluate("BTC",&route,Some(&buy),Some(&sell),1,1000,&cfg).reason,"STALE_BOOK");
  buy.exchange_ts_ms=Some(1000);sell.exchange_ts_ms=Some(0);
  assert_eq!(evaluate("BTC",&route,Some(&buy),Some(&sell),1,1000,&cfg).reason,"STALE_BOOK");
  sell.exchange_ts_ms=Some(1000);sell.bids[0].0=1.0065;
  assert_eq!(evaluate("BTC",&route,Some(&buy),Some(&sell),1,1000,&cfg).reason,"EDGE_TOO_LOW");
  sell.bids[0].0=1.0067;
  assert_eq!(evaluate("BTC",&route,Some(&buy),Some(&sell),1,1000,&cfg).reason,"EXECUTABLE_OPPORTUNITY");
  cfg.blacklist.insert("BTC".into());
  assert_eq!(evaluate("BTC",&route,Some(&buy),Some(&sell),1,1000,&cfg).reason,"BLACKLIST");
 }
 #[test]fn two_routes_share_one_symbol_owner_without_rewriting_decision(){
  let (route,cfg,buy,sell)=fixture();
  let mut another=route.clone();another.buy_exchange="gate".into();
  another.label="Gate Spot BUY -> Future SHORT".into();
  let mut map=HashMap::new();map.insert("BTC".into(),vec![route,another]);
  let mut core=DecisionCore::new(owner_index("BTC",3),3,cfg,map).unwrap();
  assert_eq!(core.update("BTC","future",sell).unwrap().len(),2);
  core.update("BTC","spot",buy.clone()).unwrap();
  let results=core.update("BTC","gate",buy).unwrap();
  assert_eq!(results.len(),2);
  assert!(results.iter().all(|d|d.reason=="EXECUTABLE_OPPORTUNITY"));
  assert_eq!(core.exchange_count("BTC"),3);
 }
 #[test]fn r1122_source_verified_okx_snapshot_rebase_matches_db1_owner_only(){
  let (_,cfg,mut book,_)=fixture();
  let mut core=DecisionCore::new(owner_index("GOAT",3),3,cfg,HashMap::new()).unwrap();
  book.epoch=1;book.sequence=Some(2178089568);
  book.exchange_ts_ms=Some(1793075373000);
  book.received_wall_ms=1793075373150;
  book.received_ns=1000;
  core.update("GOAT","okx_spot",book.clone()).unwrap();
  let mut current=book.clone();current.received_ns=2000;
  current.received_wall_ms=1793075400150;
  current.exchange_ts_ms=Some(1793075400000);
  assert!(matches!(core.update_at("GOAT","okx_spot",current.clone(),
     2000,1793075400150),Err(CoreError::OutOfOrder)));
  core.update_at_native_verified_books5("GOAT","okx_spot",
    current.clone(),2000,1793075400150,true).unwrap();
  let mut reset=current.clone();reset.sequence=Some(3);
  reset.received_ns=3000;reset.received_wall_ms=1793075400350;
  reset.exchange_ts_ms=Some(1793075400200);
  core.update_at_native_verified_books5("GOAT","okx_spot",reset.clone(),
     3000,1793075400350,true).unwrap();
  let mut old=reset;old.received_ns=4000;
  old.exchange_ts_ms=Some(1793075400100);
  old.received_wall_ms=1793075400450;
  assert!(matches!(core.update_at_native_verified_books5("GOAT",
     "okx_spot",old,4000,1793075400450,true),Err(CoreError::OutOfOrder)));
 }
 #[test]fn connection_epoch_invalidation_and_order(){
  let (route,cfg,buy,sell)=fixture();
  let mut map=HashMap::new();map.insert("BTC".into(),vec![route]);
  let mut core=DecisionCore::new(owner_index("BTC",3),3,cfg,map).unwrap();
  core.update("BTC","future",sell.clone()).unwrap();
  assert_eq!(core.update("BTC","spot",buy.clone()).unwrap()[0].reason,"EXECUTABLE_OPPORTUNITY");
  core.reset_exchange("BTC","future",2).unwrap();
  assert_eq!(core.update("BTC","spot",buy.clone()).unwrap()[0].reason,"SYSTEM_NOT_READY");
  assert!(matches!(core.update("BTC","future",sell.clone()),Err(CoreError::StaleEpoch)));
  let mut fresh=sell.clone();fresh.epoch=2;fresh.received_ns=2;fresh.changed_ns=2;
  assert_eq!(core.update("BTC","future",fresh.clone()).unwrap()[0].reason,"EXECUTABLE_OPPORTUNITY");
  let mut old=fresh.clone();old.received_ns=1;
  assert!(matches!(core.update("BTC","future",old),Err(CoreError::OutOfOrder)));
  core.reset_exchange("BTC","spot",2).unwrap();
  assert_eq!(core.update("BTC","future",fresh).unwrap()[0].reason,"SYSTEM_NOT_READY");
  assert!(matches!(core.update("BTC","spot",buy.clone()),Err(CoreError::StaleEpoch)));
  let mut new_buy=buy;new_buy.epoch=2;new_buy.received_ns=2;
  assert_eq!(core.update("BTC","spot",new_buy).unwrap()[0].reason,"EXECUTABLE_OPPORTUNITY");
 }
 #[test]fn staged_barrier_blocks_all_decisions_until_resume(){
  let (route,cfg,buy,sell)=fixture();
  let mut map=HashMap::new();map.insert("BTC".into(),vec![route.clone()]);
  let mut core=DecisionCore::new(owner_index("BTC",3),3,cfg,map.clone()).unwrap();
  core.replace_routes(1,map.clone()).unwrap();
  core.update("BTC","future",sell.clone()).unwrap();
  assert_eq!(core.update("BTC","spot",buy.clone()).unwrap()[0].reason,"EXECUTABLE_OPPORTUNITY");
  let mut inactive=route;inactive.metadata_state="INSTRUMENT_INACTIVE".into();
  let mut next=HashMap::new();next.insert("BTC".into(),vec![inactive]);
  core.stage_routes(2,next).unwrap();
  assert_eq!(core.update("BTC","spot",buy.clone()).unwrap()[0].reason,"EXECUTABLE_OPPORTUNITY");
  core.pause_decisions();
  assert!(matches!(core.update("BTC","spot",buy.clone()),Err(CoreError::Invalid("metadata transition"))));
  assert!(matches!(core.commit_staged(3),Err(CoreError::OutOfOrder)));
  assert!(core.decisions_paused());
  core.commit_staged(2).unwrap();
  assert!(matches!(core.resume_decisions(1),Err(CoreError::OutOfOrder)));
  assert!(core.decisions_paused());
  core.resume_decisions(2).unwrap();
  assert_eq!(core.update("BTC","future",sell).unwrap()[0].reason,"INSTRUMENT_INACTIVE");
 }
 #[test]fn atomic_metadata_refresh_keeps_books_and_rejects_bad_generation(){
  let (route,cfg,buy,sell)=fixture();
  let mut initial=HashMap::new();initial.insert("BTC".into(),vec![route.clone()]);
  let mut core=DecisionCore::new(owner_index("BTC",3),3,cfg,initial).unwrap();
  core.update("BTC","future",sell.clone()).unwrap();
  assert_eq!(core.update("BTC","spot",buy.clone()).unwrap()[0].reason,"EXECUTABLE_OPPORTUNITY");
  let mut stale=route.clone();stale.metadata_state="SIZE_METADATA_STALE".into();
  let mut replacement=HashMap::new();replacement.insert("BTC".into(),vec![stale]);
  core.replace_routes(1,replacement).unwrap();
  assert_eq!(core.exchange_count("BTC"),2);
  assert_eq!(core.update("BTC","spot",buy.clone()).unwrap()[0].reason,"SIZE_METADATA_STALE");
  let mut malformed=route.clone();malformed.buy.max_amount=Some("NaN".into());
  let mut bad=HashMap::new();bad.insert("BTC".into(),vec![malformed]);
  assert!(matches!(core.replace_routes(2,bad),Err(CoreError::Invalid("route limits"))));
  assert_eq!(core.metadata_generation(),1);
  assert!(matches!(core.replace_routes(2,HashMap::new()),Err(CoreError::Invalid("empty metadata"))));
  let mut ready=HashMap::new();ready.insert("BTC".into(),vec![route]);
  assert!(matches!(core.replace_routes(1,ready.clone()),Err(CoreError::OutOfOrder)));
  core.replace_routes(2,ready).unwrap();
  assert_eq!(core.update("BTC","future",sell).unwrap()[0].reason,"EXECUTABLE_OPPORTUNITY");
 }
 #[test]fn live_queue_age_and_full_pair_recovery(){
  let (route,cfg,buy,sell)=fixture();
  let mut map=HashMap::new();map.insert("BTC".into(),vec![route]);
  let mut core=DecisionCore::new(owner_index("BTC",3),3,cfg,map).unwrap();
  core.update("BTC","future",sell.clone()).unwrap();
  assert_eq!(core.update_at("BTC","spot",buy.clone(),900_000_001,1900).unwrap()[0].reason,"STALE_BOOK");
  core.invalidate_books("BTC").unwrap();
  assert_eq!(core.update("BTC","future",sell.clone()).unwrap()[0].reason,"SYSTEM_NOT_READY");
  assert_eq!(core.update("BTC","spot",buy).unwrap()[0].reason,"EXECUTABLE_OPPORTUNITY");
 }
 #[test]fn metadata_maximum_precision_and_nonfinite_deny(){
  let (mut route,cfg,buy,sell)=fixture();
  route.buy.max_amount=Some("0.0005".into());
  assert_eq!(size_for(&route,1.,1.01,10.).unwrap_err(),"EXCHANGE_MAXIMUM_DENY");
  assert_eq!(evaluate("BTC",&route,Some(&buy),Some(&sell),1,1000,&cfg).reason,"SIZE_METADATA_INVALID");
  route.buy.max_amount=None;route.buy.step="NaN".into();
  assert_eq!(evaluate("BTC",&route,Some(&buy),Some(&sell),1,1000,&cfg).reason,"SIZE_METADATA_INVALID");
  route.buy.step="0.0000000000001".into();
  assert_eq!(evaluate("BTC",&route,Some(&buy),Some(&sell),1,1000,&cfg).reason,"SIZE_METADATA_INVALID");
  route.buy.step="1E-3".into();
  assert_eq!(evaluate("BTC",&route,Some(&buy),Some(&sell),1,1000,&cfg).reason,"EXECUTABLE_OPPORTUNITY");
 }
}

#[cfg(test)]
mod r1286_timestamp_fail_closed_tests {
 use super::*;
 fn fixture()->(RouteSpec,DecisionConfig,Book,Book){
  let limits=Limits{min_amount:"0.001".into(),step:"0.001".into(),min_cost:"1".into(),max_amount:Some("100000".into())};
  let route=RouteSpec{buy_exchange:"spot".into(),sell_exchange:"future".into(),label:"Spot BUY -> Future SHORT".into(),buy:limits.clone(),sell:limits,loaded_ns:1,metadata_state:"SIZE_METADATA_READY".into()};
  let cfg=DecisionConfig{max_age_ms:800,metadata_ttl_ms:43_200_000,base_size:10.,risk_cap:10.,cost:24.,required_net:42.,min_price:0.,max_price:f64::INFINITY,blacklist:HashSet::new()};
  let buy=Book{bids:vec![(0.99,20.)],asks:vec![(1.,20.)],received_ns:1,received_wall_ms:1000,exchange_ts_ms:Some(1000),changed_ns:1,epoch:1,sequence:None};
  let sell=Book{bids:vec![(1.01,20.)],asks:vec![(1.02,20.)],..buy.clone()};(route,cfg,buy,sell)
 }
 #[test] fn decision_core_evaluation_rejects_absent_old_and_excess_future_timestamps(){
  let (route,cfg,mut buy,mut sell)=fixture();
  buy.exchange_ts_ms=None;
  assert_eq!(evaluate("BTC",&route,Some(&buy),Some(&sell),1,1000,&cfg).reason,"STALE_BOOK");
  buy.exchange_ts_ms=Some(199);
  assert_eq!(evaluate("BTC",&route,Some(&buy),Some(&sell),1,1000,&cfg).reason,"STALE_BOOK");
  buy.exchange_ts_ms=Some(2001);
  assert_eq!(evaluate("BTC",&route,Some(&buy),Some(&sell),1,1000,&cfg).reason,"STALE_BOOK");
  buy.exchange_ts_ms=Some(1000);
  assert_eq!(evaluate("BTC",&route,Some(&buy),Some(&sell),1,1000,&cfg).reason,"EXECUTABLE_OPPORTUNITY");
  buy.exchange_ts_ms=Some(2000);
  assert_eq!(evaluate("BTC",&route,Some(&buy),Some(&sell),1,1000,&cfg).reason,"EXECUTABLE_OPPORTUNITY");
 }
}
