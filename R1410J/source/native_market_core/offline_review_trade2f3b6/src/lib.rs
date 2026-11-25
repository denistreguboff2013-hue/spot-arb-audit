//! TRADE2F3 B6 — standalone NO NETWORK owner/source/page shape model.
//! All receipts are caller-supplied, UNTRUSTED metadata; a consistent set
//! is not authenticated exchange history and cannot establish actual PnL.
use std::collections::{BTreeMap,BTreeSet};

const MAX_WINDOW_MS:u64=6*24*60*60*1000;
const MIN_DELAY_MS:u64=60_000;
const MAX_DELAY_MS:u64=24*60*60*1000;
const MAX_PAGES_PER_LANE:usize=4;

#[derive(Clone,Copy,Debug,Eq,PartialEq,Ord,PartialOrd)]
pub enum Lane{
 OkxBuy,OkxSell,BinanceShortOpen,BinanceShortClose,
 OkxBaseOriginal,OkxQuoteOriginal,OkxBaseDelayed,OkxQuoteDelayed,
 BinanceIncomeOriginal,BinanceIncomeDelayed,
}
impl Lane{
 pub const ALL:[Self;10]=[
  Self::OkxBuy,Self::OkxSell,Self::BinanceShortOpen,Self::BinanceShortClose,
  Self::OkxBaseOriginal,Self::OkxQuoteOriginal,Self::OkxBaseDelayed,
  Self::OkxQuoteDelayed,Self::BinanceIncomeOriginal,Self::BinanceIncomeDelayed
 ];
 pub fn endpoint(self)->&'static str{
  match self{
   Self::OkxBuy|Self::OkxSell=>"/api/v5/trade/fills-history",
   Self::BinanceShortOpen|Self::BinanceShortClose=>"/fapi/v1/userTrades",
   Self::OkxBaseOriginal|Self::OkxQuoteOriginal|
   Self::OkxBaseDelayed|Self::OkxQuoteDelayed=>"/api/v5/account/bills-archive",
   Self::BinanceIncomeOriginal|Self::BinanceIncomeDelayed=>"/fapi/v1/income"
  }
 }
 fn okx(self)->bool{
  !matches!(self,Self::BinanceShortOpen|Self::BinanceShortClose|
     Self::BinanceIncomeOriginal|Self::BinanceIncomeDelayed)
 }
 fn fill(self)->bool{
  matches!(self,Self::OkxBuy|Self::OkxSell|
    Self::BinanceShortOpen|Self::BinanceShortClose)
 }
 fn delayed(self)->bool{
  matches!(self,Self::OkxBaseDelayed|Self::OkxQuoteDelayed|
     Self::BinanceIncomeDelayed)
 }
 fn max_rows(self)->usize{if self.okx(){100}else{1000}}
}

#[derive(Clone,Debug,Eq,PartialEq)]
pub struct OriginalOwner{
 pub generation:u64,
 pub native_state_digest:String,
 pub okx_account_fingerprint:String,
 pub binance_account_fingerprint:String,
 pub okx_buy_order_id:String,
 pub okx_sell_order_id:String,
 pub binance_open_order_id:String,
 pub binance_close_order_id:String,
 pub base:String,
 pub spot_symbol:String,
 pub futures_symbol:String,
 pub start_ms:u64,
 pub first_end_ms:u64,
 pub late_end_ms:u64,
}
fn hash_shape(s:&str)->bool{
 s.len()==64 && s.bytes().all(|b|b.is_ascii_digit()||
    (b'a'..=b'f').contains(&b)) && s.bytes().any(|b|b!=b'0')
}
fn order_id(s:&str)->bool{
 !s.is_empty()&&s.len()<=40&&!s.starts_with('0')&&
 s.bytes().all(|b|b.is_ascii_digit())
}
fn symbol_part(s:&str)->bool{
 !s.is_empty()&&s.len()<=40&&
 s.bytes().all(|b|b.is_ascii_uppercase()||b.is_ascii_digit())
}
#[derive(Clone,Debug,Eq,PartialEq)]
pub enum Deny{
 InvalidOwner,InvalidOrder,InvalidSymbol,MissingLane,
 Unbounded,WrongOwner,WrongEndpoint,WrongOrder,WrongSymbol,WrongWindow,
 BadResponse,BadPage,DuplicateRow,BadRow,
}
impl OriginalOwner{
 pub fn validate_shape(&self)->Result<(),Deny>{
  let delta=self.late_end_ms.checked_sub(self.first_end_ms)
      .ok_or(Deny::InvalidOwner)?;
  let total=self.late_end_ms.checked_sub(self.start_ms)
      .ok_or(Deny::InvalidOwner)?;
  if self.generation==0||self.start_ms==0||
    self.first_end_ms<=self.start_ms||delta<MIN_DELAY_MS||
    delta>MAX_DELAY_MS||total>MAX_WINDOW_MS||
    !hash_shape(&self.native_state_digest)||
    !hash_shape(&self.okx_account_fingerprint)||
    !hash_shape(&self.binance_account_fingerprint)||
    self.okx_account_fingerprint==self.binance_account_fingerprint{
   return Err(Deny::InvalidOwner);
  }
  if !order_id(&self.okx_buy_order_id)||!order_id(&self.okx_sell_order_id)||
    !order_id(&self.binance_open_order_id)||!order_id(&self.binance_close_order_id)||
    self.okx_buy_order_id==self.okx_sell_order_id||
    self.binance_open_order_id==self.binance_close_order_id{
   return Err(Deny::InvalidOrder);
  }
  if !symbol_part(&self.base)||self.base=="USDT"||
    self.spot_symbol!=format!("{}-USDT",self.base)||
    self.futures_symbol!=format!("{}USDT",self.base){
   return Err(Deny::InvalidSymbol);
  }
  Ok(())
 }
 fn order(&self,lane:Lane)->Option<&str>{
  match lane{
   Lane::OkxBuy=>Some(&self.okx_buy_order_id),
   Lane::OkxSell=>Some(&self.okx_sell_order_id),
   Lane::BinanceShortOpen=>Some(&self.binance_open_order_id),
   Lane::BinanceShortClose=>Some(&self.binance_close_order_id),
   _=>None,
  }.map(String::as_str)
 }
 fn account(&self,lane:Lane)->&str{
  if lane.okx(){&self.okx_account_fingerprint}
  else{&self.binance_account_fingerprint}
 }
 fn symbol(&self,lane:Lane)->Option<&str>{
  if !lane.fill(){None}
  else if lane.okx(){Some(self.spot_symbol.as_str())}
  else{Some(self.futures_symbol.as_str())}
 }
 fn window(&self,lane:Lane)->(u64,u64){
  if lane.delayed(){(self.first_end_ms,self.late_end_ms)}
  else{(self.start_ms,self.first_end_ms)}
 }
}
#[derive(Clone,Debug)]
pub struct SourceRow{
 /// Normalized source-unique key including income type/time if needed.
 pub unique_key:String,
 pub order_id:Option<String>,
 pub symbol:Option<String>,
}
#[derive(Clone,Debug)]
pub struct Page{
 pub lane:Lane,
 pub generation:u64,
 pub native_state_digest:String,
 pub account_fingerprint:String,
 pub endpoint:String,
 pub order_id:Option<String>,
 pub symbol:Option<String>,
 pub from_ms:u64,
 pub to_ms:u64,
 pub index:usize,
 /// Normalized pagination token; not proof of endpoint cursor semantics.
 pub cursor_in:Option<String>,
 pub cursor_out:Option<String>,
 pub last_page:bool,
 pub requested_limit:usize,
 pub http_status:u16,
 pub business_success:bool,
 pub body_sha256:String,
 pub rows:Vec<SourceRow>,
}
#[derive(Clone,Debug)]
pub struct LocalShapeOnly{
 pub page_count:usize,
 pub row_count:usize,
 pub lane_count:usize,
}
impl LocalShapeOnly{
 pub fn exchange_origin_proven(&self)->bool{false}
 pub fn externally_anchored_antirollback(&self)->bool{false}
 pub fn complete_account_history_proven(&self)->bool{false}
 pub fn actual_owner_proven(&self)->bool{false}
 pub fn realized_profit_proven(&self)->bool{false}
 pub fn all_four_legs_verified_closed(&self)->bool{false}
 pub fn may_trade_cancel_transfer_or_release(&self)->bool{false}
}
/// A local-only integrity/shape checker. Page ordering and owner metadata
/// are trusted only for internal consistency and must not be promoted into
/// assertions that actual account records or wallet positions were verified.
pub fn inspect_untrusted_catalogue(
 owner:&OriginalOwner,pages:&[Page],
)->Result<LocalShapeOnly,Deny>{
 inspect_selected_lanes(owner,pages,&Lane::ALL)
}

// Exactly seven ORIGINAL source lanes can be examined without inventing the
// three absent late-only H/I pages. The result is a local shape, NOT 10/10
// source completeness, verified close, balance certification or real PnL.
const ORIGINAL_SEVEN:[Lane;7]=[
 Lane::OkxBuy,Lane::OkxSell,Lane::BinanceShortOpen,Lane::BinanceShortClose,
 Lane::OkxBaseOriginal,Lane::OkxQuoteOriginal,Lane::BinanceIncomeOriginal,
];
pub fn inspect_untrusted_original_seven(
 owner:&OriginalOwner,pages:&[Page],
)->Result<LocalShapeOnly,Deny>{
 inspect_selected_lanes(owner,pages,&ORIGINAL_SEVEN)
}
/// Source-authentication is NOT implied. These must be actual delayed-only
/// page shapes, never H/I widened request windows relabelled as delayed.
pub fn inspect_untrusted_delayed_three(
 owner:&OriginalOwner,pages:&[Page],
)->Result<LocalShapeOnly,Deny>{
 const LATE:[Lane;3]=[
  Lane::OkxBaseDelayed,Lane::OkxQuoteDelayed,Lane::BinanceIncomeDelayed,
 ];
 inspect_selected_lanes(owner,pages,&LATE)
}

fn inspect_selected_lanes(
 owner:&OriginalOwner,pages:&[Page],required:&[Lane],
)->Result<LocalShapeOnly,Deny>{
 owner.validate_shape()?;
 if pages.len()<required.len(){return Err(Deny::MissingLane)}
 if pages.len()>required.len()*MAX_PAGES_PER_LANE{return Err(Deny::Unbounded)}
 let mut by_lane:BTreeMap<Lane,Vec<&Page>>=BTreeMap::new();
 for p in pages{by_lane.entry(p.lane).or_default().push(p);}
 if by_lane.len()!=required.len(){return Err(Deny::MissingLane)}
 let mut identities:BTreeSet<(String,String,String)>=BTreeSet::new();
 let mut total=0usize;
 for &lane in required{
  let group=by_lane.get(&lane).ok_or(Deny::MissingLane)?;
  if group.is_empty()||group.len()>MAX_PAGES_PER_LANE{return Err(Deny::Unbounded)}
  let mut expected_cursor:Option<String>=None;
  let mut source_page_limit:Option<usize>=None;
  let mut cursor_set:BTreeSet<String>=BTreeSet::new();
  for (expected_index,p) in group.iter().enumerate(){
   if p.generation!=owner.generation||
    p.native_state_digest!=owner.native_state_digest||
    p.account_fingerprint!=owner.account(lane){return Err(Deny::WrongOwner)}
   if p.endpoint!=lane.endpoint(){return Err(Deny::WrongEndpoint)}
   if p.order_id.as_deref()!=owner.order(lane){return Err(Deny::WrongOrder)}
   if p.symbol.as_deref()!=owner.symbol(lane){return Err(Deny::WrongSymbol)}
   if (p.from_ms,p.to_ms)!=owner.window(lane){return Err(Deny::WrongWindow)}
   if p.http_status!=200||!p.business_success||!hash_shape(&p.body_sha256){
    return Err(Deny::BadResponse)}
   if p.index!=expected_index||p.cursor_in!=expected_cursor{
    return Err(Deny::BadPage)}
   if p.requested_limit==0||p.requested_limit>lane.max_rows()||
    p.rows.len()>p.requested_limit{return Err(Deny::BadPage)}
   // A change of limit inside any source lane invalidates pagination
   // completeness: the subsequent cursor/terminal proof no longer matches.
   if source_page_limit.is_some_and(|prior|prior!=p.requested_limit){
    return Err(Deny::BadPage)
   }
   source_page_limit=Some(p.requested_limit);
   // OKX bills-archive uses `after=<last billId>`. A full terminal
   // page or a short continuation is not proof of complete pagination.
   if lane.endpoint()=="/api/v5/account/bills-archive"{
    let mut previous=match p.cursor_in.as_deref(){
     Some(s)=>Some(s.parse::<u64>().ok().filter(|id|*id>0)
       .filter(|id|id.to_string()==s).ok_or(Deny::BadPage)?),
     None=>None,
    };
    for row in &p.rows{
     let id=row.unique_key.parse::<u64>().ok().filter(|id|*id>0)
       .filter(|id|id.to_string()==row.unique_key)
       .ok_or(Deny::BadRow)?;
     if previous.is_some_and(|prev|id>=prev){return Err(Deny::BadRow)}
     previous=Some(id);
    }
    if p.last_page==(p.rows.len()==p.requested_limit){
     return Err(Deny::BadPage)
    }
    if !p.last_page&&p.cursor_out.as_deref()!=
       p.rows.last().map(|r|r.unique_key.as_str()){
     return Err(Deny::BadPage)
    }
   }
   // The existing source-bound F2G collector requires an explicit empty
   // terminating fills-history response, including after a short page.
   // A nonempty last page would let a caller assert unproved completeness.
   if matches!(lane,Lane::OkxBuy|Lane::OkxSell)&&p.last_page!=p.rows.is_empty(){
    return Err(Deny::BadPage)
   }
   // OKX fills-history uses `after=<billId>`, NOT its tradeId key.
   // B6 lacks the raw billId per row, so this only validates numeric
   // advancing cursor shape; F2G must still bind cursor to original bytes.
   if matches!(lane,Lane::OkxBuy|Lane::OkxSell)&&!p.last_page{
    let next=p.cursor_out.as_deref().and_then(|s|s.parse::<u64>().ok())
      .filter(|n|*n>0).ok_or(Deny::BadPage)?;
    if p.cursor_out.as_deref()!=Some(next.to_string().as_str()){
     return Err(Deny::BadPage)
    }
    if expected_cursor.as_deref().and_then(|s|s.parse::<u64>().ok())
      .is_some_and(|prev|next>=prev){
     return Err(Deny::BadPage)
    }
   }
   // Binance /income paginates by 1-based page number. The existing F2I
   // collector requires an explicit empty terminal page, even for short
   // nonempty responses; a caller-supplied `last_page` flag proves nothing.
   if matches!(lane,Lane::BinanceIncomeOriginal|Lane::BinanceIncomeDelayed){
    if p.last_page!=p.rows.is_empty(){return Err(Deny::BadPage)}
    if !p.last_page&&p.cursor_out.as_deref()!=
       Some((expected_index+2).to_string().as_str()){
      return Err(Deny::BadPage)
    }
    if expected_index>0&&p.cursor_in.as_deref()!=
       Some((expected_index+1).to_string().as_str()){
      return Err(Deny::BadPage)
    }
   }
   // Binance userTrades uses numeric `fromId=last trade ID + 1`.
   // A full final page needs a continuation; short continuation is invalid.
   if matches!(lane,Lane::BinanceShortOpen|Lane::BinanceShortClose){
    // fromId pagination is ordered by numeric tradeId. If even one row
    // moves backward, a last-ID cursor can silently skip an execution.
    let mut prev=p.cursor_in.as_deref().and_then(|s|s.parse::<u64>().ok())
      .and_then(|next|next.checked_sub(1));
    for row in &p.rows{
      let id=row.unique_key.parse::<u64>().map_err(|_|Deny::BadRow)?;
      if id==0||prev.is_some_and(|n|id<=n){return Err(Deny::BadRow)}
      prev=Some(id);
    }
    if p.last_page==(p.rows.len()==p.requested_limit){
     return Err(Deny::BadPage)
    }
    if !p.last_page{
     let next=p.rows.last().and_then(|r|r.unique_key.parse::<u64>().ok())
       .and_then(|v|v.checked_add(1)).map(|v|v.to_string());
     if next.as_deref()!=p.cursor_out.as_deref(){
       return Err(Deny::BadPage)
     }
    }
   }
   if p.last_page{
    if expected_index!=group.len()-1||p.cursor_out.is_some(){
     return Err(Deny::BadPage)}
   }else{
    let next=p.cursor_out.as_ref().ok_or(Deny::BadPage)?;
    if next.is_empty()||next.len()>200||p.rows.is_empty()||
     expected_index==group.len()-1||!cursor_set.insert(next.clone()){
     return Err(Deny::BadPage)}
   }
   for row in &p.rows{
    if row.unique_key.is_empty()||row.unique_key.len()>192||
      !row.unique_key.bytes().all(|b|(32..=126).contains(&b)){
     return Err(Deny::BadRow)}
    if matches!(lane,Lane::BinanceIncomeOriginal|Lane::BinanceIncomeDelayed){
     let (kind,id)=row.unique_key.split_once(':').ok_or(Deny::BadRow)?;
     if !matches!(kind,"COMMISSION"|"REALIZED_PNL"|"FUNDING_FEE")
       ||!order_id(id)||id.parse::<u64>().is_err()
       ||row.order_id.is_some()||row.symbol.is_some(){
      return Err(Deny::BadRow)
     }
    }
    if matches!(lane,Lane::BinanceShortOpen|Lane::BinanceShortClose)
      &&(!order_id(&row.unique_key)||row.unique_key.parse::<u64>().is_err()){
      return Err(Deny::BadRow)
    }
    if lane.fill()&&row.order_id.as_deref()!=owner.order(lane){
     return Err(Deny::WrongOrder)}
    if lane.fill()&&row.symbol.as_deref()!=owner.symbol(lane){
     return Err(Deny::WrongSymbol)}
    if !identities.insert((
      p.account_fingerprint.clone(),p.endpoint.clone(),row.unique_key.clone())){
     return Err(Deny::DuplicateRow)}
   }
   total=total.checked_add(p.rows.len()).ok_or(Deny::Unbounded)?;
   expected_cursor=p.cursor_out.clone();
  }
  if !group.last().map_or(false,|p|p.last_page){return Err(Deny::BadPage)}
 }
 Ok(LocalShapeOnly{page_count:pages.len(),row_count:total,lane_count:required.len()})
}
#[cfg(test)]
mod tests{
 use super::*;
 fn hash(c:char)->String{c.to_string().repeat(64)}
 fn owner()->OriginalOwner{
  OriginalOwner{
   generation:2,native_state_digest:hash('a'),
   okx_account_fingerprint:hash('b'),binance_account_fingerprint:hash('c'),
   okx_buy_order_id:"1001".into(),okx_sell_order_id:"1002".into(),
   binance_open_order_id:"2001".into(),binance_close_order_id:"2002".into(),
   base:"BTC".into(),spot_symbol:"BTC-USDT".into(),futures_symbol:"BTCUSDT".into(),
   start_ms:1_000_000,first_end_ms:2_000_000,late_end_ms:2_060_000,
  }
 }
 fn pages(o:&OriginalOwner)->Vec<Page>{
  let original=Lane::ALL.iter().enumerate().map(|(i,&lane)|{
   let (from,to)=o.window(lane);
   Page{
    lane,generation:o.generation,native_state_digest:o.native_state_digest.clone(),
    account_fingerprint:o.account(lane).to_owned(),endpoint:lane.endpoint().into(),
    order_id:o.order(lane).map(str::to_owned),
    symbol:o.symbol(lane).map(str::to_owned),
    from_ms:from,to_ms:to,index:0,cursor_in:None,cursor_out:None,
    last_page:true,requested_limit:lane.max_rows(),
    http_status:200,business_success:true,body_sha256:hash('d'),
    rows:if lane.fill(){
     vec![SourceRow{unique_key:if lane.okx(){format!("fill_{i}")}
         else{format!("{}",1_000+i)},
       order_id:o.order(lane).map(str::to_owned),
       symbol:o.symbol(lane).map(str::to_owned)}]
    }else{vec![]}
   }
  }).collect::<Vec<_>>();
  let mut result:Vec<Page>=original;
  for idx in [0usize,1]{
   let token=(301+idx*100).to_string();
   result[idx].last_page=false;
   result[idx].cursor_out=Some(token.clone());
   let mut terminal=result[idx].clone();
   terminal.index=1;terminal.rows.clear();
   terminal.last_page=true;terminal.cursor_in=Some(token);
   terminal.cursor_out=None;
   result.push(terminal);
  }
  result
 }
 #[test]fn okx_original_fills_must_have_explicit_empty_terminal(){
  let o=owner();let mut p=pages(&o);
  p.retain(|r|!(r.lane==Lane::OkxBuy&&r.index==1));
  p[0].last_page=true;p[0].cursor_out=None;
  assert_eq!(inspect_untrusted_catalogue(&o,&p).unwrap_err(),Deny::BadPage);
 }
 #[test]fn ten_lanes_never_authorize_profit(){
  let o=owner();let c=inspect_untrusted_catalogue(&o,&pages(&o)).unwrap();
  assert_eq!((c.page_count,c.row_count,c.lane_count),(12,4,10));
  assert!(!c.exchange_origin_proven()&&!c.realized_profit_proven());
  assert!(!c.complete_account_history_proven());
  assert!(!c.all_four_legs_verified_closed());
  assert!(!c.may_trade_cancel_transfer_or_release());
 }
 fn original_seven_pages(o:&OriginalOwner)->Vec<Page>{
  pages(o).into_iter().filter(|p|ORIGINAL_SEVEN.contains(&p.lane)).collect()
 }
 #[test]fn exactly_original_seven_can_be_checked_without_fake_delayed_pages(){
  let o=owner();let p=original_seven_pages(&o);
  let c=inspect_untrusted_original_seven(&o,&p).unwrap();
  assert_eq!((c.page_count,c.row_count,c.lane_count),(9,4,7));
  assert!(!c.complete_account_history_proven());
  assert!(!c.all_four_legs_verified_closed());
  assert!(!c.realized_profit_proven());
  assert!(!c.may_trade_cancel_transfer_or_release());
  assert_eq!(inspect_untrusted_catalogue(&o,&p).unwrap_err(),Deny::MissingLane);
 }
 #[test]fn original_seven_refuses_late_lane_masquerading_as_source(){
  let o=owner();let mut p=original_seven_pages(&o);
  let late=pages(&o).into_iter().find(|p|p.lane==Lane::OkxBaseDelayed).unwrap();
  p.push(late);
  assert_eq!(inspect_untrusted_original_seven(&o,&p).unwrap_err(),Deny::MissingLane);
 }
 #[test]fn original_seven_refuses_missing_or_unterminated_original(){
  let o=owner();let mut p=original_seven_pages(&o);
  p.retain(|p|!(p.lane==Lane::OkxBuy&&p.index==1));
  assert_eq!(inspect_untrusted_original_seven(&o,&p).unwrap_err(),Deny::BadPage);
  let mut p=original_seven_pages(&o);
  p.retain(|p|p.lane!=Lane::BinanceIncomeOriginal);
  assert_eq!(inspect_untrusted_original_seven(&o,&p).unwrap_err(),Deny::MissingLane);
  let mut p=original_seven_pages(&o);p[0].generation+=1;
  assert_eq!(inspect_untrusted_original_seven(&o,&p).unwrap_err(),Deny::WrongOwner);
 }
 #[test]fn reject_missing_binance_trade_history(){
  let o=owner();let mut p=pages(&o);
  p.retain(|r|r.lane!=Lane::BinanceShortClose);
  assert_eq!(inspect_untrusted_catalogue(&o,&p).unwrap_err(),Deny::MissingLane);
 }
 #[test]fn reject_wrong_generation_and_account(){
  let o=owner();let mut p=pages(&o);p[0].generation+=1;
  assert_eq!(inspect_untrusted_catalogue(&o,&p).unwrap_err(),Deny::WrongOwner);
  let mut p=pages(&o);p[2].account_fingerprint=hash('f');
  assert_eq!(inspect_untrusted_catalogue(&o,&p).unwrap_err(),Deny::WrongOwner);
 }
 #[test]fn reject_wrong_order_and_symbol(){
  let o=owner();let mut p=pages(&o);p[0].order_id=Some("1002".into());
  assert_eq!(inspect_untrusted_catalogue(&o,&p).unwrap_err(),Deny::WrongOrder);
  let mut p=pages(&o);p[2].rows[0].symbol=Some("ETHUSDT".into());
  assert_eq!(inspect_untrusted_catalogue(&o,&p).unwrap_err(),Deny::WrongSymbol);
 }
 #[test]fn reject_wrong_window_and_bad_response(){
  let o=owner();let mut p=pages(&o);p[9].to_ms+=1;
  assert_eq!(inspect_untrusted_catalogue(&o,&p).unwrap_err(),Deny::WrongWindow);
  let mut p=pages(&o);p[4].http_status=401;
  assert_eq!(inspect_untrusted_catalogue(&o,&p).unwrap_err(),Deny::BadResponse);
 }
 #[test]fn reject_incomplete_pagination(){
  let o=owner();let mut p=pages(&o);
  p[0].last_page=false;p[0].cursor_out=Some("next".into());
  assert_eq!(inspect_untrusted_catalogue(&o,&p).unwrap_err(),Deny::BadPage);
 }
 #[test]fn original_okx_fill_valid_billid_after_and_empty_terminal(){
  let o=owner();let mut p=pages(&o);
  p.retain(|r|!(r.lane==Lane::OkxBuy&&r.index==1));
  p[0].last_page=false;p[0].cursor_out=Some("301".into());
  let mut tail=p[0].clone();tail.index=1;tail.rows.clear();
  tail.cursor_in=Some("301".into());tail.cursor_out=None;tail.last_page=true;
  p.push(tail);
  assert_eq!(inspect_untrusted_catalogue(&o,&p).unwrap().page_count,12);
 }
 #[test]fn original_okx_fill_after_cannot_be_text_or_zero(){
  let o=owner();let mut p=pages(&o);
  p[0].last_page=false;p[0].cursor_out=Some("not-a-bill".into());
  let mut tail=p[0].clone();tail.index=1;tail.rows.clear();
  tail.cursor_in=p[0].cursor_out.clone();tail.cursor_out=None;tail.last_page=true;
  p.push(tail);
  assert_eq!(inspect_untrusted_catalogue(&o,&p).unwrap_err(),Deny::BadPage);
  let mut p=pages(&o);p[0].last_page=false;p[0].cursor_out=Some("0".into());
  let mut tail=p[0].clone();tail.index=1;tail.rows.clear();
  tail.cursor_in=Some("0".into());tail.cursor_out=None;tail.last_page=true;
  p.push(tail);
  assert_eq!(inspect_untrusted_catalogue(&o,&p).unwrap_err(),Deny::BadPage);
 }
 #[test]fn original_okx_fill_after_must_progress_toward_older_billids(){
  let o=owner();let mut p=pages(&o);
  p[0].last_page=false;p[0].cursor_out=Some("301".into());
  let mut second=p[0].clone();second.index=1;
  second.cursor_in=Some("301".into());second.cursor_out=Some("302".into());
  let mut tail=second.clone();tail.index=2;tail.rows.clear();
  tail.cursor_in=Some("302".into());tail.cursor_out=None;tail.last_page=true;
  p.push(second);p.push(tail);
  assert_eq!(inspect_untrusted_catalogue(&o,&p).unwrap_err(),Deny::BadPage);
 }
 #[test]fn original_okx_fill_after_descending_cursor_is_accepted(){
  let o=owner();let mut p=pages(&o);
  p.retain(|r|!(r.lane==Lane::OkxBuy&&r.index==1));
  p[0].last_page=false;p[0].cursor_out=Some("301".into());
  let mut second=p[0].clone();second.index=1;
  second.cursor_in=Some("301".into());second.cursor_out=Some("299".into());
  second.rows[0].unique_key="older-unique-fill".into();
  let mut tail=second.clone();tail.index=2;tail.rows.clear();
  tail.cursor_in=Some("299".into());tail.cursor_out=None;tail.last_page=true;
  p.push(second);p.push(tail);
  assert_eq!(inspect_untrusted_catalogue(&o,&p).unwrap().page_count,13);
 }
 #[test]fn original_okx_fill_after_noncanonical_number_rejected(){
  let o=owner();let mut p=pages(&o);
  p[0].last_page=false;p[0].cursor_out=Some("0301".into());
  let mut tail=p[0].clone();tail.index=1;tail.rows.clear();
  tail.cursor_in=Some("0301".into());tail.cursor_out=None;tail.last_page=true;
  p.push(tail);
  assert_eq!(inspect_untrusted_catalogue(&o,&p).unwrap_err(),Deny::BadPage);
 }
 #[test]fn original_bill_page_cannot_claim_full_page_is_terminal(){
  let o=owner();let mut p=pages(&o);
  p[4].requested_limit=1;
  p[4].rows.push(SourceRow{unique_key:"801".into(),order_id:None,symbol:None});
  assert_eq!(inspect_untrusted_catalogue(&o,&p).unwrap_err(),Deny::BadPage);
 }
 #[test]fn original_bill_cursor_must_follow_full_page_last_bill_id(){
  let o=owner();let mut p=pages(&o);
  p[4].requested_limit=1;
  p[4].rows.push(SourceRow{unique_key:"801".into(),order_id:None,symbol:None});
  p[4].last_page=false;p[4].cursor_out=Some("forged".into());
  let mut tail=p[4].clone();tail.index=1;tail.cursor_in=Some("forged".into());
  tail.rows.clear();tail.cursor_out=None;tail.last_page=true;
  p.insert(5,tail);
  assert_eq!(inspect_untrusted_catalogue(&o,&p).unwrap_err(),Deny::BadPage);
 }
 #[test]fn original_bill_nonterminal_page_cannot_be_short(){
  let o=owner();let mut p=pages(&o);
  p[4].requested_limit=2;
  p[4].rows.push(SourceRow{unique_key:"801".into(),order_id:None,symbol:None});
  p[4].last_page=false;p[4].cursor_out=Some("801".into());
  let mut tail=p[4].clone();tail.index=1;tail.cursor_in=Some("801".into());
  tail.rows.clear();tail.cursor_out=None;tail.last_page=true;
  p.insert(5,tail);
  assert_eq!(inspect_untrusted_catalogue(&o,&p).unwrap_err(),Deny::BadPage);
 }
 #[test]fn original_okx_bill_non_numeric_source_key_must_fail(){
  let o=owner();let mut p=pages(&o);
  p[4].rows.push(SourceRow{unique_key:"forged-bill-key".into(),
     order_id:None,symbol:None});
  assert_eq!(inspect_untrusted_catalogue(&o,&p).unwrap_err(),Deny::BadRow);
 }
 #[test]fn original_okx_bill_leading_zero_source_key_must_fail(){
  let o=owner();let mut p=pages(&o);
  p[4].rows.push(SourceRow{unique_key:"000801".into(),order_id:None,symbol:None});
  assert_eq!(inspect_untrusted_catalogue(&o,&p).unwrap_err(),Deny::BadRow);
 }
 #[test]fn original_okx_bill_ascending_ids_in_page_must_fail(){
  let o=owner();let mut p=pages(&o);
  p[4].rows.push(SourceRow{unique_key:"801".into(),order_id:None,symbol:None});
  p[4].rows.push(SourceRow{unique_key:"802".into(),order_id:None,symbol:None});
  assert_eq!(inspect_untrusted_catalogue(&o,&p).unwrap_err(),Deny::BadRow);
 }
 #[test]fn original_okx_bill_newer_id_after_cursor_must_fail(){
  let o=owner();let mut p=pages(&o);
  p[4].requested_limit=2;
  for id in ["801","800"]{
   p[4].rows.push(SourceRow{unique_key:id.into(),order_id:None,symbol:None});
  }
  p[4].last_page=false;p[4].cursor_out=Some("800".into());
  let mut later=p[4].clone();later.index=1;
  later.rows=vec![SourceRow{unique_key:"805".into(),order_id:None,symbol:None}];
  later.cursor_in=Some("800".into());later.cursor_out=None;later.last_page=true;
  p.push(later);
  assert_eq!(inspect_untrusted_catalogue(&o,&p).unwrap_err(),Deny::BadRow);
 }
 #[test]fn original_okx_bill_valid_descending_page_and_short_terminal(){
  let o=owner();let mut p=pages(&o);p[4].requested_limit=2;
  for id in ["810","809"]{
   p[4].rows.push(SourceRow{unique_key:id.into(),order_id:None,symbol:None});
  }
  p[4].last_page=false;p[4].cursor_out=Some("809".into());
  let mut later=p[4].clone();later.index=1;
  later.rows=vec![SourceRow{unique_key:"808".into(),order_id:None,symbol:None}];
  later.cursor_in=Some("809".into());later.cursor_out=None;later.last_page=true;
  p.push(later);
  assert_eq!(inspect_untrusted_catalogue(&o,&p).unwrap().page_count,13);
 }
 #[test]fn original_bill_exact_full_page_then_empty_terminal_is_ok(){
  let o=owner();let mut p=pages(&o);
  p[4].requested_limit=1;
  p[4].rows.push(SourceRow{unique_key:"801".into(),order_id:None,symbol:None});
  p[4].last_page=false;p[4].cursor_out=Some("801".into());
  let mut tail=p[4].clone();tail.index=1;tail.cursor_in=Some("801".into());
  tail.rows.clear();tail.cursor_out=None;tail.last_page=true;
  p.insert(5,tail);
  assert_eq!(inspect_untrusted_catalogue(&o,&p).unwrap().page_count,13);
 }
 #[test]fn delayed_okx_bill_full_terminal_must_also_be_denied(){
  let o=owner();let mut p=pages(&o);
  p[6].requested_limit=1;
  p[6].rows.push(SourceRow{unique_key:"601".into(),order_id:None,symbol:None});
  assert_eq!(inspect_untrusted_catalogue(&o,&p).unwrap_err(),Deny::BadPage);
 }
 #[test]fn original_okx_bill_request_limit_cannot_drift_across_pages(){
  let o=owner();let mut p=pages(&o);
  p[4].requested_limit=1;
  p[4].rows.push(SourceRow{unique_key:"801".into(),order_id:None,symbol:None});
  p[4].last_page=false;p[4].cursor_out=Some("801".into());
  let mut tail=p[4].clone();tail.index=1;tail.cursor_in=Some("801".into());
  tail.rows.clear();tail.requested_limit=2;tail.cursor_out=None;tail.last_page=true;
  p.insert(5,tail);
  assert_eq!(inspect_untrusted_catalogue(&o,&p).unwrap_err(),Deny::BadPage);
 }
 #[test]fn reject_double_income_record_across_windows(){
  let o=owner();let mut p=pages(&o);
  p[8].rows.push(SourceRow{unique_key:"COMMISSION:998".into(),
     order_id:None,symbol:None});
  p[9].rows=p[8].rows.clone();
  for idx in [8usize,9]{
   p[idx].last_page=false;p[idx].cursor_out=Some("2".into());
   let mut tail=p[idx].clone();tail.index=1;tail.rows.clear();
   tail.cursor_in=Some("2".into());tail.cursor_out=None;tail.last_page=true;
   p.push(tail);
  }
  assert_eq!(inspect_untrusted_catalogue(&o,&p).unwrap_err(),Deny::DuplicateRow);
 }
 #[test]fn binance_income_nonempty_terminal_page_cannot_prove_exhaustion(){
  let o=owner();let mut p=pages(&o);
  p[8].rows.push(SourceRow{unique_key:"COMMISSION:310".into(),order_id:None,symbol:None});
  assert_eq!(inspect_untrusted_catalogue(&o,&p).unwrap_err(),Deny::BadPage);
 }
 #[test]fn binance_income_page_continuation_must_use_exact_page_number(){
  let o=owner();let mut p=pages(&o);
  p[8].rows.push(SourceRow{unique_key:"COMMISSION:310".into(),order_id:None,symbol:None});
  p[8].last_page=false;p[8].cursor_out=Some("wrong".into());
  let mut tail=p[8].clone();tail.index=1;tail.rows.clear();
  tail.last_page=true;tail.cursor_out=None;tail.cursor_in=Some("wrong".into());
  p.insert(9,tail);
  assert_eq!(inspect_untrusted_catalogue(&o,&p).unwrap_err(),Deny::BadPage);
 }
 #[test]fn binance_income_malformed_source_key_cannot_be_proven(){
  let o=owner();let mut p=pages(&o);
  p[9].rows.push(SourceRow{unique_key:"310".into(),order_id:None,symbol:None});
  p[9].last_page=false;p[9].cursor_out=Some("2".into());
  let mut tail=p[9].clone();tail.index=1;tail.rows.clear();
  tail.last_page=true;tail.cursor_out=None;tail.cursor_in=Some("2".into());
  p.insert(10,tail);
  assert_eq!(inspect_untrusted_catalogue(&o,&p).unwrap_err(),Deny::BadRow);
 }
 #[test]fn binance_income_valid_explicit_empty_terminal_page(){
  let o=owner();let mut p=pages(&o);
  p[8].rows.push(SourceRow{unique_key:"COMMISSION:310".into(),order_id:None,symbol:None});
  p[8].last_page=false;p[8].cursor_out=Some("2".into());
  let mut tail=p[8].clone();tail.index=1;tail.rows.clear();
  tail.last_page=true;tail.cursor_out=None;tail.cursor_in=Some("2".into());
  p.insert(9,tail);
  assert_eq!(inspect_untrusted_catalogue(&o,&p).unwrap().page_count,13);
 }
 #[test]fn binance_trades_noncanonical_trade_id_must_fail(){
  let o=owner();let mut p=pages(&o);
  p[2].rows[0].unique_key="foreign".into();
  assert_eq!(inspect_untrusted_catalogue(&o,&p).unwrap_err(),Deny::BadRow);
 }
 #[test]fn binance_trades_full_terminal_page_is_not_exhaustion(){
  let o=owner();let mut p=pages(&o);p[2].requested_limit=1;
  assert_eq!(inspect_untrusted_catalogue(&o,&p).unwrap_err(),Deny::BadPage);
 }
 #[test]fn binance_trades_cursor_must_be_last_trade_id_plus_one(){
  let o=owner();let mut p=pages(&o);p[2].requested_limit=1;
  p[2].last_page=false;p[2].cursor_out=Some("777".into());
  let mut tail=p[2].clone();tail.index=1;tail.rows.clear();
  tail.cursor_in=Some("777".into());tail.cursor_out=None;tail.last_page=true;
  p.push(tail);
  assert_eq!(inspect_untrusted_catalogue(&o,&p).unwrap_err(),Deny::BadPage);
 }
 #[test]fn binance_income_limit_drift_between_pages_denied(){
  let o=owner();let mut p=pages(&o);
  p[8].rows.push(SourceRow{unique_key:"COMMISSION:310".into(),
      order_id:None,symbol:None});
  p[8].last_page=false;p[8].cursor_out=Some("2".into());
  let mut tail=p[8].clone();tail.index=1;tail.rows.clear();
  tail.cursor_in=Some("2".into());tail.cursor_out=None;
  tail.requested_limit=99;tail.last_page=true;p.push(tail);
  assert_eq!(inspect_untrusted_catalogue(&o,&p).unwrap_err(),Deny::BadPage);
 }
 #[test]fn binance_trades_limit_drift_between_pages_denied(){
  let o=owner();let mut p=pages(&o);p[2].requested_limit=1;
  p[2].last_page=false;p[2].cursor_out=Some("1003".into());
  let mut tail=p[2].clone();tail.index=1;tail.rows.clear();
  tail.cursor_in=Some("1003".into());tail.cursor_out=None;
  tail.requested_limit=2;tail.last_page=true;p.push(tail);
  assert_eq!(inspect_untrusted_catalogue(&o,&p).unwrap_err(),Deny::BadPage);
 }
 #[test]fn binance_trades_reversed_ids_inside_full_page_must_fail(){
  let o=owner();let mut p=pages(&o);
  p[2].requested_limit=2;
  let mut older=p[2].rows[0].clone();older.unique_key="1001".into();
  p[2].rows.push(older);
  p[2].last_page=false;p[2].cursor_out=Some("1002".into());
  let mut tail=p[2].clone();tail.index=1;tail.rows.clear();
  tail.cursor_in=Some("1002".into());tail.cursor_out=None;tail.last_page=true;
  p.push(tail);
  assert_eq!(inspect_untrusted_catalogue(&o,&p).unwrap_err(),Deny::BadRow);
 }
 #[test]fn binance_trades_page_must_not_go_behind_from_id(){
  let o=owner();let mut p=pages(&o);
  p[2].requested_limit=1;p[2].last_page=false;
  p[2].cursor_out=Some("1003".into());
  let mut page2=p[2].clone();page2.index=1;
  page2.rows[0].unique_key="1001".into();
  page2.cursor_in=Some("1003".into());page2.cursor_out=Some("1002".into());
  let mut tail=page2.clone();tail.index=2;tail.rows.clear();
  tail.cursor_in=Some("1002".into());tail.cursor_out=None;tail.last_page=true;
  p.push(page2);p.push(tail);
  assert_eq!(inspect_untrusted_catalogue(&o,&p).unwrap_err(),Deny::BadRow);
 }
 #[test]fn binance_trades_valid_full_then_empty_tail(){
  let o=owner();let mut p=pages(&o);p[2].requested_limit=1;
  p[2].last_page=false;p[2].cursor_out=Some("1003".into());
  let mut tail=p[2].clone();tail.index=1;tail.rows.clear();
  tail.cursor_in=Some("1003".into());tail.cursor_out=None;tail.last_page=true;
  p.push(tail);
  assert_eq!(inspect_untrusted_catalogue(&o,&p).unwrap().page_count,13);
 }
 #[test]fn empty_account_pages_do_not_mean_zero_profit(){
  let o=owner();let result=inspect_untrusted_catalogue(&o,&pages(&o)).unwrap();
  assert!(!result.realized_profit_proven());
 }
}
