// R189-R190: OFFLINE evidence SHAPE preflight, not authentic exchange API evidence.
// These schemas cannot attest exchange fills or current account state themselves.
#[derive(Debug,Clone,Copy,PartialEq,Eq,Hash)]
pub enum EvidenceVenueR189{OkxSpot,BinanceUsdtMFutures}
#[derive(Debug,Clone)]
pub struct FillCandidateR189{
 pub venue:EvidenceVenueR189,pub account_owner:String,pub symbol:String,
 pub order_id:String,pub fill_id:String,pub fee_currency:String,
 pub executed_qty:f64,pub executed_price:f64,pub fee_amount:f64,
 pub exchange_timestamp_ms:u64,pub modeled_paper:bool,
}
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct EvidenceShapeR189{
 pub records:usize,pub has_both_venues:bool,
 pub exchange_attested:bool,pub may_trade:bool,
}
pub fn preflight_fill_shape_r189(rows:&[FillCandidateR189],now:u64)
 ->Result<EvidenceShapeR189,PaperAdapterErrorR156>{
 use std::collections::HashSet;
 if rows.is_empty()||rows.len()>128{return Err(PaperAdapterErrorR156::Limit)}
 let mut seen=HashSet::new();let mut spot=false;let mut future=false;
 let owner=&rows[0].account_owner;
 if reject_identity_r154(owner){return Err(PaperAdapterErrorR156::Source)}
 for r in rows{
  if r.modeled_paper||r.account_owner!=*owner||r.symbol.is_empty()
    ||r.symbol.len()>40||r.order_id.is_empty()||r.fill_id.is_empty()
    ||r.fee_currency.is_empty()||r.fee_currency.len()>12
    ||!r.executed_qty.is_finite()||r.executed_qty<=0.0
    ||!r.executed_price.is_finite()||r.executed_price<=0.0
    ||!r.fee_amount.is_finite()||r.fee_amount<0.0
    ||r.exchange_timestamp_ms==0||r.exchange_timestamp_ms>now.saturating_add(5000)
    ||now.saturating_sub(r.exchange_timestamp_ms)>86_400_000
    ||!seen.insert((r.venue,r.fill_id.clone())){
   return Err(PaperAdapterErrorR156::Source)
  }
  match r.venue{
   EvidenceVenueR189::OkxSpot=>spot=true,
   EvidenceVenueR189::BinanceUsdtMFutures=>future=true,
  }
 }
 if !spot||!future{return Err(PaperAdapterErrorR156::Source)}
 Ok(EvidenceShapeR189{
  records:rows.len(),has_both_venues:true,exchange_attested:false,may_trade:false
 })
}
#[derive(Debug,Clone)]
pub struct ZeroPositionCandidateR190{
 pub venue:EvidenceVenueR189,pub account_owner:String,
 pub open_positions:u32,pub open_orders:u32,pub unsettled_fills:u32,
 pub reported_at_ms:u64,pub reported_by_model:bool,
 pub account_scope_complete:bool,
}
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct ZeroPositionShapeR190{
 pub venues_checked:usize,pub zero_claim_consistent:bool,
 pub independently_attested:bool,pub may_trade:bool,
}
pub fn preflight_zero_claim_r190(rows:&[ZeroPositionCandidateR190],now:u64)
 ->Result<ZeroPositionShapeR190,PaperAdapterErrorR156>{
 if rows.len()!=2{return Err(PaperAdapterErrorR156::Source)}
 let owner=&rows[0].account_owner;
 if reject_identity_r154(owner){return Err(PaperAdapterErrorR156::Source)}
 let mut spot=false;let mut future=false;
 for r in rows{
  if r.account_owner!=*owner||r.reported_by_model
    ||!r.account_scope_complete||r.reported_at_ms==0
    ||now.saturating_sub(r.reported_at_ms)>10_000
    ||r.reported_at_ms>now.saturating_add(1000)
    ||r.open_positions!=0||r.open_orders!=0||r.unsettled_fills!=0{
   return Err(PaperAdapterErrorR156::Source)
  }
  match r.venue{
   EvidenceVenueR189::OkxSpot=>{
    if spot{return Err(PaperAdapterErrorR156::Source)}
    spot=true
   },
   EvidenceVenueR189::BinanceUsdtMFutures=>{
    if future{return Err(PaperAdapterErrorR156::Source)}
    future=true
   },
  }
 }
 if !spot||!future{return Err(PaperAdapterErrorR156::Source)}
 Ok(ZeroPositionShapeR190{
  venues_checked:2,zero_claim_consistent:true,
  independently_attested:false,may_trade:false
 })
}
#[cfg(test)]
mod r189_r190_tests{
 use super::*;
 const NOW:u64=1_792_280_000_000;
 fn fills()->Vec<FillCandidateR189>{
  [EvidenceVenueR189::OkxSpot,EvidenceVenueR189::BinanceUsdtMFutures]
   .iter().enumerate().map(|(i,v)|FillCandidateR189{
    venue:*v,account_owner:"mock-account-scope".into(),symbol:"BTCUSDT".into(),
    order_id:format!("order-{i}"),fill_id:format!("fill-{i}"),
    fee_currency:if i==0{"BTC"}else{"USDT"}.into(),
    executed_qty:0.01,executed_price:100_000.0,fee_amount:0.00001,
    exchange_timestamp_ms:NOW-1000,modeled_paper:false,
   }).collect()
 }
 fn positions()->Vec<ZeroPositionCandidateR190>{
  [EvidenceVenueR189::OkxSpot,EvidenceVenueR189::BinanceUsdtMFutures]
   .iter().map(|v|ZeroPositionCandidateR190{
    venue:*v,account_owner:"mock-account-scope".into(),
    open_positions:0,open_orders:0,unsettled_fills:0,
    reported_at_ms:NOW-500,reported_by_model:false,account_scope_complete:true
   }).collect()
 }
 #[test]fn r189_valid_shape_never_claims_authenticated_fills(){
  let mut f=fills();
  let v=preflight_fill_shape_r189(&f,NOW).unwrap();
  assert_eq!(v.records,2);
  assert!(!v.exchange_attested&&!v.may_trade);
  f[0].modeled_paper=true;
  assert!(preflight_fill_shape_r189(&f,NOW).is_err());
  f=fills();f[1].fill_id=f[0].fill_id.clone();f[1].venue=f[0].venue;
  assert!(preflight_fill_shape_r189(&f,NOW).is_err());
  f=fills();f[0].fee_amount=f64::NAN;
  assert!(preflight_fill_shape_r189(&f,NOW).is_err());
 }
 #[test]fn r190_zero_shape_not_real_independent_position_proof(){
  let mut rows=positions();
  let shape=preflight_zero_claim_r190(&rows,NOW).unwrap();
  assert!(shape.zero_claim_consistent);
  assert!(!shape.independently_attested&&!shape.may_trade);
  rows[1].open_positions=1;
  assert!(preflight_zero_claim_r190(&rows,NOW).is_err());
  rows=positions();rows[0].reported_by_model=true;
  assert!(preflight_zero_claim_r190(&rows,NOW).is_err());
  rows=positions();rows[1].open_orders=1;
  assert!(preflight_zero_claim_r190(&rows,NOW).is_err());
  rows=positions();rows[0].account_scope_complete=false;
  assert!(preflight_zero_claim_r190(&rows,NOW).is_err());
  rows=positions();rows[1].reported_at_ms=NOW-100_000;
  assert!(preflight_zero_claim_r190(&rows,NOW).is_err());
 }
}
