//! R32D6C8B2B2A normal-linked synthetic signed GET response gate.
//! Absolutely no production credentials, network, order mutation or Native Hedged.
use std::sync::atomic::{AtomicUsize,Ordering};
use spotarb_native_market_core::{
 native_close_readonly::{audit_dual_close_order_receipts_once,DualCloseCandidate,CloseOrderIdentity,DualCloseError},
 private_https::{HttpExecutor,HttpsRequest,HttpResponse,TransportError},
 private_rest::{BinanceCredentials,OkxCredentials,OkxDomain,HttpMethod},
 fill_economics::decimal_to_atoms,
};
const NOW:u64=1_780_000_000_000;
#[derive(Clone,Copy,Debug)]
enum Fault{
 None,SpotStatus,FutureNotReduceOnly,FutureSide,WrongOrder,
 ForeignSpotFill,FeeMissing,WrongQty,FullOkxPage,
 SpotNonemptyTail,FutureNonemptyTail,FutureForeignOrder,
 FutureFeeUnknown,FutureDuplicateId,StaleSpotFill,Http500,
 FutureMissingRealizedPnl,FutureWrongMarginAsset,FutureBuyerFalse,
}
struct Fake{fault:Fault,calls:AtomicUsize}
impl Fake{fn new(fault:Fault)->Self{Self{fault,calls:AtomicUsize::new(0)}}}
fn anchors()->DualCloseCandidate{
 DualCloseCandidate{
  position_identity:"native-exit-1".into(),base:"BNT".into(),
  created_at_ms:NOW-20_000,expected_qty_e12:decimal_to_atoms("0.09").unwrap(),
  spot:CloseOrderIdentity{
   symbol:"BNT/USDT".into(),client_id:"spot-close-1".into(),
   exchange_order_id:"201".into(),
  },
  futures:CloseOrderIdentity{
   symbol:"BNTUSDT".into(),client_id:"futures-close-1".into(),
   exchange_order_id:"202".into(),
  },
 }
}
impl HttpExecutor for Fake {
 fn execute_request(&self,req:&HttpsRequest)->Result<HttpResponse,TransportError>{
  assert_eq!(req.method,HttpMethod::Get);
  assert!(!req.mutation);
  assert!(req.credential_bearing);
  assert!(req.body.is_none());
  let url=url::Url::parse(&req.url).unwrap();
  let qp=url.query_pairs().collect::<std::collections::BTreeMap<_,_>>();
  self.calls.fetch_add(1,Ordering::SeqCst);
  let val=match url.path(){
   "/api/v5/trade/order"=>{
     assert_eq!(qp.get("clOrdId").map(|x|x.as_ref()),Some("spot-close-1"));
     serde_json::json!({"code":"0","data":[{
      "instId":"BNT-USDT","ordId":"201","clOrdId":"spot-close-1","side":"sell",
      "state":if matches!(self.fault,Fault::SpotStatus){"partially_filled"}else{"filled"},
      "accFillSz":"0.09","avgPx":"100","fillTime":(NOW-1000).to_string(),
     }]})
   },
   "/fapi/v1/order"=>{
    assert_eq!(qp.get("origClientOrderId").map(|x|x.as_ref()),Some("futures-close-1"));
    serde_json::json!({"symbol":"BNTUSDT",
       "orderId":if matches!(self.fault,Fault::WrongOrder){999}else{202},
       "clientOrderId":"futures-close-1",
       "side":if matches!(self.fault,Fault::FutureSide){"SELL"}else{"BUY"},
       "positionSide":"BOTH",
       "reduceOnly":!matches!(self.fault,Fault::FutureNotReduceOnly),
       "status":"FILLED","executedQty":"0.09","avgPrice":"98",
       "cumQuote":"8.82","updateTime":NOW-900})
   },
   "/api/v5/trade/fills-history"=>{
    assert_eq!(qp.get("ordId").map(|x|x.as_ref()),Some("201"));
    if qp.contains_key("after"){
     if matches!(self.fault,Fault::SpotNonemptyTail){
      serde_json::json!({"code":"0","data":[{
       "ordId":"201","billId":"300","tradeId":"S2",
       "fillPx":"100","fillSz":"0.01","fee":"-0.001",
       "feeCcy":"USDT","fillTime":(NOW-990).to_string(),
      }]})
     }else{serde_json::json!({"code":"0","data":[]})}
    }else{
     let fill=serde_json::json!({"ordId":if matches!(self.fault,Fault::ForeignSpotFill){"999"}else{"201"},
      "billId":"301","tradeId":"S1","fillPx":"100",
      "fillSz":if matches!(self.fault,Fault::WrongQty){"0.08"}else{"0.09"},
      "fee":"-0.009","feeCcy":if matches!(self.fault,Fault::FeeMissing){"DOGE"}else{"USDT"},
      "fillTime":if matches!(self.fault,Fault::StaleSpotFill){NOW-30_000}else{NOW-1000}
       });
     if matches!(self.fault,Fault::FullOkxPage){
      serde_json::json!({"code":"0","data":[fill.clone(),fill]})
     }else{serde_json::json!({"code":"0","data":[fill]})}
    }
   },
   "/fapi/v1/userTrades"=>{
    assert_eq!(qp.get("orderId").map(|x|x.as_ref()),Some("202"));
    if qp.get("fromId").map(|x|x.as_ref())==Some("1002"){
     if matches!(self.fault,Fault::FutureNonemptyTail){
      serde_json::json!([{"orderId":202,"id":1002,"symbol":"BNTUSDT",
       "side":"BUY","positionSide":"BOTH","buyer":true,"marginAsset":"USDT","realizedPnl":"0.03","price":"98","qty":"0.01",
       "commission":"0.001","commissionAsset":"USDT","time":NOW-800}])
     }else{serde_json::json!([])}
    }else{
     assert_eq!(qp.get("fromId").map(|x|x.as_ref()),Some("0"));
     let fill=serde_json::json!({
      "orderId":if matches!(self.fault,Fault::FutureForeignOrder){999}else{202},
      "id":1001,"symbol":"BNTUSDT","side":"BUY","positionSide":"BOTH",
      "buyer":!matches!(self.fault,Fault::FutureBuyerFalse),
      "marginAsset":if matches!(self.fault,Fault::FutureWrongMarginAsset){"BUSD"}else{"USDT"},
      "realizedPnl":if matches!(self.fault,Fault::FutureMissingRealizedPnl){serde_json::Value::Null}else{serde_json::json!("0.27")},
      "price":"98","qty":"0.09","commission":"0.01",
      "commissionAsset":if matches!(self.fault,Fault::FutureFeeUnknown){"BNB"}else{"USDT"},
      "time":NOW-900,
     });
     if matches!(self.fault,Fault::FutureDuplicateId){serde_json::json!([fill.clone(),fill])}
     else{serde_json::json!([fill])}
    }
   },
   other=>panic!("unexpected mock GET endpoint {other}"),
  };
  Ok(HttpResponse{
   status:if matches!(self.fault,Fault::Http500){500}else{200},
   headers:vec![],body:val.to_string(),
  })
 }
}
fn audit(fake:&Fake,candidate:&DualCloseCandidate,okx_limit:u16)->Result<
 spotarb_native_market_core::native_close_readonly::DualCloseReadOnlyCandidate,
 DualCloseError>{
 let o=OkxCredentials::new("FAKEOKX","FAKESECRET","FAKEPASS").unwrap();
 let b=BinanceCredentials::new("FAKEBIN","FAKEBINSECRET").unwrap();
 audit_dual_close_order_receipts_once(
  fake,candidate,&o,&b,NOW,OkxDomain::Global,10_000,okx_limit,10)
}
#[test]
fn r32d6c8b2b2a_normal_linked_both_order_fills_exact_but_close_not_certified(){
 let fake=Fake::new(Fault::None);
 let p=audit(&fake,&anchors(),10).unwrap();
 assert_eq!(fake.calls.load(Ordering::SeqCst),6);
 assert_eq!(p.read_counts().0,2);assert_eq!(p.read_counts().1,4);
 assert_eq!(p.fill_counts().0,1);assert_eq!(p.fill_counts().1,1);
 assert_eq!(p.spot_ledger().filled_qty_atoms(),decimal_to_atoms("0.09").unwrap());
 assert_eq!(p.futures_ledger().filled_qty_atoms(),decimal_to_atoms("0.09").unwrap());
 assert_eq!(p.spot_ledger().fee_cost_quote_atoms,decimal_to_atoms("0.009").unwrap() as i128);
 assert_eq!(p.futures_ledger().fee_cost_quote_atoms,decimal_to_atoms("0.01").unwrap() as i128);
 assert_eq!(p.futures_reported_realized_pnl_e12(),decimal_to_atoms("0.27").unwrap() as i128);
 assert!(p.spot_ledger().fee_evidence_complete&&p.futures_ledger().fee_evidence_complete);
 assert!(!p.all_wallet_balance_brackets_proven());
 assert!(!p.futures_flat_proven());
 assert!(!p.funding_complete());
 assert!(!p.currency_bills_complete());
 assert!(!p.global_history_complete());
 assert!(!p.cross_venue_atomicity_proven());
 assert!(!p.close_verified()&&!p.may_send_order());
}
#[test]
fn r32d6c8b2b2a_signed_get_denies_partial_cancelled_or_wrong_reduce_only(){
 for fault in [Fault::SpotStatus,Fault::FutureNotReduceOnly,Fault::FutureSide,
               Fault::WrongOrder,Fault::Http500]{
  assert!(audit(&Fake::new(fault),&anchors(),10).is_err(),"fault {fault:?}");
 }
}
#[test]
fn r32d6c8b2b2a_exact_order_ids_qty_and_quote_fee_are_mandatory(){
 for fault in [Fault::ForeignSpotFill,Fault::FeeMissing,Fault::WrongQty,
               Fault::FutureForeignOrder,Fault::FutureFeeUnknown,Fault::StaleSpotFill,
               Fault::FutureMissingRealizedPnl,Fault::FutureWrongMarginAsset,
               Fault::FutureBuyerFalse]{
  assert!(audit(&Fake::new(fault),&anchors(),10).is_err(),"fault {fault:?}");
 }
}
#[test]
fn r32d6c8b2b2a_unexhausted_or_duplicate_page_denied(){
 for fault in [Fault::FullOkxPage,Fault::SpotNonemptyTail,
               Fault::FutureNonemptyTail,Fault::FutureDuplicateId]{
  assert!(audit(&Fake::new(fault),&anchors(),2).is_err(),"fault {fault:?}");
 }
}
#[test]
fn r32d6c8b2b2a_invalid_unanchored_order_identity_never_calls_exchange(){
 let fake=Fake::new(Fault::None);
 let mut p=anchors();p.spot.exchange_order_id="abc".into();
 assert!(audit(&fake,&p,10).is_err());
 let mut p=anchors();p.futures.client_id=p.spot.client_id.clone();
 assert!(audit(&fake,&p,10).is_err());
 let mut p=anchors();p.expected_qty_e12=0;
 assert!(audit(&fake,&p,10).is_err());
 let mut p=anchors();p.created_at_ms=NOW+1;
 assert!(audit(&fake,&p,10).is_err());
 assert_eq!(fake.calls.load(Ordering::SeqCst),0);
}
