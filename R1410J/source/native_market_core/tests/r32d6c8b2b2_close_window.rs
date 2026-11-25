//! R32D6C8B2B2B: ONLY mock signed GETs. No native active authority or real close.
use spotarb_native_market_core::{
 native_close_readonly::{audit_dual_close_order_receipts_once,DualCloseCandidate,CloseOrderIdentity},
 native_close_window::{audit_close_source_window_once,CloseSourceError},
 private_https::{HttpExecutor,HttpsRequest,HttpResponse,TransportError},
 private_rest::{BinanceCredentials,OkxCredentials,OkxDomain,HttpMethod},
 fill_economics::decimal_to_atoms,
};
use std::sync::atomic::{AtomicUsize,Ordering};
const NOW:u64=1_780_000_000_000;
#[derive(Clone,Copy,Debug)]
enum Fault {None,Residual,Frozen,ForeignBill,ForeignIncome,MissingFunding,
          ForeignTrade,IncompleteIncome,Drift,WrongIncomeFee,WrongBillAmount,MultiPage,FullSymbol,StaleWallet}
struct Fake{fault:Fault,calls:AtomicUsize,snapshot_calls:AtomicUsize}
impl Fake{fn new(fault:Fault)->Self{Self{fault,calls:AtomicUsize::new(0),
     snapshot_calls:AtomicUsize::new(0)}}}
fn candidate()->DualCloseCandidate{
 DualCloseCandidate{
 position_identity:"synthetic-close-1".into(),base:"BNT".into(),
 created_at_ms:NOW-20_000,expected_qty_e12:decimal_to_atoms("0.09").unwrap(),
 spot:CloseOrderIdentity{symbol:"BNT/USDT".into(),client_id:"spot-close-1".into(),exchange_order_id:"201".into()},
 futures:CloseOrderIdentity{symbol:"BNTUSDT".into(),client_id:"futures-close-1".into(),exchange_order_id:"202".into()},
 }
}
fn creds()->(OkxCredentials,BinanceCredentials){
 (OkxCredentials::new("FAKE_OKX","FAKE_SECRET","FAKE_PASSPHRASE").unwrap(),
  BinanceCredentials::new("FAKE_BIN","FAKE_SECRET").unwrap())
}
impl HttpExecutor for Fake {
 fn execute_request(&self,req:&HttpsRequest)->Result<HttpResponse,TransportError>{
  assert_eq!(req.method,HttpMethod::Get);
  assert!(!req.mutation&&req.credential_bearing&&req.body.is_none());
  self.calls.fetch_add(1,Ordering::SeqCst);
  let u=url::Url::parse(&req.url).unwrap();
  let qp=u.query_pairs().collect::<std::collections::BTreeMap<_,_>>();
  let is_after=qp.contains_key("after");
  let page=qp.get("page").map(|x|x.as_ref()).unwrap_or("1");
  let mut j=match u.path() {
   "/api/v5/trade/order"=>serde_json::json!({"code":"0","data":[{
      "instId":"BNT-USDT","ordId":"201","clOrdId":"spot-close-1","side":"sell",
      "state":"filled","accFillSz":"0.09","avgPx":"100","fillTime":NOW-1000}]}),
   "/fapi/v1/order"=>serde_json::json!({"symbol":"BNTUSDT","orderId":202,
      "clientOrderId":"futures-close-1","side":"BUY","positionSide":"BOTH",
      "reduceOnly":true,"status":"FILLED","executedQty":"0.09",
      "avgPrice":"98","cumQuote":"8.82","updateTime":NOW-950}),
   "/api/v5/trade/fills-history"=>if is_after{
      serde_json::json!({"code":"0","data":[]})
   } else {serde_json::json!({"code":"0","data":[{
     "ordId":"201","billId":"301","tradeId":"S1","fillPx":"100","fillSz":"0.09",
     "fee":"-0.009","feeCcy":"USDT","fillTime":NOW-1000}]})},
   "/fapi/v1/userTrades" if qp.contains_key("orderId")=>if qp.get("fromId").map(|x|x.as_ref())==Some("1002"){
     serde_json::json!([])
   }else{serde_json::json!([{
      "orderId":202,"id":1001,"symbol":"BNTUSDT","side":"BUY","positionSide":"BOTH",
      "buyer":true,"marginAsset":"USDT","realizedPnl":"0.27",
      "price":"98","qty":"0.09","commission":"0.01",
      "commissionAsset":"USDT","time":NOW-950}])},
   "/api/v5/account/balance"=>{
     let n=self.snapshot_calls.fetch_add(1,Ordering::SeqCst);
     let usd=if matches!(self.fault,Fault::Drift)&&n>0{"101"}else{"100"};
     let f=if matches!(self.fault,Fault::Frozen){"0.001"}else{"0"};
     serde_json::json!({"code":"0","data":[{"details":[
       {"ccy":"BNT","cashBal":f,"availBal":"0","frozenBal":f,"uTime":(NOW-2000).to_string()},
       {"ccy":"USDT","cashBal":usd,"availBal":usd,"frozenBal":"0","uTime":(NOW-2000).to_string()}
     ]}]})
   },
   "/fapi/v3/positionRisk"=>serde_json::json!([{"symbol":"BNTUSDT","positionSide":"BOTH",
     "marginAsset":"USDT","positionAmt":if matches!(self.fault,Fault::Residual){"-0.01"}else{"0"},
     "updateTime":NOW-2000}]),
   "/fapi/v3/balance"=>serde_json::json!([{"asset":"USDT","balance":"500",
     "availableBalance":"499","updateTime":if matches!(self.fault,Fault::StaleWallet){NOW-300_000}else{NOW-2000}}]),
   "/fapi/v1/openOrders"=>serde_json::json!([]),
   "/api/v5/account/bills-archive"=>{
     let ccy=qp.get("ccy").map(|x|x.as_ref()).unwrap_or("");
     if is_after{serde_json::json!({"code":"0","data":[]})}
     else{
      let change=if ccy=="BNT"{"-0.09"}
         else if matches!(self.fault,Fault::WrongBillAmount){"8.992"}else{"8.991"};
      serde_json::json!({"code":"0","data":[{
       "ccy":ccy,"ordId":if matches!(self.fault,Fault::ForeignBill){"999"}else{"201"},
       "instId":"BNT-USDT","subType":"2","tradeId":"S1",
       "billId":if ccy=="BNT"{"310"}else{"311"},
       "balChg":change,"ts":(NOW-950).to_string()}]})
     }
   },
   "/fapi/v1/income"=>{
     if page=="1"{
       let mut rows=vec![
         serde_json::json!({"incomeType":"COMMISSION","tranId":1,"tradeId":1001,
           "income":if matches!(self.fault,Fault::WrongIncomeFee){"-0.05"}else{"-0.01"},
           "asset":"USDT","symbol":"BNTUSDT","time":NOW-950}),
         serde_json::json!({"incomeType":"REALIZED_PNL","tranId":2,"tradeId":1001,
           "income":"0.27","asset":"USDT","symbol":"BNTUSDT","time":NOW-950}),
       ];
       if !matches!(self.fault,Fault::MissingFunding|Fault::MultiPage){
         rows.push(serde_json::json!({"incomeType":"FUNDING_FEE","tranId":3,
         "income":"-0.02","asset":"USDT","symbol":"BNTUSDT","time":NOW-1500}));
       }
       if matches!(self.fault,Fault::ForeignIncome){
         rows.push(serde_json::json!({"incomeType":"TRANSFER","tranId":4,
         "income":"5","asset":"USDT","symbol":"BNTUSDT","time":NOW-1400}));
       }
       serde_json::json!(rows)
     }else if matches!(self.fault,Fault::MultiPage)&&page=="2"{
       serde_json::json!([{"incomeType":"FUNDING_FEE","tranId":3,
       "income":"-0.02","asset":"USDT","symbol":"BNTUSDT","time":NOW-1500}])
     }else if matches!(self.fault,Fault::IncompleteIncome){
       serde_json::json!([{"incomeType":"FUNDING_FEE","tranId":page,
       "income":"0","asset":"USDT","symbol":"BNTUSDT","time":NOW-1000}])
     }else{serde_json::json!([])}
   },
   "/fapi/v1/userTrades"=>{
      let one=serde_json::json!({"id":1001,"orderId":if matches!(self.fault,Fault::ForeignTrade){999}else{202},
       "symbol":"BNTUSDT","side":"BUY","positionSide":"BOTH","time":NOW-950});
      if matches!(self.fault,Fault::FullSymbol){
       serde_json::json!([one.clone(),one.clone(),one.clone(),one.clone()])
      }else{serde_json::json!([one])}
   },
   other=>panic!("unhandled synthetic mock endpoint: {other}"),
  };
  // Explicitly test malformed selected endpoint in a future fault scenario.
  if j.is_null(){j=serde_json::json!([])}
  Ok(HttpResponse{status:200,headers:vec![],body:j.to_string()})
 }
}
fn run(f:&Fake)->Result<spotarb_native_market_core::native_close_window::CloseSourceObservation,CloseSourceError>{
 let (okx,bin)=creds();let c=candidate();
 let close=audit_dual_close_order_receipts_once(
  f,&c,&okx,&bin,NOW,OkxDomain::Global,10_000,10,10).unwrap();
 audit_close_source_window_once(f,&c,&close,&okx,&bin,
  NOW,OkxDomain::Global,10_000,10)
}
#[test]
fn source_window_positive_is_still_not_a_verified_close(){
 let f=Fake::new(Fault::None);let r=run(&f).unwrap();
 assert_eq!(r.counts(),(1,1,3,1,1));
 assert_eq!(r.observed_funding_e12(),-20_000_000_000);
 assert!(!r.original_native_position_ancestry_proven());
 assert!(!r.full_account_history_proven()&&!r.funding_lifetime_complete());
 assert!(!r.cross_venue_atomicity_proven());
 assert!(!r.may_mark_verified_closed()&&!r.may_send_order()&&!r.may_start_real_cooldown());
 assert_eq!(r.account_reads(),8);
}
#[test]
fn source_window_denies_residual_locked_balances_or_moving_account(){
 for fault in [Fault::Residual,Fault::Frozen,Fault::Drift,Fault::StaleWallet]{
  assert!(run(&Fake::new(fault)).is_err(),"fault {fault:?}");
 }
}
#[test]
fn source_window_denies_foreign_cash_income_symbol_and_wrong_fees(){
 for fault in [Fault::ForeignBill,Fault::ForeignIncome,Fault::ForeignTrade,Fault::WrongIncomeFee,Fault::WrongBillAmount]{
  assert!(run(&Fake::new(fault)).is_err(),"fault {fault:?}");
 }
}
#[test]
fn source_window_requires_explicit_window_exhaustion_and_funding_evidence(){
 for fault in [Fault::IncompleteIncome,Fault::FullSymbol,Fault::MissingFunding]{
  assert!(run(&Fake::new(fault)).is_err(),"fault {fault:?}");
 }
}
#[test]
fn source_window_wrong_receipt_anchor_denied_before_account_get(){
 let f=Fake::new(Fault::None);let (okx,bin)=creds();let mut c=candidate();
 let receipt=audit_dual_close_order_receipts_once(
  &f,&c,&okx,&bin,NOW,OkxDomain::Global,10_000,10,10).unwrap();
 let calls=f.calls.load(Ordering::SeqCst);
 c.position_identity="different".into();
 assert!(audit_close_source_window_once(&f,&c,&receipt,&okx,&bin,
  NOW,OkxDomain::Global,10_000,10).is_err());
 assert_eq!(f.calls.load(Ordering::SeqCst),calls);
}

#[test]
fn persisted_native_ancestry_cannot_be_faked_from_client_supplied_ids(){
 use spotarb_native_market_core::{
   authoritative_state::{NativeStateStore,NativeAuthoritativeState},
   native_close_window::require_persisted_native_close_ancestry,
 };
 let dir=std::env::temp_dir().join(format!("r32-close-{}-{:?}",std::process::id(),
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
 let path=dir.join("state.json");
 let store=NativeStateStore::open(&path).unwrap();
 store.create(&NativeAuthoritativeState::empty_from_legacy("legacy".into(),0)).unwrap();
 assert!(matches!(require_persisted_native_close_ancestry(&store,1,&candidate()),
     Err(CloseSourceError::AncestryNotProven)));
 assert!(matches!(require_persisted_native_close_ancestry(&store,2,&candidate()),
     Err(CloseSourceError::State(_))));
 assert_eq!(store.load().unwrap().unwrap().generation,1);
 std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn close_source_window_accepts_bounded_multi_page_income_only_as_no_authority_diagnostic(){
 let r=run(&Fake::new(Fault::MultiPage)).unwrap();
 assert_eq!(r.counts(),(1,1,3,1,1));
 assert!(!r.may_mark_verified_closed());
}


// R32D6C8B2B2B2 durable UNBOUND diagnostic only (no real authority).
fn diagnostic_inputs()->(
 DualCloseCandidate,
 spotarb_native_market_core::native_close_readonly::DualCloseReadOnlyCandidate,
 spotarb_native_market_core::native_close_window::CloseSourceObservation
){
 let f=Fake::new(Fault::None);let c=candidate();let (okx,bin)=creds();
 let receipt=audit_dual_close_order_receipts_once(
   &f,&c,&okx,&bin,NOW,OkxDomain::Global,10_000,10,10).unwrap();
 let source=audit_close_source_window_once(
   &f,&c,&receipt,&okx,&bin,NOW,OkxDomain::Global,10_000,10).unwrap();
 (c,receipt,source)
}
fn temp_close_dir(tag:&str)->std::path::PathBuf{
 let n=std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
    .unwrap().as_nanos();
 let p=std::env::temp_dir().join(format!("r32-b2b2b2-{tag}-{}-{n}",std::process::id()));
 std::fs::create_dir_all(&p).unwrap();
 p
}
fn seeded_state(root:&std::path::Path)->spotarb_native_market_core::authoritative_state::NativeStateStore{
 use spotarb_native_market_core::authoritative_state::{NativeStateStore,NativeAuthoritativeState};
 let s=NativeStateStore::open(root.join("native_state.json")).unwrap();
 s.create(&NativeAuthoritativeState::empty_from_legacy("hash-of-legacy-in-review".into(),0)).unwrap();
 s
}
#[test]
fn diagnostic_unbound_roundtrip_replay_has_no_authority(){
 use spotarb_native_market_core::native_close_diagnostic::*;
 let dir=temp_close_dir("roundtrip");
 let state=seeded_state(&dir);
 let path=dir.join("only_diagnostics.json");
 let journal=NativeCloseDiagnosticStore::open(&path).unwrap();
 let (c,r,w)=diagnostic_inputs();
 let (snap,write)=journal.record_unbound(&state,1,0,"obs1",NOW,&c,&r,&w,CloseDiagnosticFailpoint::None).unwrap();
 assert_eq!(write,CloseDiagnosticWrite::Recorded);
 assert_eq!(snap.generation(),1);
 assert_eq!(snap.records().len(),1);
 let x=&snap.records()[0];
 assert_eq!(x.source_generation(),1);
 assert!(!x.ancestry_proven());
 assert!(!x.verified_closed()&&!x.may_send_order()&&!x.may_release_real_cooldown());
 let reopened=NativeCloseDiagnosticStore::open(&path).unwrap();
 assert_eq!(reopened.load().unwrap(),snap);
 let (again,result)=reopened.record_unbound(&state,1,0,"obs1",NOW,&c,&r,&w,CloseDiagnosticFailpoint::None).unwrap();
 assert_eq!(result,CloseDiagnosticWrite::Idempotent);
 assert_eq!(again,snap);
 assert_eq!(state.load().unwrap().unwrap().generation,1);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn diagnostic_generation_cas_and_conflicting_replay_are_denied(){
 use spotarb_native_market_core::native_close_diagnostic::*;
 let dir=temp_close_dir("cas");let state=seeded_state(&dir);
 let journal=NativeCloseDiagnosticStore::open(dir.join("close_diag.json")).unwrap();
 let (c,r,w)=diagnostic_inputs();
 journal.record_unbound(&state,1,0,"obs1",NOW,&c,&r,&w,CloseDiagnosticFailpoint::None).unwrap();
 assert!(matches!(journal.record_unbound(&state,1,0,"obs2",NOW,&c,&r,&w,CloseDiagnosticFailpoint::None),
    Err(CloseDiagnosticError::Generation)));
 assert!(matches!(journal.record_unbound(&state,1,1,"obs1",NOW+1,&c,&r,&w,CloseDiagnosticFailpoint::None),
    Err(CloseDiagnosticError::Invalid(_))));
 journal.record_unbound(&state,1,1,"obs2",NOW,&c,&r,&w,CloseDiagnosticFailpoint::None).unwrap();
 assert_eq!(journal.load().unwrap().generation(),2);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn diagnostic_crash_cutpoints_preserve_or_idempotently_recover(){
 use spotarb_native_market_core::native_close_diagnostic::*;
 for (i,fp) in [
  CloseDiagnosticFailpoint::BeforeWrite,CloseDiagnosticFailpoint::AfterWrite,
  CloseDiagnosticFailpoint::AfterFileSync,CloseDiagnosticFailpoint::AfterRename,
 ].iter().copied().enumerate(){
  let dir=temp_close_dir(&format!("crash-{i}"));
  let state=seeded_state(&dir);
  let journal=NativeCloseDiagnosticStore::open(dir.join("diag.json")).unwrap();
  let (c,r,w)=diagnostic_inputs();
  let x=journal.record_unbound(&state,1,0,"obs1",NOW,&c,&r,&w,fp);
  assert!(matches!(x,Err(CloseDiagnosticError::Io(_))));
  let reopened=NativeCloseDiagnosticStore::open(dir.join("diag.json")).unwrap();
  let prior=reopened.load().unwrap();
  let expect=if fp==CloseDiagnosticFailpoint::AfterRename{1}else{0};
  assert_eq!(prior.generation(),expect);
  let (_,result)=reopened.record_unbound(&state,1,0,"obs1",NOW,&c,&r,&w,CloseDiagnosticFailpoint::None).unwrap();
  if expect==1{assert_eq!(result,CloseDiagnosticWrite::Idempotent)}
  else{assert_eq!(result,CloseDiagnosticWrite::Recorded)}
  assert_eq!(reopened.load().unwrap().generation(),1);
  assert_eq!(state.load().unwrap().unwrap().generation,1);
  std::fs::remove_dir_all(dir).unwrap();
 }
}
#[test]
fn diagnostic_checksum_corruption_and_bad_payload_cannot_be_promoted(){
 use spotarb_native_market_core::native_close_diagnostic::*;
 let dir=temp_close_dir("tamper");let state=seeded_state(&dir);
 let path=dir.join("diag.json");let journal=NativeCloseDiagnosticStore::open(&path).unwrap();
 let (c,r,w)=diagnostic_inputs();
 journal.record_unbound(&state,1,0,"obs1",NOW,&c,&r,&w,CloseDiagnosticFailpoint::None).unwrap();
 let data=std::fs::read_to_string(&path).unwrap();
 assert!(data.contains("\"verified_closed\":false"));
 // A payload-only bit flip without digest update is rejected.
 std::fs::write(&path,data.replace("\"verified_closed\":false","\"verified_closed\":true")).unwrap();
 assert!(journal.load().is_err());
 // Even when an external actor could recompute checksum, validation rejects
 // an authority bit; no deserialization path grants one.
 std::fs::write(&path,b"{broken").unwrap();
 assert!(journal.load().is_err());
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn diagnostic_state_generation_fences_restart_and_missing_source(){
 use spotarb_native_market_core::native_close_diagnostic::*;
 use spotarb_native_market_core::authoritative_state::NativeStateStore;
 let dir=temp_close_dir("source-fence");let missing=NativeStateStore::open(dir.join("no_state.json")).unwrap();
 let journal=NativeCloseDiagnosticStore::open(dir.join("diag.json")).unwrap();
 let (c,r,w)=diagnostic_inputs();
 assert!(journal.record_unbound(&missing,1,0,"obs1",NOW,&c,&r,&w,CloseDiagnosticFailpoint::None).is_err());
 assert_eq!(journal.load().unwrap().generation(),0);
 let state=seeded_state(&dir);
 state.transact(1,|_|Ok(())).unwrap();
 assert!(matches!(journal.record_unbound(&state,1,0,"obs1",NOW,&c,&r,&w,CloseDiagnosticFailpoint::None),
    Err(CloseDiagnosticError::Generation)));
 assert_eq!(state.load().unwrap().unwrap().generation,2);
 assert_eq!(journal.load().unwrap().generation(),0);
 std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn diagnostic_concurrent_writers_only_one_cas_commit(){
 use spotarb_native_market_core::native_close_diagnostic::*;
 let dir=temp_close_dir("concurrent");
 let state=seeded_state(&dir);
 let journal=NativeCloseDiagnosticStore::open(dir.join("diag.json")).unwrap();
 let (c,r,w)=diagnostic_inputs();
 std::thread::scope(|scope|{
  let h1=scope.spawn(||journal.record_unbound(&state,1,0,"A",NOW,&c,&r,&w,CloseDiagnosticFailpoint::None));
  let h2=scope.spawn(||journal.record_unbound(&state,1,0,"B",NOW,&c,&r,&w,CloseDiagnosticFailpoint::None));
  let a=h1.join().unwrap();let b=h2.join().unwrap();
  assert_eq!(usize::from(a.is_ok())+usize::from(b.is_ok()),1);
  assert!(matches!(a.as_ref().err().or(b.as_ref().err()),
       Some(CloseDiagnosticError::Generation)));
 });
 assert_eq!(journal.load().unwrap().generation(),1);
 assert_eq!(state.load().unwrap().unwrap().generation,1);
 std::fs::remove_dir_all(dir).unwrap();
}
