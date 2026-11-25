//! TRADE2F3 Stage B1: source CAPTURE INTEGRITY CONTRACT, NO source authentication.
//!
//! This module makes an in-memory SHA256 transcript of the EXISTING F2G/H/I
//! mock GET tapes. It never performs a REST request, reads secrets, claims a
//! TLS peer or creates a real exchange credential. Its hash chain is NOT an
//! external append-only checkpoint: an attacker controlling both tape and
//! manifest can trivially re-hash both. Do not use it for real trading.
use openssl::sha::sha256;
use serde::Serialize;
use std::collections::BTreeSet;
use crate::{
 authoritative_state::NativeStateStore,
 native_trade2f_okx_owner_mock::MockGetStep,
 native_trade2f3_unified_offline_gate::{
   inspect_original_owner_unified_offline,UnifiedOfflineError,UnifiedOfflineReport},
 private_rest::{OkxDomain,parse_binance_fill_events_http},
 fill_economics::{LegFillLedger,signed_decimal_to_atoms},
};
const LIMIT_PER_LANE:usize=64;
const LIMIT_BODY:usize=1_048_576;
const ZERO:&str="0000000000000000000000000000000000000000000000000000000000000000";
#[derive(Clone,Copy,Debug,PartialEq,Eq,Serialize)]
pub enum CaptureLane{OkxOriginal,OkxDelayed,BinanceOriginalAndDelayed}
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum CaptureError{
 Denied(&'static str),Encoding,Proof(UnifiedOfflineError),
}
fn deny(s:&'static str)->CaptureError{CaptureError::Denied(s)}
fn hex(v:&[u8])->String{
 let mut s=String::with_capacity(v.len()*2);
 for b in v{s.push_str(&format!("{b:02x}"))}s
}
fn hash(b:&[u8])->String{hex(&sha256(b))}
/// No response or signed query values are emitted in this manifest.
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct CaptureRecord{
 seq:u64,lane:CaptureLane,endpoint:String,
 request_sha256:String,response_sha256:String,
 prior_sha256:String,chain_sha256:String,
}
impl CaptureRecord{
 pub fn seq(&self)->u64{self.seq}
 pub fn lane(&self)->CaptureLane{self.lane}
 pub fn endpoint(&self)->&str{&self.endpoint}
 pub fn chain_sha256(&self)->&str{&self.chain_sha256}
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct OfflineCaptureManifest{
 owner_generation:u64,first_end:u64,late_end:u64,
 okx_base_url:String,records:Vec<CaptureRecord>,head_sha256:String,
}
impl OfflineCaptureManifest{
 pub fn head_sha256(&self)->&str{&self.head_sha256}
 pub fn count(&self)->usize{self.records.len()}
 pub fn records(&self)->&[CaptureRecord]{&self.records}
 pub fn owner_generation(&self)->u64{self.owner_generation}
 /// A local hash, even with correct fixture bytes, is NOT provenance.
 pub fn independent_exchange_authentication_proven(&self)->bool{false}
 pub fn independently_anchored_antirollback_proven(&self)->bool{false}
 pub fn can_mark_verified_closed_or_authorize_trading(&self)->bool{false}
}
fn params_value<'a>(
 fields:&'a std::collections::BTreeMap<String,String>,key:&str,
)->Result<&'a str,CaptureError>{
 fields.get(key).map(String::as_str).ok_or_else(||deny("missing required exchange query field"))
}
fn canonical_okx_history_id(s:&str)->bool{
 !s.starts_with('0')&&!s.is_empty()
    &&s.bytes().all(|b|b.is_ascii_digit())
    &&s.parse::<u128>().is_ok_and(|v|v>0)
}
fn vetted_path(lane:CaptureLane,path_query:&str)->Result<String,CaptureError>{
 if !path_query.starts_with('/')||path_query.len()>2048
    ||path_query.contains(['\r','\n','#'])||path_query.starts_with("//"){
    return Err(deny("not a bounded relative read-only exchange path"))}
 let parsed=url::Url::parse(&format!("https://review.invalid{path_query}"))
    .map_err(|_|deny("unparseable exchange request in capture"))?;
 if parsed.host_str()!=Some("review.invalid")
     ||parsed.query().is_none()||parsed.fragment().is_some(){
    return Err(deny("exchange request path authority mismatch"))}
 let endpoint=parsed.path();
 if path_query.split('?').next()!=Some(endpoint){
    return Err(deny("noncanonical or rewritten capture endpoint"))}
 let expected=match lane{
   CaptureLane::OkxOriginal=>
     endpoint=="/api/v5/trade/fills-history"||endpoint=="/api/v5/account/bills-archive",
   CaptureLane::OkxDelayed=>endpoint=="/api/v5/account/bills-archive",
   CaptureLane::BinanceOriginalAndDelayed=>
     endpoint=="/fapi/v1/income"||endpoint=="/fapi/v1/userTrades",
 };
 if !expected{return Err(deny("forbidden endpoint in source transcript"))}
 let expected_keys:&[&str]=match endpoint{
  "/api/v5/trade/fills-history"=>
      &["instType","instId","ordId","limit","after"],
  "/api/v5/account/bills-archive"=>
      &["ccy","begin","end","limit","after"],
  "/fapi/v1/income"=>
      &["startTime","endTime","page","limit","timestamp","recvWindow","signature"],
  "/fapi/v1/userTrades"=>
      &["symbol","orderId","fromId","limit","timestamp","recvWindow","signature"],
  _=>return Err(deny("non-history path"))};
 let mut keys=BTreeSet::new();
 let mut fields=std::collections::BTreeMap::new();
 for (k,v) in parsed.query_pairs(){
    if !expected_keys.contains(&k.as_ref())||v.is_empty()
       ||!keys.insert(k.to_string()){
       return Err(deny("unrecognized, empty or duplicated history query field"))}
    fields.insert(k.to_string(),v.to_string());
 }
 if !keys.contains("limit"){
   return Err(deny("history query limit omitted"))}
 // B1 records must honor the existing B2 inert per-source page plan,
 // not merely hash arbitrarily large caller-supplied request limits.
 let numeric=|key:&str|fields.get(key).and_then(|value|value.parse::<u64>().ok());
 let max_size=if endpoint.starts_with("/fapi/"){1000}else{100};
 if numeric("limit").map_or(true,|n|n==0||n>max_size){
   return Err(deny("source page size exceeds B2 source slot"))
 }
 if endpoint=="/fapi/v1/income"{
   for k in ["startTime","endTime","page","timestamp","recvWindow","signature"]{
      if !keys.contains(k){return Err(deny("incomplete Binance signed income query"))}
   }
   let begin=numeric("startTime");
   let end=numeric("endTime");
   if begin.map_or(true,|n|n==0)
      ||end.map_or(true,|n|Some(n)<begin)
      ||numeric("page").map_or(true,|n|n==0||n>4)
      ||numeric("timestamp").map_or(true,|n|n==0)
      ||numeric("recvWindow").map_or(true,|n|n==0||n>60000){
      return Err(deny("unbounded Binance income page or clock scope"))
   }
   let sig=&fields["signature"];
   if sig.len()!=64||!sig.bytes().all(|b|b.is_ascii_hexdigit()){
      return Err(deny("invalid signed Binance income query shape"))
   }
 }else if endpoint=="/fapi/v1/userTrades"{
   // Only owner-scoped Binance Futures fills from the existing B2 design:
   // orderId + symbol + bounded fromId pagination. These structural checks
   // are NOT exchange authentication, a valid HMAC, or permission to GET.
   for k in ["symbol","orderId","fromId","timestamp","recvWindow","signature"]{
      if !keys.contains(k){return Err(deny("incomplete Binance original fill query"))}
   }
   let num=|key:&str|fields.get(key).and_then(|s|s.parse::<u64>().ok());
   let id=fields.get("orderId").ok_or_else(||deny("missing order ID"))?;
   if id.starts_with('0')||num("orderId").map_or(true,|n|n==0)
      ||num("fromId").is_none()||num("limit").map_or(true,|n|n==0||n>1000)
      ||num("timestamp").map_or(true,|n|n==0)
      ||num("recvWindow").map_or(true,|n|n==0||n>60000){
      return Err(deny("noncanonical Binance fill ID, page or time bound"))
   }
   let symbol=&fields["symbol"];
   if !symbol.ends_with("USDT")||symbol.len()<5||symbol.len()>30
      ||!symbol.bytes().all(|b|b.is_ascii_uppercase()||b.is_ascii_digit()){
      return Err(deny("invalid Binance futures symbol"))
   }
   let sig=&fields["signature"];
   if sig.len()!=64||!sig.bytes().all(|b|b.is_ascii_hexdigit()){
      return Err(deny("invalid Binance signed query shape"))
   }
 }else if endpoint=="/api/v5/account/bills-archive"{
   for k in ["ccy","begin","end"]{
      if !keys.contains(k){return Err(deny("incomplete OKX cash bill query"))}
   }
   let start=numeric("begin");
   let end=numeric("end");
   if start.map_or(true,|n|n==0)||end.map_or(true,|n|Some(n)<start){
      return Err(deny("invalid OKX bounded bill time window"))
   }
 }else{
   for k in ["instType","instId","ordId"]{
      if !keys.contains(k){return Err(deny("incomplete OKX execution query"))}
   }
   // B1 cannot call a futures/margin instrument a valid OKX Spot source.
   // Exact original symbol/order ownership is checked again by existing F2G.
   let inst=params_value(&fields,"instType")?;
   let symbol=params_value(&fields,"instId")?;
   let order=params_value(&fields,"ordId")?;
   let base=symbol.strip_suffix("-USDT").unwrap_or("");
   if inst!="SPOT"||base.len()<2||base.len()>25
      ||!base.bytes().all(|b|b.is_ascii_uppercase()||b.is_ascii_digit())
      ||!canonical_okx_history_id(order){
      return Err(deny("noncanonical OKX spot fill instrument or order"))
   }
   if fields.get("after").is_some_and(|cursor|!canonical_okx_history_id(cursor)){
      return Err(deny("invalid OKX execution page cursor"))
   }
 }
 Ok(endpoint.to_owned())
}
fn collect(
 out:&mut Vec<CaptureRecord>,lane:CaptureLane,tape:&[MockGetStep],
 owner:u64,first:u64,late:u64,domain:&str,
)->Result<(),CaptureError>{
 if tape.is_empty()||tape.len()>LIMIT_PER_LANE{
     return Err(deny("source capture lane missing or pagination budget oversized"))}
 for item in tape{
   let endpoint=vetted_path(lane,&item.path_and_query)?;
   if item.http_status!=200||item.body.is_empty()
     ||item.body.len()>LIMIT_BODY{
      return Err(deny("untrusted source capture HTTP status or body size"))}
   let seq=(out.len()+1)as u64;
   let prior=out.last().map_or(ZERO,|v|v.chain_sha256.as_str()).to_owned();
   let req=hash(item.path_and_query.as_bytes());
   let response=hash(item.body.as_bytes());
   // Explicit domain separators, generation, windows, venue and exact
   // request/response digests. No copied bodies/secrets in emitted manifest.
   let serialized=serde_json::to_vec(&(
      "spotarb:review:source-capture:stageB1:v1",seq,lane,
      endpoint.as_str(),req.as_str(),response.as_str(),
      prior.as_str(),owner,first,late,domain
   )).map_err(|_|CaptureError::Encoding)?;
   let chain=hash(&serialized);
   out.push(CaptureRecord{
      seq,lane,endpoint,request_sha256:req,response_sha256:response,
      prior_sha256:prior,chain_sha256:chain,
   });
 }
 Ok(())
}
/// Reuses original F2G/H/I source tapes without parsing one financial row.
/// This is NOT proof the bytes were obtained from the actual exchange.
/// Any separately supplied checkpoint is untrusted until independently
/// authenticated, durable and anti-rollback-anchored outside this process.
pub fn build_offline_capture_manifest(
 generation:u64,first_end:u64,late_end:u64,domain:OkxDomain,
 okx_initial:&[MockGetStep],okx_late:&[MockGetStep],
 binance:&[MockGetStep],
)->Result<OfflineCaptureManifest,CaptureError>{
 if generation==0||first_end==0||late_end<first_end
    ||late_end-first_end<60_000||late_end-first_end>86_400_000{
    return Err(deny("invalid Native generation or two-window capture time scope"))}
 let mut records=Vec::new();
 let name=domain.base_url();
 collect(&mut records,CaptureLane::OkxOriginal,okx_initial,
        generation,first_end,late_end,name)?;
 collect(&mut records,CaptureLane::OkxDelayed,okx_late,
        generation,first_end,late_end,name)?;
 collect(&mut records,CaptureLane::BinanceOriginalAndDelayed,binance,
        generation,first_end,late_end,name)?;
 let head=records.last().ok_or(deny("empty three-lane manifest"))?.chain_sha256.clone();
 Ok(OfflineCaptureManifest{owner_generation:generation,
   first_end,late_end,okx_base_url:name.to_owned(),records,head_sha256:head})
}
/// Recompute from the *raw* inputs before invoking G/H/I. Detects mismatch
/// relative to supplied manifest only; cannot detect re-authored tape+manifest.
pub fn verify_offline_capture_manifest(
 expected:&OfflineCaptureManifest,
 generation:u64,first_end:u64,late_end:u64,domain:OkxDomain,
 okx_initial:&[MockGetStep],okx_late:&[MockGetStep],
 binance:&[MockGetStep],
)->Result<(),CaptureError>{
 let actual=build_offline_capture_manifest(
    generation,first_end,late_end,domain,okx_initial,okx_late,binance)?;
 if &actual!=expected{return Err(deny("capture SHA256 chain, source bytes or original owner scope drift"))}
 Ok(())
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct OfflineProvenanceReview{
 pub unified:UnifiedOfflineReport,
 pub manifest_head_sha256:String,
 pub local_transcript_bytes_stable:bool,
 pub independently_authenticated_exchange_source:bool,
 pub externally_anchored_antirollback:bool,
 pub may_trade_cancel_transfer_or_mark_closed:bool,
}
/// Combined diagnostic: first match manifest to exact bytes, then delegate
/// all financial semantics to existing stage-A unified H+I owner chain.
pub fn inspect_unified_with_offline_capture(
 store:&NativeStateStore,generation:u64,first_end:u64,late_end:u64,
 domain:OkxDomain,manifest:&OfflineCaptureManifest,
 okx_initial:&[MockGetStep],okx_late:&[MockGetStep],
 binance:&[MockGetStep],
)->Result<OfflineProvenanceReview,CaptureError>{
 verify_offline_capture_manifest(manifest,generation,first_end,late_end,
      domain,okx_initial,okx_late,binance)?;
 let unified=inspect_original_owner_unified_offline(
    store,generation,first_end,late_end,domain,
    okx_initial,okx_late,binance).map_err(CaptureError::Proof)?;
 Ok(OfflineProvenanceReview{
   unified,manifest_head_sha256:manifest.head_sha256().to_owned(),
   local_transcript_bytes_stable:true,
   independently_authenticated_exchange_source:false,
   externally_anchored_antirollback:false,
   may_trade_cancel_transfer_or_mark_closed:false,
 })
}

/// P1-01: one local archive page can be compared to an existing B1
/// byte-transcript record ONLY by exact signed path-query and original body
/// SHA256 plus endpoint. This cannot authenticate an exchange or certify owner.
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct LocalArchivePageFingerprint{
 pub endpoint:String,
 pub signed_path_query_sha256:String,
 pub original_response_sha256:String,
}
impl LocalArchivePageFingerprint{
 /// A receipt is untrusted until the archive has been independently reread
 /// and SHA-checked by the source sampler; never upgrades authentication.
 pub fn from_untrusted_archive_receipt(page:&serde_json::Value)->Result<Self,CaptureError>{
  let field=|k:&str|page.get(k).and_then(|v|v.as_str())
     .filter(|s|!s.is_empty()).ok_or_else(||deny("archive receipt field missing"));
  let value=Self{
   endpoint:field("endpoint")?.into(),
   signed_path_query_sha256:field("signed_path_query_sha256")?.into(),
   original_response_sha256:field("sha256")?.into(),
  };
  if !matches!(value.endpoint.as_str(),
     "/api/v5/trade/fills-history"|"/api/v5/account/bills-archive"|
     "/fapi/v1/income"|"/fapi/v1/userTrades")
    ||[&value.signed_path_query_sha256,&value.original_response_sha256]
      .iter().any(|s|s.len()!=64||!s.bytes().all(|b|b.is_ascii_hexdigit())){
   return Err(deny("invalid archive page source fingerprint"))
  }
  Ok(value)
 }
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct LocalArchiveToB1ByteParity{
 pub pages_matched:usize,
 pub captured_b1_pages:usize,
 pub original_exchange_authenticated:bool,
 pub account_history_complete:bool,
 pub original_owner_proven:bool,
 pub may_trade_or_mark_closed:bool,
}
pub fn match_scoped_archive_pages_to_existing_b1_bytes_without_network(
 captured:&OfflineCaptureManifest,samples:&[LocalArchivePageFingerprint],
)->Result<LocalArchiveToB1ByteParity,CaptureError>{
 if samples.is_empty()||samples.len()>64{
  return Err(deny("empty or excessive local archive samples"))
 }
 let hex_ok=|s:&str|s.len()==64&&s.bytes().all(|b|b.is_ascii_hexdigit());
 let mut matched=std::collections::BTreeSet::<usize>::new();
 for sample in samples{
  if !matches!(sample.endpoint.as_str(),
     "/api/v5/trade/fills-history"|"/api/v5/account/bills-archive"|
     "/fapi/v1/income"|"/fapi/v1/userTrades")
    ||!hex_ok(&sample.signed_path_query_sha256)
    ||!hex_ok(&sample.original_response_sha256){
     return Err(deny("local archive source fingerprint invalid"))
  }
  let index=captured.records.iter().position(|record|
       record.endpoint==sample.endpoint
       &&record.request_sha256==sample.signed_path_query_sha256
       &&record.response_sha256==sample.original_response_sha256)
     .ok_or_else(||deny("raw archive bytes do not match preexisting B1 source capture"))?;
  if !matched.insert(index){
     return Err(deny("local archive mapped multiple times to one B1 page"))
  }
 }
 Ok(LocalArchiveToB1ByteParity{
   pages_matched:matched.len(),captured_b1_pages:captured.records.len(),
   original_exchange_authenticated:false,account_history_complete:false,
   original_owner_proven:false,may_trade_or_mark_closed:false,
 })
}
#[cfg(test)]
mod scoped_archive_b1_byte_link_tests{
 use super::*;
 fn fixtures()->(OfflineCaptureManifest,LocalArchivePageFingerprint){
   let okx=MockGetStep{
     path_and_query:"/api/v5/trade/fills-history?instType=SPOT&instId=BNT-USDT&ordId=201&limit=100".into(),
     http_status:200,body:r#"{"code":"0","data":[]}"#.into()};
   let late=MockGetStep{
     path_and_query:"/api/v5/account/bills-archive?ccy=USDT&begin=1&end=2&limit=100".into(),
     http_status:200,body:r#"{"code":"0","data":[]}"#.into()};
   let income=MockGetStep{
     path_and_query:format!("/fapi/v1/income?startTime=10&endTime=20&page=1&limit=1000&timestamp=21&recvWindow=10000&signature={}", "a".repeat(64)),
     http_status:200,body:"[]".into()};
   let m=build_offline_capture_manifest(7,1791880000000,1791880060000,
       OkxDomain::Global,&[okx],&[late],&[income.clone()]).unwrap();
   let fingerprint=LocalArchivePageFingerprint{
     endpoint:"/fapi/v1/income".into(),
     signed_path_query_sha256:hash(income.path_and_query.as_bytes()),
     original_response_sha256:hash(income.body.as_bytes())};
   (m,fingerprint)
 }
 #[test]
 fn p101_archive_exact_request_and_original_body_match_is_local_only(){
  let (m,page)=fixtures();
  let report=match_scoped_archive_pages_to_existing_b1_bytes_without_network(
     &m,&[page]).unwrap();
  assert_eq!(report.pages_matched,1);
  assert_eq!(report.captured_b1_pages,3);
  assert!(!report.original_exchange_authenticated);
  assert!(!report.account_history_complete);
  assert!(!report.original_owner_proven);
  assert!(!report.may_trade_or_mark_closed);
 }
 #[test]
 fn p101_archive_digest_endpoint_repetition_and_invalid_shape_fail_closed(){
  let (m,good)=fixtures();
  let mut request_changed=good.clone();
  request_changed.signed_path_query_sha256="b".repeat(64);
  assert!(match_scoped_archive_pages_to_existing_b1_bytes_without_network(
    &m,&[request_changed]).is_err());
  let mut changed=good.clone();
  changed.original_response_sha256="b".repeat(64);
  assert!(match_scoped_archive_pages_to_existing_b1_bytes_without_network(&m,&[changed]).is_err());
  let mut changed=good.clone();
  changed.endpoint="/fapi/v1/userTrades".into();
  assert!(match_scoped_archive_pages_to_existing_b1_bytes_without_network(&m,&[changed]).is_err());
  assert!(match_scoped_archive_pages_to_existing_b1_bytes_without_network(
      &m,&[good.clone(),good.clone()]).is_err());
  assert!(match_scoped_archive_pages_to_existing_b1_bytes_without_network(&m,&[]).is_err());
  let mut bad=good;
  bad.signed_path_query_sha256="short".into();
  assert!(match_scoped_archive_pages_to_existing_b1_bytes_without_network(&m,&[bad]).is_err());
 }
 #[test]
 fn p101_archive_json_fingerprint_is_untrusted_and_requires_exact_fields(){
   let (manifest,page)=fixtures();
   let receipt=serde_json::json!({
      "endpoint":page.endpoint,
      "signed_path_query_sha256":page.signed_path_query_sha256,
      "sha256":page.original_response_sha256,
   });
   let parsed=LocalArchivePageFingerprint::from_untrusted_archive_receipt(&receipt).unwrap();
   let linked=match_scoped_archive_pages_to_existing_b1_bytes_without_network(
     &manifest,&[parsed]).unwrap();
   assert_eq!(linked.pages_matched,1);
   assert!(!linked.original_owner_proven&&!linked.original_exchange_authenticated);
   let mut no_signature=receipt.clone();
   no_signature.as_object_mut().unwrap().remove("signed_path_query_sha256");
   assert!(LocalArchivePageFingerprint::from_untrusted_archive_receipt(&no_signature).is_err());
   let mut invalid_endpoint=receipt;
   invalid_endpoint["endpoint"]=serde_json::json!("/fapi/v1/order");
   assert!(LocalArchivePageFingerprint::from_untrusted_archive_receipt(&invalid_endpoint).is_err());
 }
}
#[cfg(test)]
mod b1_usertrades_capture_tests {
 use super::*;
 fn signed_usertrades(order_id:&str,from_id:&str)->String{
  format!("/fapi/v1/userTrades?symbol=BNTUSDT&orderId={order_id}&fromId={from_id}&limit=1000&timestamp=1791880000000&recvWindow=10000&signature={}", "a".repeat(64))
 }
 #[test]
 fn p101_b2_original_futures_fills_are_accepted_by_b1_manifest_without_authority(){
  let get=|path:String|MockGetStep{path_and_query:path,http_status:200,body:"[]".into()};
  let okx=vec![get("/api/v5/trade/fills-history?instType=SPOT&instId=BNT-USDT&ordId=201&limit=100".into())];
  let later=vec![get("/api/v5/account/bills-archive?ccy=USDT&begin=1&end=2&limit=100".into())];
  let futures=vec![get(signed_usertrades("601","0")),get(signed_usertrades("902","0"))];
  let manifest=build_offline_capture_manifest(9,1791880000000,1791880060000,OkxDomain::Global,&okx,&later,&futures).unwrap();
  assert_eq!(manifest.count(),4);
  assert_eq!(manifest.records()[2].endpoint(),"/fapi/v1/userTrades");
  assert_eq!(manifest.records()[3].endpoint(),"/fapi/v1/userTrades");
  assert!(!manifest.can_mark_verified_closed_or_authorize_trading());
  assert!(!manifest.independent_exchange_authentication_proven());
  let mut changed=futures.clone();
  changed[0].body="[{\"id\":1001}]".into();
  assert!(verify_offline_capture_manifest(&manifest,9,1791880000000,1791880060000,OkxDomain::Global,&okx,&later,&changed).is_err());
 }
 #[test]
 fn p101_b1_okx_fill_requests_must_deny_nonspot_wrong_symbol_or_noncanonical_id(){
   let valid="/api/v5/trade/fills-history?instType=SPOT&instId=BNT-USDT&ordId=201&limit=100";
   assert_eq!(vetted_path(CaptureLane::OkxOriginal,valid).unwrap(),
      "/api/v5/trade/fills-history");
   for bad in [
      valid.replace("instType=SPOT","instType=FUTURES"),
      valid.replace("instId=BNT-USDT","instId=BNTUSDT"),
      valid.replace("instId=BNT-USDT","instId=usdt-USDT"),
      valid.replace("ordId=201","ordId=0"),
      valid.replace("ordId=201","ordId=0201"),
      valid.replace("ordId=201","ordId=not-an-order"),
      format!("{valid}&after=0"),
      format!("{valid}&after=not-a-cursor"),
   ]{
      assert!(vetted_path(CaptureLane::OkxOriginal,&bad).is_err(),
         "B1 capture accepted noncanonical original OKX fills request: {bad}");
   }
 }
 #[test]
 fn p101_binance_fills_reject_missing_owner_order_symbol_and_bad_pagination(){
  let good=signed_usertrades("601","0");
  assert_eq!(vetted_path(CaptureLane::BinanceOriginalAndDelayed,&good).unwrap(),"/fapi/v1/userTrades");
  let bad=[
   good.replace("orderId=601&",""),
   good.replace("symbol=BNTUSDT&",""),
   good.replace("orderId=601","orderId=0"),
   good.replace("orderId=601","orderId=not-an-id"),
   good.replace("fromId=0","fromId=-1"),
   good.replace("limit=1000","limit=1001"),
   good.replace("limit=1000&",""),
   good.replace("signature=","signature=nothex"),
   good.replace("recvWindow=10000","recvWindow=99999"),
   good.replace("symbol=BNTUSDT","symbol=BNT&USD"),
   good.replace("/fapi/v1/userTrades","/fapi/v1/order"),
   format!("{good}&orderId=602"),
  ];
  for (i,b) in bad.iter().enumerate(){
   assert!(vetted_path(CaptureLane::BinanceOriginalAndDelayed,b).is_err(),"accepted invalid Binance original fill request {i}");
  }
  assert!(vetted_path(CaptureLane::OkxOriginal,&good).is_err());
 }
}

/// P1-01: bind the existing B1 byte transcript's original Binance fills to
/// BOTH immutable order identities from the already implemented B2 owner plan.
/// This is a local cross-component consistency check, NOT exchange provenance.
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct OwnerBoundFuturesCapture {
 pub owner_generation:u64,
 pub owner_sha256:String,
 pub manifest_head_sha256:String,
 pub original_open_pages:usize,
 pub original_close_pages:usize,
 pub exchange_source_authenticated:bool,
 pub can_mark_verified_closed_or_trade:bool,
}
pub fn inspect_owner_bound_original_futures_pages_without_network(
 store:&NativeStateStore, generation:u64, first_end:u64, late_end:u64,
 domain:OkxDomain,okx_initial:&[MockGetStep],okx_late:&[MockGetStep],
 binance:&[MockGetStep],
)->Result<OwnerBoundFuturesCapture,CaptureError>{
 use crate::native_trade2f3_private_read_plan::{
  inspect_private_history_plan_without_network,PlannedKind};
 let plan=inspect_private_history_plan_without_network(
  store,generation,first_end,late_end,domain)
  .map_err(|_|deny("B2 original owner plan is not available"))?;
 let slots=plan.slots();
 if slots.len()!=10||slots[2].kind()!=PlannedKind::BinanceShortOpenFills
    ||slots[3].kind()!=PlannedKind::BinanceShortCloseFills{
    return Err(deny("B2 Binance original owner slots absent"))
 }
 let expected=[
   (slots[2].selector(),slots[2].query_symbol().ok_or_else(||deny("missing B2 open symbol"))?),
   (slots[3].selector(),slots[3].query_symbol().ok_or_else(||deny("missing B2 close symbol"))?),
 ];
 let manifest=build_offline_capture_manifest(generation,first_end,late_end,
       domain,okx_initial,okx_late,binance)?;
 // Reuse the preexisting F2G sealed-owner OKX fills/bills proof on
 // EXACTLY the bytes hashed by B1; no parallel OKX parser or new GET.
 let okx=crate::native_trade2f_okx_owner_mock::inspect_owner_bound_okx_offline_mock(
       store,generation,first_end,domain,okx_initial)
       .map_err(|_|deny("B1 original OKX tape failed sealed-owner F2G validation"))?;
 if okx.original_owner_generation!=generation||okx.signed_mock_gets==0
    ||okx.independent_real_source_attested||okx.real_ready||okx.verified_closed
    ||okx.may_send_cancel_retry_or_transfer{
    return Err(deny("original OKX proof attempted to assert live authority"))
 }
 let mut pages=[0usize;2];
 let mut stage=0usize;
 let mut prior_from_id=[None::<u64>,None::<u64>];
 let mut prior_trade_id=[None::<u64>,None::<u64>];
 let mut terminal_page_seen=[false;2];
 for step in binance{
    if vetted_path(CaptureLane::BinanceOriginalAndDelayed,&step.path_and_query)?
       !="/fapi/v1/userTrades"{continue}
    let url=url::Url::parse(&format!("https://review.invalid{}",step.path_and_query))
       .map_err(|_|deny("bad original Binance capture URL"))?;
    let params:std::collections::BTreeMap<String,String>=url.query_pairs()
       .map(|(k,v)|(k.into_owned(),v.into_owned())).collect();
    let id=params.get("orderId").ok_or_else(||deny("missing original future order ID"))?;
    let symbol=params.get("symbol").ok_or_else(||deny("missing original futures symbol"))?;
    if id==expected[1].0{
       if pages[0]==0{return Err(deny("close fills captured before original open"))}
       if !terminal_page_seen[0]{
          return Err(deny("opening futures fills pagination has no terminal page"))
       }
       stage=1;
    }
    if id!=expected[stage].0||symbol!=expected[stage].1{
       return Err(deny("Binance fills do not belong to immutable original owner"))
    }
    if terminal_page_seen[stage]{
       return Err(deny("Binance original fills continued after terminal page"))
    }
    let from_id=params.get("fromId").and_then(|v|v.parse::<u64>().ok())
       .ok_or_else(||deny("missing original Binance page cursor"))?;
    if pages[stage]==0{
       if from_id!=0{return Err(deny("original Binance fills must begin at first page"))}
    }else if prior_from_id[stage].map_or(true,|previous|from_id<=previous)
       ||prior_trade_id[stage].and_then(|previous|previous.checked_add(1))!=Some(from_id){
       return Err(deny("original Binance page cursor skipped or repeated executions"))
    }
    pages[stage]+=1;
    if pages[stage]>slots[stage+2].max_pages() as usize{
       return Err(deny("original futures pagination exceeds B2 budget"))
    }
    prior_from_id[stage]=Some(from_id);
    let fills:serde_json::Value=serde_json::from_str(&step.body)
       .map_err(|_|deny("malformed Binance original fill body"))?;
    let rows=fills.as_array().ok_or_else(||deny("Binance original fill response not array"))?;
    let page_limit=params.get("limit").and_then(|v|v.parse::<usize>().ok())
       .ok_or_else(||deny("missing Binance original page size"))?;
    if rows.len()>page_limit||(pages[stage]==1 && rows.is_empty()){
       return Err(deny("invalid Binance fills page length or empty original history"))
    }
    for fill in rows{
       if fill.get("orderId").and_then(|v|v.as_u64()).map(|v|v.to_string())!=Some(id.clone())
          ||fill.get("symbol").and_then(|v|v.as_str())!=Some(symbol.as_str()){
          return Err(deny("Binance original fill row foreign to sealed owner"))
       }
       let trade_id=fill.get("id").and_then(|v|v.as_u64())
          .ok_or_else(||deny("Binance original fill has no valid trade ID"))?;
       if trade_id<from_id || prior_trade_id[stage].map_or(false,|last|trade_id<=last){
          return Err(deny("duplicate or out-of-order original Binance trade ID"))
       }
       prior_trade_id[stage]=Some(trade_id);
    }
    terminal_page_seen[stage]=rows.len()<page_limit;
 }
 if pages.iter().any(|n|*n==0)||terminal_page_seen.iter().any(|x|!*x){
    return Err(deny("missing original Binance owner fill lane"))
 }
 // Re-read immutable owner after all source checks. Same-host hash is not
 // independent exchange source authentication nor antirollback evidence.
 let after=inspect_private_history_plan_without_network(
    store,generation,first_end,late_end,domain)
    .map_err(|_|deny("B2 owner changed during B1 source capture"))?;
 if after!=plan{return Err(deny("original owner drift during source capture"))}
 Ok(OwnerBoundFuturesCapture{
    owner_generation:generation,
    owner_sha256:plan.owner_sha256().into(),
    manifest_head_sha256:manifest.head_sha256().into(),
    original_open_pages:pages[0],original_close_pages:pages[1],
    exchange_source_authenticated:false,can_mark_verified_closed_or_trade:false,
 })
}

/// Offline linkage only: B1 hashes ALL original Binance fills and
/// original/delayed income, B2 binds original userTrades to sealed owner,
/// and existing H/I validate late money and funding on the income-only tape.
/// No new parser, new HTTP adapter, real-source trust or trading authority.
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct OwnerBoundFullOfflineReview{
 pub original:OwnerBoundFuturesCapture,
 pub income_and_bills:UnifiedOfflineReport,
 /// Exact B2 slot order. Each value counts original/delayed raw GET pages
 /// consumed by earlier validated source-specific G/H/I/B1/B2 inspectors.
 /// Repeated consistency scans count as pages, NOT separate trade events.
 pub ten_slot_source_page_counts:[usize;10],
 pub exchange_authenticated:bool,
 pub can_mark_verified_closed_or_trade:bool,
}
/// Count exact captured B1 request bytes into the ten existing B2 owner
/// slots. This is an audit trail of *previously validated* mock reads, not
/// another exchange decoder and not a statement of account completeness.
fn trace_ten_b2_source_slots(
 store:&NativeStateStore,generation:u64,first_end:u64,late_end:u64,
 domain:OkxDomain,okx_initial:&[MockGetStep],okx_late:&[MockGetStep],
 binance_income:&[MockGetStep],binance_fills:&[MockGetStep],
)->Result<[usize;10],CaptureError>{
 use crate::native_trade2f3_private_read_plan::inspect_private_history_plan_without_network;
 let bp=inspect_private_history_plan_without_network(store,generation,first_end,late_end,domain)
   .map_err(|_|deny("B2 owner unavailable for ten-slot source trace"))?;
 let slots=bp.slots();
 if slots.len()!=10{return Err(deny("not ten owner-scoped B2 sources"))}
 let mut counts=[0usize;10];
 for (source,tape) in [
   (0usize,okx_initial),(1usize,okx_late),
   (2usize,binance_income),(3usize,binance_fills)
 ]{
   for item in tape{
      let lane=if source==0{CaptureLane::OkxOriginal}
         else if source==1{CaptureLane::OkxDelayed}
         else{CaptureLane::BinanceOriginalAndDelayed};
      let path=vetted_path(lane,&item.path_and_query)?;
      let url=url::Url::parse(&format!("https://review.invalid{}",item.path_and_query))
          .map_err(|_|deny("invalid ten-slot captured request"))?;
      let params:std::collections::BTreeMap<String,String>=url.query_pairs()
          .map(|(k,v)|(k.into_owned(),v.into_owned())).collect();
      let slot=match (source,path.as_str()){
         (0,"/api/v5/trade/fills-history")=>{
            match params.get("ordId").map(String::as_str){
               Some(id) if id==slots[0].selector()=>0,
               Some(id) if id==slots[1].selector()=>1,
               _=>return Err(deny("unplanned OKX original order slot")),
            }
         }
         (0,"/api/v5/account/bills-archive")|
         (1,"/api/v5/account/bills-archive")=>{
            let ccy=params.get("ccy").ok_or_else(||deny("OKX bill currency absent"))?;
            let label=format!("{}:{ccy}",if source==0{"original"}else{"delayed"});
            let start=if source==0{4}else{6};
            if slots[start].selector()==label{start}
            else if slots[start+1].selector()==label{start+1}
            else{return Err(deny("unplanned OKX account bill source"))}
         }
         (2,"/fapi/v1/income")=>{
            let end=params.get("endTime").and_then(|x|x.parse::<u64>().ok())
                .ok_or_else(||deny("Binance income window missing"))?;
            if end==first_end{8}
            else if end==late_end{9}
            else{return Err(deny("Binance income outside B2 original/delayed window"))}
         }
         (3,"/fapi/v1/userTrades")=>{
            match params.get("orderId").map(String::as_str){
               Some(id) if id==slots[2].selector()=>2,
               Some(id) if id==slots[3].selector()=>3,
               _=>return Err(deny("unplanned Binance original order source")),
            }
         }
         _=>return Err(deny("source tape cannot be assigned to its B2 lane"))
      };
      if slots[slot].endpoint()!=path{
          return Err(deny("B2 endpoint differs from B1 consumed source"))
      }
      counts[slot]=counts[slot].checked_add(1)
          .ok_or_else(||deny("ten-slot source counter overflow"))?;
      if counts[slot]>usize::from(slots[slot].max_pages()){
          return Err(deny("B1 source capture exceeded B2 maximum pages for owner slot"))
      }
   }
 }
 if counts.iter().any(|n|*n==0){
    return Err(deny("ten-slot B2 capture lacks a mandatory owner source"))
 }
 Ok(counts)
}

pub fn inspect_owner_bound_b1_b2_h_i_without_network(
 store:&NativeStateStore,generation:u64,first_end:u64,late_end:u64,
 domain:OkxDomain,okx_initial:&[MockGetStep],okx_late:&[MockGetStep],
 binance_income:&[MockGetStep],binance_futures_fills:&[MockGetStep],
)->Result<OwnerBoundFullOfflineReview,CaptureError>{
 if binance_income.is_empty()||binance_futures_fills.is_empty(){
    return Err(deny("income and original futures must each have a source tape"))
 }
 for step in binance_income{
    if vetted_path(CaptureLane::BinanceOriginalAndDelayed,&step.path_and_query)?
       !="/fapi/v1/income"{
       return Err(deny("futures fills cannot enter the income accounting lane"))
    }
 }
 for step in binance_futures_fills{
    if vetted_path(CaptureLane::BinanceOriginalAndDelayed,&step.path_and_query)?
       !="/fapi/v1/userTrades"{
       return Err(deny("income cannot enter the futures fill lane"))
    }
 }
 let before=store.load().map_err(|_|deny("owner unavailable before linked history"))?;
 let mut combined=binance_income.to_vec();
 combined.extend_from_slice(binance_futures_fills);
 let original=inspect_owner_bound_original_futures_pages_without_network(
      store,generation,first_end,late_end,domain,okx_initial,okx_late,&combined)?;
 // Use same bytes, owner generation and time bounds, but DO NOT decode
 // userTrades as income: H/I already own fee/funding de-duplication.
 let income_and_bills=inspect_original_owner_unified_offline(
      store,generation,first_end,late_end,domain,
      okx_initial,okx_late,binance_income).map_err(CaptureError::Proof)?;
 let ten_slot_source_page_counts=trace_ten_b2_source_slots(
   store,generation,first_end,late_end,domain,
   okx_initial,okx_late,binance_income,binance_futures_fills)?;
 if income_and_bills.sealed_original_generation!=generation
    ||income_and_bills.real_ready||income_and_bills.verified_closed
    ||income_and_bills.may_send_order_cancel_transfer_or_release
    ||store.load().map_err(|_|deny("owner unavailable after linked history"))?!=before{
       return Err(deny("cross-venue owner drift or unsupported authority claim"))
 }
 Ok(OwnerBoundFullOfflineReview{
    original,income_and_bills,ten_slot_source_page_counts,
    exchange_authenticated:false,can_mark_verified_closed_or_trade:false,
 })
}

/// P1-01: strict *completeness of local B1 byte coverage*, not exchange
/// completeness. Only call with per-slot counts from the existing, already
/// successful owner/B2/H/I inspector; user-provided counts cannot be trusted.
fn require_all_ten_b2_local_pages(
 b1:&OfflineCaptureManifest,owner_generation:u64,
 expected_manifest_head:&str,counts:[usize;10],
 archive:&[LocalArchivePageFingerprint],
)->Result<LocalArchiveToB1ByteParity,CaptureError>{
 if b1.owner_generation()!=owner_generation
    ||b1.head_sha256()!=expected_manifest_head
    ||counts.iter().any(|n|*n==0||*n>4)
    ||counts.iter().try_fold(0usize,|total,n|total.checked_add(*n))!=Some(b1.count()){
    return Err(deny("B2 owner/head/10-slot page census differs from B1 transcript"))
 }
 if archive.len()!=b1.count(){
    return Err(deny("scoped archive omits a required original B1 source page"))
 }
 let report=match_scoped_archive_pages_to_existing_b1_bytes_without_network(b1,archive)?;
 if report.pages_matched!=b1.count()||report.captured_b1_pages!=b1.count(){
    return Err(deny("local archive did not cover every B1 source byte page"))
 }
 Ok(report)
}

/// Tightens prior diagnostic subset parity to an exact bijection with
/// the pre-existing 10-slot owner-bound B1/B2/H/I inspector. Requires a
/// validated report from that inspector; no new income/fee/fill parsing.
///
/// Even 10/10 local byte parity does NOT authenticate the exchange, validate
/// original account data, prove posting finality, or authorize REAL / close.
fn require_complete_owner_bound_archive_b1_b2_bytes_without_network(
 validated:&OwnerBoundFullOfflineReview,
 captured:&OfflineCaptureManifest,
 archive:&[LocalArchivePageFingerprint],
)->Result<LocalArchiveToB1ByteParity,CaptureError>{
 if validated.exchange_authenticated||validated.can_mark_verified_closed_or_trade
   ||validated.original.exchange_source_authenticated
   ||validated.original.can_mark_verified_closed_or_trade
   ||validated.original.owner_generation!=validated.income_and_bills.sealed_original_generation{
     return Err(deny("B2 local diagnostic attempted exchange/REAL authority"))
 }
 require_all_ten_b2_local_pages(
   captured,validated.original.owner_generation,
   &validated.original.manifest_head_sha256,
   validated.ten_slot_source_page_counts,archive)
}

/// Re-run the EXISTING B1/B2/F2G/H/I owner-bound offline inspector
/// before making the optional local archive-to-transcript byte comparison.
/// Caller cannot inject page counts, head SHA or owner identity into this gate.
/// Uses signed-in-memory MOCK tapes and stored Native state only; NO network.
pub fn inspect_owner_bound_complete_archive_byte_join_without_network(
 store:&NativeStateStore,generation:u64,first_end:u64,late_end:u64,
 domain:OkxDomain,okx_initial:&[MockGetStep],okx_late:&[MockGetStep],
 binance_income:&[MockGetStep],binance_futures_fills:&[MockGetStep],
 archive:&[LocalArchivePageFingerprint],
)->Result<LocalArchiveToB1ByteParity,CaptureError>{
 let validated=require_original_futures_exact_economics_without_network(
   store,generation,first_end,late_end,domain,
   okx_initial,okx_late,binance_income,binance_futures_fills)?;
 let mut combined=binance_income.to_vec();
 combined.extend_from_slice(binance_futures_fills);
 let manifest=build_offline_capture_manifest(generation,first_end,late_end,
   domain,okx_initial,okx_late,&combined)?;
 require_complete_owner_bound_archive_b1_b2_bytes_without_network(
    &validated,&manifest,archive)
}

#[cfg(test)]
mod ten_slot_archive_exact_byte_coverage_tests{
 use super::*;
 fn set()->(OfflineCaptureManifest,Vec<LocalArchivePageFingerprint>){
  let get=|s:String|MockGetStep{path_and_query:s,http_status:200,body:"[]".into()};
  let okx_first=vec![
   get("/api/v5/trade/fills-history?instType=SPOT&instId=BNT-USDT&ordId=1001&limit=100".into()),
   get("/api/v5/trade/fills-history?instType=SPOT&instId=BNT-USDT&ordId=1002&limit=100".into()),
   get("/api/v5/account/bills-archive?ccy=BNT&begin=1&end=2&limit=100".into()),
   get("/api/v5/account/bills-archive?ccy=USDT&begin=1&end=2&limit=100".into()),
  ];
  let okx_late=vec![
   get("/api/v5/account/bills-archive?ccy=BNT&begin=2&end=3&limit=100".into()),
   get("/api/v5/account/bills-archive?ccy=USDT&begin=2&end=3&limit=100".into()),
  ];
  let signature="a".repeat(64);
  let binance=vec![
   get(format!("/fapi/v1/income?startTime=1&endTime=2&page=1&limit=1000&timestamp=4&recvWindow=10000&signature={signature}")),
   get(format!("/fapi/v1/income?startTime=2&endTime=3&page=1&limit=1000&timestamp=4&recvWindow=10000&signature={signature}")),
   get(format!("/fapi/v1/userTrades?symbol=BNTUSDT&orderId=601&fromId=0&limit=1000&timestamp=4&recvWindow=10000&signature={signature}")),
   get(format!("/fapi/v1/userTrades?symbol=BNTUSDT&orderId=602&fromId=0&limit=1000&timestamp=4&recvWindow=10000&signature={signature}")),
  ];
  let b1=build_offline_capture_manifest(
   19,1_791_880_000_000,1_791_880_060_000,OkxDomain::Global,
   &okx_first,&okx_late,&binance).unwrap();
  assert_eq!(b1.count(),10);
  let local=b1.records.iter().map(|r|LocalArchivePageFingerprint{
    endpoint:r.endpoint.clone(),
    signed_path_query_sha256:r.request_sha256.clone(),
    original_response_sha256:r.response_sha256.clone(),
  }).collect::<Vec<_>>();
  (b1,local)
 }
 #[test]
 fn ten_original_slots_exact_bijection_still_has_no_external_truth(){
  let (b1,local)=set();
  let r=require_all_ten_b2_local_pages(
    &b1,19,b1.head_sha256(),[1;10],&local).unwrap();
  assert_eq!(r.pages_matched,10);
  assert_eq!(r.captured_b1_pages,10);
  assert!(!r.original_exchange_authenticated&&!r.account_history_complete);
  assert!(!r.original_owner_proven&&!r.may_trade_or_mark_closed);
  let reordered=local.iter().cloned().rev().collect::<Vec<_>>();
  assert_eq!(require_all_ten_b2_local_pages(
    &b1,19,b1.head_sha256(),[1;10],&reordered).unwrap().pages_matched,10);
 }
 #[test]
 fn missing_duplicated_and_foreign_archived_pages_fail_closed(){
  let (b1,local)=set();
  assert!(require_all_ten_b2_local_pages(
    &b1,19,b1.head_sha256(),[1;10],&local[..9]).is_err());
  let mut duplicated=local.clone();duplicated[9]=duplicated[8].clone();
  assert!(require_all_ten_b2_local_pages(
    &b1,19,b1.head_sha256(),[1;10],&duplicated).is_err());
  let mut foreign=local.clone();foreign[0].original_response_sha256="a".repeat(64);
  assert!(require_all_ten_b2_local_pages(
    &b1,19,b1.head_sha256(),[1;10],&foreign).is_err());
  let mut foreign=local;foreign[0].endpoint="/fapi/v1/income".into();
  assert!(require_all_ten_b2_local_pages(
    &b1,19,b1.head_sha256(),[1;10],&foreign).is_err());
 }
 #[test]
 fn owner_drift_wrong_head_or_missing_b2_slot_fail_closed(){
  let (b1,local)=set();
  assert!(require_all_ten_b2_local_pages(
    &b1,20,b1.head_sha256(),[1;10],&local).is_err());
  assert!(require_all_ten_b2_local_pages(
    &b1,19,&"b".repeat(64),[1;10],&local).is_err());
  let mut missing=[1;10];missing[2]=0;
  assert!(require_all_ten_b2_local_pages(
    &b1,19,b1.head_sha256(),missing,&local).is_err());
  let mut wrong=[1;10];wrong[2]=2;
  assert!(require_all_ten_b2_local_pages(
    &b1,19,b1.head_sha256(),wrong,&local).is_err());
  let mut excessive=[1;10];excessive[2]=5;
  assert!(require_all_ten_b2_local_pages(
    &b1,19,b1.head_sha256(),excessive,&local).is_err());
 }
 #[test]
 fn complete_join_cannot_bypass_missing_original_sealed_owner(){
  let (b1,local)=set();
  let basename=format!("spotarb-p101-no-owner-{}",std::process::id());
  let path=std::env::temp_dir().join(basename).join("empty-native-owner.json");
  let store=NativeStateStore::open(&path).unwrap();
  assert!(store.load().unwrap().is_none());
  let r=inspect_owner_bound_complete_archive_byte_join_without_network(
    &store,19,1_791_880_000_000,1_791_880_060_000,OkxDomain::Global,
    &[],&[],&[],&[],&local);
  assert!(r.is_err());
  assert_eq!(b1.count(),10);
  assert!(!path.exists());
  if let Some(parent)=path.parent(){std::fs::remove_dir(parent).unwrap();}
 }
}

/// P1-01 strict parity gate for the original Binance fill trade IDs.
/// This intentionally does not alter the older B1 *capture-only* report:
/// a matching owner/orderId and a self-hash cannot prove per-fill parity.
fn exact_futures_trade_ids(
    sealed_open:&BTreeSet<String>,sealed_close:&BTreeSet<String>,
    open_order:&str,close_order:&str,tape:&[MockGetStep],
)->Result<(),CaptureError>{
 let mut seen_open=BTreeSet::new();
 let mut seen_close=BTreeSet::new();
 for step in tape{
    if vetted_path(CaptureLane::BinanceOriginalAndDelayed,&step.path_and_query)?
       !="/fapi/v1/userTrades"{
       return Err(deny("income and original fills must remain separate"))
    }
    let uri=url::Url::parse(&format!("https://review.invalid{}",step.path_and_query))
       .map_err(|_|deny("invalid original futures source URL"))?;
    let params:std::collections::BTreeMap<String,String>=uri.query_pairs()
       .map(|(k,v)|(k.into_owned(),v.into_owned())).collect();
    let order=params.get("orderId").ok_or_else(||deny("original futures order absent"))?;
    let target=if order==open_order{&mut seen_open}
      else if order==close_order{&mut seen_close}
      else{return Err(deny("foreign original futures order"))};
    let rows:serde_json::Value=serde_json::from_str(&step.body)
      .map_err(|_|deny("invalid original futures response"))?;
    let rows=rows.as_array().ok_or_else(||deny("original futures response not array"))?;
    for row in rows{
       let id=row.get("id").and_then(|v|v.as_u64())
          .ok_or_else(||deny("original Binance trade ID must be numeric"))?;
       if !target.insert(id.to_string()){
          return Err(deny("duplicate original futures trade ID"))
       }
    }
 }
 if &seen_open!=sealed_open||&seen_close!=sealed_close{
    return Err(deny("original Binance userTrades IDs differ from persisted Native fills"))
 }
 Ok(())
}

/// Call only AFTER the existing 10-source B1/B2/F2G/H/I cross-check succeeds.
/// Refuses a positive original-futures parity statement when self-consistent
/// MOCK userTrades IDs do not equal the immutable per-fill Native ledger IDs.
/// It does not grant real exchange authentication, profit or trading authority.
pub fn require_original_futures_trade_id_parity_without_network(
 store:&NativeStateStore,generation:u64,first_end:u64,late_end:u64,
 domain:OkxDomain,okx_initial:&[MockGetStep],okx_late:&[MockGetStep],
 binance_income:&[MockGetStep],binance_futures_fills:&[MockGetStep],
)->Result<OwnerBoundFullOfflineReview,CaptureError>{
 let report=inspect_owner_bound_b1_b2_h_i_without_network(
    store,generation,first_end,late_end,domain,
    okx_initial,okx_late,binance_income,binance_futures_fills)?;
 let state=store.load().map_err(|_|deny("Native owner unavailable for futures parity"))?
    .ok_or_else(||deny("Native owner absent for futures parity"))?;
 if state.generation!=generation{return Err(deny("Native generation drift before parity"))}
 let entry=state.entry.as_ref().ok_or_else(||deny("Native entry absent for parity"))?;
 let open=&entry.recovery_checkpoint.as_ref()
    .ok_or_else(||deny("persisted original futures opening absent"))?.ledger;
 let close=&entry.shadow_close_receipt.as_ref()
    .ok_or_else(||deny("persisted original futures closing absent"))?
    .bounded_economic_ledgers().1;
 let open_order=open.exchange_order_id.as_deref()
    .ok_or_else(||deny("persisted Binance opening order absent"))?;
 let close_order=close.exchange_order_id.as_deref()
    .ok_or_else(||deny("persisted Binance closing order absent"))?;
 let sealed_open=open.fills.keys().cloned().collect::<BTreeSet<_>>();
 let sealed_close=close.fills.keys().cloned().collect::<BTreeSet<_>>();
 exact_futures_trade_ids(&sealed_open,&sealed_close,open_order,close_order,binance_futures_fills)?;
 if store.load().map_err(|_|deny("owner unreadable after parity"))?.as_ref()!=Some(&state){
    return Err(deny("Native owner changed during original futures parity"))
 }
 Ok(report)
}

#[cfg(test)]
mod strict_futures_ids_unit {
 use super::*;
 #[test]
 fn matching_trade_ids_are_necessary_and_duplicate_or_absent_ids_fail(){
    let mk=|order:&str,ids:&[u64]|MockGetStep{
        path_and_query:format!("/fapi/v1/userTrades?symbol=BNTUSDT&orderId={order}&fromId=0&limit=1000&timestamp=1780000008000&recvWindow=10000&signature={}", "a".repeat(64)),
        http_status:200,
        body:serde_json::to_string(&ids.iter().map(|id|serde_json::json!({"id":id,"orderId":order.parse::<u64>().unwrap(),"symbol":"BNTUSDT"})).collect::<Vec<_>>()).unwrap(),
    };
    let expected_open=["1001".to_string(),"1002".to_string()].into_iter().collect();
    let expected_close=["2001".to_string()].into_iter().collect();
    let good=vec![mk("601",&[1001,1002]),mk("902",&[2001])];
    assert!(exact_futures_trade_ids(&expected_open,&expected_close,"601","902",&good).is_ok());
    assert!(exact_futures_trade_ids(&expected_open,&expected_close,"601","902",&[mk("601",&[1001]),good[1].clone()]).is_err());
    assert!(exact_futures_trade_ids(&expected_open,&expected_close,"601","902",&[mk("601",&[1001,1001]),good[1].clone()]).is_err());
    assert!(exact_futures_trade_ids(&expected_open,&expected_close,"601","902",&[good[0].clone(),mk("902",&[2002])]).is_err());
 }
}

/// Strict per-fill economic identity on top of the already reviewed ID parity.
/// Reuses the existing private_rest Binance fill decoder rather than
/// duplicating quantity, price, fee and timestamp parsing.
fn exact_original_futures_fill_economics(
 open:&LegFillLedger,close:&LegFillLedger,tape:&[MockGetStep],
)->Result<(),CaptureError>{
 let open_order=open.exchange_order_id.as_deref()
    .ok_or_else(||deny("original futures open exchange order absent"))?;
 let close_order=close.exchange_order_id.as_deref()
    .ok_or_else(||deny("original futures close exchange order absent"))?;
 if open_order==close_order || open.venue!="binance" || close.venue!="binance"
   ||open.symbol!=close.symbol || open.side!="sell" || close.side!="buy"
   ||!open.fee_evidence_complete || !close.fee_evidence_complete{
    return Err(deny("incomplete original futures economic ledger"))
 }
 let expected_open=open.fills.keys().cloned().collect::<BTreeSet<_>>();
 let expected_close=close.fills.keys().cloned().collect::<BTreeSet<_>>();
 exact_futures_trade_ids(&expected_open,&expected_close,open_order,close_order,tape)?;
 let mut seen_open=BTreeSet::new();
 let mut seen_close=BTreeSet::new();
 for step in tape{
    let uri=url::Url::parse(&format!("https://review.invalid{}",step.path_and_query))
       .map_err(|_|deny("invalid original futures request"))?;
    let params:std::collections::BTreeMap<String,String>=uri.query_pairs()
       .map(|(k,v)|(k.into_owned(),v.into_owned())).collect();
    let order=params.get("orderId").ok_or_else(||deny("original futures order missing"))?;
    let (ledger,seen,is_buyer)=if order==open_order{
       (open,&mut seen_open,false)
    }else if order==close_order{
       (close,&mut seen_close,true)
    }else{return Err(deny("foreign original futures order"))};
    let data:serde_json::Value=serde_json::from_str(&step.body)
       .map_err(|_|deny("invalid original Binance fill body"))?;
    let rows=data.as_array().ok_or_else(||deny("original Binance fills must be an array"))?;
    let parsed=parse_binance_fill_events_http(step.http_status,&step.body,order,"USDT")
       .map_err(|_|deny("original Binance fill economics parse failed"))?;
    if parsed.len()!=rows.len(){
       return Err(deny("original Binance source contains foreign rows"))
    }
    for (row,observed) in rows.iter().zip(parsed.iter()){
       if row.get("symbol").and_then(|v|v.as_str())!=Some(ledger.symbol.as_str())
         ||row.get("positionSide").and_then(|v|v.as_str())!=Some("BOTH")
         ||row.get("buyer").and_then(|v|v.as_bool())!=Some(is_buyer)
         ||row.get("commissionAsset").and_then(|v|v.as_str())!=Some("USDT"){
          return Err(deny("original futures symbol side or commission asset differs"))
       }
       let saved=ledger.fills.get(&observed.fill_id)
          .ok_or_else(||deny("original futures trade ID absent in Native ledger"))?;
       if observed!=saved || observed.fee_cost_quote_atoms.is_none()
          ||observed.trade_time_ms.is_none(){
          return Err(deny("original futures qty price fee or timestamp mismatch"))
       }
       if !seen.insert(observed.fill_id.clone()){
          return Err(deny("duplicate original futures source trade"))
       }
    }
 }
 if seen_open!=expected_open||seen_close!=expected_close{
    return Err(deny("original futures source incomplete after economic matching"))
 }
 Ok(())
}

/// Offline strict acceptance: B1/B2 + original OKX F2G + H/I fee/funding
/// + exact Binance per-fill quantity/price/fee/asset/time and source IDs.
/// Still never grants authenticated exchange provenance or trading authority.
pub fn require_original_futures_exact_economics_without_network(
 store:&NativeStateStore,generation:u64,first_end:u64,late_end:u64,
 domain:OkxDomain,okx_initial:&[MockGetStep],okx_late:&[MockGetStep],
 binance_income:&[MockGetStep],binance_futures_fills:&[MockGetStep],
)->Result<OwnerBoundFullOfflineReview,CaptureError>{
 let report=require_original_futures_trade_id_parity_without_network(
    store,generation,first_end,late_end,domain,
    okx_initial,okx_late,binance_income,binance_futures_fills)?;
 let before=store.load().map_err(|_|deny("Native owner unreadable for exact economic matching"))?
    .ok_or_else(||deny("Native owner absent for exact economics"))?;
 if before.generation!=generation{return Err(deny("stale exact economics generation"))}
 let entry=before.entry.as_ref().ok_or_else(||deny("Native position missing"))?;
 let open=&entry.recovery_checkpoint.as_ref()
    .ok_or_else(||deny("original futures opening ledger missing"))?.ledger;
 let close=&entry.shadow_close_receipt.as_ref()
    .ok_or_else(||deny("original futures closing ledger missing"))?
    .bounded_economic_ledgers().1;
 exact_original_futures_fill_economics(open,close,binance_futures_fills)?;
 exact_futures_per_trade_realized_income(
    open.exchange_order_id.as_deref().ok_or_else(||deny("opening futures order ID absent"))?,
    close.exchange_order_id.as_deref().ok_or_else(||deny("closing futures order ID absent"))?,
    &open.symbol,binance_futures_fills,binance_income)?;
 if store.load().map_err(|_|deny("Native owner unreadable after economics"))?.as_ref()!=Some(&before){
    return Err(deny("Native owner changed during economic matching"))
 }
 Ok(report)
}

#[cfg(test)]
mod futures_economic_parity_tests {
 use super::*;
 use crate::fill_economics::{decimal_to_atoms,DEFAULT_AVG_TOLERANCE_ATOMS};
 fn fixture()->(LegFillLedger,LegFillLedger,Vec<MockGetStep>){
  let make=|order:&str,id:u64,buyer:bool,fee:&str,time:u64|MockGetStep{
    path_and_query:format!("/fapi/v1/userTrades?symbol=BNTUSDT&orderId={order}&fromId=0&limit=1000&timestamp=1780000008000&recvWindow=10000&signature={}", "a".repeat(64)),
    http_status:200,
    body:serde_json::json!([{"symbol":"BNTUSDT","id":id,"orderId":order.parse::<u64>().unwrap(),"price":"100","qty":"0.09","commission":fee,"commissionAsset":"USDT","time":time,"positionSide":"BOTH","buyer":buyer}]).to_string()
  };
  let tape=vec![make("601",1001,false,"0.002",1780000003050),make("902",2001,true,"0.004",1780000007150)];
  let ledger=|side:&str,order:&str,step:&MockGetStep|{
    let qty=decimal_to_atoms("0.09").unwrap();
    let mut l=LegFillLedger::new("binance","BNTUSDT",side,"intent","client",qty).unwrap();
    l.apply_cumulative_truth(qty,Some(decimal_to_atoms("100").unwrap()),true,Some("filled"),Some(order),Some(1780000008000),DEFAULT_AVG_TOLERANCE_ATOMS).unwrap();
    for e in parse_binance_fill_events_http(200,&step.body,order,"USDT").unwrap(){l.apply_fill(e,DEFAULT_AVG_TOLERANCE_ATOMS).unwrap();}
    assert!(l.fee_evidence_complete);
    l
  };
  (ledger("sell","601",&tape[0]),ledger("buy","902",&tape[1]),tape)
 }
 #[test]
 fn exact_fields_accept_matching_and_deny_modified_execution(){
  let (open,close,tape)=fixture();
  assert!(exact_original_futures_fill_economics(&open,&close,&tape).is_ok());
  for (field,value) in [
   ("qty",serde_json::json!("0.08")),
   ("price",serde_json::json!("101")),
   ("commission",serde_json::json!("0.003")),
   ("commissionAsset",serde_json::json!("BNB")),
   ("time",serde_json::json!(1780000003051u64)),
   ("positionSide",serde_json::json!("SHORT")),
   ("buyer",serde_json::json!(true)),
   ("symbol",serde_json::json!("OTHERUSDT"))
  ]{
    let mut altered=tape.clone();
    let mut body:serde_json::Value=serde_json::from_str(&altered[0].body).unwrap();
    body[0][field]=value;
    altered[0].body=body.to_string();
    assert!(exact_original_futures_fill_economics(&open,&close,&altered).is_err(),"accepted changed {field}");
  }
  let mut down=tape.clone();
  down[0].http_status=503;
  assert!(exact_original_futures_fill_economics(&open,&close,&down).is_err());
 }
}

/// P1-01: F2I already proves owner-scoped funding and total realised PnL.
/// This last local check binds the per-trade realizedPnl field from userTrades
/// to the same trade's REALIZED_PNL income record, without adding it twice.
fn exact_futures_per_trade_realized_income(
 open_order:&str,close_order:&str,symbol:&str,
 futures:&[MockGetStep],income:&[MockGetStep],
)->Result<(),CaptureError>{
 let mut observed=std::collections::BTreeMap::<String,i128>::new();
 for step in futures{
  if vetted_path(CaptureLane::BinanceOriginalAndDelayed,&step.path_and_query)?
      !="/fapi/v1/userTrades"{return Err(deny("non-futures page in original fills"))}
  let url=url::Url::parse(&format!("https://review.invalid{}",step.path_and_query))
      .map_err(|_|deny("malformed futures realized PnL URL"))?;
  let order=url.query_pairs().find(|(key,_)|key=="orderId").map(|(_,v)|v.into_owned())
      .ok_or_else(||deny("missing original futures order"))?;
  if order!=open_order&&order!=close_order{return Err(deny("foreign futures order"))}
  if step.http_status!=200{return Err(deny("bad futures realized PnL HTTP status"))}
  let raw:serde_json::Value=serde_json::from_str(&step.body)
      .map_err(|_|deny("bad futures realized PnL JSON"))?;
  let rows=raw.as_array().ok_or_else(||deny("bad futures realized PnL array"))?;
  for row in rows{
    if row.get("orderId").and_then(|x|x.as_u64()).map(|x|x.to_string())!=Some(order.clone())
      ||row.get("symbol").and_then(|x|x.as_str())!=Some(symbol){
      return Err(deny("realized PnL source row foreign to original position"))
    }
    let id=row.get("id").and_then(|x|x.as_u64()).ok_or_else(||deny("missing futures trade ID"))?;
    let raw=row.get("realizedPnl").and_then(|x|x.as_str())
       .ok_or_else(||deny("original Binance realizedPnl missing"))?;
    let amount=signed_decimal_to_atoms(raw)
       .map_err(|_|deny("invalid Binance per-trade realizedPnl"))?;
    if order==open_order {
       if amount!=0{return Err(deny("opening short has nonzero realized PnL"))}
    }else if observed.insert(id.to_string(),amount).is_some(){
       return Err(deny("duplicate closing trade realizedPnl"))
    }
  }
 }
 if observed.is_empty(){return Err(deny("missing original futures close realized PnL"))}
 let mut raw_income=std::collections::BTreeMap::<u64,(String,i128)>::new();
 for step in income{
  if vetted_path(CaptureLane::BinanceOriginalAndDelayed,&step.path_and_query)?
     !="/fapi/v1/income"||step.http_status!=200{
     return Err(deny("invalid Binance income tape for realized PnL"))
  }
  let v:serde_json::Value=serde_json::from_str(&step.body)
     .map_err(|_|deny("invalid realized income JSON"))?;
  for row in v.as_array().ok_or_else(||deny("income response is not an array"))?{
   if row.get("incomeType").and_then(|x|x.as_str())!=Some("REALIZED_PNL"){continue}
   if row.get("symbol").and_then(|x|x.as_str())!=Some(symbol)
      ||row.get("asset").and_then(|x|x.as_str())!=Some("USDT"){
      return Err(deny("realized income for foreign symbol or currency"))
   }
   let transaction=row.get("tranId").and_then(|x|x.as_u64())
       .ok_or_else(||deny("missing realized income transaction"))?;
   let id=row.get("tradeId").and_then(|x|x.as_str())
       .ok_or_else(||deny("missing realized income trade identity"))?.to_owned();
   let amount=signed_decimal_to_atoms(
       row.get("income").and_then(|x|x.as_str()).ok_or_else(||deny("missing realized income amount"))?)
       .map_err(|_|deny("invalid realized income value"))?;
   let record=(id,amount);
   if let Some(old)=raw_income.insert(transaction,record.clone()){
      if old!=record{return Err(deny("conflicting repeated Binance income transaction"))}
   }
  }
 }
 let mut attributed=std::collections::BTreeMap::<String,i128>::new();
 for (_, (trade,amount)) in raw_income{
   if !observed.contains_key(&trade){return Err(deny("realized income not in original closing fills"))}
   let target=attributed.entry(trade).or_default();
   *target=target.checked_add(amount).ok_or_else(||deny("realized income sum overflow"))?;
 }
 if observed!=attributed{return Err(deny("per-fill realizedPnl does not match Binance income"))}
 Ok(())
}

#[cfg(test)]
mod per_trade_realized_income_tests {
 use super::*;
 fn fill(order:&str,id:u64,pnl:&str)->MockGetStep{
  MockGetStep{
   path_and_query:format!("/fapi/v1/userTrades?symbol=BNTUSDT&orderId={order}&fromId=0&limit=1000&timestamp=1780000008000&recvWindow=10000&signature={}", "a".repeat(64)),
   http_status:200,
   body:serde_json::json!([{"id":id,"orderId":order.parse::<u64>().unwrap(),"symbol":"BNTUSDT","realizedPnl":pnl}]).to_string(),
  }
 }
 fn income(id:&str,amount:&str)->MockGetStep{
  MockGetStep{
   path_and_query:format!("/fapi/v1/income?startTime=1780000000000&endTime=1780000008000&page=1&limit=1000&timestamp=1780000008000&recvWindow=10000&signature={}", "a".repeat(64)),
   http_status:200,
   body:serde_json::json!([{"incomeType":"REALIZED_PNL","tranId":313,"symbol":"BNTUSDT","asset":"USDT","tradeId":id,"income":amount}]).to_string(),
  }
 }
 #[test]
 fn close_realized_pnl_matches_income_by_trade_without_double_counting(){
  let fills=vec![fill("601",1001,"0"),fill("902",2001,"0.18")];
  let money=vec![income("2001","0.18"),income("2001","0.18")];
  let check=|f:&[MockGetStep],m:&[MockGetStep]|
    exact_futures_per_trade_realized_income("601","902","BNTUSDT",f,m);
  assert!(check(&fills,&money).is_ok());
  assert!(check(&fills,&[income("2001","0.17")]).is_err());
  assert!(check(&fills,&[income("2002","0.18")]).is_err());
  assert!(check(&fills,&[income("2001","0.18"),income("2001","0.19")]).is_err());
  assert!(check(&fills,&[]).is_err());
  assert!(check(&[fill("601",1001,"0.01"),fills[1].clone()],&money).is_err());
  assert!(check(&[fills[0].clone(),fill("902",2001,"0.19")],&money).is_err());
  assert!(check(&[fills[0].clone(),fills[1].clone(),fills[1].clone()],&money).is_err());
 }
}

#[cfg(test)]
mod b2_page_window_bound_tests{
 use super::*;
 fn bin_income(limit:&str,page:&str,start:&str,end:&str,signature:&str)->String{
   format!("/fapi/v1/income?startTime={start}&endTime={end}&page={page}&limit={limit}&timestamp=1780000008000&recvWindow=10000&signature={signature}")
 }
 #[test]
 fn p101_b2_income_and_okx_source_requests_deny_invalid_page_size_and_time(){
   let sig="a".repeat(64);
   let valid=bin_income("1000","1","1780000000000","1780000008000",&sig);
   assert_eq!(vetted_path(CaptureLane::BinanceOriginalAndDelayed,&valid).unwrap(),"/fapi/v1/income");
   for bad in [
      bin_income("1001","1","1780000000000","1780000008000",&sig),
      bin_income("0","1","1780000000000","1780000008000",&sig),
      bin_income("x","1","1780000000000","1780000008000",&sig),
      bin_income("1000","0","1780000000000","1780000008000",&sig),
      bin_income("1000","x","1780000000000","1780000008000",&sig),
      bin_income("1000","1","1780000009000","1780000008000",&sig),
      bin_income("1000","1","x","1780000008000",&sig),
      bin_income("1000","1","1780000000000","1780000008000","invalid"),
      valid.replace("recvWindow=10000","recvWindow=70000"),
      valid.replace("timestamp=1780000008000","timestamp=0")
   ]{
      assert!(vetted_path(CaptureLane::BinanceOriginalAndDelayed,&bad).is_err(),"accepted unbounded income query: {bad}");
   }
   let bills="/api/v5/account/bills-archive?ccy=USDT&begin=1780000000000&end=1780000008000&limit=100";
   let fills="/api/v5/trade/fills-history?instType=SPOT&instId=BNT-USDT&ordId=201&limit=100";
   assert!(vetted_path(CaptureLane::OkxOriginal,bills).is_ok());
   assert!(vetted_path(CaptureLane::OkxOriginal,fills).is_ok());
   for bad in [
      bills.replace("limit=100","limit=101"),
      bills.replace("limit=100","limit=0"),
      bills.replace("limit=100","limit=x"),
      bills.replace("begin=1780000000000","begin=1780000009000"),
      fills.replace("limit=100","limit=101"),
      fills.replace("limit=100","limit=0"),
      fills.replace("limit=100","limit=x")
   ]{
      assert!(vetted_path(CaptureLane::OkxOriginal,&bad).is_err(),"accepted unbounded OKX query: {bad}");
   }
 }
}

/// Read-only Astra P1-01 source bridge: B1/B2/G/H/I facts are checked against
/// a Native V1 sealed four-leg owner BEFORE a separate fee-aware V2 sidecar
/// can be associated with it. Equality of generation ALONE is not ownership.
/// This is NOT a V1->V2 migration, exchange attestation or a real trading gate.
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct SourceBoundV2FirstLegReview{
 pub original:OwnerBoundFullOfflineReview,
 pub v2_gross_base_atoms_e12:u128,
 pub v2_fee_adjusted_net_base_atoms_e12:u128,
 pub v1_v2_first_leg_bytes_equal:bool,
 pub owner_v2_migration_certified:bool,
 pub independent_exchange_source_authenticated:bool,
 pub trading_or_verified_close_authorized:bool,
}
// Separate the large V1 owner snapshot from the much deeper source
// validation call stack. Keep only a digest and two scalar economics values.
fn v2_first_leg_owner_preflight(
 store:&NativeStateStore,generation:u64,
 checkpoint:&crate::native_trade2f3_first_leg_v2_checkpoint::FirstLegV2CheckpointPayload,
)->Result<(String,u128,u128),CaptureError>{
 let before=store.load().map_err(|_|deny("owner missing before cross-version source check"))?
    .ok_or_else(||deny("original owner absent before cross-version check"))?;
 if before.generation!=generation
    ||checkpoint.schema_version!=2
    ||checkpoint.original_native_generation!=generation
    ||checkpoint.original_owner_v2_seal_migrated
    ||checkpoint.exchange_global_fees_attested
    ||checkpoint.may_send_or_verify_closed{
    return Err(deny("cross-version original generation or V2 authority invalid"))
 }
 let entry=before.entry.as_ref().ok_or_else(||deny("V1 entry missing for V2 link"))?;
 if entry.plan!=checkpoint.plan
    ||entry.policy_binding.as_ref()!=Some(&checkpoint.policy)
    ||entry.spot_prehedge_evidence.as_ref()!=Some(&checkpoint.original){
    return Err(deny("V2 checkpoint plan, risk policy or exact first leg differs from V1 owner"))
 }
 let preview=crate::native_trade2f3_fee_aware_first_leg_v2::
   inspect_first_leg_fee_preview_v2(
      &checkpoint.policy,&checkpoint.plan,&checkpoint.original,
      &checkpoint.source_shaped_fee_fills)
   .map_err(|_|deny("V2 fee-shaped original fills invalid for sealed first leg"))?;
 if checkpoint.quote_spent_atoms_e12!=preview.quote_spent_with_quote_fees_e12.to_string()
    ||checkpoint.net_base_after_fee_atoms_e12!=preview.original_delivered_base_e12.to_string()
    ||preview.persisted_owner_v2_seal_available
    ||preview.independent_live_exchange_attestation
    ||preview.trading_or_close_authorized{
    return Err(deny("V2 first-leg cached economics or authority invalid"))
 }
 let digest=hash(&serde_json::to_vec(&before).map_err(|_|CaptureError::Encoding)?);
 Ok((digest,preview.original_spot_gross_e12,preview.original_delivered_base_e12))
}
pub fn inspect_v2_first_leg_against_strict_b1_b2_owner_without_network(
 store:&NativeStateStore,generation:u64,first_end:u64,late_end:u64,
 domain:OkxDomain,okx_initial:&[MockGetStep],okx_late:&[MockGetStep],
 income:&[MockGetStep],futures:&[MockGetStep],
 checkpoint:&crate::native_trade2f3_first_leg_v2_checkpoint::FirstLegV2CheckpointPayload,
)->Result<SourceBoundV2FirstLegReview,CaptureError>{
 let (digest,gross,net)=v2_first_leg_owner_preflight(store,generation,checkpoint)?;
 let original=require_original_futures_exact_economics_without_network(
    store,generation,first_end,late_end,domain,okx_initial,okx_late,income,futures)?;
 let (after_digest,_,_)=v2_first_leg_owner_preflight(store,generation,checkpoint)?;
 if digest!=after_digest{
    return Err(deny("original Native owner changed during V1/V2 offline source check"))
 }
 Ok(SourceBoundV2FirstLegReview{
   original,v2_gross_base_atoms_e12:gross,
   v2_fee_adjusted_net_base_atoms_e12:net,
   v1_v2_first_leg_bytes_equal:true,
   owner_v2_migration_certified:false,
   independent_exchange_source_authenticated:false,
   trading_or_verified_close_authorized:false,
 })
}

/// Cold-restart read-only V1/B1/B2 -> persisted fee-aware V2 sidecar proof.
/// An in-memory object is not durable evidence. Reuse the existing
/// write-once V2 checkpoint decoder and owner-scoped exact source checker.
/// This is NOT a joint V1/V2 CAS, mutation capability or source attestation.
fn read_persisted_v2_first_leg_for_owner(
 store:&NativeStateStore,generation:u64,checkpoint_path:&std::path::Path,
)->Result<crate::native_trade2f3_first_leg_v2_checkpoint::FirstLegV2CheckpointPayload,CaptureError>{
 let state=store.load().map_err(|_|deny("Native owner unreadable for V2 disk link"))?
   .ok_or_else(||deny("Native owner absent for V2 disk link"))?;
 if state.generation!=generation{
    return Err(deny("Native owner generation changed during V2 disk link"))
 }
 let entry=state.entry.as_ref().ok_or_else(||deny("Native entry absent for V2 disk link"))?;
 let policy=entry.policy_binding.as_ref().ok_or_else(||deny("V1 policy missing"))?;
 let first=entry.spot_prehedge_evidence.as_ref().ok_or_else(||deny("V1 first leg missing"))?;
 let (payload,_preview)=crate::native_trade2f3_first_leg_v2_checkpoint::load_v2(
    checkpoint_path,generation,policy,&entry.plan,first
 ).map_err(|_|deny("persisted V2 checkpoint is missing corrupt or foreign to V1"))?;
 Ok(payload)
}
pub fn inspect_persisted_v2_first_leg_against_b1_b2_without_network(
 store:&NativeStateStore,generation:u64,first_end:u64,late_end:u64,
 domain:OkxDomain,okx_initial:&[MockGetStep],okx_late:&[MockGetStep],
 income:&[MockGetStep],futures:&[MockGetStep],
 checkpoint_path:&std::path::Path,
)->Result<SourceBoundV2FirstLegReview,CaptureError>{
 let before=read_persisted_v2_first_leg_for_owner(store,generation,checkpoint_path)?;
 let reviewed=inspect_v2_first_leg_against_strict_b1_b2_owner_without_network(
    store,generation,first_end,late_end,domain,
    okx_initial,okx_late,income,futures,&before)?;
 // Reopen the sidecar after the entire V1 source check; reject changes
 // even when a new envelope has a self-consistent local checksum.
 let after=read_persisted_v2_first_leg_for_owner(store,generation,checkpoint_path)?;
 if before!=after{return Err(deny("V2 disk sidecar changed during source verification"))}
 Ok(reviewed)
}

/// Snapshot the actual V1 on-disk bytes and demand they decode to the same
/// exact NativeStateStore owner. The digest is a LOCAL pair identity only;
/// coherent rollback/re-hash on the same VM remains outside its guarantees.
fn verified_full_v1_file_sha256(
    store:&NativeStateStore,v1_path:&std::path::Path,
)->Result<String,CaptureError>{
 let parsed=NativeStateStore::open(v1_path).map_err(|_|deny("V1 path cannot open"))?
   .load().map_err(|_|deny("V1 snapshot invalid at pair path"))?
   .ok_or_else(||deny("V1 snapshot missing at pair path"))?;
 let live=store.load().map_err(|_|deny("original owner unavailable at pair check"))?
   .ok_or_else(||deny("original owner missing at pair check"))?;
 if parsed!=live{return Err(deny("pair V1 file is not the authoritative original owner"))}
 let raw=std::fs::read(v1_path).map_err(|_|deny("V1 snapshot raw bytes missing"))?;
 let check=NativeStateStore::open(v1_path).map_err(|_|deny("V1 path moved"))?
   .load().map_err(|_|deny("V1 changed during pair read"))?
   .ok_or_else(||deny("V1 disappeared during pair read"))?;
 if check!=parsed{return Err(deny("V1 changed during raw snapshot read"))}
 Ok(hash(&raw))
}
/// Attach the exact already-durable V1 byte identity to a NEW V2 payload
/// before the existing O_EXCL persist. Do not modify or bless an old sidecar.
pub fn pin_v2_first_leg_to_existing_v1_disk_once(
    store:&NativeStateStore,v1_path:&std::path::Path,
    payload:&mut crate::native_trade2f3_first_leg_v2_checkpoint::FirstLegV2CheckpointPayload,
)->Result<(),CaptureError>{
 if payload.original_v1_owner_file_sha256.is_some(){
    return Err(deny("V2 first-leg snapshot pin must be write-once"))
 }
 let current=store.load().map_err(|_|deny("V1 unavailable before pair pin"))?
   .ok_or_else(||deny("V1 absent before pair pin"))?;
 let entry=current.entry.as_ref().ok_or_else(||deny("V1 entry missing before pair pin"))?;
 if current.generation!=payload.original_native_generation
    ||entry.policy_binding.as_ref()!=Some(&payload.policy)
    ||entry.plan!=payload.plan
    ||entry.spot_prehedge_evidence.as_ref()!=Some(&payload.original){
    return Err(deny("V1 and V2 first-leg identity differ before pair pin"))
 }
 payload.original_v1_owner_file_sha256=
    Some(verified_full_v1_file_sha256(store,v1_path)?);
 Ok(())
}
/// STRICT local two-file source review. Refuses legacy sidecars without a
/// full V1 file pin; the older per-sidecar reader stays non-certifying.
/// This is not a multi-file atomic CAS, independent source trust or remote ACK.
pub fn inspect_exact_v1_v2_disk_pair_without_network(
    store:&NativeStateStore,v1_path:&std::path::Path,generation:u64,
    first_end:u64,late_end:u64,domain:OkxDomain,
    okx_initial:&[MockGetStep],okx_late:&[MockGetStep],
    income:&[MockGetStep],futures:&[MockGetStep],
    checkpoint_path:&std::path::Path,
)->Result<SourceBoundV2FirstLegReview,CaptureError>{
 let pin_before=verified_full_v1_file_sha256(store,v1_path)?;
 let payload=read_persisted_v2_first_leg_for_owner(store,generation,checkpoint_path)?;
 if payload.original_v1_owner_file_sha256.as_deref()!=Some(pin_before.as_str()){
    return Err(deny("V1/V2 pair lacks the same persisted full-owner snapshot"))
 }
 let result=inspect_persisted_v2_first_leg_against_b1_b2_without_network(
    store,generation,first_end,late_end,domain,okx_initial,okx_late,
    income,futures,checkpoint_path)?;
 let pin_after=verified_full_v1_file_sha256(store,v1_path)?;
 if pin_before!=pin_after{
    return Err(deny("V1 full-owner bytes changed across V1/V2 source review"))
 }
 Ok(result)
}

/// B1/B2->bounded-finance (B4C4/F2I) LOCAL cash bridge. These are
/// source-shaped mock rows, NOT authenticated account balances or full PnL.
/// Do not add synthetic second bills: OKX bill balChg already includes fees
/// in that bill's currency. Repeated GET snapshots are NOT new cash events.
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct BoundedSourceCashBridge{
 pub original_owner_generation:u64,
 pub unique_okx_usdt_bills:usize,
 pub unique_binance_income_postings:usize,
 pub deduplicated_okx_quote_delta_atoms_e12:i128,
 pub deduplicated_binance_money_delta_atoms_e12:i128,
 pub bounded_source_net_atoms_e12:i128,
 pub equals_persisted_bounded_native_net:bool,
 pub independently_authenticated_account:bool,
 pub complete_lifetime_profit:bool,
 pub may_trade_or_verify_closed:bool,
}
/// Source billId and original execution identifiers, NOT derived bill or
/// independently authenticated exchange account provenance.
#[derive(Clone,Debug,PartialEq,Eq)]
struct BoundedOkxBillIdentity{bill_id:String,trade_id:String,order_id:String}
/// Link every cash source row to the ALREADY sealed Native spot owner.
/// This checks identity only, not fee, PnL, or independently sourced truth.
fn require_bounded_cash_original_owner(
 buy:&LegFillLedger,sell:&LegFillLedger,original_buy_bills:&[String],
 rows:&[BoundedOkxBillIdentity],
)->Result<(),CaptureError>{
 let mut wanted=std::collections::BTreeMap::<String,(String,bool)>::new();
 for (ledger,is_buy) in [(buy,true),(sell,false)]{
  let order=ledger.exchange_order_id.as_deref()
      .ok_or_else(||deny("spot owner has no original exchange orderId"))?;
  if ledger.fills.is_empty()||!canonical_okx_history_id(order){
    return Err(deny("spot owner order/fill scope invalid for cash"))
  }
  for id in ledger.fills.keys(){
    if !canonical_okx_history_id(id)
      ||wanted.insert(id.clone(),(order.to_owned(),is_buy)).is_some(){
      return Err(deny("original Native spot owner fill identity duplicated"))
    }
  }
 }
 if rows.len()!=wanted.len(){return Err(deny("one original cash bill per spot fill required"))}
 let mut matched=std::collections::BTreeSet::new();
 let mut actual_buy_bills=std::collections::BTreeSet::new();
 for bill in rows{
   let (ord,is_buy)=wanted.get(&bill.trade_id)
     .ok_or_else(||deny("original cash bill foreign to Native owner tradeId"))?;
   if &bill.order_id!=ord||!matched.insert(bill.trade_id.as_str()){
     return Err(deny("original cash bill orderId or tradeId reused/mismatched"))
   }
   if *is_buy{actual_buy_bills.insert(bill.bill_id.as_str());}
 }
 if matched.len()!=wanted.len(){
   return Err(deny("original spot fill lacks one identifiable cash bill"))
 }
 let saved:std::collections::BTreeSet<&str>=original_buy_bills
   .iter().map(String::as_str).collect();
 if saved.len()!=original_buy_bills.len()||saved!=actual_buy_bills{
    return Err(deny("source cash opening billId differs from sealed Native owner"))
 }
 Ok(())
}
fn source_bounded_money_rows(
 okx_initial:&[MockGetStep],okx_late:&[MockGetStep],income:&[MockGetStep],
)->Result<(i128,i128,i128,i128,i128,usize,usize,Vec<BoundedOkxBillIdentity>),CaptureError>{
 let mut okx=std::collections::BTreeMap::<(String,String),serde_json::Value>::new();
 for (lane,tape) in [(CaptureLane::OkxOriginal,okx_initial),
                     (CaptureLane::OkxDelayed,okx_late)]{
  for step in tape{
   let path=vetted_path(lane,&step.path_and_query)?;
   if path!="/api/v5/account/bills-archive"{continue}
   if step.http_status!=200{return Err(deny("OKX bill cash source HTTP failed"))}
   let doc:serde_json::Value=serde_json::from_str(&step.body)
      .map_err(|_|deny("OKX bill cash JSON failed"))?;
   if doc.get("code").and_then(|v|v.as_str())!=Some("0"){
      return Err(deny("OKX bill cash response code failed"))
   }
   let rows=doc.get("data").and_then(|v|v.as_array())
      .ok_or_else(||deny("OKX bill cash rows absent"))?;
   // The GET query's currency is part of source identity. It cannot be
   // replaced by the response's row.ccy, even when that response is USDT.
   let query=url::Url::parse(&format!("https://review.invalid{}",step.path_and_query))
      .map_err(|_|deny("OKX source cash bill query malformed"))?;
   let requested_ccy=query.query_pairs().find(|(k,_)|k=="ccy")
      .map(|(_,v)|v.into_owned())
      .ok_or_else(||deny("OKX source cash request currency absent"))?;
   for r in rows{
      let ccy=r.get("ccy").and_then(|v|v.as_str())
         .ok_or_else(||deny("OKX source bill currency absent"))?;
      if ccy!=requested_ccy{
         return Err(deny("OKX source bill response currency differs from GET currency"))
      }
      let id=r.get("billId").and_then(|v|v.as_str())
         .ok_or_else(||deny("OKX source billId absent"))?;
      // Base-denominated changes cannot be included in a quote-only net
      // without independent conversion. Existing F2G/H still validate them.
      if ccy!="USDT"{return Err(deny("nonquote OKX bill requires separate valuation"))}
      // The same numeric OKX source billId must have ONE textual identity.
      // A leading-zero alias must not double-book the same economic event.
      if !canonical_okx_history_id(id){
         return Err(deny("noncanonical original OKX source billId"))
      }
      let key=(ccy.to_owned(),id.to_owned());
      if let Some(previous)=okx.insert(key,r.clone()){
         if previous!=*r{return Err(deny("same OKX bill revised across source snapshots"))}
      }
   }
  }
 }
 if okx.is_empty(){return Err(deny("no bounded original OKX quote bills"))}
 let mut identities=Vec::with_capacity(okx.len());
 for ((_,bill_id),row) in &okx{
    let trade=row.get("tradeId").and_then(|v|v.as_str())
      .ok_or_else(||deny("source cash original tradeId absent"))?;
    let order=row.get("ordId").and_then(|v|v.as_str())
      .ok_or_else(||deny("source cash original ordId absent"))?;
    if !canonical_okx_history_id(trade)||!canonical_okx_history_id(order){
       return Err(deny("noncanonical original source cash tradeId/orderId"))
    }
    identities.push(BoundedOkxBillIdentity{
        bill_id:bill_id.clone(),trade_id:trade.into(),order_id:order.into(),
    });
 }
 let mut okx_quote=0i128;
 for row in okx.values(){
  let amount=signed_decimal_to_atoms(row.get("balChg")
     .and_then(|v|v.as_str()).ok_or_else(||deny("OKX balance change absent"))?)
     .map_err(|_|deny("invalid OKX quote bill amount"))?;
  okx_quote=okx_quote.checked_add(amount)
     .ok_or_else(||deny("OKX quote source sum overflow"))?;
 }
 let mut bin=std::collections::BTreeMap::<(String,u64),serde_json::Value>::new();
 for step in income{
  if vetted_path(CaptureLane::BinanceOriginalAndDelayed,&step.path_and_query)?
     !="/fapi/v1/income"||step.http_status!=200{
     return Err(deny("non-income page in Binance bounded money source"))
  }
  let rows:serde_json::Value=serde_json::from_str(&step.body)
     .map_err(|_|deny("Binance income cash JSON failed"))?;
  for row in rows.as_array().ok_or_else(||deny("Binance income cash array absent"))?{
   let kind=row.get("incomeType").and_then(|v|v.as_str())
      .ok_or_else(||deny("Binance income category missing"))?;
   let id=row.get("tranId").and_then(|v|v.as_u64())
      .ok_or_else(||deny("Binance income transaction ID missing"))?;
   if row.get("asset").and_then(|v|v.as_str())!=Some("USDT")
      ||!matches!(kind,"COMMISSION"|"REALIZED_PNL"|"FUNDING_FEE"){
      return Err(deny("unpriced or unrelated Binance account posting"))
   }
   let key=(kind.to_owned(),id);
   if let Some(prior)=bin.insert(key,row.clone()){
      if prior!=*row{return Err(deny("same Binance transaction revised across source windows"))}
   }
  }
 }
 if bin.is_empty(){return Err(deny("missing source Binance cash events"))}
 let(mut fees,mut pnl,mut funding)=(0i128,0i128,0i128);
 for ((kind,_),row) in &bin{
  let val=signed_decimal_to_atoms(row.get("income")
     .and_then(|v|v.as_str()).ok_or_else(||deny("Binance cash posting absent"))?)
     .map_err(|_|deny("Binance cash value overprecision"))?;
  let acc=match kind.as_str(){
    "COMMISSION"=>&mut fees,"REALIZED_PNL"=>&mut pnl,
    "FUNDING_FEE"=>&mut funding,_=>return Err(deny("unknown Binance income kind")),
  };
  *acc=acc.checked_add(val).ok_or_else(||deny("Binance cash source sum overflow"))?;
 }
 let futures=fees.checked_add(pnl).and_then(|n|n.checked_add(funding))
    .ok_or_else(||deny("Binance bounded cash overflow"))?;
 Ok((okx_quote,futures,fees,pnl,funding,okx.len(),bin.len(),identities))
}
/// Bind original B1 bill pages to the EXISTING sealed B2 Native owner.
/// The standalone row parity still cannot authenticate any caller-supplied
/// HTTP bytes, prove exchange-wide completeness or authorize trading.
pub fn require_owner_bound_original_okx_b1_b8_bill_parity_without_network(
 store:&NativeStateStore,generation:u64,first_end:u64,late_end:u64,
 domain:OkxDomain,original_tape:&[MockGetStep],
 native:&[crate::native_trade2f_income_window::ObservedOkxBill],
 b8:&[crate::native_trade2f_receipt_binding::LocalB8OriginalBillShape],
)->Result<(),CaptureError>{
 let plan=crate::native_trade2f3_private_read_plan::inspect_private_history_plan_without_network(
     store,generation,first_end,late_end,domain)
     .map_err(|_|deny("B2 Native owner/source plan not valid"))?;
 let original=store.load().map_err(|_|deny("original Native owner unavailable"))?
     .ok_or_else(||deny("original Native owner missing"))?;
 let entry=original.entry.as_ref().ok_or_else(||deny("original owner entry missing"))?;
 if original.generation!=generation||plan.owner_generation()!=generation{
    return Err(deny("B2 Native owner generation drift"))
 }
 let base=entry.plan.base.as_str();
 let start=entry.plan.created_at_ms;
 let buy=&entry.spot_prehedge_evidence.as_ref()
     .ok_or_else(||deny("original spot BUY owner missing"))?.exact_ledger;
 let close=entry.shadow_close_receipt.as_ref()
     .ok_or_else(||deny("original spot SELL owner missing"))?;
 let sell=&close.bounded_economic_ledgers().0;
 if native.len()!=buy.fills.len()+sell.fills.len(){
    return Err(deny("B2 original spot fills and supplied bills differ in count"))
 }
 for r in native{
   let owner_buy=buy.fills.contains_key(&r.trade_id)
       &&buy.exchange_order_id.as_deref()==Some(r.order_id.as_str());
   let owner_sell=sell.fills.contains_key(&r.trade_id)
       &&sell.exchange_order_id.as_deref()==Some(r.order_id.as_str());
   if owner_buy==owner_sell{
      return Err(deny("original bill trade/order not unique to sealed B2 spot owner"))
   }
 }
 let mut currencies=std::collections::BTreeSet::new();
 for step in original_tape{
   if vetted_path(CaptureLane::OkxOriginal,&step.path_and_query)?
      !="/api/v5/account/bills-archive"{
      return Err(deny("B2 bill tape includes non-bill original source"))
   }
   let url=url::Url::parse(&format!("https://review.invalid{}",step.path_and_query))
      .map_err(|_|deny("B2 original cash GET query invalid"))?;
   let fields:std::collections::BTreeMap<_,_>=url.query_pairs()
      .map(|(k,v)|(k.into_owned(),v.into_owned())).collect();
   let ccy=fields.get("ccy").ok_or_else(||deny("B2 bill currency missing"))?;
   if ccy!=base&&ccy!="USDT"{
      return Err(deny("B2 original account bill currency not owner-scoped"))
   }
   let label=format!("original:{ccy}");
   if plan.slots()[4].selector()!=label&&plan.slots()[5].selector()!=label{
      return Err(deny("B2 original account slot selector differs from GET"))
   }
   if fields.get("begin").and_then(|v|v.parse::<u64>().ok())!=Some(start)
      ||fields.get("end").and_then(|v|v.parse::<u64>().ok())!=Some(first_end)
      ||fields.get("limit").and_then(|v|v.parse::<u16>().ok())
         .map_or(true,|limit|limit==0||limit>plan.slots()[4].page_size()){
      return Err(deny("B1 original bill page window/limit exceeds B2 owner plan"))
   }
   currencies.insert(ccy.to_owned());
 }
 if currencies.len()!=2||!currencies.contains(base)||!currencies.contains("USDT"){
    return Err(deny("B2 original BASE/USDT source slots incomplete"))
 }
 require_original_okx_b1_b8_bill_parity_without_network(base,original_tape,native,b8)?;
 if store.load().map_err(|_|deny("B2 owner unavailable after original page check"))?
    .as_ref()!=Some(&original){
    return Err(deny("B2 owner changed during original bill source verification"))
 }
 Ok(())
}

/// Reuse F2G's TWO complete signed-MOCK OKX scans and sealed B2 owner.
/// Map exactly the first validated read into B6 BUY/SELL fill lanes;
/// the second F2G verification scan is never a duplicate set of trades.
#[cfg(feature="offline-b6-review")]
pub fn original_okx_b1_b2_fills_as_b6_review(
 store:&NativeStateStore,generation:u64,first_end:u64,late_end:u64,
 domain:OkxDomain,tape:&[MockGetStep],claimed_okx_account_fingerprint:&str,
)->Result<Vec<b6_catalogue::Page>,CaptureError>{
 use b6_catalogue::{Lane,Page,SourceRow};
 let first_state=store.load().map_err(|_|deny("B6 OKX owner unavailable"))?
    .ok_or_else(||deny("B6 OKX owner missing"))?;
 let validated=crate::native_trade2f_okx_owner_mock::inspect_owner_bound_okx_offline_mock(
    store,generation,first_end,domain,tape)
    .map_err(|_|deny("B6 source not validated by F2G owner and double GET"))?;
 if validated.original_owner_generation!=generation||
    !validated.two_full_identical_mock_scans||validated.real_ready||
    validated.independent_real_source_attested||validated.verified_closed{
    return Err(deny("B6 original OKX source claimed unsupported authority"))
 }
 if tape.len()<4||tape.len()%2!=0{
    return Err(deny("B6 original OKX tape has no two complete scans"))
 }
 let split=tape.len()/2;
 if tape[..split]!=tape[split..]{
    return Err(deny("B6 original OKX page bytes differ across scans"))
 }
 let valid_fp=claimed_okx_account_fingerprint.len()==64&&
    claimed_okx_account_fingerprint.bytes().all(|b|b.is_ascii_digit()||
      (b'a'..=b'f').contains(&b))&&
    claimed_okx_account_fingerprint.bytes().any(|b|b!=b'0');
 if !valid_fp{return Err(deny("B6 OKX account fingerprint is invalid claim"))}
 let plan=crate::native_trade2f3_private_read_plan::inspect_private_history_plan_without_network(
    store,generation,first_end,late_end,domain)
    .map_err(|_|deny("B6 original B2 owner plan absent"))?;
 let slots=plan.slots();
 let first_state_entry=first_state.entry.as_ref()
    .ok_or_else(||deny("B6 original OKX owner entry absent"))?;
 let mut indexes=[0usize;2];
 let mut pages=Vec::new();
 let mut bills_started=false;
 for step in &tape[..split]{
  let path=vetted_path(CaptureLane::OkxOriginal,&step.path_and_query)?;
  if path=="/api/v5/account/bills-archive"{
    bills_started=true;
    continue
  }
  if path!="/api/v5/trade/fills-history"||bills_started{
    return Err(deny("B6 F2G original spot fill pages out of scan order"))
  }
  let url=url::Url::parse(&format!("https://review.invalid{}",step.path_and_query))
    .map_err(|_|deny("B6 OKX fills-history URL invalid"))?;
  let params:std::collections::BTreeMap<_,_>=url.query_pairs()
    .map(|(k,v)|(k.into_owned(),v.into_owned())).collect();
  let order=params.get("ordId").ok_or_else(||deny("B6 original spot order absent"))?;
  let stage=if order==slots[0].selector(){0}
    else if order==slots[1].selector(){1}
    else{return Err(deny("B6 spot source order not sealed in B2"))};
  let owner_okx_symbol=first_state_entry.plan.spot_symbol.replace('/',"-");
  if params.get("instId").map(String::as_str)!=Some(owner_okx_symbol.as_str()){
    return Err(deny("B6 original spot source symbol not sealed Native plan"))
  }
  let limit=params.get("limit").and_then(|v|v.parse::<usize>().ok())
    .ok_or_else(||deny("B6 original spot page limit absent"))?;
  let doc:serde_json::Value=serde_json::from_str(&step.body)
    .map_err(|_|deny("B6 F2G fill JSON invalid"))?;
  if doc.get("code").and_then(|v|v.as_str())!=Some("0"){
    return Err(deny("B6 F2G spot GET status not successful"))
  }
  let records=doc.get("data").and_then(|v|v.as_array())
    .ok_or_else(||deny("B6 F2G fill data absent"))?;
  let rows=records.iter().map(|r|{
    let field=|k|r.get(k).and_then(|v|v.as_str());
    if field("instId")!=params.get("instId").map(String::as_str)
      ||field("ordId")!=Some(order.as_str()){
      return Err(deny("B6 original F2G fill row not in sealed owner order"))
    }
    Ok(SourceRow{
      unique_key:field("tradeId")
        .ok_or_else(||deny("B6 original source tradeId absent"))?.into(),
      order_id:Some(order.to_owned()),
      symbol:Some(params["instId"].clone()),
    })
  }).collect::<Result<Vec<_>,CaptureError>>()?;
  // OKX fills-history continues with last billId, NOT the B6 tradeId key.
  // F2G demands an empty terminal page even when a previous page was short.
  let cursor_out=if records.is_empty(){None}else{
    Some(records.last().and_then(|r|r.get("billId"))
      .and_then(|v|v.as_str())
      .ok_or_else(||deny("B6 OKX fill cursor billId absent"))?.to_owned())
  };
  let index=indexes[stage];
  pages.push(Page{
    lane:if stage==0{Lane::OkxBuy}else{Lane::OkxSell},
    generation,native_state_digest:plan.owner_sha256().into(),
    account_fingerprint:claimed_okx_account_fingerprint.into(),
    endpoint:"/api/v5/trade/fills-history".into(),
    order_id:Some(order.to_owned()),symbol:Some(params["instId"].clone()),
    from_ms:first_state_entry.plan.created_at_ms,to_ms:first_end,
    index,cursor_in:params.get("after").cloned(),cursor_out,
    last_page:records.is_empty(),requested_limit:limit,
    http_status:step.http_status,business_success:true,
    body_sha256:hash(step.body.as_bytes()),rows,
  });
  indexes[stage]+=1;
 }
 if indexes.iter().any(|n|*n==0)||
    store.load().map_err(|_|deny("B6 original spot owner unavailable afterward"))?
       .as_ref()!=Some(&first_state){
   return Err(deny("B6 original spot owner changed or source lane absent"))
 }
 Ok(pages)
}

/// Map ONLY the first original Binance income scan proven by F2I into B6.
/// F2I's later scans are widened snapshots, NOT independent late-only source
/// pages. Refuse to call their original rows fresh delayed income receipts.
#[cfg(feature="offline-b6-review")]
pub fn original_binance_income_b1_b2_as_b6_review(
 store:&NativeStateStore,generation:u64,first_end:u64,late_end:u64,
 domain:OkxDomain,tape:&[MockGetStep],claimed_binance_account_fingerprint:&str,
)->Result<Vec<b6_catalogue::Page>,CaptureError>{
 use b6_catalogue::{Lane,Page,SourceRow};
 let initial=store.load().map_err(|_|deny("B6 income Native owner unavailable"))?
     .ok_or_else(||deny("B6 income Native owner missing"))?;
 let proof=crate::native_trade2f_binance_late_income::inspect_owner_bound_binance_late_income_offline(
    store,generation,first_end,late_end,tape)
    .map_err(|_|deny("B6 income source did not pass complete F2I owner validator"))?;
 if proof.native_owner_generation!=generation||
    !proof.two_original_and_two_late_mock_scans_stable||
    proof.source_income_rows==0||proof.real_ready||proof.verified_closed{
    return Err(deny("B6 Binance income F2I review not bounded and offline"))
 }
 let valid_fp=claimed_binance_account_fingerprint.len()==64&&
   claimed_binance_account_fingerprint.bytes().all(|b|b.is_ascii_digit()||
       (b'a'..=b'f').contains(&b))&&
   claimed_binance_account_fingerprint.bytes().any(|b|b!=b'0');
 if !valid_fp{return Err(deny("B6 Binance income account fingerprint invalid claim"))}
 let plan=crate::native_trade2f3_private_read_plan::inspect_private_history_plan_without_network(
    store,generation,first_end,late_end,domain)
    .map_err(|_|deny("B6 income B2 owner plan unavailable"))?;
 if plan.slots()[8].endpoint()!="/fapi/v1/income"{
    return Err(deny("B6 income original owner slot differs from source"))
 }
 let start=initial.entry.as_ref().ok_or_else(||deny("B6 income entry absent"))?
     .plan.created_at_ms;
 let mut pages=Vec::new();
 let mut ended=false;
 for step in tape{
    if ended{break}
    if vetted_path(CaptureLane::BinanceOriginalAndDelayed,&step.path_and_query)?
        !="/fapi/v1/income"{
       return Err(deny("B6 income original source endpoint unexpected"))
    }
    let url=url::Url::parse(&format!("https://review.invalid{}",step.path_and_query))
       .map_err(|_|deny("B6 original Binance income query invalid"))?;
    let q:std::collections::BTreeMap<_,_>=url.query_pairs()
       .map(|(k,v)|(k.into_owned(),v.into_owned())).collect();
    let number=|k:&str|q.get(k).and_then(|v|v.parse::<u64>().ok());
    let index=pages.len();
    if number("startTime")!=Some(start)||number("endTime")!=Some(first_end)
       ||number("page")!=Some(index as u64+1)
       ||number("limit")!=Some(plan.slots()[8].page_size() as u64){
       return Err(deny("B6 original Binance income GET not in exact B2 scope"))
    }
    let rows:Vec<serde_json::Value>=serde_json::from_str(&step.body)
       .map_err(|_|deny("B6 source income rows malformed"))?;
    if rows.len()>plan.slots()[8].page_size() as usize{
       return Err(deny("B6 income page exceeds original B2 source bound"))
    }
    let last_page=rows.is_empty();
    let mut source_rows=Vec::with_capacity(rows.len());
    for v in &rows{
       let kind=v.get("incomeType").and_then(|v|v.as_str())
           .ok_or_else(||deny("B6 Binance income type missing"))?;
       let tx=v.get("tranId").and_then(|v|v.as_u64()
           .or_else(||v.as_str().and_then(|s|s.parse::<u64>().ok())))
           .ok_or_else(||deny("B6 Binance original tranId absent"))?;
       if tx==0{return Err(deny("B6 Binance original tranId zero"))}
       source_rows.push(SourceRow{unique_key:format!("{kind}:{tx}"),
          order_id:None,symbol:None});
    }
    pages.push(Page{
      lane:Lane::BinanceIncomeOriginal,generation,
      native_state_digest:plan.owner_sha256().into(),
      account_fingerprint:claimed_binance_account_fingerprint.into(),
      endpoint:"/fapi/v1/income".into(),order_id:None,symbol:None,
      from_ms:start,to_ms:first_end,index,
      cursor_in:if index==0{None}else{Some((index+1).to_string())},
      cursor_out:if last_page{None}else{Some((index+2).to_string())},
      last_page,requested_limit:plan.slots()[8].page_size() as usize,
      http_status:step.http_status,business_success:true,
      body_sha256:hash(step.body.as_bytes()),rows:source_rows,
    });
    ended=last_page;
 }
 if !ended||pages.is_empty()||pages.len()>4{
    return Err(deny("B6 original Binance income scan did not terminate"))
 }
 let n=pages.len();
 if tape.len()<n*4||tape[..n]!=tape[n..2*n]{
    return Err(deny("B6 original Binance income reread bytes differ"))
 }
 if store.load().map_err(|_|deny("B6 Binance income owner unavailable afterward"))?
      .as_ref()!=Some(&initial){
    return Err(deny("B6 Binance income original owner changed while mapping"))
 }
 Ok(pages)
}

/// Existing B1/B2 signed-MOCK Binance userTrades -> existing B6 typed pages.
/// Offline, original open/cover slots ONLY. Do not treat supplied identity
/// fingerprints or page hashes as authenticated exchange account evidence.
#[cfg(feature="offline-b6-review")]
pub fn original_binance_b1_b2_fills_as_b6_review(
 store:&NativeStateStore,generation:u64,first_end:u64,late_end:u64,
 domain:OkxDomain,okx_initial:&[MockGetStep],okx_late:&[MockGetStep],
 binance:&[MockGetStep],claimed_binance_account_fingerprint:&str,
)->Result<Vec<b6_catalogue::Page>,CaptureError>{
 use b6_catalogue::{Page,SourceRow,Lane};
 // Reuse existing owner-bound F2G/B1/B2 source validator; never substitute
 // an independently reconstructed owner or a second fills-history parser.
 let validated=inspect_owner_bound_original_futures_pages_without_network(
    store,generation,first_end,late_end,domain,okx_initial,okx_late,binance)?;
 let valid_fingerprint=|s:&str|s.len()==64&&s.bytes().all(|b|
     b.is_ascii_digit()||(b'a'..=b'f').contains(&b))&&
     s.bytes().any(|b|b!=b'0');
 if !valid_fingerprint(claimed_binance_account_fingerprint){
    return Err(deny("B6 Binance account fingerprint claim malformed"))
 }
 let plan=crate::native_trade2f3_private_read_plan::inspect_private_history_plan_without_network(
    store,generation,first_end,late_end,domain)
    .map_err(|_|deny("B6 Binance original owner plan unavailable"))?;
 if plan.owner_sha256()!=validated.owner_sha256{
    return Err(deny("B6 Binance B2 owner digest differs from validated source"))
 }
 let initial=store.load().map_err(|_|deny("B6 Binance owner absent"))?
    .ok_or_else(||deny("B6 Binance owner has no state"))?;
 let start=initial.entry.as_ref().ok_or_else(||deny("B6 Binance owner has no entry"))?
    .plan.created_at_ms;
 let slots=plan.slots();
 let mut indices=[0usize;2];
 let mut out=Vec::with_capacity(binance.len());
 for step in binance{
  if vetted_path(CaptureLane::BinanceOriginalAndDelayed,&step.path_and_query)?
      !="/fapi/v1/userTrades"{
    return Err(deny("B6 original futures source tape contains non-userTrades GET"))
  }
  let url=url::Url::parse(&format!("https://review.invalid{}",step.path_and_query))
    .map_err(|_|deny("B6 futures source request malformed"))?;
  let params:std::collections::BTreeMap<_,_>=url.query_pairs()
    .map(|(k,v)|(k.into_owned(),v.into_owned())).collect();
  let order=params.get("orderId").ok_or_else(||deny("B6 futures order not supplied"))?;
  let stage=if order==slots[2].selector(){0}
    else if order==slots[3].selector(){1}
    else{return Err(deny("B6 futures source order not owned by B2"))};
  let symbol=params.get("symbol").ok_or_else(||deny("B6 futures symbol missing"))?;
  if Some(symbol.as_str())!=slots[stage+2].query_symbol(){
    return Err(deny("B6 futures symbol differs from sealed B2 slot"))
  }
  let limit=params.get("limit").and_then(|s|s.parse::<usize>().ok())
    .ok_or_else(||deny("B6 futures page limit absent"))?;
  let from=params.get("fromId").and_then(|s|s.parse::<u64>().ok())
    .ok_or_else(||deny("B6 futures fromId absent"))?;
  let records:Vec<serde_json::Value>=serde_json::from_str(&step.body)
    .map_err(|_|deny("B6 futures source rows invalid"))?;
  let rows=records.iter().map(|v|{
    let id=v.get("id").and_then(|n|n.as_u64())
       .ok_or_else(||deny("B6 original futures trade ID invalid"))?;
    if v.get("orderId").and_then(|n|n.as_u64())
         .map(|n|n.to_string())!=Some(order.clone())
      ||v.get("symbol").and_then(|n|n.as_str())!=Some(symbol){
      return Err(deny("B6 futures row differs from original B2 order/symbol"))
    }
    Ok(SourceRow{unique_key:id.to_string(),order_id:Some(order.clone()),
                 symbol:Some(symbol.clone())})
  }).collect::<Result<Vec<_>,CaptureError>>()?;
  let last_page=rows.len()<limit;
  let cursor_out=if last_page{None}else{
    rows.last().and_then(|r|r.unique_key.parse::<u64>().ok())
      .and_then(|id|id.checked_add(1)).map(|id|id.to_string())
  };
  if !last_page&&cursor_out.is_none(){
     return Err(deny("B6 original futures cursor overflow"))
  }
  let index=indices[stage];
  out.push(Page{
    lane:if stage==0{Lane::BinanceShortOpen}else{Lane::BinanceShortClose},
    generation,native_state_digest:validated.owner_sha256.clone(),
    account_fingerprint:claimed_binance_account_fingerprint.into(),
    endpoint:"/fapi/v1/userTrades".into(),order_id:Some(order.clone()),
    symbol:Some(symbol.clone()),from_ms:start,to_ms:first_end,
    index,cursor_in:if index==0{None}else{Some(from.to_string())},
    cursor_out,last_page,requested_limit:limit,http_status:step.http_status,
    business_success:true,body_sha256:hash(step.body.as_bytes()),rows,
  });
  indices[stage]+=1;
 }
 if out.len()!=validated.original_open_pages+validated.original_close_pages
   ||indices!=[validated.original_open_pages,validated.original_close_pages]
   ||store.load().map_err(|_|deny("B6 futures owner changed after mapping"))?
      .as_ref()!=Some(&initial){
    return Err(deny("B6 original futures typed pages differ from B1/B2 owner"))
 }
 Ok(out)
}

/// Optional review adapter into the EXACT existing B6 Page/SourceRow model.
/// Caller-supplied account fingerprint is a local CLAIM, never an attestation.
/// Only original BASE/QUOTE bill lanes are populated; eight other B6 lanes
/// still need separately sourced pages. No synthetic cash or billId created.
#[cfg(feature="offline-b6-review")]
pub fn original_b1_b2_bill_pages_as_b6_review(
 store:&NativeStateStore,generation:u64,first_end:u64,late_end:u64,
 domain:OkxDomain,tape:&[MockGetStep],
 native:&[crate::native_trade2f_income_window::ObservedOkxBill],
 b8:&[crate::native_trade2f_receipt_binding::LocalB8OriginalBillShape],
 claimed_okx_account_fingerprint:&str,
)->Result<Vec<b6_catalogue::Page>,CaptureError>{
 use b6_catalogue::{Page,SourceRow,Lane};
 require_owner_bound_original_okx_b1_b8_bill_parity_without_network(
   store,generation,first_end,late_end,domain,tape,native,b8)?;
 if claimed_okx_account_fingerprint.len()!=64||
   !claimed_okx_account_fingerprint.bytes().all(|b|b.is_ascii_digit()||
      (b'a'..=b'f').contains(&b))||
   claimed_okx_account_fingerprint.bytes().all(|b|b==b'0'){
    return Err(deny("B6 untrusted account fingerprint not canonical SHA256 shape"))
 }
 let plan=crate::native_trade2f3_private_read_plan::inspect_private_history_plan_without_network(
   store,generation,first_end,late_end,domain)
   .map_err(|_|deny("B6 owner plan no longer valid"))?;
 let owner=store.load().map_err(|_|deny("B6 original owner missing"))?
   .ok_or_else(||deny("B6 original owner absent"))?;
 let entry=owner.entry.as_ref().ok_or_else(||deny("B6 owner entry absent"))?;
 let base=entry.plan.base.as_str();
 let symbol=entry.plan.spot_symbol.replace('/',"-");
 let mut page_index=std::collections::BTreeMap::<String,usize>::new();
 let mut output=Vec::with_capacity(tape.len());
 for step in tape{
  let query=url::Url::parse(&format!("https://review.invalid{}",step.path_and_query))
    .map_err(|_|deny("B6 raw original bill URL malformed"))?;
  let params:std::collections::BTreeMap<_,_>=query.query_pairs()
    .map(|(k,v)|(k.into_owned(),v.into_owned())).collect();
  let ccy=params.get("ccy").ok_or_else(||deny("B6 original bill ccy absent"))?;
  let lane=if ccy==base{Lane::OkxBaseOriginal}
       else if ccy=="USDT"{Lane::OkxQuoteOriginal}
       else{return Err(deny("B6 original bill currency outside owner scope"))};
  let limit=params.get("limit").and_then(|s|s.parse::<usize>().ok())
    .ok_or_else(||deny("B6 original page limit absent"))?;
  let doc:serde_json::Value=serde_json::from_str(&step.body)
    .map_err(|_|deny("B6 original bill JSON malformed"))?;
  let rows=doc.get("data").and_then(|r|r.as_array())
    .ok_or_else(||deny("B6 original bill rows missing"))?;
  let source_rows=rows.iter().map(|r|{
    let field=|k|r.get(k).and_then(|v|v.as_str());
    if field("instId")!=Some(symbol.as_str()){
       return Err(deny("B6 original source bill symbol not B2 spot owner"))
    }
    Ok(SourceRow{
      unique_key:field("billId").ok_or_else(||deny("B6 source billId absent"))?.into(),
      order_id:Some(field("ordId").ok_or_else(||deny("B6 bill ordId absent"))?.into()),
      symbol:Some(symbol.clone()),
    })
  }).collect::<Result<Vec<_>,CaptureError>>()?;
  let index=page_index.entry(ccy.clone()).or_default();
  let last_page=rows.len()<limit;
  let cursor_out=if last_page{None}else{
    source_rows.last().map(|r|r.unique_key.clone())
  };
  output.push(Page{
    lane,generation,native_state_digest:plan.owner_sha256().into(),
    account_fingerprint:claimed_okx_account_fingerprint.into(),
    endpoint:"/api/v5/account/bills-archive".into(),
    order_id:None,symbol:None,
    from_ms:entry.plan.created_at_ms,to_ms:first_end,index:*index,
    cursor_in:params.get("after").cloned(),cursor_out,last_page,
    requested_limit:limit,http_status:step.http_status,business_success:true,
    body_sha256:hash(step.body.as_bytes()),rows:source_rows,
  });
  *index+=1;
 }
 if store.load().map_err(|_|deny("B6 owner changed after mapping"))?
    .as_ref()!=Some(&owner){
   return Err(deny("B6 source owner changed while creating local pages"))
 }
 Ok(output)
}

/// Narrow offline B1 original bills -> existing Native receipts -> B8-shaped
/// bill rows. Caller-supplied bytes are NOT exchange authenticated, and this
/// does not certify the absence of additional exchange pages or late bills.
pub fn require_original_okx_b1_b8_bill_parity_without_network(
 base_asset:&str,original_tape:&[MockGetStep],
 native:&[crate::native_trade2f_income_window::ObservedOkxBill],
 b8:&[crate::native_trade2f_receipt_binding::LocalB8OriginalBillShape],
)->Result<(),CaptureError>{
 crate::native_trade2f_receipt_binding::require_b8_original_bill_parity_without_network(
    base_asset,native,b8).map_err(|_|deny("Native original bills differ from B8"))?;
 if original_tape.is_empty()||original_tape.len()>LIMIT_PER_LANE{
    return Err(deny("B1 original OKX source page count missing or unbounded"))
 }
 let expected:std::collections::BTreeMap<&str,_>=native.iter()
     .map(|x|(x.bill_id.as_str(),x)).collect();
 let mut seen=std::collections::BTreeSet::new();
 let mut pages=0usize;
 // Each currency belongs to ONE bounded B2 original window. Track
 // page continuity separately so a foreign cursor cannot skip originals.
 let mut scope:Option<(u64,u64,usize)>=None;
 let mut cursors=std::collections::BTreeMap::<String,(Option<String>,bool,usize)>::new();
 for step in original_tape{
    let path=vetted_path(CaptureLane::OkxOriginal,&step.path_and_query)?;
    if path!="/api/v5/account/bills-archive"{continue}
    pages+=1;
    if step.http_status!=200||step.body.is_empty()||step.body.len()>LIMIT_BODY{
       return Err(deny("B1 original bill HTTP/body not bounded"))
    }
    let url=url::Url::parse(&format!("https://review.invalid{}",step.path_and_query))
       .map_err(|_|deny("B1 original bill query malformed"))?;
    let requested=url.query_pairs().find(|(k,_)|k=="ccy")
       .map(|(_,v)|v.into_owned()).ok_or_else(||deny("original bill query ccy absent"))?;
    if requested!=base_asset&&requested!="USDT"{
       return Err(deny("original bill GET foreign currency"))
    }
    let params:std::collections::BTreeMap<_,_>=url.query_pairs()
       .map(|(k,v)|(k.into_owned(),v.into_owned())).collect();
    let number=|k:&str|params.get(k).and_then(|v|v.parse::<u64>().ok());
    let (begin,end,limit)=(number("begin"),number("end"),number("limit"));
    let (Some(begin),Some(end),Some(limit))=(begin,end,limit) else{
       return Err(deny("original bill window or limit missing"))
    };
    if begin==0||end<begin||limit==0||limit>100{
       return Err(deny("original bill window or page limit invalid"))
    }
    let window=(begin,end,limit as usize);
    if scope.is_some_and(|prior|prior!=window){
       return Err(deny("original BASE/USDT B2 page window or limit drift"))
    }
    scope=Some(window);
    let prev=cursors.get(&requested);
    let after=params.get("after").map(String::as_str);
    if match prev{None=>after.is_some(),Some((last,full,_))=>
        !*full||after!=last.as_deref()}{
       return Err(deny("original OKX after cursor not chained to prior full page"))
    }
    let doc:serde_json::Value=serde_json::from_str(&step.body)
       .map_err(|_|deny("original bill source JSON invalid"))?;
    if doc.get("code").and_then(|v|v.as_str())!=Some("0"){
       return Err(deny("original bill source non-success response"))
    }
    let rows=doc.get("data").and_then(|v|v.as_array())
       .ok_or_else(||deny("original bill source rows absent"))?;
    if rows.len()>limit as usize{
       return Err(deny("original B2 OKX bill page larger than requested limit"))
    }
    for row in rows{
       let field=|name|row.get(name).and_then(|v|v.as_str());
       let id=field("billId").ok_or_else(||deny("raw billId missing"))?;
       if !canonical_okx_history_id(id)||!seen.insert(id.to_owned()){
          return Err(deny("raw original OKX billId malformed or replayed"))
       }
       let receipt=expected.get(id).ok_or_else(||deny("raw bill not in Native original owner"))?;
       let currency=field("ccy").ok_or_else(||deny("raw original bill ccy missing"))?;
       if currency!=requested||currency!=receipt.currency{
          return Err(deny("raw bill query currency differs from Native original"))
       }
       if receipt.at_ms<begin||receipt.at_ms>end{
          return Err(deny("Native original bill timestamp outside requested B2 window"))
       }
       let amount=signed_decimal_to_atoms(field("balChg")
          .ok_or_else(||deny("raw bill signed amount missing"))?)
          .map_err(|_|deny("raw bill signed amount invalid"))?;
       if amount!=receipt.signed_change_e12||field("tradeId")!=Some(receipt.trade_id.as_str())
          ||field("ordId")!=Some(receipt.order_id.as_str()){
          return Err(deny("raw OKX original amount/trade/order differs from Native"))
       }
    }
    let count=prev.map_or(1,|(_,_,n)|n+1);
    if count>4{return Err(deny("original OKX B2 slot exceeds four pages"))}
    let last=rows.last().and_then(|r|r.get("billId"))
       .and_then(|v|v.as_str()).map(str::to_owned);
    cursors.insert(requested,(last,rows.len()==limit as usize,count));
 }
 if cursors.values().any(|(_,full,_)|*full){
    return Err(deny("last original bill page full: source pagination unproven"))
 }
 if pages==0||seen.len()!=expected.len(){
    return Err(deny("B1 original OKX bills omit Native/B8 original source"))
 }
 Ok(())
}

/// The original F2G/H/I and exact Binance userTrades identity/fee validators
/// run FIRST. Then compare separately deduplicated, literal exchange-shaped
/// cash postings to the existing SEALED Native bounded-economics report.
pub fn inspect_b1_b2_source_bound_bounded_cash_without_network(
 store:&NativeStateStore,generation:u64,first_end:u64,late_end:u64,
 domain:OkxDomain,okx_initial:&[MockGetStep],okx_late:&[MockGetStep],
 income:&[MockGetStep],futures:&[MockGetStep],
)->Result<BoundedSourceCashBridge,CaptureError>{
 let checked=require_original_futures_exact_economics_without_network(
    store,generation,first_end,late_end,domain,okx_initial,okx_late,income,futures)?;
 if !checked.ten_slot_source_page_counts.iter().all(|n|*n>0)
    ||checked.exchange_authenticated||checked.can_mark_verified_closed_or_trade{
    return Err(deny("bounded source cash received incomplete or overclaimed source proof"))
 }
 // The full NativeState stays OUT of the deeply nested B1/B2/F2G/H/I
 // validator's stack. The cash phase loads the owner only after its frame
 // has returned; no test-only stack expansion is needed.
 inspect_source_cash_against_sealed_finance(store,generation,okx_initial,okx_late,income)
}
fn inspect_source_cash_against_sealed_finance(
 store:&NativeStateStore,generation:u64,
 okx_initial:&[MockGetStep],okx_late:&[MockGetStep],
 income:&[MockGetStep],
)->Result<BoundedSourceCashBridge,CaptureError>{
 let (quote,future,commission,realized,funding,bill_count,income_count,identities)=
    source_bounded_money_rows(okx_initial,okx_late,income)?;
 let state=store.load().map_err(|_|deny("bounded cash Native owner unreadable"))?
    .ok_or_else(||deny("bounded cash Native owner absent"))?;
 if state.generation!=generation{return Err(deny("bounded cash Native owner generation changed"))}
 let entry=state.entry.as_ref().ok_or_else(||deny("bounded cash owner entry absent"))?;
 let gap=entry.shadow_lifetime_economics.as_ref()
    .ok_or_else(||deny("bounded Native original finance gap is absent"))?;
 let first=&entry.spot_prehedge_evidence.as_ref()
    .ok_or_else(||deny("bounded original spot open absent"))?.exact_ledger;
 let close=entry.shadow_close_receipt.as_ref()
    .ok_or_else(||deny("bounded original close absent"))?;
 let sold=close.bounded_economic_ledgers().0;
 require_bounded_cash_original_owner(first,sold,
    &entry.spot_prehedge_evidence.as_ref()
       .ok_or_else(||deny("opening Native first-leg bill lineage missing"))?
       .fill_bill_ids,&identities)?;
 let expected_quote={
   let buy=i128::try_from(first.quote_notional_atoms()
      .ok_or_else(||deny("original spot quote missing"))?)
      .map_err(|_|deny("original spot quote overflow"))?;
   let sell=i128::try_from(sold.quote_notional_atoms()
      .ok_or_else(||deny("closing spot quote missing"))?)
      .map_err(|_|deny("closing spot quote overflow"))?;
   sell.checked_sub(buy).and_then(|n|
      n.checked_sub(first.fee_cost_quote_atoms)).and_then(|n|
      n.checked_sub(sold.fee_cost_quote_atoms))
      .ok_or_else(||deny("spot quote cashflow overflow"))?
 };
 let future_open=&entry.recovery_checkpoint.as_ref()
    .ok_or_else(||deny("original futures open absent"))?.ledger;
 let future_close=close.bounded_economic_ledgers().1;
 let futures_fees=future_open.fee_cost_quote_atoms
    .checked_add(future_close.fee_cost_quote_atoms)
    .ok_or_else(||deny("futures fee overflow"))?;
 let expected_fees=i128::try_from(futures_fees)
    .ok().and_then(|x|x.checked_neg())
    .ok_or_else(||deny("futures fee sign or cast overflow"))?;
 if quote!=expected_quote||commission!=expected_fees
    ||realized!=gap.recomputed_futures_close_pnl_e12()
    ||funding!=gap.bounded_funding_e12(){
    return Err(deny("source cash postings disagree with sealed four-leg economics"))
 }
 let net=quote.checked_add(future).ok_or_else(||deny("bounded source cash total overflow"))?;
 if net!=gap.observed_window_net_e12() || gap.authoritative_net_pnl_known(){
    return Err(deny("cash-source-derived net disagrees with Native bounded net or overclaims authority"))
 }
 if store.load().map_err(|_|deny("Native owner unavailable after source cash"))?
    .as_ref()!=Some(&state){
    return Err(deny("Native owner changed during bounded source cash review"))
 }
 Ok(BoundedSourceCashBridge{
  original_owner_generation:generation,
  unique_okx_usdt_bills:bill_count,
  unique_binance_income_postings:income_count,
  deduplicated_okx_quote_delta_atoms_e12:quote,
  deduplicated_binance_money_delta_atoms_e12:future,
  bounded_source_net_atoms_e12:net,
  equals_persisted_bounded_native_net:true,
  independently_authenticated_account:false,
  complete_lifetime_profit:false,
  may_trade_or_verify_closed:false,
 })
}

#[cfg(test)]
mod bounded_cash_source_dedup_tests{
 use super::*;
 #[test]
 fn p104_cash_bridge_cannot_double_book_noncanonical_alias_of_original_billid(){
   // "301" and "0301" are numerically the SAME exchange bill,
   // so they must never enter the bounded local ledger as two sources.
   let step=MockGetStep{
     path_and_query:"/api/v5/account/bills-archive?ccy=USDT&begin=1&end=2&limit=100".into(),
     http_status:200,
     body:serde_json::json!({"code":"0","data":[{
       "billId":"301","ccy":"USDT","balChg":"-9.009",
       "ordId":"201","tradeId":"9001"
     }]}).to_string(),
   };
   let mut alias=step.clone();
   let mut forged:serde_json::Value=serde_json::from_str(&alias.body).unwrap();
   forged["data"][0]["billId"]=serde_json::json!("0301");
   alias.body=forged.to_string();
   let income=MockGetStep{
     path_and_query:format!("/fapi/v1/income?startTime=1&endTime=2&page=1&limit=1000&timestamp=2&recvWindow=10000&signature={}",
       "a".repeat(64)),
     http_status:200,
     body:serde_json::json!([{"incomeType":"COMMISSION","tranId":310,
       "asset":"USDT","income":"-0.002"}]).to_string(),
   };
   assert!(source_bounded_money_rows(&[step],&[alias],&[income]).is_err(),
     "same original OKX billId aliased as leading-zero ID was counted twice");
 }
 #[test]
 fn p104_bounded_cash_rejects_bill_without_original_trade_and_order_identity(){
   let bill=MockGetStep{
     path_and_query:"/api/v5/account/bills-archive?ccy=USDT&begin=1&end=2&limit=100".into(),
     http_status:200,
     body:serde_json::json!({"code":"0","data":[{
       "billId":"301","ccy":"USDT","balChg":"-9.009"
     }]}).to_string(),
   };
   let income=MockGetStep{
     path_and_query:format!("/fapi/v1/income?startTime=1&endTime=2&page=1&limit=1000&timestamp=2&recvWindow=10000&signature={}",
         "a".repeat(64)),http_status:200,
     body:serde_json::json!([{
         "incomeType":"COMMISSION","tranId":310,"asset":"USDT","income":"-0.002"
     }]).to_string(),
   };
   assert!(source_bounded_money_rows(&[bill],&[],&[income]).is_err(),
      "bill cash source with no original tradeId/ordId entered bounded net");
 }
 #[test]
 fn p104_bounded_cash_per_bill_owner_parity_denies_trade_and_order_grafts(){
   use crate::fill_economics::FillEvent;
   let mk=|side:&str,ord:&str,trade:&str|{
     let mut x=LegFillLedger::new("okx","BNT-USDT",side,ord,ord,1).unwrap();
     x.exchange_order_id=Some(ord.into());
     x.fills.insert(trade.into(),FillEvent{
       fill_id:trade.into(),qty_atoms:1,price_atoms:1,
       fee_currency:Some("USDT".into()),fee_cost_quote_atoms:Some(0),
       trade_time_ms:Some(100),
     });x
   };
   let buy=mk("buy","201","9001");
   let sell=mk("sell","901","9002");
   let original_bills=vec!["301".to_string()];
   let actual=vec![
     BoundedOkxBillIdentity{bill_id:"301".into(),trade_id:"9001".into(),order_id:"201".into()},
     BoundedOkxBillIdentity{bill_id:"302".into(),trade_id:"9002".into(),order_id:"901".into()},
   ];
   assert!(require_bounded_cash_original_owner(&buy,&sell,&original_bills,&actual).is_ok());
   let mut graft=actual.clone();
   graft[0].bill_id="302".into();
   graft[1].bill_id="301".into();
   assert!(require_bounded_cash_original_owner(&buy,&sell,&original_bills,&graft).is_err(),
      "cash posting grafted to another tradeId despite matching totals");
   let mut foreign=actual.clone(); foreign[0].trade_id="9999".into();
   assert!(require_bounded_cash_original_owner(&buy,&sell,&original_bills,&foreign).is_err());
   let mut order=actual.clone(); order[1].order_id="201".into();
   assert!(require_bounded_cash_original_owner(&buy,&sell,&original_bills,&order).is_err());
   let mut duplicate=actual.clone();duplicate[1].trade_id="9001".into();
   assert!(require_bounded_cash_original_owner(&buy,&sell,&original_bills,&duplicate).is_err());
   assert!(require_bounded_cash_original_owner(&buy,&sell,&original_bills,&actual[0..1]).is_err());
 }
 #[test]
 fn p104_quote_cash_bill_cannot_be_read_from_base_currency_query(){
   // Wrong query currency with otherwise valid bill money and owner IDs.
   // This helper must NOT treat ccy=BNT request body as USDT source truth.
   let b=MockGetStep{
     path_and_query:"/api/v5/account/bills-archive?ccy=BNT&begin=1&end=2&limit=100".into(),
     http_status:200,
     body:serde_json::json!({"code":"0","data":[{
        "billId":"301","ccy":"USDT","ordId":"201","tradeId":"9001",
        "balChg":"-9.009"
     }]}).to_string(),
   };
   let income=MockGetStep{
     path_and_query:format!("/fapi/v1/income?startTime=1&endTime=2&page=1&limit=1000&timestamp=2&recvWindow=10000&signature={}",
        "a".repeat(64)),http_status:200,
     body:serde_json::json!([{
       "incomeType":"COMMISSION","tranId":310,"asset":"USDT","income":"-0.002"
     }]).to_string(),
   };
   assert!(source_bounded_money_rows(&[b],&[],&[income]).is_err(),
      "source cash booked a USDT row from an unrelated BASE query");
 }
 #[test]
 fn repeated_identical_cash_posts_are_once_only_but_changed_repeats_deny(){
   let okx=MockGetStep{
     path_and_query:"/api/v5/account/bills-archive?ccy=USDT&begin=1&end=2&limit=100".into(),
     http_status:200,
     body:serde_json::json!({"code":"0","data":[{
       "billId":"301","ccy":"USDT","balChg":"-9.009",
       "ordId":"201","tradeId":"9001"
     }]}).to_string(),
   };
   let binance=MockGetStep{
     path_and_query:format!("/fapi/v1/income?startTime=1&endTime=2&page=1&limit=1000&timestamp=2&recvWindow=10000&signature={}",
       "a".repeat(64)),
     http_status:200,
     body:serde_json::json!([{
       "incomeType":"COMMISSION","tranId":310,"asset":"USDT","income":"-0.002"
     }]).to_string(),
   };
   let get=|o:&[MockGetStep],l:&[MockGetStep],i:&[MockGetStep]|
      source_bounded_money_rows(o,l,i);
   let one=get(&[okx.clone()],&[],&[binance.clone()]).unwrap();
   let repeated=get(&[okx.clone()],&[okx.clone()],
       &[binance.clone(),binance.clone()]).unwrap();
   assert_eq!(one,repeated,"replayed source pages must not multiply cash postings");
   assert_eq!(repeated.5,1);
   assert_eq!(repeated.6,1);
   let mut changed_okx=okx.clone();
   let mut body:serde_json::Value=serde_json::from_str(&changed_okx.body).unwrap();
   body["data"][0]["balChg"]=serde_json::json!("-9.008");
   changed_okx.body=body.to_string();
   assert!(get(&[okx.clone()],&[changed_okx],&[binance.clone()]).is_err());
   let mut changed_binance=binance.clone();
   let mut body:serde_json::Value=serde_json::from_str(&changed_binance.body).unwrap();
   body[0]["income"]=serde_json::json!("-0.003");
   changed_binance.body=body.to_string();
   assert!(get(&[okx.clone()],&[],&[binance,changed_binance]).is_err());
 }
}

/// B6 late-lane source provenance for a verified-local WIDENED H/I scan.
/// Deliberately NOT a `b6_catalogue::Page`: the actual GET begins at the
/// owner's original start, not the B6 late-only cutoff. No fabricated rows.
#[cfg(feature="offline-b6-review")]
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct WidenedLateSourceEvidence{
 pub intended_lane:b6_catalogue::Lane,
 pub owner_generation:u64,
 pub native_owner_sha256:String,
 pub actual_get_from_ms:u64,
 pub actual_get_to_ms:u64,
 pub logical_late_cutoff_ms:u64,
 pub raw_response_sha256:Vec<String>,
 pub mock_new_late_rows:usize,
 pub may_pass_as_b6_delayed_page:bool,
 pub exchange_authenticated:bool,
}

/// Reuses the already existing G->H and I unified owner-bound scans. This
/// carries their real request windows into three explicitly INCOMPLETE B6
/// late-lane evidence slots without relabeling earlier bills as late money.
#[cfg(feature="offline-b6-review")]
pub fn inspect_widened_late_source_scopes_without_network(
 store:&NativeStateStore,generation:u64,first_end:u64,late_end:u64,
 domain:OkxDomain,original:&[MockGetStep],okx_late:&[MockGetStep],
 binance_income:&[MockGetStep],
)->Result<Vec<WidenedLateSourceEvidence>,CaptureError>{
 use b6_catalogue::Lane;
 let review=inspect_original_owner_unified_offline(
     store,generation,first_end,late_end,domain,
     original,okx_late,binance_income).map_err(CaptureError::Proof)?;
 if !review.both_venue_mock_windows_consistent||review.real_ready
    ||review.exchange_authenticated||review.history_and_delayed_postings_complete{
    return Err(deny("widened H/I review not source-only and fail-closed"))
 }
 let plan=crate::native_trade2f3_private_read_plan::inspect_private_history_plan_without_network(
   store,generation,first_end,late_end,domain)
   .map_err(|_|deny("widened late source missing B2 sealed plan"))?;
 let state=store.load().map_err(|_|deny("widened original owner inaccessible"))?
   .ok_or_else(||deny("widened original owner absent"))?;
 let start=state.entry.as_ref().ok_or_else(||deny("widened original entry absent"))?
   .plan.created_at_ms;
 let base=&state.entry.as_ref().unwrap().plan.base;
 let mut base_hash=Vec::new();let mut quote_hash=Vec::new();
 for step in okx_late{
   if vetted_path(CaptureLane::OkxDelayed,&step.path_and_query)?
      !="/api/v5/account/bills-archive"{
     return Err(deny("widened OKX late source endpoint differs from B2"))
   }
   let url=url::Url::parse(&format!("https://review.invalid{}",step.path_and_query))
     .map_err(|_|deny("widened OKX request URL malformed"))?;
   let q:std::collections::BTreeMap<_,_>=url.query_pairs()
      .map(|(k,v)|(k.into_owned(),v.into_owned())).collect();
   if q.get("begin").and_then(|v|v.parse::<u64>().ok())!=Some(start)
      ||q.get("end").and_then(|v|v.parse::<u64>().ok())!=Some(late_end)
      ||q.get("limit").and_then(|v|v.parse::<u16>().ok())
         !=Some(plan.slots()[6].page_size()){
      return Err(deny("widened OKX actual GET bounds are not owner start..late end"))
   }
   match q.get("ccy").map(String::as_str){
     Some(s) if s==base=>base_hash.push(hash(step.body.as_bytes())),
     Some("USDT")=>quote_hash.push(hash(step.body.as_bytes())),
     _=>return Err(deny("widened OKX bill currency outside original owner")),
   }
 }
 if base_hash.is_empty()||quote_hash.is_empty()||
    base_hash.len()>8||quote_hash.len()>8{
    return Err(deny("widened OKX original currency slots incomplete"))
 }
 let mut income_hash=Vec::new();
 if binance_income.len()<4{
    return Err(deny("widened Binance source lacks complete two late scans"))
 }
 let mut original_seen=0usize;
 let mut late_seen=false;
 for step in binance_income{
   if vetted_path(CaptureLane::BinanceOriginalAndDelayed,&step.path_and_query)?
        !="/fapi/v1/income"{
      return Err(deny("widened Binance income endpoint mismatch"))
   }
   let url=url::Url::parse(&format!("https://review.invalid{}",step.path_and_query))
      .map_err(|_|deny("widened Binance income URL malformed"))?;
   let q:std::collections::BTreeMap<_,_>=url.query_pairs()
      .map(|(k,v)|(k.into_owned(),v.into_owned())).collect();
   let end=q.get("endTime").and_then(|v|v.parse::<u64>().ok());
   if q.get("startTime").and_then(|v|v.parse::<u64>().ok())!=Some(start)
      ||q.get("limit").and_then(|v|v.parse::<u16>().ok())
         !=Some(plan.slots()[9].page_size()){
      return Err(deny("widened Binance income original-start or page bound drift"))
   }
   if end==Some(first_end){
      if late_seen{return Err(deny("widened Binance snapshot reverted to original"))}
      original_seen+=1;
      continue
   }
   if end!=Some(late_end){
      return Err(deny("widened Binance actual GET end differs from B2 late end"))
   }
   late_seen=true;
   income_hash.push(hash(step.body.as_bytes()));
 }
 if original_seen==0||income_hash.is_empty()||income_hash.len()>8{
    return Err(deny("widened Binance income source pages incomplete"))
 }
 if store.load().map_err(|_|deny("widened original owner unavailable afterward"))?
     .as_ref()!=Some(&state){
   return Err(deny("widened original owner changed during proof"))
 }
 Ok(vec![
  (Lane::OkxBaseDelayed,base_hash),
  (Lane::OkxQuoteDelayed,quote_hash),
  (Lane::BinanceIncomeDelayed,income_hash),
 ].into_iter().map(|(lane,hashes)|WidenedLateSourceEvidence{
     intended_lane:lane,owner_generation:generation,
     native_owner_sha256:plan.owner_sha256().into(),
     actual_get_from_ms:start,actual_get_to_ms:late_end,
     logical_late_cutoff_ms:first_end,raw_response_sha256:hashes,
     mock_new_late_rows:0,may_pass_as_b6_delayed_page:false,
     exchange_authenticated:false,
 }).collect())
}


/// Inputs are the already existing zero-network source tapes, not B6 pages
/// constructed by a caller. All seven lanes must share one persisted owner.
#[cfg(feature="offline-b6-review")]
pub struct OriginalSevenSourceTapes<'a>{
 pub okx_double_scan:&'a [MockGetStep],
 pub okx_widened:&'a [MockGetStep],
 pub binance_user_trades:&'a [MockGetStep],
 pub binance_income:&'a [MockGetStep],
 pub original_okx_bills:&'a [crate::native_trade2f_income_window::ObservedOkxBill],
 pub b8_original_bills:&'a [crate::native_trade2f_receipt_binding::LocalB8OriginalBillShape],
 pub claimed_okx_account_fingerprint:&'a str,
 pub claimed_binance_account_fingerprint:&'a str,
}

/// Bind 7 ORIGINAL typed sources to a *single* persisted B2 Native owner.
/// Never fabricates missing H/I late-only receipts. Local checks cannot
/// authenticate exchange responses or justify real execution/account PnL.
#[cfg(feature="offline-b6-review")]
fn collect_original_seven_source_pages_without_network(
 store:&NativeStateStore,generation:u64,first_end:u64,late_end:u64,
 domain:OkxDomain,source:OriginalSevenSourceTapes<'_>,
)->Result<(b6_catalogue::OriginalOwner,Vec<b6_catalogue::Page>),CaptureError>{
 let before=store.load().map_err(|_|deny("B6 original-seven Native store unreadable"))?
   .ok_or_else(||deny("B6 original-seven owner absent"))?;
 if before.generation!=generation{return Err(deny("B6 original-seven generation mismatch"))}
 let plan=crate::native_trade2f3_private_read_plan::inspect_private_history_plan_without_network(
   store,generation,first_end,late_end,domain)
   .map_err(|_|deny("B6 original-seven B2 plan not sealed"))?;
 let split=source.okx_double_scan.len()/2;
 // F2G validates the entire signed-mock source twice. Retain ONLY the
 // actual original bills from the first scan, never make empty substitutes.
 if split==0||source.okx_double_scan.len()%2!=0{
    return Err(deny("B6 original-seven F2G tape not paired"))
 }
 let mut bill_tape:Vec<MockGetStep>=Vec::new();
 let mut short_terminal=std::collections::BTreeSet::new();
 for step in &source.okx_double_scan[..split]{
   if !step.path_and_query.starts_with("/api/v5/account/bills-archive?"){continue}
   let parsed=url::Url::parse(&format!("https://review.invalid{}",step.path_and_query))
     .map_err(|_|deny("B6 original bill request invalid"))?;
   let query:std::collections::BTreeMap<_,_>=parsed.query_pairs()
      .map(|(k,v)|(k.into_owned(),v.into_owned())).collect();
   let ccy=query.get("ccy").ok_or_else(||deny("B6 bill original currency absent"))?;
   let limit=query.get("limit").and_then(|n|n.parse::<usize>().ok())
     .ok_or_else(||deny("B6 bill original limit absent"))?;
   let doc:serde_json::Value=serde_json::from_str(&step.body)
     .map_err(|_|deny("B6 bill original JSON invalid"))?;
   let rows=doc.get("data").and_then(|v|v.as_array())
     .ok_or_else(||deny("B6 bill original data missing"))?;
   if short_terminal.contains(ccy){
      if !rows.is_empty(){return Err(deny("B6 bill rows after short terminal"))}
      continue
   }
   if rows.len()<limit{short_terminal.insert(ccy.clone());}
   bill_tape.push(step.clone());
 }
 let mut pages=original_okx_b1_b2_fills_as_b6_review(
    store,generation,first_end,late_end,domain,source.okx_double_scan,
    source.claimed_okx_account_fingerprint)?;
 pages.extend(original_binance_b1_b2_fills_as_b6_review(
    store,generation,first_end,late_end,domain,source.okx_double_scan,
    source.okx_widened,source.binance_user_trades,
    source.claimed_binance_account_fingerprint)?);
 pages.extend(original_b1_b2_bill_pages_as_b6_review(
    store,generation,first_end,late_end,domain,&bill_tape,
    source.original_okx_bills,source.b8_original_bills,
    source.claimed_okx_account_fingerprint)?);
 pages.extend(original_binance_income_b1_b2_as_b6_review(
    store,generation,first_end,late_end,domain,source.binance_income,
    source.claimed_binance_account_fingerprint)?);
 let entry=before.entry.as_ref().ok_or_else(||deny("B6 seven entry missing"))?;
 let slots=plan.slots();
 let owner=b6_catalogue::OriginalOwner{
   generation,native_state_digest:plan.owner_sha256().to_owned(),
   okx_account_fingerprint:source.claimed_okx_account_fingerprint.into(),
   binance_account_fingerprint:source.claimed_binance_account_fingerprint.into(),
   okx_buy_order_id:slots[0].selector().into(),
   okx_sell_order_id:slots[1].selector().into(),
   binance_open_order_id:slots[2].selector().into(),
   binance_close_order_id:slots[3].selector().into(),
   base:entry.plan.base.clone(),
   spot_symbol:entry.plan.spot_symbol.replace('/',"-"),
   futures_symbol:entry.plan.futures_symbol.clone(),
   start_ms:entry.plan.created_at_ms,first_end_ms:first_end,
   late_end_ms:late_end,
 };
 let result=b6_catalogue::inspect_untrusted_original_seven(&owner,&pages)
    .map_err(|_|deny("B6 original-seven validation denied"))?;
 if result.lane_count!=7{
    return Err(deny("B6 original-seven lane count mismatch"))
 }
 if store.load().map_err(|_|deny("B6 original-seven owner reread failed"))?
    .as_ref()!=Some(&before){
    return Err(deny("B6 original-seven owner changed during collection"))
 }
 Ok((owner,pages))
}
#[cfg(feature="offline-b6-review")]
pub fn inspect_owner_bound_original_seven_without_network(
 store:&NativeStateStore,generation:u64,first_end:u64,late_end:u64,
 domain:OkxDomain,source:OriginalSevenSourceTapes<'_>,
)->Result<b6_catalogue::LocalShapeOnly,CaptureError>{
 let (owner,pages)=collect_original_seven_source_pages_without_network(
    store,generation,first_end,late_end,domain,source)?;
 b6_catalogue::inspect_untrusted_original_seven(&owner,&pages)
    .map_err(|_|deny("B6 original-seven shape denied"))
}

/// Original 7 B6 shapes plus three H/I WIDENED read-only scopes.
/// Raw cash rows are deduplicated once, not reclassified as delayed B6 pages.
#[cfg(feature="offline-b6-review")]
pub struct OriginalSevenWidenedNoDoubleCount{
 pub original:b6_catalogue::LocalShapeOnly,
 pub widened:Vec<WidenedLateSourceEvidence>,
 pub original_unique_okx_bills:usize,
 pub original_unique_binance_postings:usize,
 pub widened_new_cash_postings:usize,
}
#[cfg(feature="offline-b6-review")]
impl OriginalSevenWidenedNoDoubleCount{
 pub fn is_exchange_authenticated(&self)->bool{false}
 pub fn all_ten_b6_pages_complete(&self)->bool{false}
 pub fn may_book_new_cash_or_trade(&self)->bool{false}
}
/// Binds existing owner-certified offline adapters and H/I to one store
/// generation. All widened rows must already be present in the original
/// cash snapshot. Future legitimate late rows require a separate reviewed
/// reconciliation protocol; they cannot enter cash as a second posting here.
#[cfg(feature="offline-b6-review")]
pub fn inspect_original_seven_with_widened_cash_without_network(
 store:&NativeStateStore,generation:u64,first_end:u64,late_end:u64,
 domain:OkxDomain,source:OriginalSevenSourceTapes<'_>,
)->Result<OriginalSevenWidenedNoDoubleCount,CaptureError>{
 let before=store.load().map_err(|_|deny("B6/H/I owner read failed"))?
   .ok_or_else(||deny("B6/H/I owner absent"))?;
 let widened=inspect_widened_late_source_scopes_without_network(
   store,generation,first_end,late_end,domain,
   source.okx_double_scan,source.okx_widened,source.binance_income)?;
 let mut original_income=Vec::new();
 for step in source.binance_income{
   let url=url::Url::parse(&format!("https://review.invalid{}",step.path_and_query))
     .map_err(|_|deny("B6/H/I income GET malformed"))?;
   let end=url.query_pairs().find(|(k,_)|k=="endTime")
     .and_then(|(_,v)|v.parse::<u64>().ok())
     .ok_or_else(||deny("B6/H/I income endTime missing"))?;
   if end==first_end{original_income.push(step.clone())}
 }
 if original_income.is_empty(){return Err(deny("B6/H/I original income scan missing"))}
 let baseline=source_bounded_money_rows(
   source.okx_double_scan,&[],&original_income)?;
 let union=source_bounded_money_rows(
   source.okx_double_scan,source.okx_widened,source.binance_income)?;
 if baseline!=union{
   return Err(deny("B6/H/I widened cash differs from once-booked original cash"))
 }
 let original_unique_okx_bills=baseline.5;
 let original_unique_binance_postings=baseline.6;
 let original=inspect_owner_bound_original_seven_without_network(
   store,generation,first_end,late_end,domain,source)?;
 if widened.len()!=3||widened.iter().any(|scope|
   scope.owner_generation!=generation||scope.may_pass_as_b6_delayed_page
   ||scope.mock_new_late_rows!=0||scope.exchange_authenticated){
   return Err(deny("B6/H/I widened provenance cannot masquerade as delayed B6"))
 }
 if store.load().map_err(|_|deny("B6/H/I owner final read failed"))?
    .as_ref()!=Some(&before){
   return Err(deny("B6/H/I owner changed across both source sets"))
 }
 Ok(OriginalSevenWidenedNoDoubleCount{
   original,widened,original_unique_okx_bills,
   original_unique_binance_postings,widened_new_cash_postings:0,
 })
}

/// Zero-network quarantine for a genuinely novel widened source row.
/// This is NOT a replacement for the strict H/I validator, and never books cash.
#[cfg(feature="offline-b6-review")]
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct UntrustedWidenedCandidate{
 pub lane:b6_catalogue::Lane,
 pub source_key:String,
 pub raw_row_sha256:String,
 pub posting_time_ms:u64,
 pub observed_in_early_window:bool,
}
#[cfg(feature="offline-b6-review")]
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct UntrustedWidenedQuarantine{
 pub lane:b6_catalogue::Lane,
 pub owner_generation:u64,
 pub owner_sha256:String,
 pub actual_get_start_ms:u64,
 pub actual_get_end_ms:u64,
 pub logical_cutoff_ms:u64,
 pub original_page_sha256:Vec<String>,
 pub widened_first_page_sha256:Vec<String>,
 pub widened_repeat_page_sha256:Vec<String>,
 pub repeated_original_count:usize,
 pub candidates:Vec<UntrustedWidenedCandidate>,
}
#[cfg(feature="offline-b6-review")]
impl UntrustedWidenedQuarantine{
 pub fn is_exchange_authenticated(&self)->bool{false}
 pub fn proves_complete_account_history(&self)->bool{false}
 pub fn may_post_cash_or_mark_verified_closed(&self)->bool{false}
 pub fn may_send_orders(&self)->bool{false}
}

/// Compare one original source lane with TWO widened rereads. New cash
/// events are only quarantined; a missing/revised original is hard DENY.
/// The caller supplies raw mock GET pages, never a claimed B6 delayed page.
#[cfg(feature="offline-b6-review")]
pub fn quarantine_untrusted_widened_lane_without_network(
 store:&NativeStateStore,generation:u64,first_end:u64,late_end:u64,
 domain:OkxDomain,lane:b6_catalogue::Lane,
 original:&[MockGetStep],widened_a:&[MockGetStep],widened_b:&[MockGetStep],
)->Result<UntrustedWidenedQuarantine,CaptureError>{
 use b6_catalogue::Lane;
 let before=store.load().map_err(|_|deny("quarantine Native state unreadable"))?
   .ok_or_else(||deny("quarantine Native owner missing"))?;
 if before.generation!=generation{return Err(deny("quarantine owner generation drift"))}
 let plan=crate::native_trade2f3_private_read_plan::inspect_private_history_plan_without_network(
   store,generation,first_end,late_end,domain)
   .map_err(|_|deny("quarantine B2 sealed plan absent"))?;
 let entry=before.entry.as_ref().ok_or_else(||deny("quarantine owner entry missing"))?;
 let start=entry.plan.created_at_ms;
 let (endpoint,ccy)=match lane{
   Lane::OkxBaseDelayed=>("/api/v5/account/bills-archive",Some(entry.plan.base.as_str())),
   Lane::OkxQuoteDelayed=>("/api/v5/account/bills-archive",Some("USDT")),
   Lane::BinanceIncomeDelayed=>("/fapi/v1/income",None),
   _=>return Err(deny("quarantine accepts only three widened H/I lanes")),
 };
 type Rows=std::collections::BTreeMap<String,(serde_json::Value,u64)>;
 let scan=|tape:&[MockGetStep],end:u64,require_empty_tail:bool|->Result<Rows,CaptureError>{
   if tape.is_empty()||tape.len()>4{return Err(deny("quarantine page count unbounded"))}
   let mut out=Rows::new();
   let mut prior_bill:Option<String>=None;
   let mut last_numeric:Option<u64>=None;
   let mut terminal=false;
   let mut short_page=false;
   for (idx,step) in tape.iter().enumerate(){
     let kind=if ccy.is_some(){CaptureLane::OkxDelayed}
         else{CaptureLane::BinanceOriginalAndDelayed};
     if vetted_path(kind,&step.path_and_query)?!=endpoint||
       step.http_status!=200||step.body.is_empty()||step.body.len()>LIMIT_BODY{
       return Err(deny("quarantine source endpoint, HTTP or body invalid"))
     }
     let url=url::Url::parse(&format!("https://review.invalid{}",step.path_and_query))
        .map_err(|_|deny("quarantine request invalid"))?;
     let q:std::collections::BTreeMap<_,_>=url.query_pairs()
        .map(|(k,v)|(k.into_owned(),v.into_owned())).collect();
     let number=|key:&str|q.get(key).and_then(|v|v.parse::<u64>().ok());
     let limit=if ccy.is_some(){100}else{1000};
     if number("limit")!=Some(limit){return Err(deny("quarantine unexpected source limit"))}
     if let Some(c)=ccy{
       if q.get("ccy").map(String::as_str)!=Some(c)
        ||number("begin")!=Some(start)||number("end")!=Some(end)
        ||q.get("after")!=prior_bill.as_ref(){
          return Err(deny("quarantine OKX actual GET scope/cursor invalid"))
       }
     }else if number("startTime")!=Some(start)||number("endTime")!=Some(end)
          ||number("page")!=Some((idx+1) as u64){
       return Err(deny("quarantine Binance actual GET scope/page invalid"))
     }
     if terminal{return Err(deny("quarantine pages after terminal"))}
     let body:serde_json::Value=serde_json::from_str(&step.body)
       .map_err(|_|deny("quarantine source JSON malformed"))?;
     let rows=if ccy.is_some(){
       if body.get("code").and_then(|x|x.as_str())!=Some("0"){
          return Err(deny("quarantine OKX response code invalid"))
       }
       body.get("data").and_then(|x|x.as_array())
     }else{body.as_array()}.ok_or_else(||deny("quarantine missing source rows"))?;
     if rows.len()>limit as usize||short_page&&!rows.is_empty(){
       return Err(deny("quarantine incomplete or oversized source page"))
     }
     if rows.len()<limit as usize{short_page=true}
     if rows.is_empty(){terminal=true}
     for row in rows{
       let (key,at)=if let Some(c)=ccy{
         let get=|k|row.get(k).and_then(|v|v.as_str());
         if get("ccy")!=Some(c)||get("instId")!=
              Some(entry.plan.spot_symbol.replace('/',"-").as_str()){
            return Err(deny("quarantine OKX currency/symbol not owner scoped"))
         }
         let id=get("billId").ok_or_else(||deny("quarantine billId absent"))?;
         if !canonical_okx_history_id(id){
            return Err(deny("quarantine billId not canonical"))
         }
         let numeric=id.parse::<u64>().map_err(|_|deny("quarantine billId overflow"))?;
         if last_numeric.is_some_and(|n|numeric>=n){
            return Err(deny("quarantine OKX page billId not descending"))
         }
         last_numeric=Some(numeric);
         prior_bill=Some(id.to_owned());
         let at=get("ts").and_then(|t|t.parse::<u64>().ok())
            .ok_or_else(||deny("quarantine OKX timestamp absent"))?;
         (id.to_owned(),at)
       }else{
         let kind=row.get("incomeType").and_then(|v|v.as_str())
           .ok_or_else(||deny("quarantine income type absent"))?;
         if kind.is_empty()||kind.len()>48
           ||!kind.bytes().all(|c|c.is_ascii_uppercase()||c==b'_'){
            return Err(deny("quarantine income type invalid"))
         }
         if row.get("asset").and_then(|v|v.as_str())!=Some("USDT")
          ||row.get("symbol").and_then(|v|v.as_str())!=
             Some(entry.plan.futures_symbol.as_str()){
             return Err(deny("quarantine Binance asset/symbol outside owner"))
         }
         let id=row.get("tranId").and_then(|v|v.as_u64()
            .or_else(||v.as_str().and_then(|s|s.parse::<u64>().ok())))
            .filter(|id|*id>0).ok_or_else(||deny("quarantine tranId invalid"))?;
         let at=row.get("time").and_then(|v|v.as_u64())
            .ok_or_else(||deny("quarantine income time missing"))?;
         (format!("{kind}:{id}"),at)
       };
       if at<start||at>end{return Err(deny("quarantine source row outside actual GET window"))}
       if out.insert(key,(row.clone(),at)).is_some(){
          return Err(deny("quarantine duplicate raw economic identity"))
       }
     }
   }
   if !short_page || require_empty_tail&&!terminal{
     return Err(deny("quarantine source page termination incomplete"))
   }
   Ok(out)
 };
 let original_rows=scan(original,first_end,false)?;
 let widened_rows=scan(widened_a,late_end,true)?;
 let repeated_rows=scan(widened_b,late_end,true)?;
 if widened_rows!=repeated_rows{
   return Err(deny("quarantine widened reread disagrees"))
 }
 for (key,baseline) in &original_rows{
   if widened_rows.get(key)!=Some(baseline){
     return Err(deny("quarantine original posting missing or rewritten"))
   }
 }
 let mut candidates=Vec::new();
 for (key,(row,ts)) in widened_rows.iter(){
   if original_rows.contains_key(key){continue}
   candidates.push(UntrustedWidenedCandidate{
     lane,source_key:key.clone(),raw_row_sha256:hash(row.to_string().as_bytes()),
     posting_time_ms:*ts,observed_in_early_window:*ts<=first_end,
   });
 }
 if store.load().map_err(|_|deny("quarantine owner final reread failed"))?
      .as_ref()!=Some(&before){
   return Err(deny("quarantine owner changed during widened reread"))
 }
 Ok(UntrustedWidenedQuarantine{
   lane,owner_generation:generation,owner_sha256:plan.owner_sha256().into(),
   actual_get_start_ms:start,actual_get_end_ms:late_end,logical_cutoff_ms:first_end,
   original_page_sha256:original.iter().map(|p|hash(p.body.as_bytes())).collect(),
   widened_first_page_sha256:widened_a.iter().map(|p|hash(p.body.as_bytes())).collect(),
   widened_repeat_page_sha256:widened_b.iter().map(|p|hash(p.body.as_bytes())).collect(),
   repeated_original_count:original_rows.len(),candidates,
 })
}

/// Offline-only, versioned HUMAN quarantine index. The hash is a local
/// accidental-corruption check, NOT authenticated provenance or antirollback.
#[cfg(feature="offline-b6-review")]
#[derive(Clone,Copy,Debug,PartialEq,Eq,serde::Serialize,serde::Deserialize)]
pub enum ManualOnlyDisposition{
 Pending,ReviewNeeded,ExcludedFromAccounting,
}
#[cfg(feature="offline-b6-review")]
#[derive(Clone,Debug,PartialEq,Eq,serde::Serialize,serde::Deserialize)]
pub struct QuarantineJournalRow{
 pub lane_code:u8,
 pub source_key:String,
 pub raw_row_sha256:String,
 pub first_seen_page_sha256:String,
 pub posting_time_ms:u64,
 pub observed_in_early_window:bool,
 pub disposition:ManualOnlyDisposition,
 pub review_reason_sha256:Option<String>,
}
#[cfg(feature="offline-b6-review")]
#[derive(Clone,Debug,PartialEq,Eq,serde::Serialize,serde::Deserialize)]
pub struct VersionedWidenedQuarantineJournal{
 schema_version:u8,
 owner_generation:u64,
 owner_sha256:String,
 start_ms:u64,cutoff_ms:u64,end_ms:u64,
 revision:u64,
 original_pages_by_lane:std::collections::BTreeMap<u8,Vec<String>>,
 rows:std::collections::BTreeMap<String,QuarantineJournalRow>,
 snapshot_sha256:String,
}
#[cfg(feature="offline-b6-review")]
impl VersionedWidenedQuarantineJournal{
 fn lane_code(lane:b6_catalogue::Lane)->Result<u8,CaptureError>{
  use b6_catalogue::Lane;
  match lane{
   Lane::OkxBaseDelayed=>Ok(1),Lane::OkxQuoteDelayed=>Ok(2),
   Lane::BinanceIncomeDelayed=>Ok(3),
   _=>Err(deny("quarantine journal lane not delayed H/I")),
  }
 }
 fn is_hex64(s:&str)->bool{
  s.len()==64&&s.bytes().all(|c|c.is_ascii_digit()||(b'a'..=b'f').contains(&c))
 }
 fn digest(&self)->Result<String,CaptureError>{
  let blob=serde_json::to_vec(&(
   "spotarb-offline-manual-journal-v1",self.schema_version,
   self.owner_generation,&self.owner_sha256,self.start_ms,
   self.cutoff_ms,self.end_ms,self.revision,
   &self.original_pages_by_lane,&self.rows,
  )).map_err(|_|deny("quarantine journal encode failed"))?;
  Ok(hash(&blob))
 }
 fn reseal(&mut self)->Result<(),CaptureError>{
  self.snapshot_sha256=self.digest()?;
  Ok(())
 }
 pub fn from_first(q:&UntrustedWidenedQuarantine)->Result<Self,CaptureError>{
  let mut j=Self{
   schema_version:1,owner_generation:q.owner_generation,
   owner_sha256:q.owner_sha256.clone(),start_ms:q.actual_get_start_ms,
   cutoff_ms:q.logical_cutoff_ms,end_ms:q.actual_get_end_ms,
   revision:0,original_pages_by_lane:Default::default(),
   rows:Default::default(),snapshot_sha256:String::new(),
  };
  j.reseal()?;
  j.observe(q)?;
  Ok(j)
 }
 pub fn revision(&self)->u64{self.revision}
 pub fn rows(&self)->&std::collections::BTreeMap<String,QuarantineJournalRow>{&self.rows}
 pub fn local_digest(&self)->&str{&self.snapshot_sha256}
 pub fn can_book_cash_or_mark_verified_closed(&self)->bool{false}
 pub fn exchange_authenticated(&self)->bool{false}
 pub fn independently_antirollback_proven(&self)->bool{false}
 pub fn verify(&self)->Result<(),CaptureError>{
  if self.schema_version!=1||self.owner_generation==0
   ||!Self::is_hex64(&self.owner_sha256)
   ||self.start_ms==0||self.start_ms>self.cutoff_ms
   ||self.cutoff_ms>=self.end_ms||self.rows.len()>1024
   ||self.snapshot_sha256!=self.digest()?{
   return Err(deny("quarantine journal scope/schema/digest invalid"))
  }
  for (lane,pages) in &self.original_pages_by_lane{
   if !(1..=3).contains(lane)||pages.is_empty()||pages.len()>4
    ||pages.iter().any(|p|!Self::is_hex64(p)){
     return Err(deny("quarantine journal original source hashes invalid"))
   }
  }
  for (key,row) in &self.rows{
   if !self.original_pages_by_lane.contains_key(&row.lane_code){
     return Err(deny("quarantine journal row missing original lane proof"))
   }
   if !(1..=3).contains(&row.lane_code)||row.source_key.is_empty()
    ||row.source_key.len()>128
    ||!row.source_key.bytes().all(|b|
       b.is_ascii_uppercase()||b.is_ascii_digit()||b==b'_'||b==b':')
    ||*key!=format!("{}:{}",row.lane_code,row.source_key)
    ||!Self::is_hex64(&row.raw_row_sha256)
    ||!Self::is_hex64(&row.first_seen_page_sha256)
    ||row.posting_time_ms<self.start_ms||row.posting_time_ms>self.end_ms
    ||row.observed_in_early_window!=(row.posting_time_ms<=self.cutoff_ms){
      return Err(deny("quarantine journal row identity invalid"))
   }
   if (row.disposition==ManualOnlyDisposition::Pending)
         !=row.review_reason_sha256.is_none()
     ||row.review_reason_sha256.as_ref()
       .is_some_and(|s|!Self::is_hex64(s)){
      return Err(deny("quarantine journal manual review integrity invalid"))
   }
  }
  Ok(())
 }
 pub fn observe(&mut self,q:&UntrustedWidenedQuarantine)->Result<usize,CaptureError>{
  self.verify()?;
  let lane=Self::lane_code(q.lane)?;
  if q.owner_generation!=self.owner_generation
   ||q.owner_sha256!=self.owner_sha256
   ||q.actual_get_start_ms!=self.start_ms
   ||q.logical_cutoff_ms!=self.cutoff_ms
   ||q.actual_get_end_ms!=self.end_ms
   ||q.candidates.len()>64{
    return Err(deny("quarantine journal scope mismatch"))
  }
  for pages in [&q.original_page_sha256,&q.widened_first_page_sha256,
                &q.widened_repeat_page_sha256]{
   if pages.is_empty()||pages.len()>4
      ||pages.iter().any(|s|!Self::is_hex64(s)){
     return Err(deny("quarantine journal page digest invalid"))
   }
  }
  let capture=serde_json::to_vec(&(
   &q.original_page_sha256,&q.widened_first_page_sha256,
   &q.widened_repeat_page_sha256,
  )).map_err(|_|deny("quarantine journal capture encode failed"))?;
  let capture_sha=hash(&capture);
  let mut next=self.clone();
  let new_lane=!next.original_pages_by_lane.contains_key(&lane);
  if let Some(prior)=next.original_pages_by_lane.get(&lane){
   if *prior!=q.original_page_sha256{
    return Err(deny("quarantine original snapshot changed between observations"))
   }
  }else{
   next.original_pages_by_lane.insert(lane,q.original_page_sha256.clone());
  }
  let mut seen=std::collections::BTreeSet::new();
  let mut added=0usize;
  for candidate in &q.candidates{
   if candidate.lane!=q.lane
    ||candidate.source_key.is_empty()||candidate.source_key.len()>128
    ||!candidate.source_key.bytes().all(|b|
       b.is_ascii_uppercase()||b.is_ascii_digit()||b==b'_'||b==b':')
    ||!Self::is_hex64(&candidate.raw_row_sha256)
    ||candidate.posting_time_ms<self.start_ms
    ||candidate.posting_time_ms>self.end_ms
    ||candidate.observed_in_early_window!=
       (candidate.posting_time_ms<=self.cutoff_ms){
     return Err(deny("quarantine candidate identity or time inconsistent"))
   }
   let key=format!("{}:{}",lane,candidate.source_key);
   if !seen.insert(key.clone()){
     return Err(deny("quarantine candidate duplicate key in observation"))
   }
   if let Some(old)=next.rows.get(&key){
     if old.raw_row_sha256!=candidate.raw_row_sha256
      ||old.posting_time_ms!=candidate.posting_time_ms
      ||old.observed_in_early_window!=candidate.observed_in_early_window{
       return Err(deny("quarantine candidate identity rewritten across observations"))
     }
   }else{
     next.rows.insert(key,QuarantineJournalRow{
       lane_code:lane,source_key:candidate.source_key.clone(),
       raw_row_sha256:candidate.raw_row_sha256.clone(),
       first_seen_page_sha256:capture_sha.clone(),
       posting_time_ms:candidate.posting_time_ms,
       observed_in_early_window:candidate.observed_in_early_window,
       disposition:ManualOnlyDisposition::Pending,review_reason_sha256:None,
     });
     added+=1;
   }
  }
  if self.rows.iter().any(|(key,row)|
     row.lane_code==lane&&!seen.contains(key)){
    return Err(deny("quarantine prior candidate vanished on later observation"))
  }
  if added>0||new_lane{
   if next.rows.len()>1024{return Err(deny("quarantine journal capacity exceeded"))}
   next.revision=next.revision.checked_add(1)
     .ok_or_else(||deny("quarantine journal revision overflow"))?;
   next.reseal()?;
   next.verify()?;
   *self=next;
  }
  Ok(added)
 }
 pub fn review_without_cash_authority(&mut self,
   expected_revision:u64,row_key:&str,next:ManualOnlyDisposition,
   reason_sha256:&str,
 )->Result<(),CaptureError>{
  self.verify()?;
  if self.revision!=expected_revision||!Self::is_hex64(reason_sha256){
    return Err(deny("quarantine manual review stale revision or reason"))
  }
  let mut copy=self.clone();
  let row=copy.rows.get_mut(row_key)
    .ok_or_else(||deny("quarantine manual review unknown candidate"))?;
  let allowed=matches!(
   (row.disposition,next),
   (ManualOnlyDisposition::Pending,ManualOnlyDisposition::ReviewNeeded)
   |(ManualOnlyDisposition::Pending,ManualOnlyDisposition::ExcludedFromAccounting)
   |(ManualOnlyDisposition::ReviewNeeded,ManualOnlyDisposition::ExcludedFromAccounting)
  );
  if !allowed{return Err(deny("quarantine manual review transition forbidden"))}
  row.disposition=next;
  row.review_reason_sha256=Some(reason_sha256.into());
  copy.revision=copy.revision.checked_add(1)
   .ok_or_else(||deny("quarantine journal review revision overflow"))?;
  copy.reseal()?;
  copy.verify()?;
  *self=copy;
  Ok(())
 }
 pub fn to_review_json(&self)->Result<String,CaptureError>{
  self.verify()?;
  serde_json::to_string(self)
    .map_err(|_|deny("quarantine journal serialization failed"))
 }
 pub fn restore_review_json(json:&str)->Result<Self,CaptureError>{
  if json.len()>1_048_576{
   return Err(deny("quarantine journal review JSON unbounded"))
  }
  let journal:Self=serde_json::from_str(json)
    .map_err(|_|deny("quarantine journal review JSON malformed"))?;
  journal.verify()?;
  Ok(journal)
 }
}

/// Strict local raw-source adapter for three genuinely late-only B6 lanes.
/// H/I widened GETs are rejected because their actual start differs.
/// Caller-supplied signed-mock bytes are NOT independently authenticated.
#[cfg(feature="offline-b6-review")]
pub struct LateOnlyThreeRawTapes<'a>{
 pub okx_base:&'a [MockGetStep],
 pub okx_quote:&'a [MockGetStep],
 pub binance_income:&'a [MockGetStep],
 pub claimed_okx_account_fingerprint:&'a str,
 pub claimed_binance_account_fingerprint:&'a str,
}
#[cfg(feature="offline-b6-review")]
fn collect_late_only_three_pages_without_network(
 store:&NativeStateStore,generation:u64,first_end:u64,late_end:u64,
 domain:OkxDomain,raw:LateOnlyThreeRawTapes<'_>,
)->Result<(b6_catalogue::OriginalOwner,Vec<b6_catalogue::Page>),CaptureError>{
 use b6_catalogue::{Lane,Page,SourceRow};
 let state=store.load().map_err(|_|deny("B6 late source Native read failed"))?
   .ok_or_else(||deny("B6 late source owner missing"))?;
 let plan=crate::native_trade2f3_private_read_plan::
   inspect_late_only_b6_blueprint_without_network(
      store,generation,first_end,late_end,domain)
   .map_err(|_|deny("B6 delayed-only sealed read blueprint absent"))?;
 let entry=state.entry.as_ref().ok_or_else(||deny("B6 late source owner entry absent"))?;
 let original=crate::native_trade2f3_private_read_plan::
   inspect_private_history_plan_without_network(
      store,generation,first_end,late_end,domain)
   .map_err(|_|deny("B6 late source original B2 owner missing"))?;
 let slots=original.slots();
 let owner=b6_catalogue::OriginalOwner{
   generation,native_state_digest:original.owner_sha256().into(),
   okx_account_fingerprint:raw.claimed_okx_account_fingerprint.into(),
   binance_account_fingerprint:raw.claimed_binance_account_fingerprint.into(),
   okx_buy_order_id:slots[0].selector().into(),
   okx_sell_order_id:slots[1].selector().into(),
   binance_open_order_id:slots[2].selector().into(),
   binance_close_order_id:slots[3].selector().into(),
   base:entry.plan.base.clone(),
   spot_symbol:entry.plan.spot_symbol.replace('/',"-"),
   futures_symbol:entry.plan.futures_symbol.clone(),
   start_ms:entry.plan.created_at_ms,first_end_ms:first_end,late_end_ms:late_end,
 };
 let lanes=[
   (Lane::OkxBaseDelayed,raw.okx_base,raw.claimed_okx_account_fingerprint),
   (Lane::OkxQuoteDelayed,raw.okx_quote,raw.claimed_okx_account_fingerprint),
   (Lane::BinanceIncomeDelayed,raw.binance_income,raw.claimed_binance_account_fingerprint),
 ];
 let mut output=Vec::<Page>::new();
 for (i,(lane,tape,account)) in lanes.into_iter().enumerate(){
   let slot=&plan.slots()[i];
   let expected=slot.endpoint();
   let currency=slot.source_currency();
   if tape.is_empty()||tape.len()>slot.max_pages() as usize{
      return Err(deny("B6 late source lane absent or pages unbounded"))
   }
   let mut after:Option<String>=None;
   let mut terminal=false;
   for (index,step) in tape.iter().enumerate(){
     if terminal{return Err(deny("B6 late source has pages after terminal"))}
     let src_lane=if i<2{CaptureLane::OkxDelayed}
       else{CaptureLane::BinanceOriginalAndDelayed};
     if vetted_path(src_lane,&step.path_and_query)?!=expected
       ||step.http_status!=200||step.body.len()>LIMIT_BODY{
       return Err(deny("B6 late source HTTP/path/body invalid"))
     }
     let url=url::Url::parse(&format!("https://review.invalid{}",step.path_and_query))
       .map_err(|_|deny("B6 late source GET query malformed"))?;
     let q:std::collections::BTreeMap<_,_>=url.query_pairs()
       .map(|(k,v)|(k.into_owned(),v.into_owned())).collect();
     let number=|k:&str|q.get(k).and_then(|v|v.parse::<u64>().ok());
     if number("limit")!=Some(slot.limit() as u64){
       return Err(deny("B6 late source GET page size differs from B2"))
     }
     if let Some(ccy)=currency{
       if q.get("ccy").map(String::as_str)!=Some(ccy)
          ||number("begin")!=Some(first_end)||number("end")!=Some(late_end)
          ||q.get("after").cloned()!=after{
         return Err(deny("B6 late OKX actual GET scope/cursor differs from B2"))
       }
     }else if number("startTime")!=Some(first_end)
       ||number("endTime")!=Some(late_end)
       ||number("page")!=Some(index as u64+1){
       return Err(deny("B6 late Binance actual GET scope/page differs from B2"))
     }
     let doc:serde_json::Value=serde_json::from_str(&step.body)
       .map_err(|_|deny("B6 late GET JSON malformed"))?;
     let rows=if i<2{
       if doc.get("code").and_then(|v|v.as_str())!=Some("0"){
         return Err(deny("B6 late OKX source response code failed"))
       }
       doc.get("data").and_then(|v|v.as_array())
     }else{doc.as_array()}
       .ok_or_else(||deny("B6 late response data array absent"))?;
     if rows.len()>slot.limit() as usize{
       return Err(deny("B6 late source page overflow"))
     }
     let mut seen_rows=Vec::<SourceRow>::new();
     for v in rows{
       let (key,at)=if let Some(ccy)=currency{
         if v.get("ccy").and_then(|x|x.as_str())!=Some(ccy){
           return Err(deny("B6 late OKX source currency outside GET scope"))
         }
         if v.get("instId").and_then(|x|x.as_str()).is_some_and(|s|
            !s.is_empty()&&s!=entry.plan.spot_symbol.replace('/',"-")){
           return Err(deny("B6 late OKX foreign trade instrument in owner scope"))
         }
         let bill=v.get("billId").and_then(|x|x.as_str())
            .ok_or_else(||deny("B6 late OKX bill ID absent"))?;
         if !canonical_okx_history_id(bill)
           ||bill.parse::<u64>().is_err(){
           return Err(deny("B6 late OKX bill ID noncanonical"))
         }
         let at=v.get("ts").and_then(|x|x.as_str())
            .and_then(|x|x.parse::<u64>().ok())
            .ok_or_else(||deny("B6 late OKX bill timestamp invalid"))?;
         (bill.to_owned(),at)
       }else{
         let kind=v.get("incomeType").and_then(|x|x.as_str())
           .ok_or_else(||deny("B6 late Binance income type missing"))?;
         if !matches!(kind,"COMMISSION"|"REALIZED_PNL"|"FUNDING_FEE")
           ||v.get("asset").and_then(|x|x.as_str())!=Some("USDT")
           ||v.get("symbol").and_then(|x|x.as_str())!=Some(entry.plan.futures_symbol.as_str()){
           return Err(deny("B6 late Binance income type, asset or symbol not owner bound"))
         }
         let id=v.get("tranId").and_then(|x|x.as_u64()
           .or_else(||x.as_str().and_then(|s|s.parse::<u64>().ok())))
           .filter(|id|*id>0).ok_or_else(||deny("B6 late Binance tranId absent"))?;
         let at=v.get("time").and_then(|x|x.as_u64())
           .ok_or_else(||deny("B6 late Binance posting time absent"))?;
         (format!("{kind}:{id}"),at)
       };
       if at<=first_end||at>late_end{
         return Err(deny("B6 late source row not in actual delayed-only interval"))
       }
       seen_rows.push(SourceRow{unique_key:key,order_id:None,symbol:None});
     }
     let last_page=if i<2{rows.len()<slot.limit() as usize}
       else{rows.is_empty()};
     let cursor_in=after.clone();
     let cursor_out=if last_page{None}
       else if i<2{seen_rows.last().map(|r|r.unique_key.clone())}
       else{Some((index+2).to_string())};
     output.push(Page{
       lane,generation,native_state_digest:owner.native_state_digest.clone(),
       account_fingerprint:account.into(),endpoint:expected.into(),
       order_id:None,symbol:None,from_ms:first_end,to_ms:late_end,
       index,cursor_in:if i<2{cursor_in}
         else if index==0{None}else{Some((index+1).to_string())},
       cursor_out:cursor_out.clone(),last_page,
       requested_limit:slot.limit() as usize,
       http_status:step.http_status,business_success:true,
       body_sha256:hash(step.body.as_bytes()),rows:seen_rows,
     });
     after=cursor_out;
     terminal=last_page;
   }
   if !terminal{return Err(deny("B6 late raw source pagination not terminal"))}
 }
 let result=b6_catalogue::inspect_untrusted_delayed_three(&owner,&output)
   .map_err(|_|deny("B6 late source typed-page consistency denied"))?;
 if result.lane_count!=3{
   return Err(deny("B6 late source three lanes missing"))
 }
 if store.load().map_err(|_|deny("B6 late source owner final read failed"))?
   .as_ref()!=Some(&state){
   return Err(deny("B6 late source owner changed during capture"))
 }
 Ok((owner,output))
}
#[cfg(feature="offline-b6-review")]
pub fn inspect_late_only_three_raw_sources_without_network(
 store:&NativeStateStore,generation:u64,first_end:u64,late_end:u64,
 domain:OkxDomain,raw:LateOnlyThreeRawTapes<'_>,
)->Result<b6_catalogue::LocalShapeOnly,CaptureError>{
 let (owner,pages)=collect_late_only_three_pages_without_network(
    store,generation,first_end,late_end,domain,raw)?;
 b6_catalogue::inspect_untrusted_delayed_three(&owner,&pages)
   .map_err(|_|deny("B6 late source typed-page shape denied"))
}

/// P1-01 LOCAL B6 7+3 joining. Both collectors read exact same sealed Native
/// owner and distinct raw source tapes. It never authenticates the exchange,
/// certifies PnL or makes new cash/bookkeeping entries.
#[cfg(feature="offline-b6-review")]
pub struct OriginalTenOfflineShape{
 pub original_source_lanes:usize,
 pub delayed_source_lanes:usize,
 pub local_b6_shape:b6_catalogue::LocalShapeOnly,
 pub original_source_pages:usize,
 pub delayed_source_pages:usize,
 pub sealed_owner_sha256:String,
 pub raw_page_sha256:Vec<String>,
 /// Local SHA of *all seven original/corroborating and three delayed*
 /// input request/response tapes, including request query strings.
 pub input_tapes_sha256:String,
 pub local_transcript_sha256:String,
}
#[cfg(feature="offline-b6-review")]
impl OriginalTenOfflineShape{
 pub fn local_ten_lane_shape_checked(&self)->bool{
   self.original_source_lanes==7&&self.delayed_source_lanes==3
      &&self.local_b6_shape.lane_count==10
 }
 pub fn is_exchange_authenticated(&self)->bool{false}
 pub fn is_independent_account_verified(&self)->bool{false}
 pub fn is_antirollback_anchored(&self)->bool{false}
 pub fn may_post_cash_or_mark_verified_closed(&self)->bool{false}
 pub fn may_trade_cancel_transfer_or_release(&self)->bool{false}
}
/// The raw account "fingerprints" remain caller CLAIMS. A 10/10 local
/// B6 shape is not a historical-truth/real-read gate.
#[cfg(feature="offline-b6-review")]
pub fn inspect_owner_bound_ten_b6_raw_lanes_without_network(
 store:&NativeStateStore,generation:u64,first_end:u64,late_end:u64,
 domain:OkxDomain,original:OriginalSevenSourceTapes<'_>,
 delayed:LateOnlyThreeRawTapes<'_>,
)->Result<OriginalTenOfflineShape,CaptureError>{
 let before=store.load().map_err(|_|deny("B6 ten owner read failed"))?
   .ok_or_else(||deny("B6 ten Native state absent"))?;
 if original.claimed_okx_account_fingerprint!=delayed.claimed_okx_account_fingerprint
    ||original.claimed_binance_account_fingerprint!=delayed.claimed_binance_account_fingerprint{
   return Err(deny("B6 ten exchange account claims drift between source sets"))
 }
 // V2 local transcript binds the ORIGINAL untrusted request path, signed
 // query shape, exact HTTP status and response digest. V1 hashed only typed
 // response bodies, so an altered request query/signature was invisible.
 // Include all corroborating H/I and repeated original mock GET input too:
 // discarding a tape from the receipt would silently change its meaning.
 let mut raw_inputs=Vec::<(&str,usize,String,String,u16)>::new();
 for (name,steps) in [
   ("okx-original-double",original.okx_double_scan),
   ("okx-widened-corrob",original.okx_widened),
   ("binance-original-fills",original.binance_user_trades),
   ("binance-original-income",original.binance_income),
   ("okx-base-late",delayed.okx_base),
   ("okx-quote-late",delayed.okx_quote),
   ("binance-income-late",delayed.binance_income),
 ]{
   for (index,step) in steps.iter().enumerate(){
     raw_inputs.push((name,index,hash(step.path_and_query.as_bytes()),
       hash(step.body.as_bytes()),step.http_status));
   }
 }
 let original_cash_claim_hash=hash(format!("{:?}",original.original_okx_bills).as_bytes());
 let b8_cash_claim_hash=hash(format!("{:?}",original.b8_original_bills).as_bytes());
 let input_tapes_sha256=hash(&serde_json::to_vec(&(
   "B6-local-source-inputs-v2",generation,first_end,late_end,
   format!("{:?}",domain),
   original.claimed_okx_account_fingerprint,
   original.claimed_binance_account_fingerprint,
   &original_cash_claim_hash,&b8_cash_claim_hash,&raw_inputs,
 )).map_err(|_|deny("B6 ten original input digest encoding failed"))?);
 let (owner,mut original_pages)=collect_original_seven_source_pages_without_network(
   store,generation,first_end,late_end,domain,original)?;
 let seven=b6_catalogue::inspect_untrusted_original_seven(&owner,&original_pages)
    .map_err(|_|deny("B6 ten original seven failed local validation"))?;
 let (late_owner,late_pages)=collect_late_only_three_pages_without_network(
   store,generation,first_end,late_end,domain,delayed)?;
 let three=b6_catalogue::inspect_untrusted_delayed_three(&late_owner,&late_pages)
    .map_err(|_|deny("B6 ten delayed three failed local validation"))?;
 if owner!=late_owner||seven.lane_count!=7||three.lane_count!=3{
    return Err(deny("B6 ten Native owner/lanes mismatch"))
 }
 let original_source_pages=original_pages.len();
 let delayed_source_pages=late_pages.len();
 original_pages.extend(late_pages);
 let local_b6_shape=b6_catalogue::inspect_untrusted_catalogue(&owner,&original_pages)
    .map_err(|_|deny("B6 ten local cross-window identity/page collision"))?;
 let raw_page_sha256=original_pages.iter()
   .map(|p|p.body_sha256.clone()).collect::<Vec<_>>();
 let transcript=original_pages.iter().map(|p|
   (format!("{:?}",p.lane),p.index,p.endpoint.clone(),
    p.from_ms,p.to_ms,p.order_id.clone(),p.symbol.clone(),
    p.cursor_in.clone(),p.cursor_out.clone(),p.account_fingerprint.clone(),
    p.requested_limit,p.http_status,p.body_sha256.clone())
 ).collect::<Vec<_>>();
 let local_transcript_sha256=hash(&serde_json::to_vec(&(
   "B6-untrusted-local-10-v2",generation,format!("{owner:?}"),
   &input_tapes_sha256,&transcript,
 )).map_err(|_|deny("B6 ten local transcript digest encoding failed"))?);
 if local_b6_shape.lane_count!=10||local_b6_shape.page_count!=original_pages.len(){
   return Err(deny("B6 ten source page coverage incomplete"))
 }
 if store.load().map_err(|_|deny("B6 ten owner final read failed"))?
    .as_ref()!=Some(&before){
   return Err(deny("B6 ten original owner changed during both collectors"))
 }
 Ok(OriginalTenOfflineShape{
   original_source_lanes:seven.lane_count,
   delayed_source_lanes:three.lane_count,
   local_b6_shape,original_source_pages,delayed_source_pages,
   sealed_owner_sha256:owner.native_state_digest,
   raw_page_sha256,input_tapes_sha256,local_transcript_sha256,
 })
}
