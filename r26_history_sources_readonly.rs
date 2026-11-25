//! P0-03 bounded ORIGINAL private GET capture. No exchange mutations,
 //! no order permits, no public log of raw payload or signed URL.
use spotarb_native_market_core::{
 private_https::{fetch_public_time,wall_ms,HttpExecutor,HttpsRequest,PublicTimeVenue,
   ReadOnlyHttpsClient,DEFAULT_MAX_TIME_SAMPLE_RTT_MS,DEFAULT_TIME_SAMPLE_MAX_AGE_MS},
 private_rest::{BinanceCredentials,OkxCredentials,OkxDomain,SignedRequest,HttpMethod,
   BINANCE_DEFAULT_RECV_WINDOW_MS,build_binance_income_time_window_request,
   build_binance_symbol_trade_window_request,build_okx_base_currency_bills_page_request},
};
use serde_json::{Value,json};
use spotarb_native_market_core::native_trade2f3_source_provenance::LocalArchivePageFingerprint;
use openssl::sha::sha256;
use std::{env,fs,io::Write,path::{Path,PathBuf},fs::OpenOptions,
 os::unix::fs::{PermissionsExt,DirBuilderExt,OpenOptionsExt},time::Duration};
fn required(k:&str,alt:&str)->Result<String,String>{
 env::var(k).or_else(|_|env::var(alt)).map_err(|_|format!("missing {k}"))
}
fn domain()->Result<OkxDomain,String>{
 match env::var("R26_OKX_DOMAIN").unwrap_or_else(|_|"global".into()).as_str(){
  "global"=>Ok(OkxDomain::Global),"eea"=>Ok(OkxDomain::Eea),
  "us"=>Ok(OkxDomain::Us),"openapi"=>Ok(OkxDomain::OpenApi),
  _=>Err("invalid OKX domain".into())
 }
}
fn corrected(c:&ReadOnlyHttpsClient,v:PublicTimeVenue)->Result<u64,String>{
 let s=fetch_public_time(c,v).map_err(|_|"time sample failed")?;
 let now=wall_ms();
 if !s.usable_at(now,DEFAULT_TIME_SAMPLE_MAX_AGE_MS,DEFAULT_MAX_TIME_SAMPLE_RTT_MS){
  return Err("unusable time sample".into())
 }
 s.corrected_ms(now).ok_or("time arithmetic".into())
}
fn hex(x:&[u8])->String{x.iter().map(|b|format!("{b:02x}")).collect()}
fn private_write(path:&Path,data:&[u8])->Result<(),String>{
 let mut f=OpenOptions::new().write(true).create_new(true).mode(0o600).open(path)
   .map_err(|_|"refusing existing or unavailable output")?;
 f.set_permissions(fs::Permissions::from_mode(0o600)).map_err(|_|"permissions")?;
 f.write_all(data).and_then(|_|f.sync_all()).map_err(|_|"durable write failed".into())
}
fn rows_match_scope(rows:&[Value],scope:&Value)->bool{
 let Some(begin)=scope["begin"].as_u64() else{return false};
 let Some(end)=scope["end"].as_u64() else{return false};
 let typ=scope["type"].as_str().unwrap_or("");
 let mut ids=std::collections::HashSet::new();
 for row in rows{
  let t=if typ=="currency_bills"{
   row.get("ts").and_then(|v|v.as_str().and_then(|s|s.parse::<u64>().ok())
     .or_else(||v.as_u64()))
  }else{row.get("time").and_then(Value::as_u64)};
  if !t.is_some_and(|x|x>=begin&&x<=end){return false}
  if typ=="currency_bills"{
   if row.get("ccy").and_then(Value::as_str)!=scope["ccy"].as_str(){return false}
   let Some(id)=row.get("billId").and_then(Value::as_str) else{return false};
   if id.is_empty()||!ids.insert(id.to_owned()){return false}
  }else if typ=="symbol_trades"{
   let expected=format!("{}USDT",scope["symbol"].as_str().unwrap_or(""));
   if row.get("symbol").and_then(Value::as_str)!=Some(expected.as_str()){return false}
  }
 }
 true
}
#[derive(Default)]
struct OriginalPageContinuity{
 seen:std::collections::HashSet<String>,
 previous_okx_bill:Option<u128>,
}
fn scalar_identity(row:&Value,key:&str)->Option<String>{
 row.get(key).and_then(|v|{
  v.as_str().map(str::to_owned).or_else(||v.as_u64().map(|n|n.to_string()))
 }).filter(|s|!s.is_empty()&&s.bytes().all(|c|c.is_ascii_digit())&&s!="0")
}
impl OriginalPageContinuity{
 fn accept(&mut self,rows:&[Value],scope:&Value)->Result<(),String>{
  let typ=scope["type"].as_str().ok_or("missing page type")?;
  for row in rows{
   let id=match typ{
    "all_income"=>{
     let kind=row["incomeType"].as_str().filter(|s|!s.is_empty())
       .ok_or("income kind missing")?;
     let transaction=scalar_identity(row,"tranId").ok_or("income transaction missing")?;
     format!("{kind}:{transaction}")
    },
    "symbol_trades"=>scalar_identity(row,"id").ok_or("original trade id missing")?,
    "currency_bills"=>{
     let bill=scalar_identity(row,"billId").ok_or("bill identity missing")?;
     let numeric=bill.parse::<u128>().map_err(|_|"bill ID overflow")?;
     if self.previous_okx_bill.is_some_and(|prior|numeric>=prior){
      return Err("bill IDs not strictly descending".into())
     }
     self.previous_okx_bill=Some(numeric);
     bill
    },
    _=>return Err("unsupported page scope".into())
   };
   if !self.seen.insert(id){return Err("duplicate original row across pages".into())}
  }
  Ok(())
 }
}
fn b1_signed_source_path_fingerprint(
 req:&SignedRequest,label:&str,
)->Result<(String,String),String>{
 let url=url::Url::parse(&req.url).map_err(|_|"signed URL malformed")?;
 if url.scheme()!="https"||url.port().is_some()
    ||url.fragment().is_some()||!url.username().is_empty()||url.password().is_some(){
  return Err("signed source authority malformed".into())
 }
 let endpoint=url.path().to_owned();
 let host=url.host_str().ok_or("signed host missing")?;
 let allowed=if label.starts_with("okx_"){
   endpoint=="/api/v5/account/bills-archive"
    &&matches!(host,"www.okx.com"|"eea.okx.com"|"us.okx.com"|"openapi.okx.com")
 }else{
   host=="fapi.binance.com"
    &&match label{
      s if s.starts_with("binance_income_")=>endpoint=="/fapi/v1/income",
      s if s.starts_with("binance_trades_")=>endpoint=="/fapi/v1/userTrades",
      _=>false
    }
 };
 if !allowed{return Err("signed source endpoint or host unexpected".into())}
 let query=url.query().ok_or("signed source query absent")?;
 let request_path=format!("{endpoint}?{query}");
 Ok((endpoint,hex(&sha256(request_path.as_bytes()))))
}
fn capture(c:&ReadOnlyHttpsClient,req:SignedRequest,dir:&Path,label:&str,
 scope:Value,limit:usize)->Result<(Vec<Value>,Value),String>{
 if req.is_mutation||req.method!=HttpMethod::Get||req.body.is_some(){
  return Err("DENY: non-GET or mutation".into())
 }
 let (endpoint,request_sha)=b1_signed_source_path_fingerprint(&req,label)?;
 let h=HttpsRequest::from_signed(&req);
 if h.mutation{return Err("DENY: transport mutation".into())}
 let resp=c.execute_request(&h).map_err(|_|"HTTPS GET transport rejected")?;
 // Archive original JSON bytes EVEN when remote returns a non-success status.
 let file=format!("{label}.json");
 private_write(&dir.join(&file),resp.body.as_bytes())?;
 let raw_sha=hex(&sha256(resp.body.as_bytes()));
 if !(200..300).contains(&resp.status){return Err(format!("{label}: HTTP not 2xx"))}
 let parsed:Value=serde_json::from_str(&resp.body).map_err(|_|"invalid JSON")?;
 let data=if label.starts_with("okx_"){
  if parsed.get("code").and_then(Value::as_str)!=Some("0"){
   return Err(format!("{label}: OKX code failure"))
  }
  parsed.get("data").and_then(Value::as_array).cloned().ok_or("missing OKX data")?
 }else{
  parsed.as_array().cloned().ok_or("missing Binance array")?
 };
 let n=data.len();
 if n>limit {return Err(format!("{label}: over limit"))}
 if !rows_match_scope(&data,&scope){return Err(format!("{label}: scope mismatch"))}
 Ok((data,json!({"scope":scope,"file":file,"sha256":raw_sha,
   "endpoint":endpoint,"signed_path_query_sha256":request_sha,
   "http":resp.status,"rows":n,"page_filled":n==limit})))
}
fn strict_file(dir:&Path,name:&str,max:usize)->Result<Vec<u8>,String>{
 if name.contains('/')||name.contains('\\')||name=="."||name==".."{
   return Err("invalid source filename".into())
 }
 let p=dir.join(name);
 let meta=fs::symlink_metadata(&p).map_err(|_|"missing original file")?;
 if !meta.file_type().is_file()||meta.permissions().mode()&0o777!=0o600
   ||meta.len()>max as u64{
   return Err("original file type, mode or size denied".into())
 }
 fs::read(p).map_err(|_|"original file read failed".into())
}
fn check_archive_page(
 dir:&Path,entry:&Value,expected:&Value,label:&str,cap:usize,
 tracker:&mut OriginalPageContinuity,
)->Result<usize,String>{
 if entry["scope"]!=*expected ||entry["file"]!=format!("{label}.json")
   ||entry["http"]!=200{return Err("archive page scope mismatch".into())}
 let fingerprint=LocalArchivePageFingerprint::from_untrusted_archive_receipt(entry)
   .map_err(|_|"B1 fingerprint contract refused archived request digest")?;
 let expected_endpoint=match expected["type"].as_str(){
  Some("all_income")=>"/fapi/v1/income",
  Some("symbol_trades")=>"/fapi/v1/userTrades",
  Some("currency_bills")=>"/api/v5/account/bills-archive",
  _=>return Err("unknown archive source class".into()),
 };
 if fingerprint.endpoint!=expected_endpoint{
  return Err("original endpoint differs from archived scope".into())
 }
 let raw=strict_file(dir,&format!("{label}.json"),1_048_576)?;
 let calculated=hex(&sha256(&raw));
 if entry["sha256"]!=calculated{return Err("original SHA256 mismatch".into())}
 let body:Value=serde_json::from_slice(&raw).map_err(|_|"original JSON invalid")?;
 let data=if label.starts_with("okx_"){
   if body["code"]!="0"{return Err("OKX original code mismatch".into())}
   body["data"].as_array().ok_or("OKX original rows missing")?
 }else{body.as_array().ok_or("Binance original rows missing")?};
 if data.len()>cap||entry["rows"]!=data.len()
   ||entry["page_filled"]!=(data.len()==cap)
   ||!rows_match_scope(data,expected){
   return Err("original page count or rows mismatch".into())
 }
 tracker.accept(data,expected)?;
 Ok(data.len())
}
fn verify_archived_sample(dir:&Path)->Result<usize,String>{
 let meta=fs::symlink_metadata(dir).map_err(|_|"capture directory absent")?;
 if !meta.file_type().is_dir()||meta.permissions().mode()&0o777!=0o700{
  return Err("capture directory type or mode denied".into())
 }
 let raw=strict_file(dir,"manifest.json",262_144)?;
 let m:Value=serde_json::from_slice(&raw).map_err(|_|"manifest JSON invalid")?;
 if m["schema"]!="p003_private_original_get_sample_v3"
   ||m["account_wide_completeness"]!=false
   ||m["real_trading_authorized"]!=false
   ||m["original_exchange_rows_attested_for_real_trade"]!=false{
   return Err("archive manifest authority misrepresented".into())
 }
 let records=m["pages"].as_array().ok_or("archive page list missing")?;
 if records.len()<8||records.len()>14{
   return Err("archive page count unexpected".into())
 }
 let bn_end=m["binance_sample_window_end_ms"].as_u64().ok_or("Binance end absent")?;
 let ok_end=m["okx_sample_window_end_ms"].as_u64().ok_or("OKX end absent")?;
 const DAY:u64=86_400_000;
 let bn_start=bn_end.checked_add(1).and_then(|x|x.checked_sub(2*DAY))
   .filter(|x|*x>0).ok_or("Binance archive window invalid")?;
 let ok_start=ok_end.checked_add(1).and_then(|x|x.checked_sub(2*DAY))
   .filter(|x|*x>0).ok_or("OKX archive window invalid")?;
 let mut i=0usize;
 let mut exhausted=true;
 let mut income=OriginalPageContinuity::default();
 for n in 0..2_u64{
  let begin=bn_start+n*DAY;
  let end=begin+DAY-1;
  let expected=json!({"venue":"binance","type":"all_income","begin":begin,"end":end});
  let rec=records.get(i).ok_or("income original page missing")?;
  let rows=check_archive_page(dir,rec,&expected,&format!("binance_income_{n}"),1000,&mut income)?;
  exhausted &=rows<1000;
  i+=1;
 }
 for symbol in ["BTC","ONE","BNT"]{
  let expected=json!({"venue":"binance","type":"symbol_trades",
    "symbol":symbol,"begin":bn_start,"end":bn_end});
  let rec=records.get(i).ok_or("futures original page missing")?;
  let rows=check_archive_page(dir,rec,&expected,&format!("binance_trades_{symbol}"),
      1000,&mut OriginalPageContinuity::default())?;
  exhausted &=rows<1000;
  i+=1;
 }
 for ccy in ["USDT","ONE","BNT"]{
  let mut continuity=OriginalPageContinuity::default();
  for page in 0..3_u8{
   let expected=json!({"venue":"okx","type":"currency_bills",
    "ccy":ccy,"begin":ok_start,"end":ok_end,
    "page_index":page,"after_present":page!=0,
    "after_bill_id":continuity.previous_okx_bill.map(|v|v.to_string())});
   let rec=records.get(i).ok_or("OKX original page missing")?;
   let rows=check_archive_page(dir,rec,&expected,&format!("okx_bills_{ccy}_{page}"),
      100,&mut continuity)?;
   i+=1;
   if rows<100{break}
   if page==2{exhausted=false}
  }
 }
 if i!=records.len()||m["scoped_pages_exhausted"]!=exhausted{
  return Err("archive missing, surplus or falsely complete pages".into())
 }
 let file_count=fs::read_dir(dir).map_err(|_|"archive directory unreadable")?
  .try_fold(0usize,|count,entry|{
    entry.map(|_|count+1).map_err(|_|"unreadable directory entry".to_string())
  })?;
 if file_count!=i+1{return Err("unlisted original file or missing manifest".into())}
 Ok(i)
}
#[cfg(test)]
mod tests{
 use super::*;
 fn scope(typ:&str)->Value{
  json!({"type":typ,"begin":100,"end":200,"ccy":"USDT","symbol":"BTC"})
 }
 fn archived_fixture()->PathBuf{
  let nonce=std::time::SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
  let dir=env::temp_dir().join(format!("spotarb-p003-{}-{nonce}",std::process::id()));
  fs::DirBuilder::new().mode(0o700).create(&dir).unwrap();
  const DAY:u64=86_400_000;
  let end=DAY*4;
  let start=end+1-2*DAY;
  let mut pages=Vec::<Value>::new();
  let mut add=|name:String,scope:Value,okx:bool|{
   let raw=if okx{br#"{"code":"0","data":[]}"#.to_vec()}else{b"[]".to_vec()};
   private_write(&dir.join(format!("{name}.json")),&raw).unwrap();
   let endpoint=match scope["type"].as_str(){
    Some("all_income")=>"/fapi/v1/income",
    Some("symbol_trades")=>"/fapi/v1/userTrades",
    Some("currency_bills")=>"/api/v5/account/bills-archive",
    _=>panic!("invalid fixture scope"),
   };
   let signed_path_query_sha256=hex(&sha256(format!(
     "fake-signed-GET-path-and-query:{name}").as_bytes()));
   pages.push(json!({"scope":scope,"file":format!("{name}.json"),
     "endpoint":endpoint,"signed_path_query_sha256":signed_path_query_sha256,
     "sha256":hex(&sha256(&raw)),"http":200,"rows":0,"page_filled":false}));
  };
  for i in 0..2_u64{
    let begin=start+i*DAY;
    add(format!("binance_income_{i}"),
      json!({"venue":"binance","type":"all_income",
        "begin":begin,"end":begin+DAY-1}),false);
  }
  for symbol in ["BTC","ONE","BNT"]{
    add(format!("binance_trades_{symbol}"),
      json!({"venue":"binance","type":"symbol_trades",
        "symbol":symbol,"begin":start,"end":end}),false);
  }
  for ccy in ["USDT","ONE","BNT"]{
    add(format!("okx_bills_{ccy}_0"),
      json!({"venue":"okx","type":"currency_bills",
        "ccy":ccy,"begin":start,"end":end,
        "page_index":0,"after_present":false,"after_bill_id":null}),true);
  }
  let manifest=json!({"schema":"p003_private_original_get_sample_v3",
     "binance_sample_window_end_ms":end,
     "okx_sample_window_end_ms":end,"scoped_pages_exhausted":true,
     "account_wide_completeness":false,
     "original_exchange_rows_attested_for_real_trade":false,
     "real_trading_authorized":false,"pages":pages});
  private_write(&dir.join("manifest.json"),
    &serde_json::to_vec_pretty(&manifest).unwrap()).unwrap();
  dir
 }
 #[test]
 fn p003_reject_duplicate_binance_income_across_disjoint_windows(){
  let mut audit=OriginalPageContinuity::default();
  let row=json!({"incomeType":"FUNDING_FEE","tranId":111,"time":150});
  audit.accept(&[row.clone()],&scope("all_income")).unwrap();
  assert!(audit.accept(&[row],&scope("all_income")).is_err());
  assert!(audit.accept(&[json!({"incomeType":"COMMISSION","tranId":111})],
    &scope("all_income")).is_ok());
 }
 #[test]
 fn p003_okx_cursor_and_cross_page_bill_order_enforced(){
  let mut audit=OriginalPageContinuity::default();
  audit.accept(&[json!({"billId":"300"}),json!({"billId":"200"})],
    &scope("currency_bills")).unwrap();
  assert!(audit.accept(&[json!({"billId":"200"})],
    &scope("currency_bills")).is_err());
  assert!(audit.accept(&[json!({"billId":"220"})],
    &scope("currency_bills")).is_err());
 }
 #[test]
 fn p003_symbol_and_timestamp_checks_are_exact(){
  let valid=json!({"symbol":"BTCUSDT","time":150});
  assert!(rows_match_scope(&[valid],&scope("symbol_trades")));
  assert!(!rows_match_scope(&[json!({"symbol":"ETHUSDT","time":150})],
    &scope("symbol_trades")));
  assert!(!rows_match_scope(&[json!({"symbol":"BTCUSDT","time":201})],
    &scope("symbol_trades")));
  assert!(!rows_match_scope(&[json!({"symbol":"BTCUSDT"})],
    &scope("symbol_trades")));
 }
 #[test]
 fn p003_trade_identity_and_income_identity_missing_denied(){
  let mut audit=OriginalPageContinuity::default();
  assert!(audit.accept(&[json!({"id":0})],&scope("symbol_trades")).is_err());
  assert!(audit.accept(&[json!({"id":42})],&scope("symbol_trades")).is_ok());
  assert!(audit.accept(&[json!({"id":42})],&scope("symbol_trades")).is_err());
  assert!(OriginalPageContinuity::default().accept(
    &[json!({"incomeType":"COMMISSION"})],&scope("all_income")).is_err());
 }
 #[test]
 fn p003_archived_originals_verify_without_credentials(){
  let dir=archived_fixture();
  assert_eq!(verify_archived_sample(&dir).unwrap(),8);
  fs::remove_dir_all(dir).unwrap();
 }
 #[test]
 fn p003_tampered_original_and_missing_page_are_denied(){
  let dir=archived_fixture();
  fs::write(dir.join("binance_income_0.json"),b"[{}]").unwrap();
  assert!(verify_archived_sample(&dir).is_err());
  fs::remove_dir_all(dir).unwrap();
  let dir=archived_fixture();
  fs::remove_file(dir.join("okx_bills_BNT_0.json")).unwrap();
  assert!(verify_archived_sample(&dir).is_err());
  fs::remove_dir_all(dir).unwrap();
 }
 #[test]
 fn p003_manifest_scope_elevation_or_unlisted_file_is_denied(){
  let dir=archived_fixture();
  let mpath=dir.join("manifest.json");
  let mut manifest:Value=serde_json::from_slice(&fs::read(&mpath).unwrap()).unwrap();
  manifest["account_wide_completeness"]=json!(true);
  fs::write(&mpath,serde_json::to_vec(&manifest).unwrap()).unwrap();
  assert!(verify_archived_sample(&dir).is_err());
  fs::remove_dir_all(dir).unwrap();
  let dir=archived_fixture();
  private_write(&dir.join("extra.json"),b"{}").unwrap();
  assert!(verify_archived_sample(&dir).is_err());
  fs::remove_dir_all(dir).unwrap();
 }
 #[test]
 fn p003_forged_scope_or_false_completion_is_denied(){
  let dir=archived_fixture();
  let file=dir.join("manifest.json");
  let mut m:Value=serde_json::from_slice(&fs::read(&file).unwrap()).unwrap();
  m["pages"][0]["scope"]["begin"]=json!(9999_u64);
  fs::write(&file,serde_json::to_vec(&m).unwrap()).unwrap();
  assert!(verify_archived_sample(&dir).is_err());
  fs::remove_dir_all(dir).unwrap();
  let dir=archived_fixture();
  let file=dir.join("manifest.json");
  let mut m:Value=serde_json::from_slice(&fs::read(&file).unwrap()).unwrap();
  m["scoped_pages_exhausted"]=json!(false);
  fs::write(&file,serde_json::to_vec(&m).unwrap()).unwrap();
  assert!(verify_archived_sample(&dir).is_err());
  fs::remove_dir_all(dir).unwrap();
 }
 #[test]
 fn p003_okx_page_after_cursor_claim_must_match_original(){
  let dir=archived_fixture();
  let file=dir.join("manifest.json");
  let mut m:Value=serde_json::from_slice(&fs::read(&file).unwrap()).unwrap();
  m["pages"][5]["scope"]["after_bill_id"]=json!("123456");
  fs::write(&file,serde_json::to_vec(&m).unwrap()).unwrap();
  assert!(verify_archived_sample(&dir).is_err());
  fs::remove_dir_all(dir).unwrap();
 }
 #[test]
 fn p003_full_income_page_is_explicitly_not_exhausted(){
  let dir=archived_fixture();
  let file=dir.join("binance_income_0.json");
  let manifest_file=dir.join("manifest.json");
  let mut m:Value=serde_json::from_slice(&fs::read(&manifest_file).unwrap()).unwrap();
  let at=m["pages"][0]["scope"]["begin"].as_u64().unwrap();
  let rows=(1..=1000_u64).map(|i|json!({
    "time":at,"incomeType":"COMMISSION","tranId":i
  })).collect::<Vec<_>>();
  let raw=serde_json::to_vec(&rows).unwrap();
  fs::write(file,&raw).unwrap();
  m["pages"][0]["sha256"]=json!(hex(&sha256(&raw)));
  m["pages"][0]["rows"]=json!(1000);
  m["pages"][0]["page_filled"]=json!(true);
  m["scoped_pages_exhausted"]=json!(false);
  fs::write(&manifest_file,serde_json::to_vec(&m).unwrap()).unwrap();
  assert_eq!(verify_archived_sample(&dir).unwrap(),8);
  m["scoped_pages_exhausted"]=json!(true);
  fs::write(&manifest_file,serde_json::to_vec(&m).unwrap()).unwrap();
  assert!(verify_archived_sample(&dir).is_err());
  fs::remove_dir_all(dir).unwrap();
 }
 #[test]
 fn p003_reject_readable_original_and_symlinked_page(){
  let dir=archived_fixture();
  let original=dir.join("binance_income_0.json");
  fs::set_permissions(&original,fs::Permissions::from_mode(0o644)).unwrap();
  assert!(verify_archived_sample(&dir).is_err());
  fs::remove_dir_all(dir).unwrap();
  let dir=archived_fixture();
  let original=dir.join("binance_income_0.json");
  fs::rename(&original,dir.join("saved.json")).unwrap();
  std::os::unix::fs::symlink(dir.join("saved.json"),&original).unwrap();
  assert!(verify_archived_sample(&dir).is_err());
  fs::remove_dir_all(dir).unwrap();
 }
 #[test]
 fn p003_v3_requires_b1_signed_path_digest_shape_and_expected_endpoint(){
  let dir=archived_fixture();
  assert_eq!(verify_archived_sample(&dir).unwrap(),8);
  let file=dir.join("manifest.json");
  let mut m:Value=serde_json::from_slice(&fs::read(&file).unwrap()).unwrap();
  m["pages"][0]["signed_path_query_sha256"]=json!("not-a-sha");
  fs::write(&file,serde_json::to_vec(&m).unwrap()).unwrap();
  assert!(verify_archived_sample(&dir).is_err());
  m["pages"][0]["signed_path_query_sha256"]=json!("a".repeat(64));
  m["pages"][0]["endpoint"]=json!("/fapi/v1/userTrades");
  fs::write(&file,serde_json::to_vec(&m).unwrap()).unwrap();
  assert!(verify_archived_sample(&dir).is_err());
  m["schema"]=json!("p003_private_original_get_sample_v2");
  fs::write(&file,serde_json::to_vec(&m).unwrap()).unwrap();
  assert!(verify_archived_sample(&dir).is_err());
  fs::remove_dir_all(dir).unwrap();
 }
 #[test]
 fn p003_signed_get_path_hash_matches_b1_raw_request_bytes(){
  let bin=BinanceCredentials::new("TEST_BIN_KEY","TEST_BIN_SECRET").unwrap();
  let now=1_700_000_000_000;
  let req=build_binance_income_time_window_request(
    &bin,now,5000,now-86_400_000,now-1,1000).unwrap();
  let (endpoint,sha)=b1_signed_source_path_fingerprint(&req,"binance_income_0").unwrap();
  assert_eq!(endpoint,"/fapi/v1/income");
  let url=url::Url::parse(&req.url).unwrap();
  let path=format!("{}?{}",url.path(),url.query().unwrap());
  assert_eq!(sha,hex(&sha256(path.as_bytes())));
  assert!(b1_signed_source_path_fingerprint(&req,"binance_trades_BTC").is_err());
  let mut altered=req.clone();
  altered.url=altered.url.replacen("https://","http://",1);
  assert!(b1_signed_source_path_fingerprint(&altered,"binance_income_0").is_err());
  let okx=OkxCredentials::new("KEY","SECRET","PASS").unwrap();
  let o=build_okx_base_currency_bills_page_request(
     &okx,now,OkxDomain::Global,"USDT",now-86_400_000,now-1,None,100).unwrap();
  let (ep,_)=b1_signed_source_path_fingerprint(&o,"okx_bills_USDT_0").unwrap();
  assert_eq!(ep,"/api/v5/account/bills-archive");
 }
}
fn main()->Result<(),String>{
 let args:Vec<String>=env::args().collect();
 if args.len()==3&&args[1]=="--verify-archive"{
  let path=PathBuf::from(&args[2]);
  let root=Path::new("/mnt/spotarb_db_test/astra_p1_06_config_audit_20261018/private_history_20261019");
  if path.parent()!=Some(root)||!path.file_name()
    .and_then(|n|n.to_str()).is_some_and(|n|n.starts_with("capture_")
      &&n.len()==24&&n.bytes().all(|b|b.is_ascii_alphanumeric()||b==b'_')){
    return Err("offline verification path denied".into())
  }
  let count=verify_archived_sample(&path)?;
  println!("OFFLINE LOCAL ARCHIVE INTEGRITY ONLY: {count} source pages; account_wide_authenticated=false; may_trade=false");
  return Ok(())
 }
 if args.len()!=1{return Err("unknown collector arguments".into())}
 if env::var("R26_HISTORY_GET_AUTHORIZED").ok().as_deref()!=Some("YES"){
  return Err("explicit GET-only authorization missing".into())
 }
 let dir=PathBuf::from(env::var("R26_HISTORY_OUT").map_err(|_|"missing output path")?);
 let safe_root=Path::new("/mnt/spotarb_db_test/astra_p1_06_config_audit_20261018/private_history_20261019");
 if dir.parent()!=Some(safe_root)||!dir.file_name()
   .and_then(|s|s.to_str()).is_some_and(|s|s.starts_with("capture_")
   &&s.len()==24&&s.bytes().all(|b|b.is_ascii_alphanumeric()||b==b'_')){
  return Err("output scope denied".into())
 }
 let safe_real=fs::canonicalize(safe_root).map_err(|_|"safe root missing")?;
 if safe_real!=safe_root{return Err("output root noncanonical".into())}
 fs::DirBuilder::new().mode(0o700).create(&dir)
   .map_err(|_|"refusing reused capture directory")?;
 fs::set_permissions(&dir,fs::Permissions::from_mode(0o700)).map_err(|_|"dir chmod")?;
 let bin=BinanceCredentials::new(
   &required("R26_BINANCE_API_KEY","BINANCE_KEY")?,
   &required("R26_BINANCE_SECRET","BINANCE_SECRET")?)
   .map_err(|_|"binance credential validation")?;
 let okx=OkxCredentials::new(
   &required("R26_OKX_API_KEY","OKX_KEY")?,
   &required("R26_OKX_SECRET","OKX_SECRET")?,
   &required("R26_OKX_PASSPHRASE","OKX_PASSPHRASE")?)
   .map_err(|_|"okx credential validation")?;
 let domain=domain()?;
 let client=ReadOnlyHttpsClient::default();
 let bt=corrected(&client,PublicTimeVenue::BinanceUsdm)?;
 let ot=corrected(&client,PublicTimeVenue::Okx(domain))?;
 const DAY:u64=86_400_000;
 let mut manifest=Vec::<Value>::new();
 let mut scope_complete=true;
 // Two disjoint 24-hour windows; sampling has no account-wide authority.
 let mut income_continuity=OriginalPageContinuity::default();
 for i in 0..2_u64{
  let begin=bt-2*DAY+i*DAY;
  let end=begin+DAY-1;
  let req=build_binance_income_time_window_request(
     &bin,bt,BINANCE_DEFAULT_RECV_WINDOW_MS,begin,end,1000)
     .map_err(|_|"income builder")?;
  let (rows,entry)=capture(&client,req,&dir,&format!("binance_income_{i}"),
      json!({"venue":"binance","type":"all_income","begin":begin,"end":end}),1000)?;
  income_continuity.accept(&rows,&entry["scope"])?;
  scope_complete &=rows.len()<1000;
  manifest.push(entry);
  std::thread::sleep(Duration::from_millis(400));
 }
 // SAMPLE symbols only. Do not claim account-wide executions from these.
 for symbol in ["BTC","ONE","BNT"]{
  let req=build_binance_symbol_trade_window_request(
    &bin,bt,BINANCE_DEFAULT_RECV_WINDOW_MS,symbol,bt-2*DAY,bt-1,1000)
    .map_err(|_|"trade builder")?;
  let (rows,entry)=capture(&client,req,&dir,&format!("binance_trades_{symbol}"),
    json!({"venue":"binance","type":"symbol_trades","symbol":symbol,
      "begin":bt-2*DAY,"end":bt-1}),1000)?;
  OriginalPageContinuity::default().accept(&rows,&entry["scope"])?;
  scope_complete &=rows.len()<1000;
  manifest.push(entry);
  std::thread::sleep(Duration::from_millis(400));
 }
 // Only selected asset currencies; account-wide OKX bill census is NOT proven.
 for currency in ["USDT","ONE","BNT"]{
  let mut after:Option<String>=None;
  let mut bills_continuity=OriginalPageContinuity::default();
  for page in 0..3_u8{
   let req=build_okx_base_currency_bills_page_request(
     &okx,ot,domain,currency,ot-2*DAY,ot-1,after.as_deref(),100)
     .map_err(|_|"OKX bills builder")?;
   let (rows,entry)=capture(&client,req,&dir,
     &format!("okx_bills_{currency}_{page}"),
     json!({"venue":"okx","type":"currency_bills",
        "ccy":currency,"begin":ot-2*DAY,"end":ot-1,
        "page_index":page,"after_present":after.is_some(),
        "after_bill_id":after}),100)?;
   bills_continuity.accept(&rows,&entry["scope"])?;
   let n=rows.len();
   manifest.push(entry);
   if n<100 {break}
   let cursor=rows.last()
     .and_then(|r|r.get("billId")).and_then(Value::as_str)
     .ok_or("full OKX page missing billId")?.to_owned();
   if cursor.is_empty()||after.as_deref()==Some(&cursor){
      return Err("OKX cursor continuity fail".into())
   }
   after=Some(cursor);
   if page==2{scope_complete=false}
   std::thread::sleep(Duration::from_millis(450));
  }
  std::thread::sleep(Duration::from_millis(450));
 }
 let out=json!({
  "schema":"p003_private_original_get_sample_v3",
  "binance_sample_window_end_ms":bt-1,
  "okx_sample_window_end_ms":ot-1,
  "scoped_pages_exhausted":scope_complete,
  "scope":"sample-only: Binance two-day income and BTC/ONE/BNT trades; OKX USDT/ONE/BNT bills",
  "account_wide_completeness":false,
  "original_exchange_rows_attested_for_real_trade":false,
  "real_trading_authorized":false,
  "pages":manifest,
 });
 private_write(&dir.join("manifest.json"),
    &serde_json::to_vec_pretty(&out).map_err(|_|"manifest serialization")?)?;
 let dir_handle=fs::File::open(&dir).map_err(|_|"dir sync open")?;
 dir_handle.sync_all().map_err(|_|"dir sync failed")?;
 let reverified=verify_archived_sample(&dir)?;
 if reverified!=manifest.len(){return Err("archived page count changed".into())}
 println!("GET-only sample archived; scoped_pages_exhausted={} files={}; account_wide_complete=false",
   scope_complete,out["pages"].as_array().map(|x|x.len()).unwrap_or_default());
 Ok(())
}
