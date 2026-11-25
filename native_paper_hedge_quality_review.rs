//! R59: public-only PAPER spot -> futures entry-quality observations.
 //! Consumes independently verified modeled hash-chain, never exchange fills.
use crate::native_virtual_four_leg_provenance_review::{
 inspect_model_journal,ModelAuditPayload
};
use serde_json::{json,Value};
use std::{fs::OpenOptions,io::{Read,BufRead,BufReader},path::Path,os::unix::fs::OpenOptionsExt};

const MAX:u64=8*1024*1024;
#[derive(Debug,Clone,PartialEq,Eq)]
pub enum QualityError{Source,Incomplete,Invalid}
fn value(x:&Value,key:&str)->Result<f64,QualityError>{
 let n=x.get(key).and_then(Value::as_f64).ok_or(QualityError::Invalid)?;
 if !n.is_finite()||n.abs()>100_000.0{return Err(QualityError::Invalid)}
 Ok(n)
}
pub fn analyze_verified_model_rows(rows:&[ModelAuditPayload])->Result<Value,QualityError>{
 let mut samples=Vec::new();
 let mut negative=0_u64;
 let mut deteriorated_42=0_u64;
 let mut total_deterioration=0.0;
 for event in rows{
  if event.stage!="FUTURES_SHORT_MODELED"{continue}
  if event.source!="PUBLIC_DEPTH_MODEL_ONLY"||event.exchange_fills_attested
    ||event.real_orders_authorized{return Err(QualityError::Invalid)}
  let phase=&event.details["next_virtual_phase"]["Hedged"];
  let signal=value(phase,"signal_estimated_net_bps")?;
  let hedge=value(phase,"hedge_entry_estimated_net_bps")?;
  let spot_ns=phase.get("spot_buy_ns").and_then(Value::as_u64)
   .ok_or(QualityError::Invalid)?;
  let hedge_ns=phase.get("futures_short_ns").and_then(Value::as_u64)
   .ok_or(QualityError::Invalid)?;
  if hedge_ns<spot_ns||hedge_ns-spot_ns>300_000_000_000{
   return Err(QualityError::Invalid)
  }
  let deterioration=signal-hedge;
  if hedge<0.0{negative+=1;}
  if deterioration>42.0{deteriorated_42+=1;}
  total_deterioration+=deterioration.max(0.0);
  if event.base.is_empty()||event.base.len()>32||
    !event.base.bytes().all(|b|b.is_ascii_uppercase()||b.is_ascii_digit()){
   return Err(QualityError::Invalid)
  }
  if samples.len()>=500{return Err(QualityError::Invalid)}
  samples.push(json!({"base":event.base,
   "signal_estimated_net_bps":(signal*1000.0).round()/1000.0,
   "modeled_hedge_net_bps":(hedge*1000.0).round()/1000.0,
   "adverse_change_bps":(deterioration.max(0.0)*1000.0).round()/1000.0,
   "spot_to_hedge_ms":(hedge_ns-spot_ns) as f64/1_000_000.0,
   "negative_hedge_estimate":hedge<0.0,
   "real_exchange_execution_attested":false}));
 }
 Ok(json!({
  "schema":"native_paper_hedge_quality_r59_v1",
  "modeled_hedge_entries":samples.len(),
  "negative_hedge_estimate_count":negative,
  "adverse_change_over_42_bps_count":deteriorated_42,
  "sum_adverse_change_bps":(total_deterioration*1000.0).round()/1000.0,
  "samples":samples,
  "observed_source":"VERIFIED_MODELED_FOUR_LEG_JOURNAL",
  "may_trade_or_adjust_config":false,
  "authenticated_fills":false,
  "real_pnl_certified":false
 }))
}
/// Bounded owner-session journal, checked before AND after read for rotation.
pub fn inspect_modeled_hedge_quality(path:&Path)->Result<Value,QualityError>{
 let first=inspect_model_journal(path).map_err(|_|QualityError::Source)?;
 let meta=std::fs::symlink_metadata(path).map_err(|_|QualityError::Source)?;
 if !meta.file_type().is_file()||meta.len()>MAX||meta.len()==0{
  return Err(QualityError::Source)
 }
 let f=OpenOptions::new().read(true)
  .custom_flags(libc::O_NOFOLLOW|libc::O_CLOEXEC)
  .open(path).map_err(|_|QualityError::Source)?;
 let mut lines=BufReader::new(f).take(MAX+1);
 let mut buf=String::new();
 let mut rows=Vec::new();
 let mut last=String::new();
 let mut seq=0_u64;
 let mut read=0_usize;
 loop {
  buf.clear();
  let n=lines.read_line(&mut buf).map_err(|_|QualityError::Source)?;
  if n==0{break}
  read=read.checked_add(n).ok_or(QualityError::Source)?;
  if read as u64>MAX||!buf.ends_with('\n'){return Err(QualityError::Incomplete)}
  let line:Value=serde_json::from_str(&buf).map_err(|_|QualityError::Source)?;
  seq=line.get("seq").and_then(Value::as_u64).ok_or(QualityError::Source)?;
  last=line.get("checksum").and_then(Value::as_str)
   .ok_or(QualityError::Source)?.to_owned();
  let body=line.get("body").and_then(Value::as_str).ok_or(QualityError::Source)?;
  let row:ModelAuditPayload=serde_json::from_str(body).map_err(|_|QualityError::Source)?;
  if row.stage=="FUTURES_SHORT_MODELED"{rows.push(row)}
 }
 let second=inspect_model_journal(path).map_err(|_|QualityError::Source)?;
 if first!=second||first.sequence!=seq||first.last_hash!=last{
  return Err(QualityError::Incomplete)
 }
 let mut quality=analyze_verified_model_rows(&rows)?;
 quality["verified_journal_sequence"]=json!(seq);
 quality["verified_session"]=json!(first.session);
 quality["modeled_portfolio_entries"]=json!(first.entries);
 quality["modeled_portfolio_closed"]=json!(first.closed);
 quality["modeled_portfolio_open"]=json!(first.open);
 Ok(quality)
}

#[cfg(test)]
mod tests {
 use super::*;
 fn stage(base:&str,signal:f64,hedge:f64)->ModelAuditPayload{
  ModelAuditPayload{
   schema:"SPOTARB/PUBLIC-PAPER/MODELED-FOUR-LEG/V1".into(),
   session:"session_20742_1792133159".into(),
   stage:"FUTURES_SHORT_MODELED".into(),base:base.into(),
   entries:1,closed:0,open:1,modeled_realized_usdt:0.0,
   details:json!({"next_virtual_phase":{"Hedged":{
    "signal_estimated_net_bps":signal,
    "hedge_entry_estimated_net_bps":hedge,
    "spot_buy_ns":1_000_000_000_u64,
    "futures_short_ns":1_050_000_000_u64
   }}}),source:"PUBLIC_DEPTH_MODEL_ONLY".into(),
   exchange_fills_attested:false,real_orders_authorized:false,
   native_owner_verified_closed:false,
  }
 }
 #[test]
 fn adverse_paper_entry_is_flagged_without_granting_real_trading(){
  let x=analyze_verified_model_rows(&[
   stage("PARTI",44.097,-38.840),stage("VELODROME",167.285,132.891)
  ]).unwrap();
  assert_eq!(x["modeled_hedge_entries"],2);
  assert_eq!(x["negative_hedge_estimate_count"],1);
  assert_eq!(x["adverse_change_over_42_bps_count"],1);
  assert_eq!(x["samples"][0]["spot_to_hedge_ms"],50.0);
  assert_eq!(x["real_pnl_certified"],false);
  assert_eq!(x["may_trade_or_adjust_config"],false);
 }
 #[test]
 fn missing_or_forged_market_evidence_is_rejected(){
  let mut x=stage("HUMA",55.0,-36.0);
  x.details["next_virtual_phase"]["Hedged"]["futures_short_ns"]=json!(1);
  assert_eq!(analyze_verified_model_rows(&[x]),Err(QualityError::Invalid));
  let mut x=stage("HUMA",55.0,-36.0);
  x.real_orders_authorized=true;
  assert_eq!(analyze_verified_model_rows(&[x]),Err(QualityError::Invalid));
  let mut x=stage("HUMA",55.0,-36.0);
  x.details["next_virtual_phase"]["Hedged"]
    ["hedge_entry_estimated_net_bps"]=Value::Null;
  assert_eq!(analyze_verified_model_rows(&[x]),Err(QualityError::Invalid));
 }
 #[test]
 fn empty_untrusted_rows_never_certify_exchange_fills(){
  let x=analyze_verified_model_rows(&[]).unwrap();
  assert_eq!(x["modeled_hedge_entries"],0);
  assert_eq!(x["authenticated_fills"],false);
  assert_eq!(x["real_pnl_certified"],false);
 }
}
