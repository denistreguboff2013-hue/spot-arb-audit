//! R49: one-shot OFFLINE Rust AI aggregate report review (stdin -> stdout).
 //! No database, config, API credentials, HTTP, filesystem writes, or orders.
use std::io::{self,Read};
use serde_json::{json,Value};
use spotarb_native_market_core::native_ai_input_review::{
 sanitize_daily_report,advisory_from_report_without_io
};

fn review(input:&str)->Result<Value,&'static str>{
 if input.len()>1_000_000{return Err("INPUT_TOO_LARGE")}
 let report:Value=serde_json::from_str(input).map_err(|_|"INVALID_JSON")?;
 let sanitized=sanitize_daily_report(&report).map_err(|_|"SANITIZER_DENIED")?;
 let advice=advisory_from_report_without_io(&report)
   .map_err(|_|"ADVISOR_DENIED")?;
 Ok(json!({"schema":"native_r49_offline_advisor_v1",
  "advisor_input":sanitized,"advice":advice,
  "telegram_sent":false,"network_used":false,"real_orders":false,
  "config_modified":false,"database_modified":false}))
}
fn main(){
 let mut input=String::new();
 match io::stdin().take(1_000_001).read_to_string(&mut input){
  Ok(_)=>match review(&input){
   Ok(v)=>println!("{}",v),
   Err(e)=>{eprintln!("OFFLINE_AI_REVIEW_DENIED: {e}");std::process::exit(2)}
  },
  Err(_)=>{eprintln!("OFFLINE_AI_INPUT_READ_DENIED");std::process::exit(2)}
 }
}

#[cfg(test)]
mod tests{
 use super::*;
 #[test]
 fn malformed_or_huge_reports_are_rejected(){
  assert_eq!(review("not json"),Err("INVALID_JSON"));
  assert_eq!(review("{}"),Err("SANITIZER_DENIED"));
  assert_eq!(review(&"0".repeat(1_000_001)),Err("INPUT_TOO_LARGE"));
 }
 #[test]
 fn safe_aggregate_report_produces_only_pending_advice(){
  let mut modes=serde_json::Map::new();
  for mode in ["paper","real","test","replay"]{
   modes.insert(mode.into(),json!({}));
  }
  let v=json!({"date":"2026-10-16","timezone":"Asia/Yekaterinburg",
   "modes":modes,"storage_health":{"status":"OK"},
   "evidence_quality":{"verified_real_exits":0}});
  let r=review(&v.to_string()).unwrap();
  assert_eq!(r["advice"]["recommendation_count"],1);
  assert_eq!(r["advice"]["policy"]["order_submission_allowed"],false);
  assert_eq!(r["network_used"],false);
  assert_eq!(r["database_modified"],false);
 }
}
