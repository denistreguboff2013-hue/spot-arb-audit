//! R51: bounded review of native PAPER ingest rejections.
 //! OutOfOrder remains a rejected frame, never silently accepted as fresh.
use serde_json::{json,Value};

#[derive(Clone,Debug,PartialEq,Eq)]
pub enum IngestQualityError{Malformed,TooMany,Contradiction}

fn count(v:&Value,name:&str)->Result<u64,IngestQualityError>{
 v.get(name).and_then(Value::as_u64).ok_or(IngestQualityError::Malformed)
}
pub fn inspect_paper_ingest_samples(status:&Value)->Result<Value,IngestQualityError>{
 let errors=status.get("errors").ok_or(IngestQualityError::Malformed)?;
 let total=count(errors,"ingest")?;
 let parse=count(errors,"parse")?;
 let decision=count(errors,"decision")?;
 let frame_total=count(status,"frames")?;
 if total>frame_total||parse>frame_total||decision>frame_total{
  return Err(IngestQualityError::Contradiction)
 }
 let samples=status.get("ingest_error_samples")
  .and_then(Value::as_array).ok_or(IngestQualityError::Malformed)?;
 if samples.len()>20 || samples.len() as u64>total{
  return Err(IngestQualityError::TooMany)
 }
 let mut out_of_order=0u64;
 let mut other=0u64;
 let mut examples=Vec::new();
 for row in samples{
  let base=row.get("base").and_then(Value::as_str)
   .ok_or(IngestQualityError::Malformed)?;
  let venue=row.get("venue").and_then(Value::as_str)
   .ok_or(IngestQualityError::Malformed)?;
  let error=row.get("error").and_then(Value::as_str)
   .ok_or(IngestQualityError::Malformed)?;
  let stage=row.get("stage").and_then(Value::as_str)
   .ok_or(IngestQualityError::Malformed)?;
  if base.len()>32||!base.bytes().all(|b|b.is_ascii_uppercase()||b.is_ascii_digit())||
    !["OkxSpot","BinanceFutures"].contains(&venue)||
    !["ingest_parsed","missing_book"].contains(&stage)||
    error.len()>80||!error.bytes().all(|b|b.is_ascii_alphanumeric()||
       b==b'_'||b==b'('||b==b')'||b==b' '||b==b'"'){
    return Err(IngestQualityError::Malformed)
  }
  if error=="OutOfOrder"{out_of_order+=1}else{other+=1}
  examples.push(json!({"base":base,"venue":venue,"stage":stage,
   "rejection":error}));
 }
 Ok(json!({
  "schema":"r51_native_paper_ingest_quality_v1",
  "frames_seen":frame_total,
  "ingest_rejections":total,
  "decision_errors":decision,
  "parse_errors":parse,
  "sampled_out_of_order":out_of_order,
  "sampled_other":other,
  "sample_count":samples.len(),
  "unsampled_ingest_rejections":total-samples.len() as u64,
  "cause_certified":false,
  "requires_review":total>0||parse>0||decision>0,
  "change_live_book_ordering_allowed":false,
  "real_trading_allowed":false,
  "examples":examples
 }))
}

#[cfg(test)]
mod tests{
 use super::*;
 fn status()->Value{
  json!({"frames":4374313,
   "errors":{"ingest":1,"parse":0,"decision":0},
   "ingest_error_samples":[{"base":"SONIC","epoch":2,"error":"OutOfOrder",
      "stage":"ingest_parsed","venue":"OkxSpot"}]})
 }
 #[test]
 fn rejects_out_of_order_and_never_hides_quality_event(){
  let q=inspect_paper_ingest_samples(&status()).unwrap();
  assert_eq!(q["sampled_out_of_order"],1);
  assert_eq!(q["ingest_rejections"],1);
  assert_eq!(q["requires_review"],true);
  assert_eq!(q["cause_certified"],false);
  assert_eq!(q["real_trading_allowed"],false);
  assert_eq!(q["examples"][0]["base"],"SONIC");
 }
 #[test]
 fn malformed_or_incomplete_counters_fail_closed(){
  let mut s=status();s["errors"]["ingest"]=json!(0);
  assert_eq!(inspect_paper_ingest_samples(&s),Err(IngestQualityError::TooMany));
  let mut s=status();s["frames"]=json!(0);
  assert_eq!(inspect_paper_ingest_samples(&s),Err(IngestQualityError::Contradiction));
  let mut s=status();s["ingest_error_samples"][0]["venue"]=json!("unknown");
  assert_eq!(inspect_paper_ingest_samples(&s),Err(IngestQualityError::Malformed));
 }
 #[test]
 fn unsampled_events_are_not_claimed_to_have_cause(){
  let mut s=status();s["errors"]["ingest"]=json!(200);
  let q=inspect_paper_ingest_samples(&s).unwrap();
  assert_eq!(q["sampled_out_of_order"],1);
  assert_eq!(q["unsampled_ingest_rejections"],199);
  assert_eq!(q["cause_certified"],false);
 }
}
