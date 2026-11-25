//! R55: bounds-check every nested field leaving the aggregate-only AI adapter.
//! No I/O or mutation. Unknown raw structures must fail closed.
use crate::native_ai_advisor_review::AdvisorError;
use serde_json::Value;

fn safe_key(s:&str)->bool {
 !s.is_empty()&&s.len()<=64&&s.bytes().all(|c|
  c.is_ascii_alphanumeric()||c==b'_'||c==b'-'||c==b'.'||c==b':')
 && !s.contains("://")
}
fn walk(v:&Value, depth:usize, remaining:&mut usize)->Result<(),AdvisorError>{
 if depth>10||*remaining==0{return Err(AdvisorError::TooLarge)}
 *remaining-=1;
 match v {
  Value::Null|Value::Bool(_)|Value::Number(_)=>Ok(()),
  Value::String(s)=>{
   if s.len()>160||s.chars().any(char::is_control)
     ||s.contains("://")||s.contains('@')||s.contains('\\'){
    return Err(AdvisorError::InvalidInput)
   }
   Ok(())
  },
  Value::Array(rows)=>{
   if rows.len()>2000{return Err(AdvisorError::TooLarge)}
   for row in rows{walk(row,depth+1,remaining)?}
   Ok(())
  },
  Value::Object(map)=>{
   if map.len()>128{return Err(AdvisorError::TooLarge)}
   for (name,val) in map{
    if !safe_key(name){return Err(AdvisorError::InvalidInput)}
    walk(val,depth+1,remaining)?;
   }
   Ok(())
  }
 }
}
pub fn verify_aggregate_only(v:&Value)->Result<(),AdvisorError>{
 let mut remaining=24_000;
 walk(v,0,&mut remaining)
}

fn code(s:&str,max:usize)->bool{
 !s.is_empty()&&s.len()<=max&&s.bytes().all(|b|
  b.is_ascii_alphanumeric()||b==b'_'||b==b'-')
}
/// Python analyzer mode counts are a map of bounded integer counters.
pub fn checked_counts(v:Option<&Value>)->Result<Value,AdvisorError>{
 let Some(v)=v else{return Ok(serde_json::json!({}))};
 let map=v.as_object().ok_or(AdvisorError::InvalidInput)?;
 if map.len()>256{return Err(AdvisorError::TooLarge)}
 for (key,num) in map{
  if !code(key,64)||num.as_u64().map_or(true,|n|n>1_000_000_000_000){
   return Err(AdvisorError::InvalidInput)
  }
 }
 Ok(v.clone())
}
/// Python daily_metrics emits 24 per-mode {hour,events} rows.
pub fn checked_hourly(v:Option<&Value>)->Result<Value,AdvisorError>{
 let Some(v)=v else{return Ok(serde_json::json!([]))};
 let rows=v.as_array().ok_or(AdvisorError::InvalidInput)?;
 if rows.len()>48{return Err(AdvisorError::TooLarge)}
 let mut hours=std::collections::BTreeSet::new();
 let mut out=Vec::with_capacity(rows.len());
 for r in rows{
  let obj=r.as_object().ok_or(AdvisorError::InvalidInput)?;
  let hour=obj.get("hour").and_then(Value::as_u64)
   .ok_or(AdvisorError::InvalidInput)?;
  let events=obj.get("events").and_then(Value::as_u64)
   .ok_or(AdvisorError::InvalidInput)?;
  if hour>=24||events>1_000_000_000_000||!hours.insert(hour){
   return Err(AdvisorError::InvalidInput)
  }
  out.push(serde_json::json!({"hour":hour,"events":events}));
 }
 Ok(Value::Array(out))
}

pub fn checked_codes(v:Option<&Value>)->Result<Value,AdvisorError>{
 let Some(v)=v else{return Ok(serde_json::json!([]))};
 let a=v.as_array().ok_or(AdvisorError::InvalidInput)?;
 if a.len()>256{return Err(AdvisorError::TooLarge)}
 for e in a{
  if !e.as_str().is_some_and(|s|code(s,128)){
   return Err(AdvisorError::InvalidInput)
  }
 }
 Ok(v.clone())
}
pub fn checked_reasons(v:Option<&Value>)->Result<Value,AdvisorError>{
 let Some(v)=v else{return Ok(serde_json::json!([]))};
 let a=v.as_array().ok_or(AdvisorError::InvalidInput)?;
 if a.len()>512{return Err(AdvisorError::TooLarge)}
 let mut rows=Vec::new();
 for e in a{
  let mode=e.get("mode").and_then(Value::as_str).ok_or(AdvisorError::InvalidInput)?;
  let reason=e.get("reason").and_then(Value::as_str).ok_or(AdvisorError::InvalidInput)?;
  let count=e.get("count").and_then(Value::as_u64).ok_or(AdvisorError::InvalidInput)?;
  if !["paper","real","test","replay"].contains(&mode)||
    !code(reason,96)||count>1_000_000_000_000{
   return Err(AdvisorError::InvalidInput)
  }
  rows.push(serde_json::json!({"mode":mode,"reason":reason,"count":count}));
 }
 Ok(Value::Array(rows))
}

#[cfg(test)]
mod tests{
 use super::*;
 use serde_json::json;
 #[test]
 fn numeric_counts_cannot_carry_freeform_data(){
  assert_eq!(checked_counts(Some(&json!({"accepted":3}))).unwrap(),json!({"accepted":3}));
  assert_eq!(checked_counts(Some(&json!({"accepted":"bad"}))),Err(AdvisorError::InvalidInput));
  assert_eq!(checked_counts(Some(&json!({"accepted":-2}))),Err(AdvisorError::InvalidInput));
 }
 #[test]
 fn hour_rows_are_bounded_and_deduplicated(){
  assert_eq!(checked_hourly(Some(&json!([{"hour":0,"events":1,"other":5}])))
   .unwrap(),json!([{"hour":0,"events":1}]));
  assert_eq!(checked_hourly(Some(&json!([{"hour":1,"events":0},
   {"hour":1,"events":3}]))),Err(AdvisorError::InvalidInput));
  assert_eq!(checked_hourly(Some(&json!([{"hour":24,"events":2}]))),
   Err(AdvisorError::InvalidInput));
 }
 #[test]
 fn reason_and_warning_lists_accept_only_bounded_codes(){
  assert!(checked_codes(Some(&json!(["NO_VERIFIED_REAL_EXITS"]))).is_ok());
  assert!(checked_codes(Some(&json!(["a message with spaces"]))).is_err());
  assert!(checked_reasons(Some(&json!([{"mode":"paper",
   "reason":"CANCELLED","count":2,"detail":"ignored"}]))).is_ok());
  assert!(checked_reasons(Some(&json!([{"mode":"other",
   "reason":"CANCELLED","count":2}]))).is_err());
 }
 #[test]
 fn nested_budget_and_text_checks_apply_to_every_field(){
  assert!(verify_aggregate_only(&json!({"storage_health":{"status":"OK"}})).is_ok());
  assert_eq!(verify_aggregate_only(&json!({"report":{"notes":"https://example.invalid/"}})),
   Err(AdvisorError::InvalidInput));
  assert_eq!(verify_aggregate_only(&json!({"notes":"line1\nline2"})),
   Err(AdvisorError::InvalidInput));
  let huge=json!((0..2001).collect::<Vec<_>>());
  assert_eq!(verify_aggregate_only(&huge),Err(AdvisorError::TooLarge));
 }
}
