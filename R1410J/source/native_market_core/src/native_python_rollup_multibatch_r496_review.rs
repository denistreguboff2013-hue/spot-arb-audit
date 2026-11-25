// R496-R505: bounded READ-ONLY Python Decimal multibatch merge oracle.
// No SQLite DELETE, source row writes, production mirror outbox or DB2 authority.
const R496_KEYS:[&str;8]=["hour_start_ms","mode","base","category",
 "name","reason_code","decision","skip_reason"];
const R496_COUNT:[&str;6]=["event_count","raw_spread_count",
 "net_spread_count","telemetry_age_count","depth_spot_count","depth_fut_count"];
const R496_SUM:[&str;5]=["raw_spread_sum","net_spread_sum",
 "telemetry_age_sum","depth_spot_sum","depth_fut_sum"];
const R496_MIN:[&str;2]=["raw_spread_min","net_spread_min"];
const R496_MAX:[&str;3]=["raw_spread_max","net_spread_max","telemetry_age_max"];
fn r496_key(row:&serde_json::Value)->Result<String,EventOutboxR366Error>{
 use serde_json::json;
 let mut values=Vec::new();
 for k in R496_KEYS{
  let v=row.get(k).ok_or(EventOutboxR366Error::Schema)?;
  if k=="hour_start_ms"{
   if v.as_u64().is_none(){return Err(EventOutboxR366Error::Schema)}
  }else if !v.is_string(){return Err(EventOutboxR366Error::Schema)}
  values.push(v.clone());
 }
 r386_python_mirror_canonical(&json!(values))
}
fn r496_required_decimal(value:&serde_json::Value)
 ->Result<R456Decimal,EventOutboxR366Error>{
 if !value.is_string(){return Err(EventOutboxR366Error::Schema)}
 r456_finite(value)?.ok_or(EventOutboxR366Error::Schema)
}
fn r496_optional_decimal(value:&serde_json::Value)
 ->Result<Option<R456Decimal>,EventOutboxR366Error>{
 if value.is_null(){return Ok(None)}
 Ok(Some(r496_required_decimal(value)?))
}
fn r496_merge_row(old:&serde_json::Value,new:&serde_json::Value)
 ->Result<serde_json::Value,EventOutboxR366Error>{
 use serde_json::Value;
 if r496_key(old)?!=r496_key(new)?||
   old.as_object().map(|o|o.len())!=Some(24)||
   new.as_object().map(|o|o.len())!=Some(24){
   return Err(EventOutboxR366Error::Schema)
 }
 let mut out=old.as_object().ok_or(EventOutboxR366Error::Schema)?.clone();
 for k in R496_COUNT{
  let a=old[k].as_u64().ok_or(EventOutboxR366Error::Schema)?;
  let b=new[k].as_u64().ok_or(EventOutboxR366Error::Schema)?;
  out.insert(k.into(),Value::from(a.checked_add(b)
   .ok_or(EventOutboxR366Error::Schema)?));
 }
 for k in R496_SUM{
  let result=r496_required_decimal(&old[k])?
    .add(r496_required_decimal(&new[k])?)?;
  out.insert(k.into(),Value::String(result.text()));
 }
 for k in R496_MIN{
  let a=r496_optional_decimal(&old[k])?;
  let b=r496_optional_decimal(&new[k])?;
  let min=match (a,b){
   (Some(x),Some(y))=>Some(if x.ord_value(y)?.is_le(){x}else{y}),
   (x,None)=>x,(None,y)=>y
  };
  out.insert(k.into(),r456_nullable(min));
 }
 for k in R496_MAX{
  let a=r496_optional_decimal(&old[k])?;
  let b=r496_optional_decimal(&new[k])?;
  let max=match (a,b){
   (Some(x),Some(y))=>Some(if x.ord_value(y)?.is_ge(){x}else{y}),
   (x,None)=>x,(None,y)=>y
  };
  out.insert(k.into(),r456_nullable(max));
 }
 Ok(Value::Object(out))
}
#[derive(Debug,Clone,PartialEq)]
struct R496MergedBatch{
 rows:Vec<serde_json::Value>,
 operations:Vec<serde_json::Value>,
 total:u64,
 source_deleted:bool,
 mirror_recorded:bool,
}
fn r496_merge_batch(previous:&[serde_json::Value],
 incoming:&[serde_json::Value],candidates:&[(String,String)],
 prior_total:u64,
)->Result<R496MergedBatch,EventOutboxR366Error>{
 use serde_json::{json,Value};
 if incoming.is_empty()||incoming.len()>500||
  candidates.is_empty()||candidates.len()>500{
  return Err(EventOutboxR366Error::Schema)
 }
 let mut state=std::collections::BTreeMap::<String,Value>::new();
 for row in previous{
  if row.as_object().map(|o|o.len())!=Some(24){
   return Err(EventOutboxR366Error::Schema)
  }
  if state.insert(r496_key(row)?,row.clone()).is_some(){
    return Err(EventOutboxR366Error::Conflict)
  }
 }
 let mut seen=std::collections::BTreeSet::new();
 let mut operations=Vec::new();
 let mut count=0_u64;
 for row in incoming{
  let key=r496_key(row)?;
  if !seen.insert(key.clone())||
     row.as_object().map(|o|o.len())!=Some(24){
    return Err(EventOutboxR366Error::Schema)
  }
  let delta=row["event_count"].as_u64().ok_or(EventOutboxR366Error::Schema)?;
  if delta==0{return Err(EventOutboxR366Error::Schema)}
  count=count.checked_add(delta).ok_or(EventOutboxR366Error::Schema)?;
  let merged=match state.get(&key){
   Some(previous)=>r496_merge_row(previous,row)?,
   None=>row.clone()
  };
  let mut keyobj=serde_json::Map::new();
  for k in R496_KEYS{keyobj.insert(k.into(),merged[k].clone());}
  let hash=r366_sha(&r386_python_mirror_canonical(&merged)?);
  operations.push(json!({"table":"hourly_raw_rollups","op":"upsert",
    "key":keyobj,"row":merged,"after_hash":hash}));
  state.insert(key,merged);
 }
 if count!=candidates.len() as u64{
   return Err(EventOutboxR366Error::Schema)
 }
 let mut ids=std::collections::BTreeSet::new();
 for (id,cat) in candidates{
  if !ids.insert(id.clone())||id.is_empty()||
    !["signals","decisions"].contains(&cat.as_str()){
     return Err(EventOutboxR366Error::Schema)
  }
  operations.push(json!({"table":cat,"op":"delete","key":{"event_id":id}}));
 }
 for (id,_) in candidates{
  operations.push(json!({"table":"events","op":"delete","key":{"event_id":id}}));
 }
 let total=prior_total.checked_add(count).ok_or(EventOutboxR366Error::Schema)?;
 let meta=json!({"key":"rolled_up_raw_events","value":total.to_string()});
 operations.push(json!({"table":"metadata","op":"upsert",
  "key":{"key":"rolled_up_raw_events"},
  "row":meta,"after_hash":r366_sha(&r386_python_mirror_canonical(&meta)?)}));
 Ok(R496MergedBatch{rows:state.into_values().collect(),
   operations,total,source_deleted:false,mirror_recorded:false})
}
#[cfg(test)]
mod r496_tests{
 use super::*;
 use super::r366_tests::fixture_root;
 use serde_json::{json,Value};
 const NOW:u64=1_792_660_000_000;
 fn oracle()->Value{serde_json::from_str(include_str!(
  "../config/native_r496_python_multibatch_oracle.json")).unwrap()}
 fn batch_rows(src:&[Value])->Vec<Value>{
  let root=fixture_root();
  let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
  for (i,e) in src.iter().enumerate(){
   let cat=e["cat"].as_str().unwrap();
   let name=if cat=="signals"{"SIGNAL_SEEN"}else{"DECISION_MADE"};
   let input=json!({"mode":"paper","category":cat,"name":name,
    "event_id":e["id"],"base":"SOL","source":"native.paper",
    "run_id":"r496_private","occurred_at_ms":e["at"],
    "payload":e["payload"],"verification":{"status":"observed"}});
   let record=r356_paper_event_record_without_io(&input,&[],NOW).unwrap();
   w.append(&record,NOW-8*86_400_000+i as u64,false).unwrap();
  }
  let result=w.r456_rollup_preview(NOW,7,false,500).unwrap();
  assert_eq!(result.selected,src.len());
  assert_eq!(w.audit().unwrap().events,src.len() as u64);
  // Python collector's SELECT FROM events with INTEGER-free TEXT PK
  // visits SQLite event_id order, NOT rollup (hour,base,category) order.
  let mut ordered=result.rows;
  ordered.sort_by_key(|row|{
   let category=row["category"].as_str().unwrap();
   let hour=row["hour_start_ms"].as_u64().unwrap();
   src.iter().filter(|e|e["cat"]==category&&
      e["at"].as_u64().unwrap()/3_600_000*3_600_000==hour)
    .map(|e|e["id"].as_str().unwrap()).min().unwrap().to_string()
  });
  drop(w);fs::remove_dir_all(root).unwrap();
  ordered
 }
 fn reproduce(base:&str){
  let data=oracle();
  let events=data["events"].as_array().unwrap();
  let steps=data[base]["steps"].as_array().unwrap();
  let mut state=Vec::new();
  let mut meta=data[base]["initial"].as_u64().unwrap();
  for i in 0..2{
   let source=&events[i*2..(i+1)*2];
   let incoming=batch_rows(source);
   let ids=source.iter().map(|e|(
    e["id"].as_str().unwrap().to_string(),
    e["cat"].as_str().unwrap().to_string())).collect::<Vec<_>>();
   let next=r496_merge_batch(&state,&incoming,&ids,meta).unwrap();
   assert!(!next.source_deleted&&!next.mirror_recorded);
   assert_eq!(next.total.to_string(),steps[i]["rolled_up_raw_events"]);
   assert_eq!(Value::Array(next.rows.clone()),steps[i]["rollups"]);
   assert_eq!(Value::Array(next.operations.clone()),steps[i]["operations"]);
   let canonical=r386_python_mirror_canonical(&Value::Array(next.operations)).unwrap();
   assert_eq!(canonical,steps[i]["operations_canonical"]);
   assert_eq!(r366_sha(&canonical),steps[i]["operations_sha256"]);
   state=next.rows;meta=next.total;
  }
 }
 #[test]fn r496_original_python_two_batches_zero_baseline(){reproduce("baseline_zero")}
 #[test]fn r497_original_python_two_batches_five_baseline(){reproduce("baseline_five")}
 #[test]fn r498_metadata_accumulates_from_nonzero_baseline(){
  let c=oracle();
  for (name,expected) in [("baseline_zero",["2","4"]),
                          ("baseline_five",["7","9"])]{
   let steps=c[name]["steps"].as_array().unwrap();
   for (idx,step) in steps.iter().enumerate(){
    assert_eq!(step["rolled_up_raw_events"],expected[idx]);
    assert_eq!(step["operations"].as_array().unwrap().last().unwrap()["row"]["value"],
      expected[idx]);
   }
  }
 }
 #[test]fn r499_hourly_merge_preserves_min_max_and_decimal_scale(){
  let data=oracle();
  let prior=&data["baseline_zero"]["steps"][0]["rollups"][0];
  let incoming=batch_rows(&data["events"].as_array().unwrap()[2..4]);
  let row=incoming.iter().find(|x|x["category"]=="signals").unwrap();
  let merged=r496_merge_row(prior,row).unwrap();
  let target=data["baseline_zero"]["steps"][1]["rollups"].as_array().unwrap()
    .iter().find(|x|x["category"]=="signals").unwrap().clone();
  assert_eq!(merged,target);
  assert_eq!(merged["raw_spread_min"],"-0.30");
  assert_eq!(merged["raw_spread_sum"],"3.20");
  assert_eq!(merged["telemetry_age_max"],"3.5");
 }
 #[test]fn r500_zero_metrics_merge_preserves_none_and_zero(){
  let data=oracle();
  let prior=data["baseline_zero"]["steps"][1]["rollups"].as_array().unwrap()
    .iter().find(|x|x["category"]=="decisions").unwrap();
  let merged=r496_merge_row(prior,prior).unwrap();
  assert_eq!(merged["event_count"],2);
  assert_eq!(merged["raw_spread_sum"],"0");
  assert!(merged["raw_spread_min"].is_null());
 }
 #[test]fn r501_group_identity_mismatch_fails_closed(){
  let prior=oracle()["baseline_zero"]["steps"][0]["rollups"][0].clone();
  let mut other=prior.clone();other["mode"]=json!("real");
  assert_eq!(r496_merge_row(&prior,&other),Err(EventOutboxR366Error::Schema));
 }
 #[test]fn r502_unsupported_decimal_precision_refuses_merge(){
  let old=oracle()["baseline_zero"]["steps"][0]["rollups"][0].clone();
  let mut incoming=old.clone();
  incoming["raw_spread_sum"]=json!("0.0000001");
  assert_eq!(r496_merge_row(&old,&incoming),
    Err(EventOutboxR366Error::Schema));
 }
 #[test]fn r503_counter_overflow_refuses_merge(){
  let mut old=oracle()["baseline_zero"]["steps"][0]["rollups"][0].clone();
  let mut new=old.clone();old["event_count"]=json!(u64::MAX);
  new["event_count"]=json!(1);
  assert_eq!(r496_merge_row(&old,&new),Err(EventOutboxR366Error::Schema));
  assert_eq!(r496_merge_batch(&[],&[new],&[("a".into(),"signals".into())],
    u64::MAX),Err(EventOutboxR366Error::Schema));
 }
 #[test]fn r504_repeated_hour_key_in_one_batch_refuses_without_mutation(){
  let row=oracle()["baseline_zero"]["steps"][0]["rollups"][0].clone();
  let initial=Vec::new();
  assert_eq!(r496_merge_batch(&initial,&[row.clone(),row],
   &[("one".into(),"signals".into()),("two".into(),"signals".into())],0),
    Err(EventOutboxR366Error::Schema));
  assert!(initial.is_empty());
 }
 #[test]fn r505_no_empty_or_repeated_delete_intent_permitted(){
  let row=oracle()["baseline_zero"]["steps"][0]["rollups"][0].clone();
  assert_eq!(r496_merge_batch(&[],&[],&[],0),
   Err(EventOutboxR366Error::Schema));
  assert_eq!(r496_merge_batch(&[],&[row],&[
    ("reused".into(),"signals".into()),
    ("reused".into(),"signals".into())],0),
    Err(EventOutboxR366Error::Schema));
 }
}
