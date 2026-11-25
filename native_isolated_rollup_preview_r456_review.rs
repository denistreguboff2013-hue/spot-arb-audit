// R456-R465: bounded Python Decimal-style HOURLY rollup PREVIEW ONLY.
// Never invokes retention DELETE or mirror commit; deliberately rejects
// unsupported source numeric representations instead of silently rounding.
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
struct R456Decimal{coefficient:i128,scale:u32}
impl R456Decimal{
 fn zero()->Self{Self{coefficient:0,scale:0}}
 fn powers(scale:u32)->Result<i128,EventOutboxR366Error>{
  10_i128.checked_pow(scale).ok_or(EventOutboxR366Error::Schema)
 }
 fn add(self,other:Self)->Result<Self,EventOutboxR366Error>{
  let scale=self.scale.max(other.scale);
  let a=self.coefficient.checked_mul(Self::powers(scale-self.scale)?)
   .ok_or(EventOutboxR366Error::Schema)?;
  let b=other.coefficient.checked_mul(Self::powers(scale-other.scale)?)
   .ok_or(EventOutboxR366Error::Schema)?;
  let coefficient=a.checked_add(b).ok_or(EventOutboxR366Error::Schema)?;
  if coefficient.unsigned_abs()>10_u128.pow(27){return Err(EventOutboxR366Error::Schema)}
  Ok(Self{coefficient,scale})
 }
 fn ord_value(self,other:Self)->Result<std::cmp::Ordering,EventOutboxR366Error>{
  let scale=self.scale.max(other.scale);
  let a=self.coefficient.checked_mul(Self::powers(scale-self.scale)?)
   .ok_or(EventOutboxR366Error::Schema)?;
  let b=other.coefficient.checked_mul(Self::powers(scale-other.scale)?)
   .ok_or(EventOutboxR366Error::Schema)?;
  Ok(a.cmp(&b))
 }
 fn text(self)->String{
  let a=self.coefficient.unsigned_abs().to_string();
  let negative=if self.coefficient<0{"-"}else{""};
  if self.scale==0{return format!("{negative}{a}")}
  let width=self.scale as usize;
  let padded=if a.len()<=width{format!("{}{a}", "0".repeat(width+1-a.len()))}
    else{a};
  let split=padded.len()-width;
  format!("{negative}{}.{}",&padded[..split],&padded[split..])
 }
}
fn r456_finite(v:&serde_json::Value)->Result<Option<R456Decimal>,EventOutboxR366Error>{
 use serde_json::Value;
 let text=match v{
  Value::Null|Value::Bool(_)|Value::Array(_)|Value::Object(_)=>return Ok(None),
  Value::String(s)=>s.trim().to_owned(),
  Value::Number(n)=>{
   if !n.is_i64()&&!n.is_u64(){return Err(EventOutboxR366Error::Schema)}
   n.to_string()
  }
 };
 if text.is_empty()||["NaN","sNaN","Infinity","-Infinity","+Infinity"].contains(&text.as_str()){
  return Ok(None)
 }
 let (raw,exp)=if let Some(pos)=text.find(|c:char|c=='e'||c=='E'){
  let mantissa=&text[..pos];
  if !mantissa.chars().any(|c|c.is_ascii_digit()){return Ok(None)}
  let parsed=text[pos+1..].parse::<i32>().map_err(|_|EventOutboxR366Error::Schema)?;
  (mantissa,parsed)
 }else{(text.as_str(),0)};
 if exp.abs()>40{return Err(EventOutboxR366Error::Schema)}
 let negative=raw.starts_with('-');
 let raw=raw.strip_prefix(|c:char|c=='-'||c=='+').unwrap_or(raw);
 let mut point_seen=false;
 let mut digit=String::new();
 let mut frac=0_i32;
 for c in raw.chars(){
  if c=='.'&&!point_seen{point_seen=true;continue}
  if !c.is_ascii_digit(){
   if raw.chars().any(|x|x.is_ascii_digit()){
    return Err(EventOutboxR366Error::Schema)
   }
   return Ok(None)
  }
  digit.push(c);if point_seen{frac+=1}
 }
 if digit.is_empty(){return Ok(None)}
 let significant=digit.trim_start_matches('0');
 if significant.len()>18{return Err(EventOutboxR366Error::Schema)}
 let coef=significant.parse::<i128>().unwrap_or(0);
 let adjusted=if coef==0{0}else{significant.len() as i32-1-frac+exp};
 if adjusted.abs()>30{return Ok(None)}
 let scale=frac-exp;
 let (coefficient,scale)=if scale<0{
  let mult=R456Decimal::powers((-scale) as u32)?;
  (coef.checked_mul(mult).ok_or(EventOutboxR366Error::Schema)?,0)
 }else{
  if scale>6{return Err(EventOutboxR366Error::Schema)}
  (coef,scale as u32)
 };
 if coefficient.unsigned_abs()>10_u128.pow(27)||
   (negative&&coefficient==0){return Err(EventOutboxR366Error::Schema)}
 Ok(Some(R456Decimal{coefficient:if negative{-coefficient}else{coefficient},scale}))
}
#[derive(Clone,Debug)]
struct R456Metric{
 count:u64,sum:R456Decimal,min:Option<R456Decimal>,max:Option<R456Decimal>
}
impl Default for R456Metric{
 fn default()->Self{Self{count:0,sum:R456Decimal::zero(),min:None,max:None}}
}
impl R456Metric{
 fn push(&mut self,value:Option<R456Decimal>)->Result<(),EventOutboxR366Error>{
  let Some(v)=value else{return Ok(())};
  self.count=self.count.checked_add(1).ok_or(EventOutboxR366Error::Schema)?;
  self.sum=self.sum.add(v)?;
  if self.min.map(|a|v.ord_value(a)).transpose()?
    .map_or(true,|o|o.is_lt()){self.min=Some(v)}
  if self.max.map(|a|v.ord_value(a)).transpose()?
    .map_or(true,|o|o.is_gt()){self.max=Some(v)}
  Ok(())
 }
}
#[derive(Default)]
struct R456Stats{
 events:u64,raw:R456Metric,net:R456Metric,
 age:R456Metric,spot:R456Metric,fut:R456Metric
}
fn r456_nullable(v:Option<R456Decimal>)->serde_json::Value{
 v.map(|x|serde_json::Value::String(x.text()))
  .unwrap_or(serde_json::Value::Null)
}
#[derive(Debug,Clone,PartialEq)]
pub struct R456RollupPreview{
 pub rows:Vec<serde_json::Value>,pub selected:usize,
 pub deletion_authorized:bool
}
impl IsolatedEventOutboxR366{
 pub fn r456_rollup_preview(&self,now:u64,days:u64,pressure:bool,
   max_batch:usize)->Result<R456RollupPreview,EventOutboxR366Error>{
  use serde_json::{Value,json};
  let selection=self.r446_retention_preview(now,days,pressure,max_batch)?;
  let db=r366_open(&self.path)?;
  let mut groups=std::collections::BTreeMap::<
   (u64,String,String,String,String,String,String,String),R456Stats>::new();
  for id in &selection.candidates{
   let mut q=Stmt::new(&db,"SELECT source_record_json FROM events
    WHERE event_id=?1").map_err(r366_error)?;
   q.string(1,id).map_err(r366_error)?;
   if q.step().map_err(r366_error)?!=SQLITE_ROW{
    return Err(EventOutboxR366Error::Corrupt)
   }
   let src=q.strval(0).map_err(r366_error)?;
   if q.step().map_err(r366_error)?!=SQLITE_DONE{
    return Err(EventOutboxR366Error::Corrupt)
   }
   let row:Value=serde_json::from_str(&src)
     .map_err(|_|EventOutboxR366Error::Corrupt)?;
   if row["event_id"]!=id.as_str()||row["mode"]!="paper"{
    return Err(EventOutboxR366Error::Corrupt)
   }
   let name=row["name"].as_str().ok_or(EventOutboxR366Error::Corrupt)?;
   let cat=row["category"].as_str().ok_or(EventOutboxR366Error::Corrupt)?;
   let time=row["occurred_at_ms"].as_u64().ok_or(EventOutboxR366Error::Corrupt)?;
   let attr=|key:&str|->Result<String,EventOutboxR366Error>{
    match &row[key]{
     Value::Null=>Ok(String::new()),Value::String(s)=>Ok(s.clone()),
     _=>Err(EventOutboxR366Error::Schema)
    }
   };
   let payload:Value=serde_json::from_str(row["payload_json"].as_str()
    .ok_or(EventOutboxR366Error::Corrupt)?)
     .map_err(|_|EventOutboxR366Error::Corrupt)?;
   let obj=payload.as_object().ok_or(EventOutboxR366Error::Corrupt)?;
   let text=|key:&str|->Result<String,EventOutboxR366Error>{
    match obj.get(key).unwrap_or(&Value::Null){
     Value::Null=>Ok(String::new()),Value::String(s)=>Ok(s.clone()),
     _=>Err(EventOutboxR366Error::Schema)
    }
   };
   let (decision,skip)=if cat=="decisions"{
     (text("decision")?,text("skip_reason")?)
   }else{(String::new(),String::new())};
   let key=(time/3_600_000*3_600_000,"paper".into(),attr("base")?,
     cat.into(),name.into(),attr("reason_code")?,decision,skip);
   let s=groups.entry(key).or_default();
   s.events=s.events.checked_add(1).ok_or(EventOutboxR366Error::Schema)?;
   if name=="SIGNAL_SEEN"{
    let metric=|key:&str|->Result<Option<R456Decimal>,EventOutboxR366Error>{
     r456_finite(obj.get(key).unwrap_or(&Value::Null))
    };
    s.raw.push(metric("raw_spread_bps")?)?;
    s.net.push(metric("net_spread_bps")?)?;
    s.age.push(metric("telemetry_age_sec")?)?;
    s.spot.push(metric("depth_spot_usdt")?)?;
    s.fut.push(metric("depth_fut_usdt")?)?;
   }
  }
  let mut rows=Vec::new();
  for ((hour,mode,base,category,name,reason,decision,skip),s) in groups{
   rows.push(json!({
    "hour_start_ms":hour,"mode":mode,"base":base,"category":category,
    "name":name,"reason_code":reason,"decision":decision,"skip_reason":skip,
    "event_count":s.events,
    "raw_spread_count":s.raw.count,"raw_spread_sum":s.raw.sum.text(),
    "raw_spread_min":r456_nullable(s.raw.min),
    "raw_spread_max":r456_nullable(s.raw.max),
    "net_spread_count":s.net.count,"net_spread_sum":s.net.sum.text(),
    "net_spread_min":r456_nullable(s.net.min),
    "net_spread_max":r456_nullable(s.net.max),
    "telemetry_age_count":s.age.count,"telemetry_age_sum":s.age.sum.text(),
    "telemetry_age_max":r456_nullable(s.age.max),
    "depth_spot_count":s.spot.count,"depth_spot_sum":s.spot.sum.text(),
    "depth_fut_count":s.fut.count,"depth_fut_sum":s.fut.sum.text()
   }));
  }
  Ok(R456RollupPreview{rows,selected:selection.candidates.len(),
   deletion_authorized:false})
 }
}
#[cfg(test)]
mod r456_tests{
 use super::*;
 use serde_json::{Value,json};
 use super::r366_tests::fixture_root;
 const NOW:u64=1_792_660_000_000;
 #[test]fn r456_exact_independent_python_sqlite_oracle(){
  let oracle:Value=serde_json::from_str(include_str!(
    "../config/native_r456_python_rollup_oracle.json")).unwrap();
  assert_eq!(oracle["now_ms"],NOW);
  let root=fixture_root();let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
  for (i,s) in oracle["samples"].as_array().unwrap().iter().enumerate(){
   let a=json!({"mode":"paper","category":s["category"],"name":s["name"],
    "event_id":s["id"],"base":s["base"],"reason_code":s["reason_code"],
    "source":"native.paper","run_id":"r456_python_oracle",
    "occurred_at_ms":s["occurred"],"payload":s["payload"],
    "verification":{"status":"observed"}});
   let record=r356_paper_event_record_without_io(&a,&[],NOW).unwrap();
   w.append(&record,NOW-8*86_400_000+i as u64,false).unwrap();
  }
  let preview=w.r456_rollup_preview(NOW,7,false,500).unwrap();
  assert!(!preview.deletion_authorized);
  assert_eq!(preview.selected,4);
  assert_eq!(Value::Array(preview.rows),oracle["rows"]);
  assert_eq!((w.audit().unwrap().events,w.audit().unwrap().outbox),(4,4));
  drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r457_decimal_trailing_zero_sum_matches_python(){
  let a=r456_finite(&json!("1.20")).unwrap().unwrap();
  let b=r456_finite(&json!("2.30")).unwrap().unwrap();
  assert_eq!(a.add(b).unwrap().text(),"3.50");
  assert_eq!(r456_finite(&json!("0.00")).unwrap().unwrap().text(),"0.00");
 }
 #[test]fn r458_decimal_negative_min_max_and_reordered_scales(){
  let mut r=R456Metric::default();
  for item in ["0.75","-0.25","0.5"]{
   r.push(r456_finite(&json!(item)).unwrap()).unwrap();
  }
  assert_eq!(r.count,3);assert_eq!(r.sum.text(),"1.00");
  assert_eq!(r.min.unwrap().text(),"-0.25");
  assert_eq!(r.max.unwrap().text(),"0.75");
 }
 #[test]fn r459_python_nonfinite_and_nonnumeric_values_are_ignored(){
  for value in [json!(null),json!(true),json!(false),json!("NaN"),
    json!("Infinity"),json!("-Infinity"),json!("other"),json!({}),
    json!([1,2])]{assert_eq!(r456_finite(&value),Ok(None))}
 }
 #[test]fn r460_large_or_unsupported_precision_fails_closed(){
  for value in [json!("1.0000001"),json!("12345678901234567890.0"),
    json!(-0.0),json!(0.75),json!("1_000") ]{
   assert_eq!(r456_finite(&value),Err(EventOutboxR366Error::Schema));
  }
 }
 #[test]fn r461_decimal_scientific_notation_is_exact_when_bounded(){
  assert_eq!(r456_finite(&json!("1.20e+2")).unwrap().unwrap().text(),"120");
  assert_eq!(r456_finite(&json!("3E-2")).unwrap().unwrap().text(),"0.03");
  assert_eq!(r456_finite(&json!("  +1.25 ")).unwrap().unwrap().text(),"1.25");
  assert_eq!(r456_finite(&json!("1E+31")),Ok(None));
 }
 fn simple(id:&str,cat:&str,stage:&str,base:&str,
   payload:serde_json::Value,time:u64)->serde_json::Value{
  r356_paper_event_record_without_io(&json!({"mode":"paper","category":cat,
   "name":stage,"event_id":id,"source":"native.paper","run_id":"r456_simple",
   "occurred_at_ms":time,"base":base,
   "payload":payload,"verification":{"status":"observed"}}),&[],NOW).unwrap()
 }
 #[test]fn r462_different_base_and_hour_produce_distinct_rows(){
  let root=fixture_root();let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
  let time=NOW-8*86_400_000;
  for (i,base,t) in [(0,"BTC",time),(1,"ETH",time),(2,"BTC",time+3_600_000)]{
   w.append(&simple(&format!("r462_{i}"),"signals","SIGNAL_SEEN",base,
    json!({"raw_spread_bps":"1.0"}),t),time+i as u64,false).unwrap();
  }
  let p=w.r456_rollup_preview(NOW,7,false,500).unwrap();
  assert_eq!(p.selected,3);assert_eq!(p.rows.len(),3);
  assert!(p.rows.iter().all(|r|r["event_count"]==1));
  drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r463_nonnumeric_signal_metrics_retained_only_in_event_count(){
  let root=fixture_root();let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
  let t=NOW-8*86_400_000;
  w.append(&simple("r463_signal","signals","SIGNAL_SEEN","SOL",
    json!({"raw_spread_bps":"NaN","net_spread_bps":"garbled",
    "telemetry_age_sec":null,"depth_spot_usdt":"Infinity"}),t),t,false).unwrap();
  let result=w.r456_rollup_preview(NOW,7,false,500).unwrap();
  assert_eq!(result.rows.len(),1);
  assert_eq!(result.rows[0]["event_count"],1);
  assert_eq!(result.rows[0]["raw_spread_count"],0);
  assert_eq!(result.rows[0]["net_spread_sum"],"0");
  assert!(result.rows[0]["raw_spread_min"].is_null());
  drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r464_unsupported_precision_refuses_whole_preview_without_deletion(){
  let root=fixture_root();let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
  let t=NOW-8*86_400_000;
  w.append(&simple("r464_signal","signals","SIGNAL_SEEN","SOL",
   json!({"raw_spread_bps":"0.0000001"}),t),t,false).unwrap();
  assert_eq!(w.r456_rollup_preview(NOW,7,false,500),
    Err(EventOutboxR366Error::Schema));
  assert_eq!((w.audit().unwrap().events,w.audit().unwrap().outbox),(1,1));
  drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r465_preview_consistent_on_reopen_no_rollup_or_source_mutation(){
  let root=fixture_root();let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
  let t=NOW-8*86_400_000;
  w.append(&simple("r465_signal","signals","SIGNAL_SEEN","SOL",
    json!({"net_spread_bps":"2.50"}),t),t,false).unwrap();
  let before=w.r456_rollup_preview(NOW,7,false,500).unwrap();
  drop(w);
  let re=IsolatedEventOutboxR366::reopen(&root).unwrap();
  assert_eq!(before,re.r456_rollup_preview(NOW,7,false,500).unwrap());
  assert!(!before.deletion_authorized);
  assert_eq!((re.audit().unwrap().events,re.audit().unwrap().last_seq),(1,1));
  drop(re);fs::remove_dir_all(root).unwrap();
 }
}
