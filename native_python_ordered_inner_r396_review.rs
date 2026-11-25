// R396: isolated Python-compatible insertion-order preservation for JSON inner
// payload and verification. Does NOT fetch configured secrets or write events.
#[derive(Debug,PartialEq)]
enum R396OrderedJson{
 Null,Bool(bool),Int(i64),UInt(u64),Str(String),
 Array(Vec<R396OrderedJson>),Object(Vec<(String,R396OrderedJson)>),
}
impl<'de> serde::Deserialize<'de> for R396OrderedJson{
 fn deserialize<D:serde::Deserializer<'de>>(de:D)->Result<Self,D::Error>{
  use serde::de::{Visitor,SeqAccess,MapAccess,Error};
  struct V;
  impl<'de> Visitor<'de> for V{
   type Value=R396OrderedJson;
   fn expecting(&self,f:&mut std::fmt::Formatter)->std::fmt::Result{
    write!(f,"nonfloating bounded JSON value")
   }
   fn visit_unit<E:Error>(self)->Result<Self::Value,E>{Ok(R396OrderedJson::Null)}
   fn visit_none<E:Error>(self)->Result<Self::Value,E>{Ok(R396OrderedJson::Null)}
   fn visit_bool<E:Error>(self,v:bool)->Result<Self::Value,E>{Ok(R396OrderedJson::Bool(v))}
   fn visit_i64<E:Error>(self,v:i64)->Result<Self::Value,E>{Ok(R396OrderedJson::Int(v))}
   fn visit_u64<E:Error>(self,v:u64)->Result<Self::Value,E>{Ok(R396OrderedJson::UInt(v))}
   fn visit_f64<E:Error>(self,_:f64)->Result<Self::Value,E>{
    Err(E::custom("float requires independently verified Python float parity"))
   }
   fn visit_str<E:Error>(self,v:&str)->Result<Self::Value,E>{
    Ok(R396OrderedJson::Str(v.to_owned()))
   }
   fn visit_string<E:Error>(self,v:String)->Result<Self::Value,E>{
    Ok(R396OrderedJson::Str(v))
   }
   fn visit_seq<A:SeqAccess<'de>>(self,mut a:A)->Result<Self::Value,A::Error>{
    let mut v=Vec::new();
    while let Some(x)=a.next_element::<R396OrderedJson>()?{
      v.push(x);
      if v.len()>64{return Err(A::Error::custom("array too long"))}
    }
    Ok(R396OrderedJson::Array(v))
   }
   fn visit_map<A:MapAccess<'de>>(self,mut m:A)->Result<Self::Value,A::Error>{
    let mut out=Vec::<(String,R396OrderedJson)>::new();
    while let Some((k,v))=m.next_entry::<String,R396OrderedJson>()?{
      if out.iter().any(|(old,_)|old==&k){
       return Err(A::Error::custom("duplicate key forbidden"))
      }
      out.push((k,v));
      if out.len()>128{return Err(A::Error::custom("object too large"))}
    }
    Ok(R396OrderedJson::Object(out))
   }
  }
  de.deserialize_any(V)
 }
}
fn r396_python_inner_impl(raw:&str,secrets:Option<&[&str]>)
 ->Result<String,EventOutboxR366Error>{
 if raw.len()>16384{return Err(EventOutboxR366Error::Schema)}
 let value: R396OrderedJson=serde_json::from_str(raw)
    .map_err(|_|EventOutboxR366Error::Schema)?;
 fn emit(v:&R396OrderedJson,out:&mut String,depth:usize,
   budget:&mut usize,secrets:Option<&[&str]>)
    ->Result<(),EventOutboxR366Error>{
  if depth>8||*budget==0{return Err(EventOutboxR366Error::Schema)}
  *budget-=1;
  match v{
   R396OrderedJson::Null=>out.push_str("null"),
   R396OrderedJson::Bool(b)=>out.push_str(if *b{"true"}else{"false"}),
   R396OrderedJson::Int(i)=>out.push_str(&i.to_string()),
   R396OrderedJson::UInt(i)=>out.push_str(&i.to_string()),
   R396OrderedJson::Str(s)=>{
    if s.chars().count()>4096{return Err(EventOutboxR366Error::Schema)}
    let clean=if let Some(sec)=secrets{
      r336_redact_string(s,sec).map_err(|_|EventOutboxR366Error::Schema)?
    }else{s.clone()};
    out.push_str(&serde_json::to_string(&clean)
      .map_err(|_|EventOutboxR366Error::Schema)?)
   },
   R396OrderedJson::Array(xs)=>{
    out.push('[');
    for (i,x) in xs.iter().enumerate(){
     if i>0{out.push_str(", ")}
     emit(x,out,depth+1,budget,secrets)?;
    }
    out.push(']');
   },
   R396OrderedJson::Object(xs)=>{
    out.push('{');
    for (i,(k,x)) in xs.iter().enumerate(){
     if k.chars().count()>80{return Err(EventOutboxR366Error::Schema)}
     if i>0{out.push_str(", ")}
     out.push_str(&serde_json::to_string(k).map_err(|_|EventOutboxR366Error::Schema)?);
     out.push_str(": ");
     if secrets.is_some()&&r336_sensitive_key(k){
      out.push_str("\"[REDACTED]\"");
     }else{
      emit(x,out,depth+1,budget,secrets)?;
     }
    }
    out.push('}');
   }
  }
  Ok(())
 }
 let mut out=String::new();
 let mut budget=512;
 emit(&value,&mut out,0,&mut budget,secrets)?;
 if out.len()>16384{return Err(EventOutboxR366Error::Schema)}
 Ok(out)
}
fn r396_python_inner_json(raw:&str)->Result<String,EventOutboxR366Error>{
 r396_python_inner_impl(raw,None)
}
fn r396_python_scrubbed_inner_json(raw:&str,secrets:&[&str])
 ->Result<String,EventOutboxR366Error>{
 r396_python_inner_impl(raw,Some(secrets))
}
#[cfg(test)]
mod r396_tests{
 use super::*;
 use serde_json::{Value,json};
 #[test]fn r396_exact_inner_order_matches_four_actual_python_fixtures(){
  let f:Value=serde_json::from_str(include_str!(
   "../config/native_r396_python_ordered_and_after_oracle.json")).unwrap();
  let fixtures=f["ordered"].as_array().unwrap();
  assert_eq!(fixtures.len(),4);
  for row in fixtures{
   let s=r396_python_inner_json(row["input_json"].as_str().unwrap()).unwrap();
   assert_eq!(s,row["python_inner_json"],"{}",row["name"]);
  }
 }
 #[test]fn r396_ordered_redaction_matches_real_python_four_fixtures(){
  let f:Value=serde_json::from_str(include_str!(
   "../config/native_r396_python_ordered_and_after_oracle.json")).unwrap();
  let cases=f["scrubbed"].as_array().unwrap();
  assert_eq!(cases.len(),4);
  for row in cases{
   let secrets=row["secrets"].as_array().unwrap().iter()
    .map(|x|x.as_str().unwrap()).collect::<Vec<_>>();
   let actual=r396_python_scrubbed_inner_json(
      row["input_json"].as_str().unwrap(),&secrets).unwrap();
   assert_eq!(actual,row["expected_python_json"],
      "Python scrub + insertion order {}",row["name"]);
  }
 }
 #[test]fn r396_after_matches_real_python_sqlite_two_rows(){
  let f:Value=serde_json::from_str(include_str!(
   "../config/native_r396_python_ordered_and_after_oracle.json")).unwrap();
  let rows=f["after"].as_array().unwrap();
  assert_eq!(rows.len(),2);
  for row in rows{
   let expected=&row["expected"];
   let actual=r386_after_image(row["input_table"].as_str().unwrap(),
     row["input_key"].clone(),expected["row"].clone()).unwrap();
   assert_eq!(actual,*expected);
   assert_eq!(r386_python_mirror_canonical(&expected["row"]).unwrap(),
     row["canonical_row"].as_str().unwrap());
  }
 }
 #[test]fn r396_fail_closed_float_duplicate_depth_and_long_string(){
  for input in ["{\"a\":2.5}","{\"a\":1,\"a\":2}",
    "{\"nested\":[[[[[[[[[1]]]]]]]]]}"]{
   assert_eq!(r396_python_inner_json(input),Err(EventOutboxR366Error::Schema));
  }
  assert_eq!(r396_python_inner_json(&json!({"note":"a".repeat(4097)}).to_string()),
    Err(EventOutboxR366Error::Schema));
  assert_eq!(r396_python_inner_json("{\"z\":1,\"a\":2}").unwrap(),
   "{\"z\": 1, \"a\": 2}");
 }
}
