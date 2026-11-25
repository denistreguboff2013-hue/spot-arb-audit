// R386: exact Python JSON byte encoding for the tested integer/string/null/bool
// subset of Event.record and mirror_outbox.canonical. Fail closed on floats,
// rather than claiming unproven Python float formatting compatibility.
fn r386_python_json(value:&serde_json::Value,spaced:bool)
 ->Result<String,EventOutboxR366Error>{
 use serde_json::Value;
 fn encode(v:&Value,spaced:bool,out:&mut String)->Result<(),EventOutboxR366Error>{
  match v{
   Value::Null=>out.push_str("null"),
   Value::Bool(x)=>out.push_str(if *x{"true"}else{"false"}),
   Value::Number(n)=>{
    if n.is_f64(){return Err(EventOutboxR366Error::Schema)}
    out.push_str(&n.to_string());
   },
   Value::String(s)=>out.push_str(&serde_json::to_string(s)
      .map_err(|_|EventOutboxR366Error::Schema)?),
   Value::Array(xs)=>{
    out.push('[');
    for (i,x) in xs.iter().enumerate(){
     if i>0{out.push(',');if spaced{out.push(' ')}}
     encode(x,spaced,out)?;
    }
    out.push(']');
   },
   Value::Object(map)=>{
    out.push('{');
    for (i,(k,v)) in map.iter().enumerate(){
     if i>0{out.push(',');if spaced{out.push(' ')}}
     out.push_str(&serde_json::to_string(k)
        .map_err(|_|EventOutboxR366Error::Schema)?);
     out.push(':');if spaced{out.push(' ')}
     encode(v,spaced,out)?;
    }
    out.push('}');
   }
  }
  Ok(())
 }
 let mut result=String::new();
 encode(value,spaced,&mut result)?;
 Ok(result)
}
fn r386_python_collector_json(record:&serde_json::Value)
 ->Result<String,EventOutboxR366Error>{r386_python_json(record,true)}
fn r386_python_mirror_canonical(frame:&serde_json::Value)
 ->Result<String,EventOutboxR366Error>{r386_python_json(frame,false)}

fn r386_after_image(table:&str,key:serde_json::Value,row:serde_json::Value)
 ->Result<serde_json::Value,EventOutboxR366Error>{
 let after_hash=r366_sha(&r386_python_mirror_canonical(&row)?);
 Ok(serde_json::json!({"table":table,"op":"insert","key":key,
   "row":row,"after_hash":after_hash}))
}
#[cfg(test)]
mod r386_tests{
 use super::*;
 use serde_json::{json,Value};
 #[test]fn r386_original_python_five_record_hashes_and_outbox(){
  let fixtures:Value=serde_json::from_str(include_str!(
    "../config/native_r386_python_hash_oracle.json")).unwrap();
  let cases=fixtures["cases"].as_array().unwrap();
  assert_eq!(cases.len(),5);
  for row in cases{
   let record=&row["record"];
   let raw=r386_python_collector_json(record).unwrap();
   assert_eq!(raw,row["expected_python_record_json"],
     "Python JSON mismatch for {}",row["name"]);
   assert_eq!(r366_sha(&raw),row["expected_record_hash"],
     "Python SHA256 mismatch for {}",row["name"]);
  }
  let raw=r386_python_mirror_canonical(&fixtures["mirror_frame"]).unwrap();
  assert_eq!(raw,fixtures["mirror_canonical"]);
  assert_eq!(r366_sha(&raw),fixtures["mirror_hash"]);
 }
 #[test]fn r386_fail_closed_unsupported_float_and_no_secret_leak(){
  let v=json!({"a":2.5,"b":true});
  assert_eq!(r386_python_collector_json(&v),Err(EventOutboxR366Error::Schema));
  let v=json!({"ё":"проверка","a":null,"emoji":"🧪","quote":"a\\nb"});
  let raw=r386_python_collector_json(&v).unwrap();
  assert!(raw.contains("проверка"));
  assert!(raw.contains("\\n"));
 }
}
