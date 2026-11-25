// Astra P0-01 R962: private only exact physical row binding without
// SQLite json_object + serde_json parse of each *actual* event row.
// All 20 pinned original columns checked, null/integer/text fully bound,
// ordered physical PK range requires exact row count, both DBs checked.
extern "C" {fn sqlite3_column_type(stmt:*mut c_void,index:c_int)->c_int;}
fn r962_verify_new_paper_rows_raw(
 src:&Db,dst:&Db,manifest:&serde_json::Value,
 expected:&std::collections::BTreeMap<String,serde_json::Value>
)->Result<(),EventOutboxR366Error>{
 if expected.is_empty()||expected.len()>2048{return Err(EventOutboxR366Error::Pressure)}
 let tbl=manifest["table_schema"].as_array().ok_or(EventOutboxR366Error::Schema)?
  .iter().find(|x|x["table"]=="events").ok_or(EventOutboxR366Error::Schema)?;
 let columns=tbl["columns"].as_array().ok_or(EventOutboxR366Error::Schema)?;
 if columns.len()!=20{return Err(EventOutboxR366Error::Schema)}
 let fields=columns.iter().map(|col|{
  let name=col[1].as_str().ok_or(EventOutboxR366Error::Schema)?;
  Ok((name.to_owned(),r586_identifier(name)?))
 }).collect::<Result<Vec<(String,String)>,EventOutboxR366Error>>()?;
 let first=expected.first_key_value().ok_or(EventOutboxR366Error::Corrupt)?.0;
 let last=expected.last_key_value().ok_or(EventOutboxR366Error::Corrupt)?.0;
 let low=r586_sql_literal(&serde_json::json!(first))?;
 let high=r586_sql_literal(&serde_json::json!(last))?;
 let sql=format!("SELECT {} FROM events WHERE event_id>={low}
   AND event_id<={high} ORDER BY event_id",
  fields.iter().map(|x|x.1.as_str()).collect::<Vec<_>>().join(","));
 for db in [src,dst]{
  let mut q=Stmt::new(db,&sql).map_err(r366_error)?;
  for (event_id,row) in expected{
   if q.step().map_err(r366_error)?!=SQLITE_ROW{return Err(EventOutboxR366Error::Corrupt)}
   if row["event_id"]!=*event_id{return Err(EventOutboxR366Error::Corrupt)}
   let map=row.as_object().ok_or(EventOutboxR366Error::Corrupt)?;
   if map.len()!=fields.len(){return Err(EventOutboxR366Error::Corrupt)}
   for (i,(name,_)) in fields.iter().enumerate(){
    let cell=map.get(name).ok_or(EventOutboxR366Error::Corrupt)?;
    let kind=unsafe{sqlite3_column_type(q.raw,i as c_int)};
    match cell{
     serde_json::Value::Null if kind==5=>{},
     serde_json::Value::String(s) if kind==3=>{
      if q.strval(i as i32).map_err(r366_error)?!=*s{
       return Err(EventOutboxR366Error::Corrupt)
      }
     },
     serde_json::Value::Number(n) if kind==1||kind==2=>{
      if q.strval(i as i32).map_err(r366_error)?!=n.to_string(){
       return Err(EventOutboxR366Error::Corrupt)
      }
     },
     serde_json::Value::Bool(b) if kind==1=>{
      if q.num(i as i32).map_err(r366_error)?!=u64::from(*b){
       return Err(EventOutboxR366Error::Corrupt)
      }
     },
     _=>return Err(EventOutboxR366Error::Corrupt)
    }
   }
  }
  if q.step().map_err(r366_error)?!=SQLITE_DONE{
   return Err(EventOutboxR366Error::Corrupt)
  }
 }
 Ok(())
}
#[test]fn r962_2048_rows_fast_vs_json_physical_binding(){
 use std::time::Instant;
 let (root,h,_)=pair(64);let v=r586_oracle();
 r924_install_private_paper_only_guards(&h.source,&h.sink,&v).unwrap();
 assert_eq!(r931_guarded_verified_burst(&root.0,&h.source,&h.sink,&v,64),Ok(64));
 let (head,c,_)=r805_read_head(&root.0,&h.source,&h.sink).unwrap();
 let (_,_,expected,_)=r907_verify_retained_and_extract_prefix(
  &h.source,&head,r726_s(&c,"feed_generation").unwrap(),"r879_probe").unwrap();
 let start=Instant::now();
 r895_verify_new_paper_rows(&h.source,&h.sink,&v,&expected).unwrap();
 let old_ms=start.elapsed().as_secs_f64()*1000.0;
 let start=Instant::now();
 r962_verify_new_paper_rows_raw(&h.source,&h.sink,&v,&expected).unwrap();
 let new_ms=start.elapsed().as_secs_f64()*1000.0;
 eprintln!("R962 private original 2048 event physical DB1+DB2 binding JSON_ms={old_ms:.2} raw_columns_ms={new_ms:.2}; one pass, NOT live capacity");
 assert_eq!(r800_streaming_original_parity(&h.source,&h.sink,&v),Ok(2048));
}
#[test]fn r963_fast_column_binding_detects_equal_two_db_tamper(){
 let (root,h,_)=pair(1);let v=r586_oracle();
 r924_install_private_paper_only_guards(&h.source,&h.sink,&v).unwrap();
 assert_eq!(r931_guarded_verified_burst(&root.0,&h.source,&h.sink,&v,1),Ok(1));
 let (head,c,_)=r805_read_head(&root.0,&h.source,&h.sink).unwrap();
 let (_,_,expected,_)=r907_verify_retained_and_extract_prefix(
  &h.source,&head,r726_s(&c,"feed_generation").unwrap(),"r879_probe").unwrap();
 assert_eq!(r962_verify_new_paper_rows_raw(&h.source,&h.sink,&v,&expected),Ok(()));
 for db in [&h.source,&h.sink]{
  exec(db,"DROP TRIGGER r924_guard_events_update").unwrap();
  exec(db,"UPDATE events SET category='tamper_identical'
   WHERE event_id LIKE '%_00000000000000000001_00'").unwrap();
 }
 assert_eq!(r962_verify_new_paper_rows_raw(&h.source,&h.sink,&v,&expected),
  Err(EventOutboxR366Error::Corrupt));
}
