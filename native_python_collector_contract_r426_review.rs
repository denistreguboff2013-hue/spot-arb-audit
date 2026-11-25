// R426-R435 research: source Python collector.FIELDS category projection.
// Own private R366 SQLite fixture only; no production DB, no REAL, no mirror ACK.
// Field names copied from /home/tmpadmin/spotbot/ai/collector.py (2026-10-22).
const R426_CATEGORY_FIELDS:&[(&str,&[&str])]=&[
 ("signals",&["symbol_spot","symbol_fut","okx_bid","okx_ask","binance_bid",
   "binance_ask","raw_spread_bps","net_spread_bps","telemetry_age_sec",
   "depth_spot_usdt","depth_fut_usdt","trade200","liquidity_score"]),
 ("decisions",&["decision","skip_reason","original_reason"]),
 ("orders",&["exchange","leg","symbol","client_order_id","order_id","call_id",
   "action","side","order_type","time_in_force","requested_amount",
   "requested_price","status","executed_amount","remaining_amount",
   "average_price","cost","exchange_event_time_ms","request_sent_at_ms",
   "response_received_at_ms","latency_ms","retry_count","failure_stage",
   "evidence_source"]),
 ("entries",&["entry_id","planned_usdt","planned_amount","entry_spread_bps",
   "spot_entry_mode","fut_entry_mode","spot_order_id","fut_order_id",
   "spot_expected_price","spot_actual_price","fut_expected_price",
   "fut_actual_price","spot_fill_ratio","fut_fill_ratio",
   "entry_slippage_bps","fees_entry_usdt","entry_status"]),
 ("exits",&["exit_id","entry_id","exit_reason","exit_spread_bps","spot_close_price",
   "fut_close_price","exit_slippage_bps","fees_exit_usdt","gross_pnl_usdt",
   "net_pnl_usdt","dust_left","cleanup_status","stop_cancel_status",
   "telegram_status"]),
 ("fills",&["exchange","leg","symbol","side","client_order_id","order_id",
   "fill_id","price","amount","cost","fee_amount","fee_currency","fee_usdt",
   "fee_conversion_source","exchange_event_time_ms","liquidity_role",
   "evidence_source","observation_call_id"]),
 ("errors",&["module","function","exchange","error_type","error_text",
   "traceback","severity","resolved"]),
 ("daily_reports",&["date","report_id"]),
 ("parameter_snapshots",&["snapshot_id","code_revision","config_hash"]),
 ("ai_recommendations",&["recommendation_id","status","parameter","current",
   "recommended","reason"]),
];
fn r426_projection_fields(cat:&str)->Option<&'static [&'static str]>{
 R426_CATEGORY_FIELDS.iter().find(|(name,_)|*name==cat).map(|(_,cols)|*cols)
}
fn r426_category_schema(db:&Db)->Result<(),EventOutboxR366Error>{
 for (name,fields) in R426_CATEGORY_FIELDS{
  let columns=fields.iter().map(|f|format!("\"{f}\" TEXT")).collect::<Vec<_>>().join(",");
  let sql=format!("CREATE TABLE {name}(event_id TEXT PRIMARY KEY,mode TEXT NOT NULL CHECK(mode='paper'),is_exchange_verified INTEGER NOT NULL CHECK(is_exchange_verified=0),{columns},FOREIGN KEY(event_id) REFERENCES events(event_id) ON DELETE CASCADE)");
  exec(db,&sql).map_err(r366_error)?;
 }
 exec(db,"CREATE UNIQUE INDEX r430_one_entry_commit ON events(mode,correlation_id) WHERE category='entries' AND name='ENTRY_COMMIT';
 CREATE UNIQUE INDEX r430_one_exit_commit ON events(mode,correlation_id) WHERE category='exits' AND name='EXIT_COMMIT';
 CREATE UNIQUE INDEX r430_fill_identity ON fills(mode,exchange,order_id,fill_id)
 WHERE exchange IS NOT NULL AND exchange!='' AND order_id IS NOT NULL AND order_id!='' AND fill_id IS NOT NULL AND fill_id!='';").map_err(r366_error)?;
 Ok(())
}
fn r426_bind_null(q:&mut Stmt,idx:i32)->Result<(),EventOutboxR366Error>{
 // sqlite3_bind_text with NULL pointer binds SQL NULL, never an empty string.
 if unsafe{sqlite3_bind_text(q.raw,idx,std::ptr::null(),0,None)}!=SQLITE_OK{
  return Err(EventOutboxR366Error::Sql)
 }
 Ok(())
}
fn r430_dedup_before_insert(db:&Db,record:&serde_json::Value,
 payload:&serde_json::Value)->Result<(),EventOutboxR366Error>{
 let category=r366_field(record,"category")?;
 let corr=record["correlation_id"].as_str();
 if ["orders","entries","exits","fills"].contains(&category)
   &&!corr.is_some_and(|s|!s.is_empty()){return Err(EventOutboxR366Error::Schema)}
 if category=="fills"{
  let keys=["exchange","order_id","fill_id"];
  let ids=keys.map(|k|payload[k].as_str().filter(|s|!s.is_empty()));
  if let [Some(exchange),Some(order),Some(fill)]=ids{
   let mut q=Stmt::new(db,"SELECT 1 FROM fills WHERE mode='paper'
    AND exchange=?1 AND order_id=?2 AND fill_id=?3 LIMIT 1").map_err(r366_error)?;
   for (i,s) in [exchange,order,fill].iter().enumerate(){
    q.string(i as i32+1,s).map_err(r366_error)?;
   }
   match q.step().map_err(r366_error)?{
    SQLITE_ROW=>return Err(EventOutboxR366Error::Duplicate),
    SQLITE_DONE=>{},_=>return Err(EventOutboxR366Error::Corrupt)
   }
  }
 }
 let commit=match (category,record["name"].as_str()){
  ("entries",Some("ENTRY_COMMIT"))=>Some("entries"),
  ("exits",Some("EXIT_COMMIT"))=>Some("exits"),
  _=>None
 };
 if let Some(category)=commit{
  let mut q=Stmt::new(db,"SELECT 1 FROM events WHERE category=?1
   AND name=?2 AND mode='paper' AND correlation_id=?3 LIMIT 1")
    .map_err(r366_error)?;
  q.string(1,category).map_err(r366_error)?;
  q.string(2,if category=="entries"{"ENTRY_COMMIT"}else{"EXIT_COMMIT"})
   .map_err(r366_error)?;
  q.string(3,corr.ok_or(EventOutboxR366Error::Schema)?).map_err(r366_error)?;
  match q.step().map_err(r366_error)?{
   SQLITE_ROW=>return Err(EventOutboxR366Error::Duplicate),
   SQLITE_DONE=>{},_=>return Err(EventOutboxR366Error::Corrupt)
  }
 }
 Ok(())
}
fn r426_projection_string(v:&serde_json::Value)
 ->Result<Option<String>,EventOutboxR366Error>{
 use serde_json::Value;
 match v{
  Value::Null=>Ok(None),
  Value::String(s)=>Ok(Some(s.clone())),
  Value::Number(n) if !n.is_f64()=>Ok(Some(n.to_string())),
  Value::Bool(b)=>Ok(Some(if *b{"1"}else{"0"}.to_owned())),
  Value::Array(_)=>{
   fn map_inside(v:&Value)->bool{
    match v{Value::Object(_)=>true,Value::Array(xs)=>xs.iter().any(map_inside),_=>false}
   }
   if map_inside(v){return Err(EventOutboxR366Error::Schema)}
   let raw=serde_json::to_string(v).map_err(|_|EventOutboxR366Error::Schema)?;
   Ok(Some(r396_python_inner_json(&raw)?))
  },
  // Unverified object insertion order and float -> fail closed until exact Python SQLite
  // adaptors and insertion-order oracle are validated.
  _=>Err(EventOutboxR366Error::Schema)
 }
}
fn r426_category_insert(db:&Db,cat:&str,id:&str,payload:&serde_json::Value)
 ->Result<(),EventOutboxR366Error>{
 let fields=r426_projection_fields(cat).ok_or(EventOutboxR366Error::Schema)?;
 let object=payload.as_object().ok_or(EventOutboxR366Error::Schema)?;
 let cols=fields.iter().map(|f|format!("\"{f}\"")).collect::<Vec<_>>().join(",");
 let binds=(4..=3+fields.len()).map(|i|format!("?{i}")).collect::<Vec<_>>().join(",");
 let sql=format!("INSERT INTO {cat}(event_id,mode,is_exchange_verified,{cols}) VALUES(?1,'paper',0,{binds})");
 let mut q=Stmt::new(db,&sql).map_err(r366_error)?;
 q.string(1,id).map_err(r366_error)?;
 for (i,f) in fields.iter().enumerate(){
  let v=object.get(*f).unwrap_or(&serde_json::Value::Null);
  match r426_projection_string(v)?{
   Some(s)=>q.string(i as i32+4,&s).map_err(r366_error)?,
   None=>r426_bind_null(&mut q,i as i32+4)?
  }
 }
 if q.step().map_err(r366_error)?!=SQLITE_DONE{return Err(EventOutboxR366Error::Sql)}
 Ok(())
}
fn r426_category_row(db:&Db,cat:&str,id:&str)
 ->Result<serde_json::Value,EventOutboxR366Error>{
 use serde_json::{Map,Value};
 let fields=r426_projection_fields(cat).ok_or(EventOutboxR366Error::Schema)?;
 let cols=fields.iter().map(|f|format!("\"{f}\"")).collect::<Vec<_>>().join(",");
 let sql=format!("SELECT event_id,mode,is_exchange_verified,{cols} FROM {cat} WHERE event_id=?1");
 let mut q=Stmt::new(db,&sql).map_err(r366_error)?;
 q.string(1,id).map_err(r366_error)?;
 if q.step().map_err(r366_error)?!=SQLITE_ROW{return Err(EventOutboxR366Error::Corrupt)}
 let mut row=Map::new();
 row.insert("event_id".into(),Value::String(q.strval(0).map_err(r366_error)?));
 row.insert("mode".into(),Value::String(q.strval(1).map_err(r366_error)?));
 row.insert("is_exchange_verified".into(),Value::from(q.num(2).map_err(r366_error)?));
 for (i,f) in fields.iter().enumerate(){
  let col=i as i32+3;
  let value=if unsafe{sqlite3_column_text(q.raw,col)}.is_null(){
   Value::Null
  }else{Value::String(q.strval(col).map_err(r366_error)?)};
  row.insert((*f).into(),value);
 }
 if q.step().map_err(r366_error)?!=SQLITE_DONE{return Err(EventOutboxR366Error::Corrupt)}
 Ok(Value::Object(row))
}

#[cfg(test)]
mod r426_tests{
 use super::*;
 use serde_json::{json,Value};
 use super::r366_tests::{fixture_root,record};
 #[test]fn r426_ten_source_categories_and_columns(){
  assert_eq!(R426_CATEGORY_FIELDS.len(),10);
  let root=fixture_root();let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
  let db=r366_open(&w.path).unwrap();
  for (name,fields) in R426_CATEGORY_FIELDS{
   let mut q=Stmt::new(&db,&format!("PRAGMA table_info({name})")).unwrap();
   let mut names=Vec::new();
   while q.step().unwrap()==SQLITE_ROW{names.push(q.strval(1).unwrap())}
   let mut expected=vec!["event_id","mode","is_exchange_verified"];
   expected.extend(fields.iter().copied());
   assert_eq!(names,expected,"category {name}");
  }
  drop(db);drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r427_all_ten_paper_events_durable_after_images(){
  let oracle:Value=serde_json::from_str(include_str!(
   "../config/native_r426_python_sqlite_categories_oracle.json")).unwrap();
  let root=fixture_root();let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
  for (i,(category,fields)) in R426_CATEGORY_FIELDS.iter().enumerate(){
   let mut payload=serde_json::Map::new();
   payload.insert(fields[0].to_string(),json!(format!("value_{i}")));
   let correlation=if ["orders","entries","exits","fills"].contains(category){
    json!(format!("corr_{i}"))
   }else{Value::Null};
   let event=json!({"mode":"paper","category":category,"name":"STATE_SNAPSHOT",
    "event_id":format!("r427_{i}"),"source":"native.paper",
    "run_id":"r427_fixture","occurred_at_ms":1_792_660_000_000_u64,
    "correlation_id":correlation,
    "payload":Value::Object(payload),"verification":{"status":"observed"}});
   let source=r356_paper_event_record_without_io(&event,&[],
    1_792_660_000_000).unwrap();
   w.append(&source,1_792_660_000_200+i as u64,false).unwrap();
   assert_eq!(w.audit().unwrap().outbox,i as u64+1);
   let db=r366_open(&w.path).unwrap();
   let id=format!("r427_{i}");
   let actual=r406_source_after(&db,category,&id,"insert").unwrap();
   let expected=oracle["cases"].as_array().unwrap().iter()
    .find(|f|f["category"]==*category && f["event_id"]==id).unwrap();
   assert_eq!(actual,expected["expected_after"],"Python SQLite case {category}");
  }
  drop(w);
  let re=IsolatedEventOutboxR366::reopen(&root).unwrap();
  assert_eq!((re.audit().unwrap().events,re.audit().unwrap().category_rows),(10,10));
  drop(re);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r428_absent_category_fields_are_sql_null_not_empty(){
  let root=fixture_root();let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
  w.append(&record("r428","errors"),1_792_660_000_200,false).unwrap();
  let db=r366_open(&w.path).unwrap();
  let actual=r426_category_row(&db,"errors","r428").unwrap();
  assert!(actual["function"].is_null()&&actual["resolved"].is_null());
  assert_eq!(actual["module"],"watcher");
  drop(db);drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r429_unproven_complex_or_float_projection_denied_before_commit(){
  let root=fixture_root();let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
  for (i,wrong) in [json!({"x":1}),json!([{"x":1}]),json!(1.25)].iter().enumerate(){
   let mut source=record(&format!("r429_{i}"),"decisions");
   source["payload_json"]=json!(json!({"decision":wrong}).to_string());
   assert_eq!(w.append(&source,1_792_660_000_200,false),Err(EventOutboxR366Error::Schema));
  }
  assert_eq!(w.audit().unwrap().events,0);
  drop(w);fs::remove_dir_all(root).unwrap();
 }
}
