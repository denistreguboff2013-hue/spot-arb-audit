// Astra P0-01 R951: a reusable SQLite prepared INSERT specifically
// for private ORIGINAL 17-table PAPER events (one table, exact 20 cols).
// Retains R586's full key/schema/after_hash verification, physical
// SQLite foreign keys/triggers, sqlite3_changes==1 and FULL WAL durability.
// Not a replacement for arbitrary Python 17-table updates/deletes.
extern "C"{
 fn sqlite3_reset(stmt:*mut c_void)->c_int;
 fn sqlite3_clear_bindings(stmt:*mut c_void)->c_int;
 fn sqlite3_bind_null(stmt:*mut c_void,index:c_int)->c_int;
}
fn r951_original_events_projection(v:&serde_json::Value)
 ->Result<Vec<String>,EventOutboxR366Error>{
 r566_check_source_manifest(v)?;
 let keys=v["source_keys"]["events"].as_array().ok_or(EventOutboxR366Error::Schema)?;
 if keys.len()!=1||keys[0]!="event_id"{return Err(EventOutboxR366Error::Schema)}
 let schema=v["table_schema"].as_array().ok_or(EventOutboxR366Error::Schema)?
  .iter().find(|x|x["table"]=="events").ok_or(EventOutboxR366Error::Schema)?;
 let cols=schema["columns"].as_array().ok_or(EventOutboxR366Error::Schema)?;
 if cols.len()!=20{return Err(EventOutboxR366Error::Schema)}
 cols.iter().map(|c|{
  let name=c[1].as_str().ok_or(EventOutboxR366Error::Schema)?;
  r586_identifier(name)?;
  Ok(name.to_owned())
 }).collect()
}
fn r951_prepared_insert(db:&Db,cols:&[String])->Result<Stmt,EventOutboxR366Error>{
 if cols.len()!=20{return Err(EventOutboxR366Error::Schema)}
 let names=cols.iter().map(|c|r586_identifier(c))
  .collect::<Result<Vec<_>,_>>()?.join(",");
 let binds=vec!["?";cols.len()].join(",");
 Stmt::new(db,&format!("INSERT INTO events ({names}) VALUES ({binds})"))
  .map_err(r366_error)
}
fn r951_bind_and_insert(
 stmt:&mut Stmt,db:&Db,cols:&[String],op:&serde_json::Value
)->Result<(),EventOutboxR366Error>{
 if op["table"]!="events"||op["op"]!="insert"{return Err(EventOutboxR366Error::Corrupt)}
 let key=op["key"].as_object().ok_or(EventOutboxR366Error::Corrupt)?;
 let row=op["row"].as_object().ok_or(EventOutboxR366Error::Corrupt)?;
 if key.len()!=1||row.len()!=cols.len()
  ||key.get("event_id")!=row.get("event_id")
  ||cols.iter().any(|c|!row.contains_key(c))
  ||op["row"]["mode"]!="paper"
  ||op["row"]["verification_status"]!="unverified"
  ||op["row"]["is_exchange_verified"]!=0{
  return Err(EventOutboxR366Error::Corrupt)
 }
 let canonical=r386_python_mirror_canonical(&op["row"])?;
 if op["after_hash"]!=r366_sha(&canonical){return Err(EventOutboxR366Error::Corrupt)}
 // SQLite prepared bindings must keep string storage alive until step().
 if unsafe{sqlite3_reset(stmt.raw)}!=SQLITE_OK
  ||unsafe{sqlite3_clear_bindings(stmt.raw)}!=SQLITE_OK{
  return Err(EventOutboxR366Error::Sql)
 }
 stmt.texts.clear();
 for (i,c) in cols.iter().enumerate(){
  let pos=(i+1) as c_int;
  let value=&row[c];
  let result=match value{
   serde_json::Value::Null=>{
    if unsafe{sqlite3_bind_null(stmt.raw,pos)}==SQLITE_OK{Ok(())}
    else{Err(EventOutboxR366Error::Sql)}
   },
   serde_json::Value::String(s)=>stmt.string(pos,s).map_err(r366_error),
   serde_json::Value::Bool(b)=>stmt.number(pos,u64::from(*b)).map_err(r366_error),
   serde_json::Value::Number(n)=>{
    if let Some(u)=n.as_u64(){stmt.number(pos,u).map_err(r366_error)}
    else{Err(EventOutboxR366Error::Corrupt)}
   },
   _=>Err(EventOutboxR366Error::Corrupt),
  };
  result?;
 }
 if stmt.step().map_err(r366_error)?!=SQLITE_DONE
  || unsafe{sqlite3_changes(db.0)}!=1{
  return Err(EventOutboxR366Error::Corrupt)
 }
 Ok(())
}
