// R430: offline-only original Python collector commit/fill identity guards.
// Synthetic PAPER fixture, not installed PAPER, no exchange API.
#[cfg(test)]
mod r430_dedup_tests{
 use super::*;
 use serde_json::{json,Value};
 use super::r366_tests::fixture_root;
 const NOW:u64=1_792_660_000_000;
 fn source(id:&str,cat:&str,stage:&str,corr:&str,payload:Value)->Value{
  r356_paper_event_record_without_io(&json!({
   "mode":"paper","category":cat,"name":stage,"event_id":id,
   "source":"native.paper","run_id":"r430_fixture",
   "occurred_at_ms":NOW,"correlation_id":corr,
   "payload":payload,"verification":{"status":"observed"}}),&[],NOW).unwrap()
 }
 #[test]fn r430_same_fill_different_event_id_is_duplicate(){
  let root=fixture_root();let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
  let fill=json!({"exchange":"okx","order_id":"order100","fill_id":"fill42",
   "fee_currency":"USDT"});
  let a=source("r430_fill_1","fills","FILL_OBSERVED","corrA",fill.clone());
  let b=source("r430_fill_2","fills","FILL_OBSERVED","corrB",fill.clone());
  w.append(&a,NOW+20,false).unwrap();
  assert_eq!(w.append(&b,NOW+21,false),Err(EventOutboxR366Error::Duplicate));
  assert_eq!((w.audit().unwrap().events,w.audit().unwrap().outbox),(1,1));
  drop(w);
  let reopened=IsolatedEventOutboxR366::reopen(&root).unwrap();
  assert_eq!(reopened.append(&b,NOW+30,false),Err(EventOutboxR366Error::Duplicate));
  drop(reopened);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r431_second_entry_and_exit_commit_rejected(){
  let root=fixture_root();let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
  for (cat,stage) in [("entries","ENTRY_COMMIT"),("exits","EXIT_COMMIT")]{
   let a=source(&format!("r431_{cat}_a"),cat,stage,"shared_corr",json!({}));
   let b=source(&format!("r431_{cat}_b"),cat,stage,"shared_corr",json!({}));
   w.append(&a,NOW+20,false).unwrap();
   assert_eq!(w.append(&b,NOW+21,false),Err(EventOutboxR366Error::Duplicate));
  }
  assert_eq!((w.audit().unwrap().events,w.audit().unwrap().outbox),(2,2));
  drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r432_missing_execution_correlation_rejected(){
  let root=fixture_root();let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
  let mut source=source("r432_no_corr","fills","FILL_OBSERVED",
   "corr",json!({"exchange":"okx","order_id":"x","fill_id":"y"}));
  source["correlation_id"]=Value::Null;
  assert_eq!(w.append(&source,NOW+1,false),Err(EventOutboxR366Error::Schema));
  assert_eq!(w.audit().unwrap().events,0);
  drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r433_python_nested_scalar_array_unicode_after_image(){
  let root=fixture_root();let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
  let value=json!(["Екатеринбург",true,null,[1,2]]);
  let event=source("r433_array","decisions","DECISION_MADE","corr_arrays",
   json!({"decision":value}));
  w.append(&event,NOW+50,false).unwrap();
  let db=r366_open(&w.path).unwrap();
  let after=r406_source_after(&db,"decisions","r433_array","insert").unwrap();
  assert_eq!(after["row"]["decision"],r#"["Екатеринбург", true, null, [1, 2]]"#);
  assert_eq!(after["after_hash"],r366_sha(&r386_python_mirror_canonical(&after["row"]).unwrap()));
  assert_eq!(w.audit().unwrap().outbox,1);
  drop(db);drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r434_distinct_fills_same_order_are_preserved(){
  let root=fixture_root();let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
  for (i,fill) in ["fill_one","fill_two"].iter().enumerate(){
   let source=source(&format!("r434_fill_{i}"),"fills","FILL_OBSERVED",
    "corr_shared",json!({"exchange":"okx","order_id":"order_same","fill_id":fill}));
   w.append(&source,NOW+200+i as u64,false).unwrap();
  }
  assert_eq!((w.audit().unwrap().events,w.audit().unwrap().category_rows),(2,2));
  drop(w);fs::remove_dir_all(root).unwrap();
 }
}
