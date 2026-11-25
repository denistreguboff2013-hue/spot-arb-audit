// R446-R455: read-only PAPER raw retention candidate selection.
// No DELETE, no rollup mutations, no mirror ACK and no production collector.
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct R446RetentionPreview{
 pub candidates:Vec<String>,
 pub examined:u64,
 pub execution_evidence:u64,
 pub deletion_authorized:bool,
}
#[derive(Debug,Clone)]
struct R446Row{
 id:String,category:String,name:String,received:u64,
 correlation:Option<String>,signal:Option<String>,
}
fn r446_opt_identity(source:&serde_json::Value,key:&str)
 ->Result<Option<String>,EventOutboxR366Error>{
 let v=&source[key];
 if v.is_null(){return Ok(None)}
 let s=v.as_str().ok_or(EventOutboxR366Error::Corrupt)?;
 if s.is_empty()||s.len()>160{return Err(EventOutboxR366Error::Corrupt)}
 Ok(Some(s.to_string()))
}
impl IsolatedEventOutboxR366{
 /// A bounded source-owned selection witness. Never permits deletion.
 pub fn r446_retention_preview(&self,now:u64,days:u64,pressure:bool,
   max_batch:usize)->Result<R446RetentionPreview,EventOutboxR366Error>{
  if now==0||!(1..=365).contains(&days)||!(1..=500).contains(&max_batch){
   return Err(EventOutboxR366Error::Schema)
  }
  let age=if pressure{1_800_000}else{days.checked_mul(86_400_000)
   .ok_or(EventOutboxR366Error::Schema)?};
  let cutoff=now.checked_sub(age).ok_or(EventOutboxR366Error::Schema)?;
  self.audit()?;
  let db=r366_open(&self.path)?;
  let mut q=Stmt::new(&db,"SELECT event_id,mode,category,name,received_at_ms,
    source_record_json FROM events ORDER BY received_at_ms,event_id").map_err(r366_error)?;
  let mut rows=Vec::<R446Row>::new();
  let mut executed=std::collections::BTreeSet::<String>::new();
  let mut exec_signals=std::collections::BTreeSet::<String>::new();
  let mut execution_evidence=0u64;
  loop{
   match q.step().map_err(r366_error)?{
    SQLITE_DONE=>break,
    SQLITE_ROW=>{
     if rows.len()>=4096{return Err(EventOutboxR366Error::Pressure)}
     let id=q.strval(0).map_err(r366_error)?;
     if q.strval(1).map_err(r366_error)?!="paper"{
      return Err(EventOutboxR366Error::Corrupt)
     }
     let category=q.strval(2).map_err(r366_error)?;
     let name=q.strval(3).map_err(r366_error)?;
     let received=q.num(4).map_err(r366_error)?;
     let src=q.strval(5).map_err(r366_error)?;
     let source:serde_json::Value=serde_json::from_str(&src)
       .map_err(|_|EventOutboxR366Error::Corrupt)?;
     if source["event_id"]!=id||source["category"]!=category||
       source["name"]!=name||source["mode"]!="paper"{
       return Err(EventOutboxR366Error::Corrupt)
     }
     let correlation=r446_opt_identity(&source,"correlation_id")?;
     let signal=r446_opt_identity(&source,"signal_id")?;
     if ["entries","exits","fills"].contains(&category.as_str()){
      execution_evidence+=1;
      if let Some(c)=&correlation{executed.insert(c.clone());}
      if let Some(s)=&signal{exec_signals.insert(s.clone());}
     }
     rows.push(R446Row{id,category,name,received,correlation,signal});
    },
    _=>return Err(EventOutboxR366Error::Corrupt)
   }
  }
  drop(q);
  let examined=rows.len() as u64;
  let mut candidates=Vec::new();
  for row in rows{
   if candidates.len()==max_batch{break}
   if row.received>=cutoff||
      !(row.category=="signals"&&row.name=="SIGNAL_SEEN"
        ||row.category=="decisions"&&row.name=="DECISION_MADE"){
    continue
   }
   // Python SQL compares nullable values with '=': NULL does not link.
   if row.correlation.as_ref().is_some_and(|c|executed.contains(c))||
      exec_signals.contains(&row.id)||
      row.signal.as_ref().is_some_and(|s|exec_signals.contains(s)){
    continue
   }
   candidates.push(row.id);
  }
  Ok(R446RetentionPreview{candidates,examined,execution_evidence,
    deletion_authorized:false})
 }
}
#[cfg(test)]
mod r446_tests{
 use super::*;
 use super::r366_tests::fixture_root;
 use serde_json::{json,Value};
 const NOW:u64=1_792_660_000_000;
 fn record(id:&str,cat:&str,name:&str,correlation:Option<&str>,
   signal:Option<&str>)->Value{
  r356_paper_event_record_without_io(&json!({
   "mode":"paper","category":cat,"name":name,"event_id":id,
   "source":"native.paper","run_id":"r446_run",
   "occurred_at_ms":NOW-9*86_400_000,
   "correlation_id":correlation,"signal_id":signal,"base":"ONE",
   "payload":{},"verification":{"status":"observed"}
  }),&[],NOW).unwrap()
 }
 #[test]fn r446_old_unlinked_signal_and_decision_candidates_only(){
  let root=fixture_root();let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
  w.append(&record("signal_old","signals","SIGNAL_SEEN",None,None),NOW-8*86_400_000,false).unwrap();
  w.append(&record("decision_old","decisions","DECISION_MADE",None,None),NOW-8*86_400_000+1,false).unwrap();
  w.append(&record("error_old","errors","ERROR",None,None),NOW-8*86_400_000+2,false).unwrap();
  let p=w.r446_retention_preview(NOW,7,false,500).unwrap();
  assert_eq!(p.candidates,vec!["signal_old","decision_old"]);
  assert_eq!((p.examined,p.execution_evidence,p.deletion_authorized),(3,0,false));
  assert_eq!((w.audit().unwrap().events,w.audit().unwrap().outbox),(3,3));
  drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r447_shared_correlation_preserves_raw_evidence(){
  let root=fixture_root();let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
  w.append(&record("dec_corr","decisions","DECISION_MADE",Some("corr1"),None),
   NOW-8*86_400_000,false).unwrap();
  w.append(&record("entry_corr","entries","ENTRY_ABORT",Some("corr1"),None),
   NOW-8*86_400_000+1,false).unwrap();
  let p=w.r446_retention_preview(NOW,7,false,500).unwrap();
  assert!(p.candidates.is_empty());assert_eq!(p.execution_evidence,1);
  drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r448_executed_signal_id_protects_raw_event_id(){
  let root=fixture_root();let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
  w.append(&record("signal_parent","signals","SIGNAL_SEEN",None,None),
   NOW-8*86_400_000,false).unwrap();
  w.append(&record("fill_link","fills","FILL_OBSERVED",Some("c2"),Some("signal_parent")),
   NOW-8*86_400_000+1,false).unwrap();
  let p=w.r446_retention_preview(NOW,7,false,500).unwrap();
  assert!(p.candidates.is_empty());assert_eq!(p.execution_evidence,1);
  drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r449_executed_signal_id_protects_sibling_decision(){
  let root=fixture_root();let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
  w.append(&record("decision_link","decisions","DECISION_MADE",None,Some("sig_parent")),
   NOW-8*86_400_000,false).unwrap();
  w.append(&record("exit_link","exits","EXIT_FAILED",Some("c3"),Some("sig_parent")),
   NOW-8*86_400_000+1,false).unwrap();
  assert!(w.r446_retention_preview(NOW,7,false,500).unwrap().candidates.is_empty());
  drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r450_age_boundary_is_strict_and_pressure_is_30_minutes(){
  let root=fixture_root();let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
  let cutoff=NOW-7*86_400_000;
  w.append(&record("boundary","signals","SIGNAL_SEEN",None,None),cutoff,false).unwrap();
  w.append(&record("younger","decisions","DECISION_MADE",None,None),NOW-3_600_000,false).unwrap();
  let normal=w.r446_retention_preview(NOW,7,false,500).unwrap();
  assert!(normal.candidates.is_empty());
  let pressure=w.r446_retention_preview(NOW,7,true,500).unwrap();
  assert_eq!(pressure.candidates,vec!["boundary","younger"]);
  drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r451_batch_limit_oldest_first_and_nonmutation(){
  let root=fixture_root();let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
  for (i,id) in ["first","second","third"].iter().enumerate(){
   w.append(&record(id,"decisions","DECISION_MADE",None,None),
    NOW-8*86_400_000+i as u64,false).unwrap();
  }
  assert_eq!(w.r446_retention_preview(NOW,7,false,2).unwrap().candidates,
    vec!["first","second"]);
  assert_eq!((w.audit().unwrap().events,w.audit().unwrap().last_seq),(3,3));
  drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r452_invalid_retention_budget_fails_closed(){
  let root=fixture_root();let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
  for (now,days,limit) in [(0,7,500),(NOW,0,500),
    (NOW,366,500),(NOW,7,0),(NOW,7,501),(1,7,500)]{
   assert_eq!(w.r446_retention_preview(now,days,false,limit),
    Err(EventOutboxR366Error::Schema));
  }
  assert_eq!(w.audit().unwrap().last_seq,0);
  drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r453_wrong_event_name_is_not_raw_retention_candidate(){
  let root=fixture_root();let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
  w.append(&record("signal_unexpected","signals","PRECHECK_WARN",None,None),
    NOW-8*86_400_000,false).unwrap();
  assert!(w.r446_retention_preview(NOW,7,false,500).unwrap().candidates.is_empty());
  drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r454_execution_of_other_attempt_does_not_block_independent_raw(){
  let root=fixture_root();let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
  w.append(&record("decision_other","decisions","DECISION_MADE",Some("other_corr"),None),
    NOW-8*86_400_000,false).unwrap();
  w.append(&record("entry_unrelated","entries","ENTRY_ABORT",Some("exec_corr"),None),
    NOW-8*86_400_000+1,false).unwrap();
  assert_eq!(w.r446_retention_preview(NOW,7,false,500).unwrap().candidates,
    vec!["decision_other"]);
  drop(w);fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r455_preview_remains_read_only_across_reopen(){
  let root=fixture_root();let w=IsolatedEventOutboxR366::initialize(&root).unwrap();
  w.append(&record("signal_reopen","signals","SIGNAL_SEEN",None,None),
    NOW-8*86_400_000,false).unwrap();
  let before=w.r446_retention_preview(NOW,7,false,500).unwrap();
  drop(w);
  let re=IsolatedEventOutboxR366::reopen(&root).unwrap();
  let after=re.r446_retention_preview(NOW,7,false,500).unwrap();
  assert_eq!(before,after);assert!(!after.deletion_authorized);
  assert_eq!((re.audit().unwrap().events,re.audit().unwrap().outbox),(1,1));
  drop(re);fs::remove_dir_all(root).unwrap();
 }
}
