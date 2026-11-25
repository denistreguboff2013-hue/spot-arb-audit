// Integration tests use the actual isolated dispatcher; prepared, not run by Astra.
use super::*;
fn batch()->OfflineMarketBatch{serde_json::from_str(include_str!("../fixtures/r1298/full_exit.json")).unwrap()}
fn runtime(b:&OfflineMarketBatch)->IsolatedPaperRuntime{
 let n=SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
 new_isolated_runtime(b,&PathBuf::from(format!("/mnt/spotarb_db_test/r1273_r1275_engine_latency_20261111/R1298/lab/entrypoint/{n}/paper.jsonl"))).unwrap()
}
fn market(venue:Venue,n:u64,depth:&str)->OfflineInput{
 let b=batch();let old=b.inputs.iter().find(|i|matches!(i,OfflineInput::Market{frame,..} if frame.venue==venue)).unwrap();
 let OfflineInput::Market{frame,context}=old else{unreachable!()};
 let mut f=frame.clone();let mut c=context.clone();c.input_id=format!("tail-{n}");c.now_ns=1_000_000_000+n*1_000_000;c.wall_ms=1_700_000_000_000+n as i64;
 f.received_ns=c.now_ns;f.received_wall_ms=c.wall_ms;let mut v:Value=serde_json::from_str(&f.raw).unwrap();
 if venue==Venue::OkxSpot{v["data"][0]["seqId"]=json!(n);v["data"][0]["ts"]=json!(c.wall_ms.to_string());v["data"][0]["bids"][0][1]=json!(depth);v["data"][0]["asks"][0][1]=json!(depth);}
 else{v["data"]["u"]=json!(n);v["data"]["E"]=json!(c.wall_ms);v["data"]["B"]=json!(depth);v["data"]["A"]=json!(depth);}
 f.raw=serde_json::to_string(&v).unwrap();OfflineInput::Market{frame:f,context:c}
}
#[test] fn r1298_actual_full_exit_and_closed_restart(){
 let b=batch();let mut r=runtime(&b);
 for i in &b.inputs{process_isolated_input(&mut r,i).unwrap();}
 assert_eq!(r.execution.lifecycle().state,paper_risk::LifecycleState::Closed);
 assert_eq!(r.execution.lifecycle().orders.len(),4);assert_eq!(r.execution.lifecycle().processed_events.len(),4);
 assert_eq!(r.execution.projection().spot_base,0.);assert_eq!(r.execution.projection().futures_base,0.);
 assert!(r.actions.iter().any(|a|a["action"]=="durable_closed"));
 let before=serde_json::to_value(r.execution.projection()).unwrap();let p=r.execution.journal_path().to_path_buf();let seq=r.execution.sequence();drop(r);
 let d=DurablePaper::recover_isolated(p,&b.identity).unwrap();assert_eq!(d.sequence(),seq);assert_eq!(serde_json::to_value(d.projection()).unwrap(),before);
}
#[test] fn r1298_actual_partial_exit_duplicate_inputs(){
 let mut b=batch();b.inputs[7]=market(Venue::BinanceFutures,8,"0.5");
 b.inputs.extend([market(Venue::BinanceFutures,10,"20"),market(Venue::OkxSpot,11,"0.5"),market(Venue::OkxSpot,12,"20")]);
 let mut r=runtime(&b);
 for i in &b.inputs{
  process_isolated_input(&mut r,i).unwrap();let seq=r.execution.sequence();
  process_isolated_input(&mut r,i).unwrap();assert_eq!(r.execution.sequence(),seq);
 }
 assert_eq!(r.execution.lifecycle().state,paper_risk::LifecycleState::Closed);
 assert_eq!(r.execution.lifecycle().processed_events.len(),6);
}
#[test] fn r1298_actual_generation_after_closed(){
 let b=batch();let mut r=runtime(&b);for i in &b.inputs{process_isolated_input(&mut r,i).unwrap();}
 let OfflineInput::Market{context,..}=market(Venue::OkxSpot,10,"20") else{unreachable!()};
 let command=OfflineInput::NewGeneration{context};process_isolated_input(&mut r,&command).unwrap();
 let seq=r.execution.sequence();process_isolated_input(&mut r,&command).unwrap();assert_eq!(r.execution.sequence(),seq);
 process_isolated_input(&mut r,&market(Venue::OkxSpot,11,"20")).unwrap();
 process_isolated_input(&mut r,&market(Venue::BinanceFutures,12,"20")).unwrap();
 assert!(r.execution.lifecycle().orders.keys().any(|id|id.ends_with(":5")));
 assert_eq!(r.execution.isolated_entry_key(),"entry:2");
}
