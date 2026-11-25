// Tests are prepared for Luna; no execution by Astra.
use super::*;
fn batch()->OfflineMarketBatch{serde_json::from_str(include_str!("../fixtures/r1296/full_hedge.json")).unwrap()}
fn journal()->PathBuf{
 let n=SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
 PathBuf::from(format!("/mnt/spotarb_db_test/r1273_r1275_engine_latency_20261111/R1296/lab/entrypoint-tests/{n}/paper.jsonl"))
}
#[test] fn r1296_actual_entrypoint_both_legs(){
 let b=batch();let p=journal();let mut r=new_isolated_runtime(&b,&p).unwrap();
 for i in &b.inputs{if let OfflineInput::Market{frame,context}=i{process_validated_market_input(&mut r,frame,context).unwrap()}}
 assert_eq!(r.execution.lifecycle().orders.len(),2);
 assert_eq!(r.execution.lifecycle().processed_events.len(),2);
 assert!(r.execution.projection().spot_base>0.);
 assert_eq!(r.execution.projection().residual_base,0.);
 assert!(r.actions.iter().any(|v|v["action"]=="hedge_registered"));
 let before=r.execution.projection().clone();drop(r);
 let recovered=DurablePaper::recover_isolated(&p,&b.identity).unwrap();
 assert_eq!(recovered.projection().spot_base,before.spot_base);
 assert_eq!(recovered.projection().futures_base,before.futures_base);
 assert_eq!(recovered.lifecycle().state,paper_risk::LifecycleState::RecoveryRequired);
}
#[test] fn r1296_actual_entrypoint_partial_and_duplicate(){
 let mut b=batch();
 if let OfflineInput::Market{frame,..}=&mut b.inputs[2]{frame.raw=frame.raw.replace("\"20\"","\"0.5\"");}
 let p=journal();let mut r=new_isolated_runtime(&b,&p).unwrap();
 for i in &b.inputs{
  if let OfflineInput::Market{frame,context}=i{
   process_validated_market_input(&mut r,frame,context).unwrap();
   let n=r.execution.sequence();process_validated_market_input(&mut r,frame,context).unwrap();assert_eq!(n,r.execution.sequence());
  }
 }
 assert_eq!(r.execution.projection().spot_base,0.5);
 assert_eq!(r.execution.projection().futures_base,0.5);
 assert!(r.execution.lifecycle().orders.values().any(|o|o.leg==paper_risk::FillLeg::Spot&&o.remaining_quantity>0.));
}
#[test] fn r1296_selector_isolation(){
 assert_eq!(parse_runtime_plan(&[],&[]).unwrap().mode,RuntimeMode::Legacy);
 let a=vec!["--paper-runtime=execution-isolated".into(),"--offline-input=/lab/input.json".into(),"--journal-root=/lab/journal".into()];
 assert!(parse_runtime_plan(&a,&[]).is_ok());
 assert!(parse_runtime_plan(&a,&["PAPER_CONTINUITY_PATH"]).is_err());
 assert!(parse_runtime_plan(&["--paper-runtime=real".into()],&[]).is_err());
 let source=include_str!("bin/native_paper_live_portfolio_v1.rs");
 let main=source.split("async fn main()").nth(1).unwrap();
 assert!(main.find("dispatch_runtime_plan").unwrap()<main.find("PaperPortfolio").unwrap());
 assert!(!include_str!("native_paper_isolated_r1296.rs").contains(".on_frame("));
}
