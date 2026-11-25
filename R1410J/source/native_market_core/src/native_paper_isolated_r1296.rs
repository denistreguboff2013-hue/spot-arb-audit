use spotarb_native_market_core::native_paper_execution_r1289::IsolatedControl;
// Included by the real native_paper_live_portfolio_v1 entrypoint only.
use spotarb_native_market_core::native_paper_hedge_r1296 as isolated_policy;
#[derive(Clone,Debug,serde::Serialize,serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct OfflineMarketBatch {
    schema:u32,
    identity:isolated_policy::Identity,
    inputs:Vec<OfflineInput>,
}
#[derive(Clone,Debug,serde::Serialize,serde::Deserialize)]
#[serde(tag="type",deny_unknown_fields)]
enum OfflineInput {
    NewGeneration { context:isolated_policy::MarketContext },
    ExitTrigger { context:isolated_policy::MarketContext },
    Market { frame:RawFrame, context:isolated_policy::MarketContext },
    Control { control:IsolatedControl, context:isolated_policy::MarketContext },
}
struct IsolatedPaperRuntime {
    book_owner:Owner,
    decision:DecisionCore,
    history:NoHistory,
    execution:DurablePaper,
    identity:isolated_policy::Identity,
    frames_seen:usize,
    actions:Vec<Value>,
    seen_inputs:HashMap<String,String>,
}
fn isolated_route(p:&isolated_policy::InstrumentProfile)->RouteSpec{
    let limits=DecisionLimits{min_amount:p.minimum_quantity.clone(),step:p.step.clone(),
        min_cost:p.minimum_notional.clone(),max_amount:Some(p.maximum_quantity.clone())};
    RouteSpec{buy_exchange:"okx".into(),sell_exchange:"binance".into(),
        label:"explicit synthetic OKX Spot BUY -> Binance Futures SHORT".into(),
        buy:limits.clone(),sell:limits,loaded_ns:p.metadata_loaded_ns,
        metadata_state:"SIZE_METADATA_READY".into()}
}
fn offline_adapter_error<E:std::fmt::Debug>(e:E)->std::io::Error{
    std::io::Error::new(std::io::ErrorKind::InvalidData,format!("{e:?}"))
}
fn new_isolated_runtime(batch:&OfflineMarketBatch,journal:&PathBuf)->Result<IsolatedPaperRuntime,Box<dyn std::error::Error>>{
    if batch.schema!=2||batch.inputs.is_empty()||batch.inputs.len()>100_000{return Err("invalid typed offline manifest".into())}
    new_isolated_identity_runtime(&batch.identity,journal)
}
fn new_isolated_identity_runtime(identity:&isolated_policy::Identity,journal:&PathBuf)->Result<IsolatedPaperRuntime,Box<dyn std::error::Error>>{
    identity.validate().map_err(offline_adapter_error)?;
    let base=&identity.profile.base;
    let mut config=cfg();
    config.metadata_ttl_ms=identity.profile.metadata_ttl_ns/1_000_000;
    let decision=DecisionCore::new(market_owner_index(base,1),1,config,
        HashMap::from([(base.clone(),vec![isolated_route(&identity.profile)])])).map_err(offline_adapter_error)?;
    let execution=if journal.exists(){
        let owner=DurablePaper::recover_isolated(journal,&identity).map_err(offline_adapter_error)?;
        if owner.isolated_identity()!=Some(&identity){return Err("journal run/instrument/rules/clock mismatch".into())}
        owner
    }else{DurablePaper::create_isolated(journal,identity.clone()).map_err(offline_adapter_error)?};
    Ok(IsolatedPaperRuntime{book_owner:Owner::new(market_owner_index(base,1),1,800).map_err(offline_adapter_error)?,
        decision,history:NoHistory{accepted:0},execution,identity:identity.clone(),
        frames_seen:0,actions:Vec::new(),seen_inputs:HashMap::new()})
}
fn process_validated_market_input(runtime:&mut IsolatedPaperRuntime,frame:&RawFrame,c:&isolated_policy::MarketContext)
 ->Result<(),Box<dyn std::error::Error>>{
    runtime.frames_seen+=1;
    if frame.source.is_some(){return Err("offline frame cannot impersonate live custody".into())}
    let input_hash=isolated_policy::digest(&(frame,c)).map_err(offline_adapter_error)?;
    if let Some(old)=runtime.seen_inputs.get(&c.input_id){
        if old!=&input_hash{return Err("input ID collision".into())}
        runtime.actions.push(json!({"action":"duplicate_input","input_id":c.input_id}));
        return Ok(())
    }
    runtime.seen_inputs.insert(c.input_id.clone(),input_hash);
    if c.clock_domain!=runtime.identity.clock_domain||c.now_ns<frame.received_ns{return Err("clock domain/regression".into())}
    let update=parse_market_frame(frame).map_err(offline_adapter_error)?;
    let update=runtime.book_owner.prepare_parsed(frame,update).map_err(offline_adapter_error)?;
    if update.base!=runtime.identity.profile.base{return Err("unsupported instrument".into())}
    let base=update.base.clone();
    let exchange=match frame.venue{Venue::OkxSpot=>"okx",Venue::BinanceFutures=>"binance"};
    let rows=runtime.decision.update_at(&base,exchange,update.book.clone(),c.now_ns,c.wall_ms).map_err(offline_adapter_error)?;
    runtime.book_owner.ingest_parsed(frame,update,&mut runtime.history).map_err(offline_adapter_error)?;
    let (Some(spot),Some(futures))=runtime.book_owner.snapshot_pair(&base) else{return Ok(())};
    if runtime.execution.lifecycle().state==paper_risk::LifecycleState::Closed{
        runtime.actions.push(json!({"action":"closed_no_action"}));return Ok(())
    }
    process_isolated_pair(runtime,frame.venue,c,spot,futures,rows)
}
fn process_isolated_pair(runtime:&mut IsolatedPaperRuntime,venue:Venue,c:&isolated_policy::MarketContext,spot:spotarb_native_market_core::Book,futures:spotarb_native_market_core::Book,rows:Vec<spotarb_native_market_core::decision::Decision>)->Result<(),Box<dyn std::error::Error>>{
    process_isolated_pair_guarded(runtime,venue,c,spot,futures,rows,|_,_|Ok(()))
}
// One lifecycle implementation. R1322 supplies a fresh gate at every effect boundary.
fn process_isolated_pair_guarded<F>(runtime:&mut IsolatedPaperRuntime,venue:Venue,c:&isolated_policy::MarketContext,spot:spotarb_native_market_core::Book,futures:spotarb_native_market_core::Book,rows:Vec<spotarb_native_market_core::decision::Decision>,mut gate:F)->Result<(),Box<dyn std::error::Error>>
where F:FnMut(&DurablePaper,Option<f64>)->Result<(),Box<dyn std::error::Error>>{
    gate(&runtime.execution,None)?;
    runtime.execution.control_isolated(IsolatedControl::Clock,c).map_err(offline_adapter_error)?;
    if let Err(reason)=isolated_policy::validate_pair(&runtime.identity,&spot,&futures,c){
        let mut incident=c.clone();incident.input_id=format!("{}:market-denied",c.input_id);
        runtime.execution.control_isolated(IsolatedControl::RecoveryIncident{reason:reason.clone()},&incident).map_err(offline_adapter_error)?;
        runtime.actions.push(json!({"action":"market_denied","reason":reason}));return Ok(())
    }
    let leg=if venue==Venue::OkxSpot{paper_risk::FillLeg::Spot}else{paper_risk::FillLeg::Futures};
    let pending:Vec<String>=runtime.execution.lifecycle().orders.values()
        .filter(|o|o.leg==leg && !o.uncertain && matches!(o.status,paper_risk::OrderStatus::Pending|paper_risk::OrderStatus::PartiallyFilled|paper_risk::OrderStatus::CancelPending))
        .map(|o|o.order_id.clone()).collect();
    if runtime.execution.exposure_view().known {
        for id in pending {
            gate(&runtime.execution,None)?;
            if let Some(event)=runtime.execution.simulate_isolated_order(&id,&spot,&futures,c).map_err(offline_adapter_error)?{
                runtime.actions.push(json!({"action":"simulated_fill","event":event}));
            }
        }
    }
    if runtime.execution.exit_requested(){
        let decision=runtime.execution.isolated_exit_decision(&spot,&futures,c);
        if matches!(decision,spotarb_native_market_core::native_paper_exit_r1298::ExitDecision::FuturesCover{..}|spotarb_native_market_core::native_paper_exit_r1298::ExitDecision::SpotExit{..}){
            let quantity=match &decision{spotarb_native_market_core::native_paper_exit_r1298::ExitDecision::FuturesCover{quantity}|spotarb_native_market_core::native_paper_exit_r1298::ExitDecision::SpotExit{quantity}=>*quantity,_=>unreachable!()};
            gate(&runtime.execution,Some(quantity))?;
            let order=runtime.execution.register_isolated_exit(&spot,&futures,c).map_err(offline_adapter_error)?;
            runtime.actions.push(json!({"action":"exit_registered","order":order}));
        }
        if matches!(decision,spotarb_native_market_core::native_paper_exit_r1298::ExitDecision::Census){
            gate(&runtime.execution,None)?;
            runtime.execution.close_isolated_generation(c).map_err(offline_adapter_error)?;
            runtime.actions.push(json!({"action":"durable_closed"}));
        }
        if let spotarb_native_market_core::native_paper_exit_r1298::ExitDecision::FailClosed(reason)|spotarb_native_market_core::native_paper_exit_r1298::ExitDecision::RiskExit(reason)=&decision{
            let mut incident=c.clone();incident.input_id=format!("{}:exit-denied",c.input_id);
            runtime.execution.control_isolated(IsolatedControl::RecoveryIncident{reason:reason.clone()},&incident).map_err(offline_adapter_error)?;
        }
        runtime.actions.push(json!({"action":"exit_policy","decision":decision}));
        return Ok(())
    }
    let view=runtime.execution.exposure_view();
    if view.state==paper_risk::LifecycleState::Flat {
        let Some(d)=rows.iter().find(|d|d.buy_exchange=="okx"&&d.sell_exchange=="binance"&&
            d.reason=="EXECUTABLE_OPPORTUNITY"&&d.risk_result.as_deref()==Some("RISK_SIZE_ALLOW")) else{
                runtime.actions.push(json!({"action":"entry_denied","input_id":c.input_id}));return Ok(())
            };
        let q=d.quantity.ok_or("decision quantity missing")?;
        gate(&runtime.execution,Some(q))?;
        let order=runtime.execution.ensure_isolated_intent(view.revision,runtime.execution.isolated_entry_key(),
            paper_risk::FillLeg::Spot,q,&spot,&futures,c).map_err(offline_adapter_error)?;
        runtime.actions.push(json!({"action":"spot_registered","order":order}));
        return Ok(())
    }
    match isolated_policy::plan_hedge(&runtime.identity,runtime.execution.lifecycle(),view.revision,&spot,&futures,c){
        isolated_policy::HedgeDecision::Short{quantity,key}=>{
            gate(&runtime.execution,Some(quantity))?;
            let order=runtime.execution.ensure_isolated_intent(view.revision,key,
                paper_risk::FillLeg::Futures,quantity,&spot,&futures,c).map_err(offline_adapter_error)?;
            runtime.actions.push(json!({"action":"hedge_registered","order":order}));
        },
        other=>runtime.actions.push(json!({"action":"hedge_policy","decision":other})),
    }
    Ok(())
}
async fn dispatch_runtime_plan(plan:RuntimePlan)->Result<(),Box<dyn std::error::Error>>{
    match plan.mode{
        RuntimeMode::ExecutionIsolated{input,journal_root}=>run_execution_isolated_offline(input,journal_root).await,
        RuntimeMode::IntegratedPaper{manifest,journal_root}=>r1322_typed::run_integrated_r1325(manifest,journal_root).await,
        RuntimeMode::PublicObservation{manifest,journal_root}=>run_public_observation_r1301(manifest,journal_root).await,
        RuntimeMode::Legacy=>Err("legacy is dispatched by existing startup".into()),
    }
}
async fn run_execution_isolated_offline(input:PathBuf,journal_root:PathBuf)->Result<(),Box<dyn std::error::Error>>{
    if !runtime_conflicting_flags().is_empty(){return Err("legacy flags denied".into())}
    let input=input.canonicalize()?;
    let lab=["R1296","R1298"].iter().filter_map(|name|PathBuf::from(format!("/mnt/spotarb_db_test/r1273_r1275_engine_latency_20261111/{name}/lab")).canonicalize().ok())
        .find(|p|input.starts_with(p)).ok_or("input outside isolated lab")?;
    // Validate the existing parent before creating any directory.
    let parent=journal_root.parent().ok_or("journal parent")?.canonicalize()?;
    if !parent.starts_with(&lab){return Err("journal outside R1296 lab".into())}
    fs::create_dir_all(&journal_root)?;
    let journal_root=journal_root.canonicalize()?;
    if !journal_root.starts_with(&lab){return Err("journal symlink escape".into())}
    if fs::metadata(&input)?.len()>64*1024*1024{return Err("input exceeds bound".into())}
    let batch:OfflineMarketBatch=serde_json::from_slice(&fs::read(input)?)?;
    let journal=journal_root.join("paper-execution.jsonl");
    let mut runtime=new_isolated_runtime(&batch,&journal)?;
    for input in &batch.inputs{
        process_isolated_input(&mut runtime,input)?;
    }
    let before=serde_json::to_value(runtime.execution.projection())?;
    let fill_ids=runtime.execution.lifecycle().processed_events.keys().cloned().collect::<Vec<_>>();
    let actions=runtime.actions.clone();
    let identity=runtime.identity.clone();
    drop(runtime);
    let recovered=DurablePaper::recover_isolated(&journal,&identity).map_err(offline_adapter_error)?;
    if recovered.isolated_identity()!=Some(&identity)||fill_ids!=recovered.lifecycle().processed_events.keys().cloned().collect::<Vec<_>>() {
        return Err("recovery identity or event set mismatch".into())
    }
    let after=serde_json::to_value(recovered.projection())?;
    for key in ["spot_base","futures_base","futures_contracts","commissions_quote"]{
        if before[key]!=after[key]{return Err(format!("recovery quantity mismatch: {key}").into())}
    }
    println!("{}",serde_json::to_string(&json!({"run_id":identity.run_id,"actions":actions,
        "before_recovery":before,"after_recovery":after,"execution_ids":fill_ids,
        "orders":recovered.lifecycle().orders,"journal_sequence":recovered.sequence(),
        "generation":recovered.isolated_generation_summary(),"adapter_registry":recovered.adapter_registry(),
        "functional_validation":"NOT_ATTESTED_BY_COMPILATION"}))?);
    Ok(())
}

#[cfg(test)]
#[path="native_paper_isolated_r1296_tests.rs"]
mod r1296_entrypoint_tests;

fn process_isolated_input(runtime:&mut IsolatedPaperRuntime,input:&OfflineInput)->Result<(),Box<dyn std::error::Error>>{
    match input{OfflineInput::NewGeneration{context}=>runtime.execution.start_isolated_generation(context).map_err(offline_adapter_error)?,
            OfflineInput::ExitTrigger{context}=>runtime.execution.request_isolated_exit(context).map_err(offline_adapter_error)?,
            OfflineInput::Market{frame,context}=>process_validated_market_input(runtime,frame,context)?,
            OfflineInput::Control{control,context}=>runtime.execution.control_isolated(control.clone(),context).map_err(offline_adapter_error)?}
    Ok(())
}

#[cfg(test)]
#[path="native_paper_isolated_r1298_tests.rs"]
mod r1298_entrypoint_tests;

// R1325 reuses the accepted R1322 typed implementation; virtual clocks stay feature/test gated.
#[path="native_paper_typed_r1322.rs"]
mod r1322_typed;
