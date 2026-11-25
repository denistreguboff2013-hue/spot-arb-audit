//! Isolated native decision replay. No exchange credentials or order path.
use spotarb_native_market_core::{owner_index,parse,CoreError,HistorySink,Owner,RawFrame,Update,Venue};
use spotarb_native_market_core::decision::{DecisionCore,DecisionConfig,RouteSpec};
use spotarb_native_market_core::native_trading_config::NativeTradingConfig;
use spotarb_native_market_core::native_paper_config_bridge::bind_public_paper_decision_config;
use serde::Deserialize;
#[derive(Deserialize)]struct MetadataSnapshot{generation:u64,routes:HashMap<String,Vec<RouteSpec>>}
#[derive(Deserialize)]struct MetadataChange{
 at_frame:u64,generation:u64,routes:HashMap<String,Vec<RouteSpec>>,
}
use std::{collections::{HashMap,HashSet},io::{BufRead,BufReader,BufWriter,Write},time::Instant};
struct NoHistory;
impl HistorySink for NoHistory{
 fn accept(&mut self,_:&RawFrame,_:&Update)->Result<(),CoreError>{Ok(())}
}
fn cpu()->f64{
 let mut u:libc::rusage=unsafe{std::mem::zeroed()};
 unsafe{libc::getrusage(libc::RUSAGE_SELF,&mut u)};
 (u.ru_utime.tv_sec+u.ru_stime.tv_sec) as f64+
  (u.ru_utime.tv_usec+u.ru_stime.tv_usec) as f64/1e6
}
fn percentile(v:&mut [u64],p:f64)->u64{
 v.sort_unstable();v[((v.len()-1) as f64*p) as usize]
}
fn main()->Result<(),Box<dyn std::error::Error>>{
 let args:Vec<_>=std::env::args().collect();
 assert!(matches!(args.len(),4|5|7),
  "usage: native_decision_replay <frames.jsonl> <routes.json> <output.jsonl|none> [metadata_changes.json] [candidate.yaml ab_output.jsonl]");
 let snapshot:MetadataSnapshot=serde_json::from_slice(&std::fs::read(&args[2])?)?;
 let changes:Vec<MetadataChange>=if args.len()>=5{
  serde_json::from_slice(&std::fs::read(&args[4])?)?
 }else{Vec::new()};
 if changes.windows(2).any(|w|w[0].at_frame>=w[1].at_frame){
  return Err("metadata changes must have increasing frame indexes".into())
 }
 let cfg=DecisionConfig{max_age_ms:800,metadata_ttl_ms:43_200_000,base_size:10.,
  risk_cap:10.,cost:24.03,required_net:42.,min_price:0.,max_price:f64::INFINITY,
  blacklist:HashSet::new()};
 let mut owners:Vec<_>=(0..3).map(|i|Owner::new(i,3,800).unwrap()).collect();
 let mut cores:Vec<_>=(0..3).map(|i|DecisionCore::new(i,3,cfg.clone(),HashMap::new()).unwrap()).collect();
 for core in &mut cores{core.replace_routes(snapshot.generation,snapshot.routes.clone())
  .map_err(|e|format!("metadata snapshot: {e:?}"))?;}
 // R75 opt-in: the same parsed PUBLIC book is evaluated by independent
 // Rust DecisionCore policies. The YAML never grants exchange authority.
 let ab_config=if args.len()==7{
  let p=std::path::Path::new(&args[5]);
  let yaml=NativeTradingConfig::load(p)
   .map_err(|e|format!("R75 YAML load denied: {e:?}"))?;
  let policy=bind_public_paper_decision_config(&yaml)
   .map_err(|e|format!("R75 YAML policy denied: {e:?}"))?;
  Some((yaml.sha256,policy))
 }else{None};
 let mut ab_cores=if let Some((_,policy))=&ab_config{
  let mut candidate=Vec::new();
  for i in 0..3{
   let mut core=DecisionCore::new(i,3,policy.clone(),HashMap::new())
    .map_err(|e|format!("R75 candidate core: {e:?}"))?;
   core.replace_routes(snapshot.generation,snapshot.routes.clone())
    .map_err(|e|format!("R75 candidate metadata: {e:?}"))?;
   candidate.push(core);
  }
  Some(candidate)
 }else{None};
 let mut ab_output=if args.len()==7{
  Some(BufWriter::new(std::fs::File::create(&args[6])?))
 }else{None};
 let input=BufReader::new(std::fs::File::open(&args[1])?);
 let mut output=if args[3]=="none"{None}else{Some(BufWriter::new(std::fs::File::create(&args[3])?))};
 let mut history=NoHistory;
 let started=Instant::now();let cpu0=cpu();let mut count=0u64;let mut compared=0u64;
 let mut latency=Vec::with_capacity(100_000);let mut reasons=HashMap::<String,u64>::new();
 let mut ab_pairs=0u64;let mut ab_diff=0u64;
 let mut old_exec=0u64;let mut yaml_exec=0u64;
 let mut changed_reasons=HashMap::<String,u64>::new();
 let mut ab_yaml_reasons=HashMap::<String,u64>::new();
 let mut change_index=0usize;
 for line in input.lines(){
  if changes.get(change_index).is_some_and(|item|item.at_frame==count){
   let item=&changes[change_index];
   for core in &mut cores{core.replace_routes(item.generation,item.routes.clone())
    .map_err(|e|format!("metadata change {count}: {e:?}"))?;}
   if let Some(candidate)=ab_cores.as_mut(){
    for core in candidate{core.replace_routes(item.generation,item.routes.clone())
     .map_err(|e|format!("R75 metadata change {count}: {e:?}"))?;}
   }
   change_index+=1;
  }
  let line=line?;let frame:RawFrame=serde_json::from_str(&line)?;
  let update=parse(&frame).map_err(|e|format!("parse {count}: {e:?}"))?;
  let index=owner_index(&update.base,3);
  let base=update.base.clone();
  owners[index].ingest_parsed(&frame,update,&mut history)
   .map_err(|e|format!("ingest {count}: {e:?}"))?;
  let book=owners[index].snapshot_venue(&base,frame.venue)
   .ok_or("missing book after ingest")?;
  let exchange=match frame.venue{
   Venue::OkxSpot=>"okx_spot",Venue::BinanceFutures=>"binance_futures",
  };
  let tick=Instant::now();
  let candidate_book=if ab_cores.is_some(){Some(book.clone())}else{None};
  let result=cores[index].update(&base,exchange,book)
   .map_err(|e|format!("decision {count}: {e:?}"))?;
  latency.push(tick.elapsed().as_nanos() as u64);
  for decision in &result{*reasons.entry(decision.reason.clone()).or_default()+=1;compared+=1}
  if let (Some(cores),Some(candidate_book))=(ab_cores.as_mut(),candidate_book){
   let candidate=cores[index].update(&base,exchange,candidate_book)
    .map_err(|e|format!("R75 candidate frame {count}: {e:?}"))?;
   if candidate.len()!=result.len(){return Err(format!(
    "R75 route cardinality mismatch at frame {count}").into())}
   for (old,new) in result.iter().zip(&candidate){
    if old.base!=new.base||old.route!=new.route{
     return Err(format!("R75 route identity mismatch at frame {count}").into())
    }
    ab_pairs+=1;
    *ab_yaml_reasons.entry(new.reason.clone()).or_default()+=1;
    if old.reason=="EXECUTABLE_OPPORTUNITY"{old_exec+=1}
    if new.reason=="EXECUTABLE_OPPORTUNITY"{yaml_exec+=1}
    if old.reason!=new.reason{
     ab_diff+=1;
     *changed_reasons.entry(format!("{} -> {}",old.reason,new.reason)).or_default()+=1;
    }
   }
   if let Some(file)=ab_output.as_mut(){
    serde_json::to_writer(&mut *file,&serde_json::json!({
     "frame_index":count,"base":base,"fixed":result,"yaml":candidate
    }))?;
    file.write_all(b"\n")?;
   }
  }
  if let Some(ref mut file)=output{
   if result.len()==1{serde_json::to_writer(&mut *file,&result[0])?}
   else{serde_json::to_writer(&mut *file,&result)?}
   file.write_all(b"\n")?;
  }
  count+=1;
 }
 if let Some(ref mut file)=output{file.flush()?}
 if let Some(ref mut file)=ab_output{file.flush()?}
 if change_index!=changes.len(){return Err("metadata change beyond replay".into())}
 if ab_config.is_some()&&compared!=ab_pairs{
  return Err("R75 A/B missing aligned decision pairs".into())
 }
 let ab_report=ab_config.as_ref().map(|(sha,policy)|serde_json::json!({
  "status":"OFFLINE_SAME_FRAME_DUAL_RUST_DECISION_CORE",
  "candidate_config_sha256":sha,
  "fixed_policy":"R21_COMPILED_FALLBACK",
  "candidate_policy":"R64_OPERATOR_YAML_READONLY",
  "candidate_max_age_ms":policy.max_age_ms,
  "candidate_min_price":policy.min_price,
  "candidate_max_price":policy.max_price,
  "candidate_blacklist_len":policy.blacklist.len(),
  "aligned_decision_pairs":ab_pairs,
  "different_reason_pairs":ab_diff,
  "fixed_executable_signals":old_exec,
  "yaml_executable_signals":yaml_exec,
  "reason_changes":changed_reasons,
  "yaml_reasons":ab_yaml_reasons,
  "output":args[6],
  "exchange_orders_sent":false,"authenticated_fills":false,
  "production_cutover_allowed":false
 }));
 let total=started.elapsed().as_secs_f64();let process_cpu=cpu()-cpu0;
 let report=serde_json::json!({"frames":count,"decisions":compared,
  "metadata_changes":change_index,"wall_s":total,
  "cpu_s":process_cpu,"cpu_us_per_frame":process_cpu*1e6/count as f64,
  "throughput_wall":count as f64/total,"reasons":reasons,
  "decision_ns":{"p50":percentile(&mut latency.clone(),0.5),
   "p95":percentile(&mut latency.clone(),0.95),
   "p99":percentile(&mut latency.clone(),0.99),
   "max":latency.iter().max()},"output":args[3],
  "r75_dual_policy_ab":ab_report});
 eprintln!("{}",report);
 Ok(())
}
