//! Isolated OS-process metadata barrier fixture. JSON is control/test IPC only.
use serde_json::{json,Value};
use spotarb_native_market_core::{Book,owner_index};
use spotarb_native_market_core::decision::{DecisionCore,DecisionConfig,RouteSpec};
use std::{collections::{HashMap,HashSet},io::{self,BufRead,Write}};

fn main()->Result<(),Box<dyn std::error::Error>>{
 let args:Vec<String>=std::env::args().collect();
 assert_eq!(args.len(),3,"worker <owner-index> <base>");
 let index:usize=args[1].parse()?;let base=args[2].clone();
 assert_eq!(owner_index(&base,3),index);
 let cfg=DecisionConfig{max_age_ms:800,metadata_ttl_ms:43_200_000,base_size:10.,
  risk_cap:10.,cost:24.03,required_net:42.,min_price:0.,max_price:f64::INFINITY,
  blacklist:HashSet::new()};
 let mut core=DecisionCore::new(index,3,cfg,HashMap::new())
  .map_err(|e|format!("{e:?}"))?;
 let stdin=io::stdin();let mut stdout=io::stdout().lock();
 for line in stdin.lock().lines(){
  let line=line?;let value:Value=serde_json::from_str(&line)?;
  let generation=value["generation"].as_u64().unwrap_or(0);
  let op=value["op"].as_str().unwrap_or("");
  let answer:Result<Option<String>,String>=match op{
   "init"|"stage"=>{
    let routes:Result<HashMap<String,Vec<RouteSpec>>,_>=
     serde_json::from_value(value["routes"].clone());
    routes.map_err(|e|e.to_string()).and_then(|routes|{
     let outcome=if op=="init"{
      if core.metadata_generation()!=0{Err(spotarb_native_market_core::CoreError::OutOfOrder)}
      else{core.replace_routes(generation,routes)}
     }else{core.stage_routes(generation,routes)};
     outcome.map_err(|e|format!("{e:?}"))?;
     if op=="init"{core.pause_decisions()}
     Ok(None)
    })
   }
   "pause"=>{core.pause_decisions();Ok(None)}
   "commit"=>core.commit_staged(generation).map(|_|None).map_err(|e|format!("{e:?}")),
   "resume"=>core.resume_decisions(generation).map(|_|None).map_err(|e|format!("{e:?}")),
   "abort"=>{core.abort_staged();Ok(None)}
   "eval"=>{
    let book:Result<Book,_>=serde_json::from_value(value["book"].clone());
    book.map_err(|e|e.to_string()).and_then(|book|
     core.update(&base,value["exchange"].as_str().unwrap_or(""),book)
      .map(|rows|rows.first().map(|d|d.reason.clone()))
      .map_err(|e|format!("{e:?}")))
   }
   _=>Err("invalid command".into()),
  };
  let response=match answer{
   Ok(reason)=>json!({"ok":true,"reason":reason,"generation":core.metadata_generation(),
                       "paused":core.decisions_paused(),"owner":index}),
   Err(error)=>json!({"ok":false,"error":error,"generation":core.metadata_generation(),
                       "paused":core.decisions_paused(),"owner":index}),
  };
  writeln!(stdout,"{}",response)?;stdout.flush()?;
 }
 Ok(())
}
