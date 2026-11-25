// R1328 isolated, read-only public REST two-phase experiment. No trading APIs.
use serde_json::{json,Value};
use std::{collections::{HashMap,HashSet},process::Command,time::{Duration,Instant,SystemTime,UNIX_EPOCH}};
fn ms()->u128{SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis()}
fn get(url:&str)->Result<(Value,u128),String>{
 let t=Instant::now();
 let out=Command::new("curl").args(["-fsS","--max-time","8","--connect-timeout","3",url]).output().map_err(|e|e.to_string())?;
 let elapsed=t.elapsed().as_millis();
 if !out.status.success(){return Err(format!("curl exit {}",out.status))}
 let v=serde_json::from_slice(&out.stdout).map_err(|e|e.to_string())?;
 Ok((v,elapsed))
}
fn price(v:&Value,k:&str)->Option<f64>{v[k].as_str()?.parse::<f64>().ok().filter(|n|n.is_finite()&&*n>0.)}
fn binance(v:&Value)->HashMap<String,(f64,f64)>{
 let mut m=HashMap::new();
 if let Some(a)=v.as_array(){for x in a{
  if let (Some(s),Some(b),Some(a))=(x["symbol"].as_str(),price(x,"bidPrice"),price(x,"askPrice")){
   if s.ends_with("USDT")&&b<=a{m.insert(s.trim_end_matches("USDT").into(),(b,a));}
  }
 }}m
}
fn okx(v:&Value)->HashMap<String,(f64,f64)>{
 let mut m=HashMap::new();
 if let Some(a)=v["data"].as_array(){for x in a{
  if let (Some(s),Some(b),Some(a))=(x["instId"].as_str(),price(x,"bidPx"),price(x,"askPx")){
   if let Some(base)=s.strip_suffix("-USDT"){if b<=a{m.insert(base.into(),(b,a));}}
  }
 }}m
}
fn best(v:&Value,venue:&str)->Option<(f64,f64)>{
 let (b,a)=if venue=="bn"{
  let bids=v["bids"].as_array()?;let asks=v["asks"].as_array()?;
  (bids.first()?,asks.first()?)
 }else{
  let row=v["data"].as_array()?.first()?;
  (row["bids"].as_array()?.first()?,row["asks"].as_array()?.first()?)
 };
 let bid=b[0].as_str()?.parse::<f64>().ok()?;
 let ask=a[0].as_str()?.parse::<f64>().ok()?;
 if bid>0.&&ask>=bid{Some((bid,ask))}else{None}
}
fn main(){
 let seconds=std::env::args().nth(1).and_then(|x|x.parse::<u64>().ok()).unwrap_or(30).clamp(5,3600);
 let threshold=std::env::var("R1328_GROSS_BPS").ok().and_then(|x|x.parse::<f64>().ok()).unwrap_or(10.).clamp(0.,1000.);
 let max_depth=std::env::var("R1328_MAX_DEPTH").ok().and_then(|x|x.parse::<usize>().ok()).unwrap_or(8).clamp(1,100);
 let started=Instant::now();let mut cycles=0u64;let mut candidates=0u64;let mut errors=0u64;
 let mut probed=HashSet::new();
 println!("{}",json!({"kind":"start","utc_ms":ms(),"seconds":seconds,"gross_threshold_bps":threshold,"max_unique_depth_probes":max_depth,"mode":"PUBLIC_REST_READ_ONLY","orders":false}));
 while started.elapsed()<Duration::from_secs(seconds){
  let t=Instant::now();
  let bn=std::thread::spawn(||get("https://fapi.binance.com/fapi/v1/ticker/bookTicker"));
  let ok=get("https://www.okx.com/api/v5/market/tickers?instType=SPOT");
  let bn=bn.join().unwrap_or_else(|_|Err("binance thread panic".into()));
  match (bn,ok){
   (Ok((b,bms)),Ok((o,oms)))=>{
    let b=binance(&b);let o=okx(&o);let common=b.keys().filter(|s|o.contains_key(*s)).count();
    let mut ranked=Vec::new();
    for (s,(bb,ba)) in &b{if let Some((ob,oa))=o.get(s){
     // Long OKX spot / short Binance futures: buy OKX ask, sell Binance bid.
     let gross=(bb/oa-1.)*10000.;
     if gross>=threshold{ranked.push((s.clone(),gross,*bb,*oa,*ba,*ob));}
    }}
    ranked.sort_by(|a,b|b.1.total_cmp(&a.1));
    candidates+=ranked.len() as u64;cycles+=1;
    println!("{}",json!({"kind":"scan","utc_ms":ms(),"cycle":cycles,"binance_symbols":b.len(),"okx_symbols":o.len(),"intersection":common,"gross_candidates":ranked.len(),"bn_rest_ms":bms,"okx_rest_ms":oms,"scan_wall_ms":t.elapsed().as_millis()}));
    for (s,gross,_,_,_,_) in ranked.into_iter().take(3){
     if probed.len()>=max_depth||!probed.insert(s.clone()){continue}
     let start=Instant::now();
     let bs=format!("https://fapi.binance.com/fapi/v1/depth?symbol={}USDT&limit=20",s);
     let os=format!("https://www.okx.com/api/v5/market/books?instId={}-USDT&sz=20",s);
     let bt=std::thread::spawn(move||get(&bs));let ot=get(&os);
     let bt=bt.join().unwrap_or_else(|_|Err("depth thread panic".into()));
     let (mut gross_depth,mut bms,mut oms)=(None,None,None);
     if let (Ok((bv,bl)),Ok((ov,ol)))=(&bt,&ot){
      bms=Some(*bl);oms=Some(*ol);
      if let (Some((bid,_)),Some((_,ask)))=(best(bv,"bn"),best(ov,"okx")){gross_depth=Some((bid/ask-1.)*10000.);}
     }else{errors+=1}
     println!("{}",json!({"kind":"depth","utc_ms":ms(),"symbol":s,"phase1_gross_bps":gross,"phase2_gross_bps":gross_depth,"depth_wall_ms":start.elapsed().as_millis(),"bn_depth_ms":bms,"okx_depth_ms":oms,"bn_ok":bt.is_ok(),"okx_ok":ot.is_ok(),"note":"gross only; no fees, slippage, size, freshness or trading authorization"}));
    }
   }
   (b,o)=>{errors+=1;println!("{}",json!({"kind":"error","utc_ms":ms(),"binance":b.err(),"okx":o.err()}));}
  }
  std::thread::sleep(Duration::from_millis(1500).saturating_sub(t.elapsed()));
 }
 println!("{}",json!({"kind":"summary","cycles":cycles,"gross_candidates":candidates,"unique_depth_probes":probed.len(),"errors":errors,"elapsed_ms":started.elapsed().as_millis(),"trading":"disabled"}));
}
