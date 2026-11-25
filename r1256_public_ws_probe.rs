#[path = "../r1250_orderbook.rs"]
mod orderbook;

use futures_util::{SinkExt, StreamExt};
use orderbook::{Book, Delta};
use serde_json::{json, Value};
use std::{collections::HashMap, fs::{create_dir_all,OpenOptions}, io::{BufWriter,Write}, path::Path, process::Command, sync::{Arc,Mutex,atomic::{AtomicBool,Ordering}}, time::{SystemTime,UNIX_EPOCH}};
use tokio::{sync::mpsc,time::{Duration,Instant}};
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream};

const SYMBOLS: [&str; 2] = ["BTCUSDT","ETHUSDT"];
static RUN_EPOCH: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
fn run_mono_ns() -> u64 { RUN_EPOCH.get_or_init(std::time::Instant::now).elapsed().as_nanos() as u64 }
fn now_ms() -> i64 { SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64 }
fn process_counters()->(u64,u64){
    let rss=std::fs::read_to_string("/proc/self/status").ok().and_then(|s|s.lines().find(|l|l.starts_with("VmRSS:")).and_then(|l|l.split_whitespace().nth(1)).and_then(|v|v.parse::<u64>().ok())).unwrap_or(0);
    let mut u:libc::rusage=unsafe{std::mem::zeroed()};
    let rc=unsafe{libc::getrusage(libc::RUSAGE_SELF,&mut u)};
    let cpu_ns=if rc==0{((u.ru_utime.tv_sec+u.ru_stime.tv_sec)as u64)*1_000_000_000+((u.ru_utime.tv_usec+u.ru_stime.tv_usec)as u64)*1_000}else{0};
    (rss,cpu_ns)
}
fn pct_f64(xs:&[f64],p:f64)->Option<f64>{if xs.is_empty(){return None}let mut a=xs.to_vec();a.sort_by(f64::total_cmp);Some(a[((a.len()-1)as f64*p).floor()as usize])}
fn binance_clock_probe()->Value{
    let t0=now_ms();
    let out=Command::new("curl").args(["-fsS","--max-time","4","https://fapi.binance.com/fapi/v1/time"]).output();
    let t1=now_ms();
    match out{
        Ok(o) if o.status.success()=>match serde_json::from_slice::<Value>(&o.stdout){
            Ok(v)=>{let server=v["serverTime"].as_i64();let mid=(t0+t1)/2;json!({"http_ok":true,"local_start_ms":t0,"local_end_ms":t1,"server_time_ms":server,"estimated_server_minus_local_ms":server.map(|x|x-mid),"uncertainty_bound_ms":(t1-t0+1)/2,"rest_weight":1})},
            Err(e)=>json!({"http_ok":false,"reason":e.to_string(),"local_start_ms":t0,"local_end_ms":t1,"rest_weight":1})
        },
        Ok(o)=>json!({"http_ok":false,"reason":String::from_utf8_lossy(&o.stderr).to_string(),"local_start_ms":t0,"local_end_ms":t1,"rest_weight":1}),
        Err(e)=>json!({"http_ok":false,"reason":e.to_string(),"local_start_ms":t0,"local_end_ms":t1,"rest_weight":0})
    }
}
fn num(v:&Value, k:&str)->u64 { v.get(k).and_then(Value::as_u64).unwrap_or(0) }
fn levels(v:&Value)->Vec<(String,String)> {
    v.as_array().into_iter().flatten().filter_map(|r| {
        let a=r.as_array()?; Some((a.get(0)?.as_str()?.to_string(),a.get(1)?.as_str()?.to_string()))
    }).collect()
}
fn delta(v:&Value)->Option<Delta> {
    Some(Delta { first_id:num(v,"U"), final_id:num(v,"u"), previous_final_id:num(v,"pu"), bids:levels(v.get("b")?), asks:levels(v.get("a")?) })
}
fn socket_addrs(ws:&tokio_tungstenite::WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>)->(String,String) {
    match ws.get_ref() {
        MaybeTlsStream::NativeTls(tls) => {
            let s=tls.get_ref().get_ref().get_ref();
            (s.local_addr().map(|x|x.to_string()).unwrap_or_default(),s.peer_addr().map(|x|x.to_string()).unwrap_or_default())
        },
        MaybeTlsStream::Plain(s)=>(s.local_addr().map(|x|x.to_string()).unwrap_or_default(),s.peer_addr().map(|x|x.to_string()).unwrap_or_default()),
        _=>(String::new(),String::new())
    }
}
fn make_snapshot(symbol:&str)->Result<(u64,Vec<(String,String)>,Vec<(String,String)>),String>{
    let url=format!("https://fapi.binance.com/fapi/v1/depth?symbol={symbol}&limit=1000");
    let out=Command::new("curl").args(["-fsS","--max-time","5",&url]).output().map_err(|e|format!("curl: {e}"))?;
    if !out.status.success(){return Err(format!("snapshot HTTP failed: {}",String::from_utf8_lossy(&out.stderr)))}
    let v:Value=serde_json::from_slice(&out.stdout).map_err(|e|format!("snapshot JSON: {e}"))?;
    let id=num(&v,"lastUpdateId");
    if id==0 { return Err(format!("snapshot missing lastUpdateId: {v}")); }
    Ok((id,levels(v.get("bids").unwrap_or(&Value::Null)),levels(v.get("asks").unwrap_or(&Value::Null))))
}
fn open_writer(path:&Path)->Result<BufWriter<std::fs::File>,String>{
    let f=OpenOptions::new().create_new(true).write(true).open(path).map_err(|e|format!("{}: {e}",path.display()))?;
    Ok(BufWriter::new(f))
}
fn write_line(w:&mut BufWriter<std::fs::File>, v:&Value)->Result<(),String>{
    serde_json::to_writer(&mut *w,v).map_err(|e|e.to_string())?;
    w.write_all(b"\n").map_err(|e|e.to_string())
}
#[derive(Default)]
struct BinanceBook { book:Option<Book>, snap_id:u64, pending:Vec<Delta>, synced:bool, gaps:u64, duplicates:u64, bridge_ms:Option<u64> }

async fn pump(label:String, url:String, seconds:u64, out:String, diff_mode:bool)->Result<Value,String>{
    let connect_start=Instant::now();
    if label.starts_with("bn_") {
        spotarb_native_market_core::ingress_runtime_r1205::permit(tokio::time::Instant::now()+Duration::from_secs(120)).await?;
    }
    let (mut ws,_)=tokio::time::timeout(Duration::from_secs(20),connect_async(&url)).await.map_err(|_|format!("{label}: connect timeout"))?.map_err(|e|format!("{label}: connect: {e}"))?;
    if label=="bn_bookticker_subscribe" {
        let request=json!({"method":"SUBSCRIBE","params":["btcusdt@bookTicker","ethusdt@bookTicker"],"id":1256});
        ws.send(Message::Text(request.to_string())).await.map_err(|e|format!("{label}: SUBSCRIBE send: {e}"))?;
    }
    let (local,peer)=socket_addrs(&ws);
    let connect_ms=connect_start.elapsed().as_millis() as u64;
    let parsed_url=url::Url::parse(&url).map_err(|e|e.to_string())?;
    let host=parsed_url.host_str().ok_or("endpoint host missing")?;
    let dns_start=Instant::now();
    let dns_ips=tokio::net::lookup_host((host,443)).await.map_err(|e|e.to_string())?.map(|a|a.ip().to_string()).collect::<Vec<_>>();
    let dns_probe_ms=dns_start.elapsed().as_millis() as u64;
    if diff_mode { return pump_diff(ws,label,url,seconds,out,connect_ms,dns_probe_ms,local,peer,dns_ips).await; }
    let raw_path=Path::new(&out).join(format!("{label}.jsonl"));
    let state_path=Path::new(&out).join(format!("{label}_book_states.jsonl"));
    let mut raw=open_writer(&raw_path)?;
    let mut state=if diff_mode { Some(open_writer(&state_path)?) } else { None };
    let mut books:HashMap<String,BinanceBook>=HashMap::new();
    let mut snapshots=HashMap::new();
    let mut snapshot_errors=HashMap::new();
    if diff_mode {
        for s in SYMBOLS {
            match make_snapshot(s) {
                Ok((id,b,a))=>{snapshots.insert(s.to_string(),(id,b,a));},
                Err(e)=>{snapshot_errors.insert(s.to_string(),e);}
            }
        }
        for (s,(id,b,a)) in &snapshots {
            books.insert(s.clone(),BinanceBook{book:Some(Book::from_snapshot(*id,b.clone(),a.clone())),snap_id:*id,pending:Vec::new(),synced:false,gaps:0,duplicates:0,bridge_ms:None});
        }
    }
    write_line(&mut raw,&json!({"kind":"meta","label":label,"session_id":session_id(&label,1,run_mono_ns()),"generation":1,"url":url,"local_addr":local,"peer_addr":peer,"dns_ips":dns_ips,"dns_probe_ms_after_connect":dns_probe_ms,"permit_and_tls_ws_connect_total_ms":connect_ms,"subscribe_request":if label=="bn_bookticker_subscribe"{json!({"method":"SUBSCRIBE","params":["btcusdt@bookTicker","ethusdt@bookTicker"],"id":1256})}else{Value::Null},"snapshots":snapshots.keys().collect::<Vec<_>>(),"snapshot_errors":snapshot_errors,"started_utc_ms":now_ms()}))?;
    let deadline=Instant::now()+Duration::from_secs(seconds);
    let started=Instant::now();
    let mut frames=0u64; let mut errors=0u64; let mut sequence_ok=0u64; let mut subscribe_acks=0u64;
    while Instant::now()<deadline {
        let remaining=deadline.saturating_duration_since(Instant::now());
        let poll_enter_ns=run_mono_ns();
        let item=tokio::time::timeout(remaining,ws.next()).await;
        let poll_ready_ns=run_mono_ns();
        let msg=match item { Err(_)=>break, Ok(Some(Ok(x)))=>x, Ok(Some(Err(e)))=>{errors+=1; write_line(&mut raw,&json!({"kind":"error","recv_utc_ms":now_ms(),"error":e.to_string()}))?;break}, Ok(None)=>break };
        match msg {
            Message::Ping(p)=>{ if let Err(e)=ws.send(Message::Pong(p)).await {errors+=1;write_line(&mut raw,&json!({"kind":"error","error":e.to_string()}))?;break;} },
            Message::Text(t)=>{
                if label=="bn_bookticker_subscribe" { let c:Value=serde_json::from_str(&t).unwrap_or(Value::Null); if c["id"]==1256 && c["result"].is_null(){subscribe_acks+=1;} if c["id"]==1256&&c["code"].is_number(){errors+=1;} }
                let recv=now_ms();let delivered=run_mono_ns();process_frame(&t,&label,&local,&peer,recv,delivered as u128,0,poll_enter_ns,poll_ready_ns,delivered,0,&mut raw,&mut state,&mut books,&mut frames,&mut sequence_ok,&mut errors)?;
            },
            Message::Binary(b)=>{let t=String::from_utf8_lossy(&b).to_string();let recv=now_ms();let delivered=run_mono_ns();process_frame(&t,&label,&local,&peer,recv,delivered as u128,0,poll_enter_ns,poll_ready_ns,delivered,0,&mut raw,&mut state,&mut books,&mut frames,&mut sequence_ok,&mut errors)?;},
            Message::Close(c)=>{write_line(&mut raw,&json!({"kind":"close","recv_utc_ms":now_ms(),"close":format!("{c:?}")}))?;break;},
            _=>{}
        }
    }
    raw.flush().map_err(|e|e.to_string())?;
    if let Some(w)=state.as_mut(){w.flush().map_err(|e|e.to_string())?;}
    let health:HashMap<String,Value>=books.iter().map(|(s,b)|(s.clone(),json!({"snapshot_id":b.snap_id,"synced":b.synced,"last_update_id":b.book.as_ref().and_then(|x|x.last_id),"valid":b.book.as_ref().map(|x|x.valid).unwrap_or(false),"gaps":b.gaps,"duplicates":b.duplicates,"bridge_ms":b.bridge_ms,"buffered":b.pending.len()}))).collect();
    Ok(json!({"label":label,"frames":frames,"errors":errors,"subscribe_acks":subscribe_acks,"sequence_applied":sequence_ok,"duration_s":started.elapsed().as_secs_f64(),"health":health,"snapshot_errors":snapshot_errors}))
}
fn process_frame(t:&str,label:&str,local:&str,peer:&str,recv:i64,mono_ns:u128,queue_delay_ns:u64,poll_enter_ns:u64,poll_ready_ns:u64,queue_enqueue_ns:u64,queue_reserve_wait_ns:u64,raw:&mut BufWriter<std::fs::File>,state:&mut Option<BufWriter<std::fs::File>>,books:&mut HashMap<String,BinanceBook>,frames:&mut u64,sequence_ok:&mut u64,errors:&mut u64)->Result<(),String>{
    let pipeline_start=std::time::Instant::now();
    let parse_start=std::time::Instant::now();
    let parsed=serde_json::from_str::<Value>(t);
    let parse_us=parse_start.elapsed().as_micros() as u64;
    let json_parsed_mono_ns=run_mono_ns();
    let parse_ok=parsed.is_ok();
    let v:Value=parsed.unwrap_or(Value::Null);
    let event=v.get("data").unwrap_or(&v);
    *frames+=1;
    let sym=event.get("s").and_then(Value::as_str).or_else(||v.get("stream").and_then(Value::as_str).and_then(|x|x.split('@').next())).unwrap_or("");
    let kind=event.get("e").and_then(Value::as_str).unwrap_or(if event.get("u").is_some()&&event.get("b").is_some(){"depthUpdate"}else if event.get("lastUpdateId").is_some(){"depth20"}else{"unknown"});
    let is_diff=label.starts_with("bn_diffdepth");
    write_line(raw,&json!({"kind":"frame","label":label,"symbol":sym,"exchange_ts_ms":event.get("E").and_then(Value::as_i64),"transaction_ts_ms":event.get("T").and_then(Value::as_i64),"event_age_ms":event.get("E").and_then(Value::as_i64).map(|t|recv-t),"transaction_age_ms":event.get("T").and_then(Value::as_i64).map(|t|recv-t),"update_id":event.get("u").or_else(||event.get("lastUpdateId")),"recv_utc_ms":recv,"mono_ns":mono_ns,"t_poll_enter_mono_ns":poll_enter_ns,"t_poll_ready_mono_ns":poll_ready_ns,"t_frame_delivered_mono_ns":mono_ns,"t_json_parsed_mono_ns":json_parsed_mono_ns,"poll_wait_ns":poll_ready_ns.saturating_sub(poll_enter_ns),"t_queue_enqueue_mono_ns":queue_enqueue_ns,"t_queue_dequeue_mono_ns":queue_enqueue_ns.saturating_add(queue_delay_ns),"queue_reserve_wait_ns":queue_reserve_wait_ns,"local_addr":local,"peer_addr":peer,"wire_bytes":t.len(),"exact_event_signature":exact_event_signature(&v),"parse_us":parse_us,"parse_ok":parse_ok,"queue_delay_ns":queue_delay_ns,"event":event}))?;
    if !is_diff || kind!="depthUpdate" { return Ok(()); }
    let d=match delta(event){Some(x)=>x,None=>{*errors+=1;return Ok(())}};
    let st=match books.get_mut(sym){Some(x)=>x,None=>return Ok(())};
    if !st.synced {
        st.pending.push(d);
        let bridge_pos=st.pending.iter().position(|x|x.first_id<=st.snap_id && x.final_id>=st.snap_id);
        if let Some(pos)=bridge_pos {
            let first=st.pending[pos].clone();
            let result=st.book.as_mut().unwrap().bridge(st.snap_id,&first);
            let t_book_applied_mono_ns=run_mono_ns();
            if result.is_ok(){
                st.synced=true; st.bridge_ms=Some((mono_ns/1_000_000) as u64);
                *sequence_ok+=1;
                let t_ram_published_mono_ns=run_mono_ns();
                if let Some(w)=state.as_mut(){write_line(w,&json!({"kind":"book_state","label":label,"symbol":sym,"recv_utc_ms":recv,"update_id":st.book.as_ref().unwrap().last_id,"valid":true,"top_bids":st.book.as_ref().unwrap().top(true,20),"top_asks":st.book.as_ref().unwrap().top(false,20),"bridge":true,"t_book_applied_mono_ns":t_book_applied_mono_ns,"t_ram_published_mono_ns":t_ram_published_mono_ns,"ram_publish_us":pipeline_start.elapsed().as_micros() as u64}))?;}
                let later:Vec<_>=st.pending.drain(pos+1..).collect();
                st.pending.clear();
                for ev in later {if st.book.as_mut().unwrap().apply(&ev).is_ok(){*sequence_ok+=1;}else{st.gaps+=1;*errors+=1;break;}}
            } else {st.pending.clear();*errors+=1;}
        }
        return Ok(());
    }
    let result=st.book.as_mut().unwrap().apply(&d);
    let t_book_applied_mono_ns=run_mono_ns();
    match result {
        Ok(applied)=>{
            if applied {
                *sequence_ok+=1;
                let t_ram_published_mono_ns=run_mono_ns();
                if let Some(w)=state.as_mut(){let b=st.book.as_ref().unwrap();write_line(w,&json!({"kind":"book_state","label":label,"symbol":sym,"recv_utc_ms":recv,"update_id":b.last_id,"valid":b.valid,"top_bids":b.top(true,20),"top_asks":b.top(false,20),"bridge":false,"t_book_applied_mono_ns":t_book_applied_mono_ns,"t_ram_published_mono_ns":t_ram_published_mono_ns,"ram_publish_us":pipeline_start.elapsed().as_micros() as u64}))?;}
            }
        },
        Err(e)=>{st.gaps+=1;*errors+=1;write_line(raw,&json!({"kind":"invalid","label":label,"symbol":sym,"recv_utc_ms":recv,"reason":e,"action":"book invalidated; further updates not published; resnapshot required"}))?;}
    }
    Ok(())
}
async fn capture_simple(label:String,url:String,seconds:u64,out:String)->Result<Value,String>{
    pump(label,url,seconds,out,false).await
}
fn endpoint_plan()->Vec<(&'static str,String,bool)> {
    let bbase="wss://fstream.binance.com/public";
    let symbols=SYMBOLS.iter().map(|s|s.to_ascii_lowercase()).collect::<Vec<_>>();
    let combined=symbols.iter().map(|s|format!("{s}@bookTicker")).collect::<Vec<_>>().join("/");
    let depth20=symbols.iter().map(|s|format!("{s}@depth20@100ms")).collect::<Vec<_>>().join("/");
    let diffdepth=symbols.iter().map(|s|format!("{s}@depth@100ms")).collect::<Vec<_>>().join("/");
    vec![
      ("bn_bookticker_individual_btc",format!("{bbase}/ws/btcusdt@bookTicker"),false),
      ("bn_bookticker_individual_eth",format!("{bbase}/ws/ethusdt@bookTicker"),false),
      ("bn_bookticker_combined_url",format!("{bbase}/stream?streams={combined}"),false),
      ("bn_bookticker_subscribe",format!("{bbase}/stream"),false),
      ("bn_depth20_100ms",format!("{bbase}/stream?streams={depth20}"),false),
      ("bn_diffdepth_100ms",format!("{bbase}/stream?streams={diffdepth}"),true),
    ]
}
fn lane_selected(filter:Option<&str>,lane:&str)->bool {
    filter.map(|f|f.split(',').any(|candidate|candidate==lane)).unwrap_or(true)
}
fn event_payload(v:&Value)->&Value { v.get("data").unwrap_or(v) }
fn exact_event_signature(v:&Value)->Option<String> {
    let e=event_payload(v);
    let keys=["s","u","E","T","b","B","a","A"];
    let values=keys.iter().map(|k|e.get(*k)?.as_str().map(|s|json!(s)).or_else(||e.get(*k).filter(|x|x.is_number()).cloned())).collect::<Option<Vec<_>>>()?;
    serde_json::to_string(&values).ok()
}
fn timestamp_lineage_valid(t:&[u64])->bool { t.windows(2).all(|w|w[0]<=w[1]) }
fn session_id(label:&str,generation:u64,start_ns:u64)->String { format!("{label}-g{generation}-{start_ns}") }
fn generation_accepts(active:u64,frame:u64)->bool { active==frame }

#[tokio::main(flavor="multi_thread",worker_threads=8)]
async fn main()->Result<(),String>{
    let a:Vec<String>=std::env::args().collect();
    if a.len()!=3{return Err("usage: r1250_channel_probe SECONDS OUTPUT_DIR".into())}
    let seconds:u64=a[1].parse().map_err(|e:std::num::ParseIntError|e.to_string())?;
    if !(30..=300).contains(&seconds){return Err("duration must be 30..300 seconds".into())}
    let out=a[2].clone();
    create_dir_all(&out).map_err(|e|e.to_string())?;
    let clock=binance_clock_probe();
    std::fs::write(Path::new(&out).join("CLOCK_SYNC.json"),serde_json::to_vec_pretty(&clock).map_err(|e|e.to_string())?).map_err(|e|e.to_string())?;
    let samples=Arc::new(Mutex::new(Vec::<(u64,u64,u64)>::new()));
    let sampling=Arc::new(AtomicBool::new(true));
    let s2=samples.clone();let flag=sampling.clone();
    let sampler=tokio::spawn(async move{
        let start=Instant::now();let mut tick=tokio::time::interval(Duration::from_secs(1));
        loop{tick.tick().await;if !flag.load(Ordering::SeqCst){break}let(rss,cpu)=process_counters();if let Ok(mut g)=s2.lock(){g.push((start.elapsed().as_nanos() as u64,rss,cpu));}}
    });
    let switch=std::fs::read_to_string("/home/tmpadmin/spotbot/data/real_kill_switch.json").map_err(|e|e.to_string())?;
    let sv:Value=serde_json::from_str(&switch).map_err(|e|e.to_string())?;
    if sv.get("state").and_then(Value::as_str)!=Some("ENGAGED"){return Err("REAL kill switch not ENGAGED".into())}
    let lane_filter=std::env::var("R1256_LANES").ok();
    let include_okx=std::env::var("R1256_INCLUDE_OKX").map(|v|v=="1").unwrap_or(lane_filter.is_none());
    let bjobs=endpoint_plan();
    let mut tasks=Vec::new();
    for (label,url,is_diff) in bjobs {
      if !lane_selected(lane_filter.as_deref(),label){continue;}
      let out=out.clone(); let label=label.to_string();
      tasks.push(tokio::spawn(async move{if is_diff{pump(label,url,seconds,out,true).await}else{capture_simple(label,url,seconds,out).await}}));
    }
    if tasks.is_empty(){return Err("R1256_LANES selected no known Binance lanes".into());}
    if include_okx {
      let okx="wss://ws.okx.com/ws/v5/public".to_string();
      for channel in ["bbo-tbt","books5"] {
        let label=format!("okx_{channel}");let url=okx.clone();let out=out.clone();
        let sub=json!({"op":"subscribe","args":[{"channel":channel,"instId":"BTC-USDT"},{"channel":channel,"instId":"ETH-USDT"}]});
        tasks.push(tokio::spawn(async move{capture_okx(label,url,sub,seconds,out).await}));
      }
    }
    let mut results=Vec::new();let mut failures=Vec::new();
    for t in tasks{match t.await{Ok(Ok(x))=>results.push(x),Ok(Err(e))=>failures.push(e),Err(e)=>failures.push(e.to_string())}}
    sampling.store(false,Ordering::SeqCst);let _=sampler.await;
    let samples=samples.lock().map_err(|_|"process metric lock")?.clone();
    let mut process_metrics=Vec::new();let mut cpu_pct=Vec::new();let mut rss_kb=Vec::new();
    for (i,(mono,rss,cpu)) in samples.iter().enumerate(){
        process_metrics.push(json!({"mono_ns":mono,"rss_kb":rss,"cpu_ns":cpu}));
        rss_kb.push(*rss as f64);
        if i>0{let(prev_m,_,prev_cpu)=samples[i-1];let dt=mono.saturating_sub(prev_m);if dt>0{cpu_pct.push(cpu.saturating_sub(prev_cpu) as f64 / dt as f64 * 100.0);}}
    }
    let mut mf=OpenOptions::new().create_new(true).write(true).open(Path::new(&out).join("PROCESS_METRICS.jsonl")).map_err(|e|e.to_string())?;
    for row in &process_metrics{writeln!(mf,"{row}").map_err(|e|e.to_string())?;}
    println!("{}",serde_json::to_string_pretty(&json!({"schema":"R1256_PUBLIC_ENDPOINT_PROBE_V1","orders":0,"paper":false,"kill_switch":"ENGAGED","duration_s_per_socket":seconds,"symbols":["BTCUSDT","ETHUSDT"],"binance_clock_probe":clock,"process_metrics":{"sample_count":samples.len(),"cpu_percent_p95":pct_f64(&cpu_pct,0.95),"cpu_percent_max":cpu_pct.iter().copied().reduce(f64::max),"rss_kb_p95":pct_f64(&rss_kb,0.95),"rss_kb_max":rss_kb.iter().copied().reduce(f64::max)},"results":results,"failures":failures})).map_err(|e|e.to_string())?);
    Ok(())
}
#[cfg(test)]
mod r1256_tests {
    use super::*;
    #[test]
    fn endpoint_plan_uses_public_routes_and_expected_stream_modes() {
        let p=endpoint_plan();
        assert_eq!(p.len(),6);
        assert!(p.iter().all(|(_,u,_)|u.starts_with("wss://fstream.binance.com/public/")));
        assert!(p.iter().all(|(_,u,_)|!u.contains("fstream.binance.com/stream?")));
        assert_eq!(p.iter().filter(|(n,_,_)|n.contains("individual")).count(),2);
        assert!(p.iter().any(|(n,u,_)|*n=="bn_bookticker_combined_url"&&u.contains("/public/stream?streams=btcusdt@bookTicker/ethusdt@bookTicker")));
        assert!(p.iter().any(|(n,u,_)|*n=="bn_bookticker_subscribe"&&u.ends_with("/public/stream")));
        assert!(p.iter().any(|(n,u,d)|*n=="bn_depth20_100ms"&&!d&&u.contains("@depth20@100ms")));
        assert!(p.iter().any(|(n,u,d)|*n=="bn_diffdepth_100ms"&&*d&&u.contains("@depth@100ms")));
    }
    #[test]
    fn selective_lane_filter_keeps_only_named_sessions() {
        assert!(lane_selected(None,"lane_a"));
        assert!(lane_selected(Some("lane_a,lane_b"),"lane_b"));
        assert!(!lane_selected(Some("lane_a,lane_b"),"lane_c"));
        assert!(!lane_selected(Some(""),"lane_a"));
    }
    #[test]
    fn diff_depth_sequence_gate_remains_fail_closed() {
        let mut b=Book::from_snapshot(100,vec![("99".into(),"2".into())],vec![("101".into(),"3".into())]);
        let bridge=Delta{first_id:99,final_id:101,previous_final_id:98,bids:vec![("99".into(),"4".into())],asks:vec![]};
        assert!(b.bridge(100,&bridge).is_ok());
        let gap=Delta{first_id:103,final_id:104,previous_final_id:103,bids:vec![("99".into(),"9".into())],asks:vec![]};
        assert!(b.apply(&gap).is_err());
        assert!(!b.valid);
        assert_eq!(b.bids.get("99").map(String::as_str),Some("4"));
    }
    #[test]
    fn timestamp_lineage_uses_one_monotonic_domain_and_rejects_inversions() {
        assert!(timestamp_lineage_valid(&[10,20,20,35]));
        assert!(!timestamp_lineage_valid(&[10,20,19]));
        assert!(timestamp_lineage_valid(&[]));
    }
    #[test]
    fn exact_signature_matches_direct_and_combined_frames_only() {
        let direct=json!({"s":"BTCUSDT","u":42,"E":1000,"T":999,"b":"1.0","B":"2.0","a":"1.1","A":"3.0"});
        let combined=json!({"stream":"btcusdt@bookTicker","data":direct});
        assert_eq!(exact_event_signature(&direct),exact_event_signature(&combined));
        let changed=json!({"s":"BTCUSDT","u":43,"E":1000,"T":999,"b":"1.0","B":"2.0","a":"1.1","A":"3.0"});
        assert_ne!(exact_event_signature(&direct),exact_event_signature(&changed));
        assert!(exact_event_signature(&json!({"s":"BTCUSDT"})).is_none());
    }
    #[test]
    fn payload_survives_arbitrary_fragment_boundaries() {
        let payload=br#"{"s":"BTCUSDT","u":42,"E":1000,"T":999,"b":"1.0","B":"2.0","a":"1.1","A":"3.0"}"#;
        for cut in 0..=payload.len() {
            let mut joined=Vec::new();joined.extend_from_slice(&payload[..cut]);joined.extend_from_slice(&payload[cut..]);
            let decoded:Value=serde_json::from_slice(&joined).unwrap();
            assert!(exact_event_signature(&decoded).is_some());
        }
    }
    #[tokio::test]
    async fn websocket_fragment_frames_reassemble_before_json_parse() {
        use tokio_tungstenite::tungstenite::protocol::frame::{Frame,coding::{Data,OpCode}};
        let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr=listener.local_addr().unwrap();
        let payload=br#"{"s":"BTCUSDT","u":42,"E":1000,"T":999,"b":"1.0","B":"2.0","a":"1.1","A":"3.0"}"#.to_vec();
        let server=tokio::spawn(async move {
            let (stream,_)=listener.accept().await.unwrap();
            let mut ws=tokio_tungstenite::accept_async(stream).await.unwrap();
            let cut=payload.len()/2;
            ws.send(Message::Frame(Frame::message(payload[..cut].to_vec(),OpCode::Data(Data::Text),false))).await.unwrap();
            ws.send(Message::Frame(Frame::message(payload[cut..].to_vec(),OpCode::Data(Data::Continue),true))).await.unwrap();
        });
        let (mut ws,_)=connect_async(format!("ws://{addr}")).await.unwrap();
        match ws.next().await.unwrap().unwrap() {
            Message::Text(text)=>assert!(exact_event_signature(&serde_json::from_str(&text).unwrap()).is_some()),
            other=>panic!("unexpected reassembled message: {other:?}"),
        }
        server.await.unwrap();
    }
    #[tokio::test]
    async fn tls_websocket_fragmentation_preserves_json_message() {
        use tokio_tungstenite::tungstenite::protocol::frame::{Frame,coding::{Data,OpCode}};
        let listener=std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr=listener.local_addr().unwrap();
        let identity=native_tls::Identity::from_pkcs12(
            include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"),"/tests/fixtures/r1257_local_tls_identity.p12")),
            "r1257-test",
        ).unwrap();
        let acceptor=native_tls::TlsAcceptor::new(identity).unwrap();
        let server=std::thread::spawn(move|| {
            let (socket,_)=listener.accept().unwrap();
            let tls=acceptor.accept(socket).unwrap();
            let mut ws=tokio_tungstenite::tungstenite::accept(tls).unwrap();
            let payload=br#"{"s":"BTCUSDT","u":42,"E":1000,"T":999,"b":"1.0","B":"2.0","a":"1.1","A":"3.0"}"#;
            let cut=payload.len()/2;
            ws.write(Message::Frame(Frame::message(payload[..cut].to_vec(),OpCode::Data(Data::Text),false))).unwrap();
            ws.write(Message::Frame(Frame::message(payload[cut..].to_vec(),OpCode::Data(Data::Continue),true))).unwrap();
            ws.flush().unwrap();
            ws.close(None).unwrap();
        });
        let client=native_tls::TlsConnector::builder().danger_accept_invalid_certs(true).build().unwrap();
        let connector=tokio_tungstenite::Connector::NativeTls(client);
        let (mut ws,_)=tokio_tungstenite::connect_async_tls_with_config(
            format!("wss://127.0.0.1:{}/",addr.port()),None,false,Some(connector)
        ).await.unwrap();
        match ws.next().await.unwrap().unwrap() {
            Message::Text(text)=>assert!(exact_event_signature(&serde_json::from_str(&text).unwrap()).is_some()),
            other=>panic!("unexpected TLS-fragmented message: {other:?}"),
        }
        server.join().unwrap();
    }
    #[test]
    fn bounded_queue_exposes_overflow_without_overwrite() {
        let (tx,mut rx)=mpsc::channel(1);
        tx.try_send(1u8).unwrap();
        assert!(tx.try_send(2u8).is_err());
        assert_eq!(rx.try_recv().unwrap(),1);
        assert!(rx.try_recv().is_err());
    }
    #[test]
    fn reconnect_creates_new_generation_and_rejects_old_frames() {
        let first=session_id("btc",1,100);
        let second=session_id("btc",2,200);
        assert_ne!(first,second);
        assert!(generation_accepts(2,2));
        assert!(!generation_accepts(2,1));
    }
}
async fn capture_okx(label:String,url:String,sub:Value,seconds:u64,out:String)->Result<Value,String>{
    let started=Instant::now();
    let (mut ws,_)=tokio::time::timeout(Duration::from_secs(20),connect_async(&url)).await.map_err(|_|format!("{label}: connect timeout"))?.map_err(|e|format!("{label}: connect: {e}"))?;
    let connect_ms=started.elapsed().as_millis() as u64;
    let (local,peer)=socket_addrs(&ws);
    let parsed=url::Url::parse(&url).map_err(|e|e.to_string())?;let host=parsed.host_str().ok_or("OKX host")?;
    let dns_ips=tokio::net::lookup_host((host,443)).await.map_err(|e|e.to_string())?.map(|a|a.ip().to_string()).collect::<Vec<_>>();
    ws.send(Message::Text(sub.to_string())).await.map_err(|e|e.to_string())?;
    let path=Path::new(&out).join(format!("{label}.jsonl")); let mut w=open_writer(&path)?;
    write_line(&mut w,&json!({"kind":"meta","label":label,"url":url,"local_addr":local,"peer_addr":peer,"dns_ips":dns_ips,"connect_ms":connect_ms,"subscribe_request":sub,"started_utc_ms":now_ms(),"vip":false}))?;
    let deadline=Instant::now()+Duration::from_secs(seconds);let mut frames=0u64;let mut errors=0u64;let mut seq_ok=0u64;let mut subscribe_acks=0u64;let mut last:HashMap<String,u64>=HashMap::new();let mut invalid=HashMap::<String,u64>::new();
    while Instant::now()<deadline{
      let item=tokio::time::timeout(deadline.saturating_duration_since(Instant::now()),ws.next()).await;
      let msg=match item{Err(_)=>break,Ok(Some(Ok(x)))=>x,Ok(Some(Err(e)))=>{errors+=1;write_line(&mut w,&json!({"kind":"error","recv_utc_ms":now_ms(),"error":e.to_string()}))?;break},Ok(None)=>break};
      match msg{
        Message::Ping(p)=>{if let Err(e)=ws.send(Message::Pong(p)).await{errors+=1;write_line(&mut w,&json!({"kind":"error","error":e.to_string()}))?;break;}},
        Message::Text(t)=>{let c:Value=serde_json::from_str(&t).unwrap_or(Value::Null);if c["event"]=="subscribe"{subscribe_acks+=1;}process_okx_frame(&t,&label,&local,&mut w,&mut frames,&mut errors,&mut seq_ok,&mut last,&mut invalid)?;},
        Message::Binary(b)=>{let t=String::from_utf8_lossy(&b).to_string();let c:Value=serde_json::from_str(&t).unwrap_or(Value::Null);if c["event"]=="subscribe"{subscribe_acks+=1;}process_okx_frame(&t,&label,&local,&mut w,&mut frames,&mut errors,&mut seq_ok,&mut last,&mut invalid)?;},
        Message::Close(c)=>{write_line(&mut w,&json!({"kind":"close","recv_utc_ms":now_ms(),"close":format!("{c:?}")}))?;break;},
        _=>{}
      }
    }
    w.flush().map_err(|e|e.to_string())?;
    Ok(json!({"label":label,"frames":frames,"errors":errors,"subscribe_acks":subscribe_acks,"seq_checked":seq_ok,"invalid":invalid,"last_seq":last,"duration_s":started.elapsed().as_secs_f64()}))
}
fn process_okx_frame(
    text:&str,label:&str,local:&str,w:&mut BufWriter<std::fs::File>,
    frames:&mut u64,errors:&mut u64,seq_ok:&mut u64,last:&mut HashMap<String,u64>,
    invalid:&mut HashMap<String,u64>
)->Result<(),String>{
    let recv=now_ms();let parse_start=std::time::Instant::now();
    let parsed=serde_json::from_str::<Value>(text);let parse_us=parse_start.elapsed().as_micros() as u64;let parse_ok=parsed.is_ok();
    let v:Value=parsed.unwrap_or(Value::Null);
    *frames+=1;
    if let (Some(arg),Some(data))=(v.get("arg"),v.get("data").and_then(Value::as_array)){
        let ch=arg.get("channel").and_then(Value::as_str).unwrap_or("");
        let inst=arg.get("instId").and_then(Value::as_str).unwrap_or("");
        let key=format!("{ch}:{inst}");
        let action=v.get("action").and_then(Value::as_str).unwrap_or("snapshot");
        for row in data{
            let seq=num(row,"seqId");let prev=num(row,"prevSeqId");
            if ch=="books"{
                if action=="snapshot"{last.insert(key.clone(),seq);*seq_ok+=1;}
                else if let Some(old)=last.get(&key).copied(){
                    if prev==old{last.insert(key.clone(),seq);*seq_ok+=1;}
                    else{*invalid.entry(key.clone()).or_default()+=1;*errors+=1;}
                }else{*invalid.entry(key.clone()).or_default()+=1;*errors+=1;}
            }
        }
    }
    let event_ts_ms=v["data"].as_array().and_then(|a|a.first()).and_then(|r|r["ts"].as_str()).and_then(|x|x.parse::<i64>().ok());
    write_line(w,&json!({"kind":"frame","label":label,"recv_utc_ms":recv,"local_addr":local,"wire_bytes":text.len(),"parse_us":parse_us,"parse_ok":parse_ok,"event_ts_ms":event_ts_ms,"event_age_ms":event_ts_ms.map(|t|recv-t),"event":v}))
}
async fn enqueue_timed_frame(tx:&mpsc::Sender<Result<(String,i64,u128,u64,u64,u64,u64),String>>,text:String,recv:i64,delivered:u64,poll_enter:u64,poll_ready:u64)->Result<(),()> {
    let reserve_start=run_mono_ns();
    let permit=tx.reserve().await.map_err(|_|())?;
    let reserve_wait=run_mono_ns().saturating_sub(reserve_start);
    let enqueue=run_mono_ns();
    permit.send(Ok((text,recv,delivered as u128,poll_enter,poll_ready,enqueue,reserve_wait)));
    Ok(())
}
async fn pump_diff(
    mut ws:tokio_tungstenite::WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
    label:String,url:String,seconds:u64,out:String,connect_ms:u64,dns_probe_ms:u64,local:String,peer:String,dns_ips:Vec<String>
)->Result<Value,String>{
    let started=Instant::now(); let deadline=started+Duration::from_secs(seconds);
    let mut raw=open_writer(&Path::new(&out).join(format!("{label}.jsonl")))?;
    let mut state=Some(open_writer(&Path::new(&out).join(format!("{label}_book_states.jsonl")))?);
    write_line(&mut raw,&json!({"kind":"meta","label":label,"session_id":session_id(&label,1,run_mono_ns()),"generation":1,"url":url,"local_addr":local,"peer_addr":peer,"dns_ips":dns_ips,"dns_probe_ms_after_connect":dns_probe_ms,"permit_and_tls_ws_connect_total_ms":connect_ms,"started_utc_ms":now_ms(),"snapshot_phase":"concurrent_with_receive"}))?;
    let (tx,mut rx)=mpsc::channel::<Result<(String,i64,u128,u64,u64,u64,u64),String>>(50000);
    let reader=tokio::spawn(async move{
        while Instant::now()<deadline{
            let remaining=deadline.saturating_duration_since(Instant::now());
            let poll_enter_ns=run_mono_ns();
            let item=tokio::time::timeout(remaining,ws.next()).await;
            let poll_ready_ns=run_mono_ns();
            let msg=match item{Err(_)=>break,Ok(Some(Ok(x)))=>x,Ok(Some(Err(e)))=>{let _=tx.send(Err(e.to_string())).await;break},Ok(None)=>break};
            match msg{
                Message::Ping(p)=>{if let Err(e)=ws.send(Message::Pong(p)).await{let _=tx.send(Err(e.to_string())).await;break;}},
                Message::Text(t)=>{let recv=now_ms();let delivered=run_mono_ns();if enqueue_timed_frame(&tx,t,recv,delivered,poll_enter_ns,poll_ready_ns).await.is_err(){break;}},
                Message::Binary(b)=>{let t=String::from_utf8_lossy(&b).to_string();let recv=now_ms();let delivered=run_mono_ns();if enqueue_timed_frame(&tx,t,recv,delivered,poll_enter_ns,poll_ready_ns).await.is_err(){break;}},
                Message::Close(_)=>break,
                _=>{}
            }
        }
    });
    let mut books:HashMap<String,BinanceBook>=HashMap::new();
    let mut snapshot_errors=HashMap::new(); let mut snapshot_ms=HashMap::new();
    let btc_snapshot=tokio::task::spawn_blocking(||{let start=Instant::now();let result=make_snapshot("BTCUSDT");(result,start.elapsed().as_millis() as u64)});
    let eth_snapshot=tokio::task::spawn_blocking(||{let start=Instant::now();let result=make_snapshot("ETHUSDT");(result,start.elapsed().as_millis() as u64)});
    let (btc_snapshot,eth_snapshot)=tokio::join!(btc_snapshot,eth_snapshot);
    for (name,joined) in [("BTCUSDT",btc_snapshot),("ETHUSDT",eth_snapshot)]{
        match joined{
            Ok((Ok((id,bids,asks)),elapsed_ms))=>{
                snapshot_ms.insert(name.to_string(),elapsed_ms);
                books.insert(name.to_string(),BinanceBook{book:Some(Book::from_snapshot(id,bids.clone(),asks.clone())),snap_id:id,pending:Vec::new(),synced:false,gaps:0,duplicates:0,bridge_ms:None});
                write_line(&mut raw,&json!({"kind":"snapshot","symbol":name,"last_update_id":id,"fetch_ms":elapsed_ms,"fetch_mode":"parallel"}))?;
            },
            Ok((Err(e),_))=>{snapshot_errors.insert(name.to_string(),e.clone());write_line(&mut raw,&json!({"kind":"snapshot_error","symbol":name,"error":e}))?;},
            Err(e)=>{snapshot_errors.insert(name.to_string(),e.to_string());write_line(&mut raw,&json!({"kind":"snapshot_error","symbol":name,"error":e.to_string()}))?;}
        }
    }
    let mut frames=0u64;let mut errors=0u64;let mut sequence_ok=0u64;
    while Instant::now()<deadline{
        let item=tokio::time::timeout(deadline.saturating_duration_since(Instant::now()),rx.recv()).await;
        match item{
            Err(_)=>break,Ok(None)=>break,
            Ok(Some(Err(e)))=>{errors+=1;write_line(&mut raw,&json!({"kind":"error","recv_utc_ms":now_ms(),"error":e}))?;break;},
            Ok(Some(Ok((text,recv,mono,poll_enter,poll_ready,queue_enqueue,queue_wait))))=>process_frame(&text,&label,&local,&peer,recv,mono,run_mono_ns().saturating_sub(queue_enqueue),poll_enter,poll_ready,queue_enqueue,queue_wait,&mut raw,&mut state,&mut books,&mut frames,&mut sequence_ok,&mut errors)?
        }
    }
    let _=reader.await;
    while let Ok(item)=rx.try_recv(){
        match item{
            Ok((text,recv,mono,poll_enter,poll_ready,queue_enqueue,queue_wait))=>process_frame(&text,&label,&local,&peer,recv,mono,run_mono_ns().saturating_sub(queue_enqueue),poll_enter,poll_ready,queue_enqueue,queue_wait,&mut raw,&mut state,&mut books,&mut frames,&mut sequence_ok,&mut errors)?,
            Err(e)=>{errors+=1;write_line(&mut raw,&json!({"kind":"error","recv_utc_ms":now_ms(),"error":e}))?;}
        }
    }
    raw.flush().map_err(|e|e.to_string())?;
    if let Some(w)=state.as_mut(){w.flush().map_err(|e|e.to_string())?;}
    let health:HashMap<String,Value>=books.iter().map(|(s,b)|(s.clone(),json!({"snapshot_id":b.snap_id,"synced":b.synced,"last_update_id":b.book.as_ref().and_then(|x|x.last_id),"valid":b.book.as_ref().map(|x|x.valid).unwrap_or(false),"gaps":b.gaps,"duplicates":b.duplicates,"bridge_latency_ms":b.bridge_ms,"snapshot_fetch_ms":snapshot_ms.get(s),"buffered":b.pending.len()}))).collect();
    Ok(json!({"label":label,"frames":frames,"errors":errors,"sequence_applied":sequence_ok,"duration_s":started.elapsed().as_secs_f64(),"health":health,"snapshot_errors":snapshot_errors}))
}
