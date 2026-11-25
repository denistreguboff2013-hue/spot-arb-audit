use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::{fs::OpenOptions, io::{BufWriter, Write}, path::Path, time::{SystemTime, UNIX_EPOCH}};
use tokio::{time::{Duration, Instant}, select};
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream};

fn now_ms() -> i128 { SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i128 }
async fn capture(label: String, url: String, sub: Option<Value>, seconds: u64, out: String) -> Result<Value,String> {
    let started = Instant::now();
    let (mut ws, response) = tokio::time::timeout(Duration::from_secs(10), connect_async(&url)).await.map_err(|_| format!("{label}: connect timeout after 10s"))?.map_err(|e| format!("{label}: connect: {e}"))?;
    let connect_ms = started.elapsed().as_millis();
    let (local_addr,peer_addr)=match ws.get_ref() {
      MaybeTlsStream::NativeTls(tls)=>(tls.get_ref().get_ref().get_ref().local_addr().map(|x|x.to_string()).unwrap_or_default(),tls.get_ref().get_ref().get_ref().peer_addr().map(|x|x.to_string()).unwrap_or_default()),
      MaybeTlsStream::Plain(tcp)=>(tcp.local_addr().map(|x|x.to_string()).unwrap_or_default(),tcp.peer_addr().map(|x|x.to_string()).unwrap_or_default()),
      _=>(String::new(),String::new()),
    };
    let _ = response;
    if let Some(v) = sub { ws.send(Message::Text(v.to_string())).await.map_err(|e|e.to_string())?; }
    let path = Path::new(&out).join(format!("{label}.jsonl"));
    let f = OpenOptions::new().write(true).create_new(true).open(&path).map_err(|e|e.to_string())?;
    let mut w = BufWriter::new(f);
    writeln!(w,"{}",json!({"type":"meta","label":label,"connect_ms":connect_ms,"utc_ms":now_ms(),"local_addr":local_addr,"peer_addr":peer_addr})).map_err(|e|e.to_string())?;
    let deadline=Instant::now()+Duration::from_secs(seconds);
    let mut n=0u64; let mut errors=0u64; let mut max_gap_ns=0u128; let mut last=Instant::now();
    loop {
        select! {
            _=tokio::time::sleep_until(deadline)=>break,
            item=ws.next()=>{
                // Spacing between yielded WS frames only; not kernel receive time or executor poll-gap.
                let ready=Instant::now(); let gap=ready.duration_since(last).as_nanos(); max_gap_ns=max_gap_ns.max(gap); last=ready;
                match item {
                    Some(Ok(Message::Text(t)))=>{
                        n+=1; let recv=now_ms();
                        let event:Value=serde_json::from_str(&t).unwrap_or(Value::Null);
                        writeln!(w,"{}",json!({"type":"frame","recv_utc_ms":recv,"mono_ns":started.elapsed().as_nanos(),"ready_gap_ns":gap,"event":event,"raw":t})).map_err(|e|e.to_string())?;
                    },
                    Some(Ok(Message::Binary(b)))=>{
                        n+=1; let recv=now_ms(); let text=String::from_utf8_lossy(&b).to_string();
                        let event:Value=serde_json::from_str(&text).unwrap_or(Value::Null);
                        writeln!(w,"{}",json!({"type":"frame","recv_utc_ms":recv,"mono_ns":started.elapsed().as_nanos(),"ready_gap_ns":gap,"event":event,"raw":text})).map_err(|e|e.to_string())?;
                    },
                    Some(Ok(Message::Ping(p)))=>{ ws.send(Message::Pong(p)).await.map_err(|e|e.to_string())?; },
                    Some(Ok(Message::Close(_)))|None=>break,
                    Some(Err(e))=>{errors+=1; writeln!(w,"{}",json!({"type":"error","recv_utc_ms":now_ms(),"error":e.to_string()})).map_err(|x|x.to_string())?; break;},
                    _=>{}
                }
            }
        }
    }
    w.flush().map_err(|e|e.to_string())?;
    Ok(json!({"label":label,"path":path,"connect_ms":connect_ms,"frames":n,"errors":errors,"max_next_ready_gap_ms":max_gap_ns as f64/1e6,"duration_s":started.elapsed().as_secs_f64()}))
}
#[tokio::main(flavor="multi_thread",worker_threads=4)]
async fn main()->Result<(),String>{
    let a:Vec<String>=std::env::args().collect();
    if a.len()!=3{return Err("usage: r1249_causal_ws_probe SECONDS OUTPUT_DIR".into())}
    let secs:u64=a[1].parse().map_err(|e:std::num::ParseIntError|e.to_string())?;
    if !(30..=300).contains(&secs){return Err("duration must be 30..300 seconds".into())}
    let out=a[2].clone();
    if !Path::new(&out).is_dir(){return Err("output dir must already exist".into())}
    let checks=std::fs::read_to_string("/home/tmpadmin/spotbot/data/real_kill_switch.json").map_err(|e|e.to_string())?;
    if !checks.contains("\"state\": \"ENGAGED\""){return Err("REAL kill switch not ENGAGED".into())}
    let bbase="wss://fstream.binance.com/public/stream?streams=";
    let mut jobs=Vec::new();
    for (label,streams) in [
      ("bn_bookticker","btcusdt@bookTicker/ethusdt@bookTicker"),
      ("bn_depth20_100ms","btcusdt@depth20@100ms/ethusdt@depth20@100ms"),
      ("bn_diffdepth_100ms","btcusdt@depth@100ms/ethusdt@depth@100ms")
    ] { jobs.push((label.to_string(),format!("{bbase}{streams}"),None)); }
    jobs.push(("bn_bookticker_copy".to_string(),format!("{bbase}btcusdt@bookTicker/ethusdt@bookTicker"),None));
    jobs.push(("bn_depth20_100ms_copy".to_string(),format!("{bbase}btcusdt@depth20@100ms/ethusdt@depth20@100ms"),None));
    let okx="wss://ws.okx.com:8443/ws/v5/public".to_string();
    for channel in ["bbo-tbt","books5","books"] {
      let sub=json!({"op":"subscribe","args":[{"channel":channel,"instId":"BTC-USDT"},{"channel":channel,"instId":"ETH-USDT"}]});
      jobs.push((format!("okx_{channel}"),okx.clone(),Some(sub)));
    }
    let tasks=jobs.into_iter().map(|(l,u,s)|{let d=out.clone(); tokio::spawn(async move{capture(l,u,s,secs,d).await})}).collect::<Vec<_>>();
    let mut rows=Vec::new(); let mut failures=Vec::new();
    for t in tasks { match t.await { Ok(Ok(row))=>rows.push(row), Ok(Err(e))=>failures.push(e), Err(e)=>failures.push(e.to_string()) } }
    println!("{}",serde_json::to_string_pretty(&json!({"schema":"R1249_PUBLIC_WS_PROBE_V1","orders":0,"paper":false,"kill_switch":"ENGAGED","duration_s":secs,"results":rows,"failures":failures})).map_err(|e|e.to_string())?);
    Ok(())
}