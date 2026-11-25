#[path = "../r1250_orderbook.rs"]
mod orderbook;

use futures_util::{SinkExt, StreamExt};
use orderbook::{Book, Delta};
use serde_json::{json, Value};
use std::{collections::HashMap, fs::{create_dir_all,OpenOptions}, io::{BufWriter,Write}, path::Path, process::Command, time::{SystemTime,UNIX_EPOCH}};
use tokio::{sync::mpsc,time::{Duration,Instant}};
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream};

const SYMBOLS: [&str; 4] = ["BTCUSDT","ETHUSDT","BNBUSDT","SOLUSDT"];
fn now_ms() -> i64 { SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64 }
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
    let (mut ws,_)=tokio::time::timeout(Duration::from_secs(20),connect_async(&url)).await.map_err(|_|format!("{label}: connect timeout"))?.map_err(|e|format!("{label}: connect: {e}"))?;
    let (local,peer)=socket_addrs(&ws);
    let connect_ms=connect_start.elapsed().as_millis() as u64;
    if diff_mode { return pump_diff(ws,label,url,seconds,out,connect_ms,local,peer).await; }
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
    write_line(&mut raw,&json!({"kind":"meta","label":label,"url":url,"local_addr":local,"peer_addr":peer,"connect_ms":connect_ms,"snapshots":snapshots.keys().collect::<Vec<_>>(),"snapshot_errors":snapshot_errors,"started_utc_ms":now_ms()}))?;
    let deadline=Instant::now()+Duration::from_secs(seconds);
    let started=Instant::now();
    let mut frames=0u64; let mut errors=0u64; let mut sequence_ok=0u64;
    while Instant::now()<deadline {
        let remaining=deadline.saturating_duration_since(Instant::now());
        let item=tokio::time::timeout(remaining,ws.next()).await;
        let msg=match item { Err(_)=>break, Ok(Some(Ok(x)))=>x, Ok(Some(Err(e)))=>{errors+=1; write_line(&mut raw,&json!({"kind":"error","recv_utc_ms":now_ms(),"error":e.to_string()}))?;break}, Ok(None)=>break };
        match msg {
            Message::Ping(p)=>{ if let Err(e)=ws.send(Message::Pong(p)).await {errors+=1;write_line(&mut raw,&json!({"kind":"error","error":e.to_string()}))?;break;} },
            Message::Text(t)=>process_frame(&t,&label,&local,&peer,now_ms(),started.elapsed().as_nanos(),&mut raw,&mut state,&mut books,&mut frames,&mut sequence_ok,&mut errors)?,
            Message::Binary(b)=>{let t=String::from_utf8_lossy(&b).to_string();process_frame(&t,&label,&local,&peer,now_ms(),started.elapsed().as_nanos(),&mut raw,&mut state,&mut books,&mut frames,&mut sequence_ok,&mut errors)?;},
            Message::Close(c)=>{write_line(&mut raw,&json!({"kind":"close","recv_utc_ms":now_ms(),"close":format!("{c:?}")}))?;break;},
            _=>{}
        }
    }
    raw.flush().map_err(|e|e.to_string())?;
    if let Some(w)=state.as_mut(){w.flush().map_err(|e|e.to_string())?;}
    let health:HashMap<String,Value>=books.iter().map(|(s,b)|(s.clone(),json!({"snapshot_id":b.snap_id,"synced":b.synced,"last_update_id":b.book.as_ref().and_then(|x|x.last_id),"valid":b.book.as_ref().map(|x|x.valid).unwrap_or(false),"gaps":b.gaps,"duplicates":b.duplicates,"bridge_ms":b.bridge_ms,"buffered":b.pending.len()}))).collect();
    Ok(json!({"label":label,"frames":frames,"errors":errors,"sequence_applied":sequence_ok,"duration_s":started.elapsed().as_secs_f64(),"health":health,"snapshot_errors":snapshot_errors}))
}
fn process_frame(t:&str,label:&str,local:&str,peer:&str,recv:i64,mono_ns:u128,raw:&mut BufWriter<std::fs::File>,state:&mut Option<BufWriter<std::fs::File>>,books:&mut HashMap<String,BinanceBook>,frames:&mut u64,sequence_ok:&mut u64,errors:&mut u64)->Result<(),String>{
    let v:Value=serde_json::from_str(t).unwrap_or(Value::Null);
    let event=v.get("data").unwrap_or(&v);
    *frames+=1;
    let sym=event.get("s").and_then(Value::as_str).unwrap_or("");
    let kind=event.get("e").and_then(Value::as_str).unwrap_or(if event.get("u").is_some()&&event.get("b").is_some(){"depthUpdate"}else if event.get("lastUpdateId").is_some(){"depth20"}else{"unknown"});
    let is_diff=label=="bn_diffdepth";
    write_line(raw,&json!({"kind":"frame","label":label,"symbol":sym,"exchange_ts_ms":event.get("E").and_then(Value::as_i64),"transaction_ts_ms":event.get("T").and_then(Value::as_i64),"update_id":event.get("u").or_else(||event.get("lastUpdateId")),"recv_utc_ms":recv,"mono_ns":mono_ns,"local_addr":local,"peer_addr":peer,"event":event}))?;
    if !is_diff || kind!="depthUpdate" { return Ok(()); }
    let d=match delta(event){Some(x)=>x,None=>{*errors+=1;return Ok(())}};
    let st=match books.get_mut(sym){Some(x)=>x,None=>return Ok(())};
    if !st.synced {
        st.pending.push(d);
        let bridge_pos=st.pending.iter().position(|x|x.first_id<=st.snap_id && x.final_id>=st.snap_id);
        if let Some(pos)=bridge_pos {
            let first=st.pending[pos].clone();
            let result=st.book.as_mut().unwrap().bridge(st.snap_id,&first);
            if result.is_ok(){
                st.synced=true; st.bridge_ms=Some((mono_ns/1_000_000) as u64);
                *sequence_ok+=1;
                if let Some(w)=state.as_mut(){write_line(w,&json!({"kind":"book_state","label":label,"symbol":sym,"recv_utc_ms":recv,"update_id":st.book.as_ref().unwrap().last_id,"valid":true,"top_bids":st.book.as_ref().unwrap().top(true,20),"top_asks":st.book.as_ref().unwrap().top(false,20),"bridge":true}))?;}
                let later:Vec<_>=st.pending.drain(pos+1..).collect();
                st.pending.clear();
                for ev in later {if st.book.as_mut().unwrap().apply(&ev).is_ok(){*sequence_ok+=1;}else{st.gaps+=1;*errors+=1;break;}}
            } else {st.pending.clear();*errors+=1;}
        }
        return Ok(());
    }
    let result=st.book.as_mut().unwrap().apply(&d);
    match result {
        Ok(applied)=>{
            if applied {
                *sequence_ok+=1;
                if let Some(w)=state.as_mut(){let b=st.book.as_ref().unwrap();write_line(w,&json!({"kind":"book_state","label":label,"symbol":sym,"recv_utc_ms":recv,"update_id":b.last_id,"valid":b.valid,"top_bids":b.top(true,20),"top_asks":b.top(false,20),"bridge":false}))?;}
            }
        },
        Err(e)=>{st.gaps+=1;*errors+=1;write_line(raw,&json!({"kind":"invalid","label":label,"symbol":sym,"recv_utc_ms":recv,"reason":e,"action":"book invalidated; further updates not published; resnapshot required"}))?;}
    }
    Ok(())
}
async fn capture_simple(label:String,url:String,seconds:u64,out:String)->Result<Value,String>{
    pump(label,url,seconds,out,false).await
}

#[tokio::main(flavor="multi_thread",worker_threads=8)]
async fn main()->Result<(),String>{
    let a:Vec<String>=std::env::args().collect();
    if a.len()!=3{return Err("usage: r1250_channel_probe SECONDS OUTPUT_DIR".into())}
    let seconds:u64=a[1].parse().map_err(|e:std::num::ParseIntError|e.to_string())?;
    if !(30..=300).contains(&seconds){return Err("duration must be 30..300 seconds".into())}
    let out=a[2].clone();
    create_dir_all(&out).map_err(|e|e.to_string())?;
    let switch=std::fs::read_to_string("/home/tmpadmin/spotbot/data/real_kill_switch.json").map_err(|e|e.to_string())?;
    let sv:Value=serde_json::from_str(&switch).map_err(|e|e.to_string())?;
    if sv.get("state").and_then(Value::as_str)!=Some("ENGAGED"){return Err("REAL kill switch not ENGAGED".into())}
    let symbols=SYMBOLS.iter().map(|s|s.to_ascii_lowercase()).collect::<Vec<_>>();
    let bbase="wss://fstream.binance.com/public/stream?streams=";
    let bjobs=[
      ("bn_bookticker",symbols.iter().map(|s|format!("{s}@bookTicker")).collect::<Vec<_>>().join("/"),false),
      ("bn_depth20",symbols.iter().map(|s|format!("{s}@depth20@100ms")).collect::<Vec<_>>().join("/"),false),
      ("bn_diffdepth",symbols.iter().map(|s|format!("{s}@depth@100ms")).collect::<Vec<_>>().join("/"),true),
    ];
    let mut tasks=Vec::new();
    for (label,streams,is_diff) in bjobs {
      let url=format!("{bbase}{streams}"); let out=out.clone(); let label=label.to_string();
      tasks.push(tokio::spawn(async move{if is_diff{pump(label,url,seconds,out,true).await}else{capture_simple(label,url,seconds,out).await}}));
    }
    let okx="wss://ws.okx.com:443/ws/v5/public".to_string();
    for channel in ["bbo-tbt","books5","books"]{
      let label=format!("okx_{channel}"); let url=okx.clone(); let out=out.clone();
      let insts=["BTC-USDT","ETH-USDT","BNB-USDT","SOL-USDT"];
      let subscription=json!({"op":"subscribe","args":insts.iter().map(|x|json!({"channel":channel,"instId":x})).collect::<Vec<_>>()});
      tasks.push(tokio::spawn(async move{capture_okx(label,url,subscription,seconds,out).await}));
    }
    let mut results=Vec::new();let mut failures=Vec::new();
    for t in tasks{match t.await{Ok(Ok(x))=>results.push(x),Ok(Err(e))=>failures.push(e),Err(e)=>failures.push(e.to_string())}}
    println!("{}",serde_json::to_string_pretty(&json!({"schema":"R1250_PUBLIC_CHANNEL_PROBE_V1","orders":0,"paper":false,"kill_switch":"ENGAGED","duration_s":seconds,"symbols":SYMBOLS,"results":results,"failures":failures})).map_err(|e|e.to_string())?);
    Ok(())
}
async fn capture_okx(label:String,url:String,sub:Value,seconds:u64,out:String)->Result<Value,String>{
    let started=Instant::now();
    let (mut ws,_)=tokio::time::timeout(Duration::from_secs(20),connect_async(&url)).await.map_err(|_|format!("{label}: connect timeout"))?.map_err(|e|format!("{label}: connect: {e}"))?;
    let (local,peer)=socket_addrs(&ws);
    ws.send(Message::Text(sub.to_string())).await.map_err(|e|e.to_string())?;
    let path=Path::new(&out).join(format!("{label}.jsonl")); let mut w=open_writer(&path)?;
    write_line(&mut w,&json!({"kind":"meta","label":label,"url":url,"local_addr":local,"peer_addr":peer,"started_utc_ms":now_ms(),"vip":false}))?;
    let deadline=Instant::now()+Duration::from_secs(seconds);let mut frames=0u64;let mut errors=0u64;let mut seq_ok=0u64;let mut last:HashMap<String,u64>=HashMap::new();let mut invalid=HashMap::<String,u64>::new();
    while Instant::now()<deadline{
      let item=tokio::time::timeout(deadline.saturating_duration_since(Instant::now()),ws.next()).await;
      let msg=match item{Err(_)=>break,Ok(Some(Ok(x)))=>x,Ok(Some(Err(e)))=>{errors+=1;write_line(&mut w,&json!({"kind":"error","recv_utc_ms":now_ms(),"error":e.to_string()}))?;break},Ok(None)=>break};
      match msg{
        Message::Ping(p)=>{if let Err(e)=ws.send(Message::Pong(p)).await{errors+=1;write_line(&mut w,&json!({"kind":"error","error":e.to_string()}))?;break;}},
        Message::Text(t)=>{process_okx_frame(&t,&label,&local,&peer,&mut w,&mut frames,&mut errors,&mut seq_ok,&mut last,&mut invalid)?;},
        Message::Binary(b)=>{let t=String::from_utf8_lossy(&b).to_string();process_okx_frame(&t,&label,&local,&peer,&mut w,&mut frames,&mut errors,&mut seq_ok,&mut last,&mut invalid)?;},
        Message::Close(c)=>{write_line(&mut w,&json!({"kind":"close","recv_utc_ms":now_ms(),"close":format!("{c:?}")}))?;break;},
        _=>{}
      }
    }
    w.flush().map_err(|e|e.to_string())?;
    Ok(json!({"label":label,"frames":frames,"errors":errors,"seq_checked":seq_ok,"invalid":invalid,"last_seq":last,"duration_s":started.elapsed().as_secs_f64()}))
}
fn process_okx_frame(
    text:&str,label:&str,local:&str,peer:&str,w:&mut BufWriter<std::fs::File>,
    frames:&mut u64,errors:&mut u64,seq_ok:&mut u64,last:&mut HashMap<String,u64>,
    invalid:&mut HashMap<String,u64>
)->Result<(),String>{
    let recv=now_ms();
    let v:Value=serde_json::from_str(text).unwrap_or(Value::Null);
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
    write_line(w,&json!({"kind":"frame","label":label,"recv_utc_ms":recv,"local_addr":local,"peer_addr":peer,"event":v}))
}
async fn pump_diff(
    mut ws:tokio_tungstenite::WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>,
    label:String,url:String,seconds:u64,out:String,connect_ms:u64,local:String,peer:String
)->Result<Value,String>{
    let started=Instant::now(); let deadline=started+Duration::from_secs(seconds);
    let mut raw=open_writer(&Path::new(&out).join(format!("{label}.jsonl")))?;
    let mut state=Some(open_writer(&Path::new(&out).join(format!("{label}_book_states.jsonl")))?);
    write_line(&mut raw,&json!({"kind":"meta","label":label,"url":url,"local_addr":local,"peer_addr":peer,"connect_ms":connect_ms,"started_utc_ms":now_ms(),"snapshot_phase":"concurrent_with_receive"}))?;
    let (tx,mut rx)=mpsc::channel::<Result<(String,i64,u128),String>>(50000);
    let reader_start=Instant::now();
    let reader=tokio::spawn(async move{
        while Instant::now()<deadline{
            let remaining=deadline.saturating_duration_since(Instant::now());
            let item=tokio::time::timeout(remaining,ws.next()).await;
            let msg=match item{Err(_)=>break,Ok(Some(Ok(x)))=>x,Ok(Some(Err(e)))=>{let _=tx.send(Err(e.to_string())).await;break},Ok(None)=>break};
            match msg{
                Message::Ping(p)=>{if let Err(e)=ws.send(Message::Pong(p)).await{let _=tx.send(Err(e.to_string())).await;break;}},
                Message::Text(t)=>{if tx.send(Ok((t,now_ms(),reader_start.elapsed().as_nanos()))).await.is_err(){break;}},
                Message::Binary(b)=>{let t=String::from_utf8_lossy(&b).to_string();if tx.send(Ok((t,now_ms(),reader_start.elapsed().as_nanos()))).await.is_err(){break;}},
                Message::Close(_)=>break,
                _=>{}
            }
        }
    });
    let mut books:HashMap<String,BinanceBook>=HashMap::new();
    let mut snapshot_errors=HashMap::new(); let mut snapshot_ms=HashMap::new();
    for symbol in SYMBOLS{
        let name=symbol.to_string();let start=Instant::now();let copy=name.clone();
        match tokio::task::spawn_blocking(move||make_snapshot(&copy)).await{
            Ok(Ok((id,bids,asks)))=>{
                snapshot_ms.insert(name.clone(),start.elapsed().as_millis() as u64);
                books.insert(name.clone(),BinanceBook{book:Some(Book::from_snapshot(id,bids.clone(),asks.clone())),snap_id:id,pending:Vec::new(),synced:false,gaps:0,duplicates:0,bridge_ms:None});
                write_line(&mut raw,&json!({"kind":"snapshot","symbol":name,"last_update_id":id,"fetch_ms":start.elapsed().as_millis()}))?;
            },
            Ok(Err(e))=>{snapshot_errors.insert(name.clone(),e.clone());write_line(&mut raw,&json!({"kind":"snapshot_error","symbol":name,"error":e}))?;},
            Err(e)=>{snapshot_errors.insert(name.clone(),e.to_string());write_line(&mut raw,&json!({"kind":"snapshot_error","symbol":name,"error":e.to_string()}))?;}
        }
    }
    let mut frames=0u64;let mut errors=0u64;let mut sequence_ok=0u64;
    while Instant::now()<deadline{
        let item=tokio::time::timeout(deadline.saturating_duration_since(Instant::now()),rx.recv()).await;
        match item{
            Err(_)=>break,Ok(None)=>break,
            Ok(Some(Err(e)))=>{errors+=1;write_line(&mut raw,&json!({"kind":"error","recv_utc_ms":now_ms(),"error":e}))?;break;},
            Ok(Some(Ok((text,recv,mono))))=>process_frame(&text,&label,&local,&peer,recv,mono,&mut raw,&mut state,&mut books,&mut frames,&mut sequence_ok,&mut errors)?
        }
    }
    let _=reader.await;
    while let Ok(item)=rx.try_recv(){
        match item{
            Ok((text,recv,mono))=>process_frame(&text,&label,&local,&peer,recv,mono,&mut raw,&mut state,&mut books,&mut frames,&mut sequence_ok,&mut errors)?,
            Err(e)=>{errors+=1;write_line(&mut raw,&json!({"kind":"error","recv_utc_ms":now_ms(),"error":e}))?;}
        }
    }
    raw.flush().map_err(|e|e.to_string())?;
    if let Some(w)=state.as_mut(){w.flush().map_err(|e|e.to_string())?;}
    let health:HashMap<String,Value>=books.iter().map(|(s,b)|(s.clone(),json!({"snapshot_id":b.snap_id,"synced":b.synced,"last_update_id":b.book.as_ref().and_then(|x|x.last_id),"valid":b.book.as_ref().map(|x|x.valid).unwrap_or(false),"gaps":b.gaps,"duplicates":b.duplicates,"bridge_latency_ms":b.bridge_ms,"snapshot_fetch_ms":snapshot_ms.get(s),"buffered":b.pending.len()}))).collect();
    Ok(json!({"label":label,"frames":frames,"errors":errors,"sequence_applied":sequence_ok,"duration_s":started.elapsed().as_secs_f64(),"health":health,"snapshot_errors":snapshot_errors}))
}
