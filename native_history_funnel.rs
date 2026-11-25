//! R22 review-only native journal funnel.
//! Rust AF_UNIX admission -> bounded batching -> certified native_durable_registrar stdin/stdout.
//! No DB1, exchange credentials or trading endpoints.

use openssl::sha::sha256;
use serde::{Deserialize,Serialize};
use serde_json::{json,Value};
use std::{
    collections::VecDeque,
    fs,
    io::{self,BufRead,BufReader,BufWriter,Write},
    os::unix::net::{UnixListener,UnixStream},
    path::{Path,PathBuf},
    process::{Child,ChildStdin,ChildStdout,Command,Stdio},
    sync::{Arc,Condvar,Mutex,atomic::{AtomicU64,Ordering}},
    thread,
    time::{Duration,Instant},
};

const CAPACITY:usize=128;
const BATCH_MAX:usize=32;
const LINE_MAX:usize=1_048_576;

fn hex(x:&[u8])->String{x.iter().map(|b|format!("{b:02x}")).collect()}
fn mono_ns()->u64{spotarb_native_market_core::transport::monotonic_ns()}

#[derive(Clone,Serialize,Deserialize,Debug)]
struct RegistrarEvent{
    seq:u64,event_id:String,content_hash:String,record_json:String,
    event_created_ns:u64,admitted_ns:u64,
}
struct Pending{
    request_id:u64,
    writer:Arc<Mutex<UnixStream>>,
    event:RegistrarEvent,
}
struct QueueState{
    queue:VecDeque<Pending>,
    active:usize,
    next_seq:u64,
    batches:u64,
    durable:u64,
    denied:u64,
    peak:usize,
    fault:Option<String>,
    closing:bool,
    latest_ack:Value,
}
struct Shared{state:Mutex<QueueState>,cv:Condvar}
struct Registrar{
    child:Child,
    input:BufWriter<ChildStdin>,
    output:BufReader<ChildStdout>,
}
impl Registrar{
    fn start(binary:&Path,case:&Path)->Result<(Self,Value),String>{
        let mut child=Command::new(binary).arg(case)
            .stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::inherit())
            .spawn().map_err(|e|format!("registrar spawn:{e}"))?;
        let input=BufWriter::new(child.stdin.take().ok_or("registrar stdin")?);
        let mut output=BufReader::new(child.stdout.take().ok_or("registrar stdout")?);
        let mut line=String::new();output.read_line(&mut line).map_err(|e|e.to_string())?;
        if line.is_empty(){return Err("registrar ready EOF".into())}
        let ready:Value=serde_json::from_str(&line).map_err(|e|format!("registrar ready json:{e}"))?;
        if ready["ready"]!=true{return Err(format!("registrar not ready:{ready}"))}
        Ok((Self{child,input,output},ready))
    }
    fn batch(&mut self,events:&[RegistrarEvent])->Result<Value,String>{
        let line=serde_json::to_string(&json!({"op":"batch","events":events})).map_err(|e|e.to_string())?;
        if line.len()+1>LINE_MAX{return Err("registrar batch request bound".into())}
        writeln!(self.input,"{line}").map_err(|e|e.to_string())?;
        self.input.flush().map_err(|e|e.to_string())?;
        let mut raw=String::new();self.output.read_line(&mut raw).map_err(|e|e.to_string())?;
        if raw.is_empty(){return Err(format!("registrar EOF:{:?}",self.child.try_wait()))}
        serde_json::from_str(&raw).map_err(|e|format!("registrar ack json:{e}"))
    }
    fn stop(mut self)->Result<(),String>{
        writeln!(self.input,"{}",json!({"op":"stop"})).map_err(|e|e.to_string())?;
        self.input.flush().map_err(|e|e.to_string())?;
        let mut raw=String::new();self.output.read_line(&mut raw).map_err(|e|e.to_string())?;
        let _=self.child.wait().map_err(|e|e.to_string())?;
        Ok(())
    }
}

fn stable_event(context:&Value,seq:u64)->Result<RegistrarEvent,String>{
    let kind=context["kind"].as_str().ok_or("context kind")?;
    if !matches!(kind,"signal"|"decision"){return Err("context kind".into())}
    let identity=json!([
        context["run"],context["owner"],context["incarnation"],context["ordinal"],kind,
        context.get("route_index").cloned().unwrap_or(json!(0))
    ]);
    let eid=format!("native-{}",hex(&sha256(serde_json::to_string(&identity).unwrap().as_bytes())));
    let mut payload=json!({"native_context":context});
    if kind=="decision"{
        payload["decision"]=context.get("decision").and_then(|d|d.get("reason")).cloned().unwrap_or(Value::Null);
        payload["skip_reason"]=Value::Null;
        payload["original_reason"]=Value::Null;
    }
    let payload_json=serde_json::to_string(&payload).map_err(|e|e.to_string())?;
    let record=json!({
        "event_id":eid,
        "mode":"test",
        "category":if kind=="signal"{"signals"}else{"decisions"},
        "name":if kind=="signal"{"SIGNAL_SEEN"}else{"DECISION_MADE"},
        "source":"native_history_r22",
        "run_id":context["run"],
        "occurred_at_ms":context["event_created_wall_ms"],
        "correlation_id":Value::Null,
        "signal_id":Value::Null,
        "base":context["base"],
        "shadow_group_id":Value::Null,
        "comparison_id":Value::Null,
        "reason_code":Value::Null,
        "schema_version":1,
        "verification_status":"unverified",
        "is_exchange_verified":0,
        "payload_json":payload_json,
        "verification_json":"{}"
    });
    let record_json=serde_json::to_string(&record).map_err(|e|e.to_string())?;
    if record_json.len()>16_384{return Err("canonical collector event bound".into())}
    let content_hash=hex(&sha256(record_json.as_bytes()));
    Ok(RegistrarEvent{
        seq,event_id:eid,content_hash,record_json,
        event_created_ns:context["event_created_ns"].as_u64().ok_or("event_created_ns")?,
        admitted_ns:mono_ns(),
    })
}

fn send(writer:&Arc<Mutex<UnixStream>>,row:&Value)->io::Result<()>{
    let raw=serde_json::to_vec(row).map_err(io::Error::other)?;
    if raw.len()+1>65_536{return Err(io::Error::other("response bound"))}
    let mut w=writer.lock().unwrap();w.write_all(&raw)?;w.write_all(b"\n")?;w.flush()
}
fn status_value(s:&QueueState,through_start:u64)->Value{
    json!({"queue_depth":s.queue.len(),"active":s.active,"capacity":CAPACITY,"batch_max":BATCH_MAX,
        "queue_peak":s.peak,"next_seq":s.next_seq,"startup_through_seq":through_start,
        "batches":s.batches,"durable":s.durable,"denied":s.denied,"fault":s.fault,
        "closing":s.closing,"latest_ack":s.latest_ack})
}

fn worker(shared:Arc<Shared>,mut registrar:Registrar,flush:Duration){
    loop{
        let batch={
            let mut s=shared.state.lock().unwrap();
            while s.queue.is_empty()&&!s.closing&&s.fault.is_none(){s=shared.cv.wait(s).unwrap();}
            if s.fault.is_some(){return}
            if s.queue.is_empty()&&s.closing{break}
            let deadline=Instant::now()+flush;
            while s.queue.len()<BATCH_MAX&&!s.closing&&s.fault.is_none(){
                let now=Instant::now();if now>=deadline{break}
                let (next,_)=shared.cv.wait_timeout(s,deadline-now).unwrap();s=next;
            }
            let n=s.queue.len().min(BATCH_MAX);
            (0..n).filter_map(|_|s.queue.pop_front()).collect::<Vec<_>>()
        };
        let events=batch.iter().map(|p|p.event.clone()).collect::<Vec<_>>();
        let ack=match registrar.batch(&events){Ok(v)=>v,Err(e)=>{
            let mut s=shared.state.lock().unwrap();s.fault=Some(e);shared.cv.notify_all();return
        }};
        let valid=ack["ok"]==true&&ack["phase"]=="journal_durable"
            &&ack["batch_first"].as_u64()==events.first().map(|e|e.seq)
            &&ack["batch_last"].as_u64()==events.last().map(|e|e.seq)
            &&ack["batch_count"].as_u64()==Some(events.len() as u64)
            &&ack["durable_ns"].as_u64().unwrap_or(0)>0;
        if !valid{
            let mut s=shared.state.lock().unwrap();s.fault=Some(format!("invalid registrar ack:{ack}"));shared.cv.notify_all();return
        }
        let through=ack["through_seq"].as_u64().unwrap_or(0);
        let durable_ns=ack["durable_ns"].as_u64().unwrap_or(0);
        for p in &batch{
            let row=json!({"id":p.request_id,"phase":"journal_durable","ok":true,
                "event_id":p.event.event_id,"content_hash":p.event.content_hash,
                "journal_seq":p.event.seq,"journal_through_seq":through,
                "durability":"journal_sync_all_and_floor","durable_ns":durable_ns,
                "registrar_receive_ns":ack["registrar_receive_ns"],"journal_sync_ns":ack["journal_sync_ns"],
                "floor_sync_ns":ack["floor_sync_ns"],"append_ns":ack["append_ns"],
                "journal_fsync_ns":ack["journal_fsync_ns"],"chain_hash":ack["chain_hash"]});
            if send(&p.writer,&row).is_err(){
                // Peer loss cannot revoke durability. Continue releasing other peers.
            }
        }
        let mut s=shared.state.lock().unwrap();
        s.active=s.active.saturating_sub(batch.len());s.batches+=1;s.durable+=batch.len() as u64;s.latest_ack=ack;
        shared.cv.notify_all();
    }
    let _=registrar.stop();
}

fn session(stream:UnixStream,shared:Arc<Shared>,session_id:u64,through_start:u64){
    let writer=Arc::new(Mutex::new(stream.try_clone().unwrap()));
    let mut reader=BufReader::new(stream);
    let mut last_id=0u64;
    loop{
        let mut raw=String::new();
        match reader.read_line(&mut raw){Ok(0)|Err(_)=>break,Ok(_)=>{}}
        if raw.len()>65_536{break}
        let req:Value=match serde_json::from_str(&raw){Ok(v)=>v,Err(_)=>break};
        let id=req["id"].as_u64().unwrap_or(0);
        match req["op"].as_str().unwrap_or("") {
            "event"=>{
                if req["ack_mode"]!="journal"||id!=last_id+1{
                    let _=send(&writer,&json!({"id":id,"phase":"ram_admitted","ok":false,"error":"journal ordered request"}));break
                }
                last_id=id;
                let context=req["context"].clone();
                let event={
                    let mut s=shared.state.lock().unwrap();
                    if s.fault.is_some()||s.closing||s.active>=CAPACITY{
                        s.denied+=1;
                        let _=send(&writer,&json!({"id":id,"phase":"ram_admitted","ok":false,"error":s.fault.clone().unwrap_or_else(||"bounded_admission_full".into())}));
                        continue
                    }
                    let seq=s.next_seq+1;
                    match stable_event(&context,seq){
                        Ok(event)=>{
                            s.next_seq=seq;s.active+=1;s.peak=s.peak.max(s.active);
                            s.queue.push_back(Pending{request_id:id,writer:writer.clone(),event:event.clone()});
                            shared.cv.notify_all();event
                        },
                        Err(e)=>{s.denied+=1;let _=send(&writer,&json!({"id":id,"phase":"ram_admitted","ok":false,"error":e}));continue}
                    }
                };
                if send(&writer,&json!({"id":id,"phase":"ram_admitted","ok":true,
                    "event_id":event.event_id,"content_hash":event.content_hash,"ticket":event.seq,"session":session_id,"admitted_ns":event.admitted_ns})).is_err(){break}
            }
            "status"=>{
                let s=shared.state.lock().unwrap();
                let _=send(&writer,&json!({"id":id,"ok":true,"result":status_value(&s,through_start)}));
            }
            "stop"=>{
                let mut s=shared.state.lock().unwrap();s.closing=true;shared.cv.notify_all();
                let _=send(&writer,&json!({"id":id,"ok":true,"result":status_value(&s,through_start)}));break
            }
            _=>{let _=send(&writer,&json!({"id":id,"ok":false,"error":"unknown op"}));}
        }
    }
}

fn main()->Result<(),Box<dyn std::error::Error>>{
    let args:Vec<_>=std::env::args().collect();
    if args.len()!=4{return Err("usage: native_history_funnel <case_dir> <history.sock> <native_durable_registrar>".into())}
    let case=PathBuf::from(&args[1]);let socket=PathBuf::from(&args[2]);let binary=PathBuf::from(&args[3]);
    let root=Path::new("/home/tmpadmin/native_rust_registrar_review_20261002").canonicalize()?;
    fs::create_dir_all(&case)?;
    if !case.canonicalize()?.starts_with(&root){return Err("isolated case required".into())}
    if socket.parent()!=Some(case.as_path())||socket.extension().and_then(|x|x.to_str())!=Some("sock"){return Err("socket must be inside case".into())}
    let _=fs::remove_file(&socket);
    let (registrar,ready)=Registrar::start(&binary,&case).map_err(io::Error::other)?;
    let through=ready["status"]["through_seq"].as_u64().ok_or("through_seq")?;
    let shared=Arc::new(Shared{state:Mutex::new(QueueState{
        queue:VecDeque::new(),active:0,next_seq:through,batches:0,durable:0,denied:0,peak:0,
        fault:None,closing:false,latest_ack:Value::Null}),cv:Condvar::new()});
    let wshared=shared.clone();
    let worker=thread::spawn(move||worker(wshared,registrar,Duration::from_millis(2)));
    let listener=UnixListener::bind(&socket)?;listener.set_nonblocking(true)?;
    println!("{}",json!({"ready":true,"socket":socket,"registrar_ready":ready}));
    let sessions=Arc::new(AtomicU64::new(0));let mut threads=Vec::new();
    loop{
        if shared.state.lock().unwrap().closing {break}
        match listener.accept(){
            Ok((stream,_))=>{
                let id=sessions.fetch_add(1,Ordering::Relaxed)+1;let sh=shared.clone();
                threads.push(thread::spawn(move||session(stream,sh,id,through)));
            }
            Err(e) if e.kind()==io::ErrorKind::WouldBlock=>thread::sleep(Duration::from_millis(10)),
            Err(e)=>return Err(e.into()),
        }
    }
    for t in threads{let _=t.join();}
    let _=worker.join();
    let s=shared.state.lock().unwrap();
    fs::write(case.join("funnel_final.json"),serde_json::to_vec_pretty(&status_value(&s,through))?)?;
    let _=fs::remove_file(&socket);
    if s.fault.is_some(){std::process::exit(2)}
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn canonical_decision_envelope_matches_collector_contract() {
        let c=json!({"run":"r22-test","owner":1,"incarnation":2,"ordinal":3,"generation":1,
            "metadata_hash":"abc","recovery_cut":0,"kind":"decision","route_index":0,
            "base":"BTC","decision":{"reason":"EXECUTABLE_OPPORTUNITY"},
            "event_created_ns":123,"event_created_wall_ms":1700000000000i64});
        let e=stable_event(&c,1).unwrap();
        let r:Value=serde_json::from_str(&e.record_json).unwrap();
        assert_eq!(r["mode"],"test");assert_eq!(r["category"],"decisions");
        assert_eq!(r["name"],"DECISION_MADE");assert_eq!(r["source"],"native_history_r22");
        assert_eq!(r["schema_version"],1);assert_eq!(r["verification_status"],"unverified");
        assert_eq!(r["is_exchange_verified"],0);assert!(r["payload_json"].is_string());
        assert!(r["verification_json"].is_string());assert!(e.record_json.len()<=16_384);
        assert_eq!(e.content_hash,hex(&sha256(e.record_json.as_bytes())));
    }
    #[test]
    fn unsupported_kind_fails_closed() {
        let c=json!({"run":"r22-test","owner":1,"incarnation":2,"ordinal":3,"kind":"order",
            "base":"BTC","event_created_ns":123,"event_created_wall_ms":1700000000000i64});
        assert!(stable_event(&c,1).is_err());
    }
}
