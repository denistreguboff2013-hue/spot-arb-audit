use super::*;
use openssl::{asn1::Asn1Time,bn::BigNum,hash::MessageDigest,pkey::PKey,rsa::Rsa,x509::{X509,X509NameBuilder,extension::{BasicConstraints,SubjectAlternativeName}}};
use serde_json::json;
use std::{net::SocketAddr,sync::{Arc,RwLock},thread,time::{SystemTime,UNIX_EPOCH}};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::handshake::server::{Request,Response};

fn wall_ms_test()->i64{SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as i64}
fn test_tls()->(native_tls::TlsAcceptor,native_tls::TlsConnector){
 let key=PKey::from_rsa(Rsa::generate(2048).unwrap()).unwrap();let mut name=X509NameBuilder::new().unwrap();name.append_entry_by_text("CN","127.0.0.1").unwrap();let name=name.build();let mut b=X509::builder().unwrap();b.set_version(2).unwrap();let serial=BigNum::from_u32(1).unwrap().to_asn1_integer().unwrap();b.set_serial_number(&serial).unwrap();b.set_subject_name(&name).unwrap();b.set_issuer_name(&name).unwrap();b.set_pubkey(&key).unwrap();b.set_not_before(Asn1Time::days_from_now(0).unwrap().as_ref()).unwrap();b.set_not_after(Asn1Time::days_from_now(1).unwrap().as_ref()).unwrap();b.append_extension(BasicConstraints::new().critical().ca().build().unwrap()).unwrap();let san=SubjectAlternativeName::new().ip("127.0.0.1").build(&b.x509v3_context(None,None)).unwrap();b.append_extension(san).unwrap();b.sign(&key,MessageDigest::sha256()).unwrap();let cert=b.build();let cert_pem=cert.to_pem().unwrap();let key_pem=key.private_key_to_pem_pkcs8().unwrap();let identity=native_tls::Identity::from_pkcs8(&cert_pem,&key_pem).unwrap();let acceptor=native_tls::TlsAcceptor::new(identity).unwrap();let root=native_tls::Certificate::from_pem(&cert_pem).unwrap();let connector=native_tls::TlsConnector::builder().add_root_certificate(root).build().unwrap();(acceptor,connector)
}

#[tokio::test(flavor="multi_thread",worker_threads=4)]
async fn r1333_real_reader_queue_engine_offline_websocket_replay(){r1333_replay(false).await;}
#[tokio::test(flavor="multi_thread",worker_threads=4)]
async fn r1402_worker_runtime_local_wss(){r1333_replay(true).await;}
async fn r1333_replay(use_worker:bool){
 let std_listener=std::net::TcpListener::bind("127.0.0.1:0").unwrap();std_listener.set_nonblocking(true).unwrap();let addr:SocketAddr=std_listener.local_addr().unwrap();let(acceptor,connector)=test_tls();
 let server=thread::spawn(move||{
  let deadline=std::time::Instant::now()+Duration::from_secs(5);let mut sessions=Vec::new();
  while sessions.len()<6&&std::time::Instant::now()<deadline{
   let(stream,_)=match std_listener.accept(){Ok(x)=>x,Err(e) if e.kind()==std::io::ErrorKind::WouldBlock=>{thread::sleep(Duration::from_millis(5));continue},Err(e)=>panic!("local accept: {e}")};let _=stream.set_read_timeout(Some(Duration::from_secs(2)));let _=stream.set_write_timeout(Some(Duration::from_secs(2)));
   let tls=acceptor.accept(stream).unwrap();let mut path=String::new();let mut ws=tokio_tungstenite::tungstenite::accept_hdr(tls,|req:&Request,response:Response|{path=req.uri().path().to_string();Ok(response)}).unwrap();let is_okx=path.ends_with("/okx");
   if is_okx{let _=ws.read().unwrap();let ack=json!({"event":"subscribe","arg":{"channel":"books","instId":"BTC-USDT"},"code":"0","msg":""}).to_string();ws.send(tokio_tungstenite::tungstenite::Message::Text(ack)).unwrap();}sessions.push((ws,is_okx));
  }
  let accepted=sessions.len();let frame_count=if use_worker{8}else{std::env::var("R1333_REPLAY_FRAMES").ok().and_then(|v|v.parse::<i64>().ok()).unwrap_or(4).clamp(1,2048)};let pace_us=if use_worker{100_000}else{std::env::var("R1333_REPLAY_PACE_US").ok().and_then(|v|v.parse::<u64>().ok()).unwrap_or(5_000)};let base_ms=wall_ms_test();let hold_ms=std::env::var("R1333_REPLAY_HOLD_MS").ok().and_then(|v|v.parse::<u64>().ok()).unwrap_or(250);
  let senders:Vec<_>=sessions.into_iter().map(|(mut ws,is_okx)|thread::spawn(move||{for n in 0..frame_count{let event_offset_ms=if pace_us==0{0}else{(n as u64).saturating_mul(pace_us)/1000};let now=base_ms+event_offset_ms as i64;let raw=if is_okx{let action=if n==0{"snapshot"}else{"update"};let seq=10+n;json!({"arg":{"channel":"books","instId":"BTC-USDT"},"action":action,"data":[{"ts":now.to_string(),"seqId":seq,"prevSeqId":if n==0{-1}else{seq-1},"bids":[["100","3"]],"asks":[["101","3"]]}]}).to_string()}else{let u=100+n;json!({"stream":"btcusdt@depth20@100ms","data":{"s":"BTCUSDT","e":"depthUpdate","E":now,"T":now,"U":u,"u":u,"pu":u-1,"b":[["100","3"],["99","2"]],"a":[["101","3"],["102","2"]]}}).to_string()};if ws.send(tokio_tungstenite::tungstenite::Message::Text(raw)).is_err(){return}if pace_us>0{thread::sleep(Duration::from_micros(pace_us));}}thread::sleep(Duration::from_millis(hold_ms));let _=ws.close(None);})).collect();for t in senders{let _=t.join();}accepted
 });
 let endpoint=R1333OfflineEndpoint{url_template:format!("wss://{addr}/r1333/{{venue}}"),connector};let(out_tx,_out_rx)=mpsc::channel(512);let authority:Authority=Arc::new(RwLock::new(HashMap::new()));let cfg=Config{shard:733,bases:vec!["BTC".into()],copies:3,duration:Duration::from_secs(3),controlled_close_after:None,endpoint_variant:0,probe_rotation_after:None};
 let summary=if use_worker{run_internal_impl(cfg,None,None,authority,out_tx,true,Some(endpoint)).await.unwrap()}else{run_internal_offline(cfg,authority,out_tx,endpoint).await.unwrap()};let accepted=tokio::time::timeout(Duration::from_secs(2),tokio::task::spawn_blocking(move||server.join().unwrap())).await.unwrap().unwrap();
 let socket_diag:Vec<_>=summary.sockets.iter().map(|s|serde_json::json!({"venue":s.venue,"url":s.url,"end":s.end,"received":s.received,"tcp_connect_ns":s.tcp_connect_ns,"tls_ws_handshake_ns":s.tls_ws_handshake_ns})).collect();let expected=accepted as u64*if use_worker{8}else{std::env::var("R1333_REPLAY_FRAMES").ok().and_then(|v|v.parse::<u64>().ok()).unwrap_or(4)};
 if !use_worker{if let Ok(path)=std::env::var("R1333_REPLAY_JSON"){std::fs::write(path,serde_json::to_vec_pretty(&summary).unwrap()).unwrap();}}assert_eq!(accepted,6,"expected three local connections per venue");assert_eq!(summary.reader_received_total,expected,"reader frame census mismatch; sockets={}",serde_json::to_string(&socket_diag).unwrap());assert!(!summary.engine_batch_service_samples_ns.is_empty(),"Engine::receive_batch was not observed");assert_eq!(summary.reader_frame_queue_capacity_per_session,READER_FRAME_QUEUE_CAPACITY);assert_eq!(summary.central_frame_queue_capacity,CENTRAL_FRAME_QUEUE_CAPACITY);assert_eq!(summary.orders,0,"offline replay must never create orders");if use_worker{
  assert_eq!(summary.engine["r1402_ucm_worker"],true);
  assert_eq!(summary.counts["frames_received"],expected);
  assert!(summary.engine["submitted"].as_u64().unwrap_or(0)>0,"actual worker-backed runtime must publish data; engine={}",summary.engine);
  for venue in ["BinanceFutures","OkxSpot"]{assert!(summary.events.iter().any(|e|e["kind"]=="selection"&&e["venue"]==venue),"no worker selection on {venue}");}
  println!("R1402_WSS reader={} engine={} submitted={} orders={} central_queue_max={}",summary.reader_received_total,summary.counts["frames_received"],summary.engine["submitted"],summary.orders,summary.central_queue_depth_max);
 }println!("R1333_INVALID_DIAG categories={} events={} invalid={:?} duplicates={:?}",summary.engine.get("invalid_categories").unwrap_or(&serde_json::Value::Null),summary.events.len(),summary.engine.get("invalid"),summary.engine.get("duplicates"));assert_eq!(summary.engine.get("invalid_categories").and_then(|v|v.get("unknown_source")).and_then(|v|v.as_u64()).unwrap_or(0),0,"unknown source; categories={} events={}",summary.engine.get("invalid_categories").unwrap_or(&serde_json::Value::Null),summary.events.len());
}


// R1402J: actual localhost TLS/WS -> Forwarder -> Queue32 -> Engine -> UCM.
// Original universe and queue bounds, deterministic payloads and paced load.
#[tokio::test(flavor="multi_thread",worker_threads=4)]
async fn r1402j_full_universe_paced_worker_stress(){
 assert_eq!(std::env::var("R1248_BINANCE_GROUP_SIZE").as_deref(),Ok("10"));
 assert_eq!(std::env::var("R1402_UCM_PROFILE").as_deref(),Ok("1"));
 let bases=include_str!("../../INSTRUMENT_UNIVERSE.csv").lines().skip(1).map(|r|r.split(',').next().unwrap().to_owned()).collect::<Vec<_>>();
 assert_eq!(bases.len(),40);
 let expected_sockets=16;
 let groups=bases.chunks(10).map(|g|g.to_vec()).collect::<Vec<_>>();
 let listener=std::net::TcpListener::bind("127.0.0.1:0").unwrap();listener.set_nonblocking(true).unwrap();
 let addr=listener.local_addr().unwrap();let(acceptor,connector)=test_tls();
 let server=thread::spawn(move||{
  let deadline=std::time::Instant::now()+Duration::from_secs(4);let mut sessions=Vec::new();
  while sessions.len()<expected_sockets&&std::time::Instant::now()<deadline{
   let(stream,_)=match listener.accept(){Ok(x)=>x,Err(e) if e.kind()==std::io::ErrorKind::WouldBlock=>{thread::sleep(Duration::from_millis(2));continue},Err(e)=>panic!("local accept {e}")};
   stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();stream.set_write_timeout(Some(Duration::from_secs(2))).unwrap();
   let tls=acceptor.accept(stream).unwrap();let mut path=String::new();
   let mut ws=tokio_tungstenite::tungstenite::accept_hdr(tls,|req:&Request,response:Response|{path=req.uri().path().to_owned();Ok(response)}).unwrap();
   let parts=path.split('/').collect::<Vec<_>>();assert_eq!(parts.len(),4);
   let okx=parts[2]=="okx";let lane=parts[3].parse::<usize>().unwrap();assert!(lane<8);
   let members=groups[lane/2].clone();
   if okx{
    let msg=ws.read().unwrap().into_text().unwrap();let sub:serde_json::Value=serde_json::from_str(&msg).unwrap();
    let actual=sub["args"].as_array().unwrap().iter().map(|v|v["instId"].as_str().unwrap().to_owned()).collect::<Vec<_>>();
    assert_eq!(actual,members.iter().map(|b|format!("{b}-USDT")).collect::<Vec<_>>());
    for base in &members{
     ws.send(tokio_tungstenite::tungstenite::Message::Text(json!({"event":"subscribe","arg":{"channel":"books","instId":format!("{base}-USDT")},"code":"0","msg":""}).to_string())).unwrap();
    }
   }
   sessions.push((ws,okx,lane,members));
  }
  let accepted=sessions.len();
  let start=std::time::Instant::now()+Duration::from_millis(150);
  let senders=sessions.into_iter().map(|(mut ws,okx,lane,members)|thread::spawn(move||{
   let mut sent=[0u64;2];let mut max_late_ns=0u64;
   for n in 0..40i64 {
    let (phase,round_start,spacing_us)=if n<20{(0,n as u64*200_000,20_000)}else{(1,4_000_000+(n as u64-20)*100_000,10_000)};
    for (i,base) in members.iter().enumerate(){
     let slot=lane+if okx{8}else{0};let target=start+Duration::from_micros(round_start+i as u64*spacing_us+slot as u64*spacing_us/16);
     let now=std::time::Instant::now();if target>now{thread::sleep(target-now);}
     max_late_ns=max_late_ns.max(std::time::Instant::now().saturating_duration_since(target).as_nanos() as u64);
     let event=wall_ms_test();let seq=100+n;
     let raw=if okx{json!({"arg":{"channel":"books","instId":format!("{base}-USDT")},"action":if n==0{"snapshot"}else{"update"},"data":[{"ts":event.to_string(),"seqId":seq,"prevSeqId":if n==0{-1}else{seq-1},"bids":[["100","3"]],"asks":[["101","3"]]}]})}
      else{json!({"stream":format!("{}usdt@depth20@100ms",base.to_ascii_lowercase()),"data":{"s":format!("{base}USDT"),"e":"depthUpdate","E":event,"T":event,"U":seq,"u":seq,"pu":seq-1,"b":[["100","3"],["99","2"]],"a":[["101","3"],["102","2"]]}})};
     if ws.send(tokio_tungstenite::tungstenite::Message::Text(raw.to_string())).is_err(){return(sent,max_late_ns);}
     sent[phase]+=1;
    }
   }
   // Do not create a reconnect during the measured feed.
   thread::sleep(Duration::from_secs(4));let _=ws.close(None);(sent,max_late_ns)
  })).collect::<Vec<_>>();
  let mut sent=[0u64;2];let mut late=0;
  for t in senders{let (counts,m)=t.join().unwrap();sent[0]+=counts[0];sent[1]+=counts[1];late=late.max(m);}
  (accepted,sent,late)
 });
 let endpoint=R1333OfflineEndpoint{url_template:format!("wss://{addr}/r1402j/{{venue}}/{{lane}}"),connector};
 let (tx,mut rx)=mpsc::channel(64);let authority:Authority=Arc::new(RwLock::new(HashMap::new()));
 let consumer=tokio::spawn(async move{
  let mut scopes=std::collections::BTreeMap::<String,u64>::new();let mut frames=0u64;let mut max_age=0u64;
  while let Some(o)=rx.recv().await{
   let pair=match o{Output::Book(f,u)=>Some((f,u)),Output::Frame(f)=>crate::parse(&f).ok().map(|u|(f,u)),_=>None};
   if let Some((f,u))=pair{frames+=1;*scopes.entry(key(f.venue,&u.base)).or_default()+=1;max_age=max_age.max(mono_ns().saturating_sub(f.received_ns));}
  }
  (frames,scopes,max_age)
 });
 let cfg=Config{shard:734,bases,copies:2,duration:Duration::from_secs(9),controlled_close_after:None,endpoint_variant:0,probe_rotation_after:None};
 let started=std::time::Instant::now();
 let result=run_internal_impl(cfg,None,None,authority,tx,true,Some(endpoint)).await;
 let elapsed=started.elapsed().as_millis();
 let (frames,scopes,max_age)=consumer.await.unwrap();
 let (accepted,sent,late)=tokio::time::timeout(Duration::from_secs(15),tokio::task::spawn_blocking(move||server.join().unwrap())).await.unwrap().unwrap();
 println!("R1402J_STRESS {}",json!({"accepted":accepted,"sent_800":sent[0],"sent_1600":sent[1],"producer_late_max_ns":late,"runtime_ms":elapsed,"published":frames,"published_scopes":scopes.len(),"publish_receive_age_max_ns":max_age,"runtime_error":result.as_ref().err()}));
 let summary=result.expect("full-universe worker runtime failed");
 println!("R1402J_CENSUS {}",json!({"reader":summary.reader_received_total,"engine":summary.counts["frames_received"],"invalid":summary.engine["invalid"],"submitted":summary.engine["submitted"],"orders":summary.orders,"queue16_max":summary.peak_reader_frame_queue_depth,"queue32_max":summary.central_queue_depth_max,"reader_overflows":summary.reader_queue_overflow_events}));
 assert_eq!(accepted,expected_sockets);assert_eq!(sent,[3200,3200]);assert_eq!(summary.reader_received_total,6400);assert_eq!(summary.counts["frames_received"],6400);assert_eq!(summary.orders,0);assert_eq!(summary.reader_queue_overflow_events,0);
 assert_eq!(scopes.len(),80,"not all scopes published");
 assert_eq!(summary.engine["submitted"],frames);assert_eq!(summary.engine["r1402_ucm_worker"],true);
 assert_eq!(summary.reader_frame_queue_capacity_per_session,16);assert_eq!(summary.central_frame_queue_capacity,32);
 if let Ok(path)=std::env::var("R1402J_SUMMARY"){std::fs::write(path,serde_json::to_vec(&summary).unwrap()).unwrap();}
}
