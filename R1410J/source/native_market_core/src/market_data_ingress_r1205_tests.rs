use super::*;
use crate::native_r1173_compact_writer::{Writer,Inject};
use crate::native_r1173_compact_writer::ingress_store_r1205::digest;
use std::{path::PathBuf,os::unix::fs::DirBuilderExt,time::{SystemTime,UNIX_EPOCH}};
struct Fixture{engine:Engine,store:SharedStore,auth:Authority,owners:Vec<crate::Owner>,writer:Option<Writer>,root:PathBuf,db:PathBuf,published:u64}
impl Fixture{
 fn new()->Self{let nonce=SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();let root=std::env::temp_dir().join(format!("r1205_inbox_{}_{}",std::process::id(),nonce));std::fs::DirBuilder::new().mode(0o700).create(&root).unwrap();let db=std::env::temp_dir().join(format!("spotarb_r1173_r1205_{}_{}",std::process::id(),nonce));std::fs::DirBuilder::new().mode(0o700).create(&db).unwrap();let mut inbox=Store::open(&root).unwrap();inbox.bind_canonical(&db).unwrap();let store=Arc::new(Mutex::new(inbox));let auth=Arc::new(RwLock::new(HashMap::new()));let mut engine=Engine::new(store.clone(),auth.clone(),vec![key(Venue::BinanceFutures,"BTC")]).unwrap();engine.clock=None;for s in ["a","b","c"]{engine.register(s).unwrap();}Self{engine,store,auth,owners:(0..12).map(|i|crate::Owner::new(i,12,800).unwrap()).collect(),writer:Some(Writer::create(&db).unwrap()),root,db,published:0}}
 fn handle(&mut self,out:Vec<Output>){
  for item in out{
   let (item,normalized)=match item{Output::Book(f,u)=>(Output::Frame(f),Some(u)),other=>(other,None)};
   match item{
    Output::Reset{venue,base,epoch}=>{self.writer.as_mut().unwrap().source_reset(&mut self.owners,venue,&[base],epoch,1_790_000_000_000,"R1162_INFERRED_OFFLINE",Inject::None).unwrap();},
    Output::Frame(mut f)=>{
     let at=f.source.as_ref().and_then(|p|p.selected_ns).unwrap_or(f.received_ns);
     let now_wall=f.received_wall_ms+(at.saturating_sub(f.received_ns)/1_000_000)as i64;
     let u=normalized.unwrap_or_else(||parse(&f).unwrap());
     if crate::market_data_ingress_r1205::authorization_at_normalized(&self.auth,&f,&u,at,now_wall).is_none(){
      self.store.lock().unwrap().finish(&f,"quarantined","fenced",None).unwrap();continue
     }
     let ix=crate::owner_index(&u.base,12);let w=self.writer.as_mut().unwrap();
     w.source_commit_guarded(&mut self.owners,&[(f.clone(),u,ix)],&[],Inject::None).unwrap();
     w.note_guarded_decision(&f,"DECISION_DENIED",&digest(b"SOURCE_ONLY_NO_EXECUTION")).unwrap();
     let verified=w.verify_ingress_batch(std::slice::from_ref(&f)).unwrap();
     let publish_start=crate::ingress_runtime_r1205::mono_ns();if let Some(t)=f.source.as_mut().and_then(|s|s.r1279_timing.as_mut()){t.publish_durable_start_ns=Some(publish_start);}
     let committed={self.store.lock().unwrap().finish_published_batch_verified(std::slice::from_ref(&f),&verified).unwrap()};
     let publish_end=crate::ingress_runtime_r1205::mono_ns();if let Some(t)=f.source.as_mut().and_then(|s|s.r1279_timing.as_mut()){t.publish_durable_end_ns=Some(publish_end);t.ack_start_ns=Some(publish_end);}
     let ticket=w.prepare_ingress_release_after_publish(&verified,&committed).unwrap().unwrap();
     w.commit_ingress_release(ticket).unwrap();let acked=w.confirm_ingress_reclaimed(&committed).unwrap();
     let ack_end=crate::ingress_runtime_r1205::mono_ns();let terminal=crate::ingress_runtime_r1205::mono_ns();if let Some(t)=f.source.as_mut().and_then(|s|s.r1279_timing.as_mut()){t.ack_end_ns=Some(ack_end);t.terminal_ns=Some(terminal);}
     let traces=f.source.as_ref().and_then(|s|s.r1279_timing.as_ref().map(|t|(s.accepted_id,serde_json::to_string(&serde_json::json!({"publish_durable_end_ns":t.publish_durable_end_ns,"ack_start_ns":t.ack_start_ns,"ack_end_ns":t.ack_end_ns,"terminal_ns":t.terminal_ns})).unwrap())));
     self.store.lock().unwrap().forget_reclaimed_after_ack_with_trace(&acked,&traces.into_iter().collect::<Vec<_>>()).unwrap();self.published+=1;
    },
    Output::Blocked{..}=>{},
    Output::Resubscribe{..}=>{},
    Output::Book(_,_)=>unreachable!("normalized above"),
   }
  }
 }
 fn feed(&mut self,session:&str,t:u64,u:i64){let out=self.engine.receive_batch(vec![frame(session,t,u)],ns(t),wall(t)).unwrap();self.handle(out);}
 fn warm(&mut self){for t in [0,400,800]{for s in ["a","b","c"]{self.feed(s,t,t as i64+1)}}}
 fn invariant(&self,label:&str){let c=self.store.lock().unwrap().counts().unwrap();assert_eq!(c.accepted,c.published+c.quarantined+c.pending);println!("R1205_CASE {}",serde_json::json!({"case":label,"counts":c,"db1_db2_ack_parity":self.writer.as_ref().unwrap().verify_pair().is_ok(),"custody":self.writer.as_ref().unwrap().source_custody_counts().unwrap(),"orders":0}));}
}
impl Drop for Fixture{fn drop(&mut self){self.writer.take();let _=std::fs::remove_dir_all(&self.root);let _=std::fs::remove_dir_all(&self.db);}}
fn finish_replay_pending(f: &Fixture) {
    let pending = f.store.lock().unwrap().pending().unwrap();
    if !pending.is_empty() {
        let rows = pending.into_iter().map(|frame|
            (frame, "r1234_offline_replay_end".to_string())
        ).collect::<Vec<_>>();
        f.store.lock().unwrap().quarantine_batch(&rows).unwrap();
    }
}

fn ns(t:u64)->u64{1_000_000_000+t*1_000_000}
fn wall(t:u64)->i64{1_790_000_000_000+t as i64}
fn frame(s:&str,t:u64,u:i64)->RawFrame{RawFrame{venue:Venue::BinanceFutures,epoch:1,received_ns:ns(t),received_wall_ms:wall(t),source:Some(provenance(s.into(),1)),raw:serde_json::json!({"stream":"btcusdt@depth20@500ms","data":{"s":"BTCUSDT","e":"depthUpdate","E":wall(t)-50,"u":u,"U":u,"pu":u-1,"b":[["100","3"],["99","2"]],"a":[["101","3"],["102","2"]]}}).to_string()}}
#[test]
fn r1246_scoped_connection_registration_fences_unassigned_instruments(){
 let auth:Authority=Arc::new(RwLock::new(HashMap::new()));
 let mut engine=Engine::new_volatile(auth,vec![
  key(Venue::OkxSpot,"BTC"),key(Venue::OkxSpot,"ETH")
 ]).unwrap();
 let session="g0-OkxSpot-0-1-scope";
 engine.register_connection_scoped(session,Venue::OkxSpot,1,&["BTC".to_owned()]).unwrap();
 assert!(engine.manager_ids.contains_key(&(session.into(),key(Venue::OkxSpot,"BTC"))));
 assert!(!engine.manager_ids.contains_key(&(session.into(),key(Venue::OkxSpot,"ETH"))));
 let unexpected=RawFrame{
  venue:Venue::OkxSpot,epoch:1,received_ns:ns(1),received_wall_ms:wall(1),
  source:Some(provenance(session.into(),1)),
  raw:serde_json::json!({"arg":{"channel":"books","instId":"ETH-USDT"},"data":[{"ts":wall(1).to_string()}]}).to_string()
 };
 assert!(engine.ensure_manager_source(&unexpected,"ETH").is_err());
}

#[test]
fn r1246_volatile_engine_uses_live_ucm_without_sqlite_ingress(){
 let auth:Authority=Arc::new(RwLock::new(HashMap::new()));
 let mut engine=Engine::new_volatile(auth,vec![key(Venue::BinanceFutures,"BTC")]).unwrap();
 engine.clock=None;
 for session in ["a","b","c"]{engine.register(session).unwrap();}
 assert!(engine.store.is_none());
 let profile=engine.durable_writer_profile();
 assert_eq!(profile["mode"],"VOLATILE_RAM_ONLY");
 assert_eq!(profile["sqlite_commit_in_ingress"],false);
 assert_eq!(profile["source_provenance"],"RAM_SESSION_NO_DURABLE_RECEIPT");
 assert_eq!(profile["canonical_receipt"],false);
 let mut output_count=0usize;
 let mut owner_count=0usize;
 let mut owners=(0..12).map(|i|crate::Owner::new(i,12,800).unwrap()).collect::<Vec<_>>();
 for t in [0,400,800]{for session in ["a","b","c"]{
  let out=engine.receive_batch(vec![frame(session,t,t as i64+1)],ns(t),wall(t)).unwrap();
  assert!(!out.iter().any(|item|matches!(item,Output::Blocked{..})));
  for item in &out{if let Output::Frame(frame)=item{
   let source=frame.source.as_ref().unwrap();
   assert!(source.journal_id.is_empty(),"RAM provenance must not claim a durable journal");
   assert_eq!(source.accepted_id,0,"RAM provenance must not fabricate an accepted receipt ID");
   assert!(source.selected_permit.is_some(),"RAM source must retain live selection capability");
   let update=parse(frame).unwrap();
   let owner=crate::owner_index(&update.base,12);
   assert!(owners[owner].prepare_parsed(frame,update).is_ok(),"volatile frame must satisfy existing Owner provenance contract");
   owner_count+=1;
  }}
  output_count+=out.len();
 }}
 let coverage=engine.coverage(ns(800),wall(800));
 assert_eq!(coverage[0]["hot_ready"].as_array().unwrap().len(),3);
 assert!(coverage[0]["active"].is_string());
 assert_eq!(engine.metrics.received,9);
 assert!(output_count>0);
 assert!(owner_count>0);
}

fn okx_books_frame(session:&str,t:u64,action:&str,seq:i64,prev:Option<i64>,bid_qty:&str)->RawFrame{
 let prev=prev.map(|n|format!(",\"prevSeqId\":{n}")).unwrap_or_else(|| if action=="snapshot" { ",\"prevSeqId\":-1".into() } else { String::new() });
 let raw=format!(r#"{{"arg":{{"channel":"books","instId":"BTC-USDT"}},"action":"{action}","data":[{{"ts":"{}","seqId":{seq}{prev},"bids":[["100","{bid_qty}"],["99","2"]],"asks":[["101","3"],["102","2"]]}}]}}"#,wall(t)-50);
 RawFrame{venue:Venue::OkxSpot,epoch:1,received_ns:ns(t),received_wall_ms:wall(t),raw,source:Some(provenance(session.into(),1))}
}
fn okx_empty_books_frame(session:&str,t:u64,seq:i64)->RawFrame{RawFrame{venue:Venue::OkxSpot,epoch:1,received_ns:ns(t),received_wall_ms:wall(t),source:Some(provenance(session.into(),1)),raw:serde_json::json!({"arg":{"channel":"books","instId":"BTC-USDT"},"action":"update","data":[{"ts":(wall(t)-50).to_string(),"seqId":seq,"prevSeqId":seq,"bids":[],"asks":[]}]}).to_string()}}
#[test]
fn r1388_forwarder_prepared_binance_and_okx_match_original_engine_admission() {
    fn kind(out:&Output)->(&'static str,Option<String>){
        match out {
            Output::Reset{base,..}=>("reset",Some(base.clone())),
            Output::Frame(f)=>("frame",f.source.as_ref().map(|s|s.session_id.clone())),
            Output::Book(f,_)=>("book",f.source.as_ref().map(|s|s.session_id.clone())),
            Output::Blocked{..}=>("blocked",None),
            Output::Resubscribe{..}=>("resubscribe",None),
        }
    }
    for venue in [Venue::BinanceFutures,Venue::OkxSpot] {
        let key=key(venue,"BTC");
        let make=||{
            let authority:Authority=Arc::new(RwLock::new(HashMap::new()));
            let mut e=Engine::new_volatile(authority,vec![key.clone()]).unwrap();
            e.clock=None;
            for session in ["a","b","c"] {e.register(session).unwrap();}
            e
        };
        let mut direct=make();
        let mut forwarded=make();
        let mut workers=HashMap::<String,MarketDataProcessor>::new();
        for t in [0u64,400,800] {
            for session in ["a","b","c"] {
                let f=if venue==Venue::BinanceFutures {
                    frame(session,t,t as i64+1)
                }else{
                    let seq=if t==0{10}else if t==400{11}else{12};
                    okx_books_frame(session,t,if t==0{"snapshot"}else{"update"},
                        seq,if t==0{None}else{Some(seq-1)},if t==0{"1"}else{"2"})
                };
                let mut prepared_frame=f.clone();
                let prepared=workers.entry(session.into()).or_default().prepare_forwarded(&prepared_frame);
                assert!(prepared.parsed.as_ref().unwrap().is_ok(),
                    "Forwarder must produce a typed book for both exchanges");
                prepared_frame.source.as_mut().unwrap().prepared=Some(Box::new(prepared));
                let direct_output=direct.receive_batch(vec![f],ns(t),wall(t)).unwrap();
                let prepared_output=forwarded.receive_batch(vec![prepared_frame],ns(t),wall(t)).unwrap();
                assert_eq!(direct_output.iter().map(kind).collect::<Vec<_>>(),
                    prepared_output.iter().map(kind).collect::<Vec<_>>(),
                    "offload changed Engine output at {venue:?} {session} {t}");
                assert_eq!(direct.metrics.received,forwarded.metrics.received);
                assert_eq!(direct.metrics.invalid,forwarded.metrics.invalid);
                assert_eq!(direct.metrics.duplicates,forwarded.metrics.duplicates);
                assert_eq!(direct.metrics.switches,forwarded.metrics.switches);
            }
        }
        if venue==Venue::OkxSpot {
            assert_eq!(forwarded.market_data_processor.memory_usage_bytes().0,0,
                "stateful L2 reconstruction must be owned by Forwarder, not Engine");
            assert_eq!(workers.values().map(|w|w.memory_usage_bytes().0).sum::<usize>(),3);
            assert_eq!(forwarded.prepared_chain_accepted.len(),3);
        }
    }
}

#[test]
fn r1388_unadmitted_okx_predecessor_cannot_grant_following_delta() {
    let authority:Authority=Arc::new(RwLock::new(HashMap::new()));
    let mut engine=Engine::new_volatile(authority,vec![key(Venue::OkxSpot,"BTC")]).unwrap();
    engine.clock=None;engine.register("a").unwrap();
    let mut worker=MarketDataProcessor::default();
    let mut snapshot=okx_books_frame("a",0,"snapshot",10,None,"1");
    let prepared=worker.prepare_forwarded(&snapshot);
    assert_eq!(prepared.chain.as_ref().unwrap().predecessor,None);
    snapshot.source.as_mut().unwrap().prepared=Some(Box::new(prepared));
    let _=engine.receive_batch(vec![snapshot],ns(0),wall(0)).unwrap();
    assert_eq!(engine.prepared_chain_accepted.len(),1);
    // Simulate an Engine-side rejection of that predecessor. Even though the
    // Forwarder speculatively applied the delta, it cannot gain Engine authority.
    engine.prepared_chain_accepted.clear();
    let mut delta=okx_books_frame("a",400,"update",11,Some(10),"2");
    let prepared=worker.prepare_forwarded(&delta);
    assert_eq!(prepared.chain.as_ref().unwrap().predecessor,Some(1));
    delta.source.as_mut().unwrap().prepared=Some(Box::new(prepared));
    let out=engine.receive_batch(vec![delta],ns(400),wall(400)).unwrap();
    assert!(out.iter().any(|item|matches!(item,Output::Resubscribe{..})),
        "rejected speculative predecessor must resubscribe instead of granting HOT");
    assert!(!engine.prepared_chain_accepted.contains_key(&("a".into(),"BTC".into())));
}
#[test]fn r1211_books_delta_is_custodied_and_reaches_owner_as_reconstructed_book(){
 let mut f=Fixture::new();f.engine.universe.insert(key(Venue::OkxSpot,"BTC"));
 assert_eq!(f.store.lock().unwrap().next_epoch().unwrap(),1,"force selected generation above the socket generation");
 let mut emitted=0usize;
 for session in ["a","b","c"]{
  for (t,action,seq,prev,qty) in [(0,"snapshot",10,None,"1"),(400,"update",11,Some(10),"2"),(800,"update",12,Some(11),"3")]{
   let out=f.engine.receive_batch(vec![okx_books_frame(session,t,action,seq,prev,qty)],ns(t),wall(t)).unwrap();
   emitted+=out.iter().filter(|o|matches!(o,Output::Book(_,_) )).count();
   for item in &out{if let Output::Book(frame,update)=item{assert_eq!(frame.epoch,update.book.epoch);}}
   f.handle(out);
  }
 }
 assert!(emitted>0);assert!(f.published>0);
 assert!(f.writer.as_ref().unwrap().verify_pair().is_ok());
 f.invariant("R1211_books_snapshot_delta_owner_db1_db2_ack");
}
#[test]
fn r1269_stale_advancing_okx_delta_requests_resnapshot_before_gap(){
 let mut f=Fixture::new();f.engine.universe.insert(key(Venue::OkxSpot,"BTC"));
 for session in ["a","b","c"]{
  for (t,action,seq,prev,qty) in [(0,"snapshot",10,None,"1"),(400,"update",11,Some(10),"2"),(800,"update",12,Some(11),"3")]{
   let out=f.engine.receive_batch(vec![okx_books_frame(session,t,action,seq,prev,qty)],ns(t),wall(t)).unwrap();
   f.handle(out);
  }
 }
 assert_eq!(f.engine.market_data_processor.diagnostic_state("a","BTC").map(|v|v.0),Some(12));
 let mut stale_duplicate=okx_books_frame("a",850,"update",12,Some(11),"3");
 let mut duplicate_json:serde_json::Value=serde_json::from_str(&stale_duplicate.raw).unwrap();
 duplicate_json["data"][0]["ts"]=(wall(850)-901).to_string().into();stale_duplicate.raw=duplicate_json.to_string();
 let duplicate_out=f.engine.receive_batch(vec![stale_duplicate],ns(850),wall(850)).unwrap();
 assert!(!duplicate_out.iter().any(|o|matches!(o,Output::Resubscribe{..})),"stale non-advancing duplicate should not force recovery");
 assert!(!f.engine.resyncing.contains(&("a".to_string(),"BTC".to_string())));
 let mut stale=okx_books_frame("a",900,"update",13,Some(12),"4");
 let mut stale_json:serde_json::Value=serde_json::from_str(&stale.raw).unwrap();
 stale_json["data"][0]["ts"]=(wall(900)-901).to_string().into();stale.raw=stale_json.to_string();
 let out=f.engine.receive_batch(vec![stale],ns(900),wall(900)).unwrap();
 assert!(out.iter().any(|o|matches!(o,Output::Resubscribe{session,base,reason} if session=="a"&&base=="BTC"&&reason=="stale advancing delta")));
 assert!(f.engine.resyncing.contains(&("a".to_string(),"BTC".to_string())));
 assert_eq!(f.engine.market_data_processor.diagnostic_state("a","BTC").map(|v|v.0),Some(12),"stale frame must not mutate the published L2 state");
 assert!(f.engine.metrics.invalid_categories.get("market_event_stale_or_clock_skew").copied().unwrap_or(0)>0);
 let stale_audit=f.engine.stale_frame_audit();let stale_row=stale_audit.last().unwrap();
 assert_eq!(stale_row["processing_delay_ns"].as_u64(),Some(0));
 assert_eq!(stale_row["exchange_age_at_processing_ms"].as_i64(),Some(901));
 assert_eq!(stale_row["prev_seq_id"].as_i64(),Some(12));
 assert_eq!(stale_row["pre_apply_l2_sequence"].as_i64(),Some(12));
 assert_eq!(stale_row["freshness_reject_reason"],"exchange_age_over_800ms_or_clock_skew");
 let gap=okx_books_frame("a",1000,"update",14,Some(13),"5");
 let out=f.engine.receive_batch(vec![gap],ns(1000),wall(1000)).unwrap();
 assert!(!out.iter().any(|o|matches!(o,Output::Book(_,_)|Output::Frame(_))));
 assert!(!out.iter().any(|o|matches!(o,Output::Resubscribe{..})),"rate-limited early resync must not duplicate on the next gap");
 assert_eq!(f.engine.market_data_processor.diagnostic_state("a","BTC").map(|v|v.0),Some(12));
 let snapshot=okx_books_frame("a",1100,"snapshot",100,None,"6");
 let out=f.engine.receive_batch(vec![snapshot],ns(1100),wall(1100)).unwrap();f.handle(out);
 assert_eq!(f.engine.market_data_processor.diagnostic_state("a","BTC").map(|v|v.0),Some(100));
 let recovered=okx_books_frame("a",1200,"update",101,Some(100),"7");
 let out=f.engine.receive_batch(vec![recovered],ns(1200),wall(1200)).unwrap();f.handle(out);
 assert_eq!(f.engine.market_data_processor.diagnostic_state("a","BTC").map(|v|v.0),Some(101));
 f.invariant("stale_okx_delta_sequence_fenced_until_fresh_snapshot");
}

#[test]
fn r1269_actual_okx_stale_delta_replay_triggers_early_resync_without_false_hot(){
 let fixture:serde_json::Value=serde_json::from_str(include_str!("fixtures/r1269_okx_stale_gap_replay.json")).unwrap();
 let source=fixture["source"].as_str().unwrap().to_string();
 let base=fixture["instrument"].as_str().unwrap().to_string();
 let expected=fixture["expected_previous_seq"].as_i64().unwrap();
 let stale_raw=fixture["stale_delta"]["raw"].as_str().unwrap().to_string();
 let gap_raw=fixture["following_gap_delta"]["raw"].as_str().unwrap().to_string();
 let stale_json:serde_json::Value=serde_json::from_str(&stale_raw).unwrap();
 let stale_ts=stale_json["data"][0]["ts"].as_str().unwrap().parse::<i64>().unwrap();
 let mut f=Fixture::new();
 f.engine.universe.insert(key(Venue::OkxSpot,&base));
 f.engine.register(&source).unwrap();
 let seed=RawFrame{venue:Venue::OkxSpot,epoch:1,received_ns:ns(800),received_wall_ms:wall(800),
  source:None,raw:serde_json::json!({"arg":{"channel":"books","instId":format!("{base}-USDT")},
   "action":"snapshot","data":[{"ts":(stale_ts-100).to_string(),"seqId":expected,"prevSeqId":-1,
    "bids":[["10","2"]],"asks":[["10.1","3"]]}]}).to_string()};
 f.engine.market_data_processor.prepare_update(&source,&seed,None,true).unwrap();
 let stale=RawFrame{venue:Venue::OkxSpot,epoch:1,received_ns:ns(900),received_wall_ms:stale_ts+840,
  source:Some(provenance(source.clone(),1)),raw:stale_raw};
 let out=f.engine.receive_batch(vec![stale],ns(900),stale_ts+840).unwrap();
 assert!(out.iter().any(|o|matches!(o,Output::Resubscribe{session,base:b,reason}
  if session==&source&&b==&base&&reason=="stale advancing delta")));
 assert_eq!(f.engine.market_data_processor.diagnostic_state(&source,&base).map(|x|x.0),Some(expected));
 let mut stale_rows=f.engine.stale_frame_audit();
 let stale_row=stale_rows.pop().unwrap();
 assert_eq!(stale_row["sequence"].as_i64(),Some(fixture["stale_delta"]["seq"].as_i64().unwrap()));
 assert_eq!(stale_row["prev_seq_id"].as_i64(),Some(expected));
 assert_eq!(stale_row["pre_apply_l2_sequence"].as_i64(),Some(expected));
 assert_eq!(stale_row["exchange_age_at_processing_ms"].as_i64(),Some(840));
 let gap_json:serde_json::Value=serde_json::from_str(&gap_raw).unwrap();
 let gap_ts=gap_json["data"][0]["ts"].as_str().unwrap().parse::<i64>().unwrap();
 let gap=RawFrame{venue:Venue::OkxSpot,epoch:1,received_ns:ns(1000),received_wall_ms:gap_ts+100,
  source:Some(provenance(source.clone(),1)),raw:gap_raw};
 let out=f.engine.receive_batch(vec![gap],ns(1000),gap_ts+100).unwrap();
 assert!(!out.iter().any(|o|matches!(o,Output::Book(_,_)|Output::Frame(_)|Output::Resubscribe{..})));
 assert_eq!(f.engine.market_data_processor.diagnostic_state(&source,&base).map(|x|x.0),Some(expected));
 assert!(f.engine.resyncing.contains(&(source.clone(),base.clone())));
}
#[test]fn r1246_stale_recovery_snapshot_retries_with_cooldown(){
 let mut f=Fixture::new();f.engine.universe.insert(key(Venue::OkxSpot,"BTC"));
 for session in ["a","b","c"]{
  for (t,action,seq,prev) in [(0,"snapshot",10,None),(400,"update",11,Some(10)),(800,"update",12,Some(11))]{
   let out=f.engine.receive_batch(vec![okx_books_frame(session,t,action,seq,prev,"1")],ns(t),wall(t)).unwrap();f.handle(out);
  }
 }
 let gap=okx_books_frame("a",850,"update",14,Some(13),"2");
 let out=f.engine.receive_batch(vec![gap],ns(850),wall(850)).unwrap();
 assert!(out.iter().any(|o|matches!(o,Output::Resubscribe{session,reason,..} if session=="a"&&reason=="sequence gap")));
 let stale_snapshot=|t:u64|{let mut frame=okx_books_frame("a",t,"snapshot",100,None,"3");let mut v:serde_json::Value=serde_json::from_str(&frame.raw).unwrap();v["data"][0]["ts"]=(wall(t)-901).to_string().into();frame.raw=v.to_string();frame};
 let out=f.engine.receive_batch(vec![stale_snapshot(900)],ns(900),wall(900)).unwrap();
 assert!(!out.iter().any(|o|matches!(o,Output::Resubscribe{..})),"retry cooldown must suppress a rapid duplicate request");
 let retry_at=16_000;
 let out=f.engine.receive_batch(vec![stale_snapshot(retry_at)],ns(retry_at),wall(retry_at)).unwrap();
 assert!(out.iter().any(|o|matches!(o,Output::Resubscribe{session,base,reason} if session=="a"&&base=="BTC"&&reason=="stale initial snapshot")));
 let out=f.engine.receive_batch(vec![stale_snapshot(retry_at+100)],ns(retry_at+100),wall(retry_at+100)).unwrap();
 assert!(!out.iter().any(|o|matches!(o,Output::Resubscribe{..})),"retry cooldown must be per source and instrument");
 let recovered=okx_books_frame("a",retry_at+200,"snapshot",100,None,"4");
 let out=f.engine.receive_batch(vec![recovered],ns(retry_at+200),wall(retry_at+200)).unwrap();
 assert!(!out.iter().any(|o|matches!(o,Output::Resubscribe{..})));
 assert_eq!(f.engine.market_data_processor.diagnostic_state("a","BTC").map(|v|v.0),Some(100));
 assert!(!f.engine.resyncing.contains(&("a".to_string(),"BTC".to_string())));
 assert!(!f.engine.resubscribe_last_ns.contains_key(&("a".to_string(),"BTC".to_string())));
}
#[test]fn r1211_books_sequence_gap_quarantines_one_symbol_source_fail_closed(){
 let mut f=Fixture::new();f.engine.universe.insert(key(Venue::OkxSpot,"BTC"));
 for session in ["a","b","c"]{
  for (t,action,seq,prev) in [(0,"snapshot",10,None),(400,"update",11,Some(10)),(800,"update",12,Some(11))]{
   let out=f.engine.receive_batch(vec![okx_books_frame(session,t,action,seq,prev,"1")],ns(t),wall(t)).unwrap();
   f.handle(out);
  }
 }
 let scope=key(Venue::OkxSpot,"BTC");
 assert_eq!(f.engine.coverage(ns(800),wall(800)).as_array().unwrap().iter().find(|r|r["key"]==scope).unwrap()["hot_ready"].as_array().unwrap().len(),3);
 let bad=f.engine.receive_batch(vec![okx_books_frame("a",850,"update",14,Some(13),"4")],ns(850),wall(850)).unwrap();
 assert!(!bad.iter().any(|o|matches!(o,Output::Book(_,_)|Output::Frame(_))));
 assert_eq!(bad.iter().filter_map(|o|match o{Output::Resubscribe{session,base,reason}=>Some((session.as_str(),base.as_str(),reason.as_str())),_=>None}).collect::<Vec<_>>(),vec![("a","BTC","sequence gap")]);
 assert!(!f.auth.read().unwrap().contains_key(&scope));
 let row=f.engine.coverage(ns(850),wall(850)).as_array().unwrap().iter().find(|r|r["key"]==scope).unwrap().clone();
 assert_eq!(row["hot_ready"].as_array().unwrap().len(),2);assert!(row["active"].is_null());
 f.invariant("R1211_books_gap_quarantines_only_failed_source");
}
#[test]fn r1205_hot_closed_and_stale_open_promote_without_new_connection(){let mut f=Fixture::new();f.warm();assert_eq!(f.engine.coverage(ns(800),wall(800))[0]["hot_ready"].as_array().unwrap().len(),3);let old=f.engine.coverage(ns(800),wall(800))[0]["active"].as_str().unwrap().to_string();let out=f.engine.disconnected(&old,ns(810),wall(810)).unwrap();f.handle(out);let next=["a","b","c"].into_iter().find(|s|*s!=old).unwrap();f.feed(next,900,902);assert_ne!(old,f.engine.coverage(ns(900),wall(900))[0]["active"].as_str().unwrap());assert!(f.published>=2);f.invariant("active_closed_hot_promotion");let out=f.engine.tick(ns(1801),wall(1801)).unwrap();f.handle(out);assert!(f.auth.read().unwrap().is_empty());f.invariant("all_stale_fail_closed");}

#[test]
fn r1246_disconnected_active_replays_equivalent_hot_standby_after_epoch_reset() {
    let mut f = Fixture::new();
    let scope = key(Venue::OkxSpot, "BTC");
    f.engine.universe.insert(scope.clone());
    for session in ["a", "b", "c"] {
        for (t, action, seq, prev) in [
            (0, "snapshot", 10, None),
            (400, "update", 11, Some(10)),
            (800, "update", 12, Some(11)),
        ] {
            let frame = okx_books_frame(session, t, action, seq, prev, "1");
            let out = f.engine.receive_batch(vec![frame], ns(t), wall(t)).unwrap();
            f.handle(out);
        }
    }
    let before = f.published;
    let row = f.engine.coverage(ns(800), wall(800)).as_array().unwrap()
        .iter().find(|r| r["key"] == scope).unwrap().clone();
    assert_eq!(row["hot_ready"].as_array().unwrap().len(), 3);
    let old = row["active"].as_str().unwrap().to_owned();

    let out = f.engine.disconnected(&old, ns(810), wall(810)).unwrap();
    assert!(out.iter().any(|item| matches!(item, Output::Reset { venue: Venue::OkxSpot, base, .. } if base == "BTC")));
    let promoted = out.iter().find_map(|item| match item {
        Output::Book(frame, _) => frame.source.as_ref().map(|p| p.session_id.clone()),
        _ => None,
    });
    assert!(promoted.is_some(), "fresh equivalent standby must republish after Owner reset");
    assert_ne!(promoted.as_deref(), Some(old.as_str()));
    f.handle(out);
    assert!(f.published > before);
    assert!(f.auth.read().unwrap().contains_key(&scope));
    f.invariant("R1246_equivalent_hot_standby_republished_after_reset");
}
#[test]fn r1205_standby_and_no_standby_are_not_falsely_fresh(){let mut f=Fixture::new();for t in [0,400,800]{f.feed("a",t,t as i64+1);}assert_eq!(f.published,0);assert!(f.auth.read().unwrap().is_empty());let mut b=frame("b",800,1000);b.raw=b.raw.replace(&format!("\"E\":{}",wall(800)-50),&format!("\"E\":{}",wall(800)-5000));let out=f.engine.receive_batch(vec![b],ns(800),wall(800)).unwrap();f.handle(out);assert_eq!(f.published,0);f.invariant("stale_standby_no_standby");}
#[test]fn r1205_duplicate_reorder_equal_u_new_e_partial_gap(){let mut f=Fixture::new();f.warm();let before=f.published;for s in ["a","b","c"]{f.feed(s,850,801);f.feed(s,860,1);}assert_eq!(f.published,before);f.feed("a",900,10000);assert!(f.published>before);f.invariant("duplicates_reorder_equal_u_and_partial_sequence_jump");}
#[test]fn r1205_old_session_and_epoch_are_fenced_at_real_owner(){let mut f=Fixture::new();f.warm();let active=f.engine.coverage(ns(800),wall(800))[0]["active"].as_str().unwrap().to_string();f.feed(&active,900,902);let epoch=f.engine.coverage(ns(900),wall(900))[0]["epoch"].as_u64().unwrap();let mut forged=frame("standby-imposter",950,1000);f.store.lock().unwrap().accept_batch(std::slice::from_mut(&mut forged)).unwrap();forged.epoch=epoch;forged.source.as_mut().unwrap().selection_epoch=epoch;let ix=crate::owner_index("BTC",12);assert_eq!(f.owners[ix].ingest(&forged,&mut crate::history::PreparedSignal),Err(crate::CoreError::StaleEpoch));f.store.lock().unwrap().finish(&forged,"quarantined","wrong_source",None).unwrap();forged.epoch+=1;assert!(!authorized(&f.auth,&forged));f.invariant("source_identity_old_epoch");}
#[test]fn r1205_cannot_reclaim_before_ack_and_source_receipt_is_mirrored(){let mut f=Fixture::new();let mut raw=frame("a",0,1);f.store.lock().unwrap().accept_batch(std::slice::from_mut(&mut raw)).unwrap();raw.epoch=1;raw.source.as_mut().unwrap().selection_epoch=1;let u=parse(&raw).unwrap();let ix=crate::owner_index("BTC",12);let w=f.writer.as_mut().unwrap();w.source_commit_guarded(&mut f.owners,&[(raw.clone(),u,ix)],&[],Inject::None).unwrap();let p=raw.source.as_ref().unwrap();assert!(w.release_ingress_receipts(&f.store.lock().unwrap(),&[(p.journal_id.clone(),p.accepted_id)]).is_err());assert_eq!(f.store.lock().unwrap().counts().unwrap().pending,1);w.note_guarded_decision(&raw,"INPUT_REJECTED",&digest(b"not executed")).unwrap();let proof=w.ingress_receipt(&p.journal_id,p.accepted_id,&p.payload_sha256).unwrap().unwrap();assert!(w.release_ingress_receipts(&f.store.lock().unwrap(),&[(p.journal_id.clone(),p.accepted_id)]).is_err());f.store.lock().unwrap().finish(&raw,"published","ack",Some(&proof)).unwrap();w.release_ingress_receipts(&f.store.lock().unwrap(),&[(p.journal_id.clone(),p.accepted_id)]).unwrap();f.invariant("reclaim_requires_ack_and_durable_outcome");}
#[test]fn r1205_db1_db2_ack_crash_custody_is_exact(){for inject in [Inject::AfterDb1,Inject::AfterDb2]{let mut f=Fixture::new();let mut raw=frame("a",0,1);f.store.lock().unwrap().accept_batch(std::slice::from_mut(&mut raw)).unwrap();raw.source.as_mut().unwrap().selection_epoch=1;let u=parse(&raw).unwrap();let ix=crate::owner_index("BTC",12);assert!(f.writer.as_mut().unwrap().source_commit_guarded(&mut f.owners,&[(raw.clone(),u,ix)],&[],inject).is_err());f.writer.take();let mut w=Writer::resume(&f.db).unwrap();w.recover_ingress_source_only().unwrap();assert!(w.require_decisions_drained().is_err());assert_eq!(reconcile(&f.store,&w).unwrap(),1);assert!(!w.may_resume_old_book_as_fresh());assert!(!w.may_trade_real());f.writer=Some(w);f.invariant(if inject==Inject::AfterDb1{"db1_interruption"}else{"db2_ack_interruption"});}}
#[test]fn r1205_bounded_retention_reuses_pages_and_preserves_counts(){let mut f=Fixture::new();for n in 0..2500u64{let mut r=frame("a",n,n as i64+1);f.store.lock().unwrap().accept_batch(std::slice::from_mut(&mut r)).unwrap();f.store.lock().unwrap().finish(&r,"quarantined","retention_test",None).unwrap();}let s=f.store.lock().unwrap();let c=s.counts().unwrap();assert_eq!(c.accepted,2500);assert_eq!(c.pending,0);assert!(c.rows<=512);assert!(std::fs::metadata(&s.path).unwrap().len()<2*1024*1024);drop(s);f.invariant("bounded_retention");}
#[test]fn r1205_pending_queue_cap_is_explicit_no_eviction(){let f=Fixture::new();for batch in 0..64{let mut frames=(0..32).map(|n|frame("a",batch*32+n,(batch*32+n)as i64+1)).collect::<Vec<_>>();f.store.lock().unwrap().accept_batch(&mut frames).unwrap();}let before=f.store.lock().unwrap().counts().unwrap();let mut r=frame("a",3000,3001);assert!(f.store.lock().unwrap().accept_batch(std::slice::from_mut(&mut r)).unwrap_err().contains("capacity"));assert_eq!(f.store.lock().unwrap().counts().unwrap(),before);f.invariant("slow_subscriber_pending_capacity");}
#[test]fn r1205_clock_jumps_diff_schema_are_closed(){for label in ["clock_future","clock_backwards","diff_stream"]{let mut f=Fixture::new();let mut r=frame("a",100,1);let(now,wall_now)=match label{"clock_future"=>(ns(100),wall(100)-1000),"clock_backwards"=>(ns(99),wall(100)),_=>(ns(100),wall(100))};if label=="diff_stream"{r.raw=r.raw.replace("@depth20@500ms","@depth@500ms");}let out=f.engine.receive_batch(vec![r],now,wall_now).unwrap();f.handle(out);assert_eq!(f.published,0);f.invariant(label);}}
#[test]fn r1205_corrupt_sqlite_and_exclusive_writer_are_closed(){let f=Fixture::new();assert!(Store::open(&f.root).is_err());let p=f.root.join("bad");std::fs::DirBuilder::new().mode(0o700).create(&p).unwrap();std::fs::write(p.join("ingress.sqlite"),b"corrupt SQLite tail").unwrap();use std::os::unix::fs::PermissionsExt;std::fs::set_permissions(p.join("ingress.sqlite"),std::fs::Permissions::from_mode(0o600)).unwrap();assert!(Store::open(&p).is_err());}

#[test]fn r1205_inconsistent_same_update_id_revokes_authority(){let mut f=Fixture::new();f.warm();let mut r=frame("b",850,801);r.raw=r.raw.replace("\"100\"","\"100.1\"");let out=f.engine.receive_batch(vec![r],ns(850),wall(850)).unwrap();f.handle(out);assert!(f.auth.read().unwrap().is_empty());f.invariant("same_sequence_inconsistent_book");}

#[test]fn r1205_sqlite_full_rollback_keeps_accepted_counts(){let f=Fixture::new();let before=f.store.lock().unwrap().counts().unwrap();f.store.lock().unwrap().fault_full_pages().unwrap();let mut r=frame("a",0,1);r.raw="x".repeat(32700);let err=f.store.lock().unwrap().accept_batch(std::slice::from_mut(&mut r)).unwrap_err();assert!(err.contains("sqlite_code=13"),"{err}");assert_eq!(f.store.lock().unwrap().counts().unwrap(),before);f.invariant("sqlite_full_rollback");}
#[test]fn r1205_source_only_cold_obligations_are_denied_without_replay(){let mut f=Fixture::new();let mut raw=frame("a",0,1);f.store.lock().unwrap().accept_batch(std::slice::from_mut(&mut raw)).unwrap();raw.source.as_mut().unwrap().selection_epoch=1;let u=parse(&raw).unwrap();let ix=crate::owner_index("BTC",12);assert!(f.writer.as_mut().unwrap().source_commit_guarded(&mut f.owners,&[(raw,u,ix)],&[],Inject::AfterDb1).is_err());f.writer.take();let mut w=Writer::resume(&f.db).unwrap();w.recover_ingress_observer(&f.store.lock().unwrap()).unwrap();w.require_decisions_drained().unwrap();assert!(!w.may_resume_old_book_as_fresh());reconcile(&f.store,&w).unwrap();f.writer=Some(w);f.invariant("cold_source_only_obligation_denied_no_trade_replay");}
#[test]fn r1205_sigkill_child(){if let Ok(root)=std::env::var("R1205_SIGKILL_ROOT"){let path=PathBuf::from(root);let mut s=Store::open(&path).unwrap();let mut r=frame("child",0,1);s.accept_batch(std::slice::from_mut(&mut r)).unwrap();std::fs::write(path.join("ready"),b"committed").unwrap();loop{std::thread::sleep(std::time::Duration::from_millis(50));}}}
#[test]fn r1205_sigkill_after_accepted_keeps_payload_cold_pending(){let f=Fixture::new();let root=f.root.join("crash");std::fs::DirBuilder::new().mode(0o700).create(&root).unwrap();let mut child=std::process::Command::new(std::env::current_exe().unwrap()).args(["--exact","market_data_ingress_r1205::tests::r1205_sigkill_child","--nocapture"]).env("R1205_SIGKILL_ROOT",&root).stdout(std::process::Stdio::null()).spawn().unwrap();let start=std::time::Instant::now();while !root.join("ready").exists()&&start.elapsed()<std::time::Duration::from_secs(5){std::thread::sleep(std::time::Duration::from_millis(10));}let ready=root.join("ready").exists();child.kill().unwrap();child.wait().unwrap();assert!(ready);let s=Store::open(&root).unwrap();let pending=s.pending().unwrap();assert_eq!(pending.len(),1);assert_eq!(s.counts().unwrap().accepted,1);assert_eq!(s.counts().unwrap().pending,1);assert_eq!(pending[0].epoch,0);assert_eq!(digest(pending[0].raw.as_bytes()),pending[0].source.as_ref().unwrap().payload_sha256);}
#[test]fn r1205_retired_sources_do_not_accumulate_reconnect_state(){let mut f=Fixture::new();for n in 0..300{let id=format!("retired-{n}");f.engine.register(&id).unwrap();let out=f.engine.disconnected(&id,ns(1000),wall(1000)).unwrap();f.handle(out);}assert!(f.engine.sources.len()<=3);f.invariant("bounded_reconnect_state");}

#[test]fn r1205_stale_open_active_switches_to_already_warm_source(){let mut f=Fixture::new();f.warm();let active=f.engine.coverage(ns(800),wall(800))[0]["active"].as_str().unwrap().to_string();for s in ["a","b","c"]{if s!=active{f.feed(s,1200,1201)}}let before=f.published;let out=f.engine.tick(ns(1601),wall(1601)).unwrap();f.handle(out);assert!(f.published>before);assert_ne!(f.engine.coverage(ns(1601),wall(1601))[0]["active"].as_str().unwrap(),active);f.invariant("active_stale_but_open");}
#[test]fn r1205_reclaim_proof_survives_history_expiry_and_crash_boundary(){let mut f=Fixture::new();let mut raw=frame("a",0,1);f.store.lock().unwrap().accept_batch(std::slice::from_mut(&mut raw)).unwrap();raw.source.as_mut().unwrap().selection_epoch=1;let u=parse(&raw).unwrap();let ix=crate::owner_index("BTC",12);let w=f.writer.as_mut().unwrap();w.source_commit_guarded(&mut f.owners,&[(raw.clone(),u,ix)],&[],Inject::None).unwrap();w.note_guarded_decision(&raw,"INPUT_REJECTED",&digest(b"source-only")).unwrap();let p=raw.source.as_ref().unwrap();let proof=w.ingress_receipt(&p.journal_id,p.accepted_id,&p.payload_sha256).unwrap().unwrap();f.store.lock().unwrap().finish(&raw,"published","ack",Some(&proof)).unwrap();for n in 1..=520{let mut r=frame("b",n,n as i64+100);f.store.lock().unwrap().accept_batch(std::slice::from_mut(&mut r)).unwrap();f.store.lock().unwrap().finish(&r,"quarantined","superseded",None).unwrap();}assert!(f.store.lock().unwrap().published_proof(&p.journal_id,p.accepted_id,&proof).unwrap());w.release_ingress_receipts(&f.store.lock().unwrap(),&[(p.journal_id.clone(),p.accepted_id)]).unwrap();f.writer.take();let mut w=Writer::resume(&f.db).unwrap();w.recover_ingress_source_only().unwrap();w.sweep_ingress_receipts(&mut f.store.lock().unwrap()).unwrap();assert!(f.store.lock().unwrap().reclaim_refs().unwrap().is_empty());f.writer=Some(w);f.invariant("ack_reclaim_history_expiry_restart");}

fn okx_frame(session:&str,t:u64,seq:i64)->RawFrame{RawFrame{venue:Venue::OkxSpot,epoch:1,received_ns:ns(t),received_wall_ms:wall(t),source:Some(provenance(session.into(),1)),raw:serde_json::json!({"arg":{"channel":"books5","instId":"BTC-USDT"},"action":"snapshot","data":[{"ts":(wall(t)-50).to_string(),"seqId":seq,"bids":[["100","3","0","1"],["99","2","0","1"]],"asks":[["101","3","0","1"],["102","2","0","1"]]}]}).to_string()}}
fn okx_fixture()->Fixture{let mut f=Fixture::new();f.engine=Engine::new(f.store.clone(),f.auth.clone(),vec![key(Venue::OkxSpot,"BTC")]).unwrap();f.engine.clock=None;for id in ["a","b","c"]{f.engine.register(id).unwrap();}for(t,seq)in [(0,100),(400,101),(800,102)]{for id in ["a","b","c"]{let out=f.engine.receive_batch(vec![okx_frame(id,t,seq)],ns(t),wall(t)).unwrap();f.handle(out);}}f}
#[test]fn r1217_okx_invalid_audit_retains_sequence_context_after_shared_ring_eviction(){
 let mut f=Fixture::new();f.engine.universe.insert(key(Venue::OkxSpot,"BTC"));
 for session in ["a","b","c"]{
  for (t,action,seq,prev) in [(0,"snapshot",10,None),(400,"update",11,Some(10)),(800,"update",12,Some(11))]{
   let out=f.engine.receive_batch(vec![okx_books_frame(session,t,action,seq,prev,"1")],ns(t),wall(t)).unwrap();f.handle(out);
  }
 }
 let first_bad=okx_books_frame("a",850,"update",14,Some(13),"4");
 let first_hash=digest(first_bad.raw.as_bytes());
 let out=f.engine.receive_batch(vec![first_bad],ns(850),wall(850)).unwrap();f.handle(out);
 let first=f.engine.okx_books_invalid_audit().into_iter().next().unwrap();
 assert_eq!(first["failure_reason"],"sequence gap");
 assert_eq!(first["action"],"update");
 assert_eq!(first["seq_id"],14);
 assert_eq!(first["prev_seq_id"],13);
 assert_eq!(first["expected_previous_seq"],12);
 assert_eq!(first["pre_apply_book"]["bids_count"],2);
 assert_eq!(first["pre_apply_book"]["asks_count"],2);
 assert_eq!(first["checksum_validation"],"not_performed");
 assert_eq!(first["raw_sha256"],first_hash);
 for n in 0..65u64{
  let t=900+n*2;
  let snapshot=okx_books_frame("a",t,"snapshot",1000+n as i64,None,"1");
  let out=f.engine.receive_batch(vec![snapshot],ns(t),wall(t)).unwrap();f.handle(out);
  let bad=okx_books_frame("a",t+1,"update",200+n as i64,Some(199+n as i64),"1");
  let out=f.engine.receive_batch(vec![bad],ns(t+1),wall(t+1)).unwrap();f.handle(out);
 }
 let audit=f.engine.okx_books_invalid_audit();
 assert_eq!(audit.len(),66);
 assert_eq!(audit[0]["failure_reason"],"sequence gap");
 assert_eq!(audit[0]["raw_sha256"],first_hash);
 let bounded=f.engine.invalid_audit();
 assert_eq!(bounded.len(),64);
 assert!(!bounded.iter().any(|row|row["payload_sha256"]==first_hash));
 assert_eq!(f.engine.metrics.invalid_audit_evictions,2);
 f.invariant("R1217_okx_failure_diagnostic_retention");
}
#[test]
fn r1234_stale_okx_books_frame_cannot_advance_the_l2_builder() {
    for (stale_seq, stale_prev, next_seq, next_prev) in [
        (13, 12, 14, 13),
        (5, 12, 6, 5),
    ] {
        let mut f = Fixture::new();
        let scope = key(Venue::OkxSpot, "BTC");
        f.engine.universe.insert(scope.clone());
        for session in ["a", "b", "c"] {
            for (t, action, seq, prev, qty) in [
                (0, "snapshot", 10, None, "1"),
                (400, "update", 11, Some(10), "2"),
                (800, "update", 12, Some(11), "3"),
            ] {
                let out = f.engine.receive_batch(
                    vec![okx_books_frame(session, t, action, seq, prev, qty)],
                    ns(t), wall(t),
                ).unwrap();
                f.handle(out);
            }
        }
        let coverage = f.engine.coverage(ns(800), wall(800));
        let row = coverage.as_array().unwrap()
            .iter().find(|row| row["key"] == scope).unwrap();
        let active = row["active"].as_str().unwrap().to_string();

        let mut stale = okx_books_frame(
            &active, 900, "update", stale_seq, Some(stale_prev), "4",
        );
        stale.raw = stale.raw.replace(
            &(wall(900) - 50).to_string(),
            &(wall(900) - 900).to_string(),
        );
        let stale_out = f.engine.receive_batch(vec![stale], ns(900), wall(900)).unwrap();
        assert!(!stale_out.iter().any(|item| matches!(
            item, Output::Book(frame, _) if frame.source.as_ref()
                .is_some_and(|source| source.session_id == active)
        )));
        f.handle(stale_out);

        let next = okx_books_frame(
            &active, 1000, "update", next_seq, Some(next_prev), "5",
        );
        let next_out = f.engine.receive_batch(vec![next], ns(1000), wall(1000)).unwrap();
        assert!(!next_out.iter().any(|item| matches!(
            item, Output::Book(frame, _) if frame.source.as_ref()
                .is_some_and(|source| source.session_id == active)
        )), "stale frame mutated builder before freshness rejection: reset={}", stale_seq == 5);
        f.handle(next_out);
        finish_replay_pending(&f);
        f.invariant("R1234_stale_books_delta_not_replayed");
    }
}

#[test]
fn r1234_okx_books_reset_and_heartbeat_reach_owner_db1_db2_ack_decision_core() {
    let mut f = Fixture::new();
    let scope = key(Venue::OkxSpot, "BTC");
    f.engine.universe.insert(scope.clone());
    for session in ["a", "b", "c"] {
        for (t, action, seq, prev, qty) in [
            (0, "snapshot", 10, None, "1"),
            (400, "update", 11, Some(10), "2"),
            (800, "update", 12, Some(11), "3"),
        ] {
            let out = f.engine.receive_batch(
                vec![okx_books_frame(session, t, action, seq, prev, qty)],
                ns(t), wall(t),
            ).unwrap();
            f.handle(out);
        }
    }
    let coverage = f.engine.coverage(ns(800), wall(800));
    let row = coverage.as_array().unwrap()
        .iter().find(|row| row["key"] == scope).unwrap();
    let active = row["active"].as_str().unwrap().to_string();
    let owner_index = crate::owner_index("BTC", 12);
    let prior = f.owners[owner_index]
        .snapshot_venue("BTC", Venue::OkxSpot).unwrap();
    let config = crate::decision::DecisionConfig {
        max_age_ms: 800, metadata_ttl_ms: 1000, base_size: 10., risk_cap: 10.,
        cost: 24., required_net: 42., min_price: 0., max_price: f64::INFINITY,
        blacklist: std::collections::HashSet::new(),
    };
    let mut core = crate::decision::DecisionCore::new(
        owner_index, 12, config, std::collections::HashMap::new(),
    ).unwrap();
    core.update("BTC", "okx_spot", prior).unwrap();

    let reset = okx_books_frame(&active, 900, "update", 5, Some(12), "4");
    let out = f.engine.receive_batch(vec![reset], ns(900), wall(900)).unwrap();
    let (frame, update) = out.into_iter().find_map(|item| match item {
        Output::Book(frame, update) => Some((frame, update)),
        _ => None,
    }).expect("Engine must publish the valid maintenance reset");
    assert_eq!(update.book.sequence, Some(5));
    assert!(matches!(
        core.update_at("BTC", "okx_spot", update.book.clone(), ns(900), wall(900)),
        Err(crate::CoreError::OutOfOrder),
    ));
    let reset_book = update.book.clone();
    f.handle(vec![Output::Book(frame, update)]);
    assert_eq!(f.owners[owner_index]
        .snapshot_venue("BTC", Venue::OkxSpot).unwrap().sequence, Some(5));
    // Mirrors the guarded R1205 decision path, which evaluates after owner and durable ACK.
    assert!(core.update_at_native_verified_books5(
        "BTC", "okx_spot", reset_book, ns(900), wall(900), true,
    ).unwrap().is_empty());

    let next = okx_books_frame(&active, 1000, "update", 6, Some(5), "5");
    let out = f.engine.receive_batch(vec![next], ns(1000), wall(1000)).unwrap();
    let (frame, update) = out.into_iter().find_map(|item| match item {
        Output::Book(frame, update) => Some((frame, update)),
        _ => None,
    }).expect("Engine must publish the post-reset update");
    let next_book = update.book.clone();
    f.handle(vec![Output::Book(frame, update)]);
    assert_eq!(f.owners[owner_index]
        .snapshot_venue("BTC", Venue::OkxSpot).unwrap().sequence, Some(6));
    assert!(core.update_at_native_verified_books5(
        "BTC", "okx_spot", next_book, ns(1000), wall(1000), true,
    ).unwrap().is_empty());

    let owner_before_heartbeat = f.owners[owner_index]
        .snapshot_venue("BTC", Venue::OkxSpot).unwrap();
    let retained_book = f.engine.books.get(&(active.clone(), scope.clone()))
        .unwrap().update.book.clone();
    let before_heartbeat_published = f.published;
    let heartbeat = okx_empty_books_frame(&active, 1100, 6);
    let out = f.engine.receive_batch(vec![heartbeat], ns(1100), wall(1100)).unwrap();
    assert!(!out.iter().any(|item| matches!(item, Output::Book(_, _))));
    assert_eq!(f.engine.metrics.channel_keepalives, 1);
    f.handle(out);
    assert_eq!(f.published, before_heartbeat_published);
    let book_after_heartbeat = &f.engine.books.get(&(active.clone(), scope.clone()))
        .unwrap().update.book;
    assert_eq!(book_after_heartbeat.sequence, retained_book.sequence);
    assert_eq!(book_after_heartbeat.received_ns, retained_book.received_ns);
    assert_eq!(book_after_heartbeat.exchange_ts_ms, retained_book.exchange_ts_ms);
    let owner_after_heartbeat = f.owners[owner_index]
        .snapshot_venue("BTC", Venue::OkxSpot).unwrap();
    assert_eq!(owner_after_heartbeat.sequence, owner_before_heartbeat.sequence);
    assert_eq!(owner_after_heartbeat.received_ns, owner_before_heartbeat.received_ns);
    assert!(matches!(
        core.update_at_native_verified_books5(
            "BTC", "okx_spot", book_after_heartbeat.clone(), ns(1100), wall(1100), true,
        ),
        Err(crate::CoreError::OutOfOrder),
    ));

    let after_heartbeat = okx_books_frame(&active, 1200, "update", 7, Some(6), "6");
    let out = f.engine.receive_batch(
        vec![after_heartbeat], ns(1200), wall(1200),
    ).unwrap();
    let (frame, update) = out.into_iter().find_map(|item| match item {
        Output::Book(frame, update) => Some((frame, update)),
        _ => None,
    }).expect("OKX sequencing must continue after heartbeat");
    let next_book = update.book.clone();
    f.handle(vec![Output::Book(frame, update)]);
    assert_eq!(f.owners[owner_index]
        .snapshot_venue("BTC", Venue::OkxSpot).unwrap().sequence, Some(7));
    assert!(core.update_at_native_verified_books5(
        "BTC", "okx_spot", next_book, ns(1200), wall(1200), true,
    ).unwrap().is_empty());
    assert!(f.writer.as_ref().unwrap().verify_pair().is_ok());
    finish_replay_pending(&f);
    f.invariant("R1234_OKX_reset_heartbeat_followup_owner_db1_db2_ack_decision_core");
}

#[test]fn r1205_okx_identity_includes_sequence_and_maintenance_reset_is_snapshot_only(){let mut f=okx_fixture();let active=f.engine.coverage(ns(800),wall(800))[0]["active"].as_str().unwrap().to_string();let other=["a","b","c"].into_iter().find(|s|*s!=active).unwrap();let mut r=okx_frame(other,850,999);r.raw=r.raw.replace(&(wall(850)-50).to_string(),&(wall(800)-50).to_string()).replace("\"100\"","\"100.1\"");let out=f.engine.receive_batch(vec![r],ns(850),wall(850)).unwrap();f.handle(out);assert!(!f.auth.read().unwrap().is_empty());let prior=f.published;let out=f.engine.receive_batch(vec![okx_frame(&active,900,1)],ns(900),wall(900)).unwrap();f.handle(out);assert!(f.published>prior);assert_eq!(f.owners[crate::owner_index("BTC",12)].snapshot_pair("BTC").0.unwrap().sequence,Some(1));f.invariant("okx_sequence_identity_and_full_snapshot_maintenance_reset");}
#[test]fn r1205_okx_incremental_action_and_missing_sequence_never_qualify(){for label in ["incremental","missing_sequence"]{let mut f=okx_fixture();let before=f.published;let mut r=okx_frame("a",900,200);if label=="incremental"{r.raw=r.raw.replace("\"snapshot\"","\"update\"");}else{let mut v:serde_json::Value=serde_json::from_str(&r.raw).unwrap();v["data"][0].as_object_mut().unwrap().remove("seqId");r.raw=v.to_string();}let out=f.engine.receive_batch(vec![r],ns(900),wall(900)).unwrap();f.handle(out);let published=f.owners[crate::owner_index("BTC",12)].snapshot_venue("BTC",Venue::OkxSpot).unwrap();assert_eq!(published.sequence,Some(102),"invalid OKX frame replaced the owner book");assert!(f.published==before||f.published==before+1,"unexpected republish count");f.invariant(label);}}

#[test]
fn r1215_stale_snapshots_requalify_before_hot_recovery() {
    for okx in [false, true] {
        let mut f = if okx { okx_fixture() } else { let mut f = Fixture::new(); f.warm(); f };
        let venue = if okx { Venue::OkxSpot } else { Venue::BinanceFutures };
        let active = f.engine.coverage(ns(800), wall(800))[0]["active"].as_str().unwrap().to_string();
        let scope = key(venue, "BTC");
        let before = f.published;
        let out = f.engine.tick(ns(2001), wall(2001)).unwrap();
        f.handle(out);
        assert!(f.auth.read().unwrap().is_empty());
        assert_eq!(f.published, before);
        assert_eq!(f.engine.coverage(ns(2001), wall(2001))[0]["hot_ready"].as_array().unwrap().len(), 0);

        for (offset, session) in ["a", "b", "c"].into_iter().enumerate() {
            let t = 2100 + offset as u64;
            let seq = 1000 + offset as i64;
            let raw = if okx { okx_frame(session, t, seq) } else { frame(session, t, seq) };
            let out = f.engine.receive_batch(vec![raw], ns(t), wall(t)).unwrap();
            f.handle(out);
        }
        assert!(f.engine.books.get(&(active.clone(), scope.clone())).unwrap().qualified);
        assert_eq!(f.engine.coverage(ns(2102), wall(2102))[0]["hot_ready"].as_array().unwrap().len(), 0);
        assert!(f.auth.read().unwrap().is_empty());

        for (round, start) in [2500u64, 2900].into_iter().enumerate() {
            for (offset, session) in ["a", "b", "c"].into_iter().enumerate() {
                let t = start + offset as u64;
                let seq = 1100 + (round as i64 * 100) + offset as i64;
                let raw = if okx { okx_frame(session, t, seq) } else { frame(session, t, seq) };
                let out = f.engine.receive_batch(vec![raw], ns(t), wall(t)).unwrap();
                f.handle(out);
            }
        }
        assert_eq!(f.engine.coverage(ns(2902), wall(2902))[0]["hot_ready"].as_array().unwrap().len(), 3);
        assert!(f.engine.hot_complete(&active, venue, &["BTC".into()], ns(2900), wall(2900)));
        assert!(!f.auth.read().unwrap().is_empty());
        let out = f.engine.tick(ns(3803), wall(3803)).unwrap();
        f.handle(out);
        assert!(f.auth.read().unwrap().is_empty());
        f.invariant(if okx { "R1215_okx_stale_requalification" } else { "R1215_binance_stale_requalification" });
    }
}
#[test]
fn r1215_warmed_standby_fails_over_immediately_after_requalification() {
    let mut f = okx_fixture();
    let active = f.engine.coverage(ns(800), wall(800))[0]["active"].as_str().unwrap().to_string();
    let out = f.engine.tick(ns(2001), wall(2001)).unwrap();
    f.handle(out);
    assert!(f.auth.read().unwrap().is_empty());
    assert_eq!(f.engine.coverage(ns(2001), wall(2001))[0]["hot_ready"].as_array().unwrap().len(), 0);

    for (round, start) in [2100u64, 2500, 2900].into_iter().enumerate() {
        for (offset, session) in ["a", "b", "c"].into_iter().enumerate() {
            let t = start + offset as u64;
            let seq = 500 + round as i64 * 100 + offset as i64;
            let out = f.engine.receive_batch(vec![okx_frame(session, t, seq)], ns(t), wall(t)).unwrap();
            f.handle(out);
        }
    }
    assert_eq!(f.engine.coverage(ns(2902), wall(2902))[0]["hot_ready"].as_array().unwrap().len(), 3);
    assert!(!f.auth.read().unwrap().is_empty());
    let before = f.published;
    let out = f.engine.disconnected(&active, ns(2903), wall(2903)).unwrap();
    f.handle(out);
    let row = &f.engine.coverage(ns(2903), wall(2903))[0];
    assert_ne!(row["active"].as_str(), Some(active.as_str()));
    assert_eq!(f.published, before + 1);
    assert_eq!(row["hot_ready"].as_array().unwrap().len(), 2);
    f.invariant("R1215_warmed_standby_immediate_emergency_failover");
}
#[test]fn r1209_okx_books5_higher_seq_same_millisecond_is_valid_progress(){let mut f=okx_fixture();let active=f.engine.coverage(ns(800),wall(800))[0]["active"].as_str().unwrap().to_string();let before=f.published;let mut r=okx_frame(&active,810,103);r.raw=r.raw.replace(&(wall(810)-50).to_string(),&(wall(800)-50).to_string());let out=f.engine.receive_batch(vec![r],ns(810),wall(810)).unwrap();f.handle(out);assert_eq!(f.published,before+1,"before={before} audit={:?}",f.engine.audit());assert_eq!(f.engine.metrics.duplicates,0);f.invariant("okx_books5_seq_progress_same_exchange_millisecond");}
#[test]
fn r1215_stale_frame_keeps_valid_book_then_requires_fresh_manager_window() {
    let mut f = okx_fixture();
    let profile = f.store.lock().unwrap().accept_profile();
    let timing = profile["samples"].as_array().unwrap().last().unwrap();
    let commit_start = timing["commit_started_mono_ns"].as_u64().unwrap();
    assert!(commit_start > 0);
    assert!(timing["commit_finished_mono_ns"].as_u64().unwrap() >= commit_start);
    assert!(timing["store_lock_acquired_mono_ns"].as_u64().unwrap() > 0);
    let active = f.engine.coverage(ns(800), wall(800))[0]["active"].as_str().unwrap().to_string();
    let before = f.published;
    let counts = f.store.lock().unwrap().counts().unwrap();
    let mut stale = okx_frame(&active, 900, 150);
    stale.raw = stale.raw.replace(&(wall(900) - 50).to_string(), &(wall(900) - 5000).to_string());
    let out = f.engine.receive_batch(vec![stale], ns(900), wall(900)).unwrap();
    f.handle(out);
    let c = f.store.lock().unwrap().counts().unwrap();
    assert_eq!(f.published, before);
    assert_eq!(c.quarantined, counts.quarantined + 1);
    let book = f.engine.books.get(&(active.clone(), key(Venue::OkxSpot, "BTC"))).unwrap();
    assert!(book.qualified);
    assert!(Engine::fresh(book, ns(900), wall(900)));
    assert_eq!(f.engine.coverage(ns(900), wall(900))[0]["active"], active);
    assert_eq!(f.engine.coverage(ns(900), wall(900))[0]["hot_ready"].as_array().unwrap().len(), 3);

    let out = f.engine.tick(ns(1601), wall(1601)).unwrap();
    f.handle(out);
    assert!(f.auth.read().unwrap().is_empty());
    let peer = ["a", "b", "c"].into_iter().find(|id| *id != active).unwrap();
    for (round, t) in [1700u64, 2100, 2500].into_iter().enumerate() {
        let active_seq = 300 + round as i64 * 100;
        let peer_seq = active_seq + 1;
        let out = f.engine.receive_batch(vec![okx_frame(&active, t, active_seq)], ns(t), wall(t)).unwrap();
        f.handle(out);
        let out = f.engine.receive_batch(vec![okx_frame(peer, t + 1, peer_seq)], ns(t + 1), wall(t + 1)).unwrap();
        f.handle(out);
        if round == 0 {
            assert_eq!(f.engine.coverage(ns(t + 1), wall(t + 1))[0]["hot_ready"].as_array().unwrap().len(), 0);
            assert!(f.auth.read().unwrap().is_empty());
        }
    }
    assert!(f.published > before);
    assert!(f.auth.read().unwrap().contains_key(&key(Venue::OkxSpot, "BTC")));
    assert_eq!(f.engine.coverage(ns(2501), wall(2501))[0]["hot_ready"].as_array().unwrap().len(), 2);
    f.invariant("R1215_okx_stale_frame_quarantined_strict_requalification");
}
#[test]fn r1205_b_reconnected_session_does_not_inherit_old_qualification(){let mut f=okx_fixture();let old=f.engine.coverage(ns(800),wall(800))[0]["active"].as_str().unwrap().to_string();let out=f.engine.disconnected(&old,ns(2001),wall(2001)).unwrap();f.handle(out);f.engine.register("replacement").unwrap();let before=f.published;let out=f.engine.receive_batch(vec![okx_frame("replacement",2100,500)],ns(2100),wall(2100)).unwrap();f.handle(out);assert_eq!(f.published,before);assert!(!f.engine.hot_complete("replacement",Venue::OkxSpot,&["BTC".into()],ns(2100),wall(2100)));f.invariant("new_connection_not_hot_after_one_snapshot");}
#[test]fn r1205_b_inbox_payload_budget_counts_utf8_bytes(){let f=Fixture::new();let mut r=frame("a",0,1);r.raw="Ж".repeat(10000);f.store.lock().unwrap().accept_batch(std::slice::from_mut(&mut r)).unwrap();assert_eq!(f.store.lock().unwrap().counts().unwrap().payload_bytes,r.raw.len()as u64);f.store.lock().unwrap().finish(&r,"quarantined","utf8_byte_budget",None).unwrap();assert_eq!(f.store.lock().unwrap().counts().unwrap().payload_bytes,0);f.invariant("utf8_payload_byte_budget");}

// A real guarded Writer batch, with all decision obligations explicitly sealed.
fn c_published_batch(n:usize,sealed:bool)->(Fixture,Vec<RawFrame>){
 let mut f=Fixture::new();let mut frames=(0..n).map(|i|frame("batch",i as u64,i as i64+1)).collect::<Vec<_>>();
 f.store.lock().unwrap().accept_batch(&mut frames).unwrap();
 for r in &mut frames{r.source.as_mut().unwrap().selection_epoch=1;}
 let updates=frames.iter().map(|r|(r.clone(),parse(r).unwrap(),crate::owner_index("BTC",12))).collect::<Vec<_>>();
 let w=f.writer.as_mut().unwrap();w.source_commit_guarded(&mut f.owners,&updates,&[],Inject::None).unwrap();
 if sealed{for r in &frames{w.note_guarded_decision(r,"DECISION_DENIED",&digest(b"NO_EXECUTION_BATCH_TEST")).unwrap();}w.require_decisions_drained().unwrap();}
 (f,frames)
}
#[test]fn r1205_c_batch_reclaim_requires_complete_seal_and_exact_receipts(){
 let(mut f,frames)=c_published_batch(2,false);let before=f.store.lock().unwrap().counts().unwrap();
 {let w=f.writer.as_mut().unwrap();assert!(f.store.lock().unwrap().finish_published_batch(w,&frames).is_err());w.note_guarded_decision(&frames[0],"DECISION_DENIED",&digest(b"first")).unwrap();assert!(f.store.lock().unwrap().finish_published_batch(w,&frames).is_err());w.note_guarded_decision(&frames[1],"DECISION_DENIED",&digest(b"second")).unwrap();}
 let w=f.writer.as_ref().unwrap();
 let mut bad=frames.clone();bad[1].received_ns+=1;
 for items in [&bad[..],&[frames[0].clone(),frames[0].clone()][..],&[][..]]{assert!(f.store.lock().unwrap().finish_published_batch(w,items).is_err());assert_eq!(f.store.lock().unwrap().counts().unwrap(),before);}
 assert!(f.store.lock().unwrap().finish_published_batch(w,&vec![frames[0].clone();33]).is_err());
 f.store.lock().unwrap().finish_published_batch(w,&frames).unwrap();
 let after=f.store.lock().unwrap().counts().unwrap();assert_eq!((after.accepted,after.published,after.pending,after.payload_bytes),(2,2,0,0));
 assert!(f.store.lock().unwrap().finish_published_batch(w,&frames).is_err());assert_eq!(f.store.lock().unwrap().counts().unwrap(),after);
 f.invariant("batch_reclaim_complete_seal_exact_identity_duplicate_bounds");
}
#[test]fn r1205_c_batch_terminal_restart_before_canonical_reclaim_is_exact(){
 let(mut f,frames)=c_published_batch(32,true);
 f.store.lock().unwrap().finish_published_batch(f.writer.as_ref().unwrap(),&frames).unwrap();
 assert_eq!(f.store.lock().unwrap().reclaim_refs().unwrap().len(),32);
 f.writer.take();let mut w=Writer::resume(&f.db).unwrap();w.recover_ingress_observer(&f.store.lock().unwrap()).unwrap();
 w.sweep_ingress_receipts(&mut f.store.lock().unwrap()).unwrap();w.verify_pair().unwrap();w.require_decisions_drained().unwrap();
 assert!(f.store.lock().unwrap().pending().unwrap().is_empty());assert!(f.store.lock().unwrap().reclaim_refs().unwrap().is_empty());
 assert!(!w.may_resume_old_book_as_fresh());assert!(!w.may_trade_real());f.writer=Some(w);
 let c=f.store.lock().unwrap().counts().unwrap();assert_eq!((c.accepted,c.published,c.quarantined,c.pending),(32,32,0,0));f.invariant("batch32_terminal_restart_no_replay");
}
fn c_write_bytes()->u64{std::fs::read_to_string("/proc/self/io").unwrap().lines().find_map(|l|l.strip_prefix("write_bytes: ")).unwrap().parse().unwrap()}
#[test]fn r1205_c_bounded_terminal_batch_io_comparison(){
 for n in [1,16,32]{for batched in [false,true]{
  let(f,frames)=c_published_batch(n,true);let w=f.writer.as_ref().unwrap();let mut inbox=f.store.lock().unwrap();let before=c_write_bytes();let start=std::time::Instant::now();
  if batched{inbox.finish_published_batch(w,&frames).unwrap();}else{for r in &frames{let p=r.source.as_ref().unwrap();let proof=w.ingress_receipt(&p.journal_id,p.accepted_id,&p.payload_sha256).unwrap().unwrap();inbox.finish(r,"published","canonical_db1_db2_ack",Some(&proof)).unwrap();}}
  let elapsed=start.elapsed().as_nanos();let bytes=c_write_bytes()-before;let c=inbox.counts().unwrap();assert_eq!((c.accepted,c.published,c.pending,c.payload_bytes),(n as u64,n as u64,0,0));
  let refs=frames.iter().map(|r|{let p=r.source.as_ref().unwrap();(p.journal_id.clone(),p.accepted_id)}).collect::<Vec<_>>();drop(inbox);
  println!("R1205_C_IO {}",serde_json::json!({"n":n,"batched":batched,"terminal_full_transactions":if batched{1}else{n},"write_bytes":bytes,"elapsed_ns":elapsed,"scope":"terminal_only_not_network_or_source_commit"}));
  let mut f=f;let w=f.writer.as_mut().unwrap();let mut inbox=f.store.lock().unwrap();w.release_ingress_receipts(&inbox,&refs).unwrap();inbox.forget_reclaimed(w,&refs).unwrap();drop(inbox);f.invariant("terminal_batch_io_accounting");
 }}
}

#[tokio::test]async fn r1279_published_replay_retains_trace_through_bounded_queues_and_durable_ack(){
 let mut f=Fixture::new();f.warm();
 let mut traced=frame("a",1200,1201);let ws_receive=crate::ingress_runtime_r1205::mono_ns().saturating_sub(2_000_000);let source=traced.source.as_mut().unwrap();source.frame_trace_id="r1279-replay-published".into();source.r1279_timing=Some(crate::R1279FrameTiming{clock_domain_id:Some(format!("pid:{}",std::process::id())),instrument:Some("BTC".into()),exchange_event_ts_ms:Some(1_790_000_001_201),ws_receive_ns:Some(ws_receive),ws_decode_end_ns:Some(ws_receive+10),..Default::default()});
 let(reader_tx,mut reader_rx)=tokio::sync::mpsc::channel(16);assert!(crate::ingress_runtime_r1205::enqueue_reader_frame(&reader_tx,traced).await.0);let mut traced=reader_rx.recv().await.unwrap();let t=traced.source.as_mut().unwrap().r1279_timing.as_mut().unwrap();t.reader_dequeue_ns=Some(crate::ingress_runtime_r1205::mono_ns());
 let(central_tx,mut central_rx)=tokio::sync::mpsc::channel(32);let depth=central_tx.max_capacity().saturating_sub(central_tx.capacity()).saturating_add(1);{let t=traced.source.as_mut().unwrap().r1279_timing.as_mut().unwrap();t.central_enqueue_ns=Some(crate::ingress_runtime_r1205::mono_ns());t.central_queue_capacity=Some(central_tx.max_capacity());t.central_queue_depth=Some(depth);}central_tx.send(traced).await.unwrap();let mut traced=central_rx.recv().await.unwrap();traced.source.as_mut().unwrap().r1279_timing.as_mut().unwrap().central_dequeue_ns=Some(crate::ingress_runtime_r1205::mono_ns());
 let mut out=f.engine.receive_batch(vec![traced],ns(1200),wall(1200)).unwrap();
 for item in &mut out{let frame=match item{Output::Frame(frame)|Output::Book(frame,_)=>Some(frame),_=>None};if let Some(frame)=frame{let update=crate::parse(frame).unwrap();let queued=frame.clone();let(tx,mut rx)=tokio::sync::mpsc::channel(crate::r1241_ingress_pipeline::R1241_CANONICAL_INPUT_CAPACITY);crate::r1241_ingress_pipeline::submit(&tx,crate::r1241_ingress_pipeline::Command::Frame(crate::r1241_ingress_pipeline::CanonicalFrame{frame:queued,update,owner_index:crate::owner_index("BTC",12)})).unwrap();let command=rx.recv().await.unwrap();let mut canonical=match command{crate::r1241_ingress_pipeline::Command::Frame(c)=>c.frame,_=>panic!("unexpected canonical command")};canonical.source.as_mut().unwrap().r1279_timing.as_mut().unwrap().canonical_dequeue_ns=Some(crate::ingress_runtime_r1205::mono_ns());frame.source.as_mut().unwrap().r1279_timing=canonical.source.unwrap().r1279_timing;}}
 f.handle(out);
 let row=f.store.lock().unwrap().frame_trace_terminal(4096).unwrap().into_iter().find(|r|r["frame_trace_id"]=="r1279-replay-published").expect("terminal trace row");
 assert_eq!(row["terminal_state"],"published");assert_eq!(row["ack_status"],"reclaimed_ack_confirmed");assert!(row["trace"]["ws_receive_ns"].is_number());assert!(row["completion"]["ack_end_ns"].is_number());for stage in ["reader_enqueue_ns","reader_dequeue_ns","central_enqueue_ns","central_dequeue_ns","canonical_enqueue_ns","canonical_dequeue_ns"]{assert!(row["trace"][stage].is_number(),"missing queue stage {stage}");}println!("R1279_TRACE_SAMPLE {}",row);f.invariant("r1279_published_trace_bounded_queues_durable_ack");
}
#[test]fn r1206_one_hot_source_cannot_publish_without_ready_standby(){
 let mut f=Fixture::new();f.warm();let active=f.engine.coverage(ns(800),wall(800))[0]["active"].as_str().unwrap().to_string();let peers=["a","b","c"].into_iter().filter(|id|*id!=active).collect::<Vec<_>>();let before=f.published;
 for id in peers{let out=f.engine.disconnected(id,ns(810),wall(810)).unwrap();f.handle(out);}
 f.feed(&active,900,1000);assert_eq!(f.published,before);assert!(f.auth.read().unwrap().is_empty());assert!(f.engine.hot_complete(&active,Venue::BinanceFutures,&["BTC".into()],ns(900),wall(900)));
 f.engine.register("new-peer").unwrap();for(t,u)in [(1000,1100),(1400,1500)]{f.feed("new-peer",t,u);}f.feed(&active,1300,1600);f.feed(&active,1700,1800);assert_eq!(f.published,before);f.feed("new-peer",1800,1900);assert_eq!(f.published,before+1,"coverage={} qualification={} audit={:?}",f.engine.coverage(ns(1800),wall(1800)),f.engine.qualification_proof(Venue::BinanceFutures,"BTC",ns(1800),wall(1800)),f.engine.selection_audit());assert!(!f.auth.read().unwrap().is_empty());f.invariant("r1206_standby_required_for_every_publication");
}
#[test]fn r1206_additive_index_reopen_preserves_old_pending_payload_and_full(){
 let f=Fixture::new();let root=f.root.join("old_index_schema");std::fs::DirBuilder::new().mode(0o700).create(&root).unwrap();let mut s=Store::open(&root).unwrap();s.r1206_remove_index_fixture().unwrap();let mut frames=vec![frame("old",0,1),frame("old",1,2)];s.accept_batch(&mut frames).unwrap();let before=s.counts().unwrap();drop(s);
 let mut s=Store::open(&root).unwrap();assert_eq!(s.counts().unwrap(),before);let recovered=s.pending().unwrap();assert_eq!(recovered.len(),2);assert!(recovered.iter().all(|r|r.epoch==0&&digest(r.raw.as_bytes())==r.source.as_ref().unwrap().payload_sha256));let(plan,full)=s.r1206_index_fixture_state().unwrap();assert!(plan.iter().flatten().any(|x|x.contains("inbox_payload_hash")));assert_eq!(full,"2");let(mode,sync,checkpoint)=s.r1207_durability_fixture_state().unwrap();assert_eq!((mode.as_str(),sync.as_str(),checkpoint.as_str()),("wal","2","1000"));s.quarantine_batch(&recovered.into_iter().map(|r|(r,"cold_no_trade_replay".into())).collect::<Vec<_>>()).unwrap();let c=s.counts().unwrap();assert_eq!((c.accepted,c.published,c.quarantined,c.pending),(2,0,2,0));println!("R1206_INDEX_REOPEN {}",serde_json::json!({"counts":c,"synchronous":full,"plan":plan,"orders":0}));
}
#[test]fn r1206_planned_rotation_is_fast_and_unexpected_failures_keep_backoff(){use crate::ingress_runtime_r1205::reconnect_delay;use std::time::Duration;for why in ["PLANNED_PRE_24H_ROTATION","PLANNED_ROTATION_PROBE"]{assert_eq!(reconnect_delay(why,10),Duration::from_secs(1));}assert_eq!(reconnect_delay("socket_closed",1),Duration::from_secs(30));assert_eq!(reconnect_delay("socket_closed",3),Duration::from_secs(60));}

#[test]fn r1206_standby_certificate_expires_while_active_stays_fresh(){
 let mut f=Fixture::new();f.warm();let active=f.engine.coverage(ns(800),wall(800))[0]["active"].as_str().unwrap().to_string();let out=f.engine.receive_batch(vec![frame(&active,1400,1500)],ns(1400),wall(1400)).unwrap();let raw=out.into_iter().find_map(|o|match o{Output::Frame(r)=>Some(r),_=>None}).unwrap();assert!(authorized_at(&f.auth,&raw,ns(1400),wall(1400)));assert!(!authorized_at(&f.auth,&raw,ns(1601),wall(1601)));assert!(ns(1601)-raw.received_ns<800_000_000);f.store.lock().unwrap().finish(&raw,"quarantined","standby_expired_in_queue",None).unwrap();f.invariant("r1206_queued_frame_cannot_outlive_standby_certificate");
}

#[test]fn r1206_release_ticket_unlocks_inbox_and_rejects_stale_head(){
 let(mut f,frames)=c_published_batch(2,true);
 let refs=frames.iter().map(|r|{let p=r.source.as_ref().unwrap();(p.journal_id.clone(),p.accepted_id)}).collect::<Vec<_>>();
 let w=f.writer.as_mut().unwrap();
 let ticket={let mut inbox=f.store.lock().unwrap();assert!(w.prepare_ingress_release(&inbox,&refs).is_err());inbox.finish_published_batch(w,&frames).unwrap();w.prepare_ingress_release(&inbox,&refs).unwrap().unwrap()};
 {let mut inbox=f.store.try_lock().expect("canonical release must not hold inbox");let mut extra=vec![frame("parallel-ingress",0,30)];inbox.accept_batch(&mut extra).unwrap();inbox.finish(&extra[0],"quarantined","ticket_interleaving",None).unwrap();}
 w.source_reset(&mut f.owners,Venue::BinanceFutures,&["BTC".into()],2,wall(2000),"PUBLIC_WS_RESET",Inject::None).unwrap();
 assert_eq!(w.commit_ingress_release(ticket).unwrap_err(),"ingress_release_ticket_stale");
 let ticket={let inbox=f.store.lock().unwrap();assert_eq!(inbox.reclaim_refs().unwrap().len(),2);assert!(w.ingress_receipt_exists(&refs[0].0,refs[0].1).unwrap());w.prepare_ingress_release(&inbox,&refs).unwrap().unwrap()};
 w.commit_ingress_release(ticket).unwrap();f.store.lock().unwrap().forget_reclaimed(w,&refs).unwrap();w.verify_pair().unwrap();
 let c=f.store.lock().unwrap().counts().unwrap();assert_eq!((c.accepted,c.published,c.quarantined,c.pending),(3,2,1,0));
 f.invariant("ticket_proof_before_unlock_stale_head_rejected_interleaved_ingress");
}
#[test]fn r1206_release_ticket_crash_points_recover_without_republication(){
 for inject in [Inject::AfterDb1,Inject::AfterDb2]{
  let(mut f,frames)=c_published_batch(2,true);let refs=frames.iter().map(|r|{let p=r.source.as_ref().unwrap();(p.journal_id.clone(),p.accepted_id)}).collect::<Vec<_>>();
  let ticket={let w=f.writer.as_ref().unwrap();let mut inbox=f.store.lock().unwrap();inbox.finish_published_batch(w,&frames).unwrap();w.prepare_ingress_release(&inbox,&refs).unwrap().unwrap()};
  assert!(f.writer.as_mut().unwrap().release_ingress_injected(ticket,inject).is_err());assert_eq!(f.store.lock().unwrap().reclaim_refs().unwrap().len(),2);
  f.writer.take();let mut w=Writer::resume(&f.db).unwrap();w.recover_ingress_observer(&f.store.lock().unwrap()).unwrap();w.sweep_ingress_receipts(&mut f.store.lock().unwrap()).unwrap();w.verify_pair().unwrap();w.require_decisions_drained().unwrap();
  assert!(f.store.lock().unwrap().pending().unwrap().is_empty());assert!(f.store.lock().unwrap().reclaim_refs().unwrap().is_empty());let c=f.store.lock().unwrap().counts().unwrap();assert_eq!((c.accepted,c.published,c.pending),(2,2,0));f.writer=Some(w);f.invariant("ticket_reclaim_crash_retains_proof_no_republication");
 }
}
#[test]fn r1214_engine_selection_is_owned_by_universal_manager(){
 let mut f=Fixture::new();f.warm();
 let row=&f.engine.coverage(ns(800),wall(800))[0];let session=row["active"].as_str().unwrap();
 let scope=key(Venue::BinanceFutures,"BTC");let book=f.engine.books.get(&(session.to_string(),scope)).unwrap();
 assert_eq!(f.engine.manager.active_for_scope(&book.manager_id),Some(&book.manager_id));
 assert!(f.engine.manager.state(&book.manager_id).unwrap().hot_ready);
 assert_eq!(f.engine.selection_audit.len(),1);
}
// R1402: exercise the real Engine's switch publication boundary, not a second manager.
fn r1402_drain_worker_engine(e:&mut Engine,now_ns:u64,now_wall:i64)->Vec<Output>{
 let deadline=std::time::Instant::now()+std::time::Duration::from_secs(1);
 let mut out=Vec::new();
 while !e.ucm_pending.is_empty()||!e.ucm_dirty.is_empty()||e.manager.remote().is_some_and(|r|!r.retirements.is_empty()){
  assert!(std::time::Instant::now()<deadline,"worker Engine continuation stalled");
  out.extend(e.poll_ucm_worker(now_ns,now_wall).unwrap());
  std::thread::yield_now();
 }
 out
}
#[test]
fn r1402_worker_engine_group_control_fences_all_registered_scopes(){
 let auth:Authority=Arc::new(RwLock::new(HashMap::new()));
 let bases=(0..32).map(|i|format!("ASSET{i}")).collect::<Vec<_>>();
 let mut e=Engine::new_volatile(auth,bases.iter().map(|b|key(Venue::BinanceFutures,b)).collect()).unwrap();
 e.clock=None;e.enable_ucm_worker(16).unwrap();
 e.register_connection_scoped("group-a",Venue::BinanceFutures,1,&bases).unwrap();
 assert_eq!(e.manager.remote().unwrap().worker.revision(),1,"one ordered physical connection group, not 64 events");
 assert_eq!(e.manager.remote().unwrap().known.len(),32);
 let first=e.manager_ids[&("group-a".into(),key(Venue::BinanceFutures,&bases[0]))].clone();
 let last=e.manager_ids[&("group-a".into(),key(Venue::BinanceFutures,&bases[31]))].clone();
 let mut a=e.begin_ucm_snapshot(&first).unwrap();let mut b=e.begin_ucm_snapshot(&last).unwrap();
 e.mark_reconnecting("group-a");
 let r=e.manager.remote_mut().unwrap();
 assert_eq!(r.worker.revision(),2);
 assert!(r.worker.poll_snapshot(&mut a).is_err());
 assert!(r.worker.poll_snapshot(&mut b).is_err());
 assert!(r.worker.is_healthy(),"valid group must not poison worker");
 let mut pending=e.begin_ucm_snapshot(&last).unwrap();
 let view=loop{match e.manager.remote_mut().unwrap().worker.poll_snapshot(&mut pending).unwrap(){
  Some(view)=>break view,None=>std::thread::yield_now()
 }};
 assert!(!view.sources[&last].health.hot_ready);
 assert_eq!(view.sources[&last].health.state,Some(crate::universal_connection_manager_r1209::Lifecycle::Reconnecting));
}
#[test]
fn r1402_worker_engine_single_owner_cutover_and_freshness(){
 let auth:Authority=Arc::new(RwLock::new(HashMap::new()));
 let k=key(Venue::BinanceFutures,"BTC");
 let mut e=Engine::new_volatile(auth.clone(),vec![k.clone()]).unwrap();
 e.clock=None;
 let _notify=e.enable_ucm_worker(16).unwrap();
 assert!(matches!(e.manager.owner,UcmOwner::Worker(_)));
 assert!(e.enable_ucm_worker(16).is_err());
 for session in ["a","b"]{e.register(session).unwrap();}
 let mut released=0;
 for t in [0,400,800]{for session in ["a","b"]{
  let immediate=e.receive_batch(vec![frame(session,t,t as i64+1)],ns(t),wall(t)).unwrap();
  assert!(!immediate.iter().any(|o|matches!(o,Output::Frame(_)|Output::Book(_,_)|Output::Reset{..})));
  let out=r1402_drain_worker_engine(&mut e,ns(t),wall(t));
  for item in out{if let Output::Frame(f)=item{assert!(authorized_at(&auth,&f,ns(t),wall(t)));released+=1;}}
 }}
 assert!(released>0,"worker-backed actual Engine must publish validated data");
 let chosen=e.selected.get(&k).unwrap().clone();
 let book=&e.books[&(chosen.session.clone(),k.clone())];
 assert_eq!(e.manager.active_for_scope(&book.manager_id),Some(&book.manager_id));
 assert!(auth.read().unwrap().contains_key(&k));
 assert!(e.readiness_audit.iter().any(|row|row["kind"]=="two_hot_sources_established"));
 e.tick(ns(1600),wall(1550)).unwrap();r1402_drain_worker_engine(&mut e,ns(1600),wall(1550));
 assert!(auth.read().unwrap().contains_key(&k),"inclusive 800ms both clocks");
 e.tick(ns(1601),wall(1551)).unwrap();r1402_drain_worker_engine(&mut e,ns(1601),wall(1551));
 assert!(!auth.read().unwrap().contains_key(&k),"801ms must revoke");
}
#[test]
fn r1402_worker_engine_late_applied_ack_close_failclosed(){
 let auth:Authority=Arc::new(RwLock::new(HashMap::new()));
 let k=key(Venue::BinanceFutures,"BTC");
 let mut e=Engine::new_volatile(auth.clone(),vec![k.clone()]).unwrap();e.clock=None;
 e.enable_ucm_worker(16).unwrap();
 for session in ["a","b"]{e.register(session).unwrap();}
 for t in [0,400,800]{for session in ["a","b"]{
  e.receive_batch(vec![frame(session,t,t as i64+1)],ns(t),wall(t)).unwrap();
  if t!=800||session!="b"{r1402_drain_worker_engine(&mut e,ns(t),wall(t));}
 }}
 let deadline=std::time::Instant::now()+std::time::Duration::from_secs(1);
 let closed=loop{
  if let Some(PendingHotSelection{stage:PendingHotStage::Commit{candidate,..},..})=e.ucm_pending.get(&k){break candidate.logical_session.clone()}
  assert!(std::time::Instant::now()<deadline,"no actual commit request");
  assert!(e.poll_ucm_worker(ns(800),wall(800)).unwrap().is_empty());std::thread::yield_now();
 };
 assert!(!auth.read().unwrap().contains_key(&k));
 assert!(e.selected.is_empty());
 e.disconnected(&closed,ns(801),wall(801)).unwrap();
 let out=r1402_drain_worker_engine(&mut e,ns(801),wall(801));
 assert!(!out.iter().any(|o|matches!(o,Output::Reset{..}|Output::Frame(_)|Output::Book(_,_) )));
 assert!(!auth.read().unwrap().contains_key(&k));
 assert!(e.selected.is_empty(),"superseded worker ACK cannot publish old selection");
}
#[test]
fn r1402_applied_switch_required_before_selected_publication(){
 let auth:Authority=Arc::new(RwLock::new(HashMap::new()));
 let k=key(Venue::BinanceFutures,"BTC");
 let mut e=Engine::new_volatile(auth.clone(),vec![k.clone()]).unwrap();
 e.clock=None; // synthetic replay uses the supplied monotonic/wall clocks
 for session in ["a","b","c"]{e.register(session).unwrap();}
 for t in [0,400,800]{for session in ["a","b","c"]{
  e.receive_batch(vec![frame(session,t,t as i64+1)],ns(t),wall(t)).unwrap();
 }}
 let prior=e.selected.get(&k).unwrap().clone();
 let candidate=e.books.iter().find(|((session,scope),book)|
  scope==&k&&session!=&prior.session&&e.ready(book,ns(800),wall(800)))
  .unwrap().1.manager_id.clone();
 let before=e.selection_audit.len();
 let plan=e.prepare_hot_switch(&k,ns(800),wall(800),Some(&prior),candidate.clone(),"sustained_quality_improvement").unwrap();
 assert_eq!(e.selected.get(&k).unwrap().epoch,prior.epoch);
 assert_eq!(e.selection_audit.len(),before);
 let applied=e.apply_local_hot_switch(plan).unwrap();
 assert_eq!(e.manager.active_for_scope(&candidate),Some(&candidate));
 assert_eq!(e.selected.get(&k).unwrap().epoch,prior.epoch);
 assert_eq!(e.selection_audit.len(),before);
 // A lifecycle event after the applied operation must not publish a Reset.
 e.manager.close(&candidate);
 assert!(e.publish_applied_hot_switch(&k,ns(800),wall(800),applied).is_err());
 assert_eq!(e.selected.get(&k).unwrap().epoch,prior.epoch);
 assert_eq!(e.selection_audit.len(),before);
 assert!(!auth.read().unwrap().contains_key(&k));
}
#[test]fn r1214_invalid_frame_diagnostics_are_bounded_and_secret_free(){
 let mut f=Fixture::new();
 for t in 0..70u64{let mut bad=frame("a",t,t as i64+1);bad.raw=bad.raw.replace("depth20@500ms","aggTrade");let out=f.engine.receive_batch(vec![bad],ns(t),wall(t)).unwrap();f.handle(out);}
 let rows=f.engine.invalid_audit();assert_eq!(rows.len(),64);assert_eq!(f.engine.metrics.invalid_audit_evictions,6);assert_eq!(f.engine.metrics.invalid,70);
 assert!(rows.iter().all(|row|row.get("raw").is_none()&&row["generation"].as_u64()==Some(1)&&row["payload_sha256"].as_str().is_some()));
 assert_eq!(f.engine.metrics.invalid_categories.values().sum::<u64>(),70);
 f.invariant("R1214_bounded_invalid_frame_audit");
}
#[test]
fn r1215_missing_selected_book_fails_closed_and_recovers() {
    let mut f = Fixture::new();
    f.warm();
    let scope = key(Venue::BinanceFutures, "BTC");
    let selected = f.engine.selected.get("BINANCE_FUTURES:BTC").cloned()
        .or_else(|| f.engine.selected.values().next().cloned())
        .expect("warm fixture must have a selected source");
    assert!(f.auth.read().unwrap().contains_key(&scope));
    let before_published = f.published;
    let before_blocked = f.engine.metrics.blocked;

    // Model the stale selection pointer that caused the R1214 failure. Leave the
    // two fresh standby books in place and retain their frames for recovery.
    let removed = f.engine.books.remove(&(selected.session.clone(), scope.clone()));
    let missing_manager_id = removed.as_ref().unwrap().manager_id.clone();
    let out = f.engine.tick(ns(900), wall(900)).unwrap();
    assert!(out.is_empty(), "a missing selected book must publish nothing");
    assert!(!f.engine.selected.contains_key(&scope));
    assert!(!f.auth.read().unwrap().contains_key(&scope));
    assert_eq!(f.engine.manager.active_for_scope(&missing_manager_id), None);
    assert!(!f.engine.manager.state(&missing_manager_id).unwrap().hot_ready);
    assert_eq!(f.engine.metrics.blocked, before_blocked + 1);
    assert_eq!(f.published, before_published);
    assert_eq!(f.engine.books.iter().filter(|((_, k), b)| k == &scope && b.retained).count(), 2);

    // A later advancing update from one retained standby restores a manager-owned
    // selection and publishes only after the pair is again valid.
    let peer = ["a", "b", "c"].into_iter()
        .find(|session| *session != selected.session)
        .expect("fixture has a standby session");
    let out = f.engine.receive_batch(vec![frame(peer, 900, 902)], ns(900), wall(900)).unwrap();
    f.handle(out);
    assert!(f.engine.selected.contains_key(&scope));
    assert!(f.auth.read().unwrap().contains_key(&scope));
    assert!(f.published > before_published);
    assert_eq!(f.engine.manager.active_for_scope(
        &f.engine.books.get(&(f.engine.selected[&scope].session.clone(), scope.clone())).unwrap().manager_id
    ), Some(&f.engine.books.get(&(f.engine.selected[&scope].session.clone(), scope.clone())).unwrap().manager_id));
    f.invariant("R1215_missing_selected_book_fail_closed_and_recovery");
}

#[test]
fn r1216_okx_empty_books_heartbeat_updates_transport_not_book_freshness() {
    let mut f = Fixture::new();
    let scope = key(Venue::OkxSpot, "BTC");
    f.engine.universe.insert(scope.clone());
    for session in ["a", "b", "c"] {
        for (t, action, seq, prev, qty) in [
            (0, "snapshot", 10, None, "1"),
            (400, "update", 11, Some(10), "2"),
            (800, "update", 12, Some(11), "3"),
        ] {
            let out = f.engine.receive_batch(
                vec![okx_books_frame(session, t, action, seq, prev, qty)],
                ns(t), wall(t),
            ).unwrap();
            f.handle(out);
        }
    }
    let row = f.engine.coverage(ns(800), wall(800)).as_array().unwrap()
        .iter().find(|row| row["key"] == scope).unwrap().clone();
    let active = row["active"].as_str().unwrap().to_string();
    let before_switches = f.engine.metrics.switches;
    let before_audit = f.engine.selection_audit.len();
    let before_published = f.published;
    let before_invalid = f.engine.metrics.invalid;
    let before_duplicates = f.engine.metrics.duplicates;
    let old_book = f.engine.books.get(&(active.clone(), scope.clone())).unwrap().update.book.clone();

    for session in ["a", "b", "c"] {
        let mut heartbeat = okx_empty_books_frame(session, 1400, 12);
        heartbeat.raw = heartbeat.raw.replace(
            &(wall(1400) - 50).to_string(),
            &(wall(800) - 50).to_string(),
        );
        let out = f.engine.receive_batch(vec![heartbeat], ns(1400), wall(1400)).unwrap();
        assert!(!out.iter().any(|item| matches!(item, Output::Book(_, _))));
        f.handle(out);
    }

    let final_coverage = f.engine.coverage(ns(1400), wall(1400));
    let row = final_coverage.as_array().unwrap()
        .iter().find(|row| row["key"] == scope).unwrap();
    assert_eq!(row["hot_ready"].as_array().unwrap().len(), 3);
    assert_eq!(row["active"].as_str(), Some(active.as_str()));
    assert_eq!(f.engine.metrics.channel_keepalives, 3);
    assert_eq!(f.engine.metrics.switches, before_switches);
    assert_eq!(f.engine.selection_audit.len(), before_audit);
    assert_eq!(f.engine.metrics.invalid, before_invalid);
    assert_eq!(f.engine.metrics.duplicates, before_duplicates);
    assert_eq!(f.published, before_published);
    for session in ["a", "b", "c"] {
        let book = f.engine.books.get(&(session.to_string(), scope.clone())).unwrap();
        assert_eq!(book.update.book.sequence, old_book.sequence);
        assert_eq!(book.update.book.bids, old_book.bids);
        assert_eq!(book.update.book.asks, old_book.asks);
        assert_eq!(book.update.book.received_ns, old_book.received_ns);
        assert_eq!(book.update.book.exchange_ts_ms, old_book.exchange_ts_ms);
        let health = f.engine.manager.state(&book.manager_id).unwrap();
        assert_eq!(health.last_receive_ns, Some(ns(1400)));
        assert_eq!(health.last_valid_receive_ns, Some(ns(800)));
        assert_eq!(health.last_exchange_ms, old_book.exchange_ts_ms);
        assert_eq!(health.last_valid_exchange_ms, old_book.exchange_ts_ms);
    }
    let owner_index = crate::owner_index("BTC", 12);
    assert_eq!(f.owners[owner_index].snapshot_venue("BTC", Venue::OkxSpot)
        .unwrap().received_ns, old_book.received_ns);

    let stale_heartbeat = okx_empty_books_frame(&active, 1700, 12);
    let out = f.engine.receive_batch(vec![stale_heartbeat], ns(1700), wall(1700)).unwrap();
    assert!(!out.iter().any(|item| matches!(item, Output::Book(_, _))));
    f.handle(out);
    assert_eq!(f.published, before_published);
    assert!(f.auth.read().unwrap().is_empty());
    assert_eq!(f.engine.coverage(ns(1700), wall(1700)).as_array().unwrap()
        .iter().find(|row| row["key"] == scope).unwrap()["hot_ready"].as_array().unwrap().len(), 0);
    let active_book = f.engine.books.get(&(active.clone(), scope.clone())).unwrap();
    let health = f.engine.manager.state(&active_book.manager_id).unwrap();
    assert_eq!(health.last_receive_ns, Some(ns(1700)));
    assert_eq!(health.last_valid_receive_ns, Some(ns(800)));
    assert_eq!(health.last_valid_exchange_ms, old_book.exchange_ts_ms);
    assert_eq!(f.engine.metrics.channel_keepalives, 4);
    assert_eq!(f.engine.metrics.invalid, before_invalid);
    assert_eq!(f.engine.metrics.duplicates, before_duplicates);
    assert!(f.writer.as_ref().unwrap().verify_pair().is_ok());
    f.invariant("R1216_okx_empty_books_heartbeat_transport_liveness_only");
}

#[test]
fn r1216_okx_empty_books_requires_monotonic_exchange_time() {
    let mut f = Fixture::new();
    let scope = key(Venue::OkxSpot, "BTC");
    f.engine.universe.insert(scope.clone());
    for session in ["a", "b", "c"] {
        for (t, action, seq, prev, qty) in [
            (0, "snapshot", 10, None, "1"),
            (400, "update", 11, Some(10), "2"),
            (800, "update", 12, Some(11), "3"),
        ] {
            let out = f.engine.receive_batch(
                vec![okx_books_frame(session, t, action, seq, prev, qty)],
                ns(t), wall(t),
            ).unwrap();
            f.handle(out);
        }
    }
    let initial_coverage = f.engine.coverage(ns(800), wall(800));
    let row = initial_coverage.as_array().unwrap()
        .iter().find(|row| row["key"] == scope).unwrap();
    let active = row["active"].as_str().unwrap().to_string();
    let before = f.published;
    let mut heartbeat = okx_empty_books_frame(&active, 900, 12);
    heartbeat.raw = heartbeat.raw.replace(
        &(wall(900) - 50).to_string(),
        &(wall(800) - 51).to_string(),
    );
    let out = f.engine.receive_batch(vec![heartbeat], ns(900), wall(900)).unwrap();
    assert!(!out.iter().any(|item| matches!(item, Output::Frame(_) | Output::Book(_, _))));
    assert_eq!(f.engine.metrics.duplicates, 1);
    assert_eq!(f.published, before);
    assert!(f.auth.read().unwrap().contains_key(&scope));
    assert_eq!(f.engine.coverage(ns(900), wall(900)).as_array().unwrap()
        .iter().find(|row| row["key"] == scope).unwrap()["active"].as_str(), Some(active.as_str()));
    f.invariant("R1216_nonmonotonic_okx_empty_update_rejected");
}


#[cfg(test)]mod r1216_invalid_audit_tests{
 use super::*;
 #[test]
 fn invalid_audit_unwraps_combined_binance_event_fields(){
  let mut f=Fixture::new();let raw=frame("a",900,901);
  f.engine.record_invalid(&raw,"market_event_stale_or_clock_skew",true);
  let row=f.engine.invalid_audit().pop().unwrap();
  assert_eq!(row["source"],"a");assert_eq!(row["instrument"],"BTC");
  assert_eq!(row["channel"],"btcusdt@depth20@500ms");assert_eq!(row["sequence"],901);
  assert_eq!(row["timestamp_ms"],wall(900)-50);assert_eq!(row["event_age_ms"],50);
 }
 #[test]
 fn emergency_failover_audit_captures_both_readiness_causes(){
  let mut f=Fixture::new();f.warm();let scope=key(Venue::BinanceFutures,"BTC");
  let prior=f.engine.selected.get(&scope).unwrap().session.clone();
  let manager_id=f.engine.books.get(&(prior.clone(),scope.clone())).unwrap().manager_id.clone();
  f.engine.manager.quarantine(&manager_id);
  let next=if prior=="a"{"b"}else{"a"};f.feed(next,900,901);
  let row=f.engine.selection_audit().pop().unwrap();
  assert_eq!(row["reason"],"active_source_unready_emergency_failover");
  assert_eq!(row["old_readiness"]["manager_hot_ready"],false);
  assert_eq!(row["old_readiness"]["book_fresh"],true);
  assert_eq!(row["new_readiness"]["manager_hot_ready"],true);
 }
}


#[cfg(test)]
mod r1218_readiness_transition_tests {
    use super::*;

    #[test]
    fn stale_ws_timestamp_precedes_local_hot_ready_loss_and_is_audited() {
        let mut f = Fixture::new();
        f.warm();

        let mut stale = frame("a", 900, 901);
        let mut stale_value: serde_json::Value = serde_json::from_str(&stale.raw).unwrap();
        stale_value["data"]["E"] = serde_json::json!(wall(900) - 900);
        stale.raw = stale_value.to_string();
        assert_eq!(stale.received_wall_ms - (wall(900) - 900), 900);
        let out = f.engine.receive_batch(vec![stale], ns(900), wall(900)).unwrap();
        f.handle(out);

        let stale_row = f.engine.stale_frame_audit().pop().unwrap();
        assert_eq!(stale_row["source"], "a");
        assert_eq!(stale_row["event_age_ms"], 900);
        assert_eq!(stale_row["received_wall_ms"], wall(900));
        assert_eq!(stale_row["freshness_limit_ms"], 800);

        let out = f.engine.tick(ns(1601), wall(1601)).unwrap();
        f.handle(out);
        let lost = f.engine.readiness_audit().into_iter().find(|row| {
            row["kind"] == "source_readiness_predicate_change"
                && row["session"] == "a"
                && row["lost_predicates"].as_array().is_some_and(|lost| {
                    lost.iter().any(|predicate| predicate == "manager_hot_ready")
                })
        }).expect("HOT_READY loss should have a compact source record");
        assert_eq!(lost["context"]["manager_last_valid_receive_age_ms"], 801);
        assert_eq!(lost["context"]["manager_last_valid_exchange_age_ms"], 851);
        assert_eq!(lost["context"]["manager_last_event_age_at_websocket_receive_ms"], 900);
        assert_eq!(lost["context"]["manager_last_processing_delay_ns"], 0);
        assert_eq!(lost["current_predicates"]["last_event_fresh_at_websocket_receive"], false);
        assert!(lost["lost_predicates"].as_array().unwrap().iter()
            .any(|predicate| predicate == "effective_ready"));
        assert!(f.engine.metrics.readiness_audit_evictions == 0);
    }

    #[test]
    fn stale_frame_and_readiness_audits_have_explicit_bounds() {
        let mut f = Fixture::new();
        let raw = frame("a", 900, 901);
        for _ in 0..4097 {
            f.engine.record_invalid(&raw, "market_event_stale_or_clock_skew", true);
        }
        assert_eq!(f.engine.stale_frame_audit().len(), STALE_FRAME_AUDIT_LIMIT);
        assert_eq!(f.engine.metrics.stale_frame_audit_evictions, 4097-STALE_FRAME_AUDIT_LIMIT as u64);
        assert!(f.engine.stale_frame_audit_bytes<=STALE_FRAME_AUDIT_BYTES_LIMIT);
        assert_eq!(f.engine.stale_frame_audit_bytes,f.engine.stale_frame_audit.iter().map(audit_value_bytes).sum::<usize>());
        f.engine.push_stale_frame_audit(serde_json::json!({"payload":"x".repeat(STALE_FRAME_AUDIT_BYTES_LIMIT+1)}));
        assert_eq!(f.engine.metrics.stale_frame_audit_evictions, 4098-STALE_FRAME_AUDIT_LIMIT as u64);
        assert_eq!(f.engine.invalid_audit().len(), 64);
        assert_eq!(f.engine.metrics.invalid_audit_evictions, 4033);

        for n in 0..4097 {
            f.engine.push_readiness_audit(serde_json::json!({"n":n}));
        }
        assert_eq!(f.engine.readiness_audit().len(), READINESS_AUDIT_LIMIT);
        assert_eq!(f.engine.metrics.readiness_audit_evictions, 4097-READINESS_AUDIT_LIMIT as u64);
        assert!(f.engine.readiness_audit_bytes<=READINESS_AUDIT_BYTES_LIMIT);
        assert_eq!(f.engine.readiness_audit_bytes,f.engine.readiness_audit.iter().map(audit_value_bytes).sum::<usize>());
        f.engine.push_readiness_audit(serde_json::json!({"kind":"oversize","payload":"x".repeat(READINESS_AUDIT_BYTES_LIMIT+1)}));
        assert_eq!(f.engine.metrics.readiness_audit_evictions, 4098-READINESS_AUDIT_LIMIT as u64);
    }

    #[test]
    fn disconnect_releases_retired_session_readiness_state() {
        let mut f=Fixture::new();
        f.warm();
        assert!(f.engine.manager.readiness_has_session("a"));
        f.engine.connection_scopes.insert("a".into(),HashSet::from([key(Venue::BinanceFutures,"BTC")]));
        f.engine.disconnected("a",ns(801),wall(801)).unwrap();
        assert!(!f.engine.connection_scopes.contains_key("a"));
        assert!(!f.engine.manager.readiness_has_session("a"));
        assert!(f.engine.metrics.readiness_state_retirements>0);
    }
}

fn r1235_rehash(frame: &mut RawFrame) {
    let hash = openssl::sha::sha256(frame.raw.as_bytes())
        .iter().map(|v| format!("{v:02x}")).collect::<String>();
    frame.source.as_mut().unwrap().payload_sha256 = hash;
}

fn r1235_reset_candidate(session: &str) -> (RawFrame, crate::Book, crate::Book) {
    let mut frame = okx_books_frame(session, 900, "update", 5, Some(12), "4");
    {
        let source = frame.source.as_mut().unwrap();
        source.journal_id = "a".repeat(64);
        source.accepted_id = 1;
        source.connection_epoch = 7;
        source.selection_epoch = frame.epoch;
    }
    r1235_rehash(&mut frame);
    let prior = crate::Book {
        bids: vec![(100.0, 3.0)], asks: vec![(101.0, 3.0)],
        received_ns: ns(800), received_wall_ms: wall(800),
        exchange_ts_ms: Some(wall(800) - 50), changed_ns: ns(800),
        epoch: frame.epoch, sequence: Some(12),
    };
    let book = crate::Book {
        bids: vec![(100.0, 4.0)], asks: vec![(101.0, 3.0)],
        received_ns: frame.received_ns, received_wall_ms: frame.received_wall_ms,
        exchange_ts_ms: Some(wall(900) - 50), changed_ns: frame.received_ns,
        epoch: frame.epoch, sequence: Some(5),
    };
    (frame, book, prior)
}

#[test]
fn r1235_maintenance_reset_validator_fails_closed_on_invalid_variants() {
    let (valid, book, prior) = r1235_reset_candidate("active");
    assert!(crate::r1234_okx_books_maintenance_reset(&valid, "BTC", &book, &prior, 800));

    let (mut spoofed, spoof_book, spoof_prior) = r1235_reset_candidate("active");
    spoofed.source.as_mut().unwrap().payload_sha256 = "0".repeat(64);
    assert!(!crate::r1234_okx_books_maintenance_reset(
        &spoofed, "BTC", &spoof_book, &spoof_prior, 800
    ), "payload without matching source SHA must fail closed");

    let (duplicate_frame, duplicate_book, mut duplicate_prior) =
        r1235_reset_candidate("active");
    duplicate_prior.sequence = duplicate_book.sequence;
    assert!(!crate::r1234_okx_books_maintenance_reset(
        &duplicate_frame, "BTC", &duplicate_book, &duplicate_prior, 800
    ), "duplicate sequence must fail closed");

    let (mut skewed, mut skewed_book, skewed_prior) = r1235_reset_candidate("active");
    let original_ts = (wall(900) - 50).to_string();
    let future_ts = (wall(900) + 1).to_string();
    skewed.raw = skewed.raw.replace(&original_ts, &future_ts);
    skewed_book.exchange_ts_ms = Some(wall(900) + 1);
    r1235_rehash(&mut skewed);
    assert!(!crate::r1234_okx_books_maintenance_reset(
        &skewed, "BTC", &skewed_book, &skewed_prior, 800
    ), "future event timestamp must fail closed");

    let (mut stale, mut stale_book, stale_prior) = r1235_reset_candidate("active");
    let old_ts = (wall(900) - 50).to_string();
    let stale_ts = (wall(900) - 801).to_string();
    stale.raw = stale.raw.replace(&old_ts, &stale_ts);
    stale_book.exchange_ts_ms = Some(wall(900) - 801);
    r1235_rehash(&mut stale);
    assert!(!crate::r1234_okx_books_maintenance_reset(
        &stale, "BTC", &stale_book, &stale_prior, 800
    ), "event age over 800 ms must fail closed");

    let (mut wrong_instrument, wrong_book, wrong_prior) = r1235_reset_candidate("active");
    wrong_instrument.raw = wrong_instrument.raw.replace("BTC-USDT", "ETH-USDT");
    r1235_rehash(&mut wrong_instrument);
    assert!(!crate::r1234_okx_books_maintenance_reset(
        &wrong_instrument, "BTC", &wrong_book, &wrong_prior, 800
    ), "instrument mismatch must fail closed");

    let (mut switched, mut switched_book, switched_prior) =
        r1235_reset_candidate("active");
    switched.epoch += 1;
    switched.source.as_mut().unwrap().selection_epoch = switched.epoch;
    switched_book.epoch = switched.epoch;
    assert!(!crate::r1234_okx_books_maintenance_reset(
        &switched, "BTC", &switched_book, &switched_prior, 800
    ), "a reset cannot cross the prior book epoch");
}

#[test]
fn r1235_owner_rejects_maintenance_reset_from_same_epoch_failover_session() {
    let mut f = Fixture::new();
    f.engine.universe.insert(key(Venue::OkxSpot, "BTC"));
    for session in ["a", "b", "c"] {
        for (t, action, seq, prev, qty) in [
            (0, "snapshot", 10, None, "1"),
            (400, "update", 11, Some(10), "2"),
            (800, "update", 12, Some(11), "3"),
        ] {
            let out = f.engine.receive_batch(
                vec![okx_books_frame(session, t, action, seq, prev, qty)],
                ns(t), wall(t),
            ).unwrap();
            f.handle(out);
        }
    }
    let owner_index = crate::owner_index("BTC", 12);
    let (bound_session, epoch) = f.owners[owner_index]
        .source_binding("BTC", Venue::OkxSpot).expect("owner source must be bound");
    let failover = "new-failover-session";
    assert_ne!(bound_session, failover);

    let (mut frame, mut book, _) = r1235_reset_candidate(failover);
    frame.epoch = epoch;
    book.epoch = epoch;
    let source = frame.source.as_mut().unwrap();
    source.selection_epoch = epoch;
    r1235_rehash(&mut frame);
    let update = crate::Update { base: "BTC".into(), venue: Venue::OkxSpot, book };

    assert!(matches!(
        f.owners[owner_index].prepare_parsed(&frame, update),
        Err(crate::CoreError::StaleEpoch)
    ), "same-epoch session change must not inherit the previous source book");
}

#[cfg(test)]
mod r1240_store_worker_fault_tests {
    use super::*;
    use crate::native_r1173_compact_writer::ingress_store_r1205::{
        CommitDelayProbe, StoreWorker,
    };
    use std::{sync::{Arc, Mutex, atomic::Ordering}, thread, time::{Duration, Instant}};

    fn worker_fixture(delay_ms: u64) -> (PathBuf, Arc<Mutex<Store>>, StoreWorker, Arc<CommitDelayProbe>) {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let root = std::env::temp_dir().join(format!("r1240_store_worker_{}_{}", std::process::id(), nonce));
        std::fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
        let mut inbox = Store::open(&root).unwrap();
        let probe = Arc::new(CommitDelayProbe::default());
        probe.delay_ms.store(delay_ms, Ordering::Release);
        inbox.set_commit_delay_probe(probe.clone());
        let shared = Arc::new(Mutex::new(inbox));
        let worker = StoreWorker::start(shared.clone()).unwrap();
        (root, shared, worker, probe)
    }

    fn submit(worker: StoreWorker, mut raw: RawFrame) -> Result<u64, String> {
        worker.call(move |inbox, lock_wait_ns| {
            inbox.accept_batch_with_lock_wait(std::slice::from_mut(&mut raw), lock_wait_ns)?;
            Ok(raw.source.as_ref().ok_or("missing source")?.accepted_id)
        })
    }

    #[test]
    fn commit_delay_matrix_never_returns_accept_before_commit() {
        let (root, shared, worker, probe) = worker_fixture(0);
        for (index, delay_ms) in [100, 500, 1200, 2000].into_iter().enumerate() {
            probe.delay_ms.store(delay_ms, Ordering::Release);
            probe.started.store(false, Ordering::Release);
            let (done_tx, done_rx) = std::sync::mpsc::channel();
            let writer = worker.clone();
            let raw = frame("fault-delay", index as u64 + 10, index as i64 + 10);
            let started = Instant::now();
            let join = thread::spawn(move || {
                done_tx.send(submit(writer, raw)).unwrap();
            });
            let deadline = Instant::now() + Duration::from_secs(2);
            while !probe.started.load(Ordering::Acquire) && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(1));
            }
            assert!(probe.started.load(Ordering::Acquire), "commit delay probe did not start");
            thread::sleep(Duration::from_millis((delay_ms / 5).clamp(5, 30)));
            assert!(matches!(done_rx.try_recv(), Err(std::sync::mpsc::TryRecvError::Empty)),
                "accept returned before its delayed FULL commit finished");
            let accepted_id = done_rx.recv_timeout(Duration::from_secs(delay_ms / 1000 + 5)).unwrap().unwrap();
            assert_eq!(accepted_id, index as u64 + 1);
            assert!(started.elapsed() >= Duration::from_millis(delay_ms));
            join.join().unwrap();
            let counts = worker.call(|inbox, _| inbox.counts()).unwrap();
            assert_eq!((counts.accepted, counts.pending, counts.published), (index as u64 + 1, index as u64 + 1, 0));
        }
        drop(worker);
        drop(shared);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn bounded_writer_overflow_is_visible_and_fails_closed() {
        let (root, shared, worker, probe) = worker_fixture(2000);
        let (first_tx, first_rx) = std::sync::mpsc::channel();
        let first_worker = worker.clone();
        let first = thread::spawn(move || {
            first_tx.send(submit(first_worker, frame("fault-first", 20, 20))).unwrap();
        });
        let deadline = Instant::now() + Duration::from_secs(2);
        while !probe.started.load(Ordering::Acquire) && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(1));
        }
        assert!(probe.started.load(Ordering::Acquire), "first commit delay probe did not start");
        assert!(matches!(first_rx.try_recv(), Err(std::sync::mpsc::TryRecvError::Empty)));

        let extra_count = StoreWorker::QUEUE_CAPACITY + 1;
        let start = Arc::new(std::sync::Barrier::new(extra_count + 1));
        let mut submitters = Vec::with_capacity(extra_count);
        for index in 0..extra_count {
            let writer = worker.clone();
            let start = start.clone();
            submitters.push(thread::spawn(move || {
                start.wait();
                submit(writer, frame(&format!("fault-extra-{index}"), 21 + index as u64, 21 + index as i64))
            }));
        }
        start.wait();
        let overflow_deadline = Instant::now() + Duration::from_secs(1);
        while worker.profile()["overflow_fail_closed"].as_u64() != Some(1)
            && Instant::now() < overflow_deadline
        {
            thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(worker.profile()["overflow_fail_closed"].as_u64(), Some(1),
            "all queue slots should be occupied while the first commit is delayed");
        probe.delay_ms.store(0, Ordering::Release);
        let first_result = first_rx.recv_timeout(Duration::from_secs(6)).unwrap().unwrap();
        first.join().unwrap();
        let extras = submitters.into_iter().map(|thread| thread.join().unwrap()).collect::<Vec<_>>();
        assert_eq!(first_result, 1);
        assert_eq!(extras.iter().filter(|result| result.is_ok()).count(), StoreWorker::QUEUE_CAPACITY);
        assert_eq!(extras.iter().filter(|result| result.as_ref().is_err_and(|e| e.contains("queue full"))).count(), 1);
        let profile = worker.profile();
        assert_eq!(profile["overflow_fail_closed"].as_u64(), Some(1));
        assert_eq!(profile["queue_highwater_including_active"].as_u64(), Some((StoreWorker::QUEUE_CAPACITY + 1) as u64));
        let counts = worker.call(|inbox, _| inbox.counts()).unwrap();
        assert_eq!((counts.accepted, counts.pending, counts.published),
            ((StoreWorker::QUEUE_CAPACITY + 1) as u64, (StoreWorker::QUEUE_CAPACITY + 1) as u64, 0));
        drop(worker);
        drop(shared);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn commit_delay_over_800ms_keeps_freshness_cutoff_and_quarantines_frame() {
        let (root, shared, worker, _probe) = worker_fixture(1200);
        let authority = Arc::new(RwLock::new(HashMap::new()));
        let mut engine = Engine::new_with_store_worker(
            shared.clone(),
            worker.clone(),
            authority,
            vec![key(Venue::BinanceFutures, "BTC")],
        ).unwrap();
        for session in ["a", "b", "c"] { engine.register(session).unwrap(); }

        let received_ns = crate::ingress_runtime_r1205::mono_ns();
        let received_wall = crate::ingress_runtime_r1205::wall_ms();
        let mut delayed = frame("delay-stale", 0, 1);
        delayed.received_ns = received_ns.saturating_sub(50_000_000);
        delayed.received_wall_ms = received_wall - 50;
        let mut raw: serde_json::Value = serde_json::from_str(&delayed.raw).unwrap();
        raw["data"]["E"] = serde_json::json!(received_wall - 50);
        delayed.raw = raw.to_string();

        let outputs = engine.receive_batch(vec![delayed], received_ns, received_wall).unwrap();
        assert!(outputs.iter().all(|output| !matches!(output, Output::Frame(_) | Output::Book(_, _))),
            "a frame that aged beyond 800 ms during durable accept must not enter the owner path");
        let counts = worker.call(|inbox, _| inbox.counts()).unwrap();
        assert_eq!((counts.accepted, counts.published, counts.quarantined, counts.pending), (1, 0, 1, 0));
        assert_eq!(engine.metrics.invalid, 1);
        drop(engine);
        drop(worker);
        drop(shared);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bounded_ingress_reader_progresses_during_delayed_store_commit() {
        let (root, shared, worker, probe) = worker_fixture(1200);
        let authority = Arc::new(RwLock::new(HashMap::new()));
        let mut engine = Engine::new_with_store_worker(
            shared.clone(),
            worker.clone(),
            authority,
            vec![key(Venue::BinanceFutures, "BTC")],
        ).unwrap();
        for session in ["a", "b", "c"] { engine.register(session).unwrap(); }

        let (frame_tx, mut frame_rx) = tokio::sync::mpsc::channel(2);
        let receive_ns = crate::ingress_runtime_r1205::mono_ns();
        let receive_wall = crate::ingress_runtime_r1205::wall_ms();
        let mut delayed = frame("delay-reader", 0, 1);
        delayed.received_ns = receive_ns.saturating_sub(50_000_000);
        delayed.received_wall_ms = receive_wall - 50;
        let mut raw: serde_json::Value = serde_json::from_str(&delayed.raw).unwrap();
        raw["data"]["E"] = serde_json::json!(receive_wall - 50);
        delayed.raw = raw.to_string();
        frame_tx.send(delayed).await.unwrap();
        let first = frame_rx.recv().await.unwrap();

        let (progress_tx, progress_rx) = tokio::sync::oneshot::channel();
        let (third_tx, third_rx) = tokio::sync::oneshot::channel();
        let processor = tokio::spawn(async move {
            tokio::task::block_in_place(|| engine.receive_batch(vec![first], receive_ns, receive_wall))
        });

        tokio::time::timeout(Duration::from_secs(2), async {
            while !probe.started.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }).await.expect("delayed SQLite commit did not start");
        let reader = tokio::spawn(async move {
            frame_tx.send(frame("buffered-1", 1, 2)).await.unwrap();
            frame_tx.send(frame("buffered-2", 2, 3)).await.unwrap();
            progress_tx.send(()).unwrap();
            frame_tx.send(frame("backpressured-3", 3, 4)).await.unwrap();
            third_tx.send(()).unwrap();
        });
        tokio::time::timeout(Duration::from_millis(300), progress_rx).await
            .expect("reader could not fill its bounded ingress queue during the commit").unwrap();
        assert!(!processor.is_finished(), "delayed durable receive unexpectedly completed early");
        let mut third_rx = third_rx;
        assert!(tokio::time::timeout(Duration::from_millis(50), &mut third_rx).await.is_err(),
            "bounded ingress queue accepted more than its configured capacity");
        let outputs = tokio::time::timeout(Duration::from_secs(6), processor).await.unwrap().unwrap().unwrap();
        assert!(outputs.iter().all(|output| !matches!(output, Output::Frame(_) | Output::Book(_, _))));
        assert_eq!(frame_rx.recv().await.unwrap().source.as_ref().unwrap().session_id, "buffered-1");
        assert_eq!(frame_rx.recv().await.unwrap().source.as_ref().unwrap().session_id, "buffered-2");
        tokio::time::timeout(Duration::from_secs(1), &mut third_rx).await.unwrap().unwrap();
        reader.await.unwrap();
        let counts = worker.call(|inbox, _| inbox.counts()).unwrap();
        assert_eq!((counts.accepted, counts.published, counts.quarantined, counts.pending), (1, 0, 1, 0));
        drop(worker);
        drop(shared);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn sqlite_commit_failure_returns_no_accept_ack_and_leaves_no_row() {
        let (root, shared, worker, _probe) = worker_fixture(0);
        worker.call(|inbox, _| inbox.fault_full_pages()).unwrap();
        let mut raw = frame("fault-full", 30, 30);
        raw.raw = "x".repeat(32_700);
        let result = submit(worker.clone(), raw);
        assert!(result.as_ref().is_err_and(|error| error.contains("sqlite_code=13")));
        let counts = worker.call(|inbox, _| inbox.counts()).unwrap();
        assert_eq!((counts.received, counts.accepted, counts.pending, counts.published), (0, 0, 0, 0));
        drop(worker);
        drop(shared);
        std::fs::remove_dir_all(root).unwrap();
    }

    fn percentile_us(samples: &mut [u64], percentile: u64) -> u64 {
        if samples.is_empty() { return 0; }
        samples.sort_unstable();
        let rank = (samples.len() as u64 * percentile + 99) / 100;
        samples[rank.saturating_sub(1).min(samples.len() as u64 - 1) as usize]
    }

    #[test]
    fn one_writer_benchmark_with_2_4_8_ingress_submitters() {
        for submitters_count in [2usize, 4, 8] {
            let (root, shared, worker, _probe) = worker_fixture(0);
            let start = Arc::new(std::sync::Barrier::new(submitters_count + 1));
            let mut submitters = Vec::new();
            for worker_index in 0..submitters_count {
                let writer = worker.clone();
                let start = start.clone();
                submitters.push(thread::spawn(move || {
                    let mut latencies_us = Vec::new();
                    let mut accepted = 0u64;
                    let mut errors = 0u64;
                    start.wait();
                    for event_index in 0..32u64 {
                        let began = Instant::now();
                        let session = format!("bench-{worker_index}");
                        match submit(writer.clone(), frame(&session, event_index, event_index as i64 + 1)) {
                            Ok(_) => accepted += 1,
                            Err(_) => errors += 1,
                        }
                        latencies_us.push(began.elapsed().as_micros() as u64);
                    }
                    (accepted, errors, latencies_us)
                }));
            }
            let began = Instant::now();
            start.wait();
            let mut accepted = 0u64;
            let mut errors = 0u64;
            let mut latencies = Vec::new();
            for submitter in submitters {
                let (thread_accepted, thread_errors, mut thread_latencies) = submitter.join().unwrap();
                accepted += thread_accepted;
                errors += thread_errors;
                latencies.append(&mut thread_latencies);
            }
            let elapsed = began.elapsed();
            let counts = worker.call(|inbox, _| inbox.counts()).unwrap();
            assert_eq!(counts.accepted, accepted);
            assert_eq!(counts.pending, accepted);
            let mut percentile_samples = latencies.clone();
            println!("R1240_ONE_WRITER_BENCH {}", serde_json::json!({
                "sqlite_writer_threads": 1,
                "ingress_submitters": submitters_count,
                "requested": submitters_count * 32,
                "accepted": accepted,
                "explicit_errors": errors,
                "elapsed_ms": elapsed.as_millis(),
                "throughput_accepted_per_sec": if elapsed.as_secs_f64() == 0.0 { 0.0 } else { accepted as f64 / elapsed.as_secs_f64() },
                "call_latency_us": {
                    "p50": percentile_us(&mut percentile_samples, 50),
                    "p95": percentile_us(&mut percentile_samples, 95),
                    "p99": percentile_us(&mut percentile_samples, 99),
                    "max": percentile_samples.last().copied().unwrap_or(0)
                },
                "durable_writer": worker.profile(),
                "host_cpus": std::thread::available_parallelism().map(|count| count.get()).unwrap_or(0)
            }));
            drop(worker);
            drop(shared);
            std::fs::remove_dir_all(root).unwrap();
        }
    }

    #[test]
    fn worker_sigkill_child() {
        let Ok(root) = std::env::var("R1240_WORKER_SIGKILL_ROOT") else { return };
        let store = Arc::new(Mutex::new(Store::open(std::path::Path::new(&root)).unwrap()));
        let worker = StoreWorker::start(store).unwrap();
        submit(worker.clone(), frame("worker-child", 40, 40)).unwrap();
        std::fs::write(std::path::Path::new(&root).join("ready"), b"committed").unwrap();
        loop { thread::sleep(Duration::from_millis(50)); }
    }

    #[test]
    fn worker_commit_survives_process_kill_and_reopens_as_pending() {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let root = std::env::temp_dir().join(format!("r1240_worker_crash_{}_{}", std::process::id(), nonce));
        std::fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "market_data_ingress_r1205::tests::r1240_store_worker_fault_tests::worker_sigkill_child", "--nocapture"])
            .env("R1240_WORKER_SIGKILL_ROOT", &root)
            .stdout(std::process::Stdio::null())
            .spawn().unwrap();
        let started = Instant::now();
        while !root.join("ready").exists() && started.elapsed() < Duration::from_secs(5) {
            thread::sleep(Duration::from_millis(10));
        }
        let committed = root.join("ready").exists();
        child.kill().unwrap();
        child.wait().unwrap();
        assert!(committed, "child did not complete the FULL writer call");
        let recovered = Store::open(&root).unwrap();
        let counts = recovered.counts().unwrap();
        let pending = recovered.pending().unwrap();
        assert_eq!((counts.accepted, counts.pending, counts.published), (1, 1, 0));
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].source.as_ref().unwrap().session_id, "worker-child");
        drop(recovered);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[cfg(test)]
mod r1241_custody_integration {
 use super::*;
 use crate::custody_executor_r1241::CustodyWorker;
 use crate::native_r1173_compact_writer::r1241_next_commit_hook;
 use std::time::Duration;
 fn accepted(f:&mut Fixture)->(RawFrame,crate::Update,usize){
  let mut raw=frame("a",0,1);
  f.store.lock().unwrap().accept_batch(std::slice::from_mut(&mut raw)).unwrap();
  raw.source.as_mut().unwrap().selection_epoch=1;
  let update=parse(&raw).unwrap();let idx=crate::owner_index("BTC",12);
  (raw,update,idx)
 }
 async fn recover(worker:&CustodyWorker,store:SharedStore){
  worker.call(move|w|{
   w.recover_ingress_observer(&*store.lock().unwrap())?;
   reconcile(&store,w)?;
   w.sweep_ingress_receipts(&mut *store.lock().unwrap())?;
   w.require_decisions_drained()?;w.verify_pair()?;
   assert!(!w.may_resume_old_book_as_fresh());assert!(!w.may_trade_real());
   Ok(())
  }).await.unwrap();
 }
 #[tokio::test(flavor="current_thread")]
 async fn r1241_actual_engine_progresses_during_canonical_commit_stalls(){
  for delay_ms in [100u64,800,1200,2000]{
   let mut f=Fixture::new();drop(f.writer.take());
   let worker=Arc::new(CustodyWorker::open(f.db.clone(),true).await.unwrap());
   let (raw,update,idx)=accepted(&mut f);let mut owners=std::mem::take(&mut f.owners);
   let (entered_tx,entered_rx)=tokio::sync::oneshot::channel();
   let (release_tx,release_rx)=std::sync::mpsc::channel();
   let task_worker=worker.clone();let committed_frame=raw.clone();
   let task=tokio::spawn(async move{task_worker.call(move|w|{
    r1241_next_commit_hook(Box::new(move||{
     let _=entered_tx.send(());
     release_rx.recv_timeout(Duration::from_secs(10)).unwrap();
     std::thread::sleep(Duration::from_millis(delay_ms));
    }));
    w.source_commit_guarded(&mut owners,&[(committed_frame,update,idx)],&[],Inject::None)?;
    Ok(owners)
   }).await});
   tokio::time::timeout(Duration::from_secs(10),entered_rx).await.unwrap().unwrap();
   // Run the ACTUAL Engine while canonical DB1 is inside COMMIT. This would
   // deadlock on this one-thread runtime if custody still ran synchronously.
   let before=f.engine.metrics.received;
   for t in [0,400,800]{
    for session in ["b","c"]{
     let _=f.engine.receive_batch(vec![frame(session,t,t as i64+2)],ns(t),wall(t)).unwrap();
    }
   }
   assert_eq!(f.engine.metrics.received,before+6);
   assert!(!task.is_finished(),"ACK cannot precede the stalled COMMIT");
   assert_eq!(f.store.lock().unwrap().counts().unwrap().published,0);
   release_tx.send(()).unwrap();
   f.owners=task.await.unwrap().unwrap();
   let frame_for_seal=raw.clone();
   worker.call(move|w|{w.note_guarded_decision(&frame_for_seal,"DECISION_DENIED",&digest(b"r1241-stall"))?;w.verify_pair()}).await.unwrap();
   recover(&worker,f.store.clone()).await;
   let counts=f.store.lock().unwrap().counts().unwrap();
   assert_eq!(counts.pending,0);
   assert_eq!(counts.accepted,counts.published+counts.quarantined);
   println!("R1241_STALL {}",serde_json::json!({"delay_ms":delay_ms,"engine_frames_during_blocked_commit":6,"counts":counts,"executor":worker.profile(),"orders":0}));
   Arc::try_unwrap(worker).ok().unwrap().shutdown().await.unwrap();
  }
 }
 #[tokio::test(flavor="current_thread")]
 async fn r1241_writer_failure_requires_restart_and_exact_recovery(){
  for inject in [Inject::AfterDb1,Inject::AfterDb2]{
   let mut f=Fixture::new();drop(f.writer.take());
   let worker=CustodyWorker::open(f.db.clone(),true).await.unwrap();
   let item=accepted(&mut f);let mut owners=std::mem::take(&mut f.owners);
   assert!(worker.call(move|w|w.source_commit_guarded(&mut owners,&[item],&[],inject)).await.is_err());
   assert!(worker.call(|w|w.cursor()).await.is_err(),"failed executor must remain fenced");
   assert_eq!(f.store.lock().unwrap().counts().unwrap().published,0);
   worker.shutdown().await.unwrap();
   let worker=CustodyWorker::open(f.db.clone(),true).await.unwrap();
   recover(&worker,f.store.clone()).await;
   let first=f.store.lock().unwrap().counts().unwrap();
   assert_eq!(first.pending,0);
   recover(&worker,f.store.clone()).await;
   let second=f.store.lock().unwrap().counts().unwrap();
   assert_eq!(first.accepted,second.accepted);assert_eq!(first.published,second.published);
   println!("R1241_RESTART {:?}",inject);
   worker.shutdown().await.unwrap();
  }
 }
 #[tokio::test(flavor="current_thread")]
 async fn r1241_cancel_after_db1_cannot_publish_false_ack(){
  let mut f=Fixture::new();drop(f.writer.take());
  let worker=Arc::new(CustodyWorker::open(f.db.clone(),true).await.unwrap());
  let item=accepted(&mut f);let mut owners=std::mem::take(&mut f.owners);
  let (entered_tx,entered_rx)=tokio::sync::oneshot::channel();
  let (release_tx,release_rx)=std::sync::mpsc::channel();
  let cloned=worker.clone();
  let task=tokio::spawn(async move{cloned.call(move|w|{
   let result=w.source_commit_guarded(&mut owners,&[item],&[],Inject::AfterDb1);
   let _=entered_tx.send(());release_rx.recv_timeout(Duration::from_secs(10)).unwrap();result
  }).await});
  entered_rx.await.unwrap();task.abort();assert!(task.await.unwrap_err().is_cancelled());
  assert!(worker.call(|w|w.cursor()).await.is_err());
  assert_eq!(f.store.lock().unwrap().counts().unwrap().published,0);
  release_tx.send(()).unwrap();
  Arc::try_unwrap(worker).ok().unwrap().shutdown().await.unwrap();
  let restarted=CustodyWorker::open(f.db.clone(),true).await.unwrap();
  recover(&restarted,f.store.clone()).await;restarted.shutdown().await.unwrap();
 }
 #[tokio::test(flavor="current_thread")]
 async fn r1241_queue_saturation_is_bounded_and_fails_closed(){
  let mut f=Fixture::new();drop(f.writer.take());
  let worker=Arc::new(CustodyWorker::open(f.db.clone(),true).await.unwrap());
  let (entered_tx,entered_rx)=tokio::sync::oneshot::channel();
  let (release_tx,release_rx)=std::sync::mpsc::channel();
  let first_worker=worker.clone();
  let first=tokio::spawn(async move{first_worker.call(move|_|{
   let _=entered_tx.send(());release_rx.recv_timeout(Duration::from_secs(10)).unwrap();Ok(())
  }).await});
  entered_rx.await.unwrap();
  let executed=Arc::new(std::sync::atomic::AtomicU64::new(0));let mut tasks=Vec::new();
  for _ in 0..=CustodyWorker::CAPACITY{
   let w=worker.clone();let count=executed.clone();
   tasks.push(tokio::spawn(async move{w.call(move|_|{count.fetch_add(1,std::sync::atomic::Ordering::SeqCst);Ok(())}).await}));
   tokio::task::yield_now().await;
  }
  assert_eq!(worker.profile()["overflow"],1);
  assert_eq!(worker.profile()["highwater_including_active"],(CustodyWorker::CAPACITY+1)as u64);
  release_tx.send(()).unwrap();let _=first.await.unwrap();
  for task in tasks{assert!(task.await.unwrap().is_err());}
  assert_eq!(executed.load(std::sync::atomic::Ordering::SeqCst),0);
  Arc::try_unwrap(worker).ok().unwrap().shutdown().await.unwrap();
 }
}

#[cfg(test)]
mod r1241_process_crash_tests {
 use super::*;
 use crate::custody_executor_r1241::CustodyWorker;
 #[test]
 fn r1241_crash_child(){
  let Ok(phase)=std::env::var("R1241_TEST_CRASH_PHASE") else{return};
  let root=PathBuf::from(std::env::var("R1241_TEST_CRASH_DB").unwrap());
  let raw_json=std::env::var("R1241_TEST_CRASH_FRAME").unwrap();
  let rt=tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
  rt.block_on(async move{
   let mut raw:RawFrame=serde_json::from_str(&raw_json).unwrap();raw.source.as_mut().unwrap().selection_epoch=1;raw.epoch=1;
   let update=parse(&raw).unwrap();let idx=crate::owner_index(&update.base,12);
   let mut owners=(0..12).map(|i|crate::Owner::new(i,12,800).unwrap()).collect::<Vec<_>>();
   let worker=CustodyWorker::open(root,true).await.unwrap();
   worker.call(move|w|->Result<(),String>{
    if phase=="BeforeDb1"{std::process::exit(71)}
    let inject=match phase.as_str(){"AfterDb1"=>Inject::AfterDb1,"AfterDb2"=>Inject::AfterDb2,"AfterAck"=>Inject::None,_=>panic!("unknown crash phase")};
    let result=w.source_commit_guarded(&mut owners,&[(raw,update,idx)],&[],inject);
    if inject==Inject::None{assert!(result.is_ok())}else{assert!(result.is_err())}
    // Deliberately skip destructors: exercise OS lock release and WAL recovery.
    std::process::exit(71)
   }).await.unwrap();
  });
 }
 #[test]
 fn r1241_process_exit_before_after_db1_db2_ack_recovers_idempotently(){
  let rt=tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
  for phase in ["BeforeDb1","AfterDb1","AfterDb2","AfterAck"]{
   let mut f=Fixture::new();
   let mut raw=frame("a",0,1);f.store.lock().unwrap().accept_batch(std::slice::from_mut(&mut raw)).unwrap();
   drop(f.writer.take());
   let status=std::process::Command::new(std::env::current_exe().unwrap())
    .args(["--exact","market_data_ingress_r1205::tests::r1241_process_crash_tests::r1241_crash_child","--nocapture"])
    .env("R1241_TEST_CRASH_PHASE",phase).env("R1241_TEST_CRASH_DB",&f.db).env("R1241_TEST_CRASH_FRAME",serde_json::to_string(&raw).unwrap())
    .status().unwrap();
   assert_eq!(status.code(),Some(71),"child must reach requested crash point");
   assert_eq!(f.store.lock().unwrap().counts().unwrap().published,0,"no consumer ACK before recovery");
   rt.block_on(async{
    let worker=CustodyWorker::open(f.db.clone(),true).await.unwrap();
    let mut heads=Vec::new();
    for _ in 0..2{
     let store=f.store.clone();
     heads.push(worker.call(move|w|{
      w.recover_ingress_observer(&*store.lock().unwrap())?;reconcile(&store,w)?;
      w.sweep_ingress_receipts(&mut *store.lock().unwrap())?;
      w.require_decisions_drained()?;w.verify_pair()?;
      assert!(!w.may_resume_old_book_as_fresh());assert!(!w.may_trade_real());
      Ok(w.cursor()?)
     }).await.unwrap());
    }
    assert_eq!(heads[0],heads[1],"recovery must be idempotent");
    assert_eq!(f.store.lock().unwrap().counts().unwrap().pending,0);
    println!("R1241_PROCESS_CRASH {}",serde_json::json!({"phase":phase,"cursor":heads[1],"counts":f.store.lock().unwrap().counts().unwrap(),"orders":0}));
    worker.shutdown().await.unwrap();
   });
  }
 }
}

#[cfg(test)]
mod r1241_worker_comparison {
 use super::*;
 use crate::custody_executor_r1241::CustodyWorker;
 fn cpu_us()->u64{
  let mut usage:libc::rusage=unsafe{std::mem::zeroed()};
  assert_eq!(unsafe{libc::getrusage(libc::RUSAGE_SELF,&mut usage)},0);
  ((usage.ru_utime.tv_sec+usage.ru_stime.tv_sec)*1_000_000+usage.ru_utime.tv_usec+usage.ru_stime.tv_usec)as u64
 }
 #[test]
 fn r1241_identical_actual_engine_replay_2_4_8_runtime_workers(){
  for threads in [2,4,8]{
   let rt=tokio::runtime::Builder::new_multi_thread().worker_threads(threads).enable_all().build().unwrap();
   rt.block_on(async{
    let mut f=Fixture::new();drop(f.writer.take());
    f.engine.universe.insert(key(Venue::OkxSpot,"BTC"));
    let worker=CustodyWorker::open(f.db.clone(),true).await.unwrap();
    let start=std::time::Instant::now();let cpu_start=cpu_us();let mut timings=Vec::new();let mut input_count=0;
    for tick in 0..32u64{
     let t=tick*100;
     for session in ["a","b","c"]{
      let inputs=vec![frame(session,t,tick as i64+1),
       okx_books_frame(session,t,if tick==0{"snapshot"}else{"update"},tick as i64+1,if tick==0{None}else{Some(tick as i64)},"3")];
      for raw in inputs{
       let event_start=std::time::Instant::now();input_count+=1;
       let outputs=f.engine.receive_batch(vec![raw],ns(t),wall(t)).unwrap();
       for output in outputs{
        let (output,normalized)=match output{Output::Book(raw,update)=>(Output::Frame(raw),Some(update)),other=>(other,None)};
        match output{
         Output::Reset{venue,base,epoch}=>{
          let mut owned=std::mem::take(&mut f.owners);
          f.owners=worker.call(move|w|{w.source_reset(&mut owned,venue,&[base],epoch,wall(t),"R1162_INFERRED_OFFLINE",Inject::None)?;Ok(owned)}).await.unwrap();
         },
         Output::Book(_,_)=>unreachable!("normalized above"),
         Output::Frame(raw)=>{
          let update=normalized.unwrap_or_else(||parse(&raw).unwrap());
          if authorization_at_normalized(&f.auth,&raw,&update,ns(t),wall(t)).is_none(){
           f.store.lock().unwrap().finish(&raw,"quarantined","benchmark_fenced",None).unwrap();continue;
          }
          let mut owned=std::mem::take(&mut f.owners);let store=f.store.clone();
          let idx=crate::owner_index(&update.base,12);
          f.owners=worker.call(move|w|{
           w.source_commit_guarded(&mut owned,&[(raw.clone(),update,idx)],&[],Inject::None)?;
           w.note_guarded_decision(&raw,"DECISION_DENIED",&digest(b"R1241_IDENTICAL_REPLAY_NO_EXECUTION"))?;
           let verified=w.verify_ingress_batch(std::slice::from_ref(&raw))?;
           let committed=store.lock().unwrap().finish_published_batch_verified(&[raw],&verified)?;
           let ticket=w.prepare_ingress_release_after_publish(&verified,&committed)?.ok_or("release missing")?;
           w.commit_ingress_release(ticket)?;let acked=w.confirm_ingress_reclaimed(&committed)?;
           store.lock().unwrap().forget_reclaimed_after_ack(&acked)?;
           Ok(owned)
          }).await.unwrap();
         },
         Output::Blocked{..}|Output::Resubscribe{..}=>{}
        }
       }
       timings.push(event_start.elapsed().as_nanos()as u64);
      }
     }
    }
    let store=f.store.clone();
    worker.call(move|w|{
     w.recover_ingress_observer(&*store.lock().unwrap())?;reconcile(&store,w)?;
     w.sweep_ingress_receipts(&mut *store.lock().unwrap())?;w.require_decisions_drained()?;w.verify_pair()
    }).await.unwrap();
    let elapsed=start.elapsed();timings.sort_unstable();
    let counts=f.store.lock().unwrap().counts().unwrap();
    assert_eq!(counts.pending,0);assert_eq!(counts.accepted,counts.published+counts.quarantined);
    assert_eq!(input_count,192);
    println!("R1241_WORKERS {}",serde_json::json!({"runtime_workers":threads,"processing_actors":1,"canonical_writers":1,"workload":"32 ticks x 3 sessions x 2 venues BTC","input_frames":input_count,"elapsed_ns":elapsed.as_nanos()as u64,"process_cpu_us":cpu_us()-cpu_start,"throughput_hz":input_count as f64/elapsed.as_secs_f64(),"receive_to_terminal_ns":{"p50":timings[timings.len()*50/100],"p95":timings[timings.len()*95/100],"p99":timings[timings.len()*99/100],"max":timings.last()},"counts":counts,"executor":worker.profile(),"orders":0}));
    worker.shutdown().await.unwrap();
   });
  }
 }
}

#[test]
fn r1268_stale_initial_okx_snapshot_requests_recovery_before_delta() {
    let auth: Authority = Arc::new(RwLock::new(HashMap::new()));
    let mut engine = Engine::new_volatile(auth, vec![key(Venue::OkxSpot, "XRP")]).unwrap();
    engine.clock = None;
    let session = "g0-OkxSpot-0-1-r1268-replay";
    engine.register_connection_scoped(session, Venue::OkxSpot, 1, &["XRP".to_owned()]).unwrap();
    engine.mark_subscribed(session);

    let stale: serde_json::Value = serde_json::from_str(include_str!("../tests/fixtures/okx_stale_initial_snapshot.json")).unwrap();
    let stale_wall = stale["received_wall_ms"].as_i64().unwrap();
    let stale_raw = stale["raw"].as_str().unwrap().to_owned();
    let stale_frame = RawFrame { venue: Venue::OkxSpot, epoch: 1, received_ns: ns(0), received_wall_ms: stale_wall, raw: stale_raw, source: Some(provenance(session.into(), 1)) };
    let first = engine.receive_batch(vec![stale_frame], ns(0) + 1, stale_wall).unwrap();
    assert!(first.iter().any(|o| matches!(o, Output::Resubscribe { base, .. } if base == "XRP")));
    assert!(engine.resyncing.contains(&(session.into(), "XRP".into())));

    let mut delta = okx_books_frame(session, 1, "update", 58_676_039_462, Some(58_676_039_461), "2");
    let mut delta_json: serde_json::Value = serde_json::from_str(&delta.raw).unwrap();
    delta_json["arg"]["instId"] = "XRP-USDT".into();
    delta_json["data"][0]["ts"] = (stale_wall + 40).to_string().into();
    delta.raw = delta_json.to_string();
    delta.received_ns = ns(1);
    delta.received_wall_ms = stale_wall + 90;
    let _ = engine.receive_batch(vec![delta], ns(1), stale_wall + 90).unwrap();
    assert_eq!(engine.metrics.invalid_categories.get("okx_delta_before_recovery_snapshot"), Some(&1));
    assert!(!engine.metrics.invalid_categories.contains_key("okx_book_parse_or_sequence"));
    assert!(engine.market_data_processor.diagnostic_state(session, "XRP").is_none());

    let recovery: serde_json::Value = serde_json::from_str(include_str!("../tests/fixtures/okx_fresh_recovery_snapshot.json")).unwrap();
    let recovery_wall = recovery["received_wall_ms"].as_i64().unwrap();
    let recovery_seq = recovery["raw"].as_str().and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
        .and_then(|v| v["data"][0]["seqId"].as_i64()).unwrap();
    let recovery_frame = RawFrame { venue: Venue::OkxSpot, epoch: 1, received_ns: ns(1000), received_wall_ms: recovery_wall,
        raw: recovery["raw"].as_str().unwrap().to_owned(), source: Some(provenance(session.into(), 1)) };
    let _ = engine.receive_batch(vec![recovery_frame], ns(1000), recovery_wall).unwrap();
    assert!(!engine.resyncing.contains(&(session.into(), "XRP".into())));
    assert_eq!(engine.market_data_processor.diagnostic_state(session, "XRP").map(|v| v.0), Some(recovery_seq));
}

#[test]
fn r1268_captured_binance_depth20_frames_replay_to_hot_healthy_control() {
    let auth: Authority = Arc::new(RwLock::new(HashMap::new()));
    let mut engine = Engine::new_volatile(auth, vec![key(Venue::BinanceFutures, "BTC")]).unwrap();
    engine.clock = None;
    let fixtures = include_str!("../tests/fixtures/binance_depth20_live_healthy_control.jsonl");
    let rows = fixtures.lines().map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap()).collect::<Vec<_>>();
    let session = rows[0]["source_id"].as_str().unwrap().to_owned();
    let generation = rows[0]["socket_generation"].as_u64().unwrap();
    engine.register_connection_scoped(&session, Venue::BinanceFutures, generation, &["BTC".to_owned()]).unwrap();
    let first_mono = rows[0]["received_mono_ns"].as_u64().unwrap();
    let base_mono = 1_000_000_000u64;
    let mut last_wall = 0i64;
    let mut last_delta = 0u64;
    for row in rows {
        let original_mono = row["received_mono_ns"].as_u64().unwrap();
        let delta = original_mono - first_mono;
        let recv_ns = base_mono + delta;
        let recv_wall = row["received_wall_ms"].as_i64().unwrap();
        let frame = RawFrame {
            venue: Venue::BinanceFutures, epoch: 1, received_ns: recv_ns, received_wall_ms: recv_wall,
            raw: row["raw"].as_str().unwrap().to_owned(),
            source: Some(provenance(session.clone(), generation)),
        };
        let _ = engine.receive_batch(vec![frame], recv_ns, recv_wall).unwrap();
        last_wall = recv_wall;
        last_delta = delta;
    }
    assert!(engine.hot_complete(&session, Venue::BinanceFutures, &["BTC".to_owned()], base_mono + last_delta + 10_000_000, last_wall + 10));
    assert_eq!(engine.metrics.invalid, 0);
}

#[test]
fn r1271_h1_resyncing_delta_requests_retry_after_cooldown() {
    let mut f = Fixture::new();
    let scope = key(Venue::OkxSpot, "BTC");
    f.engine.universe.insert(scope.clone());

    for session in ["a", "b", "c"] {
        for (t, action, seq, prev) in [
            (0, "snapshot", 10, None),
            (400, "update", 11, Some(10)),
            (800, "update", 12, Some(11)),
        ] {
            let out = f.engine.receive_batch(
                vec![okx_books_frame(session, t, action, seq, prev, "1")],
                ns(t), wall(t)
            ).unwrap();
            f.handle(out);
        }
    }
    let row = f.engine.coverage(ns(800), wall(800)).as_array().unwrap()
        .iter().find(|r| r["key"] == scope).unwrap().clone();
    assert_eq!(row["hot_ready"].as_array().unwrap().len(), 3);

    let gap = okx_books_frame("a", 850, "update", 14, Some(13), "2");
    let first = f.engine.receive_batch(vec![gap], ns(850), wall(850)).unwrap();
    assert!(first.iter().any(|o| matches!(o, Output::Resubscribe {
        session, base, reason
    } if session == "a" && base == "BTC" && reason == "sequence gap")));
    f.handle(first);
    assert!(f.engine.resyncing.contains(&("a".into(), "BTC".into())));
    assert_eq!(f.engine.resubscribe_last_ns.get(&("a".into(), "BTC".into())), Some(&ns(850)));
    assert!(!f.auth.read().unwrap().contains_key(&scope));
    assert_eq!(f.engine.market_data_processor.diagnostic_state("a", "BTC"), None,
        "sequence-gap recovery must clear the failed session's L2 builder");

    let stale_snapshot = |t: u64| {
        let mut frame = okx_books_frame("a", t, "snapshot", 100, None, "3");
        let mut value: serde_json::Value = serde_json::from_str(&frame.raw).unwrap();
        value["data"][0]["ts"] = (wall(t) - 901).to_string().into();
        frame.raw = value.to_string();
        frame
    };
    let stale = f.engine.receive_batch(
        vec![stale_snapshot(900)], ns(900), wall(900)
    ).unwrap();
    assert!(!stale.iter().any(|o| matches!(o, Output::Resubscribe { .. })),
        "stale snapshot inside cooldown must not flood resubscribe requests");
    f.handle(stale);

    let delta = |t: u64, seq: i64, prev: i64| {
        okx_books_frame("a", t, "update", seq, Some(prev), "4")
    };
    for t in [1_000, 5_000] {
        let out = f.engine.receive_batch(vec![delta(t, 15, 14)], ns(t), wall(t)).unwrap();
        assert!(!out.iter().any(|o| matches!(o, Output::Book(_, _) | Output::Frame(_))),
            "delta while resyncing must remain rejected");
        assert!(!out.iter().any(|o| matches!(o, Output::Resubscribe { .. })),
            "duplicate deltas inside cooldown must not flood requests");
        f.handle(out);
        assert_eq!(f.engine.market_data_processor.diagnostic_state("a", "BTC"), None,
            "rejected delta must not create or advance the L2 builder");
    }

    let retry_at = 850 + 15_000 + 1;
    for (n, t) in (1_400..retry_at).step_by(400).enumerate() {
        let seq = 13 + n as i64;
        for session in ["b", "c"] {
            let refresh = okx_books_frame(session, t, "update", seq, Some(seq - 1), "5");
            let out = f.engine.receive_batch(vec![refresh], ns(t), wall(t)).unwrap();
            f.handle(out);
        }
    }
    let retry = f.engine.receive_batch(
        vec![delta(retry_at, 15, 14)], ns(retry_at), wall(retry_at)
    ).unwrap();
    assert!(!retry.iter().any(|o| matches!(o, Output::Book(_, _) | Output::Frame(_))),
        "retry-triggering delta must still be rejected");
    assert!(retry.iter().any(|o| matches!(o, Output::Resubscribe {
        session, base, reason
    } if session == "a" && base == "BTC" && reason == "delta before recovery snapshot")),
        "a valid delta after cooldown should request a fresh snapshot");
    f.handle(retry);
    assert_eq!(f.engine.market_data_processor.diagnostic_state("a", "BTC"), None,
        "retry-triggering delta must not create or advance a book");
    let coverage = f.engine.coverage(ns(retry_at), wall(retry_at));
    let row = coverage.as_array().unwrap().iter().find(|r| r["key"] == scope).unwrap();
    assert_eq!(row["hot_ready"].as_array().unwrap().len(), 2);
    assert!(!row["hot_ready"].as_array().unwrap().iter().any(|v| v == "a"),
        "resyncing source must not be counted HOT");
    assert_ne!(row["active"].as_str(), Some("a"),
        "resyncing source must not be active");

    let duplicate = f.engine.receive_batch(
        vec![delta(retry_at + 100, 15, 14)], ns(retry_at + 100), wall(retry_at + 100)
    ).unwrap();
    assert!(!duplicate.iter().any(|o| matches!(o, Output::Resubscribe { .. })),
        "retry timestamp must advance to enforce the next cooldown");
    f.handle(duplicate);

    let recovery = okx_books_frame("a", retry_at + 200, "snapshot", 100, None, "5");
    let recovered = f.engine.receive_batch(
        vec![recovery], ns(retry_at + 200), wall(retry_at + 200)
    ).unwrap();
    f.handle(recovered);
    assert_eq!(f.engine.market_data_processor.diagnostic_state("a", "BTC").map(|v| v.0), Some(100));
    assert!(!f.engine.resyncing.contains(&("a".into(), "BTC".into())));
    assert!(!f.engine.resubscribe_last_ns.contains_key(&("a".into(), "BTC".into())));

    let second_gap = okx_books_frame("a", retry_at + 300, "update", 102, Some(101), "6");
    let out = f.engine.receive_batch(
        vec![second_gap], ns(retry_at + 300), wall(retry_at + 300)
    ).unwrap();
    assert!(out.iter().any(|o| matches!(o, Output::Resubscribe { .. })));
    f.handle(out);
    assert!(f.engine.resubscribe_last_ns.contains_key(&("a".into(), "BTC".into())));
    let closed = f.engine.disconnected("a", ns(retry_at + 301), wall(retry_at + 301)).unwrap();
    f.handle(closed);
    assert!(!f.engine.resyncing.iter().any(|(session, _)| session == "a"));
    assert!(!f.engine.resubscribe_last_ns.keys().any(|(session, _)| session == "a"),
        "session reset must clear cooldown state");
}

#[test]
fn r1274_saved_shadow_engine_replay_benchmark() {
    let Ok(capture_dir)=std::env::var("R1274_BENCH_CAPTURE") else { return };
    use std::{fs,io::{BufRead,BufReader},time::Instant};
    let bases=["BNB","BTC","DOGE","ETH","LINK","SOL","SUI","XRP"].iter().map(|s|s.to_string()).collect::<Vec<_>>();
    let mut events=Vec::<(u64,serde_json::Value)>::new();
    for entry in fs::read_dir(capture_dir).unwrap() {
        let path=entry.unwrap().path();
        if path.extension().and_then(|s|s.to_str())!=Some("jsonl"){continue}
        for line in BufReader::new(fs::File::open(path).unwrap()).lines() {
            let value:serde_json::Value=serde_json::from_str(&line.unwrap()).unwrap();
            let mono=value["mono_ns"].as_u64().or_else(||value["received_mono_ns"].as_u64()).unwrap_or(0);
            if mono>0 {events.push((mono,value));}
        }
    }
    events.sort_by_key(|(mono,_)|*mono);
    let mut runs=Vec::new();
    let rounds=std::env::var("R1274_BENCH_ROUNDS").ok().and_then(|v|v.parse::<usize>().ok()).unwrap_or(3).clamp(1,5);
    for _round in 0..rounds {
        let scopes=bases.iter().flat_map(|base|[key(Venue::BinanceFutures,base),key(Venue::OkxSpot,base)]).collect();
        let auth:Authority=Arc::new(RwLock::new(HashMap::new()));
        let mut engine=Engine::new_volatile(auth,scopes).unwrap();engine.clock=None;
        let mut samples=Vec::<u64>::new();let mut frames=0u64;let mut errors=0u64;let mut output_frames=0u64;let mut resets=0u64;
        for (_,value) in &events {
            let source=value["source_id"].as_str().unwrap_or("");
            let venue=match value["venue"].as_str().unwrap_or(""){"BinanceFutures"=>Venue::BinanceFutures,"OkxSpot"=>Venue::OkxSpot,_=>continue};
            let generation=value["socket_generation"].as_u64().unwrap_or(1);
            match value["kind"].as_str().unwrap_or("") {
                "socket_connected"=>{let _=engine.register_connection_scoped(source,venue,generation,&bases);engine.mark_subscribed(source);}
                "ws_text" if value["disposition"]=="market"=>{
                    let mono=value["received_mono_ns"].as_u64().unwrap();let wall=value["received_wall_ms"].as_i64().unwrap();
                    let mut frame=RawFrame{venue,epoch:generation,received_ns:mono,received_wall_ms:wall,raw:value["raw"].as_str().unwrap().to_string(),source:Some(provenance(source.to_string(),generation))};
                    frames+=1;if let Some(p)=frame.source.as_mut(){p.accepted_id=frames;p.accepted_ns=Some(mono);}
                    let t=Instant::now();
                    match engine.receive_batch(vec![frame],mono,wall){Ok(out)=>for item in out{match item{Output::Frame(_)|Output::Book(_,_)=>output_frames+=1,Output::Reset{..}=>resets+=1,_=>{}}},Err(_)=>errors+=1}
                    samples.push(t.elapsed().as_nanos().min(u64::MAX as u128) as u64);
                }
                _=>{}
            }
        }
        samples.sort_unstable();let percentile=|p:usize|samples[(samples.len()*p/100).min(samples.len()-1)] as f64/1000.0;
        runs.push(serde_json::json!({"frames":frames,"errors":errors,"output_frames":output_frames,"resets":resets,"service_us_total":samples.iter().map(|v|*v as u128).sum::<u128>() as f64/1000.0,"service_us_per_frame":samples.iter().sum::<u64>() as f64/samples.len() as f64/1000.0,"p50_us":percentile(50),"p95_us":percentile(95),"p99_us":percentile(99),"max_us":*samples.last().unwrap() as f64/1000.0,"metrics":engine.metrics,"profile":engine.r1273_profile.lock().unwrap().snapshot(),"coverage":engine.coverage(events.last().map(|(m,_)|*m).unwrap_or(0),events.last().and_then(|(_,v)|v["received_wall_ms"].as_i64().or_else(||v["wall_ms"].as_i64())).unwrap_or(0))}));
    }
    let output=serde_json::json!({"schema":"R1274_CAPTURED_WS_ENGINE_REPLAY_V1","repeats":runs,"frames_per_repeat":runs[0]["frames"]});
    let dest=std::env::var("R1274_BENCH_OUT").unwrap();
    fs::write(dest,serde_json::to_vec_pretty(&output).unwrap()).unwrap();
    eprintln!("R1274_REPLAY_RESULT frames={} rounds={} service_us_per_frame={:?} p95_us={:?} p99_us={:?} max_us={:?} errors={:?} hot_deficits={:?}",output["frames_per_repeat"],runs.len(),runs.iter().map(|r|r["service_us_per_frame"].as_f64().unwrap()).collect::<Vec<_>>(),runs.iter().map(|r|r["p95_us"].as_f64().unwrap()).collect::<Vec<_>>(),runs.iter().map(|r|r["p99_us"].as_f64().unwrap()).collect::<Vec<_>>(),runs.iter().map(|r|r["max_us"].as_f64().unwrap()).collect::<Vec<_>>(),runs.iter().map(|r|r["errors"].as_u64().unwrap()).collect::<Vec<_>>(),runs.iter().map(|r|r["metrics"]["hot_deficits"].as_u64().unwrap()).collect::<Vec<_>>());
}

#[cfg(test)]
mod r1277_quarantine_replay_tests {
    use super::*;
    use crate::native_r1173_compact_writer::ingress_store_r1205::{CommitDelayProbe, StoreWorker};
    use std::{sync::atomic::Ordering, time::Instant};

    fn private_root(tag: &str) -> PathBuf {
        let base = std::env::var_os("R1277_TEST_ROOT").map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir).join("spotarb_r1277_replay");
        std::fs::create_dir_all(&base).unwrap();
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let root = base.join(format!("{tag}_{}_{}", std::process::id(), nonce));
        std::fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
        root
    }

    fn percentile(samples: &[u64], pct: usize) -> u64 {
        let mut sorted = samples.to_vec();
        sorted.sort_unstable();
        sorted[sorted.len().saturating_sub(1) * pct / 100]
    }

    fn load_capture_frames(dir: &std::path::Path, limit: usize) -> Vec<RawFrame> {
        let mut rows = Vec::<(u64, RawFrame)>::new();
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().and_then(|x| x.to_str()) != Some("jsonl") { continue; }
            let content = std::fs::read_to_string(path).unwrap();
            for line in content.lines() {
                let v: serde_json::Value = serde_json::from_str(line).unwrap();
                if v["kind"] != "ws_text" || v["disposition"] != "market" { continue; }
                let Some(raw) = v["raw"].as_str() else { continue };
                let venue = match v["venue"].as_str().unwrap_or("") {
                    "OkxSpot" => Venue::OkxSpot,
                    "BinanceFutures" => Venue::BinanceFutures,
                    _ => continue,
                };
                let session = v["source_id"].as_str().unwrap_or("r1277-replay").to_owned();
                let mono = v["received_mono_ns"].as_u64().unwrap_or(0);
                let frame = RawFrame {
                    venue,
                    epoch: v["socket_generation"].as_u64().unwrap_or(1),
                    received_ns: mono,
                    received_wall_ms: v["received_wall_ms"].as_i64().unwrap_or(0),
                    raw: raw.to_owned(),
                    source: Some(crate::market_data_ingress_r1205::provenance(session, 1)),
                };
                rows.push((mono, frame));
            }
        }
        rows.sort_by_key(|row| row.0);
        assert!(rows.len() >= limit, "capture too short for requested R1277 replay");
        rows.into_iter().take(limit).map(|row| row.1).collect()
    }

    async fn quarantine_run(input: &[RawFrame], batched: bool) -> serde_json::Value {
        let root = private_root(if batched { "after" } else { "baseline" });
        let mut inbox = crate::native_r1173_compact_writer::ingress_store_r1205::Store::open(&root).unwrap();
        let mut accepted = input.to_vec();
        for chunk in accepted.chunks_mut(32) { inbox.accept_batch(chunk).unwrap(); }
        let pending = inbox.pending().unwrap();
        assert_eq!(pending.len(), input.len());
        let shared = std::sync::Arc::new(std::sync::Mutex::new(inbox));
        let worker = StoreWorker::start(shared.clone()).unwrap();
        let started = Instant::now();
        let mut op_ns = Vec::new();
        if batched {
            for chunk in pending.chunks(16) {
                let items = chunk.iter().cloned().map(|f| (f, "standby_superseded".to_owned())).collect::<Vec<_>>();
                let elapsed = worker.call_async(move |store, _| {
                    let at = Instant::now();
                    store.quarantine_batch(&items)?;
                    Ok(at.elapsed().as_nanos() as u64)
                }).await.unwrap();
                op_ns.push(elapsed);
            }
        } else {
            for frame in pending {
                let elapsed = worker.call_async(move |store, _| {
                    let at = Instant::now();
                    store.finish(&frame, "quarantined", "standby_superseded", None)?;
                    Ok(at.elapsed().as_nanos() as u64)
                }).await.unwrap();
                op_ns.push(elapsed);
            }
        }
        let elapsed_ns = started.elapsed().as_nanos() as u64;
        let counts = shared.lock().unwrap().counts().unwrap();
        let profile = worker.profile();
        assert_eq!((counts.accepted, counts.quarantined, counts.pending), (input.len() as u64, input.len() as u64, 0));
        let result = serde_json::json!({
            "mode": if batched { "batch16" } else { "single_frame" },
            "frames": input.len(),
            "storage_operations": op_ns.len(),
            "elapsed_ns": elapsed_ns,
            "frames_per_second": input.len() as f64 / (elapsed_ns as f64 / 1e9),
            "operation_p50_ns": percentile(&op_ns, 50),
            "operation_p95_ns": percentile(&op_ns, 95),
            "operation_p99_ns": percentile(&op_ns, 99),
            "operation_max_ns": op_ns.iter().copied().max().unwrap_or(0),
            "worker_profile": profile,
            "counts": counts,
            "durability": "SQLite WAL synchronous=FULL",
        });
        drop(worker);
        drop(shared);
        std::fs::remove_dir_all(root).unwrap();
        result
    }

    #[tokio::test]
    async fn r1277_same_capture_quarantine_single_vs_batch16() {
        let Ok(dir) = std::env::var("R1277_CAPTURE_DIR") else { return };
        let input = load_capture_frames(std::path::Path::new(&dir), 872);
        let baseline = quarantine_run(&input, false).await;
        let after = quarantine_run(&input, true).await;
        assert_eq!(baseline["frames"], after["frames"]);
        assert_eq!(baseline["counts"]["quarantined"], after["counts"]["quarantined"]);
        assert_eq!(baseline["counts"]["pending"], after["counts"]["pending"]);
        assert_eq!(baseline["storage_operations"], 872);
        assert_eq!(after["storage_operations"], 55);
        println!("R1277_QUARANTINE_REPLAY {}", serde_json::json!({
            "capture_frames_identical": true,
            "baseline": baseline,
            "after": after,
        }));
    }

    #[tokio::test]
    async fn quarantine_batch_waits_for_full_commit_before_return() {
        let root = private_root("fault");
        let mut inbox = crate::native_r1173_compact_writer::ingress_store_r1205::Store::open(&root).unwrap();
        let mut frames = (0..16).map(|i| frame("r1277-fault", i, i as i64 + 1)).collect::<Vec<_>>();
        for (i,f) in frames.iter_mut().enumerate(){let source=f.source.as_mut().unwrap();source.frame_trace_id=format!("r1279-fault:{i}");source.r1279_timing=Some(crate::R1279FrameTiming{clock_domain_id:Some("test-clock".into()),ws_receive_ns:Some(10+i as u64),ws_decode_end_ns:Some(20+i as u64),..Default::default()});}
        inbox.accept_batch(&mut frames).unwrap();
        let pending = inbox.pending().unwrap();
        let probe = std::sync::Arc::new(CommitDelayProbe::default());
        probe.delay_ms.store(250, Ordering::Release);
        inbox.set_commit_delay_probe(probe.clone());
        let shared = std::sync::Arc::new(std::sync::Mutex::new(inbox));
        let worker = StoreWorker::start(shared.clone()).unwrap();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let writer = worker.clone();
        let join = std::thread::spawn(move || {
            let result = writer.call(move |store, _| store.quarantine_batch(
                &pending.into_iter().map(|f| (f, "fault_commit".to_owned())).collect::<Vec<_>>()
            ));
            done_tx.send(result).unwrap();
        });
        let deadline = Instant::now() + std::time::Duration::from_secs(2);
        while !probe.started.load(Ordering::Acquire) && Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert!(probe.started.load(Ordering::Acquire), "FULL transaction commit hook did not run");
        assert!(matches!(done_rx.try_recv(), Err(std::sync::mpsc::TryRecvError::Empty)),
            "quarantine acknowledged before delayed FULL commit completed");
        done_rx.recv_timeout(std::time::Duration::from_secs(3)).unwrap().unwrap();
        join.join().unwrap();
        let counts = shared.lock().unwrap().counts().unwrap();
        assert_eq!((counts.accepted, counts.quarantined, counts.pending), (16, 16, 0));
        let terminal=shared.lock().unwrap().frame_trace_terminal(16).unwrap();
        assert_eq!(terminal.len(),16);
        assert!(terminal.iter().all(|r|r["terminal_state"]=="quarantined"&&r["ack_status"]=="not_applicable_quarantine"&&r["trace"]["clock_domain_id"]=="test-clock"));
        assert_eq!(terminal.iter().map(|r|r["frame_trace_id"].as_str().unwrap()).collect::<std::collections::HashSet<_>>().len(),16);
        assert_eq!(worker.profile()["submitted"], 1, "one bounded worker transaction per quarantine batch");
        drop(worker);
        drop(shared);
        std::fs::remove_dir_all(root).unwrap();
    }
}


#[test]
fn r1281_store_worker_profiles_queue_and_store_lock_wait_separately(){
 use std::sync::mpsc;
 let f=Fixture::new();let worker=crate::native_r1173_compact_writer::ingress_store_r1205::StoreWorker::start(f.store.clone()).unwrap();
 // Occupy the sole worker operation, then queue a measured epoch transaction.
 let (entered_tx,entered_rx)=mpsc::channel();let (release_tx,release_rx)=mpsc::channel();
 let blocking=worker.clone();
 let first=std::thread::spawn(move||blocking.call(move|_,_|{entered_tx.send(()).unwrap();release_rx.recv().unwrap();Ok(())}));
 entered_rx.recv().unwrap();
 let measured=worker.clone();let second=std::thread::spawn(move||measured.call_profiled(|store|store.next_epoch()).unwrap());
 std::thread::sleep(std::time::Duration::from_millis(35));release_tx.send(()).unwrap();
 first.join().unwrap().unwrap();let(epoch,timing)=second.join().unwrap();
 assert_eq!(epoch,1);assert!(timing.queue_wait_ns>=20_000_000,"{timing:?}");
 assert!(timing.store_lock_wait_ns<timing.queue_wait_ns,"{timing:?}");
 assert!(timing.total_ns>=timing.queue_wait_ns+timing.store_lock_wait_ns+timing.operation_ns,"{timing:?}");
 println!("R1281_STORE_WORKER_QUEUE_FAULT {}",serde_json::json!({"timing":timing}));
 // Hold the shared Store lock to isolate mutex waiting from worker queue waiting.
 let held=f.store.lock().unwrap();let measured=worker.clone();
 let lock_call=std::thread::spawn(move||measured.call_profiled(|store|store.next_epoch()).unwrap());
 std::thread::sleep(std::time::Duration::from_millis(35));drop(held);
 let(epoch,timing)=lock_call.join().unwrap();assert_eq!(epoch,2);
 assert!(timing.store_lock_wait_ns>=20_000_000,"{timing:?}");
 assert!(timing.queue_wait_ns<timing.store_lock_wait_ns,"{timing:?}");
 println!("R1281_STORE_WORKER_LOCK_FAULT {}",serde_json::json!({"timing":timing}));
}

#[test]
fn r1281_offline_r1280_capture_replay_splits_engine_ucm_and_store(){
 let Ok(root)=std::env::var("R1281_CAPTURE_ROOT") else{return};
 use std::{collections::HashMap,io::{BufRead,BufReader},fs::File};
 #[derive(Clone)]struct Conn{venue:Venue,generation:u64,bases:Vec<String>}
 struct Event{mono:u64,wall:i64,venue:Venue,session:String,generation:u64,raw:String,seq:u64}
 let ws=std::path::Path::new(&root).join("ws");let mut conns=HashMap::<String,Conn>::new();let mut events=Vec::<Event>::new();let mut sequence=0u64;
 for entry in std::fs::read_dir(&ws).unwrap(){let path=entry.unwrap().path();if path.extension().and_then(|x|x.to_str())!=Some("jsonl"){continue}
  for line in BufReader::new(File::open(&path).unwrap()).lines(){let line=line.unwrap();let v:serde_json::Value=serde_json::from_str(&line).unwrap();match v["kind"].as_str().unwrap_or(""){
   "socket_connected"=>{let session=v["source_id"].as_str().unwrap().to_string();let venue=match v["venue"].as_str().unwrap(){"OkxSpot"=>Venue::OkxSpot,"BinanceFutures"=>Venue::BinanceFutures,_=>panic!("venue")};let generation=v["socket_generation"].as_str().and_then(|x|x.parse().ok()).or_else(||v["socket_generation"].as_u64()).unwrap();let bases=v["bases"].as_array().unwrap().iter().map(|x|x.as_str().unwrap().to_string()).collect();conns.insert(session,Conn{venue,generation,bases});}
   "ws_text" if v["disposition"]=="market"=>{let session=v["source_id"].as_str().unwrap().to_string();let Some(conn)=conns.get(&session) else{continue};let mono=v["received_mono_ns"].as_u64().unwrap();let wall=v["received_wall_ms"].as_i64().unwrap();sequence+=1;events.push(Event{mono,wall,venue:conn.venue,session,generation:conn.generation,raw:v["raw"].as_str().unwrap().to_string(),seq:sequence});}
   _=>{}
  }}
 }
 let universe=["BTC","ETH","SOL","XRP","DOGE","BNB","LINK","SUI"].iter().flat_map(|base|[key(Venue::OkxSpot,base),key(Venue::BinanceFutures,base)]).collect::<Vec<_>>();
 let nonce=SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();let inbox_root=std::env::temp_dir().join(format!("r1281_replay_inbox_{}_{}",std::process::id(),nonce));let db=std::env::temp_dir().join(format!("r1281_replay_store_{}_{}",std::process::id(),nonce));
 std::fs::DirBuilder::new().mode(0o700).create(&inbox_root).unwrap();std::fs::DirBuilder::new().mode(0o700).create(&db).unwrap();
 let mut inbox=Store::open(&inbox_root).unwrap();inbox.bind_canonical(&db).unwrap();let shared=Arc::new(Mutex::new(inbox));let worker=crate::native_r1173_compact_writer::ingress_store_r1205::StoreWorker::start(shared.clone()).unwrap();
 let auth:Authority=Arc::new(RwLock::new(HashMap::new()));let mut engine=Engine::new_with_store_worker(shared.clone(),worker.clone(),auth,universe).unwrap();engine.clock=None;engine.r1273_profile_enabled=true;
 for (session,c) in &conns{engine.register_connection_scoped(session,c.venue,c.generation,&c.bases).unwrap();if c.venue==Venue::OkxSpot{engine.mark_subscribed(session);}}
 events.sort_by_key(|e|(e.mono,e.seq));let captured_count=events.len();events=events.into_iter().step_by(7).collect();let input_count=events.len();assert!(input_count<=2048,"offline harness has no canonical terminalizer; keep sample within bounded pending limit");let started=std::time::Instant::now();let mono_origin=events.first().map(|e|e.mono).unwrap_or(0);let mut batches=0u64;let mut max_batch=0usize;
 for chunk in events.chunks(32){let target=std::time::Duration::from_nanos(chunk[0].mono.saturating_sub(mono_origin));let elapsed=started.elapsed();if target>elapsed{std::thread::sleep(target-elapsed);}let mut batch=Vec::with_capacity(chunk.len());for e in chunk{let mut source=provenance(e.session.clone(),e.generation);source.frame_trace_id=format!("r1281-replay:{}:{}",e.session,e.seq);source.r1279_timing=Some(crate::R1279FrameTiming{clock_domain_id:Some("r1280-captured-mono".into()),ws_receive_ns:Some(e.mono),ws_decode_end_ns:Some(e.mono),..Default::default()});batch.push(RawFrame{venue:e.venue,epoch:e.generation,received_ns:e.mono,received_wall_ms:e.wall,source:Some(source),raw:e.raw.clone()});}
  let now=chunk.last().unwrap();engine.receive_batch(batch,now.mono,now.wall).unwrap();batches+=1;max_batch=max_batch.max(chunk.len());
 }
 let wall_ns=started.elapsed().as_nanos()as u64;let profile=engine.r1273_profile.lock().unwrap().snapshot();let worker_profile=worker.profile();let counts=shared.lock().unwrap().counts().unwrap();
 println!("R1281_CAPTURE_REPLAY {}",serde_json::json!({"captured_market_frames":captured_count,"sampled_market_frames":input_count,"sampling_stride":7,"batches":batches,"batch_max":max_batch,"wall_elapsed_ns":wall_ns,"engine_received":engine.metrics.received,"engine_profile":profile,"store_worker_profile":worker_profile,"custody_counts":counts,"selected_outputs":engine.metrics.submitted,"fail_closed_hot_deficits":engine.metrics.hot_deficits,"orders":0}));
 drop(engine);drop(worker);drop(shared);let _=std::fs::remove_dir_all(&inbox_root);let _=std::fs::remove_dir_all(&db);
}

#[test]
fn r1284_receive_and_tick_select_spans_have_stable_identity_and_parent(){
 let auth:Authority=Arc::new(RwLock::new(HashMap::new()));
 let mut engine=Engine::new_volatile(auth,vec![key(Venue::BinanceFutures,"BTC")]).unwrap();
 engine.receive_batch(Vec::new(),ns(1),wall(1)).unwrap();
 let receive=engine.audit().into_iter().find(|row|row["call"]=="receive_batch").unwrap();
 assert_eq!(receive["status"],"complete");
 assert_eq!(receive["call_id"],receive["ingress_batch_id"]);
 let _=engine.tick(ns(2),wall(2)).unwrap();
 let rows=engine.audit();
 let tick=rows.iter().rev().find(|row|row["call"]=="tick").unwrap();
 assert_eq!(tick["status"],"complete");
 let select=rows.iter().find(|row|row["call"]=="select"&&row["parent_call_id"]==tick["call_id"]).unwrap();
 assert_eq!(select["status"],"complete");
 assert_eq!(select["parent_call_kind"],"tick");
 assert_ne!(receive["call_id"],tick["call_id"]);
 assert_ne!(tick["call_id"],select["call_id"]);
 assert_eq!(select["selection_batch_id"],select["call_id"]);
 assert!(select["end_ns"].as_u64().unwrap()>=select["start_ns"].as_u64().unwrap());
}

#[test]
fn r1284_retained_frame_keeps_ingress_id_and_separates_residence_from_service(){
 let mut f=Fixture::new();
 f.feed("a",0,1);
 let ((session,scope),book)=f.engine.books.iter_mut().find(|(_,book)|book.retained).expect("first frame remains retained");
 let ingress_id=987_654_321u64;
 let residence_start=crate::ingress_runtime_r1205::mono_ns();
 let timing=book.frame.source.as_mut().unwrap().r1279_timing.get_or_insert_with(crate::R1279FrameTiming::default);
 timing.receive_batch_id=Some(ingress_id);
 timing.frame_residence_start_ns=Some(residence_start);
 timing.frame_residence_status=Some("incomplete".into());
 let book_key=(session.clone(),scope.clone());
 let scope=scope.clone();
 std::thread::sleep(std::time::Duration::from_millis(60));
 let _=f.engine.select(&scope,ns(1),wall(1),44,"test").unwrap();
 let call=f.engine.audit().into_iter().rev().find(|row|row["call"]=="select"&&row["parent_call_id"]==44).unwrap();
 assert!(call["ingress_batch_ids"].as_array().unwrap().contains(&serde_json::json!(ingress_id)));
 let current=f.engine.books.get(&book_key).unwrap();
 let timing=current.frame.source.as_ref().unwrap().r1279_timing.as_ref().unwrap();
 assert_eq!(timing.receive_batch_id,Some(ingress_id));
 assert_eq!(timing.selection_batch_id,Some(call["selection_batch_id"].as_u64().unwrap()));
 let service=call["end_ns"].as_u64().unwrap()-call["start_ns"].as_u64().unwrap();
 let residence_end=timing.frame_residence_end_ns.unwrap_or_else(crate::ingress_runtime_r1205::mono_ns);
 let residence=residence_end-residence_start;
 assert!(residence>=service+20_000_000,"residence={residence} service={service}");
}

#[test]
fn r1284_standby_superseded_does_not_close_service_with_terminalization_time(){
 let mut f=Fixture::new();
 let mut old=frame("a",0,1);
 old.source.as_mut().unwrap().r1279_timing=Some(crate::R1279FrameTiming{
  receive_batch_id:Some(7),selection_batch_id:Some(9),
  source_selection_start_ns:Some(100),source_selection_end_ns:Some(150),
  frame_residence_start_ns:Some(50),..Default::default()
 });
 f.engine.finish(&old,"standby_superseded").unwrap();
 let (queued,reason)=f.engine.quarantines.lock().unwrap().pop().unwrap();
 let timing=queued.source.unwrap().r1279_timing.unwrap();
 assert_eq!(reason,"standby_superseded");
 assert_eq!(timing.source_selection_start_ns,Some(100));
 assert_eq!(timing.source_selection_end_ns,Some(150));
 assert!(timing.frame_residence_end_ns.unwrap()>=150);
 assert_eq!(timing.frame_residence_status.as_deref(),Some("terminalized"));
}

#[test]
fn r1285_repeated_select_does_not_extend_released_frame_residence(){
 let mut f=Fixture::new();
 f.feed("a",0,1);
 let ((session,scope),book)=f.engine.books.iter_mut().find(|(_,book)|book.retained).expect("accepted frame remains retained");
 let timing=book.frame.source.as_mut().unwrap().r1279_timing.get_or_insert_with(crate::R1279FrameTiming::default);
 timing.frame_residence_start_ns=Some(crate::ingress_runtime_r1205::mono_ns());
 timing.frame_residence_status=Some("incomplete".into());
 book.retained=false;
 let book_key=(session.clone(),scope.clone());
 let scope=scope.clone();
 let _=f.engine.select(&scope,ns(1),wall(1),1285,"test").unwrap();
 let first_end=f.engine.books.get(&book_key).unwrap().frame.source.as_ref().unwrap().r1279_timing.as_ref().unwrap().frame_residence_end_ns.expect("first selection closes residence");
 std::thread::sleep(std::time::Duration::from_millis(2));
 let _=f.engine.select(&scope,ns(2),wall(2),1285,"test").unwrap();
 let timing=f.engine.books.get(&book_key).unwrap().frame.source.as_ref().unwrap().r1279_timing.as_ref().unwrap();
 assert_eq!(timing.frame_residence_end_ns,Some(first_end),"later selects must not extend a completed residence interval");
 assert_eq!(timing.frame_residence_status.as_deref(),Some("released"));
 let calls=f.engine.audit().into_iter().filter(|row|row["call"]=="select"&&row["parent_call_id"]==1285).collect::<Vec<_>>();
 assert_eq!(calls.len(),2);
 assert_ne!(calls[0]["call_id"],calls[1]["call_id"]);
}

// R1318: deterministic reproduction of the live terminal-session race.
// A frame already in the reader queue can arrive after disconnected() retires
// its source. It must be terminalized, not registered again or published.
#[test]
fn r1318_queued_frame_after_session_retirement_is_fail_closed() {
 let mut f=Fixture::new();
 f.warm();
 let session="a";
 let queued=frame(session,900,902);
 let out=f.engine.disconnected(session,ns(900),wall(900)).unwrap();
 f.handle(out);
 assert!(!f.engine.sources.contains_key(session));
 let before=f.published;
 let invalid_before=f.engine.metrics.invalid_categories.get("unknown_source").copied().unwrap_or(0);
 let out=f.engine.receive_batch(vec![queued],ns(901),wall(901)).unwrap();
 f.handle(out);
 assert_eq!(f.published,before,"retired session must never publish queued input");
 assert_eq!(f.engine.metrics.invalid_categories.get("unknown_source").copied().unwrap_or(0),invalid_before+1);
 assert!(!f.engine.sources.contains_key(session),"queued input must not resurrect session");
 f.invariant("r1318_queued_after_retirement");
}

#[test]fn r1325_trace_retention_preserves_actual_pending_ack_until_confirmed(){
 let(mut f,frames)=c_published_batch(1,true);let w=f.writer.as_mut().unwrap();let mut inbox=f.store.lock().unwrap();
 let verified=w.verify_ingress_batch(&frames).unwrap();let committed=inbox.finish_published_batch_verified(&frames,&verified).unwrap();let id=frames[0].source.as_ref().unwrap().accepted_id;
 for batch in 0..20{let mut raw=(0..32).map(|i|frame("retention",10000+batch*32+i,10001+(batch*32+i)as i64)).collect::<Vec<_>>();inbox.accept_batch(&mut raw).unwrap();inbox.quarantine_batch(&raw.into_iter().map(|r|(r,"retention_fixture".into())).collect::<Vec<_>>()).unwrap();}
 assert!(inbox.reclaim_refs().unwrap().iter().any(|(_,n)|*n==id));let traces=inbox.frame_trace_terminal(4096).unwrap();assert_eq!(traces.len(),513);assert_eq!(traces.iter().find(|r|r["accepted_id"]==id).unwrap()["ack_status"],"awaiting_reclaim_ack");
 let ticket=w.prepare_ingress_release_after_publish(&verified,&committed).unwrap().unwrap();w.commit_ingress_release(ticket).unwrap();let acked=w.confirm_ingress_reclaimed(&committed).unwrap();inbox.forget_reclaimed_after_ack(&acked).unwrap();
 assert!(inbox.reclaim_refs().unwrap().is_empty());assert_eq!(inbox.frame_trace_terminal(4096).unwrap().len(),512);assert_eq!(inbox.frame_trace_retention_r1325().unwrap()["terminal_outcomes"],641);w.verify_pair().unwrap();w.require_decisions_drained().unwrap();
}

#[test]
fn r1326_paper06_328_legacy_pending_recover_under_original_receipt_contract(){
 let nonce=SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
 let root=std::env::temp_dir().join(format!("r1326_paper06_328_inbox_{}_{}",std::process::id(),nonce));
 let db=std::env::temp_dir().join(format!("spotarb_r1173_r1326_paper06_{}_{}",std::process::id(),nonce));
 std::fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
 std::fs::DirBuilder::new().mode(0o700).create(&db).unwrap();
 let mut frames=(0..328u64).map(|n|frame("paper06-legacy",n,n as i64+1)).collect::<Vec<_>>();
 {
  let mut store=Store::open(&root).unwrap();store.bind_canonical(&db).unwrap();
  let writer=Writer::create(&db).unwrap();
  for batch in frames.chunks_mut(32){store.accept_batch(batch).unwrap();}
  let counts=store.counts().unwrap();assert_eq!(counts.accepted,328);assert_eq!(counts.pending,328);
  assert_eq!(writer.trade_counts_r1326().unwrap(),(0,0));
  drop(writer);
 }
 let mut store=Store::open(&root).unwrap();
 assert_eq!(store.pending().unwrap().len(),328);
 let mut writer=Writer::resume(&db).unwrap();writer.recover_ingress_source_only().unwrap();
 let shared=Arc::new(Mutex::new(store));
 assert_eq!(reconcile(&shared,&writer).unwrap(),328);
 let counts=shared.lock().unwrap().counts().unwrap();
 assert_eq!((counts.accepted,counts.published,counts.quarantined,counts.pending),(328,0,328,0));
 assert_eq!(writer.trade_counts_r1326().unwrap(),(0,0));
 writer.verify_pair().unwrap();writer.require_decisions_drained().unwrap();
 drop(writer);drop(shared);
 std::fs::remove_dir_all(root).unwrap();std::fs::remove_dir_all(db).unwrap();
}

#[test]
fn r1326_100k_rejected_ram_frames_create_no_trade_history(){
 let nonce=SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
 let db=std::env::temp_dir().join(format!("spotarb_r1173_r1326_100k_{}_{}",std::process::id(),nonce));
 std::fs::DirBuilder::new().mode(0o700).create(&db).unwrap();
 let mut writer=Writer::create(&db).unwrap();let auth:Authority=Arc::new(RwLock::new(HashMap::new()));
 let mut engine=Engine::new_volatile(auth,vec![key(Venue::BinanceFutures,"BTC")]).unwrap();engine.clock=None;engine.register("reject-test").unwrap();
 for start in (0..100_000u64).step_by(32){
  let frames=(start..(start+32).min(100_000)).map(|i|RawFrame{venue:Venue::BinanceFutures,epoch:1,
   received_ns:ns(i),received_wall_ms:wall(i),raw:"{invalid".into(),source:Some(provenance("reject-test".into(),1))}).collect();
  assert!(engine.receive_batch(frames,ns(start),wall(start)).unwrap().is_empty());
 }
 assert_eq!(engine.metrics.received,100_000);assert_eq!(writer.trade_counts_r1326().unwrap(),(0,0));
 writer.verify_pair().unwrap();drop(writer);std::fs::remove_dir_all(db).unwrap();
}

// R1350 diagnostic: registered scopes but NO populated book frames. Not a full-load benchmark.
#[test]
fn r1350_registered_scope_phase_scaling_offline() {
    use std::time::Instant;
    for count in [8usize,40,235] {
        let bases=(0..count).map(|i|format!("R1350S{i:03}")).collect::<Vec<_>>();
        let keys=bases.iter().map(|b|key(Venue::BinanceFutures,b)).collect();
        let auth:Authority=Arc::new(RwLock::new(HashMap::new()));
        let mut engine=Engine::new_volatile(auth,keys).unwrap();
        engine.register_connection_scoped("r1350-offline",Venue::BinanceFutures,1,&bases).unwrap();
        let now=crate::ingress_runtime_r1205::mono_ns();
        let wall=wall(0);
        for _ in 0..3 { let _=engine.coverage(now,wall); let _=engine.tick(now,wall).unwrap(); }
        let repeats=20;
        let t=Instant::now();
        for _ in 0..repeats {std::hint::black_box(engine.coverage(now,wall));}
        let coverage_us=t.elapsed().as_micros() as f64/repeats as f64;
        let t=Instant::now();
        for _ in 0..repeats {std::hint::black_box(engine.tick(now,wall).unwrap());}
        let tick_us=t.elapsed().as_micros() as f64/repeats as f64;
        println!("R1350_SCOPE_PROFILE scopes={count} books={} coverage_mean_us={coverage_us:.1} tick_mean_us={tick_us:.1} mode=volatile_empty_books",engine.books.len());
        assert_eq!(engine.books.len(),0,"baseline must not be mistaken for populated books");
    }
}

// R1350: populated-book scaling in volatile offline Engine; synthetic Binance snapshots.
#[test]
fn r1350_populated_book_phase_scaling_offline() {
    use std::time::Instant;
    for count in [8usize,40,235] {
        let bases=(0..count).map(|i|format!("R{i:03}")).collect::<Vec<_>>();
        let keys=bases.iter().map(|b|key(Venue::BinanceFutures,b)).collect();
        let auth:Authority=Arc::new(RwLock::new(HashMap::new()));
        let mut engine=Engine::new_volatile(auth,keys).unwrap();
        engine.set_test_clock(||(ns(0),wall(0)));
        let (diag,diag_handle)=crate::r1352_diagnostic_worker::spawn(512);
        engine.attach_diagnostic_r1352(diag.clone());
        let (shards,shard_handles)=crate::r1352_diagnostic_worker::spawn_four(512);
        let shards=std::sync::Arc::new(shards);
        engine.attach_sharded_diagnostics_r1353(shards.clone());
        engine.register_connection_scoped("r1350-offline",Venue::BinanceFutures,1,&bases).unwrap();
        let now=ns(0);let clock=wall(0);
        for base in &bases {
            let raw=serde_json::json!({"stream":format!("{}usdt@depth20@500ms",base.to_lowercase()),"data":{"s":format!("{}USDT",base),"e":"depthUpdate","E":clock-50,"u":1,"U":1,"pu":0,"b":[["100","3"],["99","2"]],"a":[["101","3"],["102","2"]]}}).to_string();
            let frame=RawFrame{venue:Venue::BinanceFutures,epoch:1,received_ns:now,received_wall_ms:clock,source:Some(provenance("r1350-offline".into(),1)),raw};
            let _=engine.receive_batch(vec![frame],now,clock).unwrap();
        }
        let populated=engine.books.len();
        println!("R1350_POPULATE_DIAG scopes={count} books={populated} invalid={} received={}",engine.metrics.invalid,engine.metrics.received);println!("R1350_REJECT_REASONS {:?}",engine.metrics.invalid_categories);
        assert_eq!(populated,count,"synthetic frames must populate all books");
        assert_eq!(engine.diagnostic_dropped_r1352(),0);
        assert!(diag.try_publish(crate::r1352_diagnostic_worker::Message::Stop));
        let diag_state=diag_handle.join().unwrap();
        assert_eq!(diag_state.scopes.len(),count,"diagnostic per-scope updates");
        assert_eq!(shards.dropped(),0);
        for _ in 0..3 {std::hint::black_box(engine.coverage(now,clock));std::hint::black_box(engine.tick(now,clock).unwrap());}
        let repeats=20;
        let t=Instant::now();for _ in 0..repeats {std::hint::black_box(engine.coverage(now,clock));}
        let coverage_us=t.elapsed().as_micros() as f64/repeats as f64;
        let t=Instant::now();for _ in 0..repeats {std::hint::black_box(engine.tick(now,clock).unwrap());}
        let tick_us=t.elapsed().as_micros() as f64/repeats as f64;
        println!("R1350_POPULATED_PROFILE scopes={count} books={populated} coverage_mean_us={coverage_us:.1} tick_mean_us={tick_us:.1} mode=volatile_synthetic_snapshots");
        drop(engine);drop(shards);
        let _shard_states=shard_handles.into_iter().map(|h|h.join().unwrap()).collect::<Vec<_>>();
    }
}

// R1354: actual dual Engine offline comparison. Scheduler remains outside production tick.
#[test]
fn r1354_actual_dual_engine_deadline_shadow_offline(){
 use crate::r1354_deadline_scheduler::Scheduler;
 let count=235usize;
 let bases=(0..count).map(|i|format!("R{i:03}")).collect::<Vec<_>>();
 let keys=bases.iter().map(|b|key(Venue::BinanceFutures,b)).collect::<Vec<_>>();
 let mut legacy=Engine::new_volatile(Arc::new(RwLock::new(HashMap::new())),keys.clone()).unwrap();
 let mut candidate=Engine::new_volatile(Arc::new(RwLock::new(HashMap::new())),keys.clone()).unwrap();
 legacy.set_test_clock(||(ns(0),wall(0)));candidate.set_test_clock(||(ns(0),wall(0)));
 legacy.register_connection_scoped("r1354-shadow",Venue::BinanceFutures,1,&bases).unwrap();
 candidate.register_connection_scoped("r1354-shadow",Venue::BinanceFutures,1,&bases).unwrap();
 let mut scheduler=Scheduler::default();
 for base in &bases{
  let raw=serde_json::json!({"stream":format!("{}usdt@depth20@500ms",base.to_lowercase()),"data":{"s":format!("{}USDT",base),"e":"depthUpdate","E":wall(0)-50,"u":1,"U":1,"pu":0,"b":[["100","3"],["99","2"]],"a":[["101","3"],["102","2"]]}}).to_string();
  let frame=RawFrame{venue:Venue::BinanceFutures,epoch:1,received_ns:ns(0),received_wall_ms:wall(0),source:Some(provenance("r1354-shadow".into(),1)),raw};
  let left=legacy.receive_batch(vec![frame.clone()],ns(0),wall(0)).unwrap();
  let right=candidate.receive_batch(vec![frame],ns(0),wall(0)).unwrap();
  assert_eq!(left.len(),right.len());
  scheduler.schedule(&key(Venue::BinanceFutures,base),ns(801)).unwrap();
 }
 assert_eq!(legacy.books.len(),235);assert_eq!(candidate.books.len(),235);
 let mut candidate_selections=0usize;let mut legacy_selections=0usize;
 for t in (50..=1100).step_by(50){
  let now=ns(t);let clock=wall(t);
  let a=legacy.tick(now,clock).unwrap();legacy_selections+=235;
  candidate.manager.tick(now,clock);
  let mut b=Vec::new();for scope in scheduler.take_due(now){candidate_selections+=1;b.extend(candidate.select(&scope,now,clock,next_engine_call_id(),"r1354_shadow").unwrap());}
  assert_eq!(a.len(),b.len(),"output count mismatch at t={t}");
  assert_eq!(legacy.selected.len(),candidate.selected.len(),"selected mismatch at t={t}");
  assert_eq!(legacy.books.len(),candidate.books.len(),"books mismatch at t={t}");
  assert_eq!(legacy.metrics.submitted,candidate.metrics.submitted,"submitted mismatch at t={t}");
  assert_eq!(legacy.manager_ids.len(),candidate.manager_ids.len(),"manager id count mismatch at t={t}");
 }
 println!("R1354_DUAL_ENGINE scopes=235 steps=22 legacy_select_calls={legacy_selections} candidate_select_calls={candidate_selections} outputs={} note=single_connection_synthetic_no_trade",legacy.metrics.submitted);
 assert_eq!(legacy_selections,235*22);assert_eq!(candidate_selections,235);
}


#[test]
fn r1360_engine_idle_freshness_fails_closed_and_resumes_same_owner() {
    let auth: Authority = Arc::new(RwLock::new(HashMap::new()));
    let mut engine = Engine::new_volatile(auth.clone(), vec![key(Venue::BinanceFutures, "BTC")]).unwrap();
    engine.clock = None;
    for session in ["a", "b"] {
        engine.register_connection_scoped(session, Venue::BinanceFutures, 1, &["BTC".to_owned()]).unwrap();
        engine.mark_subscribed(session);
    }

    // Warm two real Engine source books through the actual parser, validators and UCM.
    for t in [0, 400, 800] {
        for session in ["a", "b"] {
            let _ = engine.receive_batch(vec![frame(session, t, t as i64 + 1)], ns(t), wall(t)).unwrap();
        }
    }
    let scope = key(Venue::BinanceFutures, "BTC");
    let original_owner = engine.selected.get(&scope).expect("dual-HOT owner").session.clone();
    assert!(auth.read().unwrap().contains_key(&scope), "fresh dual-HOT book publishes authority");

    // Market silence expires decision eligibility but must not erase the retained book or owner.
    let _ = engine.tick(ns(1601), wall(1601)).unwrap();
    assert!(!auth.read().unwrap().contains_key(&scope), "stale books must not authorize decisions");
    assert_eq!(engine.selected.get(&scope).map(|s| s.session.as_str()), Some(original_owner.as_str()));
    assert!(engine.books.contains_key(&(original_owner.clone(), scope.clone())), "book remains RAM-resident");
    assert_eq!(engine.books[&(original_owner.clone(), scope.clone())].frame.epoch, 1, "quiet market is not a generation reset");

    // Resume both copies with valid, advancing sequence updates. Freshness/authority return after normal qualification.
    for t in [2000, 2150, 2300] {
        for session in ["a", "b"] {
            let _ = engine.receive_batch(vec![frame(session, t, t as i64 + 1)], ns(t), wall(t)).unwrap();
        }
    }
    assert!(auth.read().unwrap().contains_key(&scope), "fresh dual-HOT data restores decision authority");
    assert_eq!(engine.selected.get(&scope).map(|s| s.session.as_str()), Some(original_owner.as_str()), "owner remains sticky after idle recovery");
}


#[test]
fn r1360_engine_failover_to_two_fresh_copies_and_primary_recovery_is_sticky() {
    let auth: Authority = Arc::new(RwLock::new(HashMap::new()));
    let mut engine = Engine::new_volatile(auth.clone(), vec![key(Venue::BinanceFutures, "BTC")]).unwrap();
    engine.clock = None;
    for session in ["a", "b", "c"] {
        engine.register_connection_scoped(session, Venue::BinanceFutures, 1, &["BTC".to_owned()]).unwrap();
        engine.mark_subscribed(session);
    }
    for t in [0, 400, 800] {
        for session in ["a", "b", "c"] {
            let _ = engine.receive_batch(vec![frame(session, t, t as i64 + 1)], ns(t), wall(t)).unwrap();
        }
    }
    let scope = key(Venue::BinanceFutures, "BTC");
    assert_eq!(engine.selected.get(&scope).map(|s| s.session.as_str()), Some("a"));

    // Keep b/c fresh while a remains physically connected but silent past 800ms.
    for t in [1200, 1601] {
        for session in ["b", "c"] {
            let _ = engine.receive_batch(vec![frame(session, t, t as i64 + 1)], ns(t), wall(t)).unwrap();
        }
    }
    let failover_owner = engine.selected.get(&scope).expect("fresh backup pair selected").session.clone();
    assert!(failover_owner == "b" || failover_owner == "c");
    assert_ne!(failover_owner, "a");
    assert_eq!(auth.read().unwrap().get(&scope).map(|p| p.session.as_str()), Some(failover_owner.as_str()));

    // Primary returns after the backup owns the scope; hysteresis keeps the recovered backup.
    for t in [2000, 2150] {
        for session in ["a", "b", "c"] {
            let _ = engine.receive_batch(vec![frame(session, t, t as i64 + 1)], ns(t), wall(t)).unwrap();
        }
    }
    assert_eq!(engine.selected.get(&scope).map(|s| s.session.as_str()), Some(failover_owner.as_str()));
    assert_eq!(auth.read().unwrap().get(&scope).map(|p| p.session.as_str()), Some(failover_owner.as_str()));
}

#[test]
fn r1360_engine_idle_symbol_does_not_invalidate_active_sibling_symbol() {
    let auth: Authority = Arc::new(RwLock::new(HashMap::new()));
    let scopes = vec![key(Venue::BinanceFutures, "BTC"), key(Venue::BinanceFutures, "ETH")];
    let mut engine = Engine::new_volatile(auth.clone(), scopes).unwrap();
    engine.clock = None;
    for session in ["a", "b", "c"] {
        engine.register_connection_scoped(session, Venue::BinanceFutures, 1, &["BTC".to_owned(), "ETH".to_owned()]).unwrap();
        engine.mark_subscribed(session);
    }
    let eth_frame = |session: &str, t: u64, seq: i64| {
        let mut f = frame(session, t, seq);
        f.raw = f.raw.replace("BTCUSDT", "ETHUSDT").replace("btcusdt@", "ethusdt@");
        f
    };
    for t in [0, 400, 800] {
        for session in ["a", "b", "c"] {
            let _ = engine.receive_batch(vec![frame(session, t, t as i64 + 1), eth_frame(session, t, t as i64 + 1)], ns(t), wall(t)).unwrap();
        }
    }
    let btc = key(Venue::BinanceFutures, "BTC");
    let eth = key(Venue::BinanceFutures, "ETH");
    assert!(auth.read().unwrap().contains_key(&btc));
    assert!(auth.read().unwrap().contains_key(&eth));

    // The same three physical sessions remain busy on ETH while BTC is quiet.
    for t in [1601, 1750] {
        for session in ["a", "b", "c"] {
            let _ = engine.receive_batch(vec![eth_frame(session, t, t as i64 + 1)], ns(t), wall(t)).unwrap();
        }
    }
    let _ = engine.tick(ns(1750), wall(1750)).unwrap();
    assert!(!auth.read().unwrap().contains_key(&btc), "BTC stale data cannot authorize a decision");
    assert!(auth.read().unwrap().contains_key(&eth), "fresh ETH remains eligible on active shared connections");
    assert_eq!(engine.selected.get(&btc).map(|s| s.session.as_str()), Some("a"), "BTC owner state is retained through symbol-local silence");
    assert_eq!(engine.selected.get(&eth).map(|s| s.session.as_str()), Some("a"));
}


#[test]
fn r1360_engine_freshness_boundary_is_inclusive_and_not_transport_health() {
    let auth: Authority = Arc::new(RwLock::new(HashMap::new()));
    let mut engine = Engine::new_volatile(auth.clone(), vec![key(Venue::BinanceFutures, "BTC")]).unwrap();
    engine.clock = None;
    for session in ["a", "b", "c"] {
        engine.register_connection_scoped(session, Venue::BinanceFutures, 1, &["BTC".to_owned()]).unwrap();
        engine.mark_subscribed(session);
    }
    for t in [0, 400, 800] {
        for session in ["a", "b", "c"] {
            let _ = engine.receive_batch(vec![frame(session, t, t as i64 + 1)], ns(t), wall(t)).unwrap();
        }
    }
    let scope = key(Venue::BinanceFutures, "BTC");
    let owner = engine.selected.get(&scope).unwrap().session.clone();
    assert!(auth.read().unwrap().contains_key(&scope));

    // Exchange timestamp age is exactly 800ms at t=1550; receive age is 750ms.
    let _ = engine.tick(ns(1550), wall(1550)).unwrap();
    assert!(auth.read().unwrap().contains_key(&scope), "800ms boundary remains eligible");
    let id = engine.manager_ids.get(&(owner.clone(), scope.clone())).unwrap().clone();
    assert!(engine.manager.state(&id).unwrap().websocket, "market age does not mark physical WS unhealthy");

    // At t=1551 exchange timestamp age is 801ms: fail closed without transport transition.
    let _ = engine.tick(ns(1551), wall(1551)).unwrap();
    assert!(!auth.read().unwrap().contains_key(&scope), "data older than 800ms is not authorized");
    assert!(engine.manager.state(&id).unwrap().websocket);
    assert_eq!(engine.selected.get(&scope).map(|s| s.session.as_str()), Some(owner.as_str()));
}

// R1368: test-only shadow observer; does NOT feed any decision or mutate Engine/UCM.
#[test]
fn r1368_scope_cache_shadow_matches_real_engine_dual_hot_lifecycle() {
    #[path="/mnt/spotarb_disk2/SPOT_ARB_R1367_SPLIT_PIPELINE_20261123/r1367_scope_cache.rs"]
    mod isolated_cache;
    let auth: Authority = Arc::new(RwLock::new(HashMap::new()));
    let scope=key(Venue::BinanceFutures,"BTC");
    let mut engine=Engine::new_volatile(auth.clone(),vec![scope.clone()]).unwrap();engine.clock=None;
    let mut cache=isolated_cache::ScopeCache::default();
    for session in ["a","b","c"]{engine.register_connection_scoped(session,Venue::BinanceFutures,1,&["BTC".to_string()]).unwrap();engine.mark_subscribed(session);}
    for t in [0,400,800,1200,1601,2000,2150] {
        let sessions: &[&str]=if t==1200||t==1601{&["b","c"]}else{&["a","b","c"]};
        for session in sessions {
            let _=engine.receive_batch(vec![frame(session,t,t as i64+1)],ns(t),wall(t)).unwrap();
            // Shadow cache observes the actual Engine books; it never decides authority.
            for ((sess,k),book) in &engine.books {
                cache.update(k,sess,isolated_cache::SourceState{generation:book.frame.epoch,receive_ns:book.frame.received_ns,event_ms:book.update.book.exchange_ts_ms.unwrap_or(0),connected:true,validated:book.good>0}).unwrap();
            }
            for ((sess,k),book) in &engine.books {
                let cached=&cache.scope(k).unwrap()[sess];
                assert_eq!(cached.generation,book.frame.epoch);
                assert_eq!(cached.receive_ns,book.frame.received_ns);
            }
            assert_eq!(cache.sources(),engine.books.len());
            if let Some(owner)=engine.selected.get(&scope){assert!(cache.scope(&scope).unwrap().contains_key(&owner.session));}
        }
        let _=engine.tick(ns(t),wall(t)).unwrap();
        let owner=engine.selected.get(&scope).map(|s|s.session.as_str());
        let authority=auth.read().unwrap().get(&scope).map(|s|s.session.clone());
        if let Some(s)=authority.as_deref(){assert_eq!(owner,Some(s));}
    }
    assert_eq!(cache.disconnect("a"),vec![scope.clone()]);
    assert!(cache.scope(&scope).unwrap().contains_key("b"));
    assert!(cache.scope(&scope).unwrap().contains_key("c"));
}

#[test]
fn r1386_cached_ready_set_matches_original_after_marking_and_hot_audit() {
    let authority: Authority = Arc::new(RwLock::new(HashMap::new()));
    let scope = key(Venue::BinanceFutures,"BTC");
    let mut engine = Engine::new_volatile(authority, vec![scope.clone()]).unwrap();
    engine.clock = None;
    for session in ["a", "b", "c"] { engine.register(session).unwrap(); }
    for t in [0u64,400,800] {
        for session in ["a", "b", "c"] {
            engine.receive_batch(vec![frame(session,t,t as i64+1)],ns(t),wall(t)).unwrap();
        }
    }
    // Compare the former all-books revalidation to the R1386 cached ready
    // membership before/after precisely those intervening side effects.
    for t in [800u64,801,1_601] {
        let cached = engine.books.iter().filter(|((_,k),b)|k==&scope&&engine.ready(b,ns(t),wall(t)))
            .map(|((session,_),_)|session.clone()).collect::<Vec<_>>();
        for session in &cached {
            if let Some(book) = engine.books.get_mut(&(session.clone(),scope.clone())) {
                book.qualified=true;
                if !matches!(book.state,State::Active|State::Standby){book.state=State::HotReady;}
            }
        }
        let ordered_scope_sessions=engine.books.iter().filter(|((_,k),_)|k==&scope)
            .map(|((session,_),_)|session.clone()).collect::<Vec<_>>();
        engine.record_readiness_transitions(&scope,ns(t),wall(t),&ordered_scope_sessions);
        engine.record_dual_hot_transition(&scope,&cached,ns(t),wall(t));
        let rescanned = engine.books.iter().filter(|((_,k),b)|k==&scope&&engine.ready(b,ns(t),wall(t)))
            .map(|((session,_),_)|session.clone()).collect::<Vec<_>>();
        assert_eq!(cached,rescanned,"same-time candidate source order/membership changed at {t}ms");
    }
}

// R1397: verify the original self.books HashMap order and fresh per-book state
// are retained when one ordered scope census replaces repeated global scans.
// This includes the standby tie policy (last maximum in iterator order).
#[test]
fn r1397_ordered_hot_scope_census_matches_original_full_map() {
    let authority:Authority=Arc::new(RwLock::new(HashMap::new()));
    let btc=key(Venue::BinanceFutures,"BTC");
    let eth=key(Venue::BinanceFutures,"ETH");
    let mut engine=Engine::new_volatile(authority,vec![btc.clone(),eth.clone()]).unwrap();
    engine.clock=None;
    for session in ["a","b","c"] {engine.register(session).unwrap();}
    for t in [0u64,400,800] {
        for session in ["a","b","c"] {
            let mut eth_frame=frame(session,t,t as i64+1);
            eth_frame.raw=eth_frame.raw.replace("BTCUSDT","ETHUSDT")
                .replace("btcusdt@","ethusdt@");
            engine.receive_batch(vec![frame(session,t,t as i64+1),eth_frame],ns(t),wall(t)).unwrap();
        }
    }
    for t in [800u64,801,1550,1551,1601] {
        for scope in [&btc,&eth] {
            let legacy_order=engine.books.iter()
                .filter(|((_,k),_)|k==scope).map(|((session,_),_)|session.clone())
                .collect::<Vec<_>>();
            let original_ready=engine.books.iter()
                .filter(|((_,k),book)|k==scope&&engine.ready(book,ns(t),wall(t)))
                .map(|((session,_),_)|session.clone()).collect::<Vec<_>>();
            let scoped_ready=legacy_order.iter().filter(|session|
                engine.books.get(&(session.to_string(),scope.clone()))
                    .is_some_and(|book|engine.ready(book,ns(t),wall(t))))
                .cloned().collect::<Vec<_>>();
            assert_eq!(original_ready,scoped_ready,"ready membership+order {scope} {t}");
            let old_masks=engine.books.iter().filter(|((_,k),_)|k==scope)
                .map(|((session,_),book)|(session.clone(),engine.readiness_predicate_mask(book,ns(t),wall(t))))
                .collect::<Vec<_>>();
            let reused_masks=legacy_order.iter().filter_map(|session|
                engine.books.get(&(session.clone(),scope.clone()))
                    .map(|book|(session.clone(),engine.readiness_predicate_mask(book,ns(t),wall(t)))))
                .collect::<Vec<_>>();
            assert_eq!(old_masks,reused_masks,"readiness audit order+bit mask {scope} {t}");
            if let Some(selected)=engine.selected.get(scope) {
                let old_standby=engine.books.iter()
                    .filter(|((session,k),book)|k==scope&&session!=&selected.session&&engine.ready(book,ns(t),wall(t)))
                    .max_by_key(|(_,book)|book.frame.received_ns)
                    .map(|((session,_),_)|session.clone());
                let indexed_standby=legacy_order.iter().filter_map(|session|{
                    if session==&selected.session{return None}
                    let book=engine.books.get(&(session.clone(),scope.clone()))?;
                    engine.ready(book,ns(t),wall(t)).then_some((session,book))
                }).max_by_key(|(_,book)|book.frame.received_ns)
                    .map(|(session,_)|session.clone());
                assert_eq!(old_standby,indexed_standby,"standby tie choice {scope} {t}");
            }
        }
        engine.tick(ns(t),wall(t)).unwrap();
    }
    engine.disconnected("a",ns(1602),wall(1602)).unwrap();
    for scope in [&btc,&eth] {
        let old=engine.books.iter().filter(|((_,k),_)|k==scope)
            .map(|((session,_),_)|session.clone()).collect::<HashSet<_>>();
        assert_eq!(old,engine.book_scope_sessions.get(scope).cloned().unwrap_or_default());
    }
}

#[test]
fn r1379_e0_same_input_tick_and_batch_origin_accounting() {
    fn replay(profile: bool) -> (Vec<Vec<&'static str>>, serde_json::Value, (u64, u64, u64, u64)) {
        let authority: Authority = Arc::new(RwLock::new(HashMap::new()));
        let keys = vec![key(Venue::BinanceFutures, "BTC"), key(Venue::BinanceFutures, "ETH")];
        let mut engine = Engine::new_volatile(authority, keys).unwrap();
        engine.clock = None;
        engine.r1273_profile_enabled = profile;
        engine.register("a").unwrap();
        let mut outcomes = Vec::new();
        for t in [0u64, 100, 200, 300] {
            let incoming = engine.receive_batch(vec![frame("a", t, t as i64 + 1)], ns(t), wall(t)).unwrap();
            let timed = engine.tick(ns(t + 1), wall(t + 1)).unwrap();
            let kinds = incoming.iter().chain(timed.iter()).map(|out| match out {
                Output::Reset { .. } => "reset",
                Output::Frame(_) => "frame",
                Output::Book(_, _) => "book",
                Output::Resubscribe { .. } => "resubscribe",
                Output::Blocked { .. } => "blocked",
            }).collect::<Vec<_>>();
            outcomes.push(kinds);
        }
        let counts = (engine.metrics.received, engine.metrics.invalid,
            engine.metrics.submitted, engine.metrics.switches);
        let profile_snapshot = engine.r1273_profile.lock().unwrap().snapshot();
        (outcomes, profile_snapshot, counts)
    }
    let (without_outputs, without_profile, without_counts) = replay(false);
    let (with_outputs, with_profile, with_counts) = replay(true);
    assert_eq!(without_outputs, with_outputs, "R1273 profiling changed output types");
    assert_eq!(without_counts, with_counts, "R1273 profiling changed Engine counts");
    assert!(without_profile["operations"].as_object().unwrap().is_empty());
    let ops = &with_profile["operations"];
    assert_eq!(ops["e0_tick_total"]["calls"], 4);
    assert_eq!(ops["e0_select_call_tick"]["calls"], 8);
    assert_eq!(ops["e0_select_inner_tick"]["calls"], 8);
    let batch_calls = ops["e0_select_call_batch"]["calls"].as_u64().unwrap_or(0);
    assert!(batch_calls > 0, "offline frames must exercise the batch selection path");
    assert_eq!(ops["e0_select_inner_batch"]["calls"], batch_calls);
    assert_eq!(ops["selection_recovery"]["calls"], batch_calls);
    // R1382: each completed select_inner must account for its exact shared phases.
    let total_select = 8 + batch_calls;
    for stage in ["r1382_ready_scan", "r1382_ready_mark", "r1382_readiness_audit",
        "r1382_candidate_scan", "r1382_manager_choice", "r1382_selection_eval_audit"] {
        assert_eq!(ops[stage]["calls"], total_select, "R1382 stage coverage {stage}");
    }

}

// R1393: the lookup is only a view of Engine books, never an independent
// readiness/authority cache. Compare against the old full-map scan at each step.
#[test]
fn r1393_book_scope_index_census_and_freshness_lifecycle() {
    fn verify(engine:&Engine,scopes:&[String]) {
        let mut expected=HashMap::<String,HashSet<String>>::new();
        for ((session,scope),_) in &engine.books {
            expected.entry(scope.clone()).or_default().insert(session.clone());
        }
        assert_eq!(engine.book_scope_sessions,expected,"index must exactly equal books");
        for scope in scopes {
            let old=engine.books.iter().filter(|((_,key),_)|key==scope)
                .map(|((session,_),_)|session.clone()).collect::<HashSet<_>>();
            let indexed=engine.book_scope_sessions.get(scope).cloned().unwrap_or_default();
            assert_eq!(indexed,old,"no scope member may be omitted or invented");
        }
    }
    let authority:Authority=Arc::new(RwLock::new(HashMap::new()));
    let btc=key(Venue::BinanceFutures,"BTC");
    let eth=key(Venue::BinanceFutures,"ETH");
    let scopes=[btc.clone(),eth.clone()];
    let mut engine=Engine::new_volatile(authority.clone(),scopes.to_vec()).unwrap();
    engine.clock=None;
    // Legacy direct register has no connection_scopes entry; books must still
    // be indexed as soon as they are accepted by the real Engine.
    for session in ["a","b","c"] { engine.register(session).unwrap(); }
    verify(&engine,&scopes);
    for t in [0u64,400,800] {
        for session in ["a","b","c"] {
            let mut eth_frame=frame(session,t,t as i64+1);
            eth_frame.raw=eth_frame.raw.replace("BTCUSDT","ETHUSDT")
                .replace("btcusdt@","ethusdt@");
            engine.receive_batch(vec![frame(session,t,t as i64+1),eth_frame],ns(t),wall(t)).unwrap();
            verify(&engine,&scopes);
        }
    }
    assert_eq!(engine.books.len(),6);
    for t in [800u64,1550,1551,1601] {
        engine.tick(ns(t),wall(t)).unwrap();
        verify(&engine,&scopes);
    }
    assert!(!authority.read().unwrap().contains_key(&btc),
        "no stale 800ms+ book may remain authorized");
    engine.disconnected("a",ns(1602),wall(1602)).unwrap();
    verify(&engine,&scopes);
    assert_eq!(engine.books.len(),4);
    // Direct removal helper is also used by OKX invalid-snapshot recovery.
    let removed=engine.remove_book(&("b".to_string(),btc.clone()));
    assert!(removed.is_some());
    verify(&engine,&scopes);
    assert_eq!(engine.books.len(),3);
    engine.disconnected("b",ns(1603),wall(1603)).unwrap();
    engine.disconnected("c",ns(1604),wall(1604)).unwrap();
    verify(&engine,&scopes);
    assert!(engine.book_scope_sessions.is_empty());
}

// R1402F: same original universe and capacity, but no market books yet.
// Never-ready empty scopes cannot generate per-scope query/history traffic.
#[test]
fn r1402f_empty_universe_tick_does_not_query_worker(){
 let bases=include_str!("../../INSTRUMENT_UNIVERSE.csv").lines().skip(1)
  .map(|row|row.split(',').next().unwrap().to_string()).collect::<Vec<_>>();
 assert_eq!(bases.len(),40);
 let keys=[Venue::BinanceFutures,Venue::OkxSpot].into_iter()
  .flat_map(|v|bases.iter().map(move |base|key(v,base))).collect::<Vec<_>>();
 let auth:Authority=Arc::new(RwLock::new(HashMap::new()));
 let mut e=Engine::new_volatile(auth.clone(),keys).unwrap();e.clock=None;
 e.enable_ucm_worker(32).unwrap();
 for (venue,session) in [(Venue::BinanceFutures,"bin-a"),(Venue::BinanceFutures,"bin-b"),(Venue::OkxSpot,"okx-a"),(Venue::OkxSpot,"okx-b")]{
  e.register_connection_scoped(session,venue,1,&bases).unwrap();
 }
 let before=e.manager.remote().unwrap().worker.revision();
 assert_eq!(before,4);
 for t in (50..=1000).step_by(50){
  assert!(e.tick(ns(t),wall(t)).unwrap().is_empty());
  assert!(e.ucm_dirty.is_empty());assert!(e.ucm_pending.is_empty());
 }
 assert_eq!(e.manager.remote().unwrap().worker.revision()-before,20);
 assert!(e.ucm_history_seen.is_empty());assert!(e.ucm_history_memo.is_empty());
 assert!(auth.read().unwrap().is_empty());assert!(e.manager.check().is_ok());
 println!("R1402F cold universe: 80 scopes * 20 ticks, 20 tick commands, 0 scope queries/history, 0 permits");
}
#[test]
fn r1402f_unchanged_active_does_not_commit_or_repeat_history(){
 let auth:Authority=Arc::new(RwLock::new(HashMap::new()));
 let k=key(Venue::BinanceFutures,"BTC");
 let mut e=Engine::new_volatile(auth.clone(),vec![k.clone()]).unwrap();e.clock=None;
 e.enable_ucm_worker(32).unwrap();
 for session in ["a","b"]{e.register(session).unwrap();}
 for t in [0,400,800]{for session in ["a","b"]{
  e.receive_batch(vec![frame(session,t,t as i64+1)],ns(t),wall(t)).unwrap();
  r1402_drain_worker_engine(&mut e,ns(t),wall(t));
 }}
 let chosen=e.selected.get(&k).unwrap().clone();
 assert!(auth.read().unwrap().contains_key(&k));
 e.tick(ns(850),wall(850)).unwrap();r1402_drain_worker_engine(&mut e,ns(850),wall(850));
 let memo=e.ucm_history_memo.get(&k).unwrap().clone();
 let before=e.manager.remote().unwrap().worker.revision();
 e.tick(ns(900),wall(900)).unwrap();let out=r1402_drain_worker_engine(&mut e,ns(900),wall(900));
 assert_eq!(e.ucm_history_memo.get(&k),Some(&memo));
 assert_eq!(e.manager.remote().unwrap().worker.revision()-before,1,"only Tick mutates, no redundant History/Commit");
 assert_eq!(e.selected.get(&k).unwrap().epoch,chosen.epoch);
 assert_eq!(e.selected.get(&k).unwrap().session,chosen.session);
 assert!(auth.read().unwrap().contains_key(&k));assert!(!out.iter().any(|o|matches!(o,Output::Reset{..})));
 e.tick(ns(1601),wall(1551)).unwrap();r1402_drain_worker_engine(&mut e,ns(1601),wall(1551));
 assert!(!auth.read().unwrap().contains_key(&k));
 assert!(e.readiness_audit.iter().any(|row|row["kind"]=="two_hot_source_deficit_onset"));
 println!("R1402F unchanged active: tick only mutation, no redundant History/Commit; stale gate preserved");
}

#[test]
fn r1402h_worker_coalesced_schedule_and_order_cache(){
 let auth:Authority=Arc::new(RwLock::new(HashMap::new()));
 let btc=key(Venue::BinanceFutures,"BTC");let eth=key(Venue::BinanceFutures,"ETH");
 let mut e=Engine::new_volatile(auth.clone(),vec![btc.clone(),eth.clone()]).unwrap();e.clock=None;
 e.enable_ucm_worker(32).unwrap();
 for session in ["a","b","c"]{e.register(session).unwrap();}
 for t in [0u64,400,800,850] {
  for session in ["a","b","c"]{
   let mut other=frame(session,t,t as i64+1);
   other.raw=other.raw.replace("BTCUSDT","ETHUSDT").replace("btcusdt@","ethusdt@");
   e.receive_batch(vec![frame(session,t,t as i64+1),other],ns(t),wall(t)).unwrap();
   r1402_drain_worker_engine(&mut e,ns(t),wall(t));
   for k in [&btc,&eth] {
    let original=e.books.iter().filter(|((_,scope),_)|scope==k).map(|((s,_),_)|s.clone()).collect::<Vec<_>>();
    assert_eq!(e.ucm_ordered_sessions(k),original);
   }
  }
 }
 // Explicitly schedule both scopes; a stable timer is now allowed to
 // avoid creating these dirty marks in the first place.
 e.select(&btc,ns(900),wall(900),0,"batch").unwrap();
 e.select(&eth,ns(900),wall(900),0,"batch").unwrap();
 let pending=e.ucm_dirty.clone();assert_eq!(pending.len(),2);
 let audit=e.audit.iter().filter(|v|v["kind"]=="engine_call_span"&&v["call"]=="select").count();
 e.tick(ns(950),wall(950)).unwrap();
 assert_eq!(pending,e.ucm_dirty);
 assert_eq!(e.audit.iter().filter(|v|v["kind"]=="engine_call_span"&&v["call"]=="select").count(),audit,"coalesced tick must not emit fake selection span");
 assert!(auth.read().unwrap().is_empty(),"coalesced check still revokes authority");
 r1402_drain_worker_engine(&mut e,ns(950),wall(950));
 e.disconnected("a",ns(951),wall(951)).unwrap();
 r1402_drain_worker_engine(&mut e,ns(951),wall(951));
 for k in [&btc,&eth]{
  let original=e.books.iter().filter(|((_,scope),_)|scope==k).map(|((s,_),_)|s.clone()).collect::<Vec<_>>();
  assert_eq!(e.ucm_ordered_sessions(k),original,"disconnect invalidates cache");
 }
 for b in e.books.values(){assert_eq!(b.output_is_books,serde_json::from_str::<serde_json::Value>(&b.frame.raw).unwrap()["arg"]["channel"]=="books");}
 println!("R1402H coalesced checks preserve revocation and exact scope order");
}

#[test]
fn r1402i_engine_uses_published_scope_without_snapshot_requests(){
 let auth:Authority=Arc::new(RwLock::new(HashMap::new()));
 let k=key(Venue::BinanceFutures,"BTC");
 let mut e=Engine::new_volatile(auth.clone(),vec![k.clone()]).unwrap();e.clock=None;
 e.enable_ucm_worker(32).unwrap();
 for session in ["a","b"]{e.register(session).unwrap();}
 let mut released=0;
 for t in [0u64,400,800,850,900,950]{
  for session in ["a","b"]{
   e.receive_batch(vec![frame(session,t,t as i64+1)],ns(t),wall(t)).unwrap();
   released+=r1402_drain_worker_engine(&mut e,ns(t),wall(t)).iter().filter(|o|matches!(o,Output::Frame(_)|Output::Book(_,_))).count();
  }
  e.tick(ns(t),wall(t)).unwrap();r1402_drain_worker_engine(&mut e,ns(t),wall(t));
 }
 assert!(released>0);
 assert!(auth.read().unwrap().contains_key(&k));
 let count=e.manager.remote().unwrap().worker.snapshot_submissions_for_test();
 if let Some(count)=count{assert_eq!(count,0,"actual Engine must never submit Snapshot");}
 e.tick(ns(1751),wall(1751)).unwrap();r1402_drain_worker_engine(&mut e,ns(1751),wall(1751));
 assert!(!auth.read().unwrap().contains_key(&k));
 println!("R1402I actual Engine released {released}; snapshot command census {count:?}");
}


#[test]
fn r1402k_stable_choice_reuse_and_timer_expiry(){
 let auth:Authority=Arc::new(RwLock::new(HashMap::new()));
 let k=key(Venue::BinanceFutures,"BTC");
 let mut e=Engine::new_volatile(auth.clone(),vec![k.clone()]).unwrap();e.clock=None;
 e.enable_ucm_worker(32).unwrap();
 for session in ["a","b"]{e.register(session).unwrap();}
 for t in [0u64,400,800,850,900,950]{
  for session in ["a","b"]{
   e.receive_batch(vec![frame(session,t,t as i64+1)],ns(t),wall(t)).unwrap();
   r1402_drain_worker_engine(&mut e,ns(t),wall(t));
  }
 }
 assert!(auth.read().unwrap().contains_key(&k));
 // Establish certificate with the post-publication highwater candidate set
 // via the actual ordered Tick, not the removed Proposal query.
 e.tick(ns(960),wall(960)).unwrap();
 r1402_drain_worker_engine(&mut e,ns(960),wall(960));
 for _ in 0..3{
  e.select(&k,ns(960),wall(960),0,"batch").unwrap();
  r1402_drain_worker_engine(&mut e,ns(960),wall(960));
 }
 let proof=e.ucm_stable_choices.get(&k).unwrap().clone();
 assert!(auth.read().unwrap().contains_key(&k));
 let profile=e.ucm_scope_profile.as_ref().unwrap().get(&k).unwrap();
 assert!(profile.certified_proposals>0,"expected exact owner advice reuse");
 let before=e.manager.remote().unwrap().worker.revision();
 let anchor=e.ucm_anchor(&k).unwrap();
 let ticket=e.manager.remote().unwrap().worker.scope_ticket(&anchor);
 assert!(e.ucm_stable_tick(&k,ns(970),wall(970)),"stable scope must have no new work");
 e.tick(ns(970),wall(970)).unwrap();
 assert!(e.ucm_dirty.is_empty());assert!(e.ucm_pending.is_empty());
 assert!(auth.read().unwrap().contains_key(&k));
 assert_eq!(e.manager.remote().unwrap().worker.revision(),before+1);
 assert_eq!(e.manager.remote().unwrap().worker.scope_ticket(&anchor),ticket,"idle Tick must not create a new scope fence");
 // Same candidates but changed quality cannot reuse the proof.
 let eligible=proof.eligible.iter().map(|(id,_)|id.clone()).collect::<Vec<_>>();
 let mut changed=proof.clone();changed.eligible[0].1=Some((u64::MAX,0,1));
 assert!(!e.ucm_stable_choice_matches(&k,&changed,&eligible,ns(970),wall(970)));
 // Exact receive and exchange age expiry must revoke immediately.
 e.tick(ns(1751),wall(1751)).unwrap();
 assert!(!auth.read().unwrap().contains_key(&k));
 r1402_drain_worker_engine(&mut e,ns(1751),wall(1751));
 assert!(!auth.read().unwrap().contains_key(&k));
 println!("R1402K stable advice reused, idle timer skipped, expired permit revoked");
}


// R1406 regression of R1402L. Keep the old admission semantics, but
// independently progress a scope while another continuation is parked.
#[test]
fn r1406_independent_scope_continuation_and_valid_update_revocation(){
 let auth:Authority=Arc::new(RwLock::new(HashMap::new()));
 let btc=key(Venue::BinanceFutures,"BTC");let eth=key(Venue::BinanceFutures,"ETH");
 let mut e=Engine::new_volatile(auth.clone(),vec![btc.clone(),eth.clone()]).unwrap();e.clock=None;
 e.enable_ucm_worker(32).unwrap();
 for session in ["a","b"]{e.register(session).unwrap();}
 let make_eth=|session:&str,t:u64|{
  let mut f=frame(session,t,t as i64+1);f.raw=f.raw.replace("BTCUSDT","ETHUSDT").replace("btcusdt@","ethusdt@");f
 };
 for t in [0u64,400,800,850,900,950]{
  for session in ["a","b"]{
   e.receive_batch(vec![frame(session,t,t as i64+1),make_eth(session,t)],ns(t),wall(t)).unwrap();
   r1402_drain_worker_engine(&mut e,ns(t),wall(t));
  }
 }
 let old=auth.read().unwrap().clone();
 assert_eq!(old.len(),2,"both scopes must start authorized");
 assert!(ns(1000)-old[&btc].received_ns<800_000_000);
 assert!(ns(1000)-old[&eth].received_ns<800_000_000);
 e.receive_batch(vec![frame("a",1000,1001)],ns(1000),wall(1000)).unwrap();
 assert!(!auth.read().unwrap().contains_key(&btc),"valid update revokes still-fresh BTC permit");
 assert!(auth.read().unwrap().contains_key(&eth));
 assert!(e.poll_ucm_worker(ns(1000),wall(1000)).unwrap().is_empty());
 assert!(e.ucm_pending.contains_key(&btc));
 e.receive_batch(vec![make_eth("a",1000)],ns(1000),wall(1000)).unwrap();
 assert!(!auth.read().unwrap().contains_key(&eth),"valid update revokes still-fresh ETH permit");
 assert!(e.ucm_dirty.contains(&eth));
 let eth_id=e.books.values().find(|b|b.update.base=="ETH").unwrap().manager_id.clone();
 let mut barrier=e.begin_ucm_snapshot(&eth_id).unwrap();
 let eth_view=loop{match e.manager.remote_mut().unwrap().worker.poll_snapshot(&mut barrier).unwrap(){
  Some(v)=>break v,None=>std::thread::yield_now()
 }};
 assert_eq!(eth_view.sources.values().filter(|v|v.health.hot_ready).count(),2);
 assert!(eth_view.sources.keys().all(|id|eth_view.source_ready_at(id,ns(1000),wall(1000))));
 // Actual owner barrier proves ETH is healthy. Park BTC's readiness
 // index using a deterministic test seam (not an artificial worker failure).
 // Its continuation, dirty state, and deadline stay intact.
 assert!(e.manager.remote().unwrap().worker.view_current(&eth_id,eth_view.revision));
 assert!(e.manager.view(&eth_id).is_none());
 let mut held=e.take_ucm_scope(&btc).unwrap();
 let original_ticket=held.ticket;held.ticket=u64::MAX;
 e.park_ucm_scope(held).unwrap();
 let limit=std::time::Instant::now()+std::time::Duration::from_millis(40);
 let mut simultaneous=false;
 while !auth.read().unwrap().contains_key(&eth){
  assert!(std::time::Instant::now()<limit,"ETH must progress before BTC resumes");
  e.poll_ucm_worker(ns(1000),wall(1000)).unwrap();
  assert!(e.ucm_pending.contains_key(&btc));
  simultaneous|=e.ucm_pending.contains_key(&eth);
  assert!(!auth.read().unwrap().contains_key(&btc));
  assert_eq!(e.ucm_pending.len(),e.ucm_wait.len());
  assert_eq!(e.ucm_pending.values().filter(|p|p.deadline.is_some()).count(),e.ucm_deadlines.len());
  std::thread::yield_now();
 }
 assert!(simultaneous,"both actual scope continuations must coexist");
 let mut held=e.take_ucm_scope(&btc).unwrap();held.ticket=original_ticket;e.park_ucm_scope(held).unwrap();
 r1402_drain_worker_engine(&mut e,ns(1000),wall(1000));
 assert!(auth.read().unwrap().contains_key(&eth));assert!(auth.read().unwrap().contains_key(&btc));
 assert!(e.ucm_wait.is_empty()&&e.ucm_deadlines.is_empty()&&e.ucm_startable.is_empty());
 assert_eq!(e.metrics.invalid,0);
 println!("R1406_WITNESS ETH permit restored while BTC continuation parked; concurrent scope records; old valid-update revoke unchanged");
}


#[test]
fn r1407b_actual_engine_consumes_owner_event_advice(){
 let auth:Authority=Arc::new(RwLock::new(HashMap::new()));
 let k=key(Venue::BinanceFutures,"BTC");
 let mut e=Engine::new_volatile(auth.clone(),vec![k.clone()]).unwrap();e.clock=None;
 e.enable_ucm_worker(32).unwrap();
 for session in ["a","b"]{e.register(session).unwrap();}
 for t in [0u64,400,800,850,900,950,1000,1050]{
  for session in ["a","b"]{
   e.receive_batch(vec![frame(session,t,t as i64+1)],ns(t),wall(t)).unwrap();
   r1402_drain_worker_engine(&mut e,ns(t),wall(t));
  }
 }
 assert!(auth.read().unwrap().contains_key(&k));
 let profile=e.ucm_scope_profile.as_ref().unwrap().get(&k).unwrap();
 assert!(profile.event_advice_used>0,"must consume actual owner event advice");
 let old_epoch=e.selected[&k].epoch;
 let active=e.selected[&k].session.clone();
 let before=profile.event_advice_used;
 e.receive_batch(vec![frame(&active,1100,1101)],ns(1100),wall(1100)).unwrap();
 let out=r1402_drain_worker_engine(&mut e,ns(1100),wall(1100));
 assert!(out.iter().any(|o|matches!(o,Output::Frame(_)|Output::Book(_,_))));
 assert!(out.iter().all(|o|!matches!(o,Output::Reset{..})));
 assert_eq!(e.selected[&k].epoch,old_epoch);
 assert!(e.ucm_scope_profile.as_ref().unwrap()[&k].event_advice_used>before);
 let anchor=e.books[&(active,k.clone())].manager_id.clone();
 let mut event=e.manager.view(&anchor).unwrap().event_advice.clone().unwrap();
 let inputs=e.prepare_hot_selection_inner(&k,ns(1100),wall(1100),false);
 event.eligible.clear();
 assert!(!e.ucm_event_advice_matches(&k,&inputs,&event),"mismatched basis must not be accepted");
 e.tick(ns(2001),wall(2001)).unwrap();
 r1402_drain_worker_engine(&mut e,ns(2001),wall(2001));
 assert!(!auth.read().unwrap().contains_key(&k));
 println!("R1407B_WITNESS real Engine consumes event advice; ordinary active update publishes with same epoch; mismatched eligibility rejected; stale Permit revoked");
}


// Real Engine and actual worker: hold a switch receipt while a newer,
// compatible valid snapshot arrives. No fake owner acknowledgement.
#[test]
fn r1407c_commit_survives_compatible_observe_and_history_is_not_gate(){
 let auth:Authority=Arc::new(RwLock::new(HashMap::new()));
 let k=key(Venue::BinanceFutures,"BTC");
 let mut e=Engine::new_volatile(auth.clone(),vec![k.clone()]).unwrap();e.clock=None;
 e.enable_ucm_worker(32).unwrap();
 for session in ["a","b"]{e.register(session).unwrap();}
 for t in [0u64,400,800]{for session in ["a","b"]{
  e.receive_batch(vec![frame(session,t,t as i64+1)],ns(t),wall(t)).unwrap();
  if t!=800||session!="b"{r1402_drain_worker_engine(&mut e,ns(t),wall(t));}
 }}
 let limit=std::time::Instant::now()+std::time::Duration::from_secs(1);
 let active=loop{
  if let Some(PendingHotSelection{stage:PendingHotStage::Commit{candidate,..},..})=e.ucm_pending.get(&k){break candidate.logical_session.clone()}
  assert!(std::time::Instant::now()<limit);
  assert!(e.poll_ucm_worker(ns(800),wall(800)).unwrap().is_empty());std::thread::yield_now();
 };
 assert!(!auth.read().unwrap().contains_key(&k));
 assert!(!e.ucm_history_context.is_empty(),"history receipt has not gated commit submission");
 let reserved=e.memory_epoch;
 e.tick(ns(805),wall(805)).unwrap();
 assert!(!auth.read().unwrap().contains_key(&k),"no Permit before current applied reconciliation");
 e.receive_batch(vec![frame(&active,810,811)],ns(810),wall(810)).unwrap();
 let out=r1402_drain_worker_engine(&mut e,ns(810),wall(810));
 assert_eq!(out.iter().filter(|o|matches!(o,Output::Reset{..})).count(),1);
 assert!(out.iter().any(|o|matches!(o,Output::Frame(_)|Output::Book(_,_))));
 assert_eq!(e.memory_epoch,reserved,"do not repeat the real Commit for ordinary data");
 assert_eq!(e.selected[&k].session,active);
 assert!(auth.read().unwrap().contains_key(&k));
 assert!(e.ucm_history_context.is_empty());
 let profile=&e.ucm_scope_profile.as_ref().unwrap()[&k];
 assert_eq!(profile.commit_reconciled,1);
 assert_eq!(profile.commit_destructive_rejected,0);
 println!("R1407C_WITNESS real Commit survives ordinary Tick and newer snapshot; history receipt not a gate; exactly one Reset and epoch");
}


#[test]
fn r1407c_commit_cannot_cross_local_witness_revocation_then_recovery(){
 let auth:Authority=Arc::new(RwLock::new(HashMap::new()));
 let k=key(Venue::BinanceFutures,"BTC");
 let mut e=Engine::new_volatile(auth.clone(),vec![k.clone()]).unwrap();e.clock=None;
 e.enable_ucm_worker(32).unwrap();
 for session in ["a","b"]{e.register(session).unwrap();}
 for t in [0u64,400,800]{for session in ["a","b"]{
  e.receive_batch(vec![frame(session,t,t as i64+1)],ns(t),wall(t)).unwrap();
  if t!=800||session!="b"{r1402_drain_worker_engine(&mut e,ns(t),wall(t));}
 }}
 let limit=std::time::Instant::now()+std::time::Duration::from_secs(1);
 let active=loop{
  if let Some(PendingHotSelection{stage:PendingHotStage::Commit{candidate,..},..})=e.ucm_pending.get(&k){break candidate.logical_session.clone()}
  assert!(std::time::Instant::now()<limit);
  e.poll_ucm_worker(ns(800),wall(800)).unwrap();std::thread::yield_now();
 };
 let reserved=e.memory_epoch;
 let witness=e.books[&(active.clone(),k.clone())].snapshot_witness.clone().unwrap();
 // Duplicate sequence is rejected and invalidates the old witness,
 // with no owner Observe. Next valid full snapshot is not a new session.
 e.receive_batch(vec![frame(&active,800,801)],ns(801),wall(801)).unwrap();
 assert!(!witness.is_live());assert_eq!(e.ucm_witness_revocations[&k],1);
 e.receive_batch(vec![frame(&active,810,811)],ns(810),wall(810)).unwrap();
 assert!(e.books[&(active.clone(),k.clone())].snapshot_witness.as_ref().unwrap().is_live());
 let mut rejected=false;
 let limit=std::time::Instant::now()+std::time::Duration::from_secs(1);
 while !rejected{
  assert!(std::time::Instant::now()<limit);
  let out=e.poll_ucm_worker(ns(810),wall(810)).unwrap();
  assert!(out.iter().all(|o|!matches!(o,Output::Reset{..}|Output::Frame(_)|Output::Book(_,_))));
  assert!(!auth.read().unwrap().contains_key(&k));
  rejected=e.ucm_scope_profile.as_ref().unwrap()[&k].commit_destructive_rejected>0;
  std::thread::yield_now();
 }
 assert!(e.selected.is_empty());assert_eq!(e.memory_epoch,reserved);
 r1402_drain_worker_engine(&mut e,ns(810),wall(810));
 // If recovery obtains a new applied switch, it must have a new epoch.
 if let Some(selected)=e.selected.get(&k){assert!(selected.epoch>reserved);}
 println!("R1407C_WITNESS duplicate revocation then valid recovery cannot authorize old applied Commit");
}


// An old scope's fact fence is not a receipt consumption barrier. Fill
// the existing control lane with actual completed owner census receipts,
// then advance a scope that must submit its first History observation.
#[test]
fn r1408a_completed_history_receipts_do_not_wait_for_older_scope(){
 let auth:Authority=Arc::new(RwLock::new(HashMap::new()));
 let k=key(Venue::BinanceFutures,"BTC");
 let mut e=Engine::new_volatile(auth.clone(),vec![k.clone()]).unwrap();e.clock=None;
 e.enable_ucm_worker(32).unwrap();e.register("a").unwrap();
 e.receive_batch(vec![frame("a",0,1)],ns(0),wall(0)).unwrap();
 e.poll_ucm_worker(ns(0),wall(0)).unwrap();
 let ticket=e.ucm_pending[&k].ticket;
 let cap=e.manager.remote().unwrap().capacity;
 for _ in 0..cap{
  e.request_ucm_census().unwrap();
  let r=e.manager.remote().unwrap();
  let last=r.worker.submitted_ticket();
  let limit=std::time::Instant::now()+std::time::Duration::from_millis(50);
  while r.worker.completed_ticket()<last{
   assert!(std::time::Instant::now()<limit);std::thread::yield_now();
  }
 }
 let r=e.manager.remote().unwrap();
 assert_eq!(r.retirements.len(),cap);assert!(r.census.is_none());
 assert!(r.retirements.front().unwrap().ticket>ticket);
 assert!(r.retirements.back().unwrap().ticket<=r.worker.completed_ticket());
 // Old algorithm tries to submit scope history while cap is exhausted.
 // New algorithm drains the ready FIFO front before advancing the scope.
 assert!(e.poll_ucm_worker(ns(0),wall(0)).unwrap().is_empty());
 assert!(e.manager.remote().unwrap().census.is_some());
 assert!(!e.ucm_pending.contains_key(&k));
 assert_eq!(e.ucm_history_context.len(),1);
 assert!(e.manager.remote().unwrap().retirements.len()<=cap);
 assert!(auth.read().unwrap().is_empty());
 r1402_drain_worker_engine(&mut e,ns(0),wall(0));
 assert!(e.ucm_history_context.is_empty());
 assert!(e.manager.remote().unwrap().retirements.is_empty());
 assert!(e.manager.check().is_ok());
 println!("R1408A_WITNESS full control lane of {cap} actual completed receipts drains independently of older scope fence");
}


#[test]
fn r1408b_scope_clock_regions_exact_and_bounded(){
 let mut idx=ScopeClockIndex::default();
 let a="BinanceFutures:BTC";let b="BinanceFutures:ETH";
 idx.update(a,1_000_000_000,2000,[(1_000_000_000,Some(2000))].into_iter());
 idx.update(b,1_000_000_000,2000,[(1_000_000_000,Some(1900))].into_iter());
 assert!(idx.due(1_800_000_000,2700).is_empty());
 assert_eq!(idx.due(1_800_000_000,2701),HashSet::from([b.to_string()]));
 assert_eq!(idx.due(1_800_000_001,2700),HashSet::from([a.to_string(),b.to_string()]));
 assert_eq!(idx.due(999_999_999,2000),HashSet::from([a.to_string(),b.to_string()]));
 assert_eq!(idx.due(1_000_000_000,1999),HashSet::from([a.to_string()]));
 for t in 0..10_000u64{
  idx.update(a,t,t as i64,[(t,Some(t as i64))].into_iter());
  assert_eq!(idx.regions.len(),2);assert!(idx.edges.iter().all(|x|x.len()==2));
 }
 idx.update(a,20_000_000_000,20_000,[(1000,Some(1000))].into_iter());
 assert!(!idx.due(20_100_000_000,20_100).contains(a),"already expired must not repeat");
 assert!(idx.due(1000,1000).contains(a),"backward transition re-evaluates");
 idx.remove(a);assert_eq!(idx.regions.len(),1);assert!(idx.edges.iter().all(|x|x.len()==1));
 println!("R1408B_WITNESS exact independent clock boundaries, future/backward crossing, 10000 replacements bounded");
}


#[test]
fn r1409c_engine_known_bounded_across_runtime_group_reconnect(){
 let auth:Authority=Arc::new(RwLock::new(HashMap::new()));
 let bases=vec!["BTC".to_string(),"ETH".to_string()];
 let k=key(Venue::BinanceFutures,"BTC");
 let mut e=Engine::new_volatile(auth.clone(),bases.iter().map(|b|key(Venue::BinanceFutures,b)).collect()).unwrap();e.clock=None;
 e.enable_ucm_worker(32).unwrap();
 let mut previous=None;
 for generation in 1..=32u64{
  let session=format!("g0-BinanceFutures-0-{generation}-{generation:016x}");
  e.register_connection_scoped(&session,Venue::BinanceFutures,generation,&bases).unwrap();
  let anchor=e.manager_ids[&(session.clone(),k.clone())].clone();
  let mut pending=e.begin_ucm_snapshot(&anchor).unwrap();
  let view=loop{if let Some(v)=e.manager.remote_mut().unwrap().worker.poll_snapshot(&mut pending).unwrap(){break v}std::thread::yield_now()};
  assert_eq!(view.sources.len(),1);assert!(view.sources.contains_key(&anchor));
  assert_eq!(e.manager.remote().unwrap().known.len(),2);
  assert_eq!(e.manager_ids.len(),2);assert_eq!(e.sources.len(),1);
  if let Some(old)=previous.as_ref(){assert!(!e.manager.has_source(old));assert!(!e.sources.contains_key(&old.logical_session));}
  e.disconnected(&session,ns(generation*10),wall(generation*10)).unwrap();
  r1402_drain_worker_engine(&mut e,ns(generation*10),wall(generation*10));
  assert!(auth.read().unwrap().is_empty());assert!(e.manager_ids.is_empty());assert!(e.sources.is_empty());
  previous=Some(anchor);
 }
 assert_eq!(e.manager.remote().unwrap().known.len(),2);
 println!("R1409C_WITNESS actual Engine register/disconnect 32 generations x2 scopes; known remains 2, no live session/book/Permit after close");
}


#[test]
fn r1409c_real_pending_commit_and_late_frame_after_lane_replacement(){
 let auth:Authority=Arc::new(RwLock::new(HashMap::new()));
 let k=key(Venue::BinanceFutures,"BTC");
 let bases=vec!["BTC".to_string()];
 let mut e=Engine::new_volatile(auth.clone(),vec![k.clone()]).unwrap();e.clock=None;
 e.enable_ucm_worker(32).unwrap();
 let sessions=["g0-BinanceFutures-0-1-0000000000000001","g0-BinanceFutures-1-1-0000000000000002"];
 let channel=adapter_for(Venue::BinanceFutures).channel();
 let make=|session:&str,generation:u64,t:u64|{
  let mut f=frame(session,t,t as i64+1);
  f.raw=f.raw.replace("depth20@500ms",channel);
  f.epoch=generation;f.source=Some(provenance(session.into(),generation));f
 };
 for session in sessions{e.register_connection_scoped(session,Venue::BinanceFutures,1,&bases).unwrap();}
 for t in [0u64,400,800]{for session in sessions{
  e.receive_batch(vec![make(session,1,t)],ns(t),wall(t)).unwrap();
  if t!=800||session!=sessions[1]{r1402_drain_worker_engine(&mut e,ns(t),wall(t));}
 }}
 let limit=std::time::Instant::now()+std::time::Duration::from_secs(1);
 let old=loop{
  if let Some(PendingHotSelection{stage:PendingHotStage::Commit{candidate,..},..})=e.ucm_pending.get(&k){break candidate.clone()}
  assert!(std::time::Instant::now()<limit);
  assert!(e.poll_ucm_worker(ns(800),wall(800)).unwrap().is_empty());std::thread::yield_now();
 };
 let reserved=e.memory_epoch;
 assert!(auth.read().unwrap().is_empty());
 let survivor=sessions.iter().copied().find(|s|*s!=old.logical_session.as_str()).unwrap();
 let replacement=format!("{}-2-0000000000000003",old.connection_group);
 assert!(e.disconnected(&old.logical_session,ns(801),wall(801)).unwrap().is_empty());
 e.register_connection_scoped(&replacement,Venue::BinanceFutures,2,&bases).unwrap();
 let fresh=e.manager_ids[&(replacement.clone(),k.clone())].clone();
 assert!(!e.manager.has_source(&old));
 assert_eq!(e.manager.remote().unwrap().known.len(),2);
 let invalid=e.metrics.invalid;
 assert!(e.receive_batch(vec![make(&old.logical_session,1,810)],ns(810),wall(810)).unwrap().is_empty());
 assert_eq!(e.metrics.invalid,invalid+1);
 assert!(r1402_drain_worker_engine(&mut e,ns(810),wall(810)).is_empty());
 assert!(auth.read().unwrap().is_empty());assert!(e.selected.is_empty());
 assert!(e.manager.check().is_ok());
 let mut receipt=e.begin_ucm_snapshot(&fresh).unwrap();
 let v=loop{if let Some(v)=e.manager.remote_mut().unwrap().worker.poll_snapshot(&mut receipt).unwrap(){break v}std::thread::yield_now()};
 assert!(!v.sources.contains_key(&old));assert_eq!(v.sources.len(),2);
 let mut out=Vec::new();
 for t in [900u64,1200,1500]{
  out.extend(e.receive_batch(vec![make(survivor,1,t),make(&replacement,2,t)],ns(t),wall(t)).unwrap());
  out.extend(r1402_drain_worker_engine(&mut e,ns(t),wall(t)));
 }
 assert_eq!(out.iter().filter(|o|matches!(o,Output::Reset{..})).count(),1);
 assert!(out.iter().any(|o|matches!(o,Output::Frame(_)|Output::Book(_,_))));
 assert!(e.selected[&k].epoch>reserved);
 assert_ne!(e.selected[&k].session,old.logical_session);
 assert!(auth.read().unwrap().contains_key(&k));
 println!("R1409C_WITNESS pending actual Commit cannot cross retired generation; late raw frame rejected; fresh dual HOT requalifies with new epoch");
}
