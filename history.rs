//! Bounded multi-request transport; only contiguous, matched DB1 ACKs become visible.
use crate::{CoreError,HistorySink,RawFrame,Update};
use serde_json::{json,Value};
use std::{collections::{BTreeMap,BTreeSet},io::{Read,Write},os::unix::net::UnixStream,time::{Duration,Instant}};

#[derive(Default)]
pub struct Frontier {pub through:u64,done:BTreeSet<u64>}
impl Frontier {
    pub fn mark(&mut self,id:u64){if id>self.through{self.done.insert(id);while self.done.remove(&(self.through+1)){self.through+=1;}}}
}
struct Request {deadline:Instant,out:Vec<u8>,sent:usize,ram:Option<Value>,receipt:Option<Value>}
pub struct HistoryClient {
    stream:UnixStream,active:BTreeMap<u64,Request>,input:Vec<u8>,serial:u64,capacity:usize,range:bool,
    pub admitted:u64,pub committed:u64,pub created:u64,pub frontier:Frontier,
    pub journal:bool,pub peak:usize,pub messages:u64,pub read_bytes:u64,pub write_bytes:u64,
}
impl HistoryClient {
    pub fn connect(path:&str,capacity:usize,range:bool)->std::io::Result<Self>{
        Self::connect_mode(path,capacity,range,false)
    }
    pub fn connect_journal(path:&str,capacity:usize)->std::io::Result<Self>{Self::connect_mode(path,capacity,true,true)}
    fn connect_mode(path:&str,capacity:usize,range:bool,journal:bool)->std::io::Result<Self>{
        assert!([1,2,4,8,16,32].contains(&capacity));
        let stream=UnixStream::connect(path)?;stream.set_nonblocking(true)?;
        Ok(Self{stream,active:BTreeMap::new(),input:Vec::new(),serial:0,capacity,range,admitted:0,committed:0,created:0,
            frontier:Frontier::default(),journal,peak:0,messages:0,read_bytes:0,write_bytes:0})
    }
    pub fn can_start(&self)->bool{self.active.len()<self.capacity}
    pub fn outstanding(&self)->usize{self.active.len()}
    pub fn waiting(&self)->bool{self.active.values().any(|r|r.receipt.is_none())}
    pub fn start(&mut self,context:Value)->Result<u64,CoreError>{
        if !self.can_start(){return Err(CoreError::HistoryPressure)}
        let deadline=Instant::now()+Duration::from_millis(200);let id=self.serial+1;
        let raw=json!({"id":id,"op":"event","ack_mode":if self.journal{"journal"}else if self.range{"range"}else{"event"},"context":context}).to_string();
        if raw.len()+1>65_536{return Err(CoreError::HistoryPressure)}
        let mut out=raw.into_bytes();out.push(b'\n');self.serial=id;self.created+=1;
        self.active.insert(id,Request{deadline,out,sent:0,ram:None,receipt:None});self.peak=self.peak.max(self.active.len());Ok(id)
    }
    fn ack(&mut self,mut row:Value)->Result<(),CoreError>{
        let id=row["id"].as_u64().ok_or(CoreError::HistoryPressure)?;
        let request=self.active.get_mut(&id).ok_or(CoreError::HistoryPressure)?;
        if row["ok"]!=true{return Err(CoreError::HistoryPressure)}
        if request.ram.is_none(){
            if row["phase"]!="ram_admitted"||!row["event_id"].is_string()||!row["content_hash"].is_string(){return Err(CoreError::HistoryPressure)}
            self.admitted+=1;request.ram=Some(row);
        }else{
            let ram=request.ram.as_ref().unwrap();
            let valid_durable=if self.journal {
                row["phase"]=="journal_durable"&&row["durability"]=="journal_sync_all_and_floor"
                &&row["durable_ns"].as_u64().unwrap_or(0)>0
                &&row["journal_seq"].as_u64().unwrap_or(0)>0
                &&row["journal_through_seq"].as_u64().unwrap_or(0)>=row["journal_seq"].as_u64().unwrap_or(u64::MAX)
            }else{row["phase"]=="db1_committed"&&row["db1_commit_ns"].as_u64().unwrap_or(0)>0};
            if request.receipt.is_some()||!valid_durable||row["event_id"]!=ram["event_id"]||row["content_hash"]!=ram["content_hash"]{
                return Err(CoreError::HistoryPressure)}
            row["native_ack_received_ns"]=json!(crate::transport::monotonic_ns());self.committed+=1;request.receipt=Some(row);self.frontier.mark(id);
            // A matched physical completion proves the bounded writer is moving.
            // Renew only already RAM-admitted requests; silence, RAM-only traffic,
            // and requests that never reached admission still time out.
            let deadline=Instant::now()+Duration::from_millis(200);
            for pending in self.active.values_mut(){
                if pending.ram.is_some()&&pending.receipt.is_none(){pending.deadline=deadline;}
            }
        }
        Ok(())
    }
    fn response(&mut self,row:Value)->Result<(),CoreError>{
        if row["phase"]!="db1_range"&&row["phase"]!="journal_range"{return self.ack(row)}
        if !self.range||row["ok"]!=true||self.journal!=(row["phase"]=="journal_range"){return Err(CoreError::HistoryPressure)}
        let through=row["through_id"].as_u64().ok_or(CoreError::HistoryPressure)?;
        let items=row["items"].as_array().filter(|a|!a.is_empty()&&a.len()<=32).ok_or(CoreError::HistoryPressure)?;
        for item in items {
            let mut ack=item.clone();ack["phase"]=json!(if self.journal{"journal_durable"}else{"db1_committed"});ack["ok"]=json!(true);
            if self.journal {
                for key in ["journal_through_seq","durable_ns","durability","registrar_receive_ns","journal_sync_ns","floor_sync_ns","chain_hash"]{ack[key]=row[key].clone();}
            }else{for key in ["seq","feed_generation","writer_started_ns","db1_commit_ns","commit_call_ns","transaction_ns","maintenance_lock_ns"]{ack[key]=row[key].clone();}}
            self.ack(ack)?;
        }
        if through!=self.frontier.through{return Err(CoreError::HistoryPressure)}Ok(())
    }
    pub fn poll(&mut self)->Result<bool,CoreError>{
        let now=Instant::now();if self.active.values().any(|r|r.receipt.is_none()&&now>r.deadline){return Err(CoreError::HistoryPressure)}
        let mut progress=false;
        for request in self.active.values_mut(){
            while request.sent<request.out.len(){
                match self.stream.write(&request.out[request.sent..]){
                    Ok(0)=>return Err(CoreError::HistoryPressure),Ok(n)=>{request.sent+=n;self.write_bytes+=n as u64;progress=true;},
                    Err(e) if e.kind()==std::io::ErrorKind::WouldBlock=>return Ok(progress),
                    Err(e) if e.kind()==std::io::ErrorKind::Interrupted=>continue,
                    Err(_)=>return Err(CoreError::HistoryPressure),
                }
            }
        }
        loop {
            if let Some(end)=self.input.iter().position(|b|*b==b'\n'){
                if end+1>16_384{return Err(CoreError::HistoryPressure)}
                let raw:Vec<_>=self.input.drain(..=end).collect();let row=serde_json::from_slice(&raw).map_err(|_|CoreError::HistoryPressure)?;
                self.response(row)?;self.messages+=1;progress=true;continue;
            }
            if self.input.len()>=16_384{return Err(CoreError::HistoryPressure)}
            let mut bytes=[0;8192];match self.stream.read(&mut bytes){
                Ok(0)=>return Err(CoreError::HistoryPressure),Ok(n)=>{self.input.extend_from_slice(&bytes[..n]);self.read_bytes+=n as u64;},
                Err(e) if e.kind()==std::io::ErrorKind::WouldBlock=>return Ok(progress),
                Err(e) if e.kind()==std::io::ErrorKind::Interrupted=>continue,
                Err(_)=>return Err(CoreError::HistoryPressure),
            }
        }
    }
    pub fn take(&mut self,id:u64)->Option<Value>{
        if id>self.frontier.through||self.active.get(&id)?.receipt.is_none(){return None}
        self.active.remove(&id).and_then(|r|r.receipt)
    }
}

/// Canonical application after matching durability and context validation.
pub struct DurableSignal<'a>{pub expected:&'a Update}
impl HistorySink for DurableSignal<'_>{
    fn accept(&mut self,_:&RawFrame,update:&Update)->Result<(),CoreError>{
        if update!=self.expected{return Err(CoreError::Invalid("durable signal changed"))}Ok(())
    }
}
/// Only the private prepared Owner clone uses this sink. Never canonical mutation/publication.
pub struct PreparedSignal;
impl HistorySink for PreparedSignal{fn accept(&mut self,_:&RawFrame,_:&Update)->Result<(),CoreError>{Ok(())}}

#[cfg(test)]mod tests{
 use super::*;use std::os::unix::net::UnixListener;
 #[test]fn frontier_cannot_cross_a_missing_predecessor(){let mut f=Frontier::default();f.mark(1);f.mark(3);assert_eq!(f.through,1);f.mark(2);assert_eq!(f.through,3);f.mark(3);assert_eq!(f.through,3);}
 fn pair()->(HistoryClient,UnixStream,String){
  let path=format!("/dev/shm/native-history-client-test-{}-{:?}.sock",std::process::id(),std::thread::current().id());
  let _=std::fs::remove_file(&path);let listener=UnixListener::bind(&path).unwrap();let client=HistoryClient::connect(&path,4,true).unwrap();let(server,_)=listener.accept().unwrap();(client,server,path)
 }
 fn reply(s:&mut UnixStream,r:Value){s.write_all((r.to_string()+"\n").as_bytes()).unwrap();}
 #[test]fn ram_is_not_commit_and_mismatched_ack_fails(){
  let(mut c,mut s,path)=pair();let id=c.start(json!({})).unwrap();c.poll().unwrap();
  reply(&mut s,json!({"id":id,"phase":"ram_admitted","ok":true,"event_id":"a","content_hash":"b"}));c.poll().unwrap();assert_eq!(c.committed,0);assert!(c.take(id).is_none());
  reply(&mut s,json!({"id":id,"phase":"db1_committed","ok":true,"event_id":"other","content_hash":"b","db1_commit_ns":1}));assert_eq!(c.poll(),Err(CoreError::HistoryPressure));let _=std::fs::remove_file(path);
 }
 #[test]fn out_of_order_range_ack_does_not_expose_later_commit(){
  let(mut c,mut s,path)=pair();for _ in 0..4{c.start(json!({})).unwrap();}assert!(c.start(json!({})).is_err());c.poll().unwrap();
  for id in 1..=4{reply(&mut s,json!({"id":id,"phase":"ram_admitted","ok":true,"event_id":id.to_string(),"content_hash":"h"}));}c.poll().unwrap();
  let item=|id:u64|json!({"id":id,"event_id":id.to_string(),"content_hash":"h"});
  reply(&mut s,json!({"phase":"db1_range","ok":true,"through_id":1,"db1_commit_ns":1,"items":[item(1),item(3)]}));c.poll().unwrap();assert_eq!(c.frontier.through,1);assert!(c.take(3).is_none());assert!(c.take(1).is_some());
  reply(&mut s,json!({"phase":"db1_range","ok":true,"through_id":3,"db1_commit_ns":2,"items":[item(2)]}));c.poll().unwrap();assert_eq!(c.frontier.through,3);assert!(c.take(2).is_some());assert!(c.take(3).is_some());assert!(c.take(4).is_none());let _=std::fs::remove_file(path);
 }
 #[test]fn journal_ack_is_distinct_and_ram_cannot_mutate(){
  let(mut c,mut s,path)=pair();c.journal=true;let id=c.start(json!({})).unwrap();c.poll().unwrap();
  reply(&mut s,json!({"id":id,"phase":"ram_admitted","ok":true,"event_id":"a","content_hash":"b"}));c.poll().unwrap();assert!(c.take(id).is_none());
  reply(&mut s,json!({"id":id,"phase":"journal_durable","ok":true,"event_id":"a","content_hash":"b","durability":"journal_sync_all_and_floor","journal_seq":1,"journal_through_seq":1,"durable_ns":1}));
  c.poll().unwrap();let receipt=c.take(id).unwrap();assert_eq!(receipt["phase"],"journal_durable");assert!(receipt["db1_commit_ns"].is_null());let _=std::fs::remove_file(path);
 }
 #[test]fn matched_durable_progress_renews_only_admitted_waiters(){
  let(mut c,mut s,path)=pair();c.journal=true;
  let first=c.start(json!({})).unwrap();let second=c.start(json!({})).unwrap();let unadmitted=c.start(json!({})).unwrap();
  c.poll().unwrap();
  for id in [first,second]{
   reply(&mut s,json!({"id":id,"phase":"ram_admitted","ok":true,"event_id":id.to_string(),"content_hash":"h"}));
  }
  c.poll().unwrap();
  let short=Instant::now()+Duration::from_millis(100);
  c.active.get_mut(&second).unwrap().deadline=short;
  c.active.get_mut(&unadmitted).unwrap().deadline=short;
  reply(&mut s,json!({"id":first,"phase":"journal_durable","ok":true,"event_id":first.to_string(),"content_hash":"h",
   "durability":"journal_sync_all_and_floor","journal_seq":1,"journal_through_seq":1,"durable_ns":1}));
  c.poll().unwrap();
  assert!(c.active[&second].deadline>Instant::now()+Duration::from_millis(150));
  assert_eq!(c.active[&unadmitted].deadline,short);
  assert_eq!(c.committed,1);assert_eq!(c.frontier.through,1);assert!(c.take(second).is_none());
  let _=std::fs::remove_file(path);
 }
 #[test]fn ram_only_and_stalled_durable_request_still_fail_closed(){
  let(mut c,mut s,path)=pair();c.journal=true;
  let id=c.start(json!({})).unwrap();c.poll().unwrap();
  let expired=Instant::now()-Duration::from_millis(1);
  c.active.get_mut(&id).unwrap().deadline=expired;
  reply(&mut s,json!({"id":id,"phase":"ram_admitted","ok":true,"event_id":"a","content_hash":"b"}));
  assert_eq!(c.poll(),Err(CoreError::HistoryPressure));
  assert_eq!(c.committed,0);assert_eq!(c.frontier.through,0);let _=std::fs::remove_file(path);
 }
 #[test]fn journal_client_rejects_db1_and_false_prefix_ack(){
  for bad in 0..2{
   let(mut c,mut s,path)=pair();c.journal=true;let id=c.start(json!({})).unwrap();c.poll().unwrap();
   reply(&mut s,json!({"id":id,"phase":"ram_admitted","ok":true,"event_id":"a","content_hash":"b"}));c.poll().unwrap();
   let phase=if bad==0{"db1_committed"}else{"journal_durable"};
   reply(&mut s,json!({"id":id,"phase":phase,"ok":true,"event_id":"a","content_hash":"b","db1_commit_ns":1,"durability":"journal_sync_all_and_floor","journal_seq":2,"journal_through_seq":1,"durable_ns":1}));
   assert_eq!(c.poll(),Err(CoreError::HistoryPressure));let _=std::fs::remove_file(path);
  }
 }
}
