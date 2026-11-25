//! Offline-only R1323 validation path. Never linked into a non-test library.
//! No socket, execution owner, database, scanner or refresh task is constructed.
use crate::{owner_index, parse, CoreError, HistorySink, Owner, RawFrame, Update, Venue};
use crate::decision::{Decision, DecisionConfig, DecisionCore};
use crate::market_metadata::{build_snapshot, NativeMetadataSnapshot};
use crate::native_r167_source_ws_shards_review::{split_source_groups_for_ingress, SourceWsShardPlanR167};
use crate::native_trading_config::NativeTradingConfig;
use serde_json::{json, Value};
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;

const OWNERS: usize = 12;
const QUEUE_CAPACITY: usize = 16_384;
const NS: u64 = 10_000_000_000;
const WALL: i64 = 10_000;
thread_local! { static EVALUATIONS: RefCell<BTreeMap<String,u64>> = RefCell::new(BTreeMap::new()); }
// Called at the REAL evaluate() entry; catches evaluation of unrelated symbols.
pub(crate) fn record_evaluation(base: &str) {
    EVALUATIONS.with(|v| *v.borrow_mut().entry(base.into()).or_default() += 1);
}
fn take_evaluations() -> BTreeMap<String,u64> {
    EVALUATIONS.with(|v| std::mem::take(&mut *v.borrow_mut()))
}
fn digest(bytes: &[u8]) -> String {
    openssl::sha::sha256(bytes).iter().map(|b| format!("{b:02x}")).collect()
}
fn config() -> DecisionConfig {
    DecisionConfig { max_age_ms:800, metadata_ttl_ms:43_200_000, base_size:10.0,
        risk_cap:10.0, cost:24.03, required_net:42.0, min_price:0.0,
        max_price:f64::INFINITY, blacklist:HashSet::new() }
}
fn historical() -> NativeMetadataSnapshot {
    build_snapshot(include_str!("../fixtures/r1323/binance_historical.json"),
        include_str!("../fixtures/r1323/okx_historical.json"), 1, NS, WALL as u64).unwrap()
}
fn synthetic(bases: &[&str], generation: u64) -> NativeMetadataSnapshot {
    let b: Vec<_> = bases.iter().map(|b| json!({"symbol":format!("{b}USDT"),
        "baseAsset":b,"quoteAsset":"USDT","status":"TRADING","contractType":"PERPETUAL",
        "filters":[{"filterType":"LOT_SIZE","minQty":"0.001","stepSize":"0.001","maxQty":"10000"},
        {"filterType":"PRICE_FILTER","tickSize":"0.01"},
        {"filterType":"MIN_NOTIONAL","notional":"5"}]})).collect();
    let o: Vec<_> = bases.iter().map(|b| json!({"instId":format!("{b}-USDT"),
        "baseCcy":b,"quoteCcy":"USDT","instType":"SPOT","state":"live",
        "minSz":"0.001","lotSz":"0.001","tickSz":"0.01"})).collect();
    build_snapshot(&json!({"symbols":b}).to_string(),&json!({"code":"0","data":o}).to_string(),
        generation,NS,WALL as u64).unwrap()
}
fn eligible(meta: &NativeMetadataSnapshot) -> Vec<String> {
    meta.markets.iter().filter(|(_,m)| m.metadata_state()=="SIZE_METADATA_READY")
        .map(|(b,_)|b.clone()).collect()
}
fn manifest(meta: &NativeMetadataSnapshot) -> Value {
    let bases=eligible(meta);
    let cfg=NativeTradingConfig::parse(include_str!("../config/native_trading.yaml")).unwrap();
    let plan=SourceWsShardPlanR167::from_source(&cfg,&bases).unwrap();
    let groups=split_source_groups_for_ingress(&plan.groups,40).unwrap();
    let mut seen=BTreeSet::new();
    for base in groups.iter().flatten() { assert!(seen.insert(base.clone()),"duplicate group base"); }
    assert_eq!(seen,bases.iter().cloned().collect::<BTreeSet<_>>());
    let assignments:Vec<_>=bases.iter().map(|base| {
        let candidates:Vec<_>=(0..OWNERS).filter(|i|Owner::new(*i,OWNERS,800).unwrap().owns(base)).collect();
        assert_eq!(candidates,vec![owner_index(base,OWNERS)]);
        assert!(meta.routes.contains_key(base));
        json!({"base":base,"owner":candidates[0]})
    }).collect();
    json!({"classification":"HISTORICAL_ONLY","owners":OWNERS,"assignments":assignments})
}
#[derive(Default)]
struct MemoryHistory { accepted:u64, full:bool }
impl HistorySink for MemoryHistory {
    fn accept(&mut self,_:&RawFrame,_:&Update)->Result<(),CoreError> {
        if self.full { return Err(CoreError::Invalid("R1323 history pressure")); }
        self.accepted+=1; Ok(())
    }
}
// This identity is ONLY the offline metadata publication capability.
// It does not impersonate Engine authority or replace its SnapshotWitness.
struct MetadataIdentity { generation:u64 }
struct Queued { identity:Arc<MetadataIdentity>, frame:RawFrame, enqueued:Instant }
#[derive(Default)]
struct SpanSummary { samples:usize, sum_ns:u128, max_ns:u128 }
impl SpanSummary {
    fn push(&mut self,ns:u128) {self.samples+=1;self.sum_ns+=ns;self.max_ns=self.max_ns.max(ns);}
    fn len(&self)->usize {self.samples}
    fn value(&self)->Value {json!({"samples":self.samples,"sum_ns":self.sum_ns,"max_ns":self.max_ns})}
}
#[derive(Default)]
struct Accounting {
    offered:u64, accepted:u64, queue_rejected:u64, routed:u64, processed:u64,
    rejected:u64, rejected_after_route:u64, pending:u64,
    reasons:BTreeMap<String,u64>, queue_reasons:BTreeMap<String,u64>, residence_ns:SpanSummary, service_ns:SpanSummary, decision_ns:SpanSummary,
}
impl Accounting {
    fn reject(&mut self,reason:String,routed:bool) {
        self.rejected+=1; if routed {self.rejected_after_route+=1;}
        *self.reasons.entry(reason).or_default()+=1;
    }
    fn report(&self)->Value {
        let residual=self.accepted as i128-self.processed as i128-self.rejected as i128-self.pending as i128;
        json!({"offered":self.offered,"accepted":self.accepted,"queue_rejected":self.queue_rejected,
            "routed":self.routed,"processed":self.processed,"rejected":self.rejected,
            "rejected_after_route":self.rejected_after_route,"pending":self.pending,
            "unexplained_residual":residual,"reasons":self.reasons,"queue_reasons":self.queue_reasons,
            "queue_residence":self.residence_ns.value(),"owner_decision_service":self.service_ns.value(),
            "decision_processing":self.decision_ns.value()})
    }
    fn reconcile(&self) {
        assert_eq!(self.offered,self.accepted+self.queue_rejected);
        assert_eq!(self.accepted,self.processed+self.rejected+self.pending);
        assert_eq!(self.routed,self.processed+self.rejected_after_route);
        assert_eq!(self.residence_ns.len() as u64,self.processed+self.rejected);
        assert_eq!(self.service_ns.len(),self.residence_ns.len());
        assert_eq!(self.reasons.values().sum::<u64>(),self.rejected);
        assert_eq!(self.queue_reasons.values().sum::<u64>(),self.queue_rejected);
    }
}
struct Replay {
    owners:Vec<Owner>, cores:Vec<DecisionCore>, meta:NativeMetadataSnapshot,
    identity:Arc<MetadataIdentity>, visible:bool, staged:Option<NativeMetadataSnapshot>, epoch:u64,
    tx:mpsc::Sender<Queued>, rx:mpsc::Receiver<Queued>, accounting:Accounting, history:MemoryHistory,
}
impl Replay {
    fn new(meta:NativeMetadataSnapshot,capacity:usize)->Self {
        let owners=(0..OWNERS).map(|i|Owner::new(i,OWNERS,800).unwrap()).collect();
        let cores=(0..OWNERS).map(|i| {
            let mut c=DecisionCore::new(i,OWNERS,config(),HashMap::new()).unwrap();
            c.replace_routes(meta.generation,meta.routes.clone()).unwrap();c
        }).collect();
        let(tx,rx)=mpsc::channel(capacity);
        Self{owners,cores,identity:Arc::new(MetadataIdentity{generation:meta.generation}),meta,
            visible:true,staged:None,epoch:1,tx,rx,accounting:Accounting::default(),history:MemoryHistory::default()}
    }
    fn visible_generation(&self)->Option<u64> {
        if !self.visible {return None;}
        let generation=self.identity.generation;
        self.cores.iter().all(|c|c.metadata_generation()==generation&&!c.decisions_paused()).then_some(generation)
    }
    fn submit(&mut self,frame:RawFrame,identity:Arc<MetadataIdentity>)->bool {
        self.accounting.offered+=1;
        match self.tx.try_send(Queued{identity,frame,enqueued:Instant::now()}) {
            Ok(())=>{self.accounting.accepted+=1;self.accounting.pending+=1;true},
            Err(e)=>{
                self.accounting.queue_rejected+=1;
                let reason=match e {mpsc::error::TrySendError::Full(_)=>"QUEUE_FULL",mpsc::error::TrySendError::Closed(_)=>"QUEUE_CLOSED"};
                *self.accounting.queue_reasons.entry(reason.into()).or_default()+=1;false
            }
        }
    }
    fn apply(&mut self,q:&Queued,now:u64,wall:i64)->Result<Vec<Decision>,(String,bool)> {
        if self.visible_generation().is_none() {return Err(("METADATA_BARRIER".into(),false));}
        if !Arc::ptr_eq(&q.identity,&self.identity) {return Err(("STALE_METADATA_IDENTITY".into(),false));}
        let update=parse(&q.frame).map_err(|e|(format!("PARSE:{e:?}"),false))?;
        let base=update.base.clone();
        let Some(m)=self.meta.markets.get(&base) else {return Err(("BASE_REMOVED_OR_UNKNOWN".into(),false));};
        if m.metadata_state()!="SIZE_METADATA_READY" {return Err(("INELIGIBLE_METADATA".into(),false));}
        let expected=if q.frame.venue==Venue::OkxSpot{&m.spot_symbol}else{&m.future_symbol};
        let raw:Value=serde_json::from_str(&q.frame.raw).map_err(|_|("INVALID_JSON".into(),false))?;
        let actual=if q.frame.venue==Venue::OkxSpot{raw["arg"]["instId"].as_str()}else{raw["data"]["s"].as_str()};
        if actual!=Some(expected.as_str()) {return Err(("SYMBOL_METADATA_MISMATCH".into(),false));}
        let index=owner_index(&base,OWNERS);self.accounting.routed+=1;
        self.owners[index].ingest_parsed(&q.frame,update,&mut self.history)
            .map_err(|e|(format!("OWNER:{e:?}"),true))?;
        let book=self.owners[index].snapshot_venue(&base,q.frame.venue).ok_or(("OWNER_NO_BOOK".into(),true))?;
        let exchange=if q.frame.venue==Venue::OkxSpot{&m.buy_exchange}else{&m.sell_exchange};
        let decision_start=Instant::now();
        let result=self.cores[index].update_at(&base,exchange,book,now,wall);
        self.accounting.decision_ns.push(decision_start.elapsed().as_nanos());
        result.map_err(|e|(format!("DECISION:{e:?}"),true))
    }
    fn drain(&mut self,now:u64,wall:i64)->Vec<Decision> {
        let mut rows=Vec::new();
        while let Ok(q)=self.rx.try_recv() {
            let start=Instant::now();self.accounting.residence_ns.push(start.duration_since(q.enqueued).as_nanos());
            self.accounting.pending-=1;
            match self.apply(&q,now,wall) {
                Ok(v)=>{self.accounting.processed+=1;rows.extend(v);},
                Err((why,routed))=>self.accounting.reject(why,routed),
            }
            self.accounting.service_ns.push(start.elapsed().as_nanos());
        }
        self.accounting.reconcile();rows
    }
    fn stage(&mut self,next:NativeMetadataSnapshot)->Result<(),CoreError> {
        if !self.visible||self.staged.is_some(){return Err(CoreError::Invalid("R1323 barrier active"));}
        if next.routes.len()!=next.markets.len() || next.markets.iter().any(|(base,m)| {
            serde_json::to_value(next.routes.get(base)).ok()!=serde_json::to_value(Some(&vec![m.route_spec()])).ok()
        }) {return Err(CoreError::Invalid("R1323 metadata route mismatch"));}
        // Staging may fail: abort staged routes everywhere, retain the published generation.
        for i in 0..OWNERS {
            if let Err(e)=self.cores[i].stage_routes(next.generation,next.routes.clone()) {
                for core in &mut self.cores {core.abort_staged();} return Err(e);
            }
        }
        self.staged=Some(next);Ok(())
    }
    fn pause(&mut self)->Result<(),CoreError> {
        if self.staged.is_none(){return Err(CoreError::Invalid("R1323 nothing staged"));}
        self.visible=false;for c in &mut self.cores {c.pause_decisions();} Ok(())
    }
    fn commit_owner(&mut self,index:usize)->Result<(),CoreError> {
        if self.visible{return Err(CoreError::Invalid("R1323 missing barrier"));}
        let generation=self.staged.as_ref().ok_or(CoreError::Invalid("R1323 nothing staged"))?.generation;
        self.cores[index].commit_staged(generation)
    }
    fn publish(&mut self)->Result<(),CoreError> {
        if self.visible{return Err(CoreError::Invalid("R1323 missing barrier"));}
        let next=self.staged.as_ref().ok_or(CoreError::Invalid("R1323 nothing staged"))?;
        if self.cores.iter().any(|c|c.metadata_generation()!=next.generation||!c.decisions_paused()) {
            return Err(CoreError::Invalid("R1323 incomplete commit"));
        }
        // This offline fixture has one continuous source authority. Explicit resets
        // fence its prior books; epoch increment is NOT authority replacement evidence.
        let epoch=self.epoch.checked_add(1).ok_or(CoreError::Invalid("R1323 epoch overflow"))?;
        let bases:BTreeSet<_>=self.meta.markets.keys().chain(next.markets.keys()).cloned().collect();
        for base in bases {
            let i=owner_index(&base,OWNERS);
            for (venue,exchange) in [(Venue::OkxSpot,"okx_spot"),(Venue::BinanceFutures,"binance_futures")] {
                self.owners[i].reset_venue(&base,venue,epoch)?;
                self.cores[i].reset_exchange(&base,exchange,epoch)?;
            }
        }
        let generation=next.generation;
        for core in &mut self.cores {core.resume_decisions(generation)?;}
        self.meta=self.staged.take().unwrap();self.epoch=epoch;
        self.identity=Arc::new(MetadataIdentity{generation});self.visible=true;Ok(())
    }
    fn switch(&mut self,next:NativeMetadataSnapshot)->Result<(),CoreError> {
        self.stage(next)?;self.pause()?;for i in 0..OWNERS {self.commit_owner(i)?;}self.publish()
    }
}
fn frame(base:&str,venue:Venue,epoch:u64,seq:i64,n:u64,w:i64,edge:bool)->RawFrame {
    let raw=if venue==Venue::OkxSpot {json!({"arg":{"channel":"books5","instId":format!("{base}-USDT")},
        "data":[{"bids":[["99.99","1000"]],"asks":[["100","1000"]],"ts":w.to_string(),"seqId":seq}]})}
    else {json!({"data":{"e":"depthUpdate","s":format!("{base}USDT"),"E":w,"U":seq,"u":seq,"pu":seq-1,
        "b":[[if edge{"102"}else{"100"},"1000"]],"a":[["102.01","1000"]]}})};
    RawFrame{venue,epoch,received_ns:n,received_wall_ms:w,raw:raw.to_string(),source:None}
}
fn send(r:&mut Replay,base:&str,venue:Venue,seq:i64,n:u64,w:i64,edge:bool)->Vec<Decision> {
    assert!(r.submit(frame(base,venue,r.epoch,seq,n,w,edge),r.identity.clone()));r.drain(n,w)
}
#[path="r1323_tests.rs"] mod tests;
