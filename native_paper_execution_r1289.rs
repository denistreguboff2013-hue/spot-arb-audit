//! R1289 isolated PAPER execution authority.
//!
//! This is a reusable offline execution component. It has no exchange transport,
//! credentials, DB1/DB2 handles, or REAL authority. Market data can produce an
//! intent; only explicitly SIMULATED events passed through this owner affect the
//! R1287 lifecycle and derived portfolio projection.
use crate::native_paper_risk_lifecycle_r1287 as risk;
use crate::Book;
use openssl::sha::sha256;
use serde::{Deserialize, Serialize};
use std::{fs::{File, OpenOptions}, io::{Read, Write}, os::fd::AsRawFd, os::unix::fs::OpenOptionsExt, path::{Path, PathBuf}};

const MAX_AGE_NS:u64=800_000_000;
const ZERO_HASH:&str="0000000000000000000000000000000000000000000000000000000000000000";

#[derive(Clone,Debug,Serialize,Deserialize,PartialEq)]
#[serde(deny_unknown_fields)]
pub struct OrderIntent {
    pub intent_id:String, pub order_id:String, pub leg:risk::FillLeg,
    pub side:risk::FillSide, pub quantity:f64, pub instrument:String,
    pub created_wall_ms:i64, pub created_mono_ns:u64, pub exit_reason:Option<risk::ExitReason>,
}
#[derive(Clone,Debug,Serialize,Deserialize,PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SimExecutionEvent {
    pub event_id:String, pub order_id:String, pub kind:SimEventKind,
    pub simulated:bool, pub event_wall_ms:i64, pub event_mono_ns:u64,
    pub quantity:Option<f64>, pub price:Option<f64>, pub reason:Option<String>,
    pub intent:Option<OrderIntent>, pub census_order_ids:Option<Vec<String>>,
}
#[derive(Clone,Debug,Serialize,Deserialize,PartialEq)]
pub enum SimEventKind { Genesis, Accepted, PartialFill, FullFill, Rejected, CancelRequested, Cancelled, Timeout, Unknown, OrderCensus, Recovery }
#[derive(Clone,Debug,Serialize,Deserialize,PartialEq)]
pub struct PortfolioProjection {
    pub state:risk::LifecycleState,
    pub exposure_known:bool,
    pub spot_base:f64, pub futures_contracts:f64, pub futures_base:f64,
    pub residual_base:f64, pub spot_average_price:Option<f64>,
    pub futures_average_price:Option<f64>, pub commissions_quote:f64,
    pub realized_pnl_quote:Option<f64>, pub unrealized_pnl_quote:Option<f64>,
    pub pending_orders:usize, pub unknown_orders:usize,
    pub accounting_basis:String,
}
impl PortfolioProjection {
    pub fn derive(l:&risk::Lifecycle, mark:Option<(&Book,&Book)>) -> Self {
        let mult=l.config.futures_contract_multiplier;
        let exposure=l.outstanding_exposure();
        let exposure_known=exposure.is_some() || l.state==risk::LifecycleState::Flat;
        let last_confirmed=l.ledger.as_ref().map(|x|(x.spot_exposure_base(),x.futures_short_exposure_base(mult))).unwrap_or((0.,0.));
        let (spot,fut)=exposure.unwrap_or(last_confirmed);
        let (spot_qty,spot_quote,fut_qty,fut_quote,fees)=l.ledger.as_ref().map(|x|(
            x.spot_bought_base, x.spot_buy_quote, x.futures_short_contracts,
            x.futures_short_quote, x.fees_quote)).unwrap_or((0.,0.,0.,0.,0.));
        let pending=l.orders.values().filter(|o|!matches!(o.status,risk::OrderStatus::Filled|risk::OrderStatus::Cancelled|risk::OrderStatus::Rejected)).count();
        let unknown=l.orders.values().filter(|o|o.uncertain||o.status==risk::OrderStatus::Unknown).count();
        let closed=l.state==risk::LifecycleState::Closed && spot.is_finite() && fut.is_finite() && spot.abs()<1e-10 && fut.abs()<1e-10;
        let realized=if closed {l.ledger.as_ref().map(|x|x.realized_quote())}else{None};
        let unrealized=mark.and_then(|(s,f)|{
            let ledger=l.ledger.as_ref()?;
            let spot_avg=if ledger.spot_bought_base>0.0{ledger.spot_buy_quote/ledger.spot_bought_base}else{return None};
            let short_base=ledger.futures_short_contracts*mult;
            let fut_avg=if short_base>0.0{ledger.futures_short_quote/short_base}else{return None};
            let spot_exit=crate::native_paper_market_execution_review::sell_base(&s.bids,spot.max(0.)).ok()?;
            let fut_cover=crate::native_paper_market_execution_review::buy_base(&f.asks,fut.max(0.)).ok()?;
            Some(spot_exit-spot_avg*spot.max(0.)+fut_avg*fut.max(0.)-fut_cover-ledger.fees_quote)
        });
        Self {state:l.state,exposure_known,spot_base:spot,futures_contracts:fut/mult,futures_base:fut,
            residual_base:spot-fut,spot_average_price:(spot_qty>0.).then_some(spot_quote/spot_qty),
            futures_average_price:(fut_qty>0.).then_some(fut_quote/(fut_qty*mult)),
            commissions_quote:fees,realized_pnl_quote:realized,unrealized_pnl_quote:unrealized,
            pending_orders:pending,unknown_orders:unknown,
            accounting_basis:"R1287 simulated execution ledger; marks are estimates; funding excluded".into() }
    }
}
#[derive(Clone,Debug,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
struct TxPayload { schema:u32, sequence:u64, transaction_id:String, previous_hash:String,
    event:SimExecutionEvent, lifecycle:risk::Lifecycle, projection:PortfolioProjection,
    #[serde(default,skip_serializing_if="Option::is_none")] universe:Option<UniverseState1324a>,
    #[serde(default,skip_serializing_if="Option::is_none")] isolated:Option<IsolatedState> }
#[derive(Clone,Debug,Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
struct TxLine { payload:TxPayload, checksum:String }
#[derive(Debug)]
pub enum ExecutionError { Io(std::io::Error), UncertainCommit(std::io::Error), OwnerFenced, Invalid(&'static str), Lifecycle(risk::LifecycleError), CorruptJournal }
impl From<std::io::Error> for ExecutionError { fn from(e:std::io::Error)->Self{Self::Io(e)} }
impl From<risk::LifecycleError> for ExecutionError { fn from(e:risk::LifecycleError)->Self{Self::Lifecycle(e)} }
fn hash(bytes:&[u8])->String { sha256(bytes).iter().map(|x|format!("{x:02x}")).collect() }
fn monotonic_now_ns()->Result<u64,ExecutionError>{
    let mut ts=libc::timespec{tv_sec:0,tv_nsec:0};
    let rc=unsafe{libc::clock_gettime(libc::CLOCK_MONOTONIC,&mut ts)};
    if rc!=0||ts.tv_sec<0||ts.tv_nsec<0{return Err(ExecutionError::Invalid("monotonic clock"))}
    (ts.tv_sec as u64).checked_mul(1_000_000_000).and_then(|x|x.checked_add(ts.tv_nsec as u64)).ok_or(ExecutionError::Invalid("monotonic overflow"))
}
fn lock_exclusive(file:&File)->Result<(),ExecutionError>{
    let rc=unsafe{libc::flock(file.as_raw_fd(),libc::LOCK_EX|libc::LOCK_NB)};
    if rc!=0{return Err(ExecutionError::Io(std::io::Error::last_os_error()))}Ok(())
}
fn checksum(p:&TxPayload)->Result<String,ExecutionError>{Ok(hash(&serde_json::to_vec(p).map_err(|_|ExecutionError::Invalid("serialize"))?))}

/// Deterministic conservative marketable simulation. It consumes only visible
/// top-of-book depth at the current snapshot. No touch/queue inference is used.
pub fn simulate_marketable(intent:&OrderIntent,quantity:f64,event_sequence:u64,book:&Book,available:bool,now_ns:u64,now_wall_ms:i64)->Result<Vec<SimExecutionEvent>,ExecutionError>{
    if intent.intent_id.is_empty()||intent.order_id.is_empty()||intent.instrument.is_empty()||!intent.quantity.is_finite()||intent.quantity<=0.0||!quantity.is_finite()||quantity<=0.0||quantity>intent.quantity{return Err(ExecutionError::Invalid("intent"))}
    let mk=|kind,qty,price,reason:&str|SimExecutionEvent{event_id:format!("{}:{}:{}",intent.order_id,match kind {SimEventKind::Accepted=>"ack",SimEventKind::PartialFill|SimEventKind::FullFill=>"fill",SimEventKind::Rejected=>"reject",SimEventKind::Timeout=>"timeout",SimEventKind::Unknown=>"unknown",SimEventKind::Cancelled=>"cancel-ack",SimEventKind::CancelRequested=>"cancel-request",SimEventKind::OrderCensus=>"census",SimEventKind::Recovery=>"recovery",SimEventKind::Genesis=>"genesis"},event_sequence),order_id:intent.order_id.clone(),kind,simulated:true,event_wall_ms:now_wall_ms,event_mono_ns:now_ns,quantity:qty,price,reason:(!reason.is_empty()).then(||reason.into()),intent:None,census_order_ids:None};
    if !available{return Ok(vec![mk(SimEventKind::Timeout,None,None,"market_data_unavailable")])}
    let valid_side=|levels:&[(f64,f64)],bids:bool|!levels.is_empty()&&levels.iter().all(|(p,q)|p.is_finite()&&*p>0.0&&q.is_finite()&&*q>0.0)&&if bids{!levels.windows(2).any(|w|w[0].0<=w[1].0)}else{!levels.windows(2).any(|w|w[0].0>=w[1].0)};
    if !valid_side(&book.bids,true)||!valid_side(&book.asks,false)||book.bids.first().unwrap().0>=book.asks.first().unwrap().0{return Ok(vec![mk(SimEventKind::Rejected,None,None,"invalid_or_crossed_book")])}
    if now_ns<book.received_ns||now_ns-book.received_ns>MAX_AGE_NS||book.exchange_ts_ms.map_or(true,|t|{let age=now_wall_ms.saturating_sub(t);age>800||age< -1_000}){return Ok(vec![mk(SimEventKind::Unknown,None,None,"stale_market_data")])}
    let levels=match (intent.leg,intent.side){(risk::FillLeg::Spot,risk::FillSide::Buy)|(risk::FillLeg::Futures,risk::FillSide::Cover)=>&book.asks,(risk::FillLeg::Spot,risk::FillSide::Sell)|(risk::FillLeg::Futures,risk::FillSide::Short)=>&book.bids,_=>return Err(ExecutionError::Invalid("leg-side"))};
    let mut remain=quantity;let mut done=0.;let mut quote=0.;
    for (p,q) in levels { if !p.is_finite()||*p<=0.||!q.is_finite()||*q<=0.{return Ok(vec![mk(SimEventKind::Rejected,None,None,"invalid_book")])} let n=remain.min(*q);done+=n;quote+=n*p;remain-=n;if remain<=quantity*1e-10{break} }
    if done<=0. {return Ok(vec![mk(SimEventKind::Rejected,None,None,"no_visible_depth")])}
    let mut out=vec![mk(SimEventKind::Accepted,None,None,"visible_depth_model")];
    out.push(mk(if remain>quantity*1e-10{SimEventKind::PartialFill}else{SimEventKind::FullFill},Some(done),Some(quote/done),"visible_depth_v1"));
    Ok(out)
}

pub fn simulated_order_control(order_id:&str,event_sequence:u64,kind:SimEventKind,now_ns:u64,now_wall_ms:i64,reason:&str)->Result<SimExecutionEvent,ExecutionError>{
    if order_id.is_empty()||!matches!(kind,SimEventKind::CancelRequested|SimEventKind::Cancelled|SimEventKind::Rejected|SimEventKind::Timeout|SimEventKind::Unknown){return Err(ExecutionError::Invalid("unsupported control event"))}
    let name=match kind{SimEventKind::CancelRequested=>"cancel-request",SimEventKind::Cancelled=>"cancel-ack",SimEventKind::Rejected=>"reject",SimEventKind::Timeout=>"timeout",SimEventKind::Unknown=>"unknown",_=>unreachable!()};
    Ok(SimExecutionEvent{event_id:format!("{order_id}:{name}:{event_sequence}"),order_id:order_id.into(),kind,simulated:true,event_wall_ms:now_wall_ms,event_mono_ns:now_ns,quantity:None,price:None,reason:Some(reason.into()),intent:None,census_order_ids:None})
}

/// One append-only transaction log contains event, lifecycle snapshot and its
/// projection, so recovery never joins independent PAPER databases.
pub struct DurablePaper { universe:Option<UniverseState1324a>, path:PathBuf, lock:File, lifecycle:risk::Lifecycle, projection:PortfolioProjection, sequence:u64, last_hash:String, fenced:bool, isolated:Option<IsolatedState> }
impl DurablePaper {
    pub fn create(path:impl AsRef<Path>, lifecycle:risk::Lifecycle)->Result<Self,ExecutionError>{
        Self::create_inner(path,lifecycle,None)
    }
    fn create_inner(path:impl AsRef<Path>, lifecycle:risk::Lifecycle, isolated:Option<IsolatedState>)->Result<Self,ExecutionError>{
        let mut lifecycle=lifecycle;lifecycle.order_census_complete=false;
        let p=path.as_ref();let parent=p.parent().ok_or(ExecutionError::Invalid("journal parent"))?;
        std::fs::create_dir_all(parent)?;
        let canonical=parent.canonicalize()?;
        let allowed_labs=[
            Path::new("/mnt/spotarb_db_test/r1273_r1275_engine_latency_20261111/R1289/lab"),
            Path::new("/mnt/spotarb_db_test/r1273_r1275_engine_latency_20261111/R1292/lab"),
            Path::new("/mnt/spotarb_db_test/r1273_r1275_engine_latency_20261111/R1293/lab"),
            Path::new("/mnt/spotarb_db_test/r1273_r1275_engine_latency_20261111/R1296/lab"),
            Path::new("/mnt/spotarb_db_test/r1273_r1275_engine_latency_20261111/R1298/lab"),
            Path::new("/mnt/spotarb_db_test/r1273_r1275_engine_latency_20261111/R1301/lab"),
        ];
        if !allowed_labs.iter().filter_map(|p|p.canonicalize().ok()).any(|lab|canonical.starts_with(lab)){
            return Err(ExecutionError::Invalid("journal outside isolated R1289/R1292 lab"))
        }
        if p.exists(){return Err(ExecutionError::Invalid("journal already exists; recover it"))}
        let projection=PortfolioProjection::derive(&lifecycle,None);
        let f=OpenOptions::new().create_new(true).read(true).append(true).mode(0o600).custom_flags(libc::O_NOFOLLOW).open(p)?; lock_exclusive(&f)?;f.sync_all()?;
        File::open(parent)?.sync_all()?;
        let mut owner=Self{universe:None,path:p.to_path_buf(),lock:f,lifecycle,projection,sequence:0,last_hash:ZERO_HASH.into(),fenced:false,isolated};
        let event=SimExecutionEvent{event_id:"genesis:1".into(),order_id:"genesis".into(),kind:SimEventKind::Genesis,simulated:true,event_wall_ms:0,event_mono_ns:0,quantity:None,price:None,reason:Some("versioned_empty_portfolio_genesis".into()),intent:None,census_order_ids:None};
        owner.commit_event(event,|l,_|{
            if l.state!=risk::LifecycleState::Flat||!l.orders.is_empty()||l.ledger.is_some(){return Err(ExecutionError::Invalid("nonempty genesis"))}
            Ok(())
        })?;
        Ok(owner)
    }
    pub fn recover(path:impl AsRef<Path>)->Result<Self,ExecutionError>{Self::recover_inner(path,None)}
    fn recover_inner(path:impl AsRef<Path>,expected:Option<&hedge::Identity>)->Result<Self,ExecutionError>{Self::recover_mode_r1324a(path,expected,false)}
    fn recover_mode_r1324a(path:impl AsRef<Path>,expected:Option<&hedge::Identity>,multi:bool)->Result<Self,ExecutionError>{
        let p=path.as_ref();
        let parent=p.parent().ok_or(ExecutionError::Invalid("journal parent"))?.canonicalize()?;
        let allowed_labs=[
            Path::new("/mnt/spotarb_db_test/r1273_r1275_engine_latency_20261111/R1289/lab"),
            Path::new("/mnt/spotarb_db_test/r1273_r1275_engine_latency_20261111/R1292/lab"),
            Path::new("/mnt/spotarb_db_test/r1273_r1275_engine_latency_20261111/R1293/lab"),
            Path::new("/mnt/spotarb_db_test/r1273_r1275_engine_latency_20261111/R1296/lab"),
            Path::new("/mnt/spotarb_db_test/r1273_r1275_engine_latency_20261111/R1298/lab"),
            Path::new("/mnt/spotarb_db_test/r1273_r1275_engine_latency_20261111/R1301/lab"),
        ];
        if !allowed_labs.iter().filter_map(|p|p.canonicalize().ok()).any(|lab|parent.starts_with(lab)){
            return Err(ExecutionError::Invalid("journal outside isolated R1289/R1292 lab"))
        }
        let lock=OpenOptions::new().read(true).append(true).custom_flags(libc::O_NOFOLLOW).open(p)?;lock_exclusive(&lock)?;
        let mut bytes=Vec::new();lock.try_clone()?.read_to_end(&mut bytes)?;
        let mut universe=None;let mut isolated:Option<IsolatedState>=None;let mut lifecycle=None;let mut projection=None;let mut seq=0u64;let mut prev=ZERO_HASH.to_string();
        if !bytes.is_empty()&&!bytes.ends_with(b"\n"){return Err(ExecutionError::CorruptJournal)}
        for line in bytes.split(|b|*b==b'\n').filter(|l|!l.is_empty()){
            let tx:TxLine=serde_json::from_slice(line).map_err(|_|ExecutionError::CorruptJournal)?;
            if tx.payload.schema!=if tx.payload.universe.is_some(){4}else if tx.payload.isolated.as_ref().is_some_and(|s|s.full.is_some()){3}else if tx.payload.isolated.is_some(){2}else{1}||tx.payload.sequence!=seq+1||tx.payload.previous_hash!=prev||checksum(&tx.payload)?!=tx.checksum||!tx.payload.event.simulated{return Err(ExecutionError::CorruptJournal)}
            if (seq==0&&tx.payload.event.kind!=SimEventKind::Genesis)||(seq>0&&tx.payload.event.kind==SimEventKind::Genesis){return Err(ExecutionError::CorruptJournal)}
            let derived=PortfolioProjection::derive(&tx.payload.lifecycle,None);
            if serde_json::to_vec(&derived).ok()!=serde_json::to_vec(&tx.payload.projection).ok(){return Err(ExecutionError::CorruptJournal)}
            if let Some(bound)=&tx.payload.isolated{
                bound.validate(&tx.payload.lifecycle)?;
                if !multi&&expected!=Some(&bound.identity){return Err(ExecutionError::Invalid("manifest/run/instrument identity mismatch"))}
                if let Some(previous)=&isolated{if !multi&&previous.identity!=bound.identity{return Err(ExecutionError::CorruptJournal)}}
            }else if expected.is_some()||isolated.is_some(){return Err(ExecutionError::CorruptJournal)}
            if let Some(u)=&tx.payload.universe{
                if !multi{return Err(ExecutionError::Invalid("R1324A_UNIVERSE_REQUIRED"))}
                u.validate()?;
                let active=tx.payload.isolated.as_ref().ok_or(ExecutionError::CorruptJournal)?;
                let slot=u.slots.get(&active.identity.profile.base).ok_or(ExecutionError::CorruptJournal)?;
                if serde_json::to_vec(&(&slot.lifecycle,&slot.isolated)).ok()!=serde_json::to_vec(&(&tx.payload.lifecycle,active)).ok(){return Err(ExecutionError::CorruptJournal)}
            }else if universe.is_some(){return Err(ExecutionError::CorruptJournal)}
            universe=tx.payload.universe;
            isolated=tx.payload.isolated;seq=tx.payload.sequence;prev=tx.checksum.clone();lifecycle=Some(tx.payload.lifecycle);projection=Some(tx.payload.projection);
        }
        let l=lifecycle.ok_or(ExecutionError::CorruptJournal)?;let proj=projection.ok_or(ExecutionError::CorruptJournal)?;
        let mut recovered=Self{universe,path:p.to_path_buf(),lock,lifecycle:l,projection:proj,sequence:seq,last_hash:prev,fenced:false,isolated};
        if multi {recovered.lock.sync_all()?;return Ok(recovered)}
        if !matches!(recovered.lifecycle.state,risk::LifecycleState::Flat|risk::LifecycleState::Closed|risk::LifecycleState::FailedClosed|risk::LifecycleState::RecoveryRequired){
            let now_ns=if let Some(s)=&recovered.isolated{s.last_clock_ns}else{monotonic_now_ns()?};let now_wall_ms=std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_err(|_|ExecutionError::Invalid("wall clock"))?.as_millis() as i64;
            let event=SimExecutionEvent{event_id:format!("recovery:{}",recovered.sequence+1),order_id:format!("recovery:{}",recovered.sequence+1),kind:SimEventKind::Recovery,simulated:true,event_wall_ms:now_wall_ms,event_mono_ns:now_ns,quantity:None,price:None,reason:Some("cold_restart_requires_reconciliation".into()),intent:None,census_order_ids:None};
            recovered.commit_event(event,|l,_|{l.cold_restart(now_ns);Ok(())})?;
        }
        if recovered.isolated.as_ref().is_some_and(|s|s.full.is_some()){recovered.lock.sync_all()?;}
        Ok(recovered)
    }
    /// Mutate a clone, append and fsync the complete transaction, then publish
    /// in-memory state. An error before sync leaves the caller's state unchanged.
    fn commit_event(&mut self,event:SimExecutionEvent, mutate:impl FnOnce(&mut risk::Lifecycle,&SimExecutionEvent)->Result<(),ExecutionError>)->Result<(),ExecutionError>{
        self.commit_transition(event,self.isolated.clone(),mutate)
    }
    fn commit_transition(&mut self,event:SimExecutionEvent,mut isolated:Option<IsolatedState>, mutate:impl FnOnce(&mut risk::Lifecycle,&SimExecutionEvent)->Result<(),ExecutionError>)->Result<(),ExecutionError>{
        if self.fenced{return Err(ExecutionError::OwnerFenced)}
        if !event.simulated||event.event_id.is_empty()||event.order_id.is_empty(){return Err(ExecutionError::Invalid("event identity or SIMULATED marker"))}
        if let Some(s)=isolated.as_mut(){
            if let Some(full)=s.full.as_mut(){full.adapter.observe(&event,full.generation,self.lifecycle.config.fee_bps)?;}
        }
        let mut next=self.lifecycle.clone();let r1325_started=std::time::Instant::now();let r1325_result=mutate(&mut next,&event);
        crate::pipeline_diagnostics_r1325::record("Lifecycle",r1325_started.elapsed().as_nanos().min(u64::MAX as u128) as u64,r1325_result.is_ok());r1325_result?;
        if let Some(s)=&isolated{s.validate(&next)?;}
        let next_projection=PortfolioProjection::derive(&next,None);
        let mut universe=self.universe.clone();
        if let Some(u)=universe.as_mut(){let state=isolated.as_ref().ok_or(ExecutionError::CorruptJournal)?;
            u.slots.insert(state.identity.profile.base.clone(),ExecutionSlot1324a{lifecycle:next.clone(),isolated:state.clone()});u.validate()?;}
        let payload=TxPayload{universe:universe.clone(),schema:if universe.is_some(){4}else if isolated.as_ref().is_some_and(|s|s.full.is_some()){3}else if isolated.is_some(){2}else{1},sequence:self.sequence+1,transaction_id:format!("paper-tx-{}",self.sequence+1),previous_hash:self.last_hash.clone(),event,lifecycle:next.clone(),projection:next_projection.clone(),isolated:isolated.clone()};
        let sum=checksum(&payload)?;let line=serde_json::to_vec(&TxLine{payload,checksum:sum.clone()}).map_err(|_|ExecutionError::Invalid("serialize transaction"))?;
        let mut f=self.lock.try_clone()?;
        if let Err(error)=append_transaction(&mut f,&line,&self.path){
            self.fenced=true;
            return Err(ExecutionError::UncertainCommit(error))
        }
        self.universe=universe;self.sequence+=1;self.last_hash=sum;self.lifecycle=next;self.projection=next_projection;self.isolated=isolated;Ok(())
    }
    pub fn commit_order_intent(&mut self,intent:OrderIntent,spot:&Book,futures:&Book,gate:&risk::MarketGate,now_ns:u64,now_wall_ms:i64)->Result<(),ExecutionError>{
        if self.isolated.is_some(){return Err(ExecutionError::Invalid("bound owner requires isolated registered APIs"))}
        if intent.intent_id.is_empty()||intent.instrument.is_empty()||intent.order_id.is_empty()||!intent.quantity.is_finite()||intent.quantity<=0.0{return Err(ExecutionError::Invalid("intent"))}
        let event=SimExecutionEvent{event_id:format!("{}:accepted",intent.order_id),order_id:intent.order_id.clone(),kind:SimEventKind::Accepted,simulated:true,event_wall_ms:now_wall_ms,event_mono_ns:now_ns,quantity:None,price:None,reason:Some("simulated_order_created_and_accepted".into()),intent:Some(intent.clone()),census_order_ids:None};
        self.commit_event(event,|l,e|{
            if matches!(l.state,risk::LifecycleState::RecoveryRequired|risk::LifecycleState::Closed|risk::LifecycleState::FailedClosed){return Err(ExecutionError::Invalid("recovery or terminal state blocks new order"))}
            if l.orders.values().any(|o|o.leg==intent.leg&&o.side==intent.side&&!matches!(o.status,risk::OrderStatus::Filled|risk::OrderStatus::Cancelled|risk::OrderStatus::Rejected)){return Err(ExecutionError::Invalid("same-leg obligation already pending"))}
            match l.state {
                risk::LifecycleState::Flat=>{
                    if intent.leg!=risk::FillLeg::Spot||intent.side!=risk::FillSide::Buy{return Err(ExecutionError::Invalid("first order must start spot entry"))}
                    l.begin_entry(spot,futures,gate,intent.quantity,now_ns,now_wall_ms)?;
                },
                risk::LifecycleState::AwaitingHedge|risk::LifecycleState::PartiallyFilled=>{
                    let expected=l.authorize_hedge(spot,futures,gate,now_ns,now_wall_ms)?;
                    match expected { risk::LifecycleAction::SimulatedHedgeIntent{futures_short_contracts,..} if intent.leg==risk::FillLeg::Futures&&intent.side==risk::FillSide::Short&&intent.quantity<=futures_short_contracts+1e-10=>{}, _=>return Err(ExecutionError::Invalid("order does not match authorized hedge")) }
                },
                risk::LifecycleState::Hedged=>{
                    let _=l.request_exit(spot,futures,gate,intent.exit_reason.ok_or(ExecutionError::Invalid("exit reason required"))?,now_ns,now_wall_ms)?;
                    let (s,f)=l.outstanding_exposure().ok_or(ExecutionError::Invalid("unknown exposure"))?;
                    let allowed=(intent.leg==risk::FillLeg::Spot&&intent.side==risk::FillSide::Sell&&intent.quantity<=s+1e-10)||
                        (intent.leg==risk::FillLeg::Futures&&intent.side==risk::FillSide::Cover&&intent.quantity<=f/l.config.futures_contract_multiplier+1e-10);
                    if !allowed{return Err(ExecutionError::Invalid("order is not a bounded risk-reducing exit"))}
                },
                risk::LifecycleState::ExitPending|risk::LifecycleState::ExitPartiallyFilled=>{
                    let (s,f)=l.outstanding_exposure().ok_or(ExecutionError::Invalid("unknown exposure"))?;
                    let allowed=(intent.leg==risk::FillLeg::Spot&&intent.side==risk::FillSide::Sell&&intent.quantity<=s+1e-10)||
                        (intent.leg==risk::FillLeg::Futures&&intent.side==risk::FillSide::Cover&&intent.quantity<=f/l.config.futures_contract_multiplier+1e-10);
                    if !allowed{return Err(ExecutionError::Invalid("order is not risk reducing"))}
                },
                _=>return Err(ExecutionError::Invalid("lifecycle state blocks order")),
            }
            l.register_simulated_order(&intent.order_id,intent.leg,intent.side,intent.quantity,now_ns)?;
            l.apply_order_event(risk::OrderEvent{event_id:e.event_id.clone(),order_id:intent.order_id.clone(),kind:risk::OrderEventKind::Accepted,event_ns:now_ns,simulated:true})?;
            Ok(())
        })
    }
    pub fn commit_execution(&mut self,event:SimExecutionEvent,intent:&OrderIntent,contract_multiplier:f64,sequence:u64)->Result<(),ExecutionError>{
        if self.isolated.is_some(){return Err(ExecutionError::Invalid("bound owner requires isolated registered APIs"))}
        if event.order_id!=intent.order_id||!event.simulated{return Err(ExecutionError::Invalid("execution/order identity"))}
        self.commit_event(event.clone(),|l,e|{
            match e.kind {
                SimEventKind::PartialFill|SimEventKind::FullFill=>{
                    let q=e.quantity.ok_or(ExecutionError::Invalid("fill quantity absent"))?;
                    let p=e.price.ok_or(ExecutionError::Invalid("fill price absent"))?;
                    let status=if e.kind==SimEventKind::PartialFill{risk::FillStatus::Partial}else{risk::FillStatus::Complete};
                    l.apply_fill(risk::FillEvent{event_id:e.event_id.clone(),order_id:e.order_id.clone(),leg:intent.leg,side:intent.side,quantity:q,price:p,contract_multiplier,sequence,event_ns:e.event_mono_ns,status,simulated:true})?;
                },
                SimEventKind::Rejected=>{l.apply_order_event(risk::OrderEvent{event_id:e.event_id.clone(),order_id:e.order_id.clone(),kind:risk::OrderEventKind::Rejected,event_ns:e.event_mono_ns,simulated:true})?;},
                SimEventKind::CancelRequested=>{l.apply_order_event(risk::OrderEvent{event_id:e.event_id.clone(),order_id:e.order_id.clone(),kind:risk::OrderEventKind::CancelRequested,event_ns:e.event_mono_ns,simulated:true})?;},
                SimEventKind::Cancelled=>{l.apply_order_event(risk::OrderEvent{event_id:e.event_id.clone(),order_id:e.order_id.clone(),kind:risk::OrderEventKind::Cancelled,event_ns:e.event_mono_ns,simulated:true})?;},
                SimEventKind::Unknown|SimEventKind::Timeout=>{l.apply_order_event(risk::OrderEvent{event_id:e.event_id.clone(),order_id:e.order_id.clone(),kind:risk::OrderEventKind::Unknown,event_ns:e.event_mono_ns,simulated:true})?;},
                SimEventKind::Accepted|SimEventKind::OrderCensus|SimEventKind::Recovery|SimEventKind::Genesis=>return Err(ExecutionError::Invalid("event must use its dedicated API")),
            }
            Ok(())
        })
    }
    pub fn commit_census(&mut self,mut order_ids:Vec<String>,now_ns:u64,now_wall_ms:i64)->Result<(),ExecutionError>{
        if self.isolated.is_some(){return Err(ExecutionError::Invalid("bound owner requires isolated registered APIs"))}
        order_ids.sort();
        if order_ids.windows(2).any(|w|w[0]==w[1]){return Err(ExecutionError::Invalid("duplicate census order id"))}
        let event=SimExecutionEvent{event_id:format!("census:{}",self.sequence+1),order_id:format!("census:{}",self.sequence+1),kind:SimEventKind::OrderCensus,simulated:true,event_wall_ms:now_wall_ms,event_mono_ns:now_ns,quantity:None,price:None,reason:Some("complete_simulated_order_census".into()),intent:None,census_order_ids:Some(order_ids.clone())};
        self.commit_event(event,|l,e|{
            let expected=l.orders.keys().cloned().collect::<Vec<_>>();
            if expected!=order_ids||l.orders.values().any(|o|!matches!(o.status,risk::OrderStatus::Filled|risk::OrderStatus::Cancelled|risk::OrderStatus::Rejected)||o.uncertain){return Err(ExecutionError::Invalid("census incomplete or contradicts ledger"))}
            l.confirm_order_census_complete(e.event_mono_ns)?;Ok(())
        })
    }
    pub fn sequence(&self)->u64{self.sequence}
    /// Read and checksum-verify durable owner transactions after a cursor for canonical mirroring.
    pub fn r1326_records_after(&self,after:u64)->Result<Vec<(u64,serde_json::Value)>,ExecutionError>{
        let size=std::fs::metadata(&self.path)?.len();if size>64*1024*1024{return Err(ExecutionError::Invalid("R1326 execution journal exceeds mirror bound"))}
        let mut file=File::open(&self.path)?;let mut raw=String::new();file.read_to_string(&mut raw)?;
        if !raw.is_empty()&&!raw.ends_with('\n'){return Err(ExecutionError::CorruptJournal)}
        let mut output=Vec::new();let mut expected=1u64;let mut previous=ZERO_HASH.to_string();
        for line in raw.lines(){
            let entry:TxLine=serde_json::from_str(line).map_err(|_|ExecutionError::CorruptJournal)?;
            if entry.payload.sequence!=expected||entry.payload.previous_hash!=previous||
               checksum(&entry.payload)?!=entry.checksum{return Err(ExecutionError::CorruptJournal)}
            previous=entry.checksum.clone();
            if expected>after{output.push((expected,serde_json::to_value(&entry.payload).map_err(|_|ExecutionError::CorruptJournal)?));}
            expected=expected.checked_add(1).ok_or(ExecutionError::CorruptJournal)?;
        }
        if expected!=self.sequence.checked_add(1).ok_or(ExecutionError::CorruptJournal)?||
           previous!=self.last_hash{return Err(ExecutionError::CorruptJournal)}
        Ok(output)
    }
    pub fn mark_to_market(&self,spot:&Book,futures:&Book,now_ns:u64,now_wall_ms:i64)->Result<PortfolioProjection,ExecutionError>{
        for b in [spot,futures]{if now_ns<b.received_ns||now_ns-b.received_ns>MAX_AGE_NS||b.exchange_ts_ms.map_or(true,|t|{let age=now_wall_ms.saturating_sub(t);age>800||age< -1_000}){return Err(ExecutionError::Invalid("stale mark; exposure unchanged"))}}
        Ok(PortfolioProjection::derive(&self.lifecycle,Some((spot,futures))))
    }
    pub fn journal_path(&self)->&Path{&self.path}
    pub fn lifecycle(&self)->&risk::Lifecycle{&self.lifecycle}
    pub fn projection(&self)->&PortfolioProjection{&self.projection}
    pub fn is_fenced(&self)->bool{self.fenced}
}

// Production keeps the existing append/newline/fsync protocol. Fault controls
// exist only in unit-test builds and are scoped to both thread and journal path.
fn append_transaction(f:&mut File,line:&[u8],_path:&Path)->std::io::Result<()>{
    #[cfg(test)] storage_eio::check(_path,storage_eio::Point::Append)?;
    let r1325_started=std::time::Instant::now();let r1325_result=f.write_all(line).and_then(|_|f.write_all(b"\n"));
    crate::pipeline_diagnostics_r1325::record("JournalAppend",r1325_started.elapsed().as_nanos().min(u64::MAX as u128) as u64,r1325_result.is_ok());r1325_result?;
    #[cfg(test)] storage_eio::check(_path,storage_eio::Point::Sync)?;
    let r1325_started=std::time::Instant::now();let result=f.sync_all();
    crate::pipeline_diagnostics_r1325::record("JournalSync",r1325_started.elapsed().as_nanos().min(u64::MAX as u128) as u64,result.is_ok());result
}
#[cfg(test)]
mod storage_eio {
    use super::*;
    #[derive(Clone,Copy,PartialEq)] pub(super) enum Point {Append,Sync}
    thread_local! {static FAULT:std::cell::RefCell<Option<(PathBuf,Point)>>=const {std::cell::RefCell::new(None)};}
    thread_local! {static PAUSE:std::cell::RefCell<Option<Box<dyn FnOnce()>>>=std::cell::RefCell::new(None);}
    pub(super) fn pause_before_error(h:impl FnOnce()+'static){PAUSE.with(|v|{assert!(v.borrow().is_none());*v.borrow_mut()=Some(Box::new(h));});}
    pub(super) struct Guard;
    impl Drop for Guard {fn drop(&mut self){FAULT.with(|v|*v.borrow_mut()=None);PAUSE.with(|v|*v.borrow_mut()=None);}}
    pub(super) fn arm(path:&Path,point:Point)->Guard{
        FAULT.with(|v|{assert!(v.borrow().is_none());*v.borrow_mut()=Some((path.to_path_buf(),point));});Guard
    }
    pub(super) fn check(path:&Path,point:Point)->std::io::Result<()>{
        FAULT.with(|v|{
            let mut v=v.borrow_mut();
            if v.as_ref().is_some_and(|(p,s)|p==path&&*s==point){
                PAUSE.with(|h|{if let Some(h)=h.borrow_mut().take(){h()}});
                *v=None;Err(std::io::Error::from_raw_os_error(libc::EIO))
            }else{Ok(())}
        })
    }
}

#[cfg(test)] mod tests {
 use super::*; use risk::*; use std::time::{SystemTime,UNIX_EPOCH};
 fn path(tag:&str)->PathBuf{let n=SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();PathBuf::from(format!("/mnt/spotarb_db_test/r1273_r1275_engine_latency_20261111/R1289/lab/{tag}-{n}.jsonl"))}
 fn lifecycle()->Lifecycle{Lifecycle::new(RiskConfig{hedge_timeout_ns:2_000_000_000,stop_loss_bps:-100.,max_hold_ns:900_000_000_000,futures_contract_multiplier:1.,fee_bps:5.}).unwrap()}
 include!("native_paper_execution_r1306_tests.rs");
 #[test] fn r1304_storage_eio_fences_without_publication_and_recovers(){
  for point in [storage_eio::Point::Append,storage_eio::Point::Sync]{
   let p=path("r1304-eio");
   let mut d=DurablePaper::create(&p,lifecycle()).unwrap();
   let seq=d.sequence();let before=std::fs::metadata(&p).unwrap().len();
   let event=SimExecutionEvent{event_id:"eio-probe".into(),order_id:"eio-probe".into(),kind:SimEventKind::OrderCensus,simulated:true,event_wall_ms:1000,event_mono_ns:1,quantity:None,price:None,reason:None,intent:None,census_order_ids:None};
   let guard=storage_eio::arm(&p,point);
   assert!(matches!(d.commit_event(event.clone(),|_,_|Ok(())),Err(ExecutionError::UncertainCommit(_))));
   assert!(d.is_fenced());assert_eq!(d.sequence(),seq);
   let after=std::fs::metadata(&p).unwrap().len();
   assert!(matches!(d.commit_event(event,|_,_|Ok(())),Err(ExecutionError::OwnerFenced)));
   assert_eq!(std::fs::metadata(&p).unwrap().len(),after);
   if point==storage_eio::Point::Append{assert_eq!(before,after)}else{assert!(after>before)}
   drop(guard);drop(d);
   let recovered=DurablePaper::recover(&p).unwrap();
   assert_eq!(recovered.sequence(),seq+if point==storage_eio::Point::Sync{1}else{0});
   assert_eq!(recovered.lifecycle().state,LifecycleState::Flat);
   drop(recovered);std::fs::remove_file(p).unwrap();
  }
 }
 #[test] fn r1326_record_reader_rejects_rehashed_previous_hash_break_and_tail(){
  let p=path("r1326-chain");let d=DurablePaper::create(&p,lifecycle()).unwrap();
  assert_eq!(d.r1326_records_after(0).unwrap().len(),1);
  let original=std::fs::read_to_string(&p).unwrap();let mut tx:TxLine=serde_json::from_str(original.trim_end()).unwrap();
  tx.payload.previous_hash="f".repeat(64);tx.checksum=checksum(&tx.payload).unwrap();
  std::fs::write(&p,serde_json::to_string(&tx).unwrap()+"\n").unwrap();
  assert!(matches!(d.r1326_records_after(0),Err(ExecutionError::CorruptJournal)));
  std::fs::write(&p,original.trim_end().as_bytes()).unwrap();
  assert!(matches!(d.r1326_records_after(0),Err(ExecutionError::CorruptJournal)));
  drop(d);std::fs::remove_file(p).unwrap();
 }
 #[test] fn entry_intent_adapter_fill_and_recovery_use_one_lifecycle(){
  let p=path("entry-replay");let mut d=DurablePaper::create(&p,lifecycle()).unwrap();
  let spot=Book{bids:vec![(99.,2.)],asks:vec![(100.,2.)],received_ns:1,received_wall_ms:1000,exchange_ts_ms:Some(1000),changed_ns:1,epoch:1,sequence:Some(1)};
  let fut=Book{bids:vec![(101.,2.)],asks:vec![(102.,2.)],received_ns:1,received_wall_ms:1000,exchange_ts_ms:Some(1000),changed_ns:1,epoch:1,sequence:Some(1)};
  let gate=MarketGate{spot_hot:true,futures_hot:true,spot_book_valid:true,futures_book_valid:true};
  let i=OrderIntent{intent_id:"intent-1".into(),order_id:"spot-order-1".into(),leg:FillLeg::Spot,side:FillSide::Buy,quantity:1.,instrument:"XUSDT".into(),created_wall_ms:1000,created_mono_ns:1,exit_reason:None};
  d.commit_order_intent(i.clone(),&spot,&fut,&gate,1,1000).unwrap();
  let simulated=simulate_marketable(&i,1.,1,&spot,true,2,1000).unwrap();
  assert_eq!(simulated[1].kind,SimEventKind::FullFill);
  d.commit_execution(simulated[1].clone(),&i,1.,1).unwrap();
  assert_eq!(d.projection.spot_base,1.);assert_eq!(d.lifecycle.state,LifecycleState::AwaitingHedge);
  assert!(d.commit_order_intent(i.clone(),&spot,&fut,&gate,3,1000).is_err());
  assert!(DurablePaper::recover(&p).is_err());
  drop(d);let recovered=DurablePaper::recover(&p).unwrap();assert_eq!(recovered.sequence(),4);assert_eq!(recovered.lifecycle.state,LifecycleState::RecoveryRequired);assert_eq!(recovered.projection.spot_base,1.);assert!(!recovered.projection.exposure_known);assert_eq!(recovered.lifecycle.processed_events.len(),1);
  std::fs::remove_file(p).unwrap();
 }
 #[test] fn full_entry_hedge_exit_requires_census_and_recovers_closed(){
  let p=path("roundtrip");let mut d=DurablePaper::create(&p,lifecycle()).unwrap();
  let spot=Book{bids:vec![(99.,2.)],asks:vec![(100.,2.)],received_ns:1,received_wall_ms:1000,exchange_ts_ms:Some(1000),changed_ns:1,epoch:1,sequence:Some(1)};
  let fut=Book{bids:vec![(101.,2.)],asks:vec![(102.,2.)],received_ns:1,received_wall_ms:1000,exchange_ts_ms:Some(1000),changed_ns:1,epoch:1,sequence:Some(1)};
  let gate=MarketGate{spot_hot:true,futures_hot:true,spot_book_valid:true,futures_book_valid:true};
  let intents=vec![
   OrderIntent{intent_id:"i1".into(),order_id:"o1".into(),leg:FillLeg::Spot,side:FillSide::Buy,quantity:1.,instrument:"XUSDT".into(),created_wall_ms:1000,created_mono_ns:1,exit_reason:None},
   OrderIntent{intent_id:"i2".into(),order_id:"o2".into(),leg:FillLeg::Futures,side:FillSide::Short,quantity:1.,instrument:"XUSDT-PERP".into(),created_wall_ms:1000,created_mono_ns:3,exit_reason:None},
   OrderIntent{intent_id:"i3".into(),order_id:"o3".into(),leg:FillLeg::Spot,side:FillSide::Sell,quantity:1.,instrument:"XUSDT".into(),created_wall_ms:1000,created_mono_ns:5,exit_reason:Some(ExitReason::ManualRecovery)},
   OrderIntent{intent_id:"i4".into(),order_id:"o4".into(),leg:FillLeg::Futures,side:FillSide::Cover,quantity:1.,instrument:"XUSDT-PERP".into(),created_wall_ms:1000,created_mono_ns:7,exit_reason:Some(ExitReason::ManualRecovery)},
  ];
  let books=[&spot,&fut,&spot,&fut];
  for (n,(i,b)) in intents.iter().zip(books).enumerate(){
   let now=1+n as u64*2;d.commit_order_intent(i.clone(),&spot,&fut,&gate,now,1000).unwrap();
   let evs=simulate_marketable(i,1.,1,b,true,now+1,1000).unwrap();assert_eq!(evs[1].kind,SimEventKind::FullFill);
   d.commit_execution(evs[1].clone(),i,1.,1+n as u64/2).unwrap();
   if n==2{assert_ne!(d.lifecycle.state,LifecycleState::Closed);assert_eq!(d.projection.realized_pnl_quote,None);}
  }
  assert_ne!(d.lifecycle.state,LifecycleState::Closed);
  d.commit_census(vec!["o1".into(),"o2".into(),"o3".into(),"o4".into()],10,1000).unwrap();
  assert_eq!(d.lifecycle.state,LifecycleState::Closed);assert_eq!(d.projection.realized_pnl_quote,Some(d.lifecycle.ledger.as_ref().unwrap().realized_quote()));
  drop(d);let recovered=DurablePaper::recover(&p).unwrap();assert_eq!(recovered.lifecycle.state,LifecycleState::Closed);assert_eq!(recovered.projection.spot_base,0.);assert_eq!(recovered.projection.futures_base,0.);
  std::fs::remove_file(p).unwrap();
 }
 #[test] fn offline_durable_latency_sample(){
  use std::time::Instant;
  let p=path("latency");let mut d=DurablePaper::create(&p,lifecycle()).unwrap();
  let spot=Book{bids:vec![(99.,2.)],asks:vec![(100.,2.)],received_ns:1,received_wall_ms:1000,exchange_ts_ms:Some(1000),changed_ns:1,epoch:1,sequence:Some(1)};
  let fut=Book{bids:vec![(101.,2.)],asks:vec![(102.,2.)],received_ns:1,received_wall_ms:1000,exchange_ts_ms:Some(1000),changed_ns:1,epoch:1,sequence:Some(1)};
  let gate=MarketGate{spot_hot:true,futures_hot:true,spot_book_valid:true,futures_book_valid:true};
  let i=OrderIntent{intent_id:"latency-i".into(),order_id:"latency-o".into(),leg:FillLeg::Spot,side:FillSide::Buy,quantity:1.,instrument:"XUSDT".into(),created_wall_ms:1000,created_mono_ns:1,exit_reason:None};
  let t=Instant::now();d.commit_order_intent(i.clone(),&spot,&fut,&gate,1,1000).unwrap();let create_ns=t.elapsed().as_nanos();
  let fill=simulate_marketable(&i,1.,1,&spot,true,2,1000).unwrap().remove(1);
  let mut samples=Vec::new();
  for _ in 0..51{let t=Instant::now();d.commit_execution(fill.clone(),&i,1.,1).unwrap();samples.push(t.elapsed().as_nanos());}
  samples.sort_unstable();let percentile=|p:f64|samples[((samples.len() as f64*p).ceil() as usize).saturating_sub(1).min(samples.len()-1)];
  let size=std::fs::metadata(&p).unwrap().len();drop(d);let t=Instant::now();let recovered=DurablePaper::recover(&p).unwrap();let replay_ns=t.elapsed().as_nanos();
  assert_eq!(recovered.lifecycle.processed_events.len(),1);assert_eq!(recovered.projection.spot_base,1.);
  println!("R1289_METRIC order_create_ns={} event_commit_p50_ns={} event_commit_p95_ns={} event_commit_p99_ns={} recovery_replay_ns={} event_commit_throughput_per_sec={:.2} journal_bytes={} fsync_count=54 max_rss_kib=unmeasured cpu_time_ns=unmeasured",create_ns,percentile(0.50),percentile(0.95),percentile(0.99),replay_ns,51_000_000_000f64/(samples.iter().sum::<u128>() as f64),size);
  std::fs::remove_file(p).unwrap();
 }
 #[test] fn adapter_requires_explicit_available_fresh_depth(){let i=OrderIntent{intent_id:"i".into(),order_id:"o".into(),leg:FillLeg::Spot,side:FillSide::Buy,quantity:2.,instrument:"XUSDT".into(),created_wall_ms:1000,created_mono_ns:1,exit_reason:None};let b=Book{bids:vec![(99.,2.)],asks:vec![(100.,1.)],received_ns:1,received_wall_ms:1000,exchange_ts_ms:Some(1000),changed_ns:1,epoch:1,sequence:Some(1)};let e=simulate_marketable(&i,2.,7,&b,true,1,1000).unwrap();assert_eq!(e.len(),2);assert_eq!(e[1].kind,SimEventKind::PartialFill);assert!(e.iter().all(|x|x.simulated));}
 #[test] fn malformed_tail_fails_closed(){let p=path("corrupt");let d=DurablePaper::create(&p,lifecycle()).unwrap();std::fs::OpenOptions::new().append(true).open(&p).unwrap().write_all(b"{truncated").unwrap();drop(d);assert!(matches!(DurablePaper::recover(&p),Err(ExecutionError::CorruptJournal)));std::fs::remove_file(p).unwrap();}
}

include!("native_paper_execution_r1296.rs");

include!("native_paper_execution_r1298.rs");

/// Observation receipt from the existing owner; no journal, lifecycle or order mutation.
impl DurablePaper {
 pub fn observe_public_input_r1301a(&self,input_id:&str,base:&str)->Result<serde_json::Value,ExecutionError>{
  if self.fenced{return Err(ExecutionError::OwnerFenced)}
  let isolated=self.isolated.as_ref().ok_or(ExecutionError::Invalid("isolated observation only"))?;
  if input_id.is_empty()||base!=isolated.identity.profile.base{return Err(ExecutionError::Invalid("observation identity"))}
  Ok(serde_json::json!({"stage":"owner_observation","owner":"R1289","input_id":input_id,
   "owner_sequence":self.sequence,"orders":self.lifecycle.orders.len(),"order_admission":false}))
 }
}

// R1324A completion is appended by the SAME execution owner after all effects.
impl DurablePaper {
 pub fn complete_decision_r1324a(&mut self,boundary:crate::native_paper_reconciliation_r1324a::DecisionBoundary,result_hash:String)->Result<(),ExecutionError>{
  if self.fenced{return Err(ExecutionError::OwnerFenced)}
  if !self.exposure_view().known{return Err(ExecutionError::Invalid("R1324A_UNKNOWN_EXPOSURE"))}
  if boundary.generation==0||result_hash.len()!=64||!result_hash.bytes().all(|b|b.is_ascii_hexdigit()&&!b.is_ascii_uppercase()) {return Err(ExecutionError::Invalid("R1324A_INVALID_COMPLETION"))}
  let identity=self.isolated_identity().ok_or(ExecutionError::Invalid("R1324A_UNBOUND_OWNER"))?;
  if boundary.symbol!=identity.profile.base||boundary.epoch==0||self.universe_generation_r1324a().is_some_and(|g|g!=boundary.generation){return Err(ExecutionError::Invalid("R1324A_COMPLETION_SCOPE_MISMATCH"))}
  let receipt=crate::native_paper_reconciliation_r1324a::Completion{schema:1,boundary,
   execution_sequence:self.sequence.checked_add(1).ok_or(ExecutionError::Invalid("sequence overflow"))?,execution_previous_hash:self.last_hash.clone(),
   identity_hash:identity.hash().map_err(|_|ExecutionError::Invalid("identity"))?,
   state_hash:hash(&serde_json::to_vec(&(&self.lifecycle,&self.projection)).map_err(|_|ExecutionError::Invalid("state"))?),result_hash};
  let reason=serde_json::to_string(&receipt).map_err(|_|ExecutionError::Invalid("completion serialization"))?;
  let event=SimExecutionEvent{event_id:format!("r1324a-completion:{}",receipt.execution_sequence),order_id:"owner".into(),kind:SimEventKind::Recovery,
   simulated:true,event_wall_ms:0,event_mono_ns:self.isolated.as_ref().map_or(0,|s|s.last_clock_ns),quantity:None,price:None,reason:Some(reason),intent:None,census_order_ids:None};
  self.commit_event(event,|_,_|Ok(()))
 }
 /// Recovered owner's locked journal and verified tail are the only authority.
 pub fn completion_r1324a(&self)->Result<crate::native_paper_reconciliation_r1324a::Completion,ExecutionError>{
  if self.fenced{return Err(ExecutionError::OwnerFenced)}
  if !self.exposure_view().known{return Err(ExecutionError::Invalid("R1324A_UNKNOWN_EXPOSURE"))}
  let bytes=std::fs::read(&self.path)?;
  if !bytes.ends_with(b"\n"){return Err(ExecutionError::CorruptJournal)}
  let line=bytes.split(|b|*b==b'\n').rev().find(|b|!b.is_empty()).ok_or(ExecutionError::CorruptJournal)?;
  let tx:TxLine=serde_json::from_slice(line).map_err(|_|ExecutionError::CorruptJournal)?;
  if checksum(&tx.payload)?!=tx.checksum||tx.checksum!=self.last_hash||tx.payload.sequence!=self.sequence{return Err(ExecutionError::CorruptJournal)}
  if tx.payload.event.kind!=SimEventKind::Recovery||tx.payload.event.event_id!=format!("r1324a-completion:{}",self.sequence){return Err(ExecutionError::Invalid("R1324A_COMPLETION_MISSING_OR_NOT_TAIL"))}
  let r:crate::native_paper_reconciliation_r1324a::Completion=serde_json::from_str(tx.payload.event.reason.as_deref().ok_or(ExecutionError::CorruptJournal)?).map_err(|_|ExecutionError::CorruptJournal)?;
  let identity=self.isolated_identity().ok_or(ExecutionError::CorruptJournal)?.hash().map_err(|_|ExecutionError::CorruptJournal)?;
  let state=hash(&serde_json::to_vec(&(&self.lifecycle,&self.projection)).map_err(|_|ExecutionError::CorruptJournal)?);
  if r.schema!=1||r.execution_sequence!=self.sequence||r.execution_previous_hash!=tx.payload.previous_hash||r.identity_hash!=identity||r.state_hash!=state{return Err(ExecutionError::Invalid("R1324A_COMPLETION_STATE_MISMATCH"))}
  Ok(r)
 }
}

include!("native_paper_execution_universe_r1324a.rs");

impl DurablePaper {
 pub fn completion_history_r1324a(&self)->Result<crate::native_paper_reconciliation_r1324a::CompletionHistory,ExecutionError>{
  use std::io::{BufRead,BufReader,Seek,SeekFrom};
  use crate::native_paper_reconciliation_r1324a::{Completion,CompletionHistory,CanonicalReceipt};
  if self.fenced{return Err(ExecutionError::OwnerFenced)}
  let mut file=self.lock.try_clone()?;file.seek(SeekFrom::Start(0))?;
  let mut reader=BufReader::new(file);let mut line=Vec::new();let mut sequence=0;let mut previous=ZERO_HASH.to_string();
  let mut h=CompletionHistory{count:0,chain:ZERO_HASH.into(),prior_chain:ZERO_HASH.into(),last:None,uncompleted_effects:false};
  loop {
   line.clear();if reader.read_until(b'\n',&mut line)?==0{break}
   if !line.ends_with(b"\n"){return Err(ExecutionError::CorruptJournal)}
   let tx:TxLine=serde_json::from_slice(&line).map_err(|_|ExecutionError::CorruptJournal)?;
   if tx.payload.sequence!=sequence+1||tx.payload.previous_hash!=previous||checksum(&tx.payload)?!=tx.checksum{return Err(ExecutionError::CorruptJournal)}
   sequence=tx.payload.sequence;previous=tx.checksum;
   let event=&tx.payload.event;
   if event.event_id==format!("r1324a-completion:{sequence}"){
    if event.kind!=SimEventKind::Recovery{return Err(ExecutionError::CorruptJournal)}
    let r:Completion=serde_json::from_str(event.reason.as_deref().ok_or(ExecutionError::CorruptJournal)?).map_err(|_|ExecutionError::CorruptJournal)?;
    let id=&tx.payload.isolated.as_ref().ok_or(ExecutionError::CorruptJournal)?.identity;
    let state=hash(&serde_json::to_vec(&(&tx.payload.lifecycle,&tx.payload.projection)).map_err(|_|ExecutionError::CorruptJournal)?);
    if r.schema!=1||r.execution_sequence!=sequence||r.execution_previous_hash!=tx.payload.previous_hash||r.state_hash!=state||r.identity_hash!=id.hash().map_err(|_|ExecutionError::CorruptJournal)?||r.boundary.symbol!=id.profile.base||r.boundary.generation==0||tx.payload.universe.as_ref().is_some_and(|u|u.generation!=r.boundary.generation){return Err(ExecutionError::Invalid("R1324A_COMPLETION_HISTORY_MISMATCH"))}
    if h.last.as_ref().is_some_and(|old|old.boundary.canonical!=r.boundary.canonical||old.boundary.source_sequence>=r.boundary.source_sequence||old.boundary.generation>r.boundary.generation){return Err(ExecutionError::Invalid("R1324A_COMPLETION_ORDER_MISMATCH"))}
    let proof=serde_json::to_vec(&[CanonicalReceipt{input_hash:&r.boundary.input_hash,outcome:"EVALUATED",result_hash:&r.result_hash}]).map_err(|_|ExecutionError::CorruptJournal)?;
    h.prior_chain=h.chain.clone();h.chain=hash((h.chain+&hash(&proof)).as_bytes());h.count=h.count.checked_add(1).ok_or(ExecutionError::CorruptJournal)?;h.last=Some(r);h.uncompleted_effects=false;
   }else if event.kind!=SimEventKind::Genesis&&!event.event_id.starts_with("r1324a-universe:"){h.uncompleted_effects=true;}
  }
  if sequence!=self.sequence||previous!=self.last_hash{return Err(ExecutionError::CorruptJournal)}Ok(h)
 }
}
