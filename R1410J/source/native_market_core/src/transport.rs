//! Review-only public/loopback WS transport. No credentials or order API. Optional mandatory canonical-history sink.
use crate::{decision::{DecisionConfig, DecisionCore, RouteSpec}, parse_json, CoreError,
    Owner, RawFrame, Update, Venue};
use crate::history::{HistoryClient,DurableSignal,PreparedSignal,Frontier};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{collections::{HashMap, HashSet, VecDeque}, io::{self, BufRead, Write},
    sync::{Arc, Mutex, RwLock, atomic::{AtomicBool, AtomicU64, Ordering}, mpsc},
    time::Duration};
use tokio_tungstenite::{connect_async_tls_with_config, Connector, tungstenite::{Message, protocol::WebSocketConfig}};

const CAPACITY: usize = 128;
const FRAME_BYTES: usize = 65_536;
const TRACE_CAPACITY: usize = 32_768;
const LEASE_NS: u64 = 500_000_000;

pub fn monotonic_ns() -> u64 {
    let mut t: libc::timespec = unsafe { std::mem::zeroed() };
    assert_eq!(unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut t) }, 0);
    t.tv_sec as u64 * 1_000_000_000 + t.tv_nsec as u64
}
fn wall_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64
}
fn exchange(venue: Venue) -> &'static str {
    match venue { Venue::BinanceFutures => "binance_futures", Venue::OkxSpot => "okx_spot" }
}

#[derive(Clone, Deserialize)]
pub struct Connection {
    pub id: String, pub venue: Venue, pub bases: Vec<String>, pub url: String,
}
fn one()->usize{1}
fn event_ack()->String{"event".into()}
fn okx_public_ws_endpoint(url:&url::Url)->bool{
    url.host_str()==Some("ws.okx.com")&&url.port_or_known_default()==Some(443)
        &&url.path()=="/ws/v5/public"&&url.query().is_none()
}
#[derive(Deserialize)]
pub struct Config {
    pub mode: String, pub index: usize, pub owners: usize,
    pub connections: Vec<Connection>, pub report_path: String,
    #[serde(default)] pub history_socket: Option<String>,
    #[serde(default)] pub history_run: Option<String>,
    #[serde(default="one")] pub pipeline_depth:usize,
    #[serde(default="event_ack")] pub ack_mode:String,
}
impl Config {
    fn validate(&self) -> Result<(), String> {
        if ![1,2,4,8,16,32].contains(&self.pipeline_depth)||!["event","range","journal"].contains(&self.ack_mode.as_str())
            || (self.pipeline_depth>1 && (self.mode!="fixture"||self.history_socket.is_none())){return Err("isolated bounded pipeline configuration".into());}
        if self.owners != 3 || self.index >= self.owners || self.connections.is_empty()
            || self.connections.len() > 12 { return Err("owner topology".into()); }
        if !["fixture", "public_observation"].contains(&self.mode.as_str()) {
            return Err("isolated mode required".into());
        }
        if !self.report_path.ends_with(".json") || !self.report_path.contains("native_transport_")
            || self.report_path.starts_with("/home/tmpadmin/spotbot/") {
            return Err("isolated diagnostic report path required".into());
        }
        if let Some(path)=&self.history_socket {
            let isolated_path=if self.ack_mode=="journal" {
                self.mode=="fixture"&&path.starts_with("/home/tmpadmin/native_rust_registrar_review_20261002/")
            }else{path.starts_with("/home/tmpadmin/native_durable_history_review_")};
            if !isolated_path || !path.ends_with(".sock")
                || self.history_run.as_ref().map_or(true,|r|r.is_empty()||r.len()>100) {
                return Err("isolated history endpoint/run required".into());
            }
        }
        let mut ids = HashSet::new(); let mut pairs = HashMap::<String, HashSet<Venue>>::new();
        for c in &self.connections {
            if !ids.insert(&c.id) || c.bases.is_empty() || c.bases.len() > 128 { return Err("connection identity".into()); }
            let url = url::Url::parse(&c.url).map_err(|e| e.to_string())?;
            if url.username() != "" || url.password().is_some() { return Err("credentials forbidden".into()); }
            if self.mode == "fixture" {
                if url.scheme() != "ws" || url.host_str() != Some("127.0.0.1") { return Err("loopback only".into()); }
            } else {
                let endpoint_ok = match c.venue {
                    Venue::BinanceFutures => url.host_str() == Some("fstream.binance.com") && url.path() == "/public/stream"
                        && url.query() == Some(format!("streams={}", c.bases.iter().map(|b|
                            format!("{}usdt@bookTicker", b.to_ascii_lowercase())).collect::<Vec<_>>().join("/")).as_str()),
                    Venue::OkxSpot => okx_public_ws_endpoint(&url),
                };
                if url.scheme() != "wss" || !endpoint_ok { return Err("public market endpoint only".into()); }
            }
            for b in &c.bases {
                if b.is_empty() || !b.bytes().all(|x| x.is_ascii_uppercase() || x.is_ascii_digit())
                    || crate::owner_index(b, self.owners) != self.index
                    || !pairs.entry(b.clone()).or_default().insert(c.venue) {
                    return Err("single paired ownership/subscription".into());
                }
            }
        }
        if pairs.values().any(|v| v.len() != 2) { return Err("both venues required".into()); }
        Ok(())
    }
}
#[derive(Clone, Default)]
struct Link { epoch: u64, connected: bool, connects: u64, pongs: u64, error: Option<String> }
struct Gate {
    links: Vec<Link>, cutoffs: HashMap<String, u64>, lease_until: u64,
    fault: Option<String>, fault_ns: Option<u64>, started: bool,
}
struct Envelope { ordinal: u64, connection: usize, sequence: u64, frame: RawFrame }
#[derive(Default)]
struct Queue {
    frames: VecDeque<Envelope>, bytes: usize, admitted: u64, wire_text: u64,
    denied: u64, after_fault: u64, peak: usize, peak_bytes: usize,
}
struct Shared {
    gate: RwLock<Gate>, queue: Mutex<Queue>, stop: AtomicBool, rotate: Vec<AtomicU64>,
    heartbeat_wait_max_ns: AtomicU64, heartbeat_count:AtomicU64, heartbeat_during_history:AtomicU64, history_waiting:AtomicBool,
}
impl Shared {
    fn fail(&self, reason: String) {
        let mut g = self.gate.write().unwrap();
        if g.fault.is_none() { g.fault = Some(reason); g.fault_ns = Some(monotonic_ns()); }
        self.stop.store(true, Ordering::Release);
    }
    fn link(&self, index: usize, c: &Connection, connected: bool, error: Option<String>) -> u64 {
        let mut g = self.gate.write().unwrap(); let cut = monotonic_ns();
        for b in &c.bases { g.cutoffs.insert(b.clone(), cut); }
        let link = &mut g.links[index];
        link.connected = connected; link.error = error;
        if connected { link.epoch += 1; link.connects += 1; }
        link.epoch
    }
    fn admit(&self, index: usize, sequence: u64, frame: RawFrame) -> bool {
        let mut q = self.queue.lock().unwrap(); q.wire_text += 1;
        if self.stop.load(Ordering::Acquire) { q.after_fault += 1; return false; }
        if q.frames.len() == CAPACITY || q.bytes + frame.raw.len() > CAPACITY * FRAME_BYTES {
            q.denied += 1; drop(q); self.fail(format!("queue_full:{index}:{sequence}")); return false;
        }
        q.admitted += 1; let ordinal = q.admitted; q.bytes += frame.raw.len();
        q.frames.push_back(Envelope { ordinal, connection: index, sequence, frame });
        q.peak = q.peak.max(q.frames.len()); q.peak_bytes = q.peak_bytes.max(q.bytes);
        true
    }
    fn pop(&self) -> Option<Envelope> {
        let mut q = self.queue.lock().unwrap(); let e = q.frames.pop_front()?;
        q.bytes -= e.frame.raw.len(); Some(e)
    }
}

async fn receive(c: Connection, index: usize, shared: Arc<Shared>, fixture: bool) {
    let mut attempts = 0; let mut rotation = 0;
    let heartbeat = if fixture { 100 } else { 10_000 };
    let mut tls_builder = native_tls::TlsConnector::builder();
    tls_builder.min_protocol_version(Some(native_tls::Protocol::Tlsv12));
    let tls = match tls_builder.build() {
        Ok(tls) => tls,
        Err(error) => { shared.fail(format!("tls_config:{error}")); return; }
    };
    while !shared.stop.load(Ordering::Acquire) {
        let started = shared.gate.read().unwrap().started;
        if !started { tokio::time::sleep(Duration::from_millis(5)).await; continue; }
        let config = WebSocketConfig { max_message_size: Some(FRAME_BYTES), max_frame_size: Some(FRAME_BYTES),
            write_buffer_size: 4096, max_write_buffer_size: FRAME_BYTES, ..Default::default() };
        let connection = tokio::time::timeout(Duration::from_secs(15),
            connect_async_tls_with_config(&c.url, Some(config), false, Some(Connector::NativeTls(tls.clone())))).await;
        let mut ws = match connection {
            Ok(Ok((ws, _))) => ws,
            other => {
                shared.link(index, &c, false, Some(format!("connect:{other:?}")));
                attempts += 1;
                if attempts >= 5 { shared.fail(format!("reconnect_exhausted:{index}")); break; }
                tokio::time::sleep(Duration::from_millis(100 * attempts)).await; continue;
            }
        };
        attempts = 0; let epoch = shared.link(index, &c, true, None); let mut sequence = 0;
        if c.venue == Venue::OkxSpot {
            let args: Vec<_> = c.bases.iter().map(|b| json!({"channel":"books5","instId":format!("{b}-USDT")})).collect();
            if ws.send(Message::Text(json!({"op":"subscribe","args":args}).to_string())).await.is_err() {
                shared.link(index, &c, false, Some("subscribe_write".into())); continue;
            }
        }
        let mut timer = tokio::time::interval(Duration::from_millis(25));
        let mut ping_at = monotonic_ns(); let mut outstanding = false; let mut failure = None;
        loop {
            tokio::select! {
                _ = timer.tick() => {
                    if shared.stop.load(Ordering::Acquire) { break; }
                    let requested = shared.rotate[index].load(Ordering::Acquire);
                    if requested != rotation { rotation = requested; failure = Some("requested_reconnect".into()); break; }
                    let elapsed = (monotonic_ns() - ping_at) / 1_000_000;
                    if outstanding && elapsed > heartbeat * 3 { failure = Some("pong_timeout".into()); break; }
                    if !outstanding && elapsed >= heartbeat {
                        let ping = if c.venue == Venue::OkxSpot { Message::Text("ping".into()) }
                            else { Message::Ping(b"native-transport-barrier".to_vec()) };
                        if ws.send(ping).await.is_err() { failure = Some("ping_write".into()); break; }
                        outstanding = true; ping_at = monotonic_ns();
                    }
                },
                item = ws.next() => match item {
                    Some(Ok(Message::Text(raw))) if raw == "pong" => {
                        shared.gate.write().unwrap().links[index].pongs += 1;
                        outstanding = false; ping_at = monotonic_ns();
                    },
                    Some(Ok(Message::Pong(_))) => {
                        shared.gate.write().unwrap().links[index].pongs += 1;
                        outstanding = false; ping_at = monotonic_ns();
                    },
                    Some(Ok(Message::Ping(payload))) => {
                        if ws.send(Message::Pong(payload)).await.is_err() { failure = Some("pong_write".into()); break; }
                    },
                    Some(Ok(Message::Text(raw))) => {
                        sequence += 1;
                        let frame = RawFrame{source:None, venue:c.venue, epoch, received_ns:monotonic_ns(),
                            received_wall_ms:wall_ms(), raw };
                        if !shared.admit(index, sequence, frame) { break; }
                    },
                    Some(Ok(Message::Close(_))) | None => { failure = Some("remote_close".into()); break; },
                    Some(Err(e)) => { failure = Some(format!("socket:{e}")); break; },
                    Some(Ok(_)) => { shared.fail(format!("unsupported_message:{index}")); break; },
                }
            }
        }
        shared.link(index, &c, false, failure);
        let _ = tokio::time::timeout(Duration::from_millis(200), ws.close(None)).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

struct ContextToken {
    generation:u64,metadata_hash:String,revision:u64,incarnation:u64,resume_cut:u64,link_cut:u64,
    links:Vec<(usize,u64,bool,u64)>,
}
enum PendingPhase{Signal,Decision{results:Vec<Value>,index:usize},Ready}
struct Pending{e:Envelope,update:Update,token:ContextToken,context:Value,row:Value,phase:PendingPhase,request:Option<u64>,invalid_reason:Option<String>,operation:u64,wait_start:u64,hb_start:u64,reasons:Vec<Value>}
struct Actor {
    core:DecisionCore,owner:Owner,prepared:Owner,assigned:Vec<String>,epochs:Vec<u64>,cutoffs:HashMap<String,u64>,
    trace:Vec<Value>,consumed:u64,applied:u64,suppressed:u64,rejected:u64,invalidated:u64,control:u64,resume_cut:u64,incarnation:u64,samples:HashSet<(usize,u64)>,
    history:Option<HistoryClient>,metadata_hash:String,staged_hash:Option<(u64,String)>,pending:VecDeque<Pending>,revision:u64,
    pending_peak:usize,operation_serial:u64,prepared_chain:u64,shadow_root:u64,canonical_ordinal:u64,hold_apply:bool,prepared_invalidated:bool,
    signal_frontier:Frontier,applied_frontier:Frontier,decision_frontier:Frontier,publication_frontier:Frontier,
}
impl Actor {
    fn status(&self,s:&Shared,cfg:&Config,trace:bool)->Value{
        let g=s.gate.read().unwrap();let q=s.queue.lock().unwrap();
        let links:Vec<_>=cfg.connections.iter().zip(&g.links).map(|(c,l)|json!({"id":c.id,"epoch":l.epoch,"connected":l.connected,"connects":l.connects,"pongs":l.pongs,"error":l.error})).collect();
        let pending_item=|p:&Pending|json!({"ordinal":p.e.ordinal,"operation":p.operation,"generation":p.token.generation,
            "phase":match p.phase{PendingPhase::Signal=>"signal",PendingPhase::Decision{..}=>"decision",PendingPhase::Ready=>"ready"},
            "request_id":p.request,"wait_started_ns":p.wait_start,"signal_state_mutated_ns":p.row["signal_state_mutated_ns"],
            "signal_ack_observed_ns":p.row["signal_ack_observed_ns"],"invalid_reason":p.invalid_reason});
        let mut v=json!({"owner":cfg.index,"pid":std::process::id(),"incarnation":self.incarnation,"generation":self.core.metadata_generation(),
            "paused":self.core.decisions_paused(),"fault":g.fault,"fault_ns":g.fault_ns,"lease_until_ns":g.lease_until,"links":links,
            "heartbeat_wait_max_ns":s.heartbeat_wait_max_ns.load(Ordering::Relaxed),"heartbeat_count":s.heartbeat_count.load(Ordering::Relaxed),
            "heartbeat_during_history":s.heartbeat_during_history.load(Ordering::Relaxed),"pipeline_depth":cfg.pipeline_depth,"ack_mode":cfg.ack_mode,
            "pending_count":self.pending.len(),"pending":self.pending.front().map(pending_item),"pending_items":self.pending.iter().map(pending_item).collect::<Vec<_>>(),"pending_peak":self.pending_peak,
            "queue_depth":q.frames.len(),"queue_bytes":q.bytes,"queue_peak":q.peak,"queue_byte_peak":q.peak_bytes,"capacity":CAPACITY,
            "admitted":q.admitted,"wire_text":q.wire_text,"denied":q.denied,"after_fault":q.after_fault,"consumed":self.consumed,
            "applied":self.applied,"suppressed":self.suppressed,"rejected":self.rejected,"invalidated":self.invalidated,"control":self.control,"resume_cut":self.resume_cut,
            "transport_accounting_ok":q.admitted==self.applied+self.rejected+self.invalidated+self.control+q.frames.len() as u64+self.pending.len() as u64,
            "wire_accounting_ok":q.wire_text==q.admitted+q.denied+q.after_fault,"history_certified":false,
"metadata_hash":self.metadata_hash,"trace":Value::Null});
        drop(q);drop(g);
        let telemetry=json!({            "history_created":self.history.as_ref().map(|h|h.created),"history_db1_admitted":self.history.as_ref().filter(|h|!h.journal).map(|h|h.admitted),"history_db1_committed":self.history.as_ref().filter(|h|!h.journal).map(|h|h.committed),"history_journal_admitted":self.history.as_ref().filter(|h|h.journal).map(|h|h.admitted),"history_journal_durable":self.history.as_ref().filter(|h|h.journal).map(|h|h.committed),"history_ack_source":cfg.ack_mode,
            "history_unadmitted":self.history.as_ref().map(|h|h.created-h.admitted),"history_outstanding":self.history.as_ref().map(|h|h.outstanding()),"history_outstanding_peak":self.history.as_ref().map(|h|h.peak),
            "ack_messages":self.history.as_ref().map(|h|h.messages),"ack_read_bytes":self.history.as_ref().map(|h|h.read_bytes),"admission_write_bytes":self.history.as_ref().map(|h|h.write_bytes),
            "frontiers":{"prepared":self.operation_serial,"signal_durable":self.signal_frontier.through,"applied_resolution":self.applied_frontier.through,
                "last_canonical_mutation_ordinal":self.canonical_ordinal,"decision_resolution":self.decision_frontier.through,"publication_resolution":self.publication_frontier.through,
                "request_durable":self.history.as_ref().map(|h|h.frontier.through)},"hold_apply":self.hold_apply});v.as_object_mut().unwrap().extend(telemetry.as_object().unwrap().clone());
        if trace{let mut rows=self.trace.clone();for p in &self.pending{let mut r=p.row.clone();r["disposition"]=json!("pending_history");rows.push(r);}v["trace"]=json!(rows);v["canonical_books"]=json!(self.owner.snapshot_books());v["prepared_books"]=json!(self.prepared.snapshot_books());}
        v
    }

    fn command(&mut self, v:&Value, s:&Shared, cfg:&Config) -> Result<(),String> {
        // Progress RAM/COMMIT messages outside gate even under consecutive control commands.
        // Resume certifies that old-generation submissions crossed RAM admission first.
        let healthy={s.gate.read().unwrap().fault.is_none()};
        if healthy{
            if let Some(h)=self.history.as_mut(){if h.outstanding()>0&&h.poll().is_err(){s.fail("history_pressure".into());}}
        }
        s.history_waiting.store(self.history.as_ref().is_some_and(|h|h.waiting()),Ordering::Release);
        let generation=v["generation"].as_u64().unwrap_or(0);
        let op=v["op"].as_str().unwrap_or("");
        if s.gate.read().unwrap().fault.is_some() && !["status","stop","fail"].contains(&op) {
            return Err("latched transport/coordinator fault; restart cohort required".into());
        }
        let result:Result<(),CoreError>=match op {
            "stage" => {
                let routes:HashMap<String,Vec<RouteSpec>>=serde_json::from_value(v["routes"].clone()).map_err(|e|e.to_string())?;
                if routes.len()>1024 || routes.iter().any(|(b,rows)|b.len()>64 || rows.iter().any(|r|
                    r.metadata_state.len()>64 || r.label.len()>256 || r.buy_exchange.len()>64 || r.sell_exchange.len()>64
                    || [&r.buy,&r.sell].iter().any(|l|l.min_amount.len()>64 || l.step.len()>64 || l.min_cost.len()>64
                        || l.max_amount.as_ref().is_some_and(|v|v.len()>64)))) {return Err("metadata field bound".into());}
                if self.assigned.iter().any(|b|!routes.contains_key(b)) {return Err("subscription metadata missing".into());}
                self.core.stage_routes(generation,routes).map_err(|e|format!("{e:?}"))?;
                let digest=openssl::sha::sha256(&serde_json::to_vec(&v["routes"]).unwrap());
                self.staged_hash=Some((generation,digest.iter().map(|b|format!("{b:02x}")).collect()));
                Ok(())
            },
            "pause" => { self.core.pause_decisions(); Ok(()) },
            "commit" => {
                self.core.commit_staged(generation).map_err(|e|format!("{e:?}"))?;
                let (g,hash)=self.staged_hash.take().ok_or("metadata capsule")?;
                if g!=generation {return Err("metadata capsule generation".into());}
                self.metadata_hash=hash; Ok(())
            },
            "abort" => {self.core.abort_staged();self.staged_hash=None;Ok(())},
            "resume" => {
                let certificate=v["certificate"].as_array().ok_or("cohort certificate required")?;
                if certificate.len()!=cfg.owners || certificate.iter().any(|x|x.as_u64()!=Some(generation)) {
                    return Err("mixed cohort generation".into());
                }
                let cut=v["cutover_ns"].as_u64().ok_or("common cutover required")?;
                if cut<self.resume_cut || cut>monotonic_ns() {return Err("invalid cutover".into());}
                self.core.resume_decisions(generation).map_err(|e|format!("{e:?}"))?;
                self.resume_cut=cut;
                for b in &self.assigned {self.core.invalidate_books(b).map_err(|e|format!("{e:?}"))?;}
                s.gate.write().unwrap().started=true;
                Ok(())
            },
            "reconnect" => {
                let id=v["connection"].as_str().ok_or("connection id")?;
                let index=cfg.connections.iter().position(|c|c.id==id).ok_or("unknown connection")?;
                s.rotate[index].fetch_add(1,Ordering::Release); Ok(())
            },
            "fail" => {s.fail(v["reason"].as_str().unwrap_or("cohort_failed").into());self.core.pause_decisions();Ok(())},
            "fixture_hold_apply" if cfg.mode=="fixture" => {self.hold_apply=v["enabled"].as_bool().ok_or("fixture hold boolean")?;Ok(())},
            "status"|"stop" => Ok(()),
            _=>Err(CoreError::Invalid("command")),
        };
        result.map_err(|e|format!("{e:?}"))?;
        if ["pause","commit","resume","reconnect","fail","stop"].contains(&op){self.revision+=1;}
        if op!="status" {self.trace.push(json!({"kind":"control","op":op,"at_ns":monotonic_ns(),
            "generation":self.core.metadata_generation(),"paused":self.core.decisions_paused(),
            "consumed":self.consumed,"applied":self.applied,"resume_cut":self.resume_cut}));}
        Ok(())
    }
    fn finish(&mut self,mut p:Pending,_:&Shared,kind:&str,error:Option<String>){
        p.row["disposition"]=json!(kind);p.row["finished_ns"]=json!(monotonic_ns());if let Some(error)=error{p.row["error"]=json!(error);}
        match kind{"applied"|"applied_before_recovery_cut"=>self.applied+=1,"subscription_ack"=>self.control+=1,"stale_epoch"|"out_of_order"|"invalid_frame"=>self.rejected+=1,_=>self.invalidated+=1};
        if kind=="applied_before_recovery_cut"{self.suppressed+=1;}
        self.publication_frontier.mark(p.operation);self.trace.push(p.row);
    }
    fn token_valid(&self,p:&Pending,g:&Gate,s:&Shared,cfg:&Config)->Result<(),String>{
        if g.fault.is_some()||s.stop.load(Ordering::Acquire){return Err("cohort_invalidated".into())}
        if self.incarnation!=p.token.incarnation{return Err("incarnation_changed".into())}
        if self.core.metadata_generation()!=p.token.generation||self.metadata_hash!=p.token.metadata_hash{return Err("generation_changed".into())}
        if self.revision!=p.token.revision||self.core.decisions_paused(){return Err("barrier_transition".into())}
        if monotonic_ns()>g.lease_until{return Err("coordinator_lease_expired".into())}
        if self.resume_cut!=p.token.resume_cut||*g.cutoffs.get(&p.update.base).unwrap_or(&0)!=p.token.link_cut{return Err("recovery_boundary_changed".into())}
        for &(i,epoch,connected,rotation) in &p.token.links{if g.links[i].epoch!=epoch||g.links[i].connected!=connected||s.rotate[i].load(Ordering::Acquire)!=rotation{return Err(format!("connection_context_changed:{}",cfg.connections[i].id));}}
        if !g.links[p.e.connection].connected||g.links[p.e.connection].epoch!=p.e.frame.epoch{return Err("stale_epoch".into())}Ok(())
    }
    fn start_wait(&mut self,p:&mut Pending)->Result<(),String>{
        p.wait_start=monotonic_ns();let mut c=p.context.clone();
        match &p.phase{
            PendingPhase::Signal=>{c["kind"]=json!("signal");c["frame"]=json!(p.e.frame);c["base"]=json!(p.update.base);c["book"]=json!(p.update.book);},
            PendingPhase::Decision{results,index}=>{c["kind"]=json!("decision");c["frame"]=json!(p.e.frame);c["base"]=json!(p.update.base);c["decision"]=results[*index].clone();c["route_index"]=json!(index);},
            PendingPhase::Ready=>return Err("ready cannot create event".into()),
        }
        c["event_created_ns"]=json!(p.wait_start);c["event_created_wall_ms"]=json!(wall_ms());
        p.request=Some(if let Some(h)=self.history.as_mut(){h.start(c).map_err(|e|format!("{e:?}"))?}else{0});Ok(())
    }
    fn can_prepare(&self,s:&Shared,cfg:&Config)->bool{
        if self.prepared_invalidated||self.pending.len()>=cfg.pipeline_depth||self.history.as_ref().is_some_and(|h|!h.can_start()){return false}
        let g=s.gate.read().unwrap();self.pending.iter().all(|p|p.invalid_reason.is_none()&&self.token_valid(p,&g,s,cfg).is_ok())
    }
    fn consume(&mut self,e:Envelope,s:&Shared,cfg:&Config){
        assert!(self.pending.len()<cfg.pipeline_depth);assert_eq!(e.ordinal,self.consumed+1);self.consumed=e.ordinal;
        let mut row=json!({"kind":"frame","ordinal":e.ordinal,"connection":cfg.connections[e.connection].id,"sequence":e.sequence,"epoch":e.frame.epoch,
            "received_ns":e.frame.received_ns,"processed_ns":monotonic_ns(),"generation":self.core.metadata_generation(),"paused":self.core.decisions_paused(),
            "resume_cut":self.resume_cut,"owner":cfg.index,"incarnation":self.incarnation,"gate_hold_ns":0,"gate_hold_total_ns":0,"durable_waits":[],"decision_receipts":[]});
        let gate=s.gate.read().unwrap();let entered=monotonic_ns();row["lease_until_ns"]=json!(gate.lease_until);
        let prepared=(||->Result<(Update,ContextToken,Value),String>{
            if gate.fault.is_some(){return Err("invalidated_fault".into())}
            if self.pending.iter().any(|p|self.token_valid(p,&gate,s,cfg).is_err()){
                self.prepared_invalidated=true;return Err("invalidated_boundary".into());
            }
            let link=&gate.links[e.connection];if !link.connected||link.epoch!=e.frame.epoch{return Err("stale_epoch".into())}
            for(i,c)in cfg.connections.iter().enumerate(){let next=gate.links[i].epoch;if next>self.epochs[i]{for b in &c.bases{self.owner.reset_venue(b,c.venue,next).map_err(|x|format!("{x:?}"))?;self.core.reset_exchange(b,exchange(c.venue),next).map_err(|x|format!("{x:?}"))?;}self.epochs[i]=next;}}
            for b in &self.assigned{let cut=self.resume_cut.max(*gate.cutoffs.get(b).unwrap_or(&0));if cut>*self.cutoffs.get(b).unwrap_or(&0){self.core.invalidate_books(b).map_err(|x|format!("{x:?}"))?;self.cutoffs.insert(b.clone(),cut);}}
            if self.pending.is_empty(){self.prepared=self.owner.clone();self.prepared_chain+=1;self.shadow_root=self.canonical_ordinal;}
            let value:Value=serde_json::from_str(&e.frame.raw).map_err(|x|x.to_string())?;let connection=&cfg.connections[e.connection];
            if let Some(event)=value.get("event").and_then(Value::as_str){if e.frame.venue==Venue::OkxSpot&&event=="subscribe"&&value["arg"]["channel"]=="books5"&&connection.bases.iter().any(|b|value["arg"]["instId"]==format!("{b}-USDT")){return Err("subscription_ack".into())}return Err(format!("unexpected control:{event}"));}
            if e.frame.venue==Venue::BinanceFutures&&value["data"].get("st").is_some_and(|x|x.as_u64()!=Some(1)){return Err("non-USD-M market".into())}
            let u=parse_json(&e.frame,&value).map_err(|x|format!("{x:?}"))?;if !connection.bases.contains(&u.base){return Err("unauthorized symbol".into())}
            if u.book.exchange_ts_ms.is_none()||(e.frame.venue==Venue::BinanceFutures&&u.book.sequence.is_none()){return Err("source timestamp/update ID missing".into())}
            let u=self.prepared.prepare_parsed(&e.frame,u).map_err(|x|format!("{x:?}"))?;
            if self.samples.len()<24&&self.samples.insert((e.connection,e.frame.epoch)){row["raw_sample"]=json!(e.frame.raw);}
            let cut=self.cutoffs[&u.base];row["base"]=json!(u.base);row["recovery_cut"]=json!(cut);
            let token=ContextToken{generation:self.core.metadata_generation(),metadata_hash:self.metadata_hash.clone(),revision:self.revision,incarnation:self.incarnation,
                resume_cut:self.resume_cut,link_cut:*gate.cutoffs.get(&u.base).unwrap_or(&0),links:cfg.connections.iter().enumerate().filter(|(_,c)|c.bases.contains(&u.base)).map(|(i,_)|(i,gate.links[i].epoch,gate.links[i].connected,s.rotate[i].load(Ordering::Acquire))).collect()};
            let context=json!({"run":cfg.history_run,"owner":cfg.index,"incarnation":self.incarnation,"ordinal":e.ordinal,"generation":token.generation,"metadata_hash":token.metadata_hash,
                "recovery_cut":cut,"pre_recovery_cut":e.frame.received_ns<cut,"prepared_chain":self.prepared_chain,"prepared_root_ordinal":self.shadow_root});
            Ok((u,token,context))
        })();let held=monotonic_ns()-entered;row["gate_hold_ns"]=json!(held);row["gate_hold_total_ns"]=json!(held);drop(gate);
        match prepared{
            Ok((u,token,context))=>{
                self.operation_serial+=1;row["operation"]=json!(self.operation_serial);
                let mut p=Pending{e,update:u,token,context,row,phase:PendingPhase::Signal,request:None,invalid_reason:None,
                    operation:self.operation_serial,reasons:Vec::new(),wait_start:0,hb_start:s.heartbeat_count.load(Ordering::Relaxed)};
                let preparation=self.prepared.ingest_parsed(&p.e.frame,p.update.clone(),&mut PreparedSignal).map_err(|e|format!("prepared:{e:?}"));
                if let Err(error)=preparation.and_then(|_|self.start_wait(&mut p)){self.finish(p,s,"invalid_frame",Some(error));s.fail("history_pressure".into());}
                else{self.pending.push_back(p);self.pending_peak=self.pending_peak.max(self.pending.len());}
            },
            Err(error)=>{let kind=match error.as_str(){"invalidated_fault"=>"invalidated_fault","invalidated_boundary"=>"invalidated_boundary","stale_epoch"=>"stale_epoch","OutOfOrder"=>"out_of_order","subscription_ack"=>"subscription_ack",_=>"invalid_frame"};
                match kind{"subscription_ack"=>self.control+=1,"invalidated_fault"|"invalidated_boundary"=>self.invalidated+=1,_=>self.rejected+=1};row["disposition"]=json!(kind);row["finished_ns"]=json!(monotonic_ns());if kind=="invalid_frame"{row["error"]=json!(error);s.fail("invalid_frame".into());}self.trace.push(row);},
        }
        s.history_waiting.store(self.history.as_ref().is_some_and(|h|h.waiting()),Ordering::Release);
    }
    fn held(row:&mut Value,entered:u64){let elapsed=monotonic_ns()-entered;row["gate_hold_ns"]=json!(row["gate_hold_ns"].as_u64().unwrap().max(elapsed));row["gate_hold_total_ns"]=json!(row["gate_hold_total_ns"].as_u64().unwrap()+elapsed);}
    fn advance(&mut self,s:&Shared,cfg:&Config)->bool{
        let mut progress=false;
        if let Some(h)=self.history.as_mut(){match h.poll(){Ok(p)=>progress=p,Err(error)=>{s.fail("history_pressure".into());while let Some(p)=self.pending.pop_front(){self.finish(p,s,"invalid_frame",Some(format!("{error:?}")));}s.history_waiting.store(false,Ordering::Release);return true;}}}
        let mut slots=std::mem::take(&mut self.pending);
        for p in &mut slots{
            if let Some(id)=p.request{
                let receipt=if let Some(h)=self.history.as_mut(){h.take(id)}else{Some(Value::Null)};
                if let Some(receipt)=receipt{
                    let handled=monotonic_ns();let observed=receipt["native_ack_received_ns"].as_u64().unwrap_or(handled);let phase=match p.phase{PendingPhase::Signal=>"signal",PendingPhase::Decision{..}=>"decision",PendingPhase::Ready=>unreachable!()};
                    p.row["durable_waits"].as_array_mut().unwrap().push(json!({"phase":phase,"request_id":id,"started_ns":p.wait_start,"ack_observed_ns":observed,"ack_handled_ns":handled,"db1_commit_ns":receipt["db1_commit_ns"],"journal_durable_ns":receipt["durable_ns"],"journal_seq":receipt["journal_seq"],"heartbeat_renewals":s.heartbeat_count.load(Ordering::Relaxed)-p.hb_start}));
                    p.row[format!("{phase}_durable")]=receipt.clone();p.request=None;progress=true;
                    match &mut p.phase{
                        PendingPhase::Signal=>{p.row["signal_ack_observed_ns"]=json!(observed);self.signal_frontier.mark(p.operation);},
                        PendingPhase::Decision{results,index}=>{p.row["decision_receipts"].as_array_mut().unwrap().push(receipt);*index+=1;if *index==results.len(){p.phase=PendingPhase::Ready;self.decision_frontier.mark(p.operation);}},
                        PendingPhase::Ready=>unreachable!(),
                    }
                }
            }
        }
        let mut predecessor_invalid=self.prepared_invalidated;
        // A broken prepared predecessor invalidates the remaining prepared chain.
        // A later ACK cannot silently repair a speculative dependency.
        for p in &mut slots {
            let gate=s.gate.read().unwrap();let bad=self.token_valid(p,&gate,s,cfg).err();drop(gate);
            if let Some(reason)=bad {p.invalid_reason=Some(reason);predecessor_invalid=true;self.prepared_invalidated=true;}
        }
        if self.prepared_invalidated {
            for p in &mut slots {if p.invalid_reason.is_none(){p.invalid_reason=Some("prepared_predecessor_invalidated".into());}}
        }
        for p in &mut slots{
            if !matches!(p.phase,PendingPhase::Signal){continue}
            if p.request.is_some(){break}
            let gate=s.gate.read().unwrap();let entered=monotonic_ns();p.row["lease_margin_after_ack_ns"]=json!(gate.lease_until.saturating_sub(entered));
            let validation=p.invalid_reason.clone().map_or_else(||self.token_valid(p,&gate,s,cfg),Err).and_then(|_|if predecessor_invalid{Err("prepared_predecessor_invalidated".into())}else{Ok(())});
            if self.hold_apply&&validation.is_ok(){Self::held(&mut p.row,entered);drop(gate);break}
            let action=validation.and_then(|_|{
                self.owner.ingest_parsed(&p.e.frame,p.update.clone(),&mut DurableSignal{expected:&p.update}).map_err(|e|format!("state_transition:{e:?}"))?;
                p.row["signal_state_mutated_ns"]=json!(monotonic_ns());self.canonical_ordinal=p.e.ordinal;
                let book=self.owner.snapshot_venue(&p.update.base,p.e.frame.venue).unwrap();
                if book.received_ns<p.context["recovery_cut"].as_u64().unwrap(){return Ok(Vec::new())}
                let ns=monotonic_ns();let wall=wall_ms();let result=self.core.update_at(&p.update.base,exchange(p.e.frame.venue),book,ns,wall).map_err(|e|format!("state_transition:{e:?}"))?;
                let(spot,future)=self.owner.snapshot_pair(&p.update.base);p.context["spot"]=json!(spot);p.context["future"]=json!(future);p.context["evaluation_ns"]=json!(ns);p.context["evaluation_wall_ms"]=json!(wall);
                p.row["spot_received_ns"]=json!(spot.as_ref().map(|b|b.received_ns));p.row["future_received_ns"]=json!(future.as_ref().map(|b|b.received_ns));p.row["spot_epoch"]=json!(spot.as_ref().map(|b|b.epoch));p.row["future_epoch"]=json!(future.as_ref().map(|b|b.epoch));
                Ok(result.iter().map(|d|serde_json::to_value(d).unwrap()).collect::<Vec<_>>())
            });Self::held(&mut p.row,entered);drop(gate);self.applied_frontier.mark(p.operation);progress=true;
            match action{
                Ok(results)=>{p.reasons=results.iter().map(|d|d["reason"].clone()).collect();if results.is_empty(){p.phase=PendingPhase::Ready;self.decision_frontier.mark(p.operation);}else{p.phase=PendingPhase::Decision{results,index:0};}},
                Err(error)=>{let fatal=error=="coordinator_lease_expired"||error.starts_with("state_transition:");p.invalid_reason=Some(error.clone());p.phase=PendingPhase::Ready;self.decision_frontier.mark(p.operation);predecessor_invalid=true;if fatal{s.fail(error);}},
            }
        }
        // DECISION submission order is frame/route order, including multi-route frames.
        for p in &mut slots{
            if matches!(p.phase,PendingPhase::Signal){break}
            if p.invalid_reason.is_some()&&p.request.is_none()&&matches!(p.phase,PendingPhase::Decision{..}){p.phase=PendingPhase::Ready;self.decision_frontier.mark(p.operation);}
            if let PendingPhase::Decision{results,index}=&p.phase{
                let last=*index+1==results.len();
                if p.request.is_none(){
                    if self.history.as_ref().is_some_and(|h|!h.can_start()){break}
                    p.hb_start=s.heartbeat_count.load(Ordering::Relaxed);
                    if let Err(error)=self.start_wait(p){s.fail("history_pressure".into());p.invalid_reason=Some(error);p.phase=PendingPhase::Ready;}progress=true;
                }
                if !last{break}
            }
        }
        // Ready successors never cross a missing publication predecessor.
        while slots.front().is_some_and(|p|matches!(p.phase,PendingPhase::Ready)){
            let mut p=slots.pop_front().unwrap();let gate=s.gate.read().unwrap();let entered=monotonic_ns();
            let outcome=p.invalid_reason.take().map_or_else(||self.token_valid(&p,&gate,s,cfg),Err);
            let outcome=outcome.and_then(|_|{if p.context["pre_recovery_cut"]!=true{let ns=monotonic_ns();if ns>gate.lease_until{return Err("coordinator_lease_expired".into())}p.row["published_ns"]=json!(ns);p.row["published_generation"]=json!(self.core.metadata_generation());p.row["reasons"]=json!(p.reasons);}Ok(())});
            Self::held(&mut p.row,entered);drop(gate);
            match outcome{Ok(())=>{let kind=if p.context["pre_recovery_cut"]==true{"applied_before_recovery_cut"}else{"applied"};self.finish(p,s,kind,None);},Err(error)=>{if error=="coordinator_lease_expired"{s.fail(error.clone());}p.row["reasons"]=json!([]);self.finish(p,s,"durable_context_invalidated",Some(error));}}
            progress=true;
        }
        self.pending=slots;if self.pending.is_empty(){self.prepared_invalidated=false;}s.history_waiting.store(self.history.as_ref().is_some_and(|h|h.waiting()),Ordering::Release);progress
    }

}

pub fn run(path:&str) -> Result<(),Box<dyn std::error::Error>> {
    let cfg:Config=serde_json::from_slice(&std::fs::read(path)?)?;cfg.validate()?;
    let count=cfg.connections.len(); let now=monotonic_ns();
    let shared=Arc::new(Shared {gate:RwLock::new(Gate{links:vec![Link::default();count],cutoffs:HashMap::new(),
        lease_until:now+LEASE_NS,fault:None,fault_ns:None,started:false}),queue:Mutex::new(Queue::default()),
        stop:AtomicBool::new(false),heartbeat_wait_max_ns:AtomicU64::new(0),heartbeat_count:AtomicU64::new(0),heartbeat_during_history:AtomicU64::new(0),history_waiting:AtomicBool::new(false),rotate:(0..count).map(|_|AtomicU64::new(0)).collect()});
    let runtime=tokio::runtime::Builder::new_multi_thread().worker_threads(1).thread_name("native-ws-rx").enable_all().build()?;
    for (i,c) in cfg.connections.iter().enumerate(){runtime.spawn(receive(c.clone(),i,shared.clone(),cfg.mode=="fixture"));}
    let (tx,rx)=mpsc::sync_channel::<Value>(16);let reader_shared=shared.clone();
    std::thread::spawn(move||{
        let stdin=io::stdin();let mut reader=stdin.lock();
        loop {
            let mut line=Vec::new();
            let n=std::io::Read::take(std::io::Read::by_ref(&mut reader),1_048_577).read_until(b'\n',&mut line);
            if matches!(n,Ok(0)) {break;}
            let v=n.ok().filter(|n|*n<=1_048_576).and_then(|_|serde_json::from_slice::<Value>(&line).ok());
            match v {
                Some(v) if v["op"]=="heartbeat"=>{
                    let begin=monotonic_ns();let mut g=reader_shared.gate.write().unwrap();let now=monotonic_ns();
                    reader_shared.heartbeat_wait_max_ns.fetch_max(now-begin,Ordering::Relaxed);
                    reader_shared.heartbeat_count.fetch_add(1,Ordering::Relaxed);
                    if reader_shared.history_waiting.load(Ordering::Acquire){reader_shared.heartbeat_during_history.fetch_add(1,Ordering::Relaxed);}
                    g.lease_until=now+LEASE_NS;
                },
                Some(v)=>{if tx.try_send(v).is_err(){reader_shared.fail("control_queue_full".into());break;}},
                None=>{reader_shared.fail("invalid_control".into());break;}
            }
        }
        reader_shared.fail("coordinator_eof".into());
    });
    let decision_cfg=DecisionConfig {max_age_ms:800,metadata_ttl_ms:43_200_000,base_size:10.,risk_cap:10.,
        cost:24.03,required_net:42.,min_price:0.,max_price:f64::INFINITY,blacklist:HashSet::new()};
    let mut assigned:Vec<_>=cfg.connections.iter().flat_map(|c|c.bases.clone()).collect();assigned.sort();assigned.dedup();
    let mut actor=Actor {core:DecisionCore::new(cfg.index,3,decision_cfg,HashMap::new()).unwrap(),
        owner:Owner::new(cfg.index,3,800).unwrap(),prepared:Owner::new(cfg.index,3,800).unwrap(),assigned,epochs:vec![0;count],cutoffs:HashMap::new(),
        trace:Vec::new(),consumed:0,applied:0,suppressed:0,rejected:0,invalidated:0,control:0,resume_cut:0,incarnation:now,samples:HashSet::new(),
        history:cfg.history_socket.as_ref().map(|p|if cfg.ack_mode=="journal"{HistoryClient::connect_journal(p,cfg.pipeline_depth)}else{HistoryClient::connect(p,cfg.pipeline_depth,cfg.ack_mode=="range")}).transpose()?,
        metadata_hash:String::new(),staged_hash:None,pending:VecDeque::new(),revision:0,
        pending_peak:0,operation_serial:0,prepared_chain:0,shadow_root:0,canonical_ordinal:0,hold_apply:false,prepared_invalidated:false,
        signal_frontier:Frontier::default(),applied_frontier:Frontier::default(),decision_frontier:Frontier::default(),publication_frontier:Frontier::default()};
    actor.core.pause_decisions();let mut out=io::stdout().lock();let mut notified=false;
    loop {
        let expired = monotonic_ns()>shared.gate.read().unwrap().lease_until;
        if expired {shared.fail("coordinator_lease_expired".into());}
        if actor.trace.len()>=TRACE_CAPACITY-128 {shared.fail("diagnostic_trace_full".into());}
        let disconnected=match rx.try_recv() {
            Ok(v)=>{
                let outcome=actor.command(&v,&shared,&cfg);
                let response=json!({"id":v["id"],"ok":outcome.is_ok(),"error":outcome.err(),
                    "status":actor.status(&shared,&cfg,v["trace"].as_bool()==Some(true))});
                if writeln!(out,"{response}").and_then(|_|out.flush()).is_err(){shared.fail("control_output_closed".into());break;}
                if v["op"]=="stop" {shared.fail("controlled_stop".into());break;}
                continue;
            },
            Err(mpsc::TryRecvError::Disconnected)=>true,
            Err(mpsc::TryRecvError::Empty)=>false,
        };
        let fault=shared.gate.read().unwrap().fault.is_some();
        if fault {actor.core.pause_decisions();}
        if fault && !notified {
            notified=true;
            let response=json!({"id":0,"ok":false,"error":"latched fault","status":actor.status(&shared,&cfg,false)});
            if writeln!(out,"{response}").and_then(|_|out.flush()).is_err(){break;}
        }
        let progressed=actor.advance(&shared,&cfg);
        if actor.pending.is_empty()&&fault || !fault&&!actor.core.decisions_paused()&&actor.can_prepare(&shared,&cfg){
            if let Some(e)=shared.pop(){actor.consume(e,&shared,&cfg);continue;}
        }
        if progressed{continue;}
        if disconnected {break;}
        std::thread::sleep(if actor.pending.is_empty(){Duration::from_millis(2)}else{Duration::from_micros(200)});
    }
    shared.fail("owner_exit".into());actor.core.pause_decisions();
    while let Some(p)=actor.pending.pop_front(){actor.finish(p,&shared,"invalidated_owner_exit",Some("owner_exit".into()));}
    while let Some(e)=shared.pop(){actor.consume(e,&shared,&cfg);}
    std::fs::write(&cfg.report_path,serde_json::to_vec_pretty(&actor.status(&shared,&cfg,true))?)?;
    runtime.shutdown_timeout(Duration::from_millis(300));
    Ok(())
}

#[cfg(test)]
mod r1212_okx_endpoint_tests {
    use super::okx_public_ws_endpoint;
    #[test]
    fn public_okx_transport_uses_443_after_retirement() {
        assert!(okx_public_ws_endpoint(&url::Url::parse("wss://ws.okx.com/ws/v5/public").unwrap()));
        assert!(okx_public_ws_endpoint(&url::Url::parse("wss://ws.okx.com:443/ws/v5/public").unwrap()));
        assert!(!okx_public_ws_endpoint(&url::Url::parse("wss://ws.okx.com:8443/ws/v5/public").unwrap()));
        assert!(!okx_public_ws_endpoint(&url::Url::parse("wss://evil.example/ws/v5/public").unwrap()));
    }
}
