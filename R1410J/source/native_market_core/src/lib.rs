//! Isolated, execution-free paired market state. No exchange credentials or order API.
//! Each base belongs to exactly one owner. Every accepted input is handed to the
//! required history sink before a decision snapshot may be emitted.
// Reject optimized/deployment builds carrying the offline clock API.
#[cfg(all(feature="offline-test-clock",not(debug_assertions)))]
compile_error!("offline-test-clock requires a debug offline test build; deployment is forbidden");

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Venue { BinanceFutures, OkxSpot }

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Book {
    pub bids: Vec<(f64, f64)>,
    pub asks: Vec<(f64, f64)>,
    pub received_ns: u64,
    pub received_wall_ms: i64,
    pub exchange_ts_ms: Option<i64>,
    pub changed_ns: u64,
    pub epoch: u64,
    pub sequence: Option<i64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct R1247FrameTiming {
    pub t1_ws_text_mono_ns: Option<u64>,
    pub t1_ws_text_wall_ms: Option<i64>,
    pub t1_previous_instrument_mono_ns: Option<u64>,
    pub t1_intermessage_gap_ns: Option<u64>,
    pub previous_exchange_event_ts_ms: Option<i64>,
    pub exchange_event_timestamp_advanced: Option<bool>,
    pub t3_enqueue_mono_ns: Option<u64>,
    pub t3_queue_wait_ns: Option<u64>,
    pub t4_engine_start_ns: Option<u64>,
    pub t4_engine_wall_ms: Option<i64>,
    pub t2_parse_start_ns: Option<u64>,
    pub t2_parse_end_ns: Option<u64>,
    pub t4_validation_end_ns: Option<u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct R1279FrameTiming {
    #[serde(default,skip_serializing_if="Option::is_none")] pub r1325_output_enqueued_ns:Option<u64>,
    #[serde(default,skip_serializing_if="Option::is_none")] pub r1324a_output_occupied_slots:Option<usize>,
    pub clock_domain_id:Option<String>, pub instrument:Option<String>, pub exchange_event_ts_ms:Option<i64>,
    pub ws_receive_ns:Option<u64>, pub ws_decode_start_ns:Option<u64>, pub ws_decode_end_ns:Option<u64>, pub reader_enqueue_ns:Option<u64>, pub reader_dequeue_ns:Option<u64>,
    #[serde(default)] pub r1310_trace_sampled:bool,
    #[serde(default)] pub source_storage_durable:Option<bool>,
    #[serde(default)] pub validator_start_ns:Option<u64>, pub validator_end_ns:Option<u64>,
    #[serde(default)] pub provider_start_ns:Option<u64>, pub provider_end_ns:Option<u64>,
    #[serde(default)] pub bridge_enqueue_start_ns:Option<u64>, pub bridge_enqueue_end_ns:Option<u64>,
    #[serde(default)] pub bridge_dequeue_start_ns:Option<u64>, pub bridge_dequeue_end_ns:Option<u64>,
    #[serde(default)] pub bridge_queue_wait_ns:Option<u64>, pub bridge_queue_depth:Option<usize>,
    #[serde(default)] pub dispatcher_start_ns:Option<u64>, pub dispatcher_end_ns:Option<u64>,
    #[serde(default)] pub owner_observation_start_ns:Option<u64>, pub owner_observation_end_ns:Option<u64>,
    #[serde(default,skip_serializing_if="Option::is_none")] pub central_permit_acquired_ns:Option<u64>,
    pub central_enqueue_ns:Option<u64>, pub central_dequeue_ns:Option<u64>,
    pub validation_start_ns:Option<u64>, pub validation_end_ns:Option<u64>,
    pub book_start_ns:Option<u64>, pub book_end_ns:Option<u64>,
    pub source_selection_start_ns:Option<u64>, pub source_selection_end_ns:Option<u64>,
    pub source_epoch_start_ns:Option<u64>, pub source_epoch_end_ns:Option<u64>,
    pub source_epoch_worker_queue_ns:Option<u64>, pub source_epoch_store_lock_wait_ns:Option<u64>,
    pub source_epoch_store_operation_ns:Option<u64>, pub source_epoch_other_wait_ns:Option<u64>,
    pub source_selection_authority_wait_ns:Option<u64>, pub source_selection_authority_hold_ns:Option<u64>,
    #[serde(default)] pub source_selection_wall_residual_ns:Option<u64>,
    #[serde(default)] pub receive_batch_id:Option<u64>, #[serde(default)] pub selection_batch_id:Option<u64>, pub receive_batch_start_ns:Option<u64>, pub receive_batch_end_ns:Option<u64>,
    #[serde(default)] pub frame_hold_reason:Option<String>, pub terminalization_reason:Option<String>,
    #[serde(default)] pub frame_residence_start_ns:Option<u64>, #[serde(default)] pub frame_residence_end_ns:Option<u64>, #[serde(default)] pub frame_residence_status:Option<String>,
    pub engine_receive_begin_ns:Option<u64>, pub engine_receive_end_ns:Option<u64>,
    pub canonical_enqueue_ns:Option<u64>, pub canonical_dequeue_ns:Option<u64>,
    pub quarantine_start_ns:Option<u64>, pub quarantine_end_ns:Option<u64>, #[serde(default)] pub quarantine_status:Option<String>,
    pub source_durable_start_ns:Option<u64>, pub source_durable_end_ns:Option<u64>,
    pub paper_decision_start_ns:Option<u64>, pub paper_decision_end_ns:Option<u64>, pub decision_trace_id:Option<String>,
    pub publish_durable_start_ns:Option<u64>, pub publish_durable_end_ns:Option<u64>,
    pub ack_start_ns:Option<u64>, pub ack_end_ns:Option<u64>, pub terminal_ns:Option<u64>,
    pub reader_queue_capacity:Option<usize>, pub reader_queue_depth:Option<usize>,
    pub central_queue_capacity:Option<usize>, pub central_queue_depth:Option<usize>,
    pub canonical_queue_capacity:Option<usize>, pub canonical_queue_depth:Option<usize>,
}
impl R1279FrameTiming {
    pub fn json(&self)->serde_json::Value { serde_json::to_value(self).unwrap_or(serde_json::Value::Null) }
}
pub fn r1310_duration_ns(start:Option<u64>,end:Option<u64>)->Option<u64>{start.zip(end).and_then(|(a,b)|b.checked_sub(a))}
pub fn r1310_stage_event(stage:&str,trace_id:&str,start:u64,end:u64)->serde_json::Value{
    serde_json::json!({"stage":stage,"trace_id":trace_id,"start_ns":start,"end_ns":end,"duration_ns":r1310_duration_ns(Some(start),Some(end))})
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceProvenance {
    // In-memory selection proof; JSON/recovery cannot manufacture a live capability.
    #[serde(skip)] pub selected_permit:Option<crate::market_data_ingress_r1205::Permit>,
    #[serde(default)] pub frame_trace_id:String,
    pub journal_id:String, pub accepted_id:u64,
    pub session_id:String, pub connection_epoch:u64,
    pub selection_epoch:u64, pub payload_sha256:String,
    #[serde(default,skip_serializing_if="Option::is_none")]
    pub transport:Option<crate::ingress_transport_r1205::Stamp>,
    #[serde(default,skip_serializing_if="Option::is_none")] pub accepted_ns:Option<u64>,
    #[serde(default,skip_serializing_if="Option::is_none")] pub selected_ns:Option<u64>,
    #[serde(skip)] pub(crate) prepared:Option<Box<crate::market_data_processor_r1378::PreparedFrame>>,
    #[serde(skip)] pub r1247_timing:Option<R1247FrameTiming>,
    #[serde(default)] pub r1279_timing:Option<R1279FrameTiming>,
}
impl PartialEq for SourceProvenance {
    fn eq(&self,other:&Self)->bool { self.frame_trace_id==other.frame_trace_id && self.journal_id==other.journal_id && self.accepted_id==other.accepted_id && self.session_id==other.session_id && self.connection_epoch==other.connection_epoch && self.selection_epoch==other.selection_epoch && self.payload_sha256==other.payload_sha256 && self.transport==other.transport && self.accepted_ns==other.accepted_ns && self.selected_ns==other.selected_ns && self.r1247_timing==other.r1247_timing }
}
impl Eq for SourceProvenance {}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RawFrame {
    pub venue: Venue,
    pub epoch: u64,
    pub received_ns: u64,
    pub received_wall_ms: i64,
    pub raw: String,
    #[serde(default,skip_serializing_if="Option::is_none")]
    pub source:Option<SourceProvenance>,
}

pub fn ensure_frame_trace_id(frame:&mut RawFrame) {
    static NEXT:std::sync::atomic::AtomicU64=std::sync::atomic::AtomicU64::new(1);
    if let Some(source)=frame.source.as_mut() {
        if source.frame_trace_id.is_empty() {
            let n=NEXT.fetch_add(1,std::sync::atomic::Ordering::Relaxed);
            source.frame_trace_id=format!("{}:{}:{}",source.session_id,source.connection_epoch,n);
        }
    }
}

pub fn r1279_trace_row(frame:&RawFrame,terminal:&str,ack_status:&str)->serde_json::Value {
    let source=frame.source.as_ref();
    let trace=source.and_then(|s|s.r1279_timing.as_ref());
    let instrument=trace.and_then(|t|t.instrument.as_deref());
    let exchange_ts=trace.and_then(|t|t.exchange_event_ts_ms);
    let t=trace.cloned().unwrap_or_default();
    let start=t.ws_decode_end_ns.or_else(||frame.source.as_ref().and_then(|s|s.r1247_timing.as_ref()).and_then(|v|v.t1_ws_text_mono_ns));
    let duration=|a:Option<u64>,b:Option<u64>|a.zip(b).and_then(|(x,y)|y.checked_sub(x));
    serde_json::json!({
      "frame_trace_id":source.map(|s|s.frame_trace_id.as_str()).unwrap_or("missing"),"source":source.map(|s|s.session_id.as_str()),
      "venue":format!("{:?}",frame.venue),"instrument":instrument,"exchange_event_ts_ms":exchange_ts,
      "accepted_id":source.map(|s|s.accepted_id),"terminal_state":terminal,"ack_status":ack_status,"decision_trace_id":t.decision_trace_id,
      "timestamps_mono_ns":t,"internal_ns_from_decode_to_terminal":duration(start,t.terminal_ns),
      "receive_to_terminal_ns":duration(t.ws_receive_ns,t.terminal_ns),
      "exchange_event_age_at_ws_receive_ms":exchange_ts.map(|x|frame.received_wall_ms.saturating_sub(x)),
      "reader_queue_wait_ns":duration(t.reader_enqueue_ns,t.reader_dequeue_ns),
      "central_queue_wait_ns":duration(t.central_enqueue_ns,t.central_dequeue_ns),
      "canonical_queue_wait_ns":duration(t.canonical_enqueue_ns,t.canonical_dequeue_ns),
      "engine_receive_ns":duration(t.engine_receive_begin_ns,t.engine_receive_end_ns),
      "source_durable_wait_ns":duration(t.source_durable_start_ns,t.source_durable_end_ns),
      "paper_decision_ns":duration(t.paper_decision_start_ns,t.paper_decision_end_ns),
      "publish_durable_wait_ns":duration(t.publish_durable_start_ns,t.publish_durable_end_ns),
      "ack_wait_ns":duration(t.ack_start_ns,t.ack_end_ns),
      "r1310_spans":{"engine_ns":duration(t.engine_receive_begin_ns,t.engine_receive_end_ns),
       "storage_ns":duration(t.source_durable_start_ns,t.source_durable_end_ns),
       "decode_ns":duration(t.ws_decode_start_ns,t.ws_decode_end_ns),
       "book_reconstruction_ns":duration(t.book_start_ns,t.book_end_ns),
       "validator_ns":duration(t.validator_start_ns,t.validator_end_ns),
       "provider_ns":duration(t.provider_start_ns,t.provider_end_ns),
       "bridge_enqueue_ns":duration(t.bridge_enqueue_start_ns,t.bridge_enqueue_end_ns),
       "bridge_queue_wait_ns":t.bridge_queue_wait_ns,
       "bridge_dequeue_ns":duration(t.bridge_dequeue_start_ns,t.bridge_dequeue_end_ns),
       "dispatcher_ns":duration(t.dispatcher_start_ns,t.dispatcher_end_ns),
       "owner_observation_ns":duration(t.owner_observation_start_ns,t.owner_observation_end_ns),
       "end_to_end_ns":duration(t.ws_receive_ns,t.owner_observation_end_ns)}
    })
}

#[derive(Clone, Debug, PartialEq)]
pub struct Update { pub base: String, pub venue: Venue, pub book: Book }

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct PairedSnapshot {
    pub base: String,
    pub spot: Book,
    pub future: Book,
    pub indicative_bps: f64,
    pub observed_ns: u64,
}

#[derive(Debug, PartialEq)]
pub enum CoreError {
    Invalid(&'static str), WrongOwner, StaleEpoch, OutOfOrder,
    HistoryPressure, NonFinite,
}
pub trait HistorySink {
    /// RAM admission only; durable ACK is a later, separately measured SQLite commit.
    fn accept(&mut self, original: &RawFrame, update: &Update) -> Result<(), CoreError>;
}

#[derive(Clone,Default)]
struct Pair { spot: Option<Book>, future: Option<Book> }

// R1121: OKX books5 is a FULL SNAPSHOT (not a delta). Its seqId may
// legitimately reset to a LOWER number during OKX maintenance. This
// strictly-validated exception is restricted to opt-in live PUBLIC PAPER.
// Every new book MUST carry a fresh exchange time and intact full depth.
// No exception for Binance, stale snapshots, or incremental channels.
fn r1121_okx_newer_books5_snapshot(
 frame:&RawFrame, update:&Update, prior:&Book, opted_in:bool
)->bool{
 if !opted_in||frame.venue!=Venue::OkxSpot ||
    update.book.bids.is_empty()||update.book.asks.is_empty()||
    frame.received_ns<=prior.received_ns ||
    frame.received_wall_ms<prior.received_wall_ms {
    return false
 }
 let (Some(old_seq),Some(new_seq),Some(old_ts),Some(new_ts))=(
    prior.sequence,update.book.sequence,prior.exchange_ts_ms,update.book.exchange_ts_ms)
 else{return false};
 if old_seq<0||new_seq<0||new_seq>old_seq||new_ts<=old_ts||
    frame.received_wall_ms.saturating_sub(new_ts)>10_000||
    new_ts.saturating_sub(frame.received_wall_ms)>1000{
    return false
 }
 let Ok(v)=serde_json::from_str::<Value>(&frame.raw) else{return false};
 if v["arg"]["channel"]!="books5"||
    v["arg"]["instId"].as_str()!=Some(
       format!("{}-USDT",update.base).as_str())||
    v.get("action").and_then(Value::as_str)
       .is_some_and(|a|a!="snapshot"){
    return false
 }
 let Some(rows)=v["data"].as_array() else{return false};
 if rows.len()!=1{return false}
 let row=&rows[0];
 if row["seqId"].as_i64()!=Some(new_seq)||
    row["ts"].as_str().and_then(|s|s.parse::<i64>().ok())!=Some(new_ts)||
    !row["bids"].as_array().is_some_and(|v|!v.is_empty())||
    !row["asks"].as_array().is_some_and(|v|!v.is_empty()){
    return false
 }
 // books5 snapshots may not contain prevSeqId. If present, require
 // a source-defined full snapshot or a continuous maintenance reset.
 if let Some(prev)=row.get("prevSeqId").and_then(Value::as_i64){
    if prev!=-1&&prev!=old_seq{return false}
 }
 true
}

pub(crate) fn r1216_okx_empty_books_continuity(frame:&RawFrame,update:&Update,prior:&Book)->bool{
 if frame.venue!=Venue::OkxSpot||update.venue!=Venue::OkxSpot||frame.source.is_none()||
    frame.received_ns<=prior.received_ns||frame.received_wall_ms<prior.received_wall_ms||
    update.book.sequence!=prior.sequence||update.book.bids!=prior.bids||update.book.asks!=prior.asks{
  return false
 }
 let (Some(seq),Some(old_ts),Some(new_ts))=(prior.sequence,prior.exchange_ts_ms,update.book.exchange_ts_ms)
 else{return false};
 if new_ts<=old_ts{return false}
 let Ok(v)=serde_json::from_str::<Value>(&frame.raw) else{return false};
 if v["arg"]["channel"]!="books"||v.get("action").and_then(Value::as_str)!=Some("update")||
    v["arg"]["instId"].as_str()!=Some(format!("{}-USDT",update.base).as_str()){
  return false
 }
 let Some(rows)=v["data"].as_array() else{return false};
 if rows.len()!=1{return false}
 let row=&rows[0];
 row["seqId"].as_i64()==Some(seq)&&row["prevSeqId"].as_i64()==Some(seq)&&
 row["ts"].as_str().and_then(|s|s.parse::<i64>().ok())==Some(new_ts)&&
 row["bids"].as_array().is_some_and(|levels|levels.is_empty())&&
 row["asks"].as_array().is_some_and(|levels|levels.is_empty())
}

pub(crate) fn r1234_okx_books_maintenance_reset(
    frame: &RawFrame, base: &str, book: &Book, prior: &Book, max_age_ms: u64,
) -> bool {
    let Some(source) = frame.source.as_ref() else { return false };
    if frame.venue != Venue::OkxSpot || frame.epoch != book.epoch || prior.epoch != frame.epoch ||
        frame.received_ns != book.received_ns ||
        frame.received_wall_ms != book.received_wall_ms ||
        frame.received_ns <= prior.received_ns ||
        frame.received_wall_ms < prior.received_wall_ms ||
        book.bids.is_empty() || book.asks.is_empty() ||
        source.selection_epoch != frame.epoch || source.connection_epoch == 0 ||
        source.accepted_id == 0 || source.journal_id.len() != 64 ||
        source.session_id.is_empty() || source.session_id.len() > 96 {
        return false;
    }
    let actual = openssl::sha::sha256(frame.raw.as_bytes()).iter()
        .map(|v| format!("{v:02x}")).collect::<String>();
    if source.payload_sha256 != actual { return false; }
    let (Some(old_seq), Some(new_seq), Some(old_ts), Some(new_ts)) =
        (prior.sequence, book.sequence, prior.exchange_ts_ms, book.exchange_ts_ms)
    else { return false };
    let Some(event_age_ms) = frame.received_wall_ms.checked_sub(new_ts) else { return false };
    if old_seq < 0 || new_seq < 0 || new_seq >= old_seq || new_ts <= old_ts ||
        event_age_ms < 0 || event_age_ms > max_age_ms.min(800) as i64 {
        return false;
    }
    let Ok(v) = serde_json::from_str::<Value>(&frame.raw) else { return false };
    if v["arg"]["channel"] != "books" || v["arg"]["instId"].as_str() != Some(
        format!("{base}-USDT").as_str()) ||
        v.get("action").and_then(Value::as_str) != Some("update") {
        return false;
    }
    let Some(rows) = v["data"].as_array() else { return false };
    if rows.len() != 1 { return false; }
    let row = &rows[0];
    row["seqId"].as_i64() == Some(new_seq) &&
        row["prevSeqId"].as_i64() == Some(old_seq) &&
        row["ts"].as_str().and_then(|s| s.parse::<i64>().ok()) == Some(new_ts) &&
        row["bids"].as_array().is_some() && row["asks"].as_array().is_some()
}

#[derive(Clone)]
pub struct Owner {
    pub index: usize,
    pub total: usize,
    pub max_age_ms: u64,
    epochs: HashMap<(String, Venue), u64>,
    minimum_source: HashMap<(String, Venue), i64>,
    reset_books: HashMap<(String, Venue), (Book, Option<String>)>,
    source_sessions:HashMap<(String,Venue),(String,u64)>,
    pairs: HashMap<String, Pair>,
}
impl Owner {
    pub fn new(index: usize, total: usize, max_age_ms: u64) -> Result<Self, CoreError> {
        if total == 0 || index >= total { return Err(CoreError::Invalid("owner count")); }
        Ok(Self { index, total, max_age_ms, epochs: HashMap::new(), minimum_source: HashMap::new(), reset_books: HashMap::new(), source_sessions:HashMap::new(), pairs: HashMap::new() })
    }
    pub fn source_binding(&self,base:&str,venue:Venue)->Option<(String,u64)>{self.source_sessions.get(&(base.into(),venue)).cloned()}
    pub fn owns(&self, base: &str) -> bool { owner_index(base, self.total) == self.index }
    /// Read-only complete pair, including stale/missing sides, for canonical
    /// decision evaluation. The caller must apply the same freshness policy.
    pub fn snapshot_pair(&self, base: &str) -> (Option<Book>, Option<Book>) {
        if let Some(pair) = self.pairs.get(base) {
            (pair.spot.clone(), pair.future.clone())
        } else { (None, None) }
    }
    pub fn snapshot_venue(&self, base: &str, venue: Venue) -> Option<Book> {
        self.pairs.get(base).and_then(|pair| match venue {
            Venue::OkxSpot => pair.spot.clone(), Venue::BinanceFutures => pair.future.clone(),
        })
    }
    pub fn snapshot_books(&self) -> Vec<(String, Option<Book>, Option<Book>)> {
        let mut entries: Vec<_> = self.pairs.iter().map(|(base,pair)|
            (base.clone(),pair.spot.clone(),pair.future.clone())).collect();
        entries.sort_by(|a,b|a.0.cmp(&b.0));
        entries
    }
    /// A disconnected venue is invalidated before a new connection is allowed to submit data.
    pub fn reset_venue(&mut self, base: &str, venue: Venue, new_epoch: u64) -> Result<(), CoreError> {
        if !self.owns(base) { return Err(CoreError::WrongOwner); }
        let key = (base.to_owned(), venue);
        if self.epochs.get(&key).is_some_and(|prior| new_epoch <= *prior) {
            return Err(CoreError::StaleEpoch);
        }
        let prior_session = self.source_sessions.get(&key).map(|(session, _)| session.clone());
        let old = self.pairs.get_mut(base).and_then(|pair| {
            match venue { Venue::OkxSpot => pair.spot.take(), Venue::BinanceFutures => pair.future.take() }
        });
        if let Some(book) = old {
            if let Some(source) = book.exchange_ts_ms {
                self.minimum_source.insert(key.clone(), source);
                self.reset_books.insert(key.clone(), (book, prior_session));
            }
        }
        self.source_sessions.remove(&key);
        self.epochs.insert(key, new_epoch);
        Ok(())
    }
    pub fn ingest<H: HistorySink>(&mut self, frame: &RawFrame, history: &mut H)
        -> Result<Option<PairedSnapshot>, CoreError>
    {
        let update = parse(frame)?;
        self.ingest_parsed(frame, update, history)
    }
    /// Read-only validation/reconciliation before a deferred mandatory COMMIT.
    pub fn prepare_parsed(&self, frame: &RawFrame, mut update: Update)
        -> Result<Update, CoreError>
    {
        if update.venue != frame.venue || update.book.epoch != frame.epoch ||
            update.book.received_ns != frame.received_ns ||
            update.book.received_wall_ms != frame.received_wall_ms {
            return Err(CoreError::Invalid("frame/update mismatch"));
        }
        if !self.owns(&update.base) { return Err(CoreError::WrongOwner); }
        let key = (update.base.clone(), frame.venue);
        match &frame.source {
            Some(source)=>{
                let actual:String=openssl::sha::sha256(frame.raw.as_bytes()).iter().map(|v|format!("{v:02x}")).collect();
                let ram_selected=source.accepted_id==0&&source.journal_id.is_empty()&&source.selected_permit.is_some();
                let durable_receipt=source.accepted_id>0&&source.journal_id.len()==64;
                if source.selection_epoch==0||source.selection_epoch!=frame.epoch||source.connection_epoch==0||(!ram_selected&&!durable_receipt)||
                    source.session_id.is_empty()||source.session_id.len()>96||source.payload_sha256!=actual {
                    return Err(CoreError::Invalid("ingress source provenance"));
                }
                if self.source_sessions.get(&key).is_some_and(|(session,epoch)|
                    session!=&source.session_id||*epoch!=source.selection_epoch){return Err(CoreError::StaleEpoch)}
            },
            None=>if self.source_sessions.contains_key(&key){return Err(CoreError::Invalid("protected source missing"))}
        }
        let epoch = self.epochs.get(&key).copied().unwrap_or(frame.epoch);
        if epoch != frame.epoch { return Err(CoreError::StaleEpoch); }
        let reset_key = (update.base.clone(), frame.venue);
        if self.minimum_source.get(&reset_key).is_some_and(
            |prior| update.book.exchange_ts_ms.map_or(true, |now| now <= *prior)) {
            let equivalent_reset_replay = self.reset_books.get(&reset_key).is_some_and(|(prior, prior_session)| {
                frame.source.as_ref().is_some_and(|source|
                    prior_session.as_ref().map_or(true, |session| session != &source.session_id)) &&
                    frame.received_ns >= prior.received_ns &&
                    frame.received_wall_ms >= prior.received_wall_ms &&
                    update.book.sequence.is_some() &&
                    update.book.sequence == prior.sequence &&
                    update.book.exchange_ts_ms == prior.exchange_ts_ms &&
                    update.book.bids == prior.bids &&
                    update.book.asks == prior.asks
            });
            if !equivalent_reset_replay { return Err(CoreError::OutOfOrder); }
        }
        let old = self.pairs.get(&update.base).and_then(|pair| match frame.venue { Venue::OkxSpot => pair.spot.as_ref(), Venue::BinanceFutures => pair.future.as_ref() });
        if let Some(prior) = old {
            if frame.received_ns < prior.received_ns ||
                (update.book.sequence.is_some() && prior.sequence.is_some() &&
                 update.book.sequence <= prior.sequence &&
                 !r1121_okx_newer_books5_snapshot(frame,&update,prior,
                    (frame.source.is_some() || std::env::var("ASTRA_R1121_BOOKS5_SNAPSHOT_RESET")
                      .ok().as_deref()==Some("YES") &&
                    std::env::var("ASTRA_R1119_FULL_RUST_PAPER")
                      .ok().as_deref()==Some("YES"))) &&
                 !r1216_okx_empty_books_continuity(frame,&update,prior) &&
                 !r1234_okx_books_maintenance_reset(frame,&update.base,&update.book,prior,self.max_age_ms)) {
                return Err(CoreError::OutOfOrder);
            }
            if prior.bids == update.book.bids && prior.asks == update.book.asks {
                update.book.changed_ns = prior.changed_ns;
            }
        }
        Ok(update)
    }
    /// Preserve the sink-before-book-mutation contract for synchronous and deferred callers.
    pub fn ingest_parsed<H: HistorySink>(&mut self, frame:&RawFrame, update:Update, history:&mut H)
        -> Result<Option<PairedSnapshot>,CoreError>
    {
        let update=self.prepare_parsed(frame,update)?;
        // A full mandatory history queue fails closed; no book mutation or decision.
        history.accept(frame, &update)?;
        if let Some(source)=&frame.source{
            self.source_sessions.insert((update.base.clone(),frame.venue),(source.session_id.clone(),source.selection_epoch));
        }
        self.epochs.entry((update.base.clone(),frame.venue)).or_insert(frame.epoch);
        let pair=self.pairs.entry(update.base.clone()).or_default();
        let old=match frame.venue {Venue::OkxSpot=>&mut pair.spot,Venue::BinanceFutures=>&mut pair.future};
        *old = Some(update.book);
        let reset_key = (update.base.clone(), frame.venue);
        self.minimum_source.remove(&reset_key);
        self.reset_books.remove(&reset_key);
        let (spot, future) = match (&pair.spot, &pair.future) {
            (Some(s), Some(f)) => (s, f), _ => return Ok(None),
        };
        if spot.asks.is_empty() || future.bids.is_empty() { return Ok(None); }
        let age_ok = |book: &Book| {
            frame.received_ns >= book.received_ns &&
            frame.received_ns - book.received_ns <= self.max_age_ms * 1_000_000 &&
            book.exchange_ts_ms.is_some_and(|ts|
                frame.received_wall_ms.checked_sub(ts).is_some_and(|age|
                    age >= -1000 && age <= self.max_age_ms as i64))
        };
        if !age_ok(spot) || !age_ok(future) { return Ok(None); }
        let bps = (future.bids[0].0 / spot.asks[0].0 - 1.0) * 10_000.0;
        if !bps.is_finite() { return Err(CoreError::NonFinite); }
        Ok(Some(PairedSnapshot {base:update.base, spot:spot.clone(),future:future.clone(),
            indicative_bps:bps,observed_ns:frame.received_ns}))
    }
}
impl Venue { fn key(self) -> &'static str { match self { Venue::BinanceFutures => "binance", Venue::OkxSpot => "okx" } } }
impl std::hash::Hash for Venue { fn hash<H: std::hash::Hasher>(&self, state: &mut H) { self.key().hash(state); } }

/// FNV-1a is specified here to be stable across processes and language runtimes.
pub fn owner_index(base: &str, count: usize) -> usize {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in base.bytes() { hash ^= u64::from(byte); hash = hash.wrapping_mul(0x100000001b3); }
    (hash % count as u64) as usize
}

fn decimal(value: &Value) -> Result<f64, CoreError> {
    let parsed = value.as_str().and_then(|s| s.parse::<f64>().ok())
        .or_else(|| value.as_f64()).ok_or(CoreError::Invalid("decimal"))?;
    if !parsed.is_finite() || parsed < 0.0 { return Err(CoreError::NonFinite); }
    Ok(parsed)
}
fn levels(value: &Value) -> Result<Vec<(f64,f64)>, CoreError> {
    let items = value.as_array().ok_or(CoreError::Invalid("levels"))?;
    if items.len() > 5000 { return Err(CoreError::Invalid("book bound")); }
    let mut out = Vec::with_capacity(items.len());
    for entry in items {
        let row = entry.as_array().ok_or(CoreError::Invalid("level"))?;
        if row.len() < 2 { return Err(CoreError::Invalid("level fields")); }
        let price = decimal(&row[0])?;
        if price <= 0.0 { return Err(CoreError::Invalid("price")); }
        let quantity = decimal(&row[1])?;
        if quantity <= 0.0 { return Err(CoreError::Invalid("quantity")); }
        out.push((price, quantity));
    }
    Ok(out)
}
fn validate_depth20(bids: &[(f64,f64)], asks: &[(f64,f64)]) -> Result<(), CoreError> {
    if bids.is_empty() || asks.is_empty() { return Err(CoreError::Invalid("empty depth")); }
    if bids.windows(2).any(|w| w[0].0 <= w[1].0) {
        return Err(CoreError::Invalid("bid order or duplicate"));
    }
    if asks.windows(2).any(|w| w[0].0 >= w[1].0) {
        return Err(CoreError::Invalid("ask order or duplicate"));
    }
    if bids[0].0 >= asks[0].0 { return Err(CoreError::Invalid("crossed or locked book")); }
    Ok(())
}
fn symbol(raw: &str, suffix: &str) -> Result<String, CoreError> {
    let base = raw.strip_suffix(suffix).ok_or(CoreError::Invalid("quote"))?;
    if base.is_empty() || !base.bytes().all(|c| c.is_ascii_uppercase() || c.is_ascii_digit()) {
        return Err(CoreError::Invalid("symbol"));
    }
    Ok(base.to_owned())
}
pub fn parse(frame: &RawFrame) -> Result<Update, CoreError> {
    if frame.raw.len() > 65_536 { return Err(CoreError::Invalid("frame bound")); }
    let json: Value = serde_json::from_str(&frame.raw).map_err(|_|CoreError::Invalid("json"))?;
    parse_json(frame, &json)
}
/// Reuse the owner adapter decode; identical book parsing and bounds to parse().
pub fn parse_json(frame: &RawFrame, json: &Value) -> Result<Update, CoreError> {
    if frame.raw.len() > 65_536 { return Err(CoreError::Invalid("frame bound")); }
    let (base, bids, asks, ts, seq) = match frame.venue {
        Venue::BinanceFutures => {
            let d = json.get("data").ok_or(CoreError::Invalid("data"))?;
            // R161: after Binance UM/CM stream merge, never admit a
            // COIN-M frame to the USD-M owner even if symbol/price looks valid.
            // Missing st is accepted for historical offline review fixtures;
            // present st MUST identify UM=1.
            if d.get("st").is_some() && d["st"].as_u64()!=Some(1){
                return Err(CoreError::Invalid("non-USD-M Binance source"))
            }
            if let Some(ps)=d.get("ps"){
                if ps.as_str()!=d.get("s").and_then(Value::as_str){
                    return Err(CoreError::Invalid("Binance pair/symbol mismatch"))
                }
            }
            let event = d.get("e").and_then(Value::as_str).ok_or(CoreError::Invalid("event"))?;
            let base = symbol(d.get("s").and_then(Value::as_str).ok_or(CoreError::Invalid("symbol"))?, "USDT")?;
            let ts = d.get("E").and_then(Value::as_i64).or_else(||d.get("T").and_then(Value::as_i64));
            match event {
                "bookTicker" => {
                    let bid = (decimal(&d["b"])?,decimal(&d["B"])?);
                    let ask = (decimal(&d["a"])?,decimal(&d["A"])?);
                    if bid.0 == 0.0 || ask.0 == 0.0 { return Err(CoreError::Invalid("price")); }
                    if bid.1 == 0.0 || ask.1 == 0.0 { return Err(CoreError::Invalid("quantity")); }
                    (base, vec![bid], vec![ask], ts, d.get("u").and_then(Value::as_i64))
                }
                "depthUpdate" => {
                    let bids = levels(d.get("b").ok_or(CoreError::Invalid("bids"))?)?;
                    let asks = levels(d.get("a").ok_or(CoreError::Invalid("asks"))?)?;
                    if json.get("stream").and_then(Value::as_str)
                        .is_some_and(|s| s.ends_with("@depth20@100ms")) {
                        if bids.len() > 20 || asks.len() > 20 {
                            return Err(CoreError::Invalid("depth20 level bound"));
                        }
                        validate_depth20(&bids, &asks)?;
                    } else if bids.is_empty() || asks.is_empty() {
                        return Err(CoreError::Invalid("empty depth"));
                    }
                    (base, bids, asks, ts, d.get("u").and_then(Value::as_i64))
                }
                _ => return Err(CoreError::Invalid("event")),
            }
        }
        Venue::OkxSpot => {
            let arg = json.get("arg").ok_or(CoreError::Invalid("arg"))?;
            if arg.get("channel").and_then(Value::as_str) != Some("books5") { return Err(CoreError::Invalid("channel")); }
            let base = symbol(arg.get("instId").and_then(Value::as_str).ok_or(CoreError::Invalid("symbol"))?, "-USDT")?;
            let row = json.get("data").and_then(Value::as_array).and_then(|a|a.first()).ok_or(CoreError::Invalid("data"))?;
            let ts = row.get("ts").and_then(Value::as_str).and_then(|s|s.parse::<i64>().ok());
            (base, levels(&row["bids"])?,levels(&row["asks"])?, ts,
             row.get("seqId").and_then(Value::as_i64))
        }
    };
    let book = Book { bids, asks, received_ns:frame.received_ns,
        received_wall_ms:frame.received_wall_ms, exchange_ts_ms:ts,
        changed_ns:frame.received_ns, epoch:frame.epoch, sequence:seq };
    Ok(Update {base, venue:frame.venue, book})
}

#[cfg(test)]
mod tests {
 use super::*;
 #[derive(Default)] struct History { events: Vec<String>, full: bool }
 impl HistorySink for History {
  fn accept(&mut self, f:&RawFrame,u:&Update)->Result<(),CoreError>{
   if self.full { return Err(CoreError::HistoryPressure); }
   self.events.push(format!("{:?}:{}:{}",f.venue,u.base,u.book.changed_ns)); Ok(())
  }
 }
 fn b(base:&str,price:&str,seq:i64,ns:u64,epoch:u64)->RawFrame{
  RawFrame{source:None,venue:Venue::BinanceFutures,epoch,received_ns:ns,received_wall_ms:1000,
   raw:format!(r#"{{"stream":"{}usdt@bookTicker","data":{{"e":"bookTicker","s":"{}USDT","b":"{}","B":"2","a":"1.20","A":"2","u":{},"E":1000}}}}"#,base.to_lowercase(),base,price,seq)}
 }
 fn o(base:&str,price:&str,ns:u64,epoch:u64)->RawFrame{
  RawFrame{source:None,venue:Venue::OkxSpot,epoch,received_ns:ns,received_wall_ms:1000,
   raw:format!(r#"{{"arg":{{"channel":"books5","instId":"{}-USDT"}},"data":[{{"bids":[["0.90","2","0","3"]],"asks":[["{}","2","0","3"]],"ts":"1000"}}]}}"#,base,price)}
 }
 #[test] fn deferred_prepare_keeps_book_unchanged_and_rechecks_epoch() {
  let mut c=Owner::new(owner_index("BTC",3),3,1000).unwrap();let mut h=History::default();
  let f=b("BTC","1.10",1,1,1);let u=c.prepare_parsed(&f,parse(&f).unwrap()).unwrap();
  assert_eq!(c.snapshot_pair("BTC"),(None,None));
  c.ingest_parsed(&f,u,&mut h).unwrap();
  let f2=b("BTC","1.10",2,2,1);let prepared=c.prepare_parsed(&f2,parse(&f2).unwrap()).unwrap();
  assert_eq!(prepared.book.changed_ns,1);assert_eq!(c.snapshot_pair("BTC").1.unwrap().received_ns,1);
  c.reset_venue("BTC",Venue::BinanceFutures,2).unwrap();
  assert_eq!(c.ingest_parsed(&f2,prepared,&mut h),Err(CoreError::StaleEpoch));assert_eq!(h.events.len(),1);
 }
 #[test] fn paired_order_changed_ns_and_return() {
  let mut c=Owner::new(owner_index("BTC",3),3,1000).unwrap();let mut h=History::default();
  assert!(c.ingest(&b("BTC","1.10",1,1,1),&mut h).unwrap().is_none());
  let first=c.ingest(&o("BTC","1.00",2,1),&mut h).unwrap().unwrap();
  assert!((first.indicative_bps - 1000.0).abs() < 0.000001);
  let same=c.ingest(&b("BTC","1.10",2,3,1),&mut h).unwrap().unwrap();
  assert_eq!(same.future.changed_ns,1);
  let altered=c.ingest(&b("BTC","1.11",3,4,1),&mut h).unwrap().unwrap();
  assert_eq!(altered.future.changed_ns,4);
  let returned=c.ingest(&b("BTC","1.10",4,5,1),&mut h).unwrap().unwrap();
  assert_eq!(returned.future.changed_ns,5);
  assert_eq!(h.events.len(),5);
 }
 #[test] fn wrong_owner_and_bad_market_data_fail_closed() {
  let other=owner_index("BTC",3); let mut c=Owner::new((other+1)%3,3,1000).unwrap();
  let mut h=History::default();
  assert_eq!(c.ingest(&b("BTC","1.10",1,1,1),&mut h),Err(CoreError::WrongOwner));
  assert!(h.events.is_empty());
  let mut c=Owner::new(other,3,1000).unwrap();
  let mut bad=b("BTC","NaN",1,1,1);
  assert_eq!(c.ingest(&bad,&mut h),Err(CoreError::NonFinite));
  assert!(h.events.is_empty());
  bad.raw="not json".to_owned();
  assert_eq!(c.ingest(&bad,&mut h),Err(CoreError::Invalid("json")));
  assert!(h.events.is_empty());
 }
 #[test] fn stale_pair_and_decreasing_sequence_never_decide() {
  let mut c=Owner::new(owner_index("BTC",3),3,800).unwrap();let mut h=History::default();
  c.ingest(&o("BTC","1.00",1,1),&mut h).unwrap();
  assert!(c.ingest(&b("BTC","1.10",1,900_000_002,1),&mut h).unwrap().is_none());
  assert_eq!(c.ingest(&b("BTC","1.10",1,900_000_003,1),&mut h),Err(CoreError::OutOfOrder));
  assert_eq!(h.events.len(),2);
 }
 #[test] fn both_venue_reconnects_invalidate_pairs_and_reject_old_buffered_frames() {
  let mut c=Owner::new(owner_index("BTC",3),3,1000).unwrap();
  let mut h=History::default();
  assert!(c.ingest(&b("BTC","1.10",1,1,1),&mut h).unwrap().is_none());
  assert!(c.ingest(&o("BTC","1.00",2,1),&mut h).unwrap().is_some());
  c.reset_venue("BTC",Venue::BinanceFutures,2).unwrap();
  assert!(c.snapshot_pair("BTC").1.is_none());
  let mut okx_live=o("BTC","1.01",3,1);
  okx_live.raw=okx_live.raw.replace("\"ts\":\"1000\"", "\"ts\":\"1001\"");
  assert!(c.ingest(&okx_live,&mut h).unwrap().is_none());
  assert_eq!(c.ingest(&b("BTC","1.10",2,4,1),&mut h),Err(CoreError::StaleEpoch));
  assert_eq!(c.ingest(&b("BTC","1.10",2,5,2),&mut h),Err(CoreError::OutOfOrder));
  let mut binance_live=b("BTC","1.11",2,6,2);
  binance_live.raw=binance_live.raw.replace("\"E\":1000", "\"E\":1002");
  assert!(c.ingest(&binance_live,&mut h).unwrap().is_some());
  c.reset_venue("BTC",Venue::OkxSpot,2).unwrap();
  assert!(c.snapshot_pair("BTC").0.is_none());
  assert_eq!(c.ingest(&okx_live,&mut h),Err(CoreError::StaleEpoch));
  let mut stale_okx=o("BTC","1.01",7,2);
  stale_okx.raw=stale_okx.raw.replace("\"ts\":\"1000\"", "\"ts\":\"1001\"");
  assert_eq!(c.ingest(&stale_okx,&mut h),Err(CoreError::OutOfOrder));
  stale_okx.raw=stale_okx.raw.replace("\"ts\":\"1001\"", "\"ts\":\"1003\"");
  assert!(c.ingest(&stale_okx,&mut h).unwrap().is_some());
  assert_eq!(h.events.len(),5);
 }
 #[test] fn reset_rebind_only_accepts_exact_authenticated_book_state() {
  let mut owner=Owner::new(owner_index("BTC",3),3,800).unwrap();
  let mut history=History::default();
  let raw=r#"{"arg":{"channel":"books5","instId":"BTC-USDT"},"data":[{"bids":[["0.90","2","0","3"]],"asks":[["1.00","2","0","3"]],"ts":"1000","seqId":12}]}"#.to_string();
  let make_frame=|body:&str,session:&str,epoch:u64| {
   let payload_sha256=openssl::sha::sha256(body.as_bytes()).iter()
    .map(|v|format!("{v:02x}")).collect::<String>();
   RawFrame{source:Some(SourceProvenance{selected_permit:None,frame_trace_id:"fixture:a:1".into(),journal_id:"a".repeat(64),accepted_id:1,
    session_id:session.into(),connection_epoch:1,selection_epoch:epoch,payload_sha256,
    transport:None,accepted_ns:Some(2),selected_ns:Some(2),prepared:None,r1247_timing:None,r1279_timing:None}),
    venue:Venue::OkxSpot,epoch,received_ns:2,received_wall_ms:1000,raw:body.into()}
  };
  let old=RawFrame{source:Some(SourceProvenance{selected_permit:None,frame_trace_id:"fixture:b:1".into(),journal_id:"b".repeat(64),accepted_id:1,
    session_id:"active".into(),connection_epoch:1,selection_epoch:1,
    payload_sha256:openssl::sha::sha256(raw.as_bytes()).iter().map(|v|format!("{v:02x}")).collect(),
    transport:None,accepted_ns:Some(1),selected_ns:Some(1),prepared:None,r1247_timing:None,r1279_timing:None}),
    venue:Venue::OkxSpot,epoch:1,received_ns:1,received_wall_ms:1000,raw:raw.clone()};
  owner.ingest(&old,&mut history).unwrap();
  owner.reset_venue("BTC",Venue::OkxSpot,2).unwrap();
  assert_eq!(owner.ingest(&make_frame(&raw,"active",2),&mut history),Err(CoreError::OutOfOrder));
  let changed=raw.replace("0.90","0.91");
  assert_eq!(owner.ingest(&make_frame(&changed,"standby",2),&mut history),Err(CoreError::OutOfOrder));
  assert!(owner.ingest(&make_frame(&raw,"standby",2),&mut history).unwrap().is_none());
  assert_eq!(owner.source_binding("BTC",Venue::OkxSpot),Some(("standby".into(),2)));
  assert_eq!(owner.snapshot_venue("BTC",Venue::OkxSpot).unwrap().sequence,Some(12));
 }
 #[test] fn stale_epoch_reconnect_and_history_pressure() {
  let mut c=Owner::new(owner_index("BTC",3),3,1000).unwrap();let mut h=History::default();
  c.ingest(&b("BTC","1.10",1,1,1),&mut h).unwrap();
  c.ingest(&o("BTC","1.00",2,1),&mut h).unwrap();
  c.reset_venue("BTC",Venue::BinanceFutures,2).unwrap();
  assert_eq!(c.ingest(&b("BTC","1.10",2,3,1),&mut h),Err(CoreError::StaleEpoch));
  let fresh=|ns| {let mut f=b("BTC","1.10",2,ns,2);
   f.received_wall_ms=1001;f.raw=f.raw.replace("\"E\":1000", "\"E\":1001");f};
  let old_source=b("BTC","1.10",2,4,2);
  assert_eq!(c.ingest(&old_source,&mut h),Err(CoreError::OutOfOrder));
  h.full=true;assert_eq!(c.ingest(&fresh(5),&mut h),Err(CoreError::HistoryPressure));
  h.full=false;assert!(c.ingest(&fresh(6),&mut h).unwrap().is_some());
  assert_eq!(c.ingest(&fresh(7),&mut h),Err(CoreError::OutOfOrder));
  assert_eq!(h.events.len(),3);
 }
}

pub mod ring;

pub mod mpsc_ring;

pub mod decision;

pub mod safety;
pub mod execution;
pub mod execution_adapter;
pub mod execution_store;
pub mod fill_economics;
pub mod market_metadata;
pub mod r1246_dual_archive;
pub mod async_evidence;
pub mod compressed_evidence;
pub mod position_lifecycle;
pub mod private_rest;
pub mod private_https;
pub mod private_truth;
pub mod native_entry_recovery;
pub mod native_history_audit;
pub mod native_cash_lineage;
pub mod native_binance_lineage;
pub mod native_futures_trade_scope;
pub mod native_settled_spot_audit;
pub mod native_joint_window;
pub mod native_hard_safety;
pub mod native_spot_prehedge;
pub mod native_spot_prehedge_bills;
pub mod native_futures_entry_audit;
pub mod native_prospective_joint;
pub mod native_trading_config;
pub mod native_public_market_gate_review;
pub mod native_observability_review;
pub mod native_telegram_durable_outbox;
pub mod native_telegram_delivery_review;
pub mod native_paper_telegram_observer;
pub mod native_paper_pause_review;
pub mod native_paper_telegram_shards_review;
pub mod native_paper_oldstyle_notice_review;
pub mod native_paper_daily_calendar_review;
pub mod native_paper_notice_queue_review;
pub mod native_paper_telegram_dispatch_review;
pub mod native_paper_telegram_delivery_review;
pub mod native_paper_service_lifecycle_review;
pub mod native_paper_config_bridge;
pub mod native_daily_risk;
pub mod native_real_risk_watch;
pub mod native_okx_fee_day_r208_review;
pub mod native_okx_fee_checkpoint_bridge_r209_review;
pub mod native_okx_close_fee_archive_r210_review;
pub mod native_okx_persisted_fee_census_r211_review;
pub mod native_close_readonly;
pub mod native_close_window;
pub mod native_close_diagnostic;
pub mod native_entry_origin;
pub mod native_origin_spot_link;
pub mod native_origin_dual_open;
pub mod native_origin_close_fence;
pub mod native_origin_close_receipt;
pub mod native_origin_close_account;
pub mod native_origin_lifetime_economics;
pub mod native_trade2f_economics;
pub mod native_trade2f_income_window;
pub mod native_trade2f_receipt_binding;
pub mod native_trade2f_asset_fee;
pub mod native_trade2f3_fee_adjusted_cycle_gate;
pub mod native_trade2f3_owner_fee_bridge;
pub mod native_trade2f3_fee_aware_first_leg_v2;
pub mod native_trade2f3_first_leg_v2_checkpoint;
pub mod native_trade2f3_v2_net_hedge_contract;
pub mod native_trade2f3_v2_bill_ancestry;
pub mod native_trade2f3_v2_owner_genesis;
pub mod native_trade2f3_v2_versioned_store;
pub mod native_trade2f3_v2_c3_futures_evidence;
pub mod native_trade2f3_v2_four_leg_close_review;
pub mod native_trade2f3_v2_emergency_close;
pub mod native_trade2f3_v2_c3_opening_recovery;
pub mod native_trade2f3_v2_revision_journal;
pub mod native_trade2f3_v2_signed_floor;
pub mod native_trade2f3_v2_witness_mock_store;
pub mod native_trade2f3_v2_three_way_gate;
pub mod native_trade2f3_v2_durable_ack_review;
pub mod native_trade2f3_v2_remote_cas_contract;
pub mod native_paper_market_execution_review;
pub mod native_paper_portfolio_review;
pub mod native_paper_risk_lifecycle_r1287;
pub mod native_paper_execution_r1289;
pub mod native_legacy_scanner_cooldown_review;
pub mod native_public_turnover_source_review;
pub mod native_field_consumer_matrix_r118_review;
pub mod native_public_turnover_continuity_r122_review;
pub mod native_public_turnover_ws_r123_review;
pub mod native_public_turnover_ws_continuity_r124_review;
pub mod native_public_ws_owner_topology_r126_review;
pub mod native_public_cold_boot_r130_review;
pub mod native_config_overlay_r128_review;
pub mod native_db2_mirror_status_r136_review;
pub mod native_db2_owner_topology_r138_review;
pub mod native_mirror_outbox_protocol_r139_review;
pub mod native_db2_cross_status_r140_review;
pub mod native_mirror_control_r142_review;
pub mod native_isolated_mirror_journal_r143_review;
pub mod native_isolated_sqlite_mirror_r147_review;
pub mod native_paper_partial_recovery_gate;
pub mod native_paper_durable_journal_review;
pub mod native_paper_owner_transaction_review;
pub mod native_paper_fee_hedge_sizing_review;
pub mod native_paper_joint_binance_lots_review;
pub mod native_paper_v2_closure_bridge_review;
pub mod native_r15_original_owner_binding_review;
pub mod native_r15_cross_store_cold_review;
pub mod native_r14_r15_cross_store_cold_review;
pub mod native_r14_origination_claim_review;
pub mod native_paper_v2_cold_files_review;
pub mod native_paper_v2_close_source_archive_review;
pub mod native_paper_v2_pair_witness_review;
pub mod native_paper_continuity_checkpoint_review;
pub mod native_r1062_sealed_stop_bundle_review;
pub mod native_paper_owner_virtual_alignment_review;
pub mod native_virtual_four_leg_provenance_review;
pub mod native_trade2f_okx_bill_topology;
pub mod native_trade2f_owner_binding;
pub mod native_trade2f_okx_owner_mock;
pub mod native_trade2f_post_settlement_guard;
pub mod native_trade2f_binance_late_income;
pub mod native_trade2f3_unified_offline_gate;
pub mod native_trade2f3_source_provenance;
pub mod native_trade2f3_private_read_plan;
pub mod native_futures_uncertain_quarantine;
pub mod native_pre_entry_binance_baseline;
pub mod native_wallet_income_bridge;
pub mod native_pre_entry_income_gap;
pub mod native_micro_trade_size;
pub mod native_supervised_recovery;
pub mod native_supervised_recovery_live;
pub mod native_supervised_recovery_fills;
pub mod native_supervised_close_plan;
pub mod native_recovery_dryrun_latch;
pub mod native_dryrun_close_status;
pub mod native_dryrun_close_fills;
pub mod native_dryrun_close_accounts;
pub mod native_dryrun_pending_algos;
pub mod native_dryrun_algo_history;
pub mod native_dryrun_child_lineage;
pub mod native_dryrun_multi_child;
pub mod native_ai_advisor_review;
pub mod native_ai_input_review;
pub mod native_ai_storage_review;
pub mod native_ai_economics_review;
pub mod native_paper_ai_bridge_review;
pub mod native_paper_ingest_quality_review;
pub mod native_shadow_config_review;
pub mod native_public_feed_policy_review;
pub mod native_candidate_holdoff_review;
pub mod native_ai_output_boundary_review;
pub mod native_paper_report_schedule_review;
pub mod native_paper_hedge_quality_review;
pub mod native_paper_config_drift_review;
pub mod native_paper_model_config_review;
pub mod native_legacy_strategy_policy_review;
pub mod native_legacy_adaptive_entry_review;
pub mod native_paper_strategy_frame_review;
pub mod native_source_paper_checkpoint_r153_review;
pub mod native_r155_source_paper_lane_review;
pub mod native_build_provenance_r157_review;
pub mod native_r167_source_ws_shards_review;
pub mod native_binance_ws_ip_guard_r1110;
pub mod native_r1113_offline_supervisor_join;
pub mod native_r1114_quarantine_telegram_bridge;
pub mod native_r1115_verified_model_telegram_bridge;
pub mod native_r1116_durable_model_notice;
pub mod native_r168_strategy_source_review;
pub mod native_r169_source_metadata_review;
pub mod native_r173_config_operational_preview;
pub mod native_r174_unwired_legacy_guard;
pub mod native_r176_source_runtime_manifest_review;
pub mod native_release_identity_r617_review;
pub mod native_r178_candidate_source_identity_review;
pub mod native_r258_source_admission_diagnostics;
pub mod native_r286_python_rust_consumers_review;

#[cfg(test)]
mod r161_usdm_merge_guard_tests{
 use super::*;
 fn frame(event:Value)->RawFrame{
  RawFrame{source:None,venue:Venue::BinanceFutures,epoch:1,
   received_ns:1000,received_wall_ms:1_791_000_000_000,
   raw:serde_json::json!({"stream":"btcusdt@depth20@100ms",
      "data":event}).to_string()}
 }
 #[test]
 fn r161_depth_accepts_usdm_but_rejects_coinm_and_wrong_pair(){
  let x=serde_json::json!({"e":"depthUpdate","s":"BTCUSDT",
   "st":1,"ps":"BTCUSDT","E":1_791_000_000_000_i64,
   "U":10,"u":11,"pu":9,
   "b":[["100","2"]],"a":[["101","2"]]});
  assert_eq!(parse(&frame(x.clone())).unwrap().base,"BTC");
  let mut cm=x.clone();cm["st"]=serde_json::json!(2);
  assert_eq!(parse(&frame(cm)),Err(CoreError::Invalid("non-USD-M Binance source")));
  let mut ps=x.clone();ps["ps"]=serde_json::json!("BTCUSD");
  assert_eq!(parse(&frame(ps)),Err(CoreError::Invalid("Binance pair/symbol mismatch")));
  let mut invalid=x.clone();invalid["st"]=serde_json::json!("1");
  assert_eq!(parse(&frame(invalid)),Err(CoreError::Invalid("non-USD-M Binance source")));
  let mut legacy=x;legacy.as_object_mut().unwrap().remove("st");
  legacy.as_object_mut().unwrap().remove("ps");
  assert!(parse(&frame(legacy)).is_ok());
 }
 #[test]
 fn r161_bookticker_cannot_smuggle_coinm_into_usdm_owner(){
  let x=serde_json::json!({"e":"bookTicker","s":"BTCUSDT",
   "st":2,"ps":"BTCUSD","b":"100","B":"2","a":"101","A":"2",
   "E":1_791_000_000_000_i64,"u":12});
  assert_eq!(parse(&frame(x)),Err(CoreError::Invalid("non-USD-M Binance source")));
 }
}

#[cfg(test)]
mod r1121_okx_books5_snapshot_tests{
 use super::*;
 fn book(seq:i64,ts:i64,ns:u64,wall:i64)->Book{
  Book{bids:vec![(100.,2.)],asks:vec![(101.,2.)],
       received_ns:ns,received_wall_ms:wall,
       exchange_ts_ms:Some(ts),changed_ns:ns,
       epoch:1,sequence:Some(seq)}
 }
 fn newer(seq:i64,ts:i64)->(RawFrame,Update){
  let raw=serde_json::json!({
    "arg":{"channel":"books5","instId":"SONIC-USDT"},
    "data":[{"bids":[["100","2","0","1"]],
             "asks":[["101","2","0","1"]],
             "ts":ts.to_string(),"seqId":seq}]
  }).to_string();
  let frame=RawFrame{source:None,venue:Venue::OkxSpot,epoch:1,
     received_ns:2000,received_wall_ms:ts+150,raw};
  let update=parse(&frame).unwrap();
  (frame,update)
 }
 #[test]
 fn r1121_allows_only_newer_full_book_snapshot_after_sequence_maintenance_reset(){
  let prior=book(3043880556,1793074615601,1000,1793074615750);
  let (frame,update)=newer(3,1793074680001);
  assert!(r1121_okx_newer_books5_snapshot(&frame,&update,&prior,true));
  assert!(!r1121_okx_newer_books5_snapshot(&frame,&update,&prior,false));
  let (same_frame,same_update)=newer(3043880556,1793074680001);
  assert!(r1121_okx_newer_books5_snapshot(
    &same_frame,&same_update,&prior,true),
    "fresh full snapshots can retain the same OKX seqId");
  let (old_frame,old_update)=newer(3,1793074615500);
  assert!(!r1121_okx_newer_books5_snapshot(
    &old_frame,&old_update,&prior,true));
 }
 #[test]
 fn r1121_cannot_relax_incremental_or_wrong_exchange_or_false_timestamp(){
  let prior=book(3043880556,1793074615601,1000,1793074615750);
  let (mut frame,update)=newer(3,1793074680001);
  let mut v:Value=serde_json::from_str(&frame.raw).unwrap();
  v["arg"]["channel"]=serde_json::json!("books");
  frame.raw=v.to_string();
  assert!(!r1121_okx_newer_books5_snapshot(&frame,&update,&prior,true));
  let (mut frame,update)=newer(3,1793074680001);
  frame.venue=Venue::BinanceFutures;
  assert!(!r1121_okx_newer_books5_snapshot(&frame,&update,&prior,true));
  let (mut frame,update)=newer(3,1793074680001);
  let mut v:Value=serde_json::from_str(&frame.raw).unwrap();
  v["data"][0]["prevSeqId"]=serde_json::json!(42);
  frame.raw=v.to_string();
  assert!(!r1121_okx_newer_books5_snapshot(&frame,&update,&prior,true));
 }
}

pub mod native_prepared_exit_policy_review;
pub mod native_exit_safety_config_review;
pub mod native_paper_leg_failure_review;
pub mod native_paper_r71_restart_recovery_review;
// R1171: isolated file-mirrored compact book snapshot experiment, never live.
pub mod native_r1171_compact_book_checkpoint;
// R1172 offline SQLite reader: no production sink or REAL permit.
pub mod native_r1172_compact_mirror_reader;
// R1173 native private compact SQLite writer, isolated from PAPER service.
pub mod native_r1173_compact_writer;
// R1186 independent cross-language PAPER arithmetic oracle, tests only.
#[cfg(test)] mod native_r1186_independent_oracle_review;
pub mod authoritative_state;

pub mod transport;

pub mod history;

pub mod market_data_ingress_r1205;
pub mod market_data_processor_r1378;
pub mod ingress_runtime_r1205;
pub mod r1352_diagnostic_worker;
pub mod r1354_deadline_scheduler;
pub mod universal_connection_manager_r1209;
pub mod okx_l2_book_r1211;

pub mod ingress_transport_r1205;

pub mod quality_manager_r1195;

pub mod ingress_source_only_r1205;
pub mod custody_executor_r1241;
pub mod r1241_ingress_pipeline;
#[cfg(test)]
mod r1279_replay_overhead_tests {
    use super::*;
    use std::time::Instant;

    #[derive(Default)]
    struct ReplayHistory(u64);
    impl HistorySink for ReplayHistory {
        fn accept(&mut self,_:&RawFrame,_:&Update)->Result<(),CoreError>{self.0+=1;Ok(())}
    }
    fn cpu_us()->u64{
        let mut r:libc::rusage=unsafe{std::mem::zeroed()};
        unsafe{libc::getrusage(libc::RUSAGE_SELF,&mut r)};
        ((r.ru_utime.tv_sec+r.ru_stime.tv_sec)as u64)*1_000_000+
            (r.ru_utime.tv_usec+r.ru_stime.tv_usec)as u64
    }
    fn rss_kib()->u64{
        std::fs::read_to_string("/proc/self/status").ok().and_then(|s|s.lines().find_map(|l|l.strip_prefix("VmRSS:").and_then(|v|v.split_whitespace().next()).and_then(|n|n.parse().ok()))).unwrap_or(0)
    }
    fn run(trace:bool)->(f64,Vec<u64>,u64,u64,u64){
        let mut owner=Owner::new(0,1,800).unwrap();let mut history=ReplayHistory::default();
        let mut service=Vec::with_capacity(4096);let cpu0=cpu_us();let rss0=rss_kib();let wall=Instant::now();
        for n in 1..=4096u64{
            let raw=format!(r#"{{"stream":"btcusdt@bookTicker","data":{{"e":"bookTicker","s":"BTCUSDT","b":"100.00","B":"2","a":"100.10","A":"2","u":{},"E":1700000000000}}}}"#,n);
            let hash=openssl::sha::sha256(raw.as_bytes()).iter().map(|b|format!("{b:02x}")).collect::<String>();
            let mut frame=RawFrame{venue:Venue::BinanceFutures,epoch:1,received_ns:n,received_wall_ms:1700000000000,raw,source:Some(SourceProvenance{selected_permit:None,frame_trace_id:format!("replay:{n}"),journal_id:"a".repeat(64),accepted_id:n,session_id:"replay-session".into(),connection_epoch:1,selection_epoch:1,payload_sha256:hash,transport:None,accepted_ns:Some(n),selected_ns:Some(n),prepared:None,r1247_timing:None,r1279_timing:None})};
            let started=Instant::now();
            if trace{
                let base=crate::ingress_runtime_r1205::mono_ns();
                let mut t=R1279FrameTiming{clock_domain_id:Some("replay".into()),instrument:Some("BTC".into()),exchange_event_ts_ms:Some(1700000000000),ws_receive_ns:Some(base),..Default::default()};
                t.ws_decode_end_ns=Some(crate::ingress_runtime_r1205::mono_ns());t.reader_enqueue_ns=Some(crate::ingress_runtime_r1205::mono_ns());t.reader_dequeue_ns=Some(crate::ingress_runtime_r1205::mono_ns());
                t.central_enqueue_ns=Some(crate::ingress_runtime_r1205::mono_ns());t.central_dequeue_ns=Some(crate::ingress_runtime_r1205::mono_ns());t.validation_start_ns=Some(crate::ingress_runtime_r1205::mono_ns());
                t.book_start_ns=Some(crate::ingress_runtime_r1205::mono_ns());
                frame.source.as_mut().unwrap().frame_trace_id=format!("replay:{}",n);frame.source.as_mut().unwrap().r1279_timing=Some(t);
            }
            let parsed=parse(&frame).unwrap();owner.ingest(&frame,&mut history).expect("deterministic replay frame accepted");
            if trace{let end=crate::ingress_runtime_r1205::mono_ns();let t=frame.source.as_mut().unwrap().r1279_timing.as_mut().unwrap();t.validation_end_ns=Some(end);t.book_end_ns=Some(end);t.engine_receive_begin_ns=Some(end);t.engine_receive_end_ns=Some(end);t.terminal_ns=Some(end);if n%64==0{let row=r1279_trace_row(&frame,"replayed","not_applicable_replay");std::hint::black_box(serde_json::to_vec(&row).unwrap());}}
            std::hint::black_box(parsed);service.push(started.elapsed().as_nanos()as u64);
        }
        let elapsed=wall.elapsed().as_secs_f64();let cpu=cpu_us()-cpu0;let rss=rss_kib().saturating_sub(rss0);
        (elapsed,service,cpu,rss,history.0)
    }
    fn percentile(v:&mut [u64],q:f64)->u64{v.sort_unstable();v[((v.len()-1)as f64*q).round()as usize]}
    #[test]
    fn trace_off_on_identical_replay_emits_comparable_profile(){
        let off=run(false);let on=run(true);
        let mut a=off.1.clone();let mut b=on.1.clone();
        let out=serde_json::json!({
            "schema":"R1279/off-on-replay-v1","frames_off":off.4,"frames_on":on.4,"input_frames_per_mode":4096,
            "off":{"throughput_fps":4096.0/off.0,"cpu_us":off.2,"rss_delta_kib":off.3,"service_p50_ns":percentile(&mut a,0.50),"service_p95_ns":percentile(&mut a,0.95),"service_p99_ns":percentile(&mut a,0.99),"service_max_ns":*a.last().unwrap()},
            "on":{"throughput_fps":4096.0/on.0,"cpu_us":on.2,"rss_delta_kib":on.3,"service_p50_ns":percentile(&mut b,0.50),"service_p95_ns":percentile(&mut b,0.95),"service_p99_ns":percentile(&mut b,0.99),"service_max_ns":*b.last().unwrap()},
            "scope":"deterministic parser+book replay plus trace timestamp/row serialization; no network, queue wait, or durable disk commit"
        });
        println!("R1279_REPLAY_OVERHEAD {}",out);
        assert_eq!((off.4,on.4),(4096,4096));
    }
}

#[cfg(test)]
mod r1286_binance_depth20_validation_tests {
 use super::*;
 #[derive(Default)] struct TestHistory;
 impl HistorySink for TestHistory { fn accept(&mut self,_:&RawFrame,_:&Update)->Result<(),CoreError>{Ok(())} }
 fn frame(bids:Value, asks:Value)->RawFrame {
  let data=serde_json::json!({"e":"depthUpdate","s":"BTCUSDT","E":1000,"u":7,"b":bids,"a":asks});
  RawFrame{source:None,venue:Venue::BinanceFutures,epoch:1,received_ns:10,received_wall_ms:1000,
   raw:serde_json::json!({"stream":"btcusdt@depth20@100ms","data":data}).to_string()}
 }
 fn good()->RawFrame { frame(serde_json::json!([["102","1"],["101","2"]]),serde_json::json!([["103","1"],["104","2"]])) }
 #[test] fn valid_partial_depth20_snapshot_is_retained_in_exchange_order(){
  let u=parse(&good()).unwrap();assert_eq!(u.book.bids[0].0,102.);assert_eq!(u.book.asks[0].0,103.);
 }
 #[test] fn rejects_unsorted_crossed_locked_duplicate_nonfinite_and_bad_levels(){
  let cases=[
   frame(serde_json::json!([["101","1"],["102","1"]]),serde_json::json!([["103","1"]])),
   frame(serde_json::json!([["102","1"]]),serde_json::json!([["104","1"],["103","1"]])),
   frame(serde_json::json!([["103","1"]]),serde_json::json!([["103","1"]])),
   frame(serde_json::json!([["104","1"]]),serde_json::json!([["103","1"]])),
   frame(serde_json::json!([["102","1"],["102","2"]]),serde_json::json!([["103","1"]])),
   frame(serde_json::json!([["NaN","1"]]),serde_json::json!([["103","1"]])),
   frame(serde_json::json!([["Infinity","1"]]),serde_json::json!([["103","1"]])),
   frame(serde_json::json!([["-1","1"]]),serde_json::json!([["103","1"]])),
   frame(serde_json::json!([["102","0"]]),serde_json::json!([["103","1"]])),
   frame(serde_json::json!([["102","-1"]]),serde_json::json!([["103","1"]])),
   frame(serde_json::json!([["102"]]),serde_json::json!([["103","1"]])),
   frame(serde_json::json!([]),serde_json::json!([["103","1"]]))
  ];
  for f in cases { assert!(parse(&f).is_err(),"invalid depth20 admitted: {}",f.raw); }
 }
 #[test] fn valid_snapshot_recovers_after_rejected_depth20_frame(){
  let mut owner=Owner::new(owner_index("BTC",3),3,800).unwrap();let mut h=TestHistory;
  let bad=frame(serde_json::json!([["101","1"],["102","1"]]),serde_json::json!([["103","1"]]));
  assert!(parse(&bad).is_err());assert_eq!(owner.snapshot_pair("BTC"),(None,None));
  let good=good();let update=parse(&good).unwrap();owner.ingest_parsed(&good,update,&mut h).unwrap();
  assert!(owner.snapshot_pair("BTC").1.is_some());
 }
 #[test] fn direct_owner_pair_rejects_missing_timestamp_then_recovers(){
  let mut owner=Owner::new(owner_index("BTC",3),3,800).unwrap();let mut h=TestHistory;
  let mk=|venue,ts,ns| {let book=Book{bids:vec![(102.,1.)],asks:vec![(103.,1.)],received_ns:ns,
    received_wall_ms:1000,exchange_ts_ms:ts,changed_ns:ns,epoch:1,sequence:Some(ns as i64)};
    let frame=RawFrame{source:None,venue,epoch:1,received_ns:ns,received_wall_ms:1000,raw:String::new()};
    (frame,Update{base:"BTC".into(),venue,book})};
  let (spot_f,spot)=mk(Venue::OkxSpot,None,1);let (future_f,future)=mk(Venue::BinanceFutures,Some(1000),2);
  assert!(owner.ingest_parsed(&spot_f,spot,&mut h).unwrap().is_none());
  assert!(owner.ingest_parsed(&future_f,future,&mut h).unwrap().is_none());
  let (spot_f,spot)=mk(Venue::OkxSpot,Some(1000),3);
  assert!(owner.ingest_parsed(&spot_f,spot,&mut h).unwrap().is_some());
 }
}

pub mod native_paper_hedge_r1296;

pub mod native_paper_exit_r1298;

pub mod native_paper_bridge_r1301;

#[cfg(test)]
mod r1323_whole_universe;

pub mod pipeline_diagnostics_r1325;

pub mod native_paper_reconciliation_r1324a;
