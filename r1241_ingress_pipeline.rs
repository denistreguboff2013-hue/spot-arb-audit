use crate::{
    custody_executor_r1241::CustodyClient,
    market_data_ingress_r1205::{authorization_at_normalized, Authority},
    native_r1173_compact_writer::{
        ingress_store_r1205::StoreWorker, Inject,
    },
    ingress_runtime_r1205::{mono_ns, wall_ms},
    Owner, RawFrame, Update, Venue,
};
use tokio::sync::mpsc;

pub const R1241_CANONICAL_INPUT_CAPACITY: usize = 64;
const BATCH_TARGET: usize = 16;
const BATCH_MAX: usize = 32;
const BATCH_DELAY_MS: u64 = 75;

#[derive(Debug, Clone)]
pub struct CanonicalFrame {
    pub frame: RawFrame,
    pub update: Update,
    pub owner_index: usize,
}

pub type DeferredCustodyCommit = Box<
    dyn FnOnce(&mut crate::native_r1173_compact_writer::Writer) -> Result<u64, String> + Send,
>;

pub struct DecisionSeal {
    pub frame: RawFrame,
    pub result_hash: String,
    pub pre_decision_commit: Option<DeferredCustodyCommit>,
}

#[derive(Debug)]
pub enum Command {
    Frame(CanonicalFrame),
    Quarantine { frame: RawFrame, reason: String },
    Reset { request_id: u64, venue: Venue, bases: Vec<String>, epoch: u64 },
    Shutdown,
}

pub enum Control {
    FinalizeBatch { batch_id: u64, decisions: Vec<DecisionSeal> },
    ResetApplied { request_id: u64 },
}

#[derive(Debug)]
pub enum Event {
    BatchReady { batch_id: u64, source_seq: u64, frames: Vec<CanonicalFrame> },
    BatchAcked {
        batch_id: u64,
        transition_seqs: Vec<u64>,
        decision_seq: Option<u64>,
        release_seq: Option<u64>,
        frame_count: usize,
    },
    ResetReady { request_id: u64, seq: u64, venue: Venue, bases: Vec<String>, epoch: u64 },
    Stopped { profile: serde_json::Value },
    Failed(String),
}

#[derive(Default)]
struct PipelineCounters {
    input_frames: u64,
    quarantined_frames: u64,
    reset_events: u64,
    committed_batches: u64,
    committed_frames: u64,
    acknowledged_frames: u64,
    batch_size_max: usize,
    quarantine_batches: u64,
    quarantine_batch_frames_max: usize,
    trace_samples:std::collections::VecDeque<serde_json::Value>,
    trace_dropped:u64,
    trace_sampled_out:u64,
    queue_started_ns:u64,
    slowest_internal_ns:u64,
    slowest_trace:Option<serde_json::Value>,
    queue_enqueued_start:u64,
    queue_dequeued:u64,
    queue_overflow_start:u64,
}
impl PipelineCounters {
    fn record_trace(&mut self,frame:&RawFrame,terminal:&str,ack:&str){
        let Some(timing)=frame.source.as_ref().and_then(|s|s.r1279_timing.as_ref())else{return};
        if let Some(duration)=timing.ws_receive_ns.zip(timing.terminal_ns).and_then(|(a,b)|b.checked_sub(a)){if duration>self.slowest_internal_ns{self.slowest_internal_ns=duration;self.slowest_trace=Some(crate::r1279_trace_row(frame,terminal,ack));}}
        if frame.source.as_ref().is_some_and(|s|s.accepted_id%64!=0){self.trace_sampled_out=self.trace_sampled_out.saturating_add(1);return;}
        if self.trace_samples.len()>=4096 {self.trace_samples.pop_front();self.trace_dropped=self.trace_dropped.saturating_add(1);}
        self.trace_samples.push_back(crate::r1279_trace_row(frame,terminal,ack));
    }
    fn snapshot(&self) -> serde_json::Value {
        let elapsed_ns=mono_ns().saturating_sub(self.queue_started_ns).max(1);
        let enqueued=CANONICAL_ENQUEUED.load(std::sync::atomic::Ordering::Relaxed).saturating_sub(self.queue_enqueued_start);
        serde_json::json!({
            "schema":"SPOTARB/R1241/ASYNC-CANONICAL-INGRESS/V1",
            "input_frames":self.input_frames,
            "quarantined_frames":self.quarantined_frames,
            "reset_events":self.reset_events,
            "committed_batches":self.committed_batches,
            "committed_frames":self.committed_frames,
            "acknowledged_frames":self.acknowledged_frames,
            "queue_capacity":R1241_CANONICAL_INPUT_CAPACITY,
            "batch_size_max":self.batch_size_max,
            "quarantine_batches":self.quarantine_batches,
            "quarantine_batch_frames_max":self.quarantine_batch_frames_max,
            "queue_overflow_policy":"fail_closed_no_ack",
            "source_ack_before_decision":true,
            "decision_seal_before_inbox_publish":true,
            "inbox_publish_before_reclaim_ack":true,
            "trace_samples":self.trace_samples,
            "trace_sample_capacity":4096,
            "trace_dropped":self.trace_dropped,
            "trace_sampled_out":self.trace_sampled_out,
            "slowest_trace":self.slowest_trace,
            "slowest_internal_ns":self.slowest_internal_ns,
            "queue_elapsed_ns":elapsed_ns,
            "queue_enqueue_rate_fps":(enqueued as f64)*1_000_000_000.0/(elapsed_ns as f64),
            "queue_dequeue_rate_fps":(self.queue_dequeued as f64)*1_000_000_000.0/(elapsed_ns as f64),
            "queue_enqueued":enqueued,
            "queue_dequeued":self.queue_dequeued,
            "queue_highwater":CANONICAL_HIGHWATER.load(std::sync::atomic::Ordering::Relaxed),
            "queue_current_depth":0,
            "queue_overflow_fail_closed":CANONICAL_OVERFLOW.load(std::sync::atomic::Ordering::Relaxed).saturating_sub(self.queue_overflow_start)
        })
    }
}

static CANONICAL_OVERFLOW:std::sync::atomic::AtomicU64=std::sync::atomic::AtomicU64::new(0);
static CANONICAL_ENQUEUED:std::sync::atomic::AtomicU64=std::sync::atomic::AtomicU64::new(0);
static CANONICAL_DEPTH:std::sync::atomic::AtomicUsize=std::sync::atomic::AtomicUsize::new(0);
static CANONICAL_HIGHWATER:std::sync::atomic::AtomicUsize=std::sync::atomic::AtomicUsize::new(0);
pub fn submit(sender: &mpsc::Sender<Command>, mut command: Command) -> Result<(), String> {
    let tracks_frame=matches!(&command,Command::Frame(_)|Command::Quarantine{..});
    let capacity=sender.max_capacity();let depth=capacity.saturating_sub(sender.capacity()).saturating_add(1);
    let frame=match &mut command {Command::Frame(item)=>Some(&mut item.frame),Command::Quarantine{frame,..}=>Some(frame),_=>None};
    if let Some(frame)=frame {crate::ensure_frame_trace_id(frame);if let Some(t)=frame.source.as_mut().and_then(|s|s.r1279_timing.as_mut()){t.canonical_enqueue_ns=Some(mono_ns());t.canonical_queue_capacity=Some(capacity);t.canonical_queue_depth=Some(depth);}}
    sender.try_send(command).map(|_|{if tracks_frame{CANONICAL_ENQUEUED.fetch_add(1,std::sync::atomic::Ordering::Relaxed);let d=CANONICAL_DEPTH.fetch_add(1,std::sync::atomic::Ordering::Relaxed).saturating_add(1);CANONICAL_HIGHWATER.fetch_max(d,std::sync::atomic::Ordering::Relaxed);}}).map_err(|error| match error {
        mpsc::error::TrySendError::Full(_) => {CANONICAL_OVERFLOW.fetch_add(1,std::sync::atomic::Ordering::Relaxed);"R1241 bounded canonical ingress queue full; fail closed".into()},
        mpsc::error::TrySendError::Closed(_) => "R1241 canonical ingress queue closed; fail closed".into(),
    })
}

async fn flush_quarantine_batch(
    pending: &mut Vec<(RawFrame, String)>,
    store_worker: &StoreWorker,
    counters: &mut PipelineCounters,
) -> Result<(), String> {
    if pending.is_empty() { return Ok(()); }
    let items = std::mem::take(pending);
    let count = items.len();
    if count > BATCH_MAX { return Err("R1241 quarantine batch bound exceeded".into()); }
    let durable_start=mono_ns();
    let mut items=items;for(frame,_)in &mut items{if let Some(t)=frame.source.as_mut().and_then(|s|s.r1279_timing.as_mut()){t.source_durable_start_ns=Some(durable_start);}}
    let committed=store_worker.call_async(move |store, _| {store.quarantine_batch(&items)?;Ok(items)}).await?;
    let durable_end=mono_ns();
    for(mut frame,_)in committed {if let Some(t)=frame.source.as_mut().and_then(|s|s.r1279_timing.as_mut()){t.source_durable_start_ns=Some(durable_start);t.source_durable_end_ns=Some(durable_end);t.quarantine_end_ns=Some(durable_end);t.terminal_ns=Some(durable_end);}counters.record_trace(&frame,"quarantined","not_applicable_quarantine");}
    counters.quarantine_batches = counters.quarantine_batches.saturating_add(1);
    counters.quarantine_batch_frames_max = counters.quarantine_batch_frames_max.max(count);
    Ok(())
}

pub fn spawn(
    client: CustodyClient,
    mut owners: Vec<Owner>,
    store_worker: StoreWorker,
    authority: Authority,
) -> (mpsc::Sender<Command>, mpsc::Sender<Control>, mpsc::Receiver<Event>) {
    let (command_tx, mut command_rx) = mpsc::channel(R1241_CANONICAL_INPUT_CAPACITY);
    let (control_tx, mut control_rx) = mpsc::channel(4);
    let (event_tx, event_rx) = mpsc::channel(4);
    tokio::spawn(async move {
        let result = async {
            let mut pending = Vec::<CanonicalFrame>::new();
            let mut pending_quarantines = Vec::<(RawFrame, String)>::new();
            let mut counters=PipelineCounters::default();
            counters.queue_started_ns=mono_ns();
            counters.queue_enqueued_start=CANONICAL_ENQUEUED.load(std::sync::atomic::Ordering::Relaxed);
            counters.queue_overflow_start=CANONICAL_OVERFLOW.load(std::sync::atomic::Ordering::Relaxed);
            let mut batch_id = 0_u64;
            let mut ticker = tokio::time::interval(std::time::Duration::from_millis(BATCH_DELAY_MS));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            ticker.tick().await;
            loop {
                tokio::select! {
                    biased;
                    command = command_rx.recv() => {
                        match command {
                            Some(Command::Frame(mut frame)) => {
                                let dequeued=mono_ns();if let Some(t)=frame.frame.source.as_mut().and_then(|s|s.r1279_timing.as_mut()){t.canonical_dequeue_ns=Some(dequeued);}
                                counters.queue_dequeued=counters.queue_dequeued.saturating_add(1);CANONICAL_DEPTH.fetch_sub(1,std::sync::atomic::Ordering::Relaxed);
                                flush_quarantine_batch(&mut pending_quarantines, &store_worker, &mut counters).await?;
                                counters.input_frames=counters.input_frames.saturating_add(1);
                                pending.push(frame);
                                if pending.len() >= BATCH_TARGET {
                                    flush_batch(&mut pending, &mut owners, &client, &store_worker,
                                        &authority, &event_tx, &mut control_rx, &mut batch_id,&mut counters).await?;
                                }
                            }
                            Some(Command::Quarantine { mut frame, reason }) => {
                                let dequeued=mono_ns();if let Some(t)=frame.source.as_mut().and_then(|s|s.r1279_timing.as_mut()){t.canonical_dequeue_ns=Some(dequeued);t.quarantine_start_ns=Some(dequeued);}
                                counters.queue_dequeued=counters.queue_dequeued.saturating_add(1);CANONICAL_DEPTH.fetch_sub(1,std::sync::atomic::Ordering::Relaxed);
                                counters.quarantined_frames=counters.quarantined_frames.saturating_add(1);
                                if !pending.is_empty() {
                                    flush_batch(&mut pending, &mut owners, &client, &store_worker,
                                        &authority, &event_tx, &mut control_rx, &mut batch_id,&mut counters).await?;
                                }
                                pending_quarantines.push((frame, reason));
                                if pending_quarantines.len() >= BATCH_TARGET {
                                    flush_quarantine_batch(&mut pending_quarantines, &store_worker, &mut counters).await?;
                                }
                            }
                            Some(Command::Reset { request_id, venue, bases, epoch }) => {
                                flush_quarantine_batch(&mut pending_quarantines, &store_worker, &mut counters).await?;
                                counters.reset_events=counters.reset_events.saturating_add(1);
                                if !pending.is_empty() {
                                    flush_batch(&mut pending, &mut owners, &client, &store_worker,
                                        &authority, &event_tx, &mut control_rx, &mut batch_id,&mut counters).await?;
                                }
                                apply_reset(request_id, venue, bases, epoch, &mut owners,
                                    &client, &event_tx, &mut control_rx).await?;
                            }
                            Some(Command::Shutdown) => {
                                flush_quarantine_batch(&mut pending_quarantines, &store_worker, &mut counters).await?;
                                if !pending.is_empty() {
                                    flush_batch(&mut pending, &mut owners, &client, &store_worker,
                                        &authority, &event_tx, &mut control_rx, &mut batch_id,&mut counters).await?;
                                }
                                event_tx.send(Event::Stopped{profile:counters.snapshot()}).await.map_err(|_| "R1241 event consumer closed")?;
                                return Ok::<(), String>(());
                            }
                            None => return Err("R1241 canonical ingress sender dropped without drain".into()),
                        }
                    }
                    _ = ticker.tick(), if !pending.is_empty() || !pending_quarantines.is_empty() => {
                        if !pending_quarantines.is_empty() {
                            flush_quarantine_batch(&mut pending_quarantines, &store_worker, &mut counters).await?;
                        } else {
                            flush_batch(&mut pending, &mut owners, &client, &store_worker,
                                &authority, &event_tx, &mut control_rx, &mut batch_id,&mut counters).await?;
                        }
                    }
                }
            }
        }.await;
        if let Err(error) = result {
            let _ = event_tx.send(Event::Failed(error)).await;
        }
    });
    (command_tx, control_tx, event_rx)
}

async fn flush_batch(
    pending: &mut Vec<CanonicalFrame>,
    owners: &mut Vec<Owner>,
    client: &CustodyClient,
    store_worker: &StoreWorker,
    authority: &Authority,
    event_tx: &mpsc::Sender<Event>,
    control_rx: &mut mpsc::Receiver<Control>,
    next_batch_id: &mut u64,
    counters: &mut PipelineCounters,
) -> Result<(), String> {
    if pending.is_empty() { return Ok(()); }
    if pending.len() > BATCH_MAX { return Err("R1241 canonical batch bound exceeded".into()); }

    let now_ns = mono_ns();
    let now_wall_ms = wall_ms();
    let mut eligible = Vec::with_capacity(pending.len());
    let mut fenced_quarantines = Vec::new();
    for item in pending.drain(..) {
        if authorization_at_normalized(authority, &item.frame, &item.update, now_ns, now_wall_ms).is_some() {
            eligible.push(item);
        } else {
            fenced_quarantines.push((item.frame, "selection_fenced_before_owner".to_owned()));
            counters.quarantined_frames=counters.quarantined_frames.saturating_add(1);
        }
    }
    // The consumer processes this batch atomically and no later command can
    // overtake it. Persist adjacent fenced frames in one FULL transaction.
    flush_quarantine_batch(&mut fenced_quarantines, store_worker, counters).await?;
    if eligible.is_empty() { return Ok(()); }

    let source_durable_start=mono_ns();for item in &mut eligible{if let Some(t)=item.frame.source.as_mut().and_then(|s|s.r1279_timing.as_mut()){t.source_durable_start_ns=Some(source_durable_start);}}
    let writer_frames = eligible.iter().map(|item| {
        (item.frame.clone(), item.update.clone(), item.owner_index)
    }).collect::<Vec<_>>();
    let owned = std::mem::take(owners);
    let (source_seq, returned_owners) = client.call(move |writer| {
        let mut owned = owned;
        let seq = writer.source_commit_guarded(&mut owned, &writer_frames, &[], Inject::None)?;
        Ok((seq, owned))
    }).await?;
    *owners = returned_owners;
    let source_durable_end=mono_ns();for item in &mut eligible{if let Some(t)=item.frame.source.as_mut().and_then(|s|s.r1279_timing.as_mut()){t.source_durable_end_ns=Some(source_durable_end);}}
    counters.committed_batches=counters.committed_batches.saturating_add(1);
    counters.committed_frames=counters.committed_frames.saturating_add(eligible.len() as u64);
    counters.batch_size_max=counters.batch_size_max.max(eligible.len());

    *next_batch_id = next_batch_id.checked_add(1).ok_or("R1241 batch id overflow")?;
    let batch_id = *next_batch_id;
    event_tx.send(Event::BatchReady {
        batch_id, source_seq, frames: eligible.clone(),
    }).await.map_err(|_| "R1241 event consumer closed")?;

    let decisions = match control_rx.recv().await {
        Some(Control::FinalizeBatch { batch_id: received, decisions }) if received == batch_id => decisions,
        Some(_) => return Err("R1241 out-of-order batch finalization".into()),
        None => return Err("R1241 decision finalizer closed with pending canonical obligation".into()),
    };
    let mut verify_frames = eligible.iter().map(|item| item.frame.clone()).collect::<Vec<_>>();
    verify_decision_order(&verify_frames,&decisions)?;
    for(item,seal)in eligible.iter_mut().zip(&decisions){if let (Some(dst),Some(src))=(item.frame.source.as_mut().and_then(|s|s.r1279_timing.as_mut()),seal.frame.source.as_ref().and_then(|s|s.r1279_timing.as_ref())){dst.paper_decision_start_ns=src.paper_decision_start_ns;dst.paper_decision_end_ns=src.paper_decision_end_ns;dst.decision_trace_id=src.decision_trace_id.clone();}}
    for(index,item)in eligible.iter().enumerate(){if let (Some(dst),Some(src))=(verify_frames[index].source.as_mut(),item.frame.source.as_ref()){dst.r1279_timing=src.r1279_timing.clone();}}
    let seal_frames = verify_frames.clone();
    let (transition_seqs, decision_seq, verified) = client.call(move |writer| {
        let mut transition_seqs = Vec::new();
        let mut final_seq = None;
        for seal in decisions {
            if let Some(commit) = seal.pre_decision_commit {
                transition_seqs.push(commit(writer)?);
            }
            final_seq = writer.note_guarded_decision(&seal.frame, "EVALUATED", &seal.result_hash)?;
        }
        let verified = writer.verify_ingress_batch(&seal_frames)?;
        Ok((transition_seqs, final_seq, verified))
    }).await?;
    if decision_seq.is_none() { return Err("R1241 incomplete guarded decision batch".into()); }

    let mut store_frames = verify_frames.clone();
    let publish_start=mono_ns();for frame in &mut store_frames{if let Some(t)=frame.source.as_mut().and_then(|s|s.r1279_timing.as_mut()){t.publish_durable_start_ns=Some(publish_start);}}for item in &mut eligible{if let Some(t)=item.frame.source.as_mut().and_then(|s|s.r1279_timing.as_mut()){t.publish_durable_start_ns=Some(publish_start);}}
    let (committed, verified) = store_worker.call_async(move |store, _| {
        let committed = store.finish_published_batch_verified(&store_frames, &verified)?;
        Ok((committed, verified))
    }).await?;
    let publish_end=mono_ns();for item in &mut eligible{if let Some(t)=item.frame.source.as_mut().and_then(|s|s.r1279_timing.as_mut()){t.publish_durable_end_ns=Some(publish_end);t.ack_start_ns=Some(publish_end);}}
    let (release_seq, acked) = client.call(move |writer| {
        let ticket = writer.prepare_ingress_release_after_publish(&verified, &committed)?
            .ok_or("R1241 missing ingress release ticket")?;
        let seq = writer.commit_ingress_release(ticket)?;
        let acked = writer.confirm_ingress_reclaimed(&committed)?;
        Ok((Some(seq), acked))
    }).await?;
    let ack_end=mono_ns();let terminal=mono_ns();for item in &mut eligible{if let Some(t)=item.frame.source.as_mut().and_then(|s|s.r1279_timing.as_mut()){t.ack_end_ns=Some(ack_end);t.terminal_ns=Some(terminal);}}
    let traces=eligible.iter().filter_map(|item|{
        let source=item.frame.source.as_ref()?;
        let timing=source.r1279_timing.as_ref()?;
        let completion=serde_json::json!({"publish_durable_end_ns":timing.publish_durable_end_ns,"ack_start_ns":timing.ack_start_ns,"ack_end_ns":timing.ack_end_ns,"terminal_ns":timing.terminal_ns});
        Some((source.accepted_id,serde_json::to_string(&completion).unwrap_or_else(|_|"{}".into())))
    }).collect::<Vec<_>>();
    store_worker.call_async(move |store, _| store.forget_reclaimed_after_ack_with_trace(&acked,&traces)).await?;
    for item in &eligible{counters.record_trace(&item.frame,"published","reclaimed_ack_confirmed");}
    counters.acknowledged_frames=counters.acknowledged_frames.saturating_add(eligible.len() as u64);

    event_tx.send(Event::BatchAcked {
        batch_id, transition_seqs, decision_seq, release_seq, frame_count: eligible.len(),
    }).await.map_err(|_| "R1241 event consumer closed")?;
    Ok(())
}

async fn apply_reset(
    request_id: u64,
    venue: Venue,
    bases: Vec<String>,
    epoch: u64,
    owners: &mut Vec<Owner>,
    client: &CustodyClient,
    event_tx: &mpsc::Sender<Event>,
    control_rx: &mut mpsc::Receiver<Control>,
) -> Result<(), String> {
    let reset_bases = bases.clone();
    let owned = std::mem::take(owners);
    let source_ms = wall_ms();
    let (seq, returned) = client.call(move |writer| {
        let mut owned = owned;
        let seq = writer.source_reset(&mut owned, venue, &reset_bases, epoch, source_ms,
            "PUBLIC_WS_RESET", Inject::None)?;
        Ok((seq, owned))
    }).await?;
    *owners = returned;
    event_tx.send(Event::ResetReady { request_id, seq, venue, bases, epoch })
        .await.map_err(|_| "R1241 event consumer closed")?;
    match control_rx.recv().await {
        Some(Control::ResetApplied { request_id: received }) if received == request_id => Ok(()),
        Some(_) => Err("R1241 out-of-order source reset acknowledgement".into()),
        None => Err("R1241 reset acknowledgement lost".into()),
    }
}


fn verify_decision_order(frames: &[RawFrame], decisions: &[DecisionSeal]) -> Result<(), String> {
    if frames.len() != decisions.len() {
        return Err("R1241 decision count differs from durable batch".into());
    }
    for (index, (frame, seal)) in frames.iter().zip(decisions).enumerate() {
        let same = frame.venue == seal.frame.venue
            && frame.epoch == seal.frame.epoch
            && frame.received_ns == seal.frame.received_ns
            && frame.received_wall_ms == seal.frame.received_wall_ms
            && frame.raw == seal.frame.raw
            && frame.source == seal.frame.source;
        if !same {
            return Err(format!("R1241 decision seal order/identity mismatch at index {index}"));
        }
        if seal.result_hash.is_empty() {
            return Err(format!("R1241 empty decision hash at index {index}"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SourceProvenance;

    fn frame(sequence: u64) -> RawFrame {
        RawFrame {
            venue: Venue::BinanceFutures,
            epoch: 3,
            received_ns: sequence,
            received_wall_ms: sequence as i64,
            raw: format!("frame-{sequence}"),
            source: Some(SourceProvenance {
                selected_permit:None,prepared:None,
                frame_trace_id:format!("fixture:session-a:{sequence}"),
                journal_id: "journal-a".into(),
                accepted_id: sequence,
                session_id: "session-a".into(),
                connection_epoch: 2,
                selection_epoch: 4,
                payload_sha256: format!("hash-{sequence}"),
                transport: None,
                accepted_ns: Some(sequence),
                selected_ns: Some(sequence),
                r1247_timing: None,
                r1279_timing: None,
            }),
        }
    }

    #[test]
    fn decision_seals_must_match_durable_fifo_order() {
        let first = frame(1);
        let second = frame(2);
        let decisions = vec![
            DecisionSeal { frame: first.clone(), result_hash: "r1".into(), pre_decision_commit: None },
            DecisionSeal { frame: second.clone(), result_hash: "r2".into(), pre_decision_commit: None },
        ];
        assert!(verify_decision_order(&[first.clone(), second.clone()], &decisions).is_ok());
        let swapped = vec![
            DecisionSeal { frame: second, result_hash: "r2".into(), pre_decision_commit: None },
            DecisionSeal { frame: first, result_hash: "r1".into(), pre_decision_commit: None },
        ];
        assert!(verify_decision_order(&[frame(1), frame(2)], &swapped).is_err());
    }

    #[test]
    fn decision_seal_rejects_missing_or_empty_hash() {
        let only = frame(1);
        assert!(verify_decision_order(&[only.clone()], &[]).is_err());
        let empty_hash = vec![DecisionSeal { frame: only.clone(), result_hash: String::new(), pre_decision_commit: None }];
        assert!(verify_decision_order(&[only], &empty_hash).is_err());
    }

    #[tokio::test]
    async fn bounded_ingress_submission_fails_closed_without_blocking() {
        let (tx, _rx) = mpsc::channel(1);
        submit(&tx, Command::Frame(CanonicalFrame {
            frame: frame(1),
            update: Update {
                base: "BTC".into(),
                venue: Venue::BinanceFutures,
                book: crate::Book {
                    bids: vec![(100.0, 1.0)],
                    asks: vec![(101.0, 1.0)],
                    received_ns: 1,
                    received_wall_ms: 1,
                    exchange_ts_ms: None,
                    changed_ns: 1,
                    epoch: 3,
                    sequence: Some(1),
                },
            },
            owner_index: 0,
        })).unwrap();
        let error = submit(&tx, Command::Shutdown).unwrap_err();
        assert!(error.contains("queue full"));
    }

    #[test]
    fn r1279_trace_id_and_monotonic_stages_roundtrip_without_fabricating_missing_stages() {
        let mut f=frame(8);
        let source=f.source.as_mut().unwrap();
        source.r1279_timing=Some(crate::R1279FrameTiming{
            clock_domain_id:Some("replay-clock-1".into()),
            ws_receive_ns:Some(100),
            ws_decode_end_ns:Some(110),
            reader_enqueue_ns:Some(120),
            reader_dequeue_ns:Some(150),
            terminal_ns:Some(230),
            receive_batch_id:Some(12),selection_batch_id:Some(13),receive_batch_start_ns:Some(180),receive_batch_end_ns:Some(220),
            frame_hold_reason:Some("waiting_for_two_fresh_hot_sources".into()),terminalization_reason:Some("quarantined".into()),
            source_selection_wall_residual_ns:Some(7),
            ..Default::default()
        });
        let encoded=serde_json::to_vec(&f).unwrap();
        let restored:RawFrame=serde_json::from_slice(&encoded).unwrap();
        assert_eq!(restored.source.as_ref().unwrap().frame_trace_id,"fixture:session-a:8");
        assert_eq!(restored.source.as_ref().unwrap().r1279_timing.as_ref().unwrap().reader_dequeue_ns,Some(150));
        let row=crate::r1279_trace_row(&restored,"quarantined","not_applicable_quarantine");
        assert_eq!(row["reader_queue_wait_ns"],30);
        assert_eq!(row["canonical_queue_wait_ns"],serde_json::Value::Null);
        assert_eq!(row["terminal_state"],"quarantined");
        let timing=restored.source.as_ref().unwrap().r1279_timing.as_ref().unwrap();
        assert_eq!(timing.receive_batch_id,Some(12));assert_eq!(timing.selection_batch_id,Some(13));
        assert!(timing.receive_batch_end_ns.unwrap()>=timing.receive_batch_start_ns.unwrap());
        assert_eq!(timing.frame_hold_reason.as_deref(),Some("waiting_for_two_fresh_hot_sources"));
        assert_eq!(timing.source_selection_wall_residual_ns,Some(7));
        assert_eq!(row["timestamps_mono_ns"]["publish_durable_end_ns"],serde_json::Value::Null);
    }

    #[test]
    fn r1279_trace_sample_buffer_is_bounded_and_counts_eviction() {
        let mut counters=PipelineCounters::default();
        for n in 1..=262208 {let mut f=frame(n);f.source.as_mut().unwrap().r1279_timing=Some(crate::R1279FrameTiming{clock_domain_id:Some("test".into()),..Default::default()});counters.record_trace(&f,"quarantined","not_applicable_quarantine");}
        assert_eq!(counters.trace_samples.len(),4096);
        assert_eq!(counters.trace_dropped,1);
        assert_eq!(counters.trace_samples.front().unwrap()["frame_trace_id"],"fixture:session-a:128");
    }
}
