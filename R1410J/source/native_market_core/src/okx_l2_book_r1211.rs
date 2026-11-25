//! Stateful, fail-closed reconstruction for the public OKX `books` channel.
use crate::{Book, RawFrame, Update, Venue};
use serde_json::Value;
use std::collections::HashMap;

const MAX_DEPTH: usize = 400;
/// Bound one incremental message independently from the retained snapshot depth.
/// The enclosing raw frame remains limited to MAX_FRAME_BYTES.
const MAX_DELTA_ROWS: usize = 1024;
// The durable Owner/DB schema accepts at most 20 levels per side.
// Keep the full 400-level state for delta application; publish only its bounded view.
const PUBLISH_DEPTH: usize = 20;
const MAX_FRAME_BYTES: usize = 65_536;

#[derive(Clone, Debug)]
struct SideState {
    levels: Vec<(f64, f64)>,
}
#[derive(Clone, Debug)]
struct InstrumentBook {
    bids: SideState,
    asks: SideState,
    seq: i64,
}
#[derive(Default)]
pub struct OkxL2BookBuilder {
    books: HashMap<(String, String), InstrumentBook>,
}

fn number(v: &Value) -> Result<f64, String> {
    let n = v.as_str().and_then(|s| s.parse::<f64>().ok())
        .or_else(|| v.as_f64()).ok_or("invalid decimal")?;
    if !n.is_finite() || n < 0.0 { return Err("invalid decimal range".into()); }
    Ok(n)
}

fn read_levels(v: &Value, allow_zero: bool, max_rows: usize, bound_error: &str) -> Result<Vec<(f64, f64)>, String> {
    let rows = v.as_array().ok_or("levels must be array")?;
    if rows.len() > max_rows { return Err(bound_error.into()); }
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let fields = row.as_array().ok_or("level must be array")?;
        if fields.len() < 2 { return Err("short level".into()); }
        let price = number(&fields[0])?;
        let qty = number(&fields[1])?;
        if price <= 0.0 || (!allow_zero && qty <= 0.0) {
            return Err("invalid price or quantity".into());
        }
        if out.iter().any(|(p, _)| *p == price) {
            return Err("duplicate price in update".into());
        }
        out.push((price, qty));
    }
    Ok(out)
}

fn apply_side(state: &mut SideState, changes: Vec<(f64, f64)>, bid: bool) {
    for (price, qty) in changes {
        if let Some(i) = state.levels.iter().position(|(p, _)| *p == price) {
            if qty == 0.0 { state.levels.remove(i); } else { state.levels[i].1 = qty; }
        } else if qty > 0.0 {
            state.levels.push((price, qty));
        }
    }
    if bid {
        state.levels.sort_by(|a, b| b.0.total_cmp(&a.0));
    } else {
        state.levels.sort_by(|a, b| a.0.total_cmp(&b.0));
    }
    state.levels.truncate(MAX_DEPTH);
}

impl OkxL2BookBuilder {
    /// Approximate retained capacity for parsed OKX L2 state, excluding allocator metadata.
    pub fn memory_usage_bytes(&self) -> (usize, usize) {
        let table_bytes = self.books.capacity()
            .saturating_mul(std::mem::size_of::<((String, String), InstrumentBook)>());
        let dynamic_bytes = self.books.iter().map(|((session, base), book)| {
            session.capacity()
                .saturating_add(base.capacity())
                .saturating_add(book.bids.levels.capacity()
                    .saturating_add(book.asks.levels.capacity())
                    .saturating_mul(std::mem::size_of::<(f64, f64)>()))
        }).sum::<usize>();
        (self.books.len(), table_bytes.saturating_add(dynamic_bytes))
    }

    pub fn clear_session(&mut self, session: &str) {
        self.books.retain(|(owner, _), _| owner != session);
    }

    /// Invalidate one instrument after any frame makes its sequence continuity uncertain.
    pub fn clear_instrument(&mut self, session: &str, base: &str) {
        self.books.remove(&(session.to_string(), base.to_string()));
    }

    /// Read-only pre-apply state used to explain a rejected delta without
    /// exposing raw book levels or allowing diagnostics to affect custody.
    pub fn diagnostic_state(&self, session: &str, base: &str) -> Option<(i64, usize, usize)> {
        self.books.get(&(session.to_string(), base.to_string()))
            .map(|book| (book.seq, book.bids.levels.len(), book.asks.levels.len()))
    }

    /// Applies one raw OKX frame. Any malformed or discontinuous update invalidates
    /// this session's book; only a new full snapshot can restore it.
    pub fn apply(&mut self, session: &str, frame: &RawFrame) -> Result<Update, String> {
        if frame.venue != Venue::OkxSpot { return Err("venue mismatch".into()); }
        if frame.raw.len() > MAX_FRAME_BYTES { return Err("frame bound".into()); }
        let v: Value = serde_json::from_str(&frame.raw).map_err(|e| e.to_string())?;
        self.apply_parsed(session, frame, &v)
    }

    /// Reuse the caller's decoded frame while keeping the raw-frame validation API equivalent.
    pub(crate) fn apply_parsed(&mut self, session: &str, frame: &RawFrame, v: &Value) -> Result<Update, String> {
        if frame.venue != Venue::OkxSpot { return Err("venue mismatch".into()); }
        if frame.raw.len() > MAX_FRAME_BYTES { return Err("frame bound".into()); }
        let parsed = (|| {
            let arg = v.get("arg").ok_or("missing arg")?;
            if arg.get("channel").and_then(Value::as_str) != Some("books") {
                return Err("unexpected channel".to_string());
            }
            let symbol = arg.get("instId").and_then(Value::as_str).ok_or("missing instId")?;
            let base = symbol.strip_suffix("-USDT").filter(|s| !s.is_empty())
                .ok_or("unsupported quote")?.to_string();
            if !base.bytes().all(|c| c.is_ascii_uppercase() || c.is_ascii_digit()) {
                return Err("invalid symbol".into());
            }
            let action = v.get("action").and_then(Value::as_str).ok_or("missing action")?;
            let row = v.get("data").and_then(Value::as_array).filter(|a| a.len() == 1)
                .and_then(|a| a.first()).ok_or("data must contain one book")?;
            let ts = row.get("ts").and_then(Value::as_str).and_then(|s| s.parse::<i64>().ok())
                .ok_or("invalid exchange timestamp")?;
            let seq = row.get("seqId").and_then(Value::as_i64).filter(|n| *n >= 0)
                .ok_or("invalid sequence")?;
            let key = (session.to_string(), base.clone());
            match action {
                "snapshot" => {
                    if row.get("prevSeqId").and_then(Value::as_i64) != Some(-1) {
                        return Err("snapshot prevSeqId must be -1".into());
                    }
                    let mut bids = SideState { levels: read_levels(&row["bids"], false, MAX_DEPTH, "snapshot depth bound")? };
                    let mut asks = SideState { levels: read_levels(&row["asks"], false, MAX_DEPTH, "snapshot depth bound")? };
                    if bids.levels.is_empty() || asks.levels.is_empty() {
                        return Err("empty snapshot side".into());
                    }
                    bids.levels.sort_by(|a,b| b.0.total_cmp(&a.0));
                    asks.levels.sort_by(|a,b| a.0.total_cmp(&b.0));
                    bids.levels.truncate(MAX_DEPTH);
                    asks.levels.truncate(MAX_DEPTH);
                    if bids.levels[0].0 >= asks.levels[0].0 { return Err("crossed snapshot".into()); }
                    self.books.insert(key.clone(), InstrumentBook { bids, asks, seq });
                }
                "update" => {
                    let prev = row.get("prevSeqId").and_then(Value::as_i64)
                        .ok_or("missing prevSeqId")?;
                    let Some(book) = self.books.get_mut(&key) else {
                        return Err("delta without snapshot".into());
                    };
                    if prev != book.seq {
                        self.books.remove(&key);
                        return Err("sequence gap".into());
                    }
                    let bids = read_levels(&row["bids"], true, MAX_DELTA_ROWS, "delta update row bound")?;
                    let asks = read_levels(&row["asks"], true, MAX_DELTA_ROWS, "delta update row bound")?;
                    if seq == prev && (!bids.is_empty() || !asks.is_empty()) {
                        self.books.remove(&key);
                        return Err("unchanged sequence with price changes".into());
                    }
                    apply_side(&mut book.bids, bids, true);
                    apply_side(&mut book.asks, asks, false);
                    if book.bids.levels.is_empty() || book.asks.levels.is_empty()
                        || book.bids.levels[0].0 >= book.asks.levels[0].0 {
                        self.books.remove(&key);
                        return Err("invalid reconstructed spread".into());
                    }
                    book.seq = seq;
                }
                _ => return Err("unsupported action".into()),
            }
            let book = self.books.get(&key).ok_or("book state missing")?;
            Ok(Update {
                base,
                venue: Venue::OkxSpot,
                book: Book {
                    bids: book.bids.levels.iter().take(PUBLISH_DEPTH).copied().collect(),
                    asks: book.asks.levels.iter().take(PUBLISH_DEPTH).copied().collect(),
                    received_ns: frame.received_ns,
                    received_wall_ms: frame.received_wall_ms,
                    exchange_ts_ms: Some(ts),
                    changed_ns: frame.received_ns,
                    epoch: frame.epoch,
                    sequence: Some(book.seq),
                },
            })
        })();
        if parsed.is_err() {
            // A bad frame must never leave a possibly partial state publishable.
            if let Some(s) = v.get("arg").and_then(|a| a.get("instId")).and_then(Value::as_str)
                .and_then(|s| s.strip_suffix("-USDT")) {
                self.books.remove(&(session.to_string(), s.to_string()));
            }
        }
        parsed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn frame(action: &str, seq: i64, prev: Option<i64>, bids: &str, asks: &str) -> RawFrame {
        let prev = prev.map(|n| format!(",\"prevSeqId\":{n}"))
            .unwrap_or_else(|| if action == "snapshot" { ",\"prevSeqId\":-1".into() } else { String::new() });
        RawFrame {
            venue: Venue::OkxSpot, epoch: 1, received_ns: 10, received_wall_ms: 1000,
            raw: format!(r#"{{"arg":{{"channel":"books","instId":"BTC-USDT"}},"action":"{action}","data":[{{"ts":"1000","seqId":{seq}{prev},"bids":{bids},"asks":{asks}}}]}}"#),
            source: None,
        }
    }
    fn snap(bids: &str, asks: &str) -> RawFrame { frame("snapshot", 10, None, bids, asks) }
    #[test] fn snapshot_bootstraps_and_sorts_both_sides() {
        let mut b=OkxL2BookBuilder::default();
        let u=b.apply("s1",&snap(r#"[["100","1"],["101","2"]]"#,r#"[["103","1"],["102","2"]]"#)).unwrap();
        assert_eq!(u.book.bids[0].0,101.0); assert_eq!(u.book.asks[0].0,102.0);
    }
    #[test] fn delta_replaces_adds_and_deletes_zero_quantity() {
        let mut b=OkxL2BookBuilder::default();
        b.apply("s1",&snap(r#"[["100","1"],["99","2"]]"#,r#"[["102","1"],["103","2"]]"#)).unwrap();
        let u=b.apply("s1",&frame("update",11,Some(10),r#"[["100","0"],["101","3"]]"#,r#"[["102","4"]]"#)).unwrap();
        assert_eq!(u.book.bids,vec![(101.0,3.0),(99.0,2.0)]);
        assert_eq!(u.book.asks[0],(102.0,4.0)); assert_eq!(u.book.sequence,Some(11));
    }
    #[test] fn delta_without_snapshot_fails_closed() {
        let mut b=OkxL2BookBuilder::default();
        assert!(b.apply("s1",&frame("update",11,Some(10),"[]","[]")).is_err());
    }
    #[test] fn parsed_api_matches_raw_api_for_snapshot_delta_and_gap() {
        let mut raw_builder=OkxL2BookBuilder::default();
        let mut parsed_builder=OkxL2BookBuilder::default();
        for input in [
            snap(r#"[["100","1"],["99","2"]]"#,r#"[["102","1"],["103","2"]]"#),
            frame("update",11,Some(10),r#"[["100","0"],["101","3"]]"#,r#"[["102","4"]]"#),
        ] {
            let value:Value=serde_json::from_str(&input.raw).unwrap();
            assert_eq!(raw_builder.apply("s1",&input),parsed_builder.apply_parsed("s1",&input,&value));
            assert_eq!(raw_builder.diagnostic_state("s1","BTC"),parsed_builder.diagnostic_state("s1","BTC"));
        }
        let gap=frame("update",13,Some(12),"[]","[]");
        let value:Value=serde_json::from_str(&gap.raw).unwrap();
        assert_eq!(raw_builder.apply("s1",&gap),parsed_builder.apply_parsed("s1",&gap,&value));
        assert_eq!(raw_builder.diagnostic_state("s1","BTC"),parsed_builder.diagnostic_state("s1","BTC"));
        assert_eq!(raw_builder.diagnostic_state("s1","BTC"),None);
    }
    #[test] fn sequence_gap_invalidates_book_until_snapshot() {
        let mut b=OkxL2BookBuilder::default();
        b.apply("s1",&snap(r#"[["100","1"]]"#,r#"[["102","1"]]"#)).unwrap();
        assert!(b.apply("s1",&frame("update",12,Some(11),"[]","[]")).is_err());
        assert!(b.apply("s1",&frame("update",13,Some(12),"[]","[]")).is_err());
        assert!(b.apply("s1",&snap(r#"[["100","1"]]"#,r#"[["102","1"]]"#)).is_ok());
    }
    #[test] fn zero_quantity_snapshot_is_invalid() {
        let mut b=OkxL2BookBuilder::default();
        assert!(b.apply("s1",&snap(r#"[["100","0"]]"#,r#"[["102","1"]]"#)).is_err());
    }
    #[test] fn session_books_are_isolated() {
        let mut b=OkxL2BookBuilder::default();
        b.apply("s1",&snap(r#"[["100","1"]]"#,r#"[["102","1"]]"#)).unwrap();
        assert!(b.apply("s2",&frame("update",11,Some(10),"[]","[]")).is_err());
    }
    #[test] fn invalid_crossed_delta_cannot_publish() {
        let mut b=OkxL2BookBuilder::default();
        b.apply("s1",&snap(r#"[["100","1"]]"#,r#"[["102","1"]]"#)).unwrap();
        assert!(b.apply("s1",&frame("update",11,Some(10),r#"[["103","1"]]"#,"[]")).is_err());
    }
    #[test] fn depth_is_bounded_to_exchange_snapshot_depth() {
        let mut b=OkxL2BookBuilder::default();
        let bids=(1..=400).rev().map(|p|format!(r#"["{p}","1"]"#)).collect::<Vec<_>>().join(",");
        let asks=(1000..=1399).map(|p|format!(r#"["{p}","1"]"#)).collect::<Vec<_>>().join(",");
        b.apply("s1",&snap(&format!("[{bids}]"),&format!("[{asks}]"))).unwrap();
        let u=b.apply("s1",&frame("update",11,Some(10),r#"[["401","1"]]"#,r#"[["1400","1"]]"#)).unwrap();
        assert_eq!(u.book.bids.len(),PUBLISH_DEPTH); assert_eq!(u.book.asks.len(),PUBLISH_DEPTH);
        assert_eq!(u.book.bids[0].0,401.0); assert_eq!(u.book.asks[0].0,1000.0);
        assert_eq!(u.book.bids[19].0,382.0);
        let deletes=(382..=401).rev().map(|p|format!("[\"{}\",\"0\"]",p)).collect::<Vec<_>>().join(",");
        let next=b.apply("s1",&frame("update",12,Some(11),&format!("[{deletes}]"),"[]")).unwrap();
        assert_eq!(next.book.bids[0].0,381.0,"levels beyond published depth must remain in delta state");
    }
    #[test] fn bounded_delta_batch_may_exceed_snapshot_depth() {
        let mut b=OkxL2BookBuilder::default();
        b.apply("s1",&snap(r#"[["100","1"]]"#,r#"[["1000","1"]]"#)).unwrap();
        let updates=(101..=501).map(|p|format!(r#"[{p},"1"]"#)).collect::<Vec<_>>().join(",");
        let u=b.apply("s1",&frame("update",11,Some(10),&format!("[{updates}]"),"[]")).unwrap();
        assert_eq!(u.book.sequence,Some(11));
        assert_eq!(u.book.bids.len(),PUBLISH_DEPTH);
        assert_eq!(u.book.bids[0].0,501.0);
        assert_eq!(b.diagnostic_state("s1","BTC"),Some((11,MAX_DEPTH,1)));
    }
    #[test] fn matching_prev_seq_allows_documented_sequence_reset() {
        let mut b=OkxL2BookBuilder::default();
        b.apply("s1",&frame("snapshot",20,None,r#"[["100","1"]]"#,r#"[["102","1"]]"#)).unwrap();
        let u=b.apply("s1",&frame("update",5,Some(20),"[]","[]")).unwrap();
        assert_eq!(u.book.sequence,Some(5));
        assert_eq!(b.diagnostic_state("s1","BTC").map(|s|s.0),Some(5));
    }
    #[test] fn empty_keepalive_preserves_book_and_sequence() {
        let mut b=OkxL2BookBuilder::default();
        b.apply("s1",&frame("snapshot",10,None,r#"[["100","1"]]"#,r#"[["102","1"]]"#)).unwrap();
        let before=b.diagnostic_state("s1","BTC");
        let u=b.apply("s1",&frame("update",10,Some(10),"[]","[]")).unwrap();
        assert_eq!(u.book.sequence,Some(10));
        assert_eq!(b.diagnostic_state("s1","BTC"),before);
    }
    #[test] fn checksum_zero_is_telemetry_only_and_does_not_reject_books() {
        let mut b=OkxL2BookBuilder::default();
        let mut f=frame("snapshot",10,None,r#"[["100","1"]]"#,r#"[["102","1"]]"#);
        f.raw=f.raw.replace("\"seqId\":10","\"seqId\":10,\"checksum\":0");
        assert!(b.apply("s1",&f).is_ok());
    }
}

#[cfg(test)]
mod r1286_okx_sequence_integrity_tests {
 use super::*;
 fn frame(action:&str,seq:i64,prev:Option<i64>,bids:&str,asks:&str)->RawFrame{
  let prev=prev.map(|n|format!(",\"prevSeqId\":{n}")).unwrap_or_else(||if action=="snapshot"{",\"prevSeqId\":-1".into()}else{String::new()});
  RawFrame{venue:Venue::OkxSpot,epoch:1,received_ns:10,received_wall_ms:1000,
   raw:format!(r#"{{"arg":{{"channel":"books","instId":"BTC-USDT"}},"action":"{action}","data":[{{"ts":"1000","seqId":{seq}{prev},"bids":{bids},"asks":{asks}}}]}}"#),source:None}
 }
 #[test] fn checksum_absent_or_zero_is_not_an_integrity_gate(){
  let mut b=OkxL2BookBuilder::default();
  let mut no_checksum=frame("snapshot",10,None,r#"[["100","1"]]"#,r#"[["102","1"]]"#);
  assert!(b.apply("s1",&no_checksum).is_ok());
  no_checksum.raw=no_checksum.raw.replace("\"seqId\":10","\"seqId\":10,\"checksum\":0");
  assert!(b.apply("s2",&no_checksum).is_ok());
 }
 #[test] fn sequence_gap_invalidates_until_verified_snapshot_recovery(){
  let mut b=OkxL2BookBuilder::default();
  b.apply("s1",&frame("snapshot",10,None,r#"[["100","1"]]"#,r#"[["102","1"]]"#)).unwrap();
  assert!(b.apply("s1",&frame("update",12,Some(11),"[]","[]")).is_err());
  assert_eq!(b.diagnostic_state("s1","BTC"),None);
  assert!(b.apply("s1",&frame("update",13,Some(12),"[]","[]")).is_err());
  b.apply("s1",&frame("snapshot",20,None,r#"[["100","1"]]"#,r#"[["102","1"]]"#)).unwrap();
  assert_eq!(b.diagnostic_state("s1","BTC").map(|s|s.0),Some(20));
  assert!(b.apply("s1",&frame("update",21,Some(20),"[]","[]")).is_ok());
 }
}
