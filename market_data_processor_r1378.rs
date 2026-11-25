//! R1384: ordered typed/OKX L2 preparation inside the existing Rust crate.
//! The Processor owns one book builder; Engine still owns the only safety/authority gate.
//! No queue, extra task, change of processing order or trading policy.

use crate::{RawFrame, Update, parse, parse_json};
use crate::okx_l2_book_r1211::OkxL2BookBuilder;
use serde_json::Value;
use std::collections::HashMap;

/// Single ordered owner of typed market-book preparation inside the existing
/// Engine call path. The Engine still decides when a frame may be processed,
/// keeps recovery/generation fences, and performs the final safety admission.
/// No worker, queue, additional book copy or asynchronous reordering.
#[derive(Default)]
pub struct MarketDataProcessor {
    okx_l2: OkxL2BookBuilder,
    // Forwarder-only chain. The Engine independently acknowledges each parent;
    // speculative deltas following an Engine rejection cannot gain authority.
    forwarder_chain_last: HashMap<(String, String), u64>,
    forwarder_chain_next: u64,
}

/// Prepared per-frame protocol result; never a source of trading authority.
/// Exists only in RAM and is stripped before frame storage/decision.
#[derive(Clone, Debug)]
pub(crate) struct PreparedFrame {
    pub decoded: Option<Value>,
    pub parsed: Option<Result<Update, String>>,
    pub previous_l2: Option<(i64, usize, usize)>,
    pub chain: Option<PreparedChain>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PreparedChain {
    pub base: String,
    pub predecessor: Option<u64>,
    pub token: u64,
    pub snapshot: bool,
}

impl MarketDataProcessor {
    /// Reconstructs both exchanges' market frames inside the EXISTING per-reader
    /// Forwarder. Never grants HOT or final market/trading authority.
    pub(crate) fn prepare_forwarded(&mut self, frame: &RawFrame) -> PreparedFrame {
        let decoded = Self::decode_json(&frame.raw);
        let session = frame.source.as_ref().map(|p|p.session_id.as_str()).unwrap_or("");
        let l2 = frame.venue == crate::Venue::OkxSpot
            && decoded.as_ref().is_some_and(|v|v["arg"]["channel"] == "books");
        let base = if l2 {
            decoded.as_ref().and_then(|v|v["arg"]["instId"].as_str())
                .and_then(|s|s.strip_suffix("-USDT"))
        } else { None };
        let previous_l2 = base.and_then(|base|self.diagnostic_state(session,base));
        let parsed = self.prepare_update(session,frame,decoded.as_ref(),l2);
        let mut chain = None;
        if l2 {
            match parsed.as_ref() {
                Ok(update) => {
                    let identity=(session.to_string(),update.base.clone());
                    let snapshot=decoded.as_ref().is_some_and(|v|v["action"]=="snapshot");
                    let predecessor=if snapshot {None} else {self.forwarder_chain_last.get(&identity).copied()};
                    self.forwarder_chain_next=self.forwarder_chain_next.saturating_add(1);
                    let token=self.forwarder_chain_next;
                    self.forwarder_chain_last.insert(identity,token);
                    chain=Some(PreparedChain{base:update.base.clone(),predecessor,token,snapshot});
                }
                Err(_) => {
                    if let Some(base)=base {
                        self.forwarder_chain_last.remove(&(session.to_string(),base.to_string()));
                    }
                }
            }
        }
        PreparedFrame{decoded,parsed:Some(parsed),previous_l2,chain}
    }

    /// Original R1378 stateless JSON decode contract; used unchanged by Engine.
    pub fn decode_json(raw: &str) -> Option<Value> {
        serde_json::from_str::<Value>(raw).ok()
    }

    /// Ordered per-session L2/typed preparation. No freshness or authority
    /// decisions here: the Engine owns all admission and reset boundaries.
    pub(crate) fn prepare_update(
        &mut self, session: &str, frame: &RawFrame,
        decoded: Option<&Value>, incremental_books: bool,
    ) -> Result<Update, String> {
        if incremental_books {
            match decoded {
                Some(value) => self.okx_l2.apply_parsed(session, frame, value),
                None => self.okx_l2.apply(session, frame),
            }
        } else {
            match decoded {
                Some(value) => parse_json(frame, value).map_err(|e| format!("{e:?}")),
                None => parse(frame).map_err(|e| format!("{e:?}")),
            }
        }
    }

    pub(crate) fn diagnostic_state(&self, session: &str, base: &str) -> Option<(i64, usize, usize)> {
        self.okx_l2.diagnostic_state(session, base)
    }

    pub(crate) fn clear_session(&mut self, session: &str) {
        self.okx_l2.clear_session(session);
        self.forwarder_chain_last.retain(|(owner,_),_|owner!=session);
    }

    pub(crate) fn memory_usage_bytes(&self) -> (usize, usize) {
        self.okx_l2.memory_usage_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::MarketDataProcessor;
    use crate::{RawFrame, Venue};

    #[test]
    fn r1384_ordered_okx_books_are_owned_per_session_and_cleared_independently() {
        fn frame(raw: &str) -> RawFrame {
            RawFrame { venue: Venue::OkxSpot, epoch: 1, received_ns: 10,
                received_wall_ms: 1_000, raw: raw.into(), source: None }
        }
        let snapshot = frame(r#"{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"snapshot","data":[{"ts":"1000","seqId":10,"prevSeqId":-1,"bids":[["100","1"]],"asks":[["102","1"]]}]}"#);
        let delta = frame(r#"{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"update","data":[{"ts":"1001","seqId":11,"prevSeqId":10,"bids":[["100","2"]],"asks":[]}]}"#);
        let snapshot_value: serde_json::Value = serde_json::from_str(&snapshot.raw).unwrap();
        let delta_value: serde_json::Value = serde_json::from_str(&delta.raw).unwrap();
        let mut processor = MarketDataProcessor::default();
        assert_eq!(processor.prepare_update("hot-a", &snapshot, Some(&snapshot_value), true).unwrap().book.sequence, Some(10));
        assert_eq!(processor.prepare_update("hot-b", &snapshot, None, true).unwrap().book.sequence, Some(10));
        assert_eq!(processor.memory_usage_bytes().0, 2);
        assert_eq!(processor.prepare_update("hot-a", &delta, Some(&delta_value), true).unwrap().book.sequence, Some(11));
        assert_eq!(processor.diagnostic_state("hot-b", "BTC").map(|state| state.0), Some(10));
        processor.clear_session("hot-a");
        assert!(processor.diagnostic_state("hot-a", "BTC").is_none());
        assert_eq!(processor.memory_usage_bytes().0, 1);
        assert!(processor.prepare_update("hot-a", &delta, Some(&delta_value), true).is_err(), "old session delta must not rebuild L2 without snapshot");
        assert_eq!(processor.prepare_update("hot-b", &delta, Some(&delta_value), true).unwrap().book.sequence, Some(11));
    }

    #[test]
    fn r1378_market_json_stage_parity_binance_okx_and_malformed() {
        for raw in [
            r#"{"stream":"btcusdt@depth20@100ms","data":{"s":"BTCUSDT","E":123,"b":[],"a":[]}}"#,
            r#"{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"snapshot","data":[{"ts":"123","seqId":1}]}"#,
            r#"{"invalid":true}"#,
            "{bad json",
            "",
        ] {
            let before = serde_json::from_str::<serde_json::Value>(raw).ok();
            let after = MarketDataProcessor::decode_json(raw);
            assert_eq!(before, after, "decode change on {raw:?}");
        }
    }
}
