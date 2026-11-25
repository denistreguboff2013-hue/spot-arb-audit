// R1157: opt-in, bounded public-paper WS epoch observability.
// No new sockets, writes, private API, order permissions, or changes to reset policy.
// "poll_wait" is FUTURE wait time, NOT a kernel receive or task wake timestamp.
#[derive(Default)]
struct R1157Window {
    frames: u64,
    text_bytes: u64,
    polls: u64,
    poll_timeouts: u64,
    max_poll_wait_ms: u64,
    max_frame_gap_ms: u64,
    source_first: Option<i64>,
    source_last: Option<i64>,
    source_max_age_ms: i64,
    stale_800ms: u64,
    ping_count: u64,
    pong_flush_failures: u64,
    max_pong_flush_ms: u64,
    malformed_or_no_e: u64,
    sender_samples_us: Vec<u32>,
    sender_max_us: u64,
    sender_over_10ms: u64,
    sender_over_250ms: u64,
    sender_full_at_start: u64,
    next_poll_gap_max_us: u64,
}
struct R1157EpochProbe {
    enabled: bool,
    epoch_started: Instant,
    window_started: Instant,
    last_frame: Option<Instant>,
    last_poll_return: Option<Instant>,
    local_port: Option<u16>,
    w: R1157Window,
}
impl R1157EpochProbe {
    fn new(local_port: Option<u16>) -> Self {
        let enabled = std::env::var("ASTRA_R1157_WS_EPOCH_OBSERVE").ok().as_deref()==Some("YES")
            && (std::env::var("ASTRA_R1119_FULL_RUST_PAPER").ok().as_deref()==Some("YES")
                || r1129_paper_batch32() && std::env::var("ASTRA_R1179_COMPACT_BATCH32").ok().as_deref()==Some("YES"))
            && std::env::var("ASTRA_R1087_PAPER_PUBLIC_ONLY").ok().as_deref()==Some("YES")
            && std::env::var("ASTRA_R1136_RELEASE_PAPER").ok().as_deref()==Some("YES");
        Self::with_enabled(enabled, local_port)
    }
    fn with_enabled(enabled: bool, local_port: Option<u16>) -> Self {
        Self { enabled, epoch_started: Instant::now(),
            window_started: Instant::now(), last_frame: None, last_poll_return: None,
            local_port, w: R1157Window::default() }
    }
    fn next_poll_started(&mut self) {
        if !self.enabled {return}
        if let Some(last)=self.last_poll_return {
            self.w.next_poll_gap_max_us=self.w.next_poll_gap_max_us.max(
                last.elapsed().as_micros().min(u64::MAX as u128) as u64);
        }
    }
    fn sender_sent(&mut self, elapsed:Duration, full_at_start:bool) {
        if !self.enabled {return}
        let us=elapsed.as_micros().min(u64::MAX as u128) as u64;
        self.w.sender_max_us=self.w.sender_max_us.max(us);
        self.w.sender_over_10ms+=u64::from(us>10_000);
        self.w.sender_over_250ms+=u64::from(us>250_000);
        self.w.sender_full_at_start+=u64::from(full_at_start);
        // Bounded per-window sampling. No market payload or per-frame log.
        if self.w.sender_samples_us.len()<4096 {
            self.w.sender_samples_us.push(us.min(u32::MAX as u64) as u32)
        }
    }
    fn poll_return(&mut self, elapsed: Duration, timed_out: bool) {
        if !self.enabled {return}
        self.last_poll_return=Some(Instant::now());
        self.w.polls=self.w.polls.saturating_add(1);
        self.w.poll_timeouts=self.w.poll_timeouts.saturating_add(timed_out as u64);
        self.w.max_poll_wait_ms=self.w.max_poll_wait_ms.max(
            elapsed.as_millis().min(u64::MAX as u128) as u64);
    }
    fn text_read(&mut self, raw: &str, wall_ms: i64) {
        if !self.enabled {return}
        let now=Instant::now();
        if let Some(last)=self.last_frame {
            self.w.max_frame_gap_ms=self.w.max_frame_gap_ms.max(
                now.duration_since(last).as_millis().min(u64::MAX as u128) as u64);
        }
        self.last_frame=Some(now);
        self.w.frames=self.w.frames.saturating_add(1);
        self.w.text_bytes=self.w.text_bytes.saturating_add(raw.len() as u64);
        let event=serde_json::from_str::<serde_json::Value>(raw).ok()
            .and_then(|v| v.get("data").and_then(|d|d.get("E")).and_then(|e|e.as_i64()));
        match event {
            Some(e) => {
                if self.w.source_first.is_none() {self.w.source_first=Some(e)}
                self.w.source_last=Some(e);
                let age=wall_ms.saturating_sub(e);
                self.w.source_max_age_ms=self.w.source_max_age_ms.max(age);
                if age>800 {self.w.stale_800ms=self.w.stale_800ms.saturating_add(1)}
            },
            None => self.w.malformed_or_no_e=self.w.malformed_or_no_e.saturating_add(1),
        }
    }
    fn pong_flush(&mut self, elapsed:Duration, ok:bool) {
        if !self.enabled {return}
        self.w.ping_count=self.w.ping_count.saturating_add(1);
        if !ok {self.w.pong_flush_failures=self.w.pong_flush_failures.saturating_add(1)}
        self.w.max_pong_flush_ms=self.w.max_pong_flush_ms.max(
            elapsed.as_millis().min(u64::MAX as u128) as u64);
    }
    fn maybe_report(&mut self, shard:usize, epoch:u64) {
        if self.enabled && self.window_started.elapsed()>=Duration::from_secs(10) {
            self.report(shard,epoch,"window");
        }
    }
    fn report(&mut self, shard:usize, epoch:u64, reason:&str) {
        if !self.enabled {return}
        let mut w=std::mem::take(&mut self.w);
        w.sender_samples_us.sort_unstable();
        let pct=|n:usize|->Option<u32>{
            if w.sender_samples_us.is_empty(){None}else{
                Some(w.sender_samples_us[(w.sender_samples_us.len()-1)*n/100])
            }
        };
        let sender_p50=pct(50);let sender_p95=pct(95);let sender_p99=pct(99);
        let sender_samples=w.sender_samples_us.len();
        eprintln!("R1157_WS_EPOCH_OBSERVE {}",serde_json::json!({
            "schema":"SPOTARB/R1157/PUBLIC_WS_EPOCH_WINDOW/V1",
            "utc_wall_ms":wall_ms(),
            "monotonic_epoch_elapsed_ms":self.epoch_started.elapsed().as_millis(),
            "window_elapsed_ms":self.window_started.elapsed().as_millis(),
            "pid":std::process::id(),"shard":shard,"epoch":epoch,
            "local_port":self.local_port,"reason":reason,
            "text_frames":w.frames,"text_bytes":w.text_bytes,
            "polls":w.polls,"poll_timeouts":w.poll_timeouts,
            "max_ws_next_future_wait_ms":w.max_poll_wait_ms,
            "max_text_interarrival_ms":w.max_frame_gap_ms,
            "next_poll_gap_max_us":w.next_poll_gap_max_us,
            "sender_sample_count":sender_samples,
            "sender_block_p50_us":sender_p50,"sender_block_p95_us":sender_p95,
            "sender_block_p99_us":sender_p99,"sender_block_max_us":w.sender_max_us,
            "sender_block_over_10ms":w.sender_over_10ms,
            "sender_block_over_250ms":w.sender_over_250ms,
            "sender_full_at_start":w.sender_full_at_start,
            "source_first_e_ms":w.source_first,"source_last_e_ms":w.source_last,
            "source_max_age_ms":w.source_max_age_ms,"source_stale_800ms":w.stale_800ms,
            "missing_source_e":w.malformed_or_no_e,
            "pings":w.ping_count,"pong_flush_errors":w.pong_flush_failures,
            "max_pong_flush_ms":w.max_pong_flush_ms,
            "kernel_recv_timestamp_measured":false,
            "payload_logged":false,"network_side_effects":false
        }));
        self.window_started=Instant::now();
    }
}
#[cfg(test)]
mod r1157_probe_tests {
    use super::*;
    #[test] fn malformed_source_and_timeouts_fail_as_metrics_only() {
        let mut x=R1157EpochProbe::with_enabled(true,Some(12345));
        x.poll_return(Duration::from_millis(2000),true);
        x.text_read(r#"{"data":{"E":1000}}"#,1100);
        x.text_read(r#"{"data":{"E":1001}}"#,2002);
        x.text_read("not json",2003);
        x.pong_flush(Duration::from_millis(31),false);
        assert_eq!((x.w.frames,x.w.poll_timeouts,x.w.stale_800ms),(3,1,1));
        assert_eq!((x.w.source_first,x.w.source_last),(Some(1000),Some(1001)));
        assert_eq!((x.w.pong_flush_failures,x.w.malformed_or_no_e),(1,1));
    }
    #[test] fn sender_and_poll_gap_are_bounded_passive_aggregates() {
        let mut x=R1157EpochProbe::with_enabled(true,None);
        x.poll_return(Duration::from_millis(1),false);
        x.next_poll_started();
        x.sender_sent(Duration::from_millis(251),true);
        assert_eq!(x.w.sender_samples_us,vec![251_000]);
        assert_eq!((x.w.sender_full_at_start,x.w.sender_over_10ms,
            x.w.sender_over_250ms),(1,1,1));
        x.report(0,1,"test");
        assert!(x.w.sender_samples_us.is_empty());
    }
    #[test] fn disabled_probe_does_not_parse_or_count() {
        let mut x=R1157EpochProbe::with_enabled(false,None);
        x.poll_return(Duration::from_secs(5),true);
        x.text_read("not json",1);
        assert_eq!((x.w.polls,x.w.frames,x.w.malformed_or_no_e),(0,0,0));
    }
}
