// R1158: opt-in client Ping/Pong RTT probe on the EXISTING Binance PUBLIC socket.
// No new connections, private traffic, orders, reconnect quotas or trade gates.
struct R1158WsPingProbe {
    enabled:bool, connected:Instant, last_sent:Instant, serial:u64,
    pending:Option<(Vec<u8>,Instant)>, overdue_logged:bool,
}
impl R1158WsPingProbe {
    fn new()->Self{
        let enabled=std::env::var("ASTRA_R1158_ACTIVE_PING_DIAGNOSTICS").ok().as_deref()==Some("YES")
            &&std::env::var("ASTRA_R1157_WS_EPOCH_OBSERVE").ok().as_deref()==Some("YES")
            &&std::env::var("ASTRA_R1119_FULL_RUST_PAPER").ok().as_deref()==Some("YES")
            &&std::env::var("ASTRA_R1087_PAPER_PUBLIC_ONLY").ok().as_deref()==Some("YES")
            &&std::env::var("ASTRA_R1136_RELEASE_PAPER").ok().as_deref()==Some("YES");
        Self::new_with(enabled)
    }
    fn new_with(enabled:bool)->Self{
        Self{enabled,connected:Instant::now(),last_sent:Instant::now(),
            serial:0,pending:None,overdue_logged:false}
    }
    fn due(&self)->bool{
        self.enabled&&self.pending.is_none()&&self.last_sent.elapsed()>=Duration::from_secs(30)
    }
    fn make_payload(&mut self,shard:usize,epoch:u64)->Vec<u8>{
        self.serial=self.serial.saturating_add(1);
        format!("SA:{}:{}:{}",shard,epoch,self.serial).into_bytes()
    }
    fn log(&self,shard:usize,epoch:u64,port:Option<u16>,what:&str,elapsed_ms:Option<u64>){
        if !self.enabled{return}
        eprintln!("R1158_WS_PING_DIAG {}",serde_json::json!({
            "schema":"SPOTARB/R1158/PUBLIC_PING_PROBE/V1",
            "utc_wall_ms":wall_ms(),"mono_epoch_elapsed_ms":self.connected.elapsed().as_millis(),
            "shard":shard,"epoch":epoch,"local_port":port,"event":what,
            "elapsed_ms":elapsed_ms,"pending":self.pending.is_some(),
            "client_ping_interval_ms":30000,"diagnostic_only":true,"payload_logged":false
        }));
    }
    fn sent(&mut self,payload:Vec<u8>,shard:usize,epoch:u64,port:Option<u16>,write_ms:u64){
        if !self.enabled{return}
        self.last_sent=Instant::now();
        self.pending=Some((payload,self.last_sent));
        self.overdue_logged=false;
        self.log(shard,epoch,port,"ping_sent",Some(write_ms));
    }
    fn pong(&mut self,payload:&[u8],shard:usize,epoch:u64,port:Option<u16>){
        if !self.enabled{return}
        match self.pending.as_ref(){
            Some((expected,at)) if expected.as_slice()==payload=>{
                let ms=at.elapsed().as_millis().min(u64::MAX as u128) as u64;
                self.pending=None;
                self.log(shard,epoch,port,"matched_pong",Some(ms));
            },
            _=>self.log(shard,epoch,port,"unmatched_pong",None),
        }
    }
    fn check_overdue(&mut self,shard:usize,epoch:u64,port:Option<u16>){
        if !self.enabled||self.overdue_logged{return}
        if let Some((_,at))=self.pending.as_ref(){
            if at.elapsed()>=Duration::from_secs(10){
                let ms=at.elapsed().as_millis().min(u64::MAX as u128) as u64;
                self.overdue_logged=true;
                self.log(shard,epoch,port,"pong_not_seen_10s",Some(ms));
            }
        }
    }
    fn send_error(&mut self,shard:usize,epoch:u64,port:Option<u16>,ms:u64){
        self.log(shard,epoch,port,"ping_send_error",Some(ms));
    }
}
#[cfg(test)]
mod r1158_ping_probe_tests{
    use super::*;
    #[test] fn default_disarmed_and_one_outstanding(){
        let mut p=R1158WsPingProbe::new_with(false);
        assert!(!p.due());
        p.sent(vec![1],1,2,None,1);
        assert!(p.pending.is_none());
        let mut q=R1158WsPingProbe::new_with(true);
        q.last_sent=Instant::now()-Duration::from_secs(31);
        assert!(q.due());
        let a=q.make_payload(1,2);
        q.sent(a.clone(),1,2,Some(12345),0);
        assert!(!q.due());
        q.pong(b"wrong",1,2,Some(12345));
        assert!(q.pending.is_some());
        q.pong(&a,1,2,Some(12345));
        assert!(q.pending.is_none());
        assert!(!q.due());
    }
    #[test] fn overdue_diagnostic_never_reconnects_or_resends(){
        let mut p=R1158WsPingProbe::new_with(true);
        p.sent(vec![5],0,1,None,0);
        if let Some((_,at))=p.pending.as_mut(){*at=Instant::now()-Duration::from_secs(11)}
        p.check_overdue(0,1,None);
        assert!(p.overdue_logged);
        assert!(!p.due());
        p.pong(&[5],0,1,None);
        assert!(p.pending.is_none());
    }
}
