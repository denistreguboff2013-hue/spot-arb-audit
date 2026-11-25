//! R1200: offline audit of the *existing* Owner contract, not a live bridge.
//! Deliberately demonstrates gaps that must be resolved before DB/PAPER wiring.
#[path="../quality_manager_r1195.rs"]
mod manager;
fn main(){println!("R1200 OWNER CONTRACT OFFLINE TESTS ONLY")}
#[cfg(test)]
mod tests {
    use super::manager::Manager;
    use spotarb_native_market_core::{Owner,RawFrame,Venue,HistorySink,Update,CoreError,owner_index};
    #[derive(Default)] struct Sink {accepted:usize}
    impl HistorySink for Sink {
        fn accept(&mut self,_:&RawFrame,_:&Update)->Result<(),CoreError>{
            self.accepted+=1;Ok(())
        }
    }
    fn frame(epoch:u64,seq:i64,e:i64,received:i64,ns:u64)->RawFrame{
        RawFrame{source:None,venue:Venue::BinanceFutures,epoch,received_ns:ns,received_wall_ms:received,
            raw:format!(r#"{{"stream":"btcusdt@depth20@500ms","data":{{"e":"depthUpdate","s":"BTCUSDT","E":{e},"u":{seq},"b":[["1.00","2"]],"a":[["1.10","3"]]}}}}"#)}
    }
    #[test]
    fn old_epoch_is_denied_and_new_full_snapshot_is_accepted(){
        let mut owner=Owner::new(owner_index("BTC",1),1,800).unwrap();let mut h=Sink::default();
        let a=frame(1,10,1000,1100,1_100_000_000);
        owner.ingest(&a,&mut h).unwrap();
        owner.reset_venue("BTC",Venue::BinanceFutures,2).unwrap();
        let old=frame(1,11,1200,1300,1_300_000_000);
        assert_eq!(owner.ingest(&old,&mut h),Err(CoreError::StaleEpoch));
        let next=frame(2,11,1200,1300,1_300_000_000);
        owner.ingest(&next,&mut h).unwrap();
        assert_eq!(owner.snapshot_venue("BTC",Venue::BinanceFutures).unwrap().epoch,2);
        assert_eq!(h.accepted,2);
    }
    #[test]
    fn protected_raw_frame_fences_standby_and_missing_identity(){
        let mut owner=Owner::new(0,1,800).unwrap();let mut h=Sink::default();
        let mut active=frame(1,10,1000,1100,1_100_000_000);
        let mut p=spotarb_native_market_core::market_data_ingress_r1205::provenance("active".into(),1);
        p.journal_id="a".repeat(64);p.accepted_id=1;p.selection_epoch=1;p.payload_sha256=openssl::sha::sha256(active.raw.as_bytes()).iter().map(|b|format!("{b:02x}")).collect();active.source=Some(p);
        owner.ingest(&active,&mut h).unwrap();
        let mut next=frame(1,11,1200,1300,1_300_000_000);let mut p=active.source.clone().unwrap();p.session_id="standby".into();p.accepted_id=2;p.payload_sha256=openssl::sha::sha256(next.raw.as_bytes()).iter().map(|b|format!("{b:02x}")).collect();next.source=Some(p);
        assert_eq!(owner.ingest(&next,&mut h),Err(CoreError::StaleEpoch));
        next.source=None;assert!(owner.ingest(&next,&mut h).is_err());assert_eq!(h.accepted,1);
    }
    #[test]
    fn manager_equal_u_new_e_contract_matches_owner(){
        let mut m=Manager::default();m.define_group("g",vec!["BTCUSDT".into()]).unwrap();
        m.add_session(1,"peer-A","g").unwrap();
        assert!(m.observe(1,"BTCUSDT",1000,1100,1100,Some(10),true).unwrap());
        assert!(!m.observe(1,"BTCUSDT",1200,1300,1300,Some(10),true).unwrap());
        let mut owner=Owner::new(0,1,800).unwrap();let mut h=Sink::default();
        owner.ingest(&frame(1,10,1000,1100,1_100_000_000),&mut h).unwrap();
        assert_eq!(owner.ingest(&frame(1,10,1200,1300,1_300_000_000),&mut h),
                   Err(CoreError::OutOfOrder));
        assert_eq!(h.accepted,1);
    }
}
