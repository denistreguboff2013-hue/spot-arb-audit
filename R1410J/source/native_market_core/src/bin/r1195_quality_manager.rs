//! R1195: offline-only quality manager entrypoint and acceptance tests.
//! No Binance network requests, no DB, no trading or installed PAPER integration.
#[path = "../quality_manager_r1195.rs"]
mod manager;
fn main() {
    println!("R1195 QUALITY_MANAGER_CORE: OFFLINE_ONLY; run cargo test --bin r1195_quality_manager");
}
#[cfg(test)]
mod tests {
    use super::manager::*;
    fn setup() -> Manager {
        let mut m=Manager::default();
        m.define_group("g",vec!["SOLUSDT".into(),"SEIUSDT".into()]).unwrap();
        m.add_session(1,"peer-A","g").unwrap();
        m.add_session(2,"peer-B","g").unwrap();
        m
    }
    fn feed(m:&mut Manager,id:u64,base:i64,mono:u64,seq:u64) {
        for (sym,n) in [("SOLUSDT",0_u64),("SEIUSDT",1)] {
            let recv_utc=base+mono as i64;
            assert!(m.observe(id,sym,recv_utc-100,recv_utc,mono,Some(seq*2+n),true).unwrap());
        }
    }
    fn prime(m:&mut Manager,id:u64,base:i64) {
        for (i,t) in [0,500,1000,1500].iter().enumerate() {
            feed(m,id,base,*t,(i+1) as u64);
        }
    }
    #[test]
    fn only_full_group_and_stable_three_samples_are_promotable() {
        let mut m=setup();let base=1_800_000_000_000_i64;
        feed(&mut m,1,base,0,1);
        assert!(m.promote("g",1,base+50,50).is_err());
        prime(&mut m,2,base);
        assert_eq!(m.promote("g",2,base+1550,1550).unwrap(),1);
        assert!(m.publish("g","SOLUSDT",base+1550,1550).is_some());
        assert!(m.publish("g","SOLUSDT",base+1550,1550).is_none());
    }
    #[test]
    fn fast_hot_failover_no_retry_and_monotonic_publish() {
        let mut m=setup();let base=1_800_000_000_000_i64;
        prime(&mut m,1,base);prime(&mut m,2,base);
        let e1=m.promote("g",1,base+1550,1550).unwrap();
        assert_eq!(m.publish("g","SOLUSDT",base+1550,1550).unwrap().epoch,e1);
        let chosen=m.suggest_ready_reserve("g",base+1570,1570).unwrap();
        assert_eq!(chosen,2);
        m.invalidate(1).unwrap();
        assert!(m.publish("g","SOLUSDT",base+1570,1570).is_none());
        let e2=m.promote("g",2,base+1570,1570).unwrap();
        assert!(e2>e1);
        // Same event already published on old source must NOT be duplicated.
        assert!(m.publish("g","SOLUSDT",base+1570,1570).is_none());
        feed(&mut m,2,base,1900,5);
        assert_eq!(m.publish("g","SOLUSDT",base+1940,1940).unwrap().epoch,e2);
    }
    #[test]
    fn stale_frame_fail_closed_even_with_previous_good_quote() {
        let mut m=setup();let base=1_800_000_000_000_i64;
        prime(&mut m,1,base);m.promote("g",1,base+1550,1550).unwrap();
        assert!(!m.observe(1,"SOLUSDT",base+100,base+1800,1800,Some(101),true).unwrap());
        assert!(m.publish("g","SOLUSDT",base+1800,1800).is_none());
        assert!(m.publish("g","SEIUSDT",base+2350,2350).is_none());
    }
    #[test]
    fn invalid_or_missing_snapshot_never_qualifies() {
        let mut m=setup();let base=1_800_000_000_000_i64;
        for t in [0,500,1000,1500] {
            assert!(m.observe(1,"SOLUSDT",base+t-100,base+t,t as u64,Some(t as u64+1),true).unwrap());
        }
        assert!(m.promote("g",1,base+1550,1550).is_err());
        assert!(!m.observe(1,"SEIUSDT",base+1900,base+2000,2000,Some(1),false).unwrap());
        assert!(m.promote("g",1,base+2000,2000).is_err());
    }
    #[test]
    fn future_event_and_clock_backward_rejected() {
        let mut m=setup();let base=1_800_000_000_000_i64;
        assert!(!m.observe(1,"SOLUSDT",base+100,base,0,None,true).unwrap());
        assert!(m.observe(1,"SOLUSDT",base+50,base+100,100,None,true).unwrap());
        assert!(m.observe(1,"SOLUSDT",base+100,base+200,90,None,true).is_err());
    }
    #[test]
    fn descending_and_duplicate_update_id_rejected() {
        let mut m=setup();let base=1_800_000_000_000_i64;
        assert!(m.observe(1,"SOLUSDT",base,base+50,0,Some(100),true).unwrap());
        assert!(!m.observe(1,"SOLUSDT",base,base+550,500,Some(100),true).unwrap());
        assert!(!m.observe(1,"SOLUSDT",base+500,base+550,550,Some(100),true).unwrap());
        assert!(!m.observe(1,"SOLUSDT",base+1000,base+1050,1000,Some(99),true).unwrap());
        assert!(!m.observe(1,"SOLUSDT",base+1200,base+1250,1200,None,true).unwrap());
    }
    #[test]
    fn no_backup_older_than_last_published() {
        let mut m=setup();let base=1_800_000_000_000_i64;
        prime(&mut m,1,base);prime(&mut m,2,base);
        m.promote("g",1,base+1550,1550).unwrap();
        m.publish("g","SOLUSDT",base+1550,1550).unwrap();
        feed(&mut m,1,base,1900,5);
        m.publish("g","SOLUSDT",base+1940,1940).unwrap();
        assert!(m.promote("g",2,base+1950,1950).is_err());
    }
    #[test]
    fn quarantined_active_cannot_publish_and_invalid_metrics_rejected() {
        let mut m=setup();let base=1_800_000_000_000_i64;
        assert!(m.update_tcp(1,f64::NAN,0.0).is_err());
        assert!(m.update_tcp(1,200.0,-1.0).is_err());
        prime(&mut m,1,base);
        m.promote("g",1,base+1550,1550).unwrap();
        m.invalidate(1).unwrap();
        assert!(m.publish("g","SOLUSDT",base+1570,1570).is_none());
        assert!(m.promote("g",1,base+1570,1570).is_err());
    }
    #[test]
    fn quality_history_atomic_roundtrip_and_ttl() {
        let p=std::env::temp_dir().join(format!("r1195_history_test_{}.json",std::process::id()));
        let _=std::fs::remove_file(&p);let _=std::fs::remove_file(p.with_extension("json.pending"));
        let mut m=setup();let base=1_800_000_000_000_i64;
        prime(&mut m,1,base);m.update_tcp(1,270.0,0.001).unwrap();
        m.record_quality(1,base+1550).unwrap();
        m.history.save_atomic(&p).unwrap();
        let h=History::load(&p,base+1700).unwrap();
        assert_eq!(h.peers.len(),1);assert_eq!(h.preferred(),vec!["peer-A"]);
        assert_eq!(History::load(&p,base+7*3600*1000).unwrap().peers.len(),0);
        std::fs::remove_file(&p).unwrap();
    }
    #[test]
    fn r1200_history_save_survives_orphan_pending_after_crash() {
        let dir=std::env::temp_dir().join(format!("r1200_history_crash_{}",std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path=dir.join("history.json");
        let legacy=path.with_extension("json.pending");
        std::fs::write(&legacy,b"orphan after crash").unwrap();
        let mut h=History::default();
        h.peers.push(PeerRecord{peer:"peer-A".into(),updated_utc_ms:1000,
            tested_frames:1,stale_frames:0,median_rtt_ms:None,ooo_ratio:None});
        h.save_atomic(&path).unwrap();
        assert_eq!(History::load(&path,1001).unwrap().peers.len(),1);
        assert_eq!(std::fs::read(&legacy).unwrap(),b"orphan after crash");
        std::fs::remove_file(path).unwrap();std::fs::remove_file(legacy).unwrap();
        std::fs::remove_dir(dir).unwrap();
    }
    #[test]
    fn no_reuse_session_id_and_no_unknown_symbol() {
        let mut m=setup();let base=1_800_000_000_000_i64;
        assert!(m.add_session(1,"peer-C","g").is_err());
        assert!(m.observe(1,"BTCUSDT",base,base+100,100,None,true).is_err());
        assert!(m.define_group("x",vec!["BTCUSDT".into(),"BTCUSDT".into()]).is_err());
    }

    #[test]
    fn r1198_watchdog_healthy_does_not_switch() {
        let mut m=setup();let base=1_800_000_000_000_i64;
        prime(&mut m,1,base);prime(&mut m,2,base);
        m.promote("g",1,base+1550,1550).unwrap();
        assert_eq!(m.watchdog("g",base+1700,1700).unwrap(),WatchdogOutcome::Healthy);
        assert_eq!(m.active.get("g").unwrap().session_id,1);
    }
    #[test]
    fn r1198_stale_open_socket_switches_to_fresh_hot() {
        let mut m=setup();let base=1_800_000_000_000_i64;
        prime(&mut m,1,base);prime(&mut m,2,base);
        m.promote("g",1,base+1550,1550).unwrap();
        assert!(m.publish("g","SOLUSDT",base+1570,1570).is_some());
        feed(&mut m,2,base,3100,6);
        let event=m.watchdog("g",base+3210,3210).unwrap();
        assert_eq!(event,WatchdogOutcome::Switched{old_id:1,new_id:2,new_epoch:2});
        assert_eq!(m.sessions.get(&1).unwrap().tier,Tier::Quarantined);
        assert!(m.publish("g","SOLUSDT",base+3215,3215).is_some());
        assert!(m.publish("g","SOLUSDT",base+3215,3215).is_none());
        assert_eq!(m.active.get("g").unwrap().session_id,2);
    }
    #[test]
    fn r1198_no_hot_backup_quarantines_and_blocks() {
        let mut m=setup();let base=1_800_000_000_000_i64;
        prime(&mut m,1,base);
        m.promote("g",1,base+1550,1550).unwrap();
        assert_eq!(m.watchdog("g",base+3210,3210).unwrap(),
          WatchdogOutcome::Blocked{old_id:1});
        assert!(m.publish("g","SOLUSDT",base+3210,3210).is_none());
        assert_eq!(m.watchdog("g",base+3225,3225).unwrap(),WatchdogOutcome::NoActive);
    }
    #[test]
    fn r1198_rejects_backup_with_backwards_event() {
        let mut m=setup();let base=1_800_000_000_000_i64;
        prime(&mut m,1,base);prime(&mut m,2,base);
        m.promote("g",1,base+1550,1550).unwrap();
        feed(&mut m,1,base,2100,6);
        assert!(m.publish("g","SOLUSDT",base+2150,2150).is_some());
        // Source 2 refreshed only the other symbol; its SOL update is older
        // than last published, but could still satisfy age<=800 on tick.
        assert!(m.observe(2,"SEIUSDT",base+2190,base+2200,2200,Some(200),true).unwrap());
        // At 2901, old SOL on reserve is stale too: never promote.
        assert_eq!(m.watchdog("g",base+3910,3910).unwrap(),
          WatchdogOutcome::Blocked{old_id:1});
        assert!(m.publish("g","SOLUSDT",base+3910,3910).is_none());
    }
    #[test]
    fn r1198_one_stale_required_symbol_blocks_group() {
        let mut m=setup();let base=1_800_000_000_000_i64;
        prime(&mut m,1,base);prime(&mut m,2,base);
        m.promote("g",1,base+1550,1550).unwrap();
        assert!(m.observe(1,"SOLUSDT",base+2100,base+2200,2200,Some(500),true).unwrap());
        feed(&mut m,2,base,2200,7);
        assert_eq!(m.watchdog("g",base+2330,2330).unwrap(),
          WatchdogOutcome::Healthy);
        assert_eq!(m.active.get("g").unwrap().session_id,1);
        assert!(m.publish("g","SEIUSDT",base+2330,2330).is_none());
    }

    #[test]
    fn r1199_short_pause_no_switch_even_with_two_hot_reserves() {
        let base=1_800_000_000_000_i64;let mut m=setup();
        m.add_session(3,"peer-C","g").unwrap();
        prime(&mut m,1,base);prime(&mut m,2,base);prime(&mut m,3,base);
        m.promote("g",1,base+1550,1550).unwrap();
        feed(&mut m,2,base,2950,7);
        feed(&mut m,3,base,2950,7);
        assert_eq!(m.watchdog_correlated_r1199("g",base+2990,2990).unwrap(),
                   WatchdogOutcome::Healthy);
    }
    #[test]
    fn r1199_silent_with_two_independent_fresh_routes_switches() {
        let base=1_800_000_000_000_i64;let mut m=setup();
        m.add_session(3,"peer-C","g").unwrap();
        prime(&mut m,1,base);prime(&mut m,2,base);prime(&mut m,3,base);
        m.promote("g",1,base+1550,1550).unwrap();
        assert!(m.publish("g","SOLUSDT",base+1550,1550).is_some());
        feed(&mut m,2,base,4250,8);feed(&mut m,3,base,4250,8);
        let res=m.watchdog_correlated_r1199("g",base+4300,4300).unwrap();
        assert!(matches!(res,WatchdogOutcome::Switched{old_id:1,new_epoch:2,..}));
        assert_ne!(m.active.get("g").unwrap().session_id,1);
        assert!(m.publish("g","SOLUSDT",base+4330,4330).is_some());
    }
    #[test]
    fn r1199_all_market_quiet_does_not_quarantine_route() {
        let base=1_800_000_000_000_i64;let mut m=setup();
        m.add_session(3,"peer-C","g").unwrap();
        prime(&mut m,1,base);prime(&mut m,2,base);prime(&mut m,3,base);
        m.promote("g",1,base+1550,1550).unwrap();
        let outcome=m.watchdog_correlated_r1199("g",base+5200,5200).unwrap();
        assert!(matches!(outcome,WatchdogOutcome::WaitingForEvidence{
            old_id:1,qualified_backups:0,..}));
        assert_eq!(m.active.get("g").unwrap().session_id,1);
        assert!(m.publish("g","SOLUSDT",base+5200,5200).is_none());
    }
    #[test]
    fn r1199_only_one_independent_backup_abstains() {
        let base=1_800_000_000_000_i64;let mut m=setup();
        prime(&mut m,1,base);prime(&mut m,2,base);
        m.promote("g",1,base+1550,1550).unwrap();
        feed(&mut m,2,base,4300,8);
        let x=m.watchdog_correlated_r1199("g",base+4350,4350).unwrap();
        assert!(matches!(x,WatchdogOutcome::WaitingForEvidence{old_id:1,qualified_backups:1,..}));
        assert_eq!(m.active.get("g").unwrap().session_id,1);
        assert!(m.publish("g","SOLUSDT",base+4350,4350).is_none());
    }
    #[test]
    fn r1200_one_bad_symbol_cannot_quarantine_receiving_active_group() {
        let base=1_800_000_000_000_i64;let mut m=setup();
        m.add_session(3,"peer-C","g").unwrap();
        prime(&mut m,1,base);prime(&mut m,2,base);prime(&mut m,3,base);
        m.promote("g",1,base+1550,1550).unwrap();
        feed(&mut m,1,base,2200,7);
        feed(&mut m,2,base,2450,8);feed(&mut m,3,base,2450,8);
        for i in 0..25u64 {
            assert!(!m.observe(1,"SOLUSDT",base-10_000,base+2500+i as i64,
                2500+i,Some(100+i),true).unwrap());
        }
        assert_eq!(m.watchdog_correlated_r1199("g",base+2540,2540).unwrap(),
            WatchdogOutcome::Healthy);
        assert_eq!(m.active.get("g").unwrap().session_id,1);
        assert!(m.publish("g","SOLUSDT",base+2540,2540).is_none());
        assert!(m.publish("g","SEIUSDT",base+2540,2540).is_some());
    }
    #[test]
    fn r1200_multi_symbol_degradation_with_quorum_still_switches() {
        let base=1_800_000_000_000_i64;let mut m=setup();
        m.add_session(3,"peer-C","g").unwrap();
        prime(&mut m,1,base);prime(&mut m,2,base);prime(&mut m,3,base);
        m.promote("g",1,base+1550,1550).unwrap();
        feed(&mut m,1,base,2200,7);
        feed(&mut m,2,base,2450,8);feed(&mut m,3,base,2450,8);
        for i in 0..5u64 {
            for sym in ["SOLUSDT","SEIUSDT"] {
                assert!(!m.observe(1,sym,base-10_000,base+2500+i as i64,
                    2500+i,Some(100+i),true).unwrap());
            }
        }
        assert_eq!(m.sessions.get(&1).unwrap().tier,Tier::Suspect);
        let x=m.watchdog_correlated_r1199("g",base+2550,2550).unwrap();
        assert!(matches!(x,WatchdogOutcome::Switched{old_id:1,new_epoch:2,..}));
        assert!(m.publish("g","SOLUSDT",base+2550,2550).is_some());
    }
    #[test]
    fn r1200_multi_symbol_without_quorum_stays_fail_closed() {
        let base=1_800_000_000_000_i64;let mut m=setup();
        prime(&mut m,1,base);prime(&mut m,2,base);
        m.promote("g",1,base+1550,1550).unwrap();
        feed(&mut m,1,base,2200,7);feed(&mut m,2,base,2450,8);
        for i in 0..5u64 {
            for sym in ["SOLUSDT","SEIUSDT"] {
                assert!(!m.observe(1,sym,base-10_000,base+2500+i as i64,
                    2500+i,Some(100+i),true).unwrap());
            }
        }
        assert!(matches!(m.watchdog_correlated_r1199("g",base+2550,2550).unwrap(),
            WatchdogOutcome::WaitingForEvidence{qualified_backups:1,..}));
        assert_eq!(m.active.get("g").unwrap().session_id,1);
        assert!(m.publish("g","SOLUSDT",base+2550,2550).is_none());
        assert!(m.publish("g","SEIUSDT",base+2550,2550).is_none());
    }
    #[test]
    fn r1200_no_quorum_active_can_requalify_same_socket_after_recovery() {
        let base=1_800_000_000_000_i64;let mut m=setup();
        prime(&mut m,1,base);prime(&mut m,2,base);
        m.promote("g",1,base+1550,1550).unwrap();
        feed(&mut m,1,base,2200,7);feed(&mut m,2,base,2450,8);
        for i in 0..5u64 {
            for sym in ["SOLUSDT","SEIUSDT"] {
                assert!(!m.observe(1,sym,base-10_000,base+2500+i as i64,
                    2500+i,Some(100+i),true).unwrap());
            }
        }
        assert!(matches!(m.watchdog_correlated_r1199("g",base+2550,2550).unwrap(),
            WatchdogOutcome::WaitingForEvidence{..}));
        assert!(m.publish("g","SOLUSDT",base+2550,2550).is_none());
        for (i,t) in [65000,65500,66000,66500].iter().enumerate(){
            feed(&mut m,1,base,*t,(i+200) as u64);
        }
        assert_eq!(m.sessions.get(&1).unwrap().tier,Tier::Active);
        assert_eq!(m.active.get("g").unwrap().session_id,1);
        assert_eq!(m.watchdog_correlated_r1199("g",base+66550,66550).unwrap(),
            WatchdogOutcome::Healthy);
        assert!(m.publish("g","SOLUSDT",base+66550,66550).is_some());
    }
    #[test]
    fn r1200_eight_symbol_cohort_boundary_requires_four_bad_symbols() {
        let base=1_800_000_000_000_i64;let mut m=Manager::default();
        let syms=(0..8).map(|i|format!("BASE{i}USDT")).collect::<Vec<_>>();
        m.define_group("g",syms.clone()).unwrap();
        for id in 1..=3 {m.add_session(id,&format!("peer-{id}"),"g").unwrap();}
        for id in 1..=3 {
            for t in [0u64,500,1000,1500] {
                for sym in &syms {assert!(m.observe(id,sym,base+t as i64-100,
                    base+t as i64,t,Some(t+1),true).unwrap());}
            }
        }
        m.promote("g",1,base+1550,1550).unwrap();
        for id in 1..=3 {
            for sym in &syms {assert!(m.observe(id,sym,base+2100,base+2200,
                2200,Some(2201),true).unwrap());}
        }
        for id in 2..=3 {
            for sym in &syms {assert!(m.observe(id,sym,base+2350,base+2450,
                2450,Some(2451),true).unwrap());}
        }
        for i in 0..3 {
            for t in 0..4u64 {assert!(!m.observe(1,&syms[i],base-10_000,
                base+2500+t as i64,2500+t,Some(3000+t),true).unwrap());}
        }
        assert_eq!(m.watchdog_correlated_r1199("g",base+2520,2520).unwrap(),WatchdogOutcome::Healthy);
        for t in 0..4u64 {assert!(!m.observe(1,&syms[3],base-10_000,
            base+2530+t as i64,2530+t,Some(4000+t),true).unwrap());}
        assert!(matches!(m.watchdog_correlated_r1199("g",base+2540,2540).unwrap(),
            WatchdogOutcome::Switched{old_id:1,new_epoch:2,..}));
    }
    #[test]
    fn r1200_selector_skips_lower_rtt_reserve_behind_published_highwater() {
        let base=1_800_000_000_000_i64;let mut m=setup();
        m.add_session(3,"peer-C","g").unwrap();
        prime(&mut m,1,base);prime(&mut m,2,base);prime(&mut m,3,base);
        m.promote("g",1,base+1550,1550).unwrap();
        feed(&mut m,1,base,2300,7);
        m.publish("g","SOLUSDT",base+2340,2340).unwrap();
        feed(&mut m,2,base,2150,8);feed(&mut m,3,base,2500,9);
        m.update_tcp(2,100.0,0.0).unwrap();
        m.update_tcp(3,300.0,0.0).unwrap();
        assert_eq!(m.suggest_ready_reserve("g",base+2550,2550),Some(3));
        assert_eq!(m.promote("g",2,base+2550,2550),Err("backup older than last published event".into()));
        m.invalidate(1).unwrap();
        assert_eq!(m.promote("g",3,base+2550,2550).unwrap(),2);
        assert!(m.publish("g","SOLUSDT",base+2550,2550).is_some());
    }
    #[test]
    fn r1200_stale_candidate_cannot_fake_quorum_or_quarantine_active() {
        let base=1_800_000_000_000_i64;let mut m=setup();
        m.add_session(3,"peer-C","g").unwrap();
        prime(&mut m,1,base);prime(&mut m,2,base);prime(&mut m,3,base);
        m.promote("g",1,base+1550,1550).unwrap();
        feed(&mut m,1,base,2300,7);
        m.publish("g","SOLUSDT",base+2340,2340).unwrap();
        feed(&mut m,2,base,2150,8);feed(&mut m,3,base,2500,9);
        m.sessions.get_mut(&1).unwrap().tier=Tier::Suspect;
        assert_eq!(m.watchdog_correlated_r1199("g",base+2550,2550).unwrap(),
            WatchdogOutcome::WaitingForEvidence{old_id:1,silent_ms:250,qualified_backups:1});
        assert_eq!(m.active.get("g").unwrap().session_id,1);
        assert_ne!(m.sessions.get(&1).unwrap().tier,Tier::Quarantined);
        assert!(m.publish("g","SOLUSDT",base+2550,2550).is_none());
    }
    #[test]
    fn r1199_duplicate_peer_cannot_forge_quorum() {
        let base=1_800_000_000_000_i64;let mut m=setup();
        m.add_session(3,"peer-B","g").unwrap();
        prime(&mut m,1,base);prime(&mut m,2,base);prime(&mut m,3,base);
        m.promote("g",1,base+1550,1550).unwrap();
        feed(&mut m,2,base,4300,8);feed(&mut m,3,base,4300,8);
        let x=m.watchdog_correlated_r1199("g",base+4340,4340).unwrap();
        assert!(matches!(x,WatchdogOutcome::WaitingForEvidence{old_id:1,qualified_backups:1,..}));
    }
}
