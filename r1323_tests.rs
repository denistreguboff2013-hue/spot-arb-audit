use super::*;

#[test]
fn r1323_metadata_hashes_ownership_and_routes() {
    let provenance:Value=serde_json::from_str(include_str!("../fixtures/r1323/provenance.json")).unwrap();
    let raw=[include_bytes!("../fixtures/r1323/binance_historical.json").as_slice(),
        include_bytes!("../fixtures/r1323/okx_historical.json").as_slice()];
    for (i,bytes) in raw.iter().enumerate() {
        assert_eq!(digest(bytes),provenance["files"][i]["sha256"].as_str().unwrap());
    }
    assert_eq!(provenance["classification"],"HISTORICAL_ONLY");
    assert_eq!(provenance["current_gate"],"METADATA_SNAPSHOT_BLOCKED");
    let meta=historical();let bases=eligible(&meta);
    let expected:Vec<String>=serde_json::from_str(include_str!("../fixtures/r1323/eligible_bases.json")).unwrap();
    assert_eq!(bases,expected);
    let actual=manifest(&meta);
    let stored:Value=serde_json::from_str(include_str!("../fixtures/r1323/ownership_manifest.json")).unwrap();
    assert_eq!(actual,stored);
    let replay=Replay::new(meta,QUEUE_CAPACITY);
    for base in &bases {
        let core=&replay.cores[owner_index(base,OWNERS)];
        assert_eq!(serde_json::to_value(core.routes_for(base).unwrap()).unwrap(),
            serde_json::to_value(&replay.meta.routes[base]).unwrap());
    }
    println!("R1323 HISTORICAL_ONLY bases={} manifest_sha256={} current=BLOCKED",bases.len(),
        digest(include_bytes!("../fixtures/r1323/ownership_manifest.json")));
}

#[test]
fn r1323_historical_whole_universe_decision_replay() {
    let meta=historical();let bases=eligible(&meta);let mut r=Replay::new(meta,QUEUE_CAPACITY);
    take_evaluations();
    for base in &bases {
        let missing=send(&mut r,base,Venue::OkxSpot,1,NS,WALL,false);
        assert_eq!(missing.len(),1);assert_eq!(missing[0].reason,"SYSTEM_NOT_READY");
        let paired=send(&mut r,base,Venue::BinanceFutures,1,NS,WALL,false);
        assert_eq!(paired.len(),1);assert_eq!(paired[0].base,*base);
        assert_eq!(paired[0].buy_exchange,"okx_spot");assert_eq!(paired[0].sell_exchange,"binance_futures");
        assert_eq!(paired[0].reason,"EDGE_TOO_LOW");
        assert_eq!(paired[0].gross_upper_bound_bps,Some(0.0));
        let stale=send(&mut r,base,Venue::BinanceFutures,2,NS+801_000_000,WALL+801,false);
        assert_eq!(stale[0].reason,"STALE_BOOK");
        assert!(send(&mut r,base,Venue::BinanceFutures,2,NS+801_000_000,WALL+801,false).is_empty());
        assert!(r.submit(frame(base,Venue::BinanceFutures,0,3,NS+802_000_000,WALL+802,false),r.identity.clone()));
        assert!(r.drain(NS+802_000_000,WALL+802).is_empty());
    }
    assert_eq!(take_evaluations(),bases.iter().map(|b|(b.clone(),3)).collect::<BTreeMap<String,u64>>());
    assert_eq!(r.accounting.processed,3*bases.len() as u64);
    assert_eq!(r.accounting.rejected,2*bases.len() as u64);assert_eq!(r.history.accepted,r.accounting.processed);
    assert_eq!(r.accounting.reasons.len(),2);
    assert!(r.accounting.reasons.keys().any(|s|s.contains("OutOfOrder")));
    assert!(r.accounting.reasons.keys().any(|s|s.contains("StaleEpoch")));
    assert_eq!(r.accounting.pending,0);r.accounting.reconcile();
    println!("R1323 HISTORICAL_ONLY {}",r.accounting.report());
}

#[test]
fn r1323_event_recalculates_only_changed_symbol() {
    let mut r=Replay::new(synthetic(&["AAA","BBB"],1),QUEUE_CAPACITY);
    for base in ["AAA","BBB"] {
        send(&mut r,base,Venue::OkxSpot,1,NS,WALL,false);
        send(&mut r,base,Venue::BinanceFutures,1,NS,WALL,false);
    }
    let before=r.owners[owner_index("BBB",OWNERS)].snapshot_pair("BBB");take_evaluations();
    let rows=send(&mut r,"AAA",Venue::BinanceFutures,2,NS+1_000_000,WALL+1,true);
    assert_eq!(rows.len(),1);assert_eq!(rows[0].base,"AAA");
    assert_eq!(take_evaluations(),BTreeMap::from([("AAA".into(),1)]));
    assert_eq!(r.owners[owner_index("BBB",OWNERS)].snapshot_pair("BBB"),before);
}

#[test]
fn r1323_paired_categories_missing_side_freshness_and_limits() {
    for reverse in [false,true] {
        let mut r=Replay::new(synthetic(&["AAA"],1),QUEUE_CAPACITY);
        let order=if reverse{[Venue::BinanceFutures,Venue::OkxSpot]}else{[Venue::OkxSpot,Venue::BinanceFutures]};
        assert_eq!(send(&mut r,"AAA",order[0],1,NS,WALL,true)[0].reason,"SYSTEM_NOT_READY");
        let rows=send(&mut r,"AAA",order[1],1,NS,WALL,true);
        assert_eq!(rows[0].reason,"EXECUTABLE_OPPORTUNITY");
        assert_eq!(rows[0].required_net_bps,42.0);assert_eq!(config().risk_cap,10.0);
        assert_eq!(rows[0].risk_result.as_deref(),Some("RISK_SIZE_ALLOW"));
        assert_eq!(rows[0].quantity,Some(0.098));
        assert!(rows[0].tested_size_usdt.unwrap()<=10.0);
        let stale=send(&mut r,"AAA",order[1],2,NS+801_000_000,WALL+801,true);
        assert_eq!(stale[0].reason,"STALE_BOOK");
    }
    let mut m=synthetic(&["AAA"],1);
    m.markets.get_mut("AAA").unwrap().spot_min_amount=Some("10".into());
    m.routes.insert("AAA".into(),vec![m.markets["AAA"].route_spec()]);
    let mut r=Replay::new(m,QUEUE_CAPACITY);
    send(&mut r,"AAA",Venue::OkxSpot,1,NS,WALL,true);
    assert_eq!(send(&mut r,"AAA",Venue::BinanceFutures,1,NS,WALL,true)[0].reason,"ABOVE_RESEARCH_CEILING");
}

#[test]
fn r1323_empty_book_sequence_epoch_and_wrong_venue_denied() {
    let mut r=Replay::new(synthetic(&["AAA"],1),QUEUE_CAPACITY);
    send(&mut r,"AAA",Venue::OkxSpot,1,NS,WALL,false);
    send(&mut r,"AAA",Venue::BinanceFutures,1,NS,WALL,false);
    let before=r.accounting.processed;
    assert!(send(&mut r,"AAA",Venue::BinanceFutures,1,NS,WALL,false).is_empty());
    let mut old=frame("AAA",Venue::BinanceFutures,0,2,NS,WALL,false);
    assert!(r.submit(old.clone(),r.identity.clone()));assert!(r.drain(NS,WALL).is_empty());
    old.epoch=1;old.venue=Venue::OkxSpot;
    assert!(r.submit(old,r.identity.clone()));assert!(r.drain(NS,WALL).is_empty());
    assert_eq!(r.accounting.processed,before);assert_eq!(r.accounting.rejected,3);
    let mut empty=frame("AAA",Venue::OkxSpot,1,2,NS+1_000_000,WALL+1,false);
    let mut value:Value=serde_json::from_str(&empty.raw).unwrap();value["data"][0]["asks"]=json!([]);
    empty.raw=value.to_string();assert!(r.submit(empty,r.identity.clone()));
    let rows=r.drain(NS+1_000_000,WALL+1);assert_eq!(rows[0].reason,"SYSTEM_NOT_READY");
    r.accounting.reconcile();
}

#[test]
fn r1323_ineligible_unknown_and_wrong_symbol_metadata_denied() {
    let mut meta=synthetic(&["AAA","BBB"],1);
    meta.markets.get_mut("BBB").unwrap().static_reason=Some("EXCHANGE_MINIMUM_UNKNOWN".into());
    meta.routes.insert("BBB".into(),vec![meta.markets["BBB"].route_spec()]);
    meta.markets.get_mut("AAA").unwrap().future_symbol="WRONGUSDT".into();
    let mut r=Replay::new(meta,QUEUE_CAPACITY);
    for base in ["AAA","BBB","UNKNOWN"] {assert!(send(&mut r,base,Venue::BinanceFutures,1,NS,WALL,false).is_empty());}
    assert_eq!(r.accounting.routed,0);assert_eq!(r.history.accepted,0);
    assert_eq!(r.accounting.reasons,BTreeMap::from([
        ("INELIGIBLE_METADATA".into(),1),("BASE_REMOVED_OR_UNKNOWN".into(),1),("SYMBOL_METADATA_MISMATCH".into(),1)]));
}

#[test]
fn r1323_add_remove_reorder_reload_and_stale_identity() {
    let first=synthetic(&["AAA","BBB"],1);let order=manifest(&first);
    assert_eq!(order,manifest(&synthetic(&["BBB","AAA"],1)));
    let mut r=Replay::new(first,QUEUE_CAPACITY);
    send(&mut r,"AAA",Venue::OkxSpot,1,NS,WALL,false);
    send(&mut r,"AAA",Venue::BinanceFutures,1,NS,WALL,false);
    let old=r.identity.clone();
    assert!(r.submit(frame("AAA",Venue::BinanceFutures,1,2,NS+1_000_000,WALL+1,false),old.clone()));
    r.switch(synthetic(&["CCC","AAA"],2)).unwrap();
    assert_eq!(r.visible_generation(),Some(2));assert!(r.drain(NS+1_000_000,WALL+1).is_empty());
    assert_eq!(r.accounting.reasons["STALE_METADATA_IDENTITY"],1);
    assert!(send(&mut r,"BBB",Venue::OkxSpot,2,NS+1_000_000,WALL+1,false).is_empty());
    assert_eq!(r.accounting.reasons["BASE_REMOVED_OR_UNKNOWN"],1);
    for base in ["AAA","CCC"] {
        assert_eq!(send(&mut r,base,Venue::OkxSpot,2,NS+2_000_000,WALL+2,false)[0].reason,"SYSTEM_NOT_READY");
        assert_eq!(send(&mut r,base,Venue::BinanceFutures,2,NS+2_000_000,WALL+2,false)[0].reason,"EDGE_TOO_LOW");
    }
    // Same-generation reload is rejected; equivalent content with new generation is a full cutover.
    assert!(r.stage(synthetic(&["AAA","CCC"],2)).is_err());assert_eq!(r.visible_generation(),Some(2));
    let previous=r.identity.clone();r.switch(synthetic(&["AAA","CCC"],3)).unwrap();
    assert!(!Arc::ptr_eq(&previous,&r.identity));assert_eq!(r.visible_generation(),Some(3));
    assert_eq!(r.owners[owner_index("AAA",OWNERS)].snapshot_pair("AAA"),(None,None));
    assert_eq!(manifest(&r.meta),manifest(&synthetic(&["CCC","AAA"],3)));
    // Numeric equality alone cannot forge a published metadata identity.
    let forged=Arc::new(MetadataIdentity{generation:3});
    assert!(r.submit(frame("AAA",Venue::OkxSpot,r.epoch,3,NS+3_000_000,WALL+3,false),forged));
    assert!(r.drain(NS+3_000_000,WALL+3).is_empty());
    assert_eq!(r.accounting.reasons["STALE_METADATA_IDENTITY"],2);
}

#[test]
fn r1323_atomic_barrier_partial_invalid_and_complete_cutover() {
    let mut r=Replay::new(synthetic(&["AAA"],1),QUEUE_CAPACITY);
    let mut invalid=synthetic(&["AAA"],2);invalid.routes.clear();
    assert!(r.stage(invalid).is_err());assert_eq!(r.visible_generation(),Some(1));
    r.stage(synthetic(&["BBB","AAA"],2)).unwrap();
    assert_eq!(r.visible_generation(),Some(1));assert!(r.commit_owner(0).is_err());
    r.pause().unwrap();assert_eq!(r.visible_generation(),None);
    r.commit_owner(0).unwrap();assert!(r.publish().is_err());
    assert_eq!(r.visible_generation(),None);assert_eq!(r.cores[0].metadata_generation(),2);
    assert_eq!(r.cores[1].metadata_generation(),1);
    assert!(send(&mut r,"AAA",Venue::OkxSpot,1,NS,WALL,false).is_empty());
    assert_eq!(r.history.accepted,0);assert_eq!(r.accounting.reasons["METADATA_BARRIER"],1);
    for i in 1..OWNERS {r.commit_owner(i).unwrap();assert_eq!(r.visible_generation(),None);}
    r.publish().unwrap();assert_eq!(r.visible_generation(),Some(2));
    assert!(r.cores.iter().all(|c|c.metadata_generation()==2&&!c.decisions_paused()));
    assert_eq!(send(&mut r,"AAA",Venue::OkxSpot,2,NS+1_000_000,WALL+1,false)[0].reason,"SYSTEM_NOT_READY");
    // A frame relabelled with the CURRENT metadata token still fails the actual Owner epoch fence.
    assert!(r.submit(frame("AAA",Venue::BinanceFutures,1,2,NS+1_000_000,WALL+1,false),r.identity.clone()));
    assert!(r.drain(NS+1_000_000,WALL+1).is_empty());
    assert!(r.accounting.reasons.keys().any(|s|s.contains("StaleEpoch")));
}

#[test]
fn r1323_bounded_queue_history_pressure_accounting_and_residence() {
    let mut r=Replay::new(synthetic(&["AAA"],1),1);
    assert!(r.submit(frame("AAA",Venue::OkxSpot,1,1,NS,WALL,false),r.identity.clone()));
    assert!(!r.submit(frame("AAA",Venue::BinanceFutures,1,1,NS,WALL,false),r.identity.clone()));
    assert_eq!(r.accounting.pending,1);r.accounting.reconcile();r.drain(NS,WALL);
    assert_eq!(r.accounting.processed,1);assert_eq!(r.accounting.queue_rejected,1);
    assert_eq!(r.accounting.queue_reasons["QUEUE_FULL"],1);
    r.history.full=true;let before=r.owners[owner_index("AAA",OWNERS)].snapshot_pair("AAA");
    assert!(send(&mut r,"AAA",Venue::BinanceFutures,1,NS,WALL,false).is_empty());
    assert_eq!(r.owners[owner_index("AAA",OWNERS)].snapshot_pair("AAA"),before);
    assert_eq!(r.history.accepted,1);assert_eq!(r.accounting.pending,0);r.accounting.reconcile();
    assert_eq!(r.accounting.residence_ns.len(),2);assert_eq!(r.accounting.service_ns.len(),2);
    println!("R1323 SYNTHETIC {}",r.accounting.report());
}

#[test]
fn r1323_metadata_parser_filters_duplicates_and_reorder() {
    let b:Value=serde_json::from_str(include_str!("../fixtures/r1323/binance_historical.json")).unwrap();
    let o:Value=serde_json::from_str(include_str!("../fixtures/r1323/okx_historical.json")).unwrap();
    let original=historical();let mut reversed_b=b.clone();let mut reversed_o=o.clone();
    reversed_b["symbols"].as_array_mut().unwrap().reverse();reversed_o["data"].as_array_mut().unwrap().reverse();
    let reversed=build_snapshot(&reversed_b.to_string(),&reversed_o.to_string(),1,NS,WALL as u64).unwrap();
    assert_eq!(original.canonical_control_value(),reversed.canonical_control_value());
    assert_eq!(eligible(&original),eligible(&reversed));
    let base=eligible(&original)[0].clone();
    let row=b["symbols"].as_array().unwrap().iter().find(|r|r["symbol"]==original.markets[&base].future_symbol).unwrap().clone();
    let mut duplicate=b.clone();duplicate["symbols"].as_array_mut().unwrap().push(row);
    assert!(build_snapshot(&duplicate.to_string(),&o.to_string(),1,NS,WALL as u64).is_err());
    let mut inactive=o.clone();
    for r in inactive["data"].as_array_mut().unwrap() {
        if r["instId"]==original.markets[&base].spot_symbol {r["state"]=json!("suspend");}
    }
    let reduced=build_snapshot(&b.to_string(),&inactive.to_string(),1,NS,WALL as u64).unwrap();
    assert!(!reduced.markets.contains_key(&base));assert_eq!(reduced.markets.len()+1,original.markets.len());
    assert!(build_snapshot(&b.to_string(),r#"{"code":"1","data":[]}"#,1,NS,WALL as u64).is_err());
    assert!(build_snapshot(&b.to_string(),&o.to_string(),0,NS,WALL as u64).is_err());
}
