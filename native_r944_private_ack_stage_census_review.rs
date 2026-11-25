// R944: independent original 17-table test-only ACK component census.
// No production source, no configuration mutation, no disabled integrity.
#[test]fn r944_private_2048_events_component_census(){
 use std::time::Instant;
 let (root,h,_)=pair(64);let v=r586_oracle();
 r924_install_private_paper_only_guards(&h.source,&h.sink,&v).unwrap();
 let t=Instant::now();
 assert_eq!(r931_guarded_verified_burst(&root.0,&h.source,&h.sink,&v,64),Ok(64));
 let replay_ms=t.elapsed().as_secs_f64()*1000.0;
 let t=Instant::now();
 r924_verify_installed_guards(&h.source,&v).unwrap();
 r924_verify_installed_guards(&h.sink,&v).unwrap();
 let guards_ms=t.elapsed().as_secs_f64()*1000.0;
 exec(&h.source,"BEGIN DEFERRED").unwrap();
 let snapshot=R805SourceRead(&h.source);
 let t=Instant::now();
 let (hd,c,m)=r805_read_head(&root.0,&h.source,&h.sink).unwrap();
 let head_ms=t.elapsed().as_secs_f64()*1000.0;
 let t=Instant::now();
 let (a,b,rows,hash)=r907_verify_retained_and_extract_prefix(
  &h.source,&hd,r726_s(&c,"feed_generation").unwrap(),"r879_probe").unwrap();
 let canonical_chain_ms=t.elapsed().as_secs_f64()*1000.0;
 assert_eq!(a,64);assert!(b>0);assert_eq!(rows.len(),2048);
 assert_eq!(m["applied_hash"].as_str(),Some(hash.as_str()));
 let t=Instant::now();
 r895_verify_new_paper_rows(&h.source,&h.sink,&v,&rows).unwrap();
 let physical_binding_ms=t.elapsed().as_secs_f64()*1000.0;
 drop(snapshot);
 let t=Instant::now();
 let tx=Tx::begin(&h.source).unwrap();
 r924_verify_installed_guards(&h.source,&v).unwrap();
 r924_verify_installed_guards(&h.sink,&v).unwrap();
 let (hd,c,_)=r805_read_head(&root.0,&h.source,&h.sink).unwrap();
 let final_head_and_guards_ms=t.elapsed().as_secs_f64()*1000.0;
 let t=Instant::now();
 r805_verify_chain(&h.source,&hd,r726_s(&c,"feed_generation").unwrap()).unwrap();
 let redundant_chain_in_lock_ms=t.elapsed().as_secs_f64()*1000.0;
 drop(tx);
 eprintln!("R944 ORIGINAL 2048 event stages ms: replay={replay_ms:.2} initial_guards={guards_ms:.2} head={head_ms:.2} canonical_snapshot_chain={canonical_chain_ms:.2} physical_binding={physical_binding_ms:.2} final_head_and_guards_in_WRITE_LOCK={final_head_and_guards_ms:.2} second_canonical_chain_in_WRITE_LOCK={redundant_chain_in_lock_ms:.2}; private only, does NOT certify capacity");
 assert_eq!(r800_streaming_original_parity(&h.source,&h.sink,&v),Ok(2048));
}
