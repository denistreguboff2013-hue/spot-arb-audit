use super::*;
use super::super::engine_fixture::EngineFixture;
fn fixture()->(EngineFixture,ValidatedInput,OfflineRules,AdmissionReview){
 let mut engine=EngineFixture::new("r1321-clock");engine.qualify();
 let input=engine.inputs.iter().rev().find(|i|i.venue==Venue::OkxSpot).unwrap().clone();
 let profile=policy::InstrumentProfile{
  synthetic:true,base:"AAA".into(),spot_symbol:"AAA-USDT".into(),futures_symbol:"AAAUSDT".into(),
  product:"LINEAR_USDT_PERPETUAL".into(),quantity_units:"BASE".into(),multiplier:"1".into(),hedge_ratio:"1".into(),
  step:"0.01".into(),minimum_quantity:"0.01".into(),maximum_quantity:"10".into(),minimum_notional:"1".into(),
  fee_model:"QUOTE_SIMULATED".into(),fee_bps:"10".into(),maximum_unhedged_base:"10".into(),maximum_dust_base:"0.01".into(),
  metadata_loaded_ns:input.receive_ns,metadata_ttl_ns:10_000_000_000,hedge_timeout_ns:1_000_000_000,
  stop_loss_bps:-100.0,max_hold_ns:10_000_000_000,
 };
 let identity=policy::Identity{run_id:"r1321-offline".into(),clock_domain:"r1321-clock".into(),profile,exit:None};
 let rules=OfflineRules::fixture(identity.clone(),Venue::OkxSpot,7,"0.01");
 let gate=AdmissionReview::fixture(&identity,7,8);
 (engine,input,rules,gate)
}
fn review(g:&mut AdmissionReview,e:&EngineFixture,i:ValidatedInput,r:&OfflineRules)->Result<ReviewedInput,Denial>{
 let n=i.validated_ns;let w=i.exchange_ms;
 g.review(i,r,&e.receiver,&e.health,ReviewRequest{now_ns:n,wall_ms:w,quantity:1.0,price:101.0,real_off:true,kill_engaged:true,owner_healthy:true,storage_healthy:true})
}
#[test] fn r1321_contract_preserves_input_and_does_not_admit_public_orders(){
 let(e,i,r,mut g)=fixture();let expected=i.source_provenance.clone();
 let receipt=review(&mut g,&e,i,&r).unwrap();
 assert_eq!(receipt.input.source_provenance,expected);
 assert_eq!(receipt.rules_digest,r.seal);
 assert_eq!(receipt.metadata_generation,7);
 assert_eq!(order_admission(&e.health,true,true,true,true,true),Err(Denial::Rules));
}
#[test] fn r1321_contract_duplicate_and_conflict_are_terminal(){
 for conflict in [false,true]{
  let(e,i,r,mut g)=fixture();review(&mut g,&e,i.clone(),&r).unwrap();
  let mut again=i.clone();if conflict{again.book.bids[0].1+=1.0;}
  assert_eq!(review(&mut g,&e,again,&r).unwrap_err(),Denial::Duplicate);
  assert_eq!(review(&mut g,&e,i,&r).unwrap_err(),Denial::Duplicate);
 }
}
#[test] fn r1321_contract_rejects_wrong_scope_and_unsealed_rules(){
 for case in 0..5{
  let(e,mut i,mut r,mut g)=fixture();
  match case{0=>i.instrument="BBB".into(),1=>r.venue=Venue::BinanceFutures,2=>r.metadata_generation=8,3=>r.identity.profile.fee_bps="0".into(),_=>i.clock_domain="other".into()}
  assert!(review(&mut g,&e,i,&r).is_err(),"case {case}");
 }
}
#[test] fn r1321_contract_enforces_exact_freshness_boundary(){
 for (age,ok) in [(800_000_000,true),(800_000_001,false)]{
  let(e,i,r,mut g)=fixture();let n=i.receive_ns+age;let w=i.exchange_ms+800;
  let result=g.review(i,&r,&e.receiver,&e.health,ReviewRequest{now_ns:n,wall_ms:w,quantity:1.0,price:101.0,real_off:true,kill_engaged:true,owner_healthy:true,storage_healthy:true});
  assert_eq!(result.is_ok(),ok,"age {age}");
 }
}
#[test] fn r1321_contract_rejects_custody_and_witness_tampering(){
 for case in 0..5{
  let(e,mut i,r,mut g)=fixture();
  match case{0=>i.source_provenance.selection_epoch+=1,1=>i.source_provenance.session_id="wrong".into(),2=>i.witness=None,3=>i.snapshot_generation=Some(u64::MAX),_=>{i.source_provenance.accepted_id=0;i.source_provenance.journal_id="tampered-nonempty-journal".into()}}
  assert!(review(&mut g,&e,i,&r).is_err(),"case {case}");
 }
}
#[test] fn r1326k_ram_first_selected_permit_without_durable_journal_is_admissible(){
 let(e,i,r,mut g)=fixture();
 assert_eq!(i.source_provenance.accepted_id,0);
 assert!(i.source_provenance.journal_id.is_empty());
 assert!(i.source_provenance.selected_permit.is_some());
 assert!(review(&mut g,&e,i,&r).is_ok());
}
#[test] fn r1321_contract_rejects_revoked_authority(){
 let(mut e,i,r,mut g)=fixture();e.engine.mark_reconnecting(&i.source);
 assert!(review(&mut g,&e,i,&r).is_err());
}
#[test] fn r1321_contract_denials_stick_and_capacity_is_bounded(){
 let(e,i,r,mut g)=fixture();g.capacity=0;
 assert_eq!(review(&mut g,&e,i.clone(),&r).unwrap_err(),Denial::Overflow);
 g.capacity=8;
 assert_eq!(review(&mut g,&e,i,&r).unwrap_err(),Denial::Overflow);
}
#[test] fn r1321_contract_rejects_invalid_size_tick_and_safety(){
 for case in 0..8{
  let(e,i,r,mut g)=fixture();
  let mut q=ReviewRequest{now_ns:i.validated_ns,wall_ms:i.exchange_ms,quantity:1.0,price:101.0,real_off:true,kill_engaged:true,owner_healthy:true,storage_healthy:true};
  match case{0=>q.quantity=0.001,1=>q.price=101.001,2=>q.quantity=f64::NAN,3=>q.real_off=false,4=>q.kill_engaged=false,5=>q.owner_healthy=false,6=>q.storage_healthy=false,_=>q.now_ns=i.receive_ns-1}
  assert!(g.review(i,&r,&e.receiver,&e.health,q).is_err(),"case {case}");
 }
}
#[test] fn r1321_contract_metadata_expiry_and_fee_model_fail_closed(){
 for case in 0..3{
  let(e,i,mut r,mut g)=fixture();
  match case{0=>r.identity.profile.metadata_loaded_ns=i.validated_ns+1,1=>r.identity.profile.fee_model="ACCOUNT_ATTESTED".into(),_=>r.price_tick="0".into()}
  r.seal=r.digest().unwrap();
  g.identity_digest=policy::digest(&r.identity).unwrap();
  assert!(review(&mut g,&e,i,&r).is_err(),"case {case}");
 }
}
#[test] fn r1321_contract_accepts_futures_scope_without_second_owner(){
 let(e,_,mut r,mut g)=fixture();
 let i=e.inputs.iter().find(|i|i.venue==Venue::BinanceFutures).unwrap().clone();
 r.venue=Venue::BinanceFutures;r.seal=r.digest().unwrap();
 let receipt=review(&mut g,&e,i,&r).unwrap();
 assert_eq!(receipt.input.venue,Venue::BinanceFutures);
}
#[test] fn r1321_contract_metadata_ttl_expiry_is_stale(){
 let(e,i,mut r,mut g)=fixture();
 r.identity.profile.metadata_ttl_ns=1;
 r.seal=r.digest().unwrap();g.identity_digest=r.identity.hash().unwrap();
 let n=i.validated_ns.max(i.receive_ns+2);let w=i.exchange_ms;
 let result=g.review(i,&r,&e.receiver,&e.health,ReviewRequest{now_ns:n,wall_ms:w,quantity:1.0,price:101.0,real_off:true,kill_engaged:true,owner_healthy:true,storage_healthy:true});
 assert_eq!(result.unwrap_err(),Denial::Stale);
}
#[test] fn r1321_contract_new_custody_id_cannot_bypass_duplicate(){
 let(e,i,r,mut g)=fixture();review(&mut g,&e,i.clone(),&r).unwrap();
 let mut again=i;again.source_provenance.accepted_id+=1;
 assert_eq!(review(&mut g,&e,again,&r).unwrap_err(),Denial::Duplicate);
}
#[test] fn r1321_contract_sealed_wrong_generation_and_venue_denied(){
 for wrong_generation in [true,false]{
  let(e,i,mut r,mut g)=fixture();
  if wrong_generation{r.metadata_generation=8;}else{r.venue=Venue::BinanceFutures;}
  r.seal=r.digest().unwrap();
  let expected=if wrong_generation{Denial::Generation}else{Denial::Rules};
  assert_eq!(review(&mut g,&e,i,&r).unwrap_err(),expected);
 }
}
