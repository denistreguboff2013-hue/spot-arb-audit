fn r1306_intent_eio(point:storage_eio::Point){
 let p=path("r1306-intent-eio");let d=DurablePaper::create(&p,lifecycle()).unwrap();
 let initial_seq=d.sequence();let initial_projection=serde_json::to_value(d.projection()).unwrap();
 let spot=Book{bids:vec![(99.,2.)],asks:vec![(100.,2.)],received_ns:1,received_wall_ms:1000,exchange_ts_ms:Some(1000),changed_ns:1,epoch:1,sequence:Some(1)};
 let fut=Book{bids:vec![(101.,2.)],asks:vec![(102.,2.)],received_ns:1,received_wall_ms:1000,exchange_ts_ms:Some(1000),changed_ns:1,epoch:1,sequence:Some(1)};
 let gate=MarketGate{spot_hot:true,futures_hot:true,spot_book_valid:true,futures_book_valid:true};
 let intent=OrderIntent{intent_id:"r1306-i".into(),order_id:"r1306-o".into(),leg:FillLeg::Spot,side:FillSide::Buy,quantity:1.,instrument:"XUSDT".into(),created_wall_ms:1000,created_mono_ns:1,exit_reason:None};
 let(tx,rx)=std::sync::mpsc::sync_channel(0);let(go,resume)=std::sync::mpsc::sync_channel(0);
 let(ack,ack_rx)=std::sync::mpsc::channel();
 let worker_path=p.clone();let i=intent.clone();let b=spot.clone();let f=fut.clone();let g=MarketGate{spot_hot:true,futures_hot:true,spot_book_valid:true,futures_book_valid:true};
 let worker=std::thread::spawn(move||{
  let mut d=d;let guard=storage_eio::arm(&worker_path,point);
  storage_eio::pause_before_error(move||{tx.send(()).unwrap();resume.recv_timeout(std::time::Duration::from_secs(3)).unwrap();});
  let result=d.commit_order_intent(i.clone(),&b,&f,&g,1,1000);
  ack.send(result.is_ok()).unwrap();
  assert!(matches!(result,Err(ExecutionError::UncertainCommit(_))));
  assert!(d.is_fenced());
  assert!(matches!(d.commit_order_intent(i,&b,&f,&g,1,1000),Err(ExecutionError::OwnerFenced)));
  drop(guard);d
 });
 rx.recv_timeout(std::time::Duration::from_secs(3)).unwrap();
 assert!(matches!(ack_rx.try_recv(),Err(std::sync::mpsc::TryRecvError::Empty)),"no acknowledgement before durable boundary");
 assert!(DurablePaper::recover(&p).is_err(),"competing owner during in-flight commit");
 go.send(()).unwrap();let d=worker.join().unwrap();
 assert!(!ack_rx.recv().unwrap(),"EIO cannot ACK");
 assert_eq!(d.sequence(),initial_seq);assert!(d.lifecycle().orders.is_empty());
 assert_eq!(serde_json::to_value(d.projection()).unwrap(),initial_projection);
 drop(d);let mut recovered=DurablePaper::recover(&p).unwrap();
 assert!(recovered.lifecycle().processed_events.is_empty());
 assert_eq!(recovered.projection().spot_base,0.);assert_eq!(recovered.projection().futures_base,0.);
 assert!(!recovered.lifecycle().order_census_complete);
 assert_ne!(recovered.lifecycle().state,LifecycleState::Closed);
 if point==storage_eio::Point::Sync{
  assert_eq!(recovered.lifecycle().state,LifecycleState::RecoveryRequired);
  assert_eq!(recovered.lifecycle().orders.len(),1);
  let seq=recovered.sequence();
  assert!(recovered.commit_order_intent(intent,&spot,&fut,&gate,1,1000).is_err());
  assert_eq!(recovered.sequence(),seq);
 }else{
  assert_eq!(recovered.lifecycle().state,LifecycleState::Flat);
  assert!(recovered.lifecycle().orders.is_empty());
  recovered.commit_order_intent(intent.clone(),&spot,&fut,&gate,1,1000).unwrap();
  assert!(recovered.commit_order_intent(intent,&spot,&fut,&gate,1,1000).is_err());
  assert_eq!(recovered.lifecycle().orders.len(),1);
  assert!(recovered.lifecycle().processed_events.is_empty());
 }
 drop(recovered);std::fs::remove_file(p).unwrap();
}
#[test] fn r1306_append_eio_during_order_intent(){r1306_intent_eio(storage_eio::Point::Append)}
#[test] fn r1306_sync_eio_during_order_intent(){r1306_intent_eio(storage_eio::Point::Sync)}
