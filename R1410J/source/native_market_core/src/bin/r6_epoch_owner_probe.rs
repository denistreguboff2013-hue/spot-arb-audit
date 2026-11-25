//! R6 transport->Owner epoch boundary proof. No exchange or trading I/O.
use spotarb_native_market_core::{CoreError,HistorySink,Owner,RawFrame,Update,Venue,owner_index};
struct H;impl HistorySink for H{fn accept(&mut self,_:&RawFrame,_:&Update)->Result<(),CoreError>{Ok(())}}
fn b(epoch:u64,seq:i64,ts:i64,ns:u64)->RawFrame{RawFrame{source:None,venue:Venue::BinanceFutures,epoch,received_ns:ns,received_wall_ms:ts,raw:format!(r#"{{"stream":"btcusdt@bookTicker","data":{{"e":"bookTicker","s":"BTCUSDT","b":"100","B":"2","a":"101","A":"2","u":{seq},"E":{ts}}}}}"#)}}
fn main(){let mut o=Owner::new(owner_index("BTC",3),3,12000).unwrap();let mut h=H;
 o.ingest(&b(1,1,1000,1),&mut h).unwrap();assert!(o.snapshot_venue("BTC",Venue::BinanceFutures).is_some());
 o.reset_venue("BTC",Venue::BinanceFutures,2).unwrap();assert!(o.snapshot_venue("BTC",Venue::BinanceFutures).is_none());
 assert_eq!(o.ingest(&b(1,2,1001,2),&mut h),Err(CoreError::StaleEpoch));
 assert_eq!(o.ingest(&b(2,2,1000,3),&mut h),Err(CoreError::OutOfOrder));
 o.ingest(&b(2,2,1002,4),&mut h).unwrap();assert_eq!(o.snapshot_venue("BTC",Venue::BinanceFutures).unwrap().epoch,2);
 println!(r#"{{"transport_owner_epoch_gate":"PASS","old_epoch_rejected":true,"old_book_invalidated":true,"old_source_rejected":true,"new_epoch_accepted":true,"trading":false}}"#);
}