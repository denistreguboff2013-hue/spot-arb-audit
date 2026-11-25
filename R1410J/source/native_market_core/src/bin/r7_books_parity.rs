//! R7 deterministic normalization/book-state parity gate. Review only; no exchange/order I/O.
use spotarb_native_market_core::{Book,CoreError,HistorySink,Owner,RawFrame,Update,Venue};
struct H;impl HistorySink for H{fn accept(&mut self,_:&RawFrame,_:&Update)->Result<(),CoreError>{Ok(())}}
fn f(v:Venue,e:u64,n:u64,w:i64,raw:&str)->RawFrame{RawFrame{source:None,venue:v,epoch:e,received_ns:n,received_wall_ms:w,raw:raw.into()}}
fn main(){let mut o=Owner::new(0,1,800).unwrap();let mut h=H;
let ok=f(Venue::OkxSpot,1,1_000_000_000,1_760_000_000_000,r#"{"arg":{"channel":"books5","instId":"BTC-USDT"},"data":[{"bids":[["100","2"],["99","3"]],"asks":[["101","4"],["102","5"]],"ts":"1760000000000","seqId":10}]}"#);
assert!(o.ingest(&ok,&mut h).unwrap().is_none());let b1=o.snapshot_venue("BTC",Venue::OkxSpot).unwrap();assert_eq!(b1.bids,vec![(100.0,2.0),(99.0,3.0)]);assert_eq!(b1.asks,vec![(101.0,4.0),(102.0,5.0)]);assert_eq!(b1.changed_ns,1_000_000_000);
let same=f(Venue::OkxSpot,1,1_100_000_000,1_760_000_000_100,r#"{"arg":{"channel":"books5","instId":"BTC-USDT"},"data":[{"bids":[["100","2"],["99","3"]],"asks":[["101","4"],["102","5"]],"ts":"1760000000100","seqId":11}]}"#);o.ingest(&same,&mut h).unwrap();assert_eq!(o.snapshot_venue("BTC",Venue::OkxSpot).unwrap().changed_ns,1_000_000_000);
let bn=f(Venue::BinanceFutures,1,1_200_000_000,1_760_000_000_200,r#"{"data":{"e":"bookTicker","s":"BTCUSDT","b":"102","B":"7","a":"103","A":"8","E":1760000000200,"u":20}}"#);
let s=o.ingest(&bn,&mut h).unwrap().unwrap();assert_eq!(s.spot.asks[0],(101.0,4.0));assert_eq!(s.future.bids[0],(102.0,7.0));assert!((s.indicative_bps-99.0099009900991).abs()<1e-9);
let old=f(Venue::BinanceFutures,1,1_150_000_000,1_760_000_000_210,r#"{"data":{"e":"bookTicker","s":"BTCUSDT","b":"104","B":"1","a":"105","A":"1","E":1760000000210,"u":21}}"#);assert_eq!(o.ingest(&old,&mut h),Err(CoreError::OutOfOrder));
o.reset_venue("BTC",Venue::OkxSpot,2).unwrap();assert!(o.snapshot_venue("BTC",Venue::OkxSpot).is_none());
let stale=f(Venue::OkxSpot,1,1_300_000_000,1_760_000_000_300,r#"{"arg":{"channel":"books5","instId":"BTC-USDT"},"data":[{"bids":[["100","2"]],"asks":[["101","4"]],"ts":"1760000000300","seqId":12}]}"#);assert_eq!(o.ingest(&stale,&mut h),Err(CoreError::StaleEpoch));
println!("{}", r#"{"r7_books_parity":"PASS","normalization":"PASS","unchanged_changed_ns":"PASS","paired_snapshot":"PASS","freshness_800ms":"PASS","out_of_order":"PASS","reset_invalidation":"PASS","stale_epoch":"PASS","trading":false}"#);
}