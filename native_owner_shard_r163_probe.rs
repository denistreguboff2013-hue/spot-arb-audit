//! R163: synthetic OFFLINE audit of 235 instrument pair owners.
//! Models the R21 12-owner, 32-symbol public WS shard topology.
//! Does NOT connect to an exchange, touch PAPER, or establish runtime throughput.
use serde_json::json;
use spotarb_native_market_core::{
    owner_index,CoreError,HistorySink,Owner,RawFrame,Update,Venue,
};
use spotarb_native_market_core::native_public_ws_owner_topology_r126_review::{
    PublicTickerTopologyR126,TickerSubscription,
};
use spotarb_native_market_core::native_public_turnover_ws_r123_review::PublicWsEnvelope;
use std::collections::HashSet;
use tokio::sync::mpsc;

const N:usize=235;
const OWNERS:usize=12;
const WS_SHARD:usize=32;
const EVENT_QUEUE:usize=64;
const NS:u64=100_000_000_000;
const WALL:i64=1_791_783_600_000;
fn bases()->Vec<String>{
    // 235 deterministic synthetic tickers, never presented as actual exchange symbols.
    (0..N).map(|i|format!("SIM{i:04}")).collect()
}
#[derive(Default)]
struct MandatoryHistory{accepted:usize,limit:usize}
impl HistorySink for MandatoryHistory{
    fn accept(&mut self,_:&RawFrame,_:&Update)->Result<(),CoreError>{
        if self.accepted>=self.limit{return Err(CoreError::HistoryPressure)}
        self.accepted+=1;
        Ok(())
    }
}
fn frame(base:&str,venue:Venue,epoch:u64,seq:i64,ns:u64,ts:i64)->RawFrame{
    let raw=match venue{
        Venue::OkxSpot=>serde_json::json!({
            "arg":{"channel":"books5","instId":format!("{base}-USDT")},
            "data":[{
                "bids":[["99","10"]],"asks":[["100","10"]],
                "seqId":seq,"ts":ts.to_string()
            }]
        }),
        Venue::BinanceFutures=>serde_json::json!({
            "stream":format!("{}usdt@depth20@100ms",base.to_ascii_lowercase()),
            "data":{
                "e":"depthUpdate","s":format!("{base}USDT"),
                "st":1,"E":ts,"U":seq,"u":seq,"pu":seq-1,
                "b":[["101","10"]],"a":[["102","10"]]
            }
        }),
    };
    RawFrame{source:None,venue,epoch,received_ns:ns,received_wall_ms:WALL,raw:raw.to_string()}
}
fn logical(base:&str,shard:usize)->Result<PublicTickerTopologyR126,String>{
    let owner=owner_index(base,OWNERS);
    let url=format!("wss://fstream.binance.com/market/ws/{}usdt@ticker",
        base.to_ascii_lowercase());
    let rows=[
        TickerSubscription{connection_id:format!("okx-shard-{shard}"),
            owner,venue:Venue::OkxSpot,base:base.into(),
            url:"wss://ws.okx.com/ws/v5/public".into()},
        TickerSubscription{connection_id:format!("bn-shard-{shard}"),
            owner,venue:Venue::BinanceFutures,base:base.into(),url},
    ];
    PublicTickerTopologyR126::new_with_owner_count(owner,OWNERS,&rows)
        .map_err(|e|format!("R163 topology: {e:?}"))
}
fn review()->Result<serde_json::Value,String>{
    let all=bases();
    if all.len()!=N ||all.iter().cloned().collect::<HashSet<_>>().len()!=N{
        return Err("R163 synthetic inventory duplicate/size mismatch".into())
    }
    let mut owners=(0..OWNERS).map(|i|Owner::new(i,OWNERS,800)
        .map_err(|e|format!("owner init: {e:?}")))
        .collect::<Result<Vec<_>,_>>()?;
    let mut histories=(0..OWNERS).map(|_|MandatoryHistory{
        limit:2*N+1,accepted:0}).collect::<Vec<_>>();
    let mut histogram=[0_usize;OWNERS];
    let mut sharded=[0_usize;8];
    let mut wrong_owner_denials=0_usize;
    let mut checked_pairs=0_usize;
    for (i,base) in all.iter().enumerate(){
        let idx=owner_index(base,OWNERS);
        let shard=i/WS_SHARD;
        histogram[idx]+=1;sharded[shard]+=1;
        if !owners[idx].owns(base){
            return Err("R163 owner hash mismatch".into())
        }
        if (0..OWNERS).filter(|&x|owners[x].owns(base)).count()!=1{
            return Err("R163 base assigned to multiple owners".into())
        }
        let topo=logical(base,shard)?;
        if topo.bases()!=1{return Err("R163 topology missing pair".into())}
        let okx_url="wss://ws.okx.com/ws/v5/public";
        let bn_url=format!("wss://fstream.binance.com/market/ws/{}usdt@ticker",
            base.to_ascii_lowercase());
        let o=PublicWsEnvelope{venue:Venue::OkxSpot,base,url:okx_url,
            connection_epoch:1,sequence:1,received_wall_ms:WALL,body:"{}"};
        let b=PublicWsEnvelope{venue:Venue::BinanceFutures,base,url:&bn_url,
            connection_epoch:1,sequence:1,received_wall_ms:WALL,body:"{}"};
        topo.check_pair(&format!("okx-shard-{shard}"),&format!("bn-shard-{shard}"),o,b)
            .map_err(|e|format!("R163 pair rejected: {e:?}"))?;
        if topo.check_pair("foreign-shard",&format!("bn-shard-{shard}"),o,b).is_ok(){
            return Err("R163 forgery passed topology".into())
        }
        let ts=WALL-100;let ns=NS+i as u64*1_000;
        let okx=frame(base,Venue::OkxSpot,1,1,ns,ts);
        let bn=frame(base,Venue::BinanceFutures,1,10,ns+1,ts);
        let foreign=(idx+1)%OWNERS;
        let old=histories[foreign].accepted;
        if owners[foreign].ingest(&okx,&mut histories[foreign]).err()!=Some(CoreError::WrongOwner)
           ||histories[foreign].accepted!=old{
            return Err("R163 foreign owner mutated history".into())
        }
        wrong_owner_denials+=1;
        if owners[idx].ingest(&okx,&mut histories[idx]).map_err(|e|
            format!("R163 OKX ingest failed: {e:?}"))?.is_some(){
            return Err("R163 emitted unpaired book".into())
        }
        let paired=owners[idx].ingest(&bn,&mut histories[idx]).map_err(|e|
            format!("R163 Binance ingest failed: {e:?}"))?;
        if paired.as_ref().map(|p|p.base.as_str())!=Some(base)||
           paired.as_ref().map(|p|p.indicative_bps<=0.0)!=Some(false){
            return Err("R163 two-venue paired snapshot denied".into())
        }
        checked_pairs+=1;
    }
    if histogram.iter().any(|&x|x==0)||sharded != [32,32,32,32,32,32,32,11]{
        return Err("R163 owner/shard distribution invalid".into())
    }
    if checked_pairs!=N||wrong_owner_denials!=N||
       histories.iter().map(|h|h.accepted).sum::<usize>()!=2*N{
        return Err("R163 admission totals mismatch".into())
    }
    // A full mandatory journal must fail BEFORE either new book or
    // cross-venue pair is modified. Test using an actually filled owner sink.
    let first=&all[0];let idx=owner_index(first,OWNERS);
    let (spot_before,fut_before)=owners[idx].snapshot_pair(first);
    histories[idx].limit=histories[idx].accepted;
    let extra=frame(first,Venue::OkxSpot,1,2,NS+1_000_000,WALL-20);
    if owners[idx].ingest(&extra,&mut histories[idx]).err()!=Some(CoreError::HistoryPressure){
        return Err("R163 full mandatory history did not block ingest".into())
    }
    let (spot_after,fut_after)=owners[idx].snapshot_pair(first);
    if spot_after!=spot_before||fut_after!=fut_before{
        return Err("R163 queue pressure mutated paired books".into())
    }
    // 12 owners remain separate from 8 physical 32-base WS shards.
    Ok(json!({"schema":"SPOTARB/R163/OFFLINE-235-OWNER-REVIEW/V1",
       "synthetic_not_actual_exchange_inventory":true,
       "instruments":N,"owners":OWNERS,"ws_shard_size":WS_SHARD,
       "ws_shards":sharded,"owner_histogram":histogram,
       "validated_two_venue_pairs":checked_pairs,
       "foreign_owner_denials":wrong_owner_denials,
       "mandatory_history_accepts":2*N,
       "mandatory_history_backpressure_denied_without_book_mutation":true,
       "public_topology_validated_for_each_base":true,
       "network_frames_observed":0,
       "source_r155_reconnect_gate_modified":false,
       "real_orders":false,"installed_paper_modified":false}))
}
#[tokio::main(flavor="current_thread")]
async fn main()->Result<(),Box<dyn std::error::Error>>{
    if std::env::args().skip(1).collect::<Vec<_>>()!=["--r163-offline-only"]{
        return Err("R163 requires --r163-offline-only; never connects to exchanges".into())
    }
    let mut result=review()?;
    let (tx,mut rx)=mpsc::channel::<usize>(EVENT_QUEUE);
    for i in 0..EVENT_QUEUE{tx.try_send(i)?}
    let denied=tx.try_send(EVENT_QUEUE).is_err();
    for i in 0..EVENT_QUEUE{
        if rx.try_recv()!=Ok(i){return Err("R163 bounded queue FIFO drift".into())}
    }
    if !denied{return Err("R163 saturated queue accepted extra item".into())}
    result["bounded_queue_capacity"]=json!(EVENT_QUEUE);
    result["queue_overflow_denied"]=json!(denied);
    result["queue_fifo_verified"]=json!(true);
    println!("{}",serde_json::to_string_pretty(&result)?);
    Ok(())
}
#[cfg(test)]
mod tests{
 use super::*;
 #[test]fn r163_235_synthetic_bases_twelve_owners_and_eight_shards(){
  let report=review().unwrap();
  assert_eq!(report["validated_two_venue_pairs"],N);
  assert_eq!(report["foreign_owner_denials"],N);
  assert_eq!(report["mandatory_history_accepts"],2*N);
  assert_eq!(report["mandatory_history_backpressure_denied_without_book_mutation"],true);
 }
 #[test]fn r163_stable_owner_fnv_and_shard_boundaries(){
  let all=bases();
  assert_eq!(all.len(),235);
  let groups=all.chunks(WS_SHARD).map(|x|x.len()).collect::<Vec<_>>();
  assert_eq!(groups,vec![32,32,32,32,32,32,32,11]);
  for base in all{
   assert_eq!(owner_index(&base,12),owner_index(&base,12));
  }
 }
 #[tokio::test]async fn r163_finite_queue_overflow_rejected_without_loss(){
  let (tx,mut rx)=mpsc::channel::<u64>(EVENT_QUEUE);
  for x in 0..EVENT_QUEUE{tx.try_send(x as u64).unwrap();}
  assert!(tx.try_send(999).is_err());
  for x in 0..EVENT_QUEUE{assert_eq!(rx.try_recv(),Ok(x as u64))}
 }
}
