//! R6 bounded admission overload proof. Review-only; no network/trading.
use tokio::sync::mpsc;
use std::sync::{Arc,atomic::{AtomicU64,Ordering}};
#[tokio::main(flavor="multi_thread",worker_threads=2)]
async fn main(){
 let cap=64usize;let offered=20_000u64;let (tx,mut rx)=mpsc::channel::<u64>(cap);
 let accepted=Arc::new(AtomicU64::new(0));let full=Arc::new(AtomicU64::new(0));
 let a=accepted.clone();let f=full.clone();
 let producer=tokio::spawn(async move{for i in 0..offered{match tx.try_send(i){Ok(())=>{a.fetch_add(1,Ordering::Relaxed);},Err(mpsc::error::TrySendError::Full(_))=>{f.fetch_add(1,Ordering::Relaxed);},Err(_)=>panic!("closed")}}});
 let consumer=tokio::spawn(async move{let mut n=0u64;while let Some(_)=rx.recv().await{n+=1;tokio::task::yield_now().await;}n});
 producer.await.unwrap();let consumed=consumer.await.unwrap();let ac=accepted.load(Ordering::Relaxed);let qf=full.load(Ordering::Relaxed);
 assert_eq!(offered,ac+qf);assert_eq!(ac,consumed);assert!(qf>0);assert!(ac<=offered);
 println!("{{\"r6_bounded_queue_gate\":\"PASS\",\"capacity\":{cap},\"offered\":{offered},\"accepted\":{ac},\"consumed\":{consumed},\"queue_full_signals\":{qf},\"silent_loss\":0,\"trading\":false}}");
}