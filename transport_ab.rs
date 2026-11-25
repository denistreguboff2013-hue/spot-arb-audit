//! Transport-only control; not an end-to-end market capacity certificate.
use spotarb_native_market_core::ring::SharedRing;
use std::sync::{Arc,mpsc};
use std::time::Instant;
fn thread_cpu_ns()->u64 {
 let mut usage:libc::rusage=unsafe {std::mem::zeroed()};
 unsafe {libc::getrusage(libc::RUSAGE_THREAD,&mut usage)};
 (usage.ru_utime.tv_sec+usage.ru_stime.tv_sec) as u64 *1_000_000_000+
 (usage.ru_utime.tv_usec+usage.ru_stime.tv_usec) as u64 *1000
}
fn value(i:u64)->[u8;256] {let mut p=[0_u8;256];p[..8].copy_from_slice(&i.to_le_bytes());p}
fn main() {
 let n:usize=std::env::args().nth(1).unwrap_or("1000000".into()).parse().unwrap();
 for ring_mode in [false,true] {
  let began=Instant::now();
  let ring=Arc::new(SharedRing::new().unwrap());
  let (tx,rx)=mpsc::sync_channel::<[u8;256]>(128);
  let out=ring.clone();
  let producer=std::thread::spawn(move||{
    let cpu=thread_cpu_ns();
    let mut pressure=0_u64;
    for i in 0..n {
      let frame=value(i as u64);
      if ring_mode {
        loop { match out.try_push(&frame) {
          Ok(())=>break,
          Err(_)=>{pressure+=1;std::thread::yield_now();}
        }}
      } else {tx.send(frame).unwrap();}
    }
    (thread_cpu_ns()-cpu,pressure)
  });
  let cpu=thread_cpu_ns();
  let mut mismatch=0_u64;let mut empties=0_u64;
  for i in 0..n {
    let frame=if ring_mode {loop {
      if let Some(frame)=ring.try_pop(){break frame;}
      empties+=1;std::thread::yield_now();
    }} else {rx.recv().unwrap().to_vec()};
    if u64::from_le_bytes(frame[..8].try_into().unwrap())!=i as u64{mismatch+=1;}
  }
  let consumer_cpu=thread_cpu_ns()-cpu;
  let (producer_cpu,pressure)=producer.join().unwrap();
  println!("{{\"transport\":\"{}\",\"records\":{},\"wall_s\":{:.6},\"records_s\":{:.2},\"producer_cpu_s\":{:.6},\"consumer_cpu_s\":{:.6},\"pressure_retries\":{},\"empty_polls\":{},\"mismatch\":{}}}",
   if ring_mode {"mmap_spsc_ring"} else {"rust_sync_channel"},n,began.elapsed().as_secs_f64(),
   n as f64/began.elapsed().as_secs_f64(),producer_cpu as f64/1e9,consumer_cpu as f64/1e9,
   pressure,empties,mismatch);
 }
}
