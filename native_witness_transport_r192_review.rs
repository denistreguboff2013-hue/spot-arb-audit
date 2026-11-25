// R192: loopback-only transport failure test, no exchange endpoints.
// Socket success alone NEVER authenticates remote identity or proves durability.
#[derive(Debug,Clone,Copy,PartialEq,Eq)]
pub enum WitnessTransportR192{Unavailable,Timeout,Malformed,Received}
fn probe_test_socket_r192(addr:std::net::SocketAddr,deadline_ms:u64)
 ->WitnessTransportR192{
 use std::{io::{Read,Write},net::TcpStream,time::Duration};
 if !addr.ip().is_loopback()||deadline_ms==0||deadline_ms>2000{
  return WitnessTransportR192::Unavailable
 }
 let timeout=Duration::from_millis(deadline_ms);
 let mut sock=match TcpStream::connect_timeout(&addr,timeout){
  Ok(s)=>s,Err(e)=>return if e.kind()==std::io::ErrorKind::TimedOut{
   WitnessTransportR192::Timeout
  }else{WitnessTransportR192::Unavailable}
 };
 if sock.set_read_timeout(Some(timeout)).is_err()
   ||sock.set_write_timeout(Some(timeout)).is_err(){
  return WitnessTransportR192::Unavailable
 }
 if sock.write_all(b"ASTRA-R192-PROBE\n").is_err(){
  return WitnessTransportR192::Unavailable
 }
 let mut response=[0u8;16];
 match sock.read_exact(&mut response){
  Ok(())=>if &response==b"RESEARCH_ONLY_OK"{
   WitnessTransportR192::Received
  }else{WitnessTransportR192::Malformed},
  Err(e)=>if matches!(e.kind(),std::io::ErrorKind::TimedOut
    |std::io::ErrorKind::WouldBlock){WitnessTransportR192::Timeout}
   else{WitnessTransportR192::Malformed},
 }
}
#[cfg(test)]
mod r192_tests{
 use super::*;
 use std::{net::TcpListener,io::{Read,Write},thread,time::Duration};
 fn serve(reply:&'static [u8],pause_ms:u64)
  ->(std::net::SocketAddr,thread::JoinHandle<()>){
  let listener=TcpListener::bind("127.0.0.1:0").unwrap();
  let addr=listener.local_addr().unwrap();
  let task=thread::spawn(move||{
   let (mut stream,_)=listener.accept().unwrap();
   let mut req=[0u8;16];
   let _=stream.read_exact(&mut req);
   thread::sleep(Duration::from_millis(pause_ms));
   let _=stream.write_all(reply);
  });
  (addr,task)
 }
 #[test]fn r192_loopback_success_is_transport_only(){
  let (addr,t)=serve(b"RESEARCH_ONLY_OK",0);
  assert_eq!(probe_test_socket_r192(addr,1000),WitnessTransportR192::Received);
  t.join().unwrap();
 }
 #[test]fn r192_timeout_partial_payload_and_closed_port_deny(){
  let (addr,t)=serve(b"RESEARCH_ONLY_OK",150);
  assert_eq!(probe_test_socket_r192(addr,20),WitnessTransportR192::Timeout);
  t.join().unwrap();
  let (addr,t)=serve(b"BAD",0);
  assert_eq!(probe_test_socket_r192(addr,200),WitnessTransportR192::Malformed);
  t.join().unwrap();
  let listener=TcpListener::bind("127.0.0.1:0").unwrap();
  let addr=listener.local_addr().unwrap();drop(listener);
  assert_eq!(probe_test_socket_r192(addr,50),WitnessTransportR192::Unavailable);
 }
}
