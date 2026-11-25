//! P1-06 Telegram delivery adapter (Rust, explicitly opted-in, no trade).
//!
//! Uses the already fsynced at-most-once outbox to reserve an original
//! notification before ANY network write. Lost/ambiguous HTTP results
//! remain ClaimUnknown, never reissued blindly. No background service or
//! API credential reading is performed by this library.
use crate::{
 native_telegram_durable_outbox::{
   claim_russian_preview,confirm_telegram_ack,ClaimDecision,OutboxError,
 },
 native_observability_review::{NativeObservePolicy,ObservationKind,OfflineNativeSample},
};
use native_tls::TlsConnector;
use serde_json::{json,Value};
use std::{io::{Read,Write},net::{TcpStream,ToSocketAddrs},
 path::Path,time::Duration};

const TG_HOST:&str="api.telegram.org";
const PORT:u16=443;
const MAX_HTTP_RESPONSE:usize=48*1024;
const MAX_JSON_BODY:usize=16*1024;

#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum TelegramSendError {Denied,InvalidAuth,InvalidBody,Network,Protocol,Rejected,WrongChat}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct TelegramAck{pub message_id:u64}
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum DeliveryResult{
 Suppressed{reason:&'static str},
 Confirmed{event_id:String,message_id:u64},
 Uncertain{event_id:String},
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum DeliveryError {NotAuthorized,Outbox(OutboxError)}

pub trait TelegramSendTransport {
 /// A single attempt; transport MUST NOT retry automatically.
 fn send_plain_text_once(&self,text:&str)->Result<TelegramAck,TelegramSendError>;
}
/// The explicit notification-only authorization is separate from the
/// native real-execution authorization. It conveys NO trading privileges.
pub fn deliver_one_russian_notification<T:TelegramSendTransport>(
 outbox:&Path,policy:&NativeObservePolicy,kind:ObservationKind,
 sample:&OfflineNativeSample,now_ms:u64,
 notification_send_authorized:bool,transport:&T,
)->Result<DeliveryResult,DeliveryError>{
 if !notification_send_authorized{
  return Err(DeliveryError::NotAuthorized);
 }
 let decision=claim_russian_preview(outbox,policy,kind,sample,now_ms)
  .map_err(DeliveryError::Outbox)?;
 let (event_id,body)=match decision{
  ClaimDecision::Suppressed{reason}=>return Ok(DeliveryResult::Suppressed{reason}),
  ClaimDecision::Claimed{event_id,body}=>(event_id,body),
 };
 // ClaimUnknown is durable on disk before this call. On failure including
 // timeout, broken response, or Telegram ok:false, do not retry.
 let ack=match transport.send_plain_text_once(&body){
  Ok(ack) if ack.message_id>0=>ack,
  _=>return Ok(DeliveryResult::Uncertain{event_id}),
 };
 // If this fsync fails AFTER Telegram accepted the message, keep local
 // journal ClaimUnknown and do not automatically send another copy.
 confirm_telegram_ack(outbox,&policy.config_sha256,&event_id,ack.message_id)
  .map_err(DeliveryError::Outbox)?;
 Ok(DeliveryResult::Confirmed{event_id,message_id:ack.message_id})
}

/// A plaintext-in-memory credential container. No Debug/Serialize/Display
/// impl: never log or persist bot tokens, including on TLS/DNS failures.
pub struct PinnedTelegramTls {
 token:String,
 chat_id:i64,
 // Only an explicitly configured loopback HTTP CONNECT proxy. This
 // route is never inferred from an untrusted request or message.
 local_http_proxy_port:Option<u16>,
}
impl PinnedTelegramTls {
 pub fn new(bot_token:String,chat_id:&str)->Result<Self,TelegramSendError>{
  let (prefix,suffix)=bot_token.split_once(':')
   .ok_or(TelegramSendError::InvalidAuth)?;
  if !(6..=16).contains(&prefix.len())
   ||!prefix.bytes().all(|b|b.is_ascii_digit())
   ||!(30..=128).contains(&suffix.len())
   ||!suffix.bytes().all(|b|
      b.is_ascii_alphanumeric()||b==b'-'||b==b'_')
  {return Err(TelegramSendError::InvalidAuth)}
  let id=chat_id.parse::<i64>().map_err(|_|TelegramSendError::InvalidAuth)?;
  if id==0||chat_id.trim()!=chat_id||chat_id.starts_with('+')
   ||chat_id.len()>21{return Err(TelegramSendError::InvalidAuth)}
  Ok(Self{token:bot_token,chat_id:id,local_http_proxy_port:None})
 }
 /// The existing Python notifier uses a loopback HTTP proxy. Accept
 /// only http://127.0.0.1:PORT or http://localhost:PORT; no proxy
 /// credentials, remote host, URL path, redirect or arbitrary tunnel.
 pub fn new_with_local_http_proxy(
  bot_token:String,chat_id:&str,proxy_url:&str,
 )->Result<Self,TelegramSendError>{
  let port_text=proxy_url.strip_prefix("http://127.0.0.1:")
   .or_else(||proxy_url.strip_prefix("http://localhost:"))
   .ok_or(TelegramSendError::Denied)?;
  if port_text.is_empty()||port_text.len()>5||
     !port_text.bytes().all(|b|b.is_ascii_digit()){
   return Err(TelegramSendError::Denied)
  }
  let port=port_text.parse::<u16>().map_err(|_|TelegramSendError::Denied)?;
  if port==0{return Err(TelegramSendError::Denied)}
  let mut sender=Self::new(bot_token,chat_id)?;
  sender.local_http_proxy_port=Some(port);
  Ok(sender)
 }
 pub fn chat_id(&self)->i64{self.chat_id}
 /// R84: validate a TOKEN-FREE TLS handshake before a lifecycle
 /// notification is durably claimed. Never sends a Bot API request.
 /// A failed handshake must NOT consume an at-most-once event.
 pub fn preflight_local_telegram_tls(&self)->Result<(),TelegramSendError>{
  let port=self.local_http_proxy_port.ok_or(TelegramSendError::Denied)?;
  let mut tcp=TcpStream::connect_timeout(
   &std::net::SocketAddr::from(([127,0,0,1],port)),
   Duration::from_secs(4)).map_err(|_|TelegramSendError::Network)?;
  tcp.set_read_timeout(Some(Duration::from_secs(8)))
   .map_err(|_|TelegramSendError::Network)?;
  tcp.set_write_timeout(Some(Duration::from_secs(8)))
   .map_err(|_|TelegramSendError::Network)?;
  negotiate_telegram_connect(&mut tcp)?;
  let connector=TlsConnector::new().map_err(|_|TelegramSendError::Network)?;
  let _tls=connector.connect(TG_HOST,tcp)
   .map_err(|_|TelegramSendError::Network)?;
  Ok(())
 }
 /// Only Telegram API hostname; optional *loopback* HTTP CONNECT
 /// before normal peer-certified TLS. No redirects or arbitrary hosts.
 fn send_tls_once(&self,plain_text:&str)->Result<TelegramAck,TelegramSendError>{
  if plain_text.is_empty()||plain_text.len()>4_096
    ||plain_text.chars().count()>4_096
  {return Err(TelegramSendError::InvalidBody)}
  let payload=serde_json::to_vec(&json!({
   "chat_id":self.chat_id,"text":plain_text,
   "disable_notification":false,"disable_web_page_preview":true
  })).map_err(|_|TelegramSendError::InvalidBody)?;
  if payload.len()>8_192{return Err(TelegramSendError::InvalidBody)}
  let target=format!("/bot{}/sendMessage",self.token);
  // No credential in error types, logs, or persisted outbox.
  // Direct getaddrinfo may block in the OS; local-proxy path does not
  // perform remote hostname resolution on this VM (P1-08 remains open).
  let mut tcp=if let Some(port)=self.local_http_proxy_port {
   TcpStream::connect_timeout(
    &std::net::SocketAddr::from(([127,0,0,1],port)),
    Duration::from_secs(4))
    .map_err(|_|TelegramSendError::Network)?
  }else{
   let addrs=(TG_HOST,PORT).to_socket_addrs()
    .map_err(|_|TelegramSendError::Network)?;
   let mut connected=None;
   for addr in addrs.take(6){
    if let Ok(s)=TcpStream::connect_timeout(&addr,Duration::from_secs(4)){
     connected=Some(s);break;
    }
   }
   connected.ok_or(TelegramSendError::Network)?
  };
  tcp.set_read_timeout(Some(Duration::from_secs(8)))
   .map_err(|_|TelegramSendError::Network)?;
  tcp.set_write_timeout(Some(Duration::from_secs(8)))
   .map_err(|_|TelegramSendError::Network)?;
  if self.local_http_proxy_port.is_some(){
   negotiate_telegram_connect(&mut tcp)?;
  }
  let connector=TlsConnector::new().map_err(|_|TelegramSendError::Network)?;
  // Certificate verification and hostname validation are ON by default.
  let mut tls=connector.connect(TG_HOST,tcp)
   .map_err(|_|TelegramSendError::Network)?;
  let header=format!(
   "POST {target} HTTP/1.1\r\nHost: {TG_HOST}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
   payload.len());
  tls.write_all(header.as_bytes()).map_err(|_|TelegramSendError::Network)?;
  tls.write_all(&payload).map_err(|_|TelegramSendError::Network)?;
  tls.flush().map_err(|_|TelegramSendError::Network)?;
  let (status,body)=read_complete_http_response(&mut tls)?;
  parse_bot_send_ack(status,&body,self.chat_id,plain_text)
 }
}
impl TelegramSendTransport for PinnedTelegramTls{
 fn send_plain_text_once(&self,text:&str)->Result<TelegramAck,TelegramSendError>{
  self.send_tls_once(text)
 }
}

/// Establish exactly one CONNECT tunnel to Telegram through the
/// already restricted local HTTP proxy. No bot token in this request.
fn negotiate_telegram_connect(tcp:&mut TcpStream)->Result<(),TelegramSendError>{
 tcp.write_all(b"CONNECT api.telegram.org:443 HTTP/1.1\r\nHost: api.telegram.org:443\r\nProxy-Connection: keep-alive\r\n\r\n")
  .map_err(|_|TelegramSendError::Network)?;
 let mut reply=Vec::with_capacity(128);
 while reply.len()<4096{
  let mut one=[0u8;1];
  tcp.read_exact(&mut one).map_err(|_|TelegramSendError::Network)?;
  reply.push(one[0]);
  if reply.ends_with(b"\r\n\r\n"){break}
 }
 if !reply.ends_with(b"\r\n\r\n"){return Err(TelegramSendError::Protocol)}
 let header=std::str::from_utf8(&reply).map_err(|_|TelegramSendError::Protocol)?;
 let first=header.split("\r\n").next().unwrap_or("");
 if !(first.starts_with("HTTP/1.1 200 ")||
      first.starts_with("HTTP/1.0 200 ")){
  return Err(TelegramSendError::Rejected)
 }
 Ok(())
}

/// Stop reading as soon as the complete *framed* response is present.
/// read_to_end() can time out through proxies that keep TLS connections
/// open, even after a valid JSON ACK is fully received.
fn read_complete_http_response<R:Read>(reader:&mut R)
 ->Result<(u16,Vec<u8>),TelegramSendError>{
 let mut response=Vec::with_capacity(4096);
 let mut chunk=[0u8;2048];
 loop{
  let n=match reader.read(&mut chunk){
   Ok(n)=>n,
   Err(_)=>return Err(TelegramSendError::Network)
  };
  if n==0{return parse_http_envelope(&response)}
  response.extend_from_slice(&chunk[..n]);
  if response.len()>MAX_HTTP_RESPONSE{return Err(TelegramSendError::Protocol)}
  if let Ok(complete)=parse_http_envelope(&response){return Ok(complete)}
 }
}

fn decode_chunked(input:&[u8])->Result<Vec<u8>,TelegramSendError>{
 let mut output=Vec::new();
 let mut offset=0;
 loop{
  let line_end=input.get(offset..).ok_or(TelegramSendError::Protocol)?
   .windows(2).position(|w|w==b"\r\n").ok_or(TelegramSendError::Protocol)?;
  if line_end==0||line_end>12{return Err(TelegramSendError::Protocol)}
  let line=std::str::from_utf8(&input[offset..offset+line_end])
   .map_err(|_|TelegramSendError::Protocol)?;
  if !line.bytes().all(|b|b.is_ascii_hexdigit()){
   return Err(TelegramSendError::Protocol)
  }
  let count=usize::from_str_radix(line,16).map_err(|_|TelegramSendError::Protocol)?;
  offset=offset.checked_add(line_end+2).ok_or(TelegramSendError::Protocol)?;
  if count==0{
   if input.get(offset..)!=Some(&b"\r\n"[..]){
    return Err(TelegramSendError::Protocol)
   }
   return Ok(output)
  }
  if count>MAX_JSON_BODY||output.len()+count>MAX_JSON_BODY||
   input.get(offset..offset+count).is_none()||
   input.get(offset+count..offset+count+2)!=Some(&b"\r\n"[..]){
   return Err(TelegramSendError::Protocol)
  }
  output.extend_from_slice(&input[offset..offset+count]);
  offset+=count+2;
 }
}

/// Strict TLS peer response envelope. A redirect is *never* followed,
/// a multi-response pipeline, oversized body or conflicting framing
/// always fails closed. Header/body are never logged on any error.
pub fn parse_http_envelope(input:&[u8])->Result<(u16,Vec<u8>),TelegramSendError>{
 if input.len()>MAX_HTTP_RESPONSE||input.is_empty(){
  return Err(TelegramSendError::Protocol)
 }
 let sep=input.windows(4).position(|w|w==b"\r\n\r\n")
  .ok_or(TelegramSendError::Protocol)?;
 if sep>8_192{return Err(TelegramSendError::Protocol)}
 let headers=std::str::from_utf8(&input[..sep])
  .map_err(|_|TelegramSendError::Protocol)?;
 let mut lines=headers.split("\r\n");
 let line=lines.next().ok_or(TelegramSendError::Protocol)?;
 let fields=line.split_whitespace().collect::<Vec<_>>();
 if fields.len()<3||
  !(fields[0]=="HTTP/1.1"||fields[0]=="HTTP/1.0")||
  fields[1].len()!=3||
  !fields[1].bytes().all(|b|b.is_ascii_digit()){
  return Err(TelegramSendError::Protocol)
 }
 let status=fields[1].parse::<u16>().map_err(|_|TelegramSendError::Protocol)?;
 let mut content_length=None;
 let mut transfer_encoding=None;
 let mut json_type=false;
 for row in lines{
  let (key,value)=row.split_once(':').ok_or(TelegramSendError::Protocol)?;
  if key.is_empty()||value.bytes().any(|b|b<32&&b!=9){
   return Err(TelegramSendError::Protocol)
  }
  let key=key.trim().to_ascii_lowercase();
  let value=value.trim();
  match key.as_str(){
   "content-length"=>{
    if content_length.is_some()||value.is_empty()||
       !value.bytes().all(|b|b.is_ascii_digit()){
     return Err(TelegramSendError::Protocol)
    }
    let size=value.parse::<usize>().map_err(|_|TelegramSendError::Protocol)?;
    if size>MAX_JSON_BODY{return Err(TelegramSendError::Protocol)}
    content_length=Some(size);
   },
   "transfer-encoding"=>{
    if transfer_encoding.is_some(){return Err(TelegramSendError::Protocol)}
    transfer_encoding=Some(value.to_ascii_lowercase());
   },
   "content-encoding"=>{
    if !value.eq_ignore_ascii_case("identity"){
     return Err(TelegramSendError::Protocol)
    }
   },
   "content-type"=>{
    if json_type||!value.to_ascii_lowercase().starts_with("application/json"){
     return Err(TelegramSendError::Protocol)
    }
    json_type=true;
   },
   _=>{},
  }
 }
 if content_length.is_some()&&transfer_encoding.is_some(){
  return Err(TelegramSendError::Protocol)
 }
 // Positive ACK must have explicit bounded framing; a partial JSON
 // body without length/chunk markers must never be accepted early.
 if content_length.is_none()&&transfer_encoding.is_none(){
  return Err(TelegramSendError::Protocol)
 }
 if !json_type{return Err(TelegramSendError::Protocol)}
 let body=&input[sep+4..];
 let decoded=match transfer_encoding{
  Some(x) if x=="chunked"=>decode_chunked(body)?,
  Some(_)=>return Err(TelegramSendError::Protocol),
  None=>{
   if content_length.is_some_and(|length|length!=body.len())||
    body.len()>MAX_JSON_BODY{
    return Err(TelegramSendError::Protocol)
   }
   body.to_vec()
  },
 };
 Ok((status,decoded))
}

/// Only Telegram's authenticated TLS sendMessage reply can be turned
/// into a local fsync confirmation. Validate target chat and identical
/// text to avoid accepting a bot success for the wrong chat/notice.
pub fn parse_bot_send_ack(
 status:u16,body:&[u8],expected_chat:i64,expected_text:&str,
)->Result<TelegramAck,TelegramSendError>{
 if status!=200{return Err(TelegramSendError::Rejected)}
 if body.is_empty()||body.len()>MAX_JSON_BODY{
  return Err(TelegramSendError::Protocol)
 }
 let parsed:Value=serde_json::from_slice(body)
  .map_err(|_|TelegramSendError::Protocol)?;
 if parsed.get("ok")!=Some(&Value::Bool(true)){
  return Err(TelegramSendError::Rejected)
 }
 let reply=parsed.get("result").ok_or(TelegramSendError::Protocol)?;
 if reply.get("chat").and_then(|c|c.get("id")).and_then(Value::as_i64)
  !=Some(expected_chat){return Err(TelegramSendError::WrongChat)}
 if reply.get("text").and_then(Value::as_str)!=Some(expected_text){
  return Err(TelegramSendError::Protocol)
 }
 let id=reply.get("message_id").and_then(Value::as_u64)
  .filter(|id|*id>0).ok_or(TelegramSendError::Protocol)?;
 Ok(TelegramAck{message_id:id})
}

#[cfg(test)]
mod tests{
 use super::*;
 use crate::{
  native_trading_config::NativeTradingConfig,
  native_telegram_durable_outbox::{inspect_local_claim,DeliveryState},
 };
 use std::{
  fs,sync::atomic::{AtomicUsize,Ordering},
  time::{SystemTime,UNIX_EPOCH},
 };
 fn data(name:&str)->(std::path::PathBuf,NativeObservePolicy,OfflineNativeSample){
  let n=SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
  let parent=std::env::temp_dir().join(format!(
    "native_bot_rust_once_{name}_{}_{}",std::process::id(),n));
  fs::create_dir(&parent).unwrap();
  let config=NativeTradingConfig::parse(
   include_str!("../config/native_trading.yaml")).unwrap();
  let policy=NativeObservePolicy::from_config(&config).unwrap();
  let sample=OfflineNativeSample{
   observed_ms:1_800_000_000_000,owner_generation:4,
   symbol:Some("BTC".to_owned()),net_spread_milli_bps:Some(44_000),
   book_age_ms:25,decisions:100,ws_to_decision_p99_micros:200,
   worker_queue_p99_micros:100,ws_reconnects:0,worker_drops:0,
   fault_code:None,
  };
  (parent.join("notify.json"),policy,sample)
 }
 struct FakeSend {
  calls:AtomicUsize,
  success:bool,
 }
 impl TelegramSendTransport for FakeSend {
  fn send_plain_text_once(&self,body:&str)->Result<TelegramAck,TelegramSendError>{
   self.calls.fetch_add(1,Ordering::SeqCst);
   assert!(body.contains("BTC"));
   assert!(body.contains("Только сигнал"));
   if self.success{Ok(TelegramAck{message_id:512})}
   else{Err(TelegramSendError::Network)}
  }
 }
 #[test]
 fn no_authorization_means_no_claim_no_network(){
  let (path,p,s)=data("denied");
  let mock=FakeSend{calls:AtomicUsize::new(0),success:true};
  assert_eq!(deliver_one_russian_notification(&path,&p,ObservationKind::Candidate,
   &s,s.observed_ms+10,false,&mock),Err(DeliveryError::NotAuthorized));
  assert_eq!(mock.calls.load(Ordering::SeqCst),0);
  assert!(!path.exists());
  fs::remove_dir_all(path.parent().unwrap()).unwrap();
 }
 #[test]
 fn durable_before_mock_send_ack_and_replay_suppressed(){
  let (path,p,s)=data("ack");
  let mock=FakeSend{calls:AtomicUsize::new(0),success:true};
  let first=deliver_one_russian_notification(&path,&p,
   ObservationKind::Candidate,&s,s.observed_ms+10,true,&mock).unwrap();
  let event=match first{
   DeliveryResult::Confirmed{event_id,message_id}=>{
    assert_eq!(message_id,512);event_id
   },x=>panic!("unexpected {x:?}")
  };
  assert_eq!(mock.calls.load(Ordering::SeqCst),1);
  assert_eq!(inspect_local_claim(&path,&p.config_sha256,&event).unwrap(),
   Some(DeliveryState::Confirmed{telegram_message_id:512}));
  assert_eq!(deliver_one_russian_notification(&path,&p,
   ObservationKind::Candidate,&s,s.observed_ms+20,true,&mock),
   Ok(DeliveryResult::Suppressed{
    reason:"ALREADY_CLAIMED_UNKNOWN_OR_CONFIRMED"}));
  assert_eq!(mock.calls.load(Ordering::SeqCst),1);
  fs::remove_dir_all(path.parent().unwrap()).unwrap();
 }
 #[test]
 fn unknown_http_result_never_blind_retries(){
  let (path,p,s)=data("unknown");
  let mock=FakeSend{calls:AtomicUsize::new(0),success:false};
  let first=deliver_one_russian_notification(&path,&p,
   ObservationKind::Candidate,&s,s.observed_ms+10,true,&mock).unwrap();
  let event=match first{DeliveryResult::Uncertain{event_id}=>event_id,_=>panic!("expected unknown")};
  assert_eq!(inspect_local_claim(&path,&p.config_sha256,&event).unwrap(),
   Some(DeliveryState::ClaimedUnknown));
  assert!(matches!(deliver_one_russian_notification(&path,&p,
   ObservationKind::Candidate,&s,s.observed_ms+20,true,&mock),
   Ok(DeliveryResult::Suppressed{..})));
  assert_eq!(mock.calls.load(Ordering::SeqCst),1);
  fs::remove_dir_all(path.parent().unwrap()).unwrap();
 }
 #[test]
 fn telegram_ack_chat_text_status_are_verified(){
  let text="Наблюдение BTC на Rust. Только сигнал";
  let source=json!({"ok":true,"result":{
    "message_id":123,"date":1800000000,"chat":{"id":-234},"text":text
  }});
  let b=serde_json::to_vec(&source).unwrap();
  assert_eq!(parse_bot_send_ack(200,&b,-234,text),
   Ok(TelegramAck{message_id:123}));
  assert_eq!(parse_bot_send_ack(200,&b,123,text),Err(TelegramSendError::WrongChat));
  assert_eq!(parse_bot_send_ack(200,&b,-234,"other"),Err(TelegramSendError::Protocol));
  assert_eq!(parse_bot_send_ack(302,&b,-234,text),Err(TelegramSendError::Rejected));
  assert_eq!(parse_bot_send_ack(200,&serde_json::to_vec(
   &json!({"ok":false,"result":{"message_id":456,"chat":{"id":-234},"text":text}})
   ).unwrap(),-234,text),Err(TelegramSendError::Rejected));
  assert_eq!(parse_bot_send_ack(200,&serde_json::to_vec(
   &json!({"ok":true,"result":{"message_id":0,"chat":{"id":-234},"text":text}})
   ).unwrap(),-234,text),Err(TelegramSendError::Protocol));
 }
 #[test]
 fn http_framing_redirect_chunked_duplicate_and_oversize(){
  let body=b"{\"ok\":true}";
  let hdr=format!("HTTP/1.1 200 OK\r\nContent-Type: application/json; charset=utf-8\r\nContent-Length: {}\r\n\r\n",body.len());
  let full=[hdr.as_bytes(),body].concat();
  let (status,parsed)=parse_http_envelope(&full).unwrap();
  assert_eq!(status,200);assert_eq!(parsed,body);
  let raw=b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\nb\r\n{\"ok\":true}\r\n0\r\n\r\n";
  assert_eq!(parse_http_envelope(raw).unwrap().1,body);
  let duplicate=b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 11\r\nContent-Length: 11\r\n\r\n{\"ok\":true}";
  assert_eq!(parse_http_envelope(duplicate),Err(TelegramSendError::Protocol));
  let confuse=b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 11\r\nTransfer-Encoding: chunked\r\n\r\n{\"ok\":true}";
  assert_eq!(parse_http_envelope(confuse),Err(TelegramSendError::Protocol));
  let oversized=vec![b' ';MAX_HTTP_RESPONSE+1];
  assert_eq!(parse_http_envelope(&oversized),Err(TelegramSendError::Protocol));
 }
 #[test]
 fn r84_preflight_cannot_use_direct_telegram_or_send_a_bot_message(){
   let s=PinnedTelegramTls::new(
     format!("12345678:{}", "A".repeat(40)),"12345").unwrap();
   assert_eq!(s.preflight_local_telegram_tls(),
      Err(TelegramSendError::Denied));
 }
 #[test]
 fn only_loopback_http_proxy_is_permitted_no_secret_in_proxy_url(){
  let token=format!("123456:{}", "A".repeat(35));
  assert!(PinnedTelegramTls::new_with_local_http_proxy(
   token.clone(),"-123456","http://127.0.0.1:7890").is_ok());
  assert!(PinnedTelegramTls::new_with_local_http_proxy(
   token.clone(),"-123456","http://localhost:7890").is_ok());
  for malicious in [
   "https://127.0.0.1:7890",
   "http://api.telegram.org:443",
   "http://127.0.0.1:7890/other",
   "http://127.0.0.1:0",
   "http://127.0.0.1:99999",
   "http://127.0.0.1:7890@evil.invalid",
   "http://user:pass@127.0.0.1:7890",
   "http://localhost:7890?x=1",
   "",
  ]{
   assert!(PinnedTelegramTls::new_with_local_http_proxy(
    token.clone(),"-123456",malicious).is_err(),
    "arbitrary proxy must be denied");
  }
 }
 #[test]
 fn loopback_http_connect_and_framed_reply_without_eof(){
  use std::{net::TcpListener,thread,io::{Cursor,Error,ErrorKind}};
  let listener=TcpListener::bind("127.0.0.1:0").unwrap();
  let addr=listener.local_addr().unwrap();
  let server=thread::spawn(move||{
   let (mut sock,_)=listener.accept().unwrap();
   sock.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
   let mut bytes=Vec::new();
   while !bytes.ends_with(b"\r\n\r\n"){
    let mut one=[0u8;1];sock.read_exact(&mut one).unwrap();
    bytes.push(one[0]);assert!(bytes.len()<4096);
   }
   assert!(bytes.starts_with(b"CONNECT api.telegram.org:443 HTTP/1.1\r\n"));
   assert!(!bytes.windows(4).any(|w|w==b"/bot"));
   sock.write_all(b"HTTP/1.1 200 Connection established\r\n\r\n").unwrap();
  });
  let mut client=TcpStream::connect(addr).unwrap();
  client.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
  negotiate_telegram_connect(&mut client).unwrap();
  server.join().unwrap();

  struct DataThenTimeout(Cursor<Vec<u8>>);
  impl Read for DataThenTimeout{
   fn read(&mut self,out:&mut[u8])->std::io::Result<usize>{
    let size=self.0.read(out)?;
    if size==0{return Err(Error::new(ErrorKind::TimedOut,"mock keepalive"))}
    Ok(size)
   }
  }
  let body=b"{\"ok\":true}";
  let header=format!(
   "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
   body.len());
  let mut reader=DataThenTimeout(Cursor::new([header.as_bytes(),body].concat()));
  let (status,reply)=read_complete_http_response(&mut reader).unwrap();
  assert_eq!(status,200);
  assert_eq!(reply,body);
 }
 #[test]
 fn credentials_validate_but_not_logged_or_networked(){
  let token=format!("123456:{}", "A".repeat(35));
  assert!(PinnedTelegramTls::new(token.clone(),"-123456").is_ok());
  assert!(PinnedTelegramTls::new("bad".into(),"123").is_err());
  assert!(PinnedTelegramTls::new(token.clone(),"0").is_err());
  assert!(PinnedTelegramTls::new(token.clone(),"https://evil.invalid").is_err());
  assert!(PinnedTelegramTls::new(token.clone(),"123\r\nInjected").is_err());
  assert!(PinnedTelegramTls::new("123456:abc/def".into(),"123").is_err());
 }
}
