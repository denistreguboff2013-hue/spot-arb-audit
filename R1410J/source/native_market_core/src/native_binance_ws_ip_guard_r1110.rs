//! Astra R1110: one owner-protected durable host-level gate for Binance PUBLIC WS
//! CONNECT ATTEMPTS by *participating* Rust clients. No sockets/API here.
//! A crash during update fails closed (never treats an existing empty file as fresh).
use serde::{Deserialize,Serialize};
use std::{fs::{self,File,OpenOptions},io::{Read,Seek,SeekFrom,Write},
    os::unix::{fs::{MetadataExt,OpenOptionsExt,PermissionsExt},io::AsRawFd},
    path::Path};

pub const R1110_MIN_CONNECT_GAP_MS:u64=3_000;
pub const R1110_WINDOW_MS:u64=60_000;
pub const R1110_MAX_CONNECTIONS_PER_MIN:u64=10;

#[derive(Debug,PartialEq,Eq)]
pub enum R1110Outcome{Granted,WaitMs(u64)}
#[derive(Debug)]
pub enum R1110Error{Security(&'static str),Io(std::io::Error),Corrupt}
impl From<std::io::Error> for R1110Error{
 fn from(e:std::io::Error)->Self{Self::Io(e)}
}
#[derive(Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
struct State{
 schema:String,window_start_ms:u64,attempts:u64,last_attempt_ms:u64
}
struct Lock(File);
impl Drop for Lock{
 fn drop(&mut self){unsafe{libc::flock(self.0.as_raw_fd(),libc::LOCK_UN);}}
}
/// Never probe Binance before this succeeds. A Wait is NOT a permit.
/// This gate is shared ONLY by programs calling this exact state file.
/// Explicit time is retained for deterministic failure/crash tests.
pub fn r1110_reserve(path:&Path,now_ms:u64)->Result<R1110Outcome,R1110Error>{
 r1110_reserve_clocked(path,||Ok(now_ms))
}
/// R1143: collect wall-clock time INSIDE flock, never before waiting
/// behind concurrently scheduled public WS shards. Same durable state,
/// 3-second gap, 10 connections/min and fail-closed corruption checks.
pub fn r1110_reserve_at_lock(path:&Path)->Result<R1110Outcome,R1110Error>{
 r1110_reserve_clocked(path,||{
  let age=std::time::SystemTime::now()
      .duration_since(std::time::UNIX_EPOCH)
      .map_err(|_|R1110Error::Corrupt)?;
  u64::try_from(age.as_millis()).map_err(|_|R1110Error::Corrupt)
 })
}
fn r1110_reserve_clocked<F:FnOnce()->Result<u64,R1110Error>>(
 path:&Path,read_clock:F
)->Result<R1110Outcome,R1110Error>{
 let parent=path.parent().ok_or(R1110Error::Security("no parent"))?;
 let md=fs::symlink_metadata(parent)?;
 if !md.file_type().is_dir()||md.permissions().mode()&0o077!=0||
   md.uid()!=unsafe{libc::geteuid()}{
   return Err(R1110Error::Security("shared directory is not private owner-only"))
 }
 let initially_absent=match fs::symlink_metadata(path){
   Ok(m)=>{
     if !m.file_type().is_file()||m.nlink()!=1||m.uid()!=unsafe{libc::geteuid()}||
       m.permissions().mode()&0o077!=0{
       return Err(R1110Error::Security("existing quota state is unsafe"))
     }
     false
   },
   Err(e) if e.kind()==std::io::ErrorKind::NotFound=>true,
   Err(e)=>return Err(e.into()),
 };
 let mut file=OpenOptions::new().read(true).write(true).create(true)
   .mode(0o600).custom_flags(libc::O_NOFOLLOW|libc::O_CLOEXEC)
   .open(path)?;
 if unsafe{libc::flock(file.as_raw_fd(),libc::LOCK_EX)}!=0{
    return Err(R1110Error::Io(std::io::Error::last_os_error()))
 }
 let _lock=Lock(file.try_clone()?);
 let meta=file.metadata()?;
 if !meta.is_file()||meta.len()>512||meta.nlink()!=1||
    meta.uid()!=unsafe{libc::geteuid()}||meta.permissions().mode()&0o077!=0{
    return Err(R1110Error::Security("locked quota state unsafe"))
 }
 let mut bytes=Vec::new();
 file.read_to_end(&mut bytes)?;
 // Obtain the timestamp under the same exclusive lock that orders
 // all recorded connection permits. No stale pre-flock timestamp.
 let now_ms=read_clock()?;
 let mut s=if bytes.is_empty(){
    if !initially_absent{return Err(R1110Error::Corrupt)}
    State{schema:"SPOTARB/ASTRA/R1110/BINANCE_WS_IP_ATTEMPTS/V1".into(),
      window_start_ms:now_ms,attempts:0,last_attempt_ms:0}
 }else{
    serde_json::from_slice::<State>(&bytes).map_err(|_|R1110Error::Corrupt)?
 };
 if s.schema!="SPOTARB/ASTRA/R1110/BINANCE_WS_IP_ATTEMPTS/V1"||
    s.attempts>R1110_MAX_CONNECTIONS_PER_MIN||
    s.window_start_ms>now_ms||
    s.last_attempt_ms>now_ms||
    (s.attempts>0&&s.last_attempt_ms<s.window_start_ms){
    return Err(R1110Error::Corrupt)
 }
 if now_ms.saturating_sub(s.window_start_ms)>=R1110_WINDOW_MS{
    s.window_start_ms=now_ms;s.attempts=0;
 }
 let gap_remaining=if s.attempts>0{
    R1110_MIN_CONNECT_GAP_MS.saturating_sub(
       now_ms.saturating_sub(s.last_attempt_ms))
 }else{0};
 let window_remaining=if s.attempts>=R1110_MAX_CONNECTIONS_PER_MIN{
    R1110_WINDOW_MS.saturating_sub(
       now_ms.saturating_sub(s.window_start_ms))
 }else{0};
 let wait=gap_remaining.max(window_remaining);
 if wait>0{return Ok(R1110Outcome::WaitMs(wait))}
 s.attempts+=1;s.last_attempt_ms=now_ms;
 let enc=serde_json::to_vec(&s).map_err(|_|R1110Error::Corrupt)?;
 if enc.len()>512{return Err(R1110Error::Corrupt)}
 file.seek(SeekFrom::Start(0))?;
 file.set_len(0)?;
 file.write_all(&enc)?;
 file.sync_all()?;
 Ok(R1110Outcome::Granted)
}

#[cfg(test)]
mod tests{
 use super::*;
 use std::os::unix::fs::DirBuilderExt;
 struct Private(std::path::PathBuf);
 impl Private{
 fn new()->Self{
    let mut p=std::env::temp_dir();
    p.push(format!("astra_r1110_{}_{}",std::process::id(),
        std::time::SystemTime::now().duration_since(
          std::time::UNIX_EPOCH).unwrap().as_nanos()));
    fs::DirBuilder::new().mode(0o700).create(&p).unwrap();
    Self(p)
 }
 fn state(&self)->std::path::PathBuf{self.0.join("binance_ip_guard.lock")}
 }
 impl Drop for Private{fn drop(&mut self){let _=fs::remove_dir_all(&self.0);}}
 #[test]
 fn r1110_first_grant_then_gap_then_window(){
    let p=Private::new();let f=p.state();let now=1_800_000_000_000u64;
    assert_eq!(r1110_reserve(&f,now).unwrap(),R1110Outcome::Granted);
    assert_eq!(r1110_reserve(&f,now+100).unwrap(),R1110Outcome::WaitMs(2900));
    for n in 1..10{
       assert_eq!(r1110_reserve(&f,now+n*3000).unwrap(),R1110Outcome::Granted)
    }
    assert_eq!(r1110_reserve(&f,now+30_000).unwrap(),R1110Outcome::WaitMs(30_000));
    assert_eq!(r1110_reserve(&f,now+60_000).unwrap(),R1110Outcome::Granted);
 }
 #[test]
 fn r1110_corrupt_rollback_or_empty_state_deny(){
    let p=Private::new();let f=p.state();let now=1_800_000_000_000u64;
    assert_eq!(r1110_reserve(&f,now).unwrap(),R1110Outcome::Granted);
    assert!(matches!(r1110_reserve(&f,now-1),Err(R1110Error::Corrupt)));
    fs::write(&f,b"partial-crash").unwrap();
    assert!(matches!(r1110_reserve(&f,now+4000),Err(R1110Error::Corrupt)));
    fs::write(&f,b"").unwrap();
    assert!(matches!(r1110_reserve(&f,now+4000),Err(R1110Error::Corrupt)));
 }
 #[test]
 fn r1110_symlink_or_unsafe_directory_deny(){
    let p=Private::new();
    let f=p.state();
    std::os::unix::fs::symlink("/dev/null",&f).unwrap();
    assert!(matches!(r1110_reserve(&f,100),Err(R1110Error::Security(_))));
    fs::remove_file(&f).unwrap();
    fs::set_permissions(&p.0,fs::Permissions::from_mode(0o755)).unwrap();
    assert!(matches!(r1110_reserve(&f,100),Err(R1110Error::Security(_))));
 }
 #[test]
 fn r1143_stale_prelock_timestamp_reproduces_corrupt_on_valid_state(){
    let p=Private::new();let f=p.state();
    let t=1_800_000_000_000u64;
    // Simulate asynchronous tasks: A acquires clock BEFORE worker B
    // reserves a later attempt, then A acquires flock AFTER B.
    assert_eq!(r1110_reserve(&f,t+3000).unwrap(),R1110Outcome::Granted);
    let prior=fs::read(&f).unwrap();
    assert!(matches!(r1110_reserve(&f,t),Err(R1110Error::Corrupt)));
    assert_eq!(fs::read(&f).unwrap(),prior,"no malformed on-disk guard");
 }
 #[test]
 fn r1143_clock_after_flock_prevents_false_corrupt_without_extra_permits(){
    let p=Private::new();let f=p.state();
    let t=1_800_000_000_000u64;
    let stale_prelock_clock=t;
    assert_eq!(r1110_reserve(&f,t+3000).unwrap(),R1110Outcome::Granted);
    assert!(matches!(r1110_reserve(&f,stale_prelock_clock),
                     Err(R1110Error::Corrupt)));
    // Lock-time sampling obtains the true later time, with exactly
    // the OLD 3-second wait and no extra Binance connection permit.
    assert_eq!(r1110_reserve_clocked(&f,||Ok(t+3001)).unwrap(),
               R1110Outcome::WaitMs(2999));
    assert_eq!(r1110_reserve_clocked(&f,||Ok(t+6000)).unwrap(),
               R1110Outcome::Granted);
    let stored:State=serde_json::from_slice(&fs::read(&f).unwrap()).unwrap();
    assert_eq!(stored.attempts,2);
    assert_eq!(stored.last_attempt_ms,t+6000);
    // A genuine invalid clock rollback remains fail-closed.
    assert!(matches!(r1110_reserve_clocked(&f,||Ok(t+5999)),
                     Err(R1110Error::Corrupt)));
 }
 #[test]
 fn r1143_real_clock_reservation_and_corrupt_file_stay_fail_closed(){
    let p=Private::new();let f=p.state();
    assert_eq!(r1110_reserve_at_lock(&f).unwrap(),R1110Outcome::Granted);
    assert!(matches!(r1110_reserve_at_lock(&f),
                     Ok(R1110Outcome::WaitMs(ms)) if ms>0&&ms<=3000));
    fs::write(&f,b"malformed").unwrap();
    assert!(matches!(r1110_reserve_at_lock(&f),Err(R1110Error::Corrupt)));
 }
 #[test]
 fn r1110_shared_real_file_uses_one_atomic_reservation(){
    let p=Private::new();let f=p.state();let now=1_800_000_000_000u64;
    let mut children=Vec::new();
    for _ in 0..10{
      let f=f.clone();
      children.push(std::thread::spawn(move||r1110_reserve(&f,now)));
    }
    let mut granted=0;
    for child in children{
      match child.join().unwrap().unwrap(){
        R1110Outcome::Granted=>granted+=1,
        R1110Outcome::WaitMs(_)=>{},
      }
    }
    assert_eq!(granted,1,"flock must grant only one simultaneous connect");
 }
}
