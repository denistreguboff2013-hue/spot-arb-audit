//! Local operator request for graceful PUBLIC PAPER session sealing.
 //! No network, private exchange API, real orders, DB1/DB2 or service stop.
use serde_json::{json,Value};
use std::{fs::{self,File,OpenOptions},io::{self,Write},path::{Path,PathBuf}};
const ROOT:&str="/mnt/spotarb_db_test/astra_paper_continuous_v1";
fn bad(reason:&str)->io::Error{io::Error::new(io::ErrorKind::Other,reason.to_string())}
fn bounded_json(path:&Path)->io::Result<Value>{
    let m=fs::metadata(path)?;
    if m.len()==0||m.len()>8192{return Err(bad("invalid control-file size"))}
    serde_json::from_slice(&fs::read(path)?).map_err(|_|bad("malformed control JSON"))
}
fn request(root:&Path)->io::Result<PathBuf>{
    let active=bounded_json(&root.join("active.json"))?;
    let current=bounded_json(&root.join("current_session.json"))?;
    let session=active["session"].as_str().ok_or_else(||bad("active session missing"))?;
    if session.len()>100||!session.starts_with("session_")||
       !session.bytes().all(|x|x.is_ascii_alphanumeric()||x==b'_')||
       current["session"].as_str()!=Some(session)||
       current["paper_only"]!=true||
       active["real_orders"]!=false{
        return Err(bad("current and active PAPER session mismatch"))
    }
    let dir=root.join("sessions").join(session);
    let path=dir.join("graceful_stop_request.json");
    // This field only exists on the new, tested Rust supervisor. An old
    // supervisor cannot be force-sealed using an unsupported protocol.
    if current["graceful_stop_request_path"].as_str()!=path.to_str(){
        return Err(bad("active observer has no approved graceful-stop protocol"))
    }
    if !fs::metadata(&dir)?.is_dir(){return Err(bad("session directory missing"))}
    let bytes=serde_json::to_vec(&json!({
       "schema":"SPOTARB/PAPER/GRACEFUL-STOP/V1",
       "session":session,
       "scope":"public-paper-only",
       "real_trading":false,
    }))?;
    let mut f=OpenOptions::new().write(true).create_new(true).open(&path)?;
    f.write_all(&bytes)?;
    f.sync_all()?;
    drop(f);
    File::open(&dir)?.sync_all()?;
    Ok(path)
}
fn main()->io::Result<()>{
    let path=request(Path::new(ROOT))?;
    println!("PUBLIC_PAPER_SEAL_REQUESTED {} (no exchange actions)",path.display());
    Ok(())
}
#[cfg(test)]
mod tests{
  use super::*;
  #[test]
  fn writes_only_matching_active_session_and_does_not_overwrite(){
    let root=std::env::temp_dir().join(format!(
        "paper_graceful_request_{}_{}",std::process::id(),
        spotarb_native_market_core::transport::monotonic_ns()));
    let session="session_123_456";
    let dir=root.join("sessions").join(session);
    fs::create_dir_all(&dir).unwrap();
    let path=dir.join("graceful_stop_request.json");
    fs::write(root.join("active.json"),serde_json::to_vec(&json!({
        "session":session,"real_orders":false})).unwrap()).unwrap();
    fs::write(root.join("current_session.json"),serde_json::to_vec(&json!({
        "session":session,"paper_only":true,
        "graceful_stop_request_path":path})).unwrap()).unwrap();
    assert_eq!(request(&root).unwrap(),path);
    assert!(request(&root).is_err(),"duplicate request not silently overwritten");
    fs::remove_file(&path).unwrap();
    let mut corrupted=bounded_json(&root.join("current_session.json")).unwrap();
    corrupted["graceful_stop_request_path"]=json!("/tmp/wrong.json");
    fs::write(root.join("current_session.json"),
        serde_json::to_vec(&corrupted).unwrap()).unwrap();
    assert!(request(&root).is_err());
    assert!(!path.exists());
    fs::remove_dir_all(root).unwrap();
  }
}
