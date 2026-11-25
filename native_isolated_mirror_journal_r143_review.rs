//! Astra R143–R145: isolated, local DB1 mirror-outbox **research** journal.
//! Requires a user-created 0700 /tmp/ directory named astra_r143_isolated_*.
//! Does not open an existing DB1/DB2 file, emit orders or authorize promotion.
use crate::native_mirror_outbox_protocol_r139_review::{
 audit_outbox_chain,MirrorOutboxRowR139,
};
use serde::{Deserialize,Serialize};
use openssl::sha::sha256;
use std::{fs::{self,File,OpenOptions},io::{Read,Write},path::Path,
 os::{fd::AsRawFd,unix::fs::{OpenOptionsExt,MetadataExt}},
};
const NAME:&str="astra-r143-source-review.jsonl";
const MAX_FILE:u64=16*1024*1024;
const MAX_ITEMS:usize=256;
const NOFOLLOW:i32=libc::O_NOFOLLOW|libc::O_CLOEXEC;
const ZERO:&str="0000000000000000000000000000000000000000000000000000000000000000";
#[derive(Debug,Clone,Copy,PartialEq,Eq)]
pub enum DurableError{UnsafePath,Io,Corrupt,Chain,Capacity,Conflict}
#[derive(Serialize,Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry{
 version:u8,anchor_seq:u64,previous_hash:String,
 row:MirrorOutboxRowR139,checksum:String,
}
#[derive(Debug,Clone,PartialEq,Eq)]
pub struct DurableReview{
 pub records:usize,pub last_seq:u64,pub last_hash:String,
 pub generation:Option<String>,
 pub can_apply_to_db2:bool,pub may_trade:bool,
}
fn digest(prev:&str,anchor:u64,row:&MirrorOutboxRowR139)->Result<String,DurableError>{
 let value=serde_json::to_vec(&(1u8,anchor,prev,row))
  .map_err(|_|DurableError::Corrupt)?;
 Ok(sha256(&value).iter().map(|b|format!("{b:02x}")).collect())
}
fn guard(path:&Path)->Result<(),DurableError>{
 if path.file_name().and_then(|v|v.to_str())!=Some(NAME){return Err(DurableError::UnsafePath)}
 let parent=path.parent().ok_or(DurableError::UnsafePath)?;
 let tmp=std::env::temp_dir();
 if parent.parent()!=Some(tmp.as_path())
   ||!parent.file_name().and_then(|v|v.to_str())
     .is_some_and(|x|x.starts_with("astra_r143_isolated_")&&x.len()<=120){
  return Err(DurableError::UnsafePath)
 }
 let m=fs::symlink_metadata(parent).map_err(|_|DurableError::UnsafePath)?;
 if !m.file_type().is_dir()||m.mode()&0o077!=0
   ||m.uid()!=unsafe{libc::geteuid()}{
  return Err(DurableError::UnsafePath)
 }
 Ok(())
}
fn secure(f:&File)->Result<(),DurableError>{
 let m=f.metadata().map_err(|_|DurableError::Io)?;
 if !m.is_file()||m.nlink()!=1||m.uid()!=unsafe{libc::geteuid()}
   ||m.mode()&0o077!=0||m.len()>MAX_FILE{
  return Err(DurableError::UnsafePath)
 }
 Ok(())
}
struct Guard(File);
impl Guard{
 fn acquire(path:&Path)->Result<Self,DurableError>{
  guard(path)?;
  let lock=path.with_extension("jsonl.lock");
  let f=OpenOptions::new().read(true).write(true).create(true)
   .mode(0o600).custom_flags(NOFOLLOW).open(lock)
   .map_err(|_|DurableError::Io)?;
  secure(&f)?;
  if unsafe{libc::flock(f.as_raw_fd(),libc::LOCK_EX)}!=0{
   return Err(DurableError::Io)
  }
  Ok(Self(f))
 }
}
impl Drop for Guard{
 fn drop(&mut self){unsafe{libc::flock(self.0.as_raw_fd(),libc::LOCK_UN);}}
}
fn read_entries(path:&Path,now:u64)->Result<Vec<Entry>,DurableError>{
 let mut f=OpenOptions::new().read(true).custom_flags(NOFOLLOW)
  .open(path).map_err(|_|DurableError::Io)?;
 secure(&f)?;
 let mut bytes=Vec::new();
 f.read_to_end(&mut bytes).map_err(|_|DurableError::Io)?;
 if !bytes.is_empty()&&bytes.last()!=Some(&b'\n'){return Err(DurableError::Corrupt)}
 let mut entries=Vec::new();
 let mut prior=ZERO.to_string();
 let mut anchor=None;
 let mut prior_seq=0u64;
 let mut generation=None::<String>;
 for line in bytes[..bytes.len().saturating_sub(1)].split(|&b|b==b'\n'){
  if bytes.is_empty(){break}
  if line.is_empty(){return Err(DurableError::Corrupt)}
  if entries.len()>=MAX_ITEMS{return Err(DurableError::Capacity)}
  let e:Entry=serde_json::from_slice(line).map_err(|_|DurableError::Corrupt)?;
  if e.version!=1||e.previous_hash!=prior||e.checksum.len()!=64{
   return Err(DurableError::Chain)
  }
  let origin=*anchor.get_or_insert(e.anchor_seq);
  if e.anchor_seq!=origin{return Err(DurableError::Chain)}
  let previous=if entries.is_empty(){origin}else{prior_seq};
  audit_outbox_chain(std::slice::from_ref(&e.row),previous,now)
   .map_err(|_|DurableError::Chain)?;
  if generation.as_deref().is_some_and(|v|v!=e.row.feed_generation){
   return Err(DurableError::Chain)
  }
  generation.get_or_insert_with(||e.row.feed_generation.clone());
  let expected=digest(&e.previous_hash,e.anchor_seq,&e.row)?;
  if e.checksum!=expected{return Err(DurableError::Chain)}
  prior=e.checksum.clone();prior_seq=e.row.seq;entries.push(e);
 }
 Ok(entries)
}
fn summary(e:&[Entry])->DurableReview{
 DurableReview{
  records:e.len(),last_seq:e.last().map_or(0,|x|x.row.seq),
  last_hash:e.last().map_or_else(||ZERO.into(),|x|x.checksum.clone()),
  generation:e.last().map(|x|x.row.feed_generation.clone()),
  can_apply_to_db2:false,may_trade:false,
 }
}
pub fn initialize_isolated(path:&Path)->Result<(),DurableError>{
 let _lock=Guard::acquire(path)?;
 let file=OpenOptions::new().write(true).create_new(true).mode(0o600)
  .custom_flags(NOFOLLOW).open(path).map_err(|_|DurableError::Conflict)?;
 secure(&file)?;
 file.sync_all().map_err(|_|DurableError::Io)?;
 let parent=path.parent().ok_or(DurableError::UnsafePath)?;
 File::open(parent).and_then(|f|f.sync_all()).map_err(|_|DurableError::Io)?;
 Ok(())
}
pub fn recover_isolated(path:&Path,now:u64)->Result<DurableReview,DurableError>{
 let _lock=Guard::acquire(path)?;
 Ok(summary(&read_entries(path,now)?))
}
pub fn append_source_row_isolated(
 path:&Path,row:MirrorOutboxRowR139,now:u64
)->Result<DurableReview,DurableError>{
 let _lock=Guard::acquire(path)?;
 let mut entries=read_entries(path,now)?;
 if entries.len()>=MAX_ITEMS{return Err(DurableError::Capacity)}
 let prior_seq=entries.last().map_or(row.prev_seq,|x|x.row.seq);
 audit_outbox_chain(std::slice::from_ref(&row),prior_seq,now)
  .map_err(|_|DurableError::Chain)?;
 if entries.iter().any(|e|e.row.tx_id==row.tx_id)
  ||entries.last().is_some_and(|e|e.row.feed_generation!=row.feed_generation){
  return Err(DurableError::Conflict)
 }
 let anchor=entries.first().map_or(row.prev_seq,|e|e.anchor_seq);
 let prev=entries.last().map_or(ZERO,|e|e.checksum.as_str()).to_string();
 let checksum=digest(&prev,anchor,&row)?;
 let next=Entry{version:1,anchor_seq:anchor,previous_hash:prev,row,checksum};
 let mut line=serde_json::to_vec(&next).map_err(|_|DurableError::Corrupt)?;
 line.push(b'\n');
 let mut f=OpenOptions::new().read(true).append(true).custom_flags(NOFOLLOW)
  .open(path).map_err(|_|DurableError::Io)?;
 secure(&f)?;
 if f.metadata().map_err(|_|DurableError::Io)?.len()
   .checked_add(line.len() as u64).map_or(true,|n|n>MAX_FILE){
  return Err(DurableError::Capacity)
 }
 f.write_all(&line).map_err(|_|DurableError::Io)?;
 f.sync_all().map_err(|_|DurableError::Io)?;
 entries.push(next);
 Ok(summary(&entries))
}
#[cfg(test)]
mod tests{
 use super::*;
 use std::{os::unix::fs::DirBuilderExt,
  sync::atomic::{AtomicUsize,Ordering},path::PathBuf};
 static N:AtomicUsize=AtomicUsize::new(0);
 const NOW:u64=1_791_784_000_000;
 fn setup()->(PathBuf,PathBuf){
  let root=std::env::temp_dir().join(format!(
   "astra_r143_isolated_{}_{}",std::process::id(),N.fetch_add(1,Ordering::Relaxed)));
  fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
  let path=root.join(NAME);
  (root,path)
 }
 fn row(seq:u64,prev:u64)->MirrorOutboxRowR139{
  let mut e:MirrorOutboxRowR139=serde_json::from_str(include_str!(
    "../config/native_mirror_outbox_python_fixture_r141_review.json")).unwrap();
  e.seq=seq;e.prev_seq=prev;
  e.tx_id=format!("tx-{seq:032x}");
  e
 }
 #[test]fn r143_init_append_and_cold_replay_three_source_rows(){
  let (root,path)=setup();
  initialize_isolated(&path).unwrap();
  assert_eq!(recover_isolated(&path,NOW).unwrap().records,0);
  for (seq,prev) in [(43,42),(100,43),(101,100)]{
   let report=append_source_row_isolated(&path,row(seq,prev),NOW).unwrap();
   assert_eq!(report.last_seq,seq);
   assert!(!report.can_apply_to_db2&&!report.may_trade);
  }
  let cold=recover_isolated(&path,NOW).unwrap();
  assert_eq!((cold.records,cold.last_seq),(3,101));
  assert_ne!(cold.last_hash,ZERO);
  fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r144_truncated_tail_blocks_recovery_and_next_append(){
  let (root,path)=setup();
  initialize_isolated(&path).unwrap();
  append_source_row_isolated(&path,row(43,42),NOW).unwrap();
  let mut bytes=fs::read(&path).unwrap();
  bytes.pop();
  fs::write(&path,&bytes).unwrap();
  assert_eq!(recover_isolated(&path,NOW),Err(DurableError::Corrupt));
  assert_eq!(append_source_row_isolated(&path,row(44,43),NOW),
   Err(DurableError::Corrupt));
  assert_eq!(fs::read(&path).unwrap(),bytes);
  fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r144_tampered_lineage_blocks_replay_and_append(){
  let (root,path)=setup();
  initialize_isolated(&path).unwrap();
  append_source_row_isolated(&path,row(43,42),NOW).unwrap();
  append_source_row_isolated(&path,row(44,43),NOW).unwrap();
  let mut rows=fs::read_to_string(&path).unwrap();
  rows=rows.replacen("\"previous_hash\":\"","\"previous_hash\":\"f",1);
  fs::write(&path,rows).unwrap();
  assert_eq!(recover_isolated(&path,NOW),Err(DurableError::Chain));
  assert_eq!(append_source_row_isolated(&path,row(45,44),NOW),
   Err(DurableError::Chain));
  fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r145_cold_replay_blocks_duplicate_and_missing_link(){
  let (root,path)=setup();
  initialize_isolated(&path).unwrap();
  append_source_row_isolated(&path,row(43,42),NOW).unwrap();
  assert_eq!(append_source_row_isolated(&path,row(43,42),NOW),
   Err(DurableError::Chain));
  assert_eq!(append_source_row_isolated(&path,row(45,44),NOW),
   Err(DurableError::Chain));
  let mut dup=row(45,43);dup.tx_id=row(43,42).tx_id;
  assert_eq!(append_source_row_isolated(&path,dup,NOW),
   Err(DurableError::Conflict));
  assert_eq!(recover_isolated(&path,NOW).unwrap().records,1);
  fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r145_disappearing_journal_is_not_new_clean_empty_state(){
  let (root,path)=setup();
  initialize_isolated(&path).unwrap();
  append_source_row_isolated(&path,row(43,42),NOW).unwrap();
  fs::remove_file(&path).unwrap();
  assert_eq!(recover_isolated(&path,NOW),Err(DurableError::Io));
  assert_eq!(append_source_row_isolated(&path,row(44,43),NOW),
   Err(DurableError::Io));
  fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r145_concurrent_writers_only_one_wins_source_sequence(){
  let (root,path)=setup();
  initialize_isolated(&path).unwrap();
  let a=path.clone();let b=path.clone();
  let x=std::thread::spawn(move||append_source_row_isolated(&a,row(43,42),NOW));
  let y=std::thread::spawn(move||append_source_row_isolated(&b,row(43,42),NOW));
  let results=[x.join().unwrap(),y.join().unwrap()];
  assert_eq!(results.iter().filter(|r|r.is_ok()).count(),1);
  assert_eq!(recover_isolated(&path,NOW).unwrap().records,1);
  fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r145_symlink_target_and_insecure_directory_are_rejected(){
  use std::os::unix::fs::{symlink,PermissionsExt};
  let (root,path)=setup();
  let wrong=root.join("arb_stats.sqlite");
  assert_eq!(initialize_isolated(&wrong),Err(DurableError::UnsafePath));
  let outside=root.join("witness.json");
  fs::write(&outside,"safeguard").unwrap();
  symlink(&outside,&path).unwrap();
  assert_eq!(initialize_isolated(&path),Err(DurableError::Conflict));
  assert_eq!(fs::read_to_string(&outside).unwrap(),"safeguard");
  fs::remove_file(&path).unwrap();
  fs::set_permissions(&root,fs::Permissions::from_mode(0o755)).unwrap();
  assert_eq!(initialize_isolated(&path),Err(DurableError::UnsafePath));
  fs::remove_dir_all(root).unwrap();
 }
}
