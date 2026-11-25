// R195: private signed persisted metadata for bounded modeled PAPER sessions.
// No actual orders, no production DB2, no remote service and no real fills.
#[derive(Debug,Clone,serde::Serialize,serde::Deserialize,PartialEq,Eq)]
#[serde(deny_unknown_fields)]
pub struct PersistedSessionR195{
 pub session:String,pub last_seq:u64,pub events:usize,
 pub digest:String,pub generation:String,
}
#[derive(Debug,Clone,serde::Serialize,serde::Deserialize,PartialEq,Eq)]
#[serde(deny_unknown_fields)]
pub struct QueueManifestR195{
 pub version:u32,pub revision:u64,pub accepted_session:Option<String>,
 pub sessions:Vec<PersistedSessionR195>,pub mac:String,
}
fn queue_guard_r195(path:&Path)->Result<(),PaperAdapterErrorR156>{
 use std::os::unix::fs::MetadataExt;
 if path.file_name().and_then(|n|n.to_str())!=Some("paper-session-queue.json"){
  return Err(PaperAdapterErrorR156::Path)
 }
 let parent=path.parent().ok_or(PaperAdapterErrorR156::Path)?;
 if parent.parent()!=Some(std::env::temp_dir().as_path())||
   !parent.file_name().and_then(|x|x.to_str()).is_some_and(
     |x|x.starts_with("astra_r195_isolated_")&&x.len()<=120){
  return Err(PaperAdapterErrorR156::Path)
 }
 let m=fs::symlink_metadata(parent).map_err(|_|PaperAdapterErrorR156::Path)?;
 if !m.file_type().is_dir()||m.mode()&0o077!=0||
    m.uid()!=unsafe{libc::geteuid()}{
  return Err(PaperAdapterErrorR156::Path)
 }
 Ok(())
}
fn queue_order_r195(s:&str)->Option<(u64,u64)>{
 let p:Vec<_>=s.split('_').collect();
 if p.len()!=3||p[0]!="session"{return None}
 Some((p[1].parse().ok()?,p[2].parse().ok()?))
}
fn queue_mac_r195(m:&QueueManifestR195,key:&[u8])
 ->Result<String,PaperAdapterErrorR156>{
 if key.len()<16{return Err(PaperAdapterErrorR156::Source)}
 let mut unsigned=m.clone();unsigned.mac.clear();
 let mut data=b"ASTRA-R195-PRIVATE-PAPER-QUEUE-V1\n".to_vec();
 data.extend(serde_json::to_vec(&unsigned).map_err(|_|PaperAdapterErrorR156::Source)?);
 Ok(hex_r154(&hmac_r154(key,&data)))
}
fn load_queue_r195(path:&Path,key:&[u8])
 ->Result<QueueManifestR195,PaperAdapterErrorR156>{
 use std::{io::Read,os::unix::fs::{MetadataExt,OpenOptionsExt}};
 queue_guard_r195(path)?;
 // R212: a leftover publication stage is unresolved; a read is never
 // permission to decide whether it can be discarded.
 if fs::symlink_metadata(path.with_extension("tmp")).is_ok(){
  return Err(PaperAdapterErrorR156::Source)
 }
 // FD and pathname must refer to one unchanged ordinary private inode.
 // The previous metadata(path) + fs::read(path) allowed a path swap.
 let mut file=fs::OpenOptions::new().read(true)
  .custom_flags(libc::O_NOFOLLOW|libc::O_CLOEXEC)
  .open(path).map_err(|_|PaperAdapterErrorR156::Path)?;
 let meta=file.metadata().map_err(|_|PaperAdapterErrorR156::Path)?;
 if !meta.file_type().is_file()||meta.nlink()!=1||
  meta.mode()&0o077!=0||meta.uid()!=unsafe{libc::geteuid()}
  ||meta.len()==0||meta.len()>32768{
  return Err(PaperAdapterErrorR156::Path)
 }
 let mut raw=Vec::with_capacity(meta.len() as usize);
 Read::by_ref(&mut file).take(32769).read_to_end(&mut raw)
  .map_err(|_|PaperAdapterErrorR156::Path)?;
 if raw.len() as u64!=meta.len(){return Err(PaperAdapterErrorR156::Source)}
 let current=fs::symlink_metadata(path).map_err(|_|PaperAdapterErrorR156::Path)?;
 if !current.file_type().is_file()||current.dev()!=meta.dev()
  ||current.ino()!=meta.ino()||current.nlink()!=1
  ||current.len()!=meta.len()||current.mode()&0o077!=0
  ||current.uid()!=meta.uid()||fs::symlink_metadata(path.with_extension("tmp")).is_ok(){
  return Err(PaperAdapterErrorR156::Source)
 }
 let m:QueueManifestR195=serde_json::from_slice(&raw)
  .map_err(|_|PaperAdapterErrorR156::Source)?;
 let expected=queue_mac_r195(&m,key)?;
 // A valid HMAC means "signed locally", not "structurally sound".
 // Never sum untrusted usize counters with unchecked arithmetic and
 // never accept an unparseable acknowledged session even for an empty queue.
 let total=m.sessions.iter().try_fold(0usize,|sum,item|
    sum.checked_add(item.events)).ok_or(PaperAdapterErrorR156::Source)?;
 if m.version!=1||m.revision>1_000_000
  ||((!m.sessions.is_empty()||m.accepted_session.is_some())&&m.revision==0)
  ||m.mac.len()!=expected.len()||
   m.mac.bytes().zip(expected.bytes()).fold(0u8,|a,(x,y)|a|(x^y))!=0
  ||m.sessions.len()>4||total>256
  ||m.accepted_session.as_deref().is_some_and(|s|
    queue_order_r195(s).is_none()){
  return Err(PaperAdapterErrorR156::Source)
 }
 let mut last=m.accepted_session.as_deref();
 for item in &m.sessions{
  let order=queue_order_r195(&item.session).ok_or(PaperAdapterErrorR156::Source)?;
  if item.events==0||item.events>256||item.last_seq!=42+item.events as u64
    ||!valid_hex_r156(&item.digest)
    ||!item.generation.starts_with("rebase-"){
   return Err(PaperAdapterErrorR156::Source)
  }
  if let Some(old)=last{
   if order<=queue_order_r195(old).ok_or(PaperAdapterErrorR156::Source)?{
    return Err(PaperAdapterErrorR156::Source)
   }
  }
  last=Some(&item.session);
 }
 Ok(m)
}
fn save_queue_r195(path:&Path,mut m:QueueManifestR195,key:&[u8])
 ->Result<(),PaperAdapterErrorR156>{
 use std::{io::Write,os::unix::fs::OpenOptionsExt};
 queue_guard_r195(path)?;
 m.mac=queue_mac_r195(&m,key)?;
 let bytes=serde_json::to_vec(&m).map_err(|_|PaperAdapterErrorR156::Source)?;
 if bytes.len()>32768{return Err(PaperAdapterErrorR156::Limit)}
 let tmp=path.with_extension("tmp");
 let mut f=fs::OpenOptions::new().write(true).create_new(true)
  .mode(0o600).custom_flags(libc::O_NOFOLLOW|libc::O_CLOEXEC)
  .open(&tmp).map_err(|_|PaperAdapterErrorR156::Path)?;
 f.write_all(&bytes).map_err(|_|PaperAdapterErrorR156::Path)?;
 f.sync_all().map_err(|_|PaperAdapterErrorR156::Path)?;
 drop(f);
 fs::rename(tmp,path).map_err(|_|PaperAdapterErrorR156::Path)?;
 fs::File::open(path.parent().unwrap())
  .and_then(|dir|dir.sync_all()).map_err(|_|PaperAdapterErrorR156::Path)?;
 Ok(())
}
// R213: do not remove a foreign replacement .lock at drop. The original
// private inode is held open for the entire local transaction. This lock
// is same-host mutual exclusion, NOT an independently authenticated floor.
struct QueueLockR195{path:PathBuf,file:fs::File}
impl Drop for QueueLockR195{
 fn drop(&mut self){
  use std::os::unix::fs::MetadataExt;
  if let (Ok(current),Ok(mine))=(
    fs::symlink_metadata(&self.path),self.file.metadata()
  ){
   if current.file_type().is_file()&&current.dev()==mine.dev()
     &&current.ino()==mine.ino()&&current.nlink()==1
     &&current.mode()&0o077==0{
    let _=fs::remove_file(&self.path);
   }
  }
 }
}
fn queue_lock_r195(path:&Path)->Result<QueueLockR195,PaperAdapterErrorR156>{
 use std::os::unix::fs::{MetadataExt,OpenOptionsExt};
 queue_guard_r195(path)?;
 let lock=path.with_extension("lock");
 let f=fs::OpenOptions::new().create_new(true).write(true)
  .mode(0o600).custom_flags(libc::O_NOFOLLOW|libc::O_CLOEXEC)
  .open(&lock).map_err(|_|PaperAdapterErrorR156::Source)?;
 f.sync_all().map_err(|_|PaperAdapterErrorR156::Source)?;
 let mine=f.metadata().map_err(|_|PaperAdapterErrorR156::Source)?;
 let current=fs::symlink_metadata(&lock).map_err(|_|PaperAdapterErrorR156::Source)?;
 if !current.file_type().is_file()||current.dev()!=mine.dev()
  ||current.ino()!=mine.ino()||current.nlink()!=1
  ||current.mode()&0o077!=0||current.uid()!=unsafe{libc::geteuid()}
  ||current.len()!=0{
  return Err(PaperAdapterErrorR156::Source)
 }
 Ok(QueueLockR195{path:lock,file:f})
}
pub fn initialize_queue_r195(path:&Path,key:&[u8])
 ->Result<(),PaperAdapterErrorR156>{
 let _guard=queue_lock_r195(path)?;
 // exists() silently follows broken symlinks, treating a symlink with
 // missing target as absent. Deny ALL existing path types at initialization.
 match fs::symlink_metadata(path){
  Ok(_)=>return Err(PaperAdapterErrorR156::Source),
  Err(e) if e.kind()==std::io::ErrorKind::NotFound=>{},
  Err(_)=>return Err(PaperAdapterErrorR156::Path)
 }
 save_queue_r195(path,QueueManifestR195{
  version:1,revision:0,accepted_session:None,sessions:vec![],mac:String::new()
 },key)
}
pub fn enqueue_private_queue_r195(path:&Path,p:&PaperProjectionR156,
 now:u64,key:&[u8])->Result<(),PaperAdapterErrorR156>{
 verify_projection_r162(p,now)?;
 if model_stage_r181(p.rows.first().unwrap())?!="SESSION_START"
    ||model_stage_r181(p.rows.last().unwrap())?!="SESSION_SEAL"{
  return Err(PaperAdapterErrorR156::Model)
 }
 let _guard=queue_lock_r195(path)?;
 let mut m=load_queue_r195(path,key)?;
 let n=p.rows.len();
 if m.sessions.len()>=4||m.sessions.iter().map(|x|x.events).sum::<usize>()+n>256{
  return Err(PaperAdapterErrorR156::Limit)
 }
 let prev=m.sessions.last().map(|x|x.session.as_str())
  .or(m.accepted_session.as_deref());
 let next=queue_order_r195(&p.session).ok_or(PaperAdapterErrorR156::Source)?;
 if prev.is_some_and(|prior|
   queue_order_r195(prior).map_or(true,|o|next<=o)){
  return Err(PaperAdapterErrorR156::Source)
 }
 m.revision=m.revision.checked_add(1).ok_or(PaperAdapterErrorR156::Limit)?;
 m.sessions.push(PersistedSessionR195{
  session:p.session.clone(),last_seq:p.rows.last().unwrap().seq,
  events:n,digest:hex_r154(&p.seal_r162),
  generation:p.rows.last().unwrap().feed_generation.clone(),
 });
 save_queue_r195(path,m,key)
}
pub fn private_queue_status_r195(path:&Path,key:&[u8])
 ->Result<(usize,usize),PaperAdapterErrorR156>{
 let m=load_queue_r195(path,key)?;
 Ok((m.sessions.len(),m.sessions.iter().map(|x|x.events).sum()))
}
pub fn acknowledge_private_queue_r195(path:&Path,p:&PaperProjectionR156,
 now:u64,key:&[u8],floor:&DetachedWitnessR154,
 remote:&IndependentReceiptR177,authority:&str,nonce:&str,remote_key:&[u8],
 operator_approved:bool)->Result<(),PaperAdapterErrorR156>{
 if !operator_approved{return Err(PaperAdapterErrorR156::Source)}
 verify_projection_r162(p,now)?;
 let _guard=queue_lock_r195(path)?;
 let mut m=load_queue_r195(path,key)?;
 let first=m.sessions.first().ok_or(PaperAdapterErrorR156::Source)?;
 if first.session!=p.session||first.events!=p.rows.len()
  ||first.digest!=hex_r154(&p.seal_r162)
  ||first.last_seq!=p.rows.last().unwrap().seq
  ||first.generation!=p.rows.last().unwrap().feed_generation
  ||floor.seq!=first.last_seq||floor.generation!=first.generation{
  return Err(PaperAdapterErrorR156::Source)
 }
 verify_remote_receipt_r177(remote,&floor.owner,authority,nonce,remote_key,floor)?;
 m.revision=m.revision.checked_add(1).ok_or(PaperAdapterErrorR156::Limit)?;
 m.accepted_session=Some(first.session.clone());
 m.sessions.remove(0);
 save_queue_r195(path,m,key)
}
#[cfg(test)]
mod r195_tests{
 use super::*;
 use std::{os::unix::fs::DirBuilderExt,sync::atomic::{AtomicUsize,Ordering}};
 static N:AtomicUsize=AtomicUsize::new(0);
 const NOW:u64=1_792_280_000_000;
 const KEY:&[u8]=b"r195-queue-private-synthetic-key-123456";
 const REMOTE:&[u8]=b"r195-second-domain-mock-signing-key-123";
 const NONCE:&str="cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
 fn path()->(PathBuf,PathBuf){
  let root=std::env::temp_dir().join(format!("astra_r195_isolated_{}_{}",
   std::process::id(),N.fetch_add(1,Ordering::Relaxed)));
  fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
  let p=root.join("paper-session-queue.json");
  (root,p)
 }
 fn project(n:usize)->PaperProjectionR156{
  r181_rollover_tests::project(&format!("session_{n}_1792253053"),true)
 }
 fn evidence(p:&PaperProjectionR156)->(DetachedWitnessR154,IndependentReceiptR177){
  let row=p.rows.last().unwrap();
  let w=DetachedWitnessR154{
   owner:"queue-research-owner".into(),generation:row.feed_generation.clone(),
   anchor:42,seq:row.seq,digest:"b".repeat(64),mac:"e".repeat(64)
  };
  let mut r=IndependentReceiptR177{
   authority:"synthetic-remote".into(),owner:w.owner.clone(),
   challenge:NONCE.into(),generation:w.generation.clone(),
   anchor:w.anchor,seq:w.seq,digest:w.digest.clone(),mac:String::new()
  };
  r.mac=hex_r154(&hmac_r154(REMOTE,&independent_message_r177(&r)));
  (w,r)
 }
 #[test]fn r195_queue_cold_reopen_ack_and_replay_resistance(){
  let (root,path)=path();initialize_queue_r195(&path,KEY).unwrap();
  let first=project(200);let second=project(201);
  enqueue_private_queue_r195(&path,&first,NOW,KEY).unwrap();
  enqueue_private_queue_r195(&path,&second,NOW,KEY).unwrap();
  assert_eq!(private_queue_status_r195(&path,KEY).unwrap(),(2,8));
  assert!(enqueue_private_queue_r195(&path,&first,NOW,KEY).is_err());
  let (w,r)=evidence(&first);
  assert!(acknowledge_private_queue_r195(&path,&first,NOW,KEY,
    &w,&r,"synthetic-remote",NONCE,REMOTE,false).is_err());
  assert_eq!(private_queue_status_r195(&path,KEY).unwrap(),(2,8));
  acknowledge_private_queue_r195(&path,&first,NOW,KEY,
    &w,&r,"synthetic-remote",NONCE,REMOTE,true).unwrap();
  assert_eq!(private_queue_status_r195(&path,KEY).unwrap(),(1,4));
  assert!(enqueue_private_queue_r195(&path,&first,NOW,KEY).is_err());
  assert!(acknowledge_private_queue_r195(&path,&first,NOW,KEY,
    &w,&r,"synthetic-remote",NONCE,REMOTE,true).is_err());
  fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r195_tampered_file_or_leftover_tmp_denied(){
  let (root,path)=path();initialize_queue_r195(&path,KEY).unwrap();
  enqueue_private_queue_r195(&path,&project(200),NOW,KEY).unwrap();
  let backup=fs::read(&path).unwrap();
  let mut tampered:serde_json::Value=serde_json::from_slice(&backup).unwrap();
  tampered["sessions"][0]["last_seq"]=serde_json::json!(999);
  fs::write(&path,serde_json::to_vec(&tampered).unwrap()).unwrap();
  assert!(private_queue_status_r195(&path,KEY).is_err());
  fs::write(&path,backup).unwrap();
  assert_eq!(private_queue_status_r195(&path,KEY).unwrap(),(1,4));
  fs::write(path.with_extension("tmp"),b"orphan").unwrap();
  assert!(private_queue_status_r195(&path,KEY).is_err());
  fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r212_dangling_queue_symlink_is_not_new_empty_queue(){
  use std::os::unix::fs::symlink;
  let (root,path)=path();let missing=root.join("missing.json");
  symlink(&missing,&path).unwrap();
  assert!(!path.exists(),"prior implementation wrongly saw this as absent");
  assert!(initialize_queue_r195(&path,KEY).is_err());
  assert!(fs::symlink_metadata(&path).unwrap().file_type().is_symlink());
  assert!(private_queue_status_r195(&path,KEY).is_err());
  fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r213_queue_read_rejects_symlink_hardlink_and_insecure_mode(){
  use std::os::unix::fs::{symlink,PermissionsExt};
  let (root,path)=path();initialize_queue_r195(&path,KEY).unwrap();
  let second=root.join("second.json");
  fs::hard_link(&path,&second).unwrap();
  assert!(private_queue_status_r195(&path,KEY).is_err());
  fs::remove_file(&second).unwrap();
  fs::set_permissions(&path,fs::Permissions::from_mode(0o644)).unwrap();
  assert!(private_queue_status_r195(&path,KEY).is_err());
  fs::set_permissions(&path,fs::Permissions::from_mode(0o600)).unwrap();
  assert_eq!(private_queue_status_r195(&path,KEY).unwrap(),(0,0));
  fs::rename(&path,&second).unwrap();
  symlink(&second,&path).unwrap();
  assert!(private_queue_status_r195(&path,KEY).is_err());
  assert_eq!(fs::read(&second).is_ok(),true);
  fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r214_original_queue_lock_drop_does_not_unlink_foreign_replacement(){
  let (root,path)=path();
  let guard=queue_lock_r195(&path).unwrap();
  let lock=path.with_extension("lock");
  let shifted=root.join("old-lock");
  fs::rename(&lock,&shifted).unwrap();
  fs::write(&lock,b"foreign-lock").unwrap();
  drop(guard);
  assert_eq!(fs::read(&lock).unwrap(),b"foreign-lock");
  assert!(initialize_queue_r195(&path,KEY).is_err());
  fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r215_dangling_publication_stage_and_zero_bytes_fail_closed(){
  use std::os::unix::fs::symlink;
  let (root,path)=path();initialize_queue_r195(&path,KEY).unwrap();
  let stage=path.with_extension("tmp");
  let absent=root.join("not-existing");
  symlink(&absent,&stage).unwrap();
  assert!(!stage.exists(),"exists() must not be an orphan detector");
  assert!(private_queue_status_r195(&path,KEY).is_err());
  fs::remove_file(&stage).unwrap();
  fs::write(&path,b"").unwrap();
  assert!(private_queue_status_r195(&path,KEY).is_err());
  fs::remove_dir_all(root).unwrap();
 }
 fn write_local_hmac_valid(path:&Path,mut m:QueueManifestR195){
  m.mac=queue_mac_r195(&m,KEY).unwrap();
  fs::write(path,serde_json::to_vec(&m).unwrap()).unwrap();
 }
 #[test]fn r216_locally_signed_counter_overflow_denied_not_panicked(){
  let (root,path)=path();initialize_queue_r195(&path,KEY).unwrap();
  let mut m=load_queue_r195(&path,KEY).unwrap();m.revision=1;
  m.sessions=vec![
    PersistedSessionR195{
      session:"session_200_1792253053".into(),last_seq:43,
      events:usize::MAX,digest:"a".repeat(64),generation:"rebase-foo".into()
    },
    PersistedSessionR195{
      session:"session_201_1792253053".into(),last_seq:44,
      events:2,digest:"b".repeat(64),generation:"rebase-bar".into()
    }
  ];
  write_local_hmac_valid(&path,m);
  assert!(private_queue_status_r195(&path,KEY).is_err());
  assert!(load_queue_r195(&path,KEY).is_err());
  fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r217_locally_signed_malformed_accepted_session_denied_even_if_empty(){
  let (root,path)=path();initialize_queue_r195(&path,KEY).unwrap();
  let mut m=load_queue_r195(&path,KEY).unwrap();m.revision=1;
  m.accepted_session=Some("operator_claimed_flat".into());
  write_local_hmac_valid(&path,m);
  assert!(private_queue_status_r195(&path,KEY).is_err());
  assert!(enqueue_private_queue_r195(&path,&project(200),NOW,KEY).is_err());
  fs::remove_dir_all(root).unwrap();
 }
 #[test]fn r218_ordered_acknowledged_session_still_allows_newer_only(){
  let (root,path)=path();initialize_queue_r195(&path,KEY).unwrap();
  let mut m=load_queue_r195(&path,KEY).unwrap();m.revision=1;
  m.accepted_session=Some("session_200_1792253053".into());
  write_local_hmac_valid(&path,m);
  assert_eq!(private_queue_status_r195(&path,KEY).unwrap(),(0,0));
  assert!(enqueue_private_queue_r195(&path,&project(200),NOW,KEY).is_err());
  enqueue_private_queue_r195(&path,&project(201),NOW,KEY).unwrap();
  assert_eq!(private_queue_status_r195(&path,KEY).unwrap(),(1,4));
  fs::remove_dir_all(root).unwrap();
 }
}
