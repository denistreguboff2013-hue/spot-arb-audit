// R171–R176: research-only LOCAL witness manifest, outside both isolated SQLite files.
// Same host/disk is NOT a truly independent witness service. No trading authority.
#[derive(Debug,Clone,serde::Serialize,serde::Deserialize,PartialEq,Eq)]
#[serde(deny_unknown_fields)]
pub struct ExternalPendingR171{
 pub expected_previous:u64,pub target_seq:u64,pub session:String,pub projection_hash:String,
}
#[derive(Debug,Clone,serde::Serialize,serde::Deserialize,PartialEq,Eq)]
#[serde(deny_unknown_fields)]
pub struct ExternalFloorR171{
 pub version:u32,pub owner:String,pub witness:Option<DetachedWitnessR154>,
 pub pending:Option<ExternalPendingR171>,pub mac:String,
}
#[derive(Debug,Clone,Copy,PartialEq,Eq)]
pub enum FloorReviewR173{Clean,Prepared,CommittedUnpublished,Partial,Reject}
fn floor_mac_r171(f:&ExternalFloorR171,key:&[u8])->Result<String,PaperAdapterErrorR156>{
 if key.len()<16{return Err(PaperAdapterErrorR156::Source)}
 let mut unsigned=f.clone();unsigned.mac.clear();
 let bytes=serde_json::to_vec(&unsigned).map_err(|_|PaperAdapterErrorR156::Source)?;
 let mut domain=b"ASTRA-EXTERNAL-FLOOR-R171-V1\n".to_vec();
 domain.extend_from_slice(&bytes);
 Ok(hex_r154(&hmac_r154(key,&domain)))
}
fn floor_guard_r171(path:&Path)->Result<(),PaperAdapterErrorR156>{
 use std::os::unix::fs::MetadataExt;
 if path.file_name().and_then(|x|x.to_str())!=Some("external-witness-floor.json"){
  return Err(PaperAdapterErrorR156::Path)
 }
 let parent=path.parent().ok_or(PaperAdapterErrorR156::Path)?;
 if parent.parent()!=Some(std::env::temp_dir().as_path())
    ||!parent.file_name().and_then(|x|x.to_str()).is_some_and(
      |x|x.starts_with("astra_r171_isolated_")&&x.len()<=120){
  return Err(PaperAdapterErrorR156::Path)
 }
 let m=fs::symlink_metadata(parent).map_err(|_|PaperAdapterErrorR156::Path)?;
 if !m.file_type().is_dir()||m.mode()&0o077!=0
    ||m.uid()!=unsafe{libc::geteuid()}{
  return Err(PaperAdapterErrorR156::Path)
 }
 Ok(())
}
fn load_floor_r171(path:&Path,key:&[u8])->Result<ExternalFloorR171,PaperAdapterErrorR156>{
 use std::os::unix::fs::MetadataExt;
 floor_guard_r171(path)?;
 if path.with_extension("tmp").exists(){return Err(PaperAdapterErrorR156::Source)}
 let m=fs::symlink_metadata(path).map_err(|_|PaperAdapterErrorR156::Path)?;
 if !m.file_type().is_file()||m.mode()&0o077!=0
    ||m.uid()!=unsafe{libc::geteuid()}||m.nlink()!=1||m.len()>8192{
  return Err(PaperAdapterErrorR156::Path)
 }
 let raw=fs::read(path).map_err(|_|PaperAdapterErrorR156::Path)?;
 let f:ExternalFloorR171=serde_json::from_slice(&raw).map_err(|_|PaperAdapterErrorR156::Source)?;
 if f.version!=1||reject_identity_r154(&f.owner)||f.mac.len()!=64
    ||f.witness.as_ref().is_some_and(|w|w.owner!=f.owner){
  return Err(PaperAdapterErrorR156::Source)
 }
 let actual=floor_mac_r171(&f,key)?;
 let diff=actual.bytes().zip(f.mac.bytes()).fold(0u8,|v,(a,b)|v|(a^b));
 if diff!=0{return Err(PaperAdapterErrorR156::Source)}
 Ok(f)
}
fn publish_floor_r171(path:&Path,mut f:ExternalFloorR171,key:&[u8])
 ->Result<(),PaperAdapterErrorR156>{
 use std::{io::Write,os::unix::fs::OpenOptionsExt};
 floor_guard_r171(path)?;
 f.mac=floor_mac_r171(&f,key)?;
 let bytes=serde_json::to_vec(&f).map_err(|_|PaperAdapterErrorR156::Source)?;
 if bytes.len()>8192{return Err(PaperAdapterErrorR156::Limit)}
 let tmp=path.with_extension("tmp");
 let mut out=fs::OpenOptions::new().write(true).create_new(true)
  .mode(0o600).open(&tmp).map_err(|_|PaperAdapterErrorR156::Path)?;
 out.write_all(&bytes).map_err(|_|PaperAdapterErrorR156::Path)?;
 out.sync_all().map_err(|_|PaperAdapterErrorR156::Path)?;
 drop(out);
 fs::rename(&tmp,path).map_err(|_|PaperAdapterErrorR156::Path)?;
 let dir=fs::File::open(path.parent().unwrap()).map_err(|_|PaperAdapterErrorR156::Path)?;
 dir.sync_all().map_err(|_|PaperAdapterErrorR156::Path)?;
 Ok(())
}
struct FloorLockR171(std::path::PathBuf);
impl Drop for FloorLockR171{
 fn drop(&mut self){let _=fs::remove_file(&self.0);}
}
fn acquire_floor_lock_r171(path:&Path)->Result<FloorLockR171,PaperAdapterErrorR156>{
 use std::os::unix::fs::OpenOptionsExt;
 floor_guard_r171(path)?;
 let lock=path.with_extension("lock");
 let f=fs::OpenOptions::new().write(true).create_new(true).mode(0o600)
  .open(&lock).map_err(|_|PaperAdapterErrorR156::Source)?;
 f.sync_all().map_err(|_|PaperAdapterErrorR156::Source)?;
 Ok(FloorLockR171(lock))
}
impl SqliteMirrorReview{
 pub fn initialize_local_floor_r171(&self,path:&Path,now:u64,
  owner:&str,key:&[u8])->Result<(),PaperAdapterErrorR156>{
  let _lock=acquire_floor_lock_r171(path)?;
  if path.exists()||reject_identity_r154(owner)||key.len()<16{
   return Err(PaperAdapterErrorR156::Source)
  }
  let (seq,n)=self.review_integrity_r152(now).map_err(|_|PaperAdapterErrorR156::Mirror)?;
  if seq!=0||n!=0{return Err(PaperAdapterErrorR156::Source)}
  publish_floor_r171(path,ExternalFloorR171{
   version:1,owner:owner.into(),witness:None,pending:None,mac:String::new()
  },key)
 }
 pub fn inspect_local_floor_r173(&self,path:&Path,now:u64,
   owner:&str,key:&[u8])->Result<FloorReviewR173,PaperAdapterErrorR156>{
  let f=load_floor_r171(path,key)?;
  if f.owner!=owner{return Err(PaperAdapterErrorR156::Source)}
  let (seq,n)=self.review_integrity_r152(now).map_err(|_|PaperAdapterErrorR156::Mirror)?;
  let old=f.witness.as_ref().map_or(0,|w|w.seq);
  if (old==0&&f.witness.is_some())||(old!=0&&f.witness.is_none())
    ||(n==0&&old!=0){return Ok(FloorReviewR173::Reject)}
  if let Some(w)=&f.witness{
   if self.verify_detached_witness_r154(now,w,owner,key).is_err(){
    return Ok(FloorReviewR173::Reject)
   }
  }
  let state=match &f.pending{
   None=>if seq==old{FloorReviewR173::Clean}else{FloorReviewR173::Reject},
   Some(p)=>{
    if p.expected_previous!=old||p.target_seq<=old
      ||(if old==0{p.target_seq.saturating_sub(42)}else{p.target_seq-old})>256
      ||p.session.is_empty()
      ||!valid_hex_r156(&p.projection_hash){
     FloorReviewR173::Reject
    }else if seq==old{FloorReviewR173::Prepared}
    else if seq==p.target_seq{FloorReviewR173::CommittedUnpublished}
    else if seq>old&&seq<p.target_seq{FloorReviewR173::Partial}
    else{FloorReviewR173::Reject}
   }
  };
  Ok(state)
 }
}
impl SqliteMirrorReview{
 pub fn replay_with_local_floor_r172(&self,projection:&PaperProjectionR156,
  path:&Path,now:u64,batch_size:usize,owner:&str,key:&[u8],fault:Option<&str>)
  ->Result<PaperReplayR157,PaperAdapterErrorR156>{
  if batch_size==0||batch_size>16
    ||fault.is_some_and(|f|!matches!(f,"after_intent"|"after_first_batch"
      |"after_db_commit"|"after_publish"|"sigkill_after_intent"
      |"sigkill_after_first_batch"|"sigkill_after_db_commit"
      |"sigkill_after_publish")){
   return Err(PaperAdapterErrorR156::Source)
  }
  if matches!(fault,Some("after_first_batch"|"sigkill_after_first_batch"))
    &&projection.rows.len()<2{
   return Err(PaperAdapterErrorR156::Source)
  }
  let _lock=acquire_floor_lock_r171(path)?;
  verify_projection_r162(projection,now)?;
  if self.inspect_local_floor_r173(path,now,owner,key)?!=FloorReviewR173::Clean{
   return Err(PaperAdapterErrorR156::Source)
  }
  let mut f=load_floor_r171(path,key)?;
  let current=f.witness.as_ref().map_or(0,|w|w.seq);
  let target=projection.rows.last().ok_or(PaperAdapterErrorR156::Source)?.seq;
  if target<=current
    ||(if current==0{target.saturating_sub(42)}else{target-current})>256{
   return Err(PaperAdapterErrorR156::Source)
  }
  f.pending=Some(ExternalPendingR171{
   expected_previous:current,target_seq:target,session:projection.session.clone(),
   projection_hash:hex_r154(&projection.seal_r162),
  });
  publish_floor_r171(path,f.clone(),key)?;
  r178_sigkill_at(fault,"sigkill_after_intent")?;
  if fault==Some("after_intent"){return Err(PaperAdapterErrorR156::Mirror)}
  if matches!(fault,Some("after_first_batch"|"sigkill_after_first_batch")){
   let amount=batch_size.min(projection.rows.len()-1);
   let batch=&projection.rows[..amount];
   for row in batch{
    match self.original_row(row.seq,now){
     Ok(existing)=>{
      if serde_json::to_vec(&existing).ok()!=serde_json::to_vec(row).ok(){
       return Err(PaperAdapterErrorR156::Source)
      }
     },
     Err(MirrorDbError::Source)=>{
      self.append_synthetic_source(row.clone(),now)
       .map_err(|_|PaperAdapterErrorR156::Source)?;
     },
     Err(_)=>return Err(PaperAdapterErrorR156::Source),
    }
   }
   self.apply_batch_r160(batch,now).map_err(|_|PaperAdapterErrorR156::Mirror)?;
   r178_sigkill_at(fault,"sigkill_after_first_batch")?;
   return Err(PaperAdapterErrorR156::Mirror)
  }
  let reply=self.replay_modeled_paper_with_floor_r164(projection,now,batch_size,
    owner,key,f.witness.as_ref())?;
  if reply.replay.final_seq!=target{return Err(PaperAdapterErrorR156::Mirror)}
  r178_sigkill_at(fault,"sigkill_after_db_commit")?;
  if fault==Some("after_db_commit"){return Err(PaperAdapterErrorR156::Mirror)}
  f.witness=Some(reply.next_witness);
  f.pending=None;
  publish_floor_r171(path,f,key)?;
  r178_sigkill_at(fault,"sigkill_after_publish")?;
  if fault==Some("after_publish"){return Err(PaperAdapterErrorR156::Mirror)}
  Ok(reply.replay)
 }
}
impl SqliteMirrorReview{
 pub fn cancel_unapplied_intent_r173(&self,path:&Path,now:u64,
  owner:&str,key:&[u8])->Result<(),PaperAdapterErrorR156>{
  let _lock=acquire_floor_lock_r171(path)?;
  if self.inspect_local_floor_r173(path,now,owner,key)?!=FloorReviewR173::Prepared{
   return Err(PaperAdapterErrorR156::Source)
  }
  let mut f=load_floor_r171(path,key)?;
  f.pending=None;
  publish_floor_r171(path,f,key)
 }
 pub fn publish_committed_intent_r173(&self,projection:&PaperProjectionR156,
  path:&Path,now:u64,owner:&str,key:&[u8])
  ->Result<DetachedWitnessR154,PaperAdapterErrorR156>{
  let _lock=acquire_floor_lock_r171(path)?;
  verify_projection_r162(projection,now)?;
  if self.inspect_local_floor_r173(path,now,owner,key)?
    !=FloorReviewR173::CommittedUnpublished{
   return Err(PaperAdapterErrorR156::Source)
  }
  let mut f=load_floor_r171(path,key)?;
  let pending=f.pending.as_ref().ok_or(PaperAdapterErrorR156::Source)?;
  if pending.target_seq!=projection.rows.last().unwrap().seq
    ||pending.session!=projection.session
    ||pending.projection_hash!=hex_r154(&projection.seal_r162){
   return Err(PaperAdapterErrorR156::Source)
  }
  for row in &projection.rows{
   let original=self.original_row(row.seq,now).map_err(|_|PaperAdapterErrorR156::Source)?;
   let left=serde_json::to_vec(&original).map_err(|_|PaperAdapterErrorR156::Source)?;
   let right=serde_json::to_vec(row).map_err(|_|PaperAdapterErrorR156::Source)?;
   if left!=right{return Err(PaperAdapterErrorR156::Source)}
  }
  let witness=self.create_detached_witness_r154(now,owner,key)
    .map_err(|_|PaperAdapterErrorR156::Mirror)?;
  if witness.seq!=pending.target_seq{return Err(PaperAdapterErrorR156::Mirror)}
  f.witness=Some(witness.clone());
  f.pending=None;
  publish_floor_r171(path,f,key)?;
  Ok(witness)
 }
}
// R174: each synthetic PAPER session receives an independent PRIVATE database root.
// Does not migrate or attach any production journal or actual DB2.
pub fn isolated_session_root_r174(session:&str)->Result<PathBuf,PaperAdapterErrorR156>{
 if session.len()<12||session.len()>100||!session.starts_with("session_")
    ||!session.bytes().all(|b|b.is_ascii_alphanumeric()||b==b'_'){
  return Err(PaperAdapterErrorR156::Path)
 }
 let hash=hex_r154(&openssl::sha::sha256(session.as_bytes()));
 Ok(std::env::temp_dir().join(format!("astra_r147_isolated_r174_{}",&hash[..24])))
}
// R176: pure human-facing incident classifier. No Telegram credentials/network.
pub fn floor_operator_alert_r176(s:FloorReviewR173)->&'static str{
 match s{
  FloorReviewR173::Clean=>"OK: snapshot and detached floor match; modeled PAPER only",
  FloorReviewR173::Prepared=>"ALERT: external intent written; DB2 not advanced; operator reconciliation",
  FloorReviewR173::CommittedUnpublished=>
    "CRITICAL: DB2 committed but external floor unpublished; deny replay until verified",
  FloorReviewR173::Partial=>
    "CRITICAL: partial DB2 progress under pending intent; halt and reconcile",
  FloorReviewR173::Reject=>
    "CRITICAL: inconsistent or unverified witness; halt, do not trade",
 }
}

#[cfg(test)]
mod r171_r176_tests{
 use super::*;
 use std::{os::unix::fs::DirBuilderExt,sync::atomic::{AtomicUsize,Ordering}};
 static N:AtomicUsize=AtomicUsize::new(0);
 const NOW:u64=1_792_280_000_000;
 const KEY:&[u8]=b"isolated-only-synthetic-external-floor-key-171";
 const OWNER:&str="research-171-owner";
 fn setup()->(PathBuf,PathBuf,SqliteMirrorReview){
  let unique=format!("{}_{}",std::process::id(),N.fetch_add(1,Ordering::Relaxed));
  let dbdir=std::env::temp_dir().join(format!("astra_r147_isolated_r171_{unique}"));
  let witness_dir=std::env::temp_dir().join(format!("astra_r171_isolated_{unique}"));
  fs::DirBuilder::new().mode(0o700).create(&dbdir).unwrap();
  fs::DirBuilder::new().mode(0o700).create(&witness_dir).unwrap();
  let db=SqliteMirrorReview::initialize(&dbdir).unwrap();
  (dbdir,witness_dir.join("external-witness-floor.json"),db)
 }
 fn done(root:&Path,floor:&Path){
  fs::remove_dir_all(root).unwrap();
  fs::remove_dir_all(floor.parent().unwrap()).unwrap();
 }
 fn p(n:usize)->PaperProjectionR156{r156_r157_tests::projection(n)}
 #[test]fn r171_initial_floor_is_signed_private_and_cold_readable(){
  let (root,path,db)=setup();
  db.initialize_local_floor_r171(&path,NOW,OWNER,KEY).unwrap();
  assert_eq!(db.inspect_local_floor_r173(&path,NOW,OWNER,KEY).unwrap(),
    FloorReviewR173::Clean);
  assert_eq!(db.initialize_local_floor_r171(&path,NOW,OWNER,KEY).err(),
    Some(PaperAdapterErrorR156::Source));
  assert!(load_floor_r171(&path,b"wrong-other-key-123456789").is_err());
  let cold=SqliteMirrorReview::load(&root).unwrap();
  assert_eq!(cold.inspect_local_floor_r173(&path,NOW,OWNER,KEY).unwrap(),
    FloorReviewR173::Clean);
  done(&root,&path);
 }
 #[test]fn r172_two_phase_publish_and_cold_idempotence(){
  let (root,path,db)=setup();
  db.initialize_local_floor_r171(&path,NOW,OWNER,KEY).unwrap();
  let a=db.replay_with_local_floor_r172(&p(4),&path,NOW,2,OWNER,KEY,None).unwrap();
  assert_eq!((a.newly_applied,a.final_seq),(4,46));
  let cold=SqliteMirrorReview::load(&root).unwrap();
  assert_eq!(cold.inspect_local_floor_r173(&path,NOW,OWNER,KEY).unwrap(),
    FloorReviewR173::Clean);
  let b=cold.replay_with_local_floor_r172(&p(8),&path,NOW,3,OWNER,KEY,None).unwrap();
  assert_eq!((b.newly_applied,b.already_applied,b.final_seq),(4,4,50));
  assert_eq!(cold.review_integrity_r152(NOW).unwrap(),(50,8));
  done(&root,&path);
 }
 #[test]fn r172_fault_after_intent_is_explicitly_operator_cancelled(){
  let (root,path,db)=setup();
  db.initialize_local_floor_r171(&path,NOW,OWNER,KEY).unwrap();
  assert_eq!(db.replay_with_local_floor_r172(&p(4),&path,NOW,2,OWNER,KEY,
    Some("after_intent")).err(),Some(PaperAdapterErrorR156::Mirror));
  let cold=SqliteMirrorReview::load(&root).unwrap();
  assert_eq!(cold.inspect_local_floor_r173(&path,NOW,OWNER,KEY).unwrap(),
    FloorReviewR173::Prepared);
  assert_eq!(cold.replay_with_local_floor_r172(&p(4),&path,NOW,2,OWNER,KEY,None)
    .err(),Some(PaperAdapterErrorR156::Source));
  cold.cancel_unapplied_intent_r173(&path,NOW,OWNER,KEY).unwrap();
  assert_eq!(cold.inspect_local_floor_r173(&path,NOW,OWNER,KEY).unwrap(),
    FloorReviewR173::Clean);
  assert_eq!(cold.replay_with_local_floor_r172(&p(4),&path,NOW,2,OWNER,KEY,None)
    .unwrap().final_seq,46);
  done(&root,&path);
 }
 #[test]fn r173_committed_before_external_publish_requires_manual_attestation(){
  let (root,path,db)=setup();
  db.initialize_local_floor_r171(&path,NOW,OWNER,KEY).unwrap();
  assert_eq!(db.replay_with_local_floor_r172(&p(4),&path,NOW,2,OWNER,KEY,
    Some("after_db_commit")).err(),Some(PaperAdapterErrorR156::Mirror));
  let cold=SqliteMirrorReview::load(&root).unwrap();
  assert_eq!(cold.inspect_local_floor_r173(&path,NOW,OWNER,KEY).unwrap(),
    FloorReviewR173::CommittedUnpublished);
  assert!(cold.cancel_unapplied_intent_r173(&path,NOW,OWNER,KEY).is_err());
  assert!(cold.replay_with_local_floor_r172(&p(4),&path,NOW,2,OWNER,KEY,None).is_err());
  let w=cold.publish_committed_intent_r173(&p(4),&path,NOW,OWNER,KEY).unwrap();
  assert_eq!(w.seq,46);
  assert_eq!(cold.inspect_local_floor_r173(&path,NOW,OWNER,KEY).unwrap(),
    FloorReviewR173::Clean);
  assert!(cold.publish_committed_intent_r173(&p(4),&path,NOW,OWNER,KEY).is_err());
  done(&root,&path);
 }
 #[test]fn r173_after_publication_exception_has_clean_committed_floor(){
  let (root,path,db)=setup();
  db.initialize_local_floor_r171(&path,NOW,OWNER,KEY).unwrap();
  assert_eq!(db.replay_with_local_floor_r172(&p(3),&path,NOW,1,OWNER,KEY,
    Some("after_publish")).err(),Some(PaperAdapterErrorR156::Mirror));
  let cold=SqliteMirrorReview::load(&root).unwrap();
  assert_eq!(cold.inspect_local_floor_r173(&path,NOW,OWNER,KEY).unwrap(),
    FloorReviewR173::Clean);
  assert_eq!(cold.review_integrity_r152(NOW).unwrap(),(45,3));
  done(&root,&path);
 }
 #[test]fn r171_floor_tamper_and_cross_directory_or_stale_lock_fail_closed(){
  let (root,path,db)=setup();
  db.initialize_local_floor_r171(&path,NOW,OWNER,KEY).unwrap();
  let lock=acquire_floor_lock_r171(&path).unwrap();
  assert!(acquire_floor_lock_r171(&path).is_err());
  drop(lock);
  let wrong=root.join("external-witness-floor.json");
  assert_eq!(db.initialize_local_floor_r171(&wrong,NOW,OWNER,KEY).err(),
    Some(PaperAdapterErrorR156::Path));
  let mut f=load_floor_r171(&path,KEY).unwrap();
  f.owner="another-owner".into();
  fs::write(&path,serde_json::to_vec(&f).unwrap()).unwrap();
  assert_eq!(db.inspect_local_floor_r173(&path,NOW,OWNER,KEY).err(),
    Some(PaperAdapterErrorR156::Source));
  assert_eq!(db.review_integrity_r152(NOW).unwrap(),(0,0));
  done(&root,&path);
 }
 #[test]fn r174_session_namespaces_have_independent_generations(){
  let x="session_200_1792253053";let y="session_201_1792253053";
  let ra=isolated_session_root_r174(x).unwrap();
  let rb=isolated_session_root_r174(y).unwrap();
  assert_ne!(ra,rb);
  assert!(isolated_session_root_r174("../../spotbot").is_err());
  fs::DirBuilder::new().mode(0o700).create(&ra).unwrap();
  fs::DirBuilder::new().mode(0o700).create(&rb).unwrap();
  let a=SqliteMirrorReview::initialize(&ra).unwrap();
  let b=SqliteMirrorReview::initialize(&rb).unwrap();
  let projection_a=p(3);
  let (raw,tip)=r156_r157_tests::session_journal_r159(y,3);
  let projection_b=model_projection_rows_r156(&raw,3,&tip,NOW).unwrap();
  a.replay_modeled_paper_r157(&projection_a,NOW,3).unwrap();
  b.replay_modeled_paper_r157(&projection_b,NOW,3).unwrap();
  assert_ne!(projection_a.rows[0].feed_generation,projection_b.rows[0].feed_generation);
  assert_eq!(a.review_integrity_r152(NOW).unwrap(),(45,3));
  assert_eq!(b.review_integrity_r152(NOW).unwrap(),(45,3));
  assert_eq!(a.replay_modeled_paper_r157(&projection_b,NOW,3).err(),
    Some(PaperAdapterErrorR156::Source));
  fs::remove_dir_all(ra).unwrap();fs::remove_dir_all(rb).unwrap();
 }
 #[test]fn r175_max_256_event_bounded_batch_and_reject_overflow(){
  let (root,path,db)=setup();
  db.initialize_local_floor_r171(&path,NOW,OWNER,KEY).unwrap();
  let max=p(256);
  let first=db.replay_with_local_floor_r172(&max,&path,NOW,16,OWNER,KEY,None).unwrap();
  assert_eq!((first.newly_applied,first.final_seq),(256,298));
  assert_eq!(db.review_integrity_r152(NOW).unwrap(),(298,256));
  assert_eq!(db.inspect_local_floor_r173(&path,NOW,OWNER,KEY).unwrap(),
    FloorReviewR173::Clean);
  assert_eq!(model_projection_rows_r156(
    &r156_r157_tests::journal(257).0,257,&r156_r157_tests::journal(257).1,NOW
  ).err(),Some(PaperAdapterErrorR156::Limit));
  done(&root,&path);
 }
 #[test]fn r176_operator_alerts_propagate_critical_states_without_trade_permission(){
  assert!(floor_operator_alert_r176(FloorReviewR173::CommittedUnpublished)
    .contains("CRITICAL"));
  assert!(floor_operator_alert_r176(FloorReviewR173::Partial).contains("halt"));
  assert!(floor_operator_alert_r176(FloorReviewR173::Prepared).contains("ALERT"));
  assert!(floor_operator_alert_r176(FloorReviewR173::Reject).contains("do not trade"));
  let (root,path,db)=setup();
  db.initialize_local_floor_r171(&path,NOW,OWNER,KEY).unwrap();
  let outcome=db.replay_with_local_floor_r172(&p(2),&path,NOW,1,OWNER,KEY,None).unwrap();
  assert!(!outcome.may_trade&&!outcome.production_db2);
  done(&root,&path);
 }
 #[test]fn r173_partial_batch_crash_is_detected_and_never_silently_resumed(){
  let (root,path,db)=setup();
  db.initialize_local_floor_r171(&path,NOW,OWNER,KEY).unwrap();
  assert_eq!(db.replay_with_local_floor_r172(&p(10),&path,NOW,3,OWNER,KEY,
    Some("after_first_batch")).err(),Some(PaperAdapterErrorR156::Mirror));
  let cold=SqliteMirrorReview::load(&root).unwrap();
  assert_eq!(cold.review_integrity_r152(NOW).unwrap(),(45,3));
  assert_eq!(cold.inspect_local_floor_r173(&path,NOW,OWNER,KEY).unwrap(),
    FloorReviewR173::Partial);
  assert!(cold.replay_with_local_floor_r172(&p(10),&path,NOW,3,OWNER,KEY,None).is_err());
  assert!(cold.cancel_unapplied_intent_r173(&path,NOW,OWNER,KEY).is_err());
  assert!(cold.publish_committed_intent_r173(&p(10),&path,NOW,OWNER,KEY).is_err());
  done(&root,&path);
 }
 #[test]fn r172_invalid_batch_denied_without_creating_external_intent(){
  let (root,path,db)=setup();
  db.initialize_local_floor_r171(&path,NOW,OWNER,KEY).unwrap();
  assert!(db.replay_with_local_floor_r172(&p(4),&path,NOW,0,OWNER,KEY,None).is_err());
  assert!(db.replay_with_local_floor_r172(&p(4),&path,NOW,17,OWNER,KEY,None).is_err());
  assert_eq!(db.inspect_local_floor_r173(&path,NOW,OWNER,KEY).unwrap(),
    FloorReviewR173::Clean);
  done(&root,&path);
 }
}
