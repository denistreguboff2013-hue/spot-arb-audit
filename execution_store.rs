//! R14 review-only durable execution intent store.
use crate::execution::{Intent,IntentState};
use std::{fs::{self,File,OpenOptions},io::{self,Read,Write},path::{Path,PathBuf}};

#[derive(Debug)] pub struct DurableIntentStore{path:PathBuf}
impl DurableIntentStore{
 pub fn open(path:impl AsRef<Path>)->io::Result<Self>{
  let path=path.as_ref().to_path_buf();
  if let Some(parent)=path.parent(){fs::create_dir_all(parent)?;File::open(parent)?.sync_all()?;}
  Ok(Self{path})
 }
 pub fn load(&self)->io::Result<Vec<Intent>>{
  // An orphan .tmp can be an fsynced PREPARED intent whose rename/ACK
  // was interrupted. Never accept the old or absent final snapshot as
  // "empty" and grant a new mutation while that write is unresolved.
  let pending_stage=self.path.with_extension("tmp");
  match fs::symlink_metadata(&pending_stage) {
   Ok(_)=>return Err(io::Error::new(io::ErrorKind::InvalidData,"unresolved durable intent stage")),
   Err(e) if e.kind()==io::ErrorKind::NotFound=>{},
   Err(e)=>return Err(e),
  }
  if !self.path.exists(){return Ok(Vec::new())}
  let mut s=String::new();File::open(&self.path)?.read_to_string(&mut s)?;
  if s.is_empty(){return Ok(Vec::new())}
  serde_json::from_str(&s).map_err(|e|io::Error::new(io::ErrorKind::InvalidData,e))
 }
 pub fn persist(&self,intents:&[Intent])->io::Result<()>{
  let parent=self.path.parent().ok_or_else(||io::Error::new(io::ErrorKind::InvalidInput,"missing parent"))?;
  let tmp=self.path.with_extension("tmp");
  let bytes=serde_json::to_vec(intents).map_err(|e|io::Error::new(io::ErrorKind::InvalidData,e))?;
  let mut f=OpenOptions::new().write(true).create(true).truncate(true).open(&tmp)?;
  f.write_all(&bytes)?;f.sync_all()?;
  fs::rename(&tmp,&self.path)?;File::open(parent)?.sync_all()?;
  Ok(())
 }
 pub fn persist_prepared(&self,id:&str,client_id:&str)->io::Result<Vec<Intent>>{
  // R91: check original durable identity at the actual R14 PREPARED
  // admission boundary, before fsync or return of PersistPrepare.
  if id.is_empty()||client_id.is_empty()||
     id.len()>256||client_id.len()>256{
   return Err(io::Error::new(io::ErrorKind::InvalidInput,
      "R14 intent and client identity must be nonempty and bounded"))
  }
  let mut v=self.load()?;
  if v.iter().any(|x|matches!(x.state,IntentState::Prepared|IntentState::Acknowledged|IntentState::Unknown|IntentState::Rejected)){return Err(io::Error::new(io::ErrorKind::WouldBlock,"unresolved mutation or unreviewed rejection"))}
  if v.iter().any(|x|x.id==id){return Err(io::Error::new(io::ErrorKind::AlreadyExists,"duplicate intent"))}
  if v.iter().any(|x|x.client_order_id.as_deref()==Some(client_id)){
   return Err(io::Error::new(io::ErrorKind::AlreadyExists,
      "duplicate durable R14 client order identity"))
  }
  v.push(Intent{id:id.into(),client_order_id:Some(client_id.into()),state:IntentState::Prepared});
  self.persist(&v)?;Ok(v)
 }
}

#[cfg(test)] mod tests{use super::*;use std::time::{SystemTime,UNIX_EPOCH};
 fn path(n:&str)->PathBuf{let x=SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();std::env::temp_dir().join(format!("spotarb-r14-{n}-{x}")).join("intents.json")}
 #[test]fn prepared_survives_reopen(){let p=path("reopen");let s=DurableIntentStore::open(&p).unwrap();s.persist_prepared("i1","c1").unwrap();drop(s);let s=DurableIntentStore::open(&p).unwrap();let v=s.load().unwrap();assert_eq!(v.len(),1);assert_eq!(v[0].state,IntentState::Prepared);let _=fs::remove_dir_all(p.parent().unwrap());}
 #[test]fn unresolved_blocks_next_after_reopen(){let p=path("block");DurableIntentStore::open(&p).unwrap().persist_prepared("i1","c1").unwrap();let s=DurableIntentStore::open(&p).unwrap();assert_eq!(s.persist_prepared("i2","c2").unwrap_err().kind(),io::ErrorKind::WouldBlock);let _=fs::remove_dir_all(p.parent().unwrap());}
 #[test]fn r91_reconciled_intent_does_not_reuse_client_order_identity(){
  let p=path("r91-client-reuse");
  let store=DurableIntentStore::open(&p).unwrap();
  store.persist_prepared("first","CLIENT_ORIGINAL").unwrap();
  store.persist_ack("first",false).unwrap();
  store.persist_reconciled("first",true).unwrap();
  drop(store);
  let cold=DurableIntentStore::open(&p).unwrap();
  let err=cold.persist_prepared("second","CLIENT_ORIGINAL").unwrap_err();
  assert_eq!(err.kind(),io::ErrorKind::AlreadyExists,
     "R14 must reject a repeated original client ID even after reconciliation");
  assert_eq!(cold.load().unwrap().len(),1);
  let _=fs::remove_dir_all(p.parent().unwrap());
 }
 #[test]fn r91_empty_original_intent_and_client_ids_fail_before_prepare(){
  let p=path("r91-empty-identities");
  let store=DurableIntentStore::open(&p).unwrap();
  assert_eq!(store.persist_prepared("","OK_CLIENT").unwrap_err().kind(),
     io::ErrorKind::InvalidInput);
  assert_eq!(store.persist_prepared("ID","").unwrap_err().kind(),
     io::ErrorKind::InvalidInput);
  assert!(store.load().unwrap().is_empty());
  let _=fs::remove_dir_all(p.parent().unwrap());
 }
 #[test]fn malformed_store_fails_closed(){let p=path("bad");let s=DurableIntentStore::open(&p).unwrap();fs::write(&p,b"{bad").unwrap();assert_eq!(s.load().unwrap_err().kind(),io::ErrorKind::InvalidData);let _=fs::remove_dir_all(p.parent().unwrap());}
}

impl DurableIntentStore{
 fn transition<F>(&self,id:&str,mut f:F)->io::Result<Vec<Intent>> where F:FnMut(&mut Intent)->io::Result<()>{
  let mut v=self.load()?;let x=v.iter_mut().find(|x|x.id==id).ok_or_else(||io::Error::new(io::ErrorKind::NotFound,"intent"))?;f(x)?;self.persist(&v)?;Ok(v)
 }
 pub fn persist_ack(&self,id:&str,rejected:bool)->io::Result<Vec<Intent>>{self.transition(id,|x|{if x.state!=IntentState::Prepared{return Err(io::Error::new(io::ErrorKind::InvalidInput,"invalid transition"))}x.state=if rejected{IntentState::Rejected}else{IntentState::Acknowledged};Ok(())})}
 pub fn persist_unknown(&self,id:&str)->io::Result<Vec<Intent>>{self.transition(id,|x|{if x.state!=IntentState::Prepared{return Err(io::Error::new(io::ErrorKind::InvalidInput,"invalid transition"))}x.state=IntentState::Unknown;Ok(())})}
 pub fn persist_reconciled(&self,id:&str,evidence:bool)->io::Result<Vec<Intent>>{if !evidence{return Err(io::Error::new(io::ErrorKind::PermissionDenied,"evidence required"))}self.transition(id,|x|{if !matches!(x.state,IntentState::Prepared|IntentState::Acknowledged|IntentState::Unknown){return Err(io::Error::new(io::ErrorKind::InvalidInput,"invalid transition"))}x.state=IntentState::Reconciled;Ok(())})}
}

#[cfg(test)] mod transition_tests{use super::*;use std::time::{SystemTime,UNIX_EPOCH};
 fn p(n:&str)->PathBuf{std::env::temp_dir().join(format!("spotarb-r14-t-{n}-{}",SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos())).join("i.json")}
 #[test]fn ack_and_reject_are_durable(){for reject in [false,true]{let p=p(if reject{"rej"}else{"ack"});let s=DurableIntentStore::open(&p).unwrap();s.persist_prepared("i","c").unwrap();s.persist_ack("i",reject).unwrap();drop(s);let v=DurableIntentStore::open(&p).unwrap().load().unwrap();assert_eq!(v[0].state,if reject{IntentState::Rejected}else{IntentState::Acknowledged});let _=fs::remove_dir_all(p.parent().unwrap());}}
 #[test]fn unknown_is_durable_and_blocks_after_reopen(){let p=p("unk");let s=DurableIntentStore::open(&p).unwrap();s.persist_prepared("i","c").unwrap();s.persist_unknown("i").unwrap();drop(s);let s=DurableIntentStore::open(&p).unwrap();assert_eq!(s.load().unwrap()[0].state,IntentState::Unknown);assert_eq!(s.persist_prepared("n","c2").unwrap_err().kind(),io::ErrorKind::WouldBlock);let _=fs::remove_dir_all(p.parent().unwrap());}
 #[test]fn reconcile_requires_evidence_and_is_durable(){let p=p("rec");let s=DurableIntentStore::open(&p).unwrap();s.persist_prepared("i","c").unwrap();s.persist_unknown("i").unwrap();assert_eq!(s.persist_reconciled("i",false).unwrap_err().kind(),io::ErrorKind::PermissionDenied);s.persist_reconciled("i",true).unwrap();drop(s);assert_eq!(DurableIntentStore::open(&p).unwrap().load().unwrap()[0].state,IntentState::Reconciled);let _=fs::remove_dir_all(p.parent().unwrap());}
 #[test]fn acknowledged_without_durable_reconciliation_must_block_next_order_after_restart(){
  let p=p("ack-cold-barrier");
  let s=DurableIntentStore::open(&p).unwrap();
  s.persist_prepared("first","client_first").unwrap();
  s.persist_ack("first",false).unwrap();
  drop(s);
  let reopened=DurableIntentStore::open(&p).unwrap();
  assert_eq!(reopened.load().unwrap()[0].state,IntentState::Acknowledged);
  assert_eq!(reopened.persist_prepared("second","client_second").unwrap_err().kind(),io::ErrorKind::WouldBlock);
  reopened.persist_reconciled("first",true).unwrap();
  reopened.persist_prepared("second","client_second").unwrap();
  assert_eq!(reopened.load().unwrap()[1].state,IntentState::Prepared);
  let _=fs::remove_dir_all(p.parent().unwrap());
 }
 #[test]fn rejected_journal_without_lifecycle_commit_must_block_cold_new_order(){
  let p=p("reject-cold-crash");
  let mut gate=DurableMutationGate::open(&p).unwrap();
  gate.begin("first",Leg::Futures,"client_first").unwrap();
  gate.acknowledge("first",true).unwrap();
  // Simulated crash before R15 lifecycle persisted terminal manual state.
  drop(gate);
  let mut cold=DurableMutationGate::open(&p).unwrap();
  assert_eq!(cold.store.load().unwrap()[0].state,IntentState::Rejected);
  assert_eq!(cold.begin("next",Leg::Futures,"client_next").unwrap_err().kind(),
             io::ErrorKind::WouldBlock);
  assert!(cold.after_durable_prepare("first").is_err());
  let _=fs::remove_dir_all(p.parent().unwrap());
 }
 #[test]fn terminal_transition_fails_closed(){let p=p("term");let s=DurableIntentStore::open(&p).unwrap();s.persist_prepared("i","c").unwrap();s.persist_ack("i",false).unwrap();assert_eq!(s.persist_unknown("i").unwrap_err().kind(),io::ErrorKind::InvalidInput);let _=fs::remove_dir_all(p.parent().unwrap());}
}

use crate::execution::{Action,Leg};

#[derive(Debug)] pub struct DurableMutationGate{pub store:DurableIntentStore,pending:Option<(String,Leg)>}
impl DurableMutationGate{
 pub fn open(path:impl AsRef<Path>)->io::Result<Self>{let store=DurableIntentStore::open(path)?;let _=store.load()?;Ok(Self{store,pending:None})}
 pub fn begin(&mut self,id:&str,leg:Leg,client_id:&str)->io::Result<Action>{
  self.store.persist_prepared(id,client_id)?;
  self.pending=Some((id.into(),leg));
  Ok(Action::PersistPrepare{intent_id:id.into(),leg})
 }
 pub fn after_durable_prepare(&self,id:&str)->io::Result<Action>{
  let (pending,leg)=self.pending.as_ref().ok_or_else(||io::Error::new(io::ErrorKind::PermissionDenied,"no durable prepare in this process"))?;
  if pending!=id{return Err(io::Error::new(io::ErrorKind::PermissionDenied,"intent mismatch"))}
  let v=self.store.load()?;let x=v.iter().find(|x|x.id==id).ok_or_else(||io::Error::new(io::ErrorKind::NotFound,"intent"))?;
  if x.state!=IntentState::Prepared{return Err(io::Error::new(io::ErrorKind::PermissionDenied,"not prepared"))}
  Ok(Action::NetworkMutation{intent_id:id.into(),leg:*leg})
 }
 pub fn acknowledge(&mut self,id:&str,rejected:bool)->io::Result<()> {self.store.persist_ack(id,rejected)?;self.pending=None;Ok(())}
 pub fn ambiguous(&mut self,id:&str)->io::Result<Action>{self.store.persist_unknown(id)?;self.pending=None;Ok(Action::EnterRecovery{intent_id:id.into()})}
}

#[cfg(test)] mod durable_gate_tests{use super::*;use std::time::{SystemTime,UNIX_EPOCH};
 fn p(n:&str)->PathBuf{std::env::temp_dir().join(format!("spotarb-r14-g-{n}-{}",SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos())).join("i.json")}
 #[test]fn network_action_only_after_physical_prepare(){let p=p("order");let mut g=DurableMutationGate::open(&p).unwrap();assert!(g.after_durable_prepare("i").is_err());assert!(matches!(g.begin("i",Leg::Spot,"c").unwrap(),Action::PersistPrepare{..}));assert_eq!(g.store.load().unwrap()[0].state,IntentState::Prepared);assert!(matches!(g.after_durable_prepare("i").unwrap(),Action::NetworkMutation{..}));let _=fs::remove_dir_all(p.parent().unwrap());}
 #[test]fn restart_never_reissues_prepared(){let p=p("restart");let mut g=DurableMutationGate::open(&p).unwrap();g.begin("i",Leg::Spot,"c").unwrap();drop(g);let mut r=DurableMutationGate::open(&p).unwrap();assert!(r.after_durable_prepare("i").is_err());assert_eq!(r.begin("n",Leg::Spot,"c2").unwrap_err().kind(),io::ErrorKind::WouldBlock);let _=fs::remove_dir_all(p.parent().unwrap());}
 #[test]fn durable_unknown_blocks_restart_and_send(){let p=p("unknown");let mut g=DurableMutationGate::open(&p).unwrap();g.begin("i",Leg::Futures,"c").unwrap();assert!(matches!(g.ambiguous("i").unwrap(),Action::EnterRecovery{..}));drop(g);let mut r=DurableMutationGate::open(&p).unwrap();assert!(r.after_durable_prepare("i").is_err());assert_eq!(r.begin("n",Leg::Spot,"c2").unwrap_err().kind(),io::ErrorKind::WouldBlock);let _=fs::remove_dir_all(p.parent().unwrap());}
}

#[derive(Clone,Copy,Debug,PartialEq,Eq)] pub enum PersistFailpoint{None,BeforeWrite,AfterWrite,AfterFileSync,AfterRename,AfterDirSync}
impl DurableIntentStore{
 pub fn persist_with_failpoint(&self,intents:&[Intent],fp:PersistFailpoint)->io::Result<()>{
  let parent=self.path.parent().ok_or_else(||io::Error::new(io::ErrorKind::InvalidInput,"missing parent"))?;
  let tmp=self.path.with_extension("tmp");let bytes=serde_json::to_vec(intents).map_err(|e|io::Error::new(io::ErrorKind::InvalidData,e))?;
  if fp==PersistFailpoint::BeforeWrite{return Err(io::Error::new(io::ErrorKind::Other,"failpoint before write"))}
  let mut f=OpenOptions::new().write(true).create(true).truncate(true).open(&tmp)?;f.write_all(&bytes)?;
  if fp==PersistFailpoint::AfterWrite{return Err(io::Error::new(io::ErrorKind::Other,"failpoint after write"))}
  f.sync_all()?;
  if fp==PersistFailpoint::AfterFileSync{return Err(io::Error::new(io::ErrorKind::Other,"failpoint after file sync"))}
  fs::rename(&tmp,&self.path)?;
  if fp==PersistFailpoint::AfterRename{return Err(io::Error::new(io::ErrorKind::Other,"failpoint after rename"))}
  File::open(parent)?.sync_all()?;
  if fp==PersistFailpoint::AfterDirSync{return Err(io::Error::new(io::ErrorKind::Other,"failpoint after dir sync"))}
  Ok(())
 }
}

#[cfg(test)] mod crash_window_tests{use super::*;use std::time::{SystemTime,UNIX_EPOCH};
 fn p(n:&str)->PathBuf{std::env::temp_dir().join(format!("spotarb-r14-fp-{n}-{}",SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos())).join("i.json")}
 fn one()->Vec<Intent>{vec![Intent{id:"i".into(),client_order_id:Some("c".into()),state:IntentState::Prepared}]}
 #[test]fn every_failed_persist_denies_network_permission(){
  for fp in [PersistFailpoint::BeforeWrite,PersistFailpoint::AfterWrite,PersistFailpoint::AfterFileSync,PersistFailpoint::AfterRename,PersistFailpoint::AfterDirSync]{
   let p=p(&format!("{fp:?}"));let s=DurableIntentStore::open(&p).unwrap();assert!(s.persist_with_failpoint(&one(),fp).is_err());
   let g=DurableMutationGate::open(&p);match g{Ok(g)=>assert!(g.after_durable_prepare("i").is_err()),Err(_)=>{}}let _=fs::remove_dir_all(p.parent().unwrap());
  }
 }
 #[test]fn orphan_durable_stage_must_block_cold_open_and_new_prepare(){
  for fp in [PersistFailpoint::AfterWrite,PersistFailpoint::AfterFileSync] {
   let p=p(&format!("orphan-{fp:?}"));
   let store=DurableIntentStore::open(&p).unwrap();
   assert!(store.persist_with_failpoint(&one(),fp).is_err());
   assert!(p.with_extension("tmp").exists());
   drop(store);
   assert_eq!(DurableMutationGate::open(&p).unwrap_err().kind(),io::ErrorKind::InvalidData);
   let _=fs::remove_dir_all(p.parent().unwrap());
  }
 }
 #[test]fn success_persist_survives_reopen_but_restart_still_cannot_send(){
  let p=p("ok");let s=DurableIntentStore::open(&p).unwrap();s.persist_with_failpoint(&one(),PersistFailpoint::None).unwrap();drop(s);
  let g=DurableMutationGate::open(&p).unwrap();assert_eq!(g.store.load().unwrap()[0].state,IntentState::Prepared);assert!(g.after_durable_prepare("i").is_err());let _=fs::remove_dir_all(p.parent().unwrap());
 }
}
