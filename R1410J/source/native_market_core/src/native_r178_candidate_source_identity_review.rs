//! R178: opt-in R155 candidate source identity gate. The source-on-disk
//! must still be the exact YAML admitted to the in-memory Rust strategy
//! before each local checkpoint and at completion. No deployed PAPER,
 //! remote witness, trading API or launch authority.
use crate::native_trading_config::NativeTradingConfig;
use crate::native_paper_config_bridge::bind_source_paper_quotas_review;
use crate::native_paper_strategy_frame_review::SupervisedStrategyPaper;
use openssl::sha::sha256;
use std::{fs::{self,OpenOptions},io::Read,os::unix::fs::{MetadataExt,OpenOptionsExt},path::{Path,PathBuf}};

const MAX_CONFIG_BYTES:u64=256*1024;
#[derive(Debug,Clone,Copy,PartialEq,Eq)]
pub enum CandidateSourceError{InvalidPolicy,InvalidFile,SourceChanged,ModelDrift}
#[derive(Clone,Debug)]
pub struct CandidateSourceIdentityR178{
 path:PathBuf,
 expected_sha256:String,
 expected_daily:u64,
 expected_concurrent:usize,
}
fn sha(bytes:&[u8])->String{
 sha256(bytes).iter().map(|b|format!("{b:02x}")).collect()
}
fn exact_file(path:&Path)->Result<Vec<u8>,CandidateSourceError>{
 // Open the caller's explicit public PAPER review YAML with NOFOLLOW;
 // check metadata of the open FD, not a separately resolved symlink.
 let mut f=OpenOptions::new().read(true)
  .custom_flags(libc::O_NOFOLLOW|libc::O_CLOEXEC).open(path)
  .map_err(|_|CandidateSourceError::InvalidFile)?;
 let m=f.metadata().map_err(|_|CandidateSourceError::InvalidFile)?;
 if !m.file_type().is_file()||m.len()==0||m.len()>MAX_CONFIG_BYTES
  ||m.nlink()!=1{return Err(CandidateSourceError::InvalidFile)}
 let mut bytes=Vec::with_capacity(m.len() as usize);
 f.by_ref().take(MAX_CONFIG_BYTES+1).read_to_end(&mut bytes)
  .map_err(|_|CandidateSourceError::InvalidFile)?;
 if bytes.is_empty()||bytes.len() as u64>MAX_CONFIG_BYTES{
  return Err(CandidateSourceError::InvalidFile)
 }
 // Reject replacement while the YAML bytes were being read.
 let current=fs::symlink_metadata(path).map_err(|_|CandidateSourceError::InvalidFile)?;
 if !current.file_type().is_file()||current.dev()!=m.dev()||current.ino()!=m.ino()
  ||current.len()!=bytes.len() as u64{
  return Err(CandidateSourceError::InvalidFile)
 }
 Ok(bytes)
}
impl CandidateSourceIdentityR178{
 /// This lock captures SOURCE identity, not installed binary identity.
 pub fn admit(path:&Path,config:&NativeTradingConfig)->Result<Self,CandidateSourceError>{
  if !config.source_projection_unchanged()||
    config.policy.real_execution_authorized||config.policy.real_daily_trades!=0{
   return Err(CandidateSourceError::InvalidPolicy)
  }
  let quotas=bind_source_paper_quotas_review(config)
    .map_err(|_|CandidateSourceError::InvalidPolicy)?;
  let bytes=exact_file(path)?;
  if sha(&bytes)!=config.sha256{return Err(CandidateSourceError::SourceChanged)}
  Ok(Self{path:path.to_path_buf(),expected_sha256:config.sha256.clone(),
   expected_daily:quotas.daily,expected_concurrent:quotas.concurrent})
 }
 /// Run before every R153 save; no hot-frame YAML parsing or network I/O.
 /// A replacement/rewrite of the source file makes sealing impossible.
 pub fn before_checkpoint(&self,paper:&SupervisedStrategyPaper)
  ->Result<(),CandidateSourceError>{
  let bytes=exact_file(&self.path)?;
  if sha(&bytes)!=self.expected_sha256{return Err(CandidateSourceError::SourceChanged)}
  let report=paper.effective_research_config_report()
    .map_err(|_|CandidateSourceError::ModelDrift)?;
  if report["source_sha256"].as_str()!=Some(self.expected_sha256.as_str())
   ||report["paper_max_daily_entries"].as_u64()!=Some(self.expected_daily)
   ||report["paper_max_concurrent_positions"].as_u64()!=Some(self.expected_concurrent as u64)
   ||paper.portfolio.max_daily_entries!=self.expected_daily
   ||paper.portfolio.max_concurrent_positions!=self.expected_concurrent{
   return Err(CandidateSourceError::ModelDrift)
  }
  Ok(())
 }
 pub fn source_sha256(&self)->&str{&self.expected_sha256}
 pub fn quotas(&self)->(u64,usize){(self.expected_daily,self.expected_concurrent)}
}
#[cfg(test)]
mod tests{
 use super::*;
 use std::sync::atomic::{AtomicU64,Ordering};
 use std::os::unix::fs::symlink;
 static SEQ:AtomicU64=AtomicU64::new(1);
 const BASE:&str=include_str!("../config/native_trading_operator_baseline_r64.yaml");
 const WALL:i64=1_791_783_600_000;
 fn fixture()->(PathBuf,NativeTradingConfig){
  let candidate=BASE.replacen(
   "scale_in_hints:\n  enabled: true","scale_in_hints:\n  enabled: false",1)
   .replacen("analytics:\n  enabled: true","analytics:\n  enabled: false",1);
  assert_ne!(candidate,BASE);
  let dir=std::env::temp_dir().join(format!("r178-source-{}-{}",
    std::process::id(),SEQ.fetch_add(1,Ordering::Relaxed)));
  fs::create_dir(&dir).unwrap();
  let path=dir.join("candidate.yaml");
  fs::write(&path,&candidate).unwrap();
  let parsed=NativeTradingConfig::parse(&candidate).unwrap();
  (path,parsed)
 }
 fn clean(p:&Path){fs::remove_dir_all(p.parent().unwrap()).unwrap();}
 fn strategy(c:NativeTradingConfig)->SupervisedStrategyPaper{
  SupervisedStrategyPaper::new(c,WALL).unwrap()
 }
 #[test]fn r178_exact_3000_source_can_bind_before_unsealed_checkpoint(){
  let (path,cfg)=fixture();
  let guard=CandidateSourceIdentityR178::admit(&path,&cfg).unwrap();
  let paper=strategy(cfg);
  assert_eq!(guard.quotas(),(3000,10));
  assert_eq!(paper.portfolio.max_daily_entries,3000);
  assert!(guard.before_checkpoint(&paper).is_ok());
  assert_eq!(guard.source_sha256(),paper.replay_image().unwrap().config_sha256);
  clean(&path);
 }
 #[test]fn r178_rewrite_same_schema_different_quota_denied_before_save(){
  let (path,cfg)=fixture();
  let guard=CandidateSourceIdentityR178::admit(&path,&cfg).unwrap();
  let paper=strategy(cfg);
  let original=fs::read_to_string(&path).unwrap();
  let modified=original.replacen("paper_max_trades_per_day: 3000",
    "paper_max_trades_per_day: 500",1);
  assert_ne!(original,modified);
  fs::write(&path,modified).unwrap();
  assert_eq!(guard.before_checkpoint(&paper),Err(CandidateSourceError::SourceChanged));
  clean(&path);
 }
 #[test]fn r178_same_semantics_whitespace_source_change_is_not_silent(){
  let (path,cfg)=fixture();
  let guard=CandidateSourceIdentityR178::admit(&path,&cfg).unwrap();
  let paper=strategy(cfg);
  let mut original=fs::read_to_string(&path).unwrap();
  original.push_str("\n# rewritten outside admitted source\n");
  fs::write(&path,original).unwrap();
  assert_eq!(guard.before_checkpoint(&paper),Err(CandidateSourceError::SourceChanged));
  clean(&path);
 }
 #[test]fn r178_symlink_replacement_and_missing_file_deny(){
  let (path,cfg)=fixture();
  let guard=CandidateSourceIdentityR178::admit(&path,&cfg).unwrap();
  let paper=strategy(cfg);
  let bytes=fs::read(&path).unwrap();
  let other=path.with_file_name("other.yaml");
  fs::write(&other,bytes).unwrap();
  fs::remove_file(&path).unwrap();
  symlink(&other,&path).unwrap();
  assert_eq!(guard.before_checkpoint(&paper),Err(CandidateSourceError::InvalidFile));
  fs::remove_file(&path).unwrap();
  assert_eq!(guard.before_checkpoint(&paper),Err(CandidateSourceError::InvalidFile));
  clean(&path);
 }
 #[test]fn r178_tampered_policy_after_parse_denied_at_admission(){
  let (path,mut cfg)=fixture();
  cfg.policy.paper_daily_trades=500;
  assert!(matches!(CandidateSourceIdentityR178::admit(&path,&cfg),
   Err(CandidateSourceError::InvalidPolicy)));
  clean(&path);
 }
 #[test]fn r178_yaml_altered_after_parse_denied_at_admission(){
  let (path,cfg)=fixture();
  let source=fs::read_to_string(&path).unwrap().replace(
   "paper_max_trades_per_day: 3000","paper_max_trades_per_day: 501");
  fs::write(&path,source).unwrap();
  assert!(matches!(CandidateSourceIdentityR178::admit(&path,&cfg),
   Err(CandidateSourceError::SourceChanged)));
  clean(&path);
 }
 #[test]fn r178_research_portfolio_quota_drift_denied(){
  let (path,cfg)=fixture();
  let guard=CandidateSourceIdentityR178::admit(&path,&cfg).unwrap();
  let mut paper=strategy(cfg);
  paper.portfolio.max_daily_entries=500;
  assert_eq!(guard.before_checkpoint(&paper),Err(CandidateSourceError::ModelDrift));
  clean(&path);
 }
 #[test]fn r178_no_source_file_symlink_is_admitted(){
  let (path,cfg)=fixture();
  let other=path.with_file_name("link.yaml");
  symlink(&path,&other).unwrap();
  assert!(matches!(CandidateSourceIdentityR178::admit(&other,&cfg),
   Err(CandidateSourceError::InvalidFile)));
  clean(&path);
 }
}
