//! R32D6C8B2A: durable, bounded PAPER-only daily-entry and occupancy ledger.
//! Mirrors legacy paper_max_trades_per_day vs max_trades_per_day selection.
//! No trading permission: real trial has ZERO allowed daily entries.
//! An entry reservation is an accounting record, NEVER an exchange order.
use std::collections::{BTreeMap,BTreeSet};
use serde::{Serialize,Deserialize};
use crate::native_trading_config::NativeTradingConfig;

pub const YEKT_OFFSET_MS:u64=5*60*60*1000;
pub const UTC_DAY_MS:u64=24*60*60*1000;
pub const MAX_RISK_RECORDS:usize=30_000;
pub const MAX_PAPER_SLOTS:u64=10;
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum RiskError {Malformed,Clock,Policy,Capacity,DailyLimit,Busy,Conflict}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum RiskWrite {Recorded,Idempotent}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
pub struct NativePaperRiskRecord {
 pub entry_id:String,
 pub day_yekt:u64,
 pub reserved_at_ms:u64,
 pub released_at_ms:Option<u64>,
 pub close_receipt_id:Option<String>,
}
#[derive(Clone,Debug,PartialEq,Eq,Serialize,Deserialize)]
pub struct NativeDailyRiskLedger {
 pub schema_version:u32,
 pub config_sha256:String,
 pub last_seen_at_ms:u64,
 pub general_max_positions:u64,
 pub paper_daily_limit:u64,
 pub real_daily_limit:u64,
 pub real_max_positions:u64,
 pub real_cooldown_sec:u64,
 pub records:BTreeMap<String,NativePaperRiskRecord>,
}
pub fn yekt_day_index(now_ms:u64)->Result<u64,RiskError>{
 if now_ms==0{return Err(RiskError::Clock)}
 now_ms.checked_add(YEKT_OFFSET_MS)
   .map(|v|v/UTC_DAY_MS).ok_or(RiskError::Clock)
}
fn valid_identity(id:&str)->bool{
 !id.is_empty()&&id.len()<=96&&id.bytes().all(|c|
   c.is_ascii_alphanumeric()||c==b'-'||c==b'_'||c==b'.')
}
impl NativeDailyRiskLedger {
 pub fn new(config:&NativeTradingConfig,now_ms:u64)->Result<Self,RiskError>{
  if !config.source_projection_unchanged(){return Err(RiskError::Policy)}
  yekt_day_index(now_ms)?;
  let p=&config.policy;
  if p.general_max_positions==0||p.general_max_positions>MAX_PAPER_SLOTS
   ||p.paper_daily_trades==0||p.paper_daily_trades>100_000
   ||p.real_daily_trades!=0||p.real_max_positions!=1
   ||p.real_cooldown_sec!=10_800||p.real_execution_authorized
   ||config.sha256.len()!=64{
   return Err(RiskError::Policy)
  }
  Ok(Self{schema_version:1,config_sha256:config.sha256.clone(),
   last_seen_at_ms:now_ms,general_max_positions:p.general_max_positions,
   paper_daily_limit:p.paper_daily_trades,real_daily_limit:0,
   real_max_positions:1,real_cooldown_sec:10_800,records:BTreeMap::new()})
 }
 pub fn validate(&self)->Result<(),RiskError>{
  if self.schema_version!=1||self.config_sha256.len()!=64
   ||!self.config_sha256.bytes().all(|c|c.is_ascii_hexdigit()&&!c.is_ascii_uppercase())
   ||self.last_seen_at_ms==0
   ||self.general_max_positions==0||self.general_max_positions>MAX_PAPER_SLOTS
   ||self.paper_daily_limit==0||self.paper_daily_limit>100_000
   ||self.real_daily_limit!=0||self.real_max_positions!=1
   ||self.real_cooldown_sec!=10_800||self.records.len()>MAX_RISK_RECORDS{
    return Err(RiskError::Policy)
  }
  let mut active=0u64;
  let mut daily:BTreeMap<u64,u64>=BTreeMap::new();
  let mut receipts=BTreeSet::new();
  for (id,r) in &self.records{
   if !valid_identity(id)||id!=&r.entry_id||r.reserved_at_ms==0
      ||r.reserved_at_ms>self.last_seen_at_ms
      ||yekt_day_index(r.reserved_at_ms)?!=r.day_yekt{
     return Err(RiskError::Malformed)
   }
   match (r.released_at_ms,&r.close_receipt_id){
    (None,None)=>{active=active.checked_add(1).ok_or(RiskError::Capacity)?;},
    (Some(t),Some(receipt)) if valid_identity(receipt)
       &&t>=r.reserved_at_ms&&t<=self.last_seen_at_ms=>{
      if !receipts.insert(receipt){return Err(RiskError::Conflict)}
    },
    _=>return Err(RiskError::Malformed),
   }
   let n=daily.entry(r.day_yekt).or_insert(0);
   *n=n.checked_add(1).ok_or(RiskError::DailyLimit)?;
   if *n>self.paper_daily_limit{return Err(RiskError::DailyLimit)}
  }
  if active>self.general_max_positions{return Err(RiskError::Capacity)}
  Ok(())
 }
 pub fn ensure_config(&self,cfg:&NativeTradingConfig)->Result<(),RiskError>{
  if !cfg.source_projection_unchanged(){return Err(RiskError::Policy)}
  let p=&cfg.policy;
  if cfg.sha256!=self.config_sha256
   ||p.general_max_positions!=self.general_max_positions
   ||p.paper_daily_trades!=self.paper_daily_limit
   ||p.real_daily_trades!=0||p.real_max_positions!=1
   ||p.real_cooldown_sec!=10_800||p.real_execution_authorized{
   return Err(RiskError::Policy)
  }
  Ok(())
 }
 pub fn used_paper_today(&self,now_ms:u64)->Result<u64,RiskError>{
  let day=yekt_day_index(now_ms)?;
  Ok(self.records.values().filter(|r|r.day_yekt==day).count() as u64)
 }
 pub fn open_paper_slots(&self)->u64{
  self.records.values().filter(|r|r.released_at_ms.is_none()).count() as u64
 }
 pub fn can_reserve_paper(
  &self,cfg:&NativeTradingConfig,now_ms:u64,entry_id:&str
 )->Result<(),RiskError>{
  self.validate()?;
  self.ensure_config(cfg)?;
  if !cfg.policy.paper_entries_enabled{return Err(RiskError::Policy)}
  if !valid_identity(entry_id){return Err(RiskError::Malformed)}
  if now_ms<self.last_seen_at_ms{return Err(RiskError::Clock)}
  if self.records.contains_key(entry_id){return Err(RiskError::Conflict)}
  if self.records.len()>=MAX_RISK_RECORDS{return Err(RiskError::Capacity)}
  if self.used_paper_today(now_ms)?>=self.paper_daily_limit{return Err(RiskError::DailyLimit)}
  if self.open_paper_slots()>=self.general_max_positions{return Err(RiskError::Capacity)}
  Ok(())
 }
 pub fn can_reserve_real(&self,cfg:&NativeTradingConfig,_now_ms:u64)->Result<(),RiskError>{
  self.ensure_config(cfg)?;
  // Zero real entries/day under the source YAML is a HARD policy stop. No
  // caller may reinterpret legacy operator_approved as launch authority.
  Err(RiskError::Policy)
 }
}
