//! R47 P1-06: read-only Rust analytics storage settings and pressure signals.
 //! Parity with Python collector._refresh_storage_health; NO SQLite mutation.
use crate::native_trading_config::NativeTradingConfig;

#[derive(Clone,Debug,PartialEq,Eq)]
pub enum StorageReviewError{InvalidConfig,InvalidSnapshot}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct NativeAnalyticsStorageLimits{
 pub enabled:bool,
 pub primary_relative_path:String,
 pub mirror_relative_path:String,
 pub max_db_bytes:u64,
 pub max_wal_bytes:u64,
 pub raw_retention_days:u64,
}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub enum PressureLevel{Normal,Warning,High,Critical}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct StorageSnapshot{
 pub sampled_ms:u64,
 pub used_db_bytes:u64,
 pub wal_bytes:u64,
}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct StorageAdvice{
 pub pressure:PressureLevel,
 pub wal_near_limit:bool,
 pub manual_review:bool,
 pub authenticated_usage:bool,
 pub can_prune:bool,
 pub can_write_db:bool,
 pub can_trade:bool,
}
fn safe_relative_sqlite(s:&str)->bool{
 s.starts_with("data/") && s.ends_with(".sqlite") && s.len()<=128
  && !s.contains("..") && !s.contains("//")
  && s.strip_prefix("data/").is_some_and(|n|!n.is_empty()&&!n.contains('/'))
  && s.bytes().all(|c|c.is_ascii_alphanumeric()||
    c==b'_'||c==b'-'||c==b'.'||c==b'/')
}
impl NativeAnalyticsStorageLimits{
 pub fn from_native_config(cfg:&NativeTradingConfig)->Result<Self,StorageReviewError>{
  if !cfg.source_projection_unchanged(){return Err(StorageReviewError::InvalidConfig)}
  let a=cfg.raw.get("analytics").ok_or(StorageReviewError::InvalidConfig)?;
  let str_val=|key:&str|a.get(key).and_then(|v|v.as_str())
    .ok_or(StorageReviewError::InvalidConfig).map(str::to_owned);
  let number=|key:&str|a.get(key).and_then(|v|v.as_u64())
    .ok_or(StorageReviewError::InvalidConfig);
  let enabled=a.get("enabled").and_then(|v|v.as_bool())
    .ok_or(StorageReviewError::InvalidConfig)?;
  if a.get("mode").and_then(|v|v.as_str())!=Some("observe")
    ||cfg.policy.real_execution_authorized{
   return Err(StorageReviewError::InvalidConfig)
  }
  let primary_relative_path=str_val("database_path")?;
  let mirror_relative_path=str_val("mirror_database_path")?;
  let db_mb=number("max_db_mb")?;
  let wal_mb=number("max_wal_mb")?;
  let days=number("raw_retention_days")?;
  if !safe_relative_sqlite(&primary_relative_path)
    ||!safe_relative_sqlite(&mirror_relative_path)
    ||primary_relative_path==mirror_relative_path
    ||!(2..=65536).contains(&db_mb)
    ||!(2..=8192).contains(&wal_mb)
    ||!(1..=365).contains(&days){
   return Err(StorageReviewError::InvalidConfig)
  }
  Ok(Self{enabled,primary_relative_path,mirror_relative_path,
   max_db_bytes:db_mb.checked_mul(1_048_576)
    .ok_or(StorageReviewError::InvalidConfig)?,
   max_wal_bytes:wal_mb.checked_mul(1_048_576)
    .ok_or(StorageReviewError::InvalidConfig)?,
   raw_retention_days:days})
 }
 pub fn inspect_without_io(&self,s:&StorageSnapshot,now_ms:u64)
   ->Result<StorageAdvice,StorageReviewError>{
  if now_ms==0||s.sampled_ms==0||s.sampled_ms>now_ms
    ||now_ms-s.sampled_ms>120_000{
   return Err(StorageReviewError::InvalidSnapshot)
  }
  let pct=(s.used_db_bytes as u128)*100;
  let cap=self.max_db_bytes as u128;
  let level=if pct>=cap*95{PressureLevel::Critical}
    else if pct>=cap*85{PressureLevel::High}
    else if pct>=cap*70{PressureLevel::Warning}
    else{PressureLevel::Normal};
  // Legacy WAL safety warning starts at cap minus one MiB.
  let wal_near_limit=s.wal_bytes>=self.max_wal_bytes-1_048_576;
  Ok(StorageAdvice{pressure:level,wal_near_limit,
   manual_review:level!=PressureLevel::Normal||wal_near_limit,
   authenticated_usage:false,can_prune:false,
   can_write_db:false,can_trade:false})
 }
}

#[cfg(test)]
mod tests{
 use super::*;
 fn config()->NativeTradingConfig{
  NativeTradingConfig::parse(include_str!("../config/native_trading.yaml")).unwrap()
 }
 fn snap(size:u64,wal:u64)->StorageSnapshot{
  StorageSnapshot{sampled_ms:100_000,used_db_bytes:size,wal_bytes:wal}
 }
 #[test]
 fn existing_legacy_storage_fields_are_all_bound(){
  let p=NativeAnalyticsStorageLimits::from_native_config(&config()).unwrap();
  assert!(p.enabled);
  assert_eq!(p.primary_relative_path,"data/arb_stats.sqlite");
  assert_eq!(p.mirror_relative_path,"data/arb_stats_mirror.sqlite");
  assert_eq!(p.max_db_bytes,8192*1_048_576);
  assert_eq!(p.max_wal_bytes,64*1_048_576);
  assert_eq!(p.raw_retention_days,1);
 }
 #[test]
 fn python_thresholds_match_at_byte_precision(){
  let p=NativeAnalyticsStorageLimits::from_native_config(&config()).unwrap();
  let cap=p.max_db_bytes as u128;
  for (percent,expected) in [(0,PressureLevel::Normal),
   (69,PressureLevel::Normal),(70,PressureLevel::Warning),
   (84,PressureLevel::Warning),(85,PressureLevel::High),
   (94,PressureLevel::High),(95,PressureLevel::Critical),
   (100,PressureLevel::Critical)]{
   let bytes=((cap*percent+99)/100) as u64;
   let review=p.inspect_without_io(&snap(bytes,0),100_000).unwrap();
   assert_eq!(review.pressure,expected);
   assert!(!review.can_write_db&&!review.can_prune&&!review.can_trade);
   assert!(!review.authenticated_usage);
  }
 }
 #[test]
 fn wal_and_stale_safety_are_advisory_only(){
  let p=NativeAnalyticsStorageLimits::from_native_config(&config()).unwrap();
  let a=p.inspect_without_io(
    &snap(0,p.max_wal_bytes-1_048_576),100_000).unwrap();
  assert!(a.wal_near_limit&&a.manual_review);
  assert_eq!(a.pressure,PressureLevel::Normal);
  assert_eq!(p.inspect_without_io(&snap(0,0),220_001),
    Err(StorageReviewError::InvalidSnapshot));
 }
 #[test]
 fn invalid_legacy_bounds_fail_closed(){
  let orig=include_str!("../config/native_trading.yaml");
  for (old,new) in [
   ("raw_retention_days: 1","raw_retention_days: 0"),
   ("max_db_mb: 8192","max_db_mb: 0"),
   ("max_wal_mb: 64","max_wal_mb: 0"),
   ("database_path: data/arb_stats.sqlite","database_path: /tmp/arb_stats.sqlite"),
   ("mirror_database_path: data/arb_stats_mirror.sqlite",
    "mirror_database_path: data/arb_stats.sqlite")
  ]{
   let altered=orig.replace(old,new);
   assert_ne!(altered,orig);
   let cfg=NativeTradingConfig::parse(&altered).unwrap();
   assert_eq!(NativeAnalyticsStorageLimits::from_native_config(&cfg),
     Err(StorageReviewError::InvalidConfig));
  }
 }
}
