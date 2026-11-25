//! R167: explicit SOURCE-owned, PUBLIC-only websocket grouping for R155.
//! This does not change the R21 compiled-fallback 32-wide shard profile.
//! No installed PAPER, REST/socket activity, real orders or authority.
use crate::native_shadow_config_review::NativeShadowReview;
use crate::native_trading_config::NativeTradingConfig;
use std::collections::HashSet;

#[derive(Clone,Debug,PartialEq,Eq)]
pub struct SourceWsShardPlanR167{
 pub groups:Vec<Vec<String>>,
 pub group_size:usize,
 pub start_interval_ms:u64,
 pub selected_bases:usize,
 pub config_sha256:String,
}

pub fn split_source_groups_for_ingress(groups:&[Vec<String>],max_bases:usize)->Result<Vec<Vec<String>>,&'static str>{
 if max_bases==0{return Err("R1239 ingress group width must be positive")}
 if groups.is_empty()||groups.iter().any(|group|group.is_empty()){return Err("R1239 empty source group")}
 Ok(groups.iter().flat_map(|group|group.chunks(max_bases).map(|part|part.to_vec())).collect())
}

impl SourceWsShardPlanR167{
 /// Preserve original Python ShadowWSFleet's sorted eligible-base order
 /// and group_size, but DENY values outside supported width rather than
 /// pretending the old Python clamp is already a live Rust dispatch.
 ///
 /// R155's metadata universe is already pre-screened; this plan cannot
 /// prove equivalence with Python MarketEligibility.static_reason.
 pub fn from_source(cfg:&NativeTradingConfig,bases:&[String])->Result<Self,&'static str>{
  let v=NativeShadowReview::from_config(cfg).map_err(|_|"R167 source shadow config invalid")?;
  if !v.enabled||!v.fleet_enabled{
   return Err("R167 source shadow WS fleet disabled")
  }
  // A limited subset would need source-eligibility reconciliation.
  // Keep all input bases until that audit exists.
  if v.base_limit!=0{
   return Err("R167 bounded base_limit needs independent eligibility proof")
  }
  if !(1..=60).contains(&v.group_size){
   return Err("R167 group_size outside Python-compatible 1..60")
  }
  if !(0.1..=60.0).contains(&v.start_interval_sec)||
      !v.start_interval_sec.is_finite(){
   return Err("R167 unsupported WS start interval")
  }
  if bases.is_empty()||bases.len()>512||
     bases.iter().any(|b|b.is_empty()||b.len()>32||
       !b.bytes().all(|c|c.is_ascii_uppercase()||c.is_ascii_digit())){
   return Err("R167 bad input universe")
  }
  let mut sorted=bases.to_vec();
  sorted.sort_unstable();
  if sorted.iter().collect::<HashSet<_>>().len()!=sorted.len(){
   return Err("R167 duplicate input symbol")
  }
  let group_size=v.group_size as usize;
  let start_interval_ms=(v.start_interval_sec.max(1.0)*1000.0).ceil() as u64;
  let groups=sorted.chunks(group_size).map(|group|group.to_vec())
      .collect::<Vec<_>>();
  let last_delay=start_interval_ms.checked_mul(
    u64::try_from(groups.len()-1).map_err(|_|"R167 group index overflow")?)
      .ok_or("R167 start delay overflow")?;
  if last_delay>60_000{
   return Err("R167 starts cannot exceed 60 seconds of stagger")
  }
  Ok(Self{selected_bases:sorted.len(),groups,group_size,start_interval_ms,
   config_sha256:cfg.sha256.clone()})
 }
 pub fn owner_shards(&self)->std::collections::BTreeMap<String,usize>{
  let mut map=std::collections::BTreeMap::new();
  for (i,bases) in self.groups.iter().enumerate(){
   for base in bases{map.insert(base.clone(),i);}
  }
  map
 }
 pub fn task_count(&self)->Result<usize,&'static str>{
  self.groups.len().checked_mul(3).ok_or("R167 task overflow")
 }
 pub fn delay_before(&self,shard:usize)->Result<std::time::Duration,&'static str>{
  if shard>=self.groups.len(){return Err("R167 shard out of bounds")}
  let ms=self.start_interval_ms.checked_mul(shard as u64)
      .ok_or("R167 stagger overflow")?;
  Ok(std::time::Duration::from_millis(ms))
 }
}
#[cfg(test)]
mod tests{
 use super::*;
 use crate::owner_index;
 const SOURCE:&str=include_str!("../config/native_trading.yaml");
 fn cfg(s:&str)->NativeTradingConfig{NativeTradingConfig::parse(s).unwrap()}
 fn bases(n:usize)->Vec<String>{
  // Deliberately unsorted, synthetic; NEVER claim these are actual markets.
  (0..n).rev().map(|i|format!("SIM{i:04}")).collect()
 }
 #[test]
 fn r167_original_yaml_235_bases_six_ws_groups_with_twelve_independent_owners(){
  let c=cfg(SOURCE);
  let list=bases(235);
  let p=SourceWsShardPlanR167::from_source(&c,&list).unwrap();
  assert_eq!(p.config_sha256,c.sha256);
  assert_eq!(p.group_size,40);
  assert_eq!(p.start_interval_ms,1000);
  assert_eq!(p.groups.iter().map(Vec::len).collect::<Vec<_>>(),
   vec![40,40,40,40,40,35]);
  assert_eq!(p.task_count(),Ok(18));
  assert_eq!(p.delay_before(5),Ok(std::time::Duration::from_secs(5)));
  let map=p.owner_shards();
  assert_eq!(map.len(),235);
  for (i,base) in p.groups.iter().flatten().enumerate(){
   assert_eq!(map[base],i/40);
   assert!(owner_index(base,12)<12);
  }
  assert_eq!(p.groups[0][0],"SIM0000");
  assert_eq!(p.groups[5][34],"SIM0234");
 }
 #[test]
 fn r167_trusts_source_width_not_compiled_fallback_and_bounds_task_count(){
  let s=SOURCE.replacen("group_size: 40","group_size: 32",1);
  let p=SourceWsShardPlanR167::from_source(&cfg(&s),&bases(235)).unwrap();
  assert_eq!(p.groups.iter().map(Vec::len).collect::<Vec<_>>(),
      vec![32,32,32,32,32,32,32,11]);
  assert_eq!(p.task_count(),Ok(24));
  let changed=SOURCE.replacen("group_size: 40","group_size: 60",1);
  let p=SourceWsShardPlanR167::from_source(&cfg(&changed),&bases(235)).unwrap();
  assert_eq!(p.groups.iter().map(Vec::len).collect::<Vec<_>>(),
      vec![60,60,60,55]);
  assert_eq!(p.task_count(),Ok(12));
  let reject=SOURCE.replacen("group_size: 40","group_size: 200",1);
  assert!(SourceWsShardPlanR167::from_source(&cfg(&reject),&bases(235)).is_err());
 }
 #[test]
 fn r167_source_disabled_and_partial_universe_fail_closed(){
  for (from,to) in [
   ("fast_market_shadow:\n  enabled: true",
    "fast_market_shadow:\n  enabled: false"),
   ("ws_fleet:\n    enabled: true","ws_fleet:\n    enabled: false"),
   ("base_limit: 0","base_limit: 100"),
  ]{
   let x=SOURCE.replacen(from,to,1);
   assert_ne!(x,SOURCE);
   assert!(SourceWsShardPlanR167::from_source(&cfg(&x),&bases(235)).is_err());
  }
  assert!(SourceWsShardPlanR167::from_source(&cfg(SOURCE),&[]).is_err());
  let mut duplicate=bases(235);
  duplicate.push("SIM0000".into());
  assert!(SourceWsShardPlanR167::from_source(&cfg(SOURCE),&duplicate).is_err());
  let bad=vec!["../../private".to_string()];
  assert!(SourceWsShardPlanR167::from_source(&cfg(SOURCE),&bad).is_err());
 }
 #[test]
 fn r167_start_stagger_from_yaml_not_fake_36_second_global_delay(){
  let x=SOURCE.replacen("start_interval_sec: 1.0",
    "start_interval_sec: 1.5",1);
  let p=SourceWsShardPlanR167::from_source(&cfg(&x),&bases(235)).unwrap();
  assert_eq!(p.start_interval_ms,1500);
  assert_eq!(p.delay_before(5),Ok(std::time::Duration::from_millis(7500)));
  let x=SOURCE.replacen("start_interval_sec: 1.0",
    "start_interval_sec: 0.5",1);
  let p=SourceWsShardPlanR167::from_source(&cfg(&x),&bases(235)).unwrap();
  assert_eq!(p.start_interval_ms,1000); // Python minimum 1.0s
  assert!(p.delay_before(6).is_err());
 }
 #[test]
 fn r185_234_vs_235_universe_is_one_missing_id_not_a_group_size_change(){
  // Synthetic planning proof only: no installed PAPER membership evidence.
  let c=cfg(SOURCE);
  let partial=SourceWsShardPlanR167::from_source(&c,&bases(234)).unwrap();
  let expected=SourceWsShardPlanR167::from_source(&c,&bases(235)).unwrap();
  assert_eq!(partial.group_size,40);
  assert_eq!(partial.task_count(),Ok(18));
  assert_eq!(partial.groups.iter().map(Vec::len).collect::<Vec<_>>(),
    vec![40,40,40,40,40,34]);
  assert_eq!(expected.groups.iter().map(Vec::len).collect::<Vec<_>>(),
    vec![40,40,40,40,40,35]);
  let p=partial.owner_shards();
  let full=expected.owner_shards();
  let missing=full.keys().filter(|key|!p.contains_key(*key)).collect::<Vec<_>>();
  assert_eq!(missing.len(),1);
  assert_eq!(missing[0].as_str(),"SIM0234");
  assert_eq!(partial.config_sha256,expected.config_sha256);
 }
}


#[cfg(test)]
mod r1239_ingress_group_split_tests{
 use super::*;
 #[test]
 fn configured_40_wide_groups_split_to_runtime_limit_without_reordering(){
  let bases=(0..235).map(|i|format!("SIM{i:03}")).collect::<Vec<_>>();
  let configured=bases.chunks(40).map(|g|g.to_vec()).collect::<Vec<_>>();
  let actual=split_source_groups_for_ingress(&configured,8).unwrap();
  assert_eq!(configured.iter().map(Vec::len).collect::<Vec<_>>(),vec![40,40,40,40,40,35]);
  assert_eq!(actual.len(),30);
  assert!(actual.iter().all(|g|!g.is_empty()&&g.len()<=8));
  assert_eq!(actual.last().map(Vec::len),Some(3));
  assert_eq!(actual.into_iter().flatten().collect::<Vec<_>>(),bases);
 }
 #[test]
 fn ingress_group_split_rejects_invalid_width_and_empty_groups(){
  assert!(split_source_groups_for_ingress(&[vec!["BTC".into()]],0).is_err());
  assert!(split_source_groups_for_ingress(&[Vec::new()],8).is_err());
 }
}
