//! R148: source-typed read-only original real_trial paths/flags/inventory.
//! No IO, order dispatch, real admission, path canonicalization or symlink proof.
use crate::native_trading_config::NativeTradingConfig;
use std::collections::BTreeSet;
#[derive(Clone,Debug,PartialEq,Eq)]
pub enum ArtifactPolicyError{Source,Missing,InvalidPath,InvalidInventory}
#[derive(Clone,Debug,PartialEq,Eq)]
pub struct RealTrialArtifactsReview{
 pub source_sha256:String,
 pub legacy_operator_approved:bool,
 pub legacy_autonomous_real_enabled:bool,
 pub legacy_supervised_retrial_enabled:bool,
 pub preflight_path:String,pub kill_path:String,pub incident_path:String,
 pub retrial_plan_path:String,pub retrial_permit_path:String,
 pub allowed_spot_inventory_bases:Vec<String>,
 pub path_or_symlinks_verified:bool,
 pub preflight_or_kill_read:bool,
 pub account_inventory_authenticated:bool,
 pub recovery_document_authenticated:bool,
 pub may_open_real:bool,pub may_mutate_live_or_paper:bool,
 pub installed_paper_source_attested:bool,
}
fn relative(v:&serde_yaml::Value,prefix:&str,suffix:&str)->Result<String,ArtifactPolicyError>{
 let p=v.as_str().ok_or(ArtifactPolicyError::Missing)?;
 if p.len()>160||p.len()<=prefix.len()+suffix.len()
   ||!p.starts_with(prefix)||!p.ends_with(suffix)
   ||p.contains("//")||p.contains(':')
   ||p.bytes().any(|b|!(b.is_ascii_alphanumeric()||b"_-./".contains(&b)))
   ||p[prefix.len()..].split('/').any(|seg|seg.is_empty()||seg=="."||
      seg==".."||seg.starts_with('.')){
  return Err(ArtifactPolicyError::InvalidPath)
 }
 Ok(p.to_owned())
}
pub fn inspect_real_trial_artifacts_without_io(
 cfg:&NativeTradingConfig
)->Result<RealTrialArtifactsReview,ArtifactPolicyError>{
 if cfg.inspect_execution_retry_bounds_read_only().is_err()||cfg.policy.real_execution_authorized
   ||cfg.policy.real_daily_trades!=0{return Err(ArtifactPolicyError::Source)}
 let trial=cfg.raw.get("real_trial").ok_or(ArtifactPolicyError::Missing)?;
 let flag=|name:&str|trial.get(name).and_then(|v|v.as_bool())
     .ok_or(ArtifactPolicyError::Missing);
 let path=|name:&str,prefix:&str,ext:&str|
     relative(trial.get(name).ok_or(ArtifactPolicyError::Missing)?,prefix,ext);
 let listed=trial.get("allowed_spot_inventory_bases")
    .and_then(|v|v.as_sequence()).ok_or(ArtifactPolicyError::Missing)?;
 if listed.is_empty()||listed.len()>200{return Err(ArtifactPolicyError::InvalidInventory)}
 let mut symbols=BTreeSet::new();
 for v in listed{
  let base=v.as_str().ok_or(ArtifactPolicyError::InvalidInventory)?;
  if base.is_empty()||base.len()>24||!base.bytes().all(|b|
      b.is_ascii_uppercase()||b.is_ascii_digit())
      ||!symbols.insert(base.to_owned()){
   return Err(ArtifactPolicyError::InvalidInventory)
  }
 }
 Ok(RealTrialArtifactsReview{
  source_sha256:cfg.sha256.clone(),
  legacy_operator_approved:flag("operator_approved")?,
  legacy_autonomous_real_enabled:flag("autonomous_real_enabled")?,
  legacy_supervised_retrial_enabled:flag("supervised_retrial_enabled")?,
  preflight_path:path("preflight_path","reports/",".json")?,
  kill_path:path("kill_switch_path","data/",".json")?,
  incident_path:path("resolved_incident_path","data/",".json")?,
  retrial_plan_path:path("supervised_retrial_plan_path","data/",".json")?,
  retrial_permit_path:path("supervised_retrial_permit_path","data/",".sqlite")?,
  allowed_spot_inventory_bases:symbols.into_iter().collect(),
  path_or_symlinks_verified:false,preflight_or_kill_read:false,
  account_inventory_authenticated:false,recovery_document_authenticated:false,
  may_open_real:false,may_mutate_live_or_paper:false,
  installed_paper_source_attested:false,
 })
}
#[cfg(test)]
mod tests{
 use super::*;
 const S:&str=include_str!("../config/native_trading.yaml");
 fn case(s:&str)->Result<RealTrialArtifactsReview,ArtifactPolicyError>{
  inspect_real_trial_artifacts_without_io(&NativeTradingConfig::parse(s).unwrap())
 }
 #[test]
 fn r148_nine_original_keys_have_distinct_local_consumers(){
  let base=case(S).unwrap();
  let diff=[
   ("  operator_approved: false","  operator_approved: true"),
   ("  autonomous_real_enabled: false","  autonomous_real_enabled: true"),
   ("  supervised_retrial_enabled: false","  supervised_retrial_enabled: true"),
   ("  preflight_path: reports/real_micro_preflight_latest.json",
    "  preflight_path: reports/other_preflight.json"),
   ("  kill_switch_path: data/real_kill_switch.json",
    "  kill_switch_path: data/other_kill.json"),
   ("  resolved_incident_path: data/one_archived_incident_20260925.json",
    "  resolved_incident_path: data/other_incident.json"),
   ("  supervised_retrial_plan_path: data/supervised_retrial_plan.json",
    "  supervised_retrial_plan_path: data/other_plan.json"),
   ("  supervised_retrial_permit_path: data/supervised_retrial.sqlite",
    "  supervised_retrial_permit_path: data/other_permit.sqlite"),
   ("  - BANANA\n","  - BANANAX\n"),
  ];
  assert_eq!(diff.len(),9);
  for (i,(old,new)) in diff.into_iter().enumerate(){
   let text=S.replacen(old,new,1);
   assert_ne!(text,S,"missing YAML fixture for {i}");
   let r=case(&text).unwrap();
   let same=match i{
    0=>r.legacy_operator_approved==base.legacy_operator_approved,
    1=>r.legacy_autonomous_real_enabled==base.legacy_autonomous_real_enabled,
    2=>r.legacy_supervised_retrial_enabled==base.legacy_supervised_retrial_enabled,
    3=>r.preflight_path==base.preflight_path,
    4=>r.kill_path==base.kill_path,
    5=>r.incident_path==base.incident_path,
    6=>r.retrial_plan_path==base.retrial_plan_path,
    7=>r.retrial_permit_path==base.retrial_permit_path,
    _=>r.allowed_spot_inventory_bases==base.allowed_spot_inventory_bases,
   };
   assert!(!same,"no independent field effect: {i}");
   assert!(!r.may_open_real&&!r.may_mutate_live_or_paper);
   assert!(!r.installed_paper_source_attested);
   assert!(!r.account_inventory_authenticated);
  }
 }
 #[test]
 fn r148_invalid_paths_and_inventory_fail_closed(){
  for (old,new) in [
   ("data/real_kill_switch.json","/tmp/kill.json"),
   ("data/real_kill_switch.json","data/../kill.json"),
   ("data/real_kill_switch.json","data//kill.json"),
   ("data/real_kill_switch.json","data/.hidden/kill.json"),
   ("reports/real_micro_preflight_latest.json","data/other.json"),
   ("data/supervised_retrial.sqlite","data/../permit.sqlite"),
   ("  - BANANA\n","  - banana\n"),
   ("  - BANANA\n","  - BAND\n"),
  ]{
   let changed=S.replacen(old,new,1);
   assert!(changed!=S,"missing source fixture: {old}");
   assert!(case(&changed).is_err(),"bad source admitted {new}");
  }
 }
 #[test]
 fn r148_in_memory_source_drift_and_real_flag_denied(){
  let mut cfg=NativeTradingConfig::parse(S).unwrap();
  cfg.raw["real_trial"]["operator_approved"]=serde_yaml::Value::Bool(true);
  assert_eq!(inspect_real_trial_artifacts_without_io(&cfg),
   Err(ArtifactPolicyError::Source));
  let mut cfg=NativeTradingConfig::parse(S).unwrap();
  cfg.policy.real_execution_authorized=true;
  assert_eq!(inspect_real_trial_artifacts_without_io(&cfg),
   Err(ArtifactPolicyError::Source));
  let r=case(S).unwrap();
  assert!(!r.path_or_symlinks_verified&&!r.preflight_or_kill_read);
  assert!(!r.recovery_document_authenticated&&!r.may_open_real);
 }
}
